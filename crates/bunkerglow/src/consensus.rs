// Copyright (c) Anza Technology, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Core Alpenglow consensus orchestration.
//!
//! Wires [`Blockstore`], [`Pool`], [`Votor`], block production, repair, epoch
//! transitions, and snapshot checkpoints for one validator.

mod block_producer;
mod blockstore;
mod cert;
mod epoch_info;
mod link_liveness;
mod pool;
mod vote;
mod vote_history;
pub(crate) mod votor;

use std::marker::{Send, Sync};
use std::sync::Arc;
use std::time::{Duration, Instant};

use block_producer::BlockProducer;
pub use blockstore::{BlockInfo, BlockMetadata, Blockstore, BlockstoreImpl};
pub use cert::Cert;
use color_eyre::Result;
pub use epoch_info::EpochInfo;
use fastrace::Span;
use fastrace::future::FutureExt;
pub use link_liveness::{LinkLiveness, NoLiveness, SwappableLiveness};
use log::{error, info, trace, warn};
pub use pool::{
    AddVoteError, EpochBoundaryEvent, FinalizedSlotEvent, Pool, PoolError, PoolImpl, SlashingReport,
};
use tokio::sync::{RwLock, mpsc, watch};
use tokio_util::sync::CancellationToken;
pub use vote::Vote;
use vote_history::VoteHistory;
use votor::Votor;
use wincode::{SchemaRead, SchemaWrite};

use crate::crypto::{aggsig, signature};
use crate::network::{RepairNetwork, RepairRequestNetwork, TransactionNetwork};
use crate::repair::{Repair, RepairRequestHandler};
use crate::shredder::Shred;
use crate::snapshot::{SnapshotCheckpoint, SnapshotStore};
use crate::{All2All, Disseminator, Slot, ValidatorInfo};

/// Consensus timer multiplier from `BUNKER_DELTA_MULT`; slow links stretch all deltas.
static DELTA_MULT: std::sync::LazyLock<f64> = std::sync::LazyLock::new(|| {
    std::env::var("BUNKER_DELTA_MULT")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|m| *m > 0.0)
        .unwrap_or(1.0)
});

/// Scale a base duration by [`DELTA_MULT`].
fn scaled(base_ms: u64) -> Duration {
    Duration::from_millis((base_ms as f64 * *DELTA_MULT) as u64)
}

/// Time bound assumed on network transmission delays during periods of synchrony.
pub(crate) fn delta() -> Duration {
    scaled(8_000)
}
/// Time the leader has for producing and sending the block.
fn delta_block() -> Duration {
    scaled(120_000)
}
/// Timeout to use when we have seen at least one shred from the leader's block.
fn delta_timeout() -> Duration {
    scaled(240_000)
}
/// Standstill detection is not delta-scaled; recovery is a small cert/vote rebroadcast.
fn delta_standstill() -> Duration {
    std::env::var("BUNKER_STANDSTILL_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(300))
}
/// Max time to produce and send the first slice of a block.
pub(crate) fn delta_first_slice() -> Duration {
    scaled(30_000)
}
/// First-tx wait before producing an empty slice; prevents idle mempools stalling slots.
pub(crate) fn delta_empty_slice() -> Duration {
    scaled(2_000)
}
/// Inter-tx grace for closing a partially filled slice promptly under light load.
pub(crate) fn delta_pack_grace() -> Duration {
    scaled(2_000)
}
/// Slow-path final-vote deferral window; falls back if fast-final does not land.
pub(crate) fn delta_final_vote_grace() -> Duration {
    scaled(16_000)
}

#[derive(Clone, Debug, SchemaRead, SchemaWrite)]
pub enum ConsensusMessage {
    Vote(Vote),
    Cert(Cert),
}

impl From<Vote> for ConsensusMessage {
    fn from(vote: Vote) -> Self {
        Self::Vote(vote)
    }
}

impl From<Cert> for ConsensusMessage {
    fn from(cert: Cert) -> Self {
        Self::Cert(cert)
    }
}

/// Alpenglow consensus protocol implementation.
pub struct Alpenglow<A: All2All, D: Disseminator, T>
where
    T: TransactionNetwork + 'static,
{
    epoch_info: Arc<EpochInfo>,

    blockstore: Arc<RwLock<Box<dyn Blockstore + Send + Sync>>>,
    pool: Arc<RwLock<Box<dyn Pool + Send + Sync>>>,

    block_producer: Arc<BlockProducer<D, T>>,

    all2all: Arc<A>,
    disseminator: Arc<D>,

    cancel_token: CancellationToken,
    votor_handle: tokio::task::JoinHandle<()>,

    epoch_info_tx: watch::Sender<Arc<EpochInfo>>,
    epoch_info_rx: watch::Receiver<Arc<EpochInfo>>,
    epoch_boundary_rx: mpsc::Receiver<EpochBoundaryEvent>,
    finalized_slot_rx: mpsc::Receiver<FinalizedSlotEvent>,
    slashing_rx: mpsc::Receiver<SlashingReport>,
    execution_state: Option<Arc<RwLock<bunker_coin_core::execution::State>>>,
    snapshot_store: Option<Arc<SnapshotStore>>,
    /// Epoch transition payloads waiting for this node's first block of the epoch.
    pending_epoch_transitions: Arc<RwLock<std::collections::BTreeMap<u64, Vec<u8>>>>,
    /// Swappable link-liveness source consulted by Votor's crashed-leader timeout.
    link_liveness: Arc<SwappableLiveness>,
}

impl<A, D, T> Alpenglow<A, D, T>
where
    A: All2All + Send + Sync + 'static,
    D: Disseminator + Send + Sync + 'static,
    T: TransactionNetwork + 'static,
{
    /// Creates a new Alpenglow consensus node.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new<RN, RR>(
        secret_key: signature::SecretKey,
        voting_secret_key: aggsig::SecretKey,
        all2all: A,
        disseminator: D,
        repair_network: RN,
        repair_request_network: RR,
        epoch_info: Arc<EpochInfo>,
        txs_receiver: T,
    ) -> Self
    where
        RR: RepairRequestNetwork + 'static,
        RN: RepairNetwork + 'static,
    {
        let cancel_token = CancellationToken::new();
        let (votor_tx, votor_rx) = mpsc::channel(1024);
        let (repair_tx, repair_rx) = mpsc::channel(1024);
        let (epoch_boundary_tx, epoch_boundary_rx) = mpsc::channel(16);
        let (finalized_slot_tx, finalized_slot_rx) = mpsc::channel(1024);
        let (slashing_tx, slashing_rx) = mpsc::channel(256);
        let (epoch_info_tx, epoch_info_rx) = watch::channel(epoch_info.clone());
        let pending_epoch_transitions = Arc::new(RwLock::new(std::collections::BTreeMap::new()));
        let all2all = Arc::new(all2all);

        let blockstore: Box<dyn Blockstore + Send + Sync> =
            Box::new(BlockstoreImpl::new(epoch_info.clone(), votor_tx.clone()));
        let blockstore = Arc::new(RwLock::new(blockstore));
        let mut pool = PoolImpl::new(epoch_info.clone(), votor_tx.clone(), repair_tx.clone());
        pool.set_blockstore(Arc::clone(&blockstore));
        pool.set_epoch_boundary_channel(epoch_boundary_tx);
        pool.set_finalized_slot_channel(finalized_slot_tx);
        pool.set_slashing_channel(slashing_tx);
        // Votor replays its durable own-vote log relative to the restored frontier.
        let restored_finalized_slot = Pool::finalized_slot(&pool);
        // Fast-forward the epoch watch to the restored frontier's epoch: the
        // boundary finalization that would publish it happened before the
        // restart and never re-fires, leaving the producer parked at the
        // boundary window forever.
        let restored_epoch = restored_finalized_slot.epoch();
        if restored_epoch > epoch_info.epoch() {
            let _ = epoch_info_tx.send(Arc::new(EpochInfo::new(
                restored_epoch,
                epoch_info.own_id,
                epoch_info.validators.clone(),
            )));
        }
        let pool: Box<dyn Pool + Send + Sync> = Box::new(pool);
        let pool = Arc::new(RwLock::new(pool));

        let repair_request_handler = RepairRequestHandler::new(
            epoch_info.clone(),
            blockstore.clone(),
            repair_request_network,
        );
        let _repair_request_handler =
            tokio::spawn(async move { repair_request_handler.run().await });

        let mut repair = Repair::new(
            Arc::clone(&blockstore),
            Arc::clone(&pool),
            repair_network,
            epoch_info.clone(),
        );

        let _repair_handle = tokio::spawn(
            async move { repair.repair_loop(repair_rx).await }
                .in_span(Span::enter_with_local_parent("repair loop")),
        );

        let mut votor = Votor::new(
            epoch_info.own_id,
            voting_secret_key,
            votor_tx.clone(),
            votor_rx,
            all2all.clone(),
        );
        // Radio swaps in keepalive liveness so slow-but-alive links pause timeouts.
        let link_liveness = Arc::new(SwappableLiveness::new());
        votor.set_link_liveness(link_liveness.clone());
        // Durable own-vote log prevents restart from casting conflicting votes.
        votor.set_vote_history(
            VoteHistory::open(epoch_info.own_id),
            restored_finalized_slot,
        );
        let votor_handle = tokio::spawn(
            async move { votor.voting_loop().await.unwrap() }
                .in_span(Span::enter_with_local_parent("voting loop")),
        );

        let disseminator = Arc::new(disseminator);

        let block_producer = Arc::new(BlockProducer::new(
            secret_key,
            epoch_info.clone(),
            disseminator.clone(),
            txs_receiver,
            blockstore.clone(),
            pool.clone(),
            cancel_token.clone(),
            delta_block(),
            delta_first_slice(),
            epoch_info_rx.clone(),
            pending_epoch_transitions.clone(),
        ));

        let snapshot_store = Arc::new(SnapshotStore::new(epoch_info.own_id));

        Self {
            epoch_info,
            blockstore,
            pool,
            block_producer,
            all2all,
            disseminator,
            cancel_token,
            votor_handle,
            epoch_info_tx,
            epoch_info_rx,
            epoch_boundary_rx,
            finalized_slot_rx,
            slashing_rx,
            execution_state: None,
            snapshot_store: Some(snapshot_store),
            pending_epoch_transitions,
            link_liveness,
        }
    }

    /// Swaps Votor's crashed-leader timeout liveness source.
    pub fn set_link_liveness(&self, liveness: Arc<dyn LinkLiveness>) {
        self.link_liveness.set(liveness);
    }

    /// Starts the Alpenglow node tasks.
    ///
    /// # Errors
    /// Returns an error only if a main task panics.
    #[fastrace::trace(short_name = true)]
    pub async fn run(mut self) -> Result<()> {
        {
            let pool_guard = self.pool.read().await;
            let highest_finalized = pool_guard.finalized_slot();
            drop(pool_guard);

            let mut blockstore_guard = self.blockstore.write().await;
            blockstore_guard.clean_beyond_finalized(highest_finalized);
            drop(blockstore_guard);
        }

        let epoch_boundary_rx = std::mem::replace(&mut self.epoch_boundary_rx, mpsc::channel(1).1);
        let finalized_slot_rx = std::mem::replace(&mut self.finalized_slot_rx, mpsc::channel(1).1);
        let slashing_rx = std::mem::replace(&mut self.slashing_rx, mpsc::channel(1).1);
        let snapshot_store = self.snapshot_store.take();
        let snapshot_store_for_checkpoint = snapshot_store.clone();
        let epoch_info_tx = self.epoch_info_tx.clone();
        let execution_state = self.execution_state.clone();
        let epoch_info_clone = self.epoch_info.clone();
        let blockstore = self.blockstore.clone();
        let pending_epoch_transitions = self.pending_epoch_transitions.clone();
        let epoch_transition_span = Span::enter_with_local_parent("epoch transition loop");
        let epoch_loop = tokio::spawn(
            async move {
                epoch_transition_loop(
                    epoch_boundary_rx,
                    epoch_info_tx,
                    execution_state,
                    slashing_rx,
                    snapshot_store.clone(),
                    pending_epoch_transitions,
                    epoch_info_clone,
                )
                .await;
            }
            .in_span(epoch_transition_span),
        );
        let finalized_checkpoint_loop = tokio::spawn(
            async move {
                finalized_checkpoint_loop(
                    finalized_slot_rx,
                    blockstore,
                    snapshot_store_for_checkpoint,
                )
                .await;
            }
            .in_span(Span::enter_with_local_parent(
                "snapshot checkpoint finality loop",
            )),
        );

        let msg_loop_span = Span::enter_with_local_parent("message loop");
        let node = Arc::new(self);
        let nn = node.clone();
        let msg_loop = tokio::spawn(async move { nn.message_loop().await }.in_span(msg_loop_span));

        let standstill_loop_span = Span::enter_with_local_parent("standstill detection loop");
        let nn = node.clone();
        let standstill_loop =
            tokio::spawn(async move { nn.standstill_loop().await }.in_span(standstill_loop_span));

        let block_production_span = Span::enter_with_local_parent("block production");
        let block_producer = Arc::clone(&node.block_producer);
        let prod_loop = tokio::spawn(
            async move { block_producer.block_production_loop().await }
                .in_span(block_production_span),
        );

        node.cancel_token.cancelled().await;
        node.votor_handle.abort();
        msg_loop.abort();
        standstill_loop.abort();
        prod_loop.abort();
        epoch_loop.abort();
        finalized_checkpoint_loop.abort();

        // Await all tasks so their `Arc<Alpenglow>` clones release RocksDB before reconnect.
        // Cancelled joins are expected teardown, not failure.
        let (msg_res, prod_res, _, _, _) = tokio::join!(
            msg_loop,
            prod_loop,
            standstill_loop,
            epoch_loop,
            finalized_checkpoint_loop,
        );
        drop(node);

        // Surface genuine main-loop panics; cancellations are teardown.
        if let Ok(Err(e)) = msg_res {
            return Err(e);
        }
        if let Ok(Err(e)) = prod_res {
            return Err(e);
        }
        Ok(())
    }

    pub fn get_info(&self) -> &ValidatorInfo {
        self.epoch_info.validator(self.epoch_info.own_id)
    }

    pub fn get_pool(&self) -> Arc<RwLock<Box<dyn Pool + Send + Sync>>> {
        Arc::clone(&self.pool)
    }

    /// Shared blockstore handle for out-of-band readers.
    pub fn get_blockstore(&self) -> Arc<RwLock<Box<dyn Blockstore + Send + Sync>>> {
        Arc::clone(&self.blockstore)
    }

    pub fn get_cancel_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    /// Ingests incoming votes, certs, and shreds.
    async fn message_loop(self: &Arc<Self>) -> Result<()> {
        // Ingestion errors must not kill this only vote/cert/shred receive path.
        // Log and back off so persistent failures cannot hot-loop.
        loop {
            tokio::select! {
                res = self.all2all.receive() => match res {
                    Ok(msg) => self.handle_all2all_message(msg).await,
                    Err(e) => {
                        error!("all2all receive failed (vote/cert ingestion degraded): {e}");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                },
                res = self.disseminator.receive() => match res {
                    Ok(shred) => {
                        if let Err(e) = self.handle_disseminator_shred(shred).await {
                            error!("disseminator shred handling failed: {e}");
                        }
                    }
                    Err(e) => {
                        error!("disseminator receive failed (shred ingestion degraded): {e}");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                },

                () = self.cancel_token.cancelled() => return Ok(()),
            };
        }
    }

    /// Triggers standstill recovery when finalization stops progressing.
    async fn standstill_loop(self: &Arc<Self>) -> Result<()> {
        let mut finalized_slot = Slot::new(0);
        let mut last_progress = Instant::now();
        // Dry recoveries double the interval so rebroadcasts don't starve shreds of airtime.
        let mut dry_recoveries: u32 = 0;
        loop {
            let slot = self.pool.read().await.finalized_slot();
            if slot > finalized_slot {
                finalized_slot = slot;
                last_progress = Instant::now();
                dry_recoveries = 0;
            } else if last_progress.elapsed() > delta_standstill() * 2u32.pow(dry_recoveries) {
                self.pool.read().await.recover_from_standstill().await;
                last_progress = Instant::now();
                dry_recoveries = (dry_recoveries + 1).min(2);
            }
            // Fixed cadence avoids adding a full scaled block window of detection latency.
            tokio::time::sleep(delta_block().min(Duration::from_secs(60))).await;
        }
    }

    #[fastrace::trace(short_name = true)]
    async fn handle_all2all_message(&self, msg: ConsensusMessage) {
        trace!("received all2all msg: {msg:?}");
        match msg {
            ConsensusMessage::Vote(v) => {
                // Vote logs stay info/warn because finalization stalls need visible evidence.
                let (slot, signer) = (v.slot(), v.signer());
                match self.pool.write().await.add_vote(v).await {
                    Ok(()) => info!("counted vote for slot {slot} from validator {signer}"),
                    Err(AddVoteError::Slashable(offence)) => {
                        warn!("slashable offence detected: {offence}");
                    }
                    Err(err) => {
                        warn!("ignoring vote for slot {slot} from validator {signer}: {err}");
                    }
                }
            }
            ConsensusMessage::Cert(c) => {
                let (slot, kind) = (c.slot(), c.kind_str());
                match self.pool.write().await.add_cert(c).await {
                    Ok(()) => info!("ingested {kind} cert for slot {slot}"),
                    Err(err) => warn!("ignoring {kind} cert for slot {slot}: {err}"),
                }
            }
        }
    }

    #[fastrace::trace(short_name = true)]
    async fn handle_disseminator_shred(&self, shred: Shred) -> std::io::Result<()> {
        self.disseminator.forward(&shred).await?;

        let slot = shred.payload().header.slot;
        if self.epoch_info.leader(slot).id == self.epoch_info.own_id {
            return Ok(());
        }

        let res = self
            .blockstore
            .write()
            .await
            .add_shred_from_disseminator(shred)
            .await;
        if let Ok(Some(block_info)) = res {
            let mut guard = self.pool.write().await;
            let block_id = (slot, block_info.hash);
            guard.add_block(block_id, block_info.parent).await;
        }
        Ok(())
    }

    pub fn blockstore(&self) -> Arc<RwLock<Box<dyn Blockstore + Send + Sync>>> {
        Arc::clone(&self.blockstore)
    }

    pub fn epoch_info_rx(&self) -> watch::Receiver<Arc<EpochInfo>> {
        self.epoch_info_rx.clone()
    }

    pub fn set_execution_state(&mut self, state: Arc<RwLock<bunker_coin_core::execution::State>>) {
        self.execution_state = Some(state);
    }

    pub fn snapshot_store(&self) -> Option<Arc<SnapshotStore>> {
        self.snapshot_store.clone()
    }
}

async fn epoch_transition_loop(
    mut epoch_boundary_rx: mpsc::Receiver<EpochBoundaryEvent>,
    epoch_info_tx: watch::Sender<Arc<EpochInfo>>,
    execution_state: Option<Arc<RwLock<bunker_coin_core::execution::State>>>,
    mut slashing_rx: mpsc::Receiver<SlashingReport>,
    snapshot_store: Option<Arc<SnapshotStore>>,
    pending_epoch_transitions: Arc<RwLock<std::collections::BTreeMap<u64, Vec<u8>>>>,
    epoch_info: Arc<EpochInfo>,
) {
    while let Some(event) = epoch_boundary_rx.recv().await {
        let completed_epoch = event.epoch;
        let new_epoch = completed_epoch + 1;
        info!(
            "epoch boundary reached: epoch {} completed at slot {}",
            completed_epoch, event.finalized_slot
        );

        if let Some(ref state) = execution_state {
            let mut state_guard = state.write().await;

            // Convert pending slashing reports into epoch offences.
            while let Ok(report) = slashing_rx.try_recv() {
                let validator_pk = *epoch_info.validator(report.validator_id).pubkey.as_bytes();
                let offence_kind = match report.offence {
                    pool::SlashableOffence::NotarDifferentHash(_, _)
                    | pool::SlashableOffence::SkipAndNotarize(_, _) => {
                        bunker_coin_core::staking::SlashOffenceKind::ConflictingVote
                    }
                    pool::SlashableOffence::SkipAndFinalize(_, _)
                    | pool::SlashableOffence::NotarFallbackAndFinalize(_, _) => {
                        bunker_coin_core::staking::SlashOffenceKind::DoubleVote
                    }
                };
                state_guard
                    .staking
                    .report_offence(bunker_coin_core::staking::SlashingEvent {
                        validator: validator_pk,
                        offence: offence_kind,
                        epoch: completed_epoch,
                    });
            }

            let result = state_guard.process_epoch_transition(completed_epoch);
            info!(
                "epoch transition: {} fees distributed, {} bonds activated, {} retires completed, {} slashes applied, {} deactivated, {} validators, state_hash={:x?}",
                result.fees_distributed,
                result.bonds_activated.len(),
                result.retires_completed.len(),
                result.slashes_applied.len(),
                result.deactivated.len(),
                result.new_validators.len(),
                &result.state_hash[..8],
            );

            if let Some(ref store) = snapshot_store {
                store.save_snapshot(new_epoch, &state_guard);
                if let Some(manifest) = store.load_manifest(new_epoch) {
                    if manifest.state_hash != result.state_hash {
                        warn!(
                            "epoch transition snapshot hash mismatch: epoch {}, transition_state_hash={:x?}, snapshot_state_hash={:x?}",
                            new_epoch,
                            &result.state_hash[..8],
                            &manifest.state_hash[..8],
                        );
                    } else {
                        let transition_block =
                            bunker_coin_core::epoch_transition::EpochTransitionBlock {
                                epoch: new_epoch,
                                last_slot: event.finalized_slot.inner(),
                                fees_distributed: result.fees_distributed,
                                bonds_activated: result.bonds_activated.clone(),
                                retires_completed: result.retires_completed.clone(),
                                new_validator_set: result.new_validators.clone(),
                                state_hash: result.state_hash,
                                snapshot_chunk_root: manifest
                                    .chunk_root
                                    .as_ref()
                                    .try_into()
                                    .expect("snapshot chunk root is 32 bytes"),
                                snapshot_chunk_count: manifest.chunk_count,
                                snapshot_total_bytes: manifest.total_bytes,
                                snapshot_chunk_size: manifest.chunk_size,
                                slashes_applied: result.slashes_applied.clone(),
                                deactivated_validators: result.deactivated.clone(),
                                location_claims_validated: result.location_claims_validated.clone(),
                                messages_anchored: result.messages_anchored,
                                deliveries_completed: result.deliveries_completed,
                            };
                        match bincode::serde::encode_to_vec(
                            &transition_block,
                            bincode::config::standard(),
                        ) {
                            Ok(encoded) => {
                                let mut pending = pending_epoch_transitions.write().await;
                                // Only this node's first block can consume a transition payload;
                                // drop entries older than the epoch starting now.
                                *pending = pending.split_off(&new_epoch);
                                pending.insert(new_epoch, encoded);
                            }
                            Err(e) => warn!(
                                "failed to encode epoch transition block for epoch {}: {}",
                                new_epoch, e
                            ),
                        }
                        info!(
                            "epoch transition snapshot checkpoint: epoch {}, state_hash={:x?}, chunk_root={:x?}, chunks={}, chunk_size={}, total_bytes={}",
                            manifest.epoch,
                            &manifest.state_hash[..8],
                            &manifest.chunk_root.as_ref()[..8],
                            manifest.chunk_count,
                            manifest.chunk_size,
                            manifest.total_bytes,
                        );
                    }
                } else {
                    warn!(
                        "epoch transition snapshot manifest unavailable after save: epoch {}",
                        new_epoch
                    );
                }
                store.prune_old_snapshots(5);
            }
        }

        let current = epoch_info_tx.borrow().clone();
        let new_epoch_info = Arc::new(EpochInfo::new(
            new_epoch,
            current.own_id,
            current.validators.clone(),
        ));

        let _ = epoch_info_tx.send(new_epoch_info);
        info!("epoch {} started", new_epoch);
    }
}

async fn finalized_checkpoint_loop(
    mut finalized_slot_rx: mpsc::Receiver<FinalizedSlotEvent>,
    blockstore: Arc<RwLock<Box<dyn Blockstore + Send + Sync>>>,
    snapshot_store: Option<Arc<SnapshotStore>>,
) {
    while let Some(event) = finalized_slot_rx.recv().await {
        // Prune outside pool locks to avoid the pool-to-blockstore lock edge
        // that can deadlock against shred ingestion.
        blockstore.write().await.prune_finalized(event.slot);

        let Some(ref snapshot_store) = snapshot_store else {
            continue;
        };
        let block_hash = event
            .finalization_certs
            .iter()
            .find_map(|cert| cert.block_hash().cloned());
        let Some(block_hash) = block_hash else {
            continue;
        };
        let block_id = (event.slot, block_hash);
        let Some(block) = blockstore.read().await.get_block(&block_id) else {
            continue;
        };
        let Some(transition_block) = block.epoch_transition().cloned() else {
            continue;
        };
        let Some(manifest) = snapshot_store.load_manifest(transition_block.epoch) else {
            warn!(
                "finalized epoch transition block has no snapshot manifest: epoch {}",
                transition_block.epoch
            );
            continue;
        };
        let finalization_certs: Vec<Vec<u8>> = event
            .finalization_certs
            .iter()
            .filter_map(|cert| wincode::serialize(cert).ok())
            .collect();
        let checkpoint = SnapshotCheckpoint {
            epoch: transition_block.epoch,
            finalized_slot: event.slot.inner(),
            transition_block,
            finalization_certs,
        };
        if !checkpoint.matches_manifest(&manifest) {
            warn!(
                "finalized epoch transition block does not match snapshot manifest: epoch {}",
                checkpoint.epoch
            );
            continue;
        }
        if let Err(e) = snapshot_store.save_checkpoint(checkpoint) {
            warn!(
                "failed to save finalized epoch transition checkpoint for slot {}: {}",
                event.slot, e
            );
        }
    }
}
