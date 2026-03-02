// Copyright (c) Anza Technology, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Core consensus logic and data structures.
//!
//! The central structure of the consensus protocol is [`Alpenglow`].
//! It contains all state for a single consensus instance and also has access
//! to the different necessary network protocols.
//!
//! Most important component data structures defined in this module are:
//! - [`Blockstore`] holds individual shreds and reconstructed blocks for each slot.
//! - [`Pool`] holds votes and certificates for each slot.
//! - [`Votor`] handles the main voting logic.
//!
//! Some other data types for consensus are also defined here:
//! - [`Cert`] represents a certificate of votes of a specific type.
//! - [`Vote`] represents a vote of a specific type.
//! - [`EpochInfo`] holds information about the epoch and all validators.

mod block_producer;
mod blockstore;
mod cert;
mod epoch_info;
mod pool;
mod vote;
pub(crate) mod votor;

use std::marker::{Send, Sync};
use std::sync::Arc;
use std::time::{Duration, Instant};

use color_eyre::Result;
use fastrace::Span;
use fastrace::future::FutureExt;
use log::{trace, warn};
use log::info;
use tokio::sync::{RwLock, mpsc, watch};
use tokio_util::sync::CancellationToken;
use wincode::{SchemaRead, SchemaWrite};

use crate::crypto::{aggsig, signature};
use crate::network::{RepairNetwork, RepairRequestNetwork, TransactionNetwork};
use crate::repair::{Repair, RepairRequestHandler};
use crate::shredder::Shred;
use crate::snapshot::SnapshotStore;
use crate::{All2All, Disseminator, Slot, ValidatorInfo};

pub use blockstore::{BlockInfo, Blockstore};
pub use blockstore::BlockMetadata;
pub use cert::Cert;
pub use epoch_info::EpochInfo;
pub use pool::{AddVoteError, EpochBoundaryEvent, Pool, PoolError, SlashingReport};
pub use vote::Vote;
use votor::{Votor, VotorEvent};

/// Time bound assumed on network transmission delays during periods of synchrony.
const DELTA: Duration = Duration::from_millis(8_000);
/// Target time for block production (slot length)
const TARGET_BLOCK_TIME: Duration = Duration::from_millis(60_000);
/// Time the leader has for producing and sending the block.
const DELTA_BLOCK: Duration = Duration::from_millis(120_000);
/// Timeout to use when we haven't seen any shred from the leader's block.
const DELTA_EARLY_TIMEOUT: Duration = Duration::from_millis(180_000);
// const DELTA_EARLY_TIMEOUT: Duration = DELTA.checked_mul(2).unwrap();
/// Timeout to use when we have seen at least one shred from the leader's block.
const DELTA_TIMEOUT: Duration = Duration::from_millis(240_000);
// const DELTA_TIMEOUT: Duration = DELTA_EARLY_TIMEOUT.checked_add(DELTA_BLOCK).unwrap();
/// Timeout for standstill detection mechanism.
const DELTA_STANDSTILL: Duration = Duration::from_millis(300_000);
/// Max time to produce and send the first slice of a block.
pub(crate) const DELTA_FIRST_SLICE: Duration = Duration::from_millis(30_000);

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
    /// Other validators' info.
    epoch_info: Arc<EpochInfo>,

    /// Blockstore for storing raw block data.
    blockstore: Arc<RwLock<Box<dyn Blockstore + Send + Sync>>>,
    /// Pool of votes and certificates.
    pool: Arc<RwLock<Box<dyn Pool + Send + Sync>>>,

    /// Block production (i.e. leader side) component of the consensus protocol.
    block_producer: Arc<BlockProducer<D, T>>,

    /// All-to-all broadcast network protocol for consensus messages.
    all2all: Arc<A>,
    /// Block dissemination network protocol for shreds.
    disseminator: Arc<D>,

    /// Indicates whether the node is shutting down.
    cancel_token: CancellationToken,
    /// Votor task handle.
    votor_handle: tokio::task::JoinHandle<()>,

    /// watch channel for broadcasting epoch info updates across tasks
    epoch_info_tx: watch::Sender<Arc<EpochInfo>>,
    epoch_info_rx: watch::Receiver<Arc<EpochInfo>>,
    /// receiver for epoch boundary events from the pool
    epoch_boundary_rx: mpsc::Receiver<EpochBoundaryEvent>,
    /// receiver for slashing reports from the pool
    slashing_rx: mpsc::Receiver<SlashingReport>,
    /// optional execution state for epoch transitions
    execution_state: Option<Arc<RwLock<bunker_coin_core::execution::State>>>,
    /// optional snapshot store for persisting state at epoch boundaries
    snapshot_store: Option<SnapshotStore>,
}

impl<A, D, T> Alpenglow<A, D, T>
where
    A: All2All + Send + Sync + 'static,
    D: Disseminator + Send + Sync + 'static,
    T: TransactionNetwork + 'static,
{
    /// Creates a new Alpenglow consensus node.
    ///
    /// `repair_network` - [`RepairNetwork`] for sending requests and receiving responses.
    /// `repair_request_network` - [`RepairRequestNetwork`] for answering incoming requests.
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
        let (slashing_tx, slashing_rx) = mpsc::channel(256);
        let (epoch_info_tx, epoch_info_rx) = watch::channel(epoch_info.clone());
        let all2all = Arc::new(all2all);

        let blockstore: Box<dyn Blockstore + Send + Sync> =
            Box::new(BlockstoreImpl::new(epoch_info.clone(), votor_tx.clone()));
        let blockstore = Arc::new(RwLock::new(blockstore));
        let mut pool = Pool::new(epoch_info.clone(), votor_tx.clone(), repair_tx.clone());
        pool.set_blockstore(Arc::clone(&blockstore));
        pool.set_epoch_boundary_channel(epoch_boundary_tx);
        pool.set_slashing_channel(slashing_tx);
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
            DELTA_BLOCK,
            DELTA_FIRST_SLICE,
            epoch_info_rx.clone(),
        ));

        let snapshot_store = SnapshotStore::new(epoch_info.own_id);

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
            slashing_rx,
            execution_state: None,
            snapshot_store: Some(snapshot_store),
        }
    }

    /// Starts the different tasks of the Alpenglow node.
    ///
    /// # Errors
    ///
    /// Returns an error only if any of the tasks panics.
    #[fastrace::trace(short_name = true)]
    pub async fn run(mut self) -> Result<()> {
        // clean load after startup
        {
            let pool_guard = self.pool.read().await;
            let highest_finalized = pool_guard.finalized_slot();
            drop(pool_guard);

            let mut blockstore_guard = self.blockstore.write().await;
            blockstore_guard.clean_beyond_finalized(highest_finalized);
            drop(blockstore_guard);
        }

        // take the epoch boundary receiver out so we can move it into the task
        let epoch_boundary_rx = std::mem::replace(
            &mut self.epoch_boundary_rx,
            mpsc::channel(1).1,
        );
        let slashing_rx = std::mem::replace(
            &mut self.slashing_rx,
            mpsc::channel(1).1,
        );
        let snapshot_store = self.snapshot_store.take();
        let epoch_info_tx = self.epoch_info_tx.clone();
        let execution_state = self.execution_state.clone();
        let epoch_info_clone = self.epoch_info.clone();
        let epoch_transition_span = Span::enter_with_local_parent("epoch transition loop");
        let epoch_loop = tokio::spawn(
            async move {
                epoch_transition_loop(
                    epoch_boundary_rx,
                    epoch_info_tx,
                    execution_state,
                    slashing_rx,
                    snapshot_store,
                    epoch_info_clone,
                ).await;
            }
            .in_span(epoch_transition_span),
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

        let (msg_res, prod_res) = tokio::join!(msg_loop, prod_loop);
        msg_res??;
        prod_res??;
        Ok(())
    }

    pub fn get_info(&self) -> &ValidatorInfo {
        self.epoch_info.validator(self.epoch_info.own_id)
    }

    pub fn get_pool(&self) -> Arc<RwLock<Box<dyn Pool + Send + Sync>>> {
        Arc::clone(&self.pool)
    }

    pub fn get_cancel_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    /// Handles incoming messages on all the different network interfaces.
    ///
    /// [`All2All`]: Handles incoming votes and certificates. Adds them to the [`Pool`].
    /// [`Disseminator`]: Handles incoming shreds. Adds them to the [`Blockstore`].
    async fn message_loop(self: &Arc<Self>) -> Result<()> {
        loop {
            tokio::select! {
                // handle incoming votes and certificates
                res = self.all2all.receive() => self.handle_all2all_message(res?).await,
                // handle shreds received by block dissemination protocol
                res = self.disseminator.receive() => self.handle_disseminator_shred(res?).await?,

                () = self.cancel_token.cancelled() => return Ok(()),
            };
        }
    }

    /// Handles standstill detection and triggers recovery.
    ///
    /// Keeps track of when consensus progresses, i.e., [`Pool`] finalizes new blocks.
    /// Triggers standstill recovery if no progress was detected for a long time.
    async fn standstill_loop(self: &Arc<Self>) -> Result<()> {
        let mut finalized_slot = Slot::new(0);
        let mut last_progress = Instant::now();
        loop {
            let slot = self.pool.read().await.finalized_slot();
            if slot > finalized_slot {
                finalized_slot = slot;
                last_progress = Instant::now();
            } else if last_progress.elapsed() > DELTA_STANDSTILL {
                self.pool.read().await.recover_from_standstill().await;
                last_progress = Instant::now();
            }
            tokio::time::sleep(DELTA_BLOCK).await;
        }
    }

    #[fastrace::trace(short_name = true)]
    async fn handle_all2all_message(&self, msg: ConsensusMessage) {
        trace!("received all2all msg: {msg:?}");
        match msg {
            ConsensusMessage::Vote(v) => match self.pool.write().await.add_vote(v).await {
                Ok(()) => {}
                Err(AddVoteError::Slashable(offence)) => {
                    warn!("slashable offence detected: {offence}");
                }
                Err(err) => trace!("ignoring invalid vote: {err}"),
            },
            ConsensusMessage::Cert(c) => match self.pool.write().await.add_cert(c).await {
                Ok(()) => {}
                Err(err) => trace!("ignoring invalid cert: {err}"),
            },
        }
    }

    #[fastrace::trace(short_name = true)]
    async fn handle_disseminator_shred(&self, shred: Shred) -> std::io::Result<()> {
        // potentially forward shred
        self.disseminator.forward(&shred).await?;

        // if we are the leader, we already have the shred
        let slot = shred.payload().header.slot;
        if self.epoch_info.leader(slot).id == self.epoch_info.own_id {
            return Ok(());
        }

        // otherwise, ingest into blockstore
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

    pub fn blockstore(&self) -> std::sync::Arc<tokio::sync::RwLock<crate::consensus::Blockstore>> {
        std::sync::Arc::clone(&self.blockstore)
    }

    pub fn epoch_info_rx(&self) -> watch::Receiver<Arc<EpochInfo>> {
        self.epoch_info_rx.clone()
    }

    pub fn set_execution_state(&mut self, state: Arc<RwLock<bunker_coin_core::execution::State>>) {
        self.execution_state = Some(state);
    }
}

async fn epoch_transition_loop(
    mut epoch_boundary_rx: mpsc::Receiver<EpochBoundaryEvent>,
    epoch_info_tx: watch::Sender<Arc<EpochInfo>>,
    execution_state: Option<Arc<RwLock<bunker_coin_core::execution::State>>>,
    mut slashing_rx: mpsc::Receiver<SlashingReport>,
    snapshot_store: Option<SnapshotStore>,
    epoch_info: Arc<EpochInfo>,
) {
    while let Some(event) = epoch_boundary_rx.recv().await {
        let completed_epoch = event.epoch;
        let new_epoch = completed_epoch + 1;
        info!("epoch boundary reached: epoch {} completed at slot {}", completed_epoch, event.finalized_slot);

        // run state transition if execution state is available
        if let Some(ref state) = execution_state {
            let mut state_guard = state.write().await;

            // drain slashing reports and convert to offence reports
            while let Ok(report) = slashing_rx.try_recv() {
                let validator_pk = *epoch_info.validator(report.validator_id).pubkey.as_bytes();
                let offence_kind = match report.offence {
                    pool::SlashableOffence::NotarDifferentHash(_, _) |
                    pool::SlashableOffence::SkipAndNotarize(_, _) => {
                        bunker_coin_core::staking::SlashOffenceKind::ConflictingVote
                    }
                    pool::SlashableOffence::SkipAndFinalize(_, _) |
                    pool::SlashableOffence::NotarFallbackAndFinalize(_, _) => {
                        bunker_coin_core::staking::SlashOffenceKind::DoubleVote
                    }
                };
                state_guard.staking.report_offence(bunker_coin_core::staking::SlashingEvent {
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

            // save snapshot
            if let Some(ref store) = snapshot_store {
                store.save_snapshot(new_epoch, &state_guard);
                store.prune_old_snapshots(5);
            }
        }

        // build new epoch info from current (for now, same validator set)
        let current = epoch_info_tx.borrow().clone();
        let new_epoch_info = Arc::new(EpochInfo::new(
            new_epoch,
            current.own_id,
            current.validators.clone(),
        ));

        // unblock block production with the new epoch info
        let _ = epoch_info_tx.send(new_epoch_info);
        info!("epoch {} started", new_epoch);
    }
}
