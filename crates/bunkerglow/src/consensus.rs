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
use static_assertions::const_assert;
use tokio::sync::{RwLock, mpsc};
use tokio_util::sync::CancellationToken;
use wincode::{SchemaRead, SchemaWrite};

use crate::crypto::{Hash, aggsig, signature};
use crate::network::{Network, NetworkError, NetworkMessage};
use crate::repair::{Repair, RepairMessage};
use crate::shredder::{MAX_DATA_PER_SLICE, RegularShredder, Shred, Shredder, Slice};
use crate::shredder;
use crate::{All2All, Disseminator, Slot, ValidatorInfo};

pub use blockstore::{BlockInfo, Blockstore};
pub use blockstore::BlockMetadata;
pub use cert::Cert;
pub use epoch_info::EpochInfo;
pub use pool::{Pool, PoolError};
pub use vote::Vote;
use votor::{Votor, VotorEvent};

/// Number of slots in each leader window.
pub const SLOTS_PER_WINDOW: u64 = 2;
/// Number of slots in each epoch.
pub const SLOTS_PER_EPOCH: u64 = 4_500;
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
        let all2all = Arc::new(all2all);

        let blockstore: Box<dyn Blockstore + Send + Sync> =
            Box::new(BlockstoreImpl::new(epoch_info.clone(), votor_tx.clone()));
        let blockstore = Arc::new(RwLock::new(blockstore));
        let mut pool = Pool::new(epoch_info.clone(), votor_tx.clone(), repair_tx.clone());
        pool.set_blockstore(Arc::clone(&blockstore));
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
        ));

        Self {
            epoch_info,
            blockstore,
            pool,
            block_producer,
            all2all,
            disseminator,
            cancel_token,
            votor_handle,
        }
    }

    /// Starts the different tasks of the Alpenglow node.
    ///
    /// # Errors
    ///
    /// Returns an error only if any of the tasks panics.
    #[fastrace::trace(short_name = true)]
    pub async fn run(self) -> Result<()> {
        // clean load after startup
        {
            let pool_guard = self.pool.read().await;
            let highest_finalized = pool_guard.finalized_slot();
            drop(pool_guard);
            
            let mut blockstore_guard = self.blockstore.write().await;
            blockstore_guard.clean_beyond_finalized(highest_finalized);
            drop(blockstore_guard);
        }

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

    /// Handles the leader side of the consensus protocol.
    ///
    /// Once all previous blocks have been notarized or skipped and the next
    /// slot belongs to our leader window, we will produce a block.
    async fn block_production_loop(&self) -> Result<()> {
        let mut parent: Slot = 0;
        let mut parent_hash = Hash::default();
        let mut parent_ready = true;

        'outer: for window in 0.. {
            if self.cancel_token.is_cancelled() {
                break;
            }

            let first_slot_in_window = window * SLOTS_PER_WINDOW;
            let last_slot_in_window = (window + 1) * SLOTS_PER_WINDOW - 1;

            // don't do anything if we are not the leader
            let leader = self.epoch_info.leader(first_slot_in_window);
            if leader.id != self.epoch_info.own_id {
                continue;
            }

            if self.pool.read().await.finalized_slot() >= last_slot_in_window {
                warn!(
                    "ignoring window {}..{} for block production",
                    first_slot_in_window, last_slot_in_window
                );
                continue;
            }

            // wait for potential parent of first slot (except if first window)
            if window > 0 {
                // PERF: maybe replace busy loop with events
                (parent, parent_hash, parent_ready) = loop {
                    // build on ready parent, if any
                    let pool = self.pool.read().await;
                    if let Some((slot, hash)) = pool.parents_ready(first_slot_in_window).first() {
                        let h = &hex::encode(hash)[..8];
                        debug!("building block on ready parent {h} in slot {slot}");
                        break (*slot, *hash, true);
                    }
                    drop(pool);

                    // optimisitically build on block in previous slot, if any
                    let blockstore = self.blockstore.read().await;
                    if let Some(hash) = blockstore.canonical_block_hash(first_slot_in_window - 1) {
                        let slot = first_slot_in_window - 1;
                        let h = &hex::encode(hash)[..8];
                        debug!("optimistically building block on parent {h} in slot {slot}",);
                        break (slot, hash, false);
                    }
                    drop(blockstore);

                    if self.pool.read().await.finalized_slot() >= last_slot_in_window {
                        warn!(
                            "ignoring window {}..{} for block production",
                            first_slot_in_window, last_slot_in_window
                        );
                        continue 'outer;
                    }

                    sleep(Duration::from_millis(1)).await;
                };
            }

            // produce blocks for all slots in window
            let mut block = parent;
            let mut block_hash = parent_hash;
            for slot in first_slot_in_window..=last_slot_in_window {
                self.produce_block(slot, (block, block_hash), parent_ready)
                    .await?;
                parent_ready = true;

                // build off own block next
                let blockstore = self.blockstore.read().await;
                block = slot;
                block_hash = blockstore.canonical_block_hash(slot).unwrap();
            }
        }

        Ok(())
    }

    async fn produce_block(
        &self,
        slot: Slot,
        parent: (Slot, Hash),
        parent_ready: bool,
    ) -> Result<()> {
        let (parent_slot, parent_hash) = parent;
        let _slot_span = Span::enter_with_local_parent(format!("slot {slot}"));
        let mut rng = SmallRng::seed_from_u64(slot);
        let ph = &hex::encode(parent_hash)[..8];
        info!("producing block in slot {slot} with parent {ph} in slot {parent_slot}",);

        for slice_index in 0..1 {
            let start_time = Instant::now();
            let min_len = 48;
            let padded_len = ((min_len + shredder::DATA_SHREDS - 1) / shredder::DATA_SHREDS) * shredder::DATA_SHREDS;
            let mut data = vec![0u8; padded_len]; // pad to next multiple of DATA_SHREDS
            // pack parent information in first slice
            if slice_index == 0 {
                data[0..8].copy_from_slice(&parent_slot.to_be_bytes());
                data[8..40].copy_from_slice(&parent_hash);
            }
            let slice = Slice {
                slot,
                slice_index,
                is_last: slice_index == 0,
                merkle_root: None,
                data,
            };

            // shred and disseminate slice
            let shreds = RegularShredder::shred(&slice, &self.secret_key).unwrap();
            for s in shreds {
                self.disseminator.send(&s).await?;
                // PERF: move expensive add_shred() call out of block production
                let mut blockstore = self.blockstore.write().await;
                let block = blockstore.add_shred(s, true).await;
                if let Some((slot, block_info)) = block {
                    let mut pool = self.pool.write().await;
                    pool.add_block(slot, block_info).await;
                }
            }

            // switch parent if necessary (for optimistic handover)
            if !parent_ready {
                let pool = self.pool.read().await;
                if let Some(p) = pool.parents_ready(slot).first() {
                    if *p != parent {
                        warn!("switching block production parent");
                        unimplemented!("have to switch parents");
                    }
                }
            }

            // artificially ensure block time close to target (400ms in good conditions)
            sleep(TARGET_BLOCK_TIME.saturating_sub(start_time.elapsed())).await;
        }

        Ok(())
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
}
