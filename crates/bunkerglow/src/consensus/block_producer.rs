// Copyright (c) Anza Technology, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Block production, leader-side of the consensus protocol.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use color_eyre::Result;
use either::Either;
use fastrace::Span;
use log::{debug, info, warn};
use static_assertions::const_assert;
use tokio::pin;
use tokio::sync::{RwLock, oneshot, watch};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::consensus::{Blockstore, EpochInfo, Pool};
use crate::crypto::merkle::{BlockHash, GENESIS_BLOCK_HASH, MerkleRoot};
use crate::crypto::signature;
use crate::network::{Network, TransactionNetwork};
use crate::shredder::{MAX_DATA_PER_SLICE, RegularShredder, Shredder};
use crate::types::{Slice, SliceHeader, SliceIndex, SlicePayload, Slot};
use crate::{BlockId, BlockPayload, Disseminator, MAX_TRANSACTION_SIZE, Transaction};

/// Pad each produced slice up to this many payload bytes with dummy
/// transactions so blocks keep the HF link busy (`BUNKER_BLOAT_BYTES`,
/// default `0` = disabled). Bloat txs are normal `Transaction`s on the wire,
/// so this is not a wire-format change; values above the slice capacity just
/// fill a single slice.
fn bloat_target_bytes() -> usize {
    std::env::var("BUNKER_BLOAT_BYTES")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(0)
}

/// Leader side of the consensus protocol: packs client transactions into
/// blocks on the protocol's timeouts, then shreds and disseminates them.
pub(super) struct BlockProducer<D: Disseminator, T: Network> {
    /// Own validator's secret key (used e.g. for block production).
    /// This is not the same as the voting secret key, which is held by [`super::Votor`].
    secret_key: signature::SecretKey,
    /// Other validators' info.
    epoch_info: Arc<EpochInfo>,

    /// Blockstore for storing raw block data.
    blockstore: Arc<RwLock<Box<dyn Blockstore + Send + Sync>>>,
    /// Pool of votes and certificates.
    pool: Arc<RwLock<Box<dyn Pool + Send + Sync>>>,

    /// Block dissemination network protocol for shreds.
    disseminator: Arc<D>,
    /// Network connection to receive transactions from clients.
    txs_receiver: T,

    /// Indicates whether the node is shutting down.
    cancel_token: CancellationToken,

    /// Should be set to [`super::DELTA_BLOCK`] in production.
    /// Stored as a field to aid in testing.
    delta_block: Duration,
    /// Should be set to [`super::DELTA_FIRST_SLICE`] in production.
    /// Stored as a field to aid in testing.
    delta_first_slice: Duration,

    /// watch channel for receiving epoch info updates at epoch boundaries
    epoch_info_rx: watch::Receiver<Arc<EpochInfo>>,
    /// epoch transition payloads waiting to be embedded in the first block of an epoch
    pending_epoch_transitions: Arc<RwLock<BTreeMap<u64, Vec<u8>>>>,
}

impl<D, T> BlockProducer<D, T>
where
    D: Disseminator,
    T: TransactionNetwork,
{
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        secret_key: signature::SecretKey,
        epoch_info: Arc<EpochInfo>,
        disseminator: Arc<D>,
        txs_receiver: T,
        blockstore: Arc<RwLock<Box<dyn Blockstore + Send + Sync>>>,
        pool: Arc<RwLock<Box<dyn Pool + Send + Sync>>>,
        cancel_token: CancellationToken,
        delta_block: Duration,
        delta_first_slice: Duration,
        epoch_info_rx: watch::Receiver<Arc<EpochInfo>>,
        pending_epoch_transitions: Arc<RwLock<BTreeMap<u64, Vec<u8>>>>,
    ) -> Self {
        assert!(delta_block >= delta_first_slice);
        Self {
            secret_key,
            epoch_info,
            blockstore,
            pool,
            disseminator,
            txs_receiver,
            cancel_token,
            delta_block,
            delta_first_slice,
            epoch_info_rx,
            pending_epoch_transitions,
        }
    }

    /// Handles the leader side of the consensus protocol.
    ///
    /// Once all previous blocks have been notarized or skipped and the next
    /// slot belongs to our leader window, we will produce a block.
    pub(super) async fn block_production_loop(&self) -> Result<()> {
        let mut current_epoch = self.epoch_info.epoch();
        let mut epoch_info_rx = self.epoch_info_rx.clone();

        for first_slot_in_window in Slot::windows() {
            if self.cancel_token.is_cancelled() {
                break;
            }

            // pause at epoch boundaries and wait for the new epoch info
            let slot_epoch = first_slot_in_window.epoch();
            if slot_epoch > current_epoch {
                info!(
                    "[val {}] waiting for epoch {} transition before producing window {}",
                    self.epoch_info.own_id, slot_epoch, first_slot_in_window
                );
                while epoch_info_rx.borrow().epoch() < slot_epoch {
                    if epoch_info_rx.changed().await.is_err() {
                        return Ok(());
                    }
                }
                current_epoch = slot_epoch;
                info!(
                    "[val {}] epoch {} ready, resuming block production",
                    self.epoch_info.own_id, current_epoch
                );
            }

            let last_slot_in_window = first_slot_in_window.last_slot_in_window();

            // After a restart this loop starts at genesis while the pool floor
            // is ahead; without this skip the leader re-produces every
            // historical window before emitting anything new.
            let finalized = self.pool.read().await.finalized_slot();
            if last_slot_in_window <= finalized {
                debug!(
                    "[val {}] not producing in window {first_slot_in_window}..{last_slot_in_window}, already finalized up to {finalized}",
                    self.epoch_info.own_id
                );
                continue;
            }

            let leader = self.epoch_info.leader(first_slot_in_window);
            if leader.id != self.epoch_info.own_id {
                debug!(
                    "[val {}] not producing in window {first_slot_in_window}..{last_slot_in_window}, not leader",
                    self.epoch_info.own_id
                );
                continue;
            }

            let slot_ready = wait_for_first_slot(
                self.pool.clone(),
                self.blockstore.clone(),
                first_slot_in_window,
            )
            .await;

            let start = Instant::now();
            let mut block_id = match slot_ready {
                SlotReady::Skip => {
                    warn!(
                        "not producing in window {first_slot_in_window}..{last_slot_in_window}, saw later finalization"
                    );
                    continue;
                }
                SlotReady::Ready(parent) => {
                    if first_slot_in_window.is_genesis() {
                        // genesis block is already produced so skip it
                        (first_slot_in_window, GENESIS_BLOCK_HASH)
                    } else {
                        self.produce_block_parent_ready(first_slot_in_window, parent)
                            .await?
                    }
                }
                SlotReady::ParentReadyNotSeen(parent, channel) => {
                    self.produce_block_parent_not_ready(first_slot_in_window, parent, channel)
                        .await?
                }
            };
            debug!(
                "produced block {} in {} ms",
                first_slot_in_window,
                start.elapsed().as_millis()
            );

            for slot in first_slot_in_window.slots_in_window().skip(1) {
                let slot_epoch = slot.epoch();
                if slot_epoch > current_epoch {
                    info!(
                        "[val {}] waiting for epoch {} transition before producing slot {}",
                        self.epoch_info.own_id, slot_epoch, slot
                    );
                    while epoch_info_rx.borrow().epoch() < slot_epoch {
                        if epoch_info_rx.changed().await.is_err() {
                            return Ok(());
                        }
                    }
                    current_epoch = slot_epoch;
                    info!(
                        "[val {}] epoch {} ready, resuming block production",
                        self.epoch_info.own_id, current_epoch
                    );
                }

                let start = Instant::now();
                block_id = self.produce_block_parent_ready(slot, block_id).await?;
                debug!(
                    "produced block {} in {} ms",
                    slot,
                    start.elapsed().as_millis()
                );
            }
        }

        Ok(())
    }

    /// Produces a block before the `ParentReady` event is seen;
    /// `parent_block_id` is the previous slot's block and may end up not being
    /// the actual parent.
    pub(super) async fn produce_block_parent_not_ready(
        &self,
        slot: Slot,
        parent_block_id: BlockId,
        mut parent_ready_receiver: oneshot::Receiver<BlockId>,
    ) -> Result<BlockId> {
        let _slot_span = Span::enter_with_local_parent(format!("slot {slot}"));
        let (parent_slot, parent_hash) = &parent_block_id;
        assert_eq!(*parent_slot, slot.prev());
        assert!(slot.is_start_of_window());
        info!(
            "optimistically producing block in slot {} with parent {} in slot {}",
            slot,
            &hex::encode(parent_hash.as_hash())[..8],
            *parent_slot,
        );

        // only start the DELTA_BLOCK timer once the ParentReady event is seen
        let mut duration_left = Duration::MAX;
        for slice_index in SliceIndex::all() {
            let parent = if slice_index.is_first() {
                Some(parent_block_id.clone())
            } else {
                None
            };

            let time_for_slice = if slice_index.is_first() {
                // make sure first slice is produced on time
                // TODO: this can be made more accurate, only needed if production of first slice
                // still takes more than delta_first_slice after we saw ParentReady, not if:
                // 1. first slice is produced before ParentReady is seen, OR
                // 2. first slice finishes at most delta_first_slice after ParentReady is seen
                duration_left.min(self.delta_first_slice)
            } else {
                // Cap per-slice time so optimistic production yields before the timeout.
                duration_left.min(self.delta_block)
            };
            let produce_slice_future =
                produce_slice_payload(slot, &self.txs_receiver, parent, time_for_slice, None);

            // Await ParentReady concurrently while producing the next slice.
            let (mut payload, new_duration_left, terminal_empty) = if parent_ready_receiver
                .is_terminated()
            {
                produce_slice_future.await
            } else {
                pin!(produce_slice_future);
                tokio::select! {
                    res = &mut produce_slice_future => {
                        let (payload, _new_duration_left, terminal_empty) = res;
                        // ParentReady event still not seen, do not start DELTA_BLOCK timer yet
                        (payload, Duration::MAX, terminal_empty)
                    }
                    res = &mut parent_ready_receiver => {
                        // ParentReady arrived mid-slice; a no-op if the parent is unchanged.

                        let start = Instant::now();
                        let (new_slot, new_hash) = res.unwrap();
                        let (mut payload, _maybe_duration, terminal_empty) = produce_slice_future.await;
                        if new_hash == *parent_hash {
                            debug!("parent is ready, continuing with same parent");
                        } else {
                            assert_ne!(new_slot, *parent_slot);
                            debug!(
                                "changed parent from {} in slot {} to {} in slot {}",
                                &hex::encode(parent_hash.as_hash())[..8],
                                parent_slot,
                                &hex::encode(new_hash.as_hash())[..8],
                                new_slot
                            );
                            payload.parent = Some((new_slot, new_hash));
                        }
                        // Start the DELTA_BLOCK timer, net of time already spent on the slice.
                        debug!("starting blocktime timer");
                        let duration = self.delta_block.saturating_sub(start.elapsed());
                        (payload, duration, terminal_empty)
                  }
                }
            };

            // An empty block is a single slice (see `produce_slice_payload`).
            let is_last = slice_index.is_max() || terminal_empty || new_duration_left.is_zero();
            if is_last && !parent_ready_receiver.is_terminated() {
                let (new_slot, new_hash) = (&mut parent_ready_receiver).await.unwrap();
                if new_hash != *parent_hash {
                    assert_ne!(new_slot, *parent_slot);
                    debug!(
                        "changed parent from {} in slot {} to {} in slot {}",
                        &hex::encode(parent_hash.as_hash())[..8],
                        parent_slot,
                        &hex::encode(new_hash.as_hash())[..8],
                        new_slot
                    );
                    payload.parent = Some((new_slot, new_hash));
                } else {
                    debug!("parent is ready, continuing with same parent");
                }
            }
            let header = SliceHeader {
                slot,
                slice_index,
                is_last,
            };

            match self.shred_and_disseminate(header, payload).await? {
                Some(block_hash) => return Ok((slot, block_hash)),
                None => {
                    assert!(!new_duration_left.is_zero());
                    duration_left = new_duration_left;
                }
            }
        }
        unreachable!()
    }

    /// Produces a block in the situation where we have already seen the `ParentReady` event.
    ///
    /// The `parent_block_id` refers to the block that is the ready parent.
    pub(crate) async fn produce_block_parent_ready(
        &self,
        slot: Slot,
        parent_block_id: BlockId,
    ) -> Result<BlockId> {
        let _slot_span = Span::enter_with_local_parent(format!("slot {slot}"));
        let (parent_slot, parent_hash) = &parent_block_id;
        info!(
            "producing block in slot {} with ready parent {} in slot {}",
            slot,
            &hex::encode(parent_hash.as_hash())[..8],
            parent_slot,
        );

        let mut duration_left = self.delta_block;
        for slice_index in SliceIndex::all() {
            let (payload, new_duration_left, terminal_empty) = if slice_index.is_first() {
                // make sure first slice is produced quickly enough so that other nodes do not generate the [`TimeoutCrashedLeader`] event
                let time_for_slice = self.delta_first_slice;
                let epoch_transition = self.epoch_transition_payload(slot).await;
                let (payload, slice_duration_left, terminal_empty) = produce_slice_payload(
                    slot,
                    &self.txs_receiver,
                    Some(parent_block_id.clone()),
                    time_for_slice,
                    epoch_transition,
                )
                .await;
                let elapsed = self.delta_first_slice - slice_duration_left;
                let left = duration_left.saturating_sub(elapsed);

                (payload, left, terminal_empty)
            } else {
                produce_slice_payload(slot, &self.txs_receiver, None, duration_left, None).await
            };
            // An empty block is a SINGLE slice: if the slice was produced empty via
            // the grace timeout, mark it last so we don't emit a phantom second
            // empty slice (which would double the shreds on the wire).
            let is_last = slice_index.is_max() || terminal_empty || new_duration_left.is_zero();
            let header = SliceHeader {
                slot,
                slice_index,
                is_last,
            };

            if let Some(block_hash) = self.shred_and_disseminate(header, payload).await? {
                return Ok((slot, block_hash));
            } else {
                assert!(!new_duration_left.is_zero());
                duration_left = new_duration_left;
            }
        }
        unreachable!()
    }

    /// Shreds and disseminates the slice payload.
    ///
    /// Returns Ok(Some(hash of the block)) if this is the last slice.
    /// Returns Ok(None) otherwise.
    async fn shred_and_disseminate(
        &self,
        header: SliceHeader,
        payload: SlicePayload,
    ) -> Result<Option<BlockHash>> {
        let slot = header.slot;
        let is_last = header.is_last;
        let slice = Slice::from_parts(header, payload, None);
        let mut maybe_block_hash = None;
        // PERF: new shredder every time!
        let shreds = RegularShredder::default()
            .shred(slice, &self.secret_key)
            .expect("shredding of valid slice should never fail");
        for s in shreds {
            self.disseminator.send(&s).await?;
            // PERF: move expensive add_shred() call out of block production
            let block = self
                .blockstore
                .write()
                .await
                .add_shred_from_disseminator(s.into_shred())
                .await;
            if let Ok(Some(block_info)) = block {
                assert!(maybe_block_hash.is_none());
                maybe_block_hash = Some(block_info.hash.clone());
                let block_id = (slot, block_info.hash.clone());
                self.pool
                    .write()
                    .await
                    .add_block(block_id, block_info.parent)
                    .await;
            }
        }
        if is_last {
            Ok(Some(maybe_block_hash.unwrap()))
        } else {
            assert!(maybe_block_hash.is_none());
            Ok(None)
        }
    }

    async fn epoch_transition_payload(&self, slot: Slot) -> Option<Vec<u8>> {
        if !slot.is_first_in_epoch() || slot.is_genesis() {
            return None;
        }
        self.pending_epoch_transitions
            .write()
            .await
            .remove(&slot.epoch())
    }
}

/// Produces one slice's payload, returning `(payload, duration_left, terminal)`.
///
/// `terminal` is `true` when the slice came out EMPTY via the idle-mempool
/// grace, making it the block's last; without it the caller's elapsed math
/// would spawn a phantom second empty slice, doubling the shreds on the wire.
async fn produce_slice_payload<T>(
    slot: Slot,
    txs_receiver: &T,
    parent: Option<BlockId>,
    duration_left: Duration,
    epoch_transition: Option<Vec<u8>>,
) -> (SlicePayload, Duration, bool)
where
    T: TransactionNetwork,
{
    let start_time = Instant::now();

    // each slice should be able hold at least 1 transaction
    // need 8 bytes to encode number of txs + 8 bytes to encode the length of the tx payload
    const_assert!(MAX_DATA_PER_SLICE >= MAX_TRANSACTION_SIZE + 8 + 8);

    // reserve space for the slot, parent, and block payload overhead
    let slot_encoded_len = <Slot as wincode::SchemaWrite>::size_of(&slot).unwrap();
    let parent_encoded_len = <Option<BlockId> as wincode::SchemaWrite>::size_of(&parent).unwrap();
    let fixed_payload_len = <BlockPayload as wincode::SchemaWrite>::size_of(&BlockPayload {
        epoch_transition: epoch_transition.clone(),
        transactions: Vec::new(),
    })
    .unwrap_or(8);
    let mut slice_capacity_left = MAX_DATA_PER_SLICE
        .checked_sub(slot_encoded_len + parent_encoded_len + fixed_payload_len)
        .unwrap();
    let initial_capacity = slice_capacity_left;
    let mut txs = Vec::new();

    // `(duration_left, terminal_empty)` — see the function docs for `terminal`.
    let (ret, terminal_empty) = loop {
        // With an empty mempool wait only a short grace before emitting an
        // EMPTY slice; sleeping the whole delta-scaled window would stall an
        // idle leader for minutes at high BUNKER_DELTA_MULT.
        let empty_so_far = txs.is_empty();
        let max_wait = if empty_so_far {
            duration_left.min(super::delta_empty_slice())
        } else {
            // With ≥1 tx packed, a short per-tx grace closes the slice on a
            // lull instead of holding it open for the whole block window.
            duration_left.min(start_time.elapsed() + super::delta_pack_grace())
        };
        let sleep_duration = max_wait.saturating_sub(start_time.elapsed());
        let res = tokio::select! {
            () = tokio::time::sleep(sleep_duration) => {
                // Still empty on timeout = terminal single-slice empty block;
                // otherwise a normal flush.
                break (Duration::ZERO, empty_so_far);
            }
            res = txs_receiver.receive() => {
                res
            }
        };
        let tx = res.expect("receiving tx");
        let tx_len = wincode::serialize(&tx)
            .expect("serialization should not panic")
            .len();
        slice_capacity_left = slice_capacity_left.checked_sub(tx_len).unwrap();
        txs.push(tx);

        // if there is not enough space for another tx, break
        // this needs to account for the 8 bytes to encode the length of the tx payload
        if slice_capacity_left < MAX_TRANSACTION_SIZE + 8 {
            break (duration_left.saturating_sub(start_time.elapsed()), false);
        }
    };

    // Bloat padding (`BUNKER_BLOAT_BYTES`): random bytes so the modem's PMC
    // compression cannot shrink it; the padded slice keeps its
    // `terminal_empty` flag so an idle block stays single-slice.
    let bloat_target = bloat_target_bytes().min(initial_capacity);
    let mut packed_bytes = initial_capacity - slice_capacity_left;
    if bloat_target > packed_bytes {
        use rand::{RngCore, SeedableRng};
        // Slot-seeded padding: a crashed leader re-producing the slot must
        // emit byte-identical bytes or the re-production forks the slot.
        let mut rng = rand::rngs::StdRng::seed_from_u64(slot.inner());
        while packed_bytes < bloat_target && slice_capacity_left > 8 {
            let chunk = (bloat_target - packed_bytes)
                .min(MAX_TRANSACTION_SIZE)
                .min(slice_capacity_left - 8);
            let mut bytes = vec![0u8; chunk];
            rng.fill_bytes(&mut bytes);
            let tx = Transaction(bytes);
            let tx_len = wincode::serialize(&tx)
                .expect("serialization should not panic")
                .len();
            if tx_len > slice_capacity_left {
                break;
            }
            slice_capacity_left -= tx_len;
            packed_bytes += tx_len;
            txs.push(tx);
        }
    }

    // TODO: not accounting for this potentially expensive operation in duration_left calculation above.
    let txs = wincode::serialize(&BlockPayload {
        epoch_transition,
        transactions: txs,
    })
    .expect("serialization should not panic");
    let payload = SlicePayload::new(slot, parent, txs);
    (payload, ret, terminal_empty)
}

/// Outcome of [`wait_for_first_slot`].
#[derive(Debug)]
enum SlotReady {
    /// Window was already skipped.
    Skip,
    /// Slot is ready and the Pool emitted a `ParentReady` for given `BlockId`.
    Ready(BlockId),
    /// Slot is ready as a block for the previous slot was seen but the Pool has not emitted `ParentReady` yet.
    ParentReadyNotSeen(BlockId, oneshot::Receiver<BlockId>),
}

/// Waits for the window's first slot to become ready for production: either
/// the pool emitted `ParentReady`, or the previous slot's block was stored.
async fn wait_for_first_slot(
    pool: Arc<RwLock<Box<dyn Pool + Send + Sync>>>,
    blockstore: Arc<RwLock<Box<dyn Blockstore + Send + Sync>>>,
    first_slot_in_window: Slot,
) -> SlotReady {
    assert!(first_slot_in_window.is_start_of_window());
    if first_slot_in_window.is_genesis_window() {
        return SlotReady::Ready((Slot::genesis(), GENESIS_BLOCK_HASH));
    }

    // if already have parent ready, return it, otherwise get a channel to await on
    let mut rx = {
        let mut guard = pool.write().await;
        match guard.wait_for_parent_ready(first_slot_in_window) {
            Either::Left(parent) => {
                return SlotReady::Ready(parent);
            }
            Either::Right(rx) => rx,
        }
    };

    // Concurrently wait for:
    // - `ParentReady` event,
    // - block reconstruction in blockstore, OR
    // - notification that a later slot was finalized.
    tokio::select! {
        res = &mut rx => {
            let parent = res.expect("sender dropped channel");
            SlotReady::Ready(parent)
        }

        res = async {
            let handle = tokio::spawn(async move {
                // PERF: These are burning a CPU. Can we use async here?
                loop {
                    let last_slot_in_prev_window = first_slot_in_window.prev();
                    if let Some(hash) = blockstore.read().await
                        .disseminated_block_hash(last_slot_in_prev_window)
                    {
                        return Some((last_slot_in_prev_window, hash.clone()));
                    }
                    if pool.read().await.finalized_slot() >= first_slot_in_window {
                        return None;
                    }
                    sleep(Duration::from_millis(1)).await;
                }
            });
            handle.await.expect("error in task")
        } => {
            match res {
                None => SlotReady::Skip,
                Some((slot, hash)) => SlotReady::ParentReadyNotSeen((slot, hash.clone()), rx),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mockall::{Sequence, predicate};

    use super::*;
    use crate::Transaction;
    use crate::consensus::BlockInfo;
    use crate::consensus::blockstore::MockBlockstore;
    use crate::consensus::pool::MockPool;
    use crate::crypto::Hash;
    use crate::disseminator::MockDisseminator;
    use crate::network::{UdpNetwork, localhost_ip_sockaddr};
    use crate::shredder::TOTAL_SHREDS;
    use crate::test_utils::generate_validators;

    #[tokio::test]
    async fn produce_slice_empty_slices() {
        let txs_receiver: UdpNetwork<Transaction, Transaction> = UdpNetwork::new_with_any_port();
        let duration_left = Duration::from_micros(0);

        let parent = None;
        let (payload, maybe_duration, terminal_empty) = produce_slice_payload(
            Slot::new(1),
            &txs_receiver,
            parent.clone(),
            duration_left,
            None,
        )
        .await;
        assert_eq!(maybe_duration, Duration::ZERO);
        // Empty slice produced via the grace timeout is terminal (single-slice block).
        assert!(terminal_empty, "empty slice must be marked terminal");
        assert_eq!(payload.parent, parent);
        let block_payload: BlockPayload = wincode::deserialize(&payload.data).unwrap();
        assert!(block_payload.epoch_transition.is_none());
        assert!(block_payload.transactions.is_empty());

        let parent = Some((Slot::genesis(), GENESIS_BLOCK_HASH));
        let (payload, maybe_duration, terminal_empty) = produce_slice_payload(
            Slot::new(1),
            &txs_receiver,
            parent.clone(),
            duration_left,
            None,
        )
        .await;
        assert_eq!(maybe_duration, Duration::ZERO);
        assert!(terminal_empty, "empty slice must be marked terminal");
        assert_eq!(payload.parent, parent);
        let block_payload: BlockPayload = wincode::deserialize(&payload.data).unwrap();
        assert!(block_payload.epoch_transition.is_none());
        assert!(block_payload.transactions.is_empty());
    }

    #[tokio::test]
    async fn produce_slice_full_slices() {
        let txs_receiver: UdpNetwork<Transaction, Transaction> = UdpNetwork::new_with_any_port();
        let addr = localhost_ip_sockaddr(txs_receiver.port());
        let txs_sender: UdpNetwork<Transaction, Transaction> = UdpNetwork::new_with_any_port();
        // long enough duration so hopefully doesn't fire while collecting txs
        let duration_left = Duration::from_secs(100);

        tokio::spawn(async move {
            for i in 0..255 {
                let data = vec![i; MAX_TRANSACTION_SIZE];
                let msg = Transaction(data);
                txs_sender.send(&msg, addr).await.unwrap();
            }
        });

        let parent = None;
        let (payload, maybe_duration, terminal_empty) = produce_slice_payload(
            Slot::new(1),
            &txs_receiver,
            parent.clone(),
            duration_left,
            None,
        )
        .await;
        assert!(maybe_duration > Duration::ZERO);
        // A full slice (txs packed) is NOT terminal-empty.
        assert!(
            !terminal_empty,
            "full slice must not be marked terminal-empty"
        );
        assert_eq!(payload.parent, parent);
        assert!(payload.data.len() <= MAX_DATA_PER_SLICE);
        assert!(payload.data.len() > MAX_DATA_PER_SLICE - MAX_TRANSACTION_SIZE);
    }

    #[tokio::test]
    async fn wait_for_first_slot_genesis() {
        let pool: Box<dyn Pool + Send + Sync> = Box::new(MockPool::new());
        let pool = Arc::new(RwLock::new(pool));
        let blockstore: Box<dyn Blockstore + Send + Sync> = Box::new(MockBlockstore::new());
        let blockstore = Arc::new(RwLock::new(blockstore));

        let status = wait_for_first_slot(pool, blockstore, Slot::genesis()).await;
        assert!(matches!(status, SlotReady::Ready(_)));
    }

    #[tokio::test]
    async fn wait_for_first_slot_parent_already_ready() {
        let blockstore: Box<dyn Blockstore + Send + Sync> = Box::new(MockBlockstore::new());
        let blockstore = Arc::new(RwLock::new(blockstore));

        let slot = Slot::windows().nth(10).unwrap();
        let parent = (slot.prev(), GENESIS_BLOCK_HASH);

        let mut pool = MockPool::new();
        let p = parent.clone();
        pool.expect_wait_for_parent_ready()
            .with(predicate::eq(slot))
            .return_once(move |_slot| Either::Left(p));
        let pool: Box<dyn Pool + Send + Sync> = Box::new(pool);
        let pool = Arc::new(RwLock::new(pool));

        let status = wait_for_first_slot(pool, blockstore, slot).await;
        match status {
            SlotReady::Ready(p) => assert_eq!(p, parent),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn wait_for_first_slot_parent_ready_later() {
        let blockstore: Box<dyn Blockstore + Send + Sync> = Box::new(MockBlockstore::new());
        let blockstore = Arc::new(RwLock::new(blockstore));

        let slot = Slot::windows().nth(10).unwrap();
        let parent = (slot.prev(), GENESIS_BLOCK_HASH);
        let (tx, rx) = oneshot::channel();
        tx.send(parent.clone()).unwrap();

        let mut pool = MockPool::new();
        pool.expect_wait_for_parent_ready()
            .with(predicate::eq(slot))
            .return_once(move |_slot| Either::Right(rx));
        let pool: Box<dyn Pool + Send + Sync> = Box::new(pool);
        let pool = Arc::new(RwLock::new(pool));

        let status = wait_for_first_slot(pool, blockstore, slot).await;
        match status {
            SlotReady::Ready(p) => assert_eq!(p, parent),
            other => panic!("unexpected {other:?}"),
        }
    }

    /// A bunch of boilerplate to initialize and return a [`BlockProducer`].
    fn setup(
        blockstore: MockBlockstore,
        pool: MockPool,
        disseminator: MockDisseminator,
        delta_block: Duration,
        delta_first_slice: Duration,
    ) -> BlockProducer<MockDisseminator, UdpNetwork<Transaction, Transaction>> {
        let secret_key = signature::SecretKey::new(&mut rand::rng());
        let (_, epoch_info) = generate_validators(11);
        let blockstore: Box<dyn Blockstore + Send + Sync> = Box::new(blockstore);
        let blockstore = Arc::new(RwLock::new(blockstore));
        let pool: Box<dyn Pool + Send + Sync> = Box::new(pool);
        let pool = Arc::new(RwLock::new(pool));
        let disseminator = Arc::new(disseminator);
        let txs_receiver = UdpNetwork::new_with_any_port();
        let cancel_token = CancellationToken::new();
        let (_epoch_info_tx, epoch_info_rx) = watch::channel(epoch_info.clone());

        BlockProducer::new(
            secret_key,
            epoch_info,
            disseminator,
            txs_receiver,
            blockstore,
            pool,
            cancel_token,
            delta_block,
            delta_first_slice,
            epoch_info_rx,
            Arc::new(RwLock::new(BTreeMap::new())),
        )
    }

    #[tokio::test]
    async fn verify_produce_block_parent_ready() {
        let slot = Slot::windows().nth(10).unwrap();
        let hash: BlockHash = Hash::random_for_test().into();
        let hash_prev: BlockHash = Hash::random_for_test().into();
        let block_info = BlockInfo {
            hash: hash.clone(),
            parent: (slot.prev(), hash_prev.clone()),
        };

        // Handles TOTAL_SHRED number of calls.
        // The first TOTAL_SHRED - 1 calls return None.
        // The last call returns Some.
        let mut seq = Sequence::new();
        let mut blockstore = MockBlockstore::new();
        blockstore
            .expect_add_shred_from_disseminator()
            .times(TOTAL_SHREDS - 1)
            .in_sequence(&mut seq)
            .returning(move |_| Box::pin(async move { Ok(None) }));
        let bi = block_info.clone();
        blockstore
            .expect_add_shred_from_disseminator()
            .times(1)
            .in_sequence(&mut seq)
            .returning(move |_| {
                let bi = bi.clone();
                Box::pin(async move { Ok(Some(bi)) })
            });

        let mut pool = MockPool::new();
        let bi = block_info.clone();
        pool.expect_add_block()
            .returning(move |ret_block_id, ret_parent_block_id| {
                assert_eq!(ret_block_id, (slot, bi.hash.clone()));
                assert_eq!(bi.parent, ret_parent_block_id);
                Box::pin(async {})
            });

        let mut disseminator = MockDisseminator::new();
        disseminator
            .expect_send()
            .returning(|_| Box::pin(async { Ok(()) }));
        let block_producer = setup(
            blockstore,
            pool,
            disseminator,
            Duration::from_micros(0),
            Duration::from_micros(0),
        );

        let ret = block_producer
            .produce_block_parent_ready(slot, block_info.parent)
            .await
            .unwrap();
        assert_eq!(slot, ret.0);
        assert_eq!(block_info.hash, ret.1);
    }

    #[tokio::test]
    async fn verify_produce_block_parent_not_ready() {
        // With an idle mempool the first slice is produced empty and terminal
        // (an empty block is a SINGLE slice), so the producer awaits the
        // ParentReady event before disseminating that one slice. The block must
        // adopt the parent delivered by ParentReady.
        let slot = Slot::windows().nth(10).unwrap();
        let slot_hash: BlockHash = Hash::random_for_test().into();
        let old_parent = (slot.prev(), Hash::random_for_test().into());
        let new_parent = (slot.prev().prev(), Hash::random_for_test().into());
        let old_block_info = BlockInfo {
            hash: slot_hash.clone(),
            parent: old_parent,
        };
        let new_block_info = BlockInfo {
            hash: slot_hash,
            parent: new_parent.clone(),
        };

        let mut seq = Sequence::new();
        let mut blockstore = MockBlockstore::new();
        blockstore
            .expect_add_shred_from_disseminator()
            .times(TOTAL_SHREDS - 1)
            .in_sequence(&mut seq)
            .returning(move |_| Box::pin(async move { Ok(None) }));
        let nbi = new_block_info.clone();
        blockstore
            .expect_add_shred_from_disseminator()
            .times(1)
            .in_sequence(&mut seq)
            .returning(move |_| {
                let nbi = nbi.clone();
                Box::pin(async {
                    // final shred: block is constructed with the new parent
                    Ok(Some(nbi))
                })
            });

        let mut pool = MockPool::new();
        let nbi = new_block_info.clone();
        pool.expect_add_block()
            .returning(move |ret_block_id, ret_parent_block_id| {
                assert_eq!(ret_block_id, (slot, nbi.hash.clone()));
                assert_eq!(nbi.parent, ret_parent_block_id);
                Box::pin(async {})
            });

        let mut disseminator = MockDisseminator::new();
        disseminator
            .expect_send()
            .returning(|_| Box::pin(async { Ok(()) }));
        let block_producer = setup(
            blockstore,
            pool,
            disseminator,
            Duration::from_micros(0),
            Duration::from_millis(0),
        );

        let (parent_ready_tx, parent_ready_rx) = oneshot::channel();
        parent_ready_tx.send(new_parent.clone()).unwrap();

        let ret = block_producer
            .produce_block_parent_not_ready(slot, old_block_info.parent, parent_ready_rx)
            .await
            .unwrap();

        assert_eq!(slot, ret.0);
        assert_eq!(new_block_info.hash, ret.1);
        assert_eq!(new_block_info.parent, new_parent);
    }
}
