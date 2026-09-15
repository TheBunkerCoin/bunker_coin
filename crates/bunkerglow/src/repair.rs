// Copyright (c) Anza Technology, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Double-Merkle block repair protocol with proof-verified responses.

use std::collections::{BTreeMap, BinaryHeap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use log::{debug, info, trace, warn};
use tokio::sync::RwLock;
use wincode::{SchemaRead, SchemaWrite};

use crate::consensus::{Blockstore, EpochInfo, Pool, delta_block};
use crate::crypto::merkle::{DoubleMerkleProof, DoubleMerkleTree, MerkleRoot, SliceRoot};
use crate::crypto::{Hash, hash};
use crate::disseminator::rotor::{SamplingStrategy, StakeWeightedSampler};
use crate::network::{Network, RepairNetwork, RepairRequestNetwork};
use crate::shredder::{Shred, ShredIndex};
use crate::types::SliceIndex;
use crate::{BlockId, ValidatorId};

/// Doubling cap for repair retries: on a slow link a flat timeout re-requests
/// in-flight shreds before their responses can land.
const MAX_REPAIR_BACKOFF_EXP: u32 = 3;

/// Retry delay after `retries` unanswered attempts.
fn retry_delay(retries: u32) -> Duration {
    repair_timeout() * 2u32.pow(retries.min(MAX_REPAIR_BACKOFF_EXP))
}

/// Repair response timeout before retrying; responses are bulk and queue
/// behind the peer's stream, so this must cover a block's dissemination budget.
fn repair_timeout() -> Duration {
    delta_block()
}

/// Wait before the first request for a block we only know from votes. The
/// leader's notar vote always outruns its shreds on the radio (votes take the
/// priority lane), so a freshly "missing" block is almost always in flight;
/// repairing it would fetch a duplicate over the same link.
fn repair_start_grace() -> Duration {
    delta_block()
}

/// Extra grace periods granted while the leader's stream for that window is
/// still arriving: a later block in the window waits behind its siblings.
const MAX_START_EXTENSIONS: u32 = 3;

/// Repair request kind.
#[derive(Clone, Debug, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub enum RepairRequestType {
    /// Request the last slice root for a block.
    LastSliceRoot(BlockId),
    /// Request a slice root.
    SliceRoot(BlockId, SliceIndex),
    /// Request a shred.
    Shred(BlockId, SliceIndex, ShredIndex),
}

impl RepairRequestType {
    fn block_id(&self) -> &BlockId {
        match self {
            Self::LastSliceRoot(id) | Self::SliceRoot(id, _) | Self::Shred(id, _, _) => id,
        }
    }

    /// Hashes this request type for retry tracking.
    fn hash(&self) -> Hash {
        let repair = RepairRequest {
            req_type: self.clone(),
            sender: 0,
        };
        let msg_bytes = wincode::serialize(&repair).unwrap();
        hash(&msg_bytes)
    }
}

/// Repair request message.
#[derive(Clone, Debug, SchemaRead, SchemaWrite)]
pub struct RepairRequest {
    sender: ValidatorId,
    req_type: RepairRequestType,
}

/// Repair response message; each variant echoes its request type.
#[derive(Clone, Debug, SchemaRead, SchemaWrite)]
pub enum RepairResponse {
    /// Last slice root plus proof.
    LastSliceRoot(RepairRequestType, SliceIndex, SliceRoot, DoubleMerkleProof),
    /// Slice root plus proof.
    SliceRoot(RepairRequestType, SliceRoot, DoubleMerkleProof),
    /// Shred response.
    Shred(RepairRequestType, Shred),
}

impl RepairResponse {
    /// Returns the echoed request type.
    #[must_use]
    const fn request_type(&self) -> &RepairRequestType {
        match self {
            Self::LastSliceRoot(req_type, _, _, _)
            | Self::SliceRoot(req_type, _, _)
            | Self::Shred(req_type, _) => req_type,
        }
    }
}

/// Handles peer repair requests separately from local repair response handling.
pub struct RepairRequestHandler<N: Network> {
    epoch_info: Arc<EpochInfo>,
    blockstore: Arc<RwLock<Box<dyn Blockstore + Send + Sync>>>,
    network: N,
}

impl<N> RepairRequestHandler<N>
where
    N: RepairRequestNetwork,
{
    /// Creates a repair request handler.
    pub fn new(
        epoch_info: Arc<EpochInfo>,
        blockstore: Arc<RwLock<Box<dyn Blockstore + Send + Sync>>>,
        network: N,
    ) -> Self {
        Self {
            epoch_info,
            blockstore,
            network,
        }
    }

    /// Receives repair requests and replies from blockstore data.
    pub async fn run(&self) {
        loop {
            let request = self.network.receive().await.unwrap();
            self.answer_request(request).await.unwrap();
        }
    }

    /// Answers a repair request if the blockstore has the requested data.
    async fn answer_request(&self, request: RepairRequest) -> std::io::Result<()> {
        trace!("answering repair request: {request:?}");
        let response = match &request.req_type {
            RepairRequestType::LastSliceRoot(block_id) => {
                let blockstore = self.blockstore.read().await;
                let Some(last_slice) = blockstore.get_last_slice_index(block_id) else {
                    return Ok(());
                };
                let Some(root) = blockstore.get_slice_root(block_id, last_slice) else {
                    return Ok(());
                };
                let Some(proof) = blockstore.create_double_merkle_proof(block_id, last_slice)
                else {
                    return Ok(());
                };
                RepairResponse::LastSliceRoot(request.req_type, last_slice, root.clone(), proof)
            }
            RepairRequestType::SliceRoot(block_id, slice) => {
                let blockstore = self.blockstore.read().await;
                let Some(root) = blockstore.get_slice_root(block_id, *slice) else {
                    return Ok(());
                };
                let Some(proof) = blockstore.create_double_merkle_proof(block_id, *slice) else {
                    return Ok(());
                };
                RepairResponse::SliceRoot(request.req_type, root.clone(), proof)
            }
            RepairRequestType::Shred(block_id, slice, shred) => {
                let blockstore = self.blockstore.read().await;
                let Some(shred) = blockstore.get_shred(block_id, *slice, *shred) else {
                    return Ok(());
                };
                RepairResponse::Shred(request.req_type, shred.into_shred())
            }
        };
        self.send_response(response, request.sender).await
    }

    async fn send_response(
        &self,
        response: RepairResponse,
        validator: ValidatorId,
    ) -> std::io::Result<()> {
        let to = self.epoch_info.validator(validator).repair_response_address;
        self.network.send(&response, to).await
    }
}

/// Repairs missing blocks using double-Merkle proofs.
pub struct Repair<N: Network> {
    blockstore: Arc<RwLock<Box<dyn Blockstore + Send + Sync>>>,
    pool: Arc<RwLock<Box<dyn Pool + Send + Sync>>>,
    slice_roots: BTreeMap<(BlockId, SliceIndex), SliceRoot>,
    outstanding_requests: BTreeMap<Hash, RepairRequestType>,
    /// Retry deadlines, soonest first; `Reverse` makes the heap pop earliest due.
    request_timeouts: BinaryHeap<std::cmp::Reverse<(Instant, Hash)>>,
    /// Unanswered retries per outstanding request, driving the backoff.
    retries: BTreeMap<Hash, u32>,
    /// Blocks waiting out the start grace: (first request due, extensions granted).
    deferred: BTreeMap<BlockId, (Instant, u32)>,
    start_grace: Duration,
    network: N,
    sampler: StakeWeightedSampler,
    epoch_info: Arc<EpochInfo>,
}

impl<N> Repair<N>
where
    N: RepairNetwork,
{
    /// Creates a repair instance that writes repaired shreds into `blockstore`.
    pub fn new(
        blockstore: Arc<RwLock<Box<dyn Blockstore + Send + Sync>>>,
        pool: Arc<RwLock<Box<dyn Pool + Send + Sync>>>,
        network: N,
        epoch_info: Arc<EpochInfo>,
    ) -> Self {
        let validators = epoch_info.validators.clone();
        let sampler = StakeWeightedSampler::new(validators);
        Self {
            blockstore,
            pool,
            slice_roots: BTreeMap::new(),
            outstanding_requests: BTreeMap::new(),
            request_timeouts: BinaryHeap::new(),
            retries: BTreeMap::new(),
            deferred: BTreeMap::new(),
            start_grace: repair_start_grace(),
            network,
            sampler,
            epoch_info,
        }
    }

    /// Override the start grace (tests; lossless full-duplex transports).
    pub fn with_start_grace(mut self, grace: Duration) -> Self {
        self.start_grace = grace;
        self
    }

    /// Runs repair requests, responses, deferred starts, and retry timeouts.
    pub async fn repair_loop(&mut self, mut repair_receiver: tokio::sync::mpsc::Receiver<BlockId>) {
        loop {
            let next_retry = self
                .request_timeouts
                .peek()
                .map(|std::cmp::Reverse((t, _))| *t);
            let next_start = self.deferred.values().map(|(t, _)| *t).min();
            let next_due = match (next_retry, next_start) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            let sleep_duration = match next_due {
                None => std::time::Duration::MAX,
                Some(t) => t.saturating_duration_since(Instant::now()),
            };
            tokio::select! {
                res = self.network.receive() => self.handle_response(res.unwrap()).await,
                Some(block_id) = repair_receiver.recv() => {
                    self.repair_block(block_id).await;
                }
                () = tokio::time::sleep(sleep_duration) => {
                    let now = Instant::now();
                    self.start_due_repairs(now).await;
                    self.retry_due_requests(now).await;
                }
            }
        }
    }

    /// Schedules repair for `block_id` after the start grace, unless it is
    /// already stored or already being repaired.
    pub async fn repair_block(&mut self, block_id: BlockId) {
        let (slot, block_hash) = &block_id;
        let h = &hex::encode(block_hash.as_hash())[..8];
        if self.have_block(&block_id).await {
            trace!("ignoring repair for block {h} in slot {slot}, already have the block");
            return;
        }
        if self.deferred.contains_key(&block_id)
            || self
                .outstanding_requests
                .values()
                .any(|r| r.block_id() == &block_id)
        {
            trace!("repair of block {h} in slot {slot} already in progress");
            return;
        }
        info!(
            "deferring repair of block {h} in slot {slot} for {:?}: its shreds may still be in flight",
            self.start_grace
        );
        self.deferred
            .insert(block_id, (Instant::now() + self.start_grace, 0));
    }

    /// Fires deferred starts whose grace has elapsed: drop blocks that arrived
    /// meanwhile, extend while the leader's stream for that window is still
    /// arriving, request the rest.
    async fn start_due_repairs(&mut self, now: Instant) {
        let due: Vec<(BlockId, u32)> = self
            .deferred
            .iter()
            .filter(|(_, (at, _))| *at <= now)
            .map(|(id, (_, ext))| (id.clone(), *ext))
            .collect();
        for (block_id, extensions) in due {
            self.deferred.remove(&block_id);
            let (slot, block_hash) = &block_id;
            let h = &hex::encode(block_hash.as_hash())[..8];
            if self.have_block(&block_id).await {
                debug!("block {h} in slot {slot} arrived during the repair grace");
                continue;
            }
            if extensions < MAX_START_EXTENSIONS && self.stream_in_progress(*slot).await {
                debug!("leader still streaming window of slot {slot}; extending repair grace for {h}");
                self.deferred
                    .insert(block_id, (now + self.start_grace, extensions + 1));
                continue;
            }
            info!("repairing block {h} in slot {slot}");
            let req = RepairRequestType::LastSliceRoot(block_id);
            self.send_request(req).await.unwrap();
        }
    }

    /// Re-sends timed-out requests, dropping any whose block has since arrived.
    async fn retry_due_requests(&mut self, now: Instant) {
        while let Some(std::cmp::Reverse((at, _))) = self.request_timeouts.peek() {
            if *at > now {
                break;
            }
            let std::cmp::Reverse((_, hash)) = self.request_timeouts.pop().unwrap();
            let Some(request) = self.outstanding_requests.remove(&hash) else {
                continue;
            };
            if self.have_block(request.block_id()).await {
                self.forget_block(&request.block_id().clone());
                continue;
            }
            debug!("retrying timed-out repair request {request:?}");
            *self.retries.entry(hash).or_insert(0) += 1;
            self.send_request(request).await.unwrap();
        }
    }

    async fn have_block(&self, block_id: &BlockId) -> bool {
        self.blockstore.read().await.get_block(block_id).is_some()
    }

    /// Whether disseminated shreds for `slot` or an earlier slot of the same
    /// leader window are stored without completing: the leader is mid-stream,
    /// and it sends its window in slot order.
    async fn stream_in_progress(&self, slot: crate::Slot) -> bool {
        let bs = self.blockstore.read().await;
        slot.first_slot_in_window()
            .slots_in_window()
            .take_while(|s| *s <= slot)
            .any(|s| bs.has_partial_disseminated_block(s))
    }

    /// Drops every outstanding request and retry for a block we now hold.
    fn forget_block(&mut self, block_id: &BlockId) {
        let (slot, block_hash) = block_id;
        debug!(
            "block {} in slot {slot} is complete; dropping its repair",
            &hex::encode(block_hash.as_hash())[..8]
        );
        let hashes: Vec<Hash> = self
            .outstanding_requests
            .iter()
            .filter(|(_, r)| r.block_id() == block_id)
            .map(|(h, _)| h.clone())
            .collect();
        for h in &hashes {
            self.outstanding_requests.remove(h);
            self.retries.remove(h);
        }
        self.request_timeouts
            .retain(|std::cmp::Reverse((_, h))| !hashes.contains(h));
    }

    /// Handles a repair response, storing verified metadata or shreds.
    async fn handle_response(&mut self, response: RepairResponse) {
        trace!("handling repair response: {response:?}");
        let request_hash = response.request_type().hash();

        let Some(pending) = self.outstanding_requests.remove(&request_hash) else {
            // Half-duplex retries can race with late duplicates; debug is enough.
            debug!("received repair response for already-settled request");
            return;
        };
        self.retries.remove(&request_hash);
        // Dissemination usually wins the race on the radio; never request
        // slices and shreds for a block we already hold.
        if self.have_block(pending.block_id()).await {
            self.forget_block(&pending.block_id().clone());
            return;
        }
        // Re-arm malformed responses; one bad response must not kill repair.
        let handled: bool = 'validate: {
            match response {
                RepairResponse::LastSliceRoot(req_type, last_slice, root, proof) => {
                    let RepairRequestType::LastSliceRoot(block_id) = &req_type else {
                        warn!(
                            "repair response (LastSliceRoot) to mismatching request {req_type:?}"
                        );
                        break 'validate false;
                    };
                    let (_, block_hash) = block_id;
                    if !DoubleMerkleTree::check_proof_last(
                        &root,
                        last_slice.inner(),
                        block_hash,
                        &proof,
                    ) {
                        warn!("repair response (LastSliceRoot) with invalid proof");
                        break 'validate false;
                    }

                    self.slice_roots
                        .insert((block_id.clone(), last_slice), root);

                    for slice in last_slice.until() {
                        let req_type = RepairRequestType::SliceRoot(block_id.clone(), slice);
                        self.send_request(req_type).await.unwrap();
                    }
                }
                RepairResponse::SliceRoot(req_type, root, proof) => {
                    let RepairRequestType::SliceRoot(ref block_id, slice) = req_type else {
                        warn!("repair response (SliceRoot) to mismatching request {req_type:?}");
                        break 'validate false;
                    };
                    let (_, block_hash) = block_id;
                    if !DoubleMerkleTree::check_proof(&root, slice.inner(), block_hash, &proof) {
                        warn!("repair response (SliceRoot) with invalid proof");
                        break 'validate false;
                    }

                    self.slice_roots.insert((block_id.clone(), slice), root);

                    // Request all shreds because peers may miss early data shreds.
                    for shred_index in ShredIndex::all() {
                        let req = RepairRequestType::Shred(block_id.clone(), slice, shred_index);
                        self.send_request(req).await.unwrap();
                    }
                }
                RepairResponse::Shred(req_type, shred) => {
                    let RepairRequestType::Shred(ref block_id, slice, index) = req_type else {
                        warn!("repair response (Shred) to mismatching request {req_type:?}");
                        break 'validate false;
                    };
                    let (slot, block_hash) = block_id;
                    if shred.payload().header.slot != *slot
                        || shred.payload().header.slice_index != slice
                        || shred.payload().shred_index != index
                    {
                        warn!("repair response (Shred) for mismatching shred index");
                        break 'validate false;
                    }
                    let Some(root) = self.slice_roots.get(&(block_id.clone(), slice)) else {
                        // Response races can arrive before the slice root; re-arm instead of panicking.
                        warn!("repair response (Shred) before knowing slice root — re-requesting");
                        break 'validate false;
                    };
                    if !shred.verify_path_only(root) {
                        warn!("repair response (Shred) with invalid Merkle proof");
                        break 'validate false;
                    }

                    let res = self
                        .blockstore
                        .write()
                        .await
                        .add_shred_from_repair(block_hash.clone(), shred)
                        .await;
                    if let Ok(Some(block_info)) = res {
                        assert_eq!(block_info.hash, *block_hash);
                        self.pool
                            .write()
                            .await
                            .add_block((*slot, block_info.hash), block_info.parent)
                            .await;
                        debug!(
                            "successfully repaired block {} in slot {}",
                            &hex::encode(block_hash.as_hash())[..8],
                            slot
                        );
                    }
                }
            }
            true
        };

        if !handled {
            let expiry = Instant::now() + repair_timeout();
            self.request_timeouts
                .retain(|std::cmp::Reverse((_, h))| h != &request_hash);
            self.request_timeouts
                .push(std::cmp::Reverse((expiry, request_hash.clone())));
            self.outstanding_requests.insert(request_hash, pending);
        }
    }

    async fn send_request(&mut self, req_type: RepairRequestType) -> std::io::Result<()> {
        let hash = req_type.hash();

        let retries = self.retries.get(&hash).copied().unwrap_or(0);
        let expiry = Instant::now() + retry_delay(retries);
        self.outstanding_requests
            .insert(hash.clone(), req_type.clone());
        self.request_timeouts
            .retain(|std::cmp::Reverse((_, h))| h != &hash);
        self.request_timeouts
            .push(std::cmp::Reverse((expiry, hash)));

        let request = RepairRequest {
            sender: self.epoch_info.own_id,
            req_type,
        };
        // Fan out retries to several peers to tolerate high-loss scenarios.
        let mut to_all = HashSet::new();
        for _ in 0..10 {
            to_all.insert(self.pick_random_peer());
            if to_all.len() == 3 {
                break;
            }
        }
        self.network
            .send_to_many(&request, to_all.into_iter())
            .await?;
        Ok(())
    }

    fn pick_random_peer(&self) -> SocketAddr {
        let mut rng = rand::rng();
        let mut peer_info = self.sampler.sample_info(&mut rng);
        while peer_info.id == self.epoch_info.own_id {
            peer_info = self.sampler.sample_info(&mut rng);
        }
        peer_info.repair_request_address
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use tokio::sync::mpsc::Sender;
    use tokio::time::timeout;

    use super::*;

    /// Retries double until the cap so an in-flight response is never
    /// re-requested more than once before it can land on a slow link.
    #[test]
    fn retry_delay_backs_off_and_caps() {
        let base = repair_timeout();
        assert_eq!(retry_delay(0), base);
        assert_eq!(retry_delay(1), base * 2);
        assert_eq!(retry_delay(3), base * 8);
        assert_eq!(retry_delay(9), base * 8);
    }
    use crate::consensus::{BlockstoreImpl, PoolImpl};
    use crate::crypto::signature::SecretKey;
    use crate::network::simulated::SimulatedNetworkCore;
    use crate::network::{SimulatedNetwork, localhost_ip_sockaddr};
    use crate::shredder::TOTAL_SHREDS;
    use crate::test_utils::{create_random_shredded_block, generate_validators};
    use crate::types::Slot;
    use crate::types::slice_index::MAX_SLICES_PER_BLOCK;

    /// Creates a two-validator repair test fixture with the given start grace.
    async fn create_repair_instance(
        grace: Duration,
    ) -> (
        Sender<BlockId>,
        Arc<RwLock<Box<dyn Blockstore + Send + Sync>>>,
        SimulatedNetwork<RepairResponse, RepairRequest>,
        SimulatedNetwork<RepairRequest, RepairResponse>,
        SecretKey,
    ) {
        let (_, epoch_info) = generate_validators(2);
        let mut epoch_info = Arc::try_unwrap(epoch_info).unwrap();
        let leader_key = SecretKey::new(&mut rand::rng());
        let v0 = epoch_info.validators.get_mut(0).unwrap();
        v0.pubkey = leader_key.to_pk();
        v0.repair_request_address = localhost_ip_sockaddr(0);
        v0.repair_response_address = localhost_ip_sockaddr(1);

        let core = Arc::new(SimulatedNetworkCore::new(1, 0.0, 0.0));
        let v0_repair_request_network = core
            .join_unlimited(v0.repair_request_address.port() as u64)
            .await;
        let v0_repair_network = core
            .join_unlimited(v0.repair_response_address.port() as u64)
            .await;

        let v1 = epoch_info.validators.get_mut(1).unwrap();
        v1.repair_request_address = localhost_ip_sockaddr(2);
        v1.repair_response_address = localhost_ip_sockaddr(3);
        epoch_info.own_id = 1;

        let v1_repair_request_network = core
            .join_unlimited(v1.repair_request_address.port() as u64)
            .await;
        let v1_repair_network = core
            .join_unlimited(v1.repair_response_address.port() as u64)
            .await;

        let epoch_info = Arc::new(epoch_info);

        let (votor_tx, votor_rx) = tokio::sync::mpsc::channel(100);
        let blockstore: Arc<RwLock<Box<dyn Blockstore + Send + Sync>>> = Arc::new(RwLock::new(
            Box::new(BlockstoreImpl::new(epoch_info.clone(), votor_tx.clone())),
        ));

        let (repair_tx, repair_rx) = tokio::sync::mpsc::channel(100);
        let pool: Arc<RwLock<Box<dyn Pool + Send + Sync>>> = Arc::new(RwLock::new(Box::new(
            PoolImpl::new(epoch_info.clone(), votor_tx, repair_tx.clone()),
        )));

        let mut repair = Repair::new(
            Arc::clone(&blockstore),
            pool,
            v1_repair_network,
            epoch_info.clone(),
        )
        .with_start_grace(grace);
        tokio::spawn(async move {
            repair.repair_loop(repair_rx).await;
            drop(votor_rx);
        });
        let repair_request_handler =
            RepairRequestHandler::new(epoch_info, blockstore.clone(), v1_repair_request_network);
        tokio::spawn(async move {
            repair_request_handler.run().await;
        });
        (
            repair_tx,
            blockstore,
            v0_repair_request_network,
            v0_repair_network,
            leader_key,
        )
    }

    #[tokio::test]
    async fn repair_tiny_block() {
        repair_block(1).await;
    }

    #[tokio::test]
    async fn repair_regular_block() {
        repair_block(10).await;
    }

    // Slow in debug mode; run with sequential ignored tests.
    #[tokio::test]
    #[ignore]
    async fn repair_large_block() {
        repair_block(MAX_SLICES_PER_BLOCK).await;
    }

    async fn repair_block(num_slices: usize) {
        let (repair_channel, blockstore, other_network_request, _other_network_reply, sk) =
            create_repair_instance(Duration::ZERO).await;

        let slot = Slot::genesis().next();
        let (block_hash, merkle_tree, shreds) = create_random_shredded_block(slot, num_slices, &sk);
        let block_to_repair = (slot, block_hash);

        repair_channel.send(block_to_repair.clone()).await.unwrap();

        let msg = other_network_request.receive().await.unwrap();
        let req_type = RepairRequestType::LastSliceRoot(block_to_repair.clone());
        assert_eq!(msg.req_type, req_type);

        let response = RepairResponse::LastSliceRoot(
            req_type,
            SliceIndex::new_unchecked(num_slices - 1),
            shreds.last().unwrap()[0].merkle_root.clone(),
            merkle_tree.create_proof(num_slices - 1),
        );
        let port1 = localhost_ip_sockaddr(3);
        other_network_request.send(&response, port1).await.unwrap();

        let mut slice_roots_requested = BTreeSet::new();
        for _ in 0..num_slices {
            let msg = other_network_request.receive().await.unwrap();

            for slice in SliceIndex::all().take(num_slices) {
                let req_type = RepairRequestType::SliceRoot(block_to_repair.clone(), slice);
                if msg.req_type == req_type {
                    slice_roots_requested.insert(slice);
                    break;
                }
            }
        }

        for slice in SliceIndex::all().take(num_slices) {
            assert!(slice_roots_requested.contains(&slice));
            let req_type = RepairRequestType::SliceRoot(block_to_repair.clone(), slice);
            let root = shreds[slice.inner()][0].merkle_root.clone();
            let proof = merkle_tree.create_proof(slice.inner());
            let response = RepairResponse::SliceRoot(req_type, root, proof);
            other_network_request.send(&response, port1).await.unwrap();

            let mut shreds_requested = BTreeSet::new();
            for _ in ShredIndex::all() {
                let msg = other_network_request.receive().await.unwrap();
                for shred_index in ShredIndex::all() {
                    let req_type =
                        RepairRequestType::Shred(block_to_repair.clone(), slice, shred_index);
                    if msg.req_type == req_type {
                        shreds_requested.insert(shred_index);
                        break;
                    }
                }
            }

            let slice_shreds = shreds[slice.inner()].clone();
            for (shred_index, shred) in slice_shreds.into_iter().take(TOTAL_SHREDS).enumerate() {
                let shred_index = ShredIndex::new(shred_index).unwrap();
                assert!(shreds_requested.contains(&shred_index));
                let req_type =
                    RepairRequestType::Shred(block_to_repair.clone(), slice, shred_index);
                let response = RepairResponse::Shred(req_type, shred.into_shred());
                other_network_request.send(&response, port1).await.unwrap();
            }
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            blockstore
                .read()
                .await
                .get_block(&block_to_repair)
                .is_some()
        );
    }

    /// A block known only from votes waits out the start grace before its
    /// first request, and is dropped if dissemination delivers it meanwhile.
    #[tokio::test]
    async fn repair_start_waits_grace_and_drops_on_arrival() {
        let grace = Duration::from_millis(400);
        let (repair_channel, blockstore, other_network_request, _other_network_reply, sk) =
            create_repair_instance(grace).await;

        let slot = Slot::genesis().next();
        let (block_hash, _, _) = create_random_shredded_block(slot, 1, &sk);
        let missing = (slot, block_hash);
        repair_channel.send(missing.clone()).await.unwrap();
        assert!(
            timeout(grace / 2, other_network_request.receive())
                .await
                .is_err(),
            "no request inside the grace"
        );
        let msg = timeout(grace * 2, other_network_request.receive())
            .await
            .expect("request once the grace elapses")
            .unwrap();
        assert_eq!(msg.req_type, RepairRequestType::LastSliceRoot(missing));

        let slot = slot.next();
        let (block_hash, _, shreds) = create_random_shredded_block(slot, 1, &sk);
        let arriving = (slot, block_hash);
        repair_channel.send(arriving.clone()).await.unwrap();
        for shred in shreds[0].clone() {
            let _ = blockstore
                .write()
                .await
                .add_shred_from_disseminator(shred.into_shred())
                .await;
        }
        assert!(blockstore.read().await.get_block(&arriving).is_some());
        assert!(
            timeout(grace * 2, other_network_request.receive())
                .await
                .is_err(),
            "a block that arrived during the grace is not requested"
        );
    }

    /// While the leader's shreds for that window are still arriving the grace
    /// is extended, a bounded number of times, before the first request.
    #[tokio::test]
    async fn repair_grace_extends_while_stream_in_progress() {
        let grace = Duration::from_millis(300);
        let (repair_channel, blockstore, other_network_request, _other_network_reply, sk) =
            create_repair_instance(grace).await;

        let slot = Slot::genesis().next();
        let (block_hash, _, shreds) = create_random_shredded_block(slot, 1, &sk);
        // One shred stored: dissemination is mid-flight for this slot.
        blockstore
            .write()
            .await
            .add_shred_from_disseminator(shreds[0][0].clone().into_shred())
            .await
            .unwrap();
        repair_channel.send((slot, block_hash.clone())).await.unwrap();

        let extended = grace * (MAX_START_EXTENSIONS + 1);
        assert!(
            timeout(extended - grace / 2, other_network_request.receive())
                .await
                .is_err(),
            "no request while the stream is in progress and extensions remain"
        );
        let msg = timeout(grace * 2, other_network_request.receive())
            .await
            .expect("request once the extensions are spent")
            .unwrap();
        assert_eq!(
            msg.req_type,
            RepairRequestType::LastSliceRoot((slot, block_hash))
        );
    }

    /// A block completed by dissemination cancels its outstanding repair: the
    /// last-slice-root answer must not fan out into slice and shred requests.
    #[tokio::test]
    async fn completed_block_cancels_outstanding_repair() {
        let (repair_channel, blockstore, other_network_request, _other_network_reply, sk) =
            create_repair_instance(Duration::ZERO).await;

        let slot = Slot::genesis().next();
        let (block_hash, merkle_tree, shreds) = create_random_shredded_block(slot, 2, &sk);
        let block_id = (slot, block_hash);
        repair_channel.send(block_id.clone()).await.unwrap();
        let msg = other_network_request.receive().await.unwrap();
        let req_type = RepairRequestType::LastSliceRoot(block_id.clone());
        assert_eq!(msg.req_type, req_type);

        for slice in &shreds {
            for shred in slice.clone() {
                let _ = blockstore
                    .write()
                    .await
                    .add_shred_from_disseminator(shred.into_shred())
                    .await;
            }
        }
        assert!(blockstore.read().await.get_block(&block_id).is_some());

        let response = RepairResponse::LastSliceRoot(
            req_type,
            SliceIndex::new_unchecked(1),
            shreds[1][0].merkle_root.clone(),
            merkle_tree.create_proof(1),
        );
        other_network_request
            .send(&response, localhost_ip_sockaddr(3))
            .await
            .unwrap();
        assert!(
            timeout(Duration::from_millis(500), other_network_request.receive())
                .await
                .is_err(),
            "no slice-root requests for a block we already hold"
        );
    }

    #[tokio::test]
    async fn answer_requests() {
        const SLICES: usize = 2;
        let (_sender, blockstore, _other_network_request, other_network, sk) =
            create_repair_instance(Duration::ZERO).await;

        let slot = Slot::genesis().next();
        let (block_hash, _, shreds) = create_random_shredded_block(slot, SLICES, &sk);
        let block_to_repair = (slot, block_hash.clone());

        for slice_shreds in shreds.clone() {
            let mut b = blockstore.write().await;
            for shred in slice_shreds {
                let _ = b.add_shred_from_disseminator(shred.into_shred()).await;
            }
        }
        assert_eq!(
            blockstore.read().await.disseminated_block_hash(slot),
            Some(block_hash.clone())
        );
        assert!(
            blockstore
                .read()
                .await
                .get_block(&block_to_repair)
                .is_some()
        );

        let request = RepairRequest {
            req_type: RepairRequestType::LastSliceRoot(block_to_repair.clone()),
            sender: 0,
        };
        let port1 = localhost_ip_sockaddr(2);
        other_network.send(&request, port1).await.unwrap();

        let msg = other_network.receive().await.unwrap();
        let RepairResponse::LastSliceRoot(req_type, last_slice, root, proof) = msg else {
            panic!("not LastSliceRoot response");
        };
        assert_eq!(req_type, request.req_type);
        assert_eq!(last_slice.inner(), SLICES - 1);
        assert_eq!(root, shreds[last_slice.inner()][0].merkle_root);
        let correct_proof = blockstore
            .read()
            .await
            .create_double_merkle_proof(&block_to_repair, last_slice)
            .unwrap();
        assert_eq!(proof, correct_proof);

        for slice in SliceIndex::all().take(SLICES) {
            let request = RepairRequest {
                req_type: RepairRequestType::SliceRoot(block_to_repair.clone(), slice),
                sender: 0,
            };
            other_network.send(&request, port1).await.unwrap();

            let msg = other_network.receive().await.unwrap();
            let RepairResponse::SliceRoot(req_type, root, proof) = msg else {
                panic!("not SliceRoot response");
            };
            assert_eq!(req_type, request.req_type);
            assert_eq!(root, shreds[slice.inner()][0].merkle_root);
            let correct_proof = blockstore
                .read()
                .await
                .create_double_merkle_proof(&block_to_repair, slice)
                .unwrap();
            assert_eq!(proof, correct_proof);

            for shred_index in ShredIndex::all() {
                let request = RepairRequest {
                    req_type: RepairRequestType::Shred(block_to_repair.clone(), slice, shred_index),
                    sender: 0,
                };
                other_network.send(&request, port1).await.unwrap();

                let msg = other_network.receive().await.unwrap();
                let RepairResponse::Shred(req_type, shred) = msg else {
                    panic!("not Shred response");
                };
                assert_eq!(req_type, request.req_type);
                assert_eq!(
                    shred.payload().data,
                    shreds[slice.inner()][*shred_index].payload().data
                );
            }
        }
    }
}
