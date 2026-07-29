// Copyright (c) Anza Technology, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Double-Merkle block repair protocol with proof-verified responses.

use std::collections::{BTreeMap, BinaryHeap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use log::{debug, trace, warn};
use tokio::sync::RwLock;
use wincode::{SchemaRead, SchemaWrite};

use crate::consensus::{Blockstore, EpochInfo, Pool, delta};
use crate::crypto::merkle::{DoubleMerkleProof, DoubleMerkleTree, MerkleRoot, SliceRoot};
use crate::crypto::{Hash, hash};
use crate::disseminator::rotor::{SamplingStrategy, StakeWeightedSampler};
use crate::network::{Network, RepairNetwork, RepairRequestNetwork};
use crate::shredder::{Shred, ShredIndex};
use crate::types::SliceIndex;
use crate::{BlockId, ValidatorId};

/// Repair response timeout before retrying another peer.
fn repair_timeout() -> Duration {
    delta() * 2
}

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
            network,
            sampler,
            epoch_info,
        }
    }

    /// Runs repair requests, responses, and retry timeouts.
    pub async fn repair_loop(&mut self, mut repair_receiver: tokio::sync::mpsc::Receiver<BlockId>) {
        loop {
            let next_timeout = self
                .request_timeouts
                .peek()
                .map(|std::cmp::Reverse((t, _))| t);
            let sleep_duration = match next_timeout {
                None => std::time::Duration::MAX,
                Some(t) => t.saturating_duration_since(Instant::now()),
            };
            tokio::select! {
                res = self.network.receive() => self.handle_response(res.unwrap()).await,
                Some(block_id) = repair_receiver.recv() => {
                    self.repair_block(block_id).await;
                }
                () = tokio::time::sleep(sleep_duration) => {
                    let Some(std::cmp::Reverse((_, hash))) = self.request_timeouts.pop() else {
                        continue;
                    };
                    if let Some(request) = self.outstanding_requests.remove(&hash) {
                        debug!("retrying timed-out repair request {request:?}");
                        self.send_request(request).await.unwrap();
                    }
                }
            }
        }
    }

    /// Starts repair for `block_id`.
    pub async fn repair_block(&mut self, block_id: BlockId) {
        let (slot, block_hash) = &block_id;
        let h = &hex::encode(block_hash.as_hash())[..8];
        if self.blockstore.read().await.get_block(&block_id).is_some() {
            trace!("ignoring repair for block {h} in slot {slot}, already have the block");
            return;
        }

        debug!("repairing block {h} in slot {slot}");
        let req = RepairRequestType::LastSliceRoot(block_id);
        self.send_request(req).await.unwrap();
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

        let expiry = Instant::now() + repair_timeout();
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

    use super::*;
    use crate::consensus::{BlockstoreImpl, PoolImpl};
    use crate::crypto::signature::SecretKey;
    use crate::network::simulated::SimulatedNetworkCore;
    use crate::network::{SimulatedNetwork, localhost_ip_sockaddr};
    use crate::shredder::TOTAL_SHREDS;
    use crate::test_utils::{create_random_shredded_block, generate_validators};
    use crate::types::Slot;
    use crate::types::slice_index::MAX_SLICES_PER_BLOCK;

    /// Creates a two-validator repair test fixture.
    async fn create_repair_instance() -> (
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
        );
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
            create_repair_instance().await;

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

    #[tokio::test]
    async fn answer_requests() {
        const SLICES: usize = 2;
        let (_sender, blockstore, _other_network_request, other_network, sk) =
            create_repair_instance().await;

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
