// Copyright (c) Anza Technology, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Data structure holding blocks for each slot.

mod slot_block_data;

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use log::debug;
use mockall::automock;
use tokio::sync::mpsc::Sender;

use self::slot_block_data::{AddShredError, SlotBlockData};
use super::epoch_info::EpochInfo;
use super::votor::VotorEvent;
use crate::consensus::blockstore::slot_block_data::BlockData;
use crate::crypto::merkle::{BlockHash, DoubleMerkleProof, MerkleRoot, SliceRoot};
use crate::shredder::{RegularShredder, Shred, ShredIndex, ShredderPool, ValidatedShred};
use crate::types::SliceIndex;
use crate::{Block, BlockId, Slot};

use rocksdb::{DB, Options, IteratorMode, WriteBatch};
use serde::{Serialize, Deserialize};

/// additional metadata (might need refactor @e)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlockMetadata {
    pub slot: Slot,
    pub hash: Hash,
    pub producer: u64,
    pub proposed_timestamp: u64,
    pub finalized_timestamp: Option<u64>,
}

/// Information about a block within a slot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockInfo {
    pub(crate) hash: BlockHash,
    pub(crate) parent: BlockId,
}

impl From<&Block> for BlockInfo {
    fn from(block: &Block) -> Self {
        BlockInfo {
            hash: block.hash.clone(),
            parent: (block.parent, block.parent_hash.clone()),
        }
    }
}

/// blocks kept in memory at start-up
const HOT_BLOCK_LIMIT: usize = 200;

/// Blockstore is the fundamental data structure holding block data per slot.
pub struct BlockstoreImpl {
    /// Data structure holding the actual block data per slot.
    block_data: BTreeMap<Slot, SlotBlockData>,
    /// Shredders used for reconstructing blocks.
    shredders: ShredderPool<RegularShredder>,

    /// Event channel for sending notifications to Votor.
    votor_channel: Sender<VotorEvent>,
    /// Information about all active validators.
    epoch_info: Arc<EpochInfo>,
    /// Cache of previously verified Merkle roots.
    merkle_root_cache: HashMap<(Slot, usize), Hash>,

    /// Persistent RocksDB handle for durable block storage.
    db: DB,
    /// Set of slots for which conflicting shreds have been seen (leader equivocated).
    equivocated_slots: BTreeSet<Slot>,
}

impl BlockstoreImpl {
    /// Initializes a new empty blockstore.
    ///
    /// Blockstore will send the following [`VotorEvent`]s to the provided `votor_channel`:
    /// - [`VotorEvent::FirstShred`] when receiving the first shred for a slot
    ///   from the block dissemination protocol
    /// - [`VotorEvent::Block`] for any reconstructed block
    pub fn new(epoch_info: Arc<EpochInfo>, votor_channel: Sender<VotorEvent>) -> Self {
        // ensure the data directory exists and open/create RocksDB database
        std::fs::create_dir_all("data").ok();
        let db_path = format!("data/blockstore/{}", epoch_info.own_id);
        std::fs::create_dir_all(&db_path).ok();

        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, db_path).expect("open RocksDB");

        // initialise in-memory structures
        let mut s = Self {
            shreds: BTreeMap::new(),
            slices: BTreeMap::new(),
            blocks: BTreeMap::new(),
            canonical: BTreeMap::new(),
            alternatives: BTreeMap::new(),
            first_shred_seen: BTreeSet::new(),
            double_merkle_trees: BTreeMap::new(),
            last_slices: BTreeMap::new(),
            votor_channel,
            epoch_info,
            merkle_root_cache: HashMap::new(),
            db,
            equivocated_slots: BTreeSet::new(),
        };

        // warm cache with at most HOT_BLOCK_LIMIT most recent blocks so the node can resume quickly without having to load all into ram
        let mut loaded = 0usize;
        for item in s.db.iterator(IteratorMode::End) {
            if loaded == HOT_BLOCK_LIMIT { break; }
            if let Ok((_k, raw_val)) = item {
                if let Ok((block, _)) = bincode::serde::decode_from_slice::<Block, _>(&raw_val, bincode::config::standard()) {
                    s.canonical.insert(block.slot(), block.block_hash());
                    s.blocks.insert((block.slot(), block.block_hash()), block);
                    loaded += 1;
                }
            }
        }

        s
    }

    /// Deletes everything before the given `slot` from the blockstore.
    pub fn prune(&mut self, slot: Slot) {
        self.block_data = self.block_data.split_off(&slot);
    }

    async fn send_votor_event(&self, event: VotorEvent) -> Option<BlockInfo> {
        match &event {
            VotorEvent::FirstShred(_) => {
                self.votor_channel.send(event).await.unwrap();
                None
            }
            VotorEvent::Block { slot, block_info } => {
                let block_info = block_info.clone();
                debug!(
                    "reconstructed block {} in slot {} with parent {} in slot {}",
                    &hex::encode(block_info.hash.as_hash())[..8],
                    slot,
                    &hex::encode(block_info.parent.1.as_hash())[..8],
                    block_info.parent.0,
                );
                self.votor_channel.send(event).await.unwrap();

                Some(block_info)
            }
            ev => panic!("unexpected event {ev:?}"),
        }
    }

    /// Gives reference to stored block data for the given `block_id`.
    ///
    /// Considers both, the disseminated block and any repaired blocks.
    ///
    /// Returns [`None`] if blockstore does not know about this block yet.
    fn get_block_data(&self, block_id: &BlockId) -> Option<&BlockData> {
        let (slot, hash) = block_id;
        let slot_data = self.slot_data(*slot)?;
        if let Some((h, _)) = &slot_data.disseminated.completed
            && h == hash
        {
            return Some(&slot_data.disseminated);
        }
        slot_data.repaired.get(hash)
    }

    /// Reads slot data for the given `slot`.
    fn slot_data(&self, slot: Slot) -> Option<&SlotBlockData> {
        self.block_data.get(&slot)
    }

    /// Writes slot data for the given `slot`, initializing it if necessary.
    fn slot_data_mut(&mut self, slot: Slot) -> &mut SlotBlockData {
        self.block_data
            .entry(slot)
            .or_insert_with(|| SlotBlockData::new(slot))
    }

    /// Gives the shred for the given `slot`, `slice` and `shred` index.
    ///
    /// Considers only the disseminated block.
    ///
    /// Only used for testing.
    #[cfg(test)]
    fn get_disseminated_shred(
        &self,
        slot: Slot,
        slice: SliceIndex,
        shred_index: ShredIndex,
    ) -> Option<&ValidatedShred> {
        self.slot_data(slot).and_then(|s| {
            s.disseminated
                .shreds
                .get(&slice)
                .and_then(|shreds| shreds[*shred_index].as_ref())
        })
    }

    /// Gives the number of stored shreds for a given `slot` (across all slices).
    ///
    /// Only used for testing.
    #[cfg(test)]
    fn stored_shreds_for_slot(&self, slot: Slot) -> usize {
        self.slot_data(slot).map_or(0, |s| {
            let mut cnt = 0;
            for shreds in s.disseminated.shreds.values() {
                cnt += shreds.iter().filter(|s| s.is_some()).count();
            }
            cnt
        })
    }

    /// Gives the number of stored slices for a given `slot`.
    ///
    /// Only used for testing.
    #[cfg(test)]
    pub(crate) fn stored_slices_for_slot(&self, slot: Slot) -> usize {
        self.slot_data(slot)
            .map_or(0, |s| s.disseminated.slices.len())
    }
}

#[async_trait]
impl Blockstore for BlockstoreImpl {
    /// Stores a new shred in the blockstore.
    ///
    /// This shred is stored in the default spot without a known block hash.
    /// For shreds obtained through repair, `add_shred_from_repair` should be used instead.
    /// Compared to that function, this one checks for leader equivocation.
    ///
    /// Reconstructs the corresponding slice and block if possible and necessary.
    /// If the added shred belongs to the last slice, all later shreds are deleted.
    ///
    /// Returns `Some(slot, block_info)` if a block was reconstructed, `None` otherwise.
    /// In the `Some`-case, `block_info` is the [`BlockInfo`] of the reconstructed block.
    #[fastrace::trace(short_name = true)]
    async fn add_shred_from_disseminator(
        &mut self,
        shred: Shred,
    ) -> Result<Option<BlockInfo>, AddShredError> {
        let slot = shred.payload().header.slot;
        let leader_pk = self.epoch_info.leader(slot).pubkey;
        let mut shredder = self
            .shredders
            .checkout()
            .expect("should have a shredder because of exclusive access");
        match self.slot_data_mut(slot).add_shred_from_disseminator(
            shred,
            leader_pk,
            &mut shredder,
        )? {
            Some(event) => Ok(self.send_votor_event(event).await),
            None => Ok(None),
        }
    }

    /// Stores a new shred from repair in the blockstore.
    ///
    /// This shred is stored in a spot associated with the given block`hash`.
    /// For shreds obtained through block dissemination, `add_shred_from_disseminator`
    /// should be used instead.
    /// Compared to that function, this one does not check for leader equivocation.
    ///
    /// Reconstructs the corresponding slice and block if possible and necessary.
    /// If the added shred belongs to last slice, deletes later slices and their shreds.
    ///
    /// Returns `Some(slot, block_info)` if a block was reconstructed, `None` otherwise.
    /// In the `Some`-case, `block_info` is the [`BlockInfo`] of the reconstructed block.
    #[fastrace::trace(short_name = true)]
    async fn add_shred_from_repair(
        &mut self,
        hash: BlockHash,
        shred: Shred,
    ) -> Result<Option<BlockInfo>, AddShredError> {
        let slot = shred.payload().header.slot;
        let leader_pk = self.epoch_info.leader(slot).pubkey;
        let mut shredder = self
            .shredders
            .checkout()
            .expect("should have a shredder because of exclusive access");
        match self.slot_data_mut(slot).add_shred_from_repair(
            hash,
            shred,
            leader_pk,
            &mut shredder,
        )? {
            Some(event) => Ok(self.send_votor_event(event).await),
            None => Ok(None),
        }
    }

    /// Gives the disseminated block hash for a given `slot`, if any.
    ///
    /// This refers to the block we received from block dissemination.
    ///
    /// Returns `None` if we have no block or only blocks from repair.
    fn disseminated_block_hash(&self, slot: Slot) -> Option<&BlockHash> {
        self.slot_data(slot)?
            .disseminated
            .completed
            .as_ref()
            .map(|c| &c.0)
    }

    /// Gives reference to stored block for the given `block_id`.
    ///
    /// Considers both, the disseminated block and any repaired blocks.
    /// However, the dissminated block can only be considered if it's complete.
    ///
    /// Returns `None` if blockstore does not know a block for that hash.
    fn get_block(&self, block_id: &BlockId) -> Option<&Block> {
        let block_data = self.get_block_data(block_id)?;
        if let Some((hash, block)) = block_data.completed.as_ref() {
            debug_assert_eq!(*hash, block_id.1);
            Some(block)
        } else {
            None
        }
    }

    /// Gives the last slice index for the given `block_id`.
    ///
    /// Returns `None` if blockstore does not know the last slice yet.
    fn get_last_slice_index(&self, block_id: &BlockId) -> Option<SliceIndex> {
        let block_data = self.get_block_data(block_id)?;
        block_data.last_slice
    }

    /// Gives the Merkle root for the given `slice_index` of the given `block_id`.
    ///
    /// Returns `Some(slot, block_info)` if a block was reconstructed, `None` otherwise.
    /// In the `Some`-case, `block_info` is the [`BlockInfo`] of the reconstructed block.
    async fn try_reconstruct_block(&mut self, slot: Slot) -> Option<(Slot, BlockInfo)> {
        if self.canonical_block_hash(slot).is_some() {
            trace!("already have block for slot {slot}");
            return None;
        }
        let last_slice = self.last_slices.get(&slot)?;
        if self.stored_slices_for_slot(slot) != last_slice + 1 {
            trace!("don't have all slices for slot {slot} yet");
            return None;
        }

        // calculate double-Merkle tree & block hash
        let merkle_roots: Vec<_> = self
            .slices
            .range(&(slot, 0)..)
            .take_while(|(key, _)| key.0 == slot)
            .map(|(_, s)| s.merkle_root.as_ref().unwrap())
            .collect();
        let tree = MerkleTree::new(&merkle_roots);
        let block_hash = tree.get_root();
        self.double_merkle_trees.insert(slot, tree);

        // reconstruct block header
        let first_slice = self.slices.get(&(slot, 0)).unwrap();
        let parent_slot = u64::from_be_bytes(first_slice.data[0..8].try_into().unwrap());
        let parent_hash = first_slice.data[8..40].try_into().unwrap();
        self.canonical.insert(slot, block_hash);
        // TODO: reconstruct actual block content
        let block = Block {
            slot,
            block_hash,
            parent: parent_slot,
            parent_hash,
            transactions: vec![],
        };
        let block_info = BlockInfo::from(&block);
        self.blocks.insert((slot, block_hash), block.clone());

        // persist canonical block to RocksDB for durability
        let key = format!("{:016X}{}", slot, hex::encode(block_hash));
        if let Ok(value) = bincode::serde::encode_to_vec(&block, bincode::config::standard()) {
            let _ = self.db.put(key.as_bytes(), value);
        }

        // block metadata with current timestamp
        let metadata = BlockMetadata {
            slot,
            hash: block_hash,
            producer: self.epoch_info.leader(slot).id,
            proposed_timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            finalized_timestamp: None,
        };
        let meta_key = format!("meta|{:016X}{}", slot, hex::encode(block_hash));
        if let Ok(value) = bincode::serde::encode_to_vec(&metadata, bincode::config::standard()) {
            let _ = self.db.put(meta_key.as_bytes(), value);
        }

        // clean up raw slices
        for slice_index in 0..=*last_slice {
            self.slices.remove(&(slot, slice_index));
        }

        // notify Votor of block and print block info
        let event = VotorEvent::Block { slot, block_info };
        self.votor_channel.send(event).await.unwrap();
        let h = &hex::encode(block_hash)[..8];
        let ph = &hex::encode(parent_hash)[..8];
        debug!("reconstructed block {h} in slot {slot} with parent {ph} in slot {parent_slot}");
        Some((slot, block_info))
    }

    /// Gives reference to stored shred for given `block_id`, `slice_index` and `shred_index`.
    ///
    /// Returns `None` if blockstore does not hold that shred.
    fn get_shred(
        &self,
        block_id: &BlockId,
        slice_index: SliceIndex,
        shred_index: ShredIndex,
    ) -> Option<&ValidatedShred> {
        let block_data = self.get_block_data(block_id)?;
        let slice_shreds = block_data.shreds.get(&slice_index)?;
        slice_shreds[*shred_index].as_ref()
    }

    /// Generates a Merkle proof for the given `slice_index` of the given `block_id`.
    ///
    /// Returns `None` if blockstore does not hold that block yet.
    fn create_double_merkle_proof(
        &self,
        block_id: &BlockId,
        slice_index: SliceIndex,
    ) -> Option<DoubleMerkleProof> {
        let block_data = self.get_block_data(block_id)?;
        let tree = block_data.double_merkle_tree.as_ref()?;
        Some(tree.create_proof(slice_index.inner()))
    }

    pub fn blocks_len(&self) -> usize {
        self.blocks.len()
    }
    pub fn shreds_len(&self) -> usize {
        self.shreds.len()
    }

    /// Fetches a block directly from RocksDB without caching it in RAM.
    pub fn load_block_from_db(&self, slot: Slot, hash: Hash) -> Option<Block> {
        let key = format!("{:016X}{}", slot, hex::encode(hash));
        if let Ok(Some(val)) = self.db.get(key.as_bytes()) {
            if let Ok((block, _)) = bincode::serde::decode_from_slice::<Block, _>(&val, bincode::config::standard()) {
                return Some(block);
            }
        }
        None
    }

    /// Searches RocksDB for a block with the given hash (slow path, should be o(n) tbd @e).
    /// Returns slot and block if found.
    pub fn load_block_by_hash(&self, hash: Hash) -> Option<(Slot, Block)> {
        let suffix = hex::encode(hash);
        let suffix_bytes = suffix.as_bytes();
        for item in self.db.iterator(IteratorMode::Start) {
            if let Ok((k, v)) = item {
                if k.len() >= 16 + suffix_bytes.len() && &k[k.len()-suffix_bytes.len()..] == suffix_bytes {
                    let slot_str = std::str::from_utf8(&k[0..16]).ok()?;
                    let slot = u64::from_str_radix(slot_str, 16).ok()?;
                    if let Ok((block, _)) = bincode::serde::decode_from_slice::<Block, _>(&v, bincode::config::standard()) {
                        return Some((slot, block));
                    }
                }
            }
        }
        None
    }

    /// Loads block metadata from RocksDB.
    pub fn load_block_metadata(&self, slot: Slot, hash: Hash) -> Option<BlockMetadata> {
        let key = format!("meta|{:016X}{}", slot, hex::encode(hash));
        if let Ok(Some(val)) = self.db.get(key.as_bytes()) {
            if let Ok((metadata, _)) = bincode::serde::decode_from_slice::<BlockMetadata, _>(&val, bincode::config::standard()) {
                return Some(metadata);
            }
        }
        None
    }

    /// Updates the finalized timestamp for a block.
    pub fn update_finalized_timestamp(&self, slot: Slot, hash: Hash, timestamp: u64) {
        if let Some(mut metadata) = self.load_block_metadata(slot, hash) {
            metadata.finalized_timestamp = Some(timestamp);
            let key = format!("meta|{:016X}{}", slot, hex::encode(hash));
            if let Ok(value) = bincode::serde::encode_to_vec(&metadata, bincode::config::standard()) {
                let _ = self.db.put(key.as_bytes(), value);
            }
        }
    }

    /// Loads highest finalized slot from Pool DB and prunes all blocks beyond it.
    /// This should be called after Pool has loaded its state.
    pub fn clean_beyond_finalized(&mut self, highest_finalized_slot: Slot) {
        println!("[Blockstore::clean_beyond_finalized] pruning blocks beyond slot {}", highest_finalized_slot);

        let mut batch = WriteBatch::default();
        let mut deleted_count = 0;
        let mut deleted_meta_count = 0;
        for item in self.db.iterator(IteratorMode::Start) {
            if let Ok((k, _v)) = item {
                if k.starts_with(b"meta|") {
                    if k.len() >= 21 {
                        if let Ok(slot_hex) = std::str::from_utf8(&k[5..21]) {
                            if let Ok(slot_val) = u64::from_str_radix(slot_hex, 16) {
                                if slot_val > highest_finalized_slot {
                                    batch.delete(&k);
                                    deleted_meta_count += 1;
                                }
                            }
                        }
                    }
                } else if k.len() >= 16 {
                    if let Ok(slot_hex) = std::str::from_utf8(&k[0..16]) {
                        if let Ok(slot_val) = u64::from_str_radix(slot_hex, 16) {
                            if slot_val > highest_finalized_slot {
                                batch.delete(&k);
                                deleted_count += 1;
                            }
                        }
                    }
                }
            }
        }
        let _ = self.db.write(batch);
        
        // (redundantly) clean up in-memory structures
        self.shreds.retain(|(slot, _), _| *slot <= highest_finalized_slot);
        self.slices.retain(|(slot, _), _| *slot <= highest_finalized_slot);
        self.blocks.retain(|(slot, _), _| *slot <= highest_finalized_slot);
        self.canonical.retain(|slot, _| *slot <= highest_finalized_slot);
        self.alternatives.retain(|slot, _| *slot <= highest_finalized_slot);
        self.first_shred_seen.retain(|slot| *slot <= highest_finalized_slot);
        self.double_merkle_trees.retain(|slot, _| *slot <= highest_finalized_slot);
        self.last_slices.retain(|slot, _| *slot <= highest_finalized_slot);
        self.merkle_root_cache.retain(|(slot, _), _| *slot <= highest_finalized_slot);
        
        println!("[Blockstore::clean_beyond_finalized] deleted {} blocks and {} metadata entries from DB, retained {} blocks in memory", 
                 deleted_count, deleted_meta_count, self.blocks.len());
    }
}

#[cfg(test)]
mod tests {
    use color_eyre::Result;
    use tokio::sync::mpsc;

    use super::*;
    use crate::ValidatorInfo;
    use crate::crypto::merkle::DoubleMerkleTree;
    use crate::crypto::signature::SecretKey;
    use crate::crypto::{Hash, aggsig};
    use crate::network::dontcare_sockaddr;
    use crate::shredder::{DATA_SHREDS, TOTAL_SHREDS};
    use crate::test_utils::create_random_shredded_block;
    use crate::types::SliceIndex;

    fn test_setup(tx: Sender<VotorEvent>) -> (SecretKey, BlockstoreImpl) {
        let sk = SecretKey::new(&mut rand::rng());
        let voting_sk = aggsig::SecretKey::new(&mut rand::rng());
        let info = ValidatorInfo {
            id: 0,
            stake: 1,
            pubkey: sk.to_pk(),
            voting_pubkey: voting_sk.to_pk(),
            all2all_address: dontcare_sockaddr(),
            disseminator_address: dontcare_sockaddr(),
            repair_request_address: dontcare_sockaddr(),
            repair_response_address: dontcare_sockaddr(),
        };
        let validators = vec![info];
        let epoch_info = EpochInfo::new(0, validators);
        (sk, BlockstoreImpl::new(Arc::new(epoch_info), tx))
    }

    async fn add_shred_ignore_duplicate(
        blockstore: &mut BlockstoreImpl,
        shred: Shred,
    ) -> Result<Option<BlockInfo>, AddShredError> {
        match blockstore.add_shred_from_disseminator(shred).await {
            Ok(output) => Ok(output),
            Err(AddShredError::Duplicate) => Ok(None),
            Err(e) => Err(e),
        }
    }

    #[tokio::test]
    async fn store_one_slice_block() -> Result<()> {
        let slot = Slot::genesis().next();
        let (tx, _rx) = mpsc::channel(100);
        let (sk, mut blockstore) = test_setup(tx);
        assert!(blockstore.slot_data(slot).is_none());

        // generate single-slice block
        let (block_hash, _, shreds) = create_random_shredded_block(slot, 1, &sk);
        let block_id = (slot, block_hash);

        let slice_hash = &shreds[0][0].merkle_root;
        for shred in &shreds[0] {
            // store shred
            add_shred_ignore_duplicate(&mut blockstore, shred.clone().into_shred()).await?;

            // check shred is stored
            let Some(stored_shred) = blockstore.get_disseminated_shred(
                slot,
                SliceIndex::first(),
                shred.payload().shred_index,
            ) else {
                panic!("shred not stored");
            };
            assert_eq!(stored_shred.payload().data, shred.payload().data);
        }

        // create and check double-Merkle proof
        let proof = blockstore
            .create_double_merkle_proof(&block_id, SliceIndex::first())
            .unwrap();
        let slot_data = blockstore.slot_data(slot).unwrap();
        let tree = slot_data.disseminated.double_merkle_tree.as_ref().unwrap();
        let root = tree.get_root();
        assert!(DoubleMerkleTree::check_proof(slice_hash, 0, &root, &proof));

        Ok(())
    }

    #[tokio::test]
    async fn store_two_slice_block() -> Result<()> {
        let slot = Slot::genesis().next();
        let (tx, _rx) = mpsc::channel(100);
        let (sk, mut blockstore) = test_setup(tx);
        assert!(blockstore.slot_data(slot).is_none());

        // generate two-slice block
        let (_hash, _tree, slices) = create_random_shredded_block(slot, 2, &sk);

        // first slice is not enough
        for shred in slices[0].clone() {
            add_shred_ignore_duplicate(&mut blockstore, shred.into_shred()).await?;
        }
        assert!(blockstore.disseminated_block_hash(slot).is_none());

        // after second slice we should have the block
        for shred in slices[1].clone() {
            add_shred_ignore_duplicate(&mut blockstore, shred.into_shred()).await?;
        }
        assert!(blockstore.disseminated_block_hash(slot).is_some());

        Ok(())
    }

    #[tokio::test]
    async fn store_block_from_repair() -> Result<()> {
        let slot = Slot::genesis().next();
        let (tx, _rx) = mpsc::channel(100);
        let (sk, mut blockstore) = test_setup(tx);
        assert!(blockstore.slot_data(slot).is_none());

        // generate and shred two slices
        let (block_hash, _tree, slices) = create_random_shredded_block(slot, 2, &sk);

        // first slice is not enough
        for shred in slices[0].clone().into_iter().take(DATA_SHREDS) {
            blockstore
                .add_shred_from_repair(block_hash.clone(), shred.into_shred())
                .await?;
        }
        assert!(blockstore.get_block(&(slot, block_hash.clone())).is_none());

        // after second slice we should have the block
        for shred in slices[1].clone().into_iter().take(DATA_SHREDS) {
            blockstore
                .add_shred_from_repair(block_hash.clone(), shred.into_shred())
                .await?;
        }
        assert!(blockstore.get_block(&(slot, block_hash)).is_some());

        Ok(())
    }

    #[tokio::test]
    async fn out_of_order_shreds() -> Result<()> {
        let slot = Slot::genesis().next();
        let (tx, _rx) = mpsc::channel(100);
        let (sk, mut blockstore) = test_setup(tx);
        assert!(blockstore.disseminated_block_hash(slot).is_none());

        // generate a single slice for slot 0
        let (_hash, _tree, slices) = create_random_shredded_block(slot, 1, &sk);

        // insert shreds in reverse order
        for shred in slices[0].clone().into_iter().rev() {
            add_shred_ignore_duplicate(&mut blockstore, shred.into_shred()).await?;
        }
        assert!(blockstore.disseminated_block_hash(slot).is_some());

        Ok(())
    }

    #[tokio::test]
    async fn just_enough_shreds() -> Result<()> {
        let slot = Slot::genesis().next();
        let (tx, _rx) = mpsc::channel(100);
        let (sk, mut blockstore) = test_setup(tx);
        assert!(blockstore.disseminated_block_hash(slot).is_none());

        // generate a larger block for slot 0
        let (_hash, _tree, slices) = create_random_shredded_block(slot, 4, &sk);
        assert_eq!(blockstore.stored_slices_for_slot(slot), 0);

        // insert just enough shreds to reconstruct slice 0 (from beginning)
        for shred in slices[0].clone().into_iter().take(DATA_SHREDS) {
            blockstore
                .add_shred_from_disseminator(shred.into_shred())
                .await?;
        }
        assert_eq!(blockstore.stored_slices_for_slot(slot), 1);

        // insert just enough shreds to reconstruct slice 1 (from end)
        for shred in slices[1]
            .clone()
            .into_iter()
            .skip(TOTAL_SHREDS - DATA_SHREDS)
        {
            blockstore
                .add_shred_from_disseminator(shred.into_shred())
                .await?;
        }
        assert_eq!(blockstore.stored_slices_for_slot(slot), 2);

        // insert just enough shreds to reconstruct slice 2 (from middle)
        for shred in slices[2]
            .clone()
            .into_iter()
            .skip((TOTAL_SHREDS - DATA_SHREDS) / 2)
            .take(DATA_SHREDS)
        {
            blockstore
                .add_shred_from_disseminator(shred.into_shred())
                .await?;
        }
        assert_eq!(blockstore.stored_slices_for_slot(slot), 3);

        // insert just enough shreds to reconstruct slice 3 (split)
        for (_, shred) in slices[3]
            .clone()
            .into_iter()
            .enumerate()
            .filter(|(i, _)| *i < DATA_SHREDS / 2 || *i >= TOTAL_SHREDS - DATA_SHREDS / 2)
        {
            blockstore
                .add_shred_from_disseminator(shred.into_shred())
                .await?;
        }
        assert!(blockstore.disseminated_block_hash(slot).is_some());

        // slices are deleted after reconstruction
        assert_eq!(blockstore.stored_slices_for_slot(slot), 0);

        Ok(())
    }

    #[tokio::test]
    async fn out_of_order_slices() -> Result<()> {
        let slot = Slot::genesis().next();
        let (tx, _rx) = mpsc::channel(100);
        let (sk, mut blockstore) = test_setup(tx);
        assert!(blockstore.disseminated_block_hash(slot).is_none());

        // generate two slices for slot 0
        let (_hash, _tree, slices) = create_random_shredded_block(slot, 2, &sk);

        // second slice alone is not enough
        for shred in slices[0].clone() {
            add_shred_ignore_duplicate(&mut blockstore, shred.into_shred()).await?;
        }
        assert!(blockstore.disseminated_block_hash(slot).is_none());

        // stored all shreds for slot 0
        assert_eq!(blockstore.stored_shreds_for_slot(slot), TOTAL_SHREDS);

        // after also also inserting first slice we should have the block
        for shred in slices[1].clone() {
            add_shred_ignore_duplicate(&mut blockstore, shred.into_shred()).await?;
        }
        assert!(blockstore.disseminated_block_hash(slot).is_some());

        // stored all shreds
        assert_eq!(blockstore.stored_shreds_for_slot(slot), 2 * TOTAL_SHREDS);

        Ok(())
    }

    #[tokio::test]
    async fn duplicate_shreds() -> Result<()> {
        let slot = Slot::genesis().next();
        let (tx, _rx) = mpsc::channel(100);
        let (sk, mut blockstore) = test_setup(tx);
        let (_hash, _tree, slices) = create_random_shredded_block(slot, 1, &sk);

        // inserting single shred should not throw errors
        let res = blockstore
            .add_shred_from_disseminator(slices[0][0].clone().into_shred())
            .await;
        assert!(res.is_ok());

        // inserting same shred again should give duplicate error
        let res = blockstore
            .add_shred_from_disseminator(slices[0][0].clone().into_shred())
            .await;
        assert_eq!(res, Err(AddShredError::Duplicate));

        // should only store one copy
        assert_eq!(blockstore.stored_shreds_for_slot(slot), 1);

        Ok(())
    }

    #[tokio::test]
    async fn invalid_shreds() -> Result<()> {
        let slot = Slot::genesis().next();
        let (tx, _rx) = mpsc::channel(100);
        let (sk, mut blockstore) = test_setup(tx);
        let (_hash, _tree, slices) = create_random_shredded_block(slot, 1, &sk);

        // insert shreds with wrong Merkle root
        for shred in slices[0].clone() {
            let mut shred = shred.into_shred();
            shred.merkle_root = Hash::random_for_test().into();
            let res = add_shred_ignore_duplicate(&mut blockstore, shred).await;
            assert!(res.is_err());
            assert_eq!(res.err(), Some(AddShredError::InvalidSignature));
        }

        Ok(())
    }

    #[tokio::test]
    async fn pruning() -> Result<()> {
        let block0_slot = Slot::genesis().next();
        let block1_slot = block0_slot.next();
        let block2_slot = block1_slot.next();
        let block3_slot = block2_slot.next();
        let future_slot = block3_slot.next();
        let (tx, _rx) = mpsc::channel(1000);
        let (sk, mut blockstore) = test_setup(tx);
        let block0 = create_random_shredded_block(block0_slot, 1, &sk);
        let block1 = create_random_shredded_block(block1_slot, 1, &sk);
        let block2 = create_random_shredded_block(block2_slot, 1, &sk);

        // insert shreds
        let mut shreds = vec![];
        shreds.extend(block0.2.into_iter().flatten());
        shreds.extend(block1.2.into_iter().flatten());
        shreds.extend(block2.2.into_iter().flatten());
        for shred in shreds {
            add_shred_ignore_duplicate(&mut blockstore, shred.into_shred()).await?;
        }
        assert!(blockstore.disseminated_block_hash(block0_slot).is_some());
        assert!(blockstore.disseminated_block_hash(block1_slot).is_some());
        assert!(blockstore.disseminated_block_hash(block2_slot).is_some());

        // stored all shreds
        assert_eq!(blockstore.stored_shreds_for_slot(block0_slot), TOTAL_SHREDS);
        assert_eq!(blockstore.stored_shreds_for_slot(block1_slot), TOTAL_SHREDS);
        assert_eq!(blockstore.stored_shreds_for_slot(block2_slot), TOTAL_SHREDS);

        // some (and only some) shreds deleted after partial pruning
        blockstore.prune(block1_slot);
        assert_eq!(blockstore.stored_shreds_for_slot(block0_slot), 0);
        assert_eq!(blockstore.stored_shreds_for_slot(block1_slot), TOTAL_SHREDS);
        assert_eq!(blockstore.stored_shreds_for_slot(block2_slot), TOTAL_SHREDS);

        // no shreds left after full pruning
        blockstore.prune(future_slot);
        assert_eq!(blockstore.stored_shreds_for_slot(block0_slot), 0);
        assert_eq!(blockstore.stored_shreds_for_slot(block1_slot), 0);
        assert_eq!(blockstore.stored_shreds_for_slot(block2_slot), 0);
        let shred_count = blockstore
            .block_data
            .values()
            .map(|d| {
                d.disseminated
                    .shreds
                    .values()
                    .map(|s| s.len())
                    .sum::<usize>()
            })
            .sum::<usize>();
        assert_eq!(shred_count, 0);

        Ok(())
    }
}
