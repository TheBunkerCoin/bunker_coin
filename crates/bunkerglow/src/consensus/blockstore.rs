// Copyright (c) Anza Technology, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Data structure holding blocks for each slot.

mod slot_block_data;

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use log::debug;
use mockall::automock;
use rocksdb::{DB, IteratorMode, Options, WriteBatch};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;

use self::slot_block_data::{AddShredError, SlotBlockData};
use super::epoch_info::EpochInfo;
use super::votor::VotorEvent;
use crate::consensus::blockstore::slot_block_data::BlockData;
use crate::crypto::Hash;
use crate::crypto::merkle::{BlockHash, DoubleMerkleProof, MerkleRoot, SliceRoot};
use crate::shredder::{RegularShredder, Shred, ShredIndex, ShredderPool, ValidatedShred};
use crate::types::SliceIndex;
use crate::{Block, BlockId, Slot};

/// Process-global RocksDB handles; reconnects must reuse per-path locks.
static DB_CACHE: std::sync::LazyLock<std::sync::Mutex<std::collections::HashMap<String, Arc<DB>>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

/// Opens RocksDB once per path, retrying only the first out-of-process open.
pub(crate) fn open_db_with_retry(opts: &Options, path: &str) -> Result<Arc<DB>, rocksdb::Error> {
    let mut cache = DB_CACHE.lock().unwrap();
    if let Some(db) = cache.get(path) {
        return Ok(db.clone());
    }
    const ATTEMPTS: u32 = 30;
    let mut last_err = None;
    for attempt in 0..ATTEMPTS {
        match DB::open(opts, path) {
            Ok(db) => {
                let db = Arc::new(db);
                cache.insert(path.to_owned(), db.clone());
                return Ok(db);
            }
            Err(e) => {
                if attempt + 1 < ATTEMPTS {
                    std::thread::sleep(std::time::Duration::from_millis(500));
                }
                last_err = Some(e);
            }
        }
    }
    Err(last_err.expect("at least one attempt"))
}

/// Metadata persisted alongside a block.
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

/// Recent finalized slots kept hot for repair; older blocks fall back to RocksDB.
const HOT_BLOCK_LIMIT: u64 = 200;

/// Blockstore is the fundamental data structure holding block data per slot.
pub struct BlockstoreImpl {
    block_data: BTreeMap<Slot, SlotBlockData>,
    shredders: ShredderPool<RegularShredder>,

    votor_channel: Sender<VotorEvent>,
    epoch_info: Arc<EpochInfo>,

    /// Cached shared RocksDB handle for durable block storage.
    db: Arc<DB>,
}

impl BlockstoreImpl {
    /// Initializes an empty blockstore and Votor event channel.
    pub fn new(epoch_info: Arc<EpochInfo>, votor_channel: Sender<VotorEvent>) -> Self {
        std::fs::create_dir_all("data").ok();
        let db_path = format!("data/blockstore/{}", epoch_info.own_id);
        std::fs::create_dir_all(&db_path).ok();

        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = open_db_with_retry(&opts, &db_path).expect("open RocksDB");

        Self {
            block_data: BTreeMap::new(),
            shredders: ShredderPool::with_size(1),
            votor_channel,
            epoch_info,
            db,
        }
    }

    /// Deletes in-memory block data before `slot`; RocksDB remains durable.
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
                // Persist block bytes and metadata so restarts, inspect, and RPC can serve them.
                let hash = block_info.hash.as_hash().clone();
                let block_id = (*slot, block_info.hash.clone());
                if let Some(block) = self.get_block(&block_id) {
                    self.persist_block(*slot, &hash, &block);
                }

                self.votor_channel.send(event).await.unwrap();

                Some(block_info)
            }
            ev => panic!("unexpected event {ev:?}"),
        }
    }

    /// Persists a completed block and base metadata; finalization fills timestamp later.
    fn persist_block(&self, slot: Slot, hash: &Hash, block: &Block) {
        let key = format!("{:016X}{}", slot, hex::encode(hash));
        if let Ok(value) = bincode::serde::encode_to_vec(block, bincode::config::standard()) {
            let _ = self.db.put(key.as_bytes(), value);
        }
        // Do not clobber a finalized timestamp with base metadata.
        if self.load_block_metadata(slot, hash.clone()).is_none() {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            let metadata = BlockMetadata {
                slot,
                hash: hash.clone(),
                producer: self.epoch_info.leader(slot).id,
                proposed_timestamp: now,
                finalized_timestamp: None,
            };
            let meta_key = format!("meta|{:016X}{}", slot, hex::encode(hash));
            if let Ok(value) = bincode::serde::encode_to_vec(&metadata, bincode::config::standard())
            {
                let _ = self.db.put(meta_key.as_bytes(), value);
            }
        }
    }

    /// Finds disseminated or repaired block data for `block_id`.
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

    fn slot_data(&self, slot: Slot) -> Option<&SlotBlockData> {
        self.block_data.get(&slot)
    }

    fn slot_data_mut(&mut self, slot: Slot) -> &mut SlotBlockData {
        self.block_data
            .entry(slot)
            .or_insert_with(|| SlotBlockData::new(slot))
    }

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

    #[cfg(test)]
    pub(crate) fn stored_slices_for_slot(&self, slot: Slot) -> usize {
        self.slot_data(slot)
            .map_or(0, |s| s.disseminated.slices.len())
    }
}

#[async_trait]
#[automock]
pub trait Blockstore {
    async fn add_shred_from_disseminator(
        &mut self,
        shred: Shred,
    ) -> Result<Option<BlockInfo>, AddShredError>;

    async fn add_shred_from_repair(
        &mut self,
        hash: BlockHash,
        shred: Shred,
    ) -> Result<Option<BlockInfo>, AddShredError>;

    fn disseminated_block_hash(&self, slot: Slot) -> Option<BlockHash>;

    fn get_block(&self, block_id: &BlockId) -> Option<Block>;

    fn get_last_slice_index(&self, block_id: &BlockId) -> Option<SliceIndex>;

    fn get_shred(
        &self,
        block_id: &BlockId,
        slice_index: SliceIndex,
        shred_index: ShredIndex,
    ) -> Option<ValidatedShred>;

    fn get_slice_root(&self, block_id: &BlockId, slice_index: SliceIndex) -> Option<SliceRoot>;

    fn create_double_merkle_proof(
        &self,
        block_id: &BlockId,
        slice_index: SliceIndex,
    ) -> Option<DoubleMerkleProof>;

    fn canonical_block_hash(&self, slot: Slot) -> Option<Hash>;

    fn load_block_from_db(&self, slot: Slot, hash: Hash) -> Option<Block>;

    fn load_block_by_hash(&self, hash: Hash) -> Option<(Slot, Block)>;

    fn load_block_metadata(&self, slot: Slot, hash: Hash) -> Option<BlockMetadata>;

    fn update_finalized_timestamp(&self, slot: Slot, hash: Hash, timestamp: u64);

    fn clean_beyond_finalized(&mut self, highest_finalized_slot: Slot);

    /// Drops cold in-memory finalized slots while keeping RocksDB as the fallback.
    fn prune_finalized(&mut self, finalized_slot: Slot);
}

#[async_trait]
impl Blockstore for BlockstoreImpl {
    /// Stores a disseminated shred, checking leader equivocation and reconstructing if possible.
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

    /// Stores a repair shred under a known block hash and reconstructs if possible.
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

    /// Returns the disseminated block hash for `slot`, excluding repair-only blocks.
    fn disseminated_block_hash(&self, slot: Slot) -> Option<BlockHash> {
        self.slot_data(slot)?
            .disseminated
            .completed
            .as_ref()
            .map(|c| c.0.clone())
    }

    fn get_block(&self, block_id: &BlockId) -> Option<Block> {
        if let Some(block_data) = self.get_block_data(block_id)
            && let Some((hash, block)) = block_data.completed.as_ref()
        {
            debug_assert_eq!(*hash, block_id.1);
            return Some(block.clone());
        }
        // In-memory data may be empty after restart or inspect; fall back to RocksDB.
        let (slot, hash) = block_id;
        self.load_block_from_db(*slot, hash.as_hash().clone())
    }

    /// Returns the last slice index once known.
    fn get_last_slice_index(&self, block_id: &BlockId) -> Option<SliceIndex> {
        let block_data = self.get_block_data(block_id)?;
        block_data.last_slice
    }

    /// Returns a stored shred by block, slice, and shred index.
    fn get_shred(
        &self,
        block_id: &BlockId,
        slice_index: SliceIndex,
        shred_index: ShredIndex,
    ) -> Option<ValidatedShred> {
        let block_data = self.get_block_data(block_id)?;
        let slice_shreds = block_data.shreds.get(&slice_index)?;
        slice_shreds[*shred_index].clone()
    }

    /// Builds a double-Merkle proof for a stored block slice.
    fn create_double_merkle_proof(
        &self,
        block_id: &BlockId,
        slice_index: SliceIndex,
    ) -> Option<DoubleMerkleProof> {
        let block_data = self.get_block_data(block_id)?;
        let tree = block_data.double_merkle_tree.as_ref()?;
        Some(tree.create_proof(slice_index.inner()))
    }

    fn get_slice_root(&self, block_id: &BlockId, slice_index: SliceIndex) -> Option<SliceRoot> {
        let block_data = self.get_block_data(block_id)?;
        block_data.merkle_root_cache.get(&slice_index).cloned()
    }

    fn load_block_from_db(&self, slot: Slot, hash: Hash) -> Option<Block> {
        let key = format!("{:016X}{}", slot, hex::encode(hash));
        if let Ok(Some(val)) = self.db.get(key.as_bytes())
            && let Ok((block, _)) =
                bincode::serde::decode_from_slice::<Block, _>(&val, bincode::config::standard())
        {
            return Some(block);
        }
        None
    }

    fn canonical_block_hash(&self, slot: Slot) -> Option<Hash> {
        if let Some(bh) = self.disseminated_block_hash(slot) {
            return Some(bh.as_hash().clone());
        }
        // In-memory may be empty; persisted metadata encodes the slot's canonical hash.
        let prefix = format!("meta|{:016X}", slot);
        let prefix_bytes = prefix.as_bytes();
        for item in self.db.prefix_iterator(prefix_bytes).flatten() {
            let (k, _) = item;
            if !k.starts_with(prefix_bytes) {
                break;
            }
            // `meta|{slot}{hash}` stores the canonical hash suffix.
            let hex_hash = &k[prefix_bytes.len()..];
            if let Ok(bytes) = hex::decode(hex_hash)
                && let Ok(arr) = <[u8; 32]>::try_from(bytes.as_slice())
            {
                return Some(Hash::from(arr));
            }
        }
        None
    }

    fn load_block_by_hash(&self, hash: Hash) -> Option<(Slot, Block)> {
        let suffix = hex::encode(hash);
        let suffix_bytes = suffix.as_bytes();
        for item in self.db.iterator(IteratorMode::Start) {
            if let Ok((k, v)) = item
                && k.len() >= 16 + suffix_bytes.len()
                && &k[k.len() - suffix_bytes.len()..] == suffix_bytes
            {
                let slot_str = std::str::from_utf8(&k[0..16]).ok()?;
                let slot = Slot::new(u64::from_str_radix(slot_str, 16).ok()?);
                if let Ok((block, _)) =
                    bincode::serde::decode_from_slice::<Block, _>(&v, bincode::config::standard())
                {
                    return Some((slot, block));
                }
            }
        }
        None
    }

    fn load_block_metadata(&self, slot: Slot, hash: Hash) -> Option<BlockMetadata> {
        let key = format!("meta|{:016X}{}", slot, hex::encode(hash));
        if let Ok(Some(val)) = self.db.get(key.as_bytes())
            && let Ok((metadata, _)) = bincode::serde::decode_from_slice::<BlockMetadata, _>(
                &val,
                bincode::config::standard(),
            )
        {
            return Some(metadata);
        }
        None
    }

    fn update_finalized_timestamp(&self, slot: Slot, hash: Hash, timestamp: u64) {
        if let Some(mut metadata) = self.load_block_metadata(slot, hash.clone()) {
            metadata.finalized_timestamp = Some(timestamp);
            let key = format!("meta|{:016X}{}", slot, hex::encode(&hash));
            if let Ok(value) = bincode::serde::encode_to_vec(&metadata, bincode::config::standard())
            {
                let _ = self.db.put(key.as_bytes(), value);
            }
        }
    }

    fn prune_finalized(&mut self, finalized_slot: Slot) {
        let cutoff = Slot::new(finalized_slot.inner().saturating_sub(HOT_BLOCK_LIMIT));
        self.prune(cutoff);
    }

    fn clean_beyond_finalized(&mut self, highest_finalized_slot: Slot) {
        println!(
            "[Blockstore::clean_beyond_finalized] pruning blocks beyond slot {}",
            highest_finalized_slot
        );

        let mut batch = WriteBatch::default();
        let mut deleted_count = 0;
        let mut deleted_meta_count = 0;
        for (k, _v) in self.db.iterator(IteratorMode::Start).flatten() {
            let finalized = highest_finalized_slot.inner();
            if k.starts_with(b"meta|") {
                if k.len() >= 21
                    && let Ok(slot_hex) = std::str::from_utf8(&k[5..21])
                    && let Ok(slot_val) = u64::from_str_radix(slot_hex, 16)
                    && slot_val > finalized
                {
                    batch.delete(&k);
                    deleted_meta_count += 1;
                }
            } else if k.len() >= 16
                && let Ok(slot_hex) = std::str::from_utf8(&k[0..16])
                && let Ok(slot_val) = u64::from_str_radix(slot_hex, 16)
                && slot_val > finalized
            {
                batch.delete(&k);
                deleted_count += 1;
            }
        }
        let _ = self.db.write(batch);

        let beyond = self.block_data.split_off(&highest_finalized_slot.next());
        let pruned = beyond.len();
        drop(beyond);

        println!(
            "[Blockstore::clean_beyond_finalized] deleted {} blocks and {} metadata entries from DB, pruned {} in-memory slots",
            deleted_count, deleted_meta_count, pruned
        );
    }
}

#[cfg(test)]
mod tests {
    use color_eyre::Result;
    use tokio::sync::mpsc;

    use super::*;

    /// Same-process DB reopens must hit the shared handle cache.
    #[test]
    fn open_db_with_retry_reuses_handle_for_same_path() {
        let dir = std::env::temp_dir().join(format!("bunker_db_cache_test_{}", std::process::id()));
        let path = dir.to_str().unwrap();
        let mut opts = Options::default();
        opts.create_if_missing(true);

        let a = open_db_with_retry(&opts, path).expect("first open");
        let b = open_db_with_retry(&opts, path).expect("second open of same path");
        assert!(Arc::ptr_eq(&a, &b));

        a.put(b"k", b"v").unwrap();
        assert_eq!(b.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));

        let _ = std::fs::remove_dir_all(&dir);
    }
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
            location: None,
        };
        let validators = vec![info];
        let epoch_info = EpochInfo::new(0, 0, validators);
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

        let (block_hash, _, shreds) = create_random_shredded_block(slot, 1, &sk);
        let block_id = (slot, block_hash);

        let slice_hash = &shreds[0][0].merkle_root;
        for shred in &shreds[0] {
            add_shred_ignore_duplicate(&mut blockstore, shred.clone().into_shred()).await?;

            let Some(stored_shred) = blockstore.get_disseminated_shred(
                slot,
                SliceIndex::first(),
                shred.payload().shred_index,
            ) else {
                panic!("shred not stored");
            };
            assert_eq!(stored_shred.payload().data, shred.payload().data);
        }

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

        let (_hash, _tree, slices) = create_random_shredded_block(slot, 2, &sk);

        for shred in slices[0].clone() {
            add_shred_ignore_duplicate(&mut blockstore, shred.into_shred()).await?;
        }
        assert!(blockstore.disseminated_block_hash(slot).is_none());

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

        let (block_hash, _tree, slices) = create_random_shredded_block(slot, 2, &sk);

        for shred in slices[0].clone().into_iter().take(DATA_SHREDS) {
            blockstore
                .add_shred_from_repair(block_hash.clone(), shred.into_shred())
                .await?;
        }
        assert!(blockstore.get_block(&(slot, block_hash.clone())).is_none());

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

        let (_hash, _tree, slices) = create_random_shredded_block(slot, 1, &sk);

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

        let (_hash, _tree, slices) = create_random_shredded_block(slot, 4, &sk);
        assert_eq!(blockstore.stored_slices_for_slot(slot), 0);

        for shred in slices[0].clone().into_iter().take(DATA_SHREDS) {
            blockstore
                .add_shred_from_disseminator(shred.into_shred())
                .await?;
        }
        assert_eq!(blockstore.stored_slices_for_slot(slot), 1);

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

        assert_eq!(blockstore.stored_slices_for_slot(slot), 0);

        Ok(())
    }

    #[tokio::test]
    async fn out_of_order_slices() -> Result<()> {
        let slot = Slot::genesis().next();
        let (tx, _rx) = mpsc::channel(100);
        let (sk, mut blockstore) = test_setup(tx);
        assert!(blockstore.disseminated_block_hash(slot).is_none());

        let (_hash, _tree, slices) = create_random_shredded_block(slot, 2, &sk);

        for shred in slices[0].clone() {
            add_shred_ignore_duplicate(&mut blockstore, shred.into_shred()).await?;
        }
        assert!(blockstore.disseminated_block_hash(slot).is_none());

        assert_eq!(blockstore.stored_shreds_for_slot(slot), TOTAL_SHREDS);

        for shred in slices[1].clone() {
            add_shred_ignore_duplicate(&mut blockstore, shred.into_shred()).await?;
        }
        assert!(blockstore.disseminated_block_hash(slot).is_some());

        assert_eq!(blockstore.stored_shreds_for_slot(slot), 2 * TOTAL_SHREDS);

        Ok(())
    }

    #[tokio::test]
    async fn duplicate_shreds() -> Result<()> {
        let slot = Slot::genesis().next();
        let (tx, _rx) = mpsc::channel(100);
        let (sk, mut blockstore) = test_setup(tx);
        let (_hash, _tree, slices) = create_random_shredded_block(slot, 1, &sk);

        let res = blockstore
            .add_shred_from_disseminator(slices[0][0].clone().into_shred())
            .await;
        assert!(res.is_ok());

        let res = blockstore
            .add_shred_from_disseminator(slices[0][0].clone().into_shred())
            .await;
        assert_eq!(res, Err(AddShredError::Duplicate));

        assert_eq!(blockstore.stored_shreds_for_slot(slot), 1);

        Ok(())
    }

    #[tokio::test]
    async fn invalid_shreds() -> Result<()> {
        let slot = Slot::genesis().next();
        let (tx, _rx) = mpsc::channel(100);
        let (sk, mut blockstore) = test_setup(tx);
        let (_hash, _tree, slices) = create_random_shredded_block(slot, 1, &sk);

        for shred in slices[0].clone() {
            let mut shred = shred.into_shred();
            shred.merkle_root = Hash::random_for_test().into();
            let res = add_shred_ignore_duplicate(&mut blockstore, shred).await;
            assert!(res.is_err());
            assert_eq!(res.err(), Some(AddShredError::InvalidSignature));
        }

        Ok(())
    }

    /// Cold finalized slots are pruned from memory but remain readable from RocksDB.
    #[tokio::test]
    async fn prune_finalized_keeps_hot_window_and_db_fallback() -> Result<()> {
        let old_slot = Slot::new(250);
        let new_slot = Slot::new(451);
        let (tx, _rx) = mpsc::channel(1000);
        let (sk, mut blockstore) = test_setup(tx);
        let old_block = create_random_shredded_block(old_slot, 1, &sk);
        let new_block = create_random_shredded_block(new_slot, 1, &sk);
        let mut shreds = vec![];
        shreds.extend(old_block.2.into_iter().flatten());
        shreds.extend(new_block.2.into_iter().flatten());
        for shred in shreds {
            add_shred_ignore_duplicate(&mut blockstore, shred.into_shred()).await?;
        }
        let old_hash = blockstore.disseminated_block_hash(old_slot).unwrap();
        assert!(blockstore.stored_shreds_for_slot(old_slot) > 0);
        assert!(blockstore.stored_shreds_for_slot(new_slot) > 0);

        blockstore.prune_finalized(new_slot);

        assert_eq!(blockstore.stored_shreds_for_slot(old_slot), 0);
        assert!(blockstore.stored_shreds_for_slot(new_slot) > 0);
        assert!(
            blockstore.get_block(&(old_slot, old_hash)).is_some(),
            "pruned block must remain readable via the DB fallback"
        );

        blockstore.prune_finalized(Slot::new(100));
        assert!(blockstore.stored_shreds_for_slot(new_slot) > 0);
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

        assert_eq!(blockstore.stored_shreds_for_slot(block0_slot), TOTAL_SHREDS);
        assert_eq!(blockstore.stored_shreds_for_slot(block1_slot), TOTAL_SHREDS);
        assert_eq!(blockstore.stored_shreds_for_slot(block2_slot), TOTAL_SHREDS);

        blockstore.prune(block1_slot);
        assert_eq!(blockstore.stored_shreds_for_slot(block0_slot), 0);
        assert_eq!(blockstore.stored_shreds_for_slot(block1_slot), TOTAL_SHREDS);
        assert_eq!(blockstore.stored_shreds_for_slot(block2_slot), TOTAL_SHREDS);

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
