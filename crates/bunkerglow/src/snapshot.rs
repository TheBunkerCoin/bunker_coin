use std::collections::BTreeMap;

use bunker_coin_core::epoch_transition::EpochTransitionBlock;
use bunker_coin_core::execution::State;
use log::info;
use rocksdb::{DB, IteratorMode, Options};
use serde::{Deserialize, Serialize};

use crate::crypto::Hash;
use crate::crypto::merkle::PlainMerkleTree;

pub const DEFAULT_SNAPSHOT_CHUNK_SIZE: usize = 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotManifest {
    pub epoch: u64,
    pub state_hash: [u8; 32],
    pub chunk_root: Hash,
    pub chunk_size: usize,
    pub total_bytes: usize,
    pub chunk_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotChunk {
    pub epoch: u64,
    pub index: usize,
    pub data: Vec<u8>,
    pub proof: Vec<Hash>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BootstrapError {
    EmptySnapshot,
    InvalidChunkSize,
    WrongChunkCount { expected: usize, got: usize },
    WrongEpoch { expected: u64, got: u64 },
    DuplicateChunk(usize),
    MissingChunk(usize),
    InvalidChunkProof(usize),
    TotalSizeMismatch { expected: usize, got: usize },
    DecodeFailed,
    StateHashMismatch { expected: [u8; 32], got: [u8; 32] },
}

impl std::fmt::Display for BootstrapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptySnapshot => write!(f, "snapshot contains no chunks"),
            Self::InvalidChunkSize => write!(f, "snapshot chunk size must be non-zero"),
            Self::WrongChunkCount { expected, got } => {
                write!(f, "wrong chunk count: expected {expected}, got {got}")
            }
            Self::WrongEpoch { expected, got } => {
                write!(f, "wrong chunk epoch: expected {expected}, got {got}")
            }
            Self::DuplicateChunk(index) => write!(f, "duplicate snapshot chunk {index}"),
            Self::MissingChunk(index) => write!(f, "missing snapshot chunk {index}"),
            Self::InvalidChunkProof(index) => write!(f, "invalid proof for snapshot chunk {index}"),
            Self::TotalSizeMismatch { expected, got } => {
                write!(
                    f,
                    "wrong snapshot byte length: expected {expected}, got {got}"
                )
            }
            Self::DecodeFailed => write!(f, "failed to decode snapshot state"),
            Self::StateHashMismatch { expected, got } => {
                write!(
                    f,
                    "snapshot state hash mismatch: expected {expected:x?}, got {got:x?}"
                )
            }
        }
    }
}

impl std::error::Error for BootstrapError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapSnapshot {
    pub manifest: SnapshotManifest,
    pub chunks: Vec<SnapshotChunk>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotCheckpoint {
    pub epoch: u64,
    pub finalized_slot: u64,
    pub transition_block: EpochTransitionBlock,
    pub finalization_certs: Vec<Vec<u8>>,
}

impl SnapshotCheckpoint {
    pub fn matches_manifest(&self, manifest: &SnapshotManifest) -> bool {
        let block = &self.transition_block;
        self.epoch == manifest.epoch
            && block.epoch == manifest.epoch
            && block.state_hash == manifest.state_hash
            && block.snapshot_chunk_root == manifest.chunk_root.as_ref()
            && block.snapshot_chunk_count == manifest.chunk_count
            && block.snapshot_total_bytes == manifest.total_bytes
            && block.snapshot_chunk_size == manifest.chunk_size
            && !self.finalization_certs.is_empty()
    }
}

pub struct SnapshotStore {
    db: DB,
}

impl SnapshotStore {
    pub fn new(node_id: u64) -> Self {
        let path = format!("data/snapshots/{node_id}");
        Self::open_at(path)
    }

    fn open_at(path: String) -> Self {
        std::fs::create_dir_all(&path).ok();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, path).expect("open RocksDB snapshot db");
        Self { db }
    }

    pub fn save_snapshot(&self, epoch: u64, state: &State) {
        let key = format!("snapshot|{epoch:016X}");
        match bincode::serde::encode_to_vec(state, bincode::config::standard()) {
            Ok(data) => {
                let _ = self.db.put(key.as_bytes(), data);
                info!("snapshot saved for epoch {epoch}");
            }
            Err(e) => {
                info!("failed to serialize snapshot for epoch {epoch}: {e}");
            }
        }

        match build_bootstrap_snapshot(epoch, state, DEFAULT_SNAPSHOT_CHUNK_SIZE) {
            Ok(snapshot) => self.save_bootstrap_snapshot(&snapshot),
            Err(e) => info!("failed to build bootstrap snapshot for epoch {epoch}: {e}"),
        }
    }

    pub fn load_snapshot(&self, epoch: u64) -> Option<State> {
        let key = format!("snapshot|{epoch:016X}");
        let data = self.db.get(key.as_bytes()).ok()??;
        bincode::serde::decode_from_slice::<State, _>(&data, bincode::config::standard())
            .ok()
            .map(|(state, _)| state)
    }

    pub fn latest_snapshot(&self) -> Option<(u64, State)> {
        let iter = self.db.iterator(IteratorMode::End);
        for item in iter {
            let (key, val) = item.ok()?;
            let key_str = std::str::from_utf8(&key).ok()?;
            if let Some(hex_str) = key_str.strip_prefix("snapshot|") {
                let epoch = u64::from_str_radix(hex_str, 16).ok()?;
                if let Ok((state, _)) =
                    bincode::serde::decode_from_slice::<State, _>(&val, bincode::config::standard())
                {
                    return Some((epoch, state));
                }
            }
        }
        None
    }

    pub fn load_manifest(&self, epoch: u64) -> Option<SnapshotManifest> {
        let key = manifest_key(epoch);
        let data = self.db.get(key.as_bytes()).ok()??;
        bincode::serde::decode_from_slice::<SnapshotManifest, _>(&data, bincode::config::standard())
            .ok()
            .map(|(manifest, _)| manifest)
    }

    pub fn latest_manifest(&self) -> Option<SnapshotManifest> {
        let iter = self.db.iterator(IteratorMode::End);
        for item in iter {
            let (key, val) = item.ok()?;
            let key_str = std::str::from_utf8(&key).ok()?;
            if key_str.starts_with("snapshot_manifest|")
                && let Ok((manifest, _)) = bincode::serde::decode_from_slice::<SnapshotManifest, _>(
                    &val,
                    bincode::config::standard(),
                )
                && self.load_checkpoint(manifest.epoch).is_some()
            {
                return Some(manifest);
            }
        }
        None
    }

    pub fn load_chunk(&self, epoch: u64, index: usize) -> Option<SnapshotChunk> {
        let key = chunk_key(epoch, index);
        let data = self.db.get(key.as_bytes()).ok()??;
        bincode::serde::decode_from_slice::<SnapshotChunk, _>(&data, bincode::config::standard())
            .ok()
            .map(|(chunk, _)| chunk)
    }

    pub fn load_bootstrap_snapshot(&self, epoch: u64) -> Option<BootstrapSnapshot> {
        let manifest = self.load_manifest(epoch)?;
        self.load_checkpoint(epoch)?;
        let mut chunks = Vec::with_capacity(manifest.chunk_count);
        for index in 0..manifest.chunk_count {
            chunks.push(self.load_chunk(epoch, index)?);
        }
        Some(BootstrapSnapshot { manifest, chunks })
    }

    pub fn bootstrap_state(&self, manifest: &SnapshotManifest) -> Result<State, BootstrapError> {
        let mut chunks = Vec::with_capacity(manifest.chunk_count);
        for index in 0..manifest.chunk_count {
            let chunk = self
                .load_chunk(manifest.epoch, index)
                .ok_or(BootstrapError::MissingChunk(index))?;
            chunks.push(chunk);
        }
        restore_state_from_chunks(manifest, chunks)
    }

    pub fn save_checkpoint(
        &self,
        checkpoint: SnapshotCheckpoint,
    ) -> Result<(), bincode::error::EncodeError> {
        let key = checkpoint_key(checkpoint.epoch);
        let data = bincode::serde::encode_to_vec(&checkpoint, bincode::config::standard())?;
        let _ = self.db.put(key.as_bytes(), data);
        Ok(())
    }

    pub fn load_checkpoint(&self, epoch: u64) -> Option<SnapshotCheckpoint> {
        let key = checkpoint_key(epoch);
        let data = self.db.get(key.as_bytes()).ok()??;
        bincode::serde::decode_from_slice::<SnapshotCheckpoint, _>(
            &data,
            bincode::config::standard(),
        )
        .ok()
        .map(|(checkpoint, _)| checkpoint)
    }

    pub fn prune_old_snapshots(&self, keep_latest_n: usize) {
        let iter = self.db.iterator(IteratorMode::End);
        let mut epochs: Vec<u64> = Vec::new();
        for item in iter {
            if let Ok((key, _)) = item {
                let key_str = std::str::from_utf8(&key).unwrap_or("");
                if let Some(hex_str) = key_str.strip_prefix("snapshot|") {
                    if let Ok(epoch) = u64::from_str_radix(hex_str, 16) {
                        epochs.push(epoch);
                    }
                }
            }
        }
        epochs.sort_unstable_by(|a, b| b.cmp(a));

        if epochs.len() > keep_latest_n {
            let old_epochs = &epochs[keep_latest_n..];
            let mut keys_to_delete = Vec::new();
            let iter = self.db.iterator(IteratorMode::Start);
            for item in iter {
                if let Ok((key, _)) = item {
                    let key_str = std::str::from_utf8(&key).unwrap_or("");
                    if old_epochs
                        .iter()
                        .any(|epoch| snapshot_key_matches_epoch(key_str, *epoch))
                    {
                        keys_to_delete.push(key.to_vec());
                    }
                }
            }

            for key in &keys_to_delete {
                let _ = self.db.delete(key);
            }
            info!(
                "pruned {} old snapshots, keeping {keep_latest_n}",
                epochs.len() - keep_latest_n
            );
        }
    }

    fn save_bootstrap_snapshot(&self, snapshot: &BootstrapSnapshot) {
        let manifest_key = manifest_key(snapshot.manifest.epoch);
        if let Ok(data) =
            bincode::serde::encode_to_vec(&snapshot.manifest, bincode::config::standard())
        {
            let _ = self.db.put(manifest_key.as_bytes(), data);
        }

        for chunk in &snapshot.chunks {
            if let Ok(data) = bincode::serde::encode_to_vec(chunk, bincode::config::standard()) {
                let key = chunk_key(chunk.epoch, chunk.index);
                let _ = self.db.put(key.as_bytes(), data);
            }
        }
    }
}

pub fn build_bootstrap_snapshot(
    epoch: u64,
    state: &State,
    chunk_size: usize,
) -> Result<BootstrapSnapshot, BootstrapError> {
    if chunk_size == 0 {
        return Err(BootstrapError::InvalidChunkSize);
    }

    let encoded = bincode::serde::encode_to_vec(state, bincode::config::standard())
        .map_err(|_| BootstrapError::DecodeFailed)?;
    if encoded.is_empty() {
        return Err(BootstrapError::EmptySnapshot);
    }

    let leaves: Vec<Vec<u8>> = encoded
        .chunks(chunk_size)
        .map(|chunk| chunk.to_vec())
        .collect();
    let tree = PlainMerkleTree::new(&leaves);
    let manifest = SnapshotManifest {
        epoch,
        state_hash: state.compute_state_hash(),
        chunk_root: tree.get_root(),
        chunk_size,
        total_bytes: encoded.len(),
        chunk_count: leaves.len(),
    };
    let chunks = leaves
        .into_iter()
        .enumerate()
        .map(|(index, data)| SnapshotChunk {
            epoch,
            index,
            data,
            proof: tree.create_proof(index),
        })
        .collect();

    Ok(BootstrapSnapshot { manifest, chunks })
}

pub fn restore_state_from_chunks(
    manifest: &SnapshotManifest,
    chunks: impl IntoIterator<Item = SnapshotChunk>,
) -> Result<State, BootstrapError> {
    if manifest.chunk_size == 0 {
        return Err(BootstrapError::InvalidChunkSize);
    }
    if manifest.chunk_count == 0 {
        return Err(BootstrapError::EmptySnapshot);
    }

    let mut by_index = BTreeMap::new();
    for chunk in chunks {
        if chunk.epoch != manifest.epoch {
            return Err(BootstrapError::WrongEpoch {
                expected: manifest.epoch,
                got: chunk.epoch,
            });
        }
        if chunk.index >= manifest.chunk_count {
            return Err(BootstrapError::WrongChunkCount {
                expected: manifest.chunk_count,
                got: chunk.index + 1,
            });
        }
        let index = chunk.index;
        if by_index.insert(index, chunk).is_some() {
            return Err(BootstrapError::DuplicateChunk(index));
        }
    }

    if by_index.len() != manifest.chunk_count {
        return Err(BootstrapError::WrongChunkCount {
            expected: manifest.chunk_count,
            got: by_index.len(),
        });
    }

    let mut encoded = Vec::with_capacity(manifest.total_bytes);
    for index in 0..manifest.chunk_count {
        let chunk = by_index
            .remove(&index)
            .ok_or(BootstrapError::MissingChunk(index))?;
        if !PlainMerkleTree::check_proof(&chunk.data, index, &manifest.chunk_root, &chunk.proof) {
            return Err(BootstrapError::InvalidChunkProof(index));
        }
        encoded.extend_from_slice(&chunk.data);
    }

    if encoded.len() != manifest.total_bytes {
        return Err(BootstrapError::TotalSizeMismatch {
            expected: manifest.total_bytes,
            got: encoded.len(),
        });
    }

    let (state, _) =
        bincode::serde::decode_from_slice::<State, _>(&encoded, bincode::config::standard())
            .map_err(|_| BootstrapError::DecodeFailed)?;

    let got = state.compute_state_hash();
    if got != manifest.state_hash {
        return Err(BootstrapError::StateHashMismatch {
            expected: manifest.state_hash,
            got,
        });
    }

    Ok(state)
}

fn manifest_key(epoch: u64) -> String {
    format!("snapshot_manifest|{epoch:016X}")
}

fn chunk_key(epoch: u64, index: usize) -> String {
    format!("snapshot_chunk|{epoch:016X}|{index:016X}")
}

fn checkpoint_key(epoch: u64) -> String {
    format!("snapshot_checkpoint|{epoch:016X}")
}

fn snapshot_key_matches_epoch(key: &str, epoch: u64) -> bool {
    let epoch_hex = format!("{epoch:016X}");
    key == format!("snapshot|{epoch_hex}")
        || key == format!("snapshot_manifest|{epoch_hex}")
        || key == format!("snapshot_checkpoint|{epoch_hex}")
        || key.starts_with(&format!("snapshot_chunk|{epoch_hex}|"))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    use bunker_coin_core::staking::StakingLedger;

    use super::*;

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_store() -> (SnapshotStore, String) {
        let id = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = format!("/tmp/bunker_snapshot_test_{id}_{}", std::process::id());
        let store = SnapshotStore::open_at(path.clone());
        (store, path)
    }

    fn default_state() -> State {
        State {
            accounts: HashMap::new(),
            tokens: HashMap::new(),
            next_token_id: 1,
            tx_fee_pool: 0,
            msg_fee_pool: 0,
            bridge_fee_pool: 0,
            staking: StakingLedger::new(),
            current_epoch: 0,
        }
    }

    fn cleanup(path: &str) {
        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn save_load_roundtrip() {
        let (store, path) = temp_store();
        let mut state = default_state();
        state.current_epoch = 5;
        state.tx_fee_pool = 42_000;

        store.save_snapshot(5, &state);
        let loaded = store.load_snapshot(5).unwrap();
        assert_eq!(loaded.current_epoch, 5);
        assert_eq!(loaded.tx_fee_pool, 42_000);
        cleanup(&path);
    }

    #[test]
    fn load_nonexistent_returns_none() {
        let (store, path) = temp_store();
        assert!(store.load_snapshot(999).is_none());
        cleanup(&path);
    }

    #[test]
    fn latest_snapshot_returns_highest_epoch() {
        let (store, path) = temp_store();
        for epoch in [1, 5, 3, 10, 7] {
            let mut state = default_state();
            state.current_epoch = epoch;
            store.save_snapshot(epoch, &state);
        }

        let (epoch, state) = store.latest_snapshot().unwrap();
        assert_eq!(epoch, 10);
        assert_eq!(state.current_epoch, 10);
        cleanup(&path);
    }

    #[test]
    fn latest_snapshot_empty_returns_none() {
        let (store, path) = temp_store();
        assert!(store.latest_snapshot().is_none());
        cleanup(&path);
    }

    #[test]
    fn prune_keeps_latest_n() {
        let (store, path) = temp_store();
        for epoch in 1..=5 {
            let mut state = default_state();
            state.current_epoch = epoch;
            store.save_snapshot(epoch, &state);
        }

        store.prune_old_snapshots(2);

        // latest 2 (epochs 5, 4) should remain
        assert!(store.load_snapshot(5).is_some());
        assert!(store.load_snapshot(4).is_some());
        // older ones pruned
        assert!(store.load_snapshot(3).is_none());
        assert!(store.load_snapshot(2).is_none());
        assert!(store.load_snapshot(1).is_none());
        assert!(store.load_manifest(3).is_none());
        assert!(store.load_chunk(3, 0).is_none());
        cleanup(&path);
    }

    #[test]
    fn prune_noop_when_fewer_than_n() {
        let (store, path) = temp_store();
        let state = default_state();
        store.save_snapshot(1, &state);
        store.save_snapshot(2, &state);

        store.prune_old_snapshots(5);
        assert!(store.load_snapshot(1).is_some());
        assert!(store.load_snapshot(2).is_some());
        cleanup(&path);
    }

    #[test]
    fn overwrite_same_epoch() {
        let (store, path) = temp_store();
        let mut state = default_state();
        state.tx_fee_pool = 100;
        store.save_snapshot(1, &state);

        state.tx_fee_pool = 200;
        store.save_snapshot(1, &state);

        let loaded = store.load_snapshot(1).unwrap();
        assert_eq!(loaded.tx_fee_pool, 200);
        cleanup(&path);
    }

    #[test]
    fn snapshot_preserves_accounts() {
        let (store, path) = temp_store();
        let mut state = default_state();
        let pk = [42u8; 32];
        state.accounts.insert(
            pk,
            bunker_coin_core::account::Account {
                native_balance: 1_000_000,
                token_balances: std::collections::BTreeMap::new(),
                nonce: 7,
            },
        );

        store.save_snapshot(1, &state);
        let loaded = store.load_snapshot(1).unwrap();
        let acc = loaded.accounts.get(&pk).unwrap();
        assert_eq!(acc.native_balance, 1_000_000);
        assert_eq!(acc.nonce, 7);
        cleanup(&path);
    }

    #[test]
    fn bootstrap_snapshot_roundtrip() {
        let mut state = default_state();
        state.current_epoch = 9;
        state.tx_fee_pool = 123;
        state.accounts.insert(
            [1u8; 32],
            bunker_coin_core::account::Account {
                native_balance: 55,
                token_balances: std::collections::BTreeMap::new(),
                nonce: 2,
            },
        );

        let snapshot = build_bootstrap_snapshot(9, &state, 16).unwrap();
        assert!(snapshot.manifest.chunk_count > 1);
        assert_eq!(snapshot.manifest.state_hash, state.compute_state_hash());

        let restored =
            restore_state_from_chunks(&snapshot.manifest, snapshot.chunks.clone()).unwrap();
        assert_eq!(restored.compute_state_hash(), state.compute_state_hash());
        assert_eq!(restored.current_epoch, 9);
        assert_eq!(restored.tx_fee_pool, 123);
    }

    #[test]
    fn bootstrap_rejects_tampered_chunk() {
        let state = default_state();
        let manifest_and_chunks = build_bootstrap_snapshot(1, &state, 16).unwrap();
        let mut chunks = manifest_and_chunks.chunks;
        chunks[0].data[0] ^= 0xFF;

        let err = restore_state_from_chunks(&manifest_and_chunks.manifest, chunks).unwrap_err();
        assert!(matches!(err, BootstrapError::InvalidChunkProof(0)));
    }

    #[test]
    fn bootstrap_rejects_missing_chunk() {
        let state = default_state();
        let manifest_and_chunks = build_bootstrap_snapshot(1, &state, 16).unwrap();
        let mut chunks = manifest_and_chunks.chunks;
        chunks.pop();

        let err = restore_state_from_chunks(&manifest_and_chunks.manifest, chunks).unwrap_err();
        assert!(matches!(err, BootstrapError::WrongChunkCount { .. }));
    }

    #[test]
    fn bootstrap_rejects_manifest_state_hash_mismatch() {
        let state = default_state();
        let mut snapshot = build_bootstrap_snapshot(1, &state, 16).unwrap();
        snapshot.manifest.state_hash[0] ^= 0xFF;

        let err = restore_state_from_chunks(&snapshot.manifest, snapshot.chunks).unwrap_err();
        assert!(matches!(err, BootstrapError::StateHashMismatch { .. }));
    }

    #[test]
    fn store_persists_bootstrap_manifest_and_chunks() {
        let (store, path) = temp_store();
        let mut state = default_state();
        state.current_epoch = 3;
        state.tx_fee_pool = 99;

        store.save_snapshot(3, &state);

        let manifest = store.load_manifest(3).unwrap();
        assert_eq!(manifest.epoch, 3);
        assert_eq!(manifest.state_hash, state.compute_state_hash());
        assert!(store.load_chunk(3, 0).is_some());

        let bootstrapped = store.bootstrap_state(&manifest).unwrap();
        assert_eq!(bootstrapped.current_epoch, 3);
        assert_eq!(bootstrapped.tx_fee_pool, 99);
        cleanup(&path);
    }

    #[test]
    fn store_persists_checkpoint_matching_manifest() {
        let (store, path) = temp_store();
        let state = default_state();
        store.save_snapshot(4, &state);
        let manifest = store.load_manifest(4).unwrap();

        let checkpoint = SnapshotCheckpoint {
            epoch: 4,
            finalized_slot: 71_999,
            transition_block: bunker_coin_core::epoch_transition::EpochTransitionBlock {
                epoch: 4,
                last_slot: 71_999,
                fees_distributed: 0,
                bonds_activated: Vec::new(),
                retires_completed: Vec::new(),
                new_validator_set: Vec::new(),
                state_hash: manifest.state_hash,
                snapshot_chunk_root: manifest.chunk_root.as_ref().try_into().unwrap(),
                snapshot_chunk_count: manifest.chunk_count,
                snapshot_total_bytes: manifest.total_bytes,
                snapshot_chunk_size: manifest.chunk_size,
                slashes_applied: Vec::new(),
                deactivated_validators: Vec::new(),
                location_claims_validated: Vec::new(),
            },
            finalization_certs: vec![vec![1, 2, 3]],
        };

        store.save_checkpoint(checkpoint).unwrap();
        let loaded = store.load_checkpoint(4).unwrap();
        assert!(loaded.matches_manifest(&manifest));
        assert!(store.latest_manifest().is_some());
        cleanup(&path);
    }
}
