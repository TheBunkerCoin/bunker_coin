use bunker_coin_core::execution::State;
use log::info;
use rocksdb::{DB, IteratorMode, Options};

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

    pub fn prune_old_snapshots(&self, keep_latest_n: usize) {
        let iter = self.db.iterator(IteratorMode::End);
        let mut keys: Vec<Vec<u8>> = Vec::new();
        for item in iter {
            if let Ok((key, _)) = item {
                let key_str = std::str::from_utf8(&key).unwrap_or("");
                if key_str.starts_with("snapshot|") {
                    keys.push(key.to_vec());
                }
            }
        }

        if keys.len() > keep_latest_n {
            for key in &keys[keep_latest_n..] {
                let _ = self.db.delete(key);
            }
            info!(
                "pruned {} old snapshots, keeping {keep_latest_n}",
                keys.len() - keep_latest_n
            );
        }
    }
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
}
