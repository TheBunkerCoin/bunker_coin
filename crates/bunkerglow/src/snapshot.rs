use log::info;
use rocksdb::{DB, IteratorMode, Options};

use bunker_coin_core::execution::State;

pub struct SnapshotStore {
    db: DB,
}

impl SnapshotStore {
    pub fn new(node_id: u64) -> Self {
        let path = format!("data/snapshots/{node_id}");
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
            info!("pruned {} old snapshots, keeping {keep_latest_n}", keys.len() - keep_latest_n);
        }
    }
}
