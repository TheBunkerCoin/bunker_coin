//! Durable record of this validator's own votes ("tower storage").
//!
//! Votor's voting rules (never notar and skip the same slot, never notar two
//! different hashes, never finalize a slot after a fallback vote) are enforced
//! through in-memory state (`voted`, `voted_notar`, `bad_window`). That state
//! used to die with the process: after a mid-window crash the restart path
//! emits timeouts for the unfinished slots, and the node would happily cast a
//! skip vote for a slot it had already notar-voted before the crash — a
//! slashable conflicting vote (100% slash + indefinite jail).
//!
//! [`VoteHistory`] closes that gap: every own vote is written to the pool's
//! RocksDB *before* it is broadcast, and [`Votor`](super::votor::Votor)
//! reloads the unfinalized tail on startup to rebuild its voting state.
//! Records below the finalized frontier are pruned — conflicts there can no
//! longer matter (peers reject votes for finalized slots).
//!
//! Durability matches the cert/block persistence style: WAL-backed puts
//! without fsync, which survive a process crash (the threat model here) but
//! not an OS crash.

use std::sync::Arc;

use log::{info, warn};
use rocksdb::{DB, IteratorMode, Options};

use super::Vote;
use crate::crypto::merkle::MerkleRoot;
use crate::{Slot, ValidatorId};

/// Key prefix for own-vote records in the pool DB (shared with `cert|` and
/// `meta|` keys written by [`super::PoolImpl`]).
const KEY_PREFIX: &[u8] = b"ownvote|";

/// Persistent log of this validator's own votes, backed by the pool's RocksDB.
///
/// A disabled instance (no DB) records and restores nothing; unit tests and
/// consumers that construct [`super::votor::Votor`] directly get that default.
pub struct VoteHistory {
    db: Option<Arc<DB>>,
}

impl VoteHistory {
    /// A no-op history: nothing is recorded or restored.
    pub const fn disabled() -> Self {
        Self { db: None }
    }

    /// Opens the vote history backed by the same RocksDB as the validator's
    /// pool (`data/pool/{own_id}`). The process-wide handle cache in
    /// `blockstore` returns the pool's existing handle, so this never
    /// contends for the RocksDB lock.
    pub fn open(own_id: ValidatorId) -> Self {
        Self::open_at(&format!("data/pool/{own_id}"))
    }

    /// Opens the vote history at an explicit DB path (shared handle-cache
    /// semantics as [`Self::open`]).
    pub fn open_at(db_path: &str) -> Self {
        std::fs::create_dir_all(db_path).ok();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        match super::blockstore::open_db_with_retry(&opts, db_path) {
            Ok(db) => Self { db: Some(db) },
            Err(e) => {
                warn!("[VoteHistory] failed to open {db_path}: {e} — vote persistence DISABLED");
                Self { db: None }
            }
        }
    }

    /// Records an own vote. MUST be called before the vote is broadcast, so a
    /// crash between the two can only lose a vote that never left the node.
    pub fn record(&self, vote: &Vote) {
        let Some(db) = &self.db else { return };
        // One key per (slot, kind, hash): notar-fallback votes for distinct
        // hashes in one slot each keep their own record.
        let kind = match vote {
            v if v.is_notar() => "N",
            v if v.is_notar_fallback() => "n",
            v if v.is_skip() => "S",
            v if v.is_skip_fallback() => "s",
            _ => "F",
        };
        let hash_suffix = vote
            .block_hash()
            .map(|h| hex::encode(h.as_hash()))
            .unwrap_or_default();
        let key = format!("ownvote|{:016X}|{kind}{hash_suffix}", vote.slot());
        match wincode::serialize(vote) {
            Ok(val) => {
                let _ = db.put(key.as_bytes(), val);
            }
            Err(e) => warn!("[VoteHistory] failed to serialize own vote: {e}"),
        }
    }

    /// Loads all recorded votes for slots at or after `from_slot`, deleting
    /// the settled records below it. Called once on startup.
    pub fn load_and_prune(&self, from_slot: Slot) -> Vec<Vote> {
        let Some(db) = &self.db else { return Vec::new() };
        let mut votes = Vec::new();
        for item in db.iterator(IteratorMode::Start).flatten() {
            let (k, v) = item;
            if !k.starts_with(KEY_PREFIX) {
                continue;
            }
            match Self::key_slot(&k) {
                Some(slot) if slot < from_slot => {
                    let _ = db.delete(&k);
                }
                Some(_) => match wincode::deserialize::<Vote>(&v) {
                    Ok(vote) => votes.push(vote),
                    Err(_) => {
                        // Unreadable record (e.g. wire-format change): drop it
                        // rather than carry it forever.
                        let _ = db.delete(&k);
                    }
                },
                None => {
                    let _ = db.delete(&k);
                }
            }
        }
        if !votes.is_empty() {
            info!(
                "[VoteHistory] restored {} own vote(s) at or after slot {from_slot}",
                votes.len()
            );
        }
        votes
    }

    /// Deletes all records for slots strictly below `below`. Called as
    /// finalization advances so the log stays a window-sized tail.
    pub fn prune(&self, below: Slot) {
        let Some(db) = &self.db else { return };
        for item in db.iterator(IteratorMode::Start).flatten() {
            let (k, _) = item;
            if !k.starts_with(KEY_PREFIX) {
                continue;
            }
            if matches!(Self::key_slot(&k), Some(slot) if slot < below) {
                let _ = db.delete(&k);
            }
        }
    }

    /// Parses the slot out of an `ownvote|{slot:016X}|...` key.
    fn key_slot(key: &[u8]) -> Option<Slot> {
        let hex = key.get(KEY_PREFIX.len()..KEY_PREFIX.len() + 16)?;
        let s = std::str::from_utf8(hex).ok()?;
        u64::from_str_radix(s, 16).ok().map(Slot::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::Hash;
    use crate::crypto::aggsig::SecretKey;

    fn unique_db_path(tag: &str) -> String {
        format!(
            "{}/bunkerglow-vote-history-{tag}-{}",
            std::env::temp_dir().display(),
            std::process::id()
        )
    }

    #[test]
    fn record_load_prune_roundtrip() {
        let path = unique_db_path("roundtrip");
        let _ = std::fs::remove_dir_all(&path);
        let sk = SecretKey::new(&mut rand::rng());
        let hash = Hash::random_for_test().into();

        let history = VoteHistory::open_at(&path);
        history.record(&Vote::new_notar(Slot::new(5), hash, &sk, 0));
        history.record(&Vote::new_skip(Slot::new(6), &sk, 0));
        history.record(&Vote::new_skip_fallback(Slot::new(7), &sk, 0));

        // A second open of the same path shares the cached handle — this is
        // exactly the restart path (fresh VoteHistory, same DB).
        let reopened = VoteHistory::open_at(&path);
        let votes = reopened.load_and_prune(Slot::new(6));
        // The slot-5 notar record is below the frontier: pruned, not restored.
        assert_eq!(votes.len(), 2);
        assert!(votes.iter().all(|v| v.slot() >= Slot::new(6)));

        reopened.prune(Slot::new(8));
        assert!(reopened.load_and_prune(Slot::genesis()).is_empty());
    }

    #[test]
    fn disabled_history_is_noop() {
        let sk = SecretKey::new(&mut rand::rng());
        let history = VoteHistory::disabled();
        history.record(&Vote::new_skip(Slot::new(1), &sk, 0));
        assert!(history.load_and_prune(Slot::genesis()).is_empty());
    }
}
