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
use crate::crypto::merkle::{BlockHash, MerkleRoot};
use crate::{Slot, ValidatorId};

/// Key prefix for own-vote records in the pool DB (shared with `cert|` and
/// `meta|` keys written by [`super::PoolImpl`]).
const KEY_PREFIX: &[u8] = b"ownvote|";

/// Key prefix for *pending deferred-final* markers.
///
/// When `defer_final_vote` is on, a node notar-votes and then waits a grace
/// period before sending the slow-path finalization vote (see
/// [`super::votor::Votor::defer_final_vote`]). That deadline lives only in an
/// in-memory timer: a crash during the grace window loses the intent entirely,
/// and on restart nothing re-drives it — so if fast-final cannot complete
/// (e.g. the peer's notar vote was lost in the same outage that caused the
/// crash), the slot never gets a finalization vote and finalization stalls.
///
/// A marker is written at notar time (when deferring) and deleted once the
/// final vote is sent or the slot fast-finalizes / finalizes. On restart the
/// surviving markers are replayed so Votor can re-arm the deadline.
const PENDING_FINAL_PREFIX: &[u8] = b"pendfinal|";

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
        let Some(db) = &self.db else {
            return Vec::new();
        };
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

    /// Deletes all records (own votes and pending-final markers) for slots
    /// strictly below `below`. Called as finalization advances so the log stays
    /// a window-sized tail.
    pub fn prune(&self, below: Slot) {
        let Some(db) = &self.db else { return };
        for item in db.iterator(IteratorMode::Start).flatten() {
            let (k, _) = item;
            let prefixed = k.starts_with(KEY_PREFIX) || k.starts_with(PENDING_FINAL_PREFIX);
            if !prefixed {
                continue;
            }
            if matches!(Self::key_slot(&k), Some(slot) if slot < below) {
                let _ = db.delete(&k);
            }
        }
    }

    /// Records the intent to send a deferred finalization vote for
    /// `(slot, hash)`. Written at notar time when `defer_final_vote` is on,
    /// before the grace-period timer is spawned.
    pub fn record_pending_final(&self, slot: Slot, hash: &BlockHash) {
        let Some(db) = &self.db else { return };
        let key = Self::pending_final_key(slot, hash);
        // The value carries the hash so it can be reconstructed on reload.
        let _ = db.put(key.as_bytes(), hash.as_hash());
    }

    /// Clears the pending deferred-final marker for `(slot, hash)`. Called once
    /// the finalization vote is sent, or the slot fast-finalizes / finalizes.
    pub fn clear_pending_final(&self, slot: Slot, hash: &BlockHash) {
        let Some(db) = &self.db else { return };
        let key = Self::pending_final_key(slot, hash);
        let _ = db.delete(key.as_bytes());
    }

    /// Loads all pending deferred-final markers for slots at or after
    /// `from_slot`, deleting the settled ones below it. Called once on startup
    /// so Votor can re-arm a finalization-vote deadline for each.
    pub fn load_and_prune_pending_finals(&self, from_slot: Slot) -> Vec<(Slot, BlockHash)> {
        let Some(db) = &self.db else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for item in db.iterator(IteratorMode::Start).flatten() {
            let (k, v) = item;
            if !k.starts_with(PENDING_FINAL_PREFIX) {
                continue;
            }
            match Self::key_slot(&k) {
                Some(slot) if slot < from_slot => {
                    let _ = db.delete(&k);
                }
                Some(slot) => match <[u8; 32]>::try_from(v.as_ref()) {
                    Ok(arr) => out.push((slot, BlockHash::from(crate::crypto::Hash::from(arr)))),
                    Err(_) => {
                        let _ = db.delete(&k);
                    }
                },
                None => {
                    let _ = db.delete(&k);
                }
            }
        }
        if !out.is_empty() {
            info!(
                "[VoteHistory] restored {} pending deferred-final marker(s) at or after slot {from_slot}",
                out.len()
            );
        }
        out
    }

    fn pending_final_key(slot: Slot, hash: &BlockHash) -> String {
        format!(
            "pendfinal|{:016X}|{}",
            slot.inner(),
            hex::encode(hash.as_hash())
        )
    }

    /// Parses the slot out of an `ownvote|{slot:016X}|...` or
    /// `pendfinal|{slot:016X}|...` key. Both put the 16 hex slot digits
    /// immediately after their prefix.
    fn key_slot(key: &[u8]) -> Option<Slot> {
        let prefix_len = if key.starts_with(PENDING_FINAL_PREFIX) {
            PENDING_FINAL_PREFIX.len()
        } else {
            KEY_PREFIX.len()
        };
        let hex = key.get(prefix_len..prefix_len + 16)?;
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
    fn pending_final_roundtrip_and_prune() {
        let path = unique_db_path("pendfinal");
        let _ = std::fs::remove_dir_all(&path);
        let h5: BlockHash = Hash::random_for_test().into();
        let h6: BlockHash = Hash::random_for_test().into();
        let h7: BlockHash = Hash::random_for_test().into();

        let history = VoteHistory::open_at(&path);
        history.record_pending_final(Slot::new(5), &h5);
        history.record_pending_final(Slot::new(6), &h6);
        history.record_pending_final(Slot::new(7), &h7);

        // Clearing one removes just that marker.
        history.clear_pending_final(Slot::new(6), &h6);

        // Reload from slot 6: slot-5 marker is below the frontier (pruned),
        // slot-6 was cleared, only slot-7 survives.
        let reopened = VoteHistory::open_at(&path);
        let pending = reopened.load_and_prune_pending_finals(Slot::new(6));
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, Slot::new(7));
        assert_eq!(pending[0].1, h7);

        // Own-vote records and pending-final markers share the prune sweep.
        reopened.prune(Slot::new(8));
        assert!(
            reopened
                .load_and_prune_pending_finals(Slot::genesis())
                .is_empty()
        );
    }

    #[test]
    fn disabled_history_is_noop() {
        let sk = SecretKey::new(&mut rand::rng());
        let history = VoteHistory::disabled();
        history.record(&Vote::new_skip(Slot::new(1), &sk, 0));
        assert!(history.load_and_prune(Slot::genesis()).is_empty());
    }
}
