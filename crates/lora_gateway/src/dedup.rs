//! Deduplication cache — ring-buffer with O(1) lookup.

use std::collections::{HashSet, VecDeque};

use bunker_coin_core::types::PublicKey;

/// Key: (sender, msg_id).
type DedupKey = (PublicKey, u32);

/// Ring-buffer deduplication cache. FIFO eviction when full.
pub struct DedupCache {
    max_entries: usize,
    ring: VecDeque<DedupKey>,
    set: HashSet<DedupKey>,
}

impl DedupCache {
    pub fn new(max_entries: usize) -> Self {
        Self {
            max_entries,
            ring: VecDeque::with_capacity(max_entries),
            set: HashSet::with_capacity(max_entries),
        }
    }

    /// Returns `true` if this is a duplicate (already seen).
    pub fn is_duplicate(&mut self, sender: &PublicKey, msg_id: u32) -> bool {
        let key = (*sender, msg_id);
        if self.set.contains(&key) {
            return true;
        }
        // Evict oldest if full
        if self.ring.len() >= self.max_entries {
            if let Some(old) = self.ring.pop_front() {
                self.set.remove(&old);
            }
        }
        self.set.insert(key);
        self.ring.push_back(key);
        false
    }

    pub fn len(&self) -> usize {
        self.ring.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pk(n: u8) -> PublicKey {
        let mut key = [0u8; 32];
        key[0] = n;
        key
    }

    #[test]
    fn first_not_duplicate() {
        let mut cache = DedupCache::new(4096);
        assert!(!cache.is_duplicate(&pk(1), 1));
    }

    #[test]
    fn second_is_duplicate() {
        let mut cache = DedupCache::new(4096);
        assert!(!cache.is_duplicate(&pk(1), 1));
        assert!(cache.is_duplicate(&pk(1), 1));
    }

    #[test]
    fn ring_evicts_oldest() {
        let mut cache = DedupCache::new(2);
        assert!(!cache.is_duplicate(&pk(1), 1)); // slot 0
        assert!(!cache.is_duplicate(&pk(2), 2)); // slot 1
        assert!(!cache.is_duplicate(&pk(3), 3)); // evicts (pk(1), 1)

        // (pk(1), 1) should no longer be in cache
        assert!(!cache.is_duplicate(&pk(1), 1)); // re-inserted as new
        assert_eq!(cache.len(), 2);
    }
}
