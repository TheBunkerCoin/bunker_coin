//! Fragment reassembly for multi-frame capsules.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use bunker_coin_core::capsule::{CapsuleHeader, DecodedCapsule};
use bunker_coin_core::types::{PublicKey, Signature};

type ReassemblyKey = (PublicKey, u32);

struct PendingMessage {
    header: CapsuleHeader,
    sig: Signature,
    total_frags: u8,
    chunks: HashMap<u8, Vec<u8>>,
    created: Instant,
}

/// Reassembles fragmented capsules into complete bodies.
pub struct FragmentReassembler {
    pending: HashMap<ReassemblyKey, PendingMessage>,
    max_age: Duration,
}

impl FragmentReassembler {
    pub fn new(max_age: Duration) -> Self {
        Self {
            pending: HashMap::new(),
            max_age,
        }
    }

    /// Feed a capsule and return a complete signed body when all fragments arrive.
    pub fn feed(&mut self, capsule: DecodedCapsule) -> Option<(CapsuleHeader, Vec<u8>, Signature)> {
        match capsule {
            DecodedCapsule::Single { header, body, sig } => Some((header, body, sig)),
            DecodedCapsule::FragFirst {
                header,
                frag_idx,
                total_frags,
                body_chunk,
                sig,
            } => {
                let key = (header.sender, header.msg_id);
                let entry = self.pending.entry(key).or_insert_with(|| PendingMessage {
                    header: header.clone(),
                    sig,
                    total_frags,
                    chunks: HashMap::new(),
                    created: Instant::now(),
                });
                entry.chunks.insert(frag_idx, body_chunk);
                self.try_complete(&(header.sender, header.msg_id))
            }
            DecodedCapsule::FragCont {
                msg_id,
                frag_idx,
                body_chunk,
                ..
            } => {
                // Continuations lack sender, so find the first-fragment entry by message id.
                let key = self.pending.keys().find(|(_, id)| *id == msg_id).cloned();
                if let Some(key) = key {
                    if let Some(entry) = self.pending.get_mut(&key) {
                        entry.chunks.insert(frag_idx, body_chunk);
                    }
                    self.try_complete(&key)
                } else {
                    None
                }
            }
        }
    }

    fn try_complete(&mut self, key: &ReassemblyKey) -> Option<(CapsuleHeader, Vec<u8>, Signature)> {
        let entry = self.pending.get(key)?;
        if entry.chunks.len() != entry.total_frags as usize {
            return None;
        }
        let entry = self.pending.remove(key).unwrap();
        let mut body = Vec::new();
        for i in 0..entry.total_frags {
            body.extend_from_slice(&entry.chunks[&i]);
        }
        Some((entry.header, body, entry.sig))
    }

    /// Remove entries older than `max_age`.
    pub fn evict_expired(&mut self) {
        let now = Instant::now();
        self.pending
            .retain(|_, v| now.duration_since(v.created) < self.max_age);
    }

    /// Number of pending (incomplete) messages.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bunker_coin_core::capsule::{encode_fragmented, CapsuleHeader, DecodedCapsule};

    fn header(msg_id: u32) -> CapsuleHeader {
        CapsuleHeader {
            sender: [0xAA; 32],
            nonce: 1,
            fee: 10,
            msg_id,
        }
    }

    fn make_fragments(body: &[u8], msg_id: u32) -> Vec<DecodedCapsule> {
        let h = header(msg_id);
        let sig = [0xBB; 64];
        let frames = encode_fragmented(&h, body, &sig).unwrap();
        frames
            .iter()
            .map(|f| bunker_coin_core::capsule::decode(f).unwrap())
            .collect()
    }

    #[test]
    fn in_order_reassembly() {
        let mut r = FragmentReassembler::new(Duration::from_secs(60));
        let body = vec![0x42; 400];
        let frags = make_fragments(&body, 1);

        for (i, frag) in frags.into_iter().enumerate() {
            let result = r.feed(frag);
            if i < 1 {
                // Completion is checked below.
            }
            if let Some((_, reassembled, _)) = result {
                assert_eq!(reassembled, body);
                return;
            }
        }
        panic!("reassembly never completed");
    }

    #[test]
    fn out_of_order_continuations_reassemble() {
        let mut r = FragmentReassembler::new(Duration::from_secs(60));
        let body = vec![0x42; 600];
        let mut frags = make_fragments(&body, 2);
        assert!(frags.len() >= 3);
        let first = frags.remove(0);
        frags.reverse();

        assert!(r.feed(first).is_none());
        let mut done = false;
        for frag in frags {
            if let Some((_, reassembled, _)) = r.feed(frag) {
                assert_eq!(reassembled, body);
                done = true;
            }
        }
        assert!(done, "reassembly never completed");
    }

    #[test]
    fn continuations_before_first_fragment_are_dropped() {
        let mut r = FragmentReassembler::new(Duration::from_secs(60));
        let body = vec![0x42; 600];
        let mut frags = make_fragments(&body, 2);
        frags.reverse();

        for frag in frags {
            assert!(r.feed(frag).is_none());
        }
        // Only the first fragment created state; early continuations were dropped.
        assert_eq!(r.pending_count(), 1);
    }

    #[test]
    fn incomplete_returns_none() {
        let mut r = FragmentReassembler::new(Duration::from_secs(60));
        let body = vec![0x42; 400];
        let frags = make_fragments(&body, 3);

        let result = r.feed(frags[0].clone());
        assert!(result.is_none());
        assert_eq!(r.pending_count(), 1);
    }

    #[test]
    fn expired_entries_evicted() {
        let mut r = FragmentReassembler::new(Duration::from_millis(0));
        let body = vec![0x42; 400];
        let frags = make_fragments(&body, 4);

        r.feed(frags[0].clone());
        assert_eq!(r.pending_count(), 1);

        r.evict_expired();
        assert_eq!(r.pending_count(), 0);
    }
}
