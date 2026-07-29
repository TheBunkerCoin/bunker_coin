//! PACTOR MTU fragmentation and reassembly shared by `PactorNetwork` and `PactorMux`.
//! Each fragment is one `write_data` line with a `FragmentHeader` before payload bytes.

use std::collections::HashMap;
use std::sync::LazyLock;

use serde::{Deserialize, Serialize};

/// Radio line MTU; hex framing leaves `(RADIO_MTU - 2) / 2` bytes for header plus chunk.
pub(crate) const RADIO_MTU: usize = 300;

/// Per-fragment header; `message_id` groups fragments and `fragment_index` is 0-based.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FragmentHeader {
    pub message_id: u64,
    pub fragment_index: u16,
    pub total_fragments: u16,
}

/// Encoded [`FragmentHeader`] size used to compute the fragment payload budget.
/// Fixed for a given `FragmentHeader` layout, so compute it once.
pub(crate) fn fragment_header_len() -> usize {
    static LEN: LazyLock<usize> = LazyLock::new(|| {
        bincode::serde::encode_to_vec(
            &FragmentHeader {
                message_id: u64::MAX,
                fragment_index: u16::MAX,
                total_fragments: u16::MAX,
            },
            bincode::config::standard(),
        )
        .expect("encoding a fixed header cannot fail")
        .len()
    });
    *LEN
}

/// Payload bytes per fragment after reserving header space and hex-encoded MTU overhead.
pub(crate) fn effective_chunk_len() -> usize {
    static LEN: LazyLock<usize> = LazyLock::new(|| {
        let line_byte_budget = (RADIO_MTU - 2) / 2;
        line_byte_budget
            .saturating_sub(fragment_header_len())
            .max(1)
    });
    *LEN
}

/// Encode a header followed by raw chunk bytes.
pub(crate) fn frame_fragment(header: &FragmentHeader, chunk: &[u8]) -> Vec<u8> {
    let mut packet = bincode::serde::encode_to_vec(header, bincode::config::standard())
        .expect("header encoding cannot fail");
    packet.extend_from_slice(chunk);
    packet
}

/// Decode a fragment line into its header and chunk bytes.
pub(crate) fn parse_fragment(bytes: &[u8]) -> Option<(FragmentHeader, Vec<u8>)> {
    let (header, consumed): (FragmentHeader, usize) =
        bincode::serde::decode_from_slice(bytes, bincode::config::standard()).ok()?;
    if header.total_fragments == 0 || header.fragment_index >= header.total_fragments {
        return None;
    }
    Some((header, bytes[consumed..].to_vec()))
}

/// Split serialized bytes into MTU-sized fragment lines.
pub(crate) fn fragment_message(message_id: u64, bytes: &[u8]) -> Vec<Vec<u8>> {
    let chunk_len = effective_chunk_len();
    let chunks: Vec<&[u8]> = if bytes.is_empty() {
        vec![bytes]
    } else {
        bytes.chunks(chunk_len).collect()
    };
    let total_fragments = chunks.len() as u16;
    chunks
        .iter()
        .enumerate()
        .map(|(index, chunk)| {
            let header = FragmentHeader {
                message_id,
                fragment_index: index as u16,
                total_fragments,
            };
            frame_fragment(&header, chunk)
        })
        .collect()
}

/// TTL for incomplete reassemblies; lost fragments must not pin memory or message ids.
const REASSEMBLY_TTL: std::time::Duration = std::time::Duration::from_secs(600);

/// Cap in-flight reassemblies; bogus headers can mint arbitrary message ids.
const MAX_PARTIAL_MESSAGES: usize = 32;

/// Cap wire-controlled fragment counts before allocating state.
const MAX_FRAGMENTS_PER_MESSAGE: u16 = 1024;

struct ReassemblyState {
    fragments: HashMap<u16, Vec<u8>>,
    total_fragments: u16,
    created: std::time::Instant,
}

/// Bounded, TTL-evicted reassembler for one point-to-point peer.
#[derive(Default)]
pub(crate) struct Reassembler {
    partial: HashMap<u64, ReassemblyState>,
}

impl Reassembler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed one fragment line and return a complete message when reassembly finishes.
    pub fn push_line(&mut self, line: &[u8]) -> Option<Vec<u8>> {
        self.push_line_at(line, std::time::Instant::now())
    }

    /// Test hook for driving TTL eviction with an explicit clock.
    fn push_line_at(&mut self, line: &[u8], now: std::time::Instant) -> Option<Vec<u8>> {
        // Lost fragments cannot pin message ids or memory past the TTL.
        self.partial
            .retain(|_, state| now.duration_since(state.created) < REASSEMBLY_TTL);

        let (header, chunk) = parse_fragment(line)?;

        // Bound memory pinned by corrupt or hostile wire headers.
        if header.total_fragments > MAX_FRAGMENTS_PER_MESSAGE || chunk.len() > effective_chunk_len()
        {
            return None;
        }

        if header.total_fragments == 1 {
            return Some(chunk);
        }

        // Cap concurrent reassemblies: make room by dropping the oldest.
        if !self.partial.contains_key(&header.message_id)
            && self.partial.len() >= MAX_PARTIAL_MESSAGES
        {
            if let Some(oldest) = self
                .partial
                .iter()
                .min_by_key(|(_, s)| s.created)
                .map(|(id, _)| *id)
            {
                self.partial.remove(&oldest);
            }
        }

        let state = self
            .partial
            .entry(header.message_id)
            .or_insert_with(|| ReassemblyState {
                fragments: HashMap::new(),
                total_fragments: header.total_fragments,
                created: now,
            });
        // Reject conflicting totals so a corrupt id collision cannot wedge completion.
        if header.total_fragments != state.total_fragments
            || header.fragment_index >= state.total_fragments
        {
            return None;
        }
        state.fragments.insert(header.fragment_index, chunk);

        if state.fragments.len() != state.total_fragments as usize {
            return None;
        }

        let total = state.total_fragments;
        let mut message = Vec::new();
        for i in 0..total {
            match state.fragments.get(&i) {
                Some(fragment) => message.extend_from_slice(fragment),
                None => {
                    // Count matched, but never panic on wire data.
                    self.partial.remove(&header.message_id);
                    return None;
                }
            }
        }
        self.partial.remove(&header.message_id);
        Some(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_and_chunk_len_are_sane() {
        assert!(fragment_header_len() >= 4);
        let chunk = effective_chunk_len();
        assert!(chunk >= 64, "chunk too small: {chunk}");
        assert!(1 + (fragment_header_len() + chunk) * 2 < RADIO_MTU);
    }

    #[test]
    fn small_message_single_fragment() {
        let lines = fragment_message(7, b"hello");
        assert_eq!(lines.len(), 1);
        let mut r = Reassembler::new();
        assert_eq!(r.push_line(&lines[0]).unwrap(), b"hello");
    }

    #[test]
    fn large_message_roundtrips() {
        let payload: Vec<u8> = (0..4000u32).map(|i| (i % 251) as u8).collect();
        let lines = fragment_message(1, &payload);
        assert!(lines.len() > 1);
        let mut r = Reassembler::new();
        let mut out = None;
        for line in &lines {
            if let Some(msg) = r.push_line(line) {
                out = Some(msg);
            }
        }
        assert_eq!(out.unwrap(), payload);
    }

    #[test]
    fn out_of_order_roundtrips() {
        let payload: Vec<u8> = (0..1500u32).map(|i| (i % 97) as u8).collect();
        let mut lines = fragment_message(2, &payload);
        lines.reverse();
        let mut r = Reassembler::new();
        let mut out = None;
        for line in &lines {
            if let Some(msg) = r.push_line(line) {
                out = Some(msg);
            }
        }
        assert_eq!(out.unwrap(), payload);
    }

    /// Incomplete reassemblies expire so lost fragments cannot pin ids or memory.
    #[test]
    fn stale_partial_is_evicted_and_id_reusable() {
        let payload: Vec<u8> = (0..300u32).map(|i| (i % 7) as u8).collect();
        let lines = fragment_message(42, &payload);
        assert!(lines.len() >= 2);

        let t0 = std::time::Instant::now();
        let mut r = Reassembler::new();
        assert!(r.push_line_at(&lines[0], t0).is_none());
        assert!(r.partial.contains_key(&42));

        // A fragment of a DIFFERENT message past the TTL must sweep the stale
        // entry; completion must not be what removes it.
        let other = fragment_message(43, &payload);
        let later = t0 + REASSEMBLY_TTL + std::time::Duration::from_secs(1);
        assert!(r.push_line_at(&other[0], later).is_none());
        assert!(!r.partial.contains_key(&42));
        assert!(r.partial.contains_key(&43));

        // The swept id is reusable: the full message reassembles afterwards.
        let mut out = None;
        for line in &lines {
            if let Some(m) = r.push_line_at(line, later) {
                out = Some(m);
            }
        }
        assert_eq!(out.expect("reassembly never completed"), payload);
    }

    /// Bogus message ids cannot grow the in-flight map without bound.
    #[test]
    fn partial_map_is_capped() {
        let now = std::time::Instant::now();
        let mut r = Reassembler::new();
        for id in 0..(MAX_PARTIAL_MESSAGES as u64 + 10) {
            let header = FragmentHeader {
                message_id: id,
                fragment_index: 0,
                total_fragments: 2,
            };
            let line = frame_fragment(&header, b"x");
            assert!(r.push_line_at(&line, now).is_none());
        }
        assert!(r.partial.len() <= MAX_PARTIAL_MESSAGES);
    }

    /// Conflicting totals are rejected so corrupt fragments cannot wedge completion.
    #[test]
    fn mismatched_total_is_rejected_without_wedging() {
        let payload: Vec<u8> = (0..300u32).map(|i| (i % 11) as u8).collect();
        let lines = fragment_message(7, &payload);
        assert_eq!(lines.len(), 3);

        let now = std::time::Instant::now();
        let mut r = Reassembler::new();
        assert!(r.push_line_at(&lines[0], now).is_none());

        let bogus = frame_fragment(
            &FragmentHeader {
                message_id: 7,
                fragment_index: 4,
                total_fragments: 9,
            },
            b"junk",
        );
        assert!(r.push_line_at(&bogus, now).is_none());

        let mut out = None;
        for line in &lines[1..] {
            if let Some(m) = r.push_line_at(line, now) {
                out = Some(m);
            }
        }
        assert_eq!(out.unwrap(), payload);
    }

    /// Wire-controlled memory amplification is dropped before state allocation.
    #[test]
    fn hostile_headers_are_dropped() {
        let now = std::time::Instant::now();
        let mut r = Reassembler::new();
        let huge_total = frame_fragment(
            &FragmentHeader {
                message_id: 1,
                fragment_index: 0,
                total_fragments: u16::MAX,
            },
            b"x",
        );
        assert!(r.push_line_at(&huge_total, now).is_none());
        assert!(r.partial.is_empty());

        let oversized_chunk = frame_fragment(
            &FragmentHeader {
                message_id: 2,
                fragment_index: 0,
                total_fragments: 2,
            },
            &vec![0u8; effective_chunk_len() + 1],
        );
        assert!(r.push_line_at(&oversized_chunk, now).is_none());
        assert!(r.partial.is_empty());
    }

    #[test]
    fn interleaved_messages_roundtrip() {
        let a: Vec<u8> = (0..1500u32).map(|i| (i % 13) as u8).collect();
        let b: Vec<u8> = (0..1500u32).map(|i| (i % 29) as u8).collect();
        let la = fragment_message(10, &a);
        let lb = fragment_message(11, &b);
        assert!(la.len() > 1 && lb.len() > 1);

        let mut r = Reassembler::new();
        let (mut got_a, mut got_b) = (None, None);
        for i in 0..la.len().max(lb.len()) {
            if let Some(line) = la.get(i) {
                if let Some(m) = r.push_line(line) {
                    got_a = Some(m);
                }
            }
            if let Some(line) = lb.get(i) {
                if let Some(m) = r.push_line(line) {
                    got_b = Some(m);
                }
            }
        }
        assert_eq!(got_a.unwrap(), a);
        assert_eq!(got_b.unwrap(), b);
    }

    #[test]
    fn unparseable_line_returns_none() {
        let mut r = Reassembler::new();
        assert!(r.push_line(&[0xff, 0xff]).is_none());
    }
}
