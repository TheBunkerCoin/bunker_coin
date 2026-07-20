//! Fragmentation/reassembly shared by [`PactorNetwork`](crate::pactor_network)
//! and [`PactorMux`](crate::pactor_mux).
//!
//! The transport's [`write_data`](scs_pactor::PactorTransport::write_data)
//! carries one message as a single hex line that must fit the 300-byte radio
//! MTU. Larger payloads are split across several lines with a small
//! [`FragmentHeader`] and reassembled by [`Reassembler`]. A payload that fits
//! one line is a single `total_fragments == 1` fragment, so the common case
//! (a vote) is unchanged on the wire shape.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Radio MTU (bytes per `write_data` line, hex-encoded `#...\r`). One line is
/// `1 (#) + 2*payload + 1 (\r)`, so the byte budget carried per line (header +
/// fragment chunk) is `(MTU - 2) / 2`.
pub(crate) const RADIO_MTU: usize = 300;

/// Per-fragment header prepended to each `write_data` line. `message_id` is
/// shared by all fragments of one message; `fragment_index` is 0-based;
/// `total_fragments` is the count.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FragmentHeader {
    pub message_id: u64,
    pub fragment_index: u16,
    pub total_fragments: u16,
}

/// Encoded size of a [`FragmentHeader`] — used to size the effective payload.
pub(crate) fn fragment_header_len() -> usize {
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
}

/// Bytes of message payload carried per fragment line, after reserving room for
/// the header and accounting for hex doubling within the MTU. At least 1.
pub(crate) fn effective_chunk_len() -> usize {
    let line_byte_budget = (RADIO_MTU - 2) / 2;
    line_byte_budget
        .saturating_sub(fragment_header_len())
        .max(1)
}

/// Build one fragment line: bincode header followed by the raw chunk bytes.
pub(crate) fn frame_fragment(header: &FragmentHeader, chunk: &[u8]) -> Vec<u8> {
    let mut packet = bincode::serde::encode_to_vec(header, bincode::config::standard())
        .expect("header encoding cannot fail");
    packet.extend_from_slice(chunk);
    packet
}

/// Split a framed fragment line back into its header and chunk bytes.
pub(crate) fn parse_fragment(bytes: &[u8]) -> Option<(FragmentHeader, Vec<u8>)> {
    let (header, consumed): (FragmentHeader, usize) =
        bincode::serde::decode_from_slice(bytes, bincode::config::standard()).ok()?;
    if header.total_fragments == 0 || header.fragment_index >= header.total_fragments {
        return None;
    }
    Some((header, bytes[consumed..].to_vec()))
}

/// Split a serialized message into MTU-sized fragment lines, ready for
/// `write_data`. A message that fits one line yields a single fragment.
pub(crate) fn fragment_message(message_id: u64, bytes: &[u8]) -> Vec<Vec<u8>> {
    let chunk_len = effective_chunk_len();
    let chunks: Vec<&[u8]> = if bytes.is_empty() {
        vec![&bytes[..]]
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

/// How long an incomplete reassembly may sit before it is evicted. A fragment
/// lost on-air (bad line, mid-message changeover, session drop) previously left
/// its `ReassemblyState` in the map FOREVER — unbounded growth on a flaky link,
/// and a permanently wedged `message_id` that could mis-merge with a later
/// message reusing the id (the per-mux counter restarts at 0 each session).
/// Generous for PACTOR speeds: a large multi-fragment message takes minutes.
const REASSEMBLY_TTL: std::time::Duration = std::time::Duration::from_secs(600);

/// Maximum number of concurrently in-flight reassemblies. On a point-to-point
/// half-duplex link even 2 is unusual; garbled lines that bincode-decode into
/// bogus headers can mint arbitrary `message_id`s, so cap the map and evict the
/// oldest when full.
const MAX_PARTIAL_MESSAGES: usize = 32;

/// Maximum plausible fragment count per message. `total_fragments` is
/// wire-controlled up to u16::MAX (~9 MB at the effective chunk size); real
/// consensus messages are a handful of fragments. Anything above this is a
/// corrupt or hostile header.
const MAX_FRAGMENTS_PER_MESSAGE: u16 = 1024;

/// In-progress reassembly of a fragmented message.
struct ReassemblyState {
    fragments: HashMap<u16, Vec<u8>>,
    total_fragments: u16,
    /// When the first fragment arrived, for TTL eviction.
    created: std::time::Instant,
}

/// Accumulates inbound fragment lines and yields complete messages.
///
/// Tolerates out-of-order fragments; reassembly is over a single point-to-point
/// link, so the peer's id is implicit (`message_id` alone keys an in-flight
/// message). Incomplete reassemblies are evicted after [`REASSEMBLY_TTL`], and
/// at most [`MAX_PARTIAL_MESSAGES`] are held at once.
#[derive(Default)]
pub(crate) struct Reassembler {
    partial: HashMap<u64, ReassemblyState>,
}

impl Reassembler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed one inbound fragment line. Returns `Some(message)` once the line
    /// completes a message, `None` if more fragments are still needed or the
    /// line was unparseable (logged by the caller).
    pub fn push_line(&mut self, line: &[u8]) -> Option<Vec<u8>> {
        self.push_line_at(line, std::time::Instant::now())
    }

    /// [`Self::push_line`] with an explicit clock, so tests can drive TTL
    /// eviction without waiting.
    fn push_line_at(&mut self, line: &[u8], now: std::time::Instant) -> Option<Vec<u8>> {
        // Evict expired partials first: a message that lost a fragment will
        // never complete; holding its state forever both leaks memory and
        // wedges its message_id against reuse.
        self.partial
            .retain(|_, state| now.duration_since(state.created) < REASSEMBLY_TTL);

        let (header, chunk) = parse_fragment(line)?;

        // Corrupt/hostile headers can claim absurd fragment counts or oversized
        // chunks; both bound the memory a single message_id may pin.
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
        // A fragment disagreeing with the recorded total is corrupt (or a
        // colliding message id). Accepting it could push `fragments.len()`
        // past `total_fragments`, after which the exact-equality completion
        // check below could never fire — wedging the entry forever.
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
                    // Should be unreachable (count matched), but never panic on
                    // wire data — drop and wait for a retransmit.
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
        assert!(1 + (fragment_header_len() + chunk) * 2 + 1 <= RADIO_MTU);
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

    /// Incomplete reassemblies must be evicted after [`REASSEMBLY_TTL`]: a lost
    /// fragment previously wedged its `message_id` (and its memory) forever.
    #[test]
    fn stale_partial_is_evicted_and_id_reusable() {
        let payload: Vec<u8> = (0..300u32).map(|i| (i % 7) as u8).collect();
        let lines = fragment_message(42, &payload);
        assert!(lines.len() >= 2);

        let t0 = std::time::Instant::now();
        let mut r = Reassembler::new();
        // First fragment arrives; the rest are lost on-air.
        assert!(r.push_line_at(&lines[0], t0).is_none());
        assert_eq!(r.partial.len(), 1);

        // Past the TTL, the stale partial is evicted...
        let later = t0 + REASSEMBLY_TTL + std::time::Duration::from_secs(1);
        // ...and a fresh message reusing the same id reassembles cleanly
        // instead of mis-merging with the stale fragments.
        let mut out = None;
        for line in &lines {
            if let Some(m) = r.push_line_at(line, later) {
                out = Some(m);
            }
        }
        assert_eq!(out.unwrap(), payload);
        assert!(r.partial.is_empty());
    }

    /// The in-flight map is capped: garbled lines minting arbitrary message ids
    /// must not grow it without bound.
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

    /// A fragment whose header disagrees with the recorded total (corruption or
    /// id collision) must be rejected — previously it could overshoot the
    /// exact-equality completion check and wedge the entry forever.
    #[test]
    fn mismatched_total_is_rejected_without_wedging() {
        let payload: Vec<u8> = (0..300u32).map(|i| (i % 11) as u8).collect();
        let lines = fragment_message(7, &payload);
        assert_eq!(lines.len(), 3);

        let now = std::time::Instant::now();
        let mut r = Reassembler::new();
        assert!(r.push_line_at(&lines[0], now).is_none());

        // Corrupt line: same id, bogus total. Must be dropped.
        let bogus = frame_fragment(
            &FragmentHeader {
                message_id: 7,
                fragment_index: 4,
                total_fragments: 9,
            },
            b"junk",
        );
        assert!(r.push_line_at(&bogus, now).is_none());

        // The genuine remaining fragments still complete the message.
        let mut out = None;
        for line in &lines[1..] {
            if let Some(m) = r.push_line_at(line, now) {
                out = Some(m);
            }
        }
        assert_eq!(out.unwrap(), payload);
    }

    /// Absurd fragment counts and oversized chunks are wire-controlled memory
    /// amplification; both must be dropped outright.
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
        // Interleave the two messages' fragments.
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
