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

/// In-progress reassembly of a fragmented message.
struct ReassemblyState {
    fragments: HashMap<u16, Vec<u8>>,
    total_fragments: u16,
}

/// Accumulates inbound fragment lines and yields complete messages.
///
/// Tolerates out-of-order fragments; reassembly is over a single point-to-point
/// link, so the peer's id is implicit (`message_id` alone keys an in-flight
/// message).
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
        let (header, chunk) = parse_fragment(line)?;

        if header.total_fragments == 1 {
            return Some(chunk);
        }

        let state = self
            .partial
            .entry(header.message_id)
            .or_insert_with(|| ReassemblyState {
                fragments: HashMap::new(),
                total_fragments: header.total_fragments,
            });
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
