// Copyright (c) Anza Technology, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Defines the [`Slice`] and related data structures.

use rand::{RngCore, rng};
use wincode::{SchemaRead, SchemaWrite};

use crate::crypto::merkle::{BlockHash, SliceRoot};
use crate::shredder::{MAX_DATA_PER_SLICE, ValidatedShred};
use crate::types::SliceIndex;
use crate::{BlockId, Slot};

/// A slice is the unit of data between block and shred.
///
/// It corresponds to a single batch of data that is disseminated by the leader.
/// During shredding, a slice is turned into multiple shreds.
/// During deshredding, multiple shreds are turned into a slice.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Slice {
    /// Slot number this slice is part of.
    pub slot: Slot,
    /// Index of the slice within its slot.
    pub slice_index: SliceIndex,
    /// Indicates whether this is the last slice in the slot.
    pub is_last: bool,
    /// Merkle root hash over all shreds in this slice.
    pub merkle_root: Option<SliceRoot>,
    /// If first slice in the block or parent changed due to optimistic handover,
    /// then indicates which block is the parent of the block this slice is part of.
    pub parent: Option<(Slot, BlockHash)>,
    /// Payload bytes.
    pub data: Vec<u8>,
}

impl Slice {
    /// Constructs a [`Slice`] from its component parts.
    pub(crate) fn from_parts(
        header: SliceHeader,
        payload: SlicePayload,
        merkle_root: Option<SliceRoot>,
    ) -> Self {
        let SliceHeader {
            slot,
            slice_index,
            is_last,
        } = header;
        // The payload now also carries the slot (it is part of the hashed bytes).
        // `from_parts` is only called with a header and payload from the same
        // slice, so `payload.slot` matches `header.slot`; the authenticated check
        // for *reconstructed* slices lives in the deshred path (see
        // `Slice::from_shreds_checked`).
        let SlicePayload {
            slot: _,
            parent,
            data,
        } = payload;
        Self {
            slot,
            slice_index,
            is_last,
            merkle_root,
            parent,
            data,
        }
    }

    /// Creates a [`Slice`] from raw payload bytes and the metadata extracted from
    /// a shred, **verifying the slot the (signed, Merkle-committed) payload claims
    /// matches the slot in the shred's (unauthenticated) header**.
    ///
    /// The payload's slot is covered by the leader's signature over the Merkle
    /// root, so it is the trustworthy one; the header's slot is plain metadata. A
    /// mismatch means a tampered or corrupt header and the slice is rejected.
    pub(crate) fn from_shreds_checked(
        payload: SlicePayload,
        any_shred: &ValidatedShred,
    ) -> Result<Self, SliceSlotMismatch> {
        let header = any_shred.payload().header.clone();
        if payload.slot != header.slot {
            return Err(SliceSlotMismatch {
                payload_slot: payload.slot,
                header_slot: header.slot,
            });
        }
        let merkle_root = Some(any_shred.merkle_root.clone());
        Ok(Self::from_parts(header, payload, merkle_root))
    }

    /// Deconstructs a [`Slice`] into its components: [`SliceHeader`] and [`SlicePayload`].
    pub(crate) fn deconstruct(self) -> (SliceHeader, SlicePayload) {
        let Slice {
            slot,
            slice_index,
            is_last,
            merkle_root: _,
            parent,
            data,
        } = self;
        (
            SliceHeader {
                slot,
                slice_index,
                is_last,
            },
            // Slot goes into the hashed payload as well as the header, so the
            // Merkle root (and the leader's signature over it) commit to it.
            SlicePayload { slot, parent, data },
        )
    }

    /// Extracts the [`SliceHeader`] from a [`Slice`].
    pub(crate) fn to_header(&self) -> SliceHeader {
        SliceHeader {
            slot: self.slot,
            slice_index: self.slice_index,
            is_last: self.is_last,
        }
    }
}

/// A reconstructed slice's signed payload slot did not match the slot in the
/// shred header it was carried in (a tampered or corrupt header).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SliceSlotMismatch {
    pub(crate) payload_slot: Slot,
    pub(crate) header_slot: Slot,
}

/// Struct to hold all the header payload of a [`Slice`].
///
/// This information is included in each shred after shredding.
#[derive(Clone, Debug, SchemaRead, SchemaWrite)]
pub(crate) struct SliceHeader {
    /// Same as [`Slice::slot`].
    pub(crate) slot: Slot,
    /// Same as [`Slice::slice_index`].
    pub(crate) slice_index: SliceIndex,
    /// Same as [`Slice::is_last`].
    pub(crate) is_last: bool,
}

/// Struct to hold all the actual payload of a [`Slice`].
///
/// This is what actually gets "shredded" into different shreds.
#[derive(Clone, Debug, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub(crate) struct SlicePayload {
    /// Slot this slice belongs to. Included in the *hashed* payload (not just the
    /// unauthenticated [`SliceHeader`]) so that the slice Merkle root — and hence
    /// the block hash and the leader's signature over the root — commit to the
    /// block's position in the chain. Without this, two empty blocks built on the
    /// same parent at different slots hash identically (the slot was carried only
    /// in the header, which is neither Merkle-hashed nor signed).
    pub(crate) slot: Slot,
    /// Same as [`Slice::parent`].
    pub(crate) parent: Option<(Slot, BlockHash)>,
    /// Same as [`Slice::data`].
    pub(crate) data: Vec<u8>,
}

impl SlicePayload {
    /// Constructs a new [`SlicePayload`] from its component parts.
    pub(crate) fn new(slot: Slot, parent: Option<(Slot, BlockHash)>, data: Vec<u8>) -> Self {
        Self { slot, parent, data }
    }

    /// Serializes the payload into bytes.
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        wincode::serialize(self).unwrap()
    }
}

impl From<SlicePayload> for Vec<u8> {
    fn from(payload: SlicePayload) -> Self {
        wincode::serialize(&payload).unwrap()
    }
}

impl From<&[u8]> for SlicePayload {
    fn from(payload: &[u8]) -> Self {
        assert!(
            payload.len() <= MAX_DATA_PER_SLICE,
            "payload.len()={} {MAX_DATA_PER_SLICE}",
            payload.len()
        );
        wincode::deserialize(payload).unwrap()
    }
}

/// Creates a [`SlicePayload`] with a random payload of desired size (in bytes).
///
/// The payload does not contain valid transactions.
/// This function should only be used for testing and benchmarking.
//
// XXX: This is only used in test and benchmarking code.
// Ensure it is only compiled when we are testing or benchmarking.
pub(crate) fn create_slice_payload_with_invalid_txs(
    slot: Slot,
    parent: Option<BlockId>,
    desired_size: usize,
) -> SlicePayload {
    let parent_bytes = <Option<BlockId> as wincode::SchemaWrite>::size_of(&parent).unwrap();
    // 8 bytes for the slot (Slot is a u64, fixed-length in wincode).
    let slot_bytes = <Slot as wincode::SchemaWrite>::size_of(&slot).unwrap();
    // 8 bytes for data length (usize), since wincode uses fixed-length integer encoding
    let data_len_bytes = 8;

    // `desired_size` is the total serialized payload budget; subtract the framing
    // overhead to get the raw data length. A `desired_size` smaller than the
    // overhead (e.g. the `restore_tiny` case) saturates to an empty data vec
    // rather than underflow-panicking.
    let size = desired_size.saturating_sub(parent_bytes + slot_bytes + data_len_bytes);
    let mut data = vec![0; size];
    let mut rng = rng();
    rng.fill_bytes(&mut data);

    SlicePayload { slot, parent, data }
}

/// Creates a [`Slice`] with a random payload of desired size (in bytes).
///
/// The slice does not contain valid transactions.
/// This function should only be used for testing and benchmarking.
//
// XXX: This is only used in test and benchmarking code.  Ensure it is only compiled when we are testing or benchmarking.
pub fn create_slice_with_invalid_txs(desired_size: usize) -> Slice {
    let slot = Slot::new(0);
    let payload = create_slice_payload_with_invalid_txs(slot, None, desired_size);
    let header = SliceHeader {
        slot,
        slice_index: SliceIndex::first(),
        is_last: true,
    };
    Slice::from_parts(header, payload, None)
}
