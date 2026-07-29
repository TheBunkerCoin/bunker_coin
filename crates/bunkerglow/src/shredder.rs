// Copyright (c) Anza Technology, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Block shredding and deshredding.
//!
//! Provides [`Shredder`] implementations plus the [`Shred`] wire unit carrying
//! slice payload, Merkle proof, and leader signature.

mod pool;
mod reed_solomon;
mod shred_index;
mod validated_shred;
mod validated_shreds;

use aes::Aes128;
use aes::cipher::{Array, KeyIvInit, StreamCipher};
use ctr::Ctr64LE;
use rand::{RngCore, rng};
use thiserror::Error;
use wincode::{SchemaRead, SchemaWrite};

pub use self::pool::{ShredderGuard, ShredderPool};
use self::reed_solomon::{
    RawShreds, ReedSolomonCoder, ReedSolomonDeshredError, ReedSolomonShredError,
};
pub use self::shred_index::ShredIndex;
pub use self::validated_shred::{ShredVerifyError, ValidatedShred};
use crate::crypto::merkle::{SliceMerkleTree, SliceProof, SliceRoot};
use crate::crypto::signature::{SecretKey, Signature};
use crate::crypto::{MerkleTree, hash};
use crate::shredder::validated_shreds::ValidatedShreds;
use crate::types::{Slice, SliceHeader, SlicePayload};

/// Number of data shreds the payload of a slice is split into.
pub const DATA_SHREDS: usize = 4;
/// Total shreds emitted per slice, including data and coding shreds.
pub const TOTAL_SHREDS: usize = 6;
/// Maximum number of payload bytes a single shred can hold.
pub const MAX_DATA_PER_SHRED: usize = 1024;
/// Maximum number of bytes an entire slice can hold, incl. padding.
pub const MAX_DATA_PER_SLICE_AFTER_PADDING: usize = DATA_SHREDS * MAX_DATA_PER_SHRED;
/// Maximum slice payload; padding requires one reserved byte.
pub const MAX_DATA_PER_SLICE: usize = MAX_DATA_PER_SLICE_AFTER_PADDING - 1;

/// Shredding errors.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum ShredError {
    #[error("too much data to fit into slice")]
    TooMuchData,
}

impl From<ReedSolomonShredError> for ShredError {
    fn from(err: ReedSolomonShredError) -> Self {
        match err {
            ReedSolomonShredError::TooMuchData => Self::TooMuchData,
        }
    }
}

/// Deshredding errors.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum DeshredError {
    #[error("could not deshred malformed input")]
    BadEncoding,
    #[error("too much data to fit into slice")]
    TooMuchData,
    #[error("not enough shreds to deshred")]
    NotEnoughShreds,
    #[error("shreds are part of invalid Merkle tree")]
    InvalidMerkleTree,
    #[error("shreds array contains invalid sequence")]
    InvalidLayout,
    #[error("reconstructed slice slot does not match shred header slot")]
    SlotMismatch,
}

impl From<crate::types::slice::SliceSlotMismatch> for DeshredError {
    fn from(_: crate::types::slice::SliceSlotMismatch) -> Self {
        Self::SlotMismatch
    }
}

impl From<ReedSolomonDeshredError> for DeshredError {
    fn from(err: ReedSolomonDeshredError) -> Self {
        match err {
            ReedSolomonDeshredError::TooMuchData => Self::TooMuchData,
            ReedSolomonDeshredError::NotEnoughShreds => Self::NotEnoughShreds,
            ReedSolomonDeshredError::InvalidPadding => Self::BadEncoding,
        }
    }
}

impl From<ReedSolomonShredError> for DeshredError {
    fn from(err: ReedSolomonShredError) -> Self {
        match err {
            ReedSolomonShredError::TooMuchData => Self::TooMuchData,
        }
    }
}

#[derive(Clone, Debug, SchemaRead, SchemaWrite)]
pub enum ShredPayloadType {
    Data(ShredPayload),
    Coding(ShredPayload),
}

/// Smallest block dissemination unit, sized to fit an MTU packet.
#[derive(Clone, Debug, SchemaRead, SchemaWrite)]
pub struct Shred {
    pub(crate) payload_type: ShredPayloadType,
    pub(crate) merkle_root: SliceRoot,
    merkle_root_sig: Signature,
    merkle_path: SliceProof,
}

impl Shred {
    /// Verifies only the Merkle proof; full validation uses [`ValidatedShred::try_new`].
    #[must_use]
    pub fn verify_path_only(&self, root: &SliceRoot) -> bool {
        if &self.merkle_root != root {
            return false;
        }
        SliceMerkleTree::check_proof(
            &self.payload().data,
            *self.payload().shred_index,
            &self.merkle_root,
            &self.merkle_path,
        )
    }

    /// Returns the slot number this shred belongs to.
    #[must_use]
    pub const fn slot(&self) -> crate::Slot {
        self.payload().header.slot
    }

    /// Returns the index of this shred within the entire slot.
    #[must_use]
    pub fn index_in_slot(&self) -> usize {
        self.payload().index_in_slot()
    }

    pub const fn payload(&self) -> &ShredPayload {
        match &self.payload_type {
            ShredPayloadType::Coding(p) | ShredPayloadType::Data(p) => p,
        }
    }

    /// Mutably references the payload contained in this shred.
    pub const fn payload_mut(&mut self) -> &mut ShredPayload {
        match &mut self.payload_type {
            ShredPayloadType::Coding(p) | ShredPayloadType::Data(p) => p,
        }
    }

    /// Returns `true` iff this is a data shred.
    pub const fn is_data(&self) -> bool {
        matches!(self.payload_type, ShredPayloadType::Data(_))
    }

    /// Returns `true` iff this is a coding shred.
    pub const fn is_coding(&self) -> bool {
        matches!(self.payload_type, ShredPayloadType::Coding(_))
    }
}

/// Base payload of a shred, regardless of its type.
#[derive(Clone, Debug, SchemaRead, SchemaWrite)]
pub struct ShredPayload {
    /// Slice header replicated in each shred.
    pub(crate) header: SliceHeader,
    /// Index of this shred within the slice.
    pub(crate) shred_index: ShredIndex,
    /// Raw payload bytes of this shred, part of the erasure-coded slice payload.
    pub(crate) data: Vec<u8>,
}

impl ShredPayload {
    /// Returns the index of this shred within the entire slot.
    #[must_use]
    pub fn index_in_slot(&self) -> usize {
        self.header.slice_index.inner() * TOTAL_SHREDS + *self.shred_index
    }

    /// Returns the slot number this shred belongs to.
    #[must_use]
    pub const fn slot(&self) -> crate::Slot {
        self.header.slot
    }
}

/// Converts slice payloads to shreds and reconstructs slices from shreds.
pub trait Shredder: Default {
    /// Maximum payload bytes this shredder can fit in a slice.
    const MAX_DATA_SIZE: usize;

    /// Data shreds produced by [`Shredder::shred`].
    const DATA_OUTPUT_SHREDS: usize;

    /// Coding shreds produced by [`Shredder::shred`].
    const CODING_OUTPUT_SHREDS: usize;

    /// Splits a slice into [`TOTAL_SHREDS`] data/coding shreds.
    ///
    /// # Errors
    /// Returns [`ShredError::TooMuchData`] when `slice` exceeds [`Shredder::MAX_DATA_SIZE`].
    fn shred(
        &mut self,
        slice: Slice,
        sk: &SecretKey,
    ) -> Result<[ValidatedShred; TOTAL_SHREDS], ShredError>;

    /// Reconstructs a slice and all [`TOTAL_SHREDS`] shreds under its Merkle tree.
    ///
    /// # Errors
    /// Returns layout, size, decode, or Merkle validation errors.
    fn deshred(
        &mut self,
        shreds: &[Option<ValidatedShred>; TOTAL_SHREDS],
    ) -> Result<(Slice, [ValidatedShred; TOTAL_SHREDS]), DeshredError> {
        let shreds =
            ValidatedShreds::try_new(shreds, Self::DATA_OUTPUT_SHREDS, Self::CODING_OUTPUT_SHREDS)
                .ok_or(DeshredError::InvalidLayout)?;
        self.deshred_validated_shreds(shreds)
    }

    /// Core deshredding implementation used by [`Shredder::deshred`].
    fn deshred_validated_shreds(
        &mut self,
        shreds: ValidatedShreds,
    ) -> Result<(Slice, [ValidatedShred; TOTAL_SHREDS]), DeshredError>;
}

/// Reed-Solomon shredder that emits data shreds plus coding shreds.
pub struct RegularShredder(ReedSolomonCoder);

impl Shredder for RegularShredder {
    const MAX_DATA_SIZE: usize = MAX_DATA_PER_SLICE;
    const DATA_OUTPUT_SHREDS: usize = DATA_SHREDS;
    const CODING_OUTPUT_SHREDS: usize = TOTAL_SHREDS - DATA_SHREDS;

    fn shred(
        &mut self,
        slice: Slice,
        sk: &SecretKey,
    ) -> Result<[ValidatedShred; TOTAL_SHREDS], ShredError> {
        let (header, payload) = slice.deconstruct();
        let raw_shreds = self.0.shred(&payload.to_bytes())?;
        Ok(data_and_coding_to_output_shreds(header, raw_shreds, sk))
    }

    fn deshred_validated_shreds(
        &mut self,
        shreds: ValidatedShreds,
    ) -> Result<(Slice, [ValidatedShred; TOTAL_SHREDS]), DeshredError> {
        let payload_bytes = self.0.deshred(shreds)?;
        let payload = SlicePayload::from(payload_bytes.as_slice());

        // Validated layout guarantees at least one shred.
        let any_shred = shreds.to_shreds().iter().find_map(|s| s.as_ref()).unwrap();
        let slice = Slice::from_shreds_checked(payload, any_shred)?;
        let header = slice.to_header();

        // Verify reconstructed shreds against the signed Merkle root.
        let merkle_root = any_shred.merkle_root.clone();
        let raw_shreds = self.0.shred(&payload_bytes)?;
        let tree = build_merkle_tree(&raw_shreds);
        if tree.get_root() != merkle_root {
            return Err(DeshredError::InvalidMerkleTree);
        }

        // Rebuild output shreds with root, path, and leader signature.
        let leader_sig = any_shred.merkle_root_sig;
        let reconstructed_shreds =
            create_output_shreds_for_other_leader(header, raw_shreds, tree, leader_sig);

        assert_eq!(reconstructed_shreds.len(), TOTAL_SHREDS);
        Ok((slice, reconstructed_shreds))
    }
}

impl Default for RegularShredder {
    fn default() -> Self {
        Self(ReedSolomonCoder::new(Self::CODING_OUTPUT_SHREDS))
    }
}

/// Reed-Solomon shredder that emits only coding shreds.
pub struct CodingOnlyShredder(ReedSolomonCoder);

impl Shredder for CodingOnlyShredder {
    const MAX_DATA_SIZE: usize = MAX_DATA_PER_SLICE;
    const DATA_OUTPUT_SHREDS: usize = 0;
    const CODING_OUTPUT_SHREDS: usize = TOTAL_SHREDS;

    fn shred(
        &mut self,
        slice: Slice,
        sk: &SecretKey,
    ) -> Result<[ValidatedShred; TOTAL_SHREDS], ShredError> {
        let (header, payload) = slice.deconstruct();
        let mut raw_shreds = self.0.shred(&payload.to_bytes())?;
        raw_shreds.data = vec![];
        Ok(data_and_coding_to_output_shreds(header, raw_shreds, sk))
    }

    fn deshred_validated_shreds(
        &mut self,
        shreds: ValidatedShreds,
    ) -> Result<(Slice, [ValidatedShred; TOTAL_SHREDS]), DeshredError> {
        let payload_bytes = self.0.deshred(shreds)?;
        let payload = SlicePayload::from(payload_bytes.as_slice());

        // Validated layout guarantees at least one shred.
        let any_shred = shreds.to_shreds().iter().find_map(|s| s.as_ref()).unwrap();
        let slice = Slice::from_shreds_checked(payload, any_shred)?;

        // Verify reconstructed shreds against the signed Merkle root.
        let merkle_root = any_shred.merkle_root.clone();
        let mut raw_shreds = self.0.shred(&payload_bytes)?;
        raw_shreds.data = vec![];
        let tree = build_merkle_tree(&raw_shreds);
        if tree.get_root() != merkle_root {
            return Err(DeshredError::InvalidMerkleTree);
        }

        // Rebuild output shreds with root, path, and leader signature.
        let (header, _payload) = slice.clone().deconstruct();
        let leader_sig = any_shred.merkle_root_sig;
        let reconstructed_shreds =
            create_output_shreds_for_other_leader(header, raw_shreds, tree, leader_sig);

        assert_eq!(reconstructed_shreds.len(), TOTAL_SHREDS);
        Ok((slice, reconstructed_shreds))
    }
}

impl Default for CodingOnlyShredder {
    fn default() -> Self {
        Self(ReedSolomonCoder::new(Self::CODING_OUTPUT_SHREDS))
    }
}

/// PETS all-or-nothing shredder.
/// Emits one fewer data shred and one extra coding shred; see <https://arxiv.org/abs/2502.02774>.
pub struct PetsShredder(ReedSolomonCoder);

impl Shredder for PetsShredder {
    // Reserve 16 bytes for the symmetric key.
    const MAX_DATA_SIZE: usize = MAX_DATA_PER_SLICE - 16;
    const DATA_OUTPUT_SHREDS: usize = DATA_SHREDS - 1;
    const CODING_OUTPUT_SHREDS: usize = TOTAL_SHREDS - DATA_SHREDS + 1;

    fn shred(
        &mut self,
        slice: Slice,
        sk: &SecretKey,
    ) -> Result<[ValidatedShred; TOTAL_SHREDS], ShredError> {
        let (header, payload) = slice.deconstruct();
        let mut payload: Vec<u8> = payload.into();
        assert!(payload.len() <= Self::MAX_DATA_SIZE);

        let mut rng = rng();
        let mut key = Array::from([0; 16]);
        rng.fill_bytes(&mut key);
        let iv = Array::from([0; 16]);

        let mut cipher = Ctr64LE::<Aes128>::new(&key, &iv);
        cipher.apply_keystream(&mut payload);

        payload.extend_from_slice(&key);
        let mut raw_shreds = self.0.shred(&payload)?;
        // Drop the data shred containing the key.
        raw_shreds.data.pop();

        Ok(data_and_coding_to_output_shreds(header, raw_shreds, sk))
    }

    fn deshred_validated_shreds(
        &mut self,
        shreds: ValidatedShreds,
    ) -> Result<(Slice, [ValidatedShred; TOTAL_SHREDS]), DeshredError> {
        let mut buffer = self.0.deshred(shreds)?;
        if buffer.len() < 16 {
            return Err(DeshredError::BadEncoding);
        }

        // Validated layout guarantees at least one shred.
        let any_shred = shreds.to_shreds().iter().find_map(|s| s.as_ref()).unwrap();

        // Verify reconstructed shreds against the signed Merkle root.
        let merkle_root = any_shred.merkle_root.clone();
        let header = any_shred.payload().header.clone();
        let mut raw_shreds = self.0.shred(&buffer)?;
        raw_shreds.data.pop();
        let tree = build_merkle_tree(&raw_shreds);
        if tree.get_root() != merkle_root {
            return Err(DeshredError::InvalidMerkleTree);
        }

        let tail = buffer.split_off(buffer.len() - 16);
        let iv = Array::from([0; 16]);
        let key = Array::try_from(tail.as_slice()).expect("tail should have correct length");

        let mut cipher = Ctr64LE::<Aes128>::new(&key, &iv);
        cipher.apply_keystream(&mut buffer);
        let payload = SlicePayload::from(buffer.as_slice());
        let slice = Slice::from_shreds_checked(payload, any_shred)?;

        // Rebuild output shreds with root, path, and leader signature.
        let leader_sig = any_shred.merkle_root_sig;
        let reconstructed_shreds =
            create_output_shreds_for_other_leader(header, raw_shreds, tree, leader_sig);

        assert_eq!(reconstructed_shreds.len(), TOTAL_SHREDS);
        Ok((slice, reconstructed_shreds))
    }
}

impl Default for PetsShredder {
    fn default() -> Self {
        Self(ReedSolomonCoder::new(Self::CODING_OUTPUT_SHREDS))
    }
}

/// RAONT-RS all-or-nothing shredder.
/// Emits normal data/coding counts; see <https://eprint.iacr.org/2016/1014>.
pub struct AontShredder(ReedSolomonCoder);

impl Shredder for AontShredder {
    // Reserve 16 bytes for the symmetric key.
    const MAX_DATA_SIZE: usize = MAX_DATA_PER_SLICE - 16;
    const DATA_OUTPUT_SHREDS: usize = DATA_SHREDS;
    const CODING_OUTPUT_SHREDS: usize = TOTAL_SHREDS - DATA_SHREDS;

    fn shred(
        &mut self,
        slice: Slice,
        sk: &SecretKey,
    ) -> Result<[ValidatedShred; TOTAL_SHREDS], ShredError> {
        let (header, payload) = slice.deconstruct();
        let mut payload: Vec<u8> = payload.into();
        assert!(payload.len() <= Self::MAX_DATA_SIZE);

        let mut rng = rng();
        let mut key = Array::from([0; 16]);
        rng.fill_bytes(&mut key);
        let iv = Array::from([0; 16]);

        let mut cipher = Ctr64LE::<Aes128>::new(&key, &iv);
        cipher.apply_keystream(&mut payload);

        let hash = hash(&payload);
        for i in 0..16 {
            payload.push(hash.as_ref()[i] ^ key[i]);
        }

        let raw_shreds = self.0.shred(&payload)?;
        Ok(data_and_coding_to_output_shreds(header, raw_shreds, sk))
    }

    fn deshred_validated_shreds(
        &mut self,
        shreds: ValidatedShreds,
    ) -> Result<(Slice, [ValidatedShred; TOTAL_SHREDS]), DeshredError> {
        let mut buffer = self.0.deshred(shreds)?;
        if buffer.len() < 16 {
            return Err(DeshredError::BadEncoding);
        }

        // Validated layout guarantees at least one shred.
        let any_shred = shreds.to_shreds().iter().find_map(|s| s.as_ref()).unwrap();

        // Verify reconstructed shreds against the signed Merkle root.
        let merkle_root = any_shred.merkle_root.clone();
        let header = any_shred.payload().header.clone();
        let raw_shreds = self.0.shred(&buffer)?;
        let tree = build_merkle_tree(&raw_shreds);
        if tree.get_root() != merkle_root {
            return Err(DeshredError::InvalidMerkleTree);
        }

        let tail = buffer.split_off(buffer.len() - 16);
        let hash = hash(&buffer);

        let iv = Array::from([0; 16]);
        let mut key = Array::try_from(tail.as_slice()).unwrap();
        for i in 0..16 {
            key[i] ^= hash.as_ref()[i];
        }

        let mut cipher = Ctr64LE::<Aes128>::new(&key, &iv);
        cipher.apply_keystream(&mut buffer);
        let payload = SlicePayload::from(buffer.as_slice());
        let slice = Slice::from_shreds_checked(payload, any_shred)?;

        // Rebuild output shreds with root, path, and leader signature.
        let leader_sig = any_shred.merkle_root_sig;
        let reconstructed_shreds =
            create_output_shreds_for_other_leader(header, raw_shreds, tree, leader_sig);

        assert_eq!(reconstructed_shreds.len(), TOTAL_SHREDS);
        Ok((slice, reconstructed_shreds))
    }
}

impl Default for AontShredder {
    fn default() -> Self {
        Self(ReedSolomonCoder::new(Self::CODING_OUTPUT_SHREDS))
    }
}

/// Builds the Merkle tree, signs the root, and returns validated shreds.
fn data_and_coding_to_output_shreds(
    header: SliceHeader,
    raw_shreds: RawShreds,
    sk: &SecretKey,
) -> [ValidatedShred; TOTAL_SHREDS] {
    let tree = build_merkle_tree(&raw_shreds);
    let merkle_root = tree.get_root();
    let merkle_root_sig = sk.sign(merkle_root.as_ref());

    let convert = |shred_index: ShredIndex, data: Vec<u8>| -> (SliceProof, ShredPayload) {
        let merkle_path = tree.create_proof(*shred_index);
        let payload = ShredPayload {
            header: header.clone(),
            shred_index,
            data,
        };
        (merkle_path, payload)
    };
    let num_data = raw_shreds.data.len();
    let data = raw_shreds
        .data
        .into_iter()
        .enumerate()
        .map(|(shred_index, d)| {
            let shred_index = ShredIndex::new(shred_index).unwrap();
            let (merkle_path, payload) = convert(shred_index, d);
            (merkle_path, ShredPayloadType::Data(payload))
        });
    let coding = raw_shreds
        .coding
        .into_iter()
        .enumerate()
        .map(|(offset, c)| {
            let shred_index = num_data + offset;
            let shred_index = ShredIndex::new(shred_index).unwrap();
            let (merkle_path, payload) = convert(shred_index, c);
            (merkle_path, ShredPayloadType::Coding(payload))
        });
    data.chain(coding)
        .map(|(merkle_path, payload)| {
            ValidatedShred::new_validated(Shred {
                payload_type: payload,
                merkle_root: merkle_root.clone(),
                merkle_root_sig,
                merkle_path,
            })
        })
        .collect::<Vec<_>>()
        .try_into()
        .unwrap()
}

/// Rebuilds validated shreds for another leader using its existing signature.
fn create_output_shreds_for_other_leader(
    header: SliceHeader,
    raw_shreds: RawShreds,
    tree: SliceMerkleTree,
    leader_signature: Signature,
) -> [ValidatedShred; TOTAL_SHREDS] {
    let convert = |shred_index: ShredIndex, data: Vec<u8>| -> (SliceProof, ShredPayload) {
        let merkle_path = tree.create_proof(*shred_index);
        let payload = ShredPayload {
            header: header.clone(),
            shred_index,
            data,
        };
        (merkle_path, payload)
    };
    let num_data = raw_shreds.data.len();
    let data = raw_shreds
        .data
        .into_iter()
        .enumerate()
        .map(|(shred_index, d)| {
            let shred_index = ShredIndex::new(shred_index).unwrap();
            let (merkle_path, payload) = convert(shred_index, d);
            (merkle_path, ShredPayloadType::Data(payload))
        });
    let coding = raw_shreds
        .coding
        .into_iter()
        .enumerate()
        .map(|(offset, c)| {
            let shred_index = num_data + offset;
            let shred_index = ShredIndex::new(shred_index).unwrap();
            let (merkle_path, payload) = convert(shred_index, c);
            (merkle_path, ShredPayloadType::Coding(payload))
        });
    let merkle_root = tree.get_root().clone();
    data.chain(coding)
        .map(|(merkle_path, payload)| {
            ValidatedShred::new_validated(Shred {
                payload_type: payload,
                merkle_root: merkle_root.clone(),
                merkle_root_sig: leader_signature,
                merkle_path,
            })
        })
        .collect::<Vec<_>>()
        .try_into()
        .unwrap()
}

/// Builds the Merkle tree whose leaves are the raw shreds.
fn build_merkle_tree(raw_shreds: &RawShreds) -> SliceMerkleTree {
    let leaves = raw_shreds.data.iter().chain(&raw_shreds.coding);
    MerkleTree::new(leaves)
}

#[cfg(test)]
mod tests {
    use color_eyre::Result;

    use super::*;
    use crate::Slot;
    use crate::crypto::merkle::{BlockHash, GENESIS_BLOCK_HASH};
    use crate::types::SliceIndex;
    use crate::types::slice::create_slice_with_invalid_txs;

    fn first_slice_at(slot: u64, parent: Option<(Slot, BlockHash)>, data: Vec<u8>) -> Slice {
        let slot = Slot::new(slot);
        Slice::from_parts(
            SliceHeader {
                slot,
                slice_index: SliceIndex::first(),
                is_last: true,
            },
            SlicePayload::new(slot, parent, data),
            None,
        )
    }

    /// A block's Merkle root must bind its own slot, not only its parent.
    #[test]
    fn empty_blocks_same_parent_different_slot_hash_differently() {
        let mut shredder = RegularShredder::default();
        let sk = SecretKey::new(&mut rng());

        let parent = Some((Slot::new(3), GENESIS_BLOCK_HASH));
        let slice_slot4 = first_slice_at(4, parent.clone(), vec![]);
        let slice_slot8 = first_slice_at(8, parent, vec![]);

        let root4 = shredder.shred(slice_slot4, &sk).unwrap()[0]
            .merkle_root
            .clone();
        let root8 = shredder.shred(slice_slot8, &sk).unwrap()[0]
            .merkle_root
            .clone();

        assert_ne!(
            root4, root8,
            "slice Merkle root must bind the block's own slot (it is now in the \
             hashed payload); same-parent empty blocks at different slots must differ"
        );
    }

    /// Deshredding rejects header slots that disagree with the signed payload slot.
    #[test]
    fn deshred_rejects_tampered_header_slot() {
        let mut shredder = RegularShredder::default();
        let sk = SecretKey::new(&mut rng());

        let slice = first_slice_at(5, Some((Slot::new(4), GENESIS_BLOCK_HASH)), vec![]);
        let shreds = shredder.shred(slice, &sk).unwrap();

        let mut tampered = into_array(&shreds);
        for s in tampered.iter_mut().flatten() {
            s.payload_mut().header.slot = Slot::new(9);
        }

        let result = shredder.deshred(&tampered);
        assert_eq!(
            result.err(),
            Some(DeshredError::SlotMismatch),
            "deshred must reject a header slot that disagrees with the signed payload slot"
        );
    }

    /// A block's Merkle root must also bind its parent.
    #[test]
    fn empty_blocks_different_parent_hash_differently() {
        let mut shredder = RegularShredder::default();
        let sk = SecretKey::new(&mut rng());

        let on_parent3 = first_slice_at(4, Some((Slot::new(3), GENESIS_BLOCK_HASH)), vec![]);
        let on_parent2 = first_slice_at(4, Some((Slot::new(2), GENESIS_BLOCK_HASH)), vec![]);

        let root_a = shredder.shred(on_parent3, &sk).unwrap()[0]
            .merkle_root
            .clone();
        let root_b = shredder.shred(on_parent2, &sk).unwrap()[0]
            .merkle_root
            .clone();

        assert_ne!(
            root_a, root_b,
            "slice Merkle root must bind the parent (it is in the hashed payload)"
        );
    }

    fn into_array(shreds: &[ValidatedShred]) -> [Option<ValidatedShred>; TOTAL_SHREDS] {
        assert!(shreds.len() <= TOTAL_SHREDS);
        let mut ret = [const { None }; TOTAL_SHREDS];
        for shred in shreds {
            ret[*shred.payload().shred_index] = Some(shred.clone());
        }
        ret
    }

    #[test]
    fn regular_shredding() -> Result<()> {
        let mut shredder = RegularShredder::default();
        let sk = SecretKey::new(&mut rng());
        let mut slice = create_slice_with_invalid_txs(MAX_DATA_PER_SLICE);
        let shreds = shredder.shred(slice.clone(), &sk)?;
        assert_eq!(shreds.len(), TOTAL_SHREDS);

        let all = into_array(&shreds);
        let (slice_restored, _) = shredder.deshred(&all)?;
        slice.merkle_root = slice_restored.merkle_root.clone();
        assert_eq!(slice_restored, slice);

        let coding = into_array(&shreds[..DATA_SHREDS]);
        let (slice_restored, _) = shredder.deshred(&coding)?;
        assert_eq!(slice_restored, slice);

        let data = into_array(&shreds[TOTAL_SHREDS - DATA_SHREDS..]);
        let (slice_restored, _) = shredder.deshred(&data)?;
        assert_eq!(slice_restored, slice);

        let nc_shreds = [&shreds[..1], &shreds[2..]].concat();
        let nc_shreds = into_array(&nc_shreds);
        let (slice_restored, _) = shredder.deshred(&nc_shreds)?;
        assert_eq!(slice_restored, slice);

        let start = DATA_SHREDS / 2;
        let end = DATA_SHREDS / 2 + DATA_SHREDS;
        let input = into_array(&shreds[start..end]);
        let (slice_restored, _) = shredder.deshred(&input)?;
        assert_eq!(slice_restored, slice);

        let input = into_array(&shreds[1..]);
        let (slice_restored, _) = shredder.deshred(&input)?;
        assert_eq!(slice_restored, slice);

        let input = into_array(&shreds[..1]);
        let result = shredder.deshred(&input);
        assert_eq!(result.err(), Some(DeshredError::NotEnoughShreds));

        let input = into_array(&shreds[..DATA_SHREDS - 1]);
        let result = shredder.deshred(&input);
        assert_eq!(result.err(), Some(DeshredError::NotEnoughShreds));

        Ok(())
    }

    #[test]
    fn coding_only_shredding() -> Result<()> {
        let mut shredder = CodingOnlyShredder::default();
        let sk = SecretKey::new(&mut rng());
        let mut slice = create_slice_with_invalid_txs(MAX_DATA_PER_SLICE);
        let shreds = shredder.shred(slice.clone(), &sk)?;
        assert_eq!(shreds.len(), TOTAL_SHREDS);

        let input = into_array(&shreds);
        let (slice_restored, _) = shredder.deshred(&input)?;
        slice.merkle_root = slice_restored.merkle_root.clone();
        assert_eq!(slice_restored, slice);

        let input = into_array(&shreds[..DATA_SHREDS]);
        let (slice_restored, _) = shredder.deshred(&input)?;
        assert_eq!(slice_restored, slice);

        let nc_shreds = [&shreds[..1], &shreds[2..]].concat();
        let input = into_array(&nc_shreds);
        let (slice_restored, _) = shredder.deshred(&input)?;
        assert_eq!(slice_restored, slice);

        let input = into_array(&shreds[1..]);
        let (slice_restored, _) = shredder.deshred(&input)?;
        assert_eq!(slice_restored, slice);

        let input = into_array(&shreds[..1]);
        let result = shredder.deshred(&input);
        assert_eq!(result.err(), Some(DeshredError::NotEnoughShreds));

        let input = into_array(&shreds[..DATA_SHREDS - 1]);
        let result = shredder.deshred(&input);
        assert_eq!(result.err(), Some(DeshredError::NotEnoughShreds));

        Ok(())
    }

    #[test]
    fn aont_shredding() -> Result<()> {
        let mut shredder = AontShredder::default();
        let sk = SecretKey::new(&mut rng());
        let mut slice = create_slice_with_invalid_txs(MAX_DATA_PER_SLICE - 16);
        let shreds = shredder.shred(slice.clone(), &sk)?;
        assert_eq!(shreds.len(), TOTAL_SHREDS);

        let input = into_array(&shreds);
        let (slice_restored, _) = shredder.deshred(&input)?;
        slice.merkle_root = slice_restored.merkle_root.clone();
        assert_eq!(slice_restored, slice);

        let input = into_array(&shreds[..DATA_SHREDS]);
        let (slice_restored, _) = shredder.deshred(&input)?;
        assert_eq!(slice_restored, slice);

        let nc_shreds = [&shreds[..1], &shreds[2..]].concat();
        let input = into_array(&nc_shreds);
        let (slice_restored, _) = shredder.deshred(&input)?;
        assert_eq!(slice_restored, slice);

        let start = DATA_SHREDS / 2;
        let end = DATA_SHREDS / 2 + DATA_SHREDS;
        let input = into_array(&shreds[start..end]);
        let (slice_restored, _) = shredder.deshred(&input)?;
        assert_eq!(slice_restored, slice);

        let input = into_array(&shreds[1..]);
        let (slice_restored, _) = shredder.deshred(&input)?;
        assert_eq!(slice_restored, slice);

        let input = into_array(&shreds[..1]);
        let result = shredder.deshred(&input);
        assert_eq!(result.err(), Some(DeshredError::NotEnoughShreds));

        let input = into_array(&shreds[..DATA_SHREDS - 1]);
        let result = shredder.deshred(&input);
        assert_eq!(result.err(), Some(DeshredError::NotEnoughShreds));

        Ok(())
    }

    #[test]
    fn pets_shredding() -> Result<()> {
        let mut shredder = PetsShredder::default();
        let sk = SecretKey::new(&mut rng());
        let mut slice = create_slice_with_invalid_txs(MAX_DATA_PER_SLICE - 16);
        let shreds = shredder.shred(slice.clone(), &sk)?;
        assert_eq!(shreds.len(), TOTAL_SHREDS);

        let input = into_array(&shreds);
        let (slice_restored, _) = shredder.deshred(&input)?;
        slice.merkle_root = slice_restored.merkle_root.clone();
        assert_eq!(slice_restored, slice);

        let input = into_array(&shreds[..DATA_SHREDS]);
        let (slice_restored, _) = shredder.deshred(&input)?;
        assert_eq!(slice_restored, slice);

        let nc_shreds = [&shreds[..1], &shreds[2..]].concat();
        let input = into_array(&nc_shreds);
        let (slice_restored, _) = shredder.deshred(&input)?;
        assert_eq!(slice_restored, slice);

        let start = DATA_SHREDS / 2;
        let end = DATA_SHREDS / 2 + DATA_SHREDS;
        let input = into_array(&shreds[start..end]);
        let (slice_restored, _) = shredder.deshred(&input)?;
        assert_eq!(slice_restored, slice);

        let input = into_array(&shreds[1..]);
        let (slice_restored, _) = shredder.deshred(&input)?;
        assert_eq!(slice_restored, slice);

        let input = into_array(&shreds[..1]);
        let result = shredder.deshred(&input);
        assert_eq!(result.err(), Some(DeshredError::NotEnoughShreds));

        let input = into_array(&shreds[..DATA_SHREDS - 1]);
        let result = shredder.deshred(&input);
        assert_eq!(result.err(), Some(DeshredError::NotEnoughShreds));

        Ok(())
    }
}
