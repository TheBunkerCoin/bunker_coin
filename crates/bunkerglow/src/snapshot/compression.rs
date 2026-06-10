// BunkerCoin
// SPDX-License-Identifier: Apache-2.0

//! Snapshot proof compression for bootstrapping (whitepaper section 3.3).
//!
//! A [`BootstrapSnapshot`] carries a full Merkle path per chunk, which costs
//! `chunk_count * tree_height * 32` bytes even though adjacent paths share
//! almost all of their sibling hashes. Over HF radio links every byte counts,
//! so this module replaces the per-chunk paths with a single combined proof.
//!
//! Two proof schemes are supported:
//!
//! - **Merkle multiproof** (implemented): deduplicates the shared sibling
//!   hashes across all chunk proofs. When all chunks are present — the normal
//!   bootstrap case, since every chunk is needed to reconstruct the state —
//!   the proof is empty: the verifier recomputes the chunk root from the
//!   chunk data itself. For partial chunk sets (verifying a batch received
//!   over radio before requesting the rest) only the non-derivable frontier
//!   siblings are included.
//! - **Groth16** (stub, deferred): a succinct ZK proof of the same statement
//!   in [`SNAPSHOT_ZK_PROOF_SIZE`] bytes, independent of chunk count. The
//!   whitepaper marks this as optional; verification currently returns
//!   [`BootstrapError::UnsupportedProofScheme`].

use bunker_coin_core::execution::State;
use serde::{Deserialize, Serialize};

use super::{BootstrapError, BootstrapSnapshot, SnapshotManifest};
use crate::crypto::Hash;
use crate::crypto::merkle::PlainMerkleTree;

/// Size of a succinct snapshot ZK proof in bytes (Groth16 over BLS12-381,
/// matching the 288-byte proof format used for message anchoring).
pub const SNAPSHOT_ZK_PROOF_SIZE: usize = 288;

/// A compressed proof that a set of snapshot chunks belongs to a chunk root.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressedSnapshotProof {
    /// Combined Merkle multiproof: only the sibling hashes the verifier
    /// cannot derive from the chunks themselves. Empty for a complete set.
    MerkleMultiproof { siblings: Vec<Hash> },
    /// Succinct ZK proof (deferred). Verification is not yet implemented.
    Groth16 { proof: Vec<u8> },
}

/// A bootstrap snapshot with per-chunk Merkle paths replaced by one
/// [`CompressedSnapshotProof`].
///
/// `chunk_data` is ordered by chunk index and carries the raw chunk bytes
/// only; build with [`compress_bootstrap_snapshot`] and restore with
/// [`restore_state_from_compressed`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompressedSnapshot {
    pub manifest: SnapshotManifest,
    pub chunk_data: Vec<Vec<u8>>,
    pub proof: CompressedSnapshotProof,
}

/// Compresses a bootstrap snapshot by dropping all per-chunk Merkle paths.
///
/// Since the complete chunk set lets the verifier recompute the chunk root
/// directly, the resulting multiproof is empty — the entire
/// `chunk_count * tree_height * 32` bytes of proof data disappear.
pub fn compress_bootstrap_snapshot(
    snapshot: &BootstrapSnapshot,
) -> Result<CompressedSnapshot, BootstrapError> {
    let manifest = &snapshot.manifest;
    if snapshot.chunks.len() != manifest.chunk_count {
        return Err(BootstrapError::WrongChunkCount {
            expected: manifest.chunk_count,
            got: snapshot.chunks.len(),
        });
    }

    let mut chunk_data = vec![Vec::new(); manifest.chunk_count];
    for chunk in &snapshot.chunks {
        if chunk.epoch != manifest.epoch {
            return Err(BootstrapError::WrongEpoch {
                expected: manifest.epoch,
                got: chunk.epoch,
            });
        }
        if chunk.index >= manifest.chunk_count {
            return Err(BootstrapError::WrongChunkCount {
                expected: manifest.chunk_count,
                got: chunk.index + 1,
            });
        }
        chunk_data[chunk.index] = chunk.data.clone();
    }

    Ok(CompressedSnapshot {
        manifest: manifest.clone(),
        chunk_data,
        proof: CompressedSnapshotProof::MerkleMultiproof {
            siblings: Vec::new(),
        },
    })
}

/// Verifies a compressed snapshot and restores the state from it.
///
/// Checks the compressed proof against `manifest.chunk_root`, reassembles
/// the state bytes and validates the state hash, mirroring the guarantees
/// of [`super::restore_state_from_chunks`].
pub fn restore_state_from_compressed(
    compressed: &CompressedSnapshot,
) -> Result<State, BootstrapError> {
    let manifest = &compressed.manifest;
    if manifest.chunk_size == 0 {
        return Err(BootstrapError::InvalidChunkSize);
    }
    if manifest.chunk_count == 0 {
        return Err(BootstrapError::EmptySnapshot);
    }
    if compressed.chunk_data.len() != manifest.chunk_count {
        return Err(BootstrapError::WrongChunkCount {
            expected: manifest.chunk_count,
            got: compressed.chunk_data.len(),
        });
    }

    match &compressed.proof {
        CompressedSnapshotProof::MerkleMultiproof { siblings } => {
            let leaves: Vec<(usize, &Vec<u8>)> =
                compressed.chunk_data.iter().enumerate().collect();
            if !PlainMerkleTree::check_multiproof(
                &leaves,
                manifest.chunk_count,
                &manifest.chunk_root,
                siblings,
            ) {
                return Err(BootstrapError::InvalidCompressedProof);
            }
        }
        CompressedSnapshotProof::Groth16 { .. } => {
            return Err(BootstrapError::UnsupportedProofScheme);
        }
    }

    let mut encoded = Vec::with_capacity(manifest.total_bytes);
    for data in &compressed.chunk_data {
        encoded.extend_from_slice(data);
    }
    if encoded.len() != manifest.total_bytes {
        return Err(BootstrapError::TotalSizeMismatch {
            expected: manifest.total_bytes,
            got: encoded.len(),
        });
    }

    let (state, _) =
        bincode::serde::decode_from_slice::<State, _>(&encoded, bincode::config::standard())
            .map_err(|_| BootstrapError::DecodeFailed)?;

    let got = state.compute_state_hash();
    if got != manifest.state_hash {
        return Err(BootstrapError::StateHashMismatch {
            expected: manifest.state_hash,
            got,
        });
    }

    Ok(state)
}

/// Generates a compressed proof for a subset of chunks.
///
/// `chunk_data` must be the complete, index-ordered chunk data of the
/// snapshot (the prover has it; this is for serving partial batches to a
/// bootstrapping node over radio). `indices` are the chunks the proof covers.
///
/// # Panics
///
/// Panics if `indices` is empty or contains an out-of-range index.
pub fn prove_chunk_subset(chunk_data: &[Vec<u8>], indices: &[usize]) -> CompressedSnapshotProof {
    let tree = PlainMerkleTree::new(chunk_data.iter());
    CompressedSnapshotProof::MerkleMultiproof {
        siblings: tree.create_multiproof(indices),
    }
}

/// Verifies a subset of chunks against the manifest's chunk root.
///
/// `chunks` holds `(index, data)` pairs in any order. This lets a
/// bootstrapping node validate each batch of chunks as it arrives,
/// before requesting the next batch.
pub fn verify_chunk_subset(
    manifest: &SnapshotManifest,
    chunks: &[(usize, &Vec<u8>)],
    proof: &CompressedSnapshotProof,
) -> Result<(), BootstrapError> {
    match proof {
        CompressedSnapshotProof::MerkleMultiproof { siblings } => {
            if PlainMerkleTree::check_multiproof(
                chunks,
                manifest.chunk_count,
                &manifest.chunk_root,
                siblings,
            ) {
                Ok(())
            } else {
                Err(BootstrapError::InvalidCompressedProof)
            }
        }
        CompressedSnapshotProof::Groth16 { .. } => Err(BootstrapError::UnsupportedProofScheme),
    }
}

/// Returns a placeholder Groth16 proof of [`SNAPSHOT_ZK_PROOF_SIZE`] bytes.
///
/// Stub for the deferred ZK backend; proofs produced here never verify.
pub fn groth16_proof_stub() -> CompressedSnapshotProof {
    CompressedSnapshotProof::Groth16 {
        proof: vec![0u8; SNAPSHOT_ZK_PROOF_SIZE],
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bunker_coin_core::staking::StakingLedger;

    use super::*;
    use crate::snapshot::build_bootstrap_snapshot;

    fn test_state() -> State {
        let mut state = State {
            accounts: HashMap::new(),
            tokens: HashMap::new(),
            next_token_id: 1,
            tx_fee_pool: 7_777,
            msg_fee_pool: 0,
            bridge_fee_pool: 0,
            staking: StakingLedger::new(),
            current_epoch: 11,
            epoch_messages_anchored: 0,
            epoch_deliveries_completed: 0,
        };
        for i in 0..20u8 {
            state.accounts.insert(
                [i; 32],
                bunker_coin_core::account::Account {
                    native_balance: 1_000 * u64::from(i),
                    token_balances: std::collections::BTreeMap::new(),
                    nonce: u64::from(i),
                },
            );
        }
        state
    }

    #[test]
    fn compress_restore_roundtrip() {
        let state = test_state();
        let snapshot = build_bootstrap_snapshot(11, &state, 64).unwrap();
        assert!(snapshot.manifest.chunk_count > 1);

        let compressed = compress_bootstrap_snapshot(&snapshot).unwrap();
        let restored = restore_state_from_compressed(&compressed).unwrap();
        assert_eq!(restored.compute_state_hash(), state.compute_state_hash());
        assert_eq!(restored.current_epoch, 11);
        assert_eq!(restored.tx_fee_pool, 7_777);
    }

    #[test]
    fn compression_eliminates_proof_bytes() {
        let state = test_state();
        let snapshot = build_bootstrap_snapshot(11, &state, 64).unwrap();

        // per-chunk proofs cost chunk_count * height hashes
        let per_chunk_hashes: usize = snapshot.chunks.iter().map(|c| c.proof.len()).sum();
        assert!(per_chunk_hashes > 0);

        // the compressed proof is empty
        let compressed = compress_bootstrap_snapshot(&snapshot).unwrap();
        match &compressed.proof {
            CompressedSnapshotProof::MerkleMultiproof { siblings } => {
                assert!(siblings.is_empty());
            }
            CompressedSnapshotProof::Groth16 { .. } => panic!("wrong scheme"),
        }

        // and the serialized snapshot shrinks accordingly
        let original = bincode::serde::encode_to_vec(
            (&snapshot.manifest, &snapshot.chunks),
            bincode::config::standard(),
        )
        .unwrap();
        let shrunk =
            bincode::serde::encode_to_vec(&compressed, bincode::config::standard()).unwrap();
        assert!(
            shrunk.len() + per_chunk_hashes * 32 <= original.len() + 64,
            "compressed {} vs original {} ({} proof hashes)",
            shrunk.len(),
            original.len(),
            per_chunk_hashes,
        );
    }

    #[test]
    fn restore_rejects_tampered_chunk() {
        let state = test_state();
        let snapshot = build_bootstrap_snapshot(11, &state, 64).unwrap();
        let mut compressed = compress_bootstrap_snapshot(&snapshot).unwrap();
        compressed.chunk_data[0][0] ^= 0xFF;

        let err = restore_state_from_compressed(&compressed).unwrap_err();
        assert_eq!(err, BootstrapError::InvalidCompressedProof);
    }

    #[test]
    fn restore_rejects_missing_chunk() {
        let state = test_state();
        let snapshot = build_bootstrap_snapshot(11, &state, 64).unwrap();
        let mut compressed = compress_bootstrap_snapshot(&snapshot).unwrap();
        compressed.chunk_data.pop();

        let err = restore_state_from_compressed(&compressed).unwrap_err();
        assert!(matches!(err, BootstrapError::WrongChunkCount { .. }));
    }

    #[test]
    fn restore_rejects_reordered_chunks() {
        let state = test_state();
        let snapshot = build_bootstrap_snapshot(11, &state, 64).unwrap();
        let mut compressed = compress_bootstrap_snapshot(&snapshot).unwrap();
        compressed.chunk_data.swap(0, 1);

        let err = restore_state_from_compressed(&compressed).unwrap_err();
        assert_eq!(err, BootstrapError::InvalidCompressedProof);
    }

    #[test]
    fn subset_prove_and_verify() {
        let state = test_state();
        let snapshot = build_bootstrap_snapshot(11, &state, 64).unwrap();
        let compressed = compress_bootstrap_snapshot(&snapshot).unwrap();
        let n = compressed.manifest.chunk_count;
        assert!(n >= 3);

        let indices = [0, n - 1];
        let proof = prove_chunk_subset(&compressed.chunk_data, &indices);
        let chunks: Vec<(usize, &Vec<u8>)> = indices
            .iter()
            .map(|i| (*i, &compressed.chunk_data[*i]))
            .collect();
        verify_chunk_subset(&compressed.manifest, &chunks, &proof).unwrap();

        // tampered chunk in the batch is rejected
        let tampered = vec![0xFFu8; compressed.chunk_data[0].len()];
        let bad: Vec<(usize, &Vec<u8>)> =
            vec![(0, &tampered), (n - 1, &compressed.chunk_data[n - 1])];
        let err = verify_chunk_subset(&compressed.manifest, &bad, &proof).unwrap_err();
        assert_eq!(err, BootstrapError::InvalidCompressedProof);
    }

    #[test]
    fn groth16_stub_is_unsupported() {
        let state = test_state();
        let snapshot = build_bootstrap_snapshot(11, &state, 64).unwrap();
        let mut compressed = compress_bootstrap_snapshot(&snapshot).unwrap();
        compressed.proof = groth16_proof_stub();

        match &compressed.proof {
            CompressedSnapshotProof::Groth16 { proof } => {
                assert_eq!(proof.len(), SNAPSHOT_ZK_PROOF_SIZE);
            }
            CompressedSnapshotProof::MerkleMultiproof { .. } => panic!("wrong scheme"),
        }

        let err = restore_state_from_compressed(&compressed).unwrap_err();
        assert_eq!(err, BootstrapError::UnsupportedProofScheme);

        let chunks: Vec<(usize, &Vec<u8>)> = vec![(0, &compressed.chunk_data[0])];
        let err = verify_chunk_subset(&compressed.manifest, &chunks, &compressed.proof).unwrap_err();
        assert_eq!(err, BootstrapError::UnsupportedProofScheme);
    }
}
