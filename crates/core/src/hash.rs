use ark_bn254::Fr as F;
use light_poseidon::{Poseidon, PoseidonHasher, PoseidonBytesHasher};

/// Computes Poseidon hash of `inputs` (field elements) and returns a field element
pub fn hash(inputs: &[F]) -> F {
    let mut poseidon = Poseidon::<F>::new_circom(inputs.len())
        .expect("invalid arity for Poseidon");
    poseidon.hash(inputs).expect("hash failure")
}

/// Computes Poseidon hash over raw 32-byte big-endian blobs and returns a 32-byte BE digest
pub fn hash_bytes_be(inputs: &[&[u8; 32]]) -> [u8; 32] {
    let slices: Vec<&[u8]> = inputs.iter().map(|arr| &arr[..]).collect();
    let mut poseidon = Poseidon::<F>::new_circom(slices.len())
        .expect("invalid arity for Poseidon");
    poseidon
        .hash_bytes_be(&slices)
        .expect("hash failure")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_poseidon_known_vector() {
        // taken from light-poseidon's readme
        let expected: [u8; 32] = [
            13, 84, 225, 147, 143, 138, 140, 28, 125, 235, 94, 3, 85, 242, 99, 25, 32, 123, 132,
            254, 156, 162, 206, 27, 38, 231, 53, 200, 41, 130, 25, 144,
        ];
        let inputs: [&[u8; 32]; 2] = [&[1u8; 32], &[2u8; 32]];
        let digest = hash_bytes_be(&inputs);
        assert_eq!(digest, expected);
    }
} 