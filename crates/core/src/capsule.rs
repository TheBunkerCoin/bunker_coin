//! Capsule: compact binary transaction envelope for LoRa last-mile access.
//!
//! Wire formats (all little-endian where applicable):
//!
//! **Single (kind=0x00):** `[kind:1][msg_id:4][sender:32][nonce:8][fee:8][body...][sig:64]`
//! **Fragment-first (kind=0x01):** `[kind:1][msg_id:4][frag_idx:1][total_frags:1][sender:32][nonce:8][fee:8][body_chunk...][sig:64]`
//! **Fragment-continuation (kind=0x02):** `[kind:1][msg_id:4][frag_idx:1][total_frags:1][body_chunk...]`

use crate::types::{PublicKey, Signature};
use sha2::{Digest, Sha256};

pub const CAPSULE_MTU: usize = 300;
pub const CAPSULE_SINGLE_KIND: u8 = 0x00;
pub const CAPSULE_FRAG_FIRST_KIND: u8 = 0x01;
pub const CAPSULE_FRAG_CONT_KIND: u8 = 0x02;

/// Single: kind(1) + msg_id(4) + sender(32) + nonce(8) + fee(8) + sig(64) = 117
pub const CAPSULE_SINGLE_OVERHEAD: usize = 1 + 4 + 32 + 8 + 8 + 64;
/// Fragment-first: kind(1) + msg_id(4) + frag_idx(1) + total_frags(1) + sender(32) + nonce(8) + fee(8) + sig(64) = 119
pub const CAPSULE_FRAG_FIRST_OVERHEAD: usize = 1 + 4 + 1 + 1 + 32 + 8 + 8 + 64;
/// Fragment-continuation: kind(1) + msg_id(4) + frag_idx(1) + total_frags(1) = 7
pub const CAPSULE_FRAG_CONT_OVERHEAD: usize = 1 + 4 + 1 + 1;

/// Maximum body bytes in a single capsule.
pub const CAPSULE_SINGLE_BODY_BUDGET: usize = CAPSULE_MTU - CAPSULE_SINGLE_OVERHEAD; // 183
/// Maximum body-chunk bytes in a fragment-first capsule.
pub const CAPSULE_FRAG_FIRST_BODY_BUDGET: usize = CAPSULE_MTU - CAPSULE_FRAG_FIRST_OVERHEAD; // 181
/// Maximum body-chunk bytes in a fragment-continuation capsule.
pub const CAPSULE_FRAG_CONT_BODY_BUDGET: usize = CAPSULE_MTU - CAPSULE_FRAG_CONT_OVERHEAD; // 293

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapsuleHeader {
    pub sender: PublicKey,
    pub nonce: u64,
    pub fee: u64,
    pub msg_id: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CapsuleError {
    TooShort,
    UnknownKind(u8),
    MtuExceeded(usize),
    InvalidSignature,
    InvalidSenderKey,
    MalformedBody,
}

impl core::fmt::Display for CapsuleError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::TooShort => write!(f, "capsule too short"),
            Self::UnknownKind(k) => write!(f, "unknown capsule kind: 0x{k:02x}"),
            Self::MtuExceeded(sz) => write!(f, "capsule exceeds MTU: {sz} > {CAPSULE_MTU}"),
            Self::InvalidSignature => write!(f, "invalid capsule signature"),
            Self::InvalidSenderKey => write!(f, "invalid sender public key"),
            Self::MalformedBody => write!(f, "malformed capsule body"),
        }
    }
}

impl std::error::Error for CapsuleError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodedCapsule {
    Single {
        header: CapsuleHeader,
        body: Vec<u8>,
        sig: Signature,
    },
    FragFirst {
        header: CapsuleHeader,
        frag_idx: u8,
        total_frags: u8,
        body_chunk: Vec<u8>,
        sig: Signature,
    },
    FragCont {
        msg_id: u32,
        frag_idx: u8,
        total_frags: u8,
        body_chunk: Vec<u8>,
    },
}

/// Encode a single (non-fragmented) capsule.
pub fn encode_single(
    header: &CapsuleHeader,
    body: &[u8],
    sig: &Signature,
) -> Result<Vec<u8>, CapsuleError> {
    let total = CAPSULE_SINGLE_OVERHEAD + body.len();
    if total > CAPSULE_MTU {
        return Err(CapsuleError::MtuExceeded(total));
    }
    let mut buf = Vec::with_capacity(total);
    buf.push(CAPSULE_SINGLE_KIND);
    buf.extend_from_slice(&header.msg_id.to_le_bytes());
    buf.extend_from_slice(&header.sender);
    buf.extend_from_slice(&header.nonce.to_le_bytes());
    buf.extend_from_slice(&header.fee.to_le_bytes());
    buf.extend_from_slice(body);
    buf.extend_from_slice(sig);
    Ok(buf)
}

/// Encode a body that may exceed the single-capsule budget into fragments.
///
/// Returns a vec of frame bytes. The first frame is a FragFirst, the rest are FragCont.
pub fn encode_fragmented(
    header: &CapsuleHeader,
    body: &[u8],
    sig: &Signature,
) -> Result<Vec<Vec<u8>>, CapsuleError> {
    // If it fits in a single capsule, just use that
    if body.len() <= CAPSULE_SINGLE_BODY_BUDGET {
        return Ok(vec![encode_single(header, body, sig)?]);
    }

    // Calculate how many continuation frames we need after the first
    let first_chunk_len = CAPSULE_FRAG_FIRST_BODY_BUDGET.min(body.len());
    let remaining = body.len() - first_chunk_len;
    let cont_count = if remaining == 0 {
        0
    } else {
        (remaining + CAPSULE_FRAG_CONT_BODY_BUDGET - 1) / CAPSULE_FRAG_CONT_BODY_BUDGET
    };
    let total_frags = (1 + cont_count) as u8;

    let mut frames = Vec::with_capacity(total_frags as usize);

    // First fragment
    {
        let chunk = &body[..first_chunk_len];
        let total = CAPSULE_FRAG_FIRST_OVERHEAD + chunk.len();
        if total > CAPSULE_MTU {
            return Err(CapsuleError::MtuExceeded(total));
        }
        let mut buf = Vec::with_capacity(total);
        buf.push(CAPSULE_FRAG_FIRST_KIND);
        buf.extend_from_slice(&header.msg_id.to_le_bytes());
        buf.push(0); // frag_idx
        buf.push(total_frags);
        buf.extend_from_slice(&header.sender);
        buf.extend_from_slice(&header.nonce.to_le_bytes());
        buf.extend_from_slice(&header.fee.to_le_bytes());
        buf.extend_from_slice(chunk);
        buf.extend_from_slice(sig);
        frames.push(buf);
    }

    // Continuation fragments
    let mut offset = first_chunk_len;
    for i in 1..total_frags {
        let end = (offset + CAPSULE_FRAG_CONT_BODY_BUDGET).min(body.len());
        let chunk = &body[offset..end];
        let total = CAPSULE_FRAG_CONT_OVERHEAD + chunk.len();
        if total > CAPSULE_MTU {
            return Err(CapsuleError::MtuExceeded(total));
        }
        let mut buf = Vec::with_capacity(total);
        buf.push(CAPSULE_FRAG_CONT_KIND);
        buf.extend_from_slice(&header.msg_id.to_le_bytes());
        buf.push(i);
        buf.push(total_frags);
        buf.extend_from_slice(chunk);
        frames.push(buf);
        offset = end;
    }

    Ok(frames)
}

/// Decode a raw capsule frame.
pub fn decode(bytes: &[u8]) -> Result<DecodedCapsule, CapsuleError> {
    if bytes.is_empty() {
        return Err(CapsuleError::TooShort);
    }
    if bytes.len() > CAPSULE_MTU {
        return Err(CapsuleError::MtuExceeded(bytes.len()));
    }

    let kind = bytes[0];
    match kind {
        CAPSULE_SINGLE_KIND => {
            if bytes.len() < CAPSULE_SINGLE_OVERHEAD {
                return Err(CapsuleError::TooShort);
            }
            let msg_id = u32::from_le_bytes(bytes[1..5].try_into().unwrap());
            let sender: PublicKey = bytes[5..37].try_into().unwrap();
            let nonce = u64::from_le_bytes(bytes[37..45].try_into().unwrap());
            let fee = u64::from_le_bytes(bytes[45..53].try_into().unwrap());
            let sig_start = bytes.len() - 64;
            let body = bytes[53..sig_start].to_vec();
            let sig: Signature = bytes[sig_start..].try_into().unwrap();
            Ok(DecodedCapsule::Single {
                header: CapsuleHeader {
                    sender,
                    nonce,
                    fee,
                    msg_id,
                },
                body,
                sig,
            })
        }
        CAPSULE_FRAG_FIRST_KIND => {
            if bytes.len() < CAPSULE_FRAG_FIRST_OVERHEAD {
                return Err(CapsuleError::TooShort);
            }
            let msg_id = u32::from_le_bytes(bytes[1..5].try_into().unwrap());
            let frag_idx = bytes[5];
            let total_frags = bytes[6];
            let sender: PublicKey = bytes[7..39].try_into().unwrap();
            let nonce = u64::from_le_bytes(bytes[39..47].try_into().unwrap());
            let fee = u64::from_le_bytes(bytes[47..55].try_into().unwrap());
            let sig_start = bytes.len() - 64;
            let body_chunk = bytes[55..sig_start].to_vec();
            let sig: Signature = bytes[sig_start..].try_into().unwrap();
            Ok(DecodedCapsule::FragFirst {
                header: CapsuleHeader {
                    sender,
                    nonce,
                    fee,
                    msg_id,
                },
                frag_idx,
                total_frags,
                body_chunk,
                sig,
            })
        }
        CAPSULE_FRAG_CONT_KIND => {
            if bytes.len() < CAPSULE_FRAG_CONT_OVERHEAD {
                return Err(CapsuleError::TooShort);
            }
            let msg_id = u32::from_le_bytes(bytes[1..5].try_into().unwrap());
            let frag_idx = bytes[5];
            let total_frags = bytes[6];
            let body_chunk = bytes[7..].to_vec();
            Ok(DecodedCapsule::FragCont {
                msg_id,
                frag_idx,
                total_frags,
                body_chunk,
            })
        }
        _ => Err(CapsuleError::UnknownKind(kind)),
    }
}

/// Compute the signing hash for a capsule, identical to `Transaction::signing_hash()`.
///
/// `hash = SHA256(sender || nonce_le || fee_le || body_bytes)`
/// where `body_bytes` is the bincode-serialized `TransactionBody`.
pub fn capsule_signing_hash(sender: &PublicKey, nonce: u64, fee: u64, body: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(sender);
    hasher.update(nonce.to_le_bytes());
    hasher.update(fee.to_le_bytes());
    hasher.update(body);
    hasher.finalize().into()
}

/// Verify the ed25519 signature on a capsule.
///
/// The message signed is `SHA256(sender || nonce_le || fee_le || body_bytes)`.
pub fn verify_capsule_signature(
    sender: &PublicKey,
    nonce: u64,
    fee: u64,
    body: &[u8],
    sig: &Signature,
) -> Result<(), CapsuleError> {
    use ed25519_dalek::{Signature as DalekSig, VerifyingKey};

    let vk = VerifyingKey::from_bytes(sender).map_err(|_| CapsuleError::InvalidSenderKey)?;
    let hash = capsule_signing_hash(sender, nonce, fee, body);
    let dalek_sig = DalekSig::from_bytes(sig);
    vk.verify_strict(&hash, &dalek_sig)
        .map_err(|_| CapsuleError::InvalidSignature)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_header(msg_id: u32) -> CapsuleHeader {
        CapsuleHeader {
            sender: [0xAA; 32],
            nonce: 42,
            fee: 100,
            msg_id,
        }
    }

    #[test]
    fn encode_decode_single_roundtrip() {
        let header = test_header(1);
        let body = vec![1, 2, 3, 4, 5];
        let sig = [0xBB; 64];

        let encoded = encode_single(&header, &body, &sig).unwrap();
        assert!(encoded.len() <= CAPSULE_MTU);

        let decoded = decode(&encoded).unwrap();
        match decoded {
            DecodedCapsule::Single {
                header: h,
                body: b,
                sig: s,
            } => {
                assert_eq!(h, header);
                assert_eq!(b, body);
                assert_eq!(s, sig);
            }
            _ => panic!("expected Single"),
        }
    }

    #[test]
    fn encode_decode_fragmented_roundtrip() {
        let header = test_header(2);
        // 400 bytes — requires fragmentation
        let body = vec![0x42; 400];
        let sig = [0xCC; 64];

        let frames = encode_fragmented(&header, &body, &sig).unwrap();
        assert!(frames.len() >= 2, "expected at least 2 fragments");

        for frame in &frames {
            assert!(frame.len() <= CAPSULE_MTU);
        }

        // Decode first fragment
        let first = decode(&frames[0]).unwrap();
        let mut reassembled = Vec::new();
        match first {
            DecodedCapsule::FragFirst {
                header: h,
                frag_idx,
                total_frags,
                body_chunk,
                sig: s,
            } => {
                assert_eq!(h, header);
                assert_eq!(frag_idx, 0);
                assert_eq!(total_frags, frames.len() as u8);
                assert_eq!(s, sig);
                reassembled.extend_from_slice(&body_chunk);
            }
            _ => panic!("expected FragFirst"),
        }

        // Decode continuation fragments
        for frame in &frames[1..] {
            let decoded = decode(frame).unwrap();
            match decoded {
                DecodedCapsule::FragCont {
                    msg_id, body_chunk, ..
                } => {
                    assert_eq!(msg_id, 2);
                    reassembled.extend_from_slice(&body_chunk);
                }
                _ => panic!("expected FragCont"),
            }
        }

        assert_eq!(reassembled, body);
    }

    #[test]
    fn decode_rejects_truncated() {
        // Too short for any kind
        assert_eq!(decode(&[]).unwrap_err(), CapsuleError::TooShort);
        // Single kind but truncated
        assert_eq!(
            decode(&[CAPSULE_SINGLE_KIND, 0, 0]).unwrap_err(),
            CapsuleError::TooShort
        );
    }

    #[test]
    fn decode_rejects_unknown_kind() {
        let mut buf = vec![0xFF; CAPSULE_MTU];
        buf[0] = 0x99;
        assert_eq!(decode(&buf).unwrap_err(), CapsuleError::UnknownKind(0x99));
    }

    #[test]
    fn signature_valid_single() {
        use ed25519_dalek::{Signer, SigningKey};

        let mut rng = rand::thread_rng();
        let signing_key = SigningKey::generate(&mut rng);
        let sender: PublicKey = signing_key.verifying_key().to_bytes();

        let nonce: u64 = 1;
        let fee: u64 = 50;
        let body = vec![0u8; 10];

        let hash = capsule_signing_hash(&sender, nonce, fee, &body);
        let sig: Signature = signing_key.sign(&hash).to_bytes();

        assert!(verify_capsule_signature(&sender, nonce, fee, &body, &sig).is_ok());
    }

    #[test]
    fn signature_invalid_single() {
        use ed25519_dalek::{Signer, SigningKey};

        let mut rng = rand::thread_rng();
        let signing_key = SigningKey::generate(&mut rng);
        let sender: PublicKey = signing_key.verifying_key().to_bytes();

        let nonce: u64 = 1;
        let fee: u64 = 50;
        let body = vec![0u8; 10];

        let hash = capsule_signing_hash(&sender, nonce, fee, &body);
        let mut sig: Signature = signing_key.sign(&hash).to_bytes();
        sig[0] ^= 0xFF; // flip a byte

        assert_eq!(
            verify_capsule_signature(&sender, nonce, fee, &body, &sig).unwrap_err(),
            CapsuleError::InvalidSignature
        );
    }

    #[test]
    fn single_budget_fits_simple_bodies() {
        // Verify the body budget constant is correct
        assert_eq!(CAPSULE_SINGLE_BODY_BUDGET, 183);
        assert_eq!(CAPSULE_FRAG_FIRST_BODY_BUDGET, 181);
        assert_eq!(CAPSULE_FRAG_CONT_BODY_BUDGET, 293);

        // A Transfer body: tag(1) + to(32) + amount(8) = 41 bytes (well within 183)
        // A Bond body: tag(1) + validator(32) + amount(8) = 41 bytes
        // A Mint body: tag(1) + ticker_len(4) + ticker(~8) + max_supply(8) + hash(32) = ~53 bytes
        // All simple TransactionBody variants fit within 183 bytes.
        // The largest simple body is UpdateMetadata: tag(1) + token_id(4) + hash(32) = 37 bytes.
        // So 183 bytes is more than enough for all simple tx types.
        let body_183 = vec![0u8; CAPSULE_SINGLE_BODY_BUDGET];
        let header = test_header(99);
        let sig = [0; 64];
        let encoded = encode_single(&header, &body_183, &sig).unwrap();
        assert_eq!(encoded.len(), CAPSULE_MTU);

        // One more byte should fail
        let body_184 = vec![0u8; CAPSULE_SINGLE_BODY_BUDGET + 1];
        assert!(encode_single(&header, &body_184, &sig).is_err());
    }
}
