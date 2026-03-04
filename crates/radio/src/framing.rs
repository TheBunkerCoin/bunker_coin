use crate::RadioError;
use bunkerglow::shredder::Shred;
use bunkerglow::Slot;
use reed_solomon_erasure::galois_8::ReedSolomon;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RadioFrame {
    pub slot: Slot,
    pub shred_index: usize,
    pub frame_index: usize,
    pub total_frames: usize,
    pub data: Vec<u8>,
}

pub struct RadioFramer {
    mtu: usize,
    encoder: ReedSolomon,
}

impl RadioFramer {
    pub fn new(mtu: usize) -> Result<Self, RadioError> {
        let encoder = ReedSolomon::new(32, 64).map_err(|_| RadioError::InvalidFrame)?;

        Ok(Self { mtu, encoder })
    }

    pub fn frame_shred(&self, shred: &Shred) -> Result<Vec<RadioFrame>, RadioError> {
        let serialized = wincode::serialize(shred).map_err(|_| RadioError::InvalidFrame)?;

        let encoded = self.encode_data(&serialized)?;

        let frame_header_size = 32;
        let payload_size = self.mtu - frame_header_size;

        let mut frames = Vec::new();
        let mut frame_index = 0;

        for shard in encoded.iter() {
            for chunk in shard.chunks(payload_size) {
                frames.push(RadioFrame {
                    slot: shred.payload().slot(),
                    shred_index: shred.payload().index_in_slot(),
                    frame_index,
                    total_frames: 0,
                    data: chunk.to_vec(),
                });
                frame_index += 1;
            }
        }

        let total_frames = frames.len();
        for frame in &mut frames {
            frame.total_frames = total_frames;
        }

        Ok(frames)
    }

    pub fn reconstruct_shred(&self, frames: &[RadioFrame]) -> Result<Shred, RadioError> {
        if frames.is_empty() {
            return Err(RadioError::InvalidFrame);
        }

        let mut shards: Vec<Vec<u8>> = vec![vec![]; 96];
        let mut shard_present = vec![false; 96];

        for frame in frames {
            if frame.frame_index < shards.len() {
                shards[frame.frame_index] = frame.data.clone();
                shard_present[frame.frame_index] = true;
            }
        }

        let mut option_shards: Vec<Option<Vec<u8>>> = Vec::new();
        for (_i, (shard, present)) in shards.into_iter().zip(shard_present.iter()).enumerate() {
            if *present && !shard.is_empty() {
                option_shards.push(Some(shard));
            } else {
                option_shards.push(None);
            }
        }

        let decoded = self.decode_data(option_shards)?;

        let shred: Shred = wincode::deserialize(&decoded).map_err(|_| RadioError::InvalidFrame)?;
        Ok(shred)
    }

    fn encode_data(&self, data: &[u8]) -> Result<Vec<Vec<u8>>, RadioError> {
        let shard_size = (data.len() + 31) / 32;
        let mut shards = Vec::with_capacity(96);

        for i in 0..32 {
            let start = i * shard_size;
            let end = ((i + 1) * shard_size).min(data.len());
            if start < data.len() {
                let mut shard = vec![0u8; shard_size];
                let len = end - start;
                shard[..len].copy_from_slice(&data[start..end]);
                shards.push(shard);
            } else {
                shards.push(vec![0u8; shard_size]);
            }
        }

        // add empty parity shards for RS to fill
        shards.resize(96, vec![0u8; shard_size]);

        self.encoder
            .encode(&mut shards)
            .map_err(|_| RadioError::ErasureDecodingFailed)?;

        Ok(shards)
    }

    fn decode_data(&self, mut shards: Vec<Option<Vec<u8>>>) -> Result<Vec<u8>, RadioError> {
        shards.resize(96, None);

        self.encoder
            .reconstruct(&mut shards)
            .map_err(|_| RadioError::ErasureDecodingFailed)?;

        let mut result = Vec::new();
        for i in 0..32 {
            if let Some(shard) = &shards[i] {
                result.extend_from_slice(shard);
            }
        }

        while result.last() == Some(&0) {
            result.pop();
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_framer() -> RadioFramer {
        RadioFramer::new(300).unwrap()
    }

    #[test]
    fn framer_creation() {
        let framer = RadioFramer::new(300);
        assert!(framer.is_ok());
    }

    #[test]
    fn encode_decode_roundtrip() {
        let framer = make_framer();
        let data = b"hello erasure coding world!".to_vec();
        let shards = framer.encode_data(&data).unwrap();
        assert_eq!(shards.len(), 96);

        let option_shards: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        let recovered = framer.decode_data(option_shards).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn encode_decode_large_payload() {
        let framer = make_framer();
        let data: Vec<u8> = (0..2048).map(|i| (i % 251) as u8).collect();
        let shards = framer.encode_data(&data).unwrap();

        let option_shards: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        let recovered = framer.decode_data(option_shards).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn decode_with_missing_parity_shards() {
        let framer = make_framer();
        let data = vec![42u8; 256];
        let shards = framer.encode_data(&data).unwrap();

        let mut option_shards: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        // drop all 64 parity shards — still have 32 data shards which is exactly enough
        for i in 32..96 {
            option_shards[i] = None;
        }

        let recovered = framer.decode_data(option_shards).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn decode_with_missing_data_shards() {
        let framer = make_framer();
        let data = vec![7u8; 128];
        let shards = framer.encode_data(&data).unwrap();

        let mut option_shards: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        // drop first 30 data shards — 2 data + 64 parity = 66 shards remain (need 32)
        for i in 0..30 {
            option_shards[i] = None;
        }

        let recovered = framer.decode_data(option_shards).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn decode_with_max_loss() {
        let framer = make_framer();
        let data = vec![0xAB; 512];
        let shards = framer.encode_data(&data).unwrap();

        let mut option_shards: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        // drop 64 shards (the maximum recoverable) — keep exactly 32
        for i in 0..64 {
            option_shards[i] = None;
        }

        let recovered = framer.decode_data(option_shards).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn decode_fails_with_too_few_shards() {
        let framer = make_framer();
        let data = vec![1u8; 64];
        let shards = framer.encode_data(&data).unwrap();

        let mut option_shards: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        // drop 65 shards — only 31 remain, below the threshold of 32
        for i in 0..65 {
            option_shards[i] = None;
        }

        let result = framer.decode_data(option_shards);
        assert!(result.is_err());
    }

    #[test]
    fn encode_single_byte() {
        let framer = make_framer();
        let data = vec![0xFF];
        let shards = framer.encode_data(&data).unwrap();
        assert_eq!(shards.len(), 96);

        let option_shards: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        let recovered = framer.decode_data(option_shards).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn all_shards_same_size() {
        let framer = make_framer();
        let data = vec![0u8; 333];
        let shards = framer.encode_data(&data).unwrap();
        let shard_size = shards[0].len();
        for shard in &shards {
            assert_eq!(shard.len(), shard_size);
        }
    }

    #[test]
    fn reconstruct_empty_frames_errors() {
        let framer = make_framer();
        let result = framer.reconstruct_shred(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn radio_frame_serde_roundtrip() {
        let frame = RadioFrame {
            slot: Slot::new(42),
            shred_index: 3,
            frame_index: 7,
            total_frames: 96,
            data: vec![1, 2, 3, 4, 5],
        };
        let encoded = bincode::serde::encode_to_vec(&frame, bincode::config::standard()).unwrap();
        let (decoded, _): (RadioFrame, _) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();
        assert_eq!(decoded.slot, Slot::new(42));
        assert_eq!(decoded.shred_index, 3);
        assert_eq!(decoded.frame_index, 7);
        assert_eq!(decoded.total_frames, 96);
        assert_eq!(decoded.data, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn decode_scattered_shards() {
        let framer = make_framer();
        let data: Vec<u8> = (0..200).map(|i| (i * 7 % 256) as u8).collect();
        let shards = framer.encode_data(&data).unwrap();

        // keep every third shard — should give us 32 shards (enough)
        let mut option_shards: Vec<Option<Vec<u8>>> = Vec::with_capacity(96);
        for (i, shard) in shards.into_iter().enumerate() {
            if i % 3 == 0 {
                option_shards.push(Some(shard));
            } else {
                option_shards.push(None);
            }
        }

        let recovered = framer.decode_data(option_shards).unwrap();
        assert_eq!(recovered, data);
    }
}
