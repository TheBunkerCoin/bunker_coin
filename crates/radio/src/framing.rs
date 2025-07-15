use alpenglow::shredder::Shred;
use alpenglow::Slot;
use reed_solomon_erasure::galois_8::ReedSolomon;
use serde::{Deserialize, Serialize};
use crate::RadioError;

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
        let encoder = ReedSolomon::new(32, 64)
            .map_err(|_| RadioError::InvalidFrame)?;
        
        Ok(Self { mtu, encoder })
    }
    
    pub fn frame_shred(&self, shred: &Shred) -> Result<Vec<RadioFrame>, RadioError> {
        let serialized = bincode::serde::encode_to_vec(shred, bincode::config::standard())
            .map_err(|_| RadioError::InvalidFrame)?;
        
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
        
        let (shred, _) = bincode::serde::decode_from_slice(&decoded, bincode::config::standard())
            .map_err(|_| RadioError::InvalidFrame)?;
        Ok(shred)
    }
    
    fn encode_data(&self, data: &[u8]) -> Result<Vec<Vec<u8>>, RadioError> {
        let shard_size = (data.len() + 31) / 32;
        let mut shards = Vec::with_capacity(32);
        
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
        
        self.encoder.encode(&mut shards)
            .map_err(|_| RadioError::ErasureDecodingFailed)?;
        
        Ok(shards)
    }
    
    fn decode_data(&self, mut shards: Vec<Option<Vec<u8>>>) -> Result<Vec<u8>, RadioError> {
        shards.resize(96, None);
        
        self.encoder.reconstruct(&mut shards)
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