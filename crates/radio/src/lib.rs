//! radio networking layer for BunkerCoin

use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;

pub mod framing;
pub mod network_core;
pub mod scheduler;
pub mod simulated;

pub use framing::{RadioFrame, RadioFramer};
pub use network_core::RadioNetworkCore;
pub use scheduler::RadioScheduler;
pub use simulated::SimulatedRadioNetwork;

pub type ValidatorId = u64;

#[derive(Debug, Error)]
pub enum RadioError {
    #[error("Packet too large for radio MTU")]
    PacketTooLarge,

    #[error("Radio transmission failed")]
    TransmissionFailed,

    #[error("Invalid frame format")]
    InvalidFrame,

    #[error("Erasure decoding failed")]
    ErasureDecodingFailed,
}

#[derive(Debug, Clone, Error)]
pub enum NetworkError {
    #[error("Unknown network error")]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NetworkMessage {
    Ping,
    Pong,
    Shred(Vec<u8>),
}

impl NetworkMessage {
    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serde::encode_to_vec(self, bincode::config::standard()).unwrap()
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, NetworkError> {
        bincode::serde::decode_from_slice(bytes, bincode::config::standard())
            .map(|(msg, _)| msg)
            .map_err(|_| NetworkError::Unknown)
    }
}

pub trait Network: Send + Sync {
    type Address;
    fn send(
        &self,
        msg: &NetworkMessage,
        to: impl AsRef<str> + Send,
    ) -> impl std::future::Future<Output = Result<(), NetworkError>> + Send;
    fn send_serialized(
        &self,
        bytes: &[u8],
        to: impl AsRef<str> + Send,
    ) -> impl std::future::Future<Output = Result<(), NetworkError>> + Send;
    fn receive(
        &self,
    ) -> impl std::future::Future<Output = Result<NetworkMessage, NetworkError>> + Send;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RadioConfig {
    pub mtu: usize,
    pub bandwidth_bps: u32,
    pub packet_loss: f32,
    pub latency: Duration,
    pub latency_jitter: Duration,
    pub transmission_window: Duration,
}

impl Default for RadioConfig {
    fn default() -> Self {
        Self {
            mtu: 300,
            bandwidth_bps: 1200,
            packet_loss: 0.15,
            latency: Duration::from_millis(200),
            latency_jitter: Duration::from_millis(50),
            transmission_window: Duration::from_secs(300),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn radio_config_defaults() {
        let config = RadioConfig::default();
        assert_eq!(config.mtu, 300);
        assert_eq!(config.bandwidth_bps, 1200);
        assert!((config.packet_loss - 0.15).abs() < f32::EPSILON);
        assert_eq!(config.latency, Duration::from_millis(200));
        assert_eq!(config.latency_jitter, Duration::from_millis(50));
        assert_eq!(config.transmission_window, Duration::from_secs(300));
    }

    #[test]
    fn network_message_ping_roundtrip() {
        let msg = NetworkMessage::Ping;
        let bytes = msg.to_bytes();
        let decoded = NetworkMessage::from_bytes(&bytes).unwrap();
        assert!(matches!(decoded, NetworkMessage::Ping));
    }

    #[test]
    fn network_message_pong_roundtrip() {
        let msg = NetworkMessage::Pong;
        let bytes = msg.to_bytes();
        let decoded = NetworkMessage::from_bytes(&bytes).unwrap();
        assert!(matches!(decoded, NetworkMessage::Pong));
    }

    #[test]
    fn network_message_shred_roundtrip() {
        let payload = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let msg = NetworkMessage::Shred(payload.clone());
        let bytes = msg.to_bytes();
        let decoded = NetworkMessage::from_bytes(&bytes).unwrap();
        match decoded {
            NetworkMessage::Shred(data) => assert_eq!(data, payload),
            _ => panic!("expected Shred variant"),
        }
    }

    #[test]
    fn network_message_variants_distinct() {
        let ping = NetworkMessage::Ping.to_bytes();
        let pong = NetworkMessage::Pong.to_bytes();
        let shred = NetworkMessage::Shred(vec![0]).to_bytes();
        assert_ne!(ping, pong);
        assert_ne!(ping, shred);
        assert_ne!(pong, shred);
    }

    #[test]
    fn network_message_empty_shred() {
        let msg = NetworkMessage::Shred(vec![]);
        let bytes = msg.to_bytes();
        let decoded = NetworkMessage::from_bytes(&bytes).unwrap();
        match decoded {
            NetworkMessage::Shred(data) => assert!(data.is_empty()),
            _ => panic!("expected Shred variant"),
        }
    }

    #[test]
    fn network_message_large_shred() {
        let payload = vec![0xAB; 4096];
        let msg = NetworkMessage::Shred(payload.clone());
        let bytes = msg.to_bytes();
        let decoded = NetworkMessage::from_bytes(&bytes).unwrap();
        match decoded {
            NetworkMessage::Shred(data) => assert_eq!(data, payload),
            _ => panic!("expected Shred variant"),
        }
    }

    #[test]
    fn network_message_invalid_bytes() {
        let result = NetworkMessage::from_bytes(&[0xFF, 0xFF, 0xFF]);
        assert!(result.is_err());
    }

    #[test]
    fn radio_config_serde_roundtrip() {
        let config = RadioConfig::default();
        let bytes = bincode::serde::encode_to_vec(&config, bincode::config::standard()).unwrap();
        let (decoded, _): (RadioConfig, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        assert_eq!(decoded.mtu, config.mtu);
        assert_eq!(decoded.bandwidth_bps, config.bandwidth_bps);
    }
}
