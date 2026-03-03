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
