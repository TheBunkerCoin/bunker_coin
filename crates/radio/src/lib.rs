//! radio networking layer for BunkerCoin

use std::time::Duration;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod framing;
pub mod scheduler;
pub mod simulated;
pub mod network_core;

pub use framing::{RadioFrame, RadioFramer};
pub use scheduler::RadioScheduler;
pub use simulated::SimulatedRadioNetwork;
pub use network_core::RadioNetworkCore;

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