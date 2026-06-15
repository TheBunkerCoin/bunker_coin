//! LoRa Last-Mile Access gateway for BunkerCoin.
//!
//! Receives compact `Capsule` frames over LoRa radio, validates them,
//! reassembles fragments, and relays as full `Transaction` objects
//! into the validator mempool.

pub mod dedup;
pub mod gateway;
pub mod lorawan;
pub mod rate_limit;
pub mod reassembler;
pub mod simulated;
pub mod transport;

/// Configuration for the LoRa gateway.
#[derive(Clone, Debug)]
pub struct LoRaGatewayConfig {
    /// Maximum dedup cache entries.
    pub dedup_cache_size: usize,
    /// Fragment reassembly timeout in seconds.
    pub reassembly_timeout_secs: u64,
    /// Capsules per minute per sender (sustained rate).
    pub rate_capsules_per_min: f64,
    /// Maximum burst per sender.
    pub rate_burst: f64,
    /// Maximum tracked senders for rate limiting.
    pub rate_max_senders: usize,
}

impl Default for LoRaGatewayConfig {
    fn default() -> Self {
        Self {
            dedup_cache_size: 4096,
            reassembly_timeout_secs: 60,
            rate_capsules_per_min: 2.0,
            rate_burst: 4.0,
            rate_max_senders: 256,
        }
    }
}
