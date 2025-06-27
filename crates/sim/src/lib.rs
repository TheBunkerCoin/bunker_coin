//! simulation tools for BunkerCoin

pub mod scenarios;

use std::time::Duration;
use bunker_coin_radio::RadioConfig;

pub struct SimulationPresets;

impl SimulationPresets {
    pub fn good_conditions() -> RadioConfig {
        RadioConfig {
            mtu: 300,
            bandwidth_bps: 2400,
            packet_loss: 0.05,
            latency: Duration::from_millis(100),
            latency_jitter: Duration::from_millis(20),  // ±20ms jitter (good conditions)
            transmission_window: Duration::from_secs(300),
        }
    }
    
    pub fn average_conditions() -> RadioConfig {
        RadioConfig {
            mtu: 300,
            bandwidth_bps: 1200,
            packet_loss: 0.15, 
            latency: Duration::from_millis(200),
            latency_jitter: Duration::from_millis(50),  // ±50ms jitter (average conditions)
            transmission_window: Duration::from_secs(300),
        }
    }
    
    pub fn poor_conditions() -> RadioConfig {
        RadioConfig {
            mtu: 300,
            bandwidth_bps: 300,
            packet_loss: 0.30, 
            latency: Duration::from_millis(500),
            latency_jitter: Duration::from_millis(150), // ±150ms jitter (poor conditions)
            transmission_window: Duration::from_secs(300),
        }
    }
    
    pub fn extreme_conditions() -> RadioConfig {
        RadioConfig {
            mtu: 300,
            bandwidth_bps: 50,
            packet_loss: 0.50,
            latency: Duration::from_millis(1000),
            latency_jitter: Duration::from_millis(300), // ±300ms jitter (extreme conditions)
            transmission_window: Duration::from_secs(300),
        }
    }
} 