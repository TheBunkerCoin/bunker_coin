//! Simulated radio network for testing

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use alpenglow::network::{Network, NetworkError, NetworkMessage};
use rand::Rng;
use log::{debug, trace};

use crate::{RadioConfig, RadioError};

/// Simulated radio network with realistic HF propagation characteristics
pub struct SimulatedRadioNetwork {
    config: RadioConfig,
    stats: Arc<Mutex<NetworkStats>>,
}

#[derive(Debug, Default)]
struct NetworkStats {
    packets_sent: u64,
    packets_dropped: u64,
    bytes_sent: u64,
}

impl SimulatedRadioNetwork {
    pub fn new(config: RadioConfig) -> Self {
        Self {
            config,
            stats: Arc::new(Mutex::new(NetworkStats::default())),
        }
    }
    
    /// Simulate radio channel effects
    async fn simulate_channel(&self, data: &[u8]) -> Result<(), RadioError> {
        // Check MTU
        if data.len() > self.config.mtu {
            return Err(RadioError::PacketTooLarge);
        }
        
        // Simulate transmission time based on bandwidth
        let transmission_time = Duration::from_secs_f64(
            (data.len() * 8) as f64 / self.config.bandwidth_bps as f64
        );
        sleep(transmission_time).await;
        
        // Simulate propagation delay
        sleep(self.config.latency).await;
        
        // Simulate packet loss - generate random value before async operation
        let drop_packet = {
            let mut rng = rand::rng();
            rng.random::<f32>() < self.config.packet_loss
        };
        
        if drop_packet {
            let mut stats = self.stats.lock().await;
            stats.packets_dropped += 1;
            debug!("Simulated packet loss");
            return Err(RadioError::TransmissionFailed);
        }
        
        // Update stats
        let mut stats = self.stats.lock().await;
        stats.packets_sent += 1;
        stats.bytes_sent += data.len() as u64;
        
        Ok(())
    }
    
    pub async fn get_stats(&self) -> (u64, u64, u64) {
        let stats = self.stats.lock().await;
        (stats.packets_sent, stats.packets_dropped, stats.bytes_sent)
    }
}

impl Network for SimulatedRadioNetwork {
    type Address = String;

    async fn send(&self, msg: &NetworkMessage, _to: impl AsRef<str> + Send) -> Result<(), NetworkError> {
        // Serialize the message
        let data = msg.to_bytes();
        
        trace!("Simulating radio transmission of {} bytes", data.len());
        
        // Simulate radio channel
        self.simulate_channel(&data).await
            .map_err(|_| NetworkError::Unknown)?;
        
        Ok(())
    }
    
    async fn send_serialized(&self, bytes: &[u8], _to: impl AsRef<str> + Send) -> Result<(), NetworkError> {
        trace!("Simulating radio transmission of {} bytes", bytes.len());
        
        // Simulate radio channel
        self.simulate_channel(bytes).await
            .map_err(|_| NetworkError::Unknown)?;
        
        Ok(())
    }
    
    async fn receive(&self) -> Result<NetworkMessage, NetworkError> {
        // In a real implementation, this would receive from radio
        // For simulation, we'll just wait indefinitely
        loop {
            sleep(Duration::from_secs(3600)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_mtu_enforcement() {
        let config = RadioConfig {
            mtu: 100,
            ..Default::default()
        };
        let radio = SimulatedRadioNetwork::new(config);
        
        // This should fail - too large
        let large_data = vec![0u8; 200];
        let result = radio.simulate_channel(&large_data).await;
        assert!(matches!(result, Err(RadioError::PacketTooLarge)));
        
        // This should succeed
        let small_data = vec![0u8; 50];
        let result = radio.simulate_channel(&small_data).await;
        assert!(result.is_ok());
    }
} 