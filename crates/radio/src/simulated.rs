//! Simulated radio network for testing

use crate::{Network, NetworkError, NetworkMessage};
use log::{debug, trace};
use rand::Rng;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;

use crate::{RadioConfig, RadioError};

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

    async fn simulate_channel(&self, data: &[u8]) -> Result<(), RadioError> {
        if data.len() > self.config.mtu {
            return Err(RadioError::PacketTooLarge);
        }

        let transmission_time =
            Duration::from_secs_f64((data.len() * 8) as f64 / self.config.bandwidth_bps as f64);
        sleep(transmission_time).await;

        sleep(self.config.latency).await;

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

    async fn send(
        &self,
        msg: &NetworkMessage,
        _to: impl AsRef<str> + Send,
    ) -> Result<(), NetworkError> {
        let data = msg.to_bytes();

        trace!("Simulating radio transmission of {} bytes", data.len());

        self.simulate_channel(&data)
            .await
            .map_err(|_| NetworkError::Unknown)?;

        Ok(())
    }

    async fn send_serialized(
        &self,
        bytes: &[u8],
        _to: impl AsRef<str> + Send,
    ) -> Result<(), NetworkError> {
        trace!("Simulating radio transmission of {} bytes", bytes.len());

        self.simulate_channel(bytes)
            .await
            .map_err(|_| NetworkError::Unknown)?;

        Ok(())
    }

    async fn receive(&self) -> Result<NetworkMessage, NetworkError> {
        loop {
            sleep(Duration::from_secs(3600)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mtu_enforcement() {
        let config = RadioConfig {
            mtu: 100,
            packet_loss: 0.0,
            latency: Duration::from_millis(0),
            ..Default::default()
        };
        let radio = SimulatedRadioNetwork::new(config);

        let large_data = vec![0u8; 200];
        let result = radio.simulate_channel(&large_data).await;
        assert!(matches!(result, Err(RadioError::PacketTooLarge)));

        let small_data = vec![0u8; 50];
        let result = radio.simulate_channel(&small_data).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn packet_loss_simulation() {
        let config = RadioConfig {
            mtu: 300,
            packet_loss: 1.0, // 100% loss
            latency: Duration::from_millis(0),
            ..Default::default()
        };
        let radio = SimulatedRadioNetwork::new(config);

        let data = vec![0u8; 10];
        let result = radio.simulate_channel(&data).await;
        assert!(matches!(result, Err(RadioError::TransmissionFailed)));
    }

    #[tokio::test]
    async fn stats_tracking() {
        let config = RadioConfig {
            mtu: 300,
            packet_loss: 0.0,
            latency: Duration::from_millis(0),
            ..Default::default()
        };
        let radio = SimulatedRadioNetwork::new(config);

        radio.simulate_channel(&[0u8; 10]).await.unwrap();
        radio.simulate_channel(&[0u8; 20]).await.unwrap();

        let (sent, dropped, bytes) = radio.get_stats().await;
        assert_eq!(sent, 2);
        assert_eq!(dropped, 0);
        assert_eq!(bytes, 30);
    }
}
