//! Simulated LoRa transport for testing — uses tokio mpsc channels.

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::transport::{LoRaFrame, LoRaTransport, LoRaTransportError};

/// Configuration for simulated transport.
#[derive(Clone, Debug)]
pub struct SimulatedConfig {
    /// Channel buffer size.
    pub buffer_size: usize,
}

impl Default for SimulatedConfig {
    fn default() -> Self {
        Self { buffer_size: 64 }
    }
}

/// A simulated LoRa transport backed by tokio mpsc channels.
pub struct SimulatedLoRaTransport {
    tx: mpsc::Sender<Vec<u8>>,
    rx: tokio::sync::Mutex<mpsc::Receiver<Vec<u8>>>,
}

impl SimulatedLoRaTransport {
    /// Create a linked pair of simulated transports.
    /// Data sent on `a` is received on `b` and vice versa.
    pub fn pair(config: SimulatedConfig) -> (Self, Self) {
        let (tx_a, rx_a) = mpsc::channel(config.buffer_size);
        let (tx_b, rx_b) = mpsc::channel(config.buffer_size);

        let a = Self {
            tx: tx_b,
            rx: tokio::sync::Mutex::new(rx_a),
        };
        let b = Self {
            tx: tx_a,
            rx: tokio::sync::Mutex::new(rx_b),
        };
        (a, b)
    }
}

#[async_trait]
impl LoRaTransport for SimulatedLoRaTransport {
    async fn transmit(&self, frame: &[u8]) -> Result<(), LoRaTransportError> {
        if frame.len() > bunker_coin_core::capsule::CAPSULE_MTU {
            return Err(LoRaTransportError::ExceedsMtu(frame.len()));
        }
        self.tx
            .send(frame.to_vec())
            .await
            .map_err(|e| LoRaTransportError::Io(e.to_string()))
    }

    async fn receive(&self) -> Result<LoRaFrame, LoRaTransportError> {
        let mut rx = self.rx.lock().await;
        match rx.recv().await {
            Some(payload) => Ok(LoRaFrame {
                payload,
                rssi: Some(-50),
                snr: Some(10.0),
            }),
            None => Err(LoRaTransportError::NotConnected),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn simulated_pair_roundtrip() {
        let (a, b) = SimulatedLoRaTransport::pair(SimulatedConfig::default());
        let data = vec![1, 2, 3, 4, 5];
        a.transmit(&data).await.unwrap();
        let frame = b.receive().await.unwrap();
        assert_eq!(frame.payload, data);
    }
}
