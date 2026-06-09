//! LoRa transport trait — abstraction over physical LoRa hardware.

use async_trait::async_trait;

/// A raw LoRa frame received from the radio.
#[derive(Clone, Debug)]
pub struct LoRaFrame {
    pub payload: Vec<u8>,
    pub rssi: Option<i16>,
    pub snr: Option<f32>,
}

/// Errors from the LoRa transport layer.
#[derive(Debug, thiserror::Error)]
pub enum LoRaTransportError {
    #[error("I/O error: {0}")]
    Io(String),
    #[error("frame exceeds MTU: {0} bytes")]
    ExceedsMtu(usize),
    #[error("transport not connected")]
    NotConnected,
    #[error("operation timed out")]
    Timeout,
}

/// Abstraction over a LoRa radio transport (hardware, simulated, or LoRaWAN).
#[async_trait]
pub trait LoRaTransport: Send + Sync {
    /// Transmit a raw frame.
    async fn transmit(&self, frame: &[u8]) -> Result<(), LoRaTransportError>;

    /// Receive the next incoming frame (blocks until available).
    async fn receive(&self) -> Result<LoRaFrame, LoRaTransportError>;

    /// Send a best-effort ACK. Default is a no-op.
    async fn send_ack(&self, _frame: &[u8]) -> Result<(), LoRaTransportError> {
        Ok(())
    }
}
