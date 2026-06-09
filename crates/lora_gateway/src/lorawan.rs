//! LoRaWAN transport stub.
//!
//! Placeholder for future integration with ChirpStack, TTN, or similar
//! LoRaWAN network servers via MQTT or gRPC.

use async_trait::async_trait;

use crate::transport::{LoRaFrame, LoRaTransport, LoRaTransportError};

/// Stub LoRaWAN transport — not yet implemented.
pub struct LoRaWanTransport;

#[async_trait]
impl LoRaTransport for LoRaWanTransport {
    async fn transmit(&self, _frame: &[u8]) -> Result<(), LoRaTransportError> {
        Err(LoRaTransportError::Io(
            "LoRaWAN transport not implemented".into(),
        ))
    }

    async fn receive(&self) -> Result<LoRaFrame, LoRaTransportError> {
        // Block forever — no data source connected
        std::future::pending().await
    }
}
