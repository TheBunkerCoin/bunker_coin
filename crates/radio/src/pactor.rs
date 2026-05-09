use std::sync::Arc;

use scs_pactor::PactorTransport;

use crate::{Network, NetworkError, NetworkMessage};

const DEFAULT_MAX_READ_LEN: usize = 4096;

/// Radio-network adapter backed by any PACTOR transport.
///
/// PACTOR is already a connected point-to-point link, so the `to` address is
/// used only as a guard against self-sends and empty destinations.
pub struct PactorRadioNode {
    callsign: String,
    transport: Arc<dyn PactorTransport>,
    max_read_len: usize,
}

impl PactorRadioNode {
    pub fn new<T>(callsign: impl Into<String>, transport: T) -> Self
    where
        T: PactorTransport + 'static,
    {
        Self::from_shared(callsign, Arc::new(transport))
    }

    pub fn from_shared(callsign: impl Into<String>, transport: Arc<dyn PactorTransport>) -> Self {
        Self {
            callsign: callsign.into(),
            transport,
            max_read_len: DEFAULT_MAX_READ_LEN,
        }
    }

    pub fn with_max_read_len(mut self, max_read_len: usize) -> Self {
        self.max_read_len = max_read_len;
        self
    }
}

impl Network for PactorRadioNode {
    type Address = String;

    async fn send(
        &self,
        msg: &NetworkMessage,
        to: impl AsRef<str> + Send,
    ) -> Result<(), NetworkError> {
        self.send_serialized(&msg.to_bytes(), to).await
    }

    async fn send_serialized(
        &self,
        bytes: &[u8],
        to: impl AsRef<str> + Send,
    ) -> Result<(), NetworkError> {
        let to = to.as_ref().trim();
        if to.is_empty() {
            return Err(NetworkError::Unknown);
        }
        if to.eq_ignore_ascii_case(&self.callsign) {
            return Ok(());
        }

        self.transport
            .write_data(bytes)
            .await
            .map_err(|_| NetworkError::Unknown)
    }

    async fn receive(&self) -> Result<NetworkMessage, NetworkError> {
        let payload = self
            .transport
            .read_data(self.max_read_len)
            .await
            .map_err(|_| NetworkError::Unknown)?;
        NetworkMessage::from_bytes(&payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scs_pactor::{PactorTransport, SimulatedPactorConfig, SimulatedPactorPair};

    #[tokio::test]
    async fn pactor_radio_node_round_trips_network_messages() {
        let config = SimulatedPactorConfig {
            packet_loss: 0.0,
            latency: std::time::Duration::ZERO,
            latency_jitter: std::time::Duration::ZERO,
            setup_delay: std::time::Duration::ZERO,
            ..Default::default()
        };
        let (client_transport, node_transport) = SimulatedPactorPair::new(config);
        client_transport.set_mycall("CLIENT").await.unwrap();
        node_transport.set_mycall("NODE").await.unwrap();
        client_transport.connect_peer("NODE").await.unwrap();

        let client = PactorRadioNode::new("CLIENT", client_transport);
        let node = PactorRadioNode::new("NODE", node_transport);
        let expected = b"radio-over-pactor".to_vec();

        client
            .send(&NetworkMessage::Shred(expected.clone()), "NODE")
            .await
            .unwrap();

        match node.receive().await.unwrap() {
            NetworkMessage::Shred(actual) => assert_eq!(actual, expected),
            other => panic!("expected Shred, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn pactor_radio_node_rejects_empty_destination() {
        let config = SimulatedPactorConfig {
            setup_delay: std::time::Duration::ZERO,
            ..Default::default()
        };
        let (transport, _) = SimulatedPactorPair::new(config);
        let node = PactorRadioNode::new("CLIENT", transport);

        let result = node.send(&NetworkMessage::Ping, "").await;

        assert!(result.is_err());
    }
}
