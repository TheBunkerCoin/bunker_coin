pub mod hostmode;
pub mod simulator;
pub mod tcp;
pub mod usb;

use std::time::Duration;

use async_trait::async_trait;
use thiserror::Error;

pub use simulator::{
    FadeWindow, PactorSpeed, SimulatedPactorConfig, SimulatedPactorPair, SimulatedPactorStats,
    SimulatedPactorTransport,
};
pub use tcp::{ScsPactorClient, ScsPactorConfig};
pub use usb::{UsbPactorConfig, UsbPactorTransport};

#[derive(Debug, Error)]
pub enum ScsPactorError {
    #[error("socket io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("framing error: {0}")]
    Framing(#[from] tokio_util::codec::LengthDelimitedCodecError),

    #[error("operation timed out")]
    Timeout,

    #[error("message exceeds MTU ({0} bytes)")]
    ExceedsMtu(usize),

    #[error("disconnected from peer")]
    Disconnected,

    #[error("modem is busy")]
    Busy,

    #[error("protocol error: {0}")]
    Protocol(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PactorLinkStatus {
    Idle,
    Connecting { remote_call: String },
    Connected { remote_call: String },
    Disconnected,
    LinkFailure,
    Busy,
    Queued,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PactorLinkEvent {
    Status(PactorLinkStatus),
    LinkQuality { speed_level: u8, retries: u32 },
}

#[async_trait]
pub trait PactorTransport: Send + Sync {
    async fn set_mycall(&self, callsign: &str) -> Result<(), ScsPactorError>;
    async fn connect_peer(&self, remote_call: &str) -> Result<(), ScsPactorError>;

    /// Wait (up to `timeout_after`) for an incoming connection to be accepted by
    /// this (listening) modem, then make the link ready for two-way data.
    ///
    /// The caller side uses [`connect_peer`]; the answering side uses this so it
    /// can also transmit (e.g. enter converse mode). Returns the remote callsign
    /// once connected.
    ///
    /// The default implementation assumes the underlying transport needs no
    /// extra setup to receive or reply (e.g. TCP / simulated), and is a no-op.
    async fn accept_incoming(
        &self,
        _timeout_after: Option<Duration>,
    ) -> Result<String, ScsPactorError> {
        Ok(String::new())
    }

    async fn write_data(&self, data: &[u8]) -> Result<(), ScsPactorError>;
    async fn read_data(&self, max_len: usize) -> Result<Vec<u8>, ScsPactorError>;

    /// Hand the transmit turn to the peer (PACTOR ARQ changeover).
    ///
    /// On a half-duplex ARQ link only the information-sending station transmits;
    /// to receive a reply, the sender must change over so the peer becomes the
    /// sender. The default implementation is a no-op for full-duplex transports
    /// (TCP / simulated) where both sides can send freely.
    async fn changeover(&self) -> Result<(), ScsPactorError> {
        Ok(())
    }

    async fn disconnect(&self) -> Result<(), ScsPactorError>;

    /// Whether the link is currently up.
    ///
    /// Returns `false` once the modem has reported a disconnect / STBY / link
    /// failure for the current session. Lets long-running callers (e.g. a
    /// consensus node) detect a mid-session drop and reconnect. The default is
    /// `true` for transports without an explicit link-down signal (TCP /
    /// simulated), which are considered up until a read/write errors.
    fn is_link_up(&self) -> bool {
        true
    }

    async fn next_event(
        &self,
        timeout_after: Option<Duration>,
    ) -> Result<PactorLinkEvent, ScsPactorError>;

    /// Broadcast data via PACTOR FEC mode.
    ///
    /// This is a stub for future implementation. PACTOR FEC broadcast
    /// may not be viable at PACTOR-IV speeds, so this returns an error
    /// by default. Implementations that support FEC broadcast can override.
    async fn broadcast_fec(&self, _data: &[u8]) -> Result<(), ScsPactorError> {
        Err(ScsPactorError::Protocol(
            "PACTOR FEC broadcast not yet implemented".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_pactor_broadcast_fec_stub_returns_error() {
        let (station_a, _station_b) = SimulatedPactorPair::new(SimulatedPactorConfig::default());
        let result = station_a.broadcast_fec(b"hello").await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            ScsPactorError::Protocol(msg) => {
                assert!(msg.contains("not yet implemented"), "got: {msg}");
            }
            other => panic!("expected Protocol error, got: {other:?}"),
        }
    }
}
