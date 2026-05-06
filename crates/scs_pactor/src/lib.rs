pub mod hostmode;
pub mod simulator;
pub mod tcp;
pub mod usb;

use std::time::Duration;

use async_trait::async_trait;
use thiserror::Error;

pub use simulator::{
    PactorSpeed, SimulatedPactorConfig, SimulatedPactorPair, SimulatedPactorTransport,
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
    async fn write_data(&self, data: &[u8]) -> Result<(), ScsPactorError>;
    async fn read_data(&self, max_len: usize) -> Result<Vec<u8>, ScsPactorError>;
    async fn disconnect(&self) -> Result<(), ScsPactorError>;

    async fn next_event(
        &self,
        timeout_after: Option<Duration>,
    ) -> Result<PactorLinkEvent, ScsPactorError>;
}
