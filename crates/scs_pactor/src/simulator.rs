use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rand::Rng;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{sleep, timeout};

use crate::{PactorLinkEvent, PactorLinkStatus, PactorTransport, ScsPactorError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PactorSpeed {
    P1,
    P2,
    P3,
    P4,
}

impl PactorSpeed {
    pub fn level(self) -> u8 {
        match self {
            Self::P1 => 1,
            Self::P2 => 2,
            Self::P3 => 3,
            Self::P4 => 4,
        }
    }

    pub fn raw_bps(self) -> u32 {
        match self {
            Self::P1 => 200,
            Self::P2 => 700,
            Self::P3 => 3200,
            Self::P4 => 5200,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SimulatedPactorConfig {
    pub speed: PactorSpeed,
    pub packet_loss: f32,
    pub latency: Duration,
    pub latency_jitter: Duration,
    pub setup_delay: Duration,
    pub max_retries: u32,
    pub read_timeout: Option<Duration>,
}

impl Default for SimulatedPactorConfig {
    fn default() -> Self {
        Self {
            speed: PactorSpeed::P4,
            packet_loss: 0.05,
            latency: Duration::from_millis(250),
            latency_jitter: Duration::from_millis(50),
            setup_delay: Duration::from_secs(2),
            max_retries: 8,
            read_timeout: Some(Duration::from_secs(10)),
        }
    }
}

struct EndpointState {
    mycall: Option<String>,
    connected_to: Option<String>,
}

struct Endpoint {
    state: Mutex<EndpointState>,
    data_tx: mpsc::Sender<Vec<u8>>,
    data_rx: Mutex<mpsc::Receiver<Vec<u8>>>,
    event_tx: mpsc::Sender<PactorLinkEvent>,
    event_rx: Mutex<mpsc::Receiver<PactorLinkEvent>>,
}

impl Endpoint {
    fn new() -> Arc<Self> {
        let (data_tx, data_rx) = mpsc::channel(1024);
        let (event_tx, event_rx) = mpsc::channel(1024);
        Arc::new(Self {
            state: Mutex::new(EndpointState {
                mycall: None,
                connected_to: None,
            }),
            data_tx,
            data_rx: Mutex::new(data_rx),
            event_tx,
            event_rx: Mutex::new(event_rx),
        })
    }

    async fn callsign(&self) -> Option<String> {
        self.state.lock().await.mycall.clone()
    }
}

#[derive(Clone)]
pub struct SimulatedPactorTransport {
    local: Arc<Endpoint>,
    remote: Arc<Endpoint>,
    config: SimulatedPactorConfig,
}

pub struct SimulatedPactorPair;

impl SimulatedPactorPair {
    pub fn new(
        config: SimulatedPactorConfig,
    ) -> (SimulatedPactorTransport, SimulatedPactorTransport) {
        let a = Endpoint::new();
        let b = Endpoint::new();
        (
            SimulatedPactorTransport {
                local: a.clone(),
                remote: b.clone(),
                config: config.clone(),
            },
            SimulatedPactorTransport {
                local: b,
                remote: a,
                config,
            },
        )
    }
}

#[async_trait]
impl PactorTransport for SimulatedPactorTransport {
    async fn set_mycall(&self, callsign: &str) -> Result<(), ScsPactorError> {
        self.local.state.lock().await.mycall = Some(callsign.to_owned());
        Ok(())
    }

    async fn connect_peer(&self, remote_call: &str) -> Result<(), ScsPactorError> {
        let actual_remote = self.remote.callsign().await;
        if actual_remote.as_deref() != Some(remote_call) {
            let _ = self
                .local
                .event_tx
                .send(PactorLinkEvent::Status(PactorLinkStatus::LinkFailure))
                .await;
            return Err(ScsPactorError::Disconnected);
        }

        let _ = self
            .local
            .event_tx
            .send(PactorLinkEvent::Status(PactorLinkStatus::Connecting {
                remote_call: remote_call.to_owned(),
            }))
            .await;

        sleep(self.config.setup_delay).await;

        let local_call = self
            .local
            .callsign()
            .await
            .unwrap_or_else(|| "UNKNOWN".to_owned());
        self.local.state.lock().await.connected_to = Some(remote_call.to_owned());
        self.remote.state.lock().await.connected_to = Some(local_call.clone());

        let local_connected = PactorLinkEvent::Status(PactorLinkStatus::Connected {
            remote_call: remote_call.to_owned(),
        });
        let remote_connected = PactorLinkEvent::Status(PactorLinkStatus::Connected {
            remote_call: local_call,
        });
        let _ = self.local.event_tx.send(local_connected).await;
        let _ = self.remote.event_tx.send(remote_connected).await;
        Ok(())
    }

    async fn write_data(&self, data: &[u8]) -> Result<(), ScsPactorError> {
        if self.local.state.lock().await.connected_to.is_none() {
            return Err(ScsPactorError::Disconnected);
        }

        let mut retries = 0;
        loop {
            let transmit_time = Duration::from_secs_f64(
                (data.len() * 8) as f64 / self.config.speed.raw_bps() as f64,
            );
            let jitter = if self.config.latency_jitter.is_zero() {
                Duration::ZERO
            } else {
                let jitter_ms = self.config.latency_jitter.as_millis() as u64;
                Duration::from_millis(rand::rng().random_range(0..=jitter_ms))
            };
            sleep(transmit_time + self.config.latency + jitter).await;

            if rand::rng().random::<f32>() >= self.config.packet_loss {
                self.remote
                    .data_tx
                    .send(data.to_vec())
                    .await
                    .map_err(|_| ScsPactorError::Disconnected)?;
                let _ = self
                    .local
                    .event_tx
                    .send(PactorLinkEvent::LinkQuality {
                        speed_level: self.config.speed.level(),
                        retries,
                    })
                    .await;
                return Ok(());
            }

            retries += 1;
            if retries > self.config.max_retries {
                let _ = self
                    .local
                    .event_tx
                    .send(PactorLinkEvent::Status(PactorLinkStatus::LinkFailure))
                    .await;
                return Err(ScsPactorError::Disconnected);
            }
        }
    }

    async fn read_data(&self, max_len: usize) -> Result<Vec<u8>, ScsPactorError> {
        let mut rx = self.local.data_rx.lock().await;
        let read = rx.recv();
        let mut data = if let Some(d) = self.config.read_timeout {
            timeout(d, read)
                .await
                .map_err(|_| ScsPactorError::Timeout)?
                .ok_or(ScsPactorError::Disconnected)?
        } else {
            read.await.ok_or(ScsPactorError::Disconnected)?
        };
        data.truncate(max_len);
        Ok(data)
    }

    async fn disconnect(&self) -> Result<(), ScsPactorError> {
        self.local.state.lock().await.connected_to = None;
        self.remote.state.lock().await.connected_to = None;
        let event = PactorLinkEvent::Status(PactorLinkStatus::Disconnected);
        let _ = self.local.event_tx.send(event.clone()).await;
        let _ = self.remote.event_tx.send(event).await;
        Ok(())
    }

    async fn next_event(
        &self,
        timeout_after: Option<Duration>,
    ) -> Result<PactorLinkEvent, ScsPactorError> {
        let mut rx = self.local.event_rx.lock().await;
        let read = rx.recv();
        if let Some(d) = timeout_after {
            timeout(d, read)
                .await
                .map_err(|_| ScsPactorError::Timeout)?
                .ok_or(ScsPactorError::Disconnected)
        } else {
            read.await.ok_or(ScsPactorError::Disconnected)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn two_simulators_connect_and_exchange_data() {
        let config = SimulatedPactorConfig {
            packet_loss: 0.0,
            latency: Duration::ZERO,
            latency_jitter: Duration::ZERO,
            setup_delay: Duration::ZERO,
            ..Default::default()
        };
        let (client, node) = SimulatedPactorPair::new(config);
        client.set_mycall("CLIENT").await.unwrap();
        node.set_mycall("NODE").await.unwrap();

        client.connect_peer("NODE").await.unwrap();
        assert_eq!(
            client
                .next_event(Some(Duration::from_millis(50)))
                .await
                .unwrap(),
            PactorLinkEvent::Status(PactorLinkStatus::Connecting {
                remote_call: "NODE".to_owned()
            })
        );
        assert_eq!(
            client
                .next_event(Some(Duration::from_millis(50)))
                .await
                .unwrap(),
            PactorLinkEvent::Status(PactorLinkStatus::Connected {
                remote_call: "NODE".to_owned()
            })
        );

        client.write_data(b"hello").await.unwrap();
        let received = node.read_data(1024).await.unwrap();
        assert_eq!(received, b"hello");
    }

    #[tokio::test]
    async fn simulator_retries_lost_frames_until_success_or_failure() {
        let config = SimulatedPactorConfig {
            packet_loss: 1.0,
            latency: Duration::ZERO,
            latency_jitter: Duration::ZERO,
            setup_delay: Duration::ZERO,
            max_retries: 2,
            ..Default::default()
        };
        let (client, node) = SimulatedPactorPair::new(config);
        client.set_mycall("CLIENT").await.unwrap();
        node.set_mycall("NODE").await.unwrap();
        client.connect_peer("NODE").await.unwrap();

        let err = client.write_data(b"lost").await.unwrap_err();
        assert!(matches!(err, ScsPactorError::Disconnected));
    }
}
