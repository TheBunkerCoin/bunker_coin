use std::sync::atomic::{AtomicU64, Ordering};
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

impl PactorSpeed {
    fn downshift(self) -> Self {
        match self {
            Self::P4 => Self::P3,
            Self::P3 => Self::P2,
            Self::P2 => Self::P1,
            Self::P1 => Self::P1,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FadeWindow {
    pub starts_after: Duration,
    pub duration: Duration,
}

impl FadeWindow {
    pub fn new(starts_after: Duration, duration: Duration) -> Self {
        Self {
            starts_after,
            duration,
        }
    }

    fn contains(self, elapsed: Duration) -> bool {
        elapsed >= self.starts_after && elapsed < self.starts_after + self.duration
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
    pub downshift_after_retries: u32,
    pub forced_initial_losses: u32,
    pub fade_windows: Vec<FadeWindow>,
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
            downshift_after_retries: 2,
            forced_initial_losses: 0,
            fade_windows: Vec::new(),
            read_timeout: Some(Duration::from_secs(10)),
        }
    }
}

impl SimulatedPactorConfig {
    pub fn good_link() -> Self {
        Self {
            speed: PactorSpeed::P4,
            packet_loss: 0.02,
            latency: Duration::from_millis(150),
            latency_jitter: Duration::from_millis(25),
            setup_delay: Duration::from_secs(2),
            max_retries: 8,
            downshift_after_retries: 2,
            forced_initial_losses: 0,
            fade_windows: Vec::new(),
            read_timeout: Some(Duration::from_secs(10)),
        }
    }

    pub fn marginal_link() -> Self {
        Self {
            speed: PactorSpeed::P3,
            packet_loss: 0.30,
            latency: Duration::from_millis(600),
            latency_jitter: Duration::from_millis(250),
            setup_delay: Duration::from_secs(4),
            max_retries: 12,
            downshift_after_retries: 1,
            forced_initial_losses: 0,
            fade_windows: Vec::new(),
            read_timeout: Some(Duration::from_secs(20)),
        }
    }

    pub fn fading_link(fade_windows: Vec<FadeWindow>) -> Self {
        Self {
            fade_windows,
            ..Self::marginal_link()
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

#[derive(Debug, Default)]
struct SharedStats {
    frames_attempted: AtomicU64,
    frames_delivered: AtomicU64,
    frames_lost: AtomicU64,
    retransmissions: AtomicU64,
    bytes_delivered: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SimulatedPactorStats {
    pub frames_attempted: u64,
    pub frames_delivered: u64,
    pub frames_lost: u64,
    pub retransmissions: u64,
    pub bytes_delivered: u64,
}

impl SharedStats {
    fn snapshot(&self) -> SimulatedPactorStats {
        SimulatedPactorStats {
            frames_attempted: self.frames_attempted.load(Ordering::Relaxed),
            frames_delivered: self.frames_delivered.load(Ordering::Relaxed),
            frames_lost: self.frames_lost.load(Ordering::Relaxed),
            retransmissions: self.retransmissions.load(Ordering::Relaxed),
            bytes_delivered: self.bytes_delivered.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone)]
pub struct SimulatedPactorTransport {
    local: Arc<Endpoint>,
    remote: Arc<Endpoint>,
    config: SimulatedPactorConfig,
    stats: Arc<SharedStats>,
    started_at: tokio::time::Instant,
}

pub struct SimulatedPactorPair;

impl SimulatedPactorPair {
    pub fn new(
        config: SimulatedPactorConfig,
    ) -> (SimulatedPactorTransport, SimulatedPactorTransport) {
        let a = Endpoint::new();
        let b = Endpoint::new();
        let stats = Arc::new(SharedStats::default());
        let started_at = tokio::time::Instant::now();
        (
            SimulatedPactorTransport {
                local: a.clone(),
                remote: b.clone(),
                config: config.clone(),
                stats: stats.clone(),
                started_at,
            },
            SimulatedPactorTransport {
                local: b,
                remote: a,
                config,
                stats,
                started_at,
            },
        )
    }
}

impl SimulatedPactorTransport {
    pub fn stats(&self) -> SimulatedPactorStats {
        self.stats.snapshot()
    }

    fn in_fade(&self) -> bool {
        let elapsed = self.started_at.elapsed();
        self.config
            .fade_windows
            .iter()
            .any(|window| window.contains(elapsed))
    }

    pub async fn send_command(&self, line: &str) -> Result<(), ScsPactorError> {
        let trimmed = line.trim();
        let mut parts = trimmed.splitn(2, char::is_whitespace);
        let command = parts.next().unwrap_or_default();
        let payload = parts.next().unwrap_or_default().trim();

        match command {
            "I" | "MYCALL" => {
                self.set_mycall(payload).await?;
                self.emit_status_line("OK").await;
                Ok(())
            }
            "C" | "CONNECT" => match self.connect_peer(payload).await {
                Ok(()) => Ok(()),
                Err(err) => {
                    self.emit_status_line("LINK FAILURE").await;
                    Err(err)
                }
            },
            "D" | "DISCONNECT" => {
                self.disconnect().await?;
                self.emit_status_line("DISCONNECTED").await;
                Ok(())
            }
            "G" => {
                self.emit_status_line("255,0").await;
                Ok(())
            }
            _ => Err(ScsPactorError::Protocol(format!(
                "unsupported simulator command {command}"
            ))),
        }
    }

    pub async fn read_status_line(&self) -> Result<String, ScsPactorError> {
        loop {
            match self.next_event(self.config.read_timeout).await? {
                PactorLinkEvent::Status(PactorLinkStatus::Connected { remote_call }) => {
                    return Ok(format!("CONNECTED {remote_call}"));
                }
                PactorLinkEvent::Status(PactorLinkStatus::Connecting { remote_call }) => {
                    return Ok(format!("CONNECTING {remote_call}"));
                }
                PactorLinkEvent::Status(PactorLinkStatus::Disconnected) => {
                    return Ok("DISCONNECTED".to_owned());
                }
                PactorLinkEvent::Status(PactorLinkStatus::LinkFailure) => {
                    return Ok("LINK FAILURE".to_owned());
                }
                PactorLinkEvent::Status(PactorLinkStatus::Busy) => return Ok("BUSY".to_owned()),
                PactorLinkEvent::Status(PactorLinkStatus::Queued) => {
                    return Ok("QUEUED".to_owned());
                }
                PactorLinkEvent::Status(PactorLinkStatus::Idle) => return Ok("IDLE".to_owned()),
                PactorLinkEvent::LinkQuality {
                    speed_level,
                    retries,
                } => {
                    return Ok(format!(
                        "LINK QUALITY SPEED={speed_level} RETRIES={retries}"
                    ))
                }
            }
        }
    }

    async fn emit_status_line(&self, line: &str) {
        let event = match line {
            "OK" => PactorLinkEvent::Status(PactorLinkStatus::Idle),
            "DISCONNECTED" => PactorLinkEvent::Status(PactorLinkStatus::Disconnected),
            "LINK FAILURE" => PactorLinkEvent::Status(PactorLinkStatus::LinkFailure),
            "BUSY" => PactorLinkEvent::Status(PactorLinkStatus::Busy),
            "QUEUED" => PactorLinkEvent::Status(PactorLinkStatus::Queued),
            line if line.starts_with("CONNECTED ") => {
                PactorLinkEvent::Status(PactorLinkStatus::Connected {
                    remote_call: line["CONNECTED ".len()..].trim().to_owned(),
                })
            }
            _ => PactorLinkEvent::Status(PactorLinkStatus::Idle),
        };
        let _ = self.local.event_tx.send(event).await;
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
        let mut speed = self.config.speed;
        loop {
            self.stats.frames_attempted.fetch_add(1, Ordering::Relaxed);
            let transmit_time =
                Duration::from_secs_f64((data.len() * 8) as f64 / speed.raw_bps() as f64);
            let jitter = if self.config.latency_jitter.is_zero() {
                Duration::ZERO
            } else {
                let jitter_ms = self.config.latency_jitter.as_millis() as u64;
                Duration::from_millis(rand::rng().random_range(0..=jitter_ms))
            };
            sleep(transmit_time + self.config.latency + jitter).await;

            let forced_loss = retries < self.config.forced_initial_losses;
            if !forced_loss
                && !self.in_fade()
                && rand::rng().random::<f32>() >= self.config.packet_loss
            {
                self.remote
                    .data_tx
                    .send(data.to_vec())
                    .await
                    .map_err(|_| ScsPactorError::Disconnected)?;
                self.stats.frames_delivered.fetch_add(1, Ordering::Relaxed);
                self.stats
                    .bytes_delivered
                    .fetch_add(data.len() as u64, Ordering::Relaxed);
                let _ = self
                    .local
                    .event_tx
                    .send(PactorLinkEvent::LinkQuality {
                        speed_level: speed.level(),
                        retries,
                    })
                    .await;
                return Ok(());
            }

            retries += 1;
            self.stats.frames_lost.fetch_add(1, Ordering::Relaxed);
            self.stats.retransmissions.fetch_add(1, Ordering::Relaxed);
            if retries >= self.config.downshift_after_retries {
                speed = speed.downshift();
            }
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
        let stats = client.stats();
        assert_eq!(stats.frames_delivered, 0);
        assert_eq!(stats.frames_lost, 3);
        assert_eq!(stats.retransmissions, 3);
    }

    #[tokio::test]
    async fn simulator_profiles_expose_expected_link_shapes() {
        let good = SimulatedPactorConfig::good_link();
        assert_eq!(good.speed, PactorSpeed::P4);
        assert!(good.packet_loss < 0.05);

        let marginal = SimulatedPactorConfig::marginal_link();
        assert_eq!(marginal.speed, PactorSpeed::P3);
        assert!(marginal.packet_loss > good.packet_loss);

        let fading = SimulatedPactorConfig::fading_link(vec![FadeWindow::new(
            Duration::from_secs(1),
            Duration::from_secs(2),
        )]);
        assert_eq!(fading.fade_windows.len(), 1);
    }

    #[tokio::test]
    async fn simulator_fade_window_drops_until_retry_budget_exhausts() {
        let config = SimulatedPactorConfig {
            packet_loss: 0.0,
            latency: Duration::ZERO,
            latency_jitter: Duration::ZERO,
            setup_delay: Duration::ZERO,
            max_retries: 1,
            fade_windows: vec![FadeWindow::new(Duration::ZERO, Duration::from_secs(10))],
            ..Default::default()
        };
        let (client, node) = SimulatedPactorPair::new(config);
        client.set_mycall("CLIENT").await.unwrap();
        node.set_mycall("NODE").await.unwrap();
        client.connect_peer("NODE").await.unwrap();

        let err = client.write_data(b"fade").await.unwrap_err();
        assert!(matches!(err, ScsPactorError::Disconnected));
        let stats = client.stats();
        assert_eq!(stats.frames_delivered, 0);
        assert_eq!(stats.frames_lost, 2);
    }

    #[tokio::test]
    async fn simulator_reports_downshifted_speed_after_retries() {
        let config = SimulatedPactorConfig {
            speed: PactorSpeed::P4,
            packet_loss: 0.0,
            latency: Duration::ZERO,
            latency_jitter: Duration::ZERO,
            setup_delay: Duration::ZERO,
            max_retries: 4,
            downshift_after_retries: 1,
            forced_initial_losses: 1,
            ..Default::default()
        };
        let (client, node) = SimulatedPactorPair::new(config);
        client.set_mycall("CLIENT").await.unwrap();
        node.set_mycall("NODE").await.unwrap();
        client.connect_peer("NODE").await.unwrap();

        client.write_data(b"hello").await.unwrap();
        let event = client
            .next_event(Some(Duration::from_millis(50)))
            .await
            .unwrap();
        assert_eq!(
            event,
            PactorLinkEvent::Status(PactorLinkStatus::Connecting {
                remote_call: "NODE".to_owned()
            })
        );
        let event = client
            .next_event(Some(Duration::from_millis(50)))
            .await
            .unwrap();
        assert_eq!(
            event,
            PactorLinkEvent::Status(PactorLinkStatus::Connected {
                remote_call: "NODE".to_owned()
            })
        );
        let event = client
            .next_event(Some(Duration::from_millis(50)))
            .await
            .unwrap();
        assert_eq!(
            event,
            PactorLinkEvent::LinkQuality {
                speed_level: PactorSpeed::P3.level(),
                retries: 1
            }
        );
        assert_eq!(node.read_data(1024).await.unwrap(), b"hello");
    }

    #[tokio::test]
    async fn simulator_command_surface_matches_hostmode_commands() {
        let config = SimulatedPactorConfig {
            packet_loss: 0.0,
            latency: Duration::ZERO,
            latency_jitter: Duration::ZERO,
            setup_delay: Duration::ZERO,
            ..Default::default()
        };
        let (client, node) = SimulatedPactorPair::new(config);

        client.send_command("I CLIENT").await.unwrap();
        node.send_command("I NODE").await.unwrap();

        client.send_command("C NODE").await.unwrap();
        assert_eq!(client.read_status_line().await.unwrap(), "IDLE");
        assert_eq!(client.read_status_line().await.unwrap(), "CONNECTING NODE");
        assert_eq!(client.read_status_line().await.unwrap(), "CONNECTED NODE");

        client.emit_status_line("QUEUED").await;
        assert_eq!(client.read_status_line().await.unwrap(), "QUEUED");

        client.send_command("D").await.unwrap();
        assert_eq!(client.read_status_line().await.unwrap(), "DISCONNECTED");
    }

    #[tokio::test]
    async fn simulator_clean_link_has_no_retransmission_overhead() {
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

        client.write_data(&[0xAB; 128]).await.unwrap();
        assert_eq!(node.read_data(1024).await.unwrap(), vec![0xAB; 128]);

        let stats = client.stats();
        assert_eq!(stats.frames_attempted, 1);
        assert_eq!(stats.frames_delivered, 1);
        assert_eq!(stats.frames_lost, 0);
        assert_eq!(stats.retransmissions, 0);
        assert_eq!(stats.bytes_delivered, 128);
    }

    #[tokio::test]
    async fn simulator_loss_degrades_effective_delivery_efficiency() {
        let clean = SimulatedPactorConfig {
            packet_loss: 0.0,
            latency: Duration::ZERO,
            latency_jitter: Duration::ZERO,
            setup_delay: Duration::ZERO,
            ..Default::default()
        };
        let lossy = SimulatedPactorConfig {
            packet_loss: 0.0,
            latency: Duration::ZERO,
            latency_jitter: Duration::ZERO,
            setup_delay: Duration::ZERO,
            forced_initial_losses: 2,
            max_retries: 4,
            ..Default::default()
        };

        let (clean_client, clean_node) = SimulatedPactorPair::new(clean);
        clean_client.set_mycall("CLIENT").await.unwrap();
        clean_node.set_mycall("NODE").await.unwrap();
        clean_client.connect_peer("NODE").await.unwrap();
        clean_client.write_data(&[0xCD; 64]).await.unwrap();

        let (lossy_client, lossy_node) = SimulatedPactorPair::new(lossy);
        lossy_client.set_mycall("CLIENT").await.unwrap();
        lossy_node.set_mycall("NODE").await.unwrap();
        lossy_client.connect_peer("NODE").await.unwrap();
        lossy_client.write_data(&[0xCD; 64]).await.unwrap();

        let clean_stats = clean_client.stats();
        let lossy_stats = lossy_client.stats();
        assert_eq!(clean_stats.frames_attempted, 1);
        assert_eq!(lossy_stats.frames_attempted, 3);
        assert_eq!(lossy_stats.frames_lost, 2);
        assert_eq!(lossy_stats.retransmissions, 2);
        assert_eq!(lossy_stats.bytes_delivered, clean_stats.bytes_delivered);
    }
}
