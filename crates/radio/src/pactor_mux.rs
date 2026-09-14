//! Multiplexes Alpenglow networks over one PACTOR modem link.
//!
//! PACTOR is one half-duplex serial pipe, so the mux tags each outbound frame
//! with a [`Channel`] and routes inbound payloads to typed [`MuxChannel`] queues.
//!
//! Turn discipline is the MODEM's job, not ours: the modem is brought up in
//! SCS "PACTOR duplex" (`PDUPLEX 1`, see `pactor_init`), where the ISS
//! changes over as soon as its TX buffer is empty and an IRS holding data
//! breaks in after ~12s, so both sides simply write when they have something
//! to say. An earlier software turn protocol (grant lines, Ctrl-Z
//! changeovers, silence reclaims, a caller ceiling) fought that arbiter —
//! stranded grants, 200–300s reclaim ladders, force-takes chopping frames —
//! and was removed. What remains is priority lanes, whole-message bursts
//! (never starve the modem mid-message, or it changes over per line), an
//! overflow guard on the serial feed, and idle keepalives for liveness.

use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use bunkerglow::consensus::LinkLiveness;

// Monotonic process-local clock for liveness timestamps.
static START: LazyLock<Instant> = LazyLock::new(Instant::now);

fn now_ms() -> u64 {
    START.elapsed().as_millis() as u64
}

use async_trait::async_trait;
use bunkerglow::network::Network;
use log::{debug, info, warn};
use scs_pactor::{PactorTransport, ScsPactorError};
use tokio::sync::{mpsc, Mutex};
use wincode::{SchemaRead, SchemaWrite};

use crate::pactor_framing::{fragment_message, Reassembler};

const DEFAULT_MAX_READ_LEN: usize = 8192;

/// Per-channel queue bound before backpressure reaches the single reader task.
const CHANNEL_QUEUE_DEPTH: usize = 1024;

/// Legacy control tag from the removed software turn protocol; still ignored
/// on receive so a peer running an older build cannot inject garbage.
const TURN_GRANT_TAG: u8 = 0xFF;

/// Control tag for idle traffic that keeps liveness clocks fresh.
const KEEPALIVE_TAG: u8 = 0xFE;

/// Send a keepalive once neither direction has carried anything for this
/// long. Inbound traffic counts: a keepalive from the receiving side would
/// force a modem break-in into the peer's stream for nothing.
const KEEPALIVE_IDLE: Duration = Duration::from_secs(30);

/// Periodic traffic summary interval.
const STATS_INTERVAL: Duration = Duration::from_secs(60);

/// Write-retry back-off; a writer that exits on error mutes the node forever.
const WRITE_RETRY_BACKOFF: Duration = Duration::from_secs(1);

/// Serial feed cap for the modem TX buffer, bytes/sec of hex line text. This
/// is an overflow guard, NOT link pacing: under PACTOR duplex the modem
/// changes over the moment its buffer runs dry, so feeding it slower than it
/// transmits would hand the turn away between every line and cost a ~12s
/// break-in per line. The default sits far above any PACTOR speed; pin lower
/// via BUNKER_LINK_PACE_BPS only to experiment. Tests run unpaced unless
/// they pin a rate.
fn pace_bytes_per_sec() -> u64 {
    let pinned = std::env::var("BUNKER_LINK_PACE_BPS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|v| *v > 0);
    pinned.unwrap_or(if cfg!(test) {
        u64::MAX
    } else {
        DEFAULT_PACE_BPS
    })
}

const DEFAULT_PACE_BPS: u64 = 4000;

/// Unsent backlog the pacer tolerates in the modem TX FIFO; keeps short
/// messages from serializing per-line while bounding priority inversion.
const PACE_BURST: Duration = Duration::from_secs(3);

/// Paces serial writes at the estimated on-air rate so the modem TX FIFO
/// never holds more than ~[`PACE_BURST`] of undrained data.
struct LinkPacer {
    rate_bps: u64,
    drain_until: tokio::time::Instant,
}

impl LinkPacer {
    fn new(rate_bps: u64) -> Self {
        Self {
            rate_bps: rate_bps.max(1),
            drain_until: tokio::time::Instant::now(),
        }
    }

    /// No-op pacer for full-duplex transports (simulator / TCP flow control).
    fn unpaced() -> Self {
        Self::new(u64::MAX)
    }

    /// Account one written serial line and sleep until the modem FIFO is back
    /// under the burst allowance at the estimated drain rate.
    async fn pace(&mut self, line_bytes: usize) {
        let now = tokio::time::Instant::now();
        let cost = Duration::from_millis((line_bytes as u64).saturating_mul(1000) / self.rate_bps);
        self.drain_until = self.drain_until.max(now) + cost;
        if let Some(sleep_until) = self.drain_until.checked_sub(PACE_BURST) {
            if sleep_until > now {
                tokio::time::sleep_until(sleep_until).await;
            }
        }
    }
}

/// Logical channel a multiplexed message belongs to. The discriminant is the
/// 1-byte tag prepended to every framed message before it goes over the link.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Channel {
    /// All-to-all consensus messages (`ConsensusMessage`).
    All2All = 0,
    /// Block dissemination shreds (`Shred`).
    Disseminator = 1,
    /// Repair: outgoing `RepairRequest`, incoming `RepairResponse`.
    Repair = 2,
    /// Repair-request handler: outgoing `RepairResponse`, incoming `RepairRequest`.
    RepairRequest = 3,
    /// Client transactions fed by [`MuxInjector`] and consumed by the block producer.
    Txs = 4,
}

impl Channel {
    const COUNT: usize = 5;

    fn from_tag(tag: u8) -> Option<Channel> {
        match tag {
            0 => Some(Channel::All2All),
            1 => Some(Channel::Disseminator),
            2 => Some(Channel::Repair),
            3 => Some(Channel::RepairRequest),
            4 => Some(Channel::Txs),
            _ => None,
        }
    }

    /// Peer routing tag; repair request/response channels cross, others do not.
    fn outbound_tag(self) -> Channel {
        match self {
            Channel::Repair => Channel::RepairRequest,
            Channel::RepairRequest => Channel::Repair,
            other => other,
        }
    }

    /// Votes/certs and repair requests are tiny and gate liveness; a decisive
    /// vote must not wait behind minutes of queued shreds.
    fn is_priority(self) -> bool {
        matches!(self, Channel::All2All | Channel::Repair)
    }
}

struct Outbound {
    channel: Channel,
    payload: Vec<u8>,
}

/// Two-lane outbound senders: tiny liveness-gating messages (votes, certs,
/// repair requests) preempt bulk shred and repair-response traffic.
#[derive(Clone)]
struct OutboundQueues {
    high: mpsc::Sender<Outbound>,
    low: mpsc::Sender<Outbound>,
}

impl OutboundQueues {
    fn for_channel(&self, channel: Channel) -> &mpsc::Sender<Outbound> {
        if channel.is_priority() {
            &self.high
        } else {
            &self.low
        }
    }

    fn queued(&self) -> u64 {
        let high = (self.high.max_capacity() - self.high.capacity()) as u64;
        let low = (self.low.max_capacity() - self.low.capacity()) as u64;
        high + low
    }
}

/// Owns one PACTOR modem and exposes logical [`MuxChannel`] networks over it.
pub struct PactorMux {
    transport: Arc<dyn PactorTransport>,
    max_read_len: usize,
    outbound: OutboundQueues,
    outbound_rx: Option<(mpsc::Receiver<Outbound>, mpsc::Receiver<Outbound>)>,
    /// Inbound routing: per-channel sender the reader forwards demuxed bytes to.
    inbound_tx: [Option<mpsc::Sender<Vec<u8>>>; Channel::COUNT],
    /// Per-channel inbound receivers, handed out by [`channel`](Self::channel).
    inbound_rx: [Option<mpsc::Receiver<Vec<u8>>>; Channel::COUNT],
    message_counter: Arc<AtomicU64>,
    /// Last inbound line timestamp; recent activity means [`MuxLiveness`] is up.
    last_activity_ms: Arc<AtomicU64>,
    /// Half-duplex radio link: paced writes plus idle keepalives. Full-duplex
    /// transports (simulator / TCP) write freely.
    half_duplex: bool,
    queued_gauge: Option<Arc<AtomicU64>>,
}

impl PactorMux {
    /// Wrap an already-connected full-duplex transport.
    pub fn new(transport: Arc<dyn PactorTransport>) -> Self {
        Self::build(transport, false)
    }

    /// Wrap a half-duplex PACTOR transport. The modem arbitrates who transmits,
    /// so `_is_caller` no longer changes behavior; it is kept for call-site
    /// stability (the caller/listener roles still matter to link setup).
    pub fn new_half_duplex(transport: Arc<dyn PactorTransport>, _is_caller: bool) -> Self {
        Self::build(transport, true)
    }

    fn build(transport: Arc<dyn PactorTransport>, half_duplex: bool) -> Self {
        let (high_tx, high_rx) = mpsc::channel(CHANNEL_QUEUE_DEPTH);
        let (low_tx, low_rx) = mpsc::channel(CHANNEL_QUEUE_DEPTH);
        let mut inbound_tx: [Option<mpsc::Sender<Vec<u8>>>; Channel::COUNT] = Default::default();
        let mut inbound_rx: [Option<mpsc::Receiver<Vec<u8>>>; Channel::COUNT] = Default::default();
        for i in 0..Channel::COUNT {
            let (tx, rx) = mpsc::channel(CHANNEL_QUEUE_DEPTH);
            inbound_tx[i] = Some(tx);
            inbound_rx[i] = Some(rx);
        }
        Self {
            transport,
            max_read_len: DEFAULT_MAX_READ_LEN,
            outbound: OutboundQueues {
                high: high_tx,
                low: low_tx,
            },
            outbound_rx: Some((high_rx, low_rx)),
            inbound_tx,
            inbound_rx,
            // Session-unique ids prevent stale reassembler fragments from a
            // restarted peer merging with new messages.
            message_counter: Arc::new(AtomicU64::new(u64::from(rand::random::<u32>()) << 32)),
            last_activity_ms: Arc::new(AtomicU64::new(now_ms())),
            half_duplex,
            queued_gauge: None,
        }
    }

    /// Configure the outbound-queue depth gauge. Call before [`spawn`](Self::spawn).
    pub fn set_queued_gauge(&mut self, gauge: Arc<AtomicU64>) {
        self.queued_gauge = Some(gauge);
    }

    /// Take one logical channel; panics if the channel was already taken.
    pub fn channel<S, R>(&mut self, channel: Channel) -> MuxChannel<S, R> {
        self.channel_inner(channel, false)
    }

    /// Like [`channel`](Self::channel) but self-delivering: each sent message is
    /// also delivered locally so a single link matches socket loopback semantics.
    pub fn channel_self_delivering<S, R>(&mut self, channel: Channel) -> MuxChannel<S, R> {
        self.channel_inner(channel, true)
    }

    /// Take a send-only injector for one channel. Call before [`spawn`](Self::spawn).
    pub fn injector(&self, channel: Channel) -> MuxInjector {
        MuxInjector {
            channel,
            outbound: self.outbound.clone(),
            self_delivery: self.inbound_tx[channel as usize].clone(),
        }
    }

    fn channel_inner<S, R>(&mut self, channel: Channel, self_deliver: bool) -> MuxChannel<S, R> {
        let inbound_rx = self.inbound_rx[channel as usize]
            .take()
            .unwrap_or_else(|| panic!("channel {channel:?} already taken"));
        let self_delivery = if self_deliver {
            self.inbound_tx[channel as usize].clone()
        } else {
            None
        };
        MuxChannel {
            channel,
            outbound: self.outbound.clone(),
            inbound_rx: Mutex::new(inbound_rx),
            self_delivery,
            _msg_types: PhantomData,
        }
    }

    /// Spawn reader/writer tasks after all channels have been taken.
    pub fn spawn(mut self) -> PactorMuxHandle {
        let transport = self.transport.clone();
        let max_read_len = self.max_read_len;
        let inbound_tx = self.inbound_tx.clone();
        let (high_rx, low_rx) = self.outbound_rx.take().expect("spawn called twice");
        // The writer keeps sender clones so one lane closing early cannot
        // asymmetrically end the biased receive; shutdown aborts the task.
        let outbound_keepalive = self.outbound.clone();
        let message_counter = self.message_counter.clone();
        let half_duplex = self.half_duplex;
        let last_activity_ms = self.last_activity_ms.clone();
        // Inbound line count for the periodic traffic summary.
        let rx_lines = Arc::new(AtomicU64::new(0));
        // Writer-side idle clock on the tokio timeline (virtual under paused
        // tests, monotonic in production); `last_activity_ms` stays on the
        // process clock for MuxLiveness.
        let epoch = tokio::time::Instant::now();
        let last_rx_tick = Arc::new(AtomicU64::new(0));

        // Reader: read, reassemble, strip tag, route to a channel queue.
        let reader_transport = transport.clone();
        let reader_activity = last_activity_ms.clone();
        let reader_rx_lines = rx_lines.clone();
        let reader_rx_tick = last_rx_tick.clone();
        let reader = tokio::spawn(async move {
            let mut reassembler = Reassembler::new();
            loop {
                let line = match reader_transport.read_data(max_read_len).await {
                    Ok(line) => {
                        // Stamp activity before routing so MuxLiveness sees it.
                        reader_activity.store(now_ms(), Ordering::Relaxed);
                        reader_rx_tick.store(epoch.elapsed().as_millis() as u64, Ordering::Relaxed);
                        reader_rx_lines.fetch_add(1, Ordering::Relaxed);
                        line
                    }
                    // Idle timeout: keep waiting, never tear down inbound queues.
                    Err(ScsPactorError::Timeout) => continue,
                    Err(e) => {
                        // Keep senders alive on link errors; consensus receive loops expect open queues.
                        debug!("[mux:reader] read error ({e}); link likely down, backing off");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };
                let Some(message) = reassembler.push_line(&line) else {
                    continue; // mid-message fragment or unparseable line
                };
                let Some((&tag, payload)) = message.split_first() else {
                    warn!("[mux:reader] dropping empty multiplexed message");
                    continue;
                };
                if tag == KEEPALIVE_TAG || tag == TURN_GRANT_TAG {
                    continue; // control chatter; activity was already stamped
                }
                let Some(channel) = Channel::from_tag(tag) else {
                    warn!("[mux:reader] dropping message with unknown channel tag {tag}");
                    continue;
                };
                if let Some(tx) = &inbound_tx[channel as usize] {
                    if tx.send(payload.to_vec()).await.is_err() {
                        debug!("[mux:reader] channel {channel:?} closed; dropping message");
                        continue;
                    }
                }
            }
        });

        // Writer: drain the lanes (priority first), paced; keepalive when idle.
        let writer_transport = transport.clone();
        let writer_rx_tick = last_rx_tick;
        let writer_rx_lines = rx_lines;
        let queues_for_stats = self.outbound.clone();
        let writer = tokio::spawn(async move {
            let _outbound_keepalive = outbound_keepalive;
            let mut high_rx = high_rx;
            let mut low_rx = low_rx;
            let counter = message_counter;

            /// Writes one tagged message as paced fragment lines; returns the
            /// serial bytes written.
            async fn write_message(
                transport: &Arc<dyn PactorTransport>,
                counter: &AtomicU64,
                pacer: &mut LinkPacer,
                tag: u8,
                payload: &[u8],
            ) -> Result<usize, ScsPactorError> {
                let mut tagged = Vec::with_capacity(payload.len() + 1);
                tagged.push(tag);
                tagged.extend_from_slice(payload);
                let message_id = counter.fetch_add(1, Ordering::Relaxed);
                let mut serial = 0usize;
                for line in fragment_message(message_id, &tagged) {
                    transport.write_data(&line).await?;
                    // On-air cost of the hex line: `#` + 4-len + 2×data + `\r`.
                    let cost = line.len() * 2 + 6;
                    serial += cost;
                    pacer.pace(cost).await;
                }
                Ok(serial)
            }

            // Priority receive: the high lane drains fully before the low lane.
            async fn recv_prio(
                high: &mut mpsc::Receiver<Outbound>,
                low: &mut mpsc::Receiver<Outbound>,
            ) -> Option<Outbound> {
                tokio::select! {
                    biased;
                    item = high.recv() => item,
                    item = low.recv() => item,
                }
            }

            // Full-duplex mode (simulator / TCP): transmit whenever we have data.
            if !half_duplex {
                let mut pacer = LinkPacer::unpaced();
                while let Some(item) = recv_prio(&mut high_rx, &mut low_rx).await {
                    if let Err(e) = write_message(
                        &writer_transport,
                        &counter,
                        &mut pacer,
                        item.channel as u8,
                        &item.payload,
                    )
                    .await
                    {
                        // Drop the message and keep the writer alive; consensus re-sends.
                        warn!("[mux:writer] write_data failed: {e}; dropping message");
                        tokio::time::sleep(WRITE_RETRY_BACKOFF).await;
                    }
                }
                debug!("[mux:writer] outbound queue closed; writer stopping");
                return;
            }

            // Half-duplex radio: the modem arbitrates ISS/IRS (an IRS with
            // buffered data breaks in), so we never wait for a turn — we just
            // keep the FIFO shallow and stay quiet when the link is quiet.
            let rate = pace_bytes_per_sec();
            info!("[mux] half-duplex: modem-arbitrated turns, pacing {rate} B/s");
            let mut pacer = LinkPacer::new(rate);
            // Persists across iterations: the modem FIFO does not reset.
            let mut last_tx_tick = 0u64;
            let mut stats_at = tokio::time::Instant::now() + STATS_INTERVAL;
            let (mut tx_lines, mut tx_bytes, mut rx_seen) = (0u64, 0u64, 0u64);
            loop {
                if tokio::time::Instant::now() >= stats_at {
                    stats_at += STATS_INTERVAL;
                    let rx_total = writer_rx_lines.load(Ordering::Relaxed);
                    let rx_delta = rx_total - rx_seen;
                    rx_seen = rx_total;
                    if tx_lines + rx_delta > 0 {
                        info!(
                            "[mux] last {}s: tx {tx_lines} msgs / {tx_bytes} B on air, rx {rx_delta} lines, {} queued",
                            STATS_INTERVAL.as_secs(),
                            queues_for_stats.queued()
                        );
                    }
                    tx_lines = 0;
                    tx_bytes = 0;
                }
                let idle_since = |last_tx_tick: u64| {
                    let last_rx_tick = writer_rx_tick.load(Ordering::Relaxed);
                    let now_tick = epoch.elapsed().as_millis() as u64;
                    Duration::from_millis(now_tick.saturating_sub(last_tx_tick.max(last_rx_tick)))
                };
                let keepalive_in = KEEPALIVE_IDLE.saturating_sub(idle_since(last_tx_tick));
                let item = tokio::select! {
                    biased;
                    item = recv_prio(&mut high_rx, &mut low_rx) => match item {
                        Some(item) => Some((item.channel as u8, item.payload)),
                        None => {
                            debug!("[mux:writer] outbound queue closed; writer stopping");
                            return;
                        }
                    },
                    // Re-check on wake: inbound that arrived mid-sleep means the
                    // link is alive and a keepalive would only force a break-in.
                    _ = tokio::time::sleep(keepalive_in) => {
                        (idle_since(last_tx_tick) >= KEEPALIVE_IDLE)
                            .then(|| (KEEPALIVE_TAG, Vec::new()))
                    }
                };
                if let Some((tag, payload)) = item {
                    match write_message(&writer_transport, &counter, &mut pacer, tag, &payload)
                        .await
                    {
                        Ok(serial) => {
                            last_tx_tick = epoch.elapsed().as_millis() as u64;
                            tx_lines += 1;
                            tx_bytes += serial as u64;
                        }
                        Err(e) => {
                            // Drop the message and keep the writer alive; consensus re-sends.
                            warn!("[mux:writer] write_data failed: {e}; dropping message");
                            tokio::time::sleep(WRITE_RETRY_BACKOFF).await;
                        }
                    }
                }
            }
        });

        // Sample from a separate task so the gauge updates while the writer is parked.
        let gauge_task = self.queued_gauge.take().map(|gauge| {
            let queues = self.outbound.clone();
            tokio::spawn(async move {
                loop {
                    gauge.store(queues.queued(), Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            })
        });

        PactorMuxHandle {
            transport,
            reader,
            writer,
            inbound_tx: self.inbound_tx.clone(),
            last_activity_ms,
            gauge_task,
        }
    }
}

/// Keepalive-driven [`LinkLiveness`] for the consensus crashed-leader timeout.
pub struct MuxLiveness {
    last_activity_ms: Arc<AtomicU64>,
}

/// Inbound-activity window for link liveness; must exceed the longest normal
/// inbound gap (fades), else Votor reads the link as dead and skips a leader's
/// still-in-transit block. It MUST be at least the RX-stall watchdog threshold
/// (BUNKER_RX_STALL_SECS, default 600s): the watchdog treats the link as UP
/// until that much silence, so liveness must too — otherwise there is a window
/// where the session is still up (block crawling across) yet Votor sees "dead"
/// and skips it. Observed fades run 110–559s of silence.
fn liveness_window() -> Duration {
    std::env::var("BUNKER_LIVENESS_WINDOW_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_secs(660))
}

impl LinkLiveness for MuxLiveness {
    fn is_link_alive(&self) -> bool {
        let last = self.last_activity_ms.load(Ordering::Relaxed);
        now_ms().saturating_sub(last) < liveness_window().as_millis() as u64
    }
}

/// Owns the reader/writer tasks of a spawned [`PactorMux`].
pub struct PactorMuxHandle {
    transport: Arc<dyn PactorTransport>,
    reader: tokio::task::JoinHandle<()>,
    writer: tokio::task::JoinHandle<()>,
    /// Retained until shutdown so inbound queues stay open after reader abort.
    inbound_tx: [Option<mpsc::Sender<Vec<u8>>>; Channel::COUNT],
    /// Shared inbound-activity clock surfaced via [`liveness`](Self::liveness).
    last_activity_ms: Arc<AtomicU64>,
    /// Outbound-queue gauge sampler, aborted on [`shutdown`](Self::shutdown).
    gauge_task: Option<tokio::task::JoinHandle<()>>,
}

impl PactorMuxHandle {
    /// Return a [`LinkLiveness`] backed by this mux's inbound-activity clock.
    pub fn liveness(&self) -> Arc<MuxLiveness> {
        Arc::new(MuxLiveness {
            last_activity_ms: self.last_activity_ms.clone(),
        })
    }

    /// Force a PACTOR ARQ changeover. The mux itself never issues one — the
    /// modem arbitrates turns — this exists for operator tooling only.
    pub async fn changeover(&self) -> std::io::Result<()> {
        self.transport
            .changeover()
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))
    }

    /// Shut the mux down and close inbound queues so detached receivers release DB handles.
    pub fn shutdown(&mut self) {
        self.reader.abort();
        self.writer.abort();
        if let Some(t) = &self.gauge_task {
            t.abort();
        }
        for slot in &mut self.inbound_tx {
            *slot = None;
        }
    }
}

/// One logical [`Network`] over the shared PACTOR link.
pub struct MuxChannel<S, R> {
    channel: Channel,
    outbound: OutboundQueues,
    inbound_rx: Mutex<mpsc::Receiver<Vec<u8>>>,
    /// Local loopback sender used by [`PactorMux::channel_self_delivering`].
    self_delivery: Option<mpsc::Sender<Vec<u8>>>,
    _msg_types: PhantomData<(S, R)>,
}

/// Send-only handle onto one mux channel, obtained from [`PactorMux::injector`].
#[derive(Clone)]
pub struct MuxInjector {
    channel: Channel,
    outbound: OutboundQueues,
    /// Optional local loopback sender for self-delivering channels.
    self_delivery: Option<mpsc::Sender<Vec<u8>>>,
}

impl MuxInjector {
    /// Serialize and enqueue `message`; self-delivery is best-effort.
    pub async fn send<S>(&self, message: &S) -> std::io::Result<()>
    where
        S: SchemaWrite<Src = S> + Send + Sync,
    {
        let payload = wincode::serialize(message)
            .map_err(|e| std::io::Error::other(format!("serialize failed: {e:?}")))?;
        if let Some(self_tx) = &self.self_delivery {
            let _ = self_tx.try_send(payload.clone());
        }
        self.outbound
            .for_channel(self.channel)
            .send(Outbound {
                channel: self.channel.outbound_tag(),
                payload,
            })
            .await
            .map_err(|_| std::io::Error::other("mux outbound queue closed"))
    }
}

impl<S, R> MuxChannel<S, R>
where
    S: SchemaWrite<Src = S> + Send + Sync,
{
    async fn enqueue(&self, message: &S) -> std::io::Result<()> {
        let payload = wincode::serialize(message)
            .map_err(|e| std::io::Error::other(format!("serialize failed: {e:?}")))?;
        // Best-effort loopback for consensus broadcasts that include self.
        if let Some(self_tx) = &self.self_delivery {
            let _ = self_tx.try_send(payload.clone());
        }
        self.outbound
            .for_channel(self.channel)
            .send(Outbound {
                channel: self.channel.outbound_tag(),
                payload,
            })
            .await
            .map_err(|_| std::io::Error::other("mux outbound queue closed"))
    }
}

#[async_trait]
impl<S, R> Network for MuxChannel<S, R>
where
    S: SchemaWrite<Src = S> + Send + Sync,
    R: for<'de> SchemaRead<'de, Dst = R> + Send + Sync,
{
    type Recv = R;
    type Send = S;

    async fn send_to_many(
        &self,
        message: &S,
        addrs: impl Iterator<Item = SocketAddr> + Send,
    ) -> std::io::Result<()> {
        // One physical peer on the link: enqueue once if any address was given.
        if addrs.into_iter().next().is_some() {
            self.enqueue(message).await?;
        }
        Ok(())
    }

    async fn send(&self, message: &S, _addr: SocketAddr) -> std::io::Result<()> {
        self.enqueue(message).await
    }

    async fn receive(&self) -> std::io::Result<R> {
        loop {
            let bytes = {
                let mut rx = self.inbound_rx.lock().await;
                rx.recv()
                    .await
                    .ok_or_else(|| std::io::Error::other("mux inbound queue closed"))?
            };
            match wincode::deserialize(&bytes) {
                Ok(msg) => return Ok(msg),
                Err(err) => {
                    // Log enough payload context to distinguish truncation, collisions, and type mismatches.
                    let prefix_len = bytes.len().min(48);
                    warn!(
                        "MuxChannel({:?}) deserialize failed ({err:?}); payload {} bytes, \
                         prefix {:02x?}; waiting for next message",
                        self.channel,
                        bytes.len(),
                        &bytes[..prefix_len],
                    );
                    continue;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The default liveness window must span a realistic fade — the link stays
    /// silent 110–559s but is NOT down (RX-stall watchdog only fires at 600s).
    /// A shorter window made Votor read "dead" mid-fade and skip a leader's
    /// still-in-transit block (node1's trailing window slot was lost this way).
    /// The window must sit above the worst fade yet at/above the 600s watchdog,
    /// so liveness and the watchdog agree on when the link is actually down.
    #[test]
    fn default_liveness_window_spans_a_realistic_fade() {
        // No env override: exercise the shipped default.
        // SAFETY: single-threaded test; no other test sets this var.
        unsafe {
            std::env::remove_var("BUNKER_LIVENESS_WINDOW_MS");
        }
        let window = liveness_window();
        assert!(
            window >= Duration::from_secs(560),
            "liveness window {window:?} must exceed the worst observed fade (559s), \
             else Votor reads a live-but-fading link as dead and skips the leader's block"
        );
        assert!(
            window >= Duration::from_secs(600),
            "liveness window {window:?} must be at least the RX-stall watchdog \
             threshold (600s) so liveness and the watchdog agree the link is up"
        );
    }
    use scs_pactor::{PactorLinkEvent, ScsPactorError};
    use std::collections::VecDeque;
    use std::sync::atomic::AtomicU32;
    use std::time::Duration;
    use tokio::sync::Mutex as TokioMutex;

    /// Loopback pair for two in-process muxes.
    struct LoopbackTransport {
        /// Lines written by this station.
        out_tx: mpsc::UnboundedSender<Vec<u8>>,
        /// Lines received from the peer.
        in_rx: TokioMutex<mpsc::UnboundedReceiver<Vec<u8>>>,
    }

    impl LoopbackTransport {
        fn pair() -> (Arc<LoopbackTransport>, Arc<LoopbackTransport>) {
            let (a_out, b_in) = mpsc::unbounded_channel();
            let (b_out, a_in) = mpsc::unbounded_channel();
            let a = Arc::new(LoopbackTransport {
                out_tx: a_out,
                in_rx: TokioMutex::new(a_in),
            });
            let b = Arc::new(LoopbackTransport {
                out_tx: b_out,
                in_rx: TokioMutex::new(b_in),
            });
            (a, b)
        }
    }

    #[async_trait]
    impl PactorTransport for LoopbackTransport {
        async fn set_mycall(&self, _callsign: &str) -> Result<(), ScsPactorError> {
            Ok(())
        }
        async fn connect_peer(&self, _remote_call: &str) -> Result<(), ScsPactorError> {
            Ok(())
        }
        async fn write_data(&self, data: &[u8]) -> Result<(), ScsPactorError> {
            assert!(
                1 + data.len() * 2 < crate::pactor_framing::RADIO_MTU,
                "fragment line exceeds MTU"
            );
            self.out_tx
                .send(data.to_vec())
                .map_err(|_| ScsPactorError::Disconnected)
        }
        async fn read_data(&self, _max_len: usize) -> Result<Vec<u8>, ScsPactorError> {
            self.in_rx
                .lock()
                .await
                .recv()
                .await
                .ok_or(ScsPactorError::Disconnected)
        }
        async fn disconnect(&self) -> Result<(), ScsPactorError> {
            Ok(())
        }
        async fn next_event(
            &self,
            _timeout_after: Option<Duration>,
        ) -> Result<PactorLinkEvent, ScsPactorError> {
            Err(ScsPactorError::Timeout)
        }
    }

    /// Recording-only transport for writer-path tests; counts changeovers so
    /// tests can prove the mux never issues one.
    struct RecordingTransport {
        written: TokioMutex<Vec<Vec<u8>>>,
        inbound: TokioMutex<VecDeque<Vec<u8>>>,
        changeovers: AtomicU32,
    }

    impl RecordingTransport {
        fn new() -> Self {
            Self {
                written: TokioMutex::new(Vec::new()),
                inbound: TokioMutex::new(VecDeque::new()),
                changeovers: AtomicU32::new(0),
            }
        }

        /// Tags of every complete message written so far.
        async fn written_tags(&self) -> Vec<u8> {
            let lines = self.written.lock().await.clone();
            let mut reassembler = Reassembler::new();
            lines
                .iter()
                .filter_map(|line| reassembler.push_line(line).and_then(|m| m.first().copied()))
                .collect()
        }
    }

    #[async_trait]
    impl PactorTransport for RecordingTransport {
        async fn set_mycall(&self, _callsign: &str) -> Result<(), ScsPactorError> {
            Ok(())
        }
        async fn connect_peer(&self, _remote_call: &str) -> Result<(), ScsPactorError> {
            Ok(())
        }
        async fn write_data(&self, data: &[u8]) -> Result<(), ScsPactorError> {
            self.written.lock().await.push(data.to_vec());
            Ok(())
        }
        async fn read_data(&self, _max_len: usize) -> Result<Vec<u8>, ScsPactorError> {
            // Park forever once drained so the reader keeps its queues open.
            loop {
                if let Some(line) = self.inbound.lock().await.pop_front() {
                    return Ok(line);
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
        async fn changeover(&self) -> Result<(), ScsPactorError> {
            self.changeovers.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn disconnect(&self) -> Result<(), ScsPactorError> {
            Ok(())
        }
        async fn next_event(
            &self,
            _timeout_after: Option<Duration>,
        ) -> Result<PactorLinkEvent, ScsPactorError> {
            Err(ScsPactorError::Timeout)
        }
    }

    #[tokio::test]
    async fn all_five_channels_idle_stay_open() {
        // Long-lived consensus receivers must stay pending while idle.
        let transport = Arc::new(RecordingTransport::new());
        let mut mux = PactorMux::new(transport);
        let chans = [
            Channel::All2All,
            Channel::Disseminator,
            Channel::Repair,
            Channel::RepairRequest,
            Channel::Txs,
        ];
        let mut probes = Vec::new();
        for c in chans {
            let ch: MuxChannel<Vec<u8>, Vec<u8>> = mux.channel(c);
            probes.push(tokio::spawn(async move { ch.receive().await }));
        }
        let _h = mux.spawn();

        tokio::time::sleep(Duration::from_millis(300)).await;
        for (i, p) in probes.iter().enumerate() {
            assert!(!p.is_finished(), "channel {i} receive returned early");
        }
        for p in probes {
            p.abort();
        }
    }

    #[tokio::test]
    async fn idle_channel_receiver_stays_open() {
        // An idle receiver must park, not observe a closed queue.
        let transport = Arc::new(RecordingTransport::new());
        let mut mux = PactorMux::new(transport);
        let repair: MuxChannel<Vec<u8>, Vec<u8>> = mux.channel(Channel::Repair);
        let _h = mux.spawn();

        let probe = tokio::spawn(async move { repair.receive().await });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !probe.is_finished(),
            "idle receive returned early (queue closed?)"
        );
        probe.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn pacer_bounds_modem_fifo_backlog() {
        let t0 = tokio::time::Instant::now();
        let mut pacer = LinkPacer::new(1000);
        // 1s of backlog fits inside the burst allowance: no sleep.
        pacer.pace(1000).await;
        assert_eq!(tokio::time::Instant::now(), t0);
        // 6s total backlog must sleep until only PACE_BURST remains unsent.
        pacer.pace(5000).await;
        assert_eq!(
            tokio::time::Instant::now(),
            t0 + Duration::from_secs(6) - PACE_BURST
        );
        // The unpaced variant never sleeps regardless of volume.
        let mut unpaced = LinkPacer::unpaced();
        let before = tokio::time::Instant::now();
        unpaced.pace(usize::MAX).await;
        assert_eq!(tokio::time::Instant::now(), before);
    }

    /// Half-duplex idles quietly, then keepalives once the link has carried
    /// nothing in either direction for KEEPALIVE_IDLE.
    #[tokio::test(start_paused = true)]
    async fn half_duplex_keepalives_only_after_quiet_period() {
        let transport = Arc::new(RecordingTransport::new());
        let mut mux = PactorMux::new_half_duplex(transport.clone(), true);
        let _chan: MuxChannel<Vec<u8>, Vec<u8>> = mux.channel(Channel::All2All);
        let _h = mux.spawn();

        tokio::time::sleep(KEEPALIVE_IDLE - Duration::from_secs(2)).await;
        assert!(
            transport.written.lock().await.is_empty(),
            "no keepalive before the idle period elapses"
        );
        tokio::time::sleep(Duration::from_secs(4)).await;
        assert_eq!(
            transport.written_tags().await,
            vec![KEEPALIVE_TAG],
            "exactly one keepalive once idle"
        );
        // Quiet for another period: keepalives repeat at the idle cadence.
        tokio::time::sleep(KEEPALIVE_IDLE).await;
        assert_eq!(transport.written_tags().await.len(), 2);
    }

    /// Inbound traffic proves the link alive, so the receiving side stays
    /// silent instead of forcing a modem break-in into the peer's stream.
    #[tokio::test(start_paused = true)]
    async fn half_duplex_inbound_suppresses_keepalive() {
        let transport = Arc::new(RecordingTransport::new());
        let mut mux = PactorMux::new_half_duplex(transport.clone(), false);
        let _chan: MuxChannel<Vec<u8>, Vec<u8>> = mux.channel(Channel::Disseminator);
        let _h = mux.spawn();

        // A fragment arrives every 20s for two minutes: never idle long enough.
        for _ in 0..6 {
            let line = fragment_message(7, &[Channel::Disseminator as u8, 1, 2, 3]).remove(0);
            transport.inbound.lock().await.push_back(line);
            tokio::time::sleep(Duration::from_secs(20)).await;
        }
        assert!(
            transport.written.lock().await.is_empty(),
            "receiving side must not keepalive while inbound flows"
        );
    }

    /// The modem arbitrates turns: the mux must never write turn-grant control
    /// messages or issue a changeover, in any half-duplex traffic pattern.
    #[tokio::test]
    async fn half_duplex_never_issues_turn_control() {
        let transport = Arc::new(RecordingTransport::new());
        let mut mux = PactorMux::new_half_duplex(transport.clone(), true);
        let shreds: MuxChannel<Vec<u8>, Vec<u8>> = mux.channel(Channel::Disseminator);
        let votes: MuxChannel<Vec<u8>, Vec<u8>> = mux.channel(Channel::All2All);
        let addr = "127.0.0.1:1".parse().unwrap();
        for i in 0..6u8 {
            shreds.send(&vec![i; 1024], addr).await.unwrap();
        }
        votes.send(&b"vote".to_vec(), addr).await.unwrap();
        let _h = mux.spawn();
        tokio::time::sleep(Duration::from_millis(500)).await;

        let tags = transport.written_tags().await;
        assert_eq!(tags.len(), 7, "every queued message ships without a turn");
        assert!(
            tags.iter().all(|t| *t != TURN_GRANT_TAG),
            "no turn-grant control messages"
        );
        assert_eq!(
            transport.changeovers.load(Ordering::SeqCst),
            0,
            "the mux must never issue a modem changeover"
        );
    }

    #[tokio::test]
    async fn half_duplex_both_sides_send_without_turns() {
        // Both sides write freely; the (modem-modelled) medium serializes them.
        let (a, b) = LoopbackTransport::pair();

        let mut mux_a = PactorMux::new_half_duplex(a, true);
        let a_chan: MuxChannel<Vec<u8>, Vec<u8>> = mux_a.channel(Channel::All2All);
        let _ha = mux_a.spawn();

        let mut mux_b = PactorMux::new_half_duplex(b, false);
        let b_chan: MuxChannel<Vec<u8>, Vec<u8>> = mux_b.channel(Channel::All2All);
        let _hb = mux_b.spawn();

        let addr = "127.0.0.1:1".parse().unwrap();
        b_chan.send(&b"from-b".to_vec(), addr).await.unwrap();
        a_chan.send(&b"from-a".to_vec(), addr).await.unwrap();

        let got_at_b = tokio::time::timeout(Duration::from_secs(10), b_chan.receive())
            .await
            .expect("B should receive A's message before timeout")
            .unwrap();
        let got_at_a = tokio::time::timeout(Duration::from_secs(10), a_chan.receive())
            .await
            .expect("A should receive B's message before timeout")
            .unwrap();
        assert_eq!(got_at_b, b"from-a");
        assert_eq!(got_at_a, b"from-b");
    }

    /// Repair request/response channels cross so each peer receives the right type.
    #[tokio::test]
    async fn repair_channels_cross_over_the_link() {
        let (a, b) = LoopbackTransport::pair();

        let mut mux_a = PactorMux::new(a);
        let a_repair: MuxChannel<Vec<u8>, Vec<u8>> = mux_a.channel(Channel::Repair);
        let _ha = mux_a.spawn();

        let mut mux_b = PactorMux::new(b);
        let b_repair_req: MuxChannel<Vec<u8>, Vec<u8>> = mux_b.channel(Channel::RepairRequest);
        let _hb = mux_b.spawn();

        let addr = "127.0.0.1:1".parse().unwrap();

        a_repair.send(&b"request".to_vec(), addr).await.unwrap();
        let got = tokio::time::timeout(Duration::from_secs(5), b_repair_req.receive())
            .await
            .expect("request must arrive on the peer's RepairRequest channel")
            .unwrap();
        assert_eq!(got, b"request");

        b_repair_req
            .send(&b"response".to_vec(), addr)
            .await
            .unwrap();
        let got = tokio::time::timeout(Duration::from_secs(5), a_repair.receive())
            .await
            .expect("response must arrive on the requester's Repair channel")
            .unwrap();
        assert_eq!(got, b"response");
    }

    #[tokio::test]
    async fn two_channels_demux_independently() {
        let (a, b) = LoopbackTransport::pair();

        let mut mux_a = PactorMux::new(a);
        let a_all2all: MuxChannel<Vec<u8>, Vec<u8>> = mux_a.channel(Channel::All2All);
        let a_shred: MuxChannel<Vec<u8>, Vec<u8>> = mux_a.channel(Channel::Disseminator);
        let _handle_a = mux_a.spawn();

        let mut mux_b = PactorMux::new(b);
        let b_all2all: MuxChannel<Vec<u8>, Vec<u8>> = mux_b.channel(Channel::All2All);
        let b_shred: MuxChannel<Vec<u8>, Vec<u8>> = mux_b.channel(Channel::Disseminator);
        let _handle_b = mux_b.spawn();

        let addr = "127.0.0.1:1".parse().unwrap();
        a_shred
            .send(&b"shred-payload".to_vec(), addr)
            .await
            .unwrap();
        a_all2all
            .send(&b"vote-payload".to_vec(), addr)
            .await
            .unwrap();

        let got_all2all = b_all2all.receive().await.unwrap();
        let got_shred = b_shred.receive().await.unwrap();
        assert_eq!(got_all2all, b"vote-payload");
        assert_eq!(got_shred, b"shred-payload");
    }

    #[tokio::test]
    async fn large_message_fragments_and_reassembles_across_mux() {
        let (a, b) = LoopbackTransport::pair();

        let mut mux_a = PactorMux::new(a);
        let a_shred: MuxChannel<Vec<u8>, Vec<u8>> = mux_a.channel(Channel::Disseminator);
        let _ha = mux_a.spawn();

        let mut mux_b = PactorMux::new(b);
        let b_shred: MuxChannel<Vec<u8>, Vec<u8>> = mux_b.channel(Channel::Disseminator);
        let _hb = mux_b.spawn();

        let payload: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
        let addr = "127.0.0.1:1".parse().unwrap();
        a_shred.send(&payload, addr).await.unwrap();
        let got = b_shred.receive().await.unwrap();
        assert_eq!(got, payload);
    }

    #[tokio::test]
    async fn send_prepends_channel_tag_and_fragments() {
        let transport = Arc::new(RecordingTransport::new());
        let mut mux = PactorMux::new(transport.clone());
        let shred: MuxChannel<Vec<u8>, Vec<u8>> = mux.channel(Channel::Disseminator);
        let _h = mux.spawn();

        let addr = "127.0.0.1:1".parse().unwrap();
        shred.send(&b"hi".to_vec(), addr).await.unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
        let lines = transport.written.lock().await.clone();
        assert_eq!(lines.len(), 1, "small message is one line");
        let (_hdr, body) = crate::pactor_framing::parse_fragment(&lines[0]).unwrap();
        assert_eq!(body.first().copied(), Some(Channel::Disseminator as u8));
    }

    #[tokio::test]
    #[should_panic(expected = "already taken")]
    async fn taking_a_channel_twice_panics() {
        let transport = Arc::new(RecordingTransport::new());
        let mut mux = PactorMux::new(transport);
        let _first: MuxChannel<Vec<u8>, Vec<u8>> = mux.channel(Channel::All2All);
        let _second: MuxChannel<Vec<u8>, Vec<u8>> = mux.channel(Channel::All2All);
    }

    /// Fails the first N writes, then delegates; models a transient modem error.
    struct FlakyTransport {
        inner: Arc<LoopbackTransport>,
        failures_left: AtomicU32,
    }

    #[async_trait]
    impl PactorTransport for FlakyTransport {
        async fn set_mycall(&self, c: &str) -> Result<(), ScsPactorError> {
            self.inner.set_mycall(c).await
        }
        async fn connect_peer(&self, c: &str) -> Result<(), ScsPactorError> {
            self.inner.connect_peer(c).await
        }
        async fn write_data(&self, data: &[u8]) -> Result<(), ScsPactorError> {
            if self
                .failures_left
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(ScsPactorError::Timeout);
            }
            self.inner.write_data(data).await
        }
        async fn read_data(&self, n: usize) -> Result<Vec<u8>, ScsPactorError> {
            self.inner.read_data(n).await
        }
        async fn disconnect(&self) -> Result<(), ScsPactorError> {
            self.inner.disconnect().await
        }
        async fn next_event(&self, t: Option<Duration>) -> Result<PactorLinkEvent, ScsPactorError> {
            self.inner.next_event(t).await
        }
    }

    /// A transient write failure must not kill the writer: a dead writer mutes
    /// the node while the link looks alive and the wedge watchdog sees nothing.
    #[tokio::test]
    async fn writer_survives_transient_write_failure() {
        let (a_raw, b) = LoopbackTransport::pair();
        let a = Arc::new(FlakyTransport {
            inner: a_raw,
            failures_left: AtomicU32::new(1),
        });

        let mut mux_a = PactorMux::new_half_duplex(a, true);
        let a_chan: MuxChannel<Vec<u8>, Vec<u8>> = mux_a.channel(Channel::All2All);
        let ha = mux_a.spawn();

        let mut mux_b = PactorMux::new_half_duplex(b, false);
        let b_chan: MuxChannel<Vec<u8>, Vec<u8>> = mux_b.channel(Channel::All2All);
        let _hb = mux_b.spawn();

        let addr = "127.0.0.1:1".parse().unwrap();
        // First message hits the write failure and is dropped.
        a_chan.send(&b"dropped".to_vec(), addr).await.unwrap();
        a_chan.send(&b"delivered".to_vec(), addr).await.unwrap();

        let got = tokio::time::timeout(Duration::from_secs(10), b_chan.receive())
            .await
            .expect("writer must survive a transient write failure and keep sending")
            .unwrap();
        assert_eq!(got, b"delivered");
        assert!(!ha.writer.is_finished(), "writer task must stay alive");
    }

    /// A decisive vote must not wait behind minutes of queued shreds.
    #[tokio::test]
    async fn all2all_preempts_queued_bulk_traffic() {
        let transport = Arc::new(RecordingTransport::new());
        let mut mux = PactorMux::new_half_duplex(transport.clone(), true);
        let shreds: MuxChannel<Vec<u8>, Vec<u8>> = mux.channel(Channel::Disseminator);
        let votes: MuxChannel<Vec<u8>, Vec<u8>> = mux.channel(Channel::All2All);

        // Bulk backlog enqueued first, the vote last — priority must invert that.
        let addr = "127.0.0.1:1".parse().unwrap();
        for i in 0..3u8 {
            shreds.send(&vec![i; 60], addr).await.unwrap();
        }
        votes.send(&b"vote".to_vec(), addr).await.unwrap();
        let _h = mux.spawn();
        tokio::time::sleep(Duration::from_millis(400)).await;

        let first_data_tag = transport
            .written_tags()
            .await
            .into_iter()
            .find(|tag| *tag != KEEPALIVE_TAG);
        assert_eq!(
            first_data_tag,
            Some(Channel::All2All as u8),
            "the vote must be transmitted before the queued shreds"
        );
    }
}
