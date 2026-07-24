//! Multiplexes Alpenglow networks over one PACTOR modem link.
//!
//! PACTOR is one half-duplex serial pipe, so the mux tags each outbound frame
//! with a [`Channel`] and routes inbound payloads to typed [`MuxChannel`] queues.
//! In half-duplex mode only the turn-holder writes; keepalives preserve ARQ
//! liveness and silence-based reclaim recovers a lost turn grant.

use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use bunkerglow::consensus::LinkLiveness;

// Monotonic process-local clock for liveness and reclaim timestamps.
static START: LazyLock<Instant> = LazyLock::new(Instant::now);

fn now_ms() -> u64 {
    START.elapsed().as_millis() as u64
}

use async_trait::async_trait;
use bunkerglow::network::Network;
use log::{debug, warn};
use scs_pactor::{PactorTransport, ScsPactorError};
use tokio::sync::{mpsc, Mutex, Notify};
use wincode::{SchemaRead, SchemaWrite};

use crate::pactor_framing::{fragment_message, Reassembler};

const DEFAULT_MAX_READ_LEN: usize = 8192;

/// Per-channel queue bound before backpressure reaches the single reader task.
const CHANNEL_QUEUE_DEPTH: usize = 1024;

/// Control tag used to hand the transmit turn to the peer.
const TURN_GRANT_TAG: u8 = 0xFF;

/// Control tag for idle traffic that keeps the ARQ link from timing out.
const KEEPALIVE_TAG: u8 = 0xFE;

/// Caps one-sided transmit bursts so the peer can send votes.
const MAX_TURN_HOLD: Duration = Duration::from_secs(30);

/// Wait for async follow-up messages before ending a data turn.
const TURN_DRAIN_GRACE: Duration = Duration::from_secs(3);

/// Idle keepalive interval; kept under the modem's ~40s inactivity timeout.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(7);

/// Idle turn hold before grant; long to minimize fragile changeovers.
const IDLE_TURN_GRANT: Duration = Duration::from_secs(60);

/// Silent interval before a non-holder self-grants after a lost grant or dead peer.
/// Must exceed normal keepalive/changeover gaps; test-overridable via env.
fn turn_reclaim_silence() -> Duration {
    std::env::var("BUNKER_TURN_RECLAIM_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_secs(60))
}

/// Listener-only reclaim delay; keeps the caller first and avoids mutual-reclaim livelock.
fn reclaim_role_stagger() -> Duration {
    std::env::var("BUNKER_RECLAIM_STAGGER_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_secs(40))
}

/// Blind-reclaim threshold after which the listener yields to the caller.
const LISTENER_LIVELOCK_RECLAIMS: u32 = 2;

#[derive(Debug, PartialEq, Eq)]
enum ReclaimAction {
    Take,
    Yield,
}

/// Testable reclaim policy: the caller always takes; the listener yields after blind repeats.
#[derive(Default)]
struct ReclaimDecider {
    is_listener: bool,
    blind_reclaims: u32,
    /// Activity timestamp at the previous reclaim; `None` makes the first reclaim blind.
    activity_at_last_reclaim: Option<u64>,
}

impl ReclaimDecider {
    fn new(is_listener: bool) -> Self {
        Self {
            is_listener,
            blind_reclaims: 0,
            activity_at_last_reclaim: None,
        }
    }

    /// Extended for yielding listeners so the caller gets several changeover round-trips.
    fn effective_window(&self, base: Duration, stagger: Duration) -> Duration {
        if self.is_listener && self.blind_reclaims >= LISTENER_LIVELOCK_RECLAIMS {
            base + stagger * 3
        } else {
            base
        }
    }

    /// Updates blind-reclaim state and decides whether to take or yield the turn.
    fn on_reclaim_due(&mut self, activity_now: u64) -> ReclaimAction {
        if !self.is_listener {
            // The caller is the driver of last resort.
            return ReclaimAction::Take;
        }
        // Blind iff no inbound advanced since the previous reclaim.
        let advanced = self
            .activity_at_last_reclaim
            .is_some_and(|prev| activity_now > prev);
        if advanced {
            self.blind_reclaims = 0;
        } else {
            self.blind_reclaims = self.blind_reclaims.saturating_add(1);
        }
        self.activity_at_last_reclaim = Some(activity_now);

        // First blind reclaim recovers a lost grant; repeated blind reclaims indicate livelock.
        if self.blind_reclaims >= LISTENER_LIVELOCK_RECLAIMS {
            self.blind_reclaims = LISTENER_LIVELOCK_RECLAIMS;
            ReclaimAction::Yield
        } else {
            ReclaimAction::Take
        }
    }

    /// Clear livelock backoff once inbound proves the link is flowing.
    fn note_turn_held(&mut self, activity_now: u64) {
        if self.is_listener
            && self
                .activity_at_last_reclaim
                .is_some_and(|prev| activity_now > prev)
        {
            self.blind_reclaims = 0;
        }
    }
}

/// Shared half-duplex turn state between the mux reader and writer.
struct TurnState {
    holds_turn: AtomicBool,
    granted: Notify,
    /// Whether this side started with the turn; used to stagger reclaim.
    starts_with_turn: bool,
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
}

struct Outbound {
    channel: Channel,
    payload: Vec<u8>,
}

/// Owns one PACTOR modem and exposes logical [`MuxChannel`] networks over it.
pub struct PactorMux {
    transport: Arc<dyn PactorTransport>,
    max_read_len: usize,
    outbound_tx: mpsc::Sender<Outbound>,
    outbound_rx: Option<mpsc::Receiver<Outbound>>,
    /// Inbound routing: per-channel sender the reader forwards demuxed bytes to.
    inbound_tx: [Option<mpsc::Sender<Vec<u8>>>; Channel::COUNT],
    /// Per-channel inbound receivers, handed out by [`channel`](Self::channel).
    inbound_rx: [Option<mpsc::Receiver<Vec<u8>>>; Channel::COUNT],
    message_counter: Arc<AtomicU64>,
    /// Last inbound line timestamp; recent activity means [`MuxLiveness`] is up.
    last_activity_ms: Arc<AtomicU64>,
    /// `None` writes freely; `Some` enforces one transmit turn at a time.
    turn: Option<Arc<TurnState>>,
    queued_gauge: Option<Arc<AtomicU64>>,
}

impl PactorMux {
    /// Wrap an already-connected full-duplex transport.
    pub fn new(transport: Arc<dyn PactorTransport>) -> Self {
        Self::build(transport, None)
    }

    /// Wrap a half-duplex PACTOR transport; only one side may start with the turn.
    pub fn new_half_duplex(transport: Arc<dyn PactorTransport>, starts_with_turn: bool) -> Self {
        let turn = Arc::new(TurnState {
            holds_turn: AtomicBool::new(starts_with_turn),
            granted: Notify::new(),
            starts_with_turn,
        });
        Self::build(transport, Some(turn))
    }

    fn build(transport: Arc<dyn PactorTransport>, turn: Option<Arc<TurnState>>) -> Self {
        let (outbound_tx, outbound_rx) = mpsc::channel(CHANNEL_QUEUE_DEPTH);
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
            outbound_tx,
            outbound_rx: Some(outbound_rx),
            inbound_tx,
            inbound_rx,
            // Session-unique ids prevent stale reassembler fragments from a
            // restarted peer merging with new messages.
            message_counter: Arc::new(AtomicU64::new(u64::from(rand::random::<u32>()) << 32)),
            last_activity_ms: Arc::new(AtomicU64::new(now_ms())),
            turn,
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
            outbound_tx: self.outbound_tx.clone(),
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
            outbound_tx: self.outbound_tx.clone(),
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
        let outbound_rx = self.outbound_rx.take().expect("spawn called twice");
        let message_counter = self.message_counter.clone();
        let turn = self.turn.clone();
        let last_activity_ms = self.last_activity_ms.clone();

        // Reader: read, reassemble, strip tag, route to a channel queue.
        let reader_transport = transport.clone();
        let reader_turn = turn.clone();
        let reader_activity = last_activity_ms.clone();
        let reader = tokio::spawn(async move {
            let mut reassembler = Reassembler::new();
            loop {
                let line = match reader_transport.read_data(max_read_len).await {
                    Ok(line) => {
                        // Stamp activity before routing so MuxLiveness sees it.
                        reader_activity.store(now_ms(), Ordering::Relaxed);
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
                if tag == TURN_GRANT_TAG {
                    if let Some(t) = &reader_turn {
                        t.holds_turn.store(true, Ordering::SeqCst);
                        t.granted.notify_one();
                    }
                    continue;
                }
                if tag == KEEPALIVE_TAG {
                    continue; // link-keepalive from the turn-holder; ignore
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

        // Writer: transmit while allowed, keepalive while idle, grant turns sparingly.
        let writer_transport = transport.clone();
        let writer_activity = last_activity_ms.clone();
        let writer = tokio::spawn(async move {
            let mut outbound_rx = outbound_rx;
            let counter = message_counter;

            async fn write_message(
                transport: &Arc<dyn PactorTransport>,
                counter: &AtomicU64,
                tag: u8,
                payload: &[u8],
            ) -> Result<(), ScsPactorError> {
                let mut tagged = Vec::with_capacity(payload.len() + 1);
                tagged.push(tag);
                tagged.extend_from_slice(payload);
                let message_id = counter.fetch_add(1, Ordering::Relaxed);
                for line in fragment_message(message_id, &tagged) {
                    transport.write_data(&line).await?;
                }
                Ok(())
            }

            // Full-duplex mode (simulator / TCP): transmit whenever we have data.
            let Some(turn) = turn else {
                while let Some(item) = outbound_rx.recv().await {
                    if let Err(e) = write_message(
                        &writer_transport,
                        &counter,
                        item.channel as u8,
                        &item.payload,
                    )
                    .await
                    {
                        warn!("[mux:writer] write_data failed: {e}");
                        return;
                    }
                }
                debug!("[mux:writer] outbound queue closed; writer stopping");
                return;
            };

            // Caller reclaims first; listener waits longer to avoid dual reclaim.
            let reclaim_after = if turn.starts_with_turn {
                turn_reclaim_silence()
            } else {
                turn_reclaim_silence() + reclaim_role_stagger()
            };
            // Floor reclaims during sustained silence to at most once per window.
            let mut last_reclaim_ms = 0u64;
            let is_listener = !turn.starts_with_turn;
            let mut decider = ReclaimDecider::new(is_listener);
            loop {
                // Poll for the turn so a lost grant can recover via silence reclaim.
                while !turn.holds_turn.load(Ordering::SeqCst) {
                    let now = now_ms();
                    let activity = writer_activity.load(Ordering::Relaxed);
                    let silence = now.saturating_sub(activity);
                    let since_last_reclaim = now.saturating_sub(last_reclaim_ms);

                    let effective_reclaim_after =
                        decider.effective_window(reclaim_after, reclaim_role_stagger());

                    if silence >= effective_reclaim_after.as_millis() as u64
                        && since_last_reclaim >= effective_reclaim_after.as_millis() as u64
                    {
                        last_reclaim_ms = now;
                        match decider.on_reclaim_due(activity) {
                            ReclaimAction::Yield => {
                                warn!(
                                    "[mux:writer] listener yielding: blind reclaims with no \
                                     inbound — ceding link to caller to break mutual-reclaim \
                                     livelock"
                                );
                                continue;
                            }
                            ReclaimAction::Take => {
                                warn!(
                                    "[mux:writer] link silent {silence}ms with no turn — \
                                     reclaiming (lost turn-grant or peer gone)"
                                );
                                turn.holds_turn.store(true, Ordering::SeqCst);
                                break;
                            }
                        }
                    }
                    // Poll promptly even when tests shrink the reclaim window.
                    let poll = KEEPALIVE_INTERVAL
                        .min(reclaim_after / 4)
                        .max(Duration::from_millis(10));
                    let _ = tokio::time::timeout(poll, turn.granted.notified()).await;
                }

                // Clear livelock backoff if inbound activity resumed.
                decider.note_turn_held(writer_activity.load(Ordering::Relaxed));

                // Keepalive immediately after acquiring an idle turn.
                if outbound_rx.is_empty() {
                    if let Err(e) =
                        write_message(&writer_transport, &counter, KEEPALIVE_TAG, &[]).await
                    {
                        warn!("[mux:writer] post-changeover keepalive failed: {e}");
                        return;
                    }
                }

                // Hold an idle turn with periodic keepalives; real data breaks out immediately.
                let mut first_item = None;
                let idle_until = tokio::time::Instant::now() + IDLE_TURN_GRANT;
                loop {
                    let wait = KEEPALIVE_INTERVAL
                        .min(idle_until.saturating_duration_since(tokio::time::Instant::now()));
                    match tokio::time::timeout(wait, outbound_rx.recv()).await {
                        Ok(Some(item)) => {
                            first_item = Some(item);
                            break;
                        }
                        Ok(None) => return, // outbound closed
                        Err(_) => {
                            if tokio::time::Instant::now() >= idle_until {
                                break; // idle long enough — grant the turn
                            }
                            if let Err(e) =
                                write_message(&writer_transport, &counter, KEEPALIVE_TAG, &[]).await
                            {
                                warn!("[mux:writer] keepalive write failed: {e}");
                                return;
                            }
                        }
                    }
                }
                if let Some(item) = first_item {
                    if let Err(e) = write_message(
                        &writer_transport,
                        &counter,
                        item.channel as u8,
                        &item.payload,
                    )
                    .await
                    {
                        warn!("[mux:writer] write_data failed: {e}");
                        return;
                    }
                }
                // Drain a burst with a short grace so a slot can fit in one turn.
                let turn_deadline = tokio::time::Instant::now() + MAX_TURN_HOLD;
                loop {
                    if tokio::time::Instant::now() >= turn_deadline {
                        break;
                    }
                    match tokio::time::timeout(TURN_DRAIN_GRACE, outbound_rx.recv()).await {
                        Ok(Some(item)) => {
                            if let Err(e) = write_message(
                                &writer_transport,
                                &counter,
                                item.channel as u8,
                                &item.payload,
                            )
                            .await
                            {
                                warn!("[mux:writer] write_data failed: {e}");
                                return;
                            }
                        }
                        Ok(None) => return,
                        // No follow-up: grant the turn so the peer can respond.
                        Err(_) => break,
                    }
                }

                if let Err(e) =
                    write_message(&writer_transport, &counter, TURN_GRANT_TAG, &[]).await
                {
                    warn!("[mux:writer] turn-grant write failed: {e}");
                    return;
                }
                if let Err(e) = writer_transport.changeover().await {
                    warn!("[mux:writer] changeover failed: {e}");
                    return;
                }
                turn.holds_turn.store(false, Ordering::SeqCst);
            }
        });

        // Sample from a separate task so the gauge updates while the writer is parked.
        let gauge_task = self.queued_gauge.take().map(|gauge| {
            let tx = self.outbound_tx.clone();
            tokio::spawn(async move {
                loop {
                    let queued = (tx.max_capacity() - tx.capacity()) as u64;
                    gauge.store(queued, Ordering::Relaxed);
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

/// Inbound-activity window for link liveness.
const LIVENESS_WINDOW: Duration = Duration::from_secs(30);

impl LinkLiveness for MuxLiveness {
    fn is_link_alive(&self) -> bool {
        let last = self.last_activity_ms.load(Ordering::Relaxed);
        now_ms().saturating_sub(last) < LIVENESS_WINDOW.as_millis() as u64
    }
}

/// Drives turn changeover and shutdown for a spawned [`PactorMux`].
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

    /// Hand the transmit turn to the peer (PACTOR ARQ changeover).
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
    outbound_tx: mpsc::Sender<Outbound>,
    inbound_rx: Mutex<mpsc::Receiver<Vec<u8>>>,
    /// Local loopback sender used by [`PactorMux::channel_self_delivering`].
    self_delivery: Option<mpsc::Sender<Vec<u8>>>,
    _msg_types: PhantomData<(S, R)>,
}

/// Send-only handle onto one mux channel, obtained from [`PactorMux::injector`].
#[derive(Clone)]
pub struct MuxInjector {
    channel: Channel,
    outbound_tx: mpsc::Sender<Outbound>,
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
        self.outbound_tx
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
        self.outbound_tx
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
    use scs_pactor::{PactorLinkEvent, ScsPactorError};
    use std::collections::VecDeque;
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

    /// Recording-only transport for writer-path tests.
    struct RecordingTransport {
        written: TokioMutex<Vec<Vec<u8>>>,
        inbound: TokioMutex<VecDeque<Vec<u8>>>,
    }

    impl RecordingTransport {
        fn new() -> Self {
            Self {
                written: TokioMutex::new(Vec::new()),
                inbound: TokioMutex::new(VecDeque::new()),
            }
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
    async fn idle_turn_holder_keepalives_immediately_and_repeatedly() {
        // The turn-holder must keep feeding ARQ during long idle periods.
        let transport = Arc::new(RecordingTransport::new());
        let mut mux = PactorMux::new_half_duplex(transport.clone(), true);
        let _chan: MuxChannel<Vec<u8>, Vec<u8>> = mux.channel(Channel::All2All);
        let _h = mux.spawn();

        tokio::time::sleep(Duration::from_secs(1)).await;
        let after_acquire = transport.written.lock().await.len();
        assert!(
            after_acquire >= 1,
            "expected an immediate keepalive on acquiring the turn, got {after_acquire}"
        );

        tokio::time::sleep(KEEPALIVE_INTERVAL * 5).await;
        let later = transport.written.lock().await.len();
        assert!(
            later >= after_acquire + 4,
            "expected repeated keepalives over the quiet period, got {later} (was {after_acquire})"
        );
    }

    #[tokio::test]
    async fn half_duplex_turns_let_both_sides_send() {
        // Both sides must send under turn discipline without deadlock.
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
            .expect("A should receive B's message before timeout (turn handed over)")
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

    /// Drops the first configured turn-grant frames; all other lines pass through.
    struct GrantDroppingTransport {
        inner: Arc<LoopbackTransport>,
        grants_to_drop: std::sync::atomic::AtomicU32,
    }

    #[async_trait]
    impl PactorTransport for GrantDroppingTransport {
        async fn set_mycall(&self, c: &str) -> Result<(), ScsPactorError> {
            self.inner.set_mycall(c).await
        }
        async fn connect_peer(&self, c: &str) -> Result<(), ScsPactorError> {
            self.inner.connect_peer(c).await
        }
        async fn write_data(&self, data: &[u8]) -> Result<(), ScsPactorError> {
            // Turn grants are single-fragment control messages.
            if let Some((_hdr, chunk)) = crate::pactor_framing::parse_fragment(data) {
                if chunk.first().copied() == Some(TURN_GRANT_TAG)
                    && self
                        .grants_to_drop
                        .load(std::sync::atomic::Ordering::SeqCst)
                        > 0
                {
                    self.grants_to_drop
                        .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                    // Simulate a lost grant: local write succeeds, peer hears nothing.
                    return Ok(());
                }
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

    /// A lost turn grant must not deadlock; the listener reclaims and delivers queued data.
    #[tokio::test]
    async fn lost_turn_grant_recovers_via_reclaim() {
        // SAFETY: single-threaded test shortens reclaim timers via process env.
        unsafe {
            std::env::set_var("BUNKER_TURN_RECLAIM_MS", "300");
            std::env::set_var("BUNKER_RECLAIM_STAGGER_MS", "150");
        }

        let (a_raw, b) = LoopbackTransport::pair();
        let a = Arc::new(GrantDroppingTransport {
            inner: a_raw,
            grants_to_drop: std::sync::atomic::AtomicU32::new(1),
        });

        let mut mux_a = PactorMux::new_half_duplex(a, true);
        let a_chan: MuxChannel<Vec<u8>, Vec<u8>> = mux_a.channel(Channel::All2All);
        let _ha = mux_a.spawn();

        let mut mux_b = PactorMux::new_half_duplex(b, false);
        let b_chan: MuxChannel<Vec<u8>, Vec<u8>> = mux_b.channel(Channel::All2All);
        let _hb = mux_b.spawn();

        let addr = "127.0.0.1:1".parse().unwrap();
        b_chan.send(&b"from-b".to_vec(), addr).await.unwrap();

        let got_at_a = tokio::time::timeout(Duration::from_secs(10), a_chan.receive())
            .await
            .expect("B must reclaim the turn after the lost grant and deliver its message")
            .unwrap();
        assert_eq!(got_at_a, b"from-b");

        unsafe {
            std::env::remove_var("BUNKER_TURN_RECLAIM_MS");
            std::env::remove_var("BUNKER_RECLAIM_STAGGER_MS");
        }
    }

    /// Drops frames while a shared fade flag is set.
    struct FadingTransport {
        inner: Arc<LoopbackTransport>,
        faded: Arc<std::sync::atomic::AtomicBool>,
    }

    #[async_trait]
    impl PactorTransport for FadingTransport {
        async fn set_mycall(&self, c: &str) -> Result<(), ScsPactorError> {
            self.inner.set_mycall(c).await
        }
        async fn connect_peer(&self, c: &str) -> Result<(), ScsPactorError> {
            self.inner.connect_peer(c).await
        }
        async fn write_data(&self, data: &[u8]) -> Result<(), ScsPactorError> {
            if self.faded.load(std::sync::atomic::Ordering::SeqCst) {
                // A fade looks like a successful local write with no peer delivery.
                return Ok(());
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

    /// The caller is the driver of last resort and never yields.
    #[test]
    fn caller_never_yields_on_blind_reclaims() {
        let mut d = ReclaimDecider::new(false);
        for _ in 0..10 {
            assert_eq!(d.on_reclaim_due(1000), ReclaimAction::Take);
        }
        assert_eq!(
            d.effective_window(Duration::from_secs(60), Duration::from_secs(40)),
            Duration::from_secs(60)
        );
    }

    /// A listener yields after blind repeats and extends its reclaim window.
    #[test]
    fn listener_yields_after_blind_reclaims_then_extends_window() {
        assert!(
            LISTENER_LIVELOCK_RECLAIMS >= 1,
            "test assumes at least one Take before yielding"
        );
        let mut d = ReclaimDecider::new(true);
        // Initial blind reclaims still recover a genuinely lost grant.
        for _ in 0..(LISTENER_LIVELOCK_RECLAIMS - 1) {
            assert_eq!(d.on_reclaim_due(1000), ReclaimAction::Take);
        }
        assert_eq!(d.on_reclaim_due(1000), ReclaimAction::Yield);
        let base = Duration::from_secs(60);
        let stagger = Duration::from_secs(40);
        assert!(
            d.effective_window(base, stagger) > base,
            "listener must back off with a longer window once it has yielded"
        );
        assert_eq!(d.on_reclaim_due(1000), ReclaimAction::Yield);
    }

    /// Inbound activity resets listener livelock backoff.
    #[test]
    fn listener_recovers_when_inbound_resumes() {
        let mut d = ReclaimDecider::new(true);
        for _ in 0..=LISTENER_LIVELOCK_RECLAIMS {
            d.on_reclaim_due(1000);
        }
        let base = Duration::from_secs(60);
        let stagger = Duration::from_secs(40);
        assert!(d.effective_window(base, stagger) > base);

        assert_eq!(d.on_reclaim_due(2000), ReclaimAction::Take);
        assert_eq!(
            d.effective_window(base, stagger),
            base,
            "window returns to normal once inbound resumes"
        );

        for _ in 0..=LISTENER_LIVELOCK_RECLAIMS {
            d.on_reclaim_due(2000);
        }
        assert!(d.effective_window(base, stagger) > base);
        d.note_turn_held(3000); // grant arrived, inbound advanced
        assert_eq!(d.effective_window(base, stagger), base);
    }

    /// A two-sided fade must recover without latching into mutual reclaim.
    #[tokio::test]
    async fn mutual_reclaim_fade_recovers_without_livelock() {
        // SAFETY: single-threaded test; stagger stays above the shortened reclaim window.
        unsafe {
            std::env::set_var("BUNKER_TURN_RECLAIM_MS", "200");
            std::env::set_var("BUNKER_RECLAIM_STAGGER_MS", "300");
        }

        let (a_raw, b_raw) = LoopbackTransport::pair();
        let a_faded = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let b_faded = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let a = Arc::new(FadingTransport {
            inner: a_raw,
            faded: a_faded.clone(),
        });
        let b = Arc::new(FadingTransport {
            inner: b_raw,
            faded: b_faded.clone(),
        });

        let mut mux_a = PactorMux::new_half_duplex(a, true);
        let a_chan: MuxChannel<Vec<u8>, Vec<u8>> = mux_a.channel(Channel::All2All);
        let _ha = mux_a.spawn();

        let mut mux_b = PactorMux::new_half_duplex(b, false);
        let b_chan: MuxChannel<Vec<u8>, Vec<u8>> = mux_b.channel(Channel::All2All);
        let _hb = mux_b.spawn();

        let addr = "127.0.0.1:1".parse().unwrap();

        // Silence both directions long enough to trigger repeated reclaim decisions.
        a_faded.store(true, std::sync::atomic::Ordering::SeqCst);
        b_faded.store(true, std::sync::atomic::Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(1600)).await;

        a_faded.store(false, std::sync::atomic::Ordering::SeqCst);
        b_faded.store(false, std::sync::atomic::Ordering::SeqCst);

        b_chan.send(&b"after-fade".to_vec(), addr).await.unwrap();
        let got = tokio::time::timeout(Duration::from_secs(10), a_chan.receive())
            .await
            .expect("link must recover from a two-sided fade, not latch into mutual reclaim")
            .unwrap();
        assert_eq!(got, b"after-fade");

        a_chan.send(&b"a-to-b".to_vec(), addr).await.unwrap();
        let got_b = tokio::time::timeout(Duration::from_secs(10), b_chan.receive())
            .await
            .expect("caller→listener must also flow after recovery")
            .unwrap();
        assert_eq!(got_b, b"a-to-b");

        unsafe {
            std::env::remove_var("BUNKER_TURN_RECLAIM_MS");
            std::env::remove_var("BUNKER_RECLAIM_STAGGER_MS");
        }
    }
}
