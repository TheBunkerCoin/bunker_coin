//! Channel multiplexer over a single PACTOR modem link.
//!
//! A full Alpenglow node wants five independent, concurrently-usable networks
//! (all2all, disseminator, two repair channels, txs), but a PACTOR link is one
//! half-duplex serial pipe. They cannot each own a [`PactorNetwork`] over the
//! same modem — they would all read/write the same line and interleave garbage.
//!
//! [`PactorMux`] owns the modem and multiplexes those logical networks over it:
//! every outbound message is tagged with a 1-byte [`Channel`] id; one reader
//! task demuxes inbound messages back to the right typed queue. Each logical
//! network is a [`MuxChannel`], which implements
//! [`bunkerglow::network::Network`], so `TrivialAll2All` / `Rotor` / the repair
//! and txs handlers wrap it unchanged.
//!
//! ```text
//!  all2all ─send(C)─►┐                              ┌─►all2all recv
//!  rotor   ─send(C)─►│  PactorMux (owns the modem)  │─►rotor   recv
//!  repair  ─send(C)─►│  tag = 1-byte channel id     │─►repair  recv
//!                    │  writer task → write_data    │
//!                    │  reader task ← read_data     │
//!                    └──────────────────────────────┘
//! ```
//!
//! ## Half-duplex turns
//!
//! All channels funnel into one outbound queue. This first version does **not**
//! auto-changeover: the writer transmits whatever is queued, and the turn policy
//! (when to hand the link to the peer) is driven externally via
//! [`PactorMux::changeover`]. Turn scheduling is the part most likely to need
//! on-air tuning, so it is kept out of the mux for now.
//!
//! [`PactorNetwork`]: crate::pactor_network::PactorNetwork

use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use bunkerglow::consensus::LinkLiveness;

/// Process-start reference instant for the millis-based activity clock.
static START: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Millis since process start (monotonic; used for the activity timestamp).
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

/// Maximum number of bytes read for a single inbound message line.
const DEFAULT_MAX_READ_LEN: usize = 8192;

/// Bound on each channel's inbound queue (messages buffered before a slow
/// consumer applies backpressure to the single reader task).
const CHANNEL_QUEUE_DEPTH: usize = 1024;

/// Reserved tag for the turn-grant control line (distinct from the data
/// [`Channel`] tags 0..=4). When half-duplex turn discipline is enabled, the
/// turn-holder sends a line with this tag to hand the transmit turn to the peer.
const TURN_GRANT_TAG: u8 = 0xFF;

/// Reserved tag for a keepalive line. The turn-holder sends these during quiet
/// periods so the PACTOR ARQ link keeps seeing traffic and does not time out to
/// STBY. The reader ignores them.
const KEEPALIVE_TAG: u8 = 0xFE;

/// Maximum time a side holds the transmit turn before changing over, even if it
/// still has more to send. Bounds one-directional starvation so the peer always
/// gets a turn to send its votes. Tuned conservatively for HF; expect on-air
/// adjustment.
const MAX_TURN_HOLD: Duration = Duration::from_secs(30);

/// While holding the turn, how long to wait for a follow-up message after the
/// queue momentarily empties before granting the turn to the peer. Lets a slot's
/// asynchronously-produced burst (shreds, then cert) go out in one turn instead
/// of fragmenting across changeovers. Short, so we don't waste the turn idling.
const TURN_DRAIN_GRACE: Duration = Duration::from_secs(3);

/// How often the turn-holder sends a keepalive line while idle, to keep the
/// PACTOR ARQ link from timing out to STBY during quiet periods. Must be
/// comfortably shorter than the modem's inactivity timeout (~40s observed).
///
/// Kept short and aggressive: the turn-holder is the ONLY side that can transmit
/// under ARQ half-duplex (the IRS/non-holder cannot send without forcing a
/// changeover), so on-air link liveness depends entirely on the holder feeding
/// the link. On the slow reverse path a single keepalive can take 15-20s to
/// clear, so a short nominal interval means the next keepalive is already queued
/// the moment the previous one drains — keeping the ARQ link continuously busy.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(7);

/// How long a turn-holder keeps the turn while idle — sending keepalives every
/// [`KEEPALIVE_INTERVAL`] throughout — before granting it to the peer so the peer
/// can send anything queued.
///
/// Deliberately long. Each changeover opens a fragile quiet window (the new
/// holder must receive the grant, then start keepaliving, all over a slow
/// reverse path) — that handoff gap is exactly what dropped the link to STBY
/// after a slot. Holding the turn longer means FEWER changeovers per quiet
/// period and lets the holder push many keepalives through the slow path before
/// handing over, so the link stays continuously fed by one side rather than
/// repeatedly risking the handoff. The turn still flips IMMEDIATELY when real
/// data arrives (votes are enqueued, not idle-waited), so voting stays
/// responsive despite the long idle hold.
const IDLE_TURN_GRANT: Duration = Duration::from_secs(60);

/// Shared half-duplex turn state between the mux reader and writer.
///
/// `holds_turn` is `true` while this side may transmit. The writer waits on
/// `granted` until it holds the turn; the reader sets `holds_turn` and notifies
/// `granted` when it sees a turn-grant line from the peer.
struct TurnState {
    holds_turn: AtomicBool,
    granted: Notify,
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
    /// Transactions (`Transaction`) — client-submitted txs the leader packs
    /// into blocks. Fed by a [`MuxInjector`] (from the RPC / gateway) and
    /// consumed by the block producer's `txs_receiver`.
    Txs = 4,
}

impl Channel {
    /// Number of distinct channels (used to size the inbound routing table).
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
}

/// An outbound message handed from a [`MuxChannel`] to the mux writer: the
/// channel tag plus the already-serialized payload bytes.
struct Outbound {
    channel: Channel,
    payload: Vec<u8>,
}

/// Channel multiplexer owning one PACTOR modem link.
///
/// Construct with [`PactorMux::new`], take one [`MuxChannel`] per logical
/// network with [`PactorMux::channel`], then [`PactorMux::spawn`] the reader and
/// writer tasks. The returned [`PactorMuxHandle`] drives turn changeover and
/// shutdown.
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
    /// Millis-since-process-start of the last inbound line (data, keepalive, or
    /// turn-grant). Drives [`MuxLiveness`]: recent activity ⇒ the peer/link is up.
    last_activity_ms: Arc<AtomicU64>,
    /// Half-duplex turn discipline. `None` = full-duplex (write freely — for the
    /// simulator / full-duplex transports). `Some` = enforce one transmit turn at
    /// a time with explicit changeover (for real PACTOR).
    turn: Option<Arc<TurnState>>,
}

impl PactorMux {
    /// Wrap an already-connected **full-duplex** transport (simulator, TCP).
    ///
    /// No turn discipline: the writer transmits whenever it has data. No tasks
    /// run until [`spawn`](Self::spawn) is called.
    pub fn new(transport: Arc<dyn PactorTransport>) -> Self {
        Self::build(transport, None)
    }

    /// Wrap an already-connected **half-duplex** PACTOR transport with turn
    /// discipline.
    ///
    /// Only the turn-holder transmits; when it drains its queue (or hits
    /// [`MAX_TURN_HOLD`]) it sends a turn-grant line and `changeover()`s to hand
    /// the link to the peer. `starts_with_turn` must be `true` on exactly one side
    /// — the PACTOR caller (master/ISS) — and `false` on the listener (slave), so
    /// the two sides agree on who transmits first.
    pub fn new_half_duplex(transport: Arc<dyn PactorTransport>, starts_with_turn: bool) -> Self {
        let turn = Arc::new(TurnState {
            holds_turn: AtomicBool::new(starts_with_turn),
            granted: Notify::new(),
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
            message_counter: Arc::new(AtomicU64::new(0)),
            last_activity_ms: Arc::new(AtomicU64::new(now_ms())),
            turn,
        }
    }

    /// Take the [`MuxChannel`] for one logical channel.
    ///
    /// `S` is the type sent on this channel, `R` the type received. Panics if the
    /// same channel is taken twice (each channel has exactly one inbound queue).
    pub fn channel<S, R>(&mut self, channel: Channel) -> MuxChannel<S, R> {
        self.channel_inner(channel, false)
    }

    /// Like [`channel`](Self::channel) but with **self-delivery**: every message
    /// sent on this channel is also delivered to this node's own inbound queue
    /// for the same channel.
    ///
    /// Consensus broadcasts (all2all votes/certs) target *all* validators —
    /// including the sender itself. Over a real socket the self-addressed copy
    /// loops back through the OS; over a single PACTOR link there is only the
    /// peer, so without self-delivery a node would never count its own vote and
    /// (with 2 equal-stake validators) could never reach the >60% finalization
    /// quorum. Self-delivery replicates the loopback the network layer assumes.
    pub fn channel_self_delivering<S, R>(&mut self, channel: Channel) -> MuxChannel<S, R> {
        self.channel_inner(channel, true)
    }

    /// Take a **send-only** injector for one channel: a lightweight handle that
    /// can push messages onto the channel without owning its inbound queue.
    ///
    /// The channel's own [`MuxChannel`] (owned by, e.g., the block producer)
    /// consumes the inbound queue via `receive`; an injector cannot be a second
    /// `MuxChannel` because there is exactly one inbound receiver. This handle
    /// clones only the *sending* halves — the shared outbound queue (→ over-air
    /// to the peer) and the channel's inbound sender (→ self-delivery to the
    /// local consumer) — so a message injected on the leader reaches both its own
    /// block producer and the peer's. Used to feed client transactions from the
    /// RPC server (or LoRa gateway) into [`Channel::Txs`].
    ///
    /// Call before [`spawn`](Self::spawn); cheap to clone the returned handle.
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

    /// Spawn the reader and writer tasks. Consumes the mux; returns a handle for
    /// turn changeover and shutdown. Call after all [`channel`](Self::channel)s
    /// have been taken.
    pub fn spawn(mut self) -> PactorMuxHandle {
        let transport = self.transport.clone();
        let max_read_len = self.max_read_len;
        let inbound_tx = self.inbound_tx.clone();
        let outbound_rx = self.outbound_rx.take().expect("spawn called twice");
        let message_counter = self.message_counter.clone();
        let turn = self.turn.clone();
        let last_activity_ms = self.last_activity_ms.clone();

        // Reader: read_data → reassemble → strip tag → route to channel queue.
        // A turn-grant line (tag 0xFF) hands the transmit turn to us.
        let reader_transport = transport.clone();
        let reader_turn = turn.clone();
        let reader_activity = last_activity_ms.clone();
        let reader = tokio::spawn(async move {
            let mut reassembler = Reassembler::new();
            loop {
                let line = match reader_transport.read_data(max_read_len).await {
                    Ok(line) => {
                        // Any inbound bytes — data, keepalive, or turn-grant — are
                        // evidence the peer/link is alive. Stamp the activity clock
                        // before routing so MuxLiveness reflects it immediately.
                        reader_activity.store(now_ms(), Ordering::Relaxed);
                        line
                    }
                    // A read timeout just means the link was idle (the peer holds
                    // the transmit turn, or there is nothing to send). Keep
                    // waiting; a long-lived node must not tear down its inbound
                    // queues on idle.
                    Err(ScsPactorError::Timeout) => continue,
                    Err(e) => {
                        // Link error (e.g. mid-transfer disconnect / STBY). Do NOT
                        // drop the inbound senders by exiting: that would make the
                        // consensus tasks' receive().unwrap() panic. Keep the
                        // reader (and its senders) alive so consensus stalls
                        // cleanly until the run ends; back off to avoid a hot loop.
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
                    // Peer handed us the transmit turn.
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

        // Writer: when it holds the turn (or always, in full-duplex mode), drain
        // the outbound queue and transmit. Under turn discipline it keeps the
        // link alive with keepalives during quiet periods and only hands the turn
        // to the peer after sending data, or after a long fully-idle stretch, so
        // the expensive changeover is used sparingly and the ARQ link does not
        // time out to STBY.
        let writer_transport = transport.clone();
        let writer = tokio::spawn(async move {
            let mut outbound_rx = outbound_rx;
            let counter = message_counter;

            // Send one logical message as tagged, fragmented lines.
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

            // Full-duplex mode: no turn discipline, transmit whenever we have
            // data. (Simulator / TCP — both sides write freely.)
            let Some(turn) = turn else {
                while let Some(item) = outbound_rx.recv().await {
                    if let Err(e) =
                        write_message(&writer_transport, &counter, item.channel as u8, &item.payload)
                            .await
                    {
                        warn!("[mux:writer] write_data failed: {e}");
                        return;
                    }
                }
                debug!("[mux:writer] outbound queue closed; writer stopping");
                return;
            };

            // Half-duplex mode. Hold the turn only briefly: drain whatever is
            // queued, then grant to the peer so voting stays responsive. Before
            // an idle grant, send a keepalive so the silent changeover window does
            // not let the ARQ link time out to STBY.
            loop {
                // Wait until we hold the turn.
                while !turn.holds_turn.load(Ordering::SeqCst) {
                    turn.granted.notified().await;
                }

                // Send a keepalive the INSTANT we acquire the turn, before waiting.
                // After a changeover the link has just gone quiet (the old holder
                // stopped, the grant + this changeover took seconds over a slow
                // path); feeding the link immediately — rather than idling up to
                // KEEPALIVE_INTERVAL first — closes that post-changeover gap that
                // otherwise lets the ARQ link time out to STBY. Skip it if data is
                // already queued (the drain loop below will transmit right away).
                if outbound_rx.is_empty() {
                    if let Err(e) =
                        write_message(&writer_transport, &counter, KEEPALIVE_TAG, &[]).await
                    {
                        warn!("[mux:writer] post-changeover keepalive failed: {e}");
                        return;
                    }
                }

                // Wait for the first item to send. While idle, send keepalives
                // every KEEPALIVE_INTERVAL to keep the ARQ link alive, and KEEP
                // HOLDING the turn for up to IDLE_TURN_GRANT before granting it to
                // the peer. This is the key fix: a turn-holder must keep emitting
                // keepalives throughout a long quiet period (e.g. the multi-minute
                // wait for the next block) — not send one keepalive and immediately
                // grant the turn away, after which neither side transmits and the
                // link drops to STBY.
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
                            // Still idle: keepalive and keep holding the turn.
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
                // Keep draining while we hold the turn. Consensus enqueues a
                // slot's messages asynchronously (shreds, then the cert moments
                // later), so don't grant the instant the queue momentarily empties
                // — wait a short grace period for follow-up messages so a whole
                // slot goes out in ONE transmit turn instead of fragmenting across
                // many expensive changeovers. Bounded by MAX_TURN_HOLD, measured
                // from when real data started (so a preceding long idle keepalive
                // hold does not eat into the data-burst window).
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
                        // No follow-up within the grace window: the slot's burst is
                        // done — grant the turn so the peer can respond/vote.
                        Err(_) => break,
                    }
                }

                // Hand the turn to the peer.
                if let Err(e) = write_message(&writer_transport, &counter, TURN_GRANT_TAG, &[]).await
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

        PactorMuxHandle {
            transport,
            reader,
            writer,
            inbound_tx: self.inbound_tx.clone(),
            last_activity_ms,
        }
    }
}

/// Keepalive-driven [`LinkLiveness`] for the consensus crashed-leader timeout.
///
/// The link is "alive" if the mux reader saw any inbound line (data, keepalive,
/// or turn-grant) within [`LIVENESS_WINDOW`]. On a half-duplex PACTOR link the
/// turn-holder emits keepalives every `KEEPALIVE_INTERVAL`, so a healthy link
/// produces inbound activity well within the window even during quiet periods.
pub struct MuxLiveness {
    last_activity_ms: Arc<AtomicU64>,
}

/// How recently inbound activity must have been seen for the link to count as
/// alive. Comfortably larger than `KEEPALIVE_INTERVAL` (so an in-flight keepalive
/// gap does not flap it) but small enough that a genuinely dead link is detected
/// within a couple of keepalive periods.
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
    /// Inbound senders, retained so [`shutdown`](Self::shutdown) can keep the
    /// channels open after aborting the reader (see there).
    inbound_tx: [Option<mpsc::Sender<Vec<u8>>>; Channel::COUNT],
    /// Shared inbound-activity clock, surfaced via [`liveness`](Self::liveness).
    last_activity_ms: Arc<AtomicU64>,
}

impl PactorMuxHandle {
    /// A [`LinkLiveness`] backed by this mux's inbound-activity clock, to inject
    /// into the consensus node (`Alpenglow::set_link_liveness`) so a slow-but-
    /// alive link pauses the crashed-leader timeout instead of skipping.
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

    /// Shut the mux down for a clean teardown (e.g. before a reconnect).
    ///
    /// Aborts the reader and writer tasks and closes the inbound channels (by
    /// dropping the retained senders). This is deliberate: a node being discarded
    /// has detached repair tasks looping on `receive()` with no cancellation —
    /// they only terminate (and release their `Blockstore`/`Pool` handles, which
    /// hold the RocksDB locks the *next* session must re-open) once their queue
    /// closes. Closing the queues lets them unwind and free those DBs. The
    /// resulting `receive().unwrap()` is logged as a panic in those detached
    /// tasks but is non-fatal to the process.
    pub fn shutdown(&mut self) {
        self.reader.abort();
        self.writer.abort();
        // Drop the retained inbound senders so the channels close and orphaned
        // receivers terminate (freeing the RocksDB handles for the next session).
        for slot in &mut self.inbound_tx {
            *slot = None;
        }
    }
}

/// One logical network over the shared PACTOR link. Implements
/// [`bunkerglow::network::Network`] so consensus wrappers compose unchanged.
pub struct MuxChannel<S, R> {
    channel: Channel,
    outbound_tx: mpsc::Sender<Outbound>,
    inbound_rx: Mutex<mpsc::Receiver<Vec<u8>>>,
    /// If set, a copy of every sent message is also delivered to this node's own
    /// inbound queue (see [`PactorMux::channel_self_delivering`]).
    self_delivery: Option<mpsc::Sender<Vec<u8>>>,
    _msg_types: PhantomData<(S, R)>,
}

/// A send-only handle onto one mux channel, obtained from
/// [`PactorMux::injector`]. Serializes and enqueues a message just like
/// [`MuxChannel::enqueue`] — onto the shared outbound queue (over-air to the
/// peer) and, if the channel self-delivers, onto its own inbound queue (to the
/// local consumer) — but owns no inbound receiver, so it can coexist with the
/// channel's [`MuxChannel`] and be cloned freely across injector tasks.
#[derive(Clone)]
pub struct MuxInjector {
    channel: Channel,
    outbound_tx: mpsc::Sender<Outbound>,
    /// Sender onto the channel's inbound queue, so an injected message is also
    /// delivered to this node's own consumer (the local block producer packs
    /// transactions the RPC injects here). `None` only if the channel was never
    /// set up (should not happen for a live channel).
    self_delivery: Option<mpsc::Sender<Vec<u8>>>,
}

impl MuxInjector {
    /// Serialize and inject `message` onto this channel. Delivers to the local
    /// consumer (self-delivery) and enqueues for over-air transmission to the
    /// peer. Best-effort on self-delivery (a full inbound queue does not block
    /// the over-air send); errors only if the mux outbound queue is closed.
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
                channel: self.channel,
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
        // Self-delivery: a broadcast targets all validators including this one, so
        // loop a copy back to our own inbound queue (the network layer normally
        // gets this for free via socket loopback). Best-effort; a full queue must
        // not block the over-the-air send.
        if let Some(self_tx) = &self.self_delivery {
            let _ = self_tx.try_send(payload.clone());
        }
        self.outbound_tx
            .send(Outbound {
                channel: self.channel,
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
                    warn!(
                        "MuxChannel({:?}) deserialize failed ({err:?}); waiting for next message",
                        self.channel
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

    /// A loopback transport pair: what station A writes, station B reads, and
    /// vice versa. Lets two muxes talk over a simulated link in-process.
    struct LoopbackTransport {
        /// Lines written by this station (delivered to the peer's read side).
        out_tx: mpsc::UnboundedSender<Vec<u8>>,
        /// Lines written by the peer (this station's read side).
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
                1 + data.len() * 2 + 1 <= crate::pactor_framing::RADIO_MTU,
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

    /// A recording transport (no peer) for testing the write/fragment path.
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
            // Block forever once drained so the reader task just parks.
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
        // Reproduce the consensus shape: take all five channels, spawn the mux,
        // then spawn a long-lived receiver per channel (as the repair/all2all/
        // shred/txs handlers do). None must see a closed queue while idle.
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
        // Mimics consensus: a long-lived task loops receive() on a channel that
        // gets no traffic. It must park (pend), never see a closed queue.
        let transport = Arc::new(RecordingTransport::new());
        let mut mux = PactorMux::new(transport);
        let repair: MuxChannel<Vec<u8>, Vec<u8>> = mux.channel(Channel::Repair);
        let _h = mux.spawn();

        let probe = tokio::spawn(async move { repair.receive().await });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!probe.is_finished(), "idle receive returned early (queue closed?)");
        probe.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn idle_turn_holder_keepalives_immediately_and_repeatedly() {
        // The turn-holder must feed the ARQ link during a quiet period — both the
        // instant it acquires the turn (closing the post-changeover gap) and then
        // periodically — so the modem never sees the multi-minute inter-block
        // silence that dropped the on-air link to STBY. RecordingTransport's
        // read_data never returns a grant, so this side keeps the turn and idles.
        let transport = Arc::new(RecordingTransport::new());
        let mut mux = PactorMux::new_half_duplex(transport.clone(), true);
        let _chan: MuxChannel<Vec<u8>, Vec<u8>> = mux.channel(Channel::All2All);
        let _h = mux.spawn();

        // Almost immediately (well under one KEEPALIVE_INTERVAL) there must be a
        // keepalive line: the on-acquire keepalive fired without waiting.
        tokio::time::sleep(Duration::from_secs(1)).await;
        let after_acquire = transport.written.lock().await.len();
        assert!(
            after_acquire >= 1,
            "expected an immediate keepalive on acquiring the turn, got {after_acquire}"
        );

        // Over a long quiet stretch the holder must keep emitting keepalives every
        // KEEPALIVE_INTERVAL (not fall silent after one), so the link stays busy.
        tokio::time::sleep(KEEPALIVE_INTERVAL * 5).await;
        let later = transport.written.lock().await.len();
        assert!(
            later >= after_acquire + 4,
            "expected repeated keepalives over the quiet period, got {later} (was {after_acquire})"
        );
    }

    #[tokio::test]
    async fn half_duplex_turns_let_both_sides_send() {
        // Caller (A) starts with the turn, listener (B) without. Each side sends
        // a message; both must arrive — proving the turn ping-pongs (A drains +
        // grants to B, B sends + grants back) without deadlock. The loopback's
        // changeover is a no-op; the turn-grant line carries the handoff.
        let (a, b) = LoopbackTransport::pair();

        let mut mux_a = PactorMux::new_half_duplex(a, true);
        let a_chan: MuxChannel<Vec<u8>, Vec<u8>> = mux_a.channel(Channel::All2All);
        let _ha = mux_a.spawn();

        let mut mux_b = PactorMux::new_half_duplex(b, false);
        let b_chan: MuxChannel<Vec<u8>, Vec<u8>> = mux_b.channel(Channel::All2All);
        let _hb = mux_b.spawn();

        let addr = "127.0.0.1:1".parse().unwrap();
        // B queues its message immediately, even though it does not yet hold the
        // turn; it must go out once A grants the turn to B.
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
        // A sends a shred and an all2all message; B must receive each on the
        // matching channel, never crossed.
        a_shred.send(&b"shred-payload".to_vec(), addr).await.unwrap();
        a_all2all.send(&b"vote-payload".to_vec(), addr).await.unwrap();

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

        // Let the writer task drain the queue.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let lines = transport.written.lock().await.clone();
        assert_eq!(lines.len(), 1, "small message is one line");
        // First framed line, parsed, must carry the Disseminator tag (1).
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
}
