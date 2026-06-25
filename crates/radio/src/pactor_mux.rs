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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bunkerglow::network::Network;
use log::{debug, warn};
use scs_pactor::{PactorTransport, ScsPactorError};
use tokio::sync::{mpsc, Mutex};
use wincode::{SchemaRead, SchemaWrite};

use crate::pactor_framing::{fragment_message, Reassembler};

/// Maximum number of bytes read for a single inbound message line.
const DEFAULT_MAX_READ_LEN: usize = 8192;

/// Bound on each channel's inbound queue (messages buffered before a slow
/// consumer applies backpressure to the single reader task).
const CHANNEL_QUEUE_DEPTH: usize = 1024;

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
    /// Transactions (`Transaction`) — unused for empty-block consensus.
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
}

impl PactorMux {
    /// Wrap an already-connected PACTOR transport. No tasks run until
    /// [`spawn`](Self::spawn) is called.
    pub fn new(transport: Arc<dyn PactorTransport>) -> Self {
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

        // Reader: read_data → reassemble → strip tag → route to channel queue.
        let reader_transport = transport.clone();
        let reader = tokio::spawn(async move {
            let mut reassembler = Reassembler::new();
            loop {
                let line = match reader_transport.read_data(max_read_len).await {
                    Ok(line) => line,
                    // A read timeout just means the link was idle (the peer holds
                    // the transmit turn, or there is nothing to send). Keep
                    // waiting; a long-lived node must not tear down its inbound
                    // queues on idle.
                    Err(ScsPactorError::Timeout) => continue,
                    Err(e) => {
                        debug!("[mux:reader] read_data ended: {e}");
                        break;
                    }
                };
                let Some(message) = reassembler.push_line(&line) else {
                    continue; // mid-message fragment or unparseable line
                };
                let Some((&tag, payload)) = message.split_first() else {
                    warn!("[mux:reader] dropping empty multiplexed message");
                    continue;
                };
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

        // Writer: drain outbound queue → tag + fragment → write_data.
        let writer_transport = transport.clone();
        let writer = tokio::spawn(async move {
            let mut outbound_rx = outbound_rx;
            while let Some(item) = outbound_rx.recv().await {
                // Prepend the 1-byte channel tag, then fragment tag+payload as
                // one unit so >MTU messages still split correctly.
                let mut tagged = Vec::with_capacity(item.payload.len() + 1);
                tagged.push(item.channel as u8);
                tagged.extend_from_slice(&item.payload);
                let message_id = message_counter.fetch_add(1, Ordering::Relaxed);
                for line in fragment_message(message_id, &tagged) {
                    if let Err(e) = writer_transport.write_data(&line).await {
                        warn!("[mux:writer] write_data failed: {e}");
                        return;
                    }
                }
            }
            debug!("[mux:writer] outbound queue closed; writer stopping");
        });

        PactorMuxHandle {
            transport,
            reader,
            writer,
        }
    }
}

/// Drives turn changeover and shutdown for a spawned [`PactorMux`].
pub struct PactorMuxHandle {
    transport: Arc<dyn PactorTransport>,
    reader: tokio::task::JoinHandle<()>,
    writer: tokio::task::JoinHandle<()>,
}

impl PactorMuxHandle {
    /// Hand the transmit turn to the peer (PACTOR ARQ changeover).
    pub async fn changeover(&self) -> std::io::Result<()> {
        self.transport
            .changeover()
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))
    }

    /// Abort the reader and writer tasks.
    pub fn shutdown(&self) {
        self.reader.abort();
        self.writer.abort();
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
