//! `bunkerglow::network::Network` implementation backed by a PACTOR modem link.
//!
//! This lets the existing Alpenglow consensus simulation — which is generic over
//! the [`Network`](bunkerglow::network::Network) trait — run over two real SCS
//! PACTOR modems instead of the in-process [`SimulatedNetwork`]. It mirrors
//! [`UdpNetwork`](bunkerglow::network::udp) but serializes each typed message and
//! ships it over the connected ARQ link via [`PactorTransport`].
//!
//! PACTOR is a connected point-to-point link, so the [`SocketAddr`] routing
//! arguments are ignored: every send goes to the single connected peer. This is
//! the first increment toward running the full simulation over the modems; it
//! exercises one typed channel end to end.
//!
//! ## Fragmentation
//!
//! The transport's [`write_data`](PactorTransport::write_data) carries one
//! message as a single hex line that must fit the 300-byte radio MTU. Real
//! consensus payloads (Certs with aggregated BLS sigs, Shreds, blocks) exceed
//! that. `PactorNetwork` therefore fragments a serialized message across several
//! `write_data` lines using a small [`FragmentHeader`] (mirroring the simulated
//! [`RadioNetworkCore`](crate::network_core) fragmentation) and reassembles them
//! on [`receive`](Network::receive) by looping [`read_data`](PactorTransport::read_data).
//! A message that fits one line is sent as a single `total_fragments == 1`
//! fragment, so the common case (a vote) is unchanged on the wire shape.

use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bunkerglow::network::Network;
use log::warn;
use scs_pactor::PactorTransport;
use tokio::sync::Mutex;
use wincode::{SchemaRead, SchemaWrite};

use crate::pactor_framing::{fragment_message, Reassembler};

/// Maximum number of bytes read for a single inbound message.
const DEFAULT_MAX_READ_LEN: usize = 8192;

/// Network abstraction over a connected PACTOR modem link.
///
/// `S` is the message type sent, `R` the type received. The transport must
/// already be connected (link established) before sending or receiving.
pub struct PactorNetwork<S, R> {
    transport: Arc<dyn PactorTransport>,
    max_read_len: usize,
    message_counter: AtomicU64,
    /// Partially-received messages. Reassembly is over a single point-to-point
    /// link, so the peer's id is implicit.
    reassembly: Mutex<Reassembler>,
    _msg_types: PhantomData<(S, R)>,
}

impl<S, R> PactorNetwork<S, R> {
    /// Wraps an already-connected PACTOR transport.
    pub fn new(transport: Arc<dyn PactorTransport>) -> Self {
        Self {
            transport,
            max_read_len: DEFAULT_MAX_READ_LEN,
            // Session-unique id namespace (see PactorMux::build): random high
            // 32 bits, counting low 32 bits — stale-partial merges across
            // session restarts become improbable.
            message_counter: AtomicU64::new(u64::from(rand::random::<u32>()) << 32),
            reassembly: Mutex::new(Reassembler::new()),
            _msg_types: PhantomData,
        }
    }

    /// Override the maximum inbound message size.
    pub fn with_max_read_len(mut self, max_read_len: usize) -> Self {
        self.max_read_len = max_read_len;
        self
    }

    /// Fragment a serialized message across one or more `write_data` lines so
    /// each line stays within the radio MTU. A message that fits one line is
    /// sent as a single `total_fragments == 1` fragment.
    async fn send_serialized(&self, bytes: &[u8]) -> std::io::Result<()> {
        let message_id = self.message_counter.fetch_add(1, Ordering::Relaxed);
        for line in fragment_message(message_id, bytes) {
            self.transport
                .write_data(&line)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;
        }
        Ok(())
    }

    /// Read fragment lines until a complete message is reassembled, then return
    /// its raw (still-serialized) bytes.
    async fn receive_serialized(&self) -> std::io::Result<Vec<u8>> {
        loop {
            let line = self
                .transport
                .read_data(self.max_read_len)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            let mut reassembly = self.reassembly.lock().await;
            match reassembly.push_line(&line) {
                Some(message) => return Ok(message),
                None => {
                    // Either a mid-message fragment, or an unparseable line.
                    // Keep reading; reassembly tolerates both.
                    continue;
                }
            }
        }
    }
}

impl<S, R> PactorNetwork<S, R>
where
    S: SchemaWrite<Src = S> + Send + Sync,
{
    /// Send several messages within a single transmit turn (no changeover
    /// between them). Each message is fragmented across as many framed lines as
    /// the MTU requires, but the link stays in the sending direction throughout
    /// — amortizing the expensive half-duplex ARQ changeover across the batch.
    ///
    /// The caller is responsible for the changeover before/after the batch.
    pub async fn send_batch(&self, messages: &[S]) -> std::io::Result<()> {
        for msg in messages {
            let bytes = wincode::serialize(msg)
                .map_err(|e| std::io::Error::other(format!("serialize failed: {e:?}")))?;
            self.send_serialized(&bytes).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl<S, R> Network for PactorNetwork<S, R>
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
        let bytes = wincode::serialize(message)
            .map_err(|e| std::io::Error::other(format!("serialize failed: {e:?}")))?;
        // One physical peer on the link: send once regardless of how many
        // logical addresses were requested, but only if at least one was.
        if addrs.into_iter().next().is_some() {
            self.send_serialized(&bytes).await?;
        }
        Ok(())
    }

    async fn send(&self, message: &S, _addr: SocketAddr) -> std::io::Result<()> {
        let bytes = wincode::serialize(message)
            .map_err(|e| std::io::Error::other(format!("serialize failed: {e:?}")))?;
        self.send_serialized(&bytes).await
    }

    async fn receive(&self) -> std::io::Result<R> {
        loop {
            let bytes = self.receive_serialized().await?;
            match wincode::deserialize(&bytes) {
                Ok(msg) => return Ok(msg),
                Err(err) => {
                    warn!("PactorNetwork deserialize failed ({err:?}); waiting for next message");
                    continue;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pactor_framing::{parse_fragment, RADIO_MTU};
    use async_trait::async_trait;
    use scs_pactor::ScsPactorError;
    use std::collections::VecDeque;
    use std::time::Duration;

    /// A fake transport that records written lines and replays queued lines back
    /// for `read_data`, so fragmentation/reassembly can be tested without radio.
    struct FakeTransport {
        written: Mutex<Vec<Vec<u8>>>,
        inbound: Mutex<VecDeque<Vec<u8>>>,
    }

    impl FakeTransport {
        fn new() -> Self {
            Self {
                written: Mutex::new(Vec::new()),
                inbound: Mutex::new(VecDeque::new()),
            }
        }

        async fn take_written(&self) -> Vec<Vec<u8>> {
            std::mem::take(&mut *self.written.lock().await)
        }

        async fn queue_inbound(&self, lines: Vec<Vec<u8>>) {
            self.inbound.lock().await.extend(lines);
        }
    }

    #[async_trait]
    impl PactorTransport for FakeTransport {
        async fn set_mycall(&self, _callsign: &str) -> Result<(), ScsPactorError> {
            Ok(())
        }
        async fn connect_peer(&self, _remote_call: &str) -> Result<(), ScsPactorError> {
            Ok(())
        }
        async fn write_data(&self, data: &[u8]) -> Result<(), ScsPactorError> {
            assert!(
                1 + data.len() * 2 < RADIO_MTU,
                "fragment line exceeds MTU: {} payload bytes",
                data.len()
            );
            self.written.lock().await.push(data.to_vec());
            Ok(())
        }
        async fn read_data(&self, _max_len: usize) -> Result<Vec<u8>, ScsPactorError> {
            self.inbound
                .lock()
                .await
                .pop_front()
                .ok_or(ScsPactorError::Disconnected)
        }
        async fn disconnect(&self) -> Result<(), ScsPactorError> {
            Ok(())
        }
        async fn next_event(
            &self,
            _timeout_after: Option<Duration>,
        ) -> Result<scs_pactor::PactorLinkEvent, ScsPactorError> {
            Err(ScsPactorError::Timeout)
        }
    }

    #[tokio::test]
    async fn small_message_is_single_fragment() {
        let transport = Arc::new(FakeTransport::new());
        let net: PactorNetwork<Vec<u8>, Vec<u8>> = PactorNetwork::new(transport.clone());

        net.send_serialized(b"hello").await.unwrap();
        let lines = transport.take_written().await;
        assert_eq!(lines.len(), 1, "small message should be one fragment");
        let (header, chunk) = parse_fragment(&lines[0]).unwrap();
        assert_eq!(header.total_fragments, 1);
        assert_eq!(chunk, b"hello");
    }

    #[tokio::test]
    async fn large_message_fragments_and_reassembles() {
        let transport = Arc::new(FakeTransport::new());
        let net: PactorNetwork<Vec<u8>, Vec<u8>> = PactorNetwork::new(transport.clone());

        // Several times the per-line chunk budget so it must span many lines.
        let payload: Vec<u8> = (0..4000u32).map(|i| (i % 251) as u8).collect();
        net.send_serialized(&payload).await.unwrap();

        let lines = transport.take_written().await;
        assert!(lines.len() > 1, "large message should fragment");

        // Feed the same fragments back and confirm exact reassembly.
        transport.queue_inbound(lines).await;
        let received = net.receive_serialized().await.unwrap();
        assert_eq!(received, payload);
    }

    #[tokio::test]
    async fn out_of_order_fragments_reassemble() {
        let transport = Arc::new(FakeTransport::new());
        let net: PactorNetwork<Vec<u8>, Vec<u8>> = PactorNetwork::new(transport.clone());

        let payload: Vec<u8> = (0..1500u32).map(|i| (i % 97) as u8).collect();
        net.send_serialized(&payload).await.unwrap();
        let mut lines = transport.take_written().await;
        assert!(lines.len() >= 3);
        lines.reverse(); // deliver in reverse order

        transport.queue_inbound(lines).await;
        let received = net.receive_serialized().await.unwrap();
        assert_eq!(received, payload);
    }

    #[tokio::test]
    async fn unparseable_line_is_skipped() {
        let transport = Arc::new(FakeTransport::new());
        let net: PactorNetwork<Vec<u8>, Vec<u8>> = PactorNetwork::new(transport.clone());

        // One junk line, then a valid single-fragment message.
        net.send_serialized(b"world").await.unwrap();
        let good = transport.take_written().await;
        transport.queue_inbound(vec![vec![0xff, 0xff]]).await;
        transport.queue_inbound(good).await;

        let received = net.receive_serialized().await.unwrap();
        assert_eq!(received, b"world");
    }

    #[tokio::test]
    async fn empty_message_roundtrips() {
        let transport = Arc::new(FakeTransport::new());
        let net: PactorNetwork<Vec<u8>, Vec<u8>> = PactorNetwork::new(transport.clone());

        net.send_serialized(b"").await.unwrap();
        let lines = transport.take_written().await;
        assert_eq!(lines.len(), 1);
        transport.queue_inbound(lines).await;
        let received = net.receive_serialized().await.unwrap();
        assert!(received.is_empty());
    }
}
