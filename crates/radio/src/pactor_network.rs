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

use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use bunkerglow::network::Network;
use log::warn;
use scs_pactor::PactorTransport;
use wincode::{SchemaRead, SchemaWrite};

/// Maximum number of bytes read for a single inbound message.
const DEFAULT_MAX_READ_LEN: usize = 8192;

/// Network abstraction over a connected PACTOR modem link.
///
/// `S` is the message type sent, `R` the type received. The transport must
/// already be connected (link established) before sending or receiving.
pub struct PactorNetwork<S, R> {
    transport: Arc<dyn PactorTransport>,
    max_read_len: usize,
    _msg_types: PhantomData<(S, R)>,
}

impl<S, R> PactorNetwork<S, R> {
    /// Wraps an already-connected PACTOR transport.
    pub fn new(transport: Arc<dyn PactorTransport>) -> Self {
        Self {
            transport,
            max_read_len: DEFAULT_MAX_READ_LEN,
            _msg_types: PhantomData,
        }
    }

    /// Override the maximum inbound message size.
    pub fn with_max_read_len(mut self, max_read_len: usize) -> Self {
        self.max_read_len = max_read_len;
        self
    }

    async fn send_serialized(&self, bytes: &[u8]) -> std::io::Result<()> {
        self.transport
            .write_data(bytes)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))
    }
}

impl<S, R> PactorNetwork<S, R>
where
    S: SchemaWrite<Src = S> + Send + Sync,
{
    /// Send several messages within a single transmit turn (no changeover
    /// between them). Each message goes out as its own framed data line, but the
    /// link stays in the sending direction throughout — amortizing the expensive
    /// half-duplex ARQ changeover across the whole batch.
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
            let bytes = self
                .transport
                .read_data(self.max_read_len)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;
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
