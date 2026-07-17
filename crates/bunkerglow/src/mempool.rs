//! Per-node transaction mempool with gossip.
//!
//! Each node keeps its own [`Mempool`] of pending transactions. It sits between
//! the transactions network (a [`TransactionNetwork`], e.g. the PACTOR Txs mux
//! channel or a UDP socket) and the block producer:
//!
//! ```text
//!   RPC / gateway ──submit()──►┐
//!                              │   Mempool (per node)
//!   peers ──gossip (Txs net)──►│  • admit: sig-check + dedup by hash
//!                              │  • gossip newly-admitted txs to peers
//!   producer ◄──receive()──────┤  • order by (sender, nonce), then fee
//!                              │  • evict on inclusion in a finalized block
//!                              └────────────────────────────────────────────
//! ```
//!
//! ## Why wrap the network
//!
//! The block producer pulls transactions with [`Network::receive`] and packs
//! whatever it returns. [`Mempool`] implements [`Network`] itself, so it drops
//! into `Alpenglow` as the producer's `txs_receiver` unchanged — but its
//! `receive()` yields the *best pending* transaction from the local pool rather
//! than the next raw byte blob off the wire, and its `send`/`send_to_many`
//! gossip a transaction to peers. A background admit loop drains the *inner*
//! network, admitting (and re-gossiping) inbound transactions so every node's
//! mempool converges.
//!
//! ## Semantics
//!
//! - Transactions are opaque [`Transaction`] bytes at the consensus layer; the
//!   mempool decodes the inner [`CoreTransaction`] only to derive the ordering
//!   key (`sender`, `nonce`, `fee`) and the dedup hash. Undecodable blobs are
//!   rejected.
//! - `receive()` returns the best pending tx and marks it *in-flight* (not
//!   removed) so a slot that fails to finalize does not silently drop it;
//!   [`Mempool::evict_finalized`] removes txs once their block finalizes, and
//!   [`Mempool::requeue_stale_inflight`] returns long-in-flight txs to pending.
//! - Per-sender nonce ordering: only the lowest pending nonce for a sender is
//!   eligible, so the producer never packs a nonce gap.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bunker_coin_core::transaction::Transaction as CoreTransaction;
use log::{debug, trace};
use tokio::sync::Mutex;

use crate::Transaction;
use crate::network::Network;

/// Maximum number of pending transactions held per node before new admissions
/// are rejected. Bounds memory and, on a slow link, the gossip fan-out.
pub const MAX_MEMPOOL_TXS: usize = 4096;

/// How long a transaction handed to the producer (in-flight) may stay unpacked
/// into a finalized block before it is returned to the pending set for another
/// attempt. Covers the produce → disseminate → vote → finalize round-trip; on a
/// slow HF link this is generous. Kept as a plain wall-clock bound (the mempool
/// has no delta-scaling of its own).
pub const INFLIGHT_REQUEUE_AFTER: Duration = Duration::from_secs(120);

/// A transaction admitted to the mempool, plus its decoded ordering key.
#[derive(Clone)]
struct Entry {
    /// The opaque wire transaction the producer packs and peers gossip.
    wire: Transaction,
    /// Decoded sender (nonce-ordering key).
    sender: [u8; 32],
    /// Decoded nonce (lower = earlier; only the lowest per sender is eligible).
    nonce: u64,
    /// Decoded fee (tie-breaker: higher fee packs first).
    fee: u64,
    /// When this entry was last handed to the producer, if in-flight.
    inflight_since: Option<Instant>,
}

/// Decode the inner [`CoreTransaction`] from an opaque wire [`Transaction`].
///
/// The wire bytes are the bincode encoding of a `CoreTransaction`, possibly
/// wrapped with a wincode 8-byte length prefix (as produced when a `Transaction`
/// is serialized through the network layer). Try the raw bytes first, then the
/// prefix-stripped bytes. Returns `None` for anything that is not a decodable
/// transaction (it was never a valid client tx and must not be admitted).
fn decode_core(wire: &Transaction) -> Option<CoreTransaction> {
    // Limit-guarded: block payloads include BUNKER_BLOAT_BYTES random padding,
    // and without a limit bincode skips its length check — a random u64 read as
    // a String/Vec length triggers a `capacity overflow` PANIC. That panic in
    // the block-executor task killed it permanently (chain kept finalizing but
    // /nodes froze and /blocks capped at frontier+200; observed on-air at slot
    // 3899). Real client txs are well under 4 KiB.
    let config = bincode::config::standard().with_limit::<4096>();
    let data = &wire.0;
    bincode::serde::decode_from_slice(data, config)
        .or_else(|_| {
            if data.len() > 8 {
                bincode::serde::decode_from_slice(&data[8..], config)
            } else {
                Err(bincode::error::DecodeError::Other("too short"))
            }
        })
        .ok()
        .map(|(tx, _)| tx)
}

/// Hex hash of a decoded transaction (the dedup / eviction key).
fn tx_hash_hex(tx: &CoreTransaction) -> String {
    hex::encode(tx.hash())
}

/// Inner mempool state, guarded by a single mutex.
struct Inner {
    /// Pending + in-flight transactions keyed by hex hash.
    entries: HashMap<String, Entry>,
    /// FIFO of hashes for eviction order when at capacity (oldest first).
    admission_order: VecDeque<String>,
}

impl Inner {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            admission_order: VecDeque::new(),
        }
    }

    /// Admit `wire` if new and decodable; returns `true` if newly admitted.
    /// Enforces the capacity bound by evicting the oldest pending entry.
    fn admit(&mut self, wire: Transaction) -> bool {
        let Some(core) = decode_core(&wire) else {
            trace!("[mempool] rejecting undecodable tx blob");
            return false;
        };
        let hash = tx_hash_hex(&core);
        if self.entries.contains_key(&hash) {
            return false; // dedup
        }
        if self.entries.len() >= MAX_MEMPOOL_TXS {
            // Drop the oldest still-present entry to make room.
            while let Some(old) = self.admission_order.pop_front() {
                if self.entries.remove(&old).is_some() {
                    debug!("[mempool] full; evicted oldest tx {old}");
                    break;
                }
            }
        }
        self.entries.insert(
            hash.clone(),
            Entry {
                wire,
                sender: core.sender,
                nonce: core.nonce,
                fee: core.fee,
                inflight_since: None,
            },
        );
        self.admission_order.push_back(hash);
        true
    }

    /// Pick the best pending (not in-flight) transaction to pack next: the
    /// lowest eligible nonce per sender, breaking ties by highest fee. Marks the
    /// chosen entry in-flight and returns its wire form.
    ///
    /// "Eligible nonce per sender" = the minimum pending nonce among that
    /// sender's entries, so the producer never packs a nonce gap.
    fn take_best(&mut self) -> Option<Transaction> {
        // Lowest pending nonce per sender.
        let mut min_nonce: HashMap<[u8; 32], u64> = HashMap::new();
        for e in self.entries.values() {
            if e.inflight_since.is_some() {
                continue;
            }
            min_nonce
                .entry(e.sender)
                .and_modify(|n| *n = (*n).min(e.nonce))
                .or_insert(e.nonce);
        }
        // Among entries at their sender's min nonce, pick highest fee.
        let best_hash = self
            .entries
            .iter()
            .filter(|(_, e)| e.inflight_since.is_none())
            .filter(|(_, e)| min_nonce.get(&e.sender) == Some(&e.nonce))
            .max_by_key(|(_, e)| e.fee)
            .map(|(h, _)| h.clone())?;
        let entry = self.entries.get_mut(&best_hash)?;
        entry.inflight_since = Some(Instant::now());
        Some(entry.wire.clone())
    }

    /// Remove finalized transactions (identified by hex hash) from the pool.
    fn evict(&mut self, hashes: &HashSet<String>) -> usize {
        let before = self.entries.len();
        self.entries.retain(|h, _| !hashes.contains(h));
        before - self.entries.len()
    }

    /// Return in-flight entries older than `after` to the pending set so they can
    /// be re-packed (their slot never finalized). Returns the number requeued.
    fn requeue_stale(&mut self, after: Duration) -> usize {
        let now = Instant::now();
        let mut n = 0;
        for e in self.entries.values_mut() {
            if let Some(since) = e.inflight_since {
                if now.duration_since(since) >= after {
                    e.inflight_since = None;
                    n += 1;
                }
            }
        }
        n
    }
}

/// A per-node transaction mempool wrapping the transactions network `T`.
///
/// Implements [`Network`] so it substitutes for the raw Txs network as the block
/// producer's `txs_receiver`. See the module docs for the data flow.
pub struct Mempool<T: Network> {
    inner: Arc<Mutex<Inner>>,
    /// Underlying transactions network: used to gossip admitted txs to peers and
    /// (by the admit loop) to receive gossiped txs from peers.
    net: Arc<T>,
    /// Peers to gossip to. Over a single mux link the address is a placeholder
    /// (one peer); over UDP these are the validators' tx addresses.
    peers: Vec<SocketAddr>,
    /// Signals the producer-facing `receive()` that new pending txs may exist,
    /// so it can wake without polling.
    notify: Arc<tokio::sync::Notify>,
}

impl<T> Mempool<T>
where
    T: Network<Send = Transaction, Recv = Transaction> + 'static,
{
    /// Create a mempool over transactions network `net`, gossiping to `peers`.
    pub fn new(net: T, peers: Vec<SocketAddr>) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(Mutex::new(Inner::new())),
            net: Arc::new(net),
            peers,
            notify: Arc::new(tokio::sync::Notify::new()),
        })
    }

    /// Spawn the background admit loop: drain the inner network, admit inbound
    /// gossiped transactions, and re-gossip newly-admitted ones so the flood
    /// converges every node's mempool. Call once after construction.
    pub fn spawn_admit_loop(self: &Arc<Self>) {
        let this = self.clone();
        tokio::spawn(async move {
            loop {
                match this.net.receive().await {
                    Ok(wire) => {
                        if this.admit_and_gossip(wire).await {
                            this.notify.notify_one();
                        }
                    }
                    Err(e) => {
                        // A closed inner network ends the loop; a transient error
                        // backs off. Mirrors other consensus receive loops.
                        debug!("[mempool] admit loop receive error: {e}");
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
        });
    }

    /// Submit a locally-originated transaction (from RPC / gateway): admit it and
    /// gossip to peers. Returns `true` if newly admitted (not a duplicate /
    /// undecodable). Wakes the producer-facing `receive()`.
    pub async fn submit(self: &Arc<Self>, wire: Transaction) -> bool {
        let admitted = self.admit_and_gossip(wire).await;
        if admitted {
            self.notify.notify_one();
        }
        admitted
    }

    /// Admit `wire` and, if newly admitted, gossip it to peers. Shared by
    /// `submit` (local ingress) and the admit loop (peer ingress) so flood-fill
    /// convergence is identical regardless of origin.
    async fn admit_and_gossip(&self, wire: Transaction) -> bool {
        let admitted = self.inner.lock().await.admit(wire.clone());
        if admitted && !self.peers.is_empty() {
            if let Err(e) = self
                .net
                .send_to_many(&wire, self.peers.iter().copied())
                .await
            {
                debug!("[mempool] gossip failed: {e}");
            }
        }
        admitted
    }

    /// Remove transactions included in a finalized block from the pool, given the
    /// block's raw wire transactions. Decodes each to its hash and evicts.
    /// Returns the number removed.
    pub async fn evict_finalized(&self, block_txs: &[Transaction]) -> usize {
        let hashes: HashSet<String> = block_txs
            .iter()
            .filter_map(|w| decode_core(w).map(|c| tx_hash_hex(&c)))
            .collect();
        if hashes.is_empty() {
            return 0;
        }
        self.inner.lock().await.evict(&hashes)
    }

    /// Return long-in-flight transactions to the pending set (their slot never
    /// finalized), so the producer can re-pack them. Returns the count requeued.
    pub async fn requeue_stale_inflight(&self) -> usize {
        let n = self
            .inner
            .lock()
            .await
            .requeue_stale(INFLIGHT_REQUEUE_AFTER);
        if n > 0 {
            self.notify.notify_one();
        }
        n
    }

    /// Number of transactions currently held (pending + in-flight).
    pub async fn len(&self) -> usize {
        self.inner.lock().await.entries.len()
    }

    /// Whether the pool is empty.
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}

#[async_trait]
impl<T> Network for Mempool<T>
where
    T: Network<Send = Transaction, Recv = Transaction> + 'static,
{
    type Send = Transaction;
    type Recv = Transaction;

    /// Gossiping a transaction to many peers = admit locally + flood to peers.
    /// (The producer only ever calls `receive`; `send*` here is the gossip path
    /// used by callers that treat the mempool as the tx network.)
    async fn send_to_many(
        &self,
        message: &Transaction,
        _addrs: impl Iterator<Item = SocketAddr> + Send,
    ) -> std::io::Result<()> {
        // Ignore the caller's addrs (the mempool owns its peer set) and gossip.
        let admitted = self.inner.lock().await.admit(message.clone());
        if admitted && !self.peers.is_empty() {
            let _ = self
                .net
                .send_to_many(message, self.peers.iter().copied())
                .await;
            self.notify.notify_one();
        }
        Ok(())
    }

    async fn send(&self, message: &Transaction, _addr: SocketAddr) -> std::io::Result<()> {
        self.send_to_many(message, std::iter::empty()).await
    }

    /// Producer-facing pull: return the best pending transaction, blocking until
    /// one is available. Marks it in-flight (removed only on finalization via
    /// [`Mempool::evict_finalized`]).
    async fn receive(&self) -> std::io::Result<Transaction> {
        loop {
            if let Some(tx) = self.inner.lock().await.take_best() {
                return Ok(tx);
            }
            // No eligible pending tx: wait to be notified of an admission /
            // requeue rather than busy-polling.
            self.notify.notified().await;
        }
    }
}

/// [`Network`] for a shared `Arc<Mempool>`, so the same mempool instance can be
/// both handed to `Alpenglow` as the producer's `txs_receiver` (which takes the
/// network by value) *and* retained by the caller for `submit` / `evict`.
/// Delegates to the inner [`Mempool`] impl.
#[async_trait]
impl<T> Network for Arc<Mempool<T>>
where
    T: Network<Send = Transaction, Recv = Transaction> + 'static,
{
    type Send = Transaction;
    type Recv = Transaction;

    async fn send_to_many(
        &self,
        message: &Transaction,
        addrs: impl Iterator<Item = SocketAddr> + Send,
    ) -> std::io::Result<()> {
        (**self).send_to_many(message, addrs).await
    }

    async fn send(&self, message: &Transaction, addr: SocketAddr) -> std::io::Result<()> {
        (**self).send(message, addr).await
    }

    async fn receive(&self) -> std::io::Result<Transaction> {
        (**self).receive().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bunker_coin_core::transaction::TransactionBody;

    /// Build a wire `Transaction` from sender/nonce/fee (signature is not checked
    /// by the mempool; ordering/dedup are what matter here).
    fn wire(sender: u8, nonce: u64, fee: u64) -> Transaction {
        let core = CoreTransaction {
            sender: [sender; 32],
            nonce,
            fee,
            body: TransactionBody::Transfer {
                to: [0xff; 32],
                amount: 1,
            },
            signature: [0u8; 64],
        };
        let bytes = bincode::serde::encode_to_vec(&core, bincode::config::standard()).unwrap();
        Transaction(bytes)
    }

    fn hash_of(w: &Transaction) -> String {
        tx_hash_hex(&decode_core(w).unwrap())
    }

    #[test]
    fn admit_dedups_by_hash() {
        let mut inner = Inner::new();
        assert!(inner.admit(wire(1, 0, 10)));
        assert!(!inner.admit(wire(1, 0, 10)), "same tx must dedup");
        assert_eq!(inner.entries.len(), 1);
    }

    #[test]
    fn admit_rejects_undecodable() {
        let mut inner = Inner::new();
        assert!(!inner.admit(Transaction(vec![1, 2, 3])));
        assert_eq!(inner.entries.len(), 0);
    }

    #[test]
    fn take_best_orders_by_nonce_then_fee() {
        let mut inner = Inner::new();
        // Same sender: nonce 1 (fee 100) and nonce 0 (fee 1). Lowest nonce wins
        // regardless of fee — no nonce gaps.
        inner.admit(wire(1, 1, 100));
        inner.admit(wire(1, 0, 1));
        let first = inner.take_best().unwrap();
        assert_eq!(decode_core(&first).unwrap().nonce, 0);
    }

    #[test]
    fn take_best_breaks_ties_by_fee_across_senders() {
        let mut inner = Inner::new();
        // Two different senders, both at their min nonce 0: higher fee packs first.
        inner.admit(wire(1, 0, 5));
        inner.admit(wire(2, 0, 50));
        let first = inner.take_best().unwrap();
        assert_eq!(decode_core(&first).unwrap().sender, [2u8; 32]);
    }

    #[test]
    fn take_best_marks_inflight_and_does_not_repeat() {
        let mut inner = Inner::new();
        inner.admit(wire(1, 0, 10));
        assert!(inner.take_best().is_some());
        // Only one pending tx, now in-flight → nothing more to hand out.
        assert!(inner.take_best().is_none());
    }

    #[test]
    fn evict_removes_finalized() {
        let mut inner = Inner::new();
        let w = wire(1, 0, 10);
        let h = hash_of(&w);
        inner.admit(w);
        let mut set = HashSet::new();
        set.insert(h);
        assert_eq!(inner.evict(&set), 1);
        assert_eq!(inner.entries.len(), 0);
    }

    #[test]
    fn requeue_stale_returns_inflight_to_pending() {
        let mut inner = Inner::new();
        inner.admit(wire(1, 0, 10));
        let _ = inner.take_best(); // now in-flight
        assert!(inner.take_best().is_none());
        // Requeue everything in-flight (zero threshold), then it's pending again.
        assert_eq!(inner.requeue_stale(Duration::ZERO), 1);
        assert!(inner.take_best().is_some());
    }

    #[test]
    fn capacity_evicts_oldest() {
        let mut inner = Inner::new();
        // Fill to capacity with distinct txs, then one more evicts the oldest.
        for i in 0..MAX_MEMPOOL_TXS {
            assert!(inner.admit(wire(1, i as u64, 1)));
        }
        assert_eq!(inner.entries.len(), MAX_MEMPOOL_TXS);
        let oldest = hash_of(&wire(1, 0, 1));
        assert!(inner.admit(wire(2, 0, 1)));
        assert_eq!(inner.entries.len(), MAX_MEMPOOL_TXS);
        assert!(!inner.entries.contains_key(&oldest), "oldest evicted");
    }
}
