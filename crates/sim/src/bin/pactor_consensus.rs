//! Two-validator Alpenglow consensus over one PACTOR link.
//!
//! Simulated mode runs both validators in-process; hardware mode runs one
//! validator per modem. Both derive membership from `--seed` and multiplex all
//! logical networks through [`PactorMux`].

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bunker_coin_core::execution::State as ExecutionState;
use bunker_coin_core::transaction::Transaction as CoreTransaction;
use bunker_coin_radio::{Channel, MuxChannel, PactorMux, PactorMuxHandle};
use bunker_coin_sim::pactor_init::{
    connect_with_retries, init_modem, light_init_modem, PactorInitConfig,
};
use bunkerglow::all2all::TrivialAll2All;
use bunkerglow::consensus::{Alpenglow, ConsensusMessage, EpochInfo};
use bunkerglow::crypto::aggsig;
use bunkerglow::crypto::merkle::DoubleMerkleRoot;
use bunkerglow::crypto::signature::SecretKey;
use bunkerglow::disseminator::rotor::StakeWeightedSampler;
use bunkerglow::disseminator::Rotor;
use bunkerglow::mempool::Mempool;
use bunkerglow::network::dontcare_sockaddr;
use bunkerglow::repair::{RepairRequest, RepairResponse};
use bunkerglow::shredder::Shred;
use bunkerglow::Slot;
use bunkerglow::{Transaction, ValidatorInfo};
use clap::Parser;
use ed25519_dalek::SigningKey;
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use scs_pactor::{PactorTransport, SimulatedPactorConfig, SimulatedPactorPair, UsbPactorTransport};
use std::collections::HashMap;

/// Logical networks multiplexed over one PACTOR link.
type MuxAll2All = MuxChannel<ConsensusMessage, ConsensusMessage>;
type MuxShred = MuxChannel<Shred, Shred>;
type MuxRepair = MuxChannel<RepairRequest, RepairResponse>;
type MuxRepairReq = MuxChannel<RepairResponse, RepairRequest>;
type MuxTxs = MuxChannel<Transaction, Transaction>;

/// Per-node mempool over the Txs mux channel.
type NodeMempool = Arc<Mempool<MuxTxs>>;

/// Alpenglow node whose consensus networks share the PACTOR mux.
type Node =
    Alpenglow<TrivialAll2All<MuxAll2All>, Rotor<MuxShred, StakeWeightedSampler>, NodeMempool>;

#[derive(Parser)]
#[command(version, about = "Empty-block Alpenglow consensus over PACTOR", long_about = None)]
struct Args {
    /// Run both validators in-process over a simulated PACTOR pair.
    #[arg(long)]
    simulated: bool,

    /// In simulated mode, exercise half-duplex turn handoff.
    #[arg(long)]
    half_duplex: bool,

    /// Simulated per-frame packet loss percentage; ARQ retries preserve delivery.
    #[arg(long, default_value_t = 0.0)]
    packet_loss: f32,

    /// Serial device for the modem (hardware mode).
    #[arg(long)]
    port: Option<String>,

    /// Which validator this process runs in hardware mode: 0 (caller) or 1 (listener).
    #[arg(long)]
    node: Option<u64>,

    /// This modem's callsign (hardware mode).
    #[arg(long, default_value = "NODE0")]
    mycall: String,

    /// Peer callsign to connect to (hardware mode, node 0 only).
    #[arg(long, default_value = "NODE1")]
    peercall: String,

    /// Serial baud rate (hardware mode).
    #[arg(long, default_value_t = 829_440)]
    baud: u32,

    /// Optional TRX CI-V tune frequency in kHz (hardware mode).
    #[arg(long)]
    frequency: Option<f64>,

    /// Consensus timing multiplier for slow links; sets `BUNKER_DELTA_MULT`.
    #[arg(long)]
    delta_mult: Option<f64>,

    /// Connect attempts before giving up (hardware mode, node 0).
    #[arg(long, default_value_t = 3)]
    connect_attempts: u32,

    /// Force-disconnect any stale link before init (hardware mode).
    #[arg(long)]
    reset: bool,

    /// Seed for deterministic validator-set generation (must match on both nodes).
    #[arg(long, default_value_t = 0)]
    seed: u64,

    /// Run duration in seconds; omitted means run until Ctrl-C.
    #[arg(long)]
    duration: Option<u64>,

    /// Serve the HTTP RPC API on 127.0.0.1:3001.
    #[arg(long)]
    rpc: bool,

    /// Serve RPC over the persisted chain without touching the modem.
    #[arg(long)]
    inspect: bool,
}

/// Block store shared by a node and the RPC server.
type SharedBlockstore =
    Arc<tokio::sync::RwLock<Box<dyn bunkerglow::consensus::Blockstore + Send + Sync>>>;

/// RPC-facing transaction, execution, node, and radio state.
#[derive(Clone)]
struct TxContext {
    tx_sender: tokio::sync::mpsc::UnboundedSender<CoreTransaction>,
    execution_state: Arc<tokio::sync::RwLock<ExecutionState>>,
    mempool: Arc<tokio::sync::RwLock<Vec<rpc::MempoolEntry>>>,
    tx_results: Arc<tokio::sync::RwLock<HashMap<String, rpc::TxResult>>>,
    /// FIFO order for capping retained `/tx/{hash}` results.
    tx_results_order: Arc<tokio::sync::RwLock<std::collections::VecDeque<String>>>,
    /// Genesis key used to server-side-sign zero-signature account transactions.
    genesis_signing_key: Arc<SigningKey>,
    /// `/nodes` follows the local finalized frontier in this two-validator network.
    nodes: Arc<tokio::sync::RwLock<Vec<rpc::NodeStatus>>>,
    updates: tokio::sync::broadcast::Sender<rpc::WebSocketUpdate>,
    radio_stats: Arc<tokio::sync::RwLock<rpc::RadioStats>>,
}

impl TxContext {
    /// Build a context and return the RPC transaction receiver.
    fn new(
        cluster: &Cluster,
        execution_state: Arc<tokio::sync::RwLock<ExecutionState>>,
    ) -> (Self, tokio::sync::mpsc::UnboundedReceiver<CoreTransaction>) {
        let (tx_sender, tx_rx) = tokio::sync::mpsc::unbounded_channel();
        let ctx = TxContext {
            tx_sender,
            execution_state,
            mempool: Arc::new(tokio::sync::RwLock::new(Vec::new())),
            tx_results: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            tx_results_order: Arc::new(tokio::sync::RwLock::new(std::collections::VecDeque::new())),
            genesis_signing_key: Arc::new(cluster.genesis_key.clone()),
            nodes: Arc::new(tokio::sync::RwLock::new(vec![
                rpc::NodeStatus {
                    node_id: 0,
                    finalized_slot: 0,
                },
                rpc::NodeStatus {
                    node_id: 1,
                    finalized_slot: 0,
                },
            ])),
            updates: tokio::sync::broadcast::channel(256).0,
            radio_stats: Arc::new(tokio::sync::RwLock::new(rpc::RadioStats {
                bandwidth_bps: 0,
                packet_loss_percent: 0.0,
                latency_ms: 0,
                jitter_ms: 0,
                packets_sent: 0,
                packets_dropped: 0,
                packets_queued: 0,
                current_throughput_bps: 0.0,
                link_speed_level: 0,
            })),
        };
        (ctx, tx_rx)
    }

    /// Rebind execution state for a reconnect while preserving RPC-facing state.
    fn with_execution_state(
        &self,
        execution_state: Arc<tokio::sync::RwLock<ExecutionState>>,
    ) -> Self {
        TxContext {
            tx_sender: self.tx_sender.clone(),
            execution_state,
            mempool: self.mempool.clone(),
            tx_results: self.tx_results.clone(),
            tx_results_order: self.tx_results_order.clone(),
            genesis_signing_key: self.genesis_signing_key.clone(),
            nodes: self.nodes.clone(),
            updates: self.updates.clone(),
            radio_stats: self.radio_stats.clone(),
        }
    }
}

/// Cumulative PACTOR I/O counters shared across reconnect sessions.
#[derive(Default)]
struct LinkCounters {
    frames_sent: AtomicU64,
    bytes_sent: AtomicU64,
    frames_received: AtomicU64,
    bytes_received: AtomicU64,
    /// Last modem-reported PACTOR speed level; 0 means none yet.
    speed_level: AtomicU64,
    /// Cumulative frame retransmissions reported by transports that expose them.
    frames_retried: AtomicU64,
    /// Live mux outbound-queue depth.
    outbound_queued: Arc<AtomicU64>,
}

/// Consecutive `write_data` failures after which a wedged link is reconnected.
const WRITE_FAILURES_LINK_DOWN: u64 = 3;

/// Receive-stall watchdog threshold; catches links that accept writes but deliver no bytes.
fn rx_stall_link_down_secs() -> u64 {
    std::env::var("BUNKER_RX_STALL_SECS")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(600)
}

/// Transport wrapper that counts I/O and exposes link watchdog state.
struct CountingTransport {
    inner: Arc<dyn PactorTransport>,
    counters: Arc<LinkCounters>,
    /// Session-local, so reconnects do not inherit stale failures.
    consecutive_write_failures: AtomicU64,
    /// Last receive time for the rx-stall watchdog.
    last_rx: std::sync::Mutex<Instant>,
}

#[async_trait::async_trait]
impl PactorTransport for CountingTransport {
    async fn set_mycall(&self, callsign: &str) -> Result<(), scs_pactor::ScsPactorError> {
        self.inner.set_mycall(callsign).await
    }

    async fn connect_peer(&self, remote_call: &str) -> Result<(), scs_pactor::ScsPactorError> {
        self.inner.connect_peer(remote_call).await
    }

    async fn accept_incoming(
        &self,
        timeout_after: Option<Duration>,
    ) -> Result<String, scs_pactor::ScsPactorError> {
        self.inner.accept_incoming(timeout_after).await
    }

    async fn write_data(&self, data: &[u8]) -> Result<(), scs_pactor::ScsPactorError> {
        match self.inner.write_data(data).await {
            Ok(()) => {
                self.consecutive_write_failures.store(0, Ordering::Relaxed);
                self.counters.frames_sent.fetch_add(1, Ordering::Relaxed);
                self.counters
                    .bytes_sent
                    .fetch_add(data.len() as u64, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                let failures = self
                    .consecutive_write_failures
                    .fetch_add(1, Ordering::Relaxed)
                    + 1;
                log::warn!(
                    "[link-watchdog] write failure {failures}/{WRITE_FAILURES_LINK_DOWN}: {e}"
                );
                Err(e)
            }
        }
    }

    async fn read_data(&self, max_len: usize) -> Result<Vec<u8>, scs_pactor::ScsPactorError> {
        let payload = self.inner.read_data(max_len).await?;
        *self.last_rx.lock().unwrap() = Instant::now();
        self.counters
            .frames_received
            .fetch_add(1, Ordering::Relaxed);
        self.counters
            .bytes_received
            .fetch_add(payload.len() as u64, Ordering::Relaxed);
        Ok(payload)
    }

    async fn changeover(&self) -> Result<(), scs_pactor::ScsPactorError> {
        self.inner.changeover().await
    }

    async fn disconnect(&self) -> Result<(), scs_pactor::ScsPactorError> {
        self.inner.disconnect().await
    }

    fn is_link_up(&self) -> bool {
        let rx_stalled = self.last_rx.lock().unwrap().elapsed().as_secs();
        if rx_stalled > rx_stall_link_down_secs() {
            log::warn!("[link-watchdog] no bytes received for {rx_stalled}s; declaring link down");
            return false;
        }
        if self.consecutive_write_failures.load(Ordering::Relaxed) >= WRITE_FAILURES_LINK_DOWN {
            // A wedged modem may not emit disconnect; force reconnect.
            return false;
        }
        self.inner.is_link_up()
    }

    async fn next_event(
        &self,
        timeout_after: Option<Duration>,
    ) -> Result<scs_pactor::PactorLinkEvent, scs_pactor::ScsPactorError> {
        let event = self.inner.next_event(timeout_after).await;
        // Capture passive link-quality events for radio stats.
        if let Ok(scs_pactor::PactorLinkEvent::LinkQuality {
            speed_level,
            retries,
        }) = &event
        {
            self.counters
                .speed_level
                .store(u64::from(*speed_level), Ordering::Relaxed);
            if *retries > 0 {
                self.counters
                    .frames_retried
                    .fetch_add(u64::from(*retries), Ordering::Relaxed);
            }
        }
        event
    }

    async fn broadcast_fec(&self, data: &[u8]) -> Result<(), scs_pactor::ScsPactorError> {
        self.inner.broadcast_fec(data).await
    }
}

/// Drive passive link-quality events so [`CountingTransport`] can record them.
fn spawn_link_quality_poller(
    transport: Arc<dyn PactorTransport>,
    cancel: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                ev = transport.next_event(Some(Duration::from_secs(5))) => {
                    if ev.is_err() {
                        // Avoid spinning on timeout or torn-down transport.
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }
    });
}

/// Number of 2s samples in the 30s radio-stats window.
const RADIO_STATS_WINDOW_SAMPLES: usize = 15;

/// Publish a rolling 30s radio-stats window for `/radio` and WebSocket clients.
/// Dropped/loss are reported only when the transport exposes retry counts.
fn spawn_radio_stats_sampler(counters: Arc<LinkCounters>, tx: &TxContext) {
    let updates = tx.updates.clone();
    let radio_stats = tx.radio_stats.clone();
    tokio::spawn(async move {
        let mut last_sent_frames = 0u64;
        let mut last_sent_bytes = 0u64;
        let mut last_recv_bytes = 0u64;
        let mut last_retried = 0u64;
        // Per-sample deltas: frames sent, bytes sent, bytes received, retries.
        let mut window: std::collections::VecDeque<(u64, u64, u64, u64)> =
            std::collections::VecDeque::with_capacity(RADIO_STATS_WINDOW_SAMPLES);
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;

            let sent_frames = counters.frames_sent.load(Ordering::Relaxed);
            let sent_bytes = counters.bytes_sent.load(Ordering::Relaxed);
            let recv_bytes = counters.bytes_received.load(Ordering::Relaxed);
            let retried = counters.frames_retried.load(Ordering::Relaxed);

            let d_frames = sent_frames - last_sent_frames;
            let d_sent = sent_bytes - last_sent_bytes;
            let d_recv = recv_bytes - last_recv_bytes;
            let d_retried = retried - last_retried;
            last_sent_frames = sent_frames;
            last_sent_bytes = sent_bytes;
            last_recv_bytes = recv_bytes;
            last_retried = retried;

            if window.len() == RADIO_STATS_WINDOW_SAMPLES {
                window.pop_front();
            }
            window.push_back((d_frames, d_sent, d_recv, d_retried));

            let w_frames: u64 = window.iter().map(|s| s.0).sum();
            let w_sent: u64 = window.iter().map(|s| s.1).sum();
            let w_recv: u64 = window.iter().map(|s| s.2).sum();
            let w_retried: u64 = window.iter().map(|s| s.3).sum();
            let window_secs = (window.len() * 2) as f64;

            // Loss is retries over delivered-plus-retried frame attempts.
            let loss_rate = if w_frames + w_retried > 0 {
                w_retried as f64 / (w_frames + w_retried) as f64
            } else {
                0.0
            };
            let queued = counters.outbound_queued.load(Ordering::Relaxed);

            // Throughput counts both directions on the shared channel.
            let throughput_bps = ((w_sent + w_recv) * 8) as f64 / window_secs;

            let speed_level = counters.speed_level.load(Ordering::Relaxed);

            {
                let mut stats = radio_stats.write().await;
                stats.packets_sent = sent_frames;
                stats.packets_dropped = retried;
                stats.packets_queued = queued;
                stats.packet_loss_percent = (loss_rate * 100.0) as f32;
                stats.current_throughput_bps = throughput_bps;
                stats.link_speed_level = speed_level;
            }

            // Wire field names still say 2s, but values are windowed totals.
            // No WebSocket client connected is fine; send() just returns Err.
            let _ = updates.send(rpc::WebSocketUpdate::RadioStats {
                packets_sent_2s: w_frames,
                packets_dropped_2s: w_retried,
                packets_transmitted_2s: w_frames + w_retried,
                bytes_transmitted_2s: w_sent,
                effective_throughput_bps_2s: throughput_bps,
                packet_loss_rate_2s: loss_rate,
                packets_queued: queued,
                link_speed_level: speed_level,
                bytes_received_2s: w_recv,
            });
        }
    });
}

/// Build RPC state backed by the live blockstore and transaction context.
fn rpc_state_for(blockstore: SharedBlockstore, tx: &TxContext) -> rpc::SharedState {
    rpc::SharedState {
        blocks: Arc::new(tokio::sync::RwLock::new(Vec::new())),
        nodes: tx.nodes.clone(),
        radio_stats: tx.radio_stats.clone(),
        updates: tx.updates.clone(),
        blockstore: Some(blockstore),
        mempool: tx.mempool.clone(),
        tx_sender: Some(tx.tx_sender.clone()),
        execution_state: tx.execution_state.clone(),
        tx_results: tx.tx_results.clone(),
        genesis_signing_key: Some(tx.genesis_signing_key.clone()),
        snapshot_store: None,
    }
}

/// Spawn RPC for the simulated, non-reconnect path.
fn spawn_rpc(blockstore: SharedBlockstore, tx: &TxContext) {
    println!("RPC API serving on http://127.0.0.1:3001 (try /blocks, POST /transactions)");
    tokio::spawn(rpc::run_api(rpc_state_for(blockstore, tx)));
}

/// Current per-session mempool; `None` while hardware is between links.
/// The long-lived RPC bridge drops submissions until the next session appears.
type MempoolSlot = Arc<tokio::sync::RwLock<Option<NodeMempool>>>;

/// Encode RPC-submitted transactions and hand them to the current node mempool.
fn spawn_tx_bridge(
    mut tx_rx: tokio::sync::mpsc::UnboundedReceiver<CoreTransaction>,
    mempool: MempoolSlot,
) {
    tokio::spawn(async move {
        while let Some(core_tx) = tx_rx.recv().await {
            match bincode::serde::encode_to_vec(&core_tx, bincode::config::standard()) {
                Ok(bytes) => {
                    let wire = Transaction(bytes);
                    if let Some(mp) = mempool.read().await.as_ref() {
                        let admitted = mp.submit(wire).await;
                        trace_submit(&core_tx, admitted);
                    } else {
                        log::warn!("tx dropped: no live mempool (link down)");
                    }
                }
                Err(e) => log::warn!("tx encode failed: {e}"),
            }
        }
        log::info!("tx bridge shutting down");
    });
}

fn trace_submit(core_tx: &CoreTransaction, admitted: bool) {
    if admitted {
        log::debug!("mempool admitted tx {}", hex::encode(core_tx.hash()));
    } else {
        log::debug!(
            "mempool rejected tx {} (duplicate/undecodable)",
            hex::encode(core_tx.hash())
        );
    }
}

/// Requeue long-in-flight mempool transactions until `cancel` fires.
fn spawn_mempool_maintenance(mempool: NodeMempool, cancel: tokio_util::sync::CancellationToken) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(30));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = ticker.tick() => {
                    let n = mempool.requeue_stale_inflight().await;
                    if n > 0 {
                        log::info!("requeued {n} stale in-flight mempool tx(s)");
                    }
                }
            }
        }
    });
}

/// Execute newly finalized blocks and mirror results into RPC-visible state.
/// Both nodes apply the same genesis and finalized blocks, so state stays aligned.
fn spawn_block_executor(
    label: String,
    blockstore: SharedBlockstore,
    pool: Arc<tokio::sync::RwLock<Box<dyn bunkerglow::consensus::Pool + Send + Sync>>>,
    tx: TxContext,
    mempool: NodeMempool,
    cancel: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        let mut last_executed: u64 = 0;
        loop {
            if cancel.is_cancelled() {
                break;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;

            let finalized = pool.read().await.finalized_slot().inner();

            // Keep `/nodes` tracking consensus even if execution stalls.
            for node in tx.nodes.write().await.iter_mut() {
                node.finalized_slot = finalized;
            }

            if finalized <= last_executed {
                continue;
            }

            let bs = blockstore.read().await;
            // Bound catch-up work per tick so restarts stay responsive.
            let batch_end = finalized.min(last_executed + 500);
            for slot in (last_executed + 1)..=batch_end {
                let slot_id = Slot::new(slot);
                let Some(hash) = bs.canonical_block_hash(slot_id) else {
                    continue; // skip-certified slot: no block to execute
                };
                let block_hash_hex = hex::encode(&hash);
                let block_hash: DoubleMerkleRoot = hash.into();
                let Some(block) = bs.get_block(&(slot_id, block_hash)) else {
                    continue;
                };
                let raw_txs = block.transactions();
                // Evict finalized wire txs so mempool hashes match admission hashes.
                let evicted = mempool.evict_finalized(raw_txs).await;
                if evicted > 0 {
                    log::debug!("[{label}] evicted {evicted} finalized tx(s) from mempool");
                }

                let core_txs = decode_block_txs(raw_txs);
                if core_txs.is_empty() {
                    continue;
                }

                let results = tx.execution_state.write().await.execute_block(&core_txs);
                let ok = results.iter().filter(|r| r.is_ok()).count();
                println!(
                    "[{label}] executed slot {slot}: {ok} ok, {} failed ({} txs)",
                    results.len() - ok,
                    core_txs.len()
                );

                record_tx_results(&tx, slot, &block_hash_hex, &core_txs, &results).await;
            }
            drop(bs);
            last_executed = batch_end;
        }
    });
}

/// Decode raw block transactions, accepting the legacy 8-byte length prefix.
fn decode_block_txs(raw: &[Transaction]) -> Vec<CoreTransaction> {
    // Limit decode so random padding cannot become an unbounded Vec allocation.
    let config = bincode::config::standard().with_limit::<4096>();
    raw.iter()
        .filter_map(|t| {
            let data = &t.0;
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
        })
        .collect()
}

/// Record one finalized slot's transaction results for RPC lookup.
async fn record_tx_results(
    tx: &TxContext,
    slot: u64,
    block_hash_hex: &str,
    core_txs: &[CoreTransaction],
    results: &[Result<(), bunker_coin_core::execution::ExecutionError>],
) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    /// FIFO cap: old `/tx/{hash}` results age out before the RocksDB chain.
    const MAX_TX_RESULTS: usize = 10_000;

    let mut mempool = tx.mempool.write().await;
    let mut results_map = tx.tx_results.write().await;
    let mut results_order = tx.tx_results_order.write().await;
    for (core_tx, exec_result) in core_txs.iter().zip(results.iter()) {
        let hash = hex::encode(core_tx.hash());
        let (status, error) = match exec_result {
            Ok(()) => (rpc::TxFinalStatus::Finalized, None),
            Err(e) => (rpc::TxFinalStatus::Failed, Some(e.to_string())),
        };
        // First execution wins: duplicate inclusions fail on nonce and must not
        // clobber an earlier successful result.
        if !results_map.contains_key(&hash) {
            results_map.insert(
                hash.clone(),
                rpc::TxResult {
                    hash: hash.clone(),
                    slot,
                    block_hash: block_hash_hex.to_string(),
                    status,
                    error,
                    executed_at: now,
                },
            );
            results_order.push_back(hash.clone());
            while results_order.len() > MAX_TX_RESULTS {
                if let Some(oldest) = results_order.pop_front() {
                    results_map.remove(&oldest);
                }
            }
        }
        mempool.retain(|e| e.hash != hash);
    }
}

/// Native balance credited to the genesis account for client transfers.
const GENESIS_BALANCE: u64 = 1_000_000_000_000;

/// Deterministic keys and public validator info for the two-node set.
struct Cluster {
    secret_keys: Vec<SecretKey>,
    voting_keys: Vec<aggsig::SecretKey>,
    validators: Vec<ValidatorInfo>,
    /// Genesis key shared by both nodes via the deterministic seed.
    genesis_key: SigningKey,
}

impl Cluster {
    fn genesis_pubkey(&self) -> [u8; 32] {
        self.genesis_key.verifying_key().to_bytes()
    }

    /// Fresh genesis-funded state; finalized blocks keep both nodes aligned.
    fn genesis_state(&self) -> ExecutionState {
        let mut state = ExecutionState::new();
        state
            .get_or_create_account(&self.genesis_pubkey())
            .native_balance = GENESIS_BALANCE;
        state
    }
}

/// Build the fixed two-validator set from `seed`.
/// Mux channels ignore per-channel addresses, so validators use a placeholder.
fn build_cluster(seed: u64) -> Cluster {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut secret_keys = Vec::new();
    let mut voting_keys = Vec::new();
    let mut validators = Vec::new();
    for id in 0..2u64 {
        let sk = SecretKey::new(&mut rng);
        let vk = aggsig::SecretKey::new(&mut rng);
        validators.push(ValidatorInfo {
            id,
            stake: 1,
            pubkey: sk.to_pk(),
            voting_pubkey: vk.to_pk(),
            all2all_address: dontcare_sockaddr(),
            disseminator_address: dontcare_sockaddr(),
            repair_request_address: dontcare_sockaddr(),
            repair_response_address: dontcare_sockaddr(),
            location: None,
        });
        secret_keys.push(sk);
        voting_keys.push(vk);
    }
    // Derive from raw seed bytes to avoid a rand_core version dependency.
    let mut genesis_seed = [0u8; 32];
    rng.fill_bytes(&mut genesis_seed);
    let genesis_key = SigningKey::from_bytes(&genesis_seed);
    Cluster {
        secret_keys,
        voting_keys,
        validators,
        genesis_key,
    }
}

/// Wire one Alpenglow node over a connected PACTOR transport.
/// `turn` selects full-duplex (`None`) or half-duplex initial turn ownership.
fn build_node(
    transport: Arc<dyn PactorTransport>,
    own_id: u64,
    cluster: &Cluster,
    turn: Option<bool>,
    queued_gauge: Arc<AtomicU64>,
) -> (
    Node,
    PactorMuxHandle,
    NodeMempool,
    Arc<tokio::sync::RwLock<ExecutionState>>,
) {
    let mut mux = match turn {
        None => PactorMux::new(transport),
        Some(starts_with_turn) => PactorMux::new_half_duplex(transport, starts_with_turn),
    };
    mux.set_queued_gauge(queued_gauge);
    // All2All needs self-delivery because the single mux link has no socket loopback.
    let all2all_net: MuxAll2All = mux.channel_self_delivering(Channel::All2All);
    let shred_net: MuxShred = mux.channel(Channel::Disseminator);
    let repair_net: MuxRepair = mux.channel(Channel::Repair);
    let repair_req_net: MuxRepairReq = mux.channel(Channel::RepairRequest);
    // Txs is peer-gossip only; local submissions enter through the node mempool.
    let txs_net: MuxTxs = mux.channel(Channel::Txs);
    let handle = mux.spawn();

    // A placeholder peer address is enough because the mux ignores socket addrs.
    let mempool: NodeMempool = Mempool::new(txs_net, vec![dontcare_sockaddr()]);
    mempool.spawn_admit_loop();

    let epoch_info = Arc::new(EpochInfo::new(0, own_id, cluster.validators.clone()));
    let all2all = TrivialAll2All::new(cluster.validators.clone(), all2all_net);
    let disseminator = Rotor::new(shred_net, epoch_info.clone());

    let mut node = Alpenglow::new(
        cluster.secret_keys[own_id as usize].clone(),
        cluster.voting_keys[own_id as usize].clone(),
        all2all,
        disseminator,
        repair_net,
        repair_req_net,
        epoch_info,
        mempool.clone(),
    );

    // Identical genesis plus finalized blocks keeps node execution state aligned.
    let execution_state = Arc::new(tokio::sync::RwLock::new(cluster.genesis_state()));
    node.set_execution_state(execution_state.clone());

    // Half-duplex links feed keepalive liveness into Votor so slow reverse paths
    // re-arm crashed-leader timeouts instead of jumping ahead.
    if turn.is_some() {
        node.set_link_liveness(handle.liveness());
    }

    (node, handle, mempool, execution_state)
}

/// Process-wide Ctrl-C flag shared by the reconnect loop and node runner.
static SHUTDOWN: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn shutdown_requested() -> bool {
    SHUTDOWN.load(std::sync::atomic::Ordering::Relaxed)
}

fn spawn_shutdown_watcher() {
    tokio::spawn(async {
        if tokio::signal::ctrl_c().await.is_ok() {
            println!("\n=== Ctrl-C received; shutting down gracefully ===");
            SHUTDOWN.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    });
}

/// Why [`run_node`] stopped.
enum RunStop {
    Deadline,
    LinkDown,
    Shutdown,
}

/// Drive a node until deadline, shutdown, or link drop.
/// The pool handle must be cloned before `Alpenglow::run` consumes the node.
async fn run_node(
    label: &str,
    node: Node,
    mut handle: PactorMuxHandle,
    until: tokio::time::Instant,
    link: Option<Arc<dyn PactorTransport>>,
) -> (u64, RunStop) {
    let pool = node.get_pool();
    let cancel = node.get_cancel_token();
    let run_task = tokio::spawn(node.run());

    let mut last = 0u64;
    let (highest, stop) = loop {
        let finalized = pool.read().await.finalized_slot().inner();
        if finalized != last {
            println!("[{label}] finalized slot {finalized}");
            last = finalized;
        }
        if shutdown_requested() {
            println!("[{label}] shutdown requested (finalized {finalized} so far)");
            break (finalized, RunStop::Shutdown);
        }
        if tokio::time::Instant::now() >= until {
            break (finalized, RunStop::Deadline);
        }
        if let Some(link) = &link {
            if !link.is_link_up() {
                println!("[{label}] link dropped (finalized {finalized} so far)");
                break (finalized, RunStop::LinkDown);
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    };

    // Await `run()` before closing the mux so RocksDB handles drop before reconnect.
    // The timeout only bounds a pathologically stuck teardown.
    cancel.cancel();
    match tokio::time::timeout(Duration::from_secs(15), run_task).await {
        Ok(_) => {}
        Err(_) => eprintln!("[{label}] consensus teardown did not finish within 15s"),
    }
    handle.shutdown();
    drop(handle);
    (highest, stop)
}

/// Suppress only the expected mux-closed panic from orphaned teardown tasks.
/// All other panics still delegate to the default hook.
fn install_teardown_panic_filter() {
    let default = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let msg = info
            .payload()
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| info.payload().downcast_ref::<&str>().copied())
            .unwrap_or("");
        if msg.contains("mux inbound queue closed") {
            return; // expected orphaned-task teardown; stay quiet
        }
        default(info);
    }));
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    install_teardown_panic_filter();
    let args = Args::parse();
    let cluster = build_cluster(args.seed);
    let duration = args.duration.map(Duration::from_secs);

    if args.inspect {
        run_inspect(&args, cluster, duration).await
    } else if args.simulated {
        run_simulated(
            cluster,
            duration,
            args.half_duplex,
            args.rpc,
            args.packet_loss,
        )
        .await
    } else {
        run_hardware(&args, cluster, duration).await
    }
}

async fn run_until(duration: Option<Duration>) {
    match duration {
        Some(d) => tokio::time::sleep(d).await,
        None => {
            let _ = tokio::signal::ctrl_c().await;
            println!("\n=== Ctrl-C received; shutting down gracefully ===");
        }
    }
}

/// Per-session deadline; continuous runs use a far-future instant.
fn deadline_for(duration: Option<Duration>) -> tokio::time::Instant {
    let now = tokio::time::Instant::now();
    match duration {
        Some(d) => now + d,
        None => now + Duration::from_secs(10 * 365 * 24 * 3600),
    }
}

/// Serve RPC over a node's persisted block store without modem or consensus.
async fn run_inspect(
    args: &Args,
    cluster: Cluster,
    duration: Option<Duration>,
) -> anyhow::Result<()> {
    let own_id = args
        .node
        .ok_or_else(|| anyhow::anyhow!("--inspect requires --node <id> to pick the block store"))?;

    // Pure reads do not send on the required votor channel.
    let (votor_tx, _votor_rx) = tokio::sync::mpsc::channel(1);
    let epoch_info = Arc::new(EpochInfo::new(0, own_id, cluster.validators.clone()));
    let blockstore: SharedBlockstore = Arc::new(tokio::sync::RwLock::new(Box::new(
        bunkerglow::consensus::BlockstoreImpl::new(epoch_info, votor_tx),
    )));

    println!(
        "=== inspect: serving node {own_id}'s on-disk chain (data/blockstore/{own_id}) ===\n\
         RPC API on http://127.0.0.1:3001 — try /blocks or /block/slot/1"
    );
    // Offline inspect has no tx bridge; `/submit` is a no-op.
    let exec = Arc::new(tokio::sync::RwLock::new(cluster.genesis_state()));
    let (tx_ctx, _tx_rx) = TxContext::new(&cluster, exec);
    spawn_rpc(blockstore, &tx_ctx);

    run_until(duration).await;
    Ok(())
}

/// Build both nodes over a simulated PACTOR pair and run them together.
/// `half_duplex` exercises real PACTOR turn handoff in-process.
async fn run_simulated(
    cluster: Cluster,
    duration: Option<Duration>,
    half_duplex: bool,
    rpc: bool,
    packet_loss_percent: f32,
) -> anyhow::Result<()> {
    // Simulated ARQ retries lost frames; clamp away retry-exhaustion configs.
    let packet_loss = (packet_loss_percent / 100.0).clamp(0.0, 0.9);
    spawn_shutdown_watcher();
    println!(
        "=== simulated 2-node empty-block consensus over PACTOR mux ({}) ===",
        if half_duplex {
            "half-duplex turns"
        } else {
            "full-duplex"
        }
    );
    // Half-duplex mode uses the same slow-path vote deferral as hardware.
    if half_duplex {
        // SAFETY: set before any node / Votor is built below.
        unsafe {
            std::env::set_var("BUNKER_DEFER_FINAL_VOTE", "1");
        }
    }
    // Half-duplex models one shared ARQ channel with asymmetric reverse latency.
    let config = if half_duplex {
        SimulatedPactorConfig {
            packet_loss,
            latency_jitter: Duration::ZERO,
            setup_delay: Duration::ZERO,
            forced_initial_losses: 0,
            fade_windows: Vec::new(),
            read_timeout: None,
            ..SimulatedPactorConfig::half_duplex_hf()
        }
    } else {
        SimulatedPactorConfig {
            packet_loss,
            latency: Duration::from_millis(20),
            latency_jitter: Duration::ZERO,
            setup_delay: Duration::ZERO,
            forced_initial_losses: 0,
            fade_windows: Vec::new(),
            read_timeout: None,
            ..SimulatedPactorConfig::default()
        }
    };
    let (ta, tb) = SimulatedPactorPair::new(config);

    // Connect before wiring consensus; simulated transports reject early writes.
    ta.set_mycall("NODE0").await?;
    tb.set_mycall("NODE1").await?;
    tb.accept_incoming(None).await.ok();
    ta.connect_peer("NODE1").await?;

    // Node 0 serves RPC, so its counters feed radio stats.
    let link_counters = Arc::new(LinkCounters::default());
    let ta: Arc<dyn PactorTransport> = Arc::new(CountingTransport {
        inner: Arc::new(ta),
        counters: link_counters.clone(),
        consecutive_write_failures: AtomicU64::new(0),
        last_rx: std::sync::Mutex::new(Instant::now()),
    });
    let tb: Arc<dyn PactorTransport> = Arc::new(tb);

    // Half-duplex starts with node 0 holding the transmit turn.
    let turn_a = half_duplex.then_some(true);
    let turn_b = half_duplex.then_some(false);
    // Only node A's queue depth is served through RPC.
    let (node_a, handle_a, mempool_a, exec_a) = build_node(
        ta.clone(),
        0,
        &cluster,
        turn_a,
        link_counters.outbound_queued.clone(),
    );
    let (node_b, handle_b, mempool_b, exec_b) = build_node(tb, 1, &cluster, turn_b, Arc::default());
    spawn_link_quality_poller(ta, node_a.get_cancel_token());

    let (tx_a, tx_rx_a) = TxContext::new(&cluster, exec_a);
    let (tx_b, _tx_rx_b) = TxContext::new(&cluster, exec_b);
    spawn_radio_stats_sampler(link_counters, &tx_a);
    println!(
        "genesis account (funded {GENESIS_BALANCE}): {}",
        hex::encode(cluster.genesis_pubkey())
    );

    // RPC submits into node 0; mempool gossip converges both nodes before packing.
    let mempool_slot: MempoolSlot = Arc::new(tokio::sync::RwLock::new(Some(mempool_a.clone())));
    spawn_tx_bridge(tx_rx_a, mempool_slot);
    spawn_mempool_maintenance(mempool_a.clone(), node_a.get_cancel_token());
    spawn_mempool_maintenance(mempool_b.clone(), node_b.get_cancel_token());
    spawn_block_executor(
        "node0".into(),
        node_a.get_blockstore(),
        node_a.get_pool(),
        tx_a.clone(),
        mempool_a,
        node_a.get_cancel_token(),
    );
    spawn_block_executor(
        "node1".into(),
        node_b.get_blockstore(),
        node_b.get_pool(),
        tx_b,
        mempool_b,
        node_b.get_cancel_token(),
    );

    if rpc {
        spawn_rpc(node_a.get_blockstore(), &tx_a);
    }

    let until = deadline_for(duration);
    let a = tokio::spawn(async move { run_node("node0", node_a, handle_a, until, None).await });
    let b = tokio::spawn(async move { run_node("node1", node_b, handle_b, until, None).await });

    let (slot_a, slot_b) = (a.await?.0, b.await?.0);
    println!("=== done: node0 finalized {slot_a}, node1 finalized {slot_b} ===");
    // Zero progress is only an error for bounded runs.
    if slot_a == 0 && slot_b == 0 && duration.is_some() {
        anyhow::bail!("no slots finalized — consensus did not make progress");
    }
    Ok(())
}

/// Bring up the modem and establish one caller/listener PACTOR session.
async fn establish_link(
    init_cfg: &PactorInitConfig,
    is_caller: bool,
    peercall: &str,
    connect_attempts: u32,
    full_init: bool,
) -> anyhow::Result<UsbPactorTransport> {
    // Reconnects use light init because modem config persists across STBY.
    let transport = if full_init {
        println!("bringing up modem (full init) ...");
        init_modem(init_cfg).await?
    } else {
        println!("bringing up modem (fast reconnect) ...");
        light_init_modem(init_cfg).await?
    };
    if is_caller {
        println!("connecting to {peercall} ...");
        connect_with_retries(&transport, peercall, connect_attempts).await?;
    } else {
        println!("listening for incoming connection (up to 300s) ...");
        transport
            .accept_incoming(Some(Duration::from_secs(300)))
            .await?;
    }

    // Actively keepalive during health check; a passive post-connect wait can
    // let an idle ARQ link time out before consensus has traffic.
    println!("verifying link holds ...");
    let health_deadline = tokio::time::Instant::now() + LINK_HEALTH_WINDOW;
    while tokio::time::Instant::now() < health_deadline {
        if !transport.is_link_up() {
            anyhow::bail!("link collapsed during post-connect health check");
        }
        // Minimal mux keepalive line; the peer drops tag 0xFE.
        let _ = transport.write_data(&[0x00, 0x00, 0x00, 0x01, 0xFE]).await;
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    println!("link established; starting consensus");
    Ok(transport)
}

/// Post-connect hold time before starting consensus.
const LINK_HEALTH_WINDOW: Duration = Duration::from_secs(10);

/// Run one hardware node, reconnecting across link drops until the budget ends.
/// Finalized consensus state persists in RocksDB across sessions.
async fn run_hardware(
    args: &Args,
    cluster: Cluster,
    duration: Option<Duration>,
) -> anyhow::Result<()> {
    spawn_shutdown_watcher();
    let own_id = args
        .node
        .ok_or_else(|| anyhow::anyhow!("hardware mode requires --node 0|1"))?;
    let port = args
        .port
        .clone()
        .ok_or_else(|| anyhow::anyhow!("hardware mode requires --port <dev>"))?;
    anyhow::ensure!(own_id < 2, "--node must be 0 or 1");
    let is_caller = own_id == 0;
    let label = format!("node{own_id}");

    // Set radio timing before any consensus timers exist; default 6x fits slow
    // half-duplex dissemination/voting before timeouts.
    let delta_mult = args.delta_mult.unwrap_or(6.0);
    // SAFETY: set at startup before any consensus task / timer reads it.
    unsafe {
        std::env::set_var("BUNKER_DELTA_MULT", delta_mult.to_string());
        // Defer slow-path final votes so fast-finalized slots avoid reverse-path traffic.
        std::env::set_var("BUNKER_DEFER_FINAL_VOTE", "1");
    }
    println!(
        "=== hardware node {own_id} ({}) over {port} | delta_mult={delta_mult} ===",
        args.mycall
    );

    let mut init_cfg = PactorInitConfig::new(port, args.mycall.clone());
    init_cfg.baud = args.baud;
    init_cfg.frequency = args.frequency;
    init_cfg.reset = args.reset;
    init_cfg.listen = !is_caller;

    // RPC transaction plumbing persists across reconnects; the live mempool is
    // swapped per session through `mempool_slot`.
    let (base_tx, tx_rx) = TxContext::new(
        &cluster,
        Arc::new(tokio::sync::RwLock::new(cluster.genesis_state())),
    );
    println!(
        "genesis account (funded {GENESIS_BALANCE}): {}",
        hex::encode(cluster.genesis_pubkey())
    );
    let mempool_slot: MempoolSlot = Arc::new(tokio::sync::RwLock::new(None));
    spawn_tx_bridge(tx_rx, mempool_slot.clone());

    // Radio counters span reconnects so `/radio` reports one process stream.
    let link_counters = Arc::new(LinkCounters::default());
    spawn_radio_stats_sampler(link_counters.clone(), &base_tx);

    let overall_deadline = deadline_for(duration);
    let mut highest = 0u64;
    let mut session = 0u32;

    while tokio::time::Instant::now() < overall_deadline && !shutdown_requested() {
        session += 1;
        if session > 1 {
            println!("[{label}] reconnecting (session {session}) ...");
            // Tune only on first bring-up; later reconnects keep the TRX frequency.
            init_cfg.frequency = None;
        }

        let transport = match establish_link(
            &init_cfg,
            is_caller,
            &args.peercall,
            args.connect_attempts,
            session == 1,
        )
        .await
        {
            Ok(t) => t,
            Err(e) => {
                if shutdown_requested() {
                    break;
                }
                eprintln!("[{label}] link bring-up failed: {e}; retrying ...");
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        // Avoid active hostmode status polling; it can interleave with mux writes
        // and desync the modem. Passive events plus watchdog counters are enough.
        let transport: Arc<dyn PactorTransport> = Arc::new(CountingTransport {
            inner: Arc::new(transport),
            counters: link_counters.clone(),
            consecutive_write_failures: AtomicU64::new(0),
            last_rx: std::sync::Mutex::new(Instant::now()),
        });
        let (node, handle, mempool, exec) = build_node(
            transport.clone(),
            own_id,
            &cluster,
            Some(is_caller),
            link_counters.outbound_queued.clone(),
        );
        spawn_link_quality_poller(transport.clone(), node.get_cancel_token());

        // Publish the live mempool for the long-lived RPC bridge.
        *mempool_slot.write().await = Some(mempool.clone());

        // Rebuild execution by replaying persisted finalized blocks into fresh genesis.
        let session_tx = base_tx.with_execution_state(exec);
        spawn_mempool_maintenance(mempool.clone(), node.get_cancel_token());
        spawn_block_executor(
            label.clone(),
            node.get_blockstore(),
            node.get_pool(),
            session_tx.clone(),
            mempool,
            node.get_cancel_token(),
        );

        // Session-scoped RPC must drop its blockstore Arc before the next reconnect.
        let rpc_task = if args.rpc {
            Some(tokio::spawn(rpc::run_api(rpc_state_for(
                node.get_blockstore(),
                &session_tx,
            ))))
        } else {
            None
        };
        if rpc_task.is_some() {
            println!(
                "[{label}] RPC API on http://127.0.0.1:3001 (try /blocks, POST /transactions)"
            );
        }

        let (slot, stop) = run_node(
            &label,
            node,
            handle,
            overall_deadline,
            Some(transport.clone()),
        )
        .await;
        highest = highest.max(slot);

        // Drop submissions while no session owns a live mempool.
        *mempool_slot.write().await = None;

        // Await aborted RPC so its blockstore Arc releases the RocksDB lock.
        if let Some(t) = rpc_task {
            t.abort();
            let _ = t.await;
        }

        // Drop transport and pause so the serial fd is released before reconnect.
        let _ = transport.disconnect().await;
        drop(transport);
        tokio::time::sleep(Duration::from_secs(3)).await;

        match stop {
            RunStop::Deadline | RunStop::Shutdown => break,
            RunStop::LinkDown => {}
        }
    }

    println!("=== done: node{own_id} finalized {highest} (after {session} session(s)) ===");
    // Zero progress is only an error for bounded runs.
    if highest == 0 && duration.is_some() && !shutdown_requested() {
        anyhow::bail!("no slots finalized — consensus did not make progress");
    }
    Ok(())
}
