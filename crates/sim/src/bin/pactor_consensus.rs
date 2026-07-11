//! Empty-block Alpenglow consensus over a single PACTOR link.
//!
//! Runs the real Alpenglow consensus loop (block production → shred
//! dissemination → Votor voting → certification → finalization) between **two**
//! validators, producing **empty blocks** (no transactions), with all five
//! logical networks multiplexed over **one** half-duplex PACTOR link via
//! [`PactorMux`].
//!
//! ## Modes
//!
//! - `--simulated` — builds *both* nodes in-process over a [`SimulatedPactorPair`]
//!   and runs them against each other. Proves the consensus-over-mux wiring end
//!   to end with no radio (CI-friendly). This is the focus of the current
//!   increment.
//! - `--port <dev> --node <0|1>` — runs *one* node over a real modem. **Modem
//!   bring-up is not yet wired here**; the proven init/connect flow lives in the
//!   `pactor_hw_test` binary and will be factored into a shared module next.
//!
//! Both nodes derive the same 2-validator set from a fixed `--seed` so keys and
//! stake match; they differ only in which `own_id` they run.

use std::sync::Arc;
use std::time::Duration;

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
use bunkerglow::Slot;
use ed25519_dalek::SigningKey;
use std::collections::HashMap;
use bunkerglow::disseminator::rotor::StakeWeightedSampler;
use bunkerglow::disseminator::Rotor;
use bunkerglow::mempool::Mempool;
use bunkerglow::network::dontcare_sockaddr;
use bunkerglow::repair::{RepairRequest, RepairResponse};
use bunkerglow::shredder::Shred;
use bunkerglow::{Transaction, ValidatorInfo};
use clap::Parser;
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use scs_pactor::{
    PactorTransport, SimulatedPactorConfig, SimulatedPactorPair, UsbPactorTransport,
};

/// The five logical networks, each a [`MuxChannel`] sharing one PACTOR link.
type MuxAll2All = MuxChannel<ConsensusMessage, ConsensusMessage>;
type MuxShred = MuxChannel<Shred, Shred>;
type MuxRepair = MuxChannel<RepairRequest, RepairResponse>;
type MuxRepairReq = MuxChannel<RepairResponse, RepairRequest>;
type MuxTxs = MuxChannel<Transaction, Transaction>;

/// Per-node mempool over the Txs mux channel. Shared (`Arc`) between the node
/// (as the producer's `txs_receiver`) and this binary (for `submit`/`evict`).
type NodeMempool = Arc<Mempool<MuxTxs>>;

/// A full Alpenglow node whose networks are all multiplexed over PACTOR. The
/// transactions network is the per-node [`Mempool`], not the raw Txs channel, so
/// the block producer packs from mempool-ordered pending txs.
type Node =
    Alpenglow<TrivialAll2All<MuxAll2All>, Rotor<MuxShred, StakeWeightedSampler>, NodeMempool>;

#[derive(Parser)]
#[command(version, about = "Empty-block Alpenglow consensus over PACTOR", long_about = None)]
struct Args {
    /// Run both nodes in-process over a simulated PACTOR pair (no radio).
    #[arg(long)]
    simulated: bool,

    /// With --simulated, enable half-duplex turn discipline (exercises the
    /// turn-grant/changeover handoff that real PACTOR needs).
    #[arg(long)]
    half_duplex: bool,

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

    /// Consensus timing multiplier (stretches block cadence / timeouts to match
    /// a slow link). Hardware defaults to 6; override for a faster/slower link.
    /// Sets BUNKER_DELTA_MULT before consensus starts.
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

    /// How long to run before shutting down, in seconds. Omit to run
    /// continuously until Ctrl-C (graceful shutdown on signal).
    #[arg(long)]
    duration: Option<u64>,

    /// Serve the HTTP RPC API (same endpoints as the simulation) on
    /// 127.0.0.1:3001, reading blocks from this node's live block store. Query
    /// e.g. `curl localhost:3001/block/slot/4` or `curl localhost:3001/blocks`.
    #[arg(long)]
    rpc: bool,

    /// Inspect the persisted chain WITHOUT touching the modem: open this node's
    /// on-disk block store (under the current dir's `data/`) and serve the RPC API
    /// so `/blocks` etc. are queryable offline. Needs `--node <id>` to pick which
    /// node's store to read. Runs until Ctrl-C (or `--duration`).
    #[arg(long)]
    inspect: bool,
}

/// Block-store handle type shared between a node and the RPC server.
type SharedBlockstore =
    Arc<tokio::sync::RwLock<Box<dyn bunkerglow::consensus::Blockstore + Send + Sync>>>;

/// The shared RPC surfaces threaded through the node: transaction submission,
/// mempool, execution state, and per-tx results. Constructed once per node and
/// handed to both the RPC server and the finalized-block executor so the API
/// reflects real transaction processing.
#[derive(Clone)]
struct TxContext {
    /// Sender the RPC `/submit` handler pushes client transactions onto; drained
    /// by the tx-bridge task, which injects them into consensus.
    tx_sender: tokio::sync::mpsc::UnboundedSender<CoreTransaction>,
    /// Genesis-funded execution state, updated as finalized blocks are executed.
    execution_state: Arc<tokio::sync::RwLock<ExecutionState>>,
    /// Pending client transactions awaiting inclusion (for the RPC mempool view).
    mempool: Arc<tokio::sync::RwLock<Vec<rpc::MempoolEntry>>>,
    /// Finalized/failed outcome per tx hash, populated by the executor.
    tx_results: Arc<tokio::sync::RwLock<HashMap<String, rpc::TxResult>>>,
    /// Genesis ed25519 key, so the RPC can server-side-sign transactions whose
    /// sender is the genesis account (submit with an all-zero signature).
    genesis_signing_key: Arc<SigningKey>,
    /// Validator statuses served by `/nodes`, updated by the block executor.
    /// In this two-node setup finalization requires both validators' votes, so
    /// the local finalized frontier is (modulo reverse-path lag) the network's.
    nodes: Arc<tokio::sync::RwLock<Vec<rpc::NodeStatus>>>,
}

impl TxContext {
    /// Build a fresh context around this node's genesis-funded execution state.
    /// The returned `UnboundedReceiver` is consumed by the tx-bridge task.
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
            genesis_signing_key: Arc::new(cluster.genesis_key.clone()),
            // Both validators of the two-node cluster, so `/nodes` is populated
            // from startup (finalized_slot advances via the block executor).
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
        };
        (ctx, tx_rx)
    }

    /// Rebind this context to a new session's execution state, keeping the same
    /// `tx_sender`, mempool, tx_results, and genesis key. Used on the hardware
    /// path, where each reconnect builds a fresh (genesis) execution state that
    /// the block executor rebuilds by replaying the persisted finalized chain,
    /// while the client-facing mempool/results persist across the drop.
    fn with_execution_state(
        &self,
        execution_state: Arc<tokio::sync::RwLock<ExecutionState>>,
    ) -> Self {
        TxContext {
            tx_sender: self.tx_sender.clone(),
            execution_state,
            mempool: self.mempool.clone(),
            tx_results: self.tx_results.clone(),
            genesis_signing_key: self.genesis_signing_key.clone(),
            nodes: self.nodes.clone(),
        }
    }
}

/// Build an RPC [`SharedState`](rpc::SharedState) backed by `blockstore` and the
/// node's live transaction context.
///
/// Reuses `rpc::run_api` (the exact server the simulations expose) so the chain
/// AND its transaction state are queryable through the same endpoints —
/// `/blocks`, `/block/slot/{n}`, `/transactions` (submit), `/account/{pk}`,
/// `/tx/{hash}`, `/ws`, … Transactions submitted here are injected into
/// consensus, packed into blocks, and executed on finalization (see
/// [`spawn_tx_bridge`] and [`spawn_block_executor`]).
fn rpc_state_for(blockstore: SharedBlockstore, tx: &TxContext) -> rpc::SharedState {
    let (updates_tx, _updates_rx) = tokio::sync::broadcast::channel(256);
    rpc::SharedState {
        blocks: Arc::new(tokio::sync::RwLock::new(Vec::new())),
        nodes: tx.nodes.clone(),
        radio_stats: Arc::new(tokio::sync::RwLock::new(rpc::RadioStats {
            bandwidth_bps: 0,
            packet_loss_percent: 0.0,
            latency_ms: 0,
            jitter_ms: 0,
            packets_sent: 0,
            packets_dropped: 0,
            current_throughput_bps: 0.0,
        })),
        updates: updates_tx,
        blockstore: Some(blockstore),
        mempool: tx.mempool.clone(),
        tx_sender: Some(tx.tx_sender.clone()),
        execution_state: tx.execution_state.clone(),
        tx_results: tx.tx_results.clone(),
        genesis_signing_key: Some(tx.genesis_signing_key.clone()),
        snapshot_store: None,
    }
}

/// Spawn the RPC server over `blockstore` + `tx` context (used by the simulated,
/// never-reconnect path; the hardware path spawns/aborts it per session inline).
fn spawn_rpc(blockstore: SharedBlockstore, tx: &TxContext) {
    println!("RPC API serving on http://127.0.0.1:3001 (try /blocks, POST /transactions)");
    tokio::spawn(rpc::run_api(rpc_state_for(blockstore, tx)));
}

/// A slot holding the *current* per-node mempool. On the hardware path the
/// mempool is rebuilt every reconnect (each session has a fresh mux underneath),
/// so the long-lived bridge task submits into the live mempool read from here.
/// `None` while the link is down between sessions — submissions are dropped from
/// the bridge, but a tx already admitted to a prior session's mempool that has
/// not yet finalized will not survive the reconnect; the client can resubmit.
type MempoolSlot = Arc<tokio::sync::RwLock<Option<NodeMempool>>>;

/// Drain client transactions from the RPC (`tx_rx`) and submit them into this
/// node's mempool. The mempool owns dedup, per-sender nonce/fee ordering,
/// gossip to the peer, and eviction on finalization — so the bridge only has to
/// encode each `CoreTransaction` to its `bunkerglow::Transaction` wire form and
/// hand it over.
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

/// Log the outcome of a mempool submission at debug level.
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

/// Spawn a task that periodically returns long-in-flight mempool transactions to
/// the pending set, so a tx packed into a slot that never finalized (e.g. a lost
/// shred or a band drop) is re-packed rather than stuck. Runs until `cancel`.
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

/// Spawn a task that executes the transactions of newly finalized blocks into
/// the shared execution state, recording per-tx results and pruning the mempool.
///
/// Polls the node's finalized frontier; for each newly finalized slot it reads
/// the canonical block from `blockstore`, decodes its transactions (with the
/// same raw / 8-byte-length-prefix fallback the UDP sim uses), applies them via
/// `State::execute_block`, and records a [`rpc::TxResult`] per tx so `/tx/{hash}`
/// and `/account/{pk}` reflect the outcome. Both nodes run this over identical
/// genesis + identical finalized blocks, so their state stays in agreement.
///
/// Returns immediately; the task runs until `cancel` is cancelled.
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
            if finalized <= last_executed {
                continue;
            }

            // Advance `/nodes` to the new finalized frontier. Finalizing a slot
            // in this two-node cluster requires both validators' votes, so both
            // entries track the locally observed frontier.
            for node in tx.nodes.write().await.iter_mut() {
                node.finalized_slot = finalized;
            }

            let bs = blockstore.read().await;
            for slot in (last_executed + 1)..=finalized {
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
                // Evict this block's txs from the mempool now that they are
                // finalized (whether they execute ok or fail — they will never be
                // valid to re-pack). Uses the raw wire txs so the hashes match
                // what the mempool admitted.
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
            last_executed = finalized;
        }
    });
}

/// Decode a finalized block's raw transactions into `CoreTransaction`s. A block
/// transaction is the bincode encoding of a `CoreTransaction`, possibly wrapped
/// with a wincode 8-byte length prefix; try the raw bytes first, then skip the
/// prefix. Undecodable entries are dropped (they were never valid client txs).
fn decode_block_txs(raw: &[Transaction]) -> Vec<CoreTransaction> {
    raw.iter()
        .filter_map(|t| {
            let data = &t.0;
            bincode::serde::decode_from_slice(data, bincode::config::standard())
                .or_else(|_| {
                    if data.len() > 8 {
                        bincode::serde::decode_from_slice(&data[8..], bincode::config::standard())
                    } else {
                        Err(bincode::error::DecodeError::Other("too short"))
                    }
                })
                .ok()
                .map(|(tx, _)| tx)
        })
        .collect()
}

/// Record execution results for one finalized slot: insert a [`rpc::TxResult`]
/// per tx (so `/tx/{hash}` resolves) and prune those txs from the RPC mempool.
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
    let mut mempool = tx.mempool.write().await;
    let mut results_map = tx.tx_results.write().await;
    for (core_tx, exec_result) in core_txs.iter().zip(results.iter()) {
        let hash = hex::encode(core_tx.hash());
        let (status, error) = match exec_result {
            Ok(()) => (rpc::TxFinalStatus::Finalized, None),
            Err(e) => (rpc::TxFinalStatus::Failed, Some(e.to_string())),
        };
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
        mempool.retain(|e| e.hash != hash);
    }
}

/// Native balance credited to the genesis account at startup, so
/// client-submitted transfers have funds to move. Large enough for many
/// transfers-plus-fees over a long run.
const GENESIS_BALANCE: u64 = 1_000_000_000_000;

/// Deterministically generated keys + public validator info for the 2-node set.
struct Cluster {
    secret_keys: Vec<SecretKey>,
    voting_keys: Vec<aggsig::SecretKey>,
    validators: Vec<ValidatorInfo>,
    /// Genesis ed25519 key that funds and signs client transactions. Derived
    /// from the same `--seed` on both nodes, so both fund the identical account
    /// and execute the same transactions to the same state.
    genesis_key: SigningKey,
}

impl Cluster {
    /// Public key of the genesis (funded) account.
    fn genesis_pubkey(&self) -> [u8; 32] {
        self.genesis_key.verifying_key().to_bytes()
    }

    /// Fresh execution state with the genesis account funded. Both nodes build
    /// an identical genesis, then apply the same finalized transactions, so
    /// their execution state stays in agreement without gossiping state.
    fn genesis_state(&self) -> ExecutionState {
        let mut state = ExecutionState::new();
        state.get_or_create_account(&self.genesis_pubkey()).native_balance = GENESIS_BALANCE;
        state
    }
}

/// Build the fixed 2-validator set from `seed`. Both machines call this with the
/// same seed and obtain identical keys/stake, so consensus agrees on membership.
/// Per-channel `SocketAddr`s are ignored by the mux (single peer), so they are
/// filled with a don't-care address.
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
    // Genesis key derived from the same RNG stream (after the validator keys),
    // so a given --seed always yields the same funded/ signing account on both
    // nodes. Build from 32 raw bytes to avoid a rand_core version dependency.
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

/// Wire one Alpenglow node over a connected PACTOR transport, multiplexing its
/// five logical networks across the single link. Returns the node and the mux
/// handle (kept alive so the reader/writer tasks run; used for shutdown).
///
/// `turn`: `None` for a full-duplex transport (simulator — write freely);
/// `Some(starts_with_turn)` for a real half-duplex PACTOR link, where exactly one
/// side (the caller/master) must start with the transmit turn.
fn build_node(
    transport: Arc<dyn PactorTransport>,
    own_id: u64,
    cluster: &Cluster,
    turn: Option<bool>,
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
    // All2All votes/certs are broadcast to all validators including self; over a
    // single link there is no socket loopback, so self-deliver them or a node
    // never counts its own vote toward the finalization quorum.
    let all2all_net: MuxAll2All = mux.channel_self_delivering(Channel::All2All);
    let shred_net: MuxShred = mux.channel(Channel::Disseminator);
    let repair_net: MuxRepair = mux.channel(Channel::Repair);
    let repair_req_net: MuxRepairReq = mux.channel(Channel::RepairRequest);
    // Txs channel: NOT self-delivering. The per-node mempool provides the local
    // path — a locally-submitted tx is admitted straight into this node's pool
    // (and the producer packs from the pool), so it never needs to loop back
    // through the channel. The channel carries only peer gossip, which the
    // mempool's admit loop reads.
    let txs_net: MuxTxs = mux.channel(Channel::Txs);
    let handle = mux.spawn();

    // Per-node mempool wrapping the Txs channel. Over a single mux link there is
    // one peer, addressed by a placeholder (the mux ignores the address); the
    // mempool gossips each newly-admitted tx to it and its admit loop admits +
    // re-gossips inbound peer txs, so both nodes' mempools converge.
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

    // Genesis-funded execution state, shared with the RPC server (balances,
    // accounts) and the finalized-block executor below. Both nodes start from an
    // identical genesis and apply the same finalized transactions, so their
    // execution state stays in lockstep without exchanging state.
    let execution_state = Arc::new(tokio::sync::RwLock::new(cluster.genesis_state()));
    node.set_execution_state(execution_state.clone());

    // On a real half-duplex link, feed Votor the mux's keepalive-driven liveness
    // so its crashed-leader timeout PAUSES (re-arms) while the link is up but the
    // reverse path is slow, instead of irreversibly skipping the window and
    // jumping ahead. The full-duplex simulator keeps the default always-alive
    // behavior (turn is None), so simulated runs are unchanged.
    if turn.is_some() {
        node.set_link_liveness(handle.liveness());
    }

    (node, handle, mempool, execution_state)
}

/// Process-wide "stop now" flag, set by the Ctrl-C watcher (see
/// [`spawn_shutdown_watcher`]). The hardware reconnect loop and `run_node`'s poll
/// loop both honor it so a continuous (`--duration`-less) run ends promptly and
/// cleanly on Ctrl-C — tearing down consensus and releasing the modem/DB.
static SHUTDOWN: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn shutdown_requested() -> bool {
    SHUTDOWN.load(std::sync::atomic::Ordering::Relaxed)
}

/// Spawn a task that flips [`SHUTDOWN`] on the first Ctrl-C. Idempotent enough for
/// our use: started once per run function before the work begins.
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
    /// The `until` deadline elapsed.
    Deadline,
    /// The PACTOR link dropped mid-session (caller may reconnect).
    LinkDown,
    /// Ctrl-C requested a graceful shutdown.
    Shutdown,
}

/// Drive a node until `until`, polling its finalized slot for progress and (if
/// `link` is given) watching for a mid-session link drop.
///
/// `Alpenglow::run` consumes the node, so we grab the pool handle (a cheap `Arc`
/// clone) *before* moving the node into the run task, then poll it. Returns the
/// highest finalized slot reached and why it stopped.
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

    // Stop consensus, then tear the mux down so the transport (and its serial
    // fd) can be released for a reconnect.
    //
    // Order matters: cancel consensus and let `run()` fully wind down BEFORE
    // aborting the mux reader. `run()` aborts its internal loops and returns,
    // dropping the `Arc<Alpenglow>` it owns — which is what releases the RocksDB
    // blockstore/pool handles the NEXT session must re-open. We must therefore
    // *await run() to completion*, not abandon it on a short timeout: a premature
    // timeout leaves `run()`'s task (and its blockstore Arc) alive, so the next
    // session's `DB::open` hits "lock held by current process" and panics.
    // A generous cap still bounds a pathologically stuck teardown.
    cancel.cancel();
    match tokio::time::timeout(Duration::from_secs(15), run_task).await {
        Ok(_) => {}
        Err(_) => eprintln!("[{label}] consensus teardown did not finish within 15s"),
    }
    handle.shutdown();
    drop(handle);
    (highest, stop)
}

/// Suppress only the expected teardown panic from orphaned consensus tasks.
///
/// On every reconnect, the discarded node's detached repair/consensus tasks
/// (which loop on `receive().unwrap()` with no cancellation) terminate by
/// panicking when the mux closes their queue — this is how they release their
/// RocksDB handles for the next session, so it's deliberate and non-fatal. It is
/// also noisy on a flaky link that reconnects often. Install a panic hook that
/// drops *only* that specific message and delegates everything else to the
/// default hook, so real panics still surface with full backtraces.
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
    // `None` => run continuously until Ctrl-C; `Some` => stop after that long.
    let duration = args.duration.map(Duration::from_secs);

    if args.inspect {
        run_inspect(&args, cluster, duration).await
    } else if args.simulated {
        run_simulated(cluster, duration, args.half_duplex, args.rpc).await
    } else {
        run_hardware(&args, cluster, duration).await
    }
}

/// Resolves a run budget to an instant to stop at: the deadline if a finite
/// `--duration` was given, or `Ctrl-C` if not. Returns a future that completes
/// when the run should end, so callers can `select!` consensus work against it.
async fn run_until(duration: Option<Duration>) {
    match duration {
        Some(d) => tokio::time::sleep(d).await,
        None => {
            let _ = tokio::signal::ctrl_c().await;
            println!("\n=== Ctrl-C received; shutting down gracefully ===");
        }
    }
}

/// A far-future instant used as the per-session deadline when running
/// continuously (no `--duration`). The run actually stops on Ctrl-C via
/// [`run_until`]; this just keeps per-session timing math (which expects an
/// `Instant` deadline) well-defined without special-casing every call site.
fn deadline_for(duration: Option<Duration>) -> tokio::time::Instant {
    let now = tokio::time::Instant::now();
    match duration {
        Some(d) => now + d,
        // ~10 years; the Ctrl-C path ends the run long before this.
        None => now + Duration::from_secs(10 * 365 * 24 * 3600),
    }
}

/// Serve the RPC API over a node's persisted on-disk block store, without any
/// modem or consensus. Lets you inspect the finalized chain (`/blocks`, etc.)
/// after a run has ended — the chain lives in RocksDB under the current dir's
/// `data/`, so run this from the same dir the node ran in (e.g. `/tmp/bc-node0`).
async fn run_inspect(
    args: &Args,
    cluster: Cluster,
    duration: Option<Duration>,
) -> anyhow::Result<()> {
    let own_id = args
        .node
        .ok_or_else(|| anyhow::anyhow!("--inspect requires --node <id> to pick the block store"))?;

    // A blockstore needs a votor channel, but in inspect mode nothing consumes it;
    // a dropped receiver is fine (the store performs no sends during pure reads).
    let (votor_tx, _votor_rx) = tokio::sync::mpsc::channel(1);
    let epoch_info = Arc::new(EpochInfo::new(0, own_id, cluster.validators.clone()));
    let blockstore: SharedBlockstore = Arc::new(tokio::sync::RwLock::new(Box::new(
        bunkerglow::consensus::BlockstoreImpl::new(epoch_info, votor_tx),
    )));

    println!(
        "=== inspect: serving node {own_id}'s on-disk chain (data/blockstore/{own_id}) ===\n\
         RPC API on http://127.0.0.1:3001 — try /blocks or /block/slot/1"
    );
    // Inspect is offline: no consensus, so no injection or live execution. Build
    // a context around a genesis-funded state (so `/account` shows the genesis
    // balance) with the tx-bridge receiver dropped — `/submit` is a no-op here.
    let exec = Arc::new(tokio::sync::RwLock::new(cluster.genesis_state()));
    let (tx_ctx, _tx_rx) = TxContext::new(&cluster, exec);
    spawn_rpc(blockstore, &tx_ctx);

    // Stay up so the endpoint is reachable; honor --duration as an auto-exit, or
    // run until Ctrl-C when no duration was given.
    run_until(duration).await;
    Ok(())
}

/// Build both nodes over a simulated PACTOR pair and run them against each other.
///
/// `half_duplex`: when true, enable the mux turn discipline (node 0 starts with
/// the turn) even though the simulator is full-duplex underneath. This exercises
/// the turn-grant/changeover handoff path that real PACTOR needs, catching
/// integration bugs before going on-air.
async fn run_simulated(
    cluster: Cluster,
    duration: Option<Duration>,
    half_duplex: bool,
    rpc: bool,
) -> anyhow::Result<()> {
    spawn_shutdown_watcher();
    println!(
        "=== simulated 2-node empty-block consensus over PACTOR mux ({}) ===",
        if half_duplex {
            "half-duplex turns"
        } else {
            "full-duplex"
        }
    );
    // The half-duplex sim faithfully models the slow reverse path, so exercise the
    // same reverse-path optimization the hardware path uses: defer the slow-path
    // finalization vote so a fast-finalized slot sends nothing extra back.
    if half_duplex {
        // SAFETY: set before any node / Votor is built below.
        unsafe {
            std::env::set_var("BUNKER_DEFER_FINAL_VOTE", "1");
        }
    }
    // Two link models:
    // - Full-duplex: a clean, symmetric, independent-direction link. Validates the
    //   consensus-over-mux wiring without HF physics (the original sim behavior).
    // - Half-duplex: ONE shared channel (only one side transmits at a time) with an
    //   ARQ changeover cost and a ~10× slower reverse (slave→master) path — the
    //   faithful model that reproduces the on-air "stall after a few slots".
    // Both are lossless (real PACTOR does ARQ in hardware) with no read timeout (a
    // long-lived node parks on idle reads).
    let config = if half_duplex {
        SimulatedPactorConfig {
            packet_loss: 0.0,
            latency_jitter: Duration::ZERO,
            setup_delay: Duration::ZERO,
            forced_initial_losses: 0,
            fade_windows: Vec::new(),
            read_timeout: None,
            ..SimulatedPactorConfig::half_duplex_hf()
        }
    } else {
        SimulatedPactorConfig {
            packet_loss: 0.0,
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

    // Establish the simulated link before wiring consensus: the simulated
    // transport rejects writes until connected. Node 0 calls node 1.
    ta.set_mycall("NODE0").await?;
    tb.set_mycall("NODE1").await?;
    tb.accept_incoming(None).await.ok();
    ta.connect_peer("NODE1").await?;

    let ta: Arc<dyn PactorTransport> = Arc::new(ta);
    let tb: Arc<dyn PactorTransport> = Arc::new(tb);

    // Full-duplex: no turn discipline (None). Half-duplex: node 0 starts with
    // the turn, node 1 without.
    let turn_a = half_duplex.then_some(true);
    let turn_b = half_duplex.then_some(false);
    let (node_a, handle_a, mempool_a, exec_a) = build_node(ta, 0, &cluster, turn_a);
    let (node_b, handle_b, mempool_b, exec_b) = build_node(tb, 1, &cluster, turn_b);

    // Per-node transaction contexts (each over its own genesis-funded state).
    let (tx_a, tx_rx_a) = TxContext::new(&cluster, exec_a);
    let (tx_b, _tx_rx_b) = TxContext::new(&cluster, exec_b);
    println!(
        "genesis account (funded {GENESIS_BALANCE}): {}",
        hex::encode(cluster.genesis_pubkey())
    );

    // Node 0's RPC feeds its mempool; the mempool gossips each tx to node 1, so
    // both nodes' mempools converge and whichever leads a slot packs from its own
    // pool. Both nodes execute finalized blocks into their own state (identical
    // genesis + blocks ⇒ identical state) and evict the finalized txs. The
    // simulated link never drops, so node 0's mempool stays live for the run.
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

    // Optional RPC over node 0's block store + transaction context.
    if rpc {
        spawn_rpc(node_a.get_blockstore(), &tx_a);
    }

    let until = deadline_for(duration);
    // Simulated link never "drops" (full-duplex), so no link watch / reconnect.
    let a = tokio::spawn(async move { run_node("node0", node_a, handle_a, until, None).await });
    let b = tokio::spawn(async move { run_node("node1", node_b, handle_b, until, None).await });

    let (slot_a, slot_b) = (a.await?.0, b.await?.0);
    println!("=== done: node0 finalized {slot_a}, node1 finalized {slot_b} ===");
    // Only treat zero progress as an error for a bounded run; a continuous run
    // stopped by Ctrl-C may legitimately be ended before the first finalization.
    if slot_a == 0 && slot_b == 0 && duration.is_some() {
        anyhow::bail!("no slots finalized — consensus did not make progress");
    }
    Ok(())
}

/// Bring the modem up and establish the PACTOR link for one session.
///
/// Node 0 calls (`connect_peer`); node 1 listens (`LISTEN 1` + `accept_incoming`).
/// Returns a connected transport ready for consensus.
async fn establish_link(
    init_cfg: &PactorInitConfig,
    is_caller: bool,
    peercall: &str,
    connect_attempts: u32,
    full_init: bool,
) -> anyhow::Result<UsbPactorTransport> {
    // First session does the full bring-up; reconnects use the fast path (modem
    // config persists across a STBY drop), reclaiming most of each band window.
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

    // Post-connect health check: on a marginal band the ARQ link often forms but
    // collapses within seconds (the modem returns to the `cmd:` prompt / STBY).
    // Confirm the link actually holds for a short window BEFORE declaring it up
    // and starting consensus — otherwise we'd start a node on a dead link and the
    // run would stall/exit. If it drops here, return an error so the caller's
    // reconnect loop retries rather than proceeding.
    //
    // Crucially this is an ACTIVE check: it sends keepalive lines throughout. The
    // ARQ link drops to STBY after ~43s of no traffic, and right after connect
    // neither side has consensus data yet (mid inter-block window), so a passive
    // wait would let the very link we're verifying time out. The keepalive byte
    // (0xFE) is the same tag the peer's mux reader ignores.
    println!("verifying link holds ...");
    let health_deadline = tokio::time::Instant::now() + LINK_HEALTH_WINDOW;
    while tokio::time::Instant::now() < health_deadline {
        if !transport.is_link_up() {
            anyhow::bail!("link collapsed during post-connect health check");
        }
        // `#<msgid:00000001><tag:fe>\r` — a minimal keepalive line the peer drops.
        let _ = transport.write_data(&[0x00, 0x00, 0x00, 0x01, 0xFE]).await;
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    println!("link established; starting consensus");
    Ok(transport)
}

/// How long the link must stay up after connect before we trust it and start
/// consensus. Longer than the few-seconds-to-`cmd:` collapse seen on marginal
/// bands, short enough not to waste a good window.
const LINK_HEALTH_WINDOW: Duration = Duration::from_secs(10);

/// Run one node over a real modem on this machine, **reconnecting across link
/// drops** until the total `--duration` budget is spent.
///
/// Consensus state (finalized slot, blocks) persists in the on-disk RocksDB pool
/// / blockstore, so a node rebuilt after a reconnect resumes from where it left
/// off — a marginal-band drop becomes a recoverable pause, not a restart.
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

    // Stretch consensus timing to match the slow half-duplex link, BEFORE any
    // node (and thus any timer) is built. Without this, blocks are produced
    // faster than the link can disseminate+vote+certify them, so consensus times
    // out past the first slot. Default 6x for radio (delta_first_slice = 180s,
    // at/above the ~180s reverse-path read stall, so the crashed-leader timeout
    // does not fire before a stalled first shred can arrive); the pause-on-alive
    // logic rides out longer quiets. --delta-mult overrides.
    let delta_mult = args.delta_mult.unwrap_or(6.0);
    // SAFETY: set at startup before any consensus task / timer reads it.
    unsafe {
        std::env::set_var("BUNKER_DELTA_MULT", delta_mult.to_string());
        // Over the slow half-duplex link, defer the slow-path finalization vote so
        // a slot that fast-finalizes (both notar votes meet the 80% strong quorum)
        // never sends the final vote / notar cert / final cert back over the
        // expensive reverse path. Falls back to slow-final if fast-final does not
        // fire in time. See `Votor::defer_final_vote`.
        std::env::set_var("BUNKER_DEFER_FINAL_VOTE", "1");
    }
    println!(
        "=== hardware node {own_id} ({}) over {port} | delta_mult={delta_mult} ===",
        args.mycall
    );

    let mut init_cfg = PactorInitConfig::new(port, args.mycall.clone());
    init_cfg.baud = args.baud;
    init_cfg.frequency = args.frequency;
    // Force-disconnect stale link state on every (re)connect attempt.
    init_cfg.reset = args.reset;
    // The listener must enable LISTEN 1 to accept the incoming connect.
    init_cfg.listen = !is_caller;

    // Persistent transaction plumbing, created once and shared across reconnects:
    // per-tx results, genesis key, and the long-lived tx-bridge. The mempool
    // itself is per-session (each reconnect builds a fresh mux underneath), so the
    // bridge submits into the live mempool read from `mempool_slot`, which each
    // session repopulates. A dummy execution state seeds the context; it is
    // replaced per session with the node's real (genesis) state via
    // `with_execution_state`.
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

    let overall_deadline = deadline_for(duration);
    let mut highest = 0u64;
    let mut session = 0u32;

    while tokio::time::Instant::now() < overall_deadline && !shutdown_requested() {
        session += 1;
        if session > 1 {
            println!("[{label}] reconnecting (session {session}) ...");
            // Only tune the radio on the first bring-up. The TRX is already on
            // frequency for later sessions, and re-tuning right after a STBY drop
            // often returns no confirmation and (wrongly) aborts the reconnect.
            init_cfg.frequency = None;
        }

        // Establish (or re-establish) the link for this session. Full init only
        // on the first session; reconnects use the fast path.
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

        let transport: Arc<dyn PactorTransport> = Arc::new(transport);
        // Half-duplex link: the caller (node 0) starts holding the transmit turn.
        let (node, handle, mempool, exec) =
            build_node(transport.clone(), own_id, &cluster, Some(is_caller));

        // Publish this session's mempool so the bridge submits over the new mux.
        *mempool_slot.write().await = Some(mempool.clone());

        // This session's transaction context: same client-facing mempool/results
        // as prior sessions, but this session's fresh (genesis) execution state.
        // The executor rebuilds that state by replaying the persisted finalized
        // chain from slot 1 (deterministic, so a reconnect resumes seamlessly).
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

        // Optional RPC over THIS session's block store + transaction context.
        // Spawned per session and aborted before teardown below, so each
        // reconnect's fresh RocksDB handle is the one being served and the DB
        // lock is released for the next session. Block queries still see all
        // finalized slots — the chain persists on disk and a rebuilt node resumes
        // from it.
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

        let (slot, stop) =
            run_node(&label, node, handle, overall_deadline, Some(transport.clone())).await;
        highest = highest.max(slot);

        // Link is down for teardown: clear the mempool so the bridge drops
        // submissions until the next session repopulates it.
        *mempool_slot.write().await = None;

        // Stop serving and WAIT for the server task to actually end before the
        // next session re-opens the RocksDB. The RPC `SharedState` holds an `Arc`
        // clone of this session's blockstore; that clone is only dropped once the
        // axum server future is dropped, which happens when the aborted task is
        // awaited. Skipping the await would leave the blockstore locked and the
        // next session's `DB::open` would fail with "lock held by current process".
        if let Some(t) = rpc_task {
            t.abort();
            let _ = t.await;
        }

        // Tell the modem to drop the link, then release the transport so its
        // serial port is freed before the next session re-opens it. Dropping the
        // last `Arc<UsbPactorTransport>` aborts its reader task and closes the fd;
        // the brief sleep gives the OS time to release the device (otherwise the
        // re-open hits "Device or resource busy").
        let _ = transport.disconnect().await;
        drop(transport);
        tokio::time::sleep(Duration::from_secs(3)).await;

        match stop {
            RunStop::Deadline | RunStop::Shutdown => break,
            RunStop::LinkDown => {}
        }
    }

    println!("=== done: node{own_id} finalized {highest} (after {session} session(s)) ===");
    // A bounded run that finalized nothing is a failure; a continuous run ended by
    // Ctrl-C before the first finalization is not.
    if highest == 0 && duration.is_some() && !shutdown_requested() {
        anyhow::bail!("no slots finalized — consensus did not make progress");
    }
    Ok(())
}
