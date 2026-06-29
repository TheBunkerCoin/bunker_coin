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

use bunker_coin_radio::{Channel, MuxChannel, PactorMux, PactorMuxHandle};
use bunker_coin_sim::pactor_init::{
    connect_with_retries, init_modem, light_init_modem, PactorInitConfig,
};
use bunkerglow::all2all::TrivialAll2All;
use bunkerglow::consensus::{Alpenglow, ConsensusMessage, EpochInfo};
use bunkerglow::crypto::aggsig;
use bunkerglow::crypto::signature::SecretKey;
use bunkerglow::disseminator::rotor::StakeWeightedSampler;
use bunkerglow::disseminator::Rotor;
use bunkerglow::network::dontcare_sockaddr;
use bunkerglow::repair::{RepairRequest, RepairResponse};
use bunkerglow::shredder::Shred;
use bunkerglow::{Transaction, ValidatorInfo};
use clap::Parser;
use rand::rngs::StdRng;
use rand::SeedableRng;
use scs_pactor::{
    PactorTransport, SimulatedPactorConfig, SimulatedPactorPair, UsbPactorTransport,
};

/// The five logical networks, each a [`MuxChannel`] sharing one PACTOR link.
type MuxAll2All = MuxChannel<ConsensusMessage, ConsensusMessage>;
type MuxShred = MuxChannel<Shred, Shred>;
type MuxRepair = MuxChannel<RepairRequest, RepairResponse>;
type MuxRepairReq = MuxChannel<RepairResponse, RepairRequest>;
type MuxTxs = MuxChannel<Transaction, Transaction>;

/// A full Alpenglow node whose networks are all multiplexed over PACTOR.
type Node = Alpenglow<TrivialAll2All<MuxAll2All>, Rotor<MuxShred, StakeWeightedSampler>, MuxTxs>;

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

    /// How long to run before shutting down, in seconds.
    #[arg(long, default_value_t = 120)]
    duration: u64,

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

/// Build a minimal RPC [`SharedState`](rpc::SharedState) backed by `blockstore`.
///
/// Reuses `rpc::run_api` (the exact server the simulations expose) so the on-air
/// chain is queryable through the same endpoints — `/block/slot/{n}`, `/blocks`,
/// `/block/{hash}`, `/ws`, … — without a bespoke API. Only `blockstore` is wired
/// (block queries); transaction/account/snapshot endpoints that need a populated
/// execution state are present but empty (this binary produces empty blocks).
fn rpc_state_for(blockstore: SharedBlockstore) -> rpc::SharedState {
    let (updates_tx, _updates_rx) = tokio::sync::broadcast::channel(256);
    rpc::SharedState {
        blocks: Arc::new(tokio::sync::RwLock::new(Vec::new())),
        nodes: Arc::new(tokio::sync::RwLock::new(Vec::new())),
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
        mempool: Arc::new(tokio::sync::RwLock::new(Vec::new())),
        tx_sender: None,
        execution_state: Arc::new(tokio::sync::RwLock::new(Default::default())),
        tx_results: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        genesis_signing_key: None,
        snapshot_store: None,
    }
}

/// Spawn the RPC server over `blockstore` (used by the simulated, never-reconnect
/// path; the hardware path spawns/aborts it per session inline).
fn spawn_rpc(blockstore: SharedBlockstore) {
    println!("RPC API serving on http://127.0.0.1:3001 (try /block/slot/1, /blocks)");
    tokio::spawn(rpc::run_api(rpc_state_for(blockstore)));
}

/// Deterministically generated keys + public validator info for the 2-node set.
struct Cluster {
    secret_keys: Vec<SecretKey>,
    voting_keys: Vec<aggsig::SecretKey>,
    validators: Vec<ValidatorInfo>,
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
    Cluster {
        secret_keys,
        voting_keys,
        validators,
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
) -> (Node, PactorMuxHandle) {
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
    let txs_net: MuxTxs = mux.channel(Channel::Txs);
    let handle = mux.spawn();

    let epoch_info = Arc::new(EpochInfo::new(0, own_id, cluster.validators.clone()));
    let all2all = TrivialAll2All::new(cluster.validators.clone(), all2all_net);
    let disseminator = Rotor::new(shred_net, epoch_info.clone());

    let node = Alpenglow::new(
        cluster.secret_keys[own_id as usize].clone(),
        cluster.voting_keys[own_id as usize].clone(),
        all2all,
        disseminator,
        repair_net,
        repair_req_net,
        epoch_info,
        txs_net,
    );

    // On a real half-duplex link, feed Votor the mux's keepalive-driven liveness
    // so its crashed-leader timeout PAUSES (re-arms) while the link is up but the
    // reverse path is slow, instead of irreversibly skipping the window and
    // jumping ahead. The full-duplex simulator keeps the default always-alive
    // behavior (turn is None), so simulated runs are unchanged.
    if turn.is_some() {
        node.set_link_liveness(handle.liveness());
    }

    (node, handle)
}

/// Why [`run_node`] stopped.
enum RunStop {
    /// The `until` deadline elapsed.
    Deadline,
    /// The PACTOR link dropped mid-session (caller may reconnect).
    LinkDown,
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
    let duration = Duration::from_secs(args.duration);

    if args.inspect {
        run_inspect(&args, cluster, duration).await
    } else if args.simulated {
        run_simulated(cluster, duration, args.half_duplex, args.rpc).await
    } else {
        run_hardware(&args, cluster, duration).await
    }
}

/// Serve the RPC API over a node's persisted on-disk block store, without any
/// modem or consensus. Lets you inspect the finalized chain (`/blocks`, etc.)
/// after a run has ended — the chain lives in RocksDB under the current dir's
/// `data/`, so run this from the same dir the node ran in (e.g. `/tmp/bc-node0`).
async fn run_inspect(args: &Args, cluster: Cluster, duration: Duration) -> anyhow::Result<()> {
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
    spawn_rpc(blockstore);

    // Stay up so the endpoint is reachable; honor --duration as an auto-exit.
    tokio::time::sleep(duration).await;
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
    duration: Duration,
    half_duplex: bool,
    rpc: bool,
) -> anyhow::Result<()> {
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
    let (node_a, handle_a) = build_node(ta, 0, &cluster, turn_a);
    let (node_b, handle_b) = build_node(tb, 1, &cluster, turn_b);

    // Optional RPC over node 0's block store (both nodes share the same chain).
    if rpc {
        spawn_rpc(node_a.get_blockstore());
    }

    let until = tokio::time::Instant::now() + duration;
    // Simulated link never "drops" (full-duplex), so no link watch / reconnect.
    let a = tokio::spawn(async move { run_node("node0", node_a, handle_a, until, None).await });
    let b = tokio::spawn(async move { run_node("node1", node_b, handle_b, until, None).await });

    let (slot_a, slot_b) = (a.await?.0, b.await?.0);
    println!("=== done: node0 finalized {slot_a}, node1 finalized {slot_b} ===");
    if slot_a == 0 && slot_b == 0 {
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
async fn run_hardware(args: &Args, cluster: Cluster, duration: Duration) -> anyhow::Result<()> {
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

    let overall_deadline = tokio::time::Instant::now() + duration;
    let mut highest = 0u64;
    let mut session = 0u32;

    while tokio::time::Instant::now() < overall_deadline {
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
                eprintln!("[{label}] link bring-up failed: {e}; retrying ...");
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        let transport: Arc<dyn PactorTransport> = Arc::new(transport);
        // Half-duplex link: the caller (node 0) starts holding the transmit turn.
        let (node, handle) = build_node(transport.clone(), own_id, &cluster, Some(is_caller));

        // Optional RPC over THIS session's block store. Spawned per session and
        // aborted before teardown below, so each reconnect's fresh RocksDB handle
        // is the one being served and the DB lock is released for the next
        // session. Block queries still see all finalized slots — the chain
        // persists on disk and a rebuilt node resumes from it.
        let rpc_task = if args.rpc {
            Some(tokio::spawn(rpc::run_api(rpc_state_for(node.get_blockstore()))))
        } else {
            None
        };
        if rpc_task.is_some() {
            println!("[{label}] RPC API on http://127.0.0.1:3001 (try /block/slot/1, /blocks)");
        }

        let (slot, stop) =
            run_node(&label, node, handle, overall_deadline, Some(transport.clone())).await;
        highest = highest.max(slot);

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
            RunStop::Deadline => break,
            RunStop::LinkDown => {}
        }
    }

    println!("=== done: node{own_id} finalized {highest} (after {session} session(s)) ===");
    if highest == 0 {
        anyhow::bail!("no slots finalized — consensus did not make progress");
    }
    Ok(())
}
