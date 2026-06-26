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
use bunker_coin_sim::pactor_init::{connect_with_retries, init_modem, PactorInitConfig};
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
    /// a slow link). Hardware defaults to 8; override for a faster/slower link.
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
    // Order matters: cancel consensus and give it a moment to wind down BEFORE
    // aborting the mux reader. Aborting the reader drops its inbound senders; a
    // still-running repair task's `receive().unwrap()` would then see a closed
    // queue and panic (isolated to that task, but noisy). After shutdown +
    // dropping the handle, the mux reader/writer tasks release their transport
    // clones, so the caller dropping the last transport Arc frees the modem port.
    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(2), run_task).await;
    handle.shutdown();
    drop(handle);
    (highest, stop)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = Args::parse();
    let cluster = build_cluster(args.seed);
    let duration = Duration::from_secs(args.duration);

    if args.simulated {
        run_simulated(cluster, duration, args.half_duplex).await
    } else {
        run_hardware(&args, cluster, duration).await
    }
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
) -> anyhow::Result<()> {
    println!(
        "=== simulated 2-node empty-block consensus over PACTOR mux ({}) ===",
        if half_duplex {
            "half-duplex turns"
        } else {
            "full-duplex"
        }
    );
    // Lossless link: the mux/consensus layer has no line-level retransmission of
    // its own (real PACTOR does ARQ in hardware). This increment validates the
    // consensus-over-mux wiring, not loss recovery, so model a clean link with
    // no read timeout (a long-lived node parks on idle reads).
    let config = SimulatedPactorConfig {
        packet_loss: 0.0,
        latency: Duration::from_millis(20),
        latency_jitter: Duration::ZERO,
        setup_delay: Duration::ZERO,
        forced_initial_losses: 0,
        fade_windows: Vec::new(),
        read_timeout: None,
        ..SimulatedPactorConfig::default()
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
) -> anyhow::Result<UsbPactorTransport> {
    println!("bringing up modem ...");
    let transport = init_modem(init_cfg).await?;
    if is_caller {
        println!("connecting to {peercall} ...");
        connect_with_retries(&transport, peercall, connect_attempts).await?;
    } else {
        println!("listening for incoming connection (up to 300s) ...");
        transport
            .accept_incoming(Some(Duration::from_secs(300)))
            .await?;
    }
    // Let the link settle before pushing consensus traffic.
    tokio::time::sleep(Duration::from_secs(3)).await;
    println!("link established; starting consensus");
    Ok(transport)
}

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
    // out past the first slot. Default 8x for radio; --delta-mult overrides.
    let delta_mult = args.delta_mult.unwrap_or(4.0);
    // SAFETY: set at startup before any consensus task / timer reads it.
    unsafe {
        std::env::set_var("BUNKER_DELTA_MULT", delta_mult.to_string());
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

        // Establish (or re-establish) the link for this session.
        let transport = match establish_link(
            &init_cfg,
            is_caller,
            &args.peercall,
            args.connect_attempts,
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
        let (slot, stop) =
            run_node(&label, node, handle, overall_deadline, Some(transport.clone())).await;
        highest = highest.max(slot);

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
