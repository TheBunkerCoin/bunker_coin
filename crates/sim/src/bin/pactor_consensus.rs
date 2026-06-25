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
use scs_pactor::{PactorTransport, SimulatedPactorConfig, SimulatedPactorPair};

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

    /// Serial device for the modem (hardware mode).
    #[arg(long)]
    port: Option<String>,

    /// Which validator this process runs in hardware mode: 0 or 1.
    #[arg(long)]
    node: Option<u64>,

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
fn build_node(
    transport: Arc<dyn PactorTransport>,
    own_id: u64,
    cluster: &Cluster,
) -> (Node, PactorMuxHandle) {
    let mut mux = PactorMux::new(transport);
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

/// Drive a node for `duration`, polling its finalized slot for progress.
///
/// `Alpenglow::run` consumes the node, so we grab the pool handle (a cheap `Arc`
/// clone) *before* moving the node into the run task, then poll it.
async fn run_node(label: String, node: Node, handle: PactorMuxHandle, duration: Duration) -> u64 {
    let pool = node.get_pool();
    let cancel = node.get_cancel_token();
    let run_task = tokio::spawn(node.run());

    let deadline = tokio::time::Instant::now() + duration;
    let mut last = 0u64;
    let highest = loop {
        let finalized = pool.read().await.finalized_slot().inner();
        if finalized != last {
            println!("[{label}] finalized slot {finalized}");
            last = finalized;
        }
        if tokio::time::Instant::now() >= deadline {
            break finalized;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    };

    // Stop consensus. We deliberately do NOT abort the mux reader/writer here:
    // aborting the reader drops its inbound senders, which would make the
    // (still-running) repair tasks' `receive().unwrap()` see a closed queue and
    // panic on the way out. Cancelling consensus and letting the runtime drop
    // the mux tasks on process exit is a clean shutdown. `handle` is held until
    // here so the reader's senders stay alive for the lifetime of the node.
    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(2), run_task).await;
    drop(handle);
    highest
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = Args::parse();
    let cluster = build_cluster(args.seed);
    let duration = Duration::from_secs(args.duration);

    if args.simulated {
        run_simulated(cluster, duration).await
    } else {
        run_hardware(&args, cluster, duration).await
    }
}

/// Build both nodes over a simulated PACTOR pair and run them against each other.
async fn run_simulated(cluster: Cluster, duration: Duration) -> anyhow::Result<()> {
    println!("=== simulated 2-node empty-block consensus over PACTOR mux ===");
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

    let (node_a, handle_a) = build_node(ta, 0, &cluster);
    let (node_b, handle_b) = build_node(tb, 1, &cluster);

    let a = tokio::spawn(run_node("node0".to_string(), node_a, handle_a, duration));
    let b = tokio::spawn(run_node("node1".to_string(), node_b, handle_b, duration));

    let (slot_a, slot_b) = (a.await?, b.await?);
    println!("=== done: node0 finalized {slot_a}, node1 finalized {slot_b} ===");
    if slot_a == 0 && slot_b == 0 {
        anyhow::bail!("no slots finalized — consensus did not make progress");
    }
    Ok(())
}

/// Hardware mode placeholder. The proven modem bring-up (terminal-mode init,
/// JHOST/converse handling, connect/listen, changeover) lives in the
/// `pactor_hw_test` binary; the next increment factors it into a shared module so
/// this path can construct a connected transport and call [`build_node`].
async fn run_hardware(args: &Args, _cluster: Cluster, _duration: Duration) -> anyhow::Result<()> {
    let _ = (&args.port, &args.node);
    anyhow::bail!(
        "hardware mode not yet wired: modem init/connect lives in pactor_hw_test and will be \
         factored into a shared module next. Use --simulated to exercise the consensus-over-mux \
         wiring now."
    )
}
