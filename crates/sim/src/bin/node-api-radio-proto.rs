//! multi-node radio simulation for BunkerCoin

use bunker_coin_core::execution::State as ExecutionState;
use bunker_coin_core::transaction::Transaction as CoreTransaction;
use bunker_coin_sim::scenarios;
use ed25519_dalek::SigningKey;
use rpc::{run_api, RadioStats, SharedState, TxResult};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio::task;

#[tokio::main]
async fn main() {
    env_logger::init();

    if std::env::args().any(|arg| arg == "--pactor") {
        run_pactor_radio_proto_demo().await;
        return;
    }

    let data_dir = std::env::temp_dir().join(format!(
        "bunker-coin-node-api-radio-proto-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&data_dir).expect("create isolated node data directory");
    std::env::set_current_dir(&data_dir).expect("switch to isolated node data directory");

    log::info!("=== BunkerCoin Node Starting ===");
    log::info!("Using isolated data directory: {}", data_dir.display());
    log::info!("Radio simulation with 4.8 kbps bandwidth");
    log::info!("Starting API server on port 3001...");

    let blocks = Arc::new(RwLock::new(Vec::new()));
    let nodes = Arc::new(RwLock::new(Vec::new()));
    let radio_stats = Arc::new(RwLock::new(RadioStats {
        bandwidth_bps: 4800,
        packet_loss_percent: 15.0,
        latency_ms: 250,
        jitter_ms: 50,
        packets_sent: 0,
        packets_dropped: 0,
        current_throughput_bps: 0.0,
        link_speed_level: 0,
    }));
    let (updates_tx, _) = broadcast::channel(1000);

    let num_nodes = 4;
    log::info!(
        "Starting {}-node consensus simulation over radio network",
        num_nodes
    );

    let blockstore_ref = Arc::new(RwLock::new(None));
    let blockstore_for_api = blockstore_ref.clone();
    let snapshot_store_ref = Arc::new(RwLock::new(None));
    let snapshot_store_for_api = snapshot_store_ref.clone();

    // deterministic genesis keypair from a fixed seed
    let genesis_seed: [u8; 32] = [1u8; 32];
    let genesis_sk = SigningKey::from_bytes(&genesis_seed);
    let genesis_pk: [u8; 32] = genesis_sk.verifying_key().to_bytes();

    let mut exec_state = ExecutionState::new();
    exec_state.get_or_create_account(&genesis_pk).native_balance = 1_000_000_000;
    let execution_state = Arc::new(RwLock::new(exec_state));

    log::info!(
        "Genesis account funded: {} with 1,000,000,000 native",
        hex::encode(genesis_pk)
    );

    let genesis_signing_key = Arc::new(genesis_sk);

    let tx_sender_slot: Arc<RwLock<Option<mpsc::UnboundedSender<CoreTransaction>>>> =
        Arc::new(RwLock::new(None));
    let tx_sender_for_api = tx_sender_slot.clone();
    let mempool: Arc<RwLock<Vec<rpc::MempoolEntry>>> = Arc::new(RwLock::new(Vec::new()));
    let tx_results: Arc<RwLock<HashMap<String, TxResult>>> = Arc::new(RwLock::new(HashMap::new()));

    let state = SharedState {
        blocks: blocks.clone(),
        nodes: nodes.clone(),
        radio_stats: radio_stats.clone(),
        updates: updates_tx.clone(),
        blockstore: None,
        mempool: mempool.clone(),
        tx_sender: None,
        execution_state: execution_state.clone(),
        tx_results: tx_results.clone(),
        genesis_signing_key: Some(genesis_signing_key),
        snapshot_store: None,
    };

    // api in dedicated task
    let api_handle = task::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        let bs = blockstore_for_api.read().await.clone();
        let snapshot_store = snapshot_store_for_api.read().await.clone();
        let tx_sender = tx_sender_for_api.read().await.clone();
        let mut state = state;
        state.blockstore = bs;
        state.snapshot_store = snapshot_store;
        state.tx_sender = tx_sender;

        run_api(state).await;
    });

    scenarios::multi_node_consensus_simulation_with_api(
        num_nodes,
        blocks,
        nodes,
        radio_stats,
        updates_tx,
        blockstore_ref,
        snapshot_store_ref,
        execution_state,
        tx_sender_slot,
        mempool,
        tx_results,
    )
    .await;

    log::info!("Simulation completed, shutting down API server");
    api_handle.await.unwrap();
}

async fn run_pactor_radio_proto_demo() {
    let mut config = scs_pactor::SimulatedPactorConfig::good_link();
    config.latency = std::time::Duration::from_millis(100);
    config.latency_jitter = std::time::Duration::from_millis(25);
    config.setup_delay = std::time::Duration::from_millis(250);

    let result = scenarios::pactor_radio_proto_demo(config)
        .await
        .expect("run PACTOR radio-proto simulation");

    println!("PACTOR radio-proto demo complete");
    println!("received messages: {:?}", result.received_messages);
    println!("frames attempted: {}", result.frames_attempted);
    println!("frames lost: {}", result.frames_lost);
    println!("retransmissions: {}", result.retransmissions);
    println!("bytes delivered: {}", result.bytes_delivered);

    let comparison = scenarios::pactor_radio_proto_degradation_demo()
        .await
        .expect("run PACTOR ARQ degradation simulation");
    println!();
    println!("clean vs degraded PACTOR radio-proto:");
    println!(
        "clean: attempts={}, lost={}, retransmissions={}, bytes={}",
        comparison.clean.frames_attempted,
        comparison.clean.frames_lost,
        comparison.clean.retransmissions,
        comparison.clean.bytes_delivered
    );
    println!(
        "degraded: attempts={}, lost={}, retransmissions={}, bytes={}",
        comparison.degraded.frames_attempted,
        comparison.degraded.frames_lost,
        comparison.degraded.retransmissions,
        comparison.degraded.bytes_delivered
    );

    println!();
    println!("PACTOR speed report:");
    for sample in scenarios::pactor_throughput_report() {
        println!(
            "PT-{}: raw={} bps, clean={:.0} bps ({:.1}% error), degraded ARQ={:.0} bps",
            sample.speed_level,
            sample.raw_bps,
            sample.clean_effective_bps,
            sample.clean_error_pct,
            sample.degraded_effective_bps
        );
    }

    println!();
    println!("Measured PACTOR simulator throughput:");
    let measured = scenarios::pactor_measured_throughput_report()
        .await
        .expect("measure PACTOR simulator throughput");
    for sample in measured {
        println!(
            "PT-{}: payload={} bytes, raw={} bps, clean={:.0} bps ({:.1}% error), degraded={:.0} bps ({} retransmissions)",
            sample.speed_level,
            sample.payload_bytes,
            sample.raw_bps,
            sample.clean_effective_bps,
            sample.clean_error_pct,
            sample.degraded_effective_bps,
            sample.degraded_retransmissions
        );
    }
}
