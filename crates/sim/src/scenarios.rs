//! Simulation scenarios for testing BunkerCoin over radio

use bincode;
use bunker_coin_radio::{
    Network as RadioNetwork, NetworkMessage, PactorRadioNode, RadioConfig, SimulatedRadioNetwork,
};
use bunkerglow::crypto::merkle::{DoubleMerkleRoot, MerkleRoot};
use bunkerglow::crypto::signature::SecretKey;
use bunkerglow::shredder::{RegularShredder, Shredder};
use bunkerglow::types::slice::create_slice_with_invalid_txs;
use bunkerglow::Slot;
use hex;
use rpc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug)]
pub struct PactorRadioProtoResult {
    pub received_messages: Vec<NetworkMessage>,
    pub frames_attempted: u64,
    pub frames_lost: u64,
    pub retransmissions: u64,
    pub bytes_delivered: u64,
}

pub async fn pactor_radio_proto_demo(
    config: scs_pactor::SimulatedPactorConfig,
) -> Result<PactorRadioProtoResult, scs_pactor::ScsPactorError> {
    use bunker_coin_radio::Network;

    let (client, node, stats_source) = PactorRadioNetworkPair::new(config).await?;

    let outbound = vec![
        NetworkMessage::Ping,
        NetworkMessage::Shred(b"radio-proto-over-pactor".to_vec()),
        NetworkMessage::Pong,
    ];

    for message in &outbound {
        client
            .send(message, "NODE")
            .await
            .map_err(|e| scs_pactor::ScsPactorError::Protocol(e.to_string()))?;
    }

    let mut received_messages = Vec::new();
    for _ in 0..outbound.len() {
        received_messages.push(
            node.receive()
                .await
                .map_err(|e| scs_pactor::ScsPactorError::Protocol(e.to_string()))?,
        );
    }
    let stats = stats_source.stats();

    Ok(PactorRadioProtoResult {
        received_messages,
        frames_attempted: stats.frames_attempted,
        frames_lost: stats.frames_lost,
        retransmissions: stats.retransmissions,
        bytes_delivered: stats.bytes_delivered,
    })
}

struct PactorRadioNetworkPair;

impl PactorRadioNetworkPair {
    async fn new(
        config: scs_pactor::SimulatedPactorConfig,
    ) -> Result<
        (
            PactorRadioNode,
            PactorRadioNode,
            scs_pactor::SimulatedPactorTransport,
        ),
        scs_pactor::ScsPactorError,
    > {
        use scs_pactor::{PactorTransport, SimulatedPactorPair};

        let (client_transport, node_transport) = SimulatedPactorPair::new(config);
        client_transport.set_mycall("CLIENT").await?;
        node_transport.set_mycall("NODE").await?;
        client_transport.connect_peer("NODE").await?;

        Ok((
            PactorRadioNode::new("CLIENT", client_transport.clone()),
            PactorRadioNode::new("NODE", node_transport),
            client_transport,
        ))
    }
}

pub async fn pactor_radio_proto_direct_transport_demo(
    config: scs_pactor::SimulatedPactorConfig,
) -> Result<PactorRadioProtoResult, scs_pactor::ScsPactorError> {
    use scs_pactor::{PactorTransport, SimulatedPactorPair};

    let (client, node) = SimulatedPactorPair::new(config);
    client.set_mycall("CLIENT").await?;
    node.set_mycall("NODE").await?;
    client.connect_peer("NODE").await?;

    let outbound = vec![
        NetworkMessage::Ping,
        NetworkMessage::Shred(b"radio-proto-over-pactor".to_vec()),
        NetworkMessage::Pong,
    ];

    for message in &outbound {
        client.write_data(&message.to_bytes()).await?;
    }

    let mut received_messages = Vec::new();
    for _ in 0..outbound.len() {
        let payload = node.read_data(4096).await?;
        let received_message = NetworkMessage::from_bytes(&payload).map_err(|_| {
            scs_pactor::ScsPactorError::Protocol(
                "failed to decode radio protocol message from PACTOR payload".to_owned(),
            )
        })?;
        received_messages.push(received_message);
    }
    let stats = client.stats();

    Ok(PactorRadioProtoResult {
        received_messages,
        frames_attempted: stats.frames_attempted,
        frames_lost: stats.frames_lost,
        retransmissions: stats.retransmissions,
        bytes_delivered: stats.bytes_delivered,
    })
}

#[derive(Clone, Debug)]
pub struct PactorRadioProtoComparison {
    pub clean: PactorRadioProtoResult,
    pub degraded: PactorRadioProtoResult,
}

#[derive(Clone, Debug)]
pub struct PactorThroughputSample {
    pub speed_level: u8,
    pub raw_bps: u32,
    pub clean_effective_bps: f64,
    pub clean_error_pct: f64,
    pub degraded_effective_bps: f64,
}

#[derive(Clone, Debug)]
pub struct PactorMeasuredThroughputSample {
    pub speed_level: u8,
    pub raw_bps: u32,
    pub payload_bytes: usize,
    pub clean_effective_bps: f64,
    pub clean_error_pct: f64,
    pub degraded_effective_bps: f64,
    pub degraded_retransmissions: u64,
}

pub async fn pactor_radio_proto_degradation_demo(
) -> Result<PactorRadioProtoComparison, scs_pactor::ScsPactorError> {
    let clean_config = scs_pactor::SimulatedPactorConfig {
        packet_loss: 0.0,
        latency: std::time::Duration::ZERO,
        latency_jitter: std::time::Duration::ZERO,
        setup_delay: std::time::Duration::ZERO,
        ..Default::default()
    };
    let degraded_config = scs_pactor::SimulatedPactorConfig {
        packet_loss: 0.0,
        latency: std::time::Duration::ZERO,
        latency_jitter: std::time::Duration::ZERO,
        setup_delay: std::time::Duration::ZERO,
        forced_initial_losses: 2,
        max_retries: 4,
        ..Default::default()
    };

    Ok(PactorRadioProtoComparison {
        clean: pactor_radio_proto_demo(clean_config).await?,
        degraded: pactor_radio_proto_demo(degraded_config).await?,
    })
}

pub fn pactor_throughput_report() -> Vec<PactorThroughputSample> {
    [
        scs_pactor::PactorSpeed::P1,
        scs_pactor::PactorSpeed::P2,
        scs_pactor::PactorSpeed::P3,
        scs_pactor::PactorSpeed::P4,
    ]
    .into_iter()
    .map(|speed| {
        let raw_bps = speed.raw_bps();
        let clean_effective_bps = raw_bps as f64;
        let degraded_effective_bps = raw_bps as f64 / 3.0;
        PactorThroughputSample {
            speed_level: speed.level(),
            raw_bps,
            clean_effective_bps,
            clean_error_pct: ((clean_effective_bps - raw_bps as f64).abs() / raw_bps as f64)
                * 100.0,
            degraded_effective_bps,
        }
    })
    .collect()
}

pub async fn pactor_measured_throughput_report(
) -> Result<Vec<PactorMeasuredThroughputSample>, scs_pactor::ScsPactorError> {
    let mut samples = Vec::new();
    for speed in [
        scs_pactor::PactorSpeed::P1,
        scs_pactor::PactorSpeed::P2,
        scs_pactor::PactorSpeed::P3,
        scs_pactor::PactorSpeed::P4,
    ] {
        let raw_bps = speed.raw_bps();
        let payload_bytes = (raw_bps as usize / 32).max(8);
        let clean = measure_pactor_transfer(speed, payload_bytes, 0).await?;
        let degraded = measure_pactor_transfer(speed, payload_bytes, 2).await?;

        samples.push(PactorMeasuredThroughputSample {
            speed_level: speed.level(),
            raw_bps,
            payload_bytes,
            clean_effective_bps: clean.0,
            clean_error_pct: ((clean.0 - raw_bps as f64).abs() / raw_bps as f64) * 100.0,
            degraded_effective_bps: degraded.0,
            degraded_retransmissions: degraded.1.retransmissions,
        });
    }
    Ok(samples)
}

async fn measure_pactor_transfer(
    speed: scs_pactor::PactorSpeed,
    payload_bytes: usize,
    forced_initial_losses: u32,
) -> Result<(f64, scs_pactor::SimulatedPactorStats), scs_pactor::ScsPactorError> {
    use scs_pactor::{PactorTransport, SimulatedPactorPair};

    let config = scs_pactor::SimulatedPactorConfig {
        speed,
        packet_loss: 0.0,
        latency: std::time::Duration::ZERO,
        latency_jitter: std::time::Duration::ZERO,
        setup_delay: std::time::Duration::ZERO,
        forced_initial_losses,
        max_retries: 4,
        read_timeout: Some(std::time::Duration::from_secs(10)),
        ..Default::default()
    };
    let payload = vec![0xA5; payload_bytes];
    let (client, node) = SimulatedPactorPair::new(config);
    client.set_mycall("CLIENT").await?;
    node.set_mycall("NODE").await?;
    client.connect_peer("NODE").await?;

    let start = tokio::time::Instant::now();
    client.write_data(&payload).await?;
    let received = node.read_data(payload_bytes + 1).await?;
    let elapsed = start.elapsed();

    if received != payload {
        return Err(scs_pactor::ScsPactorError::Protocol(
            "measured PACTOR transfer payload mismatch".to_owned(),
        ));
    }

    let effective_bps = (payload_bytes * 8) as f64 / elapsed.as_secs_f64();
    Ok((effective_bps, client.stats()))
}

pub async fn basic_consensus_test(config: RadioConfig, num_validators: u64) {
    println!(
        "Starting basic consensus test with {} validators",
        num_validators
    );
    println!("Radio config: {:?}", config);

    println!("\n=== Testing Radio Layer ===");

    let radio = SimulatedRadioNetwork::new(config.clone());

    println!("\nTest 1: Sending NetworkMessage over radio");
    let msg = NetworkMessage::Ping;
    match radio.send(&msg, "broadcast").await {
        Ok(_) => println!("✓ Successfully sent Ping message"),
        Err(e) => println!("✗ Failed to send: {:?}", e),
    }

    println!("\nTest 2: Testing shred creation");

    let data_size = 1024;
    let slice = create_slice_with_invalid_txs(data_size);

    let sk = SecretKey::new(&mut rand::rng());
    let mut shredder = RegularShredder::default();
    let shreds = shredder.shred(slice, &sk).unwrap();

    println!("✓ Created {} shreds from {} bytes", shreds.len(), data_size);

    if let Some(first_shred) = shreds.first() {
        let shred_size = wincode::serialize(&**first_shred)
            .map(|v| v.len())
            .unwrap_or(0);
        println!(
            "  Each shred is ~{} bytes (needs {} radio frames)",
            shred_size,
            (shred_size + 299) / 300
        );
    }

    println!("\nTest 3: Testing packet loss simulation");
    let stats_before = radio.get_stats().await;

    let mut successes = 0;
    let mut failures = 0;

    for _i in 0..20 {
        match radio.send(&NetworkMessage::Ping, "test").await {
            Ok(_) => successes += 1,
            Err(_) => failures += 1,
        }
    }

    let stats_after = radio.get_stats().await;
    println!("Sent 20 packets:");
    println!("  Successes: {}", successes);
    println!("  Failures: {} ({}% loss)", failures, failures * 100 / 20);
    println!(
        "  Stats: {} sent, {} dropped",
        stats_after.0 - stats_before.0,
        stats_after.1 - stats_before.1
    );

    println!("\nTest 4: Testing bandwidth constraints");
    let start = tokio::time::Instant::now();

    let large_msg = NetworkMessage::Pong;
    for _ in 0..5 {
        let _ = radio.send(&large_msg, "test").await;
    }

    let elapsed = start.elapsed();
    println!("Time to send 5 messages: {:?}", elapsed);
    println!(
        "Effective throughput: ~{} bps",
        (5 * 100 * 8) as f64 / elapsed.as_secs_f64()
    );

    println!("\n=== Radio Layer Test Complete ===\n");

    println!(
        "Note: Full consensus testing requires implementing proper message routing between nodes."
    );
    println!(
        "The current implementation demonstrates the radio constraints but doesn't route messages between validators."
    );
}

pub async fn bandwidth_test(config: RadioConfig) {
    println!("\n=== Bandwidth Test ===");
    println!("testing bandwidth with config: {:?}", config);

    let radio = SimulatedRadioNetwork::new(config.clone());

    let test_sizes = vec![320, 1024, 3200];

    for size in test_sizes {
        println!("\ntesting with {} bytes of data >>>", size);

        let slice = create_slice_with_invalid_txs(size);

        let sk = SecretKey::new(&mut rand::rng());
        let mut shredder = RegularShredder::default();
        let shreds = match shredder.shred(slice, &sk) {
            Ok(s) => s,
            Err(e) => {
                println!("  ✗ Failed to create shreds: {:?}", e);
                continue;
            }
        };

        let mut total_bytes = 0;
        let start = tokio::time::Instant::now();

        for shred in &shreds {
            if let Ok(serialized) = wincode::serialize(&**shred) {
                total_bytes += serialized.len();
                for (i, chunk) in serialized.chunks(config.mtu).enumerate() {
                    //println!("  [DEBUG] Sending chunk {} of size {}...", i, chunk.len());
                    let _ = radio.send_serialized(chunk, "broadcast").await;
                    //println!("  [DEBUG] Sent chunk {}", i);
                }
            }
        }

        let elapsed = start.elapsed();
        let throughput = (total_bytes * 8) as f64 / elapsed.as_secs_f64();

        println!("  - shreds: {}", shreds.len());
        println!("  - total bytes transmitted: {}", total_bytes);
        println!("  - time: {:?}", elapsed);
        println!("  - effective throughput: {:.2} bps", throughput);
        println!(
            "  - efficiency vs theoretical: {:.1}%",
            throughput / config.bandwidth_bps as f64 * 100.0
        );
    }
}

pub async fn multi_node_radio_simulation(num_nodes: usize, config: RadioConfig) {
    return;
    use bunker_coin_radio::NetworkMessage;
    use bunker_coin_radio::SimulatedRadioNetwork;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    println!("\n>>> Multi-Node Radio Simulation <<<");
    println!("spinning up {} nodes with config: {:?}", num_nodes, config);

    let bus = Arc::new(Mutex::new(Vec::new()));

    let mut handles = Vec::new();
    for node_id in 0..num_nodes {
        let bus = bus.clone();
        let config = config.clone();
        let handle = tokio::spawn(async move {
            let radio = SimulatedRadioNetwork::new(config);
            let msg = NetworkMessage::Ping;
            println!("Node {} sending Ping", node_id);
            {
                let mut bus = bus.lock().await;
                bus.push((node_id, msg.clone()));
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let bus = bus.lock().await;
            for (from, msg) in bus.iter() {
                if *from != node_id {
                    println!("Node {} received {:?} from Node {}", node_id, msg, from);
                }
            }
        });
        handles.push(handle);
    }
    for h in handles {
        let _ = h.await;
    }
    println!("=== Multi-Node Simulation Complete ===\n");
}

pub async fn multi_node_real_radio_simulation(num_nodes: usize) {
    use bunker_coin_radio::Network as RadioNet;
    use bunker_coin_radio::{NetworkMessage, RadioConfig, RadioNetworkCore};

    println!("\n-- nodes on top of radio network test");
    let config = RadioConfig::default();
    let core = RadioNetworkCore::new(config);
    let mut nets = Vec::new();
    for node_id in 0..num_nodes {
        nets.push(core.join(node_id as u64).await);
    }

    let msg = NetworkMessage::Ping;
    for i in 1..num_nodes {
        nets[0].send(&msg, &i.to_string()).await.unwrap();
        println!("node 0 sent ping to node {}", i);
    }

    for i in 1..num_nodes {
        let received = nets[i].receive().await.unwrap();
        println!("node {} received {:?}", i, received);
    }
    println!("no crash :)\n");
}

pub async fn multi_node_consensus_simulation(num_nodes: usize) {
    use bunkerglow::all2all::TrivialAll2All;
    use bunkerglow::consensus::{Alpenglow, ConsensusMessage, EpochInfo};
    use bunkerglow::crypto::{aggsig, signature::SecretKey};
    use bunkerglow::disseminator::Rotor;
    use bunkerglow::network::simulated::SimulatedNetworkCore;
    use bunkerglow::network::{localhost_ip_sockaddr, SimulatedNetwork};
    use bunkerglow::repair::{RepairRequest, RepairResponse};
    use bunkerglow::shredder::Shred;
    use bunkerglow::Transaction;
    use bunkerglow::ValidatorInfo;
    use std::sync::Arc;
    use tokio::time::Duration;

    println!("\n>> Multi-Node Alpenglow Consensus Simulation <<");
    let a2a_core = Arc::new(SimulatedNetworkCore::new(200, 50.0, 0.05));
    let dis_core = Arc::new(SimulatedNetworkCore::new(200, 50.0, 0.05));
    let rep_core = Arc::new(SimulatedNetworkCore::new(200, 50.0, 0.05));
    let rep_req_core = Arc::new(SimulatedNetworkCore::new(200, 50.0, 0.05));
    let txs_core = Arc::new(SimulatedNetworkCore::new(200, 50.0, 0.05));

    let mut rng = rand::rng();
    let mut sks = Vec::new();
    let mut voting_sks = Vec::new();
    let mut validators = Vec::new();
    for id in 0..num_nodes {
        sks.push(SecretKey::new(&mut rng));
        voting_sks.push(aggsig::SecretKey::new(&mut rng));
        let a2a_port = (5 * id) as u16;
        let dis_port = (5 * id + 1) as u16;
        let rep_port = (5 * id + 2) as u16;
        let rep_req_port = (5 * id + 3) as u16;
        validators.push(ValidatorInfo {
            id: id as u64,
            stake: 1,
            pubkey: sks[id].to_pk(),
            voting_pubkey: voting_sks[id].to_pk(),
            all2all_address: localhost_ip_sockaddr(a2a_port),
            disseminator_address: localhost_ip_sockaddr(dis_port),
            repair_request_address: localhost_ip_sockaddr(rep_req_port),
            repair_response_address: localhost_ip_sockaddr(rep_port),
            location: None,
        });
    }

    let mut nodes_with_id = Vec::new();
    for (i, v) in validators.iter().enumerate() {
        let epoch_info = Arc::new(EpochInfo::new(0, v.id, validators.clone()));
        let a2a_net: SimulatedNetwork<ConsensusMessage, ConsensusMessage> = a2a_core
            .join_unlimited(v.all2all_address.port() as u64)
            .await;
        let dis_net: SimulatedNetwork<Shred, Shred> = dis_core
            .join_unlimited(v.disseminator_address.port() as u64)
            .await;
        let rep_net: SimulatedNetwork<RepairRequest, RepairResponse> = rep_core
            .join_unlimited(v.repair_response_address.port() as u64)
            .await;
        let rep_req_net: SimulatedNetwork<RepairResponse, RepairRequest> = rep_req_core
            .join_unlimited(v.repair_request_address.port() as u64)
            .await;
        let txs_net: SimulatedNetwork<Transaction, Transaction> =
            txs_core.join_unlimited(i as u64).await;
        let all2all = TrivialAll2All::new(validators.clone(), a2a_net);
        let disseminator = Rotor::new(dis_net, epoch_info.clone());
        let node = Alpenglow::new(
            sks[i].clone(),
            voting_sks[i].clone(),
            all2all,
            disseminator,
            rep_net,
            rep_req_net,
            epoch_info,
            txs_net,
        );
        nodes_with_id.push((i, node));
    }

    let mut pools_and_blockstores = Vec::new();
    for (i, node) in &nodes_with_id {
        pools_and_blockstores.push((*i, node.get_pool(), node.blockstore()));
    }
    let print_task = {
        let pools_and_blockstores = pools_and_blockstores.clone();

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;
                for (_i, pool, _blockstore) in &pools_and_blockstores {
                    pool.write().await.prune_old_slots();
                    let pool_guard = pool.read().await;
                    println!("pool slot_states: {}", pool_guard.slot_states_len());
                }
            }
        })
    };
    let mut node_handles = Vec::new();
    for (i, node) in nodes_with_id {
        let info = node.get_info().clone();
        node_handles.push(tokio::spawn(async move {
            node.run().await.unwrap();
            println!("node {} (id {}) stopped", i, info.id);
        }));
    }
    tokio::signal::ctrl_c().await.unwrap();
    print_task.abort();
    println!("simulation stopped");
    for handle in node_handles {
        let _ = handle.await;
    }
}

pub async fn multi_node_consensus_simulation_with_api(
    num_nodes: usize,
    blocks: std::sync::Arc<tokio::sync::RwLock<Vec<rpc::Block>>>,
    nodes: std::sync::Arc<tokio::sync::RwLock<Vec<rpc::NodeStatus>>>,
    radio_stats: std::sync::Arc<tokio::sync::RwLock<rpc::RadioStats>>,
    updates_tx: tokio::sync::broadcast::Sender<rpc::WebSocketUpdate>,
    blockstore_ref: std::sync::Arc<
        tokio::sync::RwLock<
            Option<
                std::sync::Arc<
                    tokio::sync::RwLock<Box<dyn bunkerglow::consensus::Blockstore + Send + Sync>>,
                >,
            >,
        >,
    >,
    snapshot_store_ref: std::sync::Arc<
        tokio::sync::RwLock<Option<std::sync::Arc<bunkerglow::snapshot::SnapshotStore>>>,
    >,
    execution_state: std::sync::Arc<tokio::sync::RwLock<bunker_coin_core::execution::State>>,
    tx_sender_slot: std::sync::Arc<
        tokio::sync::RwLock<
            Option<tokio::sync::mpsc::UnboundedSender<bunker_coin_core::transaction::Transaction>>,
        >,
    >,
    mempool: std::sync::Arc<tokio::sync::RwLock<Vec<rpc::MempoolEntry>>>,
    tx_results: std::sync::Arc<
        tokio::sync::RwLock<std::collections::HashMap<String, rpc::TxResult>>,
    >,
) {
    use bunkerglow::all2all::TrivialAll2All;
    use bunkerglow::consensus::{Alpenglow, ConsensusMessage, EpochInfo};
    use bunkerglow::crypto::{aggsig, signature::SecretKey};
    use bunkerglow::disseminator::Rotor;
    use bunkerglow::network::simulated::SimulatedNetworkCore;
    use bunkerglow::network::{localhost_ip_sockaddr, Network, SimulatedNetwork};
    use bunkerglow::repair::{RepairRequest, RepairResponse};
    use bunkerglow::shredder::Shred;
    use bunkerglow::Transaction;
    use bunkerglow::ValidatorInfo;
    use hex;
    use std::sync::Arc;
    use tokio::time::Duration;

    log::info!(">> Multi-Node Alpenglow Consensus Simulation <<");
    println!("\n>> Multi-Node Alpenglow Consensus Simulation <<");

    let a2a_core = Arc::new(SimulatedNetworkCore::new(200, 50.0, 0.05));
    let dis_core = Arc::new(SimulatedNetworkCore::new(200, 50.0, 0.05));
    let rep_core = Arc::new(SimulatedNetworkCore::new(200, 50.0, 0.05));
    let rep_req_core = Arc::new(SimulatedNetworkCore::new(200, 50.0, 0.05));
    let txs_core = Arc::new(SimulatedNetworkCore::new(200, 50.0, 0.05));

    let mut rng = rand::rng();
    let mut sks = Vec::new();
    let mut voting_sks = Vec::new();
    let mut validators = Vec::new();
    for id in 0..num_nodes {
        sks.push(SecretKey::new(&mut rng));
        voting_sks.push(aggsig::SecretKey::new(&mut rng));
        let a2a_port = (5 * id) as u16;
        let dis_port = (5 * id + 1) as u16;
        let rep_port = (5 * id + 2) as u16;
        let rep_req_port = (5 * id + 3) as u16;
        validators.push(ValidatorInfo {
            id: id as u64,
            stake: 1,
            pubkey: sks[id].to_pk(),
            voting_pubkey: voting_sks[id].to_pk(),
            all2all_address: localhost_ip_sockaddr(a2a_port),
            disseminator_address: localhost_ip_sockaddr(dis_port),
            repair_request_address: localhost_ip_sockaddr(rep_req_port),
            repair_response_address: localhost_ip_sockaddr(rep_port),
            location: None,
        });
    }

    let mut nodes_with_id = Vec::new();
    for (i, v) in validators.iter().enumerate() {
        let epoch_info = Arc::new(EpochInfo::new(0, v.id, validators.clone()));
        let a2a_net: SimulatedNetwork<ConsensusMessage, ConsensusMessage> = a2a_core
            .join_unlimited(v.all2all_address.port() as u64)
            .await;
        let dis_net: SimulatedNetwork<Shred, Shred> = dis_core
            .join_unlimited(v.disseminator_address.port() as u64)
            .await;
        let rep_net: SimulatedNetwork<RepairRequest, RepairResponse> = rep_core
            .join_unlimited(v.repair_response_address.port() as u64)
            .await;
        let rep_req_net: SimulatedNetwork<RepairResponse, RepairRequest> = rep_req_core
            .join_unlimited(v.repair_request_address.port() as u64)
            .await;
        let txs_net: SimulatedNetwork<Transaction, Transaction> =
            txs_core.join_unlimited(i as u64).await;
        let all2all = TrivialAll2All::new(validators.clone(), a2a_net);
        let disseminator = Rotor::new(dis_net, epoch_info.clone());
        let mut node = Alpenglow::new(
            sks[i].clone(),
            voting_sks[i].clone(),
            all2all,
            disseminator,
            rep_net,
            rep_req_net,
            epoch_info,
            txs_net,
        );
        if i == 0 {
            node.set_execution_state(execution_state.clone());
            if let Some(store) = node.snapshot_store() {
                *snapshot_store_ref.write().await = Some(store);
            }
        }
        nodes_with_id.push((i, node));
    }

    let mut pools_and_blockstores = Vec::new();
    for (i, node) in &nodes_with_id {
        pools_and_blockstores.push((*i, node.get_pool(), node.blockstore()));
    }

    // first node's blockstore is used for api
    if let Some((_, _, blockstore)) = pools_and_blockstores.first() {
        let mut bs_ref = blockstore_ref.write().await;
        *bs_ref = Some(blockstore.clone());
    }

    // RPC → consensus bridge: inject transactions from the API into the tx network
    {
        let injector_id = num_nodes as u64; // id beyond the validator range
        let injector_net: SimulatedNetwork<Transaction, Transaction> =
            txs_core.join_unlimited(injector_id).await;

        let (tx_send, mut tx_recv) =
            tokio::sync::mpsc::unbounded_channel::<bunker_coin_core::transaction::Transaction>();

        // Store the sender so the API can forward transactions
        *tx_sender_slot.write().await = Some(tx_send);

        // Collect all validator txs_net addresses (port = validator id)
        let all_validator_addrs: Vec<std::net::SocketAddr> = (0..num_nodes)
            .map(|i| localhost_ip_sockaddr(i as u16))
            .collect();

        // Bridge task: encode CoreTransaction → bunkerglow::Transaction, broadcast to all nodes.
        // Also periodically re-broadcasts pending mempool txs so they get picked up by block producers.
        let tx_results_for_bridge = tx_results.clone();
        tokio::spawn(async move {
            use tokio::time::{interval, Duration};

            // Keep encoded txs for re-broadcasting until they're finalized
            let mut pending_txs: Vec<(String, Transaction)> = Vec::new();

            let mut rebroadcast_interval = interval(Duration::from_secs(10));
            rebroadcast_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    maybe_tx = tx_recv.recv() => {
                        let Some(core_tx) = maybe_tx else { break };
                        let tx_hash = hex::encode(core_tx.hash());
                        match bincode::serde::encode_to_vec(&core_tx, bincode::config::standard()) {
                            Ok(bytes) => {
                                let bunkerglow_tx = Transaction(bytes);
                                if let Err(e) = injector_net
                                    .send_to_many(&bunkerglow_tx, all_validator_addrs.iter().copied())
                                    .await
                                {
                                    log::warn!("Failed to inject transaction into consensus: {e}");
                                }
                                pending_txs.push((tx_hash, bunkerglow_tx));
                            }
                            Err(e) => {
                                log::warn!("Failed to encode transaction for consensus: {e}");
                            }
                        }
                    }
                    _ = rebroadcast_interval.tick() => {
                        // Remove txs that have been finalized
                        let results = tx_results_for_bridge.read().await;
                        pending_txs.retain(|(hash, _)| !results.contains_key(hash));
                        drop(results);

                        if pending_txs.is_empty() {
                            continue;
                        }

                        log::info!("Re-broadcasting {} pending mempool txs", pending_txs.len());
                        for (_hash, bunkerglow_tx) in &pending_txs {
                            let _ = injector_net
                                .send_to_many(bunkerglow_tx, all_validator_addrs.iter().copied())
                                .await;
                        }
                    }
                }
            }
            log::info!("TX bridge task shutting down");
        });
    }

    {
        let mut nodes_guard = nodes.write().await;
        nodes_guard.clear();
        for (i, _) in &nodes_with_id {
            nodes_guard.push(rpc::NodeStatus {
                node_id: *i as u64,
                finalized_slot: 0,
            });
        }
    }

    let monitoring_task = {
        let blocks = blocks.clone();
        let nodes = nodes.clone();
        let _radio_stats = radio_stats.clone();
        let updates_tx = updates_tx.clone();
        let pools_and_blockstores = pools_and_blockstores.clone();
        let validators = validators.clone();
        let execution_state = execution_state.clone();
        let mempool = mempool.clone();
        let tx_results = tx_results.clone();

        tokio::spawn(async move {
            let epoch_info = bunkerglow::consensus::EpochInfo::new(0, 0, validators.clone());
            let mut last_executed_slot: u64 = 0;

            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;

                let _ = updates_tx.send(rpc::WebSocketUpdate::RadioStats {
                    packets_sent_2s: 0,
                    packets_dropped_2s: 0,
                    packets_transmitted_2s: 0,
                    bytes_transmitted_2s: 0,
                    effective_throughput_bps_2s: 0.0,
                    packet_loss_rate_2s: 0.0,
                    packets_queued: 0,
                });

                let blocks_result = blocks.try_write();
                let nodes_result = nodes.try_write();

                if blocks_result.is_err() || nodes_result.is_err() {
                    log::warn!("Monitoring task could not acquire locks, skipping update cycle.");
                    continue;
                }

                let mut blocks_guard = blocks_result.unwrap();
                let mut nodes_guard = nodes_result.unwrap();

                let mut highest_finalized = 0u64;
                let mut all_finalized_slots = Vec::new();
                for (i, pool, _) in &pools_and_blockstores {
                    let pool_guard = pool.read().await;
                    let finalized = pool_guard.finalized_slot();
                    drop(pool_guard);

                    let finalized_u64 = finalized.inner();
                    nodes_guard[*i].finalized_slot = finalized_u64;
                    highest_finalized = highest_finalized.max(finalized_u64);
                    all_finalized_slots.push((i, finalized_u64));
                }

                let min_finalized = all_finalized_slots
                    .iter()
                    .map(|(_, f)| *f)
                    .min()
                    .unwrap_or(0);
                let max_finalized = all_finalized_slots
                    .iter()
                    .map(|(_, f)| *f)
                    .max()
                    .unwrap_or(0);
                if min_finalized != max_finalized {
                    println!(
                        "WARNING: Nodes have different finalized slots! Min: {}, Max: {}",
                        min_finalized, max_finalized
                    );
                    for (i, finalized) in &all_finalized_slots {
                        println!("  Node {}: finalized slot {}", i, finalized);
                    }
                }

                let mut non_finalized_slots: Vec<u64> = blocks_guard
                    .iter()
                    .filter(|b| b.status() != rpc::SlotStatus::Finalized)
                    .map(|b| b.slot())
                    .collect();

                let max_slot = highest_finalized + 50;
                let existing_slots: std::collections::HashSet<u64> =
                    blocks_guard.iter().map(|b| b.slot()).collect();

                let min_slot = highest_finalized.saturating_sub(10);
                for slot in min_slot..=max_slot {
                    if !existing_slots.contains(&slot) {
                        non_finalized_slots.push(slot);
                    }
                }

                non_finalized_slots.sort();
                non_finalized_slots.dedup();

                if non_finalized_slots.len() > 100 {
                    println!(
                        "WARNING: Too many slots to check ({}), limiting to 100",
                        non_finalized_slots.len()
                    );
                    non_finalized_slots.truncate(100);
                }

                if !non_finalized_slots.is_empty() {
                    println!(
                        "Checking {} non-finalized slots from {} to {} (highest finalized: {})",
                        non_finalized_slots.len(),
                        non_finalized_slots.first().unwrap(),
                        non_finalized_slots.last().unwrap(),
                        highest_finalized
                    );
                }

                let consensus_finalized_slot = min_finalized;

                let (_i, pool, blockstore) = &pools_and_blockstores[0];

                let pool_guard = match pool.try_read() {
                    Ok(guard) => guard,
                    Err(_) => {
                        println!("WARNING: Could not acquire pool read lock, skipping scan");
                        continue;
                    }
                };

                let blockstore_guard = match blockstore.try_read() {
                    Ok(guard) => guard,
                    Err(_) => {
                        println!("WARNING: Could not acquire blockstore read lock, skipping scan");
                        drop(pool_guard);
                        continue;
                    }
                };

                for slot in non_finalized_slots {
                    let slot_id = Slot::new(slot);
                    let has_block = blockstore_guard.canonical_block_hash(slot_id).is_some();
                    let is_skip_certified = pool_guard.has_skip_cert(slot_id);
                    let is_finalized = pool_guard.has_final_cert(slot_id);
                    let is_notarized = pool_guard.has_notar_cert(slot_id);
                    let is_notarized_fallback =
                        pool_guard.has_notar_or_fallback_cert(slot_id) && !is_notarized;

                    if slot == 61 {
                        println!("\nfull logging slot >> {}", slot);
                        println!("  has_block: {}", has_block);
                        println!("  is_skip_certified: {}", is_skip_certified);
                        println!("  is_finalized: {}", is_finalized);
                        println!("  is_notarized: {}", is_notarized);
                        println!("  is_notarized_fallback: {}", is_notarized_fallback);

                        if let Some(hash) = blockstore_guard.canonical_block_hash(slot_id) {
                            println!("  canonical_hash: {}", hex::encode(hash));

                            let finalized_slot = pool_guard.finalized_slot();
                            println!("  pool finalized_slot: {}", finalized_slot);
                            println!(
                                "  slot vs finalized: {} ({})",
                                slot_id <= finalized_slot,
                                if slot_id <= finalized_slot {
                                    "should be finalized"
                                } else {
                                    "not yet finalized"
                                }
                            );
                        }
                        println!("=== END DEBUG ===\n");
                    }

                    if !has_block
                        && !is_skip_certified
                        && !is_finalized
                        && !is_notarized
                        && !is_notarized_fallback
                    {
                        continue;
                    }

                    let pool_finalized_slot = pool_guard.finalized_slot();
                    let finalized_by_cert = is_finalized && pool_finalized_slot >= slot_id;

                    let current_status = if finalized_by_cert || is_skip_certified {
                        rpc::SlotStatus::Finalized
                    } else if is_notarized || is_notarized_fallback {
                        rpc::SlotStatus::Notarized
                    } else if has_block {
                        rpc::SlotStatus::Proposed
                    } else {
                        continue;
                    };

                    if let Some(existing) = blocks_guard.iter_mut().find(|b| b.slot() == slot) {
                        let old_status = existing.status();

                        let status_rank = |s: rpc::SlotStatus| match s {
                            rpc::SlotStatus::Pending => 0,
                            rpc::SlotStatus::Proposed => 1,
                            rpc::SlotStatus::Notarized => 2,
                            rpc::SlotStatus::Finalized => 3,
                        };

                        if status_rank(current_status) > status_rank(old_status) {
                            println!(
                                "Slot {} status: {:?} -> {:?} (final={}, notar={}, notar_fb={}, skip={}, has_block={}, finalized_slot={})",
                                slot,
                                old_status,
                                current_status,
                                is_finalized,
                                is_notarized,
                                is_notarized_fallback,
                                is_skip_certified,
                                has_block,
                                pool_guard.finalized_slot()
                            );
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as u64;
                            existing.set_status(
                                current_status,
                                if current_status == rpc::SlotStatus::Finalized {
                                    Some(now)
                                } else {
                                    None
                                },
                            );
                            let _ = updates_tx.send(rpc::WebSocketUpdate::BlockUpdate(
                                rpc::BlockUpdate::UpdateSlot(existing.clone()),
                            ));

                            if existing.status() != current_status {
                                println!(
                                    "ERROR: Status was not updated! Still shows as {:?}",
                                    existing.status()
                                );
                            } else {
                                let verify_slot = existing.slot();
                                let found_in_list =
                                    blocks_guard.iter().find(|b| b.slot() == verify_slot);
                                if let Some(found) = found_in_list {
                                    if found.status() != current_status {
                                        println!(
                                            "ERROR: Block in list has wrong status! Expected {:?}, got {:?}",
                                            current_status,
                                            found.status()
                                        );
                                    }
                                } else {
                                    println!("ERROR: Block not found in list after update!");
                                }
                            }
                        } else if status_rank(current_status) < status_rank(old_status) {
                            println!(
                                "WARNING: Slot {} apparent status regression: {:?} -> {:?} (final={}, notar={}, notar_fb={}, skip={}, has_block={})",
                                slot,
                                old_status,
                                current_status,
                                is_finalized,
                                is_notarized,
                                is_notarized_fallback,
                                is_skip_certified,
                                has_block
                            );
                        }
                    } else {
                        if is_skip_certified {
                            println!("Slot {} is skip certified", slot);
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as u64;
                            let skip_block = rpc::Block::Skip {
                                slot,
                                hash: format!("skip-{}", slot),
                                proposed_timestamp: now,
                                finalized_timestamp: Some(now),
                                status: rpc::SlotStatus::Finalized,
                            };
                            blocks_guard.push(skip_block.clone());
                        } else if has_block {
                            if let Some(hash) = blockstore_guard.canonical_block_hash(slot_id) {
                                let block_hash: DoubleMerkleRoot = hash.clone().into();
                                let block_id = (slot_id, block_hash);
                                if let Some(block) = blockstore_guard.get_block(&block_id) {
                                    let h = hex::encode(hash);
                                    let parent_hash = hex::encode(block.parent_hash().as_hash());
                                    let parent_slot = block.parent().inner();

                                    println!(
                                        "Slot {} has new block (status: {:?})",
                                        slot, current_status
                                    );

                                    let leader = epoch_info.leader(slot_id).id;
                                    let now = SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .unwrap()
                                        .as_millis()
                                        as u64;

                                    // for now only copying metadata -> todo @elia for persistence: take care of data as well
                                    blocks_guard.push(rpc::Block::Block {
                                        slot,
                                        hash: h,
                                        parent_slot,
                                        parent_hash,
                                        producer: leader,
                                        proposed_timestamp: now,
                                        finalized_timestamp: if current_status
                                            == rpc::SlotStatus::Finalized
                                        {
                                            Some(now)
                                        } else {
                                            None
                                        },
                                        status: current_status,
                                    });
                                }
                            }
                        }
                    }
                }

                drop(pool_guard);
                drop(blockstore_guard);

                let finalized_count = blocks_guard
                    .iter()
                    .filter(|b| b.status() == rpc::SlotStatus::Finalized)
                    .count();
                let notarized_count = blocks_guard
                    .iter()
                    .filter(|b| b.status() == rpc::SlotStatus::Notarized)
                    .count();
                let proposed_count = blocks_guard
                    .iter()
                    .filter(|b| b.status() == rpc::SlotStatus::Proposed)
                    .count();
                let total_count = blocks_guard.len();

                println!(
                    "Block status summary: {} finalized, {} notarized, {} proposed (total: {})",
                    finalized_count, notarized_count, proposed_count, total_count
                );

                println!(
                    "Node finalized slots: {:?}",
                    nodes_guard
                        .iter()
                        .map(|n| format!("Node {}: slot {}", n.node_id, n.finalized_slot))
                        .collect::<Vec<_>>()
                        .join(", ")
                );

                // execute transactions from newly finalized blocks
                if consensus_finalized_slot > last_executed_slot {
                    let (_i, _pool, blockstore) = &pools_and_blockstores[0];
                    if let Ok(bs) = blockstore.try_read() {
                        for slot in (last_executed_slot + 1)..=consensus_finalized_slot {
                            let slot_id = Slot::new(slot);
                            if let Some(hash) = bs.canonical_block_hash(slot_id) {
                                let blk_hash_hex = hex::encode(&hash);
                                let block_hash: DoubleMerkleRoot = hash.into();
                                let block_id = (slot_id, block_hash);
                                if let Some(block) = bs.get_block(&block_id) {
                                    let raw_txs = block.transactions();
                                    let core_txs: Vec<bunker_coin_core::transaction::Transaction> =
                                        raw_txs
                                            .iter()
                                            .filter_map(|raw| {
                                                // Transaction.0 may have a wincode Vec<u8> length
                                                // prefix (8-byte LE u64) wrapping the bincode payload.
                                                // Try raw first, then try skipping the prefix.
                                                let data = &raw.0;
                                                bincode::serde::decode_from_slice(
                                                    data,
                                                    bincode::config::standard(),
                                                )
                                                .or_else(|_| {
                                                    if data.len() > 8 {
                                                        bincode::serde::decode_from_slice(
                                                            &data[8..],
                                                            bincode::config::standard(),
                                                        )
                                                    } else {
                                                        Err(bincode::error::DecodeError::Other(
                                                            "too short",
                                                        ))
                                                    }
                                                })
                                                .ok()
                                                .map(|(tx, _)| tx)
                                            })
                                            .collect();

                                    if !core_txs.is_empty() {
                                        let results =
                                            execution_state.write().await.execute_block(&core_txs);
                                        let ok_count = results.iter().filter(|r| r.is_ok()).count();
                                        let err_count = results.len() - ok_count;
                                        println!(
                                            "Executed slot {}: {} ok, {} failed ({} total txs)",
                                            slot,
                                            ok_count,
                                            err_count,
                                            core_txs.len()
                                        );

                                        let now = SystemTime::now()
                                            .duration_since(UNIX_EPOCH)
                                            .unwrap()
                                            .as_millis()
                                            as u64;

                                        // record tx results, prune mempool, send WS events
                                        let mut pool = mempool.write().await;
                                        let mut results_map = tx_results.write().await;

                                        for (core_tx, exec_result) in
                                            core_txs.iter().zip(results.iter())
                                        {
                                            let tx_hash = hex::encode(core_tx.hash());

                                            let (status, error) = match exec_result {
                                                Ok(()) => (rpc::TxFinalStatus::Finalized, None),
                                                Err(e) => (
                                                    rpc::TxFinalStatus::Failed,
                                                    Some(e.to_string()),
                                                ),
                                            };

                                            let success = exec_result.is_ok();

                                            results_map.insert(
                                                tx_hash.clone(),
                                                rpc::TxResult {
                                                    hash: tx_hash.clone(),
                                                    slot,
                                                    block_hash: blk_hash_hex.clone(),
                                                    status,
                                                    error: error.clone(),
                                                    executed_at: now,
                                                },
                                            );

                                            // prune from mempool
                                            pool.retain(|entry| entry.hash != tx_hash);

                                            // send WS event
                                            let _ = updates_tx.send(
                                                rpc::WebSocketUpdate::TransactionFinalized {
                                                    hash: tx_hash,
                                                    slot,
                                                    block_hash: blk_hash_hex.clone(),
                                                    success,
                                                    error,
                                                },
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                    last_executed_slot = consensus_finalized_slot;
                }

                for (_i, pool, _blockstore) in &pools_and_blockstores {
                    pool.write().await.prune_old_slots();
                    let pool_guard = pool.read().await;
                    println!("Pool slot_states: {}", pool_guard.slot_states_len());
                }
            }
        })
    };

    let mut node_handles = Vec::new();
    for (i, node) in nodes_with_id {
        let info = node.get_info().clone();
        node_handles.push(tokio::spawn(async move {
            node.run().await.unwrap();
            println!("node {} (id {}) stopped", i, info.id);
        }));
    }

    tokio::signal::ctrl_c().await.unwrap();
    monitoring_task.abort();
    println!("simulation stopped");
    for handle in node_handles {
        let _ = handle.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scs_pactor::SimulatedPactorConfig;
    use std::time::Duration;

    #[tokio::test]
    async fn pactor_radio_proto_demo_round_trips_network_message() {
        let config = SimulatedPactorConfig {
            packet_loss: 0.0,
            latency: Duration::ZERO,
            latency_jitter: Duration::ZERO,
            setup_delay: Duration::ZERO,
            ..Default::default()
        };

        let result = pactor_radio_proto_demo(config).await.unwrap();
        assert_eq!(result.received_messages.len(), 3);
        assert!(matches!(result.received_messages[0], NetworkMessage::Ping));
        match &result.received_messages[1] {
            NetworkMessage::Shred(payload) => {
                assert_eq!(payload, &b"radio-proto-over-pactor".to_vec());
            }
            other => panic!("expected Shred message, got {other:?}"),
        }
        assert!(matches!(result.received_messages[2], NetworkMessage::Pong));
        assert_eq!(result.frames_attempted, 3);
        assert_eq!(result.frames_lost, 0);
        assert_eq!(result.retransmissions, 0);
        assert!(result.bytes_delivered > 0);
    }

    #[tokio::test]
    async fn pactor_radio_proto_degradation_demo_shows_arq_overhead() {
        let result = pactor_radio_proto_degradation_demo().await.unwrap();

        assert_eq!(result.clean.received_messages.len(), 3);
        assert_eq!(result.degraded.received_messages.len(), 3);
        assert_eq!(result.clean.frames_lost, 0);
        assert_eq!(result.clean.retransmissions, 0);
        assert!(result.degraded.frames_lost > result.clean.frames_lost);
        assert!(result.degraded.retransmissions > result.clean.retransmissions);
        assert!(result.degraded.frames_attempted > result.clean.frames_attempted);
        assert_eq!(
            result.degraded.bytes_delivered,
            result.clean.bytes_delivered
        );
    }

    #[test]
    fn pactor_throughput_report_tracks_speed_levels_and_degradation() {
        let report = pactor_throughput_report();

        assert_eq!(report.len(), 4);
        for sample in report {
            assert!(sample.clean_error_pct <= 10.0);
            assert!(sample.degraded_effective_bps < sample.clean_effective_bps);
        }
    }

    #[tokio::test]
    async fn pactor_measured_throughput_report_tracks_clean_rates_and_degradation() {
        let report = pactor_measured_throughput_report().await.unwrap();

        assert_eq!(report.len(), 4);
        for sample in report {
            assert!(
                sample.clean_error_pct <= 10.0,
                "PT-{} clean throughput error was {:.1}%",
                sample.speed_level,
                sample.clean_error_pct
            );
            assert!(sample.degraded_effective_bps < sample.clean_effective_bps);
            assert!(sample.degraded_retransmissions > 0);
        }
    }
}
