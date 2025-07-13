//! Simulation scenarios for testing BunkerCoin over radio

use bunker_coin_radio::{SimulatedRadioNetwork, RadioConfig, RadioNetworkCore};
use alpenglow::shredder::{Slice, RegularShredder, Shredder, MAX_DATA_PER_SLICE};
use alpenglow::network::{Network, NetworkMessage};
use alpenglow::crypto::signature::SecretKey;
use hex;
use std::time::{SystemTime, UNIX_EPOCH};
use bincode;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use rpc;

pub async fn basic_consensus_test(config: RadioConfig, num_validators: u64) {
    println!("Starting basic consensus test with {} validators", num_validators);
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
    let slice = Slice {
        slot: 1,
        slice_index: 0,
        is_last: true,
        merkle_root: None,
        data: vec![0x42; data_size],
    };
    
    let sk = SecretKey::new(&mut rand::rng());
    let shreds = RegularShredder::shred(&slice, &sk).unwrap();
    
    println!("✓ Created {} shreds from {} bytes", shreds.len(), data_size);
    

    if let Some(first_shred) = shreds.first() {
        let shred_size = bincode::serde::encode_to_vec(first_shred, bincode::config::standard())
            .map(|v| v.len())
            .unwrap_or(0);
        println!("  Each shred is ~{} bytes (needs {} radio frames)", 
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
    println!("  Stats: {} sent, {} dropped", 
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
    println!("Effective throughput: ~{} bps", 
        (5 * 100 * 8) as f64 / elapsed.as_secs_f64()
    );
    
    println!("\n=== Radio Layer Test Complete ===\n");
    
    println!("Note: Full consensus testing requires implementing proper message routing between nodes.");
    println!("The current implementation demonstrates the radio constraints but doesn't route messages between validators.");
}

pub async fn bandwidth_test(config: RadioConfig) {
    println!("\n=== Bandwidth Test ===");
    println!("testing bandwidth with config: {:?}", config);
    
    let radio = SimulatedRadioNetwork::new(config.clone());
    
    let test_sizes = vec![
        320,
        1024,
        3200
    ];
    
    for size in test_sizes {
        println!("\ntesting with {} bytes of data >>>", size);
        
        let slice = Slice {
            slot: 1,
            slice_index: 0,
            is_last: true,
            merkle_root: None,
            data: vec![0x55; size],
        };
        
        let sk = SecretKey::new(&mut rand::rng());
        let shreds = match RegularShredder::shred(&slice, &sk) {
            Ok(s) => s,
            Err(e) => {
                println!("  ✗ Failed to create shreds: {:?}", e);
                continue;
            }
        };
        
        let mut total_bytes = 0;
        let start = tokio::time::Instant::now();
        
        for shred in &shreds {
            if let Ok(serialized) = bincode::serde::encode_to_vec(shred, bincode::config::standard()) {
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
        println!("  - efficiency vs theoretical: {:.1}%", 
            throughput / config.bandwidth_bps as f64 * 100.0
        );
    }
}

pub async fn multi_node_radio_simulation(num_nodes: usize, config: RadioConfig) {
    return;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use std::collections::HashMap;
    use bunker_coin_radio::SimulatedRadioNetwork;
    use alpenglow::network::NetworkMessage;

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
    for h in handles { let _ = h.await; }
    println!("=== Multi-Node Simulation Complete ===\n");
}

pub async fn multi_node_real_radio_simulation(num_nodes: usize) {
    use std::sync::Arc;
    use alpenglow::network::simulated::SimulatedNetworkCore;
    use alpenglow::network::{Network, NetworkMessage};

    println!("\n-- nodes on top of Alpenglow test (via simulated radio)");
    let core = Arc::new(SimulatedNetworkCore::new());
    let mut nets = Vec::new();
    for node_id in 0..num_nodes {
        let net = core.join_unlimited(node_id as u64).await;
        nets.push(net);
    }

    let msg = NetworkMessage::Ping;
    for i in 1..num_nodes {
        nets[0].send(&msg, &(i as u64).to_string()).await.unwrap();
        println!("node 0 sent ping to node {}", i);
    }

    for i in 1..num_nodes {
        let received = nets[i].receive().await.unwrap();
        println!("node {} received {:?}", i, received);
    }
    println!("no crash :)\n");
}

pub async fn multi_node_consensus_simulation(num_nodes: usize) {
    use std::sync::Arc;
    use alpenglow::all2all::TrivialAll2All;
    use alpenglow::consensus::{Alpenglow, EpochInfo};
    use alpenglow::crypto::{aggsig, signature::SecretKey};
    use alpenglow::disseminator::{Rotor, rotor::StakeWeightedSampler};
    use alpenglow::network::simulated::SimulatedNetworkCore;
    use alpenglow::network::SimulatedNetwork;
    use alpenglow::ValidatorInfo;
    use tokio::time::Duration;

    println!("\n>> Multi-Node Alpenglow Consensus Simulation <<");
    let core = Arc::new(SimulatedNetworkCore::new().with_packet_loss(0.05));
    let mut networks = Vec::new();
    for i in 0..3 * num_nodes {
        networks.push(core.join_unlimited(i as u64).await);
    }

    let mut rng = rand::rng();
    let mut sks = Vec::new();
    let mut voting_sks = Vec::new();
    let mut validators = Vec::new();
    for id in 0..num_nodes {
        sks.push(SecretKey::new(&mut rng));
        voting_sks.push(aggsig::SecretKey::new(&mut rng));
        let a2a_port = 3 * id;
        let dis_port = 3 * id + 1;
        let rep_port = 3 * id + 2;
        validators.push(ValidatorInfo {
            id: id as u64,
            stake: 1,
            pubkey: sks[id].to_pk(),
            voting_pubkey: voting_sks[id].to_pk(),
            all2all_address: format!("{a2a_port}"),
            disseminator_address: format!("{dis_port}"),
            repair_address: format!("{rep_port}"),
        });
    }

    let mut nodes_with_id = Vec::new();
    for (i, v) in validators.iter().enumerate() {
        let epoch_info = Arc::new(EpochInfo::new(v.id, validators.clone()));
        let all2all = TrivialAll2All::new(validators.clone(), networks.remove(0));
        let disseminator = Rotor::new(networks.remove(0), epoch_info.clone());
        let repair_network = networks.remove(0);
        let node = Alpenglow::new(
            sks[i].clone(),
            voting_sks[i].clone(),
            all2all,
            disseminator,
            repair_network,
            epoch_info,
        );
        nodes_with_id.push((i, node));
    }

    let mut pools_and_blockstores = Vec::new();
    for (i, node) in &nodes_with_id {
        pools_and_blockstores.push((
            *i,
            node.get_pool(),
            node.blockstore(),
        ));
    }
    let print_task = {
        let pools_and_blockstores = pools_and_blockstores.clone();
        
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;
                for (i, pool, blockstore) in &pools_and_blockstores {
                    let finalized = pool.read().await.finalized_slot();
                    let prune_slot = finalized.saturating_sub(200);
                    pool.write().await.prune();
                    blockstore.write().await.prune(prune_slot);
                    let pool_guard = pool.read().await;
                    let blockstore_guard = blockstore.read().await;
                    println!("pool slot_states: {}", pool_guard.slot_states_len());
                    println!("blockstore blocks: {}", blockstore_guard.blocks_len());
                    println!("blockstore shreds: {}", blockstore_guard.shreds_len());
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
    blockstore_ref: std::sync::Arc<tokio::sync::RwLock<Option<std::sync::Arc<tokio::sync::RwLock<alpenglow::consensus::Blockstore>>>>>
) {
    use std::sync::Arc;
    use alpenglow::all2all::TrivialAll2All;
    use alpenglow::consensus::{Alpenglow, EpochInfo};
    use alpenglow::crypto::{aggsig, signature::SecretKey};
    use alpenglow::disseminator::Rotor;
    use bunker_coin_radio::{RadioNetworkCore, RadioConfig};
    use alpenglow::ValidatorInfo;
    use tokio::time::Duration;
    use hex;

    log::info!(">> Multi-Node Alpenglow Consensus Simulation (Radio Network) <<");
    println!("\n>> Multi-Node Alpenglow Consensus Simulation (Radio Network) <<");
    
    let radio_config = RadioConfig {
        mtu: 300,                              
        bandwidth_bps: 4800,                   
        packet_loss: 0.15,                     
        latency: Duration::from_millis(250),   
        latency_jitter: Duration::from_millis(50), 
        transmission_window: Duration::from_secs(300),
    };
    
    
    println!("Radio config: {:?}", radio_config);
    
    let radio_core = RadioNetworkCore::new(radio_config);
    let mut networks = Vec::new();
    
    for i in 0..(3 * num_nodes) {
        networks.push(radio_core.join(i as u64).await);
    }

    let mut rng = rand::rng();
    let mut sks = Vec::new();
    let mut voting_sks = Vec::new();
    let mut validators = Vec::new();
    for id in 0..num_nodes {
        sks.push(SecretKey::new(&mut rng));
        voting_sks.push(aggsig::SecretKey::new(&mut rng));
        let a2a_port = 3 * id;
        let dis_port = 3 * id + 1;
        let rep_port = 3 * id + 2;
        validators.push(ValidatorInfo {
            id: id as u64,
            stake: 1,
            pubkey: sks[id].to_pk(),
            voting_pubkey: voting_sks[id].to_pk(),
            all2all_address: format!("{a2a_port}"),
            disseminator_address: format!("{dis_port}"),
            repair_address: format!("{rep_port}"),
        });
    }

    let mut nodes_with_id = Vec::new();
    for (i, v) in validators.iter().enumerate() {
        let epoch_info = Arc::new(EpochInfo::new(v.id, validators.clone()));
        let all2all = TrivialAll2All::new(validators.clone(), networks.remove(0));
        let disseminator = Rotor::new(networks.remove(0), epoch_info.clone());
        let repair_network = networks.remove(0);
        let node = Alpenglow::new(
            sks[i].clone(),
            voting_sks[i].clone(),
            all2all,
            disseminator,
            repair_network,
            epoch_info,
        );
        nodes_with_id.push((i, node));
    }

    let mut pools_and_blockstores = Vec::new();
    for (i, node) in &nodes_with_id {
        pools_and_blockstores.push((
            *i,
            node.get_pool(),
            node.blockstore(),
        ));
    }
    
    // first node's blockstore is used for api
    if let Some((_, _, blockstore)) = pools_and_blockstores.first() {
        let mut bs_ref = blockstore_ref.write().await;
        *bs_ref = Some(blockstore.clone());
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
        let radio_stats = radio_stats.clone();
        let updates_tx = updates_tx.clone();
        let radio_core = radio_core.clone();
        let pools_and_blockstores = pools_and_blockstores.clone();
        let validators = validators.clone();
        
        tokio::spawn(async move {
            let epoch_info = alpenglow::consensus::EpochInfo::new(0, validators.clone());
            let mut last_stats = (0u64, 0u64, 0u64, 0u64);
            
            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;
                
                let (sent, dropped, transmitted, bytes, queued) = radio_core.get_stats().await;
                let (delta_sent, delta_dropped, delta_transmitted, delta_bytes) = (
                    sent - last_stats.0,
                    dropped - last_stats.1,
                    transmitted - last_stats.2,
                    bytes - last_stats.3,
                );
                last_stats = (sent, dropped, transmitted, bytes);

                if delta_sent > 0 || delta_dropped > 0 || delta_transmitted > 0 || queued > 0 {
                    println!("\n📡 Radio Network Stats (last 2s):");
                    println!("  Packets sent: {} (total: {})", delta_sent, sent);
                    println!("  Packets dropped: {} (total: {})", delta_dropped, dropped);
                    println!("  Packets transmitted: {} (total: {})", delta_transmitted, transmitted);
                    println!("  Packets queued: {} (current queue depth)", queued);
                    println!("  Bytes transmitted: {} (total: {})", delta_bytes, bytes);
                    println!("  Effective throughput: {:.2} bps", (delta_bytes * 8) as f64 / 2.0);
                    println!("  Packet loss rate: {:.1}%", 
                        if delta_dropped + delta_transmitted > 0 {
                            (delta_dropped as f64 / (delta_dropped + delta_transmitted) as f64) * 100.0
                        } else { 0.0 }
                    );
                }
                let _ = updates_tx.send(rpc::WebSocketUpdate::RadioStats {
                    packets_sent_2s: delta_sent,
                    packets_dropped_2s: delta_dropped,
                    packets_transmitted_2s: delta_transmitted,
                    bytes_transmitted_2s: delta_bytes,
                    effective_throughput_bps_2s: (delta_bytes * 8) as f64 / 2.0,
                    packet_loss_rate_2s: if delta_dropped + delta_transmitted > 0 {
                        (delta_dropped as f64 / (delta_dropped + delta_transmitted) as f64) * 100.0
                    } else { 
                        0.0 
                    },
                    packets_queued: queued,
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
                    let pool_finalized = finalized; 
                    drop(pool_guard);
                    
                    nodes_guard[*i].finalized_slot = finalized;
                    highest_finalized = highest_finalized.max(finalized);
                    all_finalized_slots.push((i, finalized));
                }
                
                let min_finalized = all_finalized_slots.iter().map(|(_, f)| *f).min().unwrap_or(0);
                let max_finalized = all_finalized_slots.iter().map(|(_, f)| *f).max().unwrap_or(0);
                if min_finalized != max_finalized {
                    println!("WARNING: Nodes have different finalized slots! Min: {}, Max: {}", min_finalized, max_finalized);
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
                let existing_slots: std::collections::HashSet<u64> = blocks_guard
                    .iter()
                    .map(|b| b.slot())
                    .collect();
                
                let min_slot = highest_finalized.saturating_sub(10); 
                for slot in min_slot..=max_slot {
                    if !existing_slots.contains(&slot) {
                        non_finalized_slots.push(slot);
                    }
                }
                
                non_finalized_slots.sort();
                non_finalized_slots.dedup(); 
                
                if non_finalized_slots.len() > 100 {
                    println!("WARNING: Too many slots to check ({}), limiting to 100", non_finalized_slots.len());
                    non_finalized_slots.truncate(100);
                }
                
                if !non_finalized_slots.is_empty() {
                    println!("Checking {} non-finalized slots from {} to {} (highest finalized: {})", 
                        non_finalized_slots.len(), 
                        non_finalized_slots.first().unwrap(),
                        non_finalized_slots.last().unwrap(),
                        highest_finalized);
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
                    let has_block = blockstore_guard.canonical_block_hash(slot).is_some();
                    let is_skip_certified = pool_guard.is_skip_certified(slot);
                    let is_finalized = pool_guard.is_finalized(slot);
                    let is_notarized = pool_guard.is_notarized(slot);
                    let is_notarized_fallback = pool_guard.is_notarized_fallback(slot);
                    
                    if slot == 61 {
                        println!("\nfull logging slot >> {}", slot);
                        println!("  has_block: {}", has_block);
                        println!("  is_skip_certified: {}", is_skip_certified);
                        println!("  is_finalized: {}", is_finalized);
                        println!("  is_notarized: {}", is_notarized);
                        println!("  is_notarized_fallback: {}", is_notarized_fallback);
                        
                        if let Some(hash) = blockstore_guard.canonical_block_hash(slot) {
                            println!("  canonical_hash: {}", hex::encode(hash));
                            
                            let finalized_slot = pool_guard.finalized_slot();
                            let tip = pool_guard.get_tip();
                            println!("  pool finalized_slot: {}", finalized_slot);
                            println!("  pool tip: {}", tip);
                            println!("  slot vs finalized: {} ({})", 
                                slot <= finalized_slot, 
                                if slot <= finalized_slot { "should be finalized" } else { "not yet finalized" }
                            );
                        }
                        println!("=== END DEBUG ===\n");
                    }
                    
                    if !has_block && !is_skip_certified && !is_finalized && !is_notarized && !is_notarized_fallback {
                        continue;
                    }
                    
                    let pool_finalized_slot = pool_guard.finalized_slot();
                    let finalized_by_cert = is_finalized && pool_finalized_slot >= slot;

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
                            println!("Slot {} status: {:?} -> {:?} (final={}, notar={}, notar_fb={}, skip={}, has_block={}, finalized_slot={})",
                                slot, old_status, current_status, 
                                is_finalized, is_notarized, is_notarized_fallback, is_skip_certified, has_block,
                                pool_guard.finalized_slot());
                            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
                            existing.set_status(current_status, if current_status == rpc::SlotStatus::Finalized { Some(now) } else { None });
                            let _ = updates_tx.send(rpc::WebSocketUpdate::BlockUpdate(rpc::BlockUpdate::UpdateSlot(existing.clone())));
                            
                            if existing.status() != current_status {
                                println!("ERROR: Status was not updated! Still shows as {:?}", existing.status());
                            } else {
                                let verify_slot = existing.slot();
                                let found_in_list = blocks_guard.iter().find(|b| b.slot() == verify_slot);
                                if let Some(found) = found_in_list {
                                    if found.status() != current_status {
                                        println!("ERROR: Block in list has wrong status! Expected {:?}, got {:?}", current_status, found.status());
                                    }
                                } else {
                                    println!("ERROR: Block not found in list after update!");
                                }
                            }
                        } else if status_rank(current_status) < status_rank(old_status) {
                            println!("WARNING: Slot {} apparent status regression: {:?} -> {:?} (final={}, notar={}, notar_fb={}, skip={}, has_block={})",
                                slot, old_status, current_status, 
                                is_finalized, is_notarized, is_notarized_fallback, is_skip_certified, has_block);
                        }
                    } else {
                        if is_skip_certified {
                            println!("Slot {} is skip certified", slot);
                            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
                            let skip_block = rpc::Block::Skip {
                                slot,
                                hash: format!("skip-{}", slot),
                                proposed_timestamp: now,
                                finalized_timestamp: Some(now),
                                status: rpc::SlotStatus::Finalized,
                            };
                            blocks_guard.push(skip_block.clone());
                            
                        } else if has_block {
                            if let Some(hash) = blockstore_guard.canonical_block_hash(slot) {
                                if let Some(block) = blockstore_guard.get_block(slot, hash) {
                                    let h = hex::encode(hash);
                                    let parent_hash = hex::encode(block.parent_hash());
                                    let parent_slot = block.parent();
                                    
                                    println!("Slot {} has new block (status: {:?})", slot, current_status);
                                    
                                    let leader = epoch_info.leader(slot).id;
                                    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
                                    
                                    // for now only copying metadata -> todo @elia for persistence: take care of data as well
                                    blocks_guard.push(rpc::Block::Block {
                                        slot,
                                        hash: h,
                                        parent_slot,
                                        parent_hash,
                                        producer: leader,
                                        proposed_timestamp: now,
                                        finalized_timestamp: if current_status == rpc::SlotStatus::Finalized { Some(now) } else { None },
                                        status: current_status,
                                    });
                                    
                                }
                            }
                        }
                    }
                }
                
                drop(pool_guard);
                drop(blockstore_guard);
                
                let finalized_count = blocks_guard.iter().filter(|b| b.status() == rpc::SlotStatus::Finalized).count();
                let notarized_count = blocks_guard.iter().filter(|b| b.status() == rpc::SlotStatus::Notarized).count();
                let proposed_count = blocks_guard.iter().filter(|b| b.status() == rpc::SlotStatus::Proposed).count();
                let total_count = blocks_guard.len();
                
                println!("Block status summary: {} finalized, {} notarized, {} proposed (total: {})",
                    finalized_count, notarized_count, proposed_count, total_count);
                
                // Print node finalized slots summary
                println!("Node finalized slots: {:?}", 
                    nodes_guard.iter()
                        .map(|n| format!("Node {}: slot {}", n.node_id, n.finalized_slot))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                
                for (_i, pool, blockstore) in &pools_and_blockstores {
                    let finalized = pool.read().await.finalized_slot();
                    let prune_slot = finalized.saturating_sub(200);
                    pool.write().await.prune();
                    blockstore.write().await.prune(prune_slot);
                    let pool_guard = pool.read().await;
                    let blockstore_guard = blockstore.read().await;
                    println!("Pool slot_states: {}", pool_guard.slot_states_len());
                    println!("Blockstore blocks: {}", blockstore_guard.blocks_len());
                    println!("Blockstore shreds: {}", blockstore_guard.shreds_len());
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