//! multi-node radio simulation for BunkerCoin

use bunker_coin_sim::scenarios;
use bunker_coin_radio::RadioConfig;
use rpc::{run_api, Block, NodeStatus, RadioStats, SharedState, WebSocketUpdate};
use std::sync::Arc;
use tokio::task;
use tokio::sync::{RwLock, broadcast};

#[tokio::main]
async fn main() {
    env_logger::init();
    
    log::info!("=== BunkerCoin Node Starting ===");
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
    }));
    let (updates_tx, _) = broadcast::channel(1000);
    
    let num_nodes = 4;
    log::info!("Starting {}-node consensus simulation over radio network", num_nodes);
    
    let blockstore_ref = Arc::new(RwLock::new(None));
    let blockstore_for_api = blockstore_ref.clone();
    
    let state = SharedState {
        blocks: blocks.clone(),
        nodes: nodes.clone(),
        radio_stats: radio_stats.clone(),
        updates: updates_tx.clone(),
        blockstore: None,
    };
    
    // api in dedicated task
    let api_handle = task::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        
        let bs = blockstore_for_api.read().await.clone();
        let mut state = state;
        state.blockstore = bs;
        
        run_api(state).await;
    });
    
    scenarios::multi_node_consensus_simulation_with_api(num_nodes, blocks, nodes, radio_stats, updates_tx, blockstore_ref).await;
    
    log::info!("Simulation completed, shutting down API server");
    api_handle.await.unwrap();
} 