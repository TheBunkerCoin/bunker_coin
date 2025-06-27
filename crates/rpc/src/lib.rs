use axum::{routing::get, Router, Json, extract::{Path, Query, WebSocketUpgrade, ws::{Message, WebSocket}}, response::IntoResponse};
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};
use tower_http::cors::{CorsLayer, Any, AllowOrigin};
use axum::http::{Method, HeaderValue};
use futures::{sink::SinkExt, stream::StreamExt};

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "lowercase")]
pub enum SlotStatus {
    Pending,   
    Proposed,
    Notarized,
    Finalized,
}

#[derive(Serialize, Clone)]
#[serde(tag = "type")]
pub enum Block {
    #[serde(rename = "block")]
    Block {
        slot: u64,
        hash: String,
        parent_slot: u64,
        parent_hash: String,
        producer: u64,
        proposed_timestamp: u64,
        finalized_timestamp: Option<u64>,
        status: SlotStatus,
    },
    #[serde(rename = "skip")]
    Skip {
        slot: u64,
        hash: String,
        proposed_timestamp: u64,
        finalized_timestamp: Option<u64>,
        status: SlotStatus,
    }
}

impl Block {
    pub fn slot(&self) -> u64 {
        match self {
            Block::Block { slot, .. } => *slot,
            Block::Skip { slot, .. } => *slot,
        }
    }
    
    pub fn hash(&self) -> &str {
        match self {
            Block::Block { hash, .. } => hash,
            Block::Skip { hash, .. } => hash,
        }
    }
    
    pub fn status(&self) -> SlotStatus {
        match self {
            Block::Block { status, .. } => *status,
            Block::Skip { status, .. } => *status,
        }
    }
    
    pub fn set_status(&mut self, new_status: SlotStatus, finalized_timestamp: Option<u64>) {
        match self {
            Block::Block { status, finalized_timestamp: ft, .. } => {
                *status = new_status;
                if new_status == SlotStatus::Finalized {
                    *ft = finalized_timestamp;
                }
            },
            Block::Skip { status, finalized_timestamp: ft, .. } => {
                *status = new_status;
                if new_status == SlotStatus::Finalized {
                    *ft = finalized_timestamp;
                }
            },
        }
    }
    pub fn proposed_timestamp(&self) -> u64 {
        match self {
            Block::Block { proposed_timestamp, .. } => *proposed_timestamp,
            Block::Skip { proposed_timestamp, .. } => *proposed_timestamp,
        }
    }
    pub fn finalized_timestamp(&self) -> Option<u64> {
        match self {
            Block::Block { finalized_timestamp, .. } => *finalized_timestamp,
            Block::Skip { finalized_timestamp, .. } => *finalized_timestamp,
        }
    }
}

#[derive(Serialize, Clone)]
#[serde(tag = "type")]
pub enum BlockUpdate {
    #[serde(rename = "update_slot")]
    UpdateSlot(Block),
    #[serde(rename = "status_change")]
    StatusChange {
        slot: u64,
        hash: String,
        old_status: SlotStatus,
        new_status: SlotStatus,
    },
}

#[derive(Serialize, Clone)]
#[serde(tag = "type")]
pub enum WebSocketUpdate {
    #[serde(rename = "block_update")]
    BlockUpdate(BlockUpdate),
    #[serde(rename = "radio_stats")]
    RadioStats {
        packets_sent_2s: u64,
        packets_dropped_2s: u64,
        packets_transmitted_2s: u64,
        bytes_transmitted_2s: u64,
        effective_throughput_bps_2s: f64,
        packet_loss_rate_2s: f64,
        packets_queued: u64,
    },
}

#[derive(Serialize, Clone)]
pub struct NodeStatus {
    pub node_id: u64,
    pub finalized_slot: u64,
}

#[derive(Serialize, Clone)]
pub struct RadioStats {
    pub bandwidth_bps: u32,
    pub packet_loss_percent: f32,
    pub latency_ms: u32,
    pub jitter_ms: u32,
    pub packets_sent: u64,
    pub packets_dropped: u64,
    pub current_throughput_bps: f64,
}

#[derive(Clone)]
pub struct SharedState {
    pub blocks: Arc<RwLock<Vec<Block>>>,
    pub nodes: Arc<RwLock<Vec<NodeStatus>>>,
    pub radio_stats: Arc<RwLock<RadioStats>>,
    pub updates: broadcast::Sender<WebSocketUpdate>,
}

#[derive(Deserialize)]
struct Pagination {
    limit: Option<usize>,
    offset: Option<usize>,
}

async fn blocks(Query(p): Query<Pagination>, state: axum::extract::State<SharedState>) -> Json<Vec<Block>> {
    let blocks = state.blocks.read().await;
    let total = blocks.len();
    let limit = p.limit.unwrap_or(100).min(100);
    let offset = p.offset.unwrap_or(0);
    
    if offset >= total {
        return Json(vec![]);
    }
    
    let mut sorted_blocks = blocks.clone();
    sorted_blocks.sort_by(|a, b| b.slot().cmp(&a.slot()));
    
    let start_index = offset;
    let end_index = (offset + limit).min(total);
    
    let result: Vec<Block> = sorted_blocks[start_index..end_index].to_vec();
    
    Json(result)
}

async fn nodes(state: axum::extract::State<SharedState>) -> Json<Vec<NodeStatus>> {
    let nodes = state.nodes.read().await;
    Json(nodes.clone())
}

async fn radio(state: axum::extract::State<SharedState>) -> Json<RadioStats> {
    let stats = state.radio_stats.read().await;
    Json(stats.clone())
}

async fn block(Path(hash): Path<String>, state: axum::extract::State<SharedState>) -> impl IntoResponse {
    let blocks = state.blocks.read().await;
    if let Some(block) = blocks.iter().find(|b| b.hash() == hash) {
        Json(block.clone()).into_response()
    } else {
        axum::http::StatusCode::NOT_FOUND.into_response()
    }
}

async fn websocket_handler(
    ws: WebSocketUpgrade,
    state: axum::extract::State<SharedState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state.0))
}

async fn handle_socket(socket: WebSocket, state: SharedState) {
    let (mut sender, mut receiver) = socket.split();
    let mut rx = state.updates.subscribe();
    
    
    let mut send_task = tokio::spawn(async move {
        while let Ok(update) = rx.recv().await {
            if let Ok(msg) = serde_json::to_string(&update) {
                if sender.send(Message::Text(msg)).await.is_err() {
                    break;
                }
            }
        }
    });
    
    let mut recv_task = tokio::spawn(async move {
        while let Some(msg) = receiver.next().await {
            if let Ok(Message::Close(_)) = msg {
                break;
            }
        }
    });
    
    tokio::select! {
        _ = &mut send_task => recv_task.abort(),
        _ = &mut recv_task => send_task.abort(),
    }
}

pub async fn run_api(state: SharedState) {
    let cors = CorsLayer::new()
        .allow_methods([Method::GET])
        .allow_headers(Any)
        .allow_origin(AllowOrigin::predicate(|origin: &HeaderValue, _| {
            if let Ok(o) = origin.to_str() {
                o.starts_with("http://localhost") || o.ends_with(".bunkercoin.io")
            } else {
                false
            }
        }));
    let app = Router::new()
        .route("/blocks", get(blocks))
        .route("/nodes", get(nodes))
        .route("/radio", get(radio))
        .route("/block/:hash", get(block))
        .route("/ws", get(websocket_handler))
        .layer(cors)
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await.unwrap();
    axum::serve(listener, app).await.unwrap();
} 