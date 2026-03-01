use axum::{routing::{get, post}, Router, Json, extract::{Path, Query, WebSocketUpgrade, ws::{Message, WebSocket}}, response::IntoResponse};
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{RwLock, broadcast, mpsc};
use tower_http::cors::{CorsLayer, Any, AllowOrigin};
use axum::http::{Method, HeaderValue};
use futures::{sink::SinkExt, stream::StreamExt};
use bunkerglow::consensus::Blockstore;
use bunkerglow::crypto::Hash;
use bunker_coin_core::transaction::{Transaction as CoreTransaction, TransactionBody};
use bunker_coin_core::types::MAX_TICKER_LEN;
use hex;

const MAX_MEMPOOL_SIZE: usize = 10_000;

// -- block types --

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

// -- websocket types --

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
    #[serde(rename = "transaction_received")]
    TransactionReceived {
        hash: String,
        sender: String,
        fee: u64,
        body_type: String,
    },
}

// -- node / radio types --

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

// -- transaction / mempool types --

#[derive(Serialize, Clone)]
pub struct MempoolEntry {
    pub hash: String,
    pub sender: String,
    pub nonce: u64,
    pub fee: u64,
    pub body_type: String,
    pub received_at: u64,
}

#[derive(Deserialize)]
struct SubmitTransactionRequest {
    sender: String,
    nonce: u64,
    fee: u64,
    body: TransactionBodyRequest,
    signature: String,
}

#[derive(Deserialize)]
enum TransactionBodyRequest {
    Transfer { to: String, amount: u64 },
    TokenTransfer { to: String, token_id: String, amount: u64 },
    Mint { ticker: String, max_supply: u64, metadata_hash: String },
    Bond { validator: String, amount: u64 },
    Retire { validator: String, amount: u64 },
    Withdraw { validator: String },
    UnJail,
}

// -- shared state --

#[derive(Clone)]
pub struct SharedState {
    pub blocks: Arc<RwLock<Vec<Block>>>,
    pub nodes: Arc<RwLock<Vec<NodeStatus>>>,
    pub radio_stats: Arc<RwLock<RadioStats>>,
    pub updates: broadcast::Sender<WebSocketUpdate>,
    pub blockstore: Option<Arc<RwLock<Blockstore>>>,
    pub mempool: Arc<RwLock<Vec<MempoolEntry>>>,
    pub tx_sender: Option<mpsc::UnboundedSender<CoreTransaction>>,
}

// -- hex decode helpers --

fn decode_pubkey(hex_str: &str) -> Result<[u8; 32], String> {
    let bytes = hex::decode(hex_str).map_err(|e| format!("invalid hex: {e}"))?;
    <[u8; 32]>::try_from(bytes.as_slice())
        .map_err(|_| format!("expected 32 bytes, got {}", bytes.len()))
}

fn decode_signature(hex_str: &str) -> Result<[u8; 64], String> {
    let bytes = hex::decode(hex_str).map_err(|e| format!("invalid hex: {e}"))?;
    <[u8; 64]>::try_from(bytes.as_slice())
        .map_err(|_| format!("expected 64 bytes, got {}", bytes.len()))
}

fn decode_token_id(hex_str: &str) -> Result<[u8; 4], String> {
    let bytes = hex::decode(hex_str).map_err(|e| format!("invalid hex: {e}"))?;
    <[u8; 4]>::try_from(bytes.as_slice())
        .map_err(|_| format!("expected 4 bytes, got {}", bytes.len()))
}

fn decode_hash32(hex_str: &str) -> Result<[u8; 32], String> {
    let bytes = hex::decode(hex_str).map_err(|e| format!("invalid hex: {e}"))?;
    <[u8; 32]>::try_from(bytes.as_slice())
        .map_err(|_| format!("expected 32 bytes, got {}", bytes.len()))
}

fn convert_body(body: TransactionBodyRequest) -> Result<TransactionBody, String> {
    match body {
        TransactionBodyRequest::Transfer { to, amount } => {
            Ok(TransactionBody::Transfer { to: decode_pubkey(&to)?, amount })
        }
        TransactionBodyRequest::TokenTransfer { to, token_id, amount } => {
            Ok(TransactionBody::TokenTransfer {
                to: decode_pubkey(&to)?,
                token_id: decode_token_id(&token_id)?,
                amount,
            })
        }
        TransactionBodyRequest::Mint { ticker, max_supply, metadata_hash } => {
            if ticker.len() < 3 || ticker.len() > MAX_TICKER_LEN {
                return Err(format!("ticker must be 3-{MAX_TICKER_LEN} characters"));
            }
            Ok(TransactionBody::Mint {
                ticker,
                max_supply,
                metadata_hash: decode_hash32(&metadata_hash)?,
            })
        }
        TransactionBodyRequest::Bond { validator, amount } => {
            Ok(TransactionBody::Bond { validator: decode_pubkey(&validator)?, amount })
        }
        TransactionBodyRequest::Retire { validator, amount } => {
            Ok(TransactionBody::Retire { validator: decode_pubkey(&validator)?, amount })
        }
        TransactionBodyRequest::Withdraw { validator } => {
            Ok(TransactionBody::Withdraw { validator: decode_pubkey(&validator)? })
        }
        TransactionBodyRequest::UnJail => Ok(TransactionBody::UnJail),
    }
}

fn body_type_name(body: &TransactionBody) -> &'static str {
    match body {
        TransactionBody::Transfer { .. } => "Transfer",
        TransactionBody::TokenTransfer { .. } => "TokenTransfer",
        TransactionBody::Mint { .. } => "Mint",
        TransactionBody::Bond { .. } => "Bond",
        TransactionBody::Retire { .. } => "Retire",
        TransactionBody::Withdraw { .. } => "Withdraw",
        TransactionBody::UnJail => "UnJail",
    }
}

// -- query params --

#[derive(Deserialize)]
struct Pagination {
    limit: Option<usize>,
    offset: Option<usize>,
}

// -- transaction handlers --

async fn submit_transaction(
    state: axum::extract::State<SharedState>,
    Json(req): Json<SubmitTransactionRequest>,
) -> impl IntoResponse {
    let sender = match decode_pubkey(&req.sender) {
        Ok(k) => k,
        Err(e) => return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": format!("invalid sender: {e}") })),
        ).into_response(),
    };

    let signature = match decode_signature(&req.signature) {
        Ok(s) => s,
        Err(e) => return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": format!("invalid signature: {e}") })),
        ).into_response(),
    };

    let body = match convert_body(req.body) {
        Ok(b) => b,
        Err(e) => return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": e })),
        ).into_response(),
    };

    let tx = CoreTransaction {
        sender,
        nonce: req.nonce,
        fee: req.fee,
        body,
        signature,
    };

    let hash = hex::encode(tx.hash());
    let body_type = body_type_name(&tx.body);

    // duplicate check + size limit
    {
        let mempool = state.mempool.read().await;
        if mempool.iter().any(|e| e.hash == hash) {
            return (
                axum::http::StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": "transaction already in mempool", "hash": hash })),
            ).into_response();
        }
        if mempool.len() >= MAX_MEMPOOL_SIZE {
            return (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({ "error": "mempool full" })),
            ).into_response();
        }
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let entry = MempoolEntry {
        hash: hash.clone(),
        sender: req.sender.clone(),
        nonce: tx.nonce,
        fee: tx.fee,
        body_type: body_type.to_string(),
        received_at: now,
    };

    state.mempool.write().await.push(entry);

    if let Some(tx_sender) = &state.tx_sender {
        let _ = tx_sender.send(tx);
    }

    let _ = state.updates.send(WebSocketUpdate::TransactionReceived {
        hash: hash.clone(),
        sender: req.sender,
        fee: req.fee,
        body_type: body_type.to_string(),
    });

    Json(serde_json::json!({ "hash": hash })).into_response()
}

async fn mempool(
    Query(p): Query<Pagination>,
    state: axum::extract::State<SharedState>,
) -> Json<serde_json::Value> {
    let limit = p.limit.unwrap_or(100).min(500);
    let offset = p.offset.unwrap_or(0);

    let pool = state.mempool.read().await;
    let total = pool.len();

    if offset >= total {
        return Json(serde_json::json!({ "transactions": [], "total": total, "limit": limit, "offset": offset }));
    }

    let end = (offset + limit).min(total);
    let txs: Vec<_> = pool[offset..end].to_vec();

    Json(serde_json::json!({
        "transactions": txs,
        "total": total,
        "limit": limit,
        "offset": offset,
    }))
}

async fn mempool_transaction(
    Path(hash): Path<String>,
    state: axum::extract::State<SharedState>,
) -> impl IntoResponse {
    let pool = state.mempool.read().await;
    if let Some(entry) = pool.iter().find(|e| e.hash == hash) {
        return Json(serde_json::json!(entry)).into_response();
    }
    axum::http::StatusCode::NOT_FOUND.into_response()
}

// -- block handlers --

// this probably qualifies for a rewrite soon:tm:
async fn blocks(Query(p): Query<Pagination>, state: axum::extract::State<SharedState>) -> Json<Vec<Block>> {
    let limit = p.limit.unwrap_or(100).min(100);
    let offset = p.offset.unwrap_or(0);

    let mut all_blocks = {
        let blocks = state.blocks.read().await;
        blocks.clone()
    };

    if let Some(bs_arc) = &state.blockstore {
        let bs = bs_arc.read().await;

        let highest_mem_slot = all_blocks.iter().map(|b| b.slot()).max().unwrap_or(0);

        for slot in 0..=highest_mem_slot + 200 {

            if all_blocks.iter().any(|b| b.slot() == slot) {
                continue;
            }

            if let Some(hash) = bs.canonical_block_hash(slot) {
                if let Some(block) = bs.get_block(slot, hash) {
                    let (producer, proposed_timestamp, finalized_timestamp) =
                        if let Some(metadata) = bs.load_block_metadata(slot, hash) {
                            (metadata.producer, metadata.proposed_timestamp, metadata.finalized_timestamp)
                        } else {
                            (0, 0, Some(0))
                        };

                    let status = if finalized_timestamp.is_some() {
                        SlotStatus::Finalized
                    } else {
                        SlotStatus::Proposed
                    };

                    let api_block = Block::Block {
                        slot,
                        hash: hex::encode(hash),
                        parent_slot: block.parent(),
                        parent_hash: hex::encode(block.parent_hash()),
                        producer,
                        proposed_timestamp,
                        finalized_timestamp,
                        status,
                    };
                    all_blocks.push(api_block);
                }
            }
        }
    }

    all_blocks.sort_by(|a, b| b.slot().cmp(&a.slot()));

    let total = all_blocks.len();
    if offset >= total {
        return Json(vec![]);
    }

    let start_index = offset;
    let end_index = (offset + limit).min(total);

    let result: Vec<Block> = all_blocks[start_index..end_index].to_vec();

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
        return Json(block.clone()).into_response();
    }

    if let Some(bs_arc) = &state.blockstore {
        if let Ok(hash_bytes) = hex::decode(&hash) {
            if hash_bytes.len() == 32 {
                let mut hash_arr = [0u8;32];
                hash_arr.copy_from_slice(&hash_bytes);
                let bs = bs_arc.read().await;
                if let Some((_slot, blk)) = bs.load_block_by_hash(hash_arr) {
                    let slot = blk.slot();

                    let (producer, proposed_timestamp, finalized_timestamp) =
                        if let Some(metadata) = bs.load_block_metadata(slot, hash_arr) {
                            (metadata.producer, metadata.proposed_timestamp, metadata.finalized_timestamp)
                        } else {
                            (0, 0, Some(0))
                        };

                    let status = if finalized_timestamp.is_some() {
                        SlotStatus::Finalized
                    } else {
                        SlotStatus::Proposed
                    };

                    let api_block = Block::Block {
                        slot,
                        hash: hash.clone(),
                        parent_slot: blk.parent(),
                        parent_hash: hex::encode(blk.parent_hash()),
                        producer,
                        proposed_timestamp,
                        finalized_timestamp,
                        status,
                    };
                    return Json(api_block).into_response();
                }
            }
        }
    }
    axum::http::StatusCode::NOT_FOUND.into_response()
}

// -- websocket --

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

// -- server --

pub async fn run_api(state: SharedState) {
    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST])
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
        .route("/block/{hash}", get(block))
        .route("/transactions", post(submit_transaction))
        .route("/mempool", get(mempool))
        .route("/mempool/{hash}", get(mempool_transaction))
        .route("/ws", get(websocket_handler))
        .layer(cors)
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
