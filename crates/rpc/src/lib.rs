use axum::http::{HeaderValue, Method};
use axum::{
    extract::{
        ws::{Message, WebSocket},
        Path, Query, WebSocketUpgrade,
    },
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use bunker_coin_core::execution::State as ExecutionState;
use bunker_coin_core::transaction::{Transaction as CoreTransaction, TransactionBody};
use bunker_coin_core::types::MAX_TICKER_LEN;
use bunkerglow::consensus::Blockstore;
use bunkerglow::crypto::merkle::{DoubleMerkleRoot, MerkleRoot};
use bunkerglow::crypto::Hash;
use bunkerglow::Slot;
use futures::{sink::SinkExt, stream::StreamExt};
use hex;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, mpsc, RwLock};
use tower_http::cors::{AllowOrigin, Any, CorsLayer};

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
    },
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
            Block::Block {
                status,
                finalized_timestamp: ft,
                ..
            } => {
                *status = new_status;
                if new_status == SlotStatus::Finalized {
                    *ft = finalized_timestamp;
                }
            }
            Block::Skip {
                status,
                finalized_timestamp: ft,
                ..
            } => {
                *status = new_status;
                if new_status == SlotStatus::Finalized {
                    *ft = finalized_timestamp;
                }
            }
        }
    }
    pub fn proposed_timestamp(&self) -> u64 {
        match self {
            Block::Block {
                proposed_timestamp, ..
            } => *proposed_timestamp,
            Block::Skip {
                proposed_timestamp, ..
            } => *proposed_timestamp,
        }
    }
    pub fn finalized_timestamp(&self) -> Option<u64> {
        match self {
            Block::Block {
                finalized_timestamp,
                ..
            } => *finalized_timestamp,
            Block::Skip {
                finalized_timestamp,
                ..
            } => *finalized_timestamp,
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
    Transfer {
        to: String,
        amount: u64,
    },
    TokenTransfer {
        to: String,
        token_id: String,
        amount: u64,
    },
    Mint {
        ticker: String,
        max_supply: u64,
        metadata_hash: String,
    },
    Bond {
        validator: String,
        amount: u64,
    },
    Retire {
        validator: String,
        amount: u64,
    },
    Withdraw {
        validator: String,
    },
    UnJail,
    SetCommission {
        rate: u16,
    },
}

// -- shared state --

#[derive(Clone)]
pub struct SharedState {
    pub blocks: Arc<RwLock<Vec<Block>>>,
    pub nodes: Arc<RwLock<Vec<NodeStatus>>>,
    pub radio_stats: Arc<RwLock<RadioStats>>,
    pub updates: broadcast::Sender<WebSocketUpdate>,
    pub blockstore: Option<Arc<RwLock<dyn Blockstore + Send + Sync>>>,
    pub mempool: Arc<RwLock<Vec<MempoolEntry>>>,
    pub tx_sender: Option<mpsc::UnboundedSender<CoreTransaction>>,
    pub execution_state: Arc<RwLock<ExecutionState>>,
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
        TransactionBodyRequest::Transfer { to, amount } => Ok(TransactionBody::Transfer {
            to: decode_pubkey(&to)?,
            amount,
        }),
        TransactionBodyRequest::TokenTransfer {
            to,
            token_id,
            amount,
        } => Ok(TransactionBody::TokenTransfer {
            to: decode_pubkey(&to)?,
            token_id: decode_token_id(&token_id)?,
            amount,
        }),
        TransactionBodyRequest::Mint {
            ticker,
            max_supply,
            metadata_hash,
        } => {
            if ticker.len() < 3 || ticker.len() > MAX_TICKER_LEN {
                return Err(format!("ticker must be 3-{MAX_TICKER_LEN} characters"));
            }
            Ok(TransactionBody::Mint {
                ticker,
                max_supply,
                metadata_hash: decode_hash32(&metadata_hash)?,
            })
        }
        TransactionBodyRequest::Bond { validator, amount } => Ok(TransactionBody::Bond {
            validator: decode_pubkey(&validator)?,
            amount,
        }),
        TransactionBodyRequest::Retire { validator, amount } => Ok(TransactionBody::Retire {
            validator: decode_pubkey(&validator)?,
            amount,
        }),
        TransactionBodyRequest::Withdraw { validator } => Ok(TransactionBody::Withdraw {
            validator: decode_pubkey(&validator)?,
        }),
        TransactionBodyRequest::UnJail => Ok(TransactionBody::UnJail),
        TransactionBodyRequest::SetCommission { rate } => {
            Ok(TransactionBody::SetCommission { rate })
        }
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
        TransactionBody::SetCommission { .. } => "SetCommission",
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
        Err(e) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": format!("invalid sender: {e}") })),
            )
                .into_response()
        }
    };

    let signature = match decode_signature(&req.signature) {
        Ok(s) => s,
        Err(e) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": format!("invalid signature: {e}") })),
            )
                .into_response()
        }
    };

    let body = match convert_body(req.body) {
        Ok(b) => b,
        Err(e) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response()
        }
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
                Json(
                    serde_json::json!({ "error": "transaction already in mempool", "hash": hash }),
                ),
            )
                .into_response();
        }
        if mempool.len() >= MAX_MEMPOOL_SIZE {
            return (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({ "error": "mempool full" })),
            )
                .into_response();
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
        return Json(
            serde_json::json!({ "transactions": [], "total": total, "limit": limit, "offset": offset }),
        );
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
async fn blocks(
    Query(p): Query<Pagination>,
    state: axum::extract::State<SharedState>,
) -> Json<Vec<Block>> {
    let limit = p.limit.unwrap_or(100).min(100);
    let offset = p.offset.unwrap_or(0);

    let mut all_blocks = {
        let blocks = state.blocks.read().await;
        blocks.clone()
    };

    if let Some(bs_arc) = &state.blockstore {
        let bs = bs_arc.read().await;

        let highest_mem_slot = all_blocks.iter().map(|b| b.slot()).max().unwrap_or(0);

        for slot_u64 in 0..=highest_mem_slot + 200 {
            if all_blocks.iter().any(|b| b.slot() == slot_u64) {
                continue;
            }

            let slot = Slot::new(slot_u64);
            if let Some(hash) = bs.canonical_block_hash(slot) {
                let block_hash: DoubleMerkleRoot = hash.clone().into();
                let block_id = (slot, block_hash);
                if let Some(block) = bs.get_block(&block_id) {
                    let (producer, proposed_timestamp, finalized_timestamp) =
                        if let Some(metadata) = bs.load_block_metadata(slot, hash.clone()) {
                            (
                                metadata.producer,
                                metadata.proposed_timestamp,
                                metadata.finalized_timestamp,
                            )
                        } else {
                            (0, 0, Some(0))
                        };

                    let status = if finalized_timestamp.is_some() {
                        SlotStatus::Finalized
                    } else {
                        SlotStatus::Proposed
                    };

                    let api_block = Block::Block {
                        slot: slot_u64,
                        hash: hex::encode(hash),
                        parent_slot: block.parent().inner(),
                        parent_hash: hex::encode(block.parent_hash().as_hash()),
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

async fn block(
    Path(hash): Path<String>,
    state: axum::extract::State<SharedState>,
) -> impl IntoResponse {
    let blocks = state.blocks.read().await;
    if let Some(block) = blocks.iter().find(|b| b.hash() == hash) {
        return Json(block.clone()).into_response();
    }

    if let Some(bs_arc) = &state.blockstore {
        if let Ok(hash_bytes) = hex::decode(&hash) {
            if hash_bytes.len() == 32 {
                let mut hash_arr = [0u8; 32];
                hash_arr.copy_from_slice(&hash_bytes);
                let h = Hash::from(hash_arr);
                let bs = bs_arc.read().await;
                if let Some((slot, blk)) = bs.load_block_by_hash(h.clone()) {
                    let (producer, proposed_timestamp, finalized_timestamp) =
                        if let Some(metadata) = bs.load_block_metadata(slot, h) {
                            (
                                metadata.producer,
                                metadata.proposed_timestamp,
                                metadata.finalized_timestamp,
                            )
                        } else {
                            (0, 0, Some(0))
                        };

                    let status = if finalized_timestamp.is_some() {
                        SlotStatus::Finalized
                    } else {
                        SlotStatus::Proposed
                    };

                    let api_block = Block::Block {
                        slot: slot.inner(),
                        hash: hash.clone(),
                        parent_slot: blk.parent().inner(),
                        parent_hash: hex::encode(blk.parent_hash().as_hash()),
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

// -- account / token handlers --

async fn get_account(
    Path(pubkey_hex): Path<String>,
    state: axum::extract::State<SharedState>,
) -> impl IntoResponse {
    let pubkey = match decode_pubkey(&pubkey_hex) {
        Ok(pk) => pk,
        Err(e) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": format!("invalid pubkey: {e}") })),
            )
                .into_response()
        }
    };

    let exec = state.execution_state.read().await;
    if let Some(account) = exec.get_account(&pubkey) {
        let token_balances: serde_json::Map<String, serde_json::Value> = account
            .token_balances
            .iter()
            .map(|(id, bal)| (hex::encode(id), serde_json::json!(*bal)))
            .collect();

        Json(serde_json::json!({
            "pubkey": pubkey_hex,
            "native_balance": account.native_balance,
            "token_balances": token_balances,
            "nonce": account.nonce,
        }))
        .into_response()
    } else {
        Json(serde_json::json!({
            "pubkey": pubkey_hex,
            "native_balance": 0,
            "token_balances": {},
            "nonce": 0,
        }))
        .into_response()
    }
}

async fn get_tokens(state: axum::extract::State<SharedState>) -> Json<serde_json::Value> {
    let exec = state.execution_state.read().await;
    let tokens: Vec<serde_json::Value> = exec
        .tokens
        .values()
        .map(|t| {
            serde_json::json!({
                "id": hex::encode(t.id),
                "ticker": t.ticker,
                "current_supply": t.current_supply,
                "max_supply": t.max_supply,
                "metadata_hash": hex::encode(t.metadata_hash),
                "creator": hex::encode(t.creator),
            })
        })
        .collect();
    Json(serde_json::json!({ "tokens": tokens }))
}

// -- server --

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_pubkey_valid() {
        let hex_str = "00".repeat(32);
        let result = decode_pubkey(&hex_str);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), [0u8; 32]);
    }

    #[test]
    fn decode_pubkey_nonzero() {
        let mut key = [0u8; 32];
        key[0] = 0xAB;
        key[31] = 0xCD;
        let hex_str = hex::encode(key);
        let result = decode_pubkey(&hex_str).unwrap();
        assert_eq!(result, key);
    }

    #[test]
    fn decode_pubkey_invalid_hex() {
        let result = decode_pubkey("not_valid_hex");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid hex"));
    }

    #[test]
    fn decode_pubkey_wrong_length() {
        let hex_str = "00".repeat(16);
        let result = decode_pubkey(&hex_str);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("expected 32 bytes"));
    }

    #[test]
    fn decode_pubkey_empty() {
        let result = decode_pubkey("");
        assert!(result.is_err());
    }

    #[test]
    fn decode_signature_valid() {
        let hex_str = "FF".repeat(64);
        let result = decode_signature(&hex_str);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), [0xFF; 64]);
    }

    #[test]
    fn decode_signature_wrong_length() {
        let hex_str = "00".repeat(32);
        let result = decode_signature(&hex_str);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("expected 64 bytes"));
    }

    #[test]
    fn decode_signature_invalid_hex() {
        let result = decode_signature("xyz");
        assert!(result.is_err());
    }

    #[test]
    fn decode_token_id_valid() {
        let result = decode_token_id("01020304");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), [1, 2, 3, 4]);
    }

    #[test]
    fn decode_token_id_wrong_length() {
        let result = decode_token_id("0102");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("expected 4 bytes"));
    }

    #[test]
    fn decode_hash32_valid() {
        let hex_str = "AB".repeat(32);
        let result = decode_hash32(&hex_str);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), [0xAB; 32]);
    }

    #[test]
    fn decode_hash32_wrong_length() {
        let hex_str = "AB".repeat(16);
        let result = decode_hash32(&hex_str);
        assert!(result.is_err());
    }

    #[test]
    fn convert_body_transfer() {
        let sender = "00".repeat(32);
        let body = TransactionBodyRequest::Transfer {
            to: sender.clone(),
            amount: 100,
        };
        let result = convert_body(body).unwrap();
        match result {
            TransactionBody::Transfer { to, amount } => {
                assert_eq!(to, [0u8; 32]);
                assert_eq!(amount, 100);
            }
            _ => panic!("expected Transfer"),
        }
    }

    #[test]
    fn convert_body_token_transfer() {
        let pk = "00".repeat(32);
        let token = "01020304";
        let body = TransactionBodyRequest::TokenTransfer {
            to: pk,
            token_id: token.to_string(),
            amount: 50,
        };
        let result = convert_body(body).unwrap();
        match result {
            TransactionBody::TokenTransfer {
                to,
                token_id,
                amount,
            } => {
                assert_eq!(to, [0u8; 32]);
                assert_eq!(token_id, [1, 2, 3, 4]);
                assert_eq!(amount, 50);
            }
            _ => panic!("expected TokenTransfer"),
        }
    }

    #[test]
    fn convert_body_mint_valid() {
        let hash = "00".repeat(32);
        let body = TransactionBodyRequest::Mint {
            ticker: "BNK".to_string(),
            max_supply: 1_000_000,
            metadata_hash: hash,
        };
        let result = convert_body(body).unwrap();
        match result {
            TransactionBody::Mint {
                ticker, max_supply, ..
            } => {
                assert_eq!(ticker, "BNK");
                assert_eq!(max_supply, 1_000_000);
            }
            _ => panic!("expected Mint"),
        }
    }

    #[test]
    fn convert_body_mint_ticker_too_short() {
        let hash = "00".repeat(32);
        let body = TransactionBodyRequest::Mint {
            ticker: "AB".to_string(),
            max_supply: 100,
            metadata_hash: hash,
        };
        let result = convert_body(body);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("ticker"));
    }

    #[test]
    fn convert_body_mint_ticker_too_long() {
        let hash = "00".repeat(32);
        let body = TransactionBodyRequest::Mint {
            ticker: "TOOLONGTICKERX".to_string(),
            max_supply: 100,
            metadata_hash: hash,
        };
        let result = convert_body(body);
        assert!(result.is_err());
    }

    #[test]
    fn convert_body_mint_ticker_at_bounds() {
        let hash = "00".repeat(32);
        // exactly 3 chars (min)
        let body = TransactionBodyRequest::Mint {
            ticker: "ABC".to_string(),
            max_supply: 100,
            metadata_hash: hash.clone(),
        };
        assert!(convert_body(body).is_ok());

        // exactly MAX_TICKER_LEN chars (max)
        let body = TransactionBodyRequest::Mint {
            ticker: "A".repeat(MAX_TICKER_LEN),
            max_supply: 100,
            metadata_hash: hash,
        };
        assert!(convert_body(body).is_ok());
    }

    #[test]
    fn convert_body_bond() {
        let pk = "00".repeat(32);
        let body = TransactionBodyRequest::Bond {
            validator: pk,
            amount: 500,
        };
        let result = convert_body(body).unwrap();
        assert!(matches!(result, TransactionBody::Bond { amount: 500, .. }));
    }

    #[test]
    fn convert_body_retire() {
        let pk = "00".repeat(32);
        let body = TransactionBodyRequest::Retire {
            validator: pk,
            amount: 200,
        };
        let result = convert_body(body).unwrap();
        assert!(matches!(
            result,
            TransactionBody::Retire { amount: 200, .. }
        ));
    }

    #[test]
    fn convert_body_withdraw() {
        let pk = "00".repeat(32);
        let body = TransactionBodyRequest::Withdraw { validator: pk };
        let result = convert_body(body).unwrap();
        assert!(matches!(result, TransactionBody::Withdraw { .. }));
    }

    #[test]
    fn convert_body_unjail() {
        let body = TransactionBodyRequest::UnJail;
        let result = convert_body(body).unwrap();
        assert!(matches!(result, TransactionBody::UnJail));
    }

    #[test]
    fn convert_body_set_commission() {
        let body = TransactionBodyRequest::SetCommission { rate: 1500 };
        let result = convert_body(body).unwrap();
        match result {
            TransactionBody::SetCommission { rate } => assert_eq!(rate, 1500),
            _ => panic!("expected SetCommission"),
        }
    }

    #[test]
    fn convert_body_invalid_pubkey_propagates() {
        let body = TransactionBodyRequest::Transfer {
            to: "bad_hex".to_string(),
            amount: 100,
        };
        assert!(convert_body(body).is_err());
    }

    #[test]
    fn body_type_name_all_variants() {
        assert_eq!(
            body_type_name(&TransactionBody::Transfer {
                to: [0; 32],
                amount: 0,
            }),
            "Transfer"
        );
        assert_eq!(
            body_type_name(&TransactionBody::TokenTransfer {
                to: [0; 32],
                token_id: [0; 4],
                amount: 0,
            }),
            "TokenTransfer"
        );
        assert_eq!(
            body_type_name(&TransactionBody::Mint {
                ticker: "X".into(),
                max_supply: 0,
                metadata_hash: [0; 32],
            }),
            "Mint"
        );
        assert_eq!(
            body_type_name(&TransactionBody::Bond {
                validator: [0; 32],
                amount: 0,
            }),
            "Bond"
        );
        assert_eq!(
            body_type_name(&TransactionBody::Retire {
                validator: [0; 32],
                amount: 0,
            }),
            "Retire"
        );
        assert_eq!(
            body_type_name(&TransactionBody::Withdraw { validator: [0; 32] }),
            "Withdraw"
        );
        assert_eq!(body_type_name(&TransactionBody::UnJail), "UnJail");
        assert_eq!(
            body_type_name(&TransactionBody::SetCommission { rate: 0 }),
            "SetCommission"
        );
    }

    #[test]
    fn block_slot_accessors() {
        let block = Block::Block {
            slot: 42,
            hash: "abc".to_string(),
            parent_slot: 41,
            parent_hash: "def".to_string(),
            producer: 1,
            proposed_timestamp: 1000,
            finalized_timestamp: None,
            status: SlotStatus::Proposed,
        };
        assert_eq!(block.slot(), 42);
        assert_eq!(block.hash(), "abc");
        assert_eq!(block.status(), SlotStatus::Proposed);
        assert_eq!(block.proposed_timestamp(), 1000);
        assert_eq!(block.finalized_timestamp(), None);
    }

    #[test]
    fn skip_slot_accessors() {
        let skip = Block::Skip {
            slot: 10,
            hash: "skip_hash".to_string(),
            proposed_timestamp: 500,
            finalized_timestamp: Some(600),
            status: SlotStatus::Finalized,
        };
        assert_eq!(skip.slot(), 10);
        assert_eq!(skip.hash(), "skip_hash");
        assert_eq!(skip.status(), SlotStatus::Finalized);
        assert_eq!(skip.finalized_timestamp(), Some(600));
    }

    #[test]
    fn block_set_status_to_finalized() {
        let mut block = Block::Block {
            slot: 1,
            hash: "h".to_string(),
            parent_slot: 0,
            parent_hash: "p".to_string(),
            producer: 0,
            proposed_timestamp: 100,
            finalized_timestamp: None,
            status: SlotStatus::Proposed,
        };
        block.set_status(SlotStatus::Finalized, Some(200));
        assert_eq!(block.status(), SlotStatus::Finalized);
        assert_eq!(block.finalized_timestamp(), Some(200));
    }

    #[test]
    fn block_set_status_non_finalized_keeps_timestamp() {
        let mut block = Block::Block {
            slot: 1,
            hash: "h".to_string(),
            parent_slot: 0,
            parent_hash: "p".to_string(),
            producer: 0,
            proposed_timestamp: 100,
            finalized_timestamp: None,
            status: SlotStatus::Proposed,
        };
        block.set_status(SlotStatus::Notarized, Some(200));
        assert_eq!(block.status(), SlotStatus::Notarized);
        assert_eq!(block.finalized_timestamp(), None);
    }

    #[test]
    fn skip_set_status_to_finalized() {
        let mut skip = Block::Skip {
            slot: 5,
            hash: "s".to_string(),
            proposed_timestamp: 100,
            finalized_timestamp: None,
            status: SlotStatus::Pending,
        };
        skip.set_status(SlotStatus::Finalized, Some(300));
        assert_eq!(skip.status(), SlotStatus::Finalized);
        assert_eq!(skip.finalized_timestamp(), Some(300));
    }

    #[test]
    fn slot_status_json_roundtrip() {
        let statuses = [
            SlotStatus::Pending,
            SlotStatus::Proposed,
            SlotStatus::Notarized,
            SlotStatus::Finalized,
        ];
        for status in statuses {
            let json = serde_json::to_string(&status).unwrap();
            let decoded: SlotStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded, status);
        }
    }

    #[test]
    fn slot_status_json_lowercase() {
        assert_eq!(
            serde_json::to_string(&SlotStatus::Pending).unwrap(),
            "\"pending\""
        );
        assert_eq!(
            serde_json::to_string(&SlotStatus::Finalized).unwrap(),
            "\"finalized\""
        );
    }

    #[test]
    fn block_json_has_type_tag() {
        let block = Block::Block {
            slot: 1,
            hash: "h".into(),
            parent_slot: 0,
            parent_hash: "p".into(),
            producer: 0,
            proposed_timestamp: 0,
            finalized_timestamp: None,
            status: SlotStatus::Proposed,
        };
        let json = serde_json::to_string(&block).unwrap();
        assert!(json.contains("\"type\":\"block\""));

        let skip = Block::Skip {
            slot: 2,
            hash: "s".into(),
            proposed_timestamp: 0,
            finalized_timestamp: None,
            status: SlotStatus::Pending,
        };
        let json = serde_json::to_string(&skip).unwrap();
        assert!(json.contains("\"type\":\"skip\""));
    }
}

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
        .route("/accounts/{pubkey}", get(get_account))
        .route("/tokens", get(get_tokens))
        .route("/ws", get(websocket_handler))
        .layer(cors)
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
