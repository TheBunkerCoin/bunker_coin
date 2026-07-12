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
use bunkerglow::snapshot::{SnapshotManifest, SnapshotStore};
use bunkerglow::Slot;
use ed25519_dalek::SigningKey;
use futures::{sink::SinkExt, stream::StreamExt};
use hex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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
    #[serde(rename = "transaction_finalized")]
    TransactionFinalized {
        hash: String,
        slot: u64,
        block_hash: String,
        success: bool,
        error: Option<String>,
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
    pub body: TransactionBodyResponse,
    pub received_at: u64,
}

// -- transaction result types --

#[derive(Serialize, Clone, Debug)]
#[serde(rename_all = "lowercase")]
pub enum TxFinalStatus {
    Finalized,
    Failed,
}

#[derive(Serialize, Clone, Debug)]
pub struct TxResult {
    pub hash: String,
    pub slot: u64,
    pub block_hash: String,
    pub status: TxFinalStatus,
    pub error: Option<String>,
    pub executed_at: u64,
}

// -- transaction response types --

#[derive(Serialize, Clone, Debug)]
#[serde(tag = "type")]
pub enum TransactionBodyResponse {
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
    Burn {
        token_id: String,
        amount: u64,
    },
    UpdateMetadata {
        token_id: String,
        metadata_hash: String,
    },
    LocationClaim {
        lat: i32,
        lon: i32,
    },
    MessageAnchor {
        destination: String,
        deposit: u64,
    },
    DeliveryWrapup {
        anchor_hash: String,
    },
}

#[derive(Serialize, Clone, Debug)]
pub struct TransactionSummary {
    pub hash: String,
    pub sender: String,
    pub nonce: u64,
    pub fee: u64,
    pub body: TransactionBodyResponse,
}

// -- block detail response --

#[derive(Serialize, Clone)]
pub struct BlockDetailResponse {
    #[serde(flatten)]
    pub block: Block,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transactions: Option<Vec<TransactionSummary>>,
}

#[derive(Deserialize)]
struct BlockQueryParams {
    include_transactions: Option<bool>,
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
    Burn {
        token_id: String,
        amount: u64,
    },
    UpdateMetadata {
        token_id: String,
        metadata_hash: String,
    },
}

// -- shared state --

#[derive(Clone)]
pub struct SharedState {
    pub blocks: Arc<RwLock<Vec<Block>>>,
    pub nodes: Arc<RwLock<Vec<NodeStatus>>>,
    pub radio_stats: Arc<RwLock<RadioStats>>,
    pub updates: broadcast::Sender<WebSocketUpdate>,
    pub blockstore: Option<Arc<RwLock<Box<dyn Blockstore + Send + Sync>>>>,
    pub mempool: Arc<RwLock<Vec<MempoolEntry>>>,
    pub tx_sender: Option<mpsc::UnboundedSender<CoreTransaction>>,
    pub execution_state: Arc<RwLock<ExecutionState>>,
    pub tx_results: Arc<RwLock<HashMap<String, TxResult>>>,
    pub genesis_signing_key: Option<Arc<SigningKey>>,
    pub snapshot_store: Option<Arc<SnapshotStore>>,
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
        TransactionBodyRequest::Burn { token_id, amount } => Ok(TransactionBody::Burn {
            token_id: decode_token_id(&token_id)?,
            amount,
        }),
        TransactionBodyRequest::UpdateMetadata {
            token_id,
            metadata_hash,
        } => Ok(TransactionBody::UpdateMetadata {
            token_id: decode_token_id(&token_id)?,
            metadata_hash: decode_hash32(&metadata_hash)?,
        }),
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
        TransactionBody::Burn { .. } => "Burn",
        TransactionBody::UpdateMetadata { .. } => "UpdateMetadata",
        TransactionBody::LocationClaim { .. } => "LocationClaim",
        TransactionBody::MessageAnchor { .. } => "MessageAnchor",
        TransactionBody::DeliveryWrapup { .. } => "DeliveryWrapup",
    }
}

// -- decode / conversion helpers --

fn decode_raw_transaction(raw: &bunkerglow::Transaction) -> Option<CoreTransaction> {
    // Transaction.0 may have a wincode Vec<u8> length prefix (8-byte LE u64)
    // wrapping the bincode payload. Try raw first, then skip the prefix.
    // Limit-guarded: block payloads include BUNKER_BLOAT_BYTES random padding,
    // and without a limit bincode skips its length check — a random u64 read as
    // a Vec length would abort the process on allocation. Real txs are < 4 KiB.
    let config = bincode::config::standard().with_limit::<4096>();
    let data = &raw.0;
    bincode::serde::decode_from_slice(data, config)
        .or_else(|_| {
            if data.len() > 8 {
                bincode::serde::decode_from_slice(&data[8..], config)
            } else {
                Err(bincode::error::DecodeError::Other("too short"))
            }
        })
        .ok()
        .map(|(tx, _)| tx)
}

fn core_tx_to_body_response(body: &TransactionBody) -> TransactionBodyResponse {
    match body {
        TransactionBody::Transfer { to, amount } => TransactionBodyResponse::Transfer {
            to: hex::encode(to),
            amount: *amount,
        },
        TransactionBody::TokenTransfer {
            to,
            token_id,
            amount,
        } => TransactionBodyResponse::TokenTransfer {
            to: hex::encode(to),
            token_id: hex::encode(token_id),
            amount: *amount,
        },
        TransactionBody::Mint {
            ticker,
            max_supply,
            metadata_hash,
        } => TransactionBodyResponse::Mint {
            ticker: ticker.clone(),
            max_supply: *max_supply,
            metadata_hash: hex::encode(metadata_hash),
        },
        TransactionBody::Bond { validator, amount } => TransactionBodyResponse::Bond {
            validator: hex::encode(validator),
            amount: *amount,
        },
        TransactionBody::Retire { validator, amount } => TransactionBodyResponse::Retire {
            validator: hex::encode(validator),
            amount: *amount,
        },
        TransactionBody::Withdraw { validator } => TransactionBodyResponse::Withdraw {
            validator: hex::encode(validator),
        },
        TransactionBody::UnJail => TransactionBodyResponse::UnJail,
        TransactionBody::SetCommission { rate } => {
            TransactionBodyResponse::SetCommission { rate: *rate }
        }
        TransactionBody::Burn { token_id, amount } => TransactionBodyResponse::Burn {
            token_id: hex::encode(token_id),
            amount: *amount,
        },
        TransactionBody::UpdateMetadata {
            token_id,
            metadata_hash,
        } => TransactionBodyResponse::UpdateMetadata {
            token_id: hex::encode(token_id),
            metadata_hash: hex::encode(metadata_hash),
        },
        TransactionBody::LocationClaim { lat, lon, .. } => TransactionBodyResponse::LocationClaim {
            lat: *lat,
            lon: *lon,
        },
        TransactionBody::MessageAnchor {
            destination,
            deposit,
            ..
        } => TransactionBodyResponse::MessageAnchor {
            destination: hex::encode(destination),
            deposit: *deposit,
        },
        TransactionBody::DeliveryWrapup { anchor_hash, .. } => {
            TransactionBodyResponse::DeliveryWrapup {
                anchor_hash: hex::encode(anchor_hash),
            }
        }
    }
}

fn decode_block_transactions(block: &bunkerglow::Block) -> Vec<TransactionSummary> {
    block
        .transactions()
        .iter()
        .filter_map(|raw| {
            let core_tx = decode_raw_transaction(raw)?;
            Some(TransactionSummary {
                hash: hex::encode(core_tx.hash()),
                sender: hex::encode(core_tx.sender),
                nonce: core_tx.nonce,
                fee: core_tx.fee,
                body: core_tx_to_body_response(&core_tx.body),
            })
        })
        .collect()
}

fn build_api_block(
    slot: u64,
    hash_hex: String,
    blk: &bunkerglow::Block,
    metadata: Option<bunkerglow::consensus::BlockMetadata>,
) -> Block {
    let (producer, proposed_timestamp, finalized_timestamp) = match metadata {
        Some(m) => (m.producer, m.proposed_timestamp, m.finalized_timestamp),
        None => (0, 0, Some(0)),
    };

    let status = if finalized_timestamp.is_some() {
        SlotStatus::Finalized
    } else {
        SlotStatus::Proposed
    };

    Block::Block {
        slot,
        hash: hash_hex,
        parent_slot: blk.parent().inner(),
        parent_hash: hex::encode(blk.parent_hash().as_hash()),
        producer,
        proposed_timestamp,
        finalized_timestamp,
        status,
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
                .into_response();
        }
    };

    let signature = match decode_signature(&req.signature) {
        Ok(s) => s,
        Err(e) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": format!("invalid signature: {e}") })),
            )
                .into_response();
        }
    };

    let body = match convert_body(req.body) {
        Ok(b) => b,
        Err(e) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response();
        }
    };

    let mut tx = CoreTransaction {
        sender,
        nonce: req.nonce,
        fee: req.fee,
        body,
        signature,
    };

    // server-side signing: if signature is all zeros and sender matches genesis pubkey,
    // auto-fill the nonce from current execution state and sign it
    if tx.signature == [0u8; 64] {
        if let Some(sk) = &state.genesis_signing_key {
            let genesis_pk = sk.verifying_key().to_bytes();
            if tx.sender == genesis_pk {
                // auto-fill nonce from current account state
                let exec = state.execution_state.read().await;
                let current_nonce = exec.get_account(&genesis_pk).map(|a| a.nonce).unwrap_or(0);
                drop(exec);
                tx.nonce = current_nonce;

                use ed25519_dalek::Signer;
                let msg = tx.signing_hash();
                tx.signature = sk.sign(&msg).to_bytes();
            }
        }
    }

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
        body: core_tx_to_body_response(&tx.body),
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
        // The in-memory list stays empty on the persistent-node path (e.g.
        // pactor_consensus), which used to freeze this scan at slot 200 while
        // the chain kept climbing. The validators' finalized frontier (kept
        // current in `state.nodes` by the block executor) tracks the real
        // chain height; +200 headroom covers produced-but-unfinalized slots.
        let highest_finalized_slot = {
            let nodes = state.nodes.read().await;
            nodes.iter().map(|n| n.finalized_slot).max().unwrap_or(0)
        };

        for slot_u64 in 0..=highest_mem_slot.max(highest_finalized_slot) + 200 {
            if all_blocks.iter().any(|b| b.slot() == slot_u64) {
                continue;
            }

            let slot = Slot::new(slot_u64);
            if let Some(hash) = bs.canonical_block_hash(slot) {
                let block_hash: DoubleMerkleRoot = hash.clone().into();
                let block_id = (slot, block_hash);
                if let Some(blk) = bs.get_block(&block_id) {
                    let metadata = bs.load_block_metadata(slot, hash.clone());
                    let api_block = build_api_block(slot_u64, hex::encode(hash), &blk, metadata);
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
    Query(params): Query<BlockQueryParams>,
    state: axum::extract::State<SharedState>,
) -> impl IntoResponse {
    let include_txs = params.include_transactions.unwrap_or(false);

    {
        let blocks = state.blocks.read().await;
        if let Some(block) = blocks.iter().find(|b| b.hash() == hash) {
            return Json(BlockDetailResponse {
                block: block.clone(),
                transactions: None,
            })
            .into_response();
        }
    }

    if let Some(bs_arc) = &state.blockstore {
        if let Ok(hash_bytes) = hex::decode(&hash) {
            if hash_bytes.len() == 32 {
                let mut hash_arr = [0u8; 32];
                hash_arr.copy_from_slice(&hash_bytes);
                let h = Hash::from(hash_arr);
                let bs = bs_arc.read().await;
                if let Some((slot, blk)) = bs.load_block_by_hash(h.clone()) {
                    let metadata = bs.load_block_metadata(slot, h);
                    let api_block = build_api_block(slot.inner(), hash.clone(), &blk, metadata);
                    let transactions = if include_txs {
                        Some(decode_block_transactions(&blk))
                    } else {
                        None
                    };
                    return Json(BlockDetailResponse {
                        block: api_block,
                        transactions,
                    })
                    .into_response();
                }
            }
        }
    }
    axum::http::StatusCode::NOT_FOUND.into_response()
}

async fn block_by_slot(
    Path(slot_num): Path<u64>,
    Query(params): Query<BlockQueryParams>,
    state: axum::extract::State<SharedState>,
) -> impl IntoResponse {
    let include_txs = params.include_transactions.unwrap_or(false);

    // check in-memory blocks first
    {
        let blocks = state.blocks.read().await;
        if let Some(block) = blocks.iter().find(|b| b.slot() == slot_num) {
            return Json(BlockDetailResponse {
                block: block.clone(),
                transactions: None,
            })
            .into_response();
        }
    }

    // fall back to blockstore
    if let Some(bs_arc) = &state.blockstore {
        let bs = bs_arc.read().await;
        let slot = Slot::new(slot_num);
        if let Some(hash) = bs.canonical_block_hash(slot) {
            let block_hash: DoubleMerkleRoot = hash.clone().into();
            let block_id = (slot, block_hash);
            if let Some(blk) = bs.get_block(&block_id) {
                let metadata = bs.load_block_metadata(slot, hash.clone());
                let api_block = build_api_block(slot_num, hex::encode(hash), &blk, metadata);
                let transactions = if include_txs {
                    Some(decode_block_transactions(&blk))
                } else {
                    None
                };
                return Json(BlockDetailResponse {
                    block: api_block,
                    transactions,
                })
                .into_response();
            }
        }
    }
    axum::http::StatusCode::NOT_FOUND.into_response()
}

async fn get_transaction(
    Path(hash): Path<String>,
    state: axum::extract::State<SharedState>,
) -> impl IntoResponse {
    // check tx_results first (finalized transactions)
    {
        let results = state.tx_results.read().await;
        if let Some(result) = results.get(&hash) {
            // find the original tx details from blockstore
            let (sender, nonce, fee, body) = find_tx_details_in_blockstore(&state, &hash)
                .await
                .unwrap_or_else(|| ("unknown".to_string(), 0, 0, None));

            let success = matches!(result.status, TxFinalStatus::Finalized);
            let mut resp = serde_json::json!({
                "hash": hash,
                "sender": sender,
                "nonce": nonce,
                "fee": fee,
                "status": {
                    "location": "finalized",
                    "slot": result.slot,
                    "block_hash": result.block_hash,
                    "executed": true,
                    "success": success,
                    "error": result.error,
                },
            });
            if let Some(body_resp) = body {
                resp["body"] = serde_json::to_value(body_resp).unwrap_or_default();
            }
            return Json(resp).into_response();
        }
    }

    // check mempool (in-memory)
    {
        let pool = state.mempool.read().await;
        if let Some(entry) = pool.iter().find(|e| e.hash == hash) {
            return Json(serde_json::json!({
                "hash": entry.hash,
                "sender": entry.sender,
                "nonce": entry.nonce,
                "fee": entry.fee,
                "body": entry.body,
                "status": { "location": "mempool" },
            }))
            .into_response();
        }
    }

    // scan blockstore (confirmed but not yet finalized/executed)
    if let Some((sender, nonce, fee, body, slot_u64, blk_hash)) =
        find_tx_in_blockstore(&state, &hash).await
    {
        let mut resp = serde_json::json!({
            "hash": hash,
            "sender": sender,
            "nonce": nonce,
            "fee": fee,
            "status": {
                "location": "confirmed",
                "slot": slot_u64,
                "block_hash": blk_hash,
            },
        });
        if let Some(body_resp) = body {
            resp["body"] = serde_json::to_value(body_resp).unwrap_or_default();
        }
        return Json(resp).into_response();
    }

    axum::http::StatusCode::NOT_FOUND.into_response()
}

/// `GET /transactions` — list transactions across the finalized chain,
/// newest-first, paginated. Mirrors `/blocks`. Walks the blockstore up to the
/// finalized frontier (`state.nodes`), decodes each block's real transactions
/// (bloat padding is undecodable and skipped), and annotates finalized ones
/// with their execution outcome from `tx_results`.
async fn list_transactions(
    Query(p): Query<Pagination>,
    state: axum::extract::State<SharedState>,
) -> Json<serde_json::Value> {
    let limit = p.limit.unwrap_or(50).min(200);
    let offset = p.offset.unwrap_or(0);

    let mut txs: Vec<serde_json::Value> = Vec::new();

    if let Some(bs_arc) = &state.blockstore {
        let bs = bs_arc.read().await;
        let results = state.tx_results.read().await;

        let highest_finalized_slot = {
            let nodes = state.nodes.read().await;
            nodes.iter().map(|n| n.finalized_slot).max().unwrap_or(0)
        };

        // Newest-first: scan slots high→low. The same tx can land in two
        // blocks (each node's mempool packs it before finalization evicts
        // it) with the duplicate failing on nonce mismatch; list each tx
        // once, at its canonical inclusion — the one that actually executed
        // (per tx_results).
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for slot_u64 in (0..=highest_finalized_slot).rev() {
            let slot = Slot::new(slot_u64);
            let Some(hash) = bs.canonical_block_hash(slot) else {
                continue;
            };
            let block_hash: DoubleMerkleRoot = hash.clone().into();
            let Some(blk) = bs.get_block(&(slot, block_hash)) else {
                continue;
            };
            let blk_hash_hex = hex::encode(&hash);
            for raw_tx in blk.transactions() {
                let Some(core_tx) = decode_raw_transaction(raw_tx) else {
                    continue; // bloat padding / undecodable — not a real tx
                };
                let tx_hash = hex::encode(core_tx.hash());
                if let Some(r) = results.get(&tx_hash) {
                    // Skip duplicate inclusions: emit only where it executed.
                    if r.slot != slot_u64 {
                        continue;
                    }
                }
                if !seen.insert(tx_hash.clone()) {
                    continue;
                }
                let status = match results.get(&tx_hash) {
                    Some(r) => serde_json::json!({
                        "location": "finalized",
                        "slot": r.slot,
                        "block_hash": r.block_hash,
                        "success": matches!(r.status, TxFinalStatus::Finalized),
                        "error": r.error,
                    }),
                    None => serde_json::json!({
                        "location": "confirmed",
                        "slot": slot_u64,
                        "block_hash": blk_hash_hex,
                    }),
                };
                txs.push(serde_json::json!({
                    "hash": tx_hash,
                    "sender": hex::encode(core_tx.sender),
                    "nonce": core_tx.nonce,
                    "fee": core_tx.fee,
                    "slot": slot_u64,
                    "block_hash": blk_hash_hex,
                    "body": core_tx_to_body_response(&core_tx.body),
                    "status": status,
                }));
            }
        }
    }

    let total = txs.len();
    let page: Vec<serde_json::Value> =
        txs.into_iter().skip(offset).take(limit).collect();

    Json(serde_json::json!({
        "transactions": page,
        "total": total,
        "limit": limit,
        "offset": offset,
    }))
}

async fn find_tx_details_in_blockstore(
    state: &SharedState,
    hash: &str,
) -> Option<(String, u64, u64, Option<TransactionBodyResponse>)> {
    find_tx_in_blockstore(state, hash)
        .await
        .map(|(sender, nonce, fee, body, _slot, _blk_hash)| (sender, nonce, fee, body))
}

async fn find_tx_in_blockstore(
    state: &SharedState,
    hash: &str,
) -> Option<(
    String,
    u64,
    u64,
    Option<TransactionBodyResponse>,
    u64,
    String,
)> {
    let bs_arc = state.blockstore.as_ref()?;
    let bs = bs_arc.read().await;
    let blocks = state.blocks.read().await;
    let highest_mem_slot = blocks.iter().map(|b| b.slot()).max().unwrap_or(0);
    drop(blocks);
    // Same frontier-aware bound as `blocks()`: the in-memory list is empty on
    // the persistent-node path, so without the finalized frontier this scan
    // would stop finding transactions past slot 200.
    let highest_finalized_slot = {
        let nodes = state.nodes.read().await;
        nodes.iter().map(|n| n.finalized_slot).max().unwrap_or(0)
    };

    for slot_u64 in 0..=highest_mem_slot.max(highest_finalized_slot) + 200 {
        let slot = Slot::new(slot_u64);
        if let Some(blk_hash) = bs.canonical_block_hash(slot) {
            let block_hash: DoubleMerkleRoot = blk_hash.clone().into();
            let block_id = (slot, block_hash);
            if let Some(blk) = bs.get_block(&block_id) {
                for raw_tx in blk.transactions() {
                    if let Some(core_tx) = decode_raw_transaction(raw_tx) {
                        let tx_hash = hex::encode(core_tx.hash());
                        if tx_hash == hash {
                            return Some((
                                hex::encode(core_tx.sender),
                                core_tx.nonce,
                                core_tx.fee,
                                Some(core_tx_to_body_response(&core_tx.body)),
                                slot_u64,
                                hex::encode(blk_hash),
                            ));
                        }
                    }
                }
            }
        }
    }
    None
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
                if sender.send(Message::Text(msg.into())).await.is_err() {
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
                .into_response();
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

// -- single token handler --

async fn get_token(
    Path(id_hex): Path<String>,
    state: axum::extract::State<SharedState>,
) -> impl IntoResponse {
    let token_id = match decode_token_id(&id_hex) {
        Ok(id) => id,
        Err(e) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": format!("invalid token id: {e}") })),
            )
                .into_response();
        }
    };

    let exec = state.execution_state.read().await;
    if let Some(t) = exec.tokens.get(&token_id) {
        Json(serde_json::json!({
            "id": hex::encode(t.id),
            "ticker": t.ticker,
            "current_supply": t.current_supply,
            "max_supply": t.max_supply,
            "metadata_hash": hex::encode(t.metadata_hash),
            "creator": hex::encode(t.creator),
        }))
        .into_response()
    } else {
        (
            axum::http::StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "token not found" })),
        )
            .into_response()
    }
}

// -- token holders handler --

async fn get_token_holders(
    Path(id_hex): Path<String>,
    Query(p): Query<Pagination>,
    state: axum::extract::State<SharedState>,
) -> impl IntoResponse {
    let token_id = match decode_token_id(&id_hex) {
        Ok(id) => id,
        Err(e) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": format!("invalid token id: {e}") })),
            )
                .into_response();
        }
    };

    let exec = state.execution_state.read().await;
    if !exec.tokens.contains_key(&token_id) {
        return (
            axum::http::StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "token not found" })),
        )
            .into_response();
    }

    let mut holders: Vec<serde_json::Value> = exec
        .accounts
        .iter()
        .filter_map(|(pk, acc)| {
            let bal = acc.token_balances.get(&token_id).copied().unwrap_or(0);
            if bal > 0 {
                Some(serde_json::json!({ "pubkey": hex::encode(pk), "balance": bal }))
            } else {
                None
            }
        })
        .collect();

    // sort by balance descending for deterministic output
    holders.sort_by(|a, b| {
        b["balance"]
            .as_u64()
            .unwrap_or(0)
            .cmp(&a["balance"].as_u64().unwrap_or(0))
    });

    let total = holders.len();
    let limit = p.limit.unwrap_or(100).min(500);
    let offset = p.offset.unwrap_or(0);

    let page: Vec<_> = holders.into_iter().skip(offset).take(limit).collect();

    Json(serde_json::json!({
        "holders": page,
        "total": total,
        "limit": limit,
        "offset": offset,
    }))
    .into_response()
}

// -- account tokens handler --

async fn get_account_tokens(
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
                .into_response();
        }
    };

    let exec = state.execution_state.read().await;
    let account = exec.get_account(&pubkey);

    let tokens: Vec<serde_json::Value> = match account {
        Some(acc) => acc
            .token_balances
            .iter()
            .filter(|(_, bal)| **bal > 0)
            .map(|(tid, bal)| {
                let meta = exec.tokens.get(tid);
                let mut entry = serde_json::json!({
                    "token_id": hex::encode(tid),
                    "balance": *bal,
                });
                if let Some(m) = meta {
                    entry["ticker"] = serde_json::json!(m.ticker);
                    entry["current_supply"] = serde_json::json!(m.current_supply);
                    entry["max_supply"] = serde_json::json!(m.max_supply);
                    entry["metadata_hash"] = serde_json::json!(hex::encode(m.metadata_hash));
                    entry["creator"] = serde_json::json!(hex::encode(m.creator));
                }
                entry
            })
            .collect(),
        None => vec![],
    };

    Json(serde_json::json!({
        "pubkey": pubkey_hex,
        "tokens": tokens,
    }))
    .into_response()
}

// -- staking overview handler --

async fn get_staking(state: axum::extract::State<SharedState>) -> Json<serde_json::Value> {
    let exec = state.execution_state.read().await;
    let validator_set: Vec<serde_json::Value> = exec
        .staking
        .validator_set()
        .iter()
        .map(|(pk, stake)| {
            let commission = exec.staking.commission_rates.get(pk).copied().unwrap_or(0);
            serde_json::json!({
                "pubkey": hex::encode(pk),
                "stake": *stake,
                "commission_bps": commission,
            })
        })
        .collect();

    let total_active_stake = exec.staking.total_active_stake();
    let total_stake = exec.staking.total_stake();
    let current_epoch = exec.current_epoch;

    Json(serde_json::json!({
        "validators": validator_set,
        "total_active_stake": total_active_stake,
        "total_stake": total_stake,
        "current_epoch": current_epoch,
    }))
}

// -- snapshot bootstrap handlers --

fn manifest_json(manifest: &SnapshotManifest) -> serde_json::Value {
    serde_json::json!({
        "epoch": manifest.epoch,
        "state_hash": hex::encode(manifest.state_hash),
        "chunk_root": hex::encode(manifest.chunk_root.as_ref()),
        "chunk_size": manifest.chunk_size,
        "total_bytes": manifest.total_bytes,
        "chunk_count": manifest.chunk_count,
    })
}

fn checkpoint_json(checkpoint: &bunkerglow::snapshot::SnapshotCheckpoint) -> serde_json::Value {
    let block = &checkpoint.transition_block;
    serde_json::json!({
        "epoch": checkpoint.epoch,
        "finalized_slot": checkpoint.finalized_slot,
        "transition_block": {
            "epoch": block.epoch,
            "last_slot": block.last_slot,
            "fees_distributed": block.fees_distributed,
            "bonds_activated": block.bonds_activated.len(),
            "retires_completed": block.retires_completed.len(),
            "new_validator_set": block.new_validator_set.iter().map(|(pubkey, amount)| {
                serde_json::json!({
                    "pubkey": hex::encode(pubkey),
                    "stake": amount,
                })
            }).collect::<Vec<_>>(),
            "state_hash": hex::encode(block.state_hash),
            "snapshot_chunk_root": hex::encode(block.snapshot_chunk_root),
            "snapshot_chunk_count": block.snapshot_chunk_count,
            "snapshot_total_bytes": block.snapshot_total_bytes,
            "snapshot_chunk_size": block.snapshot_chunk_size,
            "slashes_applied": block.slashes_applied.len(),
            "deactivated_validators": block.deactivated_validators.iter().map(hex::encode).collect::<Vec<_>>(),
        },
        "finalization_certs": checkpoint.finalization_certs.iter().map(hex::encode).collect::<Vec<_>>(),
    })
}

fn manifest_with_checkpoint_json(
    manifest: &SnapshotManifest,
    checkpoint: &bunkerglow::snapshot::SnapshotCheckpoint,
) -> serde_json::Value {
    serde_json::json!({
        "manifest": manifest_json(manifest),
        "checkpoint": checkpoint_json(checkpoint),
    })
}

async fn get_latest_snapshot_manifest(
    state: axum::extract::State<SharedState>,
) -> impl IntoResponse {
    let Some(store) = &state.snapshot_store else {
        return (
            axum::http::StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "snapshot store not configured" })),
        )
            .into_response();
    };

    if let Some(manifest) = store.latest_manifest() {
        if let Some(checkpoint) = store
            .load_checkpoint(manifest.epoch)
            .filter(|checkpoint| checkpoint.matches_manifest(&manifest))
        {
            Json(manifest_with_checkpoint_json(&manifest, &checkpoint)).into_response()
        } else {
            (
                axum::http::StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "snapshot checkpoint not found" })),
            )
                .into_response()
        }
    } else {
        (
            axum::http::StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "no snapshot manifest available" })),
        )
            .into_response()
    }
}

async fn get_snapshot_manifest(
    Path(epoch): Path<u64>,
    state: axum::extract::State<SharedState>,
) -> impl IntoResponse {
    let Some(store) = &state.snapshot_store else {
        return (
            axum::http::StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "snapshot store not configured" })),
        )
            .into_response();
    };

    if let Some(manifest) = store.load_manifest(epoch) {
        if let Some(checkpoint) = store
            .load_checkpoint(epoch)
            .filter(|checkpoint| checkpoint.matches_manifest(&manifest))
        {
            Json(manifest_with_checkpoint_json(&manifest, &checkpoint)).into_response()
        } else {
            (
                axum::http::StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "snapshot checkpoint not found" })),
            )
                .into_response()
        }
    } else {
        (
            axum::http::StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "snapshot manifest not found" })),
        )
            .into_response()
    }
}

async fn get_snapshot_chunk(
    Path((epoch, index)): Path<(u64, usize)>,
    state: axum::extract::State<SharedState>,
) -> impl IntoResponse {
    let Some(store) = &state.snapshot_store else {
        return (
            axum::http::StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "snapshot store not configured" })),
        )
            .into_response();
    };

    let Some(manifest) = store.load_manifest(epoch) else {
        return (
            axum::http::StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "snapshot manifest not found" })),
        )
            .into_response();
    };
    if !store
        .load_checkpoint(epoch)
        .is_some_and(|checkpoint| checkpoint.matches_manifest(&manifest))
    {
        return (
            axum::http::StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "snapshot checkpoint not found" })),
        )
            .into_response();
    }

    if index >= manifest.chunk_count {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": format!("chunk index {index} out of range for {} chunks", manifest.chunk_count)
            })),
        )
            .into_response();
    }

    if let Some(chunk) = store.load_chunk(epoch, index) {
        let proof: Vec<String> = chunk
            .proof
            .iter()
            .map(|hash| hex::encode(hash.as_ref()))
            .collect();
        Json(serde_json::json!({
            "epoch": chunk.epoch,
            "index": chunk.index,
            "data": hex::encode(chunk.data),
            "proof": proof,
        }))
        .into_response()
    } else {
        (
            axum::http::StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "snapshot chunk not found" })),
        )
            .into_response()
    }
}

// -- genesis handler --

async fn get_genesis(state: axum::extract::State<SharedState>) -> impl IntoResponse {
    let Some(sk) = &state.genesis_signing_key else {
        return (
            axum::http::StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "no genesis keypair configured" })),
        )
            .into_response();
    };

    let pk = sk.verifying_key().to_bytes();
    let pk_hex = hex::encode(pk);

    let exec = state.execution_state.read().await;
    let balance = exec.get_account(&pk).map(|a| a.native_balance).unwrap_or(0);

    Json(serde_json::json!({
        "public_key": pk_hex,
        "secret_key": hex::encode(sk.to_bytes()),
        "balance": balance,
    }))
    .into_response()
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

    // -- decode_raw_transaction tests --

    fn make_core_tx(body: TransactionBody) -> CoreTransaction {
        CoreTransaction {
            sender: [0xAA; 32],
            nonce: 1,
            fee: 10,
            body,
            signature: [0xBB; 64],
        }
    }

    fn encode_core_tx(tx: &CoreTransaction) -> bunkerglow::Transaction {
        let bytes = bincode::serde::encode_to_vec(tx, bincode::config::standard()).unwrap();
        bunkerglow::Transaction(bytes)
    }

    #[test]
    fn decode_raw_transaction_valid() {
        let tx = make_core_tx(TransactionBody::Transfer {
            to: [0x01; 32],
            amount: 100,
        });
        let raw = encode_core_tx(&tx);
        let decoded = decode_raw_transaction(&raw).unwrap();
        assert_eq!(decoded, tx);
    }

    #[test]
    fn decode_raw_transaction_garbage() {
        let raw = bunkerglow::Transaction(vec![0xFF, 0xFE, 0xFD, 0x00]);
        assert!(decode_raw_transaction(&raw).is_none());
    }

    #[test]
    fn decode_raw_transaction_empty() {
        let raw = bunkerglow::Transaction(vec![]);
        assert!(decode_raw_transaction(&raw).is_none());
    }

    // -- core_tx_to_body_response tests --

    #[test]
    fn core_tx_to_body_response_transfer() {
        let body = TransactionBody::Transfer {
            to: [0x01; 32],
            amount: 42,
        };
        let resp = core_tx_to_body_response(&body);
        match resp {
            TransactionBodyResponse::Transfer { to, amount } => {
                assert_eq!(to, hex::encode([0x01; 32]));
                assert_eq!(amount, 42);
            }
            _ => panic!("expected Transfer"),
        }
    }

    #[test]
    fn core_tx_to_body_response_token_transfer() {
        let body = TransactionBody::TokenTransfer {
            to: [0x02; 32],
            token_id: [1, 2, 3, 4],
            amount: 99,
        };
        let resp = core_tx_to_body_response(&body);
        match resp {
            TransactionBodyResponse::TokenTransfer {
                to,
                token_id,
                amount,
            } => {
                assert_eq!(to, hex::encode([0x02; 32]));
                assert_eq!(token_id, "01020304");
                assert_eq!(amount, 99);
            }
            _ => panic!("expected TokenTransfer"),
        }
    }

    #[test]
    fn core_tx_to_body_response_mint() {
        let body = TransactionBody::Mint {
            ticker: "BNK".to_string(),
            max_supply: 1_000_000,
            metadata_hash: [0xAB; 32],
        };
        let resp = core_tx_to_body_response(&body);
        match resp {
            TransactionBodyResponse::Mint {
                ticker,
                max_supply,
                metadata_hash,
            } => {
                assert_eq!(ticker, "BNK");
                assert_eq!(max_supply, 1_000_000);
                assert_eq!(metadata_hash, hex::encode([0xAB; 32]));
            }
            _ => panic!("expected Mint"),
        }
    }

    #[test]
    fn core_tx_to_body_response_bond() {
        let body = TransactionBody::Bond {
            validator: [0x03; 32],
            amount: 500,
        };
        let resp = core_tx_to_body_response(&body);
        match resp {
            TransactionBodyResponse::Bond { validator, amount } => {
                assert_eq!(validator, hex::encode([0x03; 32]));
                assert_eq!(amount, 500);
            }
            _ => panic!("expected Bond"),
        }
    }

    #[test]
    fn core_tx_to_body_response_retire() {
        let body = TransactionBody::Retire {
            validator: [0x04; 32],
            amount: 200,
        };
        let resp = core_tx_to_body_response(&body);
        match resp {
            TransactionBodyResponse::Retire { validator, amount } => {
                assert_eq!(validator, hex::encode([0x04; 32]));
                assert_eq!(amount, 200);
            }
            _ => panic!("expected Retire"),
        }
    }

    #[test]
    fn core_tx_to_body_response_withdraw() {
        let body = TransactionBody::Withdraw {
            validator: [0x05; 32],
        };
        let resp = core_tx_to_body_response(&body);
        match resp {
            TransactionBodyResponse::Withdraw { validator } => {
                assert_eq!(validator, hex::encode([0x05; 32]));
            }
            _ => panic!("expected Withdraw"),
        }
    }

    #[test]
    fn core_tx_to_body_response_unjail() {
        let resp = core_tx_to_body_response(&TransactionBody::UnJail);
        assert!(matches!(resp, TransactionBodyResponse::UnJail));
    }

    #[test]
    fn core_tx_to_body_response_set_commission() {
        let body = TransactionBody::SetCommission { rate: 1500 };
        let resp = core_tx_to_body_response(&body);
        match resp {
            TransactionBodyResponse::SetCommission { rate } => assert_eq!(rate, 1500),
            _ => panic!("expected SetCommission"),
        }
    }

    // -- BlockDetailResponse serialization tests --

    #[test]
    fn block_detail_response_without_transactions() {
        let block = Block::Block {
            slot: 1,
            hash: "abc".into(),
            parent_slot: 0,
            parent_hash: "def".into(),
            producer: 1,
            proposed_timestamp: 100,
            finalized_timestamp: None,
            status: SlotStatus::Proposed,
        };
        let resp = BlockDetailResponse {
            block: block.clone(),
            transactions: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        // without transactions, should look the same as a plain Block
        let plain_json = serde_json::to_string(&block).unwrap();
        // both should parse to same value (no extra "transactions" key)
        let resp_val: serde_json::Value = serde_json::from_str(&json).unwrap();
        let plain_val: serde_json::Value = serde_json::from_str(&plain_json).unwrap();
        assert_eq!(resp_val, plain_val);
    }

    #[test]
    fn block_detail_response_with_transactions() {
        let block = Block::Block {
            slot: 5,
            hash: "abc".into(),
            parent_slot: 4,
            parent_hash: "def".into(),
            producer: 1,
            proposed_timestamp: 100,
            finalized_timestamp: Some(200),
            status: SlotStatus::Finalized,
        };
        let txs = vec![TransactionSummary {
            hash: "txhash".into(),
            sender: "sender".into(),
            nonce: 1,
            fee: 10,
            body: TransactionBodyResponse::Transfer {
                to: "recipient".into(),
                amount: 42,
            },
        }];
        let resp = BlockDetailResponse {
            block,
            transactions: Some(txs),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let val: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(val.get("transactions").is_some());
        assert_eq!(val["transactions"].as_array().unwrap().len(), 1);
        assert_eq!(val["type"], "block");
        assert_eq!(val["slot"], 5);
    }

    // -- TransactionBodyResponse serialization --

    #[test]
    fn transaction_body_response_json_has_type_tag() {
        let resp = TransactionBodyResponse::Transfer {
            to: "abc".into(),
            amount: 100,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"type\":\"Transfer\""));

        let resp = TransactionBodyResponse::UnJail;
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"type\":\"UnJail\""));
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
        .route("/block/slot/{slot_num}", get(block_by_slot))
        .route("/transactions/{hash}", get(get_transaction))
        .route("/transactions", get(list_transactions).post(submit_transaction))
        .route("/mempool", get(mempool))
        .route("/mempool/{hash}", get(mempool_transaction))
        .route("/accounts/{pubkey}", get(get_account))
        .route("/accounts/{pubkey}/tokens", get(get_account_tokens))
        .route("/tokens", get(get_tokens))
        .route("/tokens/{id}", get(get_token))
        .route("/tokens/{id}/holders", get(get_token_holders))
        .route("/staking", get(get_staking))
        .route("/snapshots/latest", get(get_latest_snapshot_manifest))
        .route("/snapshots/{epoch}", get(get_snapshot_manifest))
        .route("/snapshots/{epoch}/chunks/{index}", get(get_snapshot_chunk))
        .route("/genesis", get(get_genesis))
        .route("/ws", get(websocket_handler))
        .layer(cors)
        .with_state(state);
    // Bind address is overridable via BUNKER_RPC_ADDR (default loopback-only).
    // Set it to e.g. `0.0.0.0:3001` (or `<tailnet-ip>:3001`) when the API must
    // be reachable from another machine, such as the bastion proxying the
    // explorer's chain queries over Tailscale.
    let bind_addr =
        std::env::var("BUNKER_RPC_ADDR").unwrap_or_else(|_| "127.0.0.1:3001".to_owned());
    let listener = match tokio::net::TcpListener::bind(&bind_addr).await {
        Ok(listener) => listener,
        Err(err) => {
            eprintln!("failed to bind API server on {bind_addr}: {err}");
            return;
        }
    };
    if let Err(err) = axum::serve(listener, app).await {
        eprintln!("API server stopped with error: {err}");
    }
}
