# BunkerCoin API Specification

Work in progress, this is just a very basic version for the block explorer.

## Overview
The BunkerCoin API provides access to blockchain data including blocks, transactions, accounts, and node status. The API runs on port 3001 and returns JSON responses.

## Endpoints

### GET /blocks
Returns a paginated list of blocks and skip certificates in reverse chronological order (newest first).

#### Query Parameters
- `limit` (optional): Maximum number of items to return (default: 100, max: 100)
- `offset` (optional): Number of items to skip from the newest (default: 0)

#### Pagination for Infinite Scroll
The pagination is designed for infinite scroll patterns:
- `offset=0` returns the newest items
- `offset=100` returns items 100-199 from newest
- `offset=200` returns items 200-299 from newest
- And so on...

#### Response
```json
[
  {
    "type": "block",
    "slot": 123,
    "hash": "a1b2c3d4e5f6...",
    "parent_slot": 122,
    "parent_hash": "9f8e7d6c5b4a...",
    "producer": 0,
    "proposed_timestamp": 1234567890123,
    "finalized_timestamp": 1234567890200,
    "status": "finalized"
  },
  {
    "type": "skip",
    "slot": 122,
    "hash": "skip-122",
    "proposed_timestamp": 1234567890100,
    "finalized_timestamp": 1234567890150,
    "status": "finalized"
  }
]
```

Note: Items are ordered newest to oldest within the response array.

### GET /block/{hash}
Returns a specific block by its hash, with optional transaction details.

#### Parameters
- `hash` (path): The block hash (64-character hex string) or skip certificate identifier

#### Query Parameters
- `include_transactions` (optional): When `true`, include decoded transactions in the response. Only available for blocks loaded from the blockstore.

#### Response
```json
{
  "type": "block",
  "slot": 123,
  "hash": "a1b2c3d4e5f6...",
  "parent_slot": 122,
  "parent_hash": "9f8e7d6c5b4a...",
  "producer": 0,
  "proposed_timestamp": 1234567890123,
  "finalized_timestamp": 1234567890200,
  "status": "finalized",
  "transactions": [
    {
      "hash": "abc123...",
      "sender": "def456...",
      "nonce": 1,
      "fee": 100,
      "body": { "type": "Transfer", "to": "789abc...", "amount": 5000 }
    }
  ]
}
```

The `transactions` field is only present when `include_transactions=true` and the block is loaded from the blockstore. Returns 404 if not found.

### GET /block/slot/{slot_num}
Returns a specific block by its slot number, with optional transaction details. Checks in-memory blocks first, then falls back to the blockstore.

#### Parameters
- `slot_num` (path): The slot number (unsigned integer)

#### Query Parameters
- `include_transactions` (optional): When `true`, include decoded transactions in the response. Only available for blocks loaded from the blockstore.

#### Response
Same shape as `GET /block/{hash}`. Returns 404 if no block exists at the given slot.

### GET /nodes
Returns the current status of all nodes in the network.

#### Response
```json
[
  {
    "node_id": 0,
    "finalized_slot": 120
  }
]
```

### GET /radio
Returns current radio network statistics.

#### Response
```json
{
  "bandwidth_bps": 4800,
  "packet_loss_percent": 15.0,
  "latency_ms": 250,
  "jitter_ms": 50,
  "packets_sent": 1024,
  "packets_dropped": 42,
  "current_throughput_bps": 3200.0
}
```

### GET /transactions/{hash}
Returns a transaction by its hash. Checks the mempool first, then scans the blockstore.

#### Parameters
- `hash` (path): The transaction hash (64-character hex string)

#### Response (mempool)
```json
{
  "hash": "abc123...",
  "sender": "def456...",
  "nonce": 1,
  "fee": 100,
  "body_type": "Transfer",
  "status": { "location": "mempool" }
}
```

#### Response (confirmed)
```json
{
  "hash": "abc123...",
  "sender": "def456...",
  "nonce": 1,
  "fee": 100,
  "body": { "type": "Transfer", "to": "789abc...", "amount": 5000 },
  "status": {
    "location": "confirmed",
    "slot": 123,
    "block_hash": "a1b2c3d4e5f6..."
  }
}
```

Returns 404 if not found.

### POST /transactions
Submit a new transaction to the mempool.

#### Request Body
```json
{
  "sender": "def456...",
  "nonce": 1,
  "fee": 100,
  "body": { "Transfer": { "to": "789abc...", "amount": 5000 } },
  "signature": "aabbccdd..."
}
```

All hex fields are 64-character hex strings (32 bytes) except `signature` which is 128 characters (64 bytes).

**Transaction body variants:**
- `Transfer`: `{ "to": "<hex>", "amount": <u64> }`
- `TokenTransfer`: `{ "to": "<hex>", "token_id": "<hex>", "amount": <u64> }`
- `Mint`: `{ "ticker": "<string 3-8 chars>", "max_supply": <u64>, "metadata_hash": "<hex>" }`
- `Bond`: `{ "validator": "<hex>", "amount": <u64> }`
- `Retire`: `{ "validator": "<hex>", "amount": <u64> }`
- `Withdraw`: `{ "validator": "<hex>" }`
- `UnJail`
- `SetCommission`: `{ "rate": <u16> }`

#### Response (success)
```json
{ "hash": "abc123..." }
```

#### Error Responses
- `400 Bad Request`: Invalid sender, signature, or body
- `409 Conflict`: Transaction already in mempool
- `503 Service Unavailable`: Mempool full (max 10,000 transactions)

### GET /mempool
Returns a paginated list of pending mempool transactions.

#### Query Parameters
- `limit` (optional): Maximum number of items to return (default: 100, max: 500)
- `offset` (optional): Number of items to skip (default: 0)

#### Response
```json
{
  "transactions": [
    {
      "hash": "abc123...",
      "sender": "def456...",
      "nonce": 1,
      "fee": 100,
      "body_type": "Transfer",
      "received_at": 1234567890123
    }
  ],
  "total": 42,
  "limit": 100,
  "offset": 0
}
```

### GET /mempool/{hash}
Returns a specific mempool transaction by hash.

#### Parameters
- `hash` (path): The transaction hash (64-character hex string)

#### Response
```json
{
  "hash": "abc123...",
  "sender": "def456...",
  "nonce": 1,
  "fee": 100,
  "body_type": "Transfer",
  "received_at": 1234567890123
}
```

Returns 404 if not found.

### GET /accounts/{pubkey}
Returns account information for a given public key. Returns zero balances for unknown accounts.

#### Parameters
- `pubkey` (path): The account public key (64-character hex string)

#### Response
```json
{
  "pubkey": "def456...",
  "native_balance": 1000000,
  "token_balances": {
    "abc123...": 500
  },
  "nonce": 5
}
```

Returns `400 Bad Request` if the pubkey is invalid hex or wrong length.

### GET /tokens
Returns all registered tokens.

#### Response
```json
{
  "tokens": [
    {
      "id": "abc123...",
      "ticker": "BNK",
      "current_supply": 1000000,
      "max_supply": 10000000,
      "metadata_hash": "def456...",
      "creator": "789abc..."
    }
  ]
}
```

### GET /snapshots/latest
Returns the latest finalized state snapshot checkpoint available for node bootstrapping. The response includes the snapshot manifest plus the epoch-transition checkpoint and finalization certificates that anchor the snapshot to consensus.

#### Response
```json
{
  "manifest": {
    "epoch": 3,
    "state_hash": "0123...",
    "chunk_root": "abcd...",
    "chunk_size": 1024,
    "total_bytes": 4096,
    "chunk_count": 4
  },
  "checkpoint": {
    "epoch": 3,
    "finalized_slot": 54000,
    "transition_block": {
      "epoch": 3,
      "last_slot": 53999,
      "state_hash": "0123...",
      "snapshot_chunk_root": "abcd...",
      "snapshot_chunk_count": 4,
      "snapshot_total_bytes": 4096,
      "snapshot_chunk_size": 1024
    },
    "finalization_certs": ["..."]
  }
}
```

Returns `404 Not Found` if no snapshot with a finalized epoch-transition block is available yet.

### GET /snapshots/{epoch}
Returns the snapshot manifest for a specific epoch.

#### Parameters
- `epoch` (path): Snapshot epoch number

Response shape is the same as `GET /snapshots/latest`. Returns `404 Not Found` if the manifest or finalized checkpoint is unavailable.

### GET /snapshots/{epoch}/chunks/{index}
Returns one snapshot chunk and its Merkle proof. Chunks are only served when the epoch has a finalized checkpoint from the block that carried the epoch transition. A bootstrapping node downloads chunks `0..chunk_count-1`, verifies each proof against `checkpoint.transition_block.snapshot_chunk_root`, reconstructs the serialized state, decodes it, and checks that the decoded state's hash equals `checkpoint.transition_block.state_hash`.

Bootstrapping must reject the snapshot unless:
- the finalization certificates verify for the block that carried the epoch transition,
- `manifest.state_hash` equals `checkpoint.transition_block.state_hash`,
- `manifest.chunk_root` equals `checkpoint.transition_block.snapshot_chunk_root`,
- the chunk count, total bytes, and chunk size match the transition block,
- every chunk proof verifies against the chunk root,
- the reconstructed state hashes to `state_hash`.

#### Parameters
- `epoch` (path): Snapshot epoch number
- `index` (path): Zero-based chunk index

#### Response
```json
{
  "epoch": 3,
  "index": 0,
  "data": "001122...",
  "proof": ["aabb...", "ccdd..."]
}
```

## WebSocket Endpoint

Connect to: `ws://localhost:3001/ws`

### Message Types

#### Radio Stats (sent every 2 seconds)
```json
{
  "type": "radio_stats",
  "packets_sent_2s": 7,
  "packets_dropped_2s": 3,
  "packets_transmitted_2s": 2,
  "bytes_transmitted_2s": 2100,
  "effective_throughput_bps_2s": 8400.0,
  "packet_loss_rate_2s": 25.0,
  "packets_queued": 12
}
```

**Fields:**
- `packets_sent_2s`: Number of packets sent in the last 2 seconds
- `packets_dropped_2s`: Number of packets dropped in the last 2 seconds
- `packets_transmitted_2s`: Number of packets transmitted in the last 2 seconds
- `bytes_transmitted_2s`: Total bytes transmitted in the last 2 seconds
- `effective_throughput_bps_2s`: Effective throughput in bits per second (last 2s)
- `packet_loss_rate_2s`: Packet loss percentage in the last 2 seconds
- `packets_queued`: Number of packets currently queued for transmission

#### Block Update
```json
{
  "type": "block_update",
  "update_slot": {
    "type": "block",
    "slot": 123,
    "hash": "abcd...",
    "parent_slot": 122,
    "parent_hash": "9876...",
    "producer": 2,
    "proposed_timestamp": 1719430000000,
    "finalized_timestamp": 1719430012345,
    "status": "finalized"
  }
}
```

#### Status Change
```json
{
  "type": "block_update",
  "status_change": {
    "slot": 123,
    "hash": "abcd...",
    "old_status": "proposed",
    "new_status": "finalized"
  }
}
```

#### Transaction Received
```json
{
  "type": "transaction_received",
  "hash": "abc123...",
  "sender": "def456...",
  "fee": 100,
  "body_type": "Transfer"
}
```

## Slot Status Values
Blocks and skip certificates progress through these statuses:
- `pending` — slot is awaiting a block or skip
- `proposed` — a block has been proposed for this slot
- `notarized` — the block has been notarized by validators
- `finalized` — the block is finalized and immutable

## CORS Policy
The API allows cross-origin requests from:
- Any `http://localhost` port
- Any subdomain of `*.bunkercoin.io`
