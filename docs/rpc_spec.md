# BunkerCoin API Specification

Work in progress, this is just a very basic version for the block explorer.

## Overview
The BunkerCoin API provides access to blockchain data including blocks and node status. The API runs on port 3001 and returns JSON responses.


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
    "timestamp": 1234567890123,
    "status": "finalized"
  },
  {
    "type": "skip",
    "slot": 122,
    "hash": "skip-122",
    "timestamp": 1234567890100,
    "status": "finalized"
  }
]
```

Note: Items are ordered newest to oldest within the response array.

### GET /block/:hash
Returns a specific block or skip certificate by its hash.

#### Parameters
- `hash`: The block hash (64 character hex string) or skip certificate identifier

#### Response for Block
```json
{
  "type": "block",
  "slot": 123,
  "hash": "a1b2c3d4e5f6...",
  "parent_slot": 122,
  "parent_hash": "9f8e7d6c5b4a...",
  "producer": 0,
  "timestamp": 1234567890123,
  "status": "finalized"
}
```

#### Response for Skip Certificate
```json
{
  "type": "skip",
  "slot": 122,
  "hash": "skip-122",
  "timestamp": 1234567890100,
  "status": "finalized"
}
```

Returns 404 if block/skip not found.

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
  "packet_loss_rate_2s": 25.0
}
```

**Fields:**
- `packets_sent_2s`: Number of packets sent in the last 2 seconds
- `packets_dropped_2s`: Number of packets dropped in the last 2 seconds
- `packets_transmitted_2s`: Number of packets transmitted in the last 2 seconds
- `bytes_transmitted_2s`: Total bytes transmitted in the last 2 seconds
- `effective_throughput_bps_2s`: Effective throughput in bits per second (last 2s)
- `packet_loss_rate_2s`: Packet loss percentage in the last 2 seconds (capped at 25%)

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

## CORS Policy
The API currently allows cross-origin requests from:
- Any `http://localhost` port
- Any subdomain of `*.bunkercoin.io`