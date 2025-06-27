# BunkerCoin
A low-bandwidth, shortwave radio-compatible blockchain protocol using Alpenglow consensus.

Contributions welcomed!

## Quick Start

### Run the simulated v0 prototype

This demo runs a multi-node Alpenglow consensus simulation over a simulated radio network, and exposes a (very basic) live block explorer API.

```bash
cargo run --bin node-api-radio-proto
```

- The explorer API will be available at http://localhost:3001
- See [Explorer API Spec](docs/rpc_spec.md) for endpoints

### Repo Overview

- `bunker_coin_core`: Shared types (hashes, blocks, transactions)
- `bunker_coin_radio`: Radio networking implementation
  - Simulated radio with realistic HF propagation
- `bunker_coin_sim`: Simulation scenarios and tools
- `bunker_coin_rpc`: API & RPC (wip)


## Radio Networking

The radio layer right now simulates realistic HF (shortwave) conditions:
- **Global Bandwidth** >> 50-2400 bps (depending on conditions)
- **Packet Loss** >> 5-50% (ionospheric effects)
- **Latency** >> 100-1000ms (propagation) ++ jitter
- _**MTU** >> 300 bytes (standard for packet radio)_ supported but inactive right now
- **Erasure Coding** (2:6 for now due to empty blocks, later 32:96)

## API

- [Explorer API Spec](docs/api_spec.md)

## Next Steps

1. Persistency, right now everything stored in-memory
2. Support for Transactions
3. Physical Radio devices instead of the simulated one
4. Remainders from the BunkerCoin spec (aggregation of bls, etc.)\
... (many more)

>  **Core contributors welcomed!** If you'd like to help shape the protocol from the ground up, get in touch.


## References

- [Alpenglow](https://github.com/qkniep/alpenglow) (A true piece of art)
- BunkerCoin Whitepaper: `bunker_coin.tex`

## Building

Ensure you have a recent stable Rust toolchain installed.

```bash
rustup update stable
cargo check
```

Nothing is stabilised yet, expect rapid iteration. 