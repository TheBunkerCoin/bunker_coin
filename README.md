# BunkerCoin

A low-bandwidth, short-wave-radio-compatible blockchain protocol.

This repository is *work-in-progress*.  At the moment it only contains the `core` crate, which will house fundamental stuff such as cryptographic primitives and consensus logic.

```
./Cargo.toml        # workspace manifest
crates/
  └── core/         # library crate (empty skeleton for now)
```

## Building

Ensure you have a recent stable Rust toolchain installed.

```bash
rustup update stable
cargo check
```

No public API is stabilised yet, expect rapid iteration. 