# BunkerCoin Wallet ↔ RPC Integration Guide

BunkerCoin's node exposes a **Solana-style JSON-RPC 2.0 API** at `POST /` so a
wallet can be structured exactly like the open-source Solana wallets
(Phantom-style architecture, `solana-web3.js` interaction patterns): the same
components, the same request/response contract shapes, the same
build → sign → send → confirm loop. The one deliberate difference is the signed
payload itself: transactions are BunkerCoin's native format (nonce-based, not
blockhash-based), so the wallet uses a thin custom `Connection` class
(~150 lines, sketched below) instead of `solana-web3.js`.

Both chains use **ed25519 with 32-byte public keys**, so all Solana wallet key
infrastructure ports unchanged: BIP-39 mnemonics, ed25519 derivation,
base58-encoded addresses, `tweetnacl`/`@noble/ed25519` signing.

## Architecture mapping

| Solana wallet component | BunkerCoin wallet equivalent |
|---|---|
| Keyring / keypair store (mnemonic → ed25519) | Identical — same curve, same base58 addresses |
| `Connection` (solana-web3.js) | Custom `BunkerConnection` (sketch below) — same method names |
| `Transaction` + instructions + recent blockhash | Native tx `{sender, nonce, fee, body}` — nonce from `getAccountInfo` |
| `sendTransaction(base64)` → signature | Same: base64 payload in, base58 tx id out |
| Confirmation tracker polling `getSignatureStatuses` | Identical — `processed` → `finalized` commitment ladder |
| Devnet faucet `requestAirdrop` | Identical (nodes configured with the genesis key) |
| Cluster identity via `getGenesisHash` | Identical |

## The wallet flow

```
1. connect      →  getVersion, getGenesisHash            (cluster identity)
2. show balance →  getBalance(pubkey)                    → {context, value: amount}
3. build tx     →  getAccountInfo(sender).value.data.nonce   ← replaces getLatestBlockhash
                   (getLatestBlockhash also exists for ported code paths)
4. fee          →  getFeeForMessage(...)                 → {context, value: 100}
5. preflight    →  simulateTransaction(base64Tx)         → {context, value:{err, logs}}
6. sign         →  ed25519.sign(signingHash(tx), secretKey)
7. send         →  sendTransaction(base64Tx)             → "base58 tx id"
8. confirm      →  poll getSignatureStatuses([id]) until
                   confirmationStatus == "finalized"     (radio finality: seconds→minutes)
```

## Encodings

- **Addresses / tx ids / block hashes**: base58 in, base58 out (hex also
  accepted everywhere, for existing tooling).
- **Transaction payload for sendTransaction/simulateTransaction**: base64 of
  the UTF-8 JSON transaction (schema below). A raw JSON object param is also
  accepted.
- Amounts are integer base units (the API calls the field `lamports` in
  Solana-shaped responses; 1 unit = 1 µBUNKER).

## Transaction wire format (inside the base64)

```json
{
  "sender":    "<32-byte pubkey, hex or base58>",
  "nonce":     0,
  "fee":       100,
  "signature": "<64-byte ed25519 signature, hex or base58>",
  "body": { "Transfer": { "to": "<32-byte pubkey>", "amount": 5000 } }
}
```

Other bodies: `TokenTransfer {to, token_id, amount}`, `Mint {ticker,
max_supply, metadata_hash}`, `Burn`, `UpdateMetadata`, … (see
`crates/core/src/transaction.rs`). Wallet v1 needs `Transfer` and
`TokenTransfer`.

### Signing

The signature is ed25519 over the **signing hash** — SHA-256 of the exact byte
layout below (all integers little-endian; this must be reproduced byte-for-byte
in the wallet):

```
sha256(
  sender          — 32 bytes
  nonce           — 8 bytes LE
  fee             — 8 bytes LE
  body tag + body fields:
    Transfer:      0x00 ‖ to (32) ‖ amount (8 LE)
    TokenTransfer: 0x01 ‖ to (32) ‖ token_id (4) ‖ amount (8 LE)
    Mint:          0x02 ‖ ticker_len (4 LE) ‖ ticker ‖ max_supply (8 LE) ‖ metadata_hash (32)
    (remaining variants: see Transaction::signing_hash in crates/core/src/transaction.rs)
)
```

The nonce must equal the sender account's current `nonce` (from
`getAccountInfo`); it increments on execution, giving replay protection —
the role recent-blockhash plays on Solana.

## Method reference

All requests: `POST /` with `{"jsonrpc":"2.0","id":…,"method":…,"params":[…]}`.
Batch arrays supported. `RpcResponse` means Solana's
`{context: {slot, apiVersion}, value: …}` envelope.

| Method | Params | Result |
|---|---|---|
| `getVersion` | — | `{"solana-core":"2.0.0-bunkercoin","feature-set":1}` |
| `getGenesisHash` | — | base58 cluster id |
| `getSlot` / `getBlockHeight` | — | finalized slot number |
| `getBalance` | `[pubkey]` | RpcResponse\<u64\> (0 for missing account) |
| `getAccountInfo` | `[pubkey]` | RpcResponse\<account \| **null**\>; account = `{lamports, owner, executable, rentEpoch, space, data:{nonce, tokenBalances}}` |
| `getLatestBlockhash` | — | RpcResponse\<{blockhash, lastValidBlockHeight}\> (informational — txs are nonce-based) |
| `getFeeForMessage` | `[any]` | RpcResponse\<u64\> — recommended fee |
| `getMinimumBalanceForRentExemption` | `[size]` | `0` (no rent) |
| `sendTransaction` | `[base64Tx]` or `[txObject]` | base58 tx id |
| `simulateTransaction` | `[base64Tx]` or `[txObject]` | RpcResponse\<{err: null\|{message}, logs, unitsConsumed}\> |
| `getSignatureStatuses` | `[[id, …]]` | RpcResponse\<[status\|null, …]\>; status = `{slot, confirmations, confirmationStatus: "processed"\|"finalized", err, status}` |
| `requestAirdrop` | `[pubkey, amount]` | base58 tx id (nodes with the genesis key only) |
| `getTokenAccountsByOwner` | `[pubkey]` | RpcResponse\<token balances\> |
| `getTransaction` | `[id]` | transaction detail |
| `getBlock` / `getBlocks` | `[slot]` / `[limit, offset]` | block(s) |

Commitment note: BunkerCoin has no observable "confirmed" middle state —
a transaction is `processed` (in mempool) and then `finalized` (executed in a
finalized block). Wallets that wait for `confirmed` should treat `finalized`
as satisfying it.

Radio-latency note: finality normally lands seconds after submission when the
HF link is healthy, but can take minutes through band fades. Wallet UX should
show `processed` immediately and keep polling patiently (e.g. 5 s interval,
10 min timeout) rather than assuming Solana's ~400 ms slots.

## `BunkerConnection` sketch (TypeScript)

```ts
import { sha256 } from "@noble/hashes/sha256";
import * as ed from "@noble/ed25519";
import bs58 from "bs58";

type Body = { Transfer: { to: string; amount: number } };

export class BunkerConnection {
  constructor(private url: string) {}

  private async rpc(method: string, params: unknown[] = []) {
    const res = await fetch(this.url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
    }).then(r => r.json());
    if (res.error) throw new Error(`${method}: ${res.error.message}`);
    return res.result;
  }

  getBalance = async (pk: string) => (await this.rpc("getBalance", [pk])).value as number;
  getAccountInfo = async (pk: string) => (await this.rpc("getAccountInfo", [pk])).value;
  getNonce = async (pk: string) => (await this.getAccountInfo(pk))?.data?.nonce ?? 0;
  getFee = async () => (await this.rpc("getFeeForMessage", [null])).value as number;
  requestAirdrop = (pk: string, amount: number) =>
    this.rpc("requestAirdrop", [pk, amount]) as Promise<string>;

  signingHash(sender: Uint8Array, nonce: bigint, fee: bigint, to: Uint8Array, amount: bigint) {
    const le = (n: bigint) => {
      const b = new Uint8Array(8);
      new DataView(b.buffer).setBigUint64(0, n, true);
      return b;
    };
    return sha256(new Uint8Array([
      ...sender, ...le(nonce), ...le(fee), 0x00, ...to, ...le(amount),
    ]));
  }

  async transfer(secretKey: Uint8Array, senderB58: string, toB58: string, amount: number) {
    const sender = bs58.decode(senderB58);
    const to = bs58.decode(toB58);
    const nonce = BigInt(await this.getNonce(senderB58));
    const fee = BigInt(await this.getFee());
    const sig = await ed.signAsync(
      this.signingHash(sender, nonce, fee, to, BigInt(amount)), secretKey);
    const tx = {
      sender: senderB58, nonce: Number(nonce), fee: Number(fee),
      signature: bs58.encode(sig),
      body: { Transfer: { to: toB58, amount } } satisfies Body,
    };
    const b64 = btoa(JSON.stringify(tx));
    return this.rpc("sendTransaction", [b64]) as Promise<string>;
  }

  async confirm(id: string, timeoutMs = 600_000, pollMs = 5_000) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const { value } = await this.rpc("getSignatureStatuses", [[id]]);
      const s = value[0];
      if (s?.err) throw new Error(`tx failed: ${JSON.stringify(s.err)}`);
      if (s?.confirmationStatus === "finalized") return s;
      await new Promise(r => setTimeout(r, pollMs));
    }
    throw new Error("confirmation timeout");
  }
}
```

## Verified end-to-end

The full loop was exercised against a simulated 2-node network
(`pactor_consensus --simulated --rpc`):
`requestAirdrop` → base58 id → `getSignatureStatuses` `processed` →
`finalized` with slot → `getBalance` reflecting the transfer;
`getAccountInfo` returns `value: null` for missing accounts; base58 and hex
inputs interchangeable throughout.
