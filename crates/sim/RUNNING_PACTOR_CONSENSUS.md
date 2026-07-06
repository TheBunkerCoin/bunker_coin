# Running BunkerCoin consensus over PACTOR

How to run the real Alpenglow consensus loop (block production → shred
dissemination → Votor voting → certification → finalization) between **two**
nodes, either over **two real SCS Dragon DR-7400 PACTOR modems** on HF (no
internet) or **in-process** with a simulated link.

Binary: `crates/sim/src/bin/pactor_consensus.rs`
(package `bunker_coin_sim`, bin `pactor_consensus`).

Currently produces **empty blocks** (no transactions); execution/tx parity is a
separate follow-up.

---

## TL;DR

```bash
# Off-air smoke test (one machine, no modems):
cargo run -p bunker_coin_sim --bin pactor_consensus -- --simulated --half-duplex

# On-air, two modems on two machines — run ONE of these per machine:
#   (build --release first; see below)
target/release/pactor_consensus --port /dev/ttyUSB0 --node 0 --mycall NODE0 --peercall NODE1 --frequency 14050.0 --reset --rpc
target/release/pactor_consensus --port /dev/ttyUSB0 --node 1 --mycall NODE1 --peercall NODE0 --frequency 14050.0 --reset

# Inspect the finalized chain (no modem needed), from the node's working dir:
target/release/pactor_consensus --inspect --node 0
curl -s localhost:3001/blocks | python3 -m json.tool
```

---

## 0. Build

Always build **release** for on-air runs — debug builds are slow enough to skew
the half-duplex timing.

```bash
cd <repo root>
cargo build --release -p bunker_coin_sim --bin pactor_consensus
# binary: target/release/pactor_consensus
```

Both machines must run the **same build** — the wire format (slice payload,
compressed signatures) must match, and several increments are breaking wire
changes. After pulling new code, rebuild on **both** machines.

---

## 1. Off-air: simulated run (no modems)

Runs **both** nodes in one process over a simulated PACTOR pair. Use this to
verify the whole pipeline without burning HF time.

```bash
# Faithful half-duplex link (single shared channel, ARQ changeover, ~10x slower
# reverse path) — reproduces real on-air behavior, including stalls:
cargo run -p bunker_coin_sim --bin pactor_consensus -- --simulated --half-duplex

# Clean full-duplex link (symmetric) — fastest; validates wiring only:
cargo run -p bunker_coin_sim --bin pactor_consensus -- --simulated
```

Add `--rpc` to serve the API on `127.0.0.1:3001`, `--duration N` to auto-stop
after N seconds (omit to run until Ctrl-C). With `RUST_LOG=info` you'll see
`finalized slot N` lines.

---

## 2. On-air: two real modems

### Hardware layout

- **Two machines**, one modem each → both use `--port /dev/ttyUSB0`.
- **One machine, two modems** → each modem is a different device
  (`/dev/ttyUSB0`, `/dev/ttyUSB1`), and **each node must run in its own working
  directory** because the on-disk chain lives in `./data/` relative to the
  current dir (two nodes in the same dir would share one RocksDB tree). Use
  `cd /tmp/bc-node0` / `cd /tmp/bc-node1` (or any two dirs) before launching.

Find your serial devices (Linux): `ls /dev/ttyUSB*`. (macOS:
`ls /dev/cu.usbserial-*`.)

### Roles

- **Node 0 is the caller** (initiates the connect) and the **master** of the
  half-duplex link.
- **Node 1 is the listener** (`LISTEN 1` + accepts the incoming connect) and the
  **slave**. The slave→master (reverse) path is ~10× slower — this is the
  bottleneck the consensus tuning is built around.

### Commands

One machine / two modems / two terminals (most common dev setup):

```bash
# Terminal 1 — node 0 (caller), modem on ttyUSB0
mkdir -p /tmp/bc-node0 && cd /tmp/bc-node0
RUST_LOG=scs_pactor=trace ~/Documents/bunker_coin/target/release/pactor_consensus \
  --port /dev/ttyUSB0 --node 0 --mycall NODE0 --peercall NODE1 \
  --frequency 14050.0 --reset --rpc

# Terminal 2 — node 1 (listener), modem on ttyUSB1
mkdir -p /tmp/bc-node1 && cd /tmp/bc-node1
RUST_LOG=scs_pactor=trace ~/Documents/bunker_coin/target/release/pactor_consensus \
  --port /dev/ttyUSB1 --node 1 --mycall NODE1 --peercall NODE0 \
  --frequency 14050.0 --reset
```

> **Proven on hardware.** This exact setup — single machine, two modems, each
> node in its own `/tmp/bc-nodeN` working dir on ttyUSB0 / ttyUSB1, run in
> **continuous mode (no `--duration`, Ctrl-C to stop)** — has finalized a long
> contiguous chain on-air, climbing to **90+ finalized slots** with the link
> stable across the multi-minute inter-block quiets. Adjust the repo path
> (`~/Documents/bunker_coin`) to wherever the repo lives on your machine.

Two machines / one modem each: identical, but both use `--port /dev/ttyUSB0`
and you don't need the per-node working dirs (each machine has its own `./data/`).

### First run: clear stale state

The persisted chain is unreadable across breaking wire changes, and a previous
run's `data/` will be reloaded on start. For a fresh chain, clear it once per
node before the first run after a rebuild that changed the wire format:

```bash
rm -rf data           # in each node's working dir
```

---

## 3. Running continuously

Omit `--duration` → the run continues until **Ctrl-C**, which shuts down
gracefully (tears down consensus, releases the modem serial port and the RocksDB
lock, persists the chain).

```bash
... pactor_consensus --port /dev/ttyUSB0 --node 0 ... --rpc      # runs until Ctrl-C
... pactor_consensus --port /dev/ttyUSB0 --node 0 ... --duration 3600   # stops after 1h
```

The hardware path **auto-reconnects across band drops**: each session
re-establishes the link and resumes from the persisted finalized slot, so a
multi-minute band collapse is a recoverable pause, not a restart. A continuous
run rides out drops indefinitely.

---

## 4. Watching the chain

With `--rpc` (serves `127.0.0.1:3001`, same API as the simulations):

```bash
watch -n 5 'curl -s localhost:3001/blocks'
# or pretty-printed once:
curl -s localhost:3001/blocks | python3 -m json.tool
```

Only one node can bind port 3001 — pass `--rpc` to **one** node (node 0 above).

You want to see finalized slots with distinct hashes and each `parent_hash`
matching the prior slot's `hash` (1←2←3←…, no skips), `status: "finalized"`,
and a real `finalized_timestamp`.

**Viewing from a laptop over SSH** — port-forward the RPC:
```bash
ssh -L 3001:localhost:3001 user@node       # on the laptop
# then start the run on the node; browse http://localhost:3001/blocks locally
```

---

## 5. Inspecting the chain offline (`--inspect`)

The live `--rpc` server only runs while the link is up. To inspect the persisted
chain **after a run ends** (or any time, no modem), open the on-disk block store
and serve the same API without touching the modem:

```bash
cd /tmp/bc-node0     # the dir that node ran in (where ./data/ lives)
<repo>/target/release/pactor_consensus --inspect --node 0
curl -s localhost:3001/blocks | python3 -m json.tool
```

Use `--node 1` from node 1's dir. Runs until Ctrl-C (or `--duration`). Note:
finalized status is only persisted for blocks finalized by a build that includes
the persistence fix — older `data/` may show less.

---

## 6. Flags reference

| Flag | Mode | Meaning |
|------|------|---------|
| `--simulated` | off-air | Run both nodes in-process over a simulated link |
| `--half-duplex` | off-air | With `--simulated`: faithful single-channel, slow-reverse model |
| `--port <dev>` | hardware | Serial device for the modem (e.g. `/dev/ttyUSB0`) |
| `--node <0\|1>` | both | Which validator this process is. 0 = caller/master, 1 = listener/slave |
| `--mycall <CALL>` | hardware | This modem's callsign (default `NODE0`) |
| `--peercall <CALL>` | hardware | Peer callsign to connect to (default `NODE1`) |
| `--frequency <kHz>` | hardware | Optional TRX CI-V tune frequency (e.g. `14050.0`) |
| `--baud <n>` | hardware | Serial baud rate (default `829440`) |
| `--delta-mult <f>` | hardware | Consensus timing multiplier (default 6). Raise for slower bands |
| `--connect-attempts <n>` | hardware | Connect attempts before giving up (default 3) |
| `--reset` | hardware | Force-disconnect any stale link before init |
| `--seed <n>` | both | Validator-set seed; **must match on both nodes** (default 0) |
| `--duration <secs>` | all | Stop after N seconds. **Omit to run until Ctrl-C** |
| `--rpc` | run | Serve the HTTP API on `127.0.0.1:3001` |
| `--inspect` | offline | Serve the API over the on-disk chain; no modem, no consensus. Needs `--node` |

Both nodes must use the **same `--seed`** (so they derive identical keys/stake)
and the **same build**.

---

## 7. Tuning & health

- **Connect time is a band-health proxy.** Fast connect (≤~12s) ⇒ healthy band ⇒
  data flows. Slow connect (≥~30s) ⇒ marginal band ⇒ mid-transfer drops.
- **`--frequency`**: pick a clear slot; time-of-day/antenna matter more than any
  code knob for a stable reverse path.
- **`--delta-mult`**: default 6 stretches block cadence / timeouts to match the
  slow link. Raise it on a slower band (so the crashed-leader timeout doesn't
  fire before a slow first shred arrives); empty-block production is decoupled
  from it, so the leader still disseminates promptly.
- **Finalization latency is the band, not the code.** Expect tens of seconds to
  several minutes per slot, variable with conditions. As long as the *finalized*
  frontier keeps climbing, it's healthy — the *produced* frontier running ahead
  is expected.

Environment variables (set automatically on the hardware path; override for
experiments):
- `BUNKER_DELTA_MULT` — same as `--delta-mult`.
- `BUNKER_DEFER_FINAL_VOTE=1` — defer the slow-path finalization vote so a
  fast-finalized slot sends nothing extra over the reverse path.

---

## 8. Troubleshooting

| Symptom | Cause / fix |
|---------|-------------|
| `Device or resource busy` on open | Another process holds the serial port; or a previous run didn't release it. Wait a few seconds / unplug-replug the modem. |
| `LOCK: No locks available` (RocksDB) | A previous node process is still alive holding `data/`. Ensure the prior run exited (Ctrl-C cleanly). |
| Nothing finalizes past slot 0 | Band too marginal to disseminate+vote+certify before STBY. Try a clearer frequency/time; raise `--delta-mult`. |
| Both nodes bind port 3001 | Pass `--rpc` to only one node. |
| `--inspect` shows `[]` / few blocks | Old `data/` written by a pre-persistence build, or wrong working dir. Run from the node's actual working dir; re-run on a current build. |
| Wrong-parent / decode errors after pull | One node on an old build (breaking wire change). Rebuild **both**; clear `data/`. |
| Two nodes on one machine collide | Use different `--port` **and** different working dirs (each node's `./data/` must be separate). |
