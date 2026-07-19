//! One-time operator checkpoint tool for the 2-node PACTOR net.
//!
//! ## Why this exists
//!
//! Slots 8808..=8815 were produced under a build whose locally-created
//! notarization/fast-final certificates were minted missing their
//! quorum-tipping vote (fixed in fba5b2b) — invalid on the wire, trusted
//! locally. The purge of those poison certs (fba5b2b) left the range without
//! provable finality, and the one-shot notar votes that could rebuild the
//! certs were consumed in that era: vote histories pruned them, the "already
//! voted" safety guard (correctly) forbids re-voting, and below-floor bounds
//! reject rebroadcasts. In-protocol recovery of the range is impossible.
//!
//! ## What it does
//!
//! Both validators' voting keys derive from the shared `--seed`, and both
//! nodes are operated by the same party — so the operator can sign a
//! checkpoint. For each slot in the canonical chain of the damaged range
//! (default 8809..=8815, skipping the orphaned 8811, whose valid skip cert is
//! left in place):
//!
//! 1. locate the block in either node's blockstore (by slot-prefixed key),
//!    verify the two stores agree on the hash, and copy block + metadata rows
//!    to whichever store is missing them;
//! 2. sign notar + final votes for the block with BOTH validators' voting
//!    keys and aggregate them into fully valid `NotarCert` + `FinalCert`
//!    (indistinguishable from honest certs: they are signed by the real
//!    validator keys);
//! 3. write the certs into BOTH pool DBs and delete any stale
//!    skip/notar-fallback/fast-final rows for those slots;
//! 4. set `meta|final_slot` in both pool DBs to the checkpoint slot.
//!
//! After a restart, both nodes load a provable finalized floor at the
//! checkpoint and production resumes on the slot above it.
//!
//! ## Safety rails
//!
//! - Validates EVERYTHING before writing ANYTHING: a slot whose block is
//!   missing from both stores, or whose hash differs between stores, aborts
//!   the run with no writes.
//! - Refuses to run if any pool/blockstore DB is locked (nodes must be
//!   stopped — kill the tmux wrapper sessions first, or the wrappers will
//!   relaunch the nodes mid-surgery).
//! - Certs are threshold- and signature-checked before being written.
//!
//! ## Usage (on the node box, both nodes STOPPED)
//!
//! ```text
//! checkpoint --seed <SEED> \
//!   --pool0  ~/bc-node0/data/pool/0  --store0 ~/bc-node0/data/blockstore/0 \
//!   --pool1  ~/bc-node1/data/pool/1  --store1 ~/bc-node1/data/blockstore/1 \
//!   --from 8809 --to 8815 --orphaned 8811
//! ```

use std::collections::BTreeMap;
use std::path::PathBuf;

use bunkerglow::consensus::{Cert, EpochInfo, FinalCert, NotarCert, Vote};
use bunkerglow::crypto::merkle::BlockHash;
use bunkerglow::crypto::{Hash, aggsig, signature};
use bunkerglow::types::Slot;
use bunkerglow::ValidatorInfo;
use clap::Parser;
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use rocksdb::{DB, IteratorMode};

#[derive(Parser)]
#[command(about = "Operator checkpoint: sign real certs for a damaged slot range")]
struct Args {
    /// Cluster seed — MUST match the --seed both nodes run with.
    #[arg(long)]
    seed: u64,
    /// Node 0 pool RocksDB directory (e.g. ~/bc-node0/data/pool/0).
    #[arg(long)]
    pool0: PathBuf,
    /// Node 1 pool RocksDB directory (e.g. ~/bc-node1/data/pool/1).
    #[arg(long)]
    pool1: PathBuf,
    /// Node 0 blockstore RocksDB directory (e.g. ~/bc-node0/data/blockstore/0).
    #[arg(long)]
    store0: PathBuf,
    /// Node 1 blockstore RocksDB directory (e.g. ~/bc-node1/data/blockstore/1).
    #[arg(long)]
    store1: PathBuf,
    /// First slot of the range to checkpoint.
    #[arg(long)]
    from: u64,
    /// Last slot of the range — becomes the new finalized floor.
    #[arg(long)]
    to: u64,
    /// Orphaned slots inside the range to leave skipped (their skip certs are
    /// kept; any notar/final rows for them are deleted).
    #[arg(long)]
    orphaned: Vec<u64>,
    /// Print what would be done without writing anything.
    #[arg(long)]
    dry_run: bool,
}

/// Block + metadata rows for one slot as found in a blockstore.
struct BlockRows {
    hash_hex: String,
    block_key: Vec<u8>,
    block_val: Vec<u8>,
    meta: Option<(Vec<u8>, Vec<u8>)>,
}

/// Scan one blockstore for the block stored under the given slot.
/// Keys are `{:016X}{64-hex-hash}` for blocks and `meta|...` for metadata.
fn find_block(db: &DB, slot: u64) -> Option<BlockRows> {
    let prefix = format!("{slot:016X}");
    let mut rows: Option<BlockRows> = None;
    for item in db.iterator(IteratorMode::From(prefix.as_bytes(), rocksdb::Direction::Forward)) {
        let Ok((k, v)) = item else { break };
        let Ok(key) = std::str::from_utf8(&k) else { break };
        if !key.starts_with(&prefix) {
            break;
        }
        if key.len() == 16 + 64 {
            if rows.is_some() {
                panic!("blockstore has MULTIPLE blocks for slot {slot} — refusing to choose");
            }
            rows = Some(BlockRows {
                hash_hex: key[16..].to_string(),
                block_key: k.to_vec(),
                block_val: v.to_vec(),
                meta: None,
            });
        }
    }
    if let Some(rows) = &mut rows {
        let meta_key = format!("meta|{:016X}{}", slot, rows.hash_hex);
        if let Ok(Some(v)) = db.get(meta_key.as_bytes()) {
            rows.meta = Some((meta_key.into_bytes(), v));
        }
    }
    rows
}

fn main() {
    let args = Args::parse();
    assert!(args.from <= args.to, "--from must be <= --to");

    // ---- derive the cluster exactly as pactor_consensus::build_cluster ----
    let mut rng = StdRng::seed_from_u64(args.seed);
    let mut voting_keys: Vec<aggsig::SecretKey> = Vec::new();
    let mut validators: Vec<ValidatorInfo> = Vec::new();
    for id in 0..2u64 {
        let sk = signature::SecretKey::new(&mut rng);
        let vk = aggsig::SecretKey::new(&mut rng);
        validators.push(ValidatorInfo {
            id,
            stake: 1,
            pubkey: sk.to_pk(),
            voting_pubkey: vk.to_pk(),
            all2all_address: "0.0.0.0:0".parse().unwrap(),
            disseminator_address: "0.0.0.0:0".parse().unwrap(),
            repair_request_address: "0.0.0.0:0".parse().unwrap(),
            repair_response_address: "0.0.0.0:0".parse().unwrap(),
            location: None,
        });
        voting_keys.push(vk);
    }
    // keep the RNG stream identical to build_cluster (genesis key draw)
    let mut genesis_seed = [0u8; 32];
    rng.fill_bytes(&mut genesis_seed);
    let epoch_info = EpochInfo::new(0, 0, validators.clone());

    // ---- open all four DBs (fails if a node is still running) ----
    let store0 = DB::open_default(&args.store0).expect("open store0 (is node 0 stopped?)");
    let store1 = DB::open_default(&args.store1).expect("open store1 (is node 1 stopped?)");
    let pool0 = DB::open_default(&args.pool0).expect("open pool0 (is node 0 stopped?)");
    let pool1 = DB::open_default(&args.pool1).expect("open pool1 (is node 1 stopped?)");

    // ---- phase 1: validate the whole range, build every write, no writes yet ----
    struct SlotPlan {
        slot: u64,
        hash_hex: String,
        notar: Vec<u8>,
        fin: Vec<u8>,
        copy_to_0: Option<BlockRows>,
        copy_to_1: Option<BlockRows>,
    }
    let mut plans: Vec<SlotPlan> = Vec::new();
    let mut summary: BTreeMap<u64, String> = BTreeMap::new();

    for slot in args.from..=args.to {
        if args.orphaned.contains(&slot) {
            summary.insert(slot, "orphaned — skip cert left in place".into());
            continue;
        }
        let in0 = find_block(&store0, slot);
        let in1 = find_block(&store1, slot);
        let (hash_hex, copy_to_0, copy_to_1) = match (in0, in1) {
            (Some(a), Some(b)) => {
                assert_eq!(
                    a.hash_hex, b.hash_hex,
                    "stores DISAGREE on the block for slot {slot} — aborting, nothing written"
                );
                (a.hash_hex.clone(), None, None)
            }
            (Some(a), None) => (a.hash_hex.clone(), None, Some(a)),
            (None, Some(b)) => (b.hash_hex.clone(), Some(b), None),
            (None, None) => panic!(
                "block for slot {slot} missing from BOTH stores — aborting, nothing written"
            ),
        };

        let mut hash_bytes = [0u8; 32];
        hex::decode_to_slice(&hash_hex, &mut hash_bytes).expect("bad hash hex in block key");
        let block_hash: BlockHash = Hash::from_bytes(hash_bytes).into();

        let notar_votes: Vec<Vote> = (0..2u64)
            .map(|v| {
                Vote::new_notar(
                    Slot::new(slot),
                    block_hash.clone(),
                    &voting_keys[v as usize],
                    v,
                )
            })
            .collect();
        let final_votes: Vec<Vote> = (0..2u64)
            .map(|v| Vote::new_final(Slot::new(slot), &voting_keys[v as usize], v))
            .collect();
        let notar = Cert::Notar(NotarCert::try_new(&notar_votes, &validators).unwrap());
        let fin = Cert::Final(FinalCert::try_new(&final_votes, &validators).unwrap());
        assert!(
            notar.check_threshold(&epoch_info) && notar.check_sig(&validators),
            "constructed notar cert for slot {slot} failed self-check"
        );
        assert!(
            fin.check_threshold(&epoch_info) && fin.check_sig(&validators),
            "constructed final cert for slot {slot} failed self-check"
        );

        summary.insert(
            slot,
            format!(
                "block {}… notar+final certs signed{}",
                &hash_hex[..8],
                match (&copy_to_0, &copy_to_1) {
                    (Some(_), _) => " (block copied → node0 store)",
                    (_, Some(_)) => " (block copied → node1 store)",
                    _ => "",
                }
            ),
        );
        plans.push(SlotPlan {
            slot,
            hash_hex,
            notar: wincode::serialize(&notar).unwrap(),
            fin: wincode::serialize(&fin).unwrap(),
            copy_to_0,
            copy_to_1,
        });
    }

    println!("== checkpoint plan (floor -> {}) ==", args.to);
    for (slot, what) in &summary {
        println!("  slot {slot}: {what}");
    }
    if args.dry_run {
        println!("dry run — nothing written.");
        return;
    }

    // ---- phase 2: apply ----
    for plan in &plans {
        if let Some(rows) = &plan.copy_to_0 {
            store0.put(&rows.block_key, &rows.block_val).unwrap();
            if let Some((mk, mv)) = &rows.meta {
                store0.put(mk, mv).unwrap();
            }
        }
        if let Some(rows) = &plan.copy_to_1 {
            store1.put(&rows.block_key, &rows.block_val).unwrap();
            if let Some((mk, mv)) = &rows.meta {
                store1.put(mk, mv).unwrap();
            }
        }
        for pool in [&pool0, &pool1] {
            // kind bytes match PoolImpl::add_valid_cert: 0=Notar, 1=NotarFallback,
            // 2=Skip, 3=FastFinal, 4=Final.
            pool.put(format!("cert|{:016X}|0", plan.slot).as_bytes(), &plan.notar)
                .unwrap();
            pool.put(format!("cert|{:016X}|4", plan.slot).as_bytes(), &plan.fin)
                .unwrap();
            // remove contradictory / stale rows for a now-finalized slot
            for stale_kind in [1u8, 2, 3] {
                let _ = pool.delete(format!("cert|{:016X}|{stale_kind}", plan.slot).as_bytes());
            }
        }
        println!(
            "slot {}: certs written for block {}…",
            plan.slot,
            &plan.hash_hex[..8]
        );
    }
    // orphaned slots: keep skip certs, drop any stale notar/fast-final/final rows
    for &slot in &args.orphaned {
        for pool in [&pool0, &pool1] {
            for stale_kind in [0u8, 1, 3, 4] {
                let _ = pool.delete(format!("cert|{slot:016X}|{stale_kind}").as_bytes());
            }
        }
        println!("slot {slot}: orphaned — skip cert kept, other cert rows cleared");
    }
    for pool in [&pool0, &pool1] {
        pool.put(b"meta|final_slot", args.to.to_be_bytes()).unwrap();
    }
    println!("floor set to {} in both pool DBs. Restart both nodes.", args.to);
}
