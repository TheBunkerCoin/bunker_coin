//! Smoke-verify checkpoint output: read pool DB rows exactly as
//! PoolImpl::load_from_db does and validate the certs.
use bunkerglow::consensus::{Cert, EpochInfo};
use bunkerglow::crypto::{aggsig, signature};
use bunkerglow::ValidatorInfo;
use rand::rngs::StdRng;
use rand::SeedableRng;
use rocksdb::IteratorMode;

fn main() {
    let sp = std::env::args().nth(1).unwrap();
    let seed: u64 = std::env::args().nth(2).unwrap().parse().unwrap();
    let mut rng = StdRng::seed_from_u64(seed);
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
    }
    let epoch_info = EpochInfo::new(0, 0, validators.clone());
    let db = rocksdb::DB::open_default(format!("{sp}/pool0")).unwrap();
    let mut n = 0;
    for item in db.iterator(IteratorMode::Start) {
        let (k, v) = item.unwrap();
        if k.starts_with(b"cert|") {
            let cert: Cert = wincode::deserialize(&v).expect("wincode deserialize");
            assert!(cert.check_threshold(&epoch_info), "threshold {k:?}");
            assert!(cert.check_sig(&validators), "sig {k:?}");
            println!(
                "OK {} {} slot {}",
                String::from_utf8_lossy(&k),
                cert.kind_str(),
                cert.slot()
            );
            n += 1;
        } else if k.as_ref() == b"meta|final_slot" {
            let arr: [u8; 8] = v[..8].try_into().unwrap();
            println!("meta|final_slot = {}", u64::from_be_bytes(arr));
        }
    }
    assert_eq!(n, 12, "expected 12 cert rows (6 slots x notar+final)");
    println!("ALL {n} CERTS VALIDATE — exactly as load_from_db will read them");
}
