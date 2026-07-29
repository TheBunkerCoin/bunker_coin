//! LoRa gateway service — receives capsules, validates, and relays as transactions.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, Mutex};

use bunker_coin_core::capsule::{decode, verify_capsule_signature, CapsuleHeader, DecodedCapsule};
use bunker_coin_core::transaction::{Transaction, TransactionBody};

use crate::dedup::DedupCache;
use crate::rate_limit::PerSenderRateLimiter;
use crate::reassembler::FragmentReassembler;
use crate::transport::LoRaTransport;
use crate::LoRaGatewayConfig;

/// Result of processing a single frame.
#[derive(Debug, PartialEq, Eq)]
pub enum GatewayResult {
    Relayed,
    Duplicate,
    RateLimited,
    Reassembling,
    DecodeFailed(String),
    SignatureInvalid,
    BodyDeserializeFailed,
}

/// The LoRa gateway service.
#[allow(dead_code)]
pub struct LoRaGateway {
    transport: Arc<dyn LoRaTransport>,
    config: LoRaGatewayConfig,
    dedup: Mutex<DedupCache>,
    reassembler: Mutex<FragmentReassembler>,
    rate_limiter: Mutex<PerSenderRateLimiter>,
    tx_relay: mpsc::UnboundedSender<Transaction>,
}

impl LoRaGateway {
    pub fn new(
        transport: Arc<dyn LoRaTransport>,
        config: LoRaGatewayConfig,
        tx_relay: mpsc::UnboundedSender<Transaction>,
    ) -> Self {
        Self {
            transport,
            dedup: Mutex::new(DedupCache::new(config.dedup_cache_size)),
            reassembler: Mutex::new(FragmentReassembler::new(Duration::from_secs(
                config.reassembly_timeout_secs,
            ))),
            rate_limiter: Mutex::new(PerSenderRateLimiter::new(
                config.rate_capsules_per_min,
                config.rate_burst,
                config.rate_max_senders,
            )),
            config,
            tx_relay,
        }
    }

    /// Process a raw frame received from the transport.
    pub async fn process_frame(&self, raw: &[u8]) -> GatewayResult {
        let capsule = match decode(raw) {
            Ok(c) => c,
            Err(e) => return GatewayResult::DecodeFailed(e.to_string()),
        };

        let (sender, msg_id) = match &capsule {
            DecodedCapsule::Single { header, .. } => (header.sender, header.msg_id),
            DecodedCapsule::FragFirst { header, .. } => (header.sender, header.msg_id),
            DecodedCapsule::FragCont { .. } => {
                // Continuations rely on sender checks from the first fragment.
                let mut reassembler = self.reassembler.lock().await;
                return match reassembler.feed(capsule) {
                    Some((header, body, sig)) => self.finalize(&header, &body, &sig).await,
                    None => GatewayResult::Reassembling,
                };
            }
        };

        {
            let mut dedup = self.dedup.lock().await;
            if dedup.is_duplicate(&sender, msg_id) {
                return GatewayResult::Duplicate;
            }
        }

        {
            let mut rl = self.rate_limiter.lock().await;
            if !rl.check(&sender, Instant::now()) {
                return GatewayResult::RateLimited;
            }
        }

        let mut reassembler = self.reassembler.lock().await;
        match reassembler.feed(capsule) {
            Some((header, body, sig)) => {
                drop(reassembler);
                self.finalize(&header, &body, &sig).await
            }
            None => GatewayResult::Reassembling,
        }
    }

    /// Verify, deserialize, and relay a completed capsule.
    async fn finalize(&self, header: &CapsuleHeader, body: &[u8], sig: &[u8; 64]) -> GatewayResult {
        if verify_capsule_signature(&header.sender, header.nonce, header.fee, body, sig).is_err() {
            return GatewayResult::SignatureInvalid;
        }

        let tx_body: TransactionBody =
            match bincode::serde::decode_from_slice(body, bincode::config::standard()) {
                Ok((b, _)) => b,
                Err(_) => return GatewayResult::BodyDeserializeFailed,
            };

        let tx = Transaction {
            sender: header.sender,
            nonce: header.nonce,
            fee: header.fee,
            body: tx_body,
            signature: *sig,
        };

        let _ = self.tx_relay.send(tx);
        GatewayResult::Relayed
    }

    /// Run the gateway receive loop. Blocks until the transport disconnects.
    pub async fn run(&self) {
        loop {
            {
                let mut reassembler = self.reassembler.lock().await;
                reassembler.evict_expired();
            }

            match self.transport.receive().await {
                Ok(frame) => {
                    let result = self.process_frame(&frame.payload).await;
                    log::debug!("gateway frame result: {:?}", result);

                    // ACK is best-effort after relay succeeds.
                    if result == GatewayResult::Relayed {
                        let _ = self.transport.send_ack(&[0x01]).await;
                    }
                }
                Err(e) => {
                    log::warn!("transport receive error: {}", e);
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bunker_coin_core::capsule::{
        capsule_signing_hash, encode_fragmented, encode_single, CapsuleHeader,
    };
    use ed25519_dalek::{Signer, SigningKey};

    fn make_config() -> LoRaGatewayConfig {
        LoRaGatewayConfig {
            dedup_cache_size: 4096,
            reassembly_timeout_secs: 60,
            rate_capsules_per_min: 60.0, // generous for tests
            rate_burst: 10.0,
            rate_max_senders: 256,
        }
    }

    fn make_signed_single(sk: &SigningKey, msg_id: u32, body_bytes: &[u8]) -> Vec<u8> {
        let sender = sk.verifying_key().to_bytes();
        let nonce = 1u64;
        let fee = 100u64;
        let hash = capsule_signing_hash(&sender, nonce, fee, body_bytes);
        let sig = sk.sign(&hash).to_bytes();
        let header = CapsuleHeader {
            sender,
            nonce,
            fee,
            msg_id,
        };
        encode_single(&header, body_bytes, &sig).unwrap()
    }

    fn transfer_body_bytes() -> Vec<u8> {
        use bunker_coin_core::transaction::TransactionBody;
        let body = TransactionBody::Transfer {
            to: [0x02; 32],
            amount: 1000,
        };
        bincode::serde::encode_to_vec(&body, bincode::config::standard()).unwrap()
    }

    #[tokio::test]
    async fn valid_single_relayed() {
        let mut rng = rand::thread_rng();
        let sk = SigningKey::generate(&mut rng);
        let body_bytes = transfer_body_bytes();
        let frame = make_signed_single(&sk, 1, &body_bytes);

        let (tx_relay, mut rx_relay) = mpsc::unbounded_channel();
        let (_user, gw) = crate::simulated::SimulatedLoRaTransport::pair(Default::default());
        let gateway = LoRaGateway::new(Arc::new(gw), make_config(), tx_relay);

        let result = gateway.process_frame(&frame).await;
        assert_eq!(result, GatewayResult::Relayed);

        let tx = rx_relay.try_recv().unwrap();
        assert_eq!(tx.sender, sk.verifying_key().to_bytes());
        assert_eq!(tx.nonce, 1);
        assert_eq!(tx.fee, 100);
    }

    #[tokio::test]
    async fn duplicate_discarded() {
        let mut rng = rand::thread_rng();
        let sk = SigningKey::generate(&mut rng);
        let body_bytes = transfer_body_bytes();
        let frame = make_signed_single(&sk, 1, &body_bytes);

        let (tx_relay, _rx_relay) = mpsc::unbounded_channel();
        let (_user, gw) = crate::simulated::SimulatedLoRaTransport::pair(Default::default());
        let gateway = LoRaGateway::new(Arc::new(gw), make_config(), tx_relay);

        assert_eq!(gateway.process_frame(&frame).await, GatewayResult::Relayed);
        assert_eq!(
            gateway.process_frame(&frame).await,
            GatewayResult::Duplicate
        );
    }

    #[tokio::test]
    async fn rate_limited() {
        let mut rng = rand::thread_rng();
        let sk = SigningKey::generate(&mut rng);
        let body_bytes = transfer_body_bytes();

        let (tx_relay, _rx_relay) = mpsc::unbounded_channel();
        let (_user, gw) = crate::simulated::SimulatedLoRaTransport::pair(Default::default());
        let mut config = make_config();
        config.rate_burst = 1.0; // only 1 allowed
        let gateway = LoRaGateway::new(Arc::new(gw), config, tx_relay);

        let frame1 = make_signed_single(&sk, 1, &body_bytes);
        assert_eq!(gateway.process_frame(&frame1).await, GatewayResult::Relayed);

        let frame2 = make_signed_single(&sk, 2, &body_bytes);
        assert_eq!(
            gateway.process_frame(&frame2).await,
            GatewayResult::RateLimited
        );
    }

    #[tokio::test]
    async fn invalid_signature_rejected() {
        let body_bytes = transfer_body_bytes();
        let sender = [0xAA; 32];
        let header = CapsuleHeader {
            sender,
            nonce: 1,
            fee: 100,
            msg_id: 1,
        };
        let bad_sig = [0xBB; 64];
        let frame = encode_single(&header, &body_bytes, &bad_sig).unwrap();

        let (tx_relay, _rx_relay) = mpsc::unbounded_channel();
        let (_user, gw) = crate::simulated::SimulatedLoRaTransport::pair(Default::default());
        let gateway = LoRaGateway::new(Arc::new(gw), make_config(), tx_relay);

        // The gateway collapses sender-key and signature failures to SignatureInvalid.
        let result = gateway.process_frame(&frame).await;
        assert!(
            result == GatewayResult::SignatureInvalid,
            "expected SignatureInvalid, got {:?}",
            result
        );
    }

    #[tokio::test]
    async fn fragmented_message_relayed() {
        let mut rng = rand::thread_rng();
        let sk = SigningKey::generate(&mut rng);
        let sender = sk.verifying_key().to_bytes();

        // MessageAnchor yields a fragmented body that still deserializes.
        use bunker_coin_core::transaction::TransactionBody;
        let body = TransactionBody::MessageAnchor {
            destination: [0x02; 32],
            length_proof: Box::new([0xAA; 288]),
            deposit: 5_000,
        };
        let body_bytes = bincode::serde::encode_to_vec(&body, bincode::config::standard()).unwrap();

        let nonce = 1u64;
        let fee = 100u64;
        let hash = capsule_signing_hash(&sender, nonce, fee, &body_bytes);
        let sig = sk.sign(&hash).to_bytes();
        let header = CapsuleHeader {
            sender,
            nonce,
            fee,
            msg_id: 42,
        };
        let frames = encode_fragmented(&header, &body_bytes, &sig).unwrap();
        assert!(frames.len() >= 2, "expected fragmentation");

        let (tx_relay, mut rx_relay) = mpsc::unbounded_channel();
        let (_user, gw) = crate::simulated::SimulatedLoRaTransport::pair(Default::default());
        let gateway = LoRaGateway::new(Arc::new(gw), make_config(), tx_relay);

        for (i, frame) in frames.iter().enumerate() {
            let result = gateway.process_frame(frame).await;
            if i < frames.len() - 1 {
                assert_eq!(result, GatewayResult::Reassembling);
            } else {
                assert_eq!(result, GatewayResult::Relayed);
            }
        }

        let tx = rx_relay.try_recv().unwrap();
        assert_eq!(tx.sender, sender);
    }
}
