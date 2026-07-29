// BunkerCoin
// SPDX-License-Identifier: Apache-2.0

//! Location-aware All2All broadcast backed by the Sherpa routing service.
//!
//! [`SherpaAll2All`] wraps any [`ConsensusNetwork`] and replaces the flat
//! broadcast of [`TrivialAll2All`] with a proximity-ordered send loop:
//!
//! 1. Ask Sherpa to order all peers by ascending great-circle distance from us.
//! 2. Skip peers whose links are marked as failed.
//! 3. Send to each peer individually (nearest first).
//! 4. Record success or failure after each send so Sherpa can update its link
//!    state for future routing decisions.
//!
//! This matches the whitepaper requirement (§1.1.1) that Sherpa "allows the
//! system to route data efficiently across the globe" and enables the BLS
//! in-flight aggregation path described in §2.1 (Votor).

use async_trait::async_trait;

use super::All2All;
use crate::ValidatorId;
use crate::consensus::ConsensusMessage;
use crate::network::{ConsensusNetwork, Network};
use crate::sherpa::SherpaHandle;

/// Location-aware all-to-all broadcast using the Sherpa routing service.
///
/// Sends consensus messages (votes, certificates) to peers in order of
/// geographic proximity, skipping links currently marked as failed.
/// Failed links are retried lazily once Sherpa clears them.
pub struct SherpaAll2All<N: Network> {
    network: N,
    sherpa: SherpaHandle,
    own_id: ValidatorId,
}

impl<N: Network> SherpaAll2All<N> {
    /// Creates a new `SherpaAll2All` instance.
    ///
    /// - `network`: the underlying consensus network for sending/receiving.
    /// - `sherpa`: shared Sherpa service (may be shared with Rotor). Peer set
    ///   and ordering come from Sherpa, so no separate validator list is kept.
    pub fn new(own_id: ValidatorId, network: N, sherpa: SherpaHandle) -> Self {
        Self {
            network,
            sherpa,
            own_id,
        }
    }
}

#[async_trait]
impl<N: Network> All2All for SherpaAll2All<N>
where
    N: ConsensusNetwork,
{
    async fn broadcast(&self, msg: &ConsensusMessage) -> std::io::Result<()> {
        // Get peers ordered nearest-first by Sherpa.
        let ordered = self.sherpa.peers_by_proximity(self.own_id);

        for peer in ordered {
            // Skip peers with failed links — they will be retried in future broadcasts
            // once Sherpa clears their link state via record_success.
            if self.sherpa.is_link_failed(self.own_id, peer.id) {
                continue;
            }

            match self.network.send(msg, peer.all2all_address).await {
                Ok(()) => {
                    self.sherpa.record_success(self.own_id, peer.id);
                }
                Err(e) => {
                    self.sherpa.record_failure(self.own_id, peer.id);
                    // Log but don't abort — partial delivery is acceptable for
                    // best-effort broadcast, consistent with TrivialAll2All behaviour.
                    log::warn!(
                        "SherpaAll2All: failed to send to validator {} at {}: {e}",
                        peer.id,
                        peer.all2all_address,
                    );
                }
            }
        }

        Ok(())
    }

    async fn receive(&self) -> std::io::Result<ConsensusMessage> {
        self.network.receive().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::task::JoinSet;

    use super::*;
    use crate::consensus::Vote;
    use crate::crypto::aggsig;
    use crate::crypto::signature::SecretKey;
    use crate::network::simulated::SimulatedNetworkCore;
    use crate::network::{SimulatedNetwork, dontcare_sockaddr, localhost_ip_sockaddr};
    use crate::sherpa::Sherpa;
    use crate::types::Slot;
    use crate::{GeoLocation, ValidatorInfo};

    fn make_validator_with_location(id: u64, lat: f64, lon: f64, stake: u64) -> ValidatorInfo {
        let sk = SecretKey::new(&mut rand::rng());
        let vsk = aggsig::SecretKey::new(&mut rand::rng());
        ValidatorInfo {
            id,
            stake,
            pubkey: sk.to_pk(),
            voting_pubkey: vsk.to_pk(),
            all2all_address: localhost_ip_sockaddr(id.try_into().unwrap()),
            disseminator_address: dontcare_sockaddr(),
            repair_request_address: dontcare_sockaddr(),
            repair_response_address: dontcare_sockaddr(),
            location: Some(GeoLocation::new(lat, lon)),
        }
    }

    #[tokio::test]
    async fn sherpa_all2all_delivers_to_all_peers() {
        let core = Arc::new(
            SimulatedNetworkCore::default()
                .with_default_latency(Duration::from_millis(5))
                .with_packet_loss(0.0),
        );

        let n = 5u64;
        // Spread validators around the globe
        let locations = [
            (51.5, -0.1),
            (48.9, 2.4),
            (40.7, -74.0),
            (35.7, 139.7),
            (-33.9, 151.2),
        ];
        let mut validators = Vec::new();
        for i in 0..n {
            let (lat, lon) = locations[i as usize];
            validators.push(make_validator_with_location(i, lat, lon, 1));
        }

        let sherpa = Arc::new(Sherpa::new(0, validators.clone()));

        // Sender network (node 0)
        let net_sender: SimulatedNetwork<ConsensusMessage, ConsensusMessage> =
            core.join_unlimited(0).await;
        let all2all_sender = SherpaAll2All::new(0, net_sender, sherpa.clone());

        // Receiver networks (nodes 1–4)
        let mut receiver_tasks = JoinSet::new();
        for i in 1..n {
            let net: SimulatedNetwork<ConsensusMessage, ConsensusMessage> =
                core.join_unlimited(i).await;
            let sherpa_recv = Arc::new(Sherpa::new(i, validators.clone()));
            let all2all_recv = SherpaAll2All::new(i, net, sherpa_recv);
            receiver_tasks.spawn(async move {
                let msg = all2all_recv.receive().await.unwrap();
                assert!(matches!(msg, ConsensusMessage::Vote(_)));
            });
        }

        // Broadcast a vote from node 0
        let voting_sk = aggsig::SecretKey::new(&mut rand::rng());
        let vote = Vote::new_skip(Slot::genesis(), &voting_sk, 0);
        all2all_sender
            .broadcast(&ConsensusMessage::Vote(vote))
            .await
            .unwrap();

        receiver_tasks.join_all().await;
    }
}
