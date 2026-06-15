// Copyright (c) Anza Technology, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementation of Alpenglow's new Rotor block dissemination protocol.
//!
//! This is an evolution of Solana's original Turbine block dissemination protocol.
//! Instead of a multi-layered tree, it always uses a single layer of relayers.
//!
//! Rotor can be instantiated with any quorum sampling strategy.
//! Therefore, this module also provides multiple implementation of such.
//! See also, the [`sampling_strategy`] module and the [`SamplingStrategy`] trait.
//!
//! For an implementation of Turbine, see [`crate::disseminator::turbine::Turbine`].

pub mod sampling_strategy;

use std::sync::Arc;

use async_trait::async_trait;

use self::sampling_strategy::PartitionSampler;
pub use self::sampling_strategy::{
    FaitAccompli1Sampler, GeoAwareSampler, SamplingStrategy, StakeWeightedSampler,
};
use super::Disseminator;
use crate::consensus::EpochInfo;
use crate::network::{Network, ShredNetwork};
use crate::sherpa::SherpaHandle;
use crate::shredder::{Shred, TOTAL_SHREDS};
use crate::{Slot, ValidatorId};

/// Rotor is a new block dissemination protocol presented together with Alpenglow.
pub struct Rotor<N: Network, S: SamplingStrategy> {
    network: N,
    sampler: S,
    epoch_info: Arc<EpochInfo>,
}

impl<N: Network> Rotor<N, StakeWeightedSampler> {
    /// Creates a new Rotor instance with the default sampling strategy.
    ///
    /// Contact information for all validators is provided in `validators`.
    /// Provided `network` will be used to send and receive shreds.
    pub fn new(network: N, epoch_info: Arc<EpochInfo>) -> Self {
        let validators = epoch_info.validators.clone();
        let sampler = StakeWeightedSampler::new(validators);
        Self {
            network,
            sampler,
            epoch_info,
        }
    }
}

impl<N: Network> Rotor<N, FaitAccompli1Sampler<PartitionSampler>> {
    /// Creates a new Rotor instance with the FA1 sampling strategy.
    ///
    /// Contact information for all validators is provided in `validators`.
    /// Provided `network` will be used to send and receive shreds.
    pub fn new_fa1(network: N, epoch_info: Arc<EpochInfo>) -> Self {
        let validators = epoch_info.validators.clone();
        let sampler =
            FaitAccompli1Sampler::new_with_partition_fallback(validators, TOTAL_SHREDS as u64);
        Self {
            network,
            sampler,
            epoch_info,
        }
    }
}

impl<N: Network> Rotor<N, GeoAwareSampler> {
    /// Creates a new Rotor instance with location-aware (Sherpa) relay sampling.
    ///
    /// Shreds of a slice are assigned round-robin to geographically diverse
    /// relays, so parallel HF links in distinct propagation regions are used
    /// and no single geographic bottleneck carries all shreds (Radiotor §2.4).
    ///
    /// The exclude list is left empty so relay assignment stays identical on
    /// every node; per-node exclusions would make nodes disagree about who
    /// the relay is.
    pub fn new_geo_aware(network: N, epoch_info: Arc<EpochInfo>, sherpa: SherpaHandle) -> Self {
        let validators = epoch_info.validators.clone();
        let sampler = GeoAwareSampler::new(sherpa, validators, TOTAL_SHREDS, Vec::new());
        Self {
            network,
            sampler,
            epoch_info,
        }
    }
}

impl<N, S: SamplingStrategy + Sync> Rotor<N, S>
where
    N: ShredNetwork,
{
    /// Turns this instance into a new instance with a different sampling strategy.
    #[must_use]
    pub fn with_sampler(self, sampler: S) -> Self {
        Self { sampler, ..self }
    }

    /// Sends the shred to the correct relay validator.
    async fn send_as_leader(&self, shred: &Shred) -> std::io::Result<()> {
        let relay = self.sample_relay(shred.payload().slot(), shred.payload().index_in_slot());
        let addr = self.epoch_info.validator(relay).disseminator_address;
        self.network.send(shred, addr).await
    }

    /// Broadcasts a shred to all validators except for the leader and itself.
    /// Does nothing if we are not the dedicated relay for this shred.
    async fn broadcast_if_relay(&self, shred: &Shred) -> std::io::Result<()> {
        let leader = self.epoch_info.leader(shred.payload().header.slot).id;

        let relay = self.sample_relay(shred.payload().header.slot, shred.payload().index_in_slot());
        if self.epoch_info.own_id != relay {
            return Ok(());
        }

        let addrs = self
            .epoch_info
            .validators
            .iter()
            .filter(|v| v.id != leader && v.id != self.epoch_info.own_id)
            .map(|v| v.disseminator_address);
        self.network.send_to_many(shred, addrs).await?;
        Ok(())
    }

    fn sample_relay(&self, slot: Slot, shred: usize) -> ValidatorId {
        self.sampler.sample_shred_relay(slot, shred)
    }
}

#[async_trait]
impl<N, S: SamplingStrategy + Send + Sync + 'static> Disseminator for Rotor<N, S>
where
    N: ShredNetwork,
{
    async fn send(&self, shred: &Shred) -> std::io::Result<()> {
        Self::send_as_leader(self, shred).await
    }

    async fn forward(&self, shred: &Shred) -> std::io::Result<()> {
        Self::broadcast_if_relay(self, shred).await
    }

    async fn receive(&self) -> std::io::Result<Shred> {
        self.network.receive().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::Mutex;
    use tokio::task;

    use super::*;
    use crate::crypto::aggsig;
    use crate::crypto::signature::SecretKey;
    use crate::network::{UdpNetwork, dontcare_sockaddr, localhost_ip_sockaddr};
    use crate::sherpa::Sherpa;
    use crate::shredder::{MAX_DATA_PER_SLICE, RegularShredder, Shredder, TOTAL_SHREDS};
    use crate::types::slice::create_slice_with_invalid_txs;
    use crate::{GeoLocation, ValidatorInfo};

    type MyRotor = Rotor<UdpNetwork<Shred, Shred>, StakeWeightedSampler>;
    type GeoRotor = Rotor<UdpNetwork<Shred, Shred>, GeoAwareSampler>;

    fn create_test_validators(
        count: u64,
        base_port: u16,
        with_locations: bool,
    ) -> (Vec<SecretKey>, Vec<ValidatorInfo>) {
        // locations spread around the globe, reused cyclically
        let locations = [
            (51.5, -0.1),   // London
            (40.7, -74.0),  // New York
            (35.7, 139.7),  // Tokyo
            (-33.9, 151.2), // Sydney
            (-23.5, -46.6), // São Paulo
        ];
        let mut sks = Vec::new();
        let mut voting_sks = Vec::new();
        let mut validators = Vec::new();
        for i in 0..count {
            sks.push(SecretKey::new(&mut rand::rng()));
            voting_sks.push(aggsig::SecretKey::new(&mut rand::rng()));
            let location = with_locations.then(|| {
                let (lat, lon) = locations[i as usize % locations.len()];
                GeoLocation::new(lat, lon)
            });
            validators.push(ValidatorInfo {
                id: i,
                stake: 1,
                pubkey: sks[i as usize].to_pk(),
                voting_pubkey: voting_sks[i as usize].to_pk(),
                all2all_address: dontcare_sockaddr(),
                disseminator_address: localhost_ip_sockaddr(base_port + i as u16),
                repair_request_address: dontcare_sockaddr(),
                repair_response_address: dontcare_sockaddr(),
                location,
            });
        }
        (sks, validators)
    }

    fn create_rotor_instances(count: u64, base_port: u16) -> (Vec<SecretKey>, Vec<MyRotor>) {
        let (sks, validators) = create_test_validators(count, base_port, false);
        let mut rotors = Vec::new();
        for i in 0..count {
            let epoch_info = Arc::new(EpochInfo::new(0, i, validators.clone()));
            let network = UdpNetwork::new(base_port + i as u16);
            rotors.push(Rotor::new(network, epoch_info));
        }
        (sks, rotors)
    }

    fn create_geo_rotor_instances(count: u64, base_port: u16) -> (Vec<SecretKey>, Vec<GeoRotor>) {
        let (sks, validators) = create_test_validators(count, base_port, true);
        let mut rotors = Vec::new();
        for i in 0..count {
            let epoch_info = Arc::new(EpochInfo::new(0, i, validators.clone()));
            let network = UdpNetwork::new(base_port + i as u16);
            let sherpa = Arc::new(Sherpa::new(i, validators.clone()));
            rotors.push(Rotor::new_geo_aware(network, epoch_info, sherpa));
        }
        (sks, rotors)
    }

    async fn run_rotor_dissemination<S: SamplingStrategy + Send + Sync + 'static>(
        sks: Vec<SecretKey>,
        mut rotors: Vec<Rotor<UdpNetwork<Shred, Shred>, S>>,
        count: u64,
    ) {
        let slice = create_slice_with_invalid_txs(MAX_DATA_PER_SLICE);
        let shreds = RegularShredder::default().shred(slice, &sks[0]).unwrap();

        let mut shreds_received = Vec::with_capacity(rotors.len());
        (0..rotors.len()).for_each(|_| shreds_received.push(Arc::new(Mutex::new(HashSet::new()))));
        let mut rotor_tasks = Vec::with_capacity(rotors.len());

        // forward & receive shreds on "non-leader" Rotor instance
        for i in 0..rotors.len() - 1 {
            let shreds_received = shreds_received[i].clone();
            let rotor_non_leader = rotors.pop().unwrap();
            rotor_tasks.push(task::spawn(async move {
                loop {
                    match rotor_non_leader.receive().await {
                        Ok(shred) => {
                            rotor_non_leader.forward(&shred).await.unwrap();
                            let mut guard = shreds_received.lock().await;
                            assert!(!guard.contains(&*shred.payload().shred_index));
                            guard.insert(*shred.payload().shred_index);
                        }
                        _ => continue,
                    }
                }
            }));
        }

        tokio::time::sleep(Duration::from_millis(10)).await;

        assert_eq!(rotors.len(), 1);
        for shred in shreds {
            rotors[0].send(&shred).await.unwrap();
        }

        // forward shreds on the "leader" Rotor instance
        let rotor_leader = rotors.pop().unwrap();
        let rotor_task_leader = task::spawn(async move {
            loop {
                match rotor_leader.receive().await {
                    Ok(shred) => {
                        rotor_leader.forward(&shred).await.unwrap();
                    }
                    _ => continue,
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        // non-leader instances should have received all shreds via Rotor
        for i in 0..(count - 1) {
            assert_eq!(shreds_received[i as usize].lock().await.len(), TOTAL_SHREDS);
        }
        rotor_task_leader.abort();
        for task in rotor_tasks {
            task.abort();
        }
    }

    #[tokio::test]
    async fn two_instances() {
        let (sks, rotors) = create_rotor_instances(2, 3000);
        run_rotor_dissemination(sks, rotors, 2).await
    }

    #[tokio::test]
    async fn many_instances() {
        let (sks, rotors) = create_rotor_instances(10, 3100);
        run_rotor_dissemination(sks, rotors, 10).await
    }

    #[tokio::test]
    async fn geo_aware_instances() {
        let (sks, rotors) = create_geo_rotor_instances(5, 3200);
        run_rotor_dissemination(sks, rotors, 5).await
    }

    #[tokio::test]
    async fn geo_aware_rotor_spreads_relays() {
        let (_sks, validators) = create_test_validators(5, 3300, true);
        let epoch_info = Arc::new(EpochInfo::new(0, 0, validators.clone()));
        let sherpa = Arc::new(Sherpa::new(0, validators));
        let network: UdpNetwork<Shred, Shred> = UdpNetwork::new_with_any_port();
        let rotor = Rotor::new_geo_aware(network, epoch_info, sherpa);

        // consecutive shreds of a slot must be assigned to distinct relays
        let relays: std::collections::HashSet<ValidatorId> = (0..5)
            .map(|shred| rotor.sample_relay(Slot::new(0), shred))
            .collect();
        assert_eq!(relays.len(), 5, "relays not geographically spread");
    }
}
