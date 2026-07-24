// Copyright (c) Anza Technology, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Sampling strategies for validator and relay selection.
//!
//! Includes uniform, stake-weighted, decaying-acceptance, Turbine, partitioned,
//! Fait Accompli, and geography-aware samplers.

use std::sync::Mutex;

use rand::distr::weighted::WeightedIndex;
use rand::prelude::*;

use crate::disseminator::turbine::DEFAULT_FANOUT;
use crate::sherpa::SherpaHandle;
use crate::{Slot, Stake, ValidatorId, ValidatorInfo};

/// Rejection samplers panic after this many failed attempts.
const MAX_TRIES_PER_SAMPLE: usize = 100_000;

/// Random validator sampler.
pub trait SamplingStrategy {
    /// Samples a validator id.
    ///
    /// # Panics
    /// Panics if the sampler state is invalid or rejection exceeds [`MAX_TRIES_PER_SAMPLE`].
    fn sample<R: RngCore>(&self, rng: &mut R) -> ValidatorId {
        self.sample_info(rng).id
    }

    /// Samples a validator's [`ValidatorInfo`].
    ///
    /// # Panics
    /// Panics if the sampler state is invalid or rejection exceeds [`MAX_TRIES_PER_SAMPLE`].
    fn sample_info<R: RngCore>(&self, rng: &mut R) -> &ValidatorInfo;

    /// Samples `k` validator ids.
    ///
    /// # Panics
    /// Panics if any individual sample panics.
    fn sample_multiple<R: RngCore>(&self, k: usize, rng: &mut R) -> Vec<ValidatorId> {
        (0..k).map(|_| self.sample(rng)).collect()
    }

    /// Deterministically samples the relay for a shred.
    /// All nodes must derive the same relay from the same `(slot, shred_index)`.
    fn sample_shred_relay(&self, slot: Slot, shred_index: usize) -> ValidatorId {
        let seed = [
            slot.inner().to_be_bytes(),
            shred_index.to_be_bytes(),
            [0; 8],
            [0; 8],
        ]
        .concat();
        let mut rng = StdRng::from_seed(seed.try_into().unwrap());
        self.sample(&mut rng)
    }

    /// Returns a printable strategy name.
    fn name() -> &'static str {
        std::any::type_name::<Self>()
    }
}

/// Sampler that always picks the same validator.
#[derive(Clone)]
pub struct AllSameSampler(pub ValidatorInfo);

impl SamplingStrategy for AllSameSampler {
    fn sample<R: RngCore>(&self, _rng: &mut R) -> ValidatorId {
        self.0.id
    }

    fn sample_info<R: RngCore>(&self, _rng: &mut R) -> &ValidatorInfo {
        &self.0
    }

    fn name() -> &'static str {
        "all_same"
    }
}

/// Stateless uniform sampler with replacement.
#[derive(Clone)]
pub struct UniformSampler {
    validators: Vec<ValidatorInfo>,
}

impl UniformSampler {
    pub const fn new(validators: Vec<ValidatorInfo>) -> Self {
        Self { validators }
    }
}

impl SamplingStrategy for UniformSampler {
    fn sample<R: RngCore>(&self, rng: &mut R) -> ValidatorId {
        rng.random_range(0..self.validators.len()) as ValidatorId
    }

    fn sample_info<R: RngCore>(&self, rng: &mut R) -> &ValidatorInfo {
        let index = self.sample(rng) as usize;
        &self.validators[index]
    }

    fn name() -> &'static str {
        "uniform"
    }
}

/// Stateless stake-weighted sampler with replacement.
#[derive(Clone)]
pub struct StakeWeightedSampler {
    validators: Vec<ValidatorInfo>,
    stake_index: WeightedIndex<u64>,
}

impl StakeWeightedSampler {
    /// Creates a stake-weighted sampler.
    pub fn new(validators: Vec<ValidatorInfo>) -> Self {
        let stakes: Vec<Stake> = validators.iter().map(|v| v.stake).collect();
        let stake_index = WeightedIndex::new(&stakes).unwrap();
        Self {
            validators,
            stake_index,
        }
    }
}

impl SamplingStrategy for StakeWeightedSampler {
    fn sample<R: RngCore>(&self, rng: &mut R) -> ValidatorId {
        self.stake_index.sample(rng) as ValidatorId
    }

    fn sample_info<R: RngCore>(&self, rng: &mut R) -> &ValidatorInfo {
        let index = self.sample(rng) as usize;
        &self.validators[index]
    }

    fn name() -> &'static str {
        "stake_weighted"
    }
}

/// Stake-weighted sampler with rejection probability rising per prior sample.
/// `max_samples = 1` approximates without replacement; infinity permits replacement.
pub struct DecayingAcceptanceSampler {
    stake_weighted: StakeWeightedSampler,
    max_samples: f64,
    sample_count: Mutex<Vec<usize>>,
}

impl DecayingAcceptanceSampler {
    /// Creates a decaying-acceptance sampler.
    pub fn new(validators: Vec<ValidatorInfo>, max_samples: f64) -> Self {
        let sample_count = vec![0; validators.len()];
        Self {
            stake_weighted: StakeWeightedSampler::new(validators),
            max_samples,
            sample_count: Mutex::new(sample_count),
        }
    }

    /// Resets the stateful sample counters.
    pub fn reset(&self) {
        let mut sample_count = self.sample_count.lock().unwrap();
        *sample_count = vec![0; self.stake_weighted.validators.len()];
    }
}

impl SamplingStrategy for DecayingAcceptanceSampler {
    /// Samples a validator id.
    ///
    /// # Panics
    /// Panics if rejection exceeds [`MAX_TRIES_PER_SAMPLE`].
    fn sample<R: RngCore>(&self, rng: &mut R) -> ValidatorId {
        for _ in 0..MAX_TRIES_PER_SAMPLE {
            let sample = self.stake_weighted.sample(rng);
            let mut sample_count = self.sample_count.lock().unwrap();
            let p_reject = sample_count[sample as usize] as f64 / self.max_samples;
            if rng.random::<f64>() >= p_reject {
                sample_count[sample as usize] += 1;
                return sample;
            }
        }

        panic!("rejected all {MAX_TRIES_PER_SAMPLE} samples");
    }

    fn sample_info<R: RngCore>(&self, rng: &mut R) -> &ValidatorInfo {
        let index = self.sample(rng) as usize;
        &self.stake_weighted.validators[index]
    }

    fn sample_multiple<R: RngCore>(&self, k: usize, rng: &mut R) -> Vec<ValidatorId> {
        let samples = (0..k).map(|_| self.sample(rng)).collect();
        self.reset();
        samples
    }

    fn name() -> &'static str {
        "decaying_acceptance"
    }
}

impl Clone for DecayingAcceptanceSampler {
    fn clone(&self) -> Self {
        Self {
            stake_weighted: self.stake_weighted.clone(),
            max_samples: self.max_samples,
            sample_count: Mutex::new(self.sample_count.lock().unwrap().clone()),
        }
    }
}

/// Rotor sampler that approximates Turbine relay workload distribution.
/// No validator should be sampled above `fanout / validators` probability.
#[derive(Clone)]
pub struct TurbineSampler {
    fanout: usize,
    stake_weighted: StakeWeightedSampler,
}

impl TurbineSampler {
    /// Creates a sampler for the default [`Turbine`](crate::disseminator::turbine::Turbine) fanout.
    pub fn new(validators: Vec<ValidatorInfo>) -> Self {
        Self::new_with_fanout(validators, DEFAULT_FANOUT)
    }

    /// Creates a sampler for a specific Turbine fanout.
    // Models two Turbine levels.
    #[must_use]
    pub fn new_with_fanout(mut validators: Vec<ValidatorInfo>, turbine_fanout: usize) -> Self {
        let total_stake: Stake = validators.iter().map(|v| v.stake).sum();

        // Estimate each validator's relay work, excluding leader work.
        let mut expected_work = vec![0.0; validators.len()];
        let validators_left = validators.len() - 1;
        for leader in &validators {
            let prob = leader.stake as f64 / total_stake as f64;
            let stake_left = total_stake - leader.stake;
            let validators_left = validators_left - 1;
            for root in &validators {
                if root.id == leader.id {
                    continue;
                }
                let prob = prob * root.stake as f64 / stake_left as f64;
                let root_work = (turbine_fanout as f64).min(validators_left as f64);
                expected_work[root.id as usize] += prob * root_work;
                let stake_left = stake_left - root.stake;
                let validators_left = validators_left.saturating_sub(turbine_fanout);
                for maybe_level1 in &validators {
                    if maybe_level1.id == leader.id || maybe_level1.id == root.id {
                        continue;
                    }
                    let select_prob = maybe_level1.stake as f64 / stake_left as f64;
                    let full_level1_slots = validators_left / turbine_fanout;
                    let prob_full =
                        prob * (1.0 - (1.0 - select_prob).powi(full_level1_slots as i32));
                    let full_level1_work = turbine_fanout as f64;
                    expected_work[maybe_level1.id as usize] += prob_full * full_level1_work;
                    let prob_partial =
                        prob * (1.0 - select_prob).powi(full_level1_slots as i32) * select_prob;
                    let partial_level1_work = (validators_left % turbine_fanout) as f64;
                    expected_work[maybe_level1.id as usize] += prob_partial * partial_level1_work;
                }
            }
        }

        for (i, w) in expected_work.into_iter().enumerate() {
            validators[i].stake = (w * 1_000_000_000.0) as Stake;
        }

        Self {
            fanout: turbine_fanout,
            stake_weighted: StakeWeightedSampler::new(validators),
        }
    }
}

impl SamplingStrategy for TurbineSampler {
    /// Samples a validator id.
    ///
    /// # Panics
    /// Panics if rejection exceeds [`MAX_TRIES_PER_SAMPLE`].
    fn sample<R: RngCore>(&self, rng: &mut R) -> ValidatorId {
        let n = self.stake_weighted.validators.len();
        let root = self.stake_weighted.sample(rng);
        if rng.random::<f64>() < self.fanout as f64 / n as f64 {
            root
        } else {
            for _ in 0..MAX_TRIES_PER_SAMPLE {
                let sample = self.stake_weighted.sample(rng);
                if sample != root {
                    return sample;
                }
            }
            panic!("rejected all {MAX_TRIES_PER_SAMPLE} samples");
        }
    }

    fn sample_info<R: RngCore>(&self, rng: &mut R) -> &ValidatorInfo {
        let index = self.sample(rng) as usize;
        &self.stake_weighted.validators[index]
    }

    fn name() -> &'static str {
        "turbine"
    }
}

/// Reduced-variance stake sampler that draws one validator from each stake bin.
/// Validators below one bin of stake appear in at most two bins.
#[derive(Clone)]
pub struct PartitionSampler {
    validators: Vec<ValidatorInfo>,
    bins: Vec<WeightedIndex<u64>>,
    pub bin_validators: Vec<Vec<ValidatorId>>,
    pub bin_stakes: Vec<Vec<Stake>>,
}

impl PartitionSampler {
    /// Creates a partition sampler by randomly splitting validators into equal-stake bins.
    pub fn new(validators: Vec<ValidatorInfo>, num_bins: usize) -> Self {
        if num_bins == 0 {
            return Self {
                validators,
                bins: Vec::new(),
                bin_validators: Vec::new(),
                bin_stakes: Vec::new(),
            };
        }

        let mut bin_validators = vec![Vec::new(); num_bins];
        let mut bin_stakes = vec![Vec::new(); num_bins];

        let total_stake: Stake = validators.iter().map(|v| v.stake).sum();
        let stake_per_bin = total_stake.div_ceil(num_bins as Stake);
        let mut validators_random = validators.clone();
        validators_random.shuffle(&mut rand::rng());

        let mut current_bin = 0;
        let mut current_bin_stake = 0;
        for v in validators_random {
            let mut stake = v.stake;
            while stake > 0 {
                bin_validators[current_bin].push(v.id);
                let stake_to_take = stake.min(stake_per_bin - current_bin_stake);
                current_bin_stake += stake_to_take;
                bin_stakes[current_bin].push(stake_to_take);
                stake -= stake_to_take;
                if current_bin < num_bins - 1 && (stake > 0 || current_bin_stake == stake_per_bin) {
                    current_bin += 1;
                    current_bin_stake = 0;
                }
            }
        }

        let mut bins = Vec::with_capacity(num_bins);
        for stakes in &bin_stakes {
            let bin = WeightedIndex::new(stakes).unwrap();
            bins.push(bin);
        }

        Self {
            validators,
            bins,
            bin_validators,
            bin_stakes,
        }
    }
}

impl SamplingStrategy for PartitionSampler {
    fn sample<R: RngCore>(&self, rng: &mut R) -> ValidatorId {
        rng.random_range(0..self.validators.len()) as ValidatorId
    }

    fn sample_info<R: RngCore>(&self, rng: &mut R) -> &ValidatorInfo {
        let index = self.sample(rng) as usize;
        &self.validators[index]
    }

    fn sample_multiple<R: RngCore>(&self, _k: usize, rng: &mut R) -> Vec<ValidatorId> {
        let mut samples = Vec::new();
        for (bin, validators) in self.bins.iter().zip(self.bin_validators.iter()) {
            let i = bin.sample(rng);
            samples.push(validators[i]);
        }
        samples
    }

    fn name() -> &'static str {
        "partition"
    }
}

/// FA1-F committee sampler: deterministic high-stake picks plus fallback.
/// Remaining samples draw from `F` using residual stake weights.
/// See also: <https://dl.acm.org/doi/pdf/10.1145/3576915.3623194>
pub struct FaitAccompli1Sampler<F: SamplingStrategy> {
    validators: Vec<ValidatorInfo>,
    required_samples: Vec<ValidatorId>,
    pub fallback_sampler: F,
}

impl FaitAccompli1Sampler<PartitionSampler> {
    /// Creates an FA1-F sampler with a partition fallback.
    #[must_use]
    pub fn new_with_partition_fallback(validators: Vec<ValidatorInfo>, k: u64) -> Self {
        let total_stake: Stake = validators.iter().map(|v| v.stake).sum();
        let mut required_samples = Vec::new();
        let mut validators_truncated_stake = validators.clone();
        for v in &mut validators_truncated_stake {
            let frac_stake = v.stake as f64 / total_stake as f64;
            let samples = (frac_stake * k as f64).floor() as u64;
            v.stake -= samples * total_stake / k;
            required_samples.extend((0..samples).map(|_| v.id));
        }
        let all_zero = validators_truncated_stake.iter().all(|v| v.stake == 0);
        let k_prime = k as usize - required_samples.len();
        let fallback_sampler = if all_zero {
            PartitionSampler::new(validators.clone(), k_prime)
        } else {
            PartitionSampler::new(validators_truncated_stake, k_prime)
        };
        Self {
            validators,
            required_samples,
            fallback_sampler,
        }
    }
}

impl FaitAccompli1Sampler<StakeWeightedSampler> {
    /// Creates an FA1-F sampler with IID stake-weighted fallback.
    #[must_use]
    pub fn new_with_stake_weighted_fallback(validators: Vec<ValidatorInfo>, k: u64) -> Self {
        let total_stake: Stake = validators.iter().map(|v| v.stake).sum();
        let mut required_samples = Vec::new();
        let mut validators_truncated_stake = validators.clone();
        for v in &mut validators_truncated_stake {
            let frac_stake = v.stake as f64 / total_stake as f64;
            let samples = (frac_stake * k as f64).floor() as u64;
            v.stake -= samples * total_stake / k;
            required_samples.extend((0..samples).map(|_| v.id));
        }
        let all_zero = validators_truncated_stake.iter().all(|v| v.stake == 0);
        let fallback_sampler = if all_zero {
            StakeWeightedSampler::new(validators.clone())
        } else {
            StakeWeightedSampler::new(validators_truncated_stake)
        };
        Self {
            validators,
            required_samples,
            fallback_sampler,
        }
    }
}

impl<F: SamplingStrategy> SamplingStrategy for FaitAccompli1Sampler<F> {
    fn sample<R: RngCore>(&self, rng: &mut R) -> ValidatorId {
        rng.random_range(0..self.validators.len()) as ValidatorId
    }

    fn sample_info<R: RngCore>(&self, rng: &mut R) -> &ValidatorInfo {
        let index = self.sample(rng) as usize;
        &self.validators[index]
    }

    fn sample_multiple<R: RngCore>(&self, k: usize, rng: &mut R) -> Vec<ValidatorId> {
        let mut validators = Vec::with_capacity(k);
        validators.extend_from_slice(&self.required_samples);
        if validators.len() < k {
            let k_prime = k - validators.len();
            let additional_samples = self.fallback_sampler.sample_multiple(k_prime, rng);
            validators.extend_from_slice(&additional_samples);
        }
        validators
    }

    fn name() -> &'static str {
        if F::name() == "stake_weighted" {
            "fa1_iid"
        } else if F::name() == "partition" {
            "fa1_partition"
        } else {
            "fa1"
        }
    }
}

impl<F: SamplingStrategy + Clone> Clone for FaitAccompli1Sampler<F> {
    fn clone(&self) -> Self {
        Self {
            validators: self.validators.clone(),
            required_samples: self.required_samples.clone(),
            fallback_sampler: self.fallback_sampler.clone(),
        }
    }
}

/// FA2 committee sampler.
/// See also: <https://dl.acm.org/doi/pdf/10.1145/3576915.3623194>
pub struct FaitAccompli2Sampler {
    validators: Vec<ValidatorInfo>,
    required_samples: Vec<ValidatorId>,
    medium_nodes: Vec<(ValidatorId, f64)>,
    fallback_sampler: StakeWeightedSampler,
}

impl FaitAccompli2Sampler {
    /// Creates an FA2 sampler for a fixed sample count `k`.
    pub fn new(validators: Vec<ValidatorInfo>, k: u64) -> Self {
        // FA1 deterministic samples.
        let total_stake: Stake = validators.iter().map(|v| v.stake).sum();
        let mut required_samples = Vec::new();
        for v in &validators {
            let frac_stake = v.stake as f64 / total_stake as f64;
            let samples = (frac_stake * k as f64).floor() as u64;
            required_samples.extend((0..samples).map(|_| v.id));
        }

        // FA2 medium-node probabilities.
        let f = Self::minimize_f(&validators, k);
        let mut medium_nodes = Vec::new();
        for (i, fi) in f.iter().enumerate() {
            let rel_stake = validators[i].stake as f64 / total_stake as f64;
            if *fi > rel_stake {
                let p = 1.0 - (fi - rel_stake) * k as f64;
                medium_nodes.push((i as ValidatorId, p));
            }
        }

        // Residual stake distribution for IID fallback.
        let r: f64 = validators
            .iter()
            .enumerate()
            .filter(|(i, v)| v.stake as f64 / total_stake as f64 > f[*i])
            .map(|(i, v)| v.stake as f64 / total_stake as f64 - f[i])
            .sum();
        let new_stake_distribution: Vec<ValidatorInfo> = validators
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, mut v)| {
                if v.stake as f64 / total_stake as f64 > f[i] {
                    v.stake = ((v.stake as f64 / total_stake as f64 - f[i]) / r
                        * total_stake as f64) as Stake;
                } else {
                    v.stake = 0;
                }
                v
            })
            .collect();
        let fallback_sampler = if r == 0.0 {
            StakeWeightedSampler::new(validators.clone())
        } else {
            StakeWeightedSampler::new(new_stake_distribution)
        };

        Self {
            validators,
            required_samples,
            medium_nodes,
            fallback_sampler,
        }
    }

    fn minimize_f(validators: &[ValidatorInfo], k: u64) -> Vec<f64> {
        let total_stake: Stake = validators.iter().map(|v| v.stake).sum();
        let f: Vec<f64> = validators
            .iter()
            .map(|v| (v.stake as f64 / total_stake as f64 * k as f64).round() / k as f64)
            .collect();
        assert!(f.iter().sum::<f64>() <= 1.0);
        f
    }
}

impl SamplingStrategy for FaitAccompli2Sampler {
    fn sample<R: RngCore>(&self, rng: &mut R) -> ValidatorId {
        rng.random_range(0..self.validators.len()) as ValidatorId
    }

    fn sample_info<R: RngCore>(&self, rng: &mut R) -> &ValidatorInfo {
        let index = self.sample(rng) as usize;
        &self.validators[index]
    }

    fn sample_multiple<R: RngCore>(&self, k: usize, rng: &mut R) -> Vec<ValidatorId> {
        // Required FA1 samples.
        let mut validators = Vec::with_capacity(k);
        validators.extend_from_slice(&self.required_samples);

        // FA2 medium-node Bernoulli samples.
        for (validator, probability) in &self.medium_nodes {
            if rng.random_bool(*probability) {
                validators.push(*validator);
            }
        }

        // Remaining slots use IID stake-weighted fallback.
        if validators.len() < k {
            let k_prime = k - validators.len();
            let additional_samples = self.fallback_sampler.sample_multiple(k_prime, rng);
            validators.extend_from_slice(&additional_samples);
        }

        validators
    }

    fn name() -> &'static str {
        "fa2"
    }
}

impl Clone for FaitAccompli2Sampler {
    fn clone(&self) -> Self {
        Self {
            validators: self.validators.clone(),
            required_samples: self.required_samples.clone(),
            medium_nodes: self.medium_nodes.clone(),
            fallback_sampler: self.fallback_sampler.clone(),
        }
    }
}

/// Relay sampler that favors geographically diverse Sherpa candidates.
/// Falls back to stake-weighted sampling when location data is insufficient.
pub struct GeoAwareSampler {
    sherpa: SherpaHandle,
    top_k: usize,
    exclude: Vec<ValidatorId>,
    fallback: StakeWeightedSampler,
    validators: Vec<ValidatorInfo>,
}

impl GeoAwareSampler {
    /// Creates a geography-aware sampler.
    pub fn new(
        sherpa: SherpaHandle,
        validators: Vec<ValidatorInfo>,
        top_k: usize,
        exclude: Vec<ValidatorId>,
    ) -> Self {
        let fallback = StakeWeightedSampler::new(validators.clone());
        Self {
            sherpa,
            top_k,
            exclude,
            fallback,
            validators,
        }
    }
}

impl SamplingStrategy for GeoAwareSampler {
    fn sample_info<R: RngCore>(&self, rng: &mut R) -> &ValidatorInfo {
        let candidates = self.sherpa.diverse_relays(self.top_k, &self.exclude);

        if candidates.is_empty() {
            return self.fallback.sample_info(rng);
        }

        // Stake-weighted sample among diverse candidates.
        let stakes: Vec<Stake> = candidates.iter().map(|v| v.stake.max(1)).collect();
        match WeightedIndex::new(&stakes) {
            Ok(dist) => {
                let idx = dist.sample(rng);
                let chosen_id = candidates[idx].id;
                // Return from `validators` so the reference has `self` lifetime.
                self.validators
                    .iter()
                    .find(|v| v.id == chosen_id)
                    .unwrap_or_else(|| self.fallback.sample_info(rng))
            }
            Err(_) => self.fallback.sample_info(rng),
        }
    }

    /// Assigns relays round-robin over Sherpa's diverse candidate order.
    /// Determinism is required for agreement; libm tie drift affects liveness only.
    fn sample_shred_relay(&self, slot: Slot, shred_index: usize) -> ValidatorId {
        let candidates = self.sherpa.diverse_relays(self.top_k, &self.exclude);

        if candidates.is_empty() {
            // Degenerate all-excluded case: seeded stake-weighted draw.
            let seed = [
                slot.inner().to_be_bytes(),
                shred_index.to_be_bytes(),
                [0; 8],
                [0; 8],
            ]
            .concat();
            let mut rng = StdRng::from_seed(seed.try_into().unwrap());
            return self.fallback.sample(&mut rng);
        }

        // Offset by slot so relay duty rotates across slots.
        let idx = (slot.inner() as usize).wrapping_add(shred_index) % candidates.len();
        candidates[idx].id
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::ValidatorId;
    use crate::crypto::aggsig;
    use crate::crypto::signature::SecretKey;
    use crate::disseminator::turbine::WeightedShuffle;
    use crate::network::dontcare_sockaddr;
    use crate::network::simulated::stake_distribution::{VALIDATOR_DATA, ValidatorData};
    use crate::shredder::TOTAL_SHREDS;

    fn create_validator_info(count: ValidatorId) -> Vec<ValidatorInfo> {
        let mut validators = Vec::new();
        for i in 0..count {
            let sk = SecretKey::new(&mut rand::rng());
            let voting_sk = aggsig::SecretKey::new(&mut rand::rng());
            validators.push(ValidatorInfo {
                id: i,
                stake: 1,
                pubkey: sk.to_pk(),
                voting_pubkey: voting_sk.to_pk(),
                all2all_address: dontcare_sockaddr(),
                disseminator_address: dontcare_sockaddr(),
                repair_request_address: dontcare_sockaddr(),
                repair_response_address: dontcare_sockaddr(),
                location: None,
            });
        }
        validators
    }

    #[test]
    fn all_same_sampler() {
        let validators = create_validator_info(10);
        let sampler = AllSameSampler(validators[3].clone());
        let mut rng = rand::rng();
        for _ in 0..1000 {
            assert_eq!(sampler.sample(&mut rng), 3);
            assert_eq!(sampler.sample_info(&mut rng).id, 3);
        }

        for _ in 0..10 {
            let sampled_vals = sampler.sample_multiple(TOTAL_SHREDS, &mut rng);
            for val in sampled_vals {
                assert_eq!(val, 3);
            }
        }
    }

    #[test]
    fn uniform_sampler() {
        let validators = create_validator_info(1000);
        let sampler = UniformSampler::new(validators);
        let sampled = sampler.sample_multiple(1000, &mut rand::rng());
        let sampled_set: HashSet<_> = sampled.iter().collect();
        assert!(sampled_set.len() > 500 && sampled_set.len() < 750);
        let max_appearances = sampled_set
            .iter()
            .map(|i| sampled.iter().filter(|v| *v == *i).count())
            .max()
            .unwrap();
        assert!(max_appearances > 1);
        assert!(max_appearances < 17);

        let mut validators = create_validator_info(1000);
        validators[0].stake = 1_000_000_000;
        let sampler = UniformSampler::new(validators);
        let sampled = sampler.sample_multiple(1000, &mut rand::rng());
        let sampled_set: HashSet<_> = sampled.iter().collect();
        assert!(sampled_set.len() > 500 && sampled_set.len() < 750);
        let max_appearances = sampled_set
            .iter()
            .map(|i| sampled.iter().filter(|v| *v == *i).count())
            .max()
            .unwrap();
        assert!(max_appearances > 1);
        assert!(max_appearances < 17);

        let mut validators = create_validator_info(1000);
        for i in (0..validators.len()).step_by(2) {
            validators[i].stake = 1_000_000_000;
        }
        let sampler = UniformSampler::new(validators);
        let sampled = sampler.sample_multiple(1000, &mut rand::rng());
        let sampled_set: HashSet<_> = sampled.iter().collect();
        assert!(sampled_set.len() > 500 && sampled_set.len() < 750);
        let max_appearances = sampled_set
            .iter()
            .map(|i| sampled.iter().filter(|v| *v == *i).count())
            .max()
            .unwrap();
        assert!(max_appearances > 1);
        assert!(max_appearances < 17);
    }

    #[test]
    fn stake_weighted_sampler() {
        let validators = create_validator_info(1000);
        let sampler = StakeWeightedSampler::new(validators);
        let sampled = sampler.sample_multiple(1000, &mut rand::rng());
        let sampled_set: HashSet<_> = sampled.iter().collect();
        assert!(sampled_set.len() > 500 && sampled_set.len() < 750);
        let max_appearances = sampled_set
            .iter()
            .map(|i| sampled.iter().filter(|v| *v == *i).count())
            .max()
            .unwrap();
        assert!(max_appearances > 1);
        assert!(max_appearances < 17);

        let mut validators = create_validator_info(100);
        validators[0].stake = 1_000_000_000;
        let sampler = StakeWeightedSampler::new(validators);
        assert_eq!(sampler.sample(&mut rand::rng()), 0);
        let sampled = sampler.sample_multiple(100, &mut rand::rng());
        let sampled0 = sampled.into_iter().filter(|v| *v == 0).count();
        assert!(sampled0 == 100);
    }

    #[test]
    fn decaying_acceptance_sampler() {
        let validators = create_validator_info(100);
        let sampler = DecayingAcceptanceSampler::new(validators, 1.0);
        let sampled = sampler.sample_multiple(100, &mut rand::rng());
        let sampled_set: HashSet<_> = sampled.iter().copied().collect();
        assert_eq!(sampled_set.len(), 100);

        let mut validators = create_validator_info(100);
        validators[0].stake = 10_000;
        let sampler = DecayingAcceptanceSampler::new(validators, 5.0);
        let sampled = sampler.sample_multiple(100, &mut rand::rng());
        let sampled0 = sampled.into_iter().filter(|v| *v == 0).count();
        assert!(sampled0 <= 5);

        let mut validators = create_validator_info(100);
        validators[0].stake = 1_000_000_000;
        let sampler = DecayingAcceptanceSampler::new(validators, f64::INFINITY);
        assert_eq!(sampler.sample(&mut rand::rng()), 0);
        let sampled = sampler.sample_multiple(100, &mut rand::rng());
        let sampled0 = sampled.into_iter().filter(|v| *v == 0).count();
        assert_eq!(sampled0, 100);

        let mut sampler = sampler.clone();
        sampler.max_samples = 5.0;
        for _ in 0..100 {
            sampler.reset();
            let id = sampler.sample(&mut rand::rng());
            assert_eq!(id, 0);
        }
    }

    #[test]
    #[ignore]
    fn turbine_sampler() {
        const SLICES: usize = 100_000;

        let mut rng = rand::rng();
        let mut validators = create_validator_info(1000);
        validators[0].stake = 55;
        validators[1].stake = 55;
        let total_stake = validators.len() as u64 - 2 + validators[0].stake + validators[1].stake;

        let sampler = TurbineSampler::new(validators.clone());
        let sampled = sampler.sample_multiple(TOTAL_SHREDS * SLICES, &mut rng);
        let appearances0 = sampled.iter().filter(|v| **v == 0).count();
        let appearances1 = sampled.iter().filter(|v| **v == 1).count();
        let work0 = ((TOTAL_SHREDS * SLICES) as u64 * validators[0].stake / total_stake)
            + (appearances0 * (validators.len() - 2)) as u64;
        let work1 = ((TOTAL_SHREDS * SLICES) as u64 * validators[1].stake / total_stake)
            + (appearances1 * (validators.len() - 2)) as u64;

        let mut turbine_work = [0, 0];
        let mut rng = SmallRng::from_rng(&mut rand::rng());
        for _ in 0..TOTAL_SHREDS * SLICES {
            let mut weighted_shuffle = WeightedShuffle::new(validators.iter().map(|v| v.stake));
            let mut validator_ids = weighted_shuffle.shuffle(&mut rng).map(|i| i as ValidatorId);

            let leader = validator_ids.next().unwrap();
            if leader == 0 || leader == 1 {
                turbine_work[leader as usize] += 1;
            }
            assert!(validators.len() > DEFAULT_FANOUT + 2);
            let root = validator_ids.next().unwrap();
            if root == 0 || root == 1 {
                turbine_work[root as usize] += DEFAULT_FANOUT;
            }
            let mut validators_left = validators.len() - 2 - DEFAULT_FANOUT;
            for _ in 0..DEFAULT_FANOUT {
                let parent = validator_ids.next().unwrap() as usize;
                if parent == 0 || parent == 1 {
                    let work = DEFAULT_FANOUT.min(validators_left);
                    turbine_work[parent as usize] += work;
                }
                if validators_left <= DEFAULT_FANOUT {
                    break;
                }
                validators_left -= DEFAULT_FANOUT;
            }
        }

        const TOLERANCE: f64 = 0.05;
        let rel_workload0 = turbine_work[0] as f64 / work0 as f64;
        println!("{rel_workload0}");
        assert!(rel_workload0 > 1.0 - TOLERANCE);
        assert!(rel_workload0 < 1.0 + TOLERANCE);
        let rel_workload1 = turbine_work[1] as f64 / work1 as f64;
        println!("{rel_workload1}");
        assert!(rel_workload1 > 1.0 - TOLERANCE);
        assert!(rel_workload1 < 1.0 + TOLERANCE);
    }

    #[test]
    #[ignore]
    fn turbine_sampler_real_world() {
        const SLICES: usize = 100_000;

        let stakes = VALIDATOR_DATA
            .iter()
            .filter_map(ValidatorData::active_stake)
            .collect::<Vec<_>>();
        let total_stake: Stake = stakes.iter().sum();
        let mut validators = create_validator_info(stakes.len() as ValidatorId);
        for (i, stake) in stakes.into_iter().enumerate() {
            validators[i].stake = stake;
        }

        let mut rng = SmallRng::from_rng(&mut rand::rng());
        let sampler = TurbineSampler::new(validators.clone());
        let mut expected_work = vec![0; validators.len()];
        let relays = sampler.sample_multiple(TOTAL_SHREDS * SLICES, &mut rng);
        for (v, stake) in validators.iter().map(|v| v.stake).enumerate() {
            let appearances = relays
                .iter()
                .filter(|val| **val == v as ValidatorId)
                .count();
            let fractional_stake = stake as f64 / total_stake as f64;
            let leader_work = ((TOTAL_SHREDS * SLICES) as f64 * fractional_stake) as u64;
            let relay_work = (appearances * (validators.len() - 2)) as u64;
            expected_work[v] = leader_work + relay_work;
        }

        let mut turbine_workload = vec![0; validators.len()];
        for _ in 0..TOTAL_SHREDS * SLICES {
            let mut weighted_shuffle = WeightedShuffle::new(validators.iter().map(|v| v.stake));
            let mut validator_ids = weighted_shuffle.shuffle(&mut rng).map(|i| i as ValidatorId);

            let leader = validator_ids.next().unwrap();
            turbine_workload[leader as usize] += 1;
            assert!(validators.len() > DEFAULT_FANOUT + 2);
            let root = validator_ids.next().unwrap();
            turbine_workload[root as usize] += DEFAULT_FANOUT;
            let mut validators_left = validators.len() - 2 - DEFAULT_FANOUT;
            for _ in 0..DEFAULT_FANOUT {
                let parent = validator_ids.next().unwrap() as usize;
                turbine_workload[parent] += DEFAULT_FANOUT.min(validators_left);
                if validators_left < DEFAULT_FANOUT {
                    break;
                }
                validators_left -= DEFAULT_FANOUT;
            }
        }

        const TOLERANCE: f64 = 0.05;
        for (tw, sw) in turbine_workload.into_iter().zip(expected_work) {
            if tw as f64 / (TOTAL_SHREDS * SLICES * (validators.len() - 1)) as f64 <= 0.001 {
                continue;
            }
            let rel_workload = tw as f64 / sw as f64;
            assert!(rel_workload > 1.0 - TOLERANCE);
            assert!(rel_workload < 1.0 + TOLERANCE);
        }
    }

    #[test]
    fn partition_sampler() {
        let validators = create_validator_info(64);
        let sampler = PartitionSampler::new(validators, 64);
        let sampled = sampler.sample_multiple(64, &mut rand::rng());
        assert_eq!(sampled.len(), 64);
        let sampled: HashSet<_> = sampled.into_iter().collect();
        assert_eq!(sampled.len(), 64);
        for id in 0..64 {
            assert!(sampled.contains(&id));
        }
    }

    #[test]
    fn fa1_sampler() {
        let validators = create_validator_info(64);
        let sampler = FaitAccompli1Sampler::new_with_stake_weighted_fallback(validators, 64);
        let sampled = sampler.sample_multiple(64, &mut rand::rng());
        assert_eq!(sampled.len(), 64);
        let sampled: HashSet<_> = sampled.into_iter().collect();
        assert_eq!(sampled.len(), 64);
        for id in 0..64 {
            assert!(sampled.contains(&id));
        }

        let validators = create_validator_info(64);
        let sampler = FaitAccompli1Sampler::new_with_partition_fallback(validators, 64);
        let sampled = sampler.sample_multiple(64, &mut rand::rng());
        assert_eq!(sampled.len(), 64);
        let sampled: HashSet<_> = sampled.into_iter().collect();
        assert_eq!(sampled.len(), 64);
        for id in 0..64 {
            assert!(sampled.contains(&id));
        }

        let mut avg_max_appearances = 0.0;
        for _ in 0..20 {
            let validators = create_validator_info(1000);
            let sampler = FaitAccompli1Sampler::new_with_stake_weighted_fallback(validators, 64);
            let sampled = sampler.sample_multiple(64, &mut rand::rng());
            assert_eq!(sampled.len(), 64);
            let sampled_set = sampled.iter().collect::<HashSet<_>>();
            let max_appearances = sampled_set
                .iter()
                .map(|i| sampled.iter().filter(|v| *v == *i).count())
                .max()
                .unwrap();
            avg_max_appearances += max_appearances as f64 / 20.0;
        }
        assert!(avg_max_appearances >= 1.0);
        assert!(avg_max_appearances < 3.0);

        let mut validators = create_validator_info(1000);
        validators[0].stake = 52;
        validators[1].stake = 52;
        let sampler = FaitAccompli1Sampler::new_with_stake_weighted_fallback(validators, 64);
        let sampled = sampler.sample_multiple(64, &mut rand::rng());
        assert_eq!(sampled.len(), 64);
        let sampled0 = sampled.iter().filter(|v| **v == 0).count();
        let sampled1 = sampled.iter().filter(|v| **v == 1).count();
        assert!(sampled0 >= 3);
        assert!(sampled1 >= 3);
    }

    #[test]
    fn fa2_sampler() {
        let validators = create_validator_info(64);
        let sampler = FaitAccompli2Sampler::new(validators, 64);
        let sampled = sampler.sample_multiple(64, &mut rand::rng());
        assert_eq!(sampled.len(), 64);
        let sampled: HashSet<_> = sampled.into_iter().collect();
        assert_eq!(sampled.len(), 64);
        for id in 0..64 {
            assert!(sampled.contains(&id));
        }
    }

    #[test]
    fn completeness() {
        let validators = create_validator_info(10);
        sample_all_validators(&UniformSampler::new(validators.clone()));
        sample_all_validators(&StakeWeightedSampler::new(validators.clone()));
        sample_all_validators(&DecayingAcceptanceSampler::new(validators.clone(), 1000.0));
        sample_all_validators(&TurbineSampler::new(validators.clone()));
        sample_all_validators(&PartitionSampler::new(validators.clone(), 10));
        sample_all_validators(&FaitAccompli1Sampler::new_with_stake_weighted_fallback(
            validators.clone(),
            10,
        ));
        sample_all_validators(&FaitAccompli1Sampler::new_with_partition_fallback(
            validators.clone(),
            10,
        ));
        sample_all_validators(&FaitAccompli2Sampler::new(validators.clone(), 10));
    }

    fn sample_all_validators<S: SamplingStrategy>(sampler: &S) {
        let mut rng = rand::rng();
        let mut sampled1 = HashSet::new();
        let mut sampled2 = HashSet::new();
        for _ in 0..1000 {
            sampled1.insert(sampler.sample(&mut rng));
            sampled2.insert(sampler.sample_info(&mut rng).id);
        }
        for id in 0..10 {
            assert!(sampled1.contains(&id));
            assert!(sampled2.contains(&id));
        }
    }

    use std::sync::Arc;

    use crate::GeoLocation;
    use crate::sherpa::Sherpa;

    fn create_geo_validators(locations: &[(f64, f64)]) -> Vec<ValidatorInfo> {
        let mut validators = create_validator_info(locations.len() as ValidatorId);
        for (v, (lat, lon)) in validators.iter_mut().zip(locations) {
            v.location = Some(GeoLocation::new(*lat, *lon));
        }
        validators
    }

    fn spread_locations() -> Vec<(f64, f64)> {
        vec![
            (51.5, -0.1),   // London
            (40.7, -74.0),  // New York
            (35.7, 139.7),  // Tokyo
            (-33.9, 151.2), // Sydney
            (-23.5, -46.6), // São Paulo
        ]
    }

    #[test]
    fn default_sample_shred_relay_is_deterministic() {
        let validators = create_validator_info(10);
        let sampler = StakeWeightedSampler::new(validators);
        for shred in 0..20 {
            let a = sampler.sample_shred_relay(Slot::new(7), shred);
            let b = sampler.sample_shred_relay(Slot::new(7), shred);
            assert_eq!(a, b);
        }
    }

    #[test]
    fn geo_aware_relay_round_robin_distinct() {
        let validators = create_geo_validators(&spread_locations());
        let sherpa = Arc::new(Sherpa::new(0, validators.clone()));
        let sampler = GeoAwareSampler::new(sherpa, validators, 5, Vec::new());

        let relays: HashSet<ValidatorId> = (0..5)
            .map(|i| sampler.sample_shred_relay(Slot::new(0), i))
            .collect();
        assert_eq!(relays.len(), 5);

        for i in 0..10 {
            let a = sampler.sample_shred_relay(Slot::new(0), i);
            let b = sampler.sample_shred_relay(Slot::new(0), i + 1);
            assert_ne!(a, b);
        }
    }

    #[test]
    fn geo_aware_relay_deterministic_across_instances() {
        let validators = create_geo_validators(&spread_locations());
        let sampler1 = GeoAwareSampler::new(
            Arc::new(Sherpa::new(0, validators.clone())),
            validators.clone(),
            5,
            Vec::new(),
        );
        let sampler2 = GeoAwareSampler::new(
            Arc::new(Sherpa::new(3, validators.clone())),
            validators,
            5,
            Vec::new(),
        );

        for slot in 0..4 {
            for shred in 0..20 {
                assert_eq!(
                    sampler1.sample_shred_relay(Slot::new(slot), shred),
                    sampler2.sample_shred_relay(Slot::new(slot), shred),
                    "relay assignment diverged at slot {slot} shred {shred}"
                );
            }
        }
    }

    #[test]
    fn geo_aware_relay_respects_exclude() {
        let validators = create_geo_validators(&spread_locations());
        let sherpa = Arc::new(Sherpa::new(0, validators.clone()));
        let sampler = GeoAwareSampler::new(sherpa, validators, 5, vec![2]);

        for slot in 0..4 {
            for shred in 0..20 {
                assert_ne!(sampler.sample_shred_relay(Slot::new(slot), shred), 2);
            }
        }
    }

    #[test]
    fn geo_aware_relay_fallback_when_all_excluded() {
        let validators = create_geo_validators(&spread_locations());
        let n = validators.len() as ValidatorId;
        let sherpa = Arc::new(Sherpa::new(0, validators.clone()));
        let sampler = GeoAwareSampler::new(sherpa, validators, 5, (0..n).collect());

        let relay = sampler.sample_shred_relay(Slot::new(0), 0);
        assert!(relay < n);
        assert_eq!(relay, sampler.sample_shred_relay(Slot::new(0), 0));
    }

    #[test]
    fn geo_aware_relay_avoids_geographic_bottleneck() {
        let validators = create_geo_validators(&[
            (51.5, -0.1),  // London
            (51.6, 0.0),   // London cluster
            (51.4, -0.2),  // London cluster
            (40.7, -74.0), // New York
            (35.7, 139.7), // Tokyo
        ]);
        let sherpa = Arc::new(Sherpa::new(0, validators.clone()));
        let sampler = GeoAwareSampler::new(sherpa, validators, 3, Vec::new());

        let relays: Vec<ValidatorId> = (0..3)
            .map(|i| sampler.sample_shred_relay(Slot::new(0), i))
            .collect();
        let relay_set: HashSet<ValidatorId> = relays.iter().copied().collect();
        assert_eq!(relay_set.len(), 3);
        assert!(relay_set.contains(&3), "New York must be a relay");
        assert!(relay_set.contains(&4), "Tokyo must be a relay");
        let london_relays = relay_set.iter().filter(|id| **id <= 2).count();
        assert_eq!(london_relays, 1, "only one London-cluster relay allowed");
    }
}
