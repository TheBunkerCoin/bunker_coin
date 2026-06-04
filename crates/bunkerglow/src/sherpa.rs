// BunkerCoin
// SPDX-License-Identifier: Apache-2.0

//! Sherpa: Location-Aware Routing Layer
//!
//! Sherpa is the central routing service described in the BunkerCoin whitepaper (§1.1.1).
//! It maintains a map of the network topology — including validator locations and
//! connectivity — and provides optimal routing paths to higher-level protocols
//! (Votor for BLS signature aggregation, Radiotor/Rotor for block propagation).
//!
//! # Design
//!
//! Sherpa works at the `ValidatorInfo` level. Every validator that has submitted a
//! Proof-of-Location claim carries a [`GeoLocation`]. Sherpa uses these locations to:
//!
//! 1. **Order peers by proximity** for consensus broadcasts: nearby validators are
//!    reached first, reducing median latency for vote aggregation.
//! 2. **Select geographically diverse relays** for block dissemination: Rotor picks
//!    relay nodes that cover different HF propagation regions, maximising parallel
//!    utilisation of distinct ionospheric paths.
//! 3. **Track neighbour liveness** and mark links as failed so routing falls back
//!    gracefully to the next best peer.
//!
//! # Usage
//!
//! ```rust,ignore
//! use bunkerglow::sherpa::Sherpa;
//! use bunkerglow::all2all::SherpaAll2All;
//!
//! let sherpa = Arc::new(Sherpa::new(epoch_info.clone()));
//! let all2all = SherpaAll2All::new(validators.clone(), network, sherpa.clone());
//! ```

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::{GeoLocation, ValidatorId, ValidatorInfo};

/// Maximum number of consecutive send failures before a link is marked as failed.
const FAILURE_THRESHOLD: u32 = 3;

/// A routing path: an ordered list of validator IDs to reach a destination.
///
/// The first element is the immediate next hop; the last is the final target.
pub type RoutePath = Vec<ValidatorId>;

/// Link state for a single (src → dst) pair.
#[derive(Clone, Debug)]
struct LinkState {
    /// Number of consecutive send failures on this link.
    failures: u32,
    /// Whether the link is currently considered failed/unreachable.
    failed: bool,
}

impl Default for LinkState {
    fn default() -> Self {
        Self {
            failures: 0,
            failed: false,
        }
    }
}

/// Sherpa — location-aware routing service.
///
/// Maintains a topology snapshot for the current epoch and answers routing queries
/// from Votor (consensus) and Rotor (block dissemination).
pub struct Sherpa {
    /// All validators for the current epoch, indexed by `ValidatorId`.
    validators: Vec<ValidatorInfo>,
    /// Per-link liveness state: `(src, dst) → LinkState`.
    link_states: RwLock<HashMap<(ValidatorId, ValidatorId), LinkState>>,
    /// This node's own ID (used when computing routes from self).
    own_id: ValidatorId,
}

impl Sherpa {
    /// Create a new Sherpa instance for the given validator set and own ID.
    pub fn new(own_id: ValidatorId, validators: Vec<ValidatorInfo>) -> Self {
        Self {
            validators,
            link_states: RwLock::new(HashMap::new()),
            own_id,
        }
    }

    /// Returns validators sorted by ascending great-circle distance from `origin`.
    ///
    /// Validators without a known location are placed at the end, ordered by ID.
    /// This is the primary ordering used by [`SherpaAll2All`] to reach nearby
    /// validators first, minimising median latency for vote aggregation.
    pub fn peers_by_proximity(&self, origin: ValidatorId) -> Vec<&ValidatorInfo> {
        let origin_loc = self
            .validators
            .get(origin as usize)
            .and_then(|v| v.location);

        let mut peers: Vec<&ValidatorInfo> =
            self.validators.iter().filter(|v| v.id != origin).collect();

        peers.sort_by(|a, b| {
            let dist_a = self.distance_km(origin_loc.as_ref(), a.location.as_ref());
            let dist_b = self.distance_km(origin_loc.as_ref(), b.location.as_ref());
            dist_a
                .partial_cmp(&dist_b)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        peers
    }

    /// Select `k` geographically diverse relay candidates from the validator set,
    /// excluding the caller (`exclude_id`) and the block leader (`leader_id`).
    ///
    /// "Diverse" means we maximise the minimum pairwise distance between selected
    /// relays (greedy farthest-point selection). This ensures shreds travel via
    /// distinct HF propagation paths, as required by the Radiotor spec (§2.4).
    ///
    /// Falls back to stake-weighted order when locations are absent.
    pub fn diverse_relays(&self, k: usize, exclude_ids: &[ValidatorId]) -> Vec<&ValidatorInfo> {
        let candidates: Vec<&ValidatorInfo> = self
            .validators
            .iter()
            .filter(|v| !exclude_ids.contains(&v.id))
            .collect();

        if candidates.is_empty() {
            return Vec::new();
        }

        // If no location data is available, return by descending stake (best effort).
        if candidates.iter().all(|v| v.location.is_none()) {
            let mut by_stake = candidates;
            by_stake.sort_by(|a, b| b.stake.cmp(&a.stake));
            return by_stake.into_iter().take(k).collect();
        }

        // Greedy farthest-point selection.
        let k = k.min(candidates.len());
        let mut selected: Vec<&ValidatorInfo> = Vec::with_capacity(k);

        // Seed: validator with known location and highest stake.
        let seed = candidates
            .iter()
            .filter(|v| v.location.is_some())
            .max_by_key(|v| v.stake)
            .copied()
            .unwrap_or(candidates[0]);
        selected.push(seed);

        while selected.len() < k {
            // Pick the candidate that maximises its minimum distance to already-selected.
            let next = candidates
                .iter()
                .filter(|c| !selected.iter().any(|s| s.id == c.id))
                .max_by(|a, b| {
                    let min_dist_a = self.min_distance_to_set(a.location.as_ref(), &selected);
                    let min_dist_b = self.min_distance_to_set(b.location.as_ref(), &selected);
                    min_dist_a
                        .partial_cmp(&min_dist_b)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
            match next {
                Some(v) => selected.push(v),
                None => break,
            }
        }

        selected
    }

    /// Returns the IDs of validators that are neighbours of `validator_id` —
    /// i.e., within `radius_km` of it — and whose links are currently healthy.
    pub fn neighbours_within(&self, validator_id: ValidatorId, radius_km: f64) -> Vec<ValidatorId> {
        let origin_loc = match self
            .validators
            .get(validator_id as usize)
            .and_then(|v| v.location)
        {
            Some(loc) => loc,
            None => return Vec::new(),
        };

        self.validators
            .iter()
            .filter(|v| {
                v.id != validator_id
                    && v.location
                        .map(|loc| loc.distance_km(&origin_loc) <= radius_km)
                        .unwrap_or(false)
                    && !self.is_link_failed(validator_id, v.id)
            })
            .map(|v| v.id)
            .collect()
    }

    /// Record a successful send on the link `(src → dst)`, clearing any failure count.
    pub fn record_success(&self, src: ValidatorId, dst: ValidatorId) {
        let mut states = self.link_states.write().expect("lock poisoned");
        let state = states.entry((src, dst)).or_default();
        state.failures = 0;
        state.failed = false;
    }

    /// Record a failed send on the link `(src → dst)`.
    /// After [`FAILURE_THRESHOLD`] consecutive failures the link is marked as failed.
    pub fn record_failure(&self, src: ValidatorId, dst: ValidatorId) {
        let mut states = self.link_states.write().expect("lock poisoned");
        let state = states.entry((src, dst)).or_default();
        state.failures += 1;
        if state.failures >= FAILURE_THRESHOLD {
            state.failed = true;
        }
    }

    /// Returns `true` if the link `(src → dst)` is currently marked as failed.
    pub fn is_link_failed(&self, src: ValidatorId, dst: ValidatorId) -> bool {
        self.link_states
            .read()
            .expect("lock poisoned")
            .get(&(src, dst))
            .map(|s| s.failed)
            .unwrap_or(false)
    }

    /// Returns all validators, in the order stored (by ID).
    pub fn validators(&self) -> &[ValidatorInfo] {
        &self.validators
    }

    /// Returns the validator info for a given ID.
    pub fn validator(&self, id: ValidatorId) -> Option<&ValidatorInfo> {
        self.validators.get(id as usize)
    }

    pub fn own_id(&self) -> ValidatorId {
        self.own_id
    }

    // --- internal helpers ---------------------------------------------------

    fn distance_km(&self, origin: Option<&GeoLocation>, target: Option<&GeoLocation>) -> f64 {
        match (origin, target) {
            (Some(o), Some(t)) => o.distance_km(t),
            // Validators without location go to the back (very large distance).
            _ => f64::MAX,
        }
    }

    fn min_distance_to_set(&self, loc: Option<&GeoLocation>, set: &[&ValidatorInfo]) -> f64 {
        set.iter()
            .map(|s| self.distance_km(loc, s.location.as_ref()))
            .fold(f64::MAX, f64::min)
    }
}

/// A shareable, cheaply-cloneable handle to a [`Sherpa`] instance.
pub type SherpaHandle = Arc<Sherpa>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GeoLocation;
    use std::net::SocketAddr;

    fn make_validator(id: u64, lat: f64, lon: f64, stake: u64) -> ValidatorInfo {
        use crate::crypto::{aggsig, signature};
        let sk = signature::SecretKey::new(&mut rand::rng());
        let vsk = aggsig::SecretKey::new(&mut rand::rng());
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        ValidatorInfo {
            id,
            stake,
            pubkey: sk.to_pk(),
            voting_pubkey: vsk.to_pk(),
            all2all_address: addr,
            disseminator_address: addr,
            repair_request_address: addr,
            repair_response_address: addr,
            location: Some(GeoLocation::new(lat, lon)),
        }
    }

    fn make_validator_no_loc(id: u64) -> ValidatorInfo {
        use crate::crypto::{aggsig, signature};
        let sk = signature::SecretKey::new(&mut rand::rng());
        let vsk = aggsig::SecretKey::new(&mut rand::rng());
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        ValidatorInfo {
            id,
            stake: 1,
            pubkey: sk.to_pk(),
            voting_pubkey: vsk.to_pk(),
            all2all_address: addr,
            disseminator_address: addr,
            repair_request_address: addr,
            repair_response_address: addr,
            location: None,
        }
    }

    #[test]
    fn geolocation_distance() {
        // London ↔ New York is ~5,570 km
        let london = GeoLocation::new(51.5, -0.1);
        let new_york = GeoLocation::new(40.7, -74.0);
        let dist = london.distance_km(&new_york);
        assert!((dist - 5570.0).abs() < 100.0, "distance was {dist}");
    }

    #[test]
    fn peers_by_proximity_nearest_first() {
        // v0 = London, v1 = Paris (~340 km), v2 = New York (~5570 km)
        let london = make_validator(0, 51.5, -0.1, 1);
        let paris = make_validator(1, 48.9, 2.4, 1);
        let new_york = make_validator(2, 40.7, -74.0, 1);
        let validators = vec![london, paris, new_york];
        let sherpa = Sherpa::new(0, validators);

        let ordered = sherpa.peers_by_proximity(0);
        assert_eq!(ordered[0].id, 1, "Paris should be closest to London");
        assert_eq!(ordered[1].id, 2, "New York should be furthest");
    }

    #[test]
    fn peers_by_proximity_unknown_location_last() {
        let london = make_validator(0, 51.5, -0.1, 1);
        let unknown = make_validator_no_loc(1);
        let paris = make_validator(2, 48.9, 2.4, 1);
        let validators = vec![london, unknown, paris];
        let sherpa = Sherpa::new(0, validators);

        let ordered = sherpa.peers_by_proximity(0);
        // Paris (known location) should come before unknown
        assert_eq!(ordered[0].id, 2, "Paris should be before unknown");
        assert_eq!(ordered[1].id, 1, "unknown should be last");
    }

    #[test]
    fn diverse_relays_selects_geographically_spread() {
        // Four validators spread around the globe
        let london = make_validator(0, 51.5, -0.1, 1);
        let tokyo = make_validator(1, 35.7, 139.7, 1);
        let new_york = make_validator(2, 40.7, -74.0, 1);
        let sydney = make_validator(3, -33.9, 151.2, 1);
        let validators = vec![london, tokyo, new_york, sydney];
        let sherpa = Sherpa::new(0, validators);

        let relays = sherpa.diverse_relays(2, &[0]);
        assert_eq!(relays.len(), 2);
        // The two selected should not be the same validator
        assert_ne!(relays[0].id, relays[1].id);
    }

    #[test]
    fn diverse_relays_excludes_specified_ids() {
        let v0 = make_validator(0, 51.5, -0.1, 1);
        let v1 = make_validator(1, 35.7, 139.7, 1);
        let v2 = make_validator(2, 40.7, -74.0, 1);
        let validators = vec![v0, v1, v2];
        let sherpa = Sherpa::new(0, validators);

        let relays = sherpa.diverse_relays(2, &[0, 1]);
        // Only v2 is available
        assert_eq!(relays.len(), 1);
        assert_eq!(relays[0].id, 2);
    }

    #[test]
    fn link_failure_tracking() {
        let validators = vec![
            make_validator(0, 0.0, 0.0, 1),
            make_validator(1, 1.0, 1.0, 1),
        ];
        let sherpa = Sherpa::new(0, validators);

        assert!(!sherpa.is_link_failed(0, 1));

        // Below threshold: not yet failed
        sherpa.record_failure(0, 1);
        sherpa.record_failure(0, 1);
        assert!(!sherpa.is_link_failed(0, 1));

        // At threshold: marked failed
        sherpa.record_failure(0, 1);
        assert!(sherpa.is_link_failed(0, 1));

        // Success clears the failure
        sherpa.record_success(0, 1);
        assert!(!sherpa.is_link_failed(0, 1));
    }

    #[test]
    fn neighbours_within_radius() {
        // v0 = London, v1 = Paris (~340 km), v2 = New York (~5570 km)
        let london = make_validator(0, 51.5, -0.1, 1);
        let paris = make_validator(1, 48.9, 2.4, 1);
        let new_york = make_validator(2, 40.7, -74.0, 1);
        let validators = vec![london, paris, new_york];
        let sherpa = Sherpa::new(0, validators);

        let near = sherpa.neighbours_within(0, 1000.0);
        assert!(near.contains(&1), "Paris should be within 1000 km");
        assert!(!near.contains(&2), "New York should be outside 1000 km");
    }

    #[test]
    fn neighbours_within_excludes_failed_links() {
        let v0 = make_validator(0, 51.5, -0.1, 1);
        let v1 = make_validator(1, 48.9, 2.4, 1); // Paris, ~340 km
        let validators = vec![v0, v1];
        let sherpa = Sherpa::new(0, validators);

        // Mark link as failed
        sherpa.record_failure(0, 1);
        sherpa.record_failure(0, 1);
        sherpa.record_failure(0, 1);
        assert!(sherpa.is_link_failed(0, 1));

        let near = sherpa.neighbours_within(0, 1000.0);
        assert!(!near.contains(&1), "failed link should be excluded");
    }
}
