use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::types::{
    Amount, PublicKey, Signature, ATTESTER_MIN_STAKE_AGE_EPOCHS, MIN_LOCATION_ATTESTERS,
    MIN_SELF_STAKE,
};

pub const ACTIVATION_DELAY_EPOCHS: u64 = 1;
pub const UNBONDING_PERIOD_EPOCHS: u64 = 2;
/// Downtime jail lasts 1 epoch; double-vote jail is indefinite (unjail requires re-stake).
pub const DOWNTIME_JAIL_PERIOD_EPOCHS: u64 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SlashOffenceKind {
    /// Signing two different blocks at the same slot — 100% stake slash, indefinite jail.
    DoubleVote,
    /// Signing conflicting votes (e.g. notar + skip) — 100% stake slash, indefinite jail.
    ConflictingVote,
    /// Missing >20% of votes in an epoch — 0% slash, 1-epoch jail.
    Downtime,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlashingEvent {
    pub validator: PublicKey,
    pub offence: SlashOffenceKind,
    pub epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JailRecord {
    pub validator: PublicKey,
    pub epoch_jailed: u64,
    pub offence: SlashOffenceKind,
    pub amount_slashed: Amount,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UnjailError {
    NotJailed,
    JailPeriodNotElapsed,
    NoStake,
    InsufficientSelfStake,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StakingLedger {
    pub delegations: HashMap<PublicKey, Amount>,
    pub pending_bonds: Vec<PendingBond>,
    pub pending_retires: Vec<PendingRetire>,
    pub completed_retires: Vec<CompletedRetire>,
    pub jailed: HashMap<PublicKey, JailRecord>,
    pub pending_slashes: Vec<SlashingEvent>,
    pub self_bonds: HashMap<PublicKey, Amount>,
    pub commission_rates: HashMap<PublicKey, u16>,
    pub pending_commission_changes: Vec<(PublicKey, u16)>,
    /// Per-delegator stakes: (delegator, validator) → amount
    pub delegator_stakes: HashMap<(PublicKey, PublicKey), Amount>,
    pub pending_location_claims: Vec<(LocationClaim, Vec<LocationAttestation>)>,
    pub validated_locations: HashMap<PublicKey, ValidatedLocation>,
    #[serde(default)]
    pub msg_relay_participants: HashSet<PublicKey>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingBond {
    pub delegator: PublicKey,
    pub validator: PublicKey,
    pub amount: Amount,
    pub epoch_queued: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingRetire {
    pub delegator: PublicKey,
    pub validator: PublicKey,
    pub amount: Amount,
    pub epoch_queued: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletedRetire {
    pub delegator: PublicKey,
    pub validator: PublicKey,
    pub amount: Amount,
    pub epoch_completed: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocationClaim {
    pub validator: PublicKey,
    pub lat: i32,
    pub lon: i32,
    pub epoch_claimed: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocationAttestation {
    pub attester: PublicKey,
    pub propagation_delay_ms: u32,
    #[serde(with = "crate::types::serde_signature")]
    pub signature: Signature,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidatedLocation {
    pub lat: i32,
    pub lon: i32,
    pub epoch_validated: u64,
    pub attestation_count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LocationClaimError {
    InsufficientAttestations { required: usize, provided: usize },
    AttesterNotEligible(PublicKey),
    NotStaked,
    LocationOutOfBounds,
}

impl Default for StakingLedger {
    fn default() -> Self {
        Self::new()
    }
}

impl StakingLedger {
    pub fn new() -> Self {
        Self {
            delegations: HashMap::new(),
            pending_bonds: Vec::new(),
            pending_retires: Vec::new(),
            completed_retires: Vec::new(),
            jailed: HashMap::new(),
            pending_slashes: Vec::new(),
            self_bonds: HashMap::new(),
            commission_rates: HashMap::new(),
            pending_commission_changes: Vec::new(),
            delegator_stakes: HashMap::new(),
            pending_location_claims: Vec::new(),
            validated_locations: HashMap::new(),
            msg_relay_participants: HashSet::new(),
        }
    }

    pub fn queue_bond(
        &mut self,
        delegator: PublicKey,
        validator: PublicKey,
        amount: Amount,
        epoch: u64,
    ) {
        self.pending_bonds.push(PendingBond {
            delegator,
            validator,
            amount,
            epoch_queued: epoch,
        });
    }

    pub fn queue_retire(
        &mut self,
        delegator: PublicKey,
        validator: PublicKey,
        amount: Amount,
        epoch: u64,
    ) {
        self.pending_retires.push(PendingRetire {
            delegator,
            validator,
            amount,
            epoch_queued: epoch,
        });
    }

    /// activate bonds queued ACTIVATION_DELAY_EPOCHS ago
    pub fn activate_pending_bonds(&mut self, current_epoch: u64) -> Vec<PendingBond> {
        let (activate, keep): (Vec<_>, Vec<_>) = self
            .pending_bonds
            .drain(..)
            .partition(|b| current_epoch >= b.epoch_queued + ACTIVATION_DELAY_EPOCHS);

        self.pending_bonds = keep;

        for bond in &activate {
            *self.delegations.entry(bond.validator).or_insert(0) += bond.amount;
            if bond.delegator == bond.validator {
                *self.self_bonds.entry(bond.validator).or_insert(0) += bond.amount;
            }
            *self
                .delegator_stakes
                .entry((bond.delegator, bond.validator))
                .or_insert(0) += bond.amount;
        }

        activate
    }

    /// complete retirements past UNBONDING_PERIOD_EPOCHS
    pub fn complete_pending_retires(&mut self, current_epoch: u64) -> Vec<PendingRetire> {
        let (complete, keep): (Vec<_>, Vec<_>) = self
            .pending_retires
            .drain(..)
            .partition(|r| current_epoch >= r.epoch_queued + UNBONDING_PERIOD_EPOCHS);

        self.pending_retires = keep;

        for retire in &complete {
            if let Some(stake) = self.delegations.get_mut(&retire.validator) {
                *stake = stake.saturating_sub(retire.amount);
                if *stake == 0 {
                    self.delegations.remove(&retire.validator);
                }
            }
            if retire.delegator == retire.validator {
                if let Some(sb) = self.self_bonds.get_mut(&retire.validator) {
                    *sb = sb.saturating_sub(retire.amount);
                    if *sb == 0 {
                        self.self_bonds.remove(&retire.validator);
                    }
                }
            }
            let key = (retire.delegator, retire.validator);
            if let Some(ds) = self.delegator_stakes.get_mut(&key) {
                *ds = ds.saturating_sub(retire.amount);
                if *ds == 0 {
                    self.delegator_stakes.remove(&key);
                }
            }
            self.completed_retires.push(CompletedRetire {
                delegator: retire.delegator,
                validator: retire.validator,
                amount: retire.amount,
                epoch_completed: current_epoch,
            });
        }

        complete
    }

    /// withdraw completed retires, returning amount available for the given delegator+validator
    pub fn withdraw(&mut self, delegator: &PublicKey, validator: &PublicKey) -> Amount {
        let mut total = 0;
        self.completed_retires.retain(|r| {
            if r.delegator == *delegator && r.validator == *validator {
                total += r.amount;
                false
            } else {
                true
            }
        });
        total
    }

    pub fn report_offence(&mut self, event: SlashingEvent) {
        self.pending_slashes.push(event);
    }

    /// Drain pending slashes, apply per-offence slash fraction, and jail offenders.
    ///
    /// - `DoubleVote` / `ConflictingVote`: 100% stake burned, indefinite jail (unjail requires
    ///   operator tx + min self-stake satisfied).
    /// - `Downtime`: 0% slash, `DOWNTIME_JAIL_PERIOD_EPOCHS` jail.
    pub fn process_slashes(&mut self, current_epoch: u64) -> Vec<JailRecord> {
        let events: Vec<SlashingEvent> = self.pending_slashes.drain(..).collect();
        let mut records = Vec::new();

        for event in events {
            if self.jailed.contains_key(&event.validator) {
                continue;
            }

            let stake = self.delegations.get(&event.validator).copied().unwrap_or(0);

            let slash_amount = match event.offence {
                SlashOffenceKind::DoubleVote | SlashOffenceKind::ConflictingVote => stake,
                SlashOffenceKind::Downtime => 0,
            };

            if slash_amount > 0 {
                if let Some(s) = self.delegations.get_mut(&event.validator) {
                    *s = s.saturating_sub(slash_amount);
                    if *s == 0 {
                        self.delegations.remove(&event.validator);
                    }
                }

                let self_stake = self.self_bonds.get(&event.validator).copied().unwrap_or(0);
                if self_stake > 0 && stake > 0 {
                    let self_slash =
                        (slash_amount as u128 * self_stake as u128 / stake as u128) as Amount;
                    if let Some(sb) = self.self_bonds.get_mut(&event.validator) {
                        *sb = sb.saturating_sub(self_slash);
                        if *sb == 0 {
                            self.self_bonds.remove(&event.validator);
                        }
                    }
                }
            }

            let record = JailRecord {
                validator: event.validator,
                epoch_jailed: current_epoch,
                offence: event.offence,
                amount_slashed: slash_amount,
            };
            self.jailed.insert(event.validator, record.clone());
            records.push(record);
        }

        records
    }

    pub fn unjail(&mut self, validator: &PublicKey, current_epoch: u64) -> Result<(), UnjailError> {
        let record = self.jailed.get(validator).ok_or(UnjailError::NotJailed)?;

        let jail_period = match record.offence {
            // Downtime: 1-epoch jail, then unjail is automatic via operator tx.
            SlashOffenceKind::Downtime => DOWNTIME_JAIL_PERIOD_EPOCHS,
            // Equivocation: indefinite — operator must re-stake before unjailing.
            // We model "indefinite" as requiring the jail period to elapse (0 epochs, i.e.
            // any epoch ≥ epoch_jailed) BUT also requiring MIN_SELF_STAKE to be satisfied
            // (which is impossible after a 100% slash without a new bond).
            SlashOffenceKind::DoubleVote | SlashOffenceKind::ConflictingVote => 0,
        };

        if current_epoch < record.epoch_jailed + jail_period {
            return Err(UnjailError::JailPeriodNotElapsed);
        }

        let stake = self.delegations.get(validator).copied().unwrap_or(0);
        if stake == 0 {
            return Err(UnjailError::NoStake);
        }

        let self_stake = self.self_bonds.get(validator).copied().unwrap_or(0);
        if self_stake < MIN_SELF_STAKE {
            return Err(UnjailError::InsufficientSelfStake);
        }

        self.jailed.remove(validator);
        Ok(())
    }

    /// Submit a location claim with attestations. Validates attester eligibility
    /// (stake age ≥1 prior epoch) and minimum attestation count.
    pub fn submit_location_claim(
        &mut self,
        validator: PublicKey,
        lat: i32,
        lon: i32,
        attestations: Vec<LocationAttestation>,
        current_epoch: u64,
    ) -> Result<(), LocationClaimError> {
        // Sender must be staked
        if !self.delegations.contains_key(&validator) {
            return Err(LocationClaimError::NotStaked);
        }

        // Validate lat/lon bounds (milli-degrees)
        if !(-90_000..=90_000).contains(&lat) || !(-180_000..=180_000).contains(&lon) {
            return Err(LocationClaimError::LocationOutOfBounds);
        }

        // Check minimum attestation count
        let total_validators = self.delegations.len();
        let required = MIN_LOCATION_ATTESTERS.min(total_validators);
        if attestations.len() < required {
            return Err(LocationClaimError::InsufficientAttestations {
                required,
                provided: attestations.len(),
            });
        }

        // Verify each attester has held stake for ≥1 prior epoch
        for att in &attestations {
            let has_mature_stake = self.delegations.contains_key(&att.attester)
                && current_epoch >= ATTESTER_MIN_STAKE_AGE_EPOCHS;
            if !has_mature_stake {
                return Err(LocationClaimError::AttesterNotEligible(att.attester));
            }
        }

        let claim = LocationClaim {
            validator,
            lat,
            lon,
            epoch_claimed: current_epoch,
        };
        self.pending_location_claims.push((claim, attestations));
        Ok(())
    }

    /// Process pending location claims at epoch transition, moving valid ones
    /// to validated_locations. Returns the list of validators whose locations
    /// were validated.
    pub fn process_location_claims(&mut self, current_epoch: u64) -> Vec<PublicKey> {
        let claims: Vec<_> = self.pending_location_claims.drain(..).collect();
        let mut validated = Vec::new();

        for (claim, attestations) in claims {
            self.validated_locations.insert(
                claim.validator,
                ValidatedLocation {
                    lat: claim.lat,
                    lon: claim.lon,
                    epoch_validated: current_epoch,
                    attestation_count: attestations.len() as u32,
                },
            );
            validated.push(claim.validator);
        }

        validated
    }

    pub fn get_location(&self, validator: &PublicKey) -> Option<&ValidatedLocation> {
        self.validated_locations.get(validator)
    }

    pub fn record_relay_participation(&mut self, validator: PublicKey) {
        self.msg_relay_participants.insert(validator);
    }

    pub fn drain_relay_participants(&mut self) -> HashSet<PublicKey> {
        std::mem::take(&mut self.msg_relay_participants)
    }

    pub fn total_stake(&self) -> Amount {
        self.delegations.values().sum()
    }

    pub fn total_active_stake(&self) -> Amount {
        self.delegations
            .iter()
            .filter(|(pk, _)| {
                !self.jailed.contains_key(*pk)
                    && self.self_bonds.get(*pk).copied().unwrap_or(0) >= MIN_SELF_STAKE
            })
            .map(|(_, &stake)| stake)
            .sum()
    }

    pub fn validator_set(&self) -> Vec<(PublicKey, Amount)> {
        self.delegations
            .iter()
            .filter(|(pk, &stake)| {
                stake > 0
                    && !self.jailed.contains_key(*pk)
                    && self.self_bonds.get(*pk).copied().unwrap_or(0) >= MIN_SELF_STAKE
            })
            .map(|(&pk, &stake)| (pk, stake))
            .collect()
    }

    pub fn queue_commission_change(&mut self, validator: PublicKey, rate: u16) {
        self.pending_commission_changes.push((validator, rate));
    }

    pub fn apply_commission_changes(&mut self) {
        for (validator, rate) in self.pending_commission_changes.drain(..) {
            self.commission_rates.insert(validator, rate);
        }
    }

    /// Returns all (delegator, amount) pairs for a given validator
    pub fn delegators_of(&self, validator: &PublicKey) -> Vec<(PublicKey, Amount)> {
        self.delegator_stakes
            .iter()
            .filter(|((_, v), amt)| v == validator && **amt > 0)
            .map(|((d, _), amt)| (*d, *amt))
            .collect()
    }

    pub fn validators_below_min_self_stake(&self) -> Vec<PublicKey> {
        self.delegations
            .keys()
            .filter(|pk| {
                self.self_bonds.get(*pk).copied().unwrap_or(0) < MIN_SELF_STAKE
                    && !self.jailed.contains_key(*pk)
            })
            .copied()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pk(n: u8) -> PublicKey {
        let mut key = [0u8; 32];
        key[0] = n;
        key
    }

    #[test]
    fn bond_lifecycle() {
        let mut ledger = StakingLedger::new();
        let delegator = pk(1);
        let validator = pk(2);

        ledger.queue_bond(delegator, validator, 1000, 0);
        assert!(ledger.delegations.is_empty());

        // epoch 0: too early
        let activated = ledger.activate_pending_bonds(0);
        assert!(activated.is_empty());

        // epoch 1: activation delay met
        let activated = ledger.activate_pending_bonds(1);
        assert_eq!(activated.len(), 1);
        assert_eq!(*ledger.delegations.get(&validator).unwrap(), 1000);
    }

    #[test]
    fn retire_lifecycle() {
        let mut ledger = StakingLedger::new();
        let delegator = pk(1);
        let validator = pk(2);

        // set up delegation
        ledger.delegations.insert(validator, 1000);
        ledger.queue_retire(delegator, validator, 500, 5);

        // epoch 6: too early
        let completed = ledger.complete_pending_retires(6);
        assert!(completed.is_empty());

        // epoch 7: unbonding period met
        let completed = ledger.complete_pending_retires(7);
        assert_eq!(completed.len(), 1);
        assert_eq!(*ledger.delegations.get(&validator).unwrap(), 500);

        // withdraw
        let amount = ledger.withdraw(&delegator, &validator);
        assert_eq!(amount, 500);
        assert!(ledger.completed_retires.is_empty());
    }

    #[test]
    fn withdraw_nothing() {
        let mut ledger = StakingLedger::new();
        let amount = ledger.withdraw(&pk(1), &pk(2));
        assert_eq!(amount, 0);
    }

    #[test]
    fn validator_set_excludes_zero_stake() {
        let mut ledger = StakingLedger::new();
        ledger.delegations.insert(pk(1), 100);
        ledger.delegations.insert(pk(2), 0);
        ledger.delegations.insert(pk(3), 500);
        ledger.self_bonds.insert(pk(1), MIN_SELF_STAKE);
        ledger.self_bonds.insert(pk(2), MIN_SELF_STAKE);
        ledger.self_bonds.insert(pk(3), MIN_SELF_STAKE);

        let set = ledger.validator_set();
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn double_vote_slash_burns_all_stake() {
        let mut ledger = StakingLedger::new();
        let validator = pk(1);
        ledger.delegations.insert(validator, 1000);

        ledger.report_offence(SlashingEvent {
            validator,
            offence: SlashOffenceKind::DoubleVote,
            epoch: 5,
        });

        let records = ledger.process_slashes(5);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].amount_slashed, 1000); // 100% of 1000
        assert!(!ledger.delegations.contains_key(&validator)); // fully burned
    }

    #[test]
    fn downtime_slash_burns_nothing() {
        let mut ledger = StakingLedger::new();
        let validator = pk(1);
        ledger.delegations.insert(validator, 1000);

        ledger.report_offence(SlashingEvent {
            validator,
            offence: SlashOffenceKind::Downtime,
            epoch: 5,
        });

        let records = ledger.process_slashes(5);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].amount_slashed, 0);
        assert_eq!(*ledger.delegations.get(&validator).unwrap(), 1000); // stake intact
        assert!(ledger.jailed.contains_key(&validator)); // still jailed
    }

    #[test]
    fn slash_jails_validator() {
        let mut ledger = StakingLedger::new();
        let validator = pk(1);
        ledger.delegations.insert(validator, 1000);

        ledger.report_offence(SlashingEvent {
            validator,
            offence: SlashOffenceKind::ConflictingVote,
            epoch: 3,
        });

        let records = ledger.process_slashes(3);
        assert_eq!(records.len(), 1);
        assert!(ledger.jailed.contains_key(&validator));
        assert_eq!(ledger.jailed[&validator].epoch_jailed, 3);
    }

    #[test]
    fn slash_already_jailed_skipped() {
        let mut ledger = StakingLedger::new();
        let validator = pk(1);
        ledger.delegations.insert(validator, 1000);

        ledger.report_offence(SlashingEvent {
            validator,
            offence: SlashOffenceKind::DoubleVote,
            epoch: 5,
        });
        ledger.process_slashes(5);
        // 100% burned
        assert!(!ledger.delegations.contains_key(&validator));

        // second slash should be skipped (already jailed)
        ledger.report_offence(SlashingEvent {
            validator,
            offence: SlashOffenceKind::DoubleVote,
            epoch: 5,
        });
        let records = ledger.process_slashes(5);
        assert!(records.is_empty());
    }

    #[test]
    fn unjail_downtime_success_after_one_epoch() {
        let mut ledger = StakingLedger::new();
        let validator = pk(1);
        ledger.delegations.insert(validator, 2 * MIN_SELF_STAKE);
        ledger.self_bonds.insert(validator, 2 * MIN_SELF_STAKE);

        ledger.report_offence(SlashingEvent {
            validator,
            offence: SlashOffenceKind::Downtime,
            epoch: 0,
        });
        ledger.process_slashes(0);
        assert!(ledger.jailed.contains_key(&validator));
        // stake intact after downtime
        assert_eq!(
            ledger.delegations.get(&validator).copied().unwrap(),
            2 * MIN_SELF_STAKE
        );

        // epoch 1: jail period elapsed (DOWNTIME_JAIL_PERIOD_EPOCHS = 1)
        let result = ledger.unjail(&validator, 1);
        assert!(result.is_ok());
        assert!(!ledger.jailed.contains_key(&validator));
    }

    #[test]
    fn unjail_downtime_too_early() {
        let mut ledger = StakingLedger::new();
        let validator = pk(1);
        ledger.delegations.insert(validator, 2 * MIN_SELF_STAKE);
        ledger.self_bonds.insert(validator, 2 * MIN_SELF_STAKE);

        ledger.report_offence(SlashingEvent {
            validator,
            offence: SlashOffenceKind::Downtime,
            epoch: 5,
        });
        ledger.process_slashes(5);

        // epoch 5: same epoch as jailed — not yet elapsed
        let result = ledger.unjail(&validator, 5);
        assert_eq!(result, Err(UnjailError::JailPeriodNotElapsed));
    }

    #[test]
    fn unjail_double_vote_requires_restake() {
        let mut ledger = StakingLedger::new();
        let validator = pk(1);
        ledger.delegations.insert(validator, 2 * MIN_SELF_STAKE);
        ledger.self_bonds.insert(validator, 2 * MIN_SELF_STAKE);

        ledger.report_offence(SlashingEvent {
            validator,
            offence: SlashOffenceKind::DoubleVote,
            epoch: 0,
        });
        ledger.process_slashes(0);
        assert!(ledger.jailed.contains_key(&validator));
        // 100% burned — no stake left
        assert!(!ledger.delegations.contains_key(&validator));

        // Even many epochs later, cannot unjail without stake
        let result = ledger.unjail(&validator, 100);
        assert_eq!(result, Err(UnjailError::NoStake));
    }

    #[test]
    fn unjail_double_vote_after_restake() {
        let mut ledger = StakingLedger::new();
        let validator = pk(1);
        ledger.delegations.insert(validator, 2 * MIN_SELF_STAKE);
        ledger.self_bonds.insert(validator, 2 * MIN_SELF_STAKE);

        ledger.report_offence(SlashingEvent {
            validator,
            offence: SlashOffenceKind::DoubleVote,
            epoch: 0,
        });
        ledger.process_slashes(0);

        // Operator re-bonds sufficient self-stake (simulating bond activation)
        ledger.delegations.insert(validator, 2 * MIN_SELF_STAKE);
        ledger.self_bonds.insert(validator, 2 * MIN_SELF_STAKE);

        // Can now unjail at any epoch (jail_period = 0 for equivocation)
        let result = ledger.unjail(&validator, 1);
        assert!(result.is_ok());
        assert!(!ledger.jailed.contains_key(&validator));
    }

    #[test]
    fn self_bond_tracked_on_activation() {
        let mut ledger = StakingLedger::new();
        let validator = pk(1);
        ledger.queue_bond(validator, validator, MIN_SELF_STAKE, 0);

        let activated = ledger.activate_pending_bonds(1);
        assert_eq!(activated.len(), 1);
        assert_eq!(
            ledger.self_bonds.get(&validator).copied().unwrap(),
            MIN_SELF_STAKE
        );
    }

    #[test]
    fn delegated_bond_not_self_bond() {
        let mut ledger = StakingLedger::new();
        let delegator = pk(1);
        let validator = pk(2);
        ledger.queue_bond(delegator, validator, 1000, 0);

        ledger.activate_pending_bonds(1);
        assert_eq!(ledger.self_bonds.get(&validator).copied().unwrap_or(0), 0);
        assert_eq!(ledger.delegations.get(&validator).copied().unwrap(), 1000);
    }

    #[test]
    fn self_retire_reduces_self_bond() {
        let mut ledger = StakingLedger::new();
        let v = pk(1);
        ledger.delegations.insert(v, 2000);
        ledger.self_bonds.insert(v, 2000);

        ledger.queue_retire(v, v, 500, 0);
        ledger.complete_pending_retires(2);

        assert_eq!(ledger.delegations.get(&v).copied().unwrap(), 1500);
        assert_eq!(ledger.self_bonds.get(&v).copied().unwrap(), 1500);
    }

    #[test]
    fn delegator_retire_does_not_reduce_self_bond() {
        let mut ledger = StakingLedger::new();
        let delegator = pk(1);
        let validator = pk(2);
        ledger.delegations.insert(validator, 2000);
        ledger.self_bonds.insert(validator, 1000);

        ledger.queue_retire(delegator, validator, 500, 0);
        ledger.complete_pending_retires(2);

        assert_eq!(ledger.delegations.get(&validator).copied().unwrap(), 1500);
        assert_eq!(ledger.self_bonds.get(&validator).copied().unwrap(), 1000);
    }

    #[test]
    fn total_active_stake_excludes_jailed_and_low_self_bond() {
        let mut ledger = StakingLedger::new();
        let v1 = pk(1);
        let v2 = pk(2);
        let v3 = pk(3);
        ledger.delegations.insert(v1, 1000);
        ledger.delegations.insert(v2, 2000);
        ledger.delegations.insert(v3, 3000);
        ledger.self_bonds.insert(v1, MIN_SELF_STAKE);
        ledger.self_bonds.insert(v3, MIN_SELF_STAKE);
        // v2 has no self_bond -> excluded

        // Use Downtime so v1's stake isn't burned (makes total_active_stake check cleaner)
        ledger.report_offence(SlashingEvent {
            validator: v1,
            offence: SlashOffenceKind::Downtime,
            epoch: 0,
        });
        ledger.process_slashes(0);
        // v1 jailed -> excluded from active stake

        assert_eq!(ledger.total_active_stake(), 3000);
    }

    #[test]
    fn commission_change_queue_and_apply() {
        let mut ledger = StakingLedger::new();
        let v = pk(1);

        ledger.queue_commission_change(v, 500);
        ledger.queue_commission_change(v, 1000);
        assert!(ledger.commission_rates.is_empty());

        ledger.apply_commission_changes();
        // last queued wins
        assert_eq!(ledger.commission_rates.get(&v).copied().unwrap(), 1000);
        assert!(ledger.pending_commission_changes.is_empty());
    }

    #[test]
    fn validators_below_min_self_stake() {
        let mut ledger = StakingLedger::new();
        let v1 = pk(1);
        let v2 = pk(2);
        let v3 = pk(3);
        ledger.delegations.insert(v1, 1000);
        ledger.delegations.insert(v2, 2000);
        ledger.delegations.insert(v3, 3000);
        ledger.self_bonds.insert(v1, MIN_SELF_STAKE);
        // v2 has no self_bond
        ledger.self_bonds.insert(v3, MIN_SELF_STAKE - 1);

        let below = ledger.validators_below_min_self_stake();
        assert_eq!(below.len(), 2);
        assert!(below.contains(&v2));
        assert!(below.contains(&v3));
    }

    #[test]
    fn unjail_insufficient_self_stake() {
        let mut ledger = StakingLedger::new();
        let v = pk(1);
        // Use Downtime so stake is preserved, but self_bond is below minimum
        ledger.delegations.insert(v, 1000);
        ledger.self_bonds.insert(v, MIN_SELF_STAKE - 1);

        ledger.report_offence(SlashingEvent {
            validator: v,
            offence: SlashOffenceKind::Downtime,
            epoch: 0,
        });
        ledger.process_slashes(0);

        let result = ledger.unjail(&v, 10);
        assert_eq!(result, Err(UnjailError::InsufficientSelfStake));
    }

    #[test]
    fn unjail_no_stake() {
        let mut ledger = StakingLedger::new();
        let v = pk(1);
        // Use Downtime so we can control stake removal manually
        ledger.delegations.insert(v, 100);
        ledger.self_bonds.insert(v, MIN_SELF_STAKE);

        ledger.report_offence(SlashingEvent {
            validator: v,
            offence: SlashOffenceKind::Downtime,
            epoch: 0,
        });
        ledger.process_slashes(0);

        // remove all stake manually
        ledger.delegations.remove(&v);
        let result = ledger.unjail(&v, 10);
        assert_eq!(result, Err(UnjailError::NoStake));
    }

    #[test]
    fn unjail_not_jailed() {
        let mut ledger = StakingLedger::new();
        let result = ledger.unjail(&pk(1), 10);
        assert_eq!(result, Err(UnjailError::NotJailed));
    }

    // ── Location claim tests ──

    fn dummy_attestation(attester_id: u8) -> LocationAttestation {
        LocationAttestation {
            attester: pk(attester_id),
            propagation_delay_ms: 50,
            signature: [0u8; 64],
        }
    }

    /// Set up a ledger with N validators (pk(1)..pk(N)) all staked at epoch 0,
    /// with bonds activated at epoch 1 so they have mature stake by epoch 1+.
    fn ledger_with_validators(n: u8) -> StakingLedger {
        let mut ledger = StakingLedger::new();
        for i in 1..=n {
            let v = pk(i);
            ledger.delegations.insert(v, MIN_SELF_STAKE);
            ledger.self_bonds.insert(v, MIN_SELF_STAKE);
        }
        ledger
    }

    #[test]
    fn location_claim_valid() {
        let mut ledger = ledger_with_validators(4);
        let attestations = vec![
            dummy_attestation(2),
            dummy_attestation(3),
            dummy_attestation(4),
        ];
        let result = ledger.submit_location_claim(pk(1), 48_856, 2_352, attestations, 1);
        assert!(result.is_ok());
        assert_eq!(ledger.pending_location_claims.len(), 1);
    }

    #[test]
    fn location_claim_insufficient_attestations() {
        let mut ledger = ledger_with_validators(4);
        let attestations = vec![dummy_attestation(2), dummy_attestation(3)];
        let result = ledger.submit_location_claim(pk(1), 48_856, 2_352, attestations, 1);
        assert_eq!(
            result,
            Err(LocationClaimError::InsufficientAttestations {
                required: 3,
                provided: 2,
            })
        );
    }

    #[test]
    fn location_claim_attester_too_new() {
        let mut ledger = ledger_with_validators(4);
        let attestations = vec![
            dummy_attestation(2),
            dummy_attestation(3),
            dummy_attestation(4),
        ];
        // epoch 0: attesters can't have mature stake yet (need ≥1 epoch)
        let result = ledger.submit_location_claim(pk(1), 48_856, 2_352, attestations, 0);
        assert!(matches!(
            result,
            Err(LocationClaimError::AttesterNotEligible(_))
        ));
    }

    #[test]
    fn location_claim_out_of_bounds() {
        let mut ledger = ledger_with_validators(4);
        let attestations = vec![
            dummy_attestation(2),
            dummy_attestation(3),
            dummy_attestation(4),
        ];
        // lat 91_000 is out of range (> 90_000)
        let result = ledger.submit_location_claim(pk(1), 91_000, 0, attestations, 1);
        assert_eq!(result, Err(LocationClaimError::LocationOutOfBounds));
    }

    #[test]
    fn location_claim_not_staked() {
        let mut ledger = ledger_with_validators(4);
        let attestations = vec![
            dummy_attestation(2),
            dummy_attestation(3),
            dummy_attestation(4),
        ];
        // pk(10) is not a staked validator
        let result = ledger.submit_location_claim(pk(10), 48_856, 2_352, attestations, 1);
        assert_eq!(result, Err(LocationClaimError::NotStaked));
    }

    #[test]
    fn location_claim_process_at_epoch_transition() {
        let mut ledger = ledger_with_validators(4);
        let attestations = vec![
            dummy_attestation(2),
            dummy_attestation(3),
            dummy_attestation(4),
        ];
        ledger
            .submit_location_claim(pk(1), 48_856, 2_352, attestations, 1)
            .unwrap();

        let validated = ledger.process_location_claims(2);
        assert_eq!(validated.len(), 1);
        assert_eq!(validated[0], pk(1));
        assert!(ledger.pending_location_claims.is_empty());

        let loc = ledger.get_location(&pk(1)).unwrap();
        assert_eq!(loc.lat, 48_856);
        assert_eq!(loc.lon, 2_352);
        assert_eq!(loc.epoch_validated, 2);
        assert_eq!(loc.attestation_count, 3);
    }

    #[test]
    fn location_claim_small_network_requires_fewer_attestations() {
        // With only 2 validators total, min(3, 2) = 2 attestations needed
        let mut ledger = ledger_with_validators(2);
        let attestations = vec![dummy_attestation(2)];
        // 1 attestation < min(3, 2) = 2
        let result = ledger.submit_location_claim(pk(1), 0, 0, attestations, 1);
        assert_eq!(
            result,
            Err(LocationClaimError::InsufficientAttestations {
                required: 2,
                provided: 1,
            })
        );

        // 2 attestations should work
        let attestations = vec![dummy_attestation(2), dummy_attestation(2)];
        let result = ledger.submit_location_claim(pk(1), 0, 0, attestations, 1);
        assert!(result.is_ok());
    }

    #[test]
    fn total_stake() {
        let mut ledger = StakingLedger::new();
        ledger.delegations.insert(pk(1), 100);
        ledger.delegations.insert(pk(2), 200);
        ledger.delegations.insert(pk(3), 300);
        assert_eq!(ledger.total_stake(), 600);
    }

    #[test]
    fn bond_activation_delay() {
        let mut ledger = StakingLedger::new();
        ledger.queue_bond(pk(1), pk(2), 500, 5);

        // epoch 5: too early (need epoch_queued + ACTIVATION_DELAY_EPOCHS = 6)
        assert!(ledger.activate_pending_bonds(5).is_empty());
        // epoch 6: exact boundary
        let activated = ledger.activate_pending_bonds(6);
        assert_eq!(activated.len(), 1);
    }

    #[test]
    fn retire_unbonding_period() {
        let mut ledger = StakingLedger::new();
        ledger.delegations.insert(pk(2), 1000);
        ledger.queue_retire(pk(1), pk(2), 500, 5);

        // epoch 6: too early (need epoch_queued + UNBONDING_PERIOD_EPOCHS = 7)
        assert!(ledger.complete_pending_retires(6).is_empty());
        // epoch 7: exact boundary
        let completed = ledger.complete_pending_retires(7);
        assert_eq!(completed.len(), 1);
    }

    #[test]
    fn full_retire_removes_delegation() {
        let mut ledger = StakingLedger::new();
        let v = pk(1);
        ledger.delegations.insert(v, 1000);
        ledger.queue_retire(pk(2), v, 1000, 0);
        ledger.complete_pending_retires(2);

        assert!(!ledger.delegations.contains_key(&v));
    }

    #[test]
    fn double_vote_slash_burns_self_bonds_proportionally() {
        let mut ledger = StakingLedger::new();
        let v = pk(1);
        ledger.delegations.insert(v, 3000);
        ledger.self_bonds.insert(v, 1000); // 1/3 of total

        ledger.report_offence(SlashingEvent {
            validator: v,
            offence: SlashOffenceKind::DoubleVote,
            epoch: 0,
        });
        ledger.process_slashes(0);

        // 100% slash: all 3000 delegation burned, all 1000 self-bond burned
        assert!(!ledger.delegations.contains_key(&v));
        assert!(!ledger.self_bonds.contains_key(&v));
    }

    #[test]
    fn multiple_withdrawals_for_different_validators() {
        let mut ledger = StakingLedger::new();
        let delegator = pk(1);
        let v1 = pk(2);
        let v2 = pk(3);

        ledger.completed_retires.push(CompletedRetire {
            delegator,
            validator: v1,
            amount: 100,
            epoch_completed: 1,
        });
        ledger.completed_retires.push(CompletedRetire {
            delegator,
            validator: v2,
            amount: 200,
            epoch_completed: 1,
        });

        assert_eq!(ledger.withdraw(&delegator, &v1), 100);
        assert_eq!(ledger.withdraw(&delegator, &v2), 200);
        assert!(ledger.completed_retires.is_empty());
    }

    #[test]
    fn validator_set_excludes_jailed() {
        let mut ledger = StakingLedger::new();
        let v1 = pk(1);
        let v2 = pk(2);
        ledger.delegations.insert(v1, 500);
        ledger.delegations.insert(v2, 500);
        ledger.self_bonds.insert(v1, MIN_SELF_STAKE);
        ledger.self_bonds.insert(v2, MIN_SELF_STAKE);

        // Use Downtime so v1 remains jailed but stake is intact (cleaner set-membership test)
        ledger.report_offence(SlashingEvent {
            validator: v1,
            offence: SlashOffenceKind::Downtime,
            epoch: 0,
        });
        ledger.process_slashes(0);

        let set = ledger.validator_set();
        assert_eq!(set.len(), 1);
        assert_eq!(set[0].0, v2);
    }

    #[test]
    fn relay_participation_recorded_and_drained() {
        let mut ledger = StakingLedger::new();
        ledger.record_relay_participation(pk(1));
        ledger.record_relay_participation(pk(2));
        assert_eq!(ledger.msg_relay_participants.len(), 2);

        let drained = ledger.drain_relay_participants();
        assert_eq!(drained.len(), 2);
        assert!(drained.contains(&pk(1)));
        assert!(drained.contains(&pk(2)));
        assert!(ledger.msg_relay_participants.is_empty());
    }

    #[test]
    fn relay_participation_idempotent() {
        let mut ledger = StakingLedger::new();
        ledger.record_relay_participation(pk(1));
        ledger.record_relay_participation(pk(1));
        assert_eq!(ledger.msg_relay_participants.len(), 1);
    }
}
