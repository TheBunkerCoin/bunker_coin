use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::types::{Amount, PublicKey, MIN_SELF_STAKE};

pub const ACTIVATION_DELAY_EPOCHS: u64 = 1;
pub const UNBONDING_PERIOD_EPOCHS: u64 = 2;
pub const SLASH_FRACTION_PERCENT: u64 = 10;
pub const JAIL_PERIOD_EPOCHS: u64 = 4;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SlashOffenceKind {
    DoubleVote,
    ConflictingVote,
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
        }
    }

    pub fn queue_bond(&mut self, delegator: PublicKey, validator: PublicKey, amount: Amount, epoch: u64) {
        self.pending_bonds.push(PendingBond {
            delegator,
            validator,
            amount,
            epoch_queued: epoch,
        });
    }

    pub fn queue_retire(&mut self, delegator: PublicKey, validator: PublicKey, amount: Amount, epoch: u64) {
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

    /// drain pending slashes, deduct SLASH_FRACTION_PERCENT, jail offenders
    pub fn process_slashes(&mut self, current_epoch: u64) -> Vec<JailRecord> {
        let events: Vec<SlashingEvent> = self.pending_slashes.drain(..).collect();
        let mut records = Vec::new();

        for event in events {
            if self.jailed.contains_key(&event.validator) {
                continue;
            }

            let stake = self.delegations.get(&event.validator).copied().unwrap_or(0);
            let slash_amount = stake * SLASH_FRACTION_PERCENT / 100;

            if let Some(s) = self.delegations.get_mut(&event.validator) {
                *s = s.saturating_sub(slash_amount);
            }

            let self_stake = self.self_bonds.get(&event.validator).copied().unwrap_or(0);
            if self_stake > 0 && stake > 0 {
                let self_slash = (slash_amount as u128 * self_stake as u128 / stake as u128) as Amount;
                if let Some(sb) = self.self_bonds.get_mut(&event.validator) {
                    *sb = sb.saturating_sub(self_slash);
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

        if current_epoch < record.epoch_jailed + JAIL_PERIOD_EPOCHS {
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
    fn slash_reduces_stake() {
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
        assert_eq!(records[0].amount_slashed, 100); // 10% of 1000
        assert_eq!(*ledger.delegations.get(&validator).unwrap(), 900);
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
        assert_eq!(*ledger.delegations.get(&validator).unwrap(), 900);

        // second slash should be skipped
        ledger.report_offence(SlashingEvent {
            validator,
            offence: SlashOffenceKind::DoubleVote,
            epoch: 5,
        });
        let records = ledger.process_slashes(5);
        assert!(records.is_empty());
        assert_eq!(*ledger.delegations.get(&validator).unwrap(), 900);
    }

    #[test]
    fn unjail_success() {
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

        // after 10% slash: self_bonds = 1_800_000, still >= MIN_SELF_STAKE
        let result = ledger.unjail(&validator, 4);
        assert!(result.is_ok());
        assert!(!ledger.jailed.contains_key(&validator));
    }

    #[test]
    fn unjail_too_early() {
        let mut ledger = StakingLedger::new();
        let validator = pk(1);
        ledger.delegations.insert(validator, 1000);

        ledger.report_offence(SlashingEvent {
            validator,
            offence: SlashOffenceKind::DoubleVote,
            epoch: 0,
        });
        ledger.process_slashes(0);

        let result = ledger.unjail(&validator, 3);
        assert_eq!(result, Err(UnjailError::JailPeriodNotElapsed));
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

        ledger.report_offence(SlashingEvent {
            validator: v1,
            offence: SlashOffenceKind::DoubleVote,
            epoch: 0,
        });
        ledger.process_slashes(0);

        let set = ledger.validator_set();
        assert_eq!(set.len(), 1);
        assert_eq!(set[0].0, v2);
    }
}
