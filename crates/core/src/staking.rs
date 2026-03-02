use std::collections::HashMap;

use crate::types::{Amount, PublicKey};

pub const ACTIVATION_DELAY_EPOCHS: u64 = 1;
pub const UNBONDING_PERIOD_EPOCHS: u64 = 2;

#[derive(Clone, Debug)]
pub struct StakingLedger {
    pub delegations: HashMap<PublicKey, Amount>,
    pub pending_bonds: Vec<PendingBond>,
    pub pending_retires: Vec<PendingRetire>,
    pub completed_retires: Vec<CompletedRetire>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingBond {
    pub delegator: PublicKey,
    pub validator: PublicKey,
    pub amount: Amount,
    pub epoch_queued: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingRetire {
    pub delegator: PublicKey,
    pub validator: PublicKey,
    pub amount: Amount,
    pub epoch_queued: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

    pub fn total_stake(&self) -> Amount {
        self.delegations.values().sum()
    }

    pub fn validator_set(&self) -> Vec<(PublicKey, Amount)> {
        self.delegations
            .iter()
            .filter(|(_, &stake)| stake > 0)
            .map(|(&pk, &stake)| (pk, stake))
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

        let set = ledger.validator_set();
        assert_eq!(set.len(), 2);
    }
}
