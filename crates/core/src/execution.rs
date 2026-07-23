use std::collections::HashMap;

use ed25519_dalek::{Signature as DalekSignature, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::account::{Account, TokenMeta};
use crate::staking::LocationAttestation;
use crate::staking::{
    JailRecord, LocationClaimError, PendingBond, PendingRetire, StakingLedger, UnjailError,
};
use crate::transaction::{Transaction, TransactionBody};
use crate::types::{
    Amount, PublicKey, TokenId, DUST_THRESHOLD, MAX_COMMISSION_BPS, MAX_TICKER_LEN, MIN_TICKER_LEN,
    MSG_MIN_FEE,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionError {
    InvalidSignature,
    NonceMismatch { expected: u64, got: u64 },
    InsufficientBalance { required: Amount, available: Amount },
    InsufficientTokenBalance { required: Amount, available: Amount },
    TickerAlreadyExists(String),
    TickerLengthInvalid(usize),
    TokenNotFound(TokenId),
    SelfTransfer,
    ZeroAmount,
    Overflow,
    NotJailed,
    JailPeriodNotElapsed,
    NoStake,
    InsufficientSelfStake,
    CommissionTooHigh,
    NotValidator,
    BurnExceedsSupply,
    NotTokenCreator,
    InsufficientAttestations { required: usize, provided: usize },
    AttesterNotEligible(PublicKey),
    LocationOutOfBounds,
    InsufficientMessageDeposit { required: Amount, provided: Amount },
    InvalidAnchorHash,
}

impl std::fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidSignature => write!(f, "invalid signature"),
            Self::NonceMismatch { expected, got } => {
                write!(f, "nonce mismatch: expected {expected}, got {got}")
            }
            Self::InsufficientBalance {
                required,
                available,
            } => {
                write!(f, "insufficient balance: need {required}, have {available}")
            }
            Self::InsufficientTokenBalance {
                required,
                available,
            } => {
                write!(
                    f,
                    "insufficient token balance: need {required}, have {available}"
                )
            }
            Self::TickerAlreadyExists(t) => write!(f, "ticker already exists: {t}"),
            Self::TickerLengthInvalid(len) => {
                write!(
                    f,
                    "ticker length {len} not in {MIN_TICKER_LEN}..={MAX_TICKER_LEN}"
                )
            }
            Self::TokenNotFound(id) => write!(f, "token not found: {:?}", id),
            Self::SelfTransfer => write!(f, "cannot transfer to self"),
            Self::ZeroAmount => write!(f, "amount must be non-zero"),
            Self::Overflow => write!(f, "arithmetic overflow"),
            Self::NotJailed => write!(f, "validator is not jailed"),
            Self::JailPeriodNotElapsed => write!(f, "jail period has not elapsed"),
            Self::NoStake => write!(f, "validator has no stake"),
            Self::InsufficientSelfStake => write!(f, "self-bond below minimum"),
            Self::CommissionTooHigh => write!(f, "commission rate exceeds maximum"),
            Self::NotValidator => write!(f, "sender is not a validator"),
            Self::BurnExceedsSupply => write!(f, "burn amount exceeds current supply"),
            Self::NotTokenCreator => write!(f, "only the token creator can update metadata"),
            Self::InsufficientAttestations { required, provided } => {
                write!(
                    f,
                    "insufficient attestations: need {required}, got {provided}"
                )
            }
            Self::AttesterNotEligible(pk) => write!(f, "attester not eligible: {:?}", pk),
            Self::LocationOutOfBounds => write!(f, "location coordinates out of bounds"),
            Self::InsufficientMessageDeposit { required, provided } => {
                write!(
                    f,
                    "insufficient message deposit: need {required}, got {provided}"
                )
            }
            Self::InvalidAnchorHash => write!(f, "invalid anchor hash"),
        }
    }
}

impl std::error::Error for ExecutionError {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct State {
    pub accounts: HashMap<PublicKey, Account>,
    pub tokens: HashMap<TokenId, TokenMeta>,
    pub next_token_id: u32,
    pub tx_fee_pool: Amount,
    pub msg_fee_pool: Amount,
    pub bridge_fee_pool: Amount,
    pub staking: StakingLedger,
    pub current_epoch: u64,
    #[serde(default)]
    pub epoch_messages_anchored: u64,
    #[serde(default)]
    pub epoch_deliveries_completed: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EpochTransitionResult {
    pub new_validators: Vec<(PublicKey, Amount)>,
    pub fees_distributed: Amount,
    pub bonds_activated: Vec<PendingBond>,
    pub retires_completed: Vec<PendingRetire>,
    pub slashes_applied: Vec<JailRecord>,
    pub deactivated: Vec<PublicKey>,
    pub location_claims_validated: Vec<PublicKey>,
    pub messages_anchored: u64,
    pub deliveries_completed: u64,
    pub state_hash: [u8; 32],
}

impl Default for State {
    fn default() -> Self {
        Self::new()
    }
}

impl State {
    pub fn new() -> Self {
        Self {
            accounts: HashMap::new(),
            tokens: HashMap::new(),
            next_token_id: 1,
            tx_fee_pool: 0,
            msg_fee_pool: 0,
            bridge_fee_pool: 0,
            staking: StakingLedger::new(),
            current_epoch: 0,
            epoch_messages_anchored: 0,
            epoch_deliveries_completed: 0,
        }
    }

    pub fn get_or_create_account(&mut self, pubkey: &PublicKey) -> &mut Account {
        self.accounts.entry(*pubkey).or_insert_with(|| Account {
            native_balance: 0,
            token_balances: Default::default(),
            nonce: 0,
        })
    }

    pub fn get_account(&self, pubkey: &PublicKey) -> Option<&Account> {
        self.accounts.get(pubkey)
    }

    pub fn execute_block(&mut self, txs: &[Transaction]) -> Vec<Result<(), ExecutionError>> {
        txs.iter().map(|tx| self.execute_tx(tx)).collect()
    }

    pub fn execute_tx(&mut self, tx: &Transaction) -> Result<(), ExecutionError> {
        Self::verify_signature(tx)?;

        let account = self.get_or_create_account(&tx.sender);
        if account.nonce != tx.nonce {
            return Err(ExecutionError::NonceMismatch {
                expected: account.nonce,
                got: tx.nonce,
            });
        }

        if account.native_balance < tx.fee {
            return Err(ExecutionError::InsufficientBalance {
                required: tx.fee,
                available: account.native_balance,
            });
        }

        // deduct fee and bump nonce
        account.native_balance -= tx.fee;
        account.nonce += 1;
        self.tx_fee_pool += tx.fee;

        self.apply_body(tx.sender, &tx.body)
    }

    fn verify_signature(tx: &Transaction) -> Result<(), ExecutionError> {
        let vk =
            VerifyingKey::from_bytes(&tx.sender).map_err(|_| ExecutionError::InvalidSignature)?;
        let sig = DalekSignature::from_bytes(&tx.signature);
        let msg = tx.signing_hash();
        vk.verify(&msg, &sig)
            .map_err(|_| ExecutionError::InvalidSignature)
    }

    fn apply_body(
        &mut self,
        sender: PublicKey,
        body: &TransactionBody,
    ) -> Result<(), ExecutionError> {
        match body {
            TransactionBody::Transfer { to, amount } => {
                if *amount == 0 {
                    return Err(ExecutionError::ZeroAmount);
                }
                if sender == *to {
                    return Err(ExecutionError::SelfTransfer);
                }
                let sender_acc = self.accounts.get_mut(&sender).unwrap();
                if sender_acc.native_balance < *amount {
                    return Err(ExecutionError::InsufficientBalance {
                        required: *amount,
                        available: sender_acc.native_balance,
                    });
                }
                sender_acc.native_balance -= amount;
                let receiver = self.get_or_create_account(to);
                receiver.native_balance = receiver
                    .native_balance
                    .checked_add(*amount)
                    .ok_or(ExecutionError::Overflow)?;
                Ok(())
            }

            TransactionBody::TokenTransfer {
                to,
                token_id,
                amount,
            } => {
                if *amount == 0 {
                    return Err(ExecutionError::ZeroAmount);
                }
                if sender == *to {
                    return Err(ExecutionError::SelfTransfer);
                }
                if !self.tokens.contains_key(token_id) {
                    return Err(ExecutionError::TokenNotFound(*token_id));
                }
                let sender_acc = self.accounts.get_mut(&sender).unwrap();
                let sender_token_bal = sender_acc
                    .token_balances
                    .get(token_id)
                    .copied()
                    .unwrap_or(0);
                if sender_token_bal < *amount {
                    return Err(ExecutionError::InsufficientTokenBalance {
                        required: *amount,
                        available: sender_token_bal,
                    });
                }
                *sender_acc.token_balances.get_mut(token_id).unwrap() -= amount;
                let receiver = self.get_or_create_account(to);
                let recv_bal = receiver.token_balances.entry(*token_id).or_insert(0);
                *recv_bal = recv_bal
                    .checked_add(*amount)
                    .ok_or(ExecutionError::Overflow)?;
                Ok(())
            }

            TransactionBody::Mint {
                ticker,
                max_supply,
                metadata_hash,
            } => {
                if ticker.len() < MIN_TICKER_LEN || ticker.len() > MAX_TICKER_LEN {
                    return Err(ExecutionError::TickerLengthInvalid(ticker.len()));
                }
                if self.tokens.values().any(|t| t.ticker == *ticker) {
                    return Err(ExecutionError::TickerAlreadyExists(ticker.clone()));
                }
                let id = self.next_token_id.to_le_bytes();
                self.next_token_id += 1;
                self.tokens.insert(
                    id,
                    TokenMeta {
                        id,
                        ticker: ticker.clone(),
                        current_supply: *max_supply,
                        max_supply: *max_supply,
                        metadata_hash: *metadata_hash,
                        creator: sender,
                    },
                );
                let acc = self.accounts.get_mut(&sender).unwrap();
                acc.token_balances.insert(id, *max_supply);
                Ok(())
            }

            TransactionBody::Bond { validator, amount } => {
                if *amount == 0 {
                    return Err(ExecutionError::ZeroAmount);
                }
                let acc = self.accounts.get_mut(&sender).unwrap();
                if acc.native_balance < *amount {
                    return Err(ExecutionError::InsufficientBalance {
                        required: *amount,
                        available: acc.native_balance,
                    });
                }
                acc.native_balance -= amount;
                // epoch 0 as placeholder; real epoch comes from consensus context
                self.staking.queue_bond(sender, *validator, *amount, 0);
                Ok(())
            }

            TransactionBody::Retire { validator, amount } => {
                if *amount == 0 {
                    return Err(ExecutionError::ZeroAmount);
                }
                let delegated = self
                    .staking
                    .delegations
                    .get(validator)
                    .copied()
                    .unwrap_or(0);
                if delegated < *amount {
                    return Err(ExecutionError::InsufficientBalance {
                        required: *amount,
                        available: delegated,
                    });
                }
                self.staking.queue_retire(sender, *validator, *amount, 0);
                Ok(())
            }

            TransactionBody::Withdraw { validator } => {
                let amount = self.staking.withdraw(&sender, validator);
                if amount == 0 {
                    return Err(ExecutionError::ZeroAmount);
                }
                let acc = self.get_or_create_account(&sender);
                acc.native_balance = acc
                    .native_balance
                    .checked_add(amount)
                    .ok_or(ExecutionError::Overflow)?;
                Ok(())
            }

            TransactionBody::UnJail => {
                self.staking
                    .unjail(&sender, self.current_epoch)
                    .map_err(|e| match e {
                        UnjailError::NotJailed => ExecutionError::NotJailed,
                        UnjailError::JailPeriodNotElapsed => ExecutionError::JailPeriodNotElapsed,
                        UnjailError::NoStake => ExecutionError::NoStake,
                        UnjailError::InsufficientSelfStake => ExecutionError::InsufficientSelfStake,
                    })
            }

            TransactionBody::SetCommission { rate } => {
                if *rate > MAX_COMMISSION_BPS {
                    return Err(ExecutionError::CommissionTooHigh);
                }
                let has_stake = self.staking.delegations.contains_key(&sender);
                if !has_stake {
                    return Err(ExecutionError::NotValidator);
                }
                self.staking.queue_commission_change(sender, *rate);
                Ok(())
            }

            TransactionBody::Burn { token_id, amount } => {
                if *amount == 0 {
                    return Err(ExecutionError::ZeroAmount);
                }
                if !self.tokens.contains_key(token_id) {
                    return Err(ExecutionError::TokenNotFound(*token_id));
                }
                let sender_acc = self.accounts.get_mut(&sender).unwrap();
                let sender_token_bal = sender_acc
                    .token_balances
                    .get(token_id)
                    .copied()
                    .unwrap_or(0);
                if sender_token_bal < *amount {
                    return Err(ExecutionError::InsufficientTokenBalance {
                        required: *amount,
                        available: sender_token_bal,
                    });
                }
                let meta = self.tokens.get(token_id).unwrap();
                if *amount > meta.current_supply {
                    return Err(ExecutionError::BurnExceedsSupply);
                }
                *sender_acc.token_balances.get_mut(token_id).unwrap() -= amount;
                if *sender_acc.token_balances.get(token_id).unwrap() == 0 {
                    sender_acc.token_balances.remove(token_id);
                }
                self.tokens.get_mut(token_id).unwrap().current_supply -= amount;
                Ok(())
            }

            TransactionBody::UpdateMetadata {
                token_id,
                metadata_hash,
            } => {
                let meta = self
                    .tokens
                    .get(token_id)
                    .ok_or(ExecutionError::TokenNotFound(*token_id))?;
                if meta.creator != sender {
                    return Err(ExecutionError::NotTokenCreator);
                }
                self.tokens.get_mut(token_id).unwrap().metadata_hash = *metadata_hash;
                Ok(())
            }

            TransactionBody::LocationClaim {
                lat,
                lon,
                attestations,
            } => {
                let staking_attestations: Vec<LocationAttestation> = attestations
                    .iter()
                    .map(|a| LocationAttestation {
                        attester: a.attester,
                        propagation_delay_ms: a.propagation_delay_ms,
                        signature: a.signature,
                    })
                    .collect();

                self.staking
                    .submit_location_claim(
                        sender,
                        *lat,
                        *lon,
                        staking_attestations,
                        self.current_epoch,
                    )
                    .map_err(|e| match e {
                        LocationClaimError::InsufficientAttestations { required, provided } => {
                            ExecutionError::InsufficientAttestations { required, provided }
                        }
                        LocationClaimError::AttesterNotEligible(pk) => {
                            ExecutionError::AttesterNotEligible(pk)
                        }
                        LocationClaimError::NotStaked => ExecutionError::NotValidator,
                        LocationClaimError::LocationOutOfBounds => {
                            ExecutionError::LocationOutOfBounds
                        }
                    })
            }

            TransactionBody::MessageAnchor {
                destination: _,
                length_proof: _,
                deposit,
            } => {
                if *deposit < MSG_MIN_FEE {
                    return Err(ExecutionError::InsufficientMessageDeposit {
                        required: MSG_MIN_FEE,
                        provided: *deposit,
                    });
                }
                let acc = self.accounts.get_mut(&sender).unwrap();
                if acc.native_balance < *deposit {
                    return Err(ExecutionError::InsufficientBalance {
                        required: *deposit,
                        available: acc.native_balance,
                    });
                }
                acc.native_balance -= deposit;
                self.msg_fee_pool += deposit;
                self.epoch_messages_anchored += 1;
                Ok(())
            }

            TransactionBody::DeliveryWrapup {
                anchor_hash: _,
                relay_receipts,
                destination_ack: _,
            } => {
                for receipt in relay_receipts {
                    if self.staking.delegations.contains_key(&receipt.relay_node) {
                        self.staking.record_relay_participation(receipt.relay_node);
                    }
                }
                if self.staking.delegations.contains_key(&sender) {
                    self.staking.record_relay_participation(sender);
                }
                self.epoch_deliveries_completed += 1;
                Ok(())
            }
        }
    }

    pub fn process_epoch_transition(&mut self, completed_epoch: u64) -> EpochTransitionResult {
        let current_epoch = completed_epoch + 1;
        self.current_epoch = current_epoch;

        // 1. process slashes
        let slashes_applied = self.staking.process_slashes(current_epoch);

        // 2. activate pending bonds
        let bonds_activated = self.staking.activate_pending_bonds(current_epoch);

        // 3. complete retirements
        let retires_completed = self.staking.complete_pending_retires(current_epoch);

        // 4. apply pending commission changes
        self.staking.apply_commission_changes();

        // 4b. process location claims
        let location_claims_validated = self.staking.process_location_claims(current_epoch);

        // 5. drain relay participants before distribution
        let relay_participants = self.staking.drain_relay_participants();

        // 5a. capture and reset epoch counters
        let messages_anchored = self.epoch_messages_anchored;
        let deliveries_completed = self.epoch_deliveries_completed;
        self.epoch_messages_anchored = 0;
        self.epoch_deliveries_completed = 0;

        // 5b. distribute fee pools with commission
        let total_stake = self.staking.total_active_stake();
        let mut pool_distributed = [0u64; 3];

        if total_stake > 0 {
            let active_validators: Vec<(PublicKey, Amount)> =
                self.staking.validator_set().into_iter().collect();

            // Helper closure to distribute a pool to a set of eligible validators
            let distribute_pool = |state_accounts: &mut HashMap<PublicKey, Account>,
                                   staking: &StakingLedger,
                                   pool_amount: Amount,
                                   eligible: &[(PublicKey, Amount)]|
             -> Amount {
                if pool_amount == 0 || eligible.is_empty() {
                    return 0;
                }
                let eligible_stake: Amount = eligible.iter().map(|(_, s)| *s).sum();
                if eligible_stake == 0 {
                    return 0;
                }
                let mut distributed = 0u64;
                for (validator, stake) in eligible {
                    let gross_share =
                        (pool_amount as u128 * *stake as u128 / eligible_stake as u128) as Amount;
                    if gross_share < DUST_THRESHOLD {
                        continue;
                    }

                    let commission_rate = staking
                        .commission_rates
                        .get(validator)
                        .copied()
                        .unwrap_or(0) as u128;
                    let commission_amount =
                        (gross_share as u128 * commission_rate / 10000) as Amount;
                    let delegator_pool = gross_share - commission_amount;

                    if commission_amount >= DUST_THRESHOLD {
                        state_accounts
                            .entry(*validator)
                            .or_insert_with(|| Account {
                                native_balance: 0,
                                token_balances: Default::default(),
                                nonce: 0,
                            })
                            .native_balance += commission_amount;
                        distributed += commission_amount;
                    }

                    let delegators = staking.delegators_of(validator);
                    let validator_total_delegated: Amount =
                        delegators.iter().map(|(_, a)| *a).sum();

                    if validator_total_delegated > 0 && delegator_pool > 0 {
                        for (delegator, del_amount) in &delegators {
                            let del_share = (delegator_pool as u128 * *del_amount as u128
                                / validator_total_delegated as u128)
                                as Amount;
                            if del_share >= DUST_THRESHOLD {
                                state_accounts
                                    .entry(*delegator)
                                    .or_insert_with(|| Account {
                                        native_balance: 0,
                                        token_balances: Default::default(),
                                        nonce: 0,
                                    })
                                    .native_balance += del_share;
                                distributed += del_share;
                            }
                        }
                    } else if delegator_pool >= DUST_THRESHOLD {
                        state_accounts
                            .entry(*validator)
                            .or_insert_with(|| Account {
                                native_balance: 0,
                                token_balances: Default::default(),
                                nonce: 0,
                            })
                            .native_balance += delegator_pool;
                        distributed += delegator_pool;
                    }
                }
                distributed
            };

            // tx_fee_pool: all active validators
            pool_distributed[0] = distribute_pool(
                &mut self.accounts,
                &self.staking,
                self.tx_fee_pool,
                &active_validators,
            );

            // msg_fee_pool: only active validators who participated in relays
            let msg_eligible: Vec<(PublicKey, Amount)> = active_validators
                .iter()
                .filter(|(pk, _)| relay_participants.contains(pk))
                .copied()
                .collect();
            pool_distributed[1] = distribute_pool(
                &mut self.accounts,
                &self.staking,
                self.msg_fee_pool,
                &msg_eligible,
            );

            // bridge_fee_pool: all active validators (bridge eligibility deferred to BCert impl)
            pool_distributed[2] = distribute_pool(
                &mut self.accounts,
                &self.staking,
                self.bridge_fee_pool,
                &active_validators,
            );
        }

        let distributed = pool_distributed[0];
        self.tx_fee_pool -= pool_distributed[0];
        self.msg_fee_pool -= pool_distributed[1];
        self.bridge_fee_pool -= pool_distributed[2];

        // 6. collect deactivated validators (below MIN_SELF_STAKE)
        let deactivated = self.staking.validators_below_min_self_stake();

        // 7. derive new validator set
        let new_validators = self.staking.validator_set();

        // 8. compute state hash
        let state_hash = self.compute_state_hash();

        EpochTransitionResult {
            new_validators,
            fees_distributed: distributed,
            bonds_activated,
            retires_completed,
            slashes_applied,
            deactivated,
            location_claims_validated,
            messages_anchored,
            deliveries_completed,
            state_hash,
        }
    }

    pub fn compute_state_hash(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();

        // accounts — sort by key for determinism
        let mut accounts: Vec<_> = self.accounts.iter().collect();
        accounts.sort_by_key(|(k, _)| *k);
        for (pk, acc) in &accounts {
            hasher.update(*pk);
            hasher.update(acc.native_balance.to_le_bytes());
            hasher.update(acc.nonce.to_le_bytes());
            // token_balances is BTreeMap so already sorted
            for (tid, bal) in &acc.token_balances {
                hasher.update(tid);
                hasher.update(bal.to_le_bytes());
            }
        }

        // tokens — sort by key
        let mut tokens: Vec<_> = self.tokens.iter().collect();
        tokens.sort_by_key(|(k, _)| *k);
        for (tid, meta) in &tokens {
            hasher.update(*tid);
            hasher.update(meta.ticker.as_bytes());
            hasher.update(meta.current_supply.to_le_bytes());
            hasher.update(meta.max_supply.to_le_bytes());
            hasher.update(meta.metadata_hash);
            hasher.update(meta.creator);
        }

        hasher.update(self.next_token_id.to_le_bytes());
        hasher.update(self.tx_fee_pool.to_le_bytes());
        hasher.update(self.msg_fee_pool.to_le_bytes());
        hasher.update(self.bridge_fee_pool.to_le_bytes());
        hasher.update(self.current_epoch.to_le_bytes());

        // staking delegations — sort by key
        let mut delegations: Vec<_> = self.staking.delegations.iter().collect();
        delegations.sort_by_key(|(k, _)| *k);
        for (pk, amount) in &delegations {
            hasher.update(*pk);
            hasher.update(amount.to_le_bytes());
        }

        // jailed — sort by key
        let mut jailed: Vec<_> = self.staking.jailed.iter().collect();
        jailed.sort_by_key(|(k, _)| *k);
        for (pk, record) in &jailed {
            hasher.update(*pk);
            hasher.update(record.epoch_jailed.to_le_bytes());
            hasher.update(record.amount_slashed.to_le_bytes());
        }

        // self_bonds — sort by key
        let mut self_bonds: Vec<_> = self.staking.self_bonds.iter().collect();
        self_bonds.sort_by_key(|(k, _)| *k);
        for (pk, amount) in &self_bonds {
            hasher.update(*pk);
            hasher.update(amount.to_le_bytes());
        }

        // commission_rates — sort by key
        let mut commissions: Vec<_> = self.staking.commission_rates.iter().collect();
        commissions.sort_by_key(|(k, _)| *k);
        for (pk, rate) in &commissions {
            hasher.update(*pk);
            hasher.update(rate.to_le_bytes());
        }

        // delegator_stakes — sort by key for determinism
        let mut del_stakes: Vec<_> = self.staking.delegator_stakes.iter().collect();
        del_stakes.sort_by_key(|(k, _)| *k);
        for ((delegator, validator), amount) in &del_stakes {
            hasher.update(delegator);
            hasher.update(validator);
            hasher.update(amount.to_le_bytes());
        }

        // validated_locations — sort by key
        let mut locations: Vec<_> = self.staking.validated_locations.iter().collect();
        locations.sort_by_key(|(k, _)| *k);
        for (pk, loc) in &locations {
            hasher.update(*pk);
            hasher.update(loc.lat.to_le_bytes());
            hasher.update(loc.lon.to_le_bytes());
            hasher.update(loc.epoch_validated.to_le_bytes());
            hasher.update(loc.attestation_count.to_le_bytes());
        }

        // pending_location_claims
        for (claim, attestations) in &self.staking.pending_location_claims {
            hasher.update(claim.validator);
            hasher.update(claim.lat.to_le_bytes());
            hasher.update(claim.lon.to_le_bytes());
            hasher.update(claim.epoch_claimed.to_le_bytes());
            for att in attestations {
                hasher.update(att.attester);
                hasher.update(att.propagation_delay_ms.to_le_bytes());
                hasher.update(att.signature);
            }
        }

        // msg_relay_participants — sort for determinism
        let mut relay_parts: Vec<_> = self
            .staking
            .msg_relay_participants
            .iter()
            .copied()
            .collect();
        relay_parts.sort();
        for pk in &relay_parts {
            hasher.update(pk);
        }

        // epoch messaging counters
        hasher.update(self.epoch_messages_anchored.to_le_bytes());
        hasher.update(self.epoch_deliveries_completed.to_le_bytes());

        // pending commission changes
        for (pk, rate) in &self.staking.pending_commission_changes {
            hasher.update(pk);
            hasher.update(rate.to_le_bytes());
        }

        // pending bonds
        for bond in &self.staking.pending_bonds {
            hasher.update(bond.delegator);
            hasher.update(bond.validator);
            hasher.update(bond.amount.to_le_bytes());
            hasher.update(bond.epoch_queued.to_le_bytes());
        }

        // pending retires
        for retire in &self.staking.pending_retires {
            hasher.update(retire.delegator);
            hasher.update(retire.validator);
            hasher.update(retire.amount.to_le_bytes());
            hasher.update(retire.epoch_queued.to_le_bytes());
        }

        // completed retires
        for retire in &self.staking.completed_retires {
            hasher.update(retire.delegator);
            hasher.update(retire.validator);
            hasher.update(retire.amount.to_le_bytes());
            hasher.update(retire.epoch_completed.to_le_bytes());
        }

        hasher.finalize().into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::MIN_SELF_STAKE;
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;

    fn make_keypair() -> (SigningKey, PublicKey) {
        let sk = SigningKey::generate(&mut OsRng);
        let pk: PublicKey = sk.verifying_key().to_bytes();
        (sk, pk)
    }

    fn sign_tx(sk: &SigningKey, tx: &mut Transaction) {
        let hash = tx.signing_hash();
        let sig = sk.sign(&hash);
        tx.signature = sig.to_bytes();
    }

    fn funded_state(pk: &PublicKey, balance: Amount) -> State {
        let mut state = State::new();
        state.get_or_create_account(pk).native_balance = balance;
        state
    }

    #[test]
    fn transfer_basic() {
        let (sk_a, pk_a) = make_keypair();
        let (_, pk_b) = make_keypair();
        let mut state = funded_state(&pk_a, 1_000);

        let mut tx = Transaction {
            sender: pk_a,
            nonce: 0,
            fee: 10,
            body: TransactionBody::Transfer {
                to: pk_b,
                amount: 100,
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk_a, &mut tx);
        state.execute_tx(&tx).unwrap();

        assert_eq!(state.get_account(&pk_a).unwrap().native_balance, 890);
        assert_eq!(state.get_account(&pk_b).unwrap().native_balance, 100);
        assert_eq!(state.tx_fee_pool, 10);
        assert_eq!(state.get_account(&pk_a).unwrap().nonce, 1);
    }

    #[test]
    fn transfer_insufficient_balance() {
        let (sk_a, pk_a) = make_keypair();
        let (_, pk_b) = make_keypair();
        let mut state = funded_state(&pk_a, 50);

        let mut tx = Transaction {
            sender: pk_a,
            nonce: 0,
            fee: 10,
            body: TransactionBody::Transfer {
                to: pk_b,
                amount: 100,
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk_a, &mut tx);
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(err, ExecutionError::InsufficientBalance { .. }));
    }

    #[test]
    fn transfer_self() {
        let (sk_a, pk_a) = make_keypair();
        let mut state = funded_state(&pk_a, 1_000);

        let mut tx = Transaction {
            sender: pk_a,
            nonce: 0,
            fee: 10,
            body: TransactionBody::Transfer {
                to: pk_a,
                amount: 100,
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk_a, &mut tx);
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(err, ExecutionError::SelfTransfer));
    }

    #[test]
    fn transfer_zero_amount() {
        let (sk_a, pk_a) = make_keypair();
        let (_, pk_b) = make_keypair();
        let mut state = funded_state(&pk_a, 1_000);

        let mut tx = Transaction {
            sender: pk_a,
            nonce: 0,
            fee: 10,
            body: TransactionBody::Transfer {
                to: pk_b,
                amount: 0,
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk_a, &mut tx);
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(err, ExecutionError::ZeroAmount));
    }

    #[test]
    fn nonce_mismatch() {
        let (sk_a, pk_a) = make_keypair();
        let (_, pk_b) = make_keypair();
        let mut state = funded_state(&pk_a, 1_000);

        let mut tx = Transaction {
            sender: pk_a,
            nonce: 5,
            fee: 10,
            body: TransactionBody::Transfer {
                to: pk_b,
                amount: 100,
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk_a, &mut tx);
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(
            err,
            ExecutionError::NonceMismatch {
                expected: 0,
                got: 5
            }
        ));
    }

    #[test]
    fn fee_deduction() {
        let (sk_a, pk_a) = make_keypair();
        let (_, pk_b) = make_keypair();
        let mut state = funded_state(&pk_a, 100);

        let mut tx = Transaction {
            sender: pk_a,
            nonce: 0,
            fee: 25,
            body: TransactionBody::Transfer {
                to: pk_b,
                amount: 50,
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk_a, &mut tx);
        state.execute_tx(&tx).unwrap();

        assert_eq!(state.get_account(&pk_a).unwrap().native_balance, 25);
        assert_eq!(state.tx_fee_pool, 25);
    }

    #[test]
    fn mint_creates_token_and_credits() {
        let (sk_a, pk_a) = make_keypair();
        let mut state = funded_state(&pk_a, 1_000);

        let mut tx = Transaction {
            sender: pk_a,
            nonce: 0,
            fee: 10,
            body: TransactionBody::Mint {
                ticker: "BNK".to_string(),
                max_supply: 1_000_000,
                metadata_hash: [0xAB; 32],
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk_a, &mut tx);
        state.execute_tx(&tx).unwrap();

        assert_eq!(state.tokens.len(), 1);
        let token_id = 1u32.to_le_bytes();
        let meta = state.tokens.get(&token_id).unwrap();
        assert_eq!(meta.ticker, "BNK");
        assert_eq!(meta.max_supply, 1_000_000);
        assert_eq!(meta.creator, pk_a);

        let acc = state.get_account(&pk_a).unwrap();
        assert_eq!(*acc.token_balances.get(&token_id).unwrap(), 1_000_000);
    }

    #[test]
    fn token_transfer() {
        let (sk_a, pk_a) = make_keypair();
        let (_, pk_b) = make_keypair();
        let mut state = funded_state(&pk_a, 1_000);

        // mint first
        let mut mint_tx = Transaction {
            sender: pk_a,
            nonce: 0,
            fee: 10,
            body: TransactionBody::Mint {
                ticker: "TKN".to_string(),
                max_supply: 500,
                metadata_hash: [0; 32],
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk_a, &mut mint_tx);
        state.execute_tx(&mint_tx).unwrap();

        let token_id = 1u32.to_le_bytes();

        let mut transfer_tx = Transaction {
            sender: pk_a,
            nonce: 1,
            fee: 10,
            body: TransactionBody::TokenTransfer {
                to: pk_b,
                token_id,
                amount: 200,
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk_a, &mut transfer_tx);
        state.execute_tx(&transfer_tx).unwrap();

        assert_eq!(
            *state
                .get_account(&pk_a)
                .unwrap()
                .token_balances
                .get(&token_id)
                .unwrap(),
            300
        );
        assert_eq!(
            *state
                .get_account(&pk_b)
                .unwrap()
                .token_balances
                .get(&token_id)
                .unwrap(),
            200
        );
    }

    #[test]
    fn bond_deducts_balance() {
        let (sk_a, pk_a) = make_keypair();
        let (_, validator) = make_keypair();
        let mut state = funded_state(&pk_a, 1_000);

        let mut tx = Transaction {
            sender: pk_a,
            nonce: 0,
            fee: 10,
            body: TransactionBody::Bond {
                validator,
                amount: 500,
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk_a, &mut tx);
        state.execute_tx(&tx).unwrap();

        assert_eq!(state.get_account(&pk_a).unwrap().native_balance, 490);
    }

    #[test]
    fn invalid_signature() {
        let (_, pk_a) = make_keypair();
        let (_, pk_b) = make_keypair();
        let mut state = funded_state(&pk_a, 1_000);

        let tx = Transaction {
            sender: pk_a,
            nonce: 0,
            fee: 10,
            body: TransactionBody::Transfer {
                to: pk_b,
                amount: 100,
            },
            signature: [0xFF; 64],
        };
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(err, ExecutionError::InvalidSignature));
    }

    #[test]
    fn execute_block_skips_invalid() {
        let (sk_a, pk_a) = make_keypair();
        let (_, pk_b) = make_keypair();
        let mut state = funded_state(&pk_a, 1_000);

        let mut good_tx = Transaction {
            sender: pk_a,
            nonce: 0,
            fee: 10,
            body: TransactionBody::Transfer {
                to: pk_b,
                amount: 100,
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk_a, &mut good_tx);

        let bad_tx = Transaction {
            sender: pk_a,
            nonce: 99,
            fee: 10,
            body: TransactionBody::Transfer {
                to: pk_b,
                amount: 100,
            },
            signature: [0xFF; 64],
        };

        let results = state.execute_block(&[good_tx, bad_tx]);
        assert!(results[0].is_ok());
        assert!(results[1].is_err());

        assert_eq!(state.get_account(&pk_a).unwrap().native_balance, 890);
        assert_eq!(state.get_account(&pk_b).unwrap().native_balance, 100);
    }

    #[test]
    fn process_epoch_transition_distributes_fees() {
        let mut state = State::new();

        let validator_a: PublicKey = [1u8; 32];
        let validator_b: PublicKey = [2u8; 32];

        state.staking.delegations.insert(validator_a, 750);
        state.staking.delegations.insert(validator_b, 250);
        state.staking.self_bonds.insert(validator_a, MIN_SELF_STAKE);
        state.staking.self_bonds.insert(validator_b, MIN_SELF_STAKE);
        state.tx_fee_pool = 1000;

        let result = state.process_epoch_transition(0);

        assert_eq!(state.get_account(&validator_a).unwrap().native_balance, 750);
        assert_eq!(state.get_account(&validator_b).unwrap().native_balance, 250);
        assert_eq!(result.fees_distributed, 1000);
        assert_eq!(state.tx_fee_pool, 0);
    }

    #[test]
    fn process_epoch_transition_activates_bonds() {
        let mut state = State::new();

        let delegator: PublicKey = [1u8; 32];
        let validator: PublicKey = [2u8; 32];

        // queue a bond at epoch 0
        state.staking.queue_bond(delegator, validator, 500, 0);
        assert!(state.staking.delegations.is_empty());

        // epoch 0 -> 1 transition: bond should activate (ACTIVATION_DELAY = 1)
        let result = state.process_epoch_transition(0);
        assert_eq!(result.bonds_activated.len(), 1);
        assert_eq!(*state.staking.delegations.get(&validator).unwrap(), 500);
    }

    #[test]
    fn process_epoch_transition_no_fees_if_no_stake() {
        let mut state = State::new();
        state.tx_fee_pool = 1000;

        let result = state.process_epoch_transition(0);
        assert_eq!(result.fees_distributed, 0);
        assert_eq!(state.tx_fee_pool, 1000);
    }

    #[test]
    fn epoch_transition_processes_slashes() {
        use crate::staking::{SlashOffenceKind, SlashingEvent};

        let mut state = State::new();
        let validator: PublicKey = [1u8; 32];
        state.staking.delegations.insert(validator, 1000);
        state.staking.self_bonds.insert(validator, MIN_SELF_STAKE);

        state.staking.report_offence(SlashingEvent {
            validator,
            offence: SlashOffenceKind::DoubleVote,
            epoch: 0,
        });

        let result = state.process_epoch_transition(0);
        assert_eq!(result.slashes_applied.len(), 1);
        // 100% slash: all 1000 burned
        assert_eq!(result.slashes_applied[0].amount_slashed, 1000);
        assert!(!state.staking.delegations.contains_key(&validator));
        assert!(state.staking.jailed.contains_key(&validator));
    }

    #[test]
    fn unjail_transaction() {
        use crate::staking::{SlashOffenceKind, SlashingEvent};

        let (sk, pk) = make_keypair();
        let mut state = funded_state(&pk, 10_000);

        state.staking.delegations.insert(pk, 2 * MIN_SELF_STAKE);
        state.staking.self_bonds.insert(pk, 2 * MIN_SELF_STAKE);

        // Use Downtime: 0% slash, 1-epoch jail
        state.staking.report_offence(SlashingEvent {
            validator: pk,
            offence: SlashOffenceKind::Downtime,
            epoch: 0,
        });
        state.staking.process_slashes(0);
        assert!(state.staking.jailed.contains_key(&pk));

        // unjail too early — jailed at epoch 0, need epoch >= 1
        state.current_epoch = 0;
        let mut tx = Transaction {
            sender: pk,
            nonce: 0,
            fee: 1,
            body: TransactionBody::UnJail,
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(err, ExecutionError::JailPeriodNotElapsed));

        // unjail succeeds at epoch 1
        state.current_epoch = 1;
        let mut tx = Transaction {
            sender: pk,
            nonce: 1,
            fee: 1,
            body: TransactionBody::UnJail,
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        state.execute_tx(&tx).unwrap();
        assert!(!state.staking.jailed.contains_key(&pk));
    }

    #[test]
    fn state_hash_deterministic() {
        let mut state = State::new();
        let v: PublicKey = [1u8; 32];
        state.staking.delegations.insert(v, 500);
        state.tx_fee_pool = 100;

        let h1 = state.compute_state_hash();
        let h2 = state.compute_state_hash();
        assert_eq!(h1, h2);
    }

    #[test]
    fn state_hash_changes() {
        let mut state = State::new();
        let v: PublicKey = [1u8; 32];
        state.staking.delegations.insert(v, 500);

        let h1 = state.compute_state_hash();
        state.tx_fee_pool += 1;
        let h2 = state.compute_state_hash();
        assert_ne!(h1, h2);
    }

    #[test]
    fn fee_distribution_excludes_jailed() {
        use crate::staking::{SlashOffenceKind, SlashingEvent};

        let mut state = State::new();
        let validator_a: PublicKey = [1u8; 32];
        let validator_b: PublicKey = [2u8; 32];

        state.staking.delegations.insert(validator_a, 500);
        state.staking.delegations.insert(validator_b, 500);
        state.staking.self_bonds.insert(validator_a, MIN_SELF_STAKE);
        state.staking.self_bonds.insert(validator_b, MIN_SELF_STAKE);
        state.tx_fee_pool = 1000;

        state.staking.report_offence(SlashingEvent {
            validator: validator_a,
            offence: SlashOffenceKind::DoubleVote,
            epoch: 0,
        });

        let result = state.process_epoch_transition(0);

        let a_balance = state
            .get_account(&validator_a)
            .map(|a| a.native_balance)
            .unwrap_or(0);
        let b_balance = state.get_account(&validator_b).unwrap().native_balance;
        assert_eq!(a_balance, 0);
        assert_eq!(b_balance, 1000);
        assert_eq!(result.fees_distributed, 1000);
    }

    #[test]
    fn self_bond_tracked_on_activation() {
        let mut state = State::new();
        let validator: PublicKey = [1u8; 32];

        state
            .staking
            .queue_bond(validator, validator, MIN_SELF_STAKE, 0);
        state.process_epoch_transition(0);

        assert_eq!(
            state
                .staking
                .self_bonds
                .get(&validator)
                .copied()
                .unwrap_or(0),
            MIN_SELF_STAKE
        );
        assert_eq!(
            state
                .staking
                .delegations
                .get(&validator)
                .copied()
                .unwrap_or(0),
            MIN_SELF_STAKE
        );
    }

    #[test]
    fn delegated_bond_not_tracked_as_self() {
        let mut state = State::new();
        let delegator: PublicKey = [1u8; 32];
        let validator: PublicKey = [2u8; 32];

        state.staking.queue_bond(delegator, validator, 5000, 0);
        state.process_epoch_transition(0);

        assert_eq!(
            state
                .staking
                .self_bonds
                .get(&validator)
                .copied()
                .unwrap_or(0),
            0
        );
        assert_eq!(
            state
                .staking
                .delegations
                .get(&validator)
                .copied()
                .unwrap_or(0),
            5000
        );
    }

    #[test]
    fn validator_set_excludes_below_min_self_stake() {
        let mut state = State::new();
        let v1: PublicKey = [1u8; 32];
        let v2: PublicKey = [2u8; 32];

        state.staking.delegations.insert(v1, 500);
        state.staking.delegations.insert(v2, 500);
        state.staking.self_bonds.insert(v1, MIN_SELF_STAKE);
        // v2 has no self_bonds

        let set = state.staking.validator_set();
        assert_eq!(set.len(), 1);
        assert_eq!(set[0].0, v1);
    }

    #[test]
    fn commission_change_applied_at_epoch() {
        let mut state = State::new();
        let validator: PublicKey = [1u8; 32];
        state.staking.delegations.insert(validator, MIN_SELF_STAKE);
        state.staking.self_bonds.insert(validator, MIN_SELF_STAKE);

        state.staking.queue_commission_change(validator, 500);
        assert!(!state.staking.commission_rates.contains_key(&validator));

        state.process_epoch_transition(0);
        assert_eq!(
            state
                .staking
                .commission_rates
                .get(&validator)
                .copied()
                .unwrap(),
            500
        );
    }

    #[test]
    fn slash_reduces_self_bonds_proportionally() {
        use crate::staking::{SlashOffenceKind, SlashingEvent};

        let mut ledger = StakingLedger::new();
        let validator: PublicKey = [1u8; 32];
        ledger.delegations.insert(validator, 2000);
        ledger.self_bonds.insert(validator, 1000);

        ledger.report_offence(SlashingEvent {
            validator,
            offence: SlashOffenceKind::DoubleVote,
            epoch: 0,
        });

        let records = ledger.process_slashes(0);
        assert_eq!(records.len(), 1);
        // 100% slash: all 2000 delegation and all 1000 self-bond burned
        assert_eq!(records[0].amount_slashed, 2000);
        assert!(!ledger.self_bonds.contains_key(&validator));
        assert!(!ledger.delegations.contains_key(&validator));
    }

    #[test]
    fn set_commission_tx() {
        let (sk, pk) = make_keypair();
        let mut state = funded_state(&pk, 10_000);
        state.staking.delegations.insert(pk, MIN_SELF_STAKE);
        state.staking.self_bonds.insert(pk, MIN_SELF_STAKE);

        let mut tx = Transaction {
            sender: pk,
            nonce: 0,
            fee: 1,
            body: TransactionBody::SetCommission { rate: 1000 },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        state.execute_tx(&tx).unwrap();

        assert_eq!(state.staking.pending_commission_changes.len(), 1);
        assert_eq!(state.staking.pending_commission_changes[0], (pk, 1000));
    }

    #[test]
    fn set_commission_rejects_above_max() {
        let (sk, pk) = make_keypair();
        let mut state = funded_state(&pk, 10_000);
        state.staking.delegations.insert(pk, MIN_SELF_STAKE);

        let mut tx = Transaction {
            sender: pk,
            nonce: 0,
            fee: 1,
            body: TransactionBody::SetCommission {
                rate: MAX_COMMISSION_BPS + 1,
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(err, ExecutionError::CommissionTooHigh));
    }

    #[test]
    fn dust_threshold_skips_tiny_rewards() {
        let mut state = State::new();
        let v1: PublicKey = [1u8; 32];
        let v2: PublicKey = [2u8; 32];

        // v1 has 999x the delegation of v2
        state.staking.delegations.insert(v1, 999);
        state.staking.delegations.insert(v2, 1);
        state.staking.self_bonds.insert(v1, MIN_SELF_STAKE);
        state.staking.self_bonds.insert(v2, MIN_SELF_STAKE);
        state.tx_fee_pool = 999;

        let result = state.process_epoch_transition(0);

        // v1 share = 999 * 999 / 1000 = 998 (>= DUST_THRESHOLD)
        // v2 share = 999 * 1 / 1000 = 0 (< DUST_THRESHOLD, skipped)
        assert_eq!(result.fees_distributed, 998);
        assert_eq!(state.get_account(&v1).unwrap().native_balance, 998);
        assert_eq!(
            state
                .get_account(&v2)
                .map(|a| a.native_balance)
                .unwrap_or(0),
            0
        );
    }

    #[test]
    fn three_fee_pools_all_distributed() {
        let mut state = State::new();
        let validator: PublicKey = [1u8; 32];

        state.staking.delegations.insert(validator, MIN_SELF_STAKE);
        state.staking.self_bonds.insert(validator, MIN_SELF_STAKE);
        state
            .staking
            .delegator_stakes
            .insert((validator, validator), MIN_SELF_STAKE);
        state.tx_fee_pool = 100;
        state.msg_fee_pool = 200;
        state.bridge_fee_pool = 300;
        // validator must have relay participation to receive msg_fee_pool
        state.staking.record_relay_participation(validator);

        let result = state.process_epoch_transition(0);

        // all pools distributed
        assert_eq!(result.fees_distributed, 100);
        assert_eq!(state.tx_fee_pool, 0);
        assert_eq!(state.msg_fee_pool, 0);
        assert_eq!(state.bridge_fee_pool, 0);
        assert_eq!(state.get_account(&validator).unwrap().native_balance, 600);
    }

    #[test]
    fn retire_insufficient_delegation() {
        let (sk, pk) = make_keypair();
        let (_, validator) = make_keypair();
        let mut state = funded_state(&pk, 10_000);
        state.staking.delegations.insert(validator, 100);

        let mut tx = Transaction {
            sender: pk,
            nonce: 0,
            fee: 1,
            body: TransactionBody::Retire {
                validator,
                amount: 500,
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(err, ExecutionError::InsufficientBalance { .. }));
    }

    #[test]
    fn retire_zero_amount() {
        let (sk, pk) = make_keypair();
        let (_, validator) = make_keypair();
        let mut state = funded_state(&pk, 10_000);

        let mut tx = Transaction {
            sender: pk,
            nonce: 0,
            fee: 1,
            body: TransactionBody::Retire {
                validator,
                amount: 0,
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(err, ExecutionError::ZeroAmount));
    }

    #[test]
    fn bond_zero_amount() {
        let (sk, pk) = make_keypair();
        let (_, validator) = make_keypair();
        let mut state = funded_state(&pk, 10_000);

        let mut tx = Transaction {
            sender: pk,
            nonce: 0,
            fee: 1,
            body: TransactionBody::Bond {
                validator,
                amount: 0,
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(err, ExecutionError::ZeroAmount));
    }

    #[test]
    fn bond_insufficient_balance() {
        let (sk, pk) = make_keypair();
        let (_, validator) = make_keypair();
        let mut state = funded_state(&pk, 50);

        let mut tx = Transaction {
            sender: pk,
            nonce: 0,
            fee: 10,
            body: TransactionBody::Bond {
                validator,
                amount: 100,
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(err, ExecutionError::InsufficientBalance { .. }));
    }

    #[test]
    fn withdraw_nothing_available() {
        let (sk, pk) = make_keypair();
        let (_, validator) = make_keypair();
        let mut state = funded_state(&pk, 10_000);

        let mut tx = Transaction {
            sender: pk,
            nonce: 0,
            fee: 1,
            body: TransactionBody::Withdraw { validator },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(err, ExecutionError::ZeroAmount));
    }

    #[test]
    fn withdraw_credits_balance() {
        let (sk, pk) = make_keypair();
        let (_, validator) = make_keypair();
        let mut state = funded_state(&pk, 10_000);

        state
            .staking
            .completed_retires
            .push(crate::staking::CompletedRetire {
                delegator: pk,
                validator,
                amount: 500,
                epoch_completed: 0,
            });

        let mut tx = Transaction {
            sender: pk,
            nonce: 0,
            fee: 1,
            body: TransactionBody::Withdraw { validator },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        state.execute_tx(&tx).unwrap();

        assert_eq!(state.get_account(&pk).unwrap().native_balance, 10_499); // 10000 - 1 fee + 500
    }

    #[test]
    fn mint_ticker_too_short() {
        let (sk, pk) = make_keypair();
        let mut state = funded_state(&pk, 10_000);

        let mut tx = Transaction {
            sender: pk,
            nonce: 0,
            fee: 1,
            body: TransactionBody::Mint {
                ticker: "AB".into(),
                max_supply: 100,
                metadata_hash: [0; 32],
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(err, ExecutionError::TickerLengthInvalid(2)));
    }

    #[test]
    fn mint_ticker_too_long() {
        let (sk, pk) = make_keypair();
        let mut state = funded_state(&pk, 10_000);

        let mut tx = Transaction {
            sender: pk,
            nonce: 0,
            fee: 1,
            body: TransactionBody::Mint {
                ticker: "TOOLONGTK".into(),
                max_supply: 100,
                metadata_hash: [0; 32],
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(err, ExecutionError::TickerLengthInvalid(_)));
    }

    #[test]
    fn mint_duplicate_ticker() {
        let (sk, pk) = make_keypair();
        let mut state = funded_state(&pk, 10_000);

        let mut tx1 = Transaction {
            sender: pk,
            nonce: 0,
            fee: 1,
            body: TransactionBody::Mint {
                ticker: "BNK".into(),
                max_supply: 100,
                metadata_hash: [0; 32],
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx1);
        state.execute_tx(&tx1).unwrap();

        let mut tx2 = Transaction {
            sender: pk,
            nonce: 1,
            fee: 1,
            body: TransactionBody::Mint {
                ticker: "BNK".into(),
                max_supply: 200,
                metadata_hash: [1; 32],
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx2);
        let err = state.execute_tx(&tx2).unwrap_err();
        assert!(matches!(err, ExecutionError::TickerAlreadyExists(_)));
    }

    #[test]
    fn token_transfer_nonexistent_token() {
        let (sk, pk) = make_keypair();
        let (_, pk_b) = make_keypair();
        let mut state = funded_state(&pk, 10_000);

        let mut tx = Transaction {
            sender: pk,
            nonce: 0,
            fee: 1,
            body: TransactionBody::TokenTransfer {
                to: pk_b,
                token_id: [0, 0, 0, 99],
                amount: 50,
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(err, ExecutionError::TokenNotFound(_)));
    }

    #[test]
    fn token_transfer_insufficient_balance() {
        let (sk, pk) = make_keypair();
        let (_, pk_b) = make_keypair();
        let mut state = funded_state(&pk, 10_000);

        // mint 100 tokens
        let mut mint_tx = Transaction {
            sender: pk,
            nonce: 0,
            fee: 1,
            body: TransactionBody::Mint {
                ticker: "TKN".into(),
                max_supply: 100,
                metadata_hash: [0; 32],
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut mint_tx);
        state.execute_tx(&mint_tx).unwrap();

        let token_id = 1u32.to_le_bytes();
        let mut tx = Transaction {
            sender: pk,
            nonce: 1,
            fee: 1,
            body: TransactionBody::TokenTransfer {
                to: pk_b,
                token_id,
                amount: 200,
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(
            err,
            ExecutionError::InsufficientTokenBalance { .. }
        ));
    }

    #[test]
    fn token_transfer_self() {
        let (sk, pk) = make_keypair();
        let mut state = funded_state(&pk, 10_000);

        let mut mint_tx = Transaction {
            sender: pk,
            nonce: 0,
            fee: 1,
            body: TransactionBody::Mint {
                ticker: "TKN".into(),
                max_supply: 100,
                metadata_hash: [0; 32],
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut mint_tx);
        state.execute_tx(&mint_tx).unwrap();

        let token_id = 1u32.to_le_bytes();
        let mut tx = Transaction {
            sender: pk,
            nonce: 1,
            fee: 1,
            body: TransactionBody::TokenTransfer {
                to: pk,
                token_id,
                amount: 50,
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(err, ExecutionError::SelfTransfer));
    }

    #[test]
    fn token_transfer_zero_amount() {
        let (sk, pk) = make_keypair();
        let (_, pk_b) = make_keypair();
        let mut state = funded_state(&pk, 10_000);

        let mut mint_tx = Transaction {
            sender: pk,
            nonce: 0,
            fee: 1,
            body: TransactionBody::Mint {
                ticker: "TKN".into(),
                max_supply: 100,
                metadata_hash: [0; 32],
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut mint_tx);
        state.execute_tx(&mint_tx).unwrap();

        let token_id = 1u32.to_le_bytes();
        let mut tx = Transaction {
            sender: pk,
            nonce: 1,
            fee: 1,
            body: TransactionBody::TokenTransfer {
                to: pk_b,
                token_id,
                amount: 0,
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(err, ExecutionError::ZeroAmount));
    }

    #[test]
    fn set_commission_not_validator() {
        let (sk, pk) = make_keypair();
        let mut state = funded_state(&pk, 10_000);
        // pk has no delegation

        let mut tx = Transaction {
            sender: pk,
            nonce: 0,
            fee: 1,
            body: TransactionBody::SetCommission { rate: 500 },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(err, ExecutionError::NotValidator));
    }

    #[test]
    fn set_commission_at_max() {
        let (sk, pk) = make_keypair();
        let mut state = funded_state(&pk, 10_000);
        state.staking.delegations.insert(pk, MIN_SELF_STAKE);

        let mut tx = Transaction {
            sender: pk,
            nonce: 0,
            fee: 1,
            body: TransactionBody::SetCommission {
                rate: MAX_COMMISSION_BPS,
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        state.execute_tx(&tx).unwrap();
        assert_eq!(
            state.staking.pending_commission_changes[0],
            (pk, MAX_COMMISSION_BPS)
        );
    }

    #[test]
    fn sequential_nonces() {
        let (sk, pk) = make_keypair();
        let (_, pk_b) = make_keypair();
        let mut state = funded_state(&pk, 100_000);

        for i in 0..5 {
            let mut tx = Transaction {
                sender: pk,
                nonce: i,
                fee: 1,
                body: TransactionBody::Transfer {
                    to: pk_b,
                    amount: 10,
                },
                signature: [0u8; 64],
            };
            sign_tx(&sk, &mut tx);
            state.execute_tx(&tx).unwrap();
        }

        assert_eq!(state.get_account(&pk).unwrap().nonce, 5);
        assert_eq!(state.get_account(&pk_b).unwrap().native_balance, 50);
    }

    #[test]
    fn fee_insufficient_for_fee() {
        let (sk, pk) = make_keypair();
        let (_, pk_b) = make_keypair();
        let mut state = funded_state(&pk, 5);

        let mut tx = Transaction {
            sender: pk,
            nonce: 0,
            fee: 10,
            body: TransactionBody::Transfer {
                to: pk_b,
                amount: 1,
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(err, ExecutionError::InsufficientBalance { .. }));
    }

    #[test]
    fn multiple_mints_increment_token_id() {
        let (sk, pk) = make_keypair();
        let mut state = funded_state(&pk, 10_000);

        for (i, ticker) in ["AAA", "BBB", "CCC"].iter().enumerate() {
            let mut tx = Transaction {
                sender: pk,
                nonce: i as u64,
                fee: 1,
                body: TransactionBody::Mint {
                    ticker: ticker.to_string(),
                    max_supply: 100,
                    metadata_hash: [i as u8; 32],
                },
                signature: [0u8; 64],
            };
            sign_tx(&sk, &mut tx);
            state.execute_tx(&tx).unwrap();
        }

        assert_eq!(state.tokens.len(), 3);
        assert_eq!(state.next_token_id, 4);
    }

    #[test]
    fn state_hash_includes_all_pools() {
        let mut state = State::new();
        let v: PublicKey = [1u8; 32];
        state.staking.delegations.insert(v, 500);
        state.staking.self_bonds.insert(v, MIN_SELF_STAKE);
        state.tx_fee_pool = 100;

        let h1 = state.compute_state_hash();
        state.msg_fee_pool = 200;
        let h2 = state.compute_state_hash();
        assert_ne!(h1, h2);

        state.bridge_fee_pool = 300;
        let h3 = state.compute_state_hash();
        assert_ne!(h2, h3);
    }

    #[test]
    fn state_hash_includes_commission() {
        let mut state = State::new();
        let v: PublicKey = [1u8; 32];
        state.staking.delegations.insert(v, 500);

        let h1 = state.compute_state_hash();
        state.staking.commission_rates.insert(v, 1000);
        let h2 = state.compute_state_hash();
        assert_ne!(h1, h2);
    }

    #[test]
    fn state_hash_includes_self_bonds() {
        let mut state = State::new();
        let v: PublicKey = [1u8; 32];
        state.staking.delegations.insert(v, 500);

        let h1 = state.compute_state_hash();
        state.staking.self_bonds.insert(v, 100);
        let h2 = state.compute_state_hash();
        assert_ne!(h1, h2);
    }

    #[test]
    fn epoch_transition_completes_retires() {
        let mut state = State::new();
        let delegator: PublicKey = [1u8; 32];
        let validator: PublicKey = [2u8; 32];

        state.staking.delegations.insert(validator, 1000);
        state.staking.self_bonds.insert(validator, MIN_SELF_STAKE);
        state.staking.queue_retire(delegator, validator, 500, 0);

        // epoch 0->1: too early (UNBONDING_PERIOD = 2)
        let r1 = state.process_epoch_transition(0);
        assert!(r1.retires_completed.is_empty());

        // epoch 1->2: should complete
        let r2 = state.process_epoch_transition(1);
        assert_eq!(r2.retires_completed.len(), 1);
        assert_eq!(
            state.staking.delegations.get(&validator).copied().unwrap(),
            500
        );
    }

    #[test]
    fn epoch_transition_deactivated_below_min_self_stake() {
        let mut state = State::new();
        let v1: PublicKey = [1u8; 32];
        let v2: PublicKey = [2u8; 32];

        state.staking.delegations.insert(v1, MIN_SELF_STAKE);
        state.staking.self_bonds.insert(v1, MIN_SELF_STAKE);
        state.staking.delegations.insert(v2, 500);
        // v2 has no self_bond

        let result = state.process_epoch_transition(0);
        assert!(result.deactivated.contains(&v2));
        assert!(!result.deactivated.contains(&v1));
    }

    // -- Burn tests --

    fn mint_token(
        sk: &SigningKey,
        pk: &PublicKey,
        state: &mut State,
        ticker: &str,
        supply: Amount,
        nonce: u64,
    ) -> TokenId {
        let mut tx = Transaction {
            sender: *pk,
            nonce,
            fee: 1,
            body: TransactionBody::Mint {
                ticker: ticker.to_string(),
                max_supply: supply,
                metadata_hash: [0; 32],
            },
            signature: [0u8; 64],
        };
        sign_tx(sk, &mut tx);
        state.execute_tx(&tx).unwrap();
        state.next_token_id.wrapping_sub(1).to_le_bytes()
    }

    #[test]
    fn burn_reduces_balance_and_supply() {
        let (sk, pk) = make_keypair();
        let mut state = funded_state(&pk, 10_000);
        let token_id = mint_token(&sk, &pk, &mut state, "BNK", 1000, 0);

        let mut tx = Transaction {
            sender: pk,
            nonce: 1,
            fee: 1,
            body: TransactionBody::Burn {
                token_id,
                amount: 300,
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        state.execute_tx(&tx).unwrap();

        assert_eq!(
            *state
                .get_account(&pk)
                .unwrap()
                .token_balances
                .get(&token_id)
                .unwrap(),
            700
        );
        assert_eq!(state.tokens.get(&token_id).unwrap().current_supply, 700);
    }

    #[test]
    fn burn_insufficient_balance() {
        let (sk, pk) = make_keypair();
        let mut state = funded_state(&pk, 10_000);
        let token_id = mint_token(&sk, &pk, &mut state, "BNK", 100, 0);

        let mut tx = Transaction {
            sender: pk,
            nonce: 1,
            fee: 1,
            body: TransactionBody::Burn {
                token_id,
                amount: 200,
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(
            err,
            ExecutionError::InsufficientTokenBalance { .. }
        ));
    }

    #[test]
    fn burn_token_not_found() {
        let (sk, pk) = make_keypair();
        let mut state = funded_state(&pk, 10_000);

        let mut tx = Transaction {
            sender: pk,
            nonce: 0,
            fee: 1,
            body: TransactionBody::Burn {
                token_id: [0, 0, 0, 99],
                amount: 50,
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(err, ExecutionError::TokenNotFound(_)));
    }

    #[test]
    fn burn_zero_amount() {
        let (sk, pk) = make_keypair();
        let mut state = funded_state(&pk, 10_000);
        let token_id = mint_token(&sk, &pk, &mut state, "BNK", 100, 0);

        let mut tx = Transaction {
            sender: pk,
            nonce: 1,
            fee: 1,
            body: TransactionBody::Burn {
                token_id,
                amount: 0,
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(err, ExecutionError::ZeroAmount));
    }

    #[test]
    fn burn_entire_balance_removes_entry() {
        let (sk, pk) = make_keypair();
        let mut state = funded_state(&pk, 10_000);
        let token_id = mint_token(&sk, &pk, &mut state, "BNK", 100, 0);

        let mut tx = Transaction {
            sender: pk,
            nonce: 1,
            fee: 1,
            body: TransactionBody::Burn {
                token_id,
                amount: 100,
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        state.execute_tx(&tx).unwrap();

        assert!(!state
            .get_account(&pk)
            .unwrap()
            .token_balances
            .contains_key(&token_id));
        assert_eq!(state.tokens.get(&token_id).unwrap().current_supply, 0);
    }

    // -- UpdateMetadata tests --

    #[test]
    fn update_metadata_success() {
        let (sk, pk) = make_keypair();
        let mut state = funded_state(&pk, 10_000);
        let token_id = mint_token(&sk, &pk, &mut state, "BNK", 100, 0);

        let new_hash = [0xFFu8; 32];
        let mut tx = Transaction {
            sender: pk,
            nonce: 1,
            fee: 1,
            body: TransactionBody::UpdateMetadata {
                token_id,
                metadata_hash: new_hash,
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        state.execute_tx(&tx).unwrap();

        assert_eq!(state.tokens.get(&token_id).unwrap().metadata_hash, new_hash);
    }

    #[test]
    fn update_metadata_not_creator() {
        let (sk_a, pk_a) = make_keypair();
        let (sk_b, pk_b) = make_keypair();
        let mut state = funded_state(&pk_a, 10_000);
        state.get_or_create_account(&pk_b).native_balance = 10_000;
        let token_id = mint_token(&sk_a, &pk_a, &mut state, "BNK", 100, 0);

        let mut tx = Transaction {
            sender: pk_b,
            nonce: 0,
            fee: 1,
            body: TransactionBody::UpdateMetadata {
                token_id,
                metadata_hash: [0xAA; 32],
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk_b, &mut tx);
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(err, ExecutionError::NotTokenCreator));
    }

    #[test]
    fn update_metadata_token_not_found() {
        let (sk, pk) = make_keypair();
        let mut state = funded_state(&pk, 10_000);

        let mut tx = Transaction {
            sender: pk,
            nonce: 0,
            fee: 1,
            body: TransactionBody::UpdateMetadata {
                token_id: [0, 0, 0, 99],
                metadata_hash: [0xAA; 32],
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(err, ExecutionError::TokenNotFound(_)));
    }

    // -- Phase 3: Fee distribution with commission tests --

    #[test]
    fn fee_distribution_with_commission() {
        let mut state = State::new();

        let validator: PublicKey = [1u8; 32];
        let delegator: PublicKey = [2u8; 32];

        // set up: validator has 500 self-stake, delegator has 500
        state.staking.delegations.insert(validator, 1000);
        state.staking.self_bonds.insert(validator, MIN_SELF_STAKE);
        state
            .staking
            .delegator_stakes
            .insert((validator, validator), 500);
        state
            .staking
            .delegator_stakes
            .insert((delegator, validator), 500);
        state.staking.commission_rates.insert(validator, 1000); // 10% commission
        state.tx_fee_pool = 1000;

        state.process_epoch_transition(0);

        // gross share = 1000 (only validator)
        // commission = 1000 * 10% = 100 → validator
        // delegator_pool = 900
        // validator share of delegator_pool = 900 * 500/1000 = 450
        // delegator share = 900 * 500/1000 = 450
        // validator total = 100 + 450 = 550
        // delegator total = 450
        assert_eq!(state.get_account(&validator).unwrap().native_balance, 550);
        assert_eq!(state.get_account(&delegator).unwrap().native_balance, 450);
        assert_eq!(state.tx_fee_pool, 0);
    }

    #[test]
    fn fee_distribution_no_commission() {
        let mut state = State::new();

        let validator: PublicKey = [1u8; 32];
        let delegator: PublicKey = [2u8; 32];

        state.staking.delegations.insert(validator, 1000);
        state.staking.self_bonds.insert(validator, MIN_SELF_STAKE);
        state
            .staking
            .delegator_stakes
            .insert((validator, validator), 600);
        state
            .staking
            .delegator_stakes
            .insert((delegator, validator), 400);
        // no commission set (defaults to 0)
        state.tx_fee_pool = 1000;

        state.process_epoch_transition(0);

        // gross share = 1000, commission = 0
        // validator share = 1000 * 600/1000 = 600
        // delegator share = 1000 * 400/1000 = 400
        assert_eq!(state.get_account(&validator).unwrap().native_balance, 600);
        assert_eq!(state.get_account(&delegator).unwrap().native_balance, 400);
    }

    #[test]
    fn fee_distribution_multi_validator_with_commission() {
        let mut state = State::new();

        let v1: PublicKey = [1u8; 32];
        let v2: PublicKey = [2u8; 32];
        let d1: PublicKey = [3u8; 32];

        state.staking.delegations.insert(v1, 500);
        state.staking.delegations.insert(v2, 500);
        state.staking.self_bonds.insert(v1, MIN_SELF_STAKE);
        state.staking.self_bonds.insert(v2, MIN_SELF_STAKE);
        state.staking.delegator_stakes.insert((v1, v1), 500);
        state.staking.delegator_stakes.insert((v2, v2), 300);
        state.staking.delegator_stakes.insert((d1, v2), 200);
        state.staking.commission_rates.insert(v2, 2000); // 20% commission on v2
        state.tx_fee_pool = 1000;

        state.process_epoch_transition(0);

        // v1 gross = 1000 * 500/1000 = 500, no commission
        // v1 gets all 500 (sole delegator)
        assert_eq!(state.get_account(&v1).unwrap().native_balance, 500);

        // v2 gross = 1000 * 500/1000 = 500
        // commission = 500 * 20% = 100 → v2
        // delegator_pool = 400
        // v2 self share = 400 * 300/500 = 240
        // d1 share = 400 * 200/500 = 160
        // v2 total = 100 + 240 = 340
        assert_eq!(state.get_account(&v2).unwrap().native_balance, 340);
        assert_eq!(state.get_account(&d1).unwrap().native_balance, 160);
    }

    #[test]
    fn all_fee_pools_distributed() {
        let mut state = State::new();

        let validator: PublicKey = [1u8; 32];

        state.staking.delegations.insert(validator, MIN_SELF_STAKE);
        state.staking.self_bonds.insert(validator, MIN_SELF_STAKE);
        state
            .staking
            .delegator_stakes
            .insert((validator, validator), MIN_SELF_STAKE);
        state.tx_fee_pool = 100;
        state.msg_fee_pool = 200;
        state.bridge_fee_pool = 300;
        // validator must have relay participation to receive msg_fee_pool
        state.staking.record_relay_participation(validator);

        state.process_epoch_transition(0);

        assert_eq!(state.tx_fee_pool, 0);
        assert_eq!(state.msg_fee_pool, 0);
        assert_eq!(state.bridge_fee_pool, 0);
        assert_eq!(state.get_account(&validator).unwrap().native_balance, 600);
    }

    #[test]
    fn delegator_stakes_tracked_on_bond_activation() {
        let mut state = State::new();
        let delegator: PublicKey = [1u8; 32];
        let validator: PublicKey = [2u8; 32];

        state.staking.queue_bond(delegator, validator, 500, 0);
        state.process_epoch_transition(0);

        assert_eq!(
            state
                .staking
                .delegator_stakes
                .get(&(delegator, validator))
                .copied()
                .unwrap(),
            500
        );
    }

    #[test]
    fn delegator_stakes_reduced_on_retire() {
        let mut state = State::new();
        let delegator: PublicKey = [1u8; 32];
        let validator: PublicKey = [2u8; 32];

        state.staking.delegations.insert(validator, 1000);
        state
            .staking
            .delegator_stakes
            .insert((delegator, validator), 1000);
        state.staking.queue_retire(delegator, validator, 500, 0);

        // epoch 0->1: too early
        state.process_epoch_transition(0);
        assert_eq!(
            state
                .staking
                .delegator_stakes
                .get(&(delegator, validator))
                .copied()
                .unwrap(),
            1000
        );

        // epoch 1->2: retirement completes
        state.process_epoch_transition(1);
        assert_eq!(
            state
                .staking
                .delegator_stakes
                .get(&(delegator, validator))
                .copied()
                .unwrap(),
            500
        );
    }

    // ── Location claim transaction tests ──

    fn state_with_validators(n: u8) -> (State, Vec<(SigningKey, PublicKey)>) {
        let mut state = State::new();
        state.current_epoch = 1; // so attesters have mature stake
        let mut keys = Vec::new();
        for _ in 0..n {
            let (sk, pk) = make_keypair();
            state.get_or_create_account(&pk).native_balance = 10_000_000;
            state.staking.delegations.insert(pk, MIN_SELF_STAKE);
            state.staking.self_bonds.insert(pk, MIN_SELF_STAKE);
            keys.push((sk, pk));
        }
        (state, keys)
    }

    #[test]
    fn location_claim_tx_success() {
        let (mut state, keys) = state_with_validators(4);
        let (ref sk, sender) = keys[0];

        let attestations: Vec<_> = keys[1..]
            .iter()
            .map(|(_, pk)| crate::transaction::LocationAttestationData {
                attester: *pk,
                propagation_delay_ms: 50,
                signature: [0u8; 64], // sig verification delegated to staking layer
            })
            .collect();

        let mut tx = Transaction {
            sender,
            nonce: 0,
            fee: 10,
            body: TransactionBody::LocationClaim {
                lat: 48_856,
                lon: 2_352,
                attestations,
            },
            signature: [0u8; 64],
        };
        sign_tx(sk, &mut tx);
        state.execute_tx(&tx).unwrap();

        assert_eq!(state.staking.pending_location_claims.len(), 1);
    }

    #[test]
    fn location_claim_tx_insufficient_attestations() {
        let (mut state, keys) = state_with_validators(4);
        let (ref sk, sender) = keys[0];

        // Only 2 attestations when 3 are required
        let attestations: Vec<_> = keys[1..3]
            .iter()
            .map(|(_, pk)| crate::transaction::LocationAttestationData {
                attester: *pk,
                propagation_delay_ms: 50,
                signature: [0u8; 64],
            })
            .collect();

        let mut tx = Transaction {
            sender,
            nonce: 0,
            fee: 10,
            body: TransactionBody::LocationClaim {
                lat: 48_856,
                lon: 2_352,
                attestations,
            },
            signature: [0u8; 64],
        };
        sign_tx(sk, &mut tx);
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(
            err,
            ExecutionError::InsufficientAttestations {
                required: 3,
                provided: 2
            }
        ));
    }

    #[test]
    fn location_claim_tx_not_validator() {
        let (mut state, keys) = state_with_validators(4);
        // Create a non-validator sender
        let (sk, pk) = make_keypair();
        state.get_or_create_account(&pk).native_balance = 10_000_000;

        let attestations: Vec<_> = keys[1..4]
            .iter()
            .map(|(_, pk)| crate::transaction::LocationAttestationData {
                attester: *pk,
                propagation_delay_ms: 50,
                signature: [0u8; 64],
            })
            .collect();

        let mut tx = Transaction {
            sender: pk,
            nonce: 0,
            fee: 10,
            body: TransactionBody::LocationClaim {
                lat: 48_856,
                lon: 2_352,
                attestations,
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(err, ExecutionError::NotValidator));
    }

    #[test]
    fn location_claim_tx_out_of_bounds() {
        let (mut state, keys) = state_with_validators(4);
        let (ref sk, sender) = keys[0];

        let attestations: Vec<_> = keys[1..]
            .iter()
            .map(|(_, pk)| crate::transaction::LocationAttestationData {
                attester: *pk,
                propagation_delay_ms: 50,
                signature: [0u8; 64],
            })
            .collect();

        let mut tx = Transaction {
            sender,
            nonce: 0,
            fee: 10,
            body: TransactionBody::LocationClaim {
                lat: 91_000, // out of bounds
                lon: 0,
                attestations,
            },
            signature: [0u8; 64],
        };
        sign_tx(sk, &mut tx);
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(err, ExecutionError::LocationOutOfBounds));
    }

    #[test]
    fn location_claim_epoch_transition() {
        let (mut state, keys) = state_with_validators(4);
        let (ref sk, sender) = keys[0];

        let attestations: Vec<_> = keys[1..]
            .iter()
            .map(|(_, pk)| crate::transaction::LocationAttestationData {
                attester: *pk,
                propagation_delay_ms: 50,
                signature: [0u8; 64],
            })
            .collect();

        let mut tx = Transaction {
            sender,
            nonce: 0,
            fee: 10,
            body: TransactionBody::LocationClaim {
                lat: 48_856,
                lon: 2_352,
                attestations,
            },
            signature: [0u8; 64],
        };
        sign_tx(sk, &mut tx);
        state.execute_tx(&tx).unwrap();

        // Process epoch transition
        let result = state.process_epoch_transition(1);
        assert_eq!(result.location_claims_validated.len(), 1);
        assert_eq!(result.location_claims_validated[0], sender);

        // Location should now be validated
        let loc = state.staking.get_location(&sender).unwrap();
        assert_eq!(loc.lat, 48_856);
        assert_eq!(loc.lon, 2_352);
        assert!(state.staking.pending_location_claims.is_empty());
    }

    // ── Verified Offline Messaging tests ──

    #[test]
    fn message_anchor_routes_deposit_to_msg_fee_pool() {
        let (sk, pk) = make_keypair();
        let (_, dest) = make_keypair();
        let mut state = funded_state(&pk, 100_000);

        let mut tx = Transaction {
            sender: pk,
            nonce: 0,
            fee: 10,
            body: TransactionBody::MessageAnchor {
                destination: dest,
                length_proof: Box::new([0u8; 288]),
                deposit: 5_000,
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        state.execute_tx(&tx).unwrap();

        assert_eq!(state.msg_fee_pool, 5_000);
        assert_eq!(state.tx_fee_pool, 10);
        // balance = 100_000 - 10 (fee) - 5_000 (deposit)
        assert_eq!(state.get_account(&pk).unwrap().native_balance, 94_990);
        assert_eq!(state.epoch_messages_anchored, 1);
    }

    #[test]
    fn message_anchor_rejects_below_min_deposit() {
        let (sk, pk) = make_keypair();
        let (_, dest) = make_keypair();
        let mut state = funded_state(&pk, 100_000);

        let mut tx = Transaction {
            sender: pk,
            nonce: 0,
            fee: 10,
            body: TransactionBody::MessageAnchor {
                destination: dest,
                length_proof: Box::new([0u8; 288]),
                deposit: 500, // below MSG_MIN_FEE (1_000)
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(
            err,
            ExecutionError::InsufficientMessageDeposit { .. }
        ));
    }

    #[test]
    fn delivery_wrapup_records_relay_participants() {
        let (sk, pk) = make_keypair();
        let mut state = funded_state(&pk, 100_000);

        // Set up two validators as staked
        let v1: PublicKey = [10u8; 32];
        let v2: PublicKey = [20u8; 32];
        state.staking.delegations.insert(v1, MIN_SELF_STAKE);
        state.staking.delegations.insert(v2, MIN_SELF_STAKE);

        let mut tx = Transaction {
            sender: pk,
            nonce: 0,
            fee: 10,
            body: TransactionBody::DeliveryWrapup {
                anchor_hash: [0xAA; 32],
                relay_receipts: vec![
                    crate::transaction::RelayReceiptData {
                        relay_node: v1,
                        hop_index: 0,
                        signature: [0u8; 64],
                    },
                    crate::transaction::RelayReceiptData {
                        relay_node: v2,
                        hop_index: 1,
                        signature: [0u8; 64],
                    },
                ],
                destination_ack: [0u8; 64],
            },
            signature: [0u8; 64],
        };
        sign_tx(&sk, &mut tx);
        state.execute_tx(&tx).unwrap();

        assert!(state.staking.msg_relay_participants.contains(&v1));
        assert!(state.staking.msg_relay_participants.contains(&v2));
        assert_eq!(state.epoch_deliveries_completed, 1);
    }

    #[test]
    fn msg_fee_pool_only_distributed_to_relay_participants() {
        let mut state = State::new();
        let v1: PublicKey = [1u8; 32];
        let v2: PublicKey = [2u8; 32];

        state.staking.delegations.insert(v1, MIN_SELF_STAKE);
        state.staking.self_bonds.insert(v1, MIN_SELF_STAKE);
        state
            .staking
            .delegator_stakes
            .insert((v1, v1), MIN_SELF_STAKE);

        state.staking.delegations.insert(v2, MIN_SELF_STAKE);
        state.staking.self_bonds.insert(v2, MIN_SELF_STAKE);
        state
            .staking
            .delegator_stakes
            .insert((v2, v2), MIN_SELF_STAKE);

        state.msg_fee_pool = 1_000;

        // Only v1 participated in relays
        state.staking.record_relay_participation(v1);

        state.process_epoch_transition(0);

        // v1 should get all msg_fee_pool rewards
        assert!(state.get_account(&v1).unwrap().native_balance > 0);
        // v2 should get nothing from msg_fee_pool
        assert!(
            state.get_account(&v2).is_none() || state.get_account(&v2).unwrap().native_balance == 0
        );
        assert_eq!(state.msg_fee_pool, 0);
    }

    #[test]
    fn msg_fee_pool_rolls_over_when_no_participants() {
        let mut state = State::new();
        let v1: PublicKey = [1u8; 32];

        state.staking.delegations.insert(v1, MIN_SELF_STAKE);
        state.staking.self_bonds.insert(v1, MIN_SELF_STAKE);
        state
            .staking
            .delegator_stakes
            .insert((v1, v1), MIN_SELF_STAKE);

        state.msg_fee_pool = 500;
        // No relay participants

        state.process_epoch_transition(0);

        // msg_fee_pool should roll over (no distribution)
        assert_eq!(state.msg_fee_pool, 500);
        // v1 should have received nothing from msg pool
        assert!(
            state.get_account(&v1).is_none() || state.get_account(&v1).unwrap().native_balance == 0
        );
    }
}
