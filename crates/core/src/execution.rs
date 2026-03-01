use std::collections::HashMap;

use ed25519_dalek::{Verifier, VerifyingKey, Signature as DalekSignature};

use crate::account::{Account, TokenMeta};
use crate::transaction::{Transaction, TransactionBody};
use crate::types::{Amount, PublicKey, TokenId, MAX_TICKER_LEN, MIN_TICKER_LEN};

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
}

impl std::fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidSignature => write!(f, "invalid signature"),
            Self::NonceMismatch { expected, got } => {
                write!(f, "nonce mismatch: expected {expected}, got {got}")
            }
            Self::InsufficientBalance { required, available } => {
                write!(f, "insufficient balance: need {required}, have {available}")
            }
            Self::InsufficientTokenBalance { required, available } => {
                write!(f, "insufficient token balance: need {required}, have {available}")
            }
            Self::TickerAlreadyExists(t) => write!(f, "ticker already exists: {t}"),
            Self::TickerLengthInvalid(len) => {
                write!(f, "ticker length {len} not in {MIN_TICKER_LEN}..={MAX_TICKER_LEN}")
            }
            Self::TokenNotFound(id) => write!(f, "token not found: {:?}", id),
            Self::SelfTransfer => write!(f, "cannot transfer to self"),
            Self::ZeroAmount => write!(f, "amount must be non-zero"),
            Self::Overflow => write!(f, "arithmetic overflow"),
        }
    }
}

impl std::error::Error for ExecutionError {}

#[derive(Clone, Debug)]
pub struct State {
    pub accounts: HashMap<PublicKey, Account>,
    pub tokens: HashMap<TokenId, TokenMeta>,
    pub next_token_id: u32,
    pub fee_pool: Amount,
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
            fee_pool: 0,
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
        self.fee_pool += tx.fee;

        self.apply_body(tx.sender, &tx.body)
    }

    fn verify_signature(tx: &Transaction) -> Result<(), ExecutionError> {
        let vk = VerifyingKey::from_bytes(&tx.sender)
            .map_err(|_| ExecutionError::InvalidSignature)?;
        let sig = DalekSignature::from_bytes(&tx.signature);
        let msg = tx.signing_hash();
        vk.verify(&msg, &sig)
            .map_err(|_| ExecutionError::InvalidSignature)
    }

    fn apply_body(&mut self, sender: PublicKey, body: &TransactionBody) -> Result<(), ExecutionError> {
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

            TransactionBody::TokenTransfer { to, token_id, amount } => {
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
                let sender_token_bal = sender_acc.token_balances.get(token_id).copied().unwrap_or(0);
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

            TransactionBody::Mint { ticker, max_supply, metadata_hash } => {
                if ticker.len() < MIN_TICKER_LEN || ticker.len() > MAX_TICKER_LEN {
                    return Err(ExecutionError::TickerLengthInvalid(ticker.len()));
                }
                if self.tokens.values().any(|t| t.ticker == *ticker) {
                    return Err(ExecutionError::TickerAlreadyExists(ticker.clone()));
                }
                let id = self.next_token_id.to_le_bytes();
                self.next_token_id += 1;
                self.tokens.insert(id, TokenMeta {
                    id,
                    ticker: ticker.clone(),
                    current_supply: *max_supply,
                    max_supply: *max_supply,
                    metadata_hash: *metadata_hash,
                    creator: sender,
                });
                let acc = self.accounts.get_mut(&sender).unwrap();
                acc.token_balances.insert(id, *max_supply);
                Ok(())
            }

            TransactionBody::Bond { amount, .. } => {
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
                Ok(())
            }

            // fee deducted + nonce bumped; future: start unbonding timer
            TransactionBody::Retire { .. } => Ok(()),
            // fee deducted + nonce bumped; future: credit unbonded stake
            TransactionBody::Withdraw { .. } => Ok(()),
            // fee deducted + nonce bumped
            TransactionBody::UnJail => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{SigningKey, Signer};
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
            body: TransactionBody::Transfer { to: pk_b, amount: 100 },
            signature: [0u8; 64],
        };
        sign_tx(&sk_a, &mut tx);
        state.execute_tx(&tx).unwrap();

        assert_eq!(state.get_account(&pk_a).unwrap().native_balance, 890);
        assert_eq!(state.get_account(&pk_b).unwrap().native_balance, 100);
        assert_eq!(state.fee_pool, 10);
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
            body: TransactionBody::Transfer { to: pk_b, amount: 100 },
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
            body: TransactionBody::Transfer { to: pk_a, amount: 100 },
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
            body: TransactionBody::Transfer { to: pk_b, amount: 0 },
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
            body: TransactionBody::Transfer { to: pk_b, amount: 100 },
            signature: [0u8; 64],
        };
        sign_tx(&sk_a, &mut tx);
        let err = state.execute_tx(&tx).unwrap_err();
        assert!(matches!(err, ExecutionError::NonceMismatch { expected: 0, got: 5 }));
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
            body: TransactionBody::Transfer { to: pk_b, amount: 50 },
            signature: [0u8; 64],
        };
        sign_tx(&sk_a, &mut tx);
        state.execute_tx(&tx).unwrap();

        assert_eq!(state.get_account(&pk_a).unwrap().native_balance, 25);
        assert_eq!(state.fee_pool, 25);
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

        assert_eq!(*state.get_account(&pk_a).unwrap().token_balances.get(&token_id).unwrap(), 300);
        assert_eq!(*state.get_account(&pk_b).unwrap().token_balances.get(&token_id).unwrap(), 200);
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
            body: TransactionBody::Bond { validator, amount: 500 },
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
            body: TransactionBody::Transfer { to: pk_b, amount: 100 },
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
            body: TransactionBody::Transfer { to: pk_b, amount: 100 },
            signature: [0u8; 64],
        };
        sign_tx(&sk_a, &mut good_tx);

        let bad_tx = Transaction {
            sender: pk_a,
            nonce: 99,
            fee: 10,
            body: TransactionBody::Transfer { to: pk_b, amount: 100 },
            signature: [0xFF; 64],
        };

        let results = state.execute_block(&[good_tx, bad_tx]);
        assert!(results[0].is_ok());
        assert!(results[1].is_err());

        assert_eq!(state.get_account(&pk_a).unwrap().native_balance, 890);
        assert_eq!(state.get_account(&pk_b).unwrap().native_balance, 100);
    }
}
