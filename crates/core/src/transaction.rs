use serde::{Deserialize, Serialize};
use sha2::{Sha256, Digest};

use crate::types::{Amount, Nonce, PublicKey, Signature, TokenId};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transaction {
    pub sender: PublicKey,
    pub nonce: Nonce,
    pub fee: Amount,
    pub body: TransactionBody,
    #[serde(with = "crate::types::serde_signature")]
    pub signature: Signature,
}

impl Transaction {
    pub fn hash(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(self.sender);
        hasher.update(self.nonce.to_le_bytes());
        hasher.update(self.fee.to_le_bytes());
        hasher.update(self.signature);
        hasher.finalize().into()
    }

    /// deterministic hash of sender + nonce + fee + body (excludes signature)
    pub fn signing_hash(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(self.sender);
        hasher.update(self.nonce.to_le_bytes());
        hasher.update(self.fee.to_le_bytes());

        match &self.body {
            TransactionBody::Transfer { to, amount } => {
                hasher.update([0u8]);
                hasher.update(to);
                hasher.update(amount.to_le_bytes());
            }
            TransactionBody::TokenTransfer { to, token_id, amount } => {
                hasher.update([1u8]);
                hasher.update(to);
                hasher.update(token_id);
                hasher.update(amount.to_le_bytes());
            }
            TransactionBody::Mint { ticker, max_supply, metadata_hash } => {
                hasher.update([2u8]);
                hasher.update((ticker.len() as u32).to_le_bytes());
                hasher.update(ticker.as_bytes());
                hasher.update(max_supply.to_le_bytes());
                hasher.update(metadata_hash);
            }
            TransactionBody::Bond { validator, amount } => {
                hasher.update([3u8]);
                hasher.update(validator);
                hasher.update(amount.to_le_bytes());
            }
            TransactionBody::Retire { validator, amount } => {
                hasher.update([4u8]);
                hasher.update(validator);
                hasher.update(amount.to_le_bytes());
            }
            TransactionBody::Withdraw { validator } => {
                hasher.update([5u8]);
                hasher.update(validator);
            }
            TransactionBody::UnJail => {
                hasher.update([6u8]);
            }
            TransactionBody::SetCommission { rate } => {
                hasher.update([7u8]);
                hasher.update(rate.to_le_bytes());
            }
        }

        hasher.finalize().into()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionBody {
    Transfer {
        to: PublicKey,
        amount: Amount,
    },
    TokenTransfer {
        to: PublicKey,
        token_id: TokenId,
        amount: Amount,
    },
    Mint {
        ticker: String,
        max_supply: Amount,
        metadata_hash: [u8; 32],
    },
    Bond {
        validator: PublicKey,
        amount: Amount,
    },
    Retire {
        validator: PublicKey,
        amount: Amount,
    },
    Withdraw {
        validator: PublicKey,
    },
    UnJail,
    SetCommission {
        rate: u16,
    },
}
