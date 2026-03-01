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
}
