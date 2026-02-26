use serde::{Deserialize, Serialize};

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
