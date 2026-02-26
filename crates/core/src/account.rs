use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::types::{Amount, Nonce, PublicKey, TokenId};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Account {
    pub native_balance: Amount,
    pub token_balances: BTreeMap<TokenId, Amount>,
    pub nonce: Nonce,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenMeta {
    pub id: TokenId,
    pub ticker: String,
    pub current_supply: Amount,
    pub max_supply: Amount,
    pub metadata_hash: [u8; 32],
    pub creator: PublicKey,
}
