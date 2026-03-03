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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_default_values() {
        let acc = Account {
            native_balance: 0,
            token_balances: BTreeMap::new(),
            nonce: 0,
        };
        assert_eq!(acc.native_balance, 0);
        assert_eq!(acc.nonce, 0);
        assert!(acc.token_balances.is_empty());
    }

    #[test]
    fn account_serde_roundtrip() {
        let acc = Account {
            native_balance: 42_000,
            token_balances: BTreeMap::new(),
            nonce: 7,
        };

        let json = serde_json::to_string(&acc).unwrap();
        let deserialized: Account = serde_json::from_str(&json).unwrap();
        assert_eq!(acc, deserialized);
    }

    #[test]
    fn account_with_tokens_equality() {
        let mut a = Account {
            native_balance: 42_000,
            token_balances: BTreeMap::new(),
            nonce: 7,
        };
        a.token_balances.insert([0, 0, 0, 1], 1000);
        a.token_balances.insert([0, 0, 0, 2], 2000);

        let b = a.clone();
        assert_eq!(a, b);

        let mut c = a.clone();
        c.token_balances.insert([0, 0, 0, 3], 3000);
        assert_ne!(a, c);
    }

    #[test]
    fn account_multiple_token_balances() {
        let mut acc = Account {
            native_balance: 100,
            token_balances: BTreeMap::new(),
            nonce: 0,
        };

        for i in 1..=5u32 {
            acc.token_balances.insert(i.to_le_bytes(), i as Amount * 100);
        }

        assert_eq!(acc.token_balances.len(), 5);
        assert_eq!(*acc.token_balances.get(&3u32.to_le_bytes()).unwrap(), 300);
    }

    #[test]
    fn token_meta_serde_roundtrip() {
        let meta = TokenMeta {
            id: [0, 0, 0, 1],
            ticker: "BNK".to_string(),
            current_supply: 1_000_000,
            max_supply: 10_000_000,
            metadata_hash: [0xAB; 32],
            creator: [42u8; 32],
        };

        let json = serde_json::to_string(&meta).unwrap();
        let deserialized: TokenMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(meta, deserialized);
    }
}
