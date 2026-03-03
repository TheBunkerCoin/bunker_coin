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

#[cfg(test)]
mod tests {
    use super::*;

    fn pk(n: u8) -> PublicKey {
        let mut key = [0u8; 32];
        key[0] = n;
        key
    }

    fn dummy_tx(body: TransactionBody) -> Transaction {
        Transaction {
            sender: pk(1),
            nonce: 0,
            fee: 10,
            body,
            signature: [0u8; 64],
        }
    }

    #[test]
    fn signing_hash_deterministic() {
        let tx = dummy_tx(TransactionBody::Transfer { to: pk(2), amount: 100 });
        let h1 = tx.signing_hash();
        let h2 = tx.signing_hash();
        assert_eq!(h1, h2);
    }

    #[test]
    fn signing_hash_differs_by_body_type() {
        let transfer = dummy_tx(TransactionBody::Transfer { to: pk(2), amount: 100 });
        let bond = dummy_tx(TransactionBody::Bond { validator: pk(2), amount: 100 });
        assert_ne!(transfer.signing_hash(), bond.signing_hash());
    }

    #[test]
    fn signing_hash_differs_by_amount() {
        let tx1 = dummy_tx(TransactionBody::Transfer { to: pk(2), amount: 100 });
        let tx2 = dummy_tx(TransactionBody::Transfer { to: pk(2), amount: 200 });
        assert_ne!(tx1.signing_hash(), tx2.signing_hash());
    }

    #[test]
    fn signing_hash_differs_by_recipient() {
        let tx1 = dummy_tx(TransactionBody::Transfer { to: pk(2), amount: 100 });
        let tx2 = dummy_tx(TransactionBody::Transfer { to: pk(3), amount: 100 });
        assert_ne!(tx1.signing_hash(), tx2.signing_hash());
    }

    #[test]
    fn signing_hash_differs_by_nonce() {
        let tx1 = Transaction { nonce: 0, ..dummy_tx(TransactionBody::Transfer { to: pk(2), amount: 100 }) };
        let tx2 = Transaction { nonce: 1, ..dummy_tx(TransactionBody::Transfer { to: pk(2), amount: 100 }) };
        assert_ne!(tx1.signing_hash(), tx2.signing_hash());
    }

    #[test]
    fn signing_hash_differs_by_fee() {
        let tx1 = Transaction { fee: 10, ..dummy_tx(TransactionBody::Transfer { to: pk(2), amount: 100 }) };
        let tx2 = Transaction { fee: 20, ..dummy_tx(TransactionBody::Transfer { to: pk(2), amount: 100 }) };
        assert_ne!(tx1.signing_hash(), tx2.signing_hash());
    }

    #[test]
    fn signing_hash_excludes_signature() {
        let tx1 = Transaction { signature: [0u8; 64], ..dummy_tx(TransactionBody::Transfer { to: pk(2), amount: 100 }) };
        let tx2 = Transaction { signature: [0xFF; 64], ..dummy_tx(TransactionBody::Transfer { to: pk(2), amount: 100 }) };
        assert_eq!(tx1.signing_hash(), tx2.signing_hash());
    }

    #[test]
    fn hash_includes_signature() {
        let tx1 = Transaction { signature: [0u8; 64], ..dummy_tx(TransactionBody::Transfer { to: pk(2), amount: 100 }) };
        let tx2 = Transaction { signature: [0xFF; 64], ..dummy_tx(TransactionBody::Transfer { to: pk(2), amount: 100 }) };
        assert_ne!(tx1.hash(), tx2.hash());
    }

    #[test]
    fn signing_hash_each_body_variant() {
        let variants = vec![
            TransactionBody::Transfer { to: pk(2), amount: 50 },
            TransactionBody::TokenTransfer { to: pk(2), token_id: [0, 0, 0, 1], amount: 50 },
            TransactionBody::Mint { ticker: "BNK".into(), max_supply: 1000, metadata_hash: [0xAB; 32] },
            TransactionBody::Bond { validator: pk(2), amount: 500 },
            TransactionBody::Retire { validator: pk(2), amount: 500 },
            TransactionBody::Withdraw { validator: pk(2) },
            TransactionBody::UnJail,
            TransactionBody::SetCommission { rate: 500 },
        ];

        let hashes: Vec<[u8; 32]> = variants.iter().map(|body| dummy_tx(body.clone()).signing_hash()).collect();

        // all hashes must be unique
        for i in 0..hashes.len() {
            for j in (i + 1)..hashes.len() {
                assert_ne!(hashes[i], hashes[j], "variant {i} and {j} produced same hash");
            }
        }
    }

    #[test]
    fn transaction_serde_roundtrip() {
        let tx = dummy_tx(TransactionBody::Transfer { to: pk(2), amount: 100 });
        let json = serde_json::to_string(&tx).unwrap();
        let deserialized: Transaction = serde_json::from_str(&json).unwrap();
        assert_eq!(tx, deserialized);
    }

    #[test]
    fn all_body_variants_serde_roundtrip() {
        let variants = vec![
            TransactionBody::Transfer { to: pk(2), amount: 50 },
            TransactionBody::TokenTransfer { to: pk(2), token_id: [0, 0, 0, 1], amount: 50 },
            TransactionBody::Mint { ticker: "BNK".into(), max_supply: 1000, metadata_hash: [0xAB; 32] },
            TransactionBody::Bond { validator: pk(2), amount: 500 },
            TransactionBody::Retire { validator: pk(2), amount: 500 },
            TransactionBody::Withdraw { validator: pk(2) },
            TransactionBody::UnJail,
            TransactionBody::SetCommission { rate: 500 },
        ];

        for body in variants {
            let tx = dummy_tx(body);
            let encoded = serde_json::to_vec(&tx).unwrap();
            let decoded: Transaction = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(tx, decoded);
        }
    }

    #[test]
    fn mint_signing_hash_differs_by_ticker() {
        let tx1 = dummy_tx(TransactionBody::Mint { ticker: "AAA".into(), max_supply: 100, metadata_hash: [0; 32] });
        let tx2 = dummy_tx(TransactionBody::Mint { ticker: "BBB".into(), max_supply: 100, metadata_hash: [0; 32] });
        assert_ne!(tx1.signing_hash(), tx2.signing_hash());
    }

    #[test]
    fn set_commission_signing_hash_differs_by_rate() {
        let tx1 = dummy_tx(TransactionBody::SetCommission { rate: 100 });
        let tx2 = dummy_tx(TransactionBody::SetCommission { rate: 200 });
        assert_ne!(tx1.signing_hash(), tx2.signing_hash());
    }
}
