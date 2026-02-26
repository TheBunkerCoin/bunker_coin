pub type PublicKey = [u8; 32];
pub type Signature = [u8; 64];
pub type TokenId = [u8; 4];
pub type Amount = u64;
pub type Nonce = u64;

pub const MAX_TRANSACTION_SIZE: usize = 512;
pub const MAX_TICKER_LEN: usize = 8;
pub const MIN_TICKER_LEN: usize = 3;

pub mod serde_signature {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8; 64], s: S) -> Result<S::Ok, S::Error> {
        serde::Serialize::serialize(bytes.as_slice(), s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<[u8; 64], D::Error> {
        let v: Vec<u8> = Deserialize::deserialize(d)?;
        v.try_into()
            .map_err(|_| serde::de::Error::custom("expected 64 bytes"))
    }
}
