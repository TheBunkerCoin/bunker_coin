use crate::staking::{JailRecord, PendingBond, PendingRetire};
use crate::types::{Amount, PublicKey};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochTransitionBlock {
    pub epoch: u64,
    pub last_slot: u64,
    pub fees_distributed: Amount,
    pub bonds_activated: Vec<PendingBond>,
    pub retires_completed: Vec<PendingRetire>,
    pub new_validator_set: Vec<(PublicKey, Amount)>,
    pub state_hash: [u8; 32],
    pub snapshot_chunk_root: [u8; 32],
    pub snapshot_chunk_count: usize,
    pub snapshot_total_bytes: usize,
    pub snapshot_chunk_size: usize,
    pub slashes_applied: Vec<JailRecord>,
    pub deactivated_validators: Vec<PublicKey>,
}
