// Copyright (c) Anza Technology, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Pool for consensus votes, certificates, slot status, and Votor notifications.

mod finality_tracker;
mod parent_ready_tracker;
mod slot_state;

use std::collections::BTreeMap;
use std::ops::RangeBounds;
use std::sync::Arc;

use async_trait::async_trait;
use either::Either;
use log::{debug, info, trace, warn};
use mockall::automock;
use parent_ready_tracker::ParentReadyTracker;
use rocksdb::{DB, IteratorMode, Options};
use slot_state::SlotState;
use thiserror::Error;
use tokio::sync::mpsc::Sender;
use tokio::sync::{RwLock, oneshot};

use self::finality_tracker::FinalityTracker;
use super::blockstore::Blockstore;
use super::votor::VotorEvent;
use super::{Cert, EpochInfo, Vote};
use crate::consensus::cert::NotarCert;
use crate::consensus::pool::finality_tracker::FinalizationEvent;
use crate::crypto::merkle::{BlockHash, MerkleRoot};
use crate::types::{SLOTS_PER_EPOCH, SLOTS_PER_WINDOW};
use crate::{BlockId, Slot, ValidatorId};

#[derive(Clone, Debug)]
pub struct EpochBoundaryEvent {
    pub epoch: u64,
    pub finalized_slot: Slot,
    pub finalization_certs: Vec<Cert>,
}

#[derive(Clone, Debug)]
pub struct FinalizedSlotEvent {
    pub slot: Slot,
    pub finalization_certs: Vec<Cert>,
}

#[derive(Clone, Debug)]
pub struct SlashingReport {
    pub validator_id: ValidatorId,
    pub offence: SlashableOffence,
    pub slot: Slot,
}

/// Errors the Pool may throw when adding a vote or certificate.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum PoolError {
    #[error("slot is either too old or too far in the future")]
    SlotOutOfBounds,
    #[error("invalid signature on the vote")]
    InvalidSignature,
    #[error("duplicate vote")]
    Duplicate,
    #[error("vote constitutes a slashable offence")]
    Slashable(SlashableOffence),
}

/// Errors the Pool may return when adding a certificate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum AddCertError {
    #[error("slot is either too old or too far in the future")]
    SlotOutOfBounds,
    #[error("stake threshold not met")]
    ThresholdNotMet,
    #[error("invalid signature on the cert")]
    InvalidSignature,
    #[error("duplicate cert")]
    Duplicate,
}

/// Slashable offences that may be detected by the Pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum SlashableOffence {
    #[error("Validator {0} already voted notar on slot {1} for a different hash")]
    NotarDifferentHash(ValidatorId, Slot),
    #[error("Validator {0} voted both skip and notarize on slot {1}")]
    SkipAndNotarize(ValidatorId, Slot),
    #[error("Validator {0} voted both skip(-fallback) and finalize on slot {1}")]
    SkipAndFinalize(ValidatorId, Slot),
    #[error("Validator {0} voted both notar-fallback and finalize on slot {1}")]
    NotarFallbackAndFinalize(ValidatorId, Slot),
}

pub type AddVoteError = PoolError;

/// Mockable pool interface.
#[async_trait]
#[automock]
pub trait Pool {
    async fn add_cert(&mut self, cert: Cert) -> Result<(), AddCertError>;
    async fn add_vote(&mut self, vote: Vote) -> Result<(), AddVoteError>;
    async fn add_block(&mut self, block_id: BlockId, parent_id: BlockId);
    async fn recover_from_standstill(&self);
    fn finalized_slot(&self) -> Slot;
    fn has_notar_or_fallback_cert(&self, slot: Slot) -> bool;
    fn has_final_cert(&self, slot: Slot) -> bool;
    fn has_notar_cert(&self, slot: Slot) -> bool;
    fn has_skip_cert(&self, slot: Slot) -> bool;
    fn slot_states_len(&self) -> usize;
    fn prune_old_slots(&mut self);
    fn parents_ready(&self, slot: Slot) -> &[BlockId];
    fn wait_for_parent_ready(&mut self, slot: Slot) -> Either<BlockId, oneshot::Receiver<BlockId>>;
}

/// Central consensus pool for per-slot votes and certificates.
pub struct PoolImpl {
    slot_states: BTreeMap<Slot, SlotState>,
    parent_ready_tracker: ParentReadyTracker,
    finality_tracker: FinalityTracker,
    s2n_waiting_parent_cert: BTreeMap<BlockId, BlockId>,

    epoch_info: Arc<EpochInfo>,
    pub(super) votor_event_channel: Sender<VotorEvent>,
    repair_channel: Sender<BlockId>,

    /// Shared RocksDB handle for persisted certs and metadata.
    db: Arc<DB>,
    blockstore: Option<Arc<RwLock<Box<dyn Blockstore + Send + Sync>>>>,
    epoch_boundary_channel: Option<Sender<EpochBoundaryEvent>>,
    finalized_slot_channel: Option<Sender<FinalizedSlotEvent>>,
    slashing_channel: Option<Sender<SlashingReport>>,

    highest_finalized_slot: Slot,
    highest_notarized_fallback_slot: Slot,
    /// Last epoch boundary emitted; prevents duplicate epoch transitions.
    last_epoch_boundary_fired: Option<u64>,
}

impl PoolImpl {
    /// Creates a new empty pool.
    pub fn new(
        epoch_info: Arc<EpochInfo>,
        votor_event_channel: Sender<VotorEvent>,
        repair_channel: Sender<BlockId>,
    ) -> Self {
        std::fs::create_dir_all("data").ok();
        let db_path = format!("data/pool/{}", epoch_info.own_id);
        Self::new_at(epoch_info, votor_event_channel, repair_channel, &db_path)
    }

    /// Creates a new pool backed by RocksDB at an explicit path.
    pub fn new_at(
        epoch_info: Arc<EpochInfo>,
        votor_event_channel: Sender<VotorEvent>,
        repair_channel: Sender<BlockId>,
        db_path: &str,
    ) -> Self {
        std::fs::create_dir_all(db_path).ok();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db =
            super::blockstore::open_db_with_retry(&opts, db_path).expect("open RocksDB pool db");

        let mut s = Self {
            slot_states: BTreeMap::new(),
            parent_ready_tracker: ParentReadyTracker::default(),
            finality_tracker: FinalityTracker::default(),
            s2n_waiting_parent_cert: BTreeMap::new(),
            epoch_info,
            votor_event_channel,
            repair_channel,
            db,
            blockstore: None,
            epoch_boundary_channel: None,
            finalized_slot_channel: None,
            slashing_channel: None,
            highest_finalized_slot: Slot::genesis(),
            highest_notarized_fallback_slot: Slot::genesis(),
            last_epoch_boundary_fired: None,
        };

        s.load_from_db();
        s
    }

    pub fn set_epoch_boundary_channel(&mut self, tx: Sender<EpochBoundaryEvent>) {
        self.epoch_boundary_channel = Some(tx);
    }

    pub fn set_finalized_slot_channel(&mut self, tx: Sender<FinalizedSlotEvent>) {
        self.finalized_slot_channel = Some(tx);
    }

    pub fn set_slashing_channel(&mut self, tx: Sender<SlashingReport>) {
        self.slashing_channel = Some(tx);
    }

    async fn check_epoch_boundary(&mut self, slot: Slot) {
        if slot.is_last_in_epoch() {
            // Fire at most once per epoch; duplicate events rerun epoch transition.
            let epoch = slot.epoch();
            if self.last_epoch_boundary_fired == Some(epoch) {
                return;
            }
            if let Some(ref tx) = self.epoch_boundary_channel {
                let event = EpochBoundaryEvent {
                    epoch,
                    finalized_slot: slot,
                    finalization_certs: self.get_final_certs(slot),
                };
                let _ = tx.send(event).await;
                self.last_epoch_boundary_fired = Some(epoch);
            }
        }
    }

    async fn notify_finalized_slot(&self, slot: Slot) {
        if let Some(ref tx) = self.finalized_slot_channel {
            let event = FinalizedSlotEvent {
                slot,
                finalization_certs: self.get_final_certs(slot),
            };
            let _ = tx.send(event).await;
        }
    }

    async fn notify_finalization_event(&mut self, event: &FinalizationEvent) {
        // Check direct and implicit finalizations; epoch ends can finalize only by descent.
        if let Some((slot, _)) = &event.finalized {
            self.notify_finalized_slot(*slot).await;
            self.check_epoch_boundary(*slot).await;
        }
        for (slot, _) in &event.implicitly_finalized {
            self.notify_finalized_slot(*slot).await;
            self.check_epoch_boundary(*slot).await;
        }
    }

    /// Sets the blockstore reference for updating finalized timestamps.
    pub fn set_blockstore(&mut self, blockstore: Arc<RwLock<Box<dyn Blockstore + Send + Sync>>>) {
        self.blockstore = Some(blockstore);
    }

    /// Adds a certificate that has already passed pool-level validity checks.
    async fn add_valid_cert(&mut self, cert: Cert) {
        let slot = cert.slot();

        let kind_byte: u8 = match &cert {
            Cert::Notar(_) => 0,
            Cert::NotarFallback(_) => 1,
            Cert::Skip(_) => 2,
            Cert::FastFinal(_) => 3,
            Cert::Final(_) => 4,
        };
        let key = format!("cert|{:016X}|{}", cert.slot(), kind_byte);
        if let Ok(val) = wincode::serialize(&cert) {
            let _ = self.db.put(key.as_bytes(), val);
        }

        trace!("adding cert to pool: {cert:?}");
        self.slot_state(slot).add_cert(cert.clone());

        match &cert {
            Cert::Notar(_) | Cert::NotarFallback(_) => {
                let block_hash = cert.block_hash().cloned().unwrap();
                let block_id = (slot, block_hash.clone());
                info!(
                    "notarized(-fallback) block {} in slot {}",
                    &hex::encode(block_hash.as_hash())[..8],
                    slot
                );
                if matches!(cert, Cert::Notar(_)) {
                    let finalization_event = self
                        .finality_tracker
                        .mark_notarized(slot, block_hash.clone());
                    self.notify_finalization_event(&finalization_event).await;
                    self.handle_finalization(finalization_event).await;
                }

                if let Some((child_slot, child_hash)) =
                    self.s2n_waiting_parent_cert.remove(&block_id)
                    && let Some(output) = self
                        .slot_state(child_slot)
                        .notify_parent_certified(child_hash)
                {
                    match output {
                        Either::Left(event) => {
                            self.votor_event_channel.send(event).await.unwrap();
                        }
                        Either::Right((slot, hash)) => {
                            self.repair_channel.send((slot, hash)).await.unwrap();
                        }
                    }
                }

                let new_parents_ready = self.parent_ready_tracker.mark_notar_fallback(&block_id);
                self.send_parent_ready_events(new_parents_ready).await;

                self.repair_channel.send((slot, block_hash)).await.unwrap();
            }
            Cert::Skip(_) => {
                warn!("skipped slot {slot}");
                let new_parents_ready = self.parent_ready_tracker.mark_skipped(slot);
                self.send_parent_ready_events(new_parents_ready).await;
            }
            Cert::FastFinal(_) => {
                info!("fast finalized slot {slot}");
                self.highest_finalized_slot = slot.max(self.highest_finalized_slot);
                if let Some(hash) = cert.block_hash() {
                    let finalization_event = self
                        .finality_tracker
                        .mark_fast_finalized(slot, hash.clone());
                    self.notify_finalization_event(&finalization_event).await;
                    self.handle_finalization(finalization_event).await;
                }

                if let Some(ref blockstore) = self.blockstore
                    && let Some(hash) = cert.block_hash()
                {
                    let timestamp = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;
                    // Do not drop finalized-status writes under transient lock contention.
                    blockstore.read().await.update_finalized_timestamp(
                        slot,
                        hash.as_hash().clone(),
                        timestamp,
                    );
                }
                // Avoid blockstore.write() here: pool lock + shred-ingest await can deadlock.
                self.prune();
            }
            Cert::Final(_) => {
                info!("slow finalized slot {slot}");
                self.highest_finalized_slot = slot.max(self.highest_finalized_slot);
                let finalization_event = self.finality_tracker.mark_finalized(slot);
                self.notify_finalization_event(&finalization_event).await;
                self.handle_finalization(finalization_event).await;

                if let Some(ref blockstore) = self.blockstore
                    && let Some(state) = self.slot_states.get(&slot)
                    && let Some(ref notar_cert) = state.certificates.notar
                    && let Some(hash) = Cert::Notar(notar_cert.clone()).block_hash()
                {
                    let timestamp = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;
                    // Do not drop finalized-status writes under transient lock contention.
                    blockstore.read().await.update_finalized_timestamp(
                        slot,
                        hash.as_hash().clone(),
                        timestamp,
                    );
                }

                self.prune();
            }
        }

        let event = VotorEvent::CertCreated(Box::new(cert));
        self.votor_event_channel.send(event).await.unwrap();
    }

    /// Mutably accesses or creates the [`SlotState`] for `slot`.
    fn slot_state(&mut self, slot: Slot) -> &mut SlotState {
        self.slot_states
            .entry(slot)
            .or_insert_with(|| SlotState::new(slot, Arc::clone(&self.epoch_info)))
    }

    /// Fetches all certficates for the provided range of `slots`.
    fn get_certs(&self, slots: impl RangeBounds<Slot>) -> Vec<Cert> {
        let mut certs = Vec::new();
        for (_, slot_state) in self.slot_states.range(slots) {
            if let Some(cert) = slot_state.certificates.finalize.clone() {
                certs.push(Cert::Final(cert));
            }
            if let Some(cert) = slot_state.certificates.fast_finalize.clone() {
                certs.push(Cert::FastFinal(cert));
            }
            if let Some(cert) = slot_state.certificates.notar.clone() {
                certs.push(Cert::Notar(cert));
            }
            for cert in slot_state.certificates.notar_fallback.iter().cloned() {
                certs.push(Cert::NotarFallback(cert));
            }
            if let Some(cert) = slot_state.certificates.skip.clone() {
                certs.push(Cert::Skip(cert));
            }
        }
        certs
    }

    /// Fetches finalization certs, preferring fast-final over slow-final+notar.
    fn get_final_certs(&self, slot: Slot) -> Vec<Cert> {
        let Some(slot_state) = self.slot_states.get(&slot) else {
            return Vec::new();
        };
        if let Some(ff_cert) = &slot_state.certificates.fast_finalize {
            return vec![Cert::FastFinal(ff_cert.clone())];
        }
        if let Some(final_cert) = &slot_state.certificates.finalize
            && let Some(notar_cert) = &slot_state.certificates.notar
        {
            return vec![
                Cert::Final(final_cert.clone()),
                Cert::Notar(notar_cert.clone()),
            ];
        }
        Vec::new()
    }

    /// Fetches all votes cast by myself for the provided range of `slots`.
    fn get_own_votes(&self, slots: impl RangeBounds<Slot>) -> Vec<Vote> {
        let mut votes = Vec::new();
        let own_id = self.epoch_info.own_id;
        for (_, slot_state) in self.slot_states.range(slots) {
            if let Some(vote) = &slot_state.votes.finalize[own_id as usize] {
                votes.push(vote.clone());
            }
            if let Some(vote) = &slot_state.votes.notar[own_id as usize] {
                votes.push(vote.clone());
            }
            for vote in slot_state.votes.notar_fallback[own_id as usize].values() {
                votes.push(vote.clone());
            }
            if let Some(vote) = &slot_state.votes.skip[own_id as usize] {
                votes.push(vote.clone());
            }
            if let Some(vote) = &slot_state.votes.skip_fallback[own_id as usize] {
                votes.push(vote.clone());
            }
        }
        votes
    }

    /// Drops finalized slots below the frontier from `slot_states` and the side
    /// trackers (which would otherwise leak one entry per slot forever). Trackers
    /// keep the whole current window, not just the frontier slot: `mark_skipped`
    /// backward-walks the window and the finality recursion stops at the frontier.
    fn prune(&mut self) {
        let last_slot = self.finalized_slot();
        self.slot_states = self.slot_states.split_off(&last_slot);

        let window_start = last_slot.first_slot_in_window();
        self.finality_tracker.prune(window_start);
        self.parent_ready_tracker.prune(window_start);
        // Waiting children below the frontier window can no longer be satisfied.
        self.s2n_waiting_parent_cert
            .retain(|(parent_slot, _), _| *parent_slot >= window_start);
    }

    /// Returns `true` iff `parent` is ready for `slot`.
    pub fn is_parent_ready(&self, slot: Slot, parent: &BlockId) -> bool {
        self.parent_ready_tracker
            .parents_ready(slot)
            .contains(parent)
    }

    /// Returns `true` iff the pool contains a notar(-fallback) certificate for the slot.
    pub fn has_notar_or_fallback_cert(&self, slot: Slot) -> bool {
        self.slot_states.get(&slot).is_some_and(|state| {
            state.certificates.notar.is_some() || !state.certificates.notar_fallback.is_empty()
        })
    }

    /// Returns the hash of the notarized block for the given slot, if any.
    pub fn get_notarized_block(&self, slot: Slot) -> Option<&BlockHash> {
        self.slot_states
            .get(&slot)
            .and_then(|state| state.certificates.notar.as_ref().map(NotarCert::block_hash))
    }

    /// Returns `true` iff the pool contains a (fast) finalization certificate for the slot.
    pub fn has_final_cert(&self, slot: Slot) -> bool {
        self.slot_states.get(&slot).is_some_and(|state| {
            state.certificates.fast_finalize.is_some() || state.certificates.finalize.is_some()
        })
    }

    /// Returns `true` iff the pool contains a notarization certificate for the slot.
    pub fn has_notar_cert(&self, slot: Slot) -> bool {
        self.slot_states
            .get(&slot)
            .is_some_and(|state| state.certificates.notar.is_some())
    }

    /// Returns `true` iff the pool contains a skip certificate for the slot.
    pub fn has_skip_cert(&self, slot: Slot) -> bool {
        self.slot_states
            .get(&slot)
            .is_some_and(|state| state.certificates.skip.is_some())
    }

    async fn handle_finalization(&mut self, event: FinalizationEvent) {
        let new_parents_ready = self.parent_ready_tracker.handle_finalization(event);
        self.send_parent_ready_events(new_parents_ready).await;
    }

    async fn send_parent_ready_events(&self, parents: impl IntoIterator<Item = (Slot, BlockId)>) {
        for (slot, (parent_slot, parent_hash)) in parents {
            debug_assert!(slot.is_start_of_window());
            let event = VotorEvent::ParentReady {
                slot,
                parent_slot,
                parent_hash,
            };
            self.votor_event_channel.send(event).await.unwrap();
        }
    }
}

#[async_trait]
impl Pool for PoolImpl {
    /// Adds a new certificate to the pool. Checks validity of the certificate.
    async fn add_cert(&mut self, cert: Cert) -> Result<(), AddCertError> {
        let slot = cert.slot();
        let slot_far_in_future = Slot::new(self.finalized_slot().inner() + 2 * SLOTS_PER_EPOCH);
        // Allow the finalized slot itself so later notarization can arrive.
        if slot < self.finalized_slot() || slot >= slot_far_in_future {
            return Err(AddCertError::SlotOutOfBounds);
        }

        if !cert.check_threshold(&self.epoch_info) {
            return Err(AddCertError::ThresholdNotMet);
        } else if !cert.check_sig(&self.epoch_info.validators) {
            return Err(AddCertError::InvalidSignature);
        }

        let certs = &mut self.slot_state(slot).certificates;

        let duplicate = match cert {
            Cert::Notar(_) => certs.notar.is_some(),
            Cert::NotarFallback(_) => certs
                .notar_fallback
                .iter()
                .any(|nf| nf.block_hash() == cert.block_hash().unwrap()),
            Cert::Skip(_) => certs.skip.is_some(),
            Cert::FastFinal(_) => certs.fast_finalize.is_some(),
            Cert::Final(_) => certs.finalize.is_some(),
        };
        if duplicate {
            return Err(AddCertError::Duplicate);
        }

        self.add_valid_cert(cert).await;
        Ok(())
    }

    /// Adds a new vote to the pool. Checks validity of the vote.
    async fn add_vote(&mut self, vote: Vote) -> Result<(), AddVoteError> {
        let slot = vote.slot();
        let slot_far_in_future = Slot::new(self.finalized_slot().inner() + 2 * SLOTS_PER_EPOCH);
        if slot < self.finalized_slot() || slot >= slot_far_in_future {
            return Err(AddVoteError::SlotOutOfBounds);
        }

        let pk = &self.epoch_info.validator(vote.signer()).voting_pubkey;
        if !vote.check_sig(pk) {
            return Err(AddVoteError::InvalidSignature);
        }

        let voter = vote.signer();
        let voter_stake = self.epoch_info.validator(voter).stake;
        if let Some(offence) = self.slot_state(slot).check_slashable_offence(&vote) {
            if let Some(ref tx) = self.slashing_channel {
                let report = SlashingReport {
                    validator_id: voter,
                    offence,
                    slot,
                };
                let _ = tx.try_send(report);
            }
            return Err(AddVoteError::Slashable(offence));
        } else if self.slot_state(slot).should_ignore_vote(&vote) {
            return Err(AddVoteError::Duplicate);
        }

        trace!("adding vote to pool: {vote:?}");
        let (new_certs, votor_events, blocks_to_repair) =
            self.slot_state(slot).add_vote(vote, voter_stake);

        for cert in new_certs {
            self.add_valid_cert(cert).await;
        }
        for event in votor_events {
            self.votor_event_channel.send(event).await.unwrap();
        }
        for (slot, block_hash) in blocks_to_repair {
            self.repair_channel.send((slot, block_hash)).await.unwrap();
        }
        Ok(())
    }

    /// Registers a valid block's parent for safe-to-notar checks.
    async fn add_block(&mut self, block_id: BlockId, parent_id: BlockId) {
        // Defense-in-depth: malformed parent links must not panic consensus.
        if block_id.0 <= parent_id.0 {
            warn!(
                "add_block: block slot {} not greater than parent slot {} — dropping",
                block_id.0, parent_id.0
            );
            return;
        }
        let (slot, block_hash) = &block_id;
        let (parent_slot, parent_hash) = &parent_id;

        let finalization_event = self
            .finality_tracker
            .add_parent(block_id.clone(), parent_id.clone());
        self.notify_finalization_event(&finalization_event).await;
        let new_parents_ready = self
            .parent_ready_tracker
            .handle_finalization(finalization_event);
        self.send_parent_ready_events(new_parents_ready).await;

        self.slot_state(*slot)
            .notify_parent_known(block_hash.clone());
        if let Some(parent_state) = self.slot_states.get(parent_slot)
            && parent_state.is_notar_fallback(parent_hash)
            && let Some(output) = self
                .slot_state(*slot)
                .notify_parent_certified(block_hash.clone())
        {
            match output {
                Either::Left(event) => {
                    self.votor_event_channel.send(event).await.unwrap();
                }
                Either::Right((slot, hash)) => {
                    self.repair_channel.send((slot, hash)).await.unwrap();
                }
            }
            return;
        }
        self.s2n_waiting_parent_cert.insert(parent_id, block_id);
    }

    /// Re-broadcasts certs and own votes after a standstill.
    async fn recover_from_standstill(&self) {
        let slot = self.finalized_slot();
        let mut certs = self.get_final_certs(slot);
        // Even without a floor cert, higher certs/votes may unwedge a lossy link.
        if certs.is_empty() {
            warn!(
                "standstill recovery at slot {slot} with no final cert for the floor; \
                 re-broadcasting higher certs and votes only"
            );
        }
        // Include the floor slot itself; lagging peers may need its certs/votes.
        certs.extend(self.get_certs(slot..));
        let votes = self.get_own_votes(slot..);
        if certs.is_empty() && votes.is_empty() {
            warn!("standstill recovery at slot {slot}: nothing at all to re-broadcast");
            return;
        }

        warn!("recovering from standstill at slot {slot}");
        debug!(
            "re-broadcasting {} certificates and {} votes",
            certs.len(),
            votes.len()
        );

        // Target the next slot so Votor ignores it if finality advanced.
        let event = VotorEvent::Standstill(slot.next(), certs, votes);

        self.votor_event_channel.send(event).await.unwrap();
    }

    /// Highest finalized slot, maxing live tracker and persisted restart frontier.
    fn finalized_slot(&self) -> Slot {
        self.finality_tracker
            .highest_finalized_slot()
            .max(self.highest_finalized_slot)
    }

    fn has_notar_or_fallback_cert(&self, slot: Slot) -> bool {
        self.has_notar_or_fallback_cert(slot)
    }

    fn has_final_cert(&self, slot: Slot) -> bool {
        self.has_final_cert(slot)
    }

    fn has_notar_cert(&self, slot: Slot) -> bool {
        self.has_notar_cert(slot)
    }

    fn has_skip_cert(&self, slot: Slot) -> bool {
        self.has_skip_cert(slot)
    }

    fn slot_states_len(&self) -> usize {
        self.slot_states_len()
    }

    fn prune_old_slots(&mut self) {
        self.prune();
    }

    fn parents_ready(&self, slot: Slot) -> &[BlockId] {
        self.parent_ready_tracker.parents_ready(slot)
    }

    fn wait_for_parent_ready(&mut self, slot: Slot) -> Either<BlockId, oneshot::Receiver<BlockId>> {
        self.parent_ready_tracker.wait_for_parent_ready(slot)
    }
}

impl PoolImpl {
    pub fn slot_states_len(&self) -> usize {
        self.slot_states.len()
    }

    fn load_from_db(&mut self) {
        // Verify the persisted floor against the valid certs below.
        let mut meta_final_slot = None;
        if let Ok(Some(val)) = self.db.get(b"meta|final_slot")
            && val.len() == 8
        {
            let arr: [u8; 8] = val[..8].try_into().unwrap();
            meta_final_slot = Some(Slot::new(u64::from_be_bytes(arr)));
        }
        let mut raw_certs: Vec<Cert> = Vec::new();
        let mut highest_nf_slot = Slot::genesis();
        let mut invalid_keys: Vec<Box<[u8]>> = Vec::new();
        for item in self.db.iterator(IteratorMode::Start) {
            if let Ok((k, v)) = item
                && k.starts_with(b"cert|")
                && let Ok(cert) = wincode::deserialize::<Cert>(&v)
            {
                // Drop invalid persisted certs so they cannot pin unaccepted finality.
                if !cert.check_threshold(&self.epoch_info) {
                    warn!(
                        "dropping persisted {} cert for slot {} failing stake \
                                 threshold — was never valid finality",
                        cert.kind_str(),
                        cert.slot()
                    );
                    invalid_keys.push(k);
                    continue;
                }
                match cert {
                    Cert::FastFinal(_) | Cert::Final(_) => {
                        self.highest_finalized_slot = self.highest_finalized_slot.max(cert.slot());
                    }
                    Cert::Notar(_) | Cert::NotarFallback(_) => {
                        highest_nf_slot = highest_nf_slot.max(cert.slot());
                    }
                    _ => {}
                }
                raw_certs.push(cert);
            }
        }
        for k in invalid_keys {
            let _ = self.db.delete(k);
        }

        // Do not roll back the persisted floor; that can delete real chain history.
        if let Some(meta_slot) = meta_final_slot {
            self.highest_finalized_slot = self.highest_finalized_slot.max(meta_slot);
        }

        let retain_up_to = highest_nf_slot.max(self.highest_finalized_slot);

        let certs: Vec<Cert> = raw_certs
            .into_iter()
            .filter(|c| c.slot() <= retain_up_to)
            .collect();
        println!(
            "[Pool::load_from_db] retaining {} certs after filter (<= slot {})",
            certs.len(),
            retain_up_to
        );

        let retain_up_to_inner = retain_up_to.inner();
        for item in self.db.iterator(IteratorMode::Start) {
            if let Ok((k, _v)) = item
                && k.starts_with(b"cert|")
                && k.len() >= 21
                && let Ok(slot_hex) = std::str::from_utf8(&k[5..21])
                && let Ok(slot_val) = u64::from_str_radix(slot_hex, 16)
                && slot_val > retain_up_to_inner
            {
                let _ = self.db.delete(k);
            }
        }

        self.parent_ready_tracker = ParentReadyTracker::default();
        self.slot_states.clear();

        for cert in certs {
            let slot = cert.slot();
            self.slot_state(slot).add_cert(cert.clone());

            match &cert {
                // Fast-final is stronger than notar and must rebuild parent-ready state.
                Cert::Notar(_) | Cert::NotarFallback(_) | Cert::FastFinal(_) => {
                    if let Some(hash) = cert.block_hash() {
                        let block_id = (slot, hash.clone());
                        let newly = self.parent_ready_tracker.mark_notar_fallback(&block_id);
                        for (s, (p_slot, p_hash)) in newly {
                            if s > self.highest_finalized_slot {
                                let _ =
                                    self.votor_event_channel.try_send(VotorEvent::ParentReady {
                                        slot: s,
                                        parent_slot: p_slot,
                                        parent_hash: p_hash,
                                    });
                            }
                        }
                    }
                    self.highest_notarized_fallback_slot =
                        self.highest_notarized_fallback_slot.max(slot);
                }
                Cert::Skip(_) => {
                    let newly = self.parent_ready_tracker.mark_skipped(slot);
                    for (s, (p_slot, p_hash)) in newly {
                        if s > self.highest_finalized_slot {
                            let _ = self.votor_event_channel.try_send(VotorEvent::ParentReady {
                                slot: s,
                                parent_slot: p_slot,
                                parent_hash: p_hash,
                            });
                        }
                    }
                }
                _ => {}
            }
        }

        let _ = self.db.put(
            b"meta|final_slot",
            self.highest_finalized_slot.inner().to_be_bytes(),
        );

        let fin = self.highest_finalized_slot.inner();
        let next_slot = fin + 1;
        let current_window_start = (fin / SLOTS_PER_WINDOW) * SLOTS_PER_WINDOW;
        let current_window_end = current_window_start + SLOTS_PER_WINDOW - 1;

        if fin < current_window_end && fin > 0 {
            // Do not emit genesis-window timeouts before the producer can run.
            println!(
                "[Pool::load_from_db] Mid-window restart detected, emitting timeouts for slots {}..{}",
                next_slot, current_window_end
            );
            for slot in next_slot..=current_window_end {
                println!(
                    "[Pool::load_from_db] emitting Timeout for mid-window slot {}",
                    slot
                );
                let _ = self
                    .votor_event_channel
                    .try_send(VotorEvent::Timeout(Slot::new(slot)));
            }
        } else if fin >= current_window_end {
            let next_window_start = Slot::new(fin + 1);
            if let Some((parent_slot, parent_hash)) = self
                .parent_ready_tracker
                .parents_ready(next_window_start)
                .first()
            {
                println!(
                    "[Pool::load_from_db] Clean window boundary, ParentReady already exists for slot {} (parent {}@{})",
                    next_window_start,
                    &hex::encode(parent_hash.as_hash())[..8],
                    parent_slot
                );
            } else {
                println!(
                    "[Pool::load_from_db] Clean window boundary, but no ParentReady for slot {} yet",
                    next_window_start
                );
            }
        }

        println!(
            "[Pool::load_from_db] finished reload; highest_finalized_slot = {}, highest_notarized_fallback_slot = {}",
            self.highest_finalized_slot, self.highest_notarized_fallback_slot
        );
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;

    use super::*;
    use crate::consensus::cert::{FastFinalCert, NotarCert, SkipCert};
    use crate::consensus::vote::VoteKind;
    use crate::crypto::Hash;
    use crate::crypto::aggsig::SecretKey;
    use crate::crypto::merkle::GENESIS_BLOCK_HASH;
    use crate::test_utils::generate_validators;
    use crate::types::SLOTS_PER_WINDOW;

    #[tokio::test]
    async fn handle_invalid_votes() {
        let (_, epoch_info) = generate_validators(11);
        let (votor_tx, _votor_rx) = mpsc::channel(1024);
        let (repair_tx, _repair_rx) = mpsc::channel(1024);
        let mut pool = PoolImpl::new(epoch_info, votor_tx, repair_tx);

        let wrong_sk = SecretKey::new(&mut rand::rng());
        let vote = Vote::new_notar(Slot::new(0), GENESIS_BLOCK_HASH, &wrong_sk, 0);
        assert_eq!(
            pool.add_vote(vote).await,
            Err(AddVoteError::InvalidSignature)
        );
    }

    #[tokio::test]
    async fn notarize_block() {
        let (sks, epoch_info) = generate_validators(11);
        let (votor_tx, _votor_rx) = mpsc::channel(1024);
        let (repair_tx, _repair_rx) = mpsc::channel(1024);
        let mut pool = PoolImpl::new(epoch_info, votor_tx, repair_tx);

        assert!(!pool.has_notar_cert(Slot::new(0)));
        for v in 0..11 {
            let vote = Vote::new_notar(Slot::new(0), GENESIS_BLOCK_HASH, &sks[v as usize], v);
            assert_eq!(pool.add_vote(vote).await, Ok(()));
        }
        assert!(pool.has_notar_cert(Slot::new(0)));

        assert!(!pool.has_notar_cert(Slot::new(1)));
        for v in 0..7 {
            let vote = Vote::new_notar(Slot::new(1), GENESIS_BLOCK_HASH, &sks[v as usize], v);
            assert_eq!(pool.add_vote(vote).await, Ok(()));
        }
        assert!(pool.has_notar_cert(Slot::new(1)));

        assert!(!pool.has_notar_cert(Slot::new(2)));
        for v in 0..6 {
            let vote = Vote::new_notar(Slot::new(2), GENESIS_BLOCK_HASH, &sks[v as usize], v);
            assert_eq!(pool.add_vote(vote).await, Ok(()));
        }
        assert!(!pool.has_notar_cert(Slot::new(2)));
    }

    #[tokio::test]
    async fn skip_block() {
        let (sks, epoch_info) = generate_validators(11);
        let (votor_tx, _votor_rx) = mpsc::channel(1024);
        let (repair_tx, _repair_rx) = mpsc::channel(1024);
        let mut pool = PoolImpl::new(epoch_info, votor_tx, repair_tx);

        assert!(!pool.has_skip_cert(Slot::new(0)));
        for v in 0..11 {
            let vote = Vote::new_skip(Slot::new(0), &sks[v as usize], v);
            assert_eq!(pool.add_vote(vote).await, Ok(()));
        }
        assert!(pool.has_skip_cert(Slot::new(0)));

        assert!(!pool.has_skip_cert(Slot::new(1)));
        for v in 0..7 {
            let vote = Vote::new_skip(Slot::new(1), &sks[v as usize], v);
            assert_eq!(pool.add_vote(vote).await, Ok(()));
        }
        assert!(pool.has_skip_cert(Slot::new(1)));

        assert!(!pool.has_skip_cert(Slot::new(2)));
        for v in 0..6 {
            let vote = Vote::new_skip(Slot::new(2), &sks[v as usize], v);
            assert_eq!(pool.add_vote(vote).await, Ok(()));
        }
        assert!(!pool.has_skip_cert(Slot::new(2)));
    }

    #[tokio::test]
    async fn finalize_block() {
        let (sks, epoch_info) = generate_validators(11);
        let (votor_tx, _votor_rx) = mpsc::channel(1024);
        let (repair_tx, _repair_rx) = mpsc::channel(1024);
        let mut pool = PoolImpl::new(epoch_info, votor_tx, repair_tx);

        let slot1 = Slot::genesis().next();
        let hash1: BlockHash = Hash::random_for_test().into();
        for v in 0..7 {
            let vote = Vote::new_notar(slot1, hash1.clone(), &sks[v as usize], v);
            assert_eq!(pool.add_vote(vote).await, Ok(()));
        }
        assert!(!pool.has_final_cert(slot1));
        assert_eq!(pool.finalized_slot(), Slot::genesis());

        for v in 0..7 {
            let vote = Vote::new_final(slot1, &sks[v as usize], v);
            assert_eq!(pool.add_vote(vote).await, Ok(()));
        }
        assert!(pool.has_final_cert(slot1));
        assert_eq!(pool.finalized_slot(), slot1);

        let slot2 = slot1.next();
        for v in 0..7 {
            let vote = Vote::new_final(slot2, &sks[v as usize], v);
            assert_eq!(pool.add_vote(vote).await, Ok(()));
        }
        assert!(pool.has_final_cert(slot2));
        assert_eq!(pool.finalized_slot(), slot1);

        let hash2: BlockHash = Hash::random_for_test().into();
        for v in 0..7 {
            let vote = Vote::new_notar(slot2, hash2.clone(), &sks[v as usize], v);
            assert_eq!(pool.add_vote(vote).await, Ok(()));
        }
        assert!(pool.has_final_cert(slot2));
        assert_eq!(pool.finalized_slot(), slot2);

        let slot3 = slot2.next();
        let hash3: BlockHash = Hash::random_for_test().into();
        for v in 0..6 {
            let vote = Vote::new_notar(slot3, hash3.clone(), &sks[v as usize], v);
            assert_eq!(pool.add_vote(vote).await, Ok(()));
            let vote = Vote::new_final(slot3, &sks[v as usize], v);
            assert_eq!(pool.add_vote(vote).await, Ok(()));
        }
        assert!(!pool.has_final_cert(slot3));
        assert_eq!(pool.finalized_slot(), slot2);
    }

    #[tokio::test]
    async fn fast_finalize_block() {
        let (sks, epoch_info) = generate_validators(11);
        let (votor_tx, _votor_rx) = mpsc::channel(1024);
        let (repair_tx, _repair_rx) = mpsc::channel(1024);
        let mut pool = PoolImpl::new(epoch_info, votor_tx, repair_tx);

        assert!(!pool.has_final_cert(Slot::new(0)));
        for v in 0..11 {
            let vote = Vote::new_notar(Slot::new(0), GENESIS_BLOCK_HASH, &sks[v as usize], v);
            assert_eq!(pool.add_vote(vote).await, Ok(()));
        }
        assert!(pool.has_final_cert(Slot::new(0)));
        assert_eq!(pool.finalized_slot(), Slot::new(0));

        assert!(!pool.has_final_cert(Slot::new(1)));
        for v in 0..9 {
            let vote = Vote::new_notar(Slot::new(1), GENESIS_BLOCK_HASH, &sks[v as usize], v);
            assert_eq!(pool.add_vote(vote).await, Ok(()));
        }
        assert!(pool.has_final_cert(Slot::new(1)));
        assert_eq!(pool.finalized_slot(), Slot::new(1));

        assert!(!pool.has_final_cert(Slot::new(2)));
        for v in 0..8 {
            let vote = Vote::new_notar(Slot::new(2), GENESIS_BLOCK_HASH, &sks[v as usize], v);
            assert_eq!(pool.add_vote(vote).await, Ok(()));
        }
        assert!(!pool.has_final_cert(Slot::new(2)));
        assert_eq!(pool.finalized_slot(), Slot::new(1));
    }

    #[tokio::test]
    async fn simple_branch_certified() {
        let (sks, epoch_info) = generate_validators(11);
        let (votor_tx, _votor_rx) = mpsc::channel(1024);
        let (repair_tx, _repair_rx) = mpsc::channel(1024);
        let mut pool = PoolImpl::new(epoch_info, votor_tx, repair_tx);

        let window = Slot::genesis().slots_in_window().collect::<Vec<_>>();
        let hashes: Vec<BlockHash> = window
            .iter()
            .map(|_| Hash::random_for_test().into())
            .collect();
        for slot in window.iter().skip(1) {
            for v in 0..7 {
                let vote = Vote::new_notar(
                    *slot,
                    hashes[slot.inner() as usize].clone(),
                    &sks[v as usize],
                    v,
                );
                assert_eq!(pool.add_vote(vote).await, Ok(()));
            }
        }
        let slot = *window.last().unwrap();
        let next = slot.next();
        assert!(pool.is_parent_ready(next, &(slot, hashes[next.inner() as usize - 1].clone())));
    }

    #[tokio::test]
    async fn branch_certified_notar_fallback() {
        let (sks, epoch_info) = generate_validators(11);
        let (votor_tx, _votor_rx) = mpsc::channel(1024);
        let (repair_tx, _repair_rx) = mpsc::channel(1024);
        let mut pool = PoolImpl::new(epoch_info, votor_tx, repair_tx);

        let window = Slot::genesis().slots_in_window().collect::<Vec<_>>();
        let hashes: Vec<BlockHash> = window
            .iter()
            .map(|_| Hash::random_for_test().into())
            .collect();
        for slot in window.iter().skip(1) {
            let hash = &hashes[slot.inner() as usize];
            assert!(!pool.is_parent_ready(slot.next(), &(*slot, hash.clone())));
            for v in 0..4 {
                let vote = Vote::new_notar(*slot, hash.clone(), &sks[v as usize], v);
                assert_eq!(pool.add_vote(vote).await, Ok(()));
            }
            for v in 4..7 {
                let vote = Vote::new_notar_fallback(*slot, hash.clone(), &sks[v as usize], v);
                assert_eq!(pool.add_vote(vote).await, Ok(()));
            }
        }
        let slot = *window.last().unwrap();
        let next = slot.next();
        let hash = hashes[next.inner() as usize - 1].clone();
        assert!(pool.is_parent_ready(next, &(slot, hash)));
    }

    #[tokio::test]
    async fn branch_certified_out_of_order() {
        let (sks, epoch_info) = generate_validators(11);
        let (votor_tx, _votor_rx) = mpsc::channel(1024);
        let (repair_tx, _repair_rx) = mpsc::channel(1024);
        let mut pool = PoolImpl::new(epoch_info, votor_tx, repair_tx);

        let mut window = Slot::new(0).slots_in_window().collect::<Vec<_>>();
        assert!(window.len() > 2);
        window.remove(0);
        window.remove(0);
        for slot in window.iter() {
            for v in 0..7 {
                let vote = Vote::new_skip(*slot, &sks[v as usize], v);
                assert_eq!(pool.add_vote(vote).await, Ok(()));
            }
        }

        let next = window.last().unwrap().next();
        assert!(pool.parents_ready(next).is_empty());

        let slot1 = Slot::new(1);
        let hash1: BlockHash = Hash::random_for_test().into();
        for v in 0..7 {
            let vote = Vote::new_notar(slot1, hash1.clone(), &sks[v as usize], v);
            assert_eq!(pool.add_vote(vote).await, Ok(()));
        }

        assert!(pool.is_parent_ready(next, &(slot1, hash1)));
        assert_eq!(pool.parents_ready(next).len(), 1);
    }

    #[tokio::test]
    async fn branch_certified_late_cert() {
        let (sks, epoch_info) = generate_validators(11);
        let (votor_tx, _votor_rx) = mpsc::channel(1024);
        let (repair_tx, _repair_rx) = mpsc::channel(1024);
        let mut pool = PoolImpl::new(epoch_info.clone(), votor_tx, repair_tx);

        let window = Slot::genesis().slots_in_window().collect::<Vec<_>>();
        assert!(window.len() > 2);
        for slot in window.iter().skip(2) {
            for v in 0..7 {
                let vote = Vote::new_skip(*slot, &sks[v as usize], v);
                assert_eq!(pool.add_vote(vote).await, Ok(()));
            }
        }

        let next = window.last().unwrap().next();
        assert!(pool.parents_ready(next).is_empty());

        let slot1 = Slot::new(1);
        let hash1: BlockHash = Hash::random_for_test().into();
        let mut votes = Vec::new();
        for v in 0..7 {
            votes.push(Vote::new_notar(slot1, hash1.clone(), &sks[v as usize], v));
        }
        let cert = NotarCert::try_new(&votes, &epoch_info.validators).unwrap();
        pool.add_cert(Cert::Notar(cert)).await.unwrap();

        assert!(pool.is_parent_ready(next, &(slot1, hash1)));
    }

    #[tokio::test]
    async fn regular_handover() {
        let (sks, epoch_info) = generate_validators(11);
        let (votor_tx, _votor_rx) = mpsc::channel(1024);
        let (repair_tx, _repair_rx) = mpsc::channel(1024);
        let mut pool = PoolImpl::new(epoch_info, votor_tx, repair_tx);

        let hashes: Vec<BlockHash> = (0..SLOTS_PER_WINDOW)
            .map(|_| Hash::random_for_test().into())
            .collect();

        for slot in 1..SLOTS_PER_WINDOW {
            let hash = &hashes[slot as usize];
            for v in 0..7 {
                let vote = Vote::new_notar(Slot::new(slot), hash.clone(), &sks[v as usize], v);
                assert_eq!(pool.add_vote(vote).await, Ok(()));
            }
        }

        assert!(pool.is_parent_ready(
            Slot::new(SLOTS_PER_WINDOW),
            &(
                Slot::new(SLOTS_PER_WINDOW - 1),
                hashes[(SLOTS_PER_WINDOW - 1) as usize].clone()
            )
        ));
    }

    #[tokio::test]
    async fn one_skip_handover() {
        let (sks, epoch_info) = generate_validators(11);
        let (votor_tx, _votor_rx) = mpsc::channel(1024);
        let (repair_tx, _repair_rx) = mpsc::channel(1024);
        let mut pool = PoolImpl::new(epoch_info, votor_tx, repair_tx);

        let hashes: Vec<BlockHash> = (0..SLOTS_PER_WINDOW)
            .map(|_| Hash::random_for_test().into())
            .collect();

        for slot in 1..SLOTS_PER_WINDOW - 1 {
            for v in 0..7 {
                let vote = Vote::new_notar(
                    Slot::new(slot),
                    hashes[slot as usize].clone(),
                    &sks[v as usize],
                    v,
                );
                assert_eq!(pool.add_vote(vote).await, Ok(()));
            }
        }

        for v in 0..7 {
            let vote = Vote::new_skip(Slot::new(SLOTS_PER_WINDOW - 1), &sks[v as usize], v);
            assert_eq!(pool.add_vote(vote).await, Ok(()));
        }

        assert!(pool.is_parent_ready(
            Slot::new(SLOTS_PER_WINDOW),
            &(
                Slot::new(SLOTS_PER_WINDOW - 2),
                hashes[(SLOTS_PER_WINDOW - 2) as usize].clone()
            )
        ));
    }

    #[tokio::test]
    async fn two_skip_handover() {
        let (sks, epoch_info) = generate_validators(11);
        let (votor_tx, _votor_rx) = mpsc::channel(1024);
        let (repair_tx, _repair_rx) = mpsc::channel(1024);
        let mut pool = PoolImpl::new(epoch_info, votor_tx, repair_tx);

        let hashes: Vec<BlockHash> = (0..SLOTS_PER_WINDOW)
            .map(|_| Hash::random_for_test().into())
            .collect::<Vec<_>>();

        for slot in 1..SLOTS_PER_WINDOW - 2 {
            for v in 0..7 {
                let vote = Vote::new_notar(
                    Slot::new(slot),
                    hashes[slot as usize].clone(),
                    &sks[v as usize],
                    v,
                );
                assert_eq!(pool.add_vote(vote).await, Ok(()));
            }
        }

        for v in 0..7 {
            let vote = Vote::new_skip(Slot::new(SLOTS_PER_WINDOW - 2), &sks[v as usize], v);
            assert_eq!(pool.add_vote(vote).await, Ok(()));
        }
        for v in 0..7 {
            let vote = Vote::new_skip(Slot::new(SLOTS_PER_WINDOW - 1), &sks[v as usize], v);
            assert_eq!(pool.add_vote(vote).await, Ok(()));
        }

        assert!(pool.is_parent_ready(
            Slot::new(SLOTS_PER_WINDOW),
            &(
                Slot::new(SLOTS_PER_WINDOW - 3),
                hashes[(SLOTS_PER_WINDOW - 3) as usize].clone()
            )
        ));
    }

    #[tokio::test]
    async fn skip_window_handover() {
        let (sks, epoch_info) = generate_validators(11);
        let (votor_tx, _votor_rx) = mpsc::channel(1024);
        let (repair_tx, _repair_rx) = mpsc::channel(1024);
        let mut pool = PoolImpl::new(epoch_info, votor_tx, repair_tx);

        let hashes: Vec<BlockHash> = (0..SLOTS_PER_WINDOW)
            .map(|_| Hash::random_for_test().into())
            .collect();

        for slot in 1..SLOTS_PER_WINDOW {
            for v in 0..7 {
                let vote = Vote::new_notar(
                    Slot::new(slot),
                    hashes[slot as usize].clone(),
                    &sks[v as usize],
                    v,
                );
                assert_eq!(pool.add_vote(vote).await, Ok(()));
            }
        }

        for slot in 0..SLOTS_PER_WINDOW {
            for v in 0..7 {
                let vote = Vote::new_skip(Slot::new(SLOTS_PER_WINDOW + slot), &sks[v as usize], v);
                assert_eq!(pool.add_vote(vote).await, Ok(()));
            }
        }

        assert!(pool.is_parent_ready(
            Slot::new(2 * SLOTS_PER_WINDOW),
            &(
                Slot::new(SLOTS_PER_WINDOW - 1),
                hashes[(SLOTS_PER_WINDOW - 1) as usize].clone()
            )
        ));
    }

    #[tokio::test]
    async fn pruning() {
        let (sks, epoch_info) = generate_validators(11);
        let (votor_tx, _votor_rx) = mpsc::channel(1024);
        let (repair_tx, _repair_rx) = mpsc::channel(1024);
        let mut pool = PoolImpl::new(epoch_info, votor_tx, repair_tx);

        let hashes: Vec<BlockHash> = (0..3 * SLOTS_PER_WINDOW + 10)
            .map(|_| Hash::random_for_test().into())
            .collect();

        for slot in 1..3 * SLOTS_PER_WINDOW {
            let slot = Slot::new(slot);
            let hash: &BlockHash = &hashes[slot.inner() as usize];
            assert!(!pool.has_final_cert(slot));
            for v in 0..11 {
                let vote = Vote::new_notar(slot, hash.clone(), &sks[v as usize], v);
                assert_eq!(pool.add_vote(vote).await, Ok(()));
            }
            assert!(pool.has_final_cert(slot));
        }
        let last_slot = Slot::new(3 * SLOTS_PER_WINDOW - 1);
        assert_eq!(pool.finalized_slot(), last_slot);

        for slot in 0..last_slot.inner() {
            let slot = Slot::new(slot);
            assert!(!pool.slot_states.contains_key(&slot));
        }
        assert!(pool.slot_states.contains_key(&(last_slot)));

        for s in 1..=10 {
            let slot = Slot::new(last_slot.inner() + s);
            let hash: &BlockHash = &hashes[slot.inner() as usize];
            for v in 0..8 {
                let vote = Vote::new_notar(slot, hash.clone(), &sks[v as usize], v);
                assert_eq!(pool.add_vote(vote).await, Ok(()));
            }
            assert!(!pool.has_final_cert(slot));
        }
        assert_eq!(pool.finalized_slot(), last_slot);

        for s in 0..=10 {
            let slot = Slot::new(last_slot.inner() + s);
            assert!(pool.slot_states.contains_key(&slot));
        }

        for s in 1..=10 {
            let slot = Slot::new(last_slot.inner() + s);
            let hash: &BlockHash = &hashes[slot.inner() as usize];
            let vote = Vote::new_notar(slot, hash.clone(), &sks[8], 8);
            assert_eq!(pool.add_vote(vote).await, Ok(()));
            assert!(pool.has_final_cert(slot));
        }
        assert_eq!(pool.finalized_slot().inner(), last_slot.inner() + 10);

        for s in 0..10 {
            let slot = Slot::new(last_slot.inner() + s);
            assert!(!pool.slot_states.contains_key(&slot));
        }
        let new_last_slot = Slot::new(last_slot.inner() + 10);
        assert!(pool.slot_states.contains_key(&new_last_slot));
    }

    #[tokio::test]
    async fn duplicate_votes() {
        let (sks, epoch_info) = generate_validators(11);
        let (votor_tx, _votor_rx) = mpsc::channel(1024);
        let (repair_tx, _repair_rx) = mpsc::channel(1024);
        let mut pool = PoolImpl::new(epoch_info, votor_tx, repair_tx);

        let vote = Vote::new_notar(Slot::new(0), GENESIS_BLOCK_HASH, &sks[0], 0);
        assert_eq!(pool.add_vote(vote).await, Ok(()));

        let vote = Vote::new_skip(Slot::new(0), &sks[1], 1);
        assert_eq!(pool.add_vote(vote).await, Ok(()));

        let vote = Vote::new_notar(Slot::new(0), GENESIS_BLOCK_HASH, &sks[0], 0);
        assert_eq!(pool.add_vote(vote).await, Err(AddVoteError::Duplicate));
        let vote = Vote::new_skip(Slot::new(0), &sks[1], 1);
        assert_eq!(pool.add_vote(vote).await, Err(AddVoteError::Duplicate));
    }

    #[tokio::test]
    async fn duplicate_certs() {
        let (sks, epoch_info) = generate_validators(11);
        let (votor_tx, _votor_rx) = mpsc::channel(1024);
        let (repair_tx, _repair_rx) = mpsc::channel(1024);
        let mut pool = PoolImpl::new(epoch_info.clone(), votor_tx, repair_tx);

        let mut votes = Vec::new();
        let first_slot = Slot::genesis().next();
        let hash: BlockHash = Hash::random_for_test().into();
        for v in 0..11 {
            votes.push(Vote::new_notar(
                first_slot,
                hash.clone(),
                &sks[v as usize],
                v,
            ));
        }
        let notar_cert = NotarCert::try_new(&votes, &epoch_info.validators).unwrap();
        assert_eq!(pool.add_cert(Cert::Notar(notar_cert.clone())).await, Ok(()));

        let mut votes = Vec::new();
        let second_slot = first_slot.next();
        for v in 0..11 {
            votes.push(Vote::new_skip(second_slot, &sks[v as usize], v));
        }
        let skip_cert = SkipCert::try_new(&votes, &epoch_info.validators).unwrap();
        assert_eq!(pool.add_cert(Cert::Skip(skip_cert.clone())).await, Ok(()));

        assert_eq!(
            pool.add_cert(Cert::Notar(notar_cert)).await,
            Err(AddCertError::Duplicate)
        );
        assert_eq!(
            pool.add_cert(Cert::Skip(skip_cert)).await,
            Err(AddCertError::Duplicate)
        );
    }

    #[tokio::test]
    async fn out_of_bounds_votes() {
        let (sks, epoch_info) = generate_validators(11);
        let (votor_tx, _votor_rx) = mpsc::channel(1024);
        let (repair_tx, _repair_rx) = mpsc::channel(1024);
        let mut pool = PoolImpl::new(epoch_info, votor_tx, repair_tx);

        let slot = Slot::new(3 * SLOTS_PER_WINDOW - 1);
        for v in 0..11 {
            let vote = Vote::new_notar(slot, GENESIS_BLOCK_HASH, &sks[v as usize], v);
            assert_eq!(pool.add_vote(vote).await, Ok(()));
        }
        assert_eq!(pool.finalized_slot(), slot);

        for slot in 0..3 * SLOTS_PER_WINDOW - 1 {
            for v in 0..11 {
                let vote = Vote::new_final(Slot::new(slot), &sks[v as usize], v);
                assert_eq!(
                    pool.add_vote(vote).await,
                    Err(AddVoteError::SlotOutOfBounds)
                );
            }
        }

        let slot = Slot::new(5 * SLOTS_PER_EPOCH);
        for v in 0..11 {
            let vote = Vote::new_final(slot, &sks[v as usize], v);
            assert_eq!(
                pool.add_vote(vote).await,
                Err(AddVoteError::SlotOutOfBounds)
            );
        }
    }

    #[tokio::test]
    async fn out_of_bounds_certs() {
        let (sks, epoch_info) = generate_validators(11);
        let (votor_tx, _votor_rx) = mpsc::channel(1024);
        let (repair_tx, _repair_rx) = mpsc::channel(1024);
        let mut pool = PoolImpl::new(epoch_info.clone(), votor_tx, repair_tx);

        let slot = Slot::new(3 * SLOTS_PER_WINDOW - 1);
        let mut votes = Vec::new();
        for v in 0..11 {
            votes.push(Vote::new_notar(
                slot,
                GENESIS_BLOCK_HASH,
                &sks[v as usize],
                v,
            ));
        }
        let ff_cert = FastFinalCert::try_new(&votes, &epoch_info.validators).unwrap();
        assert_eq!(
            pool.add_cert(Cert::FastFinal(ff_cert.clone())).await,
            Ok(())
        );

        for slot in 0..3 * SLOTS_PER_WINDOW - 1 {
            let mut votes = Vec::new();
            for v in 0..11 {
                votes.push(Vote::new_skip(Slot::new(slot), &sks[v as usize], v));
            }
            let skip_cert = SkipCert::try_new(&votes, &epoch_info.validators).unwrap();
            assert_eq!(
                pool.add_cert(Cert::Skip(skip_cert.clone())).await,
                Err(AddCertError::SlotOutOfBounds)
            );
        }

        let slot = Slot::new(3 * SLOTS_PER_EPOCH);
        let mut votes = Vec::new();
        for v in 0..11 {
            votes.push(Vote::new_skip(slot, &sks[v as usize], v));
        }
        let skip_cert = SkipCert::try_new(&votes, &epoch_info.validators).unwrap();
        assert_eq!(
            pool.add_cert(Cert::Skip(skip_cert.clone())).await,
            Err(AddCertError::SlotOutOfBounds)
        );
    }

    #[tokio::test]
    async fn standstill_recovery() {
        let (sks, epoch_info) = generate_validators(11);
        let (votor_tx, mut votor_rx) = mpsc::channel(1024);
        let (repair_tx, _repair_rx) = mpsc::channel(1024);
        let mut pool = PoolImpl::new(epoch_info, votor_tx, repair_tx);

        let slot1 = Slot::genesis().next();
        let hash1: BlockHash = Hash::random_for_test().into();
        for v in 0..11 {
            let vote = Vote::new_notar(slot1, hash1.clone(), &sks[v as usize], v);
            assert_eq!(pool.add_vote(vote).await, Ok(()));
        }

        let slot2 = slot1.next();
        for v in 0..7 {
            let vote = Vote::new_final(slot2, &sks[v as usize], v);
            assert_eq!(pool.add_vote(vote).await, Ok(()));
        }

        let slot3 = slot2.next();
        let vote = Vote::new_notar(slot3, Hash::random_for_test().into(), &sks[0], 0);
        assert_eq!(pool.add_vote(vote).await, Ok(()));

        pool.recover_from_standstill().await;

        let (slot, certs, votes) = loop {
            let event = votor_rx.recv().await.unwrap();
            match event {
                VotorEvent::CertCreated(_) => {
                    continue;
                }
                VotorEvent::Standstill(slot, certs, votes) => {
                    break (slot, certs, votes);
                }
                _ => unreachable!("unexpected event {event:?}"),
            }
        };

        assert_eq!(slot, slot2);
        assert_eq!(certs.len(), 2);
        for cert in certs {
            if matches!(cert, Cert::FastFinal(_)) {
                assert_eq!(cert.slot(), slot1);
            } else if matches!(cert, Cert::Final(_)) {
                assert_eq!(cert.slot(), slot2);
            } else {
                unreachable!("unexpected cert {cert:?}");
            }
        }
        assert_eq!(votes.len(), 2);
        for vote in votes {
            assert_eq!(vote.signer(), 0);
            if matches!(vote.kind(), VoteKind::Final(_)) {
                assert_eq!(vote.kind().slot(), slot2);
            } else if matches!(vote.kind(), VoteKind::Notar(_, _)) {
                assert_eq!(vote.kind().slot(), slot3);
            } else {
                unreachable!("unexpected vote {vote:?}");
            }
        }
    }

    /// Malformed parent links must be dropped without panicking consensus.
    #[tokio::test]
    async fn add_block_rejects_non_earlier_parent_without_panic() {
        let (_sks, epoch_info) = generate_validators(11);
        let (votor_tx, mut votor_rx) = mpsc::channel(1024);
        let (repair_tx, _repair_rx) = mpsc::channel(1024);
        let mut pool = PoolImpl::new(epoch_info, votor_tx, repair_tx);

        let slot = Slot::new(5);
        let (hash, parent_hash): (BlockHash, BlockHash) = (
            Hash::random_for_test().into(),
            Hash::random_for_test().into(),
        );

        pool.add_block((slot, hash.clone()), (slot, parent_hash.clone()))
            .await;
        pool.add_block((slot, hash), (slot.next(), parent_hash))
            .await;

        // Dropped malformed blocks must not emit consensus events.
        assert!(
            matches!(votor_rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
            "a malformed block must not drive any consensus events"
        );
    }

    /// Epoch-boundary events fire even when the boundary slot finalizes implicitly.
    #[tokio::test]
    async fn epoch_boundary_fires_on_implicit_finalization() {
        let (sks, epoch_info) = generate_validators(11);
        let (votor_tx, _votor_rx) = mpsc::channel(1024);
        let (repair_tx, _repair_rx) = mpsc::channel(1024);
        let (epoch_tx, mut epoch_rx) = mpsc::channel(16);
        // Isolate the DB because this test persists far-future certs.
        let db_path = format!(
            "{}/bunkerglow-pool-epoch-boundary-{}",
            std::env::temp_dir().display(),
            std::process::id()
        );
        let _ = std::fs::remove_dir_all(&db_path);
        let mut pool = PoolImpl::new_at(epoch_info, votor_tx, repair_tx, &db_path);
        pool.set_epoch_boundary_channel(epoch_tx);

        let boundary = Slot::new(SLOTS_PER_EPOCH - 1);
        assert!(boundary.is_last_in_epoch());
        let next = boundary.next();
        assert_eq!(next.epoch(), 1);
        let (bhash, nhash): (BlockHash, BlockHash) = (
            Hash::random_for_test().into(),
            Hash::random_for_test().into(),
        );

        // Leave the boundary notarized, not directly finalized.
        for v in 0..7 {
            let vote = Vote::new_notar(boundary, bhash.clone(), &sks[v as usize], v);
            assert_eq!(pool.add_vote(vote).await, Ok(()));
        }
        assert!(pool.has_notar_cert(boundary));

        // Fast-finalizing `next` finalizes `boundary` implicitly by descent.
        pool.add_block((next, nhash.clone()), (boundary, bhash.clone()))
            .await;
        for v in 0..9 {
            let vote = Vote::new_notar(next, nhash.clone(), &sks[v as usize], v);
            assert_eq!(pool.add_vote(vote).await, Ok(()));
        }
        assert!(pool.has_final_cert(next));

        let mut saw_epoch_0 = false;
        while let Ok(ev) = epoch_rx.try_recv() {
            if ev.epoch == 0 {
                assert_eq!(ev.finalized_slot, boundary);
                saw_epoch_0 = true;
            }
        }
        assert!(
            saw_epoch_0,
            "epoch-boundary event for epoch 0 must fire when slot {boundary} is \
             finalized only implicitly"
        );
    }

    /// Finalization prunes side trackers, not just `slot_states`.
    #[tokio::test]
    async fn prune_shrinks_side_trackers() {
        let (sks, epoch_info) = generate_validators(11);
        let (votor_tx, _votor_rx) = mpsc::channel(1024);
        let (repair_tx, _repair_rx) = mpsc::channel(1024);
        let db_path = format!(
            "{}/bunkerglow-pool-prune-trackers-{}",
            std::env::temp_dir().display(),
            std::process::id()
        );
        let _ = std::fs::remove_dir_all(&db_path);
        let mut pool = PoolImpl::new_at(epoch_info, votor_tx, repair_tx, &db_path);

        let hashes: Vec<BlockHash> = (0..=5).map(|_| Hash::random_for_test().into()).collect();
        for slot in 2..=5u64 {
            pool.add_block(
                (Slot::new(slot), hashes[slot as usize].clone()),
                (Slot::new(slot - 1), hashes[slot as usize - 1].clone()),
            )
            .await;
        }
        assert_eq!(pool.finality_tracker.parents_len(), 4);

        // Slot 5 fast-finalizes the chain and prunes at window start 4.
        for v in 0..11 {
            let vote = Vote::new_notar(Slot::new(5), hashes[5].clone(), &sks[v as usize], v);
            assert_eq!(pool.add_vote(vote).await, Ok(()));
        }
        assert_eq!(pool.finalized_slot(), Slot::new(5));

        assert_eq!(pool.finality_tracker.parents_len(), 2);
        assert_eq!(pool.finality_tracker.status_len(), 2);
        assert!(pool.parent_ready_tracker.len() <= 3);
        assert!(pool.s2n_waiting_parent_cert.len() <= 1);
        assert!(
            pool.s2n_waiting_parent_cert
                .keys()
                .all(|(parent_slot, _)| *parent_slot >= Slot::new(4))
        );
    }

    #[tokio::test]
    async fn parent_ready_upon_finalization() {
        let (sks, epoch_info) = generate_validators(11);
        let (votor_tx, mut votor_rx) = mpsc::channel(1024);
        let (repair_tx, _repair_rx) = mpsc::channel(1024);
        let mut pool = PoolImpl::new(epoch_info, votor_tx, repair_tx);

        let slot1 = Slot::windows().nth(1).unwrap();
        let slot0 = slot1.prev();
        let slot2 = slot1.next();
        let (hash0, hash1, hash2): (BlockHash, BlockHash, BlockHash) = (
            Hash::random_for_test().into(),
            Hash::random_for_test().into(),
            Hash::random_for_test().into(),
        );
        for v in 0..11 {
            let vote = Vote::new_notar(slot2, hash2.clone(), &sks[v as usize], v);
            assert_eq!(pool.add_vote(vote).await, Ok(()));
        }

        for _ in 0..3 {
            let event = votor_rx.recv().await;
            assert!(matches!(event, Some(VotorEvent::CertCreated(_))));
        }

        assert_eq!(
            votor_rx.try_recv().err(),
            Some(mpsc::error::TryRecvError::Empty)
        );

        pool.add_block((slot2, hash2.clone()), (slot1, hash1.clone()))
            .await;
        pool.add_block((slot1, hash1.clone()), (slot0, hash0.clone()))
            .await;

        let Ok(event) = votor_rx.try_recv() else {
            panic!("expected to receive ParentReady event");
        };
        match event {
            VotorEvent::ParentReady {
                slot,
                parent_slot,
                parent_hash,
            } => {
                assert_eq!(slot, slot1);
                assert_eq!(parent_slot, slot0);
                assert_eq!(parent_hash, hash0);
            }
            _ => unreachable!("unexpected event {event:?}"),
        }
    }
}
