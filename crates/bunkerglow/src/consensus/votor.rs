// Copyright (c) Anza Technology, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Main voting logic for the consensus protocol.
//!
//! [`Votor`] keeps per-slot voting state, consumes [`VotorEvent`]s, and
//! broadcasts votes over [`All2All`].

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use color_eyre::Result;
use log::{debug, trace, warn};
use tokio::sync::mpsc::{Receiver, Sender};

use super::blockstore::BlockInfo;
use super::link_liveness::{LinkLiveness, NoLiveness};
use super::vote_history::VoteHistory;
use super::{Cert, Vote, delta_block, delta_timeout};
use crate::consensus::{delta_final_vote_grace, delta_first_slice};
use crate::crypto::aggsig::SecretKey;
use crate::crypto::merkle::{BlockHash, GENESIS_BLOCK_HASH, MerkleRoot};
use crate::{All2All, Slot, ValidatorId};

/// Inputs that drive Votor's voting loop.
#[derive(Clone, Debug)]
pub enum VotorEvent {
    /// Pool marked a ready parent; `slot` is the window's first slot.
    ParentReady {
        slot: Slot,
        parent_slot: Slot,
        parent_hash: BlockHash,
    },
    /// The block reached safe-to-notar status.
    SafeToNotar(Slot, BlockHash),
    /// The slot reached safe-to-skip status.
    SafeToSkip(Slot),
    /// New cert created in pool; Votor should broadcast it.
    CertCreated(Box<Cert>),
    /// Standstill fired; re-broadcast the provided certs and votes.
    Standstill(Slot, Vec<Cert>, Vec<Vote>),

    /// First valid shred of the leader's block was received.
    FirstShred(Slot),
    /// A complete block was received in blockstore.
    Block { slot: Slot, block_info: BlockInfo },

    /// Regular slot timeout fired.
    Timeout(Slot),
    /// Crashed-leader timeout fired (nothing received).
    TimeoutCrashedLeader(Slot),
    /// Deferred-final grace elapsed; fall back to slow-path final if needed.
    FinalVoteDeadline(Slot, BlockHash),
}

/// Decides which votes to cast from per-slot state and incoming events.
pub struct Votor<A: All2All> {
    voted: BTreeSet<Slot>,
    voted_notar: BTreeMap<Slot, BlockHash>,
    bad_window: BTreeSet<Slot>,
    /// Bounds crashed-leader timeout re-arms while an alive link is still slow.
    crashed_leader_rearms: BTreeMap<Slot, u32>,
    block_notarized: BTreeMap<Slot, BlockHash>,
    parents_ready: BTreeSet<(Slot, Slot, BlockHash)>,
    received_shred: BTreeSet<Slot>,
    pending_blocks: BTreeMap<Slot, BlockInfo>,
    retired_slots: BTreeSet<Slot>,

    validator_id: ValidatorId,
    voting_key: SecretKey,
    event_receiver: Receiver<VotorEvent>,
    event_sender: Sender<VotorEvent>,
    all2all: Arc<A>,
    /// Slow-but-alive links pause crashed-leader timeout instead of skipping.
    link_liveness: Arc<dyn LinkLiveness>,
    /// Defers slow-path final votes so fast-finalized slots send fewer radio messages.
    defer_final_vote: bool,
    fast_finalized: BTreeSet<Slot>,
    /// Durable own-vote log prevents conflicting votes after restart.
    vote_history: VoteHistory,
    /// Replayed once at startup so in-flight pre-crash votes are not lost forever.
    restored_votes: Vec<Vote>,
    /// Re-armed after restart so slow-final liveness survives a grace-window crash.
    restored_final_deadlines: Vec<(Slot, BlockHash)>,
    /// Events below this pruned floor are dropped to avoid conflicting stale votes.
    finalized_floor: Slot,
}

impl<A: All2All> Votor<A> {
    /// Creates a new Votor instance with empty state.
    pub fn new(
        validator_id: ValidatorId,
        voting_key: SecretKey,
        event_sender: Sender<VotorEvent>,
        event_receiver: Receiver<VotorEvent>,
        all2all: Arc<A>,
    ) -> Self {
        // Seed genesis state.
        let voted = [Slot::genesis()].into_iter().collect();
        let voted_notar = [(Slot::genesis(), GENESIS_BLOCK_HASH)]
            .into_iter()
            .collect();
        let block_notarized = [(Slot::genesis(), GENESIS_BLOCK_HASH)]
            .into_iter()
            .collect();
        let parents_ready = [(Slot::genesis(), Slot::genesis(), GENESIS_BLOCK_HASH)]
            .into_iter()
            .collect();
        let retired_slots = [Slot::genesis()].into_iter().collect();

        let votor = Self {
            voted,
            voted_notar,
            bad_window: BTreeSet::new(),
            crashed_leader_rearms: BTreeMap::new(),
            block_notarized,
            parents_ready,
            received_shred: BTreeSet::new(),
            pending_blocks: BTreeMap::new(),
            retired_slots,
            validator_id,
            voting_key,
            event_receiver,
            event_sender,
            all2all,
            link_liveness: Arc::new(NoLiveness),
            // Env-gated so radio can opt in without changing the Alpenglow API.
            defer_final_vote: std::env::var("BUNKER_DEFER_FINAL_VOTE")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false),
            fast_finalized: BTreeSet::new(),
            vote_history: VoteHistory::disabled(),
            restored_votes: Vec::new(),
            restored_final_deadlines: Vec::new(),
            finalized_floor: Slot::genesis(),
        };
        votor.set_timeouts(Slot::new(0));
        votor
    }

    /// Inject a link-liveness signal; defaults to [`NoLiveness`].
    pub fn set_link_liveness(&mut self, liveness: Arc<dyn LinkLiveness>) {
        self.link_liveness = liveness;
    }

    /// Inject the durable own-vote log and rebuild restart voting guards from it.
    pub fn set_vote_history(&mut self, history: VoteHistory, finalized_slot: Slot) {
        self.vote_history = history;
        // The floor guard must drop stale events for slots pruned from history.
        self.finalized_floor = finalized_slot;
        for vote in self.vote_history.load_and_prune(finalized_slot) {
            let slot = vote.slot();
            if vote.is_notar() {
                self.voted.insert(slot);
                if let Some(hash) = vote.block_hash() {
                    self.voted_notar.insert(slot, hash.clone());
                }
            } else if vote.is_skip() {
                self.voted.insert(slot);
                self.bad_window.insert(slot);
            } else if vote.is_notar_fallback() || vote.is_skip_fallback() {
                self.bad_window.insert(slot);
            } else if vote.is_final() {
                self.retired_slots.insert(slot);
            }
            self.restored_votes.push(vote);
        }

        // Re-arm pending-final markers; retired-slot markers are stale.
        for (slot, hash) in self
            .vote_history
            .load_and_prune_pending_finals(finalized_slot)
        {
            if !self.retired_slots.contains(&slot) {
                self.restored_final_deadlines.push((slot, hash));
            } else {
                self.vote_history.clear_pending_final(slot, &hash);
            }
        }
    }

    /// Enable deferred slow-path finalization votes.
    #[cfg(test)]
    pub fn set_defer_final_vote(&mut self, defer: bool) {
        self.defer_final_vote = defer;
    }

    /// Drop state below `floor`; safe only with the voting-loop floor guard.
    fn prune_below_floor(&mut self, floor: Slot) {
        if floor <= self.finalized_floor {
            return;
        }
        self.finalized_floor = floor;
        self.voted = self.voted.split_off(&floor);
        self.voted_notar = self.voted_notar.split_off(&floor);
        self.bad_window = self.bad_window.split_off(&floor);
        self.crashed_leader_rearms = self.crashed_leader_rearms.split_off(&floor);
        self.block_notarized = self.block_notarized.split_off(&floor);
        self.received_shred = self.received_shred.split_off(&floor);
        self.pending_blocks = self.pending_blocks.split_off(&floor);
        self.retired_slots = self.retired_slots.split_off(&floor);
        self.fast_finalized = self.fast_finalized.split_off(&floor);
        self.parents_ready.retain(|(slot, _, _)| *slot >= floor);
    }

    /// Re-arm cap for slow-but-alive crashed-leader timeouts.
    const MAX_CRASHED_LEADER_REARMS: u32 = 5;

    /// Handle consensus voting events and broadcast resulting votes.
    #[fastrace::trace]
    pub async fn voting_loop(&mut self) -> Result<()> {
        // Re-send restored votes once; peers deduplicate identical votes.
        for vote in std::mem::take(&mut self.restored_votes) {
            debug!("rebroadcasting restored vote for slot {}", vote.slot());
            self.all2all.broadcast(&vote.into()).await.unwrap();
        }
        // Use the live-path grace delay; the enabling notar cert may arrive after restart.
        for (slot, hash) in std::mem::take(&mut self.restored_final_deadlines) {
            debug!("re-arming deferred-final deadline for slot {slot} after restart");
            let sender = self.event_sender.clone();
            tokio::spawn(async move {
                tokio::time::sleep(delta_final_vote_grace()).await;
                let _ = sender.send(VotorEvent::FinalVoteDeadline(slot, hash)).await;
            });
        }
        while let Some(event) = self.event_receiver.recv().await {
            // Drop stale voting events below the pruned floor; cert rebroadcasts are safe.
            if event.slot() < self.finalized_floor
                && !matches!(
                    event,
                    VotorEvent::CertCreated(_) | VotorEvent::Standstill(..)
                )
            {
                trace!(
                    "ignoring event below finalized floor {}: {event:?}",
                    self.finalized_floor
                );
                continue;
            }
            if self.retired_slots.contains(&event.slot()) {
                trace!("ignoring event for retired slot {}", event.slot());
                continue;
            }
            trace!("votor event: {event:?}");
            match event {
                VotorEvent::ParentReady {
                    slot,
                    parent_slot,
                    parent_hash,
                } => {
                    let h = &hex::encode(parent_hash.as_hash())[..8];
                    trace!("slot {slot} has new valid parent {h} in slot {parent_slot}");
                    self.parents_ready.insert((slot, parent_slot, parent_hash));
                    self.check_pending_blocks().await;
                    self.set_timeouts(slot);
                }
                VotorEvent::SafeToNotar(slot, hash) => {
                    debug!("voted notar-fallback in slot {slot}");
                    let vote =
                        Vote::new_notar_fallback(slot, hash, &self.voting_key, self.validator_id);
                    self.vote_history.record(&vote);
                    self.all2all.broadcast(&vote.into()).await.unwrap();
                    self.try_skip_window(slot).await;
                    self.bad_window.insert(slot);
                }
                VotorEvent::SafeToSkip(slot) => {
                    debug!("voted skip-fallback in slot {slot}");
                    let vote = Vote::new_skip_fallback(slot, &self.voting_key, self.validator_id);
                    self.vote_history.record(&vote);
                    self.all2all.broadcast(&vote.into()).await.unwrap();
                    self.try_skip_window(slot).await;
                    self.bad_window.insert(slot);
                }
                VotorEvent::CertCreated(cert) => {
                    match cert.as_ref() {
                        Cert::Notar(_) => {
                            self.block_notarized
                                .insert(cert.slot(), cert.block_hash().cloned().unwrap());
                            // Deferred mode waits for the grace deadline before slow-final voting.
                            if !self.defer_final_vote {
                                self.try_final(cert.slot(), cert.block_hash().cloned().unwrap())
                                    .await;
                            }
                        }
                        Cert::FastFinal(_) => {
                            // Fast-final suppresses any deferred slow-path final vote.
                            self.fast_finalized.insert(cert.slot());
                            // Drop the pending marker so restart does not re-arm it.
                            if let Some(hash) = cert.block_hash() {
                                self.vote_history.clear_pending_final(cert.slot(), hash);
                            }
                            let first_slot_in_window = cert.slot().first_slot_in_window();
                            self.vote_history.prune(first_slot_in_window);
                            self.prune_below_floor(first_slot_in_window);
                            self.set_timeouts(first_slot_in_window);
                        }
                        Cert::Final(_) => {
                            let first_slot_in_window = cert.slot().first_slot_in_window();
                            self.vote_history.prune(first_slot_in_window);
                            self.prune_below_floor(first_slot_in_window);
                            self.set_timeouts(first_slot_in_window);
                        }
                        _ => {}
                    }
                    self.all2all.broadcast(&(*cert).into()).await.unwrap();
                }
                VotorEvent::Standstill(_, certs, votes) => {
                    for cert in certs {
                        self.all2all.broadcast(&cert.into()).await.unwrap();
                    }
                    for vote in votes {
                        self.all2all.broadcast(&vote.into()).await.unwrap();
                    }
                }

                VotorEvent::FirstShred(slot) => {
                    self.received_shred.insert(slot);
                }
                VotorEvent::Block { slot, block_info } => {
                    println!(
                        "[Votor {}] BLOCK slot {} info {:?}",
                        self.validator_id, slot, block_info
                    );
                    if self.voted.contains(&slot) {
                        let h = &hex::encode(block_info.hash.as_hash())[..8];
                        warn!("not voting for block {h} in slot {slot}, already voted");
                        continue;
                    }
                    if self.try_notar(slot, block_info.clone()).await {
                        self.check_pending_blocks().await;
                    } else {
                        self.pending_blocks.insert(slot, block_info);
                    }
                }

                VotorEvent::Timeout(slot) => {
                    trace!("timeout for slot {slot}");
                    if !self.voted.contains(&slot) {
                        self.try_skip_window(slot).await;
                    }
                }
                VotorEvent::TimeoutCrashedLeader(slot) => {
                    trace!("timeout (crashed leader) for slot {slot}");
                    if !self.received_shred.contains(&slot) && !self.voted.contains(&slot) {
                        // On slow live links, pause before skipping to avoid gapping the chain.
                        let rearms = self.crashed_leader_rearms.entry(slot).or_insert(0);
                        if self.link_liveness.is_link_alive()
                            && *rearms < Self::MAX_CRASHED_LEADER_REARMS
                        {
                            *rearms += 1;
                            println!(
                                "[Votor {}] EARLY_TIMEOUT slot {} — link alive, pausing (re-arm {}/{})",
                                self.validator_id,
                                slot,
                                *rearms,
                                Self::MAX_CRASHED_LEADER_REARMS
                            );
                            let sender = self.event_sender.clone();
                            tokio::spawn(async move {
                                tokio::time::sleep(delta_timeout()).await;
                                let _ = sender.send(VotorEvent::TimeoutCrashedLeader(slot)).await;
                            });
                        } else {
                            // Link is down or the pause budget is exhausted; skip.
                            println!(
                                "[Votor {}] EARLY_TIMEOUT slot {} — skipping window",
                                self.validator_id, slot
                            );
                            self.try_skip_window(slot).await;
                        }
                    }
                }
                VotorEvent::FinalVoteDeadline(slot, hash) => {
                    // Grace elapsed; `try_final` is a no-op if fast-final already landed.
                    self.try_final(slot, hash).await;
                }
            }
        }

        Ok(())
    }

    /// Sets timeouts for the leader window starting at the given `slot`.
    ///
    /// # Panics
    ///
    /// Panics if `slot` is not the first slot of a window.
    fn set_timeouts(&self, slot: Slot) {
        assert!(slot.is_start_of_window());

        trace!(
            "setting timeouts for slots {slot}-{}",
            slot.last_slot_in_window()
        );
        let sender = self.event_sender.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delta_timeout() + delta_first_slice()).await;
            // Ignore send errors during shutdown.
            let event = VotorEvent::TimeoutCrashedLeader(slot);
            let _ = sender.send(event).await;
            for s in slot.slots_in_window() {
                if s.is_start_of_window() {
                    tokio::time::sleep(delta_block() - delta_first_slice()).await;
                } else {
                    tokio::time::sleep(delta_block()).await;
                }
                let event = VotorEvent::Timeout(s);
                let _ = sender.send(event).await;
            }
        });
    }

    /// Sends a notarization vote for the given block if the conditions are met.
    ///
    /// Returns `true` iff we decided to send a notarization vote for the block.
    async fn try_notar(&mut self, slot: Slot, block_info: BlockInfo) -> bool {
        let BlockInfo {
            hash,
            parent: (parent_slot, parent_hash),
        } = block_info;
        let first_slot = slot.first_slot_in_window();
        if slot == first_slot {
            let valid_parent =
                self.parents_ready
                    .contains(&(slot, parent_slot, parent_hash.clone()));
            if !valid_parent {
                return false;
            }
        } else if parent_slot != slot.prev()
            || self.voted_notar.get(&parent_slot) != Some(&parent_hash)
        {
            return false;
        }
        let vote = Vote::new_notar(slot, hash.clone(), &self.voting_key, self.validator_id);
        self.vote_history.record(&vote);
        self.all2all.broadcast(&vote.into()).await.unwrap();
        self.voted.insert(slot);
        self.voted_notar.insert(slot, hash.clone());
        self.pending_blocks.remove(&slot);
        if self.defer_final_vote {
            // Persist the deferred-final intent before spawning the memory-only timer.
            self.vote_history.record_pending_final(slot, &hash);
            let sender = self.event_sender.clone();
            let h = hash.clone();
            tokio::spawn(async move {
                tokio::time::sleep(delta_final_vote_grace()).await;
                let _ = sender.send(VotorEvent::FinalVoteDeadline(slot, h)).await;
            });
        } else {
            self.try_final(slot, hash).await;
        }
        true
    }

    /// Sends a finalization vote for the given block if the conditions are met.
    async fn try_final(&mut self, slot: Slot, hash: BlockHash) {
        // Fast-final makes the slow-path final vote redundant.
        if self.fast_finalized.contains(&slot) {
            self.vote_history.clear_pending_final(slot, &hash);
            return;
        }
        let notarized = self.block_notarized.get(&slot) == Some(&hash);
        let voted_notar = self.voted_notar.get(&slot) == Some(&hash);
        let not_bad = !self.bad_window.contains(&slot);
        if notarized && voted_notar && not_bad {
            let vote = Vote::new_final(slot, &self.voting_key, self.validator_id);
            self.vote_history.record(&vote);
            self.all2all.broadcast(&vote.into()).await.unwrap();
            self.retired_slots.insert(slot);
            // Fulfilled intent; restart must not re-arm it.
            self.vote_history.clear_pending_final(slot, &hash);
        }
    }

    /// Sends skip votes for all unvoted slots in the window that `slot` belongs to.
    async fn try_skip_window(&mut self, slot: Slot) {
        trace!("try skip window of slot {slot}");
        for s in slot.slots_in_window() {
            if self.voted.insert(s) {
                let vote = Vote::new_skip(s, &self.voting_key, self.validator_id);
                self.vote_history.record(&vote);
                self.all2all.broadcast(&vote.into()).await.unwrap();
                self.bad_window.insert(s);
                debug!("voted skip for slot {s}");
            }
        }
    }

    /// Checks if we can vote on any of the pending blocks by now.
    async fn check_pending_blocks(&mut self) {
        let slots: Vec<_> = self.pending_blocks.keys().copied().collect();
        for slot in &slots {
            if let Some(block_info) = self.pending_blocks.get(slot) {
                self.try_notar(*slot, block_info.clone()).await;
            }
        }
    }
}

impl VotorEvent {
    const fn slot(&self) -> Slot {
        match self {
            Self::ParentReady { slot, .. }
            | Self::SafeToNotar(slot, _)
            | Self::SafeToSkip(slot)
            | Self::Standstill(slot, _, _)
            | Self::FirstShred(slot)
            | Self::Block { slot, .. }
            | Self::Timeout(slot)
            | Self::TimeoutCrashedLeader(slot)
            | Self::FinalVoteDeadline(slot, _) => *slot,
            Self::CertCreated(cert) => cert.slot(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::mpsc;

    use super::*;
    use crate::all2all::TrivialAll2All;
    use crate::consensus::cert::{FastFinalCert, NotarCert};
    use crate::consensus::{ConsensusMessage, EpochInfo};
    use crate::crypto::Hash;
    use crate::network::SimulatedNetwork;
    use crate::test_utils::{generate_all2all_instances, generate_validators};

    type A2A = TrivialAll2All<SimulatedNetwork<ConsensusMessage, ConsensusMessage>>;

    async fn start_votor() -> (A2A, mpsc::Sender<VotorEvent>, Arc<EpochInfo>) {
        start_votor_with_liveness(None).await
    }

    struct TestLiveness(bool);
    impl LinkLiveness for TestLiveness {
        fn is_link_alive(&self) -> bool {
            self.0
        }
    }

    async fn start_votor_with_liveness(
        liveness: Option<Arc<dyn LinkLiveness>>,
    ) -> (A2A, mpsc::Sender<VotorEvent>, Arc<EpochInfo>) {
        start_votor_full(liveness, false).await
    }

    async fn start_votor_full(
        liveness: Option<Arc<dyn LinkLiveness>>,
        defer_final_vote: bool,
    ) -> (A2A, mpsc::Sender<VotorEvent>, Arc<EpochInfo>) {
        let (sks, epoch_info) = generate_validators(2);
        let mut a2a = generate_all2all_instances(epoch_info.validators.clone()).await;
        let (tx, rx) = mpsc::channel(100);
        let other_a2a = a2a.pop().unwrap();
        let votor_a2a = a2a.pop().unwrap();
        let mut votor = Votor::new(0, sks[0].clone(), tx.clone(), rx, Arc::new(votor_a2a));
        if let Some(l) = liveness {
            votor.set_link_liveness(l);
        }
        votor.set_defer_final_vote(defer_final_vote);
        tokio::spawn(async move {
            votor.voting_loop().await.unwrap();
        });
        (other_a2a, tx, epoch_info)
    }

    /// Crashed-leader timeout skips when the link is down.
    #[tokio::test]
    async fn crashed_leader_skips_when_link_down() {
        let (other_a2a, tx, _) =
            start_votor_with_liveness(Some(Arc::new(TestLiveness(false)))).await;

        // Pick an unvoted non-genesis window.
        let slot = Slot::new(4);
        assert!(slot.is_start_of_window());
        tx.send(VotorEvent::TimeoutCrashedLeader(slot))
            .await
            .unwrap();

        match tokio::time::timeout(Duration::from_secs(2), other_a2a.receive()).await {
            Ok(Ok(ConsensusMessage::Vote(v))) => {
                assert!(v.is_skip(), "expected skip vote, got {v:?}");
            }
            other => panic!("expected a skip vote, got {other:?}"),
        }
    }

    /// Crashed-leader timeout pauses when the link is alive.
    #[tokio::test]
    async fn crashed_leader_pauses_when_link_alive() {
        let (other_a2a, tx, _) =
            start_votor_with_liveness(Some(Arc::new(TestLiveness(true)))).await;

        let slot = Slot::genesis().next().first_slot_in_window();
        tx.send(VotorEvent::TimeoutCrashedLeader(slot))
            .await
            .unwrap();

        // A short receive timeout proves the handler paused instead of skipped.
        let got = tokio::time::timeout(Duration::from_secs(2), other_a2a.receive()).await;
        assert!(
            got.is_err(),
            "link alive: must pause, not skip — but saw a vote: {got:?}"
        );
    }

    #[tokio::test]
    async fn timeouts() {
        let (other_a2a, _, _) = start_votor().await;

        let mut skipped_slots = Vec::new();
        let mut slots = Slot::genesis().slots_in_window().collect::<Vec<_>>();
        slots.remove(0);
        for _ in slots.clone() {
            if let Ok(msg) = other_a2a.receive().await {
                match msg {
                    ConsensusMessage::Vote(v) => {
                        assert!(v.is_skip());
                        skipped_slots.push(v.slot());
                    }
                    m => panic!("other msg: {m:?}"),
                }
            }
        }
        assert_eq!(skipped_slots, slots);
    }

    #[tokio::test]
    async fn notar_and_final() {
        let (other_a2a, tx, epoch_info) = start_votor().await;

        let slot = Slot::genesis().next();
        let event = VotorEvent::FirstShred(slot);
        tx.send(event).await.unwrap();
        let block_info = BlockInfo {
            hash: Hash::random_for_test().into(),
            parent: (Slot::genesis(), GENESIS_BLOCK_HASH),
        };
        let event = VotorEvent::Block { slot, block_info };
        tx.send(event).await.unwrap();
        let vote = match other_a2a.receive().await.unwrap() {
            ConsensusMessage::Vote(v) => v,
            m => panic!("other msg: {m:?}"),
        };
        assert!(vote.is_notar());
        assert_eq!(vote.slot(), slot);

        let cert = Cert::Notar(NotarCert::new_unchecked(&[vote], &epoch_info.validators));
        let event = VotorEvent::CertCreated(Box::new(cert));
        tx.send(event).await.unwrap();
        match other_a2a.receive().await.unwrap() {
            ConsensusMessage::Vote(v) => {
                assert!(v.is_final());
                assert_eq!(v.slot(), slot);
            }
            m => panic!("other msg: {m:?}"),
        }
    }

    /// Deferred final vote is suppressed when fast-final arrives.
    #[tokio::test]
    async fn defer_final_vote_suppressed_on_fast_final() {
        let (other_a2a, tx, epoch_info) = start_votor_full(None, true).await;

        let slot = Slot::genesis().next();
        tx.send(VotorEvent::FirstShred(slot)).await.unwrap();
        let hash: BlockHash = Hash::random_for_test().into();
        let block_info = BlockInfo {
            hash: hash.clone(),
            parent: (Slot::genesis(), GENESIS_BLOCK_HASH),
        };
        tx.send(VotorEvent::Block { slot, block_info })
            .await
            .unwrap();

        let notar = match other_a2a.receive().await.unwrap() {
            ConsensusMessage::Vote(v) => v,
            m => panic!("expected notar vote, got {m:?}"),
        };
        assert!(notar.is_notar());

        let notar_cert = Cert::Notar(NotarCert::new_unchecked(
            std::slice::from_ref(&notar),
            &epoch_info.validators,
        ));
        tx.send(VotorEvent::CertCreated(Box::new(notar_cert)))
            .await
            .unwrap();
        match other_a2a.receive().await.unwrap() {
            ConsensusMessage::Cert(c) => assert!(matches!(c, Cert::Notar(_))),
            ConsensusMessage::Vote(v) => panic!("unexpected vote in deferred mode: {v:?}"),
        }

        // The later deadline must see fast-final and emit no vote.
        let ff = Cert::FastFinal(FastFinalCert::new_unchecked(
            std::slice::from_ref(&notar),
            &epoch_info.validators,
        ));
        tx.send(VotorEvent::CertCreated(Box::new(ff)))
            .await
            .unwrap();
        match other_a2a.receive().await.unwrap() {
            ConsensusMessage::Cert(c) => assert!(matches!(c, Cert::FastFinal(_))),
            ConsensusMessage::Vote(v) => panic!("unexpected vote: {v:?}"),
        }

        tx.send(VotorEvent::FinalVoteDeadline(slot, hash))
            .await
            .unwrap();
        let leftover = tokio::time::timeout(Duration::from_secs(2), other_a2a.receive()).await;
        assert!(
            leftover.is_err(),
            "fast-final must suppress the final vote, but saw: {leftover:?}"
        );
    }

    /// Deferred final vote falls back to slow-final when fast-final does not arrive.
    #[tokio::test]
    async fn defer_final_vote_falls_back_to_slow_final() {
        let (other_a2a, tx, epoch_info) = start_votor_full(None, true).await;

        let slot = Slot::genesis().next();
        tx.send(VotorEvent::FirstShred(slot)).await.unwrap();
        let hash: BlockHash = Hash::random_for_test().into();
        let block_info = BlockInfo {
            hash: hash.clone(),
            parent: (Slot::genesis(), GENESIS_BLOCK_HASH),
        };
        tx.send(VotorEvent::Block { slot, block_info })
            .await
            .unwrap();

        let notar = match other_a2a.receive().await.unwrap() {
            ConsensusMessage::Vote(v) => v,
            m => panic!("expected notar vote, got {m:?}"),
        };
        assert!(notar.is_notar());

        let notar_cert = Cert::Notar(NotarCert::new_unchecked(
            std::slice::from_ref(&notar),
            &epoch_info.validators,
        ));
        tx.send(VotorEvent::CertCreated(Box::new(notar_cert)))
            .await
            .unwrap();
        match other_a2a.receive().await.unwrap() {
            ConsensusMessage::Cert(c) => assert!(matches!(c, Cert::Notar(_))),
            ConsensusMessage::Vote(v) => panic!("premature final vote: {v:?}"),
        }

        tx.send(VotorEvent::FinalVoteDeadline(slot, hash))
            .await
            .unwrap();
        match other_a2a.receive().await.unwrap() {
            ConsensusMessage::Vote(v) => {
                assert!(v.is_final(), "expected final vote fallback, got {v:?}");
                assert_eq!(v.slot(), slot);
            }
            m => panic!("expected final vote, got {m:?}"),
        }
    }

    /// Restored vote history prevents conflicting skip votes after restart.
    #[tokio::test]
    async fn vote_history_prevents_conflicting_skip_after_restart() {
        let db_path = format!(
            "{}/bunkerglow-votor-restart-{}",
            std::env::temp_dir().display(),
            std::process::id()
        );
        let _ = std::fs::remove_dir_all(&db_path);

        let (sks, _epoch_info) = generate_validators(2);
        let slot = Slot::genesis().next();
        let hash: BlockHash = Hash::random_for_test().into();

        // First run records a notar vote.
        {
            let mut a2a = generate_all2all_instances(_epoch_info.validators.clone()).await;
            let (tx, rx) = mpsc::channel(100);
            let other_a2a = a2a.pop().unwrap();
            let votor_a2a = a2a.pop().unwrap();
            let mut votor = Votor::new(0, sks[0].clone(), tx.clone(), rx, Arc::new(votor_a2a));
            votor.set_vote_history(VoteHistory::open_at(&db_path), Slot::genesis());
            tokio::spawn(async move {
                votor.voting_loop().await.unwrap();
            });

            tx.send(VotorEvent::FirstShred(slot)).await.unwrap();
            let block_info = BlockInfo {
                hash: hash.clone(),
                parent: (Slot::genesis(), GENESIS_BLOCK_HASH),
            };
            tx.send(VotorEvent::Block { slot, block_info })
                .await
                .unwrap();
            let vote = match other_a2a.receive().await.unwrap() {
                ConsensusMessage::Vote(v) => v,
                m => panic!("expected notar vote, got {m:?}"),
            };
            assert!(vote.is_notar());
            assert_eq!(vote.slot(), slot);
        } // votor task's channel sender drops → first life ends ("crash")

        // Restart with the same history.
        let mut a2a = generate_all2all_instances(_epoch_info.validators.clone()).await;
        let (tx, rx) = mpsc::channel(100);
        let other_a2a = a2a.pop().unwrap();
        let votor_a2a = a2a.pop().unwrap();
        let mut votor = Votor::new(0, sks[0].clone(), tx.clone(), rx, Arc::new(votor_a2a));
        votor.set_vote_history(VoteHistory::open_at(&db_path), Slot::genesis());
        tokio::spawn(async move {
            votor.voting_loop().await.unwrap();
        });

        match other_a2a.receive().await.unwrap() {
            ConsensusMessage::Vote(v) => {
                assert!(v.is_notar(), "expected rebroadcast notar vote, got {v:?}");
                assert_eq!(v.slot(), slot);
                assert_eq!(v.block_hash(), Some(&hash));
            }
            m => panic!("expected rebroadcast notar vote, got {m:?}"),
        }

        // The restored guard must block a conflicting skip vote.
        tx.send(VotorEvent::Timeout(slot)).await.unwrap();
        let got = tokio::time::timeout(Duration::from_secs(2), other_a2a.receive()).await;
        assert!(
            got.is_err(),
            "restored votor must not skip a slot it already notar-voted, but sent: {got:?}"
        );
    }

    /// Pending-final markers re-arm slow-final liveness after restart.
    #[tokio::test]
    async fn deferred_final_vote_rearmed_after_crash() {
        let db_path = format!(
            "{}/bunkerglow-votor-deferfinal-{}",
            std::env::temp_dir().display(),
            std::process::id()
        );
        let _ = std::fs::remove_dir_all(&db_path);

        let (sks, epoch_info) = generate_validators(2);
        let slot = Slot::genesis().next();
        let hash: BlockHash = Hash::random_for_test().into();

        // First run persists a deferred-final intent.
        {
            let mut a2a = generate_all2all_instances(epoch_info.validators.clone()).await;
            let (tx, rx) = mpsc::channel(100);
            let other_a2a = a2a.pop().unwrap();
            let votor_a2a = a2a.pop().unwrap();
            let mut votor = Votor::new(0, sks[0].clone(), tx.clone(), rx, Arc::new(votor_a2a));
            votor.set_defer_final_vote(true);
            votor.set_vote_history(VoteHistory::open_at(&db_path), Slot::genesis());
            tokio::spawn(async move {
                votor.voting_loop().await.unwrap();
            });

            tx.send(VotorEvent::FirstShred(slot)).await.unwrap();
            let block_info = BlockInfo {
                hash: hash.clone(),
                parent: (Slot::genesis(), GENESIS_BLOCK_HASH),
            };
            tx.send(VotorEvent::Block { slot, block_info })
                .await
                .unwrap();

            let vote = match other_a2a.receive().await.unwrap() {
                ConsensusMessage::Vote(v) => v,
                m => panic!("expected notar vote, got {m:?}"),
            };
            assert!(vote.is_notar());
        }

        // Restart with the same DB and deferred-final mode.
        let mut a2a = generate_all2all_instances(epoch_info.validators.clone()).await;
        let (tx, rx) = mpsc::channel(100);
        let other_a2a = a2a.pop().unwrap();
        let votor_a2a = a2a.pop().unwrap();
        let mut votor = Votor::new(0, sks[0].clone(), tx.clone(), rx, Arc::new(votor_a2a));
        votor.set_defer_final_vote(true);
        votor.set_vote_history(VoteHistory::open_at(&db_path), Slot::genesis());
        tokio::spawn(async move {
            votor.voting_loop().await.unwrap();
        });

        // Startup rebroadcasts the restored notar vote.
        let rebroadcast = match other_a2a.receive().await.unwrap() {
            ConsensusMessage::Vote(v) => v,
            m => panic!("expected rebroadcast notar vote, got {m:?}"),
        };
        assert!(rebroadcast.is_notar());

        let notar_cert = Cert::Notar(NotarCert::new_unchecked(
            std::slice::from_ref(&rebroadcast),
            &epoch_info.validators,
        ));
        tx.send(VotorEvent::CertCreated(Box::new(notar_cert)))
            .await
            .unwrap();
        match other_a2a.receive().await.unwrap() {
            ConsensusMessage::Cert(c) => assert!(matches!(c, Cert::Notar(_))),
            ConsensusMessage::Vote(v) => panic!("unexpected vote before deadline: {v:?}"),
        }

        // Allow for the re-armed grace timer to drive the final vote.
        let final_vote = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                match other_a2a.receive().await.unwrap() {
                    ConsensusMessage::Vote(v) if v.is_final() => break v,
                    ConsensusMessage::Vote(v) => panic!("unexpected non-final vote: {v:?}"),
                    ConsensusMessage::Cert(_) => continue,
                }
            }
        })
        .await
        .expect("re-armed deferred-final deadline must send the finalization vote");
        assert!(final_vote.is_final());
        assert_eq!(final_vote.slot(), slot);
    }

    /// `prune_below_floor` prunes below the floor and keeps the floor monotone.
    #[tokio::test]
    async fn prune_below_floor_sweeps_per_slot_state() {
        let (sks, epoch_info) = generate_validators(2);
        let mut a2a = generate_all2all_instances(epoch_info.validators.clone()).await;
        let (tx, rx) = mpsc::channel(16);
        let mut votor = Votor::new(0, sks[0].clone(), tx, rx, Arc::new(a2a.pop().unwrap()));

        for s in 1..=6u64 {
            let slot = Slot::new(s);
            let hash: BlockHash = Hash::random_for_test().into();
            votor.voted.insert(slot);
            votor.voted_notar.insert(slot, hash.clone());
            votor.bad_window.insert(slot);
            votor.block_notarized.insert(slot, hash.clone());
            votor.received_shred.insert(slot);
            votor.retired_slots.insert(slot);
            votor.fast_finalized.insert(slot);
            votor.parents_ready.insert((slot, slot.prev(), hash));
        }

        votor.prune_below_floor(Slot::new(4));
        assert_eq!(votor.finalized_floor, Slot::new(4));
        for set_len in [
            votor.voted.len(),
            votor.bad_window.len(),
            votor.received_shred.len(),
            votor.retired_slots.len(),
            votor.fast_finalized.len(),
        ] {
            assert_eq!(set_len, 3, "slots 4,5,6 survive; 1,2,3 pruned");
        }
        assert_eq!(votor.voted_notar.len(), 3);
        assert_eq!(votor.block_notarized.len(), 3);
        assert!(
            votor
                .parents_ready
                .iter()
                .all(|(s, _, _)| *s >= Slot::new(4))
        );

        votor.prune_below_floor(Slot::new(0));
        assert_eq!(votor.finalized_floor, Slot::new(4));
    }

    /// Floor guard prevents stale timers from voting below pruned state.
    #[tokio::test]
    async fn stale_timeout_below_floor_casts_no_vote() {
        let (sks, epoch_info) = generate_validators(2);
        let mut a2a = generate_all2all_instances(epoch_info.validators.clone()).await;
        let (tx, rx) = mpsc::channel(100);
        let other_a2a = a2a.pop().unwrap();
        let votor_a2a = a2a.pop().unwrap();
        let votor = Votor::new(0, sks[0].clone(), tx.clone(), rx, Arc::new(votor_a2a));
        tokio::spawn(async move {
            let mut votor = votor;
            votor.voting_loop().await.unwrap();
        });

        let slot = Slot::genesis().next();
        tx.send(VotorEvent::FirstShred(slot)).await.unwrap();
        let hash: BlockHash = Hash::random_for_test().into();
        let block_info = BlockInfo {
            hash: hash.clone(),
            parent: (Slot::genesis(), GENESIS_BLOCK_HASH),
        };
        tx.send(VotorEvent::Block { slot, block_info })
            .await
            .unwrap();
        let notar = match other_a2a.receive().await.unwrap() {
            ConsensusMessage::Vote(v) => v,
            m => panic!("expected notar vote, got {m:?}"),
        };
        assert!(notar.is_notar());

        // Finalization in the next window prunes slot 1 state.
        let slot5_vote = Vote::new_notar(Slot::new(5), Hash::random_for_test().into(), &sks[0], 0);
        let ff = Cert::FastFinal(FastFinalCert::new_unchecked(
            std::slice::from_ref(&slot5_vote),
            &epoch_info.validators,
        ));
        tx.send(VotorEvent::CertCreated(Box::new(ff)))
            .await
            .unwrap();
        match other_a2a.receive().await.unwrap() {
            ConsensusMessage::Cert(c) => assert!(matches!(c, Cert::FastFinal(_))),
            ConsensusMessage::Vote(v) => panic!("unexpected vote: {v:?}"),
        }

        // A stale timeout below the floor must be dropped.
        tx.send(VotorEvent::Timeout(slot)).await.unwrap();
        let got = tokio::time::timeout(Duration::from_secs(2), other_a2a.receive()).await;
        assert!(
            got.is_err(),
            "stale timeout below the floor must not cast a vote, but sent: {got:?}"
        );
    }

    #[tokio::test]
    async fn notar_out_of_order() {
        let (other_a2a, tx, _) = start_votor().await;
        let (slot1, hash1) = (Slot::genesis().next(), Hash::random_for_test());
        let (slot2, hash2) = (slot1.next(), Hash::random_for_test());

        let event = VotorEvent::FirstShred(slot2);
        tx.send(event).await.unwrap();
        let block_info = BlockInfo {
            hash: hash2.into(),
            parent: (slot1, hash1.clone().into()),
        };
        let event = VotorEvent::Block {
            slot: slot2,
            block_info,
        };
        tx.send(event).await.unwrap();

        assert!(
            tokio::time::timeout(Duration::from_secs(1), other_a2a.receive())
                .await
                .is_err()
        );

        let event = VotorEvent::FirstShred(slot1);
        tx.send(event).await.unwrap();
        let block_info = BlockInfo {
            hash: hash1.into(),
            parent: (Slot::genesis(), GENESIS_BLOCK_HASH),
        };
        let event = VotorEvent::Block {
            slot: slot1,
            block_info,
        };
        tx.send(event).await.unwrap();

        for _ in 0..2 {
            match other_a2a.receive().await.unwrap() {
                ConsensusMessage::Vote(vote) => {
                    assert!(vote.is_notar());
                    assert!(vote.slot() == slot1 || vote.slot() == slot2);
                }
                m => panic!("other msg: {m:?}"),
            };
        }
    }

    #[tokio::test]
    async fn safe_to_notar() {
        let (other_a2a, tx, _) = start_votor().await;
        let slot = Slot::genesis().next();

        for slot in slot.slots_in_window() {
            if slot.is_genesis() {
                continue;
            }
            if let Ok(msg) = other_a2a.receive().await {
                match msg {
                    ConsensusMessage::Vote(v) => assert!(v.is_skip()),
                    m => panic!("other msg: {m:?}"),
                }
            }
        }

        let hash = Hash::random_for_test();
        let event = VotorEvent::SafeToNotar(slot, hash.clone().into());
        tx.send(event).await.unwrap();
        match other_a2a.receive().await.unwrap() {
            ConsensusMessage::Vote(v) => {
                assert!(v.is_notar_fallback());
                assert_eq!(v.slot(), slot);
                assert_eq!(v.block_hash(), Some(&hash.into()));
            }
            m => panic!("other msg: {m:?}"),
        }
    }

    #[tokio::test]
    async fn safe_to_skip() {
        let (other_a2a, tx, _) = start_votor().await;
        let slot = Slot::genesis().next();

        let event = VotorEvent::FirstShred(slot);
        tx.send(event).await.unwrap();
        let block_info = BlockInfo {
            hash: Hash::random_for_test().into(),
            parent: (Slot::genesis(), GENESIS_BLOCK_HASH),
        };
        let event = VotorEvent::Block { slot, block_info };
        tx.send(event).await.unwrap();
        let vote = match other_a2a.receive().await.unwrap() {
            ConsensusMessage::Vote(v) => v,
            m => panic!("other msg: {m:?}"),
        };
        assert!(vote.is_notar());
        assert_eq!(vote.slot(), slot);

        let event = VotorEvent::SafeToSkip(slot);
        tx.send(event).await.unwrap();
        match other_a2a.receive().await.unwrap() {
            ConsensusMessage::Vote(v) => {
                assert!(v.is_skip_fallback());
                assert_eq!(v.slot(), slot);
            }
            m => panic!("other msg: {m:?}"),
        }
    }
}
