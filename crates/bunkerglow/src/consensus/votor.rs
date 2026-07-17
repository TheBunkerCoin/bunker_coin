// Copyright (c) Anza Technology, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Main voting logic for the consensus protocol.
//!
//! Besides [`super::Pool`], [`Votor`] is the other main internal component Alpenglow.
//! It handles the main voting decisions for the consensus protocol. As input it
//! receives events of type [`VotorEvent`] over a channel, depending on the event
//! type these were emitted by  [`super::Pool`], [`super::Blockstore`] and itself.
//! Votor keeps its own internal state for each slot based on previous events and votes.
//!
//! Votor has access to an instance of [`All2All`] for broadcasting votes.

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

/// Events that Votor is interested in.
///
/// These are emitted by [`super::Pool`], [`super::Blockstore`] and [`Votor`] itself.
/// They are the inputs that drive the voting loop of Votor.
#[derive(Clone, Debug)]
pub enum VotorEvent {
    /// The pool has newly marked the given block as a ready parent for `slot`.
    ///
    /// This event is only emitted per window, `slot` is always the first slot.
    /// The parent block is identified by `parent_slot` and `parent_hash`.
    ParentReady {
        slot: Slot,
        parent_slot: Slot,
        parent_hash: BlockHash,
    },
    /// The given block has reached the safe-to-notar status.
    SafeToNotar(Slot, BlockHash),
    /// The given slot has reached the safe-to-skip status.
    SafeToSkip(Slot),
    /// New certificated created in pool (should then be broadcast by Votor).
    CertCreated(Box<Cert>),
    /// Standstill timeout has fired.
    ///
    /// The provided slot indicates the highest finalized slot as seen by Pool.
    /// The provided certificates and votes should be re-broadcast.
    Standstill(Slot, Vec<Cert>, Vec<Vote>),

    /// First valid shred of the leader's block was received for the block.
    FirstShred(Slot),
    /// New (complete) block was received in blockstore.
    Block { slot: Slot, block_info: BlockInfo },

    /// Regular timeout for the given slot has fired.
    Timeout(Slot),
    /// Early timeout for a crashed leader (nothing was received) has fired.
    TimeoutCrashedLeader(Slot),
    /// The grace period for deferring a finalization vote has elapsed (see
    /// [`Votor::defer_final_vote`]). If the slot has not fast-finalized by now,
    /// the (slow-path) finalization vote should be sent for the given block.
    FinalVoteDeadline(Slot, BlockHash),
}

/// Votor implements the decision process of which votes to cast.
///
/// It keeps some state for each slot and checks the conditions for voting.
/// On [`Votor::event_receiver`], it receives events from [`super::Pool`],
/// [`super::Blockstore`] and itself.
/// Informed by these events Votor updates its state and generates votes.
/// Votes are signed with [`Votor::voting_key`] and broadcast using [`Votor::all2all`].
pub struct Votor<A: All2All> {
    // TODO: merge all of these into `SlotState` struct?
    /// Indicates for which slots we already voted notar or skip.
    voted: BTreeSet<Slot>,
    /// Indicates for which slots we already voted notar and for what hash.
    voted_notar: BTreeMap<Slot, BlockHash>,
    /// Indicates for which slots we set the 'bad window' flag.
    bad_window: BTreeSet<Slot>,
    /// How many times each window's crashed-leader timeout has been re-armed
    /// (paused) because the link was alive but the leader's block had not yet
    /// crawled across. Bounds the pause so a genuinely gone peer is still skipped.
    crashed_leader_rearms: BTreeMap<Slot, u32>,
    /// Blocks that have a notarization certificate (not notar-fallback).
    block_notarized: BTreeMap<Slot, BlockHash>,
    /// Indicates for which slots the given (slot, hash) pair is a valid parent.
    parents_ready: BTreeSet<(Slot, Slot, BlockHash)>,
    /// Indicates for which slots we received at least one shred.
    received_shred: BTreeSet<Slot>,
    /// Blocks that are waiting for previous slots to be notarized.
    pending_blocks: BTreeMap<Slot, BlockInfo>,
    /// Slots that Votor is done with.
    retired_slots: BTreeSet<Slot>,

    /// Own validator ID.
    validator_id: ValidatorId,
    /// Secret key used to sign votes.
    voting_key: SecretKey,
    /// Channel for receiving events from pool, blockstore and Votor itself.
    event_receiver: Receiver<VotorEvent>,
    /// Sender side of event channel. Used for sending events to self.
    event_sender: Sender<VotorEvent>,
    /// [`All2All`] instance used to broadcast votes.
    all2all: Arc<A>,
    /// Reports whether the link to peers is up, so a crashed-leader timeout on a
    /// merely-slow (but alive) link pauses instead of skipping. Defaults to
    /// [`NoLiveness`] (sim/UDP); the radio path injects a keepalive-driven impl.
    link_liveness: Arc<dyn LinkLiveness>,
    /// When `true`, the (slow-path) finalization vote is **deferred** by a grace
    /// period instead of being broadcast eagerly alongside the notar vote. If the
    /// slot fast-finalizes within the grace window (the common case when all
    /// validators are reachable — e.g. a 2-node link where both notar votes meet
    /// the 80% strong quorum), the finalization vote, notar cert, and final cert
    /// are never sent — saving three messages per slot over the expensive reverse
    /// path. If fast-final does *not* happen in time, the vote is sent when the
    /// grace elapses, falling back to slow-final exactly as before. Defaults to
    /// `false` (eager, original behavior); the radio half-duplex path enables it.
    defer_final_vote: bool,
    /// Slots for which a fast-final cert has been observed (so a deferred
    /// finalization vote can be suppressed — fast-final already finalized them).
    fast_finalized: BTreeSet<Slot>,
    /// Durable log of own votes, written before each broadcast so a restarted
    /// node can never cast a vote conflicting with one sent before the crash.
    /// Disabled by default; wired up with [`Self::set_vote_history`].
    vote_history: VoteHistory,
    /// Votes restored from [`Self::vote_history`], rebroadcast once when the
    /// voting loop starts: if the pre-crash vote was lost in flight (a link
    /// drop and a crash often coincide), the double-vote guard would otherwise
    /// leave the peer waiting for a vote that never comes again until
    /// standstill recovery. Identical votes are deduplicated by peers.
    restored_votes: Vec<Vote>,
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
        // add dummy genesis block to some of the data structures
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
            // Off by default (eager finalization vote — original behavior for
            // sim/UDP/full-duplex and all existing tests). The radio half-duplex
            // path opts in via `BUNKER_DEFER_FINAL_VOTE=1` (set in `pactor_init`),
            // matching the `BUNKER_DELTA_MULT` env-config pattern, so `Alpenglow::new`
            // keeps a stable signature.
            defer_final_vote: std::env::var("BUNKER_DEFER_FINAL_VOTE")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false),
            fast_finalized: BTreeSet::new(),
            vote_history: VoteHistory::disabled(),
            restored_votes: Vec::new(),
        };
        votor.set_timeouts(Slot::new(0));
        votor
    }

    /// Inject a link-liveness signal (defaults to [`NoLiveness`]).
    ///
    /// Radio transports pass a keepalive-driven impl so a crashed-leader timeout
    /// on a slow-but-alive link pauses instead of skipping the window.
    pub fn set_link_liveness(&mut self, liveness: Arc<dyn LinkLiveness>) {
        self.link_liveness = liveness;
    }

    /// Inject the durable own-vote log and rebuild voting state from it.
    ///
    /// Replays every persisted vote for slots at or after `finalized_slot`
    /// into `voted` / `voted_notar` / `bad_window` / `retired_slots`, so the
    /// voting rules hold across a crash-restart exactly as they would have in
    /// a single uninterrupted run: a slot notar-voted before the crash cannot
    /// be skip-voted after it (and vice versa), and a slot with a fallback
    /// vote cannot receive a finalization vote. Records below the finalized
    /// frontier are pruned — peers reject votes for finalized slots, so no
    /// conflict is possible there.
    pub fn set_vote_history(&mut self, history: VoteHistory, finalized_slot: Slot) {
        self.vote_history = history;
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
    }

    /// Enable deferring the slow-path finalization vote (see [`Self::defer_final_vote`]).
    ///
    /// Saves the finalization vote + notar cert + final cert per slot over the
    /// reverse path whenever a slot fast-finalizes within the grace window. Safe:
    /// it only ever delays (never fabricates) a finalization vote, and falls back
    /// to slow-final if fast-final does not occur in time.
    pub fn set_defer_final_vote(&mut self, defer: bool) {
        self.defer_final_vote = defer;
    }

    /// Max times a window's crashed-leader timeout may be re-armed (paused) while
    /// the link is alive before we skip anyway. Bounds the pause so a genuinely
    /// gone peer (whose transport still reports "up") is eventually skipped,
    /// preserving liveness. With `delta_timeout` per re-arm this is a generous
    /// multi-window wait on a slow link.
    const MAX_CRASHED_LEADER_REARMS: u32 = 5;

    /// Handles the voting (leader and non-leader) side of consensus protocol.
    ///
    /// Checks consensus conditions and broadcasts new votes.
    #[fastrace::trace]
    pub async fn voting_loop(&mut self) -> Result<()> {
        // Rebroadcast votes restored after a crash-restart (see `restored_votes`).
        for vote in std::mem::take(&mut self.restored_votes) {
            debug!("rebroadcasting restored vote for slot {}", vote.slot());
            self.all2all.broadcast(&vote.into()).await.unwrap();
        }
        while let Some(event) = self.event_receiver.recv().await {
            //println!("[Votor {}] event: {:?}", self.validator_id, event);
            if self.retired_slots.contains(&event.slot()) {
                trace!("ignoring event for retired slot {}", event.slot());
                continue;
            }
            trace!("votor event: {event:?}");
            match event {
                // events from Pool
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
                    //println!("[Votor {}] SAFE_TO_NOTAR slot {}", self.validator_id, slot);
                    debug!("voted notar-fallback in slot {slot}");
                    let vote =
                        Vote::new_notar_fallback(slot, hash, &self.voting_key, self.validator_id);
                    self.vote_history.record(&vote);
                    self.all2all.broadcast(&vote.into()).await.unwrap();
                    self.try_skip_window(slot).await;
                    self.bad_window.insert(slot);
                }
                VotorEvent::SafeToSkip(slot) => {
                    //println!("[Votor {}] SAFE_TO_SKIP slot {}", self.validator_id, slot);
                    debug!("voted skip-fallback in slot {slot}");
                    let vote = Vote::new_skip_fallback(slot, &self.voting_key, self.validator_id);
                    self.vote_history.record(&vote);
                    self.all2all.broadcast(&vote.into()).await.unwrap();
                    self.try_skip_window(slot).await;
                    self.bad_window.insert(slot);
                }
                VotorEvent::CertCreated(cert) => {
                    //println!("[Votor {}] CERT_CREATED {:?}", self.validator_id, cert);
                    match cert.as_ref() {
                        Cert::Notar(_) => {
                            self.block_notarized
                                .insert(cert.slot(), cert.block_hash().cloned().unwrap());
                            // When deferring, do NOT send the finalization vote on the
                            // notar cert; the FinalVoteDeadline path sends it later only
                            // if fast-final has not finalized the slot by then.
                            if !self.defer_final_vote {
                                self.try_final(cert.slot(), cert.block_hash().cloned().unwrap())
                                    .await;
                            }
                        }
                        Cert::FastFinal(_) => {
                            // Record so a deferred (or pending) finalization vote is
                            // suppressed — fast-final already finalized this slot.
                            self.fast_finalized.insert(cert.slot());
                            let first_slot_in_window = cert.slot().first_slot_in_window();
                            self.vote_history.prune(first_slot_in_window);
                            self.set_timeouts(first_slot_in_window);
                        }
                        Cert::Final(_) => {
                            let first_slot_in_window = cert.slot().first_slot_in_window();
                            self.vote_history.prune(first_slot_in_window);
                            self.set_timeouts(first_slot_in_window);
                        }
                        _ => {}
                    }
                    self.all2all.broadcast(&(*cert).into()).await.unwrap();
                }
                VotorEvent::Standstill(_, certs, votes) => {
                    //println!("[Votor {}] STANDSTILL event", self.validator_id);
                    for cert in certs {
                        self.all2all.broadcast(&cert.into()).await.unwrap();
                    }
                    for vote in votes {
                        self.all2all.broadcast(&vote.into()).await.unwrap();
                    }
                }

                // events from Blockstore
                VotorEvent::FirstShred(slot) => {
                    //println!("[Votor {}] FIRST_SHRED slot {}", self.validator_id, slot);
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

                // events from Votor itself
                VotorEvent::Timeout(slot) => {
                    //println!("[Votor {}] TIMEOUT slot {}", self.validator_id, slot);
                    trace!("timeout for slot {slot}");
                    if !self.voted.contains(&slot) {
                        self.try_skip_window(slot).await;
                    }
                }
                VotorEvent::TimeoutCrashedLeader(slot) => {
                    trace!("timeout (crashed leader) for slot {slot}");
                    if !self.received_shred.contains(&slot) && !self.voted.contains(&slot) {
                        // The leader's first shred has not arrived. Normally this
                        // means the leader crashed → skip the window. But over a
                        // half-duplex link that has merely gone quiet (the slow
                        // reverse ARQ path), the leader is alive and its block is
                        // still crawling across; skipping would gap the chain
                        // irreversibly. If the link is alive and we have not
                        // exhausted the re-arm budget, PAUSE: re-arm the timeout
                        // and wait, rather than skip.
                        let rearms = self.crashed_leader_rearms.entry(slot).or_insert(0);
                        if self.link_liveness.is_link_alive()
                            && *rearms < Self::MAX_CRASHED_LEADER_REARMS
                        {
                            *rearms += 1;
                            println!(
                                "[Votor {}] EARLY_TIMEOUT slot {} — link alive, pausing (re-arm {}/{})",
                                self.validator_id, slot, *rearms, Self::MAX_CRASHED_LEADER_REARMS
                            );
                            let sender = self.event_sender.clone();
                            tokio::spawn(async move {
                                tokio::time::sleep(delta_timeout()).await;
                                let _ = sender.send(VotorEvent::TimeoutCrashedLeader(slot)).await;
                            });
                        } else {
                            // Link is down (peer gone) or we have paused long
                            // enough — treat as a genuine crashed leader and skip.
                            println!(
                                "[Votor {}] EARLY_TIMEOUT slot {} — skipping window",
                                self.validator_id, slot
                            );
                            self.try_skip_window(slot).await;
                        }
                    }
                }
                VotorEvent::FinalVoteDeadline(slot, hash) => {
                    // The deferral grace has elapsed. If fast-final already
                    // finalized the slot, `try_final` is a no-op (the savings).
                    // Otherwise, send the slow-path finalization vote now, exactly
                    // as the eager path would have — preserving slow-final liveness.
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
            // HACK: ignoring errors to prevent panic when shutting down votor
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
            // Defer the slow-path finalization vote: give fast-final a chance to
            // fire from the notar votes alone (saving the final vote + notar cert
            // + final cert over the slow reverse path). If it has not fired by the
            // deadline, `FinalVoteDeadline` triggers the vote as a fallback.
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
        // If the slot already fast-finalized, the slow-path finalization vote is
        // redundant — skip it (this is the whole point of deferring it).
        if self.fast_finalized.contains(&slot) {
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

    /// Fixed-value [`LinkLiveness`] for tests.
    struct TestLiveness(bool);
    impl LinkLiveness for TestLiveness {
        fn is_link_alive(&self) -> bool {
            self.0
        }
    }

    /// Start a votor; if `liveness` is given, inject it (else default NoLiveness).
    async fn start_votor_with_liveness(
        liveness: Option<Arc<dyn LinkLiveness>>,
    ) -> (A2A, mpsc::Sender<VotorEvent>, Arc<EpochInfo>) {
        start_votor_full(liveness, false).await
    }

    /// Start a votor with explicit liveness and `defer_final_vote` settings.
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

    /// A crashed-leader timeout with the link reported DOWN must skip the window
    /// (the leader is presumed gone — original behavior, liveness = false).
    #[tokio::test]
    async fn crashed_leader_skips_when_link_down() {
        let (other_a2a, tx, _) =
            start_votor_with_liveness(Some(Arc::new(TestLiveness(false)))).await;

        // Start of a non-genesis window (slot 4; SLOTS_PER_WINDOW=4) we have not
        // voted on / seen a shred for; the genesis window is pre-seeded as voted.
        let slot = Slot::new(4);
        assert!(slot.is_start_of_window());
        tx.send(VotorEvent::TimeoutCrashedLeader(slot)).await.unwrap();

        // Should skip the whole window.
        match tokio::time::timeout(Duration::from_secs(2), other_a2a.receive()).await {
            Ok(Ok(ConsensusMessage::Vote(v))) => {
                assert!(v.is_skip(), "expected skip vote, got {v:?}");
            }
            other => panic!("expected a skip vote, got {other:?}"),
        }
    }

    /// A crashed-leader timeout with the link reported ALIVE must NOT skip — it
    /// pauses (re-arms the timeout), waiting for the slow reverse path to deliver
    /// the leader's block instead of irreversibly gapping the chain.
    #[tokio::test]
    async fn crashed_leader_pauses_when_link_alive() {
        let (other_a2a, tx, _) =
            start_votor_with_liveness(Some(Arc::new(TestLiveness(true)))).await;

        let slot = Slot::genesis().next().first_slot_in_window();
        tx.send(VotorEvent::TimeoutCrashedLeader(slot)).await.unwrap();

        // Must NOT see any skip vote promptly: the re-arm waits delta_timeout (long),
        // so a short window proves it paused rather than skipped.
        let got = tokio::time::timeout(Duration::from_secs(2), other_a2a.receive()).await;
        assert!(
            got.is_err(),
            "link alive: must pause, not skip — but saw a vote: {got:?}"
        );
    }

    #[tokio::test]
    async fn timeouts() {
        let (other_a2a, _, _) = start_votor().await;

        // should vote skip for all slots
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

        // vote notar after seeing block
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

        // vote finalize after seeing branch-certified
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

    /// With `defer_final_vote` enabled, after a node notar-votes it must NOT emit
    /// a finalization vote on the notar cert. If a fast-final cert then arrives,
    /// the finalization vote must be suppressed entirely (the reverse-path saving):
    /// the only consensus messages over the link are the notar vote and the
    /// re-broadcast certs — never a `Final` vote.
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

        // First message must be the notar vote.
        let notar = match other_a2a.receive().await.unwrap() {
            ConsensusMessage::Vote(v) => v,
            m => panic!("expected notar vote, got {m:?}"),
        };
        assert!(notar.is_notar());

        // Deliver a Notar cert: in deferred mode this must NOT trigger a final vote.
        let notar_cert = Cert::Notar(NotarCert::new_unchecked(
            std::slice::from_ref(&notar),
            &epoch_info.validators,
        ));
        tx.send(VotorEvent::CertCreated(Box::new(notar_cert)))
            .await
            .unwrap();
        // The notar cert is re-broadcast, but no final vote rides along.
        match other_a2a.receive().await.unwrap() {
            ConsensusMessage::Cert(c) => assert!(matches!(c, Cert::Notar(_))),
            ConsensusMessage::Vote(v) => panic!("unexpected vote in deferred mode: {v:?}"),
        }

        // Now fast-final fires. After this, the deferred finalization-vote deadline
        // must find the slot already fast-finalized and send nothing.
        let ff = Cert::FastFinal(FastFinalCert::new_unchecked(
            std::slice::from_ref(&notar),
            &epoch_info.validators,
        ));
        tx.send(VotorEvent::CertCreated(Box::new(ff)))
            .await
            .unwrap();
        // The fast-final cert is re-broadcast (a cert, not a vote)...
        match other_a2a.receive().await.unwrap() {
            ConsensusMessage::Cert(c) => assert!(matches!(c, Cert::FastFinal(_))),
            ConsensusMessage::Vote(v) => panic!("unexpected vote: {v:?}"),
        }

        // Fire the deferral deadline explicitly (rather than waiting the full grace):
        // because the slot already fast-finalized, it must emit NOTHING. This is the
        // reverse-path saving — the final vote is suppressed.
        tx.send(VotorEvent::FinalVoteDeadline(slot, hash))
            .await
            .unwrap();
        let leftover = tokio::time::timeout(Duration::from_secs(2), other_a2a.receive()).await;
        assert!(
            leftover.is_err(),
            "fast-final must suppress the final vote, but saw: {leftover:?}"
        );
    }

    /// With `defer_final_vote` enabled but fast-final NOT occurring, the deferral
    /// deadline must still send the finalization vote (slow-final fallback) — so a
    /// flaky peer that prevents the 80% strong quorum cannot stall finalization.
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

        // A notar cert forms (60%) but never a fast-final (no 80% strong quorum).
        let notar_cert = Cert::Notar(NotarCert::new_unchecked(
            std::slice::from_ref(&notar),
            &epoch_info.validators,
        ));
        tx.send(VotorEvent::CertCreated(Box::new(notar_cert)))
            .await
            .unwrap();
        // Re-broadcast of the notar cert, no final vote yet (deferred).
        match other_a2a.receive().await.unwrap() {
            ConsensusMessage::Cert(c) => assert!(matches!(c, Cert::Notar(_))),
            ConsensusMessage::Vote(v) => panic!("premature final vote: {v:?}"),
        }

        // Deadline fires without a fast-final: must now send the final vote.
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

    /// Crash-restart voting safety: a Votor that notar-voted a slot, then
    /// "crashed" (state dropped) and was rebuilt with the same [`VoteHistory`],
    /// must NOT cast a skip vote for that slot when the restart path fires its
    /// timeout — that would be a slashable conflicting vote. Without the
    /// restored history the same timeout provably produces skip votes (see the
    /// `timeouts` test).
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

        // --- First life: notar-vote `slot`, recording into the history.
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
            tx.send(VotorEvent::Block { slot, block_info }).await.unwrap();
            let vote = match other_a2a.receive().await.unwrap() {
                ConsensusMessage::Vote(v) => v,
                m => panic!("expected notar vote, got {m:?}"),
            };
            assert!(vote.is_notar());
            assert_eq!(vote.slot(), slot);
        } // votor task's channel sender drops → first life ends ("crash")

        // --- Second life: fresh Votor, same history, empty in-memory state.
        let mut a2a = generate_all2all_instances(_epoch_info.validators.clone()).await;
        let (tx, rx) = mpsc::channel(100);
        let other_a2a = a2a.pop().unwrap();
        let votor_a2a = a2a.pop().unwrap();
        let mut votor = Votor::new(0, sks[0].clone(), tx.clone(), rx, Arc::new(votor_a2a));
        votor.set_vote_history(VoteHistory::open_at(&db_path), Slot::genesis());
        tokio::spawn(async move {
            votor.voting_loop().await.unwrap();
        });

        // Liveness: the restored notar vote is rebroadcast on startup (byte-
        // identical to the pre-crash vote, in case it was lost in flight).
        match other_a2a.receive().await.unwrap() {
            ConsensusMessage::Vote(v) => {
                assert!(v.is_notar(), "expected rebroadcast notar vote, got {v:?}");
                assert_eq!(v.slot(), slot);
                assert_eq!(v.block_hash(), Some(&hash));
            }
            m => panic!("expected rebroadcast notar vote, got {m:?}"),
        }

        // Safety: the restart path fires a timeout for the unfinished slot.
        // With the restored history the slot counts as voted → no skip vote.
        tx.send(VotorEvent::Timeout(slot)).await.unwrap();
        let got = tokio::time::timeout(Duration::from_secs(2), other_a2a.receive()).await;
        assert!(
            got.is_err(),
            "restored votor must not skip a slot it already notar-voted, but sent: {got:?}"
        );
    }

    #[tokio::test]
    async fn notar_out_of_order() {
        let (other_a2a, tx, _) = start_votor().await;
        let (slot1, hash1) = (Slot::genesis().next(), Hash::random_for_test());
        let (slot2, hash2) = (slot1.next(), Hash::random_for_test());

        // give later block to votor first
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

        // should not vote yet
        assert!(
            tokio::time::timeout(Duration::from_secs(1), other_a2a.receive())
                .await
                .is_err()
        );

        // now notify votor of earlier block
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

        // should now see notar votes
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

        // wait for skip votes
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

        // vote notar-fallback after safe-to-notar
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

        // vote notar after seeing block
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

        // vote skip-fallback after safe-to-skip
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
