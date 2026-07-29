// Copyright (c) Anza Technology, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Per-slot pool state for votes, stake totals, and certificates.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use either::Either;
use smallvec::SmallVec;

use super::SlashableOffence;
use crate::consensus::cert::{FastFinalCert, FinalCert, NotarCert, NotarFallbackCert, SkipCert};
use crate::consensus::vote::VoteKind;
use crate::consensus::votor::VotorEvent;
use crate::consensus::{Cert, EpochInfo, Vote};
use crate::crypto::merkle::BlockHash;
use crate::{BlockId, Slot, Stake};

/// Pool state for one slot.
pub struct SlotState {
    pub(super) votes: SlotVotes,
    pub(super) voted_stakes: SlotVotedStake,
    pub(super) certificates: SlotCertificates,
    parents: BTreeMap<BlockHash, ParentStatus>,
    pending_safe_to_notar: BTreeSet<BlockHash>,
    sent_safe_to_notar: BTreeSet<BlockHash>,
    sent_safe_to_skip: bool,

    slot: Slot,
    pub(super) epoch_info: Arc<EpochInfo>,
}

pub struct SlotVotes {
    pub(super) notar: Vec<Option<Vote>>,
    pub(super) notar_fallback: Vec<BTreeMap<BlockHash, Vote>>,
    pub(super) skip: Vec<Option<Vote>>,
    pub(super) skip_fallback: Vec<Option<Vote>>,
    pub(super) finalize: Vec<Option<Vote>>,
}

#[derive(Default)]
pub struct SlotVotedStake {
    pub(super) notar: BTreeMap<BlockHash, Stake>,
    pub(super) notar_fallback: BTreeMap<BlockHash, Stake>,
    pub(super) skip: Stake,
    pub(super) skip_fallback: Stake,
    pub(super) finalize: Stake,
    pub(super) notar_or_skip: Stake,
    pub(super) top_notar: Stake,
}

#[derive(Default)]
pub struct SlotCertificates {
    pub(super) notar: Option<NotarCert>,
    pub(super) notar_fallback: Vec<NotarFallbackCert>,
    pub(super) skip: Option<SkipCert>,
    pub(super) fast_finalize: Option<FastFinalCert>,
    pub(super) finalize: Option<FinalCert>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ParentStatus {
    Known,
    Certified,
}

/// Possible states for the safe-to-notar check.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SafeToNotarStatus {
    SafeToNotar,
    MissingBlock,
    AwaitingVotes,
}

type SlotStateOutputs = (
    SmallVec<[Cert; 2]>,
    SmallVec<[VotorEvent; 2]>,
    SmallVec<[(Slot, BlockHash); 1]>,
);

impl SlotState {
    /// Creates empty state for one slot.
    pub fn new(slot: Slot, epoch_info: Arc<EpochInfo>) -> Self {
        Self {
            votes: SlotVotes::new(epoch_info.validators.len()),
            voted_stakes: SlotVotedStake::default(),
            certificates: SlotCertificates::default(),
            parents: BTreeMap::new(),
            pending_safe_to_notar: BTreeSet::new(),
            sent_safe_to_notar: BTreeSet::new(),
            sent_safe_to_skip: false,

            slot,
            epoch_info,
        }
    }

    /// Emit locally-built certs only when collected votes meet the threshold.
    fn push_cert_checked(&self, new_certs: &mut SmallVec<[Cert; 2]>, cert: Cert) {
        if cert.check_threshold(&self.epoch_info) {
            new_certs.push(cert);
        } else {
            log::error!(
                "BUG: locally-created {} cert for slot {} fails its stake threshold — \
                 stake tally and collected votes diverged; cert suppressed",
                cert.kind_str(),
                cert.slot()
            );
        }
    }

    /// Adds a certificate to this slot.
    pub fn add_cert(&mut self, cert: Cert) {
        match cert {
            Cert::Notar(n) => self.certificates.notar = Some(n),
            Cert::NotarFallback(n) => {
                if !self.is_notar_fallback(n.block_hash()) {
                    self.certificates.notar_fallback.push(n);
                }
            }
            Cert::Skip(s) => self.certificates.skip = Some(s),
            Cert::FastFinal(s) => self.certificates.fast_finalize = Some(s),
            Cert::Final(f) => self.certificates.finalize = Some(f),
        }
    }

    /// Adds a vote and returns any resulting certs, Votor events, or repairs.
    pub fn add_vote(&mut self, vote: Vote, voter_stake: Stake) -> SlotStateOutputs {
        let slot = vote.slot();
        let voter = vote.signer();
        let v = voter as usize;

        let (certs_created, mut votor_events, mut blocks_to_repair) = match vote.kind() {
            // Store before counting: cert creation reads stored votes at quorum.
            VoteKind::Notar(_, block_hash) => {
                let block_hash = block_hash.clone();
                self.votes.notar[v] = Some(vote);
                self.count_notar_stake(slot, &block_hash, voter_stake)
            }
            VoteKind::NotarFallback(_, block_hash) => {
                let block_hash = block_hash.clone();
                let res = self.votes.notar_fallback[v].insert(block_hash.clone(), vote);
                assert!(res.is_none());
                self.count_notar_fallback_stake(&block_hash, voter_stake)
            }
            VoteKind::Skip(_) => {
                self.votes.skip[v] = Some(vote);
                self.voted_stakes.notar_or_skip += voter_stake;
                self.count_skip_stake(slot, voter_stake, false)
            }
            VoteKind::SkipFallback(_) => {
                self.votes.skip_fallback[v] = Some(vote);
                self.count_skip_stake(slot, voter_stake, true)
            }
            VoteKind::Final(_) => {
                self.votes.finalize[v] = Some(vote);
                self.count_finalize_stake(voter_stake)
            }
        };

        if voter == self.epoch_info.own_id {
            for hash in self.pending_safe_to_notar.clone() {
                if self.sent_safe_to_notar.contains(&hash) {
                    continue;
                }
                match self.check_safe_to_notar(hash.clone()) {
                    SafeToNotarStatus::SafeToNotar => {
                        votor_events.push(VotorEvent::SafeToNotar(slot, hash));
                    }
                    SafeToNotarStatus::MissingBlock => blocks_to_repair.push((slot, hash)),
                    SafeToNotarStatus::AwaitingVotes => {}
                }
            }
        }

        (certs_created, votor_events, blocks_to_repair)
    }

    /// Mark the parent of `hash` as known.
    pub fn notify_parent_known(&mut self, hash: BlockHash) {
        self.parents.entry(hash).or_insert(ParentStatus::Known);
    }

    /// Mark the parent of `hash` as notarized-fallback.
    ///
    /// # Panics
    ///
    /// If [`SlotState::notify_parent_known`] has not yet been called for this block.
    pub fn notify_parent_certified(
        &mut self,
        hash: BlockHash,
    ) -> Option<Either<VotorEvent, BlockId>> {
        let Some(parent_info) = self.parents.get_mut(&hash) else {
            panic!("parent not known")
        };
        *parent_info = ParentStatus::Certified;

        if self.sent_safe_to_notar.contains(&hash) {
            return None;
        }
        match self.check_safe_to_notar(hash.clone()) {
            SafeToNotarStatus::SafeToNotar => {
                Some(Either::Left(VotorEvent::SafeToNotar(self.slot, hash)))
            }
            SafeToNotarStatus::MissingBlock => Some(Either::Right((self.slot, hash))),
            SafeToNotarStatus::AwaitingVotes => None,
        }
    }

    fn is_weakest_quorum(&self, stake: Stake) -> bool {
        stake >= (self.epoch_info.total_stake()).div_ceil(5)
    }

    fn is_weak_quorum(&self, stake: Stake) -> bool {
        stake >= (self.epoch_info.total_stake() * 2).div_ceil(5)
    }

    fn is_quorum(&self, stake: Stake) -> bool {
        stake >= (self.epoch_info.total_stake() * 3).div_ceil(5)
    }

    fn is_strong_quorum(&self, stake: Stake) -> bool {
        stake >= (self.epoch_info.total_stake() * 4).div_ceil(5)
    }

    fn count_notar_stake(
        &mut self,
        slot: Slot,
        block_hash: &BlockHash,
        stake: Stake,
    ) -> SlotStateOutputs {
        let mut new_certs = SmallVec::new();
        let mut votor_events = SmallVec::new();
        let mut blocks_to_repair = SmallVec::new();

        let notar_stake = self
            .voted_stakes
            .notar
            .entry(block_hash.clone())
            .or_insert(0);
        *notar_stake += stake;
        self.voted_stakes.notar_or_skip += stake;
        let notar_stake = *notar_stake;
        self.voted_stakes.top_notar = notar_stake.max(self.voted_stakes.top_notar);

        if !self.sent_safe_to_notar.contains(block_hash) {
            match self.check_safe_to_notar(block_hash.clone()) {
                SafeToNotarStatus::SafeToNotar => {
                    votor_events.push(VotorEvent::SafeToNotar(slot, block_hash.clone()));
                }
                SafeToNotarStatus::MissingBlock => {
                    blocks_to_repair.push((slot, block_hash.clone()));
                }
                SafeToNotarStatus::AwaitingVotes => {}
            }
        }
        if !self.sent_safe_to_skip
            && self.is_weak_quorum(self.voted_stakes.notar_or_skip - self.voted_stakes.top_notar)
            && self.votes.notar[self.epoch_info.own_id as usize].is_some()
        {
            votor_events.push(VotorEvent::SafeToSkip(slot));
            self.sent_safe_to_skip = true;
        }
        let nf_stake = *self
            .voted_stakes
            .notar_fallback
            .get(block_hash)
            .unwrap_or(&0);
        if self.is_quorum(nf_stake + notar_stake) && !self.is_notar_fallback(block_hash) {
            let mut votes = self.votes.notar_votes(block_hash);
            votes.extend(self.votes.notar_fallback_votes(block_hash));
            let cert = NotarFallbackCert::new_unchecked(&votes, &self.epoch_info.validators);
            self.push_cert_checked(&mut new_certs, Cert::NotarFallback(cert));
        }
        if self.is_quorum(notar_stake) && self.certificates.notar.is_none() {
            let votes = self.votes.notar_votes(block_hash);
            let cert = NotarCert::new_unchecked(&votes, &self.epoch_info.validators);
            self.push_cert_checked(&mut new_certs, Cert::Notar(cert));
        }
        if self.is_strong_quorum(notar_stake) && self.certificates.fast_finalize.is_none() {
            let votes = self.votes.notar_votes(block_hash);
            let cert = FastFinalCert::new_unchecked(&votes, &self.epoch_info.validators);
            self.push_cert_checked(&mut new_certs, Cert::FastFinal(cert));
        }

        (new_certs, votor_events, blocks_to_repair)
    }

    fn count_notar_fallback_stake(
        &mut self,
        block_hash: &BlockHash,
        stake: Stake,
    ) -> SlotStateOutputs {
        let mut new_certs = SmallVec::new();
        let nf_stakes = &mut self.voted_stakes.notar_fallback;
        let nf_stake = nf_stakes.entry(block_hash.clone()).or_insert(0);
        *nf_stake += stake;
        let nf_stake = *nf_stake;
        let notar_stake = *self.voted_stakes.notar.get(block_hash).unwrap_or(&0);
        if self.is_quorum(nf_stake + notar_stake) && !self.is_notar_fallback(block_hash) {
            let mut votes = self.votes.notar_votes(block_hash);
            votes.extend(self.votes.notar_fallback_votes(block_hash));
            let cert = NotarFallbackCert::new_unchecked(&votes, &self.epoch_info.validators);
            self.push_cert_checked(&mut new_certs, Cert::NotarFallback(cert));
        }
        (new_certs, SmallVec::new(), SmallVec::new())
    }

    fn count_skip_stake(&mut self, slot: Slot, stake: Stake, fallback: bool) -> SlotStateOutputs {
        let mut new_certs = SmallVec::new();
        let mut votor_events = SmallVec::new();
        let mut blocks_to_repair = SmallVec::new();
        if fallback {
            self.voted_stakes.skip_fallback += stake;
        } else {
            self.voted_stakes.skip += stake;
        }
        for hash in self.pending_safe_to_notar.clone() {
            if self.sent_safe_to_notar.contains(&hash) {
                continue;
            }
            match self.check_safe_to_notar(hash.clone()) {
                SafeToNotarStatus::SafeToNotar => {
                    votor_events.push(VotorEvent::SafeToNotar(slot, hash));
                }
                SafeToNotarStatus::MissingBlock => blocks_to_repair.push((slot, hash)),
                SafeToNotarStatus::AwaitingVotes => {}
            }
        }
        let total_skip_stake = self.voted_stakes.skip + self.voted_stakes.skip_fallback;
        if self.is_quorum(total_skip_stake) && self.certificates.skip.is_none() {
            let mut votes = self.votes.skip_votes();
            votes.extend(self.votes.skip_fallback_votes());
            let cert = SkipCert::new_unchecked(&votes, &self.epoch_info.validators);
            self.push_cert_checked(&mut new_certs, Cert::Skip(cert));
        }
        if !self.sent_safe_to_skip
            && self.is_weak_quorum(self.voted_stakes.notar_or_skip - self.voted_stakes.top_notar)
            && self.votes.notar[self.epoch_info.own_id as usize].is_some()
        {
            votor_events.push(VotorEvent::SafeToSkip(slot));
            self.sent_safe_to_skip = true;
        }
        (new_certs, votor_events, blocks_to_repair)
    }

    fn count_finalize_stake(&mut self, stake: Stake) -> SlotStateOutputs {
        let mut new_certs = SmallVec::new();
        self.voted_stakes.finalize += stake;
        if self.is_quorum(self.voted_stakes.finalize) && self.certificates.finalize.is_none() {
            let votes: Vec<_> = self.votes.final_votes();
            let cert = FinalCert::new_unchecked(&votes, &self.epoch_info.validators);
            self.push_cert_checked(&mut new_certs, Cert::Final(cert));
        }
        (new_certs, SmallVec::new(), SmallVec::new())
    }

    /// Checks slashable offences before duplicate filtering.
    pub fn check_slashable_offence(&self, vote: &Vote) -> Option<SlashableOffence> {
        let slot = vote.slot();
        let voter = vote.signer();
        let v = voter as usize;
        match vote.kind() {
            VoteKind::Notar(_, block_hash) => {
                if self.votes.skip[v].is_some() {
                    return Some(SlashableOffence::SkipAndNotarize(voter, slot));
                }
                if let Some(notar_vote) = &self.votes.notar[v]
                    && block_hash != notar_vote.block_hash().unwrap()
                {
                    return Some(SlashableOffence::NotarDifferentHash(voter, slot));
                }
            }
            VoteKind::NotarFallback(_, _) => {
                if self.votes.finalize[v].is_some() {
                    return Some(SlashableOffence::NotarFallbackAndFinalize(voter, slot));
                }
            }
            VoteKind::Skip(_) => {
                if self.votes.finalize[v].is_some() {
                    return Some(SlashableOffence::SkipAndFinalize(voter, slot));
                } else if self.votes.notar[v].is_some() {
                    return Some(SlashableOffence::SkipAndNotarize(voter, slot));
                }
            }
            VoteKind::SkipFallback(_) => {
                if self.votes.finalize[v].is_some() {
                    return Some(SlashableOffence::SkipAndFinalize(voter, slot));
                }
            }
            VoteKind::Final(_) => {
                if self.votes.skip[v].is_some() || self.votes.skip_fallback[v].is_some() {
                    return Some(SlashableOffence::SkipAndFinalize(voter, slot));
                } else if !self.votes.notar_fallback[v].is_empty() {
                    return Some(SlashableOffence::NotarFallbackAndFinalize(voter, slot));
                }
            }
        }
        None
    }

    /// Returns whether `vote` is a duplicate that must not be counted.
    pub fn should_ignore_vote(&self, vote: &Vote) -> bool {
        let v = vote.signer() as usize;
        match vote.kind() {
            VoteKind::Notar(_, _) => self.votes.notar[v].is_some(),
            VoteKind::NotarFallback(_, block_hash) => {
                self.votes.notar_fallback[v].contains_key(block_hash)
            }
            VoteKind::Skip(_) | VoteKind::SkipFallback(_) => {
                self.votes.skip[v].is_some() || self.votes.skip_fallback[v].is_some()
            }
            VoteKind::Final(_) => self.votes.finalize[v].is_some(),
        }
    }

    fn check_safe_to_notar(&mut self, block_hash: BlockHash) -> SafeToNotarStatus {
        let notar_stake = *self.voted_stakes.notar.get(&block_hash).unwrap_or(&0);
        let skip_stake = self.voted_stakes.skip;
        if !self.is_weakest_quorum(notar_stake) {
            return SafeToNotarStatus::AwaitingVotes;
        }
        if !self.is_weak_quorum(notar_stake) && !self.is_quorum(notar_stake + skip_stake) {
            self.pending_safe_to_notar.insert(block_hash);
            return SafeToNotarStatus::AwaitingVotes;
        }

        match self.parents.entry(block_hash.clone()) {
            Entry::Vacant(_) => return SafeToNotarStatus::MissingBlock,
            Entry::Occupied(entry) => {
                if entry.get() != &ParentStatus::Certified {
                    return SafeToNotarStatus::AwaitingVotes;
                }
            }
        }

        let own_id = self.epoch_info.own_id;
        let skip = &self.votes.skip[own_id as usize];
        let notar = &self.votes.notar[own_id as usize];

        match (skip, notar) {
            (Some(_), _) => {
                self.pending_safe_to_notar.remove(&block_hash);
                self.sent_safe_to_notar.insert(block_hash);
                SafeToNotarStatus::SafeToNotar
            }
            (_, Some(n)) => {
                if n.block_hash().unwrap() != &block_hash {
                    self.pending_safe_to_notar.remove(&block_hash);
                    self.sent_safe_to_notar.insert(block_hash);
                    SafeToNotarStatus::SafeToNotar
                } else {
                    SafeToNotarStatus::AwaitingVotes
                }
            }
            (None, None) => {
                self.pending_safe_to_notar.insert(block_hash);
                SafeToNotarStatus::AwaitingVotes
            }
        }
    }

    /// Returns whether `block_hash` has a notar-fallback cert in this slot.
    pub fn is_notar_fallback(&self, block_hash: &BlockHash) -> bool {
        self.certificates
            .notar_fallback
            .iter()
            .any(|n| n.block_hash() == block_hash)
    }
}

impl SlotVotes {
    /// Creates an empty vote table for `num_validators`.
    pub fn new(num_validators: usize) -> Self {
        Self {
            notar: vec![None; num_validators],
            notar_fallback: vec![BTreeMap::new(); num_validators],
            skip: vec![None; num_validators],
            skip_fallback: vec![None; num_validators],
            finalize: vec![None; num_validators],
        }
    }

    /// Returns all notarization votes for the given block hash.
    pub fn notar_votes(&self, block_hash: &BlockHash) -> Vec<Vote> {
        self.notar
            .iter()
            .filter_map(|vote| {
                vote.as_ref()
                    .and_then(|vote| (vote.block_hash().unwrap() == block_hash).then_some(vote))
            })
            .cloned()
            .collect()
    }

    /// Returns all notar-fallback votes for the given block hash.
    pub fn notar_fallback_votes(&self, block_hash: &BlockHash) -> Vec<Vote> {
        self.notar_fallback
            .iter()
            .filter_map(|m| m.get(block_hash).cloned())
            .collect()
    }

    /// Returns all skip votes for this slot.
    pub fn skip_votes(&self) -> Vec<Vote> {
        self.skip.iter().filter_map(Clone::clone).collect()
    }

    /// Returns all skip-fallback votes for this slot.
    pub fn skip_fallback_votes(&self) -> Vec<Vote> {
        self.skip_fallback.iter().filter_map(Clone::clone).collect()
    }

    /// Returns all finalization votes for this slot.
    pub fn final_votes(&self) -> Vec<Vote> {
        self.finalize.iter().filter_map(Clone::clone).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ValidatorId;
    use crate::crypto::Hash;
    use crate::test_utils::generate_validators;

    #[test]
    fn quorums() {
        let (_, epoch_info) = generate_validators(6);
        let slot_state = SlotState::new(Slot::new(0), epoch_info);
        assert!(slot_state.is_weak_quorum(3));
        assert!(!slot_state.is_quorum(3));
        assert!(slot_state.is_quorum(4));
        assert!(!slot_state.is_strong_quorum(4));
        assert!(slot_state.is_strong_quorum(5));

        let (_, epoch_info) = generate_validators(11);
        let slot_state = SlotState::new(Slot::new(0), epoch_info);
        assert!(slot_state.is_weak_quorum(5));
        assert!(!slot_state.is_quorum(5));
        assert!(slot_state.is_quorum(7));
        assert!(!slot_state.is_strong_quorum(7));
        assert!(slot_state.is_strong_quorum(9));
    }

    #[test]
    fn add_cert() {
        let (sks, epoch_info) = generate_validators(11);
        let (slot, hash): BlockId = (Slot::new(1), Hash::random_for_test().into());
        let mut slot_state = SlotState::new(slot, epoch_info.clone());
        let votes: Vec<_> = sks
            .iter()
            .enumerate()
            .map(|(i, sk)| Vote::new_notar(slot, hash.clone(), sk, i as ValidatorId))
            .collect();
        let cert = NotarCert::try_new(&votes, &epoch_info.validators).unwrap();
        assert!(slot_state.certificates.notar.is_none());
        slot_state.add_cert(Cert::Notar(cert));
        assert!(slot_state.certificates.notar.is_some());
    }

    #[test]
    fn add_vote() {
        let (sks, epoch_info) = generate_validators(11);
        let (slot, hash): BlockId = (Slot::new(1), Hash::random_for_test().into());
        let mut slot_state = SlotState::new(slot, epoch_info.clone());
        for (i, sk) in sks.iter().enumerate() {
            let vote = Vote::new_notar(slot, hash.clone(), sk, i as ValidatorId);
            let voter_stake = epoch_info.validator(i as ValidatorId).stake;
            assert!(slot_state.votes.notar[i].is_none());
            slot_state.add_vote(vote.clone(), voter_stake);
            let notar_vote = &slot_state.votes.notar[i];
            assert!(notar_vote.is_some());
            assert_eq!(
                slot_state.voted_stakes.notar.get(&hash),
                Some(&((i + 1) as Stake))
            );
            assert_eq!(slot_state.voted_stakes.notar_or_skip, (i + 1) as Stake);
        }
    }

    /// Locally-created certs must include the quorum-tipping vote and validate.
    #[test]
    fn locally_created_certs_meet_their_threshold() {
        let (sks, epoch_info) = generate_validators(2);
        let (slot, hash): BlockId = (Slot::new(1), Hash::random_for_test().into());
        let mut slot_state = SlotState::new(slot, epoch_info.clone());

        let mut all_certs = Vec::new();
        for (i, sk) in sks.iter().enumerate() {
            let vote = Vote::new_notar(slot, hash.clone(), sk, i as ValidatorId);
            let voter_stake = epoch_info.validator(i as ValidatorId).stake;
            let (certs, _, _) = slot_state.add_vote(vote, voter_stake);
            all_certs.extend(certs);
        }

        assert!(
            all_certs.iter().any(|c| matches!(c, Cert::Notar(_))),
            "notar cert must be created once both votes are counted"
        );
        assert!(
            all_certs.iter().any(|c| matches!(c, Cert::FastFinal(_))),
            "fast-final cert must be created once both votes are counted"
        );

        for cert in &all_certs {
            assert!(
                cert.check_threshold(&epoch_info),
                "locally-created {} cert fails its own stake threshold — \
                 it was built missing the tipping vote",
                cert.kind_str()
            );
        }
    }

    #[test]
    fn safe_to_notar() {
        let (sks, epoch_info) = generate_validators(3);
        let (slot, hash): BlockId = (Slot::new(1), Hash::random_for_test().into());
        let mut slot_state = SlotState::new(slot, epoch_info.clone());

        slot_state.notify_parent_known(hash.clone());
        slot_state.notify_parent_certified(hash.clone());

        let vote = Vote::new_notar(slot, hash.clone(), &sks[1], 1);
        let voter_stake = epoch_info.validator(1).stake;
        let (certs, events, blocks) = slot_state.add_vote(vote.clone(), voter_stake);
        assert!(certs.is_empty());
        assert!(events.is_empty());
        assert!(blocks.is_empty());

        let vote = Vote::new_skip(slot, &sks[0], 0);
        let voter_stake = epoch_info.validator(0).stake;
        let (certs, events, blocks) = slot_state.add_vote(vote.clone(), voter_stake);
        assert!(certs.is_empty());
        assert_eq!(events.len(), 1);
        assert!(blocks.is_empty());
        match &events[0] {
            VotorEvent::SafeToNotar(s, h) => {
                assert_eq!(*s, slot);
                assert_eq!(*h, hash);
            }
            _ => unreachable!(),
        }
    }
}
