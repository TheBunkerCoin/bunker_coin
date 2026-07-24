// Copyright (c) Anza Technology, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tracks finality of blocks.
//!
//! This is used internally as part of [`PoolImpl`].
//!
//! Keeps track of:
//! - Direct finalization of blocks,
//! - resulting indirect finalizations of blocks, AND
//! - resulting implicit skipping of slots
//!
//! It does this based on:
//! - Notarization of blocks,
//! - finalization of slots, AND
//! - availability of blocks and knowledge of their parents.
//!
//! [`PoolImpl`]: crate::consensus::pool::PoolImpl

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use crate::BlockId;
use crate::crypto::merkle::{BlockHash, GENESIS_BLOCK_HASH};
use crate::types::Slot;

/// Tracks finality of blocks.
pub struct FinalityTracker {
    /// Current finalization status for each slot.
    status: BTreeMap<Slot, FinalizationStatus>,
    /// Maps blocks to their parents.
    parents: BTreeMap<BlockId, BlockId>,
    /// The highest finalized slot so far.
    ///
    /// This means that slot has a fast finalization OR finalization + notarization.
    /// Also, all prior slots are finalized (directly or implicitly) OR implicitly skipped.
    highest_finalized_slot: Slot,
}

/// Possible states a slot can be in regarding finality.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FinalizationStatus {
    /// Block with given hash is notarized, but slot is not yet (known to be) finalized.
    Notarized(BlockHash),
    /// Slot is known to be finalized, but we are missing the notarization certificate.
    FinalPendingNotar,
    /// Slot is finalized, and notarized block is known to have the given hash.
    Finalized(BlockHash),
    /// Block with given hash was implicitly finalized through later finalization.
    ImplicitlyFinalized(BlockHash),
    /// Slot was implicitly skipped through later finalization.
    ImplicitlySkipped,
}

/// Information about newly finalized slots.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FinalizationEvent {
    // TODO: instead use `Option<FinalizationEvent>`?
    /// Directly finalized block, if any.
    pub(super) finalized: Option<BlockId>,
    /// Any implicitly finalized blocks.
    pub(super) implicitly_finalized: Vec<BlockId>,
    /// Any implicitly skipped slots.
    pub(super) implicitly_skipped: Vec<Slot>,
}

impl FinalityTracker {
    /// Adds the given `parent` for the given `block`.
    ///
    /// Handles possibly resulting implicit finalizations.
    ///
    /// Returns a [`FinalizationEvent`] that contains information about newly finalized slots.
    pub fn add_parent(&mut self, block: BlockId, parent: BlockId) -> FinalizationEvent {
        assert!(block.0 > parent.0);
        match self.parents.entry(block.clone()) {
            Entry::Occupied(e) => {
                assert!(e.get() == &parent);
                return FinalizationEvent::default();
            }
            Entry::Vacant(e) => {
                e.insert(parent.clone());
            }
        }

        let (slot, block_hash) = block;
        let Some(status) = self.status.get(&slot) else {
            return FinalizationEvent::default();
        };
        match status {
            FinalizationStatus::Finalized(hash) | FinalizationStatus::ImplicitlyFinalized(hash) => {
                let mut event = FinalizationEvent::default();
                if &block_hash == hash {
                    self.handle_implicitly_finalized(block.0, parent, &mut event);
                }
                event
            }
            FinalizationStatus::Notarized(_)
            | FinalizationStatus::FinalPendingNotar
            | FinalizationStatus::ImplicitlySkipped => FinalizationEvent::default(),
        }
    }

    /// Marks the given block as fast finalized.
    ///
    /// If the block was newly finalized, handles resulting implicit finalizations.
    ///
    /// Returns a [`FinalizationEvent`] that contains information about newly finalized slots.
    pub fn mark_fast_finalized(&mut self, slot: Slot, block_hash: BlockHash) -> FinalizationEvent {
        let old = self
            .status
            .insert(slot, FinalizationStatus::Finalized(block_hash.clone()));
        if let Some(status) = old {
            match status {
                FinalizationStatus::Finalized(hash)
                | FinalizationStatus::ImplicitlyFinalized(hash) => {
                    assert_eq!(hash, block_hash, "consensus safety violation");
                    return FinalizationEvent::default();
                }
                FinalizationStatus::Notarized(hash) => {
                    assert_eq!(hash, block_hash, "consensus safety violation");
                }
                FinalizationStatus::FinalPendingNotar => {}
                FinalizationStatus::ImplicitlySkipped => unreachable!("consensus safety violation"),
            }
        }

        let mut event = FinalizationEvent::default();
        self.handle_finalized_block((slot, block_hash), &mut event);
        event
    }

    /// Marks the given block as notarized.
    ///
    /// Handles possibly resulting direct finalization of the block.
    /// Further, also handles any possibly resulting implicit finalizations.
    ///
    /// Returns a [`FinalizationEvent`] that contains information about newly finalized slots.
    pub fn mark_notarized(&mut self, slot: Slot, block_hash: BlockHash) -> FinalizationEvent {
        let old = self
            .status
            .insert(slot, FinalizationStatus::Notarized(block_hash.clone()));
        let Some(status) = old else {
            return FinalizationEvent::default();
        };

        match &status {
            FinalizationStatus::Notarized(hash) => {
                assert_eq!(*hash, block_hash, "consensus safety violation");
                FinalizationEvent::default()
            }
            FinalizationStatus::Finalized(hash) | FinalizationStatus::ImplicitlyFinalized(hash) => {
                assert_eq!(*hash, block_hash, "consensus safety violation");
                // Restore the finalized status: the insert above downgraded it
                // to Notarized. A downgrade here lets a later descendant's
                // finalization walk descend through this supposedly-final slot
                // and re-report already-finalized (possibly pruned) ancestors,
                // duplicating parent-ready bookkeeping. A notar cert arriving
                // after finalization is routine (cert ordering races).
                self.status.insert(slot, status);
                FinalizationEvent::default()
            }
            FinalizationStatus::ImplicitlySkipped => FinalizationEvent::default(),
            FinalizationStatus::FinalPendingNotar => {
                let mut event = FinalizationEvent::default();
                self.status
                    .insert(slot, FinalizationStatus::Finalized(block_hash.clone()));
                self.handle_finalized_block((slot, block_hash), &mut event);
                event
            }
        }
    }

    /// Marks the given slot as finalized.
    ///
    /// Handles possibly resulting direct finalization of a block in this slot.
    /// Further, also handles any possibly resulting implicit finalizations.
    ///
    /// Returns a [`FinalizationEvent`] that contains information about newly finalized slots.
    pub fn mark_finalized(&mut self, slot: Slot) -> FinalizationEvent {
        let old = self
            .status
            .insert(slot, FinalizationStatus::FinalPendingNotar);
        let Some(status) = old else {
            return FinalizationEvent::default();
        };

        match status {
            FinalizationStatus::FinalPendingNotar => FinalizationEvent::default(),
            status @ (FinalizationStatus::Finalized(_)
            | FinalizationStatus::ImplicitlyFinalized(_)) => {
                // Restore: the insert above would downgrade a finalized slot to
                // FinalPendingNotar; put the finalized status back.
                self.status.insert(slot, status);
                FinalizationEvent::default()
            }
            FinalizationStatus::Notarized(block_hash) => {
                let mut event = FinalizationEvent::default();
                self.status
                    .insert(slot, FinalizationStatus::Finalized(block_hash.clone()));
                self.handle_finalized_block((slot, block_hash), &mut event);
                event
            }
            FinalizationStatus::ImplicitlySkipped => unreachable!("consensus safety violation"),
        }
    }

    /// Returns the highest finalized slot.
    ///
    /// This means that slot has a fast finalization OR finalization + notarization.
    /// Also, all prior slots are finalized (directly or implicitly) OR implicitly skipped.
    pub fn highest_finalized_slot(&self) -> Slot {
        self.highest_finalized_slot
    }

    /// Drops per-slot state for slots strictly below `below`.
    ///
    /// Called as finalization advances (with `below` = the first slot of the
    /// finalized slot's window), since without pruning `status` grows by one
    /// entry per slot and `parents` by one entry per block, forever.
    ///
    /// Safe because nothing consults entries that far back: certs/votes below
    /// the finalized frontier are rejected by the pool before reaching the
    /// tracker, and the implicit-finalization recursion stops at the first
    /// already-finalized ancestor — the frontier slot's own entry, which is at
    /// or above `below` and therefore kept. `highest_finalized_slot` is a
    /// stored field, unaffected.
    pub fn prune(&mut self, below: Slot) {
        self.status = self.status.split_off(&below);
        self.parents
            .retain(|(child_slot, _), _| *child_slot >= below);
    }

    /// Number of tracked per-slot status entries (test-only, for prune assertions).
    #[cfg(test)]
    pub fn status_len(&self) -> usize {
        self.status.len()
    }

    /// Number of tracked parent links (test-only, for prune assertions).
    #[cfg(test)]
    pub fn parents_len(&self) -> usize {
        self.parents.len()
    }

    /// Handles the direct finalization of the given block.
    ///
    /// Recurses through ancestors, potentially implicitly finalizing them.
    ///
    /// Updates the `event` all along the way with:
    /// - The finalized block,
    /// - any potentially implicitly finalized blocks, AND
    /// - any implicitly skipped slots.
    fn handle_finalized_block(&mut self, finalized: BlockId, event: &mut FinalizationEvent) {
        let (slot, _) = finalized;
        event.finalized = Some(finalized.clone());
        self.highest_finalized_slot = slot.max(self.highest_finalized_slot);

        if let Some(parent) = self.parents.get(&finalized).cloned() {
            self.handle_implicitly_finalized(slot, parent, event);
        }
    }

    /// Handles the indirect finalization of the given block.
    ///
    /// Recurses through ancestors, potentially implicitly finalizing them as well.
    ///
    /// Updates the `event` all along the way with:
    /// - Any potentially implicitly finalized blocks, AND
    /// - any implicitly skipped slots.
    fn handle_implicitly_finalized(
        &mut self,
        source_slot: Slot,
        implicitly_finalized: BlockId,
        event: &mut FinalizationEvent,
    ) {
        assert!(source_slot > implicitly_finalized.0);
        // NOTE on pruning: this walk may cross the prune floor when parent
        // links arrive out of order (a block learned only after finalization).
        // That is a genuine FIRST report and must proceed — downstream
        // parent-ready derivation depends on it. Re-reports of already-
        // finalized ancestors cannot get here: the walk stops at the first
        // kept Finalized/ImplicitlyFinalized status, and `mark_notarized`/
        // `mark_finalized` restore (never downgrade) those statuses.

        for slot in implicitly_finalized.0.future_slots() {
            if slot == source_slot {
                break;
            }
            let old = self
                .status
                .insert(slot, FinalizationStatus::ImplicitlySkipped);
            if let Some(status) = old {
                match status {
                    FinalizationStatus::ImplicitlySkipped => {
                        return;
                    }
                    FinalizationStatus::Notarized(_) => {}
                    FinalizationStatus::FinalPendingNotar
                    | FinalizationStatus::Finalized(_)
                    | FinalizationStatus::ImplicitlyFinalized(_) => {
                        unreachable!("consensus safety violation")
                    }
                }
            }
            event.implicitly_skipped.push(slot);
        }

        let (slot, block_hash) = implicitly_finalized.clone();
        let old = self.status.insert(
            slot,
            FinalizationStatus::ImplicitlyFinalized(block_hash.clone()),
        );
        if let Some(status) = old {
            match &status {
                FinalizationStatus::Finalized(hash)
                | FinalizationStatus::ImplicitlyFinalized(hash) => {
                    assert_eq!(*hash, block_hash, "consensus safety violation");
                    self.status.insert(slot, status);
                    return;
                }
                FinalizationStatus::Notarized(_) | FinalizationStatus::FinalPendingNotar => {}
                FinalizationStatus::ImplicitlySkipped => {
                    unreachable!("consensus safety violation")
                }
            }
        }
        event
            .implicitly_finalized
            .push(implicitly_finalized.clone());

        if let Some(parent) = self.parents.get(&implicitly_finalized).cloned() {
            self.handle_implicitly_finalized(implicitly_finalized.0, parent, event);
        }
    }
}

impl Default for FinalityTracker {
    /// Creates a new empty tracker.
    ///
    /// Initially, only the genesis block is considered (directly) finalized.
    fn default() -> Self {
        let mut status = BTreeMap::new();
        status.insert(
            Slot::genesis(),
            FinalizationStatus::Notarized(GENESIS_BLOCK_HASH),
        );
        Self {
            status,
            parents: BTreeMap::new(),
            highest_finalized_slot: Slot::genesis(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::Hash;

    /// A notar (or final) cert arriving AFTER a slot finalized must not
    /// downgrade its status: `mark_notarized`/`mark_finalized` insert first
    /// and must restore `Finalized` afterwards. A downgraded slot, combined
    /// with state pruning, lets a descendant's finalization walk re-descend
    /// through it and re-report pruned, already-finalized ancestors
    /// (duplicate parent-ready add → consensus task panic).
    #[test]
    fn late_certs_do_not_downgrade_finalized_status() {
        let mut tracker = FinalityTracker::default();
        let (h3, h4, h5): (BlockHash, BlockHash, BlockHash) = (
            Hash::random_for_test().into(),
            Hash::random_for_test().into(),
            Hash::random_for_test().into(),
        );
        let (s3, s4, s5) = (Slot::new(3), Slot::new(4), Slot::new(5));

        // Chain 3 <- 4 <- 5; slot 3 finalized; slot 4 fast-finalized.
        tracker.mark_notarized(s3, h3.clone());
        tracker.mark_finalized(s3);
        tracker.add_parent((s4, h4.clone()), (s3, h3.clone()));
        tracker.add_parent((s5, h5.clone()), (s4, h4.clone()));
        let ev = tracker.mark_fast_finalized(s4, h4.clone());
        assert_eq!(ev.finalized, Some((s4, h4.clone())));

        // Prune below the finalized window start (as the pool does).
        tracker.prune(s4);

        // LATE certs for slot 4 (routine cert-ordering races) must not
        // downgrade its Finalized status...
        assert_eq!(
            tracker.mark_notarized(s4, h4.clone()),
            FinalizationEvent::default()
        );
        assert_eq!(tracker.mark_finalized(s4), FinalizationEvent::default());

        // ...so finalizing slot 5 must NOT re-report already-finalized
        // ancestors by descending through a downgraded slot 4 into the
        // pruned region.
        let ev = tracker.mark_fast_finalized(s5, h5.clone());
        assert_eq!(ev.finalized, Some((s5, h5)));
        assert_eq!(
            ev.implicitly_finalized,
            vec![],
            "already-finalized ancestors must not be re-reported"
        );
    }

    /// Out-of-order parent links learned AFTER pruning must still produce a
    /// FIRST report of implicitly finalized ancestors below the prune floor —
    /// the downstream parent-ready derivation for the current window depends
    /// on it (this is the `parent_ready_upon_finalization` pool scenario).
    #[test]
    fn out_of_order_parents_below_prune_floor_still_first_reported() {
        let mut tracker = FinalityTracker::default();
        let (h3, h4, h5): (BlockHash, BlockHash, BlockHash) = (
            Hash::random_for_test().into(),
            Hash::random_for_test().into(),
            Hash::random_for_test().into(),
        );
        let (s3, s4, s5) = (Slot::new(3), Slot::new(4), Slot::new(5));

        // Slot 5 fast-finalizes with NO parent links known yet, then prune.
        tracker.mark_notarized(s5, h5.clone());
        let ev = tracker.mark_fast_finalized(s5, h5.clone());
        assert_eq!(ev.finalized, Some((s5, h5.clone())));
        tracker.prune(s4);

        // Parent links arrive late (block bodies learned after finalization).
        let ev = tracker.add_parent((s5, h5), (s4, h4.clone()));
        assert_eq!(ev.implicitly_finalized, vec![(s4, h4.clone())]);
        let ev = tracker.add_parent((s4, h4), (s3, h3.clone()));
        assert_eq!(
            ev.implicitly_finalized,
            vec![(s3, h3)],
            "first report of a below-floor ancestor must still fire"
        );
    }

    #[test]
    fn basic() {
        let mut tracker = FinalityTracker::default();

        // slow finalize a block
        let slot1 = Slot::genesis().next();
        let hash1: BlockHash = Hash::random_for_test().into();
        let event = tracker.mark_notarized(slot1, hash1.clone());
        assert_eq!(event, FinalizationEvent::default());
        let event = tracker.mark_finalized(slot1);
        assert_eq!(event.finalized, Some((slot1, hash1)));
        assert_eq!(event.implicitly_finalized, vec![]);
        assert_eq!(event.implicitly_skipped, vec![]);

        // fast finalize a block
        let slot2 = slot1.next();
        let hash2: BlockHash = Hash::random_for_test().into();
        let event = tracker.mark_fast_finalized(slot2, hash2.clone());
        assert_eq!(event.finalized, Some((slot2, hash2)));
        assert_eq!(event.implicitly_finalized, vec![]);
        assert_eq!(event.implicitly_skipped, vec![]);

        // implicitly finalize a block WITHOUT skips
        let slot4 = slot2.next().next();
        let hash3: BlockHash = Hash::random_for_test().into();
        let hash4: BlockHash = Hash::random_for_test().into();
        let event = tracker.add_parent((slot4, hash4.clone()), (slot4.prev(), hash3.clone()));
        assert_eq!(event, FinalizationEvent::default());
        let event = tracker.mark_fast_finalized(slot4, hash4.clone());
        assert_eq!(event.finalized, Some((slot4, hash4)));
        assert_eq!(event.implicitly_finalized, vec![(slot4.prev(), hash3)]);
        assert_eq!(event.implicitly_skipped, vec![]);

        // implicitly finalize a block WITH skips
        let slot7 = slot4.next().next().next();
        let hash5: BlockHash = Hash::random_for_test().into();
        let hash7: BlockHash = Hash::random_for_test().into();
        let event =
            tracker.add_parent((slot7, hash7.clone()), (slot7.prev().prev(), hash5.clone()));
        assert_eq!(event, FinalizationEvent::default());
        let event = tracker.mark_fast_finalized(slot7, hash7.clone());
        assert_eq!(event.finalized, Some((slot7, hash7.clone())));
        assert_eq!(
            event.implicitly_finalized,
            vec![(slot7.prev().prev(), hash5)]
        );
        assert_eq!(event.implicitly_skipped, vec![slot7.prev()]);
    }

    #[test]
    fn no_duplicates() {
        let mut tracker = FinalityTracker::default();

        // slow finalize + fast finalize a block
        let slot1 = Slot::genesis().next();
        let hash1: BlockHash = Hash::random_for_test().into();
        let event = tracker.mark_finalized(slot1);
        assert_eq!(event, FinalizationEvent::default());
        let event = tracker.mark_notarized(slot1, hash1.clone());
        assert_eq!(event.finalized, Some((slot1, hash1.clone())));
        assert_eq!(event.implicitly_finalized, vec![]);
        assert_eq!(event.implicitly_skipped, vec![]);
        let event = tracker.mark_fast_finalized(slot1, hash1.clone());
        assert_eq!(event, FinalizationEvent::default());

        // do NOT implicitly finalize parent, that is already directly finalized
        let slot2 = slot1.next();
        let hash2: BlockHash = Hash::random_for_test().into();
        let event = tracker.add_parent((slot2, hash2.clone()), (slot2.prev(), hash1.clone()));
        assert_eq!(event, FinalizationEvent::default());
        let event = tracker.mark_fast_finalized(slot2, hash2.clone());
        assert_eq!(event.finalized, Some((slot2, hash2.clone())));
        assert_eq!(event.implicitly_finalized, vec![]);
        assert_eq!(event.implicitly_skipped, vec![]);

        // implicitly finalize a block WITHOUT skips
        let slot4 = slot2.next().next();
        let hash3: BlockHash = Hash::random_for_test().into();
        let hash4: BlockHash = Hash::random_for_test().into();
        let event = tracker.add_parent((slot4, hash4.clone()), (slot4.prev(), hash3.clone()));
        assert_eq!(event, FinalizationEvent::default());
        let event = tracker.mark_fast_finalized(slot4, hash4.clone());
        assert_eq!(event.finalized, Some((slot4, hash4.clone())));
        assert_eq!(
            event.implicitly_finalized,
            vec![(slot4.prev(), hash3.clone())]
        );
        assert_eq!(event.implicitly_skipped, vec![]);

        // do NOT implicitly finalize parent again when adding parent again
        let event = tracker.add_parent((slot4, hash4.clone()), (slot4.prev(), hash3.clone()));
        assert_eq!(event, FinalizationEvent::default());
    }
}
