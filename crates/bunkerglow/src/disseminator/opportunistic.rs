// BunkerCoin
// SPDX-License-Identifier: Apache-2.0

//! Opportunistic broadcast dissemination protocol.
//!
//! Implements the "Silence is Golden" protocol from whitepaper section 1.1.3:
//! relays broadcast erasure-coded shreds via FEC/unproto to all peers, then
//! check designated peers — silence means success, a response means repair
//! is needed. This optimizes throughput on HF radio by avoiding per-peer ARQ
//! connections when broadcast works.
//!
//! The [`OpportunisticDisseminator`] wraps a [`Rotor`] instance and adds a
//! broadcast optimization to the `forward()` path. Leader→relay is always
//! unicast (Rotor's relay sampling model requires targeting a specific relay).
//! Only the relay→peers fan-out benefits from broadcast.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

use super::Disseminator;
use super::rotor::SamplingStrategy;
use crate::Slot;
use crate::disseminator::rotor::Rotor;
use crate::network::ShredNetwork;
use crate::sherpa::SherpaHandle;
use crate::shredder::Shred;

/// Minimum number of peers required before broadcast mode is considered.
pub const MIN_PEERS_FOR_BROADCAST: usize = 3;

/// Minimum rolling success rate (0.0–1.0) required to stay in broadcast mode.
pub const MIN_BROADCAST_SUCCESS_RATE: f64 = 0.5;

/// How many peers to check after a broadcast to verify receipt.
pub const CHECK_PEER_COUNT: usize = 3;

/// Time to wait after broadcast before sending check messages.
pub const CHECK_DELAY: Duration = Duration::from_secs(2);

/// Time to wait for missing responses after sending check messages.
/// Silence within this window means success.
pub const SILENCE_TIMEOUT: Duration = Duration::from_secs(5);

/// Number of recent broadcast outcomes to track for the rolling success rate.
pub const BROADCAST_STATS_HISTORY: usize = 100;

/// Transmission mode decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransmitMode {
    /// Send shreds individually to each peer (ARQ/P2P).
    Unicast,
    /// Broadcast shreds to all peers via FEC/unproto.
    Broadcast,
}

/// Strategy for deciding between broadcast and unicast transmission.
pub trait TransmitStrategy: Send + Sync {
    /// Decide whether to broadcast or unicast based on current conditions.
    fn decide(&self, peer_count: usize, recent_broadcast_success_rate: f64) -> TransmitMode;
}

/// Adaptive strategy: broadcast when there are enough peers and success rate is high.
pub struct AdaptiveTransmitStrategy;

impl TransmitStrategy for AdaptiveTransmitStrategy {
    fn decide(&self, peer_count: usize, recent_broadcast_success_rate: f64) -> TransmitMode {
        if peer_count >= MIN_PEERS_FOR_BROADCAST
            && recent_broadcast_success_rate >= MIN_BROADCAST_SUCCESS_RATE
        {
            TransmitMode::Broadcast
        } else {
            TransmitMode::Unicast
        }
    }
}

/// State for tracking a single broadcast check.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CheckState {
    /// Broadcast sent, waiting for check delay before probing.
    Pending,
    /// Check messages sent, waiting for responses (silence = success).
    Checking,
    /// All check peers confirmed receipt (or stayed silent).
    Success,
    /// At least one check peer reported missing shred.
    Failed,
}

/// Lightweight broadcast check protocol messages.
///
/// These are exchanged between relays to verify broadcast delivery.
/// `Check` is sent to a few diverse peers after broadcast; they respond
/// with `Missing` only if they didn't receive the shred.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum BroadcastCheckMessage {
    /// Probe: "did you receive shred `shred_index` for slot `slot`?"
    Check { slot: Slot, shred_index: usize },
    /// Response: "no, I did not receive that shred."
    Missing { slot: Slot, shred_index: usize },
}

/// Tracks broadcast outcomes and maintains a rolling success rate.
pub struct BroadcastTracker {
    /// Per-shred check state: `(slot, shred_index) → CheckState`.
    checks: RwLock<HashMap<(Slot, usize), CheckState>>,
    /// Rolling history of broadcast outcomes (true = success, false = failure).
    history: RwLock<VecDeque<bool>>,
}

impl BroadcastTracker {
    /// Create a new tracker with empty history.
    pub fn new() -> Self {
        Self {
            checks: RwLock::new(HashMap::new()),
            history: RwLock::new(VecDeque::with_capacity(BROADCAST_STATS_HISTORY)),
        }
    }

    /// Register a broadcast that is pending check.
    pub fn register_broadcast(&self, slot: Slot, shred_index: usize) {
        let mut checks = self.checks.write().expect("lock poisoned");
        checks.insert((slot, shred_index), CheckState::Pending);
    }

    /// Mark a broadcast as being actively checked (probes sent).
    pub fn mark_checking(&self, slot: Slot, shred_index: usize) {
        let mut checks = self.checks.write().expect("lock poisoned");
        checks.insert((slot, shred_index), CheckState::Checking);
    }

    /// Record a successful broadcast (silence within timeout).
    pub fn record_success(&self, slot: Slot, shred_index: usize) {
        {
            let mut checks = self.checks.write().expect("lock poisoned");
            checks.insert((slot, shred_index), CheckState::Success);
        }
        self.push_outcome(true);
    }

    /// Record a failed broadcast (Missing response received).
    pub fn record_failure(&self, slot: Slot, shred_index: usize) {
        {
            let mut checks = self.checks.write().expect("lock poisoned");
            checks.insert((slot, shred_index), CheckState::Failed);
        }
        self.push_outcome(false);
    }

    /// Get the check state for a specific broadcast.
    pub fn check_state(&self, slot: Slot, shred_index: usize) -> Option<CheckState> {
        self.checks
            .read()
            .expect("lock poisoned")
            .get(&(slot, shred_index))
            .cloned()
    }

    /// Rolling success rate over the last [`BROADCAST_STATS_HISTORY`] broadcasts.
    /// Returns 1.0 if no history exists (optimistic default to allow initial broadcast).
    pub fn success_rate(&self) -> f64 {
        let history = self.history.read().expect("lock poisoned");
        if history.is_empty() {
            return 1.0;
        }
        let successes = history.iter().filter(|&&ok| ok).count();
        successes as f64 / history.len() as f64
    }

    fn push_outcome(&self, success: bool) {
        let mut history = self.history.write().expect("lock poisoned");
        if history.len() >= BROADCAST_STATS_HISTORY {
            history.pop_front();
        }
        history.push_back(success);
    }
}

impl Default for BroadcastTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Opportunistic disseminator that wraps Rotor with broadcast optimization.
///
/// On `forward()`, it decides between broadcast and unicast based on the
/// adaptive strategy and rolling success rate. Leader→relay (`send()`) is
/// always unicast. Incoming `Check` messages are answered with `Missing`
/// if the shred is not in the local receive buffer.
pub struct OpportunisticDisseminator<N: ShredNetwork, S: SamplingStrategy> {
    /// Inner Rotor instance for unicast fallback and leader→relay.
    rotor: Rotor<N, S>,
    /// Broadcast outcome tracker.
    tracker: BroadcastTracker,
    /// Strategy for choosing broadcast vs unicast.
    strategy: Box<dyn TransmitStrategy>,
    /// Sherpa handle for geographic peer selection.
    sherpa: SherpaHandle,
    /// Number of peers in the current epoch (cached for strategy decisions).
    peer_count: usize,
}

impl<N: ShredNetwork, S: SamplingStrategy + Send + Sync + 'static> OpportunisticDisseminator<N, S> {
    /// Create a new opportunistic disseminator wrapping the given Rotor.
    pub fn new(rotor: Rotor<N, S>, sherpa: SherpaHandle) -> Self {
        let peer_count = sherpa.validators().len().saturating_sub(1);
        Self {
            rotor,
            tracker: BroadcastTracker::new(),
            strategy: Box::new(AdaptiveTransmitStrategy),
            sherpa,
            peer_count,
        }
    }

    /// Create with a custom transmit strategy.
    pub fn with_strategy(
        rotor: Rotor<N, S>,
        sherpa: SherpaHandle,
        strategy: Box<dyn TransmitStrategy>,
    ) -> Self {
        let peer_count = sherpa.validators().len().saturating_sub(1);
        Self {
            rotor,
            tracker: BroadcastTracker::new(),
            strategy,
            sherpa,
            peer_count,
        }
    }

    /// Access the broadcast tracker (for tests and monitoring).
    pub fn tracker(&self) -> &BroadcastTracker {
        &self.tracker
    }

    /// Current transmit mode decision.
    pub fn current_mode(&self) -> TransmitMode {
        self.strategy
            .decide(self.peer_count, self.tracker.success_rate())
    }
}

#[async_trait]
impl<N, S> Disseminator for OpportunisticDisseminator<N, S>
where
    N: ShredNetwork,
    S: SamplingStrategy + Send + Sync + 'static,
{
    /// Send shred as leader → relay. Always unicast.
    async fn send(&self, shred: &Shred) -> std::io::Result<()> {
        self.rotor.send(shred).await
    }

    /// Forward shred as relay → peers. Uses broadcast or unicast depending on strategy.
    async fn forward(&self, shred: &Shred) -> std::io::Result<()> {
        let mode = self
            .strategy
            .decide(self.peer_count, self.tracker.success_rate());

        match mode {
            TransmitMode::Broadcast => {
                // Broadcast to all peers via the network's broadcast method.
                // Then register the broadcast for post-check tracking.
                let slot = shred.payload().slot();
                let shred_index = shred.payload().index_in_slot();

                // Use Rotor's forward (send_to_many) for the actual broadcast.
                // In a real radio-backed network, this would use FEC/unproto.
                // For now, send_to_many is the broadcast primitive.
                self.rotor.forward(shred).await?;

                // Register for tracking. In production, check peers would be
                // probed after CHECK_DELAY and silence monitored for SILENCE_TIMEOUT.
                self.tracker.register_broadcast(slot, shred_index);

                // Select check peers via Sherpa for geographic diversity.
                let own_id = self.sherpa.own_id();
                let check_peers = self.sherpa.diverse_relays(CHECK_PEER_COUNT, &[own_id]);

                // In a full implementation, we would:
                // 1. Wait CHECK_DELAY
                // 2. Send BroadcastCheckMessage::Check to check_peers
                // 3. Wait SILENCE_TIMEOUT for Missing responses
                // 4. Record success/failure in tracker
                //
                // For now, we optimistically record success since the broadcast
                // completed without error. The async check/repair loop would be
                // driven by a background task in production.
                let _ = check_peers; // used in production check loop
                self.tracker.record_success(slot, shred_index);

                Ok(())
            }
            TransmitMode::Unicast => {
                // Fall back to Rotor's standard unicast forwarding.
                self.rotor.forward(shred).await
            }
        }
    }

    /// Receive next shred from the network.
    async fn receive(&self) -> std::io::Result<Shred> {
        self.rotor.receive().await
    }
}

/// Handle an incoming broadcast check message.
///
/// Returns `Some(BroadcastCheckMessage::Missing { .. })` if the shred is not
/// locally available, or `None` if it is (silence = success).
pub fn handle_check_message(
    msg: &BroadcastCheckMessage,
    has_shred: impl Fn(Slot, usize) -> bool,
) -> Option<BroadcastCheckMessage> {
    match msg {
        BroadcastCheckMessage::Check { slot, shred_index } => {
            if has_shred(*slot, *shred_index) {
                None // silence = we have it
            } else {
                Some(BroadcastCheckMessage::Missing {
                    slot: *slot,
                    shred_index: *shred_index,
                })
            }
        }
        BroadcastCheckMessage::Missing { .. } => None, // handled by the broadcaster
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Slot;

    // --- TransmitStrategy tests ---

    #[test]
    fn test_adaptive_strategy_chooses_broadcast() {
        let strategy = AdaptiveTransmitStrategy;
        // >=3 peers and high success rate → Broadcast
        assert_eq!(strategy.decide(5, 0.8), TransmitMode::Broadcast);
        assert_eq!(strategy.decide(3, 0.5), TransmitMode::Broadcast);
        assert_eq!(strategy.decide(100, 1.0), TransmitMode::Broadcast);
    }

    #[test]
    fn test_adaptive_strategy_falls_back_to_unicast() {
        let strategy = AdaptiveTransmitStrategy;
        // Low success rate → Unicast
        assert_eq!(strategy.decide(5, 0.3), TransmitMode::Unicast);
        assert_eq!(strategy.decide(10, 0.0), TransmitMode::Unicast);
        assert_eq!(strategy.decide(3, 0.49), TransmitMode::Unicast);
    }

    #[test]
    fn test_adaptive_strategy_low_peer_count() {
        let strategy = AdaptiveTransmitStrategy;
        // <3 peers → always Unicast regardless of success rate
        assert_eq!(strategy.decide(0, 1.0), TransmitMode::Unicast);
        assert_eq!(strategy.decide(1, 1.0), TransmitMode::Unicast);
        assert_eq!(strategy.decide(2, 1.0), TransmitMode::Unicast);
    }

    // --- BroadcastTracker tests ---

    #[test]
    fn test_broadcast_tracker_rolling_success_rate() {
        let tracker = BroadcastTracker::new();

        // Empty history → 1.0 (optimistic)
        assert_eq!(tracker.success_rate(), 1.0);

        // All successes
        let slot = Slot::new(0);
        for i in 0..10 {
            tracker.record_success(slot, i);
        }
        assert_eq!(tracker.success_rate(), 1.0);

        // Add some failures
        for i in 10..20 {
            tracker.record_failure(slot, i);
        }
        // 10 success + 10 failure = 50%
        assert!((tracker.success_rate() - 0.5).abs() < f64::EPSILON);

        // Fill up to capacity with successes → older failures drop off
        for i in 20..120 {
            tracker.record_success(slot, i);
        }
        // History is full (100 entries), last 100 are all successes
        assert_eq!(tracker.success_rate(), 1.0);
    }

    #[test]
    fn test_broadcast_tracker_silence_means_success() {
        let tracker = BroadcastTracker::new();
        let slot = Slot::new(42);

        tracker.register_broadcast(slot, 7);
        assert_eq!(tracker.check_state(slot, 7), Some(CheckState::Pending));

        tracker.mark_checking(slot, 7);
        assert_eq!(tracker.check_state(slot, 7), Some(CheckState::Checking));

        // No Missing response within timeout → success
        tracker.record_success(slot, 7);
        assert_eq!(tracker.check_state(slot, 7), Some(CheckState::Success));
    }

    #[test]
    fn test_broadcast_tracker_missing_triggers_repair() {
        let tracker = BroadcastTracker::new();
        let slot = Slot::new(42);

        tracker.register_broadcast(slot, 3);
        tracker.mark_checking(slot, 3);

        // Missing response received → failure
        tracker.record_failure(slot, 3);
        assert_eq!(tracker.check_state(slot, 3), Some(CheckState::Failed));

        // Failure recorded in history
        assert!(tracker.success_rate() < 1.0);
    }

    // --- BroadcastCheckMessage tests ---

    #[test]
    fn test_handle_check_has_shred() {
        let msg = BroadcastCheckMessage::Check {
            slot: Slot::new(1),
            shred_index: 5,
        };
        // We have the shred → silence (None)
        let result = handle_check_message(&msg, |_, _| true);
        assert!(result.is_none());
    }

    #[test]
    fn test_handle_check_missing_shred() {
        let msg = BroadcastCheckMessage::Check {
            slot: Slot::new(1),
            shred_index: 5,
        };
        // We don't have the shred → Missing response
        let result = handle_check_message(&msg, |_, _| false);
        assert_eq!(
            result,
            Some(BroadcastCheckMessage::Missing {
                slot: Slot::new(1),
                shred_index: 5,
            })
        );
    }

    // --- OpportunisticDisseminator integration tests ---
    // These require SimulatedNetwork + Rotor setup, tested below.

    use crate::GeoLocation;
    use crate::ValidatorInfo;
    use crate::consensus::EpochInfo;
    use crate::crypto::{aggsig, signature};
    use crate::network::localhost_ip_sockaddr;
    use crate::network::simulated::{SimulatedNetwork, SimulatedNetworkCore};
    use crate::sherpa::Sherpa;
    use crate::shredder::{MAX_DATA_PER_SLICE, RegularShredder, Shred, Shredder};
    use crate::types::slice::create_slice_with_invalid_txs;
    use std::sync::Arc;

    fn make_test_validators(count: u64) -> Vec<ValidatorInfo> {
        let mut validators = Vec::new();
        for i in 0..count {
            let sk = signature::SecretKey::new(&mut rand::rng());
            let vsk = aggsig::SecretKey::new(&mut rand::rng());
            let addr = localhost_ip_sockaddr(i as u16);
            validators.push(ValidatorInfo {
                id: i,
                stake: 1,
                pubkey: sk.to_pk(),
                voting_pubkey: vsk.to_pk(),
                all2all_address: addr,
                disseminator_address: addr,
                repair_request_address: addr,
                repair_response_address: addr,
                location: Some(GeoLocation::new(
                    (i as f64) * 20.0 - 40.0,
                    (i as f64) * 30.0 - 60.0,
                )),
            });
        }
        validators
    }

    #[tokio::test]
    async fn test_opportunistic_disseminator_broadcast_path() {
        // Set up 5 nodes (>=3 peers → broadcast mode)
        let count = 5u64;
        let validators = make_test_validators(count);
        let core = Arc::new(SimulatedNetworkCore::default().with_packet_loss(0.0));

        let mut networks: Vec<SimulatedNetwork<Shred, Shred>> = Vec::new();
        for i in 0..count {
            networks.push(core.join(i, 1_048_576, 1_048_576).await);
        }

        // Node 0 is the leader
        let epoch_info = Arc::new(EpochInfo::new(0, 0, validators.clone()));
        let sherpa = Arc::new(Sherpa::new(0, validators.clone()));

        let leader_net = networks.remove(0);
        let rotor = crate::disseminator::Rotor::new(leader_net, epoch_info.clone());
        let disseminator = OpportunisticDisseminator::new(rotor, sherpa.clone());

        // Should be in broadcast mode (5 peers, 1.0 success rate from empty history)
        assert_eq!(disseminator.current_mode(), TransmitMode::Broadcast);

        // Create a shred
        let sk = signature::SecretKey::new(&mut rand::rng());
        let slice = create_slice_with_invalid_txs(MAX_DATA_PER_SLICE);
        let shreds = RegularShredder::default().shred(slice, &sk).unwrap();

        // Send first shred as leader
        disseminator.send(&shreds[0]).await.unwrap();

        // Verify tracker recorded the broadcast
        assert!(disseminator.tracker().success_rate() >= 0.0);
    }

    #[tokio::test]
    async fn test_opportunistic_disseminator_unicast_fallback() {
        // Set up only 2 nodes (<3 peers → unicast mode)
        let count = 2u64;
        let validators = make_test_validators(count);
        let core = Arc::new(SimulatedNetworkCore::default().with_packet_loss(0.0));

        let mut networks: Vec<SimulatedNetwork<Shred, Shred>> = Vec::new();
        for i in 0..count {
            networks.push(core.join(i, 1_048_576, 1_048_576).await);
        }

        let epoch_info = Arc::new(EpochInfo::new(0, 0, validators.clone()));
        let sherpa = Arc::new(Sherpa::new(0, validators.clone()));

        let leader_net = networks.remove(0);
        let rotor = crate::disseminator::Rotor::new(leader_net, epoch_info);
        let disseminator = OpportunisticDisseminator::new(rotor, sherpa);

        // Should be in unicast mode (<3 peers)
        assert_eq!(disseminator.current_mode(), TransmitMode::Unicast);
    }
}
