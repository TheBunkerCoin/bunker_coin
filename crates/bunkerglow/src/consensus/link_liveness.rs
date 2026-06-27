//! Link-liveness signal for distinguishing a *crashed leader* from a *slow but
//! alive* link.
//!
//! Alpenglow's crashed-leader timeout assumes network synchrony: if no shred
//! arrives within the timeout, the leader is presumed crashed and the window is
//! skipped (a *liveness* mechanism). Over a half-duplex PACTOR link that goes
//! quiet for minutes while still up (the slow reverse ARQ path), that assumption
//! breaks: the leader is alive and its block is merely crawling across, but the
//! timeout fires and the window is skipped irreversibly — leaving a permanent
//! chain gap and forcing a jump-ahead when the peer's catch-up cert arrives.
//!
//! [`LinkLiveness`] lets the transport tell Votor whether the link is actually
//! up (e.g. keepalives are still being received). When it is, Votor *pauses*
//! (re-arms the timeout) instead of skipping — see the crashed-leader handling
//! in [`super::votor`]. This only ever makes skipping *more* conservative, so it
//! cannot violate safety; it trades a bounded amount of skip-promptness for not
//! gapping the chain on a slow link.

use std::sync::Arc;

/// Reports whether there is *positive evidence* the link to peers is up.
///
/// The crashed-leader timeout only pauses (instead of skipping) when this returns
/// `true`. So the default must be `false`: absent a real liveness source, behave
/// exactly as before (skip on timeout). Only a transport that can affirmatively
/// observe liveness — e.g. radio keepalive receipt — returns `true`, and only
/// while it actually sees that evidence.
pub trait LinkLiveness: Send + Sync {
    /// `true` only if there is recent positive evidence the link is alive.
    fn is_link_alive(&self) -> bool;
}

/// Default: no liveness evidence available → never pauses the crashed-leader
/// timeout (preserves the original skip-on-timeout behavior).
///
/// Used for the simulator and UDP paths, which have no half-duplex quiet periods
/// to ride out: there, a peer that stops sending really is gone, so the timeout
/// should skip as before. This impl changes nothing for those paths.
pub struct NoLiveness;

impl LinkLiveness for NoLiveness {
    fn is_link_alive(&self) -> bool {
        false
    }
}

/// A [`LinkLiveness`] whose backing source can be swapped after construction.
///
/// Votor is moved into a detached task at node construction, so the liveness
/// source cannot be passed in afterwards directly. Votor instead holds a
/// `SwappableLiveness` (defaulting to [`NoLiveness`]); the node exposes a setter
/// that swaps in the real (e.g. keepalive-driven) source once the transport is
/// wired, without changing the node constructor's signature.
pub struct SwappableLiveness {
    inner: std::sync::Mutex<Arc<dyn LinkLiveness>>,
}

impl SwappableLiveness {
    /// New swappable source, initially [`NoLiveness`].
    pub fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(Arc::new(NoLiveness)),
        }
    }

    /// Replace the backing liveness source.
    pub fn set(&self, liveness: Arc<dyn LinkLiveness>) {
        *self.inner.lock().unwrap() = liveness;
    }
}

impl Default for SwappableLiveness {
    fn default() -> Self {
        Self::new()
    }
}

impl LinkLiveness for SwappableLiveness {
    fn is_link_alive(&self) -> bool {
        self.inner.lock().unwrap().is_link_alive()
    }
}
