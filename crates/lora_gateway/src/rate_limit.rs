//! Per-sender token-bucket rate limiter.

use std::collections::HashMap;
use std::time::Instant;

use bunker_coin_core::types::PublicKey;

struct Bucket {
    tokens: f64,
    last_refill: Instant,
}

/// Token-bucket rate limiter keyed by sender public key.
pub struct PerSenderRateLimiter {
    rate: f64,
    burst: f64,
    max_senders: usize,
    buckets: HashMap<PublicKey, Bucket>,
    lru: Vec<PublicKey>,
}

impl PerSenderRateLimiter {
    /// Create a limiter with sustained rate, burst capacity, and sender cap.
    pub fn new(capsules_per_minute: f64, burst: f64, max_senders: usize) -> Self {
        Self {
            rate: capsules_per_minute / 60.0,
            burst,
            max_senders,
            buckets: HashMap::new(),
            lru: Vec::new(),
        }
    }

    /// Returns `true` if the sender is allowed (token consumed), `false` if rate-limited.
    pub fn check(&mut self, sender: &PublicKey, now: Instant) -> bool {
        if let Some(pos) = self.lru.iter().position(|k| k == sender) {
            self.lru.remove(pos);
        }
        self.lru.push(*sender);

        let bucket = self.buckets.entry(*sender).or_insert(Bucket {
            tokens: self.burst,
            last_refill: now,
        });

        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.rate).min(self.burst);
        bucket.last_refill = now;

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            while self.lru.len() > self.max_senders {
                if let Some(old) = self.lru.first().cloned() {
                    self.lru.remove(0);
                    self.buckets.remove(&old);
                }
            }
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn pk(n: u8) -> PublicKey {
        let mut key = [0u8; 32];
        key[0] = n;
        key
    }

    #[test]
    fn within_limit_ok() {
        let mut rl = PerSenderRateLimiter::new(2.0, 4.0, 256);
        let now = Instant::now();
        assert!(rl.check(&pk(1), now));
        assert!(rl.check(&pk(1), now));
        assert!(rl.check(&pk(1), now));
        assert!(rl.check(&pk(1), now));
    }

    #[test]
    fn exceeds_burst_throttled() {
        let mut rl = PerSenderRateLimiter::new(2.0, 4.0, 256);
        let now = Instant::now();
        for _ in 0..4 {
            assert!(rl.check(&pk(1), now));
        }
        assert!(!rl.check(&pk(1), now));
    }

    #[test]
    fn refill_allows_more() {
        let mut rl = PerSenderRateLimiter::new(2.0, 4.0, 256);
        let now = Instant::now();
        for _ in 0..4 {
            assert!(rl.check(&pk(1), now));
        }
        assert!(!rl.check(&pk(1), now));

        let later = now + Duration::from_secs(30);
        assert!(rl.check(&pk(1), later));
    }
}
