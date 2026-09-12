use std::time::Duration;

use crate::LixError;

const MAX_EXPIRED_READ_RETRY_DURATION: Duration = Duration::from_secs(3);

/// Shared bounded policy for restarting one coherent read unit.
///
/// Callers own the unit that is safe to restart. This type owns only the
/// `LIX_STORAGE_READ_EXPIRED` classification, elapsed-time budget, and bounded
/// backoff so repository open and SQL execution cannot drift into separate
/// retry policies.
#[derive(Default)]
pub(crate) struct ExpiredReadRetryState {
    started_at: Option<web_time::Instant>,
    attempts: usize,
}

impl ExpiredReadRetryState {
    pub(crate) fn next_delay(&mut self, error: &LixError) -> Option<Duration> {
        if error.automatic_retry_is_forbidden() || error.code != LixError::CODE_STORAGE_READ_EXPIRED
        {
            return None;
        }
        let now = web_time::Instant::now();
        let started_at = self.started_at.get_or_insert(now);
        if now.duration_since(*started_at) >= MAX_EXPIRED_READ_RETRY_DURATION {
            return None;
        }
        self.attempts += 1;
        let exponent = self.attempts.saturating_sub(2).min(4) as u32;
        Some(if self.attempts > 1 {
            Duration::from_millis(1_u64 << exponent)
        } else {
            Duration::ZERO
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completed_execution_does_not_consume_expired_read_retry_budget() {
        for marker in ["nonRetryableAfterCommit", "nonRetryableAfterExecution"] {
            let mut state = ExpiredReadRetryState::default();
            let error = LixError::new(LixError::CODE_STORAGE_READ_EXPIRED, "completion failed")
                .with_details(serde_json::json!({marker: true}));
            assert_eq!(state.next_delay(&error), None);
            assert!(state.started_at.is_none());
            assert_eq!(state.attempts, 0);
            let ordinary = LixError::new(LixError::CODE_STORAGE_READ_EXPIRED, "read expired");
            assert_eq!(state.next_delay(&ordinary), Some(Duration::ZERO));
        }
    }
}
