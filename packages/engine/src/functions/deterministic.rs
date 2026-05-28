use crate::common::LixTimestamp;
use crate::functions::FunctionProvider;

const DETERMINISTIC_UUID_COUNTER_MASK: u64 = 0x0000_FFFF_FFFF_FFFF;

/// Deterministic function provider for engine execution.
///
/// The provider is pure runtime state: it does not load or persist the sequence
/// itself. Session/transaction code owns that boundary so tests can decide when
/// deterministic state is read and written.
#[derive(Debug, Clone)]
pub(crate) struct DeterministicFunctionProvider {
    next_sequence: i64,
    timestamp_shuffle: bool,
    highest_seen: Option<i64>,
}

impl DeterministicFunctionProvider {
    pub(crate) fn new(next_sequence: i64, timestamp_shuffle: bool) -> Self {
        Self {
            next_sequence,
            timestamp_shuffle,
            highest_seen: None,
        }
    }

    pub(crate) fn highest_seen(&self) -> Option<i64> {
        self.highest_seen
    }

    fn take_sequence(&mut self) -> i64 {
        let current = self.next_sequence;
        self.next_sequence += 1;
        self.highest_seen = Some(current);
        current
    }
}

impl FunctionProvider for DeterministicFunctionProvider {
    fn uuid_v7(&mut self) -> uuid::Uuid {
        let counter = self.take_sequence();
        let counter_bits = (counter as u64) & DETERMINISTIC_UUID_COUNTER_MASK;
        uuid::Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_0000 + counter_bits as u128)
    }

    fn timestamp(&mut self) -> LixTimestamp {
        let counter = self.take_sequence();
        let millis = if self.timestamp_shuffle {
            shuffled_timestamp_millis(counter)
        } else {
            counter
        };
        LixTimestamp::from_unix_millis_utc_lossy(millis)
    }

    fn deterministic_sequence_persist_highest_seen(&self) -> Option<i64> {
        self.highest_seen()
    }
}

fn shuffled_timestamp_millis(counter: i64) -> i64 {
    const WINDOW: i64 = 1000;
    const MULTIPLIER: i64 = 733;
    const OFFSET: i64 = 271;

    let cycle = counter.div_euclid(WINDOW);
    let within = counter.rem_euclid(WINDOW);
    let shuffled = (within * MULTIPLIER + OFFSET).rem_euclid(WINDOW);
    cycle * WINDOW + shuffled
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::functions::DeterministicSequence;

    #[test]
    fn deterministic_uuid_uses_sequence_counter() {
        let mut provider = DeterministicFunctionProvider::new(0, false);

        assert_eq!(
            provider.uuid_v7().to_string(),
            "01920000-0000-7000-8000-000000000000"
        );
        assert_eq!(
            provider.uuid_v7().to_string(),
            "01920000-0000-7000-8000-000000000001"
        );
        assert_eq!(provider.highest_seen(), Some(1));
    }

    #[test]
    fn deterministic_timestamp_uses_sequence_counter() {
        let mut provider = DeterministicFunctionProvider::new(1, false);

        assert_eq!(provider.timestamp().to_string(), "1970-01-01T00:00:00.001Z");
        assert_eq!(provider.highest_seen(), Some(1));
    }

    #[test]
    fn deterministic_timestamp_shuffle_can_be_non_monotonic() {
        let mut provider = DeterministicFunctionProvider::new(0, true);
        let first = provider.timestamp();
        let second = provider.timestamp();

        assert!(second < first);
        assert_eq!(provider.highest_seen(), Some(1));
    }

    #[test]
    fn deterministic_sequence_can_start_after_persisted_highest_seen() {
        let sequence = DeterministicSequence { highest_seen: 41 };
        let mut provider = DeterministicFunctionProvider::new(sequence.next_sequence(), false);

        assert_eq!(
            provider.uuid_v7().to_string(),
            "01920000-0000-7000-8000-00000000002a"
        );
        assert_eq!(provider.highest_seen(), Some(42));
    }
}
