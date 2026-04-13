use crate::contracts::LixFunctionProvider;

use super::SystemFunctionProvider;

const DETERMINISTIC_UUID_COUNTER_MASK: u64 = 0x0000_FFFF_FFFF_FFFF;

#[derive(Debug, Clone)]
pub(crate) struct RuntimeFunctionProvider {
    enabled: bool,
    uuid_v7_enabled: bool,
    timestamp_enabled: bool,
    timestamp_shuffle_enabled: bool,
    sequence_start: Option<i64>,
    next_sequence: i64,
}

impl RuntimeFunctionProvider {
    pub(crate) fn new(
        enabled: bool,
        uuid_v7_enabled: bool,
        timestamp_enabled: bool,
        timestamp_shuffle_enabled: bool,
        sequence_start: Option<i64>,
    ) -> Self {
        let next_sequence = sequence_start.unwrap_or(0);
        Self {
            enabled,
            uuid_v7_enabled,
            timestamp_enabled,
            timestamp_shuffle_enabled,
            sequence_start,
            next_sequence,
        }
    }

    fn take_sequence(&mut self) -> i64 {
        assert!(
            !self.enabled || self.sequence_start.is_some(),
            "deterministic runtime sequence used before initialization"
        );
        let current = self.next_sequence;
        self.next_sequence += 1;
        current
    }
}

impl LixFunctionProvider for RuntimeFunctionProvider {
    fn uuid_v7(&mut self) -> String {
        if self.enabled && self.uuid_v7_enabled {
            let counter = self.take_sequence();
            let counter_bits = (counter as u64) & DETERMINISTIC_UUID_COUNTER_MASK;
            return format!("01920000-0000-7000-8000-{counter_bits:012x}");
        }
        let mut system = SystemFunctionProvider;
        system.uuid_v7()
    }

    fn timestamp(&mut self) -> String {
        if self.enabled && self.timestamp_enabled {
            let counter = self.take_sequence();
            let millis = if self.timestamp_shuffle_enabled {
                shuffled_timestamp_millis(counter)
            } else {
                counter
            };
            let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(millis)
                .unwrap_or(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH);
            return dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        }
        let mut system = SystemFunctionProvider;
        system.timestamp()
    }

    fn deterministic_sequence_enabled(&self) -> bool {
        self.enabled
    }

    fn deterministic_sequence_initialized(&self) -> bool {
        !self.enabled || self.sequence_start.is_some()
    }

    fn initialize_deterministic_sequence(&mut self, sequence_start: i64) {
        if !self.enabled || self.sequence_start.is_some() {
            return;
        }
        self.sequence_start = Some(sequence_start);
        self.next_sequence = sequence_start;
    }

    fn deterministic_sequence_persist_highest_seen(&self) -> Option<i64> {
        let sequence_start = self.sequence_start?;
        if !self.enabled || self.next_sequence <= sequence_start {
            return None;
        }
        Some(self.next_sequence - 1)
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
