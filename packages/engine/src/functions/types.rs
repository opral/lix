/// Decoded deterministic-mode setting.
///
/// Storage can decide where this setting lives. The type only describes the
/// behavior engine should apply while preparing runtime functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DeterministicMode {
    pub(crate) enabled: bool,
    pub(crate) timestamp_shuffle: bool,
}

impl DeterministicMode {
    pub(crate) fn disabled() -> Self {
        Self {
            enabled: false,
            timestamp_shuffle: false,
        }
    }
}

/// Persisted deterministic sequence position.
///
/// `highest_seen` is the last sequence value returned by the runtime provider.
/// The next deterministic execution starts at `highest_seen + 1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DeterministicSequence {
    pub(crate) highest_seen: i64,
}

impl DeterministicSequence {
    pub(crate) fn uninitialized() -> Self {
        Self { highest_seen: -1 }
    }

    pub(crate) fn next_sequence(self) -> i64 {
        self.highest_seen + 1
    }
}
