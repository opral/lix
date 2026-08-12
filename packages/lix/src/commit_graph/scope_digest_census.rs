//! Always-on census for the per-commit touched-scope digest.
//!
//! # Why this is not behind a bench feature
//!
//! The digest has a silent failure mode. A repository whose commits carry no
//! digest is still *correct* — the reader falls back to loading each reached
//! commit's replay-state authority, exactly as it did before — but it is
//! silently slow, and the whole optimization evaporates with nothing to say
//! so. An operator upgrading a repository needs to be able to tell a fully
//! digested repository from a fully pre-digest one, so these counters compile
//! in every configuration and are emitted per history read.
//!
//! The counter lives **inside** the new route (the membership test itself),
//! not at the traversal layer above it, so "no effect" from the timing
//! instrument can always be told apart from "the new code never ran".
//!
//! Cost is one relaxed atomic add per reached commit on a path that otherwise
//! performs a storage point read, so it is not measurable.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::changelog::CommitId;

/// Outcome of one commit's membership test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScopeDigestOutcome {
    /// The digest proved this commit has no member in any requested scope, so
    /// its replay-state authority was never loaded. This is the win.
    Pruned,
    /// The digest is exact but could not prove absence: the commit really does
    /// (or, on a filter collision, may) carry a member in a requested scope.
    LoadedPresent,
    /// The commit's member scopes were not enumerable when it was written.
    LoadedOpaque,
    /// The commit carries no digest at all. **This is the silent-slowness
    /// counter**: a repository written before this format reports every reached
    /// commit here.
    LoadedAbsent,
    /// The request placed no schema constraint, so no digest could apply.
    Unconstrained,
}

static PRUNED: AtomicU64 = AtomicU64::new(0);
static LOADED_PRESENT: AtomicU64 = AtomicU64::new(0);
static LOADED_OPAQUE: AtomicU64 = AtomicU64::new(0);
static LOADED_ABSENT: AtomicU64 = AtomicU64::new(0);
static UNCONSTRAINED: AtomicU64 = AtomicU64::new(0);

/// Per-projection breakdown, test-only.
///
/// The aggregate counters answer "is the digest working". They cannot answer
/// the sharper question this optimization actually has to pass: history-by-path
/// is **four** independent traversals — what the path pointed at, what blob it
/// held, which directories moved it, which plugin rendered it — each with its
/// own schema-key set, and an artifact that only serves one of them buys a
/// quarter of the win while looking identical in a benchmark. Keying the
/// breakdown by schema-key set makes that visible per projection.
///
/// A `Mutex<HashMap>` has no business on a per-commit path, so this is compiled
/// only for tests; the aggregate counters above are the shipping instrument.
#[cfg(test)]
pub(crate) mod by_projection {
    use super::ScopeDigestOutcome;
    use std::collections::BTreeMap;
    use std::sync::{Mutex, OnceLock};

    type Breakdown = BTreeMap<String, BTreeMap<&'static str, u64>>;

    fn table() -> &'static Mutex<Breakdown> {
        static TABLE: OnceLock<Mutex<Breakdown>> = OnceLock::new();
        TABLE.get_or_init(|| Mutex::new(BTreeMap::new()))
    }

    pub(crate) fn record(schema_keys: &[String], outcome: ScopeDigestOutcome) {
        let label = if schema_keys.is_empty() {
            "<unconstrained>".to_string()
        } else {
            schema_keys.join("+")
        };
        let bucket = match outcome {
            ScopeDigestOutcome::Pruned => "pruned",
            ScopeDigestOutcome::LoadedPresent => "loaded_present",
            ScopeDigestOutcome::LoadedOpaque => "loaded_opaque",
            ScopeDigestOutcome::LoadedAbsent => "loaded_absent",
            ScopeDigestOutcome::Unconstrained => "unconstrained",
        };
        let mut table = table().lock().expect("projection census is not poisoned");
        *table.entry(label).or_default().entry(bucket).or_insert(0) += 1;
    }

    pub(crate) fn take() -> Breakdown {
        std::mem::take(&mut *table().lock().expect("projection census is not poisoned"))
    }
}

pub(crate) fn record_scope_digest_outcome(outcome: ScopeDigestOutcome) {
    let counter = match outcome {
        ScopeDigestOutcome::Pruned => &PRUNED,
        ScopeDigestOutcome::LoadedPresent => &LOADED_PRESENT,
        ScopeDigestOutcome::LoadedOpaque => &LOADED_OPAQUE,
        ScopeDigestOutcome::LoadedAbsent => &LOADED_ABSENT,
        ScopeDigestOutcome::Unconstrained => &UNCONSTRAINED,
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ScopeDigestCensus {
    pub(crate) pruned: u64,
    pub(crate) loaded_present: u64,
    pub(crate) loaded_opaque: u64,
    pub(crate) loaded_absent: u64,
    pub(crate) unconstrained: u64,
}

/// Reads the process-wide census.
pub(crate) fn scope_digest_census() -> ScopeDigestCensus {
    ScopeDigestCensus {
        pruned: PRUNED.load(Ordering::Relaxed),
        loaded_present: LOADED_PRESENT.load(Ordering::Relaxed),
        loaded_opaque: LOADED_OPAQUE.load(Ordering::Relaxed),
        loaded_absent: LOADED_ABSENT.load(Ordering::Relaxed),
        unconstrained: UNCONSTRAINED.load(Ordering::Relaxed),
    }
}

impl ScopeDigestCensus {
    /// Commits whose membership test consulted a digest, whatever the answer.
    pub(crate) fn probed(&self) -> u64 {
        self.pruned + self.loaded_present + self.loaded_opaque + self.loaded_absent
    }

    pub(crate) fn since(&self, earlier: &Self) -> Self {
        Self {
            pruned: self.pruned.saturating_sub(earlier.pruned),
            loaded_present: self.loaded_present.saturating_sub(earlier.loaded_present),
            loaded_opaque: self.loaded_opaque.saturating_sub(earlier.loaded_opaque),
            loaded_absent: self.loaded_absent.saturating_sub(earlier.loaded_absent),
            unconstrained: self.unconstrained.saturating_sub(earlier.unconstrained),
        }
    }

    /// Emits one line per history read.
    ///
    /// `loaded_absent` dominating this line is what a pre-digest repository
    /// looks like from the outside: the read stays correct and stays slow.
    pub(crate) fn emit(&self, start_commit_id: &CommitId) {
        if self.probed() == 0 && self.unconstrained == 0 {
            return;
        }
        tracing::debug!(
            target: "lix_perf",
            start_commit_id = %start_commit_id,
            pruned = self.pruned,
            loaded_present = self.loaded_present,
            loaded_opaque = self.loaded_opaque,
            loaded_absent = self.loaded_absent,
            unconstrained = self.unconstrained,
            "lix.commit_graph.touched_scope_digest",
        );
    }
}
