//! Always-on census for the per-commit touched-scope digest.
//!
//! # Why this is not behind a bench feature
//!
//! The digest has a silent *degradation* mode that is not the one it first
//! appears to be, and the difference matters for what these counters are for.
//!
//! There is **no pre-digest repository to fall back for.** `CommitRecord` is
//! `#[musli(packed)]` — positional and untagged — so a commit written before
//! this format does not decode at all, and `init::REPOSITORY_PROTOCOL_VALUE`
//! rejects such a repository at open. `LoadedAbsent` is therefore not "an old
//! repository being read slowly"; it is a **writer that failed to derive a
//! digest**, which is a defect in this build, not a legacy state.
//!
//! The degradation that can really happen is `LoadedOpaque`: a commit whose
//! member scopes were not enumerable when it was written. That is correct and
//! slow, silently, and if a workload shape made it common the optimization
//! would quietly evaporate with nothing in the logs to say so.
//!
//! Both are counted, separately, in every configuration and emitted per history
//! read, so the two are distinguishable from the outside without a profiler.
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
    /// The commit carries no digest at all.
    ///
    /// Not a legacy state — a v66 repository is rejected at open, not read
    /// slowly — so a non-zero count here means a commit writer in *this* build
    /// failed to derive one. Treat it as a defect, not as degradation.
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
    #[cfg(test)]
    THREAD_CENSUS.with(|slot| {
        let mut census = slot.get();
        census.record(outcome);
        slot.set(census);
    });
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ScopeDigestCensus {
    pub(crate) pruned: u64,
    pub(crate) loaded_present: u64,
    pub(crate) loaded_opaque: u64,
    pub(crate) loaded_absent: u64,
    pub(crate) unconstrained: u64,
}

#[cfg(test)]
thread_local! {
    static THREAD_CENSUS: std::cell::Cell<ScopeDigestCensus> =
        const { std::cell::Cell::new(ScopeDigestCensus::empty()) };
}

#[cfg(test)]
pub(crate) fn reset_thread_scope_digest_census() {
    THREAD_CENSUS.with(|census| census.set(ScopeDigestCensus::empty()));
}

#[cfg(test)]
pub(crate) fn thread_scope_digest_census() -> ScopeDigestCensus {
    THREAD_CENSUS.with(std::cell::Cell::get)
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
    const fn empty() -> Self {
        Self {
            pruned: 0,
            loaded_present: 0,
            loaded_opaque: 0,
            loaded_absent: 0,
            unconstrained: 0,
        }
    }

    #[cfg(test)]
    fn record(&mut self, outcome: ScopeDigestOutcome) {
        let counter = match outcome {
            ScopeDigestOutcome::Pruned => &mut self.pruned,
            ScopeDigestOutcome::LoadedPresent => &mut self.loaded_present,
            ScopeDigestOutcome::LoadedOpaque => &mut self.loaded_opaque,
            ScopeDigestOutcome::LoadedAbsent => &mut self.loaded_absent,
            ScopeDigestOutcome::Unconstrained => &mut self.unconstrained,
        };
        *counter = counter.saturating_add(1);
    }

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
    /// `loaded_opaque` dominating this line is what a workload the digest
    /// cannot serve looks like from the outside: the read stays correct and
    /// stays slow. `loaded_absent` above zero is a writer defect.
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
