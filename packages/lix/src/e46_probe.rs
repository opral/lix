//! E46 engagement counters for the commit-state manifests phase.
//!
//! Inert unless `LIX_E46_PROBE` is set. Temporary instrumentation; removed
//! before the PR. Answers one question: on the lane we actually measure, does
//! the encode→decode round trip happen at all, and would a
//! `selected_source_commit_id.is_none()` fast path engage?

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) const LABELS: [&str; 8] = [
    "scoped_ranges_entered",
    "selected_source_none",
    "ret_columnar",
    "ret_touched_empty",
    "ret_replacement_generation",
    "decode_reached",
    "touched_scopes_call_a",
    "touched_scopes_call_b",
];

static COUNTS: [AtomicU64; 8] = [const { AtomicU64::new(0) }; 8];
static ENABLED: OnceLock<bool> = OnceLock::new();
static EVERY: OnceLock<u64> = OnceLock::new();

pub(crate) fn enabled() -> bool {
    *ENABLED.get_or_init(|| std::env::var_os("LIX_E46_PROBE").is_some())
}

pub(crate) fn bump(index: usize) {
    if enabled() {
        COUNTS[index].fetch_add(1, Ordering::Relaxed);
    }
}

/// Bump the entry counter and print a census line every `LIX_E46_PROBE_EVERY`
/// entries.
pub(crate) fn enter() {
    if !enabled() {
        return;
    }
    let entered = COUNTS[0].fetch_add(1, Ordering::Relaxed) + 1;
    let every = *EVERY.get_or_init(|| {
        std::env::var("LIX_E46_PROBE_EVERY")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(1000)
    });
    if entered % every != 0 {
        return;
    }
    let mut line = String::from("E46_ENGAGEMENT");
    for (index, label) in LABELS.iter().enumerate() {
        line.push_str(&format!(" {label}={}", COUNTS[index].load(Ordering::Relaxed)));
    }
    eprintln!("{line}");
}
