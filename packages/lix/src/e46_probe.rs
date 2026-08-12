//! E46 phase probe for `Transaction::open`.
//!
//! Inert unless `LIX_OPEN_TX_PROBE` is set in the environment. Temporary
//! instrumentation; removed before the PR.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Phases 0..=8 are the disjoint top-level phases of `Transaction::open` and
/// sum to the whole call. Phases 9..=11 are nested inside phase 2
/// (`FunctionContext::prepare`) and are reported separately.
pub(crate) const PHASES: [&str; 12] = [
    "begin_read",
    "branch_selector",
    "fn_prepare",
    "runtime_boundary",
    "load_revisions",
    "catalog_sql",
    "catalog_tracked",
    "branch_heads",
    "tail",
    "kv_control",
    "kv_live_batch",
    "kv_uncached_total",
];
pub(crate) const TOP_LEVEL_PHASES: usize = 9;

static NANOS: [AtomicU64; 12] = [const { AtomicU64::new(0) }; 12];
static CALLS: [AtomicU64; 12] = [const { AtomicU64::new(0) }; 12];
static COUNT: AtomicU64 = AtomicU64::new(0);
static ENABLED: OnceLock<bool> = OnceLock::new();
static EVERY: OnceLock<u64> = OnceLock::new();

pub(crate) fn enabled() -> bool {
    *ENABLED.get_or_init(|| std::env::var_os("LIX_OPEN_TX_PROBE").is_some())
}

pub(crate) struct Mark(Option<Instant>);

pub(crate) fn mark() -> Mark {
    if enabled() {
        Mark(Some(Instant::now()))
    } else {
        Mark(None)
    }
}

impl Mark {
    pub(crate) fn lap(&mut self, phase: usize) {
        if let Some(previous) = self.0 {
            let now = Instant::now();
            NANOS[phase].fetch_add(
                now.duration_since(previous).as_nanos() as u64,
                Ordering::Relaxed,
            );
            CALLS[phase].fetch_add(1, Ordering::Relaxed);
            self.0 = Some(now);
        }
    }

    /// Restarts the mark without attributing the elapsed span to any phase.
    pub(crate) fn skip(&mut self) {
        if self.0.is_some() {
            self.0 = Some(Instant::now());
        }
    }
}

static LAST_CONTROL: AtomicU64 = AtomicU64::new(u64::MAX);
static CONTROL_SAME: AtomicU64 = AtomicU64::new(0);
static CONTROL_CHANGED: AtomicU64 = AtomicU64::new(0);

/// Records whether the global branch-head control token is byte-identical to
/// the one the previous observation saw. This is the achievable hit rate of a
/// control-fenced deterministic-mode cache.
pub(crate) fn note_control_token(bytes: Option<&[u8]>) {
    if !enabled() {
        return;
    }
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes.unwrap_or(&[]) {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    let previous = LAST_CONTROL.swap(hash, Ordering::Relaxed);
    if previous == hash {
        CONTROL_SAME.fetch_add(1, Ordering::Relaxed);
    } else {
        CONTROL_CHANGED.fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) fn finish() {
    if !enabled() {
        return;
    }
    let opens = COUNT.fetch_add(1, Ordering::Relaxed) + 1;
    let every = *EVERY.get_or_init(|| {
        std::env::var("LIX_OPEN_TX_PROBE_EVERY")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(2000)
    });
    if opens % every != 0 {
        return;
    }
    let mut line = format!("OPEN_TX_PROBE opens={opens}");
    let mut total = 0_f64;
    for (index, name) in PHASES.iter().enumerate() {
        let nanos = NANOS[index].load(Ordering::Relaxed) as f64 / opens as f64;
        let calls = CALLS[index].load(Ordering::Relaxed) as f64 / opens as f64;
        if index < TOP_LEVEL_PHASES {
            total += nanos;
        }
        line.push_str(&format!(" {name}={nanos:.0}/{calls:.2}"));
    }
    line.push_str(&format!(" TOTAL={total:.0}"));
    line.push_str(&format!(
        " control_same={} control_changed={}",
        CONTROL_SAME.load(Ordering::Relaxed),
        CONTROL_CHANGED.load(Ordering::Relaxed)
    ));
    eprintln!("{line}");
}
