
//! **DELIBERATELY INCORRECT ENGINE — MEASUREMENT ONLY.**
//!
//! Experiment E1. Behind the `floor-unreadable` cargo feature *and* the
//! `LIX_FLOOR_UNREADABLE` environment variable. When both are on, the commit
//! path skips the derived staging phases (`stage_tracked_head`,
//! `stage_tracked_roots`, `stage_commit_state_manifests`) and writes only the
//! durable minimum. **The resulting repository cannot be read back.** This
//! exists to measure a floor, never to ship.
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

pub const PHASE_TRACKED_HEAD: usize = 0;
pub const PHASE_TRACKED_ROOTS: usize = 1;
pub const PHASE_COMMIT_STATE_MANIFESTS: usize = 2;
pub const PHASE_COUNT: usize = 3;
pub const PHASE_NAMES: [&str; PHASE_COUNT] = [
    "stage_tracked_head",
    "stage_tracked_roots",
    "stage_commit_state_manifests",
];

#[allow(clippy::declare_interior_mutable_const)]
const ZERO: AtomicU64 = AtomicU64::new(0);
static SKIPS: [AtomicU64; PHASE_COUNT] = [ZERO; PHASE_COUNT];
static BODIES: [AtomicU64; PHASE_COUNT] = [ZERO; PHASE_COUNT];

static ARMED: [std::sync::atomic::AtomicBool; PHASE_COUNT] = [
    std::sync::atomic::AtomicBool::new(false),
    std::sync::atomic::AtomicBool::new(false),
    std::sync::atomic::AtomicBool::new(false),
];

/// Arms the floor engine. Only ever called by the E1 measurement harness,
/// after the fixture has been seeded by the real engine. Requires
/// `LIX_FLOOR_UNREADABLE` in the environment so a stray call cannot arm it.
pub fn arm() -> bool {
    let Ok(spec) = std::env::var("LIX_FLOOR_UNREADABLE") else {
        eprintln!("FLOOR-ARM spec=none");
        return false;
    };
    let mut any = false;
    for phase in 0..PHASE_COUNT {
        let on = spec == "all" || spec.split(',').any(|token| token == PHASE_NAMES[phase]);
        ARMED[phase].store(on, Ordering::SeqCst);
        any |= on;
    }
    eprintln!(
        "FLOOR-ARM spec={spec} head={} roots={} manifests={}",
        ARMED[0].load(Ordering::SeqCst),
        ARMED[1].load(Ordering::SeqCst),
        ARMED[2].load(Ordering::SeqCst)
    );
    any
}

/// True when the given phase is armed for skipping in this process.
#[inline]
pub fn skip_derived(phase: usize) -> bool {
    ARMED[phase].load(Ordering::Relaxed)
}

/// Counted inside the skip branch of each phase.
pub fn note_skip(phase: usize) {
    let n = SKIPS[phase].fetch_add(1, Ordering::Relaxed) + 1;
    if phase == PHASE_TRACKED_HEAD && n % 1000 == 0 {
        eprintln!("{}", engagement_report());
    }
}

/// Counted at the first statement of each phase's *real* body. Must stay at
/// zero for every armed phase, or the phase was not actually skipped.
pub fn note_body(phase: usize) {
    let n = BODIES[phase].fetch_add(1, Ordering::Relaxed) + 1;
    if skip_derived(phase) && n <= 3 {
        eprintln!(
            "FLOOR-VIOLATION phase={} body_ran={}",
            PHASE_NAMES[phase], n
        );
    }
}

pub fn engagement_report() -> String {
    let mut out = String::from("FLOOR-ENGAGEMENT");
    for phase in 0..PHASE_COUNT {
        out.push_str(&format!(
            " {}:skip={},body={}",
            PHASE_NAMES[phase],
            SKIPS[phase].load(Ordering::Relaxed),
            BODIES[phase].load(Ordering::Relaxed),
        ));
    }
    out
}
