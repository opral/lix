//! Intentional compile-fail consumer.
//!
//! Never wire this into the normal workspace test graph. A future candidate
//! runner compiles it against the candidate crate and requires a non-zero
//! result naming an unresolved old module, type, or space.

use lix::live_state::tracked_head::TrackedHeadContext;
use lix::live_state::{
    TrackedHeadContext as ReexportedTrackedHeadContext,
    TRACKED_WORKING_DIFF_MARKER_SPACE,
};

fn main() {
    let _ = TrackedHeadContext::new();
    let _ = ReexportedTrackedHeadContext::new();
    let _ = TRACKED_WORKING_DIFF_MARKER_SPACE;
}
