//! Intentional direct rustc compile-fail consumer for deleted TrackedHead API.

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
