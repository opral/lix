// EXPECT_COMPILE_FAIL: TrackedStateStoreReader and reader-only diff exports
// must be absent from the first compile-green ForkTree deletion candidate.
#![allow(dead_code)]

use lix::tracked_state::{TrackedStateDiff, TrackedStateStoreReader};

fn main() {
    let _: Option<TrackedStateStoreReader<()>> = None;
    let _: Option<TrackedStateDiff> = None;
}
