//! EXPECT_COMPILE_FAIL: the tracked-state reader and reader-only facade are deleted.
//!
//! Dormant negative probe; intentionally not registered or compiled here.

extern crate lix;

use lix::tracked_state::{TrackedStateDiff, TrackedStateStoreReader};

fn main() {
    let _ = (TrackedStateDiff, std::mem::size_of::<TrackedStateStoreReader<()>>());
}
