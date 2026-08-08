//! EXPECT_COMPILE_FAIL: legacy tracked-state spaces are deleted.
//!
//! Dormant negative probe; intentionally not registered or compiled here.

extern crate lix;

use lix::tracked_state::TRACKED_STATE_TREE_CHUNK_SPACE;

fn main() {
    let _ = TRACKED_STATE_TREE_CHUNK_SPACE;
}
