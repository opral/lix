// EXPECT_COMPILE_FAIL: the old tracked-state physical namespace must be gone.
#![allow(dead_code)]

use lix::tracked_state::TRACKED_STATE_TREE_CHUNK_SPACE;

fn main() {
    let _ = TRACKED_STATE_TREE_CHUNK_SPACE;
}
