//! EXPECT_COMPILE_FAIL: the legacy control space must not resolve.
//!
//! This file is a dormant negative probe. It is intentionally not registered
//! as a Lix test and is never compiled on the current non-runnable frontier.

extern crate lix;

use lix::branch::BRANCH_HEAD_CONTROL_SPACE;

fn main() {
    let _ = BRANCH_HEAD_CONTROL_SPACE;
}
