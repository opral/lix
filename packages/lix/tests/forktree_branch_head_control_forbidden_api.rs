//! EXPECT_COMPILE_FAIL: the deleted BranchHeadControl API must not resolve.
//!
//! This file is a dormant negative probe. It is intentionally not registered
//! as a Lix test and is never compiled on the current non-runnable frontier.

extern crate lix;

use lix::branch::{BranchHeadControl, BranchHeadControlCache, BranchHeadControlContext};

fn main() {
    let _ = (BranchHeadControl, BranchHeadControlCache, BranchHeadControlContext);
}
