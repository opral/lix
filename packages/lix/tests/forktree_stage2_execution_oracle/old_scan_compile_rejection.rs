//! Compile-fail probe for the deleted page-reconstruction scan API.
//!
//! This compiles on pinned main b5e. It must fail on the cursor PR and every
//! runnable Stage-2 candidate: no alias or compatibility wrapper is allowed.

extern crate lix;

use lix::storage::{CoreProjection, KeyRange, ScanOptions, StorageRead, StorageSpace};

#[allow(dead_code)]
fn reconstruct_page<R: StorageRead>(read: &R, space: StorageSpace, range: KeyRange) {
    drop(read.scan(
        space,
        range,
        ScanOptions {
            projection: CoreProjection::FullValue,
            limit_rows: 64,
            resume_after: None,
        },
    ));
}

fn main() {}
