//! Compile-fail probe for persisted or caller-owned adapter continuation state.

extern crate lix;

use lix::storage::{BeginScanOptions, CoreProjection, Key, ScanOrder};

#[allow(dead_code)]
fn smuggle_resume_state(key: Key) -> BeginScanOptions {
    BeginScanOptions {
        projection: CoreProjection::FullValue,
        order: ScanOrder::Ascending,
        resume_after: Some(key),
    }
}

fn main() {}
