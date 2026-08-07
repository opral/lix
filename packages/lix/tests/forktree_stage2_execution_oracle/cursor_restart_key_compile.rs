//! Compile-pass probe for crash/reopen restart from an authenticated key.
//!
//! The durable owner authenticates the saved key. Reopen creates a new
//! coherent read and a new ephemeral cursor whose lower bound excludes that
//! key; no adapter continuation or opaque cursor state is persisted.

extern crate lix;

use std::ops::Bound;

use lix::storage::{
    BeginScanOptions, Key, KeyRange, ScanCursor, StorageError, StorageRead, StorageSpace,
};

pub async fn restart_after_authenticated_key<'read, R: StorageRead>(
    read: &'read R,
    space: StorageSpace,
    authenticated_last_key: Key,
    upper: Bound<Key>,
) -> Result<ScanCursor<'read>, StorageError> {
    read.begin_scan(
        space,
        KeyRange {
            lower: Bound::Excluded(authenticated_last_key),
            upper,
        },
        BeginScanOptions::default(),
    )
    .await
}

fn main() {}
