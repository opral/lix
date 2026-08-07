//! Compile-pass probe for the sole storage scan entry point.

extern crate lix;

use std::ops::Bound;

use lix::storage::{
    BeginScanOptions, CoreProjection, Key, KeyRange, ScanOrder, StorageError, StorageRead,
    StorageSpace,
};

pub async fn direct_streaming_range<R: StorageRead>(
    read: &R,
    space: StorageSpace,
    lower: Key,
    upper: Key,
) -> Result<(), StorageError> {
    let mut cursor = read
        .begin_scan(
            space,
            KeyRange {
                lower: Bound::Included(lower),
                upper: Bound::Excluded(upper),
            },
            BeginScanOptions {
                projection: CoreProjection::FullValue,
                order: ScanOrder::Ascending,
            },
        )
        .await?;
    let _page = cursor.next_page(64).await?;
    Ok(())
}

fn main() {}
