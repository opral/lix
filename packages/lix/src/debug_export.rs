//! One-off investigation tooling: copies a live repository out of any
//! storage backend into the deterministic [`Memory`] snapshot format used by
//! golden fixtures, so a production repository can be replayed inside the
//! test harnesses byte-for-byte.
//!
//! The Storage trait deals in opaque `(space, key, value)` bytes, so a raw
//! per-space copy is adapter-agnostic: what SlateDB serves through a coherent
//! read view is exactly what `Memory` will serve after the import.

use std::collections::BTreeMap;
use std::ops::Bound;

use bytes::Bytes;

use crate::storage::{
    BeginScanOptions, Key, KeyRange, Memory, ProjectedValue, ReadOptions, Storage, StorageError,
    StorageRead as _,
};

/// Exports every physical entry of `storage` as a deterministic snapshot
/// decodable by [`Memory::from_snapshot`].
pub async fn export_storage_snapshot<S: Storage>(storage: &S) -> Result<Vec<u8>, StorageError> {
    let read = storage.begin_read(ReadOptions::default()).await?;
    let mut entries = BTreeMap::<Key, Bytes>::new();
    for space in crate::storage_spaces::ALL_STORAGE_SPACES {
        let mut cursor = read
            .begin_scan(
                *space,
                KeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                BeginScanOptions::default(),
            )
            .await?;
        for entry in cursor.collect_all().await? {
            let ProjectedValue::FullValue(value) = entry.value else {
                return Err(StorageError::Io(format!(
                    "space '{}' returned a key-only row under a full-value scan",
                    space.name
                )));
            };
            let mut physical = Vec::with_capacity(4 + entry.key.0.len());
            physical.extend_from_slice(&space.id.0.to_be_bytes());
            physical.extend_from_slice(&entry.key.0);
            entries.insert(Key(Bytes::from(physical)), value);
        }
    }
    Memory::from_physical_entries(entries).export_snapshot()
}
