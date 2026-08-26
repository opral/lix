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

use crate::Memory;
use crate::storage_adapter::{
    Storage, StorageBeginScanOptions, StorageError, StorageKey, StorageKeyRange,
    StorageProjectedValue, StorageRead as _, StorageReadOptions,
};

/// Exports every physical entry of `storage` as a deterministic snapshot
/// decodable by [`Memory::from_snapshot`].
pub async fn export_storage_snapshot<S: Storage>(storage: &S) -> Result<Vec<u8>, StorageError> {
    let read = storage.begin_read(StorageReadOptions::default()).await?;
    let mut entries = BTreeMap::<StorageKey, Bytes>::new();
    for space in crate::storage_spaces::ALL_STORAGE_SPACES {
        let mut cursor = read
            .begin_scan(
                *space,
                StorageKeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                StorageBeginScanOptions::default(),
            )
            .await?;
        for entry in cursor.collect_all().await? {
            let StorageProjectedValue::FullValue(value) = entry.value else {
                return Err(StorageError::Io(format!(
                    "space '{}' returned a key-only row under a full-value scan",
                    space.name
                )));
            };
            let mut physical = Vec::with_capacity(4 + entry.key.0.len());
            physical.extend_from_slice(&space.id.0.to_be_bytes());
            physical.extend_from_slice(&entry.key.0);
            entries.insert(StorageKey(Bytes::from(physical)), value);
        }
    }
    Memory::from_physical_entries(entries).export_snapshot()
}
