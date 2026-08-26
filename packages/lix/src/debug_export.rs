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

/// Restores a repository from a snapshot produced by
/// [`export_storage_snapshot`], replacing the target's contents wholesale.
///
/// Destructive by design: every space is cleared and refilled from the
/// snapshot in one atomic write. The target must not be open anywhere.
pub async fn import_storage_snapshot<S: Storage>(
    storage: &S,
    snapshot: &[u8],
) -> Result<(), StorageError> {
    use crate::storage_adapter::{
        PutBatch, PutEntry, StorageValue, StorageWrite as _, StorageWriteOptions, ValueIntegrity,
        ValueSemantics,
    };

    let source = Memory::from_snapshot(snapshot)?;
    let source_read = source.begin_read(StorageReadOptions::default()).await?;
    let mut write = storage
        .begin_write(StorageWriteOptions {
            await_durable: true,
            ..StorageWriteOptions::default()
        })
        .await?;
    for space in crate::storage_spaces::ALL_STORAGE_SPACES {
        write
            .delete_range(
                *space,
                StorageKeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
            )
            .await?;
        let mut cursor = source_read
            .begin_scan(
                *space,
                StorageKeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                StorageBeginScanOptions::default(),
            )
            .await?;
        let mut entries = Vec::new();
        for entry in cursor.collect_all().await? {
            let StorageProjectedValue::FullValue(value) = entry.value else {
                return Err(StorageError::Io(format!(
                    "space '{}' returned a key-only row under a full-value scan",
                    space.name
                )));
            };
            entries.push(PutEntry {
                key: entry.key,
                value: StorageValue { bytes: value },
            });
        }
        if entries.is_empty() {
            continue;
        }
        let batch = PutBatch { entries };
        match (space.value_semantics, space.value_integrity) {
            // Content-addressed keys are digests of their own bytes, so a
            // restore re-puts byte-identical values; replace_many rejects
            // those spaces by design.
            (ValueSemantics::Immutable, ValueIntegrity::BackendVerified) => {
                write.replace_many(*space, batch).await?
            }
            _ => write.put_many(*space, batch).await?,
        }
    }
    write.commit().await?;
    Ok(())
}
