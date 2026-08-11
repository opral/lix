//! Write-once interning of hot-plane schema keys.
//!
//! Every hot-plane physical key embeds the row's schema key. An escaped
//! schema string repeats tens of bytes per row for a handful of distinct
//! values per repository, so the serving plane pays string-width key
//! comparisons, index blocks, and cache traffic for a few bits of
//! information. This module assigns each schema key one immutable `u32` and
//! persists the mapping in one tiny append-only space; hot keys carry the
//! fixed-width id instead of the string.
//!
//! Single authority: the persisted `id -> schema_key` row is the only durable
//! form of the mapping. Ids are sequence-assigned, never renumbered, and a
//! mapping row is staged into the same write set as the first hot key that
//! uses its id, so no crash can publish a hot key whose id the table does not
//! explain. The in-memory table is a superset cache of the persisted space:
//! it is loaded once at engine open and extended at assignment time, before
//! any reader can observe a hot key carrying the new id. A persisted hot key
//! whose id misses the table is therefore corruption and fails closed.

use std::collections::HashMap;
use std::sync::RwLock;

use bytes::Bytes;

use crate::LixError;
use crate::common::SharedStr;
use crate::storage::{
    BeginScanOptions, CoreProjection, GetManyRequest, GetOptions, Key, KeyRange,
    MAX_SCAN_PAGE_ROWS, ProjectedValue, SpaceId, StorageError, StorageSpace,
};
use crate::storage_adapter::read_scope::StorageAdapterRead;
use crate::storage_adapter::write_set::StorageWriteSet;

/// Append-only `u32 -> schema_key` rows. Key: 4-byte BE id. Value: the raw
/// UTF-8 schema key.
pub(crate) const SCHEMA_INTERN_SPACE: StorageSpace =
    StorageSpace::mutable(SpaceId(0x0004_0033), "live_state.schema_intern.v1");

/// Fixed-width encoded schema id inside hot-plane physical keys.
pub(crate) const SCHEMA_INTERN_ID_BYTES: usize = 4;

/// One immutable interned schema id.
///
/// Ids order by allocation sequence, not by schema-key lexical order, so the
/// physical hot-plane order across schemas is allocation order. Logical
/// (SQL-visible) ordering stays string-based and is restored by the scan
/// canonicalization layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SchemaInternId(pub(crate) u32);

impl SchemaInternId {
    /// Reserved id that `assign` never allocates. Encoding a point-read key
    /// with it is the canonical way to probe for a schema that has no
    /// interned id: no persisted key can carry it, so the read misses.
    pub(crate) const UNMAPPED: Self = Self(u32::MAX);

    pub(crate) fn to_be_bytes(self) -> [u8; SCHEMA_INTERN_ID_BYTES] {
        self.0.to_be_bytes()
    }
}

#[derive(Default)]
struct SchemaInternInner {
    /// `id as usize -> schema_key`; index is the id.
    by_id: Vec<SharedStr>,
    by_name: HashMap<SharedStr, u32>,
    /// Ids below this bound were loaded from storage and are durable. Ids at
    /// or above it were assigned by this process and re-stage their mapping
    /// row into every write set that uses them until the engine reopens.
    loaded_len: u32,
}

/// Engine-lifetime schema-key interning table for the hot serving plane.
#[derive(Default)]
pub struct SchemaIntern {
    inner: RwLock<SchemaInternInner>,
    /// Set once the whole space has been scanned; before that a miss cannot
    /// be distinguished from an unloaded table.
    scanned: std::sync::atomic::AtomicBool,
}

impl std::fmt::Debug for SchemaIntern {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.read().expect("schema intern lock poisoned");
        formatter
            .debug_struct("SchemaIntern")
            .field("len", &inner.by_id.len())
            .field("loaded_len", &inner.loaded_len)
            .finish()
    }
}

impl SchemaIntern {
    const UNMAPPED_RAW: u32 = u32::MAX;
    /// Ids probed per round when catching up with another engine's writes.
    const TAIL_PROBE_WIDTH: u32 = 8;

    /// Brings the table up to date with `store`'s snapshot.
    ///
    /// The first call scans the (tiny) space once. Afterwards the table is a
    /// dense `0..len` prefix, so catching up only has to probe the ids past
    /// `len`: one batched point read that misses in the steady state, no
    /// iterator setup and no per-operation scan. Callers must invoke this at
    /// the async boundary before any synchronous `resolve`/`name` on the same
    /// snapshot.
    pub(crate) async fn ensure_current<S>(&self, store: &S) -> Result<(), StorageError>
    where
        S: StorageAdapterRead + ?Sized,
    {
        if !self.scanned.load(std::sync::atomic::Ordering::Acquire) {
            self.load(store).await?;
            self.scanned
                .store(true, std::sync::atomic::Ordering::Release);
            return Ok(());
        }
        self.probe_tail(store).await
    }

    /// Reconciles the unconfirmed tail of the table with storage.
    ///
    /// Ids below `loaded_len` are already known durable. From there upward the
    /// probe point-reads the dense id sequence and:
    ///   * confirms an id this process assigned once storage agrees,
    ///   * adopts an id another engine published,
    ///   * stops at the first absent id and truncates any in-memory
    ///     assignments past it — those belong to a write set that never
    ///     committed, so nothing durable can reference them.
    /// In the steady state this is one batched point read that misses.
    async fn probe_tail<S>(&self, store: &S) -> Result<(), StorageError>
    where
        S: StorageAdapterRead + ?Sized,
    {
        loop {
            let start = {
                let inner = self.inner.read().expect("schema intern lock poisoned");
                inner.loaded_len
            };
            let keys = (start..start.saturating_add(Self::TAIL_PROBE_WIDTH))
                .map(|id| Key(Bytes::copy_from_slice(&id.to_be_bytes())))
                .collect::<Vec<_>>();
            let result = crate::storage_adapter::exact_get_many(
                store,
                &[GetManyRequest {
                    space: SCHEMA_INTERN_SPACE,
                    keys: &keys,
                    opts: GetOptions {
                        projection: CoreProjection::FullValue,
                    },
                }],
            )
            .await?;
            let mut inner = self.inner.write().expect("schema intern lock poisoned");
            if inner.loaded_len != start {
                // Another task advanced the tail while this probe was in
                // flight; re-read the bound instead of merging stale offsets.
                continue;
            }
            let mut confirmed = 0_u32;
            let mut exhausted = false;
            for (offset, value) in result.values.into_iter().enumerate() {
                let id = start.saturating_add(offset as u32);
                let Some(value) = value else {
                    exhausted = true;
                    break;
                };
                let ProjectedValue::FullValue(bytes) = value else {
                    return Err(intern_corruption("value projection"));
                };
                let name = SharedStr::from_utf8(bytes)
                    .map_err(|_| intern_corruption("schema key utf-8"))?;
                match inner.by_id.get(id as usize) {
                    Some(existing) if *existing == name => {}
                    Some(_) => {
                        // Storage is the authority: a concurrent writer won
                        // this id. Drop this process's unconfirmed tail so the
                        // rejected assignments are re-made against the
                        // published sequence.
                        let keep = id as usize;
                        for stale in inner.by_id.split_off(keep) {
                            inner.by_name.remove(&stale);
                        }
                        inner.by_name.insert(name.clone(), id);
                        inner.by_id.push(name);
                    }
                    None => {
                        if inner.by_name.insert(name.clone(), id).is_some() {
                            return Err(intern_corruption("duplicate schema key"));
                        }
                        inner.by_id.push(name);
                    }
                }
                confirmed += 1;
            }
            inner.loaded_len = start.saturating_add(confirmed);
            if exhausted {
                // Ids past the durable end are this process's own pending
                // assignments, staged in a write set that has not committed
                // yet. They are kept: nothing durable references them, and
                // their commit carries a key-absent precondition, so a racing
                // publisher makes that commit retry rather than split the id.
                return Ok(());
            }
            if confirmed < Self::TAIL_PROBE_WIDTH {
                return Ok(());
            }
        }
    }

    /// Scans the whole persisted table. Used once per adapter; afterwards
    /// `ensure_current` catches up with bounded point reads instead.
    async fn load<S>(&self, store: &S) -> Result<(), StorageError>
    where
        S: StorageAdapterRead + ?Sized,
    {
        let mut rows: Vec<(u32, SharedStr)> = Vec::new();
        let mut cursor = store
            .begin_scan(
                SCHEMA_INTERN_SPACE,
                KeyRange {
                    lower: std::ops::Bound::Unbounded,
                    upper: std::ops::Bound::Unbounded,
                },
                BeginScanOptions::default(),
            )
            .await?;
        loop {
            let page = cursor.next_page(MAX_SCAN_PAGE_ROWS).await?;
            for entry in page.entries {
                let key = entry.key.0.as_ref();
                let id_bytes: [u8; SCHEMA_INTERN_ID_BYTES] =
                    key.try_into().map_err(|_| intern_corruption("key width"))?;
                let value = match entry.value {
                    ProjectedValue::FullValue(bytes) => bytes,
                    ProjectedValue::KeyOnly => {
                        return Err(intern_corruption("value projection"));
                    }
                };
                let name = SharedStr::from_utf8(value)
                    .map_err(|_| intern_corruption("schema key utf-8"))?;
                rows.push((u32::from_be_bytes(id_bytes), name));
            }
            if !page.has_more {
                break;
            }
        }
        let mut inner = self.inner.write().expect("schema intern lock poisoned");
        // Storage is the authority. Persisted rows are adopted as-is;
        // unconfirmed in-memory assignments that disagree belong to a write
        // set that never committed and are dropped, so this process re-assigns
        // them against the published sequence.
        for (index, (id, name)) in rows.iter().enumerate() {
            if *id as usize != index {
                return Err(intern_corruption("id sequence"));
            }
            match inner.by_id.get(index) {
                Some(existing) if existing == name => {}
                Some(_) => {
                    for stale in inner.by_id.split_off(index) {
                        inner.by_name.remove(&stale);
                    }
                    inner.by_name.insert(name.clone(), *id);
                    inner.by_id.push(name.clone());
                }
                None => {
                    // A schema key bound to two ids can only come from a
                    // writer that bypassed the write-once precondition. Keep
                    // the lowest id as the encode target; decode still maps
                    // every persisted id back to its own name.
                    inner.by_name.entry(name.clone()).or_insert(*id);
                    inner.by_id.push(name.clone());
                }
            }
        }
        inner.loaded_len = rows.len() as u32;
        // Ids past the persisted end stay: they belong to a write set this
        // process has staged but not yet committed (see `probe_tail`).
        Ok(())
    }

    /// Read-path lookup. `None` means no hot key visible to the resolving
    /// snapshot can carry this schema: the mapping row commits atomically with
    /// the first hot key that uses it, so once the table has been refreshed
    /// through a snapshot, an unmapped schema has no rows in that snapshot.
    pub(crate) fn resolve(&self, schema_key: &str) -> Option<SchemaInternId> {
        let inner = self.inner.read().expect("schema intern lock poisoned");
        inner.by_name.get(schema_key).copied().map(SchemaInternId)
    }

    /// Cheap variant of `ensure_current` for operations whose schema set is
    /// known up front: when every name already resolves, no storage read is
    /// issued at all. A miss means either the schema has no rows or another
    /// engine published it, and only then is the table caught up.
    pub(crate) async fn ensure_current_for<'a, S>(
        &self,
        store: &S,
        mut schema_keys: impl Iterator<Item = &'a str>,
    ) -> Result<(), StorageError>
    where
        S: StorageAdapterRead + ?Sized,
    {
        let all_known = {
            let inner = self.inner.read().expect("schema intern lock poisoned");
            self.scanned.load(std::sync::atomic::Ordering::Acquire)
                && schema_keys.all(|schema_key| inner.by_name.contains_key(schema_key))
        };
        if all_known {
            return Ok(());
        }
        self.ensure_current(store).await
    }

    /// Decode-path lookup; a miss is corruption because assignment always
    /// publishes the in-memory entry before any hot key with the id exists.
    pub(crate) fn name(&self, id: SchemaInternId) -> Result<SharedStr, LixError> {
        let inner = self.inner.read().expect("schema intern lock poisoned");
        inner.by_id.get(id.0 as usize).cloned().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "hot-plane physical key carries schema intern id {} outside the \
                     persisted intern table ({} entries)",
                    id.0,
                    inner.by_id.len()
                ),
            )
        })
    }

    /// Write-path assignment. Callers MUST have brought the table up to date
    /// with `ensure_current` on the same snapshot first: allocating against a
    /// stale table would hand a durable id to a second schema key.
    ///
    /// Returns the schema's id, allocating the next
    /// sequence id on first use, and stages the mapping row into `writes`
    /// whenever this process has not yet loaded it back from storage. The
    /// staged put is byte-identical on every repetition, so re-staging an
    /// already-durable row is a no-op overwrite.
    pub(crate) fn assign(
        &self,
        schema_key: &str,
        writes: &mut StorageWriteSet,
    ) -> Result<SchemaInternId, LixError> {
        {
            let inner = self.inner.read().expect("schema intern lock poisoned");
            if let Some(&id) = inner.by_name.get(schema_key) {
                if id < inner.loaded_len {
                    return Ok(SchemaInternId(id));
                }
                stage_mapping_row(writes, id, schema_key);
                return Ok(SchemaInternId(id));
            }
        }
        let mut inner = self.inner.write().expect("schema intern lock poisoned");
        if let Some(&id) = inner.by_name.get(schema_key) {
            if id >= inner.loaded_len {
                stage_mapping_row(writes, id, schema_key);
            }
            return Ok(SchemaInternId(id));
        }
        let id = u32::try_from(inner.by_id.len())
            .ok()
            .filter(|id| *id != Self::UNMAPPED_RAW)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "schema intern table exhausted the u32 id domain".to_string(),
                )
            })?;
        let name = SharedStr::from(schema_key.to_owned());
        inner.by_id.push(name.clone());
        inner.by_name.insert(name, id);
        stage_mapping_row(writes, id, schema_key);
        Ok(SchemaInternId(id))
    }
}

fn stage_mapping_row(writes: &mut StorageWriteSet, id: u32, schema_key: &str) {
    writes.put_write_once(
        SCHEMA_INTERN_SPACE,
        Key(Bytes::copy_from_slice(&id.to_be_bytes())),
        crate::storage::StoredValue {
            bytes: Bytes::copy_from_slice(schema_key.as_bytes()),
        },
    );
}

fn intern_corruption(what: &str) -> StorageError {
    StorageError::Corruption(format!("schema intern table row has invalid {what}").into())
}

/// Shared per-adapter intern state: the table plus its once-per-adapter load.
///
/// The load runs lazily on the adapter's first `begin_read`, through the same
/// snapshot the caller is about to use, so every read or write scope observes
/// a table at least as new as its own storage view.
#[derive(Debug, Default)]
pub struct SchemaInternHandle {
    intern: SchemaIntern,
}

impl SchemaInternHandle {
    pub(crate) fn intern(&self) -> &SchemaIntern {
        &self.intern
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn refresh_confirms_committed_ids_and_keeps_pending_ones() {
        use crate::storage::{Memory, WriteOptions};
        use crate::storage_adapter::StorageAdapter;

        let storage = StorageAdapter::new(Memory::new());
        let intern = SchemaIntern::default();

        let mut writes = StorageWriteSet::new();
        let committed = intern
            .assign("lix_key_value", &mut writes)
            .expect("assign committed id");
        storage
            .commit_write_set(writes, WriteOptions::default())
            .await
            .expect("publish mapping row");

        let mut pending = StorageWriteSet::new();
        let staged = intern
            .assign("markdown_block", &mut pending)
            .expect("assign pending id");

        let read = storage
            .begin_read(crate::storage::ReadOptions::default())
            .await
            .expect("open read");
        intern.ensure_current(&read).await.expect("refresh");

        // The committed id is confirmed, so it is not staged twice.
        let mut second = StorageWriteSet::new();
        assert_eq!(
            intern.assign("lix_key_value", &mut second).expect("assign"),
            committed
        );
        assert_eq!(second.stats().staged_puts, 0);
        // The uncommitted id survives the refresh and still re-stages.
        let mut third = StorageWriteSet::new();
        assert_eq!(
            intern.assign("markdown_block", &mut third).expect("assign"),
            staged
        );
        assert_eq!(third.stats().staged_puts, 1);
    }

    #[test]
    fn assignment_is_sequential_and_stable() {
        let intern = SchemaIntern::default();
        let mut writes = StorageWriteSet::default();
        let first = intern.assign("lix_key_value", &mut writes).expect("assign");
        let second = intern.assign("json_pointer", &mut writes).expect("assign");
        let repeat = intern.assign("lix_key_value", &mut writes).expect("assign");
        assert_eq!(first, SchemaInternId(0));
        assert_eq!(second, SchemaInternId(1));
        assert_eq!(repeat, first);
        assert_eq!(intern.resolve("json_pointer"), Some(SchemaInternId(1)));
        assert_eq!(intern.resolve("missing"), None);
        assert_eq!(
            intern.name(SchemaInternId(0)).expect("name").as_str(),
            "lix_key_value"
        );
        assert!(intern.name(SchemaInternId(9)).is_err());
    }
}
