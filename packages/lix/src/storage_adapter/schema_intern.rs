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
    BeginScanOptions, KeyRange, MAX_SCAN_PAGE_ROWS, ProjectedValue, SpaceId, StorageError,
    StorageSpace,
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
pub(crate) struct SchemaIntern {
    inner: RwLock<SchemaInternInner>,
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

    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Loads the persisted table once per adapter, replacing any prior
    /// in-memory state.
    pub(crate) async fn load<S>(&self, store: &S) -> Result<(), StorageError>
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
        let mut by_id: Vec<SharedStr> = Vec::with_capacity(rows.len());
        let mut by_name: HashMap<SharedStr, u32> = HashMap::with_capacity(rows.len());
        for (index, (id, name)) in rows.into_iter().enumerate() {
            if id as usize != index {
                return Err(intern_corruption("id sequence"));
            }
            if by_name.insert(name.clone(), id).is_some() {
                return Err(intern_corruption("duplicate schema key"));
            }
            by_id.push(name);
        }
        let mut inner = self.inner.write().expect("schema intern lock poisoned");
        inner.loaded_len = by_id.len() as u32;
        inner.by_id = by_id;
        inner.by_name = by_name;
        Ok(())
    }

    /// Read-path lookup. `None` means no hot key can carry this schema: the
    /// mapping row is staged with the first hot key that uses it, so an
    /// unmapped schema has no rows anywhere in the hot plane.
    pub(crate) fn resolve(&self, schema_key: &str) -> Option<SchemaInternId> {
        let inner = self.inner.read().expect("schema intern lock poisoned");
        inner.by_name.get(schema_key).copied().map(SchemaInternId)
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

    /// Write-path assignment. Returns the schema's id, allocating the next
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
    writes.put_if_absent(
        SCHEMA_INTERN_SPACE,
        Bytes::copy_from_slice(&id.to_be_bytes()),
        Bytes::copy_from_slice(schema_key.as_bytes()),
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
pub(crate) struct SchemaInternHandle {
    intern: SchemaIntern,
    loaded: tokio::sync::OnceCell<()>,
}

impl SchemaInternHandle {
    pub(crate) fn intern(&self) -> &SchemaIntern {
        &self.intern
    }

    pub(crate) async fn ensure_loaded<S>(&self, store: &S) -> Result<(), StorageError>
    where
        S: StorageAdapterRead + ?Sized,
    {
        self.loaded
            .get_or_try_init(|| self.intern.load(store))
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assignment_is_sequential_and_stable() {
        let intern = SchemaIntern::new();
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
