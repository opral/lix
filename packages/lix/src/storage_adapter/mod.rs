//! Primary storage adapter.
//!
//! This module is the Lix-neutral layer between domain stores and
//! `storage`. Domain stores own schemas and key layouts; the adapter owns
//! shared scopes, batching, lowering, cursors, and adapter stats.
//!
//! The storage adapter is intentionally below the session transaction lifecycle. Direct
//! users of `StorageAdapter` or `StorageWriteSet` bypass session close/commit
//! accounting and rely on storage-provided serialization.

#![cfg_attr(
    not(feature = "storage-benches"),
    allow(dead_code, unfulfilled_lint_expectations, unused_imports)
)]

mod context;
mod point;
mod read_scope;
#[cfg(test)]
mod reader;
mod spaces;
mod stats;
mod write_set;

#[cfg(test)]
mod conformance;

pub use crate::storage::{
    BeginScanOptions as StorageBeginScanOptions, BufferRange,
    CoreProjection as StorageCoreProjection, EncodedMutationBatch, EncodedPut,
    GetManyRequest as StorageGetManyRequest, GetManyResult as StorageGetManyResult,
    GetOptions as StorageGetOptions, Key as StorageKey, KeyRange as StorageKeyRange,
    MAX_SCAN_PAGE_ROWS, Memory, MemoryRead, MemoryWrite, Precondition as StoragePrecondition,
    Prefix as StoragePrefix, ProjectedValue as StorageProjectedValue,
    ReadDurability as StorageReadDurability, ReadEntry as StorageReadEntry,
    ReadOptions as StorageReadOptions, ScanChunk as StorageScanChunk,
    ScanCursor as StorageScanCursor, ScanOrder as StorageScanOrder, SpaceId as StorageSpaceId,
    Storage, StorageError, StorageRead, StorageSpace, StoredValue as StorageValue, ValueIntegrity,
    ValueSemantics, WriteOptions as StorageWriteOptions,
};
pub(crate) use crate::storage::{Capability as StorageCapability, PutBatch, PutEntry};
pub(crate) use crate::storage::StorageWrite;

pub use context::StorageAdapter;
pub(crate) use context::stage_mutation_revision;
pub(crate) use point::exact_get_many;
pub use point::{PointReadPlan, PointValues, RequestedToUnique, RequestedToUniqueRef};
pub(crate) use read_scope::SharedStorageAdapterRead;
pub use read_scope::{StorageAdapterRead, StorageAdapterReadScope};
pub(crate) use spaces::{
    REVISION_KEY_ACCOUNT, REVISION_KEY_BINARY_CAS_PUBLICATION, REVISION_KEY_BINARY_CAS_RECLAMATION,
    REVISION_KEY_CATALOG, REVISION_KEY_FILESYSTEM_PATH, REVISION_KEY_JSON_STORE_PUBLICATION,
    REVISION_KEY_JSON_STORE_RECLAMATION, REVISION_KEY_TRACKED_MUTATION, REVISION_SPACE,
    load_revision, load_revisions, revision_key,
};
pub use stats::{
    StorageReadResult, StorageReadStats, StorageReadStatsCollector, StorageWriteSetStats,
};
#[cfg(any(test, feature = "storage-benches"))]
pub use write_set::StorageWriteSetArenaStats;
pub use write_set::{StorageWriteSet, StorageWriteSetError};
