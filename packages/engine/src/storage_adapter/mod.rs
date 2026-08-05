//! Primary storage adapter.
//!
//! This module is the Lix-neutral layer between domain stores and
//! `storage`. Domain stores own schemas and key layouts; the adapter owns
//! shared scopes, batching, lowering, cursors, and adapter stats.
//!
//! The storage adapter is intentionally below the session transaction lifecycle. Direct
//! users of `StorageAdapter` or `StorageWriteSet` bypass session close/commit
//! accounting and rely on storage-provided serialization.

mod context;
mod point;
mod read_scope;
#[cfg(test)]
mod reader;
mod scan;
mod spaces;
mod stats;
mod write_set;

#[cfg(test)]
mod conformance;

pub use crate::storage::{
    BufferRange, CoreProjection as StorageCoreProjection, EncodedMutationBatch, EncodedPut,
    GetManyRequest as StorageGetManyRequest, GetManyResult as StorageGetManyResult,
    GetOptions as StorageGetOptions, Key as StorageKey, KeyRange as StorageKeyRange,
    MAX_SCAN_PAGE_ROWS, Memory, MemoryRead, MemoryWrite, Precondition as StoragePrecondition,
    Prefix as StoragePrefix, ProjectedValue as StorageProjectedValue,
    ReadDurability as StorageReadDurability, ReadEntry as StorageReadEntry,
    ReadOptions as StorageReadOptions, ScanChunk as StorageScanChunk,
    ScanOptions as StorageScanOptions, SpaceId as StorageSpaceId, Storage, StorageError,
    StorageRead, StorageSpace, StoredValue as StorageValue, ValueSemantics,
    WriteOptions as StorageWriteOptions,
};
pub(crate) use crate::storage::{PutBatch, PutEntry};

pub use context::StorageAdapter;
pub(crate) use point::exact_get_many;
pub use point::{PointReadPlan, PointValues, RequestedToUnique, RequestedToUniqueRef};
pub(crate) use read_scope::SharedStorageAdapterRead;
pub use read_scope::{StorageAdapterRead, StorageAdapterReadScope};
pub use scan::ScanPlan;
pub(crate) use scan::collect_many;
pub use stats::{
    StorageReadResult, StorageReadStats, StorageReadStatsCollector, StorageWriteSetStats,
};
#[cfg(any(test, feature = "storage-benches"))]
pub use write_set::StorageWriteSetArenaStats;
pub(crate) use write_set::{DeferredFinalPutPage, DeferredFinalPutSource};
pub use write_set::{StorageWriteSet, StorageWriteSetError};
