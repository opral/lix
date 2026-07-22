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
    CoreProjection as StorageCoreProjection, GetManyResult as StorageGetManyResult,
    GetOptions as StorageGetOptions, Key as StorageKey, KeyRange as StorageKeyRange, Memory,
    MemoryRead, MemoryWrite, Prefix as StoragePrefix, ProjectedValue as StorageProjectedValue,
    ReadEntry as StorageReadEntry, ReadOptions as StorageReadOptions,
    ScanChunk as StorageScanChunk, ScanOptions as StorageScanOptions, SpaceId as StorageSpaceId,
    Storage, StorageError, StorageRead, StoredValue as StorageValue,
    WriteOptions as StorageWriteOptions,
};

pub use context::StorageAdapter;
pub use point::{PointReadPlan, PointValues, RequestedToUnique, RequestedToUniqueRef};
pub(crate) use read_scope::SharedStorageAdapterRead;
pub use read_scope::{StorageAdapterRead, StorageAdapterReadScope};
pub use scan::ScanPlan;
pub use spaces::StorageSpace;
pub use stats::{
    StorageReadResult, StorageReadStats, StorageReadStatsCollector, StorageWriteSetStats,
};
pub use write_set::{StorageWriteSet, StorageWriteSetError};
