//! Primary storage adapter.
//!
//! This module is the Lix-neutral layer between domain stores and
//! `backend`. Domain stores own schemas and key layouts; storage owns
//! shared scopes, batching, lowering, cursors, and adapter stats.
//!
//! Storage is intentionally below the session transaction lifecycle. Direct
//! users of `StorageContext` or `StorageWriteSet` bypass session close/commit
//! accounting and rely on backend-provided serialization.

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

pub use crate::backend::{
    Backend as StorageBackend, BackendError as StorageBackendError,
    BackendRead as StorageBackendRead, CoreProjection as StorageCoreProjection,
    GetOptions as StorageGetOptions, InMemoryBackend as InMemoryStorageBackend,
    InMemoryRead as InMemoryStorageRead, InMemoryWrite as InMemoryStorageWrite, Key as StorageKey,
    KeyRange as StorageKeyRange, Prefix as StoragePrefix, ProjectedValue as StorageProjectedValue,
    ReadEntry as StorageReadEntry, ReadOptions as StorageReadOptions,
    ScanOptions as StorageScanOptions, SpaceId as StorageSpaceId, StoredValue as StorageValue,
    WriteOptions as StorageWriteOptions,
};

pub use context::StorageContext;
pub use point::{PointReadPlan, PointValues, RequestedToUnique, RequestedToUniqueRef};
pub(crate) use read_scope::SharedStorageRead;
pub use read_scope::{StorageRead, StorageReadScope};
pub use scan::ScanPlan;
pub use spaces::StorageSpace;
pub use stats::{
    StorageReadResult, StorageReadStats, StorageReadStatsCollector, StorageWriteSetStats,
};
pub use write_set::{StorageWriteSet, StorageWriteSetError};
