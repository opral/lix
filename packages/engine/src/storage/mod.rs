//! Primary storage adapter.
//!
//! This module is the Lix-neutral layer between domain stores and
//! `backend`. Domain stores own schemas and key layouts; storage owns
//! shared scopes, batching, lowering, cursors, and adapter stats.

mod context;
mod legacy;
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

pub trait StorageBackend: crate::backend::Backend {}
impl<T> StorageBackend for T where T: crate::backend::Backend {}
pub trait StorageBackendRead: crate::backend::BackendRead {}
impl<T> StorageBackendRead for T where T: crate::backend::BackendRead {}

pub use crate::backend::{
    BackendError as StorageBackendError, CoreProjection as StorageCoreProjection,
    GetOptions as StorageGetOptions, InMemoryBackend as InMemoryStorageBackend,
    InMemoryRead as InMemoryStorageRead, InMemoryWrite as InMemoryStorageWrite, Key as StorageKey,
    Prefix as StoragePrefix, ProjectedValue as StorageProjectedValue,
    ReadOptions as StorageReadOptions, ScanOptions as StorageScanOptions,
    SpaceId as StorageSpaceId, StoredValue as StorageValue, WriteOptions as StorageWriteOptions,
};

pub use context::StorageContext;
pub(crate) use legacy::{
    KvEntryPage, KvExistsBatch, KvExistsGroup, KvGetGroup, KvGetRequest, KvKeyPage, KvScanRange,
    KvScanRequest, KvValueBatch, KvValueGroup, KvValuePage, KvWriteStats, StorageReader,
};
pub use point::{
    PointReadBuffer, PointReadPlan, PointValues, PointValuesRef, RequestedToUnique,
    RequestedToUniqueRef,
};
pub use read_scope::{StorageRead, StorageReadScope};
pub use scan::{ScanBuffer, ScanChunkRef, ScanCursor, ScanPlan};
pub(crate) use spaces::decode_logical_key_ref;
pub use spaces::StorageSpace;
pub use stats::{
    StorageReadResult, StorageReadStats, StorageReadStatsCollector, StorageWriteSetStats,
};
pub use write_set::{StorageWriteSet, StorageWriteSetError};
