//! Experimental v2 storage adapter.
//!
//! This module is the Lix-neutral layer between domain stores and
//! `backend_v2`. Domain stores own schemas and key layouts; storage_v2 owns
//! shared scopes, batching, lowering, cursors, and adapter stats.

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

pub use context::StorageContext;
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
