//! Primary storage API.

pub mod conformance;
mod error;
mod in_memory;
mod predicate;
mod traits;
mod types;

pub use error::{
    Capability, Precondition, PreconditionFailure, PreconditionItemSupport,
    PreconditionSupportReport, StorageError,
};
pub use in_memory::{Memory, MemoryFactory, MemoryFixture, MemoryRead, MemoryWrite};
pub use predicate::{
    HeaderFieldId, HeaderPredicate, KeyPredicate, PredicateExpr, PredicateId,
    PredicateSupportLevel, RefKind, RefsPredicate, ScalarValue, StoragePredicate, Support,
};
pub use traits::{Storage, StorageRead, StorageWrite};
pub use types::{
    CommitResult, CoreProjection, Durability, GetManyResult, GetOptions, Key, KeyRange,
    MAX_SCAN_PAGE_ROWS, Prefix, ProjectedValue, PutBatch, PutEntry, ReadConsistency, ReadEntry,
    ReadOptions, ScanChunk, ScanOptions, SnapshotRef, SpaceId, StoredValue, WriteOptions,
    WriteStats,
};
