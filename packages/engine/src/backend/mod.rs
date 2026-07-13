//! Primary backend API.

pub mod conformance;
mod error;
mod in_memory;
mod predicate;
mod traits;
mod types;

pub use error::{
    BackendError, Capability, Precondition, PreconditionFailure, PreconditionItemSupport,
    PreconditionSupportReport,
};
pub use in_memory::{
    InMemoryBackend, InMemoryBackendFactory, InMemoryBackendFixture, InMemoryRead, InMemoryWrite,
};
pub use predicate::{
    BackendPredicate, HeaderFieldId, HeaderPredicate, KeyPredicate, PredicateExpr, PredicateId,
    PredicateSupportLevel, RefKind, RefsPredicate, ScalarValue, Support,
};
pub use traits::{Backend, BackendRead, BackendWrite};
pub use types::{
    CommitResult, CoreProjection, Durability, GetManyResult, GetOptions, Key, KeyRange,
    MAX_SCAN_PAGE_ROWS, Prefix, ProjectedValue, PutBatch, PutEntry, ReadConsistency, ReadEntry,
    ReadOptions, ScanChunk, ScanOptions, SnapshotRef, SpaceId, StoredValue, WriteOptions,
    WriteStats,
};
