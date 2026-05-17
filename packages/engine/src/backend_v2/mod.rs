//! Experimental v2 backend API.
//!
//! This module is intentionally isolated from the current `backend` module
//! while the ordered byte-key API and its conformance suite settle.

mod capabilities;
pub mod conformance;
mod error;
mod in_memory;
mod predicate;
mod traits;
mod types;

pub use capabilities::{
    BackendCapabilities, BackendProfile, ProjectionCapabilities, PushdownCapabilities,
    ScanCapabilities, WriteCapabilities, WriteConcurrency,
};
pub use error::{
    BackendError, Capability, Precondition, PreconditionFailure, PreconditionItemSupport,
    PreconditionSupportReport,
};
pub use in_memory::{
    InMemoryBackend, InMemoryBackendFactory, InMemoryBackendFixture, InMemoryRead,
    InMemoryScanVisitResult, InMemoryWrite,
};
pub use predicate::{
    BackendPredicate, HeaderFieldId, HeaderPredicate, KeyPredicate, PredicateExpr, PredicateId,
    PredicateSupportLevel, RefKind, RefsPredicate, ScalarValue, Support,
};
pub use traits::{
    get_many, visit_range, Backend, BackendRangeScan, BackendRead, BackendWrite, BufferedRangeScan,
    PointVisitor, ScanVisitor,
};
pub use types::{
    CommitResult, CoreProjection, Durability, GetManyResult, GetOptions, Key, KeyRange, KeyRef,
    Prefix, ProjectedValue, ProjectedValueRef, PutBatch, PutEntry, ReadConsistency, ReadEntry,
    ReadOptions, ScanChunk, ScanOptions, ScanResult, SnapshotRef, SpaceId, StoredValue, Value,
    WriteOptions, WriteStats,
};
