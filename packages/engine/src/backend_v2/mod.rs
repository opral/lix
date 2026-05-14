//! Experimental v2 backend API.
//!
//! This module is intentionally isolated from the current `backend` module
//! while the ordered byte-key API and its conformance suite settle.

mod capabilities;
pub mod conformance;
mod error;
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
pub use predicate::{
    BackendPredicate, HeaderFieldId, HeaderPredicate, KeyPredicate, PredicateExpr, PredicateId,
    PredicateSupportLevel, RefKind, RefsPredicate, ScalarValue, Support,
};
pub use traits::{Backend, BackendRead, BackendWrite};
pub use types::{
    CommitResult, CoreProjection, Durability, GetManyResult, GetOptions, Key, KeyRange, Prefix,
    ProjectedValue, PutBatch, PutEntry, ReadBatch, ReadConsistency, ReadEntry, ReadOptions,
    ScanOptions, ScanPage, SnapshotRef, SpaceId, StoredValue, Value, WriteOptions, WriteStats,
};
