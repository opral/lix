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
    BackendPredicate, HeaderFieldId, HeaderPredicate, KeyPredicate, LimitSupport, OrderSupport,
    PredicateExpr, PredicateId, PredicatePushdown, PredicateSupportLevel, ProjectionSupport,
    ReadSupport, RefKind, RefsPredicate, ScalarValue, Support,
};
pub use traits::{Backend, BackendRead, BackendWrite};
pub use types::{
    CommitResult, Cursor, Durability, GetManyResult, GetOptions, GetSlot, Key, KeyRange, Prefix,
    ProjectedValue, PutBatch, PutEntry, ReadBatch, ReadConsistency, ReadEntry, ReadOptions,
    ReadStats, ScanDirection, ScanOptions, ScanPage, SnapshotRef, SpaceId, StoredValue, Value,
    ValueProjection, WriteOptions, WriteStats,
};
