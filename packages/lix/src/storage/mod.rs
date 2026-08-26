//! Primary storage API.

mod change_watch;
pub mod conformance;
mod cursor;
mod error;
#[doc(hidden)]
pub mod immutable;
mod in_memory;
mod predicate;
mod traits;
mod types;

pub use change_watch::{StorageChangeSource, StorageChangeWatch};
pub use cursor::{ScanCursor, StorageScanSource};
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
    BeginScanOptions, BufferRange, CommitResult, CoreProjection, EncodedMutationBatch,
    EncodedMutationBatchError, EncodedPut, GetManyRequest, GetManyResult, GetOptions, Key,
    KeyRange, MAX_SCAN_PAGE_ROWS, Prefix, ProjectedValue, PutBatch, PutEntry, ReadConsistency,
    ReadDurability, ReadEntry, ReadOptions, ScanChunk, ScanOrder, SnapshotRef, SpaceId,
    StorageSpace, StorageSpaceRole, StoredValue, ValueIntegrity, ValueSemantics, WriteOptions,
    WriteStats,
};
