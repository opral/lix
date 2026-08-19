use std::fmt;

use bytes::Bytes;

use crate::storage::{Key, KeyRange, StorageSpace, Support};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    Unsupported(Capability),
    InvalidKey,
    InvalidCursor,
    ReadExpired,
    WriteConflict,
    PreconditionFailed(Vec<PreconditionFailure>),
    Durability,
    /// The storage writer was superseded by a newer client and cannot be reused.
    Fenced,
    /// The storage instance has stopped and cannot be reused.
    Closed(String),
    /// A commit may have been applied, but the caller did not receive a
    /// definitive result.
    CommitOutcomeUnknown(String),
    Corruption(String),
    Io(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Capability {
    EnvelopeProjection,
    KeyOrderedPoints,
    UnorderedPoints,
    ReverseScan,
    DeleteRange,
    MigrationReplace,
    Preconditions,
    IdempotentCommit,
    PredicatePushdown,
    ParallelPartitions,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Precondition {
    KeyAbsent {
        space: StorageSpace,
        key: Key,
    },
    KeyPresent {
        space: StorageSpace,
        key: Key,
    },
    KeyValueHashEquals {
        space: StorageSpace,
        key: Key,
        hash: [u8; 32],
    },
    KeyValueEquals {
        space: StorageSpace,
        key: Key,
        expected: Bytes,
    },
    RangeEmpty {
        space: StorageSpace,
        range: KeyRange,
    },
    BranchEquals {
        ref_key: Key,
        expected: Bytes,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreconditionSupportReport {
    pub items: Vec<PreconditionItemSupport>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PreconditionItemSupport {
    pub index: usize,
    pub support: Support,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PreconditionFailure {
    pub index: usize,
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported(capability) => {
                write!(f, "unsupported capability: {capability:?}")
            }
            Self::InvalidKey => f.write_str("invalid key encoding"),
            Self::InvalidCursor => f.write_str("cursor is invalid for this read view"),
            Self::ReadExpired => f.write_str("read transaction is no longer valid"),
            Self::WriteConflict => f.write_str("write conflict"),
            Self::PreconditionFailed(failures) => {
                write!(f, "precondition failed: {failures:?}")
            }
            Self::Durability => f.write_str("durability failure"),
            Self::Fenced => f.write_str("storage writer was fenced by a newer client"),
            Self::Closed(message) => write!(f, "storage instance is closed: {message}"),
            Self::CommitOutcomeUnknown(message) => {
                write!(f, "storage commit outcome is unknown: {message}")
            }
            Self::Corruption(message) => write!(f, "storage corruption: {message}"),
            Self::Io(message) => write!(f, "io error: {message}"),
        }
    }
}

impl std::error::Error for StorageError {}
