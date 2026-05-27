use std::fmt;

use bytes::Bytes;

use crate::backend::{Key, KeyRange, SpaceId, Support};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendError {
    Unsupported(Capability),
    InvalidKey,
    InvalidCursor,
    ReadExpired,
    WriteConflict,
    PreconditionFailed(Vec<PreconditionFailure>),
    Durability,
    Corruption(String),
    Io(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Capability {
    EnvelopeProjection,
    KeyOrderedPoints,
    UnorderedPoints,
    ReverseScan,
    DeleteRange,
    Preconditions,
    IdempotentCommit,
    PredicatePushdown,
    ParallelPartitions,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Precondition {
    KeyAbsent {
        space: SpaceId,
        key: Key,
    },
    KeyPresent {
        space: SpaceId,
        key: Key,
    },
    KeyValueHashEquals {
        space: SpaceId,
        key: Key,
        hash: [u8; 32],
    },
    RangeEmpty {
        space: SpaceId,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreconditionItemSupport {
    pub index: usize,
    pub support: Support,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreconditionFailure {
    pub index: usize,
}

impl fmt::Display for BackendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BackendError::Unsupported(capability) => {
                write!(f, "unsupported capability: {capability:?}")
            }
            BackendError::InvalidKey => f.write_str("invalid key encoding"),
            BackendError::InvalidCursor => f.write_str("cursor is invalid for this read view"),
            BackendError::ReadExpired => f.write_str("read transaction is no longer valid"),
            BackendError::WriteConflict => f.write_str("write conflict"),
            BackendError::PreconditionFailed(failures) => {
                write!(f, "precondition failed: {failures:?}")
            }
            BackendError::Durability => f.write_str("durability failure"),
            BackendError::Corruption(message) => write!(f, "backend corruption: {message}"),
            BackendError::Io(message) => write!(f, "io error: {message}"),
        }
    }
}

impl std::error::Error for BackendError {}
