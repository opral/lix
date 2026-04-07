//! Canonical journal owner package.
//!
//! This package owns durable canonical fact persistence.

pub(crate) mod write;

pub(crate) use write::{
    build_prepared_batch_from_canonical_output, CanonicalCommitOutput, ChangeRow, CHANGE_TABLE,
    SNAPSHOT_TABLE,
};
