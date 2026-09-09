//! Shared current-state evidence carried by prepared mutations.
//!
//! HOT resolves and decodes these values; transaction batches only carry them.
//! Keeping their data definitions here prevents serving types owning the
//! mutation vocabulary. Their durable encodings and validation are unchanged.

use crate::changelog::{ChangeId, CommitId};
use crate::common::LixTimestamp;
use bytes::Bytes;

/// Stable physical address of a row in an immutable columnar base.
///
/// The owner commit is part of the address so consumers can fail closed when
/// a stale coordinate is presented against a different base.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ColumnarBaseCoordinate {
    pub(crate) base_commit_id: CommitId,
    pub(crate) group_index: u32,
    pub(crate) row_index: u32,
}

#[derive(Debug, Clone)]
pub(crate) enum CertifiedCurrentStatePredecessor {
    Encoded(Bytes),
    Packed(PackedHeadValue),
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PackedHeadValue {
    pub(crate) change_id: ChangeId,
    pub(crate) commit_id: CommitId,
    pub(crate) deleted: bool,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
    pub(crate) working_diff_baseline: PackedWorkingDiffBaseline,
    pub(crate) columnar_base_coordinate: Option<ColumnarBaseCoordinate>,
}

/// Checkpoint-relative position of a current-state base row that is served
/// without a branch-local hot row.
///
/// The two bases are not interchangeable and must not share one encoding. A
/// *packed* current base is a collection published **inside** the active
/// working interval, so its rows were absent at the checkpoint. A *root*
/// current base is the referenced head itself, so its rows **are** the
/// checkpoint state. Collapsing both onto "has an active checkpoint id" made
/// the first branch-local mutation of a checkpointed identity look like a
/// creation.
#[derive(Debug, Clone, Copy)]
pub(crate) enum PackedWorkingDiffBaseline {
    /// No active checkpoint owns this generation.
    Disabled,
    /// Published inside the active working interval: absent at the checkpoint.
    AbsentAtCheckpoint { checkpoint_commit_id: CommitId },
    /// Served from the referenced root current base: present at the active
    /// checkpoint and unchanged since.
    CleanAtCheckpoint,
}

/// One indexed value, encoded so that equality is a key prefix.
///
/// Integers use the same order-preserving flip as row-pk components so a
/// future range predicate can reuse this encoding unchanged.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum HotIndexValue {
    String(String),
    Integer(i64),
}
