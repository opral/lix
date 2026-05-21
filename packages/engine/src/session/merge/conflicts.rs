use crate::tracked_state::{
    TrackedStateDiffEntry, TrackedStateDiffKind, TrackedStateMergeConflict, TrackedStateMergePlan,
};
use crate::LixError;
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MergeConflict {
    pub(crate) kind: MergeConflictKind,
    pub(crate) schema_key: String,
    pub(crate) entity_pk: JsonValue,
    pub(crate) file_id: Option<String>,
    pub(crate) target: MergeConflictSide,
    pub(crate) source: MergeConflictSide,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MergeConflictKind {
    SameEntityChanged,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MergeConflictSide {
    pub(crate) kind: MergeConflictChangeKind,
    pub(crate) before_change_id: Option<String>,
    pub(crate) after_change_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MergeConflictChangeKind {
    Added,
    Modified,
    Removed,
}

pub(crate) fn conflicts_from_plan(
    plan: &TrackedStateMergePlan,
) -> Result<Vec<MergeConflict>, LixError> {
    plan.conflicts.iter().map(conflict_from_tracked).collect()
}

fn conflict_from_tracked(conflict: &TrackedStateMergeConflict) -> Result<MergeConflict, LixError> {
    Ok(MergeConflict {
        kind: MergeConflictKind::SameEntityChanged,
        schema_key: conflict.identity.schema_key.clone(),
        entity_pk: conflict.identity.entity_pk.as_json_array_value()?,
        file_id: conflict.identity.file_id.clone(),
        target: conflict_side_from_diff_entry(&conflict.target),
        source: conflict_side_from_diff_entry(&conflict.source),
    })
}

fn conflict_side_from_diff_entry(entry: &TrackedStateDiffEntry) -> MergeConflictSide {
    MergeConflictSide {
        kind: match entry.kind {
            TrackedStateDiffKind::Added => MergeConflictChangeKind::Added,
            TrackedStateDiffKind::Modified => MergeConflictChangeKind::Modified,
            TrackedStateDiffKind::Removed => MergeConflictChangeKind::Removed,
        },
        before_change_id: entry.before.as_ref().map(|row| row.change_id.clone()),
        after_change_id: entry.after.as_ref().map(|row| row.change_id.clone()),
    }
}
