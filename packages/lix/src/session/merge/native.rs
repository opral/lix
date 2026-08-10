//! Native merge planning over authenticated ForkTree state values.
//!
//! This module is deliberately session-domain specific. It owns the small
//! merge plan needed by the public merge API and consumes native `StateKey`
//!/`StateDiffEntry` data; it is not a compatibility facade for the retired
//! generic state module.

use std::cmp::Ordering;
use std::collections::HashMap;

use crate::LixError;
use crate::changelog::{ChangeId, CommitId};
use crate::common::{LixTimestamp, SharedStr};
use crate::forktree::{StateCell, StateKey, StateValue};
use crate::json_store::JsonSlot;
use crate::state::StateDiffEntry;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct MergeDiff {
    pub(crate) entries: Vec<MergeDiffEntry>,
    payloads: MergePayloadBatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MergeDiffEntry {
    pub(crate) identity: StateKey,
    pub(crate) kind: MergeDiffKind,
    pub(crate) before: Option<MergeRow>,
    pub(crate) after: Option<MergeRow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MergeDiffKind {
    Added,
    Modified,
    Removed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MergeRow {
    pub(crate) key: StateKey,
    pub(crate) deleted: bool,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
    pub(crate) change_id: ChangeId,
    pub(crate) commit_id: CommitId,
    pub(crate) snapshot_content: Option<SharedStr>,
    pub(crate) metadata: Option<SharedStr>,
}

impl MergeRow {
    pub(crate) fn from_state_value(key: StateKey, value: &StateValue) -> Self {
        let (deleted, snapshot_content) = match &value.cell {
            StateCell::Tombstone => (true, None),
            StateCell::Null => (false, None),
            StateCell::Value(value) => (false, Some(value.clone())),
        };
        Self {
            key,
            deleted,
            created_at: value.created_at,
            updated_at: value.updated_at,
            change_id: value.change_id,
            commit_id: value.commit_id,
            snapshot_content,
            metadata: value.metadata.clone(),
        }
    }

    pub(crate) fn schema_key(&self) -> &str {
        &self.key.schema_key
    }

    pub(crate) fn file_id(&self) -> Option<&str> {
        self.key.file_id.as_deref()
    }

    pub(crate) fn entity_pk(&self) -> &crate::entity_pk::EntityPk {
        &self.key.entity_pk
    }
}

/// Converts the native ordered endpoint diff into the session merge row form.
/// The native entry remains the source of identity/order; this conversion only
/// attaches the fields required by the existing merge semantics.
pub(crate) fn merge_entry_from_native(entry: StateDiffEntry) -> Option<MergeDiffEntry> {
    let identity = entry.key.clone();
    let before = entry
        .before
        .as_ref()
        .map(|value| MergeRow::from_state_value(identity.clone(), &value.value));
    let after = entry
        .after
        .as_ref()
        .map(|value| MergeRow::from_state_value(identity.clone(), &value.value));
    let before_live = before.as_ref().is_some_and(|row| !row.deleted);
    let after_live = after.as_ref().is_some_and(|row| !row.deleted);
    let kind = match (before_live, after_live) {
        (false, true) => MergeDiffKind::Added,
        (true, false) => MergeDiffKind::Removed,
        (true, true) => MergeDiffKind::Modified,
        (false, false) => return None,
    };
    Some(MergeDiffEntry {
        identity,
        kind,
        before,
        after,
    })
}

impl MergeDiff {
    pub(crate) fn from_native(entries: Vec<StateDiffEntry>, payloads: MergePayloadBatch) -> Self {
        Self {
            entries: entries
                .into_iter()
                .filter_map(merge_entry_from_native)
                .collect(),
            payloads,
        }
    }

    pub(crate) fn from_entries_with_payloads(
        entries: Vec<MergeDiffEntry>,
        payloads: MergePayloadBatch,
    ) -> Self {
        Self { entries, payloads }
    }

    pub(crate) fn payloads(&self) -> &MergePayloadBatch {
        &self.payloads
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct MergePayloadBatch {
    values: HashMap<ChangeId, (JsonSlot, JsonSlot)>,
}

impl MergePayloadBatch {
    pub(crate) fn from_payloads(
        payloads: impl IntoIterator<Item = (ChangeId, JsonSlot, JsonSlot)>,
    ) -> Result<Self, LixError> {
        let mut values = HashMap::new();
        for (change_id, snapshot, metadata) in payloads {
            if values.insert(change_id, (snapshot, metadata)).is_some() {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("merge payload batch contains duplicate change id '{change_id}'"),
                ));
            }
        }
        Ok(Self { values })
    }

    pub(crate) fn get(&self, change_id: ChangeId) -> Option<(&JsonSlot, &JsonSlot)> {
        self.values
            .get(&change_id)
            .map(|(snapshot, metadata)| (snapshot, metadata))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct MergePlan {
    pub(crate) picks: Vec<MergePick>,
    pub(crate) conflicts: Vec<MergeConflict>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MergePick {
    pub(crate) identity: StateKey,
    pub(crate) change_id: ChangeId,
    pub(crate) selected_row: MergeRow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MergeConflict {
    pub(crate) identity: StateKey,
    pub(crate) target: MergeDiffEntry,
    pub(crate) source: MergeDiffEntry,
}

pub(crate) trait MergeKeyExt {
    fn schema_key(&self) -> &str;
    fn file_id(&self) -> Option<&str>;
    fn entity_pk(&self) -> &crate::entity_pk::EntityPk;
    fn schema_key_shared(&self) -> SharedStr;
    fn file_id_shared(&self) -> Option<SharedStr>;
    fn shares_key_with(&self, other: &StateKey) -> bool;
}

impl MergeKeyExt for StateKey {
    fn schema_key(&self) -> &str {
        &self.schema_key
    }

    fn file_id(&self) -> Option<&str> {
        self.file_id.as_deref()
    }

    fn entity_pk(&self) -> &crate::entity_pk::EntityPk {
        &self.entity_pk
    }

    fn schema_key_shared(&self) -> SharedStr {
        self.schema_key.clone().into()
    }

    fn file_id_shared(&self) -> Option<SharedStr> {
        self.file_id.clone().map(Into::into)
    }

    fn shares_key_with(&self, other: &StateKey) -> bool {
        self == other
    }
}

pub(crate) fn plan_merge(
    target: &MergeDiff,
    source: &MergeDiff,
    fallback_payloads: &MergePayloadBatch,
) -> Result<MergePlan, LixError> {
    ensure_strictly_sorted(&target.entries)?;
    ensure_strictly_sorted(&source.entries)?;
    let mut plan = MergePlan::default();
    let mut target_index = 0;
    let mut source_index = 0;
    while target_index < target.entries.len() && source_index < source.entries.len() {
        let target_entry = &target.entries[target_index];
        let source_entry = &source.entries[source_index];
        match target_entry.identity.cmp(&source_entry.identity) {
            Ordering::Less => target_index += 1,
            Ordering::Greater => {
                plan.picks.push(source_pick(source_entry)?);
                source_index += 1;
            }
            Ordering::Equal => {
                if !same_final_state(
                    target_entry,
                    source_entry,
                    target,
                    source,
                    fallback_payloads,
                ) {
                    plan.conflicts.push(MergeConflict {
                        identity: target_entry.identity.clone(),
                        target: target_entry.clone(),
                        source: source_entry.clone(),
                    });
                }
                target_index += 1;
                source_index += 1;
            }
        }
    }
    for entry in &source.entries[source_index..] {
        plan.picks.push(source_pick(entry)?);
    }
    Ok(plan)
}

fn ensure_strictly_sorted(entries: &[MergeDiffEntry]) -> Result<(), LixError> {
    for pair in entries.windows(2) {
        match pair[0].identity.cmp(&pair[1].identity) {
            Ordering::Less => {}
            Ordering::Equal => {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "merge received duplicate diff entry for schema '{}' entity '{}'",
                        pair[1].identity.schema_key,
                        pair[1].identity.entity_pk.as_json_array_text()?
                    ),
                ));
            }
            Ordering::Greater => {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "native merge diff is not in canonical StateKey order",
                ));
            }
        }
    }
    Ok(())
}

fn source_pick(entry: &MergeDiffEntry) -> Result<MergePick, LixError> {
    let Some(row) = entry.after.clone() else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "merge cannot pick source removal for schema '{}' entity '{}' without a tombstone row",
                entry.identity.schema_key,
                entry.identity.entity_pk.as_json_array_text()?
            ),
        ));
    };
    Ok(MergePick {
        identity: entry.identity.clone(),
        change_id: row.change_id,
        selected_row: row,
    })
}

fn same_final_state(
    target: &MergeDiffEntry,
    source: &MergeDiffEntry,
    target_diff: &MergeDiff,
    source_diff: &MergeDiff,
    fallback: &MergePayloadBatch,
) -> bool {
    match (target.after.as_ref(), source.after.as_ref()) {
        (None, None) => true,
        (Some(target), Some(source)) if target.deleted && source.deleted => true,
        (Some(target), Some(source)) if !target.deleted && !source.deleted => {
            row_payload_eq(target, source, target_diff, source_diff, fallback)
        }
        _ => false,
    }
}

fn row_payload_eq(
    left: &MergeRow,
    right: &MergeRow,
    target: &MergeDiff,
    source: &MergeDiff,
    fallback: &MergePayloadBatch,
) -> bool {
    if left.change_id == right.change_id {
        return true;
    }
    let left = target
        .payloads()
        .get(left.change_id)
        .or_else(|| source.payloads().get(left.change_id))
        .or_else(|| fallback.get(left.change_id));
    let right = target
        .payloads()
        .get(right.change_id)
        .or_else(|| source.payloads().get(right.change_id))
        .or_else(|| fallback.get(right.change_id));
    match (left, right) {
        (Some((left_snapshot, left_metadata)), Some((right_snapshot, right_metadata))) => {
            left_snapshot == right_snapshot && left_metadata == right_metadata
        }
        _ => false,
    }
}
