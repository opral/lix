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
    pub(crate) deleted: bool,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
    pub(crate) change_id: ChangeId,
    pub(crate) commit_id: CommitId,
}

impl MergeRow {
    pub(crate) fn from_state_value(value: &StateValue) -> Self {
        let deleted = matches!(&value.cell, StateCell::Tombstone);
        Self {
            deleted,
            created_at: value.created_at,
            updated_at: value.updated_at,
            change_id: value.change_id,
            commit_id: value.commit_id,
        }
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
        .map(|value| MergeRow::from_state_value(&value.value));
    let after = entry
        .after
        .as_ref()
        .map(|value| MergeRow::from_state_value(&value.value));
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
    values: HashMap<ChangeId, (StateCell, Option<SharedStr>)>,
}

impl MergePayloadBatch {
    pub(crate) fn from_payloads(
        payloads: impl IntoIterator<Item = (ChangeId, StateCell, Option<SharedStr>)>,
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

    pub(crate) fn get(&self, change_id: ChangeId) -> Option<(&StateCell, Option<&SharedStr>)> {
        self.values
            .get(&change_id)
            .map(|(snapshot, metadata)| (snapshot, metadata.as_ref()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct MergePlan {
    pub(crate) picks: Vec<MergePick>,
    pub(crate) conflicts: Vec<MergeConflict>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MergePick {
    pub(crate) source_index: usize,
    pub(crate) change_id: ChangeId,
}

impl MergePick {
    pub(crate) fn identity<'a>(&self, source: &'a MergeDiff) -> &'a StateKey {
        &source.entries[self.source_index].identity
    }

    pub(crate) fn selected_row<'a>(&self, source: &'a MergeDiff) -> &'a MergeRow {
        source.entries[self.source_index]
            .after
            .as_ref()
            .expect("source pick always references a live after row")
    }
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
    fn row_pk(&self) -> &crate::row_pk::RowPk;
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

    fn row_pk(&self) -> &crate::row_pk::RowPk {
        &self.row_pk
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

pub(crate) fn plan_merge(target: &MergeDiff, source: &MergeDiff) -> Result<MergePlan, LixError> {
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
                plan.picks.push(source_pick(source_entry, source_index)?);
                source_index += 1;
            }
            Ordering::Equal => {
                let target_unchanged = same_state(
                    target_entry.before.as_ref(),
                    target_entry.after.as_ref(),
                    target,
                    source,
                );
                let source_unchanged = same_state(
                    source_entry.before.as_ref(),
                    source_entry.after.as_ref(),
                    target,
                    source,
                );
                if target_unchanged && !source_unchanged {
                    plan.picks.push(source_pick(source_entry, source_index)?);
                } else if !target_unchanged
                    && !source_unchanged
                    && !same_state(
                        target_entry.after.as_ref(),
                        source_entry.after.as_ref(),
                        target,
                        source,
                    )
                {
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
    for (index, entry) in source.entries.iter().enumerate().skip(source_index) {
        plan.picks.push(source_pick(entry, index)?);
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
                        "merge received duplicate diff entry for schema '{}' row '{}'",
                        pair[1].identity.schema_key,
                        pair[1].identity.row_pk.as_json_array_text()?
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

fn source_pick(entry: &MergeDiffEntry, source_index: usize) -> Result<MergePick, LixError> {
    let Some(row) = entry.after.clone() else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "merge cannot pick source removal for schema '{}' row '{}' without a tombstone row",
                entry.identity.schema_key,
                entry.identity.row_pk.as_json_array_text()?
            ),
        ));
    };
    Ok(MergePick {
        source_index,
        change_id: row.change_id,
    })
}

fn same_state(
    left: Option<&MergeRow>,
    right: Option<&MergeRow>,
    target_diff: &MergeDiff,
    source_diff: &MergeDiff,
) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) if left.deleted && right.deleted => true,
        (Some(left), Some(right)) if !left.deleted && !right.deleted => {
            row_payload_eq(left, right, target_diff, source_diff)
        }
        _ => false,
    }
}

fn row_payload_eq(
    left: &MergeRow,
    right: &MergeRow,
    target: &MergeDiff,
    source: &MergeDiff,
) -> bool {
    if left.change_id == right.change_id {
        return true;
    }
    let left = target
        .payloads()
        .get(left.change_id)
        .or_else(|| source.payloads().get(left.change_id));
    let right = target
        .payloads()
        .get(right.change_id)
        .or_else(|| source.payloads().get(right.change_id));
    match (left, right) {
        (Some((left_snapshot, left_metadata)), Some((right_snapshot, right_metadata))) => {
            left_snapshot == right_snapshot && left_metadata == right_metadata
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row_pk::RowPk;

    fn key(row: &str) -> StateKey {
        StateKey {
            schema_key: "test_schema".to_owned(),
            file_id: None,
            row_pk: RowPk::single(row),
        }
    }

    fn timestamp() -> LixTimestamp {
        LixTimestamp::from_unix_millis_utc_lossy(0)
    }

    fn row(change: &str, deleted: bool) -> MergeRow {
        MergeRow {
            deleted,
            created_at: timestamp(),
            updated_at: timestamp(),
            change_id: ChangeId::for_test_label(change),
            commit_id: CommitId::for_test_label(&format!("{change}-commit")),
        }
    }

    fn entry(
        row: &str,
        kind: MergeDiffKind,
        before: Option<MergeRow>,
        after: Option<MergeRow>,
    ) -> MergeDiffEntry {
        MergeDiffEntry {
            identity: key(row),
            kind,
            before,
            after,
        }
    }

    fn diff(entries: Vec<MergeDiffEntry>) -> MergeDiff {
        MergeDiff::from_entries_with_payloads(entries, MergePayloadBatch::default())
    }

    fn diff_with_payloads(
        entries: Vec<MergeDiffEntry>,
        payloads: impl IntoIterator<Item = (ChangeId, StateCell, Option<SharedStr>)>,
    ) -> MergeDiff {
        MergeDiff::from_entries_with_payloads(
            entries,
            MergePayloadBatch::from_payloads(payloads).expect("payload batch should be unique"),
        )
    }

    fn pick_rows(plan: &MergePlan, source: &MergeDiff) -> Vec<String> {
        plan.picks
            .iter()
            .map(|pick| {
                pick.identity(source)
                    .row_pk
                    .as_single_string_owned()
                    .expect("single-string row key")
            })
            .collect()
    }

    fn conflict_rows(plan: &MergePlan) -> Vec<String> {
        plan.conflicts
            .iter()
            .map(|conflict| {
                conflict
                    .identity
                    .row_pk
                    .as_single_string_owned()
                    .expect("single-string row key")
            })
            .collect()
    }

    #[test]
    fn source_add_applies() {
        let source = diff(vec![entry(
            "row-a",
            MergeDiffKind::Added,
            None,
            Some(row("source", false)),
        )]);
        let plan = plan_merge(&MergeDiff::default(), &source).expect("merge should plan");

        assert_eq!(pick_rows(&plan, &source), vec!["row-a"]);
        assert_eq!(plan.picks[0].change_id, ChangeId::for_test_label("source"));
        assert!(plan.conflicts.is_empty());
    }

    #[test]
    fn source_modify_applies() {
        let source = diff(vec![entry(
            "row-a",
            MergeDiffKind::Modified,
            Some(row("base", false)),
            Some(row("source", false)),
        )]);
        let plan = plan_merge(&MergeDiff::default(), &source).expect("merge should plan");

        assert_eq!(pick_rows(&plan, &source), vec!["row-a"]);
        assert_eq!(
            plan.picks[0].selected_row(&source).change_id,
            ChangeId::for_test_label("source")
        );
        assert!(!plan.picks[0].selected_row(&source).deleted);
    }

    #[test]
    fn source_delete_applies_tombstone() {
        let source = diff(vec![entry(
            "row-a",
            MergeDiffKind::Removed,
            Some(row("base", false)),
            Some(row("source-delete", true)),
        )]);
        let plan = plan_merge(&MergeDiff::default(), &source).expect("merge should plan");

        assert_eq!(pick_rows(&plan, &source), vec!["row-a"]);
        assert!(plan.picks[0].selected_row(&source).deleted);
        assert_eq!(
            plan.picks[0].change_id,
            ChangeId::for_test_label("source-delete")
        );
    }

    #[test]
    fn target_only_change_is_noop() {
        let target = diff(vec![entry(
            "row-a",
            MergeDiffKind::Modified,
            Some(row("base", false)),
            Some(row("target", false)),
        )]);
        let plan = plan_merge(&target, &MergeDiff::default()).expect("merge should plan");

        assert!(plan.picks.is_empty());
        assert!(plan.conflicts.is_empty());
    }

    #[test]
    fn both_sides_same_final_value_is_convergent_noop() {
        let target_change = ChangeId::for_test_label("target");
        let source_change = ChangeId::for_test_label("source");
        let same_snapshot = StateCell::Value(r#"{"value":"same"}"#.into());
        let target = diff_with_payloads(
            vec![entry(
                "row-a",
                MergeDiffKind::Modified,
                Some(row("base", false)),
                Some(row("target", false)),
            )],
            [(target_change, same_snapshot.clone(), None)],
        );
        let source = diff_with_payloads(
            vec![entry(
                "row-a",
                MergeDiffKind::Modified,
                Some(row("base", false)),
                Some(row("source", false)),
            )],
            [(source_change, same_snapshot, None)],
        );

        let plan = plan_merge(&target, &source).expect("merge should plan");
        assert!(plan.picks.is_empty());
        assert!(plan.conflicts.is_empty());
    }

    #[test]
    fn both_sides_delete_is_convergent_noop() {
        let target = diff(vec![entry(
            "row-a",
            MergeDiffKind::Removed,
            Some(row("base", false)),
            Some(row("target-delete", true)),
        )]);
        let source = diff(vec![entry(
            "row-a",
            MergeDiffKind::Removed,
            Some(row("base", false)),
            Some(row("source-delete", true)),
        )]);

        let plan = plan_merge(&target, &source).expect("merge should plan");
        assert!(plan.picks.is_empty());
        assert!(plan.conflicts.is_empty());
    }

    #[test]
    fn different_modifications_conflict() {
        let target = diff(vec![entry(
            "row-a",
            MergeDiffKind::Modified,
            Some(row("base", false)),
            Some(row("target", false)),
        )]);
        let source = diff(vec![entry(
            "row-a",
            MergeDiffKind::Modified,
            Some(row("base", false)),
            Some(row("source", false)),
        )]);

        let plan = plan_merge(&target, &source).expect("merge should plan");
        assert!(plan.picks.is_empty());
        assert_eq!(conflict_rows(&plan), vec!["row-a"]);
    }

    #[test]
    fn delete_modify_conflicts() {
        let target = diff(vec![entry(
            "row-a",
            MergeDiffKind::Removed,
            Some(row("base", false)),
            Some(row("target-delete", true)),
        )]);
        let source = diff(vec![entry(
            "row-a",
            MergeDiffKind::Modified,
            Some(row("base", false)),
            Some(row("source", false)),
        )]);

        let plan = plan_merge(&target, &source).expect("merge should plan");
        assert_eq!(conflict_rows(&plan), vec!["row-a"]);
    }

    #[test]
    fn modify_delete_conflicts() {
        let target = diff(vec![entry(
            "row-a",
            MergeDiffKind::Modified,
            Some(row("base", false)),
            Some(row("target", false)),
        )]);
        let source = diff(vec![entry(
            "row-a",
            MergeDiffKind::Removed,
            Some(row("base", false)),
            Some(row("source-delete", true)),
        )]);

        let plan = plan_merge(&target, &source).expect("merge should plan");
        assert_eq!(conflict_rows(&plan), vec!["row-a"]);
    }

    #[test]
    fn source_removal_without_tombstone_errors() {
        let source = diff(vec![entry(
            "row-a",
            MergeDiffKind::Removed,
            Some(row("base", false)),
            None,
        )]);
        let error = plan_merge(&MergeDiff::default(), &source)
            .expect_err("a source removal without a tombstone must fail");

        assert!(error.message.contains("without a tombstone row"));
    }

    #[test]
    fn pick_and_conflict_order_is_deterministic_by_identity() {
        let target = diff(vec![entry(
            "row-b",
            MergeDiffKind::Modified,
            Some(row("base", false)),
            Some(row("target", false)),
        )]);
        let source = diff(vec![
            entry(
                "row-a",
                MergeDiffKind::Added,
                None,
                Some(row("source-a", false)),
            ),
            entry(
                "row-b",
                MergeDiffKind::Modified,
                Some(row("base", false)),
                Some(row("source-b", false)),
            ),
            entry(
                "row-c",
                MergeDiffKind::Added,
                None,
                Some(row("source-c", false)),
            ),
        ]);

        let plan = plan_merge(&target, &source).expect("ordered merge should plan");
        assert_eq!(pick_rows(&plan, &source), vec!["row-a", "row-c"]);
        assert_eq!(conflict_rows(&plan), vec!["row-b"]);
    }

    #[test]
    fn large_sorted_merge_consumes_native_order_without_sorting_fallback() {
        const ROW_COUNT: usize = 10_000;
        let source = diff(
            (0..ROW_COUNT)
                .map(|index| {
                    let row_key = format!("row-{index:05}");
                    entry(
                        &row_key,
                        MergeDiffKind::Added,
                        None,
                        Some(row(&format!("source-{index:05}"), false)),
                    )
                })
                .collect(),
        );
        let plan = plan_merge(&MergeDiff::default(), &source)
            .expect("identity-sorted native merge should plan");

        assert_eq!(plan.picks.len(), ROW_COUNT);
        assert!(plan.conflicts.is_empty());
        assert_eq!(
            pick_rows(&plan, &source).first().unwrap(),
            "row-00000"
        );
        assert_eq!(
            pick_rows(&plan, &source).last().unwrap(),
            "row-09999"
        );
    }

    #[test]
    fn sparse_early_pick_does_not_require_payload_materialization_for_convergent_rows() {
        const CONVERGENT_ROW_COUNT: usize = 10_000;
        let target = diff(
            (0..CONVERGENT_ROW_COUNT)
                .map(|index| {
                    let row_key = format!("row-{index:05}");
                    entry(
                        &row_key,
                        MergeDiffKind::Modified,
                        Some(row(&format!("base-{index:05}"), false)),
                        Some(row(&format!("target-{index:05}"), false)),
                    )
                })
                .collect(),
        );
        let source = diff(vec![
            entry(
                "row-00000",
                MergeDiffKind::Modified,
                Some(row("base-00000", false)),
                Some(row("target-00000", false)),
            ),
            entry(
                "row-10000",
                MergeDiffKind::Added,
                None,
                Some(row("source-10000", false)),
            ),
        ]);

        let plan = plan_merge(&target, &source).expect("sparse native merge should plan");
        assert_eq!(pick_rows(&plan, &source), vec!["row-10000"]);
        assert!(plan.conflicts.is_empty());
    }

    #[test]
    fn large_conflict_merge_preserves_each_identity_and_side() {
        const ROW_COUNT: usize = 10_000;
        let target = diff(
            (0..ROW_COUNT)
                .map(|index| {
                    let row_key = format!("row-{index:05}");
                    entry(
                        &row_key,
                        MergeDiffKind::Modified,
                        Some(row(&format!("base-{index:05}"), false)),
                        Some(row(&format!("target-{index:05}"), false)),
                    )
                })
                .collect(),
        );
        let source = diff(
            (0..ROW_COUNT)
                .map(|index| {
                    let row_key = format!("row-{index:05}");
                    entry(
                        &row_key,
                        MergeDiffKind::Modified,
                        Some(row(&format!("base-{index:05}"), false)),
                        Some(row(&format!("source-{index:05}"), false)),
                    )
                })
                .collect(),
        );

        let plan = plan_merge(&target, &source).expect("large conflict merge should plan");
        assert!(plan.picks.is_empty());
        assert_eq!(plan.conflicts.len(), ROW_COUNT);
        assert_eq!(conflict_rows(&plan).first().unwrap(), "row-00000");
        assert_eq!(conflict_rows(&plan).last().unwrap(), "row-09999");
        assert!(plan.conflicts.iter().all(|conflict| {
            conflict.target.after.is_some() && conflict.source.after.is_some()
        }));
    }

    #[test]
    fn live_payload_comparison_only_reads_intersecting_changed_identity() {
        let same = StateCell::Value(r#"{"same":true}"#.into());
        let target = diff_with_payloads(
            vec![entry(
                "row-a",
                MergeDiffKind::Modified,
                Some(row("base-a", false)),
                Some(row("target-a", false)),
            )],
            [(
                ChangeId::for_test_label("target-a"),
                same.clone(),
                None,
            )],
        );
        let source = diff_with_payloads(
            vec![entry(
                "row-a",
                MergeDiffKind::Modified,
                Some(row("base-a", false)),
                Some(row("source-a", false)),
            )],
            [(ChangeId::for_test_label("source-a"), same, None)],
        );

        let plan = plan_merge(&target, &source).expect("payload comparison should plan");
        assert!(plan.picks.is_empty());
        assert!(plan.conflicts.is_empty());
    }

    #[test]
    fn unsorted_native_merge_is_rejected_instead_of_sorting_a_second_authority() {
        let source = diff(vec![
            entry(
                "row-b",
                MergeDiffKind::Added,
                None,
                Some(row("b", false)),
            ),
            entry(
                "row-a",
                MergeDiffKind::Added,
                None,
                Some(row("a", false)),
            ),
        ]);
        let error = plan_merge(&MergeDiff::default(), &source)
            .expect_err("native merge requires the ordered StateDiffEntry contract");

        assert!(error.message.contains("canonical StateKey order"));
    }

    #[test]
    fn unsorted_non_adjacent_duplicate_identity_is_rejected() {
        let duplicate = entry(
            "row-a",
            MergeDiffKind::Added,
            None,
            Some(row("a", false)),
        );
        let source = diff(vec![
            duplicate.clone(),
            entry(
                "row-b",
                MergeDiffKind::Added,
                None,
                Some(row("b", false)),
            ),
            duplicate,
        ]);
        let error = plan_merge(&MergeDiff::default(), &source)
            .expect_err("duplicate source identity must be rejected");

        assert!(error.message.contains("canonical StateKey order"));
    }

    #[test]
    fn adjacent_duplicate_identity_is_rejected() {
        let source = diff(vec![
            entry(
                "row-a",
                MergeDiffKind::Added,
                None,
                Some(row("a1", false)),
            ),
            entry(
                "row-a",
                MergeDiffKind::Added,
                None,
                Some(row("a2", false)),
            ),
        ]);
        let error = plan_merge(&MergeDiff::default(), &source)
            .expect_err("duplicate source identity must be rejected");

        assert!(error.message.contains("duplicate diff entry"));
    }

    #[test]
    fn merge_payload_batch_rejects_duplicate_change_ids() {
        let change_id = ChangeId::for_test_label("duplicate");
        let error = MergePayloadBatch::from_payloads([
            (change_id, StateCell::Tombstone, None),
            (change_id, StateCell::Tombstone, None),
        ])
        .expect_err("duplicate payload identitys must fail closed");

        assert!(error.message.contains("duplicate change id"));
    }

    #[test]
    fn native_tombstone_endpoint_is_removed_and_not_a_pick_without_after_row() {
        let source = MergeDiff {
            entries: vec![entry(
                "row-a",
                MergeDiffKind::Removed,
                Some(row("base", false)),
                None,
            )],
            payloads: MergePayloadBatch::default(),
        };
        let error = plan_merge(&MergeDiff::default(), &source)
            .expect_err("a missing authenticated tombstone endpoint must fail closed");

        assert!(error.message.contains("without a tombstone row"));
    }
}
