use std::collections::{BTreeMap, BTreeSet};

use crate::tracked_state::{
    MaterializedTrackedStateRow, TrackedStateDiff, TrackedStateDiffEntry, TrackedStateDiffIdentity,
};
use crate::LixError;

/// Planned tracked-state merge result.
///
/// This is intentionally a pure planner. It does not know about versions,
/// sessions, changelog writes, or live-state overlays. Callers provide two
/// diffs from the same merge base:
///
/// - `base -> target`: what the destination version changed.
/// - `base -> source`: what the incoming version changed.
///
/// The planner returns source-side patches that can be applied to the target
/// root plus first-class conflicts for identities changed differently on both
/// sides.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct TrackedStateMergePlan {
    pub(crate) patches: Vec<TrackedStateMergePatch>,
    pub(crate) conflicts: Vec<TrackedStateMergeConflict>,
}

/// One source-side patch to apply to the target root.
///
/// Merge patches are expressed as canonical change adoption, not as new row
/// writes. The projected row carries the target-root materialization shape,
/// including tombstones, while `change_id` preserves the source canonical
/// change identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TrackedStateMergePatch {
    Adopt {
        identity: TrackedStateDiffIdentity,
        change_id: String,
        projected_row: MaterializedTrackedStateRow,
    },
}

impl TrackedStateMergePatch {
    #[cfg(test)]
    pub(crate) fn identity(&self) -> &TrackedStateDiffIdentity {
        match self {
            Self::Adopt { identity, .. } => identity,
        }
    }

    pub(crate) fn change_id(&self) -> &str {
        match self {
            Self::Adopt { change_id, .. } => change_id,
        }
    }

    pub(crate) fn projected_row(&self) -> &MaterializedTrackedStateRow {
        match self {
            Self::Adopt { projected_row, .. } => projected_row,
        }
    }
}

/// One identity that both sides changed incompatibly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateMergeConflict {
    pub(crate) identity: TrackedStateDiffIdentity,
    pub(crate) target: TrackedStateDiffEntry,
    pub(crate) source: TrackedStateDiffEntry,
}

/// Plans a three-way tracked-state merge from two base-relative diffs.
///
/// This follows the same shape as prolly-tree merge systems: compare
/// `base -> target` and `base -> source` by identity, emit source-only patches
/// for the target root, ignore target-only changes, collapse convergent
/// changes, and report divergent same-identity changes as conflicts.
pub(crate) fn plan_merge(
    target_diff: &TrackedStateDiff,
    source_diff: &TrackedStateDiff,
) -> Result<TrackedStateMergePlan, LixError> {
    let target_by_identity = diff_by_identity(target_diff)?;
    let source_by_identity = diff_by_identity(source_diff)?;
    let identities = target_by_identity
        .keys()
        .chain(source_by_identity.keys())
        .cloned()
        .collect::<BTreeSet<_>>();

    let mut plan = TrackedStateMergePlan::default();
    for identity in identities {
        match (
            target_by_identity.get(&identity),
            source_by_identity.get(&identity),
        ) {
            (None, None) => {}
            (Some(_target), None) => {
                // Target already changed this identity. Source did not, so
                // there is nothing to apply.
            }
            (None, Some(source)) => {
                plan.patches
                    .push(adopt_source_change_patch(identity, source)?);
            }
            (Some(target), Some(source)) if same_final_state(target, source) => {
                // Both sides reached the same visible state. Keep target to
                // avoid writing duplicate source metadata.
            }
            (Some(target), Some(source)) => {
                plan.conflicts.push(TrackedStateMergeConflict {
                    identity,
                    target: (*target).clone(),
                    source: (*source).clone(),
                });
            }
        }
    }

    Ok(plan)
}

fn diff_by_identity(
    diff: &TrackedStateDiff,
) -> Result<BTreeMap<TrackedStateDiffIdentity, &TrackedStateDiffEntry>, LixError> {
    let mut entries = BTreeMap::new();
    for entry in &diff.entries {
        if entries.insert(entry.identity.clone(), entry).is_some() {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "tracked-state merge received duplicate diff entry for schema '{}' entity '{}'",
                    entry.identity.schema_key,
                    entry.identity.entity_id.as_json_array_text()?
                ),
            ));
        }
    }
    Ok(entries)
}

fn adopt_source_change_patch(
    identity: TrackedStateDiffIdentity,
    entry: &TrackedStateDiffEntry,
) -> Result<TrackedStateMergePatch, LixError> {
    let Some(row) = entry.after.clone() else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "tracked-state merge cannot apply source removal for schema '{}' entity '{}' without a tombstone row",
                entry.identity.schema_key,
                entry.identity.entity_id.as_json_array_text()?
            ),
        ));
    };
    Ok(TrackedStateMergePatch::Adopt {
        identity,
        change_id: row.change_id.clone(),
        projected_row: row,
    })
}

fn same_final_state(target: &TrackedStateDiffEntry, source: &TrackedStateDiffEntry) -> bool {
    match (target.after.as_ref(), source.after.as_ref()) {
        (None, None) => true,
        (Some(target), Some(source)) if !row_is_live(target) && !row_is_live(source) => true,
        (Some(target), Some(source)) if row_is_live(target) && row_is_live(source) => {
            tracked_row_payload_eq(target, source)
        }
        _ => false,
    }
}

fn row_is_live(row: &MaterializedTrackedStateRow) -> bool {
    row.snapshot_content.is_some()
}

fn tracked_row_payload_eq(
    left: &MaterializedTrackedStateRow,
    right: &MaterializedTrackedStateRow,
) -> bool {
    left.snapshot_content == right.snapshot_content && left.metadata == right.metadata
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity_identity::EntityIdentity;
    use crate::tracked_state::TrackedStateDiffKind;

    #[test]
    fn source_add_applies() {
        let plan = plan_merge(
            &TrackedStateDiff::default(),
            &diff(vec![entry(
                "entity-a",
                TrackedStateDiffKind::Added,
                None,
                Some(row("entity-a", "source")),
            )]),
        )
        .expect("merge should plan");

        assert_eq!(patch_ids(&plan), vec!["entity-a"]);
        assert!(plan.conflicts.is_empty());
    }

    #[test]
    fn source_modify_applies() {
        let plan = plan_merge(
            &TrackedStateDiff::default(),
            &diff(vec![entry(
                "entity-a",
                TrackedStateDiffKind::Modified,
                Some(row_with_value("entity-a", "base", "base")),
                Some(row_with_value("entity-a", "source", "source")),
            )]),
        )
        .expect("merge should plan");

        assert_eq!(patch_ids(&plan), vec!["entity-a"]);
        assert_eq!(
            plan.patches[0].projected_row().snapshot_content.as_deref(),
            Some("{\"value\":\"source\"}")
        );
        assert_eq!(plan.patches[0].change_id(), "source");
    }

    #[test]
    fn source_delete_applies_tombstone() {
        let plan = plan_merge(
            &TrackedStateDiff::default(),
            &diff(vec![entry(
                "entity-a",
                TrackedStateDiffKind::Removed,
                Some(row("entity-a", "base")),
                Some(tombstone("entity-a", "source-delete")),
            )]),
        )
        .expect("merge should plan");

        assert_eq!(patch_ids(&plan), vec!["entity-a"]);
        assert_eq!(plan.patches[0].projected_row().snapshot_content, None);
        assert_eq!(plan.patches[0].change_id(), "source-delete");
    }

    #[test]
    fn target_only_change_is_noop() {
        let plan = plan_merge(
            &diff(vec![entry(
                "entity-a",
                TrackedStateDiffKind::Modified,
                Some(row("entity-a", "base")),
                Some(row("entity-a", "target")),
            )]),
            &TrackedStateDiff::default(),
        )
        .expect("merge should plan");

        assert!(plan.patches.is_empty());
        assert!(plan.conflicts.is_empty());
    }

    #[test]
    fn both_sides_same_final_value_is_convergent_noop() {
        let target = entry(
            "entity-a",
            TrackedStateDiffKind::Modified,
            Some(row_with_value("entity-a", "base", "base")),
            Some(row_with_value("entity-a", "target", "same")),
        );
        let source = entry(
            "entity-a",
            TrackedStateDiffKind::Modified,
            Some(row_with_value("entity-a", "base", "base")),
            Some(row_with_value("entity-a", "source", "same")),
        );

        let plan = plan_merge(&diff(vec![target]), &diff(vec![source])).expect("merge should plan");

        assert!(plan.patches.is_empty());
        assert!(plan.conflicts.is_empty());
    }

    #[test]
    fn both_sides_delete_is_convergent_noop() {
        let target = entry(
            "entity-a",
            TrackedStateDiffKind::Removed,
            Some(row("entity-a", "base")),
            Some(tombstone("entity-a", "target-delete")),
        );
        let source = entry(
            "entity-a",
            TrackedStateDiffKind::Removed,
            Some(row("entity-a", "base")),
            Some(tombstone("entity-a", "source-delete")),
        );

        let plan = plan_merge(&diff(vec![target]), &diff(vec![source])).expect("merge should plan");

        assert!(plan.patches.is_empty());
        assert!(plan.conflicts.is_empty());
    }

    #[test]
    fn different_modifications_conflict() {
        let target = entry(
            "entity-a",
            TrackedStateDiffKind::Modified,
            Some(row_with_value("entity-a", "base", "base")),
            Some(row_with_value("entity-a", "target", "target")),
        );
        let source = entry(
            "entity-a",
            TrackedStateDiffKind::Modified,
            Some(row_with_value("entity-a", "base", "base")),
            Some(row_with_value("entity-a", "source", "source")),
        );

        let plan = plan_merge(&diff(vec![target]), &diff(vec![source])).expect("merge should plan");

        assert!(plan.patches.is_empty());
        assert_eq!(conflict_ids(&plan), vec!["entity-a"]);
    }

    #[test]
    fn delete_modify_conflicts() {
        let target = entry(
            "entity-a",
            TrackedStateDiffKind::Removed,
            Some(row("entity-a", "base")),
            Some(tombstone("entity-a", "target-delete")),
        );
        let source = entry(
            "entity-a",
            TrackedStateDiffKind::Modified,
            Some(row("entity-a", "base")),
            Some(row_with_value("entity-a", "source", "source")),
        );

        let plan = plan_merge(&diff(vec![target]), &diff(vec![source])).expect("merge should plan");

        assert_eq!(conflict_ids(&plan), vec!["entity-a"]);
    }

    #[test]
    fn modify_delete_conflicts() {
        let target = entry(
            "entity-a",
            TrackedStateDiffKind::Modified,
            Some(row("entity-a", "base")),
            Some(row_with_value("entity-a", "target", "target")),
        );
        let source = entry(
            "entity-a",
            TrackedStateDiffKind::Removed,
            Some(row("entity-a", "base")),
            Some(tombstone("entity-a", "source-delete")),
        );

        let plan = plan_merge(&diff(vec![target]), &diff(vec![source])).expect("merge should plan");

        assert_eq!(conflict_ids(&plan), vec!["entity-a"]);
    }

    #[test]
    fn source_removal_without_tombstone_errors() {
        let error = plan_merge(
            &TrackedStateDiff::default(),
            &diff(vec![entry(
                "entity-a",
                TrackedStateDiffKind::Removed,
                Some(row("entity-a", "base")),
                None,
            )]),
        )
        .expect_err("merge should reject impossible source removal");

        assert!(error.message.contains("without a tombstone row"));
    }

    #[test]
    fn patch_and_conflict_order_is_deterministic_by_identity() {
        let target = diff(vec![entry(
            "entity-b",
            TrackedStateDiffKind::Modified,
            Some(row_with_value("entity-b", "base", "base")),
            Some(row_with_value("entity-b", "target", "target")),
        )]);
        let source = diff(vec![
            entry(
                "entity-c",
                TrackedStateDiffKind::Added,
                None,
                Some(row("entity-c", "source-c")),
            ),
            entry(
                "entity-a",
                TrackedStateDiffKind::Added,
                None,
                Some(row("entity-a", "source-a")),
            ),
            entry(
                "entity-b",
                TrackedStateDiffKind::Modified,
                Some(row_with_value("entity-b", "base", "base")),
                Some(row_with_value("entity-b", "source", "source")),
            ),
        ]);

        let plan = plan_merge(&target, &source).expect("merge should plan");

        assert_eq!(patch_ids(&plan), vec!["entity-a", "entity-c"]);
        assert_eq!(conflict_ids(&plan), vec!["entity-b"]);
    }

    fn diff(entries: Vec<TrackedStateDiffEntry>) -> TrackedStateDiff {
        TrackedStateDiff { entries }
    }

    fn entry(
        entity_id: &str,
        kind: TrackedStateDiffKind,
        before: Option<MaterializedTrackedStateRow>,
        after: Option<MaterializedTrackedStateRow>,
    ) -> TrackedStateDiffEntry {
        TrackedStateDiffEntry {
            identity: TrackedStateDiffIdentity {
                schema_key: "test_schema".to_string(),
                entity_id: EntityIdentity::single(entity_id),
                file_id: None,
            },
            kind,
            before,
            after,
        }
    }

    fn patch_ids(plan: &TrackedStateMergePlan) -> Vec<String> {
        plan.patches
            .iter()
            .map(|entry| {
                entry
                    .identity()
                    .entity_id
                    .as_single_string_owned()
                    .expect("identity")
            })
            .collect()
    }

    fn conflict_ids(plan: &TrackedStateMergePlan) -> Vec<String> {
        plan.conflicts
            .iter()
            .map(|entry| {
                entry
                    .identity
                    .entity_id
                    .as_single_string_owned()
                    .expect("identity")
            })
            .collect()
    }

    fn tombstone(entity_id: &str, change_id: &str) -> MaterializedTrackedStateRow {
        let mut row = row(entity_id, change_id);
        row.snapshot_content = None;
        row
    }

    fn row(entity_id: &str, change_id: &str) -> MaterializedTrackedStateRow {
        row_with_value(entity_id, change_id, "value")
    }

    fn row_with_value(
        entity_id: &str,
        change_id: &str,
        value: &str,
    ) -> MaterializedTrackedStateRow {
        MaterializedTrackedStateRow {
            entity_id: EntityIdentity::single(entity_id),
            schema_key: "test_schema".to_string(),
            file_id: None,
            snapshot_content: Some(format!("{{\"value\":\"{value}\"}}")),
            metadata: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            change_id: change_id.to_string(),
            commit_id: change_id.replace("change", "commit"),
        }
    }
}
