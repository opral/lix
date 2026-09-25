use std::cmp::Ordering;
use std::ops::Deref;

use crate::LixError;
use crate::changelog::ChangeId;
use crate::tracked_state::{
    TrackedStateDiff, TrackedStateDiffEntry, TrackedStateDiffIdentity, TrackedStateDiffRow,
    TrackedStatePayloadBatch, TrackedStatePayloadRef,
};

/// Planned tracked-state merge result.
///
/// This is intentionally a pure planner. It does not know about branches,
/// sessions, changelog writes, or live-state overlays. Callers provide two
/// diffs from the same merge base:
///
/// - `base -> target`: what the destination branch changed.
/// - `base -> source`: what the incoming branch changed.
///
/// The planner returns source-side picks plus first-class conflicts for
/// identities changed differently on both sides.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct TrackedStateMergePlan {
    pub(crate) picks: TrackedStateMergePickBatch,
    pub(crate) conflicts: TrackedStateMergeConflictBatch,
}

/// Contiguous fixed-metadata rows for source-side merge selections.
///
/// Every row retains a compact identity handle into the source diff's shared
/// identity columns. The batch therefore owns one descriptor buffer and no
/// row-owned schema, file, row, or payload allocation.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct TrackedStateMergePickBatch {
    rows: Vec<TrackedStateMergePick>,
}

/// Contiguous fixed-metadata rows for divergent merge identities.
///
/// Conflict entries and their before/after rows all retain the target diff's
/// shared identity handle. Cloning or iterating the batch never clones key
/// strings, row keys, or JSON payloads.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct TrackedStateMergeConflictBatch {
    rows: Vec<TrackedStateMergeConflict>,
}

macro_rules! impl_merge_row_batch {
    ($batch:ident, $row:ident) => {
        impl $batch {
            fn reserve_exact_once(&mut self, row_count: usize) {
                if self.rows.capacity() == 0 {
                    self.rows.reserve_exact(row_count);
                }
            }

            fn push(&mut self, row: $row) {
                self.rows.push(row);
            }

            #[cfg(test)]
            pub(crate) fn large_buffer_count(&self) -> usize {
                usize::from(!self.rows.is_empty())
            }

            #[cfg(test)]
            pub(crate) fn row_capacity(&self) -> usize {
                self.rows.capacity()
            }
        }

        impl Deref for $batch {
            type Target = [$row];

            fn deref(&self) -> &Self::Target {
                &self.rows
            }
        }

        impl From<Vec<$row>> for $batch {
            fn from(rows: Vec<$row>) -> Self {
                Self { rows }
            }
        }

        impl FromIterator<$row> for $batch {
            fn from_iter<T: IntoIterator<Item = $row>>(iter: T) -> Self {
                Self {
                    rows: iter.into_iter().collect(),
                }
            }
        }

        impl<'a> IntoIterator for &'a $batch {
            type Item = &'a $row;
            type IntoIter = std::slice::Iter<'a, $row>;

            fn into_iter(self) -> Self::IntoIter {
                self.rows.iter()
            }
        }
    };
}

impl_merge_row_batch!(TrackedStateMergePickBatch, TrackedStateMergePick);
impl_merge_row_batch!(TrackedStateMergeConflictBatch, TrackedStateMergeConflict);

/// One source-side change selected for the merge result.
///
/// Merge picks describe source-side state that will be selected into
/// the target root. The selected row carries the target-root materialization
/// shape, including tombstones.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateMergePick {
    pub(crate) identity: TrackedStateDiffIdentity,
    pub(crate) change_id: ChangeId,
    pub(crate) selected_row: TrackedStateDiffRow,
}

impl TrackedStateMergePick {
    #[cfg(test)]
    pub(crate) fn identity(&self) -> &TrackedStateDiffIdentity {
        &self.identity
    }

    #[cfg(test)]
    pub(crate) fn source_change_id(&self) -> String {
        self.change_id.to_string()
    }

    #[cfg(test)]
    pub(crate) fn source_row(&self) -> &TrackedStateDiffRow {
        &self.selected_row
    }
}

/// One identity that both sides changed incompatibly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateMergeConflict {
    pub(crate) identity: TrackedStateDiffIdentity,
    pub(crate) target: TrackedStateDiffEntry,
    pub(crate) source: TrackedStateDiffEntry,
}

/// Change ids whose payloads the merge planner needs for cross-change
/// equality (live/live after-pairs with differing change ids).
pub(crate) fn merge_payload_fallback_ids(
    target_diff: &TrackedStateDiff,
    source_diff: &TrackedStateDiff,
) -> Result<Vec<ChangeId>, LixError> {
    let mut ids = match SortedMergeInputs::new(target_diff, source_diff)? {
        SortedMergeInputs::Borrowed { target, source } => {
            sorted_merge_payload_fallback_ids(target, source)
        }
        SortedMergeInputs::Fallback {
            entries,
            target_len,
        } => {
            let (target, source) = entries.split_at(target_len);
            sorted_merge_payload_fallback_ids(target, source)
        }
    };
    ids.retain(|change_id| {
        !target_diff.payloads().contains(*change_id) && !source_diff.payloads().contains(*change_id)
    });
    Ok(ids)
}

/// Collects payload ids only for identities that can reach the expensive
/// live/live cross-change equality comparison.
///
/// Tree diffs are already identity sorted, so the production path performs
/// one linear intersection without an index or candidate buffer. Defensive
/// unsorted callers are normalized once by [`SortedMergeInputs`].
fn sorted_merge_payload_fallback_ids<T, S>(
    target_entries: &[T],
    source_entries: &[S],
) -> Vec<ChangeId>
where
    T: MergeDiffEntry,
    S: MergeDiffEntry,
{
    let mut ids = Vec::new();
    let mut target_index = 0;
    let mut source_index = 0;
    while target_index < target_entries.len() && source_index < source_entries.len() {
        let target = target_entries[target_index].as_diff_entry();
        let source = source_entries[source_index].as_diff_entry();
        match target.identity.cmp(&source.identity) {
            Ordering::Less => target_index += 1,
            Ordering::Greater => source_index += 1,
            Ordering::Equal => {
                if let (Some(target), Some(source)) = (target.after.as_ref(), source.after.as_ref())
                    && !target.deleted
                    && !source.deleted
                    && target.change_id != source.change_id
                {
                    ids.push(target.change_id);
                    ids.push(source.change_id);
                }
                target_index += 1;
                source_index += 1;
            }
        }
    }
    ids
}

/// Plans a three-way tracked-state merge from two base-relative diffs.
///
/// This follows the same shape as prolly-tree merge systems: compare
/// `base -> target` and `base -> source` by identity, emit source-only picks
/// for the target root, ignore target-only changes, collapse convergent
/// changes, and report divergent same-identity changes as conflicts.
pub(crate) fn plan_merge(
    target_diff: &TrackedStateDiff,
    source_diff: &TrackedStateDiff,
    fallback_payloads: &TrackedStatePayloadBatch,
) -> Result<TrackedStateMergePlan, LixError> {
    let payloads = MergePayloadOwners {
        target: target_diff.payloads(),
        source: source_diff.payloads(),
        fallback: fallback_payloads,
    };
    match SortedMergeInputs::new(target_diff, source_diff)? {
        SortedMergeInputs::Borrowed { target, source } => {
            plan_sorted_merge(target, source, &payloads)
        }
        SortedMergeInputs::Fallback {
            entries,
            target_len,
        } => {
            let (target, source) = entries.split_at(target_len);
            plan_sorted_merge(target, source, &payloads)
        }
    }
}

/// Borrowed payload owners available to one merge analysis.
///
/// Target and source diffs retain the records loaded during diff validation.
/// The fallback owner is only populated by defensive callers that construct
/// payload-light diffs outside the durable diff path.
struct MergePayloadOwners<'a> {
    target: &'a TrackedStatePayloadBatch,
    source: &'a TrackedStatePayloadBatch,
    fallback: &'a TrackedStatePayloadBatch,
}

impl MergePayloadOwners<'_> {
    fn get(&self, change_id: ChangeId) -> Option<TrackedStatePayloadRef<'_>> {
        self.target
            .get(change_id)
            .or_else(|| self.source.get(change_id))
            .or_else(|| self.fallback.get(change_id))
    }
}

/// Identity-sorted diff inputs consumed by the two-pointer merge.
///
/// Tracked-state tree diffs are emitted in key order, so the production path
/// borrows both entry slices without building an index. Hand-built or
/// defensive unsorted inputs share one contiguous reference buffer: its two
/// partitions are sorted independently before merge planning.
enum SortedMergeInputs<'a> {
    Borrowed {
        target: &'a [TrackedStateDiffEntry],
        source: &'a [TrackedStateDiffEntry],
    },
    Fallback {
        entries: Vec<&'a TrackedStateDiffEntry>,
        target_len: usize,
    },
}

impl<'a> SortedMergeInputs<'a> {
    fn new(
        target_diff: &'a TrackedStateDiff,
        source_diff: &'a TrackedStateDiff,
    ) -> Result<Self, LixError> {
        let target_is_sorted = entries_are_strictly_sorted(&target_diff.entries)?;
        let source_is_sorted = entries_are_strictly_sorted(&source_diff.entries)?;
        if target_is_sorted && source_is_sorted {
            return Ok(Self::Borrowed {
                target: &target_diff.entries,
                source: &source_diff.entries,
            });
        }

        let target_len = target_diff.entries.len();
        let mut entries = Vec::with_capacity(target_len.saturating_add(source_diff.entries.len()));
        entries.extend(&target_diff.entries);
        entries.extend(&source_diff.entries);
        entries[..target_len].sort_unstable_by(|left, right| left.identity.cmp(&right.identity));
        entries[target_len..].sort_unstable_by(|left, right| left.identity.cmp(&right.identity));
        reject_adjacent_duplicates(&entries[..target_len])?;
        reject_adjacent_duplicates(&entries[target_len..])?;
        Ok(Self::Fallback {
            entries,
            target_len,
        })
    }
}

/// Returns `true` for the borrowed merge path and `false` when sorting is
/// required. Adjacent duplicates are rejected immediately; non-adjacent
/// duplicates are rejected after the fallback sort.
fn entries_are_strictly_sorted(entries: &[TrackedStateDiffEntry]) -> Result<bool, LixError> {
    let mut is_sorted = true;
    for pair in entries.windows(2) {
        match pair[0].identity.cmp(&pair[1].identity) {
            Ordering::Less => {}
            Ordering::Equal => return Err(duplicate_diff_entry_error(&pair[1])),
            Ordering::Greater => is_sorted = false,
        }
    }
    Ok(is_sorted)
}

fn reject_adjacent_duplicates(entries: &[&TrackedStateDiffEntry]) -> Result<(), LixError> {
    for pair in entries.windows(2) {
        if pair[0].identity == pair[1].identity {
            return Err(duplicate_diff_entry_error(pair[1]));
        }
    }
    Ok(())
}

fn duplicate_diff_entry_error(entry: &TrackedStateDiffEntry) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!(
            "tracked-state merge received duplicate diff entry for schema '{}' row '{}'",
            entry.identity.schema_key(),
            entry
                .identity
                .row_pk()
                .as_json_array_text()
                .unwrap_or_else(|_| "<invalid row pk>".to_string())
        ),
    )
}

trait MergeDiffEntry {
    fn as_diff_entry(&self) -> &TrackedStateDiffEntry;
}

impl MergeDiffEntry for TrackedStateDiffEntry {
    fn as_diff_entry(&self) -> &TrackedStateDiffEntry {
        self
    }
}

impl MergeDiffEntry for &TrackedStateDiffEntry {
    fn as_diff_entry(&self) -> &TrackedStateDiffEntry {
        self
    }
}

fn plan_sorted_merge<T, S>(
    target_entries: &[T],
    source_entries: &[S],
    payloads: &MergePayloadOwners<'_>,
) -> Result<TrackedStateMergePlan, LixError>
where
    T: MergeDiffEntry,
    S: MergeDiffEntry,
{
    let mut plan = TrackedStateMergePlan::default();
    let (pick_count, conflict_count) =
        count_sorted_merge_outputs(target_entries, source_entries, payloads);
    plan.picks.reserve_exact_once(pick_count);
    plan.conflicts.reserve_exact_once(conflict_count);

    let mut target_index = 0;
    let mut source_index = 0;
    while target_index < target_entries.len() && source_index < source_entries.len() {
        let target = target_entries[target_index].as_diff_entry();
        let source = source_entries[source_index].as_diff_entry();
        match target.identity.cmp(&source.identity) {
            Ordering::Less => {
                // Target already changed this identity. Source did not, so
                // there is nothing to pick.
                target_index += 1;
            }
            Ordering::Greater => {
                plan.picks.push(source_change_pick(source)?);
                source_index += 1;
            }
            Ordering::Equal => {
                if !same_final_state(target, source, payloads) {
                    // Keep the target entry's identity owner explicitly, then
                    // rebind both cloned sides to it so one conflict stores
                    // schema/file/row buffers once.
                    let identity = target.identity.clone();
                    plan.conflicts.push(TrackedStateMergeConflict {
                        target: clone_entry_with_identity(target, &identity),
                        source: clone_entry_with_identity(source, &identity),
                        identity,
                    });
                }
                target_index += 1;
                source_index += 1;
            }
        }
    }

    for source in &source_entries[source_index..] {
        plan.picks.push(source_change_pick(source.as_diff_entry())?);
    }
    debug_assert_eq!(plan.picks.len(), pick_count);
    debug_assert_eq!(plan.conflicts.len(), conflict_count);
    Ok(plan)
}

/// Counts the sparse merge outputs before allocating their descriptor columns.
///
/// The merge inputs are already sorted and payload equality is read-only, so
/// a second linear pass avoids reserving all remaining input rows when only an
/// early pick or conflict survives a mostly convergent merge.
fn count_sorted_merge_outputs<T, S>(
    target_entries: &[T],
    source_entries: &[S],
    payloads: &MergePayloadOwners<'_>,
) -> (usize, usize)
where
    T: MergeDiffEntry,
    S: MergeDiffEntry,
{
    let mut pick_count = 0;
    let mut conflict_count = 0;
    let mut target_index = 0;
    let mut source_index = 0;
    while target_index < target_entries.len() && source_index < source_entries.len() {
        let target = target_entries[target_index].as_diff_entry();
        let source = source_entries[source_index].as_diff_entry();
        match target.identity.cmp(&source.identity) {
            Ordering::Less => target_index += 1,
            Ordering::Greater => {
                pick_count += 1;
                source_index += 1;
            }
            Ordering::Equal => {
                conflict_count += usize::from(!same_final_state(target, source, payloads));
                target_index += 1;
                source_index += 1;
            }
        }
    }
    pick_count += source_entries.len() - source_index;
    (pick_count, conflict_count)
}

fn source_change_pick(entry: &TrackedStateDiffEntry) -> Result<TrackedStateMergePick, LixError> {
    let Some(mut row) = entry.after.clone() else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "tracked-state merge cannot pick source removal for schema '{}' row '{}' without a tombstone row",
                entry.identity.schema_key(),
                entry.identity.row_pk().as_json_array_text()?
            ),
        ));
    };
    let identity = entry.identity.clone();
    row.identity = identity.clone();
    Ok(TrackedStateMergePick {
        identity,
        change_id: row.change_id,
        selected_row: row,
    })
}

fn clone_entry_with_identity(
    entry: &TrackedStateDiffEntry,
    identity: &TrackedStateDiffIdentity,
) -> TrackedStateDiffEntry {
    let mut entry = entry.clone();
    entry.identity = identity.clone();
    if let Some(before) = entry.before.as_mut() {
        before.identity = identity.clone();
    }
    if let Some(after) = entry.after.as_mut() {
        after.identity = identity.clone();
    }
    entry
}

fn same_final_state(
    target: &TrackedStateDiffEntry,
    source: &TrackedStateDiffEntry,
    payloads: &MergePayloadOwners<'_>,
) -> bool {
    match (target.after.as_ref(), source.after.as_ref()) {
        (None, None) => true,
        (Some(target), Some(source)) if !row_is_live(target) && !row_is_live(source) => true,
        (Some(target), Some(source)) if row_is_live(target) && row_is_live(source) => {
            tracked_row_payload_eq(target, source, payloads)
        }
        _ => false,
    }
}

fn row_is_live(row: &TrackedStateDiffRow) -> bool {
    !row.deleted
}

fn tracked_row_payload_eq(
    left: &TrackedStateDiffRow,
    right: &TrackedStateDiffRow,
    payloads: &MergePayloadOwners<'_>,
) -> bool {
    if left.change_id == right.change_id {
        return true;
    }
    // A change id missing from the map compares unequal: the conservative
    // direction (a conflict is surfaced rather than a difference hidden).
    match (payloads.get(left.change_id), payloads.get(right.change_id)) {
        (Some(left), Some(right)) => {
            left.snapshot == right.snapshot && left.metadata == right.metadata
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::CommitId;
    use crate::row_pk::RowPk;
    use crate::tracked_state::TrackedStateDiffKind;

    fn change_id(label: &str) -> String {
        ChangeId::for_test_label(label).to_string()
    }

    #[test]
    fn source_add_applies() {
        let plan = plan_merge(
            &TrackedStateDiff::default(),
            &diff(vec![entry(
                "row-a",
                TrackedStateDiffKind::Added,
                None,
                Some(row("row-a", "source")),
            )]),
            &TrackedStatePayloadBatch::default(),
        )
        .expect("merge should plan");

        assert_eq!(pick_ids(&plan), vec!["row-a"]);
        assert!(plan.conflicts.is_empty());
    }

    #[test]
    fn source_modify_applies() {
        let plan = plan_merge(
            &TrackedStateDiff::default(),
            &diff(vec![entry(
                "row-a",
                TrackedStateDiffKind::Modified,
                Some(row_with_value("row-a", "base")),
                Some(row_with_value("row-a", "source")),
            )]),
            &TrackedStatePayloadBatch::default(),
        )
        .expect("merge should plan");

        assert_eq!(pick_ids(&plan), vec!["row-a"]);
        assert!(!plan.picks[0].source_row().deleted);
        assert_eq!(plan.picks[0].source_change_id(), change_id("source"));
    }

    #[test]
    fn source_delete_applies_tombstone() {
        let plan = plan_merge(
            &TrackedStateDiff::default(),
            &diff(vec![entry(
                "row-a",
                TrackedStateDiffKind::Removed,
                Some(row("row-a", "base")),
                Some(tombstone("row-a", "source-delete")),
            )]),
            &TrackedStatePayloadBatch::default(),
        )
        .expect("merge should plan");

        assert_eq!(pick_ids(&plan), vec!["row-a"]);
        assert!(plan.picks[0].source_row().deleted);
        assert_eq!(plan.picks[0].source_change_id(), change_id("source-delete"));
    }

    #[test]
    fn target_only_change_is_noop() {
        let plan = plan_merge(
            &diff(vec![entry(
                "row-a",
                TrackedStateDiffKind::Modified,
                Some(row("row-a", "base")),
                Some(row("row-a", "target")),
            )]),
            &TrackedStateDiff::default(),
            &TrackedStatePayloadBatch::default(),
        )
        .expect("merge should plan");

        assert!(plan.picks.is_empty());
        assert!(plan.conflicts.is_empty());
    }

    #[test]
    fn both_sides_same_final_value_is_convergent_noop() {
        let target = entry(
            "row-a",
            TrackedStateDiffKind::Modified,
            Some(row_with_value("row-a", "base")),
            Some(row_with_value("row-a", "target")),
        );
        let source = entry(
            "row-a",
            TrackedStateDiffKind::Modified,
            Some(row_with_value("row-a", "base")),
            Some(row_with_value("row-a", "source")),
        );

        // Different change ids with identical content: each diff retains the
        // payload owner loaded during validation, so merge analysis must not
        // reload either record.
        let same = b"same-native-snapshot".to_vec();
        let target = TrackedStateDiff::from_entries_with_payloads(
            vec![target],
            TrackedStatePayloadBatch::from_payloads([(
                ChangeId::for_test_label("target"),
                Some(same.clone()),
                None,
            )])
            .expect("target payload batch should seal"),
        );
        let source = TrackedStateDiff::from_entries_with_payloads(
            vec![source],
            TrackedStatePayloadBatch::from_payloads([(
                ChangeId::for_test_label("source"),
                Some(same),
                None,
            )])
            .expect("source payload batch should seal"),
        );
        assert!(
            merge_payload_fallback_ids(&target, &source)
                .expect("retained payloads should prepare")
                .is_empty(),
            "retained diff owners must eliminate merge payload reloads"
        );
        let plan = plan_merge(&target, &source, &TrackedStatePayloadBatch::default())
            .expect("merge should plan from retained owners");

        assert!(plan.picks.is_empty());
        assert!(plan.conflicts.is_empty());
    }

    #[test]
    fn both_sides_delete_is_convergent_noop() {
        let target = entry(
            "row-a",
            TrackedStateDiffKind::Removed,
            Some(row("row-a", "base")),
            Some(tombstone("row-a", "target-delete")),
        );
        let source = entry(
            "row-a",
            TrackedStateDiffKind::Removed,
            Some(row("row-a", "base")),
            Some(tombstone("row-a", "source-delete")),
        );

        let plan = plan_merge(
            &diff(vec![target]),
            &diff(vec![source]),
            &TrackedStatePayloadBatch::default(),
        )
        .expect("merge should plan");

        assert!(plan.picks.is_empty());
        assert!(plan.conflicts.is_empty());
    }

    #[test]
    fn different_modifications_conflict() {
        let target = entry(
            "row-a",
            TrackedStateDiffKind::Modified,
            Some(row_with_value("row-a", "base")),
            Some(row_with_value("row-a", "target")),
        );
        let source = entry(
            "row-a",
            TrackedStateDiffKind::Modified,
            Some(row_with_value("row-a", "base")),
            Some(row_with_value("row-a", "source")),
        );

        let plan = plan_merge(
            &diff(vec![target]),
            &diff(vec![source]),
            &TrackedStatePayloadBatch::default(),
        )
        .expect("merge should plan");

        assert!(plan.picks.is_empty());
        assert_eq!(conflict_ids(&plan), vec!["row-a"]);
    }

    #[test]
    fn delete_modify_conflicts() {
        let target = entry(
            "row-a",
            TrackedStateDiffKind::Removed,
            Some(row("row-a", "base")),
            Some(tombstone("row-a", "target-delete")),
        );
        let source = entry(
            "row-a",
            TrackedStateDiffKind::Modified,
            Some(row("row-a", "base")),
            Some(row_with_value("row-a", "source")),
        );

        let plan = plan_merge(
            &diff(vec![target]),
            &diff(vec![source]),
            &TrackedStatePayloadBatch::default(),
        )
        .expect("merge should plan");

        assert_eq!(conflict_ids(&plan), vec!["row-a"]);
    }

    #[test]
    fn modify_delete_conflicts() {
        let target = entry(
            "row-a",
            TrackedStateDiffKind::Modified,
            Some(row("row-a", "base")),
            Some(row_with_value("row-a", "target")),
        );
        let source = entry(
            "row-a",
            TrackedStateDiffKind::Removed,
            Some(row("row-a", "base")),
            Some(tombstone("row-a", "source-delete")),
        );

        let plan = plan_merge(
            &diff(vec![target]),
            &diff(vec![source]),
            &TrackedStatePayloadBatch::default(),
        )
        .expect("merge should plan");

        assert_eq!(conflict_ids(&plan), vec!["row-a"]);
    }

    #[test]
    fn source_removal_without_tombstone_errors() {
        let error = plan_merge(
            &TrackedStateDiff::default(),
            &diff(vec![entry(
                "row-a",
                TrackedStateDiffKind::Removed,
                Some(row("row-a", "base")),
                None,
            )]),
            &TrackedStatePayloadBatch::default(),
        )
        .expect_err("merge should reject impossible source removal");

        assert!(error.message.contains("without a tombstone row"));
    }

    #[test]
    fn pick_and_conflict_order_is_deterministic_by_identity() {
        let target = diff(vec![entry(
            "row-b",
            TrackedStateDiffKind::Modified,
            Some(row_with_value("row-b", "base")),
            Some(row_with_value("row-b", "target")),
        )]);
        let source = diff(vec![
            entry(
                "row-c",
                TrackedStateDiffKind::Added,
                None,
                Some(row("row-c", "source-c")),
            ),
            entry(
                "row-a",
                TrackedStateDiffKind::Added,
                None,
                Some(row("row-a", "source-a")),
            ),
            entry(
                "row-b",
                TrackedStateDiffKind::Modified,
                Some(row_with_value("row-b", "base")),
                Some(row_with_value("row-b", "source")),
            ),
        ]);

        let plan = plan_merge(&target, &source, &TrackedStatePayloadBatch::default())
            .expect("merge should plan");

        assert_eq!(pick_ids(&plan), vec!["row-a", "row-c"]);
        assert_eq!(conflict_ids(&plan), vec!["row-b"]);
    }

    #[test]
    fn large_sorted_merge_borrows_both_diff_batches() {
        let target = TrackedStateDiff::default();
        let source = diff(
            (0..10_000)
                .map(|index| {
                    let row_pk = format!("row-{index:05}");
                    entry(
                        &row_pk,
                        TrackedStateDiffKind::Added,
                        None,
                        Some(row(&row_pk, &format!("source-{index:05}"))),
                    )
                })
                .collect(),
        );

        match SortedMergeInputs::new(&target, &source).expect("merge inputs should prepare") {
            SortedMergeInputs::Borrowed {
                target: borrowed_target,
                source: borrowed_source,
            } => {
                assert_eq!(borrowed_target.as_ptr(), target.entries.as_ptr());
                assert_eq!(borrowed_source.as_ptr(), source.entries.as_ptr());
            }
            SortedMergeInputs::Fallback { .. } => {
                panic!("identity-sorted engine batches must not allocate a sorting fallback");
            }
        }

        let plan = plan_merge(&target, &source, &TrackedStatePayloadBatch::default())
            .expect("large sorted merge should plan");
        assert_eq!(plan.picks.len(), source.entries.len());
        assert!(plan.conflicts.is_empty());
    }

    #[test]
    fn early_pick_before_hundred_thousand_convergent_rows_reserves_one_output() {
        const CONVERGENT_ROW_COUNT: usize = 100_000;
        let target = TrackedStateDiff::from_entries(
            integer_identity_batch(0, CONVERGENT_ROW_COUNT)
                .into_iter()
                .enumerate()
                .map(|(index, identity)| {
                    live_after_entry(
                        identity,
                        TrackedStateDiffKind::Modified,
                        numbered_change_id(index),
                    )
                })
                .collect(),
        );
        let mut source_entries = Vec::with_capacity(CONVERGENT_ROW_COUNT + 1);
        source_entries.push(live_after_entry(
            integer_identity_batch(-1, 1)
                .pop()
                .expect("early pick identity"),
            TrackedStateDiffKind::Added,
            ChangeId::new(uuid::Uuid::from_u128(u128::MAX)),
        ));
        source_entries.extend(
            integer_identity_batch(0, CONVERGENT_ROW_COUNT)
                .into_iter()
                .enumerate()
                .map(|(index, identity)| {
                    live_after_entry(
                        identity,
                        TrackedStateDiffKind::Modified,
                        numbered_change_id(index),
                    )
                }),
        );
        let source = TrackedStateDiff::from_entries(source_entries);

        let plan = plan_merge(&target, &source, &TrackedStatePayloadBatch::default())
            .expect("sparse pick merge should plan");

        assert_eq!(plan.picks.len(), 1);
        assert_eq!(plan.picks.row_capacity(), 1);
        assert_eq!(plan.picks[0].identity.row_pk(), &integer_row_pk(-1));
        assert!(plan.conflicts.is_empty());
        assert_eq!(plan.conflicts.row_capacity(), 0);
    }

    #[test]
    fn early_conflict_before_hundred_thousand_convergent_rows_reserves_one_output() {
        const CONVERGENT_ROW_COUNT: usize = 100_000;
        let target = TrackedStateDiff::from_entries(
            integer_identity_batch(-1, CONVERGENT_ROW_COUNT + 1)
                .into_iter()
                .enumerate()
                .map(|(index, identity)| {
                    let change_id = if index == 0 {
                        ChangeId::new(uuid::Uuid::from_u128(u128::MAX - 1))
                    } else {
                        numbered_change_id(index - 1)
                    };
                    live_after_entry(identity, TrackedStateDiffKind::Modified, change_id)
                })
                .collect(),
        );
        let source = TrackedStateDiff::from_entries(
            integer_identity_batch(-1, CONVERGENT_ROW_COUNT + 1)
                .into_iter()
                .enumerate()
                .map(|(index, identity)| {
                    let change_id = if index == 0 {
                        ChangeId::new(uuid::Uuid::from_u128(u128::MAX))
                    } else {
                        numbered_change_id(index - 1)
                    };
                    live_after_entry(identity, TrackedStateDiffKind::Modified, change_id)
                })
                .collect(),
        );

        let plan = plan_merge(&target, &source, &TrackedStatePayloadBatch::default())
            .expect("sparse conflict merge should plan");

        assert!(plan.picks.is_empty());
        assert_eq!(plan.picks.row_capacity(), 0);
        assert_eq!(plan.conflicts.len(), 1);
        assert_eq!(plan.conflicts.row_capacity(), 1);
        assert_eq!(plan.conflicts[0].identity.row_pk(), &integer_row_pk(-1));
    }

    #[test]
    fn ten_thousand_disjoint_sorted_rows_need_no_payload_fallback_reads() {
        const ROW_COUNT: usize = 10_000;
        let batched_diff = |identity_prefix: &str, change_prefix: &str| {
            let identities = TrackedStateDiffIdentity::from_key_batch(
                (0..ROW_COUNT)
                    .map(|index| crate::tracked_state::TrackedStateKey {
                        schema_key: "shared_schema".to_string(),
                        file_id: None,
                        row_pk: RowPk::single(format!("{identity_prefix}-{index:05}")),
                    })
                    .collect(),
            )
            .expect("identity batch should seal");
            let timestamp = crate::common::LixTimestamp::from_unix_millis_utc_lossy(0);
            let commit_id = CommitId::for_test_label(change_prefix);
            TrackedStateDiff::from_entries(
                identities
                    .into_iter()
                    .enumerate()
                    .map(|(index, identity)| {
                        let after = TrackedStateDiffRow {
                            identity: identity.clone(),
                            deleted: false,
                            created_at: timestamp,
                            updated_at: timestamp,
                            change_id: ChangeId::for_test_label(&format!(
                                "{change_prefix}-{index:05}"
                            )),
                            commit_id,
                        };
                        TrackedStateDiffEntry {
                            identity,
                            kind: TrackedStateDiffKind::Added,
                            before: None,
                            after: Some(after),
                        }
                    })
                    .collect(),
            )
        };
        let target = batched_diff("row-a", "target");
        let source = batched_diff("row-b", "source");

        let fallback_ids = merge_payload_fallback_ids(&target, &source)
            .expect("disjoint sorted batches should intersect");

        assert!(
            fallback_ids.is_empty(),
            "disjoint identities must not issue changelog payload reads"
        );
    }

    #[test]
    fn unsorted_payload_fallback_intersects_only_live_differing_changes() {
        let mut target_deleted = row("row-c", "target-c");
        target_deleted.deleted = true;
        let target = diff(vec![
            entry(
                "row-c",
                TrackedStateDiffKind::Removed,
                None,
                Some(target_deleted),
            ),
            entry(
                "row-a",
                TrackedStateDiffKind::Modified,
                None,
                Some(row("row-a", "target-a")),
            ),
            entry(
                "row-b",
                TrackedStateDiffKind::Modified,
                None,
                Some(row("row-b", "same-b")),
            ),
        ]);
        let source = diff(vec![
            entry(
                "row-b",
                TrackedStateDiffKind::Modified,
                None,
                Some(row("row-b", "same-b")),
            ),
            entry(
                "row-c",
                TrackedStateDiffKind::Modified,
                None,
                Some(row("row-c", "source-c")),
            ),
            entry(
                "row-a",
                TrackedStateDiffKind::Modified,
                None,
                Some(row("row-a", "source-a")),
            ),
            entry(
                "row-d",
                TrackedStateDiffKind::Added,
                None,
                Some(row("row-d", "source-d")),
            ),
        ]);

        let fallback_ids =
            merge_payload_fallback_ids(&target, &source).expect("unsorted inputs should normalize");

        assert_eq!(
            fallback_ids,
            vec![
                ChangeId::for_test_label("target-a"),
                ChangeId::for_test_label("source-a")
            ]
        );
    }

    #[test]
    fn ten_thousand_conflicts_retain_one_shared_identity_and_metadata_batch() {
        const ROW_COUNT: usize = 10_000;
        let keys = || {
            (0..ROW_COUNT)
                .map(|index| crate::tracked_state::TrackedStateKey {
                    schema_key: "shared_schema".to_string(),
                    file_id: Some("shared_file".to_string()),
                    row_pk: RowPk::single(format!("row-{index:05}")),
                })
                .collect()
        };
        let target_identities =
            TrackedStateDiffIdentity::from_key_batch(keys()).expect("target identity batch");
        let source_identities =
            TrackedStateDiffIdentity::from_key_batch(keys()).expect("source identity batch");
        let timestamp = crate::common::LixTimestamp::from_unix_millis_utc_lossy(0);
        let commit_id = CommitId::for_test_label("conflict-batch-commit");
        let entries = |identities: &[TrackedStateDiffIdentity], side: &str| {
            identities
                .iter()
                .enumerate()
                .map(|(index, identity)| {
                    let change_id = ChangeId::for_test_label(&format!("{side}-change-{index:05}"));
                    TrackedStateDiffEntry {
                        identity: identity.clone(),
                        kind: TrackedStateDiffKind::Modified,
                        before: None,
                        after: Some(TrackedStateDiffRow {
                            identity: identity.clone(),
                            deleted: false,
                            created_at: timestamp,
                            updated_at: timestamp,
                            change_id,
                            commit_id,
                        }),
                    }
                })
                .collect()
        };
        let target = TrackedStateDiff::from_entries(entries(&target_identities, "target"));
        let source = TrackedStateDiff::from_entries(entries(&source_identities, "source"));

        let plan = plan_merge(&target, &source, &TrackedStatePayloadBatch::default())
            .expect("divergent batches should plan");

        assert!(plan.picks.is_empty());
        assert_eq!(plan.conflicts.len(), ROW_COUNT);
        assert_eq!(
            plan.conflicts.large_buffer_count(),
            1,
            "conflict metadata must use one contiguous descriptor buffer"
        );
        for (index, (identity, conflict)) in target_identities
            .iter()
            .zip(plan.conflicts.iter())
            .enumerate()
        {
            assert!(
                identity.shares_key_with(&conflict.identity),
                "conflict {index} cloned its identity fields"
            );
            assert!(conflict.identity.shares_key_with(&conflict.target.identity));
            assert!(conflict.identity.shares_key_with(&conflict.source.identity));
            assert!(
                conflict
                    .identity
                    .shares_key_with(&conflict.source.after.as_ref().expect("source row").identity)
            );
        }
    }

    #[test]
    fn unsorted_merge_uses_one_partitioned_reference_buffer() {
        let target = diff(vec![
            entry(
                "row-c",
                TrackedStateDiffKind::Modified,
                Some(row("row-c", "base-c")),
                Some(row("row-c", "target-c")),
            ),
            entry(
                "row-a",
                TrackedStateDiffKind::Modified,
                Some(row("row-a", "base-a")),
                Some(row("row-a", "target-a")),
            ),
        ]);
        let source = diff(vec![
            entry(
                "row-d",
                TrackedStateDiffKind::Added,
                None,
                Some(row("row-d", "source-d")),
            ),
            entry(
                "row-b",
                TrackedStateDiffKind::Added,
                None,
                Some(row("row-b", "source-b")),
            ),
        ]);

        match SortedMergeInputs::new(&target, &source).expect("merge inputs should prepare") {
            SortedMergeInputs::Borrowed { .. } => {
                panic!("unsorted input must use the defensive sorting fallback");
            }
            SortedMergeInputs::Fallback {
                entries,
                target_len,
            } => {
                assert_eq!(entries.len(), 4);
                assert_eq!(target_len, 2);
                assert_eq!(
                    entries[0]
                        .identity
                        .row_pk()
                        .as_single_string_owned()
                        .expect("target identity"),
                    "row-a"
                );
                assert_eq!(
                    entries[target_len]
                        .identity
                        .row_pk()
                        .as_single_string_owned()
                        .expect("source identity"),
                    "row-b"
                );
            }
        }

        let plan = plan_merge(&target, &source, &TrackedStatePayloadBatch::default())
            .expect("unsorted merge should plan");
        assert_eq!(pick_ids(&plan), vec!["row-b", "row-d"]);
        assert!(plan.conflicts.is_empty());
    }

    #[test]
    fn unsorted_non_adjacent_duplicate_identity_is_rejected() {
        let duplicate = entry(
            "row-a",
            TrackedStateDiffKind::Added,
            None,
            Some(row("row-a", "source-a")),
        );
        let source = diff(vec![
            duplicate.clone(),
            entry(
                "row-b",
                TrackedStateDiffKind::Added,
                None,
                Some(row("row-b", "source-b")),
            ),
            duplicate,
        ]);

        let error = plan_merge(
            &TrackedStateDiff::default(),
            &source,
            &TrackedStatePayloadBatch::default(),
        )
        .expect_err("duplicate source identity must be rejected");

        assert!(error.message.contains("duplicate diff entry"));
        assert!(error.message.contains("row-a"));
    }

    fn diff(entries: Vec<TrackedStateDiffEntry>) -> TrackedStateDiff {
        TrackedStateDiff::from_entries(entries)
    }

    fn integer_row_pk(value: i64) -> RowPk {
        RowPk::from_components(smallvec::smallvec![crate::row_pk::RowPkComponent::Integer(
            value
        )])
        .expect("one integer is a valid row primary key")
    }

    fn integer_identity_batch(first: i64, row_count: usize) -> Vec<TrackedStateDiffIdentity> {
        let row_pks = (0..row_count)
            .map(|offset| {
                integer_row_pk(first + i64::try_from(offset).expect("test row count fits an i64"))
            })
            .collect::<Vec<_>>();
        TrackedStateDiffIdentity::from_key_refs(row_count, |index| {
            crate::tracked_state::TrackedStateKeyRef {
                schema_key: "test_schema",
                file_id: None,
                row_pk: &row_pks[index],
            }
        })
        .expect("integer identity batch should seal")
    }

    fn numbered_change_id(index: usize) -> ChangeId {
        ChangeId::new(uuid::Uuid::from_u128(
            u128::try_from(index).expect("test row index fits u128") + 1,
        ))
    }

    fn live_after_entry(
        identity: TrackedStateDiffIdentity,
        kind: TrackedStateDiffKind,
        change_id: ChangeId,
    ) -> TrackedStateDiffEntry {
        let timestamp = crate::common::LixTimestamp::from_unix_millis_utc_lossy(0);
        TrackedStateDiffEntry {
            identity: identity.clone(),
            kind,
            before: None,
            after: Some(TrackedStateDiffRow {
                identity,
                deleted: false,
                created_at: timestamp,
                updated_at: timestamp,
                change_id,
                commit_id: CommitId::new(uuid::Uuid::from_u128(1)),
            }),
        }
    }

    fn entry(
        row_pk: &str,
        kind: TrackedStateDiffKind,
        mut before: Option<TrackedStateDiffRow>,
        mut after: Option<TrackedStateDiffRow>,
    ) -> TrackedStateDiffEntry {
        let identity = TrackedStateDiffIdentity::from_key(crate::tracked_state::TrackedStateKey {
            schema_key: "test_schema".to_string(),
            row_pk: RowPk::single(row_pk),
            file_id: None,
        });
        if let Some(before) = before.as_mut() {
            before.identity = identity.clone();
        }
        if let Some(after) = after.as_mut() {
            after.identity = identity.clone();
        }
        TrackedStateDiffEntry {
            identity,
            kind,
            before,
            after,
        }
    }

    fn pick_ids(plan: &TrackedStateMergePlan) -> Vec<String> {
        plan.picks
            .iter()
            .map(|entry| {
                entry
                    .identity()
                    .row_pk()
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
                    .row_pk()
                    .as_single_string_owned()
                    .expect("identity")
            })
            .collect()
    }

    fn tombstone(row_pk: &str, change_id: &str) -> TrackedStateDiffRow {
        let mut row = row(row_pk, change_id);
        row.deleted = true;
        row
    }

    fn row(row_pk: &str, change_id: &str) -> TrackedStateDiffRow {
        row_with_value(row_pk, change_id)
    }

    fn row_with_value(row_pk: &str, change_id: &str) -> TrackedStateDiffRow {
        TrackedStateDiffRow {
            identity: TrackedStateDiffIdentity::from_key(crate::tracked_state::TrackedStateKey {
                row_pk: RowPk::single(row_pk),
                schema_key: "test_schema".to_string(),
                file_id: None,
            }),
            deleted: false,
            created_at: crate::common::LixTimestamp::expect_parse(
                "created_at",
                "2026-01-01T00:00:00Z",
            ),
            updated_at: crate::common::LixTimestamp::expect_parse(
                "updated_at",
                "2026-01-01T00:00:00Z",
            ),
            change_id: ChangeId::for_test_label(change_id),
            commit_id: CommitId::for_test_label(&change_id.replace("change", "commit")),
        }
    }

    #[test]
    fn merge_plan_reuses_diff_identity_owner_for_conflicts_and_side_rows() {
        let target = entry(
            "row-a",
            TrackedStateDiffKind::Modified,
            Some(row("row-a", "base")),
            Some(row("row-a", "target")),
        );
        let target_owner = target.identity.clone();
        assert!(
            target
                .before
                .as_ref()
                .is_some_and(|row| target_owner.shares_key_with(&row.identity))
        );
        assert!(
            target
                .after
                .as_ref()
                .is_some_and(|row| target_owner.shares_key_with(&row.identity))
        );
        let source = entry(
            "row-a",
            TrackedStateDiffKind::Modified,
            Some(row("row-a", "base")),
            Some(row("row-a", "source")),
        );
        let source_owner = source.identity.clone();

        let plan = plan_merge(
            &diff(vec![target]),
            &diff(vec![source]),
            &TrackedStatePayloadBatch::default(),
        )
        .expect("merge should plan");
        let conflict = &plan.conflicts[0];

        assert!(target_owner.shares_key_with(&conflict.identity));
        assert!(target_owner.shares_key_with(&conflict.target.identity));
        assert!(target_owner.shares_key_with(&conflict.source.identity));
        assert!(
            conflict
                .target
                .after
                .as_ref()
                .is_some_and(|row| { target_owner.shares_key_with(&row.identity) })
        );
        assert!(
            conflict
                .source
                .before
                .as_ref()
                .is_some_and(|row| { target_owner.shares_key_with(&row.identity) })
        );
        assert!(
            conflict
                .source
                .after
                .as_ref()
                .is_some_and(|row| { target_owner.shares_key_with(&row.identity) })
        );
        assert!(
            !source_owner.shares_key_with(&conflict.identity),
            "equal source identity must be rebound to the target-owned conflict key"
        );
    }
}
