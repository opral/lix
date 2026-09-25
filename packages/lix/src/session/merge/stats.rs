use std::cmp::Ordering;

use crate::LixError;
use crate::tracked_state::{
    TrackedStateDiff, TrackedStateDiffIdentity, TrackedStateDiffKind, TrackedStateMergePick,
    TrackedStateMergePlan,
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct MergeStats {
    pub(crate) total: usize,
    pub(crate) added: usize,
    pub(crate) modified: usize,
    pub(crate) removed: usize,
}

pub(crate) fn stats_from_diff(diff: &TrackedStateDiff) -> MergeStats {
    let mut stats = MergeStats::default();
    for entry in &diff.entries {
        stats.add(entry.kind);
    }
    stats
}

pub(crate) fn stats_from_plan(
    plan: &TrackedStateMergePlan,
    source_diff: &TrackedStateDiff,
) -> Result<MergeStats, LixError> {
    if identities_are_strictly_sorted(plan.picks.iter().map(pick_identity), plan.picks.len())
        && identities_are_strictly_sorted(
            source_diff.entries.iter().map(|entry| &entry.identity),
            source_diff.entries.len(),
        )
    {
        return stats_from_sorted_plan(plan, source_diff);
    }

    // Defensive hand-built callers can supply unsorted inputs. Keep their
    // behavior correct without imposing an index allocation on the normal
    // tree-diff path, whose entries and merge picks are already key ordered.
    stats_from_unsorted_plan(plan, source_diff)
}

fn stats_from_sorted_plan(
    plan: &TrackedStateMergePlan,
    source_diff: &TrackedStateDiff,
) -> Result<MergeStats, LixError> {
    let mut stats = MergeStats::default();
    let mut source_index = 0;
    for pick in &plan.picks {
        let identity = pick_identity(pick);
        let mut found = false;
        while let Some(entry) = source_diff.entries.get(source_index) {
            match identity_cmp(&entry.identity, identity) {
                Ordering::Less => source_index += 1,
                Ordering::Equal => {
                    stats.add(entry.kind);
                    source_index += 1;
                    found = true;
                    break;
                }
                Ordering::Greater => return Err(missing_source_entry(identity)),
            }
        }
        if !found {
            return Err(missing_source_entry(identity));
        }
    }
    Ok(stats)
}

fn stats_from_unsorted_plan(
    plan: &TrackedStateMergePlan,
    source_diff: &TrackedStateDiff,
) -> Result<MergeStats, LixError> {
    let mut stats = MergeStats::default();
    for pick in &plan.picks {
        let identity = pick_identity(pick);
        let Some(entry) = source_diff
            .entries
            .iter()
            .find(|entry| &entry.identity == identity)
        else {
            return Err(missing_source_entry(identity));
        };
        stats.add(entry.kind);
    }
    Ok(stats)
}

fn identities_are_strictly_sorted<'a>(
    mut identities: impl Iterator<Item = &'a TrackedStateDiffIdentity>,
    len: usize,
) -> bool {
    if len < 2 {
        return true;
    }
    let Some(mut previous) = identities.next() else {
        return true;
    };
    for identity in identities {
        if identity_cmp(previous, identity) != Ordering::Less {
            return false;
        }
        previous = identity;
    }
    true
}

fn missing_source_entry(identity: &TrackedStateDiffIdentity) -> LixError {
    let row_pk = identity
        .row_pk()
        .as_json_array_text()
        .unwrap_or_else(|_| "<invalid row pk>".to_string());
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!(
            "merge analysis could not find source diff entry for source schema '{}' row '{}'",
            identity.schema_key(),
            row_pk
        ),
    )
}

fn identity_cmp(left: &TrackedStateDiffIdentity, right: &TrackedStateDiffIdentity) -> Ordering {
    #[cfg(test)]
    STATS_IDENTITY_COMPARISONS.with(|comparisons| comparisons.set(comparisons.get() + 1));
    left.cmp(right)
}

#[cfg(test)]
thread_local! {
    static STATS_IDENTITY_COMPARISONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn reset_identity_comparison_count() {
    STATS_IDENTITY_COMPARISONS.with(|comparisons| comparisons.set(0));
}

#[cfg(test)]
fn identity_comparison_count() -> usize {
    STATS_IDENTITY_COMPARISONS.with(std::cell::Cell::get)
}

impl MergeStats {
    fn add(&mut self, kind: TrackedStateDiffKind) {
        self.total += 1;
        match kind {
            TrackedStateDiffKind::Added => self.added += 1,
            TrackedStateDiffKind::Modified => self.modified += 1,
            TrackedStateDiffKind::Removed => self.removed += 1,
        }
    }
}

fn pick_identity(pick: &TrackedStateMergePick) -> &TrackedStateDiffIdentity {
    &pick.identity
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::{ChangeId, CommitId};
    use crate::common::LixTimestamp;
    use crate::row_pk::RowPk;
    use crate::tracked_state::{
        TrackedStateDiffEntry, TrackedStateDiffRow, TrackedStateKey, TrackedStateMergePick,
    };

    #[test]
    fn ten_thousand_sorted_merge_picks_have_linear_stats_scan() {
        const ROW_COUNT: usize = 10_000;
        let identities = TrackedStateDiffIdentity::from_key_batch(
            (0..ROW_COUNT)
                .map(|index| TrackedStateKey {
                    schema_key: "shared_schema".to_string(),
                    file_id: Some("shared_file".to_string()),
                    row_pk: RowPk::single(format!("row-{index:05}")),
                })
                .collect(),
        )
        .expect("identity batch should fit");
        let timestamp = LixTimestamp::from_unix_millis_utc_lossy(0);
        let change_id = ChangeId::for_test_label("stats-change");
        let commit_id = CommitId::for_test_label("stats-commit");
        let source_diff = TrackedStateDiff::from_entries(
            identities
                .iter()
                .enumerate()
                .map(|(index, identity)| {
                    let row = TrackedStateDiffRow {
                        identity: identity.clone(),
                        deleted: false,
                        created_at: timestamp,
                        updated_at: timestamp,
                        change_id,
                        commit_id,
                    };
                    TrackedStateDiffEntry {
                        identity: identity.clone(),
                        kind: match index % 3 {
                            0 => TrackedStateDiffKind::Added,
                            1 => TrackedStateDiffKind::Modified,
                            _ => TrackedStateDiffKind::Removed,
                        },
                        before: None,
                        after: Some(row),
                    }
                })
                .collect(),
        );
        let plan = TrackedStateMergePlan {
            picks: source_diff
                .entries
                .iter()
                .map(|entry| TrackedStateMergePick {
                    identity: entry.identity.clone(),
                    change_id,
                    selected_row: entry.after.clone().expect("source row"),
                })
                .collect(),
            conflicts: Vec::new().into(),
        };

        reset_identity_comparison_count();
        let stats = stats_from_plan(&plan, &source_diff).expect("sorted stats should resolve");
        let comparisons = identity_comparison_count();

        assert_eq!(stats.total, ROW_COUNT);
        assert_eq!(stats.added, 3_334);
        assert_eq!(stats.modified, 3_333);
        assert_eq!(stats.removed, 3_333);
        assert!(
            comparisons <= ROW_COUNT * 3,
            "sorted stats should inspect at most three linear passes, got {comparisons}"
        );
    }
}
