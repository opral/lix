use std::cmp::Ordering;

use crate::LixError;

use super::native::{MergeDiff, MergeDiffKind, MergePlan};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct MergeStats {
    pub(crate) total: usize,
    pub(crate) added: usize,
    pub(crate) modified: usize,
    pub(crate) removed: usize,
}

pub(crate) fn stats_from_diff(diff: &MergeDiff) -> MergeStats {
    let mut stats = MergeStats::default();
    for entry in &diff.entries {
        stats.add(entry.kind);
    }
    stats
}

pub(crate) fn stats_from_plan(
    plan: &MergePlan,
    source_diff: &MergeDiff,
) -> Result<MergeStats, LixError> {
    if !identities_are_strictly_sorted(
        plan.picks.iter().map(|pick| pick.identity(source_diff)),
        plan.picks.len(),
    ) || !identities_are_strictly_sorted(
        source_diff.entries.iter().map(|entry| &entry.identity),
        source_diff.entries.len(),
    ) {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "native merge stats received non-canonical StateKey order",
        ));
    }
    stats_from_sorted_plan(plan, source_diff)
}

fn stats_from_sorted_plan(
    plan: &MergePlan,
    source_diff: &MergeDiff,
) -> Result<MergeStats, LixError> {
    let mut stats = MergeStats::default();
    let mut source_index = 0;
    for pick in &plan.picks {
        let identity = pick.identity(source_diff);
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

fn identities_are_strictly_sorted<'a>(
    mut identities: impl Iterator<Item = &'a crate::forktree::StateKey>,
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

fn missing_source_entry(identity: &crate::forktree::StateKey) -> LixError {
    let entity_pk = identity
        .entity_pk
        .as_json_array_text()
        .unwrap_or_else(|_| "<invalid entity pk>".to_string());
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!(
            "merge analysis could not find source diff entry for source schema '{}' entity '{}'",
            identity.schema_key, entity_pk
        ),
    )
}

fn identity_cmp(left: &crate::forktree::StateKey, right: &crate::forktree::StateKey) -> Ordering {
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
    fn add(&mut self, kind: MergeDiffKind) {
        self.total += 1;
        match kind {
            MergeDiffKind::Added => self.added += 1,
            MergeDiffKind::Modified => self.modified += 1,
            MergeDiffKind::Removed => self.removed += 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::native::{MergeDiffEntry, MergeDiffKind, MergePick, MergePlan, MergeRow};
    use super::*;
    use crate::changelog::{ChangeId, CommitId};
    use crate::common::LixTimestamp;
    use crate::entity_pk::EntityPk;

    #[test]
    fn ten_thousand_sorted_merge_picks_have_linear_stats_scan() {
        const ROW_COUNT: usize = 10_000;
        let identities = (0..ROW_COUNT)
            .map(|index| crate::forktree::StateKey {
                schema_key: "shared_schema".to_string(),
                file_id: Some("shared_file".to_string()),
                entity_pk: EntityPk::single(format!("entity-{index:05}")),
            })
            .collect::<Vec<_>>();
        let timestamp = LixTimestamp::from_unix_millis_utc_lossy(0);
        let change_id = ChangeId::for_test_label("stats-change");
        let commit_id = CommitId::for_test_label("stats-commit");
        let source_diff = MergeDiff::from_entries_with_payloads(
            identities
                .iter()
                .enumerate()
                .map(|(index, identity)| {
                    let row = MergeRow {
                        deleted: false,
                        created_at: timestamp,
                        updated_at: timestamp,
                        change_id,
                        commit_id,
                    };
                    MergeDiffEntry {
                        identity: identity.clone(),
                        kind: match index % 3 {
                            0 => MergeDiffKind::Added,
                            1 => MergeDiffKind::Modified,
                            _ => MergeDiffKind::Removed,
                        },
                        before: None,
                        after: Some(row),
                    }
                })
                .collect(),
            super::super::native::MergePayloadBatch::default(),
        );
        let plan = MergePlan {
            picks: source_diff
                .entries
                .iter()
                .enumerate()
                .map(|(index, _entry)| MergePick {
                    change_id,
                    source_index: index,
                })
                .collect(),
            conflicts: Vec::new(),
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
