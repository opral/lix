#[cfg(test)]
use crate::tracked_state::TrackedStateDiffIdentity;
use crate::tracked_state::{TrackedStateMergeConflict, TrackedStateMergePlan};

/// Borrowed, typed view over a merge plan's conflict column.
///
/// The merge plan remains the sole owner. Iterating this batch creates only
/// pointer-sized row views for plugin preflight.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MergeConflictBatch<'a> {
    rows: &'a [TrackedStateMergeConflict],
}

impl<'a> MergeConflictBatch<'a> {
    pub(crate) fn from_plan(plan: &'a TrackedStateMergePlan) -> Self {
        Self {
            rows: &plan.conflicts,
        }
    }

    pub(crate) fn iter(
        self,
    ) -> impl DoubleEndedIterator<Item = MergeConflictRow<'a>> + ExactSizeIterator + 'a {
        self.rows.iter().map(MergeConflictRow::new)
    }
}

/// One allocation-free row view in [`MergeConflictBatch`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct MergeConflictRow<'a> {
    tracked: &'a TrackedStateMergeConflict,
}

impl<'a> MergeConflictRow<'a> {
    fn new(tracked: &'a TrackedStateMergeConflict) -> Self {
        Self { tracked }
    }

    #[cfg(test)]
    pub(crate) fn tracked(self) -> &'a TrackedStateMergeConflict {
        self.tracked
    }

    #[cfg(test)]
    pub(crate) fn identity(self) -> &'a TrackedStateDiffIdentity {
        &self.tracked.identity
    }

    pub(crate) fn file_id(self) -> Option<&'a str> {
        self.tracked.identity.file_id()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::{ChangeId, CommitId};
    use crate::common::LixTimestamp;
    use crate::row_pk::RowPk;
    use crate::tracked_state::{TrackedStateDiffEntry, TrackedStateDiffKind};
    use crate::tracked_state::{TrackedStateDiffRow, TrackedStateKey};

    fn row(identity: TrackedStateDiffIdentity, label: &str) -> TrackedStateDiffRow {
        TrackedStateDiffRow {
            identity,
            deleted: false,
            created_at: LixTimestamp::expect_parse("created", "2026-01-01T00:00:00Z"),
            updated_at: LixTimestamp::expect_parse("updated", "2026-01-01T00:00:00Z"),
            change_id: ChangeId::for_test_label(label),
            commit_id: CommitId::for_test_label(label),
            author_id: crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
        }
    }

    #[test]
    fn conflict_batch_rows_borrow_the_plan_and_share_one_identity_owner() {
        let identity = TrackedStateDiffIdentity::from_key(TrackedStateKey {
            schema_key: "schema".to_owned(),
            file_id: Some("file".to_owned()),
            row_pk: RowPk::single("row"),
        });
        let target_row = row(identity.clone(), "target");
        let source_row = row(identity.clone(), "source");
        let conflict = TrackedStateMergeConflict {
            identity: identity.clone(),
            target: TrackedStateDiffEntry {
                identity: identity.clone(),
                kind: TrackedStateDiffKind::Modified,
                before: None,
                after: Some(target_row),
            },
            source: TrackedStateDiffEntry {
                identity: identity.clone(),
                kind: TrackedStateDiffKind::Modified,
                before: None,
                after: Some(source_row),
            },
        };
        let plan = TrackedStateMergePlan {
            picks: Vec::new().into(),
            conflicts: vec![conflict].into(),
        };

        let batch = MergeConflictBatch::from_plan(&plan);
        let view = batch.iter().next().expect("one conflict");

        assert!(std::ptr::eq(view.tracked(), &plan.conflicts[0]));
        assert!(identity.shares_key_with(view.identity()));
        assert!(identity.shares_key_with(&view.tracked().target.identity));
        assert!(
            identity.shares_key_with(
                &view
                    .tracked()
                    .source
                    .after
                    .as_ref()
                    .expect("source row")
                    .identity
            )
        );
    }
}
