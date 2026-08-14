use crate::changelog::ChangeId;
use crate::row_pk::RowPk;

use super::native::{
    MergeConflict as NativeMergeConflict, MergeDiffEntry, MergeDiffKind, MergeKeyExt, MergePlan,
};

/// Borrowed, typed view over a merge plan's conflict column.
///
/// The merge plan remains the sole owner. Iterating this batch creates only
/// pointer-sized row views, so analysis, plugin preflight, error construction,
/// and public preview all inspect the same identity and side records.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MergeConflictBatch<'a> {
    rows: &'a [NativeMergeConflict],
}

impl<'a> MergeConflictBatch<'a> {
    pub(crate) fn from_plan(plan: &'a MergePlan) -> Self {
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
    tracked: &'a NativeMergeConflict,
}

impl<'a> MergeConflictRow<'a> {
    fn new(tracked: &'a NativeMergeConflict) -> Self {
        Self { tracked }
    }

    pub(crate) fn tracked(self) -> &'a NativeMergeConflict {
        self.tracked
    }

    pub(crate) fn kind(self) -> MergeConflictKind {
        MergeConflictKind::SameRowChanged
    }

    #[cfg(test)]
    pub(crate) fn identity(self) -> &'a crate::forktree::StateKey {
        &self.tracked.identity
    }

    pub(crate) fn schema_key(self) -> &'a str {
        self.tracked.identity.schema_key()
    }

    pub(crate) fn row_pk(self) -> &'a RowPk {
        self.tracked.identity.row_pk()
    }

    pub(crate) fn file_id(self) -> Option<&'a str> {
        self.tracked.identity.file_id()
    }

    pub(crate) fn target(self) -> MergeConflictSideRow<'a> {
        MergeConflictSideRow {
            entry: &self.tracked.target,
        }
    }

    pub(crate) fn source(self) -> MergeConflictSideRow<'a> {
        MergeConflictSideRow {
            entry: &self.tracked.source,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MergeConflictKind {
    SameRowChanged,
}

/// One side-column view in a conflict batch row.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MergeConflictSideRow<'a> {
    entry: &'a MergeDiffEntry,
}

impl<'a> MergeConflictSideRow<'a> {
    pub(crate) fn kind(self) -> MergeConflictChangeKind {
        match self.entry.kind {
            MergeDiffKind::Added => MergeConflictChangeKind::Added,
            MergeDiffKind::Modified => MergeConflictChangeKind::Modified,
            MergeDiffKind::Removed => MergeConflictChangeKind::Removed,
        }
    }

    pub(crate) fn before_change_id(self) -> Option<ChangeId> {
        self.entry.before.as_ref().map(|row| row.change_id)
    }

    pub(crate) fn after_change_id(self) -> Option<ChangeId> {
        self.entry.after.as_ref().map(|row| row.change_id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MergeConflictChangeKind {
    Added,
    Modified,
    Removed,
}

#[cfg(test)]
mod tests {
    use super::super::native::{MergeDiffEntry, MergeDiffKind, MergeKeyExt, MergePlan, MergeRow};
    use super::*;
    use crate::changelog::{ChangeId, CommitId};
    use crate::common::LixTimestamp;

    fn row(identity: crate::forktree::StateKey, label: &str) -> MergeRow {
        let _ = identity;
        MergeRow {
            deleted: false,
            created_at: LixTimestamp::expect_parse("created", "2026-01-01T00:00:00Z"),
            updated_at: LixTimestamp::expect_parse("updated", "2026-01-01T00:00:00Z"),
            change_id: ChangeId::for_test_label(label),
            commit_id: CommitId::for_test_label(label),
        }
    }

    #[test]
    fn conflict_batch_rows_borrow_the_plan_and_share_one_identity_owner() {
        let identity = crate::forktree::StateKey {
            schema_key: "schema".to_owned(),
            file_id: Some("file".to_owned()),
            row_pk: RowPk::single("row"),
        };
        let target_row = row(identity.clone(), "target");
        let source_row = row(identity.clone(), "source");
        let conflict = NativeMergeConflict {
            identity: identity.clone(),
            target: MergeDiffEntry {
                identity: identity.clone(),
                kind: MergeDiffKind::Modified,
                before: None,
                after: Some(target_row),
            },
            source: MergeDiffEntry {
                identity: identity.clone(),
                kind: MergeDiffKind::Modified,
                before: None,
                after: Some(source_row),
            },
        };
        let plan = MergePlan {
            picks: Vec::new(),
            conflicts: vec![conflict],
        };

        let batch = MergeConflictBatch::from_plan(&plan);
        let view = batch.iter().next().expect("one conflict");

        assert!(std::ptr::eq(view.tracked(), &plan.conflicts[0]));
        assert!(identity.shares_key_with(view.identity()));
        assert!(identity.shares_key_with(&view.tracked().target.identity));
        assert!(identity.shares_key_with(&view.tracked().source.identity));
    }
}
