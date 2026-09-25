//! Commit-change refs accumulated while transaction rows are staged.
//!
//! Relocated out of the shared write-row vocabulary in
//! [`crate::transaction_types`]: this cluster is the only part of it that names
//! the write path, because [`StagedCommitChangeRefs`] owns the certified
//! [`OrderedMutationJournal`](super::staging::OrderedMutationJournal) that
//! staging produces. Nothing outside `transaction` reads a
//! `StagedCommitChangeRefs`.

use std::collections::HashSet;
use std::sync::Arc;

use crate::LixError;
use crate::changelog::{ChangeId, CommitId};
use crate::common::LixTimestamp;
use crate::row_pk::RowPk;
use crate::tracked_state::TrackedStateDiffIdentity;

/// Transaction-local commit change refs accumulated while rows are staged.
///
/// Final commit row materialization owns commit ids, parent heads, and commit
/// row timestamps. Staging only tracks which hydrated tracked changes the
/// future commit introduces for a branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StagedCommitChangeRefs {
    pub(crate) commit_id: CommitId,
    /// Every normal branch-head publication has a distinct public
    /// `lixcol_change_id`, even though v6 stores the moving ref directly
    /// rather than as a flat current row.
    pub(crate) branch_ref_change_id: ChangeId,
    pub(crate) created_at: LixTimestamp,
    /// Normal prepared rows are already the authoritative commit-membership
    /// sequence. Keeping every fresh row change id in another BTreeSet only
    /// duplicates 10k+ UUIDs on bulk commits, so staging tracks their count.
    pub(crate) tracked_change_count: usize,
    /// Selected historical refs retain their immutable identity and metadata
    /// columns through merge/checkpoint staging and commit materialization.
    ///
    /// A branch normally owns exactly one batch. Multiple batches remain
    /// separate so staging/finalization clones only `Arc` owners rather than
    /// copying schema/file/row or metadata columns.
    selected_change_batches: Vec<StagedCommitChangeBatch>,
    /// Certified immutable mutation columns for this commit. This owner is
    /// attached only at drain, after complete-replacement certification, and
    /// is cloned through commit finalization in O(1).
    ordered_mutation_journal: Option<Arc<crate::transaction::staging::OrderedMutationJournal>>,
    pub(crate) allow_empty: bool,
}

impl Default for StagedCommitChangeRefs {
    fn default() -> Self {
        Self {
            commit_id: CommitId::default(),
            branch_ref_change_id: ChangeId::default(),
            created_at: LixTimestamp::expect_parse("created_at", "1970-01-01T00:00:00.000Z"),
            tracked_change_count: 0,
            selected_change_batches: Vec::new(),
            ordered_mutation_journal: None,
            allow_empty: false,
        }
    }
}

impl StagedCommitChangeRefs {
    pub(crate) fn absorb_cohort_membership(&mut self, mut other: Self) {
        debug_assert!(
            self.ordered_mutation_journal.is_none() && other.ordered_mutation_journal.is_none(),
            "immutable replacement journals cannot join commit cohorts"
        );
        self.tracked_change_count = self
            .tracked_change_count
            .saturating_add(other.tracked_change_count);
        self.selected_change_batches
            .append(&mut other.selected_change_batches);
        self.allow_empty |= other.allow_empty;
    }
}

/// Immutable typed columns for historical changes selected into a new commit.
///
/// Identities retain the diff batch owner, so schema keys, file ids, and
/// row primary keys are never lowered into row-owned transaction strings.
/// All remaining metadata is stored in fixed typed columns allocated once per
/// batch. Cloning this batch through transaction staging is O(1).
#[derive(Debug, Default, PartialEq, Eq)]
struct StagedCommitChangeColumns {
    identities: Vec<TrackedStateDiffIdentity>,
    source_commit_ids: Vec<CommitId>,
    change_ids: Vec<ChangeId>,
    deleted: Vec<bool>,
    created_at: Vec<LixTimestamp>,
    updated_at: Vec<LixTimestamp>,
    /// Logical row ordinals whose selected change is a deletion. Built once
    /// while the typed columns are produced so checkpoint publication can
    /// visit O(deletes) rather than rescan every selected member.
    deleted_rows: Vec<u32>,
}

#[derive(Debug, Clone)]
pub(crate) struct StagedCommitChangeBatch {
    columns: Arc<StagedCommitChangeColumns>,
    /// Every `(source_commit_id, change_id, identity)` tuple was produced by
    /// the tracked diff reader from immutable commit authority.
    source_membership_certified: bool,
    /// Present only when duplicate identities were filtered while combining
    /// independently supplied batches. The normal merge/checkpoint path views
    /// every row directly and allocates no selection column.
    selection: Option<Arc<[u32]>>,
}

#[derive(Debug)]
pub(crate) struct StagedCommitChangeBatchBuilder {
    columns: StagedCommitChangeColumns,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StagedCommitChangeRef<'a> {
    identity: &'a TrackedStateDiffIdentity,
    pub(crate) source_commit_id: CommitId,
    pub(crate) change_id: ChangeId,
    pub(crate) deleted: bool,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
}

impl StagedCommitChangeBatchBuilder {
    pub(crate) fn with_capacity(row_count: usize) -> Self {
        Self {
            columns: StagedCommitChangeColumns {
                identities: Vec::with_capacity(row_count),
                source_commit_ids: Vec::with_capacity(row_count),
                change_ids: Vec::with_capacity(row_count),
                deleted: Vec::with_capacity(row_count),
                created_at: Vec::with_capacity(row_count),
                updated_at: Vec::with_capacity(row_count),
                deleted_rows: Vec::new(),
            },
        }
    }

    pub(crate) fn push(
        &mut self,
        identity: TrackedStateDiffIdentity,
        source_commit_id: CommitId,
        change_id: ChangeId,
        deleted: bool,
        created_at: LixTimestamp,
        updated_at: LixTimestamp,
    ) {
        if deleted {
            self.columns.deleted_rows.push(
                u32::try_from(self.columns.identities.len())
                    .expect("staged commit change batch exceeds u32 rows"),
            );
        }
        self.columns.identities.push(identity);
        self.columns.source_commit_ids.push(source_commit_id);
        self.columns.change_ids.push(change_id);
        self.columns.deleted.push(deleted);
        self.columns.created_at.push(created_at);
        self.columns.updated_at.push(updated_at);
    }

    pub(crate) fn finish(self) -> StagedCommitChangeBatch {
        StagedCommitChangeBatch {
            columns: Arc::new(self.columns),
            source_membership_certified: false,
            selection: None,
        }
    }

    /// Finishes rows materialized directly from a tracked-state diff.
    ///
    /// The diff reader binds every direct change address to the decoded source
    /// identity before exposing the row. Checkpoint lowering may therefore
    /// prove a complete dense source selection from coordinates alone instead
    /// of hashing all identity bytes again.
    pub(crate) fn finish_source_certified(self) -> StagedCommitChangeBatch {
        StagedCommitChangeBatch {
            columns: Arc::new(self.columns),
            source_membership_certified: true,
            selection: None,
        }
    }
}

impl Default for StagedCommitChangeBatch {
    fn default() -> Self {
        StagedCommitChangeBatchBuilder::with_capacity(0).finish()
    }
}

impl PartialEq for StagedCommitChangeBatch {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl Eq for StagedCommitChangeBatch {}

impl StagedCommitChangeBatch {
    pub(crate) fn len(&self) -> usize {
        self.selection
            .as_ref()
            .map_or(self.columns.identities.len(), |selection| selection.len())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn iter(&self) -> impl ExactSizeIterator<Item = StagedCommitChangeRef<'_>> + '_ {
        (0..self.len()).map(|row_index| self.row(row_index))
    }

    /// Selected deletion members without scanning non-deleted rows.
    pub(crate) fn deleted_iter(
        &self,
    ) -> impl Iterator<Item = StagedCommitChangeRef<'_>> + '_ {
        self.columns
            .deleted_rows
            .iter()
            .copied()
            .filter(move |&column_index| {
                self.selection
                    .as_ref()
                    .is_none_or(|selection| selection.binary_search(&column_index).is_ok())
            })
            .map(move |column_index| {
                let logical_index = self.selection.as_ref().map_or(column_index, |selection| {
                    u32::try_from(
                        selection
                            .binary_search(&column_index)
                            .expect("filtered deleted row belongs to selection"),
                    )
                    .expect("selected commit change batch exceeds u32 rows")
                });
                self.row(logical_index as usize)
            })
    }

    fn row(&self, row_index: usize) -> StagedCommitChangeRef<'_> {
        let column_index = self
            .selection
            .as_ref()
            .map_or(row_index, |selection| selection[row_index] as usize);
        StagedCommitChangeRef {
            identity: &self.columns.identities[column_index],
            source_commit_id: self.columns.source_commit_ids[column_index],
            change_id: self.columns.change_ids[column_index],
            deleted: self.columns.deleted[column_index],
            created_at: self.columns.created_at[column_index],
            updated_at: self.columns.updated_at[column_index],
        }
    }

    fn select(self, selection: Vec<u32>) -> Self {
        if selection.len() == self.columns.identities.len() {
            self
        } else {
            Self {
                columns: self.columns,
                source_membership_certified: self.source_membership_certified,
                selection: Some(selection.into()),
            }
        }
    }

    pub(crate) fn source_membership_certified(&self) -> bool {
        self.source_membership_certified
    }

    #[cfg(test)]
    pub(crate) fn shares_storage_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.columns, &other.columns)
    }

    #[cfg(test)]
    pub(crate) fn large_buffer_count(&self) -> usize {
        usize::from(!self.columns.identities.is_empty())
            + usize::from(!self.columns.source_commit_ids.is_empty())
            + usize::from(!self.columns.change_ids.is_empty())
            + usize::from(!self.columns.deleted.is_empty())
            + usize::from(!self.columns.created_at.is_empty())
            + usize::from(!self.columns.updated_at.is_empty())
            + usize::from(!self.columns.deleted_rows.is_empty())
            + usize::from(self.selection.is_some())
    }
}

impl<'a> StagedCommitChangeRef<'a> {
    #[cfg(test)]
    pub(crate) fn identity(&self) -> &'a TrackedStateDiffIdentity {
        self.identity
    }

    pub(crate) fn schema_key(&self) -> &'a str {
        self.identity.schema_key()
    }

    pub(crate) fn file_id(&self) -> Option<&'a str> {
        self.identity.file_id()
    }

    pub(crate) fn row_pk(&self) -> &'a RowPk {
        self.identity.row_pk()
    }
}

impl StagedCommitChangeRefs {
    pub(crate) fn attach_ordered_mutation_journal(
        &mut self,
        journal: Arc<crate::transaction::staging::OrderedMutationJournal>,
    ) -> Result<(), LixError> {
        if self.ordered_mutation_journal.replace(journal).is_some() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "commit received more than one immutable mutation journal",
            ));
        }
        Ok(())
    }

    pub(crate) fn ordered_mutation_journal(
        &self,
    ) -> Option<&Arc<crate::transaction::staging::OrderedMutationJournal>> {
        self.ordered_mutation_journal.as_ref()
    }

    pub(crate) fn take_ordered_mutation_journal(
        &mut self,
    ) -> Option<Arc<crate::transaction::staging::OrderedMutationJournal>> {
        self.ordered_mutation_journal.take()
    }

    pub(crate) fn new(
        commit_id: CommitId,
        branch_ref_change_id: ChangeId,
        created_at: LixTimestamp,
    ) -> Self {
        Self {
            commit_id,
            branch_ref_change_id,
            created_at,
            tracked_change_count: 0,
            selected_change_batches: Vec::new(),
            ordered_mutation_journal: None,
            allow_empty: false,
        }
    }

    pub(crate) fn add_change_id(&mut self, _change_id: ChangeId) {
        self.tracked_change_count += 1;
    }

    pub(crate) fn add_change_count(&mut self, count: usize) {
        self.tracked_change_count += count;
    }

    pub(crate) fn add_selected_change_batch(&mut self, batch: StagedCommitChangeBatch) {
        if batch.is_empty() {
            return;
        }
        if self.selected_change_batches.is_empty() {
            // Merge and checkpoint each stage one already validated diff
            // batch. Preserve that overwhelmingly common handoff as a pure
            // move with no deduplication index or selection allocation.
            self.selected_change_batches.push(batch);
            return;
        }

        // Deduplicate logical identities only while batches are combined,
        // then drop the index. Eager plugin rows may legitimately share one
        // source change id.
        // Unlike the former BTreeSet retained for the transaction lifetime,
        // this is one temporary contiguous table rather than one allocation
        // per selected mutation.
        let mut identities =
            HashSet::with_capacity(self.selected_change_count().saturating_add(batch.len()));
        identities.extend(
            self.selected_changes()
                .map(|change| (change.schema_key(), change.file_id(), change.row_pk())),
        );
        let mut selected: Option<Vec<u32>> = None;
        for (row_index, change) in batch.iter().enumerate() {
            if identities.insert((change.schema_key(), change.file_id(), change.row_pk())) {
                if let Some(selected) = selected.as_mut() {
                    selected.push(
                        u32::try_from(row_index)
                            .expect("selected commit change batch exceeds the ordinal range"),
                    );
                }
            } else if selected.is_none() {
                let mut retained = Vec::with_capacity(batch.len().saturating_sub(1));
                retained.extend((0..row_index).map(|index| {
                    u32::try_from(index)
                        .expect("selected commit change batch exceeds the ordinal range")
                }));
                selected = Some(retained);
            }
        }
        match selected {
            None => self.selected_change_batches.push(batch),
            Some(selected) if !selected.is_empty() => {
                self.selected_change_batches.push(batch.select(selected));
            }
            Some(_) => {}
        }
    }

    pub(crate) fn selected_changes(&self) -> impl Iterator<Item = StagedCommitChangeRef<'_>> + '_ {
        self.selected_change_batches
            .iter()
            .flat_map(StagedCommitChangeBatch::iter)
    }

    pub(crate) fn selected_change_count(&self) -> usize {
        self.selected_change_batches
            .iter()
            .map(StagedCommitChangeBatch::len)
            .sum()
    }

    pub(crate) fn has_selected_changes(&self) -> bool {
        self.selected_change_batches
            .iter()
            .any(|batch| !batch.is_empty())
    }

    pub(crate) fn into_selected_change_batches(self) -> Vec<StagedCommitChangeBatch> {
        self.selected_change_batches
    }

    pub(crate) fn remove_change_id(&mut self, _change_id: &ChangeId) {
        self.tracked_change_count = self
            .tracked_change_count
            .checked_sub(1)
            .expect("staged tracked change count must not underflow");
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.tracked_change_count == 0 && !self.has_selected_changes()
    }

    pub(crate) fn allow_empty(&mut self) {
        self.allow_empty = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tracked_state::TrackedStateKey;

    #[test]
    fn ten_thousand_selected_changes_retain_one_shared_typed_batch() {
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
        let mut builder = StagedCommitChangeBatchBuilder::with_capacity(ROW_COUNT);
        for (index, identity) in identities.iter().enumerate() {
            builder.push(
                identity.clone(),
                CommitId::for_test_label(&format!("selected-owner-{index}")),
                ChangeId::for_test_label(&format!("selected-change-{index}")),
                false,
                timestamp,
                timestamp,
            );
        }
        let batch = builder.finish();
        let source = batch.clone();

        assert_eq!(batch.large_buffer_count(), 6);
        assert!(
            batch
                .iter()
                .zip(&identities)
                .all(|(change, identity)| change.identity().shares_batch_with(identity)),
            "selected changes must retain the diff identity batch"
        );

        let mut staged = StagedCommitChangeRefs::default();
        staged.add_selected_change_batch(batch);
        assert_eq!(staged.selected_change_count(), ROW_COUNT);
        assert_eq!(staged.selected_change_batches.len(), 1);
        assert!(
            staged.selected_change_batches[0].shares_storage_with(&source),
            "transaction staging must retain the same metadata columns"
        );
        let finalized_clone = staged.clone();
        assert!(
            finalized_clone.selected_change_batches[0]
                .shares_storage_with(&staged.selected_change_batches[0]),
            "commit finalization clones must be O(1) batch-owner clones"
        );
    }

    #[test]
    fn selected_batches_dedupe_identity_not_source_change_id() {
        let timestamp = LixTimestamp::from_unix_millis_utc_lossy(0);
        let source_commit = CommitId::for_test_label("shared-source");
        let shared_change = ChangeId::for_test_label("shared-change");
        let identities = TrackedStateDiffIdentity::from_key_batch(
            ["row-a", "row-b"]
                .into_iter()
                .map(|row| TrackedStateKey {
                    schema_key: "schema".to_string(),
                    file_id: Some("file".to_string()),
                    row_pk: RowPk::single(row),
                })
                .collect(),
        )
        .expect("identity batch should fit");

        let mut first = StagedCommitChangeBatchBuilder::with_capacity(1);
        first.push(
            identities[0].clone(),
            source_commit,
            shared_change,
            false,
            timestamp,
            timestamp,
        );
        let mut second = StagedCommitChangeBatchBuilder::with_capacity(2);
        second.push(
            identities[1].clone(),
            source_commit,
            shared_change,
            false,
            timestamp,
            timestamp,
        );
        second.push(
            identities[0].clone(),
            source_commit,
            ChangeId::for_test_label("duplicate-identity"),
            false,
            timestamp,
            timestamp,
        );

        let mut staged = StagedCommitChangeRefs::default();
        staged.add_selected_change_batch(first.finish());
        staged.add_selected_change_batch(second.finish());
        assert_eq!(staged.selected_change_count(), 2);
        assert_eq!(
            staged
                .selected_changes()
                .map(|change| change.change_id)
                .collect::<Vec<_>>(),
            vec![shared_change, shared_change]
        );
    }
}
