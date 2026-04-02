use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;

use crate::contracts::artifacts::{
    matches_constraints, BatchRowRequest, RowIdentity, ScanRequest, TrackedRow,
    TrackedTombstoneMarker, UntrackedRow,
};
use crate::contracts::traits::{
    LiveReadContext, TrackedReadView, TrackedTombstoneView, UntrackedReadView,
};
use crate::workspace::writer_key::WorkspaceWriterKeyReadView;
use crate::LixError;

use crate::write_runtime::overlay::PendingWriteOverlay;

type BatchTrackedRowRequest = BatchRowRequest;
type BatchUntrackedRowRequest = BatchRowRequest;
type TrackedScanRequest = ScanRequest;
type UntrackedScanRequest = ScanRequest;

pub struct ReadContext<'a> {
    base: LiveReadContext<'a>,
}

impl<'a> ReadContext<'a> {
    #[cfg(test)]
    pub(crate) fn new(
        tracked: &'a dyn TrackedReadView,
        untracked: &'a dyn UntrackedReadView,
        workspace_writer_keys: &'a dyn WorkspaceWriterKeyReadView,
    ) -> Self {
        Self {
            base: LiveReadContext::new(tracked, untracked, workspace_writer_keys),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_tracked_tombstones(
        mut self,
        tracked_tombstones: &'a dyn TrackedTombstoneView,
    ) -> Self {
        self.base = self.base.with_tracked_tombstones(tracked_tombstones);
        self
    }

    pub(crate) fn with_pending<'b>(
        &'b self,
        pending: &'b PendingWriteOverlay,
    ) -> PendingReadContext<'b> {
        PendingReadContext {
            tracked: PendingTrackedReadView {
                base: self.base.tracked,
                pending,
            },
            untracked: PendingUntrackedReadView {
                base: self.base.untracked,
                pending,
            },
            tracked_tombstones: PendingTrackedTombstoneView {
                base: self.base.tracked_tombstones,
                pending,
            },
            workspace_writer_keys: PendingWorkspaceWriterKeyReadView {
                base: self.base.workspace_writer_keys,
                pending,
            },
        }
    }
}

pub(crate) struct PendingReadContext<'a> {
    tracked: PendingTrackedReadView<'a>,
    untracked: PendingUntrackedReadView<'a>,
    tracked_tombstones: PendingTrackedTombstoneView<'a>,
    workspace_writer_keys: PendingWorkspaceWriterKeyReadView<'a>,
}

impl<'a> PendingReadContext<'a> {
    pub(crate) fn effective_state_context(&'a self) -> LiveReadContext<'a> {
        let context =
            LiveReadContext::new(&self.tracked, &self.untracked, &self.workspace_writer_keys);
        if self.tracked_tombstones.has_source() {
            context.with_tracked_tombstones(&self.tracked_tombstones)
        } else {
            context
        }
    }
}

struct PendingTrackedReadView<'a> {
    base: &'a dyn TrackedReadView,
    pending: &'a PendingWriteOverlay,
}

struct PendingUntrackedReadView<'a> {
    base: &'a dyn UntrackedReadView,
    pending: &'a PendingWriteOverlay,
}

struct PendingTrackedTombstoneView<'a> {
    base: Option<&'a dyn TrackedTombstoneView>,
    pending: &'a PendingWriteOverlay,
}

struct PendingWorkspaceWriterKeyReadView<'a> {
    base: &'a dyn WorkspaceWriterKeyReadView,
    pending: &'a PendingWriteOverlay,
}

impl PendingTrackedTombstoneView<'_> {
    fn has_source(&self) -> bool {
        self.base.is_some() || self.pending.has_tombstones()
    }
}

#[async_trait(?Send)]
impl WorkspaceWriterKeyReadView for PendingWorkspaceWriterKeyReadView<'_> {
    async fn load_annotation(
        &self,
        row_identity: &RowIdentity,
    ) -> Result<Option<String>, LixError> {
        if let Some(annotation) =
            pending_workspace_writer_key_annotation(self.pending, row_identity)
        {
            return Ok(annotation);
        }
        self.base.load_annotation(row_identity).await
    }

    async fn load_annotations(
        &self,
        row_identities: &BTreeSet<RowIdentity>,
    ) -> Result<BTreeMap<RowIdentity, Option<String>>, LixError> {
        let mut annotations = self.base.load_annotations(row_identities).await?;
        for row_identity in row_identities {
            if let Some(annotation) =
                pending_workspace_writer_key_annotation(self.pending, row_identity)
            {
                annotations.insert(row_identity.clone(), annotation);
            }
        }
        Ok(annotations)
    }
}

#[async_trait(?Send)]
impl TrackedReadView for PendingTrackedReadView<'_> {
    async fn load_exact_rows(
        &self,
        request: &BatchTrackedRowRequest,
    ) -> Result<Vec<TrackedRow>, LixError> {
        let mut rows = self.base.load_exact_rows(request).await?;
        let mut shadowed = BTreeSet::new();

        for (identity, _row) in self.pending.tracked_rows() {
            if matches_tracked_batch_request(identity, request) {
                shadowed.insert(identity.clone());
            }
        }
        for identity in self.pending.tracked_tombstones().keys() {
            if matches_tracked_batch_request(identity, request) {
                shadowed.insert(identity.clone());
            }
        }

        rows.retain(|row| !shadowed.contains(&RowIdentity::from_tracked_row(row)));
        rows.extend(
            self.pending
                .tracked_rows()
                .iter()
                .filter(|(identity, _)| matches_tracked_batch_request(identity, request))
                .map(|(_, row)| row.clone()),
        );
        sort_tracked_rows(&mut rows);
        rows.dedup_by(|left, right| {
            RowIdentity::from_tracked_row(left) == RowIdentity::from_tracked_row(right)
        });
        Ok(rows)
    }

    async fn scan_rows(&self, request: &TrackedScanRequest) -> Result<Vec<TrackedRow>, LixError> {
        let mut rows = self.base.scan_rows(request).await?;
        let mut shadowed = BTreeSet::new();

        for (identity, row) in self.pending.tracked_rows() {
            if matches_tracked_scan_request(identity, row, request) {
                shadowed.insert(identity.clone());
            }
        }
        for identity in self.pending.tracked_tombstones().keys() {
            if matches_tracked_scan_identity(identity, request) {
                shadowed.insert(identity.clone());
            }
        }

        rows.retain(|row| !shadowed.contains(&RowIdentity::from_tracked_row(row)));
        rows.extend(
            self.pending
                .tracked_rows()
                .iter()
                .filter(|(identity, row)| matches_tracked_scan_request(identity, row, request))
                .map(|(_, row)| row.clone()),
        );
        sort_tracked_rows(&mut rows);
        Ok(rows)
    }
}

#[async_trait(?Send)]
impl UntrackedReadView for PendingUntrackedReadView<'_> {
    async fn load_exact_rows(
        &self,
        request: &BatchUntrackedRowRequest,
    ) -> Result<Vec<UntrackedRow>, LixError> {
        let mut rows = self.base.load_exact_rows(request).await?;
        let mut shadowed = BTreeSet::new();

        for (identity, _row) in self.pending.untracked_rows() {
            if matches_untracked_batch_request(identity, request) {
                shadowed.insert(identity.clone());
            }
        }
        for identity in self.pending.untracked_deletes() {
            if matches_untracked_batch_request(identity, request) {
                shadowed.insert(identity.clone());
            }
        }

        rows.retain(|row| !shadowed.contains(&RowIdentity::from_untracked_row(row)));
        rows.extend(
            self.pending
                .untracked_rows()
                .iter()
                .filter(|(identity, _)| matches_untracked_batch_request(identity, request))
                .map(|(_, row)| row.clone()),
        );
        sort_untracked_rows(&mut rows);
        rows.dedup_by(|left, right| {
            RowIdentity::from_untracked_row(left) == RowIdentity::from_untracked_row(right)
        });
        Ok(rows)
    }

    async fn scan_rows(
        &self,
        request: &UntrackedScanRequest,
    ) -> Result<Vec<UntrackedRow>, LixError> {
        let mut rows = self.base.scan_rows(request).await?;
        let mut shadowed = BTreeSet::new();

        for (identity, row) in self.pending.untracked_rows() {
            if matches_untracked_scan_request(identity, row, request) {
                shadowed.insert(identity.clone());
            }
        }
        for identity in self.pending.untracked_deletes() {
            if matches_untracked_scan_identity(identity, request) {
                shadowed.insert(identity.clone());
            }
        }

        rows.retain(|row| !shadowed.contains(&RowIdentity::from_untracked_row(row)));
        rows.extend(
            self.pending
                .untracked_rows()
                .iter()
                .filter(|(identity, row)| matches_untracked_scan_request(identity, row, request))
                .map(|(_, row)| row.clone()),
        );
        sort_untracked_rows(&mut rows);
        Ok(rows)
    }
}

#[async_trait(?Send)]
impl TrackedTombstoneView for PendingTrackedTombstoneView<'_> {
    async fn scan_tombstones(
        &self,
        request: &TrackedScanRequest,
    ) -> Result<Vec<TrackedTombstoneMarker>, LixError> {
        let mut rows = match self.base {
            Some(base) => base.scan_tombstones(request).await?,
            None => Vec::new(),
        };
        let mut shadowed = BTreeSet::new();

        for identity in self.pending.tracked_rows().keys() {
            if matches_tracked_scan_identity(identity, request) {
                shadowed.insert(identity.clone());
            }
        }
        for identity in self.pending.tracked_tombstones().keys() {
            if matches_tracked_scan_identity(identity, request) {
                shadowed.insert(identity.clone());
            }
        }

        rows.retain(|row| !shadowed.contains(&RowIdentity::from_tombstone(row)));
        rows.extend(
            self.pending
                .tracked_tombstones()
                .iter()
                .filter(|(identity, _)| matches_tracked_scan_identity(identity, request))
                .map(|(_, row)| row.clone()),
        );
        rows.sort_by_key(RowIdentity::from_tombstone);
        Ok(rows)
    }
}

fn matches_tracked_batch_request(identity: &RowIdentity, request: &BatchTrackedRowRequest) -> bool {
    identity.matches_batch(request)
}

fn matches_untracked_batch_request(
    identity: &RowIdentity,
    request: &BatchUntrackedRowRequest,
) -> bool {
    identity.matches_batch(request)
}

fn matches_tracked_scan_request(
    identity: &RowIdentity,
    row: &TrackedRow,
    request: &TrackedScanRequest,
) -> bool {
    matches_tracked_scan_identity(identity, request)
        && matches_constraints(
            &row.entity_id,
            &row.file_id,
            &row.plugin_key,
            &row.schema_version,
            &request.constraints,
        )
}

fn matches_untracked_scan_request(
    identity: &RowIdentity,
    row: &UntrackedRow,
    request: &UntrackedScanRequest,
) -> bool {
    matches_untracked_scan_identity(identity, request)
        && matches_constraints(
            &row.entity_id,
            &row.file_id,
            &row.plugin_key,
            &row.schema_version,
            &request.constraints,
        )
}

fn matches_tracked_scan_identity(identity: &RowIdentity, request: &TrackedScanRequest) -> bool {
    identity.matches_scan_partition(request)
}

fn matches_untracked_scan_identity(identity: &RowIdentity, request: &UntrackedScanRequest) -> bool {
    identity.matches_scan_partition(request)
}

fn sort_tracked_rows(rows: &mut [TrackedRow]) {
    rows.sort_by_key(RowIdentity::from_tracked_row);
}

fn sort_untracked_rows(rows: &mut [UntrackedRow]) {
    rows.sort_by_key(RowIdentity::from_untracked_row);
}

fn pending_workspace_writer_key_annotation(
    pending: &PendingWriteOverlay,
    row_identity: &RowIdentity,
) -> Option<Option<String>> {
    if let Some(row) = pending.tracked_rows().get(row_identity) {
        return Some(row.writer_key.clone());
    }
    pending
        .tracked_tombstones()
        .get(row_identity)
        .map(|row| row.writer_key.clone())
}
