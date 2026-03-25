use std::collections::BTreeSet;

use async_trait::async_trait;

use crate::live_state::constraints::matches_constraints;
use crate::live_state::effective;
use crate::live_state::shared::identity::RowIdentity;
use crate::live_state::shared::views::ReadViews;
use crate::live_state::tracked::{
    BatchTrackedRowRequest, ExactTrackedRowRequest, TrackedReadView, TrackedRow,
    TrackedScanRequest, TrackedTombstoneMarker, TrackedTombstoneView,
};
use crate::live_state::untracked::{
    BatchUntrackedRowRequest, ExactUntrackedRowRequest, UntrackedReadView, UntrackedRow,
    UntrackedScanRequest,
};
use crate::LixError;

use super::overlay::PendingWriteOverlay;

pub struct ReadContext<'a> {
    base: ReadViews<'a>,
}

impl<'a> ReadContext<'a> {
    pub fn new(tracked: &'a dyn TrackedReadView, untracked: &'a dyn UntrackedReadView) -> Self {
        Self {
            base: ReadViews::new(tracked, untracked),
        }
    }

    pub fn with_tracked_tombstones(
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
        }
    }
}

pub(crate) struct PendingReadContext<'a> {
    tracked: PendingTrackedReadView<'a>,
    untracked: PendingUntrackedReadView<'a>,
    tracked_tombstones: PendingTrackedTombstoneView<'a>,
}

impl<'a> PendingReadContext<'a> {
    pub(crate) fn effective_state_context(&'a self) -> effective::ReadContext<'a> {
        let context = effective::ReadContext::new(&self.tracked, &self.untracked);
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

impl PendingTrackedTombstoneView<'_> {
    fn has_source(&self) -> bool {
        self.base.is_some() || self.pending.has_tombstones()
    }
}

#[async_trait(?Send)]
impl TrackedReadView for PendingTrackedReadView<'_> {
    async fn load_exact_row(
        &self,
        request: &ExactTrackedRowRequest,
    ) -> Result<Option<TrackedRow>, LixError> {
        if let Some(row) = pending_tracked_row(self.pending, request) {
            return Ok(Some(row.clone()));
        }
        if pending_tracked_tombstone(self.pending, request).is_some() {
            return Ok(None);
        }
        self.base.load_exact_row(request).await
    }

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
    async fn load_exact_row(
        &self,
        request: &ExactUntrackedRowRequest,
    ) -> Result<Option<UntrackedRow>, LixError> {
        if let Some(row) = pending_untracked_row(self.pending, request) {
            return Ok(Some(row.clone()));
        }
        if pending_untracked_delete(self.pending, request) {
            return Ok(None);
        }
        self.base.load_exact_row(request).await
    }

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
    async fn load_exact_tombstone(
        &self,
        request: &ExactTrackedRowRequest,
    ) -> Result<Option<TrackedTombstoneMarker>, LixError> {
        if let Some(row) = pending_tracked_tombstone(self.pending, request) {
            return Ok(Some(row.clone()));
        }
        if pending_tracked_row(self.pending, request).is_some() {
            return Ok(None);
        }
        match self.base {
            Some(base) => base.load_exact_tombstone(request).await,
            None => Ok(None),
        }
    }

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

fn pending_tracked_row<'a>(
    pending: &'a PendingWriteOverlay,
    request: &ExactTrackedRowRequest,
) -> Option<&'a TrackedRow> {
    pending
        .tracked_rows()
        .iter()
        .find(|(identity, _)| identity.matches_exact(request))
        .map(|(_, row)| row)
}

fn pending_tracked_tombstone<'a>(
    pending: &'a PendingWriteOverlay,
    request: &ExactTrackedRowRequest,
) -> Option<&'a TrackedTombstoneMarker> {
    pending
        .tracked_tombstones()
        .iter()
        .find(|(identity, _)| identity.matches_exact(request))
        .map(|(_, row)| row)
}

fn pending_untracked_row<'a>(
    pending: &'a PendingWriteOverlay,
    request: &ExactUntrackedRowRequest,
) -> Option<&'a UntrackedRow> {
    pending
        .untracked_rows()
        .iter()
        .find(|(identity, _)| identity.matches_exact(request))
        .map(|(_, row)| row)
}

fn pending_untracked_delete(
    pending: &PendingWriteOverlay,
    request: &ExactUntrackedRowRequest,
) -> bool {
    pending
        .untracked_deletes()
        .iter()
        .any(|identity| identity.matches_exact(request))
}
