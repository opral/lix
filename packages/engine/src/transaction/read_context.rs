use std::collections::BTreeSet;

use async_trait::async_trait;

use crate::constraints::{ScanConstraint, ScanField, ScanOperator};
use crate::effective_state;
use crate::live_tracked_state::{
    BatchTrackedRowRequest, ExactTrackedRowRequest, TrackedReadView, TrackedRow,
    TrackedScanRequest, TrackedTombstoneMarker, TrackedTombstoneView,
};
use crate::live_untracked_state::{
    BatchUntrackedRowRequest, ExactUntrackedRowRequest, UntrackedReadView, UntrackedRow,
    UntrackedScanRequest,
};
use crate::{LixError, Value};

use super::participants::{PendingTxnParticipants, RowIdentity};

pub struct ReadContext<'a> {
    tracked: &'a dyn TrackedReadView,
    untracked: &'a dyn UntrackedReadView,
    tracked_tombstones: Option<&'a dyn TrackedTombstoneView>,
}

impl<'a> ReadContext<'a> {
    pub fn new(tracked: &'a dyn TrackedReadView, untracked: &'a dyn UntrackedReadView) -> Self {
        Self {
            tracked,
            untracked,
            tracked_tombstones: None,
        }
    }

    pub fn with_tracked_tombstones(
        mut self,
        tracked_tombstones: &'a dyn TrackedTombstoneView,
    ) -> Self {
        self.tracked_tombstones = Some(tracked_tombstones);
        self
    }

    pub(crate) fn with_pending<'b>(
        &'b self,
        pending: &'b PendingTxnParticipants,
    ) -> PendingReadContext<'b> {
        PendingReadContext {
            tracked: PendingTrackedReadView {
                base: self.tracked,
                pending,
            },
            untracked: PendingUntrackedReadView {
                base: self.untracked,
                pending,
            },
            tracked_tombstones: PendingTrackedTombstoneView {
                base: self.tracked_tombstones,
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
    pub(crate) fn effective_state_context(&'a self) -> effective_state::ReadContext<'a> {
        let context = effective_state::ReadContext::new(&self.tracked, &self.untracked);
        if self.tracked_tombstones.has_source() {
            context.with_tracked_tombstones(&self.tracked_tombstones)
        } else {
            context
        }
    }
}

struct PendingTrackedReadView<'a> {
    base: &'a dyn TrackedReadView,
    pending: &'a PendingTxnParticipants,
}

struct PendingUntrackedReadView<'a> {
    base: &'a dyn UntrackedReadView,
    pending: &'a PendingTxnParticipants,
}

struct PendingTrackedTombstoneView<'a> {
    base: Option<&'a dyn TrackedTombstoneView>,
    pending: &'a PendingTxnParticipants,
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
        rows.dedup_by(|left, right| RowIdentity::from_tracked_row(left) == RowIdentity::from_tracked_row(right));
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
    identity.schema_key == request.schema_key
        && identity.version_id == request.version_id
        && request.entity_ids.contains(&identity.entity_id)
        && request
            .file_id
            .as_ref()
            .is_none_or(|file_id| identity.file_id == *file_id)
}

fn matches_untracked_batch_request(
    identity: &RowIdentity,
    request: &BatchUntrackedRowRequest,
) -> bool {
    identity.schema_key == request.schema_key
        && identity.version_id == request.version_id
        && request.entity_ids.contains(&identity.entity_id)
        && request
            .file_id
            .as_ref()
            .is_none_or(|file_id| identity.file_id == *file_id)
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
    identity.schema_key == request.schema_key && identity.version_id == request.version_id
}

fn matches_untracked_scan_identity(identity: &RowIdentity, request: &UntrackedScanRequest) -> bool {
    identity.schema_key == request.schema_key && identity.version_id == request.version_id
}

fn matches_constraints(
    entity_id: &str,
    file_id: &str,
    plugin_key: &str,
    schema_version: &str,
    constraints: &[ScanConstraint],
) -> bool {
    constraints.iter().all(|constraint| {
        let candidate = match constraint.field {
            ScanField::EntityId => entity_id,
            ScanField::FileId => file_id,
            ScanField::PluginKey => plugin_key,
            ScanField::SchemaVersion => schema_version,
        };
        matches_constraint(candidate, &constraint.operator)
    })
}

fn matches_constraint(candidate: &str, operator: &ScanOperator) -> bool {
    match operator {
        ScanOperator::Eq(value) => text_value(value).is_some_and(|value| value == candidate),
        ScanOperator::In(values) => values
            .iter()
            .filter_map(text_value)
            .any(|value| value == candidate),
        ScanOperator::Range { lower, upper } => {
            lower
                .as_ref()
                .is_none_or(|bound| compare_lower(candidate, &bound.value, bound.inclusive))
                && upper
                    .as_ref()
                    .is_none_or(|bound| compare_upper(candidate, &bound.value, bound.inclusive))
        }
    }
}

fn compare_lower(candidate: &str, bound: &Value, inclusive: bool) -> bool {
    text_value(bound).is_some_and(|value| {
        if inclusive {
            candidate >= value
        } else {
            candidate > value
        }
    })
}

fn compare_upper(candidate: &str, bound: &Value, inclusive: bool) -> bool {
    text_value(bound).is_some_and(|value| {
        if inclusive {
            candidate <= value
        } else {
            candidate < value
        }
    })
}

fn text_value(value: &Value) -> Option<&str> {
    match value {
        Value::Text(value) => Some(value.as_str()),
        _ => None,
    }
}

fn sort_tracked_rows(rows: &mut [TrackedRow]) {
    rows.sort_by_key(RowIdentity::from_tracked_row);
}

fn sort_untracked_rows(rows: &mut [UntrackedRow]) {
    rows.sort_by_key(RowIdentity::from_untracked_row);
}

fn pending_tracked_row<'a>(
    pending: &'a PendingTxnParticipants,
    request: &ExactTrackedRowRequest,
) -> Option<&'a TrackedRow> {
    pending
        .tracked_rows()
        .iter()
        .find(|(identity, _)| matches_exact_identity(identity, &request.schema_key, &request.version_id, &request.entity_id, request.file_id.as_deref()))
        .map(|(_, row)| row)
}

fn pending_tracked_tombstone<'a>(
    pending: &'a PendingTxnParticipants,
    request: &ExactTrackedRowRequest,
) -> Option<&'a TrackedTombstoneMarker> {
    pending
        .tracked_tombstones()
        .iter()
        .find(|(identity, _)| matches_exact_identity(identity, &request.schema_key, &request.version_id, &request.entity_id, request.file_id.as_deref()))
        .map(|(_, row)| row)
}

fn pending_untracked_row<'a>(
    pending: &'a PendingTxnParticipants,
    request: &ExactUntrackedRowRequest,
) -> Option<&'a UntrackedRow> {
    pending
        .untracked_rows()
        .iter()
        .find(|(identity, _)| matches_exact_identity(identity, &request.schema_key, &request.version_id, &request.entity_id, request.file_id.as_deref()))
        .map(|(_, row)| row)
}

fn pending_untracked_delete(
    pending: &PendingTxnParticipants,
    request: &ExactUntrackedRowRequest,
) -> bool {
    pending
        .untracked_deletes()
        .iter()
        .any(|identity| matches_exact_identity(identity, &request.schema_key, &request.version_id, &request.entity_id, request.file_id.as_deref()))
}

fn matches_exact_identity(
    identity: &RowIdentity,
    schema_key: &str,
    version_id: &str,
    entity_id: &str,
    file_id: Option<&str>,
) -> bool {
    identity.schema_key == schema_key
        && identity.version_id == version_id
        && identity.entity_id == entity_id
        && file_id.is_none_or(|file_id| identity.file_id == file_id)
}
