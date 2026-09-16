//! Process-local synchronization health, independent of local SQL availability.
use crate::LixError;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SyncPhase {
    Descriptor,
    Publication,
    Upload,
    Lease,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncFailure {
    pub code: String,
    pub message: String,
}
impl From<&LixError> for SyncFailure {
    fn from(error: &LixError) -> Self {
        Self {
            code: error.code.clone(),
            message: error.message.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SyncHealthState {
    #[default]
    Inactive,
    Running,
    Stalled,
    Failed,
    Stopped,
}

/// A local worker snapshot. Running describes worker health, not a guarantee
/// that no unseen remote writes exist. Cursors describe repository sync progress.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncHealth {
    pub state: SyncHealthState,
    pub applied_cursor: Option<u64>,
    pub observed_cursor: Option<u64>,
    pub failures: BTreeMap<SyncPhase, SyncFailure>,
    pub terminal_error: Option<SyncFailure>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SyncHealthTracker(Arc<RwLock<SyncHealth>>);
impl SyncHealthTracker {
    pub(crate) fn snapshot(&self) -> SyncHealth {
        self.0.read().clone()
    }
    pub(crate) fn started(&self, cursor: u64) {
        *self.0.write() = SyncHealth {
            state: SyncHealthState::Running,
            applied_cursor: Some(cursor),
            observed_cursor: Some(cursor),
            ..Default::default()
        };
    }
    pub(crate) fn observed(&self, cursor: u64) {
        let mut value = self.0.write();
        value.observed_cursor = Some(value.observed_cursor.unwrap_or(0).max(cursor));
    }
    pub(crate) fn applied(&self, cursor: u64) {
        let mut value = self.0.write();
        value.applied_cursor = Some(cursor);
        value.observed_cursor = Some(value.observed_cursor.unwrap_or(0).max(cursor));
    }
    pub(crate) fn failed(&self, phase: SyncPhase, error: &LixError) {
        let mut value = self.0.write();
        value.failures.insert(phase, error.into());
        if matches!(
            value.state,
            SyncHealthState::Running | SyncHealthState::Stalled
        ) {
            value.state = SyncHealthState::Stalled;
        }
    }
    pub(crate) fn succeeded(&self, phase: SyncPhase) {
        let mut value = self.0.write();
        value.failures.remove(&phase);
        if value.state == SyncHealthState::Stalled && value.failures.is_empty() {
            value.state = SyncHealthState::Running;
        }
    }
    pub(crate) fn stopped(&self, error: Option<&LixError>) {
        let mut value = self.0.write();
        value.terminal_error = error.map(Into::into);
        value.state = if error.is_some() {
            SyncHealthState::Failed
        } else {
            SyncHealthState::Stopped
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unrelated_success_preserves_stall_and_shared_progress() {
        let tracker = SyncHealthTracker::default();
        let sibling = tracker.clone();
        tracker.started(492);
        tracker.observed(543);
        tracker.failed(
            SyncPhase::Descriptor,
            &LixError::new("OFFLINE", "unavailable"),
        );
        tracker.succeeded(SyncPhase::Lease);
        tracker.succeeded(SyncPhase::Upload);
        assert_eq!(sibling.snapshot().state, SyncHealthState::Stalled);
        assert_eq!(sibling.snapshot().applied_cursor, Some(492));
        assert_eq!(sibling.snapshot().observed_cursor, Some(543));
        tracker.applied(543);
        tracker.succeeded(SyncPhase::Descriptor);
        assert_eq!(sibling.snapshot().state, SyncHealthState::Running);
        tracker.stopped(Some(&LixError::new("AUTH", "rejected")));
        tracker.succeeded(SyncPhase::Lease);
        assert_eq!(sibling.snapshot().state, SyncHealthState::Failed);
        assert_eq!(sibling.snapshot().terminal_error.unwrap().code, "AUTH");
    }
    #[tokio::test]
    async fn successful_sql_does_not_hide_sync_failure() {
        let lix = crate::open_lix().await.unwrap();
        let tracker = lix.sync_mode_state().health();
        tracker.started(492);
        tracker.observed(543);
        tracker.failed(
            SyncPhase::Descriptor,
            &LixError::new("OFFLINE", "unavailable"),
        );
        for _ in 0..3 {
            lix.execute("SELECT 1", &[]).await.unwrap();
        }
        assert_eq!(lix.sync_health().state, SyncHealthState::Stalled);
        assert_eq!(lix.sync_health().applied_cursor, Some(492));
        assert_eq!(lix.sync_health().observed_cursor, Some(543));
        assert_eq!(
            lix.sync_health().failures[&SyncPhase::Descriptor].code,
            "OFFLINE"
        );
        lix.close().await.unwrap();
    }
}
