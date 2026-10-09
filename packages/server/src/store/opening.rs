//! An opener belongs to the manager, never to the request waiting for it.
//! A stalled writer remains owned and fenced until it completes or closes;
//! reporting a stall must not detach it and start a competing migration.
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OpenPhase {
    Queued,
    CheckingSource,
    StorageInitializing,
    StorageMigrating,
    StoragePublishing,
    EngineInspecting,
    EngineOpening,
    EngineMigrating,
    Validating,
    Publishing,
    Ready,
    Failed,
}

impl OpenPhase {
    fn name(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::CheckingSource => "checking_source",
            Self::StorageInitializing => "storage_initializing",
            Self::StorageMigrating => "storage_migrating",
            Self::StoragePublishing => "storage_publishing",
            Self::EngineInspecting => "engine_inspecting",
            Self::EngineOpening => "engine_opening",
            Self::EngineMigrating => "engine_migrating",
            Self::Validating => "validating",
            Self::Publishing => "publishing",
            Self::Ready => "ready",
            Self::Failed => "failed",
        }
    }
    fn terminal(self) -> bool {
        matches!(self, Self::Ready | Self::Failed)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OpenSnapshot {
    pub operation_id: String,
    pub phase: &'static str,
    pub dependency: &'static str,
    pub elapsed_ms: u64,
    pub idle_ms: u64,
    pub rows: u64,
    pub bytes: u64,
    /// Completed engine copy pages or repair steps; physical totals stay separate.
    pub completed_groups: Option<u64>,
    pub from_format: Option<u32>,
    pub to_format: u32,
    pub stalled: bool,
    pub failure_code: Option<String>,
    pub failure_phase: Option<&'static str>,
}

impl OpenSnapshot {
    pub(crate) fn details(&self) -> serde_json::Value {
        serde_json::json!({
            "openOperationId": self.operation_id, "openPhase": self.phase,
            "openDependency": self.dependency, "openElapsedMs": self.elapsed_ms,
            "openIdleMs": self.idle_ms, "openRows": self.rows,
            "openBytes": self.bytes, "openCompletedGroups": self.completed_groups,
            "fromVersion": self.from_format, "toVersion": self.to_format,
            "retryable": !self.stalled && self.phase != "failed",
            "openerRetained": self.phase != "failed",
            "openFailurePhase": self.failure_phase,
        })
    }
}

struct Progress {
    phase: OpenPhase,
    dependency: &'static str,
    last_progress: Instant,
    rows: u64,
    bytes: u64,
    completed_groups: Option<u64>,
    from_format: Option<u32>,
    to_format: u32,
    failure_code: Option<String>,
    failure_phase: Option<&'static str>,
    engine_completed: Option<u64>,
}

pub(super) struct OpenOperation {
    id: String,
    repository_id: String,
    started: Instant,
    stall_timeout: Duration,
    progress: Mutex<Progress>,
    changed: Notify,
}

impl OpenOperation {
    pub(super) fn new(repository_id: String, stall_timeout: Duration) -> Arc<Self> {
        Arc::new(Self {
            id: uuid::Uuid::new_v4().to_string(),
            repository_id,
            started: Instant::now(),
            stall_timeout,
            progress: Mutex::new(Progress {
                phase: OpenPhase::Queued,
                dependency: "worker_queue",
                last_progress: Instant::now(),
                rows: 0,
                bytes: 0,
                completed_groups: None,
                from_format: None,
                to_format: lix_sdk::CURRENT_STORAGE_FORMAT_VERSION,
                failure_code: None,
                failure_phase: None,
                engine_completed: None,
            }),
            changed: Notify::new(),
        })
    }

    pub(super) fn advance(
        &self,
        phase: OpenPhase,
        dependency: &'static str,
        rows: u64,
        bytes: u64,
        completed_groups: Option<u64>,
        from_format: Option<u32>,
    ) {
        let mut p = self
            .progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if p.phase.terminal() {
            return;
        }
        // A timer heartbeat or a repeated callback is not evidence of progress.
        let phase_changed = p.phase != phase;
        let progressed = phase_changed
            || rows > p.rows
            || bytes > p.bytes
            || completed_groups.is_some_and(|n| n > p.completed_groups.unwrap_or(0));
        p.phase = phase;
        p.dependency = dependency;
        p.rows = p.rows.max(rows);
        p.bytes = p.bytes.max(bytes);
        if let Some(n) = completed_groups {
            p.completed_groups = Some(n.max(p.completed_groups.unwrap_or(0)));
        }
        p.from_format = from_format.or(p.from_format);
        if progressed {
            p.last_progress = Instant::now();
        }
        drop(p);
        if phase_changed {
            self.changed.notify_one();
        }
    }

    pub(super) fn phase(&self, phase: OpenPhase, dependency: &'static str) {
        self.advance(phase, dependency, 0, 0, None, None);
    }

    pub(super) fn fail(&self, error: &anyhow::Error) {
        let code = error
            .chain()
            .filter_map(|source| source.downcast_ref::<lix_sdk::LixError>())
            .map(|error| error.code.as_str())
            .find(|code| {
                code.len() <= 128
                    && code.starts_with("LIX_")
                    && code
                        .bytes()
                        .all(|b| b.is_ascii_uppercase() || b == b'_' || b.is_ascii_digit())
            })
            .unwrap_or("LIX_OPEN_FAILED")
            .to_owned();
        let dependency = {
            let mut p = self
                .progress
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if p.phase.terminal() {
                return;
            }
            p.failure_code = Some(code);
            p.failure_phase = Some(p.phase.name());
            p.dependency
        };
        self.phase(OpenPhase::Failed, dependency);
    }

    pub(super) fn snapshot(&self) -> OpenSnapshot {
        let p = self
            .progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        OpenSnapshot {
            operation_id: self.id.clone(),
            phase: p.phase.name(),
            dependency: p.dependency,
            elapsed_ms: millis(self.started.elapsed()),
            idle_ms: millis(p.last_progress.elapsed()),
            rows: p.rows,
            bytes: p.bytes,
            completed_groups: p.completed_groups,
            from_format: p.from_format,
            to_format: p.to_format,
            failure_code: p.failure_code.clone(),
            failure_phase: p.failure_phase,
            stalled: !p.phase.terminal() && p.last_progress.elapsed() >= self.stall_timeout,
        }
    }

    pub(super) fn storage_progress(&self, event: lix_slatedb_storage::SlateDBOpenProgress) {
        use lix_slatedb_storage::{SlateDBOpenPhase as P, SlateDBOpenProgressEvent as E};
        let phase = match event.phase {
            P::LegacyMigration => OpenPhase::StorageMigrating,
            P::MigrationPublication => OpenPhase::StoragePublishing,
            P::CurrentStorageInitialization
            | P::SourceLayoutCheck
            | P::LegacyStorageInitialization
            | P::Ready => OpenPhase::StorageInitializing,
        };
        self.advance(
            phase,
            event.dependency.as_str(),
            event.rows,
            event.bytes,
            None,
            None,
        );
        if matches!(
            event.event,
            E::DependencyFinished | E::CheckpointDurable | E::Ready
        ) {
            let mut p = self
                .progress
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !p.phase.terminal() {
                p.last_progress = Instant::now();
            }
            drop(p);
        }
    }

    pub(super) fn engine_progress(&self, event: lix_sdk::OpenProgress) {
        let phase = match event.phase {
            lix_sdk::OpenPhase::Inspecting => OpenPhase::EngineInspecting,
            lix_sdk::OpenPhase::Migrating => OpenPhase::EngineMigrating,
            lix_sdk::OpenPhase::Validating => OpenPhase::Validating,
            lix_sdk::OpenPhase::Opening | lix_sdk::OpenPhase::Complete => OpenPhase::EngineOpening,
            _ => return,
        };
        // SDK stages count different units (copied rows, then repair steps).
        // Count completed work groups here, rather than exposing a mixed-unit
        // scalar that resets between stages. A zero/reset is not completed work.
        let completed_groups = {
            let mut p = self
                .progress
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if p.phase.terminal() {
                return;
            }
            if event.phase == lix_sdk::OpenPhase::Migrating {
                if let Some(n) = event.completed {
                    if n > p.engine_completed.unwrap_or(0) {
                        p.completed_groups =
                            Some(p.completed_groups.unwrap_or(0).saturating_add(1));
                        p.last_progress = Instant::now();
                    }
                    p.engine_completed = Some(n);
                }
            } else {
                p.engine_completed = None;
            }
            p.completed_groups
        };
        self.advance(phase, "engine", 0, 0, completed_groups, event.from_format);
    }

    /// Export short, completed observations even when the parent never ends.
    pub(super) async fn monitor(self: Arc<Self>) {
        let mut was_stalled = false;
        loop {
            let s = self.snapshot();
            let span = tracing::info_span!("lix.repository.open.progress",
                "lix.id" = %self.repository_id, "lix.open.operation_id" = %s.operation_id,
                "lix.open.phase" = s.phase, "lix.open.dependency" = s.dependency,
                "lix.open.elapsed_ms" = s.elapsed_ms, "lix.open.idle_ms" = s.idle_ms,
                "lix.open.rows" = s.rows, "lix.open.bytes" = s.bytes,
                "lix.open.stalled" = s.stalled, "lix.error.owner" = "opening",
                "lix.open.opener_retained" = s.phase != "failed",
                "otel.status_code" = tracing::field::Empty,
                "error.type" = tracing::field::Empty);
            span.set_attribute("lix.open.to_format", i64::from(s.to_format));
            if let Some(phase) = s.failure_phase {
                span.set_attribute("lix.open.failed_phase", phase);
            }
            if let Some(v) = s.from_format {
                span.set_attribute("lix.open.from_format", i64::from(v));
            }
            if let Some(v) = s.completed_groups {
                span.set_attribute("lix.open.completed_groups", v as i64);
            }
            if (s.stalled && !was_stalled) || s.phase == "failed" {
                span.record("otel.status_code", "ERROR");
                span.record(
                    "error.type",
                    if s.stalled {
                        "LIX_OPEN_STALLED"
                    } else {
                        s.failure_code.as_deref().unwrap_or("LIX_OPEN_FAILED")
                    },
                );
                if s.stalled {
                    span.in_scope(|| tracing::error!("repository opener stopped making observable progress; opener remains owned"));
                } else {
                    span.in_scope(|| {
                        tracing::error!("repository opener returned a terminal failure")
                    });
                }
            }
            was_stalled = s.stalled;
            // Drop before waiting: exports must not depend on opener completion.
            drop(span);
            if matches!(s.phase, "ready" | "failed") {
                return;
            }
            tokio::select! {
                _ = self.changed.notified() => {},
                _ = tokio::time::sleep(Duration::from_secs(10).min(self.stall_timeout)) => {},
            }
        }
    }
}

fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod state_tests {
    use super::*;
    #[test]
    fn engine_progress_counts_work_groups_across_row_and_step_counter_resets() {
        let op = OpenOperation::new("repo".into(), Duration::from_secs(1));
        for (completed, groups) in [(32, 1), (64, 2), (0, 2), (1, 3), (1, 3)] {
            op.engine_progress(lix_sdk::OpenProgress {
                scope: lix_sdk::OpenScope::Local,
                phase: lix_sdk::OpenPhase::Migrating,
                from_format: Some(79),
                to_format: 87,
                completed: Some(completed),
                total: None,
            });
            assert_eq!(op.snapshot().completed_groups, Some(groups));
        }
        op.phase(OpenPhase::Validating, "engine");
        assert_eq!(op.snapshot().completed_groups, Some(3));
    }

    #[test]
    fn terminal_failure_retains_dependency_and_only_reports_a_safe_error_code() {
        let op = OpenOperation::new("repo".into(), Duration::from_secs(1));
        op.phase(OpenPhase::StoragePublishing, "migration_publication_write");
        op.fail(&anyhow::Error::new(lix_sdk::LixError::new(
            "LIX_STORAGE_ERROR",
            "private object key and credentials",
        )));
        let failure = op.snapshot();
        assert_eq!(failure.phase, "failed");
        assert_eq!(failure.failure_phase, Some("storage_publishing"));
        assert_eq!(failure.dependency, "migration_publication_write");
        assert_eq!(failure.failure_code.as_deref(), Some("LIX_STORAGE_ERROR"));
        assert!(!failure.stalled);
        op.phase(OpenPhase::Ready, "none");
        assert_eq!(op.snapshot().phase, "failed");
        let invalid = OpenOperation::new("repo".into(), Duration::from_secs(1));
        invalid.fail(&anyhow::Error::new(lix_sdk::LixError::new(
            "LIX_secret-key",
            "private",
        )));
        assert_eq!(
            invalid.snapshot().failure_code.as_deref(),
            Some("LIX_OPEN_FAILED")
        );
    }

    #[test]
    fn heartbeat_does_not_disguise_a_stall_and_real_progress_resumes() {
        let op = OpenOperation::new("repo".into(), Duration::from_secs(1));
        op.progress.lock().unwrap().last_progress = Instant::now() - Duration::from_secs(2);
        op.phase(OpenPhase::Queued, "worker_queue");
        assert!(op.snapshot().stalled);
        op.advance(
            OpenPhase::StorageMigrating,
            "checkpoint",
            10,
            1024,
            None,
            None,
        );
        assert!(!op.snapshot().stalled);
        assert_eq!(op.snapshot().rows, 10);
        let id = op.snapshot().operation_id;
        op.phase(OpenPhase::Ready, "none");
        op.phase(OpenPhase::EngineMigrating, "engine");
        assert_eq!(op.snapshot().phase, "ready");
        assert_eq!(op.snapshot().operation_id, id);
    }
}

#[cfg(test)]
mod tests;
