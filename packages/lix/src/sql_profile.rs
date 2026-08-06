//! Benchmark-only SQL phase and scan diagnostics.

use std::cell::RefCell;
use std::future::Future;
use std::time::Duration;

use crate::{ExecuteResult, LixError};

tokio::task_local! {
    static ACTIVE_PROFILE: RefCell<SqlReadProfile>;
}

/// Disjoint wall-clock phases for one public columnar SQL read.
///
/// Scan elapsed time is operator poll time summed across scan partitions. It
/// can overlap physical execution and therefore is diagnostic rather than a
/// fifth wall-clock phase.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SqlReadProfile {
    pub total: Duration,
    pub logical_planning: Duration,
    pub physical_planning: Duration,
    pub arrow_execution: Duration,
    pub public_result_materialization: Duration,
    pub scan_elapsed: Duration,
    pub scan_rows: u64,
    pub scan_batches: u64,
    /// Sum of Arrow's in-memory array sizes at the scan output boundary.
    /// This is not the number of bytes read from the storage backend.
    pub scan_arrow_bytes: u64,
}

impl SqlReadProfile {
    /// Time inside the public call outside the four instrumented phases, such
    /// as snapshot acquisition, provider registration, and statement routing.
    pub fn unattributed_overhead(&self) -> Duration {
        self.total.saturating_sub(
            self.logical_planning
                + self.physical_planning
                + self.arrow_execution
                + self.public_result_materialization,
        )
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Phase {
    LogicalPlanning,
    PhysicalPlanning,
    ArrowExecution,
    PublicResultMaterialization,
}

pub(crate) fn is_active() -> bool {
    ACTIVE_PROFILE.try_with(|_| ()).is_ok()
}

pub(crate) fn record_phase(phase: Phase, elapsed: Duration) {
    let _ = ACTIVE_PROFILE.try_with(|profile| {
        let mut profile = profile.borrow_mut();
        let target = match phase {
            Phase::LogicalPlanning => &mut profile.logical_planning,
            Phase::PhysicalPlanning => &mut profile.physical_planning,
            Phase::ArrowExecution => &mut profile.arrow_execution,
            Phase::PublicResultMaterialization => &mut profile.public_result_materialization,
        };
        *target += elapsed;
    });
}

pub(crate) fn record_scan(rows: usize, batches: usize, arrow_bytes: usize, elapsed: Duration) {
    let _ = ACTIVE_PROFILE.try_with(|profile| {
        let mut profile = profile.borrow_mut();
        profile.scan_rows = profile.scan_rows.saturating_add(rows as u64);
        profile.scan_batches = profile.scan_batches.saturating_add(batches as u64);
        profile.scan_arrow_bytes = profile.scan_arrow_bytes.saturating_add(arrow_bytes as u64);
        profile.scan_elapsed += elapsed;
    });
}

pub(crate) async fn scope<F>(future: F) -> (Result<ExecuteResult, LixError>, SqlReadProfile)
where
    F: Future<Output = Result<ExecuteResult, LixError>>,
{
    let started = std::time::Instant::now();
    let (result, mut profile) = ACTIVE_PROFILE
        .scope(RefCell::new(SqlReadProfile::default()), async move {
            let result = future.await;
            let profile = ACTIVE_PROFILE.with(|profile| profile.borrow().clone());
            (result, profile)
        })
        .await;
    profile.total = started.elapsed();
    (result, profile)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Memory, Value, engine::Engine};

    #[tokio::test]
    async fn profile_scope_accumulates_disjoint_phases_and_scan_diagnostics() {
        let (result, profile) = scope(async {
            record_phase(Phase::LogicalPlanning, Duration::from_millis(2));
            record_phase(Phase::PublicResultMaterialization, Duration::from_millis(3));
            record_scan(11, 2, 4096, Duration::from_millis(4));
            Ok(ExecuteResult::from_rows(vec!["value".into()], vec![]))
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(profile.logical_planning, Duration::from_millis(2));
        assert_eq!(
            profile.public_result_materialization,
            Duration::from_millis(3)
        );
        assert_eq!(profile.scan_elapsed, Duration::from_millis(4));
        assert_eq!(profile.scan_rows, 11);
        assert_eq!(profile.scan_batches, 2);
        assert_eq!(profile.scan_arrow_bytes, 4096);
        assert!(profile.unattributed_overhead() <= profile.total);
    }

    #[test]
    fn recording_without_a_scope_is_a_noop() {
        assert!(!is_active());
        record_phase(Phase::ArrowExecution, Duration::from_secs(1));
        record_scan(1, 1, 1, Duration::from_secs(1));
    }

    #[tokio::test]
    async fn concurrent_scopes_are_independent_and_do_not_leak() {
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let first_barrier = std::sync::Arc::clone(&barrier);
        let first = tokio::spawn(async move {
            scope(async move {
                record_phase(Phase::LogicalPlanning, Duration::from_millis(1));
                first_barrier.wait().await;
                record_scan(3, 1, 30, Duration::from_millis(2));
                Ok(ExecuteResult::from_rows(vec![], vec![]))
            })
            .await
            .1
        });
        let second = tokio::spawn(async move {
            scope(async move {
                record_phase(Phase::PhysicalPlanning, Duration::from_millis(4));
                barrier.wait().await;
                record_scan(7, 2, 70, Duration::from_millis(5));
                Ok(ExecuteResult::from_rows(vec![], vec![]))
            })
            .await
            .1
        });

        let (first, second) = tokio::join!(first, second);
        let first = first.expect("first profile task should join");
        let second = second.expect("second profile task should join");
        assert_eq!(first.logical_planning, Duration::from_millis(1));
        assert_eq!(first.physical_planning, Duration::ZERO);
        assert_eq!((first.scan_rows, first.scan_batches), (3, 1));
        assert_eq!(second.logical_planning, Duration::ZERO);
        assert_eq!(second.physical_planning, Duration::from_millis(4));
        assert_eq!((second.scan_rows, second.scan_batches), (7, 2));
        assert!(
            !is_active(),
            "completed scopes must restore task-local state"
        );
    }

    #[tokio::test]
    async fn profiled_select_reports_exact_spec_scan_output() {
        let storage = Memory::default();
        Engine::initialize(storage.clone())
            .await
            .expect("profile test storage should initialize");
        let engine = Engine::new(storage)
            .await
            .expect("profile test engine should open");
        let session = engine
            .open_workspace_session()
            .await
            .expect("profile test session should open");
        let schema = serde_json::json!({
            "x-lix-key": "sql_profile_probe",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "amount": { "type": "integer" }
            },
            "required": ["id", "amount"],
            "additionalProperties": false
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .expect("profile test schema should register");
        session
            .execute(
                "INSERT INTO sql_profile_probe (id, amount) VALUES \
                 ('a', 10), ('b', 20), ('c', 30)",
                &[],
            )
            .await
            .expect("profile test rows should insert");

        let (result, profile) = session
            .execute_profiled(
                "SELECT id, amount FROM sql_profile_probe ORDER BY amount",
                &[],
            )
            .await
            .expect("profiled SELECT should execute");
        assert_eq!(result.len(), 3);
        assert_eq!(profile.scan_rows, 3);
        assert!(profile.scan_batches > 0);
        assert!(profile.scan_arrow_bytes > 0);
        assert!(profile.scan_elapsed > Duration::ZERO);
        let disjoint_phase_sum = profile.logical_planning
            + profile.physical_planning
            + profile.arrow_execution
            + profile.public_result_materialization;
        assert!(disjoint_phase_sum <= profile.total);
    }
}
