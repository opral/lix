//! Benchmark-only SQL phase and scan diagnostics.

#[cfg(test)]
mod point_join_scan_scaling;

use std::cell::RefCell;
use std::future::Future;
use std::time::Duration;

use crate::LixError;

tokio::task_local! {
    static ACTIVE_PROFILE: RefCell<SqlReadProfile>;
}

/// Disjoint wall-clock phases for one public columnar SQL read.
///
/// Scan elapsed time is operator poll time summed across scan partitions. It
/// can overlap physical execution and therefore is diagnostic rather than a
/// fifth wall-clock phase.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SqlReadProfile {
    pub(crate) total: Duration,
    pub(crate) logical_planning: Duration,
    pub(crate) physical_planning: Duration,
    pub(crate) arrow_execution: Duration,
    pub(crate) public_result_materialization: Duration,
    pub(crate) scan_elapsed: Duration,
    pub(crate) scan_rows: u64,
    pub(crate) scan_batches: u64,
    /// Sum of Arrow's in-memory array sizes at the scan output boundary.
    /// This is not the number of bytes read from the storage backend.
    pub(crate) scan_arrow_bytes: u64,
    /// Number of stored rows a provider had to look at to produce its scan
    /// output, counted *before* the provider applies its own row filters.
    ///
    /// `scan_rows` counts the scan *output* boundary and is therefore blind to
    /// work a provider does and then discards. A predicate that has no indexed
    /// access path reads the whole collection and returns two rows: `scan_rows`
    /// reports 2 while this counter reports the collection size. The pair is
    /// what makes an access-path claim falsifiable — a fix that only moves
    /// filtering earlier in the plan changes `scan_rows` and leaves this
    /// unchanged.
    ///
    /// Entity surfaces record it at every route they can take, so no scan is
    /// silently unaccounted: primary-key projection and direct-snapshot reads
    /// examine exactly what they emit, the generic row route examines the
    /// whole scanned batch before `EntityRowFilter`s run, and the columnar
    /// route examines the rows of the row groups that survived manifest
    /// pruning plus its overlay rows.
    pub(crate) provider_rows_examined: u64,
    /// Number of rows retained by the benchmark-only result ceiling probe.
    /// A nonzero value means result conversion was intentionally bypassed;
    /// it is never set by the production result path.
    pub(crate) result_count_only_rows: u64,
    pub(crate) result_count_only_batches: u64,
    /// Number of public rows consumed while the profile scope was active.
    pub(crate) result_rows_consumed: u64,
    /// Number of rows materialized into the public `Vec<Row>` representation
    /// or an owned benchmark scalar row while the profile scope was active.
    pub(crate) result_rows_materialized: u64,
    /// Number of owned rows retained through the end of result consumption.
    /// Cursor modes materialize one scalar row at a time but retain none.
    pub(crate) result_rows_retained: u64,
    /// Checksum of consumed scalar values. This is a benchmark-only
    /// correctness witness, not a public result API.
    pub(crate) result_checksum: u64,
}

impl SqlReadProfile {
    /// Time inside the public call outside the four instrumented phases, such
    /// as snapshot acquisition, provider registration, and statement routing.
    pub(crate) fn unattributed_overhead(&self) -> Duration {
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

/// Records stored rows a provider looked at while serving one scan batch.
///
/// Providers call this with the pre-filter row count of whatever they read.
/// Recording is a no-op outside a profile scope, so the production read path
/// pays one task-local probe per batch, never per row.
pub(crate) fn record_provider_rows_examined(rows: usize) {
    let _ = ACTIVE_PROFILE.try_with(|profile| {
        let mut profile = profile.borrow_mut();
        profile.provider_rows_examined = profile.provider_rows_examined.saturating_add(rows as u64);
    });
}

#[cfg(feature = "storage-benches")]
pub(crate) fn record_result_count_only(rows: usize, batches: usize) {
    let _ = ACTIVE_PROFILE.try_with(|profile| {
        let mut profile = profile.borrow_mut();
        profile.result_count_only_rows = profile.result_count_only_rows.saturating_add(rows as u64);
        profile.result_count_only_batches = profile
            .result_count_only_batches
            .saturating_add(batches as u64);
    });
}

#[cfg(feature = "storage-benches")]
pub(crate) fn record_result_rows(consumed: usize, materialized: usize, retained: usize) {
    let _ = ACTIVE_PROFILE.try_with(|profile| {
        let mut profile = profile.borrow_mut();
        profile.result_rows_consumed = profile.result_rows_consumed.saturating_add(consumed as u64);
        profile.result_rows_materialized = profile
            .result_rows_materialized
            .saturating_add(materialized as u64);
        profile.result_rows_retained = profile.result_rows_retained.saturating_add(retained as u64);
    });
}

#[cfg(feature = "storage-benches")]
pub(crate) fn record_result_checksum(checksum: u64) {
    let _ = ACTIVE_PROFILE.try_with(|profile| {
        let mut profile = profile.borrow_mut();
        profile.result_checksum = profile.result_checksum.wrapping_add(checksum);
    });
}

pub(crate) async fn scope<T, F>(future: F) -> (Result<T, LixError>, SqlReadProfile)
where
    F: Future<Output = Result<T, LixError>>,
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
    use crate::ExecuteResult;
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
            .open_session()
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

    /// The counter's reason for existing: a predicate with an indexed access
    /// path examines what it returns, and a predicate without one examines the
    /// whole collection while `scan_rows` reports the same small output.
    ///
    /// This is the instrument for the point-lookup access-path work. If a
    /// change ever makes the non-indexed case examine a bounded number of
    /// rows, this test is the thing that should be updated to assert the new
    /// bound — not deleted.
    #[tokio::test]
    async fn provider_rows_examined_separates_indexed_from_scanned_predicates() {
        let storage = Memory::default();
        Engine::initialize(storage.clone())
            .await
            .expect("rows-examined storage should initialize");
        let engine = Engine::new(storage)
            .await
            .expect("rows-examined engine should open");
        let session = engine
            .open_session()
            .await
            .expect("rows-examined session should open");
        let schema = serde_json::json!({
            "x-lix-key": "rows_examined_probe",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "parent": { "type": "string" }
            },
            "required": ["id", "parent"],
            "additionalProperties": false
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .expect("rows-examined schema should register");
        const ROWS: usize = 24;
        for index in 0..ROWS {
            session
                .execute(
                    "INSERT INTO rows_examined_probe (id, parent) VALUES ($1, $2)",
                    &[
                        Value::Text(format!("row-{index}")),
                        Value::Text(format!("parent-{index}")),
                    ],
                )
                .await
                .expect("rows-examined row should insert");
        }

        let (indexed, indexed_profile) = session
            .execute_profiled(
                "SELECT id, parent FROM rows_examined_probe WHERE id = $1",
                &[Value::Text("row-7".into())],
            )
            .await
            .expect("primary-key lookup should execute");
        assert_eq!(indexed.len(), 1);
        assert_eq!(indexed_profile.scan_rows, 1);
        assert_eq!(
            indexed_profile.provider_rows_examined, 1,
            "a primary-key predicate has an indexed access path and must not \
             examine the rest of the collection"
        );

        let (scanned, scanned_profile) = session
            .execute_profiled(
                "SELECT id, parent FROM rows_examined_probe WHERE parent = $1",
                &[Value::Text("parent-7".into())],
            )
            .await
            .expect("non-primary-key lookup should execute");
        assert_eq!(scanned.len(), 1);
        assert_eq!(
            scanned_profile.scan_rows, 1,
            "the provider filters before its output boundary, so scan_rows \
             cannot see the scan this predicate pays for"
        );
        assert_eq!(
            scanned_profile.provider_rows_examined, ROWS as u64,
            "a non-primary-key predicate has no indexed access path today and \
             examines the whole collection"
        );
    }

    #[tokio::test]
    async fn live_profile_early_drop_stops_before_later_datafusion_partition() {
        let storage = Memory::default();
        Engine::initialize(storage.clone())
            .await
            .expect("profile cancellation storage should initialize");
        let engine = Engine::new(storage)
            .await
            .expect("profile cancellation engine should open");
        let session = engine
            .open_session()
            .await
            .expect("profile cancellation session should open");

        for table in ["sql_profile_cancel_a", "sql_profile_cancel_b"] {
            let schema = serde_json::json!({
                "x-lix-key": table,
                "x-lix-primary-key": ["/id"],
                "type": "object",
                "properties": {
                    "id": { "type": "string" },
                    "ordinal": { "type": "integer" },
                    "payload": { "type": "string" }
                },
                "required": ["id", "ordinal", "payload"],
                "additionalProperties": false
            });
            session
                .execute(
                    "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
                    &[Value::Text(schema.to_string())],
                )
                .await
                .expect("profile cancellation schema should register");
        }
        session
            .execute(
                "INSERT INTO sql_profile_cancel_a (id, ordinal, payload) VALUES \
                 ('row-00000000', 0, 'payload-00000000'), \
                 ('row-00000001', 1, 'payload-00000001'), \
                 ('row-00000002', 2, 'payload-00000002'), \
                 ('row-00000003', 3, 'payload-00000003')",
                &[],
            )
            .await
            .expect("first cancellation partition should seed");
        session
            .execute(
                "INSERT INTO sql_profile_cancel_b (id, ordinal, payload) VALUES \
                 ('row-00000004', 4, 'payload-00000004'), \
                 ('row-00000005', 5, 'payload-00000005'), \
                 ('row-00000006', 6, 'payload-00000006'), \
                 ('row-00000007', 7, 'payload-00000007')",
                &[],
            )
            .await
            .expect("second cancellation partition should seed");

        let profile = session
            .execute_result_streaming_profiled(
                "SELECT id, ordinal, payload FROM sql_profile_cancel_a \
                 UNION ALL SELECT id, ordinal, payload FROM sql_profile_cancel_b",
                &[],
                "live",
                Some(1),
            )
            .await
            .expect("live cancellation profile should execute");

        assert_eq!(profile.result_rows_consumed, 1);
        assert_eq!(profile.result_rows_materialized, 1);
        assert_eq!(profile.result_rows_retained, 0);
        assert_eq!(profile.scan_rows, 4);
        assert!(profile.scan_batches > 0);
    }
}
