#![recursion_limit = "256"]

#[allow(dead_code)]
mod benchmark_metrics;

use std::convert::Infallible;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::io::Cursor;
use http::header::CONTENT_TYPE;
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt as _, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use lix::server_protocol::{
    LixServerProtocol, ServerProtocolBody, ServerProtocolContext, ServerProtocolPrincipal,
};
use lix::storage::Storage;
use lix::{
    CreateBranchOptions, ExecuteBatchStatement, Lix, LixError, Memory, MergeBranchOptions,
    ServerOptions, SwitchBranchOptions, Value, WireValue, open_lix,
};
use lix_storage_filesystem::FilesystemStorage;
use serde_json::{Value as JsonValue, json};
use tempfile::TempDir;
use tokio::net::TcpListener;

use benchmark_metrics::AllocationScope;

const WAIT_TIMEOUT: Duration = Duration::from_secs(15);
const OFFLINE_COMMIT_COUNT: usize = 513;
const HOT_STATE_PROFILE_RECORD_PREFIX: &str = "LIX_HOT_STATE_PROFILE_JSON=";

#[derive(Debug, Default)]
struct HttpProbe {
    handshakes: AtomicU64,
    pushes: AtomicU64,
    push_conflicts: AtomicU64,
    delta_pulls: AtomicU64,
    publication_fences: AtomicU64,
    snapshot_row_pulls: AtomicU64,
    history_gets: AtomicU64,
    active_history_gets: AtomicU64,
    max_concurrent_history_gets: AtomicU64,
    blob_gets: AtomicU64,
    chunk_gets: AtomicU64,
    chunk_puts: AtomicU64,
    drop_next_push_ack: AtomicBool,
    reject_requests: AtomicBool,
    reject_pushes: AtomicBool,
    mismatch_handshake_protocol: AtomicBool,
    one_way_delay_millis: AtomicU64,
    gated_pushes: AtomicU64,
    push_gate: Mutex<Option<Arc<tokio::sync::Barrier>>>,
}

#[derive(Debug)]
struct HotStateProfileCase {
    label: &'static str,
    live_rows: usize,
    dirty_rows: usize,
    history_commits: usize,
    authority_commits: i64,
    bootstrap_elapsed: Duration,
    bootstrap_allocations: benchmark_metrics::AllocationMetrics,
    working_diff_elapsed: Duration,
    working_diff_allocations: benchmark_metrics::AllocationMetrics,
    selected_content_bytes: usize,
    selected_content_elapsed: Duration,
    selected_content_allocations: benchmark_metrics::AllocationMetrics,
    snapshot_row_pulls: u64,
    bootstrap_history_gets: u64,
    working_diff_history_gets: u64,
}

impl HotStateProfileCase {
    fn json(&self) -> JsonValue {
        json!({
            "schema": "lix.certified-hot-state-profile.v1",
            "case": self.label,
            "dimensions": {
                "live_rows": self.live_rows,
                "dirty_rows": self.dirty_rows,
                "history_commits_requested": self.history_commits,
                "authority_commits_observed": self.authority_commits,
                "branches": 2,
            },
            "bootstrap": {
                "elapsed_ns": duration_nanos(self.bootstrap_elapsed),
                "allocation_count": self.bootstrap_allocations.allocation_count,
                "allocated_bytes": self.bootstrap_allocations.allocated_bytes,
                "live_bytes_delta": self.bootstrap_allocations.live_bytes_delta,
                "peak_live_bytes_delta": self.bootstrap_allocations.peak_live_bytes_delta,
                "process_rss_start_bytes": self.bootstrap_allocations.process_rss_start_bytes,
                "process_rss_end_bytes": self.bootstrap_allocations.process_rss_end_bytes,
                "snapshot_row_pulls": self.snapshot_row_pulls,
                "history_gets": self.bootstrap_history_gets,
            },
            "working_diff": {
                "elapsed_ns": duration_nanos(self.working_diff_elapsed),
                "allocation_count": self.working_diff_allocations.allocation_count,
                "allocated_bytes": self.working_diff_allocations.allocated_bytes,
                "live_bytes_delta": self.working_diff_allocations.live_bytes_delta,
                "peak_live_bytes_delta": self.working_diff_allocations.peak_live_bytes_delta,
                "process_rss_start_bytes": self.working_diff_allocations.process_rss_start_bytes,
                "process_rss_end_bytes": self.working_diff_allocations.process_rss_end_bytes,
                "history_gets": self.working_diff_history_gets,
            },
            "selected_working_file": {
                "payload_bytes": self.selected_content_bytes,
                "elapsed_ns": duration_nanos(self.selected_content_elapsed),
                "allocation_count": self.selected_content_allocations.allocation_count,
                "allocated_bytes": self.selected_content_allocations.allocated_bytes,
                "live_bytes_delta": self.selected_content_allocations.live_bytes_delta,
                "peak_live_bytes_delta": self.selected_content_allocations.peak_live_bytes_delta,
                "process_rss_start_bytes": self.selected_content_allocations.process_rss_start_bytes,
                "process_rss_end_bytes": self.selected_content_allocations.process_rss_end_bytes,
            },
        })
    }
}

/// Deterministic architecture scorecard for the certified HOT serving plane.
///
/// This is ignored because it deliberately constructs three non-trivial remote
/// repositories and owns a process-global allocation window. Run only this
/// exact test; optional JSON output is controlled by
/// `LIX_HOT_STATE_PROFILE_OUTPUT`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual certified HOT bootstrap/allocation scorecard"]
async fn certified_hot_state_profile_scorecard() {
    let shallow = profile_certified_hot_case("history-shallow", 256, 32, 2).await;
    let deep = profile_certified_hot_case("history-deep", 256, 32, 64).await;
    let wide = profile_certified_hot_case("rows-wide", 768, 96, 2).await;
    let tombstone_checkpoint = profile_net_zero_tombstone_checkpoint(128).await;

    assert_eq!(
        shallow.snapshot_row_pulls, deep.snapshot_row_pulls,
        "bootstrap page count must be independent of cold history depth"
    );
    assert_eq!(
        shallow.bootstrap_history_gets, deep.bootstrap_history_gets,
        "bootstrap topology requests must be independent of cold history depth"
    );
    assert_eq!(shallow.working_diff_history_gets, 0);
    assert_eq!(deep.working_diff_history_gets, 0);
    assert_eq!(wide.working_diff_history_gets, 0);

    // Allocation ratios intentionally have generous envelopes: the exact
    // network/count assertions above are the blocking complexity proof, while
    // the allocator includes the in-process HTTP authority and background
    // protocol tasks. These bounds catch super-linear explosions without
    // pretending to be a machine-independent latency benchmark.
    assert_bounded_growth(
        "bootstrap history-depth allocated bytes",
        shallow.bootstrap_allocations.allocated_bytes,
        deep.bootstrap_allocations.allocated_bytes,
        2,
        1024 * 1024,
    );
    assert_bounded_growth(
        "bootstrap history-depth peak live bytes",
        shallow.bootstrap_allocations.peak_live_bytes_delta,
        deep.bootstrap_allocations.peak_live_bytes_delta,
        2,
        1024 * 1024,
    );
    assert_bounded_growth(
        "bootstrap row scaling allocated bytes",
        shallow.bootstrap_allocations.allocated_bytes,
        wide.bootstrap_allocations.allocated_bytes,
        6,
        2 * 1024 * 1024,
    );
    assert_bounded_growth(
        "working-diff dirty-row scaling peak live bytes",
        shallow.working_diff_allocations.peak_live_bytes_delta,
        wide.working_diff_allocations.peak_live_bytes_delta,
        6,
        1024 * 1024,
    );

    let cases = [&shallow, &deep, &wide];
    let case_records = cases.iter().map(|case| case.json()).collect::<Vec<_>>();
    for record in &case_records {
        eprintln!(
            "{HOT_STATE_PROFILE_RECORD_PREFIX}{}",
            serde_json::to_string(record).expect("HOT profile record serializes")
        );
    }
    let artifact = json!({
        "schema": "lix.certified-hot-state-profile-artifact.v1",
        "contract": {
            "bootstrap": "O(M log M + (B + Q)M), independent of cold history depth H",
            "working_diff": "O(D log D), with no cold-history request",
            "selected_working_file": "O(S + A_f log A_f + P_f) exact HOT file-id read; O(A_f + P_f) transient memory",
            "memory": "O(transferred HOT payload P + distinct bootstrap rows M)",
            "measurement_boundary": "in-process sync client plus HTTP authority",
        },
        "assertions": {
            "history_independent_snapshot_pages": true,
            "history_independent_bootstrap_topology_requests": true,
            "working_diff_history_requests": 0,
            "allocator_growth_envelopes_passed": true,
        },
        "cases": case_records,
        "net_zero_tombstone_checkpoint": tombstone_checkpoint,
    });
    if let Some(output) = std::env::var_os("LIX_HOT_STATE_PROFILE_OUTPUT") {
        let output = Path::new(&output);
        if let Some(parent) = output.parent() {
            std::fs::create_dir_all(parent).expect("create HOT profile output directory");
        }
        std::fs::write(
            output,
            format!(
                "{}\n",
                serde_json::to_string_pretty(&artifact).expect("HOT profile artifact serializes")
            ),
        )
        .expect("write HOT profile artifact");
    }
}

async fn profile_net_zero_tombstone_checkpoint(churn_rows: usize) -> JsonValue {
    let (authority_storage, authority) = open_authority().await;
    authority
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .expect("checkpoint tombstone profile baseline");
    for index in 0..churn_rows {
        let path = format!("/net-zero-profile-{index:05}.txt");
        authority
            .execute_batch(&[
                ExecuteBatchStatement {
                    label: None,
                    sql: "INSERT INTO lix_file (path, content) VALUES ($1, $2)".to_owned(),
                    params: vec![
                        Value::Text(path.clone()),
                        Value::Blob(b"temporary".to_vec().into()),
                    ],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: "DELETE FROM lix_file WHERE path = $1".to_owned(),
                    params: vec![Value::Text(path)],
                },
            ])
            .await
            .expect("create net-zero retained tombstone");
    }
    authority.close().await.expect("close churn authority");

    let probe = Arc::new(HttpProbe::default());
    let (url, server_task) = serve(authority_storage.clone(), Arc::clone(&probe)).await;
    let pre_dir = TempDir::new().expect("pre-checkpoint replica tempdir");
    let pre = open_replica(pre_dir.path(), &url).await;
    let pre_scope = AllocationScope::start();
    let pre_count = pre
        .execute("SELECT COUNT(*) AS count FROM lix_diff('lix_file')", &[])
        .await
        .expect("query net-zero working diff before checkpoint")
        .rows()[0]
        .get::<i64>("count")
        .expect("working diff count is integer");
    let pre_allocations = pre_scope.finish();
    assert_eq!(pre_count, 0);
    pre.close().await.expect("close pre-checkpoint replica");
    stop_server(server_task).await;

    let (_, checkpoint_server_task, checkpoint_authority) =
        serve_with_authority_session(authority_storage.clone(), Arc::default()).await;
    checkpoint_authority
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await;
    drop(checkpoint_authority);
    stop_server(checkpoint_server_task).await;

    let probe = Arc::new(HttpProbe::default());
    let (url, server_task) = serve(authority_storage, Arc::clone(&probe)).await;
    let post_dir = TempDir::new().expect("post-checkpoint replica tempdir");
    let post = open_replica(post_dir.path(), &url).await;
    let post_scope = AllocationScope::start();
    let post_count = post
        .execute("SELECT COUNT(*) AS count FROM lix_diff('lix_file')", &[])
        .await
        .expect("query working diff after tombstone checkpoint")
        .rows()[0]
        .get::<i64>("count")
        .expect("working diff count is integer");
    let post_allocations = post_scope.finish();
    assert_eq!(post_count, 0);
    assert_bounded_growth(
        "post-checkpoint net-zero tombstone peak live bytes",
        pre_allocations.peak_live_bytes_delta,
        post_allocations.peak_live_bytes_delta,
        2,
        1024 * 1024,
    );
    post.close().await.expect("close post-checkpoint replica");
    stop_server(server_task).await;

    json!({
        "churn_rows": churn_rows,
        "working_diff_rows_before_checkpoint": pre_count,
        "working_diff_rows_after_checkpoint": post_count,
        "before_checkpoint": {
            "allocated_bytes": pre_allocations.allocated_bytes,
            "peak_live_bytes_delta": pre_allocations.peak_live_bytes_delta,
        },
        "after_checkpoint": {
            "allocated_bytes": post_allocations.allocated_bytes,
            "peak_live_bytes_delta": post_allocations.peak_live_bytes_delta,
        },
    })
}

async fn profile_certified_hot_case(
    label: &'static str,
    live_rows: usize,
    dirty_rows: usize,
    history_commits: usize,
) -> HotStateProfileCase {
    assert!(dirty_rows <= live_rows);
    let (authority_storage, authority) = open_authority().await;
    seed_hot_profile_rows(&authority, live_rows).await;
    for history_index in 0..history_commits {
        put_value(
            &authority,
            "hot-profile-history-probe",
            &format!("history-{history_index:05}"),
        )
        .await;
    }
    authority
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .expect("checkpoint HOT profile baseline");
    let updates = (0..dirty_rows)
        .map(|index| ExecuteBatchStatement {
            label: None,
            sql: "UPDATE lix_file SET content = $1 WHERE path = $2".to_owned(),
            params: vec![
                Value::Blob(if index == 0 {
                    vec![b'x'; 1024 * 1024].into()
                } else {
                    format!("dirty-{index:05}").into_bytes().into()
                }),
                Value::Text(format!("/hot-profile-row-{index:05}.txt")),
            ],
        })
        .collect::<Vec<_>>();
    authority
        .execute_batch(&updates)
        .await
        .expect("dirty HOT profile rows");
    let authority_commits = commit_count(&authority).await;
    authority
        .close()
        .await
        .expect("close HOT profile authority setup");

    let probe = Arc::new(HttpProbe::default());
    let (url, server_task) = serve(authority_storage, Arc::clone(&probe)).await;
    let replica_dir = TempDir::new().expect("HOT profile replica tempdir");
    let bootstrap_scope = AllocationScope::start();
    let bootstrap_started = Instant::now();
    let replica = open_replica(replica_dir.path(), &url).await;
    let bootstrap_elapsed = bootstrap_started.elapsed();
    let bootstrap_allocations = bootstrap_scope.finish();

    let replicated_rows = replica
        .execute(
            "SELECT COUNT(*) AS count FROM lix_file WHERE path LIKE '/hot-profile-row-%'",
            &[],
        )
        .await
        .expect("count certified HOT rows")
        .rows()[0]
        .get::<i64>("count")
        .expect("HOT row count is integer");
    assert_eq!(replicated_rows, live_rows as i64);

    let history_before_diff = probe.history_gets.load(Ordering::Acquire);
    let diff_scope = AllocationScope::start();
    let diff_started = Instant::now();
    let diff = replica
        .execute("SELECT COUNT(*) AS count FROM lix_diff('lix_file')", &[])
        .await
        .expect("query certified HOT working diff");
    let working_diff_elapsed = diff_started.elapsed();
    let working_diff_allocations = diff_scope.finish();
    let diff_count = diff.rows()[0]
        .get::<i64>("count")
        .expect("working diff count is integer");
    assert_eq!(diff_count, dirty_rows as i64);
    let working_diff_history_gets = probe
        .history_gets
        .load(Ordering::Acquire)
        .saturating_sub(history_before_diff);

    let selected_file_id = replica
        .execute(
            "SELECT id FROM lix_file WHERE path = '/hot-profile-row-00000.txt'",
            &[],
        )
        .await
        .expect("load selected HOT profile file id")
        .rows()[0]
        .get::<String>("id")
        .expect("selected HOT profile file id is text");
    let selected_scope = AllocationScope::start();
    let selected_started = Instant::now();
    let selected = replica
        .execute(
            "SELECT content FROM lix_file WHERE id = $1",
            &[Value::Text(selected_file_id)],
        )
        .await
        .expect("load one selected certified working payload");
    let selected_content_elapsed = selected_started.elapsed();
    let selected_content_allocations = selected_scope.finish();
    assert_eq!(selected.rows().len(), 1);
    let selected_content_bytes = selected.rows()[0]
        .get::<Value>("content")
        .expect("selected payload exists");
    let selected_content_bytes = match selected_content_bytes {
        Value::Blob(bytes) => bytes.len(),
        other => panic!("selected payload must be bytes, got {other:?}"),
    };
    assert_eq!(selected_content_bytes, 1024 * 1024);
    assert_eq!(
        probe.history_gets.load(Ordering::Acquire),
        history_before_diff,
        "selected working payload must not request cold history",
    );

    let case = HotStateProfileCase {
        label,
        live_rows,
        dirty_rows,
        history_commits,
        authority_commits,
        bootstrap_elapsed,
        bootstrap_allocations,
        working_diff_elapsed,
        working_diff_allocations,
        selected_content_bytes,
        selected_content_elapsed,
        selected_content_allocations,
        snapshot_row_pulls: probe.snapshot_row_pulls.load(Ordering::Acquire),
        bootstrap_history_gets: history_before_diff,
        working_diff_history_gets,
    };
    replica.close().await.expect("close HOT profile replica");
    stop_server(server_task).await;
    case
}

async fn seed_hot_profile_rows(authority: &Lix<Memory>, live_rows: usize) {
    const BATCH_ROWS: usize = 256;
    for batch_start in (0..live_rows).step_by(BATCH_ROWS) {
        let batch_end = (batch_start + BATCH_ROWS).min(live_rows);
        let statements = (batch_start..batch_end)
            .map(|index| ExecuteBatchStatement {
                label: None,
                sql: "INSERT INTO lix_file (path, content) VALUES ($1, $2)".to_owned(),
                params: vec![
                    Value::Text(format!("/hot-profile-row-{index:05}.txt")),
                    Value::Blob(format!("baseline-{index:05}").into_bytes().into()),
                ],
            })
            .collect::<Vec<_>>();
        authority
            .execute_batch(&statements)
            .await
            .expect("seed HOT profile row batch");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connected_api_routes_authority_work_and_hot_reads_need_no_round_trip() {
    let (authority_storage, authority) = open_authority().await;
    put_value(&authority, "authority-fence", "before").await;
    authority
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .expect("checkpoint authoritative baseline");
    authority.close().await.expect("close authority setup");

    let probe = Arc::new(HttpProbe::default());
    let principal = ServerProtocolPrincipal::Authenticated {
        account_id: lix::SYSTEM_ACCOUNT_ID.to_owned(),
        idempotency_scope: "connected-api-e2e".to_owned(),
    };
    let (url, server_task, protocol_authority) =
        serve_as_with_authority_session(authority_storage, Arc::clone(&probe), principal).await;
    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_replica(replica_dir.path(), &url).await;

    protocol_authority
        .put_value("authority-fence", "after")
        .await;
    wait_for_value(&replica, "authority-fence", "after").await;
    let fences_before_read = probe.publication_fences.load(Ordering::Acquire);
    for _ in 0..100 {
        assert_eq!(
            read_value(&replica, "authority-fence").await.as_deref(),
            Some("after"),
        );
    }
    assert_eq!(
        probe.publication_fences.load(Ordering::Acquire),
        fences_before_read,
        "certified HOT reads must not issue finite publication pulls",
    );

    let history_gets = probe.history_gets.load(Ordering::Acquire);
    let diff_count = replica
        .execute(
            "SELECT COUNT(*) AS count FROM lix_diff('lix_key_value') WHERE key = 'authority-fence'",
            &[],
        )
        .await
        .expect("one-argument lix_diff uses certified HOT state")
        .rows()[0]
        .get::<i64>("count")
        .expect("working diff count is integer");
    assert_eq!(diff_count, 1);
    assert_eq!(
        probe.history_gets.load(Ordering::Acquire),
        history_gets,
        "working diff must not request cold history",
    );

    replica
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('replica-write', 'authoritative')",
            &[],
        )
        .await
        .expect("connected mutation executes on the authority");
    assert_eq!(
        read_value(&replica, "replica-write").await.as_deref(),
        Some("authoritative"),
        "a successful authority mutation returns only after certified publication",
    );
    protocol_authority
        .wait_for_value("replica-write", "authoritative")
        .await;

    replica
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('branch-race', 'main')",
            &[],
        )
        .await
        .expect("seed main-branch race marker");
    let main_branch_id = replica
        .active_branch_id()
        .await
        .expect("load connected main branch");
    let race_branch = replica
        .create_branch(CreateBranchOptions {
            id: None,
            name: "connected-switch-read-race".to_owned(),
            from_commit_id: None,
        })
        .await
        .expect("create connected race branch");
    replica
        .switch_branch(SwitchBranchOptions {
            branch_id: race_branch.id.clone(),
        })
        .await
        .expect("switch to race branch");
    replica
        .execute(
            "UPDATE lix_key_value SET value = 'child' WHERE key = 'branch-race'",
            &[],
        )
        .await
        .expect("seed child-branch race marker");
    replica
        .switch_branch(SwitchBranchOptions {
            branch_id: main_branch_id.clone(),
        })
        .await
        .expect("return to connected main branch");

    tokio::time::timeout(WAIT_TIMEOUT, async {
        tokio::try_join!(
            async {
                for _ in 0..8 {
                    replica
                        .switch_branch(SwitchBranchOptions {
                            branch_id: race_branch.id.clone(),
                        })
                        .await?;
                    tokio::task::yield_now().await;
                    replica
                        .switch_branch(SwitchBranchOptions {
                            branch_id: main_branch_id.clone(),
                        })
                        .await?;
                }
                Ok::<(), LixError>(())
            },
            async {
                for _ in 0..64 {
                    let result = replica
                        .execute(
                            "SELECT lix_active_branch_id() AS branch_id, value \
                             FROM lix_key_value WHERE key = 'branch-race'",
                            &[],
                        )
                        .await?;
                    let branch_id = result.rows()[0].get::<String>("branch_id")?;
                    let value = match result.rows()[0].get::<Value>("value")? {
                        Value::Jsonb(value) => value.as_json_string().ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                "branch race marker must be a JSON string",
                            )
                        })?,
                        Value::Text(value) => value,
                        value => {
                            return Err(LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                format!("branch race marker has unexpected value {value:?}"),
                            ));
                        }
                    };
                    let expected = if branch_id == main_branch_id {
                        "main"
                    } else if branch_id == race_branch.id {
                        "child"
                    } else {
                        panic!("read exposed unknown connected branch '{branch_id}'")
                    };
                    assert_eq!(value, expected, "branch selector and HOT row must be atomic");
                    tokio::task::yield_now().await;
                }
                Ok::<(), LixError>(())
            },
        )
    })
    .await
    .expect("concurrent connected switch/read must not deadlock")
    .expect("concurrent connected switch/read should succeed");
    assert_eq!(
        replica.active_branch_id().await.expect("branch after race"),
        main_branch_id,
    );

    let mut transaction = replica
        .begin_transaction()
        .await
        .expect("connected transaction begins on the authority");
    transaction
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('transaction-write', 'committed')",
            &[],
        )
        .await
        .expect("connected transaction stages on the authority");
    let staged = transaction
        .execute(
            "SELECT value FROM lix_key_value WHERE key = 'transaction-write'",
            &[],
        )
        .await
        .expect("connected transaction preserves read-after-write");
    assert_eq!(staged.rows().len(), 1);
    transaction
        .commit()
        .await
        .expect("connected transaction commits and publishes");
    assert_eq!(
        read_value(&replica, "transaction-write").await.as_deref(),
        Some("committed"),
    );

    let guarded = replica
        .begin_transaction()
        .await
        .expect("connected transaction reserves the ordinary session state");
    for (label, result) in [
        (
            "read",
            replica.execute("SELECT 1 AS value", &[]).await.map(|_| ()),
        ),
        (
            "write",
            replica
                .execute(
                    "INSERT INTO lix_key_value (key, value) VALUES ('outside-transaction', 'forbidden')",
                    &[],
                )
                .await
                .map(|_| ()),
        ),
    ] {
        let error = result.expect_err("same-handle work must be blocked by connected transaction");
        assert_eq!(error.code, "LIX_INVALID_TRANSACTION_STATE", "{label}");
    }
    let observe_error = match replica.observe("SELECT 1 AS value", &[]) {
        Err(error) => error,
        Ok(mut observation) => {
            observation.close();
            panic!("same-handle observe must be blocked by connected transaction");
        }
    };
    assert_eq!(observe_error.code, "LIX_INVALID_TRANSACTION_STATE");
    let close_error = replica
        .close()
        .await
        .expect_err("close must reject an active connected transaction");
    assert_eq!(close_error.code, "LIX_INVALID_TRANSACTION_STATE");
    let switch_error = tokio::time::timeout(
        Duration::from_secs(5),
        replica.switch_branch(SwitchBranchOptions {
            branch_id: race_branch.id.clone(),
        }),
    )
    .await
    .expect("switch under connected transaction must not deadlock")
    .expect_err("switch must reject an active connected transaction");
    assert_eq!(switch_error.code, "LIX_INVALID_TRANSACTION_STATE");
    guarded
        .rollback()
        .await
        .expect("connected transaction rollback releases session state");
    replica
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('after-failed-close', 'usable')",
            &[],
        )
        .await
        .expect("failed close must leave the connected handle usable");
    assert_eq!(
        read_value(&replica, "after-failed-close").await.as_deref(),
        Some("usable"),
    );

    let mut abandoned = replica
        .begin_transaction()
        .await
        .expect("connected transaction uses an isolated authority session");
    abandoned
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('abandoned-write', 'never-committed')",
            &[],
        )
        .await
        .expect("stage abandoned authority write");
    drop(abandoned);
    replica
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('after-abandoned', 'available')",
            &[],
        )
        .await
        .expect("dropping a transaction must not wedge the shared authority session");
    assert_eq!(
        read_value(&replica, "after-abandoned").await.as_deref(),
        Some("available"),
    );
    assert_eq!(
        protocol_authority.read_value("abandoned-write").await,
        None,
        "closing the dedicated transaction session rolls back staged writes",
    );

    let history = replica
        .execute("SELECT * FROM lix_history('lix_key_value')", &[])
        .await
        .expect("connected history executes on the authority");
    assert!(!history.rows().is_empty());
    let coherent_history_statements: [(&str, &[Value]); 1] =
        [("SELECT * FROM lix_history('lix_key_value')", &[])];
    let coherent_history = replica
        .execute_coherent_read_batch(&coherent_history_statements)
        .await
        .expect("connected coherent history retains one authority snapshot");
    assert!(!coherent_history.results[0].rows().is_empty());
    assert_eq!(
        coherent_history.active_branch_id,
        replica.active_branch_id().await.expect("active branch"),
    );
    assert_eq!(
        coherent_history.storage_mutation_revision, None,
        "authority snapshots must not claim a local adapter revision",
    );
    let mut snapshot = Vec::new();
    replica
        .export_snapshot()
        .write_to(&mut snapshot)
        .await
        .expect("connected snapshot export streams from the authority");
    assert!(snapshot.starts_with(b"LIXSNAP\0"));

    replica.close().await.expect("close replica");

    let reopened_without_server = open_lix()
        .with_storage(
            FilesystemStorage::new(replica_dir.path())
                .open()
                .expect("reopen persisted replica storage"),
        )
        .await
        .expect("open persisted replica without a server");
    let persisted_export_error = reopened_without_server
        .export_snapshot()
        .write_to(&mut Vec::new())
        .await
        .expect_err("persisted sparse cache must not export through a standalone handle");
    assert_eq!(persisted_export_error.code, LixError::CODE_INVALID_PARAM);
    reopened_without_server
        .close()
        .await
        .expect("close standalone replica handle");
    stop_server(server_task).await;
}

fn assert_bounded_growth(label: &str, baseline: u64, candidate: u64, multiple: u64, slack: u64) {
    let maximum = baseline.saturating_mul(multiple).saturating_add(slack);
    assert!(
        candidate <= maximum,
        "{label}: {candidate} exceeds {multiple} * {baseline} + {slack} = {maximum}"
    );
}

fn duration_nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "legacy replica-local mutation/history semantics removed by the authority hard cut"]
async fn synced_partial_file_checkpoint_stays_off_cold_history() {
    let (authority_storage, authority) = open_authority().await;
    for index in 0..50 {
        put_value(
            &authority,
            &format!("cold-owner-{index:02}"),
            &format!("baseline-{index:02}"),
        )
        .await;
    }
    authority
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .expect("checkpoint cold snapshot baseline");
    authority.close().await.expect("close authority setup");

    let probe = Arc::new(HttpProbe::default());
    let (url, server_task) = serve(authority_storage.clone(), Arc::clone(&probe)).await;
    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_replica(replica_dir.path(), &url).await;
    probe.set_push_offline(true);
    let history_after_bootstrap = probe.history_gets.load(Ordering::Acquire);

    for (path, content) in [
        ("/selected.md", b"selected content".as_slice()),
        ("/remaining.md", b"remaining content".as_slice()),
    ] {
        replica
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
                &[
                    Value::Text(path.to_owned()),
                    Value::Blob(content.to_vec().into()),
                ],
            )
            .await
            .expect("create local file");
    }

    let selected_file_id = replica
        .execute("SELECT id FROM lix_file WHERE path = '/selected.md'", &[])
        .await
        .expect("load selected file id")
        .rows()[0]
        .get::<String>("id")
        .expect("selected file id decodes");
    assert_eq!(
        probe.history_gets.load(Ordering::Acquire),
        history_after_bootstrap,
        "local file setup must not hydrate additional snapshot history",
    );

    let history_before_checkpoint = probe.history_gets.load(Ordering::Acquire);
    let checkpoint = replica
        .execute(
            "SELECT commit_id FROM lix_create_checkpoint(ARRAY( \
             SELECT row_ref \
             FROM lix_diff('lix_file', lix_root_commit_id(), lix_active_branch_commit_id()) \
             WHERE id = $1))",
            &[Value::Text(selected_file_id.clone())],
        )
        .await
        .expect("partial file checkpoint stays HOT");
    assert!(
        checkpoint.rows_affected() > 0,
        "checkpoint must select the file diff",
    );
    assert_eq!(
        probe.history_gets.load(Ordering::Acquire),
        history_before_checkpoint,
        "checkpoint must not reconstruct the snapshot's cold commit owners",
    );
    assert_eq!(
        replica
            .execute(
                "SELECT COUNT(*) AS count \
                 FROM lix_diff('lix_file', $2, lix_active_branch_commit_id()) \
                 WHERE id = $1",
                &[
                    Value::Text(selected_file_id.clone()),
                    Value::Text(checkpoint.rows()[0].get::<String>("commit_id").unwrap()),
                ],
            )
            .await
            .expect("first reactive working-diff read stays HOT")
            .rows()[0]
            .get::<i64>("count")
            .unwrap(),
        0,
    );
    assert!(
        replica
            .execute(
                "SELECT COUNT(*) AS count \
                 FROM lix_diff('lix_file', $1, lix_active_branch_commit_id()) \
                 WHERE to_path = '/remaining.md'",
                &[Value::Text(
                    checkpoint.rows()[0].get::<String>("commit_id").unwrap()
                )],
            )
            .await
            .expect("unselected file remains dirty")
            .rows()[0]
            .get::<i64>("count")
            .unwrap()
            > 0,
    );
    assert_eq!(
        read_file_content(&replica, "/selected.md").await.as_deref(),
        Some(b"selected content".as_slice()),
    );
    assert_eq!(
        probe.history_gets.load(Ordering::Acquire),
        history_before_checkpoint,
        "reactive refresh must not defer the same cold reconstruction",
    );
    // Clear the deliberately injected push failure before close drains the
    // connected outbox.
    probe.set_push_offline(false);
    replica.close().await.expect("close replica");
    stop_server(server_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "legacy replica-local mutation/history semantics removed by the authority hard cut"]
async fn first_local_write_pushes_before_deferred_history_is_read() {
    let (authority_storage, authority) = open_authority().await;
    authority
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/shared.md', CAST('Hello world' AS BYTEA))",
            &[],
        )
        .await
        .expect("seed shared file");
    put_value(&authority, "head", "visible").await;
    authority.close().await.expect("close authority setup");
    let probe = Arc::new(HttpProbe::default());
    let (url, server_task, protocol_authority) =
        serve_with_authority_session(authority_storage.clone(), Arc::clone(&probe)).await;
    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_replica(replica_dir.path(), &url).await;

    replica
        .execute(
            "SELECT content FROM lix_file WHERE path = '/shared.md'",
            &[],
        )
        .await
        .expect("hydrate only the visible file");
    replica
        .execute(
            "UPDATE lix_file SET content = CAST('Hello worlds' AS BYTEA) WHERE path = '/shared.md'",
            &[],
        )
        .await
        .expect("edit file before reading history");
    protocol_authority
        .wait_for_file_content("/shared.md", b"Hello worlds")
        .await;

    replica.close().await.expect("close replica");
    stop_server(server_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "legacy replica-local mutation/history semantics removed by the authority hard cut"]
async fn sync_runtime_outlives_the_primary_session() {
    let (authority_storage, authority) = open_authority().await;
    put_value(&authority, "seed", "authority").await;
    authority.close().await.expect("close authority setup");
    let (url, server_task, protocol_authority) =
        serve_with_authority_session(authority_storage.clone(), Arc::default()).await;
    let replica_dir = TempDir::new().expect("replica tempdir");
    let primary = open_replica(replica_dir.path(), &url).await;
    let child = primary
        .open_another_session()
        .await
        .expect("open child session");

    primary.close().await.expect("close primary session");
    put_value(&child, "from-child", "after-primary-close").await;
    protocol_authority
        .wait_for_value("from-child", "after-primary-close")
        .await;

    protocol_authority
        .put_value("from-authority", "child-still-live")
        .await;
    wait_for_value(&child, "from-authority", "child-still-live").await;

    child.close().await.expect("close final session");
    stop_server(server_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn warm_runtime_protocol_mismatch_is_terminal_without_reconnect() {
    let (authority_storage, authority) = open_authority().await;
    put_value(&authority, "protocol-seed", "authority").await;
    authority.close().await.expect("close authority setup");
    let probe = Arc::new(HttpProbe::default());
    let (url, server_task) = serve(authority_storage, Arc::clone(&probe)).await;
    let replica_dir = TempDir::new().expect("replica tempdir");

    let initial = open_replica(replica_dir.path(), &url).await;
    initial.close().await.expect("close initial replica");
    let initial_handshakes = probe.handshakes.load(Ordering::Acquire);
    probe
        .mismatch_handshake_protocol
        .store(true, Ordering::Release);

    let reopened = open_replica(replica_dir.path(), &url).await;
    wait_for_counter(&probe.handshakes, initial_handshakes + 1).await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(
        probe.handshakes.load(Ordering::Acquire),
        initial_handshakes + 1,
        "a terminal protocol mismatch must not enter the reconnect loop"
    );
    let error = reopened
        .close()
        .await
        .expect_err("close should surface the worker's terminal mismatch");
    assert_eq!(error.code, "LIX_SYNC_PROTOCOL_MISMATCH");
    stop_server(server_task).await;
}

impl HttpProbe {
    fn gate_next_two_pushes(&self) {
        *self.push_gate.lock().expect("push gate lock") =
            Some(Arc::new(tokio::sync::Barrier::new(2)));
        self.gated_pushes.store(2, Ordering::Release);
    }

    fn push_gate_slot(&self) -> Option<Arc<tokio::sync::Barrier>> {
        self.gated_pushes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .ok()?;
        self.push_gate.lock().expect("push gate lock").clone()
    }

    fn drop_next_push_ack(&self) {
        self.drop_next_push_ack.store(true, Ordering::Release);
    }

    fn set_round_trip_delay(&self, round_trip: Duration) {
        self.one_way_delay_millis.store(
            u64::try_from(round_trip.as_millis() / 2).expect("test delay fits u64"),
            Ordering::Release,
        );
    }

    fn set_offline(&self, offline: bool) {
        self.reject_requests.store(offline, Ordering::Release);
    }

    fn set_push_offline(&self, offline: bool) {
        self.reject_pushes.store(offline, Ordering::Release);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "legacy replica-local mutation/history semantics removed by the authority hard cut"]
async fn fresh_bootstrap_reads_authority_then_local_write_reaches_server() {
    let (authority_storage, authority) = open_authority().await;
    let authority_lix_id = authority.lix_id().to_owned();
    put_value(&authority, "bootstrap-parent", "lazy-history").await;
    let history_parent = active_head(&authority).await;
    put_value(&authority, "bootstrap", "from-authority").await;
    let history_head = active_head(&authority).await;
    authority.close().await.expect("close authority setup");
    let probe = Arc::new(HttpProbe::default());
    let (url, server_task, protocol_authority) =
        serve_with_authority_session(authority_storage.clone(), Arc::clone(&probe)).await;
    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_replica(replica_dir.path(), &url).await;

    assert_eq!(
        replica.lix_id(),
        authority_lix_id,
        "the first local session must bind to the authority repository identity",
    );

    assert_eq!(
        read_value(&replica, "bootstrap").await.as_deref(),
        Some("from-authority")
    );
    let history_gets_before_demand = probe.history_gets.load(Ordering::Acquire);
    replica
        .execute(
            "SELECT COUNT(*) AS entries FROM lix_diff('lix_key_value', $1, $2)",
            &[Value::Text(history_parent), Value::Text(history_head)],
        )
        .await
        .expect("deferred history hydrates through the real HTTP transport and retries");
    assert_eq!(
        probe.history_gets.load(Ordering::Acquire),
        history_gets_before_demand + 1,
        "one lazy commit body should require one bounded history request",
    );
    replica
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('local', 'from-replica')",
            &[],
        )
        .await
        .expect("local write should not wait for the network");
    assert_eq!(
        read_value(&replica, "local").await.as_deref(),
        Some("from-replica")
    );
    replica
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/after-bootstrap.txt', CAST('works' AS BYTEA))",
            &[],
        )
        .await
        .expect("the first file creation after bootstrap has a checkpoint cursor");
    replica
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .expect("the first checkpoint after sync bootstrap succeeds");
    assert_eq!(
        replica
            .execute(
                "SELECT content FROM lix_file WHERE path = '/after-bootstrap.txt'",
                &[],
            )
            .await
            .expect("new local file should read")
            .rows()[0]
            .get::<Vec<u8>>("content")
            .expect("new file content should decode"),
        b"works",
    );
    protocol_authority
        .wait_for_value("local", "from-replica")
        .await;
    protocol_authority
        .wait_for_file_content("/after-bootstrap.txt", b"works")
        .await;

    replica.close().await.expect("close replica");
    drop(replica);
    let reopened = open_replica(replica_dir.path(), &url).await;
    assert_eq!(
        reopened.lix_id(),
        authority_lix_id,
        "a warm reopen must retain the authority repository identity",
    );
    reopened.close().await.expect("close reopened replica");
    stop_server(server_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "legacy replica-local mutation/history semantics removed by the authority hard cut"]
async fn fresh_replica_lists_checkpoints_then_hydrates_file_history_in_bounded_pages() {
    let (authority_storage, authority) = open_authority().await;
    authority
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/gone.md', CAST('version one' AS BYTEA))",
            &[],
        )
        .await
        .expect("create historical file");
    let file_id = authority
        .execute("SELECT id FROM lix_file WHERE path = '/gone.md'", &[])
        .await
        .expect("read historical file id")
        .rows()[0]
        .get::<String>("id")
        .expect("file id decodes");
    authority
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .expect("checkpoint first file version");
    authority
        .execute(
            "UPDATE lix_file SET content = CAST('version two' AS BYTEA) WHERE id = $1",
            &[Value::Text(file_id.clone())],
        )
        .await
        .expect("update historical file");
    authority
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .expect("checkpoint second file version");
    authority
        .execute(
            "DELETE FROM lix_file WHERE id = $1",
            &[Value::Text(file_id.clone())],
        )
        .await
        .expect("delete historical file");
    authority
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/kept.md', CAST('kept' AS BYTEA))",
            &[],
        )
        .await
        .expect("create surviving file");
    authority
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .expect("checkpoint deletion and surviving file");
    for index in 0..105 {
        put_value(&authority, &format!("history-page-{index:03}"), "value").await;
    }
    let head = active_head(&authority).await;
    authority.close().await.expect("close authority setup");

    let probe = Arc::new(HttpProbe::default());
    let (url, server_task) = serve(authority_storage.clone(), Arc::clone(&probe)).await;
    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_replica(replica_dir.path(), &url).await;

    let history_before_timeline = probe.history_gets.load(Ordering::Acquire);
    let checkpoints = replica
        .execute("SELECT commit_id FROM lix_checkpoint", &[])
        .await
        .expect("hot checkpoint timeline renders without cold history");
    assert!(checkpoints.len() >= 3);
    assert_eq!(
        probe.history_gets.load(Ordering::Acquire),
        history_before_timeline,
        "listing checkpoints must not fetch file history",
    );

    let history = replica
        .execute(
            "SELECT content FROM lix_history('lix_file', $1) WHERE id = $2 ORDER BY lixcol_depth",
            &[Value::Text(head.clone()), Value::Text(file_id.clone())],
        )
        .await
        .expect("cold file history hydrates through bounded pages");
    let versions = history
        .rows()
        .iter()
        .filter_map(|row| row.get::<Vec<u8>>("content").ok())
        .collect::<Vec<_>>();
    assert!(versions.iter().any(|bytes| bytes == b"version one"));
    assert!(versions.iter().any(|bytes| bytes == b"version two"));
    let page_requests = probe.history_gets.load(Ordering::Acquire) - history_before_timeline;
    assert!(
        (2..=5).contains(&page_requests),
        "more than 100 cold commits plus checkpoint topology should hydrate in a few bounded pages, got {page_requests}",
    );

    replica
        .execute(
            "SELECT content FROM lix_history('lix_file', $1) WHERE id = $2 ORDER BY lixcol_depth",
            &[Value::Text(head), Value::Text(file_id)],
        )
        .await
        .expect("repeat history read is local");
    assert_eq!(
        probe.history_gets.load(Ordering::Acquire) - history_before_timeline,
        page_requests,
        "a repeated history query must issue zero network requests",
    );

    replica.close().await.expect("close replica");
    stop_server(server_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "legacy replica-local mutation/history semantics removed by the authority hard cut"]
async fn exact_checkpoint_file_history_hydrates_only_its_anchor_boundary() {
    let (authority_storage, authority) = open_authority().await;
    let inserted = authority
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/bounded.md', CAST('version-00' AS BYTEA)) RETURNING id",
            &[],
        )
        .await
        .expect("create checkpointed file");
    let file_id = inserted.rows()[0]
        .get::<String>("id")
        .expect("file id decodes");
    let mut target_checkpoint = None;
    for index in 0..64 {
        authority
            .execute(
                "UPDATE lix_file SET content = CAST($1 AS BYTEA) WHERE id = $2",
                &[
                    Value::Text(format!("version-{index:02}")),
                    Value::Text(file_id.clone()),
                ],
            )
            .await
            .expect("update checkpointed file");
        let checkpoint = authority
            .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
            .await
            .expect("create file checkpoint")
            .rows()[0]
            .get::<String>("commit_id")
            .expect("checkpoint commit id decodes");
        if index == 31 {
            target_checkpoint = Some(checkpoint);
        }
    }
    let target_checkpoint = target_checkpoint.expect("target checkpoint captured");
    let authority_history = authority
        .execute(
            "SELECT content FROM lix_history('lix_file', $1) WHERE id = $2 ORDER BY lixcol_depth ASC LIMIT 1",
            &[
                Value::Text(target_checkpoint.clone()),
                Value::Text(file_id.clone()),
            ],
        )
        .await
        .expect("authority retains exact checkpoint content");
    assert_eq!(
        authority_history.rows()[0]
            .get::<Vec<u8>>("content")
            .expect("authority history content decodes"),
        b"version-31",
    );
    authority.close().await.expect("close authority setup");

    let probe = Arc::new(HttpProbe::default());
    let (url, server_task) = serve(authority_storage.clone(), Arc::clone(&probe)).await;
    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_replica(replica_dir.path(), &url).await;
    let history_before = probe.history_gets.load(Ordering::Acquire);

    let history = replica
        .execute(
            "SELECT id, path, content FROM lix_history('lix_file', $1) WHERE id = $2 ORDER BY lixcol_depth ASC LIMIT 1",
            &[
                Value::Text(target_checkpoint),
                Value::Text(file_id.clone()),
            ],
        )
        .await
        .expect("exact checkpoint file history hydrates");
    assert_eq!(history.rows().len(), 1);
    assert_eq!(
        history.rows()[0]
            .get::<String>("path")
            .expect("history path decodes"),
        "/bounded.md",
    );
    assert_eq!(
        history.rows()[0]
            .get::<Vec<u8>>("content")
            .expect("history content decodes"),
        b"version-31",
    );
    assert_eq!(
        probe.history_gets.load(Ordering::Acquire) - history_before,
        1,
        "a depth-ordered point lookup should hydrate only the selected checkpoint",
    );

    replica.close().await.expect("close replica");
    stop_server(server_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "legacy replica-local mutation/history semantics removed by the authority hard cut"]
async fn sparse_checkpoint_history_hydrates_missing_bodies_concurrently() {
    let (authority_storage, authority) = open_authority().await;
    let inserted = authority
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/checkpoint.md', CAST('version-00' AS BYTEA)) RETURNING id",
            &[],
        )
        .await
        .expect("create checkpointed file");
    let file_id = inserted.rows()[0]
        .get::<String>("id")
        .expect("file id decodes");
    let mut latest_checkpoint = None;
    for index in 0..12 {
        authority
            .execute(
                "UPDATE lix_file SET content = CAST($1 AS BYTEA) WHERE id = $2",
                &[
                    Value::Text(format!("version-{index:02}")),
                    Value::Text(file_id.clone()),
                ],
            )
            .await
            .expect("update checkpointed file");
        latest_checkpoint = Some(
            authority
                .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
                .await
                .expect("create file checkpoint")
                .rows()[0]
                .get::<String>("commit_id")
                .expect("checkpoint commit id decodes"),
        );
    }
    let latest_checkpoint = latest_checkpoint.expect("latest checkpoint captured");
    authority.close().await.expect("close authority setup");

    let probe = Arc::new(HttpProbe::default());
    let (url, server_task) = serve(authority_storage.clone(), Arc::clone(&probe)).await;
    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_replica(replica_dir.path(), &url).await;
    probe.set_round_trip_delay(Duration::from_millis(500));
    let history_before = probe.history_gets.load(Ordering::Acquire);

    let history = tokio::time::timeout(
        WAIT_TIMEOUT,
        replica.execute(
            "SELECT content FROM lix_history('lix_file', $1) WHERE id = $2 ORDER BY lixcol_depth",
            &[Value::Text(latest_checkpoint), Value::Text(file_id)],
        ),
    )
    .await
    .expect("sparse checkpoint history must complete promptly")
    .expect("sparse checkpoint history succeeds");
    assert!(history.rows().len() >= 12);
    assert!(
        probe.history_gets.load(Ordering::Acquire) - history_before >= 8,
        "fixture must retain enough cold checkpoint bodies to exercise batching",
    );
    assert!(
        probe.max_concurrent_history_gets.load(Ordering::Acquire) > 1,
        "one history attempt must hydrate its censused sparse bodies concurrently",
    );

    replica.close().await.expect("close replica");
    stop_server(server_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_bootstrap_pages_more_than_one_window_of_hot_rows() {
    let (authority_storage, authority) = open_authority().await;
    let statements = (0..OFFLINE_COMMIT_COUNT)
        .map(|index| ExecuteBatchStatement {
            label: None,
            sql: "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)".to_owned(),
            params: vec![
                Value::Text(format!("snapshot-page-{index:04}")),
                Value::Text(format!("value-{index}")),
            ],
        })
        .collect::<Vec<_>>();
    authority
        .execute_batch(&statements)
        .await
        .expect("seed more hot rows than one snapshot page");
    authority.close().await.expect("close authority setup");
    let probe = Arc::new(HttpProbe::default());
    let (url, server_task) = serve(authority_storage.clone(), Arc::clone(&probe)).await;
    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_replica(replica_dir.path(), &url).await;

    assert_eq!(
        read_value(&replica, "snapshot-page-0000").await.as_deref(),
        Some("value-0"),
    );
    assert_eq!(
        read_value(&replica, "snapshot-page-0512").await.as_deref(),
        Some("value-512"),
    );
    assert!(
        probe.snapshot_row_pulls.load(Ordering::Acquire) >= 2,
        "513 user rows plus system rows must cross multiple immutable snapshot pages",
    );

    replica.close().await.expect("close replica");
    stop_server(server_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "legacy replica-local mutation/history semantics removed by the authority hard cut"]
async fn warm_filesystem_replica_reopens_and_writes_while_offline() {
    let (authority_storage, authority) = open_authority().await;
    put_value(&authority, "cached", "durable").await;
    authority.close().await.expect("close authority setup");
    let (url, server_task) = serve(authority_storage.clone(), Arc::default()).await;
    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_replica(replica_dir.path(), &url).await;
    assert_eq!(
        read_value(&replica, "cached").await.as_deref(),
        Some("durable")
    );
    replica.close().await.expect("close online replica");
    drop(replica);
    stop_server(server_task).await;

    let opened_at = Instant::now();
    let offline = tokio::time::timeout(
        Duration::from_secs(2),
        open_replica(replica_dir.path(), &url),
    )
    .await
    .expect("warm reopen must not await an unavailable server");
    assert!(opened_at.elapsed() < Duration::from_secs(2));
    assert_eq!(
        read_value(&offline, "cached").await.as_deref(),
        Some("durable")
    );
    put_value(&offline, "offline", "queued").await;
    assert_eq!(
        read_value(&offline, "offline").await.as_deref(),
        Some("queued")
    );

    offline.close().await.expect("close offline replica");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "legacy replica-local mutation/history semantics removed by the authority hard cut"]
async fn authenticated_identity_survives_fresh_push_and_offline_reopen() {
    let (authority_storage, authority) = open_authority().await;
    put_value(&authority, "authenticated-seed", "server").await;
    let principal = ServerProtocolPrincipal::Authenticated {
        account_id: lix::SYSTEM_ACCOUNT_ID.to_owned(),
        idempotency_scope: "sync-mode-authenticated-e2e".to_owned(),
    };
    authority.close().await.expect("close authority setup");
    let probe = Arc::new(HttpProbe::default());
    let (url, server_task, protocol_authority) = serve_as_with_authority_session(
        authority_storage.clone(),
        Arc::clone(&probe),
        principal.clone(),
    )
    .await;
    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_replica(replica_dir.path(), &url).await;

    assert_eq!(active_account(&replica).await, lix::SYSTEM_ACCOUNT_ID);
    put_value(&replica, "authenticated-fresh", "accepted").await;
    protocol_authority
        .wait_for_value("authenticated-fresh", "accepted")
        .await;
    assert!(
        probe.pushes.load(Ordering::Acquire) > 0,
        "authenticated fresh write should reach the authority through sync push",
    );
    replica.close().await.expect("close authenticated replica");
    drop(replica);
    stop_server(server_task).await;

    let reopened = tokio::time::timeout(
        Duration::from_secs(2),
        open_replica(replica_dir.path(), &url),
    )
    .await
    .expect("authenticated warm reopen must not await the offline authority");
    assert_eq!(
        active_account(&reopened).await,
        lix::SYSTEM_ACCOUNT_ID,
        "durable replica identity must be installed before the primary session opens",
    );
    put_value(&reopened, "authenticated-offline", "queued").await;
    assert_eq!(
        read_value(&reopened, "authenticated-offline")
            .await
            .as_deref(),
        Some("queued"),
    );

    reopened
        .close()
        .await
        .expect("close authenticated offline replica");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "legacy replica-local mutation/history semantics removed by the authority hard cut"]
async fn more_than_one_offline_push_window_drains_after_reconnect() {
    let (authority_storage, authority) = open_authority().await;
    let commits_before = commit_count(&authority).await;
    authority.close().await.expect("close authority setup");
    let probe = Arc::new(HttpProbe::default());
    let (url, server_task, protocol_authority) =
        serve_with_authority_session(authority_storage.clone(), Arc::clone(&probe)).await;
    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_replica(replica_dir.path(), &url).await;
    wait_for_counter(&probe.delta_pulls, 1).await;

    probe.set_offline(true);
    for index in 0..OFFLINE_COMMIT_COUNT {
        put_value(&replica, "offline-window", &format!("value-{index}")).await;
    }
    let expected = format!("value-{}", OFFLINE_COMMIT_COUNT - 1);
    assert_eq!(
        read_value(&replica, "offline-window").await.as_deref(),
        Some(expected.as_str()),
    );
    replica
        .close()
        .await
        .expect("close replica with durable offline outbox");
    drop(replica);
    let replica = tokio::time::timeout(
        Duration::from_secs(2),
        open_replica(replica_dir.path(), &url),
    )
    .await
    .expect("warm outbox reopen must not await the offline authority");
    assert_eq!(
        read_value(&replica, "offline-window").await.as_deref(),
        Some(expected.as_str()),
    );

    probe.set_offline(false);
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if protocol_authority
                .read_value("offline-window")
                .await
                .as_deref()
                == Some(expected.as_str())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("offline outbox should drain after reconnect");
    assert_eq!(
        protocol_authority.commit_count().await,
        commits_before + OFFLINE_COMMIT_COUNT as i64,
    );
    assert!(
        probe.pushes.load(Ordering::Acquire) >= 2,
        "the outbox must cross at least two bounded pushes",
    );

    replica.close().await.expect("close replica");
    stop_server(server_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "legacy replica-local mutation/history semantics removed by the authority hard cut"]
async fn offline_restore_survives_reopen_and_resets_the_authority() {
    let (authority_storage, authority) = open_authority().await;
    put_value(&authority, "restore-reopen", "target").await;
    let restore_target = active_head(&authority).await;
    put_value(&authority, "restore-reopen", "later").await;
    let authority_head_before_restore = active_head(&authority).await;
    authority.close().await.expect("close authority setup");

    let probe = Arc::new(HttpProbe::default());
    let (url, server_task, protocol_authority) =
        serve_with_authority_session(authority_storage.clone(), Arc::clone(&probe)).await;
    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_replica(replica_dir.path(), &url).await;
    assert_eq!(
        read_value(&replica, "restore-reopen").await.as_deref(),
        Some("later"),
    );
    let historical = replica
        .execute(
            "SELECT value FROM lix_state_at('lix_key_value', $1) WHERE key = 'restore-reopen'",
            &[Value::Text(restore_target.clone())],
        )
        .await
        .expect("hydrate the historical restore target before going offline");
    assert_eq!(historical.len(), 1);

    // Keep the restore in the durable local outbox across close/reopen. This
    // models a browser losing connectivity (or being terminated) immediately
    // after the local-first restore commits.
    probe.set_offline(true);
    replica
        .execute(
            "INSERT INTO lix_restore (commit_id) VALUES ($1)",
            &[Value::Text(restore_target.clone())],
        )
        .await
        .expect("restore local replica to an authority ancestor");
    assert_eq!(active_head(&replica).await, restore_target);
    assert_eq!(
        read_value(&replica, "restore-reopen").await.as_deref(),
        Some("target"),
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    replica
        .close()
        .await
        .expect("close replica with an offline restore in its durable outbox");
    drop(replica);

    let reopened = tokio::time::timeout(
        Duration::from_secs(2),
        open_replica(replica_dir.path(), &url),
    )
    .await
    .expect("warm restore reopen must not await the unavailable authority");
    assert_eq!(active_head(&reopened).await, restore_target);
    assert_eq!(
        read_value(&reopened, "restore-reopen").await.as_deref(),
        Some("target"),
    );
    assert_eq!(
        protocol_authority
            .execute("SELECT lix_active_branch_commit_id()", &[])
            .await[0][0],
        Value::Text(authority_head_before_restore),
        "the rejected push must leave the authority at its later head",
    );

    probe.set_offline(false);
    tokio::time::timeout(WAIT_TIMEOUT, async {
        loop {
            let head = protocol_authority
                .execute("SELECT lix_active_branch_commit_id()", &[])
                .await;
            if head[0][0] == Value::Text(restore_target.clone()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the reopened replica should push the historical ref reset");
    assert_eq!(
        read_value(&reopened, "restore-reopen").await.as_deref(),
        Some("target"),
        "the authority lineage must not overwrite the local restore",
    );
    assert_eq!(
        protocol_authority
            .read_value("restore-reopen")
            .await
            .as_deref(),
        Some("target"),
        "the local restore should reset the authority through a ref CAS",
    );

    reopened.close().await.expect("close reopened replica");
    stop_server(server_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "legacy replica-local mutation/history semantics removed by the authority hard cut"]
async fn two_clients_receive_remote_writes_through_a_held_long_poll() {
    let (authority_storage, authority) = open_authority().await;
    put_value(&authority, "seed", "ready").await;
    authority.close().await.expect("close authority setup");
    let probe = Arc::new(HttpProbe::default());
    let (url, server_task, protocol_authority) =
        serve_with_authority_session(authority_storage.clone(), Arc::clone(&probe)).await;
    let alice_dir = TempDir::new().expect("alice tempdir");
    let bob_dir = TempDir::new().expect("bob tempdir");
    let alice = open_replica(alice_dir.path(), &url).await;
    let bob = open_replica(bob_dir.path(), &url).await;

    wait_for_counter(&probe.delta_pulls, 2).await;
    put_value(&alice, "shared", "from-alice").await;
    wait_for_value(&bob, "shared", "from-alice").await;
    protocol_authority
        .wait_for_value("shared", "from-alice")
        .await;
    wait_for_counter(&probe.delta_pulls, 3).await;
    protocol_authority
        .put_value("server-originated", "from-authority")
        .await;
    wait_for_value(&alice, "server-originated", "from-authority").await;
    wait_for_value(&bob, "server-originated", "from-authority").await;

    let ((), ()) = tokio::join!(
        put_value(&alice, "concurrent-alice", "alice"),
        put_value(&bob, "concurrent-bob", "bob"),
    );
    protocol_authority
        .wait_for_value("concurrent-alice", "alice")
        .await;
    protocol_authority
        .wait_for_value("concurrent-bob", "bob")
        .await;
    wait_for_value(&alice, "concurrent-bob", "bob").await;
    wait_for_value(&bob, "concurrent-alice", "alice").await;

    alice.close().await.expect("close alice");
    bob.close().await.expect("close bob");
    stop_server(server_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "legacy replica-local mutation/history semantics removed by the authority hard cut"]
async fn concurrent_file_insert_and_edit_reconcile_after_one_stale_push() {
    let (authority_storage, authority) = open_authority().await;
    authority
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/existing.md', CAST('base' AS BYTEA))",
            &[],
        )
        .await
        .expect("seed existing file");
    authority.close().await.expect("close authority setup");
    let probe = Arc::new(HttpProbe::default());
    let (url, server_task, protocol_authority) =
        serve_with_authority_session(authority_storage.clone(), Arc::clone(&probe)).await;
    let alice_dir = TempDir::new().expect("alice tempdir");
    let bob_dir = TempDir::new().expect("bob tempdir");
    let alice = open_replica(alice_dir.path(), &url).await;
    let bob = open_replica(bob_dir.path(), &url).await;

    let (alice_content, bob_content) = tokio::join!(
        read_file_content(&alice, "/existing.md"),
        read_file_content(&bob, "/existing.md"),
    );
    assert_eq!(alice_content, Some(b"base".to_vec()));
    assert_eq!(bob_content, Some(b"base".to_vec()));
    wait_for_counter(&probe.delta_pulls, 2).await;

    // Hold both publish requests until they have been built from the same
    // authority head. Exactly one CAS must then lose with HTTP 409.
    probe.gate_next_two_pushes();
    let (created, edited) = tokio::join!(
        alice.execute(
            "INSERT INTO lix_file (path, content) VALUES ('/created.md', CAST('created' AS BYTEA))",
            &[],
        ),
        bob.execute(
            "UPDATE lix_file SET content = CAST('edited' AS BYTEA) WHERE path = '/existing.md'",
            &[],
        ),
    );
    created.expect("Alice creates a file locally");
    edited.expect("Bob edits the existing file locally");

    protocol_authority
        .wait_for_file_content("/created.md", b"created")
        .await;
    protocol_authority
        .wait_for_file_content("/existing.md", b"edited")
        .await;
    wait_for_file_content(&alice, "/created.md", b"created").await;
    wait_for_file_content(&alice, "/existing.md", b"edited").await;
    wait_for_file_content(&bob, "/created.md", b"created").await;
    wait_for_file_content(&bob, "/existing.md", b"edited").await;
    assert_eq!(
        probe.push_conflicts.load(Ordering::Acquire),
        1,
        "two publishes from one head must exercise exactly one stale ref CAS",
    );

    // Sync publishes pending work before it waits for a remote event. If both
    // clients consume this authority-only event without another push, their
    // durable outboxes were empty after reconciliation.
    let pushes_after_convergence = probe.pushes.load(Ordering::Acquire);
    protocol_authority
        .put_value("outbox-sentinel", "authority-only")
        .await;
    wait_for_value(&alice, "outbox-sentinel", "authority-only").await;
    wait_for_value(&bob, "outbox-sentinel", "authority-only").await;
    assert_eq!(
        probe.pushes.load(Ordering::Acquire),
        pushes_after_convergence,
        "converged replicas must not retain pending outbox work",
    );

    alice.close().await.expect("close alice");
    bob.close().await.expect("close bob");
    stop_server(server_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "legacy replica-local mutation/history semantics removed by the authority hard cut"]
async fn small_file_observer_receives_remote_edit_without_a_chunk_round_trip() {
    let (authority_storage, authority) = open_authority().await;
    authority
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/shared.md', CAST('Hello world' AS BYTEA))",
            &[],
        )
        .await
        .expect("seed shared file");
    authority.close().await.expect("close authority setup");
    let probe = Arc::new(HttpProbe::default());
    probe.set_round_trip_delay(Duration::from_millis(100));
    let (url, server_task) = serve(authority_storage.clone(), Arc::clone(&probe)).await;
    let alice_dir = TempDir::new().expect("alice tempdir");
    let bob_dir = TempDir::new().expect("bob tempdir");
    let alice = open_replica(alice_dir.path(), &url).await;
    let bob = open_replica(bob_dir.path(), &url).await;

    let mut events = alice
        .observe(
            "SELECT content FROM lix_file WHERE path = '/shared.md'",
            &[],
        )
        .expect("observe shared file");
    let initial = events
        .next()
        .await
        .expect("initial observer evaluation succeeds")
        .expect("initial observer event exists");
    assert_eq!(
        initial.rows.rows()[0]
            .get::<Vec<u8>>("content")
            .expect("initial content decodes"),
        b"Hello world",
    );
    wait_for_counter(&probe.delta_pulls, 2).await;

    let chunk_gets_before_remote_edit = probe.chunk_gets.load(Ordering::Acquire);
    let chunk_puts_before_remote_edit = probe.chunk_puts.load(Ordering::Acquire);
    let started = Instant::now();
    bob.execute(
        "UPDATE lix_file SET content = CAST('Hello worlds' AS BYTEA) WHERE path = '/shared.md'",
        &[],
    )
    .await
    .expect("Bob updates the shared file");
    let remote = tokio::time::timeout(WAIT_TIMEOUT, events.next())
        .await
        .expect("timed out waiting for remote observer event")
        .expect("remote observer evaluation succeeds")
        .expect("remote observer event exists");
    assert_eq!(
        remote.rows.rows()[0]
            .get::<Vec<u8>>("content")
            .expect("remote content decodes"),
        b"Hello worlds",
    );
    assert_eq!(
        probe.chunk_gets.load(Ordering::Acquire),
        chunk_gets_before_remote_edit,
        "self-contained small manifests must not schedule chunk hydration",
    );
    assert_eq!(
        probe.chunk_puts.load(Ordering::Acquire),
        chunk_puts_before_remote_edit,
        "self-contained small manifests must not upload a separate chunk",
    );
    assert!(
        started.elapsed() < Duration::from_millis(700),
        "small-file propagation should stay below 700 ms at 100 ms RTT",
    );

    alice.close().await.expect("close alice");
    bob.close().await.expect("close bob");
    stop_server(server_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "legacy replica-local mutation/history semantics removed by the authority hard cut"]
async fn held_long_poll_stays_realtime_with_one_hundred_millisecond_rtt() {
    let (authority_storage, authority) = open_authority().await;
    put_value(&authority, "seed", "ready").await;
    authority.close().await.expect("close authority setup");
    let probe = Arc::new(HttpProbe::default());
    probe.set_round_trip_delay(Duration::from_millis(100));
    let (url, server_task) = serve(authority_storage.clone(), Arc::clone(&probe)).await;
    let alice_dir = TempDir::new().expect("alice tempdir");
    let bob_dir = TempDir::new().expect("bob tempdir");
    let alice = open_replica(alice_dir.path(), &url).await;
    let bob = open_replica(bob_dir.path(), &url).await;
    wait_for_counter(&probe.delta_pulls, 2).await;

    let started = Instant::now();
    put_value(&alice, "realtime", "local-first").await;
    assert_eq!(
        read_value(&alice, "realtime").await.as_deref(),
        Some("local-first"),
        "the originating client reads its write without a network round trip",
    );
    assert!(
        started.elapsed() < Duration::from_millis(100),
        "the local interaction path must stay below the simulated RTT",
    );
    wait_for_value(&bob, "realtime", "local-first").await;
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "held long-poll propagation should remain comfortably below 500 ms at 100 ms RTT",
    );

    alice.close().await.expect("close alice");
    bob.close().await.expect("close bob");
    stop_server(server_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "legacy replica-local mutation/history semantics removed by the authority hard cut"]
async fn lost_push_ack_retries_the_same_commit_idempotently() {
    let (authority_storage, authority) = open_authority().await;
    let commits_before = commit_count(&authority).await;
    authority.close().await.expect("close authority setup");
    let probe = Arc::new(HttpProbe::default());
    let (url, server_task, protocol_authority) =
        serve_with_authority_session(authority_storage.clone(), Arc::clone(&probe)).await;
    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_replica(replica_dir.path(), &url).await;
    wait_for_counter(&probe.delta_pulls, 1).await;

    probe.drop_next_push_ack();
    put_value(&replica, "lost-ack", "once").await;
    protocol_authority.wait_for_value("lost-ack", "once").await;
    replica
        .close()
        .await
        .expect("close after the authority committed but before a durable acknowledgement");
    drop(replica);
    let replica = open_replica(replica_dir.path(), &url).await;
    wait_for_counter(&probe.pushes, 2).await;
    assert_eq!(protocol_authority.commit_count().await, commits_before + 1);
    assert_eq!(
        read_value(&replica, "lost-ack").await.as_deref(),
        Some("once")
    );

    replica.close().await.expect("close replica");
    stop_server(server_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn binary_chunks_are_hydrated_before_hot_state_is_certified() {
    let (authority_storage, authority) = open_authority().await;
    let payload = (0..5 * 1024 * 1024 + 19)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    authority
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/lazy.bin', $1)",
            &[Value::Blob(payload.clone().into())],
        )
        .await
        .expect("write authority binary file");
    authority.close().await.expect("close authority setup");
    let probe = Arc::new(HttpProbe::default());
    let (url, server_task) = serve(authority_storage.clone(), Arc::clone(&probe)).await;
    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_replica(replica_dir.path(), &url).await;

    assert!(probe.blob_gets.load(Ordering::Acquire) > 0);
    let chunk_gets_after_open = probe.chunk_gets.load(Ordering::Acquire);
    assert!(
        chunk_gets_after_open > 0,
        "large live content must be hydrated before open returns"
    );
    let result = replica
        .execute("SELECT content FROM lix_file WHERE path = '/lazy.bin'", &[])
        .await
        .expect("first content read is served entirely from certified HOT state");
    assert_eq!(result.rows()[0].get::<Vec<u8>>("content").unwrap(), payload);
    assert_eq!(
        probe.chunk_gets.load(Ordering::Acquire),
        chunk_gets_after_open,
        "HOT content reads must not make a chunk round trip"
    );

    replica.close().await.expect("close replica");
    stop_server(server_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delta_hydrates_only_final_hot_blob_payloads_after_large_churn() {
    let (authority_storage, authority) = open_authority().await;
    let original = vec![1_u8; 5 * 1024 * 1024 + 17];
    authority
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/stable.bin', $1)",
            &[Value::Blob(original.clone().into())],
        )
        .await
        .expect("seed stable authority blob");
    authority.close().await.expect("close authority setup");

    let probe = Arc::new(HttpProbe::default());
    let (url, server_task, protocol_authority) =
        serve_with_authority_session(authority_storage, Arc::clone(&probe)).await;
    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_replica(replica_dir.path(), &url).await;
    assert_eq!(
        replica
            .execute(
                "SELECT content FROM lix_file WHERE path = '/stable.bin'",
                &[],
            )
            .await
            .expect("read certified stable blob")
            .rows()[0]
            .get::<Vec<u8>>("content")
            .unwrap(),
        original,
    );

    // Let the current long poll cross the offline boundary, then reject its
    // replacement so all following authority events arrive in one catch-up
    // page rather than being observed one by one.
    wait_for_counter(&probe.delta_pulls, 1).await;
    probe.set_offline(true);
    protocol_authority
        .put_value("blob-churn-boundary", "installed")
        .await;
    wait_for_value(&replica, "blob-churn-boundary", "installed").await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    let transient = vec![2_u8; 5 * 1024 * 1024 + 31];
    let deleted = vec![3_u8; 5 * 1024 * 1024 + 47];
    protocol_authority
        .execute(
            "UPDATE lix_file SET content = $1 WHERE path = '/stable.bin'",
            &[Value::Blob(transient.into())],
        )
        .await;
    protocol_authority
        .execute(
            "UPDATE lix_file SET content = $1 WHERE path = '/stable.bin'",
            &[Value::Blob(original.clone().into())],
        )
        .await;
    protocol_authority
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/deleted.bin', $1)",
            &[Value::Blob(deleted.into())],
        )
        .await;
    protocol_authority
        .execute("DELETE FROM lix_file WHERE path = '/deleted.bin'", &[])
        .await;
    protocol_authority
        .put_value("blob-churn-caught-up", "yes")
        .await;

    let chunks_before_catch_up = probe.chunk_gets.load(Ordering::Acquire);
    probe.set_offline(false);
    wait_for_value(&replica, "blob-churn-caught-up", "yes").await;
    assert_eq!(
        probe.chunk_gets.load(Ordering::Acquire),
        chunks_before_catch_up,
        "intermediate and deleted large blobs must retain only cold manifest metadata",
    );
    assert_eq!(
        replica
            .execute(
                "SELECT content FROM lix_file WHERE path = '/stable.bin'",
                &[],
            )
            .await
            .expect("read unchanged final stable blob")
            .rows()[0]
            .get::<Vec<u8>>("content")
            .unwrap(),
        original,
    );
    assert!(
        replica
            .execute("SELECT id FROM lix_file WHERE path = '/deleted.bin'", &[],)
            .await
            .expect("read final deleted path")
            .rows()
            .is_empty()
    );

    replica.close().await.expect("close replica");
    stop_server(server_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delta_hydrates_external_blob_survivors_in_an_in_page_child() {
    let (authority_storage, authority) = open_authority().await;
    let default_branch_id = authority
        .active_branch_id()
        .await
        .expect("load default authority branch");
    // Each payload is just over the inline ceiling and remains one canonical
    // chunk. One survives at the child head; the other survives only at the
    // branch's pinned checkpoint after the child replaces it.
    let inherited_head = vec![41_u8; 65 * 1024];
    let inherited_checkpoint = vec![73_u8; 65 * 1024];
    authority
        .execute_batch(&[
            ExecuteBatchStatement {
                label: None,
                sql: "INSERT INTO lix_file (path, content) VALUES ('/inherited-head.bin', $1)"
                    .to_owned(),
                params: vec![Value::Blob(inherited_head.clone().into())],
            },
            ExecuteBatchStatement {
                label: None,
                sql:
                    "INSERT INTO lix_file (path, content) VALUES ('/inherited-checkpoint.bin', $1)"
                        .to_owned(),
                params: vec![Value::Blob(inherited_checkpoint.clone().into())],
            },
        ])
        .await
        .expect("seed historical binary ancestor");
    let ancestor = active_head(&authority).await;
    authority
        .execute(
            "DELETE FROM lix_file WHERE path IN ('/inherited-head.bin', '/inherited-checkpoint.bin')",
            &[],
        )
        .await
        .expect("remove historical binaries from bootstrap HOT state");
    authority.close().await.expect("close authority setup");

    let probe = Arc::new(HttpProbe::default());
    let (url, server_task, protocol_authority) =
        serve_with_authority_session(authority_storage, Arc::clone(&probe)).await;
    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_replica(replica_dir.path(), &url).await;
    assert!(
        replica
            .execute(
                "SELECT id FROM lix_file WHERE path = '/inherited-head.bin'",
                &[],
            )
            .await
            .expect("historical file is absent at bootstrap head")
            .rows()
            .is_empty(),
    );

    // Allow the held poll to publish one boundary, then keep the replacement
    // poll offline so branch creation and its child arrive in one catch-up page.
    probe.set_offline(true);
    protocol_authority
        .put_value("external-survivor-boundary", "installed")
        .await;
    wait_for_value(&replica, "external-survivor-boundary", "installed").await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    let inherited_branch_id = "01920000-0000-7000-8000-000000009041";
    protocol_authority
        .create_branch(inherited_branch_id, "inherited-survivors", &ancestor)
        .await;
    protocol_authority.switch_branch(inherited_branch_id).await;
    protocol_authority
        .execute(
            "UPDATE lix_file SET content = CAST('new child payload' AS BYTEA) \
             WHERE path = '/inherited-checkpoint.bin'",
            &[],
        )
        .await;
    protocol_authority.switch_branch(&default_branch_id).await;
    protocol_authority
        .put_value("external-survivor-caught-up", "yes")
        .await;

    let chunks_before_catch_up = probe.chunk_gets.load(Ordering::Acquire);
    probe.set_offline(false);
    wait_for_value(&replica, "external-survivor-caught-up", "yes").await;
    assert_eq!(
        probe
            .chunk_gets
            .load(Ordering::Acquire)
            .saturating_sub(chunks_before_catch_up),
        2,
        "the inherited head payload and checkpoint-only payload must hydrate before publication",
    );

    tokio::time::timeout(
        Duration::from_secs(5),
        replica.switch_branch(SwitchBranchOptions {
            branch_id: inherited_branch_id.to_owned(),
        }),
    )
    .await
    .expect("connected switch_branch must not deadlock")
    .expect("switch replica session to inherited branch");
    let chunks_before_hot_read = probe.chunk_gets.load(Ordering::Acquire);
    assert_eq!(
        read_file_content(&replica, "/inherited-head.bin")
            .await
            .as_deref(),
        Some(inherited_head.as_slice()),
    );
    assert_eq!(
        replica
            .execute(
                "SELECT COUNT(*) AS count FROM lix_diff('lix_file') \
                 WHERE coalesce(to_path, from_path) = '/inherited-checkpoint.bin'",
                &[],
            )
            .await
            .expect("checkpoint-backed working diff remains HOT")
            .rows()[0]
            .get::<i64>("count")
            .unwrap(),
        1,
    );
    assert_eq!(
        probe.chunk_gets.load(Ordering::Acquire),
        chunks_before_hot_read,
        "first inherited file and working-diff reads must remain zero-RTT",
    );

    replica.close().await.expect("close replica");
    stop_server(server_task).await;
}

async fn open_replica(path: &Path, url: &str) -> Lix<FilesystemStorage> {
    open_lix()
        .with_storage(
            FilesystemStorage::new(path)
                .open()
                .expect("open filesystem storage"),
        )
        .with_server(ServerOptions::sync(url))
        .await
        .expect("open sync replica")
}

async fn put_value<S>(lix: &Lix<S>, key: &str, value: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ($1, $2) \
		 ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        &[Value::Text(key.to_owned()), Value::Text(value.to_owned())],
    )
    .await
    .expect("write key/value");
}

async fn read_value<S>(lix: &Lix<S>, key: &str) -> Option<String>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "SELECT value FROM lix_key_value WHERE key = $1",
        &[Value::Text(key.to_owned())],
    )
    .await
    .expect("read key/value")
    .rows()
    .first()
    .and_then(|row| row.get::<Value>("value").ok())
    .and_then(|value| match value {
        Value::Jsonb(value) => value.as_json_string(),
        Value::Text(value) => Some(value),
        _ => None,
    })
}

async fn read_file_content<S>(lix: &Lix<S>, path: &str) -> Option<Vec<u8>>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "SELECT content FROM lix_file WHERE path = $1",
        &[Value::Text(path.to_owned())],
    )
    .await
    .expect("read file content")
    .rows()
    .first()
    .and_then(|row| row.get::<Vec<u8>>("content").ok())
}

async fn wait_for_file_content<S>(lix: &Lix<S>, path: &str, expected: &[u8])
where
    S: Storage + Clone + Send + Sync + 'static,
{
    tokio::time::timeout(WAIT_TIMEOUT, async {
        loop {
            if read_file_content(lix, path).await.as_deref() == Some(expected) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "timed out waiting for synchronized file content at {path}: {:?}",
            String::from_utf8_lossy(expected),
        )
    });
}

async fn wait_for_value<S>(lix: &Lix<S>, key: &str, expected: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    tokio::time::timeout(WAIT_TIMEOUT, async {
        loop {
            if read_value(lix, key).await.as_deref() == Some(expected) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for synchronized value {key:?} = {expected:?}"));
}

async fn commit_count<S>(lix: &Lix<S>) -> i64
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute("SELECT COUNT(*) AS count FROM lix_commit", &[])
        .await
        .expect("count commits")
        .rows()[0]
        .get::<i64>("count")
        .expect("integer commit count")
}

async fn active_head<S>(lix: &Lix<S>) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute("SELECT lix_active_branch_commit_id() AS id", &[])
        .await
        .expect("read active head")
        .rows()[0]
        .get::<String>("id")
        .expect("active head id")
}

async fn active_account<S>(lix: &Lix<S>) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute("SELECT lix_active_account_id() AS id", &[])
        .await
        .expect("read active account")
        .rows()[0]
        .get::<String>("id")
        .expect("active account id")
}

async fn wait_for_counter(counter: &AtomicU64, expected: u64) {
    tokio::time::timeout(WAIT_TIMEOUT, async {
        while counter.load(Ordering::Acquire) < expected {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("timed out waiting for HTTP protocol activity");
}

async fn open_authority() -> (Memory, Arc<Lix<Memory>>) {
    let storage = Memory::new();
    let authority = Arc::new(
        open_lix()
            .with_storage(storage.clone())
            .await
            .expect("open authority"),
    );
    (storage, authority)
}

async fn serve(storage: Memory, probe: Arc<HttpProbe>) -> (String, tokio::task::JoinHandle<()>) {
    serve_as(storage, probe, ServerProtocolPrincipal::Anonymous).await
}

async fn serve_as(
    storage: Memory,
    probe: Arc<HttpProbe>,
    principal: ServerProtocolPrincipal,
) -> (String, tokio::task::JoinHandle<()>) {
    let protocol = open_lix()
        .with_storage(storage)
        .serve()
        .with_embedded_lix_id()
        .await
        .expect("serve authority");
    spawn_http_server(protocol, probe, principal).await
}

async fn serve_with_authority_session(
    storage: Memory,
    probe: Arc<HttpProbe>,
) -> (String, tokio::task::JoinHandle<()>, ProtocolAuthority) {
    serve_as_with_authority_session(storage, probe, ServerProtocolPrincipal::Anonymous).await
}

async fn serve_as_with_authority_session(
    storage: Memory,
    probe: Arc<HttpProbe>,
    principal: ServerProtocolPrincipal,
) -> (String, tokio::task::JoinHandle<()>, ProtocolAuthority) {
    let protocol = open_lix()
        .with_storage(storage)
        .serve()
        .with_embedded_lix_id()
        .await
        .expect("serve authority");
    let authority = ProtocolAuthority::open(protocol.clone(), principal.clone()).await;
    let (url, task) = spawn_http_server(protocol, probe, principal).await;
    (url, task, authority)
}

struct ProtocolAuthority {
    protocol: LixServerProtocol<Memory>,
    context: ServerProtocolContext,
    session_id: String,
    next_idempotency_key: AtomicU64,
}

impl ProtocolAuthority {
    async fn open(protocol: LixServerProtocol<Memory>, principal: ServerProtocolPrincipal) -> Self {
        let context = ServerProtocolContext {
            principal,
            durable_terminal_storage_notifier: None,
        };
        let response = protocol
            .handle(
                Request::builder()
                    .method("GET")
                    .uri(format!("/lix/v1/{}", protocol.lix_id()))
                    .body(ServerProtocolBody::empty())
                    .expect("build authority handshake"),
                context.clone(),
            )
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("collect authority handshake")
            .to_bytes();
        let body: JsonValue = serde_json::from_slice(&body).expect("decode authority handshake");
        Self {
            protocol,
            context,
            session_id: body["sessionId"]
                .as_str()
                .expect("authority session id")
                .to_owned(),
            next_idempotency_key: AtomicU64::new(0),
        }
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Vec<Vec<Value>> {
        let idempotency_key = self.next_idempotency_key.fetch_add(1, Ordering::AcqRel);
        let params = params
            .iter()
            .map(WireValue::try_from_engine)
            .collect::<Result<Vec<_>, _>>()
            .expect("encode authority execute params");
        let response = self
            .protocol
            .handle(
                Request::builder()
                    .method("POST")
                    .uri(format!("/lix/v1/{}/execute", self.protocol.lix_id()))
                    .header("lix-session-id", &self.session_id)
                    .header(
                        "idempotency-key",
                        format!("test-authority-{idempotency_key}"),
                    )
                    .header(CONTENT_TYPE, "application/json")
                    .body(ServerProtocolBody::from(
                        json!({ "sql": sql, "params": params }).to_string(),
                    ))
                    .expect("build authority execute"),
                self.context.clone(),
            )
            .await;
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("collect authority execute")
            .to_bytes();
        let body: JsonValue = serde_json::from_slice(&body).expect("decode authority execute");
        assert_eq!(status, StatusCode::OK, "authority execute failed: {body}");
        serde_json::from_value::<Vec<Vec<WireValue>>>(body["rows"].clone())
            .expect("decode authority execute rows")
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(WireValue::try_into_engine)
                    .collect::<Result<Vec<_>, _>>()
                    .expect("decode authority row values")
            })
            .collect()
    }

    async fn create_branch(&self, branch_id: &str, name: &str, from_commit_id: &str) {
        let response = self
            .protocol
            .handle(
                Request::builder()
                    .method("POST")
                    .uri(format!("/lix/v1/{}/branch/create", self.protocol.lix_id()))
                    .header("lix-session-id", &self.session_id)
                    .header(CONTENT_TYPE, "application/json")
                    .body(ServerProtocolBody::from(
                        json!({
                            "id": branch_id,
                            "name": name,
                            "fromCommitId": from_commit_id,
                        })
                        .to_string(),
                    ))
                    .expect("build authority create-branch request"),
                self.context.clone(),
            )
            .await;
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("collect authority create-branch response")
            .to_bytes();
        assert_eq!(
            status,
            StatusCode::OK,
            "authority create branch failed: {}",
            String::from_utf8_lossy(&body),
        );
    }

    async fn switch_branch(&self, branch_id: &str) {
        let response = self
            .protocol
            .handle(
                Request::builder()
                    .method("POST")
                    .uri(format!("/lix/v1/{}/branch/switch", self.protocol.lix_id()))
                    .header("lix-session-id", &self.session_id)
                    .header(CONTENT_TYPE, "application/json")
                    .body(ServerProtocolBody::from(
                        json!({ "branchId": branch_id }).to_string(),
                    ))
                    .expect("build authority switch-branch request"),
                self.context.clone(),
            )
            .await;
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("collect authority switch-branch response")
            .to_bytes();
        assert_eq!(
            status,
            StatusCode::OK,
            "authority switch branch failed: {}",
            String::from_utf8_lossy(&body),
        );
    }

    async fn put_value(&self, key: &str, value: &str) {
        self.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ($1, $2) \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
            &[Value::Text(key.to_owned()), Value::Text(value.to_owned())],
        )
        .await;
    }

    async fn read_value(&self, key: &str) -> Option<String> {
        self.execute(
            "SELECT value FROM lix_key_value WHERE key = $1",
            &[Value::Text(key.to_owned())],
        )
        .await
        .into_iter()
        .next()
        .and_then(|row| row.into_iter().next())
        .and_then(|value| match value {
            Value::Jsonb(value) => value.as_json_string(),
            Value::Text(value) => Some(value),
            _ => None,
        })
    }

    async fn read_file_content(&self, path: &str) -> Option<Vec<u8>> {
        self.execute(
            "SELECT content FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_owned())],
        )
        .await
        .into_iter()
        .next()
        .and_then(|row| row.into_iter().next())
        .and_then(|value| match value {
            Value::Blob(value) => Some(value.to_vec()),
            _ => None,
        })
    }

    async fn commit_count(&self) -> i64 {
        self.execute("SELECT COUNT(*) AS count FROM lix_commit", &[])
            .await
            .into_iter()
            .next()
            .and_then(|row| row.into_iter().next())
            .and_then(|value| match value {
                Value::Integer(value) => Some(value),
                _ => None,
            })
            .expect("integer authority commit count")
    }

    async fn wait_for_value(&self, key: &str, expected: &str) {
        tokio::time::timeout(WAIT_TIMEOUT, async {
            loop {
                if self.read_value(key).await.as_deref() == Some(expected) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("timed out waiting for authoritative value");
    }

    async fn wait_for_file_content(&self, path: &str, expected: &[u8]) {
        tokio::time::timeout(WAIT_TIMEOUT, async {
            loop {
                if self.read_file_content(path).await.as_deref() == Some(expected) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "timed out waiting for authoritative file content at {path}: {:?}",
                String::from_utf8_lossy(expected),
            )
        });
    }
}

async fn spawn_http_server(
    protocol: LixServerProtocol<Memory>,
    probe: Arc<HttpProbe>,
    principal: ServerProtocolPrincipal,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind HTTP protocol listener");
    let address = listener.local_addr().expect("HTTP protocol address");
    let locator = format!("http://{address}/lix/{}", protocol.lix_id());
    let task = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("accept HTTP client");
            let protocol = protocol.clone();
            let probe = Arc::clone(&probe);
            let principal = principal.clone();
            tokio::spawn(async move {
                let service = service_fn(move |request| {
                    handle_http(
                        protocol.clone(),
                        Arc::clone(&probe),
                        principal.clone(),
                        request,
                    )
                });
                let _ = http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    (locator, task)
}

async fn handle_http<S>(
    protocol: LixServerProtocol<S>,
    probe: Arc<HttpProbe>,
    principal: ServerProtocolPrincipal,
    request: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let (parts, body) = request.into_parts();
    if probe.reject_requests.load(Ordering::Acquire) {
        return Ok(Response::builder()
            .status(503)
            .header(CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from_static(
                br#"{"error":{"code":"LIX_SYNC_TEST_OFFLINE","message":"test server offline"}}"#,
            )))
            .expect("build offline response"));
    }
    let one_way_delay = Duration::from_millis(probe.one_way_delay_millis.load(Ordering::Acquire));
    tokio::time::sleep(one_way_delay).await;
    let path = parts.uri.path();
    let is_handshake = parts.method == Method::GET
        && path
            .strip_prefix("/lix/v1/")
            .is_some_and(|lix_id| !lix_id.is_empty() && !lix_id.contains('/'));
    if is_handshake {
        probe.handshakes.fetch_add(1, Ordering::Release);
        if probe.mismatch_handshake_protocol.load(Ordering::Acquire) {
            let body = serde_json::to_vec(&json!({
                "protocolVersion": lix::server_protocol::PROTOCOL_VERSION,
                "syncProtocolVersion": 999,
                "lixId": "01920000-0000-7000-8000-000000001234",
                "sessionId": "incompatible-test-session",
                "activeBranchId": "01920000-0000-7000-8000-000000001234",
                "activeAccountId": lix::ANONYMOUS_ACCOUNT_ID,
            }))
            .expect("encode mismatched handshake");
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/json")
                .body(Full::new(Bytes::from(body)))
                .expect("build mismatched handshake"));
        }
    }
    let is_push = parts.method == Method::POST && path.ends_with("/sync/push");
    if is_push && probe.reject_pushes.load(Ordering::Acquire) {
        return Ok(Response::builder()
            .status(503)
            .header(CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from_static(
                br#"{"error":{"code":"LIX_SYNC_TEST_PUSH_OFFLINE","message":"test push endpoint offline"}}"#,
            )))
            .expect("build push-offline response"));
    }
    let is_delta_pull = parts.method == Method::GET
        && path.ends_with("/sync/pull")
        && parts
            .uri
            .query()
            .is_some_and(|query| query.split('&').any(|part| part.starts_with("after=")));
    let is_publication_fence = is_delta_pull
        && parts
            .headers
            .get("prefer")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .any(|preference| preference.eq_ignore_ascii_case("wait=0"))
            });
    let is_snapshot_row_pull = parts.method == Method::GET
        && path.ends_with("/sync/pull")
        && parts.uri.query().is_some_and(|query| {
            query
                .split('&')
                .any(|part| part.starts_with("snapshotBranchId="))
        });
    let is_history_get = parts.method == Method::GET && path.ends_with("/sync/history");
    if is_push {
        probe.pushes.fetch_add(1, Ordering::Release);
    }
    if is_delta_pull {
        probe.delta_pulls.fetch_add(1, Ordering::Release);
    }
    if is_publication_fence {
        probe.publication_fences.fetch_add(1, Ordering::Release);
    }
    if is_snapshot_row_pull {
        probe.snapshot_row_pulls.fetch_add(1, Ordering::Release);
    }
    if parts.method == Method::GET && path.ends_with("/sync/blob") {
        probe.blob_gets.fetch_add(1, Ordering::Release);
    }
    if is_history_get {
        probe.history_gets.fetch_add(1, Ordering::Release);
        let active = probe.active_history_gets.fetch_add(1, Ordering::AcqRel) + 1;
        probe
            .max_concurrent_history_gets
            .fetch_max(active, Ordering::AcqRel);
    }
    if parts.method == Method::GET && path.ends_with("/sync/chunk") {
        probe.chunk_gets.fetch_add(1, Ordering::Release);
    }
    if parts.method == Method::PUT && path.ends_with("/sync/chunk") {
        probe.chunk_puts.fetch_add(1, Ordering::Release);
    }
    let body = body
        .collect()
        .await
        .expect("collect HTTP request body")
        .to_bytes();
    if is_push && let Some(gate) = probe.push_gate_slot() {
        gate.wait().await;
    }
    let response = protocol
        .handle(
            Request::from_parts(parts, ServerProtocolBody::full(body)),
            ServerProtocolContext {
                principal,
                durable_terminal_storage_notifier: None,
            },
        )
        .await;
    if is_history_get {
        probe.active_history_gets.fetch_sub(1, Ordering::AcqRel);
    }
    if is_push && response.status() == StatusCode::CONFLICT {
        probe.push_conflicts.fetch_add(1, Ordering::Release);
    }
    let (parts, body) = response.into_parts();
    let body = body
        .collect()
        .await
        .expect("collect protocol response")
        .to_bytes();
    tokio::time::sleep(one_way_delay).await;
    if is_push && probe.drop_next_push_ack.swap(false, Ordering::AcqRel) {
        return Ok(Response::builder()
			.status(503)
			.header(CONTENT_TYPE, "application/json")
			.body(Full::new(Bytes::from_static(
				br#"{"error":{"code":"LIX_SYNC_TEST_LOST_ACK","message":"test acknowledgement dropped"}}"#,
			)))
			.expect("build lost-ack response"));
    }
    Ok(Response::from_parts(parts, Full::new(body)))
}

/// Asserts every file row's `directory_id` resolves among the same tree's
/// directory rows — the filesystem-closure invariant the v75 repair restores.
fn assert_files_resolve_directories(files: &lix::ExecuteResult, directories: &lix::ExecuteResult) {
    let directory_ids = directories
        .rows()
        .iter()
        .map(|row| row.get::<String>("id").expect("directory id"))
        .collect::<std::collections::HashSet<_>>();
    for file in files.rows() {
        if let Ok(directory_id) = file.get::<String>("directory_id") {
            assert!(
                directory_ids.contains(&directory_id),
                "file references directory '{directory_id}' missing from the same tree"
            );
        }
    }
}

async fn stop_server(task: tokio::task::JoinHandle<()>) {
    task.abort();
    let _ = task.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "legacy replica-local mutation/history semantics removed by the authority hard cut"]
async fn fresh_replica_reads_point_in_time_filesystem_state() {
    // Regression: a freshly bootstrapped replica holds hot state plus
    // deferred history payloads. lix_state_at and lix_diff at an old
    // checkpoint must hydrate the missing payloads (like lix_history does)
    // instead of reading a partial tree — which surfaced as
    // "filesystem descriptor references missing directory" and empty
    // directory trees on a repository whose server copy is consistent.
    let (authority_storage, authority) = open_authority().await;
    authority
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/sales/playbook.md', CAST('one' AS BYTEA))",
            &[],
        )
        .await
        .expect("create /sales/playbook.md");
    authority
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/docs/handbook/inside.md', CAST('two' AS BYTEA))",
            &[],
        )
        .await
        .expect("create /docs/handbook/inside.md");
    let first_checkpoint = authority
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .expect("checkpoint seeded filesystem")
        .rows()[0]
        .get::<String>("commit_id")
        .expect("checkpoint commit id decodes");
    authority
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/brand/logo.md', CAST('three' AS BYTEA))",
            &[],
        )
        .await
        .expect("create /brand/logo.md");
    let second_checkpoint = authority
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .expect("checkpoint second filesystem state")
        .rows()[0]
        .get::<String>("commit_id")
        .expect("checkpoint commit id decodes");
    // Enough later commits that the checkpoint payloads stay cold on a
    // fresh bootstrap.
    for index in 0..105 {
        put_value(&authority, &format!("state-at-page-{index:03}"), "value").await;
    }
    authority.close().await.expect("close authority setup");

    let probe = Arc::new(HttpProbe::default());
    let (url, server_task) = serve(authority_storage.clone(), Arc::clone(&probe)).await;
    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_replica(replica_dir.path(), &url).await;

    let commit_id = first_checkpoint.clone();
    let files = replica
        .execute(
            "SELECT name, directory_id FROM lix_state_at('lix_file', $1)",
            &[Value::Text(commit_id.clone())],
        )
        .await
        .expect("point-in-time file state hydrates on a fresh replica");
    let file_names = files
        .rows()
        .iter()
        .map(|row| row.get::<String>("name").expect("file name"))
        .collect::<Vec<_>>();
    assert_eq!(
        file_names.len(),
        2,
        "first checkpoint holds both seeded files, got {file_names:?}"
    );

    let directories = replica
        .execute(
            "SELECT id, name FROM lix_state_at('lix_directory', $1)",
            &[Value::Text(commit_id.clone())],
        )
        .await
        .expect("point-in-time directory state hydrates on a fresh replica");
    let directory_names = directories
        .rows()
        .iter()
        .map(|row| row.get::<String>("name").expect("directory name"))
        .collect::<Vec<_>>();
    assert_eq!(
        directory_names.len(),
        3,
        "first checkpoint holds sales, docs, handbook — got {directory_names:?}"
    );
    assert_files_resolve_directories(&files, &directories);

    // The product's checkpoint-open read: a root diff with resolved paths.
    let diff = replica
        .execute(
            "SELECT to_path FROM lix_diff('lix_file', lix_root_commit_id(), $1) ORDER BY to_path",
            &[Value::Text(commit_id)],
        )
        .await
        .expect("root diff with paths hydrates on a fresh replica");
    let paths = diff
        .rows()
        .iter()
        .map(|row| row.get::<String>("to_path").expect("path"))
        .collect::<Vec<_>>();
    assert_eq!(
        paths,
        vec![
            "/docs/handbook/inside.md".to_string(),
            "/sales/playbook.md".to_string(),
        ],
        "resolved historical paths"
    );

    // The product's checkpoint click: the span between two checkpoints,
    // with both sides' paths resolved through their own commit trees.
    let span = replica
        .execute(
            "SELECT diff_type, coalesce(to_path, from_path) AS path
             FROM lix_diff('lix_file', $1, $2)
             ORDER BY coalesce(to_path, from_path)",
            &[
                Value::Text(first_checkpoint.clone()),
                Value::Text(second_checkpoint.clone()),
            ],
        )
        .await
        .expect("checkpoint-span diff hydrates on a fresh replica");
    let span_rows = span
        .rows()
        .iter()
        .map(|row| {
            (
                row.get::<String>("diff_type").expect("diff type"),
                row.get::<String>("path").expect("path"),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        span_rows,
        vec![("added".to_string(), "/brand/logo.md".to_string())],
        "checkpoint span resolves through both pinned trees"
    );

    // The newest checkpoint reads completely as well — the tree the
    // product opens first.
    let latest_directories = replica
        .execute(
            "SELECT name FROM lix_state_at('lix_directory', $1) ORDER BY name",
            &[Value::Text(second_checkpoint.clone())],
        )
        .await
        .expect("latest checkpoint directory state hydrates");
    let latest_names = latest_directories
        .rows()
        .iter()
        .map(|row| row.get::<String>("name").expect("directory name"))
        .collect::<Vec<_>>();
    assert_eq!(
        latest_names,
        vec![
            "brand".to_string(),
            "docs".to_string(),
            "handbook".to_string(),
            "sales".to_string(),
        ],
        "latest checkpoint holds every directory"
    );

    replica.close().await.expect("close replica");
    stop_server(server_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "legacy replica-local mutation/history semantics removed by the authority hard cut"]
async fn sparse_replica_observer_hydrates_root_history_past_a_merge_frontier() {
    // A bounded bootstrap can retain a recent linear head and a merge in its
    // jump/base closure while deferring the merge's direct first parent. Root
    // resolution must expose that absence as a sparse graph demand so observe
    // hydrates and retries.
    let (authority_storage, authority) = open_authority().await;
    let root_commit_id = authority
        .execute("SELECT lix_root_commit_id() AS commit_id", &[])
        .await
        .expect("authority root should resolve")
        .rows()[0]
        .get::<String>("commit_id")
        .expect("authority root id decodes");
    let main_branch_id = authority
        .active_branch_id()
        .await
        .expect("main branch id should load");
    let source = authority
        .create_branch(CreateBranchOptions {
            id: None,
            name: "sparse-root-source".to_owned(),
            from_commit_id: None,
        })
        .await
        .expect("source branch should be created");

    put_value(&authority, "main-only", "main").await;
    authority
        .switch_branch(SwitchBranchOptions {
            branch_id: source.id.clone(),
        })
        .await
        .expect("source branch should become active");
    put_value(&authority, "source-only", "source").await;
    authority
        .switch_branch(SwitchBranchOptions {
            branch_id: main_branch_id,
        })
        .await
        .expect("main branch should become active again");
    let merge = authority
        .merge_branch(MergeBranchOptions {
            source_branch_id: source.id,
        })
        .await
        .expect("diverged source should merge");
    assert!(
        merge.created_merge_commit_id.is_some(),
        "fixture requires a merge commit at the authority head"
    );
    for index in 0..3 {
        put_value(&authority, &format!("after-merge-{index}"), "after-merge").await;
    }
    authority.close().await.expect("close authority setup");

    let probe = Arc::new(HttpProbe::default());
    let (url, server_task) = serve(authority_storage, Arc::clone(&probe)).await;
    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_replica(replica_dir.path(), &url).await;
    let bootstrap_history_gets = probe.history_gets.load(Ordering::Acquire);

    let mut roots = replica
        .observe("SELECT lix_root_commit_id() AS commit_id", &[])
        .expect("root observer should open");
    let first = tokio::time::timeout(WAIT_TIMEOUT, roots.next())
        .await
        .expect("timed out waiting for hydrated root")
        .expect("root observer should hydrate sparse history")
        .expect("root observer should yield a first event");
    assert_eq!(
        first.rows.rows()[0]
            .get::<String>("commit_id")
            .expect("replica root id decodes"),
        root_commit_id,
    );
    assert_eq!(
        probe.history_gets.load(Ordering::Acquire),
        bootstrap_history_gets + 1,
        "root traversal past the merge frontier should demand one bounded history page"
    );

    roots.close();
    replica.close().await.expect("close replica");

    // Write diff commands resolve their source commits in a transaction-local
    // walker. A separate sparse replica ensures the read observer above has
    // not already hydrated the missing merge parent for this code path.
    let command_replica_dir = TempDir::new().expect("command replica tempdir");
    let command_replica = open_replica(command_replica_dir.path(), &url).await;
    let command_bootstrap_history_gets = probe.history_gets.load(Ordering::Acquire);
    let applied = command_replica
        .execute(
            "INSERT INTO lix_apply (row_ref) \
             SELECT row_ref \
             FROM lix_diff(\
               'lix_key_value', lix_root_commit_id(), lix_active_branch_commit_id()\
             ) \
             WHERE false",
            &[],
        )
        .await
        .expect("root-based write diff should hydrate sparse history");
    assert_eq!(applied.rows_affected(), 0, "empty apply should not mutate");
    assert_eq!(
        probe.history_gets.load(Ordering::Acquire),
        command_bootstrap_history_gets + 1,
        "write diff root traversal should demand one bounded history page"
    );

    command_replica
        .close()
        .await
        .expect("close command replica");
    stop_server(server_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "legacy replica-local mutation/history semantics removed by the authority hard cut"]
async fn partially_hydrated_replica_reads_point_in_time_filesystem_state() {
    // The live-repository shape behind the "filesystem descriptor references
    // missing directory" failure: a bounded history read hydrates ONLY the
    // checkpoint anchor commit, while the content commit that authored the
    // file and directory rows stays deferred. lix_state_at at the hydrated
    // anchor must then hydrate the owning commits it resolves rows from —
    // not silently materialize a partial tree.
    let (authority_storage, authority) = open_authority().await;
    authority
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/sales/playbook.md', CAST('one' AS BYTEA))",
            &[],
        )
        .await
        .expect("create /sales/playbook.md");
    let file_id = authority
        .execute(
            "SELECT id FROM lix_file WHERE path = '/sales/playbook.md'",
            &[],
        )
        .await
        .expect("read file id")
        .rows()[0]
        .get::<String>("id")
        .expect("file id decodes");
    let checkpoint = authority
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .expect("checkpoint seeded filesystem")
        .rows()[0]
        .get::<String>("commit_id")
        .expect("checkpoint commit id decodes");
    for index in 0..105 {
        put_value(
            &authority,
            &format!("partial-hydration-{index:03}"),
            "value",
        )
        .await;
    }
    authority.close().await.expect("close authority setup");

    let probe = Arc::new(HttpProbe::default());
    let (url, server_task) = serve(authority_storage.clone(), Arc::clone(&probe)).await;
    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_replica(replica_dir.path(), &url).await;

    // Selective hydration: the bounded point lookup hydrates only the
    // checkpoint anchor boundary, leaving the authoring commit deferred.
    replica
        .execute(
            "SELECT path FROM lix_history('lix_file', $1) WHERE id = $2 ORDER BY lixcol_depth ASC LIMIT 1",
            &[Value::Text(checkpoint.clone()), Value::Text(file_id.clone())],
        )
        .await
        .expect("bounded anchor lookup hydrates");

    // The point-in-time read at the now-hydrated anchor.
    let files = replica
        .execute(
            "SELECT name, directory_id FROM lix_state_at('lix_file', $1)",
            &[Value::Text(checkpoint.clone())],
        )
        .await
        .expect("file state at the anchor hydrates its owning commits");
    let directories = replica
        .execute(
            "SELECT id, name FROM lix_state_at('lix_directory', $1)",
            &[Value::Text(checkpoint.clone())],
        )
        .await
        .expect("directory state at the anchor hydrates its owning commits");
    assert_eq!(directories.rows().len(), 1, "sales directory present");
    assert_files_resolve_directories(&files, &directories);

    let diff = replica
        .execute(
            "SELECT to_path FROM lix_diff('lix_file', lix_root_commit_id(), $1)",
            &[Value::Text(checkpoint)],
        )
        .await
        .expect("root diff with paths at the anchor hydrates");
    assert_eq!(
        diff.rows()[0].get::<String>("to_path").expect("path"),
        "/sales/playbook.md",
    );

    replica.close().await.expect("close replica");
    stop_server(server_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "legacy replica-local mutation/history semantics removed by the authority hard cut"]
async fn migrated_partial_checkpoint_repository_reads_state_on_a_sparse_replica() {
    // Full lineage of the failing live repository: authored on the v71
    // engine, migrated and partial-checkpointed on the v72 engine
    // (fixture generated from 4816fdba5, SHA-256
    // 634eefb12a96bbb656214d5f203fb2f0dbd0fc552379754e3c86eb9cb99b6f70),
    // migrated to the current format here, served, and read from a fresh
    // sync replica after a bounded history lookup hydrated only the
    // checkpoint anchor.
    const V72_PARTIAL_CHECKPOINTS: &[u8] =
        include_bytes!("fixtures/v72_partial_checkpoints.lixsnap");
    let authority_storage = Memory::new();
    let authority = Arc::new(
        open_lix()
            .with_storage(authority_storage.clone())
            .from_snapshot(Cursor::new(V72_PARTIAL_CHECKPOINTS))
            .await
            .expect("open and automatically upgrade authority"),
    );
    let checkpoints = authority
        .execute(
            "SELECT commit_id FROM lix_checkpoint ORDER BY lixcol_created_at ASC",
            &[],
        )
        .await
        .expect("checkpoint listing");
    // Bootstrap + seed + two partial checkpoints (edited seeded file, new
    // file in a new directory).
    let last_checkpoint = checkpoints
        .rows()
        .last()
        .expect("fixture has checkpoints")
        .get::<String>("commit_id")
        .expect("commit id");
    let brand_file_id = authority
        .execute("SELECT id FROM lix_file WHERE path = '/brand/logo.md'", &[])
        .await
        .expect("read brand file id")
        .rows()[0]
        .get::<String>("id")
        .expect("file id decodes");
    for index in 0..105 {
        put_value(&authority, &format!("migrated-page-{index:03}"), "value").await;
    }
    authority.close().await.expect("close authority setup");

    let probe = Arc::new(HttpProbe::default());
    let (url, server_task) = serve(authority_storage.clone(), Arc::clone(&probe)).await;
    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_replica(replica_dir.path(), &url).await;

    // Selective hydration of the checkpoint anchor only.
    replica
        .execute(
            "SELECT path FROM lix_history('lix_file', $1) WHERE id = $2 ORDER BY lixcol_depth ASC LIMIT 1",
            &[
                Value::Text(last_checkpoint.clone()),
                Value::Text(brand_file_id.clone()),
            ],
        )
        .await
        .expect("bounded anchor lookup hydrates");

    let files = replica
        .execute(
            "SELECT name, directory_id FROM lix_state_at('lix_file', $1)",
            &[Value::Text(last_checkpoint.clone())],
        )
        .await
        .expect("file state at the migrated partial checkpoint hydrates");
    let directories = replica
        .execute(
            "SELECT id, name FROM lix_state_at('lix_directory', $1)",
            &[Value::Text(last_checkpoint.clone())],
        )
        .await
        .expect("directory state at the migrated partial checkpoint hydrates");
    assert_eq!(
        directories.rows().len(),
        4,
        "brand, docs, handbook, sales all present"
    );
    assert_eq!(files.rows().len(), 4, "all four files present");
    assert_files_resolve_directories(&files, &directories);

    let diff = replica
        .execute(
            "SELECT to_path FROM lix_diff('lix_file', lix_root_commit_id(), $1) ORDER BY to_path",
            &[Value::Text(last_checkpoint)],
        )
        .await
        .expect("root diff with paths at the migrated partial checkpoint hydrates");
    assert_eq!(diff.rows().len(), 4, "resolved paths for all four files");

    replica
        .execute(
            "UPDATE lix_file SET name = 'logo-working.md' WHERE id = $1",
            &[Value::Text(brand_file_id.clone())],
        )
        .await
        .expect("working edit on a checkpoint-selected row succeeds");
    replica
        .execute(
            "INSERT INTO lix_revert (row_ref) \
             SELECT row_ref FROM lix_diff('lix_file') WHERE id = $1 \
             RETURNING commit_id",
            &[Value::Text(brand_file_id)],
        )
        .await
        .expect("sparse replica reverts a working edit against the migrated checkpoint");
    let reverted = replica
        .execute(
            "SELECT path FROM lix_file WHERE path = '/brand/logo.md'",
            &[],
        )
        .await
        .expect("reverted file resolves");
    assert_eq!(reverted.rows().len(), 1, "working edit was reverted");

    replica.close().await.expect("close replica");
    stop_server(server_task).await;
}
