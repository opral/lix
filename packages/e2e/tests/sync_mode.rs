#![recursion_limit = "256"]

#[allow(dead_code)]
mod benchmark_metrics;

use std::convert::Infallible;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures_util::StreamExt as _;
use futures_util::io::Cursor;
use http::header::CONTENT_TYPE;
use http::{Method, Request, Response, StatusCode};
use http_body_util::BodyExt as _;
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use lix::server_protocol::{
    LixServerProtocol, PROTOCOL_VERSION, SERVER_PROTOCOL_VERSION_HEADER, ServerProtocolBody,
    ServerProtocolContext, ServerProtocolPrincipal,
};
use lix::storage::Storage;
use lix::{
    CreateBranchOptions, ExecuteBatchStatement, Lix, LixError, Memory, ServerOptions,
    SwitchBranchOptions, Value, WireValue, open_lix,
};
use lix_storage_filesystem::FilesystemStorage;
use serde_json::{Value as JsonValue, json};
use tempfile::TempDir;
use tokio::net::TcpListener;

use benchmark_metrics::AllocationScope;

const WAIT_TIMEOUT: Duration = Duration::from_secs(15);
const BOOTSTRAP_ROW_COUNT: usize = 513;
const HOT_STATE_PROFILE_RECORD_PREFIX: &str = "LIX_HOT_STATE_PROFILE_JSON=";

#[derive(Debug, Default)]
struct HttpProbe {
    attempted_requests: AtomicU64,
    response_body_bytes: AtomicU64,
    handshakes: AtomicU64,
    delta_pulls: AtomicU64,
    descriptor_pulls: AtomicU64,
    descriptor_waits: AtomicU64,
    merge_conflicts: AtomicU64,
    native_object_reads: AtomicU64,
    native_metadata_reads: AtomicU64,
    publication_fences: AtomicU64,
    snapshot_row_pulls: AtomicU64,
    history_gets: AtomicU64,
    blob_gets: AtomicU64,
    chunk_gets: AtomicU64,
    chunk_puts: AtomicU64,
    reject_requests: AtomicBool,
    mismatch_handshake_protocol: AtomicBool,
    one_way_delay_millis: AtomicU64,
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
async fn connected_api_routes_local_work_and_hot_reads_need_no_round_trip() {
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
        .expect("connected mutation commits locally");
    assert_eq!(
        read_value(&replica, "replica-write").await.as_deref(),
        Some("authoritative"),
        "a successful local mutation is immediately readable",
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
    switch_to_uploaded_branch(&replica, &race_branch.id).await;
    assert_eq!(
        read_value(&replica, "branch-race").await.as_deref(),
        Some("main")
    );
    replica
        .execute(
            "UPDATE lix_key_value SET value = 'child' WHERE key = 'branch-race'",
            &[],
        )
        .await
        .expect("write admitted created branch");
    assert_eq!(
        read_value(&replica, "branch-race").await.as_deref(),
        Some("child")
    );
    replica
        .switch_branch(SwitchBranchOptions {
            branch_id: lix::GLOBAL_BRANCH_ID.to_owned(),
        })
        .await
        .expect("switch to admitted global plane");
    replica
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('branch-race', 'global')",
            &[],
        )
        .await
        .expect("seed global-plane race marker");
    switch_to_uploaded_branch(&replica, &main_branch_id).await;

    tokio::time::timeout(WAIT_TIMEOUT, async {
        tokio::try_join!(
            async {
                for _ in 0..8 {
                    replica
                        .switch_branch(SwitchBranchOptions {
                            branch_id: lix::GLOBAL_BRANCH_ID.to_owned(),
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
                    } else if branch_id == lix::GLOBAL_BRANCH_ID {
                        "global"
                    } else {
                        panic!("read exposed unknown connected branch '{branch_id}'")
                    };
                    assert_eq!(
                        value, expected,
                        "branch selector and HOT row must be atomic"
                    );
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
        .expect("connected transaction begins locally");
    transaction
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('transaction-write', 'committed')",
            &[],
        )
        .await
        .expect("connected transaction stages locally");
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

    let mut guarded = replica
        .begin_transaction()
        .await
        .expect("connected transaction captures its own context");
    guarded
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('pending-isolated', 'discarded')",
            &[],
        )
        .await
        .expect("stage data in the isolated transaction");
    assert_eq!(read_value(&replica, "pending-isolated").await, None);
    replica
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('outside-transaction', 'independent')",
            &[],
        )
        .await
        .expect("parent writes remain independent of the active transaction");
    let mut observation = replica
        .observe("SELECT key FROM lix_key_value WHERE key IN ('pending-isolated', 'outside-transaction') ORDER BY key", &[])
        .expect("parent can register observations during a transaction");
    let initial = tokio::time::timeout(WAIT_TIMEOUT, observation.next())
        .await
        .expect("parent observation must not stall during a transaction")
        .expect("parent observation remains usable")
        .expect("parent observation returns committed state");
    assert_eq!(initial.rows.rows().len(), 1);
    assert_eq!(
        initial.rows.rows()[0].get::<String>("key").unwrap(),
        "outside-transaction"
    );
    observation.close();
    let close_error = replica
        .close()
        .await
        .expect_err("close must reject an active connected transaction");
    assert_eq!(close_error.code, "LIX_INVALID_TRANSACTION_STATE");
    tokio::time::timeout(
        Duration::from_secs(5),
        replica.switch_branch(SwitchBranchOptions {
            branch_id: lix::GLOBAL_BRANCH_ID.to_owned(),
        }),
    )
    .await
    .expect("switch under connected transaction must not deadlock")
    .expect("parent branch can change independently of the transaction");
    let captured = guarded
        .execute("SELECT lix_active_branch_id() AS branch_id", &[])
        .await
        .expect("transaction retains its captured branch");
    assert_eq!(
        captured.rows()[0].get::<String>("branch_id").unwrap(),
        main_branch_id
    );
    guarded
        .rollback()
        .await
        .expect("connected transaction rollback releases its lifecycle reservation");
    assert_eq!(
        replica.active_branch_id().await.unwrap(),
        lix::GLOBAL_BRANCH_ID
    );
    replica
        .switch_branch(SwitchBranchOptions {
            branch_id: main_branch_id.clone(),
        })
        .await
        .unwrap();
    assert_eq!(read_value(&replica, "pending-isolated").await, None);
    assert_eq!(
        read_value(&replica, "outside-transaction").await.as_deref(),
        Some("independent")
    );
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
        .expect("connected transaction uses an isolated local session");
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
    for sql in [
        "INSERT INTO lix_key_value (key, value) VALUES ('coherent-write', 'must-not-exist')",
        "SELECT uuidv7()",
        "SELECT current_timestamp",
    ] {
        let error = replica
            .execute_coherent_read_batch(&[
                ("SELECT * FROM lix_commit WHERE is_checkpoint", &[]),
                (sql, &[]),
            ])
            .await
            .expect_err("connected coherent batches must reject mutations");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM, "{sql}");
    }
    assert_eq!(protocol_authority.read_value("coherent-write").await, None);
    // The history query above hydrated its immutable inputs. Background upload
    // acknowledgments may independently issue finite pulls, so verify local
    // completion with the authority unavailable instead of counting those pulls.
    probe.set_offline(true);
    let coherent_history_statements: [(&str, &[Value]); 1] =
        [("SELECT * FROM lix_history('lix_key_value')", &[])];
    let coherent_history = replica
        .execute_coherent_read_batch(&coherent_history_statements)
        .await
        .expect("cached coherent history completes without the authority");
    assert!(!coherent_history.results[0].rows().is_empty());
    assert_eq!(
        coherent_history.active_branch_id,
        replica.active_branch_id().await.expect("active branch"),
    );
    assert!(
        coherent_history.storage_mutation_revision.is_some(),
        "hydrated history shares a local adapter snapshot",
    );
    probe.set_offline(false);
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
    let offline_read = reopened_without_server
        .execute_coherent_read_batch(&[(
            "SELECT value FROM lix_key_value WHERE key = 'after-abandoned'",
            &[],
        )])
        .await
        .expect("a persisted certified HOT cache remains readable without a server");
    assert_eq!(offline_read.results[0].rows().len(), 1);
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
    fn set_round_trip_delay(&self, round_trip: Duration) {
        self.one_way_delay_millis.store(
            u64::try_from(round_trip.as_millis() / 2).expect("test delay fits u64"),
            Ordering::Release,
        );
    }

    fn set_offline(&self, offline: bool) {
        self.reject_requests.store(offline, Ordering::Release);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_open_defers_rows_and_point_reads_hydrate_native_inputs() {
    let (authority_storage, authority) = open_authority().await;
    let statements = (0..BOOTSTRAP_ROW_COUNT)
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

    assert_eq!(probe.snapshot_row_pulls.load(Ordering::Acquire), 0);
    assert_eq!(probe.native_object_reads.load(Ordering::Acquire), 0);
    assert_eq!(probe.native_metadata_reads.load(Ordering::Acquire), 0);
    assert_eq!(
        read_value(&replica, "snapshot-page-0000").await.as_deref(),
        Some("value-0"),
    );
    assert_eq!(
        read_value(&replica, "snapshot-page-0512").await.as_deref(),
        Some("value-512"),
    );
    assert_eq!(probe.snapshot_row_pulls.load(Ordering::Acquire), 0);
    assert!(probe.native_object_reads.load(Ordering::Acquire) > 0);
    probe.set_offline(true);
    assert_eq!(
        read_value(&replica, "snapshot-page-0000").await.as_deref(),
        Some("value-0")
    );
    assert_eq!(
        read_value(&replica, "snapshot-page-0512").await.as_deref(),
        Some("value-512")
    );

    replica.close().await.expect("close replica");
    stop_server(server_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

    for replica in [&alice, &bob] {
        for key in ["shared", "server-originated", "next-alice", "next-bob"] {
            assert_eq!(read_value(replica, key).await, None);
        }
    }
    wait_for_counter(&probe.descriptor_waits, 2).await;
    put_value(&alice, "shared", "from-alice").await;
    wait_for_value(&bob, "shared", "from-alice").await;
    protocol_authority
        .wait_for_value("shared", "from-alice")
        .await;
    wait_for_counter(&probe.descriptor_waits, 3).await;
    protocol_authority
        .put_value("server-originated", "from-authority")
        .await;
    wait_for_value(&alice, "server-originated", "from-authority").await;
    wait_for_value(&bob, "server-originated", "from-authority").await;

    // Serial acceptance exercises both clients' native local upload paths.
    put_value(&alice, "next-alice", "alice").await;
    wait_for_value(&bob, "next-alice", "alice").await;
    put_value(&bob, "next-bob", "bob").await;
    wait_for_value(&alice, "next-bob", "bob").await;
    protocol_authority.wait_for_value("next-bob", "bob").await;

    alice.close().await.expect("close alice");
    bob.close().await.expect("close bob");
    stop_server(server_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
    wait_for_counter(&probe.descriptor_waits, 2).await;

    let chunk_gets_before_remote_edit = probe.chunk_gets.load(Ordering::Acquire);
    let chunk_puts_before_remote_edit = probe.chunk_puts.load(Ordering::Acquire);
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

    alice.close().await.expect("close alice");
    bob.close().await.expect("close bob");
    stop_server(server_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn binary_chunks_hydrate_on_demand_and_remain_available_offline() {
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

    assert_eq!(probe.blob_gets.load(Ordering::Acquire), 0);
    assert_eq!(probe.chunk_gets.load(Ordering::Acquire), 0);
    let result = replica
        .execute("SELECT content FROM lix_file WHERE path = '/lazy.bin'", &[])
        .await
        .expect("first content read hydrates the requested native content");
    assert_eq!(result.rows()[0].get::<Vec<u8>>("content").unwrap(), payload);
    assert!(probe.blob_gets.load(Ordering::Acquire) > 0);
    let chunk_gets = probe.chunk_gets.load(Ordering::Acquire);
    assert!(chunk_gets > 0);
    probe.set_offline(true);
    assert_eq!(
        read_file_content(&replica, "/lazy.bin").await,
        Some(payload.clone())
    );
    assert_eq!(probe.chunk_gets.load(Ordering::Acquire), chunk_gets);
    replica.close().await.expect("close hydrated replica");
    let replica = open_replica(replica_dir.path(), &url).await;
    assert_eq!(
        read_file_content(&replica, "/lazy.bin").await,
        Some(payload)
    );
    assert_eq!(probe.chunk_gets.load(Ordering::Acquire), chunk_gets);

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

    assert_eq!(read_value(&replica, "blob-churn-caught-up").await, None);
    wait_for_counter(&probe.descriptor_waits, 1).await;
    probe.set_offline(true);

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
async fn remote_branch_content_is_hydrated_only_after_explicit_selection() {
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

    assert_eq!(
        read_value(&replica, "external-survivor-caught-up").await,
        None
    );
    wait_for_counter(&probe.descriptor_waits, 1).await;
    probe.set_offline(true);

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
        probe.chunk_gets.load(Ordering::Acquire),
        chunks_before_catch_up,
        "an unrelated branch must not hydrate its head or checkpoint content",
    );
    switch_to_uploaded_branch(&replica, inherited_branch_id).await;
    assert_eq!(
        replica.active_branch_id().await.unwrap(),
        inherited_branch_id
    );
    assert_eq!(
        probe.chunk_gets.load(Ordering::Acquire),
        chunks_before_catch_up,
        "branch admission alone must not fetch unrelated content bytes"
    );
    replica
        .close()
        .await
        .expect("close original branch replica");
    // Fresh primary opens select the authority's tracked default branch.
    // Set that default through the public global-session SQL API.
    protocol_authority
        .switch_branch(lix::GLOBAL_BRANCH_ID)
        .await;
    protocol_authority
        .execute(
            "UPDATE lix_key_value SET value = $1 WHERE key = 'lix_default_branch_id'",
            &[Value::Text(inherited_branch_id.to_owned())],
        )
        .await;
    let selected_dir = TempDir::new().expect("selected branch tempdir");
    let replica = open_replica(selected_dir.path(), &url).await;
    assert_eq!(
        replica.active_branch_id().await.unwrap(),
        inherited_branch_id
    );
    assert_eq!(
        probe.chunk_gets.load(Ordering::Acquire),
        chunks_before_catch_up
    );
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
        chunks_before_hot_read + 1,
        "only the requested inherited head chunk should hydrate",
    );

    probe.set_offline(true);
    assert_eq!(
        read_file_content(&replica, "/inherited-head.bin")
            .await
            .as_deref(),
        Some(inherited_head.as_slice())
    );
    replica.close().await.expect("close replica");
    stop_server(server_task).await;
}

/// A successful cold write must leave its current value usable offline without
/// requiring an earlier SELECT or a historical checkpoint read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_update_supports_offline_current_reads_and_repeated_edits() {
    let (storage, authority) = open_authority().await;
    put_value(&authority, "cold-update", "baseline").await;
    authority.close().await.unwrap();
    let probe = Arc::new(HttpProbe::default());
    let (url, server_task, _remote) =
        serve_with_authority_session(storage, Arc::clone(&probe)).await;
    let directory = TempDir::new().unwrap();
    let replica = open_replica(directory.path(), &url).await;
    replica
        .execute(
            "UPDATE lix_key_value SET value = 'first-local' WHERE key = 'cold-update'",
            &[],
        )
        .await
        .expect("cold update hydrates through ordinary runtime retries");
    probe.set_offline(true);
    assert_eq!(
        read_value(&replica, "cold-update").await.as_deref(),
        Some("first-local")
    );
    for index in 0..3 {
        let value = format!("offline-{index}");
        replica
            .execute(
                "UPDATE lix_key_value SET value = $1 WHERE key = 'cold-update'",
                &[Value::Text(value.clone())],
            )
            .await
            .expect("repeat same-key update offline");
        assert_eq!(read_value(&replica, "cold-update").await, Some(value));
    }
    replica
        .close()
        .await
        .expect("close offline cold-write replica");
    drop(replica);
    let reopened = open_replica(directory.path(), &url).await;
    assert_eq!(
        read_value(&reopened, "cold-update").await.as_deref(),
        Some("offline-2")
    );
    reopened
        .close()
        .await
        .expect("close durable offline reopen");
    stop_server(server_task).await;
}

/// Match the browser's tall native tree: SELECT alone must prepare the first
/// existing-key write, not just repeat a write whose online trial filled gaps.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn first_select_at_16000_rows_supports_offline_updates_and_reopen() {
    let (storage, authority) = open_authority().await;
    for start in (0..16000).step_by(256) {
        let values = (start..(start + 256).min(16000))
            .map(|row| {
                format!(
                    "('partial-open-{row:06}', 'payload-{row:06}-{}')",
                    "x".repeat(128)
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        authority
            .execute(
                &format!("INSERT INTO lix_key_value(key,value) VALUES {values}"),
                &[],
            )
            .await
            .expect("seed browser-equivalent row batch");
    }
    authority.close().await.unwrap();
    let probe = Arc::new(HttpProbe::default());
    let (url, server_task, _remote) =
        serve_with_authority_session(storage, Arc::clone(&probe)).await;
    let directory = TempDir::new().unwrap();
    let replica = open_replica(directory.path(), &url).await;
    let first = replica
        .execute(
            "SELECT value FROM lix_key_value WHERE key=$1",
            &[Value::Text("partial-open-000000".into())],
        )
        .await
        .expect("first SELECT hydrates existing-key dependencies");
    probe.set_offline(true);
    let first_value = match first.rows()[0].get::<Value>("value").unwrap() {
        Value::Jsonb(value) => value.as_json_string(),
        Value::Text(value) => Some(value),
        _ => None,
    };
    assert_eq!(
        first_value,
        Some(format!("payload-000000-{}", "x".repeat(128)))
    );
    for iteration in 0..31 {
        let value = format!("offline-{iteration}");
        let result = replica
            .execute(
                "UPDATE lix_key_value SET value=$1 WHERE key=$2",
                &[
                    Value::Text(value.clone()),
                    Value::Text("partial-open-000000".into()),
                ],
            )
            .await;
        assert!(
            result.is_ok(),
            "offline UPDATE iteration {iteration}: {:?}",
            result.err()
        );
        assert_eq!(
            read_value(&replica, "partial-open-000000").await,
            Some(value)
        );
    }
    replica.close().await.unwrap();
    drop(replica);
    let reopened = open_replica(directory.path(), &url).await;
    assert_eq!(
        read_value(&reopened, "partial-open-000000")
            .await
            .as_deref(),
        Some("offline-30")
    );
    reopened.close().await.unwrap();
    stop_server(server_task).await;
}

/// SELECT hydrates current rows without performing a preparatory mutation.
/// The same ordinary edits must then survive a completely disconnected worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_select_supports_repeated_offline_kv_and_file_edits() {
    let (storage, authority) = open_authority().await;
    authority
        .execute(
            "INSERT INTO lix_key_value(key,value) VALUES('ordinary-prefetch','initial')",
            &[],
        )
        .await
        .expect("seed current key/value");
    let original = vec![b'x'; 96 * 1024];
    authority
        .execute(
            "INSERT INTO lix_file(path,content) VALUES('/ordinary-prefetch.bin',$1)",
            &[Value::Blob(original.clone().into())],
        )
        .await
        .expect("seed ordinary file");
    authority.close().await.unwrap();
    let probe = Arc::new(HttpProbe::default());
    let (url, server_task, _remote) =
        serve_with_authority_session(storage, Arc::clone(&probe)).await;
    let directory = TempDir::new().unwrap();
    let replica = open_replica(directory.path(), &url).await;
    assert_eq!(
        read_value(&replica, "ordinary-prefetch").await.as_deref(),
        Some("initial")
    );
    assert_eq!(
        read_file_content(&replica, "/ordinary-prefetch.bin").await,
        Some(original.clone())
    );
    probe.set_offline(true);
    let mut expected = original;
    for index in 0..3 {
        expected[100 + index] = b'a' + u8::try_from(index).unwrap();
        let value = format!("offline-{index}");
        replica
            .execute(
                "UPDATE lix_key_value SET value=$1 WHERE key='ordinary-prefetch'",
                &[Value::Text(value.clone())],
            )
            .await
            .expect("edit previously selected key/value offline");
        replica
            .execute(
                "UPDATE lix_file SET content=$1 WHERE path='/ordinary-prefetch.bin'",
                &[Value::Blob(expected.clone().into())],
            )
            .await
            .expect("edit previously selected file offline");
        assert_eq!(read_value(&replica, "ordinary-prefetch").await, Some(value));
        assert_eq!(
            read_file_content(&replica, "/ordinary-prefetch.bin").await,
            Some(expected.clone())
        );
    }
    replica.close().await.expect("close offline replica");
    drop(replica);
    let reopened = open_replica(directory.path(), &url).await;
    assert_eq!(
        read_value(&reopened, "ordinary-prefetch").await.as_deref(),
        Some("offline-2")
    );
    assert_eq!(
        read_file_content(&reopened, "/ordinary-prefetch.bin").await,
        Some(expected)
    );
    reopened
        .close()
        .await
        .expect("close durable offline reopen");
    stop_server(server_task).await;
}

/// A disconnected warm replica is a stronger zero-RTT check than a timing
/// threshold: no successful authority call can be hidden in these operations.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_writes_checkpoints_and_folder_moves_survive_offline_reopen() {
    let (storage, authority) = open_authority().await;
    authority
        .execute(
            "INSERT INTO lix_directory (path) VALUES ('/a'), ('/b')",
            &[],
        )
        .await
        .expect("seed folders");
    authority
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/a/note.txt', $1)",
            &[Value::Blob(b"original".to_vec().into())],
        )
        .await
        .expect("seed nested file");
    authority
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .expect("seed checkpoint");
    authority.close().await.unwrap();
    let probe = Arc::new(HttpProbe::default());
    let (url, server_task, remote) =
        serve_with_authority_session(storage, Arc::clone(&probe)).await;
    let directory = TempDir::new().unwrap();
    let replica = open_replica(directory.path(), &url).await;
    replica
        .execute(
            "SELECT content FROM lix_file WHERE path = '/a/note.txt'",
            &[],
        )
        .await
        .expect("prefetch file content through ordinary SQL");
    for sql in [
        "SELECT * FROM lix_directory",
        "SELECT * FROM lix_file",
        "SELECT * FROM lix_key_value",
        "SELECT row_ref, from_path, to_path FROM lix_diff('lix_file')",
        "SELECT * FROM lix_diff('lix_key_value')",
        "SELECT row_ref FROM lix_history('lix_file')",
        "SELECT row_ref FROM lix_history('lix_directory')",
        "SELECT row_ref FROM lix_history('lix_key_value')",
    ] {
        replica
            .execute(sql, &[])
            .await
            .expect("hydrate fixture move and checkpoint scopes");
    }
    probe.set_offline(true);

    let mut transaction = replica
        .begin_transaction()
        .await
        .expect("begin offline transaction");
    transaction
        .execute(
            "UPDATE lix_directory SET path = '/b/a' WHERE path = '/a'",
            &[],
        )
        .await
        .expect("move folder offline");
    transaction
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('offline-marker', 'durable')",
            &[],
        )
        .await
        .expect("stage offline marker");
    transaction
        .commit()
        .await
        .expect("commit offline transaction");
    assert_eq!(
        read_file_content(&replica, "/b/a/note.txt").await,
        Some(b"original".to_vec())
    );
    let partial = replica.execute(
        "SELECT commit_id FROM lix_create_checkpoint(ARRAY[lix_row_ref('lix_key_value', 'offline-marker')])", &[])
        .await.expect("create partial checkpoint offline").rows()[0]
        .get::<String>("commit_id").unwrap();
    let full = replica
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .expect("create full checkpoint offline")
        .rows()[0]
        .get::<String>("commit_id")
        .unwrap();
    assert_ne!(partial, full);
    replica
        .close()
        .await
        .expect("close with durable pending transactions");

    let replica = open_replica(directory.path(), &url).await;
    assert_eq!(
        read_value(&replica, "offline-marker").await.as_deref(),
        Some("durable")
    );
    assert_eq!(
        read_file_content(&replica, "/b/a/note.txt").await,
        Some(b"original".to_vec())
    );
    probe.set_offline(false);
    remote.wait_for_value("offline-marker", "durable").await;
    tokio::time::timeout(WAIT_TIMEOUT, async {
        loop {
            let rows = remote
                .execute(
                    "SELECT commit_id FROM lix_log() WHERE is_checkpoint ORDER BY position LIMIT 1",
                    &[],
                )
                .await;
            if rows.first().and_then(|row| row.first()) == Some(&Value::Text(full.clone())) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("upload preserves local checkpoint identity");
    // A second reconnect must not duplicate accepted checkpoints.
    replica.close().await.unwrap();
    let replica = open_replica(directory.path(), &url).await;
    assert_eq!(
        replica
            .execute("SELECT commit_id AS id FROM lix_log() WHERE is_checkpoint ORDER BY position LIMIT 1", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("id")
            .unwrap(),
        full
    );
    replica.close().await.unwrap();
    stop_server(server_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn selected_all_checkpoint_from_uncheckpointed_authority_survives_reconnect() {
    scoped_checkpoint_from_uncheckpointed_authority_survives_reconnect(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn selected_subset_checkpoint_from_uncheckpointed_authority_survives_reconnect() {
    scoped_checkpoint_from_uncheckpointed_authority_survives_reconnect(true).await;
}

async fn scoped_checkpoint_from_uncheckpointed_authority_survives_reconnect(
    leave_unselected_work: bool,
) {
    let (storage, authority) = open_authority().await;
    authority
        .execute(
            "INSERT INTO lix_directory (path) VALUES ('/a'), ('/b')",
            &[],
        )
        .await
        .unwrap();
    authority
        .execute("INSERT INTO lix_file (path) VALUES ('/a/note.txt')", &[])
        .await
        .unwrap();
    // Unlike a checkpointed seed, this reproduces a client checkpoint that
    // compacts already accepted working commits out of its canonical ancestry.
    let coordinates = authority
        .execute(
            "SELECT commit_id AS head, working_base_commit_id AS checkpoint FROM lix_branch WHERE id = lix_active_branch_id()",
            &[],
        )
        .await
        .unwrap();
    assert_ne!(
        coordinates.rows()[0].get::<String>("head").unwrap(),
        coordinates.rows()[0].get::<String>("checkpoint").unwrap(),
    );
    authority.close().await.unwrap();
    let probe = Arc::new(HttpProbe::default());
    let (url, server_task, remote) =
        serve_with_authority_session(storage, Arc::clone(&probe)).await;
    let directory = TempDir::new().unwrap();
    let replica = open_replica(directory.path(), &url).await;
    replica
        .execute(
            "SELECT content FROM lix_file WHERE path = '/a/note.txt'",
            &[],
        )
        .await
        .expect("prefetch file content through ordinary SQL");
    for sql in [
        "SELECT * FROM lix_directory",
        "SELECT * FROM lix_file",
        "SELECT * FROM lix_key_value",
        "SELECT row_ref, from_path, to_path FROM lix_diff('lix_file')",
        "SELECT * FROM lix_diff('lix_key_value')",
        "SELECT row_ref FROM lix_history('lix_file')",
        "SELECT row_ref FROM lix_history('lix_directory')",
        "SELECT row_ref FROM lix_history('lix_key_value')",
    ] {
        replica
            .execute(sql, &[])
            .await
            .expect("hydrate fixture move and checkpoint scopes");
    }
    probe.set_offline(true);
    replica
        .execute(
            "UPDATE lix_directory SET path = '/b/a' WHERE path = '/a'",
            &[],
        )
        .await
        .unwrap();
    if leave_unselected_work {
        put_value(&replica, "unselected", "keep working").await;
    }
    let checkpoint = replica
        .execute(
            "SELECT commit_id FROM lix_create_checkpoint(ARRAY(SELECT row_ref FROM lix_diff('lix_file') WHERE to_path = '/b/a/note.txt'))",
            &[],
        )
        .await
        .unwrap()
        .rows()[0]
        .get::<String>("commit_id")
        .unwrap();
    let local_head = replica
        .execute("SELECT lix_active_branch_commit_id() AS id", &[])
        .await
        .unwrap()
        .rows()[0]
        .get::<String>("id")
        .unwrap();
    assert_eq!(
        local_head != checkpoint,
        leave_unselected_work,
        "unselected rows require a working head beyond the partial checkpoint",
    );
    replica.close().await.unwrap();
    let replica = open_replica(directory.path(), &url).await;
    assert_eq!(
        replica
            .execute("SELECT commit_id AS id FROM lix_log() WHERE is_checkpoint ORDER BY position LIMIT 1", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("id")
            .unwrap(),
        checkpoint,
    );
    assert_eq!(
        replica
            .execute("SELECT count(*) AS count FROM lix_diff('lix_file')", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<i64>("count")
            .unwrap(),
        0,
    );
    probe.set_offline(false);
    tokio::time::timeout(WAIT_TIMEOUT, async {
        loop {
            let rows = remote
                .execute(
                    "SELECT commit_id FROM lix_log() WHERE is_checkpoint ORDER BY position LIMIT 1",
                    &[],
                )
                .await;
            if rows.first().and_then(|row| row.first()) == Some(&Value::Text(checkpoint.clone())) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a scoped checkpoint must upload without a false divergence reset");
    assert_eq!(
        remote
            .execute("SELECT path FROM lix_file ORDER BY path", &[])
            .await,
        vec![
            vec![Value::Text("/.lix/README.md".to_owned())],
            vec![Value::Text("/b/a/note.txt".to_owned())],
        ],
    );
    let checkpoint_params = [Value::Text(checkpoint.clone())];
    assert_eq!(
        remote
            .execute(
                "SELECT path FROM lix_as_of('lix_file', $1) ORDER BY path",
                &checkpoint_params
            )
            .await,
        vec![
            vec![Value::Text("/.lix/README.md".to_owned())],
            vec![Value::Text("/b/a/note.txt".to_owned())],
        ],
    );
    if leave_unselected_work {
        remote.wait_for_value("unselected", "keep working").await;
        assert!(
            remote
                .execute(
                    "SELECT key FROM lix_as_of('lix_key_value', $1) WHERE key = 'unselected'",
                    &checkpoint_params,
                )
                .await
                .is_empty(),
            "upload must not promote unselected working rows into the checkpoint",
        );
    }
    replica.close().await.unwrap();
    stop_server(server_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conflicting_remote_edits_converge_and_preserve_pending_rows_across_reopen() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let (storage, authority) = open_authority().await;
    put_value(&authority, "shared", "base").await;
    authority.close().await.unwrap();
    let probe = Arc::new(HttpProbe::default());
    let (url, server_task, remote) =
        serve_with_authority_session(storage, Arc::clone(&probe)).await;
    let directory = TempDir::new().unwrap();
    let replica = open_replica(directory.path(), &url).await;
    replica
        .execute("SELECT value FROM lix_key_value WHERE key = 'shared'", &[])
        .await
        .expect("prefetch existing value");
    assert_eq!(read_value(&replica, "dependent").await, None);
    probe.set_offline(true);
    put_value(&replica, "shared", "pending").await;
    put_value(&replica, "dependent", "pending").await;
    assert_eq!(
        read_value(&replica, "shared").await.as_deref(),
        Some("pending")
    );
    // Close first so an already held long poll cannot publish the server
    // update before the intended durable-pending reconnect scenario.
    replica.close().await.unwrap();
    remote.put_value("shared", "server").await;
    probe.set_offline(false);
    let replica = open_replica(directory.path(), &url).await;
    tokio::time::timeout(WAIT_TIMEOUT, async {
        loop {
            if remote.read_value("shared").await.as_deref() == Some("pending")
                && remote.read_value("dependent").await.as_deref() == Some("pending")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("later accepted incoming rows must converge after reconnect");
    assert_eq!(probe.merge_conflicts.load(Ordering::Acquire), 0);
    assert_eq!(
        read_value(&replica, "shared").await.as_deref(),
        Some("pending")
    );
    assert_eq!(
        read_value(&replica, "dependent").await.as_deref(),
        Some("pending")
    );
    assert_eq!(
        remote.read_value("shared").await.as_deref(),
        Some("pending")
    );
    assert_eq!(
        remote.read_value("dependent").await.as_deref(),
        Some("pending")
    );
    replica.close().await.unwrap();
    probe.set_offline(true);
    let replica = open_replica(directory.path(), &url).await;
    assert_eq!(
        read_value(&replica, "shared").await.as_deref(),
        Some("pending")
    );
    assert_eq!(
        read_value(&replica, "dependent").await.as_deref(),
        Some("pending")
    );
    replica.close().await.unwrap();
    stop_server(server_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fetched_immutable_history_is_cached_across_offline_reopen() {
    let (storage, authority) = open_authority().await;
    put_value(&authority, "history-marker", "historical").await;
    let checkpoint = authority
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .unwrap()
        .rows()[0]
        .get::<String>("commit_id")
        .unwrap();
    put_value(&authority, "history-marker", "current").await;
    authority
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .unwrap();
    authority.close().await.unwrap();
    let probe = Arc::new(HttpProbe::default());
    let (url, server_task) = serve(storage, Arc::clone(&probe)).await;
    let directory = TempDir::new().unwrap();
    let replica = open_replica(directory.path(), &url).await;
    let sql = "SELECT value FROM lix_as_of('lix_key_value', $1) WHERE key = 'history-marker'";
    let params = [Value::Text(checkpoint)];
    let historical = replica
        .execute(sql, &params)
        .await
        .expect("fetch historical state");
    assert_eq!(historical.rows().len(), 1);
    let value = historical.rows()[0].get::<Value>("value").unwrap();
    let historical_text = match &value {
        Value::Text(value) => Some(value.clone()),
        Value::Jsonb(value) => value.as_json_string(),
        _ => None,
    };
    assert_eq!(historical_text.as_deref(), Some("historical"));
    assert_eq!(
        read_value(&replica, "history-marker").await.as_deref(),
        Some("current")
    );
    let history_gets = probe.history_gets.load(Ordering::Acquire);
    probe.set_offline(true);
    assert_eq!(
        replica
            .execute(sql, &params)
            .await
            .expect("cached history while offline")
            .rows()[0]
            .get::<Value>("value")
            .unwrap(),
        value
    );
    replica.close().await.unwrap();
    let replica = open_replica(directory.path(), &url).await;
    assert_eq!(
        replica
            .execute(sql, &params)
            .await
            .expect("persisted history while offline")
            .rows()[0]
            .get::<Value>("value")
            .unwrap(),
        value
    );
    assert_eq!(
        read_value(&replica, "history-marker").await.as_deref(),
        Some("current")
    );
    assert_eq!(probe.history_gets.load(Ordering::Acquire), history_gets);
    replica.close().await.unwrap();
    stop_server(server_task).await;
}

/// Baseline diagnostic, not an assertion that opening is already size independent.
/// Run alone; allocations and network counters include the background worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual partial replica opening baseline"]
async fn partial_replica_open_profile() {
    let mut records = Vec::new();
    // Independently vary one dimension. Content is deterministic and distinct
    // by block to avoid measuring a highly deduplicated all-zero fixture.
    for (label, rows, branches, history, content_bytes) in [
        ("base", 16usize, 0usize, 2usize, 1024usize),
        ("rows", 1600, 0, 2, 1024),
        ("branches", 16, 16, 2, 1024),
        ("history", 16, 0, 200, 1024),
        ("content", 16, 0, 2, 1024 * 1024),
    ] {
        let (storage, authority) = open_authority().await;
        seed_hot_profile_rows(&authority, rows).await;
        put_value(&authority, "partial-profile-marker", "before").await;
        let payload = (0..content_bytes)
            .map(|index| {
                let mut value = (index as u64).wrapping_add(0x9e3779b97f4a7c15);
                value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
                value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
                (value ^ (value >> 31)) as u8
            })
            .collect::<Vec<_>>();
        authority
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ('/unopened-payload.bin', $1)",
                &[Value::Blob(payload.into())],
            )
            .await
            .unwrap();
        for index in 0..history {
            put_value(&authority, "unopened-history", &index.to_string()).await;
        }
        authority
            .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
            .await
            .unwrap();
        for index in 0..branches {
            authority
                .create_branch(CreateBranchOptions {
                    id: None,
                    name: format!("unopened-{index}"),
                    from_commit_id: None,
                })
                .await
                .unwrap();
        }
        authority.close().await.unwrap();
        let probe = Arc::new(HttpProbe::default());
        let (url, task) = serve(storage, Arc::clone(&probe)).await;
        let directory = TempDir::new().unwrap();
        let scope = AllocationScope::start();
        let started = Instant::now();
        let replica = open_replica(directory.path(), &url).await;
        let elapsed_ns = duration_nanos(started.elapsed());
        let allocation = scope.finish();
        let opening = json!({"elapsed_ns": elapsed_ns,
            "attempted_requests": probe.attempted_requests.load(Ordering::Relaxed),
            "response_body_bytes": probe.response_body_bytes.load(Ordering::Relaxed),
            "snapshot_row_pulls": probe.snapshot_row_pulls.load(Ordering::Relaxed),
            "chunk_gets": probe.chunk_gets.load(Ordering::Relaxed),
            "allocated_bytes": allocation.allocated_bytes,
            "peak_live_bytes": allocation.peak_live_bytes_delta});
        let mut operations = Vec::new();
        for (operation, sql) in [
            (
                "first_point_read",
                "SELECT value FROM lix_key_value WHERE key = 'partial-profile-marker'",
            ),
            (
                "warm_point_read",
                "SELECT value FROM lix_key_value WHERE key = 'partial-profile-marker'",
            ),
            (
                "warm_point_write",
                "UPDATE lix_key_value SET value = 'after' WHERE key = 'partial-profile-marker'",
            ),
            (
                "read_own_write",
                "SELECT value FROM lix_key_value WHERE key = 'partial-profile-marker'",
            ),
        ] {
            let before = probe.attempted_requests.load(Ordering::Relaxed);
            let bytes_before = probe.response_body_bytes.load(Ordering::Relaxed);
            let started = Instant::now();
            let result = replica.execute(sql, &[]).await.unwrap();
            if operation == "read_own_write" {
                let value = result.rows()[0].get::<Value>("value").unwrap();
                let value = match value {
                    Value::Jsonb(value) => value.as_json_string(),
                    Value::Text(value) => Some(value),
                    _ => None,
                };
                assert_eq!(value.as_deref(), Some("after"));
            }
            operations.push(json!({"operation": operation, "elapsed_ns": duration_nanos(started.elapsed()),
                "attempted_requests_including_background": probe.attempted_requests.load(Ordering::Relaxed) - before,
                "response_body_bytes_including_background": probe.response_body_bytes.load(Ordering::Relaxed) - bytes_before}));
        }
        records.push(json!({"case": label, "dimensions": {"file_rows": rows, "additional_branches": branches, "history_updates": history, "unopened_content_bytes": content_bytes}, "open": opening, "operations": operations}));
        replica.close().await.unwrap();
        stop_server(task).await;
        println!(
            "LIX_PARTIAL_REPLICA_PROFILE_CASE={}",
            records.last().unwrap()
        );
    }
    let artifact = json!({"schema": "lix.partial-replica-open-profile.v1", "samples_per_case": 1,
        "limits": "Native filesystem replica with in-process memory authority; counters include background work, bytes are emitted HTTP body bytes excluding headers/offline errors; latency diagnostic, no browser result or zero-RTT attribution claimed; schemas not varied.", "records": records});
    println!("LIX_PARTIAL_REPLICA_PROFILE_JSON={artifact}");
    if let Ok(path) = std::env::var("LIX_PARTIAL_REPLICA_PROFILE_OUTPUT") {
        std::fs::write(path, serde_json::to_vec_pretty(&artifact).unwrap()).unwrap();
    }
}

/// Run alone: allocations include the in-process authority and sync worker.
/// Latency is diagnostic; the offline regression above gates zero network
/// dependency without fragile wall-clock limits on shared CI hosts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual local-first foreground RTT and width scorecard"]
async fn local_first_foreground_profile_scorecard() {
    let mut records = Vec::new();
    for width in [32usize, 256] {
        for rtt_ms in [0u64, 100] {
            let (storage, authority) = open_authority().await;
            seed_hot_profile_rows(&authority, width).await;
            authority
                .execute(
                    "INSERT INTO lix_directory (path) VALUES ('/a'), ('/b')",
                    &[],
                )
                .await
                .unwrap();
            put_value(&authority, "profile-marker", "before").await;
            authority
                .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
                .await
                .unwrap();
            authority.close().await.unwrap();
            let probe = Arc::new(HttpProbe::default());
            let (url, task) = serve(storage, Arc::clone(&probe)).await;
            let directory = TempDir::new().unwrap();
            let replica = open_replica(directory.path(), &url).await;
            probe.set_round_trip_delay(Duration::from_millis(rtt_ms));
            for (operation, sql) in [
                (
                    "current_read",
                    "SELECT value FROM lix_key_value WHERE key = 'profile-marker'",
                ),
                (
                    "current_write",
                    "UPDATE lix_key_value SET value = 'after' WHERE key = 'profile-marker'",
                ),
                (
                    "partial_checkpoint",
                    "SELECT commit_id FROM lix_create_checkpoint(ARRAY[lix_row_ref('lix_key_value', 'profile-marker')])",
                ),
                (
                    "folder_move",
                    "UPDATE lix_directory SET path = '/b/a' WHERE path = '/a'",
                ),
                (
                    "full_checkpoint",
                    "SELECT commit_id FROM lix_create_checkpoint()",
                ),
            ] {
                let scope = AllocationScope::start();
                let start = Instant::now();
                replica
                    .execute(sql, &[])
                    .await
                    .expect("profile foreground operation");
                let elapsed = start.elapsed();
                let allocation = scope.finish();
                records.push(json!({"live_rows": width, "rtt_ms": rtt_ms,
                    "operation": operation, "elapsed_ns": duration_nanos(elapsed),
                    "allocated_bytes": allocation.allocated_bytes,
                    "peak_live_bytes": allocation.peak_live_bytes_delta}));
            }
            probe.set_round_trip_delay(Duration::ZERO);
            replica.close().await.unwrap();
            stop_server(task).await;
        }
    }
    let artifact = json!({"schema": "lix.local-first-foreground-profile.v1", "records": records});
    println!("LIX_LOCAL_FIRST_PROFILE_JSON={artifact}");
    if let Ok(path) = std::env::var("LIX_LOCAL_FIRST_PROFILE_OUTPUT") {
        std::fs::write(path, serde_json::to_vec_pretty(&artifact).unwrap()).unwrap();
    }
}

async fn open_replica(path: &Path, url: &str) -> Lix<FilesystemStorage> {
    open_lix()
        .with_storage(
            FilesystemStorage::new(path)
                .open()
                .expect("open filesystem storage"),
        )
        .with_server(ServerOptions::new(url))
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
                    .header(SERVER_PROTOCOL_VERSION_HEADER, PROTOCOL_VERSION)
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
                    .header(SERVER_PROTOCOL_VERSION_HEADER, PROTOCOL_VERSION)
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
                    .header(SERVER_PROTOCOL_VERSION_HEADER, PROTOCOL_VERSION)
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
                    .header(SERVER_PROTOCOL_VERSION_HEADER, PROTOCOL_VERSION)
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
) -> Result<Response<ServerProtocolBody>, Infallible>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let (parts, body) = request.into_parts();
    probe.attempted_requests.fetch_add(1, Ordering::Relaxed);
    if probe.reject_requests.load(Ordering::Acquire) {
        return Ok(Response::builder()
            .status(503)
            .header(CONTENT_TYPE, "application/json")
            .body(ServerProtocolBody::full(Bytes::from_static(
                br#"{"error":{"code":"LIX_SYNC_TEST_OFFLINE","message":"test server offline"}}"#,
            )))
            .expect("build offline response"));
    }
    let one_way_delay = Duration::from_millis(probe.one_way_delay_millis.load(Ordering::Acquire));
    tokio::time::sleep(one_way_delay).await;
    let path = parts.uri.path();
    let is_partial_merge = parts.method == Method::POST && path.ends_with("/sync/merge");
    let is_handshake = parts.method == Method::GET
        && path
            .strip_prefix("/lix/v1/")
            .is_some_and(|lix_id| !lix_id.is_empty() && !lix_id.contains('/'));
    if is_handshake {
        probe.handshakes.fetch_add(1, Ordering::Release);
        if probe.mismatch_handshake_protocol.load(Ordering::Acquire) {
            let body = serde_json::to_vec(&json!({
                "protocolVersion": PROTOCOL_VERSION,
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
                .body(ServerProtocolBody::full(Bytes::from(body)))
                .expect("build mismatched handshake"));
        }
    }
    if parts.method == Method::GET && path.ends_with("/sync/descriptor") {
        probe.descriptor_pulls.fetch_add(1, Ordering::Release);
        if parts
            .uri
            .query()
            .is_some_and(|query| query.split('&').any(|part| part.starts_with("after=")))
        {
            probe.descriptor_waits.fetch_add(1, Ordering::Release);
        }
    }
    if parts.method == Method::POST
        && (path.ends_with("/sync/native-objects") || path.ends_with("/sync/native-object-range"))
    {
        probe.native_object_reads.fetch_add(1, Ordering::Release);
    }
    if parts.method == Method::POST && path.ends_with("/sync/native-metadata") {
        probe.native_metadata_reads.fetch_add(1, Ordering::Release);
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
    let response = protocol
        .handle(
            Request::from_parts(parts, ServerProtocolBody::full(body)),
            ServerProtocolContext {
                principal,
                durable_terminal_storage_notifier: None,
            },
        )
        .await;
    let (parts, body) = response.into_parts();
    let body = if is_partial_merge && parts.status == StatusCode::CONFLICT {
        let bytes = body
            .collect()
            .await
            .expect("collect bounded merge error")
            .to_bytes();
        if serde_json::from_slice::<JsonValue>(&bytes)
            .ok()
            .and_then(|value| value.get("error")?.get("code")?.as_str().map(str::to_owned))
            .as_deref()
            == Some("LIX_PARTIAL_MERGE_CONFLICT")
        {
            probe.merge_conflicts.fetch_add(1, Ordering::Release);
        }
        ServerProtocolBody::full(bytes)
    } else {
        body
    };
    // Observations are streaming responses. Collecting them here would wait
    // forever before sending headers and conceal authority-backed observers.
    tokio::time::sleep(one_way_delay).await;
    // Count emitted data frames without collecting potentially unbounded SSE.
    let counted = body.into_data_stream().map(move |chunk| {
        if let Ok(bytes) = &chunk {
            probe
                .response_body_bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
        chunk
    });
    Ok(Response::from_parts(
        parts,
        ServerProtocolBody::stream(counted),
    ))
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
async fn fresh_replica_reads_point_in_time_filesystem_state() {
    // Historical file and directory reads must resolve complete trees on the
    // authority, including when the replica has only a bounded HOT bootstrap.
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
            "SELECT name, directory_id FROM lix_as_of('lix_file', $1)",
            &[Value::Text(commit_id.clone())],
        )
        .await
        .expect("point-in-time file state executes on the authority");
    let file_names = files
        .rows()
        .iter()
        .map(|row| row.get::<String>("name").expect("file name"))
        .collect::<Vec<_>>();
    assert_eq!(file_names.len(), 3);
    assert_eq!(
        file_names
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
        ["README.md", "playbook.md", "inside.md"]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        "first checkpoint holds both test files and the bootstrap README"
    );

    let directories = replica
        .execute(
            "SELECT id, name FROM lix_as_of('lix_directory', $1)",
            &[Value::Text(commit_id.clone())],
        )
        .await
        .expect("point-in-time directory state executes on the authority");
    let directory_names = directories
        .rows()
        .iter()
        .map(|row| row.get::<String>("name").expect("directory name"))
        .collect::<Vec<_>>();
    assert_eq!(directory_names.len(), 6);
    assert_eq!(
        directory_names
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
        [".lix", "app_data", "plugins", "sales", "docs", "handbook"]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        "first checkpoint holds the test and bootstrap directories"
    );
    assert_files_resolve_directories(&files, &directories);

    // The product's checkpoint-open read: a root diff with resolved paths.
    let diff = replica
        .execute(
            "SELECT to_path FROM lix_diff('lix_file', lix_root_commit_id(), $1) ORDER BY to_path",
            &[Value::Text(commit_id)],
        )
        .await
        .expect("root diff with paths executes on the authority");
    let paths = diff
        .rows()
        .iter()
        .map(|row| row.get::<String>("to_path").expect("path"))
        .collect::<Vec<_>>();
    assert_eq!(
        paths,
        vec![
            "/.lix/README.md".to_string(),
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
        .expect("checkpoint-span diff executes on the authority");
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
            "SELECT name FROM lix_as_of('lix_directory', $1) ORDER BY name",
            &[Value::Text(second_checkpoint.clone())],
        )
        .await
        .expect("latest checkpoint directory state executes on the authority");
    let latest_names = latest_directories
        .rows()
        .iter()
        .map(|row| row.get::<String>("name").expect("directory name"))
        .collect::<Vec<_>>();
    assert_eq!(
        latest_names,
        vec![
            ".lix".to_string(),
            "app_data".to_string(),
            "brand".to_string(),
            "docs".to_string(),
            "handbook".to_string(),
            "plugins".to_string(),
            "sales".to_string(),
        ],
        "latest checkpoint holds every directory"
    );

    replica.close().await.expect("close replica");
    stop_server(server_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn migrated_partial_checkpoint_repository_reads_state_on_a_sparse_replica() {
    // Full lineage of the failing live repository: authored on the v71
    // engine, migrated and partial-checkpointed on the v72 engine
    // (fixture generated from 4816fdba5, SHA-256
    // 634eefb12a96bbb656214d5f203fb2f0dbd0fc552379754e3c86eb9cb99b6f70),
    // migrated to the current format here, served, and read from a fresh
    // sync replica using server-first history reads.
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
            "SELECT id AS commit_id FROM lix_commit WHERE is_checkpoint ORDER BY created_at ASC",
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

    // Read the historical anchor through the connected authority handle.
    replica
        .execute(
            "SELECT path FROM lix_as_of('lix_file', $1) WHERE id = $2",
            &[
                Value::Text(last_checkpoint.clone()),
                Value::Text(brand_file_id.clone()),
            ],
        )
        .await
        .expect("bounded anchor lookup executes on the authority");

    let files = replica
        .execute(
            "SELECT name, directory_id FROM lix_as_of('lix_file', $1)",
            &[Value::Text(last_checkpoint.clone())],
        )
        .await
        .expect("file state at the migrated partial checkpoint executes on the authority");
    let directories = replica
        .execute(
            "SELECT id, name FROM lix_as_of('lix_directory', $1)",
            &[Value::Text(last_checkpoint.clone())],
        )
        .await
        .expect("directory state at the migrated partial checkpoint executes on the authority");
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
        .expect(
            "root diff with paths at the migrated partial checkpoint executes on the authority",
        );
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn existing_branch_admission_preserves_pending_work_and_restores_archived_reads() {
    let (storage, authority) = open_authority().await;
    let main = authority.active_branch_id().await.unwrap();
    put_value(&authority, "branch-marker", "main").await;
    let ancestor = active_head(&authority).await;
    authority.close().await.unwrap();
    let probe = Arc::new(HttpProbe::default());
    let (url, server, remote) = serve_with_authority_session(storage, probe.clone()).await;
    let target = "01920000-0000-7000-8000-000000009061";
    remote
        .create_branch(target, "admission-target", &ancestor)
        .await;
    remote.switch_branch(target).await;
    remote.put_value("branch-marker", "target").await;

    remote.switch_branch(&main).await;
    let directory = TempDir::new().unwrap();
    let replica = open_replica(directory.path(), &url).await;
    assert_eq!(
        read_value(&replica, "branch-marker").await.as_deref(),
        Some("main")
    );
    let independent = replica.open_another_session().await.unwrap();
    replica
        .switch_branch(SwitchBranchOptions {
            branch_id: target.into(),
        })
        .await
        .unwrap();
    assert_eq!(independent.active_branch_id().await.unwrap(), main);
    let unavailable = independent
        .execute(
            "SELECT value FROM lix_key_value WHERE key='branch-marker'",
            &[],
        )
        .await
        .unwrap_err();
    assert_eq!(unavailable.code, "LIX_PARTIAL_SCOPE_PREPARATION_REQUIRED");
    independent
        .switch_branch(SwitchBranchOptions {
            branch_id: target.into(),
        })
        .await
        .unwrap();
    assert_eq!(
        read_value(&independent, "branch-marker").await.as_deref(),
        Some("target")
    );
    independent.close().await.unwrap();
    assert_eq!(replica.active_branch_id().await.unwrap(), target);
    assert_eq!(
        read_value(&replica, "branch-marker").await.as_deref(),
        Some("target")
    );
    probe.set_offline(true);
    replica
        .execute(
            "UPDATE lix_key_value SET value='pending-target' WHERE key='branch-marker'",
            &[],
        )
        .await
        .expect("update fetched row offline after branch admission");

    let error = replica
        .switch_branch(SwitchBranchOptions {
            branch_id: main.clone(),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, "LIX_PARTIAL_BRANCH_SWITCH_PENDING", "{error:?}");
    assert_eq!(replica.active_branch_id().await.unwrap(), target);
    assert_eq!(
        read_value(&replica, "branch-marker").await.as_deref(),
        Some("pending-target")
    );
    replica.close().await.unwrap();
    let replica = open_replica(directory.path(), &url).await;
    assert_eq!(replica.active_branch_id().await.unwrap(), target);
    assert_eq!(
        read_value(&replica, "branch-marker").await.as_deref(),
        Some("pending-target")
    );
    remote.switch_branch(target).await;
    probe.set_offline(false);
    remote
        .wait_for_value("branch-marker", "pending-target")
        .await;
    tokio::time::timeout(WAIT_TIMEOUT, async {
        loop {
            match replica
                .switch_branch(SwitchBranchOptions {
                    branch_id: main.clone(),
                })
                .await
            {
                Ok(_) => break,
                Err(error)
                    if error.code == "LIX_PARTIAL_BRANCH_SWITCH_PENDING"
                        || error.code == LixError::CODE_TRANSACTION_CONFLICT
                        || error.code == "LIX_PARTIAL_READ_INTEREST_CHANGED" =>
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => panic!("branch switch after exact upload acknowledgement: {error:?}"),
            }
        }
    })
    .await
    .expect("branch switch after selected upload");
    // The old exact recipe survived in the bounded archive and was prepared
    // before publication; this first returning read must already be local.
    probe.set_offline(true);
    assert_eq!(
        read_value(&replica, "branch-marker").await.as_deref(),
        Some("main")
    );
    replica.close().await.unwrap();
    let replica = open_replica(directory.path(), &url).await;
    assert_eq!(replica.active_branch_id().await.unwrap(), main);
    assert_eq!(
        read_value(&replica, "branch-marker").await.as_deref(),
        Some("main")
    );
    replica.close().await.unwrap();
    stop_server(server).await;
}

async fn switch_to_uploaded_branch(replica: &Lix<FilesystemStorage>, target: &str) {
    tokio::time::timeout(WAIT_TIMEOUT, async {
        loop {
            match replica
                .switch_branch(SwitchBranchOptions {
                    branch_id: target.into(),
                })
                .await
            {
                Ok(_) => break,
                Err(error)
                    if error.code == "LIX_PARTIAL_BRANCH_SWITCH_PENDING"
                        || error.code == LixError::CODE_TRANSACTION_CONFLICT
                        || error.code == "LIX_PARTIAL_REPLICA_INTEREST_CHANGED" =>
                {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(error) => panic!("branch admission failed: {error:?}"),
            }
        }
    })
    .await
    .expect("pending upload must unblock branch admission");
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_created_branch_publishes_refs_then_admits_without_losing_main() {
    let (storage, authority) = open_authority().await;
    put_value(&authority, "creation-value", "source").await;
    let main = authority.active_branch_id().await.unwrap();
    authority.close().await.unwrap();
    let probe = Arc::new(HttpProbe::default());
    let (url, server, remote) = serve_with_authority_session(storage, probe.clone()).await;
    let directory = TempDir::new().unwrap();
    let replica = open_replica(directory.path(), &url).await;
    assert_eq!(
        read_value(&replica, "creation-value").await.as_deref(),
        Some("source")
    );
    let created = replica
        .create_branch(CreateBranchOptions {
            id: None,
            name: "partial-created".into(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    tokio::time::timeout(WAIT_TIMEOUT, async {
        loop {
            match replica
                .switch_branch(SwitchBranchOptions {
                    branch_id: created.id.clone(),
                })
                .await
            {
                Ok(_) => break,
                Err(error)
                    if error.code == "LIX_PARTIAL_BRANCH_SWITCH_PENDING"
                        || error.code == "LIX_TRANSACTION_CONFLICT"
                        || error.code == "LIX_PARTIAL_REPLICA_INTEREST_CHANGED" =>
                {
                    assert_eq!(replica.active_branch_id().await.unwrap(), main);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(error) => panic!("create-to-switch admission failed: {error:?}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        read_value(&replica, "creation-value").await.as_deref(),
        Some("source")
    );
    remote.switch_branch(&created.id).await;
    probe.set_offline(true);
    put_value(&replica, "creation-value", "child-offline").await;
    assert_eq!(
        read_value(&replica, "creation-value").await.as_deref(),
        Some("child-offline")
    );
    replica.close().await.unwrap();
    let replica = open_replica(directory.path(), &url).await;
    assert_eq!(replica.active_branch_id().await.unwrap(), created.id);
    assert_eq!(
        read_value(&replica, "creation-value").await.as_deref(),
        Some("child-offline")
    );
    probe.set_offline(false);
    remote
        .wait_for_value("creation-value", "child-offline")
        .await;
    remote.switch_branch(&main).await;
    remote.wait_for_value("creation-value", "source").await;
    replica.close().await.unwrap();
    stop_server(server).await;
}

#[path = "sync_mode/plugin_merge.rs"]
mod plugin_merge;
