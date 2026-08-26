#![recursion_limit = "256"]

use std::convert::Infallible;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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
use lix::{ExecuteBatchStatement, Lix, Memory, ServerOptions, Value, WireValue, open_lix};
use lix_storage_filesystem::FilesystemStorage;
use serde_json::{Value as JsonValue, json};
use tempfile::TempDir;
use tokio::net::TcpListener;

const WAIT_TIMEOUT: Duration = Duration::from_secs(15);
const OFFLINE_COMMIT_COUNT: usize = 513;

#[derive(Debug, Default)]
struct HttpProbe {
    pushes: AtomicU64,
    push_conflicts: AtomicU64,
    delta_pulls: AtomicU64,
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
    one_way_delay_millis: AtomicU64,
    gated_pushes: AtomicU64,
    push_gate: Mutex<Option<Arc<tokio::sync::Barrier>>>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
        .create_checkpoint()
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
            "INSERT INTO lix_create_checkpoint (relation, row_pk) \
             SELECT 'lix_file', lixcol_row_pk \
             FROM lix_diff('lix_file', lix_root_commit_id(), lix_active_branch_commit_id()) \
             WHERE lixcol_row_pk ->> 0 = $1 \
             RETURNING commit_id",
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
                 WHERE lixcol_row_pk ->> 0 = $1",
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
                &[Value::Text(checkpoint.rows()[0].get::<String>("commit_id").unwrap())],
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
        .create_checkpoint()
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
        .create_checkpoint()
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
        .create_checkpoint()
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
        .create_checkpoint()
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
            .create_checkpoint()
            .await
            .expect("create file checkpoint")
            .commit_id;
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
                .create_checkpoint()
                .await
                .expect("create file checkpoint")
                .commit_id,
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
async fn binary_chunks_remain_lazy_until_file_content_is_read() {
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
    assert_eq!(probe.chunk_gets.load(Ordering::Acquire), 0);
    let result = replica
        .execute("SELECT content FROM lix_file WHERE path = '/lazy.bin'", &[])
        .await
        .expect("first content read hydrates demanded chunks and retries");
    assert_eq!(result.rows()[0].get::<Vec<u8>>("content").unwrap(), payload);
    assert!(probe.chunk_gets.load(Ordering::Acquire) > 0);

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
    .expect("timed out waiting for synchronized value");
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
                    .uri("/lix/v1")
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
                    .uri("/lix/v1/execute")
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
    (format!("http://{address}"), task)
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
    let is_push = parts.method == Method::POST && path == "/lix/v1/sync/push";
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
        && path == "/lix/v1/sync/pull"
        && parts
            .uri
            .query()
            .is_some_and(|query| query.split('&').any(|part| part.starts_with("after=")));
    let is_snapshot_row_pull = parts.method == Method::GET
        && path == "/lix/v1/sync/pull"
        && parts.uri.query().is_some_and(|query| {
            query
                .split('&')
                .any(|part| part.starts_with("snapshotBranchId="))
        });
    let is_history_get = parts.method == Method::GET && path == "/lix/v1/sync/history";
    if is_push {
        probe.pushes.fetch_add(1, Ordering::Release);
    }
    if is_delta_pull {
        probe.delta_pulls.fetch_add(1, Ordering::Release);
    }
    if is_snapshot_row_pull {
        probe.snapshot_row_pulls.fetch_add(1, Ordering::Release);
    }
    if parts.method == Method::GET && path == "/lix/v1/sync/blob" {
        probe.blob_gets.fetch_add(1, Ordering::Release);
    }
    if is_history_get {
        probe.history_gets.fetch_add(1, Ordering::Release);
        let active = probe.active_history_gets.fetch_add(1, Ordering::AcqRel) + 1;
        probe
            .max_concurrent_history_gets
            .fetch_max(active, Ordering::AcqRel);
    }
    if parts.method == Method::GET && path == "/lix/v1/sync/chunk" {
        probe.chunk_gets.fetch_add(1, Ordering::Release);
    }
    if parts.method == Method::PUT && path == "/lix/v1/sync/chunk" {
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

async fn stop_server(task: tokio::task::JoinHandle<()>) {
    task.abort();
    let _ = task.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
        .create_checkpoint()
        .await
        .expect("checkpoint seeded filesystem");
    authority
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/brand/logo.md', CAST('three' AS BYTEA))",
            &[],
        )
        .await
        .expect("create /brand/logo.md");
    let second_checkpoint = authority
        .create_checkpoint()
        .await
        .expect("checkpoint second filesystem state");
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

    let commit_id = first_checkpoint.commit_id.clone();
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
            "SELECT lixcol_diff_type, coalesce(to_path, from_path) AS path
             FROM lix_diff('lix_file', $1, $2)
             ORDER BY coalesce(to_path, from_path)",
            &[
                Value::Text(first_checkpoint.commit_id.clone()),
                Value::Text(second_checkpoint.commit_id.clone()),
            ],
        )
        .await
        .expect("checkpoint-span diff hydrates on a fresh replica");
    let span_rows = span
        .rows()
        .iter()
        .map(|row| {
            (
                row.get::<String>("lixcol_diff_type").expect("diff type"),
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
            &[Value::Text(second_checkpoint.commit_id.clone())],
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
