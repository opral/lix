#![recursion_limit = "256"]

use std::convert::Infallible;
use std::io::{Cursor, Write as _};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use http::{Request, Response};
use http_body_util::{BodyExt as _, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use lix::server_protocol::{LixServerProtocol, ServerProtocolBody, ServerProtocolContext};
use lix::storage::Storage;
use lix::sync::{
    SyncAdmission, SyncPullResponse, SyncTransactionPack, SyncTransport, SyncTransportFuture,
};
use lix::{Lix, LixError, Memory, ServerOptions, Value, open_lix};
use lix_storage_filesystem::FilesystemStorage;
use tempfile::TempDir;
use tokio::net::TcpListener;

const ROW_SCHEMA: &str = r#"{
  "$schema": "https://lix.dev/schema-v1.json",
  "key": "sync_mode_row",
  "columns": [
    { "name": "row_id", "type": "text", "nullable": false },
    { "name": "value", "type": "text", "nullable": false }
  ],
  "primary_key": ["row_id"]
}"#;

const SECOND_ROW_SCHEMA: &str = r#"{
  "$schema": "https://lix.dev/schema-v1.json",
  "key": "sync_mode_second_row",
  "columns": [
    { "name": "row_id", "type": "text", "nullable": false },
    { "name": "value", "type": "text", "nullable": false }
  ],
  "primary_key": ["row_id"]
}"#;

// The suite intentionally runs several HTTP servers and background sync
// workers at once. Keep convergence assertions independent of host scheduler
// contention; the implementation's local-read readiness deadline remains a
// separate five-second contract.
const SYNC_TEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Read-only probe used by restart tests to inspect the durable outbox without
/// allowing a second worker to contact the server.
struct PendingStateProbe {
    remote_id: String,
}

impl SyncTransport for PendingStateProbe {
    fn remote_id(&self) -> &str {
        &self.remote_id
    }

    fn admit<'a>(
        &'a self,
        _pack: &'a SyncTransactionPack,
    ) -> SyncTransportFuture<'a, SyncAdmission> {
        Box::pin(async {
            Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "pending state probe must not admit",
            ))
        })
    }

    fn pull<'a>(
        &'a self,
        _branch_id: &'a str,
        _after_cursor: u64,
        _limit: usize,
        _schema_keys: &'a [String],
    ) -> SyncTransportFuture<'a, SyncPullResponse> {
        Box::pin(async {
            Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "pending state probe must not pull",
            ))
        })
    }
}

/// Transport-level counters for the performance contract. They deliberately
/// live in the HTTP harness instead of the public Lix API: a cached read must
/// be measurable without adding a synchronization surface to applications.
#[derive(Debug, Default)]
struct ProtocolMetrics {
    requests: AtomicU64,
    request_bytes: AtomicU64,
    response_bytes: AtomicU64,
    drop_first_admission_response: std::sync::atomic::AtomicBool,
}

impl ProtocolMetrics {
    fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.requests.load(Ordering::Relaxed),
            self.request_bytes.load(Ordering::Relaxed),
            self.response_bytes.load(Ordering::Relaxed),
        )
    }

    fn drop_next_admission_response(&self) {
        self.drop_first_admission_response
            .store(true, Ordering::Relaxed);
    }
}

#[tokio::test]
async fn open_execute_observe_syncs_two_filesystem_replicas_and_server_writes() {
    let server_lix = Arc::new(open_lix().await.expect("open server repository"));
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let (server_url, server_task) = serve_protocol(protocol).await;

    server_lix
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(ROW_SCHEMA.to_owned())],
        )
        .await
        .expect("register synchronized schema");

    let alice_dir = TempDir::new().expect("alice tempdir");
    let bob_dir = TempDir::new().expect("bob tempdir");
    let alice_storage = FilesystemStorage::new(alice_dir.path())
        .open()
        .expect("open alice filesystem storage");
    let bob_storage = FilesystemStorage::new(bob_dir.path())
        .open()
        .expect("open bob filesystem storage");
    let alice = open_lix()
        .with_storage(alice_storage)
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open alice sync replica");
    let bob = open_lix()
        .with_storage(bob_storage)
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open bob sync replica");
    // Explicit transactions must use the same lazy hydration barrier as
    // ordinary execute(). The relation is absent from Bob's fresh local
    // catalog until this read demand is fulfilled.
    let mut bob_transaction = bob
        .begin_transaction()
        .await
        .expect("begin bob lazy-read transaction");
    let initial_rows = bob_transaction
        .execute(
            "SELECT value FROM sync_mode_row WHERE row_id = 'shared'",
            &[],
        )
        .await
        .expect("hydrate and execute inside explicit transaction");
    assert!(initial_rows.rows().is_empty());
    bob_transaction
        .rollback()
        .await
        .expect("rollback read-only transaction");

    let mut bob_observation = bob
        .observe(
            "SELECT value FROM sync_mode_row WHERE row_id = 'shared'",
            &[],
        )
        .expect("observe bob replica");
    bob_observation
        .next()
        .await
        .expect("initial observation event")
        .expect("initial observation remains open");

    alice
        .execute(
            "INSERT INTO sync_mode_row (row_id, value) VALUES ('shared', 'from-alice')",
            &[],
        )
        .await
        .expect("alice local execute");

    let bob_event = tokio::time::timeout(SYNC_TEST_TIMEOUT, bob_observation.next())
        .await
        .expect("bob observes alice before timeout")
        .expect("bob observation event")
        .expect("bob observation remains open");
    assert_eq!(
        bob_event.rows.rows()[0].get::<String>("value").unwrap(),
        "from-alice"
    );

    wait_for_value(&server_lix, "from-alice").await;
    server_lix
        .execute(
            "UPDATE sync_mode_row SET value = 'from-server' WHERE row_id = 'shared'",
            &[],
        )
        .await
        .expect("ordinary server execute");
    wait_for_value(&alice, "from-server").await;
    wait_for_value(&bob, "from-server").await;

    bob_observation.close();
    alice.close().await.expect("close alice");
    bob.close().await.expect("close bob");
    server_lix.close().await.expect("close server");
    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_long_poll_delivery_stays_below_300ms_with_100ms_rtt() {
    let server_lix = Arc::new(open_lix().await.expect("open server repository"));
    server_lix
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(ROW_SCHEMA.to_owned())],
        )
        .await
        .expect("register synchronized schema");
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let (server_url, server_task) =
        serve_protocol_with_response_delay(protocol, Duration::from_millis(100)).await;
    let alice_dir = TempDir::new().expect("delayed alice tempdir");
    let bob_dir = TempDir::new().expect("delayed bob tempdir");
    let alice_storage = FilesystemStorage::new(alice_dir.path())
        .open()
        .expect("open delayed alice filesystem storage");
    let bob_storage = FilesystemStorage::new(bob_dir.path())
        .open()
        .expect("open delayed bob filesystem storage");
    let alice = open_lix()
        .with_storage(alice_storage)
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open delayed alice replica");
    let bob = open_lix()
        .with_storage(bob_storage)
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open delayed bob replica");
    let mut observation = bob
        .observe(
            "SELECT value FROM sync_mode_row WHERE row_id = 'rtt-budget'",
            &[],
        )
        .expect("observe delayed replica");
    observation
        .next()
        .await
        .expect("initial delayed observation")
        .expect("delayed observation remains open");
    alice
        .execute(
            "SELECT value FROM sync_mode_row WHERE row_id = 'rtt-budget'",
            &[],
        )
        .await
        .expect("hydrate delayed alice scope");
    // Let bootstrap and lazy schema hydration finish so the measured path is
    // one steady-state outbox admission plus the already-held long poll.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let started = std::time::Instant::now();
    alice
        .execute(
            "INSERT INTO sync_mode_row (row_id, value) VALUES ('rtt-budget', 'remote')",
            &[],
        )
        .await
        .expect("write delayed alice value");
    let event = tokio::time::timeout(Duration::from_millis(300), observation.next())
        .await
        .expect("remote update must arrive inside the 100 ms RTT budget")
        .expect("remote delivery observation")
        .expect("remote delivery observation remains open");
    assert_eq!(
        event.rows.rows()[0].get::<String>("value").unwrap(),
        "remote"
    );
    let delivery = started.elapsed();
    println!("sync_remote_delivery_100ms_rtt_us={}", delivery.as_micros());
    assert!(
        delivery < Duration::from_millis(300),
        "100 ms RTT remote delivery exceeded 300 ms: {:?}",
        delivery
    );

    observation.close();
    alice.close().await.expect("close delayed alice replica");
    bob.close().await.expect("close delayed bob replica");
    server_lix.close().await.expect("close delayed server");
    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_transaction_writes_replicate_through_the_same_local_api() {
    let server_lix = Arc::new(open_lix().await.expect("open server repository"));
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let (server_url, server_task) = serve_protocol(protocol).await;
    server_lix
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(ROW_SCHEMA.to_owned())],
        )
        .await
        .expect("register synchronized schema");

    let alice_dir = TempDir::new().expect("alice tempdir");
    let bob_dir = TempDir::new().expect("bob tempdir");
    let alice = open_lix()
        .with_storage(
            FilesystemStorage::new(alice_dir.path())
                .open()
                .expect("open alice storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open alice sync replica");
    let bob = open_lix()
        .with_storage(
            FilesystemStorage::new(bob_dir.path())
                .open()
                .expect("open bob storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open bob sync replica");
    let mut transaction = alice
        .begin_transaction()
        .await
        .expect("begin explicit transaction");
    transaction
        .execute(
            "INSERT INTO sync_mode_row (row_id, value) VALUES ('transaction', 'committed')",
            &[],
        )
        .await
        .expect("stage transaction write");
    transaction
        .commit()
        .await
        .expect("commit transaction write");
    wait_for_named_value(&bob, "transaction", "committed").await;

    // A rolled-back transaction must not create a sync event or leak its
    // uncommitted overlay to another replica.
    let mut rolled_back = alice
        .begin_transaction()
        .await
        .expect("begin rollback transaction");
    rolled_back
        .execute(
            "UPDATE sync_mode_row SET value = 'rolled-back' WHERE row_id = 'transaction'",
            &[],
        )
        .await
        .expect("stage rollback write");
    rolled_back.rollback().await.expect("rollback transaction");
    tokio::time::sleep(Duration::from_millis(400)).await;
    let bob_value = bob
        .execute(
            "SELECT value FROM sync_mode_row WHERE row_id = 'transaction'",
            &[],
        )
        .await
        .expect("read transaction row after rollback")
        .rows()[0]
        .get::<String>("value")
        .expect("transaction value");
    assert_eq!(bob_value, "committed");

    alice.close().await.expect("close alice");
    bob.close().await.expect("close bob");
    server_lix.close().await.expect("close server");
    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_sync_admission_response_retries_idempotently_in_background() {
    let server_lix = Arc::new(open_lix().await.expect("open server repository"));
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let metrics = Arc::new(ProtocolMetrics::default());
    let (server_url, server_task) =
        serve_protocol_with_metrics(protocol, Some(Arc::clone(&metrics))).await;
    server_lix
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(ROW_SCHEMA.to_owned())],
        )
        .await
        .expect("register synchronized schema");

    let alice_dir = TempDir::new().expect("alice tempdir");
    let bob_dir = TempDir::new().expect("bob tempdir");
    let alice = open_lix()
        .with_storage(
            FilesystemStorage::new(alice_dir.path())
                .open()
                .expect("open alice storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open alice sync replica");
    let bob = open_lix()
        .with_storage(
            FilesystemStorage::new(bob_dir.path())
                .open()
                .expect("open bob storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open bob sync replica");

    // The server commits the admission before this harness turns its receipt
    // into a retryable transport failure. The durable operation ID must make
    // the worker's next admission return the original receipt rather than
    // publishing a duplicate canonical event.
    metrics.drop_next_admission_response();
    alice
        .execute(
            "INSERT INTO sync_mode_row (row_id, value) VALUES ('lost-ack', 'retried')",
            &[],
        )
        .await
        .expect("commit local write after enabling lost-ack injection");
    wait_for_named_value(&server_lix, "lost-ack", "retried").await;
    wait_for_named_value(&bob, "lost-ack", "retried").await;

    let server_head = active_head(server_lix.as_ref()).await;
    let bob_head = active_head(&bob).await;
    assert_eq!(
        bob_head, server_head,
        "retry must converge to one canonical head"
    );
    let server_change_count = server_lix
        .execute(
            "SELECT COUNT(*) AS count FROM lix_change WHERE schema_key = 'sync_mode_row'",
            &[],
        )
        .await
        .expect("read admitted change count")
        .rows()[0]
        .get::<i64>("count")
        .expect("admitted change count");
    assert_eq!(
        server_change_count, 1,
        "lost ack must not publish a duplicate change"
    );

    alice.close().await.expect("close alice");
    bob.close().await.expect("close bob");
    server_lix.close().await.expect("close server");
    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filesystem_replica_restart_replays_pending_outbox_after_server_restart() {
    let server_dir = TempDir::new().expect("server tempdir");
    let server_storage = FilesystemStorage::new(server_dir.path())
        .open()
        .expect("open server filesystem storage");
    let server_lix = open_lix()
        .with_storage(server_storage)
        .await
        .expect("open filesystem server repository");
    server_lix
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(ROW_SCHEMA.to_owned())],
        )
        .await
        .expect("register synchronized schema");

    // Keep the address stable so the replica can reconnect after both the
    // listener and server storage are reopened below.
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind restartable protocol listener");
    let address = listener
        .local_addr()
        .expect("read restartable protocol listener address");
    let protocol = LixServerProtocol::new(Arc::new(server_lix.clone()));
    let (server_url, server_task) = serve_protocol_with_listener(protocol, listener, None).await;

    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica_storage = FilesystemStorage::new(replica_dir.path())
        .open()
        .expect("open replica filesystem storage");
    let replica = open_lix()
        .with_storage(replica_storage)
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("bootstrap filesystem replica");
    // Materialize the row scope while the server is available. The restart
    // assertion below is then a read-your-writes check against an already
    // cached relation, rather than an unrelated cold-hydration request.
    replica
        .execute(
            "SELECT value FROM sync_mode_row WHERE row_id = 'restart-pending'",
            &[],
        )
        .await
        .expect("hydrate synchronized row scope before disconnect");

    // Stop both authority layers before the local write. The transaction must
    // still commit locally and persist its outbox, even though admission is
    // impossible at this point.
    server_task.abort();
    let _ = server_task.await;
    server_lix.close().await.expect("close server before restart");
    replica
        .execute(
            "INSERT INTO sync_mode_row (row_id, value) VALUES ('restart-pending', 'queued')",
            &[],
        )
        .await
        .expect("commit local write while server is offline");
    let local_value = replica
        .execute(
            "SELECT value FROM sync_mode_row WHERE row_id = 'restart-pending'",
            &[],
        )
        .await
        .expect("read optimistic local write")
        .rows()[0]
        .get::<String>("value")
        .expect("local queued value");
    assert_eq!(local_value, "queued");
    replica.close().await.expect("close replica with pending outbox");

    // Reopen the authoritative RocksDB-backed filesystem storage and protocol
    // listener at the same URL. This models a process restart, not merely a
    // transient HTTP response failure.
    let restarted_server_storage = FilesystemStorage::new(server_dir.path())
        .open()
        .expect("reopen server filesystem storage");
    let restarted_server = open_lix()
        .with_storage(restarted_server_storage)
        .await
        .expect("reopen filesystem server repository");
    let restarted_protocol = LixServerProtocol::new(Arc::new(restarted_server.clone()));
    let restarted_listener = TcpListener::bind(address)
        .await
        .expect("rebind restartable protocol listener");
    let (_restarted_url, restarted_task) =
        serve_protocol_with_listener(restarted_protocol, restarted_listener, None).await;

    let reopened_replica_storage = FilesystemStorage::new(replica_dir.path())
        .open()
        .expect("reopen replica filesystem storage");
    let reopened_replica = open_lix()
        .with_storage(reopened_replica_storage)
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("reopen initialized replica after restart");
    let pending_probe = reopened_replica
        .sync(PendingStateProbe {
            remote_id: server_url.clone(),
        })
        .await
        .expect("open durable pending-state probe");
    assert_eq!(
        pending_probe.pending_operations(),
        1,
        "restart must restore the pending outbox before reconnecting"
    );
    // The pending overlay is restored locally before network convergence.
    wait_for_named_value(&reopened_replica, "restart-pending", "queued").await;
    wait_for_named_value(&restarted_server, "restart-pending", "queued").await;

    reopened_replica
        .close()
        .await
        .expect("close reopened replica");
    restarted_server
        .close()
        .await
        .expect("close restarted server");
    restarted_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lazy_scope_hydration_retains_rows_from_a_shared_canonical_commit() {
    let server_lix = Arc::new(open_lix().await.expect("open server repository"));
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let (server_url, server_task) = serve_protocol(protocol).await;
    for schema in [ROW_SCHEMA, SECOND_ROW_SCHEMA] {
        server_lix
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
                &[Value::Text(schema.to_owned())],
            )
            .await
            .expect("register synchronized schema");
    }

    // Both relation writes belong to one commit. Sync projection is
    // commit-level: once a requested scope intersects a canonical pack, the
    // complete matching commit is retained. A later query for the other
    // relation therefore reads the same local commit without manufacturing a
    // projection child or changing its topology.
    let mut seed = server_lix
        .begin_transaction()
        .await
        .expect("begin shared canonical transaction");
    seed.execute(
        "INSERT INTO sync_mode_row (row_id, value) VALUES ('shared-commit', 'first')",
        &[],
    )
    .await
    .expect("write first relation");
    seed.execute(
        "INSERT INTO sync_mode_second_row (row_id, value) VALUES ('shared-commit', 'second')",
        &[],
    )
    .await
    .expect("write second relation");
    seed.commit()
        .await
        .expect("commit shared canonical transaction");

    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_lix()
        .with_storage(
            FilesystemStorage::new(replica_dir.path())
                .open()
                .expect("open replica storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open sync replica");

    let first = replica
        .execute(
            "SELECT value FROM sync_mode_row WHERE row_id = 'shared-commit'",
            &[],
        )
        .await
        .expect("hydrate first relation");
    assert_eq!(first.rows()[0].get::<String>("value").unwrap(), "first");
    let second = replica
        .execute(
            "SELECT value FROM sync_mode_second_row WHERE row_id = 'shared-commit'",
            &[],
        )
        .await
        .expect("hydrate second relation from shared commit");
    assert_eq!(second.rows()[0].get::<String>("value").unwrap(), "second");

    replica.close().await.expect("close replica");
    server_lix.close().await.expect("close server");
    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lazy_scope_projection_preserves_other_scope_after_later_canonical_event() {
    let server_lix = Arc::new(open_lix().await.expect("open server repository"));
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let (server_url, server_task) = serve_protocol(protocol).await;
    for schema in [ROW_SCHEMA, SECOND_ROW_SCHEMA] {
        server_lix
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
                &[Value::Text(schema.to_owned())],
            )
            .await
            .expect("register synchronized schema");
    }

    let mut seed = server_lix
        .begin_transaction()
        .await
        .expect("begin shared canonical transaction");
    seed.execute(
        "INSERT INTO sync_mode_row (row_id, value) VALUES ('topology-shared', 'a-before')",
        &[],
    )
    .await
    .expect("write first topology scope");
    seed.execute(
        "INSERT INTO sync_mode_second_row (row_id, value) VALUES ('topology-shared', 'b-before')",
        &[],
    )
    .await
    .expect("write second topology scope");
    seed.commit()
        .await
        .expect("commit shared topology transaction");

    let replica = open_lix()
        .with_storage(Memory::new())
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open lazy topology replica");
    // A scope request selects a canonical commit, not individual rows. This
    // keeps the shared transaction atomic and lets later scope reads reuse the
    // same commit identity without a local projection child.
    let first = replica
        .execute(
            "SELECT value FROM sync_mode_row WHERE row_id = 'topology-shared'",
            &[],
        )
        .await
        .expect("hydrate first topology scope");
    assert_eq!(first.rows()[0].get::<String>("value").unwrap(), "a-before");
    let first_projection_head = active_head(&replica).await;
    let second = replica
        .execute(
            "SELECT value FROM sync_mode_second_row WHERE row_id = 'topology-shared'",
            &[],
        )
        .await
        .expect("hydrate second topology scope");
    assert_eq!(second.rows()[0].get::<String>("value").unwrap(), "b-before");
    assert_eq!(
        active_head(&replica).await,
        first_projection_head,
        "a second scope from the same canonical event must not create a local child commit"
    );

    server_lix
        .execute(
            "UPDATE sync_mode_row SET value = 'a-after' WHERE row_id = 'topology-shared'",
            &[],
        )
        .await
        .expect("write later first-scope event");
    wait_for_named_value(&replica, "topology-shared", "a-after").await;
    assert_eq!(
        active_head(&replica).await,
        active_head(&server_lix).await,
        "later canonical scope events must preserve the authoritative head identity"
    );
    let second_after = replica
        .execute(
            "SELECT value FROM sync_mode_second_row WHERE row_id = 'topology-shared'",
            &[],
        )
        .await
        .expect("retain second scope after first-scope event");
    assert_eq!(
        second_after.rows()[0]
            .get::<String>("value")
            .expect("second scope value"),
        "b-before"
    );

    replica.close().await.expect("close topology replica");
    server_lix.close().await.expect("close server");
    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_cached_reads_stay_local_and_report_replication_cost() {
    let server_storage = Memory::new();
    let server_lix = Arc::new(
        open_lix()
            .with_storage(server_storage.clone())
            .await
            .expect("open server repository"),
    );
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let metrics = Arc::new(ProtocolMetrics::default());
    let (server_url, server_task) =
        serve_protocol_with_metrics(protocol, Some(Arc::clone(&metrics))).await;
    server_lix
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(ROW_SCHEMA.to_owned())],
        )
        .await
        .expect("register synchronized schema");

    let replica_storage = Memory::new();
    let replica = open_lix()
        .with_storage(replica_storage.clone())
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open sync replica");
    let cold_metrics_before = metrics.snapshot();
    let cold_requests_before = cold_metrics_before.0;
    let cold_started = std::time::Instant::now();
    server_lix
        .execute(
            "INSERT INTO sync_mode_row (row_id, value) VALUES ('performance', 'warm')",
            &[],
        )
        .await
        .expect("write performance fixture");
    wait_for_named_value(&replica, "performance", "warm").await;
    let cold_hydration = cold_started.elapsed();
    let query_hydration_metrics = metrics.snapshot();
    let query_hydration_requests = query_hydration_metrics
        .0
        .saturating_sub(cold_requests_before);
    // Let the initial handshake/pull settle so the benchmark measures the
    // steady-state read path rather than competing with bootstrap work.
    tokio::time::sleep(Duration::from_millis(1_200)).await;

    // This is the direct hot-path contract: once the relation is materialized,
    // an execute() must not synchronously cause another HTTP request. The
    // background worker is deliberately left running; it remains blocked in
    // one server long poll while this cached read stays entirely local.
    let requests_before_cached_read = metrics.snapshot();
    replica
        .execute(
            "SELECT value FROM sync_mode_row WHERE row_id = 'performance'",
            &[],
        )
        .await
        .expect("cached sync read");
    let requests_after_cached_read = metrics.snapshot();
    assert_eq!(
        requests_after_cached_read.0, requests_before_cached_read.0,
        "cached execute read must not add a network request"
    );

    // Time small batches rather than individual futures.  A single
    // `Instant::now()` around an async execute is dominated by scheduler and
    // timer quantization on some CI hosts, which made a strict 10% gate
    // report a false regression even when the aggregate hot path was within
    // target.  The percentile is still per-read; only the measurement noise
    // is amortized.
    const BENCH_BATCHES: usize = 9;
    const BENCH_READS_PER_BATCH: usize = 128;
    let mut sync_samples = Vec::with_capacity(BENCH_BATCHES);
    for _ in 0..BENCH_BATCHES {
        let started = std::time::Instant::now();
        for _ in 0..BENCH_READS_PER_BATCH {
            replica
                .execute(
                    "SELECT value FROM sync_mode_row WHERE row_id = 'performance'",
                    &[],
                )
                .await
                .expect("repeat cached sync read");
        }
        sync_samples.push(Duration::from_secs_f64(
            started.elapsed().as_secs_f64() / BENCH_READS_PER_BATCH as f64,
        ));
    }
    let sync_p50 = duration_percentile_us(&mut sync_samples, 0.50);
    let sync_p95 = duration_percentile_us(&mut sync_samples, 0.95);
    let query_retained_bytes = replica_storage
        .export_snapshot()
        .expect("export query-derived replica snapshot")
        .len();

    // Measure the comparison strategy without adding a public prefetch API:
    // a history-shaped query is the existing full-scope demand. Its retained
    // snapshot and transfer volume provide the upper bound against the
    // query-derived scope above.
    let prefetch_storage = Memory::new();
    let prefetch = open_lix()
        .with_storage(prefetch_storage.clone())
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open full-scope comparison replica");
    let prefetch_metrics_before = metrics.snapshot();
    let prefetch_started = std::time::Instant::now();
    prefetch
        .execute("SELECT COUNT(*) AS count FROM lix_change", &[])
        .await
        .expect("hydrate full history comparison scope");
    let prefetch_hydration = prefetch_started.elapsed();
    let prefetch_metrics = metrics.snapshot();
    let prefetch_retained_bytes = prefetch_storage
        .export_snapshot()
        .expect("export full-scope replica snapshot")
        .len();
    prefetch.close().await.expect("close full-scope comparison");

    // Measure durable scope retention separately from a fresh full-history
    // prefetch: restart the inferred-scope replica with the server offline.
    replica
        .close()
        .await
        .expect("close query-derived replica before offline reopen");
    server_task.abort();
    let reopen_started = std::time::Instant::now();
    let reopened = open_lix()
        .with_storage(replica_storage.clone())
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("reopen query-derived replica offline");
    let scope_retention_reopen_us = reopen_started.elapsed().as_secs_f64() * 1_000_000.0;
    let retention_read_started = std::time::Instant::now();
    reopened
        .execute(
            "SELECT value FROM sync_mode_row WHERE row_id = 'performance'",
            &[],
        )
        .await
        .expect("read retained scope after offline reopen");
    let scope_retention_read_us = retention_read_started.elapsed().as_secs_f64() * 1_000_000.0;
    let scope_retention_bytes = replica_storage
        .export_snapshot()
        .expect("export retained-scope replica snapshot")
        .len();
    reopened
        .close()
        .await
        .expect("close offline retained-scope replica");

    // Compare against ordinary local Lix over the exact same materialized
    // repository image. This removes history-shape and storage-cache bias
    // from the 10% hot-read gate; the only intended difference is sync-mode
    // scope/barrier bookkeeping.
    let local = open_lix()
        .with_storage(server_storage.clone())
        .await
        .expect("open ordinary local comparison lix");
    local
        .execute(
            "SELECT value FROM sync_mode_row WHERE row_id = 'performance'",
            &[],
        )
        .await
        .expect("warm ordinary local comparison read");
    let mut local_samples = Vec::with_capacity(BENCH_BATCHES);
    for _ in 0..BENCH_BATCHES {
        let started = std::time::Instant::now();
        for _ in 0..BENCH_READS_PER_BATCH {
            local
                .execute(
                    "SELECT value FROM sync_mode_row WHERE row_id = 'performance'",
                    &[],
                )
                .await
                .expect("repeat ordinary local read");
        }
        local_samples.push(Duration::from_secs_f64(
            started.elapsed().as_secs_f64() / BENCH_READS_PER_BATCH as f64,
        ));
    }
    let local_p50 = duration_percentile_us(&mut local_samples, 0.50);
    let local_p95 = duration_percentile_us(&mut local_samples, 0.95);
    let (requests, request_bytes, response_bytes) = metrics.snapshot();
    println!(
        "sync_lazy_performance,sync_p50_us={sync_p50:.3},sync_p95_us={sync_p95:.3},local_p50_us={local_p50:.3},local_p95_us={local_p95:.3},cached_read_ratio_p50={:.3},cold_hydration_p50_us={:.3},cold_hydration_p95_us={:.3},prefetch_hydration_us={:.3},scope_retention_reopen_us={scope_retention_reopen_us:.3},scope_retention_read_us={scope_retention_read_us:.3},replication_lag_us={:.3},query_scope_requests={query_hydration_requests},query_scope_response_bytes={},prefetch_scope_requests={},prefetch_scope_response_bytes={},query_retained_bytes={query_retained_bytes},prefetch_retained_bytes={prefetch_retained_bytes},scope_retention_bytes={scope_retention_bytes},requests={requests},request_bytes={request_bytes},response_bytes={response_bytes}",
        sync_p50 / local_p50.max(f64::EPSILON),
        duration_percentile_us(&mut [cold_hydration], 0.50),
        duration_percentile_us(&mut [cold_hydration], 0.95),
        prefetch_hydration.as_secs_f64() * 1_000_000.0,
        cold_hydration.as_secs_f64() * 1_000_000.0,
        query_hydration_metrics
            .2
            .saturating_sub(cold_metrics_before.2),
        prefetch_metrics.0.saturating_sub(prefetch_metrics_before.0),
        prefetch_metrics.2.saturating_sub(prefetch_metrics_before.2),
    );
    // This is the actual performance gate. Batched timing above makes the
    // comparison stable enough to enforce in optimized and debug builds;
    // network work is still measured separately and the cached-read request
    // count assertion above proves the network is not on this hot path.
    assert!(
        sync_p50 <= local_p50 * 1.10,
        "cached sync p50 {sync_p50:.3}us exceeds local p50 {local_p50:.3}us by more than 10%"
    );

    local.close().await.expect("close local comparison lix");
    server_lix.close().await.expect("close server");
    server_task.abort();
}

/// Manual scale probe for the lazy protocol shape. It deliberately reports
/// measurements instead of asserting latency: CI scheduler and host storage
/// make wall-clock thresholds noisy, while the output is stable enough for
/// comparing protocol changes on the same machine. Run with
/// `--ignored --nocapture` when profiling event/page and branch-catalog costs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual sync scale probe; run with --ignored --nocapture"]
async fn sync_scale_probe_many_events_and_branches() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_test_writer()
        .try_init();
    const EVENT_COUNT: usize = 144;
    const BRANCH_COUNT: usize = 24;

    let server_storage = Memory::new();
    let server_lix = Arc::new(
        open_lix()
            .with_storage(server_storage)
            .await
            .expect("open scale probe server repository"),
    );
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let metrics = Arc::new(ProtocolMetrics::default());
    let (server_url, server_task) =
        serve_protocol_with_metrics(protocol, Some(Arc::clone(&metrics))).await;
    server_lix
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(ROW_SCHEMA.to_owned())],
        )
        .await
        .expect("register scale probe schema");

    // Keep one replica connected while the server appends history. This
    // creates a canonical sync head before direct authoritative writes and
    // exercises the normal long-poll wake path rather than bootstrap-only
    // history construction.
    let warm = open_lix()
        .with_storage(Memory::new())
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open warm scale probe replica");
    for index in 0..BRANCH_COUNT {
        let branch_id = format!("0198a000-0000-7000-8000-{index:012x}");
        server_lix
            .create_branch(lix::CreateBranchOptions {
                id: Some(branch_id),
                name: format!("scale-{index}"),
                from_commit_id: None,
            })
            .await
            .expect("create scale probe branch");
    }
    for index in 0..EVENT_COUNT {
        server_lix
            .execute(
                &format!(
                    "INSERT INTO sync_mode_row (row_id, value) VALUES ('scale-{index}', 'value-{index}')"
                ),
                &[],
            )
            .await
            .expect("append scale probe event");
    }
    let server_count = server_lix
        .execute("SELECT COUNT(*) AS count FROM sync_mode_row", &[])
        .await
        .expect("read scale probe server count")
        .rows()
        .first()
        .and_then(|row| row.get::<i64>("count").ok())
        .expect("scale probe server count result");
    println!("sync_scale_probe_server_rows={server_count}");

    let before = metrics.snapshot();
    let cold_storage = Memory::new();
    let open_started = std::time::Instant::now();
    let cold = open_lix()
        .with_storage(cold_storage.clone())
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open cold scale probe replica");
    let open_us = open_started.elapsed().as_secs_f64() * 1_000_000.0;

    // The first relation query is the lazy demand. A generous correctness
    // timeout prevents an unavailable worker from hanging this ignored probe,
    // but no timing assertion is made on the measured hydration duration.
    let hydration_started = std::time::Instant::now();
    let count_result = tokio::time::timeout(SYNC_TEST_TIMEOUT, async {
        loop {
            let count = cold
                .execute("SELECT COUNT(*) AS count FROM sync_mode_row", &[])
                .await
                .expect("scale probe count query")
                .rows()
                .first()
                .and_then(|row| row.get::<i64>("count").ok())
                .expect("scale probe count result");
            if count as usize == EVENT_COUNT {
                return count;
            }
            // A cold replica can complete the scope marker just before a
            // concurrent worker page publishes its final local projection.
            // Polling the local query keeps this probe measuring eventual
            // convergence, not scheduler timing, without adding a network
            // call to the read path.
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if count_result.is_err() {
        let local_rows = cold
            .execute("SELECT COUNT(*) AS count FROM sync_mode_row", &[])
            .await
            .ok()
            .and_then(|result| result.rows().first().and_then(|row| row.get::<i64>("count").ok()));
        let local_commits = cold
            .execute("SELECT COUNT(*) AS count FROM lix_commit", &[])
            .await
            .ok()
            .and_then(|result| result.rows().first().and_then(|row| row.get::<i64>("count").ok()));
        let metrics_snapshot = metrics.snapshot();
        println!(
            "sync_scale_probe_timeout,local_rows={local_rows:?},local_commits={local_commits:?},requests={},request_bytes={},response_bytes={}",
            metrics_snapshot.0.saturating_sub(before.0),
            metrics_snapshot.1.saturating_sub(before.1),
            metrics_snapshot.2.saturating_sub(before.2),
        );
    }
    let count = count_result.expect("scale probe hydration should converge");
    let hydration_us = hydration_started.elapsed().as_secs_f64() * 1_000_000.0;
    let after = metrics.snapshot();
    let retained_bytes = cold_storage
        .export_snapshot()
        .expect("export scale probe snapshot")
        .len();
    println!(
        "sync_scale_probe,event_count={EVENT_COUNT},branch_count={BRANCH_COUNT},rows={count},open_us={open_us:.3},hydration_us={hydration_us:.3},requests={},request_bytes={},response_bytes={},retained_bytes={retained_bytes}",
        after.0.saturating_sub(before.0),
        after.1.saturating_sub(before.1),
        after.2.saturating_sub(before.2),
    );

    assert_eq!(server_count as usize, EVENT_COUNT);
    assert_eq!(count as usize, EVENT_COUNT);
    cold.close().await.expect("close cold scale probe replica");
    warm.close().await.expect("close warm scale probe replica");
    server_lix.close().await.expect("close scale probe server");
    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_registered_schema_is_durable_for_offline_restart() {
    let server_lix = Arc::new(open_lix().await.expect("open server repository"));
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let (server_url, server_task) = serve_protocol(protocol).await;
    server_lix
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(ROW_SCHEMA.to_owned())],
        )
        .await
        .expect("register synchronized schema");

    let replica_storage = Memory::new();
    let replica = open_lix()
        .with_storage(replica_storage.clone())
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open sync replica");
    server_lix
        .execute(
            "INSERT INTO sync_mode_row (row_id, value) VALUES ('offline-schema', 'durable')",
            &[],
        )
        .await
        .expect("write schema-backed row");
    wait_for_named_value(&replica, "offline-schema", "durable").await;
    replica.close().await.expect("close sync replica");
    server_lix.close().await.expect("close server");
    server_task.abort();

    // Reopen against the now-unreachable URL. The relation catalog and its
    // hydrated scope must come from local storage; a cached read must not
    // wait for or require the network.
    let offline = open_lix()
        .with_storage(replica_storage)
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("reopen sync replica offline");
    let result = offline
        .execute(
            "SELECT value FROM sync_mode_row WHERE row_id = 'offline-schema'",
            &[],
        )
        .await
        .expect("read hydrated schema while offline");
    assert_eq!(result.rows()[0].get::<String>("value").unwrap(), "durable");
    offline.close().await.expect("close offline replica");
}

fn duration_percentile_us(samples: &mut [Duration], percentile: f64) -> f64 {
    samples.sort_unstable();
    let index = ((samples.len().saturating_sub(1) as f64) * percentile).round() as usize;
    samples[index.min(samples.len().saturating_sub(1))].as_secs_f64() * 1_000_000.0
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_created_branch_becomes_visible_on_sync_replicas() {
    let server_lix = Arc::new(open_lix().await.expect("open server repository"));
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let (server_url, server_task) = serve_protocol(protocol).await;
    server_lix
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(ROW_SCHEMA.to_owned())],
        )
        .await
        .expect("register synchronized schema");

    let alice_dir = TempDir::new().expect("alice tempdir");
    let bob_dir = TempDir::new().expect("bob tempdir");
    let alice = open_lix()
        .with_storage(
            FilesystemStorage::new(alice_dir.path())
                .open()
                .expect("open alice storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open alice replica");
    let bob = open_lix()
        .with_storage(
            FilesystemStorage::new(bob_dir.path())
                .open()
                .expect("open bob storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open bob replica");

    // Establish a canonical commit that is present in both local commit
    // graphs. The branch catalog can then use that identity as its source.
    alice
        .execute(
            "INSERT INTO sync_mode_row (row_id, value) VALUES ('branch-seed', 'seed')",
            &[],
        )
        .await
        .expect("seed canonical row");
    wait_for_named_value(&bob, "branch-seed", "seed").await;
    wait_for_named_value(server_lix.as_ref(), "branch-seed", "seed").await;

    let branch_id = "0198a000-0000-7000-8000-0000000000c1";
    let server_branch = server_lix
        .create_branch(lix::CreateBranchOptions {
            id: Some(branch_id.to_owned()),
            name: "server-feature".to_owned(),
            from_commit_id: None,
        })
        .await
        .expect("create branch on server");

    // Opening an independent session may race the background branch-catalog
    // pass. The explicit branch target must lazily demand that catalog rather
    // than failing with a local branch-not-found error.
    let branch_session = alice
        .open_another_session()
        .with_branch(branch_id.to_owned())
        .await
        .expect("open a secondary session on a remote-only branch");
    assert_eq!(
        branch_session
            .active_branch_id()
            .await
            .expect("read secondary branch"),
        branch_id
    );
    branch_session
        .close()
        .await
        .expect("close secondary branch session");

    wait_for_branch(&alice, branch_id).await;
    wait_for_branch(&bob, branch_id).await;
    let server_branch_row = server_lix
        .execute(
            "SELECT name, commit_id FROM lix_branch WHERE id = $1",
            &[Value::Text(branch_id.to_owned())],
        )
        .await
        .expect("read server branch")
        .rows()
        .first()
        .cloned()
        .expect("server branch row");
    for replica in [&alice, &bob] {
        let row = replica
            .execute(
                "SELECT name, commit_id FROM lix_branch WHERE id = $1",
                &[Value::Text(branch_id.to_owned())],
            )
            .await
            .expect("read replicated branch")
            .rows()
            .first()
            .cloned()
            .expect("replicated branch row");
        assert_eq!(
            row.get::<String>("name").unwrap(),
            server_branch_row.get::<String>("name").unwrap()
        );
        assert_eq!(
            row.get::<String>("commit_id").unwrap(),
            server_branch_row.get::<String>("commit_id").unwrap()
        );
    }
    assert_eq!(server_branch.id, branch_id);

    server_lix
        .execute(
            "DELETE FROM lix_branch WHERE id = $1",
            &[Value::Text(branch_id.to_owned())],
        )
        .await
        .expect("delete server branch");
    wait_for_branch_absent(&alice, branch_id).await;
    wait_for_branch_absent(&bob, branch_id).await;

    alice.close().await.expect("close alice");
    bob.close().await.expect("close bob");
    server_lix.close().await.expect("close server");
    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_branch_creation_replicates_through_sync_admission() {
    let server_lix = Arc::new(open_lix().await.expect("open server repository"));
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let (server_url, server_task) = serve_protocol(protocol).await;
    server_lix
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(ROW_SCHEMA.to_owned())],
        )
        .await
        .expect("register synchronized schema");

    let alice_dir = TempDir::new().expect("alice tempdir");
    let bob_dir = TempDir::new().expect("bob tempdir");
    let alice = open_lix()
        .with_storage(
            FilesystemStorage::new(alice_dir.path())
                .open()
                .expect("open alice storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open alice replica");
    let bob = open_lix()
        .with_storage(
            FilesystemStorage::new(bob_dir.path())
                .open()
                .expect("open bob storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open bob replica");

    let branch_id = "0198a000-0000-7000-8000-0000000000d1";
    let created = alice
        .create_branch(lix::CreateBranchOptions {
            id: Some(branch_id.to_owned()),
            name: "alice-feature".to_owned(),
            from_commit_id: None,
        })
        .await
        .expect("create branch on alice");
    assert_eq!(created.id, branch_id);

    wait_for_branch(server_lix.as_ref(), branch_id).await;
    wait_for_branch(&bob, branch_id).await;
    let server_row = server_lix
        .execute(
            "SELECT name, commit_id FROM lix_branch WHERE id = $1",
            &[Value::Text(branch_id.to_owned())],
        )
        .await
        .expect("read server branch")
        .rows()
        .first()
        .cloned()
        .expect("server branch row");
    for replica in [&alice, &bob] {
        let row = replica
            .execute(
                "SELECT name, commit_id FROM lix_branch WHERE id = $1",
                &[Value::Text(branch_id.to_owned())],
            )
            .await
            .expect("read replicated branch")
            .rows()
            .first()
            .cloned()
            .expect("replicated branch row");
        assert_eq!(
            row.get::<String>("name").unwrap(),
            server_row.get::<String>("name").unwrap()
        );
        assert_eq!(
            row.get::<String>("commit_id").unwrap(),
            server_row.get::<String>("commit_id").unwrap()
        );
    }

    // Deleting a branch is an ordinary global-control write from the
    // application's point of view. It should travel through the same local
    // outbox and disappear from every replica's normal branch catalog.
    alice
        .execute(
            "DELETE FROM lix_branch WHERE id = $1",
            &[Value::Text(branch_id.to_owned())],
        )
        .await
        .expect("delete branch on alice");
    wait_for_branch_absent(server_lix.as_ref(), branch_id).await;
    wait_for_branch_absent(&bob, branch_id).await;

    alice.close().await.expect("close alice");
    bob.close().await.expect("close bob");
    server_lix.close().await.expect("close server");
    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn history_diff_and_working_diff_hydrate_canonical_sync_topology() {
    let server_lix = Arc::new(open_lix().await.expect("open server repository"));
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let (server_url, server_task) = serve_protocol(protocol).await;
    server_lix
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(ROW_SCHEMA.to_owned())],
        )
        .await
        .expect("register synchronized schema");
    server_lix
        .execute(
            "INSERT INTO sync_mode_row (row_id, value) VALUES ('history', 'before')",
            &[],
        )
        .await
        .expect("seed history row");
    let seed_head = active_head(server_lix.as_ref()).await;

    let alice_dir = TempDir::new().expect("alice tempdir");
    let bob_dir = TempDir::new().expect("bob tempdir");
    let alice = open_lix()
        .with_storage(
            FilesystemStorage::new(alice_dir.path())
                .open()
                .expect("open alice storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open alice replica");
    let bob = open_lix()
        .with_storage(
            FilesystemStorage::new(bob_dir.path())
                .open()
                .expect("open bob storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open bob replica");

    server_lix
        .execute(
            "UPDATE sync_mode_row SET value = 'after' WHERE row_id = 'history'",
            &[],
        )
        .await
        .expect("write remote history change");
    wait_for_named_value(&bob, "history", "after").await;
    let updated_head = active_head(server_lix.as_ref()).await;

    let diff = bob
        .execute(
            "SELECT diff_type FROM lix_diff($1, $2) WHERE schema_key = 'sync_mode_row'",
            &[
                Value::Text(seed_head.clone()),
                Value::Text(updated_head.clone()),
            ],
        )
        .await
        .expect("sync diff should hydrate canonical history");
    assert_eq!(diff.rows().len(), 1, "one row should differ across heads");

    alice
        .execute(
            "UPDATE sync_mode_row SET value = 'local' WHERE row_id = 'history'",
            &[],
        )
        .await
        .expect("write local optimistic change");
    let working_diff = alice
        .execute(
            "SELECT COUNT(*) AS entries FROM lix_working_diff WHERE schema_key = 'sync_mode_row'",
            &[],
        )
        .await
        .expect("working diff should stay local");
    assert_eq!(
        working_diff.rows()[0].get::<i64>("entries").unwrap(),
        1,
        "local pending edit must appear in working diff"
    );

    alice.close().await.expect("close alice");
    bob.close().await.expect("close bob");
    server_lix.close().await.expect("close server");
    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_commit_topology_and_branch_head_replicate_without_conflict_api() {
    let template_dir = TempDir::new().expect("template tempdir");
    let main_branch_id = {
        let seed = open_lix()
            .with_storage(
                FilesystemStorage::new(template_dir.path())
                    .open()
                    .expect("open template storage"),
            )
            .await
            .expect("open template repository");
        seed.execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(ROW_SCHEMA.to_owned())],
        )
        .await
        .expect("register synchronized schema");
        seed.execute(
            "INSERT INTO sync_mode_row (row_id, value) VALUES ('merge-base', 'base')",
            &[],
        )
        .await
        .expect("seed merge base");
        let main_branch_id = seed.active_branch_id().await.expect("main branch id");
        seed.create_branch(lix::CreateBranchOptions {
            id: Some("0198a000-0000-7000-8000-0000000000e1".to_owned()),
            name: "merge-feature".to_owned(),
            from_commit_id: None,
        })
        .await
        .expect("create merge feature branch");
        seed.close().await.expect("close template repository");
        main_branch_id
    };

    let server_dir = TempDir::new().expect("server tempdir");
    let alice_dir = TempDir::new().expect("alice tempdir");
    let bob_dir = TempDir::new().expect("bob tempdir");
    copy_directory(template_dir.path(), server_dir.path());
    copy_directory(template_dir.path(), alice_dir.path());
    copy_directory(template_dir.path(), bob_dir.path());

    let server_lix = Arc::new(
        open_lix()
            .with_storage(
                FilesystemStorage::new(server_dir.path())
                    .open()
                    .expect("open server storage"),
            )
            .await
            .expect("open server repository"),
    );
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let (server_url, server_task) = serve_protocol(protocol).await;
    let alice = open_lix()
        .with_storage(
            FilesystemStorage::new(alice_dir.path())
                .open()
                .expect("open alice storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open alice replica");
    let bob = open_lix()
        .with_storage(
            FilesystemStorage::new(bob_dir.path())
                .open()
                .expect("open bob storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open bob replica");

    server_lix
        .execute(
            "INSERT INTO sync_mode_row (row_id, value) VALUES ('merge-main', 'main')",
            &[],
        )
        .await
        .expect("create divergent main change");

    let feature_branch_id = "0198a000-0000-7000-8000-0000000000e1";
    server_lix
        .switch_branch(lix::SwitchBranchOptions {
            branch_id: feature_branch_id.to_owned(),
        })
        .await
        .expect("switch server to feature branch");
    server_lix
        .execute(
            "INSERT INTO sync_mode_row (row_id, value) VALUES ('merge-feature', 'feature')",
            &[],
        )
        .await
        .expect("write feature branch change");
    let feature_head = active_head(server_lix.as_ref()).await;
    server_lix
        .switch_branch(lix::SwitchBranchOptions {
            branch_id: main_branch_id.clone(),
        })
        .await
        .expect("switch server to main branch");
    let main_head_before_merge = active_head(server_lix.as_ref()).await;
    let merge = server_lix
        .merge_branch(lix::MergeBranchOptions {
            source_branch_id: feature_branch_id.to_owned(),
        })
        .await
        .expect("merge feature branch into main");
    let server_main_head = active_head(server_lix.as_ref()).await;
    assert_eq!(merge.target_head_after_commit_id, server_main_head);

    wait_for_named_value(&alice, "merge-feature", "feature").await;
    wait_for_named_value(&bob, "merge-feature", "feature").await;
    for replica in [&alice, &bob] {
        let head = active_head(replica).await;
        assert_eq!(
            head, server_main_head,
            "merge head must be canonical on replicas"
        );
        let branch_row = replica
            .execute(
                "SELECT commit_id FROM lix_branch WHERE id = $1",
                &[Value::Text(main_branch_id.clone())],
            )
            .await
            .expect("read replicated main branch")
            .rows()[0]
            .get::<String>("commit_id")
            .unwrap();
        assert_eq!(branch_row, server_main_head);
        let parents = replica
            .execute(
                "SELECT parent_id, parent_order FROM lix_commit_edge \
                 WHERE child_id = $1 ORDER BY parent_order",
                &[Value::Text(server_main_head.clone())],
            )
            .await
            .expect("read replicated merge parents")
            .rows()
            .iter()
            .map(|row| {
                (
                    row.get::<String>("parent_id").unwrap(),
                    row.get::<i64>("parent_order").unwrap(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            parents,
            vec![
                (main_head_before_merge.clone(), 0),
                (feature_head.clone(), 1)
            ],
            "replicas must retain the canonical two-parent merge topology"
        );
    }

    alice.close().await.expect("close alice");
    bob.close().await.expect("close bob");
    server_lix.close().await.expect("close server");
    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn binary_file_bytes_follow_sync_mode_between_filesystem_replicas() {
    let server_lix = Arc::new(open_lix().await.expect("open server repository"));
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let (server_url, server_task) = serve_protocol(protocol).await;
    server_lix
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(ROW_SCHEMA.to_owned())],
        )
        .await
        .expect("register synchronized schema");
    server_lix
        .execute(
            "INSERT INTO sync_mode_row (row_id, value) VALUES ('seed', 'seed')",
            &[],
        )
        .await
        .expect("seed canonical sync event");

    let alice_dir = TempDir::new().expect("alice tempdir");
    let bob_dir = TempDir::new().expect("bob tempdir");
    let alice = open_lix()
        .with_storage(
            FilesystemStorage::new(alice_dir.path())
                .open()
                .expect("open alice filesystem storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open alice sync replica");
    let bob = open_lix()
        .with_storage(
            FilesystemStorage::new(bob_dir.path())
                .open()
                .expect("open bob filesystem storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open bob sync replica");

    let payload = b"\0lix-sync-binary\xff";
    alice
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/binary.bin', $1)",
            &[Value::Blob(payload.to_vec().into())],
        )
        .await
        .expect("write binary file on alice");
    wait_for_binary_file(&bob, payload).await;
    wait_for_binary_file(server_lix.as_ref(), payload).await;

    alice.close().await.expect("close alice");
    bob.close().await.expect("close bob");
    server_lix.close().await.expect("close server");
    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn file_renames_and_deletes_follow_sync_mode() {
    let server_lix = Arc::new(open_lix().await.expect("open server repository"));
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let (server_url, server_task) = serve_protocol(protocol).await;
    server_lix
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(ROW_SCHEMA.to_owned())],
        )
        .await
        .expect("register synchronized schema");
    server_lix
        .execute(
            "INSERT INTO sync_mode_row (row_id, value) VALUES ('seed', 'seed')",
            &[],
        )
        .await
        .expect("seed canonical sync event");
    let alice_dir = TempDir::new().expect("alice tempdir");
    let bob_dir = TempDir::new().expect("bob tempdir");
    let alice = open_lix()
        .with_storage(
            FilesystemStorage::new(alice_dir.path())
                .open()
                .expect("open alice filesystem storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open alice sync replica");
    let bob = open_lix()
        .with_storage(
            FilesystemStorage::new(bob_dir.path())
                .open()
                .expect("open bob filesystem storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open bob sync replica");

    let payload = b"rename-delete";
    alice
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/before.bin', $1)",
            &[Value::Blob(payload.to_vec().into())],
        )
        .await
        .expect("write file before rename");
    wait_for_file_at(&bob, "/before.bin", payload).await;

    alice
        .execute(
            "UPDATE lix_file SET path = '/after.bin' WHERE path = '/before.bin'",
            &[],
        )
        .await
        .expect("rename file");
    wait_for_file_at(&bob, "/after.bin", payload).await;
    wait_for_file_absent(&bob, "/before.bin").await;

    alice
        .execute("DELETE FROM lix_file WHERE path = '/after.bin'", &[])
        .await
        .expect("delete renamed file");
    wait_for_file_absent(&bob, "/after.bin").await;

    alice.close().await.expect("close alice");
    bob.close().await.expect("close bob");
    server_lix.close().await.expect("close server");
    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn initialized_filesystem_replica_reopens_and_accepts_writes_offline() {
    let server_lix = Arc::new(open_lix().await.expect("open server repository"));
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let (server_url, server_task) = serve_protocol(protocol).await;
    server_lix
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(ROW_SCHEMA.to_owned())],
        )
        .await
        .expect("register synchronized schema");

    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_lix()
        .with_storage(
            FilesystemStorage::new(replica_dir.path())
                .open()
                .expect("open replica storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("bootstrap replica online");
    replica
        .execute(
            "INSERT INTO sync_mode_row (row_id, value) VALUES ('offline', 'online')",
            &[],
        )
        .await
        .expect("write before disconnect");
    wait_for_named_value(&server_lix, "offline", "online").await;
    replica.close().await.expect("close online replica");
    drop(replica);

    // A durable replica can also reopen while the server is available. The
    // open still returns from local state; the background worker reconnects
    // and applies a subsequent authoritative write asynchronously.
    let reopen_online_started = std::time::Instant::now();
    let reopened_online = open_lix()
        .with_storage(
            FilesystemStorage::new(replica_dir.path())
                .open()
                .expect("reopen online replica storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("reopen initialized replica online");
    assert!(
        reopen_online_started.elapsed() < Duration::from_secs(2),
        "initialized sync reopen should expose local state before its reconnect"
    );
    server_lix
        .execute(
            "UPDATE sync_mode_row SET value = 'reconnected' WHERE row_id = 'offline'",
            &[],
        )
        .await
        .expect("write after asynchronous reopen");
    wait_for_named_value(&reopened_online, "offline", "reconnected").await;
    reopened_online
        .close()
        .await
        .expect("close asynchronously reopened replica");

    let listener_address = server_url
        .strip_prefix("http://")
        .expect("test protocol uses http")
        .parse::<SocketAddr>()
        .expect("parse protocol listener address");
    server_task.abort();
    let _ = server_task.await;

    // Keep the endpoint accepting TCP connections without sending a
    // handshake response. A synchronous reopen would wait for the transport
    // timeout; an initialized local replica must return from open immediately
    // and let its worker retry in the background.
    let hanging_listener = TcpListener::bind(listener_address)
        .await
        .expect("rebind protocol listener for offline reopen");
    let hanging_stop = Arc::new(tokio::sync::Notify::new());
    let hanging_stop_for_task = Arc::clone(&hanging_stop);
    let hanging_task = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = hanging_listener.accept().await else {
                break;
            };
            let stop = Arc::clone(&hanging_stop_for_task);
            tokio::spawn(async move {
                tokio::select! {
                    _ = stop.notified() => drop(stream),
                    _ = tokio::time::sleep(Duration::from_secs(30)) => drop(stream),
                }
            });
        }
    });

    let reopen_started = std::time::Instant::now();
    let offline = open_lix()
        .with_storage(
            FilesystemStorage::new(replica_dir.path())
                .open()
                .expect("reopen replica storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("reopen initialized replica offline");
    assert!(
        reopen_started.elapsed() < Duration::from_secs(2),
        "initialized sync reopen should not await the network before exposing cached state"
    );
    hanging_stop.notify_waiters();
    hanging_task.abort();
    wait_for_named_value(&offline, "offline", "reconnected").await;
    // Cached scopes remain local-only while an uncached relation fails closed
    // instead of returning a partial snapshot after the server disappears.
    let uncached_error = offline
        .execute("SELECT * FROM sync_mode_uncached", &[])
        .await
        .expect_err("offline uncached scope must not fabricate a result");
    assert_eq!(uncached_error.code, LixError::CODE_INVALID_PARAM);
    offline
        .execute(
            "UPDATE sync_mode_row SET value = 'queued-offline' WHERE row_id = 'offline'",
            &[],
        )
        .await
        .expect("commit local row and outbox while offline");
    wait_for_named_value(&offline, "offline", "queued-offline").await;
    offline.close().await.expect("close offline replica");
    server_lix.close().await.expect("close server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_replica_bootstraps_pre_sync_server_history() {
    let server_lix = Arc::new(open_lix().await.expect("open server repository"));
    server_lix
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(ROW_SCHEMA.to_owned())],
        )
        .await
        .expect("register schema before sync authority starts");
    server_lix
        .execute(
            "INSERT INTO sync_mode_row (row_id, value) VALUES ('existing', 'server-only')",
            &[],
        )
        .await
        .expect("write pre-sync server row");
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let (server_url, server_task) = serve_protocol(protocol).await;

    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_lix()
        .with_storage(
            FilesystemStorage::new(replica_dir.path())
                .open()
                .expect("open fresh replica storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await;
    let replica = replica.expect("fresh replica should bootstrap authoritative history");
    let server_head = active_head(server_lix.as_ref()).await;
    let existing = replica
        .execute(
            "SELECT value FROM sync_mode_row WHERE row_id = 'existing'",
            &[],
        )
        .await
        .expect("read pre-sync server row after bootstrap");
    assert_eq!(
        existing.rows()[0].get::<String>("value").unwrap(),
        "server-only"
    );
    assert_eq!(
        active_head(&replica).await,
        server_head,
        "bootstrap must retain the authoritative head identity"
    );
    server_lix
        .execute(
            "UPDATE sync_mode_row SET value = 'after-bootstrap' WHERE row_id = 'existing'",
            &[],
        )
        .await
        .expect("write after bootstrap");
    wait_for_named_value(&replica, "existing", "after-bootstrap").await;

    replica.close().await.expect("close bootstrapped replica");

    server_lix.close().await.expect("close server");
    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authority_writes_before_first_pull_do_not_hide_pre_sync_history() {
    let server_lix = Arc::new(open_lix().await.expect("open server repository"));
    server_lix
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(ROW_SCHEMA.to_owned())],
        )
        .await
        .expect("register schema before sync authority starts");
    server_lix
        .execute(
            "INSERT INTO sync_mode_row (row_id, value) VALUES ('before-first-pull', 'old')",
            &[],
        )
        .await
        .expect("write pre-sync server row");
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let (server_url, server_task) = serve_protocol(protocol).await;

    // The authority is active, but no replica has requested a cursor yet.
    // This write must be included in the eventual oldest-first bootstrap
    // rather than creating cursor 1 and hiding the older row.
    server_lix
        .execute(
            "INSERT INTO sync_mode_row (row_id, value) VALUES ('after-authority', 'new')",
            &[],
        )
        .await
        .expect("write before first replica pull");

    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_lix()
        .with_storage(
            FilesystemStorage::new(replica_dir.path())
                .open()
                .expect("open fresh replica storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("fresh replica should bootstrap complete history");
    for (row_id, expected) in [("before-first-pull", "old"), ("after-authority", "new")] {
        let value = replica
            .execute(
                "SELECT value FROM sync_mode_row WHERE row_id = $1",
                &[Value::Text(row_id.to_owned())],
            )
            .await
            .expect("read bootstrapped row")
            .rows()[0]
            .get::<String>("value")
            .expect("bootstrapped row value");
        assert_eq!(value, expected, "history row {row_id} must be retained");
    }

    replica.close().await.expect("close replica");
    server_lix.close().await.expect("close server");
    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_replica_branch_and_pre_sync_topology_matches_server() {
    let server_lix = Arc::new(open_lix().await.expect("open server repository"));
    server_lix
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(ROW_SCHEMA.to_owned())],
        )
        .await
        .expect("register schema before sync authority starts");
    server_lix
        .execute(
            "INSERT INTO sync_mode_row (row_id, value) VALUES ('topology', 'one')",
            &[],
        )
        .await
        .expect("write first pre-sync row");
    let first_head = active_head(server_lix.as_ref()).await;
    server_lix
        .execute(
            "UPDATE sync_mode_row SET value = 'two' WHERE row_id = 'topology'",
            &[],
        )
        .await
        .expect("write second pre-sync row");
    let second_head = active_head(server_lix.as_ref()).await;
    let branch_id = "0198a000-0000-7000-8000-0000000000f1";
    server_lix
        .create_branch(lix::CreateBranchOptions {
            id: Some(branch_id.to_owned()),
            name: "pre-sync-feature".to_owned(),
            from_commit_id: Some(first_head.clone()),
        })
        .await
        .expect("create pre-sync branch");
    let server_catalog = server_lix
        .execute(
            "SELECT id, name, commit_id FROM lix_branch WHERE id != $1 ORDER BY id",
            &[Value::Text(lix::GLOBAL_BRANCH_ID.to_owned())],
        )
        .await
        .expect("read server branch catalog");
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let (server_url, server_task) = serve_protocol(protocol).await;
    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_lix()
        .with_storage(
            FilesystemStorage::new(replica_dir.path())
                .open()
                .expect("open fresh replica storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open fresh replica");

    wait_for_branch(&replica, branch_id).await;
    let replica_catalog = replica
        .execute(
            "SELECT id, name, commit_id FROM lix_branch WHERE id != $1 ORDER BY id",
            &[Value::Text(lix::GLOBAL_BRANCH_ID.to_owned())],
        )
        .await
        .expect("read replica branch catalog");
    let catalog_rows = |result: &lix::ExecuteResult| {
        result
            .rows()
            .iter()
            .map(|row| {
                (
                    row.get::<String>("id").expect("branch id"),
                    row.get::<String>("name").expect("branch name"),
                    row.get::<String>("commit_id").expect("branch commit"),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        catalog_rows(&replica_catalog),
        catalog_rows(&server_catalog),
        "fresh replica branch catalog must preserve server IDs, names, and heads"
    );
    assert_eq!(
        active_head(&replica).await,
        second_head,
        "fresh replica must select the authoritative server branch"
    );
    let active_value = replica
        .execute(
            "SELECT value FROM sync_mode_row WHERE row_id = 'topology'",
            &[],
        )
        .await
        .expect("read active pre-sync row")
        .rows()[0]
        .get::<String>("value")
        .expect("active pre-sync row value");
    assert_eq!(
        active_value, "two",
        "hydrating a parent from a feature branch must not leak its rows into the active branch"
    );
    let replica_first = replica
        .execute(
            "SELECT COUNT(*) AS count FROM lix_commit WHERE id = $1",
            &[Value::Text(first_head.clone())],
        )
        .await
        .expect("read first pre-sync commit")
        .rows()[0]
        .get::<i64>("count")
        .unwrap();
    let replica_second_parents = replica
        .execute(
            "SELECT parent_id FROM lix_commit_edge WHERE child_id = $1 ORDER BY parent_order",
            &[Value::Text(second_head.clone())],
        )
        .await
        .expect("read second pre-sync commit parents");
    let server_second_parents = server_lix
        .execute(
            "SELECT parent_id FROM lix_commit_edge WHERE child_id = $1 ORDER BY parent_order",
            &[Value::Text(second_head.clone())],
        )
        .await
        .expect("read server second pre-sync commit parents");
    assert_eq!(
        replica_first, 1,
        "the first pre-sync commit must be present"
    );
    assert_eq!(server_second_parents.rows().len(), 1);
    assert_eq!(replica_second_parents.rows().len(), 1);
    let server_parent = server_second_parents.rows()[0]
        .get::<String>("parent_id")
        .expect("server parent id");
    let replica_parent = replica_second_parents.rows()[0]
        .get::<String>("parent_id")
        .expect("replica parent id");
    assert_eq!(server_parent, first_head);
    assert_eq!(
        replica_parent, server_parent,
        "commit parent topology must match"
    );
    let server_commit_count = server_lix
        .execute("SELECT COUNT(*) AS count FROM lix_commit", &[])
        .await
        .expect("read server repository commit count")
        .rows()[0]
        .get::<i64>("count")
        .expect("server commit count");
    let replica_commit_count = replica
        .execute("SELECT COUNT(*) AS count FROM lix_commit", &[])
        .await
        .expect("read replica repository commit count")
        .rows()[0]
        .get::<i64>("count")
        .expect("replica commit count");
    assert_eq!(
        replica_commit_count, server_commit_count,
        "full-history hydration must expose repository-wide commit topology"
    );
    let server_change_count = server_lix
        .execute(
            "SELECT COUNT(*) AS count FROM lix_change WHERE schema_key = 'sync_mode_row'",
            &[],
        )
        .await
        .expect("read server pre-sync changes")
        .rows()[0]
        .get::<i64>("count")
        .expect("server pre-sync change count");
    let replica_change_count = replica
        .execute(
            "SELECT COUNT(*) AS count FROM lix_change WHERE schema_key = 'sync_mode_row'",
            &[],
        )
        .await
        .expect("read replica pre-sync changes")
        .rows()[0]
        .get::<i64>("count")
        .expect("replica pre-sync change count");
    assert_eq!(
        replica_change_count, server_change_count,
        "hidden bootstrap commits must not leak through lix_change"
    );

    replica.close().await.expect("close replica");
    server_lix.close().await.expect("close server");
    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plugin_rows_sync_and_each_replica_renders_its_local_file() {
    let template_dir = TempDir::new().expect("template tempdir");
    let (first_id, second_id) = {
        let storage = FilesystemStorage::new(template_dir.path())
            .open()
            .expect("open template storage");
        let seed = open_lix()
            .with_storage(storage)
            .await
            .expect("open template repository");
        install_markdown_plugin(&seed).await;
        write_file(
            &seed,
            "/shared.md",
            b"First paragraph.\n\nSecond paragraph.\n",
        )
        .await;
        let paragraphs = seed
            .execute(
                "SELECT id, payload_json FROM markdown_node WHERE kind = 'paragraph'",
                &[],
            )
            .await
            .expect("read template paragraphs");
        let paragraph_id = |needle: &str| {
            paragraphs
                .rows()
                .iter()
                .find(|row| row.get::<String>("payload_json").unwrap().contains(needle))
                .expect("paragraph exists")
                .get::<String>("id")
                .unwrap()
        };
        let ids = (
            paragraph_id("First paragraph."),
            paragraph_id("Second paragraph."),
        );
        seed.close().await.expect("close template repository");
        ids
    };

    let server_dir = TempDir::new().expect("server tempdir");
    let alice_dir = TempDir::new().expect("alice tempdir");
    let bob_dir = TempDir::new().expect("bob tempdir");
    copy_directory(template_dir.path(), server_dir.path());
    copy_directory(template_dir.path(), alice_dir.path());
    copy_directory(template_dir.path(), bob_dir.path());

    let server_storage = FilesystemStorage::new(server_dir.path())
        .open()
        .expect("open server filesystem storage");
    let server_lix = Arc::new(
        open_lix()
            .with_storage(server_storage)
            .await
            .expect("open server repository"),
    );
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let (server_url, server_task) = serve_protocol(protocol).await;
    let alice = open_lix()
        .with_storage(
            FilesystemStorage::new(alice_dir.path())
                .open()
                .expect("open alice storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open alice replica");
    let bob = open_lix()
        .with_storage(
            FilesystemStorage::new(bob_dir.path())
                .open()
                .expect("open bob storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open bob replica");

    update_markdown_paragraph(&alice, &first_id, "First from Alice.").await;
    update_markdown_paragraph(&bob, &second_id, "Second from Bob.").await;

    let expected = b"First from Alice.\n\nSecond from Bob.\n";
    wait_for_file(server_lix.as_ref(), expected).await;
    wait_for_file(&alice, expected).await;
    wait_for_file(&bob, expected).await;

    alice.close().await.expect("close alice");
    bob.close().await.expect("close bob");
    server_lix.close().await.expect("close server");
    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_replica_file_observe_hydrates_only_file_view_semantics() {
    let server_lix = Arc::new(open_lix().await.expect("open server repository"));
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let (server_url, server_task) = serve_protocol(protocol).await;
    install_markdown_plugin(server_lix.as_ref()).await;
    write_file(
        server_lix.as_ref(),
        "/shared.md",
        b"First paragraph.\n\nSecond paragraph.\n",
    )
    .await;

    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_lix()
        .with_storage(
            FilesystemStorage::new(replica_dir.path())
                .open()
                .expect("open replica storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open fresh lazy replica");
    let mut observation = replica
        .observe(
            "SELECT content FROM lix_file WHERE path = '/shared.md'",
            &[],
        )
        .expect("observe demanded file view");
    let event = tokio::time::timeout(SYNC_TEST_TIMEOUT, observation.next())
        .await
        .expect("lazy file hydration before timeout")
        .expect("lazy file observation event")
        .expect("lazy file observation remains open");
    assert_eq!(
        event.rows.rows()[0].get::<Vec<u8>>("content").unwrap(),
        b"First paragraph.\n\nSecond paragraph.\n"
    );

    // The file-view marker also covers plugin rows rendered from those bytes.
    // A later semantic query must therefore reuse the canonical event rather
    // than creating a local projection child or moving the branch head.
    let plugin_rows = replica
        .execute(
            "SELECT payload_json FROM markdown_node WHERE kind = 'paragraph' ORDER BY id",
            &[],
        )
        .await
        .expect("read plugin rows after file-scope hydration");
    assert_eq!(plugin_rows.rows().len(), 2);
    let server_head = active_head(server_lix.as_ref()).await;
    assert_eq!(active_head(&replica).await, server_head);

    observation.close();
    replica.close().await.expect("close replica");
    server_lix.close().await.expect("close server");
    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lazy_file_projection_survives_replica_restart_offline() {
    let server_lix = Arc::new(open_lix().await.expect("open server repository"));
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let (server_url, server_task) = serve_protocol(protocol).await;
    install_markdown_plugin(server_lix.as_ref()).await;
    write_file(
        server_lix.as_ref(),
        "/restart.md",
        b"Cached after restart.\n",
    )
    .await;

    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_lix()
        .with_storage(
            FilesystemStorage::new(replica_dir.path())
                .open()
                .expect("open replica storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open sync replica");
    let hydrated = replica
        .execute(
            "SELECT content FROM lix_file WHERE path = '/restart.md'",
            &[],
        )
        .await
        .expect("hydrate restart file");
    assert_eq!(
        hydrated.rows()[0].get::<Vec<u8>>("content").unwrap(),
        b"Cached after restart.\n"
    );
    replica.close().await.expect("close replica before restart");

    // The persisted replica is reopened after the server is gone. The
    // cached file projection and scope readiness must be sufficient for this
    // read; no network is available to mask a missing durable projection.
    server_lix.close().await.expect("close server");
    server_task.abort();
    let reopened = open_lix()
        .with_storage(
            FilesystemStorage::new(replica_dir.path())
                .open()
                .expect("reopen replica storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("reopen initialized replica offline");
    let cached = reopened
        .execute(
            "SELECT content FROM lix_file WHERE path = '/restart.md'",
            &[],
        )
        .await
        .expect("read cached file after restart");
    assert_eq!(
        cached.rows()[0].get::<Vec<u8>>("content").unwrap(),
        b"Cached after restart.\n"
    );
    reopened.close().await.expect("close reopened replica");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_replica_plugin_rows_hydrate_without_a_preinstalled_plugin() {
    let server_lix = Arc::new(open_lix().await.expect("open server repository"));
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let (server_url, server_task) = serve_protocol(protocol).await;
    install_markdown_plugin(server_lix.as_ref()).await;
    write_file(
        server_lix.as_ref(),
        "/fresh-plugin.md",
        b"First paragraph.\n\nSecond paragraph.\n",
    )
    .await;

    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_lix()
        .with_storage(
            FilesystemStorage::new(replica_dir.path())
                .open()
                .expect("open fresh replica storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open fresh lazy replica");

    let server_rows = server_lix
        .execute(
            "SELECT payload_json FROM markdown_node WHERE kind = 'paragraph'",
            &[],
        )
        .await
        .expect("read server plugin rows");
    assert_eq!(server_rows.rows().len(), 2);

    // The fresh local filesystem has no plugin archive. A row-first query
    // hydrates the registered schema and canonical plugin rows; only the
    // plugin-owned source payload needed to establish ownership is used when
    // the server event combines certified rows with a file mutation.
    let rows = replica
        .execute(
            "SELECT payload_json FROM markdown_node WHERE kind = 'paragraph' ORDER BY id",
            &[],
        )
        .await
        .expect("hydrate plugin rows on a fresh replica");
    let local_schema_count = replica
        .execute(
            "SELECT COUNT(*) AS count FROM lix_registered_schema WHERE schema_key = 'markdown_node'",
            &[],
        )
        .await
        .expect("read local registered plugin schema")
        .rows()[0]
        .get::<i64>("count")
        .expect("local schema count");
    let local_archive_count = replica
        .execute(
            "SELECT COUNT(*) AS count FROM lix_file WHERE path = '/.lix/plugins/plugin_markdown.lixplugin'",
            &[],
        )
        .await
        .expect("read local plugin archive")
        .rows()[0]
        .get::<i64>("count")
        .expect("local archive count");
    assert_eq!(local_schema_count, 1);
    assert_eq!(local_archive_count, 1);
    assert_eq!(rows.rows().len(), 2);
    assert!(
        rows.rows()[0]
            .get::<String>("payload_json")
            .expect("first plugin payload")
            .contains("First paragraph.")
    );
    assert!(
        rows.rows()[1]
            .get::<String>("payload_json")
            .expect("second plugin payload")
            .contains("Second paragraph.")
    );

    replica.close().await.expect("close replica");
    server_lix.close().await.expect("close server");
    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn switching_sync_branches_rebinds_each_replica_to_that_branch() {
    // Build one identical repository image first. This gives every replica a
    // certified local copy of both branch heads before sync authority starts.
    let template_dir = TempDir::new().expect("template tempdir");
    let main_branch_id = {
        let seed = open_lix()
            .with_storage(
                FilesystemStorage::new(template_dir.path())
                    .open()
                    .expect("open template storage"),
            )
            .await
            .expect("open template repository");
        seed.execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(ROW_SCHEMA.to_owned())],
        )
        .await
        .expect("register synchronized schema");
        let main_branch_id = seed.active_branch_id().await.expect("main branch id");
        seed.create_branch(lix::CreateBranchOptions {
            id: Some("0198a000-0000-7000-8000-0000000000b1".to_owned()),
            name: "feature".to_owned(),
            from_commit_id: None,
        })
        .await
        .expect("create feature branch");
        seed.close().await.expect("close template repository");
        main_branch_id
    };

    let server_dir = TempDir::new().expect("server tempdir");
    let alice_dir = TempDir::new().expect("alice tempdir");
    let bob_dir = TempDir::new().expect("bob tempdir");
    copy_directory(template_dir.path(), server_dir.path());
    copy_directory(template_dir.path(), alice_dir.path());
    copy_directory(template_dir.path(), bob_dir.path());

    let server_lix = Arc::new(
        open_lix()
            .with_storage(
                FilesystemStorage::new(server_dir.path())
                    .open()
                    .expect("open server storage"),
            )
            .await
            .expect("open server repository"),
    );
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let (server_url, server_task) = serve_protocol(protocol).await;
    let alice = open_lix()
        .with_storage(
            FilesystemStorage::new(alice_dir.path())
                .open()
                .expect("open alice storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open alice replica");
    let bob = open_lix()
        .with_storage(
            FilesystemStorage::new(bob_dir.path())
                .open()
                .expect("open bob storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open bob replica");
    let feature_branch_id = "0198a000-0000-7000-8000-0000000000b1";

    alice
        .switch_branch(lix::SwitchBranchOptions {
            branch_id: feature_branch_id.to_owned(),
        })
        .await
        .expect("switch alice to feature branch");
    bob.switch_branch(lix::SwitchBranchOptions {
        branch_id: feature_branch_id.to_owned(),
    })
    .await
    .expect("switch bob to feature branch");
    server_lix
        .switch_branch(lix::SwitchBranchOptions {
            branch_id: feature_branch_id.to_owned(),
        })
        .await
        .expect("switch server to feature branch");
    server_lix
        .execute(
            "INSERT INTO sync_mode_row (row_id, value) VALUES ('feature', 'from-feature')",
            &[],
        )
        .await
        .expect("write feature branch");
    wait_for_named_value(&alice, "feature", "from-feature").await;
    wait_for_named_value(&bob, "feature", "from-feature").await;

    alice
        .switch_branch(lix::SwitchBranchOptions {
            branch_id: main_branch_id.clone(),
        })
        .await
        .expect("switch alice back to main branch");
    server_lix
        .switch_branch(lix::SwitchBranchOptions {
            branch_id: main_branch_id,
        })
        .await
        .expect("switch server back to main branch");
    server_lix
        .execute(
            "INSERT INTO sync_mode_row (row_id, value) VALUES ('main-only', 'from-main')",
            &[],
        )
        .await
        .expect("write main branch");
    wait_for_named_value(&alice, "main-only", "from-main").await;

    alice.close().await.expect("close alice");
    bob.close().await.expect("close bob");
    server_lix.close().await.expect("close server");
    server_task.abort();
}

async fn wait_for_value<S>(lix: &Lix<S>, expected: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    tokio::time::timeout(SYNC_TEST_TIMEOUT, async {
        loop {
            let result = lix
                .execute(
                    "SELECT value FROM sync_mode_row WHERE row_id = 'shared'",
                    &[],
                )
                .await
                .expect("read synchronized row");
            if result
                .rows()
                .first()
                .and_then(|row| row.get::<String>("value").ok())
                .as_deref()
                == Some(expected)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for synchronized value '{expected}'"));
}

async fn active_head<S>(lix: &Lix<S>) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
        .await
        .expect("read active branch head")
        .rows()[0]
        .get::<String>("commit_id")
        .expect("active branch head commit id")
}

async fn wait_for_named_value<S>(lix: &Lix<S>, row_id: &str, expected: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    tokio::time::timeout(SYNC_TEST_TIMEOUT, async {
        loop {
            let result = lix
                .execute(
                    "SELECT value FROM sync_mode_row WHERE row_id = $1",
                    &[Value::Text(row_id.to_owned())],
                )
                .await
                .expect("read named synchronized row");
            if result
                .rows()
                .first()
                .and_then(|row| row.get::<String>("value").ok())
                .as_deref()
                == Some(expected)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for row '{row_id}' value '{expected}'"));
}

async fn wait_for_branch<S>(lix: &Lix<S>, branch_id: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    tokio::time::timeout(SYNC_TEST_TIMEOUT, async {
        loop {
            match lix
                .execute(
                    "SELECT id FROM lix_branch WHERE id = $1",
                    &[Value::Text(branch_id.to_owned())],
                )
                .await
            {
                Ok(result) if !result.rows().is_empty() => return,
                Ok(_) => {}
                Err(error) if error.code == LixError::CODE_INVALID_PARAM => {}
                Err(error) => panic!("read synchronized branch catalog: {error:?}"),
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for branch '{branch_id}'"));
}

async fn wait_for_branch_absent<S>(lix: &Lix<S>, branch_id: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    tokio::time::timeout(SYNC_TEST_TIMEOUT, async {
        loop {
            let result = lix
                .execute(
                    "SELECT id FROM lix_branch WHERE id = $1",
                    &[Value::Text(branch_id.to_owned())],
                )
                .await
                .expect("read synchronized branch deletion");
            if result.rows().is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for branch '{branch_id}' deletion"));
}

async fn wait_for_file<S>(lix: &Lix<S>, expected: &[u8])
where
    S: Storage + Clone + Send + Sync + 'static,
{
    tokio::time::timeout(SYNC_TEST_TIMEOUT, async {
        loop {
            let result = lix
                .execute(
                    "SELECT content FROM lix_file WHERE path = '/shared.md'",
                    &[],
                )
                .await
                .expect("read rendered file");
            let content = result.rows()[0].get::<Vec<u8>>("content").unwrap();
            if content == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("timed out waiting for rendered plugin file");
}

async fn wait_for_binary_file<S>(lix: &Lix<S>, expected: &[u8])
where
    S: Storage + Clone + Send + Sync + 'static,
{
    wait_for_file_at(lix, "/binary.bin", expected).await;
}

async fn wait_for_file_at<S>(lix: &Lix<S>, path: &str, expected: &[u8])
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let path = path.to_owned();
    tokio::time::timeout(SYNC_TEST_TIMEOUT, async {
        loop {
            let result = lix
                .execute(
                    "SELECT content FROM lix_file WHERE path = $1",
                    &[Value::Text(path.clone())],
                )
                .await
                .expect("read synchronized binary file");
            let content = result
                .rows()
                .first()
                .and_then(|row| row.get::<Vec<u8>>("content").ok());
            if content.as_deref() == Some(expected) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("timed out waiting for synchronized binary file");
}

async fn wait_for_file_absent<S>(lix: &Lix<S>, path: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let path = path.to_owned();
    tokio::time::timeout(SYNC_TEST_TIMEOUT, async {
        loop {
            let result = lix
                .execute(
                    "SELECT id FROM lix_file WHERE path = $1",
                    &[Value::Text(path.clone())],
                )
                .await
                .expect("read synchronized file absence");
            if result.rows().is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("timed out waiting for synchronized file deletion");
}

async fn install_markdown_plugin<S>(lix: &Lix<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ('/.lix/plugins/plugin_markdown.lixplugin', $1)",
        &[Value::Blob(markdown_plugin_archive().into())],
    )
    .await
    .expect("install markdown plugin");
}

async fn write_file<S>(lix: &Lix<S>, path: &str, content: &[u8])
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
        &[
            Value::Text(path.to_owned()),
            Value::Blob(content.to_vec().into()),
        ],
    )
    .await
    .expect("write plugin-backed file");
}

async fn update_markdown_paragraph<S>(lix: &Lix<S>, id: &str, text: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "UPDATE markdown_node SET payload_json = $1 WHERE id = $2",
        &[
            Value::Text(serde_json::json!({"inline":[{"type":"text","value":text}]}).to_string()),
            Value::Text(id.to_owned()),
        ],
    )
    .await
    .expect("update markdown semantic row");
}

fn markdown_plugin_archive() -> Vec<u8> {
    let wasm = std::fs::read(Path::new(env!(
        "CARGO_CDYLIB_FILE_PLUGIN_MARKDOWN_plugin_markdown"
    )))
    .expect("read markdown plugin component");
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    writer.start_file("manifest.json", options).unwrap();
    writer
        .write_all(include_bytes!("../../../plugins/markdown/manifest.json"))
        .unwrap();
    writer
        .start_file("schema/markdown_node.json", options)
        .unwrap();
    writer
        .write_all(include_bytes!(
            "../../../plugins/markdown/schema/markdown_node.json"
        ))
        .unwrap();
    writer.start_file("plugin.wasm", options).unwrap();
    writer.write_all(&wasm).unwrap();
    writer.finish().unwrap().into_inner()
}

fn copy_directory(source: &Path, target: &Path) {
    for entry in std::fs::read_dir(source).expect("read template directory") {
        let entry = entry.expect("template directory entry");
        let destination = target.join(entry.file_name());
        if entry.file_type().expect("template entry type").is_dir() {
            std::fs::create_dir_all(&destination).expect("create cloned directory");
            copy_directory(&entry.path(), &destination);
        } else {
            std::fs::copy(entry.path(), destination).expect("copy template file");
        }
    }
}

async fn serve_protocol<S>(protocol: LixServerProtocol<S>) -> (String, tokio::task::JoinHandle<()>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    serve_protocol_with_metrics(protocol, None).await
}

async fn serve_protocol_with_metrics<S>(
    protocol: LixServerProtocol<S>,
    metrics: Option<Arc<ProtocolMetrics>>,
) -> (String, tokio::task::JoinHandle<()>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind protocol listener");
    serve_protocol_with_listener(protocol, listener, metrics).await
}

async fn serve_protocol_with_response_delay<S>(
    protocol: LixServerProtocol<S>,
    response_delay: Duration,
) -> (String, tokio::task::JoinHandle<()>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind delayed protocol listener");
    serve_protocol_with_listener_and_delay(protocol, listener, None, response_delay).await
}

async fn serve_protocol_with_listener<S>(
    protocol: LixServerProtocol<S>,
    listener: TcpListener,
    metrics: Option<Arc<ProtocolMetrics>>,
) -> (String, tokio::task::JoinHandle<()>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    serve_protocol_with_listener_and_delay(protocol, listener, metrics, Duration::ZERO).await
}

async fn serve_protocol_with_listener_and_delay<S>(
    protocol: LixServerProtocol<S>,
    listener: TcpListener,
    metrics: Option<Arc<ProtocolMetrics>>,
    response_delay: Duration,
) -> (String, tokio::task::JoinHandle<()>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let address = listener.local_addr().expect("protocol listener address");
    let task = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("accept protocol client");
            let protocol = protocol.clone();
            let metrics = metrics.clone();
            tokio::spawn(async move {
                let service = service_fn(move |request| {
                    serve_request_with_metrics(
                        protocol.clone(),
                        request,
                        metrics.clone(),
                        response_delay,
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

async fn serve_request_with_metrics<S>(
    protocol: LixServerProtocol<S>,
    request: Request<Incoming>,
    metrics: Option<Arc<ProtocolMetrics>>,
    response_delay: Duration,
) -> Result<Response<Full<Bytes>>, Infallible>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let (parts, body) = request.into_parts();
    let is_sync_admission = parts.uri.path() == "/lix/v1/sync/admit";
    let body = body
        .collect()
        .await
        .expect("collect request body")
        .to_bytes();
    if let Some(metrics) = &metrics {
        metrics.requests.fetch_add(1, Ordering::Relaxed);
        metrics
            .request_bytes
            .fetch_add(body.len() as u64, Ordering::Relaxed);
    }
    let response = protocol
        .handle(
            Request::from_parts(parts, ServerProtocolBody::full(body)),
            ServerProtocolContext::anonymous(),
        )
        .await;
    if response_delay > Duration::ZERO {
        tokio::time::sleep(response_delay).await;
    }
    let (parts, body) = response.into_parts();
    let body = body
        .collect()
        .await
        .expect("collect protocol response")
        .to_bytes();
    if let Some(metrics) = &metrics {
        metrics
            .response_bytes
            .fetch_add(body.len() as u64, Ordering::Relaxed);
        if is_sync_admission
            && metrics
                .drop_first_admission_response
                .swap(false, Ordering::Relaxed)
        {
            // Return a retryable status after the protocol has already
            // admitted the transaction. The client therefore observes an
            // ambiguous receipt and must retry the same operation ID.
            return Ok(Response::builder()
                .status(503)
                .body(Full::new(Bytes::from_static(
                    br#"{"error":{"code":"LIX_SYNC_TEST_LOST_ACK","message":"test receipt dropped"}}"#,
                )))
                .expect("build lost-ack response"));
        }
    }
    Ok(Response::from_parts(parts, Full::new(body)))
}
