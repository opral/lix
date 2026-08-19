#![recursion_limit = "256"]

use std::convert::Infallible;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures_util::future::join_all;
use http::header::CONTENT_TYPE;
use http::{Method, Request, Response};
use http_body_util::{BodyExt as _, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use lix::server_protocol::{
    LixServerProtocol, ServerProtocolBody, ServerProtocolContext, ServerProtocolPrincipal,
};
use lix::storage::Storage;
use lix::{ExecuteBatchStatement, Lix, Memory, ServerOptions, Value, open_lix};
use lix_storage_filesystem::FilesystemStorage;
use tempfile::TempDir;
use tokio::net::TcpListener;

const WAIT_TIMEOUT: Duration = Duration::from_secs(15);
const OFFLINE_COMMIT_COUNT: usize = 513;

#[derive(Debug, Default)]
struct HttpProbe {
    pushes: AtomicU64,
    delta_pulls: AtomicU64,
    snapshot_row_pulls: AtomicU64,
    history_gets: AtomicU64,
    blob_gets: AtomicU64,
    chunk_gets: AtomicU64,
    drop_next_push_ack: AtomicBool,
    reject_requests: AtomicBool,
    one_way_delay_millis: AtomicU64,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn first_local_write_pushes_before_deferred_history_is_read() {
    let authority = Arc::new(open_lix().await.expect("open authority"));
    authority
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/shared.md', CAST('Hello world' AS BYTEA))",
            &[],
        )
        .await
        .expect("seed shared file");
    put_value(&authority, "head", "visible").await;
    let probe = Arc::new(HttpProbe::default());
    let (url, server_task) = serve(Arc::clone(&authority), Arc::clone(&probe)).await;
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
    tokio::time::timeout(WAIT_TIMEOUT, async {
        loop {
            let result = authority
                .execute(
                    "SELECT content FROM lix_file WHERE path = '/shared.md'",
                    &[],
                )
                .await
                .expect("read authority file");
            if result.rows()[0]
                .get::<Vec<u8>>("content")
                .expect("file content decodes")
                == b"Hello worlds"
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("file edit reaches authority");

    replica.close().await.expect("close replica");
    stop_server(server_task).await;
    authority.close().await.expect("close authority");
}

impl HttpProbe {
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_bootstrap_reads_authority_then_local_write_reaches_server() {
    let authority = Arc::new(open_lix().await.expect("open authority"));
    let authority_lix_id = authority.lix_id().to_owned();
    put_value(&authority, "bootstrap-parent", "lazy-history").await;
    let history_parent = active_head(&authority).await;
    put_value(&authority, "bootstrap", "from-authority").await;
    let history_head = active_head(&authority).await;
    let probe = Arc::new(HttpProbe::default());
    let (url, server_task) = serve(Arc::clone(&authority), Arc::clone(&probe)).await;
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
            "SELECT COUNT(*) AS entries FROM lix_diff($1, $2)",
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
    wait_for_value(&authority, "local", "from-replica").await;
    tokio::time::timeout(WAIT_TIMEOUT, async {
        loop {
            let rows = authority
                .execute(
                    "SELECT content FROM lix_file WHERE path = '/after-bootstrap.txt'",
                    &[],
                )
                .await
                .expect("authority file query succeeds");
            if rows
                .rows()
                .first()
                .is_some_and(|row| row.get::<Vec<u8>>("content").ok().as_deref() == Some(b"works"))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("timed out waiting for first synchronized file");

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
    authority.close().await.expect("close authority");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_bootstrap_pages_more_than_one_window_of_hot_rows() {
    let authority = Arc::new(open_lix().await.expect("open authority"));
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
    let probe = Arc::new(HttpProbe::default());
    let (url, server_task) = serve(Arc::clone(&authority), Arc::clone(&probe)).await;
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
    authority.close().await.expect("close authority");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn warm_filesystem_replica_reopens_and_writes_while_offline() {
    let authority = Arc::new(open_lix().await.expect("open authority"));
    put_value(&authority, "cached", "durable").await;
    let (url, server_task) = serve(Arc::clone(&authority), Arc::default()).await;
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
    authority.close().await.expect("close authority");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authenticated_identity_survives_fresh_push_and_offline_reopen() {
    let authority = Arc::new(open_lix().await.expect("open authority"));
    put_value(&authority, "authenticated-seed", "server").await;
    let principal = ServerProtocolPrincipal::Authenticated {
        account_id: lix::SYSTEM_ACCOUNT_ID.to_owned(),
        idempotency_scope: "sync-mode-authenticated-e2e".to_owned(),
    };
    let probe = Arc::new(HttpProbe::default());
    let (url, server_task) = serve_as(
        Arc::clone(&authority),
        Arc::clone(&probe),
        principal.clone(),
    )
    .await;
    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_replica(replica_dir.path(), &url).await;

    assert_eq!(active_account(&replica).await, lix::SYSTEM_ACCOUNT_ID);
    put_value(&replica, "authenticated-fresh", "accepted").await;
    wait_for_value(&authority, "authenticated-fresh", "accepted").await;
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
    authority.close().await.expect("close authority");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn more_than_one_offline_push_window_drains_after_reconnect() {
    let authority = Arc::new(open_lix().await.expect("open authority"));
    let commits_before = commit_count(&authority).await;
    let probe = Arc::new(HttpProbe::default());
    let (url, server_task) = serve(Arc::clone(&authority), Arc::clone(&probe)).await;
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
            if read_value(&authority, "offline-window").await.as_deref() == Some(expected.as_str())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("offline outbox should drain after reconnect");
    assert_eq!(
        commit_count(&authority).await,
        commits_before + OFFLINE_COMMIT_COUNT as i64,
    );
    assert!(
        probe.pushes.load(Ordering::Acquire) >= 2,
        "the outbox must cross at least two bounded pushes",
    );

    replica.close().await.expect("close replica");
    stop_server(server_task).await;
    authority.close().await.expect("close authority");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_clients_receive_remote_writes_through_a_held_long_poll() {
    let authority = Arc::new(open_lix().await.expect("open authority"));
    put_value(&authority, "seed", "ready").await;
    let probe = Arc::new(HttpProbe::default());
    let (url, server_task) = serve(Arc::clone(&authority), Arc::clone(&probe)).await;
    let alice_dir = TempDir::new().expect("alice tempdir");
    let bob_dir = TempDir::new().expect("bob tempdir");
    let alice = open_replica(alice_dir.path(), &url).await;
    let bob = open_replica(bob_dir.path(), &url).await;

    wait_for_counter(&probe.delta_pulls, 2).await;
    put_value(&alice, "shared", "from-alice").await;
    wait_for_value(&bob, "shared", "from-alice").await;
    wait_for_value(&authority, "shared", "from-alice").await;
    wait_for_counter(&probe.delta_pulls, 3).await;
    put_value(&authority, "server-originated", "from-authority").await;
    wait_for_value(&alice, "server-originated", "from-authority").await;
    wait_for_value(&bob, "server-originated", "from-authority").await;

    let ((), ()) = tokio::join!(
        put_value(&alice, "concurrent-alice", "alice"),
        put_value(&bob, "concurrent-bob", "bob"),
    );
    wait_for_value(&authority, "concurrent-alice", "alice").await;
    wait_for_value(&authority, "concurrent-bob", "bob").await;
    wait_for_value(&alice, "concurrent-bob", "bob").await;
    wait_for_value(&bob, "concurrent-alice", "alice").await;

    alice.close().await.expect("close alice");
    bob.close().await.expect("close bob");
    stop_server(server_task).await;
    authority.close().await.expect("close authority");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn file_observer_hydrates_lazy_chunks_after_a_remote_edit() {
    const OBSERVER_COUNT: usize = 70;
    let authority = Arc::new(open_lix().await.expect("open authority"));
    authority
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/shared.md', CAST('Hello world' AS BYTEA))",
            &[],
        )
        .await
        .expect("seed shared file");
    let probe = Arc::new(HttpProbe::default());
    let (url, server_task) = serve(Arc::clone(&authority), Arc::clone(&probe)).await;
    let alice_dir = TempDir::new().expect("alice tempdir");
    let bob_dir = TempDir::new().expect("bob tempdir");
    let alice = open_replica(alice_dir.path(), &url).await;
    let bob = open_replica(bob_dir.path(), &url).await;

    alice
        .execute(
            "SELECT content FROM lix_file WHERE path = '/shared.md'",
            &[],
        )
        .await
        .expect("hydrate Alice's initial file content");
    bob.execute(
        "SELECT content FROM lix_file WHERE path = '/shared.md'",
        &[],
    )
    .await
    .expect("hydrate Bob's initial file content");
    let initial_change_id = alice
        .execute(
            "SELECT lixcol_change_id FROM lix_file WHERE path = '/shared.md'",
            &[],
        )
        .await
        .expect("read Alice's initial file change id")
        .rows()[0]
        .get::<String>("lixcol_change_id")
        .expect("initial file change id decodes");
    let mut observers = Vec::with_capacity(OBSERVER_COUNT);
    for _ in 0..OBSERVER_COUNT {
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
        observers.push(events);
    }
    let mut writer_observers = Vec::with_capacity(OBSERVER_COUNT);
    for _ in 0..OBSERVER_COUNT {
        let mut events = bob
            .observe(
                "SELECT content FROM lix_file WHERE path = '/shared.md'",
                &[],
            )
            .expect("observe Bob's shared file");
        events
            .next()
            .await
            .expect("Bob's initial observer evaluation succeeds")
            .expect("Bob's initial observer event exists");
        writer_observers.push(events);
    }

    let chunk_gets_before_remote_edit = probe.chunk_gets.load(Ordering::Acquire);
    bob.execute(
        "UPDATE lix_file SET content = CAST('Hello worlds' AS BYTEA) WHERE path = '/shared.md'",
        &[],
    )
    .await
    .expect("Bob updates the shared file");
    tokio::time::timeout(WAIT_TIMEOUT, async {
        loop {
            let change_id = alice
                .execute(
                    "SELECT lixcol_change_id FROM lix_file WHERE path = '/shared.md'",
                    &[],
                )
                .await
                .expect("read Alice's file change id")
                .rows()[0]
                .get::<String>("lixcol_change_id")
                .expect("remote file change id decodes");
            if change_id != initial_change_id {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("timed out waiting for Alice to apply Bob's file metadata");
    assert_eq!(
        probe.chunk_gets.load(Ordering::Acquire),
        chunk_gets_before_remote_edit,
        "pull applies the remote commit without eagerly hydrating file chunks",
    );

    let remote = tokio::time::timeout(
        WAIT_TIMEOUT,
        join_all(observers.iter_mut().map(|events| events.next())),
    )
    .await
    .expect("timed out waiting for remote observer events");
    for event in remote {
        let event = event
            .expect("remote observer evaluation hydrates demanded chunks")
            .expect("remote observer event exists");
        assert_eq!(
            event.rows.rows()[0]
                .get::<Vec<u8>>("content")
                .expect("remote content decodes"),
            b"Hello worlds",
        );
    }
    assert_eq!(
        probe.chunk_gets.load(Ordering::Acquire),
        chunk_gets_before_remote_edit + 1,
        "identical observers share one exact lazy chunk hydration request",
    );

    alice.close().await.expect("close alice");
    bob.close().await.expect("close bob");
    stop_server(server_task).await;
    authority.close().await.expect("close authority");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn held_long_poll_stays_realtime_with_one_hundred_millisecond_rtt() {
    let authority = Arc::new(open_lix().await.expect("open authority"));
    put_value(&authority, "seed", "ready").await;
    let probe = Arc::new(HttpProbe::default());
    probe.set_round_trip_delay(Duration::from_millis(100));
    let (url, server_task) = serve(Arc::clone(&authority), Arc::clone(&probe)).await;
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
    authority.close().await.expect("close authority");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_push_ack_retries_the_same_commit_idempotently() {
    let authority = Arc::new(open_lix().await.expect("open authority"));
    let commits_before = commit_count(&authority).await;
    let probe = Arc::new(HttpProbe::default());
    let (url, server_task) = serve(Arc::clone(&authority), Arc::clone(&probe)).await;
    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_replica(replica_dir.path(), &url).await;
    wait_for_counter(&probe.delta_pulls, 1).await;

    probe.drop_next_push_ack();
    put_value(&replica, "lost-ack", "once").await;
    wait_for_value(&authority, "lost-ack", "once").await;
    replica
        .close()
        .await
        .expect("close after the authority committed but before a durable acknowledgement");
    drop(replica);
    let replica = open_replica(replica_dir.path(), &url).await;
    wait_for_counter(&probe.pushes, 2).await;
    assert_eq!(commit_count(&authority).await, commits_before + 1);
    assert_eq!(
        read_value(&replica, "lost-ack").await.as_deref(),
        Some("once")
    );

    replica.close().await.expect("close replica");
    stop_server(server_task).await;
    authority.close().await.expect("close authority");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn binary_chunks_remain_lazy_until_file_content_is_read() {
    let authority = Arc::new(open_lix().await.expect("open authority"));
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
    let probe = Arc::new(HttpProbe::default());
    let (url, server_task) = serve(Arc::clone(&authority), Arc::clone(&probe)).await;
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
    authority.close().await.expect("close authority");
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

async fn serve(
    authority: Arc<Lix<Memory>>,
    probe: Arc<HttpProbe>,
) -> (String, tokio::task::JoinHandle<()>) {
    serve_as(authority, probe, ServerProtocolPrincipal::Anonymous).await
}

async fn serve_as(
    authority: Arc<Lix<Memory>>,
    probe: Arc<HttpProbe>,
    principal: ServerProtocolPrincipal,
) -> (String, tokio::task::JoinHandle<()>) {
    let protocol = LixServerProtocol::new(authority);
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
    if parts.method == Method::GET && path == "/lix/v1/sync/history" {
        probe.history_gets.fetch_add(1, Ordering::Release);
    }
    if parts.method == Method::GET && path == "/lix/v1/sync/chunk" {
        probe.chunk_gets.fetch_add(1, Ordering::Release);
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
