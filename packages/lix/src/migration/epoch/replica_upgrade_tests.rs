//! HTTP integration coverage for upgrading a persisted, sparse replica.
use super::*;
use crate::server_protocol::{LixServerProtocol, ServerProtocolBody, ServerProtocolContext};
use crate::sync::SyncTransport;
use http_body_util::BodyExt;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};

struct Authority {
    url: String,
    stop: Arc<AtomicBool>,
    fail_snapshot: Arc<AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl Authority {
    async fn new() -> Self {
        let storage = crate::Memory::new();
        let lix = crate::open_lix()
            .with_storage(storage.clone())
            .await
            .unwrap();
        lix.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('upgrade-test', 'preserved')",
            &[],
        )
        .await
        .unwrap();
        lix.close().await.unwrap();
        let protocol = crate::open_lix()
            .with_storage(storage)
            .serve()
            .with_embedded_lix_id()
            .await
            .unwrap();
        let id = protocol.lix_id().to_owned();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}/lix/{id}", listener.local_addr().unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        let fail_snapshot = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let failing = fail_snapshot.clone();
        let runtime = tokio::runtime::Handle::current();
        let worker = std::thread::spawn(move || {
            while !stopped.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => serve_request(stream, &protocol, &runtime, &failing),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(2));
                    }
                    Err(error) => panic!("test listener: {error}"),
                }
            }
            runtime.block_on(protocol.close()).unwrap();
        });
        Self {
            url,
            stop,
            fail_snapshot,
            worker: Some(worker),
        }
    }

    fn options(&self) -> crate::ServerOptions {
        crate::ServerOptions {
            url: self.url.clone(),
            headers: Default::default(),
        }
    }
}

impl Drop for Authority {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.worker.take().unwrap().join().unwrap();
    }
}

fn serve_request(
    mut stream: std::net::TcpStream,
    protocol: &LixServerProtocol<crate::Memory>,
    runtime: &tokio::runtime::Handle,
    fail: &AtomicBool,
) {
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut header = Vec::new();
    while !header.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        stream.read_exact(&mut byte).unwrap();
        header.push(byte[0]);
        assert!(header.len() < 65536);
    }
    let header = String::from_utf8(header).unwrap();
    let mut lines = header.split("\r\n");
    let mut start = lines.next().unwrap().split_whitespace();
    let method = start.next().unwrap();
    let path = start.next().unwrap();
    let mut request = http::Request::builder().method(method).uri(path);
    let mut length = 0;
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line.split_once(':').unwrap();
        if name.eq_ignore_ascii_case("content-length") {
            length = value.trim().parse().unwrap();
        }
        request = request.header(name, value.trim());
    }
    let mut body = vec![0; length];
    stream.read_exact(&mut body).unwrap();
    let response = if fail.load(Ordering::Acquire) && path.contains("/sync/pull") {
        http::Response::builder()
            .status(503)
            .body(ServerProtocolBody::full(
                r#"{"error":{"code":"TEST_UNAVAILABLE","message":"snapshot unavailable"}}"#,
            ))
            .unwrap()
    } else {
        runtime.block_on(protocol.handle(
            request.body(ServerProtocolBody::full(body)).unwrap(),
            ServerProtocolContext::anonymous(),
        ))
    };
    let (parts, body) = response.into_parts();
    let bytes = runtime.block_on(body.collect()).unwrap().to_bytes();
    write!(
        stream,
        "HTTP/1.1 {} OK\r\nContent-Length: {}\r\nConnection: close\r\n",
        parts.status.as_u16(),
        bytes.len()
    )
    .unwrap();
    for (name, value) in &parts.headers {
        if name != http::header::CONTENT_LENGTH && name != http::header::CONNECTION {
            write!(stream, "{}: {}\r\n", name, value.to_str().unwrap()).unwrap();
        }
    }
    stream.write_all(b"\r\n").unwrap();
    stream.write_all(&bytes).unwrap();
}

type ReplicaStorage = crate::storage::StorageSession<crate::Memory>;

async fn old_replica(authority: &Authority, bank: EpochBank) -> ReplicaStorage {
    old_replica_with_local_data(authority, bank, false).await
}

async fn old_replica_with_local_data(
    authority: &Authority,
    bank: EpochBank,
    local_only: bool,
) -> ReplicaStorage {
    let storage = ReplicaStorage::acquire(crate::Memory::new()).await.unwrap();
    let adapter = StorageAdapter::for_epoch_unfenced(storage.clone(), bank);
    let server = authority.options();
    let prepared = crate::sync::prepare_sync_bootstrap(&server).await.unwrap();
    let mut lix = crate::handle::new_replica_migration_candidate(
        adapter.clone(),
        &prepared.default_branch_id,
    )
    .await
    .unwrap();
    let transport = crate::sync::install_sync_bootstrap(&mut lix, &server, prepared)
        .await
        .unwrap();
    assert!(!transport.active_account_id().is_empty());
    if local_only {
        lix.set_sync_role(crate::sync::SyncRole::Replica).unwrap();
        lix.execute("INSERT INTO lix_key_value (key, value, lixcol_untracked) VALUES ('local-only-upgrade-test', 'local-only', true)", &[]).await.unwrap();
    }
    lix.close().await.unwrap();
    transport.close_session().await.unwrap();
    drop(lix);
    let mut write = adapter
        .begin_migration_write(WriteOptions::default())
        .await
        .unwrap();
    write
        .put_many(
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            single_put(
                crate::init::REPOSITORY_PROTOCOL_KEY,
                Bytes::from_static(crate::init::REPOSITORY_PROTOCOL_V77),
            ),
        )
        .await
        .unwrap();
    // A sparse replica need not contain historical commit bodies. Deliberately
    // remove them all: upgrading must use receipts/current state, never graph
    // traversal or decoding the old commit record format.
    write
        .delete_range(
            crate::changelog::COMMIT_SPACE,
            KeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
        )
        .await
        .unwrap();
    write.commit().await.unwrap();
    if bank != EpochBank::Legacy {
        publish_pointer_absent(
            &storage,
            &encode_pointer(PointerState::Active {
                bank,
                generation: 1,
                format: 77,
                publication: None,
            }),
        )
        .await
        .unwrap();
    }
    storage
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clean_sparse_replica_bootstraps_before_publishing_new_epoch() {
    let authority = Authority::new().await;
    for bank in [EpochBank::Legacy, EpochBank::A] {
        let storage = old_replica(&authority, bank).await;
        let admitted = admit_repository_with_server(&storage, None, Some(&authority.options()))
            .await
            .unwrap();
        assert_eq!(admitted.report.migration.unwrap().from_format, 77);
        let old = StorageAdapter::for_epoch_unfenced(storage.clone(), bank);
        assert_eq!(
            crate::migration::api::load_repository_protocol_marker(&old)
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            if bank == EpochBank::Legacy {
                LEGACY_FENCE
            } else {
                crate::init::REPOSITORY_PROTOCOL_V77
            }
        );
        let lix = crate::open_lix().with_storage(storage).await.unwrap();
        let rows = lix
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'upgrade-test'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.rows().len(), 1);
        assert_eq!(
            rows.rows()[0].get::<serde_json::Value>("value").unwrap(),
            "preserved"
        );
        lix.close().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_replica_download_preserves_source_and_can_retry() {
    let authority = Authority::new().await;
    for bank in [EpochBank::Legacy, EpochBank::A] {
        let storage = old_replica(&authority, bank).await;
        let before = load_pointer(&storage)
            .await
            .unwrap()
            .map(|(_, bytes)| bytes);
        authority.fail_snapshot.store(true, Ordering::Release);
        let error =
            match admit_repository_with_server(&storage, None, Some(&authority.options())).await {
                Ok(_) => panic!("download must fail"),
                Err(error) => error,
            };
        assert_eq!(error.code, "TEST_UNAVAILABLE");
        assert_eq!(
            load_pointer(&storage)
                .await
                .unwrap()
                .map(|(_, bytes)| bytes),
            before
        );
        let old = StorageAdapter::for_epoch_unfenced(storage.clone(), bank);
        assert_eq!(
            crate::migration::api::load_repository_protocol_marker(&old)
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            crate::init::REPOSITORY_PROTOCOL_V77
        );
        authority.fail_snapshot.store(false, Ordering::Release);
        admit_repository_with_server(&storage, None, Some(&authority.options()))
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unacknowledged_replica_is_preserved_without_downloading() {
    let authority = Authority::new().await;
    for bank in [EpochBank::Legacy, EpochBank::A] {
        let storage = old_replica(&authority, bank).await;
        let source = StorageAdapter::for_epoch_unfenced(storage.clone(), bank);
        let read = source.begin_read(ReadOptions::default()).await.unwrap();
        let keys = [Key(Bytes::from_static(b"repository"))];
        let values = read
            .get_many(&[GetManyRequest {
                space: crate::sync::SYNC_AUTHORITY_STATE_SPACE,
                keys: &keys,
                opts: GetOptions::default(),
            }])
            .await
            .unwrap();
        let Some(ProjectedValue::FullValue(raw)) = &values.values[0] else {
            panic!("receipt");
        };
        let mut receipt: serde_json::Value = serde_json::from_slice(raw).unwrap();
        // A local branch is ahead of its last server receipt.
        receipt["authoritativeBranches"][crate::GLOBAL_BRANCH_ID]["headCommitId"] =
            serde_json::Value::String(uuid::Uuid::now_v7().to_string());
        drop(read);
        let expected = Bytes::from(serde_json::to_vec(&receipt).unwrap());
        let mut write = source
            .begin_migration_write(WriteOptions::default())
            .await
            .unwrap();
        write
            .put_many(
                crate::sync::SYNC_AUTHORITY_STATE_SPACE,
                single_put(b"repository", expected.clone()),
            )
            .await
            .unwrap();
        write.commit().await.unwrap();
        let before = load_pointer(&storage)
            .await
            .unwrap()
            .map(|(_, bytes)| bytes);
        authority.fail_snapshot.store(true, Ordering::Release);
        let error =
            match admit_repository_with_server(&storage, None, Some(&authority.options())).await {
                Ok(_) => panic!("pending work must block"),
                Err(error) => error,
            };
        assert_eq!(error.code, "LIX_ERROR_REPLICA_UPGRADE_BLOCKED");
        assert_eq!(error.details.unwrap()["reason"], "pending_changes");
        assert_eq!(
            load_pointer(&storage)
                .await
                .unwrap()
                .map(|(_, bytes)| bytes),
            before
        );
        let read = source.begin_read(ReadOptions::default()).await.unwrap();
        let values = read
            .get_many(&[GetManyRequest {
                space: crate::sync::SYNC_AUTHORITY_STATE_SPACE,
                keys: &keys,
                opts: GetOptions::default(),
            }])
            .await
            .unwrap();
        assert_eq!(values.values[0], Some(ProjectedValue::FullValue(expected)));
        authority.fail_snapshot.store(false, Ordering::Release);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_only_data_blocks_rebuild_and_keeps_source() {
    let authority = Authority::new().await;
    for bank in [EpochBank::Legacy, EpochBank::A] {
        let storage = old_replica_with_local_data(&authority, bank, true).await;
        let before = load_pointer(&storage)
            .await
            .unwrap()
            .map(|(_, bytes)| bytes);
        let error =
            match admit_repository_with_server(&storage, None, Some(&authority.options())).await {
                Ok(_) => panic!("local-only data must block"),
                Err(error) => error,
            };
        assert_eq!(error.code, "LIX_ERROR_REPLICA_UPGRADE_BLOCKED");
        assert_eq!(error.details.unwrap()["reason"], "local_only_data");
        assert_eq!(
            load_pointer(&storage)
                .await
                .unwrap()
                .map(|(_, bytes)| bytes),
            before
        );
        let source = StorageAdapter::for_epoch_unfenced(storage.clone(), bank);
        let read = source.begin_read(ReadOptions::default()).await.unwrap();
        let hot = crate::hot_state::HotStateContext::new(
            crate::tracked_state::TrackedStateContext::new(),
            crate::commit_graph::CommitGraphContext::new(),
        )
        .reader(&read);
        let rows = hot
            .scan_batch(&crate::hot_state::HotStateScanRequest {
                filter: crate::hot_state::HotStateFilter {
                    schema_keys: vec!["lix_key_value".to_owned()],
                    row_pks: vec![crate::row_pk::RowPk::single("local-only-upgrade-test")],
                    untracked: Some(true),
                    ..Default::default()
                },
                projection: Default::default(),
                limit: None,
            })
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        let snapshot: serde_json::Value =
            serde_json::from_str(rows.row(0).snapshot_content().unwrap().as_ref()).unwrap();
        assert_eq!(snapshot["value"], "local-only");
    }
}
