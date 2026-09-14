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
        Self::new_with_browser_files(false).await
    }

    async fn new_with_browser_files(include_files: bool) -> Self {
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
        if include_files {
            let schema = serde_json::json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "legacy_custom_note",
                "columns": [
                    { "name": "id", "type": "text", "nullable": false },
                    { "name": "value", "type": "text", "nullable": false }
                ],
                "primary_key": ["id"]
            });
            lix.execute(
                "INSERT INTO lix_registered_schema(value) VALUES(CAST($1 AS JSONB))",
                &[crate::Value::Text(schema.to_string())],
            )
            .await
            .unwrap();
            lix.execute(
                "INSERT INTO legacy_custom_note(id,value) VALUES('legacy','custom-preserved')",
                &[],
            )
            .await
            .unwrap();
            lix.upsert_file_content("/legacy-small.bin", vec![1, 2, 3])
                .await
                .unwrap();
            let large: Vec<u8> = (0..300 * 1024).map(|index| (index % 251) as u8).collect();
            lix.upsert_file_content("/legacy-large.bin", large)
                .await
                .unwrap();
        }
        lix.close().await.unwrap();
        let protocol = crate::open_lix()
            .with_storage(storage)
            .serve()
            .with_embedded_lix_id()
            .await
            .unwrap();
        let id = protocol.lix_id().to_owned();
        let protocol = Arc::new(protocol);
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}/lix/{id}", listener.local_addr().unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        let fail_snapshot = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let failing = fail_snapshot.clone();
        let runtime = tokio::runtime::Handle::current();
        let worker = std::thread::spawn(move || {
            let mut connections = Vec::new();
            while !stopped.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        // A partial update may wait for an authority change. It must
                        // not block concurrent immutable-object hydration requests.
                        let protocol = protocol.clone();
                        let runtime = runtime.clone();
                        let failing = failing.clone();
                        connections.push(std::thread::spawn(move || {
                            if let Err(error) = serve_request(stream, &protocol, &runtime, &failing)
                            {
                                // Background sync can cancel a connection while the
                                // fixture reads a request or writes its response.
                                // Malformed complete requests and other I/O errors
                                // remain test failures.
                                if !matches!(
                                    error.kind(),
                                    std::io::ErrorKind::UnexpectedEof
                                        | std::io::ErrorKind::ConnectionReset
                                        | std::io::ErrorKind::ConnectionAborted
                                        | std::io::ErrorKind::BrokenPipe
                                ) {
                                    panic!("test HTTP request: {error}");
                                }
                            }
                        }));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(2));
                    }
                    Err(error) => panic!("test listener: {error}"),
                }
            }
            // Closing the protocol releases outstanding update waits before
            // shutdown joins the connection handlers.
            runtime.block_on(protocol.close()).unwrap();
            for connection in connections {
                connection.join().unwrap();
            }
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
) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    let mut header = Vec::new();
    while !header.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        stream.read_exact(&mut byte)?;
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
    stream.read_exact(&mut body)?;
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
    )?;
    for (name, value) in &parts.headers {
        if name != http::header::CONTENT_LENGTH && name != http::header::CONNECTION {
            write!(stream, "{}: {}\r\n", name, value.to_str().unwrap())?;
        }
    }
    stream.write_all(b"\r\n")?;
    stream.write_all(&bytes)?;
    Ok(())
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
    old_replica_with_recovery_data(authority, bank, local_only, false, crate::Memory::new()).await
}

async fn old_replica_with_recovery_data(
    authority: &Authority,
    bank: EpochBank,
    local_only: bool,
    tracked: bool,
    memory: crate::Memory,
) -> ReplicaStorage {
    let storage = ReplicaStorage::acquire(memory).await.unwrap();
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
    if tracked {
        lix.set_sync_role(crate::sync::SyncRole::Replica).unwrap();
        lix.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('offline-recovery', 'saved')",
            &[],
        )
        .await
        .unwrap();
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
        let sources = lix.replica_recovery_sources().await.unwrap();
        assert_eq!(sources.len(), 1);
        assert!(
            !sources[0].recovery_required,
            "acknowledged cache needs no recovery banner"
        );
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
async fn unacknowledged_replica_is_preserved_when_bootstrap_fails() {
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
                Ok(_) => panic!("unavailable bootstrap must fail"),
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
async fn local_only_data_is_exportable_after_rebuild_and_source_is_kept() {
    let authority = Authority::new().await;
    for bank in [EpochBank::Legacy, EpochBank::A] {
        let storage = old_replica_with_local_data(&authority, bank, true).await;
        admit_repository_with_server(&storage, None, Some(&authority.options()))
            .await
            .unwrap();
        let retained = list_retained_replica_sources(&storage).await.unwrap();
        assert_eq!(retained.len(), 1);
        assert!(retained[0].recovery_required);
        let lix = crate::open_lix()
            .with_storage(storage.clone())
            .await
            .unwrap();
        let sources = lix.replica_recovery_sources().await.unwrap();
        assert_eq!(sources.len(), 1);
        let export = lix.export_replica_recovery(&sources[0].id).await.unwrap();
        assert!(
            export
                .branches
                .iter()
                .flat_map(|branch| &branch.rows)
                .any(|row| row.untracked
                    && row.snapshot.as_ref().is_some_and(|snapshot| snapshot
                        .get("key")
                        .and_then(serde_json::Value::as_str)
                        == Some("local-only-upgrade-test")))
        );
        assert!(
            lix.execute(
                "SELECT value FROM lix_key_value WHERE key = 'local-only-upgrade-test'",
                &[]
            )
            .await
            .unwrap()
            .rows()
            .is_empty()
        );
        lix.close().await.unwrap();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_checkpoint_rewrite_reopens_from_server_without_historical_commits() {
    let authority = Authority::new().await;
    for bank in [EpochBank::Legacy, EpochBank::A] {
        // This fixture has local-only content and no historical commit bodies.
        // An old checkpoint rewrite cannot finish, but replacement must open.
        let storage = old_replica_with_local_data(&authority, bank, true).await;
        let source = StorageAdapter::for_epoch_unfenced(storage.clone(), bank);
        let mut write = source
            .begin_migration_write(WriteOptions::default())
            .await
            .unwrap();
        write
            .put_many(
                crate::init::REPOSITORY_PROTOCOL_SPACE,
                single_put(
                    crate::init::REPOSITORY_PROTOCOL_KEY,
                    Bytes::from_static(crate::init::REPOSITORY_PROTOCOL_V77_CHECKPOINT_REWRITE),
                ),
            )
            .await
            .unwrap();
        write.commit().await.unwrap();

        // A failed replacement must restore the exact transitional marker too.
        authority.fail_snapshot.store(true, Ordering::Release);
        let error =
            match admit_repository_with_server(&storage, None, Some(&authority.options())).await {
                Ok(_) => panic!("injected snapshot failure must fail"),
                Err(error) => error,
            };
        assert_eq!(error.code, "TEST_UNAVAILABLE");
        assert_eq!(
            crate::migration::api::load_repository_protocol_marker(&source)
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            crate::init::REPOSITORY_PROTOCOL_V77_CHECKPOINT_REWRITE,
        );
        authority.fail_snapshot.store(false, Ordering::Release);
        let admitted = admit_repository_with_server(&storage, None, Some(&authority.options()))
            .await
            .unwrap();
        assert_eq!(admitted.report.migration.unwrap().from_format, 77);
        assert!(matches!(
            admitted.adapter.epoch_bank(),
            EpochBank::Generation(_)
        ));
        let retained = list_retained_replica_sources(&storage).await.unwrap();
        assert_eq!(retained.len(), 1);
        assert!(retained[0].recovery_required);

        let lix = crate::open_lix()
            .with_storage(storage.clone())
            .await
            .unwrap();
        let rows = lix
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'upgrade-test'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            rows.rows()[0].get::<serde_json::Value>("value").unwrap(),
            "preserved"
        );
        let export = lix
            .export_replica_recovery(&retained[0].bank)
            .await
            .unwrap();
        assert!(
            export
                .branches
                .iter()
                .flat_map(|branch| &branch.rows)
                .any(|row| {
                    row.untracked
                        && row.snapshot.as_ref().is_some_and(|snapshot| {
                            snapshot.get("key").and_then(serde_json::Value::as_str)
                                == Some("local-only-upgrade-test")
                        })
                })
        );
        lix.close().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_hydrates_sparse_global_history_without_inheriting_caller_rows() {
    let authority = Authority::new().await;
    let memory = crate::Memory::new();
    let old_session =
        old_replica_with_recovery_data(&authority, EpochBank::A, false, true, memory.clone()).await;
    drop(old_session);
    let storage = crate::sync::durable_memory_for_test(memory);
    // This test exercises the explicit full-layout recovery writer. Admit that
    // layout directly; partial conversion separately preserves the archive.
    let owned = crate::storage_adapter::StorageSession::acquire(storage.clone())
        .await
        .unwrap();
    admit_repository_with_server(&owned, None, Some(&authority.options()))
        .await
        .unwrap();
    drop(owned);
    let lix = crate::open_lix().with_storage(storage).await.unwrap();
    // Fixture-only authoring admission creates unrelated caller state. The
    // recovery API independently authenticates its isolated writer context.
    lix.set_sync_role(crate::sync::SyncRole::Replica).unwrap();
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('caller-only', 'unrelated')",
        &[],
    )
    .await
    .unwrap();
    let sources = lix.replica_recovery_sources().await.unwrap();
    assert_eq!(sources.len(), 1);
    let adapter = lix.storage_adapter();
    let read = adapter.begin_read(ReadOptions::default()).await.unwrap();
    let global = crate::branch::BranchHeadControlContext::new()
        .reader(&read)
        .scan()
        .await
        .unwrap()
        .into_iter()
        .find(|(branch, _)| branch == crate::GLOBAL_BRANCH_ID)
        .unwrap()
        .1;
    drop(read);
    let mut write = adapter
        .begin_migration_write(WriteOptions::default())
        .await
        .unwrap();
    write
        .delete_many(
            crate::changelog::COMMIT_SPACE,
            &[Key(Bytes::from(crate::changelog::commit_key(
                global.head_commit_id,
            )))],
        )
        .await
        .unwrap();
    write.commit().await.unwrap();
    let read = adapter.begin_read(ReadOptions::default()).await.unwrap();
    assert!(
        crate::commit_graph::CommitGraphContext::new()
            .reader(&read)
            .load_node(&global.head_commit_id)
            .await
            .unwrap()
            .is_none(),
        "global ancestry must start cold"
    );
    drop(read);
    // An authority with a different repository must not hydrate or publish.
    let wrong_authority = Authority::new().await;
    let mismatch = lix
        .recover_replica_with_server(&sources[0].id, wrong_authority.options())
        .await
        .unwrap_err();
    assert_eq!(mismatch.code, LixError::CODE_INVALID_PARAM);
    let read = adapter.begin_read(ReadOptions::default()).await.unwrap();
    assert!(
        crate::commit_graph::CommitGraphContext::new()
            .reader(&read)
            .load_node(&global.head_commit_id)
            .await
            .unwrap()
            .is_none()
    );
    drop(read);
    let receipt = tokio::time::timeout(
        Duration::from_secs(15),
        lix.recover_replica_with_server(&sources[0].id, authority.options()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(receipt.branch_ids.len(), 1, "{:?}", receipt.unresolved);
    let recovered = lix
        .open_internal_session(&receipt.branch_ids[0], lix.active_account_id())
        .await
        .unwrap();
    assert_eq!(
        recovered
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'offline-recovery'",
                &[]
            )
            .await
            .unwrap()
            .rows()
            .len(),
        1
    );
    assert!(
        recovered
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'caller-only'",
                &[]
            )
            .await
            .unwrap()
            .rows()
            .is_empty()
    );
    recovered.close().await.unwrap();
    lix.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_conversion_readmits_already_partial_without_initializing_or_rebinding() {
    let authority = Authority::new().await;
    let storage = crate::sync::durable_memory_for_test(crate::Memory::new());
    let authenticated = crate::sync::authenticate_partial_conversion(authority.options(), None)
        .await
        .unwrap();
    let selected = authenticated
        .state()
        .descriptor()
        .selected_branch
        .branch_id
        .clone();
    install_fresh_partial_epoch(storage.clone(), authenticated.state())
        .await
        .unwrap();
    for _ in 0..2 {
        crate::convert_replica_to_partial(storage.clone(), authority.options(), Some(&selected))
            .await
            .unwrap();
    }
    let wrong_branch = "00000000-0000-7000-8000-000000000599";
    let error =
        crate::convert_replica_to_partial(storage.clone(), authority.options(), Some(wrong_branch))
            .await
            .unwrap_err();
    assert_eq!(error.code, "LIX_PARTIAL_CONVERSION_BRANCH_MISMATCH");
    let other = Authority::new().await;
    assert!(
        crate::convert_replica_to_partial(storage.clone(), other.options(), None)
            .await
            .is_err()
    );
    // Each public conversion acquires a new writer generation. Inspect through
    // a newly acquired session rather than the raw adapter's original token.
    let inspected = crate::storage_adapter::StorageSession::acquire(storage.clone())
        .await
        .unwrap();
    assert_eq!(
        admit_partial_epoch(&inspected)
            .await
            .unwrap()
            .state
            .repository_id(),
        authenticated.state().repository_id()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_conversion_does_not_initialize_missing_storage() {
    let authority = Authority::new().await;
    let storage = crate::sync::durable_memory_for_test(crate::Memory::new());
    assert!(
        crate::convert_replica_to_partial(storage.clone(), authority.options(), None)
            .await
            .is_err()
    );
    let inspected = crate::storage_adapter::StorageSession::acquire(storage)
        .await
        .unwrap();
    let status = crate::migration::inspect_lix(&inspected).await.unwrap();
    assert!(matches!(status, crate::migration::MigrationStatus::Missing));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_child_admission_can_overlap_same_engine_commits() {
    let lix = crate::open_lix().await.unwrap();
    let (writes, opens) = tokio::join!(
        async {
            for i in 0..12 {
                lix.execute(
                    &format!(
                        "INSERT INTO lix_key_value (key, value) VALUES ('admission-{i}', '{i}')"
                    ),
                    &[],
                )
                .await?;
            }
            Ok::<_, LixError>(())
        },
        async {
            for _ in 0..12 {
                let child = lix.open_another_session().await?;
                child.close().await?;
            }
            Ok::<_, LixError>(())
        }
    );
    writes.unwrap();
    opens.unwrap();
    let result = lix
        .execute(
            "SELECT key FROM lix_key_value WHERE key LIKE 'admission-%'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(result.rows().len(), 12);
    lix.close().await.unwrap();
}

#[tokio::test]
async fn engine_admission_restarts_expired_read_without_reinitializing() {
    let backing = crate::migration::CommitExpiringStorage::from_memory(crate::Memory::new());
    let adapter = StorageAdapter::new(backing.clone());
    let initialized =
        crate::init::initialize(adapter, &crate::tracked_state::TrackedStateContext::new())
            .await
            .unwrap();
    backing.expire_after_point_read(
        crate::init::REPOSITORY_PROTOCOL_SPACE,
        Key(Bytes::from_static(crate::init::REPOSITORY_PROTOCOL_KEY)),
    );
    let engine = Engine::new(backing).await.unwrap();
    assert_eq!(engine.lix_id(), initialized.lix_id);
    let session = engine.open_session().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test]
async fn initialization_seed_planning_survives_physical_read_expiration() {
    let backing = crate::migration::CommitExpiringStorage::from_memory(crate::Memory::new());
    backing.expire_after_point_read(
        crate::init::REPOSITORY_PROTOCOL_SPACE,
        Key(Bytes::from_static(crate::init::REPOSITORY_PROTOCOL_KEY)),
    );
    let adapter = StorageAdapter::new(backing.clone());
    crate::init::initialize(adapter, &crate::tracked_state::TrackedStateContext::new())
        .await
        .unwrap();
    assert!(backing.point_expiration_was_observed());
    let engine = Engine::new(backing).await.unwrap();
    let session = engine.open_session().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_full_sync_bootstrap_survives_read_expiry_during_candidate_install() {
    let authority = Authority::new().await;
    let memory = crate::Memory::new();
    let source =
        old_replica_with_recovery_data(&authority, EpochBank::Legacy, false, false, memory.clone())
            .await;
    drop(source);
    let backing = crate::migration::CommitExpiringStorage::from_memory(memory);
    backing.expire_after_point_read(
        replica_generation_bank(1)
            .unwrap()
            .map_space(crate::sync::SYNC_REPLICA_STATE_SPACE),
        crate::sync::replica_state_key(),
    );
    let storage = crate::storage_adapter::StorageSession::acquire(backing.clone())
        .await
        .unwrap();
    let admitted = admit_repository_with_server(&storage, None, Some(&authority.options()))
        .await
        .unwrap();
    assert!(
        backing.point_expiration_was_observed(),
        "must exercise candidate snapshot installation"
    );
    assert_eq!(admitted.report.migration.unwrap().from_format, 77);
    let lix = crate::open_lix().with_storage(storage).await.unwrap();
    let rows = lix
        .execute(
            "SELECT value FROM lix_key_value WHERE key = 'upgrade-test'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        rows.rows()[0].get::<serde_json::Value>("value").unwrap(),
        "preserved"
    );
    assert_eq!(lix.replica_recovery_sources().await.unwrap().len(), 1);
    lix.close().await.unwrap();
}

mod browser_fixture;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_dirty_archive_survives_partial_conversion_and_remains_exportable() {
    let authority = Authority::new().await;
    let memory = crate::Memory::new();
    let source =
        old_replica_with_recovery_data(&authority, EpochBank::Legacy, true, true, memory.clone())
            .await;
    drop(source);
    let storage = crate::sync::durable_memory_for_test(memory);
    let owned = crate::storage_adapter::StorageSession::acquire(storage.clone())
        .await
        .unwrap();
    admit_repository_with_server(&owned, None, Some(&authority.options()))
        .await
        .unwrap();
    let full = crate::open_lix().with_storage(owned).await.unwrap();
    let sources = full.replica_recovery_sources().await.unwrap();
    assert_eq!(sources.len(), 1);
    assert!(sources[0].recovery_required);
    let id = sources[0].id.clone();
    let before = full.export_replica_recovery(&id).await.unwrap();
    assert!(
        before
            .branches
            .iter()
            .flat_map(|branch| &branch.rows)
            .any(|row| row.untracked)
    );
    assert!(
        before
            .branches
            .iter()
            .flat_map(|branch| &branch.rows)
            .any(|row| {
                row.snapshot.as_ref().is_some_and(|value| {
                    value.get("key").and_then(serde_json::Value::as_str) == Some("offline-recovery")
                })
            })
    );
    full.close().await.unwrap();
    drop(full);
    crate::convert_replica_to_partial(storage.clone(), authority.options(), None)
        .await
        .unwrap();
    let partial = crate::open_lix()
        .with_storage(storage)
        .with_server(authority.options())
        .await
        .unwrap();
    let sources = partial.replica_recovery_sources().await.unwrap();
    assert!(
        sources
            .iter()
            .any(|source| source.id == id && source.recovery_required)
    );
    let after = partial.export_replica_recovery(&id).await.unwrap();
    assert_eq!(
        serde_json::to_value(&after).unwrap(),
        serde_json::to_value(&before).unwrap()
    );
    // Excluding background collaboration writes makes the revision assertion a
    // proof that rejected restoration cannot leave pending GLOBAL publication.
    let writes = partial.lock_collaboration_writes().await;
    let adapter = partial.storage_adapter();
    let branches = vec![
        partial.active_branch_id().await.unwrap(),
        crate::GLOBAL_BRANCH_ID.to_owned(),
    ];
    let read = adapter.begin_read(ReadOptions::default()).await.unwrap();
    let revision_before = crate::storage_adapter::load_repository_mutation_revision(&read)
        .await
        .unwrap();
    let heads_before = crate::branch::BranchHeadControlContext::new()
        .reader(&read)
        .load_many(&branches)
        .await
        .unwrap();
    drop(read);
    let error = partial.recover_replica(&id).await.unwrap_err();
    assert_eq!(error.code, "LIX_PARTIAL_RECOVERY_REQUIRES_EXPORT");
    let read = adapter.begin_read(ReadOptions::default()).await.unwrap();
    assert_eq!(
        crate::storage_adapter::load_repository_mutation_revision(&read)
            .await
            .unwrap(),
        revision_before
    );
    let heads_after = crate::branch::BranchHeadControlContext::new()
        .reader(&read)
        .load_many(&branches)
        .await
        .unwrap();
    assert_eq!(heads_after, heads_before);
    drop(read);
    drop(writes);
    partial
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('after-rejected-recovery', 'usable')",
            &[],
        )
        .await
        .unwrap();
    let rows = partial
        .execute(
            "SELECT value FROM lix_key_value WHERE key = 'after-rejected-recovery'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        rows.rows()[0].get::<serde_json::Value>("value").unwrap(),
        "usable"
    );
    let after_recovery = partial.export_replica_recovery(&id).await.unwrap();
    assert_eq!(
        serde_json::to_value(&after_recovery).unwrap(),
        serde_json::to_value(&before).unwrap()
    );
    partial.close().await.unwrap();
}
