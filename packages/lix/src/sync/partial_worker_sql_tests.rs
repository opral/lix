use super::*;
use crate::sync::http::{HttpSyncTransport, RawHttpClient, RawHttpRequest, RawHttpResponse};
use crate::sync::{SyncPushRequest, SyncTransportFuture};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Clone)]
pub(super) struct AuthorityClient {
    pub(super) authority: Arc<Lix<Memory>>,
    pub(super) pushes: Arc<AtomicUsize>,
    pub(super) chunks: Arc<AtomicUsize>,
    pub(super) metadata: Arc<AtomicUsize>,
    pub(super) first_accepted: Arc<tokio::sync::Notify>,
    pub(super) block_first: bool,
}
impl AuthorityClient {
    pub(super) fn new(authority: Arc<Lix<Memory>>, block_first: bool) -> Self {
        Self {
            authority,
            block_first,
            pushes: Arc::default(),
            chunks: Arc::default(),
            metadata: Arc::default(),
            first_accepted: Arc::default(),
        }
    }
}
impl RawHttpClient for AuthorityClient {
    fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
        Box::pin(async move {
            let url = url::Url::parse(&request.url).unwrap();
            let result = if request.method == http::Method::GET && !url.path().contains("/sync/") {
                serde_json::json!({"protocolVersion":crate::SERVER_PROTOCOL_VERSION,"syncProtocolVersion":crate::sync::SYNC_PROTOCOL_VERSION,"lixId":self.authority.lix_id(),"sessionId":"partial-worker-authority","activeAccountId":self.authority.active_account_id()})
            } else if url.path().ends_with("/sync/native-metadata") {
                let body = serde_json::from_slice(request.body.as_ref().unwrap()).unwrap();
                self.metadata.fetch_add(1, Ordering::SeqCst);
                serde_json::to_value(self.authority.read_sync_native_metadata(&body).await?)
                    .unwrap()
            } else if url.path().ends_with("/sync/push") {
                let body: SyncPushRequest =
                    serde_json::from_slice(request.body.as_ref().unwrap()).unwrap();
                let receipt = self
                    .authority
                    .push_sync_repository_for_account(&body, self.authority.active_account_id())
                    .await?;
                let index = self.pushes.fetch_add(1, Ordering::SeqCst);
                if index == 0 && self.block_first {
                    self.first_accepted.notify_one();
                    futures_util::future::pending::<()>().await;
                }
                serde_json::to_value(receipt).unwrap()
            } else if request.method == http::Method::POST && url.path().ends_with("/sync/blob") {
                let manifest = serde_json::from_slice(request.body.as_ref().unwrap()).unwrap();
                serde_json::to_value(
                    self.authority
                        .register_sync_blob_manifest(&manifest)
                        .await?,
                )
                .unwrap()
            } else if request.method == http::Method::PUT && url.path().ends_with("/sync/chunk") {
                let id = url
                    .query_pairs()
                    .find(|(key, _)| key == "chunkId")
                    .unwrap()
                    .1
                    .into_owned();
                self.authority
                    .put_sync_chunk(&id, request.body.as_ref().unwrap())
                    .await?;
                self.chunks.fetch_add(1, Ordering::SeqCst);
                serde_json::json!({})
            } else if request.method == http::Method::DELETE {
                serde_json::json!({})
            } else {
                panic!(
                    "unexpected worker request {} {}",
                    request.method, request.url
                );
            };
            Ok(RawHttpResponse {
                status: 200,
                status_text: "OK".into(),
                body: serde_json::to_vec(&result).unwrap(),
            })
        })
    }
}

#[tokio::test]
async fn partial_upload_worker_yields_to_demands_and_retries_ambiguous_acceptance() {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let width = 16;
        let authority = Arc::new(open_lix().await.unwrap());
        let values = (0..width)
            .map(|index| format!("('upload-{index:06}', 'before')"))
            .collect::<Vec<_>>()
            .join(",");
        authority
            .execute(
                &format!("INSERT INTO lix_key_value (key,value) VALUES {values}"),
                &[],
            )
            .await
            .unwrap();
        let state = PartialReplicaState::new(
            format!("https://example.test/lix/{}", authority.lix_id()),
            authority.active_account_id().into(),
            "00000000-0000-7000-8000-000000003399".into(),
            authority.partial_replica_descriptor(None).await.unwrap(),
        )
        .unwrap();
        let branch = state.descriptor().selected_branch.branch_id.clone();
        let memory = Memory::new();
        let storage = StorageAdapter::new(memory.clone());
        let read = storage.begin_read(Default::default()).await.unwrap();
        let mut writes = storage.new_write_set();
        let preconditions = stage_partial_bootstrap(&read, &mut writes, &state).unwrap();
        crate::init::stage_partial_repository_protocol(&mut writes);
        drop(read);
        storage
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions,
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let (engine, session) =
            Engine::new_partial_replica(storage.clone(), EngineOptions::new(), &state)
                .await
                .unwrap();
        engine.sync_mode().admit_partial_replica(
            Arc::new(state.clone()),
            crate::sync::partial_replica_write_capability(),
        );
        storage.admit_partial_replica_writer(crate::sync::partial_replica_write_capability());
        let update = "UPDATE lix_key_value SET value=$2 WHERE key=$1";
        let select = "SELECT value FROM lix_key_value WHERE key=$1";
        let key = Value::Text("upload-000000".into());
        let mut fetches = Fetches::default();
        execute_hydrating(
            &session,
            &storage,
            &state,
            &authority,
            update,
            &[key.clone(), Value::Text("uploaded".into())],
            &mut fetches,
        )
        .await
        .unwrap();

        crate::sync::partial_write_frontier::prepare_baseline_write_frontier(
            &storage,
            &state,
            |address| {
                let storage = &storage;
                let state = &state;
                let authority = &authority;
                async move {
                    hydrate_metadata(storage, state, authority, address, &mut Fetches::default())
                        .await
                }
            },
        )
        .await
        .unwrap();
        let client = AuthorityClient::new(authority.clone(), true);
        let transport = HttpSyncTransport::connect_with(client.clone(), state.remote_id())
            .await
            .unwrap();
        let (shutdown, shutdown_rx) =
            tokio::sync::watch::channel(crate::sync::runtime::SyncShutdown::Running);
        let (sender, receiver) = tokio::sync::mpsc::channel(4);
        let worker = crate::sync::partial_runtime::run_partial_worker_with_changes(
            storage.clone(),
            Arc::new(state.clone()),
            Some(transport),
            || Box::pin(async { Err(LixError::unknown("unexpected reconnect")) }),
            shutdown_rx,
            receiver,
            Some(engine.sync_mode().change_watcher()),
        );
        let caller = async {
            client.first_accepted.notified().await;
            session
                .execute(update, &[key.clone(), Value::Text("newer".into())])
                .await
                .unwrap();
            // This already-resident demand interrupts the unresolved upload.
            // It needs no HTTP but exercises foreground arbitration.
            let (response, done) = tokio::sync::oneshot::channel();
            sender
                .send(crate::sync::runtime::SyncDemand {
                    request: crate::sync::runtime::SyncDemandRequest::NativeMetadata(
                        NativeMetadataRef::CommitGraphRecord(
                            state.descriptor().selected_branch.head.commit_id.clone(),
                        ),
                        LixError::unknown("queued resident demand"),
                    ),
                    response,
                })
                .await
                .unwrap();
            done.await.unwrap().unwrap();
            loop {
                let read = storage.begin_read(Default::default()).await.unwrap();
                let push = crate::sync::partial_push_state::load_partial_push_state(
                    &read, &state, &branch,
                )
                .await
                .unwrap()
                .0;
                let control = crate::branch::BranchHeadControlContext::new()
                    .reader(&read)
                    .load(&branch)
                    .await
                    .unwrap()
                    .unwrap();
                if push.prepared.is_none()
                    && push.confirmed.head == control.head_commit_id
                {
                    break;
                }
                drop(read);
                tokio::task::yield_now().await;
            }
            assert_eq!(
                value(session.execute(select, &[key.clone()]).await.unwrap()),
                "newer"
            );
            assert_eq!(
                value(authority.execute(select, &[key.clone()]).await.unwrap()),
                "newer"
            );
            assert_eq!(client.pushes.load(Ordering::SeqCst), 3);
            shutdown.send_replace(crate::sync::runtime::SyncShutdown::Stop);
        };
        let (result, ()) = futures_util::join!(worker, caller);
        result.unwrap();
        session.close().await.unwrap();
    })
    .await
    .expect("worker must yield to foreground and finish retries");
}

#[tokio::test]
async fn production_frontier_preparation_supports_thirty_local_appends() {
    let width = 16;
    let authority = Arc::new(open_lix().await.unwrap());
    let values = (0..width)
        .map(|index| format!("('upload-{index:06}', 'before')"))
        .collect::<Vec<_>>()
        .join(",");
    authority
        .execute(
            &format!("INSERT INTO lix_key_value (key,value) VALUES {values}"),
            &[],
        )
        .await
        .unwrap();
    // A nontrivial first-parent lane makes preparation exercise ancestors
    // beyond the immediate baseline parent/jump pair needed by one write.
    for index in 0..40 {
        authority
            .execute(
                "UPDATE lix_key_value SET value = $1 WHERE key = 'upload-000000'",
                &[Value::Text(format!("baseline-{index}"))],
            )
            .await
            .unwrap();
    }
    let state = PartialReplicaState::new(
        format!("https://example.test/lix/{}", authority.lix_id()),
        authority.active_account_id().into(),
        "00000000-0000-7000-8000-000000003399".into(),
        authority.partial_replica_descriptor(None).await.unwrap(),
    )
    .unwrap();
    let memory = Memory::new();
    let storage = StorageAdapter::new(memory.clone());
    let read = storage.begin_read(Default::default()).await.unwrap();
    let mut writes = storage.new_write_set();
    let preconditions = stage_partial_bootstrap(&read, &mut writes, &state).unwrap();
    crate::init::stage_partial_repository_protocol(&mut writes);
    drop(read);
    storage
        .commit_write_set(
            writes,
            StorageWriteOptions {
                preconditions,
                await_durable: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let (engine, session) =
        Engine::new_partial_replica(storage.clone(), EngineOptions::new(), &state)
            .await
            .unwrap();
    engine.sync_mode().admit_partial_replica(
        Arc::new(state.clone()),
        crate::sync::partial_replica_write_capability(),
    );
    storage.admit_partial_replica_writer(crate::sync::partial_replica_write_capability());
    let update = "UPDATE lix_key_value SET value=$2 WHERE key=$1";
    let select = "SELECT value FROM lix_key_value WHERE key=$1";
    let key = Value::Text("upload-000000".into());
    let mut fetches = Fetches::default();
    execute_hydrating(
        &session,
        &storage,
        &state,
        &authority,
        update,
        &[key.clone(), Value::Text("uploaded".into())],
        &mut fetches,
    )
    .await
    .unwrap();

    let client = AuthorityClient::new(authority.clone(), false);
    let transport = HttpSyncTransport::connect_with(client.clone(), state.remote_id())
        .await
        .unwrap();
    transport
        .bind_native_baseline_lease(state.baseline_lease())
        .unwrap();
    let (shutdown, shutdown_rx) =
        tokio::sync::watch::channel(crate::sync::runtime::SyncShutdown::Running);
    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    let worker = crate::sync::partial_runtime::run_partial_worker_with_changes(
        storage.clone(),
        Arc::new(state.clone()),
        Some(transport),
        || Box::pin(async { Err(LixError::unknown("unexpected reconnect")) }),
        shutdown_rx,
        receiver,
        None,
    );
    let caller = async {
        // A resident baseline graph demand must still prepare its missing
        // jump frontier through the real authenticated metadata transport.
        let (response, done) = tokio::sync::oneshot::channel();
        sender
            .send(crate::sync::runtime::SyncDemand {
                request: crate::sync::runtime::SyncDemandRequest::NativeMetadata(
                    NativeMetadataRef::CommitGraphRecord(
                        state.descriptor().selected_branch.head.commit_id.clone(),
                    ),
                    LixError::unknown("prepare baseline write frontier"),
                ),
                response,
            })
            .await
            .unwrap();
        done.await.unwrap().unwrap();
        let prepared_fetches = client.metadata.load(Ordering::SeqCst);
        assert!(
            prepared_fetches > 0,
            "fixture must exercise missing production frontier metadata"
        );
        for index in 0..30 {
            session
                .execute(
                    update,
                    &[key.clone(), Value::Text(format!("prepared-{index}"))],
                )
                .await
                .unwrap();
            session.execute(select, &[key.clone()]).await.unwrap();
        }
        assert_eq!(
            client.metadata.load(Ordering::SeqCst),
            prepared_fetches,
            "prepared local appends must not require additional graph metadata"
        );
        assert_eq!(client.pushes.load(Ordering::SeqCst), 0);
        shutdown.send_replace(crate::sync::runtime::SyncShutdown::Stop);
    };
    let (result, ()) = futures_util::join!(worker, caller);
    result.unwrap();
    session.close().await.unwrap();
}

#[derive(Clone)]
struct WatchingAuthorityClient {
    base: AuthorityClient,
    watches: Arc<AtomicUsize>,
    native_reads: Arc<AtomicUsize>,
    blocked: Arc<tokio::sync::Notify>,
    changed: Arc<tokio::sync::Notify>,
}
impl RawHttpClient for WatchingAuthorityClient {
    fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
        Box::pin(async move {
            let url = url::Url::parse(&request.url).unwrap();
            let response = if url.path().ends_with("/sync/descriptor") {
                self.watches.fetch_add(1, Ordering::SeqCst);
                let branch = url
                    .query_pairs()
                    .find(|(key, _)| key == "branchId")
                    .map(|(_, value)| value.into_owned());
                let after = url
                    .query_pairs()
                    .find(|(key, _)| key == "after")
                    .map(|(_, value)| value.parse::<u64>().unwrap());
                let current = self
                    .base
                    .authority
                    .partial_replica_descriptor(branch.as_deref())
                    .await?;
                if after.is_some_and(|after| current.cursor <= after) {
                    self.blocked.notify_one();
                    self.changed.notified().await;
                }
                serde_json::to_value(
                    self.base
                        .authority
                        .leased_partial_replica_descriptor(branch.as_deref())
                        .await?,
                )
                .unwrap()
            } else if url.path().ends_with("/sync/native-object-range")
                || url.path().ends_with("/sync/native-metadata")
                || url.path().ends_with("/sync/native-objects")
            {
                self.native_reads.fetch_add(1, Ordering::SeqCst);
                let lease = request
                    .headers
                    .iter()
                    .find(|(name, _)| name == "lix-native-baseline-lease")
                    .expect("native request carries authority pin")
                    .1
                    .as_str();
                if url.path().ends_with("/sync/native-objects") {
                    #[derive(serde::Deserialize)]
                    #[serde(deny_unknown_fields)]
                    struct Request {
                        objects: Vec<NativeObjectRef>,
                    }
                    let body: Request =
                        serde_json::from_slice(request.body.as_ref().unwrap()).unwrap();
                    serde_json::to_value(
                        self.base
                            .authority
                            .read_sync_native_objects_leased(&body.objects, lease)
                            .await?,
                    )
                    .unwrap()
                } else if url.path().ends_with("/sync/native-object-range") {
                    let body = serde_json::from_slice(request.body.as_ref().unwrap()).unwrap();
                    serde_json::to_value(
                        self.base
                            .authority
                            .read_sync_native_object_range_leased(&body, lease)
                            .await?,
                    )
                    .unwrap()
                } else {
                    let body = serde_json::from_slice(request.body.as_ref().unwrap()).unwrap();
                    serde_json::to_value(
                        self.base
                            .authority
                            .read_sync_native_metadata_leased(&body, lease)
                            .await?,
                    )
                    .unwrap()
                }
            } else {
                return self.base.send(request).await;
            };
            Ok(RawHttpResponse {
                status: 200,
                status_text: "OK".into(),
                body: serde_json::to_vec(&response).unwrap(),
            })
        })
    }
}

#[tokio::test]
async fn engine_worker_preempts_watch_then_publishes_retained_negative_scope() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let authority = Arc::new(open_lix().await.unwrap());
        authority
            .set_sync_role(crate::sync::SyncRole::Authority)
            .unwrap();
        authority
            .execute(
                "INSERT INTO lix_key_value (key,value) VALUES ('seed','before')",
                &[],
            )
            .await
            .unwrap();
        let leased = authority
            .leased_partial_replica_descriptor(None)
            .await
            .unwrap();
        let old = Arc::new(
            PartialReplicaState::from_leased(
                format!("https://example.test/lix/{}", authority.lix_id()),
                authority.active_account_id().into(),
                uuid::Uuid::now_v7().to_string(),
                leased,
            )
            .unwrap(),
        );
        let storage = StorageAdapter::new(Memory::new());
        let read = storage.begin_read(Default::default()).await.unwrap();
        let mut writes = storage.new_write_set();
        let preconditions = stage_partial_bootstrap(&read, &mut writes, &old).unwrap();
        crate::init::stage_partial_repository_protocol(&mut writes);
        drop(read);
        storage
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions,
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let (engine, session) =
            Engine::new_partial_replica(storage.clone(), EngineOptions::new(), &old)
                .await
                .unwrap();
        let engine = Arc::new(engine);
        engine
            .sync_mode()
            .admit_partial_replica(old.clone(), crate::sync::partial_replica_write_capability());
        storage.admit_partial_replica_writer(crate::sync::partial_replica_write_capability());
        let sql = "SELECT value FROM lix_key_value WHERE key='arrives-later'";
        assert!(
            execute_hydrating(
                &session,
                &storage,
                &old,
                &authority,
                sql,
                &[],
                &mut Fetches::default()
            )
            .await
            .unwrap()
            .rows()
            .is_empty()
        );
        let client = WatchingAuthorityClient {
            base: AuthorityClient::new(authority.clone(), false),
            watches: Arc::default(),
            native_reads: Arc::default(),
            blocked: Arc::default(),
            changed: Arc::default(),
        };
        let transport = HttpSyncTransport::connect_with(client.clone(), old.remote_id())
            .await
            .unwrap();
        transport
            .bind_native_baseline_lease(old.baseline_lease())
            .unwrap();
        let (shutdown, shutdown_rx) =
            tokio::sync::watch::channel(crate::sync::runtime::SyncShutdown::Running);
        let (sender, receiver) = tokio::sync::mpsc::channel(4);
        let worker = crate::sync::partial_runtime::run_partial_worker_with_engine(
            storage.clone(),
            old.clone(),
            Some(transport),
            || Box::pin(async { Err(LixError::unknown("unexpected reconnect")) }),
            shutdown_rx,
            receiver,
            Some(engine.sync_mode().change_watcher()),
            Some(engine.clone()),
        );
        let caller = async {
            client.blocked.notified().await;
            let (response, done) = tokio::sync::oneshot::channel();
            sender
                .send(crate::sync::runtime::SyncDemand {
                    request: crate::sync::runtime::SyncDemandRequest::NativeMetadata(
                        NativeMetadataRef::CommitStateHeader(
                            old.descriptor().selected_branch.head.commit_id.clone(),
                        ),
                        LixError::unknown("resident foreground demand"),
                    ),
                    response,
                })
                .await
                .unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(1), done)
                .await
                .expect("foreground demand must cancel blocked watch")
                .unwrap()
                .unwrap();
            client.blocked.notified().await;
            authority
                .execute(
                    "INSERT INTO lix_key_value (key,value) VALUES ('arrives-later','remote')",
                    &[],
                )
                .await
                .unwrap();
            client.changed.notify_one();
            while engine.sync_mode().partial_admission().as_deref() == Some(old.as_ref()) {
                tokio::task::yield_now().await;
            }
            let native_reads = client.native_reads.load(Ordering::SeqCst);
            assert!(
                native_reads > 0,
                "background candidate must hydrate authority inputs"
            );
            for _ in 0..10 {
                assert!(value(session.execute(sql, &[]).await.unwrap()).contains("remote"));
            }
            assert_eq!(
                client.native_reads.load(Ordering::SeqCst),
                native_reads,
                "warm direct SQL adds no native network requests"
            );
            assert_eq!(
                client.base.pushes.load(Ordering::SeqCst),
                0,
                "remote read publication creates no local pending edit"
            );
            shutdown.send_replace(crate::sync::runtime::SyncShutdown::Stop);
        };
        let (result, ()) = futures_util::join!(worker, caller);
        result.unwrap();
        assert!(client.watches.load(Ordering::SeqCst) >= 2);
    })
    .await
    .expect("live partial worker publication timed out");
}
