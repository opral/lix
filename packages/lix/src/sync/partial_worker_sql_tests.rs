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
            } else if url.path().ends_with("/sync/native-metadata-walk") {
                let body = serde_json::from_slice(request.body.as_ref().unwrap()).unwrap();
                self.metadata.fetch_add(1, Ordering::SeqCst);
                serde_json::to_value(self.authority.read_sync_native_metadata_walk(&body).await?)
                    .unwrap()
            } else if url.path().ends_with("/sync/native-metadata") {
                let body = serde_json::from_slice(request.body.as_ref().unwrap()).unwrap();
                self.metadata.fetch_add(1, Ordering::SeqCst);
                serde_json::to_value(self.authority.read_sync_native_metadata(&body).await?)
                    .unwrap()
            } else if url.path().ends_with("/sync/retained-bodies") {
                let body = serde_json::from_slice(request.body.as_ref().unwrap()).unwrap();
                serde_json::to_value(
                    Box::pin(self.authority.push_retained_body_wave_for_account(
                        &body,
                        self.authority.active_account_id(),
                    ))
                    .await?,
                )
                .unwrap()
            } else if url.path().ends_with("/sync/merge") {
                let body = serde_json::from_slice(request.body.as_ref().unwrap()).unwrap();
                let authority = self.authority.clone();
                // A real HTTP authority runs independently from the client.
                // Keep its commit poll off the client's recovery stack too.
                let receipt =
                    tokio::spawn(async move {
                        Box::pin(authority.merge_partial_replica_for_account(
                            &body,
                            authority.active_account_id(),
                        ))
                        .await
                    })
                    .await
                    .unwrap()?;
                serde_json::to_value(receipt).unwrap()
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
                        vec![NativeMetadataRef::CommitGraphRecord(
                            state.descriptor().selected_branch.head.commit_id.clone(),
                        )],
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
                if push.prepared.is_none() && push.confirmed.head == control.head_commit_id {
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
                    vec![NativeMetadataRef::CommitGraphRecord(
                        state.descriptor().selected_branch.head.commit_id.clone(),
                    )],
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
                let descriptor = self
                    .base
                    .authority
                    .leased_partial_replica_descriptor(branch.as_deref())
                    .await?;
                serde_json::to_value(descriptor).unwrap()
            } else if url.path().ends_with("/sync/native-object-range")
                || url.path().ends_with("/sync/native-metadata")
                || url.path().ends_with("/sync/native-metadata-walk")
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
                } else if url.path().ends_with("/sync/native-metadata-walk") {
                    let body = serde_json::from_slice(request.body.as_ref().unwrap()).unwrap();
                    serde_json::to_value(
                        self.base
                            .authority
                            .read_sync_native_metadata_walk_leased(&body, lease)
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
async fn engine_worker_retains_watch_across_demands_then_publishes_negative_scope() {
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
            for _ in 0..8 {
                let (response, done) = tokio::sync::oneshot::channel();
                sender
                    .send(crate::sync::runtime::SyncDemand {
                        request: crate::sync::runtime::SyncDemandRequest::NativeMetadata(
                            vec![NativeMetadataRef::CommitStateHeader(
                                old.descriptor().selected_branch.head.commit_id.clone(),
                            )],
                            LixError::unknown("resident foreground demand"),
                        ),
                        response,
                    })
                    .await
                    .unwrap();
                tokio::time::timeout(std::time::Duration::from_secs(1), done)
                    .await
                    .expect("foreground demand must complete while the watch is blocked")
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    client.watches.load(Ordering::SeqCst),
                    1,
                    "foreground work must retain the same descriptor request"
                );
            }
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
                "candidate preparation hydrates the changed scope on demand"
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
            // Adoption changes the admission basis and must create a new watch.
            while client.watches.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
            shutdown.send_replace(crate::sync::runtime::SyncShutdown::Stop);
        };
        let (result, ()) = futures_util::join!(worker, caller);
        result.unwrap();
        assert!(client.watches.load(Ordering::SeqCst) >= 2);
    })
    .await
    .expect("live partial worker publication timed out");
}

#[derive(Clone)]
struct ExpiredAuthorityClient {
    inner: WatchingAuthorityClient,
    expired_lease: String,
    descriptors: Arc<AtomicUsize>,
    expirations: Arc<AtomicUsize>,
}
impl RawHttpClient for ExpiredAuthorityClient {
    fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
        Box::pin(async move {
            let url = url::Url::parse(&request.url).unwrap();
            // Foreground tests stop at the demand result; keep subsequent
            // background reconciliation pending so the caller can assert it.
            if url.path().ends_with("/sync/descriptor")
                && url.query_pairs().any(|(key, _)| key == "after")
            {
                futures_util::future::pending::<()>().await;
            }
            if url.path().ends_with("/sync/baseline-lease/renew") {
                self.expirations.fetch_add(1, Ordering::SeqCst);
                return Err(LixError::new(
                    "LIX_PARTIAL_BASELINE_EXPIRED",
                    "test renewal expired",
                ));
            }
            if url.path().ends_with("/sync/descriptor") {
                assert!(!url.query_pairs().any(|(key, _)| key == "after"));
                self.descriptors.fetch_add(1, Ordering::SeqCst);
                let branch = url
                    .query_pairs()
                    .find(|(key, _)| key == "branchId")
                    .unwrap()
                    .1
                    .into_owned();
                let descriptor = self
                    .inner
                    .base
                    .authority
                    .leased_partial_replica_descriptor(Some(&branch))
                    .await?;
                return Ok(RawHttpResponse {
                    status: 200,
                    status_text: "OK".into(),
                    body: serde_json::to_vec(&descriptor).unwrap(),
                });
            }
            if url.path().contains("/sync/native-")
                && request.headers.iter().any(|(name, value)| {
                    name == "lix-native-baseline-lease" && value == &self.expired_lease
                })
            {
                self.expirations.fetch_add(1, Ordering::SeqCst);
                return Err(LixError::new(
                    "LIX_PARTIAL_BASELINE_EXPIRED",
                    "test authority expired the old baseline",
                ));
            }
            self.inner.send(request).await
        })
    }
}

async fn expired_foreground_read_recovers(advance_authority: bool, dirty: bool) {
    tokio::time::timeout(std::time::Duration::from_secs(15), Box::pin(async {
        let authority = Arc::new(open_lix().await.unwrap());
        authority
            .set_sync_role(crate::sync::SyncRole::Authority)
            .unwrap();
        authority
            .execute(
                "INSERT INTO lix_key_value (key,value) VALUES ('lease-read','before')",
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
        if dirty {
            execute_hydrating(
                &session,
                &storage,
                &old,
                &authority,
                "UPDATE lix_key_value SET value='local-pending' WHERE key='lease-read'",
                &[],
                &mut Fetches::default(),
            )
            .await
            .unwrap();
        }
        if advance_authority || dirty {
            authority
                .execute(
                    "UPDATE lix_key_value SET value='after' WHERE key='lease-read'",
                    &[],
                )
                .await
                .unwrap();
        }
        let client = ExpiredAuthorityClient {
            inner: WatchingAuthorityClient {
                base: AuthorityClient::new(authority.clone(), false),
                watches: Arc::default(),
                native_reads: Arc::default(),
                blocked: Arc::default(),
                changed: Arc::default(),
            },
            expired_lease: old.baseline_lease().lease_id.clone(),
            descriptors: Arc::default(),
            expirations: Arc::default(),
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
        let sql = "SELECT value FROM lix_key_value WHERE key='lease-read'";
        // Queue the first real SQL hydration demand before the worker can watch.
        let request = if dirty {
            let latest = authority.partial_replica_descriptor(None).await.unwrap();
            crate::sync::runtime::SyncDemandRequest::NativeMetadata(
                vec![NativeMetadataRef::CommitStateHeader(latest.selected_branch.head.commit_id)],
                LixError::unknown("nonresident history with local edits"),
            )
        } else {
            let error = session.execute(sql, &[]).await.unwrap_err();
            crate::sync::runtime::native_sync_demand_request_for_error(&error)
                .unwrap()
                .expect("cold SQL requires hydration")
        };
        let (response, done) = tokio::sync::oneshot::channel();
        sender
            .send(crate::sync::runtime::SyncDemand { request, response })
            .await
            .unwrap();
        let worker = crate::sync::partial_runtime::run_partial_worker_with_engine(
            storage.clone(),
            old.clone(),
            Some(transport),
            || Box::pin(async { Err(LixError::unknown("unexpected reconnect")) }),
            shutdown_rx,
            receiver,
            None,
            Some(engine.clone()),
        );
        let started = Instant::now();
        let caller = async {
            let result = done.await.unwrap();
            eprintln!("{}", serde_json::json!({"profile":"foreground_recovery", "dirty":dirty, "advanced":advance_authority || dirty, "elapsed_us":started.elapsed().as_micros(), "descriptors":client.descriptors.load(Ordering::SeqCst), "expired_requests":client.expirations.load(Ordering::SeqCst), "native_requests":client.inner.native_reads.load(Ordering::SeqCst), "result":result.as_ref().err().map(|e|e.code.as_str())}));
            if advance_authority || dirty {
                assert_eq!(
                    result
                        .expect_err("changed basis restarts SQL against new roots")
                        .code,
                    "LIX_PARTIAL_ADMISSION_CHANGED"
                );
            } else {
                result.expect("original demand must recover without exposing expiration");
            }
            assert!(client.descriptors.load(Ordering::SeqCst) >= 1);
            assert_eq!(client.expirations.load(Ordering::SeqCst), 1);
            assert_ne!(
                engine
                    .sync_mode()
                    .partial_admission()
                    .unwrap()
                    .baseline_lease()
                    .lease_id,
                old.baseline_lease().lease_id
            );
            loop {
                match session.execute(sql, &[]).await {
                    Ok(result) => {
                        assert!(value(result).contains(if dirty {
                            "local-pending"
                        } else if advance_authority {
                            "after"
                        } else {
                            "before"
                        }));
                        break;
                    }
                    Err(error) => {
                        let request =
                            crate::sync::runtime::native_sync_demand_request_for_error(&error)
                                .unwrap()
                                .unwrap_or_else(|| panic!("unexpected SQL error: {error:?}"));
                        let (response, done) = tokio::sync::oneshot::channel();
                        sender
                            .send(crate::sync::runtime::SyncDemand { request, response })
                            .await
                            .unwrap();
                        done.await.unwrap().unwrap();
                    }
                }
            }
            assert!(
                client.inner.native_reads.load(Ordering::SeqCst) > 0,
                "recovery must hydrate the original demand, not merely acknowledge publication"
            );
            shutdown.send_replace(crate::sync::runtime::SyncShutdown::Stop);
        };
        let (result, ()) = futures_util::join!(Box::pin(worker), Box::pin(caller));
        result.unwrap();
    }))
    .await
    .expect("expired foreground recovery timed out");
}

#[tokio::test]
async fn expired_foreground_read_reacquires_unchanged_authority() {
    Box::pin(expired_foreground_read_recovers(false, false)).await;
}

#[tokio::test]
async fn expired_foreground_read_adopts_advanced_authority() {
    Box::pin(expired_foreground_read_recovers(true, false)).await;
}

#[tokio::test]
async fn expired_foreground_read_preserves_pending_local_edit() {
    Box::pin(expired_foreground_read_recovers(false, true)).await;
}

#[tokio::test]
async fn expired_background_renewal_refreshes_unchanged_authority_without_long_poll() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let authority = Arc::new(open_lix().await.unwrap());
        authority
            .set_sync_role(crate::sync::SyncRole::Authority)
            .unwrap();
        authority
            .execute(
                "INSERT INTO lix_key_value (key,value) VALUES ('lease-read','before')",
                &[],
            )
            .await
            .unwrap();
        let mut leased = authority
            .leased_partial_replica_descriptor(None)
            .await
            .unwrap();
        leased.lease.expires_at_ms = 1;
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
        let client = ExpiredAuthorityClient {
            inner: WatchingAuthorityClient {
                base: AuthorityClient::new(authority.clone(), false),
                watches: Arc::default(),
                native_reads: Arc::default(),
                blocked: Arc::default(),
                changed: Arc::default(),
            },
            expired_lease: old.baseline_lease().lease_id.clone(),
            descriptors: Arc::default(),
            expirations: Arc::default(),
        };

        let transport = HttpSyncTransport::connect_with(client.clone(), old.remote_id())
            .await
            .unwrap();
        transport
            .bind_native_baseline_lease(old.baseline_lease())
            .unwrap();
        let (shutdown, shutdown_rx) =
            tokio::sync::watch::channel(crate::sync::runtime::SyncShutdown::Running);
        let (_sender, receiver) = tokio::sync::mpsc::channel(4);
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
            while engine.sync_mode().partial_admission().as_deref() == Some(old.as_ref()) {
                tokio::task::yield_now().await;
            }
            assert_eq!(client.expirations.load(Ordering::SeqCst), 1);
            assert_eq!(
                client.descriptors.load(Ordering::SeqCst),
                1,
                "expired lease recovery requests a fresh descriptor without long polling"
            );
            assert_eq!(
                engine.sync_mode().partial_admission().unwrap().descriptor(),
                old.descriptor()
            );
            shutdown.send_replace(crate::sync::runtime::SyncShutdown::Stop);
        };
        let (result, ()) = futures_util::join!(worker, caller);
        result.unwrap();
        session.close().await.unwrap();
    })
    .await
    .expect("unchanged authority must not long-poll after lease expiration");
}
