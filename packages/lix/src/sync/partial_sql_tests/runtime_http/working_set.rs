//! Actual HTTP delivery, candidate publication, then disconnected reads/writes.
use super::*;

#[derive(Clone)]
struct CountDelivery {
    inner: Client,
    requests: Arc<std::sync::Mutex<Vec<String>>>,
    offline: Arc<AtomicBool>,
}
impl RawHttpClient for CountDelivery {
    fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
        self.requests.lock().unwrap().push(request.url.clone());
        if self.offline.load(Ordering::SeqCst) {
            return Box::pin(async { Err(LixError::unknown("offline delivery regression")) });
        }
        self.inner.send(request)
    }
}

#[tokio::test]
async fn delivered_working_set_publishes_without_hydration_and_remains_writable_offline() {
    let backing = Memory::new();
    let authority = open_lix().with_storage(backing.clone()).await.unwrap();
    authority
        .set_sync_role(crate::sync::SyncRole::Authority)
        .unwrap();
    authority.execute("INSERT INTO lix_file (id,path,content) VALUES ('00000000-0000-7000-8000-000000000123','/a.txt',CAST('a' AS BYTEA)),('00000000-0000-7000-8000-000000000124','/b.txt',CAST('b' AS BYTEA))", &[]).await.unwrap();
    let server = open_lix()
        .with_storage(backing)
        .serve()
        .with_embedded_lix_id()
        .await
        .unwrap();
    let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let offline = Arc::new(AtomicBool::new(false));
    let transport = HttpSyncTransport::connect_with(
        CountDelivery {
            inner: Client {
                server,
                lose_body: Arc::new(AtomicBool::new(false)),
            },
            requests: requests.clone(),
            offline: offline.clone(),
        },
        &format!("https://example.test/lix/{}", authority.lix_id()),
    )
    .await
    .unwrap();
    let wrapper = transport.partial_replica_descriptor(None).await.unwrap();
    let transport = transport
        .fork_native_baseline_lease(&wrapper.wire.lease)
        .unwrap();
    let old = Arc::new(
        PartialReplicaState::from_leased(
            transport.protocol_url().into(),
            authority.active_account_id().into(),
            uuid::Uuid::now_v7().to_string(),
            wrapper.wire,
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
    for sql in [
        "SELECT content FROM lix_file WHERE path='/a.txt'",
        "SELECT content FROM lix_file WHERE path='/b.txt'",
    ] {
        let mut complete = false;
        for _ in 0..128 {
            match session.execute(sql, &[]).await {
                Ok(_) => {
                    complete = true;
                    break;
                }
                Err(error) => {
                    let demand = crate::sync::runtime::native_sync_demand_request_for_error(&error)
                        .unwrap()
                        .unwrap_or_else(|| panic!("unexpected initial read: {error:?}"));
                    crate::sync::partial_runtime::hydrate_demand(
                        &storage, &old, &transport, demand,
                    )
                    .await
                    .unwrap();
                }
            }
        }
        assert!(complete);
    }
    let interests = engine
        .sync_mode()
        .read_interests()
        .unwrap()
        .snapshot()
        .unwrap();
    assert!(!interests.interests.is_empty());
    crate::sync::admit_sync_authority_storage(&authority.storage_adapter(), None)
        .await
        .unwrap();
    authority
        .execute(
            "UPDATE lix_file SET content=CAST('new' AS BYTEA) WHERE path='/a.txt'",
            &[],
        )
        .await
        .unwrap();
    requests.lock().unwrap().clear();
    let request = crate::sync::partial_update::PartialUpdateRequest {
        branch_id: old.descriptor().selected_branch.branch_id.clone(),
        after: None,
        known_cursor: old.descriptor().cursor,
        interests: interests
            .interests
            .iter()
            .map(|interest| interest.as_ref().clone())
            .collect(),
    };
    let (wrapper, bundle) = transport.partial_replica_update(&request).await.unwrap();
    assert!(bundle.complete, "small working set must arrive complete");
    assert!(!bundle.blobs.is_empty());
    crate::sync::partial_update::install_bundle(&storage, &old, &bundle)
        .await
        .unwrap();
    let prepared = crate::sync::partial_reconcile::prepare_clean_descriptor(
        engine.clone(),
        old.clone(),
        &transport,
        wrapper,
        crate::sync::partial_publication::PartialRecoveryPolicy::Normal,
    )
    .await
    .unwrap();
    let crate::sync::partial_reconcile::PreparedDescriptor::Ready(prepared) = prepared else {
        panic!("changed loaded file must publish")
    };
    crate::sync::partial_publication::publish_prepared_partial(engine.clone(), prepared)
        .await
        .unwrap();
    assert_eq!(
        requests.lock().unwrap().len(),
        1,
        "publication must not discover more native dependencies"
    );
    assert!(requests.lock().unwrap()[0].ends_with("/sync/update"));
    offline.store(true, Ordering::SeqCst);
    // No hydration/retry helper after adoption: every read/write must already
    // have its complete native dependencies, including the unchanged loaded file.
    let rows = session
        .execute(
            "SELECT CAST(content AS TEXT) AS content FROM lix_file WHERE path='/a.txt'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.rows()[0].get::<String>("content").unwrap(), "new");
    session
        .execute("SELECT content FROM lix_file WHERE path='/b.txt'", &[])
        .await
        .unwrap();
    session
        .execute(
            "UPDATE lix_file SET content=CAST('local' AS BYTEA) WHERE path='/a.txt'",
            &[],
        )
        .await
        .unwrap();
    session
        .execute(
            "UPDATE lix_file SET content=CAST('other' AS BYTEA) WHERE path='/b.txt'",
            &[],
        )
        .await
        .unwrap();
    let local = session
        .execute(
            "SELECT CAST(content AS TEXT) AS content FROM lix_file WHERE path='/a.txt'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(local.rows()[0].get::<String>("content").unwrap(), "local");
    assert_eq!(requests.lock().unwrap().len(), 1);
}
