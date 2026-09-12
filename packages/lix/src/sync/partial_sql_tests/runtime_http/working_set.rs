//! Actual HTTP delivery, candidate publication, then disconnected reads/writes.
use super::*;
use crate::storage_adapter::StorageAdapterRead as _;

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
    // Exercise the first locally authored ACK directly from initial admission,
    // before a remote publication has prepared any additional native inputs.
    offline.store(true, Ordering::SeqCst);
    session
        .execute(
            "UPDATE lix_file SET content=CAST('first-local' AS BYTEA) WHERE path='/a.txt'",
            &[],
        )
        .await
        .expect("initial prefetched file must be writable offline");
    offline.store(false, Ordering::SeqCst);
    verify_own_ack_delivery(storage.clone(), engine.clone(), transport.clone()).await;
    let old = engine.sync_mode().partial_admission().unwrap();
    let transport = transport
        .fork_native_baseline_lease(old.baseline_lease())
        .unwrap();
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
    // Deliver the authority representation back to the replica that authored
    // and durably acknowledged these commits. Metadata may already be local.
    offline.store(false, Ordering::SeqCst);
    verify_own_ack_delivery(storage, engine, transport).await;
}

async fn verify_own_ack_delivery(
    storage: StorageAdapter<Memory>,
    engine: Arc<Engine<Memory>>,
    transport: HttpSyncTransport<CountDelivery>,
) {
    let admitted = engine.sync_mode().partial_admission().unwrap();
    let transport = transport
        .fork_native_baseline_lease(admitted.baseline_lease())
        .unwrap();
    assert!(
        crate::sync::partial_upload_cycle::upload_partial_once(
            &storage,
            &admitted,
            &admitted.descriptor().selected_branch.branch_id,
            uuid::Uuid::now_v7().to_string(),
            32,
            1024 * 1024,
            |request| {
                let storage = &storage;
                let admitted = &admitted;
                let transport = &transport;
                async move {
                    crate::sync::partial_blob_upload::push_partial_with_blobs(
                        storage, admitted, transport, &request,
                    )
                    .await
                }
            },
        )
        .await
        .unwrap()
    );
    let interests = engine
        .sync_mode()
        .read_interests()
        .unwrap()
        .snapshot()
        .unwrap();
    let request = crate::sync::PartialUpdateRequest {
        branch_id: admitted.descriptor().selected_branch.branch_id.clone(),
        after: None,
        known_cursor: admitted.descriptor().cursor,
        interests: interests
            .interests
            .iter()
            .map(|interest| interest.as_ref().clone())
            .collect(),
    };
    let (wrapper, acknowledged_bundle) = transport
        .partial_replica_update(&request)
        .await
        .expect("own ACK update response must load");
    assert!(acknowledged_bundle.complete);
    let read = storage.begin_read(Default::default()).await.unwrap();
    let mut resident_count = 0;
    let mut resident_originals = Vec::new();
    let mut alternate_representations = Vec::new();
    for metadata in &acknowledged_bundle.metadata {
        if crate::sync::native_metadata::native_metadata_is_resident(
            &read,
            &admitted,
            &metadata.address,
        )
        .await
        .unwrap()
        {
            resident_count += 1;
            let space = match &metadata.address {
                NativeMetadataRef::CommitStateHeader(_) => {
                    crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE
                }
                NativeMetadataRef::CommitGraphRecord(_) => crate::changelog::COMMIT_SPACE,
                NativeMetadataRef::ChangeLocator(_) => {
                    crate::tracked_state::TRACKED_STATE_CHANGE_LOCATOR_SPACE
                }
            };
            let key = crate::storage_adapter::StorageKey(bytes::Bytes::copy_from_slice(
                uuid::Uuid::parse_str(metadata.address.id())
                    .unwrap()
                    .as_bytes(),
            ));
            let value = read
                .get_many(&[crate::storage_adapter::StorageGetManyRequest {
                    space,
                    keys: std::slice::from_ref(&key),
                    opts: Default::default(),
                }])
                .await
                .unwrap()
                .values
                .pop()
                .flatten()
                .unwrap();
            let crate::storage_adapter::StorageProjectedValue::FullValue(bytes) = value else {
                panic!("resident metadata omitted payload")
            };
            if bytes.as_ref() != metadata.bytes.as_slice() {
                alternate_representations.push(metadata.address.clone());
            }
            resident_originals.push((space, key, bytes));
        }
    }
    assert!(
        !alternate_representations.is_empty(),
        "own ACK must exercise different physical metadata representations"
    );
    eprintln!("own ACK alternate physical metadata: {alternate_representations:?}");
    assert!(
        resident_count > 0,
        "own ACK fixture must already contain native metadata"
    );
    let mut resident_objects = Vec::new();
    let mut alternate_objects = Vec::new();
    for object in &acknowledged_bundle.objects {
        let key =
            crate::storage_adapter::StorageKey(bytes::Bytes::from(object.address.storage_key()));
        let value = read
            .get_many(&[crate::storage_adapter::StorageGetManyRequest {
                space: object.address.space(),
                keys: std::slice::from_ref(&key),
                opts: Default::default(),
            }])
            .await
            .unwrap()
            .values
            .pop()
            .flatten();
        if let Some(crate::storage_adapter::StorageProjectedValue::FullValue(bytes)) = value {
            if bytes.as_ref() != object.bytes.as_slice() {
                alternate_objects.push(object.address);
            }
            resident_objects.push((object.address.space(), key, bytes));
        }
    }
    assert!(
        !alternate_objects.is_empty(),
        "own ACK must exercise alternate native physical objects"
    );
    assert!(alternate_objects.iter().all(|address| matches!(
        address,
        NativeObjectRef::MutationCatalog { .. } | NativeObjectRef::CommitDeltaPart { .. }
    )));
    eprintln!("own ACK alternate physical objects: {alternate_objects:?}");
    drop(read);
    crate::sync::partial_update::install_bundle(&storage, &admitted, &acknowledged_bundle)
        .await
        .expect("own acknowledged commit metadata must not make delivery fail");
    crate::sync::partial_update::install_bundle(&storage, &admitted, &acknowledged_bundle)
        .await
        .expect("repeated acknowledged delivery must remain idempotent");
    let read = storage.begin_read(Default::default()).await.unwrap();
    for (space, key, original) in resident_originals.into_iter().chain(resident_objects) {
        let actual = read
            .get_many(&[crate::storage_adapter::StorageGetManyRequest {
                space,
                keys: std::slice::from_ref(&key),
                opts: Default::default(),
            }])
            .await
            .unwrap()
            .values
            .pop()
            .flatten()
            .unwrap();
        assert_eq!(
            actual,
            crate::storage_adapter::StorageProjectedValue::FullValue(original),
            "delivery must preserve validated resident metadata"
        );
    }
    drop(read);
    let mut corrupt_delivery = acknowledged_bundle.clone();
    corrupt_delivery.metadata[0].bytes.clear();
    assert!(
        crate::sync::partial_update::install_bundle(&storage, &admitted, &corrupt_delivery)
            .await
            .is_err(),
        "malformed delivered metadata must fail even when resident"
    );
    let mut corrupt_object = acknowledged_bundle.clone();
    corrupt_object.objects[0].bytes.clear();
    assert!(
        crate::sync::partial_update::install_bundle(&storage, &admitted, &corrupt_object)
            .await
            .is_err(),
        "malformed delivered native object must fail even when resident"
    );
    let prepared = crate::sync::partial_global_merge_runtime::prepare_descriptor_with_global_merge(
        engine.clone(),
        admitted,
        &transport,
        wrapper,
        crate::sync::partial_publication::PartialRecoveryPolicy::Normal,
    )
    .await
    .expect("own ACK descriptor reconciliation must complete without backoff");
    match prepared {
        crate::sync::partial_reconcile::PreparedDescriptor::Ready(prepared) => {
            crate::sync::partial_publication::publish_prepared_partial(engine, prepared)
                .await
                .expect("own ACK candidate publication must complete");
        }
        crate::sync::partial_reconcile::PreparedDescriptor::NoChange => {}
        crate::sync::partial_reconcile::PreparedDescriptor::LocalProgress => {
            panic!("own ACK must not require another descriptor exchange")
        }
    }
}
