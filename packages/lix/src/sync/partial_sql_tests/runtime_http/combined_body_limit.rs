//! The combined optimization must respect a host's smaller body limit without
//! treating an ambiguous publication response as a rejected request.
use super::*;

const BODY_CAP: usize = 110 * 1024;

#[derive(Clone)]
struct Probe {
    inner: Client,
    requests: Arc<std::sync::Mutex<Vec<(String, usize, u16)>>>,
    lose_ack: Arc<AtomicBool>,
    pushes: Arc<std::sync::Mutex<Vec<crate::sync::SyncPushRequest>>>,
}
impl RawHttpClient for Probe {
    fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
        Box::pin(async move {
            let url = request.url.clone();
            if url.ends_with("/sync/push") {
                self.pushes
                    .lock()
                    .unwrap()
                    .push(serde_json::from_slice(request.body.as_ref().unwrap()).unwrap());
            }
            let size = request.body.as_ref().map_or(0, Vec::len);
            let publication = url.ends_with("/sync/push")
                || (!matches!(request.method.as_str(), "GET")
                    && (url.contains("/sync/blob") || url.contains("/sync/chunk")));
            let response = self.inner.send(request).await?;
            if publication {
                self.requests
                    .lock()
                    .unwrap()
                    .push((url.clone(), size, response.status));
            }
            if url.ends_with("/sync/push")
                && response.status == 200
                && self.lose_ack.swap(false, Ordering::SeqCst)
            {
                return Err(LixError::new(
                    "TEST_LOST_PUSH_ACK",
                    "accepted response was lost",
                ));
            }
            Ok(response)
        })
    }
}

#[tokio::test]
async fn configured_body_cap_falls_back_to_chunks_with_same_frozen_publication() {
    run(false).await;
}

#[tokio::test]
async fn ambiguous_combined_publication_does_not_fall_back() {
    run(true).await;
}

async fn run(lose_ack: bool) {
    let backing = Memory::new();
    let authority = open_lix().with_storage(backing.clone()).await.unwrap();
    authority
        .set_sync_role(crate::sync::SyncRole::Authority)
        .unwrap();
    authority
        .execute(
            "INSERT INTO lix_file(path,content) VALUES('/checkpoint.bin',$1)",
            &[Value::Blob(vec![17u8; 96 * 1024].into())],
        )
        .await
        .unwrap();
    let server = open_lix()
        .with_storage(backing)
        .serve()
        .with_options(crate::server_protocol::ServerProtocolOptions {
            max_request_body_bytes: if lose_ack { 1024 * 1024 } else { BODY_CAP },
            ..Default::default()
        })
        .with_embedded_lix_id()
        .await
        .unwrap();
    let publication_requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let pushes = Arc::new(std::sync::Mutex::new(Vec::new()));
    let transport = HttpSyncTransport::connect_with(
        Probe {
            inner: Client {
                server,
                lose_body: Arc::new(AtomicBool::new(false)),
            },
            requests: Arc::clone(&publication_requests),
            lose_ack: Arc::new(AtomicBool::new(lose_ack)),
            pushes: pushes.clone(),
        },
        &format!("https://example.test/lix/{}", authority.lix_id()),
    )
    .await
    .unwrap();
    let wrapper = transport.partial_replica_descriptor(None).await.unwrap();
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

    let mut fetches = Fetches::default();
    execute_hydrating(
        &session,
        &storage,
        &old,
        &authority,
        "SELECT id,path FROM lix_file WHERE path='/checkpoint.bin'",
        &[],
        &mut fetches,
    )
    .await
    .unwrap();
    let content = vec![43u8; 96 * 1024];
    execute_hydrating(
        &session,
        &storage,
        &old,
        &authority,
        "UPDATE lix_file SET content=$1 WHERE path='/checkpoint.bin'",
        &[Value::Blob(content.clone().into())],
        &mut fetches,
    )
    .await
    .unwrap();
    let branch = &old.descriptor().selected_branch.branch_id;
    let result = crate::sync::partial_upload_cycle::upload_partial_once(
        &storage,
        &old,
        branch,
        uuid::Uuid::now_v7().to_string(),
        32,
        1024 * 1024,
        |request| {
            let (storage, old, transport) = (&storage, &old, &transport);
            async move {
                crate::sync::partial_blob_upload::push_partial_with_blobs(
                    storage, old, transport, &request,
                )
                .await
            }
        },
    )
    .await;
    if lose_ack {
        assert_eq!(result.unwrap_err().code, "TEST_LOST_PUSH_ACK");
    } else {
        assert!(result.unwrap());
    }
    {
        let requests = publication_requests.lock().unwrap();
        if lose_ack {
            assert_eq!(
                requests.len(),
                1,
                "ambiguous completion must not trigger another send: {requests:?}"
            );
            assert!(requests[0].0.ends_with("/sync/push"));
            assert_eq!(requests[0].2, 200);
        } else {
            assert_eq!(
                requests.len(),
                5,
                "combined rejection, manifest, chunk, manifest, exact push: {requests:?}"
            );
            assert!(requests[0].0.ends_with("/sync/push"));
            assert!(requests[0].1 > BODY_CAP);
            assert_eq!(requests[0].2, 413);
            for (_, size, status) in &requests[1..] {
                assert!(*size <= BODY_CAP);
                assert!((200..300).contains(status));
            }
            assert!(requests.last().unwrap().0.ends_with("/sync/push"));
        }
    }
    {
        let pushes = pushes.lock().unwrap();
        assert_eq!(pushes.len(), if lose_ack { 1 } else { 2 });
        if !lose_ack {
            let mut original = pushes[0].clone();
            assert!(!original.inline_blobs.is_empty());
            original.inline_blobs.clear();
            assert_eq!(
                serde_json::to_value(&original).unwrap(),
                serde_json::to_value(&pushes[1]).unwrap(),
                "fallback retains the exact native publication tuple"
            );
        }
    }
    let actual = authority
        .execute(
            "SELECT content FROM lix_file WHERE path='/checkpoint.bin'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(actual.rows()[0].get::<Vec<u8>>("content").unwrap(), content);
    let read = storage.begin_read(Default::default()).await.unwrap();
    let (pending, _, _) =
        crate::sync::partial_push_state::load_partial_push_state(&read, &old, branch)
            .await
            .unwrap();
    assert_eq!(
        pending.prepared.is_some(),
        lose_ack,
        "only an acknowledged publication settles the frozen attempt"
    );
    drop(read);
    drop(session);
    drop(engine);
    authority.close().await.unwrap();
}
