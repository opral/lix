//! File size must not escape the awaited upload path as a preparation error.
use super::*;

#[derive(Clone)]
struct ChunkProbe {
    inner: Client,
    sizes: Arc<std::sync::Mutex<Vec<usize>>>,
}
impl RawHttpClient for ChunkProbe {
    fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
        if request.method == "PUT" && request.url.contains("/sync/chunk") {
            self.sizes
                .lock()
                .unwrap()
                .push(request.body.as_ref().unwrap().len());
        }
        self.inner.send(request)
    }
}

#[tokio::test]
async fn file_above_former_64_mib_limit_uploads_bounded_chunks_and_reuses_remote_cache() {
    let backing = Memory::new();
    let authority = open_lix().with_storage(backing.clone()).await.unwrap();
    authority
        .set_sync_role(crate::sync::SyncRole::Authority)
        .unwrap();
    authority
        .execute(
            "INSERT INTO lix_file(path,content) VALUES('/large.bin',$1)",
            &[Value::Blob(bytes::Bytes::from_static(b"old").into())],
        )
        .await
        .unwrap();
    let server = open_lix()
        .with_storage(backing)
        .serve()
        .with_embedded_lix_id()
        .await
        .unwrap();
    let sizes = Arc::new(std::sync::Mutex::new(Vec::new()));
    let transport = HttpSyncTransport::connect_with(
        ChunkProbe {
            inner: Client {
                server,
                lose_body: Arc::new(AtomicBool::new(false)),
            },
            sizes: sizes.clone(),
        },
        &format!("https://example.test/lix/{}", authority.lix_id()),
    )
    .await
    .unwrap();
    let (engine, session, state) = {
        let (_, engine, session, state) =
            publication::fixture_from_authority(authority.clone(), None).await;
        (engine, session, state)
    };
    // Fixture uses the same authority identity; transfer admission is local.
    let storage = engine.storage();
    let content = bytes::Bytes::from(vec![43u8; 65 * 1024 * 1024]);
    execute_hydrating(
        &session,
        &storage,
        &state,
        &authority,
        "UPDATE lix_file SET content=$1 WHERE path='/large.bin'",
        &[Value::Blob(content.clone().into())],
        &mut Fetches::default(),
    )
    .await
    .unwrap();
    let captured = Arc::new(std::sync::Mutex::new(None));
    assert!(
        crate::sync::partial_upload_cycle::upload_partial_once(
            &storage,
            &state,
            &state.descriptor().selected_branch.branch_id,
            uuid::Uuid::now_v7().to_string(),
            32,
            1024 * 1024,
            |request| {
                *captured.lock().unwrap() = Some(request.clone());
                let (storage, state, transport) = (&storage, &state, &transport);
                async move {
                    crate::sync::partial_blob_upload::push_partial_with_blobs(
                        storage, state, transport, &request,
                    )
                    .await
                }
            },
        )
        .await
        .unwrap()
    );
    let first_count = sizes.lock().unwrap().len();
    assert!(first_count > 0);
    assert!(
        sizes
            .lock()
            .unwrap()
            .iter()
            .all(|size| *size <= 4 * 1024 * 1024)
    );
    let request = captured.lock().unwrap().take().unwrap();
    crate::sync::partial_blob_upload::prepare_partial_upload_blobs(
        &storage, &state, &transport, &request,
    )
    .await
    .unwrap();
    assert_eq!(
        sizes.lock().unwrap().len(),
        first_count,
        "cached remote chunks must not be uploaded again"
    );
    // Verify the entire accepted file with bounded reads; a single SQL cell
    // above 64 MiB deliberately exceeds the unrelated buffered-result budget.
    for (index, expected) in content.chunks(8 * 1024 * 1024).enumerate() {
        let start = (index * 8 * 1024 * 1024) as u64;
        let read = authority
            .read_file_content("/large.bin", Some(start..start + expected.len() as u64))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read.content().as_ref(), expected);
    }
    drop(engine);
}
