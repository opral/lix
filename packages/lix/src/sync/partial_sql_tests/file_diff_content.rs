//! Descriptor-only replicas materialize only the selected historical file.
use super::*;

#[tokio::test]
async fn descriptor_only_file_diff_and_history_hydrate_selected_bytes() {
    let authority = open_lix().with_storage(Memory::new()).await.unwrap();
    let id = "0193182b-2a72-7ed5-9015-76bf271af333";
    let before_bytes = vec![b'a'; 300 * 1024];
    let after_bytes = vec![b'b'; 300 * 1024];
    authority
        .execute(
            "INSERT INTO lix_file (id, path, content) VALUES ($1, '/selected', $2)",
            &[
                Value::Text(id.into()),
                Value::Blob(before_bytes.clone().into()),
            ],
        )
        .await
        .unwrap();
    let before = authority
        .execute("SELECT commit_id FROM lix_create_checkpoint(NULL, NULL)", &[])
        .await
        .unwrap()
        .rows()[0]
        .get::<String>("commit_id")
        .unwrap();
    authority
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/unrelated.asset', $1)",
            &[Value::Blob(vec![0xab; 40 * 1024 * 1024].into())],
        )
        .await
        .unwrap();
    authority
        .execute(
            "UPDATE lix_file SET content = $1 WHERE id = $2",
            &[
                Value::Blob(after_bytes.clone().into()),
                Value::Text(id.into()),
            ],
        )
        .await
        .unwrap();
    let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
    let after = descriptor.selected_branch.head.commit_id.clone();
    let state = PartialReplicaState::new(
        format!("https://example.test/lix/{}", authority.lix_id()),
        crate::ANONYMOUS_ACCOUNT_ID.into(),
        uuid::Uuid::now_v7().to_string(),
        descriptor,
    )
    .unwrap();
    let storage = StorageAdapter::new(Memory::new());
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
        std::sync::Arc::new(state.clone()),
        super::super::partial_replica_write_capability(),
    );
    storage.admit_partial_replica_writer(super::super::partial_replica_write_capability());
    let allowed = [&before_bytes, &after_bytes]
        .map(|bytes| crate::binary_cas::BlobId::from_content(bytes).to_hex());
    let allowed_chunks = [&before_bytes, &after_bytes]
        .into_iter()
        .flat_map(|bytes| {
            crate::binary_cas::CanonicalBlobManifest::from_bytes(bytes)
                .chunks
                .into_iter()
                .map(|chunk| chunk.hash.to_hex())
        })
        .collect::<BTreeSet<_>>();
    let mut blob_fetches = 0;
    let mut chunk_fetches = 0;
    for source in [
        format!("lix_diff('lix_file', '{before}', '{after}')"),
        "lix_diff('lix_file')".into(),
        format!("lix_history('lix_file', '{after}')"),
    ] {
        let sql = format!(
            "SELECT from_content, to_content FROM {source} WHERE id = '{id}' AND diff_type = 'modified'"
        );
        let mut fetches = Fetches::default();
        let mut attempts = 0;
        let result = loop {
            attempts += 1;
            assert!(attempts < 20, "content hydration must make progress");
            let error = match execute_hydrating(
                &session,
                &storage,
                &state,
                &authority,
                &sql,
                &[],
                &mut fetches,
            )
            .await
            {
                Ok(result) => break result,
                Err(error) => error,
            };
            let mut writes = storage.new_write_set();
            let read = storage.begin_read(Default::default()).await.unwrap();
            let mut preconditions = Vec::new();
            match error.code.as_str() {
                "LIX_PARTIAL_BLOB_MANIFEST_REQUIRED" => {
                    let demand = crate::binary_cas::BlobManifestRequired::from_error(&error)
                        .unwrap()
                        .unwrap();
                    let id = demand.0.to_hex();
                    assert!(allowed.contains(&id), "unrelated blob demanded: {id}");
                    let wire = authority
                        .get_sync_blob_manifest(&id)
                        .await
                        .unwrap()
                        .unwrap();
                    let manifest = super::super::blob::decode_manifest(&wire).unwrap();
                    crate::binary_cas::stage_deferred_canonical_manifest(
                        &read,
                        &mut writes,
                        &manifest,
                    )
                    .await
                    .unwrap();
                    blob_fetches += 1;
                }
                "LIX_SYNC_CHUNKS_REQUIRED" => {
                    for value in error.details.as_ref().unwrap()["chunkIds"]
                        .as_array()
                        .unwrap()
                    {
                        let id = value.as_str().unwrap();
                        assert!(
                            allowed_chunks.contains(id),
                            "unrelated chunk demanded: {id}"
                        );
                        let bytes = authority.get_sync_chunk(id).await.unwrap().unwrap();
                        crate::binary_cas::stage_verified_raw_chunk(
                            &mut writes,
                            crate::binary_cas::ChunkHash::from_hex(id).unwrap(),
                            &bytes,
                        )
                        .unwrap();
                        chunk_fetches += 1;
                    }
                }
                _ => panic!("unexpected content demand: {error:?}"),
            }
            crate::binary_cas::stage_transfer_publication_fence(
                &read,
                &mut writes,
                &mut preconditions,
            )
            .await
            .unwrap();
            drop(read);
            storage
                .commit_partial_replica_write_set(
                    super::super::partial_replica_write_capability(),
                    writes,
                    StorageWriteOptions {
                        preconditions,
                        await_durable: true,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
        };
        assert_eq!(result.len(), 1, "{source}");
        assert_eq!(
            result.rows()[0].get::<Vec<u8>>("from_content").unwrap(),
            before_bytes
        );
        assert_eq!(
            result.rows()[0].get::<Vec<u8>>("to_content").unwrap(),
            after_bytes
        );
    }
    assert_eq!(blob_fetches, 2, "each selected endpoint fetched once");
    assert!(chunk_fetches >= 2);
    session.close().await.unwrap();
    authority.close().await.unwrap();
}
