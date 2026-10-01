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
    let mut fetches = Fetches::default();
    for source in [
        format!("lix_diff('lix_file', '{before}', '{after}')"),
        "lix_diff('lix_file')".into(),
        format!("lix_history('lix_file', '{after}')"),
    ] {
        let sql = format!(
            "SELECT from_content, to_content FROM {source} WHERE id = '{id}' AND diff_type = 'modified'"
        );
        let result = execute_hydrating(
            &session,
            &storage,
            &state,
            &authority,
            &sql,
            &[],
            &mut fetches,
        )
        .await
        .unwrap();
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
    assert_eq!(
        fetches.blob_manifest_ids,
        allowed.into_iter().collect(),
        "only the selected endpoints' manifests should be fetched"
    );
    assert_eq!(
        fetches.chunk_ids, allowed_chunks,
        "only selected endpoint chunks should be fetched"
    );
    session.close().await.unwrap();
    authority.close().await.unwrap();
}

#[tokio::test]
async fn descriptor_only_preview_hydrates_only_content_within_the_case_limit() {
    let authority = open_lix().with_storage(Memory::new()).await.unwrap();
    let large_id = "0193182b-2a72-7ed5-9015-76bf271af334";
    let small_id = "0193182b-2a72-7ed5-9015-76bf271af335";
    let large_bytes = vec![b'l'; 300 * 1024];
    let small_bytes = b"small preview".to_vec();
    authority
        .execute(
            "INSERT INTO lix_file (id, path, content) VALUES ($1, '/preview-large', $2), ($3, '/preview-small', $4)",
            &[
                Value::Text(large_id.into()),
                Value::Blob(large_bytes.clone().into()),
                Value::Text(small_id.into()),
                Value::Blob(small_bytes.clone().into()),
            ],
        )
        .await
        .unwrap();
    let large_blob_id = crate::binary_cas::BlobId::from_content(&large_bytes).to_hex();
    let small_blob_id = crate::binary_cas::BlobId::from_content(&small_bytes).to_hex();
    let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
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

    for (label, sql, params) in [
        (
            "registered schema projection",
            "SELECT schema_key, value, lixcol_file_id, lixcol_metadata, \
             lixcol_created_at, lixcol_updated_at, lixcol_global, lixcol_change_id, \
             lixcol_author_id, lixcol_commit_id, lixcol_untracked \
             FROM lix_registered_schema LIMIT $1 OFFSET $2",
            vec![Value::Integer(10), Value::Integer(0)],
        ),
        (
            "working diff count",
            "SELECT count(*) AS file_count FROM lix_diff('lix_file')",
            Vec::new(),
        ),
        (
            "active account projection",
            "SELECT id, name FROM lix_account WHERE id = lix_active_account_id()",
            Vec::new(),
        ),
    ] {
        execute_hydrating(
            &session,
            &storage,
            &state,
            &authority,
            sql,
            &params,
            &mut Fetches::default(),
        )
        .await
        .unwrap_or_else(|error| panic!("partial {label} query failed: {error:?}"));
    }

    let mut fetches = Fetches::default();
    let result = execute_hydrating(
        &session,
        &storage,
        &state,
        &authority,
        "SELECT id, path, CASE WHEN OCTET_LENGTH(content) <= $1 THEN content END AS content, \
         OCTET_LENGTH(content) AS size_bytes FROM lix_file \
         WHERE id IN ($2, $3) ORDER BY path",
        &[
            Value::Integer(1024),
            Value::Text(large_id.into()),
            Value::Text(small_id.into()),
        ],
        &mut fetches,
    )
    .await
    .unwrap();
    assert_eq!(result.len(), 2);
    let large_row = result
        .rows()
        .iter()
        .find(|row| row.get::<String>("id").unwrap() == large_id)
        .expect("large file row");
    assert_eq!(large_row.get::<Value>("content").unwrap(), Value::Null);
    assert_eq!(
        large_row.get::<i64>("size_bytes").unwrap(),
        i64::try_from(large_bytes.len()).unwrap()
    );
    let small_row = result
        .rows()
        .iter()
        .find(|row| row.get::<String>("id").unwrap() == small_id)
        .expect("small file row");
    assert_eq!(
        small_row.get::<Vec<u8>>("content").unwrap(),
        small_bytes
    );
    assert_eq!(
        small_row.get::<i64>("size_bytes").unwrap(),
        i64::try_from(small_bytes.len()).unwrap()
    );
    assert_eq!(fetches.blob_manifest_ids, BTreeSet::from([small_blob_id]));
    assert!(!fetches.blob_manifest_ids.contains(&large_blob_id));
    session.close().await.unwrap();
    authority.close().await.unwrap();
}
