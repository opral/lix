//! Child of partial_sql_tests: authentic native demand path plus existing
//! canonical blob transfer, starting from descriptor/bootstrap only.
use super::*;
use crate::binary_cas::{BlobId, BlobManifestRequired, ChunkHash};

#[derive(Default, Debug)]
struct FileFetches {
    native: Fetches,
    manifests: usize,
    chunks: usize,
    content_bytes: usize,
}

async fn execute_file_hydrating(
    session: &SessionContext<Memory>,
    storage: &StorageAdapter<Memory>,
    state: &PartialReplicaState,
    authority: &Lix<Memory>,
    sql: &str,
    params: &[Value],
    fetches: &mut FileFetches,
) -> Result<ExecuteResult, LixError> {
    let mut seen = BTreeSet::new();
    for _ in 0..128 {
        let error = match execute_hydrating(
            session,
            storage,
            state,
            authority,
            sql,
            params,
            &mut fetches.native,
        )
        .await
        {
            Ok(result) => return Ok(result),
            Err(error) => error,
        };
        if error.code == LixError::CODE_STORAGE_COMMIT_OUTCOME_UNKNOWN
            || error
                .details
                .as_ref()
                .and_then(|details| details.get("nonRetryableAfterCommit"))
                .and_then(serde_json::Value::as_bool)
                == Some(true)
        {
            return Err(error);
        }
        if let Some(BlobManifestRequired(hash)) = BlobManifestRequired::from_error(&error)? {
            if !seen.insert(format!("manifest:{}", hash.to_hex())) {
                return Err(error);
            }
            let wire = authority
                .get_sync_blob_manifest(&hash.to_hex())
                .await?
                .ok_or_else(|| LixError::unknown("authority lacks referenced blob"))?;
            let wire = serde_json::from_slice(&serde_json::to_vec(&wire).unwrap()).unwrap();
            super::super::partial_blob::install_manifest(storage, state, hash, &wire).await?;
            fetches.manifests += 1;
            continue;
        }
        if error.code == "LIX_SYNC_CHUNKS_REQUIRED" {
            let ids = error
                .details
                .as_ref()
                .and_then(|details| details.get("chunkIds"))
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| LixError::unknown("chunk demand lacks ids"))?;
            for id in ids {
                let id = id
                    .as_str()
                    .ok_or_else(|| LixError::unknown("invalid chunk demand"))?;
                let hash = ChunkHash::from_hex(id)?;
                if !seen.insert(format!("chunk:{id}")) {
                    return Err(error.clone());
                }
                let bytes = authority
                    .get_sync_chunk(id)
                    .await?
                    .ok_or_else(|| LixError::unknown("authority lacks requested chunk"))?;
                super::super::partial_blob::install_chunk(storage, state, hash, &bytes).await?;
                fetches.chunks += 1;
                fetches.content_bytes += bytes.len();
            }
            continue;
        }
        return Err(error);
    }
    Err(LixError::unknown("file SQL demand budget exhausted"))
}

fn content(result: ExecuteResult) -> Vec<u8> {
    assert_eq!(
        result.rows().len(),
        1,
        "referenced file must not become an empty result on missing inputs"
    );
    result.rows()[0].get::<Vec<u8>>("content").unwrap()
}

#[tokio::test]
#[ignore = "manual descriptor-only file SQL partial replica gate"]
async fn descriptor_only_file_content_reads_and_prepared_writes_remain_local() {
    for width in [16usize, 1600] {
        let authority = open_lix().await.unwrap();
        let values = (0..width)
            .map(|index| format!("('file-demand-{index:06}', 'unrelated')"))
            .collect::<Vec<_>>()
            .join(",");
        authority
            .execute(
                &format!("INSERT INTO lix_key_value (key, value) VALUES {values}"),
                &[],
            )
            .await
            .unwrap();
        let original = (0..96 * 1024)
            .map(|index| ((index * 37 + index / 251) % 256) as u8)
            .collect::<Vec<_>>();
        let unrelated = vec![231u8; 1024 * 1024];
        authority.execute("INSERT INTO lix_file (path, content) VALUES ('/target.bin', $1), ('/unrelated.bin', $2)", &[Value::Blob(original.clone().into()), Value::Blob(unrelated.clone().into())]).await.unwrap();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let descriptor = serde_json::from_slice(&serde_json::to_vec(&descriptor).unwrap()).unwrap();
        let state = PartialReplicaState::new(
            format!("https://example.test/lix/{}", authority.lix_id()),
            crate::ANONYMOUS_ACCOUNT_ID.into(),
            "00000000-0000-7000-8000-000000000399".into(),
            descriptor,
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
        let opened = Instant::now();
        let (engine, session) =
            Engine::new_partial_replica(storage.clone(), EngineOptions::new(), &state)
                .await
                .unwrap();
        engine.sync_mode().admit_partial_replica(
            std::sync::Arc::new(state.clone()),
            super::super::partial_replica_write_capability(),
        );
        storage.admit_partial_replica_writer(super::super::partial_replica_write_capability());
        let open_us = opened.elapsed().as_micros();
        let sql = "SELECT content FROM lix_file WHERE path = '/target.bin'";
        let mut cold = FileFetches::default();
        let started = Instant::now();
        assert_eq!(
            content(
                execute_file_hydrating(&session, &storage, &state, &authority, sql, &[], &mut cold)
                    .await
                    .unwrap()
            ),
            original
        );
        let cold_us = started.elapsed().as_micros();
        assert!(
            cold.manifests > 0,
            "fixture must exercise missing manifest rather than inline metadata bytes"
        );
        assert!(cold.chunks > 0);
        assert_eq!(cold.content_bytes, original.len());
        let read = storage.begin_read(Default::default()).await.unwrap();
        assert!(
            crate::binary_cas::load_metadata_many(&read, &[BlobId::from_content(&unrelated)])
                .await
                .unwrap()
                .into_vec()[0]
                .is_none(),
            "unrelated repository content must not be fetched"
        );
        drop(read);
        for _ in 0..3 {
            assert_eq!(content(session.execute(sql, &[]).await.unwrap()), original);
        }
        let mut prepared = original.clone();
        prepared[3] ^= 1;
        let update = "UPDATE lix_file SET content = $1 WHERE path = '/target.bin'";
        let mut preparation = FileFetches::default();
        let started = Instant::now();
        execute_file_hydrating(
            &session,
            &storage,
            &state,
            &authority,
            update,
            &[Value::Blob(prepared.clone().into())],
            &mut preparation,
        )
        .await
        .unwrap();
        let preparation_us = started.elapsed().as_micros();
        let mut spine = Fetches::default();
        prepare_baseline_jump_spines(&storage, &state, &authority, &mut spine)
            .await
            .unwrap();
        let mut warm_writes_us = Vec::new();
        for index in 0..3 {
            prepared[4 + index] ^= 1;
            let started = Instant::now();
            session
                .execute(update, &[Value::Blob(prepared.clone().into())])
                .await
                .unwrap();
            warm_writes_us.push(started.elapsed().as_micros());
            assert_eq!(content(session.execute(sql, &[]).await.unwrap()), prepared);
        }
        let authority = std::sync::Arc::new(authority);
        let client = super::worker::AuthorityClient::new(authority.clone(), false);
        let transport =
            crate::sync::http::HttpSyncTransport::connect_with(client.clone(), state.remote_id())
                .await
                .unwrap();
        let controls_before_ack = admitted_controls(&storage, &state).await.unwrap();
        assert!(
            crate::sync::partial_upload_cycle::upload_partial_once(
                &storage,
                &state,
                &state.descriptor().selected_branch.branch_id,
                "00000000-0000-7000-8000-000000004499".into(),
                32,
                1024 * 1024,
                |request| {
                    let storage = &storage;
                    let state = &state;
                    let transport = &transport;
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
        assert_eq!(
            admitted_controls(&storage, &state).await.unwrap(),
            controls_before_ack
        );
        assert_eq!(content(session.execute(sql, &[]).await.unwrap()), prepared);
        assert_eq!(
            content(authority.execute(sql, &[]).await.unwrap()),
            prepared
        );
        assert!(client.chunks.load(std::sync::atomic::Ordering::SeqCst) > 0);
        eprintln!(
            "partial file upload width={width} pushes={} chunks={}",
            client.pushes.load(std::sync::atomic::Ordering::SeqCst),
            client.chunks.load(std::sync::atomic::Ordering::SeqCst)
        );
        session.close().await.unwrap();
        drop(session);
        drop(engine);
        drop(storage);
        let storage = StorageAdapter::new(memory);
        let (engine, reopened) =
            Engine::new_partial_replica(storage.clone(), EngineOptions::new(), &state)
                .await
                .unwrap();
        engine.sync_mode().admit_partial_replica(
            std::sync::Arc::new(state.clone()),
            super::super::partial_replica_write_capability(),
        );
        storage.admit_partial_replica_writer(super::super::partial_replica_write_capability());
        assert_eq!(content(reopened.execute(sql, &[]).await.unwrap()), prepared);
        eprintln!(
            "partial file SQL width={width} open_us={open_us} cold_us={cold_us} cold={cold:?} preparation_us={preparation_us} preparation={preparation:?} spine={spine:?} warm_writes_us={warm_writes_us:?}"
        );
    }
}
