//! Synthetic sparse v77 replica fixture for real app/OPFS admission QA.
//! This is the existing supported sparse-upgrade shape, not bytes produced by
//! a historical codec: historical commit bodies are intentionally absent.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual local browser fixture; requires manifest and stop paths"]
async fn legacy_browser_app_fixture_authority() {
    let manifest_path =
        std::env::var("LIX_BROWSER_LEGACY_MANIFEST").expect("set a synthetic fixture output path");
    let stop_path =
        std::env::var("LIX_BROWSER_LEGACY_STOP").expect("set a synthetic fixture stop path");
    assert!(!std::path::Path::new(&stop_path).exists());
    let pending = std::env::var("LIX_BROWSER_LEGACY_PENDING").as_deref() == Ok("1");
    let authority = Authority::new_with_browser_files(true).await;
    let storage = old_replica_with_recovery_data(
        &authority,
        EpochBank::Legacy,
        pending,
        pending,
        crate::Memory::new(),
    )
    .await;
    let adapter = StorageAdapter::for_epoch_unfenced(storage, EpochBank::Legacy);
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let mut entries = Vec::new();
    for &space in crate::storage_spaces::ALL_STORAGE_SPACES {
        let mut cursor = read
            .begin_scan(
                space,
                KeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                Default::default(),
            )
            .await
            .unwrap();
        loop {
            let (page, more) = cursor.next_page(256).await.unwrap().into_parts();
            for entry in page {
                let ProjectedValue::FullValue(value) = entry.value else {
                    panic!("fixture requires complete values");
                };
                entries.push(serde_json::json!({
                    "space": space.id.0,
                    "key": entry.key.0.to_vec(),
                    "value": value.to_vec(),
                }));
            }
            if !more {
                break;
            }
        }
    }
    drop(read);
    drop(adapter);
    std::fs::write(
        &manifest_path,
        serde_json::to_vec(&serde_json::json!({
            "url": authority.url,
            "repositoryId": authority.url.rsplit('/').next().unwrap(),
            "fixtureKind": "synthetic-sparse-v77-full-sync",
            "pending": pending,
            "customSchema": "legacy_custom_note",
            "customRow": { "id": "legacy", "value": "custom-preserved" },
            "files": [
                { "path": "/legacy-small.bin", "length": 3, "bytes": [1, 2, 3] },
                { "path": "/legacy-large.bin", "length": 300 * 1024, "byteModulo": 251 }
            ],
            "entries": entries,
        }))
        .unwrap(),
    )
    .unwrap();
    eprintln!("Legacy sparse replica browser fixture ready: {manifest_path}");
    while !std::path::Path::new(&stop_path).exists() {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Synthetic logical v2 partial-replica fixture for the actual app opener.
/// Its rows are seeded by the historical OPFS writer, but this is not a
/// historical v2 codec export.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual local browser fixture; requires manifest and stop paths"]
async fn v2_partial_browser_app_fixture_authority() {
    let manifest_path = std::env::var("LIX_BROWSER_V2_PARTIAL_MANIFEST")
        .expect("set a synthetic v2 partial fixture output path");
    let stop_path = std::env::var("LIX_BROWSER_V2_PARTIAL_STOP")
        .expect("set a synthetic v2 partial fixture stop path");
    assert!(!std::path::Path::new(&stop_path).exists());

    let authority = Authority::new().await;
    let authenticated = crate::sync::authenticate_partial_conversion(authority.options(), None)
        .await
        .unwrap();
    let state = authenticated.state().clone();
    let storage = crate::sync::durable_memory_for_test(crate::Memory::new());
    let installed = install_fresh_partial_epoch(storage.clone(), &state)
        .await
        .unwrap();
    let (engine, session) =
        Engine::new_partial_replica(installed.adapter.clone(), EngineOptions::new(), &state)
            .await
            .unwrap();
    engine.sync_mode().admit_partial_replica(
        Arc::new(state.clone()),
        crate::sync::partial_replica_write_capability(),
    );
    installed
        .adapter
        .admit_partial_replica_writer(crate::sync::partial_replica_write_capability());
    crate::sync::execute_hydrating_over_http(
        &session,
        &installed.adapter,
        &state,
        authenticated.server(),
        "INSERT INTO lix_key_value (key,value) VALUES ('v2-browser-pending','local-pending')",
        &[],
    )
    .await
    .unwrap();
    drop(session);
    drop(engine);

    let journal_key =
        crate::storage_codec::id_string::uuid_bytes_from_canonical(state.epoch_id()).unwrap();
    let mut writes = installed.adapter.new_write_set();
    writes.put(
        crate::sync::PARTIAL_READ_INTEREST_SPACE,
        journal_key.as_slice(),
        serde_json::to_vec(&serde_json::json!({
            "version": 2,
            "epochId": state.epoch_id(),
            "recipes": [{
                "kind": "filesystem_paths",
                "file_ids": [],
                "branch_ids": [state.descriptor().selected_branch.branch_id],
                "include_blob_refs": false,
                "cache_small_blob_data": false
            }]
        }))
        .unwrap(),
    );
    let mut write = installed
        .adapter
        .begin_migration_write(WriteOptions::default())
        .await
        .unwrap();
    writes.lower_into(&mut write).await.unwrap();
    write.commit().await.unwrap();
    stage_repository_format_for_test(&storage, true, 82)
        .await
        .unwrap();

    let current = inspect_existing_epoch_adapter(&storage).await.unwrap();
    let read = current.begin_read(Default::default()).await.unwrap();
    let mut entries = Vec::new();
    for &space in crate::storage_spaces::ALL_STORAGE_SPACES {
        let mut cursor = read
            .begin_scan(
                space,
                KeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                Default::default(),
            )
            .await
            .unwrap();
        loop {
            let (page, more) = cursor.next_page(256).await.unwrap().into_parts();
            for entry in page {
                let ProjectedValue::FullValue(value) = entry.value else {
                    panic!("fixture requires complete values");
                };
                entries.push(serde_json::json!({
                    "space": space.id.0,
                    "key": entry.key.0.to_vec(),
                    "value": value.to_vec(),
                }));
            }
            if !more {
                break;
            }
        }
    }
    drop(read);
    drop(installed.adapter);
    std::fs::write(
        &manifest_path,
        serde_json::to_vec(&serde_json::json!({
            "url": authority.url,
            "repositoryId": state.repository_id(),
            "fixtureKind": "synthetic-v82-v2-partial-replica",
            "pendingRow": {"key": "v2-browser-pending", "value": "local-pending"},
            "entries": entries,
        }))
        .unwrap(),
    )
    .unwrap();
    eprintln!("Synthetic v2 partial browser fixture ready: {manifest_path}");
    while !std::path::Path::new(&stop_path).exists() {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
