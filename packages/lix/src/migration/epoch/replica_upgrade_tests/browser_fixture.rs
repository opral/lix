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
