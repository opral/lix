mod common;

use common::{ORIGINAL_RUST_SOURCE, UPDATED_RUST_SOURCE, file_changes, file_id_for_path};
use lix_sdk::FsWriteOptions;

#[tokio::test]
async fn detects_semantic_entity_changes_from_write_file() {
    let lix = common::open_lix_with_sem_plugin().await;

    lix.write_file(
        "/src/lib.rs",
        ORIGINAL_RUST_SOURCE.to_vec(),
        FsWriteOptions::default(),
    )
    .await
    .unwrap();
    let file_id = file_id_for_path(&lix, "/src/lib.rs").await;
    let initial_changes = file_changes(&lix, &file_id).await;
    assert!(initial_changes.iter().any(|change| {
        change.schema_key == "sem_entity"
            && change.snapshot_content.as_ref().is_some_and(|snapshot| {
                snapshot
                    .get("entity_type")
                    .and_then(serde_json::Value::as_str)
                    == Some("function")
                    && snapshot
                        .get("content")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|content| content.contains("Hello, {}!"))
            })
    }));

    let changes_before_update = file_changes(&lix, &file_id).await;
    lix.write_file(
        "/src/lib.rs",
        UPDATED_RUST_SOURCE.to_vec(),
        FsWriteOptions::default(),
    )
    .await
    .unwrap();

    let update_changes = file_changes(&lix, &file_id)
        .await
        .into_iter()
        .skip(changes_before_update.len())
        .collect::<Vec<_>>();
    assert!(update_changes.iter().any(|change| {
        change.schema_key == "sem_entity"
            && change.snapshot_content.as_ref().is_some_and(|snapshot| {
                snapshot
                    .get("entity_type")
                    .and_then(serde_json::Value::as_str)
                    == Some("function")
                    && snapshot
                        .get("content")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|content| content.contains("Hi, {}!"))
            })
    }));

    lix.close().await.unwrap();
}
