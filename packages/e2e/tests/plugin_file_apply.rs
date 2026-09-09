use lix::{ExecuteBatchStatement, Lix, Value, open_lix};
use std::io::{Cursor, Write};

async fn workspace() -> Lix {
    let lix = open_lix().await.unwrap();
    let wasm = std::fs::read(env!("CARGO_CDYLIB_FILE_PLUGIN_MARKDOWN_plugin_markdown")).unwrap();
    let mut archive = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, content) in [
        (
            "manifest.json",
            include_str!("../../../plugins/markdown/manifest.json").as_bytes(),
        ),
        (
            "schema/markdown_node.json",
            include_str!("../../../plugins/markdown/schema/markdown_node.json").as_bytes(),
        ),
        ("plugin.wasm", wasm.as_slice()),
    ] {
        archive.start_file(path, options).unwrap();
        archive.write_all(content).unwrap();
    }
    lix.execute(
        "INSERT INTO lix_file(path,content) VALUES($1,$2)",
        &[
            Value::Text("/.lix/plugins/plugin_markdown.lixplugin".into()),
            Value::Blob(archive.finish().unwrap().into_inner().into()),
        ],
    )
    .await
    .unwrap();
    lix
}

async fn head(lix: &Lix) -> String {
    lix.execute("SELECT lix_active_branch_commit_id() AS id", &[])
        .await
        .unwrap()
        .rows()[0]
        .get("id")
        .unwrap()
}

async fn apply(lix: &Lix, to: &str) {
    // Each apply creates new materialization versions. Build the next diff from
    // the current head, rather than reusing stale historical source row refs.
    let from = head(lix).await;
    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "INSERT INTO lix_apply(row_ref) SELECT row_ref FROM lix_diff('lix_file',$1,$2)",
        &[Value::Text(from), Value::Text(to.into())],
    )
    .await
    .expect("plugin-aware file apply must stage one blob reference per file");
    tx.commit().await.unwrap();
}

async fn content(lix: &Lix, path: &str) -> Option<Vec<u8>> {
    let result = lix
        .execute(
            "SELECT content FROM lix_file WHERE path=$1",
            &[Value::Text(path.into())],
        )
        .await
        .unwrap();
    result.rows().first().map(|row| row.get("content").unwrap())
}

#[tokio::test]
async fn apply_plugin_file_undo_redo_preserves_mixed_binary_files() {
    let lix = workspace().await;
    lix.execute(
        "INSERT INTO lix_file(path,content) VALUES('/a.md',$1),('/raw.bin',$2)",
        &[
            Value::Blob(b"seed 0000\n".to_vec().into()),
            Value::Blob(vec![0, 255, 1].into()),
        ],
    )
    .await
    .unwrap();
    let before = head(&lix).await;
    lix.execute(
        "UPDATE lix_file SET content=$1 WHERE path='/a.md'",
        &[Value::Blob(b"agent 0001\n".to_vec().into())],
    )
    .await
    .unwrap();
    lix.execute(
        "UPDATE lix_file SET content=$1 WHERE path='/raw.bin'",
        &[Value::Blob(vec![0, 254, 2].into())],
    )
    .await
    .unwrap();
    let after = head(&lix).await;
    for _ in 0..3 {
        apply(&lix, &before).await;
        assert_eq!(content(&lix, "/a.md").await, Some(b"seed 0000\n".to_vec()));
        assert_eq!(content(&lix, "/raw.bin").await, Some(vec![0, 255, 1]));
        let semantic = lix
            .execute(
                "SELECT payload_json FROM markdown_node WHERE kind='paragraph'",
                &[],
            )
            .await
            .unwrap();
        assert!(format!("{semantic:?}").contains("seed 0000"));
        apply(&lix, &after).await;
        assert_eq!(content(&lix, "/a.md").await, Some(b"agent 0001\n".to_vec()));
        assert_eq!(content(&lix, "/raw.bin").await, Some(vec![0, 254, 2]));
    }
    lix.close().await.unwrap();
}

#[tokio::test]
async fn apply_plugin_file_add_delete_restore_preserves_tombstones() {
    let lix = workspace().await;
    let empty = head(&lix).await;
    lix.execute(
        "INSERT INTO lix_file(path,content) VALUES('/a.md',$1)",
        &[Value::Blob(b"keep me\n".to_vec().into())],
    )
    .await
    .unwrap();
    let added = head(&lix).await;
    apply(&lix, &empty).await;
    assert_eq!(content(&lix, "/a.md").await, None);
    apply(&lix, &added).await;
    assert_eq!(content(&lix, "/a.md").await, Some(b"keep me\n".to_vec()));
    lix.execute("DELETE FROM lix_file WHERE path='/a.md'", &[])
        .await
        .unwrap();
    let deleted = head(&lix).await;
    apply(&lix, &added).await;
    assert_eq!(content(&lix, "/a.md").await, Some(b"keep me\n".to_vec()));
    apply(&lix, &deleted).await;
    assert_eq!(content(&lix, "/a.md").await, None);
    lix.close().await.unwrap();
}

#[tokio::test]
async fn restored_plugin_file_accepts_semantic_edits_but_not_public_owner_writes() {
    let lix = workspace().await;
    lix.execute(
        "INSERT INTO lix_file(path,content) VALUES('/a.md',$1)",
        &[Value::Blob(b"original\n".to_vec().into())],
    )
    .await
    .unwrap();
    let added = head(&lix).await;
    lix.execute("DELETE FROM lix_file WHERE path='/a.md'", &[])
        .await
        .unwrap();
    let deleted = head(&lix).await;
    let owner_only = lix.execute(
        "INSERT INTO lix_apply(row_ref) SELECT row_ref FROM lix_diff('lix_key_value',$1,$2) WHERE key='lix_plugin_owner_v2'",
        &[Value::Text(deleted), Value::Text(added.clone())],
    ).await.expect_err("selecting owner history alone must not authorize its restoration");
    assert!(
        owner_only
            .to_string()
            .contains("reserved for engine-managed plugin state")
    );
    apply(&lix, &added).await;
    lix.execute(
        "UPDATE markdown_node SET payload_json=$1 WHERE kind='paragraph'",
        &[Value::Text(
            serde_json::json!({"inline": [{"type": "text", "value": "Edited"}]}).to_string(),
        )],
    )
    .await
    .expect("restored plugin state must cold-open for a later semantic edit");
    assert_eq!(content(&lix, "/a.md").await, Some(b"Edited\n".to_vec()));
    let error = lix
        .execute(
            "INSERT INTO lix_key_value(key,value) VALUES('lix_plugin_owner_v2',$1)",
            &[Value::Text("{}".into())],
        )
        .await
        .expect_err("public owner writes must remain forbidden");
    assert!(
        error
            .to_string()
            .contains("reserved for engine-managed plugin state")
    );
    let batch_error = lix
        .execute_batch(
            &["lix_plugin_owner_v2", "lix_plugin_registry_v2"]
                .into_iter()
                .map(|key| ExecuteBatchStatement {
                    label: None,
                    sql: "INSERT INTO lix_key_value(key,value) VALUES($1,$2)".into(),
                    params: vec![Value::Text(key.into()), Value::Text("{}".into())],
                })
                .collect::<Vec<_>>(),
        )
        .await
        .expect_err("optimized parameter batches must preserve reserved-key protection");
    assert!(
        batch_error
            .to_string()
            .contains("reserved for engine-managed plugin state")
    );
    let error = lix
        .execute(
            "UPDATE lix_key_value SET value=$1 WHERE key='lix_plugin_registry_v2'",
            &[Value::Text("{}".into())],
        )
        .await
        .expect_err("public fileless registry updates must remain forbidden");
    assert!(
        error
            .to_string()
            .contains("reserved for engine-managed plugin state"),
        "{error}"
    );
    lix.execute(
        "UPDATE markdown_node SET payload_json=$1 WHERE kind='paragraph'",
        &[Value::Text(
            serde_json::json!({"inline": [{"type": "text", "value": "Still works"}]}).to_string(),
        )],
    )
    .await
    .expect("rejected registry edit must preserve plugin functionality");
    assert_eq!(
        content(&lix, "/a.md").await,
        Some(b"Still works\n".to_vec())
    );
    lix.close().await.unwrap();
}

#[tokio::test]
async fn apply_mixed_plugin_file_lifecycle_and_live_semantic_change() {
    let lix = workspace().await;
    lix.execute(
        "INSERT INTO lix_file(path,content) VALUES('/live.md',$1)",
        &[Value::Blob(b"before\n".to_vec().into())],
    )
    .await
    .unwrap();
    let before = head(&lix).await;
    lix.execute(
        "INSERT INTO lix_file(path,content) VALUES('/added.md',$1)",
        &[Value::Blob(b"added\n".to_vec().into())],
    )
    .await
    .unwrap();
    lix.execute(
        "UPDATE lix_file SET content=$1 WHERE path='/live.md'",
        &[Value::Blob(b"after\n".to_vec().into())],
    )
    .await
    .unwrap();
    let after = head(&lix).await;
    apply(&lix, &before).await;
    assert_eq!(content(&lix, "/added.md").await, None);
    assert_eq!(content(&lix, "/live.md").await, Some(b"before\n".to_vec()));
    apply(&lix, &after).await;
    assert_eq!(content(&lix, "/added.md").await, Some(b"added\n".to_vec()));
    assert_eq!(content(&lix, "/live.md").await, Some(b"after\n".to_vec()));
    lix.close().await.unwrap();
}
