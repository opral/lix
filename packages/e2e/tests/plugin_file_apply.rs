use lix::{ExecuteBatchStatement, Lix, Value, open_lix};
use std::io::{Cursor, Write};

async fn workspace() -> Lix {
    let lix = open_lix().await.unwrap();
    install_markdown_plugin(&lix).await;
    lix
}

async fn install_markdown_plugin(lix: &Lix) {
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
}

async fn install_text_plugin(lix: &Lix) {
    let wasm = std::fs::read(env!("CARGO_CDYLIB_FILE_PLUGIN_TEXT_plugin_text")).unwrap();
    let mut archive = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, content) in [
        (
            "manifest.json",
            include_str!("../../../plugins/text/manifest.json").as_bytes(),
        ),
        (
            "schema/text_line.json",
            include_str!("../../../plugins/text/schema/text_line.json").as_bytes(),
        ),
        ("plugin.wasm", wasm.as_slice()),
    ] {
        archive.start_file(path, options).unwrap();
        archive.write_all(content).unwrap();
    }
    lix.execute(
        "INSERT INTO lix_file(path,content) VALUES($1,$2)",
        &[
            Value::Text("/.lix/plugins/plugin_text.lixplugin".into()),
            Value::Blob(archive.finish().unwrap().into_inner().into()),
        ],
    )
    .await
    .unwrap();
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
        "SELECT commit_id FROM lix_apply(\
           $1, $2, ARRAY(SELECT row_ref FROM lix_diff('lix_file',$1,$2)))",
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
async fn scoped_revert_restores_existing_markdown_file_and_plugin_state() {
    let lix = open_lix().await.unwrap();
    lix.execute(
        "INSERT INTO lix_file(path,content) VALUES('/welcome.md',$1),('/changelog.md',$2)",
        &[
            Value::Blob(b"Welcome before\n".to_vec().into()),
            Value::Blob(b"Changelog before\n".to_vec().into()),
        ],
    )
    .await
    .unwrap();
    install_markdown_plugin(&lix).await;
    let before = head(&lix).await;
    lix.execute(
        "UPDATE lix_file SET content=$1 WHERE path='/welcome.md'",
        &[Value::Blob(b"Welcome after\n".to_vec().into())],
    )
    .await
    .unwrap();
    lix.execute(
        "UPDATE lix_file SET content=$1 WHERE path='/changelog.md'",
        &[Value::Blob(b"Changelog after\n".to_vec().into())],
    )
    .await
    .unwrap();
    let after = head(&lix).await;
    let changelog_id: String = lix
        .execute("SELECT id FROM lix_file WHERE path='/changelog.md'", &[])
        .await
        .unwrap()
        .rows()[0]
        .get("id")
        .unwrap();
    let welcome_id: String = lix
        .execute("SELECT id FROM lix_file WHERE path='/welcome.md'", &[])
        .await
        .unwrap()
        .rows()[0]
        .get("id")
        .unwrap();
    let reservations = lix
        .execute(
            "SELECT key FROM lix_diff('lix_key_value',$1,$2) \
             WHERE key LIKE 'lix_plugin_create_v1:%' \
               AND coalesce(to_lixcol_file_id,from_lixcol_file_id)=$3",
            &[
                Value::Text(before.clone()),
                Value::Text(after.clone()),
                Value::Text(changelog_id.clone()),
            ],
        )
        .await
        .unwrap();
    assert!(!reservations.rows().is_empty());
    let owners = lix
        .execute(
            "SELECT key FROM lix_diff('lix_key_value',$1,$2) \
             WHERE key='lix_plugin_owner_v2' \
               AND coalesce(to_lixcol_file_id,from_lixcol_file_id)=$3",
            &[
                Value::Text(before.clone()),
                Value::Text(after.clone()),
                Value::Text(changelog_id.clone()),
            ],
        )
        .await
        .unwrap();
    assert!(!owners.rows().is_empty());
    lix.execute(
        "SELECT commit_id FROM lix_revert_range($1,$2,ARRAY[lix_row_ref('lix_file',NULL,$3)])",
        &[
            Value::Text(before),
            Value::Text(after),
            Value::Text(changelog_id.clone()),
        ],
    )
    .await
    .expect("selected file and its engine-managed plugin state must revert together");
    assert_eq!(
        content(&lix, "/changelog.md").await,
        Some(b"Changelog before\n".to_vec())
    );
    assert_eq!(
        content(&lix, "/welcome.md").await,
        Some(b"Welcome after\n".to_vec())
    );
    for (file_id, expected_count) in [(&changelog_id, 0), (&welcome_id, 1)] {
        let result = lix
            .execute(
                "SELECT key FROM lix_key_value WHERE lixcol_file_id=$1 \
                 AND key LIKE 'lix_plugin_create_v1:%'",
                &[Value::Text(file_id.clone())],
            )
            .await
            .unwrap();
        assert_eq!(result.rows().len(), expected_count);
    }
    lix.execute(
        "UPDATE lix_file SET content=$1 WHERE path='/changelog.md'",
        &[Value::Blob(b"Changelog initialized\n".to_vec().into())],
    )
    .await
    .expect("a restored file must initialize its plugin state on the next file edit");
    lix.execute(
        "UPDATE markdown_node SET payload_json=$1 WHERE kind='paragraph' AND lixcol_file_id=$2",
        &[
            Value::Text(
                serde_json::json!({"inline": [{"type": "text", "value": "Edited again"}]})
                    .to_string(),
            ),
            Value::Text(changelog_id),
        ],
    )
    .await
    .expect("restored plugin state must accept a later semantic edit");
    assert_eq!(
        content(&lix, "/changelog.md").await,
        Some(b"Edited again\n".to_vec())
    );
    lix.close().await.unwrap();
}

#[tokio::test]
async fn scoped_revert_replays_reservation_without_owner_change() {
    let lix = workspace().await;
    lix.execute(
        "INSERT INTO lix_file(path,content) VALUES('/notes.md',$1)",
        &[Value::Blob(b"Original\n".to_vec().into())],
    )
    .await
    .unwrap();
    let file_id: String = lix
        .execute("SELECT id FROM lix_file WHERE path='/notes.md'", &[])
        .await
        .unwrap()
        .rows()[0]
        .get("id")
        .unwrap();
    let before = head(&lix).await;
    lix.execute(
        "UPDATE lix_file SET content=$1 WHERE path='/notes.md'",
        &[Value::Blob(b"Original\n\nSecond\n".to_vec().into())],
    )
    .await
    .unwrap();
    let after = head(&lix).await;
    let reserved_changes = lix
        .execute(
            "SELECT key FROM lix_diff('lix_key_value',$1,$2) \
             WHERE coalesce(to_lixcol_file_id,from_lixcol_file_id)=$3 \
               AND (key='lix_plugin_owner_v2' OR key LIKE 'lix_plugin_create_v1:%')",
            &[
                Value::Text(before.clone()),
                Value::Text(after.clone()),
                Value::Text(file_id.clone()),
            ],
        )
        .await
        .unwrap();
    assert!(
        reserved_changes.rows().iter().any(|row| {
            row.get::<String>("key")
                .unwrap()
                .starts_with("lix_plugin_create_v1:")
        }),
        "adding a Markdown node must create a reservation"
    );
    assert!(
        reserved_changes
            .rows()
            .iter()
            .all(|row| row.get::<String>("key").unwrap() != "lix_plugin_owner_v2"),
        "the owner must remain unchanged so this exercises reservation-only replay"
    );
    lix.execute(
        "SELECT commit_id FROM lix_revert_range($1,$2,ARRAY[lix_row_ref('lix_file',NULL,$3)])",
        &[
            Value::Text(before),
            Value::Text(after),
            Value::Text(file_id.clone()),
        ],
    )
    .await
    .expect("reservation-only file history must revert");
    assert_eq!(
        content(&lix, "/notes.md").await,
        Some(b"Original\n".to_vec())
    );
    lix.execute(
        "UPDATE markdown_node SET payload_json=$1 WHERE kind='paragraph' AND lixcol_file_id=$2",
        &[
            Value::Text(
                serde_json::json!({"inline": [{"type": "text", "value": "Edited again"}]})
                    .to_string(),
            ),
            Value::Text(file_id),
        ],
    )
    .await
    .expect("reservation replay must leave the owner ready for semantic edits");
    assert_eq!(
        content(&lix, "/notes.md").await,
        Some(b"Edited again\n".to_vec())
    );
    lix.close().await.unwrap();
}

#[tokio::test]
async fn scoped_revert_replays_owner_without_reservation_change() {
    let lix = open_lix().await.unwrap();
    install_text_plugin(&lix).await;
    lix.execute(
        "INSERT INTO lix_file(path,content) VALUES('/empty.txt',$1)",
        &[Value::Blob(Vec::new().into())],
    )
    .await
    .unwrap();
    let file_id: String = lix
        .execute("SELECT id FROM lix_file WHERE path='/empty.txt'", &[])
        .await
        .unwrap()
        .rows()[0]
        .get("id")
        .unwrap();
    let before = head(&lix).await;
    lix.execute(
        "UPDATE lix_file SET content=$1 WHERE path='/empty.txt'",
        &[Value::Blob(vec![0].into())],
    )
    .await
    .unwrap();
    let after = head(&lix).await;
    let reserved_changes = lix
        .execute(
            "SELECT key FROM lix_diff('lix_key_value',$1,$2) \
             WHERE coalesce(to_lixcol_file_id,from_lixcol_file_id)=$3 \
               AND (key='lix_plugin_owner_v2' OR key LIKE 'lix_plugin_create_v1:%')",
            &[
                Value::Text(before.clone()),
                Value::Text(after.clone()),
                Value::Text(file_id.clone()),
            ],
        )
        .await
        .unwrap();
    assert_eq!(reserved_changes.rows().len(), 1);
    assert_eq!(
        reserved_changes.rows()[0].get::<String>("key").unwrap(),
        "lix_plugin_owner_v2",
        "an empty text file must change ownership without creating reservations"
    );
    lix.execute(
        "SELECT commit_id FROM lix_revert_range($1,$2,ARRAY[lix_row_ref('lix_file',NULL,$3)])",
        &[
            Value::Text(before),
            Value::Text(after),
            Value::Text(file_id.clone()),
        ],
    )
    .await
    .expect("owner-only file history must revert");
    assert_eq!(content(&lix, "/empty.txt").await, Some(Vec::new()));
    let owner = lix
        .execute(
            "SELECT key FROM lix_key_value WHERE lixcol_file_id=$1 AND key='lix_plugin_owner_v2'",
            &[Value::Text(file_id)],
        )
        .await
        .unwrap();
    assert_eq!(owner.rows().len(), 1);
    lix.close().await.unwrap();
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
    let owner_only = lix
        .execute(
            "SELECT commit_id FROM lix_apply(\
           $1, $2, ARRAY(SELECT row_ref FROM lix_diff('lix_key_value',$1,$2) \
                         WHERE key='lix_plugin_owner_v2'))",
            &[Value::Text(deleted), Value::Text(added.clone())],
        )
        .await
        .expect_err("selecting owner history alone must not authorize its restoration");
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
    let reservation_error = lix
        .execute(
            "INSERT INTO lix_key_value(key,value) VALUES('lix_plugin_create_v1:000000000000000000000000',$1)",
            &[Value::Text("{}".into())],
        )
        .await
        .expect_err("public reservation writes must remain forbidden");
    assert!(
        reservation_error
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
