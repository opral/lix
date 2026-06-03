#[cfg(feature = "default_wasm_runtime")]
use lix_sdk::FsWriteOptions;
use lix_sdk::{
    CreateBranchOptions, InMemoryBackend, LixError, MergeBranchOptions, MergeBranchOutcome,
    OpenLixOptions, SwitchBranchOptions, Value, open_lix,
};
#[cfg(feature = "default_wasm_runtime")]
use std::io::{Cursor, Write};
#[cfg(feature = "default_wasm_runtime")]
use std::path::Path;

#[tokio::test]
#[cfg(feature = "default_wasm_runtime")]
async fn rs_sdk_installs_built_csv_plugin_archive_and_uses_schema() {
    let archive = build_csv_plugin_archive();
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();

    lix.install_plugin_archive(&archive).await.unwrap();
    let plugins = lix.list_installed_plugins().await.unwrap();
    assert_eq!(plugins.len(), 1);
    assert_eq!(plugins[0].key, "plugin_csv");
    assert_eq!(
        plugins[0].schema_keys,
        vec!["csv_table".to_string(), "csv_row".to_string()]
    );

    let stored_archive = lix
        .read_file("/.lix/plugins/plugin_csv.lixplugin")
        .await
        .unwrap();
    assert_eq!(stored_archive.as_deref(), Some(archive.as_slice()));

    let schemas = lix
        .execute(
            "SELECT table_name \
             FROM information_schema.tables \
             WHERE table_name IN ('csv_row', 'csv_table') \
             ORDER BY table_name",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        schemas
            .rows()
            .iter()
            .map(|row| row.get::<String>("table_name").unwrap())
            .collect::<Vec<_>>(),
        vec!["csv_row".to_string(), "csv_table".to_string()]
    );

    let original_csv = b"name,age\nAda,37\n".to_vec();
    lix.write_file(
        "/people.csv",
        original_csv.clone(),
        FsWriteOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        lix.read_file("/people.csv").await.unwrap().as_deref(),
        Some(original_csv.as_slice())
    );

    let file_id = lix
        .execute(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text("/people.csv".to_string())],
        )
        .await
        .unwrap();
    assert_eq!(file_id.len(), 1);
    let file_id = file_id.rows()[0].get::<String>("id").unwrap();
    let file_changes_before_update = file_changes(&lix, &file_id).await;

    let updated_csv = b"name,age\nAda,37\nGrace,85\n".to_vec();
    lix.write_file(
        "/people.csv",
        updated_csv.clone(),
        FsWriteOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        lix.read_file("/people.csv").await.unwrap().as_deref(),
        Some(updated_csv.as_slice())
    );

    let file_changes_after_update = file_changes(&lix, &file_id).await;
    let resulting_diff_changes = file_changes_after_update
        .into_iter()
        .skip(file_changes_before_update.len())
        .collect::<Vec<_>>();
    assert_eq!(resulting_diff_changes.len(), 1);
    let change = &resulting_diff_changes[0];
    assert_eq!(change.schema_key, "csv_row");
    let snapshot = change
        .snapshot_content
        .as_ref()
        .expect("updated file write should produce a csv row snapshot");
    assert_eq!(
        snapshot
            .get("cells")
            .and_then(serde_json::Value::as_array)
            .unwrap(),
        &vec![
            serde_json::Value::String("Grace".to_string()),
            serde_json::Value::String("85".to_string())
        ]
    );

    let files = lix
        .execute(
            "SELECT path, data FROM lix_file WHERE path = $1",
            &[Value::Text("/people.csv".to_string())],
        )
        .await
        .unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(
        files.rows()[0].values(),
        &[
            Value::Text("/people.csv".to_string()),
            Value::Blob(updated_csv.clone())
        ]
    );

    let files_by_id = lix
        .execute(
            "SELECT data FROM lix_file WHERE id = $1",
            &[Value::Text(file_id.clone())],
        )
        .await
        .unwrap();
    assert_eq!(files_by_id.len(), 1);
    assert_eq!(
        files_by_id.rows()[0].values(),
        &[Value::Blob(updated_csv.clone())]
    );

    let file_changes_before_empty = file_changes(&lix, &file_id).await;
    let empty_csv = Vec::new();
    lix.write_file("/people.csv", empty_csv.clone(), FsWriteOptions::default())
        .await
        .unwrap();
    assert_eq!(
        lix.read_file("/people.csv").await.unwrap().as_deref(),
        Some(empty_csv.as_slice())
    );
    let files_empty_by_id = lix
        .execute(
            "SELECT data FROM lix_file WHERE id = $1",
            &[Value::Text(file_id.clone())],
        )
        .await
        .unwrap();
    assert_eq!(files_empty_by_id.len(), 1);
    assert_eq!(
        files_empty_by_id.rows()[0].values(),
        &[Value::Blob(empty_csv)]
    );
    let empty_changes = file_changes(&lix, &file_id)
        .await
        .into_iter()
        .skip(file_changes_before_empty.len())
        .collect::<Vec<_>>();
    assert!(
        empty_changes
            .iter()
            .any(|change| change.schema_key == "csv_row" && change.snapshot_content.is_none())
    );

    let sql_csv = b"name,age\nLin,44\n".to_vec();
    lix.execute(
        "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
        &[
            Value::Text("/sql-people.csv".to_string()),
            Value::Blob(sql_csv.clone()),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        lix.read_file("/sql-people.csv").await.unwrap().as_deref(),
        Some(sql_csv.as_slice())
    );

    let sql_file_id = lix
        .execute(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text("/sql-people.csv".to_string())],
        )
        .await
        .unwrap();
    assert_eq!(sql_file_id.len(), 1);
    let sql_file_id = sql_file_id.rows()[0].get::<String>("id").unwrap();
    let sql_insert_changes = file_changes(&lix, &sql_file_id).await;
    assert!(
        sql_insert_changes
            .iter()
            .any(|change| change.schema_key == "csv_table")
    );
    assert!(
        sql_insert_changes
            .iter()
            .any(|change| change.schema_key == "csv_row")
    );
    assert!(
        !sql_insert_changes
            .iter()
            .any(|change| change.schema_key == "lix_binary_blob_ref")
    );

    let sql_changes_before_update = sql_insert_changes.len();
    let sql_updated_csv = b"name,age\nLin,44\nMina,29\n".to_vec();
    lix.execute(
        "UPDATE lix_file SET data = $1 WHERE path = $2",
        &[
            Value::Blob(sql_updated_csv.clone()),
            Value::Text("/sql-people.csv".to_string()),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        lix.read_file("/sql-people.csv").await.unwrap().as_deref(),
        Some(sql_updated_csv.as_slice())
    );
    let sql_update_changes = file_changes(&lix, &sql_file_id)
        .await
        .into_iter()
        .skip(sql_changes_before_update)
        .collect::<Vec<_>>();
    assert!(sql_update_changes.iter().any(|change| {
        change.schema_key == "csv_row"
            && change
                .snapshot_content
                .as_ref()
                .and_then(|snapshot| snapshot.get("cells"))
                .and_then(serde_json::Value::as_array)
                == Some(&vec![
                    serde_json::Value::String("Mina".to_string()),
                    serde_json::Value::String("29".to_string()),
                ])
    }));
    assert!(
        !sql_update_changes
            .iter()
            .any(|change| change.schema_key == "lix_binary_blob_ref")
    );

    let sql_changes_before_predicate_update = sql_changes_before_update + sql_update_changes.len();
    let sql_predicate_updated_csv = b"name,age\nLin,44\nMina,29\nKatherine,101\n".to_vec();
    lix.execute(
        "UPDATE lix_file SET data = $1 WHERE path = $2 AND data = $3",
        &[
            Value::Blob(sql_predicate_updated_csv.clone()),
            Value::Text("/sql-people.csv".to_string()),
            Value::Blob(sql_updated_csv.clone()),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        lix.read_file("/sql-people.csv").await.unwrap().as_deref(),
        Some(sql_predicate_updated_csv.as_slice())
    );
    let sql_predicate_update_changes = file_changes(&lix, &sql_file_id)
        .await
        .into_iter()
        .skip(sql_changes_before_predicate_update)
        .collect::<Vec<_>>();
    assert!(sql_predicate_update_changes.iter().any(|change| {
        change.schema_key == "csv_row"
            && change
                .snapshot_content
                .as_ref()
                .and_then(|snapshot| snapshot.get("cells"))
                .and_then(serde_json::Value::as_array)
                == Some(&vec![
                    serde_json::Value::String("Katherine".to_string()),
                    serde_json::Value::String("101".to_string()),
                ])
    }));
    assert!(
        !sql_predicate_update_changes
            .iter()
            .any(|change| change.schema_key == "lix_binary_blob_ref")
    );

    let sql_empty_target = b"name,age\nNoor,10\n".to_vec();
    lix.execute(
        "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
        &[
            Value::Text("/sql-empty.csv".to_string()),
            Value::Blob(sql_empty_target),
        ],
    )
    .await
    .unwrap();
    let sql_empty_file_id = lix
        .execute(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text("/sql-empty.csv".to_string())],
        )
        .await
        .unwrap();
    assert_eq!(sql_empty_file_id.len(), 1);
    let sql_empty_file_id = sql_empty_file_id.rows()[0].get::<String>("id").unwrap();
    let sql_empty_changes_before_update = file_changes(&lix, &sql_empty_file_id).await;
    let sql_empty_bytes = Vec::new();
    lix.execute(
        "UPDATE lix_file SET data = $1 WHERE path = $2",
        &[
            Value::Blob(sql_empty_bytes.clone()),
            Value::Text("/sql-empty.csv".to_string()),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        lix.read_file("/sql-empty.csv").await.unwrap().as_deref(),
        Some(sql_empty_bytes.as_slice())
    );
    let sql_empty_update_changes = file_changes(&lix, &sql_empty_file_id)
        .await
        .into_iter()
        .skip(sql_empty_changes_before_update.len())
        .collect::<Vec<_>>();
    assert!(
        sql_empty_update_changes
            .iter()
            .any(|change| change.schema_key == "csv_row" && change.snapshot_content.is_none())
    );

    let sql_rename_csv = b"name,age\nRuth,99\n".to_vec();
    lix.execute(
        "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
        &[
            Value::Text("/sql-rename.csv".to_string()),
            Value::Blob(sql_rename_csv.clone()),
        ],
    )
    .await
    .unwrap();
    let sql_rename_file_id = lix
        .execute(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text("/sql-rename.csv".to_string())],
        )
        .await
        .unwrap();
    assert_eq!(sql_rename_file_id.len(), 1);
    let sql_rename_file_id = sql_rename_file_id.rows()[0].get::<String>("id").unwrap();
    let rename = lix
        .execute(
            "UPDATE lix_file SET path = $1 WHERE path = $2",
            &[
                Value::Text("/sql-rename.txt".to_string()),
                Value::Text("/sql-rename.csv".to_string()),
            ],
        )
        .await
        .unwrap();
    assert_eq!(rename.rows_affected(), 1);
    assert_eq!(lix.read_file("/sql-rename.csv").await.unwrap(), None);
    assert_eq!(
        lix.read_file("/sql-rename.txt").await.unwrap().as_deref(),
        Some(sql_rename_csv.as_slice())
    );
    let renamed_files = lix
        .execute(
            "SELECT data FROM lix_file WHERE path = $1",
            &[Value::Text("/sql-rename.txt".to_string())],
        )
        .await
        .unwrap();
    assert_eq!(renamed_files.len(), 1);
    assert_eq!(
        renamed_files.rows()[0].values(),
        &[Value::Blob(sql_rename_csv.clone())]
    );
    let active_plugin_rows_after_rename = lix
        .execute(
            "SELECT schema_key FROM lix_state \
             WHERE file_id = $1 AND schema_key IN ('csv_table', 'csv_row')",
            &[Value::Text(sql_rename_file_id.clone())],
        )
        .await
        .unwrap();
    assert_eq!(active_plugin_rows_after_rename.len(), 0);
    let active_blob_rows_after_rename = lix
        .execute(
            "SELECT schema_key FROM lix_state \
             WHERE file_id = $1 AND schema_key = 'lix_binary_blob_ref'",
            &[Value::Text(sql_rename_file_id)],
        )
        .await
        .unwrap();
    assert_eq!(active_blob_rows_after_rename.len(), 1);

    let sql_changes_before_delete =
        sql_changes_before_predicate_update + sql_predicate_update_changes.len();
    lix.execute(
        "DELETE FROM lix_file WHERE path = $1 AND data = $2",
        &[
            Value::Text("/sql-people.csv".to_string()),
            Value::Blob(sql_predicate_updated_csv),
        ],
    )
    .await
    .unwrap();
    assert_eq!(lix.read_file("/sql-people.csv").await.unwrap(), None);
    let sql_delete_changes = file_changes(&lix, &sql_file_id)
        .await
        .into_iter()
        .skip(sql_changes_before_delete)
        .collect::<Vec<_>>();
    assert!(
        sql_delete_changes.iter().any(|change| {
            change.schema_key == "csv_table" && change.snapshot_content.is_none()
        })
    );
    assert!(
        sql_delete_changes
            .iter()
            .filter(|change| change.schema_key == "csv_row" && change.snapshot_content.is_none())
            .count()
            >= 2
    );
    let active_plugin_rows_after_delete = lix
        .execute(
            "SELECT schema_key FROM lix_state \
             WHERE file_id = $1 AND schema_key IN ('csv_table', 'csv_row')",
            &[Value::Text(sql_file_id.clone())],
        )
        .await
        .unwrap();
    assert_eq!(active_plugin_rows_after_delete.len(), 0);

    lix.close().await.unwrap();
}

#[tokio::test]
#[cfg(feature = "default_wasm_runtime")]
async fn transaction_lix_file_data_uses_session_plugin_runtime() {
    let archive = build_csv_plugin_archive();
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();

    lix.install_plugin_archive(&archive).await.unwrap();
    let csv = b"name,age\nAda,37\nGrace,85\n".to_vec();
    lix.write_file("/tx-plugin.csv", csv.clone(), FsWriteOptions::default())
        .await
        .unwrap();
    let file_id = lix
        .execute(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text("/tx-plugin.csv".to_string())],
        )
        .await
        .unwrap()
        .rows()[0]
        .get::<String>("id")
        .unwrap();

    let mut tx = lix.begin_transaction().await.unwrap();
    let files = tx
        .execute(
            "SELECT data FROM lix_file WHERE id = $1",
            &[Value::Text(file_id)],
        )
        .await
        .unwrap();

    assert_eq!(files.len(), 1);
    assert_eq!(files.rows()[0].values(), &[Value::Blob(csv)]);

    tx.rollback().await.unwrap();
    lix.close().await.unwrap();
}

#[derive(Debug, Clone, PartialEq)]
#[cfg(feature = "default_wasm_runtime")]
struct FileChange {
    schema_key: String,
    entity_pk: serde_json::Value,
    snapshot_content: Option<serde_json::Value>,
}

#[cfg(feature = "default_wasm_runtime")]
async fn file_changes(lix: &lix_sdk::Lix, file_id: &str) -> Vec<FileChange> {
    let changes = lix
        .execute(
            "SELECT schema_key, entity_pk, snapshot_content \
             FROM lix_change \
             WHERE file_id = $1 \
             ORDER BY created_at, id",
            &[Value::Text(file_id.to_string())],
        )
        .await
        .unwrap();

    changes
        .rows()
        .iter()
        .map(|row| {
            let snapshot_content = match row.value("snapshot_content").unwrap() {
                Value::Json(value) => Some(value.clone()),
                Value::Null => None,
                other => panic!("expected JSON or null snapshot_content, got {other:?}"),
            };
            FileChange {
                schema_key: row.get::<String>("schema_key").unwrap(),
                entity_pk: row.get::<serde_json::Value>("entity_pk").unwrap(),
                snapshot_content,
            }
        })
        .collect()
}

#[tokio::test]
async fn rs_sdk_open_register_write_query_branch_and_merge_flow() {
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    let main_branch_id = lix.active_branch_id().await.unwrap();

    register_crm_task_schema(&lix).await;

    lix.execute(
        "INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
        &[
            Value::Text("task-1".to_string()),
            Value::Text("Draft RS SDK flow".to_string()),
            Value::Boolean(false),
            Value::Text(r#"{"priority":"high","tags":["sdk","json"]}"#.to_string()),
        ],
    )
    .await
    .unwrap();

    let projected = lix
        .execute(
            "SELECT title, done, meta, lixcol_snapshot_content FROM crm_task WHERE id = $1",
            &[Value::Text("task-1".to_string())],
        )
        .await
        .unwrap();
    assert_crm_task_projection(&projected);

    assert!(!task_done(&lix, "task-1").await);

    let draft = lix
        .create_branch(CreateBranchOptions {
            id: Some("draft-branch".to_string()),
            name: "Draft".to_string(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    assert_eq!(draft.id, "draft-branch");
    assert_eq!(draft.name, "Draft");
    assert!(!draft.hidden);

    lix.switch_branch(SwitchBranchOptions {
        branch_id: draft.id.clone(),
    })
    .await
    .unwrap();

    lix.execute(
        "UPDATE crm_task SET done = $1 WHERE id = $2",
        &[Value::Boolean(true), Value::Text("task-1".to_string())],
    )
    .await
    .unwrap();

    assert!(task_done(&lix, "task-1").await);

    lix.switch_branch(SwitchBranchOptions {
        branch_id: main_branch_id.clone(),
    })
    .await
    .unwrap();

    assert!(!task_done(&lix, "task-1").await);

    let merge = lix
        .merge_branch(MergeBranchOptions {
            source_branch_id: draft.id,
        })
        .await
        .unwrap();

    assert_eq!(merge.outcome, MergeBranchOutcome::FastForward);
    assert_eq!(merge.target_branch_id, main_branch_id);
    assert_eq!(merge.change_stats.total, 1);
    assert_eq!(merge.change_stats.modified, 1);
    assert_eq!(merge.created_merge_commit_id, None);
    assert!(task_done(&lix, "task-1").await);

    lix.close().await.unwrap();
}

#[tokio::test]
async fn rs_sdk_close_is_idempotent_and_rejects_later_operations() {
    let lix = open_lix(OpenLixOptions {
        backend: InMemoryBackend::new(),
        ..Default::default()
    })
    .await
    .unwrap();

    lix.close().await.unwrap();
    lix.close().await.unwrap();

    let error = lix
        .execute("SELECT value FROM lix_key_value WHERE key = 'lix_id'", &[])
        .await
        .expect_err("execute after close should fail");
    assert_closed(error);

    let error = lix
        .active_branch_id()
        .await
        .expect_err("active_branch_id after close should fail");
    assert_closed(error);
}

#[tokio::test]
async fn rs_sdk_close_does_not_destroy_committed_data() {
    let backend = InMemoryBackend::new();
    let first = open_lix(OpenLixOptions {
        backend: backend.clone(),
        ..Default::default()
    })
    .await
    .unwrap();

    first
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('close-key', 'close-value')",
            &[],
        )
        .await
        .unwrap();
    first.close().await.unwrap();

    let error = first
        .execute(
            "SELECT value FROM lix_key_value WHERE key = 'close-key'",
            &[],
        )
        .await
        .expect_err("closed handle should not be usable");
    assert_closed(error);

    let second = open_lix(OpenLixOptions {
        backend,
        ..Default::default()
    })
    .await
    .unwrap();
    let result = second
        .execute(
            "SELECT key FROM lix_key_value WHERE key = 'close-key' AND value = lix_json('\"close-value\"')",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Text("close-key".to_string())]
    );
    second.close().await.unwrap();
}

#[tokio::test]
async fn failed_write_validation_does_not_poison_backend_transaction() {
    let lix = open_lix(OpenLixOptions {
        backend: InMemoryBackend::new(),
        ..Default::default()
    })
    .await
    .unwrap();

    register_poison_task_schema(&lix).await;

    let error = lix
        .execute(
            "INSERT INTO poison_task (id, title) VALUES ($1, $2)",
            &[
                Value::Text("bad-task".to_string()),
                Value::Text("missing meta".to_string()),
            ],
        )
        .await
        .expect_err("schema validation should reject missing required field");
    assert_eq!(error.code, "LIX_ERROR_SCHEMA_VALIDATION");

    let result = lix.execute("SELECT 1 AS ok", &[]).await.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].values(), &[Value::Integer(1)]);

    lix.execute(
        "INSERT INTO poison_task (id, title, meta) VALUES ($1, $2, lix_json($3))",
        &[
            Value::Text("good-task".to_string()),
            Value::Text("valid".to_string()),
            Value::Text(r#"{"priority":"high"}"#.to_string()),
        ],
    )
    .await
    .expect("valid write after failed write should succeed");

    lix.close().await.unwrap();
}

#[tokio::test]
async fn transaction_commits_multiple_statements_together() {
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    register_crm_task_schema(&lix).await;

    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
        &[
            Value::Text("tx-task-1".to_string()),
            Value::Text("First".to_string()),
            Value::Boolean(false),
            Value::Text(r#"{"batch":1}"#.to_string()),
        ],
    )
    .await
    .unwrap();
    tx.execute(
        "INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
        &[
            Value::Text("tx-task-2".to_string()),
            Value::Text("Second".to_string()),
            Value::Boolean(true),
            Value::Text(r#"{"batch":1}"#.to_string()),
        ],
    )
    .await
    .unwrap();

    let staged = tx
        .execute(
            "SELECT id FROM crm_task WHERE id IN ($1, $2) ORDER BY id",
            &[
                Value::Text("tx-task-1".to_string()),
                Value::Text("tx-task-2".to_string()),
            ],
        )
        .await
        .unwrap();
    assert_eq!(staged.len(), 2);

    tx.commit().await.unwrap();

    let committed = lix
        .execute(
            "SELECT id FROM crm_task WHERE id IN ($1, $2) ORDER BY id",
            &[
                Value::Text("tx-task-1".to_string()),
                Value::Text("tx-task-2".to_string()),
            ],
        )
        .await
        .unwrap();
    assert_eq!(committed.len(), 2);
    lix.close().await.unwrap();
}

#[tokio::test]
async fn transaction_lix_file_data_reads_staged_file_bytes() {
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    let mut tx = lix.begin_transaction().await.unwrap();
    let path = "/tx-file-data.bin".to_string();
    let original = b"staged bytes before commit".to_vec();

    tx.execute(
        "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
        &[Value::Text(path.clone()), Value::Blob(original.clone())],
    )
    .await
    .unwrap();

    let selected = tx
        .execute(
            "SELECT data FROM lix_file WHERE path = $1 AND data = $2",
            &[Value::Text(path.clone()), Value::Blob(original.clone())],
        )
        .await
        .unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(
        selected.rows()[0].values(),
        &[Value::Blob(original.clone())]
    );

    let updated = b"updated bytes before commit".to_vec();
    let update = tx
        .execute(
            "UPDATE lix_file SET data = $1 WHERE path = $2 AND data = $3",
            &[
                Value::Blob(updated.clone()),
                Value::Text(path.clone()),
                Value::Blob(original),
            ],
        )
        .await
        .unwrap();
    assert_eq!(update.rows_affected(), 1);

    let after_update = tx
        .execute(
            "SELECT data FROM lix_file WHERE path = $1 AND data = $2",
            &[Value::Text(path.clone()), Value::Blob(updated.clone())],
        )
        .await
        .unwrap();
    assert_eq!(after_update.len(), 1);
    assert_eq!(
        after_update.rows()[0].values(),
        &[Value::Blob(updated.clone())]
    );

    let delete = tx
        .execute(
            "DELETE FROM lix_file WHERE path = $1 AND data = $2",
            &[Value::Text(path.clone()), Value::Blob(updated)],
        )
        .await
        .unwrap();
    assert_eq!(delete.rows_affected(), 1);

    let after_delete = tx
        .execute(
            "SELECT data FROM lix_file WHERE path = $1",
            &[Value::Text(path)],
        )
        .await
        .unwrap();
    assert_eq!(after_delete.len(), 0);

    tx.rollback().await.unwrap();
    lix.close().await.unwrap();
}

#[tokio::test]
async fn transaction_rollback_discards_staged_writes() {
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    register_crm_task_schema(&lix).await;

    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
        &[
            Value::Text("rolled-back-task".to_string()),
            Value::Text("Rollback".to_string()),
            Value::Boolean(false),
            Value::Text(r#"{"batch":1}"#.to_string()),
        ],
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();

    let result = lix
        .execute(
            "SELECT id FROM crm_task WHERE id = $1",
            &[Value::Text("rolled-back-task".to_string())],
        )
        .await
        .unwrap();
    assert_eq!(result.len(), 0);
    lix.close().await.unwrap();
}

#[tokio::test]
async fn transaction_blocks_session_execute_on_same_handle() {
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    register_crm_task_schema(&lix).await;

    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
        &[
            Value::Text("tx-only-task".to_string()),
            Value::Text("Inside tx".to_string()),
            Value::Boolean(false),
            Value::Text(r#"{"batch":1}"#.to_string()),
        ],
    )
    .await
    .unwrap();

    let error = lix
        .execute(
            "INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
            &[
                Value::Text("outside-task".to_string()),
                Value::Text("Outside tx".to_string()),
                Value::Boolean(false),
                Value::Text(r#"{"batch":1}"#.to_string()),
            ],
        )
        .await
        .expect_err("session writes should be blocked while explicit transaction is active");
    assert_eq!(error.code, "LIX_INVALID_TRANSACTION_STATE");

    let error = lix
        .execute("SELECT 1 AS ok", &[])
        .await
        .expect_err("session reads should be blocked while explicit transaction is active");
    assert_eq!(error.code, "LIX_INVALID_TRANSACTION_STATE");

    let tx_read = tx
        .execute("SELECT 1 AS ok", &[])
        .await
        .expect("transaction reads should remain available");
    assert_eq!(tx_read.rows()[0].get::<i64>("ok").unwrap(), 1);

    tx.commit().await.unwrap();

    let committed = lix
        .execute(
            "SELECT id FROM crm_task WHERE id IN ($1, $2) ORDER BY id",
            &[
                Value::Text("outside-task".to_string()),
                Value::Text("tx-only-task".to_string()),
            ],
        )
        .await
        .unwrap();
    assert_eq!(committed.len(), 1);
    assert_eq!(
        committed.rows()[0].values(),
        &[Value::Text("tx-only-task".to_string())]
    );
    lix.close().await.unwrap();
}

#[cfg(feature = "default_wasm_runtime")]
fn build_csv_plugin_archive() -> Vec<u8> {
    let wasm_path = Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_plugin_csv"));
    let wasm = std::fs::read(wasm_path).unwrap_or_else(|error| {
        panic!(
            "failed to read bindep-built CSV plugin wasm at {}: {error}",
            wasm_path.display()
        )
    });
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        (
            "manifest.json",
            include_str!("../../../plugins/csv/manifest.json").as_bytes(),
        ),
        (
            "schema/csv_table.json",
            include_str!("../../../plugins/csv/schema/csv_table.json").as_bytes(),
        ),
        (
            "schema/csv_row.json",
            include_str!("../../../plugins/csv/schema/csv_row.json").as_bytes(),
        ),
        ("plugin.wasm", wasm.as_slice()),
    ] {
        writer.start_file(path, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}

async fn register_crm_task_schema(lix: &lix_sdk::Lix) {
    let schema = r#"{
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "x-lix-key": "crm_task",
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "required": ["id", "title", "done", "meta"],
        "properties": {
            "id": { "type": "string" },
            "title": { "type": "string" },
            "done": { "type": "boolean" },
            "meta": { "type": "object" }
        },
        "additionalProperties": false
    }"#;

    lix.execute(
        "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
        &[Value::Text(schema.to_string())],
    )
    .await
    .unwrap();
}

fn assert_crm_task_projection(result: &lix_sdk::ExecuteResult) {
    assert_eq!(result.len(), 1);
    let row = &result.rows()[0];
    assert_eq!(
        row.get::<String>("title").unwrap(),
        "Draft RS SDK flow".to_string()
    );
    assert!(!row.get::<bool>("done").unwrap());

    let meta = row.get::<Value>("meta").unwrap();
    let Value::Json(meta) = meta else {
        panic!("expected meta JSON value, got {meta:?}");
    };
    assert_eq!(
        meta.get("priority").and_then(|value| value.as_str()),
        Some("high")
    );
    assert_eq!(
        meta.get("tags")
            .and_then(|value| value.as_array())
            .map(Vec::len),
        Some(2)
    );

    let snapshot = row.get::<Value>("lixcol_snapshot_content").unwrap();
    let Value::Json(snapshot) = snapshot else {
        panic!("expected snapshot JSON value, got {snapshot:?}");
    };
    assert_eq!(
        snapshot.get("id").and_then(|value| value.as_str()),
        Some("task-1")
    );
    assert_eq!(
        snapshot
            .get("meta")
            .and_then(|value| value.get("priority"))
            .and_then(|value| value.as_str()),
        Some("high")
    );

    let missing = row
        .value("missing")
        .expect_err("missing column should return a structured error");
    assert_eq!(missing.code, "LIX_COLUMN_NOT_FOUND");
}

async fn register_poison_task_schema(lix: &lix_sdk::Lix) {
    let schema = r#"{
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "x-lix-key": "poison_task",
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "required": ["id", "title", "meta"],
        "properties": {
            "id": { "type": "string" },
            "title": { "type": "string" },
            "meta": { "type": "object" }
        },
        "additionalProperties": false
    }"#;

    lix.execute(
        "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
        &[Value::Text(schema.to_string())],
    )
    .await
    .unwrap();
}

async fn task_done(lix: &lix_sdk::Lix, task_id: &str) -> bool {
    let result = lix
        .execute(
            "SELECT done FROM crm_task WHERE id = $1",
            &[Value::Text(task_id.to_string())],
        )
        .await
        .unwrap();

    let rows = result;
    assert_eq!(rows.len(), 1);

    match rows.rows()[0].values().first() {
        Some(Value::Boolean(done)) => *done,
        value => panic!("expected boolean done value, got {value:?}"),
    }
}

fn assert_closed(error: LixError) {
    assert_eq!(error.code, LixError::CODE_CLOSED);
}
