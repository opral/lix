use lix_sdk::{
    CreateBranchOptions, FsWriteOptions, InMemoryBackend, LixError, MergeBranchOptions,
    MergeBranchOutcome, OpenLixOptions, SwitchBranchOptions, Value, open_lix,
};
use std::io::{Cursor, Write};
use std::path::Path;

#[tokio::test]
async fn rs_sdk_installs_built_csv_plugin_archive_and_uses_schema() {
    let archive = build_csv_plugin_archive();
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();

    lix.install_plugin_archive(&archive).await.unwrap();

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
    assert_eq!(change.schema_key, "lix_binary_blob_ref");
    assert_eq!(change.entity_pk, serde_json::json!([file_id.as_str()]));
    let snapshot = change
        .snapshot_content
        .as_ref()
        .expect("updated file write should produce a blob ref snapshot");
    assert_eq!(
        snapshot.get("id").and_then(|value| value.as_str()),
        Some(file_id.as_str())
    );
    assert_eq!(
        snapshot
            .get("size_bytes")
            .and_then(serde_json::Value::as_u64),
        Some(updated_csv.len() as u64)
    );
    assert!(
        snapshot
            .get("blob_hash")
            .and_then(|value| value.as_str())
            .is_some_and(|value| !value.is_empty())
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

    lix.close().await.unwrap();
}

#[derive(Debug, Clone, PartialEq)]
struct FileChange {
    schema_key: String,
    entity_pk: serde_json::Value,
    snapshot_content: Option<serde_json::Value>,
}

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
        backend: Some(InMemoryBackend::new()),
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
        backend: Some(backend.clone()),
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
        backend: Some(backend),
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
        backend: Some(InMemoryBackend::new()),
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

fn build_csv_plugin_archive() -> Vec<u8> {
    let wasm_path = Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_WASM_plugin_csv"));
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
        ("plugin.wasm", &wasm),
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
