use lix_sdk::{Lix, LixError, Storage};
use lix_sdk::{LocalFilesystem, open_lix_with_storage};
use lix_sdk::{OpenLixOptions, Value, open_lix};
use std::io::{Cursor, Read, Write};
use std::path::Path;
use std::time::{Duration, Instant};

#[tokio::test]
async fn rs_sdk_installs_built_csv_plugin_archive_and_uses_schema() {
    let archive = build_csv_plugin_archive();
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();

    install_plugin(&lix, "plugin_csv", &archive).await.unwrap();
    let plugins = list_installed_plugins(&lix).await;
    assert_eq!(plugins.len(), 1);
    assert_eq!(plugins[0].key, "plugin_csv");
    assert_eq!(
        plugins[0].schema_keys,
        vec!["csv_table".to_string(), "csv_row".to_string()]
    );

    let stored_archive = read_file(&lix, "/.lix/plugins/plugin_csv.lixplugin")
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
    write_file(&lix, "/people.csv", original_csv.clone())
        .await
        .unwrap();
    assert_eq!(
        read_file(&lix, "/people.csv").await.unwrap().as_deref(),
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
    write_file(&lix, "/people.csv", updated_csv.clone())
        .await
        .unwrap();
    assert_eq!(
        read_file(&lix, "/people.csv").await.unwrap().as_deref(),
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
            Value::Blob(updated_csv.clone().into())
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
        &[Value::Blob(updated_csv.clone().into())]
    );

    let file_changes_before_empty = file_changes(&lix, &file_id).await;
    let empty_csv = Vec::new();
    write_file(&lix, "/people.csv", empty_csv.clone())
        .await
        .unwrap();
    assert_eq!(
        read_file(&lix, "/people.csv").await.unwrap().as_deref(),
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
        &[Value::Blob(empty_csv.into())]
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
            Value::Blob(sql_csv.clone().into()),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        read_file(&lix, "/sql-people.csv").await.unwrap().as_deref(),
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
            Value::Blob(sql_updated_csv.clone().into()),
            Value::Text("/sql-people.csv".to_string()),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        read_file(&lix, "/sql-people.csv").await.unwrap().as_deref(),
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
            Value::Blob(sql_predicate_updated_csv.clone().into()),
            Value::Text("/sql-people.csv".to_string()),
            Value::Blob(sql_updated_csv.clone().into()),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        read_file(&lix, "/sql-people.csv").await.unwrap().as_deref(),
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
            Value::Blob(sql_empty_target.into()),
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
            Value::Blob(sql_empty_bytes.clone().into()),
            Value::Text("/sql-empty.csv".to_string()),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        read_file(&lix, "/sql-empty.csv").await.unwrap().as_deref(),
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
            Value::Blob(sql_rename_csv.clone().into()),
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
    assert_eq!(read_file(&lix, "/sql-rename.csv").await.unwrap(), None);
    assert_eq!(
        read_file(&lix, "/sql-rename.txt").await.unwrap().as_deref(),
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
        &[Value::Blob(sql_rename_csv.clone().into())]
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
            Value::Blob(sql_predicate_updated_csv.into()),
        ],
    )
    .await
    .unwrap();
    assert_eq!(read_file(&lix, "/sql-people.csv").await.unwrap(), None);
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
async fn transaction_lix_file_data_uses_session_plugin_runtime() {
    let archive = build_csv_plugin_archive();
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();

    install_plugin(&lix, "plugin_csv", &archive).await.unwrap();
    let csv = b"name,age\nAda,37\nGrace,85\n".to_vec();
    write_file(&lix, "/tx-plugin.csv", csv.clone())
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
    assert_eq!(files.rows()[0].values(), &[Value::Blob(csv.into())]);

    tx.rollback().await.unwrap();
    lix.close().await.unwrap();
}

#[tokio::test]
async fn filesystem_materializes_internal_lix_plugin_paths() {
    let tempdir = tempfile::tempdir().unwrap();
    let lix = open_lix_with_filesystem(tempdir.path()).await;
    let archive = build_csv_plugin_archive();

    install_plugin(&lix, "plugin_csv", &archive).await.unwrap();

    wait_for_disk_file(
        &tempdir.path().join(".lix/plugins/plugin_csv.lixplugin"),
        Some(archive.as_slice()),
    );
    lix.close().await.unwrap();
}

#[tokio::test]
async fn filesystem_imports_lix_plugin_archives_from_disk() {
    let tempdir = tempfile::tempdir().unwrap();
    let archive = build_csv_plugin_archive();
    let plugin_path = tempdir.path().join(".lix/plugins/plugin_csv.lixplugin");
    std::fs::create_dir_all(plugin_path.parent().unwrap()).unwrap();
    std::fs::write(&plugin_path, &archive).unwrap();

    let lix = open_lix_with_filesystem(tempdir.path()).await;

    let plugins = list_installed_plugins(&lix).await;
    assert_eq!(plugins.len(), 1);
    assert_eq!(plugins[0].key, "plugin_csv");
    assert_eq!(
        read_file(&lix, "/.lix/plugins/plugin_csv.lixplugin")
            .await
            .unwrap()
            .as_deref(),
        Some(archive.as_slice())
    );
    lix.close().await.unwrap();
}

#[derive(Debug, Clone, PartialEq)]
struct FileChange {
    schema_key: String,
    entity_pk: serde_json::Value,
    snapshot_content: Option<serde_json::Value>,
}

async fn file_changes(lix: &Lix, file_id: &str) -> Vec<FileChange> {
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

async fn open_lix_with_filesystem(path: &Path) -> Lix<LocalFilesystem> {
    let storage = LocalFilesystem::open(path).await.unwrap();
    open_lix_with_storage(storage).await.unwrap()
}

async fn install_plugin<StorageImpl>(
    lix: &Lix<StorageImpl>,
    key: &str,
    archive: &[u8],
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    write_file(
        lix,
        &format!("/.lix/plugins/{key}.lixplugin"),
        archive.to_vec(),
    )
    .await
}

async fn write_file<StorageImpl>(
    lix: &Lix<StorageImpl>,
    path: &str,
    data: Vec<u8>,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_file (path, data) VALUES ($1, $2) \
         ON CONFLICT (path) DO UPDATE SET data = excluded.data",
        &[Value::Text(path.to_string()), Value::Blob(data.into())],
    )
    .await?;
    Ok(())
}

async fn read_file<StorageImpl>(
    lix: &Lix<StorageImpl>,
    path: &str,
) -> Result<Option<Vec<u8>>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let result = lix
        .execute(
            "SELECT data FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await?;
    result
        .rows()
        .first()
        .map(|row| row.get::<Vec<u8>>("data"))
        .transpose()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InstalledPluginInfo {
    key: String,
    schema_keys: Vec<String>,
}

async fn list_installed_plugins<StorageImpl>(lix: &Lix<StorageImpl>) -> Vec<InstalledPluginInfo>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let archives = lix
        .execute("SELECT path, data FROM lix_file ORDER BY path", &[])
        .await
        .unwrap();
    archives
        .rows()
        .iter()
        .filter_map(|row| {
            let path = row.get::<String>("path").unwrap();
            if !path.starts_with("/.lix/plugins/") || !path.ends_with(".lixplugin") {
                return None;
            }
            Some(plugin_info_from_archive(
                row.get::<Vec<u8>>("data").unwrap(),
            ))
        })
        .collect()
}

fn plugin_info_from_archive(archive_bytes: Vec<u8>) -> InstalledPluginInfo {
    let mut archive = zip::ZipArchive::new(Cursor::new(archive_bytes)).unwrap();
    let mut manifest_json = String::new();
    archive
        .by_name("manifest.json")
        .unwrap()
        .read_to_string(&mut manifest_json)
        .unwrap();
    let manifest: serde_json::Value = serde_json::from_str(&manifest_json).unwrap();
    let key = manifest["key"].as_str().unwrap().to_string();
    let schema_paths = manifest["schemas"].as_array().unwrap();
    let mut schema_keys = Vec::with_capacity(schema_paths.len());
    for schema_path in schema_paths {
        let mut schema_json = String::new();
        archive
            .by_name(schema_path.as_str().unwrap())
            .unwrap()
            .read_to_string(&mut schema_json)
            .unwrap();
        let schema: serde_json::Value = serde_json::from_str(&schema_json).unwrap();
        schema_keys.push(schema["x-lix-key"].as_str().unwrap().to_string());
    }
    InstalledPluginInfo { key, schema_keys }
}

fn wait_for_disk_file(path: &Path, expected: Option<&[u8]>) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let path_display = path.display();
    loop {
        let actual = std::fs::read(path).ok();
        if actual.as_deref() == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for disk file {path_display} to be {expected:?}, got {actual:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

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
