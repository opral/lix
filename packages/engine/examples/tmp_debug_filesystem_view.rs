use std::sync::Arc;

use lix_engine::{
    boot, BootArgs, BootKeyValue, ExecuteOptions, MaterializationDebugMode, MaterializationRequest,
    MaterializationScope, NoopWasmRuntime, Value,
};
use serde_json::json;

#[path = "../benches/support/sqlite_backend.rs"]
mod sqlite_backend;

fn text(value: &Value) -> String {
    match value {
        Value::Text(v) => v.clone(),
        Value::Integer(v) => v.to_string(),
        Value::Real(v) => v.to_string(),
        Value::Blob(v) => String::from_utf8_lossy(v).to_string(),
        Value::Null => "NULL".to_string(),
    }
}

fn print_rows(label: &str, rows: &[Vec<Value>]) {
    println!("-- {label} ({} row(s))", rows.len());
    for row in rows {
        let rendered = row.iter().map(text).collect::<Vec<_>>();
        println!("  {}", rendered.join(" | "));
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let backend = sqlite_backend::BenchSqliteBackend::in_memory();
    let mut args = BootArgs::new(Box::new(backend), Arc::new(NoopWasmRuntime::default()));
    args.access_to_internal = true;
    args.key_values.push(BootKeyValue {
        key: "lix_deterministic_mode".to_string(),
        value: json!({"enabled": true}),
        version_id: None,
    });
    let engine = boot(args);
    engine.init().await?;

    let options = ExecuteOptions::default();

    let active_version = engine
        .execute(
            "SELECT av.version_id \
             FROM lix_version v \
             JOIN lix_active_version av ON av.version_id = v.id \
             ORDER BY av.id \
             LIMIT 1",
            &[],
            options.clone(),
        )
        .await?;
    let version_a = text(&active_version.rows[0][0]);

    engine
        .execute(
            "INSERT INTO lix_version (\
             id, name, inherits_from_version_id, hidden, commit_id, working_commit_id\
             ) VALUES (\
             'version-b', 'version-b', $1, 0, 'commit-version-b', 'working-version-b'\
             )",
            &[Value::Text(version_a.clone())],
            options.clone(),
        )
        .await?;

    engine
        .execute(
            "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id) \
             VALUES ('file-shared', '/shared/config.json', 'ignored', $1)",
            &[Value::Text(version_a.clone())],
            options.clone(),
        )
        .await?;
    engine
        .execute(
            "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id) \
             VALUES ('file-shared', '/shared/config.json', 'ignored', 'version-b')",
            &[],
            options.clone(),
        )
        .await?;
    engine
        .execute(
            "UPDATE lix_file_by_version \
             SET path = '/shared/config-renamed.json', data = 'ignored-again' \
             WHERE id = 'file-shared' AND lixcol_version_id = 'version-b'",
            &[],
            options.clone(),
        )
        .await?;

    let commit_count = engine
        .execute("SELECT COUNT(*) FROM lix_commit", &[], options.clone())
        .await?;
    print_rows("commit_count_after_update", &commit_count.rows);

    let version_b_pointer = engine
        .execute(
            "SELECT id, commit_id, working_commit_id \
             FROM lix_version \
             WHERE id = 'version-b'",
            &[],
            options.clone(),
        )
        .await?;
    print_rows("version_b_pointer_after_update", &version_b_pointer.rows);

    let version_b_commit = engine
        .execute(
            "SELECT c.id, c.change_ids \
             FROM lix_commit c \
             JOIN lix_version v ON v.commit_id = c.id \
             WHERE v.id = 'version-b'",
            &[],
            options.clone(),
        )
        .await?;
    print_rows("version_b_commit_after_update", &version_b_commit.rows);

    let descriptor_changes = engine
        .execute(
            "SELECT id, entity_id, schema_key, file_id, created_at \
             FROM lix_internal_change \
             WHERE schema_key = 'lix_file_descriptor' AND entity_id = 'file-shared' \
             ORDER BY created_at DESC, id DESC \
             LIMIT 5",
            &[],
            options.clone(),
        )
        .await?;
    print_rows("internal_descriptor_changes_after_update", &descriptor_changes.rows);

    let commit_changes = engine
        .execute(
            "SELECT id, entity_id, schema_key, created_at, metadata \
             FROM lix_internal_change \
             WHERE schema_key = 'lix_commit' \
             ORDER BY created_at DESC, id DESC \
             LIMIT 5",
            &[],
            options.clone(),
        )
        .await?;
    print_rows("internal_commit_changes_after_update", &commit_changes.rows);

    let commit_snapshots = engine
        .execute(
            "SELECT c.id, s.content \
             FROM lix_internal_change c \
             LEFT JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
             WHERE c.schema_key = 'lix_commit' \
             ORDER BY c.created_at DESC, c.id DESC \
             LIMIT 2",
            &[],
            options.clone(),
        )
        .await?;
    print_rows("internal_commit_snapshots_top2", &commit_snapshots.rows);

    let pointer_changes = engine
        .execute(
            "SELECT c.id, c.entity_id, c.created_at, c.metadata, s.content \
             FROM lix_internal_change c \
             LEFT JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
             WHERE c.schema_key = 'lix_version_pointer' AND c.entity_id = 'version-b' \
             ORDER BY c.created_at DESC, c.id DESC",
            &[],
            options.clone(),
        )
        .await?;
    print_rows("internal_version_pointer_changes_version_b", &pointer_changes.rows);

    let no_materialize = engine
        .execute(
            "SELECT path, data, lixcol_change_id FROM lix_file_by_version \
             WHERE id = 'file-shared' AND lixcol_version_id = 'version-b'",
            &[],
            options.clone(),
        )
        .await?;
    print_rows("row_b_before_materialize", &no_materialize.rows);

    engine
        .materialize(&MaterializationRequest {
            scope: MaterializationScope::Full,
            debug: MaterializationDebugMode::Off,
            debug_row_limit: 1,
        })
        .await?;

    let after_materialize = engine
        .execute(
            "SELECT path, data, lixcol_change_id FROM lix_file_by_version \
             WHERE id = 'file-shared' AND lixcol_version_id = 'version-b'",
            &[],
            options.clone(),
        )
        .await?;
    print_rows("row_b_after_materialize", &after_materialize.rows);

    engine
        .execute(
            "DELETE FROM lix_file_by_version \
             WHERE id = 'file-shared' AND lixcol_version_id = 'version-b'",
            &[],
            options.clone(),
        )
        .await?;

    let by_version_after_delete = engine
        .execute(
            "SELECT id, path, data, lixcol_change_id \
             FROM lix_file_by_version \
             WHERE id = 'file-shared' AND lixcol_version_id = 'version-b'",
            &[],
            options.clone(),
        )
        .await?;
    print_rows("lix_file_by_version_after_delete", &by_version_after_delete.rows);

    let internal_file_descriptor_rows = engine
        .execute(
            "SELECT \
               entity_id, version_id, snapshot_content, change_id, created_at, updated_at, untracked \
             FROM lix_internal_state_vtable \
             WHERE schema_key = 'lix_file_descriptor' \
               AND entity_id = 'file-shared' \
               AND version_id = 'version-b' \
             ORDER BY created_at DESC, change_id DESC",
            &[],
            options.clone(),
        )
        .await?;
    print_rows(
        "internal_vtable_file_descriptor_rows_version_b",
        &internal_file_descriptor_rows.rows,
    );

    let version_b_after_delete = engine
        .execute(
            "SELECT id, commit_id \
             FROM lix_version \
             WHERE id = 'version-b'",
            &[],
            options.clone(),
        )
        .await?;
    print_rows("version_b_after_delete", &version_b_after_delete.rows);

    let version_b_commit_after_delete = engine
        .execute(
            "SELECT c.id, c.change_ids \
             FROM lix_commit c \
             JOIN lix_version v ON v.commit_id = c.id \
             WHERE v.id = 'version-b'",
            &[],
            options.clone(),
        )
        .await?;
    print_rows(
        "version_b_commit_after_delete",
        &version_b_commit_after_delete.rows,
    );

    if let Some(row) = version_b_commit_after_delete.rows.first() {
        let change_ids_json = row
            .get(1)
            .and_then(|value| match value {
                Value::Text(text) => Some(text.clone()),
                _ => None,
            })
            .unwrap_or_else(|| "[]".to_string());
        let parsed_ids: Vec<String> = serde_json::from_str(&change_ids_json).unwrap_or_default();
        if !parsed_ids.is_empty() {
            let in_list = parsed_ids
                .iter()
                .map(|id| format!("'{}'", id.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT c.id, c.entity_id, c.schema_key, c.file_id, c.created_at, s.content \
                 FROM lix_internal_change c \
                 LEFT JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
                 WHERE c.id IN ({in_list}) \
                 ORDER BY c.created_at DESC, c.id DESC"
            );
            let delete_commit_change_rows = engine.execute(&sql, &[], options.clone()).await?;
            print_rows(
                "version_b_commit_change_rows_after_delete",
                &delete_commit_change_rows.rows,
            );
        }
    }

    let effective_by_version_rows = engine
        .execute(
            "SELECT entity_id, version_id, snapshot_content, change_id, created_at, updated_at, untracked \
             FROM lix_state_by_version \
             WHERE schema_key = 'lix_file_descriptor' \
               AND entity_id = 'file-shared' \
               AND version_id = 'version-b'",
            &[],
            options.clone(),
        )
        .await?;
    print_rows("lix_state_by_version_rows_version_b", &effective_by_version_rows.rows);

    Ok(())
}
