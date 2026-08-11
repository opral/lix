//! Seeds a persistent SlateDB directory with a `tracked_state_crud`-style
//! workload so `storage_layout` can report per-space logical key/value bytes.
//!
//! Usage: `hot_layout_fixture <slatedb-dir> <rows>`
//!
//! The directory must not already contain a repository; the example
//! initializes one, registers the bench's `json_pointer` schema, inserts
//! `<rows>` tracked rows plus a small untracked overlay, flushes, and exits so
//! the directory can be inspected offline.

use std::sync::Arc;

use lix::integration::Engine;
use lix::{PreparedDmlParameterBatch, Value};
use lix_storage_slatedb::SlateDB;

const BOUND_SEED_JSON_SQL: &str =
    "INSERT INTO json_pointer (path, value) VALUES ($1, lix_json($2))";
const CHUNK_ROWS: usize = 1_000;

#[tokio::main]
async fn main() {
    let mut args = std::env::args_os().skip(1);
    let path = args.next().expect("usage: hot_layout_fixture <dir> <rows>");
    let rows: usize = args
        .next()
        .expect("usage: hot_layout_fixture <dir> <rows>")
        .to_string_lossy()
        .parse()
        .expect("row count must be an unsigned integer");

    let storage = SlateDB::open(&path).expect("open SlateDB");
    Engine::initialize(storage.clone())
        .await
        .expect("initialize repository");
    let engine = Engine::new(storage.clone())
        .await
        .expect("open engine over initialized repository");
    let session = engine
        .open_workspace_session()
        .await
        .expect("open workspace session");

    let schema = serde_json::json!({
        "x-lix-key": "json_pointer",
        "x-lix-primary-key": ["/path"],
        "type": "object",
        "required": ["path", "value"],
        "properties": {
            "path": { "type": "string" },
            "value": {
                "type": ["object", "array", "string", "number", "integer", "boolean", "null"]
            }
        },
        "additionalProperties": false
    });
    session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (lix_json($1), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register json_pointer schema");

    let mut inserted = 0_usize;
    while inserted < rows {
        let chunk = CHUNK_ROWS.min(rows - inserted);
        let parameter_rows = (inserted..inserted + chunk).map(|index| {
            vec![
                Value::Text(format!("/fixture/path/{index:08}")),
                Value::Text(format!("{{\"ordinal\":{index},\"payload\":\"row-{index:08}\"}}")),
            ]
        });
        let affected = session
            .execute_prepared_dml_batch(
                Arc::from(BOUND_SEED_JSON_SQL),
                PreparedDmlParameterBatch::from_rows(parameter_rows)
                    .expect("fixture parameter batch is rectangular"),
            )
            .await
            .expect("insert fixture chunk")
            .iter()
            .map(lix::ExecuteResult::rows_affected)
            .sum::<u64>();
        assert_eq!(affected as usize, chunk);
        inserted += chunk;
    }

    // A small untracked overlay so HOT rows carry both domains.
    for index in 0..64_usize {
        session
            .execute(
                "INSERT INTO json_pointer (path, value, lixcol_untracked) \
                 VALUES ($1, lix_json($2), true)",
                &[
                    Value::Text(format!("/fixture/untracked/{index:04}")),
                    Value::Text(format!("\"untracked-{index:04}\"")),
                ],
            )
            .await
            .expect("insert untracked fixture row");
    }

    drop(session);
    drop(engine);
    storage.flush().await.expect("flush SlateDB");
    println!("hot_layout_fixture: seeded {rows} tracked rows at {:?}", path);
}
