use lix_engine::{Lix, LixError};
use serde_json::json;
use std::sync::Arc;
use tokio::runtime::Runtime;

pub const BENCH_STATE_SCHEMA_KEY: &str = "bench_state_schema";
pub const BENCH_STATE_SCHEMA_VERSION: &str = "1";
pub const BENCH_STATE_PLUGIN_KEY: &str = "lix";
pub const BENCH_STATE_FILE_ID: &str = "bench-state-file";

pub fn register_bench_state_schema(runtime: &Runtime, lix: &Arc<Lix>) {
    runtime
        .block_on(lix.register_schema(&json!({
            "x-lix-key": BENCH_STATE_SCHEMA_KEY,
            "x-lix-version": BENCH_STATE_SCHEMA_VERSION,
            "type": "object",
            "properties": {
                "value": { "type": "string" }
            },
            "required": ["value"],
            "additionalProperties": false
        })))
        .expect("bench state schema should register");
}

pub fn build_state_insert_sql_batches(
    row_count: usize,
    chunk_size: usize,
) -> Result<Vec<String>, LixError> {
    build_state_insert_sql_batches_with_range(0, row_count, chunk_size, "value")
}

pub fn build_state_insert_sql_batches_with_prefix(
    row_count: usize,
    chunk_size: usize,
    value_prefix: &str,
) -> Result<Vec<String>, LixError> {
    build_state_insert_sql_batches_with_range(0, row_count, chunk_size, value_prefix)
}

pub fn build_state_insert_sql_batches_with_range(
    start_index: usize,
    row_count: usize,
    chunk_size: usize,
    value_prefix: &str,
) -> Result<Vec<String>, LixError> {
    if chunk_size == 0 {
        return Err(LixError::unknown(
            "state_insert_bulk chunk size must be greater than 0",
        ));
    }

    let mut entries = Vec::with_capacity(row_count);
    for index in start_index..(start_index + row_count) {
        let entity_id = format!("entity-{index:05}");
        let snapshot_content = serde_json::to_string(&json!({
            "value": format!("{value_prefix}-{index:05}")
        }))
        .map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("failed to serialize bench state snapshot: {error}"),
            )
        })?;

        entries.push(format!(
            "('{}', '{}', '{}', '{}', '{}', '{}')",
            escape_sql_string(&entity_id),
            BENCH_STATE_FILE_ID,
            BENCH_STATE_SCHEMA_KEY,
            BENCH_STATE_PLUGIN_KEY,
            BENCH_STATE_SCHEMA_VERSION,
            escape_sql_string(&snapshot_content),
        ));
    }

    let mut statements = Vec::new();
    for chunk in entries.chunks(chunk_size) {
        statements.push(format!(
            "INSERT INTO lix_state (entity_id, file_id, schema_key, plugin_key, schema_version, snapshot_content) VALUES {}",
            chunk.join(", ")
        ));
    }

    Ok(statements)
}

pub fn build_state_update_sql_batches(
    changed_count: usize,
    value_prefix: &str,
) -> Result<Vec<String>, LixError> {
    build_state_update_sql_batches_with_range(0, changed_count, value_prefix)
}

pub fn build_state_update_sql_batches_with_range(
    start_index: usize,
    changed_count: usize,
    value_prefix: &str,
) -> Result<Vec<String>, LixError> {
    let mut statements = Vec::with_capacity(changed_count);
    for index in start_index..(start_index + changed_count) {
        let entity_id = format!("entity-{index:05}");
        let snapshot_content = serde_json::to_string(&json!({
            "value": format!("{value_prefix}-{index:05}")
        }))
        .map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("failed to serialize bench state update snapshot: {error}"),
            )
        })?;

        statements.push(format!(
            "UPDATE lix_state \
             SET snapshot_content = '{}' \
             WHERE schema_key = '{}' \
               AND entity_id = '{}' \
               AND file_id = '{}'",
            escape_sql_string(&snapshot_content),
            BENCH_STATE_SCHEMA_KEY,
            escape_sql_string(&entity_id),
            BENCH_STATE_FILE_ID,
        ));
    }
    Ok(statements)
}

fn escape_sql_string(input: &str) -> String {
    input.replace('\'', "''")
}
