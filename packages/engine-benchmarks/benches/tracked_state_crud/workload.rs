use serde_json::Value as JsonValue;

pub(crate) const SMOKE_ROWS: usize = 1_000;
pub(crate) const REAL_WORKLOAD_ROWS: usize = 10_000;

const PNPM_LOCK_JSON: &str = include_str!("../fixtures/pnpm-lock.fixture.json");

#[derive(Clone)]
pub(crate) struct WorkloadRow {
    pub(crate) path: String,
    pub(crate) value_json: String,
    pub(crate) updated_value_json: String,
}

pub(crate) struct UpdateWorkloadRow {
    pub(crate) path: String,
    pub(crate) updated_value_json: String,
}

pub(crate) fn fixture_rows(row_count: usize) -> Vec<WorkloadRow> {
    assert!(row_count > 0, "tracked-state CRUD fixture cannot be empty");
    let json: JsonValue = serde_json::from_str(PNPM_LOCK_JSON).expect("parse pnpm-lock fixture");
    let mut rows = Vec::new();
    flatten_json("", &json, &mut rows);
    rows.sort_by(|left, right| left.path.cmp(&right.path));
    assert!(rows.len() >= REAL_WORKLOAD_ROWS);
    if row_count <= rows.len() {
        rows.truncate(row_count);
        return rows;
    }

    rows.reserve(row_count - rows.len());
    for ordinal in rows.len()..row_count {
        let path = format!("/~lix-scale/{ordinal:09}");
        let value_json = format!(r#"{{"ordinal":{ordinal},"lane":"scale"}}"#);
        let updated_value_json =
            format!(r#"{{"ordinal":{ordinal},"lane":"scale","updated":true}}"#);
        rows.push(WorkloadRow {
            path,
            value_json,
            updated_value_json,
        });
    }
    // Synthetic rows intentionally extend the real fixture rather than
    // replacing it. Restore physical-key order once so dense mutation profiles
    // at 1M rows exercise the same ordered transaction path as the 10k case.
    rows.sort_by(|left, right| left.path.cmp(&right.path));
    rows
}

/// Regenerates only the columns retained by the bound-update parameter page.
/// Keeping the seed-only JSON out of this second generation makes process RSS
/// measure the engine and the active input page, rather than dead fixture data.
pub(crate) fn fixture_update_rows(row_count: usize) -> Vec<UpdateWorkloadRow> {
    assert!(row_count > 0, "tracked-state CRUD fixture cannot be empty");
    let json: JsonValue = serde_json::from_str(PNPM_LOCK_JSON).expect("parse pnpm-lock fixture");
    let mut rows = Vec::new();
    flatten_update_json("", &json, &mut rows);
    rows.sort_by(|left, right| left.path.cmp(&right.path));
    assert!(rows.len() >= REAL_WORKLOAD_ROWS);
    if row_count <= rows.len() {
        rows.truncate(row_count);
        return rows;
    }

    rows.reserve(row_count - rows.len());
    for ordinal in rows.len()..row_count {
        rows.push(UpdateWorkloadRow {
            path: format!("/~lix-scale/{ordinal:09}"),
            updated_value_json: format!(r#"{{"ordinal":{ordinal},"lane":"scale","updated":true}}"#),
        });
    }
    rows.sort_by(|left, right| left.path.cmp(&right.path));
    rows
}

fn flatten_json(path: &str, value: &JsonValue, rows: &mut Vec<WorkloadRow>) {
    if !path.is_empty() {
        let value_json = serde_json::to_string(value).expect("serialize JSON pointer value");
        let updated_value_json = serde_json::to_string(&serde_json::json!({
            "path": path,
            "value": value,
            "updated": true
        }))
        .expect("serialize updated JSON pointer value");
        rows.push(WorkloadRow {
            path: path.to_string(),
            value_json,
            updated_value_json,
        });
    }

    match value {
        JsonValue::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                flatten_json(&format!("{path}/{index}"), item, rows);
            }
        }
        JsonValue::Object(map) => {
            for (key, item) in map {
                flatten_json(&format!("{path}/{}", escape_json_pointer(key)), item, rows);
            }
        }
        _ => {}
    }
}

fn flatten_update_json(path: &str, value: &JsonValue, rows: &mut Vec<UpdateWorkloadRow>) {
    if !path.is_empty() {
        rows.push(UpdateWorkloadRow {
            path: path.to_string(),
            updated_value_json: serde_json::to_string(&serde_json::json!({
                "path": path,
                "value": value,
                "updated": true
            }))
            .expect("serialize updated JSON pointer value"),
        });
    }

    match value {
        JsonValue::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                flatten_update_json(&format!("{path}/{index}"), item, rows);
            }
        }
        JsonValue::Object(map) => {
            for (key, item) in map {
                flatten_update_json(&format!("{path}/{}", escape_json_pointer(key)), item, rows);
            }
        }
        _ => {}
    }
}

fn escape_json_pointer(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}

pub(crate) fn row_label(row_count: usize) -> &'static str {
    match row_count {
        SMOKE_ROWS => "1k",
        REAL_WORKLOAD_ROWS => "10k",
        100_000 => "100k",
        1_000_000 => "1m",
        _ => "custom",
    }
}

pub(crate) fn snapshot_value(path: &str, value_json: &str) -> String {
    format!(r#"{{"path":{},"value":{}}}"#, json_string(path), value_json)
}

pub(crate) fn json_string(value: &str) -> String {
    serde_json::to_string(value).expect("serialize JSON string")
}

pub(crate) fn sql_string(value: &str) -> String {
    value.replace('\'', "''")
}
