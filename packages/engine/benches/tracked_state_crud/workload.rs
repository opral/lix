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

pub(crate) fn fixture_rows() -> Vec<WorkloadRow> {
    let json: JsonValue = serde_json::from_str(PNPM_LOCK_JSON).expect("parse pnpm-lock fixture");
    let mut rows = Vec::new();
    flatten_json("", &json, &mut rows);
    rows.sort_by(|left, right| left.path.cmp(&right.path));
    assert!(rows.len() >= REAL_WORKLOAD_ROWS);
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

fn escape_json_pointer(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}

pub(crate) fn row_label(row_count: usize) -> &'static str {
    match row_count {
        SMOKE_ROWS => "1k",
        REAL_WORKLOAD_ROWS => "10k",
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
