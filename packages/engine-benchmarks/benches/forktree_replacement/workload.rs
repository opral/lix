use serde_json::Value as JsonValue;

const CURRENT_APPLICATION_FIXTURE: &str = include_str!("../fixtures/pnpm-lock.fixture.json");
const REAL_FIXTURE_ROWS: usize = 10_000;

#[derive(Clone)]
pub(crate) struct WorkloadRow {
    pub(crate) path: String,
    pub(crate) value_json: String,
    pub(crate) updated_value_json: String,
}

/// Uses the exact pnpm-lock JSON-pointer fixture from the current tracked CRUD
/// benchmark. Larger gates retain those real rows and append deterministic
/// scale rows rather than replacing application provenance with random bytes.
pub(crate) fn fixture_rows(row_count: usize) -> Vec<WorkloadRow> {
    assert!(row_count > 0, "ForkTree fixture cannot be empty");
    let json: JsonValue =
        serde_json::from_str(CURRENT_APPLICATION_FIXTURE).expect("parse pnpm-lock fixture");
    let mut rows = Vec::new();
    flatten_json("", &json, &mut rows);
    rows.sort_by(|left, right| left.path.cmp(&right.path));
    assert!(rows.len() >= REAL_FIXTURE_ROWS);
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
                flatten_json(
                    &format!("{path}/{}", key.replace('~', "~0").replace('/', "~1")),
                    item,
                    rows,
                );
            }
        }
        _ => {}
    }
}
