use base64::Engine as _;
use comfy_table::{presets::UTF8_BORDERS_ONLY, Cell, ContentArrangement, Row, Table};
use lix_rs_sdk::{ExecuteResult, QueryResult, Value};
use serde_json::Value as JsonValue;

pub fn print_execute_result_table(result: &ExecuteResult) {
    if result.statements.is_empty() {
        println!("OK");
        return;
    }

    let total = result.statements.len();
    for (index, statement) in result.statements.iter().enumerate() {
        println!("Statement {}/{}:", index + 1, total);
        print_query_result_table(statement);
        if index + 1 < total {
            println!();
        }
    }
}

pub fn print_execute_result_json(result: &ExecuteResult) {
    let payload = execute_result_to_json(result);
    println!(
        "{}",
        serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_string())
    );
}

fn execute_result_to_json(result: &ExecuteResult) -> JsonValue {
    serde_json::json!({
        "statements": result.statements.iter().map(query_result_to_json).collect::<Vec<_>>(),
    })
}

fn print_query_result_table(result: &QueryResult) {
    if result.columns.is_empty() && result.rows.is_empty() {
        println!("OK");
        return;
    }

    let mut table = Table::new();
    table
        .load_preset(UTF8_BORDERS_ONLY)
        .set_content_arrangement(ContentArrangement::Dynamic);

    if !result.columns.is_empty() {
        let header = Row::from(result.columns.iter().map(Cell::new).collect::<Vec<_>>());
        table.set_header(header);
    }

    for row in &result.rows {
        let rendered = Row::from(
            row.iter()
                .map(|value| Cell::new(value_to_text(value)))
                .collect::<Vec<_>>(),
        );
        table.add_row(rendered);
    }

    println!("{table}");
    println!("({} rows)", result.rows.len());
}

fn query_result_to_json(result: &QueryResult) -> JsonValue {
    serde_json::json!({
        "columns": result.columns,
        "rows": result.rows.iter().map(|row| row.iter().map(value_to_json).collect::<Vec<_>>()).collect::<Vec<_>>(),
    })
}

fn value_to_text(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Boolean(v) => v.to_string(),
        Value::Integer(v) => v.to_string(),
        Value::Real(v) => v.to_string(),
        Value::Text(v) => v.clone(),
        Value::Json(v) => v.to_string(),
        Value::Blob(bytes) => bytes_to_hex(bytes),
    }
}

fn value_to_json(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Boolean(v) => JsonValue::Bool(*v),
        Value::Integer(v) => serde_json::json!(v),
        Value::Real(v) => serde_json::Number::from_f64(*v)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Value::Text(v) => JsonValue::String(v.clone()),
        Value::Json(v) => v.clone(),
        Value::Blob(bytes) => serde_json::json!({
            "$blob": base64::engine::general_purpose::STANDARD.encode(bytes),
        }),
    }
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2 + 2);
    out.push_str("0x");
    for byte in bytes {
        out.push(hex_digit(byte >> 4));
        out.push(hex_digit(byte & 0x0f));
    }
    out
}

fn hex_digit(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'a' + (value - 10)) as char,
        _ => '0',
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_to_json_uses_blob_tagged_shape() {
        let value = Value::Blob(vec![0x01, 0x02, 0x03]);
        let json = value_to_json(&value);
        assert_eq!(
            json,
            serde_json::json!({
                "$blob": "AQID"
            })
        );
    }

    #[test]
    fn value_to_json_uses_native_scalars() {
        assert_eq!(value_to_json(&Value::Null), JsonValue::Null);
        assert_eq!(value_to_json(&Value::Boolean(true)), JsonValue::Bool(true));
        assert_eq!(value_to_json(&Value::Integer(7)), serde_json::json!(7));
        assert_eq!(value_to_json(&Value::Real(2.5)), serde_json::json!(2.5));
        assert_eq!(
            value_to_json(&Value::Text("hello".to_string())),
            JsonValue::String("hello".to_string())
        );
        assert_eq!(
            value_to_json(&Value::Json(serde_json::json!({"ok": true}))),
            serde_json::json!({"ok": true})
        );
    }

    #[test]
    fn execute_result_to_json_preserves_envelope_and_order() {
        let result = ExecuteResult {
            statements: vec![
                QueryResult {
                    columns: vec!["n".to_string(), "payload".to_string()],
                    rows: vec![
                        vec![Value::Integer(1), Value::Text("a".to_string())],
                        vec![Value::Integer(2), Value::Blob(vec![0x01, 0x02])],
                    ],
                },
                QueryResult {
                    columns: vec![],
                    rows: vec![],
                },
            ],
        };

        assert_eq!(
            execute_result_to_json(&result),
            serde_json::json!({
                "statements": [
                    {
                        "columns": ["n", "payload"],
                        "rows": [
                            [1, "a"],
                            [2, {"$blob": "AQI="}],
                        ],
                    },
                    {
                        "columns": [],
                        "rows": [],
                    },
                ],
            })
        );
    }
}
