use base64::Engine as _;
use comfy_table::{presets::UTF8_BORDERS_ONLY, Cell, ContentArrangement, Row, Table};
use lix_rs_sdk::{QueryResult, Value};
use serde_json::Value as JsonValue;

pub fn print_query_result_table(result: &QueryResult) {
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

pub fn print_query_result_json(result: &QueryResult) {
    let payload = serde_json::json!({
        "columns": result.columns,
        "rows": result.rows.iter().map(|row| row.iter().map(value_to_json).collect::<Vec<_>>()).collect::<Vec<_>>(),
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&payload).unwrap_or_else(|_| "{}".to_string())
    );
}

fn value_to_text(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Boolean(v) => v.to_string(),
        Value::Integer(v) => v.to_string(),
        Value::Real(v) => v.to_string(),
        Value::Text(v) => v.clone(),
        Value::Blob(bytes) => bytes_to_hex(bytes),
    }
}

fn value_to_json(value: &Value) -> JsonValue {
    match value {
        Value::Null => serde_json::json!({ "kind": "null", "value": null }),
        Value::Boolean(v) => serde_json::json!({ "kind": "bool", "value": v }),
        Value::Integer(v) => serde_json::json!({ "kind": "int", "value": v }),
        Value::Real(v) => serde_json::json!({ "kind": "float", "value": v }),
        Value::Text(v) => serde_json::json!({ "kind": "text", "value": v }),
        Value::Blob(bytes) => serde_json::json!({
            "kind": "blob",
            "base64": base64::engine::general_purpose::STANDARD.encode(bytes),
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
    fn value_to_json_uses_canonical_blob_shape() {
        let value = Value::Blob(vec![0x01, 0x02, 0x03]);
        let json = value_to_json(&value);
        assert_eq!(
            json,
            serde_json::json!({
                "kind": "blob",
                "base64": "AQID"
            })
        );
    }

    #[test]
    fn value_to_json_uses_canonical_int_shape() {
        let value = Value::Integer(7);
        let json = value_to_json(&value);
        assert_eq!(
            json,
            serde_json::json!({
                "kind": "int",
                "value": 7
            })
        );
    }
}
