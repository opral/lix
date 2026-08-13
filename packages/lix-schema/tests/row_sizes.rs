//! Bytes per row: the typed-row body layout versus the incumbent typed-slots
//! record, over every Schema v1 fixture, INCLUDING the value bytes.
//!
//! Incumbent format (`packages/lix/src/hot_state/typed_slots.rs` at
//! `f36b3d845`, read from that tree):
//!   3-byte header + 9 bytes of directory per DECLARED column + payload,
//!   where payload is 0 bytes for NULL/ABSENT/bool, 8 for i64/u64/f64, and
//!   the raw UTF-8 bytes for str and json. Primary-key columns are NOT elided.
//!
//! Candidate format: 1-byte header + ceil(k_nullable/8) null bitmap +
//!   fixed area (1/8/8/16 by type) + 2*(nvar-1) offsets + var payload, with
//!   primary-key columns elided (they live in the envelope's `entity_pk`).
//!
//! Values are SYNTHESISED, not sampled from a corpus: each column uses its
//! declared `examples[0]` when the fixture provides one, otherwise a
//! type-appropriate realistic value. That is stated rather than hidden — this
//! measures the LAYOUTS against a fixed value population, not the population.
//!
//! Run nonce: vlayout-8813.

use std::collections::BTreeSet;

use lix_schema::{DataType, Schema};
use lix_schema::value_layout::{BodyColumn, BodyKind, BodyValue, encode_body};
use serde_json::Value;

fn kind_of(data_type: DataType) -> BodyKind {
    match data_type {
        DataType::Text => BodyKind::Text,
        DataType::Uuid => BodyKind::Uuid,
        DataType::BigInt => BodyKind::BigInt,
        DataType::DoublePrecision => BodyKind::DoublePrecision,
        DataType::Boolean => BodyKind::Boolean,
        DataType::Jsonb => BodyKind::Jsonb,
    }
}

/// A realistic value for a column, as both a `BodyValue` and the JSON the
/// incumbent would have stored, so the two formats price the same population.
fn synthesise(name: &str, data_type: DataType, example: Option<&Value>) -> (BodyValue, Value) {
    if let Some(example) = example {
        if let Some(value) = from_json(data_type, example) {
            return (value, example.clone());
        }
    }
    match data_type {
        DataType::Uuid => {
            let uuid = uuid::Uuid::parse_str("0191b7e4-1f2c-7c3a-9d4e-5f6a7b8c9d0e").unwrap();
            (BodyValue::Uuid(uuid), Value::String(uuid.to_string()))
        }
        DataType::Boolean => (BodyValue::Boolean(true), Value::Bool(true)),
        DataType::BigInt => (BodyValue::BigInt(42), Value::from(42)),
        DataType::DoublePrecision => (BodyValue::DoublePrecision(1.5), Value::from(1.5)),
        DataType::Text => {
            // Length-realistic by role: hashes and timestamps are the common
            // long text columns; everything else gets a short identifier.
            let text = if name.contains("hash") {
                "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262".to_owned()
            } else if name.contains("_at") || name.contains("time") {
                "2026-05-08T17:42:31.123Z".to_owned()
            } else if name.contains("path") {
                "/docs/architecture/storage.md".to_owned()
            } else if name.contains("key") || name.contains("schema") {
                "lix_file_descriptor".to_owned()
            } else {
                "entity_value".to_owned()
            };
            (BodyValue::Text(text.clone()), Value::String(text))
        }
        DataType::Jsonb => {
            let json = if name.contains("entity_pk") {
                serde_json::json!(["0191b7e4-1f2c-7c3a-9d4e-5f6a7b8c9d0e"])
            } else if name.contains("metadata") {
                serde_json::json!({"author": "agent", "source": "cli"})
            } else {
                serde_json::json!({"kind": "paragraph", "text": "the quick brown fox"})
            };
            (BodyValue::Jsonb(json.clone()), json)
        }
    }
}

fn from_json(data_type: DataType, value: &Value) -> Option<BodyValue> {
    Some(match data_type {
        DataType::Text => BodyValue::Text(value.as_str()?.to_owned()),
        DataType::Uuid => BodyValue::Uuid(uuid::Uuid::parse_str(value.as_str()?).ok()?),
        DataType::BigInt => BodyValue::BigInt(value.as_i64()?),
        DataType::DoublePrecision => {
            let number = value.as_f64()?;
            if !number.is_finite() {
                return None;
            }
            BodyValue::DoublePrecision(number)
        }
        DataType::Boolean => BodyValue::Boolean(value.as_bool()?),
        DataType::Jsonb => BodyValue::Jsonb(value.clone()),
    })
}

/// Incumbent typed-slots record size for one row.
fn incumbent_bytes(columns: &[(String, DataType, Value)]) -> usize {
    const HEADER_BYTES: usize = 3;
    const DIRECTORY_ENTRY_BYTES: usize = 9;
    let mut total = HEADER_BYTES + columns.len() * DIRECTORY_ENTRY_BYTES;
    for (_, _, value) in columns {
        total += match value {
            Value::Null | Value::Bool(_) => 0,
            Value::Number(_) => 8,
            Value::String(text) => text.len(),
            // A JSON-declared slot stores the canonical JSON text.
            other => other.to_string().len(),
        };
    }
    total
}

#[test]
fn bytes_per_row_over_every_schema_v1_fixture() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/current");
    let mut paths = std::fs::read_dir(&dir)
        .expect("fixtures/current must exist")
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .collect::<Vec<_>>();
    paths.sort();
    assert!(!paths.is_empty(), "the fixture sweep must not be vacuous");

    let mut candidate_total = 0usize;
    let mut incumbent_total = 0usize;
    let mut rows = Vec::new();

    for path in &paths {
        let text = std::fs::read_to_string(path).unwrap();
        let schema: Schema = lix_schema::from_json(&text)
            .unwrap_or_else(|error| panic!("{} must parse: {error}", path.display()));
        let raw: Value = serde_json::from_str(&text).unwrap();
        let raw_columns = raw["columns"].as_array().unwrap();

        let pk = schema.primary_key.iter().cloned().collect::<BTreeSet<_>>();

        let mut plan = Vec::new();
        let mut values = Vec::new();
        let mut incumbent_columns = Vec::new();

        for (index, column) in schema.columns.iter().enumerate() {
            let example = raw_columns[index]["examples"]
                .as_array()
                .and_then(|examples| examples.first());
            let (body_value, json_value) = synthesise(&column.name, column.data_type, example);

            // The incumbent stores every declared column, PK included.
            incumbent_columns.push((column.name.clone(), column.data_type, json_value));

            // The candidate elides PK columns into the envelope.
            if pk.contains(&column.name) {
                continue;
            }
            plan.push(BodyColumn {
                kind: kind_of(column.data_type),
                nullable: column.nullable,
            });
            values.push(body_value);
        }

        let mut body = Vec::new();
        encode_body(&plan, &values, &mut body)
            .unwrap_or_else(|error| panic!("{} must encode: {error}", path.display()));

        let incumbent = incumbent_bytes(&incumbent_columns);
        candidate_total += body.len();
        incumbent_total += incumbent;
        rows.push((
            path.file_stem().unwrap().to_string_lossy().to_string(),
            schema.columns.len(),
            pk.len(),
            body.len(),
            incumbent,
        ));
    }

    println!("\nvlayout-8813 BYTES PER ROW (values included), {} fixtures", rows.len());
    println!(
        "{:<28} {:>4} {:>3} {:>10} {:>10} {:>8}",
        "schema", "cols", "pk", "candidate", "incumbent", "ratio"
    );
    for (name, cols, pk, candidate, incumbent) in &rows {
        println!(
            "{name:<28} {cols:>4} {pk:>3} {candidate:>10} {incumbent:>10} {:>7.2}x",
            *incumbent as f64 / *candidate as f64
        );
    }
    println!(
        "{:<28} {:>4} {:>3} {candidate_total:>10} {incumbent_total:>10} {:>7.2}x",
        "TOTAL", "", "",
        incumbent_total as f64 / candidate_total as f64
    );
    println!(
        "mean candidate {:.1} B/row, mean incumbent {:.1} B/row",
        candidate_total as f64 / rows.len() as f64,
        incumbent_total as f64 / rows.len() as f64
    );

    // Non-vacuity: the sweep covered every fixture and priced real bytes.
    assert_eq!(rows.len(), paths.len());
    assert!(candidate_total > 0 && incumbent_total > 0);
}

/// Metadata-only comparison, to separate the layout win from the value bytes.
#[test]
fn metadata_bytes_per_row_over_every_schema_v1_fixture() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/current");
    let mut paths = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .collect::<Vec<_>>();
    paths.sort();

    let mut candidate_total = 0usize;
    let mut incumbent_total = 0usize;
    for path in &paths {
        let schema: Schema = lix_schema::from_json(&std::fs::read_to_string(path).unwrap()).unwrap();
        let pk = schema.primary_key.iter().cloned().collect::<BTreeSet<_>>();
        let body_columns = schema
            .columns
            .iter()
            .filter(|column| !pk.contains(&column.name))
            .collect::<Vec<_>>();
        let nullable = body_columns.iter().filter(|column| column.nullable).count();
        let nvar = body_columns
            .iter()
            .filter(|column| matches!(column.data_type, DataType::Text | DataType::Jsonb))
            .count();
        // header + bitmap + offsets. Fixed-area widths are value bytes.
        candidate_total += 1 + nullable.div_ceil(8) + 2 * nvar.saturating_sub(1);
        // header + 9 bytes of directory per declared column.
        incumbent_total += 3 + schema.columns.len() * 9;
    }
    println!(
        "\nvlayout-8813 METADATA ONLY: candidate {:.2} B/row, incumbent {:.2} B/row, {:.2}x",
        candidate_total as f64 / paths.len() as f64,
        incumbent_total as f64 / paths.len() as f64,
        incumbent_total as f64 / candidate_total as f64
    );
    assert!(candidate_total > 0);
}
