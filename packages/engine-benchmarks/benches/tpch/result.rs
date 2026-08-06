use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use lix::{ExecuteResult, Value};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Cell {
    Null,
    Bool(bool),
    Int(i128),
    Float(f64),
    Text(String),
}

pub(crate) type Rows = Vec<Vec<Cell>>;

pub(crate) fn from_lix(result: &ExecuteResult) -> Rows {
    result
        .rows()
        .iter()
        .map(|row| row.values().iter().map(cell_from_lix).collect())
        .collect()
}

pub(crate) fn from_arrow(batches: &[RecordBatch]) -> Rows {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|array| {
                        let value = ScalarValue::try_from_array(array, row)
                            .expect("DuckDB Arrow result scalar");
                        cell_from_scalar(&value)
                    })
                    .collect(),
            );
        }
    }
    rows
}

pub(crate) fn assert_equivalent(label: &str, lix: &Rows, duckdb: &Rows) {
    assert_eq!(lix.len(), duckdb.len(), "{label} row count");
    for (row_index, (left, right)) in lix.iter().zip(duckdb).enumerate() {
        assert_eq!(
            left.len(),
            right.len(),
            "{label} column count at row {row_index}"
        );
        for (column_index, (left, right)) in left.iter().zip(right).enumerate() {
            let equivalent = match (left, right) {
                (Cell::Float(left), Cell::Float(right)) => {
                    let tolerance = 1e-9 * left.abs().max(right.abs()).max(1.0);
                    (left - right).abs() <= tolerance
                }
                (Cell::Int(left), Cell::Float(right)) | (Cell::Float(right), Cell::Int(left)) => {
                    let left = *left as f64;
                    let tolerance = 1e-9 * left.abs().max(right.abs()).max(1.0);
                    (left - right).abs() <= tolerance
                }
                _ => left == right,
            };
            assert!(
                equivalent,
                "{label} mismatch at row {row_index}, column {column_index}: Lix={left:?}, DuckDB={right:?}"
            );
        }
    }
}

fn cell_from_lix(value: &Value) -> Cell {
    match value {
        Value::Null => Cell::Null,
        Value::Boolean(value) => Cell::Bool(*value),
        Value::Integer(value) => Cell::Int(i128::from(*value)),
        Value::Real(value) => Cell::Float(*value),
        Value::Text(value) => Cell::Text(value.clone()),
        Value::Json(value) => Cell::Text(value.to_string()),
        Value::Blob(value) => Cell::Text(format!("{value:?}")),
    }
}

fn cell_from_scalar(value: &ScalarValue) -> Cell {
    match value {
        ScalarValue::Null => Cell::Null,
        ScalarValue::Boolean(value) => value.map_or(Cell::Null, Cell::Bool),
        ScalarValue::Int8(value) => value.map_or(Cell::Null, |value| Cell::Int(i128::from(value))),
        ScalarValue::Int16(value) => value.map_or(Cell::Null, |value| Cell::Int(i128::from(value))),
        ScalarValue::Int32(value) => value.map_or(Cell::Null, |value| Cell::Int(i128::from(value))),
        ScalarValue::Int64(value) => value.map_or(Cell::Null, |value| Cell::Int(i128::from(value))),
        ScalarValue::UInt8(value) => value.map_or(Cell::Null, |value| Cell::Int(i128::from(value))),
        ScalarValue::UInt16(value) => {
            value.map_or(Cell::Null, |value| Cell::Int(i128::from(value)))
        }
        ScalarValue::UInt32(value) => {
            value.map_or(Cell::Null, |value| Cell::Int(i128::from(value)))
        }
        ScalarValue::UInt64(value) => {
            value.map_or(Cell::Null, |value| Cell::Int(i128::from(value)))
        }
        ScalarValue::Float32(value) => {
            value.map_or(Cell::Null, |value| Cell::Float(f64::from(value)))
        }
        ScalarValue::Float64(value) => value.map_or(Cell::Null, Cell::Float),
        ScalarValue::Utf8(value) | ScalarValue::Utf8View(value) | ScalarValue::LargeUtf8(value) => {
            value.clone().map_or(Cell::Null, Cell::Text)
        }
        ScalarValue::Decimal128(value, _, scale) => value.map_or(Cell::Null, |value| {
            Cell::Float(value as f64 / 10_f64.powi(i32::from(*scale)))
        }),
        other => Cell::Text(other.to_string()),
    }
}
