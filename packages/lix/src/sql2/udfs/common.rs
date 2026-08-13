use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use serde_json::Value as JsonValue;

/// Parse and normalize the subset of PostgreSQL JSONB represented by Lix.
/// Object order and duplicate spelling are discarded by parsing, numerically
/// integral decimal spellings compare like PostgreSQL numerics, and NUL is
/// rejected because PostgreSQL's `jsonb` cannot represent it in text values.
pub(crate) fn parse_jsonb(raw: &str) -> std::result::Result<JsonValue, String> {
    let mut value = serde_json::from_str::<JsonValue>(raw).map_err(|error| error.to_string())?;
    normalize_jsonb(&mut value)?;
    Ok(value)
}

pub(crate) fn canonical_jsonb_text(raw: &str) -> std::result::Result<String, String> {
    serde_json::to_string(&parse_jsonb(raw)?).map_err(|error| error.to_string())
}

fn normalize_jsonb(value: &mut JsonValue) -> std::result::Result<(), String> {
    match value {
        JsonValue::String(value) => reject_jsonb_nul(value)?,
        JsonValue::Array(values) => {
            for value in values {
                normalize_jsonb(value)?;
            }
        }
        JsonValue::Object(values) => {
            let old = std::mem::take(values);
            let mut entries = old.into_iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));
            for (key, mut value) in entries {
                reject_jsonb_nul(&key)?;
                normalize_jsonb(&mut value)?;
                values.insert(key, value);
            }
        }
        JsonValue::Number(number) if !number.is_i64() && !number.is_u64() => {
            if let Some(number) = number.as_f64()
                && number.is_finite()
                && number.fract() == 0.0
                && number.abs() <= 9_007_199_254_740_992.0
            {
                *value = JsonValue::from(number as i64);
            }
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) => {}
    }
    Ok(())
}

fn reject_jsonb_nul(value: &str) -> std::result::Result<(), String> {
    if value.contains('\0') {
        Err("PostgreSQL JSONB does not support the Unicode NUL escape (\\u0000)".to_owned())
    } else {
        Ok(())
    }
}

pub(super) fn scalar_inputs(args: &[ColumnarValue]) -> bool {
    args.iter()
        .all(|value| matches!(value, ColumnarValue::Scalar(_)))
}

pub(super) fn json_value_to_serde(array: &dyn Array, row: usize) -> Result<Option<JsonValue>> {
    let Some(raw) = text_like_value(array, row)? else {
        return Ok(None);
    };
    parse_jsonb(&raw).map(Some).map_err(|error| {
        DataFusionError::Execution(format!(
            "JSON function expected valid JSON text in its first argument, got error: {error}"
        ))
    })
}

pub(super) fn text_like_value(array: &dyn Array, row: usize) -> Result<Option<String>> {
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        return Ok((!array.is_null(row)).then(|| array.value(row).to_string()));
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok((!array.is_null(row)).then(|| array.value(row).to_string()));
    }
    if let Some(value) = numeric_value(array, row)? {
        return Ok(Some(value));
    }
    if let Some(array) = array.as_any().downcast_ref::<BooleanArray>() {
        return Ok((!array.is_null(row)).then(|| {
            if array.value(row) {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }));
    }
    if let Some(array) = array.as_any().downcast_ref::<BinaryArray>() {
        return Ok(
            (!array.is_null(row)).then(|| String::from_utf8_lossy(array.value(row)).to_string())
        );
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        return Ok(
            (!array.is_null(row)).then(|| String::from_utf8_lossy(array.value(row)).to_string())
        );
    }
    Err(DataFusionError::Execution(format!(
        "unsupported argument type for JSON/text function: {:?}",
        array.data_type()
    )))
}

pub(super) fn numeric_value(array: &dyn Array, row: usize) -> Result<Option<String>> {
    macro_rules! numeric_array {
        ($ty:ty) => {
            if let Some(array) = array.as_any().downcast_ref::<$ty>() {
                return Ok((!array.is_null(row)).then(|| array.value(row).to_string()));
            }
        };
    }

    numeric_array!(Int8Array);
    numeric_array!(Int16Array);
    numeric_array!(Int32Array);
    numeric_array!(Int64Array);
    numeric_array!(UInt8Array);
    numeric_array!(UInt16Array);
    numeric_array!(UInt32Array);
    numeric_array!(UInt64Array);
    numeric_array!(Float32Array);
    numeric_array!(Float64Array);
    Ok(None)
}

pub(super) fn extract_json_path(
    fn_name: &str,
    arrays: &[ArrayRef],
    row: usize,
) -> Result<Option<JsonValue>> {
    let Some(mut current) = json_value_to_serde(arrays[0].as_ref(), row)? else {
        return Ok(None);
    };

    if fn_name.contains("path_get") {
        let Some(path) = text_like_value(arrays[1].as_ref(), row)? else {
            return Ok(None);
        };
        for segment in postgres_text_array_path(&path)? {
            let Some(next) = dynamic_path_get(&current, &segment) else {
                return Ok(None);
            };
            current = next;
        }
        return Ok(Some(current));
    }

    for path in &arrays[1..] {
        let Some(segment) = json_path_segment(fn_name, path.as_ref(), row)? else {
            return Ok(None);
        };
        let next = match segment {
            JsonPathSegment::Key(key) => current.get(&key).cloned(),
            JsonPathSegment::Index(index) => current.as_array().and_then(|values| {
                let index = if index < 0 {
                    i64::try_from(values.len()).ok()?.checked_add(index)?
                } else {
                    index
                };
                usize::try_from(index)
                    .ok()
                    .and_then(|index| values.get(index))
                    .cloned()
            }),
        };
        let Some(value) = next else {
            return Ok(None);
        };
        current = value;
    }

    Ok(Some(current))
}

pub(super) fn json_text_value(value: &JsonValue) -> Result<String> {
    match value {
        JsonValue::String(text) => Ok(text.clone()),
        JsonValue::Number(number) => Ok(number.to_string()),
        JsonValue::Bool(boolean) => Ok(if *boolean {
            "true".to_string()
        } else {
            "false".to_string()
        }),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            serde_json::to_string(value).map_err(|error| {
                DataFusionError::Execution(format!(
                    "JSONB ->> could not render JSON value: {error}"
                ))
            })
        }
        JsonValue::Null => Ok("null".to_string()),
    }
}

pub(super) fn json_json_value(value: &JsonValue) -> Result<String> {
    serde_json::to_string(value).map_err(|error| {
        DataFusionError::Execution(format!("JSONB -> could not render JSON value: {error}"))
    })
}

enum JsonPathSegment {
    Key(String),
    Index(i64),
}

fn dynamic_path_get(value: &JsonValue, segment: &str) -> Option<JsonValue> {
    match value {
        JsonValue::Object(value) => value.get(segment).cloned(),
        JsonValue::Array(value) => {
            let index = segment.parse::<i64>().ok()?;
            let index = if index < 0 {
                i64::try_from(value.len()).ok()?.checked_add(index)?
            } else {
                index
            };
            usize::try_from(index)
                .ok()
                .and_then(|index| value.get(index))
                .cloned()
        }
        _ => None,
    }
}

fn postgres_text_array_path(value: &str) -> Result<Vec<String>> {
    let Some(inner) = value
        .strip_prefix('{')
        .and_then(|value| value.strip_suffix('}'))
    else {
        return Err(DataFusionError::Execution(format!(
            "JSONB path must use PostgreSQL text-array syntax such as '{{user,name}}', got '{value}'"
        )));
    };
    if inner.is_empty() {
        return Ok(Vec::new());
    }
    Ok(inner.split(',').map(str::to_owned).collect())
}

fn json_path_segment(
    fn_name: &str,
    array: &dyn Array,
    row: usize,
) -> Result<Option<JsonPathSegment>> {
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        if array.is_null(row) {
            return Ok(None);
        }
        let value = array.value(row).to_string();
        validate_json_path_key_segment(fn_name, &value)?;
        return Ok(Some(JsonPathSegment::Key(value)));
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        if array.is_null(row) {
            return Ok(None);
        }
        let value = array.value(row).to_string();
        validate_json_path_key_segment(fn_name, &value)?;
        return Ok(Some(JsonPathSegment::Key(value)));
    }
    macro_rules! index_array {
        ($ty:ty) => {
            if let Some(array) = array.as_any().downcast_ref::<$ty>() {
                if array.is_null(row) {
                    return Ok(None);
                }
                let value = array.value(row);
                let index = i64::try_from(value).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "{fn_name}() path index is outside the supported integer range"
                    ))
                })?;
                return Ok(Some(JsonPathSegment::Index(index)));
            }
        };
    }
    index_array!(UInt8Array);
    index_array!(UInt16Array);
    index_array!(UInt32Array);
    index_array!(UInt64Array);
    index_array!(Int8Array);
    index_array!(Int16Array);
    index_array!(Int32Array);
    index_array!(Int64Array);
    Err(DataFusionError::Execution(format!(
        "{fn_name}() path arguments must be strings or integers, got {:?}",
        array.data_type()
    )))
}

fn validate_json_path_key_segment(fn_name: &str, value: &str) -> Result<()> {
    if value == "$" || value.starts_with("$.") || value.starts_with("$[") || value.starts_with('/')
    {
        return Err(DataFusionError::Execution(format!(
            "{fn_name}() uses variadic path segments, not JSONPath or JSON Pointer; got '{value}'"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::canonical_jsonb_text;

    #[test]
    fn canonical_jsonb_collapses_equivalent_numeric_spellings() {
        assert_eq!(canonical_jsonb_text("[42]").unwrap(), "[42]");
        assert_eq!(canonical_jsonb_text("[42.0]").unwrap(), "[42]");
        assert_eq!(canonical_jsonb_text("[4.2e1]").unwrap(), "[42]");
        assert_eq!(canonical_jsonb_text("[ 42 ]").unwrap(), "[42]");
    }

    #[test]
    fn canonical_jsonb_rejects_nul_everywhere() {
        assert!(canonical_jsonb_text(r#"["a\u0000b"]"#).is_err());
        assert!(canonical_jsonb_text(r#"{"a\u0000b":1}"#).is_err());
    }
}
