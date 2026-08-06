use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use serde_json::Value as JsonValue;

pub(super) fn scalar_inputs(args: &[ColumnarValue]) -> bool {
    args.iter()
        .all(|value| matches!(value, ColumnarValue::Scalar(_)))
}

pub(super) fn json_value_to_serde(array: &dyn Array, row: usize) -> Result<Option<JsonValue>> {
    let Some(raw) = text_like_value(array, row)? else {
        return Ok(None);
    };
    serde_json::from_str::<JsonValue>(&raw)
        .map(Some)
        .map_err(|error| {
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

    for path in &arrays[1..] {
        let Some(segment) = json_path_segment(fn_name, path.as_ref(), row)? else {
            return Ok(None);
        };
        let next = match segment {
            JsonPathSegment::Key(key) => current.get(&key).cloned(),
            JsonPathSegment::Index(index) => current
                .as_array()
                .and_then(|values| values.get(index))
                .cloned(),
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
                    "lix_json_get_text() could not render JSON value: {error}"
                ))
            })
        }
        JsonValue::Null => Ok("null".to_string()),
    }
}

pub(super) fn json_json_value(value: &JsonValue) -> Result<String> {
    serde_json::to_string(value).map_err(|error| {
        DataFusionError::Execution(format!(
            "lix_json_get() could not render JSON value: {error}"
        ))
    })
}

enum JsonPathSegment {
    Key(String),
    Index(usize),
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
                let index = usize::try_from(value).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "{fn_name}() path indexes must be non-negative integers"
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
        "{fn_name}() path arguments must be strings or non-negative integers, got {:?}",
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
