use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeListArray, LargeStringArray, ListArray,
    StringArray, StringViewArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use serde_json::Value as JsonValue;

/// Parse and normalize the subset of PostgreSQL JSONB represented by Lix.
/// Object order and duplicate spelling are discarded by parsing, JSON numbers
/// are normalized exactly, and NUL is rejected because PostgreSQL's `jsonb`
/// cannot represent it in text values.
pub(crate) fn parse_jsonb(raw: &str) -> std::result::Result<JsonValue, String> {
    let mut value = serde_json::from_str::<JsonValue>(raw).map_err(|error| error.to_string())?;
    normalize_jsonb(&mut value)?;
    Ok(value)
}

pub(crate) fn canonical_jsonb_text(raw: &str) -> std::result::Result<String, String> {
    serde_json::to_string(&parse_jsonb(raw)?).map_err(|error| error.to_string())
}

/// Returns the canonical JSONB comparison key without expanding scientific
/// numeric tokens into large decimal strings.
///
/// This is for SQL equality operands. Storage and public JSON serialization
/// continue to use `canonical_jsonb_text`, which preserves their established
/// number rendering contract.
pub(crate) fn canonical_jsonb_equality_key(raw: &str) -> std::result::Result<String, String> {
    let mut value = serde_json::from_str::<JsonValue>(raw).map_err(|error| error.to_string())?;
    sort_jsonb_keys_and_reject_nul(&mut value)?;
    let compact = serde_json::to_string(&value).map_err(|error| error.to_string())?;
    lix_schema::jsonb_equality_key(&compact).map_err(|error| error.to_string())
}

/// Returns an equality key from a decoded JSON value without parsing its
/// serialized form back into a second DOM. Bound SQL literals arrive here as
/// values already parsed by the binder, but still need recursive key sorting
/// and PostgreSQL JSONB's NUL rejection.
pub(crate) fn jsonb_equality_key_value(value: &JsonValue) -> std::result::Result<String, String> {
    let mut value = value.clone();
    sort_jsonb_keys_and_reject_nul(&mut value)?;
    let compact = serde_json::to_string(&value).map_err(|error| error.to_string())?;
    lix_schema::jsonb_equality_key(&compact).map_err(|error| error.to_string())
}

fn sort_jsonb_keys_and_reject_nul(value: &mut JsonValue) -> std::result::Result<(), String> {
    match value {
        JsonValue::String(value) => reject_jsonb_nul(value),
        JsonValue::Array(values) => {
            for value in values {
                sort_jsonb_keys_and_reject_nul(value)?;
            }
            Ok(())
        }
        JsonValue::Object(values) => {
            values.sort_keys();
            for (key, value) in values.iter_mut() {
                reject_jsonb_nul(key)?;
                sort_jsonb_keys_and_reject_nul(value)?;
            }
            Ok(())
        }
        JsonValue::Number(_) | JsonValue::Null | JsonValue::Bool(_) => Ok(()),
    }
}

pub(crate) fn normalize_jsonb(value: &mut JsonValue) -> std::result::Result<(), String> {
    match value {
        JsonValue::String(value) => reject_jsonb_nul(value)?,
        JsonValue::Array(values) => {
            for value in values {
                normalize_jsonb(value)?;
            }
        }
        JsonValue::Object(values) => {
            // DataFusion enables serde_json's `preserve_order` feature across
            // the workspace. Sort the parsed object in place so JSONB keeps
            // the stable key order used by Lix's previous BTreeMap-backed
            // representation without rebuilding every object in an
            // IndexMap.
            values.sort_keys();
            for (key, value) in values.iter_mut() {
                reject_jsonb_nul(key)?;
                normalize_jsonb(value)?;
            }
        }
        JsonValue::Number(number) => {
            *value = JsonValue::Number(
                lix_schema::normalize_jsonb_number(number).map_err(|error| error.to_string())?,
            );
        }
        JsonValue::Null | JsonValue::Bool(_) => {}
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
    if matches!(
        array.data_type(),
        datafusion::arrow::datatypes::DataType::Null
    ) {
        return Ok(None);
    }
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        return Ok((!array.is_null(row)).then(|| array.value(row).to_string()));
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok((!array.is_null(row)).then(|| array.value(row).to_string()));
    }
    if let Some(array) = array.as_any().downcast_ref::<StringViewArray>() {
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
        return (!array.is_null(row))
            .then(|| utf8_text(array.value(row)).map(str::to_owned))
            .transpose();
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        return (!array.is_null(row))
            .then(|| utf8_text(array.value(row)).map(str::to_owned))
            .transpose();
    }
    Err(DataFusionError::Execution(format!(
        "unsupported argument type for JSON/text function: {:?}",
        array.data_type()
    )))
}

pub(crate) fn utf8_text(bytes: &[u8]) -> Result<&str> {
    std::str::from_utf8(bytes).map_err(|error| {
        DataFusionError::Execution(format!("binary text input is not valid UTF-8: {error}"))
    })
}

pub(crate) fn parse_uuid(raw: &str) -> Result<uuid::Uuid> {
    uuid::Uuid::parse_str(raw)
        .map_err(|error| DataFusionError::Execution(format!("invalid UUID value: {error}")))
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
    constant_path: Option<&[Option<String>]>,
) -> Result<Option<JsonValue>> {
    let Some(mut current) = json_value_to_serde(arrays[0].as_ref(), row)? else {
        return Ok(None);
    };

    if fn_name.contains("path_get") {
        let parsed_path;
        let path = if let Some(path) = constant_path {
            path
        } else {
            let Some(path) = text_array_path(arrays[1].as_ref(), row)? else {
                return Ok(None);
            };
            parsed_path = path;
            &parsed_path
        };
        for segment in path {
            let Some(segment) = segment else {
                return Ok(None);
            };
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

pub(super) fn constant_text_array_path(
    value: &ColumnarValue,
) -> Result<Option<Vec<Option<String>>>> {
    let ColumnarValue::Scalar(value) = value else {
        return Ok(None);
    };
    let array = ColumnarValue::Scalar(value.clone()).into_array_of_size(1)?;
    text_array_path(array.as_ref(), 0)
}

fn text_array_path(array: &dyn Array, row: usize) -> Result<Option<Vec<Option<String>>>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let (values, start, end): (&dyn Array, usize, usize) =
        if let Some(array) = array.as_any().downcast_ref::<ListArray>() {
            let offsets = array.value_offsets();
            (
                array.values().as_ref(),
                offsets[row] as usize,
                offsets[row + 1] as usize,
            )
        } else if let Some(array) = array.as_any().downcast_ref::<LargeListArray>() {
            let offsets = array.value_offsets();
            (
                array.values().as_ref(),
                usize::try_from(offsets[row]).map_err(|error| {
                    DataFusionError::Execution(format!("invalid JSONB path offset: {error}"))
                })?,
                usize::try_from(offsets[row + 1]).map_err(|error| {
                    DataFusionError::Execution(format!("invalid JSONB path offset: {error}"))
                })?,
            )
        } else {
            let Some(path) = text_like_value(array, row)? else {
                return Ok(None);
            };
            return postgres_text_array_path(&path).map(Some);
        };

    let mut path = Vec::with_capacity(end.saturating_sub(start));
    if let Some(values) = values.as_any().downcast_ref::<StringArray>() {
        for index in start..end {
            path.push((!values.is_null(index)).then(|| values.value(index).to_owned()));
        }
    } else if let Some(values) = values.as_any().downcast_ref::<LargeStringArray>() {
        for index in start..end {
            path.push((!values.is_null(index)).then(|| values.value(index).to_owned()));
        }
    } else if let Some(values) = values.as_any().downcast_ref::<StringViewArray>() {
        for index in start..end {
            path.push((!values.is_null(index)).then(|| values.value(index).to_owned()));
        }
    } else {
        return Err(DataFusionError::Execution(format!(
            "JSONB path array must contain text values, got {:?}",
            values.data_type()
        )));
    }
    Ok(Some(path))
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

fn postgres_text_array_path(value: &str) -> Result<Vec<Option<String>>> {
    let value = value.trim();
    let Some(inner) = value
        .strip_prefix('{')
        .and_then(|value| value.strip_suffix('}'))
    else {
        return Err(DataFusionError::Execution(format!(
            "JSONB path must use one-dimensional PostgreSQL text-array syntax such as '{{user,name}}', got '{value}'"
        )));
    };
    if inner.is_empty() {
        return Ok(Vec::new());
    }

    let mut elements = Vec::new();
    let mut chars = inner.chars().peekable();
    loop {
        while chars
            .peek()
            .is_some_and(|character| character.is_whitespace())
        {
            chars.next();
        }
        let mut element = String::new();
        let mut quoted = false;
        let mut escaped = false;
        if chars.peek() == Some(&'"') {
            quoted = true;
            chars.next();
            let mut closed = false;
            while let Some(character) = chars.next() {
                match character {
                    '\\' => {
                        let Some(escaped_character) = chars.next() else {
                            return Err(malformed_postgres_text_array(value));
                        };
                        element.push(escaped_character);
                        escaped = true;
                    }
                    '"' => {
                        closed = true;
                        break;
                    }
                    character => element.push(character),
                }
            }
            if !closed {
                return Err(malformed_postgres_text_array(value));
            }
            while chars
                .peek()
                .is_some_and(|character| character.is_whitespace())
            {
                chars.next();
            }
        } else {
            let mut started = false;
            let mut last_significant_length = 0;
            while let Some(character) = chars.peek().copied() {
                match character {
                    ',' => break,
                    '"' | '{' | '}' => return Err(malformed_postgres_text_array(value)),
                    '\\' => {
                        chars.next();
                        let Some(escaped_character) = chars.next() else {
                            return Err(malformed_postgres_text_array(value));
                        };
                        element.push(escaped_character);
                        started = true;
                        escaped = true;
                        last_significant_length = element.len();
                    }
                    character => {
                        chars.next();
                        if !started && character.is_whitespace() {
                            continue;
                        }
                        element.push(character);
                        started = true;
                        if !character.is_whitespace() {
                            last_significant_length = element.len();
                        }
                    }
                }
            }
            element.truncate(last_significant_length);
        }

        if !quoted && element.is_empty() {
            return Err(malformed_postgres_text_array(value));
        }
        elements.push(
            if !quoted && !escaped && element.eq_ignore_ascii_case("NULL") {
                None
            } else {
                Some(element)
            },
        );

        match chars.next() {
            Some(',') => {
                if chars.peek().is_none() {
                    return Err(malformed_postgres_text_array(value));
                }
            }
            None => break,
            _ => return Err(malformed_postgres_text_array(value)),
        }
    }

    Ok(elements)
}

fn malformed_postgres_text_array(value: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "malformed PostgreSQL text-array JSONB path '{value}'"
    ))
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
        return Ok(Some(JsonPathSegment::Key(value)));
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        if array.is_null(row) {
            return Ok(None);
        }
        let value = array.value(row).to_string();
        return Ok(Some(JsonPathSegment::Key(value)));
    }
    if let Some(array) = array.as_any().downcast_ref::<StringViewArray>() {
        if array.is_null(row) {
            return Ok(None);
        }
        let value = array.value(row).to_string();
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

#[cfg(test)]
mod tests {
    use super::{
        JsonValue, canonical_jsonb_equality_key, canonical_jsonb_text, jsonb_equality_key_value,
    };

    #[test]
    fn canonical_jsonb_collapses_equivalent_numeric_spellings() {
        assert_eq!(canonical_jsonb_text("[42]").unwrap(), "[42]");
        assert_eq!(canonical_jsonb_text("[42.0]").unwrap(), "[42]");
        assert_eq!(canonical_jsonb_text("[4.2e1]").unwrap(), "[42]");
        assert_eq!(canonical_jsonb_text("[ 42 ]").unwrap(), "[42]");
    }

    #[test]
    fn canonical_jsonb_sql_key_sorts_objects_and_keeps_wide_exponents_compact() {
        let exact = format!(r#"{{"n":0.{}1,"z":1}}"#, "0".repeat(322));

        assert_eq!(
            canonical_jsonb_equality_key(r#"{"z":1,"n":1e-323}"#).unwrap(),
            canonical_jsonb_equality_key(&exact).unwrap()
        );
        assert_eq!(
            canonical_jsonb_equality_key(r#"{"n":1e3,"z":1}"#).unwrap(),
            r#"{"n":1e3,"z":1}"#
        );
        assert_eq!(
            canonical_jsonb_equality_key("1e-16383").unwrap(),
            "1e-16383"
        );
    }

    #[test]
    fn decoded_jsonb_literal_key_sorts_without_reparsing_a_dom() {
        let value = serde_json::from_str::<JsonValue>(r#"{"z":1,"a":1000}"#).unwrap();
        assert_eq!(
            jsonb_equality_key_value(&value).unwrap(),
            r#"{"a":1e3,"z":1}"#
        );
    }

    #[test]
    fn jsonb_numeric_scale_limit_is_checked_before_zero_normalization() {
        let fractional_zeroes = "0".repeat(16_384);
        assert!(canonical_jsonb_text(&format!("1.{fractional_zeroes}")).is_err());
        assert!(canonical_jsonb_text(&format!("0.{fractional_zeroes}")).is_err());
        assert!(canonical_jsonb_text("1e-16384").is_err());
        assert_eq!(canonical_jsonb_text("1e-16383").unwrap().len(), 16_385);
    }

    #[test]
    fn sql_and_typed_row_jsonb_share_exact_wide_number_normalization() {
        let raw = format!(
            r#"{{"signed":{},"unsigned":{},"decimal":1.23456789012345678901234567890123456789}}"#,
            i128::MIN,
            u128::MAX,
        );
        let sql = canonical_jsonb_text(&raw).unwrap();
        let value: JsonValue = serde_json::from_str(&raw).unwrap();
        let typed_row = lix_schema::value_layout::canonical_jsonb_bytes(&value).unwrap();
        assert_eq!(sql.as_bytes(), typed_row);
        assert!(sql.contains(&u128::MAX.to_string()));
        assert!(sql.contains("1.23456789012345678901234567890123456789"));
    }

    #[test]
    fn canonical_jsonb_rejects_nul_everywhere() {
        assert!(canonical_jsonb_text(r#"["a\u0000b"]"#).is_err());
        assert!(canonical_jsonb_text(r#"{"a\u0000b":1}"#).is_err());
    }
}
