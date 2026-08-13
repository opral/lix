use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PathSegment {
    Key(String),
    Index(usize),
}

pub trait JsonbCodec {
    const NAME: &'static str;

    fn encode(value: &Value) -> Result<Vec<u8>, String>;
    fn decode(bytes: &[u8]) -> Result<Value, String>;
    fn project_path(bytes: &[u8], path: &[PathSegment]) -> Result<Option<Vec<u8>>, String>;
    fn rewrite_path(
        bytes: &[u8],
        path: &[PathSegment],
        replacement: &Value,
    ) -> Result<Vec<u8>, String>;

    fn diff_count(before: &[u8], after: &[u8]) -> Result<usize, String> {
        Ok(semantic_diff_count(
            &Self::decode(before)?,
            &Self::decode(after)?,
        ))
    }
}

pub fn parse_jsonb(raw: &str) -> Result<Value, String> {
    let mut value = serde_json::from_str::<Value>(raw).map_err(|error| error.to_string())?;
    normalize_jsonb(&mut value)?;
    Ok(value)
}

pub fn normalize_jsonb(value: &mut Value) -> Result<(), String> {
    match value {
        Value::String(value) => reject_nul(value)?,
        Value::Array(values) => {
            for value in values {
                normalize_jsonb(value)?;
            }
        }
        Value::Object(values) => {
            let old = std::mem::take(values);
            let mut entries = old.into_iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| left.as_bytes().cmp(right.as_bytes()));
            for (key, mut value) in entries {
                reject_nul(&key)?;
                normalize_jsonb(&mut value)?;
                values.insert(key, value);
            }
        }
        Value::Number(number) if !number.is_i64() && !number.is_u64() => {
            if let Some(number) = number.as_f64() {
                if number.is_finite()
                    && number.fract() == 0.0
                    && number.abs() <= 9_007_199_254_740_992.0
                {
                    *value = Value::from(number as i64);
                }
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
    Ok(())
}

pub fn canonical_text(value: &Value) -> Result<Vec<u8>, String> {
    serde_json::to_vec(value).map_err(|error| error.to_string())
}

pub fn content_id(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key("lix/jsonb/canonical-value/v1");
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

pub fn dictionary_id(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key("lix/jsonb/fixed-dictionary/v1");
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

pub fn value_at_path<'a>(mut value: &'a Value, path: &[PathSegment]) -> Option<&'a Value> {
    for segment in path {
        value = match segment {
            PathSegment::Key(key) => value.as_object()?.get(key)?,
            PathSegment::Index(index) => value.as_array()?.get(*index)?,
        };
    }
    Some(value)
}

pub fn rewrite_value(
    value: &mut Value,
    path: &[PathSegment],
    replacement: Value,
) -> Result<(), String> {
    let Some((last, parents)) = path.split_last() else {
        *value = replacement;
        return Ok(());
    };
    let mut parent = value;
    for segment in parents {
        parent = match segment {
            PathSegment::Key(key) => parent
                .as_object_mut()
                .and_then(|object| object.get_mut(key))
                .ok_or_else(|| format!("missing object path segment {key:?}"))?,
            PathSegment::Index(index) => parent
                .as_array_mut()
                .and_then(|array| array.get_mut(*index))
                .ok_or_else(|| format!("missing array path segment {index}"))?,
        };
    }
    match last {
        PathSegment::Key(key) => {
            parent
                .as_object_mut()
                .ok_or_else(|| "path parent is not an object".to_owned())?
                .insert(key.clone(), replacement);
        }
        PathSegment::Index(index) => {
            let slot = parent
                .as_array_mut()
                .and_then(|array| array.get_mut(*index))
                .ok_or_else(|| format!("missing array path segment {index}"))?;
            *slot = replacement;
        }
    }
    Ok(())
}

pub fn semantic_diff_count(before: &Value, after: &Value) -> usize {
    if before == after {
        return 0;
    }
    match (before, after) {
        (Value::Array(before), Value::Array(after)) => {
            let shared = before.len().min(after.len());
            before[..shared]
                .iter()
                .zip(&after[..shared])
                .map(|(before, after)| semantic_diff_count(before, after))
                .sum::<usize>()
                + before.len().abs_diff(after.len())
        }
        (Value::Object(before), Value::Object(after)) => {
            let mut count = 0;
            let mut before = before.iter().peekable();
            let mut after = after.iter().peekable();
            loop {
                match (before.peek(), after.peek()) {
                    (Some((before_key, before_value)), Some((after_key, after_value))) => {
                        match before_key.as_bytes().cmp(after_key.as_bytes()) {
                            std::cmp::Ordering::Less => {
                                count += 1;
                                before.next();
                            }
                            std::cmp::Ordering::Greater => {
                                count += 1;
                                after.next();
                            }
                            std::cmp::Ordering::Equal => {
                                count += semantic_diff_count(before_value, after_value);
                                before.next();
                                after.next();
                            }
                        }
                    }
                    (Some(_), None) => {
                        count += before.count();
                        break;
                    }
                    (None, Some(_)) => {
                        count += after.count();
                        break;
                    }
                    (None, None) => break,
                }
            }
            count
        }
        _ => 1,
    }
}

fn reject_nul(value: &str) -> Result<(), String> {
    if value.contains('\0') {
        Err("Lix JSONB does not encode Unicode NUL in keys or strings".to_owned())
    } else {
        Ok(())
    }
}

pub fn read_u32(bytes: &[u8], offset: usize, context: &str) -> Result<u32, String> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| format!("{context} offset overflow"))?;
    let raw = bytes
        .get(offset..end)
        .ok_or_else(|| format!("{context} is truncated"))?;
    Ok(u32::from_le_bytes(raw.try_into().expect("four bytes")))
}

pub fn push_u32(output: &mut Vec<u8>, value: usize, context: &str) -> Result<(), String> {
    let value = u32::try_from(value).map_err(|_| format!("{context} exceeds u32"))?;
    output.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jsonb_equivalence_classes_have_one_normalized_value() {
        let one = parse_jsonb("1").unwrap();
        for equivalent in ["1.0", "1e0", "10e-1", " 1 "] {
            assert_eq!(parse_jsonb(equivalent).unwrap(), one, "{equivalent}");
        }
        assert_eq!(parse_jsonb("-0").unwrap(), parse_jsonb("0").unwrap());

        let object = parse_jsonb(r#"{"a":3,"b":2}"#).unwrap();
        for equivalent in [r#" { "b" : 2, "a" : 3 } "#, r#"{"a":1,"\u0061":3,"b":2}"#] {
            assert_eq!(parse_jsonb(equivalent).unwrap(), object, "{equivalent}");
        }
    }

    #[test]
    fn jsonb_rejects_decoded_nul_in_keys_and_values() {
        for invalid in [r#""a\u0000b""#, r#"{"a\u0000b":1}"#] {
            assert!(parse_jsonb(invalid).is_err(), "{invalid}");
        }
    }
}
