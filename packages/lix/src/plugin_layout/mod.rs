//! Canonical JSON identity insertion and validation for plugin entity snapshots.

#![allow(clippy::missing_errors_doc)]

use std::{borrow::Cow, cmp::Ordering, fmt};

/// Inserts a generated string identity at a compiled object path and returns
/// canonical JSON bytes. Every segment before the leaf must already be an
/// object; the leaf must be absent.
pub fn insert_generated_id(
    snapshot: &[u8],
    path: &[String],
    generated_id: &str,
) -> Result<Vec<u8>, Error> {
    if path.is_empty() {
        return Err(Error::new("generated identity path is empty"));
    }
    if snapshot.first() != Some(&b'{') || snapshot.last() != Some(&b'}') {
        return Err(Error::new(
            "generated identity snapshot must be a canonical JSON object",
        ));
    }

    let insertion = find_identity_insertion(snapshot, 0, snapshot.len(), path, 0)?;
    let mut field = Vec::with_capacity(path.last().unwrap().len() + generated_id.len() + 7);
    write_json_string(&mut field, path.last().unwrap());
    field.push(b':');
    write_json_string(&mut field, generated_id);

    let separator = usize::from(insertion.needs_comma);
    let mut output = Vec::with_capacity(snapshot.len() + field.len() + separator);
    output.extend_from_slice(&snapshot[..insertion.offset]);
    if insertion.comma_before {
        output.push(b',');
    }
    output.extend_from_slice(&field);
    if insertion.comma_after {
        output.push(b',');
    }
    output.extend_from_slice(&snapshot[insertion.offset..]);
    Ok(output)
}

/// Validates that a complete canonical snapshot contains the generated string
/// identity at the compiled path.
pub fn validate_generated_id(
    snapshot: &[u8],
    path: &[String],
    expected: &str,
) -> Result<(), Error> {
    if path.is_empty() {
        return Err(Error::new("generated identity path is empty"));
    }
    if snapshot.first() != Some(&b'{') || snapshot.last() != Some(&b'}') {
        return Err(Error::new(
            "generated identity snapshot must be a canonical JSON object",
        ));
    }
    validate_identity_at(snapshot, 0, snapshot.len(), path, 0, expected)
}

fn validate_identity_at(
    bytes: &[u8],
    object_start: usize,
    object_end: usize,
    path: &[String],
    depth: usize,
    expected: &str,
) -> Result<(), Error> {
    if object_end <= object_start + 1
        || bytes.get(object_start) != Some(&b'{')
        || bytes.get(object_end - 1) != Some(&b'}')
    {
        return Err(Error::new(format!(
            "generated identity parent '{}' is missing or not an object",
            path[..depth].join(".")
        )));
    }
    let target = &path[depth];
    let mut entry_start = object_start + 1;
    while entry_start < object_end - 1 {
        if bytes.get(entry_start) != Some(&b'\"') {
            return Err(Error::new("canonical JSON object has an invalid key"));
        }
        let key_end = json_string_end(bytes, entry_start, object_end)?;
        let key = decoded_json_key(&bytes[entry_start..key_end])?;
        let ordering = key.as_ref().cmp(target.as_str());
        if ordering == Ordering::Greater {
            break;
        }
        if bytes.get(key_end) != Some(&b':') {
            return Err(Error::new("canonical JSON object key has no value"));
        }
        let value_start = key_end + 1;
        let boundary = json_value_boundary(bytes, value_start, object_end)?;
        if ordering == Ordering::Equal {
            if depth + 1 == path.len() {
                let value = decoded_json_key(&bytes[value_start..boundary.delimiter])
                    .map_err(|_| Error::new("generated identity must be a JSON string"))?;
                if value.as_ref() != expected {
                    return Err(Error::new(
                        "generated identity does not match its create context",
                    ));
                }
                return Ok(());
            }
            return validate_identity_at(
                bytes,
                value_start,
                boundary.delimiter,
                path,
                depth + 1,
                expected,
            );
        }
        if !boundary.has_next {
            break;
        }
        entry_start = boundary.delimiter + 1;
    }
    Err(Error::new(format!(
        "generated identity field '{}' is missing",
        target
    )))
}

#[derive(Clone, Copy)]
struct IdentityInsertion {
    offset: usize,
    needs_comma: bool,
    comma_before: bool,
    comma_after: bool,
}

fn find_identity_insertion(
    bytes: &[u8],
    object_start: usize,
    object_end: usize,
    path: &[String],
    depth: usize,
) -> Result<IdentityInsertion, Error> {
    if object_end <= object_start + 1
        || bytes.get(object_start) != Some(&b'{')
        || bytes.get(object_end - 1) != Some(&b'}')
    {
        return Err(Error::new(format!(
            "generated identity parent '{}' is missing or not an object",
            path[..depth].join(".")
        )));
    }

    let target = &path[depth];
    let mut entry_start = object_start + 1;
    if entry_start == object_end - 1 {
        if depth + 1 != path.len() {
            return Err(Error::new(format!(
                "generated identity parent '{}' is missing or not an object",
                target
            )));
        }
        return Ok(IdentityInsertion {
            offset: entry_start,
            needs_comma: false,
            comma_before: false,
            comma_after: false,
        });
    }

    loop {
        if bytes.get(entry_start) != Some(&b'\"') {
            return Err(Error::new("canonical JSON object has an invalid key"));
        }
        let key_end = json_string_end(bytes, entry_start, object_end)?;
        let key = decoded_json_key(&bytes[entry_start..key_end])?;
        let ordering = key.as_ref().cmp(target.as_str());
        if bytes.get(key_end) != Some(&b':') {
            return Err(Error::new("canonical JSON object key has no value"));
        }
        let value_start = key_end + 1;
        let boundary = json_value_boundary(bytes, value_start, object_end)?;

        if depth + 1 == path.len() {
            match ordering {
                Ordering::Equal => {
                    return Err(Error::new(
                        "snapshot already contains its generated identity",
                    ));
                }
                Ordering::Greater => {
                    return Ok(IdentityInsertion {
                        offset: entry_start,
                        needs_comma: true,
                        comma_before: false,
                        comma_after: true,
                    });
                }
                Ordering::Less => {}
            }
        } else {
            match ordering {
                Ordering::Equal => {
                    return find_identity_insertion(
                        bytes,
                        value_start,
                        boundary.delimiter,
                        path,
                        depth + 1,
                    );
                }
                Ordering::Greater => {
                    return Err(Error::new(format!(
                        "generated identity parent '{}' is missing or not an object",
                        target
                    )));
                }
                Ordering::Less => {}
            }
        }

        if boundary.has_next {
            entry_start = boundary.delimiter + 1;
        } else {
            if depth + 1 != path.len() {
                return Err(Error::new(format!(
                    "generated identity parent '{}' is missing or not an object",
                    target
                )));
            }
            return Ok(IdentityInsertion {
                offset: boundary.delimiter,
                needs_comma: true,
                comma_before: true,
                comma_after: false,
            });
        }
    }
}

fn decoded_json_key(encoded: &[u8]) -> Result<Cow<'_, str>, Error> {
    if encoded.first() != Some(&b'\"') || encoded.last() != Some(&b'\"') {
        return Err(Error::new("canonical JSON string is not quoted"));
    }
    let inner = encoded
        .get(1..encoded.len().saturating_sub(1))
        .ok_or_else(|| Error::new("canonical JSON object has an invalid key"))?;
    if inner.contains(&b'\\') {
        serde_json::from_slice::<String>(encoded)
            .map(Cow::Owned)
            .map_err(|error| Error::new(format!("invalid canonical JSON key: {error}")))
    } else {
        std::str::from_utf8(inner)
            .map(Cow::Borrowed)
            .map_err(|error| Error::new(format!("invalid canonical JSON key: {error}")))
    }
}

fn json_string_end(bytes: &[u8], start: usize, limit: usize) -> Result<usize, Error> {
    let mut escaped = false;
    for (offset, byte) in bytes[start + 1..limit].iter().enumerate() {
        if escaped {
            escaped = false;
        } else if *byte == b'\\' {
            escaped = true;
        } else if *byte == b'\"' {
            return Ok(start + offset + 2);
        }
    }
    Err(Error::new("unterminated canonical JSON string"))
}

struct JsonValueBoundary {
    delimiter: usize,
    has_next: bool,
}

fn json_value_boundary(
    bytes: &[u8],
    start: usize,
    object_end: usize,
) -> Result<JsonValueBoundary, Error> {
    let mut nested = 0_u32;
    let mut in_string = false;
    let mut escaped = false;
    for (offset, byte) in bytes[start..object_end].iter().enumerate() {
        let index = start + offset;
        if in_string {
            if escaped {
                escaped = false;
            } else if *byte == b'\\' {
                escaped = true;
            } else if *byte == b'\"' {
                in_string = false;
            }
            continue;
        }
        match *byte {
            b'\"' => in_string = true,
            b'[' | b'{' => {
                nested = nested
                    .checked_add(1)
                    .ok_or_else(|| Error::new("canonical JSON nesting overflowed"))?;
            }
            b']' | b'}' if nested > 0 => nested -= 1,
            b',' if nested == 0 => {
                return Ok(JsonValueBoundary {
                    delimiter: index,
                    has_next: true,
                });
            }
            b'}' if nested == 0 && index == object_end - 1 => {
                return Ok(JsonValueBoundary {
                    delimiter: index,
                    has_next: false,
                });
            }
            _ => {}
        }
    }
    Err(Error::new("canonical JSON object has no closing boundary"))
}

fn write_json_string(output: &mut Vec<u8>, value: &str) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    output.push(b'"');
    for &byte in value.as_bytes() {
        match byte {
            b'"' => output.extend_from_slice(br#"\""#),
            b'\\' => output.extend_from_slice(br#"\\"#),
            b'\n' => output.extend_from_slice(br#"\n"#),
            b'\r' => output.extend_from_slice(br#"\r"#),
            b'\t' => output.extend_from_slice(br#"\t"#),
            0x08 => output.extend_from_slice(br#"\b"#),
            0x0c => output.extend_from_slice(br#"\f"#),
            0x00..=0x1f => {
                output.extend_from_slice(b"\\u00");
                output.push(HEX[usize::from(byte >> 4)]);
                output.push(HEX[usize::from(byte & 0x0f)]);
            }
            _ => output.push(byte),
        }
    }
    output.push(b'"');
}

/// Invalid canonical JSON identity payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Error {
    message: String,
}

impl Error {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

#[cfg(test)]
mod tests {
    use super::{insert_generated_id, validate_generated_id};

    #[test]
    fn inserts_generated_identity_in_canonical_key_order() {
        let generated = "01920000-0000-7000-8000-000000000001";
        assert_eq!(
            insert_generated_id(br#"{}"#, &["key".to_owned()], generated).unwrap(),
            br#"{"key":"01920000-0000-7000-8000-000000000001"}"#
        );
        assert_eq!(
            insert_generated_id(br#"{"alpha":1,"omega":2}"#, &["key".to_owned()], generated,)
                .unwrap(),
            br#"{"alpha":1,"key":"01920000-0000-7000-8000-000000000001","omega":2}"#
        );
        assert_eq!(
            insert_generated_id(br#"{"alpha":1}"#, &["zeta".to_owned()], generated).unwrap(),
            br#"{"alpha":1,"zeta":"01920000-0000-7000-8000-000000000001"}"#
        );
    }

    #[test]
    fn inserts_generated_identity_through_escaped_parent_key() {
        let snapshot = insert_generated_id(
            br#"{"a\tb":{"other":true}}"#,
            &["a\tb".to_owned(), "stable\"key".to_owned()],
            "generated",
        )
        .unwrap();
        assert_eq!(
            snapshot,
            br#"{"a\tb":{"other":true,"stable\"key":"generated"}}"#
        );
    }

    #[test]
    fn rejects_existing_or_missing_generated_identity_paths() {
        assert!(
            insert_generated_id(br#"{"key":"existing"}"#, &["key".to_owned()], "new")
                .unwrap_err()
                .message()
                .contains("already contains")
        );
        assert!(
            insert_generated_id(
                br#"{"identity":null}"#,
                &["identity".to_owned(), "key".to_owned()],
                "new",
            )
            .unwrap_err()
            .message()
            .contains("missing or not an object")
        );
        assert!(
            insert_generated_id(
                br#"{"other":{}}"#,
                &["identity".to_owned(), "key".to_owned()],
                "new",
            )
            .unwrap_err()
            .message()
            .contains("missing or not an object")
        );
    }

    #[test]
    fn validates_complete_generated_identity_snapshots() {
        let path = ["identity".to_owned(), "stable_key".to_owned()];
        validate_generated_id(
            br#"{"identity":{"stable_key":"generated"},"value":1}"#,
            &path,
            "generated",
        )
        .unwrap();
        assert!(
            validate_generated_id(
                br#"{"identity":{"stable_key":"wrong"}}"#,
                &path,
                "generated",
            )
            .unwrap_err()
            .message()
            .contains("does not match")
        );
        assert!(
            validate_generated_id(br#"{"identity":{"stable_key":1}}"#, &path, "generated",)
                .unwrap_err()
                .message()
                .contains("JSON string")
        );
    }
}
