use serde_json::{Map, Number, Value};

use super::common::{self, JsonbCodec, PathSegment, content_id};

pub struct CompactCodec;

const MAGIC: &[u8; 4] = b"LJCI";
const VERSION: u8 = 1;
const HEADER: usize = 41;
const SMALL_LIMIT: usize = 12;
const INDEX_PAGE_ENTRIES: usize = 32;

const NULL: u8 = 0;
const FALSE: u8 = 1;
const TRUE: u8 = 2;
const NUMBER: u8 = 3;
const STRING: u8 = 4;
const SMALL_ARRAY: u8 = 5;
const INDEXED_ARRAY: u8 = 6;
const SMALL_OBJECT: u8 = 7;
const INDEXED_OBJECT: u8 = 8;

impl JsonbCodec for CompactCodec {
    const NAME: &'static str = "compact-indexed";

    fn encode(value: &Value) -> Result<Vec<u8>, String> {
        let mut value = value.clone();
        common::normalize_jsonb(&mut value)?;
        wrap(encode_frame(&value)?)
    }

    fn decode(bytes: &[u8]) -> Result<Value, String> {
        decode_frame(open(bytes)?)
    }

    fn project_path(bytes: &[u8], path: &[PathSegment]) -> Result<Option<Vec<u8>>, String> {
        let mut frame = open(bytes)?;
        validate_frame(frame)?;
        for segment in path {
            let parsed = Container::parse(frame)?;
            frame = match (parsed, segment) {
                (Container::Array(children), PathSegment::Index(index)) => {
                    let Some(child) = children.get(*index) else {
                        return Ok(None);
                    };
                    child
                }
                (Container::Object(entries), PathSegment::Key(key)) => {
                    let Ok(index) = entries.binary_search_by(|entry| entry.0.cmp(key.as_bytes()))
                    else {
                        return Ok(None);
                    };
                    entries[index].1
                }
                (Container::Scalar, _)
                | (Container::Array(_), PathSegment::Key(_))
                | (Container::Object(_), PathSegment::Index(_)) => return Ok(None),
            };
        }
        validate_frame(frame)?;
        wrap(frame.to_vec()).map(Some)
    }

    fn rewrite_path(
        bytes: &[u8],
        path: &[PathSegment],
        replacement: &Value,
    ) -> Result<Vec<u8>, String> {
        let mut replacement = replacement.clone();
        common::normalize_jsonb(&mut replacement)?;
        let replacement = encode_frame(&replacement)?;
        validate_frame(open(bytes)?)?;
        if path.is_empty() {
            return wrap(replacement);
        }
        wrap(rewrite_frame(open(bytes)?, path, &replacement)?)
    }
}

fn wrap(root: Vec<u8>) -> Result<Vec<u8>, String> {
    let total = HEADER
        .checked_add(root.len())
        .ok_or_else(|| "compact JSONB length overflow".to_owned())?;
    let total = u32::try_from(total).map_err(|_| "compact JSONB exceeds u32".to_owned())?;
    let mut output = Vec::with_capacity(total as usize);
    output.extend_from_slice(MAGIC);
    output.push(VERSION);
    output.extend_from_slice(&total.to_le_bytes());
    output.extend_from_slice(&content_id(&root));
    output.extend_from_slice(&root);
    Ok(output)
}

fn open(bytes: &[u8]) -> Result<&[u8], String> {
    if bytes.len() < HEADER + 1 {
        return Err("compact JSONB header is truncated".to_owned());
    }
    if bytes.get(..4) != Some(MAGIC) {
        return Err("compact JSONB magic mismatch".to_owned());
    }
    if bytes[4] != VERSION {
        return Err(format!("unsupported compact JSONB version {}", bytes[4]));
    }
    let declared = u32::from_le_bytes(bytes[5..9].try_into().expect("four bytes")) as usize;
    if declared != bytes.len() {
        return Err("compact JSONB declared length mismatch".to_owned());
    }
    let root = &bytes[HEADER..];
    if bytes[9..HEADER] != content_id(root) {
        return Err("compact JSONB content hash mismatch".to_owned());
    }
    Ok(root)
}

fn encode_frame(value: &Value) -> Result<Vec<u8>, String> {
    let mut output = Vec::new();
    match value {
        Value::Null => output.push(NULL),
        Value::Bool(false) => output.push(FALSE),
        Value::Bool(true) => output.push(TRUE),
        Value::Number(number) => encode_number(number, &mut output)?,
        Value::String(value) => {
            reject_nul(value, "string")?;
            output.push(STRING);
            output.extend_from_slice(value.as_bytes());
        }
        Value::Array(values) => {
            let children = values
                .iter()
                .map(encode_frame)
                .collect::<Result<Vec<_>, _>>()?;
            output = encode_array(&children)?;
        }
        Value::Object(values) => {
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| left.as_bytes().cmp(right.as_bytes()));
            let entries = entries
                .into_iter()
                .map(|(key, value)| {
                    reject_nul(key, "object key")?;
                    Ok((key.as_bytes(), encode_frame(value)?))
                })
                .collect::<Result<Vec<_>, String>>()?;
            output = encode_object(&entries)?;
        }
    }
    Ok(output)
}

fn encode_number(number: &Number, output: &mut Vec<u8>) -> Result<(), String> {
    let canonical = common::canonical_decimal(&number.to_string())?;
    let (negative, unsigned) = canonical
        .strip_prefix('-')
        .map_or((false, canonical.as_str()), |value| (true, value));
    let (digits, exponent) = unsigned
        .split_once('e')
        .map_or((unsigned, 0_i64), |(digits, exponent)| {
            (digits, exponent.parse().expect("canonical exponent"))
        });
    let scale = exponent
        .checked_neg()
        .ok_or("JSONB numeric scale overflow")?;
    output.push(NUMBER);
    output.push(u8::from(negative));
    put_varint(((scale << 1) ^ (scale >> 63)) as u64, output);
    output.extend_from_slice(digits.as_bytes());
    Ok(())
}

fn encode_array(children: &[Vec<u8>]) -> Result<Vec<u8>, String> {
    let mut output = Vec::new();
    if children.len() <= SMALL_LIMIT {
        output.push(SMALL_ARRAY);
        put_varint(children.len() as u64, &mut output);
        for child in children {
            put_varint(child.len() as u64, &mut output);
            output.extend_from_slice(child);
        }
    } else {
        output.push(INDEXED_ARRAY);
        push_u32(children.len(), &mut output, "array count")?;
        let page_count = children.len().div_ceil(INDEX_PAGE_ENTRIES);
        push_u32(page_count, &mut output, "array page count")?;
        let mut page_data = Vec::new();
        push_u32(0, &mut output, "array page offset")?;
        for page in children.chunks(INDEX_PAGE_ENTRIES) {
            for child in page {
                put_varint(child.len() as u64, &mut page_data);
                page_data.extend_from_slice(child);
            }
            push_u32(page_data.len(), &mut output, "array page offset")?;
        }
        output.extend_from_slice(&page_data);
    }
    Ok(output)
}

fn encode_object(entries: &[(&[u8], Vec<u8>)]) -> Result<Vec<u8>, String> {
    let mut output = Vec::new();
    if entries.len() <= SMALL_LIMIT {
        output.push(SMALL_OBJECT);
        put_varint(entries.len() as u64, &mut output);
        for (key, child) in entries {
            put_varint(key.len() as u64, &mut output);
            output.extend_from_slice(key);
            put_varint(child.len() as u64, &mut output);
            output.extend_from_slice(child);
        }
    } else {
        output.push(INDEXED_OBJECT);
        push_u32(entries.len(), &mut output, "object count")?;
        let mut key_offset = 0usize;
        push_u32(key_offset, &mut output, "object key offset")?;
        for (key, _) in entries {
            key_offset = key_offset
                .checked_add(key.len())
                .ok_or_else(|| "object key data overflow".to_owned())?;
            push_u32(key_offset, &mut output, "object key offset")?;
        }
        let mut child_offset = 0usize;
        push_u32(child_offset, &mut output, "object child offset")?;
        for (_, child) in entries {
            child_offset = child_offset
                .checked_add(child.len())
                .ok_or_else(|| "object child data overflow".to_owned())?;
            push_u32(child_offset, &mut output, "object child offset")?;
        }
        for (key, _) in entries {
            output.extend_from_slice(key);
        }
        for (_, child) in entries {
            output.extend_from_slice(child);
        }
    }
    Ok(output)
}

enum Container<'a> {
    Scalar,
    Array(Vec<&'a [u8]>),
    Object(Vec<(&'a [u8], &'a [u8])>),
}

impl<'a> Container<'a> {
    fn parse(frame: &'a [u8]) -> Result<Self, String> {
        let (&tag, payload) = frame.split_first().ok_or("empty compact JSONB frame")?;
        match tag {
            SMALL_ARRAY => parse_small_array(payload).map(Self::Array),
            INDEXED_ARRAY => parse_indexed_array(payload).map(Self::Array),
            SMALL_OBJECT => parse_small_object(payload).map(Self::Object),
            INDEXED_OBJECT => parse_indexed_object(payload).map(Self::Object),
            NULL | FALSE | TRUE | NUMBER | STRING => Ok(Self::Scalar),
            _ => Err(format!("unknown compact JSONB tag {tag}")),
        }
    }
}

fn parse_small_array(mut bytes: &[u8]) -> Result<Vec<&[u8]>, String> {
    let count = take_varint(&mut bytes, "small array count")? as usize;
    if count > SMALL_LIMIT {
        return Err("small array exceeds its canonical count".to_owned());
    }
    let mut children = Vec::with_capacity(count);
    for _ in 0..count {
        let length = take_varint(&mut bytes, "small array child length")? as usize;
        let (child, rest) = bytes
            .split_at_checked(length)
            .ok_or("small array child is truncated")?;
        if child.is_empty() {
            return Err("small array child is empty".to_owned());
        }
        children.push(child);
        bytes = rest;
    }
    if !bytes.is_empty() {
        return Err("small array has trailing bytes".to_owned());
    }
    Ok(children)
}

fn parse_indexed_array(bytes: &[u8]) -> Result<Vec<&[u8]>, String> {
    let count = read_u32(bytes, 0, "array count")?;
    if count <= SMALL_LIMIT {
        return Err("indexed array is not canonical for its count".to_owned());
    }
    let page_count = read_u32(bytes, 4, "array page count")?;
    let expected_pages = count.div_ceil(INDEX_PAGE_ENTRIES);
    if page_count != expected_pages {
        return Err("indexed array page count is not canonical".to_owned());
    }
    let table_end = 8usize
        .checked_add(
            (page_count + 1)
                .checked_mul(4)
                .ok_or("array page table overflow")?,
        )
        .ok_or("array table overflow")?;
    let data = bytes
        .get(table_end..)
        .ok_or("array page table is truncated")?;
    let pages = slices_from_offsets(bytes, 8, page_count, data, "array page")?;
    let mut children = Vec::with_capacity(count);
    for (page_index, mut page) in pages.into_iter().enumerate() {
        let remaining = count - children.len();
        let expected_children = remaining.min(INDEX_PAGE_ENTRIES);
        for _ in 0..expected_children {
            let length = take_varint(&mut page, "array child length")? as usize;
            let (child, rest) = page
                .split_at_checked(length)
                .ok_or("array child is truncated")?;
            if child.is_empty() {
                return Err("array child is empty".to_owned());
            }
            children.push(child);
            page = rest;
        }
        if !page.is_empty() {
            return Err(format!("array page {page_index} has trailing bytes"));
        }
    }
    if children.len() != count {
        return Err("indexed array child count mismatch".to_owned());
    }
    Ok(children)
}

fn parse_small_object(mut bytes: &[u8]) -> Result<Vec<(&[u8], &[u8])>, String> {
    let count = take_varint(&mut bytes, "small object count")? as usize;
    if count > SMALL_LIMIT {
        return Err("small object exceeds its canonical count".to_owned());
    }
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let key_length = take_varint(&mut bytes, "small object key length")? as usize;
        let (key, rest) = bytes
            .split_at_checked(key_length)
            .ok_or("small object key is truncated")?;
        validate_key(key, entries.last().map(|entry: &(&[u8], &[u8])| entry.0))?;
        bytes = rest;
        let child_length = take_varint(&mut bytes, "small object child length")? as usize;
        let (child, rest) = bytes
            .split_at_checked(child_length)
            .ok_or("small object child is truncated")?;
        if child.is_empty() {
            return Err("small object child is empty".to_owned());
        }
        entries.push((key, child));
        bytes = rest;
    }
    if !bytes.is_empty() {
        return Err("small object has trailing bytes".to_owned());
    }
    Ok(entries)
}

fn parse_indexed_object(bytes: &[u8]) -> Result<Vec<(&[u8], &[u8])>, String> {
    let count = read_u32(bytes, 0, "object count")?;
    if count <= SMALL_LIMIT {
        return Err("indexed object is not canonical for its count".to_owned());
    }
    let table_size = (count + 1).checked_mul(4).ok_or("object table overflow")?;
    let child_offsets = 4usize
        .checked_add(table_size)
        .ok_or("object table overflow")?;
    let key_data = child_offsets
        .checked_add(table_size)
        .ok_or("object table overflow")?;
    let key_total = read_u32(bytes, 4 + count * 4, "terminal key offset")?;
    let child_data = key_data
        .checked_add(key_total)
        .ok_or("object key overflow")?;
    let keys = bytes
        .get(key_data..child_data)
        .ok_or("object keys are truncated")?;
    let children = bytes
        .get(child_data..)
        .ok_or("object children are truncated")?;
    let key_slices = slices_from_offsets(bytes, 4, count, keys, "object key")?;
    let child_slices = slices_from_offsets(bytes, child_offsets, count, children, "object child")?;
    let mut entries = Vec::with_capacity(count);
    for (key, child) in key_slices.into_iter().zip(child_slices) {
        validate_key(key, entries.last().map(|entry: &(&[u8], &[u8])| entry.0))?;
        entries.push((key, child));
    }
    Ok(entries)
}

fn slices_from_offsets<'a>(
    table: &[u8],
    start: usize,
    count: usize,
    data: &'a [u8],
    context: &str,
) -> Result<Vec<&'a [u8]>, String> {
    let mut slices = Vec::with_capacity(count);
    let mut previous = read_u32(table, start, context)?;
    if previous != 0 {
        return Err(format!("{context} offsets do not start at zero"));
    }
    for index in 0..count {
        let next = read_u32(table, start + (index + 1) * 4, context)?;
        if next <= previous || next > data.len() {
            return Err(format!("{context} offsets are invalid"));
        }
        slices.push(&data[previous..next]);
        previous = next;
    }
    if previous != data.len() {
        return Err(format!("{context} offsets do not span their data"));
    }
    Ok(slices)
}

fn decode_frame(frame: &[u8]) -> Result<Value, String> {
    let (&tag, payload) = frame.split_first().ok_or("empty compact JSONB frame")?;
    match tag {
        NULL if payload.is_empty() => Ok(Value::Null),
        FALSE if payload.is_empty() => Ok(Value::Bool(false)),
        TRUE if payload.is_empty() => Ok(Value::Bool(true)),
        NUMBER => decode_number(payload),
        STRING => {
            let value = std::str::from_utf8(payload).map_err(|_| "invalid UTF-8 string")?;
            reject_nul(value, "string")?;
            Ok(Value::String(value.to_owned()))
        }
        SMALL_ARRAY | INDEXED_ARRAY => Container::parse(frame).and_then(|container| {
            let Container::Array(children) = container else {
                unreachable!()
            };
            children
                .into_iter()
                .map(decode_frame)
                .collect::<Result<Vec<_>, _>>()
                .map(Value::Array)
        }),
        SMALL_OBJECT | INDEXED_OBJECT => Container::parse(frame).and_then(|container| {
            let Container::Object(entries) = container else {
                unreachable!()
            };
            let mut object = Map::new();
            for (key, child) in entries {
                object.insert(
                    std::str::from_utf8(key).expect("validated key").to_owned(),
                    decode_frame(child)?,
                );
            }
            Ok(Value::Object(object))
        }),
        _ => Err(format!("invalid compact JSONB tag or payload {tag}")),
    }
}

fn decode_number(mut payload: &[u8]) -> Result<Value, String> {
    let (&negative, rest) = payload.split_first().ok_or("number sign is truncated")?;
    if negative > 1 {
        return Err("number sign is invalid".into());
    }
    payload = rest;
    let raw_scale = take_varint(&mut payload, "number scale")?;
    let scale = ((raw_scale >> 1) as i64) ^ -((raw_scale & 1) as i64);
    let digits = std::str::from_utf8(payload).map_err(|_| "number digits are not UTF-8")?;
    if digits.len() > common::MAX_NUMBER_DIGITS {
        return Err("number coefficient exceeds the v1 cell bound".to_owned());
    }
    if digits.is_empty()
        || !digits.bytes().all(|byte| byte.is_ascii_digit())
        || (digits.len() > 1 && (digits.starts_with('0') || digits.ends_with('0')))
        || (digits == "0" && (negative != 0 || scale != 0))
    {
        return Err("number coefficient is not canonical".into());
    }
    let sign = if negative == 1 { "-" } else { "" };
    let spelling = if scale == 0 {
        format!("{sign}{digits}")
    } else {
        let exponent = scale.checked_neg().ok_or("number exponent overflow")?;
        format!("{sign}{digits}e{exponent}")
    };
    let number = serde_json::from_str::<Number>(&spelling)
        .map_err(|error| format!("invalid canonical number: {error}"))?;
    if common::canonical_decimal(&number.to_string())? != spelling {
        return Err("number spelling is not canonical".into());
    }
    Ok(Value::Number(number))
}

fn validate_frame(frame: &[u8]) -> Result<(), String> {
    decode_frame(frame).map(|_| ())
}

fn rewrite_frame(
    frame: &[u8],
    path: &[PathSegment],
    replacement: &[u8],
) -> Result<Vec<u8>, String> {
    let (segment, rest) = path.split_first().expect("nonempty path");
    match (Container::parse(frame)?, segment) {
        (Container::Array(children), PathSegment::Index(index)) => {
            if *index >= children.len() {
                return Err(format!("missing array path segment {index}"));
            }
            let mut encoded = children.into_iter().map(<[u8]>::to_vec).collect::<Vec<_>>();
            encoded[*index] = if rest.is_empty() {
                replacement.to_vec()
            } else {
                rewrite_frame(&encoded[*index], rest, replacement)?
            };
            encode_array(&encoded)
        }
        (Container::Object(entries), PathSegment::Key(key)) => {
            reject_nul(key, "path key")?;
            let sought = key.as_bytes();
            let position = entries.binary_search_by(|entry| entry.0.cmp(sought));
            let mut encoded = Vec::with_capacity(entries.len() + usize::from(position.is_err()));
            match position {
                Ok(found) => {
                    for (index, (existing, child)) in entries.into_iter().enumerate() {
                        let child = if index == found {
                            if rest.is_empty() {
                                replacement.to_vec()
                            } else {
                                rewrite_frame(child, rest, replacement)?
                            }
                        } else {
                            child.to_vec()
                        };
                        encoded.push((existing, child));
                    }
                }
                Err(insert) if rest.is_empty() => {
                    for (index, (existing, child)) in entries.into_iter().enumerate() {
                        if index == insert {
                            encoded.push((sought, replacement.to_vec()));
                        }
                        encoded.push((existing, child.to_vec()));
                    }
                    if insert == encoded.len() {
                        encoded.push((sought, replacement.to_vec()));
                    }
                }
                Err(_) => return Err(format!("missing object path segment {key:?}")),
            }
            encode_object(&encoded)
        }
        _ => Err("path container type mismatch".to_owned()),
    }
}

fn validate_key(key: &[u8], previous: Option<&[u8]>) -> Result<(), String> {
    let key_text = std::str::from_utf8(key).map_err(|_| "object key is not UTF-8")?;
    reject_nul(key_text, "object key")?;
    if previous.is_some_and(|previous| previous >= key) {
        return Err("object keys are not strictly sorted".to_owned());
    }
    Ok(())
}

fn reject_nul(value: &str, context: &str) -> Result<(), String> {
    if value.contains('\0') {
        Err(format!("{context} contains Unicode NUL"))
    } else {
        Ok(())
    }
}

fn put_varint(mut value: u64, output: &mut Vec<u8>) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn take_varint(bytes: &mut &[u8], context: &str) -> Result<u64, String> {
    let original = *bytes;
    let mut value = 0u64;
    for index in 0..10 {
        let byte = *original
            .get(index)
            .ok_or_else(|| format!("{context} varint is truncated"))?;
        if index == 9 && byte > 1 {
            return Err(format!("{context} varint overflows u64"));
        }
        value |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            let consumed = index + 1;
            if consumed > 1 && value < (1u64 << ((consumed - 1) * 7)) {
                return Err(format!("{context} varint is not canonical"));
            }
            *bytes = &original[consumed..];
            return Ok(value);
        }
    }
    Err(format!("{context} varint is too long"))
}

fn read_u32(bytes: &[u8], offset: usize, context: &str) -> Result<usize, String> {
    let raw = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| format!("{context} is truncated"))?;
    Ok(u32::from_le_bytes(raw.try_into().expect("four bytes")) as usize)
}

fn push_u32(value: usize, output: &mut Vec<u8>, context: &str) -> Result<(), String> {
    output.extend_from_slice(
        &u32::try_from(value)
            .map_err(|_| format!("{context} exceeds u32"))?
            .to_le_bytes(),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_untouched_sibling_fails_projection_and_rewrite() {
        let mut encoded = CompactCodec::encode(&serde_json::json!({"a": 1, "z": 2})).unwrap();
        let child_offset = {
            let Container::Object(entries) = Container::parse(open(&encoded).unwrap()).unwrap()
            else {
                panic!("object")
            };
            entries[1].1.as_ptr() as usize - encoded.as_ptr() as usize
        };
        encoded[child_offset] = 0xff;
        let hash = content_id(&encoded[HEADER..]);
        encoded[9..HEADER].copy_from_slice(&hash);

        assert!(
            CompactCodec::project_path(&encoded, &[PathSegment::Key("missing".into())]).is_err()
        );
        assert!(
            CompactCodec::rewrite_path(
                &encoded,
                &[PathSegment::Key("a".into())],
                &serde_json::json!(3),
            )
            .is_err()
        );
    }

    #[test]
    fn trusted_outer_content_id_rejects_recomputed_substitution() {
        let original = CompactCodec::encode(&serde_json::json!({"a": 1})).unwrap();
        let substituted = CompactCodec::encode(&serde_json::json!({"a": 2})).unwrap();
        assert_eq!(original.len(), substituted.len());
        let trusted_id = content_id(&original);
        assert_ne!(content_id(&substituted), trusted_id);
        assert_eq!(
            CompactCodec::decode(&substituted).unwrap(),
            serde_json::json!({"a": 2})
        );
    }

    #[test]
    fn noncanonical_numeric_coefficient_fails_after_envelope_rehash() {
        let root = [NUMBER, 0, 0, b'4', b'0'];
        let mut encoded = Vec::new();
        encoded.extend_from_slice(MAGIC);
        encoded.push(VERSION);
        encoded.extend_from_slice(&u32::try_from(HEADER + root.len()).unwrap().to_le_bytes());
        encoded.extend_from_slice(&content_id(&root));
        encoded.extend_from_slice(&root);
        assert!(CompactCodec::decode(&encoded).is_err());
    }
}
