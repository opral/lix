//! Canonical durable bytes for a declared PostgreSQL `jsonb` cell.
//!
//! The containing typed row authenticates slot placement. This envelope gives
//! the JSONB value its stable semantic identity without per-child hashes.

use serde_json::{Map, Number, Value};

const MAGIC: &[u8; 4] = b"LJCI";
const VERSION: u8 = 1;
const HEADER_BYTES: usize = 41;
const INDEX_PAGE_ENTRIES: usize = 32;
const SMALL_ARRAY_LIMIT: usize = INDEX_PAGE_ENTRIES;
const SMALL_OBJECT_LIMIT: usize = 8;
const MAX_ENCODED_BYTES: usize = u32::MAX as usize;
// A number frame needs a tag, sign, and at most ten bytes for its zigzag i64
// scale. The remaining envelope-addressable bytes are canonical coefficient
// digits. This is arbitrary precision within the explicit v1 cell bound.
const MAX_NUMBER_DIGITS: usize = MAX_ENCODED_BYTES - HEADER_BYTES - 12;

const NULL: u8 = 0;
const FALSE: u8 = 1;
const TRUE: u8 = 2;
const NUMBER: u8 = 3;
const STRING: u8 = 4;
const SMALL_ARRAY: u8 = 5;
const INDEXED_ARRAY: u8 = 6;
const SMALL_OBJECT: u8 = 7;
const INDEXED_OBJECT: u8 = 8;

pub(crate) fn encode(value: &Value) -> Result<Vec<u8>, String> {
    let mut value = value.clone();
    normalize(&mut value)?;
    let root = encode_frame(&value)?;
    let total = HEADER_BYTES
        .checked_add(root.len())
        .ok_or("JSONB length overflow")?;
    let total = u32::try_from(total).map_err(|_| "JSONB exceeds u32")?;
    let mut output = Vec::with_capacity(total as usize);
    output.extend_from_slice(MAGIC);
    output.push(VERSION);
    output.extend_from_slice(&total.to_le_bytes());
    output.extend_from_slice(&content_id(&root));
    output.extend_from_slice(&root);
    Ok(output)
}

pub(crate) fn decode(bytes: &[u8]) -> Result<Value, String> {
    if bytes.len() < HEADER_BYTES + 1 {
        return Err("JSONB header is truncated".into());
    }
    if bytes.get(..4) != Some(MAGIC) {
        return Err("JSONB magic mismatch".into());
    }
    if bytes[4] != VERSION {
        return Err(format!("unsupported JSONB version {}", bytes[4]));
    }
    let declared = u32::from_le_bytes(bytes[5..9].try_into().expect("four bytes")) as usize;
    if declared != bytes.len() {
        return Err("JSONB declared length mismatch".into());
    }
    let root = &bytes[HEADER_BYTES..];
    if bytes[9..HEADER_BYTES] != content_id(root) {
        return Err("JSONB content hash mismatch".into());
    }
    decode_frame(root)
}

/// Address used by an indirect durable carrier. The carrier must authenticate
/// this expected ID externally; a digest stored beside replaceable bytes is
/// corruption detection, not substitution authority.
pub(crate) fn durable_cell_id(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key("lix/jsonb/durable-cell/v1");
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

pub(crate) fn decode_bound(bytes: &[u8], expected: [u8; 32]) -> Result<Value, String> {
    if durable_cell_id(bytes) != expected {
        return Err("JSONB durable cell identity mismatch".into());
    }
    decode(bytes)
}

fn content_id(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key("lix/jsonb/canonical-value/v1");
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

fn normalize(value: &mut Value) -> Result<(), String> {
    match value {
        Value::String(value) => reject_nul(value, "string")?,
        Value::Array(values) => {
            for value in values {
                normalize(value)?;
            }
        }
        Value::Object(values) => {
            let old = std::mem::take(values);
            let mut entries = old.into_iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| left.as_bytes().cmp(right.as_bytes()));
            for (key, mut value) in entries {
                reject_nul(&key, "object key")?;
                normalize(&mut value)?;
                values.insert(key, value);
            }
        }
        Value::Number(number) => {
            let canonical = canonical_decimal(&number.to_string())?;
            *number = serde_json::from_str(&canonical)
                .map_err(|error| format!("canonical JSONB number is invalid: {error}"))?;
        }
        Value::Null | Value::Bool(_) => {}
    }
    Ok(())
}

fn canonical_decimal(raw: &str) -> Result<String, String> {
    let (negative, raw) = raw
        .strip_prefix('-')
        .map_or((false, raw), |raw| (true, raw));
    let exponent_at = raw.find(['e', 'E']);
    let (mantissa, exponent) = exponent_at.map_or((raw, "0"), |at| (&raw[..at], &raw[at + 1..]));
    let exponent = exponent
        .parse::<i64>()
        .map_err(|_| "JSONB exponent exceeds i64")?;
    let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if integer.is_empty()
        || !integer.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err("JSONB number has invalid decimal digits".into());
    }
    let mut digits = format!("{integer}{fraction}");
    let Some(first_nonzero) = digits.find(|byte: char| byte != '0') else {
        return Ok("0".into());
    };
    digits.drain(..first_nonzero);
    let mut scale = i64::try_from(fraction.len())
        .map_err(|_| "JSONB fractional scale exceeds i64")?
        .checked_sub(exponent)
        .ok_or("JSONB decimal scale overflow")?;
    while digits.len() > 1 && digits.ends_with('0') {
        digits.pop();
        scale = scale.checked_sub(1).ok_or("JSONB decimal scale overflow")?;
    }
    if digits.len() > MAX_NUMBER_DIGITS {
        return Err("JSONB numeric coefficient exceeds the v1 cell bound".into());
    }
    let sign = if negative { "-" } else { "" };
    if scale == 0 {
        Ok(format!("{sign}{digits}"))
    } else {
        let exponent = scale.checked_neg().ok_or("JSONB exponent overflow")?;
        Ok(format!("{sign}{digits}e{exponent}"))
    }
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
                .map(|(key, value)| Ok((key.as_bytes(), encode_frame(value)?)))
                .collect::<Result<Vec<_>, String>>()?;
            output = encode_object(&entries)?;
        }
    }
    Ok(output)
}

fn encode_number(number: &Number, output: &mut Vec<u8>) -> Result<(), String> {
    let canonical = canonical_decimal(&number.to_string())?;
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
    if children.len() <= SMALL_ARRAY_LIMIT {
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
    if entries.len() <= SMALL_OBJECT_LIMIT {
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
        let mut key_offset = 0_usize;
        push_u32(0, &mut output, "object key offset")?;
        for (key, _) in entries {
            key_offset = key_offset
                .checked_add(key.len())
                .ok_or("object key overflow")?;
            push_u32(key_offset, &mut output, "object key offset")?;
        }
        let mut child_offset = 0_usize;
        push_u32(0, &mut output, "object child offset")?;
        for (_, child) in entries {
            child_offset = child_offset
                .checked_add(child.len())
                .ok_or("object child overflow")?;
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

fn decode_frame(frame: &[u8]) -> Result<Value, String> {
    let (&tag, payload) = frame.split_first().ok_or("empty JSONB frame")?;
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
        SMALL_ARRAY => decode_small_array(payload),
        INDEXED_ARRAY => decode_indexed_array(payload),
        SMALL_OBJECT => decode_small_object(payload),
        INDEXED_OBJECT => decode_indexed_object(payload),
        _ => Err(format!("invalid JSONB tag or payload {tag}")),
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
    if digits.len() > MAX_NUMBER_DIGITS {
        return Err("number coefficient exceeds the v1 cell bound".into());
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
    if canonical_decimal(&number.to_string())? != spelling {
        return Err("number spelling is not canonical".into());
    }
    Ok(Value::Number(number))
}

fn decode_small_array(mut bytes: &[u8]) -> Result<Value, String> {
    let count = take_varint(&mut bytes, "small array count")? as usize;
    if count > SMALL_ARRAY_LIMIT {
        return Err("small array exceeds canonical count".into());
    }
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let child = take_sized(&mut bytes, "small array child")?;
        values.push(decode_frame(child)?);
    }
    require_empty(bytes, "small array")?;
    Ok(Value::Array(values))
}

fn decode_indexed_array(bytes: &[u8]) -> Result<Value, String> {
    let count = read_u32(bytes, 0, "array count")?;
    if count <= SMALL_ARRAY_LIMIT {
        return Err("indexed array is not canonical for its count".into());
    }
    let pages = read_u32(bytes, 4, "array page count")?;
    if pages != count.div_ceil(INDEX_PAGE_ENTRIES) {
        return Err("indexed array page count is not canonical".into());
    }
    let table_end = 8 + (pages + 1).checked_mul(4).ok_or("array table overflow")?;
    let data = bytes
        .get(table_end..)
        .ok_or("array page table is truncated")?;
    let page_slices = slices_from_offsets(bytes, 8, pages, data, "array page", false)?;
    let mut values = Vec::with_capacity(count);
    for mut page in page_slices {
        let expected = (count - values.len()).min(INDEX_PAGE_ENTRIES);
        for _ in 0..expected {
            values.push(decode_frame(take_sized(&mut page, "array child")?)?);
        }
        require_empty(page, "array page")?;
    }
    if values.len() != count {
        return Err("indexed array child count mismatch".into());
    }
    Ok(Value::Array(values))
}

fn decode_small_object(mut bytes: &[u8]) -> Result<Value, String> {
    let count = take_varint(&mut bytes, "small object count")? as usize;
    if count > SMALL_OBJECT_LIMIT {
        return Err("small object exceeds canonical count".into());
    }
    let mut object = Map::new();
    let mut previous: Option<Vec<u8>> = None;
    for _ in 0..count {
        let key = take_sized(&mut bytes, "small object key")?;
        validate_key(key, previous.as_deref())?;
        let child = take_sized(&mut bytes, "small object child")?;
        let key = std::str::from_utf8(key)
            .expect("validated UTF-8")
            .to_owned();
        previous = Some(key.as_bytes().to_vec());
        object.insert(key, decode_frame(child)?);
    }
    require_empty(bytes, "small object")?;
    Ok(Value::Object(object))
}

fn decode_indexed_object(bytes: &[u8]) -> Result<Value, String> {
    let count = read_u32(bytes, 0, "object count")?;
    if count <= SMALL_OBJECT_LIMIT {
        return Err("indexed object is not canonical for its count".into());
    }
    let table = (count + 1).checked_mul(4).ok_or("object table overflow")?;
    let child_offsets = 4 + table;
    let key_data = child_offsets + table;
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
    let keys = slices_from_offsets(bytes, 4, count, keys, "object key", true)?;
    let children =
        slices_from_offsets(bytes, child_offsets, count, children, "object child", false)?;
    let mut object = Map::new();
    let mut previous: Option<Vec<u8>> = None;
    for (key, child) in keys.into_iter().zip(children) {
        validate_key(key, previous.as_deref())?;
        let key = std::str::from_utf8(key)
            .expect("validated UTF-8")
            .to_owned();
        previous = Some(key.as_bytes().to_vec());
        object.insert(key, decode_frame(child)?);
    }
    Ok(Value::Object(object))
}

fn slices_from_offsets<'a>(
    table: &[u8],
    start: usize,
    count: usize,
    data: &'a [u8],
    context: &str,
    allow_empty: bool,
) -> Result<Vec<&'a [u8]>, String> {
    let mut output = Vec::with_capacity(count);
    let mut previous = read_u32(table, start, context)?;
    if previous != 0 {
        return Err(format!("{context} offsets do not start at zero"));
    }
    for index in 0..count {
        let next = read_u32(table, start + (index + 1) * 4, context)?;
        if next < previous || (!allow_empty && next == previous) || next > data.len() {
            return Err(format!("{context} offsets are invalid"));
        }
        output.push(&data[previous..next]);
        previous = next;
    }
    if previous != data.len() {
        return Err(format!("{context} offsets do not span their data"));
    }
    Ok(output)
}

fn take_sized<'a>(bytes: &mut &'a [u8], context: &str) -> Result<&'a [u8], String> {
    let length = take_varint(bytes, context)? as usize;
    let (value, rest) = bytes
        .split_at_checked(length)
        .ok_or_else(|| format!("{context} is truncated"))?;
    if value.is_empty() {
        return Err(format!("{context} is empty"));
    }
    *bytes = rest;
    Ok(value)
}

fn validate_key(key: &[u8], previous: Option<&[u8]>) -> Result<(), String> {
    let key = std::str::from_utf8(key).map_err(|_| "object key is not UTF-8")?;
    reject_nul(key, "object key")?;
    if previous.is_some_and(|previous| previous >= key.as_bytes()) {
        return Err("object keys are not strictly sorted".into());
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

fn require_empty(bytes: &[u8], context: &str) -> Result<(), String> {
    if bytes.is_empty() {
        Ok(())
    } else {
        Err(format!("{context} has trailing bytes"))
    }
}

fn put_varint(mut value: u64, output: &mut Vec<u8>) {
    while value >= 0x80 {
        output.push(value as u8 | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn take_varint(bytes: &mut &[u8], context: &str) -> Result<u64, String> {
    let original = *bytes;
    let mut value = 0;
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
            if consumed > 1 && value < (1_u64 << ((consumed - 1) * 7)) {
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
    fn semantic_equivalence_and_corruption_fail_closed() {
        for equivalents in [
            ["42", "42.0", "4.2e1"],
            ["0", "-0", "0.000e999"],
            [
                "9007199254740993",
                "9007199254740993.0",
                "90071992547409930e-1",
            ],
            [
                "1.234567890123456789",
                "1234567890123456789e-18",
                "1.2345678901234567890",
            ],
            [
                "18446744073709551616000000000000000000",
                "18446744073709551616e18",
                "1.8446744073709551616e37",
            ],
        ] {
            let expected = encode(&serde_json::from_str(equivalents[0]).unwrap()).unwrap();
            for equivalent in equivalents {
                assert_eq!(
                    encode(&serde_json::from_str(equivalent).unwrap()).unwrap(),
                    expected,
                    "{equivalent}"
                );
            }
        }
        for spelling in ["1e9223372036854775807", "1e-9223372036854775807"] {
            let extreme_value: Value = serde_json::from_str(spelling).unwrap();
            let extreme = encode(&extreme_value).expect("extreme valid exponent");
            assert_eq!(decode(&extreme).unwrap(), extreme_value);
        }
        for overflow in ["1e9223372036854775808", "1e-9223372036854775808"] {
            let value: Value = serde_json::from_str(overflow).unwrap();
            assert!(encode(&value).is_err(), "{overflow}");
        }

        let mut substituted = encode(&serde_json::json!({"a": 1, "b": [2, 3]})).unwrap();
        *substituted.last_mut().unwrap() ^= 1;
        assert!(decode(&substituted).is_err());
        let mut wrong_version = encode(&serde_json::json!(1)).unwrap();
        wrong_version[4] = 2;
        assert!(decode(&wrong_version).is_err());

        let original = encode(&serde_json::from_str("9007199254740993").unwrap()).unwrap();
        let replacement = encode(&serde_json::from_str("9007199254740994").unwrap()).unwrap();
        assert_eq!(original.len(), replacement.len());
        let expected = durable_cell_id(&original);
        assert!(decode_bound(&replacement, expected).is_err());

        // Recompute the envelope hash around a deliberately noncanonical
        // coefficient. Canonical decode, rather than the corruption checksum,
        // must reject it.
        let root = [NUMBER, 0, 0, b'4', b'0'];
        let mut noncanonical = Vec::new();
        noncanonical.extend_from_slice(MAGIC);
        noncanonical.push(VERSION);
        noncanonical.extend_from_slice(
            &u32::try_from(HEADER_BYTES + root.len())
                .unwrap()
                .to_le_bytes(),
        );
        noncanonical.extend_from_slice(&content_id(&root));
        noncanonical.extend_from_slice(&root);
        assert!(decode(&noncanonical).is_err());
    }

    #[test]
    fn containers_have_one_canonical_size_class() {
        for count in [0, 1, 8, 9, 32, 33, 1000] {
            let value = Value::Array((0..count).map(Value::from).collect());
            let encoded = encode(&value).unwrap();
            let decoded = decode(&encoded).unwrap();
            assert_eq!(encode(&decoded).unwrap(), encoded);
        }

        let mut object = Map::new();
        object.insert(String::new(), Value::from("empty"));
        for index in 0..8 {
            object.insert(format!("key-{index:02}"), Value::from(index));
        }
        let value = Value::Object(object);
        let encoded = encode(&value).unwrap();
        let decoded = decode(&encoded).unwrap();
        assert_eq!(encode(&decoded).unwrap(), encoded);

        assert!(decode_small_object(&[9]).is_err());
        assert!(decode_indexed_object(&8_u32.to_le_bytes()).is_err());
        assert!(decode_small_array(&[33]).is_err());
        assert!(decode_indexed_array(&32_u32.to_le_bytes()).is_err());
    }
}
