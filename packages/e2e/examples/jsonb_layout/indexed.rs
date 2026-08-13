use serde_json::{Map, Number, Value};

use super::common::{normalize_jsonb, push_u32, read_u32, JsonbCodec, PathSegment};

const TAG_NULL: u8 = 0;
const TAG_FALSE: u8 = 1;
const TAG_TRUE: u8 = 2;
const TAG_I64: u8 = 3;
const TAG_U64: u8 = 4;
const TAG_F64: u8 = 5;
const TAG_STRING: u8 = 6;
const TAG_ARRAY: u8 = 7;
const TAG_OBJECT: u8 = 8;

const FRAME_HEADER: usize = 5;
const MAGIC: &[u8; 4] = b"LJIX";
const VERSION: u8 = 1;
const DOCUMENT_HEADER: usize = 41;

/// Candidate 2: recursively framed JSONB with indexed arrays and objects.
pub struct IndexedCodec;

impl JsonbCodec for IndexedCodec {
    const NAME: &'static str = "indexed";

    fn encode(value: &Value) -> Result<Vec<u8>, String> {
        let mut value = value.clone();
        normalize_jsonb(&mut value)?;
        wrap_document(encode_value(&value)?)
    }

    fn decode(bytes: &[u8]) -> Result<Value, String> {
        decode_frame(parse_document(bytes)?)
    }

    fn project_path(bytes: &[u8], path: &[PathSegment]) -> Result<Option<Vec<u8>>, String> {
        let mut frame = parse_document(bytes)?;
        for segment in path {
            match (frame.tag, segment) {
                (TAG_ARRAY, PathSegment::Key(_)) => {
                    ArrayIndex::parse(frame.payload)?;
                    return Ok(None);
                }
                (TAG_OBJECT, PathSegment::Index(_)) => {
                    ObjectIndex::parse(frame.payload)?;
                    return Ok(None);
                }
                (TAG_ARRAY | TAG_OBJECT, _) => {}
                _ => {
                    validate_recursive(frame)?;
                    return Ok(None);
                }
            }
            frame = match segment {
                PathSegment::Index(index) if frame.tag == TAG_ARRAY => {
                    let array = ArrayIndex::parse(frame.payload)?;
                    let Some(child) = array.child(*index) else {
                        return Ok(None);
                    };
                    child
                }
                PathSegment::Key(key) if frame.tag == TAG_OBJECT => {
                    let object = ObjectIndex::parse(frame.payload)?;
                    let Some(index) = object.find(key.as_bytes()) else {
                        return Ok(None);
                    };
                    object.child(index)
                }
                _ => return Ok(None),
            };
        }
        validate_recursive(frame)?;
        Ok(Some(wrap_document(frame.bytes.to_vec())?))
    }

    fn rewrite_path(
        bytes: &[u8],
        path: &[PathSegment],
        replacement: &Value,
    ) -> Result<Vec<u8>, String> {
        let root = parse_document(bytes)?;
        let mut replacement_value = replacement.clone();
        normalize_jsonb(&mut replacement_value)?;
        let replacement = encode_value(&replacement_value)?;
        if path.is_empty() {
            validate_recursive(root)?;
            return wrap_document(replacement);
        }
        wrap_document(rewrite_frame(root, path, &replacement)?)
    }
}

fn wrap_document(frame: Vec<u8>) -> Result<Vec<u8>, String> {
    let total = DOCUMENT_HEADER
        .checked_add(frame.len())
        .ok_or_else(|| "indexed document length overflow".to_owned())?;
    let total = u32::try_from(total).map_err(|_| "indexed document exceeds u32".to_owned())?;
    let mut output = Vec::with_capacity(total as usize);
    output.extend_from_slice(MAGIC);
    output.push(VERSION);
    output.extend_from_slice(&total.to_le_bytes());
    output.extend_from_slice(&super::common::content_id(&frame));
    output.extend_from_slice(&frame);
    Ok(output)
}

fn parse_document(bytes: &[u8]) -> Result<Frame<'_>, String> {
    if bytes.len() < DOCUMENT_HEADER {
        return Err("indexed document header is truncated".to_owned());
    }
    if bytes.get(..4) != Some(MAGIC) {
        return Err("invalid indexed document magic".to_owned());
    }
    if bytes[4] != VERSION {
        return Err(format!("unsupported indexed document version {}", bytes[4]));
    }
    let declared = read_u32(bytes, 5, "indexed document length")? as usize;
    if declared != bytes.len() {
        return Err(format!(
            "indexed document length is {declared}, but {} bytes were supplied",
            bytes.len()
        ));
    }
    let frame = &bytes[DOCUMENT_HEADER..];
    if bytes[9..DOCUMENT_HEADER] != super::common::content_id(frame) {
        return Err("indexed document content hash mismatch".to_owned());
    }
    Frame::parse(frame, "root frame")
}

#[derive(Clone, Copy)]
struct Frame<'a> {
    bytes: &'a [u8],
    tag: u8,
    payload: &'a [u8],
}

impl<'a> Frame<'a> {
    fn parse(bytes: &'a [u8], context: &str) -> Result<Self, String> {
        if bytes.len() < FRAME_HEADER {
            return Err(format!("{context} is truncated"));
        }
        let declared = read_u32(bytes, 0, context)? as usize;
        if declared != bytes.len() {
            return Err(format!(
                "{context} length is {declared}, but {} bytes were supplied",
                bytes.len()
            ));
        }
        let tag = bytes[4];
        if tag > TAG_OBJECT {
            return Err(format!("{context} has unknown tag {tag}"));
        }
        Ok(Self {
            bytes,
            tag,
            payload: &bytes[FRAME_HEADER..],
        })
    }
}

fn encode_value(value: &Value) -> Result<Vec<u8>, String> {
    match value {
        Value::Null => make_frame(TAG_NULL, Vec::new()),
        Value::Bool(false) => make_frame(TAG_FALSE, Vec::new()),
        Value::Bool(true) => make_frame(TAG_TRUE, Vec::new()),
        Value::Number(number) => encode_number(number),
        Value::String(value) => {
            reject_nul(value, "string")?;
            make_frame(TAG_STRING, value.as_bytes().to_vec())
        }
        Value::Array(values) => {
            let children = values
                .iter()
                .map(encode_value)
                .collect::<Result<Vec<_>, _>>()?;
            encode_array(&children)
        }
        Value::Object(values) => {
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| left.as_bytes().cmp(right.as_bytes()));
            let entries = entries
                .into_iter()
                .map(|(key, value)| {
                    reject_nul(key, "object key")?;
                    Ok((key.as_bytes(), encode_value(value)?))
                })
                .collect::<Result<Vec<_>, String>>()?;
            encode_object(&entries)
        }
    }
}

fn encode_number(number: &Number) -> Result<Vec<u8>, String> {
    if let Some(number) = number.as_i64() {
        make_frame(TAG_I64, number.to_le_bytes().to_vec())
    } else if let Some(number) = number.as_u64() {
        make_frame(TAG_U64, number.to_le_bytes().to_vec())
    } else {
        let number = number
            .as_f64()
            .ok_or_else(|| "JSON number has no finite representation".to_owned())?;
        if !number.is_finite() {
            return Err("non-finite JSON number".to_owned());
        }
        make_frame(TAG_F64, number.to_bits().to_le_bytes().to_vec())
    }
}

fn make_frame(tag: u8, payload: Vec<u8>) -> Result<Vec<u8>, String> {
    let length = FRAME_HEADER
        .checked_add(payload.len())
        .ok_or_else(|| "frame length overflow".to_owned())?;
    let mut output = Vec::with_capacity(length);
    push_u32(&mut output, length, "frame length")?;
    output.push(tag);
    output.extend_from_slice(&payload);
    Ok(output)
}

fn encode_array(children: &[Vec<u8>]) -> Result<Vec<u8>, String> {
    let offsets = cumulative_offsets(children.iter().map(Vec::len), "array child data")?;
    let mut payload = Vec::new();
    push_u32(&mut payload, children.len(), "array element count")?;
    for offset in offsets {
        push_u32(&mut payload, offset, "array child offset")?;
    }
    for child in children {
        payload.extend_from_slice(child);
    }
    make_frame(TAG_ARRAY, payload)
}

fn encode_object(entries: &[(&[u8], Vec<u8>)]) -> Result<Vec<u8>, String> {
    let key_offsets = cumulative_offsets(entries.iter().map(|(key, _)| key.len()), "object keys")?;
    let child_offsets = cumulative_offsets(
        entries.iter().map(|(_, child)| child.len()),
        "object children",
    )?;
    let mut payload = Vec::new();
    push_u32(&mut payload, entries.len(), "object entry count")?;
    for offset in key_offsets {
        push_u32(&mut payload, offset, "object key offset")?;
    }
    for offset in child_offsets {
        push_u32(&mut payload, offset, "object child offset")?;
    }
    for (key, _) in entries {
        payload.extend_from_slice(key);
    }
    for (_, child) in entries {
        payload.extend_from_slice(child);
    }
    make_frame(TAG_OBJECT, payload)
}

fn cumulative_offsets(
    lengths: impl IntoIterator<Item = usize>,
    context: &str,
) -> Result<Vec<usize>, String> {
    let mut offsets = vec![0];
    let mut offset = 0usize;
    for length in lengths {
        offset = offset
            .checked_add(length)
            .ok_or_else(|| format!("{context} length overflow"))?;
        u32::try_from(offset).map_err(|_| format!("{context} exceeds u32"))?;
        offsets.push(offset);
    }
    Ok(offsets)
}

struct ArrayIndex<'a> {
    children: Vec<Frame<'a>>,
}

impl<'a> ArrayIndex<'a> {
    fn parse(payload: &'a [u8]) -> Result<Self, String> {
        let count = read_u32(payload, 0, "array count")? as usize;
        let offset_count = count
            .checked_add(1)
            .ok_or_else(|| "array offset count overflow".to_owned())?;
        let offsets_size = offset_count
            .checked_mul(4)
            .ok_or_else(|| "array offsets size overflow".to_owned())?;
        let data_start = 4usize
            .checked_add(offsets_size)
            .ok_or_else(|| "array header size overflow".to_owned())?;
        let data = payload
            .get(data_start..)
            .ok_or_else(|| "array header is truncated".to_owned())?;
        let offsets = read_offsets(payload, 4, offset_count, data.len(), true, "array")?;
        let mut children = Vec::with_capacity(count);
        for index in 0..count {
            let child_bytes = &data[offsets[index]..offsets[index + 1]];
            let child = Frame::parse(child_bytes, "array child frame")?;
            children.push(child);
        }
        Ok(Self { children })
    }

    fn child(&self, index: usize) -> Option<Frame<'a>> {
        self.children.get(index).copied()
    }
}

struct ObjectIndex<'a> {
    keys: Vec<&'a [u8]>,
    children: Vec<Frame<'a>>,
}

impl<'a> ObjectIndex<'a> {
    fn parse(payload: &'a [u8]) -> Result<Self, String> {
        let count = read_u32(payload, 0, "object count")? as usize;
        let offset_count = count
            .checked_add(1)
            .ok_or_else(|| "object offset count overflow".to_owned())?;
        let one_offsets_size = offset_count
            .checked_mul(4)
            .ok_or_else(|| "object offsets size overflow".to_owned())?;
        let key_offsets_start = 4usize;
        let child_offsets_start = key_offsets_start
            .checked_add(one_offsets_size)
            .ok_or_else(|| "object header size overflow".to_owned())?;
        let key_data_start = child_offsets_start
            .checked_add(one_offsets_size)
            .ok_or_else(|| "object header size overflow".to_owned())?;
        if key_data_start > payload.len() {
            return Err("object header is truncated".to_owned());
        }

        let key_total = read_u32(
            payload,
            key_offsets_start + count * 4,
            "object terminal key offset",
        )? as usize;
        let child_data_start = key_data_start
            .checked_add(key_total)
            .ok_or_else(|| "object key data size overflow".to_owned())?;
        if child_data_start > payload.len() {
            return Err("object key data is truncated".to_owned());
        }
        let key_data = &payload[key_data_start..child_data_start];
        let child_data = &payload[child_data_start..];
        let key_offsets = read_offsets(
            payload,
            key_offsets_start,
            offset_count,
            key_data.len(),
            false,
            "object key",
        )?;
        let child_offsets = read_offsets(
            payload,
            child_offsets_start,
            offset_count,
            child_data.len(),
            true,
            "object child",
        )?;

        let mut keys = Vec::with_capacity(count);
        let mut children = Vec::with_capacity(count);
        for index in 0..count {
            let key = &key_data[key_offsets[index]..key_offsets[index + 1]];
            let key_text =
                std::str::from_utf8(key).map_err(|_| "object key is not valid UTF-8".to_owned())?;
            reject_nul(key_text, "object key")?;
            if let Some(previous) = keys.last() {
                if *previous >= key {
                    return Err("object keys are duplicated or not strictly sorted".to_owned());
                }
            }
            let child_bytes = &child_data[child_offsets[index]..child_offsets[index + 1]];
            let child = Frame::parse(child_bytes, "object child frame")?;
            keys.push(key);
            children.push(child);
        }
        Ok(Self { keys, children })
    }

    fn find(&self, key: &[u8]) -> Option<usize> {
        self.keys
            .binary_search_by(|candidate| candidate.cmp(&key))
            .ok()
    }

    fn child(&self, index: usize) -> Frame<'a> {
        self.children[index]
    }
}

fn read_offsets(
    bytes: &[u8],
    start: usize,
    count: usize,
    data_len: usize,
    require_nonempty: bool,
    context: &str,
) -> Result<Vec<usize>, String> {
    let table_size = count
        .checked_mul(4)
        .ok_or_else(|| format!("{context} offset table size overflow"))?;
    let end = start
        .checked_add(table_size)
        .ok_or_else(|| format!("{context} offset table overflow"))?;
    if end > bytes.len() {
        return Err(format!("{context} offset table is truncated"));
    }
    let mut offsets = Vec::with_capacity(count);
    for index in 0..count {
        offsets.push(read_u32(bytes, start + index * 4, context)? as usize);
    }
    if offsets.first() != Some(&0) || offsets.last() != Some(&data_len) {
        return Err(format!("{context} offsets do not span their data"));
    }
    for pair in offsets.windows(2) {
        if pair[0] > pair[1] || (require_nonempty && pair[0] == pair[1]) {
            return Err(format!("{context} offsets are not strictly ordered"));
        }
    }
    Ok(offsets)
}

fn validate_recursive(frame: Frame<'_>) -> Result<(), String> {
    match frame.tag {
        TAG_NULL | TAG_FALSE | TAG_TRUE => require_payload_len(frame, 0),
        TAG_I64 | TAG_U64 | TAG_F64 => {
            require_payload_len(frame, 8)?;
            if frame.tag == TAG_U64 {
                let value = u64::from_le_bytes(frame.payload.try_into().expect("eight bytes"));
                if value <= i64::MAX as u64 {
                    return Err("non-canonical u64 tag".to_owned());
                }
            } else if frame.tag == TAG_F64 {
                canonical_f64(frame.payload)?;
            }
            Ok(())
        }
        TAG_STRING => {
            let value = std::str::from_utf8(frame.payload)
                .map_err(|_| "string payload is not valid UTF-8".to_owned())?;
            reject_nul(value, "string")
        }
        TAG_ARRAY => {
            let array = ArrayIndex::parse(frame.payload)?;
            for child in array.children {
                validate_recursive(child)?;
            }
            Ok(())
        }
        TAG_OBJECT => {
            let object = ObjectIndex::parse(frame.payload)?;
            for child in object.children {
                validate_recursive(child)?;
            }
            Ok(())
        }
        _ => unreachable!("Frame::parse rejects unknown tags"),
    }
}

fn require_payload_len(frame: Frame<'_>, expected: usize) -> Result<(), String> {
    if frame.payload.len() == expected {
        Ok(())
    } else {
        Err(format!(
            "tag {} requires a {expected}-byte payload, found {} bytes",
            frame.tag,
            frame.payload.len()
        ))
    }
}

fn canonical_f64(payload: &[u8]) -> Result<f64, String> {
    if payload.len() != 8 {
        return Err(format!(
            "f64 tag requires an 8-byte payload, found {} bytes",
            payload.len()
        ));
    }
    let value = f64::from_bits(u64::from_le_bytes(payload.try_into().expect("eight bytes")));
    if !value.is_finite() {
        return Err("non-finite f64 payload".to_owned());
    }
    if value.fract() == 0.0 && value.abs() <= 9_007_199_254_740_992.0 {
        return Err("non-canonical integral f64 tag".to_owned());
    }
    Ok(value)
}

fn decode_frame(frame: Frame<'_>) -> Result<Value, String> {
    match frame.tag {
        TAG_NULL => {
            require_payload_len(frame, 0)?;
            Ok(Value::Null)
        }
        TAG_FALSE | TAG_TRUE => {
            require_payload_len(frame, 0)?;
            Ok(Value::Bool(frame.tag == TAG_TRUE))
        }
        TAG_I64 => {
            require_payload_len(frame, 8)?;
            Ok(Value::from(i64::from_le_bytes(
                frame.payload.try_into().expect("eight bytes"),
            )))
        }
        TAG_U64 => {
            require_payload_len(frame, 8)?;
            let value = u64::from_le_bytes(frame.payload.try_into().expect("eight bytes"));
            if value <= i64::MAX as u64 {
                return Err("non-canonical u64 tag".to_owned());
            }
            Ok(Value::from(value))
        }
        TAG_F64 => {
            let value = canonical_f64(frame.payload)?;
            let number = Number::from_f64(value)
                .ok_or_else(|| "f64 payload is not a JSON number".to_owned())?;
            Ok(Value::Number(number))
        }
        TAG_STRING => {
            let value = std::str::from_utf8(frame.payload)
                .map_err(|_| "string payload is not valid UTF-8".to_owned())?;
            reject_nul(value, "string")?;
            Ok(Value::String(value.to_owned()))
        }
        TAG_ARRAY => {
            let array = ArrayIndex::parse(frame.payload)?;
            let values = array
                .children
                .into_iter()
                .map(decode_frame)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Value::Array(values))
        }
        TAG_OBJECT => {
            let object = ObjectIndex::parse(frame.payload)?;
            let mut values = Map::new();
            for (key, child) in object.keys.into_iter().zip(object.children) {
                let key = std::str::from_utf8(key)
                    .expect("ObjectIndex already validated UTF-8")
                    .to_owned();
                values.insert(key, decode_frame(child)?);
            }
            Ok(Value::Object(values))
        }
        _ => unreachable!("Frame::parse rejects unknown tags"),
    }
}

fn rewrite_frame(
    frame: Frame<'_>,
    path: &[PathSegment],
    replacement: &[u8],
) -> Result<Vec<u8>, String> {
    let (segment, rest) = path.split_first().expect("caller handles the empty path");
    match (frame.tag, segment) {
        (TAG_ARRAY, PathSegment::Index(index)) => {
            let array = ArrayIndex::parse(frame.payload)?;
            if *index >= array.children.len() {
                return Err(format!("missing array path segment {index}"));
            }
            let mut children = Vec::with_capacity(array.children.len());
            for (child_index, child) in array.children.into_iter().enumerate() {
                if child_index == *index {
                    let rewritten = if rest.is_empty() {
                        validate_recursive(child)?;
                        replacement.to_vec()
                    } else {
                        rewrite_frame(child, rest, replacement)?
                    };
                    children.push(rewritten);
                } else {
                    children.push(child.bytes.to_vec());
                }
            }
            encode_array(&children)
        }
        (TAG_OBJECT, PathSegment::Key(key)) => {
            let object = ObjectIndex::parse(frame.payload)?;
            let sought = key.as_bytes();
            reject_nul(key, "path key")?;
            let position = object
                .keys
                .binary_search_by(|candidate| candidate.cmp(&sought));
            let mut entries =
                Vec::with_capacity(object.keys.len() + usize::from(position.is_err()));
            match position {
                Ok(found) => {
                    for (index, (existing_key, child)) in
                        object.keys.into_iter().zip(object.children).enumerate()
                    {
                        let bytes = if index == found {
                            if rest.is_empty() {
                                validate_recursive(child)?;
                                replacement.to_vec()
                            } else {
                                rewrite_frame(child, rest, replacement)?
                            }
                        } else {
                            child.bytes.to_vec()
                        };
                        entries.push((existing_key, bytes));
                    }
                }
                Err(insert_at) if rest.is_empty() => {
                    for (index, (existing_key, child)) in
                        object.keys.into_iter().zip(object.children).enumerate()
                    {
                        if index == insert_at {
                            entries.push((sought, replacement.to_vec()));
                        }
                        entries.push((existing_key, child.bytes.to_vec()));
                    }
                    if insert_at == entries.len() {
                        entries.push((sought, replacement.to_vec()));
                    }
                }
                Err(_) => return Err(format!("missing object path segment {key:?}")),
            }
            encode_object(&entries)
        }
        (TAG_ARRAY, PathSegment::Key(_)) => {
            ArrayIndex::parse(frame.payload)?;
            Err("path parent is not an object".to_owned())
        }
        (TAG_OBJECT, PathSegment::Index(index)) => {
            ObjectIndex::parse(frame.payload)?;
            Err(format!("missing array path segment {index}"))
        }
        (_, PathSegment::Key(_)) => {
            validate_recursive(frame)?;
            Err("path parent is not an object".to_owned())
        }
        (_, PathSegment::Index(index)) => {
            validate_recursive(frame)?;
            Err(format!("missing array path segment {index}"))
        }
    }
}

fn reject_nul(value: &str, context: &str) -> Result<(), String> {
    if value.contains('\0') {
        Err(format!("{context} contains Unicode NUL"))
    } else {
        Ok(())
    }
}
