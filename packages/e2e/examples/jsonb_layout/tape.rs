use std::collections::HashSet;

use serde_json::{Map, Number, Value};

use super::common::{self, JsonbCodec, PathSegment};

pub struct TapeCodec;

const MAGIC: &[u8; 4] = b"LJTP";
const VERSION: u8 = 1;
const HEADER_SIZE: usize = 32;
const NODE_SIZE: usize = 24;
const EDGE_SIZE: usize = 16;

const TAG_NULL: u8 = 0;
const TAG_FALSE: u8 = 1;
const TAG_TRUE: u8 = 2;
const TAG_I64: u8 = 3;
const TAG_U64: u8 = 4;
const TAG_F64: u8 = 5;
const TAG_STRING: u8 = 6;
const TAG_ARRAY: u8 = 7;
const TAG_OBJECT: u8 = 8;

#[derive(Clone, Copy)]
struct Header {
    node_count: usize,
    edge_count: usize,
    blob_len: usize,
    root: usize,
}

#[derive(Clone, Copy)]
struct Node {
    tag: u8,
    payload: u64,
    aux: usize,
    count: usize,
}

struct Builder {
    nodes: Vec<Node>,
    edges: Vec<(usize, usize)>,
    blob: Vec<u8>,
}

struct Document<'a> {
    bytes: &'a [u8],
    header: Header,
    nodes_start: usize,
    edges_start: usize,
    blob_start: usize,
}

impl JsonbCodec for TapeCodec {
    const NAME: &'static str = "typed-tape";

    fn encode(value: &Value) -> Result<Vec<u8>, String> {
        encode_value(value)
    }

    fn decode(bytes: &[u8]) -> Result<Value, String> {
        let document = Document::parse(bytes)?;
        document.decode_node(document.header.root)
    }

    fn project_path(bytes: &[u8], path: &[PathSegment]) -> Result<Option<Vec<u8>>, String> {
        let document = Document::parse(bytes)?;
        let mut node_offset = document.header.root;
        for segment in path {
            let node = document.node(node_offset)?;
            node_offset = match (segment, node.tag) {
                (PathSegment::Index(index), TAG_ARRAY) => {
                    if *index >= node.count {
                        return Ok(None);
                    }
                    document.edge(node.aux + index * EDGE_SIZE)?.1
                }
                (PathSegment::Key(key), TAG_OBJECT) => {
                    let mut low = 0;
                    let mut high = node.count;
                    let target = key.as_bytes();
                    let mut found = None;
                    while low < high {
                        let middle = low + (high - low) / 2;
                        let (key_offset, child) = document.edge(node.aux + middle * EDGE_SIZE)?;
                        match document.c_string(key_offset)?.cmp(target) {
                            std::cmp::Ordering::Less => low = middle + 1,
                            std::cmp::Ordering::Greater => high = middle,
                            std::cmp::Ordering::Equal => {
                                found = Some(child);
                                break;
                            }
                        }
                    }
                    let Some(child) = found else {
                        return Ok(None);
                    };
                    child
                }
                _ => return Ok(None),
            };
        }
        Ok(Some(encode_value(&document.decode_node(node_offset)?)?))
    }

    fn rewrite_path(
        bytes: &[u8],
        path: &[PathSegment],
        replacement: &Value,
    ) -> Result<Vec<u8>, String> {
        let mut value = Self::decode(bytes)?;
        let mut replacement = replacement.clone();
        common::normalize_jsonb(&mut replacement)?;
        common::rewrite_value(&mut value, path, replacement)?;
        encode_value(&value)
    }
}

fn encode_value(value: &Value) -> Result<Vec<u8>, String> {
    let mut value = value.clone();
    common::normalize_jsonb(&mut value)?;
    let mut builder = Builder {
        nodes: Vec::new(),
        edges: Vec::new(),
        blob: Vec::new(),
    };
    let root_index = builder.add_value(&value)?;
    builder.finish(root_index)
}

impl Builder {
    fn add_value(&mut self, value: &Value) -> Result<usize, String> {
        let index = self.nodes.len();
        self.nodes.push(Node {
            tag: TAG_NULL,
            payload: 0,
            aux: 0,
            count: 0,
        });
        let node = match value {
            Value::Null => Node::scalar(TAG_NULL, 0),
            Value::Bool(false) => Node::scalar(TAG_FALSE, 0),
            Value::Bool(true) => Node::scalar(TAG_TRUE, 0),
            Value::Number(number) => {
                if let Some(value) = number.as_i64() {
                    Node::scalar(TAG_I64, value as u64)
                } else if let Some(value) = number.as_u64() {
                    Node::scalar(TAG_U64, value)
                } else {
                    let value = number
                        .as_f64()
                        .filter(|value| value.is_finite())
                        .ok_or_else(|| "number is not a finite i64, u64, or f64".to_owned())?;
                    Node::scalar(TAG_F64, canonical_f64(value)?.to_bits())
                }
            }
            Value::String(value) => {
                let offset = self.push_string(value)?;
                Node {
                    tag: TAG_STRING,
                    payload: 0,
                    aux: offset,
                    count: value.len(),
                }
            }
            Value::Array(values) => {
                let edge_index = self.edges.len();
                self.edges
                    .resize(edge_index + values.len(), (usize::MAX, 0));
                for (position, value) in values.iter().enumerate() {
                    let child = self.add_value(value)?;
                    self.edges[edge_index + position] = (usize::MAX, child);
                }
                Node {
                    tag: TAG_ARRAY,
                    payload: 0,
                    aux: edge_index,
                    count: values.len(),
                }
            }
            Value::Object(values) => {
                let mut entries = values.iter().collect::<Vec<_>>();
                entries.sort_by(|(left, _), (right, _)| left.as_bytes().cmp(right.as_bytes()));
                let edge_index = self.edges.len();
                self.edges.resize(edge_index + entries.len(), (0, 0));
                for (position, (key, value)) in entries.into_iter().enumerate() {
                    let key_offset = self.push_c_string(key)?;
                    let child = self.add_value(value)?;
                    self.edges[edge_index + position] = (key_offset, child);
                }
                Node {
                    tag: TAG_OBJECT,
                    payload: 0,
                    aux: edge_index,
                    count: values.len(),
                }
            }
        };
        self.nodes[index] = node;
        Ok(index)
    }

    fn push_string(&mut self, value: &str) -> Result<usize, String> {
        if value.contains('\0') {
            return Err("Lix JSONB does not encode Unicode NUL in keys or strings".to_owned());
        }
        let offset = self.blob.len();
        self.blob.extend_from_slice(value.as_bytes());
        Ok(offset)
    }

    fn push_c_string(&mut self, value: &str) -> Result<usize, String> {
        let offset = self.push_string(value)?;
        self.blob.push(0);
        Ok(offset)
    }

    fn finish(self, root_index: usize) -> Result<Vec<u8>, String> {
        let nodes_len = checked_mul(self.nodes.len(), NODE_SIZE, "node table")?;
        let edges_len = checked_mul(self.edges.len(), EDGE_SIZE, "edge table")?;
        let edges_start = checked_add(HEADER_SIZE, nodes_len, "edge table")?;
        let blob_start = checked_add(edges_start, edges_len, "blob")?;
        let total_len = checked_add(blob_start, self.blob.len(), "document")?;
        u32::try_from(total_len).map_err(|_| "document exceeds u32".to_owned())?;

        let mut output = Vec::with_capacity(total_len);
        output.extend_from_slice(MAGIC);
        output.push(VERSION);
        output.extend_from_slice(&[0; 3]);
        push_u32(&mut output, self.nodes.len(), "node count")?;
        push_u32(&mut output, self.edges.len(), "edge count")?;
        push_u32(&mut output, self.blob.len(), "blob length")?;
        push_u32(
            &mut output,
            HEADER_SIZE + root_index * NODE_SIZE,
            "root offset",
        )?;
        push_u32(&mut output, total_len, "document length")?;
        output.extend_from_slice(&[0; 4]);

        for node in self.nodes {
            output.push(node.tag);
            output.extend_from_slice(&[0; 7]);
            output.extend_from_slice(&node.payload.to_le_bytes());
            let aux = match node.tag {
                TAG_STRING => blob_start + node.aux,
                TAG_ARRAY | TAG_OBJECT => edges_start + node.aux * EDGE_SIZE,
                _ => node.aux,
            };
            push_u32(&mut output, aux, "node auxiliary offset")?;
            push_u32(&mut output, node.count, "node count")?;
        }
        for (key_offset, child_index) in self.edges {
            let key_offset = if key_offset == usize::MAX {
                0
            } else {
                blob_start + key_offset
            };
            push_u32(&mut output, key_offset, "object key offset")?;
            push_u32(
                &mut output,
                HEADER_SIZE + child_index * NODE_SIZE,
                "child offset",
            )?;
            output.extend_from_slice(&[0; 8]);
        }
        output.extend_from_slice(&self.blob);
        Ok(output)
    }
}

impl Node {
    fn scalar(tag: u8, payload: u64) -> Self {
        Self {
            tag,
            payload,
            aux: 0,
            count: 0,
        }
    }
}

impl<'a> Document<'a> {
    fn parse(bytes: &'a [u8]) -> Result<Self, String> {
        if bytes.len() < HEADER_SIZE {
            return Err("tape header is truncated".to_owned());
        }
        if bytes.get(..4) != Some(MAGIC) {
            return Err("invalid tape magic".to_owned());
        }
        if bytes[4] != VERSION {
            return Err(format!("unsupported tape version {}", bytes[4]));
        }
        require_zero(&bytes[5..8], "header reserved bytes")?;
        require_zero(&bytes[28..32], "header reserved bytes")?;

        let header = Header {
            node_count: read_u32(bytes, 8, "node count")?,
            edge_count: read_u32(bytes, 12, "edge count")?,
            blob_len: read_u32(bytes, 16, "blob length")?,
            root: read_u32(bytes, 20, "root offset")?,
        };
        let declared_len = read_u32(bytes, 24, "document length")?;
        if declared_len != bytes.len() {
            return Err("document length does not match input".to_owned());
        }
        if header.node_count == 0 {
            return Err("tape contains no root node".to_owned());
        }
        let nodes_len = checked_mul(header.node_count, NODE_SIZE, "node table")?;
        let edges_len = checked_mul(header.edge_count, EDGE_SIZE, "edge table")?;
        let nodes_start = HEADER_SIZE;
        let edges_start = checked_add(nodes_start, nodes_len, "edge table")?;
        let blob_start = checked_add(edges_start, edges_len, "blob")?;
        let expected_len = checked_add(blob_start, header.blob_len, "document")?;
        if expected_len != bytes.len() {
            return Err("header counts do not describe the input exactly".to_owned());
        }
        if header.root != nodes_start {
            return Err("root offset is not canonical".to_owned());
        }

        let document = Self {
            bytes,
            header,
            nodes_start,
            edges_start,
            blob_start,
        };
        document.validate()?;
        Ok(document)
    }

    fn validate(&self) -> Result<(), String> {
        let mut seen_nodes = HashSet::new();
        let mut next_node = self.nodes_start;
        let mut next_edge = self.edges_start;
        let mut next_blob = self.blob_start;
        self.validate_node(
            self.header.root,
            &mut seen_nodes,
            &mut next_node,
            &mut next_edge,
            &mut next_blob,
        )?;
        if seen_nodes.len() != self.header.node_count || next_node != self.edges_start {
            return Err("node count includes unreachable or out-of-order nodes".to_owned());
        }
        if next_edge != self.blob_start {
            return Err("edge count includes unused or out-of-order edges".to_owned());
        }
        if next_blob != self.bytes.len() {
            return Err("blob contains unused or out-of-order bytes".to_owned());
        }
        Ok(())
    }

    fn validate_node(
        &self,
        offset: usize,
        seen: &mut HashSet<usize>,
        next_node: &mut usize,
        next_edge: &mut usize,
        next_blob: &mut usize,
    ) -> Result<(), String> {
        if offset != *next_node || !seen.insert(offset) {
            return Err("child node offset is not canonical".to_owned());
        }
        *next_node = checked_add(*next_node, NODE_SIZE, "node traversal")?;
        let node = self.node(offset)?;
        match node.tag {
            TAG_NULL | TAG_FALSE | TAG_TRUE => {
                if node.payload != 0 || node.aux != 0 || node.count != 0 {
                    return Err("scalar has nonzero unused fields".to_owned());
                }
            }
            TAG_I64 => {
                if node.aux != 0 || node.count != 0 {
                    return Err("i64 has nonzero unused fields".to_owned());
                }
            }
            TAG_U64 => {
                if node.aux != 0 || node.count != 0 || node.payload <= i64::MAX as u64 {
                    return Err("u64 is not canonically represented".to_owned());
                }
            }
            TAG_F64 => {
                if node.aux != 0 || node.count != 0 {
                    return Err("f64 has nonzero unused fields".to_owned());
                }
                let value = f64::from_bits(node.payload);
                if canonical_f64(value)?.to_bits() != node.payload {
                    return Err("f64 is not canonically represented".to_owned());
                }
            }
            TAG_STRING => {
                if node.payload != 0 || node.aux != *next_blob {
                    return Err("string blob offset is not canonical".to_owned());
                }
                let end = checked_add(node.aux, node.count, "string")?;
                let raw = self
                    .bytes
                    .get(node.aux..end)
                    .ok_or_else(|| "string is outside the blob".to_owned())?;
                if node.aux < self.blob_start || raw.contains(&0) {
                    return Err("invalid string blob range".to_owned());
                }
                std::str::from_utf8(raw).map_err(|_| "string is not UTF-8".to_owned())?;
                *next_blob = end;
            }
            TAG_ARRAY | TAG_OBJECT => {
                if node.payload != 0 || node.aux != *next_edge {
                    return Err("container edge offset is not canonical".to_owned());
                }
                let edge_bytes = checked_mul(node.count, EDGE_SIZE, "container edges")?;
                *next_edge = checked_add(*next_edge, edge_bytes, "edge traversal")?;
                if *next_edge > self.blob_start {
                    return Err("container edges exceed the edge table".to_owned());
                }
                let mut previous_key: Option<&[u8]> = None;
                for index in 0..node.count {
                    let edge_offset = node.aux + index * EDGE_SIZE;
                    let (key_offset, child_offset) = self.edge(edge_offset)?;
                    if node.tag == TAG_ARRAY {
                        if key_offset != 0 {
                            return Err("array edge has an object key".to_owned());
                        }
                    } else {
                        if key_offset != *next_blob {
                            return Err("object key blob offset is not canonical".to_owned());
                        }
                        let key = self.c_string(key_offset)?;
                        if let Some(previous) = previous_key {
                            if previous >= key {
                                return Err("object keys are not strictly byte-sorted".to_owned());
                            }
                        }
                        previous_key = Some(key);
                        *next_blob = key_offset + key.len() + 1;
                    }
                    self.validate_node(child_offset, seen, next_node, next_edge, next_blob)?;
                }
            }
            _ => return Err(format!("unknown node tag {}", node.tag)),
        }
        Ok(())
    }

    fn node(&self, offset: usize) -> Result<Node, String> {
        if offset < self.nodes_start
            || offset >= self.edges_start
            || (offset - self.nodes_start) % NODE_SIZE != 0
        {
            return Err("invalid node offset".to_owned());
        }
        let raw = self
            .bytes
            .get(offset..offset + NODE_SIZE)
            .ok_or_else(|| "node is truncated".to_owned())?;
        require_zero(&raw[1..8], "node reserved bytes")?;
        Ok(Node {
            tag: raw[0],
            payload: u64::from_le_bytes(raw[8..16].try_into().expect("eight bytes")),
            aux: read_u32(raw, 16, "node auxiliary offset")?,
            count: read_u32(raw, 20, "node count")?,
        })
    }

    fn edge(&self, offset: usize) -> Result<(usize, usize), String> {
        if offset < self.edges_start
            || offset >= self.blob_start
            || (offset - self.edges_start) % EDGE_SIZE != 0
        {
            return Err("invalid edge offset".to_owned());
        }
        let raw = self
            .bytes
            .get(offset..offset + EDGE_SIZE)
            .ok_or_else(|| "edge is truncated".to_owned())?;
        require_zero(&raw[8..16], "edge reserved bytes")?;
        Ok((
            read_u32(raw, 0, "edge key offset")?,
            read_u32(raw, 4, "edge child offset")?,
        ))
    }

    fn c_string(&self, offset: usize) -> Result<&'a [u8], String> {
        if offset < self.blob_start || offset >= self.bytes.len() {
            return Err("object key offset is outside the blob".to_owned());
        }
        let tail = &self.bytes[offset..];
        let length = tail
            .iter()
            .position(|byte| *byte == 0)
            .ok_or_else(|| "object key is not NUL-terminated".to_owned())?;
        let raw = &tail[..length];
        std::str::from_utf8(raw).map_err(|_| "object key is not UTF-8".to_owned())?;
        Ok(raw)
    }

    fn decode_node(&self, offset: usize) -> Result<Value, String> {
        let node = self.node(offset)?;
        match node.tag {
            TAG_NULL => Ok(Value::Null),
            TAG_FALSE => Ok(Value::Bool(false)),
            TAG_TRUE => Ok(Value::Bool(true)),
            TAG_I64 => Ok(Value::Number(Number::from(node.payload as i64))),
            TAG_U64 => Ok(Value::Number(Number::from(node.payload))),
            TAG_F64 => Number::from_f64(f64::from_bits(node.payload))
                .map(Value::Number)
                .ok_or_else(|| "invalid f64".to_owned()),
            TAG_STRING => {
                let end = node.aux + node.count;
                let value = std::str::from_utf8(&self.bytes[node.aux..end])
                    .map_err(|_| "string is not UTF-8".to_owned())?;
                Ok(Value::String(value.to_owned()))
            }
            TAG_ARRAY => {
                let mut values = Vec::with_capacity(node.count);
                for index in 0..node.count {
                    let (_, child) = self.edge(node.aux + index * EDGE_SIZE)?;
                    values.push(self.decode_node(child)?);
                }
                Ok(Value::Array(values))
            }
            TAG_OBJECT => {
                let mut values = Map::with_capacity(node.count);
                for index in 0..node.count {
                    let (key, child) = self.edge(node.aux + index * EDGE_SIZE)?;
                    let key = std::str::from_utf8(self.c_string(key)?)
                        .map_err(|_| "object key is not UTF-8".to_owned())?;
                    values.insert(key.to_owned(), self.decode_node(child)?);
                }
                Ok(Value::Object(values))
            }
            _ => Err(format!("unknown node tag {}", node.tag)),
        }
    }
}

fn canonical_f64(value: f64) -> Result<f64, String> {
    if !value.is_finite() {
        return Err("f64 must be finite".to_owned());
    }
    if value == 0.0 && value.is_sign_negative() {
        return Err("negative zero is not canonical".to_owned());
    }
    if value.fract() == 0.0 && value.abs() <= 9_007_199_254_740_992.0 {
        return Err("integral f64 is not canonical".to_owned());
    }
    Ok(value)
}

fn read_u32(bytes: &[u8], offset: usize, context: &str) -> Result<usize, String> {
    Ok(common::read_u32(bytes, offset, context)? as usize)
}

fn push_u32(output: &mut Vec<u8>, value: usize, context: &str) -> Result<(), String> {
    common::push_u32(output, value, context)
}

fn checked_add(left: usize, right: usize, context: &str) -> Result<usize, String> {
    left.checked_add(right)
        .ok_or_else(|| format!("{context} size overflow"))
}

fn checked_mul(left: usize, right: usize, context: &str) -> Result<usize, String> {
    left.checked_mul(right)
        .ok_or_else(|| format!("{context} size overflow"))
}

fn require_zero(bytes: &[u8], context: &str) -> Result<(), String> {
    if bytes.iter().any(|byte| *byte != 0) {
        Err(format!("{context} must be zero"))
    } else {
        Ok(())
    }
}
