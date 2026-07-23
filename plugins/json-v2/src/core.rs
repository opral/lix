use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet, VecDeque};
use std::mem::size_of;
use std::sync::Arc;

pub const ROOT_SCHEMA_KEY: &str = "json_root";
pub const OBJECT_MEMBER_SCHEMA_KEY: &str = "json_object_member";
pub const ARRAY_ITEM_SCHEMA_KEY: &str = "json_array_item";

const ROOT_ID: &str = "root";
const OBJECT_CONTAINER_DOMAIN: &[u8] = b"lix-json-object-container-v1\0";
const NODES_PER_SPAN_CHUNK: usize = 512;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IdNamespace(pub [u8; 16]);

impl IdNamespace {
    pub fn from_halves(high: u64, low: u64) -> Self {
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&high.to_be_bytes());
        bytes[8..].copy_from_slice(&low.to_be_bytes());
        Self(bytes)
    }

    pub fn encode(self, ordinal: u64) -> String {
        let mut bytes = [0; 24];
        bytes[..16].copy_from_slice(&self.0);
        bytes[16..].copy_from_slice(&ordinal.to_be_bytes());
        URL_SAFE_NO_PAD.encode(bytes)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InputSplice<'a> {
    pub offset: u64,
    pub delete_len: u64,
    pub insert: &'a [u8],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChangeEffect {
    Content,
    FormatOnly,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntityChange {
    pub schema_key: String,
    pub entity_pk: Vec<String>,
    pub snapshot: Option<Vec<u8>>,
    pub effect: ChangeEffect,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntityRecord {
    pub schema_key: String,
    pub entity_pk: Vec<String>,
    pub snapshot: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ByteEdit {
    pub offset: u64,
    pub delete_len: u64,
    pub insert: Arc<Vec<u8>>,
}

impl EntityChange {
    fn upsert(identity: &EntityIdentity, snapshot: Vec<u8>) -> Self {
        Self {
            schema_key: identity.schema_key().to_owned(),
            entity_pk: identity.entity_pk(),
            snapshot: Some(snapshot),
            effect: ChangeEffect::Content,
        }
    }

    fn delete(identity: &EntityIdentity) -> Self {
        Self {
            schema_key: identity.schema_key().to_owned(),
            entity_pk: identity.entity_pk(),
            snapshot: None,
            effect: ChangeEffect::Content,
        }
    }
}

/// Immutable piece table for accepted JSON bytes. Sparse successors retain
/// accepted allocations and own only their inserted bytes.
#[derive(Clone, Debug)]
struct PersistentBlob {
    pieces: Arc<Vec<BlobPiece>>,
    len: u32,
}

#[derive(Clone, Debug)]
struct BlobPiece {
    bytes: Arc<Vec<u8>>,
    start: u32,
    len: u32,
}

impl PersistentBlob {
    fn from_shared(bytes: Arc<Vec<u8>>) -> Result<Self, String> {
        let len = u32::try_from(bytes.len())
            .map_err(|_| "JSON v2 currently supports files smaller than 4GiB".to_owned())?;
        let pieces = if len == 0 {
            Vec::new()
        } else {
            vec![BlobPiece {
                bytes,
                start: 0,
                len,
            }]
        };
        Ok(Self {
            pieces: Arc::new(pieces),
            len,
        })
    }

    fn len(&self) -> usize {
        usize::try_from(self.len).expect("u32 fits usize")
    }

    fn materialize(&self) -> Vec<u8> {
        let mut output = Vec::with_capacity(self.len());
        self.append_range(0, self.len, &mut output)
            .expect("complete JSON range is valid");
        output
    }

    fn range(&self, start: u32, end: u32) -> Result<Vec<u8>, String> {
        if start > end || end > self.len {
            return Err("JSON byte range is out of bounds".to_owned());
        }
        let mut output = Vec::with_capacity(usize::try_from(end - start).expect("u32 fits usize"));
        self.append_range(start, end, &mut output)?;
        Ok(output)
    }

    fn append_range(&self, start: u32, end: u32, output: &mut Vec<u8>) -> Result<(), String> {
        if start > end || end > self.len {
            return Err("JSON byte range is out of bounds".to_owned());
        }
        let mut logical_start = 0u32;
        for piece in self.pieces.iter() {
            let logical_end = logical_start + piece.len;
            let selected_start = start.max(logical_start);
            let selected_end = end.min(logical_end);
            if selected_start < selected_end {
                let piece_start = piece.start + selected_start - logical_start;
                let piece_end = piece.start + selected_end - logical_start;
                output.extend_from_slice(
                    &piece.bytes[usize::try_from(piece_start).expect("u32 fits usize")
                        ..usize::try_from(piece_end).expect("u32 fits usize")],
                );
            }
            if logical_end >= end {
                break;
            }
            logical_start = logical_end;
        }
        Ok(())
    }

    fn append_piece_range(
        &self,
        start: u32,
        end: u32,
        output: &mut Vec<BlobPiece>,
    ) -> Result<(), String> {
        if start > end || end > self.len {
            return Err("JSON piece range is out of bounds".to_owned());
        }
        let mut logical_start = 0u32;
        for piece in self.pieces.iter() {
            let logical_end = logical_start + piece.len;
            let selected_start = start.max(logical_start);
            let selected_end = end.min(logical_end);
            if selected_start < selected_end {
                push_blob_piece(
                    output,
                    BlobPiece {
                        bytes: Arc::clone(&piece.bytes),
                        start: piece.start + selected_start - logical_start,
                        len: selected_end - selected_start,
                    },
                );
            }
            if logical_end >= end {
                break;
            }
            logical_start = logical_end;
        }
        Ok(())
    }

    fn splice(&self, splices: &[InputSplice<'_>]) -> Result<Self, String> {
        validate_splices(self.len(), splices)?;
        let deleted = splices.iter().try_fold(0u64, |total, splice| {
            total
                .checked_add(splice.delete_len)
                .ok_or_else(|| "splice size overflow".to_owned())
        })?;
        let inserted = splices.iter().try_fold(0u64, |total, splice| {
            total
                .checked_add(u64::try_from(splice.insert.len()).expect("usize fits u64"))
                .ok_or_else(|| "splice size overflow".to_owned())
        })?;
        let result_len = u64::from(self.len)
            .checked_sub(deleted)
            .and_then(|value| value.checked_add(inserted))
            .ok_or_else(|| "reconstructed JSON size overflow".to_owned())?;
        let result_len = u32::try_from(result_len)
            .map_err(|_| "JSON v2 currently supports files smaller than 4GiB".to_owned())?;
        let mut pieces = Vec::with_capacity(self.pieces.len() + splices.len() * 2);
        let mut cursor = 0u32;
        for splice in splices {
            let start = u32::try_from(splice.offset)
                .map_err(|_| "splice offset exceeds 4GiB".to_owned())?;
            let end = u32::try_from(
                splice
                    .offset
                    .checked_add(splice.delete_len)
                    .ok_or_else(|| "splice end overflow".to_owned())?,
            )
            .map_err(|_| "splice end exceeds 4GiB".to_owned())?;
            self.append_piece_range(cursor, start, &mut pieces)?;
            if !splice.insert.is_empty() {
                let bytes = Arc::new(splice.insert.to_vec());
                push_blob_piece(
                    &mut pieces,
                    BlobPiece {
                        start: 0,
                        len: u32::try_from(bytes.len())
                            .map_err(|_| "JSON insert exceeds 4GiB".to_owned())?,
                        bytes,
                    },
                );
            }
            cursor = end;
        }
        self.append_piece_range(cursor, self.len, &mut pieces)?;
        Ok(Self {
            pieces: Arc::new(pieces),
            len: result_len,
        })
    }

    fn retained_backing_bytes(&self) -> usize {
        let mut seen = HashSet::new();
        self.pieces
            .iter()
            .filter_map(|piece| {
                let address = Arc::as_ptr(&piece.bytes) as usize;
                seen.insert(address).then_some(piece.bytes.len())
            })
            .sum::<usize>()
            + self.pieces.len() * size_of::<BlobPiece>()
    }
}

fn push_blob_piece(output: &mut Vec<BlobPiece>, piece: BlobPiece) {
    if piece.len == 0 {
        return;
    }
    if let Some(previous) = output.last_mut()
        && Arc::ptr_eq(&previous.bytes, &piece.bytes)
        && previous.start + previous.len == piece.start
    {
        previous.len += piece.len;
        return;
    }
    output.push(piece);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NodeKind {
    Object,
    Array,
    String,
    Number,
    Boolean,
    Null,
}

impl NodeKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Object => "object",
            Self::Array => "array",
            Self::String => "string",
            Self::Number => "number",
            Self::Boolean => "boolean",
            Self::Null => "null",
        }
    }

    fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "object" => Ok(Self::Object),
            "array" => Ok(Self::Array),
            "string" => Ok(Self::String),
            "number" => Ok(Self::Number),
            "boolean" => Ok(Self::Boolean),
            "null" => Ok(Self::Null),
            _ => Err(format!("unsupported JSON entity kind {raw:?}")),
        }
    }

    const fn is_container(self) -> bool {
        matches!(self, Self::Object | Self::Array)
    }
}

#[derive(Clone, Debug)]
enum NodeRelation {
    Root,
    Object {
        parent_id: Arc<str>,
        key: Arc<str>,
        order_key: Arc<str>,
        container_id: Option<Arc<str>>,
    },
    Array {
        id: Arc<str>,
        parent_id: Arc<str>,
        order_key: Arc<str>,
    },
}

#[derive(Clone, Debug)]
struct Node {
    relation: NodeRelation,
    kind: NodeKind,
    parent: Option<u32>,
    first_child: Option<u32>,
    next_sibling: Option<u32>,
    value_start: u32,
    value_len: u32,
}

#[derive(Clone, Copy, Debug)]
struct CompactSpan {
    relative_start: u32,
    len: u32,
}

#[derive(Clone, Debug)]
struct SpanChunk {
    spans: Box<[CompactSpan]>,
}

#[derive(Clone, Debug)]
struct SpanChunkRef {
    byte_start: u32,
    data: Arc<SpanChunk>,
}

#[derive(Clone, Debug)]
struct SpanIndex {
    chunks: Arc<Vec<SpanChunkRef>>,
    count: u32,
}

impl SpanIndex {
    fn from_nodes(nodes: &[Node]) -> Result<Self, String> {
        let count = u32::try_from(nodes.len()).map_err(|_| "JSON has too many semantic nodes")?;
        let mut chunks = Vec::with_capacity(nodes.len().div_ceil(NODES_PER_SPAN_CHUNK));
        for group in nodes.chunks(NODES_PER_SPAN_CHUNK) {
            let byte_start = group.first().map_or(0, |node| node.value_start);
            let spans = group
                .iter()
                .map(|node| CompactSpan {
                    relative_start: node.value_start - byte_start,
                    len: node.value_len,
                })
                .collect::<Vec<_>>()
                .into_boxed_slice();
            chunks.push(SpanChunkRef {
                byte_start,
                data: Arc::new(SpanChunk { spans }),
            });
        }
        Ok(Self {
            chunks: Arc::new(chunks),
            count,
        })
    }

    fn span(&self, ordinal: usize) -> Option<(u32, u32)> {
        if ordinal >= usize::try_from(self.count).expect("u32 fits usize") {
            return None;
        }
        let chunk = ordinal / NODES_PER_SPAN_CHUNK;
        let within = ordinal % NODES_PER_SPAN_CHUNK;
        let chunk = &self.chunks[chunk];
        let span = chunk.data.spans[within];
        Some((chunk.byte_start + span.relative_start, span.len))
    }

    fn scalar_at_offset(&self, offset: u32) -> Option<usize> {
        let chunk = self
            .chunks
            .partition_point(|candidate| candidate.byte_start <= offset)
            .checked_sub(1)?;
        let chunk_ref = &self.chunks[chunk];
        let relative = offset.checked_sub(chunk_ref.byte_start)?;
        let within = chunk_ref
            .data
            .spans
            .partition_point(|span| span.relative_start <= relative)
            .checked_sub(1)?;
        Some(chunk * NODES_PER_SPAN_CHUNK + within)
    }

    fn replace_scalar(
        &self,
        selected: usize,
        ancestors: &HashSet<usize>,
        new_len: u32,
        delta: i64,
    ) -> Result<Self, String> {
        if delta == 0 {
            return Ok(self.clone());
        }
        let selected_chunk = selected / NODES_PER_SPAN_CHUNK;
        let mut chunks = self.chunks.as_ref().clone();
        let mut affected_chunks = ancestors
            .iter()
            .map(|ordinal| ordinal / NODES_PER_SPAN_CHUNK)
            .collect::<HashSet<_>>();
        affected_chunks.insert(selected_chunk);
        let mut affected_chunks = affected_chunks.into_iter().collect::<Vec<_>>();
        affected_chunks.sort_unstable();
        for chunk_index in affected_chunks {
            let chunk_start_ordinal = chunk_index * NODES_PER_SPAN_CHUNK;
            let chunk = chunks[chunk_index].clone();
            let mut spans = chunk.data.spans.to_vec();
            for (within, span) in spans.iter_mut().enumerate() {
                let ordinal = chunk_start_ordinal + within;
                if ordinal == selected {
                    span.len = new_len;
                } else if ancestors.contains(&ordinal) {
                    span.len = add_signed(span.len, delta)?;
                }
                if chunk_index == selected_chunk && ordinal > selected {
                    span.relative_start = add_signed(span.relative_start, delta)?;
                }
            }
            chunks[chunk_index] = SpanChunkRef {
                byte_start: chunk.byte_start,
                data: Arc::new(SpanChunk {
                    spans: spans.into_boxed_slice(),
                }),
            };
        }
        for chunk in &mut chunks[selected_chunk + 1..] {
            chunk.byte_start = add_signed(chunk.byte_start, delta)?;
        }
        Ok(Self {
            chunks: Arc::new(chunks),
            count: self.count,
        })
    }

    fn estimated_bytes(&self) -> usize {
        self.chunks.len() * size_of::<SpanChunkRef>()
            + self
                .chunks
                .iter()
                .map(|chunk| chunk.data.spans.len() * size_of::<CompactSpan>())
                .sum::<usize>()
    }
}

impl Node {
    fn container_id(&self) -> Option<Arc<str>> {
        if !self.kind.is_container() {
            return None;
        }
        match &self.relation {
            NodeRelation::Root => Some(Arc::from(ROOT_ID)),
            NodeRelation::Object { container_id, .. } => container_id.clone(),
            NodeRelation::Array { id, .. } => Some(Arc::clone(id)),
        }
    }

    fn identity(&self) -> EntityIdentity {
        match &self.relation {
            NodeRelation::Root => EntityIdentity::Root,
            NodeRelation::Object { parent_id, key, .. } => EntityIdentity::Object {
                parent_id: parent_id.to_string(),
                key: key.to_string(),
            },
            NodeRelation::Array { id, .. } => EntityIdentity::Array(id.to_string()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum EntityIdentity {
    Root,
    Object { parent_id: String, key: String },
    Array(String),
}

impl EntityIdentity {
    const fn schema_key(&self) -> &'static str {
        match self {
            Self::Root => ROOT_SCHEMA_KEY,
            Self::Object { .. } => OBJECT_MEMBER_SCHEMA_KEY,
            Self::Array(_) => ARRAY_ITEM_SCHEMA_KEY,
        }
    }

    fn entity_pk(&self) -> Vec<String> {
        match self {
            Self::Root => vec![ROOT_ID.to_owned()],
            Self::Object { parent_id, key } => vec![parent_id.clone(), key.clone()],
            Self::Array(id) => vec![id.clone()],
        }
    }

    fn from_parts(schema_key: &str, entity_pk: &[String]) -> Result<Self, String> {
        match (schema_key, entity_pk) {
            (ROOT_SCHEMA_KEY, [id]) if id == ROOT_ID => Ok(Self::Root),
            (ROOT_SCHEMA_KEY, _) => Err("json_root requires the single key \"root\"".to_owned()),
            (OBJECT_MEMBER_SCHEMA_KEY, [parent_id, key]) => Ok(Self::Object {
                parent_id: parent_id.clone(),
                key: key.clone(),
            }),
            (OBJECT_MEMBER_SCHEMA_KEY, _) => {
                Err("json_object_member requires parent_id and key components".to_owned())
            }
            (ARRAY_ITEM_SCHEMA_KEY, [id]) => Ok(Self::Array(id.clone())),
            (ARRAY_ITEM_SCHEMA_KEY, _) => {
                Err("json_array_item requires one ID component".to_owned())
            }
            (other, _) => Err(format!("unsupported JSON entity schema {other:?}")),
        }
    }
}

fn identity_fingerprint(identity: &EntityIdentity) -> [u8; 16] {
    match identity {
        EntityIdentity::Root => fingerprint_components(ROOT_SCHEMA_KEY, &[ROOT_ID]),
        EntityIdentity::Object { parent_id, key } => {
            fingerprint_components(OBJECT_MEMBER_SCHEMA_KEY, &[parent_id, key])
        }
        EntityIdentity::Array(id) => fingerprint_components(ARRAY_ITEM_SCHEMA_KEY, &[id]),
    }
}

fn identity_fingerprint_node(node: &Node) -> [u8; 16] {
    match &node.relation {
        NodeRelation::Root => fingerprint_components(ROOT_SCHEMA_KEY, &[ROOT_ID]),
        NodeRelation::Object { parent_id, key, .. } => fingerprint_components(
            OBJECT_MEMBER_SCHEMA_KEY,
            &[parent_id.as_ref(), key.as_ref()],
        ),
        NodeRelation::Array { id, .. } => {
            fingerprint_components(ARRAY_ITEM_SCHEMA_KEY, &[id.as_ref()])
        }
    }
}

fn fingerprint_components(schema_key: &str, components: &[&str]) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"lix-json-entity-lookup-v1\0");
    hasher.update(
        &u64::try_from(schema_key.len())
            .expect("usize fits u64")
            .to_be_bytes(),
    );
    hasher.update(schema_key.as_bytes());
    for component in components {
        hasher.update(
            &u64::try_from(component.len())
                .expect("usize fits u64")
                .to_be_bytes(),
        );
        hasher.update(component.as_bytes());
    }
    let hash = hasher.finalize();
    let mut output = [0; 16];
    output.copy_from_slice(&hash.as_bytes()[..16]);
    output
}

#[derive(Clone, Debug)]
enum RelationSeed {
    Root,
    Object { parent_id: Arc<str>, key: Arc<str> },
    Array { id: Arc<str>, parent_id: Arc<str> },
}

struct JsonParser<'a> {
    bytes: &'a [u8],
    cursor: usize,
    namespace: IdNamespace,
    next_array_ordinal: u64,
    nodes: Vec<Node>,
}

impl<'a> JsonParser<'a> {
    fn parse(bytes: &'a [u8], namespace: IdNamespace) -> Result<Vec<Node>, String> {
        if bytes.len() > u32::MAX as usize {
            return Err("JSON v2 currently supports files smaller than 4GiB".to_owned());
        }
        std::str::from_utf8(bytes).map_err(|error| format!("JSON must be UTF-8: {error}"))?;
        let mut parser = Self {
            bytes,
            cursor: 0,
            namespace,
            next_array_ordinal: 0,
            nodes: Vec::new(),
        };
        parser.skip_whitespace();
        parser.parse_value(RelationSeed::Root, None, 0)?;
        parser.skip_whitespace();
        if parser.cursor != bytes.len() {
            return Err("JSON document has trailing non-whitespace bytes".to_owned());
        }
        Ok(parser.nodes)
    }

    fn parse_value(
        &mut self,
        relation: RelationSeed,
        parent: Option<u32>,
        depth: usize,
    ) -> Result<u32, String> {
        if depth > 1024 {
            return Err("JSON nesting exceeds 1024 levels".to_owned());
        }
        self.skip_whitespace();
        let start = self.cursor;
        let kind = match self.bytes.get(self.cursor) {
            Some(b'{') => NodeKind::Object,
            Some(b'[') => NodeKind::Array,
            Some(b'"') => NodeKind::String,
            Some(b't' | b'f') => NodeKind::Boolean,
            Some(b'n') => NodeKind::Null,
            Some(b'-' | b'0'..=b'9') => NodeKind::Number,
            Some(byte) => {
                return Err(format!(
                    "unexpected byte {:?} at JSON offset {}",
                    char::from(*byte),
                    self.cursor
                ));
            }
            None => return Err("JSON value is missing".to_owned()),
        };
        let relation = match relation {
            RelationSeed::Root => NodeRelation::Root,
            RelationSeed::Object { parent_id, key } => {
                let container_id = kind.is_container().then(|| {
                    Arc::<str>::from(derive_object_container_id(parent_id.as_ref(), key.as_ref()))
                });
                NodeRelation::Object {
                    parent_id,
                    key,
                    order_key: Arc::from("01"),
                    container_id,
                }
            }
            RelationSeed::Array { id, parent_id } => NodeRelation::Array {
                id,
                parent_id,
                order_key: Arc::from("01"),
            },
        };
        let index = u32::try_from(self.nodes.len())
            .map_err(|_| "JSON contains too many semantic nodes".to_owned())?;
        self.nodes.push(Node {
            relation,
            kind,
            parent,
            first_child: None,
            next_sibling: None,
            value_start: u32::try_from(start).map_err(|_| "JSON offset exceeds 4GiB")?,
            value_len: 0,
        });

        match kind {
            NodeKind::Object => self.parse_object(index, depth + 1)?,
            NodeKind::Array => self.parse_array(index, depth + 1)?,
            NodeKind::String => {
                self.cursor = scan_string_end(self.bytes, self.cursor)?;
                serde_json::from_slice::<String>(&self.bytes[start..self.cursor])
                    .map_err(|error| format!("invalid JSON string: {error}"))?;
            }
            NodeKind::Number => self.parse_number()?,
            NodeKind::Boolean => {
                if self.bytes[self.cursor..].starts_with(b"true") {
                    self.cursor += 4;
                } else if self.bytes[self.cursor..].starts_with(b"false") {
                    self.cursor += 5;
                } else {
                    return Err(format!("invalid boolean at JSON offset {start}"));
                }
            }
            NodeKind::Null => {
                if !self.bytes[self.cursor..].starts_with(b"null") {
                    return Err(format!("invalid null at JSON offset {start}"));
                }
                self.cursor += 4;
            }
        }
        let end = self.cursor;
        self.nodes[usize::try_from(index).expect("u32 fits usize")].value_len =
            u32::try_from(end - start).map_err(|_| "JSON value exceeds 4GiB")?;
        Ok(index)
    }

    fn parse_object(&mut self, node: u32, depth: usize) -> Result<(), String> {
        self.cursor += 1;
        self.skip_whitespace();
        let parent_id = self.nodes[usize::try_from(node).expect("u32 fits usize")]
            .container_id()
            .expect("object node has a stable container ID");
        let mut children = Vec::new();
        let mut keys = HashSet::new();
        if self.bytes.get(self.cursor) == Some(&b'}') {
            self.cursor += 1;
            return Ok(());
        }
        loop {
            self.skip_whitespace();
            let key_start = self.cursor;
            let key_end = scan_string_end(self.bytes, key_start)?;
            let key: String = serde_json::from_slice(&self.bytes[key_start..key_end])
                .map_err(|error| format!("invalid JSON object key: {error}"))?;
            if !keys.insert(key.clone()) {
                return Err(format!("duplicate JSON object key {key:?}"));
            }
            self.cursor = key_end;
            self.skip_whitespace();
            if self.bytes.get(self.cursor) != Some(&b':') {
                return Err(format!(
                    "JSON object key at offset {key_start} is not followed by ':'"
                ));
            }
            self.cursor += 1;
            let child = self.parse_value(
                RelationSeed::Object {
                    parent_id: Arc::clone(&parent_id),
                    key: Arc::from(key),
                },
                Some(node),
                depth,
            )?;
            children.push(child);
            self.skip_whitespace();
            match self.bytes.get(self.cursor) {
                Some(b',') => {
                    self.cursor += 1;
                    self.skip_whitespace();
                    if self.bytes.get(self.cursor) == Some(&b'}') {
                        return Err("JSON objects cannot have a trailing comma".to_owned());
                    }
                }
                Some(b'}') => {
                    self.cursor += 1;
                    break;
                }
                _ => {
                    return Err(format!(
                        "JSON object requires ',' or '}}' at offset {}",
                        self.cursor
                    ));
                }
            }
        }
        self.link_and_order_children(node, &children)?;
        Ok(())
    }

    fn parse_array(&mut self, node: u32, depth: usize) -> Result<(), String> {
        self.cursor += 1;
        self.skip_whitespace();
        let parent_id = self.nodes[usize::try_from(node).expect("u32 fits usize")]
            .container_id()
            .expect("array node has a stable container ID");
        let mut children = Vec::new();
        if self.bytes.get(self.cursor) == Some(&b']') {
            self.cursor += 1;
            return Ok(());
        }
        loop {
            let id = Arc::<str>::from(self.namespace.encode(self.next_array_ordinal));
            self.next_array_ordinal = self
                .next_array_ordinal
                .checked_add(1)
                .ok_or_else(|| "JSON array ID ordinal overflow".to_owned())?;
            let child = self.parse_value(
                RelationSeed::Array {
                    id,
                    parent_id: Arc::clone(&parent_id),
                },
                Some(node),
                depth,
            )?;
            children.push(child);
            self.skip_whitespace();
            match self.bytes.get(self.cursor) {
                Some(b',') => {
                    self.cursor += 1;
                    self.skip_whitespace();
                    if self.bytes.get(self.cursor) == Some(&b']') {
                        return Err("JSON arrays cannot have a trailing comma".to_owned());
                    }
                }
                Some(b']') => {
                    self.cursor += 1;
                    break;
                }
                _ => {
                    return Err(format!(
                        "JSON array requires ',' or ']' at offset {}",
                        self.cursor
                    ));
                }
            }
        }
        self.link_and_order_children(node, &children)?;
        Ok(())
    }

    fn link_and_order_children(&mut self, parent: u32, children: &[u32]) -> Result<(), String> {
        if let Some(first) = children.first() {
            self.nodes[usize::try_from(parent).expect("u32 fits usize")].first_child = Some(*first);
        }
        let order_keys = even_order_keys(children.len())?;
        for (position, child) in children.iter().copied().enumerate() {
            let next = children.get(position + 1).copied();
            let child = &mut self.nodes[usize::try_from(child).expect("u32 fits usize")];
            child.next_sibling = next;
            match &mut child.relation {
                NodeRelation::Root => {
                    return Err("JSON root cannot be a container child".to_owned());
                }
                NodeRelation::Object { order_key, .. } | NodeRelation::Array { order_key, .. } => {
                    *order_key = Arc::from(order_keys[position].as_str());
                }
            }
        }
        Ok(())
    }

    fn parse_number(&mut self) -> Result<(), String> {
        let start = self.cursor;
        if self.bytes.get(self.cursor) == Some(&b'-') {
            self.cursor += 1;
        }
        match self.bytes.get(self.cursor) {
            Some(b'0') => {
                self.cursor += 1;
                if self.bytes.get(self.cursor).is_some_and(u8::is_ascii_digit) {
                    return Err(format!("JSON number has a leading zero at offset {start}"));
                }
            }
            Some(b'1'..=b'9') => {
                self.cursor += 1;
                while self.bytes.get(self.cursor).is_some_and(u8::is_ascii_digit) {
                    self.cursor += 1;
                }
            }
            _ => return Err(format!("invalid JSON number at offset {start}")),
        }
        if self.bytes.get(self.cursor) == Some(&b'.') {
            self.cursor += 1;
            let fraction_start = self.cursor;
            while self.bytes.get(self.cursor).is_some_and(u8::is_ascii_digit) {
                self.cursor += 1;
            }
            if self.cursor == fraction_start {
                return Err(format!(
                    "JSON number has an empty fraction at offset {start}"
                ));
            }
        }
        if self
            .bytes
            .get(self.cursor)
            .is_some_and(|byte| matches!(byte, b'e' | b'E'))
        {
            self.cursor += 1;
            if self
                .bytes
                .get(self.cursor)
                .is_some_and(|byte| matches!(byte, b'+' | b'-'))
            {
                self.cursor += 1;
            }
            let exponent_start = self.cursor;
            while self.bytes.get(self.cursor).is_some_and(u8::is_ascii_digit) {
                self.cursor += 1;
            }
            if self.cursor == exponent_start {
                return Err(format!(
                    "JSON number has an empty exponent at offset {start}"
                ));
            }
        }
        Ok(())
    }

    fn skip_whitespace(&mut self) {
        while self
            .bytes
            .get(self.cursor)
            .is_some_and(|byte| matches!(byte, b' ' | b'\t' | b'\n' | b'\r'))
        {
            self.cursor += 1;
        }
    }
}

#[derive(Clone, Debug)]
pub struct Document(Arc<DocumentInner>);

#[derive(Debug)]
struct DocumentInner {
    blob: PersistentBlob,
    nodes: Arc<Vec<Node>>,
    spans: SpanIndex,
    lookup: Arc<HashMap<[u8; 16], u32>>,
    sparse_nodes_touched: usize,
}

impl Document {
    pub fn open_file(
        bytes: Vec<u8>,
        _path: Option<&str>,
        namespace: IdNamespace,
    ) -> Result<(Self, InitialChanges), String> {
        let shared = Arc::new(bytes);
        let nodes = JsonParser::parse(shared.as_slice(), namespace)?;
        let document = Self::from_parts(PersistentBlob::from_shared(shared)?, nodes, 0)?;
        let changes = document.initial_changes();
        Ok((document, changes))
    }

    fn from_parts(
        blob: PersistentBlob,
        nodes: Vec<Node>,
        sparse_nodes_touched: usize,
    ) -> Result<Self, String> {
        if nodes.is_empty() {
            return Err("JSON semantic graph has no root".to_owned());
        }
        let mut lookup = HashMap::with_capacity(nodes.len());
        for (ordinal, node) in nodes.iter().enumerate() {
            let fingerprint = identity_fingerprint_node(node);
            if lookup
                .insert(
                    fingerprint,
                    u32::try_from(ordinal).map_err(|_| "JSON has too many entities")?,
                )
                .is_some()
            {
                return Err(format!(
                    "duplicate or colliding JSON entity identity {:?}",
                    node.identity()
                ));
            }
        }
        let spans = SpanIndex::from_nodes(&nodes)?;
        Ok(Self(Arc::new(DocumentInner {
            blob,
            nodes: Arc::new(nodes),
            spans,
            lookup: Arc::new(lookup),
            sparse_nodes_touched,
        })))
    }

    fn from_sparse_parts(
        blob: PersistentBlob,
        nodes: Arc<Vec<Node>>,
        spans: SpanIndex,
        lookup: Arc<HashMap<[u8; 16], u32>>,
    ) -> Self {
        Self(Arc::new(DocumentInner {
            blob,
            nodes,
            spans,
            lookup,
            sparse_nodes_touched: 1,
        }))
    }

    pub fn fork(&self) -> Self {
        self.clone()
    }

    pub fn initial_changes(&self) -> InitialChanges {
        InitialChanges {
            document: self.clone(),
            node: 0,
        }
    }

    pub fn bytes(&self) -> Vec<u8> {
        self.0.blob.materialize()
    }

    pub fn retained_bytes_estimate(&self) -> usize {
        self.0.blob.retained_backing_bytes()
            + self.0.nodes.len() * size_of::<Node>()
            + self.0.spans.estimated_bytes()
            + self.0.lookup.len() * (size_of::<[u8; 16]>() + size_of::<u32>() + size_of::<usize>())
    }

    pub fn sparse_properties_touched(&self) -> usize {
        self.0.sparse_nodes_touched
    }

    pub fn file_changed(
        &self,
        splices: &[InputSplice<'_>],
        namespace: IdNamespace,
    ) -> Result<(Self, Vec<EntityChange>), String> {
        validate_splices(self.0.blob.len(), splices)?;
        if splices.is_empty() {
            return Ok((self.clone(), Vec::new()));
        }
        let scalar = self.single_scalar_node(splices)?;
        let after_blob = self.0.blob.splice(splices)?;
        if let Some(node_index) = scalar
            && let Some(result) =
                self.sparse_scalar_file_changed(after_blob.clone(), splices, node_index)?
        {
            return Ok(result);
        }
        let before_bytes = self.0.blob.materialize();
        let after_bytes = after_blob.materialize();
        let mut after_nodes = JsonParser::parse(&after_bytes, namespace)?;
        let before_nodes = self.nodes_with_current_spans()?;
        reconcile_trees(
            &before_nodes,
            &mut after_nodes,
            &before_bytes,
            &after_bytes,
            splices,
        )?;
        let after = Self::from_parts(after_blob, after_nodes, 0)?;
        self.full_file_changed_from_parsed(after, &before_bytes)
    }

    fn nodes_with_current_spans(&self) -> Result<Vec<Node>, String> {
        let mut nodes = self.0.nodes.as_ref().clone();
        for (ordinal, node) in nodes.iter_mut().enumerate() {
            let (start, len) = self
                .0
                .spans
                .span(ordinal)
                .ok_or_else(|| "JSON node span is missing".to_owned())?;
            node.value_start = start;
            node.value_len = len;
        }
        Ok(nodes)
    }

    fn sparse_scalar_file_changed(
        &self,
        after_blob: PersistentBlob,
        splices: &[InputSplice<'_>],
        node_index: usize,
    ) -> Result<Option<(Self, Vec<EntityChange>)>, String> {
        let (value_start, old_len) = self
            .0
            .spans
            .span(node_index)
            .ok_or_else(|| "JSON scalar span is missing".to_owned())?;
        let deleted = splices.iter().try_fold(0u64, |total, splice| {
            total
                .checked_add(splice.delete_len)
                .ok_or_else(|| "splice size overflow".to_owned())
        })?;
        let inserted = splices.iter().try_fold(0u64, |total, splice| {
            total
                .checked_add(u64::try_from(splice.insert.len()).expect("usize fits u64"))
                .ok_or_else(|| "splice size overflow".to_owned())
        })?;
        let delta = i64::try_from(inserted).map_err(|_| "JSON insert size overflow")?
            - i64::try_from(deleted).map_err(|_| "JSON delete size overflow")?;
        let new_len = add_signed(old_len, delta)?;
        let scalar = after_blob.range(
            value_start,
            value_start
                .checked_add(new_len)
                .ok_or_else(|| "JSON scalar range overflow".to_owned())?,
        )?;
        let Ok(new_kind) = parse_complete_scalar(&scalar) else {
            return Ok(None);
        };
        let node = &self.0.nodes[node_index];
        if new_kind.is_container() {
            return Ok(None);
        }

        let mut ancestors = HashSet::new();
        let mut parent = node.parent;
        while let Some(ordinal) = parent {
            let ordinal = usize::try_from(ordinal).expect("u32 fits usize");
            ancestors.insert(ordinal);
            parent = self.0.nodes[ordinal].parent;
        }
        let spans = self
            .0
            .spans
            .replace_scalar(node_index, &ancestors, new_len, delta)?;
        let nodes = if node.kind == new_kind {
            Arc::clone(&self.0.nodes)
        } else {
            let mut nodes = self.0.nodes.as_ref().clone();
            nodes[node_index].kind = new_kind;
            Arc::new(nodes)
        };
        let identity = node.identity();
        let before_snapshot = self.node_snapshot(node_index)?;
        let after = Self::from_sparse_parts(after_blob, nodes, spans, Arc::clone(&self.0.lookup));
        let after_snapshot = after.node_snapshot(node_index)?;
        let changes = if before_snapshot == after_snapshot {
            Vec::new()
        } else {
            vec![EntityChange::upsert(&identity, after_snapshot)]
        };
        Ok(Some((after, changes)))
    }

    fn full_file_changed_from_parsed(
        &self,
        after: Self,
        _before_bytes: &[u8],
    ) -> Result<(Self, Vec<EntityChange>), String> {
        let before = self.entity_snapshots()?;
        let after_snapshots = after.entity_snapshots()?;
        let mut changes = Vec::new();
        for identity in before.keys() {
            if !after_snapshots.contains_key(identity) {
                changes.push(EntityChange::delete(identity));
            }
        }
        for (identity, snapshot) in after_snapshots {
            if before.get(&identity) != Some(&snapshot) {
                changes.push(EntityChange::upsert(&identity, snapshot));
            }
        }
        Ok((after, changes))
    }

    fn single_scalar_node(&self, splices: &[InputSplice<'_>]) -> Result<Option<usize>, String> {
        let Some(first) = splices.first() else {
            return Ok(None);
        };
        let offset =
            u32::try_from(first.offset).map_err(|_| "splice offset exceeds 4GiB".to_owned())?;
        let candidate = self.0.spans.scalar_at_offset(offset);
        let Some(candidate) = candidate else {
            return Ok(None);
        };
        let node = &self.0.nodes[candidate];
        if node.kind.is_container() {
            return Ok(None);
        }
        let (value_start, value_len) = self
            .0
            .spans
            .span(candidate)
            .ok_or_else(|| "JSON scalar span is missing".to_owned())?;
        let end = value_start
            .checked_add(value_len)
            .ok_or_else(|| "JSON scalar range overflow".to_owned())?;
        for splice in splices {
            let splice_end = splice
                .offset
                .checked_add(splice.delete_len)
                .ok_or_else(|| "splice end overflow".to_owned())?;
            if splice.offset < u64::from(value_start) || splice_end > u64::from(end) {
                return Ok(None);
            }
        }
        Ok(Some(candidate))
    }

    pub fn entities_changed(
        &self,
        changes: &[EntityChange],
    ) -> Result<(Self, Vec<ByteEdit>), String> {
        if changes.is_empty() {
            return Ok((self.clone(), Vec::new()));
        }
        if changes.len() == 1
            && let Some(result) = self.single_scalar_entity_changed(&changes[0])?
        {
            return Ok(result);
        }

        let mut entities = self
            .initial_changes()
            .map(|change| {
                let change = change?;
                let record = EntityRecord {
                    schema_key: change.schema_key,
                    entity_pk: change.entity_pk,
                    snapshot: change.snapshot.expect("initial changes are upserts"),
                };
                Ok((
                    (record.schema_key.clone(), record.entity_pk.clone()),
                    record,
                ))
            })
            .collect::<Result<HashMap<_, _>, String>>()?;
        for change in changes {
            let key = (change.schema_key.clone(), change.entity_pk.clone());
            if let Some(snapshot) = &change.snapshot {
                entities.insert(
                    key,
                    EntityRecord {
                        schema_key: change.schema_key.clone(),
                        entity_pk: change.entity_pk.clone(),
                        snapshot: snapshot.clone(),
                    },
                );
            } else {
                entities.remove(&key);
            }
        }
        let (document, mut edit) = Self::open_entities(entities.into_values().collect())?;
        edit.delete_len = u64::try_from(self.0.blob.len()).expect("usize fits u64");
        Ok((document, vec![edit]))
    }

    fn single_scalar_entity_changed(
        &self,
        change: &EntityChange,
    ) -> Result<Option<(Self, Vec<ByteEdit>)>, String> {
        let identity = EntityIdentity::from_parts(&change.schema_key, &change.entity_pk)?;
        let Some(&node_index) = self.0.lookup.get(&identity_fingerprint(&identity)) else {
            return Ok(None);
        };
        let node_index = usize::try_from(node_index).expect("u32 fits usize");
        let node = &self.0.nodes[node_index];
        if node.identity() != identity {
            return Err("JSON entity identity fingerprint collision".to_owned());
        }
        if node.kind.is_container() {
            return Ok(None);
        }
        let Some(snapshot) = &change.snapshot else {
            return Ok(None);
        };
        let entity = SemanticEntity::parse(EntityRecord {
            schema_key: change.schema_key.clone(),
            entity_pk: change.entity_pk.clone(),
            snapshot: snapshot.clone(),
        })?;
        if entity.identity() != identity || entity.kind().is_container() {
            return Ok(None);
        }
        let current = SemanticEntity::parse(EntityRecord {
            schema_key: change.schema_key.clone(),
            entity_pk: change.entity_pk.clone(),
            snapshot: self.node_snapshot(node_index)?,
        })?;
        if !entity.same_location(&current) {
            return Ok(None);
        }
        let scalar = entity
            .scalar_json()
            .ok_or_else(|| "scalar JSON entity is missing scalar_json".to_owned())?;
        let scalar_kind = parse_complete_scalar(scalar.as_bytes())?;
        if scalar_kind != entity.kind() {
            return Err("scalar_json does not match the entity kind".to_owned());
        }
        let (value_start, value_len) = self
            .0
            .spans
            .span(node_index)
            .ok_or_else(|| "JSON scalar span is missing".to_owned())?;
        let before = self.0.blob.range(
            value_start,
            value_start
                .checked_add(value_len)
                .ok_or_else(|| "JSON scalar range overflow".to_owned())?,
        )?;
        if before == scalar.as_bytes() {
            return Ok(Some((self.clone(), Vec::new())));
        }
        let splice = InputSplice {
            offset: u64::from(value_start),
            delete_len: u64::from(value_len),
            insert: scalar.as_bytes(),
        };
        let (after, _) = self.file_changed(&[splice], IdNamespace([0; 16]))?;
        let insert = Arc::new(scalar.as_bytes().to_vec());
        Ok(Some((
            after,
            vec![ByteEdit {
                offset: u64::from(value_start),
                delete_len: u64::from(value_len),
                insert,
            }],
        )))
    }

    pub fn open_entities(entities: Vec<EntityRecord>) -> Result<(Self, ByteEdit), String> {
        let mut builder = EntityImportBuilder::new();
        for entity in entities {
            builder.push(entity)?;
        }
        builder.finish()
    }

    fn node_snapshot(&self, index: usize) -> Result<Vec<u8>, String> {
        let (value_start, value_len) = self
            .0
            .spans
            .span(index)
            .ok_or_else(|| "JSON node span is missing".to_owned())?;
        snapshot_node(&self.0.blob, &self.0.nodes[index], value_start, value_len)
    }

    fn entity_snapshots(&self) -> Result<HashMap<EntityIdentity, Vec<u8>>, String> {
        self.0
            .nodes
            .iter()
            .enumerate()
            .map(|(index, node)| Ok((node.identity(), self.node_snapshot(index)?)))
            .collect()
    }
}

#[derive(Clone, Debug)]
pub struct InitialChanges {
    document: Document,
    node: usize,
}

impl Iterator for InitialChanges {
    type Item = Result<EntityChange, String>;

    fn next(&mut self) -> Option<Self::Item> {
        let node = self.document.0.nodes.get(self.node)?;
        let identity = node.identity();
        let snapshot = self.document.node_snapshot(self.node);
        self.node += 1;
        Some(snapshot.map(|snapshot| EntityChange::upsert(&identity, snapshot)))
    }
}

#[derive(Debug, Default)]
pub struct EntityImportBuilder {
    entities: Vec<SemanticEntity>,
}

impl EntityImportBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, entity: EntityRecord) -> Result<(), String> {
        self.entities.push(SemanticEntity::parse(entity)?);
        Ok(())
    }

    pub fn finish(self) -> Result<(Document, ByteEdit), String> {
        let model = SemanticModel::new(self.entities)?;
        let rendered = Arc::new(model.render()?);
        let mut nodes = JsonParser::parse(rendered.as_slice(), IdNamespace([0; 16]))?;
        model.overlay(&mut nodes)?;
        drop(model);
        let document = Document::from_parts(
            PersistentBlob::from_shared(Arc::clone(&rendered))?,
            nodes,
            0,
        )?;
        Ok((
            document,
            ByteEdit {
                offset: 0,
                delete_len: 0,
                insert: rendered,
            },
        ))
    }
}

#[derive(Clone, Debug)]
enum SemanticEntity {
    Root {
        kind: NodeKind,
        scalar_json: Option<String>,
    },
    Object {
        parent_id: String,
        key: String,
        order_key: String,
        kind: NodeKind,
        scalar_json: Option<String>,
        container_id: Option<String>,
    },
    Array {
        id: String,
        parent_id: String,
        order_key: String,
        kind: NodeKind,
        scalar_json: Option<String>,
    },
}

impl SemanticEntity {
    fn parse(record: EntityRecord) -> Result<Self, String> {
        let identity = EntityIdentity::from_parts(&record.schema_key, &record.entity_pk)?;
        let value: Value = serde_json::from_slice(&record.snapshot)
            .map_err(|error| format!("invalid JSON entity snapshot: {error}"))?;
        reject_numbers(&value)?;
        let object = value
            .as_object()
            .ok_or_else(|| "JSON entity snapshot must be an object".to_owned())?;
        let kind = required_string(object, "kind").and_then(NodeKind::parse)?;
        let scalar_json = optional_string(object, "scalar_json")?;
        validate_scalar_fields(kind, scalar_json.as_deref())?;
        match identity {
            EntityIdentity::Root => {
                require_fields(object, &["id", "kind"], &["scalar_json"])?;
                if required_string(object, "id")? != ROOT_ID {
                    return Err("json_root snapshot id must be \"root\"".to_owned());
                }
                Ok(Self::Root { kind, scalar_json })
            }
            EntityIdentity::Object { parent_id, key } => {
                require_fields(
                    object,
                    &["parent_id", "key", "order_key", "kind"],
                    &["scalar_json", "container_id"],
                )?;
                if required_string(object, "parent_id")? != parent_id
                    || required_string(object, "key")? != key
                {
                    return Err(
                        "json_object_member snapshot does not match its primary key".to_owned()
                    );
                }
                let order_key = parse_order_key(required_string(object, "order_key")?)?;
                let container_id = optional_string(object, "container_id")?;
                if kind.is_container() {
                    let expected = derive_object_container_id(&parent_id, &key);
                    if container_id.as_deref() != Some(expected.as_str()) {
                        return Err(
                            "json_object_member container_id is not derived from parent/key"
                                .to_owned(),
                        );
                    }
                } else if container_id.is_some() {
                    return Err("scalar object member cannot have container_id".to_owned());
                }
                Ok(Self::Object {
                    parent_id,
                    key,
                    order_key,
                    kind,
                    scalar_json,
                    container_id,
                })
            }
            EntityIdentity::Array(id) => {
                require_fields(
                    object,
                    &["id", "parent_id", "order_key", "kind"],
                    &["scalar_json"],
                )?;
                if required_string(object, "id")? != id {
                    return Err("json_array_item snapshot ID does not match its key".to_owned());
                }
                let parent_id = required_string(object, "parent_id")?.to_owned();
                let order_key = parse_order_key(required_string(object, "order_key")?)?;
                Ok(Self::Array {
                    id,
                    parent_id,
                    order_key,
                    kind,
                    scalar_json,
                })
            }
        }
    }

    fn identity(&self) -> EntityIdentity {
        match self {
            Self::Root { .. } => EntityIdentity::Root,
            Self::Object { parent_id, key, .. } => EntityIdentity::Object {
                parent_id: parent_id.clone(),
                key: key.clone(),
            },
            Self::Array { id, .. } => EntityIdentity::Array(id.clone()),
        }
    }

    const fn kind(&self) -> NodeKind {
        match self {
            Self::Root { kind, .. } | Self::Object { kind, .. } | Self::Array { kind, .. } => *kind,
        }
    }

    fn scalar_json(&self) -> Option<&str> {
        match self {
            Self::Root { scalar_json, .. }
            | Self::Object { scalar_json, .. }
            | Self::Array { scalar_json, .. } => scalar_json.as_deref(),
        }
    }

    fn parent_id(&self) -> Option<&str> {
        match self {
            Self::Root { .. } => None,
            Self::Object { parent_id, .. } | Self::Array { parent_id, .. } => Some(parent_id),
        }
    }

    fn order_key(&self) -> Option<&str> {
        match self {
            Self::Root { .. } => None,
            Self::Object { order_key, .. } | Self::Array { order_key, .. } => Some(order_key),
        }
    }

    fn container_id(&self) -> Option<&str> {
        if !self.kind().is_container() {
            return None;
        }
        match self {
            Self::Root { .. } => Some(ROOT_ID),
            Self::Object { container_id, .. } => container_id.as_deref(),
            Self::Array { id, .. } => Some(id),
        }
    }

    fn same_location(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Root { .. }, Self::Root { .. }) => true,
            (
                Self::Object {
                    parent_id,
                    key,
                    order_key,
                    ..
                },
                Self::Object {
                    parent_id: other_parent,
                    key: other_key,
                    order_key: other_order,
                    ..
                },
            ) => parent_id == other_parent && key == other_key && order_key == other_order,
            (
                Self::Array {
                    id,
                    parent_id,
                    order_key,
                    ..
                },
                Self::Array {
                    id: other_id,
                    parent_id: other_parent,
                    order_key: other_order,
                    ..
                },
            ) => id == other_id && parent_id == other_parent && order_key == other_order,
            _ => false,
        }
    }
}

struct SemanticModel {
    entities: HashMap<EntityIdentity, SemanticEntity>,
    children: HashMap<String, Vec<EntityIdentity>>,
}

impl SemanticModel {
    fn new(entities: Vec<SemanticEntity>) -> Result<Self, String> {
        let mut by_identity = HashMap::with_capacity(entities.len());
        let mut children: HashMap<String, Vec<EntityIdentity>> = HashMap::new();
        for entity in entities {
            let identity = entity.identity();
            if let Some(parent_id) = entity.parent_id() {
                children
                    .entry(parent_id.to_owned())
                    .or_default()
                    .push(identity.clone());
            }
            if by_identity.insert(identity.clone(), entity).is_some() {
                return Err(format!("duplicate JSON entity {identity:?}"));
            }
        }
        if !by_identity.contains_key(&EntityIdentity::Root) {
            return Err("JSON entity graph requires one root".to_owned());
        }
        for identities in children.values_mut() {
            identities.sort_unstable_by(|left, right| {
                let left = &by_identity[left];
                let right = &by_identity[right];
                (left.order_key(), identity_tiebreak(left))
                    .cmp(&(right.order_key(), identity_tiebreak(right)))
            });
        }
        Ok(Self {
            entities: by_identity,
            children,
        })
    }

    fn render(&self) -> Result<Vec<u8>, String> {
        let mut output = Vec::new();
        let mut visiting = HashSet::new();
        let mut visited = HashSet::new();
        self.render_entity(
            &EntityIdentity::Root,
            &mut output,
            &mut visiting,
            &mut visited,
            0,
        )?;
        if visited.len() != self.entities.len() {
            return Err("JSON entity graph contains unreachable entities".to_owned());
        }
        Ok(output)
    }

    fn render_entity(
        &self,
        identity: &EntityIdentity,
        output: &mut Vec<u8>,
        visiting: &mut HashSet<EntityIdentity>,
        visited: &mut HashSet<EntityIdentity>,
        depth: usize,
    ) -> Result<(), String> {
        if depth > 1024 {
            return Err("JSON entity graph nesting exceeds 1024 levels".to_owned());
        }
        if !visiting.insert(identity.clone()) {
            return Err("JSON entity graph contains an owning cycle".to_owned());
        }
        let entity = self
            .entities
            .get(identity)
            .ok_or_else(|| format!("missing JSON entity {identity:?}"))?;
        match entity.kind() {
            NodeKind::Object => {
                output.push(b'{');
                let container_id = entity
                    .container_id()
                    .ok_or_else(|| "JSON object has no container identity".to_owned())?;
                let children = self
                    .children
                    .get(container_id)
                    .map_or(&[][..], Vec::as_slice);
                for (position, child_identity) in children.iter().enumerate() {
                    let child = &self.entities[child_identity];
                    let SemanticEntity::Object { key, .. } = child else {
                        return Err("JSON object contains a non-object-member entity".to_owned());
                    };
                    if position > 0 {
                        output.push(b',');
                    }
                    output.extend(
                        serde_json::to_vec(key)
                            .map_err(|error| format!("failed to render JSON key: {error}"))?,
                    );
                    output.push(b':');
                    self.render_entity(child_identity, output, visiting, visited, depth + 1)?;
                }
                output.push(b'}');
            }
            NodeKind::Array => {
                output.push(b'[');
                let container_id = entity
                    .container_id()
                    .ok_or_else(|| "JSON array has no container identity".to_owned())?;
                let children = self
                    .children
                    .get(container_id)
                    .map_or(&[][..], Vec::as_slice);
                for (position, child_identity) in children.iter().enumerate() {
                    if !matches!(self.entities[child_identity], SemanticEntity::Array { .. }) {
                        return Err("JSON array contains a non-array-item entity".to_owned());
                    }
                    if position > 0 {
                        output.push(b',');
                    }
                    self.render_entity(child_identity, output, visiting, visited, depth + 1)?;
                }
                output.push(b']');
            }
            _ => output.extend_from_slice(
                entity
                    .scalar_json()
                    .ok_or_else(|| "JSON scalar has no scalar_json".to_owned())?
                    .as_bytes(),
            ),
        }
        visiting.remove(identity);
        if !visited.insert(identity.clone()) {
            return Err("JSON entity graph has multiple owning parents".to_owned());
        }
        Ok(())
    }

    fn overlay(&self, nodes: &mut [Node]) -> Result<(), String> {
        let mut parsed_lookup = HashMap::<EntityIdentity, u32>::new();
        overlay_node(self, nodes, 0, &EntityIdentity::Root, &mut parsed_lookup)?;
        if parsed_lookup.len() != nodes.len() {
            return Err("rendered JSON node count differs from entity graph".to_owned());
        }
        Ok(())
    }
}

fn overlay_node(
    model: &SemanticModel,
    nodes: &mut [Node],
    node_index: u32,
    identity: &EntityIdentity,
    seen: &mut HashMap<EntityIdentity, u32>,
) -> Result<(), String> {
    let entity = model
        .entities
        .get(identity)
        .ok_or_else(|| format!("missing overlay entity {identity:?}"))?;
    let index = usize::try_from(node_index).expect("u32 fits usize");
    nodes[index].relation = match entity {
        SemanticEntity::Root { .. } => NodeRelation::Root,
        SemanticEntity::Object {
            parent_id,
            key,
            order_key,
            container_id,
            ..
        } => NodeRelation::Object {
            parent_id: Arc::from(parent_id.as_str()),
            key: Arc::from(key.as_str()),
            order_key: Arc::from(order_key.as_str()),
            container_id: container_id
                .as_ref()
                .map(|value| Arc::<str>::from(value.as_str())),
        },
        SemanticEntity::Array {
            id,
            parent_id,
            order_key,
            ..
        } => NodeRelation::Array {
            id: Arc::from(id.as_str()),
            parent_id: Arc::from(parent_id.as_str()),
            order_key: Arc::from(order_key.as_str()),
        },
    };
    if nodes[index].kind != entity.kind() {
        return Err("rendered JSON kind differs from entity graph".to_owned());
    }
    if seen.insert(identity.clone(), node_index).is_some() {
        return Err("entity graph overlays one identity more than once".to_owned());
    }
    let semantic_children = entity
        .container_id()
        .and_then(|container| model.children.get(container))
        .map_or(&[][..], Vec::as_slice);
    let parsed_children = direct_children(nodes, node_index);
    if parsed_children.len() != semantic_children.len() {
        return Err("rendered JSON child count differs from entity graph".to_owned());
    }
    for (child_index, child_identity) in parsed_children.into_iter().zip(semantic_children.iter()) {
        overlay_node(model, nodes, child_index, child_identity, seen)?;
    }
    Ok(())
}

fn snapshot_node(
    blob: &PersistentBlob,
    node: &Node,
    value_start: u32,
    value_len: u32,
) -> Result<Vec<u8>, String> {
    let scalar = if node.kind.is_container() {
        None
    } else {
        let bytes = blob.range(
            value_start,
            value_start
                .checked_add(value_len)
                .ok_or_else(|| "JSON scalar range overflow".to_owned())?,
        )?;
        Some(
            String::from_utf8(bytes)
                .map_err(|error| format!("retained JSON scalar is not UTF-8: {error}"))?,
        )
    };
    let value = match &node.relation {
        NodeRelation::Root => {
            let mut value = Map::new();
            value.insert("id".to_owned(), json!(ROOT_ID));
            value.insert("kind".to_owned(), json!(node.kind.as_str()));
            if let Some(scalar) = scalar {
                value.insert("scalar_json".to_owned(), Value::String(scalar));
            }
            Value::Object(value)
        }
        NodeRelation::Object {
            parent_id,
            key,
            order_key,
            container_id,
        } => {
            let mut value = Map::new();
            value.insert("parent_id".to_owned(), json!(parent_id.as_ref()));
            value.insert("key".to_owned(), json!(key.as_ref()));
            value.insert("order_key".to_owned(), json!(order_key.as_ref()));
            value.insert("kind".to_owned(), json!(node.kind.as_str()));
            if let Some(scalar) = scalar {
                value.insert("scalar_json".to_owned(), Value::String(scalar));
            }
            if let Some(container_id) = container_id {
                value.insert(
                    "container_id".to_owned(),
                    Value::String(container_id.to_string()),
                );
            }
            Value::Object(value)
        }
        NodeRelation::Array {
            id,
            parent_id,
            order_key,
        } => {
            let mut value = Map::new();
            value.insert("id".to_owned(), json!(id.as_ref()));
            value.insert("parent_id".to_owned(), json!(parent_id.as_ref()));
            value.insert("order_key".to_owned(), json!(order_key.as_ref()));
            value.insert("kind".to_owned(), json!(node.kind.as_str()));
            if let Some(scalar) = scalar {
                value.insert("scalar_json".to_owned(), Value::String(scalar));
            }
            Value::Object(value)
        }
    };
    serde_json::to_vec(&value)
        .map_err(|error| format!("failed to serialize JSON entity snapshot: {error}"))
}

#[derive(Clone, Copy, Debug)]
struct UnchangedSegment {
    after_start: u32,
    after_end: u32,
    before_start: u32,
}

#[derive(Clone, Debug)]
struct SpliceProvenance {
    segments: Vec<UnchangedSegment>,
}

impl SpliceProvenance {
    fn new(before_len: u32, splices: &[InputSplice<'_>]) -> Result<Self, String> {
        let mut segments = Vec::with_capacity(splices.len() + 1);
        let mut before_cursor = 0u32;
        let mut after_cursor = 0u32;
        for splice in splices {
            let splice_start =
                u32::try_from(splice.offset).map_err(|_| "splice offset exceeds 4GiB")?;
            let splice_end = u32::try_from(
                splice
                    .offset
                    .checked_add(splice.delete_len)
                    .ok_or_else(|| "splice end overflow".to_owned())?,
            )
            .map_err(|_| "splice end exceeds 4GiB")?;
            if splice_start > before_cursor {
                let len = splice_start - before_cursor;
                segments.push(UnchangedSegment {
                    after_start: after_cursor,
                    after_end: after_cursor + len,
                    before_start: before_cursor,
                });
                after_cursor = after_cursor
                    .checked_add(len)
                    .ok_or_else(|| "JSON after offset overflow".to_owned())?;
            }
            after_cursor = after_cursor
                .checked_add(
                    u32::try_from(splice.insert.len())
                        .map_err(|_| "JSON insert exceeds 4GiB".to_owned())?,
                )
                .ok_or_else(|| "JSON after offset overflow".to_owned())?;
            before_cursor = splice_end;
        }
        if before_cursor < before_len {
            let len = before_len - before_cursor;
            segments.push(UnchangedSegment {
                after_start: after_cursor,
                after_end: after_cursor + len,
                before_start: before_cursor,
            });
        }
        Ok(Self { segments })
    }

    fn before_span(&self, after_start: u32, len: u32) -> Option<(u32, u32)> {
        let after_end = after_start.checked_add(len)?;
        let segment = self
            .segments
            .iter()
            .find(|segment| segment.after_start <= after_start && after_end <= segment.after_end)?;
        Some((
            segment.before_start + after_start - segment.after_start,
            len,
        ))
    }
}

fn reconcile_trees(
    before: &[Node],
    after: &mut [Node],
    before_bytes: &[u8],
    after_bytes: &[u8],
    splices: &[InputSplice<'_>],
) -> Result<(), String> {
    if before.is_empty() || after.is_empty() {
        return Err("JSON reconciliation requires roots".to_owned());
    }
    let provenance = SpliceProvenance::new(
        u32::try_from(before_bytes.len()).map_err(|_| "JSON file exceeds 4GiB")?,
        splices,
    )?;
    reconcile_node(before, after, 0, 0, before_bytes, after_bytes, &provenance)
}

fn reconcile_node(
    before: &[Node],
    after: &mut [Node],
    before_index: u32,
    after_index: u32,
    before_bytes: &[u8],
    after_bytes: &[u8],
    provenance: &SpliceProvenance,
) -> Result<(), String> {
    let before_node = before[usize::try_from(before_index).expect("u32 fits usize")].clone();
    let after_kind = after[usize::try_from(after_index).expect("u32 fits usize")].kind;
    let adopted_array_id = if let (
        NodeRelation::Array {
            id: before_id,
            order_key: before_order,
            ..
        },
        NodeRelation::Array { id, order_key, .. },
    ) = (
        &before_node.relation,
        &mut after[usize::try_from(after_index).expect("u32 fits usize")].relation,
    ) {
        *id = Arc::clone(before_id);
        *order_key = Arc::clone(before_order);
        true
    } else {
        false
    };
    if adopted_array_id {
        rebase_descendants(after, after_index);
    }
    if before_node.kind != after_kind {
        return Ok(());
    }
    let Some(after_parent_id) =
        after[usize::try_from(after_index).expect("u32 fits usize")].container_id()
    else {
        return Ok(());
    };
    let before_children = direct_children(before, before_index);
    let after_children = direct_children(after, after_index);
    match after_kind {
        NodeKind::Object => {
            let mut old_by_key = HashMap::with_capacity(before_children.len());
            for old in &before_children {
                if let NodeRelation::Object { key, .. } =
                    &before[usize::try_from(*old).expect("u32 fits usize")].relation
                {
                    old_by_key.insert(key.to_string(), *old);
                }
            }
            let mut matches = Vec::new();
            for new in &after_children {
                refresh_parent(after, *new, Arc::clone(&after_parent_id));
                let key = match &after[usize::try_from(*new).expect("u32 fits usize")].relation {
                    NodeRelation::Object { key, .. } => key.to_string(),
                    _ => continue,
                };
                if let Some(old) = old_by_key.get(&key) {
                    matches.push((*old, *new));
                    reconcile_node(
                        before,
                        after,
                        *old,
                        *new,
                        before_bytes,
                        after_bytes,
                        provenance,
                    )?;
                }
            }
            reconcile_child_order(before, after, &before_children, &after_children, &matches)?;
        }
        NodeKind::Array => {
            let matches = match_array_children(
                before,
                after,
                &before_children,
                &after_children,
                before_bytes,
                after_bytes,
                provenance,
            );
            let match_by_new = matches
                .iter()
                .copied()
                .map(|(old, new)| (new, old))
                .collect::<HashMap<_, _>>();
            for new in &after_children {
                refresh_parent(after, *new, Arc::clone(&after_parent_id));
                if let Some(old) = match_by_new.get(new) {
                    reconcile_node(
                        before,
                        after,
                        *old,
                        *new,
                        before_bytes,
                        after_bytes,
                        provenance,
                    )?;
                }
            }
            reconcile_child_order(before, after, &before_children, &after_children, &matches)?;
        }
        _ => {}
    }
    Ok(())
}

fn rebase_descendants(nodes: &mut [Node], parent: u32) {
    let Some(parent_id) = nodes[usize::try_from(parent).expect("u32 fits usize")].container_id()
    else {
        return;
    };
    let children = direct_children(nodes, parent);
    for child in children {
        refresh_parent(nodes, child, Arc::clone(&parent_id));
        rebase_descendants(nodes, child);
    }
}

fn match_array_children(
    before: &[Node],
    after: &[Node],
    before_children: &[u32],
    after_children: &[u32],
    before_bytes: &[u8],
    after_bytes: &[u8],
    provenance: &SpliceProvenance,
) -> Vec<(u32, u32)> {
    let old_by_span = before_children
        .iter()
        .map(|old| {
            let node = &before[usize::try_from(*old).expect("u32 fits usize")];
            ((node.value_start, node.value_len), *old)
        })
        .collect::<HashMap<_, _>>();
    let mut matches = Vec::new();
    let mut matched_old = HashSet::new();
    let mut matched_new = HashSet::new();
    for new in after_children {
        let node = &after[usize::try_from(*new).expect("u32 fits usize")];
        let Some(before_span) = provenance.before_span(node.value_start, node.value_len) else {
            continue;
        };
        let Some(old) = old_by_span.get(&before_span) else {
            continue;
        };
        matched_old.insert(*old);
        matched_new.insert(*new);
        matches.push((*old, *new));
    }
    let provenance_matches = matches.clone();

    let mut old_by_hash: HashMap<[u8; 32], VecDeque<u32>> = HashMap::new();
    for old in before_children {
        if matched_old.contains(old) {
            continue;
        }
        old_by_hash
            .entry(node_hash(
                &before[usize::try_from(*old).expect("u32 fits usize")],
                before_bytes,
            ))
            .or_default()
            .push_back(*old);
    }
    for new in after_children {
        if matched_new.contains(new) {
            continue;
        }
        let hash = node_hash(
            &after[usize::try_from(*new).expect("u32 fits usize")],
            after_bytes,
        );
        let Some(candidates) = old_by_hash.get_mut(&hash) else {
            continue;
        };
        while let Some(old) = candidates.pop_front() {
            if node_bytes(
                &before[usize::try_from(old).expect("u32 fits usize")],
                before_bytes,
            ) == node_bytes(
                &after[usize::try_from(*new).expect("u32 fits usize")],
                after_bytes,
            ) {
                matched_old.insert(old);
                matched_new.insert(*new);
                matches.push((old, *new));
                break;
            }
        }
    }
    let old_positions = before_children
        .iter()
        .enumerate()
        .map(|(position, child)| (*child, position))
        .collect::<HashMap<_, _>>();
    let new_positions = after_children
        .iter()
        .enumerate()
        .map(|(position, child)| (*child, position))
        .collect::<HashMap<_, _>>();
    let mut anchors = provenance_matches
        .iter()
        .map(|(old, new)| (old_positions[old], new_positions[new]))
        .collect::<Vec<_>>();
    anchors.sort_unstable();
    let mut previous_new = None;
    anchors.retain(|(_, new)| {
        let keep = previous_new.is_none_or(|previous| previous < *new);
        if keep {
            previous_new = Some(*new);
        }
        keep
    });
    anchors.push((before_children.len(), after_children.len()));
    let mut old_start = 0usize;
    let mut new_start = 0usize;
    for (old_end, new_end) in anchors {
        let remaining_old = before_children[old_start..old_end]
            .iter()
            .copied()
            .filter(|old| !matched_old.contains(old))
            .collect::<Vec<_>>();
        let remaining_new = after_children[new_start..new_end]
            .iter()
            .copied()
            .filter(|new| !matched_new.contains(new))
            .collect::<Vec<_>>();
        for (old, new) in remaining_old.into_iter().zip(remaining_new) {
            matched_old.insert(old);
            matched_new.insert(new);
            matches.push((old, new));
        }
        old_start = old_end.saturating_add(1).min(before_children.len());
        new_start = new_end.saturating_add(1).min(after_children.len());
    }
    matches
}

fn reconcile_child_order(
    before: &[Node],
    after: &mut [Node],
    before_children: &[u32],
    after_children: &[u32],
    matches: &[(u32, u32)],
) -> Result<(), String> {
    if after_children.is_empty() {
        return Ok(());
    }
    let old_positions = before_children
        .iter()
        .enumerate()
        .map(|(position, child)| (*child, position))
        .collect::<HashMap<_, _>>();
    let old_by_new = matches
        .iter()
        .map(|(old, new)| (*new, *old))
        .collect::<HashMap<_, _>>();
    let sequence = after_children
        .iter()
        .enumerate()
        .filter_map(|(after_position, child)| {
            let old = old_by_new.get(child)?;
            Some((after_position, old_positions[old]))
        })
        .collect::<Vec<_>>();
    let mut anchors = vec![false; after_children.len()];
    if !sequence.is_empty() {
        let mut tails = Vec::<usize>::new();
        let mut previous = vec![None; sequence.len()];
        for (sequence_index, &(_, old_position)) in sequence.iter().enumerate() {
            let insertion = tails.partition_point(|tail| sequence[*tail].1 < old_position);
            if insertion > 0 {
                previous[sequence_index] = Some(tails[insertion - 1]);
            }
            if insertion == tails.len() {
                tails.push(sequence_index);
            } else {
                tails[insertion] = sequence_index;
            }
        }
        let mut cursor = tails.last().copied();
        while let Some(sequence_index) = cursor {
            anchors[sequence[sequence_index].0] = true;
            cursor = previous[sequence_index];
        }
    }

    for (position, child) in after_children.iter().copied().enumerate() {
        if !anchors[position] {
            continue;
        }
        let old = old_by_new[&child];
        let order_key =
            relation_order(&before[usize::try_from(old).expect("u32 fits usize")].relation)
                .to_owned();
        set_relation_order(
            &mut after[usize::try_from(child).expect("u32 fits usize")].relation,
            &order_key,
        );
    }

    let anchor_keys = after_children
        .iter()
        .enumerate()
        .filter(|(position, _)| anchors[*position])
        .map(|(_, child)| {
            relation_order(&after[usize::try_from(*child).expect("u32 fits usize")].relation)
        })
        .collect::<Vec<_>>();
    if anchor_keys.windows(2).any(|pair| pair[0] >= pair[1]) {
        let keys = even_order_keys(after_children.len())?;
        for (child, key) in after_children.iter().copied().zip(keys) {
            set_relation_order(
                &mut after[usize::try_from(child).expect("u32 fits usize")].relation,
                &key,
            );
        }
        return Ok(());
    }

    let mut position = 0usize;
    let mut previous_key: Option<String> = None;
    while position < after_children.len() {
        if anchors[position] {
            previous_key = Some(
                relation_order(
                    &after[usize::try_from(after_children[position]).expect("u32 fits usize")]
                        .relation,
                )
                .to_owned(),
            );
            position += 1;
            continue;
        }
        let run_start = position;
        while position < after_children.len() && !anchors[position] {
            position += 1;
        }
        let next_key = (position < after_children.len()).then(|| {
            relation_order(
                &after[usize::try_from(after_children[position]).expect("u32 fits usize")].relation,
            )
            .to_owned()
        });
        let keys = order_keys_between(
            previous_key.as_deref(),
            next_key.as_deref(),
            position - run_start,
        )?;
        for (child, key) in after_children[run_start..position]
            .iter()
            .copied()
            .zip(keys)
        {
            set_relation_order(
                &mut after[usize::try_from(child).expect("u32 fits usize")].relation,
                &key,
            );
            previous_key = Some(key);
        }
    }
    Ok(())
}

fn relation_order(relation: &NodeRelation) -> &str {
    match relation {
        NodeRelation::Root => "",
        NodeRelation::Object { order_key, .. } | NodeRelation::Array { order_key, .. } => {
            order_key.as_ref()
        }
    }
}

fn set_relation_order(relation: &mut NodeRelation, value: &str) {
    match relation {
        NodeRelation::Root => {}
        NodeRelation::Object { order_key, .. } | NodeRelation::Array { order_key, .. } => {
            *order_key = Arc::from(value);
        }
    }
}

fn refresh_parent(nodes: &mut [Node], child: u32, parent_id: Arc<str>) {
    let node = &mut nodes[usize::try_from(child).expect("u32 fits usize")];
    match &mut node.relation {
        NodeRelation::Root => {}
        NodeRelation::Object {
            parent_id: current,
            key,
            container_id,
            ..
        } => {
            *current = Arc::clone(&parent_id);
            *container_id = node.kind.is_container().then(|| {
                Arc::<str>::from(derive_object_container_id(parent_id.as_ref(), key.as_ref()))
            });
        }
        NodeRelation::Array {
            parent_id: current, ..
        } => *current = parent_id,
    }
}

fn direct_children(nodes: &[Node], parent: u32) -> Vec<u32> {
    let mut output = Vec::new();
    let mut cursor = nodes[usize::try_from(parent).expect("u32 fits usize")].first_child;
    while let Some(child) = cursor {
        output.push(child);
        cursor = nodes[usize::try_from(child).expect("u32 fits usize")].next_sibling;
    }
    output
}

fn node_bytes<'a>(node: &Node, bytes: &'a [u8]) -> &'a [u8] {
    let start = usize::try_from(node.value_start).expect("u32 fits usize");
    let end = start + usize::try_from(node.value_len).expect("u32 fits usize");
    &bytes[start..end]
}

fn node_hash(node: &Node, bytes: &[u8]) -> [u8; 32] {
    *blake3::hash(node_bytes(node, bytes)).as_bytes()
}

fn derive_object_container_id(parent_id: &str, key: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(OBJECT_CONTAINER_DOMAIN);
    hasher.update(
        &u64::try_from(parent_id.len())
            .expect("usize fits u64")
            .to_be_bytes(),
    );
    hasher.update(parent_id.as_bytes());
    hasher.update(
        &u64::try_from(key.len())
            .expect("usize fits u64")
            .to_be_bytes(),
    );
    hasher.update(key.as_bytes());
    format!("o_{}", URL_SAFE_NO_PAD.encode(hasher.finalize().as_bytes()))
}

fn even_order_keys(count: usize) -> Result<Vec<String>, String> {
    let denominator = u128::try_from(count + 1).map_err(|_| "JSON child count overflow")?;
    (0..count)
        .map(|index| {
            let numerator = u128::try_from(index + 1).map_err(|_| "JSON child index overflow")?
                * u128::from(u64::MAX);
            let rank =
                u64::try_from(numerator / denominator).map_err(|_| "JSON order rank overflow")? | 1;
            Ok(format!("{rank:016x}"))
        })
        .collect()
}

fn order_keys_between(
    previous: Option<&str>,
    next: Option<&str>,
    count: usize,
) -> Result<Vec<String>, String> {
    let previous = previous.map(decode_order_key).transpose()?;
    let next = next.map(decode_order_key).transpose()?;
    if let (Some(previous), Some(next)) = (&previous, &next)
        && previous >= next
    {
        return Err("JSON order-key bounds are not increasing".to_owned());
    }
    let mut output = Vec::with_capacity(count);
    fill_order_keys(previous.as_deref(), next.as_deref(), count, &mut output);
    Ok(output.into_iter().map(|bytes| encode_hex(&bytes)).collect())
}

fn fill_order_keys(
    previous: Option<&[u8]>,
    next: Option<&[u8]>,
    count: usize,
    output: &mut Vec<Vec<u8>>,
) {
    if count == 0 {
        return;
    }
    let left_count = count / 2;
    let right_count = count - left_count - 1;
    let key = midpoint_order_key(previous, next);
    fill_order_keys(previous, Some(&key), left_count, output);
    output.push(key.clone());
    fill_order_keys(Some(&key), next, right_count, output);
}

fn midpoint_order_key(previous: Option<&[u8]>, next: Option<&[u8]>) -> Vec<u8> {
    let previous = previous.unwrap_or_default();
    let next = next.unwrap_or_default();
    let mut prefix = Vec::new();
    let mut index = 0usize;
    loop {
        let previous_digit = previous.get(index).map_or(0, |byte| u16::from(*byte));
        let next_digit = next.get(index).map_or(256, |byte| u16::from(*byte));
        if next_digit > previous_digit + 1 {
            prefix.push(
                u8::try_from(previous_digit + (next_digit - previous_digit) / 2)
                    .expect("order-key midpoint fits u8"),
            );
            return prefix;
        }
        prefix.push(u8::try_from(previous_digit).expect("previous order-key byte fits u8"));
        index += 1;
    }
}

fn decode_order_key(raw: &str) -> Result<Vec<u8>, String> {
    parse_order_key(raw)?;
    let (pairs, remainder) = raw.as_bytes().as_chunks::<2>();
    debug_assert!(remainder.is_empty());
    pairs
        .iter()
        .map(|pair| {
            let high = hex_value(pair[0])?;
            let low = hex_value(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn hex_value(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err("invalid lowercase hexadecimal order key".to_owned()),
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn scan_string_end(bytes: &[u8], start: usize) -> Result<usize, String> {
    if bytes.get(start) != Some(&b'"') {
        return Err(format!("JSON string is missing at offset {start}"));
    }
    let mut cursor = start + 1;
    while let Some(&byte) = bytes.get(cursor) {
        match byte {
            b'"' => return Ok(cursor + 1),
            b'\\' => {
                cursor += 1;
                match bytes.get(cursor) {
                    Some(b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't') => {}
                    Some(b'u') => {
                        for digit in 1..=4 {
                            if !bytes.get(cursor + digit).is_some_and(u8::is_ascii_hexdigit) {
                                return Err(format!(
                                    "invalid Unicode escape at JSON offset {cursor}"
                                ));
                            }
                        }
                        cursor += 4;
                    }
                    _ => return Err(format!("invalid escape at JSON offset {cursor}")),
                }
            }
            0x00..=0x1f => {
                return Err(format!("control byte in JSON string at offset {cursor}"));
            }
            _ => {}
        }
        cursor += 1;
    }
    Err(format!("unterminated JSON string at offset {start}"))
}

fn parse_complete_scalar(bytes: &[u8]) -> Result<NodeKind, String> {
    let nodes = JsonParser::parse(bytes, IdNamespace([0; 16]))?;
    let root = nodes
        .first()
        .ok_or_else(|| "JSON scalar is missing".to_owned())?;
    if root.kind.is_container() {
        return Err("expected a JSON scalar, got a container".to_owned());
    }
    if root.value_start != 0
        || usize::try_from(root.value_len).expect("u32 fits usize") != bytes.len()
    {
        return Err("JSON scalar cannot have outer whitespace".to_owned());
    }
    Ok(root.kind)
}

fn validate_scalar_fields(kind: NodeKind, scalar_json: Option<&str>) -> Result<(), String> {
    if kind.is_container() {
        if scalar_json.is_some() {
            return Err("JSON container entity cannot carry scalar_json".to_owned());
        }
        return Ok(());
    }
    let scalar_json =
        scalar_json.ok_or_else(|| "JSON scalar entity requires scalar_json".to_owned())?;
    if parse_complete_scalar(scalar_json.as_bytes())? != kind {
        return Err("scalar_json kind does not match entity kind".to_owned());
    }
    Ok(())
}

fn require_fields(
    object: &Map<String, Value>,
    required: &[&str],
    optional: &[&str],
) -> Result<(), String> {
    for field in required {
        if !object.contains_key(*field) {
            return Err(format!("JSON entity snapshot is missing {field:?}"));
        }
    }
    if let Some(field) = object
        .keys()
        .find(|field| !required.contains(&field.as_str()) && !optional.contains(&field.as_str()))
    {
        return Err(format!(
            "JSON entity snapshot contains unsupported field {field:?}"
        ));
    }
    Ok(())
}

fn required_string<'a>(object: &'a Map<String, Value>, field: &str) -> Result<&'a str, String> {
    object
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("JSON entity field {field:?} must be a string"))
}

fn optional_string(object: &Map<String, Value>, field: &str) -> Result<Option<String>, String> {
    object
        .get(field)
        .map(|value| {
            value
                .as_str()
                .map(ToOwned::to_owned)
                .ok_or_else(|| format!("JSON entity field {field:?} must be a string"))
        })
        .transpose()
}

fn parse_order_key(raw: &str) -> Result<String, String> {
    if raw.is_empty()
        || !raw.len().is_multiple_of(2)
        || !raw
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || raw.ends_with("00")
    {
        return Err("invalid JSON order_key".to_owned());
    }
    Ok(raw.to_owned())
}

fn identity_tiebreak(entity: &SemanticEntity) -> &str {
    match entity {
        SemanticEntity::Root { .. } => "",
        SemanticEntity::Object { key, .. } => key,
        SemanticEntity::Array { id, .. } => id,
    }
}

fn reject_numbers(value: &Value) -> Result<(), String> {
    match value {
        Value::Number(_) => Err(
            "durable JSON entity snapshots must encode JSON numbers inside scalar_json strings"
                .to_owned(),
        ),
        Value::Array(values) => {
            for value in values {
                reject_numbers(value)?;
            }
            Ok(())
        }
        Value::Object(object) => {
            for value in object.values() {
                reject_numbers(value)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_splices(file_len: usize, splices: &[InputSplice<'_>]) -> Result<(), String> {
    let file_len = u64::try_from(file_len).expect("usize fits u64");
    let mut previous_end = 0u64;
    for splice in splices {
        let end = splice
            .offset
            .checked_add(splice.delete_len)
            .ok_or_else(|| "splice end overflow".to_owned())?;
        if splice.offset < previous_end {
            return Err("JSON splices must be sorted and non-overlapping".to_owned());
        }
        if end > file_len {
            return Err("JSON splice exceeds accepted file length".to_owned());
        }
        previous_end = end;
    }
    Ok(())
}

fn add_signed(value: u32, delta: i64) -> Result<u32, String> {
    let value = i64::from(value)
        .checked_add(delta)
        .ok_or_else(|| "JSON span arithmetic overflow".to_owned())?;
    u32::try_from(value).map_err(|_| "JSON span exceeds the supported range".to_owned())
}
