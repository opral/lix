wit_bindgen::generate!({
    path: "../wit",
    world: "plugin",
});

use exports::lix::plugin_p3_candidate::api::{
    Document, DocumentStats, EntityChange, EntitySummary, EntitySummaryStream, FileTransition,
    Guest, GuestDocument, InputSplice, OpenResult, PluginError,
};
use lix::plugin_p3_candidate::host::{ByteSource, SourceError};
use std::collections::HashSet;
use std::sync::Arc;
use wit_bindgen::rt::async_support::{StreamResult, spawn_local};

const INPUT_CHUNK_BYTES: usize = 1024 * 1024;
const OUTPUT_CHUNK_ENTITIES: usize = 4096;
const MAX_JSON_NESTING: usize = 256;
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

struct Candidate;

#[derive(Clone, Debug)]
struct IndexedEntity {
    id: u64,
    hash: u64,
    start: u64,
    end: u64,
}

#[derive(Clone, Debug)]
struct Override {
    index: usize,
    entity: IndexedEntity,
    parent: Option<Arc<Self>>,
}

#[derive(Clone, Debug)]
struct CandidateDocument {
    byte_length: u64,
    entities: Arc<Vec<IndexedEntity>>,
    overrides: Option<Arc<Override>>,
    revision: u64,
}

#[derive(Clone, Copy, Debug, Default)]
enum ScanState {
    #[default]
    BeforeRoot,
    BeforeMember {
        allow_end: bool,
    },
    InMember,
    AfterRoot,
}

#[derive(Debug, Default)]
struct IndexBuilder {
    entities: Vec<IndexedEntity>,
    entity_ids: HashSet<u64>,
    pending: Vec<u8>,
    pending_start: u64,
    offset: u64,
    state: ScanState,
    in_string: bool,
    escaped: bool,
    nested_closers: Vec<u8>,
}

impl IndexBuilder {
    fn push(&mut self, bytes: &[u8]) -> Result<(), PluginError> {
        for &byte in bytes {
            let offset = self.offset;
            self.offset = self
                .offset
                .checked_add(1)
                .ok_or_else(|| PluginError::LimitExceeded("file offset overflow".to_owned()))?;
            match self.state {
                ScanState::BeforeRoot => {
                    if is_json_whitespace(byte) {
                        continue;
                    }
                    if byte != b'{' {
                        return Err(PluginError::InvalidInput(
                            "JSON file must contain one top-level object".to_owned(),
                        ));
                    }
                    self.state = ScanState::BeforeMember { allow_end: true };
                }
                ScanState::BeforeMember { allow_end } => {
                    if is_json_whitespace(byte) {
                        continue;
                    }
                    if byte == b'}' {
                        if !allow_end {
                            return Err(PluginError::InvalidInput(
                                "JSON object has a trailing comma".to_owned(),
                            ));
                        }
                        self.state = ScanState::AfterRoot;
                        continue;
                    }
                    self.pending.clear();
                    self.pending_start = offset;
                    self.in_string = false;
                    self.escaped = false;
                    self.nested_closers.clear();
                    self.state = ScanState::InMember;
                    self.push_member_byte(byte)?;
                }
                ScanState::InMember => self.push_member_byte(byte)?,
                ScanState::AfterRoot => {
                    if !is_json_whitespace(byte) {
                        return Err(PluginError::InvalidInput(
                            "bytes follow the top-level JSON object".to_owned(),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    fn push_member_byte(&mut self, byte: u8) -> Result<(), PluginError> {
        if self.in_string {
            self.pending.push(byte);
            if self.escaped {
                self.escaped = false;
            } else if byte == b'\\' {
                self.escaped = true;
            } else if byte == b'"' {
                self.in_string = false;
            }
            return Ok(());
        }

        match byte {
            b'"' => {
                self.pending.push(byte);
                self.in_string = true;
            }
            b'{' => {
                self.pending.push(byte);
                self.nested_closers.push(b'}');
            }
            b'[' => {
                self.pending.push(byte);
                self.nested_closers.push(b']');
            }
            b'}' => {
                if let Some(expected) = self.nested_closers.last().copied() {
                    if expected != byte {
                        return Err(PluginError::InvalidInput(
                            "JSON value has mismatched delimiters".to_owned(),
                        ));
                    }
                    self.nested_closers.pop();
                    self.pending.push(byte);
                } else {
                    self.finish_member()?;
                    self.state = ScanState::AfterRoot;
                }
            }
            b']' => {
                let Some(expected) = self.nested_closers.pop() else {
                    return Err(PluginError::InvalidInput(
                        "JSON value has an unmatched closing bracket".to_owned(),
                    ));
                };
                if expected != byte {
                    return Err(PluginError::InvalidInput(
                        "JSON value has mismatched delimiters".to_owned(),
                    ));
                }
                self.pending.push(byte);
            }
            b',' if self.nested_closers.is_empty() => {
                self.finish_member()?;
                self.state = ScanState::BeforeMember { allow_end: false };
            }
            _ => self.pending.push(byte),
        }
        Ok(())
    }

    fn finish_member(&mut self) -> Result<(), PluginError> {
        let member_len = self
            .pending
            .iter()
            .rposition(|byte| !is_json_whitespace(*byte))
            .map_or(0, |index| index + 1);
        let member = self.pending.get(..member_len).ok_or_else(|| {
            PluginError::Internal("member range outside parser buffer".to_owned())
        })?;
        if member.is_empty() {
            return Err(PluginError::InvalidInput(
                "JSON object contains an empty member".to_owned(),
            ));
        }
        let id = parse_member(member)?;
        if !self.entity_ids.insert(id) {
            return Err(PluginError::InvalidInput(
                "JSON object contains duplicate property identity".to_owned(),
            ));
        }
        let start = self.pending_start;
        let length = u64::try_from(member.len()).expect("usize fits u64");
        u32::try_from(length)
            .map_err(|_| PluginError::LimitExceeded("JSON property exceeds 4 GiB".to_owned()))?;
        let end = start
            .checked_add(length)
            .ok_or_else(|| PluginError::LimitExceeded("member end overflow".to_owned()))?;
        self.entities.push(IndexedEntity {
            id,
            hash: fnv1a(member),
            start,
            end,
        });
        self.pending.clear();
        self.in_string = false;
        self.escaped = false;
        self.nested_closers.clear();
        Ok(())
    }

    fn finish(self) -> Result<Vec<IndexedEntity>, PluginError> {
        match self.state {
            ScanState::AfterRoot => Ok(self.entities),
            ScanState::BeforeRoot => {
                Err(PluginError::InvalidInput("JSON input is empty".to_owned()))
            }
            ScanState::BeforeMember { .. } | ScanState::InMember => Err(PluginError::InvalidInput(
                "JSON object is truncated".to_owned(),
            )),
        }
    }
}

struct JsonParser<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> JsonParser<'a> {
    fn new(bytes: &'a [u8]) -> Result<Self, PluginError> {
        std::str::from_utf8(bytes)
            .map_err(|error| PluginError::InvalidInput(format!("JSON is not UTF-8: {error}")))?;
        Ok(Self { bytes, position: 0 })
    }

    fn parse_member(mut self) -> Result<u64, PluginError> {
        self.skip_whitespace();
        let key_start = self.position;
        let key_end = self.parse_string()?;
        self.skip_whitespace();
        self.expect_byte(b':', "JSON object property is missing ':'")?;
        self.skip_whitespace();
        self.parse_value(0)?;
        self.skip_whitespace();
        if self.position != self.bytes.len() {
            return Err(PluginError::InvalidInput(
                "JSON object member has trailing syntax".to_owned(),
            ));
        }
        let decoded_key = decode_json_string_contents(&self.bytes[key_start + 1..key_end - 1])?;
        Ok(fnv1a(&decoded_key))
    }

    fn parse_value(&mut self, depth: usize) -> Result<(), PluginError> {
        if depth > MAX_JSON_NESTING {
            return Err(PluginError::LimitExceeded(
                "JSON nesting exceeds the candidate limit".to_owned(),
            ));
        }
        let Some(byte) = self.peek() else {
            return Err(PluginError::InvalidInput(
                "JSON value is truncated".to_owned(),
            ));
        };
        match byte {
            b'"' => {
                self.parse_string()?;
                Ok(())
            }
            b'{' => self.parse_object(depth + 1),
            b'[' => self.parse_array(depth + 1),
            b't' => self.parse_keyword(b"true"),
            b'f' => self.parse_keyword(b"false"),
            b'n' => self.parse_keyword(b"null"),
            b'-' | b'0'..=b'9' => self.parse_number(),
            _ => Err(PluginError::InvalidInput(
                "JSON value starts with an invalid token".to_owned(),
            )),
        }
    }

    fn parse_object(&mut self, depth: usize) -> Result<(), PluginError> {
        self.expect_byte(b'{', "JSON object is missing '{'")?;
        self.skip_whitespace();
        if self.consume(b'}') {
            return Ok(());
        }
        loop {
            self.parse_string()?;
            self.skip_whitespace();
            self.expect_byte(b':', "nested JSON property is missing ':'")?;
            self.skip_whitespace();
            self.parse_value(depth)?;
            self.skip_whitespace();
            if self.consume(b'}') {
                return Ok(());
            }
            self.expect_byte(b',', "nested JSON object requires ',' or '}'")?;
            self.skip_whitespace();
            if self.peek() == Some(b'}') {
                return Err(PluginError::InvalidInput(
                    "nested JSON object has a trailing comma".to_owned(),
                ));
            }
        }
    }

    fn parse_array(&mut self, depth: usize) -> Result<(), PluginError> {
        self.expect_byte(b'[', "JSON array is missing '['")?;
        self.skip_whitespace();
        if self.consume(b']') {
            return Ok(());
        }
        loop {
            self.parse_value(depth)?;
            self.skip_whitespace();
            if self.consume(b']') {
                return Ok(());
            }
            self.expect_byte(b',', "JSON array requires ',' or ']'")?;
            self.skip_whitespace();
            if self.peek() == Some(b']') {
                return Err(PluginError::InvalidInput(
                    "JSON array has a trailing comma".to_owned(),
                ));
            }
        }
    }

    fn parse_string(&mut self) -> Result<usize, PluginError> {
        self.expect_byte(b'"', "JSON object key/value must start with a quote")?;
        while let Some(byte) = self.peek() {
            self.position += 1;
            match byte {
                b'"' => return Ok(self.position),
                0x00..=0x1f => {
                    return Err(PluginError::InvalidInput(
                        "JSON string contains an unescaped control byte".to_owned(),
                    ));
                }
                b'\\' => {
                    let escape = self.peek().ok_or_else(|| {
                        PluginError::InvalidInput("JSON string escape is truncated".to_owned())
                    })?;
                    self.position += 1;
                    match escape {
                        b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => {}
                        b'u' => {
                            let first = self.parse_hex_quad()?;
                            if (0xd800..=0xdbff).contains(&first) {
                                if !self.consume(b'\\') || !self.consume(b'u') {
                                    return Err(PluginError::InvalidInput(
                                        "JSON high surrogate is missing its low surrogate"
                                            .to_owned(),
                                    ));
                                }
                                let second = self.parse_hex_quad()?;
                                if !(0xdc00..=0xdfff).contains(&second) {
                                    return Err(PluginError::InvalidInput(
                                        "JSON high surrogate is followed by an invalid low surrogate"
                                            .to_owned(),
                                    ));
                                }
                            } else if (0xdc00..=0xdfff).contains(&first) {
                                return Err(PluginError::InvalidInput(
                                    "JSON string contains an unpaired low surrogate".to_owned(),
                                ));
                            }
                        }
                        _ => {
                            return Err(PluginError::InvalidInput(
                                "JSON string contains an invalid escape".to_owned(),
                            ));
                        }
                    }
                }
                _ => {}
            }
        }
        Err(PluginError::InvalidInput(
            "JSON string is truncated".to_owned(),
        ))
    }

    fn parse_hex_quad(&mut self) -> Result<u16, PluginError> {
        let mut value = 0_u16;
        for _ in 0..4 {
            let digit = self.peek().ok_or_else(|| {
                PluginError::InvalidInput("JSON unicode escape is truncated".to_owned())
            })?;
            let nibble = hex_nibble(digit).ok_or_else(|| {
                PluginError::InvalidInput("JSON unicode escape contains a non-hex digit".to_owned())
            })?;
            self.position += 1;
            value = (value << 4) | u16::from(nibble);
        }
        Ok(value)
    }

    fn parse_keyword(&mut self, keyword: &[u8]) -> Result<(), PluginError> {
        let end = self
            .position
            .checked_add(keyword.len())
            .ok_or_else(|| PluginError::LimitExceeded("JSON keyword offset overflow".to_owned()))?;
        if self.bytes.get(self.position..end) != Some(keyword) {
            return Err(PluginError::InvalidInput(
                "JSON literal is invalid".to_owned(),
            ));
        }
        self.position = end;
        Ok(())
    }

    fn parse_number(&mut self) -> Result<(), PluginError> {
        self.consume(b'-');
        match self.peek() {
            Some(b'0') => {
                self.position += 1;
                if matches!(self.peek(), Some(b'0'..=b'9')) {
                    return Err(PluginError::InvalidInput(
                        "JSON number has a leading zero".to_owned(),
                    ));
                }
            }
            Some(b'1'..=b'9') => {
                self.position += 1;
                while matches!(self.peek(), Some(b'0'..=b'9')) {
                    self.position += 1;
                }
            }
            _ => {
                return Err(PluginError::InvalidInput(
                    "JSON number has no integer digits".to_owned(),
                ));
            }
        }
        if self.consume(b'.') {
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return Err(PluginError::InvalidInput(
                    "JSON number fraction has no digits".to_owned(),
                ));
            }
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.position += 1;
            }
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.position += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.position += 1;
            }
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return Err(PluginError::InvalidInput(
                    "JSON number exponent has no digits".to_owned(),
                ));
            }
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.position += 1;
            }
        }
        Ok(())
    }

    fn expect_byte(&mut self, expected: u8, message: &str) -> Result<(), PluginError> {
        if self.consume(expected) {
            Ok(())
        } else {
            Err(PluginError::InvalidInput(message.to_owned()))
        }
    }

    fn consume(&mut self, expected: u8) -> bool {
        if self.peek() == Some(expected) {
            self.position += 1;
            true
        } else {
            false
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.position).copied()
    }

    fn skip_whitespace(&mut self) {
        while self.peek().is_some_and(is_json_whitespace) {
            self.position += 1;
        }
    }
}

fn parse_member(member: &[u8]) -> Result<u64, PluginError> {
    JsonParser::new(member)?.parse_member()
}

fn decode_json_string_contents(input: &[u8]) -> Result<Vec<u8>, PluginError> {
    let mut output = Vec::with_capacity(input.len());
    let mut position = 0;
    while position < input.len() {
        let byte = input[position];
        position += 1;
        if byte != b'\\' {
            output.push(byte);
            continue;
        }
        let escape = *input.get(position).ok_or_else(|| {
            PluginError::InvalidInput("JSON string escape is truncated".to_owned())
        })?;
        position += 1;
        match escape {
            b'"' | b'\\' | b'/' => output.push(escape),
            b'b' => output.push(0x08),
            b'f' => output.push(0x0c),
            b'n' => output.push(b'\n'),
            b'r' => output.push(b'\r'),
            b't' => output.push(b'\t'),
            b'u' => {
                let first = decode_hex_quad(input, &mut position)?;
                let scalar = if (0xd800..=0xdbff).contains(&first) {
                    if input.get(position..position + 2) != Some(b"\\u") {
                        return Err(PluginError::InvalidInput(
                            "JSON high surrogate is missing its low surrogate".to_owned(),
                        ));
                    }
                    position += 2;
                    let second = decode_hex_quad(input, &mut position)?;
                    if !(0xdc00..=0xdfff).contains(&second) {
                        return Err(PluginError::InvalidInput(
                            "JSON high surrogate is followed by an invalid low surrogate"
                                .to_owned(),
                        ));
                    }
                    0x1_0000 + ((u32::from(first) - 0xd800) << 10) + (u32::from(second) - 0xdc00)
                } else if (0xdc00..=0xdfff).contains(&first) {
                    return Err(PluginError::InvalidInput(
                        "JSON string contains an unpaired low surrogate".to_owned(),
                    ));
                } else {
                    u32::from(first)
                };
                let character = char::from_u32(scalar).ok_or_else(|| {
                    PluginError::InvalidInput("JSON unicode escape is not a scalar".to_owned())
                })?;
                let mut encoded = [0_u8; 4];
                output.extend_from_slice(character.encode_utf8(&mut encoded).as_bytes());
            }
            _ => {
                return Err(PluginError::InvalidInput(
                    "JSON string contains an invalid escape".to_owned(),
                ));
            }
        }
    }
    Ok(output)
}

fn decode_hex_quad(input: &[u8], position: &mut usize) -> Result<u16, PluginError> {
    let end = position
        .checked_add(4)
        .ok_or_else(|| PluginError::LimitExceeded("JSON escape offset overflow".to_owned()))?;
    let digits = input
        .get(*position..end)
        .ok_or_else(|| PluginError::InvalidInput("JSON unicode escape is truncated".to_owned()))?;
    let mut value = 0_u16;
    for &digit in digits {
        let nibble = hex_nibble(digit).ok_or_else(|| {
            PluginError::InvalidInput("JSON unicode escape contains a non-hex digit".to_owned())
        })?;
        value = (value << 4) | u16::from(nibble);
    }
    *position = end;
    Ok(value)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn is_json_whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\r' | b'\n')
}

impl CandidateDocument {
    fn from_index(byte_length: u64, entities: Vec<IndexedEntity>) -> Self {
        Self {
            byte_length,
            entities: Arc::new(entities),
            overrides: None,
            revision: 0,
        }
    }

    fn entity(&self, index: usize) -> &IndexedEntity {
        let mut current = self.overrides.as_deref();
        while let Some(overlay) = current {
            if overlay.index == index {
                return &overlay.entity;
            }
            current = overlay.parent.as_deref();
        }
        &self.entities[index]
    }

    fn entity_index_at(&self, offset: u64) -> Option<usize> {
        self.entities
            .partition_point(|entity| entity.start <= offset)
            .checked_sub(1)
            .filter(|index| offset < self.entity(*index).end)
    }

    fn successor(
        &self,
        index: usize,
        entity: IndexedEntity,
        byte_length: u64,
    ) -> CandidateDocument {
        let parent = match self.overrides.as_ref() {
            Some(overlay) if overlay.index == index => overlay.parent.clone(),
            Some(overlay) => Some(Arc::clone(overlay)),
            None => None,
        };
        CandidateDocument {
            byte_length,
            entities: Arc::clone(&self.entities),
            overrides: Some(Arc::new(Override {
                index,
                entity,
                parent,
            })),
            revision: self.revision.wrapping_add(1),
        }
    }
}

impl Guest for Candidate {
    type Document = CandidateDocument;

    async fn open_list(file: Vec<u8>) -> Result<OpenResult, PluginError> {
        let byte_length = u64::try_from(file.len()).expect("usize fits u64");
        let mut builder = IndexBuilder::default();
        for chunk in file.chunks(INPUT_CHUNK_BYTES) {
            builder.push(chunk)?;
        }
        open_result(CandidateDocument::from_index(
            byte_length,
            builder.finish()?,
        ))
    }

    async fn open_stream(file: ByteSource) -> Result<OpenResult, PluginError> {
        let byte_length = file.len();
        let source = file.read_stream(0, byte_length).map_err(map_source_error)?;
        let mut reader = source.data;
        let mut buffer = Vec::with_capacity(INPUT_CHUNK_BYTES);
        let mut builder = IndexBuilder::default();
        let mut received = 0_u64;
        let mut stream_error = None;
        loop {
            let (status, mut filled) = reader.read(buffer).await;
            match received.checked_add(u64::try_from(filled.len()).expect("usize fits u64")) {
                Some(total) => received = total,
                None => {
                    stream_error.get_or_insert_with(|| {
                        PluginError::LimitExceeded("stream length overflow".to_owned())
                    });
                }
            }
            if stream_error.is_none()
                && let Err(error) = builder.push(&filled)
            {
                stream_error = Some(error);
            }
            filled.clear();
            buffer = filled;
            match status {
                StreamResult::Complete(_) => {}
                StreamResult::Dropped => break,
                StreamResult::Cancelled => {
                    stream_error.get_or_insert(PluginError::Cancelled);
                    break;
                }
            }
        }
        drop(reader);
        match source.done.await {
            Ok(()) => {}
            Err(error) => return Err(map_source_error(error)),
        }
        if received != byte_length {
            return Err(PluginError::InvalidInput(format!(
                "source stream length mismatch: expected {byte_length}, received {received}"
            )));
        }
        if let Some(error) = stream_error {
            return Err(error);
        }
        open_result(CandidateDocument::from_index(
            byte_length,
            builder.finish()?,
        ))
    }
}

impl GuestDocument for CandidateDocument {
    fn fork(&self) -> Document {
        Document::new(self.clone())
    }

    fn stats(&self) -> DocumentStats {
        DocumentStats {
            byte_length: self.byte_length,
            entity_count: u64::try_from(self.entities.len()).expect("usize fits u64"),
            revision: self.revision,
        }
    }

    fn file_changed(
        &self,
        before: ByteSource,
        after: ByteSource,
        edits: Vec<InputSplice>,
    ) -> Result<FileTransition, PluginError> {
        let [edit] = edits.as_slice() else {
            return Err(PluginError::InvalidInput(
                "candidate requires exactly one localized splice".to_owned(),
            ));
        };
        if edit.delete_len == 0 {
            return Err(PluginError::InvalidInput(
                "candidate requires a non-empty localized splice".to_owned(),
            ));
        }
        if edit.delete_len != u64::try_from(edit.insert.len()).expect("usize fits u64") {
            return Err(PluginError::InvalidInput(
                "candidate hot path currently requires an equal-length splice".to_owned(),
            ));
        }
        if before.len() != self.byte_length || after.len() != self.byte_length {
            return Err(PluginError::InvalidInput(
                "source length does not match accepted document".to_owned(),
            ));
        }
        let end = edit
            .offset
            .checked_add(edit.delete_len)
            .ok_or_else(|| PluginError::LimitExceeded("splice end overflow".to_owned()))?;
        let index = self
            .entity_index_at(edit.offset)
            .filter(|index| end <= self.entity(*index).end)
            .ok_or_else(|| {
                PluginError::InvalidInput("splice crosses an entity boundary".to_owned())
            })?;
        let accepted = self.entity(index);
        let length = accepted
            .end
            .checked_sub(accepted.start)
            .ok_or_else(|| PluginError::Internal("entity range is inverted".to_owned()))?;
        let length_u32 = u32::try_from(length)
            .map_err(|_| PluginError::LimitExceeded("entity exceeds u32 reads".to_owned()))?;
        let before_row = before
            .read(accepted.start, length_u32)
            .map_err(map_source_error)?;
        if before_row.len() != usize::try_from(length).expect("u32-sized member fits usize") {
            return Err(PluginError::InvalidInput(
                "accepted source returned a short property member".to_owned(),
            ));
        }
        if fnv1a(&before_row) != accepted.hash {
            return Err(PluginError::InvalidInput(
                "accepted source and retained index disagree".to_owned(),
            ));
        }
        if parse_member(&before_row)? != accepted.id {
            return Err(PluginError::InvalidInput(
                "accepted source property identity disagrees with retained index".to_owned(),
            ));
        }
        let after_row = after
            .read(accepted.start, length_u32)
            .map_err(map_source_error)?;
        if after_row.len() != before_row.len() {
            return Err(PluginError::InvalidInput(
                "successor source returned a short property member".to_owned(),
            ));
        }
        let relative_start = usize::try_from(edit.offset - accepted.start).map_err(|_| {
            PluginError::LimitExceeded("relative splice offset overflow".to_owned())
        })?;
        let delete_len = usize::try_from(edit.delete_len)
            .map_err(|_| PluginError::LimitExceeded("splice length exceeds usize".to_owned()))?;
        let relative_end = relative_start
            .checked_add(delete_len)
            .ok_or_else(|| PluginError::LimitExceeded("relative splice end overflow".to_owned()))?;
        if relative_end > before_row.len()
            || before_row[..relative_start] != after_row[..relative_start]
            || after_row[relative_start..relative_end] != *edit.insert.as_slice()
            || before_row[relative_end..] != after_row[relative_end..]
        {
            return Err(PluginError::InvalidInput(
                "successor source does not match the declared localized splice".to_owned(),
            ));
        }
        let after_id = parse_member(&after_row)?;
        if after_id != accepted.id {
            return Err(PluginError::InvalidInput(
                "localized value edits may not change the JSON property key".to_owned(),
            ));
        }
        let entity = IndexedEntity {
            id: accepted.id,
            hash: fnv1a(&after_row),
            start: accepted.start,
            end: accepted.end,
        };
        let successor = self.successor(index, entity, self.byte_length);
        Ok(FileTransition {
            document: Document::new(successor),
            changes: vec![EntityChange {
                entity_id: accepted.id,
                snapshot: after_row,
            }],
        })
    }
}

fn open_result(document: CandidateDocument) -> Result<OpenResult, PluginError> {
    let entries = Arc::clone(&document.entities);
    let count = u64::try_from(entries.len()).expect("usize fits u64");
    let (mut writer, reader) = wit_stream::new::<EntitySummary>();
    let (done_writer, done_reader) = wit_future::new::<Result<(), PluginError>>(cancelled_output);
    spawn_local(async move {
        for chunk in entries.chunks(OUTPUT_CHUNK_ENTITIES) {
            let values = chunk
                .iter()
                .map(|entity| EntitySummary {
                    entity_id: entity.id,
                    start: entity.start,
                    length: u32::try_from(entity.end - entity.start)
                        .expect("candidate entity length fits u32"),
                    hash: entity.hash,
                })
                .collect();
            if !writer.write_all(values).await.is_empty() {
                let _ = done_writer.write(Err(PluginError::Cancelled)).await;
                return;
            }
        }
        drop(writer);
        let _ = done_writer.write(Ok(())).await;
    });
    Ok(OpenResult {
        document: Document::new(document),
        entities: EntitySummaryStream {
            count,
            items: reader,
            done: done_reader,
        },
    })
}

fn cancelled_output() -> Result<(), PluginError> {
    Err(PluginError::Cancelled)
}

fn map_source_error(error: SourceError) -> PluginError {
    match error {
        SourceError::InvalidRange => {
            PluginError::InvalidInput("host byte-source range is invalid".to_owned())
        }
        SourceError::LimitExceeded(message) => PluginError::LimitExceeded(message),
        SourceError::Cancelled => PluginError::Cancelled,
        SourceError::Unavailable(message) => PluginError::Internal(message),
    }
}

fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(FNV_OFFSET, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index_with_chunk_size(
        input: &[u8],
        chunk_size: usize,
    ) -> Result<Vec<IndexedEntity>, PluginError> {
        let mut builder = IndexBuilder::default();
        for chunk in input.chunks(chunk_size) {
            builder.push(chunk)?;
        }
        builder.finish()
    }

    fn entity_bytes<'a>(input: &'a [u8], entity: &IndexedEntity) -> &'a [u8] {
        &input[usize::try_from(entity.start).unwrap()..usize::try_from(entity.end).unwrap()]
    }

    #[test]
    fn indexes_nested_members_across_every_small_chunk_size() {
        let input = r#"
          {
            "alpha":{"nested":[1,{"x":"comma, brace } and quote \""}]},
            "beta" : [true,false,null,-12.5e+2],
            "unicode_é":"snowman ☃"
          }
        "#
        .as_bytes();
        let expected = index_with_chunk_size(input, input.len()).unwrap();
        assert_eq!(expected.len(), 3);
        assert_eq!(expected[0].id, fnv1a(b"alpha"));
        assert_eq!(expected[1].id, fnv1a(b"beta"));
        assert_eq!(expected[2].id, fnv1a("unicode_é".as_bytes()));
        assert!(
            entity_bytes(input, &expected[0]).starts_with(br#""alpha":"#),
            "member starts at its key"
        );
        assert!(
            entity_bytes(input, &expected[0]).ends_with(b"}"),
            "member ends at its complete nested value"
        );
        assert_eq!(
            entity_bytes(input, &expected[1]),
            br#""beta" : [true,false,null,-12.5e+2]"#
        );

        for chunk_size in 1..=31 {
            let actual = index_with_chunk_size(input, chunk_size).unwrap();
            assert_eq!(actual.len(), expected.len(), "chunk size {chunk_size}");
            for (actual, expected) in actual.iter().zip(&expected) {
                assert_eq!(actual.id, expected.id, "chunk size {chunk_size}");
                assert_eq!(actual.hash, expected.hash, "chunk size {chunk_size}");
                assert_eq!(actual.start, expected.start, "chunk size {chunk_size}");
                assert_eq!(actual.end, expected.end, "chunk size {chunk_size}");
            }
        }
    }

    #[test]
    fn ranges_exclude_outer_syntax_separators_and_padding() {
        let input = b"{\n  \"a\" : 1 \n,\t\"b\":[2]\r\n}";
        let entities = index_with_chunk_size(input, 1).unwrap();
        assert_eq!(entities.len(), 2);
        assert_eq!(entity_bytes(input, &entities[0]), b"\"a\" : 1");
        assert_eq!(entity_bytes(input, &entities[1]), b"\"b\":[2]");
        assert_eq!(entities[0].hash, fnv1a(b"\"a\" : 1"));
        assert_eq!(entities[1].hash, fnv1a(b"\"b\":[2]"));
    }

    #[test]
    fn accepts_all_json_value_shapes() {
        let input = br#"{"s":"x\u0041","n":-0.1e+2,"t":true,"f":false,"z":null,"a":[],"o":{}}"#;
        let entities = index_with_chunk_size(input, 2).unwrap();
        assert_eq!(entities.len(), 7);
        for entity in &entities {
            assert_eq!(
                parse_member(entity_bytes(input, entity)).unwrap(),
                entity.id
            );
        }
    }

    #[test]
    fn rejects_malformed_or_truncated_json() {
        let malformed: &[&[u8]] = &[
            b"",
            b"[]",
            b"{",
            b"{\"a\":}",
            b"{\"a\":[1,]}",
            b"{\"a\":01}",
            b"{\"a\":\"unterminated}",
            b"{\"a\":true,}",
            b"{\"a\":{\"x\":1]}",
            b"{\"a\":true} trailing",
            b"{\"a\":\"bad\\q\"}",
            b"{\"a\":\"control\nbyte\"}",
        ];
        for input in malformed {
            assert!(
                index_with_chunk_size(input, 1).is_err(),
                "accepted malformed input: {}",
                String::from_utf8_lossy(input)
            );
        }
    }

    #[test]
    fn rejects_duplicate_property_identity() {
        let input = br#"{"same":1,"same":2}"#;
        assert!(index_with_chunk_size(input, 3).is_err());
    }

    #[test]
    fn decoded_key_spelling_defines_identity_and_duplicates() {
        let plain = parse_member(br#""a":1"#).unwrap();
        let escaped = parse_member(br#""\u0061":1"#).unwrap();
        assert_eq!(plain, escaped);
        assert_eq!(plain, fnv1a(b"a"));

        let literal_emoji = parse_member("\"😀\":1".as_bytes()).unwrap();
        let escaped_emoji = parse_member(br#""\uD83D\uDE00":1"#).unwrap();
        assert_eq!(literal_emoji, escaped_emoji);

        let duplicate = br#"{"a":1,"\u0061":2}"#;
        assert!(index_with_chunk_size(duplicate, 1).is_err());
        assert!(parse_member(br#""bad\uD800":1"#).is_err());
        assert!(parse_member(br#""bad\uDC00":1"#).is_err());
    }

    #[test]
    fn replacing_a_head_override_preserves_older_other_entity_overrides() {
        let base_entities = vec![
            IndexedEntity {
                id: 1,
                hash: 1,
                start: 1,
                end: 2,
            },
            IndexedEntity {
                id: 2,
                hash: 2,
                start: 3,
                end: 4,
            },
        ];
        let base = CandidateDocument::from_index(5, base_entities);
        let first = base.successor(
            0,
            IndexedEntity {
                hash: 10,
                ..base.entity(0).clone()
            },
            5,
        );
        let second = first.successor(
            1,
            IndexedEntity {
                hash: 20,
                ..first.entity(1).clone()
            },
            5,
        );
        let third = second.successor(
            0,
            IndexedEntity {
                hash: 30,
                ..second.entity(0).clone()
            },
            5,
        );

        assert_eq!(base.entity(0).hash, 1);
        assert_eq!(first.entity(0).hash, 10);
        assert_eq!(second.entity(0).hash, 10);
        assert_eq!(second.entity(1).hash, 20);
        assert_eq!(third.entity(0).hash, 30);
        assert_eq!(third.entity(1).hash, 20);
    }
}

#[cfg(target_family = "wasm")]
export!(Candidate);
