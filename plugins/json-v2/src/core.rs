use serde_json::value::RawValue;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::mem::size_of;
use std::sync::Arc;

pub const PROPERTY_SCHEMA_KEY: &str = "json_property";

const PROPERTIES_PER_CHUNK: usize = 512;
const EMPTY_LOOKUP_SLOT: u32 = u32::MAX;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IdNamespace(pub [u8; 16]);

impl IdNamespace {
    pub fn from_halves(high: u64, low: u64) -> Self {
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&high.to_be_bytes());
        bytes[8..].copy_from_slice(&low.to_be_bytes());
        Self(bytes)
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JsonPropertySnapshot {
    pub key: String,
    pub order_key: String,
    pub value_json: String,
}

impl EntityChange {
    fn upsert(key: String, snapshot: Vec<u8>) -> Self {
        Self {
            schema_key: PROPERTY_SCHEMA_KEY.to_owned(),
            entity_pk: vec![key],
            snapshot: Some(snapshot),
            effect: ChangeEffect::Content,
        }
    }

    fn delete(key: String) -> Self {
        Self {
            schema_key: PROPERTY_SCHEMA_KEY.to_owned(),
            entity_pk: vec![key],
            snapshot: None,
            effect: ChangeEffect::Content,
        }
    }
}

/// Immutable piece table for accepted JSON bytes. A sparse successor keeps
/// the accepted backing allocation and owns only the inserted bytes.
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
            .expect("the complete JSON blob range is valid");
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
                let piece_start = piece.start + (selected_start - logical_start);
                let piece_end = piece.start + (selected_end - logical_start);
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
                        start: piece.start + (selected_start - logical_start),
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

    #[cfg(test)]
    fn single_backing(&self) -> Option<&Arc<Vec<u8>>> {
        (self.pieces.len() == 1 && self.pieces[0].start == 0 && self.pieces[0].len == self.len)
            .then(|| &self.pieces[0].bytes)
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

#[derive(Clone, Copy, Debug)]
struct PropertyDraft {
    member_start: u32,
    member_len: u32,
    key_start: u32,
    key_len: u32,
    value_start: u32,
    value_len: u32,
    order_rank: u64,
    key_hash: u64,
}

#[derive(Clone, Copy, Debug)]
struct CompactProperty {
    relative_member_start: u32,
    member_len: u32,
    relative_key_start: u32,
    key_len: u32,
    relative_value_start: u32,
    value_len: u32,
    order_rank: u64,
    key_hash: u64,
}

#[derive(Clone, Debug)]
struct PropertyChunk {
    properties: Box<[CompactProperty]>,
}

#[derive(Clone, Debug)]
struct ChunkRef {
    byte_start: u32,
    data: Arc<PropertyChunk>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PropertyLocation {
    ordinal: usize,
    chunk: usize,
    within_chunk: usize,
}

#[derive(Clone, Debug)]
struct PropertyIndex {
    chunks: Arc<Vec<ChunkRef>>,
    lookup: Arc<Vec<u32>>,
    property_count: u32,
}

impl PropertyIndex {
    fn from_drafts(bytes: &[u8], drafts: Vec<PropertyDraft>) -> Result<Self, String> {
        let property_count =
            u32::try_from(drafts.len()).map_err(|_| "JSON has too many properties".to_owned())?;
        let lookup_len = drafts
            .len()
            .saturating_mul(2)
            .max(2)
            .checked_next_power_of_two()
            .ok_or_else(|| "JSON property lookup is too large".to_owned())?;
        let mut lookup = vec![EMPTY_LOOKUP_SLOT; lookup_len];
        let mask = lookup_len - 1;
        for (ordinal, draft) in drafts.iter().enumerate() {
            let key = decode_key_range(bytes, draft.key_start, draft.key_len)?;
            let mut slot = folded_hash(draft.key_hash) & mask;
            loop {
                let existing = lookup[slot];
                if existing == EMPTY_LOOKUP_SLOT {
                    lookup[slot] =
                        u32::try_from(ordinal).map_err(|_| "JSON has too many properties")?;
                    break;
                }
                let existing = drafts[usize::try_from(existing).expect("u32 fits usize")];
                if existing.key_hash == draft.key_hash
                    && decode_key_range(bytes, existing.key_start, existing.key_len)? == key
                {
                    return Err(format!("duplicate top-level JSON property {key:?}"));
                }
                slot = (slot + 1) & mask;
            }
        }

        let mut chunks = Vec::with_capacity(drafts.len().div_ceil(PROPERTIES_PER_CHUNK));
        for group in drafts.chunks(PROPERTIES_PER_CHUNK) {
            let byte_start = group.first().map_or(0, |property| property.member_start);
            let properties = group
                .iter()
                .map(|property| CompactProperty {
                    relative_member_start: property.member_start - byte_start,
                    member_len: property.member_len,
                    relative_key_start: property.key_start - byte_start,
                    key_len: property.key_len,
                    relative_value_start: property.value_start - byte_start,
                    value_len: property.value_len,
                    order_rank: property.order_rank,
                    key_hash: property.key_hash,
                })
                .collect::<Vec<_>>()
                .into_boxed_slice();
            chunks.push(ChunkRef {
                byte_start,
                data: Arc::new(PropertyChunk { properties }),
            });
        }
        Ok(Self {
            chunks: Arc::new(chunks),
            lookup: Arc::new(lookup),
            property_count,
        })
    }

    fn count(&self) -> usize {
        usize::try_from(self.property_count).expect("u32 fits usize")
    }

    fn location(&self, ordinal: usize) -> Option<PropertyLocation> {
        (ordinal < self.count()).then_some(PropertyLocation {
            ordinal,
            chunk: ordinal / PROPERTIES_PER_CHUNK,
            within_chunk: ordinal % PROPERTIES_PER_CHUNK,
        })
    }

    fn property(&self, location: PropertyLocation) -> (&ChunkRef, &CompactProperty) {
        let chunk = &self.chunks[location.chunk];
        (chunk, &chunk.data.properties[location.within_chunk])
    }

    fn absolute(&self, location: PropertyLocation) -> PropertyDraft {
        let (chunk, property) = self.property(location);
        PropertyDraft {
            member_start: chunk.byte_start + property.relative_member_start,
            member_len: property.member_len,
            key_start: chunk.byte_start + property.relative_key_start,
            key_len: property.key_len,
            value_start: chunk.byte_start + property.relative_value_start,
            value_len: property.value_len,
            order_rank: property.order_rank,
            key_hash: property.key_hash,
        }
    }

    fn find_key(
        &self,
        blob: &PersistentBlob,
        key: &str,
    ) -> Result<Option<PropertyLocation>, String> {
        if self.property_count == 0 {
            return Ok(None);
        }
        let hash = hash_key(key);
        let mask = self.lookup.len() - 1;
        let mut slot = folded_hash(hash) & mask;
        loop {
            let ordinal = self.lookup[slot];
            if ordinal == EMPTY_LOOKUP_SLOT {
                return Ok(None);
            }
            let location = self
                .location(usize::try_from(ordinal).expect("u32 fits usize"))
                .expect("lookup ordinal is valid");
            let property = self.absolute(location);
            if property.key_hash == hash && decode_key(blob, property)? == key {
                return Ok(Some(location));
            }
            slot = (slot + 1) & mask;
        }
    }

    fn location_at_or_before_offset(&self, offset: u32) -> Option<PropertyLocation> {
        let chunk = self
            .chunks
            .partition_point(|candidate| candidate.byte_start <= offset)
            .checked_sub(1)?;
        let chunk_ref = &self.chunks[chunk];
        let relative = offset.checked_sub(chunk_ref.byte_start)?;
        let within_chunk = chunk_ref
            .data
            .properties
            .partition_point(|property| property.relative_member_start <= relative)
            .checked_sub(1)?;
        let ordinal = chunk
            .checked_mul(PROPERTIES_PER_CHUNK)?
            .checked_add(within_chunk)?;
        self.location(ordinal)
    }

    fn replace_value(
        &self,
        location: PropertyLocation,
        new_value_len: u32,
        delta: i64,
    ) -> Result<Self, String> {
        if delta == 0 {
            return Ok(self.clone());
        }
        let mut chunks = self.chunks.as_ref().clone();
        let chunk = &chunks[location.chunk];
        let mut properties = chunk.data.properties.to_vec();
        let selected = &mut properties[location.within_chunk];
        selected.value_len = new_value_len;
        selected.member_len = add_signed(selected.member_len, delta)?;
        for property in &mut properties[location.within_chunk + 1..] {
            property.relative_member_start = add_signed(property.relative_member_start, delta)?;
            property.relative_key_start = add_signed(property.relative_key_start, delta)?;
            property.relative_value_start = add_signed(property.relative_value_start, delta)?;
        }
        chunks[location.chunk] = ChunkRef {
            byte_start: chunk.byte_start,
            data: Arc::new(PropertyChunk {
                properties: properties.into_boxed_slice(),
            }),
        };
        for chunk in &mut chunks[location.chunk + 1..] {
            chunk.byte_start = add_signed(chunk.byte_start, delta)?;
        }
        Ok(Self {
            chunks: Arc::new(chunks),
            lookup: Arc::clone(&self.lookup),
            property_count: self.property_count,
        })
    }

    fn estimated_bytes(&self) -> usize {
        self.chunks.len() * size_of::<ChunkRef>()
            + self
                .chunks
                .iter()
                .map(|chunk| {
                    size_of::<PropertyChunk>()
                        + chunk.data.properties.len() * size_of::<CompactProperty>()
                })
                .sum::<usize>()
            + self.lookup.len() * size_of::<u32>()
    }
}

#[derive(Clone, Debug)]
pub struct Document(Arc<DocumentInner>);

#[derive(Debug)]
struct DocumentInner {
    blob: PersistentBlob,
    index: PropertyIndex,
    order_overrides: Arc<HashMap<u32, Arc<str>>>,
    sparse_properties_touched: usize,
}

impl Document {
    pub fn open_file(
        bytes: Vec<u8>,
        _path: Option<&str>,
        _namespace: IdNamespace,
    ) -> Result<(Self, InitialChanges), String> {
        let document = Self::from_shared(Arc::new(bytes))?;
        let changes = document.initial_changes();
        Ok((document, changes))
    }

    fn from_shared(bytes: Arc<Vec<u8>>) -> Result<Self, String> {
        if bytes.len() > u32::MAX as usize {
            return Err("JSON v2 currently supports files smaller than 4GiB".to_owned());
        }
        std::str::from_utf8(&bytes).map_err(|error| format!("JSON must be UTF-8: {error}"))?;
        let drafts = scan_top_level_object(&bytes)?;
        let index = PropertyIndex::from_drafts(&bytes, drafts)?;
        Ok(Self(Arc::new(DocumentInner {
            blob: PersistentBlob::from_shared(bytes)?,
            index,
            order_overrides: Arc::new(HashMap::new()),
            sparse_properties_touched: 0,
        })))
    }

    fn from_rendered_properties(
        bytes: Arc<Vec<u8>>,
        properties: &[JsonPropertySnapshot],
    ) -> Result<Self, String> {
        let document = Self::from_shared(bytes)?;
        if document.property_count() != properties.len() {
            return Err("rendered JSON property count changed unexpectedly".to_owned());
        }
        let mut overrides = HashMap::new();
        for (ordinal, property) in properties.iter().enumerate() {
            let location = document
                .0
                .index
                .location(ordinal)
                .expect("rendered property ordinal is valid");
            let expected = format!("{:016x}", document.0.index.absolute(location).order_rank);
            if property.order_key != expected {
                overrides.insert(
                    u32::try_from(ordinal).expect("property count fits u32"),
                    Arc::from(property.order_key.as_str()),
                );
            }
        }
        Ok(Self(Arc::new(DocumentInner {
            blob: document.0.blob.clone(),
            index: document.0.index.clone(),
            order_overrides: Arc::new(overrides),
            sparse_properties_touched: 0,
        })))
    }

    pub fn fork(&self) -> Self {
        self.clone()
    }

    pub fn initial_changes(&self) -> InitialChanges {
        InitialChanges {
            document: self.clone(),
            property: 0,
        }
    }

    pub fn bytes(&self) -> Vec<u8> {
        self.0.blob.materialize()
    }

    pub fn property_count(&self) -> usize {
        self.0.index.count()
    }

    pub fn retained_bytes_estimate(&self) -> usize {
        self.0.blob.retained_backing_bytes()
            + self.0.index.estimated_bytes()
            + self.0.order_overrides.len()
                * (size_of::<u32>() + size_of::<Arc<str>>() + size_of::<usize>())
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

        if let Some(location) = self.single_value_location(splices)? {
            let before_property = self.0.index.absolute(location);
            let after_blob = self.0.blob.splice(splices)?;
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
            let delta = i64::try_from(inserted).expect("u64 fits i64 for a u32 file")
                - i64::try_from(deleted).expect("u64 fits i64 for a u32 file");
            let new_value_len = add_signed(before_property.value_len, delta)?;
            let new_value_end = before_property
                .value_start
                .checked_add(new_value_len)
                .ok_or_else(|| "JSON value range overflow".to_owned())?;
            let new_value = after_blob.range(before_property.value_start, new_value_end)?;
            if validate_raw_value(&new_value).is_err() {
                return self.full_file_changed(splices, namespace);
            }

            let before_value = self.0.blob.range(
                before_property.value_start,
                before_property.value_start + before_property.value_len,
            )?;
            let index = self.0.index.replace_value(location, new_value_len, delta)?;
            let document = Self(Arc::new(DocumentInner {
                blob: after_blob,
                index,
                order_overrides: Arc::clone(&self.0.order_overrides),
                sparse_properties_touched: 1,
            }));
            if before_value == new_value {
                return Ok((document, Vec::new()));
            }
            let key = document.property_key(location)?;
            let snapshot = document.property_snapshot(location)?;
            return Ok((document, vec![EntityChange::upsert(key, snapshot)]));
        }

        self.full_file_changed(splices, namespace)
    }

    pub fn entities_changed(
        &self,
        changes: &[EntityChange],
    ) -> Result<(Self, Vec<ByteEdit>), String> {
        if changes.is_empty() {
            return Ok((self.clone(), Vec::new()));
        }
        if changes.len() == 1 && changes[0].schema_key == PROPERTY_SCHEMA_KEY {
            let change = &changes[0];
            let key = single_entity_key(change)?;
            let existing = self.0.index.find_key(&self.0.blob, key)?;
            if let (Some(snapshot_bytes), Some(location)) = (&change.snapshot, existing) {
                let snapshot = parse_property_snapshot(snapshot_bytes)?;
                if snapshot.key != key {
                    return Err("JSON property snapshot key does not match entity key".to_owned());
                }
                let current = self.property_semantic(location)?;
                if snapshot.order_key == current.order_key {
                    if snapshot.value_json == current.value_json {
                        return Ok((self.clone(), Vec::new()));
                    }
                    let property = self.0.index.absolute(location);
                    let insert = Arc::new(snapshot.value_json.as_bytes().to_vec());
                    let splice = InputSplice {
                        offset: u64::from(property.value_start),
                        delete_len: u64::from(property.value_len),
                        insert: insert.as_slice(),
                    };
                    let blob = self.0.blob.splice(&[splice])?;
                    let delta = i64::try_from(insert.len()).expect("usize fits i64")
                        - i64::from(property.value_len);
                    let index = self.0.index.replace_value(
                        location,
                        u32::try_from(insert.len())
                            .map_err(|_| "JSON value exceeds 4GiB".to_owned())?,
                        delta,
                    )?;
                    let document = Self(Arc::new(DocumentInner {
                        blob,
                        index,
                        order_overrides: Arc::clone(&self.0.order_overrides),
                        sparse_properties_touched: 1,
                    }));
                    return Ok((
                        document,
                        vec![ByteEdit {
                            offset: u64::from(property.value_start),
                            delete_len: u64::from(property.value_len),
                            insert,
                        }],
                    ));
                }
            } else if change.snapshot.is_none() && existing.is_none() {
                return Ok((self.clone(), Vec::new()));
            }
        }
        self.full_entities_changed(changes)
    }

    pub fn open_entities(entities: Vec<EntityRecord>) -> Result<(Self, ByteEdit), String> {
        let mut builder = EntityImportBuilder::new();
        for entity in entities {
            builder.push(entity)?;
        }
        builder.finish()
    }

    fn single_value_location(
        &self,
        splices: &[InputSplice<'_>],
    ) -> Result<Option<PropertyLocation>, String> {
        let Some(first) = splices.first() else {
            return Ok(None);
        };
        let first_offset =
            u32::try_from(first.offset).map_err(|_| "splice offset exceeds 4GiB".to_owned())?;
        let Some(location) = self.0.index.location_at_or_before_offset(first_offset) else {
            return Ok(None);
        };
        let property = self.0.index.absolute(location);
        let property_value_end = property.value_start + property.value_len;
        if first_offset < property.value_start || first_offset > property_value_end {
            return Ok(None);
        }
        let value_end = u64::from(property.value_start + property.value_len);
        for splice in splices {
            let splice_end = splice
                .offset
                .checked_add(splice.delete_len)
                .ok_or_else(|| "splice end overflow".to_owned())?;
            if splice.offset < u64::from(property.value_start) || splice_end > value_end {
                return Ok(None);
            }
        }
        Ok(Some(location))
    }

    fn full_file_changed(
        &self,
        splices: &[InputSplice<'_>],
        _namespace: IdNamespace,
    ) -> Result<(Self, Vec<EntityChange>), String> {
        let after_blob = self.0.blob.splice(splices)?;
        let after = Self::from_shared(Arc::new(after_blob.materialize()))?;
        let mut changes = Vec::new();
        for ordinal in 0..self.property_count() {
            let old_location = self.0.index.location(ordinal).expect("ordinal is valid");
            let key = self.property_key(old_location)?;
            match after.0.index.find_key(&after.0.blob, &key)? {
                None => changes.push(EntityChange::delete(key)),
                Some(new_location) => {
                    let old_snapshot = self.property_snapshot(old_location)?;
                    let new_snapshot = after.property_snapshot(new_location)?;
                    if old_snapshot != new_snapshot {
                        changes.push(EntityChange::upsert(key, new_snapshot));
                    }
                }
            }
        }
        for ordinal in 0..after.property_count() {
            let new_location = after.0.index.location(ordinal).expect("ordinal is valid");
            let key = after.property_key(new_location)?;
            if self.0.index.find_key(&self.0.blob, &key)?.is_none() {
                changes.push(EntityChange::upsert(
                    key,
                    after.property_snapshot(new_location)?,
                ));
            }
        }
        Ok((after, changes))
    }

    fn full_entities_changed(
        &self,
        changes: &[EntityChange],
    ) -> Result<(Self, Vec<ByteEdit>), String> {
        let mut properties = HashMap::with_capacity(self.property_count() + changes.len());
        for ordinal in 0..self.property_count() {
            let location = self.0.index.location(ordinal).expect("ordinal is valid");
            let snapshot = self.property_semantic(location)?;
            properties.insert(snapshot.key.clone(), snapshot);
        }
        for change in changes {
            if change.schema_key != PROPERTY_SCHEMA_KEY {
                return Err(format!(
                    "unsupported JSON entity schema {:?}",
                    change.schema_key
                ));
            }
            let key = single_entity_key(change)?.to_owned();
            if let Some(snapshot) = &change.snapshot {
                let snapshot = parse_property_snapshot(snapshot)?;
                if snapshot.key != key {
                    return Err("JSON property snapshot key does not match entity key".to_owned());
                }
                properties.insert(key, snapshot);
            } else {
                properties.remove(&key);
            }
        }
        let mut properties = properties.into_values().collect::<Vec<_>>();
        properties.sort_unstable_by(|left, right| {
            (&left.order_key, &left.key).cmp(&(&right.order_key, &right.key))
        });
        let rendered = Arc::new(render_properties(&properties)?);
        let document = Self::from_rendered_properties(Arc::clone(&rendered), &properties)?;
        Ok((
            document,
            vec![ByteEdit {
                offset: 0,
                delete_len: u64::try_from(self.0.blob.len()).expect("usize fits u64"),
                insert: rendered,
            }],
        ))
    }

    fn property_key(&self, location: PropertyLocation) -> Result<String, String> {
        decode_key(&self.0.blob, self.0.index.absolute(location))
    }

    fn property_semantic(
        &self,
        location: PropertyLocation,
    ) -> Result<JsonPropertySnapshot, String> {
        let property = self.0.index.absolute(location);
        let key = decode_key(&self.0.blob, property)?;
        let value = self.0.blob.range(
            property.value_start,
            property.value_start + property.value_len,
        )?;
        let value_json = String::from_utf8(value)
            .map_err(|error| format!("JSON value is not UTF-8: {error}"))?;
        Ok(JsonPropertySnapshot {
            key,
            order_key: self
                .0
                .order_overrides
                .get(&u32::try_from(location.ordinal).expect("property ordinal fits u32"))
                .map_or_else(
                    || format!("{:016x}", property.order_rank),
                    ToString::to_string,
                ),
            value_json,
        })
    }

    fn property_snapshot(&self, location: PropertyLocation) -> Result<Vec<u8>, String> {
        snapshot_bytes(&self.property_semantic(location)?)
    }

    #[cfg(test)]
    pub(crate) fn shares_blob_backing_with(&self, other: &Self) -> bool {
        self.0.blob.pieces.iter().any(|left| {
            other
                .0
                .blob
                .pieces
                .iter()
                .any(|right| Arc::ptr_eq(&left.bytes, &right.bytes))
        })
    }

    #[cfg(test)]
    pub(crate) fn shares_single_blob_with(&self, bytes: &Arc<Vec<u8>>) -> bool {
        self.0
            .blob
            .single_backing()
            .is_some_and(|backing| Arc::ptr_eq(backing, bytes))
    }

    #[cfg(test)]
    pub(crate) fn blob_piece_count(&self) -> usize {
        self.0.blob.pieces.len()
    }

    pub fn sparse_properties_touched(&self) -> usize {
        self.0.sparse_properties_touched
    }
}

#[derive(Clone, Debug)]
pub struct InitialChanges {
    document: Document,
    property: usize,
}

impl Iterator for InitialChanges {
    type Item = Result<EntityChange, String>;

    fn next(&mut self) -> Option<Self::Item> {
        let location = self.document.0.index.location(self.property)?;
        self.property += 1;
        let key = match self.document.property_key(location) {
            Ok(key) => key,
            Err(error) => return Some(Err(error)),
        };
        Some(
            self.document
                .property_snapshot(location)
                .map(|snapshot| EntityChange::upsert(key, snapshot)),
        )
    }
}

#[derive(Debug, Default)]
pub struct EntityImportBuilder {
    properties: Vec<JsonPropertySnapshot>,
}

impl EntityImportBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, entity: EntityRecord) -> Result<(), String> {
        if entity.schema_key != PROPERTY_SCHEMA_KEY {
            return Err(format!(
                "unsupported JSON entity schema {:?}",
                entity.schema_key
            ));
        }
        if entity.entity_pk.len() != 1 {
            return Err("JSON property entities require one primary-key component".to_owned());
        }
        let snapshot = parse_property_snapshot(&entity.snapshot)?;
        if snapshot.key != entity.entity_pk[0] {
            return Err("JSON property snapshot key does not match entity key".to_owned());
        }
        self.properties.push(snapshot);
        Ok(())
    }

    pub fn finish(mut self) -> Result<(Document, ByteEdit), String> {
        self.properties
            .sort_unstable_by(|left, right| left.key.cmp(&right.key));
        if let Some(duplicate) = self
            .properties
            .windows(2)
            .find(|pair| pair[0].key == pair[1].key)
        {
            return Err(format!(
                "duplicate JSON property entity {:?}",
                duplicate[0].key
            ));
        }
        self.properties.sort_unstable_by(|left, right| {
            (&left.order_key, &left.key).cmp(&(&right.order_key, &right.key))
        });
        let rendered = Arc::new(render_properties(&self.properties)?);
        let document = Document::from_rendered_properties(Arc::clone(&rendered), &self.properties)?;
        drop(self.properties);
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

pub fn parse_property_snapshot(bytes: &[u8]) -> Result<JsonPropertySnapshot, String> {
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|error| format!("invalid JSON property snapshot: {error}"))?;
    reject_numbers(&value)?;
    let object = value
        .as_object()
        .ok_or_else(|| "JSON property snapshot must be an object".to_owned())?;
    if object.len() != 3
        || !object.contains_key("key")
        || !object.contains_key("order_key")
        || !object.contains_key("value_json")
    {
        return Err(
            "JSON property snapshot must contain only key, order_key, and value_json".to_owned(),
        );
    }
    let key = object
        .get("key")
        .and_then(Value::as_str)
        .ok_or_else(|| "JSON property key must be a string".to_owned())?
        .to_owned();
    let order_key = object
        .get("order_key")
        .and_then(Value::as_str)
        .filter(|key| valid_order_key(key))
        .ok_or_else(|| "JSON property order_key is invalid".to_owned())?
        .to_owned();
    let value_json = object
        .get("value_json")
        .and_then(Value::as_str)
        .ok_or_else(|| "JSON property value_json must be a string".to_owned())?
        .to_owned();
    validate_raw_value(value_json.as_bytes())?;
    Ok(JsonPropertySnapshot {
        key,
        order_key,
        value_json,
    })
}

fn snapshot_bytes(snapshot: &JsonPropertySnapshot) -> Result<Vec<u8>, String> {
    serde_json::to_vec(&json!({
        "key": snapshot.key,
        "order_key": snapshot.order_key,
        "value_json": snapshot.value_json,
    }))
    .map_err(|error| format!("failed to serialize JSON property snapshot: {error}"))
}

fn scan_top_level_object(bytes: &[u8]) -> Result<Vec<PropertyDraft>, String> {
    if bytes.len() < 2 || bytes.first() != Some(&b'{') || bytes.last() != Some(&b'}') {
        return Err(
            "JSON v2 requires a canonical top-level object with no outer whitespace".to_owned(),
        );
    }
    if bytes.len() == 2 {
        return Ok(Vec::new());
    }
    let mut drafts = Vec::new();
    let mut cursor = 1usize;
    loop {
        let member_start = cursor;
        let key_end = scan_string_end(bytes, cursor)?;
        let key_literal = &bytes[cursor..key_end];
        let key: String = serde_json::from_slice(key_literal)
            .map_err(|error| format!("invalid JSON property key: {error}"))?;
        if serde_json::to_vec(&key)
            .map_err(|error| format!("failed to encode JSON property key: {error}"))?
            != key_literal
        {
            return Err("JSON property keys must use canonical JSON string encoding".to_owned());
        }
        cursor = key_end;
        if bytes.get(cursor) != Some(&b':') {
            return Err("JSON v2 requires ':' immediately after each property key".to_owned());
        }
        cursor += 1;
        let value_start = cursor;
        let value_len = raw_value_len(&bytes[value_start..])?;
        cursor = value_start
            .checked_add(value_len)
            .ok_or_else(|| "JSON value range overflow".to_owned())?;
        let member_len = cursor
            .checked_sub(member_start)
            .ok_or_else(|| "JSON member range underflow".to_owned())?;
        drafts.push(PropertyDraft {
            member_start: u32::try_from(member_start)
                .map_err(|_| "JSON offset exceeds 4GiB".to_owned())?,
            member_len: u32::try_from(member_len)
                .map_err(|_| "JSON member exceeds 4GiB".to_owned())?,
            key_start: u32::try_from(member_start)
                .map_err(|_| "JSON offset exceeds 4GiB".to_owned())?,
            key_len: u32::try_from(key_literal.len())
                .map_err(|_| "JSON key exceeds 4GiB".to_owned())?,
            value_start: u32::try_from(value_start)
                .map_err(|_| "JSON offset exceeds 4GiB".to_owned())?,
            value_len: u32::try_from(value_len)
                .map_err(|_| "JSON value exceeds 4GiB".to_owned())?,
            order_rank: 0,
            key_hash: hash_key(&key),
        });
        match bytes.get(cursor) {
            Some(b',') => {
                cursor += 1;
                if bytes.get(cursor) == Some(&b'}') {
                    return Err("JSON objects cannot have a trailing comma".to_owned());
                }
            }
            Some(b'}') if cursor + 1 == bytes.len() => break,
            Some(b'}') => {
                return Err("JSON document has bytes after the top-level object".to_owned());
            }
            _ => {
                return Err(
                    "JSON v2 requires ',' or '}' immediately after each property value".to_owned(),
                );
            }
        }
    }
    let denominator = u128::try_from(drafts.len() + 1).expect("usize fits u128");
    for (index, draft) in drafts.iter_mut().enumerate() {
        let numerator = u128::try_from(index + 1).expect("usize fits u128") * u128::from(u64::MAX);
        draft.order_rank = u64::try_from(numerator / denominator).expect("rank fits u64") | 1;
    }
    Ok(drafts)
}

fn scan_string_end(bytes: &[u8], start: usize) -> Result<usize, String> {
    if bytes.get(start) != Some(&b'"') {
        return Err("JSON v2 requires a property string immediately after '{' or ','".to_owned());
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
                                return Err(
                                    "invalid Unicode escape in JSON property key".to_owned()
                                );
                            }
                        }
                        cursor += 4;
                    }
                    _ => return Err("invalid escape in JSON property key".to_owned()),
                }
            }
            0x00..=0x1f => return Err("control byte in JSON property key".to_owned()),
            _ => {}
        }
        cursor += 1;
    }
    Err("unterminated JSON property key".to_owned())
}

fn raw_value_len(bytes: &[u8]) -> Result<usize, String> {
    if bytes.is_empty() || bytes[0].is_ascii_whitespace() {
        return Err("JSON property values cannot have leading outer whitespace".to_owned());
    }
    let mut stream = serde_json::Deserializer::from_slice(bytes).into_iter::<&RawValue>();
    let value = stream
        .next()
        .ok_or_else(|| "JSON property value is missing".to_owned())?
        .map_err(|error| format!("invalid JSON property value: {error}"))?;
    let consumed = stream.byte_offset();
    if consumed != value.get().len() {
        return Err("JSON property value parser consumed inconsistent bytes".to_owned());
    }
    Ok(consumed)
}

fn validate_raw_value(bytes: &[u8]) -> Result<(), String> {
    let len = raw_value_len(bytes)?;
    if len != bytes.len() {
        return Err("JSON property value has trailing bytes or outer whitespace".to_owned());
    }
    Ok(())
}

fn decode_key(blob: &PersistentBlob, property: PropertyDraft) -> Result<String, String> {
    let literal = blob.range(
        property.key_start,
        property
            .key_start
            .checked_add(property.key_len)
            .ok_or_else(|| "JSON key range overflow".to_owned())?,
    )?;
    serde_json::from_slice(&literal).map_err(|error| format!("invalid retained JSON key: {error}"))
}

fn decode_key_range(bytes: &[u8], start: u32, len: u32) -> Result<String, String> {
    let start = usize::try_from(start).expect("u32 fits usize");
    let end = start
        .checked_add(usize::try_from(len).expect("u32 fits usize"))
        .ok_or_else(|| "JSON key range overflow".to_owned())?;
    serde_json::from_slice(
        bytes
            .get(start..end)
            .ok_or_else(|| "JSON key range exceeds document".to_owned())?,
    )
    .map_err(|error| format!("invalid indexed JSON key: {error}"))
}

fn render_properties(properties: &[JsonPropertySnapshot]) -> Result<Vec<u8>, String> {
    let rendered_len = properties.iter().try_fold(2usize, |total, property| {
        validate_raw_value(property.value_json.as_bytes())?;
        let key = serde_json::to_vec(&property.key)
            .map_err(|error| format!("failed to encode JSON property key: {error}"))?;
        total
            .checked_add(key.len())
            .and_then(|value| value.checked_add(1))
            .and_then(|value| value.checked_add(property.value_json.len()))
            .and_then(|value| value.checked_add(usize::from(total > 2)))
            .ok_or_else(|| "rendered JSON size overflow".to_owned())
    })?;
    if rendered_len > u32::MAX as usize {
        return Err("JSON v2 currently supports files smaller than 4GiB".to_owned());
    }
    let mut output = Vec::with_capacity(rendered_len);
    output.push(b'{');
    for (index, property) in properties.iter().enumerate() {
        if index > 0 {
            output.push(b',');
        }
        output.extend(
            serde_json::to_vec(&property.key)
                .map_err(|error| format!("failed to encode JSON property key: {error}"))?,
        );
        output.push(b':');
        output.extend_from_slice(property.value_json.as_bytes());
    }
    output.push(b'}');
    Ok(output)
}

fn valid_order_key(key: &str) -> bool {
    !key.is_empty()
        && key.len().is_multiple_of(2)
        && key
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        && key.as_bytes()[key.len() - 2..] != *b"00"
}

fn reject_numbers(value: &Value) -> Result<(), String> {
    match value {
        Value::Number(_) => {
            return Err("JSON property snapshots cannot contain JSON number nodes".to_owned());
        }
        Value::Array(values) => {
            for value in values {
                reject_numbers(value)?;
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                reject_numbers(value)?;
            }
        }
        Value::Null | Value::Bool(_) | Value::String(_) => {}
    }
    Ok(())
}

fn single_entity_key(change: &EntityChange) -> Result<&str, String> {
    if change.entity_pk.len() != 1 {
        return Err("JSON property entities require one primary-key component".to_owned());
    }
    Ok(&change.entity_pk[0])
}

fn validate_splices(file_len: usize, splices: &[InputSplice<'_>]) -> Result<(), String> {
    let file_len = u64::try_from(file_len).expect("usize fits u64");
    let mut previous_end = 0u64;
    for (index, splice) in splices.iter().enumerate() {
        let end = splice
            .offset
            .checked_add(splice.delete_len)
            .ok_or_else(|| "splice end overflow".to_owned())?;
        if end > file_len {
            return Err("splice range exceeds accepted JSON bytes".to_owned());
        }
        if index > 0 && splice.offset < previous_end {
            return Err("splices must be increasing and non-overlapping".to_owned());
        }
        previous_end = end;
    }
    Ok(())
}

fn add_signed(value: u32, delta: i64) -> Result<u32, String> {
    let result = i64::from(value)
        .checked_add(delta)
        .ok_or_else(|| "JSON offset overflow".to_owned())?;
    u32::try_from(result).map_err(|_| "JSON offset escaped the supported range".to_owned())
}

fn hash_key(key: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in key.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn folded_hash(hash: u64) -> usize {
    let bytes = hash.to_le_bytes();
    let low = u32::from_le_bytes(bytes[..4].try_into().expect("four bytes"));
    let high = u32::from_le_bytes(bytes[4..].try_into().expect("four bytes"));
    usize::try_from(low ^ high).expect("u32 fits usize")
}
