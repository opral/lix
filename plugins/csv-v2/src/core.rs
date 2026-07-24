use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Write as _;
use std::sync::Arc;

pub const TABLE_SCHEMA_KEY: &str = "csv_v2_table";
pub const ROW_SCHEMA_KEY: &str = "csv_v2_row";
pub const ROOT_ENTITY_PK: &str = "root";

const ROWS_PER_CHUNK: usize = 512;
const IDENTITIES_PER_CHUNK: usize = 64;
const QUOTED_FIELD: u32 = 1 << 31;
const FIELD_LENGTH_MASK: u32 = !QUOTED_FIELD;

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
pub enum Terminator {
    Lf,
    CrLf,
    Cr,
}

impl Terminator {
    fn bytes(self) -> &'static [u8] {
        match self {
            Self::Lf => b"\n",
            Self::CrLf => b"\r\n",
            Self::Cr => b"\r",
        }
    }

    fn snapshot(self) -> &'static str {
        match self {
            Self::Lf => "\n",
            Self::CrLf => "\r\n",
            Self::Cr => "\r",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Dialect {
    pub delimiter: u8,
    pub quote: Option<u8>,
    pub terminator: Terminator,
}

impl Dialect {
    pub fn for_path(path: Option<&str>) -> Self {
        let delimiter = if path.is_some_and(|path| path.as_bytes().ends_with(b".tsv")) {
            b'\t'
        } else {
            b','
        };
        Self {
            delimiter,
            quote: Some(b'"'),
            terminator: Terminator::Lf,
        }
    }

    fn validate_entity(self) -> Result<Self, String> {
        let safe_delimiter = self.delimiter == b'\t' || matches!(self.delimiter, b' '..=b'~');
        if !safe_delimiter {
            return Err(
                "CSV delimiter must be one safe ASCII byte (tab or printable ASCII)".to_owned(),
            );
        }
        if let Some(quote) = self.quote {
            if !matches!(quote, b'!'..=b'~') {
                return Err("CSV quote must be one printable non-space ASCII byte".to_owned());
            }
            if quote == self.delimiter {
                return Err("CSV delimiter and quote must differ".to_owned());
            }
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RowLayout {
    /// Base64url bitset, decoded into one bit per field. A set bit forces
    /// quoting even when the field's decoded value does not require it.
    force_quote: Vec<u8>,
    /// `None` inherits the table terminator, `Some(None)` is an unterminated
    /// row, and `Some(Some(_))` selects an exceptional row terminator.
    terminator: Option<Option<Terminator>>,
}

impl RowLayout {
    fn is_default(&self) -> bool {
        self.force_quote.is_empty() && self.terminator.is_none()
    }

    fn force_quotes(&self, field: usize) -> bool {
        self.force_quote
            .get(field / 8)
            .is_some_and(|byte| byte & (1 << (field % 8)) != 0)
    }

    fn ending(&self, dialect: Dialect) -> Option<Terminator> {
        self.terminator.unwrap_or(Some(dialect.terminator))
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

/// Immutable piece table for accepted CSV bytes. A localized successor keeps
/// references to the accepted backing allocation and owns only inserted
/// bytes; it never copies the unchanged prefix and suffix into a second full
/// guest blob.
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
    fn from_vec(bytes: Vec<u8>) -> Result<Self, String> {
        Self::from_shared(Arc::new(bytes))
    }

    fn from_shared(bytes: Arc<Vec<u8>>) -> Result<Self, String> {
        let len = u32::try_from(bytes.len())
            .map_err(|_| "CSV v2 currently supports files smaller than 4GiB".to_owned())?;
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
            .expect("the complete persistent blob range is valid");
        output
    }

    fn range(&self, start: usize, end: usize) -> Result<Vec<u8>, String> {
        let start = u32::try_from(start).map_err(|_| "CSV range exceeds 4GiB".to_owned())?;
        let end = u32::try_from(end).map_err(|_| "CSV range exceeds 4GiB".to_owned())?;
        if start > end || end > self.len {
            return Err("CSV byte range is out of bounds".to_owned());
        }
        let mut output = Vec::with_capacity(usize::try_from(end - start).expect("u32 fits usize"));
        self.append_range(start, end, &mut output)?;
        Ok(output)
    }

    fn byte(&self, offset: usize) -> Option<u8> {
        let offset = u32::try_from(offset).ok()?;
        if offset >= self.len {
            return None;
        }
        let mut logical_start = 0u32;
        for piece in self.pieces.iter() {
            let logical_end = logical_start + piece.len;
            if offset < logical_end {
                let index = piece.start + (offset - logical_start);
                return Some(piece.bytes[usize::try_from(index).expect("u32 fits usize")]);
            }
            logical_start = logical_end;
        }
        None
    }

    fn splice(&self, splices: &[InputSplice<'_>]) -> Result<Self, String> {
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
            .ok_or_else(|| "reconstructed CSV size overflow".to_owned())?;
        let result_len = u32::try_from(result_len)
            .map_err(|_| "CSV v2 currently supports files smaller than 4GiB".to_owned())?;
        let mut pieces = Vec::with_capacity(self.pieces.len() + splices.len() * 2);
        let mut cursor = 0u32;
        for splice in splices {
            let start = u32::try_from(splice.offset)
                .map_err(|_| "splice offset exceeds 4GiB".to_owned())?;
            let end = u32::try_from(splice.offset + splice.delete_len)
                .map_err(|_| "splice end exceeds 4GiB".to_owned())?;
            self.append_piece_range(cursor, start, &mut pieces)?;
            if !splice.insert.is_empty() {
                let bytes = Arc::new(splice.insert.to_vec());
                push_blob_piece(
                    &mut pieces,
                    BlobPiece {
                        start: 0,
                        len: u32::try_from(bytes.len())
                            .map_err(|_| "CSV insert exceeds 4GiB".to_owned())?,
                        bytes,
                    },
                );
            }
            cursor = end;
        }
        self.append_piece_range(cursor, self.len, &mut pieces)?;
        debug_assert_eq!(
            pieces.iter().map(|piece| u64::from(piece.len)).sum::<u64>(),
            u64::from(result_len)
        );
        Ok(Self {
            pieces: Arc::new(pieces),
            len: result_len,
        })
    }

    fn append_range(&self, start: u32, end: u32, output: &mut Vec<u8>) -> Result<(), String> {
        if start > end || end > self.len {
            return Err("CSV byte range is out of bounds".to_owned());
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
            return Err("CSV byte range is out of bounds".to_owned());
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

impl EntityChange {
    fn upsert(schema_key: &str, entity_pk: String, snapshot: Vec<u8>) -> Self {
        Self::upsert_with_effect(schema_key, entity_pk, snapshot, ChangeEffect::Content)
    }

    fn upsert_with_effect(
        schema_key: &str,
        entity_pk: String,
        snapshot: Vec<u8>,
        effect: ChangeEffect,
    ) -> Self {
        Self {
            schema_key: schema_key.to_owned(),
            entity_pk: vec![entity_pk],
            snapshot: Some(snapshot),
            effect,
        }
    }

    fn delete(schema_key: &str, entity_pk: String) -> Self {
        Self {
            schema_key: schema_key.to_owned(),
            entity_pk: vec![entity_pk],
            snapshot: None,
            effect: ChangeEffect::Content,
        }
    }
}

#[derive(Clone, Debug)]
struct IdentityStore {
    namespaces: Arc<Vec<[u8; 16]>>,
    /// `slot = base + ordinal` for namespaces whose imported/generated IDs
    /// remain dense. The common 220k import therefore resolves a row ID in
    /// constant time without a document-wide string scan.
    dense_slot_bases: Arc<Vec<Option<u32>>>,
    base_namespace_indices: Arc<Vec<u16>>,
    base_ordinals: Arc<Vec<u64>>,
    base_noncompact_bytes: Arc<Vec<u8>>,
    base_noncompact_ranges: Arc<Vec<IdentityRange>>,
    /// Immutable import-sized storage. Sparse successors append into small
    /// copy-on-write chunks instead of cloning every imported identity.
    appended: Arc<Vec<Arc<IdentityChunk>>>,
    /// Compact open-addressed lookup for imported noncompact/non-dense IDs.
    base_lookup: Arc<IdentityLookup>,
    /// Persistent hash trie for successor-only IDs. Updating it clones one
    /// path, not an import-sized hash map.
    appended_lookup: Option<Arc<IdentityLookupNode>>,
}

#[derive(Clone, Debug)]
struct IdentityChunk {
    entries: Box<[StoredIdentity]>,
}

#[derive(Clone, Debug)]
enum StoredIdentity {
    Generated { namespace_index: u16, ordinal: u64 },
    NonCompact(Arc<str>),
}

#[derive(Clone, Debug, Default)]
struct IdentityLookup {
    slots: Box<[u32]>,
}

#[derive(Clone, Debug, Default)]
struct IdentityLookupNode {
    zero: Option<Arc<Self>>,
    one: Option<Arc<Self>>,
    slots: Box<[u32]>,
}

#[derive(Clone, Copy, Debug, Default)]
struct IdentityRange {
    start: u32,
    len: u32,
}

impl IdentityStore {
    fn initial(namespace: IdNamespace, count: usize) -> Result<Self, String> {
        let count_u32 = u32::try_from(count).map_err(|_| "CSV has too many rows".to_owned())?;
        let namespace_indices = vec![0; count];
        let ordinals = (0..u64::from(count_u32)).collect();
        Ok(Self {
            namespaces: Arc::new(vec![namespace.0]),
            dense_slot_bases: Arc::new(vec![Some(0)]),
            base_namespace_indices: Arc::new(namespace_indices),
            base_ordinals: Arc::new(ordinals),
            base_noncompact_bytes: Arc::new(Vec::new()),
            base_noncompact_ranges: Arc::new(vec![IdentityRange::default(); count]),
            appended: Arc::new(Vec::new()),
            base_lookup: Arc::new(IdentityLookup::default()),
            appended_lookup: None,
        })
    }

    fn from_noncompact(bytes: Vec<u8>, ranges: Vec<IdentityRange>) -> Result<Self, String> {
        let count = ranges.len();
        let mut namespaces = Vec::<[u8; 16]>::new();
        let mut namespace_lookup = HashMap::<[u8; 16], u16>::new();
        let mut namespace_counts = Vec::<u32>::new();
        let mut dense_slot_bases = Vec::<Option<u32>>::new();
        let mut namespace_indices = Vec::with_capacity(count);
        let mut ordinals = Vec::with_capacity(count);
        let mut noncompact_bytes = Vec::new();
        let mut noncompact_ranges = Vec::with_capacity(count);
        for (slot, range) in ranges.into_iter().enumerate() {
            let start = usize::try_from(range.start).expect("u32 fits usize");
            let end = start + usize::try_from(range.len).expect("u32 fits usize");
            let id = &bytes[start..end];
            if let Some((namespace, ordinal)) = decode_generated_id(id) {
                let namespace_index = if let Some(index) = namespace_lookup.get(&namespace) {
                    usize::from(*index)
                } else {
                    let index = u16::try_from(namespaces.len())
                        .map_err(|_| "CSV import has too many ID namespaces".to_owned())?;
                    namespace_lookup.insert(namespace, index);
                    namespaces.push(namespace);
                    dense_slot_bases.push(None);
                    namespace_counts.push(0);
                    usize::from(index)
                };
                let slot = u32::try_from(slot).expect("validated row count");
                let candidate_base = u32::try_from(ordinal)
                    .ok()
                    .and_then(|ordinal| slot.checked_sub(ordinal));
                dense_slot_bases[namespace_index] = match dense_slot_bases[namespace_index] {
                    _ if namespace_counts[namespace_index] == 0 => candidate_base,
                    Some(base) if candidate_base == Some(base) => Some(base),
                    _ => None,
                };
                namespace_counts[namespace_index] = namespace_counts[namespace_index]
                    .checked_add(1)
                    .ok_or_else(|| "CSV namespace row count overflowed".to_owned())?;
                namespace_indices.push(
                    u16::try_from(namespace_index)
                        .map_err(|_| "CSV import has too many ID namespaces".to_owned())?,
                );
                ordinals.push(ordinal);
                noncompact_ranges.push(IdentityRange::default());
            } else {
                let noncompact_start = u32::try_from(noncompact_bytes.len())
                    .expect("validated noncompact identity bytes fit u32");
                noncompact_bytes.extend_from_slice(id);
                namespace_indices.push(u16::MAX);
                ordinals.push(0);
                noncompact_ranges.push(IdentityRange {
                    start: noncompact_start,
                    len: u32::try_from(id.len()).expect("validated identity length fits u32"),
                });
            }
        }
        let mut store = Self {
            namespaces: Arc::new(namespaces),
            dense_slot_bases: Arc::new(dense_slot_bases),
            base_namespace_indices: Arc::new(namespace_indices),
            base_ordinals: Arc::new(ordinals),
            base_noncompact_bytes: Arc::new(noncompact_bytes),
            base_noncompact_ranges: Arc::new(noncompact_ranges),
            appended: Arc::new(Vec::new()),
            base_lookup: Arc::new(IdentityLookup::default()),
            appended_lookup: None,
        };
        store.base_lookup = Arc::new(IdentityLookup::build(&store)?);
        Ok(store)
    }

    fn id(&self, slot: u32) -> String {
        let index = usize::try_from(slot).expect("u32 fits usize");
        if index >= self.base_len() {
            return self.appended_identity(slot).to_string(self);
        }
        let noncompact = self.base_noncompact_ranges[index];
        if noncompact.len != 0 {
            let start = usize::try_from(noncompact.start).expect("u32 fits usize");
            let end = start + usize::try_from(noncompact.len).expect("u32 fits usize");
            return std::str::from_utf8(&self.base_noncompact_bytes[start..end])
                .expect("noncompact entity IDs were validated as UTF-8")
                .to_owned();
        }
        let namespace_index = usize::from(self.base_namespace_indices[index]);
        IdNamespace(self.namespaces[namespace_index]).encode(self.base_ordinals[index])
    }

    fn slot_for_id(&self, id: &str) -> Option<u32> {
        if let Some((namespace, ordinal)) = decode_generated_id(id.as_bytes())
            && let Some(namespace_index) = self
                .namespaces
                .iter()
                .position(|candidate| candidate == &namespace)
        {
            let namespace_index_u16 = u16::try_from(namespace_index).ok()?;
            if let Some(base) = self.dense_slot_bases[namespace_index]
                && let Ok(ordinal) = u32::try_from(ordinal)
                && let Some(slot) = base.checked_add(ordinal)
            {
                let index = usize::try_from(slot).ok()?;
                if self.base_namespace_indices.get(index).copied() == Some(namespace_index_u16)
                    && self.base_ordinals.get(index).copied() == Some(u64::from(ordinal))
                {
                    return Some(slot);
                }
            }
        }
        let hash = identity_hash(id.as_bytes());
        identity_lookup_node(self.appended_lookup.as_deref(), hash)
            .and_then(|slots| {
                slots
                    .iter()
                    .rev()
                    .copied()
                    .find(|slot| self.id_eq(*slot, id.as_bytes()))
            })
            .or_else(|| self.base_lookup.find(self, hash, id.as_bytes()))
    }

    fn append_generated(&mut self, namespace: IdNamespace, ordinal: u64) -> Result<u32, String> {
        if self.slot_for_id(&namespace.encode(ordinal)).is_some() {
            return Err("generated CSV row identity already exists".to_owned());
        }
        let namespace_index = if let Some(index) = self
            .namespaces
            .iter()
            .position(|candidate| candidate == &namespace.0)
        {
            u16::try_from(index).map_err(|_| "too many ID namespaces".to_owned())?
        } else {
            let index = u16::try_from(self.namespaces.len())
                .map_err(|_| "too many ID namespaces".to_owned())?;
            Arc::make_mut(&mut self.namespaces).push(namespace.0);
            Arc::make_mut(&mut self.dense_slot_bases).push(None);
            index
        };
        let slot = self.len_u32()?;
        // Dense bases describe immutable import storage only. Appended IDs
        // live in the persistent trie even when they share a namespace, so a
        // sparse ordinal can never invalidate O(1) lookup of imported rows.
        self.append_identity(StoredIdentity::Generated {
            namespace_index,
            ordinal,
        })?;
        Ok(slot)
    }

    fn append_id(&mut self, id: &str) -> Result<u32, String> {
        if self.slot_for_id(id).is_some() {
            return Err("CSV row identity already exists".to_owned());
        }
        let slot = self.len_u32()?;
        let identity = if let Some((namespace, ordinal)) = decode_generated_id(id.as_bytes()) {
            let namespace_index = if let Some(index) = self
                .namespaces
                .iter()
                .position(|candidate| candidate == &namespace)
            {
                u16::try_from(index).map_err(|_| "too many ID namespaces".to_owned())?
            } else {
                let index = u16::try_from(self.namespaces.len())
                    .map_err(|_| "too many ID namespaces".to_owned())?;
                Arc::make_mut(&mut self.namespaces).push(namespace);
                Arc::make_mut(&mut self.dense_slot_bases).push(None);
                index
            };
            StoredIdentity::Generated {
                namespace_index,
                ordinal,
            }
        } else {
            StoredIdentity::NonCompact(Arc::from(id))
        };
        self.append_identity(identity)?;
        Ok(slot)
    }

    fn append_identity(&mut self, identity: StoredIdentity) -> Result<(), String> {
        let slot = self.len_u32()?;
        let hash = identity_hash_for_stored(self, &identity);
        let chunks = Arc::make_mut(&mut self.appended);
        if let Some(last) = chunks.last_mut()
            && last.entries.len() < IDENTITIES_PER_CHUNK
        {
            let mut entries = last.entries.to_vec();
            entries.push(identity);
            *last = Arc::new(IdentityChunk {
                entries: entries.into_boxed_slice(),
            });
        } else {
            chunks.push(Arc::new(IdentityChunk {
                entries: vec![identity].into_boxed_slice(),
            }));
        }
        self.appended_lookup = Some(identity_lookup_insert(
            self.appended_lookup.as_ref(),
            hash,
            slot,
            0,
        ));
        Ok(())
    }

    fn base_len(&self) -> usize {
        self.base_ordinals.len()
    }

    fn len(&self) -> usize {
        self.base_len()
            + self
                .appended
                .iter()
                .map(|chunk| chunk.entries.len())
                .sum::<usize>()
    }

    fn len_u32(&self) -> Result<u32, String> {
        u32::try_from(self.len()).map_err(|_| "CSV has too many row identities".to_owned())
    }

    fn appended_identity(&self, slot: u32) -> &StoredIdentity {
        let offset = usize::try_from(slot).expect("u32 fits usize") - self.base_len();
        &self.appended[offset / IDENTITIES_PER_CHUNK].entries[offset % IDENTITIES_PER_CHUNK]
    }

    fn id_eq(&self, slot: u32, id: &[u8]) -> bool {
        let index = usize::try_from(slot).expect("u32 fits usize");
        if index >= self.base_len() {
            return self.appended_identity(slot).eq_bytes(self, id);
        }
        let range = self.base_noncompact_ranges[index];
        if range.len != 0 {
            let start = usize::try_from(range.start).expect("u32 fits usize");
            let end = start + usize::try_from(range.len).expect("u32 fits usize");
            return &self.base_noncompact_bytes[start..end] == id;
        }
        decode_generated_id(id).is_some_and(|(namespace, ordinal)| {
            self.namespaces[usize::from(self.base_namespace_indices[index])] == namespace
                && self.base_ordinals[index] == ordinal
        })
    }

    fn estimated_bytes(&self) -> usize {
        self.namespaces.len() * 16
            + self.dense_slot_bases.len() * size_of::<Option<u32>>()
            + self.base_namespace_indices.len() * size_of::<u16>()
            + self.base_ordinals.len() * size_of::<u64>()
            + self.base_noncompact_bytes.len()
            + self.base_noncompact_ranges.len() * size_of::<IdentityRange>()
            + self.base_lookup.slots.len() * size_of::<u32>()
            + self
                .appended
                .iter()
                .map(|chunk| {
                    chunk.entries.len() * size_of::<StoredIdentity>()
                        + chunk
                            .entries
                            .iter()
                            .map(|identity| match identity {
                                StoredIdentity::NonCompact(value) => value.len(),
                                StoredIdentity::Generated { .. } => 0,
                            })
                            .sum::<usize>()
                })
                .sum::<usize>()
    }
}

impl StoredIdentity {
    fn to_string(&self, store: &IdentityStore) -> String {
        match self {
            Self::Generated {
                namespace_index,
                ordinal,
            } => IdNamespace(store.namespaces[usize::from(*namespace_index)]).encode(*ordinal),
            Self::NonCompact(value) => value.to_string(),
        }
    }

    fn eq_bytes(&self, store: &IdentityStore, id: &[u8]) -> bool {
        match self {
            Self::Generated {
                namespace_index,
                ordinal,
            } => decode_generated_id(id).is_some_and(|(namespace, candidate)| {
                store.namespaces[usize::from(*namespace_index)] == namespace
                    && *ordinal == candidate
            }),
            Self::NonCompact(value) => value.as_bytes() == id,
        }
    }
}

impl IdentityLookup {
    fn build(store: &IdentityStore) -> Result<Self, String> {
        let indexed = (0..store.base_len())
            .filter(|index| {
                let noncompact = store.base_noncompact_ranges[*index];
                noncompact.len != 0
                    || store.dense_slot_bases[usize::from(store.base_namespace_indices[*index])]
                        .is_none()
            })
            .collect::<Vec<_>>();
        if indexed.is_empty() {
            return Ok(Self::default());
        }
        let capacity = indexed.len().saturating_mul(2).next_power_of_two().max(2);
        let mut slots = vec![u32::MAX; capacity].into_boxed_slice();
        for index in indexed {
            let slot =
                u32::try_from(index).map_err(|_| "CSV has too many row identities".to_owned())?;
            let hash = store.base_identity_hash(index);
            let mut bucket =
                usize::try_from(hash & (capacity as u64 - 1)).expect("hash bucket fits usize");
            loop {
                let existing = slots[bucket];
                if existing == u32::MAX {
                    slots[bucket] = slot;
                    break;
                }
                if store.identities_equal(existing, slot) {
                    return Err("CSV row identities must be unique".to_owned());
                }
                bucket = (bucket + 1) & (capacity - 1);
            }
        }
        Ok(Self { slots })
    }

    fn find(&self, store: &IdentityStore, hash: u64, id: &[u8]) -> Option<u32> {
        if self.slots.is_empty() {
            return None;
        }
        let mask = self.slots.len() - 1;
        let mut bucket = usize::try_from(hash & mask as u64).ok()?;
        loop {
            let slot = self.slots[bucket];
            if slot == u32::MAX {
                return None;
            }
            if store.id_eq(slot, id) {
                return Some(slot);
            }
            bucket = (bucket + 1) & mask;
        }
    }
}

impl IdentityStore {
    fn base_identity_hash(&self, index: usize) -> u64 {
        let range = self.base_noncompact_ranges[index];
        if range.len != 0 {
            let start = usize::try_from(range.start).expect("u32 fits usize");
            let end = start + usize::try_from(range.len).expect("u32 fits usize");
            identity_hash(&self.base_noncompact_bytes[start..end])
        } else {
            generated_identity_hash(
                self.namespaces[usize::from(self.base_namespace_indices[index])],
                self.base_ordinals[index],
            )
        }
    }

    fn identities_equal(&self, left: u32, right: u32) -> bool {
        let left_index = usize::try_from(left).expect("u32 fits usize");
        let right_index = usize::try_from(right).expect("u32 fits usize");
        let left_range = self.base_noncompact_ranges[left_index];
        let right_range = self.base_noncompact_ranges[right_index];
        if left_range.len != 0 && right_range.len != 0 {
            let left_start = usize::try_from(left_range.start).expect("u32 fits usize");
            let right_start = usize::try_from(right_range.start).expect("u32 fits usize");
            return self.base_noncompact_bytes[left_start
                ..left_start + usize::try_from(left_range.len).expect("u32 fits usize")]
                == self.base_noncompact_bytes[right_start
                    ..right_start + usize::try_from(right_range.len).expect("u32 fits usize")];
        }
        if left_range.len == 0 && right_range.len == 0 {
            return self.base_namespace_indices[left_index]
                == self.base_namespace_indices[right_index]
                && self.base_ordinals[left_index] == self.base_ordinals[right_index];
        }
        false
    }
}

fn identity_hash(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}

fn generated_identity_hash(namespace: [u8; 16], ordinal: u64) -> u64 {
    let mut bytes = [0u8; 24];
    bytes[..16].copy_from_slice(&namespace);
    bytes[16..].copy_from_slice(&ordinal.to_be_bytes());
    identity_hash(&URL_SAFE_NO_PAD.encode(bytes).into_bytes())
}

fn identity_hash_for_stored(store: &IdentityStore, identity: &StoredIdentity) -> u64 {
    match identity {
        StoredIdentity::Generated {
            namespace_index,
            ordinal,
        } => generated_identity_hash(store.namespaces[usize::from(*namespace_index)], *ordinal),
        StoredIdentity::NonCompact(value) => identity_hash(value.as_bytes()),
    }
}

fn identity_lookup_node(node: Option<&IdentityLookupNode>, hash: u64) -> Option<&[u32]> {
    let mut node = node?;
    for depth in 0..64 {
        node = if hash & (1u64 << (63 - depth)) == 0 {
            node.zero.as_deref()?
        } else {
            node.one.as_deref()?
        };
    }
    Some(&node.slots)
}

fn identity_lookup_insert(
    node: Option<&Arc<IdentityLookupNode>>,
    hash: u64,
    slot: u32,
    depth: u32,
) -> Arc<IdentityLookupNode> {
    let mut output = node.map_or_else(IdentityLookupNode::default, |value| (**value).clone());
    if depth == 64 {
        let mut slots = output.slots.into_vec();
        slots.push(slot);
        output.slots = slots.into_boxed_slice();
        return Arc::new(output);
    }
    let child = if hash & (1u64 << (63 - depth)) == 0 {
        &mut output.zero
    } else {
        &mut output.one
    };
    *child = Some(identity_lookup_insert(
        child.as_ref(),
        hash,
        slot,
        depth + 1,
    ));
    Arc::new(output)
}

fn decode_generated_id(id: &[u8]) -> Option<([u8; 16], u64)> {
    if id.len() != 32 {
        return None;
    }
    let mut decoded = [0u8; 24];
    if URL_SAFE_NO_PAD.decode_slice(id, &mut decoded).ok()? != decoded.len() {
        return None;
    }
    let namespace = decoded[..16].try_into().ok()?;
    let ordinal = u64::from_be_bytes(decoded[16..].try_into().ok()?);
    Some((namespace, ordinal))
}

#[derive(Clone, Copy, Debug)]
struct FieldRange {
    start: u32,
    length_and_flags: u32,
}

impl FieldRange {
    fn new(start: usize, length: usize, quoted: bool) -> Result<Self, String> {
        let start = u32::try_from(start).map_err(|_| "CSV field offset exceeds 4GiB".to_owned())?;
        let length = u32::try_from(length).map_err(|_| "CSV field exceeds 2GiB".to_owned())?;
        if length > FIELD_LENGTH_MASK {
            return Err("CSV field exceeds compact index limit".to_owned());
        }
        Ok(Self {
            start,
            length_and_flags: length | if quoted { QUOTED_FIELD } else { 0 },
        })
    }

    fn length(self) -> u32 {
        self.length_and_flags & FIELD_LENGTH_MASK
    }

    fn quoted(self) -> bool {
        self.length_and_flags & QUOTED_FIELD != 0
    }
}

#[derive(Clone, Copy, Debug)]
struct CompactRow {
    relative_start: u32,
    byte_len: u32,
    first_field: u32,
    field_count: u16,
    ending: u8,
    id_slot: u32,
    order_rank: u64,
}

impl CompactRow {
    fn ending(self) -> Option<Terminator> {
        match self.ending {
            0 => None,
            1 => Some(Terminator::Lf),
            2 => Some(Terminator::CrLf),
            3 => Some(Terminator::Cr),
            _ => unreachable!("validated compact terminator"),
        }
    }
}

#[derive(Clone, Debug)]
struct RowChunk {
    rows: Box<[CompactRow]>,
    fields: Box<[FieldRange]>,
}

#[derive(Clone, Debug)]
struct ChunkRef {
    key: u32,
    byte_start: u32,
    data: Arc<RowChunk>,
}

#[derive(Clone, Debug)]
struct RowIndex {
    chunks: Arc<Vec<ChunkRef>>,
    row_starts: Arc<Vec<u32>>,
    chunk_positions: Arc<HashMap<u32, usize>>,
    slot_locations: SlotLocationIndex,
    row_count: u32,
    field_count: u32,
    next_chunk_key: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PackedRowLocation {
    chunk_key: u32,
    row: u16,
}

const MISSING_ROW_LOCATION: PackedRowLocation = PackedRowLocation {
    chunk_key: u32::MAX,
    row: u16::MAX,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SlotLocationOverride {
    Present(PackedRowLocation),
    Removed,
}

#[derive(Clone, Debug)]
struct SlotLocationOverlay {
    previous: Option<Arc<Self>>,
    entries: Box<[(u32, SlotLocationOverride)]>,
}

#[derive(Clone, Debug, Default)]
struct SlotLocationIndex {
    base: Arc<Vec<PackedRowLocation>>,
    overlay: Option<Arc<SlotLocationOverlay>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RowLocation {
    chunk: usize,
    row: usize,
}

#[derive(Clone, Debug)]
struct RowDraft {
    start: u32,
    byte_len: u32,
    ending: Option<Terminator>,
    fields: Vec<FieldRange>,
    id_slot: Option<u32>,
    order_rank: Option<u64>,
}

impl RowIndex {
    fn from_drafts(drafts: Vec<RowDraft>) -> Result<Self, String> {
        let row_count =
            u32::try_from(drafts.len()).map_err(|_| "CSV has too many rows".to_owned())?;
        let field_count_usize = drafts.iter().map(|row| row.fields.len()).sum::<usize>();
        let field_count =
            u32::try_from(field_count_usize).map_err(|_| "CSV has too many fields".to_owned())?;
        let mut next_chunk_key = 0;
        let chunks = build_chunks(drafts, &mut next_chunk_key)?;
        Self::from_initial_chunks(chunks, row_count, field_count, next_chunk_key)
    }

    fn from_initial_chunks(
        chunks: Vec<ChunkRef>,
        row_count: u32,
        field_count: u32,
        next_chunk_key: u32,
    ) -> Result<Self, String> {
        let max_slot = chunks
            .iter()
            .flat_map(|chunk| chunk.data.rows.iter())
            .map(|row| row.id_slot)
            .max()
            .map_or(0usize, |slot| {
                usize::try_from(slot).expect("u32 fits usize") + 1
            });
        let mut base = vec![MISSING_ROW_LOCATION; max_slot];
        for chunk in &chunks {
            for (row, value) in chunk.data.rows.iter().enumerate() {
                let row = u16::try_from(row).expect("row chunks contain at most 512 rows");
                let location = PackedRowLocation {
                    chunk_key: chunk.key,
                    row,
                };
                let slot = usize::try_from(value.id_slot).expect("u32 fits usize");
                if base[slot] != MISSING_ROW_LOCATION {
                    return Err("CSV row identity appears more than once".to_owned());
                }
                base[slot] = location;
            }
        }
        let (row_starts, chunk_positions) = index_chunk_metadata(&chunks)?;
        Ok(Self {
            chunks: Arc::new(chunks),
            row_starts: Arc::new(row_starts),
            chunk_positions: Arc::new(chunk_positions),
            slot_locations: SlotLocationIndex {
                base: Arc::new(base),
                overlay: None,
            },
            row_count,
            field_count,
            next_chunk_key,
        })
    }

    fn row(&self, location: RowLocation) -> (&ChunkRef, &CompactRow) {
        let chunk = &self.chunks[location.chunk];
        (chunk, &chunk.data.rows[location.row])
    }

    fn locations(&self) -> impl Iterator<Item = RowLocation> + '_ {
        self.chunks.iter().enumerate().flat_map(|(chunk, value)| {
            (0..value.data.rows.len()).map(move |row| RowLocation { chunk, row })
        })
    }

    fn ordinal_location(&self, ordinal: usize) -> Option<RowLocation> {
        if ordinal >= usize::try_from(self.row_count).expect("u32 fits usize") {
            return None;
        }
        let ordinal = u32::try_from(ordinal).ok()?;
        let upper = self.row_starts.partition_point(|start| *start <= ordinal);
        let chunk = upper.checked_sub(1)?;
        Some(RowLocation {
            chunk,
            row: usize::try_from(ordinal - self.row_starts[chunk]).expect("u32 fits usize"),
        })
    }

    fn ordinal_of(&self, location: RowLocation) -> usize {
        usize::try_from(self.row_starts[location.chunk]).expect("u32 fits usize") + location.row
    }

    fn row_start(&self, location: RowLocation) -> u32 {
        let (chunk, row) = self.row(location);
        chunk.byte_start + row.relative_start
    }

    fn row_end(&self, location: RowLocation) -> u32 {
        let (chunk, row) = self.row(location);
        chunk.byte_start + row.relative_start + row.byte_len
    }

    fn location_for_offset(
        &self,
        offset: u32,
        prefer_previous_at_eof: bool,
    ) -> Option<RowLocation> {
        if self.row_count == 0 {
            return None;
        }
        let upper = self
            .chunks
            .partition_point(|chunk| chunk.byte_start <= offset);
        let mut chunk = upper.saturating_sub(1);
        if prefer_previous_at_eof && upper > 1 && self.chunks[chunk].byte_start == offset {
            chunk -= 1;
        }
        let chunk_ref = &self.chunks[chunk];
        let row = chunk_ref.data.rows.partition_point(|row| {
            let end = chunk_ref.byte_start + row.relative_start + row.byte_len;
            if prefer_previous_at_eof {
                end < offset
            } else {
                end <= offset
            }
        });
        if row < chunk_ref.data.rows.len() {
            Some(RowLocation { chunk, row })
        } else if chunk + 1 < self.chunks.len() {
            Some(RowLocation {
                chunk: chunk + 1,
                row: 0,
            })
        } else {
            self.ordinal_location(usize::try_from(self.row_count - 1).expect("u32 fits usize"))
        }
    }

    fn location_for_identity_slot(&self, slot: u32) -> Option<RowLocation> {
        let packed = self.slot_locations.get(slot)?;
        let chunk = *self.chunk_positions.get(&packed.chunk_key)?;
        let row = usize::from(packed.row);
        (self.chunks[chunk]
            .data
            .rows
            .get(row)
            .is_some_and(|value| value.id_slot == slot))
        .then_some(RowLocation { chunk, row })
    }

    fn estimated_bytes(&self) -> usize {
        self.chunks.len() * size_of::<ChunkRef>()
            + self.row_starts.len() * size_of::<u32>()
            + self.chunk_positions.len() * size_of::<(u32, usize)>()
            + self.slot_locations.estimated_bytes()
            + self
                .chunks
                .iter()
                .map(|chunk| {
                    chunk.data.rows.len() * size_of::<CompactRow>()
                        + chunk.data.fields.len() * size_of::<FieldRange>()
                })
                .sum::<usize>()
    }
}

impl SlotLocationIndex {
    fn get(&self, slot: u32) -> Option<PackedRowLocation> {
        let mut overlay = self.overlay.as_deref();
        while let Some(value) = overlay {
            if let Ok(index) = value.entries.binary_search_by_key(&slot, |entry| entry.0) {
                return match value.entries[index].1 {
                    SlotLocationOverride::Present(location) => Some(location),
                    SlotLocationOverride::Removed => None,
                };
            }
            overlay = value.previous.as_deref();
        }
        self.base
            .get(usize::try_from(slot).ok()?)
            .copied()
            .filter(|location| *location != MISSING_ROW_LOCATION)
    }

    fn with_changes(&self, entries: Vec<(u32, SlotLocationOverride)>) -> Self {
        let mut latest = HashMap::with_capacity(entries.len());
        for (slot, value) in entries {
            latest.insert(slot, value);
        }
        let mut entries = latest.into_iter().collect::<Vec<_>>();
        entries.sort_unstable_by_key(|entry| entry.0);
        if entries.is_empty() {
            return self.clone();
        }
        Self {
            base: Arc::clone(&self.base),
            overlay: Some(Arc::new(SlotLocationOverlay {
                previous: self.overlay.clone(),
                entries: entries.into_boxed_slice(),
            })),
        }
    }

    fn estimated_bytes(&self) -> usize {
        let mut bytes = self.base.len() * size_of::<PackedRowLocation>();
        let mut overlay = self.overlay.as_deref();
        while let Some(value) = overlay {
            bytes += value.entries.len() * size_of::<(u32, SlotLocationOverride)>();
            overlay = value.previous.as_deref();
        }
        bytes
    }
}

fn index_chunk_metadata(chunks: &[ChunkRef]) -> Result<(Vec<u32>, HashMap<u32, usize>), String> {
    let mut row_starts = Vec::with_capacity(chunks.len());
    let mut chunk_positions = HashMap::with_capacity(chunks.len());
    let mut ordinal = 0u32;
    for (index, chunk) in chunks.iter().enumerate() {
        row_starts.push(ordinal);
        if chunk_positions.insert(chunk.key, index).is_some() {
            return Err("CSV chunk key was reused".to_owned());
        }
        ordinal = ordinal
            .checked_add(u32::try_from(chunk.data.rows.len()).expect("chunk rows fit u32"))
            .ok_or_else(|| "CSV has too many rows".to_owned())?;
    }
    Ok((row_starts, chunk_positions))
}

fn next_chunk_key(next: &mut u32) -> Result<u32, String> {
    let key = *next;
    *next = next
        .checked_add(1)
        .ok_or_else(|| "CSV chunk key space exhausted".to_owned())?;
    Ok(key)
}

fn build_chunks(drafts: Vec<RowDraft>, next_key: &mut u32) -> Result<Vec<ChunkRef>, String> {
    let mut chunks = Vec::with_capacity(drafts.len().div_ceil(ROWS_PER_CHUNK));
    for draft_chunk in drafts.chunks(ROWS_PER_CHUNK) {
        let byte_start = draft_chunk.first().map_or(0, |row| row.start);
        let mut rows = Vec::with_capacity(draft_chunk.len());
        let field_len = draft_chunk.iter().map(|row| row.fields.len()).sum();
        let mut fields = Vec::with_capacity(field_len);
        for draft in draft_chunk {
            let first_field = u32::try_from(fields.len())
                .map_err(|_| "CSV chunk has too many fields".to_owned())?;
            let field_count = u16::try_from(draft.fields.len())
                .map_err(|_| "CSV row has more than 65535 fields".to_owned())?;
            fields.extend_from_slice(&draft.fields);
            rows.push(CompactRow {
                relative_start: draft.start - byte_start,
                byte_len: draft.byte_len,
                first_field,
                field_count,
                ending: match draft.ending {
                    None => 0,
                    Some(Terminator::Lf) => 1,
                    Some(Terminator::CrLf) => 2,
                    Some(Terminator::Cr) => 3,
                },
                id_slot: draft
                    .id_slot
                    .ok_or_else(|| "row identity was not assigned".to_owned())?,
                order_rank: draft
                    .order_rank
                    .ok_or_else(|| "row order was not assigned".to_owned())?,
            });
        }
        chunks.push(ChunkRef {
            key: next_chunk_key(next_key)?,
            byte_start,
            data: Arc::new(RowChunk {
                rows: rows.into_boxed_slice(),
                fields: fields.into_boxed_slice(),
            }),
        });
    }
    Ok(chunks)
}

#[derive(Clone, Debug)]
pub struct Document(Arc<DocumentInner>);

#[derive(Debug)]
struct DocumentInner {
    blob: PersistentBlob,
    index: RowIndex,
    identities: IdentityStore,
    order_overrides: OrderKeyStore,
    dialect: Dialect,
    /// Row metadata copied or re-indexed by the most recent sparse
    /// transition. This is deliberately independent of file row count.
    sparse_rows_touched: usize,
}

#[derive(Clone, Debug, Default)]
struct OrderKeyStore {
    base: Arc<HashMap<u32, Arc<str>>>,
    overlay: Option<Arc<OrderKeyOverlay>>,
}

#[derive(Clone, Debug)]
struct OrderKeyOverlay {
    previous: Option<Arc<Self>>,
    slot: u32,
    value: Option<Arc<str>>,
}

impl OrderKeyStore {
    fn from_base(base: HashMap<u32, Arc<str>>) -> Self {
        Self {
            base: Arc::new(base),
            overlay: None,
        }
    }

    fn get(&self, slot: u32) -> Option<&str> {
        let mut overlay = self.overlay.as_deref();
        while let Some(value) = overlay {
            if value.slot == slot {
                return value.value.as_deref();
            }
            overlay = value.previous.as_deref();
        }
        self.base.get(&slot).map(AsRef::as_ref)
    }

    fn with_key(&self, slot: u32, order_key: &str, order_rank: u64) -> Self {
        let canonical = format!("{order_rank:016x}");
        let value = (order_key != canonical).then(|| Arc::from(order_key));
        Self {
            base: Arc::clone(&self.base),
            overlay: Some(Arc::new(OrderKeyOverlay {
                previous: self.overlay.clone(),
                slot,
                value,
            })),
        }
    }

    fn estimated_bytes(&self) -> usize {
        let mut bytes = self
            .base
            .values()
            .map(|value| value.len() + size_of::<(u32, Arc<str>)>())
            .sum::<usize>();
        let mut overlay = self.overlay.as_deref();
        while let Some(value) = overlay {
            bytes +=
                size_of::<OrderKeyOverlay>() + value.value.as_ref().map_or(0, |value| value.len());
            overlay = value.previous.as_deref();
        }
        bytes
    }
}

#[derive(Clone, Copy, Debug)]
struct ImportRange {
    start: u32,
    len: u32,
}

impl ImportRange {
    fn bytes(self, arena: &[u8]) -> &[u8] {
        let start = usize::try_from(self.start).expect("u32 fits usize");
        let end = start + usize::try_from(self.len).expect("u32 fits usize");
        &arena[start..end]
    }

    fn identity(self) -> IdentityRange {
        IdentityRange {
            start: self.start,
            len: self.len,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct ImportedRow {
    id: ImportRange,
    order_key: ImportRange,
    cell_start: u32,
    cell_count: u16,
}

#[derive(Clone, Debug)]
struct ImportedLayout {
    id: ImportRange,
    layout: RowLayout,
}

/// Incremental cold-start importer. Packet pages are decoded and compacted as
/// they arrive, so the guest never retains all packet-v1 JSON records or a
/// `Vec<String>` per row. The compact arenas are consumed directly when the
/// accepted renderer document is constructed.
#[derive(Debug)]
pub(crate) struct EntityImportBuilder {
    dialect: Dialect,
    table_root_seen: bool,
    rows: Vec<ImportedRow>,
    id_bytes: Vec<u8>,
    order_key_bytes: Vec<u8>,
    cell_bytes: Vec<u8>,
    /// Exceptional lexical facts are sparse and keyed by the row's immutable
    /// import-arena range. Canonical rows pay no per-row pointer or mask cost.
    layout_overrides: Vec<ImportedLayout>,
}

impl EntityImportBuilder {
    pub(crate) fn new() -> Self {
        Self {
            dialect: Dialect::for_path(None),
            table_root_seen: false,
            rows: Vec::new(),
            id_bytes: Vec::new(),
            order_key_bytes: Vec::new(),
            cell_bytes: Vec::new(),
            layout_overrides: Vec::new(),
        }
    }

    pub(crate) fn push(&mut self, entity: EntityRecord) -> Result<(), String> {
        if entity.entity_pk.len() != 1 {
            return Err("CSV entities require one primary-key component".to_owned());
        }
        match entity.schema_key.as_str() {
            TABLE_SCHEMA_KEY => {
                if entity.entity_pk[0] != ROOT_ENTITY_PK {
                    return Err("CSV table primary key must be root".to_owned());
                }
                if self.table_root_seen {
                    return Err("CSV cold state contains duplicate table root".to_owned());
                }
                self.dialect = parse_table_snapshot(&entity.snapshot)?;
                self.table_root_seen = true;
            }
            ROW_SCHEMA_KEY => {
                let row = parse_row_snapshot(&entity.snapshot)?;
                if row.id != entity.entity_pk[0] {
                    return Err("CSV row snapshot id does not match entity key".to_owned());
                }
                let id = append_import_bytes(&mut self.id_bytes, row.id.as_bytes())?;
                if !row.layout.is_default() {
                    self.layout_overrides.push(ImportedLayout {
                        id,
                        layout: row.layout.clone(),
                    });
                }
                let order_key =
                    append_import_bytes(&mut self.order_key_bytes, row.order_key.as_bytes())?;
                let cell_start = u32::try_from(self.cell_bytes.len())
                    .map_err(|_| "CSV imported cell bytes exceed 4GiB".to_owned())?;
                let cell_count = u16::try_from(row.cells.len())
                    .map_err(|_| "CSV row has more than 65535 fields".to_owned())?;
                for cell in row.cells {
                    let len = u32::try_from(cell.len())
                        .map_err(|_| "CSV imported cell exceeds 4GiB".to_owned())?;
                    reserve_import_bytes(&mut self.cell_bytes, 4 + cell.len())?;
                    self.cell_bytes.extend_from_slice(&len.to_le_bytes());
                    self.cell_bytes.extend_from_slice(cell.as_bytes());
                }
                if self.rows.len() == self.rows.capacity() {
                    self.rows
                        .try_reserve(4_096)
                        .map_err(|_| "CSV imported row index allocation failed".to_owned())?;
                }
                self.rows.push(ImportedRow {
                    id,
                    order_key,
                    cell_start,
                    cell_count,
                });
            }
            key => return Err(format!("unsupported CSV schema key {key}")),
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<(Document, ByteEdit), String> {
        if !self.table_root_seen {
            return Err("CSV cold state is missing the table root".to_owned());
        }
        // Amortized growth keeps page ingestion cheap; compact the four
        // long-lived import buffers before renderer/index construction so
        // spare capacity cannot consume the 64 MiB cold-open headroom.
        self.cell_bytes.shrink_to_fit();
        self.id_bytes.shrink_to_fit();
        self.order_key_bytes.shrink_to_fit();
        self.rows.shrink_to_fit();
        self.layout_overrides.shrink_to_fit();
        let id_bytes = &self.id_bytes;
        let order_key_bytes = &self.order_key_bytes;
        self.rows.sort_unstable_by(|left, right| {
            left.order_key
                .bytes(order_key_bytes)
                .cmp(right.order_key.bytes(order_key_bytes))
                .then_with(|| left.id.bytes(id_bytes).cmp(right.id.bytes(id_bytes)))
        });
        let layouts = self
            .layout_overrides
            .iter()
            .map(|value| ((value.id.start, value.id.len), &value.layout))
            .collect::<HashMap<_, _>>();
        for (index, row) in self.rows.iter().enumerate() {
            let ending = layouts
                .get(&(row.id.start, row.id.len))
                .map_or(Some(self.dialect.terminator), |layout| {
                    layout.ending(self.dialect)
                });
            if ending.is_none() && index + 1 != self.rows.len() {
                return Err("only the final CSV row may be unterminated".to_owned());
            }
        }

        let rendered_len = self.rows.iter().try_fold(0usize, |total, row| {
            let layout = layouts.get(&(row.id.start, row.id.len)).copied();
            let ending = layout.map_or(Some(self.dialect.terminator), |layout| {
                layout.ending(self.dialect)
            });
            let mut row_len = ending.map_or(0, |ending| ending.bytes().len());
            let mut cursor = usize::try_from(row.cell_start).expect("u32 fits usize");
            for index in 0..usize::from(row.cell_count) {
                let cell = read_import_cell(&self.cell_bytes, &mut cursor)?;
                if index > 0 {
                    row_len = row_len
                        .checked_add(1)
                        .ok_or_else(|| "CSV rendered length overflowed".to_owned())?;
                }
                row_len = row_len
                    .checked_add(rendered_cell_len(
                        cell,
                        self.dialect,
                        layout.is_some_and(|layout| layout.force_quotes(index)),
                    )?)
                    .ok_or_else(|| "CSV rendered length overflowed".to_owned())?;
            }
            total
                .checked_add(row_len)
                .ok_or_else(|| "CSV rendered length overflowed".to_owned())
        })?;
        if rendered_len > u32::MAX as usize {
            return Err("CSV v2 currently supports files smaller than 4GiB".to_owned());
        }

        let row_count =
            u32::try_from(self.rows.len()).map_err(|_| "CSV has too many rows".to_owned())?;
        let mut blob = Vec::with_capacity(rendered_len);
        let mut chunks = Vec::with_capacity(self.rows.len().div_ceil(ROWS_PER_CHUNK));
        let mut chunk_rows = Vec::with_capacity(ROWS_PER_CHUNK);
        let mut chunk_fields = Vec::new();
        let mut chunk_start = 0u32;
        let mut field_count = 0u32;
        let mut chunk_key_cursor = 0u32;
        let mut noncompact_ranges = Vec::with_capacity(self.rows.len());
        let mut order_overrides = HashMap::new();
        let denominator = u128::try_from(self.rows.len() + 1).expect("usize fits u128");

        for (index, row) in self.rows.iter().copied().enumerate() {
            if chunk_rows.len() == ROWS_PER_CHUNK {
                chunks.push(ChunkRef {
                    key: next_chunk_key(&mut chunk_key_cursor)?,
                    byte_start: chunk_start,
                    data: Arc::new(RowChunk {
                        rows: std::mem::take(&mut chunk_rows).into_boxed_slice(),
                        fields: std::mem::take(&mut chunk_fields).into_boxed_slice(),
                    }),
                });
                chunk_rows = Vec::with_capacity(ROWS_PER_CHUNK);
                chunk_start = u32::try_from(blob.len()).expect("validated rendered length");
            }

            let row_start = u32::try_from(blob.len()).expect("validated rendered length");
            let first_field = u32::try_from(chunk_fields.len())
                .map_err(|_| "CSV chunk has too many fields".to_owned())?;
            let mut cursor = usize::try_from(row.cell_start).expect("u32 fits usize");
            let layout = layouts.get(&(row.id.start, row.id.len)).copied();
            for cell_index in 0..usize::from(row.cell_count) {
                if cell_index > 0 {
                    blob.push(self.dialect.delimiter);
                }
                let field_start =
                    blob.len() - usize::try_from(row_start).expect("validated rendered offset");
                let cell = read_import_cell(&self.cell_bytes, &mut cursor)?;
                let quoted = render_import_cell(
                    &mut blob,
                    cell,
                    self.dialect,
                    layout.is_some_and(|layout| layout.force_quotes(cell_index)),
                )?;
                let field_len = blob.len()
                    - usize::try_from(row_start).expect("validated rendered offset")
                    - field_start;
                chunk_fields.push(FieldRange::new(field_start, field_len, quoted)?);
                field_count = field_count
                    .checked_add(1)
                    .ok_or_else(|| "CSV has too many fields".to_owned())?;
            }
            let ending = layout.map_or(Some(self.dialect.terminator), |layout| {
                layout.ending(self.dialect)
            });
            if let Some(ending) = ending {
                blob.extend_from_slice(ending.bytes());
            }
            let byte_len = u32::try_from(blob.len())
                .expect("validated rendered length")
                .checked_sub(row_start)
                .expect("row starts inside rendered bytes");
            let slot = u32::try_from(index).expect("validated row count");
            noncompact_ranges.push(row.id.identity());
            let numerator =
                u128::try_from(index + 1).expect("usize fits u128") * u128::from(u64::MAX);
            let order_rank = u64::try_from(numerator / denominator).expect("rank fits") | 1;
            let expected_order_key = format!("{order_rank:016x}");
            let supplied_order_key = row.order_key.bytes(&self.order_key_bytes);
            if supplied_order_key != expected_order_key.as_bytes() {
                let supplied_order_key = std::str::from_utf8(supplied_order_key)
                    .expect("order keys were validated as UTF-8");
                order_overrides.insert(slot, Arc::from(supplied_order_key));
            }
            chunk_rows.push(CompactRow {
                relative_start: row_start - chunk_start,
                byte_len,
                first_field,
                field_count: row.cell_count,
                ending: match ending {
                    None => 0,
                    Some(Terminator::Lf) => 1,
                    Some(Terminator::CrLf) => 2,
                    Some(Terminator::Cr) => 3,
                },
                id_slot: slot,
                order_rank,
            });
        }
        if !chunk_rows.is_empty() {
            chunks.push(ChunkRef {
                key: next_chunk_key(&mut chunk_key_cursor)?,
                byte_start: chunk_start,
                data: Arc::new(RowChunk {
                    rows: chunk_rows.into_boxed_slice(),
                    fields: chunk_fields.into_boxed_slice(),
                }),
            });
        }
        debug_assert_eq!(blob.len(), rendered_len);

        // Rendering and indexing no longer need the transient import arenas.
        // Release them before allocating the final identity side tables so
        // cold-open peak memory, not merely retained memory, stays bounded.
        let id_bytes = std::mem::take(&mut self.id_bytes);
        drop(std::mem::take(&mut self.rows));
        drop(std::mem::take(&mut self.order_key_bytes));
        drop(std::mem::take(&mut self.cell_bytes));
        drop(std::mem::take(&mut self.layout_overrides));
        let identities = IdentityStore::from_noncompact(id_bytes, noncompact_ranges)?;
        let blob = Arc::new(blob);
        let persistent_blob = PersistentBlob::from_shared(Arc::clone(&blob))?;
        let index =
            RowIndex::from_initial_chunks(chunks, row_count, field_count, chunk_key_cursor)?;
        let document = Document(Arc::new(DocumentInner {
            blob: persistent_blob,
            index,
            identities,
            order_overrides: OrderKeyStore::from_base(order_overrides),
            dialect: self.dialect,
            sparse_rows_touched: 0,
        }));
        let edit = ByteEdit {
            offset: 0,
            delete_len: 0,
            insert: blob,
        };
        Ok((document, edit))
    }
}

fn append_import_bytes(arena: &mut Vec<u8>, bytes: &[u8]) -> Result<ImportRange, String> {
    let start = u32::try_from(arena.len())
        .map_err(|_| "CSV imported string bytes exceed 4GiB".to_owned())?;
    let len =
        u32::try_from(bytes.len()).map_err(|_| "CSV imported string exceeds 4GiB".to_owned())?;
    reserve_import_bytes(arena, bytes.len())?;
    arena.extend_from_slice(bytes);
    Ok(ImportRange { start, len })
}

fn reserve_import_bytes(arena: &mut Vec<u8>, additional: usize) -> Result<(), String> {
    arena
        .try_reserve(additional)
        .map_err(|_| "CSV import arena allocation failed".to_owned())
}

fn read_import_cell<'a>(bytes: &'a [u8], cursor: &mut usize) -> Result<&'a [u8], String> {
    let len_bytes = bytes
        .get(*cursor..cursor.saturating_add(4))
        .ok_or_else(|| "CSV imported cell header is truncated".to_owned())?;
    let len = usize::try_from(u32::from_le_bytes(
        len_bytes.try_into().expect("four-byte cell length"),
    ))
    .expect("u32 fits usize");
    *cursor += 4;
    let cell = bytes
        .get(*cursor..cursor.saturating_add(len))
        .ok_or_else(|| "CSV imported cell is truncated".to_owned())?;
    *cursor += len;
    Ok(cell)
}

fn imported_cell_needs_quotes(cell: &[u8], dialect: Dialect) -> Result<bool, String> {
    let structural = cell
        .iter()
        .any(|byte| *byte == dialect.delimiter || *byte == b'\r' || *byte == b'\n');
    let Some(quote) = dialect.quote else {
        return if structural {
            Err(
                "CSV dialect without a quote cannot represent a delimiter or newline in a cell"
                    .to_owned(),
            )
        } else {
            Ok(false)
        };
    };
    Ok(structural || cell.contains(&quote))
}

fn rendered_cell_len(cell: &[u8], dialect: Dialect, force_quote: bool) -> Result<usize, String> {
    let canonical_quote = imported_cell_needs_quotes(cell, dialect)?;
    if force_quote && canonical_quote {
        return Err("CSV force_quote may select only otherwise-unnecessary quotes".to_owned());
    }
    if !force_quote && !canonical_quote {
        return Ok(cell.len());
    }
    let quote = dialect
        .quote
        .ok_or_else(|| "CSV dialect without a quote cannot force quoted fields".to_owned())?;
    let quote_count = cell
        .iter()
        .fold(0usize, |count, byte| count + usize::from(*byte == quote));
    cell.len()
        .checked_add(quote_count)
        .and_then(|len| len.checked_add(2))
        .ok_or_else(|| "CSV rendered cell length overflowed".to_owned())
}

fn render_import_cell(
    output: &mut Vec<u8>,
    cell: &[u8],
    dialect: Dialect,
    force_quote: bool,
) -> Result<bool, String> {
    let canonical_quote = imported_cell_needs_quotes(cell, dialect)?;
    if force_quote && canonical_quote {
        return Err("CSV force_quote may select only otherwise-unnecessary quotes".to_owned());
    }
    let quoted = force_quote || canonical_quote;
    if !quoted {
        output.extend_from_slice(cell);
        return Ok(false);
    }
    let quote = dialect
        .quote
        .ok_or_else(|| "CSV dialect without a quote cannot force quoted fields".to_owned())?;
    output.push(quote);
    for &byte in cell {
        output.push(byte);
        if byte == quote {
            output.push(quote);
        }
    }
    output.push(quote);
    Ok(true)
}

impl Document {
    pub fn open_file(
        bytes: Vec<u8>,
        path: Option<&str>,
        namespace: IdNamespace,
    ) -> Result<(Self, InitialChanges), String> {
        if bytes.len() > u32::MAX as usize {
            return Err("CSV v2 currently supports files smaller than 4GiB".to_owned());
        }
        std::str::from_utf8(&bytes).map_err(|error| format!("CSV must be UTF-8: {error}"))?;
        let mut dialect = Dialect::for_path(path);
        let mut drafts = scan_rows(&bytes, 0, bytes.len(), dialect)?;
        dialect.terminator = preferred_terminator(&drafts);
        let identities = IdentityStore::initial(namespace, drafts.len())?;
        assign_initial_rows(&mut drafts);
        let document = Self(Arc::new(DocumentInner {
            blob: PersistentBlob::from_vec(bytes)?,
            index: RowIndex::from_drafts(drafts)?,
            identities,
            order_overrides: OrderKeyStore::default(),
            dialect,
            sparse_rows_touched: 0,
        }));
        let changes = InitialChanges {
            document: document.clone(),
            row: 0,
            table_pending: true,
        };
        Ok((document, changes))
    }

    pub fn fork(&self) -> Self {
        self.clone()
    }

    pub fn initial_changes(&self) -> InitialChanges {
        InitialChanges {
            document: self.clone(),
            row: 0,
            table_pending: true,
        }
    }

    pub fn bytes(&self) -> Vec<u8> {
        self.0.blob.materialize()
    }

    pub fn row_count(&self) -> usize {
        usize::try_from(self.0.index.row_count).expect("u32 fits usize")
    }

    pub fn field_count(&self) -> usize {
        usize::try_from(self.0.index.field_count).expect("u32 fits usize")
    }

    pub fn retained_bytes_estimate(&self) -> usize {
        self.0.blob.retained_backing_bytes()
            + self.0.index.estimated_bytes()
            + self.0.identities.estimated_bytes()
            + self.0.order_overrides.estimated_bytes()
    }

    #[cfg(test)]
    pub(crate) fn shares_single_blob_with(&self, bytes: &Arc<Vec<u8>>) -> bool {
        self.0
            .blob
            .single_backing()
            .is_some_and(|backing| Arc::ptr_eq(backing, bytes))
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
    pub(crate) fn blob_piece_count(&self) -> usize {
        self.0.blob.pieces.len()
    }

    #[cfg(test)]
    pub(crate) fn sparse_rows_touched(&self) -> usize {
        self.0.sparse_rows_touched
    }

    pub fn dialect(&self) -> Dialect {
        self.0.dialect
    }

    pub fn file_changed(
        &self,
        splices: &[InputSplice<'_>],
        namespace: IdNamespace,
    ) -> Result<(Self, Vec<EntityChange>), String> {
        self.file_changed_with_paths(splices, None, None, namespace)
    }

    pub fn file_changed_with_paths(
        &self,
        splices: &[InputSplice<'_>],
        before_path: Option<&str>,
        after_path: Option<&str>,
        namespace: IdNamespace,
    ) -> Result<(Self, Vec<EntityChange>), String> {
        validate_splices(self.0.blob.len(), splices)?;
        let before_path_dialect = Dialect::for_path(before_path);
        let after_path_dialect = Dialect::for_path(after_path);
        let descriptor_dialect_changed = before_path_dialect.delimiter
            != after_path_dialect.delimiter
            || before_path_dialect.quote != after_path_dialect.quote;
        if splices.is_empty() && !descriptor_dialect_changed {
            return Ok((self.clone(), Vec::new()));
        }
        let after = if splices.is_empty() {
            self.0.blob.clone()
        } else {
            self.0.blob.splice(splices)?
        };
        if descriptor_dialect_changed {
            return self.reparse_after_descriptor_change(after, after_path, namespace);
        }

        let (first_old, last_old) = affected_row_window(&self.0.index, splices)?;
        let old_start = first_old.map_or(0, |location| self.0.index.row_start(location));
        let old_end = last_old.map_or(0, |location| self.0.index.row_end(location));
        let new_start = map_offset(old_start, splices, false)?;
        let mut new_end = map_offset(old_end, splices, true)?;
        if new_end < new_start || new_end > after.len() {
            return Err("invalid reconstructed CSV row window".to_owned());
        }

        let mut new_drafts = loop {
            let window = after.range(new_start, new_end)?;
            match scan_rows(&window, 0, window.len(), self.0.dialect) {
                Ok(mut rows) => {
                    for row in &mut rows {
                        row.start = row
                            .start
                            .checked_add(u32::try_from(new_start).expect("file offset fits u32"))
                            .ok_or_else(|| "CSV row offset overflow".to_owned())?;
                    }
                    break rows;
                }
                Err(error)
                    if new_end < after.len() && error.contains("unterminated quoted field") =>
                {
                    new_end = extend_to_next_record_boundary(&after, new_end);
                }
                Err(error) => return Err(error),
            }
        };
        let old_locations = collect_window_locations(&self.0.index, first_old, last_old);
        let mut identities = self.0.identities.clone();
        match_rows(
            self,
            &old_locations,
            &after,
            &mut new_drafts,
            self.0.dialect,
            &mut identities,
            namespace,
        )?;
        assign_missing_order_ranks(self, &old_locations, &mut new_drafts)?;

        let delta = i64::try_from(after.len()).expect("u32-sized file fits i64")
            - i64::try_from(self.0.blob.len()).expect("u32-sized file fits i64");
        let mut next_chunk_key = self.0.index.next_chunk_key;
        let replacement = build_chunks(new_drafts.clone(), &mut next_chunk_key)?;
        let (new_index, sparse_rows_touched) =
            replace_index_window(&self.0.index, first_old, last_old, replacement, delta)?;
        let document = Self(Arc::new(DocumentInner {
            blob: after,
            index: new_index,
            identities,
            order_overrides: self.0.order_overrides.clone(),
            // A local edit does not re-sniff global dialect metadata. Mixed
            // line endings stay attached to their indexed rows.
            dialect: self.0.dialect,
            sparse_rows_touched,
        }));
        let changes = changed_entities(self, &document, &old_locations, &new_drafts)?;
        Ok((document, changes))
    }

    fn reparse_after_descriptor_change(
        &self,
        after: PersistentBlob,
        after_path: Option<&str>,
        namespace: IdNamespace,
    ) -> Result<(Self, Vec<EntityChange>), String> {
        let bytes = after.materialize();
        std::str::from_utf8(&bytes).map_err(|error| format!("CSV must be UTF-8: {error}"))?;
        let mut dialect = Dialect::for_path(after_path);
        let mut drafts = scan_rows(&bytes, 0, bytes.len(), dialect)?;
        dialect.terminator = preferred_terminator(&drafts);
        let old_locations = self.0.index.locations().collect::<Vec<_>>();
        let mut identities = self.0.identities.clone();
        match_rows(
            self,
            &old_locations,
            &after,
            &mut drafts,
            dialect,
            &mut identities,
            namespace,
        )?;
        assign_missing_order_ranks(self, &old_locations, &mut drafts)?;
        let index = RowIndex::from_drafts(drafts.clone())?;
        let sparse_rows_touched = self.row_count().saturating_add(drafts.len());
        let document = Self(Arc::new(DocumentInner {
            blob: after,
            index,
            identities,
            order_overrides: self.0.order_overrides.clone(),
            dialect,
            sparse_rows_touched,
        }));
        let changes = changed_entities(self, &document, &old_locations, &drafts)?;
        Ok((document, changes))
    }

    pub fn open_entities(entities: Vec<EntityRecord>) -> Result<(Self, ByteEdit), String> {
        let mut builder = EntityImportBuilder::new();
        for entity in entities {
            builder.push(entity)?;
        }
        builder.finish()
    }

    pub fn entities_changed(
        &self,
        changes: &[EntityChange],
    ) -> Result<(Self, Vec<ByteEdit>), String> {
        if changes.is_empty() {
            return Ok((self.clone(), Vec::new()));
        }
        if changes
            .iter()
            .any(|change| change.schema_key == TABLE_SCHEMA_KEY && change.snapshot.is_none())
        {
            return Err("CSV table root cannot be deleted".to_owned());
        }
        if changes.len() == 1 && changes[0].schema_key == ROW_SCHEMA_KEY {
            let change = &changes[0];
            if change.entity_pk.len() != 1 {
                return Err("CSV row changes require one primary-key component".to_owned());
            }
            let id = &change.entity_pk[0];
            let slot = self.0.identities.slot_for_id(id);
            let existing = slot.and_then(|slot| self.0.index.location_for_identity_slot(slot));
            match (&change.snapshot, existing) {
                (None, Some(location)) => return self.delete_sparse_row(location),
                (None, None) => return Ok((self.clone(), Vec::new())),
                (Some(snapshot), Some(location)) => {
                    let semantic = parse_row_snapshot(snapshot)?;
                    if semantic.id != *id {
                        return Err("CSV row snapshot id does not match entity key".to_owned());
                    }
                    if let Some(result) = self.update_or_reorder_sparse_row(location, &semantic)? {
                        return Ok(result);
                    }
                }
                (Some(snapshot), None) => {
                    let semantic = parse_row_snapshot(snapshot)?;
                    if semantic.id != *id {
                        return Err("CSV row snapshot id does not match entity key".to_owned());
                    }
                    return self.insert_sparse_row(slot, &semantic);
                }
            }
        }

        // Multi-row sparse sets and dialect mutation use the exact cold
        // renderer. Every single-row content/delete/insert/reorder case above
        // stays local except an unterminated-EOF reorder.
        let mut records = self.entity_records()?;
        for change in changes {
            let key = (&change.schema_key, &change.entity_pk);
            records.retain(|record| (&record.schema_key, &record.entity_pk) != key);
            if let Some(snapshot) = &change.snapshot {
                records.push(EntityRecord {
                    schema_key: change.schema_key.clone(),
                    entity_pk: change.entity_pk.clone(),
                    snapshot: snapshot.clone(),
                });
            }
        }
        let (document, mut edit) = Self::open_entities(records)?;
        edit.delete_len = u64::try_from(self.0.blob.len()).expect("file length fits u64");
        Ok((document, vec![edit]))
    }

    fn entity_records(&self) -> Result<Vec<EntityRecord>, String> {
        let mut records = Vec::with_capacity(self.row_count() + 1);
        records.push(EntityRecord {
            schema_key: TABLE_SCHEMA_KEY.to_owned(),
            entity_pk: vec![ROOT_ENTITY_PK.to_owned()],
            snapshot: table_snapshot(self.0.dialect),
        });
        for location in self.0.index.locations() {
            records.push(EntityRecord {
                schema_key: ROW_SCHEMA_KEY.to_owned(),
                entity_pk: vec![self.row_id(location)],
                snapshot: self.row_snapshot(location)?,
            });
        }
        Ok(records)
    }

    fn row_snapshot(&self, location: RowLocation) -> Result<Vec<u8>, String> {
        let (chunk, row) = self.0.index.row(location);
        let id = self.0.identities.id(row.id_slot);
        let order = self.order_key(row);
        row_snapshot_bytes(&self.0.blob, chunk, row, &id, &order, self.0.dialect)
    }

    fn order_key(&self, row: &CompactRow) -> String {
        self.0
            .order_overrides
            .get(row.id_slot)
            .map_or_else(|| format!("{:016x}", row.order_rank), ToOwned::to_owned)
    }

    fn row_id(&self, location: RowLocation) -> String {
        let (_, row) = self.0.index.row(location);
        self.0.identities.id(row.id_slot)
    }

    fn delete_sparse_row(&self, location: RowLocation) -> Result<(Self, Vec<ByteEdit>), String> {
        let ordinal = self.0.index.ordinal_of(location);
        let (chunk, row) = self.0.index.row(location);
        let start = chunk.byte_start + row.relative_start;
        let splice = InputSplice {
            offset: u64::from(start),
            delete_len: u64::from(row.byte_len),
            insert: &[],
        };
        let blob = self.0.blob.splice(&[splice])?;
        let (index, sparse_rows_touched) = replace_index_range(
            &self.0.index,
            ordinal,
            ordinal + 1,
            Vec::new(),
            -i64::from(row.byte_len),
        )?;
        let document = Self(Arc::new(DocumentInner {
            blob,
            index,
            identities: self.0.identities.clone(),
            order_overrides: self.0.order_overrides.clone(),
            dialect: self.0.dialect,
            sparse_rows_touched,
        }));
        Ok((
            document,
            vec![ByteEdit {
                offset: splice.offset,
                delete_len: splice.delete_len,
                insert: Arc::new(Vec::new()),
            }],
        ))
    }

    fn update_or_reorder_sparse_row(
        &self,
        location: RowLocation,
        semantic: &RowSnapshot,
    ) -> Result<Option<(Self, Vec<ByteEdit>)>, String> {
        let source_ordinal = self.0.index.ordinal_of(location);
        let (source_chunk, source_row) = self.0.index.row(location);
        if semantic.order_key == self.order_key(source_row) {
            return self
                .replace_sparse_row(location, semantic, source_row.order_rank, 0)
                .map(Some);
        }
        let (target_ordinal, lookup_rows_touched) =
            self.insertion_ordinal(&semantic.order_key, &semantic.id, Some(source_ordinal));
        if target_ordinal == source_ordinal {
            return self
                .replace_sparse_row(
                    location,
                    semantic,
                    source_row.order_rank,
                    lookup_rows_touched,
                )
                .map(Some);
        }

        // A missing final terminator is position metadata rather than row
        // metadata. Keep that uncommon edge on the exact full renderer until
        // it can be represented as a third bounded splice.
        let last = self
            .0
            .index
            .ordinal_location(self.row_count().saturating_sub(1))
            .expect("a reordered document has at least one row");
        let last_has_ending = self.0.index.row(last).1.ending().is_some();
        if source_row.ending().is_none()
            || (target_ordinal + 1 == self.row_count() && !last_has_ending)
        {
            return Ok(None);
        }

        let (order_rank, rank_rows_touched) =
            self.rank_at_insertion(target_ordinal, Some(source_ordinal))?;
        let desired_ending = semantic.layout.ending(self.0.dialect);
        if desired_ending.is_none() && target_ordinal + 1 != self.row_count() {
            return Ok(None);
        }
        let insert = render_row_with_layout(
            &semantic.cells,
            self.0.dialect,
            desired_ending,
            &semantic.layout.force_quote,
        )?;
        let source_start = source_chunk.byte_start + source_row.relative_start;
        let source_len = source_row.byte_len;
        let destination = if target_ordinal < source_ordinal {
            let location = self
                .0
                .index
                .ordinal_location(target_ordinal)
                .expect("move destination is in bounds");
            self.0.index.row_start(location)
        } else if target_ordinal + 1 < self.row_count() {
            let location = self
                .0
                .index
                .ordinal_location(target_ordinal + 1)
                .expect("move destination is in bounds");
            self.0.index.row_start(location)
        } else {
            u32::try_from(self.0.blob.len()).expect("CSV length fits u32")
        };

        let deletion = InputSplice {
            offset: u64::from(source_start),
            delete_len: u64::from(source_len),
            insert: &[],
        };
        let without_row = self.0.blob.splice(&[deletion])?;
        let (without_index, delete_touched) = replace_index_range(
            &self.0.index,
            source_ordinal,
            source_ordinal + 1,
            Vec::new(),
            -i64::from(source_len),
        )?;
        let insertion_offset = if destination > source_start {
            destination - source_len
        } else {
            destination
        };
        let insertion = InputSplice {
            offset: u64::from(insertion_offset),
            delete_len: 0,
            insert: &insert,
        };
        let blob = without_row.splice(&[insertion])?;
        let draft = row_draft_from_rendered(
            &insert,
            insertion_offset,
            source_row.id_slot,
            order_rank,
            self.0.dialect,
        )?;
        let mut next_chunk_key = without_index.next_chunk_key;
        let replacement = build_chunks(vec![draft], &mut next_chunk_key)?;
        let (index, insert_touched) = replace_index_range(
            &without_index,
            target_ordinal,
            target_ordinal,
            replacement,
            i64::try_from(insert.len()).map_err(|_| "CSV row is too large")?,
        )?;
        let document = Self(Arc::new(DocumentInner {
            blob,
            index,
            identities: self.0.identities.clone(),
            order_overrides: self.0.order_overrides.with_key(
                source_row.id_slot,
                &semantic.order_key,
                order_rank,
            ),
            dialect: self.0.dialect,
            sparse_rows_touched: delete_touched
                + insert_touched
                + lookup_rows_touched
                + rank_rows_touched,
        }));
        let mut edits = vec![
            ByteEdit {
                offset: u64::from(source_start),
                delete_len: u64::from(source_len),
                insert: Arc::new(Vec::new()),
            },
            ByteEdit {
                offset: u64::from(destination),
                delete_len: 0,
                insert: Arc::new(insert),
            },
        ];
        edits.sort_unstable_by_key(|edit| edit.offset);
        Ok(Some((document, edits)))
    }

    fn replace_sparse_row(
        &self,
        location: RowLocation,
        semantic: &RowSnapshot,
        order_rank: u64,
        lookup_rows_touched: usize,
    ) -> Result<(Self, Vec<ByteEdit>), String> {
        let ordinal = self.0.index.ordinal_of(location);
        let (chunk, row) = self.0.index.row(location);
        let start = chunk.byte_start + row.relative_start;
        let ending = semantic.layout.ending(self.0.dialect);
        if ending.is_none() && ordinal + 1 != self.row_count() {
            return Err("only the final CSV row may be unterminated".to_owned());
        }
        let insert = render_row_with_layout(
            &semantic.cells,
            self.0.dialect,
            ending,
            &semantic.layout.force_quote,
        )?;
        let splice = InputSplice {
            offset: u64::from(start),
            delete_len: u64::from(row.byte_len),
            insert: &insert,
        };
        let blob = self.0.blob.splice(&[splice])?;
        let draft =
            row_draft_from_rendered(&insert, start, row.id_slot, order_rank, self.0.dialect)?;
        let mut next_chunk_key = self.0.index.next_chunk_key;
        let replacement = build_chunks(vec![draft], &mut next_chunk_key)?;
        let delta = i64::try_from(insert.len()).map_err(|_| "CSV row is too large")?
            - i64::from(row.byte_len);
        let (index, sparse_rows_touched) =
            replace_index_range(&self.0.index, ordinal, ordinal + 1, replacement, delta)?;
        let document = Self(Arc::new(DocumentInner {
            blob,
            index,
            identities: self.0.identities.clone(),
            order_overrides: self.0.order_overrides.with_key(
                row.id_slot,
                &semantic.order_key,
                order_rank,
            ),
            dialect: self.0.dialect,
            sparse_rows_touched: sparse_rows_touched + lookup_rows_touched,
        }));
        Ok((
            document,
            vec![ByteEdit {
                offset: splice.offset,
                delete_len: splice.delete_len,
                insert: Arc::new(insert),
            }],
        ))
    }

    fn insert_sparse_row(
        &self,
        existing_slot: Option<u32>,
        semantic: &RowSnapshot,
    ) -> Result<(Self, Vec<ByteEdit>), String> {
        let (target_ordinal, lookup_rows_touched) =
            self.insertion_ordinal(&semantic.order_key, &semantic.id, None);
        let (order_rank, rank_rows_touched) = self.rank_at_insertion(target_ordinal, None)?;
        let mut identities = self.0.identities.clone();
        let slot = if let Some(slot) = existing_slot {
            slot
        } else {
            identities.append_id(&semantic.id)?
        };
        let ending = semantic.layout.ending(self.0.dialect);
        if ending.is_none() && target_ordinal != self.row_count() {
            return Err("only the final CSV row may be unterminated".to_owned());
        }
        let mut insert = render_row_with_layout(
            &semantic.cells,
            self.0.dialect,
            ending,
            &semantic.layout.force_quote,
        )?;
        let mut replacement_drafts = Vec::with_capacity(2);
        let offset = if target_ordinal < self.row_count() {
            let location = self
                .0
                .index
                .ordinal_location(target_ordinal)
                .expect("insert position is in bounds");
            self.0.index.row_start(location)
        } else {
            u32::try_from(self.0.blob.len()).expect("CSV length fits u32")
        };

        if target_ordinal == self.row_count() && self.row_count() > 0 {
            let last = self
                .0
                .index
                .ordinal_location(self.row_count() - 1)
                .expect("nonempty CSV has a last row");
            let (_, last_row) = self.0.index.row(last);
            if last_row.ending().is_none() {
                let mut prefixed = self.0.dialect.terminator.bytes().to_vec();
                prefixed.append(&mut insert);
                insert = prefixed;
                let last_start = self.0.index.row_start(last);
                let last_bytes = self.0.blob.range(
                    usize::try_from(last_start).expect("u32 fits usize"),
                    self.0.blob.len(),
                )?;
                let mut local = last_bytes;
                local.extend_from_slice(&insert);
                let mut drafts = scan_rows(&local, 0, local.len(), self.0.dialect)?;
                if drafts.len() != 2 {
                    return Err("CSV EOF insertion did not produce two rows".to_owned());
                }
                drafts[0].start = last_start;
                drafts[0].id_slot = Some(last_row.id_slot);
                drafts[0].order_rank = Some(last_row.order_rank);
                drafts[1].start = last_start
                    .checked_add(drafts[1].start)
                    .ok_or_else(|| "CSV row offset overflow".to_owned())?;
                drafts[1].id_slot = Some(slot);
                drafts[1].order_rank = Some(order_rank);
                replacement_drafts = drafts;
            }
        }
        if replacement_drafts.is_empty() {
            replacement_drafts.push(row_draft_from_rendered(
                &insert,
                offset,
                slot,
                order_rank,
                self.0.dialect,
            )?);
        }
        let splice = InputSplice {
            offset: u64::from(offset),
            delete_len: 0,
            insert: &insert,
        };
        let blob = self.0.blob.splice(&[splice])?;
        let replaces_unterminated_last = replacement_drafts.len() == 2;
        let mut next_chunk_key = self.0.index.next_chunk_key;
        let replacement = build_chunks(replacement_drafts, &mut next_chunk_key)?;
        let (first, last) = if replaces_unterminated_last {
            (target_ordinal - 1, target_ordinal)
        } else {
            (target_ordinal, target_ordinal)
        };
        let (index, sparse_rows_touched) = replace_index_range(
            &self.0.index,
            first,
            last,
            replacement,
            i64::try_from(insert.len()).map_err(|_| "CSV row is too large")?,
        )?;
        let document = Self(Arc::new(DocumentInner {
            blob,
            index,
            identities,
            order_overrides: self
                .0
                .order_overrides
                .with_key(slot, &semantic.order_key, order_rank),
            dialect: self.0.dialect,
            sparse_rows_touched: sparse_rows_touched + lookup_rows_touched + rank_rows_touched,
        }));
        Ok((
            document,
            vec![ByteEdit {
                offset: splice.offset,
                delete_len: 0,
                insert: Arc::new(insert),
            }],
        ))
    }

    fn insertion_ordinal(
        &self,
        order_key: &str,
        id: &str,
        excluding: Option<usize>,
    ) -> (usize, usize) {
        let len = self.row_count() - usize::from(excluding.is_some());
        let mut low = 0usize;
        let mut high = len;
        let mut rows_touched = 0usize;
        while low < high {
            rows_touched += 1;
            let middle = low + (high - low) / 2;
            let ordinal = virtual_ordinal(middle, excluding);
            let location = self
                .0
                .index
                .ordinal_location(ordinal)
                .expect("virtual row ordinal is in bounds");
            let (_, row) = self.0.index.row(location);
            let candidate_order = self.order_key(row);
            let candidate_id = self.row_id(location);
            if (candidate_order.as_str(), candidate_id.as_str()) < (order_key, id) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        (low, rows_touched)
    }

    fn rank_at_insertion(
        &self,
        target: usize,
        excluding: Option<usize>,
    ) -> Result<(u64, usize), String> {
        let len = self.row_count() - usize::from(excluding.is_some());
        let lower_location = target.checked_sub(1).and_then(|ordinal| {
            self.0
                .index
                .ordinal_location(virtual_ordinal(ordinal, excluding))
        });
        let upper_location = (target < len)
            .then(|| virtual_ordinal(target, excluding))
            .and_then(|ordinal| self.0.index.ordinal_location(ordinal));
        let lower = lower_location.map(|location| self.0.index.row(location).1.order_rank);
        let upper = upper_location.map(|location| self.0.index.row(location).1.order_rank);
        let rows_touched =
            usize::from(lower_location.is_some()) + usize::from(upper_location.is_some());
        ranks_between(lower, upper, 1).map(|mut ranks| (ranks.remove(0), rows_touched))
    }
}

fn virtual_ordinal(ordinal: usize, excluding: Option<usize>) -> usize {
    if excluding.is_some_and(|excluded| ordinal >= excluded) {
        ordinal + 1
    } else {
        ordinal
    }
}

fn row_draft_from_rendered(
    rendered: &[u8],
    start: u32,
    id_slot: u32,
    order_rank: u64,
    dialect: Dialect,
) -> Result<RowDraft, String> {
    let mut drafts = scan_rows(rendered, 0, rendered.len(), dialect)?;
    if drafts.len() != 1 {
        return Err("rendered CSV entity must contain exactly one row".to_owned());
    }
    let mut draft = drafts.remove(0);
    draft.start = draft
        .start
        .checked_add(start)
        .ok_or_else(|| "CSV row offset overflow".to_owned())?;
    draft.id_slot = Some(id_slot);
    draft.order_rank = Some(order_rank);
    Ok(draft)
}

fn assign_initial_rows(rows: &mut [RowDraft]) {
    let denominator = u128::try_from(rows.len() + 1).expect("usize fits u128");
    for (index, row) in rows.iter_mut().enumerate() {
        row.id_slot = Some(u32::try_from(index).expect("validated row count"));
        let numerator = u128::try_from(index + 1).expect("usize fits u128") * u128::from(u64::MAX);
        let rank = u64::try_from(numerator / denominator).expect("ratio fits u64") | 1;
        row.order_rank = Some(rank);
    }
}

fn preferred_terminator(rows: &[RowDraft]) -> Terminator {
    preferred_terminator_for_document(rows, Terminator::Lf)
}

fn preferred_terminator_for_document(rows: &[RowDraft], fallback: Terminator) -> Terminator {
    let mut counts = [0usize; 3];
    for row in rows {
        match row.ending {
            Some(Terminator::Lf) => counts[0] += 1,
            Some(Terminator::CrLf) => counts[1] += 1,
            Some(Terminator::Cr) => counts[2] += 1,
            None => {}
        }
    }
    match counts.iter().enumerate().max_by_key(|(_, count)| *count) {
        Some((0, count)) if *count > 0 => Terminator::Lf,
        Some((1, count)) if *count > 0 => Terminator::CrLf,
        Some((2, count)) if *count > 0 => Terminator::Cr,
        _ => fallback,
    }
}

fn scan_rows(
    bytes: &[u8],
    start: usize,
    end: usize,
    dialect: Dialect,
) -> Result<Vec<RowDraft>, String> {
    if start > end || end > bytes.len() {
        return Err("invalid CSV scan range".to_owned());
    }
    if start == end {
        return Ok(Vec::new());
    }
    let mut rows = Vec::new();
    let mut row_start = start;
    let mut field_start = start;
    let mut fields = Vec::new();
    let mut quoted = false;
    let mut field_was_quoted = false;
    let mut just_closed_quote = false;
    let mut cursor = start;

    while cursor < end {
        let byte = bytes[cursor];
        if quoted {
            if Some(byte) == dialect.quote {
                if cursor + 1 < end && bytes[cursor + 1] == byte {
                    cursor += 2;
                    continue;
                }
                quoted = false;
                just_closed_quote = true;
            }
            cursor += 1;
            continue;
        }

        if just_closed_quote {
            let is_terminator = byte == b'\r' || byte == b'\n';
            if byte != dialect.delimiter && !is_terminator {
                return Err(format!(
                    "unexpected byte after closing quote at offset {cursor}"
                ));
            }
        }

        if Some(byte) == dialect.quote && cursor == field_start {
            quoted = true;
            field_was_quoted = true;
            just_closed_quote = false;
            cursor += 1;
            continue;
        }

        if byte == dialect.delimiter {
            fields.push(FieldRange::new(
                field_start - row_start,
                cursor - field_start,
                field_was_quoted,
            )?);
            field_start = cursor + 1;
            field_was_quoted = false;
            just_closed_quote = false;
            cursor += 1;
            continue;
        }

        let ending = if byte == b'\r' {
            if cursor + 1 < end && bytes[cursor + 1] == b'\n' {
                Some((Terminator::CrLf, 2usize))
            } else {
                Some((Terminator::Cr, 1usize))
            }
        } else if byte == b'\n' {
            Some((Terminator::Lf, 1usize))
        } else {
            None
        };
        if let Some((terminator, ending_len)) = ending {
            fields.push(FieldRange::new(
                field_start - row_start,
                cursor - field_start,
                field_was_quoted,
            )?);
            rows.push(RowDraft {
                start: u32::try_from(row_start)
                    .map_err(|_| "CSV offset exceeds 4GiB".to_owned())?,
                byte_len: u32::try_from(cursor + ending_len - row_start)
                    .map_err(|_| "CSV row exceeds 4GiB".to_owned())?,
                ending: Some(terminator),
                fields: std::mem::take(&mut fields),
                id_slot: None,
                order_rank: None,
            });
            cursor += ending_len;
            row_start = cursor;
            field_start = cursor;
            field_was_quoted = false;
            just_closed_quote = false;
            continue;
        }
        just_closed_quote = false;
        cursor += 1;
    }

    if quoted {
        return Err(format!("unterminated quoted field at offset {field_start}"));
    }
    if row_start < end {
        fields.push(FieldRange::new(
            field_start - row_start,
            end - field_start,
            field_was_quoted,
        )?);
        rows.push(RowDraft {
            start: u32::try_from(row_start).map_err(|_| "CSV offset exceeds 4GiB".to_owned())?,
            byte_len: u32::try_from(end - row_start)
                .map_err(|_| "CSV row exceeds 4GiB".to_owned())?,
            ending: None,
            fields,
            id_slot: None,
            order_rank: None,
        });
    }
    Ok(rows)
}

fn validate_splices(file_len: usize, splices: &[InputSplice<'_>]) -> Result<(), String> {
    let mut previous_end = 0u64;
    for (index, splice) in splices.iter().enumerate() {
        let end = splice
            .offset
            .checked_add(splice.delete_len)
            .ok_or_else(|| "CSV splice range overflow".to_owned())?;
        if end > u64::try_from(file_len).expect("usize fits u64") {
            return Err("CSV splice exceeds accepted file".to_owned());
        }
        if index > 0 && splice.offset <= previous_end {
            return Err(
                "CSV splices must have strictly increasing, non-overlapping starts".to_owned(),
            );
        }
        previous_end = end;
    }
    Ok(())
}

fn affected_row_window(
    index: &RowIndex,
    splices: &[InputSplice<'_>],
) -> Result<(Option<RowLocation>, Option<RowLocation>), String> {
    if index.row_count == 0 {
        return Ok((None, None));
    }
    let first_offset = u32::try_from(splices.first().expect("nonempty").offset)
        .map_err(|_| "splice offset exceeds 4GiB".to_owned())?;
    let last = splices.last().expect("nonempty");
    let last_end = u32::try_from(last.offset + last.delete_len)
        .map_err(|_| "splice end exceeds 4GiB".to_owned())?;
    let first = index.location_for_offset(first_offset, first_offset == u32::MAX);
    let mut last_location = index.location_for_offset(last_end, last.delete_len == 0);
    if let (Some(first), Some(last)) = (first, last_location) {
        if index.ordinal_of(last) < index.ordinal_of(first) {
            last_location = Some(first);
        }
    }
    Ok((first, last_location))
}

fn map_offset(
    offset: u32,
    splices: &[InputSplice<'_>],
    include_at_offset: bool,
) -> Result<usize, String> {
    let mut mapped = i128::from(offset);
    for splice in splices {
        if splice.offset < u64::from(offset)
            || (include_at_offset && splice.offset == u64::from(offset))
        {
            mapped += i128::try_from(splice.insert.len()).map_err(|_| "insert is too large")?
                - i128::from(splice.delete_len);
        }
    }
    usize::try_from(mapped).map_err(|_| "mapped CSV offset overflow".to_owned())
}

fn extend_to_next_record_boundary(bytes: &PersistentBlob, offset: usize) -> usize {
    let mut cursor = offset;
    while cursor < bytes.len() {
        let byte = bytes
            .byte(cursor)
            .expect("cursor is inside the persistent blob");
        if byte == b'\n' {
            return cursor + 1;
        }
        if byte == b'\r' {
            return if cursor + 1 < bytes.len() && bytes.byte(cursor + 1) == Some(b'\n') {
                cursor + 2
            } else {
                cursor + 1
            };
        }
        cursor += 1;
    }
    bytes.len()
}

fn collect_window_locations(
    index: &RowIndex,
    first: Option<RowLocation>,
    last: Option<RowLocation>,
) -> Vec<RowLocation> {
    let (Some(first), Some(last)) = (first, last) else {
        return Vec::new();
    };
    let first_ordinal = index.ordinal_of(first);
    let last_ordinal = index.ordinal_of(last);
    (first_ordinal..=last_ordinal)
        .filter_map(|ordinal| index.ordinal_location(ordinal))
        .collect()
}

fn decoded_field(
    blob: &[u8],
    row_start: usize,
    field: FieldRange,
    quote: Option<u8>,
) -> Result<String, String> {
    let start = row_start + usize::try_from(field.start).expect("u32 fits usize");
    let end = start + usize::try_from(field.length()).expect("u32 fits usize");
    let raw = &blob[start..end];
    if !field.quoted() {
        return std::str::from_utf8(raw)
            .map(ToOwned::to_owned)
            .map_err(|error| format!("CSV field is not UTF-8: {error}"));
    }
    let quote = quote.ok_or_else(|| "quoted CSV field has no configured quote".to_owned())?;
    if raw.len() < 2 || raw.first() != Some(&quote) || raw.last() != Some(&quote) {
        return Err("invalid quoted CSV field".to_owned());
    }
    let body = &raw[1..raw.len() - 1];
    if !body
        .windows(2)
        .any(|pair| pair[0] == quote && pair[1] == quote)
    {
        return std::str::from_utf8(body)
            .map(ToOwned::to_owned)
            .map_err(|error| format!("CSV field is not UTF-8: {error}"));
    }
    let mut decoded = Vec::with_capacity(body.len());
    let mut cursor = 0;
    while cursor < body.len() {
        if body[cursor] == quote && cursor + 1 < body.len() && body[cursor + 1] == quote {
            decoded.push(quote);
            cursor += 2;
        } else {
            decoded.push(body[cursor]);
            cursor += 1;
        }
    }
    String::from_utf8(decoded).map_err(|error| format!("CSV field is not UTF-8: {error}"))
}

fn field_has_unnecessary_quotes(
    blob: &[u8],
    row_start: usize,
    field: FieldRange,
    dialect: Dialect,
) -> Result<bool, String> {
    if !field.quoted() {
        return Ok(false);
    }
    let quote = dialect
        .quote
        .ok_or_else(|| "quoted CSV field has no configured quote".to_owned())?;
    let start = row_start + usize::try_from(field.start).expect("u32 fits usize");
    let end = start + usize::try_from(field.length()).expect("u32 fits usize");
    let raw = &blob[start..end];
    if raw.len() < 2 || raw.first() != Some(&quote) || raw.last() != Some(&quote) {
        return Err("invalid quoted CSV field".to_owned());
    }
    Ok(!raw[1..raw.len() - 1].iter().any(|byte| {
        *byte == dialect.delimiter || *byte == b'\r' || *byte == b'\n' || *byte == quote
    }))
}

fn draft_cells(
    blob: &PersistentBlob,
    draft: &RowDraft,
    dialect: Dialect,
) -> Result<Vec<String>, String> {
    let start = usize::try_from(draft.start).expect("u32 fits usize");
    let row = blob.range(
        start,
        start + usize::try_from(draft.byte_len).expect("u32 fits usize"),
    )?;
    draft
        .fields
        .iter()
        .copied()
        .map(|field| decoded_field(&row, 0, field, dialect.quote))
        .collect()
}

fn indexed_cells(document: &Document, location: RowLocation) -> Result<Vec<String>, String> {
    let (chunk, row) = document.0.index.row(location);
    let row_start = usize::try_from(chunk.byte_start + row.relative_start).expect("u32 fits usize");
    let row_bytes = document.0.blob.range(
        row_start,
        row_start + usize::try_from(row.byte_len).expect("u32 fits usize"),
    )?;
    let first = usize::try_from(row.first_field).expect("u32 fits usize");
    let end = first + usize::from(row.field_count);
    chunk.data.fields[first..end]
        .iter()
        .copied()
        .map(|field| decoded_field(&row_bytes, 0, field, document.0.dialect.quote))
        .collect()
}

fn cells_hash(cells: &[String]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for cell in cells {
        for byte in u64::try_from(cell.len())
            .expect("usize fits u64")
            .to_le_bytes()
        {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100_0000_01b3);
        }
        for &byte in cell.as_bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100_0000_01b3);
        }
    }
    hash
}

fn match_rows(
    document: &Document,
    old_locations: &[RowLocation],
    new_blob: &PersistentBlob,
    new_rows: &mut [RowDraft],
    new_dialect: Dialect,
    identities: &mut IdentityStore,
    namespace: IdNamespace,
) -> Result<(), String> {
    let old_cells = old_locations
        .iter()
        .copied()
        .map(|location| indexed_cells(document, location))
        .collect::<Result<Vec<_>, _>>()?;
    let new_cells = new_rows
        .iter()
        .map(|row| draft_cells(new_blob, row, new_dialect))
        .collect::<Result<Vec<_>, _>>()?;
    let mut old_used = vec![false; old_locations.len()];
    let mut new_used = vec![false; new_rows.len()];

    // Anchor unchanged prefix/suffix rows before content matching. This is
    // what distinguishes two byte-identical duplicates when a neighboring
    // duplicate changes: the unchanged one keeps its positional identity.
    let shared = old_cells.len().min(new_cells.len());
    let mut prefix = 0usize;
    while prefix < shared && old_cells[prefix] == new_cells[prefix] {
        assign_matched_row(
            document,
            old_locations,
            new_rows,
            &mut old_used,
            &mut new_used,
            prefix,
            prefix,
        );
        prefix += 1;
    }
    let mut old_suffix = old_cells.len();
    let mut new_suffix = new_cells.len();
    while old_suffix > prefix && new_suffix > prefix {
        if old_cells[old_suffix - 1] != new_cells[new_suffix - 1] {
            break;
        }
        old_suffix -= 1;
        new_suffix -= 1;
        assign_matched_row(
            document,
            old_locations,
            new_rows,
            &mut old_used,
            &mut new_used,
            old_suffix,
            new_suffix,
        );
    }

    let mut old_by_hash = HashMap::<u64, VecDeque<usize>>::new();
    for (index, cells) in old_cells.iter().enumerate() {
        if old_used[index] {
            continue;
        }
        old_by_hash
            .entry(cells_hash(cells))
            .or_default()
            .push_back(index);
    }
    for (new_index, cells) in new_cells.iter().enumerate() {
        if new_used[new_index] {
            continue;
        }
        let Some(candidates) = old_by_hash.get_mut(&cells_hash(cells)) else {
            continue;
        };
        let candidate_position = candidates
            .iter()
            .position(|old_index| old_cells[*old_index] == *cells);
        let Some(candidate_position) = candidate_position else {
            continue;
        };
        let old_index = candidates
            .remove(candidate_position)
            .expect("candidate exists");
        assign_matched_row(
            document,
            old_locations,
            new_rows,
            &mut old_used,
            &mut new_used,
            old_index,
            new_index,
        );
    }

    let remaining_old = old_used
        .iter()
        .enumerate()
        .filter_map(|(index, used)| (!used).then_some(index))
        .collect::<Vec<_>>();
    let remaining_new = new_used
        .iter()
        .enumerate()
        .filter_map(|(index, used)| (!used).then_some(index))
        .collect::<Vec<_>>();
    for (old_index, new_index) in remaining_old.into_iter().zip(remaining_new) {
        let (_, old_row) = document.0.index.row(old_locations[old_index]);
        new_rows[new_index].id_slot = Some(old_row.id_slot);
        new_rows[new_index].order_rank = Some(old_row.order_rank);
        old_used[old_index] = true;
        new_used[new_index] = true;
    }

    let mut ordinal = 0u64;
    for (index, row) in new_rows.iter_mut().enumerate() {
        if !new_used[index] {
            row.id_slot = Some(identities.append_generated(namespace, ordinal)?);
            ordinal = ordinal
                .checked_add(1)
                .ok_or_else(|| "generated ID ordinal overflow".to_owned())?;
        }
    }
    Ok(())
}

fn assign_matched_row(
    document: &Document,
    old_locations: &[RowLocation],
    new_rows: &mut [RowDraft],
    old_used: &mut [bool],
    new_used: &mut [bool],
    old_index: usize,
    new_index: usize,
) {
    old_used[old_index] = true;
    new_used[new_index] = true;
    let (_, old_row) = document.0.index.row(old_locations[old_index]);
    new_rows[new_index].id_slot = Some(old_row.id_slot);
    new_rows[new_index].order_rank = Some(old_row.order_rank);
}

fn assign_missing_order_ranks(
    document: &Document,
    old_locations: &[RowLocation],
    rows: &mut [RowDraft],
) -> Result<(), String> {
    let previous = old_locations
        .first()
        .and_then(|first| {
            let ordinal = document.0.index.ordinal_of(*first);
            ordinal
                .checked_sub(1)
                .and_then(|value| document.0.index.ordinal_location(value))
        })
        .map(|location| document.0.index.row(location).1.order_rank);
    let next = old_locations
        .last()
        .and_then(|last| {
            let ordinal = document.0.index.ordinal_of(*last) + 1;
            document.0.index.ordinal_location(ordinal)
        })
        .map(|location| document.0.index.row(location).1.order_rank);

    let mut cursor = 0;
    let mut lower = previous;
    while cursor < rows.len() {
        if let Some(rank) = rows[cursor].order_rank {
            if lower.is_some_and(|value| value >= rank) || next.is_some_and(|value| rank >= value) {
                rows[cursor].order_rank = None;
            } else {
                lower = Some(rank);
                cursor += 1;
                continue;
            }
        }
        let run_start = cursor;
        let ranks = loop {
            while cursor < rows.len() && rows[cursor].order_rank.is_none() {
                cursor += 1;
            }
            if let Some(rank) = rows.get(cursor).and_then(|row| row.order_rank)
                && (lower.is_some_and(|value| value >= rank)
                    || next.is_some_and(|value| rank >= value))
            {
                rows[cursor].order_rank = None;
                cursor += 1;
                continue;
            }
            let upper = rows.get(cursor).and_then(|row| row.order_rank).or(next);
            match ranks_between(lower, upper, cursor - run_start) {
                Ok(ranks) => break ranks,
                Err(_) if cursor < rows.len() => {
                    // A matched row can carry a rank from its former physical
                    // position. If an inserted/unmatched run precedes such a
                    // row after a reorder, that stale rank is not a usable
                    // upper anchor. Fold the anchor into the run and keep
                    // searching instead of reporting an exhausted interval.
                    // This is deterministic and remains linear in the byte
                    // edit's already-bounded row window.
                    rows[cursor].order_rank = None;
                    cursor += 1;
                }
                Err(error) => return Err(error),
            }
        };
        for (row, rank) in rows[run_start..cursor].iter_mut().zip(ranks) {
            row.order_rank = Some(rank);
            lower = Some(rank);
        }
    }
    Ok(())
}

pub(crate) fn ranks_between(
    lower: Option<u64>,
    upper: Option<u64>,
    count: usize,
) -> Result<Vec<u64>, String> {
    if count == 0 {
        return Ok(Vec::new());
    }
    let low = u128::from(lower.unwrap_or(0));
    let high = u128::from(upper.unwrap_or(u64::MAX));
    if high <= low + u128::try_from(count).expect("usize fits u128") {
        return Err("CSV order-key interval exhausted".to_owned());
    }
    let divisor = u128::try_from(count + 1).expect("usize fits u128");
    let span = high - low;
    let mut result = Vec::with_capacity(count);
    let mut previous = u64::try_from(low).expect("lower fits u64");
    for index in 1..=count {
        let interpolated =
            u64::try_from(low + span * u128::try_from(index).expect("usize fits u128") / divisor)
                .expect("interpolated rank fits u64");
        let mut rank = interpolated.max(
            previous
                .checked_add(1)
                .ok_or_else(|| "CSV order-key interval exhausted".to_owned())?,
        );
        if rank.trailing_zeros() >= 8 {
            rank = rank
                .checked_add(1)
                .ok_or_else(|| "CSV order-key interval exhausted".to_owned())?;
        }
        if rank <= previous || upper.is_some_and(|value| rank >= value) {
            return Err("CSV order-key interval exhausted".to_owned());
        }
        result.push(rank);
        previous = rank;
    }
    Ok(result)
}

fn replace_index_window(
    index: &RowIndex,
    first: Option<RowLocation>,
    last: Option<RowLocation>,
    replacement: Vec<ChunkRef>,
    delta: i64,
) -> Result<(RowIndex, usize), String> {
    let first_ordinal = first.map_or(0, |value| index.ordinal_of(value));
    let last_ordinal_exclusive = last.map_or(first_ordinal, |value| index.ordinal_of(value) + 1);
    replace_index_range(
        index,
        first_ordinal,
        last_ordinal_exclusive,
        replacement,
        delta,
    )
}

fn replace_index_range(
    index: &RowIndex,
    first_ordinal: usize,
    last_ordinal_exclusive: usize,
    replacement: Vec<ChunkRef>,
    delta: i64,
) -> Result<(RowIndex, usize), String> {
    let row_count = usize::try_from(index.row_count).expect("u32 fits usize");
    if first_ordinal > last_ordinal_exclusive || last_ordinal_exclusive > row_count {
        return Err("CSV row replacement range is out of bounds".to_owned());
    }
    let mut next_chunk_key = replacement
        .iter()
        .map(|chunk| chunk.key)
        .max()
        .and_then(|key| key.checked_add(1))
        .unwrap_or(index.next_chunk_key)
        .max(index.next_chunk_key);
    let mut chunks = Vec::with_capacity(index.chunks.len() + replacement.len() + 2);
    let mut chunk_first = 0usize;
    for chunk in index.chunks.iter() {
        let chunk_last = chunk_first + chunk.data.rows.len();
        if chunk_last <= first_ordinal {
            chunks.push(chunk.clone());
        } else if chunk_first < first_ordinal {
            chunks.push(slice_chunk(
                chunk,
                0,
                first_ordinal - chunk_first,
                0,
                &mut next_chunk_key,
            )?);
        }
        chunk_first = chunk_last;
    }
    chunks.extend(replacement);
    let mut chunk_first = 0usize;
    for chunk in index.chunks.iter() {
        let chunk_last = chunk_first + chunk.data.rows.len();
        if chunk_first >= last_ordinal_exclusive {
            chunks.push(shift_chunk(chunk, delta)?);
        } else if chunk_last > last_ordinal_exclusive {
            chunks.push(slice_chunk(
                chunk,
                last_ordinal_exclusive - chunk_first,
                chunk.data.rows.len(),
                delta,
                &mut next_chunk_key,
            )?);
        }
        chunk_first = chunk_last;
    }
    let retained_keys = chunks.iter().map(|chunk| chunk.key).collect::<HashSet<_>>();
    let old_keys = index
        .chunks
        .iter()
        .map(|chunk| chunk.key)
        .collect::<HashSet<_>>();
    let mut location_changes = Vec::new();
    let mut rows_touched = 0usize;
    for chunk in index
        .chunks
        .iter()
        .filter(|chunk| !retained_keys.contains(&chunk.key))
    {
        rows_touched += chunk.data.rows.len();
        location_changes.extend(
            chunk
                .data
                .rows
                .iter()
                .map(|row| (row.id_slot, SlotLocationOverride::Removed)),
        );
    }
    for chunk in chunks.iter().filter(|chunk| !old_keys.contains(&chunk.key)) {
        rows_touched += chunk.data.rows.len();
        for (row, value) in chunk.data.rows.iter().enumerate() {
            location_changes.push((
                value.id_slot,
                SlotLocationOverride::Present(PackedRowLocation {
                    chunk_key: chunk.key,
                    row: u16::try_from(row).expect("row chunks contain at most 512 rows"),
                }),
            ));
        }
    }
    let row_count = chunks
        .iter()
        .map(|chunk| chunk.data.rows.len())
        .sum::<usize>();
    let field_count = chunks
        .iter()
        .map(|chunk| chunk.data.fields.len())
        .sum::<usize>();
    let (row_starts, chunk_positions) = index_chunk_metadata(&chunks)?;
    Ok((
        RowIndex {
            chunks: Arc::new(chunks),
            row_starts: Arc::new(row_starts),
            chunk_positions: Arc::new(chunk_positions),
            slot_locations: index.slot_locations.with_changes(location_changes),
            row_count: u32::try_from(row_count).map_err(|_| "CSV has too many rows".to_owned())?,
            field_count: u32::try_from(field_count)
                .map_err(|_| "CSV has too many fields".to_owned())?,
            next_chunk_key,
        },
        rows_touched,
    ))
}

fn shift_u32(value: u32, delta: i64) -> Result<u32, String> {
    u32::try_from(i64::from(value) + delta).map_err(|_| "CSV offset overflow".to_owned())
}

fn shift_chunk(chunk: &ChunkRef, delta: i64) -> Result<ChunkRef, String> {
    Ok(ChunkRef {
        key: chunk.key,
        byte_start: shift_u32(chunk.byte_start, delta)?,
        data: chunk.data.clone(),
    })
}

fn slice_chunk(
    chunk: &ChunkRef,
    start: usize,
    end: usize,
    delta: i64,
    next_key: &mut u32,
) -> Result<ChunkRef, String> {
    let selected = &chunk.data.rows[start..end];
    let original_start = selected.first().map_or(chunk.byte_start, |row| {
        chunk.byte_start + row.relative_start
    });
    let byte_start = shift_u32(original_start, delta)?;
    let mut rows = Vec::with_capacity(selected.len());
    let mut fields = Vec::new();
    for row in selected {
        let first = usize::try_from(row.first_field).expect("u32 fits usize");
        let field_end = first + usize::from(row.field_count);
        let new_first =
            u32::try_from(fields.len()).map_err(|_| "CSV chunk has too many fields".to_owned())?;
        fields.extend_from_slice(&chunk.data.fields[first..field_end]);
        let mut copied = *row;
        copied.relative_start = chunk.byte_start + row.relative_start - original_start;
        copied.first_field = new_first;
        rows.push(copied);
    }
    Ok(ChunkRef {
        key: next_chunk_key(next_key)?,
        byte_start,
        data: Arc::new(RowChunk {
            rows: rows.into_boxed_slice(),
            fields: fields.into_boxed_slice(),
        }),
    })
}

fn changed_entities(
    before: &Document,
    after: &Document,
    old_locations: &[RowLocation],
    new_drafts: &[RowDraft],
) -> Result<Vec<EntityChange>, String> {
    let mut old_by_slot = HashMap::new();
    for &location in old_locations {
        let (_, row) = before.0.index.row(location);
        old_by_slot.insert(row.id_slot, location);
    }
    let mut new_slots = HashMap::new();
    for draft in new_drafts {
        let slot = draft.id_slot.expect("assigned row identity");
        new_slots.insert(slot, draft);
    }
    let mut changes = Vec::new();
    for (&slot, &location) in &old_by_slot {
        if !new_slots.contains_key(&slot) {
            changes.push(EntityChange::delete(
                ROW_SCHEMA_KEY,
                before.row_id(location),
            ));
        }
    }
    for (&slot, draft) in &new_slots {
        let location = after
            .0
            .index
            .location_for_identity_slot(slot)
            .ok_or_else(|| "new CSV row was not indexed".to_owned())?;
        let new_snapshot = after.row_snapshot(location)?;
        let effect = if let Some(&old_location) = old_by_slot.get(&slot) {
            let old_cells = indexed_cells(before, old_location)?;
            let new_cells = draft_cells(&after.0.blob, draft, after.0.dialect)?;
            let (_, old_row) = before.0.index.row(old_location);
            let semantic_changed = old_cells != new_cells
                || old_row.order_rank != draft.order_rank.expect("assigned rank");
            let lexical_changed = before.row_snapshot(old_location)? != new_snapshot;
            if semantic_changed {
                Some(ChangeEffect::Content)
            } else if lexical_changed {
                Some(ChangeEffect::FormatOnly)
            } else {
                None
            }
        } else {
            Some(ChangeEffect::Content)
        };
        if let Some(effect) = effect {
            changes.push(EntityChange::upsert_with_effect(
                ROW_SCHEMA_KEY,
                after.row_id(location),
                new_snapshot,
                effect,
            ));
        }
    }
    if before.0.dialect != after.0.dialect {
        changes.push(EntityChange::upsert(
            TABLE_SCHEMA_KEY,
            ROOT_ENTITY_PK.to_owned(),
            table_snapshot(after.0.dialect),
        ));
    }
    changes.sort_by(|left, right| {
        left.schema_key
            .cmp(&right.schema_key)
            .then_with(|| left.entity_pk.cmp(&right.entity_pk))
    });
    Ok(changes)
}

fn row_snapshot_bytes(
    blob: &PersistentBlob,
    chunk: &ChunkRef,
    row: &CompactRow,
    id: &str,
    order_key: &str,
    dialect: Dialect,
) -> Result<Vec<u8>, String> {
    let mut output = Vec::with_capacity(128);
    output.extend_from_slice(b"{\"id\":");
    serde_json::to_writer(&mut output, id).map_err(|error| error.to_string())?;
    output.extend_from_slice(b",\"order_key\":");
    serde_json::to_writer(&mut output, order_key).map_err(|error| error.to_string())?;
    output.extend_from_slice(b",\"cells\":[");
    let row_start = usize::try_from(chunk.byte_start + row.relative_start).expect("u32 fits usize");
    let row_bytes = blob.range(
        row_start,
        row_start + usize::try_from(row.byte_len).expect("u32 fits usize"),
    )?;
    let first = usize::try_from(row.first_field).expect("u32 fits usize");
    let end = first + usize::from(row.field_count);
    let mut force_quote = Vec::new();
    for (index, field) in chunk.data.fields[first..end].iter().copied().enumerate() {
        if index > 0 {
            output.push(b',');
        }
        if field_has_unnecessary_quotes(&row_bytes, 0, field, dialect)? {
            if force_quote.len() <= index / 8 {
                force_quote.resize(index / 8 + 1, 0);
            }
            force_quote[index / 8] |= 1 << (index % 8);
        }
        let value = decoded_field(&row_bytes, 0, field, dialect.quote)?;
        serde_json::to_writer(&mut output, &value).map_err(|error| error.to_string())?;
    }
    output.push(b']');
    let has_force_quote = !force_quote.is_empty();
    let exceptional_ending = (row.ending() != Some(dialect.terminator)).then_some(row.ending());
    if has_force_quote || exceptional_ending.is_some() {
        output.extend_from_slice(b",\"layout\":{");
        let needs_comma = if has_force_quote {
            output.extend_from_slice(b"\"force_quote\":");
            serde_json::to_writer(&mut output, &URL_SAFE_NO_PAD.encode(force_quote))
                .map_err(|error| error.to_string())?;
            true
        } else {
            false
        };
        if let Some(ending) = exceptional_ending {
            if needs_comma {
                output.push(b',');
            }
            output.extend_from_slice(b"\"terminator\":");
            serde_json::to_writer(
                &mut output,
                ending.map_or("", |terminator| terminator.snapshot()),
            )
            .map_err(|error| error.to_string())?;
        }
        output.push(b'}');
    }
    output.push(b'}');
    Ok(output)
}

fn table_snapshot(dialect: Dialect) -> Vec<u8> {
    let mut output = String::with_capacity(96);
    output.push_str("{\"id\":\"root\",\"dialect\":{\"delimiter\":");
    let delimiter = char::from(dialect.delimiter).to_string();
    output.push_str(&serde_json::to_string(&delimiter).expect("string serialization cannot fail"));
    output.push_str(",\"quote\":");
    match dialect.quote {
        Some(quote) => output.push_str(
            &serde_json::to_string(&char::from(quote).to_string())
                .expect("string serialization cannot fail"),
        ),
        None => output.push_str("null"),
    }
    output.push_str(",\"terminator\":");
    output.push_str(
        &serde_json::to_string(dialect.terminator.snapshot())
            .expect("string serialization cannot fail"),
    );
    output.push_str("}}");
    output.into_bytes()
}

fn parse_table_snapshot(bytes: &[u8]) -> Result<Dialect, String> {
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|error| format!("invalid CSV table snapshot: {error}"))?;
    reject_numbers(&value)?;
    let object = value
        .as_object()
        .ok_or_else(|| "CSV table snapshot must be an object".to_owned())?;
    if object.get("id").and_then(Value::as_str) != Some(ROOT_ENTITY_PK) {
        return Err("CSV table id must be root".to_owned());
    }
    if object.len() != 2 || !object.contains_key("dialect") {
        return Err("CSV table snapshot must contain only id and dialect".to_owned());
    }
    let dialect = object
        .get("dialect")
        .and_then(Value::as_object)
        .ok_or_else(|| "CSV table dialect must be an object".to_owned())?;
    if dialect.len() != 3
        || !dialect.contains_key("delimiter")
        || !dialect.contains_key("quote")
        || !dialect.contains_key("terminator")
    {
        return Err("CSV dialect must contain only delimiter, quote, and terminator".to_owned());
    }
    let delimiter = dialect
        .get("delimiter")
        .and_then(Value::as_str)
        .and_then(|value| value.chars().next().filter(|_| value.chars().count() == 1))
        .and_then(|value| u8::try_from(u32::from(value)).ok())
        .ok_or_else(|| "CSV delimiter must be one Latin-1 character".to_owned())?;
    let quote = match dialect.get("quote") {
        Some(Value::Null) => None,
        Some(Value::String(value)) => value
            .chars()
            .next()
            .filter(|_| value.chars().count() == 1)
            .and_then(|value| u8::try_from(u32::from(value)).ok())
            .map(Some)
            .ok_or_else(|| "CSV quote must be one Latin-1 character".to_owned())?,
        _ => return Err("CSV quote must be a string or null".to_owned()),
    };
    let terminator = match dialect.get("terminator").and_then(Value::as_str) {
        Some("\n") => Terminator::Lf,
        Some("\r\n") => Terminator::CrLf,
        Some("\r") => Terminator::Cr,
        _ => return Err("CSV terminator is invalid".to_owned()),
    };
    Dialect {
        delimiter,
        quote,
        terminator,
    }
    .validate_entity()
}

#[derive(Clone, Debug)]
pub struct InitialChanges {
    document: Document,
    row: usize,
    table_pending: bool,
}

impl Iterator for InitialChanges {
    type Item = Result<EntityChange, String>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.table_pending {
            self.table_pending = false;
            return Some(Ok(EntityChange::upsert(
                TABLE_SCHEMA_KEY,
                ROOT_ENTITY_PK.to_owned(),
                table_snapshot(self.document.0.dialect),
            )));
        }
        let location = self.document.0.index.ordinal_location(self.row)?;
        self.row += 1;
        Some(self.document.row_snapshot(location).map(|snapshot| {
            EntityChange::upsert(ROW_SCHEMA_KEY, self.document.row_id(location), snapshot)
        }))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowSnapshot {
    pub id: String,
    pub order_key: String,
    pub cells: Vec<String>,
    pub layout: RowLayout,
}

pub fn parse_row_snapshot(bytes: &[u8]) -> Result<RowSnapshot, String> {
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|error| format!("invalid CSV row snapshot: {error}"))?;
    reject_numbers(&value)?;
    let object = value
        .as_object()
        .ok_or_else(|| "CSV row snapshot must be an object".to_owned())?;
    if !(object.len() == 3 || object.len() == 4)
        || !object.contains_key("id")
        || !object.contains_key("order_key")
        || !object.contains_key("cells")
        || object
            .keys()
            .any(|key| !matches!(key.as_str(), "id" | "order_key" | "cells" | "layout"))
    {
        return Err(
            "CSV row snapshot must contain id, order_key, cells, and optional layout".to_owned(),
        );
    }
    let id = object
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| "CSV row id must be a non-empty string".to_owned())?
        .to_owned();
    let order_key = object
        .get("order_key")
        .and_then(Value::as_str)
        .filter(|key| valid_order_key(key))
        .ok_or_else(|| "CSV row order_key is invalid".to_owned())?
        .to_owned();
    let cells = object
        .get("cells")
        .and_then(Value::as_array)
        .ok_or_else(|| "CSV row cells must be an array".to_owned())?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(ToOwned::to_owned)
                .ok_or_else(|| "CSV cells must be strings".to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?;
    if cells.is_empty() {
        return Err("CSV rows require at least one cell".to_owned());
    }
    let layout = object
        .get("layout")
        .map(|value| parse_row_layout(value, cells.len()))
        .transpose()?
        .unwrap_or_default();
    Ok(RowSnapshot {
        id,
        order_key,
        cells,
        layout,
    })
}

fn parse_row_layout(value: &Value, field_count: usize) -> Result<RowLayout, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "CSV row layout must be an object".to_owned())?;
    if object.is_empty()
        || object
            .keys()
            .any(|key| !matches!(key.as_str(), "force_quote" | "terminator"))
    {
        return Err("CSV row layout must contain force_quote and/or terminator only".to_owned());
    }
    let force_quote = match object.get("force_quote") {
        None => Vec::new(),
        Some(Value::String(value)) => {
            let decoded = URL_SAFE_NO_PAD
                .decode(value)
                .map_err(|_| "CSV force_quote must be unpadded base64url".to_owned())?;
            if URL_SAFE_NO_PAD.encode(&decoded) != *value {
                return Err("CSV force_quote must use canonical unpadded base64url".to_owned());
            }
            let maximum = field_count.div_ceil(8);
            if decoded.is_empty()
                || decoded.len() > maximum
                || decoded.last().is_some_and(|byte| *byte == 0)
            {
                return Err(
                    "CSV force_quote must be a minimal nonzero bitset within the field count"
                        .to_owned(),
                );
            }
            let remainder = field_count % 8;
            if remainder != 0
                && decoded.len() == maximum
                && decoded
                    .last()
                    .is_some_and(|byte| byte & !((1 << remainder) - 1) != 0)
            {
                return Err("CSV force_quote has bits beyond the final field".to_owned());
            }
            decoded
        }
        Some(_) => return Err("CSV force_quote must be a base64url string".to_owned()),
    };
    let terminator = match object.get("terminator") {
        None => None,
        Some(Value::String(value)) => Some(match value.as_str() {
            "" => None,
            "\n" => Some(Terminator::Lf),
            "\r\n" => Some(Terminator::CrLf),
            "\r" => Some(Terminator::Cr),
            _ => return Err("CSV row layout terminator is invalid".to_owned()),
        }),
        Some(_) => return Err("CSV row layout terminator must be a string".to_owned()),
    };
    Ok(RowLayout {
        force_quote,
        terminator,
    })
}

fn reject_numbers(value: &Value) -> Result<(), String> {
    match value {
        Value::Number(_) => {
            Err("number-bearing snapshots are not eligible for packet v1".to_owned())
        }
        Value::Array(values) => values.iter().try_for_each(reject_numbers),
        Value::Object(values) => values.values().try_for_each(reject_numbers),
        _ => Ok(()),
    }
}

fn valid_order_key(key: &str) -> bool {
    !key.is_empty()
        && key.len().is_multiple_of(2)
        && key.as_bytes().iter().all(u8::is_ascii_hexdigit)
        && key.as_bytes().iter().all(|byte| !byte.is_ascii_uppercase())
        && !key.ends_with("00")
}

pub fn render_row(
    cells: &[String],
    dialect: Dialect,
    ending: Option<Terminator>,
) -> Result<Vec<u8>, String> {
    render_row_with_layout(cells, dialect, ending, &[])
}

fn render_row_with_layout(
    cells: &[String],
    dialect: Dialect,
    ending: Option<Terminator>,
    force_quote: &[u8],
) -> Result<Vec<u8>, String> {
    if cells.is_empty() {
        return Err("CSV rows require at least one cell".to_owned());
    }
    let mut output = Vec::new();
    for (index, cell) in cells.iter().enumerate() {
        if index > 0 {
            output.push(dialect.delimiter);
        }
        let force_quote = force_quote
            .get(index / 8)
            .is_some_and(|byte| byte & (1 << (index % 8)) != 0);
        let canonical_quote = imported_cell_needs_quotes(cell.as_bytes(), dialect)?;
        if force_quote && canonical_quote {
            return Err("CSV force_quote may select only otherwise-unnecessary quotes".to_owned());
        }
        let needs_quote = force_quote || canonical_quote;
        if needs_quote {
            let quote = dialect.quote.ok_or_else(|| {
                "CSV dialect without a quote cannot force quoted fields".to_owned()
            })?;
            output.push(quote);
            for &byte in cell.as_bytes() {
                output.push(byte);
                if byte == quote {
                    output.push(quote);
                }
            }
            output.push(quote);
        } else {
            output.extend_from_slice(cell.as_bytes());
        }
    }
    if let Some(ending) = ending {
        output.extend_from_slice(ending.bytes());
    }
    Ok(output)
}

pub fn describe_memory(document: &Document) -> String {
    let mut description = String::new();
    let _ = write!(
        description,
        "blob={} rows={} fields={} retained_estimate={} sparse_rows_touched={}",
        document.bytes().len(),
        document.row_count(),
        document.field_count(),
        document.retained_bytes_estimate(),
        document.0.sparse_rows_touched,
    );
    description
}
