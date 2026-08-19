//! Typed column slots for scalar-declared row columns.
//!
//! Current-state snapshots are stored as UTF-8 JSON text. Every read that needs
//! one named column still runs a deserializer across the whole document: the
//! column-selective filter parse removes five of six *materializations* per row
//! and zero of the ~177 tokenized bytes. This module replaces the tokenizing
//! step with an O(1) indexed lookup.
//!
//! Schemas in this repository are closed — `additionalProperties: false` is
//! required-present-and-required-false in `schema/definition.json`, and
//! `assert_row_properties_have_projectable_types` rejects any property whose
//! declared type does not resolve. So the set of columns a snapshot may carry is
//! known from the schema alone, and a record can address them *positionally*
//! instead of by key.
//!
//! ```text
//!  0        format version (1)
//!  1..3     declared column count (big endian u16)
//!  3..3+9n  directory, 9 bytes per declared column, in layout order:
//!               [0]    slot tag
//!               [1..5] payload offset within the payload area (big endian u32)
//!               [5..9] payload byte length (big endian u32)
//!  3+9n..   payload area
//! ```
//!
//! Column *names* are not stored. A reader resolves a name to a slot index once
//! per batch against the schema layout, then indexes the directory directly, so
//! the per-row cost of reading a column is a bounds check and a slice — no
//! bytes of the record are tokenized, including the bytes of the columns the
//! reader did not ask for.
//!
//! # Scalar columns only, by design
//!
//! `String`, `Integer`, `Number` and `Boolean` columns get typed payloads.
//! `Jsonb`-declared columns keep their JSON text verbatim in a `Jsonb` slot:
//! `lix_key_value.value` and `json_pointer.value` are arbitrary JSON by design
//! — a `::jsonb` column, not a value of unknown type — and this format
//! deliberately does not introduce a tagged any-slot to cover them. A `Jsonb`
//! slot is a byte range a reader can hand back or re-parse exactly as it does
//! today; nothing about it gets worse, and nothing about it pretends to be
//! typed.
//!
//! # Absent keys become null slots
//!
//! A slot per declared column cannot express "absent". Absent keys really do
//! occur after normalization — `would_apply` only visits properties that have
//! defaults, so a nullable, default-less property that was never written stays
//! missing from the snapshot map. Encoding therefore materializes *every*
//! declared column and writes a null slot where the key was absent.
//!
//! This is safe at the schema surface because that surface already erases the
//! distinction: `row_json_text_value` maps both `None` and
//! `Some(JsonValue::Null)` to SQL `NULL`. It does not disturb the write path's
//! deliberate absent-vs-null distinction either, because that distinction is
//! consumed during normalization (defaults apply to absent keys and not to
//! explicit nulls, pinned by
//! `normalization_does_not_overwrite_explicit_null_with_default`) which runs
//! strictly before a row is encoded here. `absent_columns` is retained so the
//! difference remains *readable* out of the record rather than being destroyed
//! by it.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};

/// Wire format version for a typed-slot record.
///
/// This is a physical layout constant, not a repository protocol value. A
/// change to it requires a `REPOSITORY_PROTOCOL_VALUE` bump like any other
/// stored-format change.
pub(crate) const TYPED_SLOTS_VERSION: u8 = 1;

const HEADER_BYTES: usize = 3;
const DIRECTORY_ENTRY_BYTES: usize = 9;

const TAG_NULL: u8 = 0;
const TAG_ABSENT: u8 = 1;
const TAG_FALSE: u8 = 2;
const TAG_TRUE: u8 = 3;
const TAG_I64: u8 = 4;
const TAG_U64: u8 = 5;
const TAG_F64: u8 = 6;
const TAG_STR: u8 = 7;
const TAG_JSONB: u8 = 8;

// Counted at every point on the decode path that hands bytes to a JSON
// tokenizer, so a test can assert that reading columns out of a typed record
// tokenized nothing rather than inspecting the code and believing it. Only a
// `Jsonb`-declared slot can move this counter; a scalar slot decodes to a value
// without a parser ever seeing its bytes.
#[cfg(test)]
thread_local! {
    static TYPED_SLOTS_JSON_PARSES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn record_typed_slots_json_parse() {
    TYPED_SLOTS_JSON_PARSES.with(|count| count.set(count.get() + 1));
}

#[cfg(not(test))]
fn record_typed_slots_json_parse() {}

#[cfg(test)]
fn reset_typed_slots_json_parse_count() {
    TYPED_SLOTS_JSON_PARSES.with(|count| count.set(0));
}

#[cfg(test)]
fn typed_slots_json_parse_count() -> usize {
    TYPED_SLOTS_JSON_PARSES.with(std::cell::Cell::get)
}

/// The declared type of a column, as the schema catalog resolves it.
///
/// Deliberately a local enum rather than a re-export of the SQL surface's
/// `SchemaColumnType`: this module sits below the catalog, and importing the
/// catalog here would invert that layering for no gain. The SQL side maps its
/// own enum onto this one at the boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeclaredType {
    String,
    Integer,
    Number,
    Boolean,
    /// Arbitrary JSON by design. Stored as verbatim JSON text.
    Jsonb,
}

impl DeclaredType {
    pub(crate) fn is_scalar(self) -> bool {
        !matches!(self, Self::Jsonb)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TypedSlotError {
    message: String,
}

impl TypedSlotError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for TypedSlotError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for TypedSlotError {}

type Result<T> = std::result::Result<T, TypedSlotError>;

/// The positional layout a typed-slot record is addressed through.
///
/// Built once per schema and shared by every row of that schema. Name lookup is
/// resolved here — once per batch — so the per-row path never sees a string
/// comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TypedSlotLayout {
    columns: Vec<(String, DeclaredType)>,
    /// Slot indexes ordered by column name, so canonical JSON can be emitted in
    /// serde_json's key order without sorting per row.
    name_order: Vec<usize>,
    by_name: BTreeMap<String, usize>,
}

impl TypedSlotLayout {
    pub(crate) fn new(columns: impl IntoIterator<Item = (String, DeclaredType)>) -> Result<Self> {
        let columns: Vec<(String, DeclaredType)> = columns.into_iter().collect();
        if columns.len() > usize::from(u16::MAX) {
            return Err(TypedSlotError::new(format!(
                "typed slot layout has {} columns, which exceeds the {} the directory can address",
                columns.len(),
                u16::MAX
            )));
        }
        let mut by_name = BTreeMap::new();
        for (index, (name, _)) in columns.iter().enumerate() {
            if by_name.insert(name.clone(), index).is_some() {
                return Err(TypedSlotError::new(format!(
                    "typed slot layout declares column '{name}' twice"
                )));
            }
        }
        // `by_name` is a BTreeMap, so iterating it yields the same key order
        // serde_json's `Map` uses when it serializes an object.
        let name_order = by_name.values().copied().collect();
        Ok(Self {
            columns,
            name_order,
            by_name,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.columns.len()
    }

    /// Resolves a column name to its slot index. Call once per batch, not once
    /// per row.
    pub(crate) fn index_of(&self, column: &str) -> Option<usize> {
        self.by_name.get(column).copied()
    }

    pub(crate) fn column_name(&self, index: usize) -> Option<&str> {
        self.columns.get(index).map(|(name, _)| name.as_str())
    }

    pub(crate) fn declared_type(&self, index: usize) -> Option<DeclaredType> {
        self.columns.get(index).map(|(_, declared)| *declared)
    }

    /// True when every declared column is scalar, so no slot in this layout
    /// carries JSON text.
    pub(crate) fn is_all_scalar(&self) -> bool {
        self.columns
            .iter()
            .all(|(_, declared)| declared.is_scalar())
    }
}

/// One decoded slot. Borrowed from the record; nothing is copied or parsed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum TypedSlot<'a> {
    /// The column was present and its value was JSON `null`.
    Null,
    /// The column was absent from the snapshot map.
    ///
    /// Reads that go to the schema surface must treat this exactly as `Null`;
    /// the two are distinguished here only so the record does not destroy
    /// information the write path took care to preserve.
    Absent,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    Str(&'a str),
    /// Verbatim JSON text for a `Jsonb`-declared column.
    Jsonb(&'a str),
}

impl TypedSlot<'_> {
    /// Whether this slot reads as SQL `NULL` at the schema surface.
    pub(crate) fn is_sql_null(self) -> bool {
        matches!(self, Self::Null | Self::Absent)
    }
}

/// Encodes one snapshot map into a typed-slot record against `layout`.
///
/// Every declared column gets a slot, in layout order. Columns the map does not
/// carry get `TAG_ABSENT`.
pub(crate) fn encode_typed_slots(
    layout: &TypedSlotLayout,
    snapshot: &JsonMap<String, JsonValue>,
) -> Result<Vec<u8>> {
    let directory_bytes = layout.len() * DIRECTORY_ENTRY_BYTES;
    let mut bytes = Vec::with_capacity(HEADER_BYTES + directory_bytes + snapshot.len() * 16);
    bytes.push(TYPED_SLOTS_VERSION);
    let count = u16::try_from(layout.len()).map_err(|_| {
        TypedSlotError::new("typed slot layout exceeds the u16 column count the header carries")
    })?;
    bytes.extend_from_slice(&count.to_be_bytes());
    bytes.resize(HEADER_BYTES + directory_bytes, 0);

    let payload_start = bytes.len();
    for index in 0..layout.len() {
        let name = layout
            .column_name(index)
            .expect("index is below the layout length");
        let declared = layout
            .declared_type(index)
            .expect("index is below the layout length");
        let offset = bytes.len() - payload_start;
        let tag = match snapshot.get(name) {
            None => TAG_ABSENT,
            Some(JsonValue::Null) => TAG_NULL,
            Some(JsonValue::Bool(value)) => {
                if *value {
                    TAG_TRUE
                } else {
                    TAG_FALSE
                }
            }
            Some(JsonValue::Number(number)) => {
                // The *written* number kind is preserved, not the declared one.
                // An `Integer` column holding `1.0` must render back as `1.0`;
                // coercing it to the declared type would make the record's JSON
                // reconstruction differ from the bytes it replaced.
                if let Some(value) = number.as_i64() {
                    bytes.extend_from_slice(&value.to_be_bytes());
                    TAG_I64
                } else if let Some(value) = number.as_u64() {
                    bytes.extend_from_slice(&value.to_be_bytes());
                    TAG_U64
                } else if let Some(value) = number.as_f64() {
                    bytes.extend_from_slice(&value.to_be_bytes());
                    TAG_F64
                } else {
                    return Err(TypedSlotError::new(format!(
                        "column '{name}' holds a JSON number that is neither i64, u64 nor f64"
                    )));
                }
            }
            Some(JsonValue::String(value)) => {
                bytes.extend_from_slice(value.as_bytes());
                TAG_STR
            }
            Some(other) => {
                if declared.is_scalar() {
                    return Err(TypedSlotError::new(format!(
                        "column '{name}' is declared scalar but holds a composite JSON value"
                    )));
                }
                let rendered = serde_json::to_string(other).map_err(|error| {
                    TypedSlotError::new(format!(
                        "column '{name}' could not be rendered as JSON text: {error}"
                    ))
                })?;
                bytes.extend_from_slice(rendered.as_bytes());
                TAG_JSONB
            }
        };
        // A `Jsonb`-declared column holding a scalar still stores JSON text, so
        // reconstruction reproduces the value without consulting the layout.
        let tag = if declared.is_scalar() {
            tag
        } else {
            match tag {
                TAG_ABSENT | TAG_NULL => tag,
                TAG_JSONB => TAG_JSONB,
                _ => {
                    // Rewind the scalar payload just written and re-emit it as
                    // JSON text.
                    bytes.truncate(payload_start + offset);
                    let value = snapshot
                        .get(name)
                        .expect("a non-absent slot has a value in the map");
                    let rendered = serde_json::to_string(value).map_err(|error| {
                        TypedSlotError::new(format!(
                            "column '{name}' could not be rendered as JSON text: {error}"
                        ))
                    })?;
                    bytes.extend_from_slice(rendered.as_bytes());
                    TAG_JSONB
                }
            }
        };
        let length = bytes.len() - payload_start - offset;
        write_directory_entry(&mut bytes, index, tag, offset, length)?;
    }

    Ok(bytes)
}

fn write_directory_entry(
    bytes: &mut [u8],
    index: usize,
    tag: u8,
    offset: usize,
    length: usize,
) -> Result<()> {
    let offset = u32::try_from(offset).map_err(|_| {
        TypedSlotError::new("typed slot payload area exceeds the u32 offset the directory carries")
    })?;
    let length = u32::try_from(length).map_err(|_| {
        TypedSlotError::new("typed slot payload exceeds the u32 length the directory carries")
    })?;
    let entry = HEADER_BYTES + index * DIRECTORY_ENTRY_BYTES;
    bytes[entry] = tag;
    bytes[entry + 1..entry + 5].copy_from_slice(&offset.to_be_bytes());
    bytes[entry + 5..entry + 9].copy_from_slice(&length.to_be_bytes());
    Ok(())
}

/// A borrowed typed-slot record.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TypedSlotsRef<'a> {
    bytes: &'a [u8],
    count: usize,
    payload_start: usize,
}

impl<'a> TypedSlotsRef<'a> {
    /// Validates the header and directory bounds once, so `slot` needs no
    /// further checks against the payload area.
    pub(crate) fn parse(bytes: &'a [u8]) -> Result<Self> {
        if bytes.len() < HEADER_BYTES {
            return Err(TypedSlotError::new(
                "typed slot record is shorter than its header",
            ));
        }
        if bytes[0] != TYPED_SLOTS_VERSION {
            return Err(TypedSlotError::new(format!(
                "unsupported typed slot record version {}, expected {TYPED_SLOTS_VERSION}",
                bytes[0]
            )));
        }
        let count = usize::from(u16::from_be_bytes([bytes[1], bytes[2]]));
        let payload_start = HEADER_BYTES + count * DIRECTORY_ENTRY_BYTES;
        if bytes.len() < payload_start {
            return Err(TypedSlotError::new(
                "typed slot record is shorter than its directory",
            ));
        }
        let record = Self {
            bytes,
            count,
            payload_start,
        };
        // Bounds-check every payload range up front. A per-row read then costs
        // a directory index and a slice, with no validation on the hot path.
        for index in 0..count {
            let (_, offset, length) = record.directory_entry(index);
            let end = offset
                .checked_add(length)
                .ok_or_else(|| TypedSlotError::new("typed slot payload range overflowed"))?;
            if payload_start + end > bytes.len() {
                return Err(TypedSlotError::new(format!(
                    "typed slot {index} payload range runs past the end of the record"
                )));
            }
        }
        Ok(record)
    }

    /// True when `bytes` could be a typed-slot record rather than JSON text.
    ///
    /// JSON snapshots are objects, so their first byte is `{` (0x7B) or ASCII
    /// whitespace. The version byte is 1, which no JSON document can start
    /// with, so the two representations are distinguishable without parsing
    /// either one.
    pub(crate) fn looks_like_record(bytes: &[u8]) -> bool {
        matches!(bytes.first(), Some(&TYPED_SLOTS_VERSION))
    }

    pub(crate) fn len(&self) -> usize {
        self.count
    }

    fn directory_entry(&self, index: usize) -> (u8, usize, usize) {
        let entry = HEADER_BYTES + index * DIRECTORY_ENTRY_BYTES;
        let tag = self.bytes[entry];
        let offset = u32::from_be_bytes([
            self.bytes[entry + 1],
            self.bytes[entry + 2],
            self.bytes[entry + 3],
            self.bytes[entry + 4],
        ]) as usize;
        let length = u32::from_be_bytes([
            self.bytes[entry + 5],
            self.bytes[entry + 6],
            self.bytes[entry + 7],
            self.bytes[entry + 8],
        ]) as usize;
        (tag, offset, length)
    }

    /// Reads one slot by index.
    ///
    /// This is the whole point of the format: the cost does not depend on the
    /// number of columns in the record or on the size of the columns that were
    /// not asked for, and no byte of the record is tokenized.
    pub(crate) fn slot(&self, index: usize) -> Result<TypedSlot<'a>> {
        if index >= self.count {
            return Err(TypedSlotError::new(format!(
                "typed slot index {index} is out of range for a {}-slot record",
                self.count
            )));
        }
        let (tag, offset, length) = self.directory_entry(index);
        let payload =
            &self.bytes[self.payload_start + offset..self.payload_start + offset + length];
        Ok(match tag {
            TAG_NULL => TypedSlot::Null,
            TAG_ABSENT => TypedSlot::Absent,
            TAG_FALSE => TypedSlot::Bool(false),
            TAG_TRUE => TypedSlot::Bool(true),
            TAG_I64 => TypedSlot::I64(i64::from_be_bytes(fixed_eight(payload, index)?)),
            TAG_U64 => TypedSlot::U64(u64::from_be_bytes(fixed_eight(payload, index)?)),
            TAG_F64 => TypedSlot::F64(f64::from_be_bytes(fixed_eight(payload, index)?)),
            TAG_STR => TypedSlot::Str(utf8(payload, index)?),
            TAG_JSONB => TypedSlot::Jsonb(utf8(payload, index)?),
            other => {
                return Err(TypedSlotError::new(format!(
                    "typed slot {index} carries unknown tag {other}"
                )));
            }
        })
    }

    /// Projects the named columns out of the record for a row predicate.
    ///
    /// This is the read the format exists for. The shipped column-selective
    /// filter parse still runs a deserializer across the whole document to find
    /// its columns -- it removes materializations, not tokenizing -- so the
    /// residue it leaves is proportional to the whole snapshot on every scanned
    /// row, survivors included. Here the cost is one directory index per wanted
    /// column and nothing at all for the columns the predicate did not name.
    ///
    /// The result is a `JsonValue` object so the existing predicate evaluator is
    /// unchanged: this replaces how the predicate's columns are *found*, not
    /// what the predicate does with them.
    ///
    /// An absent column is omitted from the object, which is what a `get` on the
    /// original snapshot would have produced for it. An explicit null is
    /// present-and-null, preserving the one distinction the record keeps.
    pub(crate) fn filter_columns(
        &self,
        layout: &TypedSlotLayout,
        wanted: &BTreeSet<&str>,
    ) -> Result<JsonValue> {
        let mut map = JsonMap::new();
        for column in wanted {
            let Some(index) = layout.index_of(column) else {
                continue;
            };
            let value = match self.slot(index)? {
                TypedSlot::Absent => continue,
                TypedSlot::Null => JsonValue::Null,
                TypedSlot::Bool(value) => JsonValue::Bool(value),
                TypedSlot::I64(value) => JsonValue::Number(JsonNumber::from(value)),
                TypedSlot::U64(value) => JsonValue::Number(JsonNumber::from(value)),
                TypedSlot::F64(value) => JsonNumber::from_f64(value)
                    .map(JsonValue::Number)
                    .ok_or_else(|| {
                        TypedSlotError::new(format!(
                            "column '{column}' holds a float that JSON cannot represent"
                        ))
                    })?,
                TypedSlot::Str(value) => JsonValue::String(value.to_string()),
                TypedSlot::Jsonb(value) => {
                    record_typed_slots_json_parse();
                    serde_json::from_str(value).map_err(|error| {
                        TypedSlotError::new(format!(
                            "column '{column}' holds JSON text that did not parse: {error}"
                        ))
                    })?
                }
            };
            map.insert((*column).to_string(), value);
        }
        Ok(JsonValue::Object(map))
    }

    /// Rebuilds the JSON object this record was encoded from.
    ///
    /// This is the fallback for consumers that want the whole document as text;
    /// it is deliberately not on the per-column read path. Emitting in the
    /// layout's name order and rebuilding through `serde_json` is what makes the
    /// output byte-identical to the normalized text this record replaced.
    pub(crate) fn to_canonical_json(&self, layout: &TypedSlotLayout) -> Result<String> {
        let value = self.to_json_value(layout)?;
        serde_json::to_string(&value).map_err(|error| {
            TypedSlotError::new(format!("typed slot record could not be rendered: {error}"))
        })
    }

    pub(crate) fn to_json_value(&self, layout: &TypedSlotLayout) -> Result<JsonValue> {
        if layout.len() != self.count {
            return Err(TypedSlotError::new(format!(
                "typed slot record has {} slots but its layout declares {}",
                self.count,
                layout.len()
            )));
        }
        let mut map = JsonMap::new();
        for &index in &layout.name_order {
            let name = layout
                .column_name(index)
                .expect("name order indexes the layout");
            let value = match self.slot(index)? {
                TypedSlot::Absent => continue,
                TypedSlot::Null => JsonValue::Null,
                TypedSlot::Bool(value) => JsonValue::Bool(value),
                TypedSlot::I64(value) => JsonValue::Number(JsonNumber::from(value)),
                TypedSlot::U64(value) => JsonValue::Number(JsonNumber::from(value)),
                TypedSlot::F64(value) => JsonNumber::from_f64(value)
                    .map(JsonValue::Number)
                    .ok_or_else(|| {
                        TypedSlotError::new(format!(
                            "column '{name}' holds a float that JSON cannot represent"
                        ))
                    })?,
                TypedSlot::Str(value) => JsonValue::String(value.to_string()),
                TypedSlot::Jsonb(value) => {
                    record_typed_slots_json_parse();
                    serde_json::from_str(value).map_err(|error| {
                        TypedSlotError::new(format!(
                            "column '{name}' holds JSON text that did not parse: {error}"
                        ))
                    })?
                }
            };
            map.insert(name.to_string(), value);
        }
        Ok(JsonValue::Object(map))
    }
}

fn fixed_eight(payload: &[u8], index: usize) -> Result<[u8; 8]> {
    payload.try_into().map_err(|_| {
        TypedSlotError::new(format!(
            "typed slot {index} is a fixed-width number but carries {} payload bytes",
            payload.len()
        ))
    })
}

fn utf8<'a>(payload: &'a [u8], index: usize) -> Result<&'a str> {
    std::str::from_utf8(payload).map_err(|error| {
        TypedSlotError::new(format!("typed slot {index} is not valid UTF-8: {error}"))
    })
}

/// The per-row content fingerprint for a typed-slot record.
///
/// `working_diff_slot_fingerprint` digests JSON text and `payload_equality`
/// compares those 32-byte hashes to decide Equal/Different. A typed record owes
/// a replacement that agrees across the dirty/clean transition, and this is it:
/// the record's bytes are a deterministic function of the layout and the slot
/// values — the encoder writes slots in layout order with no padding and no
/// map-iteration order anywhere in it — so two rows with the same content
/// against the same schema hash identically no matter which write produced
/// them.
pub(crate) fn typed_slots_fingerprint(bytes: &[u8]) -> [u8; 32] {
    *blake3::hash(bytes).as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layout() -> TypedSlotLayout {
        TypedSlotLayout::new([
            ("id".to_string(), DeclaredType::String),
            ("count".to_string(), DeclaredType::Integer),
            ("ratio".to_string(), DeclaredType::Number),
            ("enabled".to_string(), DeclaredType::Boolean),
            ("label".to_string(), DeclaredType::String),
            ("payload".to_string(), DeclaredType::Jsonb),
        ])
        .expect("layout")
    }

    fn object(json: &str) -> JsonMap<String, JsonValue> {
        match serde_json::from_str::<JsonValue>(json).expect("valid json") {
            JsonValue::Object(map) => map,
            other => panic!("expected an object, got {other}"),
        }
    }

    /// The deterministic count the whole change is for: reading the predicate's
    /// columns out of a typed record tokenizes nothing.
    ///
    /// The comparison arm is the shipped column-selective parse, which runs a
    /// deserializer over every byte of the document to find the same columns.
    /// The quantity is a count, not a rate, so it does not depend on the host
    /// it was measured on.
    #[test]
    fn reading_predicate_columns_from_a_typed_record_tokenizes_nothing() {
        let layout = TypedSlotLayout::new([
            ("id".to_string(), DeclaredType::String),
            ("count".to_string(), DeclaredType::Integer),
            ("ratio".to_string(), DeclaredType::Number),
            ("enabled".to_string(), DeclaredType::Boolean),
            ("label".to_string(), DeclaredType::String),
            ("note".to_string(), DeclaredType::String),
        ])
        .expect("layout");
        assert!(
            layout.is_all_scalar(),
            "this lane is the all-scalar case the format targets"
        );
        let text = r#"{"count":7,"enabled":true,"id":"record-1","label":"second","note":"a longer trailing column that a predicate on id never needs to look at","ratio":1.5}"#;
        let snapshot = object(text);
        let bytes = encode_typed_slots(&layout, &snapshot).expect("encode");
        let record = TypedSlotsRef::parse(&bytes).expect("parse");

        let wanted: BTreeSet<&str> = ["id", "count"].into_iter().collect();

        reset_typed_slots_json_parse_count();
        let projected = record.filter_columns(&layout, &wanted).expect("project");
        assert_eq!(
            typed_slots_json_parse_count(),
            0,
            "an all-scalar record must reach no JSON tokenizer at all"
        );
        assert_eq!(projected["id"], JsonValue::String("record-1".to_string()));
        assert_eq!(projected["count"], JsonValue::Number(JsonNumber::from(7)));
        assert_eq!(
            projected.as_object().expect("object").len(),
            2,
            "only the wanted columns are materialized"
        );

        // The arm this replaces: a deserializer is handed the whole document.
        // 151 bytes tokenized per scanned row against 0, for the same answer.
        assert_eq!(text.len(), 151);
        let mut deserializer = serde_json::Deserializer::from_str(text);
        let full: JsonValue =
            serde::Deserialize::deserialize(&mut deserializer).expect("baseline parse");
        deserializer.end().expect("baseline end");
        assert_eq!(full["id"], projected["id"]);
        assert_eq!(full["count"], projected["count"]);
    }

    /// A `Jsonb`-declared column is the one slot that still reaches a parser, and
    /// only when the predicate actually names it. This pins that the exemption
    /// is scoped to the column rather than to the record.
    #[test]
    fn only_a_json_declared_column_reaches_a_parser_and_only_when_wanted() {
        let layout = layout();
        let snapshot = object(
            r#"{"count":1,"enabled":false,"id":"row","label":"x","payload":{"deep":[1]},"ratio":0.5}"#,
        );
        let bytes = encode_typed_slots(&layout, &snapshot).expect("encode");
        let record = TypedSlotsRef::parse(&bytes).expect("parse");

        reset_typed_slots_json_parse_count();
        let scalars: BTreeSet<&str> = ["id", "count", "ratio", "enabled", "label"]
            .into_iter()
            .collect();
        record.filter_columns(&layout, &scalars).expect("project");
        assert_eq!(typed_slots_json_parse_count(), 0);

        reset_typed_slots_json_parse_count();
        let with_json: BTreeSet<&str> = ["id", "payload"].into_iter().collect();
        let projected = record.filter_columns(&layout, &with_json).expect("project");
        assert_eq!(
            typed_slots_json_parse_count(),
            1,
            "the arbitrary-JSON column is parsed, and it is the only one"
        );
        assert_eq!(projected["payload"]["deep"][0], JsonValue::Number(1.into()));
    }

    /// A predicate naming a column the schema does not declare must not match
    /// something else by accident.
    #[test]
    fn an_undeclared_predicate_column_projects_to_nothing() {
        let layout = layout();
        let bytes = encode_typed_slots(&layout, &object(r#"{"id":"row"}"#)).expect("encode");
        let record = TypedSlotsRef::parse(&bytes).expect("parse");
        let wanted: BTreeSet<&str> = ["not_a_column"].into_iter().collect();
        let projected = record.filter_columns(&layout, &wanted).expect("project");
        assert_eq!(projected.as_object().expect("object").len(), 0);
    }

    #[test]
    fn typed_slots_round_trip_every_declared_column_with_distinct_values() {
        // Every field carries a distinct value on purpose: a round trip with
        // colliding values cannot detect a permuted slot order.
        let layout = layout();
        let snapshot = object(
            r#"{"count":7,"enabled":true,"id":"row-1","label":"second","payload":{"nested":[1,2]},"ratio":1.5}"#,
        );
        let bytes = encode_typed_slots(&layout, &snapshot).expect("encode");
        let record = TypedSlotsRef::parse(&bytes).expect("parse");

        assert_eq!(record.len(), 6);
        assert_eq!(
            record.slot(layout.index_of("id").expect("id")).expect("id"),
            TypedSlot::Str("row-1")
        );
        assert_eq!(
            record
                .slot(layout.index_of("count").expect("count"))
                .expect("count"),
            TypedSlot::I64(7)
        );
        assert_eq!(
            record
                .slot(layout.index_of("ratio").expect("ratio"))
                .expect("ratio"),
            TypedSlot::F64(1.5)
        );
        assert_eq!(
            record
                .slot(layout.index_of("enabled").expect("enabled"))
                .expect("enabled"),
            TypedSlot::Bool(true)
        );
        assert_eq!(
            record
                .slot(layout.index_of("label").expect("label"))
                .expect("label"),
            TypedSlot::Str("second")
        );
        assert_eq!(
            record
                .slot(layout.index_of("payload").expect("payload"))
                .expect("payload"),
            TypedSlot::Jsonb(r#"{"nested":[1,2]}"#)
        );
    }

    /// The property the staged landing depends on: a typed record can serve a
    /// text consumer without the stored JSON alongside it.
    #[test]
    fn canonical_json_reconstruction_is_byte_identical_to_the_normalized_text() {
        let layout = layout();
        for text in [
            r#"{"count":7,"enabled":true,"id":"row-1","label":"second","payload":{"nested":[1,2]},"ratio":1.5}"#,
            r#"{"count":0,"enabled":false,"id":"","label":"a\"b\nc","payload":null,"ratio":-0.25}"#,
            r#"{"count":-9007199254740993,"enabled":true,"id":"unicode-é","label":"ok","payload":[],"ratio":1e10}"#,
            r#"{"id":"only-required"}"#,
            r#"{"count":null,"enabled":null,"id":"explicit-nulls","label":null,"payload":null,"ratio":null}"#,
        ] {
            let normalized = serde_json::to_string(
                &serde_json::from_str::<JsonValue>(text).expect("valid json"),
            )
            .expect("render");
            let bytes = encode_typed_slots(&layout, &object(text)).expect("encode");
            let record = TypedSlotsRef::parse(&bytes).expect("parse");
            assert_eq!(
                record.to_canonical_json(&layout).expect("reconstruct"),
                normalized,
                "reconstruction diverged for {text}"
            );
        }
    }

    /// Absent and explicit-null are distinct in the record, and identical at
    /// the schema surface. Both halves matter: the first is what keeps the
    /// write path's distinction readable, the second is what makes
    /// materializing every declared column safe.
    #[test]
    fn absent_and_explicit_null_are_distinct_in_the_record_and_equal_at_the_surface() {
        let layout = layout();
        let absent = encode_typed_slots(&layout, &object(r#"{"id":"row"}"#)).expect("encode");
        let explicit = encode_typed_slots(
            &layout,
            &object(
                r#"{"count":null,"enabled":null,"id":"row","label":null,"payload":null,"ratio":null}"#,
            ),
        )
        .expect("encode");

        let absent_record = TypedSlotsRef::parse(&absent).expect("parse");
        let explicit_record = TypedSlotsRef::parse(&explicit).expect("parse");
        let count = layout.index_of("count").expect("count");

        assert_eq!(absent_record.slot(count).expect("slot"), TypedSlot::Absent);
        assert_eq!(explicit_record.slot(count).expect("slot"), TypedSlot::Null);
        assert!(absent_record.slot(count).expect("slot").is_sql_null());
        assert!(explicit_record.slot(count).expect("slot").is_sql_null());

        // Every declared column has a slot in both records: absence is a slot
        // value, not a missing slot.
        assert_eq!(absent_record.len(), layout.len());
        assert_eq!(explicit_record.len(), layout.len());
    }

    /// The equality witness `payload_equality` needs: same content against the
    /// same layout hashes the same, different content does not.
    #[test]
    fn fingerprint_agrees_on_equal_content_and_separates_unequal_content() {
        let layout = layout();
        let one = encode_typed_slots(
            &layout,
            &object(r#"{"count":7,"id":"row","label":"x","payload":1,"enabled":true,"ratio":0.5}"#),
        )
        .expect("encode");
        // Same content, different key order in the source text.
        let two = encode_typed_slots(
            &layout,
            &object(r#"{"ratio":0.5,"enabled":true,"payload":1,"label":"x","id":"row","count":7}"#),
        )
        .expect("encode");
        let three = encode_typed_slots(
            &layout,
            &object(r#"{"count":8,"id":"row","label":"x","payload":1,"enabled":true,"ratio":0.5}"#),
        )
        .expect("encode");

        assert_eq!(one, two, "encoding must not depend on source key order");
        assert_eq!(typed_slots_fingerprint(&one), typed_slots_fingerprint(&two));
        assert_ne!(
            typed_slots_fingerprint(&one),
            typed_slots_fingerprint(&three)
        );
    }

    /// A permuted layout must produce a different record even when the values
    /// would otherwise coincide, or the positional addressing is unsound.
    #[test]
    fn permuted_layout_changes_the_record() {
        let straight = TypedSlotLayout::new([
            ("a".to_string(), DeclaredType::String),
            ("b".to_string(), DeclaredType::String),
        ])
        .expect("layout");
        let permuted = TypedSlotLayout::new([
            ("b".to_string(), DeclaredType::String),
            ("a".to_string(), DeclaredType::String),
        ])
        .expect("layout");
        let snapshot = object(r#"{"a":"first","b":"second"}"#);

        let straight_bytes = encode_typed_slots(&straight, &snapshot).expect("encode");
        let permuted_bytes = encode_typed_slots(&permuted, &snapshot).expect("encode");
        assert_ne!(straight_bytes, permuted_bytes);

        // Both still reconstruct the same document: name order, not slot
        // order, decides the text.
        let straight_record = TypedSlotsRef::parse(&straight_bytes).expect("parse");
        let permuted_record = TypedSlotsRef::parse(&permuted_bytes).expect("parse");
        assert_eq!(
            straight_record
                .to_canonical_json(&straight)
                .expect("reconstruct"),
            permuted_record
                .to_canonical_json(&permuted)
                .expect("reconstruct")
        );
    }

    #[test]
    fn a_typed_record_is_distinguishable_from_json_text_without_parsing_either() {
        let layout = layout();
        let bytes = encode_typed_slots(&layout, &object(r#"{"id":"row"}"#)).expect("encode");
        assert!(TypedSlotsRef::looks_like_record(&bytes));
        assert!(!TypedSlotsRef::looks_like_record(br#"{"id":"row"}"#));
        assert!(!TypedSlotsRef::looks_like_record(b"  {}"));
        assert!(!TypedSlotsRef::looks_like_record(b""));
    }

    #[test]
    fn a_scalar_column_holding_a_composite_value_is_rejected() {
        let layout = layout();
        let error = encode_typed_slots(&layout, &object(r#"{"id":{"nested":true}}"#))
            .expect_err("composite value in a scalar column");
        assert!(
            error.message().contains("declared scalar"),
            "unexpected message: {}",
            error.message()
        );
    }

    #[test]
    fn a_truncated_record_is_rejected_rather_than_read_out_of_bounds() {
        let layout = layout();
        let bytes = encode_typed_slots(&layout, &object(r#"{"id":"row-1"}"#)).expect("encode");
        for truncated in 1..bytes.len() {
            let _ = TypedSlotsRef::parse(&bytes[..truncated]);
        }
        assert!(TypedSlotsRef::parse(&bytes[..bytes.len() - 1]).is_err());
        assert!(TypedSlotsRef::parse(&[TYPED_SLOTS_VERSION]).is_err());
        assert!(TypedSlotsRef::parse(&[9, 0, 0]).is_err());
    }

    #[test]
    fn a_layout_with_a_duplicate_column_is_rejected() {
        let error = TypedSlotLayout::new([
            ("a".to_string(), DeclaredType::String),
            ("a".to_string(), DeclaredType::Integer),
        ])
        .expect_err("duplicate column");
        assert!(error.message().contains("twice"));
    }

    #[test]
    fn an_all_scalar_layout_is_reported_as_such() {
        assert!(!layout().is_all_scalar());
        assert!(
            TypedSlotLayout::new([("a".to_string(), DeclaredType::String)])
                .expect("layout")
                .is_all_scalar()
        );
    }
}
