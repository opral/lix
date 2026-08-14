use std::borrow::Cow;

use super::model::AUTHENTICATED_EDGE_PAGE_ENTRIES;
use super::object::ObjectId;
use crate::LixError;
use crate::common::{LixTimestamp, SharedStr};
use crate::entity_pk::{EntityPk, EntityPkComponent};

const STATE_VALUE_MAGIC: &[u8; 8] = b"LIXFTV\0\x03";
const CURRENT_STATE_VALUE_MAGIC: &[u8; 8] = b"LIXFCV\0\x02";
const MAX_BLOB_ROOTS_PER_STATE_ROW: usize = AUTHENTICATED_EDGE_PAGE_ENTRIES;

const KEY_ESCAPE: u8 = 0xff;
const KEY_PART_FINAL: u8 = 0x00;
const KEY_PART_MORE: u8 = 0x01;
const FILE_ID_NONE: u8 = 0x00;
const FILE_ID_SOME: u8 = 0x01;
const ENTITY_PK_CODEC_V1: u8 = 0x01;
const ENTITY_PK_UUID: u8 = 0x00;
const ENTITY_PK_INTEGER: u8 = 0x01;
const ENTITY_PK_STRING: u8 = 0x02;
const ENTITY_PK_BYTES: u8 = 0x03;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) struct StateKeyRef<'a> {
    pub(crate) schema_key: &'a str,
    pub(crate) file_id: Option<&'a str>,
    pub(crate) entity_pk: &'a EntityPk,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct StateKey {
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) entity_pk: EntityPk,
}

/// Half-open bounds for a canonical byte prefix. `upper == None` means that
/// the prefix has no finite lexicographic successor and therefore extends to
/// the end of the key space.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CanonicalPrefixBounds {
    pub(crate) lower: Vec<u8>,
    pub(crate) upper: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StateValueRef {
    pub(crate) pack_object_id: ObjectId,
    pub(crate) pack_ordinal: u32,
    pub(crate) tombstone: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StateValue {
    pub(crate) change_id: crate::changelog::ChangeId,
    pub(crate) commit_id: crate::changelog::CommitId,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
    pub(crate) cell: StateCell,
    pub(crate) metadata: Option<SharedStr>,
    pub(crate) origin_key: Option<String>,
    pub(crate) blob_manifest_object_ids: Vec<ObjectId>,
}

/// ForkTree-owned historical row shape for semantic readers that have not yet
/// moved their public result DTOs. It deliberately has no dependency on the
/// superseded tracked-state module.
#[derive(Clone, Debug)]
pub(crate) struct HistoricalStateRow {
    pub(crate) key: StateKey,
    /// Whether this visible row was selected from the authenticated global
    /// state root rather than the branch-local overlay.
    pub(crate) global: bool,
    pub(crate) change_id: crate::changelog::ChangeId,
    pub(crate) commit_id: crate::changelog::CommitId,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
    pub(crate) snapshot_content: Option<SharedStr>,
    pub(crate) metadata: Option<SharedStr>,
    pub(crate) deleted: bool,
    /// Authenticated BlobManifest edges carried with the state value. A
    /// payload-free historical transition must preserve this edge when it
    /// republishes a prior blob-ref row.
    pub(crate) blob_manifest_object_ids: Vec<ObjectId>,
}

/// One semantic change between two authenticated historical ForkTree state
/// roots. This neutral shape is consumed by SQL/checkpoint projections and
/// deliberately does not depend on the superseded tracked-state DTOs.
#[derive(Clone, Debug)]
pub(crate) struct HistoricalStateDiffEntry {
    pub(crate) before: Option<HistoricalStateRow>,
    pub(crate) after: Option<HistoricalStateRow>,
}

/// Authenticated write identity for one historical state row. Payload bytes
/// are intentionally absent: stale classification receives this separately
/// from endpoint payload diffs so equal bytes with a new write identity still
/// remain observable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HistoricalStateWriteIdentity {
    pub(crate) change_id: crate::changelog::ChangeId,
    pub(crate) commit_id: crate::changelog::CommitId,
}

/// One logical key whose authenticated write identity changed between two
/// retained ForkTree commits. Both endpoints are preserved for chronology and
/// deletion handling; the payload itself is owned by the separate diff path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HistoricalStateIdentityChange {
    pub(crate) key: StateKey,
    pub(crate) before: Option<HistoricalStateWriteIdentity>,
    pub(crate) after: Option<HistoricalStateWriteIdentity>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StateCellRef<'a> {
    Value(&'a str),
    Null,
    Tombstone,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum StateCell {
    Value(SharedStr),
    NativeRow(NativeRowCell),
    Null,
    Tombstone,
}

impl StateCell {
    /// Materializes one trusted built-in Schema-v1 row at a semantic/API
    /// boundary. Durable state remains the native tuple; this never serves as
    /// a storage fallback and rejects unknown schema ownership.
    pub(crate) fn seed_logical_text(
        &self,
        key: &StateKey,
        global: bool,
    ) -> Result<Option<crate::common::SharedStr>, crate::LixError> {
        match self {
            Self::NativeRow(native) => {
                crate::native_row::logical_text_for_seed(key, global, native).map(Some)
            }
            Self::Value(_) => Err(crate::LixError::new(
                crate::LixError::CODE_STORAGE_ERROR,
                "Schema-v1 current-state row uses the removed JSON snapshot representation",
            )),
            Self::Null | Self::Tombstone => Ok(None),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NativeRowCell {
    pub(crate) layout_id: [u8; 32],
    /// Stable authenticated state domain. The selected global/local root
    /// authenticates visibility; local rows remain shareable across branch
    /// creation without embedding a mutable branch UUID.
    pub(crate) global: bool,
    pub(crate) owner_digest: [u8; 32],
    pub(crate) semantic_digest: [u8; 32],
    pub(crate) body: bytes::Bytes,
}

impl StateCell {
    pub(crate) fn deleted(&self) -> bool {
        matches!(self, Self::Tombstone)
    }
}

pub(crate) fn encode_state_key(key: StateKeyRef<'_>) -> Vec<u8> {
    let mut output = Vec::with_capacity(
        key.schema_key.len()
            + key.file_id.map_or(0, str::len)
            + 6
            + key.entity_pk.components.len() * 18,
    );
    write_key_string(&mut output, key.schema_key, KEY_PART_FINAL);
    write_entity_pk(&mut output, key.entity_pk);
    write_file_id(&mut output, key.file_id);
    output
}

/// Encodes the canonical contiguous lookup prefix for one entity identity.
/// File ownership is the final key component, so a SQL entity-PK predicate
/// can authenticate every matching file/global row without scanning unrelated
/// identities or maintaining a second index.
pub(crate) fn encode_state_entity_prefix(schema_key: &str, entity_pk: &EntityPk) -> Vec<u8> {
    let mut output = Vec::with_capacity(schema_key.len() + 4 + entity_pk.components.len() * 18);
    write_key_string(&mut output, schema_key, KEY_PART_FINAL);
    write_entity_pk(&mut output, entity_pk);
    output
}

/// Returns the strict lexicographic successor of a byte prefix. Carry is
/// propagated from the end, and an all-`0xff` prefix has no finite successor.
pub(crate) fn exclusive_prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    for index in (0..upper.len()).rev() {
        if upper[index] != u8::MAX {
            upper[index] += 1;
            upper.truncate(index + 1);
            return Some(upper);
        }
    }
    None
}

pub(crate) fn encode_state_entity_prefix_bounds(
    schema_key: &str,
    entity_pk: &EntityPk,
) -> CanonicalPrefixBounds {
    let lower = encode_state_entity_prefix(schema_key, entity_pk);
    let upper = exclusive_prefix_upper_bound(&lower);
    CanonicalPrefixBounds { lower, upper }
}

/// Accepts the strict successor emitted for a canonical entity prefix. The
/// successor is a valid storage boundary, but it is intentionally not itself
/// decodable as a state key/prefix because the increment truncates the
/// terminal key-part byte.
fn canonical_state_entity_prefix_upper_bound(bytes: &[u8]) -> Result<(), LixError> {
    let Some(last) = bytes.last().copied() else {
        return Err(state_error("state entity prefix upper bound is empty"));
    };
    if last == 0 {
        return Err(state_error(
            "state entity prefix upper bound has no predecessor",
        ));
    }
    let mut predecessor = bytes.to_vec();
    *predecessor
        .last_mut()
        .expect("non-empty upper bound has a last byte") -= 1;
    if exclusive_prefix_upper_bound(&predecessor).as_deref() != Some(bytes) {
        return Err(state_error(
            "state entity prefix upper bound is not canonical",
        ));
    }
    canonical_state_entity_prefix(&predecessor).map(|_| ())
}

/// Canonicalizes the state-key prefix emitted by
/// `encode_state_entity_prefix`.  Prefixes intentionally stop before the
/// file-id component, and the empty-PK form stops after the codec version;
/// neither is a complete `StateKey` accepted by `decode_state_key`.
fn canonical_state_entity_prefix(bytes: &[u8]) -> Result<Vec<u8>, LixError> {
    let mut offset = 0_usize;
    let (schema_key, terminator) = read_key_string(bytes, &mut offset, "state schema key")?;
    if terminator != KEY_PART_FINAL {
        return Err(state_error("state schema key has an invalid terminator"));
    }
    let version = *bytes
        .get(offset)
        .ok_or_else(|| state_error("state entity primary key prefix is truncated"))?;
    offset += 1;
    if version != ENTITY_PK_CODEC_V1 {
        return Err(state_error(format!(
            "state entity primary key has unsupported codec version {version}"
        )));
    }
    if offset == bytes.len() {
        return Ok(encode_state_entity_prefix(
            &schema_key,
            &EntityPk {
                components: crate::entity_pk::EntityPkComponents::Empty,
            },
        ));
    }

    let mut components = smallvec::SmallVec::new();
    loop {
        let (component, terminator) = read_entity_pk_part(bytes, &mut offset)?;
        components.push(component);
        if terminator == KEY_PART_FINAL {
            if offset != bytes.len() {
                return Err(state_error(
                    "state entity primary key prefix has trailing bytes",
                ));
            }
            let entity_pk = EntityPk::from_components(components).map_err(|error| {
                state_error(format!("state entity primary key is invalid: {error}"))
            })?;
            return Ok(encode_state_entity_prefix(&schema_key, &entity_pk));
        }
        if offset == bytes.len() {
            return Err(state_error("state entity primary key prefix is truncated"));
        }
    }
}

pub(crate) fn validate_state_entity_prefix(bytes: &[u8]) -> Result<(), LixError> {
    canonical_state_entity_prefix(bytes).map(|_| ())
}

fn write_entity_pk(output: &mut Vec<u8>, entity_pk: &EntityPk) {
    output.push(ENTITY_PK_CODEC_V1);
    for (index, component) in entity_pk.components.iter().enumerate() {
        let terminator = if index + 1 == entity_pk.components.len() {
            KEY_PART_FINAL
        } else {
            KEY_PART_MORE
        };
        match component {
            EntityPkComponent::Uuid(bytes) => {
                output.push(ENTITY_PK_UUID);
                output.extend_from_slice(bytes);
                output.push(terminator);
            }
            EntityPkComponent::Integer(value) => {
                output.push(ENTITY_PK_INTEGER);
                let ordered = u64::from_be_bytes(value.to_be_bytes()) ^ (1_u64 << 63);
                output.extend_from_slice(&ordered.to_be_bytes());
                output.push(terminator);
            }
            EntityPkComponent::String(value) => {
                output.push(ENTITY_PK_STRING);
                write_key_bytes(output, value.as_bytes(), terminator);
            }
            EntityPkComponent::Bytes(value) => {
                output.push(ENTITY_PK_BYTES);
                write_key_bytes(output, value, terminator);
            }
        }
    }
}

pub(crate) fn decode_state_key(bytes: &[u8]) -> Result<StateKey, LixError> {
    let mut offset = 0_usize;
    let (schema_key, terminator) = read_key_string(bytes, &mut offset, "state schema key")?;
    if terminator != KEY_PART_FINAL {
        return Err(state_error("state schema key has an invalid terminator"));
    }
    let entity_pk = read_entity_pk(bytes, &mut offset)?;
    let file_id = read_file_id(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(state_error("state key has trailing bytes"));
    }
    Ok(StateKey {
        schema_key,
        file_id,
        entity_pk,
    })
}

pub(crate) fn encode_state_value(value: StateValueRef) -> Result<Vec<u8>, LixError> {
    if value.pack_object_id == ObjectId::ZERO {
        return Err(state_error("state value pack object id is zero"));
    }
    let mut output = Vec::with_capacity(STATE_VALUE_MAGIC.len() + 32 + 4 + 1);
    output.extend_from_slice(STATE_VALUE_MAGIC);
    output.extend_from_slice(value.pack_object_id.as_bytes());
    output.extend_from_slice(&value.pack_ordinal.to_be_bytes());
    output.push(u8::from(value.tombstone));
    Ok(output)
}

pub(crate) fn decode_state_value(bytes: &[u8]) -> Result<StateValueRef, LixError> {
    let mut decoder = ValueDecoder::after_magic(bytes, STATE_VALUE_MAGIC, "state value")?;
    let pack_object_id = ObjectId::from_bytes(decoder.fixed_32("state pack object id")?);
    if pack_object_id == ObjectId::ZERO {
        return Err(state_error("state value pack object id is zero"));
    }
    let pack_ordinal = decoder.u32("state pack ordinal")?;
    let tombstone = match decoder.take(1, "state tombstone tag")?[0] {
        0 => false,
        1 => true,
        value => {
            return Err(state_error(format!(
                "state value tombstone tag {value} is invalid"
            )));
        }
    };
    decoder.finish("state value")?;
    Ok(StateValueRef {
        pack_object_id,
        pack_ordinal,
        tombstone,
    })
}

pub(super) fn encode_current_state_value(value: &StateValue) -> Result<Vec<u8>, LixError> {
    if value.change_id.as_uuid().is_nil() || value.commit_id.as_uuid().is_nil() {
        return Err(state_error(
            "current state value contains a nil change or commit id",
        ));
    }
    let mut output = Vec::new();
    output.extend_from_slice(CURRENT_STATE_VALUE_MAGIC);
    output.extend_from_slice(value.change_id.as_uuid().as_bytes());
    output.extend_from_slice(value.commit_id.as_uuid().as_bytes());
    output.extend_from_slice(&value.created_at.packed().to_be_bytes());
    output.extend_from_slice(&value.updated_at.packed().to_be_bytes());
    match &value.cell {
        StateCell::Value(_) => {
            return Err(state_error(
                "current state cannot encode the removed JSON row representation",
            ));
        }
        StateCell::Null => {
            return Err(state_error(
                "current state cannot encode the removed whole-row null representation",
            ));
        }
        StateCell::Tombstone => output.push(2),
        StateCell::NativeRow(value) => {
            output.push(3);
            output.extend_from_slice(&value.layout_id);
            output.push(u8::from(value.global));
            output.extend_from_slice(&value.owner_digest);
            output.extend_from_slice(&value.semantic_digest);
            put_bytes(&mut output, &value.body)?;
        }
    }
    put_optional_bytes(&mut output, value.metadata.as_deref().map(str::as_bytes))?;
    put_optional_bytes(&mut output, value.origin_key.as_deref().map(str::as_bytes))?;
    put_object_ids(&mut output, &value.blob_manifest_object_ids)?;
    Ok(output)
}

pub(super) fn decode_current_state_value(bytes: &[u8]) -> Result<StateValue, LixError> {
    let mut decoder =
        ValueDecoder::after_magic(bytes, CURRENT_STATE_VALUE_MAGIC, "current state value")?;
    let change_id = crate::changelog::ChangeId::new(uuid::Uuid::from_bytes(
        decoder.fixed_16("current state change id")?,
    ));
    let commit_id = crate::changelog::CommitId::new(uuid::Uuid::from_bytes(
        decoder.fixed_16("current state commit id")?,
    ));
    if change_id.as_uuid().is_nil() || commit_id.as_uuid().is_nil() {
        return Err(state_error(
            "current state value contains a nil change or commit id",
        ));
    }
    let created_at =
        LixTimestamp::from_packed(decoder.u64("current state created_at")?).map_err(state_error)?;
    let updated_at =
        LixTimestamp::from_packed(decoder.u64("current state updated_at")?).map_err(state_error)?;
    let cell = decoder.state_cell("current state cell")?;
    let metadata = decoder
        .optional_string("current state metadata")?
        .map(SharedStr::from);
    let origin_key = decoder.optional_string("current state origin key")?;
    let blob_manifest_object_ids = decoder.object_ids("current state blob manifests")?;
    decoder.finish("current state value")?;
    Ok(StateValue {
        change_id,
        commit_id,
        created_at,
        updated_at,
        cell,
        metadata,
        origin_key,
        blob_manifest_object_ids,
    })
}

fn put_optional_bytes(output: &mut Vec<u8>, value: Option<&[u8]>) -> Result<(), LixError> {
    match value {
        Some(value) => {
            output.push(1);
            put_bytes(output, value)
        }
        None => {
            output.push(0);
            Ok(())
        }
    }
}

fn put_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<(), LixError> {
    let length = u32::try_from(value.len())
        .map_err(|_| state_error("ForkTree current-state field exceeds u32 length"))?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value);
    Ok(())
}

fn put_object_ids(output: &mut Vec<u8>, values: &[ObjectId]) -> Result<(), LixError> {
    if values.len() > MAX_BLOB_ROOTS_PER_STATE_ROW {
        return Err(state_error(
            "ForkTree current-state row exceeds its authenticated blob-root edge bound",
        ));
    }
    output.extend_from_slice(
        &u32::try_from(values.len())
            .map_err(|_| state_error("current-state blob-root count exceeds u32"))?
            .to_be_bytes(),
    );
    for value in values {
        if *value == ObjectId::ZERO {
            return Err(state_error("current-state row contains a zero blob root"));
        }
        output.extend_from_slice(value.as_bytes());
    }
    Ok(())
}

fn write_file_id(output: &mut Vec<u8>, file_id: Option<&str>) {
    match file_id {
        None => output.push(FILE_ID_NONE),
        Some(file_id) => {
            output.push(FILE_ID_SOME);
            write_key_string(output, file_id, KEY_PART_FINAL);
        }
    }
}

fn write_key_string(output: &mut Vec<u8>, value: &str, terminator: u8) {
    write_key_bytes(output, value.as_bytes(), terminator);
}

fn write_key_bytes(output: &mut Vec<u8>, value: &[u8], terminator: u8) {
    for &byte in value {
        if byte == 0 {
            output.extend_from_slice(&[0, KEY_ESCAPE]);
        } else {
            output.push(byte);
        }
    }
    output.extend_from_slice(&[0, terminator]);
}

fn read_file_id(bytes: &[u8], offset: &mut usize) -> Result<Option<String>, LixError> {
    let tag = *bytes
        .get(*offset)
        .ok_or_else(|| state_error("state file id tag is truncated"))?;
    *offset += 1;
    match tag {
        FILE_ID_NONE => Ok(None),
        FILE_ID_SOME => {
            let (file_id, terminator) = read_key_string(bytes, offset, "state file id")?;
            if terminator != KEY_PART_FINAL {
                return Err(state_error("state file id has an invalid terminator"));
            }
            Ok(Some(file_id))
        }
        other => Err(state_error(format!(
            "state file id has unknown tag {other}"
        ))),
    }
}

fn read_entity_pk(bytes: &[u8], offset: &mut usize) -> Result<EntityPk, LixError> {
    let version = bytes
        .get(*offset)
        .copied()
        .ok_or_else(|| state_error("state entity primary key is truncated"))?;
    *offset += 1;
    if version != ENTITY_PK_CODEC_V1 {
        return Err(state_error(format!(
            "state entity primary key has unsupported codec version {version}"
        )));
    }
    let mut components = smallvec::SmallVec::new();
    loop {
        let (component, terminator) = read_entity_pk_part(bytes, offset)?;
        components.push(component);
        if terminator == KEY_PART_FINAL {
            break;
        }
    }
    EntityPk::from_components(components)
        .map_err(|error| state_error(format!("state entity primary key is invalid: {error}")))
}

fn read_entity_pk_part(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<(EntityPkComponent, u8), LixError> {
    let tag = *bytes
        .get(*offset)
        .ok_or_else(|| state_error("state entity primary-key part tag is truncated"))?;
    *offset += 1;
    match tag {
        ENTITY_PK_STRING => read_key_string(bytes, offset, "state string primary-key part")
            .map(|(value, terminator)| (EntityPkComponent::String(value.into()), terminator)),
        ENTITY_PK_BYTES => read_key_bytes(bytes, offset, "state bytes primary-key part")
            .map(|(value, terminator)| (EntityPkComponent::Bytes(value.into()), terminator)),
        ENTITY_PK_UUID => {
            let end = offset
                .checked_add(16)
                .ok_or_else(|| state_error("state UUID primary-key part overflows"))?;
            let value = bytes
                .get(*offset..end)
                .ok_or_else(|| state_error("state UUID primary-key part is truncated"))?
                .try_into()
                .expect("UUID slice has fixed length");
            let terminator = read_terminator(bytes, end, "state UUID primary-key part")?;
            *offset = end + 1;
            Ok((EntityPkComponent::Uuid(value), terminator))
        }
        ENTITY_PK_INTEGER => {
            let end = offset
                .checked_add(8)
                .ok_or_else(|| state_error("state integer primary-key part overflows"))?;
            let ordered = u64::from_be_bytes(
                bytes
                    .get(*offset..end)
                    .ok_or_else(|| state_error("state integer primary-key part is truncated"))?
                    .try_into()
                    .expect("integer slice has fixed length"),
            );
            let terminator = read_terminator(bytes, end, "state integer primary-key part")?;
            *offset = end + 1;
            Ok((
                EntityPkComponent::Integer(i64::from_be_bytes(
                    (ordered ^ (1_u64 << 63)).to_be_bytes(),
                )),
                terminator,
            ))
        }
        other => Err(state_error(format!(
            "state entity primary-key part has unknown tag {other}"
        ))),
    }
}

fn read_terminator(bytes: &[u8], offset: usize, field: &str) -> Result<u8, LixError> {
    match bytes
        .get(offset)
        .copied()
        .ok_or_else(|| state_error(format!("{field} terminator is truncated")))?
    {
        KEY_PART_FINAL => Ok(KEY_PART_FINAL),
        KEY_PART_MORE => Ok(KEY_PART_MORE),
        other => Err(state_error(format!(
            "{field} has invalid terminator {other}"
        ))),
    }
}

fn read_key_string(
    bytes: &[u8],
    offset: &mut usize,
    field: &str,
) -> Result<(String, u8), LixError> {
    let (value, terminator) = read_key_bytes_cow(bytes, offset, field)?;
    let value = match value {
        Cow::Borrowed(value) => std::str::from_utf8(value)
            .map(str::to_owned)
            .map_err(|_| state_error(format!("{field} is not UTF-8")))?,
        Cow::Owned(value) => {
            String::from_utf8(value).map_err(|_| state_error(format!("{field} is not UTF-8")))?
        }
    };
    Ok((value, terminator))
}

fn read_key_bytes(
    bytes: &[u8],
    offset: &mut usize,
    field: &str,
) -> Result<(Vec<u8>, u8), LixError> {
    read_key_bytes_cow(bytes, offset, field)
        .map(|(value, terminator)| (value.into_owned(), terminator))
}

fn read_key_bytes_cow<'a>(
    bytes: &'a [u8],
    offset: &mut usize,
    field: &str,
) -> Result<(Cow<'a, [u8]>, u8), LixError> {
    let start = *offset;
    let mut segment_start = start;
    let mut decoded: Option<Vec<u8>> = None;
    loop {
        let tail = bytes
            .get(segment_start..)
            .ok_or_else(|| state_error(format!("{field} is truncated")))?;
        let relative_zero =
            memchr::memchr(0, tail).ok_or_else(|| state_error(format!("{field} is truncated")))?;
        let zero = segment_start + relative_zero;
        let escape = *bytes
            .get(zero + 1)
            .ok_or_else(|| state_error(format!("{field} escape is truncated")))?;
        *offset = zero + 2;
        match escape {
            KEY_ESCAPE => {
                let output = decoded.get_or_insert_with(|| {
                    Vec::with_capacity(zero.saturating_sub(start).saturating_add(16))
                });
                output.extend_from_slice(&bytes[segment_start..zero]);
                output.push(0);
                segment_start = *offset;
            }
            KEY_PART_FINAL | KEY_PART_MORE => {
                let value = decoded.map_or_else(
                    || Cow::Borrowed(&bytes[start..zero]),
                    |mut output| {
                        output.extend_from_slice(&bytes[segment_start..zero]);
                        Cow::Owned(output)
                    },
                );
                return Ok((value, escape));
            }
            other => return Err(state_error(format!("{field} has unknown escape {other}"))),
        }
    }
}

struct ValueDecoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> ValueDecoder<'a> {
    fn after_magic(bytes: &'a [u8], magic: &[u8], field: &str) -> Result<Self, LixError> {
        if !bytes.starts_with(magic) {
            return Err(state_error(format!("{field} magic or version is invalid")));
        }
        Ok(Self {
            bytes,
            offset: magic.len(),
        })
    }

    fn fixed_32(&mut self, field: &str) -> Result<[u8; 32], LixError> {
        Ok(self
            .take(32, field)?
            .try_into()
            .expect("decoder returned fixed object-id width"))
    }

    fn fixed_16(&mut self, field: &str) -> Result<[u8; 16], LixError> {
        Ok(self
            .take(16, field)?
            .try_into()
            .expect("decoder returned fixed UUID width"))
    }

    fn u32(&mut self, field: &str) -> Result<u32, LixError> {
        Ok(u32::from_be_bytes(
            self.take(4, field)?
                .try_into()
                .expect("decoder returned fixed u32 width"),
        ))
    }

    fn u64(&mut self, field: &str) -> Result<u64, LixError> {
        Ok(u64::from_be_bytes(
            self.take(8, field)?
                .try_into()
                .expect("decoder returned fixed u64 width"),
        ))
    }

    fn state_cell(&mut self, field: &str) -> Result<StateCell, LixError> {
        match self.take(1, field)?[0] {
            0 => Err(state_error(format!(
                "{field} uses the removed JSON current-state representation"
            ))),
            1 => Err(state_error(format!(
                "{field} uses the removed whole-row null current-state representation"
            ))),
            2 => Ok(StateCell::Tombstone),
            3 => {
                let layout_id = self.fixed_32(field)?;
                let global = match self.take(1, field)?[0] {
                    0 => false,
                    1 => true,
                    _ => return Err(state_error(format!("{field} has invalid owner domain"))),
                };
                let owner_digest = self.fixed_32(field)?;
                let semantic_digest = self.fixed_32(field)?;
                let length = self.u32(field)? as usize;
                let body = bytes::Bytes::copy_from_slice(self.take(length, field)?);
                Ok(StateCell::NativeRow(NativeRowCell {
                    layout_id,
                    global,
                    owner_digest,
                    semantic_digest,
                    body,
                }))
            }
            other => Err(state_error(format!("{field} has invalid tag {other}"))),
        }
    }

    fn optional_string(&mut self, field: &str) -> Result<Option<String>, LixError> {
        match self.take(1, field)?[0] {
            0 => Ok(None),
            1 => {
                let length = self.u32(field)? as usize;
                std::str::from_utf8(self.take(length, field)?)
                    .map(str::to_owned)
                    .map(Some)
                    .map_err(|_| state_error(format!("{field} is not UTF-8")))
            }
            other => Err(state_error(format!(
                "{field} has invalid option tag {other}"
            ))),
        }
    }

    fn object_ids(&mut self, field: &str) -> Result<Vec<ObjectId>, LixError> {
        let count = self.u32(field)? as usize;
        if count > MAX_BLOB_ROOTS_PER_STATE_ROW
            || count > self.bytes.len().saturating_sub(self.offset) / 32
        {
            return Err(state_error(format!("{field} count exceeds encoded body")));
        }
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            let value = ObjectId::from_bytes(self.fixed_32(field)?);
            if value == ObjectId::ZERO {
                return Err(state_error(format!("{field} contains a zero object id")));
            }
            values.push(value);
        }
        Ok(values)
    }

    fn take(&mut self, length: usize, field: &str) -> Result<&'a [u8], LixError> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| state_error(format!("{field} is truncated")))?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn finish(self, field: &str) -> Result<(), LixError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(state_error(format!("{field} has trailing bytes")))
        }
    }
}

fn state_error(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INTERNAL_ERROR, message.into())
}

#[cfg(test)]
mod current_state_v2_tests {
    use super::*;

    fn native_value() -> StateValue {
        StateValue {
            change_id: crate::changelog::ChangeId::new(
                uuid::Uuid::parse_str("01950000-0000-7000-8000-000000000001").unwrap(),
            ),
            commit_id: crate::changelog::CommitId::new(
                uuid::Uuid::parse_str("01950000-0000-7000-8000-000000000002").unwrap(),
            ),
            created_at: LixTimestamp::expect_parse("created_at", "2026-01-01T00:00:00.000Z"),
            updated_at: LixTimestamp::expect_parse("updated_at", "2026-01-01T00:00:00.001Z"),
            cell: StateCell::NativeRow(NativeRowCell {
                layout_id: [1; 32],
                global: false,
                owner_digest: [2; 32],
                semantic_digest: [3; 32],
                body: bytes::Bytes::from_static(b"native-body"),
            }),
            metadata: None,
            origin_key: None,
            blob_manifest_object_ids: Vec::new(),
        }
    }

    #[test]
    fn current_state_v2_rejects_v1_unknown_domain_truncation_and_trailing_bytes() {
        let encoded = encode_current_state_value(&native_value()).expect("v2 encodes");
        assert_eq!(decode_current_state_value(&encoded).unwrap(), native_value());

        let mut v1 = encoded.clone();
        v1[7] = 1;
        assert!(decode_current_state_value(&v1).is_err());

        // magic + change + commit + created_at + updated_at + cell tag + layout
        let domain_offset = 8 + 16 + 16 + 8 + 8 + 1 + 32;
        let mut unknown_domain = encoded.clone();
        unknown_domain[domain_offset] = 2;
        assert!(decode_current_state_value(&unknown_domain).is_err());

        assert!(decode_current_state_value(&encoded[..encoded.len() - 1]).is_err());
        let mut trailing = encoded;
        trailing.push(0);
        assert!(decode_current_state_value(&trailing).is_err());
    }

    #[test]
    fn current_state_v2_rejects_removed_value_and_null_tags() {
        let encoded = encode_current_state_value(&native_value()).expect("v2 encodes");
        let cell_tag_offset = 8 + 16 + 16 + 8 + 8;
        for tag in [0, 1] {
            let mut removed = encoded.clone();
            removed[cell_tag_offset] = tag;
            assert!(decode_current_state_value(&removed).is_err());
        }
    }
}
