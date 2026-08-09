use std::borrow::Cow;

use crate::LixError;
use crate::common::{LixTimestamp, SharedStr};
use crate::entity_pk::{EntityPk, EntityPkComponent};
use crate::storage::StorageSpace;

use super::model::CanonicalBranchId;
use super::object::ObjectId;

// A state-tree leaf contains at most 64 rows. Four roots per row keeps the
// authenticated leaf edge page at or below the owner-wide 256-edge bound.
const MAX_BLOB_ROOTS_PER_STATE_ROW: usize = 4;

const STATE_VALUE_MAGIC: &[u8; 8] = b"LIXFTV\0\x01";
const UNTRACKED_KEY_MAGIC: &[u8; 8] = b"LIXFTU\0\x01";
const UNTRACKED_VALUE_MAGIC: &[u8; 8] = b"LIXFTW\0\x01";

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

/// Mutable authority retained only for rows whose schema explicitly declares
/// them untracked. Tracked rows, selectors, catalogs, and roots are forbidden
/// from this space.
pub(crate) const UNTRACKED_ROW_SPACE: StorageSpace = StorageSpace::engine_declared(
    0x0009_0003,
    "forktree.untracked_row.v1",
    crate::storage::ValueSemantics::Mutable,
);

pub(crate) fn untracked_owner_prefix(branch_id: CanonicalBranchId) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(UNTRACKED_KEY_MAGIC.len() + 16);
    prefix.extend_from_slice(UNTRACKED_KEY_MAGIC);
    prefix.extend_from_slice(branch_id.as_bytes());
    prefix
}

#[derive(Clone, Copy, Debug)]
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

#[derive(Clone, Copy, Debug)]
pub(crate) struct StateValueRef<'a> {
    pub(crate) change_id: crate::changelog::ChangeId,
    pub(crate) commit_id: crate::changelog::CommitId,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
    pub(crate) cell: StateCellRef<'a>,
    pub(crate) metadata: Option<&'a str>,
    pub(crate) origin_key: Option<&'a str>,
    pub(crate) blob_manifest_object_ids: &'a [ObjectId],
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
    Null,
    Tombstone,
}

impl StateCell {
    pub(crate) fn deleted(&self) -> bool {
        matches!(self, Self::Tombstone)
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct UntrackedValueRef<'a> {
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
    pub(crate) cell: StateCellRef<'a>,
    pub(crate) metadata: Option<&'a str>,
    pub(crate) origin_key: Option<&'a str>,
    pub(crate) blob_manifest_object_ids: &'a [ObjectId],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct UntrackedValue {
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
    pub(crate) cell: StateCell,
    pub(crate) metadata: Option<SharedStr>,
    pub(crate) origin_key: Option<String>,
    pub(crate) blob_manifest_object_ids: Vec<ObjectId>,
}

pub(crate) fn encode_state_key(key: StateKeyRef<'_>) -> Vec<u8> {
    let mut output = Vec::with_capacity(
        key.schema_key.len()
            + key.file_id.map_or(0, str::len)
            + 6
            + key.entity_pk.components.len() * 18,
    );
    write_key_string(&mut output, key.schema_key, KEY_PART_FINAL);
    write_file_id(&mut output, key.file_id);
    output.push(ENTITY_PK_CODEC_V1);
    for (index, component) in key.entity_pk.components.iter().enumerate() {
        let terminator = if index + 1 == key.entity_pk.components.len() {
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
                write_key_bytes(&mut output, value.as_bytes(), terminator);
            }
            EntityPkComponent::Bytes(value) => {
                output.push(ENTITY_PK_BYTES);
                write_key_bytes(&mut output, value, terminator);
            }
        }
    }
    output
}

pub(crate) fn encode_state_prefix(schema_key: &str, file_id: Option<&str>) -> Vec<u8> {
    let mut output = Vec::with_capacity(schema_key.len() + file_id.map_or(1, str::len) + 4);
    write_key_string(&mut output, schema_key, KEY_PART_FINAL);
    write_file_id(&mut output, file_id);
    output
}

pub(crate) fn decode_state_key(bytes: &[u8]) -> Result<StateKey, LixError> {
    let mut offset = 0_usize;
    let (schema_key, terminator) = read_key_string(bytes, &mut offset, "state schema key")?;
    if terminator != KEY_PART_FINAL {
        return Err(state_error("state schema key has an invalid terminator"));
    }
    let file_id = read_file_id(bytes, &mut offset)?;
    let entity_pk = read_entity_pk(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(state_error("state key has trailing bytes"));
    }
    Ok(StateKey {
        schema_key,
        file_id,
        entity_pk,
    })
}

pub(crate) fn encode_state_value(value: StateValueRef<'_>) -> Result<Vec<u8>, LixError> {
    let mut output = Vec::with_capacity(
        STATE_VALUE_MAGIC.len()
            + 16
            + 16
            + 17
            + match value.cell {
                StateCellRef::Value(value) => 5 + value.len(),
                StateCellRef::Null | StateCellRef::Tombstone => 1,
            }
            + value.metadata.map_or(1, |value| 5 + value.len())
            + value.origin_key.map_or(1, |value| 5 + value.len()),
    );
    output.extend_from_slice(STATE_VALUE_MAGIC);
    output.extend_from_slice(value.change_id.as_uuid().as_bytes());
    output.extend_from_slice(value.commit_id.as_uuid().as_bytes());
    output.extend_from_slice(&value.created_at.packed().to_be_bytes());
    output.extend_from_slice(&value.updated_at.packed().to_be_bytes());
    put_state_cell(&mut output, value.cell)?;
    put_optional_bytes(&mut output, value.metadata.map(str::as_bytes))?;
    put_optional_bytes(&mut output, value.origin_key.map(str::as_bytes))?;
    put_object_ids(&mut output, value.blob_manifest_object_ids)?;
    Ok(output)
}

pub(crate) fn decode_state_value(bytes: &[u8]) -> Result<StateValue, LixError> {
    let mut decoder = ValueDecoder::after_magic(bytes, STATE_VALUE_MAGIC, "state value")?;
    let change_id = crate::changelog::ChangeId::new(uuid::Uuid::from_bytes(
        decoder.fixed_16("state change id")?,
    ));
    let commit_id = crate::changelog::CommitId::new(uuid::Uuid::from_bytes(
        decoder.fixed_16("state commit id")?,
    ));
    let created_at = LixTimestamp::from_packed(decoder.u64("state created_at")?)
        .map_err(|error| state_error(error.to_string()))?;
    let updated_at = LixTimestamp::from_packed(decoder.u64("state updated_at")?)
        .map_err(|error| state_error(error.to_string()))?;
    let cell = decoder.state_cell("state cell")?;
    let metadata = decoder
        .optional_string("state metadata")?
        .map(SharedStr::from);
    let origin_key = decoder.optional_string("state origin key")?;
    let blob_manifest_object_ids = decoder.object_ids("state blob manifests")?;
    decoder.finish("state value")?;
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

pub(crate) fn encode_untracked_key(branch_id: CanonicalBranchId, key: StateKeyRef<'_>) -> Vec<u8> {
    let state_key = encode_state_key(key);
    let mut output = Vec::with_capacity(UNTRACKED_KEY_MAGIC.len() + 16 + state_key.len());
    output.extend_from_slice(UNTRACKED_KEY_MAGIC);
    output.extend_from_slice(branch_id.as_bytes());
    output.extend_from_slice(&state_key);
    output
}

pub(crate) fn decode_untracked_key(
    bytes: &[u8],
) -> Result<(CanonicalBranchId, StateKey), LixError> {
    if !bytes.starts_with(UNTRACKED_KEY_MAGIC) || bytes.len() < UNTRACKED_KEY_MAGIC.len() + 16 {
        return Err(state_error("untracked key magic or version is invalid"));
    }
    let offset = UNTRACKED_KEY_MAGIC.len() + 16;
    let branch_id = CanonicalBranchId::from_bytes(
        bytes[UNTRACKED_KEY_MAGIC.len()..offset]
            .try_into()
            .map_err(|_| state_error("untracked branch id is not 16 bytes"))?,
    );
    let state_key = decode_state_key(&bytes[offset..])?;
    Ok((branch_id, state_key))
}

pub(crate) fn encode_untracked_value(value: UntrackedValueRef<'_>) -> Result<Vec<u8>, LixError> {
    let mut output = Vec::new();
    output.extend_from_slice(UNTRACKED_VALUE_MAGIC);
    output.extend_from_slice(&value.created_at.packed().to_be_bytes());
    output.extend_from_slice(&value.updated_at.packed().to_be_bytes());
    put_state_cell(&mut output, value.cell)?;
    put_optional_bytes(&mut output, value.metadata.map(str::as_bytes))?;
    put_optional_bytes(&mut output, value.origin_key.map(str::as_bytes))?;
    put_object_ids(&mut output, value.blob_manifest_object_ids)?;
    Ok(output)
}

pub(crate) fn decode_untracked_value(bytes: &[u8]) -> Result<UntrackedValue, LixError> {
    let mut decoder = ValueDecoder::after_magic(bytes, UNTRACKED_VALUE_MAGIC, "untracked value")?;
    let created_at = LixTimestamp::from_packed(decoder.u64("untracked created_at")?)
        .map_err(|error| state_error(error.to_string()))?;
    let updated_at = LixTimestamp::from_packed(decoder.u64("untracked updated_at")?)
        .map_err(|error| state_error(error.to_string()))?;
    let cell = decoder.state_cell("untracked cell")?;
    let metadata = decoder
        .optional_string("untracked metadata")?
        .map(SharedStr::from);
    let origin_key = decoder.optional_string("untracked origin key")?;
    let blob_manifest_object_ids = decoder.object_ids("untracked blob manifests")?;
    decoder.finish("untracked value")?;
    Ok(UntrackedValue {
        created_at,
        updated_at,
        cell,
        metadata,
        origin_key,
        blob_manifest_object_ids,
    })
}

fn put_state_cell(output: &mut Vec<u8>, value: StateCellRef<'_>) -> Result<(), LixError> {
    match value {
        StateCellRef::Value(value) => {
            output.push(0);
            put_bytes(output, value.as_bytes())
        }
        StateCellRef::Null => {
            output.push(1);
            Ok(())
        }
        StateCellRef::Tombstone => {
            output.push(2);
            Ok(())
        }
    }
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
        .map_err(|_| state_error("ForkTree state field exceeds u32 length"))?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value);
    Ok(())
}

fn put_object_ids(output: &mut Vec<u8>, values: &[ObjectId]) -> Result<(), LixError> {
    if values.len() > MAX_BLOB_ROOTS_PER_STATE_ROW {
        return Err(state_error(
            "ForkTree state row exceeds its authenticated blob-root edge bound",
        ));
    }
    let count = u32::try_from(values.len())
        .map_err(|_| state_error("ForkTree state blob-root count exceeds u32"))?;
    output.extend_from_slice(&count.to_be_bytes());
    for value in values {
        if *value == ObjectId::ZERO {
            return Err(state_error("ForkTree state contains a zero blob root"));
        }
        output.extend_from_slice(value.as_bytes());
    }
    Ok(())
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

    fn fixed_16(&mut self, field: &str) -> Result<[u8; 16], LixError> {
        Ok(self
            .take(16, field)?
            .try_into()
            .expect("decoder returned fixed UUID width"))
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
            0 => {
                let length = u32::from_be_bytes(
                    self.take(4, field)?
                        .try_into()
                        .expect("decoder returned fixed u32 width"),
                ) as usize;
                std::str::from_utf8(self.take(length, field)?)
                    .map(SharedStr::from)
                    .map(StateCell::Value)
                    .map_err(|_| state_error(format!("{field} value is not UTF-8")))
            }
            1 => Ok(StateCell::Null),
            2 => Ok(StateCell::Tombstone),
            other => Err(state_error(format!("{field} has invalid tag {other}"))),
        }
    }

    fn optional_string(&mut self, field: &str) -> Result<Option<String>, LixError> {
        match self.take(1, field)?[0] {
            0 => Ok(None),
            1 => {
                let length = u32::from_be_bytes(
                    self.take(4, field)?
                        .try_into()
                        .expect("decoder returned fixed u32 width"),
                ) as usize;
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
        let count = u32::from_be_bytes(
            self.take(4, field)?
                .try_into()
                .expect("decoder returned fixed u32 width"),
        ) as usize;
        if count > MAX_BLOB_ROOTS_PER_STATE_ROW
            || count > self.bytes.len().saturating_sub(self.offset) / 32
        {
            return Err(state_error(format!("{field} count exceeds encoded body")));
        }
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            let value = ObjectId::from_bytes(
                self.take(32, field)?
                    .try_into()
                    .expect("decoder returned fixed object-id width"),
            );
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
