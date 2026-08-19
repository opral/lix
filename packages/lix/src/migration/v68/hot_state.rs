//! Frozen decoder for protocol-v68 hot-state values.
//!
//! Protocol v68 wrote tracked-head values with `HEAD_VALUE_VERSION == 9`.
//! The live codec is allowed to evolve, so migration reads decode into the
//! owned types in this module rather than borrowing from, or constructing,
//! the current hot-state implementation's private wire views.

use crate::changelog::{ChangeId, CommitId};
use crate::common::{LixError, LixTimestamp};
use crate::json_store::JsonRef;

const HEAD_VALUE_VERSION: u8 = 9;
const HEAD_VALUE_HEADER_BYTES: usize = 59;
const UUID_BYTES: usize = 16;
const JSON_REF_BYTES: usize = 32;

const HEAD_VALUE_DELETED: u8 = 0b0000_0001;
const HEAD_VALUE_SNAPSHOT_SHIFT: u8 = 1;
const HEAD_VALUE_METADATA_SHIFT: u8 = 3;
const HEAD_VALUE_UNTRACKED: u8 = 0b0010_0000;
const HEAD_VALUE_WORKING_DIFF_SHIFT: u8 = 6;
const TWO_BIT_MASK: u8 = 0b11;

const HEAD_SLOT_NONE: u8 = 0;
const HEAD_SLOT_REF: u8 = 1;
const HEAD_SLOT_INLINE: u8 = 2;
const HEAD_SLOT_INLINE_FINGERPRINTED: u8 = 3;

const HEAD_WORKING_DIFF_DISABLED: u8 = 0;
const HEAD_WORKING_DIFF_CLEAN: u8 = 1;
const HEAD_WORKING_DIFF_BEFORE_ABSENT: u8 = 2;
const HEAD_WORKING_DIFF_BEFORE_PRESENT: u8 = 3;

const WORKING_DIFF_SLOT_NONE: u8 = 0;
const WORKING_DIFF_SLOT_REF: u8 = 1;
const WORKING_DIFF_SLOT_INLINE: u8 = 2;
const WORKING_DIFF_SLOT_UNRESOLVED: u8 = 3;
const WORKING_DIFF_VERSION_BYTES: usize =
    UUID_BYTES + UUID_BYTES + 1 + 8 + 8 + 1 + JSON_REF_BYTES + 1 + JSON_REF_BYTES;

/// Owned, runtime-neutral form of one v68 tracked-head value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::migration) struct V68HotStateValue {
    pub(in crate::migration) change_id: ChangeId,
    pub(in crate::migration) commit_id: Option<CommitId>,
    pub(in crate::migration) untracked: bool,
    pub(in crate::migration) deleted: bool,
    pub(in crate::migration) created_at: LixTimestamp,
    pub(in crate::migration) updated_at: LixTimestamp,
    pub(in crate::migration) snapshot: V68HotStateSlot,
    pub(in crate::migration) metadata: V68HotStateSlot,
    pub(in crate::migration) columnar_base_coordinate: Option<V68ColumnarBaseCoordinate>,
    pub(in crate::migration) working_diff_baseline: V68WorkingDiffBaseline,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::migration) enum V68HotStateSlot {
    None,
    Ref(JsonRef),
    Inline(String),
    InlineFingerprinted { json_ref: JsonRef, json: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::migration) struct V68ColumnarBaseCoordinate {
    pub(in crate::migration) base_commit_id: CommitId,
    pub(in crate::migration) group_index: u32,
    pub(in crate::migration) row_index: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::migration) enum V68WorkingDiffBaseline {
    Disabled,
    Clean,
    BeforeAbsent {
        checkpoint_commit_id: CommitId,
    },
    BeforePresent {
        checkpoint_commit_id: CommitId,
        version: V68WorkingDiffVersion,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::migration) struct V68WorkingDiffVersion {
    pub(in crate::migration) change_id: ChangeId,
    pub(in crate::migration) commit_id: CommitId,
    pub(in crate::migration) deleted: bool,
    pub(in crate::migration) created_at: LixTimestamp,
    pub(in crate::migration) updated_at: LixTimestamp,
    pub(in crate::migration) snapshot: V68WorkingDiffSlot,
    pub(in crate::migration) metadata: V68WorkingDiffSlot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::migration) enum V68WorkingDiffSlot {
    None,
    Ref(JsonRef),
    Inline(JsonRef),
    Unresolved,
}

/// Decode the exact version-9 tracked-head value written by protocol v68.
pub(in crate::migration) fn decode_hot_state_value(
    bytes: &[u8],
) -> Result<V68HotStateValue, LixError> {
    if bytes.len() < HEAD_VALUE_HEADER_BYTES {
        return Err(hot_state_error("row is shorter than the v9 fixed header"));
    }
    if bytes[0] != HEAD_VALUE_VERSION {
        return Err(hot_state_error(format!(
            "unsupported row format version {}",
            bytes[0]
        )));
    }

    let flags = bytes[1];
    let snapshot_kind = (flags >> HEAD_VALUE_SNAPSHOT_SHIFT) & TWO_BIT_MASK;
    let metadata_kind = (flags >> HEAD_VALUE_METADATA_SHIFT) & TWO_BIT_MASK;
    let change_uuid = read_uuid(&bytes[2..18], "change id")?;
    let commit_uuid = read_uuid(&bytes[18..34], "commit id")?;
    let created_at = read_timestamp(&bytes[34..42], "created_at")?;
    let updated_at = read_timestamp(&bytes[42..50], "updated_at")?;
    let snapshot_len = usize::try_from(read_u32(&bytes[50..54], "snapshot length")?)
        .map_err(|_| hot_state_error("snapshot length exceeds usize"))?;
    let metadata_len = usize::try_from(read_u32(&bytes[54..58], "metadata length")?)
        .map_err(|_| hot_state_error("metadata length exceeds usize"))?;
    let has_columnar_base_coordinate = match bytes[58] {
        0 => false,
        1 => true,
        _ => return Err(hot_state_error("invalid columnar base-coordinate tag")),
    };

    let snapshot_end = HEAD_VALUE_HEADER_BYTES
        .checked_add(snapshot_len)
        .ok_or_else(|| hot_state_error("snapshot payload length overflow"))?;
    let metadata_end = snapshot_end
        .checked_add(metadata_len)
        .ok_or_else(|| hot_state_error("metadata payload length overflow"))?;
    let snapshot_bytes = bytes
        .get(HEAD_VALUE_HEADER_BYTES..snapshot_end)
        .ok_or_else(|| hot_state_error("snapshot payload is truncated"))?;
    let metadata_bytes = bytes
        .get(snapshot_end..metadata_end)
        .ok_or_else(|| hot_state_error("metadata payload is truncated"))?;
    let snapshot = decode_slot(snapshot_kind, snapshot_bytes, "snapshot")?;
    let metadata = decode_slot(metadata_kind, metadata_bytes, "metadata")?;

    let mut offset = metadata_end;
    let working_diff_baseline = match (flags >> HEAD_VALUE_WORKING_DIFF_SHIFT) & TWO_BIT_MASK {
        HEAD_WORKING_DIFF_DISABLED => V68WorkingDiffBaseline::Disabled,
        HEAD_WORKING_DIFF_CLEAN => V68WorkingDiffBaseline::Clean,
        HEAD_WORKING_DIFF_BEFORE_ABSENT => V68WorkingDiffBaseline::BeforeAbsent {
            checkpoint_commit_id: read_commit_id(bytes, &mut offset, "checkpoint commit id")?,
        },
        HEAD_WORKING_DIFF_BEFORE_PRESENT => V68WorkingDiffBaseline::BeforePresent {
            checkpoint_commit_id: read_commit_id(bytes, &mut offset, "checkpoint commit id")?,
            version: decode_working_diff_version(bytes, &mut offset)?,
        },
        _ => unreachable!("two-bit working-diff tag is exhaustive"),
    };

    let columnar_base_coordinate = if has_columnar_base_coordinate {
        let base_commit_id = read_commit_id(bytes, &mut offset, "columnar base commit id")?;
        if base_commit_id.as_uuid().is_nil() {
            return Err(hot_state_error(
                "columnar base coordinate has a nil owner commit",
            ));
        }
        let group_index = read_u32(
            take(bytes, &mut offset, 4, "columnar base group index")?,
            "columnar base group index",
        )?;
        let row_index = read_u32(
            take(bytes, &mut offset, 4, "columnar base row index")?,
            "columnar base row index",
        )?;
        Some(V68ColumnarBaseCoordinate {
            base_commit_id,
            group_index,
            row_index,
        })
    } else {
        None
    };

    if offset != bytes.len() {
        return Err(hot_state_error(
            "row payload lengths do not match the buffer",
        ));
    }

    let deleted = flags & HEAD_VALUE_DELETED != 0;
    let untracked = flags & HEAD_VALUE_UNTRACKED != 0;
    if deleted && (snapshot != V68HotStateSlot::None || metadata != V68HotStateSlot::None) {
        return Err(hot_state_error(
            "deleted current-state rows must not carry JSON payloads",
        ));
    }
    let (change_id, commit_id) = if untracked {
        if deleted {
            return Err(hot_state_error(
                "untracked current-state rows must be deleted physically",
            ));
        }
        if change_uuid.is_nil() || !commit_uuid.is_nil() {
            return Err(hot_state_error(
                "untracked current-state rows must use a non-nil change id and a nil commit id",
            ));
        }
        if working_diff_baseline != V68WorkingDiffBaseline::Disabled {
            return Err(hot_state_error(
                "untracked current-state rows must not carry a working-diff baseline",
            ));
        }
        if columnar_base_coordinate.is_some() {
            return Err(hot_state_error(
                "untracked current-state rows must not carry a columnar base coordinate",
            ));
        }
        (ChangeId::new(change_uuid), None)
    } else {
        if change_uuid.is_nil() || commit_uuid.is_nil() {
            return Err(hot_state_error(
                "tracked current-state rows must use non-nil change and commit ids",
            ));
        }
        (ChangeId::new(change_uuid), Some(CommitId::new(commit_uuid)))
    };

    Ok(V68HotStateValue {
        change_id,
        commit_id,
        untracked,
        deleted,
        created_at,
        updated_at,
        snapshot,
        metadata,
        columnar_base_coordinate,
        working_diff_baseline,
    })
}

fn decode_slot(kind: u8, bytes: &[u8], field: &str) -> Result<V68HotStateSlot, LixError> {
    match kind {
        HEAD_SLOT_NONE if bytes.is_empty() => Ok(V68HotStateSlot::None),
        HEAD_SLOT_NONE => Err(hot_state_error(format!(
            "{field} none slot must have an empty payload"
        ))),
        HEAD_SLOT_REF if bytes.len() == JSON_REF_BYTES => Ok(V68HotStateSlot::Ref(
            JsonRef::from_hash_bytes(bytes.try_into().expect("length was checked")),
        )),
        HEAD_SLOT_REF => Err(hot_state_error(format!(
            "{field} ref payload must have {JSON_REF_BYTES} bytes"
        ))),
        HEAD_SLOT_INLINE => String::from_utf8(bytes.to_vec())
            .map(V68HotStateSlot::Inline)
            .map_err(|error| {
                hot_state_error(format!("{field} inline payload is not UTF-8: {error}"))
            }),
        HEAD_SLOT_INLINE_FINGERPRINTED if bytes.len() >= JSON_REF_BYTES => {
            let (hash, json) = bytes.split_at(JSON_REF_BYTES);
            let json_ref = JsonRef::from_hash_bytes(hash.try_into().expect("length was checked"));
            let json = String::from_utf8(json.to_vec()).map_err(|error| {
                hot_state_error(format!("{field} inline payload is not UTF-8: {error}"))
            })?;
            Ok(V68HotStateSlot::InlineFingerprinted { json_ref, json })
        }
        HEAD_SLOT_INLINE_FINGERPRINTED => Err(hot_state_error(format!(
            "{field} inline payload is shorter than its {JSON_REF_BYTES}-byte fingerprint"
        ))),
        _ => Err(hot_state_error(format!(
            "{field} has an unknown slot kind {kind}"
        ))),
    }
}

fn decode_working_diff_version(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<V68WorkingDiffVersion, LixError> {
    let payload = take(
        bytes,
        offset,
        WORKING_DIFF_VERSION_BYTES,
        "working-diff version",
    )?;
    let mut field_offset = 0;
    let change_id = ChangeId::new(read_uuid(
        take(
            payload,
            &mut field_offset,
            UUID_BYTES,
            "working-diff change id",
        )?,
        "working-diff change id",
    )?);
    let commit_id = CommitId::new(read_uuid(
        take(
            payload,
            &mut field_offset,
            UUID_BYTES,
            "working-diff commit id",
        )?,
        "working-diff commit id",
    )?);
    let deleted = match take(payload, &mut field_offset, 1, "working-diff deletion flag")?[0] {
        0 => false,
        1 => true,
        _ => return Err(hot_state_error("working-diff deletion flag is invalid")),
    };
    let created_at = read_timestamp(
        take(payload, &mut field_offset, 8, "working-diff created_at")?,
        "working-diff created_at",
    )?;
    let updated_at = read_timestamp(
        take(payload, &mut field_offset, 8, "working-diff updated_at")?,
        "working-diff updated_at",
    )?;
    let snapshot = decode_working_diff_slot(payload, &mut field_offset, "snapshot")?;
    let metadata = decode_working_diff_slot(payload, &mut field_offset, "metadata")?;
    debug_assert_eq!(field_offset, WORKING_DIFF_VERSION_BYTES);
    Ok(V68WorkingDiffVersion {
        change_id,
        commit_id,
        deleted,
        created_at,
        updated_at,
        snapshot,
        metadata,
    })
}

fn decode_working_diff_slot(
    bytes: &[u8],
    offset: &mut usize,
    field: &str,
) -> Result<V68WorkingDiffSlot, LixError> {
    let kind = take(bytes, offset, 1, &format!("working-diff {field} kind"))?[0];
    let hash: [u8; JSON_REF_BYTES] = take(
        bytes,
        offset,
        JSON_REF_BYTES,
        &format!("working-diff {field} hash"),
    )?
    .try_into()
    .expect("length was checked");
    match kind {
        WORKING_DIFF_SLOT_NONE if hash == [0; JSON_REF_BYTES] => Ok(V68WorkingDiffSlot::None),
        WORKING_DIFF_SLOT_REF => Ok(V68WorkingDiffSlot::Ref(JsonRef::from_hash_bytes(hash))),
        WORKING_DIFF_SLOT_INLINE => Ok(V68WorkingDiffSlot::Inline(JsonRef::from_hash_bytes(hash))),
        WORKING_DIFF_SLOT_UNRESOLVED if hash == [0; JSON_REF_BYTES] => {
            Ok(V68WorkingDiffSlot::Unresolved)
        }
        WORKING_DIFF_SLOT_NONE | WORKING_DIFF_SLOT_UNRESOLVED => Err(hot_state_error(format!(
            "working-diff {field} slot kind must have a zero hash"
        ))),
        _ => Err(hot_state_error(format!(
            "working-diff {field} slot kind is invalid"
        ))),
    }
}

fn read_commit_id(bytes: &[u8], offset: &mut usize, field: &str) -> Result<CommitId, LixError> {
    Ok(CommitId::new(read_uuid(
        take(bytes, offset, UUID_BYTES, field)?,
        field,
    )?))
}

fn read_timestamp(bytes: &[u8], field: &str) -> Result<LixTimestamp, LixError> {
    LixTimestamp::from_packed(read_u64(bytes, field)?)
        .map_err(|error| hot_state_error(format!("invalid {field}: {error}")))
}

fn read_uuid(bytes: &[u8], field: &str) -> Result<uuid::Uuid, LixError> {
    let bytes: [u8; UUID_BYTES] = bytes.try_into().map_err(|_| {
        hot_state_error(format!(
            "{field} must have {UUID_BYTES} bytes in the v9 value"
        ))
    })?;
    Ok(uuid::Uuid::from_bytes(bytes))
}

fn read_u64(bytes: &[u8], field: &str) -> Result<u64, LixError> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| hot_state_error(format!("{field} has an invalid width")))?;
    Ok(u64::from_be_bytes(bytes))
}

fn read_u32(bytes: &[u8], field: &str) -> Result<u32, LixError> {
    let bytes: [u8; 4] = bytes
        .try_into()
        .map_err(|_| hot_state_error(format!("{field} has an invalid width")))?;
    Ok(u32::from_be_bytes(bytes))
}

fn take<'a>(
    bytes: &'a [u8],
    offset: &mut usize,
    length: usize,
    field: &str,
) -> Result<&'a [u8], LixError> {
    let end = offset
        .checked_add(length)
        .ok_or_else(|| hot_state_error(format!("{field} offset overflow")))?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(|| hot_state_error(format!("{field} is truncated")))?;
    *offset = end;
    Ok(value)
}

fn hot_state_error(message: impl std::fmt::Display) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("invalid v68 hot-state row: {message}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timestamp(value: &str) -> LixTimestamp {
        LixTimestamp::expect_parse("fixture timestamp", value)
    }

    fn append_slot(bytes: &mut Vec<u8>, kind: u8, payload: &[u8]) {
        bytes[1] |= kind << HEAD_VALUE_SNAPSHOT_SHIFT;
        bytes[50..54].copy_from_slice(&(payload.len() as u32).to_be_bytes());
        bytes.extend_from_slice(payload);
    }

    fn frozen_v9_header(
        change_id: ChangeId,
        commit_id: Option<CommitId>,
        created_at: LixTimestamp,
        updated_at: LixTimestamp,
    ) -> Vec<u8> {
        let mut bytes = vec![0; HEAD_VALUE_HEADER_BYTES];
        bytes[0] = HEAD_VALUE_VERSION;
        bytes[2..18].copy_from_slice(change_id.as_uuid().as_bytes());
        if let Some(commit_id) = commit_id {
            bytes[18..34].copy_from_slice(commit_id.as_uuid().as_bytes());
        }
        bytes[34..42].copy_from_slice(&created_at.packed().to_be_bytes());
        bytes[42..50].copy_from_slice(&updated_at.packed().to_be_bytes());
        bytes
    }

    fn append_working_diff_slot(bytes: &mut Vec<u8>, kind: u8, hash: [u8; JSON_REF_BYTES]) {
        bytes.push(kind);
        bytes.extend_from_slice(&hash);
    }

    #[test]
    fn decodes_frozen_v9_value_into_owned_fields() {
        let change_id = ChangeId::for_test_label("v68-current-change");
        let commit_id = CommitId::for_test_label("v68-current-commit");
        let checkpoint_commit_id = CommitId::for_test_label("v68-checkpoint");
        let before_change_id = ChangeId::for_test_label("v68-before-change");
        let before_commit_id = CommitId::for_test_label("v68-before-commit");
        let base_commit_id = CommitId::for_test_label("v68-columnar-base");
        let created_at = timestamp("2026-08-17T12:00:00Z");
        let updated_at = timestamp("2026-08-18T13:14:15.016Z");
        let before_created_at = timestamp("2026-08-10T01:02:03Z");
        let before_updated_at = timestamp("2026-08-11T04:05:06Z");
        let snapshot_json = br#"{"version":68}"#;
        let snapshot_ref = JsonRef::for_content(snapshot_json);
        let metadata_ref = JsonRef::from_hash_bytes([0x5a; JSON_REF_BYTES]);

        let mut bytes = frozen_v9_header(change_id, Some(commit_id), created_at, updated_at);
        bytes[1] |= HEAD_SLOT_INLINE_FINGERPRINTED << HEAD_VALUE_SNAPSHOT_SHIFT;
        bytes[1] |= HEAD_SLOT_REF << HEAD_VALUE_METADATA_SHIFT;
        bytes[1] |= HEAD_WORKING_DIFF_BEFORE_PRESENT << HEAD_VALUE_WORKING_DIFF_SHIFT;
        bytes[50..54]
            .copy_from_slice(&((JSON_REF_BYTES + snapshot_json.len()) as u32).to_be_bytes());
        bytes[54..58].copy_from_slice(&(JSON_REF_BYTES as u32).to_be_bytes());
        bytes[58] = 1;
        bytes.extend_from_slice(snapshot_ref.as_hash_bytes());
        bytes.extend_from_slice(snapshot_json);
        bytes.extend_from_slice(metadata_ref.as_hash_bytes());
        bytes.extend_from_slice(checkpoint_commit_id.as_uuid().as_bytes());
        bytes.extend_from_slice(before_change_id.as_uuid().as_bytes());
        bytes.extend_from_slice(before_commit_id.as_uuid().as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(&before_created_at.packed().to_be_bytes());
        bytes.extend_from_slice(&before_updated_at.packed().to_be_bytes());
        append_working_diff_slot(&mut bytes, WORKING_DIFF_SLOT_INLINE, [0x33; JSON_REF_BYTES]);
        append_working_diff_slot(
            &mut bytes,
            WORKING_DIFF_SLOT_UNRESOLVED,
            [0; JSON_REF_BYTES],
        );
        bytes.extend_from_slice(base_commit_id.as_uuid().as_bytes());
        bytes.extend_from_slice(&17u32.to_be_bytes());
        bytes.extend_from_slice(&42u32.to_be_bytes());

        let decoded = decode_hot_state_value(&bytes).expect("frozen v9 value should decode");

        assert_eq!(decoded.change_id, change_id);
        assert_eq!(decoded.commit_id, Some(commit_id));
        assert!(!decoded.untracked);
        assert!(!decoded.deleted);
        assert_eq!(decoded.created_at, created_at);
        assert_eq!(decoded.updated_at, updated_at);
        assert_eq!(
            decoded.snapshot,
            V68HotStateSlot::InlineFingerprinted {
                json_ref: snapshot_ref,
                json: String::from_utf8(snapshot_json.to_vec()).unwrap(),
            }
        );
        assert_eq!(decoded.metadata, V68HotStateSlot::Ref(metadata_ref));
        assert_eq!(
            decoded.columnar_base_coordinate,
            Some(V68ColumnarBaseCoordinate {
                base_commit_id,
                group_index: 17,
                row_index: 42,
            })
        );
        assert_eq!(
            decoded.working_diff_baseline,
            V68WorkingDiffBaseline::BeforePresent {
                checkpoint_commit_id,
                version: V68WorkingDiffVersion {
                    change_id: before_change_id,
                    commit_id: before_commit_id,
                    deleted: false,
                    created_at: before_created_at,
                    updated_at: before_updated_at,
                    snapshot: V68WorkingDiffSlot::Inline(JsonRef::from_hash_bytes([0x33; 32])),
                    metadata: V68WorkingDiffSlot::Unresolved,
                },
            }
        );
    }

    #[test]
    fn decoded_inline_json_is_owned() {
        let change_id = ChangeId::for_test_label("v68-owned-change");
        let commit_id = CommitId::for_test_label("v68-owned-commit");
        let created_at = timestamp("2026-08-17T12:00:00Z");
        let mut bytes = frozen_v9_header(change_id, Some(commit_id), created_at, created_at);
        append_slot(&mut bytes, HEAD_SLOT_INLINE, br#"{"owned":true}"#);

        let decoded = decode_hot_state_value(&bytes).expect("inline value should decode");
        drop(bytes);

        assert_eq!(
            decoded.snapshot,
            V68HotStateSlot::Inline(r#"{"owned":true}"#.to_string())
        );
    }

    #[test]
    fn decodes_valid_untracked_v9_value() {
        let change_id = ChangeId::for_test_label("v68-untracked-change");
        let created_at = timestamp("2026-08-17T12:00:00Z");
        let mut bytes = frozen_v9_header(change_id, None, created_at, created_at);
        bytes[1] |= HEAD_VALUE_UNTRACKED;

        let decoded = decode_hot_state_value(&bytes).expect("untracked value should decode");

        assert_eq!(decoded.change_id, change_id);
        assert_eq!(decoded.commit_id, None);
        assert!(decoded.untracked);
        assert_eq!(
            decoded.working_diff_baseline,
            V68WorkingDiffBaseline::Disabled
        );
    }

    #[test]
    fn rejects_truncated_payload_without_panicking() {
        let change_id = ChangeId::for_test_label("v68-truncated-change");
        let commit_id = CommitId::for_test_label("v68-truncated-commit");
        let created_at = timestamp("2026-08-17T12:00:00Z");
        let mut bytes = frozen_v9_header(change_id, Some(commit_id), created_at, created_at);
        bytes[1] |= HEAD_SLOT_INLINE << HEAD_VALUE_SNAPSHOT_SHIFT;
        bytes[50..54].copy_from_slice(&u32::MAX.to_be_bytes());

        let error = decode_hot_state_value(&bytes).expect_err("truncated payload must fail");
        assert!(error.to_string().contains("snapshot payload is truncated"));
    }

    #[test]
    fn rejects_v9_identity_and_slot_invariant_violations() {
        let change_id = ChangeId::for_test_label("v68-invalid-change");
        let commit_id = CommitId::for_test_label("v68-invalid-commit");
        let created_at = timestamp("2026-08-17T12:00:00Z");

        let mut untracked_with_commit =
            frozen_v9_header(change_id, Some(commit_id), created_at, created_at);
        untracked_with_commit[1] |= HEAD_VALUE_UNTRACKED;
        assert!(decode_hot_state_value(&untracked_with_commit).is_err());

        let mut deleted_with_payload =
            frozen_v9_header(change_id, Some(commit_id), created_at, created_at);
        deleted_with_payload[1] |= HEAD_VALUE_DELETED;
        append_slot(&mut deleted_with_payload, HEAD_SLOT_INLINE, b"null");
        assert!(decode_hot_state_value(&deleted_with_payload).is_err());

        let mut bad_ref = frozen_v9_header(change_id, Some(commit_id), created_at, created_at);
        append_slot(&mut bad_ref, HEAD_SLOT_REF, &[0; JSON_REF_BYTES - 1]);
        assert!(decode_hot_state_value(&bad_ref).is_err());
    }
}
