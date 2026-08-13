//! Payload-opaque adapter between current-state locators and the generic
//! authenticated scoped-range tree.

use crate::tracked_state::scoped_range::{
    ScopedRangePart, ScopedRangePartPayload, ScopedRangePrefix,
};
use crate::tracked_state::types::{
    CommitDeltaReplacementScope, CurrentStatePartDescriptor, CurrentStatePartSource,
};
use crate::{LixError, storage_codec};

const LOCATOR_PAYLOAD_VERSION: u16 = 4;

/// Wire form of one part locator.
///
/// Only the fields every source kind uses live here; the rest live in the
/// variant that uses them, so a locator never spends bytes on another kind's
/// addressing.
#[derive(musli::Encode, musli::Decode)]
#[musli(packed)]
struct CurrentStatePartLocatorPayload {
    content_digest: [u8; 32],
    source_row_offset: u16,
    fragmented: bool,
    source: CurrentStatePartSource,
}

pub(crate) fn current_state_scope_prefix(
    scope: &CommitDeltaReplacementScope,
) -> Result<ScopedRangePrefix, LixError> {
    current_state_scope_prefix_from_parts(&scope.schema_key, scope.file_id.as_deref())
}

pub(crate) fn current_state_scope_prefix_from_parts(
    schema_key: &str,
    file_id: Option<&str>,
) -> Result<ScopedRangePrefix, LixError> {
    let file_tag = [u8::from(file_id.is_some())];
    ScopedRangePrefix::try_from_components([
        schema_key.as_bytes(),
        file_tag.as_slice(),
        file_id.unwrap_or_default().as_bytes(),
    ])
}

pub(crate) fn scoped_range_part_from_current_state_descriptor(
    scope: &CommitDeltaReplacementScope,
    descriptor: &CurrentStatePartDescriptor,
) -> Result<ScopedRangePart, LixError> {
    Ok(ScopedRangePart {
        scope: current_state_scope_prefix(scope)?,
        first_key: descriptor.first_key.clone(),
        last_key: descriptor.last_key.clone(),
        row_count: u64::from(descriptor.row_count),
        payload: ScopedRangePartPayload {
            version: LOCATOR_PAYLOAD_VERSION,
            bytes: storage_codec::encode(
                "current-state scoped-range locator payload",
                &CurrentStatePartLocatorPayload {
                    content_digest: descriptor.content_digest,
                    source_row_offset: descriptor.source_row_offset,
                    fragmented: descriptor.fragmented,
                    source: descriptor.source.clone(),
                },
            )?,
        },
    })
}

pub(crate) fn current_state_descriptor_from_scoped_range_part(
    part: &ScopedRangePart,
) -> Result<CurrentStatePartDescriptor, LixError> {
    if part.payload.version != LOCATOR_PAYLOAD_VERSION {
        return Err(envelope_error("part has an unknown payload codec"));
    }
    let payload: CurrentStatePartLocatorPayload = storage_codec::decode(
        "current-state scoped-range locator payload",
        &part.payload.bytes,
    )?;
    let descriptor = CurrentStatePartDescriptor {
        first_key: part.first_key.clone(),
        last_key: part.last_key.clone(),
        content_digest: payload.content_digest,
        source: payload.source,
        source_row_offset: payload.source_row_offset,
        row_count: u16::try_from(part.row_count)
            .map_err(|_| envelope_error("part row count exceeds its locator codec"))?,
        fragmented: payload.fragmented,
    };
    validate_current_state_descriptor(&descriptor)?;
    Ok(descriptor)
}

/// Checks the invariants the type system cannot carry.
///
/// Every "this field must be zero for this kind" clause this validator used to
/// run is gone: those fields no longer exist outside the variant that owns
/// them. What remains is genuine content validation - key bounds, the source's
/// physical row bound, and identifiers that must be present.
fn validate_current_state_descriptor(
    descriptor: &CurrentStatePartDescriptor,
) -> Result<(), LixError> {
    let slice_end = u32::from(descriptor.source_row_offset)
        .checked_add(u32::from(descriptor.row_count))
        .ok_or_else(|| envelope_error("part source slice overflows"))?;
    let source_row_bound = match descriptor.source {
        CurrentStatePartSource::ColumnarPage(_) => {
            crate::columnar_row_group::ROW_GROUP_PAGE_ROWS as u32
        }
        _ => crate::tracked_state::current_state_data_part::CURRENT_STATE_DATA_PART_MAX_ROWS as u32,
    };
    let source_identifiers_present = match &descriptor.source {
        CurrentStatePartSource::Replacement(source) => source.owner_commit_id != [0; 16],
        CurrentStatePartSource::NativeDataPart {
            payload_refs_digest,
        } => *payload_refs_digest != [0; 32],
        CurrentStatePartSource::ColumnarPage(source) => {
            source.source_id != [0; 16] && source.owner_commit_id != [0; 16]
        }
    };
    if descriptor.first_key.is_empty()
        || descriptor.last_key.is_empty()
        || descriptor.first_key > descriptor.last_key
        || descriptor.row_count == 0
        || slice_end > source_row_bound
        || descriptor.content_digest == [0; 32]
        || !source_identifiers_present
    {
        return Err(envelope_error("part locator invariants are invalid"));
    }
    Ok(())
}

pub(crate) fn replacement_directory_digest(
    descriptors: &[CurrentStatePartDescriptor],
) -> Result<[u8; 32], LixError> {
    let mut first_ordinal = 0u32;
    let entries = descriptors
        .iter()
        .map(|descriptor| {
            let entry = crate::tracked_state::replacement_part::ReplacementPartDirectoryEntry::new(
                descriptor.content_digest,
                &descriptor.first_key,
                &descriptor.last_key,
                first_ordinal,
                descriptor.row_count,
            );
            first_ordinal = first_ordinal
                .checked_add(u32::from(descriptor.row_count))
                .expect("validated replacement row count fits u32");
            entry
        })
        .collect();
    crate::tracked_state::replacement_part::ReplacementPartDirectory::try_new(
        entries,
        u32::try_from(
            descriptors
                .iter()
                .map(|descriptor| u64::from(descriptor.row_count))
                .sum::<u64>(),
        )
        .map_err(|_| envelope_error("row count overflows u32"))?,
    )?
    .digest()
}

fn envelope_error(message: impl std::fmt::Display) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("tracked_state current-state scoped-range envelope {message}"),
    )
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::LixTimestamp;
    use crate::tracked_state::types::{ColumnarPageSource, ReplacementPartSource};

    /// The v3 locator payload: one flattened tagged union whose every locator
    /// carried the other kinds' fields pinned to zero.
    #[derive(musli::Encode, musli::Decode)]
    #[musli(packed)]
    struct LocatorPayloadV3 {
        content_digest: [u8; 32],
        payload_refs_digest: [u8; 32],
        source_kind: u8,
        source_id: [u8; 16],
        owner_commit_id: [u8; 16],
        part_index: u32,
        source_page_index: u16,
        source_row_offset: u16,
        fragmented: bool,
        uniform_created_at: LixTimestamp,
        uniform_updated_at: LixTimestamp,
    }

    const PART_INDEX: u32 = 3;
    const PAGE_INDEX: u16 = 5;
    const CREATED_AT_MS: i64 = 1_760_000_000_000;
    const UPDATED_AT_MS: i64 = 1_760_000_060_000;

    fn scope() -> CommitDeltaReplacementScope {
        CommitDeltaReplacementScope {
            schema_key: "locator_bytes".to_owned(),
            file_id: None,
        }
    }

    fn descriptor(source: CurrentStatePartSource) -> CurrentStatePartDescriptor {
        CurrentStatePartDescriptor {
            first_key: vec![1],
            last_key: vec![2],
            content_digest: [7; 32],
            source,
            source_row_offset: 0,
            row_count: 1,
            fragmented: false,
        }
    }

    fn replacement() -> CurrentStatePartSource {
        CurrentStatePartSource::Replacement(ReplacementPartSource {
            owner_commit_id: [3; 16],
            part_index: PART_INDEX,
            uniform_created_at: LixTimestamp::from_unix_millis_utc_lossy(CREATED_AT_MS),
            uniform_updated_at: LixTimestamp::from_unix_millis_utc_lossy(UPDATED_AT_MS),
        })
    }

    fn native() -> CurrentStatePartSource {
        CurrentStatePartSource::NativeDataPart {
            payload_refs_digest: [9; 32],
        }
    }

    fn columnar() -> CurrentStatePartSource {
        CurrentStatePartSource::ColumnarPage(ColumnarPageSource {
            source_id: [2; 16],
            owner_commit_id: [3; 16],
            part_index: PART_INDEX,
            source_page_index: PAGE_INDEX,
            uniform_created_at: LixTimestamp::from_unix_millis_utc_lossy(CREATED_AT_MS),
            uniform_updated_at: LixTimestamp::from_unix_millis_utc_lossy(UPDATED_AT_MS),
        })
    }

    fn payload_len(source: CurrentStatePartSource) -> usize {
        scoped_range_part_from_current_state_descriptor(&scope(), &descriptor(source))
            .expect("descriptor should encode")
            .payload
            .bytes
            .len()
    }

    fn v3_len(kind: u8) -> usize {
        v3_encoded(kind).len()
    }

    /// The same locator, in the shape v3 required: every field present, with
    /// the ones this kind may not use pinned to zero.
    fn v3_encoded(kind: u8) -> Vec<u8> {
        storage_codec::encode(
            "v3 locator payload",
            &LocatorPayloadV3 {
                content_digest: [7; 32],
                payload_refs_digest: if kind == 1 { [9; 32] } else { [0; 32] },
                source_kind: kind,
                source_id: if kind == 2 { [2; 16] } else { [0; 16] },
                owner_commit_id: if kind == 1 { [0; 16] } else { [3; 16] },
                part_index: if kind == 1 { 0 } else { PART_INDEX },
                source_page_index: if kind == 2 { PAGE_INDEX } else { 0 },
                source_row_offset: 0,
                fragmented: false,
                uniform_created_at: if kind == 1 {
                    LixTimestamp::from_unix_millis_utc_lossy(0)
                } else {
                    LixTimestamp::from_unix_millis_utc_lossy(CREATED_AT_MS)
                },
                uniform_updated_at: if kind == 1 {
                    LixTimestamp::from_unix_millis_utc_lossy(0)
                } else {
                    LixTimestamp::from_unix_millis_utc_lossy(UPDATED_AT_MS)
                },
            },
        )
        .expect("v3 payload should encode")
    }

    /// Every part locator is embedded per part inside a serving-tree leaf, so
    /// these byte counts are fetched and decoded on every scoped-range read.
    ///
    /// v3 spent the same bytes on every locator whatever its kind, because the
    /// fields a kind could not use were still encoded - pinned to zero and
    /// re-proved zero by the validator on each decode. These figures are the
    /// same three locators, measured through the same codec.
    #[test]
    fn locator_payload_spends_no_bytes_on_other_kinds_fields() {
        assert_eq!(
            [
                (v3_len(0), payload_len(replacement())),
                (v3_len(1), payload_len(native())),
                (v3_len(2), payload_len(columnar())),
            ],
            [(121, 72), (107, 71), (121, 90)]
        );
    }

    /// `#[musli(packed)]` records are positional and untagged, so a format
    /// break is only safe if an old reader cannot silently accept new bytes.
    #[test]
    fn v3_readers_and_v4_records_are_mutually_undecodable() {
        for source in [replacement(), native(), columnar()] {
            let part = scoped_range_part_from_current_state_descriptor(&scope(), &descriptor(source))
                .expect("descriptor should encode");
            assert_eq!(part.payload.version, LOCATOR_PAYLOAD_VERSION);
            assert!(
                storage_codec::decode::<LocatorPayloadV3>("v3 locator payload", &part.payload.bytes)
                    .is_err(),
                "a v3 reader must not decode a v4 record"
            );
        }

        let v3_bytes = v3_encoded(0);
        assert!(
            storage_codec::decode::<CurrentStatePartLocatorPayload>(
                "current-state scoped-range locator payload",
                &v3_bytes,
            )
            .is_err(),
            "a v4 reader must not decode a v3 record"
        );
    }

    /// The payload codec self-versions, so a v3 part is refused before its
    /// bytes are ever handed to a decoder.
    #[test]
    fn stale_payload_version_is_refused_before_decoding() {
        let mut part = scoped_range_part_from_current_state_descriptor(&scope(), &descriptor(replacement()))
            .expect("descriptor should encode");
        part.payload.version = 3;
        assert!(current_state_descriptor_from_scoped_range_part(&part).is_err());
    }

    #[test]
    fn locator_round_trips_every_source_kind() {
        for source in [replacement(), native(), columnar()] {
            let expected = descriptor(source);
            let part = scoped_range_part_from_current_state_descriptor(&scope(), &expected)
                .expect("descriptor should encode");
            let decoded = current_state_descriptor_from_scoped_range_part(&part)
                .expect("descriptor should decode");
            assert_eq!(decoded, expected);
        }
    }
}
