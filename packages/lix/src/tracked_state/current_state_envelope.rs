//! Payload-opaque adapter between current-state locators and the generic
//! authenticated scoped-range tree.

use crate::tracked_state::scoped_range::{
    ScopedRangePart, ScopedRangePartPayload, ScopedRangePrefix,
};
use crate::tracked_state::types::{CommitDeltaReplacementScope, CurrentStatePartDescriptor};
use crate::{LixError, storage_codec};

const LOCATOR_PAYLOAD_VERSION: u16 = 3;

#[derive(musli::Encode, musli::Decode)]
#[musli(packed)]
struct CurrentStatePartLocatorPayload {
    content_digest: [u8; 32],
    payload_refs_digest: [u8; 32],
    source_kind: u8,
    source_id: [u8; 16],
    owner_commit_id: [u8; 16],
    part_index: u32,
    source_page_index: u16,
    source_row_offset: u16,
    fragmented: bool,
    uniform_created_at: crate::common::LixTimestamp,
    uniform_updated_at: crate::common::LixTimestamp,
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
                    payload_refs_digest: descriptor.payload_refs_digest,
                    source_kind: descriptor.source_kind,
                    source_id: descriptor.source_id,
                    owner_commit_id: descriptor.owner_commit_id,
                    part_index: descriptor.part_index,
                    source_page_index: descriptor.source_page_index,
                    source_row_offset: descriptor.source_row_offset,
                    fragmented: descriptor.fragmented,
                    uniform_created_at: descriptor.uniform_created_at,
                    uniform_updated_at: descriptor.uniform_updated_at,
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
        payload_refs_digest: payload.payload_refs_digest,
        source_kind: payload.source_kind,
        source_id: payload.source_id,
        owner_commit_id: payload.owner_commit_id,
        part_index: payload.part_index,
        source_page_index: payload.source_page_index,
        source_row_offset: payload.source_row_offset,
        row_count: u16::try_from(part.row_count)
            .map_err(|_| envelope_error("part row count exceeds its locator codec"))?,
        fragmented: payload.fragmented,
        uniform_created_at: payload.uniform_created_at,
        uniform_updated_at: payload.uniform_updated_at,
    };
    validate_current_state_descriptor(&descriptor)?;
    Ok(descriptor)
}

fn validate_current_state_descriptor(
    descriptor: &CurrentStatePartDescriptor,
) -> Result<(), LixError> {
    let slice_end = u32::from(descriptor.source_row_offset)
        .checked_add(u32::from(descriptor.row_count))
        .ok_or_else(|| envelope_error("part source slice overflows"))?;
    if descriptor.first_key.is_empty()
        || descriptor.last_key.is_empty()
        || descriptor.first_key > descriptor.last_key
        || descriptor.row_count == 0
        || slice_end
            > match descriptor.source_kind {
                2 => crate::columnar_row_group::ROW_GROUP_PAGE_ROWS as u32,
                _ => {
                    crate::tracked_state::current_state_data_part::CURRENT_STATE_DATA_PART_MAX_ROWS
                        as u32
                }
            }
        || descriptor.content_digest == [0; 32]
        || descriptor.source_kind > 2
        || (descriptor.source_kind == 0
            && (descriptor.source_id != [0; 16]
                || descriptor.source_page_index != 0
                || descriptor.owner_commit_id == [0; 16]
                || descriptor.payload_refs_digest != [0; 32]))
        || (descriptor.source_kind == 1
            && (descriptor.source_id != [0; 16]
                || descriptor.source_page_index != 0
                || descriptor.owner_commit_id != [0; 16]
                || descriptor.part_index != 0
                || descriptor.payload_refs_digest == [0; 32]
                || descriptor.uniform_created_at.packed() != 0
                || descriptor.uniform_updated_at.packed() != 0))
        || (descriptor.source_kind == 2
            && (descriptor.source_id == [0; 16]
                || descriptor.owner_commit_id == [0; 16]
                || descriptor.payload_refs_digest != [0; 32]))
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
