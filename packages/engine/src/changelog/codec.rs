use super::types::{
    ByChangeEntry, ByCommitEntry, CommitBody, CommitHeader, CommitVisibility, MembershipRecord,
    MembershipRole, Segment, SegmentChange, SegmentChangeDirectory, SegmentCommit,
    SegmentCommitDirectory, SegmentDirectory, SegmentDirectoryEntryRef, SegmentHeader,
    SegmentInlinePayload, SegmentObjectLocation, SegmentObjectLocationRef, SegmentObjectSlice,
    SegmentPayloadLocation, SegmentView, StateRowIdentity,
};
use crate::common::{CanonicalSchemaKey, EntityId, FileId, LixError};
use crate::entity_identity::EntityIdentity;
use crate::json_store::JsonRef;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

const SEGMENT_MAGIC: &[u8; 5] = b"LXSG1";
const COMMIT_VISIBILITY_MAGIC: &[u8; 5] = b"LXCV1";
const BY_COMMIT_MAGIC: &[u8; 5] = b"LXBC1";
const BY_CHANGE_MAGIC: &[u8; 5] = b"LXBG1";
const SEGMENT_FORMAT_VERSION: u32 = 1;
const MIN_STRING_BYTES: usize = 4;
const MIN_LOCATION_BYTES: usize = MIN_STRING_BYTES + 8 + 8 + MIN_STRING_BYTES;
const MIN_DIRECTORY_ENTRY_BYTES: usize = MIN_STRING_BYTES + MIN_LOCATION_BYTES;
const MIN_MEMBERSHIP_RECORD_BYTES: usize = MIN_STRING_BYTES + 1 + 1;
const MIN_STATE_ROW_IDENTITY_ENTRY_BYTES: usize =
    MIN_STRING_BYTES + MIN_STRING_BYTES + MIN_STRING_BYTES + MIN_STRING_BYTES;
const MIN_MEMBERSHIP_ORDINAL_BYTES: usize = MIN_STRING_BYTES + 4;
const MIN_INLINE_PAYLOAD_BYTES: usize = 32 + 4;
const MIN_PAYLOAD_LOCATION_BYTES: usize = 32 + 8 + 8;
const MIN_COMMIT_OBJECT_BYTES: usize =
    MIN_STRING_BYTES + 4 + MIN_STRING_BYTES + 4 + MIN_STRING_BYTES + 4 + MIN_STRING_BYTES;
const MIN_CHANGE_OBJECT_BYTES: usize =
    MIN_STRING_BYTES + 1 + 4 + MIN_STRING_BYTES + 1 + 1 + 1 + MIN_STRING_BYTES + 4 + 4;

pub(crate) fn encode_segment(segment: &Segment) -> Result<Vec<u8>, LixError> {
    Ok(encode_segment_with_object_locations(segment)?.bytes)
}

pub(crate) fn encode_segment_with_object_locations(
    segment: &Segment,
) -> Result<EncodedSegment, LixError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(SEGMENT_MAGIC);
    write_segment_header(&mut bytes, &segment.header)?;
    write_segment_directory(&mut bytes, &segment.directory)?;
    write_len(&mut bytes, segment.commits.len(), "segment commits")?;
    let mut commits = Vec::with_capacity(segment.commits.len());
    for commit in &segment.commits {
        let start = bytes.len() as u64;
        write_segment_commit(&mut bytes, commit)?;
        commits.push(EncodedSegmentObject {
            id: commit.header.id.clone(),
            offset: start,
            len: bytes.len() as u64 - start,
            checksum: Some(commit.checksum.clone()),
        });
    }
    write_len(&mut bytes, segment.changes.len(), "segment changes")?;
    let mut changes = Vec::with_capacity(segment.changes.len());
    for change in &segment.changes {
        let start = bytes.len() as u64;
        write_segment_change(&mut bytes, change)?;
        changes.push(EncodedSegmentObject {
            id: change.id.clone(),
            offset: start,
            len: bytes.len() as u64 - start,
            checksum: None,
        });
    }
    Ok(EncodedSegment {
        bytes,
        commits,
        changes,
    })
}

pub(crate) struct EncodedSegment {
    pub(crate) bytes: Vec<u8>,
    pub(crate) commits: Vec<EncodedSegmentObject>,
    pub(crate) changes: Vec<EncodedSegmentObject>,
}

pub(crate) struct EncodedSegmentObject {
    pub(crate) id: String,
    pub(crate) offset: u64,
    pub(crate) len: u64,
    pub(crate) checksum: Option<String>,
}

pub(crate) fn decode_segment(bytes: &[u8]) -> Result<Segment, LixError> {
    let mut cursor = ByteCursor::new(bytes);
    cursor.expect_magic(SEGMENT_MAGIC, "segment")?;
    let header = cursor.read_segment_header("header")?;
    let directory = cursor.read_segment_directory(
        "directory",
        header.commit_count as usize,
        header.change_count as usize,
    )?;
    let commit_len = cursor.read_len("commits")?;
    cursor.ensure_len_fits_remaining(commit_len, "commits")?;
    cursor.ensure_counted_records_fit_remaining(commit_len, MIN_COMMIT_OBJECT_BYTES, "commits")?;
    validate_count_matches_usize("commit_count", header.commit_count, commit_len, "commits")?;
    let mut commits = Vec::with_capacity(commit_len);
    for index in 0..commit_len {
        commits.push(cursor.read_segment_commit(&format!("commits[{index}]"))?);
    }
    let change_len = cursor.read_len("changes")?;
    cursor.ensure_len_fits_remaining(change_len, "changes")?;
    cursor.ensure_counted_records_fit_remaining(change_len, MIN_CHANGE_OBJECT_BYTES, "changes")?;
    validate_count_matches_usize("change_count", header.change_count, change_len, "changes")?;
    let mut changes = Vec::with_capacity(change_len);
    let mut remaining_payload_count = header.payload_count as usize;
    for index in 0..change_len {
        changes.push(cursor.read_segment_change(
            &format!("changes[{index}]"),
            Some(&mut remaining_payload_count),
        )?);
    }
    cursor.expect_end("segment")?;
    validate_count_matches_usize(
        "payload_count",
        header.payload_count,
        header.payload_count as usize - remaining_payload_count,
        "inline payloads",
    )?;
    validate_segment_header_object_counts(
        header.commit_count,
        header.change_count,
        header.payload_count,
        commits.len(),
        changes.as_slice(),
    )?;
    let commit_memberships = commits
        .iter()
        .map(|commit| {
            commit
                .body
                .membership
                .iter()
                .map(|membership| CommitMembershipDescriptor {
                    member_change_id: Cow::Owned(membership.member_change_id.clone()),
                    role: membership.role,
                    source_parent_ordinal: membership.source_parent_ordinal,
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    validate_segment_commit_state_row_identities(
        commits
            .iter()
            .zip(commit_memberships.iter())
            .map(|(commit, memberships)| {
                (
                    commit.header.id.as_str(),
                    memberships.as_slice(),
                    commit.directory.state_row_identities.as_slice(),
                )
            }),
        changes.iter().map(|change| {
            Ok((
                change.id.as_str(),
                ChangeValidationDescriptor {
                    state_row_identity: state_row_identity_for_change(change)?,
                    authored_commit_id: change.authored_commit_id.as_deref(),
                },
            ))
        }),
    )?;
    let (commit_slices, change_slices) = read_segment_object_slices_view(bytes)?;
    validate_segment_directory_object_slices_owned(
        &header.segment_id,
        &directory,
        &commit_slices,
        &change_slices,
    )?;
    let segment = Segment {
        header,
        directory,
        commits,
        changes,
    };
    Ok(segment)
}

fn validate_commit_checksum(commit: &SegmentCommit) -> Result<(), LixError> {
    let canonical = checksum_commit(commit)?;
    if commit.checksum != canonical {
        return Err(LixError::unknown(format!(
            "changelog commit '{}' checksum '{}' does not match canonical checksum '{}'",
            commit.header.id, commit.checksum, canonical
        )));
    }
    Ok(())
}

fn validate_segment_view_checksum(
    bytes: &[u8],
    header: &SegmentHeaderView<'_>,
    directory_changes: &[SegmentDirectoryEntryRef<'_>],
    commits: &[SegmentCommitSliceRead<'_>],
    changes: &[SegmentChangeSliceRead<'_>],
) -> Result<(), LixError> {
    if header.byte_count != bytes.len() as u64 {
        return Err(LixError::unknown(format!(
            "changelog segment '{}' byte_count {} does not match encoded length {}",
            header.segment_id,
            header.byte_count,
            bytes.len()
        )));
    }
    let change_checksums = changes
        .iter()
        .map(|change| (change.slice.id, change.checksum.as_str()))
        .collect::<HashMap<_, _>>();
    for entry in directory_changes {
        let Some(canonical) = change_checksums.get(entry.id) else {
            continue;
        };
        if entry.location.checksum != *canonical {
            return Err(LixError::unknown(format!(
                "changelog change '{}' locator checksum '{}' does not match canonical checksum '{}'",
                entry.id, entry.location.checksum, canonical
            )));
        }
    }
    let canonical = checksum_segment_from_view(header, commits, changes)?;
    if header.checksum != canonical {
        return Err(LixError::unknown(format!(
            "changelog segment '{}' checksum '{}' does not match canonical checksum '{}'",
            header.segment_id, header.checksum, canonical
        )));
    }
    Ok(())
}

fn checksum_segment_from_view(
    header: &SegmentHeaderView<'_>,
    commits: &[SegmentCommitSliceRead<'_>],
    changes: &[SegmentChangeSliceRead<'_>],
) -> Result<String, LixError> {
    let mut hasher = blake3::Hasher::new();
    hash_part(&mut hasher, "segment");
    hash_field(&mut hasher, "segment_id", header.segment_id);
    hash_field(
        &mut hasher,
        "format_version",
        &header.format_version.to_string(),
    );
    hash_field(
        &mut hasher,
        "commit_count",
        &header.commit_count.to_string(),
    );
    hash_field(
        &mut hasher,
        "change_count",
        &header.change_count.to_string(),
    );
    hash_field(&mut hasher, "byte_count", &header.byte_count.to_string());
    hash_field(
        &mut hasher,
        "payload_count",
        &header.payload_count.to_string(),
    );
    hash_list_len(&mut hasher, "commits", commits.len());
    for commit in commits {
        hash_part(&mut hasher, "commit");
        hash_field(&mut hasher, "id", commit.slice.id);
        hash_field(&mut hasher, "checksum", &commit.checksum);
    }
    hash_list_len(&mut hasher, "changes", changes.len());
    for change in changes {
        hash_part(&mut hasher, "change");
        hash_field(&mut hasher, "id", change.slice.id);
        hash_field(&mut hasher, "checksum", &change.checksum);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn checksum_segment(segment: &Segment) -> Result<String, LixError> {
    let mut hasher = blake3::Hasher::new();
    hash_part(&mut hasher, "segment");
    hash_field(&mut hasher, "segment_id", &segment.header.segment_id);
    hash_field(
        &mut hasher,
        "format_version",
        &segment.header.format_version.to_string(),
    );
    hash_field(
        &mut hasher,
        "commit_count",
        &segment.header.commit_count.to_string(),
    );
    hash_field(
        &mut hasher,
        "change_count",
        &segment.header.change_count.to_string(),
    );
    hash_field(
        &mut hasher,
        "byte_count",
        &segment.header.byte_count.to_string(),
    );
    hash_field(
        &mut hasher,
        "payload_count",
        &segment.header.payload_count.to_string(),
    );
    hash_list_len(&mut hasher, "commits", segment.commits.len());
    for commit in &segment.commits {
        hash_part(&mut hasher, "commit");
        hash_field(&mut hasher, "id", &commit.header.id);
        hash_field(&mut hasher, "checksum", &commit.checksum);
    }
    hash_list_len(&mut hasher, "changes", segment.changes.len());
    for change in &segment.changes {
        hash_part(&mut hasher, "change");
        hash_field(&mut hasher, "id", &change.id);
        hash_field(&mut hasher, "checksum", &checksum_change(change)?);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn checksum_commit(commit: &SegmentCommit) -> Result<String, LixError> {
    let memberships = commit
        .body
        .membership
        .iter()
        .map(|membership| CommitMembershipDescriptor {
            member_change_id: Cow::Owned(membership.member_change_id.clone()),
            role: membership.role,
            source_parent_ordinal: membership.source_parent_ordinal,
        })
        .collect::<Vec<_>>();
    checksum_commit_parts(
        &commit.header.id,
        commit.header.parent_commit_ids.iter().map(String::as_str),
        &commit.header.derivable_change_id,
        commit.header.author_account_ids.iter().map(String::as_str),
        &commit.header.created_at,
        commit.header.membership_count,
        &memberships,
        &commit.directory,
    )
}

fn checksum_commit_parts<'a>(
    id: &str,
    parent_commit_ids: impl Iterator<Item = &'a str>,
    derivable_change_id: &str,
    author_account_ids: impl Iterator<Item = &'a str>,
    created_at: &str,
    membership_count: u32,
    memberships: &[CommitMembershipDescriptor<'_>],
    directory: &SegmentCommitDirectory,
) -> Result<String, LixError> {
    let mut hasher = blake3::Hasher::new();
    hash_part(&mut hasher, "commit");
    hash_field(&mut hasher, "id", id);
    let parent_commit_ids = parent_commit_ids.collect::<Vec<_>>();
    hash_list_len(&mut hasher, "parent_commit_ids", parent_commit_ids.len());
    for parent in parent_commit_ids {
        hash_field(&mut hasher, "parent_commit_id", parent);
    }
    hash_field(&mut hasher, "derivable_change_id", derivable_change_id);
    let author_account_ids = author_account_ids.collect::<Vec<_>>();
    hash_list_len(&mut hasher, "author_account_ids", author_account_ids.len());
    for account_id in author_account_ids {
        hash_field(&mut hasher, "author_account_id", account_id);
    }
    hash_field(&mut hasher, "created_at", created_at);
    hash_field(
        &mut hasher,
        "membership_count",
        &membership_count.to_string(),
    );
    hash_list_len(&mut hasher, "memberships", memberships.len());
    for membership in memberships {
        hash_part(&mut hasher, "membership");
        hash_field(
            &mut hasher,
            "member_change_id",
            &membership.member_change_id,
        );
        hash_field(
            &mut hasher,
            "role",
            match membership.role {
                MembershipRole::Authored => "authored",
                MembershipRole::Adopted => "adopted",
            },
        );
        hash_field(
            &mut hasher,
            "source_parent_ordinal",
            &membership
                .source_parent_ordinal
                .map(|ordinal| ordinal.to_string())
                .unwrap_or_default(),
        );
    }
    hash_list_len(
        &mut hasher,
        "state_row_identities",
        directory.state_row_identities.len(),
    );
    for (identity, change_id) in &directory.state_row_identities {
        hash_part(&mut hasher, "state_row_identity");
        hash_field(&mut hasher, "schema_key", identity.schema_key.as_str());
        hash_field(&mut hasher, "file_id", identity.file_id.as_str());
        hash_field(&mut hasher, "entity_id", identity.entity_id.as_str());
        hash_field(&mut hasher, "change_id", change_id);
    }
    hash_list_len(
        &mut hasher,
        "membership_ordinals",
        directory.membership_ordinals.len(),
    );
    for (change_id, ordinal) in &directory.membership_ordinals {
        hash_part(&mut hasher, "membership_ordinal");
        hash_field(&mut hasher, "change_id", change_id);
        hash_field(&mut hasher, "ordinal", &ordinal.to_string());
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn checksum_change(change: &SegmentChange) -> Result<String, LixError> {
    checksum_change_parts(
        &change.id,
        change.authored_commit_id.as_deref(),
        &change.entity_id,
        &change.schema_key,
        change.file_id.as_deref(),
        change.snapshot_ref.as_ref(),
        change.metadata_ref.as_ref(),
        &change.created_at,
        &change
            .inline_payloads
            .iter()
            .map(|payload| SegmentInlinePayloadRef {
                json_ref: payload.json_ref.clone(),
                bytes: payload.bytes.as_slice(),
            })
            .collect::<Vec<_>>(),
        &change.directory,
    )
}

fn checksum_change_parts(
    id: &str,
    authored_commit_id: Option<&str>,
    entity_id: &EntityIdentity,
    schema_key: &str,
    file_id: Option<&str>,
    snapshot_ref: Option<&JsonRef>,
    metadata_ref: Option<&JsonRef>,
    created_at: &str,
    inline_payloads: &[SegmentInlinePayloadRef<'_>],
    directory: &SegmentChangeDirectory,
) -> Result<String, LixError> {
    let mut hasher = blake3::Hasher::new();
    hash_part(&mut hasher, "change");
    hash_field(&mut hasher, "id", id);
    hash_optional_str(&mut hasher, "authored_commit_id", authored_commit_id);
    hash_list_len(&mut hasher, "entity_id", entity_id.parts.len());
    for part in &entity_id.parts {
        hash_field(&mut hasher, "entity_id_part", part);
    }
    hash_field(&mut hasher, "schema_key", schema_key);
    hash_optional_str(&mut hasher, "file_id", file_id);
    hash_optional_json_ref(&mut hasher, "snapshot_ref", snapshot_ref);
    hash_optional_json_ref(&mut hasher, "metadata_ref", metadata_ref);
    hash_field(&mut hasher, "created_at", created_at);
    hash_list_len(&mut hasher, "inline_payloads", inline_payloads.len());
    for payload in inline_payloads {
        hash_part(&mut hasher, "inline_payload");
        hash_json_ref(&mut hasher, "json_ref", &payload.json_ref);
        hash_bytes_field(&mut hasher, "bytes", payload.bytes);
    }
    hash_list_len(&mut hasher, "directory_payloads", directory.payloads.len());
    for payload_location in &directory.payloads {
        hash_part(&mut hasher, "payload_location");
        hash_json_ref(&mut hasher, "json_ref", &payload_location.json_ref);
        hash_field(&mut hasher, "offset", &payload_location.offset.to_string());
        hash_field(&mut hasher, "len", &payload_location.len.to_string());
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn hash_part(hasher: &mut blake3::Hasher, value: &str) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value.as_bytes());
}

fn hash_field(hasher: &mut blake3::Hasher, field: &str, value: &str) {
    hash_part(hasher, field);
    hash_part(hasher, value);
}

fn hash_list_len(hasher: &mut blake3::Hasher, field: &str, len: usize) {
    hash_part(hasher, field);
    hasher.update(&(len as u64).to_le_bytes());
}

fn hash_bytes_part(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value);
}

fn hash_bytes_field(hasher: &mut blake3::Hasher, field: &str, value: &[u8]) {
    hash_part(hasher, field);
    hash_bytes_part(hasher, value);
}

fn hash_optional_str(hasher: &mut blake3::Hasher, field: &str, value: Option<&str>) {
    hash_part(hasher, field);
    match value {
        Some(value) => {
            hash_part(hasher, "some");
            hash_part(hasher, value);
        }
        None => hash_part(hasher, "none"),
    }
}

fn hash_optional_json_ref(hasher: &mut blake3::Hasher, field: &str, value: Option<&JsonRef>) {
    hash_part(hasher, field);
    match value {
        Some(value) => {
            hash_part(hasher, "some");
            hash_json_ref(hasher, "json_ref", value);
        }
        None => hash_part(hasher, "none"),
    }
}

fn hash_json_ref(hasher: &mut blake3::Hasher, field: &str, value: &JsonRef) {
    hash_bytes_field(hasher, field, value.as_hash_bytes());
}

fn empty_checksum() -> String {
    "0".repeat(64)
}

pub(crate) fn decode_segment_commit(bytes: &[u8]) -> Result<SegmentCommit, LixError> {
    let mut cursor = ByteCursor::new(bytes);
    let commit = cursor.read_segment_commit("commit")?;
    cursor.expect_end("commit")?;
    Ok(commit)
}

pub(crate) fn segment_commit_membership_contains_any(
    bytes: &[u8],
    expected_commit_id: &str,
    expected_checksum: &str,
    requested_change_ids: &std::collections::HashSet<String>,
) -> Result<Vec<String>, LixError> {
    let mut cursor = ByteCursor::new(bytes);
    let id = cursor.read_string_ref_fast()?;
    if id != expected_commit_id {
        return Err(LixError::unknown(format!(
            "changelog commit locator for '{expected_commit_id}' decoded commit '{id}'"
        )));
    }
    let parent_commit_ids = cursor.read_strings_fast_refs()?;
    let parent_count = parent_commit_ids.len();
    let derivable_change_id = cursor.read_string_ref_fast()?;
    let author_account_ids = cursor.read_strings_fast_refs()?;
    let created_at = cursor.read_string_ref_fast()?;
    let membership_count = cursor.read_u32_fast()? as usize;
    let (matches, memberships) =
        cursor.read_matching_membership_change_ids(membership_count, requested_change_ids)?;
    let directory =
        cursor.read_segment_commit_directory_fast_validated(id, parent_count, &memberships)?;
    let checksum = cursor.read_string_ref_fast()?;
    if checksum != expected_checksum {
        return Err(LixError::unknown(format!(
            "changelog commit '{id}' locator checksum '{expected_checksum}' does not match encoded checksum '{checksum}'"
        )));
    }
    cursor.expect_end("commit")?;
    let canonical = checksum_commit_parts(
        id,
        parent_commit_ids.iter().copied(),
        derivable_change_id,
        author_account_ids.iter().copied(),
        created_at,
        membership_count as u32,
        &memberships,
        &directory,
    )?;
    if checksum != canonical {
        return Err(LixError::unknown(format!(
            "changelog commit '{id}' checksum '{checksum}' does not match canonical checksum '{canonical}'"
        )));
    }
    Ok(matches)
}

pub(crate) fn decode_segment_change(bytes: &[u8]) -> Result<SegmentChange, LixError> {
    let mut cursor = ByteCursor::new(bytes);
    let change = cursor.read_segment_change("change", None)?;
    cursor.expect_end("change")?;
    state_row_identity_for_change(&change)?;
    Ok(change)
}

pub(crate) fn view_segment(bytes: &[u8]) -> Result<SegmentView<'_>, LixError> {
    let cursor = read_segment_header_and_directory_view(bytes, false)?;
    let header = cursor.header;
    let directory_commits = cursor.directory_commits;
    let directory_changes = cursor.directory_changes;
    let mut object_cursor = cursor.cursor.clone();
    let objects = read_segment_object_slices_for_header_fast(&mut object_cursor, &header)?;
    let commit_slices = objects.commit_slices();
    let change_slices = objects.change_slices();
    validate_segment_directory_object_slices_ref(
        header.segment_id,
        &directory_commits,
        &directory_changes,
        &commit_slices,
        &change_slices,
    )?;
    validate_segment_view_checksum(
        bytes,
        &header,
        &directory_changes,
        &objects.commits,
        &objects.changes,
    )?;
    let object_bytes = cursor.cursor.remaining_bytes();

    Ok(segment_view_from_parts(
        bytes,
        header,
        directory_commits,
        directory_changes,
        object_bytes,
    ))
}

pub(crate) fn view_segment_directory(bytes: &[u8]) -> Result<SegmentView<'_>, LixError> {
    let cursor = read_segment_header_and_directory_view(bytes, true)?;
    let object_bytes = cursor.cursor.remaining_bytes();

    Ok(segment_view_from_parts(
        bytes,
        cursor.header,
        cursor.directory_commits,
        cursor.directory_changes,
        object_bytes,
    ))
}

struct SegmentDirectoryViewRead<'a> {
    cursor: ByteCursor<'a>,
    header: SegmentHeaderView<'a>,
    directory_commits: Vec<SegmentDirectoryEntryRef<'a>>,
    directory_changes: Vec<SegmentDirectoryEntryRef<'a>>,
}

fn read_segment_header_and_directory_view(
    bytes: &[u8],
    validate_encoded_bounds: bool,
) -> Result<SegmentDirectoryViewRead<'_>, LixError> {
    let mut cursor = ByteCursor::new(bytes);
    cursor.expect_magic(SEGMENT_MAGIC, "segment")?;
    let header = cursor.read_segment_header_view_fast()?;
    let directory_commits =
        cursor.read_segment_directory_commit_views_fast(header.commit_count as usize)?;
    let directory_changes =
        cursor.read_segment_directory_change_views_fast(header.change_count as usize)?;
    if validate_encoded_bounds && header.byte_count != bytes.len() as u64 {
        return Err(LixError::unknown(format!(
            "changelog segment '{}' byte_count {} does not match encoded length {}",
            header.segment_id,
            header.byte_count,
            bytes.len()
        )));
    }
    if validate_encoded_bounds {
        validate_directory_locations_are_in_bounds(
            header.segment_id,
            bytes.len() as u64,
            directory_commits
                .iter()
                .chain(directory_changes.iter())
                .copied(),
        )?;
    }
    Ok(SegmentDirectoryViewRead {
        cursor,
        header,
        directory_commits,
        directory_changes,
    })
}

fn segment_view_from_parts<'a>(
    bytes: &'a [u8],
    header: SegmentHeaderView<'a>,
    directory_commits: Vec<SegmentDirectoryEntryRef<'a>>,
    directory_changes: Vec<SegmentDirectoryEntryRef<'a>>,
    object_bytes: &'a [u8],
) -> SegmentView<'a> {
    SegmentView {
        bytes,
        segment_id: header.segment_id,
        format_version: header.format_version,
        commit_count: header.commit_count,
        change_count: header.change_count,
        byte_count: header.byte_count,
        payload_count: header.payload_count,
        checksum: header.checksum,
        directory_commits,
        directory_changes,
        object_bytes,
    }
}

fn validate_directory_locations_are_in_bounds<'a>(
    segment_id: &str,
    byte_count: u64,
    entries: impl Iterator<Item = SegmentDirectoryEntryRef<'a>>,
) -> Result<(), LixError> {
    let mut seen = HashSet::new();
    for entry in entries {
        let location = entry.location;
        if location.segment_id != segment_id {
            return Err(LixError::unknown(format!(
                "changelog segment '{segment_id}' directory locator for '{}' points to segment '{}'",
                entry.id, location.segment_id
            )));
        }
        if !seen.insert(entry.id) {
            return Err(LixError::unknown(format!(
                "changelog segment '{segment_id}' contains duplicate directory locator for '{}'",
                entry.id
            )));
        }
        let end = location.offset.checked_add(location.len).ok_or_else(|| {
            LixError::unknown(format!(
                "changelog segment '{segment_id}' directory locator for '{}' overflows",
                entry.id
            ))
        })?;
        if end > byte_count {
            return Err(LixError::unknown(format!(
                "changelog segment '{segment_id}' directory locator for '{}' is outside segment byte range",
                entry.id
            )));
        }
    }
    Ok(())
}

pub(crate) fn view_segment_object_slices(
    bytes: &[u8],
) -> Result<(Vec<SegmentObjectSlice<'_>>, Vec<SegmentObjectSlice<'_>>), LixError> {
    read_segment_object_slices_view(bytes)
}

pub(crate) fn view_segment_object_ranges(
    bytes: &[u8],
) -> Result<(Vec<SegmentObjectSlice<'_>>, Vec<SegmentObjectSlice<'_>>), LixError> {
    let cursor = read_segment_header_and_directory_view(bytes, true)?;
    let header = cursor.header;
    let directory_commits = cursor.directory_commits;
    let directory_changes = cursor.directory_changes;
    let mut object_cursor = cursor.cursor.clone();

    let commit_len = object_cursor.read_len("commits")?;
    object_cursor.ensure_len_fits_remaining(commit_len, "commits")?;
    object_cursor.ensure_counted_records_fit_remaining(
        commit_len,
        MIN_COMMIT_OBJECT_BYTES,
        "commits",
    )?;
    validate_count_matches_usize("commit_count", header.commit_count, commit_len, "commits")?;
    let mut commits = Vec::with_capacity(commit_len);
    for index in 0..commit_len {
        commits.push(object_cursor.read_segment_commit_slice(&format!("commits[{index}]"))?);
    }

    let change_len = object_cursor.read_len("changes")?;
    object_cursor.ensure_len_fits_remaining(change_len, "changes")?;
    object_cursor.ensure_counted_records_fit_remaining(
        change_len,
        MIN_CHANGE_OBJECT_BYTES,
        "changes",
    )?;
    validate_count_matches_usize("change_count", header.change_count, change_len, "changes")?;
    let mut changes = Vec::with_capacity(change_len);
    for index in 0..change_len {
        changes.push(object_cursor.read_segment_change_slice(&format!("changes[{index}]"))?);
    }

    object_cursor.expect_end("segment")?;
    validate_segment_directory_object_slices_ref(
        header.segment_id,
        &directory_commits,
        &directory_changes,
        &commits,
        &changes,
    )?;
    Ok((commits, changes))
}

fn read_segment_object_slices_view(
    bytes: &[u8],
) -> Result<(Vec<SegmentObjectSlice<'_>>, Vec<SegmentObjectSlice<'_>>), LixError> {
    let mut cursor = ByteCursor::new(bytes);
    cursor.expect_magic(SEGMENT_MAGIC, "segment")?;
    let header = cursor.read_segment_header_view_fast()?;
    let _directory_commits =
        cursor.read_segment_directory_commit_views_fast(header.commit_count as usize)?;
    let _directory_changes =
        cursor.read_segment_directory_change_views_fast(header.change_count as usize)?;

    let objects = read_segment_object_slices_for_header_fast(&mut cursor, &header)?;
    let commits = objects.commit_slices();
    let changes = objects.change_slices();
    validate_segment_directory_object_slices_ref(
        header.segment_id,
        &_directory_commits,
        &_directory_changes,
        &commits,
        &changes,
    )?;
    validate_segment_view_checksum(
        bytes,
        &header,
        &_directory_changes,
        &objects.commits,
        &objects.changes,
    )?;
    Ok((commits, changes))
}

#[derive(Clone)]
struct PayloadDescriptor {
    json_ref: JsonRef,
    len: u64,
}

struct SegmentInlinePayloadRef<'a> {
    json_ref: JsonRef,
    bytes: &'a [u8],
}

#[derive(Clone)]
struct CommitMembershipDescriptor<'a> {
    member_change_id: Cow<'a, str>,
    role: MembershipRole,
    source_parent_ordinal: Option<u32>,
}

struct SegmentCommitSliceRead<'a> {
    slice: SegmentObjectSlice<'a>,
    memberships: Vec<CommitMembershipDescriptor<'a>>,
    state_row_identities: Vec<(StateRowIdentity, String)>,
    checksum: String,
}

struct SegmentChangeSliceRead<'a> {
    slice: SegmentObjectSlice<'a>,
    state_row_identity: StateRowIdentity,
    authored_commit_id: Option<&'a str>,
    checksum: String,
}

struct SegmentObjectSliceRead<'a> {
    commits: Vec<SegmentCommitSliceRead<'a>>,
    changes: Vec<SegmentChangeSliceRead<'a>>,
}

impl<'a> SegmentObjectSliceRead<'a> {
    fn commit_slices(&self) -> Vec<SegmentObjectSlice<'a>> {
        self.commits.iter().map(|commit| commit.slice).collect()
    }

    fn change_slices(&self) -> Vec<SegmentObjectSlice<'a>> {
        self.changes.iter().map(|change| change.slice).collect()
    }
}

fn validate_segment_header_object_counts(
    header_commit_count: u32,
    header_change_count: u32,
    header_payload_count: u32,
    commit_count: usize,
    changes: &[SegmentChange],
) -> Result<(), LixError> {
    validate_count_matches_usize("commit_count", header_commit_count, commit_count, "commits")?;
    validate_count_matches_usize(
        "change_count",
        header_change_count,
        changes.len(),
        "changes",
    )?;
    let payload_count = changes.iter().try_fold(0_usize, |count, change| {
        count
            .checked_add(change.inline_payloads.len())
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "failed to decode changelog segment: inline payload count overflows",
                )
            })
    })?;
    validate_count_matches_usize(
        "payload_count",
        header_payload_count,
        payload_count,
        "inline payloads",
    )
}

fn validate_change_payload_directory(
    change_id: &str,
    inline_payloads: &[SegmentInlinePayload],
    directory: &SegmentChangeDirectory,
) -> Result<(), LixError> {
    validate_count_matches_usize(
        "payload_count",
        inline_payloads.len() as u32,
        directory.payloads.len(),
        "change directory payloads",
    )?;
    let inline_payloads = inline_payloads
        .iter()
        .map(|payload| PayloadDescriptor {
            json_ref: payload.json_ref.clone(),
            len: payload.bytes.len() as u64,
        })
        .collect::<Vec<_>>();
    validate_payload_descriptors_against_directory(change_id, &inline_payloads, directory)
}

fn validate_payload_descriptors_against_directory(
    change_id: &str,
    inline_payloads: &[PayloadDescriptor],
    directory: &SegmentChangeDirectory,
) -> Result<(), LixError> {
    let mut inline_by_ref = HashMap::with_capacity(inline_payloads.len());
    for (ordinal, payload) in inline_payloads.iter().enumerate() {
        if inline_by_ref
            .insert(
                payload.json_ref.as_hash_bytes(),
                (ordinal as u64, payload.len),
            )
            .is_some()
        {
            return Err(LixError::unknown(format!(
                "changelog change '{change_id}' contains duplicate inline payload ref"
            )));
        }
    }

    let mut directory_payload_refs = HashSet::with_capacity(directory.payloads.len());
    for location in &directory.payloads {
        let json_ref = location.json_ref.as_hash_bytes();
        if !directory_payload_refs.insert(json_ref) {
            return Err(LixError::unknown(format!(
                "changelog change '{change_id}' contains duplicate payload directory entry"
            )));
        }
        let Some((ordinal, len)) = inline_by_ref.get(&json_ref) else {
            return Err(LixError::unknown(format!(
                "changelog change '{change_id}' payload directory references unknown inline payload"
            )));
        };
        if location.offset != *ordinal || location.len != *len {
            return Err(LixError::unknown(format!(
                "changelog change '{change_id}' payload directory entry does not match inline payload"
            )));
        }
    }

    if directory_payload_refs.len() != inline_by_ref.len() {
        return Err(LixError::unknown(format!(
            "changelog change '{change_id}' is missing payload directory entry"
        )));
    }
    Ok(())
}

fn validate_inline_payload_ref(
    change_id: &str,
    json_ref: &JsonRef,
    bytes: &[u8],
) -> Result<(), LixError> {
    let actual = JsonRef::for_content(bytes);
    if json_ref != &actual {
        return Err(LixError::unknown(format!(
            "changelog change '{change_id}' inline payload ref '{}' does not match payload bytes '{}'",
            json_ref.to_hex(),
            actual.to_hex()
        )));
    }
    Ok(())
}

fn validate_commit_object_consistency(
    header: &CommitHeader,
    body: &CommitBody,
    directory: &SegmentCommitDirectory,
) -> Result<(), LixError> {
    let memberships = body
        .membership
        .iter()
        .map(|membership| CommitMembershipDescriptor {
            member_change_id: Cow::Owned(membership.member_change_id.clone()),
            role: membership.role,
            source_parent_ordinal: membership.source_parent_ordinal,
        })
        .collect::<Vec<_>>();
    validate_commit_membership_descriptors(
        &header.id,
        header.parent_commit_ids.len(),
        &memberships,
    )?;
    validate_commit_directory_descriptors(
        &header.id,
        &memberships,
        directory
            .state_row_identities
            .iter()
            .map(|(identity, change_id)| (identity, change_id.as_str())),
        directory
            .membership_ordinals
            .iter()
            .map(|(change_id, ordinal)| (change_id.as_str(), *ordinal)),
    )
}

fn validate_commit_membership_descriptors(
    commit_id: &str,
    parent_count: usize,
    memberships: &[CommitMembershipDescriptor<'_>],
) -> Result<(), LixError> {
    let mut member_ids = HashSet::with_capacity(memberships.len());
    for membership in memberships {
        match membership.role {
            MembershipRole::Authored => {
                if membership.source_parent_ordinal.is_some() {
                    return Err(LixError::unknown(format!(
                        "changelog commit '{commit_id}' authored membership change '{}' must not record a source_parent_ordinal",
                        membership.member_change_id
                    )));
                }
            }
            MembershipRole::Adopted => {
                let Some(source_parent_ordinal) = membership.source_parent_ordinal else {
                    return Err(LixError::unknown(format!(
                        "changelog commit '{commit_id}' adopted membership change '{}' is missing source_parent_ordinal",
                        membership.member_change_id
                    )));
                };
                if source_parent_ordinal as usize >= parent_count {
                    return Err(LixError::unknown(format!(
                        "changelog commit '{commit_id}' adopted membership change '{}' source_parent_ordinal {} is out of bounds for {} parents",
                        membership.member_change_id, source_parent_ordinal, parent_count
                    )));
                }
            }
        }
        if !member_ids.insert(membership.member_change_id.as_ref()) {
            return Err(LixError::unknown(format!(
                "changelog commit '{commit_id}' contains duplicate membership change '{}'",
                membership.member_change_id
            )));
        }
    }
    Ok(())
}

fn validate_commit_directory_descriptors<'a>(
    commit_id: &str,
    memberships: &[CommitMembershipDescriptor<'_>],
    state_row_identities: impl Iterator<Item = (&'a StateRowIdentity, &'a str)>,
    membership_ordinals: impl Iterator<Item = (&'a str, u32)>,
) -> Result<(), LixError> {
    let member_ids = memberships
        .iter()
        .map(|membership| membership.member_change_id.as_ref())
        .collect::<HashSet<_>>();

    let mut ordinal_ids = HashSet::with_capacity(memberships.len());
    let membership_ordinals_by_id = memberships
        .iter()
        .enumerate()
        .map(|(ordinal, membership)| (membership.member_change_id.as_ref(), ordinal))
        .collect::<HashMap<_, _>>();
    for (change_id, ordinal) in membership_ordinals {
        if !ordinal_ids.insert(change_id) {
            return Err(LixError::unknown(format!(
                "changelog commit '{commit_id}' contains duplicate membership ordinal for change '{change_id}'"
            )));
        }
        let Some(expected_ordinal) = membership_ordinals_by_id.get(change_id) else {
            return Err(LixError::unknown(format!(
                "changelog commit '{commit_id}' membership ordinal references non-member change '{change_id}'"
            )));
        };
        if ordinal as usize != *expected_ordinal {
            return Err(LixError::unknown(format!(
                "changelog commit '{commit_id}' membership ordinal for change '{change_id}' is {ordinal}, expected {expected_ordinal}"
            )));
        }
    }
    for membership in memberships {
        if !ordinal_ids.contains(membership.member_change_id.as_ref()) {
            return Err(LixError::unknown(format!(
                "changelog commit '{commit_id}' is missing membership ordinal for change '{}'",
                membership.member_change_id
            )));
        }
    }

    let mut identities = HashSet::new();
    let mut directory_change_ids = HashSet::with_capacity(memberships.len());
    for (identity, change_id) in state_row_identities {
        if !identities.insert(identity) {
            return Err(LixError::unknown(format!(
                "changelog commit '{commit_id}' contains duplicate StateRowIdentity winner"
            )));
        }
        if !directory_change_ids.insert(change_id) {
            return Err(LixError::unknown(format!(
                "changelog commit '{commit_id}' contains duplicate StateRowIdentity winner for change '{change_id}'"
            )));
        }
        if !member_ids.contains(change_id) {
            return Err(LixError::unknown(format!(
                "changelog commit '{commit_id}' StateRowIdentity winner references non-member change '{change_id}'"
            )));
        }
    }
    for membership in memberships {
        if !directory_change_ids.contains(membership.member_change_id.as_ref()) {
            return Err(LixError::unknown(format!(
                "changelog commit '{commit_id}' membership change '{}' is missing from SegmentCommitDirectory",
                membership.member_change_id
            )));
        }
    }
    Ok(())
}

fn validate_segment_commit_state_row_identities<'a>(
    commits: impl Iterator<
        Item = (
            &'a str,
            &'a [CommitMembershipDescriptor<'a>],
            &'a [(StateRowIdentity, String)],
        ),
    >,
    changes: impl Iterator<Item = Result<(&'a str, ChangeValidationDescriptor<'a>), LixError>>,
) -> Result<(), LixError> {
    let mut changes_by_id = HashMap::new();
    for change in changes {
        let (change_id, descriptor) = change?;
        changes_by_id.insert(change_id, descriptor);
    }
    for (commit_id, memberships, state_row_identities) in commits {
        let memberships_by_id = memberships
            .iter()
            .map(|membership| (membership.member_change_id.as_ref(), membership.role))
            .collect::<HashMap<_, _>>();
        for membership in memberships {
            let Some(change) = changes_by_id.get(membership.member_change_id.as_ref()) else {
                continue;
            };
            match membership.role {
                MembershipRole::Authored => {
                    if change.authored_commit_id != Some(commit_id) {
                        return Err(LixError::unknown(format!(
                            "changelog commit '{commit_id}' authored membership change '{}' has mismatched authored_commit_id",
                            membership.member_change_id
                        )));
                    }
                }
                MembershipRole::Adopted => {
                    if change.authored_commit_id == Some(commit_id) {
                        return Err(LixError::unknown(format!(
                            "changelog commit '{commit_id}' adopted membership change '{}' must not be authored by the same commit",
                            membership.member_change_id
                        )));
                    }
                }
            }
        }
        for (identity, change_id) in state_row_identities {
            let Some(change) = changes_by_id.get(change_id.as_str()) else {
                if memberships_by_id.get(change_id.as_str()) == Some(&MembershipRole::Adopted) {
                    continue;
                }
                return Err(LixError::unknown(format!(
                    "changelog commit '{commit_id}' StateRowIdentity winner references missing authored change '{change_id}'"
                )));
            };
            if change.state_row_identity != *identity {
                return Err(LixError::unknown(format!(
                    "changelog commit '{commit_id}' StateRowIdentity winner for change '{change_id}' does not match changelog.change"
                )));
            }
        }
    }
    Ok(())
}

struct ChangeValidationDescriptor<'a> {
    state_row_identity: StateRowIdentity,
    authored_commit_id: Option<&'a str>,
}

fn state_row_identity_for_change(change: &SegmentChange) -> Result<StateRowIdentity, LixError> {
    state_row_identity_for_change_fields(
        &change.schema_key,
        change.file_id.as_deref(),
        &change.entity_id,
    )
}

fn state_row_identity_for_change_fields(
    schema_key: &str,
    file_id: Option<&str>,
    entity_id: &EntityIdentity,
) -> Result<StateRowIdentity, LixError> {
    Ok(StateRowIdentity {
        schema_key: CanonicalSchemaKey::new(schema_key.to_string())?,
        file_id: FileId::new(file_id.unwrap_or("__global__").to_string())?,
        entity_id: EntityId::new(entity_id.as_json_array_text()?)?,
    })
}

fn read_segment_object_slices_for_header_fast<'a>(
    cursor: &mut ByteCursor<'a>,
    header: &SegmentHeaderView<'_>,
) -> Result<SegmentObjectSliceRead<'a>, LixError> {
    let commit_len = cursor.read_len_fast()?;
    cursor.ensure_len_fits_remaining_fast(commit_len)?;
    cursor.ensure_counted_records_fit_remaining_fast(commit_len, MIN_COMMIT_OBJECT_BYTES)?;
    validate_count_matches_usize("commit_count", header.commit_count, commit_len, "commits")?;
    let mut commits = Vec::with_capacity(commit_len);
    for _ in 0..commit_len {
        commits.push(cursor.read_segment_commit_slice_fast()?);
    }

    let change_len = cursor.read_len_fast()?;
    cursor.ensure_len_fits_remaining_fast(change_len)?;
    cursor.ensure_counted_records_fit_remaining_fast(change_len, MIN_CHANGE_OBJECT_BYTES)?;
    validate_count_matches_usize("change_count", header.change_count, change_len, "changes")?;
    let mut changes = Vec::with_capacity(change_len);
    let mut payload_count = 0_usize;
    let mut remaining_payload_count = header.payload_count as usize;
    for _ in 0..change_len {
        let (change, change_payload_count) =
            cursor.read_segment_change_slice_with_count_fast(Some(remaining_payload_count))?;
        remaining_payload_count = remaining_payload_count
            .checked_sub(change_payload_count)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "failed to decode changelog segment: inline payload count exceeds header payload_count",
                )
            })?;
        payload_count = payload_count
            .checked_add(change_payload_count)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "failed to decode changelog segment: inline payload count overflows",
                )
            })?;
        changes.push(change);
    }

    cursor.expect_end("segment")?;
    validate_count_matches_usize(
        "payload_count",
        header.payload_count,
        payload_count,
        "inline payloads",
    )?;
    validate_segment_commit_state_row_identities(
        commits.iter().map(|commit| {
            (
                commit.slice.id,
                commit.memberships.as_slice(),
                commit.state_row_identities.as_slice(),
            )
        }),
        changes.iter().map(|change| {
            Ok((
                change.slice.id,
                ChangeValidationDescriptor {
                    state_row_identity: change.state_row_identity.clone(),
                    authored_commit_id: change.authored_commit_id,
                },
            ))
        }),
    )?;
    Ok(SegmentObjectSliceRead { commits, changes })
}

fn validate_segment_directory_object_slices_owned(
    segment_id: &str,
    directory: &SegmentDirectory,
    commit_slices: &[SegmentObjectSlice<'_>],
    change_slices: &[SegmentObjectSlice<'_>],
) -> Result<(), LixError> {
    let commits = directory
        .commits
        .iter()
        .map(|(id, location)| (id.as_str(), location.as_ref()));
    let changes = directory
        .changes
        .iter()
        .map(|(id, location)| (id.as_str(), location.as_ref()));
    validate_segment_directory_object_slices(
        segment_id,
        commits,
        changes,
        commit_slices,
        change_slices,
    )
}

fn validate_segment_directory_object_slices_ref(
    segment_id: &str,
    directory_commits: &[SegmentDirectoryEntryRef<'_>],
    directory_changes: &[SegmentDirectoryEntryRef<'_>],
    commit_slices: &[SegmentObjectSlice<'_>],
    change_slices: &[SegmentObjectSlice<'_>],
) -> Result<(), LixError> {
    validate_segment_directory_object_slices(
        segment_id,
        directory_commits
            .iter()
            .map(|entry| (entry.id, entry.location)),
        directory_changes
            .iter()
            .map(|entry| (entry.id, entry.location)),
        commit_slices,
        change_slices,
    )
}

fn validate_segment_directory_object_slices<'a>(
    segment_id: &str,
    directory_commits: impl Iterator<Item = (&'a str, SegmentObjectLocationRef<'a>)>,
    directory_changes: impl Iterator<Item = (&'a str, SegmentObjectLocationRef<'a>)>,
    commit_slices: &[SegmentObjectSlice<'_>],
    change_slices: &[SegmentObjectSlice<'_>],
) -> Result<(), LixError> {
    validate_directory_entries_against_slices(
        segment_id,
        "commit",
        directory_commits,
        commit_slices,
        true,
    )?;
    validate_directory_entries_against_slices(
        segment_id,
        "change",
        directory_changes,
        change_slices,
        false,
    )
}

fn validate_directory_entries_against_slices<'a>(
    segment_id: &str,
    kind: &str,
    entries: impl Iterator<Item = (&'a str, SegmentObjectLocationRef<'a>)>,
    slices: &[SegmentObjectSlice<'_>],
    validate_encoded_checksum: bool,
) -> Result<(), LixError> {
    let mut seen = HashSet::with_capacity(slices.len());
    let mut entry_count = 0_usize;
    for (index, (id, location)) in entries.enumerate() {
        entry_count += 1;
        if location.segment_id != segment_id {
            return Err(LixError::unknown(format!(
                "changelog segment '{segment_id}' {kind} '{id}' locator points to segment '{}'",
                location.segment_id
            )));
        }
        let Some(slice) = slices.get(index) else {
            return Err(LixError::unknown(format!(
                "changelog segment '{segment_id}' contains extra {kind} directory entry '{id}'"
            )));
        };
        if id != slice.id {
            return Err(LixError::unknown(format!(
                "changelog segment '{segment_id}' {kind} directory order does not match encoded object order"
            )));
        }
        if !seen.insert(id) {
            return Err(LixError::unknown(format!(
                "changelog segment '{segment_id}' contains duplicate {kind} directory entry '{id}'"
            )));
        }
        if location.offset != slice.offset || location.len != slice.len {
            return Err(LixError::unknown(format!(
                "changelog {kind} '{id}' locator offset/len does not match encoded byte range"
            )));
        }
        if validate_encoded_checksum {
            if let Some(encoded_checksum) = slice.encoded_checksum {
                if location.checksum != encoded_checksum {
                    return Err(LixError::unknown(format!(
                        "changelog {kind} '{id}' locator checksum does not match encoded object checksum"
                    )));
                }
            }
        }
    }
    if entry_count != slices.len() {
        return Err(LixError::unknown(format!(
            "changelog segment '{segment_id}' {kind} directory count does not match encoded object count"
        )));
    }
    Ok(())
}

fn validate_segment_format_version(format_version: u32, field: &str) -> Result<(), LixError> {
    if format_version != SEGMENT_FORMAT_VERSION {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "failed to decode changelog segment: {field} format_version {format_version} is not supported"
            ),
        ));
    }
    Ok(())
}

fn validate_count_matches_usize(
    header_field: &str,
    header_count: u32,
    actual_count: usize,
    actual_field: &str,
) -> Result<(), LixError> {
    let actual_count = u32::try_from(actual_count).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to decode changelog segment: {actual_field} count exceeds u32"),
        )
    })?;
    if header_count != actual_count {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "failed to decode changelog segment: header {header_field} {header_count} does not match {actual_count} {actual_field}"
            ),
        ));
    }
    Ok(())
}

pub(crate) fn encode_commit_visibility(visibility: &CommitVisibility) -> Result<Vec<u8>, LixError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(COMMIT_VISIBILITY_MAGIC);
    write_str(&mut bytes, &visibility.commit_id)?;
    write_location(&mut bytes, &visibility.location)?;
    write_str(&mut bytes, &visibility.checksum)?;
    Ok(bytes)
}

pub(crate) fn decode_commit_visibility(bytes: &[u8]) -> Result<CommitVisibility, LixError> {
    let mut cursor = ByteCursor::new(bytes);
    cursor.expect_magic(COMMIT_VISIBILITY_MAGIC, "commit visibility")?;
    let commit_id = cursor.read_string("commit_id")?;
    let location = cursor.read_location("location")?;
    let checksum = cursor.read_string("checksum")?;
    cursor.expect_end("commit visibility")?;
    Ok(CommitVisibility {
        commit_id,
        location,
        checksum,
    })
}

pub(crate) fn encode_by_commit_entry(entry: &ByCommitEntry) -> Result<Vec<u8>, LixError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(BY_COMMIT_MAGIC);
    write_str(&mut bytes, &entry.commit_id)?;
    write_location(&mut bytes, &entry.location)?;
    write_strings(
        &mut bytes,
        entry.parent_commit_ids.iter().map(String::as_str),
    )?;
    bytes.extend_from_slice(&entry.generation.to_le_bytes());
    Ok(bytes)
}

pub(crate) fn decode_by_commit_entry(bytes: &[u8]) -> Result<ByCommitEntry, LixError> {
    let mut cursor = ByteCursor::new(bytes);
    cursor.expect_magic(BY_COMMIT_MAGIC, "by_commit entry")?;
    let commit_id = cursor.read_string("commit_id")?;
    let location = cursor.read_location("location")?;
    let parent_commit_ids = cursor.read_strings("parent_commit_ids")?;
    let generation = cursor.read_u64("generation")?;
    cursor.expect_end("by_commit entry")?;
    Ok(ByCommitEntry {
        commit_id,
        location,
        parent_commit_ids,
        generation,
    })
}

pub(crate) fn encode_by_change_entry(entry: &ByChangeEntry) -> Result<Vec<u8>, LixError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(BY_CHANGE_MAGIC);
    write_str(&mut bytes, &entry.change_id)?;
    write_location(&mut bytes, &entry.location)?;
    Ok(bytes)
}

pub(crate) fn decode_by_change_entry(bytes: &[u8]) -> Result<ByChangeEntry, LixError> {
    let mut cursor = ByteCursor::new(bytes);
    cursor.expect_magic(BY_CHANGE_MAGIC, "by_change entry")?;
    let change_id = cursor.read_string("change_id")?;
    let location = cursor.read_location("location")?;
    cursor.expect_end("by_change entry")?;
    Ok(ByChangeEntry {
        change_id,
        location,
    })
}

pub(crate) fn encode_empty_index_value() -> Vec<u8> {
    Vec::new()
}

pub(crate) fn decode_empty_index_value(bytes: &[u8]) -> Result<(), LixError> {
    if bytes.is_empty() {
        return Ok(());
    }
    Err(LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        "changelog empty index value must be zero bytes",
    ))
}

fn write_location(bytes: &mut Vec<u8>, location: &SegmentObjectLocation) -> Result<(), LixError> {
    write_str(bytes, &location.segment_id)?;
    bytes.extend_from_slice(&location.offset.to_le_bytes());
    bytes.extend_from_slice(&location.len.to_le_bytes());
    write_str(bytes, &location.checksum)
}

fn write_segment_header(bytes: &mut Vec<u8>, header: &SegmentHeader) -> Result<(), LixError> {
    write_str(bytes, &header.segment_id)?;
    write_u32(bytes, header.format_version);
    write_u32(bytes, header.commit_count);
    write_u32(bytes, header.change_count);
    write_u64(bytes, header.byte_count);
    write_u32(bytes, header.payload_count);
    write_str(bytes, &header.checksum)
}

fn write_segment_directory(
    bytes: &mut Vec<u8>,
    directory: &SegmentDirectory,
) -> Result<(), LixError> {
    write_len(bytes, directory.commits.len(), "segment directory commits")?;
    for (commit_id, location) in &directory.commits {
        write_str(bytes, commit_id)?;
        write_location(bytes, location)?;
    }
    write_len(bytes, directory.changes.len(), "segment directory changes")?;
    for (change_id, location) in &directory.changes {
        write_str(bytes, change_id)?;
        write_location(bytes, location)?;
    }
    Ok(())
}

fn write_segment_commit(bytes: &mut Vec<u8>, commit: &SegmentCommit) -> Result<(), LixError> {
    write_commit_header(bytes, &commit.header)?;
    write_commit_body(bytes, &commit.body)?;
    write_segment_commit_directory(bytes, &commit.directory)?;
    write_str(bytes, &commit.checksum)
}

fn write_commit_header(bytes: &mut Vec<u8>, header: &CommitHeader) -> Result<(), LixError> {
    write_str(bytes, &header.id)?;
    write_strings(bytes, header.parent_commit_ids.iter().map(String::as_str))?;
    write_str(bytes, &header.derivable_change_id)?;
    write_strings(bytes, header.author_account_ids.iter().map(String::as_str))?;
    write_str(bytes, &header.created_at)?;
    write_u32(bytes, header.membership_count);
    Ok(())
}

fn write_commit_body(bytes: &mut Vec<u8>, body: &CommitBody) -> Result<(), LixError> {
    write_len(bytes, body.membership.len(), "commit membership")?;
    for record in &body.membership {
        write_membership_record(bytes, record)?;
    }
    Ok(())
}

fn write_membership_record(bytes: &mut Vec<u8>, record: &MembershipRecord) -> Result<(), LixError> {
    write_str(bytes, &record.member_change_id)?;
    write_membership_role(bytes, record.role);
    write_optional_u32(bytes, record.source_parent_ordinal);
    Ok(())
}

fn write_segment_commit_directory(
    bytes: &mut Vec<u8>,
    directory: &SegmentCommitDirectory,
) -> Result<(), LixError> {
    write_len(
        bytes,
        directory.state_row_identities.len(),
        "segment commit directory state row identities",
    )?;
    for (identity, change_id) in &directory.state_row_identities {
        write_state_row_identity(bytes, identity)?;
        write_str(bytes, change_id)?;
    }
    write_len(
        bytes,
        directory.membership_ordinals.len(),
        "segment commit directory membership ordinals",
    )?;
    for (change_id, ordinal) in &directory.membership_ordinals {
        write_str(bytes, change_id)?;
        write_u32(bytes, *ordinal);
    }
    Ok(())
}

fn write_segment_change(bytes: &mut Vec<u8>, change: &SegmentChange) -> Result<(), LixError> {
    write_str(bytes, &change.id)?;
    write_optional_str(bytes, change.authored_commit_id.as_deref())?;
    write_entity_identity(bytes, &change.entity_id)?;
    write_str(bytes, &change.schema_key)?;
    write_optional_str(bytes, change.file_id.as_deref())?;
    write_optional_json_ref(bytes, change.snapshot_ref.as_ref());
    write_optional_json_ref(bytes, change.metadata_ref.as_ref());
    write_str(bytes, &change.created_at)?;
    write_len(
        bytes,
        change.inline_payloads.len(),
        "segment inline payloads",
    )?;
    for payload in &change.inline_payloads {
        write_segment_inline_payload(bytes, payload)?;
    }
    write_segment_change_directory(bytes, &change.directory)
}

fn write_segment_inline_payload(
    bytes: &mut Vec<u8>,
    payload: &SegmentInlinePayload,
) -> Result<(), LixError> {
    write_json_ref(bytes, &payload.json_ref);
    write_bytes(bytes, &payload.bytes)
}

fn write_segment_change_directory(
    bytes: &mut Vec<u8>,
    directory: &SegmentChangeDirectory,
) -> Result<(), LixError> {
    write_len(
        bytes,
        directory.payloads.len(),
        "segment change directory payloads",
    )?;
    for location in &directory.payloads {
        write_payload_location(bytes, location);
    }
    Ok(())
}

fn write_payload_location(bytes: &mut Vec<u8>, location: &SegmentPayloadLocation) {
    write_json_ref(bytes, &location.json_ref);
    write_u64(bytes, location.offset);
    write_u64(bytes, location.len);
}

fn write_state_row_identity(
    bytes: &mut Vec<u8>,
    identity: &StateRowIdentity,
) -> Result<(), LixError> {
    write_str(bytes, identity.schema_key.as_str())?;
    write_str(bytes, identity.file_id.as_str())?;
    write_str(bytes, identity.entity_id.as_str())
}

fn write_entity_identity(bytes: &mut Vec<u8>, identity: &EntityIdentity) -> Result<(), LixError> {
    write_strings(bytes, identity.parts.iter().map(String::as_str))
}

fn write_membership_role(bytes: &mut Vec<u8>, role: MembershipRole) {
    write_u8(
        bytes,
        match role {
            MembershipRole::Authored => 0,
            MembershipRole::Adopted => 1,
        },
    );
}

fn write_optional_str(bytes: &mut Vec<u8>, value: Option<&str>) -> Result<(), LixError> {
    match value {
        Some(value) => {
            write_bool(bytes, true);
            write_str(bytes, value)
        }
        None => {
            write_bool(bytes, false);
            Ok(())
        }
    }
}

fn write_optional_u32(bytes: &mut Vec<u8>, value: Option<u32>) {
    match value {
        Some(value) => {
            write_bool(bytes, true);
            write_u32(bytes, value);
        }
        None => write_bool(bytes, false),
    }
}

fn write_json_ref(bytes: &mut Vec<u8>, json_ref: &JsonRef) {
    bytes.extend_from_slice(json_ref.as_hash_bytes());
}

fn write_optional_json_ref(bytes: &mut Vec<u8>, json_ref: Option<&JsonRef>) {
    match json_ref {
        Some(json_ref) => {
            write_bool(bytes, true);
            write_json_ref(bytes, json_ref);
        }
        None => write_bool(bytes, false),
    }
}

fn write_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<(), LixError> {
    write_len(bytes, value.len(), "byte vector")?;
    bytes.extend_from_slice(value);
    Ok(())
}

fn write_len(bytes: &mut Vec<u8>, len: usize, label: &str) -> Result<(), LixError> {
    let len = u32::try_from(len).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("changelog codec {label} length exceeds u32"),
        )
    })?;
    write_u32(bytes, len);
    Ok(())
}

fn write_bool(bytes: &mut Vec<u8>, value: bool) {
    write_u8(bytes, u8::from(value));
}

fn write_u8(bytes: &mut Vec<u8>, value: u8) {
    bytes.push(value);
}

fn write_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn write_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn write_str(bytes: &mut Vec<u8>, value: &str) -> Result<(), LixError> {
    let len = u32::try_from(value.len()).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "changelog codec string exceeds u32 length",
        )
    })?;
    write_u32(bytes, len);
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn write_strings<'a>(
    bytes: &mut Vec<u8>,
    values: impl ExactSizeIterator<Item = &'a str>,
) -> Result<(), LixError> {
    let len = u32::try_from(values.len()).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "changelog codec string list exceeds u32 length",
        )
    })?;
    write_u32(bytes, len);
    for value in values {
        write_str(bytes, value)?;
    }
    Ok(())
}

#[derive(Clone)]
struct ByteCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> ByteCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn expect_magic(&mut self, magic: &[u8], label: &str) -> Result<(), LixError> {
        let actual = self.read_bytes(magic.len(), "magic")?;
        if actual != magic {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode changelog {label}: invalid magic"),
            ));
        }
        Ok(())
    }

    fn read_location(&mut self, field: &str) -> Result<SegmentObjectLocation, LixError> {
        let segment_id = self.read_string(&format!("{field}.segment_id"))?;
        let offset = self.read_u64(&format!("{field}.offset"))?;
        let len = self.read_u64(&format!("{field}.len"))?;
        let checksum = self.read_string(&format!("{field}.checksum"))?;
        Ok(SegmentObjectLocation {
            segment_id,
            offset,
            len,
            checksum,
        })
    }

    fn read_location_view(
        &mut self,
        field: &str,
    ) -> Result<SegmentObjectLocationRef<'a>, LixError> {
        let segment_id = self.read_string_ref(&format!("{field}.segment_id"))?;
        let offset = self.read_u64(&format!("{field}.offset"))?;
        let len = self.read_u64(&format!("{field}.len"))?;
        let checksum = self.read_string_ref(&format!("{field}.checksum"))?;
        Ok(SegmentObjectLocationRef {
            segment_id,
            offset,
            len,
            checksum,
        })
    }

    fn read_location_view_fast(&mut self) -> Result<SegmentObjectLocationRef<'a>, LixError> {
        let segment_id = self.read_string_ref_fast()?;
        let offset = self.read_u64_fast()?;
        let len = self.read_u64_fast()?;
        let checksum = self.read_string_ref_fast()?;
        Ok(SegmentObjectLocationRef {
            segment_id,
            offset,
            len,
            checksum,
        })
    }

    fn read_segment_header(&mut self, field: &str) -> Result<SegmentHeader, LixError> {
        let header = SegmentHeader {
            segment_id: self.read_string(&format!("{field}.segment_id"))?,
            format_version: self.read_u32(&format!("{field}.format_version"))?,
            commit_count: self.read_u32(&format!("{field}.commit_count"))?,
            change_count: self.read_u32(&format!("{field}.change_count"))?,
            byte_count: self.read_u64(&format!("{field}.byte_count"))?,
            payload_count: self.read_u32(&format!("{field}.payload_count"))?,
            checksum: self.read_string(&format!("{field}.checksum"))?,
        };
        validate_segment_format_version(header.format_version, field)?;
        Ok(header)
    }

    fn read_segment_header_view(&mut self, field: &str) -> Result<SegmentHeaderView<'a>, LixError> {
        let header = SegmentHeaderView {
            segment_id: self.read_string_ref(&format!("{field}.segment_id"))?,
            format_version: self.read_u32(&format!("{field}.format_version"))?,
            commit_count: self.read_u32(&format!("{field}.commit_count"))?,
            change_count: self.read_u32(&format!("{field}.change_count"))?,
            byte_count: self.read_u64(&format!("{field}.byte_count"))?,
            payload_count: self.read_u32(&format!("{field}.payload_count"))?,
            checksum: self.read_string_ref(&format!("{field}.checksum"))?,
        };
        validate_segment_format_version(header.format_version, field)?;
        Ok(header)
    }

    fn read_segment_header_view_fast(&mut self) -> Result<SegmentHeaderView<'a>, LixError> {
        let header = SegmentHeaderView {
            segment_id: self.read_string_ref_fast()?,
            format_version: self.read_u32_fast()?,
            commit_count: self.read_u32_fast()?,
            change_count: self.read_u32_fast()?,
            byte_count: self.read_u64_fast()?,
            payload_count: self.read_u32_fast()?,
            checksum: self.read_string_ref_fast()?,
        };
        validate_segment_format_version(header.format_version, "header")?;
        Ok(header)
    }

    fn read_segment_directory(
        &mut self,
        field: &str,
        expected_commit_count: usize,
        expected_change_count: usize,
    ) -> Result<SegmentDirectory, LixError> {
        let commit_len = self.read_len(&format!("{field}.commits"))?;
        self.ensure_len_fits_remaining(commit_len, &format!("{field}.commits"))?;
        self.ensure_counted_records_fit_remaining(
            commit_len,
            MIN_DIRECTORY_ENTRY_BYTES,
            &format!("{field}.commits"),
        )?;
        validate_count_matches_usize(
            "commit_count",
            expected_commit_count as u32,
            commit_len,
            "directory commits",
        )?;
        let mut commits = Vec::with_capacity(commit_len);
        for index in 0..commit_len {
            let commit_id = self.read_string(&format!("{field}.commits[{index}].commit_id"))?;
            let location = self.read_location(&format!("{field}.commits[{index}].location"))?;
            commits.push((commit_id, location));
        }

        let change_len = self.read_len(&format!("{field}.changes"))?;
        self.ensure_len_fits_remaining(change_len, &format!("{field}.changes"))?;
        self.ensure_counted_records_fit_remaining(
            change_len,
            MIN_DIRECTORY_ENTRY_BYTES,
            &format!("{field}.changes"),
        )?;
        validate_count_matches_usize(
            "change_count",
            expected_change_count as u32,
            change_len,
            "directory changes",
        )?;
        let mut changes = Vec::with_capacity(change_len);
        for index in 0..change_len {
            let change_id = self.read_string(&format!("{field}.changes[{index}].change_id"))?;
            let location = self.read_location(&format!("{field}.changes[{index}].location"))?;
            changes.push((change_id, location));
        }

        Ok(SegmentDirectory { commits, changes })
    }

    fn read_segment_directory_commit_views(
        &mut self,
        field: &str,
    ) -> Result<Vec<SegmentDirectoryEntryRef<'a>>, LixError> {
        let len = self.read_len(field)?;
        self.ensure_len_fits_remaining(len, field)?;
        self.ensure_counted_records_fit_remaining(len, MIN_DIRECTORY_ENTRY_BYTES, field)?;
        let mut commits = Vec::with_capacity(len);
        for index in 0..len {
            let id = self.read_string_ref(&format!("{field}[{index}].commit_id"))?;
            let location = self.read_location_view(&format!("{field}[{index}].location"))?;
            commits.push(SegmentDirectoryEntryRef { id, location });
        }
        Ok(commits)
    }

    fn read_segment_directory_commit_views_fast(
        &mut self,
        expected_len: usize,
    ) -> Result<Vec<SegmentDirectoryEntryRef<'a>>, LixError> {
        let len = self.read_len_fast()?;
        self.ensure_len_fits_remaining_fast(len)?;
        self.ensure_counted_records_fit_remaining_fast(len, MIN_DIRECTORY_ENTRY_BYTES)?;
        validate_count_matches_usize(
            "commit_count",
            expected_len as u32,
            len,
            "directory commits",
        )?;
        let mut commits = Vec::with_capacity(len);
        for _ in 0..len {
            let id = self.read_string_ref_fast()?;
            let location = self.read_location_view_fast()?;
            commits.push(SegmentDirectoryEntryRef { id, location });
        }
        Ok(commits)
    }

    fn read_segment_directory_change_views(
        &mut self,
        field: &str,
    ) -> Result<Vec<SegmentDirectoryEntryRef<'a>>, LixError> {
        let len = self.read_len(field)?;
        self.ensure_len_fits_remaining(len, field)?;
        self.ensure_counted_records_fit_remaining(len, MIN_DIRECTORY_ENTRY_BYTES, field)?;
        let mut changes = Vec::with_capacity(len);
        for index in 0..len {
            let id = self.read_string_ref(&format!("{field}[{index}].change_id"))?;
            let location = self.read_location_view(&format!("{field}[{index}].location"))?;
            changes.push(SegmentDirectoryEntryRef { id, location });
        }
        Ok(changes)
    }

    fn read_segment_directory_change_views_fast(
        &mut self,
        expected_len: usize,
    ) -> Result<Vec<SegmentDirectoryEntryRef<'a>>, LixError> {
        let len = self.read_len_fast()?;
        self.ensure_len_fits_remaining_fast(len)?;
        self.ensure_counted_records_fit_remaining_fast(len, MIN_DIRECTORY_ENTRY_BYTES)?;
        validate_count_matches_usize(
            "change_count",
            expected_len as u32,
            len,
            "directory changes",
        )?;
        let mut changes = Vec::with_capacity(len);
        for _ in 0..len {
            let id = self.read_string_ref_fast()?;
            let location = self.read_location_view_fast()?;
            changes.push(SegmentDirectoryEntryRef { id, location });
        }
        Ok(changes)
    }

    fn read_segment_commit(&mut self, field: &str) -> Result<SegmentCommit, LixError> {
        let header = self.read_commit_header(&format!("{field}.header"))?;
        let body =
            self.read_commit_body(&format!("{field}.body"), header.membership_count as usize)?;
        let directory = self.read_segment_commit_directory(&format!("{field}.directory"))?;
        validate_commit_object_consistency(&header, &body, &directory)?;
        let commit = SegmentCommit {
            header,
            body,
            directory,
            checksum: self.read_string(&format!("{field}.checksum"))?,
        };
        validate_commit_checksum(&commit)?;
        Ok(commit)
    }

    fn read_segment_commit_slice(
        &mut self,
        field: &str,
    ) -> Result<SegmentObjectSlice<'a>, LixError> {
        let start = self.offset;
        let id = self.read_string_ref(&format!("{field}.header.id"))?;
        self.skip_strings(&format!("{field}.header.parent_commit_ids"))?;
        self.skip_string(&format!("{field}.header.derivable_change_id"))?;
        self.skip_strings(&format!("{field}.header.author_account_ids"))?;
        self.skip_string(&format!("{field}.header.created_at"))?;
        let membership_count = self.read_u32(&format!("{field}.header.membership_count"))?;
        self.skip_commit_body(&format!("{field}.body"), membership_count as usize)?;
        self.skip_segment_commit_directory(&format!("{field}.directory"))?;
        let checksum = self.read_string_ref(&format!("{field}.checksum"))?;
        let end = self.offset;
        Ok(SegmentObjectSlice {
            id,
            offset: start as u64,
            len: (end - start) as u64,
            encoded_checksum: Some(checksum),
            bytes: &self.bytes[start..end],
        })
    }

    fn read_segment_commit_slice_fast(&mut self) -> Result<SegmentCommitSliceRead<'a>, LixError> {
        let start = self.offset;
        let id = self.read_string_ref_fast()?;
        let parent_commit_ids = self.read_strings_fast_owned()?;
        let parent_count = parent_commit_ids.len();
        let derivable_change_id = self.read_string_ref_fast()?;
        let author_account_ids = self.read_strings_fast_owned()?;
        let created_at = self.read_string_ref_fast()?;
        let membership_count = self.read_u32_fast()? as usize;
        let memberships = self.read_commit_body_descriptors_fast(membership_count)?;
        let directory =
            self.read_segment_commit_directory_fast_validated(id, parent_count, &memberships)?;
        let checksum = self.read_string_ref_fast()?;
        let canonical = checksum_commit_parts(
            id,
            parent_commit_ids.iter().map(String::as_str),
            derivable_change_id,
            author_account_ids.iter().map(String::as_str),
            created_at,
            membership_count as u32,
            &memberships,
            &directory,
        )?;
        if checksum != canonical {
            return Err(LixError::unknown(format!(
                "changelog commit '{id}' checksum '{checksum}' does not match canonical checksum '{canonical}'"
            )));
        }
        let end = self.offset;
        Ok(SegmentCommitSliceRead {
            slice: SegmentObjectSlice {
                id,
                offset: start as u64,
                len: (end - start) as u64,
                encoded_checksum: Some(checksum),
                bytes: &self.bytes[start..end],
            },
            memberships,
            state_row_identities: directory.state_row_identities,
            checksum: canonical,
        })
    }

    fn read_commit_header(&mut self, field: &str) -> Result<CommitHeader, LixError> {
        Ok(CommitHeader {
            id: self.read_string(&format!("{field}.id"))?,
            parent_commit_ids: self.read_strings(&format!("{field}.parent_commit_ids"))?,
            derivable_change_id: self.read_string(&format!("{field}.derivable_change_id"))?,
            author_account_ids: self.read_strings(&format!("{field}.author_account_ids"))?,
            created_at: self.read_string(&format!("{field}.created_at"))?,
            membership_count: self.read_u32(&format!("{field}.membership_count"))?,
        })
    }

    fn read_commit_header_fast(&mut self) -> Result<CommitHeaderView<'a>, LixError> {
        let id = self.read_string_ref_fast()?;
        let parent_count = self.read_strings_fast_owned()?.len();
        self.skip_string_fast()?;
        self.read_strings_fast_owned()?;
        self.skip_string_fast()?;
        let membership_count = self.read_u32_fast()?;
        Ok(CommitHeaderView {
            id,
            parent_count,
            membership_count,
        })
    }

    fn read_commit_body(
        &mut self,
        field: &str,
        expected_membership_count: usize,
    ) -> Result<CommitBody, LixError> {
        let len = self.read_len(&format!("{field}.membership"))?;
        self.ensure_len_fits_remaining(len, &format!("{field}.membership"))?;
        self.ensure_counted_records_fit_remaining(
            len,
            MIN_MEMBERSHIP_RECORD_BYTES,
            &format!("{field}.membership"),
        )?;
        validate_count_matches_usize(
            "membership_count",
            expected_membership_count as u32,
            len,
            "membership records",
        )?;
        let mut membership = Vec::with_capacity(len);
        for index in 0..len {
            membership.push(self.read_membership_record(&format!("{field}.membership[{index}]"))?);
        }
        Ok(CommitBody { membership })
    }

    fn read_commit_body_descriptors_fast(
        &mut self,
        expected_membership_count: usize,
    ) -> Result<Vec<CommitMembershipDescriptor<'a>>, LixError> {
        let len = self.read_len_fast()?;
        self.ensure_len_fits_remaining_fast(len)?;
        self.ensure_counted_records_fit_remaining_fast(len, MIN_MEMBERSHIP_RECORD_BYTES)?;
        validate_count_matches_usize(
            "membership_count",
            expected_membership_count as u32,
            len,
            "membership records",
        )?;
        let mut memberships = Vec::with_capacity(len);
        for _ in 0..len {
            memberships.push(CommitMembershipDescriptor {
                member_change_id: Cow::Borrowed(self.read_string_ref_fast()?),
                role: self.read_membership_role_fast()?,
                source_parent_ordinal: self.read_optional_u32_fast()?,
            });
        }
        Ok(memberships)
    }

    fn read_membership_record(&mut self, field: &str) -> Result<MembershipRecord, LixError> {
        Ok(MembershipRecord {
            member_change_id: self.read_string(&format!("{field}.member_change_id"))?,
            role: self.read_membership_role(&format!("{field}.role"))?,
            source_parent_ordinal: self
                .read_optional_u32(&format!("{field}.source_parent_ordinal"))?,
        })
    }

    fn read_matching_membership_change_ids(
        &mut self,
        expected_membership_count: usize,
        requested_change_ids: &std::collections::HashSet<String>,
    ) -> Result<(Vec<String>, Vec<CommitMembershipDescriptor<'a>>), LixError> {
        let len = self.read_len_fast()?;
        self.ensure_len_fits_remaining_fast(len)?;
        self.ensure_counted_records_fit_remaining_fast(len, MIN_MEMBERSHIP_RECORD_BYTES)?;
        validate_count_matches_usize(
            "membership_count",
            expected_membership_count as u32,
            len,
            "membership records",
        )?;
        let mut matches = Vec::new();
        let mut memberships = Vec::with_capacity(len);
        for _ in 0..len {
            let member_change_id = self.read_string_ref_fast()?;
            if requested_change_ids.contains(member_change_id) {
                matches.push(member_change_id.to_string());
            }
            let role = self.read_membership_role_fast()?;
            let source_parent_ordinal = self.read_optional_u32_fast()?;
            memberships.push(CommitMembershipDescriptor {
                member_change_id: Cow::Borrowed(member_change_id),
                role,
                source_parent_ordinal,
            });
        }
        Ok((matches, memberships))
    }

    fn read_segment_commit_directory(
        &mut self,
        field: &str,
    ) -> Result<SegmentCommitDirectory, LixError> {
        let identity_len = self.read_len(&format!("{field}.state_row_identities"))?;
        self.ensure_len_fits_remaining(identity_len, &format!("{field}.state_row_identities"))?;
        self.ensure_counted_records_fit_remaining(
            identity_len,
            MIN_STATE_ROW_IDENTITY_ENTRY_BYTES,
            &format!("{field}.state_row_identities"),
        )?;
        let mut state_row_identities = Vec::with_capacity(identity_len);
        for index in 0..identity_len {
            let identity = self.read_state_row_identity(&format!(
                "{field}.state_row_identities[{index}].state_row_identity"
            ))?;
            let change_id =
                self.read_string(&format!("{field}.state_row_identities[{index}].change_id"))?;
            state_row_identities.push((identity, change_id));
        }

        let ordinal_len = self.read_len(&format!("{field}.membership_ordinals"))?;
        self.ensure_len_fits_remaining(ordinal_len, &format!("{field}.membership_ordinals"))?;
        self.ensure_counted_records_fit_remaining(
            ordinal_len,
            MIN_MEMBERSHIP_ORDINAL_BYTES,
            &format!("{field}.membership_ordinals"),
        )?;
        let mut membership_ordinals = Vec::with_capacity(ordinal_len);
        for index in 0..ordinal_len {
            let change_id =
                self.read_string(&format!("{field}.membership_ordinals[{index}].change_id"))?;
            let ordinal =
                self.read_u32(&format!("{field}.membership_ordinals[{index}].ordinal"))?;
            membership_ordinals.push((change_id, ordinal));
        }

        Ok(SegmentCommitDirectory {
            state_row_identities,
            membership_ordinals,
        })
    }

    fn read_segment_change(
        &mut self,
        field: &str,
        payload_budget: Option<&mut usize>,
    ) -> Result<SegmentChange, LixError> {
        let id = self.read_string(&format!("{field}.id"))?;
        let authored_commit_id =
            self.read_optional_string(&format!("{field}.authored_commit_id"))?;
        let entity_id = self.read_entity_identity(&format!("{field}.entity_id"))?;
        let schema_key = self.read_string(&format!("{field}.schema_key"))?;
        let file_id = self.read_optional_string(&format!("{field}.file_id"))?;
        let snapshot_ref = self.read_optional_json_ref(&format!("{field}.snapshot_ref"))?;
        let metadata_ref = self.read_optional_json_ref(&format!("{field}.metadata_ref"))?;
        let created_at = self.read_string(&format!("{field}.created_at"))?;

        let payload_len = self.read_len(&format!("{field}.inline_payloads"))?;
        self.ensure_len_fits_remaining(payload_len, &format!("{field}.inline_payloads"))?;
        self.ensure_counted_records_fit_remaining(
            payload_len,
            MIN_INLINE_PAYLOAD_BYTES,
            &format!("{field}.inline_payloads"),
        )?;
        if let Some(payload_budget) = payload_budget {
            *payload_budget = payload_budget.checked_sub(payload_len).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "failed to decode changelog {field}.inline_payloads: declared inline payload count exceeds header payload_count"
                    ),
                )
            })?;
        }
        let mut inline_payloads = Vec::with_capacity(payload_len);
        let mut payload_descriptors = Vec::with_capacity(payload_len);
        for index in 0..payload_len {
            let payload =
                self.read_segment_inline_payload_ref(&format!("{field}.inline_payloads[{index}]"))?;
            validate_inline_payload_ref(&id, &payload.json_ref, payload.bytes)?;
            payload_descriptors.push(PayloadDescriptor {
                json_ref: payload.json_ref.clone(),
                len: payload.bytes.len() as u64,
            });
            inline_payloads.push(payload);
        }

        let directory =
            self.read_segment_change_directory(&format!("{field}.directory"), payload_len)?;
        validate_payload_descriptors_against_directory(&id, &payload_descriptors, &directory)?;
        let inline_payloads = inline_payloads
            .into_iter()
            .map(|payload| SegmentInlinePayload {
                json_ref: payload.json_ref,
                bytes: payload.bytes.to_vec(),
            })
            .collect();

        Ok(SegmentChange {
            id,
            authored_commit_id,
            entity_id,
            schema_key,
            file_id,
            snapshot_ref,
            metadata_ref,
            created_at,
            inline_payloads,
            directory,
        })
    }

    fn read_segment_change_slice(
        &mut self,
        field: &str,
    ) -> Result<SegmentObjectSlice<'a>, LixError> {
        let start = self.offset;
        let id = self.read_string_ref(&format!("{field}.id"))?;
        self.skip_optional_string(&format!("{field}.authored_commit_id"))?;
        self.read_entity_identity(&format!("{field}.entity_id"))?;
        self.skip_string(&format!("{field}.schema_key"))?;
        self.skip_optional_string(&format!("{field}.file_id"))?;
        self.skip_optional_json_ref(&format!("{field}.snapshot_ref"))?;
        self.skip_optional_json_ref(&format!("{field}.metadata_ref"))?;
        self.skip_string(&format!("{field}.created_at"))?;
        let payloads = self.skip_segment_inline_payloads(&format!("{field}.inline_payloads"))?;
        self.skip_segment_change_directory(&format!("{field}.directory"), id, &payloads)?;
        let end = self.offset;
        Ok(SegmentObjectSlice {
            id,
            offset: start as u64,
            len: (end - start) as u64,
            encoded_checksum: None,
            bytes: &self.bytes[start..end],
        })
    }

    fn read_segment_change_slice_fast(&mut self) -> Result<SegmentObjectSlice<'a>, LixError> {
        self.read_segment_change_slice_with_count_fast(None)
            .map(|(change, _)| change.slice)
    }

    fn read_segment_change_slice_with_count_fast(
        &mut self,
        payload_budget: Option<usize>,
    ) -> Result<(SegmentChangeSliceRead<'a>, usize), LixError> {
        let start = self.offset;
        let id = self.read_string_ref_fast()?;
        let authored_commit_id = self.read_optional_string_ref_fast()?;
        let entity_id = self.read_entity_identity_fast()?;
        let schema_key = self.read_string_ref_fast()?;
        let file_id = self.read_optional_string_ref_fast()?;
        let snapshot_ref = self.read_optional_json_ref_fast()?;
        let metadata_ref = self.read_optional_json_ref_fast()?;
        let created_at = self.read_string_ref_fast()?;
        let payloads = self.read_segment_inline_payloads_fast(payload_budget)?;
        for payload in &payloads {
            validate_inline_payload_ref(id, &payload.json_ref, payload.bytes)?;
        }
        let payload_descriptors = payloads
            .iter()
            .map(|payload| PayloadDescriptor {
                json_ref: payload.json_ref.clone(),
                len: payload.bytes.len() as u64,
            })
            .collect::<Vec<_>>();
        let directory = self.read_segment_change_directory_fast(id, &payload_descriptors)?;
        let checksum = checksum_change_parts(
            id,
            authored_commit_id,
            &entity_id,
            schema_key,
            file_id,
            snapshot_ref.as_ref(),
            metadata_ref.as_ref(),
            created_at,
            &payloads,
            &directory,
        )?;
        let end = self.offset;
        Ok((
            SegmentChangeSliceRead {
                slice: SegmentObjectSlice {
                    id,
                    offset: start as u64,
                    len: (end - start) as u64,
                    encoded_checksum: None,
                    bytes: &self.bytes[start..end],
                },
                state_row_identity: state_row_identity_for_change_fields(
                    schema_key, file_id, &entity_id,
                )?,
                authored_commit_id,
                checksum,
            },
            payloads.len(),
        ))
    }

    fn remaining_bytes(&self) -> &'a [u8] {
        &self.bytes[self.offset..]
    }

    fn read_segment_inline_payload_ref(
        &mut self,
        field: &str,
    ) -> Result<SegmentInlinePayloadRef<'a>, LixError> {
        Ok(SegmentInlinePayloadRef {
            json_ref: self.read_json_ref(&format!("{field}.json_ref"))?,
            bytes: self.read_byte_vec_ref(&format!("{field}.bytes"))?,
        })
    }

    fn read_segment_change_directory(
        &mut self,
        field: &str,
        expected_payload_count: usize,
    ) -> Result<SegmentChangeDirectory, LixError> {
        let len = self.read_len(&format!("{field}.payloads"))?;
        self.ensure_len_fits_remaining(len, &format!("{field}.payloads"))?;
        self.ensure_counted_records_fit_remaining(
            len,
            MIN_PAYLOAD_LOCATION_BYTES,
            &format!("{field}.payloads"),
        )?;
        validate_count_matches_usize(
            "payload_count",
            expected_payload_count as u32,
            len,
            "change directory payloads",
        )?;
        let mut payloads = Vec::with_capacity(len);
        for index in 0..len {
            payloads.push(self.read_payload_location(&format!("{field}.payloads[{index}]"))?);
        }
        Ok(SegmentChangeDirectory { payloads })
    }

    fn read_payload_location(&mut self, field: &str) -> Result<SegmentPayloadLocation, LixError> {
        Ok(SegmentPayloadLocation {
            json_ref: self.read_json_ref(&format!("{field}.json_ref"))?,
            offset: self.read_u64(&format!("{field}.offset"))?,
            len: self.read_u64(&format!("{field}.len"))?,
        })
    }

    fn read_state_row_identity(&mut self, field: &str) -> Result<StateRowIdentity, LixError> {
        Ok(StateRowIdentity {
            schema_key: CanonicalSchemaKey::new(self.read_string(&format!("{field}.schema_key"))?)?,
            file_id: FileId::new(self.read_string(&format!("{field}.file_id"))?)?,
            entity_id: EntityId::new(self.read_string(&format!("{field}.entity_id"))?)?,
        })
    }

    fn read_state_row_identity_fast(&mut self) -> Result<StateRowIdentity, LixError> {
        let schema_key = self.read_string_ref_fast()?;
        let schema_key = CanonicalSchemaKey::new(schema_key.to_string()).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode changelog state row identity schema_key: {error}"),
            )
        })?;
        let file_id = self.read_string_ref_fast()?;
        let file_id = FileId::new(file_id.to_string()).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode changelog state row identity file_id: {error}"),
            )
        })?;
        let entity_id = self.read_string_ref_fast()?;
        let entity_id = EntityId::new(entity_id.to_string()).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode changelog state row identity entity_id: {error}"),
            )
        })?;
        Ok(StateRowIdentity {
            schema_key,
            file_id,
            entity_id,
        })
    }

    fn read_entity_identity(&mut self, field: &str) -> Result<EntityIdentity, LixError> {
        let parts = self.read_strings_fast_owned().map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode changelog {field}: {error}"),
            )
        })?;
        EntityIdentity::from_parts(parts).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "failed to decode changelog {field}: invalid entity identity parts: {error}"
                ),
            )
        })
    }

    fn read_entity_identity_fast(&mut self) -> Result<EntityIdentity, LixError> {
        let parts = self.read_strings_fast_owned()?;
        EntityIdentity::from_parts(parts).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "failed to decode changelog entity_id: invalid entity identity parts: {error}"
                ),
            )
        })
    }

    fn read_membership_role(&mut self, field: &str) -> Result<MembershipRole, LixError> {
        match self.read_u8(field)? {
            0 => Ok(MembershipRole::Authored),
            1 => Ok(MembershipRole::Adopted),
            value => Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode changelog {field}: invalid membership role {value}"),
            )),
        }
    }

    fn read_membership_role_fast(&mut self) -> Result<MembershipRole, LixError> {
        match self.read_u8_fast()? {
            0 => Ok(MembershipRole::Authored),
            1 => Ok(MembershipRole::Adopted),
            _ => Err(self.fast_error("invalid membership role")),
        }
    }

    fn read_strings(&mut self, field: &str) -> Result<Vec<String>, LixError> {
        let len = self.read_len(field)?;
        self.ensure_len_fits_remaining(len, field)?;
        self.ensure_counted_records_fit_remaining(len, MIN_STRING_BYTES, field)?;
        let mut out = Vec::with_capacity(len);
        for index in 0..len {
            out.push(self.read_string(&format!("{field}[{index}]"))?);
        }
        Ok(out)
    }

    fn read_strings_fast_owned(&mut self) -> Result<Vec<String>, LixError> {
        let len = self.read_len_fast()?;
        self.ensure_len_fits_remaining_fast(len)?;
        self.ensure_counted_records_fit_remaining_fast(len, MIN_STRING_BYTES)?;
        let mut out = Vec::with_capacity(len);
        for _ in 0..len {
            out.push(self.read_string_fast_owned()?);
        }
        Ok(out)
    }

    fn read_strings_fast_refs(&mut self) -> Result<Vec<&'a str>, LixError> {
        let len = self.read_len_fast()?;
        self.ensure_len_fits_remaining_fast(len)?;
        self.ensure_counted_records_fit_remaining_fast(len, MIN_STRING_BYTES)?;
        let mut out = Vec::with_capacity(len);
        for _ in 0..len {
            out.push(self.read_string_ref_fast()?);
        }
        Ok(out)
    }

    fn skip_strings(&mut self, field: &str) -> Result<(), LixError> {
        let len = self.read_len(field)?;
        self.ensure_len_fits_remaining(len, field)?;
        self.ensure_counted_records_fit_remaining(len, MIN_STRING_BYTES, field)?;
        for index in 0..len {
            self.skip_string(&format!("{field}[{index}]"))?;
        }
        Ok(())
    }

    fn skip_strings_fast(&mut self) -> Result<(), LixError> {
        let len = self.read_len_fast()?;
        self.ensure_len_fits_remaining_fast(len)?;
        self.ensure_counted_records_fit_remaining_fast(len, MIN_STRING_BYTES)?;
        for _ in 0..len {
            self.skip_string_fast()?;
        }
        Ok(())
    }

    fn read_string(&mut self, field: &str) -> Result<String, LixError> {
        let len = self.read_u32(&format!("{field}.len"))?;
        let len = usize::try_from(len).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode changelog {field}: length exceeds usize"),
            )
        })?;
        let bytes = self.read_bytes(len, field)?;
        std::str::from_utf8(bytes)
            .map(str::to_string)
            .map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("failed to decode changelog {field}: invalid UTF-8: {error}"),
                )
            })
    }

    fn read_string_fast_owned(&mut self) -> Result<String, LixError> {
        let len = self.read_len_fast()?;
        let bytes = self.read_bytes_fast(len)?;
        std::str::from_utf8(bytes)
            .map(str::to_string)
            .map_err(|_| self.fast_error("invalid UTF-8 string"))
    }

    fn read_string_ref(&mut self, field: &str) -> Result<&'a str, LixError> {
        let len = self.read_u32(&format!("{field}.len"))?;
        let len = usize::try_from(len).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode changelog {field}: length exceeds usize"),
            )
        })?;
        let bytes = self.read_bytes(len, field)?;
        std::str::from_utf8(bytes).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode changelog {field}: invalid UTF-8: {error}"),
            )
        })
    }

    fn read_string_ref_fast(&mut self) -> Result<&'a str, LixError> {
        let len = self.read_len_fast()?;
        let bytes = self.read_bytes_fast(len)?;
        std::str::from_utf8(bytes).map_err(|_| self.fast_error("invalid UTF-8 string"))
    }

    fn skip_string(&mut self, field: &str) -> Result<(), LixError> {
        let _ = self.read_string_ref(field)?;
        Ok(())
    }

    fn skip_string_fast(&mut self) -> Result<(), LixError> {
        let _ = self.read_string_ref_fast()?;
        Ok(())
    }

    fn read_optional_string(&mut self, field: &str) -> Result<Option<String>, LixError> {
        if self.read_bool(&format!("{field}.present"))? {
            Ok(Some(self.read_string(field)?))
        } else {
            Ok(None)
        }
    }

    fn skip_optional_string(&mut self, field: &str) -> Result<(), LixError> {
        if self.read_bool(&format!("{field}.present"))? {
            self.skip_string(field)
        } else {
            Ok(())
        }
    }

    fn skip_optional_string_fast(&mut self) -> Result<(), LixError> {
        if self.read_bool_fast()? {
            self.skip_string_fast()
        } else {
            Ok(())
        }
    }

    fn read_optional_string_ref_fast(&mut self) -> Result<Option<&'a str>, LixError> {
        if self.read_bool_fast()? {
            Ok(Some(self.read_string_ref_fast()?))
        } else {
            Ok(None)
        }
    }

    fn read_optional_u32(&mut self, field: &str) -> Result<Option<u32>, LixError> {
        if self.read_bool(&format!("{field}.present"))? {
            Ok(Some(self.read_u32(field)?))
        } else {
            Ok(None)
        }
    }

    fn skip_optional_u32(&mut self, field: &str) -> Result<(), LixError> {
        if self.read_bool(&format!("{field}.present"))? {
            self.skip_u32(field)
        } else {
            Ok(())
        }
    }

    fn read_optional_u32_fast(&mut self) -> Result<Option<u32>, LixError> {
        if self.read_bool_fast()? {
            Ok(Some(self.read_u32_fast()?))
        } else {
            Ok(None)
        }
    }

    fn skip_optional_u32_fast(&mut self) -> Result<(), LixError> {
        if self.read_bool_fast()? {
            self.skip_u32_fast()
        } else {
            Ok(())
        }
    }

    fn read_optional_json_ref(&mut self, field: &str) -> Result<Option<JsonRef>, LixError> {
        if self.read_bool(&format!("{field}.present"))? {
            Ok(Some(self.read_json_ref(field)?))
        } else {
            Ok(None)
        }
    }

    fn skip_optional_json_ref(&mut self, field: &str) -> Result<(), LixError> {
        if self.read_bool(&format!("{field}.present"))? {
            self.skip_bytes(32, field)
        } else {
            Ok(())
        }
    }

    fn skip_optional_json_ref_fast(&mut self) -> Result<(), LixError> {
        if self.read_bool_fast()? {
            self.skip_bytes_fast(32)
        } else {
            Ok(())
        }
    }

    fn read_optional_json_ref_fast(&mut self) -> Result<Option<JsonRef>, LixError> {
        if self.read_bool_fast()? {
            Ok(Some(JsonRef::from_hash_bytes(self.read_fixed_hash_fast()?)))
        } else {
            Ok(None)
        }
    }

    fn read_json_ref(&mut self, field: &str) -> Result<JsonRef, LixError> {
        let bytes = self.read_bytes(32, field)?;
        let mut hash = [0_u8; 32];
        hash.copy_from_slice(bytes);
        Ok(JsonRef::from_hash_bytes(hash))
    }

    fn read_fixed_hash_fast(&mut self) -> Result<[u8; 32], LixError> {
        let bytes = self.read_bytes_fast(32)?;
        let mut hash = [0_u8; 32];
        hash.copy_from_slice(bytes);
        Ok(hash)
    }

    fn read_byte_vec(&mut self, field: &str) -> Result<Vec<u8>, LixError> {
        let len = self.read_len(field)?;
        Ok(self.read_bytes(len, field)?.to_vec())
    }

    fn read_byte_vec_ref(&mut self, field: &str) -> Result<&'a [u8], LixError> {
        let len = self.read_len(field)?;
        self.read_bytes(len, field)
    }

    fn read_byte_vec_ref_fast(&mut self) -> Result<&'a [u8], LixError> {
        let len = self.read_len_fast()?;
        self.read_bytes_fast(len)
    }

    fn skip_byte_vec(&mut self, field: &str) -> Result<usize, LixError> {
        let len = self.read_len(field)?;
        self.skip_bytes(len, field)?;
        Ok(len)
    }

    fn skip_byte_vec_fast(&mut self) -> Result<usize, LixError> {
        let len = self.read_len_fast()?;
        self.skip_bytes_fast(len)?;
        Ok(len)
    }

    fn skip_commit_body(
        &mut self,
        field: &str,
        expected_membership_count: usize,
    ) -> Result<(), LixError> {
        let len = self.read_len(&format!("{field}.membership"))?;
        self.ensure_len_fits_remaining(len, &format!("{field}.membership"))?;
        validate_count_matches_usize(
            "membership_count",
            expected_membership_count as u32,
            len,
            "membership records",
        )?;
        for index in 0..len {
            self.skip_string(&format!("{field}.membership[{index}].member_change_id"))?;
            self.read_membership_role(&format!("{field}.membership[{index}].role"))?;
            self.skip_optional_u32(&format!(
                "{field}.membership[{index}].source_parent_ordinal"
            ))?;
        }
        Ok(())
    }

    fn skip_commit_body_fast(&mut self, expected_membership_count: usize) -> Result<(), LixError> {
        let _ = self.read_commit_body_descriptors_fast(expected_membership_count)?;
        Ok(())
    }

    fn skip_segment_commit_directory(&mut self, field: &str) -> Result<(), LixError> {
        let identity_len = self.read_len(&format!("{field}.state_row_identities"))?;
        self.ensure_len_fits_remaining(identity_len, &format!("{field}.state_row_identities"))?;
        self.ensure_counted_records_fit_remaining(
            identity_len,
            MIN_STATE_ROW_IDENTITY_ENTRY_BYTES,
            &format!("{field}.state_row_identities"),
        )?;
        for index in 0..identity_len {
            self.skip_string(&format!(
                "{field}.state_row_identities[{index}].state_row_identity.schema_key"
            ))?;
            self.skip_string(&format!(
                "{field}.state_row_identities[{index}].state_row_identity.file_id"
            ))?;
            self.skip_string(&format!(
                "{field}.state_row_identities[{index}].state_row_identity.entity_id"
            ))?;
            self.skip_string(&format!("{field}.state_row_identities[{index}].change_id"))?;
        }

        let ordinal_len = self.read_len(&format!("{field}.membership_ordinals"))?;
        self.ensure_len_fits_remaining(ordinal_len, &format!("{field}.membership_ordinals"))?;
        self.ensure_counted_records_fit_remaining(
            ordinal_len,
            MIN_MEMBERSHIP_ORDINAL_BYTES,
            &format!("{field}.membership_ordinals"),
        )?;
        for index in 0..ordinal_len {
            self.skip_string(&format!("{field}.membership_ordinals[{index}].change_id"))?;
            self.skip_u32(&format!("{field}.membership_ordinals[{index}].ordinal"))?;
        }
        Ok(())
    }

    fn skip_segment_commit_directory_fast(&mut self) -> Result<(), LixError> {
        let _ = self.read_segment_commit_directory_fast()?;
        Ok(())
    }

    fn read_segment_commit_directory_fast(&mut self) -> Result<SegmentCommitDirectory, LixError> {
        let identity_len = self.read_len_fast()?;
        self.ensure_len_fits_remaining_fast(identity_len)?;
        self.ensure_counted_records_fit_remaining_fast(
            identity_len,
            MIN_STATE_ROW_IDENTITY_ENTRY_BYTES,
        )?;
        let mut state_row_identities = Vec::with_capacity(identity_len);
        for _ in 0..identity_len {
            let identity = self.read_state_row_identity_fast()?;
            let change_id = self.read_string_ref_fast()?.to_string();
            state_row_identities.push((identity, change_id));
        }

        let ordinal_len = self.read_len_fast()?;
        self.ensure_len_fits_remaining_fast(ordinal_len)?;
        self.ensure_counted_records_fit_remaining_fast(ordinal_len, MIN_MEMBERSHIP_ORDINAL_BYTES)?;
        let mut membership_ordinals = Vec::with_capacity(ordinal_len);
        for _ in 0..ordinal_len {
            let change_id = self.read_string_ref_fast()?.to_string();
            let ordinal = self.read_u32_fast()?;
            membership_ordinals.push((change_id, ordinal));
        }
        Ok(SegmentCommitDirectory {
            state_row_identities,
            membership_ordinals,
        })
    }

    fn read_segment_commit_directory_fast_validated(
        &mut self,
        commit_id: &str,
        parent_count: usize,
        memberships: &[CommitMembershipDescriptor<'_>],
    ) -> Result<SegmentCommitDirectory, LixError> {
        validate_commit_membership_descriptors(commit_id, parent_count, memberships)?;
        let directory = self.read_segment_commit_directory_fast()?;
        validate_commit_directory_descriptors(
            commit_id,
            memberships,
            directory
                .state_row_identities
                .iter()
                .map(|(identity, change_id)| (identity, change_id.as_str())),
            directory
                .membership_ordinals
                .iter()
                .map(|(change_id, ordinal)| (change_id.as_str(), *ordinal)),
        )?;
        Ok(directory)
    }

    fn skip_segment_inline_payloads(
        &mut self,
        field: &str,
    ) -> Result<Vec<PayloadDescriptor>, LixError> {
        let len = self.read_len(field)?;
        self.ensure_len_fits_remaining(len, field)?;
        self.ensure_counted_records_fit_remaining(len, MIN_INLINE_PAYLOAD_BYTES, field)?;
        let mut payloads = Vec::with_capacity(len);
        for index in 0..len {
            let json_ref = self.read_json_ref(&format!("{field}[{index}].json_ref"))?;
            let bytes_len = self.skip_byte_vec(&format!("{field}[{index}].bytes"))?;
            payloads.push(PayloadDescriptor {
                json_ref,
                len: bytes_len as u64,
            });
        }
        Ok(payloads)
    }

    fn read_segment_inline_payloads_fast(
        &mut self,
        payload_budget: Option<usize>,
    ) -> Result<Vec<SegmentInlinePayloadRef<'a>>, LixError> {
        let len = self.read_len_fast()?;
        self.ensure_len_fits_remaining_fast(len)?;
        self.ensure_counted_records_fit_remaining_fast(len, MIN_INLINE_PAYLOAD_BYTES)?;
        if payload_budget.is_some_and(|budget| len > budget) {
            return Err(
                self.fast_error("declared inline payload count exceeds header payload_count")
            );
        }
        let mut payloads = Vec::with_capacity(len);
        for _ in 0..len {
            let json_ref = JsonRef::from_hash_bytes(self.read_fixed_hash_fast()?);
            let bytes = self.read_byte_vec_ref_fast()?;
            payloads.push(SegmentInlinePayloadRef { json_ref, bytes });
        }
        Ok(payloads)
    }

    fn skip_segment_change_directory(
        &mut self,
        field: &str,
        change_id: &str,
        inline_payloads: &[PayloadDescriptor],
    ) -> Result<(), LixError> {
        let len = self.read_len(&format!("{field}.payloads"))?;
        self.ensure_len_fits_remaining(len, &format!("{field}.payloads"))?;
        self.ensure_counted_records_fit_remaining(
            len,
            MIN_PAYLOAD_LOCATION_BYTES,
            &format!("{field}.payloads"),
        )?;
        validate_count_matches_usize(
            "payload_count",
            inline_payloads.len() as u32,
            len,
            "change directory payloads",
        )?;
        let mut payloads = Vec::with_capacity(len);
        for index in 0..len {
            payloads.push(self.read_payload_location(&format!("{field}.payloads[{index}]"))?);
        }
        validate_payload_descriptors_against_directory(
            change_id,
            inline_payloads,
            &SegmentChangeDirectory { payloads },
        )
    }

    fn read_segment_change_directory_fast(
        &mut self,
        change_id: &str,
        inline_payloads: &[PayloadDescriptor],
    ) -> Result<SegmentChangeDirectory, LixError> {
        let len = self.read_len_fast()?;
        self.ensure_len_fits_remaining_fast(len)?;
        self.ensure_counted_records_fit_remaining_fast(len, MIN_PAYLOAD_LOCATION_BYTES)?;
        validate_count_matches_usize(
            "payload_count",
            inline_payloads.len() as u32,
            len,
            "change directory payloads",
        )?;
        let mut payloads = Vec::with_capacity(len);
        for _ in 0..len {
            payloads.push(SegmentPayloadLocation {
                json_ref: JsonRef::from_hash_bytes(self.read_fixed_hash_fast()?),
                offset: self.read_u64_fast()?,
                len: self.read_u64_fast()?,
            });
        }
        let directory = SegmentChangeDirectory { payloads };
        validate_payload_descriptors_against_directory(change_id, inline_payloads, &directory)?;
        Ok(directory)
    }

    fn read_len(&mut self, field: &str) -> Result<usize, LixError> {
        let len = self.read_u32(&format!("{field}.len"))?;
        usize::try_from(len).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode changelog {field}: length exceeds usize"),
            )
        })
    }

    fn read_len_fast(&mut self) -> Result<usize, LixError> {
        Ok(self.read_u32_fast()? as usize)
    }

    fn ensure_len_fits_remaining(&self, len: usize, field: &str) -> Result<(), LixError> {
        if len <= self.bytes.len().saturating_sub(self.offset) {
            return Ok(());
        }
        Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to decode changelog {field}: declared length exceeds remaining bytes"),
        ))
    }

    fn ensure_len_fits_remaining_fast(&self, len: usize) -> Result<(), LixError> {
        if len <= self.bytes.len().saturating_sub(self.offset) {
            return Ok(());
        }
        Err(self.fast_error("declared length exceeds remaining bytes"))
    }

    fn ensure_counted_records_fit_remaining(
        &self,
        len: usize,
        min_record_bytes: usize,
        field: &str,
    ) -> Result<(), LixError> {
        let min_bytes = len.checked_mul(min_record_bytes).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode changelog {field}: declared count byte size overflows"),
            )
        })?;
        if min_bytes <= self.bytes.len().saturating_sub(self.offset) {
            return Ok(());
        }
        Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "failed to decode changelog {field}: declared count exceeds remaining bytes for minimum record size"
            ),
        ))
    }

    fn ensure_counted_records_fit_remaining_fast(
        &self,
        len: usize,
        min_record_bytes: usize,
    ) -> Result<(), LixError> {
        let min_bytes = len
            .checked_mul(min_record_bytes)
            .ok_or_else(|| self.fast_error("declared count byte size overflows"))?;
        if min_bytes <= self.bytes.len().saturating_sub(self.offset) {
            return Ok(());
        }
        Err(self.fast_error("declared count exceeds remaining bytes for minimum record size"))
    }

    fn read_bool(&mut self, field: &str) -> Result<bool, LixError> {
        match self.read_u8(field)? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode changelog {field}: invalid bool value {value}"),
            )),
        }
    }

    fn read_bool_fast(&mut self) -> Result<bool, LixError> {
        match self.read_u8_fast()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(self.fast_error("invalid bool value")),
        }
    }

    fn read_u8(&mut self, field: &str) -> Result<u8, LixError> {
        Ok(self.read_bytes(1, field)?[0])
    }

    fn read_u8_fast(&mut self) -> Result<u8, LixError> {
        Ok(self.read_bytes_fast(1)?[0])
    }

    fn skip_u8(&mut self, field: &str) -> Result<(), LixError> {
        self.skip_bytes(1, field)
    }

    fn skip_u8_fast(&mut self) -> Result<(), LixError> {
        self.skip_bytes_fast(1)
    }

    fn read_u32(&mut self, field: &str) -> Result<u32, LixError> {
        let bytes = self.read_bytes(4, field)?;
        let mut out = [0_u8; 4];
        out.copy_from_slice(bytes);
        Ok(u32::from_le_bytes(out))
    }

    fn read_u32_fast(&mut self) -> Result<u32, LixError> {
        let bytes = self.read_bytes_fast(4)?;
        let mut out = [0_u8; 4];
        out.copy_from_slice(bytes);
        Ok(u32::from_le_bytes(out))
    }

    fn skip_u32(&mut self, field: &str) -> Result<(), LixError> {
        self.skip_bytes(4, field)
    }

    fn skip_u32_fast(&mut self) -> Result<(), LixError> {
        self.skip_bytes_fast(4)
    }

    fn read_u64(&mut self, field: &str) -> Result<u64, LixError> {
        let bytes = self.read_bytes(8, field)?;
        let mut out = [0_u8; 8];
        out.copy_from_slice(bytes);
        Ok(u64::from_le_bytes(out))
    }

    fn read_u64_fast(&mut self) -> Result<u64, LixError> {
        let bytes = self.read_bytes_fast(8)?;
        let mut out = [0_u8; 8];
        out.copy_from_slice(bytes);
        Ok(u64::from_le_bytes(out))
    }

    fn skip_u64(&mut self, field: &str) -> Result<(), LixError> {
        self.skip_bytes(8, field)
    }

    fn skip_u64_fast(&mut self) -> Result<(), LixError> {
        self.skip_bytes_fast(8)
    }

    fn read_bytes(&mut self, len: usize, field: &str) -> Result<&'a [u8], LixError> {
        let end = self.offset.checked_add(len).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode changelog {field}: offset overflow"),
            )
        })?;
        if end > self.bytes.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode changelog {field}: truncated bytes"),
            ));
        }
        let out = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(out)
    }

    fn read_bytes_fast(&mut self, len: usize) -> Result<&'a [u8], LixError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| self.fast_error("offset overflow"))?;
        if end > self.bytes.len() {
            return Err(self.fast_error("truncated bytes"));
        }
        let out = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(out)
    }

    fn skip_bytes(&mut self, len: usize, field: &str) -> Result<(), LixError> {
        let _ = self.read_bytes(len, field)?;
        Ok(())
    }

    fn skip_bytes_fast(&mut self, len: usize) -> Result<(), LixError> {
        let _ = self.read_bytes_fast(len)?;
        Ok(())
    }

    fn expect_end(&self, label: &str) -> Result<(), LixError> {
        if self.offset == self.bytes.len() {
            return Ok(());
        }
        Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to decode changelog {label}: trailing bytes"),
        ))
    }

    fn fast_error(&self, message: &'static str) -> LixError {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "failed to decode changelog segment at byte {}: {message}",
                self.offset
            ),
        )
    }
}

struct SegmentHeaderView<'a> {
    segment_id: &'a str,
    format_version: u32,
    commit_count: u32,
    change_count: u32,
    byte_count: u64,
    payload_count: u32,
    checksum: &'a str,
}

struct CommitHeaderView<'a> {
    id: &'a str,
    parent_count: usize,
    membership_count: u32,
}

fn changelog_codec_not_implemented(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INTERNAL_ERROR, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_roundtrips() {
        let segment = sample_segment();

        assert_eq!(
            decode_segment(&encode_segment(&segment).unwrap()).unwrap(),
            segment
        );

        let encoded = encode_segment(&segment).unwrap();
        let view = view_segment(&encoded).unwrap();
        assert_eq!(view.segment_id, "segment-1");
        assert_eq!(view.commit_count, 1);
        assert_eq!(view.change_count, 1);
        assert_eq!(view.directory_commits[0].id, "commit-1");
        assert_eq!(view.directory_changes[0].id, "change-1");
        assert!(!view.object_bytes.is_empty());
    }

    #[test]
    fn commit_visibility_roundtrips() {
        let visibility = CommitVisibility {
            commit_id: "commit-1".to_string(),
            location: location("segment-1", 10, 20, "segment-checksum"),
            checksum: "commit-checksum".to_string(),
        };

        assert_eq!(
            decode_commit_visibility(&encode_commit_visibility(&visibility).unwrap()).unwrap(),
            visibility
        );
    }

    #[test]
    fn by_commit_entry_roundtrips() {
        let entry = ByCommitEntry {
            commit_id: "commit-2".to_string(),
            location: location("segment-1", 30, 40, "commit-checksum"),
            parent_commit_ids: vec!["parent-1".to_string(), "parent-2".to_string()],
            generation: 42,
        };

        assert_eq!(
            decode_by_commit_entry(&encode_by_commit_entry(&entry).unwrap()).unwrap(),
            entry
        );
    }

    #[test]
    fn by_change_entry_roundtrips() {
        let entry = ByChangeEntry {
            change_id: "change-1".to_string(),
            location: location("segment-2", 50, 60, "change-checksum"),
        };

        assert_eq!(
            decode_by_change_entry(&encode_by_change_entry(&entry).unwrap()).unwrap(),
            entry
        );
    }

    #[test]
    fn empty_index_value_accepts_only_zero_bytes() {
        decode_empty_index_value(&encode_empty_index_value()).unwrap();
        assert!(decode_empty_index_value(&[1]).is_err());
    }

    #[test]
    fn tiny_codecs_reject_wrong_magic() {
        assert!(decode_segment(b"wrong").is_err());
        assert!(decode_by_change_entry(b"wrong").is_err());
    }

    #[test]
    fn segment_decode_rejects_declared_vector_length_larger_than_remaining_bytes() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(SEGMENT_MAGIC);
        write_segment_header(
            &mut bytes,
            &SegmentHeader {
                segment_id: "segment-1".to_string(),
                format_version: 1,
                commit_count: 0,
                change_count: 0,
                byte_count: 0,
                payload_count: 0,
                checksum: "checksum".to_string(),
            },
        )
        .unwrap();
        write_segment_directory(&mut bytes, &SegmentDirectory::default()).unwrap();
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());

        let error = decode_segment(&bytes).expect_err("huge vector length must be rejected");

        assert!(
            error
                .message
                .contains("declared length exceeds remaining bytes"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_decode_rejects_header_count_mismatch() {
        let mut segment = sample_segment();
        segment.header.commit_count = 2;
        let bytes = encode_segment(&segment).unwrap();

        let error = decode_segment(&bytes).expect_err("header count mismatch should reject");

        assert!(
            error
                .message
                .contains("header commit_count 2 does not match 1 directory commits"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_decode_rejects_object_count_mismatch_before_allocating_objects() {
        let bytes = segment_bytes_with_empty_object_lists(sample_segment());

        let error = decode_segment(&bytes).expect_err("object count mismatch should reject");

        assert!(
            error
                .message
                .contains("header commit_count 1 does not match 0 commits"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_decode_rejects_impossible_directory_count_before_allocating() {
        let bytes = segment_bytes_with_directory_lengths(sample_segment(), 1, 1_000);

        let error = decode_segment(&bytes).expect_err("impossible directory count should reject");

        assert!(
            error
                .message
                .contains("declared count exceeds remaining bytes for minimum record size"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_view_rejects_header_count_mismatch() {
        let mut segment = sample_segment();
        segment.header.change_count = 2;
        let bytes = encode_segment(&segment).unwrap();

        let error = view_segment(&bytes).expect_err("header count mismatch should reject");

        assert!(
            error
                .message
                .contains("header change_count 2 does not match 1 directory changes"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_decode_rejects_unsupported_format_version() {
        let mut segment = sample_segment();
        segment.header.format_version = 2;
        let bytes = encode_segment(&segment).unwrap();

        let error = decode_segment(&bytes).expect_err("unsupported format version should reject");

        assert!(
            error.message.contains("format_version 2 is not supported"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_view_rejects_unsupported_format_version() {
        let mut segment = sample_segment();
        segment.header.format_version = 2;
        let bytes = encode_segment(&segment).unwrap();

        let error = view_segment(&bytes).expect_err("unsupported format version should reject");

        assert!(
            error.message.contains("format_version 2 is not supported"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_decode_rejects_header_byte_count_mismatch() {
        let mut segment = sample_segment();
        segment.header.byte_count += 1;
        let bytes = encode_segment(&segment).unwrap();

        let error = decode_segment(&bytes).expect_err("byte count mismatch should reject");

        assert!(
            error.message.contains("byte_count"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_view_rejects_header_checksum_mismatch() {
        let mut segment = sample_segment();
        segment.header.checksum = empty_checksum();
        let bytes = encode_segment(&segment).unwrap();

        let error = view_segment(&bytes).expect_err("header checksum mismatch should reject");

        assert!(
            error.message.contains("checksum"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_object_slices_rejects_header_checksum_mismatch() {
        let mut segment = sample_segment();
        segment.header.checksum = empty_checksum();
        let bytes = encode_segment(&segment).unwrap();

        let error =
            view_segment_object_slices(&bytes).expect_err("header checksum mismatch should reject");

        assert!(
            error.message.contains("checksum"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_object_slices_rejects_header_directory_count_mismatch() {
        let mut segment = sample_segment();
        segment.header.change_count = 2;
        let bytes = encode_segment(&segment).unwrap();

        let error =
            view_segment_object_slices(&bytes).expect_err("directory count mismatch should reject");

        assert!(
            error
                .message
                .contains("header change_count 2 does not match 1 directory changes"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_view_rejects_directory_locator_offset_mismatch() {
        let mut segment = sample_segment();
        segment.directory.commits[0].1.offset += 1;
        let bytes = encode_segment(&segment).unwrap();

        let error = view_segment(&bytes).expect_err("directory locator mismatch should reject");

        assert!(
            error
                .message
                .contains("locator offset/len does not match encoded byte range"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_decode_rejects_directory_locator_offset_mismatch() {
        let mut segment = sample_segment();
        segment.directory.commits[0].1.offset += 1;
        let bytes = encode_segment(&segment).unwrap();

        let error = decode_segment(&bytes).expect_err("directory locator mismatch should reject");

        assert!(
            error
                .message
                .contains("locator offset/len does not match encoded byte range"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_view_rejects_directory_order_mismatch() {
        let mut segment = sample_segment_with_two_changes();
        apply_sample_encoded_locations(&mut segment);
        segment.directory.changes.swap(0, 1);
        let bytes = encode_segment(&segment).unwrap();

        let error = view_segment(&bytes).expect_err("directory order mismatch should reject");

        assert!(
            error
                .message
                .contains("directory order does not match encoded object order"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_directory_view_rejects_duplicate_change_ids_with_different_locations() {
        let mut segment = sample_segment_with_two_changes();
        segment.directory.changes[1].0 = segment.directory.changes[0].0.clone();
        let bytes = encode_segment(&segment).unwrap();

        let error =
            view_segment_directory(&bytes).expect_err("duplicate directory id should reject");

        assert!(
            error.message.contains("duplicate directory locator"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_decode_rejects_directory_order_mismatch() {
        let mut segment = sample_segment_with_two_changes();
        apply_sample_encoded_locations(&mut segment);
        segment.directory.changes.swap(0, 1);
        let bytes = encode_segment(&segment).unwrap();

        let error = decode_segment(&bytes).expect_err("directory order mismatch should reject");

        assert!(
            error
                .message
                .contains("directory order does not match encoded object order"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_decode_rejects_change_locator_checksum_mismatch() {
        let mut segment = sample_segment();
        segment.directory.changes[0].1.checksum = empty_checksum();
        let bytes = encode_segment(&segment).unwrap();

        let error =
            decode_segment(&bytes).expect_err("change locator checksum mismatch should reject");

        assert!(
            error.message.contains("locator checksum"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_decode_rejects_inline_payload_ref_mismatch() {
        let mut segment = sample_segment();
        let bogus_ref = JsonRef::from_hash_bytes([9; 32]);
        segment.changes[0].inline_payloads[0].json_ref = bogus_ref;
        segment.changes[0].directory.payloads[0].json_ref = bogus_ref;
        apply_sample_encoded_locations(&mut segment);
        let bytes = encode_segment(&segment).unwrap();

        let error = decode_segment(&bytes).expect_err("inline payload hash mismatch should reject");

        assert!(
            error.message.contains("does not match payload bytes"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_view_rejects_inline_payload_ref_mismatch() {
        let mut segment = sample_segment();
        let bogus_ref = JsonRef::from_hash_bytes([9; 32]);
        segment.changes[0].inline_payloads[0].json_ref = bogus_ref;
        segment.changes[0].directory.payloads[0].json_ref = bogus_ref;
        apply_sample_encoded_locations(&mut segment);
        let bytes = encode_segment(&segment).unwrap();

        let error = view_segment(&bytes).expect_err("inline payload hash mismatch should reject");

        assert!(
            error.message.contains("does not match payload bytes"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_view_rejects_change_locator_checksum_mismatch() {
        let mut segment = sample_segment();
        segment.directory.changes[0].1.checksum = empty_checksum();
        let bytes = encode_segment(&segment).unwrap();

        let error =
            view_segment(&bytes).expect_err("change locator checksum mismatch should reject");

        assert!(
            error.message.contains("locator checksum"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_view_rejects_object_count_mismatch() {
        let bytes = segment_bytes_with_empty_object_lists(sample_segment());

        let error = view_segment(&bytes).expect_err("object count mismatch should reject");

        assert!(
            error
                .message
                .contains("header commit_count 1 does not match 0 commits"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_object_slices_rejects_object_count_mismatch() {
        let bytes = segment_bytes_with_empty_object_lists(sample_segment());

        let error =
            view_segment_object_slices(&bytes).expect_err("object count mismatch should reject");

        assert!(
            error
                .message
                .contains("header commit_count 1 does not match 0 commits"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_decode_rejects_payload_count_mismatch() {
        let mut segment = sample_segment();
        segment.header.payload_count = 2;
        let bytes = encode_segment(&segment).unwrap();

        let error = decode_segment(&bytes).expect_err("payload count mismatch should reject");

        assert!(
            error
                .message
                .contains("header payload_count 2 does not match 1 inline payloads"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_view_rejects_payload_count_mismatch() {
        let mut segment = sample_segment();
        segment.header.payload_count = 2;
        let bytes = encode_segment(&segment).unwrap();

        let error = view_segment(&bytes).expect_err("payload count mismatch should reject");

        assert!(
            error
                .message
                .contains("header payload_count 2 does not match 1 inline payloads"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_object_slices_rejects_payload_count_mismatch() {
        let mut segment = sample_segment();
        segment.header.payload_count = 2;
        let bytes = encode_segment(&segment).unwrap();

        let error =
            view_segment_object_slices(&bytes).expect_err("payload count mismatch should reject");

        assert!(
            error
                .message
                .contains("header payload_count 2 does not match 1 inline payloads"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_decode_rejects_inline_payloads_exceeding_header_payload_count_before_allocating() {
        let mut segment = sample_segment();
        segment.header.payload_count = 0;
        let bytes = encode_segment(&segment).unwrap();

        let error = decode_segment(&bytes).expect_err("payload budget overrun should reject");

        assert!(
            error
                .message
                .contains("declared inline payload count exceeds header payload_count"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_view_rejects_inline_payloads_exceeding_header_payload_count_before_scanning() {
        let mut segment = sample_segment();
        segment.header.payload_count = 0;
        let bytes = encode_segment(&segment).unwrap();

        let error = view_segment(&bytes).expect_err("payload budget overrun should reject");

        assert!(
            error
                .message
                .contains("declared inline payload count exceeds header payload_count"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_object_slices_rejects_inline_payloads_exceeding_header_payload_count_before_scanning(
    ) {
        let mut segment = sample_segment();
        segment.header.payload_count = 0;
        let bytes = encode_segment(&segment).unwrap();

        let error =
            view_segment_object_slices(&bytes).expect_err("payload budget overrun should reject");

        assert!(
            error
                .message
                .contains("declared inline payload count exceeds header payload_count"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_decode_rejects_impossible_inline_payload_count_before_allocating() {
        let bytes = segment_change_bytes_with_inline_payload_len(sample_segment(), 1_000);

        let error =
            decode_segment(&bytes).expect_err("impossible inline payload count should reject");

        assert!(
            error
                .message
                .contains("declared count exceeds remaining bytes for minimum record size"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_view_rejects_impossible_inline_payload_count_before_allocating() {
        let bytes = segment_change_bytes_with_inline_payload_len(sample_segment(), 1_000);

        let error =
            view_segment(&bytes).expect_err("impossible inline payload count should reject");

        assert!(
            error
                .message
                .contains("declared count exceeds remaining bytes for minimum record size"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_decode_rejects_impossible_string_list_count_before_allocating() {
        let bytes = commit_bytes_with_parent_count(sample_segment(), 1_000);

        let error = decode_segment(&bytes).expect_err("impossible string list count should reject");

        assert!(
            error
                .message
                .contains("declared count exceeds remaining bytes for minimum record size"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_view_rejects_impossible_string_list_count_before_allocating() {
        let bytes = commit_bytes_with_parent_count(sample_segment(), 1_000);

        let error = view_segment(&bytes).expect_err("impossible string list count should reject");

        assert!(
            error
                .message
                .contains("declared count exceeds remaining bytes for minimum record size"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_decode_rejects_impossible_top_level_commit_count_before_allocating() {
        let bytes = segment_bytes_with_object_lengths(sample_segment(), 1_000, 0);

        let error = decode_segment(&bytes).expect_err("impossible commit count should reject");

        assert!(
            error
                .message
                .contains("declared count exceeds remaining bytes for minimum record size"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_view_rejects_impossible_top_level_commit_count_before_allocating() {
        let bytes = segment_bytes_with_object_lengths(sample_segment(), 1_000, 0);

        let error = view_segment(&bytes).expect_err("impossible commit count should reject");

        assert!(
            error
                .message
                .contains("declared count exceeds remaining bytes for minimum record size"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_decode_rejects_change_directory_payload_count_mismatch() {
        let mut segment = sample_segment();
        segment.changes[0]
            .directory
            .payloads
            .push(SegmentPayloadLocation {
                json_ref: JsonRef::from_hash_bytes([9; 32]),
                offset: 1,
                len: 0,
            });
        let bytes = encode_segment(&segment).unwrap();

        let error =
            decode_segment(&bytes).expect_err("change directory count mismatch should reject");

        assert!(
            error
                .message
                .contains("header payload_count 1 does not match 2 change directory payloads"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_change_decode_rejects_change_directory_payload_count_mismatch() {
        let mut segment = sample_segment();
        segment.changes[0]
            .directory
            .payloads
            .push(SegmentPayloadLocation {
                json_ref: JsonRef::from_hash_bytes([9; 32]),
                offset: 1,
                len: 0,
            });
        let mut bytes = Vec::new();
        write_segment_change(&mut bytes, &segment.changes[0]).unwrap();

        let error = decode_segment_change(&bytes)
            .expect_err("change directory count mismatch should reject");

        assert!(
            error
                .message
                .contains("header payload_count 1 does not match 2 change directory payloads"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_decode_rejects_change_directory_payload_len_mismatch() {
        let mut segment = sample_segment();
        segment.changes[0].directory.payloads[0].len = 999;
        let bytes = encode_segment(&segment).unwrap();

        let error = decode_segment(&bytes)
            .expect_err("change directory payload len mismatch should reject");

        assert!(
            error
                .message
                .contains("payload directory entry does not match inline payload"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_view_rejects_change_directory_payload_len_mismatch() {
        let mut segment = sample_segment();
        segment.changes[0].directory.payloads[0].len = 999;
        let bytes = encode_segment(&segment).unwrap();

        let error =
            view_segment(&bytes).expect_err("change directory payload len mismatch should reject");

        assert!(
            error
                .message
                .contains("payload directory entry does not match inline payload"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_object_slices_rejects_change_directory_payload_len_mismatch() {
        let mut segment = sample_segment();
        segment.changes[0].directory.payloads[0].len = 999;
        let bytes = encode_segment(&segment).unwrap();

        let error = view_segment_object_slices(&bytes)
            .expect_err("change directory payload len mismatch should reject");

        assert!(
            error
                .message
                .contains("payload directory entry does not match inline payload"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_change_decode_rejects_change_directory_payload_len_mismatch() {
        let mut segment = sample_segment();
        segment.changes[0].directory.payloads[0].len = 999;
        let mut bytes = Vec::new();
        write_segment_change(&mut bytes, &segment.changes[0]).unwrap();

        let error = decode_segment_change(&bytes)
            .expect_err("change directory payload len mismatch should reject");

        assert!(
            error
                .message
                .contains("payload directory entry does not match inline payload"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_decode_rejects_change_directory_payload_offset_mismatch() {
        let mut segment = sample_segment();
        segment.changes[0].directory.payloads[0].offset = 1;
        let bytes = encode_segment(&segment).unwrap();

        let error = decode_segment(&bytes)
            .expect_err("change directory payload offset mismatch should reject");

        assert!(
            error
                .message
                .contains("payload directory entry does not match inline payload"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_decode_rejects_commit_membership_count_mismatch() {
        let mut segment = sample_segment();
        segment.commits[0].header.membership_count = 2;
        let bytes = encode_segment(&segment).unwrap();

        let error = decode_segment(&bytes).expect_err("membership count mismatch should reject");

        assert!(
            error
                .message
                .contains("header membership_count 2 does not match 1 membership records"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_view_rejects_commit_membership_count_mismatch() {
        let mut segment = sample_segment();
        segment.commits[0].header.membership_count = 2;
        let bytes = encode_segment(&segment).unwrap();

        let error = view_segment(&bytes).expect_err("membership count mismatch should reject");

        assert!(
            error
                .message
                .contains("header membership_count 2 does not match 1 membership records"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_object_slices_rejects_commit_membership_count_mismatch() {
        let mut segment = sample_segment();
        segment.commits[0].header.membership_count = 2;
        let bytes = encode_segment(&segment).unwrap();

        let error = view_segment_object_slices(&bytes)
            .expect_err("membership count mismatch should reject");

        assert!(
            error
                .message
                .contains("header membership_count 2 does not match 1 membership records"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn commit_membership_scan_rejects_membership_count_mismatch() {
        let mut segment = sample_segment();
        segment.commits[0].header.membership_count = 2;
        let mut bytes = Vec::new();
        write_segment_commit(&mut bytes, &segment.commits[0]).unwrap();

        let error = segment_commit_membership_contains_any(
            &bytes,
            "commit-1",
            &segment.commits[0].checksum,
            &std::collections::HashSet::new(),
        )
        .expect_err("membership count mismatch should reject");

        assert!(
            error
                .message
                .contains("header membership_count 2 does not match 1 membership records"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_view_rejects_invalid_membership_role() {
        let bytes = segment_bytes_with_membership_role(sample_segment(), 9);

        let error = view_segment(&bytes).expect_err("invalid role should reject");

        assert!(
            error.message.contains("invalid membership role"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_view_rejects_invalid_utf8_in_skipped_commit_string() {
        let mut segment = sample_segment();
        segment.commits[0].header.created_at = "commit-created".to_string();
        apply_sample_encoded_locations(&mut segment);
        let bytes = segment_bytes_with_invalid_string_content(segment, "commit-created");

        let error = view_segment(&bytes).expect_err("invalid UTF-8 should reject");

        assert!(
            error.message.contains("invalid UTF-8"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_view_rejects_invalid_utf8_in_skipped_change_string() {
        let mut segment = sample_segment();
        segment.changes[0].created_at = "change-created".to_string();
        apply_sample_encoded_locations(&mut segment);
        let bytes = segment_bytes_with_invalid_string_content(segment, "change-created");

        let error = view_segment(&bytes).expect_err("invalid UTF-8 should reject");

        assert!(
            error.message.contains("invalid UTF-8"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_view_rejects_empty_change_entity_identity_part() {
        let mut segment = sample_segment();
        segment.changes[0].entity_id = EntityIdentity {
            parts: vec![String::new()],
        };
        let bytes = encode_segment(&segment).unwrap();

        let error = view_segment(&bytes).expect_err("empty entity identity should reject");

        assert!(
            error.message.contains("invalid entity identity parts"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_object_slices_rejects_empty_state_row_identity() {
        let bytes = segment_bytes_with_empty_state_row_entity_id(sample_segment());

        let error =
            view_segment_object_slices(&bytes).expect_err("empty state-row identity should reject");

        assert!(
            error.message.contains("entity_id"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_decode_rejects_state_row_identity_change_mismatch() {
        let mut segment = sample_segment();
        segment.commits[0].directory.state_row_identities[0]
            .0
            .schema_key = CanonicalSchemaKey::new("other").unwrap();
        apply_sample_encoded_locations(&mut segment);
        let bytes = encode_segment(&segment).unwrap();

        let error = decode_segment(&bytes).expect_err("state-row identity mismatch should reject");

        assert!(
            error
                .message
                .contains("StateRowIdentity winner for change 'change-1' does not match"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_view_rejects_state_row_identity_change_mismatch() {
        let mut segment = sample_segment();
        segment.commits[0].directory.state_row_identities[0]
            .0
            .schema_key = CanonicalSchemaKey::new("other").unwrap();
        apply_sample_encoded_locations(&mut segment);
        let bytes = encode_segment(&segment).unwrap();

        let error = view_segment(&bytes).expect_err("state-row identity mismatch should reject");

        assert!(
            error
                .message
                .contains("StateRowIdentity winner for change 'change-1' does not match"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_decode_rejects_authored_state_row_identity_missing_change() {
        let mut segment = sample_segment();
        segment.commits[0].body.membership[0].member_change_id = "missing-change".to_string();
        segment.commits[0].directory.state_row_identities[0].1 = "missing-change".to_string();
        segment.commits[0].directory.membership_ordinals[0].0 = "missing-change".to_string();
        apply_sample_encoded_locations(&mut segment);
        let bytes = encode_segment(&segment).unwrap();

        let error =
            decode_segment(&bytes).expect_err("missing authored change object should reject");

        assert!(
            error
                .message
                .contains("StateRowIdentity winner references missing authored change"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_view_rejects_authored_state_row_identity_missing_change() {
        let mut segment = sample_segment();
        segment.commits[0].body.membership[0].member_change_id = "missing-change".to_string();
        segment.commits[0].directory.state_row_identities[0].1 = "missing-change".to_string();
        segment.commits[0].directory.membership_ordinals[0].0 = "missing-change".to_string();
        apply_sample_encoded_locations(&mut segment);
        let bytes = encode_segment(&segment).unwrap();

        let error = view_segment(&bytes).expect_err("missing authored change object should reject");

        assert!(
            error
                .message
                .contains("StateRowIdentity winner references missing authored change"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_decode_rejects_authored_membership_commit_mismatch() {
        let mut segment = sample_segment();
        segment.changes[0].authored_commit_id = Some("other-commit".to_string());
        apply_sample_encoded_locations(&mut segment);
        let bytes = encode_segment(&segment).unwrap();

        let error =
            decode_segment(&bytes).expect_err("authored commit ownership mismatch should reject");

        assert!(
            error.message.contains("mismatched authored_commit_id"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_view_rejects_authored_membership_commit_mismatch() {
        let mut segment = sample_segment();
        segment.changes[0].authored_commit_id = Some("other-commit".to_string());
        apply_sample_encoded_locations(&mut segment);
        let bytes = encode_segment(&segment).unwrap();

        let error =
            view_segment(&bytes).expect_err("authored commit ownership mismatch should reject");

        assert!(
            error.message.contains("mismatched authored_commit_id"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_decode_rejects_adopted_membership_authored_by_same_commit() {
        let mut segment = sample_segment();
        segment.commits[0].body.membership[0].role = MembershipRole::Adopted;
        segment.commits[0].body.membership[0].source_parent_ordinal = Some(0);
        apply_sample_encoded_locations(&mut segment);
        let bytes = encode_segment(&segment).unwrap();

        let error =
            decode_segment(&bytes).expect_err("self-authored adopted membership should reject");

        assert!(
            error
                .message
                .contains("must not be authored by the same commit"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_view_rejects_adopted_membership_authored_by_same_commit() {
        let mut segment = sample_segment();
        segment.commits[0].body.membership[0].role = MembershipRole::Adopted;
        segment.commits[0].body.membership[0].source_parent_ordinal = Some(0);
        apply_sample_encoded_locations(&mut segment);
        let bytes = encode_segment(&segment).unwrap();

        let error =
            view_segment(&bytes).expect_err("self-authored adopted membership should reject");

        assert!(
            error
                .message
                .contains("must not be authored by the same commit"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn commit_checksum_distinguishes_list_boundaries() {
        let segment = sample_segment();
        let mut left = segment.commits[0].clone();
        left.header.parent_commit_ids = vec!["a".to_string(), "b".to_string()];
        left.header.derivable_change_id = "c".to_string();
        left.header.author_account_ids = vec!["d".to_string()];

        let mut right = segment.commits[0].clone();
        right.header.parent_commit_ids = vec!["a".to_string()];
        right.header.derivable_change_id = "b".to_string();
        right.header.author_account_ids = vec!["c".to_string(), "d".to_string()];

        assert_ne!(
            checksum_commit(&left).unwrap(),
            checksum_commit(&right).unwrap()
        );
    }

    #[test]
    fn change_checksum_distinguishes_entity_schema_boundaries() {
        let segment = sample_segment();
        let mut left = segment.changes[0].clone();
        left.entity_id = EntityIdentity {
            parts: vec!["a".to_string(), "b".to_string()],
        };
        left.schema_key = "c".to_string();
        left.file_id = Some("d".to_string());

        let mut right = segment.changes[0].clone();
        right.entity_id = EntityIdentity {
            parts: vec!["a".to_string()],
        };
        right.schema_key = "b".to_string();
        right.file_id = Some("c".to_string());

        assert_ne!(
            checksum_change(&left).unwrap(),
            checksum_change(&right).unwrap()
        );
    }

    #[test]
    fn segment_decode_rejects_invalid_membership_role() {
        let bytes = segment_bytes_with_membership_role(sample_segment(), 9);

        let error = decode_segment(&bytes).expect_err("invalid role should reject");

        assert!(
            error.message.contains("invalid membership role"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_commit_decode_rejects_invalid_membership_role() {
        let segment = sample_segment();
        let mut bytes = Vec::new();
        write_segment_commit(&mut bytes, &segment.commits[0]).unwrap();
        let role_offset = find_membership_role_offset(&bytes, "change-1");
        bytes[role_offset] = 9;

        let error = decode_segment_commit(&bytes).expect_err("invalid role should reject");

        assert!(
            error.message.contains("invalid membership role"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn commit_membership_scan_rejects_invalid_membership_role() {
        let mut segment = sample_segment();
        segment.commits[0].body.membership[0].role = MembershipRole::Authored;
        let mut bytes = Vec::new();
        write_segment_commit(&mut bytes, &segment.commits[0]).unwrap();
        let role_offset = find_membership_role_offset(&bytes, "change-1");
        bytes[role_offset] = 9;

        let error = segment_commit_membership_contains_any(
            &bytes,
            "commit-1",
            &segment.commits[0].checksum,
            &std::collections::HashSet::new(),
        )
        .expect_err("invalid role should reject");

        assert!(
            error.message.contains("invalid membership role"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn commit_membership_scan_rejects_wrong_expected_commit_id() {
        let segment = sample_segment();
        let mut bytes = Vec::new();
        write_segment_commit(&mut bytes, &segment.commits[0]).unwrap();

        let error = segment_commit_membership_contains_any(
            &bytes,
            "other-commit",
            &segment.commits[0].checksum,
            &std::collections::HashSet::new(),
        )
        .expect_err("wrong expected commit id should reject");

        assert!(
            error.message.contains("decoded commit 'commit-1'"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn commit_membership_scan_rejects_membership_ordinal_mismatch() {
        let mut segment = sample_segment();
        segment.commits[0].directory.membership_ordinals[0].1 = 7;
        let mut bytes = Vec::new();
        write_segment_commit(&mut bytes, &segment.commits[0]).unwrap();

        let error = segment_commit_membership_contains_any(
            &bytes,
            "commit-1",
            &segment.commits[0].checksum,
            &std::collections::HashSet::new(),
        )
        .expect_err("membership ordinal mismatch should reject");

        assert!(
            error.message.contains("membership ordinal"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_commit_decode_rejects_membership_ordinal_mismatch() {
        let mut segment = sample_segment();
        segment.commits[0].directory.membership_ordinals[0].1 = 7;
        let mut bytes = Vec::new();
        write_segment_commit(&mut bytes, &segment.commits[0]).unwrap();

        let error =
            decode_segment_commit(&bytes).expect_err("membership ordinal mismatch should reject");

        assert!(
            error.message.contains("membership ordinal"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn segment_commit_decode_rejects_checksum_mismatch() {
        let mut segment = sample_segment();
        segment.commits[0].checksum = empty_checksum();
        let mut bytes = Vec::new();
        write_segment_commit(&mut bytes, &segment.commits[0]).unwrap();

        let error =
            decode_segment_commit(&bytes).expect_err("commit checksum mismatch should reject");

        assert!(
            error.message.contains("checksum"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn commit_membership_scan_rejects_locator_checksum_mismatch() {
        let segment = sample_segment();
        let mut bytes = Vec::new();
        write_segment_commit(&mut bytes, &segment.commits[0]).unwrap();

        let error = segment_commit_membership_contains_any(
            &bytes,
            "commit-1",
            "wrong-checksum",
            &std::collections::HashSet::new(),
        )
        .expect_err("locator checksum mismatch should reject");

        assert!(
            error.message.contains("locator checksum"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn commit_membership_scan_rejects_checksum_mismatch() {
        let mut segment = sample_segment();
        segment.commits[0].checksum = empty_checksum();
        let mut bytes = Vec::new();
        write_segment_commit(&mut bytes, &segment.commits[0]).unwrap();

        let error = segment_commit_membership_contains_any(
            &bytes,
            "commit-1",
            &segment.commits[0].checksum,
            &std::collections::HashSet::new(),
        )
        .expect_err("commit checksum mismatch should reject");

        assert!(
            error.message.contains("checksum"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn commit_membership_scan_rejects_truncated_commit_after_body() {
        let segment = sample_segment();
        let mut bytes = Vec::new();
        write_commit_header(&mut bytes, &segment.commits[0].header).unwrap();
        write_commit_body(&mut bytes, &segment.commits[0].body).unwrap();

        let error = segment_commit_membership_contains_any(
            &bytes,
            "commit-1",
            &segment.commits[0].checksum,
            &std::collections::HashSet::new(),
        )
        .expect_err("truncated commit should reject");

        assert!(
            error.message.contains("truncated bytes"),
            "unexpected error: {error}"
        );
    }

    fn segment_bytes_with_empty_object_lists(segment: Segment) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(SEGMENT_MAGIC);
        write_segment_header(&mut bytes, &segment.header).unwrap();
        write_segment_directory(&mut bytes, &segment.directory).unwrap();
        write_len(&mut bytes, 0, "commits").unwrap();
        write_len(&mut bytes, 0, "changes").unwrap();
        bytes
    }

    fn segment_bytes_with_directory_lengths(
        segment: Segment,
        commit_len: usize,
        change_len: usize,
    ) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(SEGMENT_MAGIC);
        write_segment_header(&mut bytes, &segment.header).unwrap();
        write_len(&mut bytes, commit_len, "directory commits").unwrap();
        write_len(&mut bytes, change_len, "directory changes").unwrap();
        write_len(&mut bytes, 0, "commits").unwrap();
        write_len(&mut bytes, 0, "changes").unwrap();
        bytes
    }

    fn segment_bytes_with_object_lengths(
        segment: Segment,
        commit_len: usize,
        change_len: usize,
    ) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(SEGMENT_MAGIC);
        write_segment_header(&mut bytes, &segment.header).unwrap();
        write_segment_directory(&mut bytes, &segment.directory).unwrap();
        write_len(&mut bytes, commit_len, "commits").unwrap();
        write_len(&mut bytes, change_len, "changes").unwrap();
        bytes.resize(bytes.len() + commit_len + change_len, 0);
        bytes
    }

    fn segment_change_bytes_with_inline_payload_len(
        segment: Segment,
        payload_len: usize,
    ) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(SEGMENT_MAGIC);
        write_segment_header(&mut bytes, &segment.header).unwrap();
        write_segment_directory(&mut bytes, &segment.directory).unwrap();
        write_len(&mut bytes, segment.commits.len(), "commits").unwrap();
        for commit in &segment.commits {
            write_segment_commit(&mut bytes, commit).unwrap();
        }
        write_len(&mut bytes, segment.changes.len(), "changes").unwrap();
        let change = &segment.changes[0];
        write_str(&mut bytes, &change.id).unwrap();
        write_optional_str(&mut bytes, change.authored_commit_id.as_deref()).unwrap();
        write_entity_identity(&mut bytes, &change.entity_id).unwrap();
        write_str(&mut bytes, &change.schema_key).unwrap();
        write_optional_str(&mut bytes, change.file_id.as_deref()).unwrap();
        write_optional_json_ref(&mut bytes, change.snapshot_ref.as_ref());
        write_optional_json_ref(&mut bytes, change.metadata_ref.as_ref());
        write_str(&mut bytes, &change.created_at).unwrap();
        write_len(&mut bytes, payload_len, "inline payloads").unwrap();
        bytes.resize(bytes.len() + payload_len, 0);
        bytes
    }

    fn commit_bytes_with_parent_count(segment: Segment, parent_count: usize) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(SEGMENT_MAGIC);
        write_segment_header(&mut bytes, &segment.header).unwrap();
        write_segment_directory(&mut bytes, &segment.directory).unwrap();
        write_len(&mut bytes, segment.commits.len(), "commits").unwrap();
        let commit = &segment.commits[0];
        write_str(&mut bytes, &commit.header.id).unwrap();
        write_len(&mut bytes, parent_count, "parent commits").unwrap();
        bytes.resize(bytes.len() + parent_count, 0);
        bytes
    }

    fn segment_bytes_with_membership_role(segment: Segment, role: u8) -> Vec<u8> {
        let mut bytes = encode_segment(&segment).unwrap();
        let role_offset = find_membership_role_offset(&bytes, "change-1");
        bytes[role_offset] = role;
        bytes
    }

    fn segment_bytes_with_invalid_string_content(segment: Segment, value: &str) -> Vec<u8> {
        let mut bytes = encode_segment(&segment).unwrap();
        let mut pattern = Vec::new();
        append_test_string(&mut pattern, value);
        let offset = bytes
            .windows(pattern.len())
            .position(|window| window == pattern.as_slice())
            .expect("test string should exist");
        bytes[offset + 4] = 0xff;
        bytes
    }

    fn segment_bytes_with_empty_state_row_entity_id(segment: Segment) -> Vec<u8> {
        let mut bytes = encode_segment(&segment).unwrap();
        let entity_len_offset = find_state_row_entity_id_len_offset(&bytes);
        let entity = EntityIdentity::single("entity-1")
            .as_json_array_text()
            .unwrap();
        let entity_len = entity.len();
        bytes[entity_len_offset..entity_len_offset + 4].copy_from_slice(&0_u32.to_le_bytes());
        bytes.drain(entity_len_offset + 4..entity_len_offset + 4 + entity_len);
        bytes
    }

    fn find_state_row_entity_id_len_offset(bytes: &[u8]) -> usize {
        let mut pattern = Vec::new();
        append_test_string(&mut pattern, "message");
        append_test_string(&mut pattern, "file-1");
        let entity_len_offset = pattern.len();
        append_test_string(
            &mut pattern,
            &EntityIdentity::single("entity-1")
                .as_json_array_text()
                .unwrap(),
        );
        append_test_string(&mut pattern, "change-1");
        bytes
            .windows(pattern.len())
            .position(|window| window == pattern.as_slice())
            .map(|offset| offset + entity_len_offset)
            .expect("state-row identity should exist")
    }

    fn append_test_string(bytes: &mut Vec<u8>, value: &str) {
        bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
        bytes.extend_from_slice(value.as_bytes());
    }

    fn find_membership_role_offset(bytes: &[u8], member_change_id: &str) -> usize {
        let needle_len = (member_change_id.len() as u32).to_le_bytes();
        let needle = member_change_id.as_bytes();
        bytes
            .windows(4 + needle.len())
            .enumerate()
            .find_map(|(offset, window)| {
                if &window[..4] != needle_len.as_slice() || &window[4..] != needle {
                    return None;
                }
                let role_offset = offset + 4 + needle.len();
                let expected_after_role = [0, 0];
                (bytes.get(role_offset..role_offset + expected_after_role.len())
                    == Some(expected_after_role.as_slice()))
                .then_some(role_offset)
            })
            .expect("membership record should exist")
    }

    fn sample_segment() -> Segment {
        let entity_identity = EntityIdentity::single("entity-1");
        let state_row_identity = StateRowIdentity {
            schema_key: CanonicalSchemaKey::new("message").unwrap(),
            file_id: FileId::new("file-1").unwrap(),
            entity_id: EntityId::new(entity_identity.as_json_array_text().unwrap()).unwrap(),
        };
        let snapshot_ref = JsonRef::from_hash_bytes([1; 32]);
        let metadata_ref = JsonRef::from_hash_bytes([2; 32]);
        let payload_bytes = br#"{"hello":"world"}"#.to_vec();
        let payload_ref = JsonRef::for_content(&payload_bytes);

        let mut segment = Segment {
            header: SegmentHeader {
                segment_id: "segment-1".to_string(),
                format_version: 1,
                commit_count: 1,
                change_count: 1,
                byte_count: 123,
                payload_count: 1,
                checksum: "segment-checksum".to_string(),
            },
            directory: SegmentDirectory {
                commits: vec![(
                    "commit-1".to_string(),
                    location("segment-1", 10, 20, "commit-checksum"),
                )],
                changes: vec![(
                    "change-1".to_string(),
                    location("segment-1", 30, 40, "change-checksum"),
                )],
            },
            commits: vec![SegmentCommit {
                header: CommitHeader {
                    id: "commit-1".to_string(),
                    parent_commit_ids: vec!["parent-1".to_string()],
                    derivable_change_id: "commit-change-1".to_string(),
                    author_account_ids: vec!["account-1".to_string()],
                    created_at: "2026-05-12T00:00:00Z".to_string(),
                    membership_count: 1,
                },
                body: CommitBody {
                    membership: vec![MembershipRecord {
                        member_change_id: "change-1".to_string(),
                        role: MembershipRole::Authored,
                        source_parent_ordinal: None,
                    }],
                },
                directory: SegmentCommitDirectory {
                    state_row_identities: vec![(state_row_identity, "change-1".to_string())],
                    membership_ordinals: vec![("change-1".to_string(), 0)],
                },
                checksum: "commit-checksum".to_string(),
            }],
            changes: vec![SegmentChange {
                id: "change-1".to_string(),
                authored_commit_id: Some("commit-1".to_string()),
                entity_id: entity_identity,
                schema_key: "message".to_string(),
                file_id: Some("file-1".to_string()),
                snapshot_ref: Some(snapshot_ref),
                metadata_ref: Some(metadata_ref),
                created_at: "2026-05-12T00:00:00Z".to_string(),
                inline_payloads: vec![SegmentInlinePayload {
                    json_ref: payload_ref,
                    bytes: payload_bytes,
                }],
                directory: SegmentChangeDirectory {
                    payloads: vec![SegmentPayloadLocation {
                        json_ref: payload_ref,
                        offset: 0,
                        len: 17,
                    }],
                },
            }],
        };
        apply_sample_encoded_locations(&mut segment);
        segment
    }

    fn sample_segment_with_two_changes() -> Segment {
        let mut segment = sample_segment();
        let entity_identity = EntityIdentity::single("entity-2");
        let state_row_identity = StateRowIdentity {
            schema_key: CanonicalSchemaKey::new("message").unwrap(),
            file_id: FileId::new("file-1").unwrap(),
            entity_id: EntityId::new(entity_identity.as_json_array_text().unwrap()).unwrap(),
        };
        let payload_bytes = br#"{"goodbye":"world"}"#.to_vec();
        let payload_ref = JsonRef::for_content(&payload_bytes);
        segment.commits[0].header.membership_count = 2;
        segment.commits[0].body.membership.push(MembershipRecord {
            member_change_id: "change-2".to_string(),
            role: MembershipRole::Authored,
            source_parent_ordinal: None,
        });
        segment.commits[0]
            .directory
            .state_row_identities
            .push((state_row_identity, "change-2".to_string()));
        segment.commits[0]
            .directory
            .membership_ordinals
            .push(("change-2".to_string(), 1));
        segment.changes.push(SegmentChange {
            id: "change-2".to_string(),
            authored_commit_id: Some("commit-1".to_string()),
            entity_id: entity_identity,
            schema_key: "message".to_string(),
            file_id: Some("file-1".to_string()),
            snapshot_ref: None,
            metadata_ref: None,
            created_at: "2026-05-12T00:00:01Z".to_string(),
            inline_payloads: vec![SegmentInlinePayload {
                json_ref: payload_ref,
                bytes: payload_bytes,
            }],
            directory: SegmentChangeDirectory {
                payloads: vec![SegmentPayloadLocation {
                    json_ref: payload_ref,
                    offset: 0,
                    len: 19,
                }],
            },
        });
        segment.header.change_count = 2;
        segment.header.payload_count = 2;
        segment.directory.changes.push((
            "change-2".to_string(),
            location("segment-1", 0, 0, "change-checksum-2"),
        ));
        apply_sample_encoded_locations(&mut segment);
        segment
    }

    fn apply_sample_encoded_locations(segment: &mut Segment) {
        for commit in &mut segment.commits {
            commit.checksum = checksum_commit(commit).unwrap();
        }
        let change_checksums = segment
            .changes
            .iter()
            .map(|change| (change.id.clone(), checksum_change(change).unwrap()))
            .collect::<HashMap<_, _>>();
        for (change_id, location) in &mut segment.directory.changes {
            location.checksum = change_checksums
                .get(change_id)
                .expect("change checksum should exist")
                .clone();
        }
        for (commit_id, location) in &mut segment.directory.commits {
            location.checksum = segment
                .commits
                .iter()
                .find(|commit| commit.header.id == *commit_id)
                .expect("commit checksum should exist")
                .checksum
                .clone();
        }
        segment.header.byte_count = 0;
        segment.header.checksum = empty_checksum();
        let encoded = encode_segment_with_object_locations(segment).unwrap();
        for (id, location) in &mut segment.directory.commits {
            let object = encoded
                .commits
                .iter()
                .find(|object| object.id == *id)
                .expect("commit location should exist");
            location.offset = object.offset;
            location.len = object.len;
        }
        for (id, location) in &mut segment.directory.changes {
            let object = encoded
                .changes
                .iter()
                .find(|object| object.id == *id)
                .expect("change location should exist");
            location.offset = object.offset;
            location.len = object.len;
        }
        segment.header.byte_count = encode_segment(segment).unwrap().len() as u64;
        segment.header.checksum = checksum_segment(segment).unwrap();
    }

    fn location(segment_id: &str, offset: u64, len: u64, checksum: &str) -> SegmentObjectLocation {
        SegmentObjectLocation {
            segment_id: segment_id.to_string(),
            offset,
            len,
            checksum: checksum.to_string(),
        }
    }
}
