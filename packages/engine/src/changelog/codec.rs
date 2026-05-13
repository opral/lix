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

const SEGMENT_MAGIC: &[u8; 5] = b"LXSG1";
const COMMIT_VISIBILITY_MAGIC: &[u8; 5] = b"LXCV1";
const BY_COMMIT_MAGIC: &[u8; 5] = b"LXBC1";
const BY_CHANGE_MAGIC: &[u8; 5] = b"LXBG1";

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
    let directory = cursor.read_segment_directory("directory")?;
    let commit_len = cursor.read_len("commits")?;
    cursor.ensure_len_fits_remaining(commit_len, "commits")?;
    let mut commits = Vec::with_capacity(commit_len);
    for index in 0..commit_len {
        commits.push(cursor.read_segment_commit(&format!("commits[{index}]"))?);
    }
    let change_len = cursor.read_len("changes")?;
    cursor.ensure_len_fits_remaining(change_len, "changes")?;
    let mut changes = Vec::with_capacity(change_len);
    for index in 0..change_len {
        changes.push(cursor.read_segment_change(&format!("changes[{index}]"))?);
    }
    cursor.expect_end("segment")?;
    Ok(Segment {
        header,
        directory,
        commits,
        changes,
    })
}

pub(crate) fn decode_segment_commit(bytes: &[u8]) -> Result<SegmentCommit, LixError> {
    let mut cursor = ByteCursor::new(bytes);
    let commit = cursor.read_segment_commit("commit")?;
    cursor.expect_end("commit")?;
    Ok(commit)
}

pub(crate) fn decode_segment_change(bytes: &[u8]) -> Result<SegmentChange, LixError> {
    let mut cursor = ByteCursor::new(bytes);
    let change = cursor.read_segment_change("change")?;
    cursor.expect_end("change")?;
    Ok(change)
}

pub(crate) fn view_segment(bytes: &[u8]) -> Result<SegmentView<'_>, LixError> {
    let mut cursor = ByteCursor::new(bytes);
    cursor.expect_magic(SEGMENT_MAGIC, "segment")?;
    let header = cursor.read_segment_header_view("header")?;
    let directory_commits = cursor.read_segment_directory_commit_views("directory.commits")?;
    let directory_changes = cursor.read_segment_directory_change_views("directory.changes")?;
    let object_bytes = cursor.remaining_bytes();

    Ok(SegmentView {
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
    })
}

pub(crate) fn view_segment_object_slices(
    bytes: &[u8],
) -> Result<(Vec<SegmentObjectSlice<'_>>, Vec<SegmentObjectSlice<'_>>), LixError> {
    let mut cursor = ByteCursor::new(bytes);
    cursor.expect_magic(SEGMENT_MAGIC, "segment")?;
    let _ = cursor.read_segment_header_view("header")?;
    let _ = cursor.read_segment_directory_commit_views("directory.commits")?;
    let _ = cursor.read_segment_directory_change_views("directory.changes")?;

    let commit_len = cursor.read_len("commits")?;
    cursor.ensure_len_fits_remaining(commit_len, "commits")?;
    let mut commits = Vec::with_capacity(commit_len);
    for index in 0..commit_len {
        commits.push(cursor.read_segment_commit_slice(&format!("commits[{index}]"))?);
    }

    let change_len = cursor.read_len("changes")?;
    cursor.ensure_len_fits_remaining(change_len, "changes")?;
    let mut changes = Vec::with_capacity(change_len);
    for index in 0..change_len {
        changes.push(cursor.read_segment_change_slice(&format!("changes[{index}]"))?);
    }

    cursor.expect_end("segment")?;
    Ok((commits, changes))
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
    write_str(bytes, &identity.as_json_array_text()?)
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

    fn read_segment_header(&mut self, field: &str) -> Result<SegmentHeader, LixError> {
        Ok(SegmentHeader {
            segment_id: self.read_string(&format!("{field}.segment_id"))?,
            format_version: self.read_u32(&format!("{field}.format_version"))?,
            commit_count: self.read_u32(&format!("{field}.commit_count"))?,
            change_count: self.read_u32(&format!("{field}.change_count"))?,
            byte_count: self.read_u64(&format!("{field}.byte_count"))?,
            payload_count: self.read_u32(&format!("{field}.payload_count"))?,
            checksum: self.read_string(&format!("{field}.checksum"))?,
        })
    }

    fn read_segment_header_view(&mut self, field: &str) -> Result<SegmentHeaderView<'a>, LixError> {
        Ok(SegmentHeaderView {
            segment_id: self.read_string_ref(&format!("{field}.segment_id"))?,
            format_version: self.read_u32(&format!("{field}.format_version"))?,
            commit_count: self.read_u32(&format!("{field}.commit_count"))?,
            change_count: self.read_u32(&format!("{field}.change_count"))?,
            byte_count: self.read_u64(&format!("{field}.byte_count"))?,
            payload_count: self.read_u32(&format!("{field}.payload_count"))?,
            checksum: self.read_string_ref(&format!("{field}.checksum"))?,
        })
    }

    fn read_segment_directory(&mut self, field: &str) -> Result<SegmentDirectory, LixError> {
        let commit_len = self.read_len(&format!("{field}.commits"))?;
        self.ensure_len_fits_remaining(commit_len, &format!("{field}.commits"))?;
        let mut commits = Vec::with_capacity(commit_len);
        for index in 0..commit_len {
            let commit_id = self.read_string(&format!("{field}.commits[{index}].commit_id"))?;
            let location = self.read_location(&format!("{field}.commits[{index}].location"))?;
            commits.push((commit_id, location));
        }

        let change_len = self.read_len(&format!("{field}.changes"))?;
        self.ensure_len_fits_remaining(change_len, &format!("{field}.changes"))?;
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
        let mut commits = Vec::with_capacity(len);
        for index in 0..len {
            let id = self.read_string_ref(&format!("{field}[{index}].commit_id"))?;
            let location = self.read_location_view(&format!("{field}[{index}].location"))?;
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
        let mut changes = Vec::with_capacity(len);
        for index in 0..len {
            let id = self.read_string_ref(&format!("{field}[{index}].change_id"))?;
            let location = self.read_location_view(&format!("{field}[{index}].location"))?;
            changes.push(SegmentDirectoryEntryRef { id, location });
        }
        Ok(changes)
    }

    fn read_segment_commit(&mut self, field: &str) -> Result<SegmentCommit, LixError> {
        Ok(SegmentCommit {
            header: self.read_commit_header(&format!("{field}.header"))?,
            body: self.read_commit_body(&format!("{field}.body"))?,
            directory: self.read_segment_commit_directory(&format!("{field}.directory"))?,
            checksum: self.read_string(&format!("{field}.checksum"))?,
        })
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
        self.skip_u32(&format!("{field}.header.membership_count"))?;
        self.skip_commit_body(&format!("{field}.body"))?;
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

    fn read_commit_body(&mut self, field: &str) -> Result<CommitBody, LixError> {
        let len = self.read_len(&format!("{field}.membership"))?;
        self.ensure_len_fits_remaining(len, &format!("{field}.membership"))?;
        let mut membership = Vec::with_capacity(len);
        for index in 0..len {
            membership.push(self.read_membership_record(&format!("{field}.membership[{index}]"))?);
        }
        Ok(CommitBody { membership })
    }

    fn read_membership_record(&mut self, field: &str) -> Result<MembershipRecord, LixError> {
        Ok(MembershipRecord {
            member_change_id: self.read_string(&format!("{field}.member_change_id"))?,
            role: self.read_membership_role(&format!("{field}.role"))?,
            source_parent_ordinal: self
                .read_optional_u32(&format!("{field}.source_parent_ordinal"))?,
        })
    }

    fn read_segment_commit_directory(
        &mut self,
        field: &str,
    ) -> Result<SegmentCommitDirectory, LixError> {
        let identity_len = self.read_len(&format!("{field}.state_row_identities"))?;
        self.ensure_len_fits_remaining(identity_len, &format!("{field}.state_row_identities"))?;
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

    fn read_segment_change(&mut self, field: &str) -> Result<SegmentChange, LixError> {
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
        let mut inline_payloads = Vec::with_capacity(payload_len);
        for index in 0..payload_len {
            inline_payloads.push(
                self.read_segment_inline_payload(&format!("{field}.inline_payloads[{index}]"))?,
            );
        }

        let directory = self.read_segment_change_directory(&format!("{field}.directory"))?;

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
        self.skip_string(&format!("{field}.entity_id"))?;
        self.skip_string(&format!("{field}.schema_key"))?;
        self.skip_optional_string(&format!("{field}.file_id"))?;
        self.skip_optional_json_ref(&format!("{field}.snapshot_ref"))?;
        self.skip_optional_json_ref(&format!("{field}.metadata_ref"))?;
        self.skip_string(&format!("{field}.created_at"))?;
        self.skip_segment_inline_payloads(&format!("{field}.inline_payloads"))?;
        self.skip_segment_change_directory(&format!("{field}.directory"))?;
        let end = self.offset;
        Ok(SegmentObjectSlice {
            id,
            offset: start as u64,
            len: (end - start) as u64,
            encoded_checksum: None,
            bytes: &self.bytes[start..end],
        })
    }

    fn remaining_bytes(&self) -> &'a [u8] {
        &self.bytes[self.offset..]
    }

    fn read_segment_inline_payload(
        &mut self,
        field: &str,
    ) -> Result<SegmentInlinePayload, LixError> {
        Ok(SegmentInlinePayload {
            json_ref: self.read_json_ref(&format!("{field}.json_ref"))?,
            bytes: self.read_byte_vec(&format!("{field}.bytes"))?,
        })
    }

    fn read_segment_change_directory(
        &mut self,
        field: &str,
    ) -> Result<SegmentChangeDirectory, LixError> {
        let len = self.read_len(&format!("{field}.payloads"))?;
        self.ensure_len_fits_remaining(len, &format!("{field}.payloads"))?;
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

    fn read_entity_identity(&mut self, field: &str) -> Result<EntityIdentity, LixError> {
        let value = self.read_string(field)?;
        EntityIdentity::from_json_array_text(&value).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode changelog {field}: invalid entity identity: {error}"),
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

    fn read_strings(&mut self, field: &str) -> Result<Vec<String>, LixError> {
        let len = self.read_len(field)?;
        self.ensure_len_fits_remaining(len, field)?;
        let mut out = Vec::with_capacity(len);
        for index in 0..len {
            out.push(self.read_string(&format!("{field}[{index}]"))?);
        }
        Ok(out)
    }

    fn skip_strings(&mut self, field: &str) -> Result<(), LixError> {
        let len = self.read_len(field)?;
        self.ensure_len_fits_remaining(len, field)?;
        for index in 0..len {
            self.skip_string(&format!("{field}[{index}]"))?;
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

    fn skip_string(&mut self, field: &str) -> Result<(), LixError> {
        let len = self.read_u32(&format!("{field}.len"))?;
        let len = usize::try_from(len).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to decode changelog {field}: length exceeds usize"),
            )
        })?;
        self.skip_bytes(len, field)
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

    fn read_json_ref(&mut self, field: &str) -> Result<JsonRef, LixError> {
        let bytes = self.read_bytes(32, field)?;
        let mut hash = [0_u8; 32];
        hash.copy_from_slice(bytes);
        Ok(JsonRef::from_hash_bytes(hash))
    }

    fn read_byte_vec(&mut self, field: &str) -> Result<Vec<u8>, LixError> {
        let len = self.read_len(field)?;
        Ok(self.read_bytes(len, field)?.to_vec())
    }

    fn skip_byte_vec(&mut self, field: &str) -> Result<(), LixError> {
        let len = self.read_len(field)?;
        self.skip_bytes(len, field)
    }

    fn skip_commit_body(&mut self, field: &str) -> Result<(), LixError> {
        let len = self.read_len(&format!("{field}.membership"))?;
        self.ensure_len_fits_remaining(len, &format!("{field}.membership"))?;
        for index in 0..len {
            self.skip_string(&format!("{field}.membership[{index}].member_change_id"))?;
            self.skip_u8(&format!("{field}.membership[{index}].role"))?;
            self.skip_optional_u32(&format!(
                "{field}.membership[{index}].source_parent_ordinal"
            ))?;
        }
        Ok(())
    }

    fn skip_segment_commit_directory(&mut self, field: &str) -> Result<(), LixError> {
        let identity_len = self.read_len(&format!("{field}.state_row_identities"))?;
        self.ensure_len_fits_remaining(identity_len, &format!("{field}.state_row_identities"))?;
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
        for index in 0..ordinal_len {
            self.skip_string(&format!("{field}.membership_ordinals[{index}].change_id"))?;
            self.skip_u32(&format!("{field}.membership_ordinals[{index}].ordinal"))?;
        }
        Ok(())
    }

    fn skip_segment_inline_payloads(&mut self, field: &str) -> Result<(), LixError> {
        let len = self.read_len(field)?;
        self.ensure_len_fits_remaining(len, field)?;
        for index in 0..len {
            self.skip_bytes(32, &format!("{field}[{index}].json_ref"))?;
            self.skip_byte_vec(&format!("{field}[{index}].bytes"))?;
        }
        Ok(())
    }

    fn skip_segment_change_directory(&mut self, field: &str) -> Result<(), LixError> {
        let len = self.read_len(&format!("{field}.payloads"))?;
        self.ensure_len_fits_remaining(len, &format!("{field}.payloads"))?;
        for index in 0..len {
            self.skip_bytes(32, &format!("{field}.payloads[{index}].json_ref"))?;
            self.skip_u64(&format!("{field}.payloads[{index}].offset"))?;
            self.skip_u64(&format!("{field}.payloads[{index}].len"))?;
        }
        Ok(())
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

    fn ensure_len_fits_remaining(&self, len: usize, field: &str) -> Result<(), LixError> {
        if len <= self.bytes.len().saturating_sub(self.offset) {
            return Ok(());
        }
        Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to decode changelog {field}: declared length exceeds remaining bytes"),
        ))
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

    fn read_u8(&mut self, field: &str) -> Result<u8, LixError> {
        Ok(self.read_bytes(1, field)?[0])
    }

    fn skip_u8(&mut self, field: &str) -> Result<(), LixError> {
        self.skip_bytes(1, field)
    }

    fn read_u32(&mut self, field: &str) -> Result<u32, LixError> {
        let bytes = self.read_bytes(4, field)?;
        let mut out = [0_u8; 4];
        out.copy_from_slice(bytes);
        Ok(u32::from_le_bytes(out))
    }

    fn skip_u32(&mut self, field: &str) -> Result<(), LixError> {
        self.skip_bytes(4, field)
    }

    fn read_u64(&mut self, field: &str) -> Result<u64, LixError> {
        let bytes = self.read_bytes(8, field)?;
        let mut out = [0_u8; 8];
        out.copy_from_slice(bytes);
        Ok(u64::from_le_bytes(out))
    }

    fn skip_u64(&mut self, field: &str) -> Result<(), LixError> {
        self.skip_bytes(8, field)
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

    fn skip_bytes(&mut self, len: usize, field: &str) -> Result<(), LixError> {
        let _ = self.read_bytes(len, field)?;
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

fn changelog_codec_not_implemented(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INTERNAL_ERROR, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_roundtrips() {
        let state_row_identity = StateRowIdentity {
            schema_key: CanonicalSchemaKey::new("message").unwrap(),
            file_id: FileId::new("file-1").unwrap(),
            entity_id: EntityId::new("entity-1").unwrap(),
        };
        let snapshot_ref = JsonRef::from_hash_bytes([1; 32]);
        let metadata_ref = JsonRef::from_hash_bytes([2; 32]);
        let payload_ref = JsonRef::from_hash_bytes([3; 32]);

        let segment = Segment {
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
                commits: vec![("commit-1".to_string(), location("segment-1", 10, 20, "c"))],
                changes: vec![("change-1".to_string(), location("segment-1", 30, 40, "ch"))],
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
                        source_parent_ordinal: Some(0),
                    }],
                },
                directory: SegmentCommitDirectory {
                    state_row_identities: vec![(
                        state_row_identity.clone(),
                        "change-1".to_string(),
                    )],
                    membership_ordinals: vec![("change-1".to_string(), 0)],
                },
                checksum: "commit-checksum".to_string(),
            }],
            changes: vec![SegmentChange {
                id: "change-1".to_string(),
                authored_commit_id: Some("commit-1".to_string()),
                entity_id: EntityIdentity::single("entity-1"),
                schema_key: "message".to_string(),
                file_id: Some("file-1".to_string()),
                snapshot_ref: Some(snapshot_ref),
                metadata_ref: Some(metadata_ref),
                created_at: "2026-05-12T00:00:00Z".to_string(),
                inline_payloads: vec![SegmentInlinePayload {
                    json_ref: payload_ref,
                    bytes: br#"{"hello":"world"}"#.to_vec(),
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
                commit_count: u32::MAX,
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

    fn location(segment_id: &str, offset: u64, len: u64, checksum: &str) -> SegmentObjectLocation {
        SegmentObjectLocation {
            segment_id: segment_id.to_string(),
            offset,
            len,
            checksum: checksum.to_string(),
        }
    }
}
