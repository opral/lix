use crate::common::{CanonicalSchemaKey, EntityId, FileId};
use crate::entity_identity::EntityIdentity;
use crate::json_store::JsonRef;

pub(crate) type CommitId = String;
pub(crate) type ChangeId = String;
pub(crate) type SegmentId = String;
pub(crate) type SegmentOffset = u64;
pub(crate) type SegmentLength = u64;
pub(crate) type SegmentChecksum = String;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Commit {
    pub(crate) header: CommitHeader,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitHeader {
    pub(crate) id: CommitId,
    pub(crate) parent_commit_ids: Vec<CommitId>,
    pub(crate) derivable_change_id: ChangeId,
    pub(crate) author_account_ids: Vec<String>,
    pub(crate) created_at: String,
    pub(crate) membership_count: u32,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct CommitBody {
    pub(crate) membership: Vec<MembershipRecord>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommitProjection {
    Header,
    Body,
    Full,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommitVisibilityMode {
    RequireVisible,
    PhysicalOnly,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CommitLoadRequest<'a> {
    pub(crate) commit_ids: &'a [CommitId],
    pub(crate) projection: CommitProjection,
    pub(crate) visibility: CommitVisibilityMode,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitLoadBatch {
    pub(crate) entries: Vec<Option<CommitLoadEntry>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CommitLoadEntry {
    Header(CommitHeader),
    Body(CommitBody),
    Full {
        header: CommitHeader,
        body: CommitBody,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MembershipRecord {
    pub(crate) member_change_id: ChangeId,
    pub(crate) role: MembershipRole,
    pub(crate) source_parent_ordinal: Option<u32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MembershipRole {
    Authored,
    Adopted,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct StateRowIdentity {
    pub(crate) schema_key: CanonicalSchemaKey,
    pub(crate) file_id: FileId,
    pub(crate) entity_id: EntityId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Change {
    pub(crate) id: ChangeId,
    pub(crate) authored_commit_id: Option<CommitId>,
    pub(crate) entity_id: EntityIdentity,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_ref: Option<JsonRef>,
    pub(crate) metadata_ref: Option<JsonRef>,
    pub(crate) created_at: String,
}

impl Change {
    pub(crate) fn as_ref(&self) -> ChangeRef<'_> {
        ChangeRef {
            id: &self.id,
            authored_commit_id: self.authored_commit_id.as_ref(),
            entity_id: &self.entity_id,
            schema_key: &self.schema_key,
            file_id: self.file_id.as_deref(),
            snapshot_ref: self.snapshot_ref.as_ref(),
            metadata_ref: self.metadata_ref.as_ref(),
            created_at: &self.created_at,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ChangeRef<'a> {
    pub(crate) id: &'a str,
    pub(crate) authored_commit_id: Option<&'a CommitId>,
    pub(crate) entity_id: &'a EntityIdentity,
    pub(crate) schema_key: &'a str,
    pub(crate) file_id: Option<&'a str>,
    pub(crate) snapshot_ref: Option<&'a JsonRef>,
    pub(crate) metadata_ref: Option<&'a JsonRef>,
    pub(crate) created_at: &'a str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ChangeLocator {
    pub(crate) change_id: ChangeId,
    pub(crate) commit_id: CommitId,
    pub(crate) location: SegmentObjectLocation,
}

impl ChangeLocator {
    pub(crate) fn as_ref(&self) -> ChangeLocatorRef<'_> {
        ChangeLocatorRef {
            change_id: &self.change_id,
            commit_id: &self.commit_id,
            location: self.location.as_ref(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ChangeLocatorRef<'a> {
    pub(crate) change_id: &'a str,
    pub(crate) commit_id: &'a str,
    pub(crate) location: SegmentObjectLocationRef<'a>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ChangeProjection {
    Logical,
    Segment,
    PhysicalLocation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ChangeVisibilityMode {
    RequireReachableFromVisibleCommit,
    PhysicalOnly,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ChangeLoadRequest<'a> {
    pub(crate) change_ids: &'a [ChangeId],
    pub(crate) projection: ChangeProjection,
    pub(crate) visibility: ChangeVisibilityMode,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ChangeLoadBatch {
    pub(crate) entries: Vec<Option<ChangeLoadEntry>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ChangeLoadEntry {
    Logical(Change),
    Segment(SegmentChange),
    PhysicalLocation(SegmentObjectLocation),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Segment {
    pub(crate) header: SegmentHeader,
    pub(crate) directory: SegmentDirectory,
    pub(crate) commits: Vec<SegmentCommit>,
    pub(crate) changes: Vec<SegmentChange>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SegmentStageReport {
    pub(crate) segment_id: SegmentId,
    pub(crate) commit_locations: Vec<(CommitId, SegmentObjectLocation)>,
    pub(crate) change_locations: Vec<(ChangeId, SegmentObjectLocation)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SegmentHeader {
    pub(crate) segment_id: SegmentId,
    pub(crate) format_version: u32,
    pub(crate) commit_count: u32,
    pub(crate) change_count: u32,
    pub(crate) byte_count: u64,
    pub(crate) payload_count: u32,
    pub(crate) checksum: SegmentChecksum,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct SegmentDirectory {
    pub(crate) commits: Vec<(CommitId, SegmentObjectLocation)>,
    pub(crate) changes: Vec<(ChangeId, SegmentObjectLocation)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SegmentObjectLocation {
    pub(crate) segment_id: SegmentId,
    pub(crate) offset: SegmentOffset,
    pub(crate) len: SegmentLength,
    pub(crate) checksum: SegmentChecksum,
}

impl SegmentObjectLocation {
    pub(crate) fn as_ref(&self) -> SegmentObjectLocationRef<'_> {
        SegmentObjectLocationRef {
            segment_id: &self.segment_id,
            offset: self.offset,
            len: self.len,
            checksum: &self.checksum,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SegmentObjectLocationRef<'a> {
    pub(crate) segment_id: &'a str,
    pub(crate) offset: SegmentOffset,
    pub(crate) len: SegmentLength,
    pub(crate) checksum: &'a str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SegmentDirectoryEntryRef<'a> {
    pub(crate) id: &'a str,
    pub(crate) location: SegmentObjectLocationRef<'a>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SegmentObjectSlice<'a> {
    pub(crate) id: &'a str,
    pub(crate) offset: SegmentOffset,
    pub(crate) len: SegmentLength,
    pub(crate) encoded_checksum: Option<&'a str>,
    pub(crate) bytes: &'a [u8],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SegmentView<'a> {
    pub(crate) bytes: &'a [u8],
    pub(crate) segment_id: &'a str,
    pub(crate) format_version: u32,
    pub(crate) commit_count: u32,
    pub(crate) change_count: u32,
    pub(crate) byte_count: u64,
    pub(crate) payload_count: u32,
    pub(crate) checksum: &'a str,
    pub(crate) directory_commits: Vec<SegmentDirectoryEntryRef<'a>>,
    pub(crate) directory_changes: Vec<SegmentDirectoryEntryRef<'a>>,
    pub(crate) object_bytes: &'a [u8],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SegmentCommit {
    pub(crate) header: CommitHeader,
    pub(crate) body: CommitBody,
    pub(crate) directory: SegmentCommitDirectory,
    pub(crate) checksum: SegmentChecksum,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct SegmentCommitDirectory {
    pub(crate) state_row_identities: Vec<(StateRowIdentity, ChangeId)>,
    pub(crate) membership_ordinals: Vec<(ChangeId, u32)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SegmentChange {
    pub(crate) id: ChangeId,
    pub(crate) authored_commit_id: Option<CommitId>,
    pub(crate) entity_id: EntityIdentity,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_ref: Option<JsonRef>,
    pub(crate) metadata_ref: Option<JsonRef>,
    pub(crate) created_at: String,
    pub(crate) inline_payloads: Vec<SegmentInlinePayload>,
    pub(crate) directory: SegmentChangeDirectory,
}

impl SegmentChange {
    pub(crate) fn as_change_ref(&self) -> ChangeRef<'_> {
        ChangeRef {
            id: &self.id,
            authored_commit_id: self.authored_commit_id.as_ref(),
            entity_id: &self.entity_id,
            schema_key: &self.schema_key,
            file_id: self.file_id.as_deref(),
            snapshot_ref: self.snapshot_ref.as_ref(),
            metadata_ref: self.metadata_ref.as_ref(),
            created_at: &self.created_at,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SegmentInlinePayload {
    pub(crate) json_ref: JsonRef,
    pub(crate) bytes: Vec<u8>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct SegmentChangeDirectory {
    pub(crate) payloads: Vec<SegmentPayloadLocation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SegmentPayloadLocation {
    pub(crate) json_ref: JsonRef,
    pub(crate) offset: u64,
    pub(crate) len: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitVisibility {
    pub(crate) commit_id: CommitId,
    pub(crate) location: SegmentObjectLocation,
    pub(crate) checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ByCommitEntry {
    pub(crate) commit_id: CommitId,
    pub(crate) location: SegmentObjectLocation,
    pub(crate) parent_commit_ids: Vec<CommitId>,
    pub(crate) generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ByChangeEntry {
    pub(crate) change_id: ChangeId,
    pub(crate) location: SegmentObjectLocation,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RebuildIndexStats {
    pub(crate) expected: usize,
    pub(crate) put: usize,
    pub(crate) deleted: usize,
    pub(crate) unchanged: usize,
}

impl RebuildIndexStats {
    pub(crate) fn combine(self, other: Self) -> Self {
        Self {
            expected: self.expected + other.expected,
            put: self.put + other.put,
            deleted: self.deleted + other.deleted,
            unchanged: self.unchanged + other.unchanged,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum GcRoot {
    VersionHead(CommitId),
    PinnedCommit(CommitId),
    RemoteRef(CommitId),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct GcLiveSet {
    pub(crate) commits: Vec<CommitId>,
    pub(crate) changes: Vec<ChangeId>,
    pub(crate) payloads: Vec<JsonRef>,
    pub(crate) segments: Vec<SegmentId>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct GcSweepSet {
    pub(crate) segments: Vec<SegmentId>,
    pub(crate) commit_visibility: Vec<CommitId>,
    pub(crate) by_commit: Vec<CommitId>,
    pub(crate) by_change: Vec<ChangeId>,
    pub(crate) by_change_membership: Vec<(ChangeId, CommitId)>,
    pub(crate) visible_change_proof: Vec<ChangeId>,
    pub(crate) json_payloads: Vec<JsonRef>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct GcRepairSet {
    pub(crate) by_commit: Vec<ByCommitEntry>,
    pub(crate) by_change: Vec<ByChangeEntry>,
    pub(crate) by_change_membership: Vec<(ChangeId, CommitId)>,
    pub(crate) visible_change_proof: Vec<(ChangeId, CommitVisibility)>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct GcPlan {
    pub(crate) roots: Vec<GcRoot>,
    pub(crate) live: GcLiveSet,
    pub(crate) sweep: GcSweepSet,
    pub(crate) repair: GcRepairSet,
}
