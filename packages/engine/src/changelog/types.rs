use crate::common::LixTimestamp;
use crate::entity_pk::EntityPk;
use crate::json_store::JsonRef;

mod entity_pk_ref_storage {
    use super::EntityPkRef;

    pub(crate) fn decode<'de, D>(decoder: D) -> Result<EntityPkRef<'de>, D::Error>
    where
        D: musli::Decoder<'de>,
        Vec<&'de str>: musli::Decode<'de, D::Mode, D::Allocator>,
    {
        let parts = musli::Decode::decode(decoder)?;
        Ok(EntityPkRef { parts })
    }
}

pub(crate) type CommitId = String;
pub(crate) type ChangeId = String;

pub(crate) type CommitIdRef<'a> = &'a str;
pub(crate) type ChangeIdRef<'a> = &'a str;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ChangelogAppend {
    pub(crate) commits: Vec<CommitRecord>,
    pub(crate) changes: Vec<ChangeRecord>,
    pub(crate) commit_change_refs: Vec<CommitChangeRefSet>,
}

#[derive(Clone, Debug, Eq, PartialEq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct CommitRecord {
    pub(crate) format_version: u32,
    pub(crate) commit_id: CommitId,
    pub(crate) parent_commit_ids: Vec<CommitId>,
    pub(crate) change_id: ChangeId,
    pub(crate) author_account_ids: Vec<String>,
    pub(crate) created_at: LixTimestamp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitChangeRefSet {
    pub(crate) commit_id: CommitId,
    pub(crate) entries: Vec<CommitChangeRef>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitChangeRefChunk {
    pub(crate) format_version: u32,
    pub(crate) commit_id: CommitId,
    pub(crate) entries: Vec<CommitChangeRef>,
}

#[derive(musli::Encode)]
#[musli(packed)]
pub(crate) struct CommitChangeRefChunkRef<'a> {
    pub(crate) format_version: u32,
    pub(crate) schema_keys: Vec<&'a str>,
    #[musli(with = crate::storage_codec::vec_option)]
    pub(crate) file_ids: Vec<Option<&'a str>>,
    pub(crate) entries: Vec<CommitChangeRefEntryRef<'a>>,
}

#[derive(musli::Decode)]
#[musli(packed)]
pub(crate) struct CommitChangeRefChunkView<'a> {
    pub(crate) format_version: u32,
    pub(crate) schema_keys: Vec<&'a str>,
    #[musli(with = crate::storage_codec::vec_option)]
    pub(crate) file_ids: Vec<Option<&'a str>>,
    pub(crate) entries: Vec<CommitChangeRefEntryView<'a>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExpandedCommitChangeRefChunkView<'a> {
    pub(crate) format_version: u32,
    pub(crate) commit_id: CommitIdRef<'a>,
    pub(crate) entries: Vec<CommitChangeRefView<'a>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitChangeRef {
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) entity_pk: EntityPk,
    pub(crate) change_id: ChangeId,
}

#[derive(musli::Encode)]
#[musli(packed)]
pub(crate) struct CommitChangeRefEntryRef<'a> {
    pub(crate) schema_index: u16,
    pub(crate) file_index: u16,
    pub(crate) entity_pk: &'a [String],
    pub(crate) change_id: &'a str,
}

#[derive(musli::Decode)]
#[musli(packed)]
pub(crate) struct CommitChangeRefEntryView<'a> {
    pub(crate) schema_index: u16,
    pub(crate) file_index: u16,
    pub(crate) entity_pk: Vec<&'a str>,
    pub(crate) change_id: &'a str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitChangeRefView<'a> {
    pub(crate) schema_key: &'a str,
    pub(crate) file_id: Option<&'a str>,
    pub(crate) entity_pk: EntityPkRef<'a>,
    pub(crate) change_id: ChangeIdRef<'a>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EntityPkRef<'a> {
    pub(crate) parts: Vec<&'a str>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommitProjection {
    Record,
    ChangeRefs,
    Full,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CommitLoadRequest<'a> {
    pub(crate) commit_ids: &'a [CommitId],
    pub(crate) projection: CommitProjection,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitLoadBatch {
    pub(crate) entries: Vec<Option<CommitLoadEntry>>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CommitScanRequest<'a> {
    pub(crate) start_after: Option<&'a str>,
    pub(crate) limit: Option<usize>,
    pub(crate) projection: CommitProjection,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitScanBatch {
    pub(crate) entries: Vec<CommitLoadEntry>,
    pub(crate) next_start_after: Option<CommitId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CommitLoadEntry {
    Record(CommitRecord),
    ChangeRefs(Vec<CommitChangeRefChunk>),
    Full {
        record: CommitRecord,
        change_ref_chunks: Vec<CommitChangeRefChunk>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct ChangeRecord {
    pub(crate) format_version: u32,
    pub(crate) change_id: ChangeId,
    pub(crate) schema_key: String,
    pub(crate) entity_pk: EntityPk,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) file_id: Option<String>,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) snapshot_ref: Option<JsonRef>,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) metadata_ref: Option<JsonRef>,
    pub(crate) created_at: LixTimestamp,
}

#[derive(musli::Encode)]
#[musli(packed)]
pub(crate) struct ChangeRecordRef<'a> {
    pub(crate) format_version: u32,
    pub(crate) change_id: &'a str,
    pub(crate) schema_key: &'a str,
    pub(crate) entity_pk: &'a [String],
    #[musli(with = crate::storage_codec::option)]
    pub(crate) file_id: Option<&'a str>,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) snapshot_ref: Option<&'a JsonRef>,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) metadata_ref: Option<&'a JsonRef>,
    pub(crate) created_at: LixTimestamp,
}

#[derive(Clone, Debug, Eq, PartialEq, musli::Decode)]
#[musli(packed)]
pub(crate) struct ChangeRecordView<'a> {
    pub(crate) format_version: u32,
    pub(crate) change_id: ChangeIdRef<'a>,
    pub(crate) schema_key: &'a str,
    #[musli(with = entity_pk_ref_storage)]
    pub(crate) entity_pk: EntityPkRef<'a>,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) file_id: Option<&'a str>,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) snapshot_ref: Option<JsonRef>,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) metadata_ref: Option<JsonRef>,
    pub(crate) created_at: LixTimestamp,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ChangeLoadRequest<'a> {
    pub(crate) change_ids: &'a [ChangeId],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ChangeLoadBatch {
    pub(crate) entries: Vec<Option<ChangeRecord>>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ChangeScanRequest<'a> {
    pub(crate) start_after: Option<&'a str>,
    pub(crate) limit: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ChangeScanBatch {
    pub(crate) entries: Vec<ChangeRecord>,
    pub(crate) next_start_after: Option<ChangeId>,
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
    BranchHead(CommitId),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct GcLiveSet {
    pub(crate) commits: Vec<CommitId>,
    pub(crate) changes: Vec<ChangeId>,
    pub(crate) payloads: Vec<JsonRef>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct GcSweepSet {
    pub(crate) commits: Vec<CommitId>,
    pub(crate) changes: Vec<ChangeId>,
    pub(crate) commit_change_ref_chunks: Vec<(CommitId, u32)>,
    pub(crate) json_payloads: Vec<JsonRef>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct GcRepairSet {}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct GcPlan {
    pub(crate) roots: Vec<GcRoot>,
    pub(crate) live: GcLiveSet,
    pub(crate) sweep: GcSweepSet,
    pub(crate) repair: GcRepairSet,
}
