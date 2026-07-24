#[cfg(feature = "storage-benches")]
mod bench_support;
#[cfg(feature = "storage-benches")]
pub mod bench {
    pub use super::bench_support::*;
}
mod codec;
mod context;
mod materialization;
mod store;
#[cfg(test)]
mod test_support;
mod types;

pub(crate) use codec::{
    decode_change_record, decode_commit_change_ref_chunk, encode_change_record,
    encode_commit_change_ref_chunk, encode_commit_record,
};
pub(crate) use context::{ChangelogContext, ChangelogStoreReader, ChangelogStoreWriter};
pub(crate) use materialization::{
    ChangeRecordProjection, MaterializedChangeIdentity, MaterializedChangePayload,
    load_change_records, materialize_change_payloads,
};
pub(crate) use store::{
    CHANGE_SPACE, COMMIT_CHANGE_ID_SPACE, COMMIT_CHANGE_REF_CHUNK_SPACE, COMMIT_SPACE, change_key,
};
pub(crate) use store::{ChangelogReader, ChangelogWriter};
pub(crate) use types::{
    ChangeId, ChangeLoadBatch, ChangeLoadRequest, ChangeRecord, ChangeRecordView, ChangeScanBatch,
    ChangeScanRequest, ChangelogAppend, CommitChangeRefChunk, CommitChangeRefSet, CommitId,
    CommitLoadBatch, CommitLoadEntry, CommitLoadRequest, CommitProjection, CommitRecord,
    CommitScanBatch, CommitScanRequest, commit_row_snapshot_json,
};
#[cfg(feature = "storage-benches")]
pub(crate) use types::{GcLiveSet, GcPlan, GcRepairSet, GcRoot, GcSweepSet, RebuildIndexStats};
