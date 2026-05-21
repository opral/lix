#[cfg(feature = "storage-benches")]
mod bench_support;
#[cfg(feature = "storage-benches")]
pub mod bench {
    pub use super::bench_support::*;
}
mod codec;
mod context;
mod store;
#[cfg(test)]
mod test_support;
mod types;

pub(crate) use codec::{
    decode_change_record, decode_commit_change_ref_chunk, decode_commit_record,
    encode_change_record, encode_commit_change_ref_chunk, encode_commit_record, view_change_record,
    view_commit_change_ref_chunk, view_commit_record,
};
pub(crate) use context::{ChangelogContext, ChangelogStoreReader, ChangelogStoreWriter};
pub(crate) use store::{ChangelogReader, ChangelogWriter};
pub(crate) use store::{CHANGE_SPACE, COMMIT_CHANGE_REF_CHUNK_SPACE, COMMIT_SPACE};
pub(crate) use types::{
    ChangeId, ChangeIdRef, ChangeLoadBatch, ChangeLoadRequest, ChangeRecord, ChangeRecordView,
    ChangeScanBatch, ChangeScanRequest, ChangelogAppend, CommitChangeRef, CommitChangeRefChunk,
    CommitChangeRefChunkView, CommitChangeRefSet, CommitChangeRefView, CommitId, CommitIdRef,
    CommitLoadBatch, CommitLoadEntry, CommitLoadRequest, CommitProjection, CommitRecord,
    CommitRecordView, CommitScanBatch, CommitScanRequest, EntityIdentityRef, GcLiveSet, GcPlan,
    GcRepairSet, GcRoot, GcSweepSet, RebuildIndexStats,
};
