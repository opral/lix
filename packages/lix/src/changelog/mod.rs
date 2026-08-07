#[cfg(feature = "storage-benches")]
mod bench_support;
#[cfg(feature = "storage-benches")]
pub mod bench {
    pub use super::bench_support::*;
}
mod codec;
mod context;
mod materialization;
mod semantic_history;
mod store;
#[cfg(test)]
mod test_support;
mod types;

#[cfg(test)]
pub(crate) use codec::decode_change_record;
#[cfg(test)]
pub(crate) use codec::encode_commit_record;
pub(crate) use context::{ChangelogContext, load_commit_id_for_change};
#[cfg(test)]
pub(crate) use materialization::MaterializedChangeIdentity;
pub(crate) use materialization::{
    ChangeRecordProjection, MaterializedChangePayload, load_change_records,
    materialize_known_change_payloads, materialize_known_change_payloads_in_order,
};
pub(crate) use store::SEMANTIC_HISTORY_SPACE;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use store::{
    CHANGE_SPACE, COMMIT_CHANGE_ID_SPACE, COMMIT_SPACE, change_key, commit_change_id_key,
    commit_key,
};
pub(crate) use store::{ChangelogReader, ChangelogWriter};
pub(crate) use types::{
    ChangeId, ChangeLoadBatch, ChangeLoadRequest, ChangeRecord, ChangeScanBatch, ChangeScanRequest,
    ChangelogAppend, CommitId, CommitLoadBatch, CommitLoadRequest, CommitRecord, CommitScanBatch,
    CommitScanRequest, TransactionChangeRecordRef, TransactionChangelogAppend,
    commit_row_snapshot_json,
};
pub(crate) use types::{GcLiveSet, GcPlan, GcRepairSet, GcRoot, GcSweepSet};
