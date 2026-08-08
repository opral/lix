mod context;
mod materialization;
#[cfg(test)]
mod test_support;
mod types;

pub(crate) use context::{ChangelogContext, ChangelogReader, ChangelogWriter};
#[cfg(test)]
pub(crate) use materialization::MaterializedChangeIdentity;
pub(crate) use materialization::{
    ChangeRecordProjection, MaterializedChangePayload, load_change_records,
    materialize_known_change_payloads, materialize_known_change_payloads_in_order,
};
pub(crate) use types::{
    ChangeId, ChangeLoadBatch, ChangeLoadRequest, ChangeRecord, ChangeScanBatch, ChangeScanRequest,
    ChangelogAppend, CommitId, CommitLoadBatch, CommitLoadRequest, CommitRecord, CommitScanBatch,
    CommitScanRequest, TransactionChangeRecordRef, TransactionChangelogAppend,
    commit_row_snapshot_json, decode_forktree_change_payload, decode_forktree_commit_payload,
    encode_forktree_change_payload, encode_forktree_commit_payload,
};
pub(crate) use types::{GcLiveSet, GcPlan, GcRepairSet, GcRoot, GcSweepSet};
