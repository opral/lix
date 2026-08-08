#[cfg(feature = "storage-benches")]
mod bench_support;
#[cfg(feature = "storage-benches")]
pub mod bench {
    pub use super::bench_support::*;
}
mod context;
mod materialization;
#[cfg(test)]
mod test_support;
mod types;

pub(crate) use context::ChangelogContext;
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
    commit_row_snapshot_json,
};
pub(crate) use types::{GcLiveSet, GcPlan, GcRepairSet, GcRoot, GcSweepSet};
