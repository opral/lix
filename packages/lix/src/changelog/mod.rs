#[cfg(feature = "storage-benches")]
mod bench_support;
#[cfg(feature = "storage-benches")]
pub mod bench {
    pub use super::bench_support::*;
}
mod codec;
mod context;
mod gc;
mod materialization;
mod scope_digest;
mod store;
#[cfg(test)]
mod test_support;
mod types;

pub(crate) use codec::decode_change_record;
#[cfg(test)]
pub(crate) use codec::encode_commit_record;
pub(crate) use context::ChangelogContext;
#[cfg(test)]
pub(crate) use gc::{stage_delete_changes, stage_delete_commits};
pub(crate) use gc::{stage_delete_commit_projection, stage_delete_standalone_change};
#[cfg(test)]
pub(crate) use materialization::MaterializedChangeIdentity;
pub(crate) use materialization::{
    ChangeRecordProjection, MaterializedChangePayload, load_change_records,
    materialize_known_change_payloads, materialize_known_change_payloads_in_order,
};
pub(crate) use scope_digest::{CommitScopeKey, CommitTouchedScopeDigest};
pub(crate) use store::{CHANGE_SPACE, COMMIT_SPACE, commit_key};
pub(crate) use store::{ChangelogReader, ChangelogWriter};
pub(crate) use types::COMMIT_RECORD_FORMAT_VERSION;
pub(crate) use types::{
    AuthenticatedNativeRow, ChangeId, ChangeLoadBatch, ChangeLoadRequest, ChangePayload,
    ChangeRecord, ChangeScanBatch, ChangeScanRequest, ChangelogAppend, CommitId, CommitLoadBatch,
    CommitLoadRequest, CommitRecord, CommitScanBatch, CommitScanRequest, NativeRowField,
    NativeScalarCell,
    TransactionChangeRecordRef, TransactionChangelogAppend,
    commit_row_snapshot_json, next_first_parent_jump,
};
pub(crate) use types::{GcLiveSet, GcPlan, GcRepairSet, GcRoot, GcSweepSet};
