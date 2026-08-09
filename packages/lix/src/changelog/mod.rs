mod materialization;
#[cfg(test)]
mod test_support;
mod types;

pub(crate) use materialization::{
    ChangeRecordProjection, load_change_records, materialize_known_change_payloads,
};
#[cfg(test)]
pub(crate) use types::ChangeLoadBatch;
pub(crate) use types::{
    ChangeId, ChangeRecord, CommitId, CommitRecord, commit_row_snapshot_json,
    decode_forktree_change_payload, decode_forktree_commit_payload, encode_forktree_change_payload,
    encode_forktree_commit_payload, forktree_change_json_payload_ids,
};
