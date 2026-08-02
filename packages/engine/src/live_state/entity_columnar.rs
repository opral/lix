//! Storage identities for derived entity columnar row groups.

use std::collections::BTreeMap;

use crate::changelog::CommitId;
use crate::columnar_row_group::{EncodedRowGroupSet, RowGroupSetId};

pub(crate) type EntityColumnarWriteSets = BTreeMap<(CommitId, String), EncodedRowGroupSet>;

pub(crate) fn entity_row_group_set_id(commit_id: CommitId, schema_key: &str) -> RowGroupSetId {
    let mut digest = blake3::Hasher::new();
    digest.update(b"lix.entity_columnar.v1");
    digest.update(commit_id.as_uuid().as_bytes());
    digest.update(&(schema_key.len() as u64).to_be_bytes());
    digest.update(schema_key.as_bytes());
    let mut id = [0_u8; 16];
    id.copy_from_slice(&digest.finalize().as_bytes()[..16]);
    RowGroupSetId::new(id)
}
