use serde_json::json;

use crate::LixError;
use crate::changelog::CommitId;
use crate::common::SharedStr;
use crate::row_pk::RowPk;
use crate::transaction_types::{TransactionJson, TransactionWriteRow};

pub(crate) const COLLECTION_GENERATION_SCHEMA_KEY: &str = "lix_collection_generation";
/// Reserved internal count for a root-backed collection whose exact
/// cardinality has intentionally not been materialized.
pub(crate) const DEFERRED_LIVE_COUNT: u64 = u64::MAX;

/// One logical collection whose current physical generation can be replaced
/// without expanding the replacement into one tombstone per member.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct CollectionScopeRef<'a> {
    pub(crate) schema_key: &'a str,
    pub(crate) file_id: Option<&'a str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CollectionGeneration {
    pub(crate) active_generation: CommitId,
    pub(crate) live_count: u64,
    /// Present only while the active collection is exactly one certified,
    /// ordered, tracked, unfiled generation with no incremental mutations.
    pub(crate) ordered_identity_digest: Option<[u8; 32]>,
}

pub(crate) fn ordered_single_string_identity_digest<'a>(
    row_pks: impl IntoIterator<Item = &'a RowPk>,
) -> Option<[u8; 32]> {
    let mut hasher = blake3::Hasher::new();
    for row_pk in row_pks {
        let value = row_pk.as_single_string().ok()?;
        hasher.update(&(value.len() as u64).to_le_bytes());
        hasher.update(value.as_bytes());
    }
    Some(*hasher.finalize().as_bytes())
}

pub(crate) fn collection_scope_key(scope: CollectionScopeRef<'_>) -> String {
    serde_json::to_string(&(scope.schema_key, scope.file_id))
        .expect("serializing a collection scope tuple cannot fail")
}

pub(crate) fn collection_scope_from_row_pk(
    row_pk: &RowPk,
) -> Result<(String, Option<String>), LixError> {
    let scope_key = row_pk.as_single_string()?;
    serde_json::from_str(scope_key).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("collection-generation scope identity is malformed: {error}"),
        )
    })
}

pub(crate) fn collection_delete_stage_row(
    branch_id: &str,
    scope: CollectionScopeRef<'_>,
) -> TransactionWriteRow {
    TransactionWriteRow {
        row_pk: None,
        schema_key: SharedStr::from_static(COLLECTION_GENERATION_SCHEMA_KEY),
        file_id: None,
        snapshot: Some(TransactionJson::from_value_unchecked(json!({
            "scope_key": collection_scope_key(scope),
            "schema_key": scope.schema_key,
            "file_id": scope.file_id,
            "live_count": 0,
        }))),
        metadata: None,
        origin: None,
        created_at: None,
        updated_at: None,
        global: false,
        change_id: None,
        commit_id: None,
        untracked: false,
        branch_id: branch_id.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stage_row_is_one_branch_local_negative_fact() {
        let row = collection_delete_stage_row(
            "01920000-0000-7000-8000-0000000000a1",
            CollectionScopeRef {
                schema_key: "json_pointer",
                file_id: None,
            },
        );

        assert_eq!(row.schema_key, COLLECTION_GENERATION_SCHEMA_KEY);
        assert_eq!(row.file_id, None);
        assert!(!row.global);
        assert_eq!(
            row.snapshot.as_ref().expect("marker snapshot").value(),
            &json!({
                "schema_key": "json_pointer",
                "scope_key": "[\"json_pointer\",null]",
                "file_id": null,
                "live_count": 0,
            })
        );
    }
}
