//! Current-state reader backed only by the authenticated ForkTree view.
//!
//! This adapter is deliberately small while the rest of the historical
//! reader conversion is in flight. It accepts the single-branch tracked
//! serving shape, keeps the caller's existing `StorageRead`, and refuses
//! lanes that still need a separate owner instead of falling back to a
//! deleted current-layout reader.

use std::sync::Arc;

use crate::LixError;
use crate::entity_pk::EntityPk;
use crate::forktree::{
    CanonicalBranchId, StateCell, StateKeyRef, StateSource, decode_state_key, encode_state_key,
    open_coherent_view_on_read, state_point, state_range,
};
use crate::live_state::{
    LiveStateExactBatchRequest, LiveStateRowFilter, LiveStateScanRequest,
    MaterializedLiveStateBatch, MaterializedLiveStateBatchBuilder, MaterializedLiveStateExactBatch,
    MaterializedLiveStateRow,
};
use crate::storage_adapter::StorageAdapterRead;

use super::derived::{is_derived_schema, request_may_include_derived};

/// Reads one selected branch through its authenticated global/local state
/// pair. Unsupported lanes return a typed error so callers cannot silently
/// revive an old reader.
pub(crate) async fn scan_branch<S>(
    store: &S,
    request: &LiveStateScanRequest,
) -> Result<MaterializedLiveStateBatch, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    validate_scan_request(request)?;
    let [branch_id] = request.filter.branch_ids.as_slice() else {
        return Err(unsupported("current ForkTree reader requires one branch"));
    };
    if request.filter.untracked == Some(true)
        || !request.filter.constraints.is_empty()
        || !matches!(request.filter.rows, LiveStateRowFilter::All)
    {
        return Err(unsupported(
            "current ForkTree reader does not yet own this scan lane",
        ));
    }

    let branch_id = parse_branch_id(branch_id)?;
    let view = open_coherent_view_on_read(store, branch_id).await?;
    let rows = state_range(&view, None, None, None, true).await?;
    let mut output = Vec::with_capacity(rows.len());
    for row in rows {
        let key = decode_state_key(&row.encoded_key)?;
        if !request.filter.schema_keys.is_empty()
            && !request
                .filter
                .schema_keys
                .iter()
                .any(|schema| schema == &key.schema_key)
        {
            continue;
        }
        if !request.filter.entity_pks.is_empty()
            && !request
                .filter
                .entity_pks
                .iter()
                .any(|entity| entity == &key.entity_pk)
        {
            continue;
        }
        if !request
            .filter
            .file_ids
            .iter()
            .all(|filter| filter.matches(key.file_id.as_ref()))
        {
            continue;
        }
        if row.value.cell.deleted() && !request.filter.include_tombstones {
            continue;
        }
        let branch_owner = match row.source {
            StateSource::Global => crate::GLOBAL_BRANCH_ID.to_owned(),
            StateSource::Branch => branch_id_text(branch_id),
        };
        output.push(materialize_row(
            row,
            key.entity_pk,
            key.schema_key,
            key.file_id,
            branch_owner,
        ));
        if request.limit.is_some_and(|limit| output.len() >= limit) {
            break;
        }
    }
    Ok(MaterializedLiveStateBatch::from_rows(output))
}

/// Loads correlated current-state identities from one authenticated
/// selector/root view. This deliberately has no scan-scope or legacy
/// tracked-head fallback: unsupported derived, untracked, and multi-branch
/// requests fail before a view is opened.
pub(crate) async fn load_exact_batch<S>(
    store: &S,
    request: &LiveStateExactBatchRequest,
) -> Result<MaterializedLiveStateExactBatch, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    validate_exact_request(request)?;
    if request.rows.is_empty() {
        return Ok(MaterializedLiveStateExactBatch::default());
    }
    let branch_id = parse_branch_id(&request.rows[0].branch_id)?;
    let view = open_coherent_view_on_read(store, branch_id).await?;
    let mut builder = MaterializedLiveStateBatchBuilder::with_capacity(request.rows.len());
    let mut slots = Vec::with_capacity(request.rows.len());
    for requested in &request.rows {
        let key = encode_state_key(StateKeyRef {
            schema_key: &requested.schema_key,
            file_id: requested.file_id.as_deref(),
            entity_pk: &requested.entity_pk,
        });
        let Some(row) = state_point(&view, &key, request.include_tombstones).await? else {
            slots.push(None);
            continue;
        };
        let decoded_key = decode_state_key(&row.encoded_key)?;
        builder.push_owned(materialize_row(
            row,
            decoded_key.entity_pk,
            decoded_key.schema_key,
            decoded_key.file_id,
            requested.branch_id.clone(),
        ));
        let ordinal = u32::try_from(builder.len().saturating_sub(1)).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "exact live-state result exceeds u32 rows",
            )
        })?;
        slots.push(Some(ordinal));
    }
    MaterializedLiveStateExactBatch::new(builder.finish(), slots)
}

fn validate_scan_request(request: &LiveStateScanRequest) -> Result<(), LixError> {
    if request_may_include_derived(request) {
        return Err(unsupported(
            "current ForkTree reader does not serve derived or history schemas",
        ));
    }
    Ok(())
}

fn validate_exact_request(request: &LiveStateExactBatchRequest) -> Result<(), LixError> {
    if request.untracked == Some(true) {
        return Err(unsupported(
            "current ForkTree reader does not serve untracked exact rows",
        ));
    }
    let Some(first) = request.rows.first() else {
        return Ok(());
    };
    if request
        .rows
        .iter()
        .any(|row| is_derived_schema(&row.schema_key))
    {
        return Err(unsupported(
            "current ForkTree reader does not serve derived or history schemas",
        ));
    }
    if request
        .rows
        .iter()
        .any(|row| row.branch_id != first.branch_id)
    {
        return Err(unsupported(
            "current ForkTree exact reader requires one branch per coherent view",
        ));
    }
    Ok(())
}

fn parse_branch_id(value: &str) -> Result<CanonicalBranchId, LixError> {
    let uuid = uuid::Uuid::parse_str(value).map_err(|error| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("branch ID must be a UUID: {error}"),
        )
    })?;
    Ok(CanonicalBranchId::from_bytes(*uuid.as_bytes()))
}

fn branch_id_text(branch_id: CanonicalBranchId) -> String {
    uuid::Uuid::from_bytes(*branch_id.as_bytes()).to_string()
}

fn materialize_row(
    row: crate::forktree::VisibleStateRow,
    entity_pk: EntityPk,
    schema_key: String,
    file_id: Option<String>,
    branch_id: String,
) -> MaterializedLiveStateRow {
    let deleted = row.value.cell.deleted();
    let snapshot_content = match &row.value.cell {
        StateCell::Value(value) => Some(value.clone()),
        StateCell::Null | StateCell::Tombstone => None,
    };
    MaterializedLiveStateRow {
        entity_pk,
        schema_key,
        file_id,
        snapshot_content,
        metadata: row.value.metadata,
        deleted,
        created_at: row.value.created_at,
        updated_at: row.value.updated_at,
        global: matches!(row.source, StateSource::Global),
        change_id: Some(row.value.change_id),
        commit_id: Some(row.value.commit_id),
        untracked: false,
        branch_id: Arc::from(branch_id),
    }
}

fn unsupported(message: &'static str) -> LixError {
    LixError::new(LixError::CODE_INTERNAL_ERROR, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity_pk::EntityPk;
    use crate::live_state::LiveStateExactRowRequest;

    fn exact(schema_key: &str) -> LiveStateExactBatchRequest {
        LiveStateExactBatchRequest {
            rows: vec![LiveStateExactRowRequest {
                schema_key: schema_key.to_owned(),
                branch_id: "01920000-0000-7000-8000-0000000000a1".to_owned(),
                entity_pk: EntityPk::single("row"),
                file_id: None,
            }],
            ..Default::default()
        }
    }

    #[test]
    fn scan_rejects_derived_schema_before_view_acquisition() {
        let request = LiveStateScanRequest {
            filter: crate::live_state::LiveStateFilter {
                schema_keys: vec!["lix_commit".to_owned()],
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(validate_scan_request(&request).is_err());
    }

    #[test]
    fn exact_rejects_history_and_untracked_before_view_acquisition() {
        assert!(validate_exact_request(&exact("lix_commit")).is_err());
        assert!(validate_exact_request(&exact("lix_commit_edge")).is_err());

        let mut untracked = exact("app.schema");
        untracked.untracked = Some(true);
        assert!(validate_exact_request(&untracked).is_err());
    }

    #[test]
    fn exact_rejects_cross_branch_batches_before_view_acquisition() {
        let mut request = exact("app.schema");
        request.rows.push(LiveStateExactRowRequest {
            branch_id: "01920000-0000-7000-8000-0000000000a2".to_owned(),
            ..request.rows[0].clone()
        });
        assert!(validate_exact_request(&request).is_err());
    }
}
