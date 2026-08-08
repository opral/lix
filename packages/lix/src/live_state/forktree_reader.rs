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
    CanonicalBranchId, StateCell, StateSource, decode_state_key, open_coherent_view_on_read,
    state_range,
};
use crate::live_state::{
    LiveStateRowFilter, LiveStateScanRequest, MaterializedLiveStateBatch, MaterializedLiveStateRow,
};
use crate::storage_adapter::StorageAdapterRead;

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
