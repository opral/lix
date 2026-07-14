use crate::LixError;
use std::collections::HashMap;

use crate::changelog::{
    ChangeId, ChangeRecordProjection, MaterializedChangePayload, materialize_change_payloads,
};
use crate::entity_pk::EntityPk;
use crate::storage_adapter::StorageAdapterRead;
use crate::tracked_state::MaterializedTrackedStateRow;
use crate::tracked_state::types::{TrackedStateIndexValue, TrackedStateKey};

/// Materializes tracked-state index entries.
///
/// The durable tracked_state value carries only identity (change id, commit
/// id, flags, timestamps); payloads live once, in the change record the row
/// already references. Hydration is a batched changelog point read per
/// distinct change id, then json_store loads for the rare large payloads.
/// The GC contract makes this sound: a change record is only deletable when
/// no tracked-state row references its change id.
pub(crate) async fn materialize_rows_from_index_entries<S>(
    store: &S,
    entries: Vec<(TrackedStateKey, TrackedStateIndexValue)>,
    materialization: &ChangeRecordProjection,
) -> Result<Vec<MaterializedTrackedStateRow>, LixError>
where
    S: StorageAdapterRead,
{
    if !materialization.snapshot_content && !materialization.metadata {
        return Ok(entries
            .into_iter()
            .map(materialize_entry_without_json)
            .collect());
    }

    let mut remaining_payload_uses = HashMap::<ChangeId, usize>::new();
    for (_, value) in entries
        .iter()
        .filter(|(key, value)| !value.deleted && key.schema_key != COMMIT_SCHEMA_KEY)
    {
        *remaining_payload_uses.entry(value.change_id).or_default() += 1;
    }
    let mut payloads = materialize_change_payloads(
        store,
        entries
            .iter()
            .filter(|(key, value)| !value.deleted && key.schema_key != COMMIT_SCHEMA_KEY)
            .map(|(_, value)| value.change_id),
        *materialization,
        "tracked-state row",
    )
    .await?;

    let mut rows = Vec::with_capacity(entries.len());
    for (key, value) in entries {
        let (snapshot_content, metadata) = if value.deleted {
            (None, None)
        } else if key.schema_key == COMMIT_SCHEMA_KEY {
            // Commit rows have no change record; their snapshot is fully
            // derivable from the key (the entity pk is the commit id).
            (
                if materialization.snapshot_content {
                    Some(commit_row_snapshot_json(&key.entity_pk)?)
                } else {
                    None
                },
                None,
            )
        } else {
            let payload =
                take_payload(&mut payloads, &mut remaining_payload_uses, value.change_id)?;
            if let Some(identity) = payload.identity.as_ref()
                && (identity.schema_key != key.schema_key
                    || identity.entity_pk != key.entity_pk
                    || identity.file_id != key.file_id)
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "tracked-state row identity does not match referenced ChangeRecord '{}'",
                        value.change_id
                    ),
                ));
            }
            (payload.snapshot_content, payload.metadata)
        };
        rows.push(MaterializedTrackedStateRow {
            entity_pk: key.entity_pk,
            schema_key: key.schema_key,
            file_id: key.file_id,
            snapshot_content,
            metadata,
            deleted: value.deleted,
            created_at: value.created_at().to_string(),
            updated_at: value.updated_at().to_string(),
            change_id: value.change_id,
            commit_id: value.commit_id,
        });
    }
    Ok(rows)
}

fn take_payload(
    payloads: &mut HashMap<ChangeId, MaterializedChangePayload>,
    remaining_uses: &mut HashMap<ChangeId, usize>,
    change_id: ChangeId,
) -> Result<MaterializedChangePayload, LixError> {
    let remaining = remaining_uses.get_mut(&change_id).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("tracked-state row lost payload use count for ChangeRecord '{change_id}'"),
        )
    })?;
    if *remaining > 1 {
        *remaining -= 1;
        return payloads.get(&change_id).cloned().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked-state row references ChangeRecord '{change_id}' that was not materialized"
                ),
            )
        });
    }
    payloads.remove(&change_id).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked-state row references ChangeRecord '{change_id}' that was not materialized"
            ),
        )
    })
}

const COMMIT_SCHEMA_KEY: &str = "lix_commit";

fn commit_row_snapshot_json(entity_pk: &EntityPk) -> Result<String, LixError> {
    let Some(commit_id) = entity_pk.parts.first() else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "lix_commit row has an empty entity pk",
        ));
    };
    crate::changelog::commit_row_snapshot_json(commit_id)
}

fn materialize_entry_without_json(
    (key, value): (TrackedStateKey, TrackedStateIndexValue),
) -> MaterializedTrackedStateRow {
    MaterializedTrackedStateRow {
        entity_pk: key.entity_pk,
        schema_key: key.schema_key,
        file_id: key.file_id,
        snapshot_content: None,
        metadata: None,
        deleted: value.deleted,
        created_at: value.created_at().to_string(),
        updated_at: value.updated_at().to_string(),
        change_id: value.change_id,
        commit_id: value.commit_id,
    }
}
