use crate::changelog::{CHANGE_SPACE, ChangeRecord, change_key, encode_change_record};
use crate::json_store::{
    JsonSlot, JsonStoreContext, JsonWritePlacementRef, NormalizedJsonRef,
};
use crate::migration::publish::PublicationPlan;
use crate::migration::row_rewrite::{
    HistoricalSchemaCatalog, MaterializedV68Change, RewrittenChange,
};
use crate::migration::v68::V68StandaloneChange;
use crate::storage_adapter::StorageWriteSet;
use crate::LixError;

pub(super) struct StandalonePlan {
    pub(super) rewritten: Vec<RewrittenChange>,
    pub(super) catalog: HistoricalSchemaCatalog,
}

pub(super) fn plan_standalone_changes(
    changes: Vec<V68StandaloneChange>,
    publication: &mut PublicationPlan,
) -> Result<StandalonePlan, LixError> {
    let materialized = changes
        .into_iter()
        .map(materialized_change)
        .collect::<Vec<_>>();
    let catalog = HistoricalSchemaCatalog::from_changes(&materialized)?;
    let rewritten = materialized
        .iter()
        .map(|change| catalog.rewrite(change))
        .collect::<Result<Vec<_>, _>>()?;

    let encoded = rewritten
        .iter()
        .map(|change| {
            encode_change_record(&change.record)
                .map(|value| (change_key(change.record.change_id), value))
        })
        .collect::<Result<Vec<_>, _>>()?;
    publication.put_mutable(CHANGE_SPACE, encoded)?;

    let mut json_payloads = Vec::new();
    for change in &rewritten {
        if let Some(json) = change.staged_json.as_deref() {
            json_payloads.push(json);
        }
    }
    if !json_payloads.is_empty() {
        let mut writes = StorageWriteSet::new();
        JsonStoreContext::new().writer().stage_batch(
            &mut writes,
            JsonWritePlacementRef::OutOfBand,
            json_payloads.iter().copied().map(NormalizedJsonRef::new),
        )?;
        for (space, batch) in writes
            .into_migration_put_batches()
            .map_err(|error| migration_error(error.to_string()))?
        {
            publication.add_builder_batch(space, batch)?;
        }
    }
    Ok(StandalonePlan { rewritten, catalog })
}

fn materialized_change(change: V68StandaloneChange) -> MaterializedV68Change {
    let snapshot = change.snapshot_json.as_deref().map(JsonSlot::from_json).unwrap_or(JsonSlot::None);
    let metadata = change.metadata_json.as_deref().map(JsonSlot::from_json).unwrap_or(JsonSlot::None);
    MaterializedV68Change {
        snapshot_json: change.snapshot_json,
        record: ChangeRecord {
            format_version: change.format_version,
            change_id: change.change_id,
            account_id: change.account_id,
            schema_key: change.schema_key,
            row_pk: change.row_pk,
            file_id: change.file_id,
            snapshot,
            metadata,
            typed_payload: None,
            created_at: change.created_at,
            origin_key: change.origin_key,
        },
    }
}

fn migration_error(message: impl Into<String>) -> LixError {
    LixError::new("LIX_ERROR_MIGRATION_FAILED", message.into())
}
