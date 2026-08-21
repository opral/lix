use crate::LixError;
use crate::changelog::{CHANGE_SPACE, change_key, encode_change_record};
use crate::json_store::LegacyJsonValue;
use crate::migration::publish::PublicationPlan;
use crate::migration::row_rewrite::{
    HistoricalSchemaCatalog, MaterializedV68Change, RewrittenChange,
};
use crate::migration::v68::{V68ChangeRecord, V68StandaloneChange};

pub(super) struct StandalonePlan {
    pub(super) rewritten: Vec<RewrittenChange>,
    pub(super) catalog: HistoricalSchemaCatalog,
}

pub(super) fn plan_standalone_changes(
    changes: Vec<V68StandaloneChange>,
    authority_registrations: &[MaterializedV68Change],
    publication: &mut PublicationPlan,
) -> Result<StandalonePlan, LixError> {
    let materialized = changes
        .into_iter()
        .map(materialized_change)
        .collect::<Vec<_>>();
    let catalog = HistoricalSchemaCatalog::from_changes(
        &materialized
            .iter()
            .chain(authority_registrations)
            .cloned()
            .collect::<Vec<_>>(),
    )?;
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

    Ok(StandalonePlan { rewritten, catalog })
}

fn materialized_change(change: V68StandaloneChange) -> MaterializedV68Change {
    let snapshot = change
        .snapshot_json
        .as_deref()
        .map(LegacyJsonValue::from_json)
        .unwrap_or(LegacyJsonValue::None);
    let metadata = change
        .metadata_json
        .as_deref()
        .map(LegacyJsonValue::from_json)
        .unwrap_or(LegacyJsonValue::None);
    MaterializedV68Change {
        snapshot_json: change.snapshot_json,
        metadata_json: change.metadata_json,
        record: V68ChangeRecord {
            format_version: change.format_version,
            change_id: change.change_id,
            account_id: change.account_id,
            schema_key: change.schema_key,
            row_pk: change.row_pk,
            file_id: change.file_id,
            snapshot,
            metadata,
            created_at: change.created_at,
            origin_key: change.origin_key,
        },
    }
}
