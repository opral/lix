use bytes::Bytes;

use crate::storage::{
    PointReadPlan, ScanPlan, StorageCoreProjection, StorageGetOptions, StorageKey, StoragePrefix,
    StorageProjectedValue, StorageRead, StorageScanOptions, StorageSpace, StorageSpaceId,
    StorageValue, StorageWriteSet,
};
use crate::untracked_state::{
    MaterializedUntrackedStateRow, UntrackedMaterializationProjection, UntrackedStateIdentity,
    UntrackedStateIdentityRef, UntrackedStateRow, UntrackedStateRowRef, UntrackedStateRowRequest,
    UntrackedStateScanRequest,
};
use crate::{LixError, NullableKeyFilter};

pub(super) const UNTRACKED_STATE_ROW_NAMESPACE: &str = "untracked_state.row";
const UNTRACKED_STATE_ROW_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0001_0001), UNTRACKED_STATE_ROW_NAMESPACE);

pub(crate) async fn scan_rows(
    store: &impl StorageRead,
    request: &UntrackedStateScanRequest,
) -> Result<Vec<MaterializedUntrackedStateRow>, LixError> {
    let mut rows = scan_all_canonical_rows(store).await?;
    rows.retain(|row| row_matches_scan(row, request));
    if let Some(limit) = request.limit {
        rows.truncate(limit);
    }
    let projection = UntrackedMaterializationProjection::from_columns(&request.projection.columns);
    let mut materialized = Vec::with_capacity(rows.len());
    for row in rows {
        materialized.push(crate::untracked_state::materialize_row(row, &projection)?);
    }
    Ok(materialized)
}

pub(crate) async fn load_row(
    store: &impl StorageRead,
    request: &UntrackedStateRowRequest,
) -> Result<Option<MaterializedUntrackedStateRow>, LixError> {
    let Some(identity) = identity_from_request(request) else {
        return Ok(None);
    };
    let result = PointReadPlan::new(
        UNTRACKED_STATE_ROW_SPACE,
        &[StorageKey(Bytes::from(encode_untracked_state_row_key(
            &identity,
        )))],
    )
    .materialize(store, StorageGetOptions::default())?;
    let bytes = result
        .value
        .into_iter()
        .next()
        .flatten()
        .and_then(full_value);
    let Some(bytes) = bytes else {
        return Ok(None);
    };
    let row = crate::untracked_state::codec::decode_row(&bytes)?;
    crate::untracked_state::materialize_row(row, &UntrackedMaterializationProjection::full())
        .map(Some)
}

pub(super) async fn existing_identities<'a>(
    store: &(impl StorageRead + ?Sized),
    identities: impl IntoIterator<Item = UntrackedStateIdentityRef<'a>>,
) -> Result<Vec<UntrackedStateIdentity>, LixError> {
    let mut candidates = identities
        .into_iter()
        .map(|identity| {
            let owned = UntrackedStateIdentity {
                version_id: identity.version_id.to_string(),
                schema_key: identity.schema_key.to_string(),
                entity_id: identity.entity_id.clone(),
                file_id: identity.file_id.map(str::to_string),
            };
            let key = encode_untracked_state_row_key_ref(owned.as_ref());
            (key, owned)
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|(left, _), (right, _)| left.cmp(right));
    candidates.dedup_by(|(left, _), (right, _)| left == right);
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let keys = candidates
        .iter()
        .map(|(key, _)| StorageKey(Bytes::from(key.clone())))
        .collect::<Vec<_>>();

    let result = PointReadPlan::from_unique_keys(UNTRACKED_STATE_ROW_SPACE, keys).materialize(
        store,
        StorageGetOptions {
            projection: StorageCoreProjection::KeyOnly,
            ..StorageGetOptions::default()
        },
    )?;
    let exists = result
        .value
        .into_iter()
        .map(|value| value.is_some())
        .collect::<Vec<_>>();
    if exists.len() != candidates.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "untracked identity existence probe returned {} results for {} requested keys",
                exists.len(),
                candidates.len()
            ),
        ));
    }

    Ok(candidates
        .into_iter()
        .zip(exists)
        .filter_map(|((_, identity), exists)| exists.then_some(identity))
        .collect())
}

pub(crate) fn stage_rows<'a, I>(writes: &mut StorageWriteSet, rows: I) -> Result<(), LixError>
where
    I: IntoIterator<Item = UntrackedStateRowRef<'a>>,
{
    for row in rows {
        if row.snapshot_content.is_none() {
            writes.delete(
                UNTRACKED_STATE_ROW_SPACE,
                StorageKey(Bytes::from(encode_untracked_state_row_key_ref(row.into()))),
            );
        } else {
            writes.put(
                UNTRACKED_STATE_ROW_SPACE,
                StorageKey(Bytes::from(encode_untracked_state_row_key_ref(row.into()))),
                StorageValue {
                    bytes: Bytes::from(crate::untracked_state::codec::encode_row_ref(row)?),
                },
            );
        }
    }
    Ok(())
}

pub(crate) fn stage_delete_rows<'a, I>(writes: &mut StorageWriteSet, identities: I)
where
    I: IntoIterator<Item = UntrackedStateIdentityRef<'a>>,
{
    for identity in identities {
        writes.delete(
            UNTRACKED_STATE_ROW_SPACE,
            StorageKey(Bytes::from(encode_untracked_state_row_key_ref(identity))),
        );
    }
}

async fn scan_all_canonical_rows(
    store: &impl StorageRead,
) -> Result<Vec<UntrackedStateRow>, LixError> {
    let page = ScanPlan::prefix(
        UNTRACKED_STATE_ROW_SPACE,
        StoragePrefix {
            bytes: Bytes::new(),
        },
    )
    .collect(store, StorageScanOptions::default())?;
    page.value
        .entries
        .into_iter()
        .filter_map(|entry| full_value(entry.value))
        .map(|bytes| crate::untracked_state::codec::decode_row(bytes.as_ref()))
        .collect()
}

fn full_value(value: StorageProjectedValue) -> Option<Bytes> {
    match value {
        StorageProjectedValue::FullValue(bytes) => Some(bytes),
        StorageProjectedValue::KeyOnly => None,
    }
}

fn row_matches_scan(row: &UntrackedStateRow, request: &UntrackedStateScanRequest) -> bool {
    (request.filter.schema_keys.is_empty() || request.filter.schema_keys.contains(&row.schema_key))
        && (request.filter.entity_ids.is_empty()
            || request.filter.entity_ids.contains(&row.entity_id))
        && (request.filter.version_ids.is_empty()
            || request.filter.version_ids.contains(&row.version_id))
        && nullable_matches_filters(&row.file_id, &request.filter.file_ids)
}

fn nullable_matches_filters(value: &Option<String>, filters: &[NullableKeyFilter<String>]) -> bool {
    filters.is_empty()
        || filters.iter().any(|filter| match filter {
            NullableKeyFilter::Any => true,
            NullableKeyFilter::Null => value.is_none(),
            NullableKeyFilter::Value(expected) => value.as_ref() == Some(expected),
        })
}

fn identity_from_request(request: &UntrackedStateRowRequest) -> Option<UntrackedStateIdentity> {
    let file_id = match &request.file_id {
        NullableKeyFilter::Null => None,
        NullableKeyFilter::Value(value) => Some(value.clone()),
        NullableKeyFilter::Any => return None,
    };
    Some(UntrackedStateIdentity {
        version_id: request.version_id.clone(),
        schema_key: request.schema_key.clone(),
        entity_id: request.entity_id.clone(),
        file_id,
    })
}

fn encode_untracked_state_row_key(identity: &UntrackedStateIdentity) -> Vec<u8> {
    encode_untracked_state_row_key_ref(identity.as_ref())
}

pub(super) fn encode_untracked_state_row_key_ref(
    identity: UntrackedStateIdentityRef<'_>,
) -> Vec<u8> {
    let mut out = Vec::new();
    push_component(&mut out, identity.version_id);
    push_component(&mut out, identity.schema_key);
    let entity_id = identity
        .entity_id
        .as_json_array_text()
        .expect("untracked-state identity should project");
    push_component(&mut out, &entity_id);
    match identity.file_id {
        Some(file_id) => {
            out.push(1);
            push_component(&mut out, file_id);
        }
        None => out.push(0),
    }
    out
}

fn push_component(out: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageContext;
    use crate::storage::{InMemoryStorageBackend, StorageReadOptions, StorageWriteOptions};
    use crate::untracked_state::UntrackedStateContext;

    async fn write_materialized_rows_to_store(
        context: &UntrackedStateContext,
        storage: &StorageContext,
        rows: &[MaterializedUntrackedStateRow],
    ) {
        let mut writes = storage.new_write_set();
        let canonical_rows = rows
            .iter()
            .map(|row| crate::test_support::untracked_state_row_from_materialized(&mut writes, row))
            .collect::<Result<Vec<_>, _>>()
            .expect("rows should canonicalize");
        context
            .writer(&mut writes)
            .stage_rows(canonical_rows.iter().map(|row| row.as_ref()))
            .expect("rows should write");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("rows should commit");
    }

    #[tokio::test]
    async fn write_and_load_roundtrips() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let context = UntrackedStateContext::new();
        let row = untracked_row("global", "lix_key_value", "ui-tab");

        write_materialized_rows_to_store(&context, &storage, std::slice::from_ref(&row)).await;

        let loaded = {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open");
            let mut reader = context.reader(read);
            reader
                .load_row(&UntrackedStateRowRequest {
                    schema_key: "lix_key_value".to_string(),
                    version_id: "global".to_string(),
                    entity_id: crate::entity_identity::EntityIdentity::single("ui-tab"),
                    file_id: NullableKeyFilter::Null,
                })
                .await
        }
        .expect("load should succeed");
        assert_eq!(loaded, Some(row));
    }

    #[tokio::test]
    async fn scan_filters_by_schema_and_version() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let context = UntrackedStateContext::new();
        write_materialized_rows_to_store(
            &context,
            &storage,
            &[
                untracked_row("global", "lix_key_value", "global-ui"),
                untracked_row("version-a", "lix_key_value", "version-ui"),
                untracked_row("version-a", "other_schema", "other"),
            ],
        )
        .await;

        let rows = {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open");
            let mut reader = context.reader(read);
            reader
                .scan_rows(&UntrackedStateScanRequest {
                    filter: crate::untracked_state::UntrackedStateFilter {
                        schema_keys: vec!["lix_key_value".to_string()],
                        version_ids: vec!["version-a".to_string()],
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .await
        }
        .expect("scan should succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].entity_id,
            crate::entity_identity::EntityIdentity::single("version-ui")
        );
    }

    #[tokio::test]
    async fn delete_removes_row() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let context = UntrackedStateContext::new();
        let row = untracked_row("global", "lix_key_value", "ui-tab");
        let identity = UntrackedStateIdentity {
            version_id: row.version_id.clone(),
            schema_key: row.schema_key.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        };
        write_materialized_rows_to_store(&context, &storage, std::slice::from_ref(&row)).await;

        let mut writes = storage.new_write_set();
        let mut writer = context.writer(&mut writes);
        writer.stage_delete_rows(std::iter::once(identity.as_ref()));
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("writes should commit");

        let loaded = {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .expect("read should open");
            let mut reader = context.reader(read);
            reader
                .load_row(&UntrackedStateRowRequest {
                    schema_key: "lix_key_value".to_string(),
                    version_id: "global".to_string(),
                    entity_id: crate::entity_identity::EntityIdentity::single("ui-tab"),
                    file_id: NullableKeyFilter::Null,
                })
                .await
        }
        .expect("load should succeed");
        assert_eq!(loaded, None);
    }

    fn untracked_row(
        version_id: &str,
        schema_key: &str,
        entity_id: &str,
    ) -> MaterializedUntrackedStateRow {
        MaterializedUntrackedStateRow {
            entity_id: crate::entity_identity::EntityIdentity::single(entity_id),
            schema_key: schema_key.to_string(),
            file_id: None,
            snapshot_content: Some(format!("{{\"key\":\"{}\",\"value\":\"value\"}}", entity_id)),
            metadata: None,
            deleted: false,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            global: version_id == "global",
            version_id: version_id.to_string(),
        }
    }
}
