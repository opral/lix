use crate::json_store::JsonStoreContext;
use crate::storage::KvScanRange;
use crate::storage::{KvGetGroup, KvGetRequest, KvScanRequest, StorageReader, StorageWriteSet};
use crate::untracked_state::{
    MaterializedUntrackedStateRow, UntrackedMaterializationProjection, UntrackedStateIdentity,
    UntrackedStateIdentityRef, UntrackedStateRow, UntrackedStateRowRef, UntrackedStateRowRequest,
    UntrackedStateScanRequest,
};
use crate::{LixError, NullableKeyFilter};

pub(super) const UNTRACKED_STATE_ROW_NAMESPACE: &str = "untracked_state.row";

pub(crate) async fn scan_rows(
    store: &mut impl StorageReader,
    request: &UntrackedStateScanRequest,
) -> Result<Vec<MaterializedUntrackedStateRow>, LixError> {
    let mut rows = scan_all_canonical_rows(store).await?;
    rows.retain(|row| row_matches_scan(row, request));
    if let Some(limit) = request.limit {
        rows.truncate(limit);
    }
    let projection = UntrackedMaterializationProjection::from_columns(&request.projection.columns);
    let mut json_reader = JsonStoreContext::new().reader(store);
    let mut materialized = Vec::with_capacity(rows.len());
    for row in rows {
        materialized.push(
            crate::untracked_state::materialize_row(&mut json_reader, row, &projection).await?,
        );
    }
    Ok(materialized)
}

pub(crate) async fn load_row(
    store: &mut impl StorageReader,
    request: &UntrackedStateRowRequest,
) -> Result<Option<MaterializedUntrackedStateRow>, LixError> {
    let Some(identity) = identity_from_request(request) else {
        return Ok(None);
    };
    let bytes = store
        .get_values(KvGetRequest {
            groups: vec![KvGetGroup {
                namespace: UNTRACKED_STATE_ROW_NAMESPACE.to_string(),
                keys: vec![encode_untracked_state_row_key(&identity)],
            }],
        })
        .await?
        .groups
        .into_iter()
        .next()
        .and_then(|group| group.single_value_owned());
    let Some(bytes) = bytes else {
        return Ok(None);
    };
    let row = crate::untracked_state::codec::decode_row(&bytes)?;
    let mut json_reader = JsonStoreContext::new().reader(store);
    crate::untracked_state::materialize_row(
        &mut json_reader,
        row,
        &UntrackedMaterializationProjection::full(),
    )
    .await
    .map(Some)
}

pub(super) async fn existing_identities<'a>(
    store: &mut (impl StorageReader + ?Sized),
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
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();

    let result = store
        .exists_many(KvGetRequest {
            groups: vec![KvGetGroup {
                namespace: UNTRACKED_STATE_ROW_NAMESPACE.to_string(),
                keys,
            }],
        })
        .await?;
    let group = result.groups.into_iter().next().ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "untracked identity existence probe returned no result group",
        )
    })?;
    if group.exists.len() != candidates.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "untracked identity existence probe returned {} results for {} requested keys",
                group.exists.len(),
                candidates.len()
            ),
        ));
    }

    Ok(candidates
        .into_iter()
        .zip(group.exists)
        .filter_map(|((_, identity), exists)| exists.then_some(identity))
        .collect())
}

pub(crate) fn stage_rows<'a, I>(writes: &mut StorageWriteSet, rows: I) -> Result<(), LixError>
where
    I: IntoIterator<Item = UntrackedStateRowRef<'a>>,
{
    for row in rows {
        if row.snapshot_ref.is_none() {
            writes.delete(
                UNTRACKED_STATE_ROW_NAMESPACE,
                encode_untracked_state_row_key_ref(row.into()),
            );
        } else {
            writes.put(
                UNTRACKED_STATE_ROW_NAMESPACE,
                encode_untracked_state_row_key_ref(row.into()),
                crate::untracked_state::codec::encode_row_ref(row)?,
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
            UNTRACKED_STATE_ROW_NAMESPACE,
            encode_untracked_state_row_key_ref(identity),
        );
    }
}

async fn scan_all_canonical_rows(
    store: &mut impl StorageReader,
) -> Result<Vec<UntrackedStateRow>, LixError> {
    let page = store
        .scan_values(KvScanRequest {
            namespace: UNTRACKED_STATE_ROW_NAMESPACE.to_string(),
            range: KvScanRange::prefix(Vec::new()),
            after: None,
            limit: usize::MAX,
        })
        .await?;
    page.values
        .iter()
        .map(crate::untracked_state::codec::decode_row)
        .collect()
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
    use std::sync::Arc;

    use super::*;
    use crate::backend::testing::UnitTestBackend;
    use crate::storage::{StorageContext, StorageWriteTransaction};
    use crate::untracked_state::UntrackedStateContext;

    async fn write_materialized_rows_to_store(
        context: &UntrackedStateContext,
        store: &mut (impl StorageWriteTransaction + ?Sized),
        rows: &[MaterializedUntrackedStateRow],
    ) {
        let mut writes = StorageWriteSet::new();
        let canonical_rows = {
            let mut json_writer = JsonStoreContext::new().writer();
            rows.iter()
                .map(|row| {
                    crate::test_support::untracked_state_row_from_materialized(
                        &mut writes,
                        &mut json_writer,
                        row,
                    )
                })
                .collect::<Result<Vec<_>, _>>()
                .expect("rows should canonicalize")
        };
        context
            .writer(&mut writes)
            .stage_rows(canonical_rows.iter().map(|row| row.as_ref()))
            .expect("rows should write");
        writes.apply(store).await.expect("rows should apply");
    }

    #[tokio::test]
    async fn write_and_load_roundtrips() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let context = UntrackedStateContext::new();
        let row = untracked_row("global", "lix_key_value", "ui-tab");

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        write_materialized_rows_to_store(
            &context,
            transaction.as_mut(),
            std::slice::from_ref(&row),
        )
        .await;
        transaction.commit().await.expect("commit should succeed");

        let loaded = {
            let mut reader = context.reader(storage.clone());
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
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let context = UntrackedStateContext::new();
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        write_materialized_rows_to_store(
            &context,
            transaction.as_mut(),
            &[
                untracked_row("global", "lix_key_value", "global-ui"),
                untracked_row("version-a", "lix_key_value", "version-ui"),
                untracked_row("version-a", "other_schema", "other"),
            ],
        )
        .await;
        transaction.commit().await.expect("commit should succeed");

        let rows = {
            let mut reader = context.reader(storage.clone());
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
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let context = UntrackedStateContext::new();
        let row = untracked_row("global", "lix_key_value", "ui-tab");
        let identity = UntrackedStateIdentity {
            version_id: row.version_id.clone(),
            schema_key: row.schema_key.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        };
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        let mut writes = StorageWriteSet::new();
        let canonical_row = {
            let mut json_writer = JsonStoreContext::new().writer();
            crate::test_support::untracked_state_row_from_materialized(
                &mut writes,
                &mut json_writer,
                &row,
            )
            .expect("row should canonicalize")
        };
        let mut writer = context.writer(&mut writes);
        writer
            .stage_rows(std::iter::once(canonical_row.as_ref()))
            .expect("write should succeed");
        writer.stage_delete_rows(std::iter::once(identity.as_ref()));
        writes
            .apply(&mut transaction.as_mut())
            .await
            .expect("writes should apply");
        transaction.commit().await.expect("commit should succeed");

        let loaded = {
            let mut reader = context.reader(storage.clone());
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
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            global: version_id == "global",
            version_id: version_id.to_string(),
        }
    }
}
