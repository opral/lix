#![allow(clippy::ref_option, clippy::uninlined_format_args)]

use bytes::Bytes;

use super::types::{
    UntrackedBranchPrefixRef, UntrackedBranchSchemaEntityFilePrefixRef,
    UntrackedBranchSchemaEntityPrefixRef, UntrackedBranchSchemaPrefixRef,
};
use crate::entity_pk::EntityPk;
use crate::storage::{
    PointReadPlan, ScanPlan, StorageCoreProjection, StorageGetOptions, StorageKey, StoragePrefix,
    StorageProjectedValue, StorageRead, StorageScanOptions, StorageSpace, StorageSpaceId,
    StorageValue, StorageWriteSet,
};
use crate::storage_codec;
use crate::untracked_state::{
    MaterializedUntrackedStateRow, UntrackedMaterializationProjection, UntrackedStateIdentity,
    UntrackedStateIdentityRef, UntrackedStateRow, UntrackedStateRowRef, UntrackedStateRowRequest,
    UntrackedStateScanRequest,
};
use crate::{LixError, NullableKeyFilter};

pub(super) const UNTRACKED_STATE_ROW_NAMESPACE: &str = "untracked_state.row.v1";
pub(crate) const UNTRACKED_STATE_ROW_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0001_0002), UNTRACKED_STATE_ROW_NAMESPACE);

pub(crate) async fn scan_rows(
    store: &impl StorageRead,
    request: &UntrackedStateScanRequest,
) -> Result<Vec<MaterializedUntrackedStateRow>, LixError> {
    let projection = UntrackedMaterializationProjection::from_columns(&request.projection.columns);
    let plans = scan_plans_for_request(request)?;
    let mut materialized = Vec::new();

    for plan in plans {
        scan_matching_rows(store, request, &projection, &plan, &mut materialized).await?;
        if request
            .limit
            .is_some_and(|limit| materialized.len() >= limit)
        {
            break;
        }
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
        )?))],
    )
    .materialize(store, StorageGetOptions::default())
    .await?;
    let bytes = result
        .value
        .into_iter()
        .next()
        .flatten()
        .and_then(full_value);
    let Some(bytes) = bytes else {
        return Ok(None);
    };
    let row = crate::untracked_state::codec::decode_payload_with_identity(identity, &bytes)?;
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
                branch_id: identity.branch_id.to_string(),
                schema_key: identity.schema_key.to_string(),
                entity_pk: identity.entity_pk.clone(),
                file_id: identity.file_id.map(str::to_string),
            };
            let key = encode_untracked_state_row_key_ref(owned.as_ref())?;
            Ok((key, owned))
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    candidates.sort_by(|(left, _), (right, _)| left.cmp(right));
    candidates.dedup_by(|(left, _), (right, _)| left == right);
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let keys = candidates
        .iter()
        .map(|(key, _)| StorageKey(Bytes::from(key.clone())))
        .collect::<Vec<_>>();

    let result = PointReadPlan::from_unique_keys(UNTRACKED_STATE_ROW_SPACE, keys)
        .materialize(
            store,
            StorageGetOptions {
                projection: StorageCoreProjection::KeyOnly,
            },
        )
        .await?;
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
                StorageKey(Bytes::from(encode_untracked_state_row_key_ref(row.into())?)),
            );
        } else {
            writes.put(
                UNTRACKED_STATE_ROW_SPACE,
                StorageKey(Bytes::from(encode_untracked_state_row_key_ref(row.into())?)),
                StorageValue {
                    bytes: Bytes::from(crate::untracked_state::codec::encode_payload_ref(row)?),
                },
            );
        }
    }
    Ok(())
}

pub(crate) fn stage_delete_rows<'a, I>(
    writes: &mut StorageWriteSet,
    identities: I,
) -> Result<(), LixError>
where
    I: IntoIterator<Item = UntrackedStateIdentityRef<'a>>,
{
    for identity in identities {
        writes.delete(
            UNTRACKED_STATE_ROW_SPACE,
            StorageKey(Bytes::from(encode_untracked_state_row_key_ref(identity)?)),
        );
    }
    Ok(())
}

async fn scan_matching_rows(
    store: &impl StorageRead,
    request: &UntrackedStateScanRequest,
    projection: &UntrackedMaterializationProjection,
    plan: &ScanPlan,
    materialized: &mut Vec<MaterializedUntrackedStateRow>,
) -> Result<(), LixError> {
    let mut resume_after = None;
    loop {
        let remaining_limit = request
            .limit
            .map(|limit| limit.saturating_sub(materialized.len()));
        if matches!(remaining_limit, Some(0)) {
            break;
        }
        let page = plan
            .collect(
                store,
                StorageScanOptions {
                    resume_after: resume_after.clone(),
                    limit_rows: remaining_limit
                        .unwrap_or_else(|| StorageScanOptions::default().limit_rows),
                    ..StorageScanOptions::default()
                },
            )
            .await?;
        resume_after = page.value.entries.last().map(|entry| entry.key.clone());

        for entry in page.value.entries {
            let Some(bytes) = full_value(entry.value) else {
                continue;
            };
            let identity = decode_untracked_state_row_key_ref(entry.key.0.as_ref())?;
            let row = crate::untracked_state::codec::decode_payload_with_identity(
                identity,
                bytes.as_ref(),
            )?;
            if !row_matches_scan(&row, request) {
                continue;
            }
            materialized.push(crate::untracked_state::materialize_row(row, projection)?);
            if request
                .limit
                .is_some_and(|limit| materialized.len() >= limit)
            {
                break;
            }
        }

        if !page.value.has_more || resume_after.is_none() {
            break;
        }
    }
    Ok(())
}

fn scan_plans_for_request(request: &UntrackedStateScanRequest) -> Result<Vec<ScanPlan>, LixError> {
    let mut prefixes = scan_prefixes_for_filter(&request.filter)?;
    prefixes.sort();
    prefixes.dedup();
    Ok(prefixes
        .into_iter()
        .map(|prefix| {
            ScanPlan::prefix(
                UNTRACKED_STATE_ROW_SPACE,
                StoragePrefix {
                    bytes: Bytes::from(prefix),
                },
            )
        })
        .collect())
}

fn scan_prefixes_for_filter(
    filter: &crate::untracked_state::UntrackedStateFilter,
) -> Result<Vec<Vec<u8>>, LixError> {
    if filter.branch_ids.is_empty() {
        return Ok(vec![Vec::new()]);
    }

    let mut prefixes = Vec::new();
    for branch_id in &filter.branch_ids {
        if filter.schema_keys.is_empty() {
            prefixes.push(encode_untracked_branch_prefix(branch_id)?);
            continue;
        }

        for schema_key in &filter.schema_keys {
            if filter.entity_pks.is_empty() {
                prefixes.push(encode_untracked_branch_schema_prefix(
                    branch_id, schema_key,
                )?);
                continue;
            }

            for entity_pk in &filter.entity_pks {
                append_file_prefixes(
                    &mut prefixes,
                    branch_id,
                    schema_key,
                    entity_pk,
                    &filter.file_ids,
                )?;
            }
        }
    }
    Ok(prefixes)
}

fn append_file_prefixes(
    prefixes: &mut Vec<Vec<u8>>,
    branch_id: &str,
    schema_key: &str,
    entity_pk: &EntityPk,
    file_filters: &[NullableKeyFilter<String>],
) -> Result<(), LixError> {
    if file_filters.is_empty()
        || file_filters
            .iter()
            .any(|filter| matches!(filter, NullableKeyFilter::Any))
    {
        prefixes.push(encode_untracked_branch_schema_entity_prefix(
            branch_id, schema_key, entity_pk,
        )?);
        return Ok(());
    }

    for filter in file_filters {
        match filter {
            NullableKeyFilter::Null => {
                prefixes.push(encode_untracked_branch_schema_entity_file_prefix(
                    branch_id, schema_key, entity_pk, None,
                )?);
            }
            NullableKeyFilter::Value(file_id) => {
                prefixes.push(encode_untracked_branch_schema_entity_file_prefix(
                    branch_id,
                    schema_key,
                    entity_pk,
                    Some(file_id),
                )?);
            }
            NullableKeyFilter::Any => unreachable!("Any handled before exact file prefixes"),
        }
    }
    Ok(())
}

fn full_value(value: StorageProjectedValue) -> Option<Bytes> {
    match value {
        StorageProjectedValue::FullValue(bytes) => Some(bytes),
        StorageProjectedValue::KeyOnly => None,
    }
}

fn row_matches_scan(row: &UntrackedStateRow, request: &UntrackedStateScanRequest) -> bool {
    (request.filter.schema_keys.is_empty() || request.filter.schema_keys.contains(&row.schema_key))
        && (request.filter.entity_pks.is_empty()
            || request.filter.entity_pks.contains(&row.entity_pk))
        && (request.filter.branch_ids.is_empty()
            || request.filter.branch_ids.contains(&row.branch_id))
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
        branch_id: request.branch_id.clone(),
        schema_key: request.schema_key.clone(),
        entity_pk: request.entity_pk.clone(),
        file_id,
    })
}

fn encode_untracked_state_row_key(identity: &UntrackedStateIdentity) -> Result<Vec<u8>, LixError> {
    encode_untracked_state_row_key_ref(identity.as_ref())
}

pub(crate) fn encode_untracked_state_row_key_ref(
    identity: UntrackedStateIdentityRef<'_>,
) -> Result<Vec<u8>, LixError> {
    storage_codec::encode("untracked-state key", &identity)
}

pub(crate) fn decode_untracked_state_row_key_ref(
    bytes: &[u8],
) -> Result<UntrackedStateIdentity, LixError> {
    storage_codec::decode("untracked-state key", bytes)
}

fn encode_untracked_branch_prefix(branch_id: &str) -> Result<Vec<u8>, LixError> {
    storage_codec::encode(
        "untracked-state branch key prefix",
        &UntrackedBranchPrefixRef { branch_id },
    )
}

fn encode_untracked_branch_schema_prefix(
    branch_id: &str,
    schema_key: &str,
) -> Result<Vec<u8>, LixError> {
    storage_codec::encode(
        "untracked-state branch/schema key prefix",
        &UntrackedBranchSchemaPrefixRef {
            branch_id,
            schema_key,
        },
    )
}

fn encode_untracked_branch_schema_entity_prefix(
    branch_id: &str,
    schema_key: &str,
    entity_pk: &EntityPk,
) -> Result<Vec<u8>, LixError> {
    storage_codec::encode(
        "untracked-state branch/schema/entity key prefix",
        &UntrackedBranchSchemaEntityPrefixRef {
            branch_id,
            schema_key,
            entity_pk,
        },
    )
}

fn encode_untracked_branch_schema_entity_file_prefix(
    branch_id: &str,
    schema_key: &str,
    entity_pk: &EntityPk,
    file_id: Option<&str>,
) -> Result<Vec<u8>, LixError> {
    storage_codec::encode(
        "untracked-state branch/schema/entity/file key prefix",
        &UntrackedBranchSchemaEntityFilePrefixRef {
            branch_id,
            schema_key,
            entity_pk,
            file_id,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageContext;
    use crate::storage::{InMemoryStorageBackend, StorageReadOptions, StorageWriteOptions};
    use crate::untracked_state::UntrackedStateContext;

    #[test]
    fn key_roundtrips_null_file_id() {
        let identity = UntrackedStateIdentity {
            branch_id: "branch-1".to_string(),
            schema_key: "schema-1".to_string(),
            entity_pk: EntityPk::single("entity-1"),
            file_id: None,
        };
        let key = encode_untracked_state_row_key_ref(identity.as_ref()).expect("key should encode");

        assert_eq!(
            decode_untracked_state_row_key_ref(&key).expect("key should decode"),
            identity
        );
    }

    #[test]
    fn key_roundtrips_empty_file_id() {
        let identity = UntrackedStateIdentity {
            branch_id: "branch-1".to_string(),
            schema_key: "schema-1".to_string(),
            entity_pk: EntityPk::single("entity-1"),
            file_id: Some(String::new()),
        };
        let key = encode_untracked_state_row_key_ref(identity.as_ref()).expect("key should encode");

        assert_eq!(
            decode_untracked_state_row_key_ref(&key).expect("key should decode"),
            identity
        );
    }

    #[test]
    fn key_roundtrips_empty_entity_pk_part() {
        let identity = UntrackedStateIdentity {
            branch_id: "branch-1".to_string(),
            schema_key: "json_pointer".to_string(),
            entity_pk: EntityPk::single(""),
            file_id: Some("file-1".to_string()),
        };
        let key = encode_untracked_state_row_key_ref(identity.as_ref()).expect("key should encode");

        assert_eq!(
            decode_untracked_state_row_key_ref(&key).expect("key should decode"),
            identity
        );
    }

    #[test]
    fn key_roundtrips_tuple_and_unicode_identity() {
        let identity = UntrackedStateIdentity {
            branch_id: "branch-東京".to_string(),
            schema_key: "schema-1".to_string(),
            entity_pk: EntityPk::tuple(vec!["entity-1".to_string(), "ключ".to_string()])
                .expect("entity primary key should build"),
            file_id: Some("file-δ".to_string()),
        };
        let key = encode_untracked_state_row_key_ref(identity.as_ref()).expect("key should encode");

        assert_eq!(
            decode_untracked_state_row_key_ref(&key).expect("key should decode"),
            identity
        );
    }

    #[test]
    fn key_encodes_entity_as_binary_tuple_not_json_text() {
        let identity = UntrackedStateIdentity {
            branch_id: "branch-1".to_string(),
            schema_key: "schema-1".to_string(),
            entity_pk: EntityPk::tuple(vec!["entity/1".to_string(), "quote\"part".to_string()])
                .expect("entity primary key should build"),
            file_id: None,
        };
        let key = encode_untracked_state_row_key_ref(identity.as_ref()).expect("key should encode");

        assert!(
            !key.windows(2)
                .any(|window| window == br#"[""# || window == br#""]"#)
        );
        assert_eq!(
            decode_untracked_state_row_key_ref(&key).expect("key should decode"),
            identity
        );
    }

    #[test]
    fn key_decode_rejects_malformed_storage_bytes() {
        let error = decode_untracked_state_row_key_ref(b"LXUQ\x01")
            .expect_err("malformed storage key should fail");
        assert!(
            error
                .to_string()
                .contains("failed to decode untracked-state key")
        );
    }

    #[test]
    fn key_decode_rejects_trailing_bytes() {
        let identity = UntrackedStateIdentity {
            branch_id: "branch-1".to_string(),
            schema_key: "schema-1".to_string(),
            entity_pk: EntityPk::single("entity-1"),
            file_id: None,
        };
        let mut key =
            encode_untracked_state_row_key_ref(identity.as_ref()).expect("key should encode");
        key.push(0);
        let error =
            decode_untracked_state_row_key_ref(&key).expect_err("trailing key bytes should fail");
        assert!(
            error
                .to_string()
                .contains("failed to decode untracked-state key")
        );
    }

    #[test]
    fn key_decode_rejects_truncated_component() {
        let identity = UntrackedStateIdentity {
            branch_id: "branch-1".to_string(),
            schema_key: "schema-1".to_string(),
            entity_pk: EntityPk::single("entity-1"),
            file_id: None,
        };
        let mut key =
            encode_untracked_state_row_key_ref(identity.as_ref()).expect("key should encode");
        key.truncate(key.len() - 2);
        let error =
            decode_untracked_state_row_key_ref(&key).expect_err("truncated key should fail");
        assert!(
            error
                .to_string()
                .contains("failed to decode untracked-state key")
        );
    }

    #[test]
    fn key_decode_rejects_empty_entity_pk() {
        let identity = UntrackedStateIdentity {
            branch_id: "branch-1".to_string(),
            schema_key: "schema-1".to_string(),
            entity_pk: EntityPk { parts: Vec::new() },
            file_id: None,
        };
        let key = encode_untracked_state_row_key_ref(identity.as_ref())
            .expect("invalid key should encode");

        let error =
            decode_untracked_state_row_key_ref(&key).expect_err("empty entity pk should reject");

        assert!(
            error
                .message
                .contains("entity primary key decoded from storage is invalid")
        );
    }

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
            .await
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
                .await
                .expect("read should open");
            let mut reader = context.reader(read);
            reader
                .load_row(&UntrackedStateRowRequest {
                    schema_key: "lix_key_value".to_string(),
                    branch_id: "global".to_string(),
                    entity_pk: EntityPk::single("ui-tab"),
                    file_id: NullableKeyFilter::Null,
                })
                .await
        }
        .expect("load should succeed");
        assert_eq!(loaded, Some(row));
    }

    #[tokio::test]
    async fn scan_filters_by_schema_and_branch() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let context = UntrackedStateContext::new();
        write_materialized_rows_to_store(
            &context,
            &storage,
            &[
                untracked_row("global", "lix_key_value", "global-ui"),
                untracked_row("branch-a", "lix_key_value", "branch-ui"),
                untracked_row("branch-a", "other_schema", "other"),
            ],
        )
        .await;

        let rows = {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open");
            let mut reader = context.reader(read);
            reader
                .scan_rows(&UntrackedStateScanRequest {
                    filter: crate::untracked_state::UntrackedStateFilter {
                        schema_keys: vec!["lix_key_value".to_string()],
                        branch_ids: vec!["branch-a".to_string()],
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .await
        }
        .expect("scan should succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_pk, EntityPk::single("branch-ui"));
    }

    #[tokio::test]
    async fn delete_removes_row() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let context = UntrackedStateContext::new();
        let row = untracked_row("global", "lix_key_value", "ui-tab");
        let identity = UntrackedStateIdentity {
            branch_id: row.branch_id.clone(),
            schema_key: row.schema_key.clone(),
            entity_pk: row.entity_pk.clone(),
            file_id: row.file_id.clone(),
        };
        write_materialized_rows_to_store(&context, &storage, std::slice::from_ref(&row)).await;

        let mut writes = storage.new_write_set();
        let mut writer = context.writer(&mut writes);
        writer
            .stage_delete_rows(std::iter::once(identity.as_ref()))
            .expect("delete should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("writes should commit");

        let loaded = {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open");
            let mut reader = context.reader(read);
            reader
                .load_row(&UntrackedStateRowRequest {
                    schema_key: "lix_key_value".to_string(),
                    branch_id: "global".to_string(),
                    entity_pk: EntityPk::single("ui-tab"),
                    file_id: NullableKeyFilter::Null,
                })
                .await
        }
        .expect("load should succeed");
        assert_eq!(loaded, None);
    }

    #[tokio::test]
    async fn v1_layout_ignores_previous_untracked_row_space() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let context = UntrackedStateContext::new();
        let legacy_space = StorageSpace::new(StorageSpaceId(0x0001_0001), "untracked_state.row");
        let mut writes = storage.new_write_set();
        writes.put(
            legacy_space,
            StorageKey(Bytes::from_static(b"legacy-row-key")),
            StorageValue {
                bytes: Bytes::from_static(b"legacy-row-value"),
            },
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("legacy row should commit");

        let loaded = {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open");
            let mut reader = context.reader(read);
            reader
                .load_row(&UntrackedStateRowRequest {
                    schema_key: "lix_key_value".to_string(),
                    branch_id: "global".to_string(),
                    entity_pk: EntityPk::single("legacy-row-key"),
                    file_id: NullableKeyFilter::Null,
                })
                .await
        }
        .expect("load should succeed");
        assert_eq!(loaded, None);

        let rows = {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open");
            let mut reader = context.reader(read);
            reader
                .scan_rows(&UntrackedStateScanRequest {
                    filter: crate::untracked_state::UntrackedStateFilter {
                        branch_ids: vec!["global".to_string()],
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .await
        }
        .expect("scan should succeed");
        assert!(rows.is_empty());
    }

    fn untracked_row(
        branch_id: &str,
        schema_key: &str,
        entity_pk: &str,
    ) -> MaterializedUntrackedStateRow {
        MaterializedUntrackedStateRow {
            entity_pk: EntityPk::single(entity_pk),
            schema_key: schema_key.to_string(),
            file_id: None,
            snapshot_content: Some(format!("{{\"key\":\"{}\",\"value\":\"value\"}}", entity_pk)),
            metadata: None,
            deleted: false,
            created_at: "2026-01-01T00:00:00.000Z".to_string(),
            updated_at: "2026-01-01T00:00:00.000Z".to_string(),
            global: branch_id == "global",
            branch_id: branch_id.to_string(),
        }
    }
}
