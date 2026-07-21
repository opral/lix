use bytes::Bytes;

use crate::NullableKeyFilter;
use crate::changelog::ChangeId;
use crate::common::LixTimestamp;
use crate::entity_pk::EntityPk;
use crate::storage_adapter::{
    PointReadPlan, ScanPlan, StorageAdapterRead, StorageGetOptions, StorageKey, StoragePrefix,
    StorageProjectedValue, StorageScanOptions, StorageSpace, StorageSpaceId, StorageValue,
    StorageWriteSet,
};
use crate::{LixError, storage_codec};

use super::{LiveStateIndexFilter, LiveStateIndexRowRequest};

pub(crate) const LIVE_STATE_INDEX_ROW_NAMESPACE: &str = "live_state.flat_row.v1";
pub(crate) const LIVE_STATE_INDEX_ROW_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0004_0006), LIVE_STATE_INDEX_ROW_NAMESPACE);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(super) struct FlatIdentity {
    pub(super) branch_id: String,
    pub(super) schema_key: String,
    pub(super) entity_pk: EntityPk,
    #[musli(with = storage_codec::option)]
    pub(super) file_id: Option<String>,
}

#[derive(Debug, Clone, Copy, musli::Encode)]
#[musli(packed)]
struct FlatIdentityRef<'a> {
    branch_id: &'a str,
    schema_key: &'a str,
    entity_pk: &'a EntityPk,
    #[musli(with = storage_codec::option)]
    file_id: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(super) struct FlatValue {
    pub(super) change_id: ChangeId,
    pub(super) created_at: LixTimestamp,
    pub(super) updated_at: LixTimestamp,
}

#[derive(musli::Encode)]
#[musli(packed)]
struct BranchPrefixRef<'a> {
    branch_id: &'a str,
}

#[derive(musli::Encode)]
#[musli(packed)]
struct BranchSchemaPrefixRef<'a> {
    branch_id: &'a str,
    schema_key: &'a str,
}

#[derive(musli::Encode)]
#[musli(packed)]
struct BranchSchemaEntityPrefixRef<'a> {
    branch_id: &'a str,
    schema_key: &'a str,
    entity_pk: &'a EntityPk,
}

#[derive(musli::Encode)]
#[musli(packed)]
struct BranchSchemaEntityFilePrefixRef<'a> {
    branch_id: &'a str,
    schema_key: &'a str,
    entity_pk: &'a EntityPk,
    #[musli(with = storage_codec::option)]
    file_id: Option<&'a str>,
}

impl FlatIdentity {
    pub(super) fn from_request(request: &LiveStateIndexRowRequest) -> Self {
        Self {
            branch_id: request.branch_id.clone(),
            schema_key: request.schema_key.clone(),
            entity_pk: request.entity_pk.clone(),
            file_id: request.file_id.clone(),
        }
    }

    fn as_ref(&self) -> FlatIdentityRef<'_> {
        FlatIdentityRef {
            branch_id: &self.branch_id,
            schema_key: &self.schema_key,
            entity_pk: &self.entity_pk,
            file_id: self.file_id.as_deref(),
        }
    }
}

#[cfg(test)]
pub(super) async fn load_value(
    store: &(impl StorageAdapterRead + ?Sized),
    identity: &FlatIdentity,
) -> Result<Option<FlatValue>, LixError> {
    let result = PointReadPlan::new(
        LIVE_STATE_INDEX_ROW_SPACE,
        &[StorageKey(Bytes::from(encode_key(identity)?))],
    )
    .materialize(store, StorageGetOptions::default())
    .await?;
    result
        .value
        .into_iter()
        .next()
        .flatten()
        .map(decode_projected_value)
        .transpose()
}

pub(super) async fn load_values(
    store: &(impl StorageAdapterRead + ?Sized),
    identities: &[FlatIdentity],
) -> Result<Vec<Option<FlatValue>>, LixError> {
    if identities.is_empty() {
        return Ok(Vec::new());
    }
    let keys = identities
        .iter()
        .map(|identity| encode_key(identity).map(|key| StorageKey(Bytes::from(key))))
        .collect::<Result<Vec<_>, _>>()?;
    let result = PointReadPlan::new(LIVE_STATE_INDEX_ROW_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?;
    result
        .value
        .into_iter()
        .map(|value| value.map(decode_projected_value).transpose())
        .collect()
}

pub(super) async fn scan_values(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    filter: &LiveStateIndexFilter,
    limit: Option<usize>,
) -> Result<Vec<(FlatIdentity, FlatValue)>, LixError> {
    if let Some(identities) = exact_filter_identities(branch_id, filter) {
        let values = load_values(store, &identities).await?;
        return Ok(identities
            .into_iter()
            .zip(values)
            .filter_map(|(identity, value)| value.map(|value| (identity, value)))
            .take(limit.unwrap_or(usize::MAX))
            .collect());
    }

    let mut prefixes = scan_prefixes(branch_id, filter)?;
    prefixes.sort();
    prefixes.dedup();
    let mut rows = Vec::new();
    for prefix in prefixes {
        let plan = ScanPlan::prefix(
            LIVE_STATE_INDEX_ROW_SPACE,
            StoragePrefix {
                bytes: Bytes::from(prefix),
            },
        );
        let mut resume_after = None;
        loop {
            let remaining = limit.map(|limit| limit.saturating_sub(rows.len()));
            if matches!(remaining, Some(0)) {
                return Ok(rows);
            }
            let page = plan
                .collect(
                    store,
                    StorageScanOptions {
                        resume_after: resume_after.clone(),
                        limit_rows: remaining
                            .unwrap_or_else(|| StorageScanOptions::default().limit_rows),
                        ..StorageScanOptions::default()
                    },
                )
                .await?;
            resume_after = page.value.entries.last().map(|entry| entry.key.clone());
            for entry in page.value.entries {
                let identity = decode_key(entry.key.0.as_ref())?;
                if !matches_filter(&identity, filter) {
                    continue;
                }
                rows.push((identity, decode_projected_value(entry.value)?));
                if limit.is_some_and(|limit| rows.len() >= limit) {
                    return Ok(rows);
                }
            }
            if !page.value.has_more || resume_after.is_none() {
                break;
            }
        }
    }
    Ok(rows)
}

/// Returns concrete flat identities when every filter dimension resolves to
/// exact keys. This lets multi-row validation use one batched point read
/// instead of one prefix scan per entity.
fn exact_filter_identities(
    branch_id: &str,
    filter: &LiveStateIndexFilter,
) -> Option<Vec<FlatIdentity>> {
    if filter.schema_keys.is_empty()
        || filter.entity_pks.is_empty()
        || filter.file_ids.is_empty()
        || filter
            .file_ids
            .iter()
            .any(|filter| matches!(filter, NullableKeyFilter::Any))
    {
        return None;
    }

    let mut identities = Vec::with_capacity(
        filter.schema_keys.len() * filter.entity_pks.len() * filter.file_ids.len(),
    );
    for schema_key in &filter.schema_keys {
        for entity_pk in &filter.entity_pks {
            for file_id in &filter.file_ids {
                let file_id = match file_id {
                    NullableKeyFilter::Null => None,
                    NullableKeyFilter::Value(value) => Some(value.clone()),
                    NullableKeyFilter::Any => unreachable!("Any rejected above"),
                };
                identities.push(FlatIdentity {
                    branch_id: branch_id.to_string(),
                    schema_key: schema_key.clone(),
                    entity_pk: entity_pk.clone(),
                    file_id,
                });
            }
        }
    }
    identities.sort();
    identities.dedup();
    Some(identities)
}

pub(super) fn stage_put(
    writes: &mut StorageWriteSet,
    identity: &FlatIdentity,
    value: &FlatValue,
) -> Result<(), LixError> {
    writes.put(
        LIVE_STATE_INDEX_ROW_SPACE,
        StorageKey(Bytes::from(encode_key(identity)?)),
        StorageValue {
            bytes: Bytes::from(storage_codec::encode("flat live-state value", value)?),
        },
    );
    Ok(())
}

pub(super) fn stage_delete(
    writes: &mut StorageWriteSet,
    identity: &FlatIdentity,
) -> Result<(), LixError> {
    writes.delete(
        LIVE_STATE_INDEX_ROW_SPACE,
        StorageKey(Bytes::from(encode_key(identity)?)),
    );
    Ok(())
}

fn encode_key(identity: &FlatIdentity) -> Result<Vec<u8>, LixError> {
    storage_codec::encode("flat live-state key", &identity.as_ref())
}

fn decode_key(bytes: &[u8]) -> Result<FlatIdentity, LixError> {
    storage_codec::decode("flat live-state key", bytes)
}

fn decode_projected_value(value: StorageProjectedValue) -> Result<FlatValue, LixError> {
    let StorageProjectedValue::FullValue(bytes) = value else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "flat live-state read unexpectedly omitted its value",
        ));
    };
    storage_codec::decode("flat live-state value", &bytes)
}

fn scan_prefixes(branch_id: &str, filter: &LiveStateIndexFilter) -> Result<Vec<Vec<u8>>, LixError> {
    if filter.schema_keys.is_empty() {
        return Ok(vec![storage_codec::encode(
            "flat live-state branch prefix",
            &BranchPrefixRef { branch_id },
        )?]);
    }
    let mut prefixes = Vec::new();
    for schema_key in &filter.schema_keys {
        if filter.entity_pks.is_empty() {
            prefixes.push(storage_codec::encode(
                "flat live-state branch/schema prefix",
                &BranchSchemaPrefixRef {
                    branch_id,
                    schema_key,
                },
            )?);
            continue;
        }
        for entity_pk in &filter.entity_pks {
            if filter.file_ids.is_empty()
                || filter
                    .file_ids
                    .iter()
                    .any(|filter| matches!(filter, NullableKeyFilter::Any))
            {
                prefixes.push(storage_codec::encode(
                    "flat live-state branch/schema/entity prefix",
                    &BranchSchemaEntityPrefixRef {
                        branch_id,
                        schema_key,
                        entity_pk,
                    },
                )?);
                continue;
            }
            for file_id in &filter.file_ids {
                let file_id = match file_id {
                    NullableKeyFilter::Null => None,
                    NullableKeyFilter::Value(value) => Some(value.as_str()),
                    NullableKeyFilter::Any => unreachable!("Any handled above"),
                };
                prefixes.push(storage_codec::encode(
                    "flat live-state branch/schema/entity/file prefix",
                    &BranchSchemaEntityFilePrefixRef {
                        branch_id,
                        schema_key,
                        entity_pk,
                        file_id,
                    },
                )?);
            }
        }
    }
    Ok(prefixes)
}

fn matches_filter(identity: &FlatIdentity, filter: &LiveStateIndexFilter) -> bool {
    (filter.schema_keys.is_empty() || filter.schema_keys.contains(&identity.schema_key))
        && (filter.entity_pks.is_empty() || filter.entity_pks.contains(&identity.entity_pk))
        && (filter.file_ids.is_empty()
            || filter.file_ids.iter().any(|filter| match filter {
                NullableKeyFilter::Any => true,
                NullableKeyFilter::Null => identity.file_id.is_none(),
                NullableKeyFilter::Value(value) => identity.file_id.as_ref() == Some(value),
            }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};

    fn identity(
        branch_id: &str,
        schema_key: &str,
        parts: &[&str],
        file_id: Option<&str>,
    ) -> FlatIdentity {
        FlatIdentity {
            branch_id: branch_id.to_string(),
            schema_key: schema_key.to_string(),
            entity_pk: EntityPk::tuple(parts.iter().map(|part| (*part).to_string()).collect())
                .expect("test entity pk"),
            file_id: file_id.map(str::to_string),
        }
    }

    fn value(label: &str, timestamp: &str) -> FlatValue {
        let timestamp = LixTimestamp::expect_parse("test timestamp", timestamp);
        FlatValue {
            change_id: ChangeId::for_test_label(label),
            created_at: timestamp,
            updated_at: timestamp,
        }
    }

    #[test]
    fn key_roundtrips_and_all_structural_prefixes_match() {
        let identity = identity(
            "brånch/東京",
            "schema.ü",
            &["first", "二番"],
            Some("file/ß"),
        );
        let key = encode_key(&identity).expect("key should encode");
        assert_eq!(decode_key(&key).expect("key should decode"), identity);

        let prefixes = [
            storage_codec::encode(
                "branch prefix",
                &BranchPrefixRef {
                    branch_id: &identity.branch_id,
                },
            )
            .expect("branch prefix"),
            storage_codec::encode(
                "branch/schema prefix",
                &BranchSchemaPrefixRef {
                    branch_id: &identity.branch_id,
                    schema_key: &identity.schema_key,
                },
            )
            .expect("branch/schema prefix"),
            storage_codec::encode(
                "branch/schema/entity prefix",
                &BranchSchemaEntityPrefixRef {
                    branch_id: &identity.branch_id,
                    schema_key: &identity.schema_key,
                    entity_pk: &identity.entity_pk,
                },
            )
            .expect("branch/schema/entity prefix"),
            storage_codec::encode(
                "branch/schema/entity/file prefix",
                &BranchSchemaEntityFilePrefixRef {
                    branch_id: &identity.branch_id,
                    schema_key: &identity.schema_key,
                    entity_pk: &identity.entity_pk,
                    file_id: identity.file_id.as_deref(),
                },
            )
            .expect("branch/schema/entity/file prefix"),
        ];
        for prefix in prefixes {
            assert!(key.starts_with(&prefix));
        }
    }

    #[tokio::test]
    async fn scans_isolate_branches_and_nullable_file_filters_then_delete_physically() {
        let storage = StorageAdapter::new(Memory::new());
        let rows = [
            (
                identity("branch-a", "schema", &["one"], None),
                value("a-null", "2026-01-01T00:00:00Z"),
            ),
            (
                identity("branch-a", "schema", &["one"], Some("file-a")),
                value("a-file", "2026-01-01T00:00:01Z"),
            ),
            (
                identity("branch-a", "other", &["二"], None),
                value("a-other", "2026-01-01T00:00:02Z"),
            ),
            (
                identity("branch-b", "schema", &["one"], None),
                value("b-null", "2026-01-01T00:00:03Z"),
            ),
        ];
        let mut writes = StorageWriteSet::new();
        for (identity, value) in &rows {
            stage_put(&mut writes, identity, value).expect("row should stage");
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("rows should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");

        let branch_rows = scan_values(&read, "branch-a", &LiveStateIndexFilter::default(), None)
            .await
            .expect("branch scan");
        assert_eq!(branch_rows.len(), 3);
        assert!(
            branch_rows
                .iter()
                .all(|(row, _)| row.branch_id == "branch-a")
        );

        for (filter, expected_file_id) in [
            (NullableKeyFilter::Null, None),
            (
                NullableKeyFilter::Value("file-a".to_string()),
                Some("file-a"),
            ),
        ] {
            let matches = scan_values(
                &read,
                "branch-a",
                &LiveStateIndexFilter {
                    schema_keys: vec!["schema".to_string()],
                    entity_pks: vec![EntityPk::single("one")],
                    file_ids: vec![filter],
                },
                None,
            )
            .await
            .expect("filtered scan");
            assert_eq!(matches.len(), 1);
            assert_eq!(matches[0].0.file_id.as_deref(), expected_file_id);
        }

        let any = scan_values(
            &read,
            "branch-a",
            &LiveStateIndexFilter {
                schema_keys: vec!["schema".to_string()],
                entity_pks: vec![EntityPk::single("one")],
                file_ids: vec![NullableKeyFilter::Any],
            },
            None,
        )
        .await
        .expect("any-file scan");
        assert_eq!(any.len(), 2);
        drop(read);

        let mut writes = StorageWriteSet::new();
        stage_delete(&mut writes, &rows[0].0).expect("delete should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("delete should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should reopen");
        assert_eq!(load_value(&read, &rows[0].0).await.expect("load"), None);
    }
}
