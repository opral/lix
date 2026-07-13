use bytes::Bytes;

use crate::NullableKeyFilter;
use crate::changelog::ChangeId;
use crate::common::LixTimestamp;
use crate::entity_pk::EntityPk;
use crate::storage::{
    PointReadPlan, ScanPlan, StorageGetOptions, StorageKey, StoragePrefix, StorageProjectedValue,
    StorageRead, StorageScanOptions, StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet,
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

pub(super) async fn load_value(
    store: &(impl StorageRead + ?Sized),
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
    store: &(impl StorageRead + ?Sized),
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
    store: &(impl StorageRead + ?Sized),
    branch_id: &str,
    filter: &LiveStateIndexFilter,
    limit: Option<usize>,
) -> Result<Vec<(FlatIdentity, FlatValue)>, LixError> {
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
