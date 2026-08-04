//! Branch-stable physical state for history-free rows.
//!
//! Untracked rows do not participate in commit history, merge, diff, working
//! diff, or generation rotation. Their storage identity is therefore exactly
//! `(branch, schema, entity, file)` and a delete removes that key physically.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;

use crate::NullableKeyFilter;
use crate::common::{LixTimestamp, SharedStr};
use crate::entity_pk::EntityPk;
use crate::json_store::{
    JsonLoadRequestRef, JsonReadScopeRef, JsonRef, JsonSlotRef, JsonStoreContext,
};
use crate::live_state::{
    CurrentStateDeltaRef, LiveStateExactBatchRequest, LiveStateScanRequest,
    MaterializedLiveStateBatch, MaterializedLiveStateBatchBuilder, MaterializedLiveStateExactBatch,
    MaterializedLiveStateRow,
};
use crate::storage_adapter::{
    PointReadPlan, ScanPlan, StorageAdapterRead, StorageGetOptions, StorageKey, StoragePrefix,
    StorageProjectedValue, StorageScanOptions, StorageSpace, StorageSpaceId, StorageValue,
    StorageWriteSet,
};
use crate::{GLOBAL_BRANCH_ID, LixError};

pub(crate) const UNTRACKED_ROW_SPACE: StorageSpace =
    StorageSpace::mutable(StorageSpaceId(0x0004_0033), "live_state.untracked_row.v1");
pub(crate) const UNTRACKED_FILE_FANOUT_SPACE: StorageSpace = StorageSpace::mutable(
    StorageSpaceId(0x0004_0034),
    "live_state.untracked_file_fanout.v1",
);

const VALUE_VERSION: u8 = 1;
const FLAG_GLOBAL: u8 = 1;
const SLOT_NONE: u8 = 0;
const SLOT_REF: u8 = 1;
const SLOT_INLINE: u8 = 2;
const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";

pub(crate) async fn stage_untracked_deltas(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    branch_id: &str,
    deltas: &[CurrentStateDeltaRef<'_>],
    known_absent: &[bool],
) -> Result<(), LixError> {
    if known_absent.len() != deltas.len() {
        return Err(codec_error(
            "untracked absence certificates do not align with deltas",
        ));
    }
    let mut mutations = BTreeMap::<StorageKey, Option<StorageValue>>::new();
    let mut retired_refs = BTreeSet::new();
    let untracked = deltas
        .iter()
        .zip(known_absent)
        .filter(|(delta, absent)| delta.untracked && !**absent)
        .map(|(delta, _)| delta)
        .collect::<Vec<_>>();
    let mut keys = untracked
        .iter()
        .map(|delta| {
            encode_key(branch_id, delta.schema_key, delta.file_id, delta.entity_pk)
                .map(|key| StorageKey(Bytes::from(key)))
        })
        .collect::<Result<Vec<_>, _>>()?;
    keys.sort_unstable();
    keys.dedup();
    let previous = PointReadPlan::from_unique_keys(UNTRACKED_ROW_SPACE, keys.clone())
        .materialize(store, StorageGetOptions::default())
        .await?
        .value;
    let mut previous_keys = BTreeSet::new();
    let mut previous_created_at = BTreeMap::new();
    for (key, value) in keys.into_iter().zip(previous) {
        let Some(StorageProjectedValue::FullValue(value)) = value else {
            if value.is_some() {
                return Err(codec_error("untracked point read omitted its row value"));
            }
            continue;
        };
        let decoded = decode_value(value)?;
        collect_value_refs(&decoded, &mut retired_refs);
        previous_created_at.insert(key.clone(), decoded.created_at);
        previous_keys.insert(key);
    }
    for delta in deltas {
        if !delta.untracked {
            continue;
        }
        if delta.change_id.is_some() || delta.commit_id.is_some() {
            return Err(codec_error(
                "dedicated untracked row carried commit identity",
            ));
        }
        let key = StorageKey(Bytes::from(encode_key(
            branch_id,
            delta.schema_key,
            delta.file_id,
            delta.entity_pk,
        )?));
        if delta.deleted {
            mutations.insert(key, None);
        } else {
            let created_at = previous_created_at
                .get(&key)
                .copied()
                .unwrap_or(delta.created_at);
            mutations.insert(
                key,
                Some(StorageValue {
                    bytes: Bytes::from(encode_value(branch_id, *delta, created_at)?),
                }),
            );
        }
    }

    let deleted_file_ids = deltas
        .iter()
        .filter(|delta| delta.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY && delta.deleted)
        .map(|delta| delta.entity_pk.as_single_string_owned())
        .collect::<Result<BTreeSet<_>, _>>()?;
    if !deleted_file_ids.is_empty() {
        for (key, value) in scan_raw_prefix(store, &branch_prefix(branch_id)?).await? {
            let identity = decode_key(&key.0)?;
            if identity
                .file_id
                .as_ref()
                .is_some_and(|file_id| deleted_file_ids.contains(file_id))
            {
                collect_value_refs(&decode_value(value)?, &mut retired_refs);
                previous_keys.insert(key.clone());
                mutations.insert(key, None);
            }
        }
        for (key, value) in &mut mutations {
            let identity = decode_key(&key.0)?;
            if identity
                .file_id
                .as_ref()
                .is_some_and(|file_id| deleted_file_ids.contains(file_id))
            {
                if let Some(value) = value.take() {
                    collect_value_refs(&decode_value(value.bytes)?, &mut retired_refs);
                }
            }
        }
    }

    if !deleted_file_ids.is_empty()
        || deltas
            .iter()
            .any(|delta| delta.untracked && delta.file_id.is_some())
    {
        stage_file_fanout_changes(store, writes, &previous_keys, &mutations).await?;
    }

    for (key, value) in mutations {
        match value {
            Some(value) => writes.put(UNTRACKED_ROW_SPACE, key, value),
            None => writes.delete(UNTRACKED_ROW_SPACE, key),
        }
    }
    crate::json_store::JsonStoreWriter::stage_untracked_reclaim_candidates(
        writes,
        retired_refs.into_iter().map(JsonRef::from_hash_bytes),
    );
    Ok(())
}

async fn stage_file_fanout_changes(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    previous_keys: &BTreeSet<StorageKey>,
    mutations: &BTreeMap<StorageKey, Option<StorageValue>>,
) -> Result<(), LixError> {
    let mut changes = BTreeMap::<StorageKey, (BTreeSet<String>, BTreeSet<String>)>::new();
    for (key, value) in mutations {
        let identity = decode_key(&key.0)?;
        let Some(file_id) = identity.file_id else {
            continue;
        };
        let existed = previous_keys.contains(key);
        let exists = value.is_some();
        if existed == exists {
            continue;
        }
        let fanout_key = StorageKey(Bytes::from(entity_prefix(
            &identity.branch_id,
            &identity.schema_key,
            &identity.entity_pk,
        )?));
        let (adds, removes) = changes.entry(fanout_key).or_default();
        if exists {
            adds.insert(file_id);
        } else {
            removes.insert(file_id);
        }
    }
    if changes.is_empty() {
        return Ok(());
    }

    let keys = changes.keys().cloned().collect::<Vec<_>>();
    let previous = PointReadPlan::from_unique_keys(UNTRACKED_FILE_FANOUT_SPACE, keys.clone())
        .materialize(store, StorageGetOptions::default())
        .await?
        .value;
    for ((key, (adds, removes)), previous) in changes.into_iter().zip(previous) {
        let mut file_ids = match previous {
            None => BTreeSet::new(),
            Some(StorageProjectedValue::FullValue(value)) => decode_file_fanout(&value)?,
            Some(StorageProjectedValue::KeyOnly) => {
                return Err(codec_error("untracked fanout read omitted its value"));
            }
        };
        for file_id in removes {
            file_ids.remove(&file_id);
        }
        file_ids.extend(adds);
        if file_ids.is_empty() {
            writes.delete(UNTRACKED_FILE_FANOUT_SPACE, key);
        } else {
            writes.put(
                UNTRACKED_FILE_FANOUT_SPACE,
                key,
                StorageValue {
                    bytes: Bytes::from(encode_file_fanout(&file_ids)?),
                },
            );
        }
    }
    Ok(())
}

fn encode_file_fanout(file_ids: &BTreeSet<String>) -> Result<Vec<u8>, LixError> {
    let count = u32::try_from(file_ids.len())
        .map_err(|_| codec_error("untracked file fanout has too many members"))?;
    let mut out = Vec::new();
    out.extend_from_slice(&count.to_be_bytes());
    for file_id in file_ids {
        push_text(&mut out, file_id)?;
    }
    Ok(out)
}

fn decode_file_fanout(bytes: &Bytes) -> Result<BTreeSet<String>, LixError> {
    let mut offset = 0;
    let count = u32::from_be_bytes(
        take(bytes, &mut offset, 4, "fanout count")?
            .try_into()
            .expect("four bytes"),
    );
    let mut file_ids = BTreeSet::new();
    for _ in 0..count {
        if !file_ids.insert(read_text(bytes, &mut offset, "fanout file")?) {
            return Err(codec_error("untracked file fanout contains a duplicate"));
        }
    }
    if offset != bytes.len() {
        return Err(codec_error("untracked file fanout has trailing bytes"));
    }
    Ok(file_ids)
}

async fn scan_raw_prefix(
    store: &(impl StorageAdapterRead + ?Sized),
    prefix: &[u8],
) -> Result<Vec<(StorageKey, Bytes)>, LixError> {
    let plan = ScanPlan::prefix(
        UNTRACKED_ROW_SPACE,
        StoragePrefix {
            bytes: Bytes::copy_from_slice(prefix),
        },
    );
    let mut rows = Vec::new();
    let mut resume_after = None;
    loop {
        let page = plan
            .collect(
                store,
                StorageScanOptions {
                    resume_after: resume_after.clone(),
                    ..StorageScanOptions::default()
                },
            )
            .await?;
        resume_after = page.value.entries.last().map(|entry| entry.key.clone());
        for entry in page.value.entries {
            let StorageProjectedValue::FullValue(value) = entry.value else {
                return Err(codec_error("untracked scan omitted its row value"));
            };
            rows.push((entry.key, value));
        }
        if !page.value.has_more || resume_after.is_none() {
            break;
        }
    }
    Ok(rows)
}

pub(crate) async fn scan_untracked_batch(
    store: &(impl StorageAdapterRead + ?Sized),
    request: &LiveStateScanRequest,
    branch_ids: &[String],
) -> Result<MaterializedLiveStateBatch, LixError> {
    if matches!(
        request.filter.rows,
        crate::live_state::LiveStateRowFilter::None
    ) || request.filter.untracked == Some(false)
    {
        return Ok(MaterializedLiveStateBatch::default());
    }
    if !request.filter.schema_keys.is_empty() && !request.filter.entity_pks.is_empty() {
        if let Some(file_ids) = exact_file_filters(&request.filter.file_ids) {
            return load_untracked_points(
                store,
                request,
                branch_ids,
                &request.filter.schema_keys,
                &request.filter.entity_pks,
                &file_ids,
            )
            .await;
        }
        return load_untracked_entities(store, request, branch_ids).await;
    }
    let mut decoded = Vec::new();
    for branch_id in branch_ids {
        if request.filter.schema_keys.is_empty() {
            scan_prefix(store, &branch_prefix(branch_id)?, request, &mut decoded).await?;
        } else {
            for schema_key in &request.filter.schema_keys {
                scan_prefix(
                    store,
                    &schema_prefix(branch_id, schema_key)?,
                    request,
                    &mut decoded,
                )
                .await?;
            }
        }
    }
    materialize_rows(store, decoded).await
}

async fn load_untracked_entities(
    store: &(impl StorageAdapterRead + ?Sized),
    request: &LiveStateScanRequest,
    branch_ids: &[String],
) -> Result<MaterializedLiveStateBatch, LixError> {
    let mut lookups = BTreeMap::new();
    for branch_id in branch_ids {
        for schema_key in &request.filter.schema_keys {
            for entity_pk in &request.filter.entity_pks {
                let fanout_key = StorageKey(Bytes::from(entity_prefix(
                    branch_id, schema_key, entity_pk,
                )?));
                lookups
                    .entry(fanout_key)
                    .or_insert_with(|| (branch_id.clone(), schema_key.clone(), entity_pk.clone()));
            }
        }
    }
    let fanout_keys = lookups.keys().cloned().collect::<Vec<_>>();
    let null_keys = lookups
        .values()
        .map(|(branch_id, schema_key, entity_pk)| {
            encode_key(branch_id, schema_key, None, entity_pk)
                .map(|key| StorageKey(Bytes::from(key)))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let null_read = PointReadPlan::from_unique_keys(UNTRACKED_ROW_SPACE, null_keys.clone());
    let fanout_read = PointReadPlan::from_unique_keys(UNTRACKED_FILE_FANOUT_SPACE, fanout_keys);
    let (null_values, fanouts) = tokio::try_join!(
        async {
            Ok::<_, LixError>(
                null_read
                    .materialize(store, StorageGetOptions::default())
                    .await?
                    .value,
            )
        },
        async {
            Ok::<_, LixError>(
                fanout_read
                    .materialize(store, StorageGetOptions::default())
                    .await?
                    .value,
            )
        }
    )?;

    let mut decoded = Vec::new();
    for (key, value) in null_keys.into_iter().zip(null_values) {
        let Some(StorageProjectedValue::FullValue(value)) = value else {
            if value.is_some() {
                return Err(codec_error("untracked null-row read omitted its value"));
            }
            continue;
        };
        let identity = decode_key(&key.0)?;
        decoded.push(DecodedRow {
            branch_id: identity.branch_id,
            schema_key: identity.schema_key,
            file_id: None,
            entity_pk: identity.entity_pk,
            value: decode_value(value)?,
        });
    }

    let mut variant_keys = Vec::new();
    for ((_, (branch_id, schema_key, entity_pk)), fanout) in lookups.into_iter().zip(fanouts) {
        let Some(StorageProjectedValue::FullValue(value)) = fanout else {
            if fanout.is_some() {
                return Err(codec_error("untracked fanout read omitted its value"));
            }
            continue;
        };
        for file_id in decode_file_fanout(&value)? {
            variant_keys.push(StorageKey(Bytes::from(encode_key(
                &branch_id,
                &schema_key,
                Some(&file_id),
                &entity_pk,
            )?)));
        }
    }
    variant_keys.sort_unstable();
    variant_keys.dedup();
    if variant_keys.is_empty() {
        return materialize_rows(store, decoded).await;
    }
    let variant_values = PointReadPlan::from_unique_keys(UNTRACKED_ROW_SPACE, variant_keys.clone())
        .materialize(store, StorageGetOptions::default())
        .await?
        .value;
    for (key, value) in variant_keys.into_iter().zip(variant_values) {
        let Some(StorageProjectedValue::FullValue(value)) = value else {
            return Err(codec_error(
                "untracked fanout points at a missing row value",
            ));
        };
        let identity = decode_key(&key.0)?;
        if matches_filter(&identity, request) {
            decoded.push(DecodedRow {
                branch_id: identity.branch_id,
                schema_key: identity.schema_key,
                file_id: identity.file_id,
                entity_pk: identity.entity_pk,
                value: decode_value(value)?,
            });
        }
    }
    materialize_rows(store, decoded).await
}

fn exact_file_filters(filters: &[NullableKeyFilter<String>]) -> Option<Vec<Option<&str>>> {
    if filters.is_empty()
        || filters
            .iter()
            .any(|filter| matches!(filter, NullableKeyFilter::Any))
    {
        return None;
    }
    Some(
        filters
            .iter()
            .map(|filter| match filter {
                NullableKeyFilter::Null => None,
                NullableKeyFilter::Value(value) => Some(value.as_str()),
                NullableKeyFilter::Any => unreachable!("Any was rejected above"),
            })
            .collect(),
    )
}

async fn load_untracked_points(
    store: &(impl StorageAdapterRead + ?Sized),
    request: &LiveStateScanRequest,
    branch_ids: &[String],
    schema_keys: &[String],
    entity_pks: &[EntityPk],
    file_ids: &[Option<&str>],
) -> Result<MaterializedLiveStateBatch, LixError> {
    let mut keys = Vec::new();
    for branch_id in branch_ids {
        for schema_key in schema_keys {
            for file_id in file_ids {
                for entity_pk in entity_pks {
                    keys.push(StorageKey(Bytes::from(encode_key(
                        branch_id, schema_key, *file_id, entity_pk,
                    )?)));
                }
            }
        }
    }
    keys.sort_unstable();
    keys.dedup();
    let values = PointReadPlan::from_unique_keys(UNTRACKED_ROW_SPACE, keys.clone())
        .materialize(store, StorageGetOptions::default())
        .await?
        .value;
    let mut decoded = Vec::new();
    for (key, value) in keys.into_iter().zip(values) {
        let Some(StorageProjectedValue::FullValue(value)) = value else {
            continue;
        };
        let identity = decode_key(&key.0)?;
        if matches_filter(&identity, request) {
            decoded.push(DecodedRow {
                branch_id: identity.branch_id,
                schema_key: identity.schema_key,
                file_id: identity.file_id,
                entity_pk: identity.entity_pk,
                value: decode_value(value)?,
            });
        }
    }
    materialize_rows(store, decoded).await
}

pub(crate) async fn load_untracked_exact_batch(
    store: &(impl StorageAdapterRead + ?Sized),
    request: &LiveStateExactBatchRequest,
    visible_branch_ids: &BTreeSet<String>,
) -> Result<MaterializedLiveStateExactBatch, LixError> {
    let mut keys = Vec::with_capacity(request.rows.len().saturating_mul(2));
    let mut requested_key_pairs = Vec::with_capacity(request.rows.len());
    for row in &request.rows {
        if !visible_branch_ids.contains(&row.branch_id) {
            requested_key_pairs.push(None);
            continue;
        }
        let branch = StorageKey(Bytes::from(encode_key(
            &row.branch_id,
            &row.schema_key,
            row.file_id.as_deref(),
            &row.entity_pk,
        )?));
        let branch_index = keys.len();
        keys.push(branch);
        let global_index = if row.branch_id == GLOBAL_BRANCH_ID {
            branch_index
        } else {
            let index = keys.len();
            keys.push(StorageKey(Bytes::from(encode_key(
                GLOBAL_BRANCH_ID,
                &row.schema_key,
                row.file_id.as_deref(),
                &row.entity_pk,
            )?)));
            index
        };
        requested_key_pairs.push(Some((branch_index, global_index)));
    }
    let values = PointReadPlan::new(UNTRACKED_ROW_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?
        .value;
    let mut decoded = Vec::new();
    let mut selected = Vec::with_capacity(request.rows.len());
    for (requested, pair) in request.rows.iter().zip(requested_key_pairs) {
        let Some((branch_index, global_index)) = pair else {
            selected.push(None);
            continue;
        };
        let chosen = values[branch_index]
            .as_ref()
            .or_else(|| values[global_index].as_ref());
        let Some(StorageProjectedValue::FullValue(value)) = chosen else {
            selected.push(None);
            continue;
        };
        let (branch_id, branch_override) = if values[branch_index].is_some() {
            (requested.branch_id.clone(), None)
        } else {
            (
                GLOBAL_BRANCH_ID.to_owned(),
                Some(requested.branch_id.clone()),
            )
        };
        let index = decoded.len();
        decoded.push(DecodedRow {
            branch_id,
            schema_key: requested.schema_key.clone(),
            file_id: requested.file_id.clone(),
            entity_pk: requested.entity_pk.clone(),
            value: decode_value(value.clone())?,
        });
        selected.push(Some((index, branch_override)));
    }
    let rows = materialize_rows(store, decoded).await?;
    let mut builder = MaterializedLiveStateBatchBuilder::with_capacity(rows.len());
    let mut slots = Vec::with_capacity(selected.len());
    for selection in selected {
        slots.push(selection.map(|(index, branch_override)| {
            let ordinal = u32::try_from(builder.len()).expect("untracked exact batch exceeds u32");
            builder.push_ref(rows.row(index), branch_override.as_deref());
            ordinal
        }));
    }
    MaterializedLiveStateExactBatch::new(builder.finish(), slots)
}

pub(crate) async fn untracked_json_refs(
    store: &(impl StorageAdapterRead + ?Sized),
) -> Result<Vec<JsonRef>, LixError> {
    let mut refs = BTreeSet::new();
    for (_, value) in scan_raw_prefix(store, &[]).await? {
        collect_value_refs(&decode_value(value)?, &mut refs);
    }
    Ok(refs.into_iter().map(JsonRef::from_hash_bytes).collect())
}

async fn scan_prefix(
    store: &(impl StorageAdapterRead + ?Sized),
    prefix: &[u8],
    request: &LiveStateScanRequest,
    decoded: &mut Vec<DecodedRow>,
) -> Result<(), LixError> {
    let plan = ScanPlan::prefix(
        UNTRACKED_ROW_SPACE,
        StoragePrefix {
            bytes: Bytes::copy_from_slice(prefix),
        },
    );
    let mut resume_after = None;
    loop {
        let page = plan
            .collect(
                store,
                StorageScanOptions {
                    resume_after: resume_after.clone(),
                    ..StorageScanOptions::default()
                },
            )
            .await?;
        resume_after = page.value.entries.last().map(|entry| entry.key.clone());
        for entry in page.value.entries {
            let identity = decode_key(&entry.key.0)?;
            if !matches_filter(&identity, request) {
                continue;
            }
            let StorageProjectedValue::FullValue(value) = entry.value else {
                return Err(codec_error("untracked scan omitted its row value"));
            };
            decoded.push(DecodedRow {
                branch_id: identity.branch_id,
                schema_key: identity.schema_key,
                file_id: identity.file_id,
                entity_pk: identity.entity_pk,
                value: decode_value(value)?,
            });
        }
        if !page.value.has_more {
            break;
        }
    }
    Ok(())
}

fn matches_filter(identity: &DecodedIdentity, request: &LiveStateScanRequest) -> bool {
    (request.filter.entity_pks.is_empty()
        || request.filter.entity_pks.contains(&identity.entity_pk))
        && (request.filter.file_ids.is_empty()
            || request.filter.file_ids.iter().any(|filter| match filter {
                NullableKeyFilter::Any => true,
                NullableKeyFilter::Null => identity.file_id.is_none(),
                NullableKeyFilter::Value(value) => identity.file_id.as_ref() == Some(value),
            }))
}

async fn materialize_rows(
    store: &(impl StorageAdapterRead + ?Sized),
    mut rows: Vec<DecodedRow>,
) -> Result<MaterializedLiveStateBatch, LixError> {
    let refs = rows
        .iter()
        .flat_map(|row| [&row.value.snapshot, &row.value.metadata])
        .filter_map(|slot| match slot {
            DecodedSlot::Ref(value) => Some(*value),
            DecodedSlot::None | DecodedSlot::Inline(_) => None,
        })
        .collect::<Vec<_>>();
    let loaded = if refs.is_empty() {
        Vec::new()
    } else {
        JsonStoreContext::new()
            .load_bytes_many(
                store,
                JsonLoadRequestRef {
                    refs: &refs,
                    scope: JsonReadScopeRef::OutOfBand,
                },
            )
            .await?
            .into_values()
    };
    let mut loaded = loaded.into_iter();
    let mut builder = MaterializedLiveStateBatchBuilder::with_capacity(rows.len());
    for row in &mut rows {
        let snapshot = materialize_slot(&mut row.value.snapshot, &mut loaded)?;
        let metadata = materialize_slot(&mut row.value.metadata, &mut loaded)?;
        builder.push_owned(MaterializedLiveStateRow {
            entity_pk: row.entity_pk.clone(),
            schema_key: std::mem::take(&mut row.schema_key),
            file_id: row.file_id.take(),
            snapshot_content: snapshot,
            metadata,
            deleted: false,
            created_at: row.value.created_at,
            updated_at: row.value.updated_at,
            global: row.value.global,
            change_id: None,
            commit_id: None,
            untracked: true,
            branch_id: Arc::from(std::mem::take(&mut row.branch_id)),
        });
    }
    Ok(builder.finish())
}

fn materialize_slot(
    slot: &mut DecodedSlot,
    loaded: &mut impl Iterator<Item = Option<Bytes>>,
) -> Result<Option<SharedStr>, LixError> {
    match slot {
        DecodedSlot::None => Ok(None),
        DecodedSlot::Inline(value) => Ok(Some(value.clone())),
        DecodedSlot::Ref(_) => {
            let bytes = loaded
                .next()
                .flatten()
                .ok_or_else(|| codec_error("untracked JSON payload is missing"))?;
            SharedStr::from_utf8(bytes)
                .map(Some)
                .map_err(|_| codec_error("untracked JSON payload is not UTF-8"))
        }
    }
}

struct DecodedIdentity {
    branch_id: String,
    schema_key: String,
    file_id: Option<String>,
    entity_pk: EntityPk,
}

struct DecodedRow {
    branch_id: String,
    schema_key: String,
    file_id: Option<String>,
    entity_pk: EntityPk,
    value: DecodedValue,
}

struct DecodedValue {
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    global: bool,
    snapshot: DecodedSlot,
    metadata: DecodedSlot,
}

fn collect_value_refs(value: &DecodedValue, refs: &mut BTreeSet<[u8; 32]>) {
    for slot in [&value.snapshot, &value.metadata] {
        if let DecodedSlot::Ref(json_ref) = slot {
            refs.insert(*json_ref.as_hash_array());
        }
    }
}

enum DecodedSlot {
    None,
    Ref(JsonRef),
    Inline(SharedStr),
}

fn encode_key(
    branch_id: &str,
    schema_key: &str,
    file_id: Option<&str>,
    entity_pk: &EntityPk,
) -> Result<Vec<u8>, LixError> {
    let mut out = branch_prefix(branch_id)?;
    push_text(&mut out, schema_key)?;
    let entity_pk = crate::storage_codec::encode("untracked entity key", entity_pk)?;
    push_bytes(&mut out, &entity_pk)?;
    match file_id {
        None => out.push(0),
        Some(value) => {
            out.push(1);
            push_text(&mut out, value)?;
        }
    }
    Ok(out)
}

fn branch_prefix(branch_id: &str) -> Result<Vec<u8>, LixError> {
    let mut out = Vec::with_capacity(branch_id.len() + 4);
    push_text(&mut out, branch_id)?;
    Ok(out)
}

fn schema_prefix(branch_id: &str, schema_key: &str) -> Result<Vec<u8>, LixError> {
    let mut out = branch_prefix(branch_id)?;
    push_text(&mut out, schema_key)?;
    Ok(out)
}

fn entity_prefix(
    branch_id: &str,
    schema_key: &str,
    entity_pk: &EntityPk,
) -> Result<Vec<u8>, LixError> {
    let mut out = schema_prefix(branch_id, schema_key)?;
    let entity_pk = crate::storage_codec::encode("untracked entity key", entity_pk)?;
    push_bytes(&mut out, &entity_pk)?;
    Ok(out)
}

fn push_text(out: &mut Vec<u8>, value: &str) -> Result<(), LixError> {
    let len =
        u32::try_from(value.len()).map_err(|_| codec_error("untracked key text is too long"))?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn push_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<(), LixError> {
    let len = u32::try_from(value.len())
        .map_err(|_| codec_error("untracked key component is too long"))?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value);
    Ok(())
}

fn decode_key(bytes: &Bytes) -> Result<DecodedIdentity, LixError> {
    let mut offset = 0;
    let branch_id = read_text(bytes, &mut offset, "branch")?;
    let schema_key = read_text(bytes, &mut offset, "schema")?;
    let entity_pk_bytes = read_bytes(bytes, &mut offset, "entity")?;
    let entity_pk = crate::storage_codec::decode("untracked entity key", entity_pk_bytes)?;
    let file_id = match take(bytes, &mut offset, 1, "file tag")?[0] {
        0 => None,
        1 => Some(read_text(bytes, &mut offset, "file")?),
        _ => return Err(codec_error("untracked key has an invalid file tag")),
    };
    if offset != bytes.len() {
        return Err(codec_error("untracked row key has trailing bytes"));
    }
    Ok(DecodedIdentity {
        branch_id,
        schema_key,
        file_id,
        entity_pk,
    })
}

fn read_bytes<'a>(bytes: &'a [u8], offset: &mut usize, field: &str) -> Result<&'a [u8], LixError> {
    let len = take(bytes, offset, 4, field)?;
    let len = u32::from_be_bytes(len.try_into().expect("four bytes")) as usize;
    take(bytes, offset, len, field)
}

fn read_text(bytes: &[u8], offset: &mut usize, field: &str) -> Result<String, LixError> {
    let len = take(bytes, offset, 4, field)?;
    let len = u32::from_be_bytes(len.try_into().expect("four bytes")) as usize;
    let value = take(bytes, offset, len, field)?;
    String::from_utf8(value.to_vec())
        .map_err(|_| codec_error(format!("untracked {field} is not UTF-8")))
}

fn encode_value(
    branch_id: &str,
    delta: CurrentStateDeltaRef<'_>,
    created_at: LixTimestamp,
) -> Result<Vec<u8>, LixError> {
    let mut out = Vec::with_capacity(18 + slot_len(delta.snapshot) + slot_len(delta.metadata));
    out.push(VALUE_VERSION);
    out.push(if branch_id == GLOBAL_BRANCH_ID {
        FLAG_GLOBAL
    } else {
        0
    });
    out.extend_from_slice(&created_at.packed().to_le_bytes());
    out.extend_from_slice(&delta.updated_at.packed().to_le_bytes());
    encode_slot(&mut out, delta.snapshot)?;
    encode_slot(&mut out, delta.metadata)?;
    Ok(out)
}

fn slot_len(slot: JsonSlotRef<'_>) -> usize {
    match slot {
        JsonSlotRef::None => 1,
        JsonSlotRef::Ref(_) => 33,
        JsonSlotRef::Inline(value) => 5 + value.len(),
    }
}

fn encode_slot(out: &mut Vec<u8>, slot: JsonSlotRef<'_>) -> Result<(), LixError> {
    match slot {
        JsonSlotRef::None => out.push(SLOT_NONE),
        JsonSlotRef::Ref(value) => {
            out.push(SLOT_REF);
            out.extend_from_slice(value.as_hash_bytes());
        }
        JsonSlotRef::Inline(value) => {
            out.push(SLOT_INLINE);
            let len = u32::try_from(value.len())
                .map_err(|_| codec_error("untracked inline JSON is too long"))?;
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(value.as_bytes());
        }
    }
    Ok(())
}

fn decode_value(bytes: Bytes) -> Result<DecodedValue, LixError> {
    let mut offset = 0;
    if take(&bytes, &mut offset, 1, "value version")?[0] != VALUE_VERSION {
        return Err(codec_error(
            "untracked row has an unsupported value version",
        ));
    }
    let flags = take(&bytes, &mut offset, 1, "value flags")?[0];
    let created_at = LixTimestamp::from_packed(u64::from_le_bytes(
        take(&bytes, &mut offset, 8, "created_at")?
            .try_into()
            .expect("eight bytes"),
    ))
    .map_err(codec_error)?;
    let updated_at = LixTimestamp::from_packed(u64::from_le_bytes(
        take(&bytes, &mut offset, 8, "updated_at")?
            .try_into()
            .expect("eight bytes"),
    ))
    .map_err(codec_error)?;
    let snapshot = decode_slot(&bytes, &mut offset)?;
    let metadata = decode_slot(&bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(codec_error("untracked row value has trailing bytes"));
    }
    Ok(DecodedValue {
        created_at,
        updated_at,
        global: flags & FLAG_GLOBAL != 0,
        snapshot,
        metadata,
    })
}

fn decode_slot(bytes: &Bytes, offset: &mut usize) -> Result<DecodedSlot, LixError> {
    match take(bytes, offset, 1, "JSON slot tag")?[0] {
        SLOT_NONE => Ok(DecodedSlot::None),
        SLOT_REF => {
            let hash: [u8; 32] = take(bytes, offset, 32, "JSON ref")?
                .try_into()
                .expect("32 bytes");
            Ok(DecodedSlot::Ref(JsonRef::from_hash_bytes(hash)))
        }
        SLOT_INLINE => {
            let len = u32::from_le_bytes(
                take(bytes, offset, 4, "inline JSON length")?
                    .try_into()
                    .expect("four bytes"),
            ) as usize;
            let range = take_range(bytes, offset, len, "inline JSON")?;
            let value = SharedStr::from_utf8(bytes.slice(range))
                .map_err(|_| codec_error("untracked inline JSON is not UTF-8"))?;
            Ok(DecodedSlot::Inline(value))
        }
        _ => Err(codec_error("untracked row has an invalid JSON slot tag")),
    }
}

fn take<'a>(
    bytes: &'a [u8],
    offset: &mut usize,
    len: usize,
    field: &str,
) -> Result<&'a [u8], LixError> {
    let range = take_range(bytes, offset, len, field)?;
    Ok(&bytes[range])
}

fn take_range(
    bytes: &[u8],
    offset: &mut usize,
    len: usize,
    field: &str,
) -> Result<Range<usize>, LixError> {
    let end = offset
        .checked_add(len)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| codec_error(format!("untracked {field} is truncated")))?;
    let range = *offset..end;
    *offset = end;
    Ok(range)
}

fn codec_error(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INTERNAL_ERROR, message.into())
}
