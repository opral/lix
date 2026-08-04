//! Branch-stable physical state for history-free rows.
//!
//! Untracked rows do not participate in commit history, merge, diff, working
//! diff, or generation rotation. Their storage identity is therefore exactly
//! `(branch, schema, entity, file)` and a delete removes that key physically.

#[cfg(test)]
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::Arc;
#[cfg(all(feature = "storage-benches", not(test)))]
use std::sync::atomic::{AtomicU64, Ordering};

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

const VALUE_VERSION: u8 = 2;
const SLOT_NONE: u8 = 0;
const SLOT_REF: u8 = 1;
const SLOT_INLINE: u8 = 2;
const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";

#[cfg(any(test, feature = "storage-benches"))]
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct UntrackedFileCascadeReadProfile {
    pub(crate) authoritative_branch_scans: u64,
    pub(crate) authoritative_pages: u64,
    pub(crate) authoritative_rows: u64,
    pub(crate) authoritative_bytes: u64,
    pub(crate) retained_candidates: u64,
}

#[cfg(test)]
thread_local! {
    static AUTHORITATIVE_BRANCH_SCANS: Cell<u64> = const { Cell::new(0) };
    static AUTHORITATIVE_PAGES: Cell<u64> = const { Cell::new(0) };
    static AUTHORITATIVE_ROWS: Cell<u64> = const { Cell::new(0) };
    static AUTHORITATIVE_BYTES: Cell<u64> = const { Cell::new(0) };
    static RETAINED_CANDIDATES: Cell<u64> = const { Cell::new(0) };
}

#[cfg(all(feature = "storage-benches", not(test)))]
static AUTHORITATIVE_BRANCH_SCANS: AtomicU64 = AtomicU64::new(0);
#[cfg(all(feature = "storage-benches", not(test)))]
static AUTHORITATIVE_PAGES: AtomicU64 = AtomicU64::new(0);
#[cfg(all(feature = "storage-benches", not(test)))]
static AUTHORITATIVE_ROWS: AtomicU64 = AtomicU64::new(0);
#[cfg(all(feature = "storage-benches", not(test)))]
static AUTHORITATIVE_BYTES: AtomicU64 = AtomicU64::new(0);
#[cfg(all(feature = "storage-benches", not(test)))]
static RETAINED_CANDIDATES: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
pub(crate) fn take_untracked_file_cascade_read_profile() -> UntrackedFileCascadeReadProfile {
    UntrackedFileCascadeReadProfile {
        authoritative_branch_scans: AUTHORITATIVE_BRANCH_SCANS.with(|value| value.replace(0)),
        authoritative_pages: AUTHORITATIVE_PAGES.with(|value| value.replace(0)),
        authoritative_rows: AUTHORITATIVE_ROWS.with(|value| value.replace(0)),
        authoritative_bytes: AUTHORITATIVE_BYTES.with(|value| value.replace(0)),
        retained_candidates: RETAINED_CANDIDATES.with(|value| value.replace(0)),
    }
}

#[cfg(test)]
fn record_authoritative_branch_scan() {
    AUTHORITATIVE_BRANCH_SCANS.with(|value| value.set(value.get().saturating_add(1)));
}
#[cfg(all(feature = "storage-benches", not(test)))]
fn record_authoritative_branch_scan() {
    AUTHORITATIVE_BRANCH_SCANS.fetch_add(1, Ordering::Relaxed);
}

#[cfg(test)]
fn record_authoritative_page() {
    AUTHORITATIVE_PAGES.with(|value| value.set(value.get().saturating_add(1)));
}
#[cfg(all(feature = "storage-benches", not(test)))]
fn record_authoritative_page() {
    AUTHORITATIVE_PAGES.fetch_add(1, Ordering::Relaxed);
}

#[cfg(test)]
fn record_authoritative_row(bytes: u64) {
    AUTHORITATIVE_ROWS.with(|value| value.set(value.get().saturating_add(1)));
    AUTHORITATIVE_BYTES.with(|value| value.set(value.get().saturating_add(bytes)));
}
#[cfg(all(feature = "storage-benches", not(test)))]
fn record_authoritative_row(bytes: u64) {
    AUTHORITATIVE_ROWS.fetch_add(1, Ordering::Relaxed);
    AUTHORITATIVE_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

#[cfg(test)]
fn record_retained_candidate() {
    RETAINED_CANDIDATES.with(|value| value.set(value.get().saturating_add(1)));
}
#[cfg(all(feature = "storage-benches", not(test)))]
fn record_retained_candidate() {
    RETAINED_CANDIDATES.fetch_add(1, Ordering::Relaxed);
}

#[cfg(all(feature = "storage-benches", not(test)))]
#[allow(dead_code)]
pub(crate) fn take_untracked_file_cascade_read_profile() -> UntrackedFileCascadeReadProfile {
    UntrackedFileCascadeReadProfile {
        authoritative_branch_scans: AUTHORITATIVE_BRANCH_SCANS.swap(0, Ordering::Relaxed),
        authoritative_pages: AUTHORITATIVE_PAGES.swap(0, Ordering::Relaxed),
        authoritative_rows: AUTHORITATIVE_ROWS.swap(0, Ordering::Relaxed),
        authoritative_bytes: AUTHORITATIVE_BYTES.swap(0, Ordering::Relaxed),
        retained_candidates: RETAINED_CANDIDATES.swap(0, Ordering::Relaxed),
    }
}

pub(crate) async fn stage_untracked_deltas(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    branch_id: &str,
    deltas: &[CurrentStateDeltaRef<'_>],
    known_absent: &[bool],
) -> Result<(), LixError> {
    if known_absent.len() != deltas.len() {
        return Err(codec_error(
            "untracked known-absent flags do not align with deltas",
        ));
    }
    let mut physical_identities = BTreeSet::new();
    for delta in deltas.iter().filter(|delta| delta.untracked) {
        let key = StorageKey(Bytes::from(encode_key(
            branch_id,
            delta.schema_key,
            delta.file_id,
            delta.entity_pk,
        )?));
        if !physical_identities.insert(key) {
            return Err(codec_error(
                "untracked batch contains a duplicate physical identity without a certified canonical last-write-wins reduction",
            ));
        }
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
        previous_created_at.insert(key, decoded.created_at);
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
                    bytes: Bytes::from(encode_value(*delta, created_at)?),
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
        // File is not a leading row-key component, so cascade deletion is an
        // honest O(N) scan of this branch's sole authoritative row space.
        // Pages are validated to exhaustion before caller-owned writes are
        // staged; only matching canonical mutations survive each page.
        stage_file_cascade_from_pages(
            store,
            branch_id,
            &deleted_file_ids,
            &mut mutations,
            &mut retired_refs,
        )
        .await?;
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

async fn stage_file_cascade_from_pages(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    deleted_file_ids: &BTreeSet<String>,
    mutations: &mut BTreeMap<StorageKey, Option<StorageValue>>,
    retired_refs: &mut BTreeSet<[u8; 32]>,
) -> Result<(), LixError> {
    #[cfg(any(test, feature = "storage-benches"))]
    record_authoritative_branch_scan();
    let prefix = branch_prefix(branch_id)?;
    let plan = ScanPlan::prefix(
        UNTRACKED_ROW_SPACE,
        StoragePrefix {
            bytes: Bytes::copy_from_slice(&prefix),
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
        #[cfg(any(test, feature = "storage-benches"))]
        record_authoritative_page();
        let next_cursor = validate_scan_page_progress(
            &prefix,
            resume_after.as_ref(),
            page.value.entries.iter().map(|entry| &entry.key),
            page.value.has_more,
        )?;
        for entry in page.value.entries {
            let StorageProjectedValue::FullValue(value) = entry.value else {
                return Err(codec_error("untracked cascade scan omitted its row value"));
            };
            #[cfg(any(test, feature = "storage-benches"))]
            record_authoritative_row(
                u64::try_from(entry.key.0.len())
                    .unwrap_or(u64::MAX)
                    .saturating_add(u64::try_from(value.len()).unwrap_or(u64::MAX)),
            );
            let identity = decode_key(&entry.key.0)?;
            let decoded_value = decode_value(value)?;
            if identity.branch_id != branch_id {
                return Err(codec_error(
                    "untracked branch scan escaped its requested prefix",
                ));
            }
            if identity
                .file_id
                .as_ref()
                .is_some_and(|file_id| deleted_file_ids.contains(file_id))
            {
                #[cfg(any(test, feature = "storage-benches"))]
                record_retained_candidate();
                collect_value_refs(&decoded_value, retired_refs);
                if let Some(Some(staged)) = mutations.get(&entry.key) {
                    collect_value_refs(&decode_value(staged.bytes.clone())?, retired_refs);
                }
                mutations.insert(entry.key, None);
            }
        }
        if !page.value.has_more {
            break;
        }
        resume_after = next_cursor;
    }
    Ok(())
}

fn validate_scan_page_progress<'a>(
    prefix: &[u8],
    resume_after: Option<&StorageKey>,
    keys: impl IntoIterator<Item = &'a StorageKey>,
    has_more: bool,
) -> Result<Option<StorageKey>, LixError> {
    let mut keys = keys.into_iter();
    let Some(first) = keys.next() else {
        if has_more {
            return Err(codec_error(
                "untracked cascade scan reported more rows without a usable cursor",
            ));
        }
        return Ok(None);
    };
    if !first.0.starts_with(prefix) {
        return Err(codec_error("untracked scan escaped its requested prefix"));
    }
    if resume_after.is_some_and(|cursor| first <= cursor) {
        return Err(codec_error("untracked cascade scan cursor did not advance"));
    }
    let mut last = first;
    for key in keys {
        if !key.0.starts_with(prefix) {
            return Err(codec_error("untracked scan escaped its requested prefix"));
        }
        if key <= last {
            return Err(codec_error(
                "untracked cascade scan keys are not strictly increasing",
            ));
        }
        last = key;
    }
    Ok(Some(last.clone()))
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
    let mut decoded = Vec::new();
    let requested = request.filter.entity_pks.iter().collect::<BTreeSet<_>>();
    for branch_id in branch_ids {
        for schema_key in &request.filter.schema_keys {
            // The entity component is not the leading physical key. One
            // schema-prefix scan is therefore the bounded generic candidate
            // route for a homogeneous batch; filter the requested identities
            // from that one ordered stream instead of issuing one scan per
            // entity. The authoritative row space remains the sole source.
            let mut schema_rows = Vec::new();
            scan_prefix(
                store,
                &schema_prefix(branch_id, schema_key)?,
                request,
                &mut schema_rows,
            )
            .await?;
            decoded.extend(
                schema_rows
                    .into_iter()
                    .filter(|row| requested.contains(&row.entity_pk)),
            );
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
        let value = match value {
            None => continue,
            Some(StorageProjectedValue::FullValue(value)) => value,
            Some(StorageProjectedValue::KeyOnly) => {
                return Err(codec_error("untracked point read omitted its row value"));
            }
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
    load_untracked_exact_batch_inner(store, request, visible_branch_ids, true).await
}

pub(crate) async fn load_untracked_exact_owner_batch(
    store: &(impl StorageAdapterRead + ?Sized),
    request: &LiveStateExactBatchRequest,
    visible_branch_ids: &BTreeSet<String>,
) -> Result<MaterializedLiveStateExactBatch, LixError> {
    load_untracked_exact_batch_inner(store, request, visible_branch_ids, false).await
}

async fn load_untracked_exact_batch_inner(
    store: &(impl StorageAdapterRead + ?Sized),
    request: &LiveStateExactBatchRequest,
    visible_branch_ids: &BTreeSet<String>,
    include_global_fallback: bool,
) -> Result<MaterializedLiveStateExactBatch, LixError> {
    let mut keys = Vec::with_capacity(
        request
            .rows
            .len()
            .saturating_mul(if include_global_fallback { 2 } else { 1 }),
    );
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
        let global_index = if include_global_fallback && row.branch_id != GLOBAL_BRANCH_ID {
            let index = keys.len();
            keys.push(StorageKey(Bytes::from(encode_key(
                GLOBAL_BRANCH_ID,
                &row.schema_key,
                row.file_id.as_deref(),
                &row.entity_pk,
            )?)));
            Some(index)
        } else {
            None
        };
        requested_key_pairs.push(Some((branch_index, global_index)));
    }
    let values = PointReadPlan::new(UNTRACKED_ROW_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?
        .value
        .into_iter()
        .map(|value| match value {
            None => Ok(None),
            Some(StorageProjectedValue::FullValue(value)) => decode_value(value).map(Some),
            Some(StorageProjectedValue::KeyOnly) => {
                Err(codec_error("untracked exact read omitted its row value"))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut values = values;
    let mut decoded = Vec::new();
    let mut selected = Vec::with_capacity(request.rows.len());
    for (requested, pair) in request.rows.iter().zip(requested_key_pairs) {
        let Some((branch_index, global_index)) = pair else {
            selected.push(None);
            continue;
        };
        let branch_value = values[branch_index].take();
        let chosen_global = branch_value.is_none()
            && global_index.is_some_and(|global_index| values[global_index].is_some());
        let chosen = branch_value
            .or_else(|| global_index.and_then(|global_index| values[global_index].take()));
        let Some(value) = chosen else {
            selected.push(None);
            continue;
        };
        let (branch_id, branch_override) = if chosen_global {
            (
                GLOBAL_BRANCH_ID.to_owned(),
                Some(requested.branch_id.clone()),
            )
        } else {
            (requested.branch_id.clone(), None)
        };
        let index = decoded.len();
        decoded.push(DecodedRow {
            branch_id,
            schema_key: requested.schema_key.clone(),
            file_id: requested.file_id.clone(),
            entity_pk: requested.entity_pk.clone(),
            value,
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
    controlled_branches: &BTreeSet<String>,
) -> Result<Vec<JsonRef>, LixError> {
    let mut refs = BTreeSet::new();
    let plan = ScanPlan::prefix(
        UNTRACKED_ROW_SPACE,
        StoragePrefix {
            bytes: Bytes::new(),
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
        let next_cursor = validate_scan_page_progress(
            &[],
            resume_after.as_ref(),
            page.value.entries.iter().map(|entry| &entry.key),
            page.value.has_more,
        )?;
        for entry in page.value.entries {
            let identity = decode_key(&entry.key.0)?;
            if identity.branch_id != GLOBAL_BRANCH_ID
                && !controlled_branches.contains(&identity.branch_id)
            {
                return Err(codec_error(format!(
                    "untracked row belongs to orphan branch '{}'",
                    identity.branch_id
                )));
            }
            let StorageProjectedValue::FullValue(value) = entry.value else {
                return Err(codec_error("untracked GC scan omitted its row value"));
            };
            collect_value_refs(&decode_value(value)?, &mut refs);
        }
        if !page.value.has_more {
            break;
        }
        resume_after = next_cursor;
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
        let next_cursor = validate_scan_page_progress(
            prefix,
            resume_after.as_ref(),
            page.value.entries.iter().map(|entry| &entry.key),
            page.value.has_more,
        )?;
        for entry in page.value.entries {
            let identity = decode_key(&entry.key.0)?;
            let StorageProjectedValue::FullValue(value) = entry.value else {
                return Err(codec_error("untracked scan omitted its row value"));
            };
            let value = decode_value(value)?;
            if !matches_filter(&identity, request) {
                continue;
            }
            decoded.push(DecodedRow {
                branch_id: identity.branch_id,
                schema_key: identity.schema_key,
                file_id: identity.file_id,
                entity_pk: identity.entity_pk,
                value,
            });
        }
        if !page.value.has_more {
            break;
        }
        resume_after = next_cursor;
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
            global: row.branch_id == GLOBAL_BRANCH_ID,
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
    delta: CurrentStateDeltaRef<'_>,
    created_at: LixTimestamp,
) -> Result<Vec<u8>, LixError> {
    let mut out = Vec::with_capacity(17 + slot_len(delta.snapshot) + slot_len(delta.metadata));
    out.push(VALUE_VERSION);
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

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use super::*;
    use crate::storage_adapter::{
        Memory, StorageAdapter, StorageError, StorageGetManyRequest, StorageGetManyResult,
        StorageKeyRange, StorageReadEntry, StorageReadOptions, StorageScanChunk,
        StorageWriteOptions,
    };

    fn timestamp() -> LixTimestamp {
        LixTimestamp::expect_parse("untracked state test timestamp", "2026-01-01T00:00:00Z")
    }

    fn untracked<'a>(
        schema_key: &'a str,
        file_id: Option<&'a str>,
        entity_pk: &'a EntityPk,
        snapshot: &'a str,
        timestamp: LixTimestamp,
    ) -> CurrentStateDeltaRef<'a> {
        CurrentStateDeltaRef {
            schema_key,
            file_id,
            entity_pk,
            change_id: None,
            commit_id: None,
            untracked: true,
            deleted: false,
            created_at: timestamp,
            updated_at: timestamp,
            snapshot: JsonSlotRef::Inline(snapshot),
            metadata: JsonSlotRef::None,
            columnar_base_coordinate: None,
        }
    }

    fn file_delete(entity_pk: &EntityPk, timestamp: LixTimestamp) -> CurrentStateDeltaRef<'_> {
        CurrentStateDeltaRef {
            schema_key: FILE_DESCRIPTOR_SCHEMA_KEY,
            file_id: None,
            entity_pk,
            change_id: None,
            commit_id: None,
            untracked: false,
            deleted: true,
            created_at: timestamp,
            updated_at: timestamp,
            snapshot: JsonSlotRef::None,
            metadata: JsonSlotRef::None,
            columnar_base_coordinate: None,
        }
    }

    async fn commit_deltas(
        storage: &StorageAdapter<Memory>,
        branch_id: &str,
        deltas: &[CurrentStateDeltaRef<'_>],
        known_absent: &[bool],
    ) -> Result<(), LixError> {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let mut writes = storage.new_write_set();
        stage_untracked_deltas(&read, &mut writes, branch_id, deltas, known_absent).await?;
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .map_err(LixError::from)?;
        Ok(())
    }

    async fn commit_raw_rows(
        storage: &StorageAdapter<Memory>,
        rows: Vec<(StorageKey, StorageValue)>,
    ) -> Result<(), LixError> {
        let mut writes = storage.new_write_set();
        for (key, value) in rows {
            writes.put(UNTRACKED_ROW_SPACE, key, value);
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .map_err(LixError::from)?;
        Ok(())
    }

    struct ScriptedScanRead {
        pages: Mutex<VecDeque<StorageScanChunk>>,
    }

    impl ScriptedScanRead {
        fn new(pages: impl IntoIterator<Item = StorageScanChunk>) -> Self {
            Self {
                pages: Mutex::new(pages.into_iter().collect()),
            }
        }
    }

    impl StorageAdapterRead for ScriptedScanRead {
        fn get_many(
            &self,
            requests: &[StorageGetManyRequest<'_>],
        ) -> impl Future<Output = Result<StorageGetManyResult, StorageError>> + Send {
            let requested = requests.iter().map(|request| request.keys.len()).sum();
            async move { Ok(StorageGetManyResult::new(vec![None; requested])) }
        }

        fn scan(
            &self,
            _space: StorageSpace,
            _range: StorageKeyRange,
            _opts: StorageScanOptions,
        ) -> impl Future<Output = Result<StorageScanChunk, StorageError>> + Send {
            let page = self
                .pages
                .lock()
                .expect("scripted scan pages lock")
                .pop_front()
                .expect("scripted scan should not request an extra page");
            async move { Ok(page) }
        }
    }

    #[tokio::test]
    async fn file_cascade_with_zero_members_scans_authoritative_rows_once() -> Result<(), LixError>
    {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch-empty";
        let file_pk = EntityPk::single("file-empty");
        let delete = file_delete(&file_pk, timestamp());
        let _ = take_untracked_file_cascade_read_profile();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let mut writes = storage.new_write_set();
        stage_untracked_deltas(&read, &mut writes, branch_id, &[delete], &[false]).await?;

        assert!(writes.is_empty());
        assert_eq!(
            take_untracked_file_cascade_read_profile(),
            UntrackedFileCascadeReadProfile {
                authoritative_branch_scans: 1,
                authoritative_pages: 1,
                authoritative_rows: 0,
                authoritative_bytes: 0,
                retained_candidates: 0,
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn duplicate_physical_identity_is_rejected_before_staging() -> Result<(), LixError> {
        let storage = StorageAdapter::new(Memory::new());
        let entity_pk = EntityPk::single("duplicate");
        let first = untracked(
            "schema-duplicate",
            Some("file"),
            &entity_pk,
            "{\"v\":1}",
            timestamp(),
        );
        let second = untracked(
            "schema-duplicate",
            Some("file"),
            &entity_pk,
            "{\"v\":2}",
            timestamp(),
        );
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let mut writes = storage.new_write_set();
        let error = stage_untracked_deltas(
            &read,
            &mut writes,
            "branch-duplicate",
            &[first, second],
            &[false, false],
        )
        .await
        .expect_err("duplicate physical identities require an explicit upstream LWW proof");
        assert!(error.message.contains("duplicate physical identity"));
        assert!(
            writes.is_empty(),
            "duplicate rejection must stage no writes"
        );
        Ok(())
    }

    #[tokio::test]
    async fn gc_root_discovery_rejects_orphan_branch_rows() -> Result<(), LixError> {
        let storage = StorageAdapter::new(Memory::new());
        let entity_pk = EntityPk::single("orphan");
        let row = untracked("schema-orphan", None, &entity_pk, "{\"v\":1}", timestamp());
        let key = StorageKey(Bytes::from(encode_key(
            "branch-orphan",
            row.schema_key,
            row.file_id,
            row.entity_pk,
        )?));
        let value = StorageValue {
            bytes: Bytes::from(encode_value(row, row.created_at)?),
        };
        commit_raw_rows(&storage, vec![(key, value)]).await?;
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let error = untracked_json_refs(&read, &BTreeSet::new())
            .await
            .expect_err("GC must reject rows whose branch has no durable control");
        assert!(error.message.contains("orphan branch"));
        Ok(())
    }

    #[tokio::test]
    async fn file_cascade_deletes_multiple_members_and_same_batch_replacements()
    -> Result<(), LixError> {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch-members";
        let file_id = "file-a";
        let other_file_id = "file-b";
        let first_pk = EntityPk::single("first");
        let second_pk = EntityPk::single("second");
        let new_pk = EntityPk::single("new");
        let other_pk = EntityPk::single("other");
        let unfiled_pk = EntityPk::single("unfiled");
        let file_pk = EntityPk::single(file_id);
        let now = timestamp();
        let initial = [
            untracked(
                "schema-a",
                Some(file_id),
                &first_pk,
                r#"{"version":1}"#,
                now,
            ),
            untracked(
                "schema-b",
                Some(file_id),
                &second_pk,
                r#"{"version":1}"#,
                now,
            ),
            untracked(
                "schema-a",
                Some(other_file_id),
                &other_pk,
                r#"{"keep":"other-file"}"#,
                now,
            ),
            untracked("schema-a", None, &unfiled_pk, r#"{"keep":"unfiled"}"#, now),
        ];
        commit_deltas(&storage, branch_id, &initial, &[true, true, true, true]).await?;

        let replacement = untracked(
            "schema-a",
            Some(file_id),
            &first_pk,
            r#"{"version":2}"#,
            now,
        );
        let same_batch_new = untracked("schema-c", Some(file_id), &new_pk, r#"{"version":1}"#, now);
        let delete = file_delete(&file_pk, now);
        let _ = take_untracked_file_cascade_read_profile();
        commit_deltas(
            &storage,
            branch_id,
            &[replacement, same_batch_new, delete],
            &[false, true, false],
        )
        .await?;

        let profile = take_untracked_file_cascade_read_profile();
        assert_eq!(profile.authoritative_branch_scans, 1);
        assert_eq!(profile.authoritative_pages, 1);
        assert_eq!(profile.authoritative_rows, 4);
        assert!(profile.authoritative_bytes > 0);
        assert_eq!(profile.retained_candidates, 2);

        let keys = [
            StorageKey(Bytes::from(encode_key(
                branch_id,
                "schema-a",
                Some(file_id),
                &first_pk,
            )?)),
            StorageKey(Bytes::from(encode_key(
                branch_id,
                "schema-b",
                Some(file_id),
                &second_pk,
            )?)),
            StorageKey(Bytes::from(encode_key(
                branch_id,
                "schema-c",
                Some(file_id),
                &new_pk,
            )?)),
            StorageKey(Bytes::from(encode_key(
                branch_id,
                "schema-a",
                Some(other_file_id),
                &other_pk,
            )?)),
            StorageKey(Bytes::from(encode_key(
                branch_id,
                "schema-a",
                None,
                &unfiled_pk,
            )?)),
        ];
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let values = PointReadPlan::new(UNTRACKED_ROW_SPACE, &keys)
            .materialize(&read, StorageGetOptions::default())
            .await?
            .value;
        assert!(values[0].is_none());
        assert!(values[1].is_none());
        assert!(values[2].is_none());
        assert!(values[3].is_some());
        assert!(values[4].is_some());
        Ok(())
    }

    #[tokio::test]
    async fn file_cascade_malformed_key_fails_with_zero_staged_writes() -> Result<(), LixError> {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch-bad-key";
        let file_pk = EntityPk::single("file-a");
        let now = timestamp();
        let value_pk = EntityPk::single("value");
        let value_delta = untracked("schema", Some("file-a"), &value_pk, r#"{"ok":true}"#, now);
        let mut malformed_key = branch_prefix(branch_id)?;
        malformed_key.extend_from_slice(&u32::MAX.to_be_bytes());
        let raw_value = StorageValue {
            bytes: Bytes::from(encode_value(value_delta, now)?),
        };
        let raw_key = StorageKey(Bytes::from(malformed_key));
        let expected_bytes =
            u64::try_from(raw_key.0.len() + raw_value.bytes.len()).unwrap_or(u64::MAX);
        commit_raw_rows(&storage, vec![(raw_key, raw_value)]).await?;

        let delete = file_delete(&file_pk, now);
        let _ = take_untracked_file_cascade_read_profile();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let mut writes = storage.new_write_set();
        assert!(
            stage_untracked_deltas(&read, &mut writes, branch_id, &[delete], &[false])
                .await
                .is_err()
        );
        assert!(writes.is_empty());
        assert_eq!(
            take_untracked_file_cascade_read_profile(),
            UntrackedFileCascadeReadProfile {
                authoritative_branch_scans: 1,
                authoritative_pages: 1,
                authoritative_rows: 1,
                authoritative_bytes: expected_bytes,
                retained_candidates: 0,
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn file_cascade_malformed_nonmember_value_fails_after_match_without_staging()
    -> Result<(), LixError> {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch-bad-value";
        let file_id = "file-a";
        let valid_pk = EntityPk::single("valid");
        let corrupt_pk = EntityPk::single("corrupt");
        let file_pk = EntityPk::single(file_id);
        let now = timestamp();
        let valid_delta = untracked("a-valid", Some(file_id), &valid_pk, r#"{"ok":true}"#, now);
        let valid_key = StorageKey(Bytes::from(encode_key(
            branch_id,
            "a-valid",
            Some(file_id),
            &valid_pk,
        )?));
        let corrupt_key = StorageKey(Bytes::from(encode_key(
            branch_id,
            "z-corrupt",
            Some("other-file"),
            &corrupt_pk,
        )?));
        commit_raw_rows(
            &storage,
            vec![
                (
                    valid_key.clone(),
                    StorageValue {
                        bytes: Bytes::from(encode_value(valid_delta, now)?),
                    },
                ),
                (
                    corrupt_key,
                    StorageValue {
                        bytes: Bytes::from_static(b"malformed"),
                    },
                ),
            ],
        )
        .await?;

        let delete = file_delete(&file_pk, now);
        let _ = take_untracked_file_cascade_read_profile();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let mut writes = storage.new_write_set();
        assert!(
            stage_untracked_deltas(&read, &mut writes, branch_id, &[delete], &[false])
                .await
                .is_err()
        );
        assert!(
            writes.is_empty(),
            "terminal decode failure must not expose locally planned deletions"
        );
        let profile = take_untracked_file_cascade_read_profile();
        assert_eq!(profile.authoritative_branch_scans, 1);
        assert_eq!(profile.authoritative_pages, 1);
        assert_eq!(profile.authoritative_rows, 2);
        assert!(profile.authoritative_bytes > 0);
        assert_eq!(profile.retained_candidates, 1);

        let still_present = PointReadPlan::new(UNTRACKED_ROW_SPACE, &[valid_key])
            .materialize(&read, StorageGetOptions::default())
            .await?
            .value;
        assert!(still_present[0].is_some());
        Ok(())
    }

    #[tokio::test]
    async fn file_cascade_sparse_match_retains_only_matching_candidates() -> Result<(), LixError> {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch-sparse";
        let target_file_id = "file-target";
        let file_pk = EntityPk::single(target_file_id);
        let now = timestamp();
        let mut rows = Vec::new();
        let mut target_keys = Vec::new();
        let mut expected_bytes = 0_u64;
        for index in 0..128 {
            let schema_key = format!("schema-{index:04}");
            let entity_id = format!("entity-{index:04}");
            let entity_pk = EntityPk::single(entity_id.as_str());
            let file_id = if matches!(index, 17 | 109) {
                target_file_id
            } else {
                "file-other"
            };
            let key = StorageKey(Bytes::from(encode_key(
                branch_id,
                &schema_key,
                Some(file_id),
                &entity_pk,
            )?));
            let value = StorageValue {
                bytes: Bytes::from(encode_value(
                    untracked(
                        &schema_key,
                        Some(file_id),
                        &entity_pk,
                        r#"{"row":true}"#,
                        now,
                    ),
                    now,
                )?),
            };
            if file_id == target_file_id {
                target_keys.push(key.clone());
            }
            expected_bytes = expected_bytes
                .saturating_add(u64::try_from(key.0.len() + value.bytes.len()).unwrap_or(u64::MAX));
            rows.push((key, value));
        }
        commit_raw_rows(&storage, rows).await?;

        let delete = file_delete(&file_pk, now);
        let _ = take_untracked_file_cascade_read_profile();
        commit_deltas(&storage, branch_id, &[delete], &[false]).await?;
        assert_eq!(
            take_untracked_file_cascade_read_profile(),
            UntrackedFileCascadeReadProfile {
                authoritative_branch_scans: 1,
                authoritative_pages: 1,
                authoritative_rows: 128,
                authoritative_bytes: expected_bytes,
                retained_candidates: 2,
            }
        );

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let values = PointReadPlan::new(UNTRACKED_ROW_SPACE, &target_keys)
            .materialize(&read, StorageGetOptions::default())
            .await?
            .value;
        assert!(values.into_iter().all(|value| value.is_none()));
        Ok(())
    }

    #[tokio::test]
    async fn file_cascade_late_page_corruption_fails_with_zero_staged_writes()
    -> Result<(), LixError> {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch-multi-page";
        let target_file_id = "file-target";
        let file_pk = EntityPk::single(target_file_id);
        let now = timestamp();
        let sample_pk = EntityPk::single("sample");
        let valid_value = StorageValue {
            bytes: Bytes::from(encode_value(
                untracked(
                    "schema-sample",
                    Some("file-other"),
                    &sample_pk,
                    r#"{"row":true}"#,
                    now,
                ),
                now,
            )?),
        };
        let mut rows = Vec::with_capacity(crate::storage::MAX_SCAN_PAGE_ROWS + 1);
        let mut first_key = None;
        for index in 0..crate::storage::MAX_SCAN_PAGE_ROWS {
            let schema_key = format!("schema-{index:04}");
            let entity_id = format!("entity-{index:04}");
            let entity_pk = EntityPk::single(entity_id.as_str());
            let key = StorageKey(Bytes::from(encode_key(
                branch_id,
                &schema_key,
                Some("file-other"),
                &entity_pk,
            )?));
            first_key.get_or_insert_with(|| key.clone());
            rows.push((key, valid_value.clone()));
        }
        let corrupt_pk = EntityPk::single("corrupt");
        rows.push((
            StorageKey(Bytes::from(encode_key(
                branch_id,
                "schema-zzzz",
                Some("file-other"),
                &corrupt_pk,
            )?)),
            StorageValue {
                bytes: Bytes::from_static(b"malformed"),
            },
        ));
        commit_raw_rows(&storage, rows).await?;

        let delete = file_delete(&file_pk, now);
        let _ = take_untracked_file_cascade_read_profile();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let mut writes = storage.new_write_set();
        assert!(
            stage_untracked_deltas(&read, &mut writes, branch_id, &[delete], &[false])
                .await
                .is_err()
        );
        assert!(writes.is_empty());
        let profile = take_untracked_file_cascade_read_profile();
        assert_eq!(profile.authoritative_branch_scans, 1);
        assert_eq!(profile.authoritative_pages, 2);
        assert_eq!(
            profile.authoritative_rows,
            u64::try_from(crate::storage::MAX_SCAN_PAGE_ROWS + 1).unwrap_or(u64::MAX)
        );
        assert!(profile.authoritative_bytes > 0);
        assert_eq!(profile.retained_candidates, 0);

        let still_present = PointReadPlan::new(
            UNTRACKED_ROW_SPACE,
            &[first_key.expect("seeded at least one first-page row")],
        )
        .materialize(&read, StorageGetOptions::default())
        .await?
        .value;
        assert!(still_present[0].is_some());
        Ok(())
    }

    #[test]
    fn file_cascade_cursor_validator_requires_strict_global_key_order() -> Result<(), LixError> {
        let a = StorageKey(Bytes::from_static(b"a"));
        let b = StorageKey(Bytes::from_static(b"b"));
        let c = StorageKey(Bytes::from_static(b"c"));

        assert_eq!(
            validate_scan_page_progress(b"", Some(&a), [&b, &c], true)?,
            Some(c.clone())
        );
        assert!(validate_scan_page_progress(b"", Some(&a), [&a], true).is_err());
        assert!(validate_scan_page_progress(b"", Some(&b), [&a], true).is_err());
        assert!(validate_scan_page_progress(b"", None, [&a, &a], false).is_err());
        assert!(validate_scan_page_progress(b"", None, [&b, &a], false).is_err());
        assert_eq!(
            validate_scan_page_progress(b"", None, std::iter::empty(), false)?,
            None
        );
        assert!(validate_scan_page_progress(b"", None, std::iter::empty(), true).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn file_cascade_repeated_page_cursor_fails_with_zero_staged_writes()
    -> Result<(), LixError> {
        let branch_id = "branch-repeated-cursor";
        let file_id = "file-target";
        let file_pk = EntityPk::single(file_id);
        let row_pk = EntityPk::single("row");
        let now = timestamp();
        let key = StorageKey(Bytes::from(encode_key(
            branch_id,
            "schema",
            Some(file_id),
            &row_pk,
        )?));
        let value = Bytes::from(encode_value(
            untracked("schema", Some(file_id), &row_pk, r#"{"row":true}"#, now),
            now,
        )?);
        let entry = StorageReadEntry {
            key,
            value: StorageProjectedValue::FullValue(value),
        };
        let read = ScriptedScanRead::new([
            StorageScanChunk {
                entries: vec![entry.clone()],
                has_more: true,
            },
            StorageScanChunk {
                entries: vec![entry],
                has_more: false,
            },
        ]);
        let delete = file_delete(&file_pk, now);
        let _ = take_untracked_file_cascade_read_profile();
        let mut writes = StorageWriteSet::new();
        assert!(
            stage_untracked_deltas(&read, &mut writes, branch_id, &[delete], &[false])
                .await
                .is_err()
        );
        assert!(writes.is_empty());
        let profile = take_untracked_file_cascade_read_profile();
        assert_eq!(profile.authoritative_branch_scans, 1);
        assert_eq!(profile.authoritative_pages, 2);
        assert_eq!(profile.authoritative_rows, 1);
        assert_eq!(profile.retained_candidates, 1);
        Ok(())
    }

    #[tokio::test]
    async fn file_cascade_empty_has_more_page_fails_with_zero_staged_writes() -> Result<(), LixError>
    {
        let branch_id = "branch-empty-page";
        let file_pk = EntityPk::single("file-target");
        let read = ScriptedScanRead::new([StorageScanChunk {
            entries: Vec::new(),
            has_more: true,
        }]);
        let delete = file_delete(&file_pk, timestamp());
        let _ = take_untracked_file_cascade_read_profile();
        let mut writes = StorageWriteSet::new();
        assert!(
            stage_untracked_deltas(&read, &mut writes, branch_id, &[delete], &[false])
                .await
                .is_err()
        );
        assert!(writes.is_empty());
        assert_eq!(
            take_untracked_file_cascade_read_profile(),
            UntrackedFileCascadeReadProfile {
                authoritative_branch_scans: 1,
                authoritative_pages: 1,
                authoritative_rows: 0,
                authoritative_bytes: 0,
                retained_candidates: 0,
            }
        );
        Ok(())
    }
}
