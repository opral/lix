//! Branch-stable physical state for history-free rows.
//!
//! Untracked rows do not participate in commit history, merge, diff, working
//! diff, or generation rotation. Their canonical storage key is `(branch,
//! schema, entity)` and its value bundles the complete set of file variants;
//! deleting the final member removes that bundle key physically.

#[cfg(test)]
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::Arc;
#[cfg(all(feature = "storage-benches", not(test)))]
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use crate::NullableKeyFilter;
use crate::branch::BranchHeadControl;
use crate::common::{LixTimestamp, SharedStr};
use crate::entity_pk::EntityPk;
use crate::json_store::{
    JsonLoadRequestRef, JsonReadScopeRef, JsonRef, JsonSlotRef, JsonStoreContext,
};
#[cfg(test)]
use crate::live_state::LiveStateExactRowRequest;
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

pub(crate) const UNTRACKED_ROW_SPACE: StorageSpace = StorageSpace::mutable(
    StorageSpaceId(0x0004_0033),
    "live_state.untracked_bundle.v2",
);
const VALUE_VERSION: u8 = 2;
// The physical key names one canonical entity, while this value contains the
// complete set of file variants for that entity.  A bundle is the sole
// untracked authority; file fan-out records are never consulted or written by
// the bundle path.
const BUNDLE_MAGIC: &[u8] = b"LXUB2";
const SLOT_NONE: u8 = 0;
const SLOT_REF: u8 = 1;
const SLOT_INLINE: u8 = 2;
const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";

#[cfg(any(test, feature = "storage-benches"))]
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct UntrackedMutationReadProfile {
    pub(crate) previous_point_read_keys: u64,
    pub(crate) previous_scan_rows: u64,
    pub(crate) previous_scan_bytes: u64,
}

#[cfg(test)]
thread_local! {
    static PREVIOUS_POINT_READ_KEYS: Cell<u64> = const { Cell::new(0) };
    static PREVIOUS_SCAN_ROWS: Cell<u64> = const { Cell::new(0) };
    static PREVIOUS_SCAN_BYTES: Cell<u64> = const { Cell::new(0) };
}

#[cfg(all(feature = "storage-benches", not(test)))]
static PREVIOUS_POINT_READ_KEYS: AtomicU64 = AtomicU64::new(0);
#[cfg(all(feature = "storage-benches", not(test)))]
static PREVIOUS_SCAN_ROWS: AtomicU64 = AtomicU64::new(0);
#[cfg(all(feature = "storage-benches", not(test)))]
static PREVIOUS_SCAN_BYTES: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn take_untracked_mutation_read_profile() -> UntrackedMutationReadProfile {
    UntrackedMutationReadProfile {
        previous_point_read_keys: PREVIOUS_POINT_READ_KEYS.with(|value| value.replace(0)),
        previous_scan_rows: PREVIOUS_SCAN_ROWS.with(|value| value.replace(0)),
        previous_scan_bytes: PREVIOUS_SCAN_BYTES.with(|value| value.replace(0)),
    }
}

#[cfg(all(feature = "storage-benches", not(test)))]
#[allow(dead_code)]
pub(crate) fn take_untracked_mutation_read_profile() -> UntrackedMutationReadProfile {
    UntrackedMutationReadProfile {
        previous_point_read_keys: PREVIOUS_POINT_READ_KEYS.swap(0, Ordering::Relaxed),
        previous_scan_rows: PREVIOUS_SCAN_ROWS.swap(0, Ordering::Relaxed),
        previous_scan_bytes: PREVIOUS_SCAN_BYTES.swap(0, Ordering::Relaxed),
    }
}

#[cfg(test)]
fn record_previous_point_read_keys(keys: usize) {
    PREVIOUS_POINT_READ_KEYS.with(|value| {
        value.set(
            value
                .get()
                .saturating_add(u64::try_from(keys).unwrap_or(u64::MAX)),
        );
    });
}
#[cfg(all(feature = "storage-benches", not(test)))]
fn record_previous_point_read_keys(keys: usize) {
    PREVIOUS_POINT_READ_KEYS.fetch_add(u64::try_from(keys).unwrap_or(u64::MAX), Ordering::Relaxed);
}

#[cfg(test)]
fn record_previous_scan_row(bytes: usize) {
    PREVIOUS_SCAN_ROWS.with(|value| {
        value.set(value.get().saturating_add(1));
    });
    PREVIOUS_SCAN_BYTES.with(|value| {
        value.set(
            value
                .get()
                .saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX)),
        );
    });
}
#[cfg(all(feature = "storage-benches", not(test)))]
fn record_previous_scan_row(bytes: usize) {
    PREVIOUS_SCAN_ROWS.fetch_add(1, Ordering::Relaxed);
    PREVIOUS_SCAN_BYTES.fetch_add(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
}

pub(crate) async fn stage_untracked_deltas(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    branch_id: &str,
    control: BranchHeadControl,
    deltas: &[CurrentStateDeltaRef<'_>],
    known_absent: &[bool],
) -> Result<BranchHeadControl, LixError> {
    let empty = BTreeSet::new();
    stage_untracked_deltas_with_deleted_file_ids(
        store,
        writes,
        branch_id,
        control,
        deltas,
        known_absent,
        &empty,
    )
    .await
}

pub(crate) async fn stage_untracked_deltas_with_deleted_file_ids(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    branch_id: &str,
    control: BranchHeadControl,
    deltas: &[CurrentStateDeltaRef<'_>],
    known_absent: &[bool],
    deleted_file_ids_from_lifecycle: &BTreeSet<String>,
) -> Result<BranchHeadControl, LixError> {
    stage_untracked_deltas_inner(
        store,
        writes,
        branch_id,
        control,
        deltas,
        known_absent,
        deleted_file_ids_from_lifecycle,
        false,
    )
    .await
}

pub(crate) async fn stage_untracked_deltas_for_branch_deletion(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    branch_id: &str,
    control: BranchHeadControl,
    deltas: &[CurrentStateDeltaRef<'_>],
    known_absent: &[bool],
) -> Result<BranchHeadControl, LixError> {
    let empty = BTreeSet::new();
    stage_untracked_deltas_inner(
        store,
        writes,
        branch_id,
        control,
        deltas,
        known_absent,
        &empty,
        true,
    )
    .await
}
async fn stage_untracked_deltas_inner(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    branch_id: &str,
    control: BranchHeadControl,
    deltas: &[CurrentStateDeltaRef<'_>],
    known_absent: &[bool],
    deleted_file_ids: &BTreeSet<String>,
    drop_branch: bool,
) -> Result<BranchHeadControl, LixError> {
    stage_untracked_bundles(
        store,
        writes,
        branch_id,
        control,
        deltas,
        known_absent,
        deleted_file_ids,
        drop_branch,
    )
    .await
}
async fn read_untracked_bundles(
    store: &(impl StorageAdapterRead + ?Sized),
    keys: &[StorageKey],
) -> Result<BTreeMap<StorageKey, DecodedBundle>, LixError> {
    if keys.is_empty() {
        return Ok(BTreeMap::new());
    }
    #[cfg(any(test, feature = "storage-benches"))]
    record_previous_point_read_keys(keys.len());
    let values = PointReadPlan::from_unique_keys(UNTRACKED_ROW_SPACE, keys.to_vec())
        .materialize(store, StorageGetOptions::default())
        .await?
        .value;
    let mut bundles = BTreeMap::new();
    for (key, value) in keys.iter().cloned().zip(values) {
        let Some(value) = value else { continue };
        let StorageProjectedValue::FullValue(value) = value else {
            return Err(codec_error("untracked bundle point read omitted its value"));
        };
        let identity = decode_bundle_key(&key.0)?;
        let bundle = decode_bundle(value)?;
        bundles.insert(key, bundle);
        let _ = identity;
    }
    Ok(bundles)
}

async fn scan_all_untracked_bundles(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
) -> Result<BTreeMap<StorageKey, DecodedBundle>, LixError> {
    let prefix = branch_prefix(branch_id)?;
    let plan = ScanPlan::prefix(
        UNTRACKED_ROW_SPACE,
        StoragePrefix {
            bytes: Bytes::copy_from_slice(&prefix),
        },
    );
    let mut resume_after = None;
    let mut bundles = BTreeMap::new();
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
            &prefix,
            resume_after.as_ref(),
            page.value.entries.iter().map(|entry| &entry.key),
            page.value.has_more,
        )?;
        for entry in page.value.entries {
            let StorageProjectedValue::FullValue(value) = entry.value else {
                return Err(codec_error("untracked bundle scan omitted its value"));
            };
            let identity = decode_bundle_key(&entry.key.0)?;
            if identity.branch_id != branch_id {
                return Err(codec_error("untracked bundle scan escaped its branch"));
            }
            #[cfg(any(test, feature = "storage-benches"))]
            record_previous_scan_row(entry.key.0.len().saturating_add(value.len()));
            bundles.insert(entry.key, decode_bundle(value)?);
        }
        if !page.value.has_more {
            break;
        }
        resume_after = next_cursor;
    }
    Ok(bundles)
}

/// Validate every bundle in a branch cascade scan, retaining only bundles
/// containing one of the retired file identities.  The scan is still the
/// sole authority proof and remains O(N) in time, but successful staging keeps
/// only the affected K bundles rather than materializing the whole branch.
async fn scan_untracked_bundles_for_file_cascade(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    deleted_file_ids: &BTreeSet<String>,
) -> Result<BTreeMap<StorageKey, DecodedBundle>, LixError> {
    let prefix = branch_prefix(branch_id)?;
    let plan = ScanPlan::prefix(
        UNTRACKED_ROW_SPACE,
        StoragePrefix {
            bytes: Bytes::copy_from_slice(&prefix),
        },
    );
    let mut resume_after = None;
    let mut affected = BTreeMap::new();
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
            &prefix,
            resume_after.as_ref(),
            page.value.entries.iter().map(|entry| &entry.key),
            page.value.has_more,
        )?;
        for entry in page.value.entries {
            let StorageProjectedValue::FullValue(value) = entry.value else {
                return Err(codec_error(
                    "untracked bundle cascade scan omitted its value",
                ));
            };
            let identity = decode_bundle_key(&entry.key.0)?;
            if identity.branch_id != branch_id {
                return Err(codec_error(
                    "untracked bundle cascade scan escaped its branch",
                ));
            }
            #[cfg(any(test, feature = "storage-benches"))]
            record_previous_scan_row(entry.key.0.len().saturating_add(value.len()));
            let bundle = decode_bundle(value)?;
            if bundle.keys().any(|file_id| {
                file_id
                    .as_deref()
                    .is_some_and(|file_id| deleted_file_ids.contains(file_id))
            }) {
                affected.insert(entry.key, bundle);
            }
        }
        if !page.value.has_more {
            break;
        }
        resume_after = next_cursor;
    }
    Ok(affected)
}

async fn load_untracked_bundle_points(
    store: &(impl StorageAdapterRead + ?Sized),
    request: &LiveStateScanRequest,
    branch_ids: &[String],
    schema_keys: &[String],
    entity_pks: &[EntityPk],
) -> Result<MaterializedLiveStateBatch, LixError> {
    let mut keys = BTreeSet::new();
    for branch_id in branch_ids {
        for schema_key in schema_keys {
            for entity_pk in entity_pks {
                keys.insert(StorageKey(Bytes::from(encode_bundle_key(
                    branch_id, schema_key, entity_pk,
                )?)));
            }
        }
    }
    let bundles = read_untracked_bundles(store, &keys.into_iter().collect::<Vec<_>>()).await?;
    let mut decoded = Vec::new();
    for (key, bundle) in bundles {
        let identity = decode_bundle_key(&key.0)?;
        for (file_id, member) in bundle {
            let row_identity = DecodedIdentity {
                branch_id: identity.branch_id.clone(),
                schema_key: identity.schema_key.clone(),
                file_id: file_id.clone(),
                entity_pk: identity.entity_pk.clone(),
            };
            if matches_filter(&row_identity, request) {
                decoded.push(DecodedRow {
                    branch_id: row_identity.branch_id,
                    schema_key: row_identity.schema_key,
                    file_id: row_identity.file_id,
                    entity_pk: row_identity.entity_pk,
                    value: member.value,
                });
            }
        }
    }
    materialize_rows(store, decoded).await
}

async fn stage_untracked_bundles(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    branch_id: &str,
    control: BranchHeadControl,
    deltas: &[CurrentStateDeltaRef<'_>],
    known_absent: &[bool],
    deleted_file_ids_from_lifecycle: &BTreeSet<String>,
    drop_branch: bool,
) -> Result<BranchHeadControl, LixError> {
    if known_absent.len() != deltas.len() {
        return Err(codec_error(
            "untracked known-absent flags do not align with deltas",
        ));
    }
    let mut physical = BTreeSet::<(StorageKey, Option<String>)>::new();
    let mut keys = BTreeSet::new();
    for delta in deltas.iter().filter(|delta| delta.untracked) {
        if delta.change_id.is_some() || delta.commit_id.is_some() {
            return Err(codec_error(
                "dedicated untracked row carried commit identity",
            ));
        }
        let key = StorageKey(Bytes::from(encode_bundle_key(
            branch_id,
            delta.schema_key,
            delta.entity_pk,
        )?));
        if !physical.insert((key.clone(), delta.file_id.map(str::to_owned))) {
            return Err(codec_error(
                "untracked batch contains a duplicate physical identity without certified LWW",
            ));
        }
        keys.insert(key);
    }
    let key_vec = keys.iter().cloned().collect::<Vec<_>>();
    let mut states = read_untracked_bundles(store, &key_vec).await?;
    let mut changed_keys = keys.clone();

    // Validate the caller's absence proof against the exact bundle member.
    for (delta, absent) in deltas.iter().zip(known_absent).filter(|(d, _)| d.untracked) {
        let key = StorageKey(Bytes::from(encode_bundle_key(
            branch_id,
            delta.schema_key,
            delta.entity_pk,
        )?));
        let present = states
            .get(&key)
            .and_then(|bundle| bundle.get(&delta.file_id.map(str::to_owned)))
            .is_some();
        if *absent && present {
            return Err(codec_error(format!(
                "untracked bundle presence violates caller absence proof for schema '{}' entity {:?} file {:?}",
                delta.schema_key, delta.entity_pk, delta.file_id
            )));
        }
    }

    let mut deleted_file_ids = deltas
        .iter()
        .filter(|delta| delta.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY && delta.deleted)
        .map(|delta| delta.entity_pk.as_single_string_owned())
        .collect::<Result<BTreeSet<_>, _>>()?;
    deleted_file_ids.extend(deleted_file_ids_from_lifecycle.iter().cloned());
    if !deleted_file_ids.is_empty() {
        for (key, bundle) in
            scan_untracked_bundles_for_file_cascade(store, branch_id, &deleted_file_ids).await?
        {
            states.entry(key).or_insert(bundle);
        }
    }

    let mut retired_refs = BTreeSet::new();
    for delta in deltas.iter().filter(|delta| delta.untracked) {
        let key = StorageKey(Bytes::from(encode_bundle_key(
            branch_id,
            delta.schema_key,
            delta.entity_pk,
        )?));
        let bundle = states.entry(key.clone()).or_default();
        let file_id = delta.file_id.map(str::to_owned);
        if let Some(previous) = bundle.get(&file_id) {
            if delta.deleted || !delta.deleted {
                collect_value_refs(&previous.value, &mut retired_refs);
            }
        }
        if delta.deleted {
            bundle.remove(&file_id);
        } else {
            let created_at = bundle
                .get(&file_id)
                .map(|member| member.value.created_at)
                .unwrap_or(delta.created_at);
            let encoded = Bytes::from(encode_value(*delta, created_at)?);
            let value = decode_value(encoded.clone())?;
            bundle.insert(file_id, BundleMember { encoded, value });
        }
    }
    let mut cascaded_keys = BTreeSet::new();
    for (key, bundle) in states.iter_mut() {
        for file_id in deleted_file_ids.iter() {
            if let Some(member) = bundle.remove(&Some(file_id.clone())) {
                collect_value_refs(&member.value, &mut retired_refs);
                cascaded_keys.insert(key.clone());
            }
        }
    }
    changed_keys.extend(cascaded_keys);

    if drop_branch {
        let existing = scan_all_untracked_bundles(store, branch_id).await?;
        for key in existing.keys() {
            writes.delete(UNTRACKED_ROW_SPACE, key.clone());
        }
    } else {
        for key in changed_keys {
            let Some(bundle) = states.remove(&key) else {
                continue;
            };
            if bundle.is_empty() {
                writes.delete(UNTRACKED_ROW_SPACE, key);
            } else {
                writes.put(
                    UNTRACKED_ROW_SPACE,
                    key,
                    StorageValue {
                        bytes: encode_bundle(&bundle)?,
                    },
                );
            }
        }
    }
    crate::json_store::JsonStoreWriter::stage_untracked_reclaim_candidates(
        writes,
        retired_refs.into_iter().map(JsonRef::from_hash_bytes),
    );
    control.next_current_state_revision()
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
        return load_untracked_bundle_points(
            store,
            request,
            branch_ids,
            &request.filter.schema_keys,
            &request.filter.entity_pks,
        )
        .await;
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
    return load_untracked_exact_bundle_batch(
        store,
        request,
        visible_branch_ids,
        include_global_fallback,
    )
    .await;
}

async fn load_untracked_exact_bundle_batch(
    store: &(impl StorageAdapterRead + ?Sized),
    request: &LiveStateExactBatchRequest,
    visible_branch_ids: &BTreeSet<String>,
    include_global_fallback: bool,
) -> Result<MaterializedLiveStateExactBatch, LixError> {
    let mut requested_keys = Vec::with_capacity(request.rows.len());
    let mut keys = BTreeSet::new();
    for row in &request.rows {
        if !visible_branch_ids.contains(&row.branch_id) {
            requested_keys.push(None);
            continue;
        }
        let branch_key = StorageKey(Bytes::from(encode_bundle_key(
            &row.branch_id,
            &row.schema_key,
            &row.entity_pk,
        )?));
        let global_key = if include_global_fallback && row.branch_id != GLOBAL_BRANCH_ID {
            Some(StorageKey(Bytes::from(encode_bundle_key(
                GLOBAL_BRANCH_ID,
                &row.schema_key,
                &row.entity_pk,
            )?)))
        } else {
            None
        };
        keys.insert(branch_key.clone());
        if let Some(global_key) = &global_key {
            keys.insert(global_key.clone());
        }
        requested_keys.push(Some((branch_key, global_key)));
    }
    let keys = keys.into_iter().collect::<Vec<_>>();
    let bundles = read_untracked_bundles(store, &keys).await?;
    let mut decoded = Vec::new();
    let mut selections = Vec::with_capacity(request.rows.len());
    for (row, requested) in request.rows.iter().zip(requested_keys) {
        let Some((branch_key, global_key)) = requested else {
            selections.push(None);
            continue;
        };
        let file_id = row.file_id.clone();
        let (chosen_key, branch_override, member) = if let Some(bundle) = bundles.get(&branch_key)
            && let Some(member) = bundle.get(&file_id)
        {
            (branch_key, None, member)
        } else if let Some(global_key) = global_key
            && let Some(bundle) = bundles.get(&global_key)
            && let Some(member) = bundle.get(&file_id)
        {
            (global_key, Some(row.branch_id.clone()), member)
        } else {
            selections.push(None);
            continue;
        };
        let identity = decode_bundle_key(&chosen_key.0)?;
        let index = decoded.len();
        decoded.push(DecodedRow {
            branch_id: identity.branch_id,
            schema_key: identity.schema_key,
            file_id,
            entity_pk: identity.entity_pk,
            value: member.value.clone(),
        });
        selections.push(Some((index, branch_override)));
    }
    let rows = materialize_rows(store, decoded).await?;
    let mut builder = MaterializedLiveStateBatchBuilder::with_capacity(rows.len());
    let mut slots = Vec::with_capacity(selections.len());
    for selection in selections {
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
    return untracked_bundle_json_refs(store, controlled_branches).await;
}

async fn untracked_bundle_json_refs(
    store: &(impl StorageAdapterRead + ?Sized),
    controlled_branches: &BTreeSet<String>,
) -> Result<Vec<JsonRef>, LixError> {
    let plan = ScanPlan::prefix(
        UNTRACKED_ROW_SPACE,
        StoragePrefix {
            bytes: Bytes::new(),
        },
    );
    let mut resume_after = None;
    let mut refs = BTreeSet::new();
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
            let identity = decode_bundle_key(&entry.key.0)?;
            if identity.branch_id != GLOBAL_BRANCH_ID
                && !controlled_branches.contains(&identity.branch_id)
            {
                return Err(codec_error(format!(
                    "untracked bundle belongs to orphan branch '{}'",
                    identity.branch_id
                )));
            }
            let StorageProjectedValue::FullValue(value) = entry.value else {
                return Err(codec_error("untracked bundle GC scan omitted its value"));
            };
            for member in decode_bundle(value)?.values() {
                collect_value_refs(&member.value, &mut refs);
            }
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
    scan_bundle_prefix(store, prefix, request, decoded).await
}

async fn scan_bundle_prefix(
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
            let StorageProjectedValue::FullValue(value) = entry.value else {
                return Err(codec_error("untracked bundle scan omitted its value"));
            };
            let identity = decode_bundle_key(&entry.key.0)?;
            let bundle = decode_bundle(value)?;
            for (file_id, member) in bundle {
                let row_identity = DecodedIdentity {
                    branch_id: identity.branch_id.clone(),
                    schema_key: identity.schema_key.clone(),
                    file_id: file_id.clone(),
                    entity_pk: identity.entity_pk.clone(),
                };
                if matches_filter(&row_identity, request) {
                    decoded.push(DecodedRow {
                        branch_id: row_identity.branch_id,
                        schema_key: row_identity.schema_key,
                        file_id: row_identity.file_id,
                        entity_pk: row_identity.entity_pk,
                        value: member.value,
                    });
                }
            }
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

#[derive(Clone)]
struct DecodedValue {
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    snapshot: DecodedSlot,
    metadata: DecodedSlot,
}

#[derive(Clone)]
struct BundleMember {
    encoded: Bytes,
    value: DecodedValue,
}

type DecodedBundle = BTreeMap<Option<String>, BundleMember>;

fn collect_value_refs(value: &DecodedValue, refs: &mut BTreeSet<[u8; 32]>) {
    for slot in [&value.snapshot, &value.metadata] {
        if let DecodedSlot::Ref(json_ref) = slot {
            refs.insert(*json_ref.as_hash_array());
        }
    }
}

#[derive(Clone)]
enum DecodedSlot {
    None,
    Ref(JsonRef),
    Inline(SharedStr),
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

fn encode_bundle_key(
    branch_id: &str,
    schema_key: &str,
    entity_pk: &EntityPk,
) -> Result<Vec<u8>, LixError> {
    let mut out = schema_prefix(branch_id, schema_key)?;
    let entity_pk = crate::storage_codec::encode("untracked bundle entity key", entity_pk)?;
    push_bytes(&mut out, &entity_pk)?;
    Ok(out)
}

fn decode_bundle_key(bytes: &Bytes) -> Result<DecodedIdentity, LixError> {
    let mut offset = 0;
    let branch_id = read_text(bytes, &mut offset, "bundle branch")?;
    let schema_key = read_text(bytes, &mut offset, "bundle schema")?;
    let entity_pk_bytes = read_bytes(bytes, &mut offset, "bundle entity")?;
    let entity_pk = crate::storage_codec::decode("untracked bundle entity key", entity_pk_bytes)?;
    if offset != bytes.len() {
        return Err(codec_error("untracked bundle key has trailing bytes"));
    }
    let canonical = encode_bundle_key(&branch_id, &schema_key, &entity_pk)?;
    if canonical.as_slice() != bytes.as_ref() {
        return Err(codec_error("untracked bundle key is not canonical"));
    }
    Ok(DecodedIdentity {
        branch_id,
        schema_key,
        file_id: None,
        entity_pk,
    })
}

fn encode_bundle(bundle: &DecodedBundle) -> Result<Bytes, LixError> {
    let count = u32::try_from(bundle.len())
        .map_err(|_| codec_error("untracked bundle has too many file variants"))?;
    let mut out = Vec::new();
    out.extend_from_slice(BUNDLE_MAGIC);
    out.extend_from_slice(&count.to_be_bytes());
    let mut previous: Option<&str> = None;
    for (file_id, member) in bundle {
        match file_id {
            None => {
                if previous.is_some() {
                    return Err(codec_error("untracked bundle variants are not sorted"));
                }
                out.push(0);
            }
            Some(file_id) => {
                if previous.is_none() {
                    previous = Some(file_id);
                } else if previous.is_some_and(|previous| previous >= file_id.as_str()) {
                    return Err(codec_error(
                        "untracked bundle variants are not strictly sorted",
                    ));
                } else {
                    previous = Some(file_id);
                }
                out.push(1);
                push_text(&mut out, file_id)?;
            }
        }
        let len = u32::try_from(member.encoded.len())
            .map_err(|_| codec_error("untracked bundle member is too large"))?;
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(&member.encoded);
    }
    Ok(Bytes::from(out))
}

fn decode_bundle(bytes: Bytes) -> Result<DecodedBundle, LixError> {
    if !bytes.starts_with(BUNDLE_MAGIC) {
        return Err(codec_error("untracked bundle has an unsupported format"));
    }
    let mut offset = BUNDLE_MAGIC.len();
    let count = u32::from_be_bytes(
        take(&bytes, &mut offset, 4, "bundle variant count")?
            .try_into()
            .expect("four bytes"),
    ) as usize;
    if count == 0 {
        return Err(codec_error("untracked bundle cannot have zero variants"));
    }
    let mut bundle = BTreeMap::new();
    let mut previous: Option<String> = None;
    for _ in 0..count {
        let file_id = match take(&bytes, &mut offset, 1, "bundle file tag")?[0] {
            0 => None,
            1 => Some(read_text(&bytes, &mut offset, "bundle file")?),
            _ => return Err(codec_error("untracked bundle has an invalid file tag")),
        };
        if let Some(file_id) = &file_id {
            if previous
                .as_deref()
                .is_some_and(|previous| previous >= file_id.as_str())
            {
                return Err(codec_error("untracked bundle variants are not canonical"));
            }
            previous = Some(file_id.clone());
        } else if previous.is_some() {
            return Err(codec_error("untracked bundle null variant is out of order"));
        }
        let value_len = u32::from_be_bytes(
            take(&bytes, &mut offset, 4, "bundle member length")?
                .try_into()
                .expect("four bytes"),
        ) as usize;
        let encoded =
            Bytes::copy_from_slice(take(&bytes, &mut offset, value_len, "bundle member value")?);
        let value = decode_value(encoded.clone())?;
        if bundle
            .insert(file_id, BundleMember { encoded, value })
            .is_some()
        {
            return Err(codec_error(
                "untracked bundle contains a duplicate file variant",
            ));
        }
    }
    if offset != bytes.len() {
        return Err(codec_error("untracked bundle has trailing bytes"));
    }
    Ok(bundle)
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
    use super::*;
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};

    fn timestamp() -> LixTimestamp {
        LixTimestamp::expect_parse("untracked state test timestamp", "2026-01-01T00:00:00Z")
    }

    fn test_control(branch_id: &str) -> BranchHeadControl {
        BranchHeadControl {
            head_commit_id: crate::changelog::CommitId::for_test_label(branch_id),
            generation: crate::changelog::CommitId::for_test_label(branch_id),
            current_state_revision: 0,
            schema_presence_bloom: [0; 4],
            working_diff_checkpoint_commit_id: None,
            created_at: timestamp(),
            updated_at: timestamp(),
            ref_change_id: crate::changelog::ChangeId::for_test_label(branch_id),
        }
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
        let control = crate::branch::BranchHeadControlContext::new()
            .reader(&read)
            .load(branch_id)
            .await
            .map_err(LixError::from)?
            .unwrap_or_else(|| test_control(branch_id));
        let updated_control =
            stage_untracked_deltas(&read, &mut writes, branch_id, control, deltas, known_absent)
                .await?;
        crate::branch::stage_branch_head_control(&mut writes, branch_id, updated_control)?;
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .map_err(LixError::from)?;
        Ok(())
    }

    #[tokio::test]
    async fn exact_bundle_lookup_falls_back_per_file_variant() -> Result<(), LixError> {
        let storage = StorageAdapter::new(Memory::new());
        let entity = EntityPk::single("shadowed-entity");
        let global = untracked(
            "bundle-schema",
            Some("file-a"),
            &entity,
            r#"{"owner":"global"}"#,
            timestamp(),
        );
        let branch = untracked(
            "bundle-schema",
            Some("file-b"),
            &entity,
            r#"{"owner":"branch"}"#,
            timestamp(),
        );
        commit_deltas(&storage, GLOBAL_BRANCH_ID, &[global], &[true]).await?;
        commit_deltas(&storage, "bundle-shadow", &[branch], &[true]).await?;
        let request = LiveStateExactBatchRequest {
            rows: vec![
                LiveStateExactRowRequest {
                    schema_key: "bundle-schema".to_owned(),
                    branch_id: "bundle-shadow".to_owned(),
                    entity_pk: entity.clone(),
                    file_id: Some("file-a".to_owned()),
                },
                LiveStateExactRowRequest {
                    schema_key: "bundle-schema".to_owned(),
                    branch_id: "bundle-shadow".to_owned(),
                    entity_pk: entity,
                    file_id: Some("file-b".to_owned()),
                },
            ],
            untracked: Some(true),
            ..LiveStateExactBatchRequest::default()
        };
        let visible = BTreeSet::from([GLOBAL_BRANCH_ID.to_owned(), "bundle-shadow".to_owned()]);
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let rows = load_untracked_exact_batch(&read, &request, &visible)
            .await?
            .into_rows();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0]
                .as_ref()
                .and_then(|row| row.snapshot_content.as_deref()),
            Some(r#"{"owner":"global"}"#)
        );
        assert_eq!(
            rows[1]
                .as_ref()
                .and_then(|row| row.snapshot_content.as_deref()),
            Some(r#"{"owner":"branch"}"#)
        );
        assert!(
            rows[0]
                .as_ref()
                .is_some_and(|row| row.branch_id.as_ref() == "bundle-shadow")
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
            test_control("branch-duplicate"),
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
    async fn existing_untracked_row_rejects_an_absence_proof() -> Result<(), LixError> {
        let storage = StorageAdapter::new(Memory::new());
        let entity_pk = EntityPk::single("absence-proof");
        let initial = untracked(
            "schema-absence-proof",
            Some("file"),
            &entity_pk,
            "{\"v\":1}",
            timestamp(),
        );
        commit_deltas(&storage, "branch-absence-proof", &[initial], &[true]).await?;
        let update = untracked(
            "schema-absence-proof",
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
            "branch-absence-proof",
            test_control("branch-absence-proof"),
            &[update],
            &[true],
        )
        .await
        .expect_err("an absence proof must fail when the row already exists");
        assert!(error.message.contains("absence proof"));
        assert!(
            writes.is_empty(),
            "absence-proof rejection must stage no writes"
        );
        Ok(())
    }

    #[test]
    fn bundle_codec_rejects_duplicate_and_noncanonical_variants() -> Result<(), LixError> {
        let entity = EntityPk::single("bundle-codec");
        let delta = untracked("schema", None, &entity, r#"{"v":1}"#, timestamp());
        let encoded = Bytes::from(encode_value(delta, timestamp())?);
        let mut bundle = BTreeMap::new();
        bundle.insert(
            None,
            BundleMember {
                value: decode_value(encoded.clone())?,
                encoded,
            },
        );
        let bytes = encode_bundle(&bundle)?;
        assert_eq!(decode_bundle(bytes)?.len(), 1);
        let mut malformed = BUNDLE_MAGIC.to_vec();
        malformed.extend_from_slice(&1_u32.to_be_bytes());
        malformed.push(0);
        malformed.extend_from_slice(&0_u32.to_be_bytes());
        assert!(decode_bundle(Bytes::from(malformed)).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn bundle_sparse_update_reads_one_exact_bundle_per_identity() -> Result<(), LixError> {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "bundle-sparse";
        let entities = (0..8)
            .map(|ordinal| EntityPk::single(format!("entity-{ordinal}")))
            .collect::<Vec<_>>();
        let inserts = entities
            .iter()
            .map(|entity| {
                untracked(
                    "bundle-schema",
                    Some("file-a"),
                    entity,
                    r#"{"v":1}"#,
                    timestamp(),
                )
            })
            .collect::<Vec<_>>();
        commit_deltas(&storage, branch_id, &inserts, &vec![true; inserts.len()]).await?;
        let _ = take_untracked_mutation_read_profile();
        let update = untracked(
            "bundle-schema",
            Some("file-a"),
            &entities[3],
            r#"{"v":2}"#,
            timestamp(),
        );
        commit_deltas(&storage, branch_id, &[update], &[false]).await?;
        let profile = take_untracked_mutation_read_profile();
        assert_eq!(profile.previous_point_read_keys, 1);
        assert_eq!(profile.previous_scan_rows, 0);
        assert_eq!(profile.previous_scan_bytes, 0);
        Ok(())
    }

    #[tokio::test]
    async fn bundle_replacement_preserves_other_variants_and_created_at() -> Result<(), LixError> {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "bundle-replacement";
        let entity = EntityPk::single("same-entity");
        let first_created =
            LixTimestamp::expect_parse("first bundle creation timestamp", "2026-01-01T00:00:00Z");
        let second_created =
            LixTimestamp::expect_parse("second bundle creation timestamp", "2026-01-02T00:00:00Z");
        let initial = [
            untracked(
                "bundle-schema",
                Some("file-a"),
                &entity,
                r#"{"v":"a1"}"#,
                first_created,
            ),
            untracked(
                "bundle-schema",
                Some("file-b"),
                &entity,
                r#"{"v":"b1"}"#,
                first_created,
            ),
        ];
        commit_deltas(&storage, branch_id, &initial, &[true, true]).await?;
        let replacement = untracked(
            "bundle-schema",
            Some("file-a"),
            &entity,
            r#"{"v":"a2"}"#,
            second_created,
        );
        commit_deltas(&storage, branch_id, &[replacement], &[false]).await?;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let key = StorageKey(Bytes::from(encode_bundle_key(
            branch_id,
            "bundle-schema",
            &entity,
        )?));
        let value = PointReadPlan::new(UNTRACKED_ROW_SPACE, &[key])
            .materialize(&read, StorageGetOptions::default())
            .await?
            .value
            .into_iter()
            .next()
            .flatten()
            .ok_or_else(|| codec_error("replacement bundle disappeared"))?;
        let StorageProjectedValue::FullValue(value) = value else {
            return Err(codec_error("replacement bundle omitted its value"));
        };
        let bundle = decode_bundle(value)?;
        assert_eq!(bundle.len(), 2);
        assert_eq!(
            bundle[&Some("file-a".to_string())].value.created_at,
            first_created
        );
        assert_eq!(
            bundle[&Some("file-b".to_string())].value.created_at,
            first_created
        );
        assert_eq!(
            bundle[&Some("file-a".to_string())].value.updated_at,
            second_created
        );
        Ok(())
    }

    #[tokio::test]
    async fn ten_thousand_bundle_updates_use_exact_keys_not_range_scans() -> Result<(), LixError> {
        const ROWS: usize = 10_000;
        let storage = StorageAdapter::new(Memory::new());
        let entities = (0..ROWS)
            .map(|ordinal| EntityPk::single(format!("bundle-{ordinal:05}")))
            .collect::<Vec<_>>();
        let inserts = entities
            .iter()
            .map(|entity| untracked("bundle-schema", None, entity, r#"{"v":1}"#, timestamp()))
            .collect::<Vec<_>>();
        commit_deltas(&storage, "bundle-10k", &inserts, &vec![true; ROWS]).await?;
        let _ = take_untracked_mutation_read_profile();
        let updates = entities
            .iter()
            .map(|entity| untracked("bundle-schema", None, entity, r#"{"v":2}"#, timestamp()))
            .collect::<Vec<_>>();
        commit_deltas(&storage, "bundle-10k", &updates, &vec![false; ROWS]).await?;
        let profile = take_untracked_mutation_read_profile();
        assert_eq!(profile.previous_point_read_keys, ROWS as u64);
        assert_eq!(profile.previous_scan_rows, 0);
        assert_eq!(profile.previous_scan_bytes, 0);
        Ok(())
    }

    #[tokio::test]
    async fn bundle_file_cascade_rewrites_only_authoritative_bundles() -> Result<(), LixError> {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "bundle-cascade";
        let entity = EntityPk::single("member");
        let unaffected_entity = EntityPk::single("unaffected-member");
        let row = untracked(
            "schema",
            Some("file-cascade"),
            &entity,
            r#"{"v":1}"#,
            timestamp(),
        );
        let unaffected_row = untracked(
            "schema",
            Some("file-keep"),
            &unaffected_entity,
            r#"{"v":2}"#,
            timestamp(),
        );
        commit_deltas(&storage, branch_id, &[row, unaffected_row], &[true, true]).await?;
        let _ = take_untracked_mutation_read_profile();
        let file = EntityPk::single("file-cascade");
        let delete = file_delete(&file, timestamp());
        commit_deltas(&storage, branch_id, &[delete], &[false]).await?;
        let profile = take_untracked_mutation_read_profile();
        assert_eq!(profile.previous_scan_rows, 2);
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let key = StorageKey(Bytes::from(encode_bundle_key(
            branch_id, "schema", &entity,
        )?));
        let value = PointReadPlan::new(UNTRACKED_ROW_SPACE, &[key])
            .materialize(&read, StorageGetOptions::default())
            .await?
            .value
            .into_iter()
            .next()
            .flatten();
        assert!(value.is_none());
        let unaffected_key = StorageKey(Bytes::from(encode_bundle_key(
            branch_id,
            "schema",
            &unaffected_entity,
        )?));
        let unaffected = PointReadPlan::new(UNTRACKED_ROW_SPACE, &[unaffected_key])
            .materialize(&read, StorageGetOptions::default())
            .await?
            .value
            .into_iter()
            .next()
            .flatten();
        assert!(
            unaffected.is_some(),
            "unaffected bundle must survive cascade"
        );
        Ok(())
    }

    #[tokio::test]
    async fn malformed_bundle_fails_closed_before_file_cascade_writes() -> Result<(), LixError> {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "bundle-corrupt";
        let entity = EntityPk::single("member");
        let key = StorageKey(Bytes::from(encode_bundle_key(
            branch_id, "schema", &entity,
        )?));
        let mut corrupt = storage.new_write_set();
        corrupt.put(
            UNTRACKED_ROW_SPACE,
            key,
            StorageValue {
                bytes: Bytes::from_static(b"not-a-bundle"),
            },
        );
        storage
            .commit_write_set(corrupt, StorageWriteOptions::default())
            .await
            .map_err(LixError::from)?;
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let mut writes = storage.new_write_set();
        let file = EntityPk::single("file-corrupt");
        let delete = file_delete(&file, timestamp());
        assert!(
            stage_untracked_deltas(
                &read,
                &mut writes,
                branch_id,
                test_control(branch_id),
                &[delete],
                &[false]
            )
            .await
            .is_err()
        );
        assert!(writes.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn malformed_bundle_key_fails_closed_before_file_cascade_writes() -> Result<(), LixError>
    {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "bundle-key-corrupt";
        let mut malformed_key = branch_prefix(branch_id)?;
        malformed_key.extend_from_slice(b"not-a-canonical-bundle-key");
        let member_entity = EntityPk::single("malformed-key-member");
        let member_encoded = Bytes::from(encode_value(
            untracked("schema", None, &member_entity, r#"{"v":1}"#, timestamp()),
            timestamp(),
        )?);
        let mut member_bundle = BTreeMap::new();
        member_bundle.insert(
            None,
            BundleMember {
                value: decode_value(member_encoded.clone())?,
                encoded: member_encoded,
            },
        );
        let mut corrupt = storage.new_write_set();
        corrupt.put(
            UNTRACKED_ROW_SPACE,
            StorageKey(Bytes::from(malformed_key)),
            StorageValue {
                bytes: encode_bundle(&member_bundle)?,
            },
        );
        storage
            .commit_write_set(corrupt, StorageWriteOptions::default())
            .await
            .map_err(LixError::from)?;
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let mut writes = storage.new_write_set();
        let file = EntityPk::single("file-key-corrupt");
        let delete = file_delete(&file, timestamp());
        assert!(
            stage_untracked_deltas(
                &read,
                &mut writes,
                branch_id,
                test_control(branch_id),
                &[delete],
                &[false],
            )
            .await
            .is_err()
        );
        assert!(writes.is_empty());
        Ok(())
    }
}
