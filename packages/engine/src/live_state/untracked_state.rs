//! Branch-stable physical state for history-free rows.
//!
//! Untracked rows do not participate in commit history, merge, diff, working
//! diff, or generation rotation. Their canonical storage key is `(branch,
//! schema, entity)` and its value is authenticated metadata for a
//! content-addressed descriptor hierarchy over all file variants. Variant
//! payloads and descriptor nodes live in immutable chunks; deleting the final
//! member removes the root physically.

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
/// Immutable descriptor and payload chunks referenced by an authoritative
/// bundle root. A chunk is content addressed and has no entity/file lookup
/// semantics; the root is the sole owner of logical untracked facts.
pub(crate) const UNTRACKED_BUNDLE_CHUNK_SPACE: StorageSpace = StorageSpace::immutable(
    StorageSpaceId(0x0004_0034),
    "live_state.untracked_bundle_chunk.v1",
);
const VALUE_VERSION: u8 = 2;
// The physical key names one canonical entity, while this value contains only
// authenticated root metadata for that entity's descriptor hierarchy. The
// root is the sole untracked authority; nodes/chunks never supply logical
// identity.
const BUNDLE_ROOT_MAGIC: &[u8] = b"LXUB4";
const BUNDLE_NODE_LEAF_MAGIC: &[u8] = b"LXBNL4";
const BUNDLE_NODE_BRANCH_MAGIC: &[u8] = b"LXBNB4";
const BUNDLE_NODE_FANOUT: usize = 32;
const BUNDLE_LEAF_CAPACITY: usize = 32;
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
    pub(crate) descriptor_node_reads: u64,
    pub(crate) descriptor_node_writes: u64,
    pub(crate) descriptor_node_reused: u64,
    pub(crate) descriptor_splits: u64,
    pub(crate) descriptor_max_depth: u16,
    pub(crate) descriptor_node_bytes: u64,
}

#[cfg(test)]
thread_local! {
    static PREVIOUS_POINT_READ_KEYS: Cell<u64> = const { Cell::new(0) };
    static PREVIOUS_SCAN_ROWS: Cell<u64> = const { Cell::new(0) };
    static PREVIOUS_SCAN_BYTES: Cell<u64> = const { Cell::new(0) };
    static DESCRIPTOR_NODE_READS: Cell<u64> = const { Cell::new(0) };
    static DESCRIPTOR_NODE_WRITES: Cell<u64> = const { Cell::new(0) };
    static DESCRIPTOR_NODE_REUSED: Cell<u64> = const { Cell::new(0) };
    static DESCRIPTOR_SPLITS: Cell<u64> = const { Cell::new(0) };
    static DESCRIPTOR_MAX_DEPTH: Cell<u16> = const { Cell::new(0) };
    static DESCRIPTOR_NODE_BYTES: Cell<u64> = const { Cell::new(0) };
}

#[cfg(all(feature = "storage-benches", not(test)))]
static PREVIOUS_POINT_READ_KEYS: AtomicU64 = AtomicU64::new(0);
#[cfg(all(feature = "storage-benches", not(test)))]
static PREVIOUS_SCAN_ROWS: AtomicU64 = AtomicU64::new(0);
#[cfg(all(feature = "storage-benches", not(test)))]
static PREVIOUS_SCAN_BYTES: AtomicU64 = AtomicU64::new(0);
#[cfg(all(feature = "storage-benches", not(test)))]
static DESCRIPTOR_NODE_READS: AtomicU64 = AtomicU64::new(0);
#[cfg(all(feature = "storage-benches", not(test)))]
static DESCRIPTOR_NODE_WRITES: AtomicU64 = AtomicU64::new(0);
#[cfg(all(feature = "storage-benches", not(test)))]
static DESCRIPTOR_NODE_REUSED: AtomicU64 = AtomicU64::new(0);
#[cfg(all(feature = "storage-benches", not(test)))]
static DESCRIPTOR_SPLITS: AtomicU64 = AtomicU64::new(0);
#[cfg(all(feature = "storage-benches", not(test)))]
static DESCRIPTOR_MAX_DEPTH: AtomicU64 = AtomicU64::new(0);
#[cfg(all(feature = "storage-benches", not(test)))]
static DESCRIPTOR_NODE_BYTES: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn take_untracked_mutation_read_profile() -> UntrackedMutationReadProfile {
    UntrackedMutationReadProfile {
        previous_point_read_keys: PREVIOUS_POINT_READ_KEYS.with(|value| value.replace(0)),
        previous_scan_rows: PREVIOUS_SCAN_ROWS.with(|value| value.replace(0)),
        previous_scan_bytes: PREVIOUS_SCAN_BYTES.with(|value| value.replace(0)),
        descriptor_node_reads: DESCRIPTOR_NODE_READS.with(|value| value.replace(0)),
        descriptor_node_writes: DESCRIPTOR_NODE_WRITES.with(|value| value.replace(0)),
        descriptor_node_reused: DESCRIPTOR_NODE_REUSED.with(|value| value.replace(0)),
        descriptor_splits: DESCRIPTOR_SPLITS.with(|value| value.replace(0)),
        descriptor_max_depth: DESCRIPTOR_MAX_DEPTH.with(|value| value.replace(0)),
        descriptor_node_bytes: DESCRIPTOR_NODE_BYTES.with(|value| value.replace(0)),
    }
}

#[cfg(all(feature = "storage-benches", not(test)))]
#[allow(dead_code)]
pub(crate) fn take_untracked_mutation_read_profile() -> UntrackedMutationReadProfile {
    UntrackedMutationReadProfile {
        previous_point_read_keys: PREVIOUS_POINT_READ_KEYS.swap(0, Ordering::Relaxed),
        previous_scan_rows: PREVIOUS_SCAN_ROWS.swap(0, Ordering::Relaxed),
        previous_scan_bytes: PREVIOUS_SCAN_BYTES.swap(0, Ordering::Relaxed),
        descriptor_node_reads: DESCRIPTOR_NODE_READS.swap(0, Ordering::Relaxed),
        descriptor_node_writes: DESCRIPTOR_NODE_WRITES.swap(0, Ordering::Relaxed),
        descriptor_node_reused: DESCRIPTOR_NODE_REUSED.swap(0, Ordering::Relaxed),
        descriptor_splits: DESCRIPTOR_SPLITS.swap(0, Ordering::Relaxed),
        descriptor_max_depth: DESCRIPTOR_MAX_DEPTH.swap(0, Ordering::Relaxed) as u16,
        descriptor_node_bytes: DESCRIPTOR_NODE_BYTES.swap(0, Ordering::Relaxed),
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

#[cfg(test)]
fn record_descriptor_node_read() {
    DESCRIPTOR_NODE_READS.with(|value| value.set(value.get().saturating_add(1)));
}
#[cfg(all(feature = "storage-benches", not(test)))]
fn record_descriptor_node_read() {
    DESCRIPTOR_NODE_READS.fetch_add(1, Ordering::Relaxed);
}
#[cfg(not(any(test, feature = "storage-benches")))]
fn record_descriptor_node_read() {}

#[cfg(test)]
fn record_descriptor_node_write(bytes: usize) {
    DESCRIPTOR_NODE_WRITES.with(|value| value.set(value.get().saturating_add(1)));
    DESCRIPTOR_NODE_BYTES.with(|value| {
        value.set(
            value
                .get()
                .saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX)),
        )
    });
}
#[cfg(all(feature = "storage-benches", not(test)))]
fn record_descriptor_node_write(bytes: usize) {
    DESCRIPTOR_NODE_WRITES.fetch_add(1, Ordering::Relaxed);
    DESCRIPTOR_NODE_BYTES.fetch_add(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
}
#[cfg(not(any(test, feature = "storage-benches")))]
fn record_descriptor_node_write(_bytes: usize) {}

#[cfg(test)]
fn record_descriptor_reuse() {
    DESCRIPTOR_NODE_REUSED.with(|value| value.set(value.get().saturating_add(1)));
}
#[cfg(all(feature = "storage-benches", not(test)))]
fn record_descriptor_reuse() {
    DESCRIPTOR_NODE_REUSED.fetch_add(1, Ordering::Relaxed);
}
#[cfg(not(any(test, feature = "storage-benches")))]
fn record_descriptor_reuse() {}

#[cfg(test)]
fn record_descriptor_split() {
    DESCRIPTOR_SPLITS.with(|value| value.set(value.get().saturating_add(1)));
}
#[cfg(all(feature = "storage-benches", not(test)))]
fn record_descriptor_split() {
    DESCRIPTOR_SPLITS.fetch_add(1, Ordering::Relaxed);
}
#[cfg(not(any(test, feature = "storage-benches")))]
fn record_descriptor_split() {}

#[cfg(test)]
fn record_descriptor_depth(depth: u16) {
    DESCRIPTOR_MAX_DEPTH.with(|value| value.set(value.get().max(depth)));
}
#[cfg(not(any(test, feature = "storage-benches")))]
fn record_descriptor_depth(_depth: u16) {}
#[cfg(all(feature = "storage-benches", not(test)))]
fn record_descriptor_depth(depth: u16) {
    let mut previous = DESCRIPTOR_MAX_DEPTH.load(Ordering::Relaxed);
    while previous < u64::from(depth)
        && DESCRIPTOR_MAX_DEPTH
            .compare_exchange(
                previous,
                u64::from(depth),
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_err()
    {
        previous = DESCRIPTOR_MAX_DEPTH.load(Ordering::Relaxed);
    }
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
#[cfg(test)]
async fn read_untracked_bundles(
    store: &(impl StorageAdapterRead + ?Sized),
    keys: &[StorageKey],
) -> Result<BTreeMap<StorageKey, DecodedBundle>, LixError> {
    let roots = read_untracked_bundle_roots(store, keys).await?;
    resolve_untracked_bundle_roots(store, roots).await
}

#[cfg(test)]
async fn read_untracked_bundle_roots(
    store: &(impl StorageAdapterRead + ?Sized),
    keys: &[StorageKey],
) -> Result<BTreeMap<StorageKey, DescriptorMap>, LixError> {
    let metas = read_untracked_bundle_root_metas(store, keys).await?;
    let mut roots = BTreeMap::new();
    for (key, meta) in metas {
        roots.insert(key, load_descriptor_tree(store, &meta).await?);
    }
    Ok(roots)
}

async fn resolve_untracked_bundle_roots(
    store: &(impl StorageAdapterRead + ?Sized),
    roots: BTreeMap<StorageKey, DescriptorMap>,
) -> Result<BTreeMap<StorageKey, DecodedBundle>, LixError> {
    if roots.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut chunk_keys = BTreeSet::new();
    for root in roots.values() {
        for descriptor in root.values() {
            chunk_keys.insert(StorageKey(Bytes::copy_from_slice(&descriptor.hash)));
        }
    }
    let chunk_keys = chunk_keys.into_iter().collect::<Vec<_>>();
    let chunk_values = if chunk_keys.is_empty() {
        Vec::new()
    } else {
        #[cfg(any(test, feature = "storage-benches"))]
        record_previous_point_read_keys(chunk_keys.len());
        let values =
            PointReadPlan::from_unique_keys(UNTRACKED_BUNDLE_CHUNK_SPACE, chunk_keys.clone())
                .materialize(store, StorageGetOptions::default())
                .await?
                .value;
        let mut decoded = Vec::with_capacity(values.len());
        for (key, value) in chunk_keys.iter().zip(values) {
            let Some(value) = value else {
                return Err(codec_error(
                    "untracked bundle root references a missing chunk",
                ));
            };
            let StorageProjectedValue::FullValue(value) = value else {
                return Err(codec_error("untracked bundle chunk read omitted its value"));
            };
            if value.len() > u32::MAX as usize || blake3::hash(&value).as_bytes() != key.0.as_ref()
            {
                return Err(codec_error("untracked bundle chunk hash validation failed"));
            }
            decoded.push(value);
        }
        decoded
    };
    let chunk_by_hash = chunk_keys
        .into_iter()
        .zip(chunk_values)
        .map(|(key, value)| {
            let hash: [u8; 32] = key.0.as_ref().try_into().expect("chunk key is a hash");
            (hash, value)
        })
        .collect::<BTreeMap<_, _>>();
    let mut bundles = BTreeMap::new();
    for (key, root) in roots {
        let mut bundle = BTreeMap::new();
        for (file_id, descriptor) in root {
            let encoded = chunk_by_hash
                .get(&descriptor.hash)
                .ok_or_else(|| codec_error("untracked bundle root references an unknown chunk"))?;
            if encoded.len() != descriptor.len as usize {
                return Err(codec_error(
                    "untracked bundle chunk length validation failed",
                ));
            }
            let encoded = encoded.clone();
            let value = decode_value(encoded.clone())?;
            bundle.insert(file_id, BundleMember { encoded, value });
        }
        bundles.insert(key, bundle);
    }
    Ok(bundles)
}

/// Resolve only the variants named by a mutation/cascade plan. The root
/// remains complete, while payload reads are proportional to touched file
/// identities rather than the number of variants already present.
async fn resolve_selected_bundle_members(
    store: &(impl StorageAdapterRead + ?Sized),
    roots: &BTreeMap<StorageKey, DescriptorMap>,
    requested: &BTreeMap<StorageKey, BTreeSet<Option<String>>>,
) -> Result<BTreeMap<(StorageKey, Option<String>), BundleMember>, LixError> {
    let mut descriptors = BTreeMap::<[u8; 32], ChunkDescriptor>::new();
    for (key, files) in requested {
        let Some(root) = roots.get(key) else { continue };
        for file_id in files {
            let Some(descriptor) = root.get(file_id) else {
                continue;
            };
            descriptors
                .entry(descriptor.hash)
                .or_insert_with(|| descriptor.clone());
        }
    }
    if descriptors.is_empty() {
        return Ok(BTreeMap::new());
    }
    let chunk_keys = descriptors
        .keys()
        .map(|hash| StorageKey(Bytes::copy_from_slice(hash)))
        .collect::<Vec<_>>();
    #[cfg(any(test, feature = "storage-benches"))]
    record_previous_point_read_keys(chunk_keys.len());
    let values = PointReadPlan::from_unique_keys(UNTRACKED_BUNDLE_CHUNK_SPACE, chunk_keys.clone())
        .materialize(store, StorageGetOptions::default())
        .await?
        .value;
    let mut chunks = BTreeMap::<[u8; 32], Bytes>::new();
    for (key, value) in chunk_keys.into_iter().zip(values) {
        let Some(value) = value else {
            return Err(codec_error(
                "untracked bundle root references a missing chunk",
            ));
        };
        let StorageProjectedValue::FullValue(value) = value else {
            return Err(codec_error("untracked bundle chunk read omitted its value"));
        };
        let hash: [u8; 32] = key.0.as_ref().try_into().expect("chunk key is a hash");
        if value.len() != descriptors[&hash].len as usize
            || *blake3::hash(&value).as_bytes() != hash
        {
            return Err(codec_error("untracked bundle chunk validation failed"));
        }
        chunks.insert(hash, value);
    }
    let mut members = BTreeMap::new();
    for (key, files) in requested {
        let Some(root) = roots.get(key) else { continue };
        for file_id in files {
            let Some(descriptor) = root.get(file_id) else {
                continue;
            };
            let encoded = chunks
                .get(&descriptor.hash)
                .ok_or_else(|| codec_error("untracked bundle chunk was not loaded"))?
                .clone();
            if encoded.len() != descriptor.len as usize {
                return Err(codec_error(
                    "untracked bundle selected descriptor length validation failed",
                ));
            }
            let value = decode_value(encoded.clone())?;
            members.insert(
                (key.clone(), file_id.clone()),
                BundleMember { encoded, value },
            );
        }
    }
    Ok(members)
}

async fn scan_all_untracked_bundles(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
) -> Result<BTreeMap<StorageKey, DescriptorMap>, LixError> {
    let prefix = branch_prefix(branch_id)?;
    let plan = ScanPlan::prefix(
        UNTRACKED_ROW_SPACE,
        StoragePrefix {
            bytes: Bytes::copy_from_slice(&prefix),
        },
    );
    let mut resume_after = None;
    let mut roots = BTreeMap::new();
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
            let root = decode_bundle_root(&entry.key.0, value)?;
            roots.insert(entry.key.clone(), load_descriptor_tree(store, &root).await?);
        }
        if !page.value.has_more {
            break;
        }
        resume_after = next_cursor;
    }
    Ok(roots)
}

/// Validate every bundle in a branch cascade scan, retaining only bundles
/// containing one of the retired file identities.  The scan is still the
/// sole authority proof and remains O(N) in time, but successful staging keeps
/// only the affected K bundles rather than materializing the whole branch.
async fn scan_untracked_bundles_for_file_cascade(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    deleted_file_ids: &BTreeSet<String>,
) -> Result<BTreeMap<StorageKey, DescriptorMap>, LixError> {
    let prefix = branch_prefix(branch_id)?;
    let plan = ScanPlan::prefix(
        UNTRACKED_ROW_SPACE,
        StoragePrefix {
            bytes: Bytes::copy_from_slice(&prefix),
        },
    );
    let mut resume_after = None;
    let mut affected_roots = BTreeMap::new();
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
            let root = decode_bundle_root(&entry.key.0, value)?;
            let mut contains = false;
            for file_id in deleted_file_ids {
                let target = Some(file_id.clone());
                if lookup_descriptor(store, &root, &target).await?.is_some() {
                    contains = true;
                    break;
                }
            }
            if contains {
                affected_roots.insert(entry.key, load_descriptor_tree(store, &root).await?);
            }
        }
        if !page.value.has_more {
            break;
        }
        resume_after = next_cursor;
    }
    Ok(affected_roots)
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
    let keys = keys.into_iter().collect::<Vec<_>>();
    let roots = read_untracked_bundle_root_metas(store, &keys).await?;
    let mut requested_members = BTreeMap::new();
    let mut dense_roots = BTreeMap::new();
    for (key, root) in &roots {
        let mut files = BTreeSet::new();
        let needs_dense = request.filter.file_ids.is_empty()
            || request
                .filter
                .file_ids
                .iter()
                .any(|filter| matches!(filter, NullableKeyFilter::Any));
        if needs_dense {
            let materialized = load_descriptor_tree(store, root).await?;
            files.extend(materialized.keys().cloned());
            dense_roots.insert(key.clone(), materialized);
        } else {
            for filter in &request.filter.file_ids {
                let target = match filter {
                    NullableKeyFilter::Null => None,
                    NullableKeyFilter::Value(value) => Some(value.clone()),
                    NullableKeyFilter::Any => unreachable!("handled above"),
                };
                if lookup_descriptor(store, root, &target).await?.is_some() {
                    files.insert(target);
                }
            }
        }
        requested_members.insert(key.clone(), files);
    }
    let members = if dense_roots.len() == roots.len() {
        resolve_selected_bundle_members(store, &dense_roots, &requested_members).await?
    } else {
        let descriptors =
            load_selected_bundle_descriptors(store, &roots, &requested_members).await?;
        resolve_selected_bundle_members_from_descriptors(store, &descriptors).await?
    };
    let mut decoded = Vec::new();
    for (key, files) in requested_members {
        let identity = decode_bundle_key(&key.0)?;
        for file_id in files {
            let Some(member) = members.get(&(key.clone(), file_id.clone())) else {
                continue;
            };
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
                    value: member.value.clone(),
                });
            }
        }
    }
    materialize_rows(store, decoded).await
}

async fn load_selected_bundle_descriptors(
    store: &(impl StorageAdapterRead + ?Sized),
    roots: &BTreeMap<StorageKey, BundleRootMeta>,
    requested: &BTreeMap<StorageKey, BTreeSet<Option<String>>>,
) -> Result<BTreeMap<(StorageKey, Option<String>), ChunkDescriptor>, LixError> {
    let mut descriptors = BTreeMap::new();
    for (key, files) in requested {
        let Some(root) = roots.get(key) else { continue };
        for file_id in files {
            if let Some(descriptor) = lookup_descriptor(store, root, file_id).await? {
                descriptors.insert((key.clone(), file_id.clone()), descriptor);
            }
        }
    }
    Ok(descriptors)
}

async fn resolve_selected_bundle_members_from_descriptors(
    store: &(impl StorageAdapterRead + ?Sized),
    descriptors: &BTreeMap<(StorageKey, Option<String>), ChunkDescriptor>,
) -> Result<BTreeMap<(StorageKey, Option<String>), BundleMember>, LixError> {
    let mut by_hash = BTreeMap::<[u8; 32], ChunkDescriptor>::new();
    for descriptor in descriptors.values() {
        by_hash
            .entry(descriptor.hash)
            .or_insert_with(|| descriptor.clone());
    }
    let keys = by_hash
        .keys()
        .map(|hash| StorageKey(Bytes::copy_from_slice(hash)))
        .collect::<Vec<_>>();
    if keys.is_empty() {
        return Ok(BTreeMap::new());
    }
    #[cfg(any(test, feature = "storage-benches"))]
    record_previous_point_read_keys(keys.len());
    let values = PointReadPlan::from_unique_keys(UNTRACKED_BUNDLE_CHUNK_SPACE, keys.clone())
        .materialize(store, StorageGetOptions::default())
        .await?
        .value;
    let mut chunks = BTreeMap::new();
    for (key, value) in keys.into_iter().zip(values) {
        let Some(value) = value else {
            return Err(codec_error("untracked bundle selected chunk is missing"));
        };
        let StorageProjectedValue::FullValue(value) = value else {
            return Err(codec_error(
                "untracked bundle selected chunk omitted its value",
            ));
        };
        let hash: [u8; 32] = key.0.as_ref().try_into().expect("chunk hash is 32 bytes");
        let descriptor = by_hash
            .get(&hash)
            .ok_or_else(|| codec_error("untracked selected chunk descriptor disappeared"))?;
        if value.len() != descriptor.len as usize || *blake3::hash(&value).as_bytes() != hash {
            return Err(codec_error("untracked selected chunk validation failed"));
        }
        chunks.insert(hash, value);
    }
    let mut members = BTreeMap::new();
    for (identity, descriptor) in descriptors {
        let encoded = chunks
            .get(&descriptor.hash)
            .ok_or_else(|| codec_error("untracked selected chunk was not loaded"))?
            .clone();
        if encoded.len() != descriptor.len as usize {
            return Err(codec_error(
                "untracked selected descriptor length validation failed",
            ));
        }
        let value = decode_value(encoded.clone())?;
        members.insert(identity.clone(), BundleMember { encoded, value });
    }
    Ok(members)
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
    let mut root_metas = read_untracked_bundle_root_metas(store, &key_vec).await?;
    let mut changed_keys = keys.clone();

    let mut requested_members = BTreeMap::<StorageKey, BTreeSet<Option<String>>>::new();
    for delta in deltas.iter().filter(|delta| delta.untracked) {
        let key = StorageKey(Bytes::from(encode_bundle_key(
            branch_id,
            delta.schema_key,
            delta.entity_pk,
        )?));
        requested_members
            .entry(key)
            .or_default()
            .insert(delta.file_id.map(str::to_owned));
    }

    let mut cascade_roots = BTreeMap::new();

    // Validate the caller's absence proof against the exact bundle member.
    for (delta, absent) in deltas.iter().zip(known_absent).filter(|(d, _)| d.untracked) {
        let key = StorageKey(Bytes::from(encode_bundle_key(
            branch_id,
            delta.schema_key,
            delta.entity_pk,
        )?));
        let file_id = delta.file_id.map(str::to_owned);
        let present = if let Some(root) = root_metas.get(&key) {
            lookup_descriptor(store, root, &file_id).await?.is_some()
        } else {
            false
        };
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
        for (key, root) in
            scan_untracked_bundles_for_file_cascade(store, branch_id, &deleted_file_ids).await?
        {
            let files = deleted_file_ids
                .iter()
                .filter(|file_id| root.contains_key(&Some((*file_id).clone())))
                .map(|file_id| Some(file_id.clone()))
                .collect::<BTreeSet<_>>();
            requested_members
                .entry(key.clone())
                .or_default()
                .extend(files);
            cascade_roots.insert(key, root);
        }
    }

    let cascade_keys = cascade_roots.keys().cloned().collect::<BTreeSet<_>>();
    let additional_keys = cascade_keys.difference(&keys).cloned().collect::<Vec<_>>();
    if !additional_keys.is_empty() {
        root_metas.extend(read_untracked_bundle_root_metas(store, &additional_keys).await?);
    }
    changed_keys.extend(cascade_keys.clone());
    let previous_descriptors =
        load_selected_bundle_descriptors(store, &root_metas, &requested_members).await?;
    let mut members =
        resolve_selected_bundle_members_from_descriptors(store, &previous_descriptors).await?;

    let mut retired_refs = BTreeSet::new();
    let mut mutations =
        BTreeMap::<StorageKey, BTreeMap<Option<String>, Option<ChunkDescriptor>>>::new();
    let mut new_payloads = BTreeMap::<[u8; 32], Bytes>::new();
    for delta in deltas.iter().filter(|delta| delta.untracked) {
        let key = StorageKey(Bytes::from(encode_bundle_key(
            branch_id,
            delta.schema_key,
            delta.entity_pk,
        )?));
        let file_id = delta.file_id.map(str::to_owned);
        let previous = members.remove(&(key.clone(), file_id.clone()));
        if let Some(previous) = &previous {
            collect_value_refs(&previous.value, &mut retired_refs);
        }
        if delta.deleted {
            mutations.entry(key).or_default().insert(file_id, None);
        } else {
            let created_at = previous
                .as_ref()
                .map(|member| member.value.created_at)
                .unwrap_or(delta.created_at);
            let encoded = Bytes::from(encode_value(*delta, created_at)?);
            let value = decode_value(encoded.clone())?;
            let descriptor = chunk_descriptor(&encoded)?;
            new_payloads.insert(descriptor.hash, encoded.clone());
            mutations
                .entry(key.clone())
                .or_default()
                .insert(file_id.clone(), Some(descriptor));
            members.insert((key, file_id), BundleMember { encoded, value });
        }
    }
    for (key, root) in &cascade_roots {
        for file_id in deleted_file_ids.iter() {
            let member_key = (key.clone(), Some(file_id.clone()));
            if root.contains_key(&Some(file_id.clone())) {
                if let Some(member) = members.remove(&member_key) {
                    collect_value_refs(&member.value, &mut retired_refs);
                }
                mutations
                    .entry(key.clone())
                    .or_default()
                    .insert(Some(file_id.clone()), None);
            }
        }
    }

    if drop_branch {
        let existing = scan_all_untracked_bundles(store, branch_id).await?;
        // Branch deletion is a destructive lifecycle operation. Validate every
        // referenced immutable chunk before staging any root delete so a
        // missing/corrupt payload cannot be hidden by deleting its root.
        let _validated = resolve_untracked_bundle_roots(store, existing.clone()).await?;
        for key in existing.keys() {
            writes.delete(UNTRACKED_ROW_SPACE, key.clone());
        }
    } else {
        let mut new_nodes = BTreeMap::<[u8; 32], Bytes>::new();
        for key in changed_keys {
            let Some(ops) = mutations.remove(&key) else {
                continue;
            };
            let mut editor = DescriptorEditor::new(store, root_metas.get(&key).cloned());
            for (file_id, descriptor) in ops {
                editor.update(file_id, descriptor).await?;
            }
            for (hash, bytes) in editor.staged {
                new_nodes.insert(hash, bytes);
            }
            if let Some(root) = editor.root {
                let root_bytes = encode_bundle_root(&key.0, &root)?;
                writes.put(UNTRACKED_ROW_SPACE, key, StorageValue { bytes: root_bytes });
            } else {
                writes.delete(UNTRACKED_ROW_SPACE, key);
            }
        }
        for (hash, bytes) in new_payloads.into_iter().chain(new_nodes) {
            writes.put(
                UNTRACKED_BUNDLE_CHUNK_SPACE,
                StorageKey(Bytes::copy_from_slice(&hash)),
                bytes.to_vec(),
            );
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
    let roots = read_untracked_bundle_root_metas(store, &keys).await?;
    let mut requested_members = BTreeMap::<StorageKey, BTreeSet<Option<String>>>::new();
    for (row, requested) in request.rows.iter().zip(&requested_keys) {
        if let Some((branch_key, global_key)) = requested {
            requested_members
                .entry(branch_key.clone())
                .or_default()
                .insert(row.file_id.clone());
            if let Some(global_key) = global_key {
                requested_members
                    .entry(global_key.clone())
                    .or_default()
                    .insert(row.file_id.clone());
            }
        }
    }
    let descriptors = load_selected_bundle_descriptors(store, &roots, &requested_members).await?;
    let members = resolve_selected_bundle_members_from_descriptors(store, &descriptors).await?;
    let mut decoded = Vec::new();
    let mut selections = Vec::with_capacity(request.rows.len());
    for (row, requested) in request.rows.iter().zip(requested_keys) {
        let Some((branch_key, global_key)) = requested else {
            selections.push(None);
            continue;
        };
        let file_id = row.file_id.clone();
        let (chosen_key, branch_override, member) =
            if let Some(member) = members.get(&(branch_key.clone(), file_id.clone())) {
                (branch_key, None, member)
            } else if let Some(global_key) = global_key
                && let Some(member) = members.get(&(global_key.clone(), file_id.clone()))
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
        let mut page_roots = BTreeMap::new();
        for entry in page.value.entries {
            #[cfg(any(test, feature = "storage-benches"))]
            record_previous_scan_row(
                entry.key.0.len()
                    + match &entry.value {
                        StorageProjectedValue::FullValue(value) => value.len(),
                        StorageProjectedValue::KeyOnly => 0,
                    },
            );
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
            page_roots.insert(entry.key.clone(), decode_bundle_root(&entry.key.0, value)?);
        }
        let mut descriptor_roots = BTreeMap::new();
        for (key, root) in page_roots {
            descriptor_roots.insert(key, load_descriptor_tree(store, &root).await?);
        }
        for bundle in resolve_untracked_bundle_roots(store, descriptor_roots)
            .await?
            .values()
        {
            for member in bundle.values() {
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

/// Sweep immutable variant chunks against the pinned authoritative roots.
/// Every root is checked for a controlled branch, every reachable chunk must
/// exist and validate, and only then are orphan deletes staged. This is a
/// derived-payload maintenance operation; roots remain the sole authority.
pub(crate) async fn stage_untracked_chunk_gc(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    controlled_branches: &BTreeSet<String>,
) -> Result<(), LixError> {
    let root_plan = ScanPlan::prefix(
        UNTRACKED_ROW_SPACE,
        StoragePrefix {
            bytes: Bytes::new(),
        },
    );
    let mut root_cursor = None;
    let mut reachable = BTreeSet::<[u8; 32]>::new();
    let mut descriptor_cache = BTreeMap::new();
    loop {
        let page = root_plan
            .collect(
                store,
                StorageScanOptions {
                    resume_after: root_cursor.clone(),
                    ..StorageScanOptions::default()
                },
            )
            .await?;
        let next = validate_scan_page_progress(
            &[],
            root_cursor.as_ref(),
            page.value.entries.iter().map(|entry| &entry.key),
            page.value.has_more,
        )?;
        for entry in page.value.entries {
            #[cfg(any(test, feature = "storage-benches"))]
            record_previous_scan_row(
                entry.key.0.len()
                    + match &entry.value {
                        StorageProjectedValue::FullValue(value) => value.len(),
                        StorageProjectedValue::KeyOnly => 0,
                    },
            );
            let identity = decode_bundle_key(&entry.key.0)?;
            if !controlled_branches.contains(&identity.branch_id) {
                return Err(codec_error(format!(
                    "untracked chunk GC encountered orphan branch '{}'",
                    identity.branch_id
                )));
            }
            let StorageProjectedValue::FullValue(value) = entry.value else {
                return Err(codec_error("untracked chunk GC root omitted its value"));
            };
            let root = decode_bundle_root(&entry.key.0, value)?;
            // Validation is per root: a content-addressed node may be shared
            // by multiple roots, and skipping it globally would hide a bad
            // count/depth relationship in a later root.
            let mut descriptor_visited = BTreeSet::new();
            collect_descriptor_tree_hashes(
                store,
                &root,
                &mut reachable,
                &mut descriptor_cache,
                &mut descriptor_visited,
            )
            .await?;
        }
        if !page.value.has_more {
            break;
        }
        root_cursor = next;
    }

    let chunk_plan = ScanPlan::prefix(
        UNTRACKED_BUNDLE_CHUNK_SPACE,
        StoragePrefix {
            bytes: Bytes::new(),
        },
    );
    let mut chunk_cursor = None;
    let mut seen = BTreeSet::<[u8; 32]>::new();
    let mut orphaned = Vec::new();
    loop {
        let page = chunk_plan
            .collect(
                store,
                StorageScanOptions {
                    resume_after: chunk_cursor.clone(),
                    ..StorageScanOptions::default()
                },
            )
            .await?;
        let next = validate_scan_page_progress(
            &[],
            chunk_cursor.as_ref(),
            page.value.entries.iter().map(|entry| &entry.key),
            page.value.has_more,
        )?;
        for entry in page.value.entries {
            #[cfg(any(test, feature = "storage-benches"))]
            record_previous_scan_row(
                entry.key.0.len()
                    + match &entry.value {
                        StorageProjectedValue::FullValue(value) => value.len(),
                        StorageProjectedValue::KeyOnly => 0,
                    },
            );
            if entry.key.0.len() != 32 {
                return Err(codec_error("untracked bundle chunk key is not a hash"));
            }
            let hash: [u8; 32] = entry.key.0.as_ref().try_into().expect("32-byte chunk key");
            let StorageProjectedValue::FullValue(value) = entry.value else {
                return Err(codec_error("untracked bundle chunk GC omitted its value"));
            };
            if *blake3::hash(&value).as_bytes() != hash {
                return Err(codec_error(
                    "untracked bundle chunk GC hash validation failed",
                ));
            }
            seen.insert(hash);
            if !reachable.contains(&hash) {
                orphaned.push(hash);
            }
        }
        if !page.value.has_more {
            break;
        }
        chunk_cursor = next;
    }
    if reachable.iter().any(|hash| !seen.contains(hash)) {
        return Err(codec_error(
            "untracked bundle GC found a root with a missing chunk",
        ));
    }
    for hash in orphaned {
        writes.delete(
            UNTRACKED_BUNDLE_CHUNK_SPACE,
            StorageKey(Bytes::copy_from_slice(&hash)),
        );
    }
    Ok(())
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
        let mut page_roots = BTreeMap::new();
        for entry in page.value.entries {
            let StorageProjectedValue::FullValue(value) = entry.value else {
                return Err(codec_error("untracked bundle scan omitted its value"));
            };
            let identity = decode_bundle_key(&entry.key.0)?;
            page_roots.insert(
                entry.key.clone(),
                (identity, decode_bundle_root(&entry.key.0, value)?),
            );
        }
        let roots = page_roots.iter().map(|(key, (_, root))| async move {
            Ok::<_, LixError>((key.clone(), load_descriptor_tree(store, root).await?))
        });
        let roots = futures_util::future::try_join_all(roots)
            .await?
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        let bundles = resolve_untracked_bundle_roots(store, roots).await?;
        for (key, (identity, _)) in page_roots {
            let bundle = bundles
                .get(&key)
                .ok_or_else(|| codec_error("untracked bundle page root disappeared"))?;
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
                        value: member.value.clone(),
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
    #[allow(dead_code)]
    encoded: Bytes,
    value: DecodedValue,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ChunkDescriptor {
    hash: [u8; 32],
    len: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BundleRootMeta {
    node_hash: [u8; 32],
    count: u32,
    depth: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DescriptorChild {
    first: Option<String>,
    hash: [u8; 32],
    count: u32,
    depth: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum DescriptorNode {
    Leaf(Vec<(Option<String>, ChunkDescriptor)>),
    Branch(Vec<DescriptorChild>),
}

type DescriptorMap = BTreeMap<Option<String>, ChunkDescriptor>;

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

fn encode_file_id(out: &mut Vec<u8>, file_id: &Option<String>) -> Result<(), LixError> {
    match file_id {
        None => out.push(0),
        Some(file_id) => {
            out.push(1);
            push_text(out, file_id)?;
        }
    }
    Ok(())
}

fn decode_file_id(
    bytes: &Bytes,
    offset: &mut usize,
    context: &str,
) -> Result<Option<String>, LixError> {
    match take(bytes, offset, 1, context)?[0] {
        0 => Ok(None),
        1 => Ok(Some(read_text(bytes, offset, context)?)),
        _ => Err(codec_error(format!("{context} has an invalid file tag"))),
    }
}

fn ensure_strict_file_order(
    previous: Option<&Option<String>>,
    current: &Option<String>,
    context: &str,
) -> Result<(), LixError> {
    if previous.is_some_and(|previous| previous >= current) {
        return Err(codec_error(format!("{context} are not strictly sorted")));
    }
    Ok(())
}

fn encode_bundle_root_meta(binding: &[u8], root: &BundleRootMeta) -> Result<Bytes, LixError> {
    if root.count == 0 || root.node_hash == [0; 32] {
        return Err(codec_error("untracked bundle root has no descriptor node"));
    }
    let mut body = Vec::with_capacity(4 + 2 + 32);
    body.extend_from_slice(&root.count.to_be_bytes());
    body.extend_from_slice(&root.depth.to_be_bytes());
    body.extend_from_slice(&root.node_hash);
    let mut out = Vec::with_capacity(BUNDLE_ROOT_MAGIC.len() + body.len() + 32);
    out.extend_from_slice(BUNDLE_ROOT_MAGIC);
    out.extend_from_slice(&body);
    let mut digest_input = Vec::with_capacity(binding.len() + body.len());
    digest_input.extend_from_slice(binding);
    digest_input.extend_from_slice(&body);
    out.extend_from_slice(blake3::hash(&digest_input).as_bytes());
    Ok(Bytes::from(out))
}

fn decode_bundle_root_meta(binding: &[u8], bytes: Bytes) -> Result<BundleRootMeta, LixError> {
    if !bytes.starts_with(BUNDLE_ROOT_MAGIC) {
        return Err(codec_error(
            "untracked bundle root has an unsupported format",
        ));
    }
    let mut offset = BUNDLE_ROOT_MAGIC.len();
    let count = u32::from_be_bytes(
        take(&bytes, &mut offset, 4, "bundle root descriptor count")?
            .try_into()
            .expect("four bytes"),
    );
    let depth = u16::from_be_bytes(
        take(&bytes, &mut offset, 2, "bundle root descriptor depth")?
            .try_into()
            .expect("two bytes"),
    );
    let node_hash: [u8; 32] = take(&bytes, &mut offset, 32, "bundle root descriptor hash")?
        .try_into()
        .expect("32 bytes");
    if count == 0 || node_hash == [0; 32] {
        return Err(codec_error(
            "untracked bundle root has an empty descriptor tree",
        ));
    }
    let digest_start = offset;
    let expected: [u8; 32] = take(&bytes, &mut offset, 32, "bundle root digest")?
        .try_into()
        .expect("32 bytes");
    if offset != bytes.len() {
        return Err(codec_error("untracked bundle root has trailing bytes"));
    }
    let mut digest_input =
        Vec::with_capacity(binding.len() + digest_start - BUNDLE_ROOT_MAGIC.len());
    digest_input.extend_from_slice(binding);
    digest_input.extend_from_slice(&bytes[BUNDLE_ROOT_MAGIC.len()..digest_start]);
    if *blake3::hash(&digest_input).as_bytes() != expected {
        return Err(codec_error(
            "untracked bundle root digest validation failed",
        ));
    }
    Ok(BundleRootMeta {
        node_hash,
        count,
        depth,
    })
}

fn encode_descriptor_node(node: &DescriptorNode) -> Result<Bytes, LixError> {
    let mut out = Vec::new();
    match node {
        DescriptorNode::Leaf(entries) => {
            if entries.is_empty() || entries.len() > BUNDLE_LEAF_CAPACITY {
                return Err(codec_error("descriptor leaf has an invalid entry count"));
            }
            out.extend_from_slice(BUNDLE_NODE_LEAF_MAGIC);
            out.extend_from_slice(
                &u32::try_from(entries.len())
                    .map_err(|_| codec_error("descriptor leaf is too large"))?
                    .to_be_bytes(),
            );
            let mut previous = None;
            for (file_id, descriptor) in entries {
                ensure_strict_file_order(previous.as_ref(), file_id, "descriptor leaf entries")?;
                if descriptor.len == 0 || descriptor.hash == [0; 32] {
                    return Err(codec_error(
                        "descriptor leaf contains an invalid chunk descriptor",
                    ));
                }
                encode_file_id(&mut out, file_id)?;
                out.extend_from_slice(&descriptor.hash);
                out.extend_from_slice(&descriptor.len.to_be_bytes());
                previous = Some(file_id.clone());
            }
        }
        DescriptorNode::Branch(children) => {
            if children.len() < 2 || children.len() > BUNDLE_NODE_FANOUT {
                return Err(codec_error("descriptor branch has an invalid child count"));
            }
            out.extend_from_slice(BUNDLE_NODE_BRANCH_MAGIC);
            out.extend_from_slice(
                &u32::try_from(children.len())
                    .map_err(|_| codec_error("descriptor branch is too large"))?
                    .to_be_bytes(),
            );
            let mut previous = None;
            let expected_depth = children[0].depth;
            let mut total = 0_u64;
            for child in children {
                if child.depth != expected_depth || child.count == 0 || child.hash == [0; 32] {
                    return Err(codec_error(
                        "descriptor branch child metadata is inconsistent",
                    ));
                }
                ensure_strict_file_order(
                    previous.as_ref(),
                    &child.first,
                    "descriptor branch keys",
                )?;
                encode_file_id(&mut out, &child.first)?;
                out.extend_from_slice(&child.hash);
                out.extend_from_slice(&child.count.to_be_bytes());
                out.extend_from_slice(&child.depth.to_be_bytes());
                total = total
                    .checked_add(u64::from(child.count))
                    .ok_or_else(|| codec_error("descriptor branch count overflow"))?;
                previous = Some(child.first.clone());
            }
            if total > u64::from(u32::MAX) {
                return Err(codec_error("descriptor branch count overflow"));
            }
        }
    }
    Ok(Bytes::from(out))
}

fn decode_descriptor_node(bytes: Bytes) -> Result<DescriptorNode, LixError> {
    let mut offset = 0;
    if bytes.starts_with(BUNDLE_NODE_LEAF_MAGIC) {
        offset += BUNDLE_NODE_LEAF_MAGIC.len();
        let count = u32::from_be_bytes(
            take(&bytes, &mut offset, 4, "descriptor leaf count")?
                .try_into()
                .expect("four bytes"),
        ) as usize;
        if count == 0 || count > BUNDLE_LEAF_CAPACITY {
            return Err(codec_error("descriptor leaf has an invalid entry count"));
        }
        let mut entries = Vec::with_capacity(count);
        let mut previous = None;
        for _ in 0..count {
            let file_id = decode_file_id(&bytes, &mut offset, "descriptor leaf file")?;
            ensure_strict_file_order(previous.as_ref(), &file_id, "descriptor leaf entries")?;
            let hash: [u8; 32] = take(&bytes, &mut offset, 32, "descriptor leaf hash")?
                .try_into()
                .expect("32 bytes");
            let len = u32::from_be_bytes(
                take(&bytes, &mut offset, 4, "descriptor leaf length")?
                    .try_into()
                    .expect("four bytes"),
            );
            if len == 0 || hash == [0; 32] {
                return Err(codec_error(
                    "descriptor leaf contains an invalid chunk descriptor",
                ));
            }
            entries.push((file_id.clone(), ChunkDescriptor { hash, len }));
            previous = Some(file_id);
        }
        if offset != bytes.len() {
            return Err(codec_error("descriptor leaf has trailing bytes"));
        }
        return Ok(DescriptorNode::Leaf(entries));
    }
    if bytes.starts_with(BUNDLE_NODE_BRANCH_MAGIC) {
        offset += BUNDLE_NODE_BRANCH_MAGIC.len();
        let count = u32::from_be_bytes(
            take(&bytes, &mut offset, 4, "descriptor branch count")?
                .try_into()
                .expect("four bytes"),
        ) as usize;
        if count < 2 || count > BUNDLE_NODE_FANOUT {
            return Err(codec_error("descriptor branch has an invalid child count"));
        }
        let mut children = Vec::with_capacity(count);
        let mut previous = None;
        let mut expected_depth = None;
        for _ in 0..count {
            let first = decode_file_id(&bytes, &mut offset, "descriptor branch key")?;
            ensure_strict_file_order(previous.as_ref(), &first, "descriptor branch keys")?;
            let hash: [u8; 32] = take(&bytes, &mut offset, 32, "descriptor branch hash")?
                .try_into()
                .expect("32 bytes");
            let child_count = u32::from_be_bytes(
                take(&bytes, &mut offset, 4, "descriptor branch child count")?
                    .try_into()
                    .expect("four bytes"),
            );
            let depth = u16::from_be_bytes(
                take(&bytes, &mut offset, 2, "descriptor branch child depth")?
                    .try_into()
                    .expect("two bytes"),
            );
            if hash == [0; 32] || child_count == 0 {
                return Err(codec_error("descriptor branch child metadata is invalid"));
            }
            if let Some(expected) = expected_depth {
                if expected != depth {
                    return Err(codec_error("descriptor branch child depths differ"));
                }
            } else {
                expected_depth = Some(depth);
            }
            children.push(DescriptorChild {
                first: first.clone(),
                hash,
                count: child_count,
                depth,
            });
            previous = Some(first);
        }
        if offset != bytes.len() {
            return Err(codec_error("descriptor branch has trailing bytes"));
        }
        return Ok(DescriptorNode::Branch(children));
    }
    Err(codec_error("descriptor node has an unsupported format"))
}

fn descriptor_node_ref(node: &DescriptorNode) -> Result<DescriptorChild, LixError> {
    let bytes = encode_descriptor_node(node)?;
    let hash = *blake3::hash(&bytes).as_bytes();
    match node {
        DescriptorNode::Leaf(entries) => Ok(DescriptorChild {
            first: entries
                .first()
                .map(|(file_id, _)| file_id.clone())
                .ok_or_else(|| codec_error("descriptor leaf cannot be empty"))?,
            hash,
            count: u32::try_from(entries.len())
                .map_err(|_| codec_error("descriptor count overflow"))?,
            depth: 0,
        }),
        DescriptorNode::Branch(children) => Ok(DescriptorChild {
            first: children
                .first()
                .map(|child| child.first.clone())
                .ok_or_else(|| codec_error("descriptor branch cannot be empty"))?,
            hash,
            count: children.iter().try_fold(0_u32, |total, child| {
                total
                    .checked_add(child.count)
                    .ok_or_else(|| codec_error("descriptor count overflow"))
            })?,
            depth: children[0]
                .depth
                .checked_add(1)
                .ok_or_else(|| codec_error("descriptor depth overflow"))?,
        }),
    }
}

async fn read_untracked_bundle_root_metas(
    store: &(impl StorageAdapterRead + ?Sized),
    keys: &[StorageKey],
) -> Result<BTreeMap<StorageKey, BundleRootMeta>, LixError> {
    if keys.is_empty() {
        return Ok(BTreeMap::new());
    }
    #[cfg(any(test, feature = "storage-benches"))]
    record_previous_point_read_keys(keys.len());
    let values = PointReadPlan::from_unique_keys(UNTRACKED_ROW_SPACE, keys.to_vec())
        .materialize(store, StorageGetOptions::default())
        .await?
        .value;
    let mut roots = BTreeMap::new();
    for (key, value) in keys.iter().cloned().zip(values) {
        let Some(value) = value else { continue };
        let StorageProjectedValue::FullValue(value) = value else {
            return Err(codec_error(
                "untracked bundle point read omitted its root value",
            ));
        };
        decode_bundle_key(&key.0)?;
        roots.insert(key.clone(), decode_bundle_root_meta(&key.0, value)?);
    }
    Ok(roots)
}

async fn load_descriptor_node(
    store: &(impl StorageAdapterRead + ?Sized),
    hash: [u8; 32],
    cache: &mut BTreeMap<[u8; 32], DescriptorNode>,
) -> Result<DescriptorNode, LixError> {
    if let Some(node) = cache.get(&hash) {
        return Ok(node.clone());
    }
    record_descriptor_node_read();
    #[cfg(any(test, feature = "storage-benches"))]
    record_previous_point_read_keys(1);
    let value = PointReadPlan::new(
        UNTRACKED_BUNDLE_CHUNK_SPACE,
        &[StorageKey(Bytes::copy_from_slice(&hash))],
    )
    .materialize(store, StorageGetOptions::default())
    .await?
    .value
    .into_iter()
    .next()
    .flatten()
    .ok_or_else(|| codec_error("untracked descriptor node is missing"))?;
    let StorageProjectedValue::FullValue(value) = value else {
        return Err(codec_error("untracked descriptor node omitted its value"));
    };
    if *blake3::hash(&value).as_bytes() != hash {
        return Err(codec_error(
            "untracked descriptor node hash validation failed",
        ));
    }
    let node = decode_descriptor_node(value)?;
    cache.insert(hash, node.clone());
    Ok(node)
}

async fn load_descriptor_tree(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &BundleRootMeta,
) -> Result<DescriptorMap, LixError> {
    let mut cache = BTreeMap::new();
    let mut visited = BTreeSet::new();
    let mut out = BTreeMap::new();
    load_descriptor_tree_node(
        store,
        root.node_hash,
        root.depth,
        root.count,
        &mut cache,
        &mut visited,
        &mut out,
    )
    .await?;
    if out.len() != root.count as usize {
        return Err(codec_error(
            "descriptor tree count does not match root metadata",
        ));
    }
    Ok(out)
}

async fn load_descriptor_tree_node(
    store: &(impl StorageAdapterRead + ?Sized),
    hash: [u8; 32],
    expected_depth: u16,
    expected_count: u32,
    cache: &mut BTreeMap<[u8; 32], DescriptorNode>,
    visited: &mut BTreeSet<[u8; 32]>,
    out: &mut DescriptorMap,
) -> Result<(), LixError> {
    let mut stack = vec![(hash, expected_depth, expected_count, None::<Option<String>>)];
    while let Some((hash, expected_depth, expected_count, expected_first)) = stack.pop() {
        if !visited.insert(hash) {
            continue;
        }
        let node = load_descriptor_node(store, hash, cache).await?;
        match node {
            DescriptorNode::Leaf(entries) => {
                if expected_depth != 0 || expected_count != entries.len() as u32 {
                    return Err(codec_error("descriptor leaf metadata does not match root"));
                }
                if let Some(expected_first) = expected_first.as_ref()
                    && entries.first().map(|(file_id, _)| file_id) != Some(expected_first)
                {
                    return Err(codec_error(
                        "descriptor leaf first key disagrees with parent",
                    ));
                }
                for (file_id, descriptor) in entries {
                    if out.insert(file_id, descriptor).is_some() {
                        return Err(codec_error("descriptor tree contains a duplicate variant"));
                    }
                }
            }
            DescriptorNode::Branch(children) => {
                if expected_depth == 0
                    || children
                        .iter()
                        .any(|child| child.depth + 1 != expected_depth)
                {
                    return Err(codec_error("descriptor branch depth is invalid"));
                }
                let count = children.iter().try_fold(0_u32, |total, child| {
                    total
                        .checked_add(child.count)
                        .ok_or_else(|| codec_error("descriptor tree count overflow"))
                })?;
                if count != expected_count {
                    return Err(codec_error("descriptor branch count does not match root"));
                }
                if let Some(expected_first) = expected_first.as_ref()
                    && children.first().map(|child| &child.first) != Some(expected_first)
                {
                    return Err(codec_error(
                        "descriptor branch first key disagrees with parent",
                    ));
                }
                stack.extend(
                    children
                        .into_iter()
                        .map(|child| (child.hash, child.depth, child.count, Some(child.first))),
                );
            }
        }
    }
    Ok(())
}

async fn lookup_descriptor(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &BundleRootMeta,
    target: &Option<String>,
) -> Result<Option<ChunkDescriptor>, LixError> {
    let mut cache = BTreeMap::new();
    let mut hash = root.node_hash;
    let mut depth = root.depth;
    let mut expected_first = None;
    loop {
        let node = load_descriptor_node(store, hash, &mut cache).await?;
        match node {
            DescriptorNode::Leaf(entries) => {
                if let Some(expected_first) = expected_first.as_ref()
                    && entries.first().map(|(file_id, _)| file_id) != Some(expected_first)
                {
                    return Err(codec_error(
                        "descriptor lookup first key disagrees with parent",
                    ));
                }
                return Ok(entries
                    .binary_search_by(|(file_id, _)| file_id.cmp(target))
                    .ok()
                    .map(|index| entries[index].1.clone()));
            }
            DescriptorNode::Branch(children) => {
                if depth == 0 || children.iter().any(|child| child.depth + 1 != depth) {
                    return Err(codec_error("descriptor lookup encountered invalid depth"));
                }
                let index = children.partition_point(|child| {
                    child.first.cmp(target) != std::cmp::Ordering::Greater
                });
                let child = children
                    .get(index.saturating_sub(1))
                    .ok_or_else(|| codec_error("descriptor branch has no target child"))?;
                if let Some(expected_first) = expected_first.as_ref()
                    && children.first().map(|child| &child.first) != Some(expected_first)
                {
                    return Err(codec_error("descriptor lookup branch first key is invalid"));
                }
                expected_first = Some(child.first.clone());
                hash = child.hash;
                depth = child.depth;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn collect_descriptor_tree_hashes(
    store: &(impl StorageAdapterRead + ?Sized),
    root: &BundleRootMeta,
    reachable: &mut BTreeSet<[u8; 32]>,
    cache: &mut BTreeMap<[u8; 32], DescriptorNode>,
    visited: &mut BTreeSet<[u8; 32]>,
) -> Result<(), LixError> {
    let mut stack = vec![(
        root.node_hash,
        root.depth,
        root.count,
        None::<Option<String>>,
    )];
    let mut count = 0_u32;
    while let Some((hash, depth, expected_count, expected_first)) = stack.pop() {
        if !visited.insert(hash) {
            continue;
        }
        reachable.insert(hash);
        let node = load_descriptor_node(store, hash, cache).await?;
        match node {
            DescriptorNode::Leaf(entries) => {
                if depth != 0 || expected_count != entries.len() as u32 {
                    return Err(codec_error("descriptor GC leaf metadata is invalid"));
                }
                if let Some(expected_first) = expected_first.as_ref()
                    && entries.first().map(|(file_id, _)| file_id) != Some(expected_first)
                {
                    return Err(codec_error("descriptor GC leaf first key is invalid"));
                }
                count = count
                    .checked_add(expected_count)
                    .ok_or_else(|| codec_error("descriptor GC count overflow"))?;
                reachable.extend(entries.into_iter().map(|(_, descriptor)| descriptor.hash));
            }
            DescriptorNode::Branch(children) => {
                if depth == 0 || children.iter().any(|child| child.depth + 1 != depth) {
                    return Err(codec_error("descriptor GC branch depth is invalid"));
                }
                let branch_count = children.iter().try_fold(0_u32, |total, child| {
                    total
                        .checked_add(child.count)
                        .ok_or_else(|| codec_error("descriptor GC count overflow"))
                })?;
                if branch_count != expected_count {
                    return Err(codec_error("descriptor GC branch count is invalid"));
                }
                if let Some(expected_first) = expected_first.as_ref()
                    && children.first().map(|child| &child.first) != Some(expected_first)
                {
                    return Err(codec_error("descriptor GC branch first key is invalid"));
                }
                stack.extend(
                    children
                        .into_iter()
                        .map(|child| (child.hash, child.depth, child.count, Some(child.first))),
                );
            }
        }
    }
    if count != root.count {
        return Err(codec_error("descriptor GC root count is invalid"));
    }
    Ok(())
}

struct DescriptorEditor<'a, S: ?Sized> {
    store: &'a S,
    root: Option<BundleRootMeta>,
    cache: BTreeMap<[u8; 32], DescriptorNode>,
    staged: BTreeMap<[u8; 32], Bytes>,
    reused: u64,
    splits: u64,
    max_depth: u16,
}

impl<'a, S: StorageAdapterRead + ?Sized> DescriptorEditor<'a, S> {
    fn new(store: &'a S, root: Option<BundleRootMeta>) -> Self {
        let max_depth = root.as_ref().map_or(0, |root| root.depth);
        Self {
            store,
            root,
            cache: BTreeMap::new(),
            staged: BTreeMap::new(),
            reused: 0,
            splits: 0,
            max_depth,
        }
    }

    async fn load(&mut self, hash: [u8; 32]) -> Result<DescriptorNode, LixError> {
        load_descriptor_node(self.store, hash, &mut self.cache).await
    }

    fn stage_node(&mut self, node: DescriptorNode) -> Result<DescriptorChild, LixError> {
        let child = descriptor_node_ref(&node)?;
        let bytes = encode_descriptor_node(&node)?;
        if self.staged.contains_key(&child.hash) {
            self.reused = self.reused.saturating_add(1);
            record_descriptor_reuse();
        } else {
            record_descriptor_node_write(bytes.len());
            self.staged.insert(child.hash, bytes);
        }
        self.cache.insert(child.hash, node);
        record_descriptor_depth(child.depth);
        Ok(child)
    }

    fn leaf_groups(
        &mut self,
        entries: Vec<(Option<String>, ChunkDescriptor)>,
    ) -> Result<Vec<DescriptorChild>, LixError> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let mut groups = Vec::new();
        for group in entries.chunks(BUNDLE_LEAF_CAPACITY) {
            groups.push(self.stage_node(DescriptorNode::Leaf(group.to_vec()))?);
        }
        if groups.len() > 1 {
            self.splits = self.splits.saturating_add(1);
            record_descriptor_split();
        }
        Ok(groups)
    }

    fn branch_groups(
        &mut self,
        children: Vec<DescriptorChild>,
    ) -> Result<Vec<DescriptorChild>, LixError> {
        if children.is_empty() {
            return Ok(Vec::new());
        }
        if children.len() == 1 {
            return Ok(children);
        }
        let mut groups = Vec::new();
        for group in children.chunks(BUNDLE_NODE_FANOUT) {
            groups.push(self.stage_node(DescriptorNode::Branch(group.to_vec()))?);
        }
        if groups.len() > 1 {
            self.splits = self.splits.saturating_add(1);
            record_descriptor_split();
        }
        Ok(groups)
    }

    async fn update(
        &mut self,
        target: Option<String>,
        replacement: Option<ChunkDescriptor>,
    ) -> Result<(), LixError> {
        let Some(root) = self.root.clone() else {
            let Some(replacement) = replacement else {
                return Ok(());
            };
            let refs = self.leaf_groups(vec![(target, replacement)])?;
            return self.finish_refs(refs);
        };
        let mut path = Vec::<(DescriptorNode, usize)>::new();
        let mut hash = root.node_hash;
        let mut depth = root.depth;
        let (mut refs, found) = loop {
            let node = self.load(hash).await?;
            match node.clone() {
                DescriptorNode::Leaf(mut entries) => {
                    let index = entries.binary_search_by(|(file_id, _)| file_id.cmp(&target));
                    let found = index.is_ok();
                    if let (Some(replacement), Ok(index)) = (&replacement, index)
                        && entries[index].1 == *replacement
                    {
                        return Ok(());
                    }
                    match (replacement.clone(), index) {
                        (Some(replacement), Ok(index)) => entries[index].1 = replacement,
                        (Some(replacement), Err(index)) => {
                            entries.insert(index, (target.clone(), replacement))
                        }
                        (None, Ok(index)) => {
                            entries.remove(index);
                        }
                        (None, Err(_)) => {}
                    }
                    break (self.leaf_groups(entries)?, found);
                }
                DescriptorNode::Branch(children) => {
                    if depth == 0 || children.iter().any(|child| child.depth + 1 != depth) {
                        return Err(codec_error("descriptor update encountered invalid depth"));
                    }
                    let index = children.partition_point(|child| {
                        child.first.cmp(&target) != std::cmp::Ordering::Greater
                    });
                    let index = index.saturating_sub(1);
                    let child = children
                        .get(index)
                        .ok_or_else(|| codec_error("descriptor branch has no update child"))?;
                    path.push((node, index));
                    hash = child.hash;
                    depth = child.depth;
                }
            }
        };
        if !found && replacement.is_none() {
            // A delete of an absent member must not rewrite the path.
            return Ok(());
        }
        for (node, index) in path.into_iter().rev() {
            let DescriptorNode::Branch(mut children) = node else {
                return Err(codec_error("descriptor update path contained a leaf"));
            };
            if index >= children.len() {
                return Err(codec_error(
                    "descriptor update child index is out of bounds",
                ));
            }
            let old_len = children.len();
            children.splice(index..=index, refs);
            let reused = u64::try_from(old_len.saturating_sub(1)).unwrap_or(u64::MAX);
            self.reused = self.reused.saturating_add(reused);
            for _ in 0..old_len.saturating_sub(1) {
                record_descriptor_reuse();
            }
            refs = self.branch_groups(children)?;
        }
        self.finish_refs(refs)
    }

    fn finish_refs(&mut self, refs: Vec<DescriptorChild>) -> Result<(), LixError> {
        let Some(mut root_ref) = refs.first().cloned() else {
            self.root = None;
            self.max_depth = 0;
            return Ok(());
        };
        if refs.len() > 1 {
            let mut children = refs;
            while children.len() > 1 {
                let groups = self.branch_groups(children)?;
                if groups.len() == 1 {
                    root_ref = groups[0].clone();
                    break;
                }
                children = groups;
            }
        }
        self.max_depth = root_ref.depth;
        record_descriptor_depth(root_ref.depth);
        self.root = Some(BundleRootMeta {
            node_hash: root_ref.hash,
            count: root_ref.count,
            depth: root_ref.depth,
        });
        Ok(())
    }
}

fn encode_bundle_root(binding: &[u8], root: &BundleRootMeta) -> Result<Bytes, LixError> {
    encode_bundle_root_meta(binding, root)
}

fn decode_bundle_root(binding: &[u8], bytes: Bytes) -> Result<BundleRootMeta, LixError> {
    decode_bundle_root_meta(binding, bytes)
}

fn chunk_descriptor(encoded: &Bytes) -> Result<ChunkDescriptor, LixError> {
    let len = u32::try_from(encoded.len())
        .map_err(|_| codec_error("untracked bundle chunk is too large"))?;
    Ok(ChunkDescriptor {
        hash: *blake3::hash(encoded).as_bytes(),
        len,
    })
}

#[cfg(test)]
fn bundle_tree_from_decoded(
    bundle: &DecodedBundle,
) -> Result<(BundleRootMeta, BTreeMap<[u8; 32], Bytes>), LixError> {
    let entries = bundle
        .iter()
        .map(|(file_id, member)| Ok((file_id.clone(), chunk_descriptor(&member.encoded)?)))
        .collect::<Result<Vec<_>, LixError>>()?;
    let node = DescriptorNode::Leaf(entries);
    let child = descriptor_node_ref(&node)?;
    let bytes = encode_descriptor_node(&node)?;
    Ok((
        BundleRootMeta {
            node_hash: child.hash,
            count: child.count,
            depth: child.depth,
        },
        BTreeMap::from([(child.hash, bytes)]),
    ))
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
        let (meta, _) = bundle_tree_from_decoded(&bundle)?;
        let bytes = encode_bundle_root(b"bundle-codec-key", &meta)?;
        assert_eq!(
            decode_bundle_root(b"bundle-codec-key", bytes.clone())?.count,
            1
        );
        assert!(decode_bundle_root(b"other-bundle-key", bytes.clone()).is_err());
        let mut tampered = bytes.to_vec();
        tampered[BUNDLE_ROOT_MAGIC.len() + 4] ^= 1;
        assert!(decode_bundle_root(b"bundle-codec-key", Bytes::from(tampered)).is_err());
        let mut malformed = BUNDLE_ROOT_MAGIC.to_vec();
        malformed.extend_from_slice(&1_u32.to_be_bytes());
        malformed.extend_from_slice(&0_u16.to_be_bytes());
        malformed.extend_from_slice(&[0; 32]);
        malformed.extend_from_slice(&[0; 32]);
        assert!(decode_bundle_root(b"bundle-codec-key", Bytes::from(malformed)).is_err());
        let node_bytes = encode_descriptor_node(&DescriptorNode::Leaf(vec![(
            None,
            ChunkDescriptor {
                hash: [3; 32],
                len: 1,
            },
        )]))?;
        let mut corrupt_node = node_bytes.to_vec();
        *corrupt_node
            .last_mut()
            .expect("descriptor node is non-empty") ^= 1;
        assert!(decode_descriptor_node(Bytes::from(corrupt_node)).is_err());
        let duplicate = DescriptorNode::Leaf(vec![
            (
                None,
                ChunkDescriptor {
                    hash: [1; 32],
                    len: 1,
                },
            ),
            (
                None,
                ChunkDescriptor {
                    hash: [2; 32],
                    len: 1,
                },
            ),
        ]);
        assert!(encode_descriptor_node(&duplicate).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn descriptor_hierarchy_sparse_update_rewrites_only_frontier() -> Result<(), LixError> {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "descriptor-frontier";
        let entity = EntityPk::single("one-entity");
        let file_ids = (0..40)
            .map(|index| format!("file-{index:02}"))
            .collect::<Vec<_>>();
        let initial = file_ids
            .iter()
            .map(|file_id| {
                untracked(
                    "schema",
                    Some(file_id.as_str()),
                    &entity,
                    r#"{"v":1}"#,
                    timestamp(),
                )
            })
            .collect::<Vec<_>>();
        commit_deltas(&storage, branch_id, &initial, &vec![true; initial.len()]).await?;
        let _ = take_untracked_mutation_read_profile();
        let update = untracked(
            "schema",
            Some("file-17"),
            &entity,
            r#"{"v":2}"#,
            timestamp(),
        );
        commit_deltas(&storage, branch_id, &[update], &[false]).await?;
        let profile = take_untracked_mutation_read_profile();
        assert_eq!(profile.previous_scan_rows, 0);
        assert!(profile.descriptor_node_reads <= 8, "{profile:?}");
        assert!(profile.descriptor_node_writes <= 3, "{profile:?}");
        assert!(profile.descriptor_node_writes < 40);
        assert!(profile.descriptor_max_depth >= 1);
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
        assert_eq!(profile.previous_point_read_keys, 5);
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
        let _ = take_untracked_mutation_read_profile();
        let replacement = untracked(
            "bundle-schema",
            Some("file-a"),
            &entity,
            r#"{"v":"a2"}"#,
            second_created,
        );
        commit_deltas(&storage, branch_id, &[replacement], &[false]).await?;
        let profile = take_untracked_mutation_read_profile();
        assert_eq!(
            profile.previous_point_read_keys, 5,
            "sparse replacement reads one root, descriptor frontier, and selected payload"
        );
        assert_eq!(profile.previous_scan_rows, 0);

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let key = StorageKey(Bytes::from(encode_bundle_key(
            branch_id,
            "bundle-schema",
            &entity,
        )?));
        let bundle = read_untracked_bundles(&read, &[key])
            .await?
            .into_values()
            .next()
            .ok_or_else(|| codec_error("replacement bundle disappeared"))?;
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
        assert_eq!(profile.previous_point_read_keys, ROWS as u64 * 4 + 1);
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
    async fn missing_variant_chunk_fails_closed_before_mutation_writes() -> Result<(), LixError> {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "bundle-missing-chunk";
        let entity = EntityPk::single("member");
        let initial = untracked("schema", Some("file-a"), &entity, r#"{"v":1}"#, timestamp());
        commit_deltas(&storage, branch_id, &[initial], &[true]).await?;

        let key = StorageKey(Bytes::from(encode_bundle_key(
            branch_id, "schema", &entity,
        )?));
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let root = read_untracked_bundle_roots(&read, std::slice::from_ref(&key))
            .await?
            .remove(&key)
            .ok_or_else(|| codec_error("bundle root disappeared"))?;
        let descriptor = root
            .get(&Some("file-a".to_string()))
            .ok_or_else(|| codec_error("bundle chunk descriptor disappeared"))?;
        let mut delete = storage.new_write_set();
        delete.delete(
            UNTRACKED_BUNDLE_CHUNK_SPACE,
            StorageKey(Bytes::copy_from_slice(&descriptor.hash)),
        );
        storage
            .commit_write_set(delete, StorageWriteOptions::default())
            .await
            .map_err(LixError::from)?;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let mut writes = storage.new_write_set();
        let update = untracked("schema", Some("file-a"), &entity, r#"{"v":2}"#, timestamp());
        assert!(
            stage_untracked_deltas(
                &read,
                &mut writes,
                branch_id,
                test_control(branch_id),
                &[update],
                &[false],
            )
            .await
            .is_err()
        );
        assert!(writes.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn branch_delete_validates_chunks_before_staging_root_deletes() -> Result<(), LixError> {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "bundle-delete-missing-chunk";
        let entity = EntityPk::single("member");
        commit_deltas(
            &storage,
            branch_id,
            &[untracked(
                "schema",
                Some("file-a"),
                &entity,
                r#"{"v":1}"#,
                timestamp(),
            )],
            &[true],
        )
        .await?;
        let key = StorageKey(Bytes::from(encode_bundle_key(
            branch_id, "schema", &entity,
        )?));
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let root = read_untracked_bundle_roots(&read, std::slice::from_ref(&key))
            .await?
            .remove(&key)
            .ok_or_else(|| codec_error("branch-delete root disappeared"))?;
        let descriptor = root
            .values()
            .next()
            .ok_or_else(|| codec_error("branch-delete descriptor disappeared"))?;
        let mut delete_chunk = storage.new_write_set();
        delete_chunk.delete(
            UNTRACKED_BUNDLE_CHUNK_SPACE,
            StorageKey(Bytes::copy_from_slice(&descriptor.hash)),
        );
        storage
            .commit_write_set(delete_chunk, StorageWriteOptions::default())
            .await
            .map_err(LixError::from)?;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let mut writes = storage.new_write_set();
        assert!(
            stage_untracked_deltas_for_branch_deletion(
                &read,
                &mut writes,
                branch_id,
                test_control(branch_id),
                &[],
                &[],
            )
            .await
            .is_err()
        );
        assert!(writes.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn chunk_gc_reclaims_orphans_and_rejects_unowned_or_missing_roots() -> Result<(), LixError>
    {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "bundle-gc";
        let entity = EntityPk::single("member");
        commit_deltas(
            &storage,
            branch_id,
            &[untracked(
                "schema",
                Some("file-a"),
                &entity,
                r#"{"v":1}"#,
                timestamp(),
            )],
            &[true],
        )
        .await?;
        let root_key = StorageKey(Bytes::from(encode_bundle_key(
            branch_id, "schema", &entity,
        )?));
        let mut remove_root = storage.new_write_set();
        remove_root.delete(UNTRACKED_ROW_SPACE, root_key);
        storage
            .commit_write_set(remove_root, StorageWriteOptions::default())
            .await
            .map_err(LixError::from)?;
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let mut writes = storage.new_write_set();
        let mut controlled = BTreeSet::new();
        controlled.insert(branch_id.to_owned());
        controlled.insert(GLOBAL_BRANCH_ID.to_owned());
        stage_untracked_chunk_gc(&read, &mut writes, &controlled).await?;
        assert!(
            !writes.is_empty(),
            "orphan chunk must be staged for deletion"
        );

        let orphan_storage = StorageAdapter::new(Memory::new());
        commit_deltas(
            &orphan_storage,
            "orphan-branch",
            &[untracked(
                "schema",
                None,
                &entity,
                r#"{"v":1}"#,
                timestamp(),
            )],
            &[true],
        )
        .await?;
        let orphan_read = orphan_storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let mut orphan_writes = orphan_storage.new_write_set();
        let controlled = BTreeSet::from([GLOBAL_BRANCH_ID.to_owned()]);
        assert!(
            stage_untracked_chunk_gc(&orphan_read, &mut orphan_writes, &controlled)
                .await
                .is_err()
        );
        assert!(orphan_writes.is_empty());

        let missing_storage = StorageAdapter::new(Memory::new());
        let missing_branch = "missing-gc-chunk";
        commit_deltas(
            &missing_storage,
            missing_branch,
            &[untracked(
                "schema",
                None,
                &entity,
                r#"{"v":1}"#,
                timestamp(),
            )],
            &[true],
        )
        .await?;
        let missing_read = missing_storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let missing_key = StorageKey(Bytes::from(encode_bundle_key(
            missing_branch,
            "schema",
            &entity,
        )?));
        let missing_root =
            read_untracked_bundle_roots(&missing_read, std::slice::from_ref(&missing_key))
                .await?
                .remove(&missing_key)
                .ok_or_else(|| codec_error("missing-GC root disappeared"))?;
        let missing_hash = missing_root
            .values()
            .next()
            .ok_or_else(|| codec_error("missing-GC descriptor disappeared"))?
            .hash;
        let mut delete_chunk = missing_storage.new_write_set();
        delete_chunk.delete(
            UNTRACKED_BUNDLE_CHUNK_SPACE,
            StorageKey(Bytes::copy_from_slice(&missing_hash)),
        );
        missing_storage
            .commit_write_set(delete_chunk, StorageWriteOptions::default())
            .await
            .map_err(LixError::from)?;
        let missing_read = missing_storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let mut missing_writes = missing_storage.new_write_set();
        let controlled = BTreeSet::from([missing_branch.to_owned(), GLOBAL_BRANCH_ID.to_owned()]);
        assert!(
            stage_untracked_chunk_gc(&missing_read, &mut missing_writes, &controlled)
                .await
                .is_err()
        );
        assert!(missing_writes.is_empty());
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
        let (malformed_root, _) = bundle_tree_from_decoded(&member_bundle)?;
        corrupt.put(
            UNTRACKED_ROW_SPACE,
            StorageKey(Bytes::from(malformed_key.clone())),
            StorageValue {
                bytes: encode_bundle_root(&malformed_key, &malformed_root)?,
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
