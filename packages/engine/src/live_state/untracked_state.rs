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
use crate::branch::BranchHeadControl;
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
pub(crate) const UNTRACKED_FILE_LOCATOR_SPACE: StorageSpace = StorageSpace::mutable(
    StorageSpaceId(0x0004_0034),
    "live_state.untracked_file_locator.v1",
);

const VALUE_VERSION: u8 = 2;
const SLOT_NONE: u8 = 0;
const SLOT_REF: u8 = 1;
const SLOT_INLINE: u8 = 2;
const LOCATOR_ROOT_MAGIC: &[u8] = b"LXULR1";
const LOCATOR_ENTRY_MARKER: &[u8] = b"LXULE1";
const LOCATOR_ROOT_TAG: u8 = 0;
const LOCATOR_ENTRY_TAG: u8 = 1;
const LOCATOR_SUMMARY_TAG: u8 = 2;
const LOCATOR_SUMMARY_MAGIC: &[u8] = b"LXULS1";
const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";

#[cfg(any(test, feature = "storage-benches"))]
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct UntrackedMutationReadProfile {
    pub(crate) previous_point_read_keys: u64,
    pub(crate) previous_scan_rows: u64,
    pub(crate) previous_scan_bytes: u64,
}

#[cfg(any(test, feature = "storage-benches"))]
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct UntrackedFileLocatorReadProfile {
    pub(crate) root_point_reads: u64,
    pub(crate) summary_point_reads: u64,
    pub(crate) root_bytes_written: u64,
    pub(crate) summary_writes: u64,
    pub(crate) entry_writes: u64,
    pub(crate) locator_scans: u64,
    pub(crate) locator_rows: u64,
    pub(crate) authoritative_exact_reads: u64,
    pub(crate) rebuilt_rows: u64,
}

#[cfg(test)]
thread_local! {
    static PREVIOUS_POINT_READ_KEYS: Cell<u64> = const { Cell::new(0) };
    static PREVIOUS_SCAN_ROWS: Cell<u64> = const { Cell::new(0) };
    static PREVIOUS_SCAN_BYTES: Cell<u64> = const { Cell::new(0) };
    static LOCATOR_ROOT_POINT_READS: Cell<u64> = const { Cell::new(0) };
    static LOCATOR_SUMMARY_POINT_READS: Cell<u64> = const { Cell::new(0) };
    static LOCATOR_ROOT_BYTES_WRITTEN: Cell<u64> = const { Cell::new(0) };
    static LOCATOR_SUMMARY_WRITES: Cell<u64> = const { Cell::new(0) };
    static LOCATOR_ENTRY_WRITES: Cell<u64> = const { Cell::new(0) };
    static LOCATOR_SCANS: Cell<u64> = const { Cell::new(0) };
    static LOCATOR_ROWS: Cell<u64> = const { Cell::new(0) };
    static LOCATOR_AUTHORITATIVE_EXACT_READS: Cell<u64> = const { Cell::new(0) };
    static LOCATOR_REBUILT_ROWS: Cell<u64> = const { Cell::new(0) };
}

#[cfg(all(feature = "storage-benches", not(test)))]
static PREVIOUS_POINT_READ_KEYS: AtomicU64 = AtomicU64::new(0);
#[cfg(all(feature = "storage-benches", not(test)))]
static PREVIOUS_SCAN_ROWS: AtomicU64 = AtomicU64::new(0);
#[cfg(all(feature = "storage-benches", not(test)))]
static PREVIOUS_SCAN_BYTES: AtomicU64 = AtomicU64::new(0);
#[cfg(all(feature = "storage-benches", not(test)))]
static LOCATOR_ROOT_POINT_READS: AtomicU64 = AtomicU64::new(0);
#[cfg(all(feature = "storage-benches", not(test)))]
static LOCATOR_SUMMARY_POINT_READS: AtomicU64 = AtomicU64::new(0);
#[cfg(all(feature = "storage-benches", not(test)))]
static LOCATOR_ROOT_BYTES_WRITTEN: AtomicU64 = AtomicU64::new(0);
#[cfg(all(feature = "storage-benches", not(test)))]
static LOCATOR_SUMMARY_WRITES: AtomicU64 = AtomicU64::new(0);
#[cfg(all(feature = "storage-benches", not(test)))]
static LOCATOR_ENTRY_WRITES: AtomicU64 = AtomicU64::new(0);
#[cfg(all(feature = "storage-benches", not(test)))]
static LOCATOR_SCANS: AtomicU64 = AtomicU64::new(0);
#[cfg(all(feature = "storage-benches", not(test)))]
static LOCATOR_ROWS: AtomicU64 = AtomicU64::new(0);
#[cfg(all(feature = "storage-benches", not(test)))]
static LOCATOR_AUTHORITATIVE_EXACT_READS: AtomicU64 = AtomicU64::new(0);
#[cfg(all(feature = "storage-benches", not(test)))]
static LOCATOR_REBUILT_ROWS: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn take_untracked_mutation_read_profile() -> UntrackedMutationReadProfile {
    UntrackedMutationReadProfile {
        previous_point_read_keys: PREVIOUS_POINT_READ_KEYS.with(|value| value.replace(0)),
        previous_scan_rows: PREVIOUS_SCAN_ROWS.with(|value| value.replace(0)),
        previous_scan_bytes: PREVIOUS_SCAN_BYTES.with(|value| value.replace(0)),
    }
}

#[cfg(test)]
pub(crate) fn take_untracked_file_locator_read_profile() -> UntrackedFileLocatorReadProfile {
    UntrackedFileLocatorReadProfile {
        root_point_reads: LOCATOR_ROOT_POINT_READS.with(|value| value.replace(0)),
        summary_point_reads: LOCATOR_SUMMARY_POINT_READS.with(|value| value.replace(0)),
        root_bytes_written: LOCATOR_ROOT_BYTES_WRITTEN.with(|value| value.replace(0)),
        summary_writes: LOCATOR_SUMMARY_WRITES.with(|value| value.replace(0)),
        entry_writes: LOCATOR_ENTRY_WRITES.with(|value| value.replace(0)),
        locator_scans: LOCATOR_SCANS.with(|value| value.replace(0)),
        locator_rows: LOCATOR_ROWS.with(|value| value.replace(0)),
        authoritative_exact_reads: LOCATOR_AUTHORITATIVE_EXACT_READS.with(|value| value.replace(0)),
        rebuilt_rows: LOCATOR_REBUILT_ROWS.with(|value| value.replace(0)),
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

#[cfg(test)]
fn record_locator_root_point_read() {
    LOCATOR_ROOT_POINT_READS.with(|value| value.set(value.get().saturating_add(1)));
}
#[cfg(all(feature = "storage-benches", not(test)))]
fn record_locator_root_point_read() {
    LOCATOR_ROOT_POINT_READS.fetch_add(1, Ordering::Relaxed);
}

#[cfg(test)]
fn record_locator_scan() {
    LOCATOR_SCANS.with(|value| value.set(value.get().saturating_add(1)));
}

#[cfg(test)]
fn record_locator_summary_point_reads(keys: usize) {
    LOCATOR_SUMMARY_POINT_READS.with(|value| {
        value.set(
            value
                .get()
                .saturating_add(u64::try_from(keys).unwrap_or(u64::MAX)),
        )
    });
}

#[cfg(test)]
fn record_locator_root_write(bytes: usize) {
    LOCATOR_ROOT_BYTES_WRITTEN.with(|value| {
        value.set(
            value
                .get()
                .saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX)),
        )
    });
}
#[cfg(all(feature = "storage-benches", not(test)))]
fn record_locator_root_write(bytes: usize) {
    LOCATOR_ROOT_BYTES_WRITTEN
        .fetch_add(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
}

#[cfg(test)]
fn record_locator_summary_write() {
    LOCATOR_SUMMARY_WRITES.with(|value| value.set(value.get().saturating_add(1)));
}
#[cfg(all(feature = "storage-benches", not(test)))]
fn record_locator_summary_write() {
    LOCATOR_SUMMARY_WRITES.fetch_add(1, Ordering::Relaxed);
}

#[cfg(test)]
fn record_locator_entry_write() {
    LOCATOR_ENTRY_WRITES.with(|value| value.set(value.get().saturating_add(1)));
}
#[cfg(all(feature = "storage-benches", not(test)))]
fn record_locator_entry_write() {
    LOCATOR_ENTRY_WRITES.fetch_add(1, Ordering::Relaxed);
}
#[cfg(all(feature = "storage-benches", not(test)))]
fn record_locator_summary_point_reads(keys: usize) {
    LOCATOR_SUMMARY_POINT_READS
        .fetch_add(u64::try_from(keys).unwrap_or(u64::MAX), Ordering::Relaxed);
}
#[cfg(all(feature = "storage-benches", not(test)))]
fn record_locator_scan() {
    LOCATOR_SCANS.fetch_add(1, Ordering::Relaxed);
}

#[cfg(test)]
fn record_locator_row() {
    LOCATOR_ROWS.with(|value| value.set(value.get().saturating_add(1)));
}
#[cfg(all(feature = "storage-benches", not(test)))]
fn record_locator_row() {
    LOCATOR_ROWS.fetch_add(1, Ordering::Relaxed);
}

#[cfg(test)]
fn record_locator_authoritative_exact_reads(keys: usize) {
    LOCATOR_AUTHORITATIVE_EXACT_READS.with(|value| {
        value.set(
            value
                .get()
                .saturating_add(u64::try_from(keys).unwrap_or(u64::MAX)),
        )
    });
}
#[cfg(all(feature = "storage-benches", not(test)))]
fn record_locator_authoritative_exact_reads(keys: usize) {
    LOCATOR_AUTHORITATIVE_EXACT_READS
        .fetch_add(u64::try_from(keys).unwrap_or(u64::MAX), Ordering::Relaxed);
}

#[cfg(test)]
fn record_locator_rebuilt_row() {
    LOCATOR_REBUILT_ROWS.with(|value| value.set(value.get().saturating_add(1)));
}
#[cfg(not(any(test, feature = "storage-benches")))]
fn record_locator_summary_point_reads(_keys: usize) {}
#[cfg(not(any(test, feature = "storage-benches")))]
fn record_locator_root_write(_bytes: usize) {}
#[cfg(not(any(test, feature = "storage-benches")))]
fn record_locator_summary_write() {}
#[cfg(not(any(test, feature = "storage-benches")))]
fn record_locator_entry_write() {}
#[cfg(all(feature = "storage-benches", not(test)))]
fn record_locator_rebuilt_row() {
    LOCATOR_REBUILT_ROWS.fetch_add(1, Ordering::Relaxed);
}

#[cfg(not(any(test, feature = "storage-benches")))]
fn record_locator_root_point_read() {}
#[cfg(not(any(test, feature = "storage-benches")))]
fn record_locator_scan() {}
#[cfg(not(any(test, feature = "storage-benches")))]
fn record_locator_row() {}
#[cfg(not(any(test, feature = "storage-benches")))]
fn record_locator_authoritative_exact_reads(_keys: usize) {}
#[cfg(not(any(test, feature = "storage-benches")))]
fn record_locator_rebuilt_row() {}

#[cfg(all(feature = "storage-benches", not(test)))]
#[allow(dead_code)]
pub(crate) fn take_untracked_file_locator_read_profile() -> UntrackedFileLocatorReadProfile {
    UntrackedFileLocatorReadProfile {
        root_point_reads: LOCATOR_ROOT_POINT_READS.swap(0, Ordering::Relaxed),
        summary_point_reads: LOCATOR_SUMMARY_POINT_READS.swap(0, Ordering::Relaxed),
        root_bytes_written: LOCATOR_ROOT_BYTES_WRITTEN.swap(0, Ordering::Relaxed),
        summary_writes: LOCATOR_SUMMARY_WRITES.swap(0, Ordering::Relaxed),
        entry_writes: LOCATOR_ENTRY_WRITES.swap(0, Ordering::Relaxed),
        locator_scans: LOCATOR_SCANS.swap(0, Ordering::Relaxed),
        locator_rows: LOCATOR_ROWS.swap(0, Ordering::Relaxed),
        authoritative_exact_reads: LOCATOR_AUTHORITATIVE_EXACT_READS.swap(0, Ordering::Relaxed),
        rebuilt_rows: LOCATOR_REBUILT_ROWS.swap(0, Ordering::Relaxed),
    }
}

// Preserve point-read behavior for sparse/ordinary updates. Large
// homogeneous batches switch to one schema-prefix predecessor set scan so
// owner reads scale with the authoritative row set rather than one get per
// mutation. The crossover leaves the 1K #1210 workload on the point path.
const PREVIOUS_POINT_READ_MAX_KEYS: usize = 4_096;

async fn load_previous_untracked_values(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    deltas: &[&CurrentStateDeltaRef<'_>],
) -> Result<BTreeMap<StorageKey, DecodedValue>, LixError> {
    let mut requested_by_schema = BTreeMap::<String, BTreeSet<StorageKey>>::new();
    for delta in deltas {
        let key = StorageKey(Bytes::from(encode_key(
            branch_id,
            delta.schema_key,
            delta.file_id,
            delta.entity_pk,
        )?));
        requested_by_schema
            .entry(delta.schema_key.to_owned())
            .or_default()
            .insert(key);
    }

    let mut decoded_by_key = BTreeMap::new();
    for (schema_key, requested) in requested_by_schema {
        if requested.len() <= PREVIOUS_POINT_READ_MAX_KEYS {
            let keys = requested.into_iter().collect::<Vec<_>>();
            #[cfg(any(test, feature = "storage-benches"))]
            record_previous_point_read_keys(keys.len());
            let values = PointReadPlan::from_unique_keys(UNTRACKED_ROW_SPACE, keys.clone())
                .materialize(store, StorageGetOptions::default())
                .await?
                .value;
            for (key, value) in keys.into_iter().zip(values) {
                let Some(StorageProjectedValue::FullValue(value)) = value else {
                    if value.is_some() {
                        return Err(codec_error("untracked point read omitted its row value"));
                    }
                    continue;
                };
                decoded_by_key.insert(key, decode_value(value)?);
            }
            continue;
        }

        tracing::debug!(
            target: "lix_perf",
            phase = "untracked_previous_values",
            route = "schema_prefix",
            branch = branch_id,
            schema = schema_key,
            requested_keys = requested.len(),
            "authoritative untracked predecessor set scan"
        );
        let prefix = schema_prefix(branch_id, &schema_key)?;
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
            let next_cursor = validate_scan_page_progress(
                &prefix,
                resume_after.as_ref(),
                page.value.entries.iter().map(|entry| &entry.key),
                page.value.has_more,
            )?;
            for entry in page.value.entries {
                let StorageProjectedValue::FullValue(value) = entry.value else {
                    return Err(codec_error(
                        "untracked predecessor scan omitted its row value",
                    ));
                };
                #[cfg(any(test, feature = "storage-benches"))]
                record_previous_scan_row(entry.key.0.len().saturating_add(value.len()));
                let identity = decode_key(&entry.key.0)?;
                if identity.branch_id != branch_id || identity.schema_key != schema_key {
                    return Err(codec_error(
                        "untracked predecessor scan escaped its requested schema",
                    ));
                }
                let decoded = decode_value(value)?;
                if requested.contains(&entry.key) {
                    decoded_by_key.insert(entry.key, decoded);
                }
            }
            if !page.value.has_more {
                break;
            }
            resume_after = next_cursor;
        }
    }
    Ok(decoded_by_key)
}

async fn load_locator_root(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    control: &BranchHeadControl,
) -> Result<LocatorRoot, LixError> {
    let key = locator_root_key(branch_id)?;
    record_locator_root_point_read();
    let value = PointReadPlan::new(UNTRACKED_FILE_LOCATOR_SPACE, &[key])
        .materialize(store, StorageGetOptions::default())
        .await?
        .value
        .into_iter()
        .next()
        .flatten();
    let Some(value) = value else {
        if control.untracked_locator_generation == 0
            && control.untracked_locator_count == 0
            && control.untracked_locator_root == empty_locator_root_hash()
        {
            // A missing root is a valid state only for a genuinely fresh
            // branch.  Prove that no derived locator bytes survived a control
            // loss or branch-ID reuse; otherwise the default root would make
            // stale entries disappear from the authority boundary.
            ensure_locator_branch_empty(store, branch_id).await?;
            return Ok(LocatorRoot::default());
        }
        return Err(codec_error("untracked locator root is missing"));
    };
    let StorageProjectedValue::FullValue(bytes) = value else {
        return Err(codec_error("untracked locator root omitted its value"));
    };
    let root = decode_locator_root(&bytes)?;
    if locator_root_hash(&bytes) != control.untracked_locator_root
        || root.generation != control.untracked_locator_generation
        || root.count != control.untracked_locator_count
    {
        return Err(codec_error(format!(
            "untracked locator root is stale or disagrees with branch control (root generation={} count={} control generation={} count={})",
            root.generation,
            root.count,
            control.untracked_locator_generation,
            control.untracked_locator_count
        )));
    }
    if root == LocatorRoot::default() {
        // A serialized zero root is still only a pristine marker.  Prove the
        // branch prefix is empty so a stale root record cannot mask surviving
        // entries after partial corruption.
        ensure_locator_branch_empty(store, branch_id).await?;
    }
    Ok(root)
}

async fn ensure_locator_branch_empty(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
) -> Result<(), LixError> {
    let prefix = branch_prefix(branch_id)?;
    let plan = ScanPlan::prefix(
        UNTRACKED_FILE_LOCATOR_SPACE,
        StoragePrefix {
            bytes: Bytes::copy_from_slice(&prefix),
        },
    );
    record_locator_scan();
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
            &prefix,
            resume_after.as_ref(),
            page.value.entries.iter().map(|entry| &entry.key),
            page.value.has_more,
        )?;
        if !page.value.entries.is_empty() {
            return Err(codec_error(
                "untracked locator root is missing while locator bytes remain",
            ));
        }
        if !page.value.has_more {
            return Ok(());
        }
        resume_after = next_cursor;
    }
}

async fn load_locator_summaries(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    file_ids: &BTreeSet<String>,
) -> Result<BTreeMap<String, Option<LocatorSummary>>, LixError> {
    if file_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let names = file_ids.iter().cloned().collect::<Vec<_>>();
    let keys = names
        .iter()
        .map(|file_id| locator_summary_key(branch_id, file_id))
        .collect::<Result<Vec<_>, _>>()?;
    record_locator_summary_point_reads(keys.len());
    let values = PointReadPlan::from_unique_keys(UNTRACKED_FILE_LOCATOR_SPACE, keys)
        .materialize(store, StorageGetOptions::default())
        .await?
        .value;
    let mut summaries = BTreeMap::new();
    for (file_id, value) in names.into_iter().zip(values) {
        let summary = match value {
            None => None,
            Some(StorageProjectedValue::FullValue(value)) => Some(decode_locator_summary(&value)?),
            Some(StorageProjectedValue::KeyOnly) => {
                return Err(codec_error("untracked locator summary omitted its value"));
            }
        };
        summaries.insert(file_id, summary);
    }
    Ok(summaries)
}

struct LocatorMember {
    locator_key: StorageKey,
    identity: DecodedIdentity,
}

async fn scan_locator_members(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    file_id: &str,
    expected: Option<&LocatorSummary>,
) -> Result<Vec<LocatorMember>, LixError> {
    let prefix = locator_entry_prefix(branch_id, file_id)?;
    let plan = ScanPlan::prefix(
        UNTRACKED_FILE_LOCATOR_SPACE,
        StoragePrefix {
            bytes: Bytes::copy_from_slice(&prefix),
        },
    );
    record_locator_scan();
    let mut resume_after = None;
    let mut members = Vec::new();
    let mut digest = [0_u8; 32];
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
                return Err(codec_error("untracked locator entry omitted its marker"));
            };
            if value.as_ref() != LOCATOR_ENTRY_MARKER {
                return Err(codec_error(
                    "untracked locator entry has a malformed marker",
                ));
            }
            let identity = decode_locator_key(&entry.key.0)?;
            if identity.branch_id != branch_id || identity.file_id.as_deref() != Some(file_id) {
                return Err(codec_error(
                    "untracked locator entry escaped its file prefix",
                ));
            }
            xor_digest(&mut digest, locator_key_digest(&entry.key));
            record_locator_row();
            members.push(LocatorMember {
                locator_key: entry.key,
                identity,
            });
        }
        if !page.value.has_more {
            break;
        }
        resume_after = next_cursor;
    }
    let actual = LocatorSummary {
        count: u64::try_from(members.len()).unwrap_or(u64::MAX),
        digest,
    };
    match expected {
        Some(expected) if *expected == actual => Ok(members),
        Some(_) => Err(codec_error(
            "untracked locator file membership disagrees with its authenticated root",
        )),
        None if members.is_empty() => Ok(members),
        None => Err(codec_error(
            "untracked locator has members without an authenticated file summary",
        )),
    }
}

async fn validate_locator_authority(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    members: &[LocatorMember],
) -> Result<BTreeMap<StorageKey, DecodedValue>, LixError> {
    if members.is_empty() {
        return Ok(BTreeMap::new());
    }
    let keys = members
        .iter()
        .map(|member| {
            Ok(StorageKey(Bytes::from(encode_key(
                branch_id,
                &member.identity.schema_key,
                member.identity.file_id.as_deref(),
                &member.identity.entity_pk,
            )?)))
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    record_locator_authoritative_exact_reads(keys.len());
    let values = PointReadPlan::new(UNTRACKED_ROW_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?
        .value;
    let mut decoded = BTreeMap::new();
    for ((key, member), value) in keys.into_iter().zip(members).zip(values) {
        let Some(StorageProjectedValue::FullValue(value)) = value else {
            return Err(codec_error(
                "untracked locator member has no authoritative row value",
            ));
        };
        let identity = decode_key(&key.0)?;
        if identity.branch_id != branch_id
            || identity.schema_key != member.identity.schema_key
            || identity.file_id != member.identity.file_id
            || identity.entity_pk != member.identity.entity_pk
        {
            return Err(codec_error(
                "untracked locator member does not match its authoritative key",
            ));
        }
        decoded.insert(key, decode_value(value)?);
    }
    Ok(decoded)
}

pub(crate) async fn stage_untracked_deltas(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    branch_id: &str,
    control: BranchHeadControl,
    deltas: &[CurrentStateDeltaRef<'_>],
    known_absent: &[bool],
) -> Result<BranchHeadControl, LixError> {
    let live_file_ids = BTreeSet::new();
    let deleted_file_ids = BTreeSet::new();
    stage_untracked_deltas_inner(
        store,
        writes,
        branch_id,
        control,
        deltas,
        known_absent,
        &live_file_ids,
        &deleted_file_ids,
        false,
    )
    .await
}

/// Stages the ordinary untracked mutation set together with tracked file
/// lifecycle identities that were certified absent by the tracked planner.
/// These identities contribute only zero-member locator anchors; all member
/// truth remains in `UNTRACKED_ROW_SPACE`. Keeping this handoff explicit lets
/// selected merge/restore rows publish their anchor in the same write set as
/// the tracked generation without treating every descriptor update as a new
/// file.
pub(crate) async fn stage_untracked_deltas_with_live_file_ids(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    branch_id: &str,
    control: BranchHeadControl,
    deltas: &[CurrentStateDeltaRef<'_>],
    known_absent: &[bool],
    live_file_ids: &BTreeSet<String>,
    deleted_file_ids: &BTreeSet<String>,
) -> Result<BranchHeadControl, LixError> {
    stage_untracked_deltas_inner(
        store,
        writes,
        branch_id,
        control,
        deltas,
        known_absent,
        live_file_ids,
        deleted_file_ids,
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
    let live_file_ids = BTreeSet::new();
    let deleted_file_ids = BTreeSet::new();
    stage_untracked_deltas_inner(
        store,
        writes,
        branch_id,
        control,
        deltas,
        known_absent,
        &live_file_ids,
        &deleted_file_ids,
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
    live_file_ids: &BTreeSet<String>,
    lifecycle_deleted_file_ids: &BTreeSet<String>,
    drop_locator: bool,
) -> Result<BranchHeadControl, LixError> {
    if known_absent.len() != deltas.len() {
        return Err(codec_error(
            "untracked known-absent flags do not align with deltas",
        ));
    }
    let mut locator_root = load_locator_root(store, branch_id, &control).await?;
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
    let previous = load_previous_untracked_values(store, branch_id, &untracked).await?;
    let mut previous_created_at = BTreeMap::new();
    for (key, decoded) in previous {
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

    let locator_probes = deltas
        .iter()
        .zip(known_absent)
        .filter(|(delta, _)| delta.untracked && delta.file_id.is_some())
        .map(|(delta, absent)| {
            Ok((
                locator_entry_key(
                    branch_id,
                    delta.file_id.expect("file id was checked"),
                    delta.schema_key,
                    delta.entity_pk,
                )?,
                !*absent,
            ))
        })
        .collect::<Result<Vec<(StorageKey, bool)>, LixError>>()?;
    if !locator_probes.is_empty() {
        let keys = locator_probes
            .iter()
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        let values = PointReadPlan::new(UNTRACKED_FILE_LOCATOR_SPACE, &keys)
            .materialize(store, StorageGetOptions::default())
            .await?
            .value;
        for (((key, expected_present), value), delta) in locator_probes.iter().zip(values).zip(
            deltas
                .iter()
                .filter(|delta| delta.untracked && delta.file_id.is_some()),
        ) {
            let present = match value {
                None => false,
                Some(StorageProjectedValue::FullValue(value))
                    if value.as_ref() == LOCATOR_ENTRY_MARKER =>
                {
                    true
                }
                Some(StorageProjectedValue::KeyOnly) => {
                    return Err(codec_error("untracked locator probe omitted its marker"));
                }
                Some(StorageProjectedValue::FullValue(_)) => {
                    return Err(codec_error(
                        "untracked locator probe has a malformed marker",
                    ));
                }
            };
            if present != *expected_present {
                return Err(codec_error(format!(
                    "untracked locator presence disagrees with delta for schema '{}' entity {:?} file {:?}",
                    delta.schema_key, delta.entity_pk, delta.file_id
                )));
            }
            let _ = key;
        }
    }

    let mut deleted_file_ids = deltas
        .iter()
        .filter(|delta| delta.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY && delta.deleted)
        .map(|delta| delta.entity_pk.as_single_string_owned())
        .collect::<Result<BTreeSet<_>, _>>()?;
    deleted_file_ids.extend(lifecycle_deleted_file_ids.iter().cloned());
    let affected_file_ids = deltas
        .iter()
        .filter_map(|delta| delta.file_id.map(str::to_owned))
        .chain(deleted_file_ids.iter().cloned())
        .collect::<BTreeSet<_>>();
    let new_file_ids = deltas
        .iter()
        .zip(known_absent)
        .filter(|(delta, absent)| {
            // The lifecycle owner is the tracked file descriptor, not an
            // untracked row. A tracked file can become live with zero
            // untracked members, so publish its zero-member anchor in this
            // same write set before any locator invariant is checked.
            delta.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY && !delta.deleted && **absent
        })
        .map(|(delta, _)| delta.entity_pk.as_single_string_owned())
        .collect::<Result<BTreeSet<_>, _>>()?;
    let new_file_ids = new_file_ids
        .into_iter()
        .chain(live_file_ids.iter().cloned())
        .collect::<BTreeSet<_>>();
    let affected_file_ids = affected_file_ids
        .into_iter()
        .chain(new_file_ids.iter().cloned())
        .collect::<BTreeSet<_>>();
    let mut locator_summaries =
        load_locator_summaries(store, branch_id, &affected_file_ids).await?;
    // A certified absent->live transition may create a zero-member anchor
    // only when the entire locator prefix is absent. Always inspect the
    // prefix on this transition: an old summary (even a valid zero-member
    // summary) or any member is stale state from a prior incarnation and
    // must fail closed rather than being reused for the new file.
    for file_id in &new_file_ids {
        let expected = locator_summaries.get(file_id).and_then(Option::as_ref);
        let members = scan_locator_members(store, branch_id, file_id, expected).await?;
        if expected.is_some() || !members.is_empty() {
            return Err(codec_error(format!(
                "untracked locator state already exists for fresh file '{file_id}'"
            )));
        }
    }
    // A live file descriptor owns a zero-member summary even before its first
    // member. This lifecycle anchor makes simultaneous loss of a summary and
    // all of its entries fail closed instead of being mistaken for emptiness.
    for file_id in &new_file_ids {
        locator_summaries
            .entry(file_id.clone())
            .and_modify(|summary| {
                if summary.is_none() {
                    *summary = Some(LocatorSummary::default());
                }
            })
            .or_insert(Some(LocatorSummary::default()));
    }
    let mut locator_delete_keys = BTreeSet::new();
    let mut locator_entry_put_keys = BTreeSet::new();
    if !deleted_file_ids.is_empty() {
        stage_file_cascade_from_locator(
            store,
            branch_id,
            &mut locator_root,
            &mut locator_summaries,
            &deleted_file_ids,
            &mut mutations,
            &mut retired_refs,
            &mut locator_delete_keys,
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
        // A file descriptor tombstone dominates same-batch untracked member
        // puts: the final file state has no members. Remove any staged-only
        // values before publication and reclaim their payload references.
        for delta in deltas
            .iter()
            .filter(|delta| delta.untracked && delta.file_id.is_some())
            .filter(|delta| deleted_file_ids.contains(delta.file_id.expect("file id was checked")))
        {
            let key = StorageKey(Bytes::from(encode_key(
                branch_id,
                delta.schema_key,
                delta.file_id,
                delta.entity_pk,
            )?));
            // Keep an authoritative tombstone that the cascade already
            // staged.  Removing the map entry here would silently resurrect
            // a replaced member; only discard a staged-only put and retain
            // the `None` deletion for the final write set.
            if let Some(slot) = mutations.get_mut(&key) {
                if let Some(value) = slot.take() {
                    collect_value_refs(&decode_value(value.bytes)?, &mut retired_refs);
                }
            }
        }
    }

    for (delta, absent) in deltas.iter().zip(known_absent) {
        if drop_locator {
            continue;
        }
        if !delta.untracked {
            continue;
        }
        let Some(file_id) = delta.file_id else {
            continue;
        };
        if deleted_file_ids.contains(file_id) {
            continue;
        }
        let key = locator_entry_key(branch_id, file_id, delta.schema_key, delta.entity_pk)?;
        let summary = match locator_summaries.get_mut(file_id) {
            Some(Some(summary)) => summary,
            Some(None) if new_file_ids.contains(file_id) => locator_summaries
                .get_mut(file_id)
                .expect("new file summary entry exists")
                .get_or_insert_default(),
            Some(None) | None => {
                return Err(codec_error(format!(
                    "untracked locator summary is missing for existing file '{file_id}'"
                )));
            }
        };
        if delta.deleted {
            if !*absent {
                if summary.count == 0 {
                    return Err(codec_error(
                        "untracked locator member deletion has no authenticated summary",
                    ));
                }
                summary.count = summary
                    .count
                    .checked_sub(1)
                    .ok_or_else(|| codec_error("untracked locator member count underflowed"))?;
                xor_digest(&mut summary.digest, locator_key_digest(&key));
                locator_root.count = locator_root
                    .count
                    .checked_sub(1)
                    .ok_or_else(|| codec_error("untracked locator total count underflowed"))?;
                xor_digest(&mut locator_root.digest, locator_key_digest(&key));
                locator_delete_keys.insert(key);
            }
        } else if *absent {
            summary.count = summary
                .count
                .checked_add(1)
                .ok_or_else(|| codec_error("untracked locator member count overflowed"))?;
            xor_digest(&mut summary.digest, locator_key_digest(&key));
            locator_root.count = locator_root
                .count
                .checked_add(1)
                .ok_or_else(|| codec_error("untracked locator total count overflowed"))?;
            xor_digest(&mut locator_root.digest, locator_key_digest(&key));
            locator_entry_put_keys.insert(key);
        }
    }
    // The branch-deletion wrapper deliberately leaves locator bytes to the
    // exact branch-control deletion path below, but authoritative row
    // tombstones and payload-reference retirement still belong in this same
    // write set. Do not return before publishing those mutations.
    if drop_locator {
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
        return control.next_current_state_revision();
    }
    locator_root.generation = control
        .untracked_locator_generation
        .checked_add(1)
        .ok_or_else(|| codec_error("untracked locator generation overflowed"))?;
    let root_bytes = encode_locator_root(&locator_root)?;
    let root_key = locator_root_key(branch_id)?;
    let mut locator_summary_writes = Vec::with_capacity(locator_summaries.len());
    for (file_id, summary) in locator_summaries {
        let key = locator_summary_key(branch_id, &file_id)?;
        if deleted_file_ids.contains(&file_id) {
            locator_summary_writes.push((key, None));
        } else {
            let Some(summary) = summary else {
                return Err(codec_error(format!(
                    "untracked locator summary is missing for live file '{file_id}'"
                )));
            };
            locator_summary_writes.push((
                key,
                Some(StorageValue {
                    bytes: encode_locator_summary(&summary),
                }),
            ));
        }
    }

    // All locator validation and encoding is complete. Publish the projection
    // as one final append to the caller's write set so any terminal failure
    // above leaves no partial locator writes staged.
    record_locator_root_write(root_bytes.len());
    writes.put(
        UNTRACKED_FILE_LOCATOR_SPACE,
        root_key,
        StorageValue {
            bytes: root_bytes.clone(),
        },
    );
    for key in locator_entry_put_keys {
        record_locator_entry_write();
        writes.put(
            UNTRACKED_FILE_LOCATOR_SPACE,
            key,
            StorageValue {
                bytes: Bytes::from_static(LOCATOR_ENTRY_MARKER),
            },
        );
    }
    for key in locator_delete_keys {
        record_locator_entry_write();
        writes.delete(UNTRACKED_FILE_LOCATOR_SPACE, key);
    }
    for (key, value) in locator_summary_writes {
        record_locator_summary_write();
        match value {
            Some(value) => writes.put(UNTRACKED_FILE_LOCATOR_SPACE, key, value),
            None => writes.delete(UNTRACKED_FILE_LOCATOR_SPACE, key),
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
    update_locator_control(control, &locator_root, &root_bytes)
}

async fn stage_file_cascade_from_locator(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    locator_root: &mut LocatorRoot,
    locator_summaries: &mut BTreeMap<String, Option<LocatorSummary>>,
    deleted_file_ids: &BTreeSet<String>,
    mutations: &mut BTreeMap<StorageKey, Option<StorageValue>>,
    retired_refs: &mut BTreeSet<[u8; 32]>,
    locator_delete_keys: &mut BTreeSet<StorageKey>,
) -> Result<(), LixError> {
    for file_id in deleted_file_ids {
        let expected = locator_summaries.get(file_id).and_then(Option::as_ref);
        let summary_missing = expected.is_none();
        let members = scan_locator_members(store, branch_id, file_id, expected).await?;
        if summary_missing && members.is_empty() {
            return Err(codec_error(format!(
                "untracked locator summary is missing for deleted file '{file_id}'"
            )));
        }
        let authoritative = validate_locator_authority(store, branch_id, &members).await?;
        for member in members {
            let key = StorageKey(Bytes::from(encode_key(
                branch_id,
                &member.identity.schema_key,
                member.identity.file_id.as_deref(),
                &member.identity.entity_pk,
            )?));
            let decoded_value = authoritative
                .get(&key)
                .ok_or_else(|| codec_error("untracked locator authority row disappeared"))?;
            collect_value_refs(&decoded_value, retired_refs);
            if let Some(Some(staged)) = mutations.get(&key) {
                collect_value_refs(&decode_value(staged.bytes.clone())?, retired_refs);
            }
            mutations.insert(key, None);
            let locator_key_digest_value = locator_key_digest(&member.locator_key);
            locator_delete_keys.insert(member.locator_key);
            locator_root.count = locator_root
                .count
                .checked_sub(1)
                .ok_or_else(|| codec_error("untracked locator total count underflowed"))?;
            xor_digest(&mut locator_root.digest, locator_key_digest_value);
        }
        locator_summaries.insert(file_id.clone(), None);
    }
    Ok(())
}

/// Removes every locator byte for a branch during an already-validated
/// destructive branch lifecycle publication. The caller separately proves
/// that the authoritative branch rows are gone (including same-batch
/// tombstones); this function only verifies the projection's own pagination
/// and marker/key grammar before staging its physical deletion.
pub(crate) async fn stage_delete_untracked_file_locator(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    branch_id: &str,
    control: BranchHeadControl,
) -> Result<(), LixError> {
    let _ = load_locator_root(store, branch_id, &control).await?;
    let prefix = branch_prefix(branch_id)?;
    let plan = ScanPlan::prefix(
        UNTRACKED_FILE_LOCATOR_SPACE,
        StoragePrefix {
            bytes: Bytes::copy_from_slice(&prefix),
        },
    );
    let mut resume_after = None;
    let mut keys = Vec::new();
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
            let suffix = entry
                .key
                .0
                .get(prefix.len()..)
                .ok_or_else(|| codec_error("untracked locator key escaped branch prefix"))?;
            match suffix.first().copied() {
                Some(LOCATOR_ROOT_TAG) if suffix.len() == 1 => {}
                Some(LOCATOR_ENTRY_TAG) => {
                    let StorageProjectedValue::FullValue(value) = entry.value else {
                        return Err(codec_error("untracked locator deletion omitted a marker"));
                    };
                    if value.as_ref() != LOCATOR_ENTRY_MARKER {
                        return Err(codec_error(
                            "untracked locator deletion found a malformed marker",
                        ));
                    }
                    decode_locator_key(&entry.key.0)?;
                    keys.push(entry.key);
                }
                Some(LOCATOR_SUMMARY_TAG) => {
                    let mut offset = prefix.len() + 1;
                    read_text(&entry.key.0, &mut offset, "locator summary file")?;
                    if offset != entry.key.0.len() {
                        return Err(codec_error(
                            "untracked locator summary key has trailing bytes",
                        ));
                    }
                    let StorageProjectedValue::FullValue(value) = entry.value else {
                        return Err(codec_error("untracked locator deletion omitted a summary"));
                    };
                    decode_locator_summary(&value)?;
                    keys.push(entry.key);
                }
                _ => {
                    return Err(codec_error(
                        "untracked locator deletion found an invalid key",
                    ));
                }
            }
        }
        if !page.value.has_more {
            break;
        }
        resume_after = next_cursor;
    }
    for key in keys {
        writes.delete(UNTRACKED_FILE_LOCATOR_SPACE, key);
    }
    writes.delete(UNTRACKED_FILE_LOCATOR_SPACE, locator_root_key(branch_id)?);
    Ok(())
}

/// Explicit maintenance rebuild of the derived file locator.
///
/// This is intentionally not called by reads or by the cascade path.  It
/// validates a complete authoritative branch snapshot first, then replaces
/// every old locator member and root in one write set. `live_file_ids` is the
/// exact lifecycle set supplied by tracked authority; it contributes only
/// zero-member anchors, never row values or locator truth. The locator remains
/// a projection: all logical member values come from `UNTRACKED_ROW_SPACE`.
#[allow(dead_code)]
pub(crate) async fn rebuild_untracked_file_locator(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    branch_id: &str,
    control: BranchHeadControl,
    live_file_ids: &BTreeSet<String>,
) -> Result<BranchHeadControl, LixError> {
    let authority_prefix = branch_prefix(branch_id)?;
    let authority_plan = ScanPlan::prefix(
        UNTRACKED_ROW_SPACE,
        StoragePrefix {
            bytes: Bytes::copy_from_slice(&authority_prefix),
        },
    );
    let mut resume_after = None;
    let mut rebuilt_entries = Vec::<StorageKey>::new();
    let mut rebuilt_summaries = BTreeMap::<String, LocatorSummary>::new();
    let mut rebuilt_root = LocatorRoot {
        generation: control
            .untracked_locator_generation
            .checked_add(1)
            .ok_or_else(|| codec_error("untracked locator generation overflowed"))?,
        ..LocatorRoot::default()
    };
    loop {
        let page = authority_plan
            .collect(
                store,
                StorageScanOptions {
                    resume_after: resume_after.clone(),
                    ..StorageScanOptions::default()
                },
            )
            .await?;
        let next_cursor = validate_scan_page_progress(
            &authority_prefix,
            resume_after.as_ref(),
            page.value.entries.iter().map(|entry| &entry.key),
            page.value.has_more,
        )?;
        for entry in page.value.entries {
            let identity = decode_key(&entry.key.0)?;
            if identity.branch_id != branch_id {
                return Err(codec_error("untracked rebuild escaped its branch prefix"));
            }
            let StorageProjectedValue::FullValue(value) = entry.value else {
                return Err(codec_error(
                    "untracked rebuild omitted an authoritative value",
                ));
            };
            decode_value(value)?;
            let Some(file_id) = identity.file_id.as_deref() else {
                continue;
            };
            let locator_key = locator_entry_key(
                branch_id,
                file_id,
                &identity.schema_key,
                &identity.entity_pk,
            )?;
            let summary = rebuilt_summaries.entry(file_id.to_owned()).or_default();
            summary.count = summary
                .count
                .checked_add(1)
                .ok_or_else(|| codec_error("untracked locator member count overflowed"))?;
            xor_digest(&mut summary.digest, locator_key_digest(&locator_key));
            rebuilt_root.count = rebuilt_root
                .count
                .checked_add(1)
                .ok_or_else(|| codec_error("untracked locator total count overflowed"))?;
            xor_digest(&mut rebuilt_root.digest, locator_key_digest(&locator_key));
            rebuilt_entries.push(locator_key);
            record_locator_rebuilt_row();
        }
        if !page.value.has_more {
            break;
        }
        resume_after = next_cursor;
    }
    // The tracked authority supplies lifecycle identities, not payload truth.
    // Keeping an empty summary for every live file lets a later descriptor
    // delete fail closed if its member projection is missing, while all
    // locator entries/counts/digests still come exclusively from untracked
    // authoritative rows above.
    for file_id in live_file_ids {
        rebuilt_summaries.entry(file_id.clone()).or_default();
    }
    let locator_prefix = branch_prefix(branch_id)?;
    let locator_plan = ScanPlan::prefix(
        UNTRACKED_FILE_LOCATOR_SPACE,
        StoragePrefix {
            bytes: Bytes::copy_from_slice(&locator_prefix),
        },
    );
    let mut old_locator_keys = Vec::new();
    let mut resume_after = None;
    loop {
        let page = locator_plan
            .collect(
                store,
                StorageScanOptions {
                    resume_after: resume_after.clone(),
                    ..StorageScanOptions::default()
                },
            )
            .await?;
        let next_cursor = validate_scan_page_progress(
            &locator_prefix,
            resume_after.as_ref(),
            page.value.entries.iter().map(|entry| &entry.key),
            page.value.has_more,
        )?;
        // Rebuild is the explicit repair boundary: after the authoritative
        // scan has validated every row, old projection bytes are opaque and
        // are deleted/replaced regardless of stale markers or key grammar.
        for entry in page.value.entries {
            old_locator_keys.push(entry.key);
        }
        if !page.value.has_more {
            break;
        }
        resume_after = next_cursor;
    }
    let root_bytes = encode_locator_root(&rebuilt_root)?;
    let root_key = locator_root_key(branch_id)?;
    let mut rebuilt_locator_set = rebuilt_entries.iter().cloned().collect::<BTreeSet<_>>();
    for file_id in rebuilt_summaries.keys() {
        rebuilt_locator_set.insert(locator_summary_key(branch_id, file_id)?);
    }
    rebuilt_locator_set.insert(root_key.clone());
    for key in old_locator_keys {
        if !rebuilt_locator_set.contains(&key) {
            record_locator_entry_write();
            writes.delete(UNTRACKED_FILE_LOCATOR_SPACE, key);
        }
    }
    for key in rebuilt_entries {
        record_locator_entry_write();
        writes.put(
            UNTRACKED_FILE_LOCATOR_SPACE,
            key,
            StorageValue {
                bytes: Bytes::from_static(LOCATOR_ENTRY_MARKER),
            },
        );
    }
    for (file_id, summary) in rebuilt_summaries {
        record_locator_summary_write();
        writes.put(
            UNTRACKED_FILE_LOCATOR_SPACE,
            locator_summary_key(branch_id, &file_id)?,
            StorageValue {
                bytes: encode_locator_summary(&summary),
            },
        );
    }
    writes.put(
        UNTRACKED_FILE_LOCATOR_SPACE,
        root_key,
        StorageValue {
            bytes: root_bytes.clone(),
        },
    );
    record_locator_root_write(root_bytes.len());
    update_locator_control(control, &rebuilt_root, &root_bytes)
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
    tracing::debug!(
        target: "lix_perf",
        route = "entity_prefix",
        entity_count = requested.len(),
        branch_count = branch_ids.len(),
        "bounded untracked entity candidate probes"
    );
    for branch_id in branch_ids {
        for schema_key in &request.filter.schema_keys {
            for entity_pk in requested.iter().copied() {
                scan_prefix(
                    store,
                    &entity_prefix(branch_id, schema_key, entity_pk)?,
                    request,
                    &mut decoded,
                )
                .await?;
            }
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
    validate_file_locator_branches(store, controlled_branches).await?;
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

async fn validate_file_locator_branches(
    store: &(impl StorageAdapterRead + ?Sized),
    controlled_branches: &BTreeSet<String>,
) -> Result<(), LixError> {
    let plan = ScanPlan::prefix(
        UNTRACKED_FILE_LOCATOR_SPACE,
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
            let mut offset = 0;
            let branch_id = read_text(&entry.key.0, &mut offset, "locator branch")?;
            if branch_id != GLOBAL_BRANCH_ID && !controlled_branches.contains(&branch_id) {
                return Err(codec_error(format!(
                    "untracked locator belongs to orphan branch '{branch_id}'"
                )));
            }
            let tag = take(&entry.key.0, &mut offset, 1, "locator tag")?[0];
            match tag {
                LOCATOR_ROOT_TAG => {
                    if offset != entry.key.0.len() {
                        return Err(codec_error("untracked locator root key has trailing bytes"));
                    }
                    let StorageProjectedValue::FullValue(value) = entry.value else {
                        return Err(codec_error("untracked locator root omitted its value"));
                    };
                    decode_locator_root(&value)?;
                }
                LOCATOR_ENTRY_TAG => {
                    let StorageProjectedValue::FullValue(value) = entry.value else {
                        return Err(codec_error("untracked locator entry omitted its marker"));
                    };
                    if value.as_ref() != LOCATOR_ENTRY_MARKER {
                        return Err(codec_error(
                            "untracked locator entry has a malformed marker",
                        ));
                    }
                    decode_locator_key(&entry.key.0)?;
                }
                LOCATOR_SUMMARY_TAG => {
                    let mut summary_offset = offset;
                    read_text(&entry.key.0, &mut summary_offset, "locator summary file")?;
                    if summary_offset != entry.key.0.len() {
                        return Err(codec_error(
                            "untracked locator summary key has trailing bytes",
                        ));
                    }
                    let StorageProjectedValue::FullValue(value) = entry.value else {
                        return Err(codec_error("untracked locator summary omitted its value"));
                    };
                    decode_locator_summary(&value)?;
                }
                _ => return Err(codec_error("untracked locator key has an invalid tag")),
            }
        }
        if !page.value.has_more {
            break;
        }
        resume_after = next_cursor;
    }
    Ok(())
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct LocatorSummary {
    count: u64,
    digest: [u8; 32],
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct LocatorRoot {
    generation: u64,
    count: u64,
    digest: [u8; 32],
}

fn locator_root_key(branch_id: &str) -> Result<StorageKey, LixError> {
    let mut key = branch_prefix(branch_id)?;
    key.push(LOCATOR_ROOT_TAG);
    Ok(StorageKey(Bytes::from(key)))
}

fn locator_entry_key(
    branch_id: &str,
    file_id: &str,
    schema_key: &str,
    entity_pk: &EntityPk,
) -> Result<StorageKey, LixError> {
    let mut key = branch_prefix(branch_id)?;
    key.push(LOCATOR_ENTRY_TAG);
    push_text(&mut key, file_id)?;
    push_text(&mut key, schema_key)?;
    let entity_pk = crate::storage_codec::encode("untracked locator entity key", entity_pk)?;
    push_bytes(&mut key, &entity_pk)?;
    Ok(StorageKey(Bytes::from(key)))
}

fn locator_entry_prefix(branch_id: &str, file_id: &str) -> Result<Vec<u8>, LixError> {
    let mut prefix = branch_prefix(branch_id)?;
    prefix.push(LOCATOR_ENTRY_TAG);
    push_text(&mut prefix, file_id)?;
    Ok(prefix)
}

fn locator_summary_key(branch_id: &str, file_id: &str) -> Result<StorageKey, LixError> {
    let mut key = branch_prefix(branch_id)?;
    key.push(LOCATOR_SUMMARY_TAG);
    push_text(&mut key, file_id)?;
    Ok(StorageKey(Bytes::from(key)))
}

fn locator_key_digest(key: &StorageKey) -> [u8; 32] {
    *blake3::hash(&key.0).as_bytes()
}

fn xor_digest(target: &mut [u8; 32], value: [u8; 32]) {
    for (left, right) in target.iter_mut().zip(value) {
        *left ^= right;
    }
}

fn encode_locator_root(root: &LocatorRoot) -> Result<Bytes, LixError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(LOCATOR_ROOT_MAGIC);
    bytes.extend_from_slice(&root.generation.to_be_bytes());
    bytes.extend_from_slice(&root.count.to_be_bytes());
    bytes.extend_from_slice(&root.digest);
    Ok(Bytes::from(bytes))
}

fn decode_locator_root(bytes: &Bytes) -> Result<LocatorRoot, LixError> {
    if !bytes.starts_with(LOCATOR_ROOT_MAGIC) {
        return Err(codec_error("untracked locator root has an invalid format"));
    }
    let mut offset = LOCATOR_ROOT_MAGIC.len();
    let generation = u64::from_be_bytes(
        take(bytes, &mut offset, 8, "locator generation")?
            .try_into()
            .expect("eight bytes"),
    );
    let count = u64::from_be_bytes(
        take(bytes, &mut offset, 8, "locator count")?
            .try_into()
            .expect("eight bytes"),
    );
    let digest = take(bytes, &mut offset, 32, "locator digest")?
        .try_into()
        .expect("thirty-two bytes");
    if offset != bytes.len() {
        return Err(codec_error("untracked locator root has trailing bytes"));
    }
    if count == 0 && digest != [0; 32] {
        return Err(codec_error(
            "untracked locator root has a non-empty digest with zero members",
        ));
    }
    Ok(LocatorRoot {
        generation,
        count,
        digest,
    })
}

fn encode_locator_summary(summary: &LocatorSummary) -> Bytes {
    let mut bytes = Vec::with_capacity(LOCATOR_SUMMARY_MAGIC.len() + 8 + 32);
    bytes.extend_from_slice(LOCATOR_SUMMARY_MAGIC);
    bytes.extend_from_slice(&summary.count.to_be_bytes());
    bytes.extend_from_slice(&summary.digest);
    Bytes::from(bytes)
}

fn decode_locator_summary(bytes: &Bytes) -> Result<LocatorSummary, LixError> {
    if !bytes.starts_with(LOCATOR_SUMMARY_MAGIC) {
        return Err(codec_error(
            "untracked locator summary has an invalid format",
        ));
    }
    let mut offset = LOCATOR_SUMMARY_MAGIC.len();
    let count = u64::from_be_bytes(
        take(bytes, &mut offset, 8, "locator summary count")?
            .try_into()
            .expect("eight bytes"),
    );
    let digest = take(bytes, &mut offset, 32, "locator summary digest")?
        .try_into()
        .expect("thirty-two bytes");
    if offset != bytes.len() {
        return Err(codec_error("untracked locator summary has trailing bytes"));
    }
    if count == 0 && digest != [0; 32] {
        return Err(codec_error(
            "untracked locator summary has a non-empty digest with zero members",
        ));
    }
    Ok(LocatorSummary { count, digest })
}

fn locator_root_hash(bytes: &Bytes) -> [u8; 32] {
    *blake3::hash(bytes).as_bytes()
}

fn empty_locator_root() -> Result<(Bytes, [u8; 32]), LixError> {
    let bytes = encode_locator_root(&LocatorRoot::default())?;
    let hash = locator_root_hash(&bytes);
    Ok((bytes, hash))
}

pub(crate) fn empty_locator_root_hash() -> [u8; 32] {
    empty_locator_root()
        .expect("empty untracked locator root encoding is infallible")
        .1
}

fn update_locator_control(
    mut control: BranchHeadControl,
    root: &LocatorRoot,
    root_bytes: &Bytes,
) -> Result<BranchHeadControl, LixError> {
    if root.generation != control.untracked_locator_generation.saturating_add(1) {
        return Err(codec_error(
            "untracked locator generation did not advance from branch control",
        ));
    }
    control.untracked_locator_root = locator_root_hash(root_bytes);
    control.untracked_locator_generation = root.generation;
    control.untracked_locator_count = root.count;
    control.next_current_state_revision()
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

fn decode_locator_key(bytes: &Bytes) -> Result<DecodedIdentity, LixError> {
    let mut offset = 0;
    let branch_id = read_text(bytes, &mut offset, "locator branch")?;
    let tag = take(bytes, &mut offset, 1, "locator tag")?[0];
    if tag != LOCATOR_ENTRY_TAG {
        return Err(codec_error("untracked locator key has an invalid tag"));
    }
    let file_id = read_text(bytes, &mut offset, "locator file")?;
    let schema_key = read_text(bytes, &mut offset, "locator schema")?;
    let entity_pk_bytes = read_bytes(bytes, &mut offset, "locator entity")?;
    let entity_pk = crate::storage_codec::decode("untracked locator entity", entity_pk_bytes)?;
    if offset != bytes.len() {
        return Err(codec_error("untracked locator key has trailing bytes"));
    }
    let identity = DecodedIdentity {
        branch_id,
        schema_key,
        file_id: Some(file_id),
        entity_pk,
    };
    // The locator is only a derived locator, but its key grammar is part of
    // the corruption boundary.  Re-encoding catches alternate/non-canonical
    // component encodings that a permissive decoder could otherwise accept
    // as a different member.  Such a key must never be treated as an empty
    // file or allowed to alter a summary.
    let canonical = locator_entry_key(
        &identity.branch_id,
        identity
            .file_id
            .as_deref()
            .expect("locator file is present"),
        &identity.schema_key,
        &identity.entity_pk,
    )?;
    if canonical.0.as_ref() != bytes.as_ref() {
        return Err(codec_error(
            "untracked locator key is not in canonical format",
        ));
    }
    Ok(identity)
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
            untracked_locator_root: empty_locator_root_hash(),
            untracked_locator_generation: 0,
            untracked_locator_count: 0,
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

    fn file_descriptor(entity_pk: &EntityPk, timestamp: LixTimestamp) -> CurrentStateDeltaRef<'_> {
        CurrentStateDeltaRef {
            schema_key: FILE_DESCRIPTOR_SCHEMA_KEY,
            file_id: None,
            entity_pk,
            change_id: None,
            commit_id: None,
            untracked: false,
            deleted: false,
            created_at: timestamp,
            updated_at: timestamp,
            snapshot: JsonSlotRef::Inline("{}"),
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
        let mut owned_file_pks = Vec::new();
        let mut staged_deltas = deltas.to_vec();
        let mut staged_known_absent = known_absent.to_vec();
        if control.untracked_locator_generation == 0 {
            let file_ids = deltas
                .iter()
                .filter_map(|delta| delta.file_id)
                .collect::<BTreeSet<_>>();
            owned_file_pks.extend(file_ids.iter().map(|file_id| EntityPk::single(*file_id)));
            for (_file_id, entity_pk) in file_ids.into_iter().zip(owned_file_pks.iter()) {
                staged_deltas.push(file_descriptor(entity_pk, timestamp()));
                staged_known_absent.push(true);
            }
        }
        let updated_control = stage_untracked_deltas(
            &read,
            &mut writes,
            branch_id,
            control,
            &staged_deltas,
            &staged_known_absent,
        )
        .await?;
        crate::branch::stage_branch_head_control(&mut writes, branch_id, updated_control)?;
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .map_err(LixError::from)?;
        Ok(())
    }

    #[tokio::test]
    async fn ten_thousand_predecessor_reads_use_one_authoritative_schema_scan()
    -> Result<(), LixError> {
        const ROWS: usize = 10_000;
        let storage = StorageAdapter::new(Memory::new());
        let timestamp = timestamp();
        let entity_pks = (0..ROWS)
            .map(|ordinal| EntityPk::single(format!("phase-{ordinal:05}")))
            .collect::<Vec<_>>();
        let inserts = entity_pks
            .iter()
            .map(|entity_pk| untracked("phase_schema", None, entity_pk, r#"{"v":1}"#, timestamp))
            .collect::<Vec<_>>();
        let known_absent = vec![true; ROWS];
        commit_deltas(&storage, "main", &inserts, &known_absent).await?;
        let _ = take_untracked_mutation_read_profile();

        let updates = entity_pks
            .iter()
            .map(|entity_pk| untracked("phase_schema", None, entity_pk, r#"{"v":2}"#, timestamp))
            .collect::<Vec<_>>();
        let known_present = vec![false; ROWS];
        commit_deltas(&storage, "main", &updates, &known_present).await?;
        let profile = take_untracked_mutation_read_profile();
        assert_eq!(profile.previous_point_read_keys, 0);
        assert_eq!(profile.previous_scan_rows, ROWS as u64);
        assert!(profile.previous_scan_bytes > 0);
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
    async fn file_cascade_with_zero_members_uses_one_locator_scan() -> Result<(), LixError> {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch-empty";
        let file_id = "file-empty";
        let file_pk = EntityPk::single(file_id);
        let member_pk = EntityPk::single("member");
        let member = untracked(
            "schema",
            Some(file_id),
            &member_pk,
            r#"{"v":1}"#,
            timestamp(),
        );
        commit_deltas(&storage, branch_id, &[member], &[true]).await?;
        let mut member_delete = untracked(
            "schema",
            Some(file_id),
            &member_pk,
            r#"{"v":1}"#,
            timestamp(),
        );
        member_delete.deleted = true;
        commit_deltas(&storage, branch_id, &[member_delete], &[false]).await?;
        let delete = file_delete(&file_pk, timestamp());
        let _ = take_untracked_file_locator_read_profile();

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
            .expect("empty-file cascade control");
        let _updated_control =
            stage_untracked_deltas(&read, &mut writes, branch_id, control, &[delete], &[false])
                .await?;

        assert!(!writes.is_empty());
        let profile = take_untracked_file_locator_read_profile();
        assert_eq!(profile.root_point_reads, 1);
        assert_eq!(profile.locator_scans, 1);
        assert_eq!(profile.locator_rows, 0);
        assert_eq!(profile.authoritative_exact_reads, 0);
        Ok(())
    }

    #[tokio::test]
    async fn zero_member_summary_anchor_is_persisted_and_missing_fails_closed()
    -> Result<(), LixError> {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch-zero-anchor";
        let file_id = "file-zero-anchor";
        let entity = EntityPk::single("member");
        let member = untracked("schema", Some(file_id), &entity, r#"{"v":1}"#, timestamp());
        commit_deltas(&storage, branch_id, &[member], &[true]).await?;
        let mut member_delete =
            untracked("schema", Some(file_id), &entity, r#"{"v":1}"#, timestamp());
        member_delete.deleted = true;
        commit_deltas(&storage, branch_id, &[member_delete], &[false]).await?;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let summary_key = locator_summary_key(branch_id, file_id)?;
        let summary = PointReadPlan::new(UNTRACKED_FILE_LOCATOR_SPACE, &[summary_key.clone()])
            .materialize(&read, StorageGetOptions::default())
            .await?
            .value;
        let Some(Some(StorageProjectedValue::FullValue(bytes))) = summary.into_iter().next() else {
            return Err(codec_error(
                "zero-member locator summary anchor was not persisted",
            ));
        };
        assert_eq!(decode_locator_summary(&bytes)?.count, 0);

        let mut corrupt = storage.new_write_set();
        corrupt.delete(UNTRACKED_FILE_LOCATOR_SPACE, summary_key);
        storage
            .commit_write_set(corrupt, StorageWriteOptions::default())
            .await
            .map_err(LixError::from)?;
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let control = crate::branch::BranchHeadControlContext::new()
            .reader(&read)
            .load(branch_id)
            .await
            .map_err(LixError::from)?
            .expect("zero-anchor fixture control");
        let mut writes = storage.new_write_set();
        let file_entity = EntityPk::single(file_id);
        let delete = file_delete(&file_entity, timestamp());
        assert!(
            stage_untracked_deltas(&read, &mut writes, branch_id, control, &[delete], &[false])
                .await
                .is_err()
        );
        assert!(writes.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn fresh_file_transition_rejects_existing_zero_member_summary() -> Result<(), LixError> {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch-fresh-summary";
        let file_id = "file-fresh-summary";
        let live_file_ids = BTreeSet::from([file_id.to_owned()]);
        let empty = BTreeSet::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let mut writes = storage.new_write_set();
        let control = stage_untracked_deltas_with_live_file_ids(
            &read,
            &mut writes,
            branch_id,
            test_control(branch_id),
            &[],
            &[],
            &live_file_ids,
            &empty,
        )
        .await?;
        crate::branch::stage_branch_head_control(&mut writes, branch_id, control)?;
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .map_err(LixError::from)?;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let control = crate::branch::BranchHeadControlContext::new()
            .reader(&read)
            .load(branch_id)
            .await
            .map_err(LixError::from)?
            .expect("fresh-summary control");
        let mut writes = storage.new_write_set();
        let error = stage_untracked_deltas_with_live_file_ids(
            &read,
            &mut writes,
            branch_id,
            control,
            &[],
            &[],
            &live_file_ids,
            &empty,
        )
        .await
        .expect_err("a fresh lifecycle must not reuse an old zero-member summary");
        assert!(error.message.contains("already exists"));
        assert!(writes.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn member_delete_preserves_exact_multi_member_summary() -> Result<(), LixError> {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch-summary-count";
        let file_id = "file-summary-count";
        let first = EntityPk::single("first");
        let second = EntityPk::single("second");
        let now = timestamp();
        let rows = [
            untracked("schema-a", Some(file_id), &first, r#"{"v":1}"#, now),
            untracked("schema-b", Some(file_id), &second, r#"{"v":1}"#, now),
        ];
        commit_deltas(&storage, branch_id, &rows, &[true, true]).await?;
        let mut first_delete = untracked("schema-a", Some(file_id), &first, r#"{"v":1}"#, now);
        first_delete.deleted = true;
        commit_deltas(&storage, branch_id, &[first_delete], &[false]).await?;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let summary = PointReadPlan::new(
            UNTRACKED_FILE_LOCATOR_SPACE,
            &[locator_summary_key(branch_id, file_id)?],
        )
        .materialize(&read, StorageGetOptions::default())
        .await?
        .value;
        let Some(Some(StorageProjectedValue::FullValue(bytes))) = summary.into_iter().next() else {
            return Err(codec_error("multi-member locator summary is missing"));
        };
        let summary = decode_locator_summary(&bytes)?;
        let remaining_key = locator_entry_key(branch_id, file_id, "schema-b", &second)?;
        assert_eq!(summary.count, 1);
        assert_eq!(summary.digest, locator_key_digest(&remaining_key));
        Ok(())
    }

    #[test]
    fn zero_member_locator_metadata_rejects_nonzero_digest() -> Result<(), LixError> {
        let mut summary = encode_locator_summary(&LocatorSummary::default()).to_vec();
        *summary.last_mut().expect("summary has digest") = 1;
        assert!(decode_locator_summary(&Bytes::from(summary)).is_err());

        let mut root = encode_locator_root(&LocatorRoot::default())?.to_vec();
        *root.last_mut().expect("root has digest") = 1;
        assert!(decode_locator_root(&Bytes::from(root)).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn certified_tracked_file_lifecycle_publishes_zero_member_anchor() -> Result<(), LixError>
    {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch-certified-anchor";
        let file_id = "file-certified-anchor";
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let mut writes = storage.new_write_set();
        let control = test_control(branch_id);
        let live_file_ids = BTreeSet::from([file_id.to_owned()]);
        let deleted_file_ids = BTreeSet::new();
        let updated = stage_untracked_deltas_with_live_file_ids(
            &read,
            &mut writes,
            branch_id,
            control,
            &[],
            &[],
            &live_file_ids,
            &deleted_file_ids,
        )
        .await?;
        assert!(!writes.is_empty());
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .map_err(LixError::from)?;
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let summary = PointReadPlan::new(
            UNTRACKED_FILE_LOCATOR_SPACE,
            &[locator_summary_key(branch_id, file_id)?],
        )
        .materialize(&read, StorageGetOptions::default())
        .await?
        .value;
        let Some(Some(StorageProjectedValue::FullValue(bytes))) = summary.into_iter().next() else {
            return Err(codec_error(
                "certified tracked file anchor was not persisted",
            ));
        };
        assert_eq!(decode_locator_summary(&bytes)?.count, 0);
        assert_eq!(updated.untracked_locator_count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn certified_tracked_file_lifecycle_deletes_anchor_atomically() -> Result<(), LixError> {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch-certified-delete";
        let file_id = "file-certified-delete";
        let empty = BTreeSet::new();
        let live_file_ids = BTreeSet::from([file_id.to_owned()]);
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let mut writes = storage.new_write_set();
        let control = stage_untracked_deltas_with_live_file_ids(
            &read,
            &mut writes,
            branch_id,
            test_control(branch_id),
            &[],
            &[],
            &live_file_ids,
            &empty,
        )
        .await?;
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .map_err(LixError::from)?;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let mut writes = storage.new_write_set();
        let deleted = BTreeSet::from([file_id.to_owned()]);
        stage_untracked_deltas_with_live_file_ids(
            &read,
            &mut writes,
            branch_id,
            control,
            &[],
            &[],
            &empty,
            &deleted,
        )
        .await?;
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .map_err(LixError::from)?;
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let summary = PointReadPlan::new(
            UNTRACKED_FILE_LOCATOR_SPACE,
            &[locator_summary_key(branch_id, file_id)?],
        )
        .materialize(&read, StorageGetOptions::default())
        .await?
        .value;
        assert!(summary.into_iter().next().flatten().is_none());
        Ok(())
    }

    #[tokio::test]
    async fn missing_summary_in_late_file_leaves_valid_file_unstaged() -> Result<(), LixError> {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch-summary-atomic";
        let first_file_id = "file-summary-first";
        let second_file_id = "file-summary-second";
        let first_entity = EntityPk::single("first");
        let second_entity = EntityPk::single("second");
        let first = untracked(
            "schema",
            Some(first_file_id),
            &first_entity,
            r#"{"v":1}"#,
            timestamp(),
        );
        let second = untracked(
            "schema",
            Some(second_file_id),
            &second_entity,
            r#"{"v":1}"#,
            timestamp(),
        );
        commit_deltas(&storage, branch_id, &[first, second], &[true, true]).await?;

        let mut corrupt = storage.new_write_set();
        corrupt.delete(
            UNTRACKED_FILE_LOCATOR_SPACE,
            locator_summary_key(branch_id, second_file_id)?,
        );
        storage
            .commit_write_set(corrupt, StorageWriteOptions::default())
            .await
            .map_err(LixError::from)?;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let control = crate::branch::BranchHeadControlContext::new()
            .reader(&read)
            .load(branch_id)
            .await
            .map_err(LixError::from)?
            .expect("summary atomicity fixture control");
        let first_insert = EntityPk::single("first-new");
        let second_insert = EntityPk::single("second-new");
        let first_update = untracked(
            "schema",
            Some(first_file_id),
            &first_insert,
            r#"{"v":2}"#,
            timestamp(),
        );
        let second_update = untracked(
            "schema",
            Some(second_file_id),
            &second_insert,
            r#"{"v":2}"#,
            timestamp(),
        );
        let mut writes = storage.new_write_set();
        let error = stage_untracked_deltas(
            &read,
            &mut writes,
            branch_id,
            control,
            &[first_update, second_update],
            &[true, true],
        )
        .await
        .expect_err("a missing later summary must fail closed");
        assert!(error.message.contains("summary is missing"));
        assert!(
            writes.is_empty(),
            "summary validation failure must stage no locator or authoritative writes"
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
        let _ = take_untracked_file_locator_read_profile();
        commit_deltas(
            &storage,
            branch_id,
            &[replacement, same_batch_new, delete],
            &[false, true, false],
        )
        .await?;

        let profile = take_untracked_file_locator_read_profile();
        assert_eq!(profile.root_point_reads, 1);
        assert_eq!(profile.locator_scans, 1);
        assert_eq!(profile.locator_rows, 2);
        assert_eq!(profile.authoritative_exact_reads, 2);

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
        let now = timestamp();
        let value_pk = EntityPk::single("value");
        let file_id = "file-a";
        let value_delta = untracked("schema", Some(file_id), &value_pk, r#"{"ok":true}"#, now);
        commit_deltas(&storage, branch_id, &[value_delta], &[true]).await?;
        let mut malformed_key = locator_entry_prefix(branch_id, file_id)?;
        malformed_key.extend_from_slice(b"malformed");
        let mut corrupt = storage.new_write_set();
        corrupt.put(
            UNTRACKED_FILE_LOCATOR_SPACE,
            StorageKey(Bytes::from(malformed_key)),
            StorageValue {
                bytes: Bytes::from_static(LOCATOR_ENTRY_MARKER),
            },
        );
        storage
            .commit_write_set(corrupt, StorageWriteOptions::default())
            .await
            .map_err(LixError::from)?;
        let file_entity = EntityPk::single(file_id);
        let delete = file_delete(&file_entity, now);
        let _ = take_untracked_file_locator_read_profile();
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
            .expect("malformed-key cascade control");
        assert!(
            stage_untracked_deltas(&read, &mut writes, branch_id, control, &[delete], &[false],)
                .await
                .is_err()
        );
        assert!(writes.is_empty());
        let profile = take_untracked_file_locator_read_profile();
        assert_eq!(profile.root_point_reads, 1);
        assert_eq!(profile.locator_scans, 1);
        assert_eq!(profile.authoritative_exact_reads, 0);
        Ok(())
    }

    #[tokio::test]
    async fn file_cascade_malformed_nonmember_value_fails_after_match_without_staging()
    -> Result<(), LixError> {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "branch-bad-value";
        let file_id = "file-a";
        let valid_pk = EntityPk::single("valid");
        let now = timestamp();
        let valid_delta = untracked("schema", Some(file_id), &valid_pk, r#"{"ok":true}"#, now);
        commit_deltas(&storage, branch_id, &[valid_delta], &[true]).await?;
        let valid_key = StorageKey(Bytes::from(encode_key(
            branch_id,
            "schema",
            Some(file_id),
            &valid_pk,
        )?));
        let mut corrupt = storage.new_write_set();
        corrupt.put(
            UNTRACKED_ROW_SPACE,
            valid_key.clone(),
            StorageValue {
                bytes: Bytes::from_static(b"malformed"),
            },
        );
        storage
            .commit_write_set(corrupt, StorageWriteOptions::default())
            .await
            .map_err(LixError::from)?;

        let file_entity = EntityPk::single(file_id);
        let delete = file_delete(&file_entity, now);
        let _ = take_untracked_file_locator_read_profile();
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
            .expect("malformed-authority cascade control");
        assert!(
            stage_untracked_deltas(&read, &mut writes, branch_id, control, &[delete], &[false],)
                .await
                .is_err()
        );
        assert!(
            writes.is_empty(),
            "terminal decode failure must not expose locally planned deletions"
        );
        let profile = take_untracked_file_locator_read_profile();
        assert_eq!(profile.locator_scans, 1);
        assert_eq!(profile.locator_rows, 1);
        assert_eq!(profile.authoritative_exact_reads, 1);

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
            rows.push((key, value));
        }
        commit_raw_rows(&storage, rows).await?;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let mut rebuild_writes = storage.new_write_set();
        let live_file_ids = BTreeSet::from([target_file_id.to_owned(), "file-other".to_owned()]);
        let rebuilt = rebuild_untracked_file_locator(
            &read,
            &mut rebuild_writes,
            branch_id,
            test_control(branch_id),
            &live_file_ids,
        )
        .await?;
        crate::branch::stage_branch_head_control(&mut rebuild_writes, branch_id, rebuilt)?;
        storage
            .commit_write_set(rebuild_writes, StorageWriteOptions::default())
            .await
            .map_err(LixError::from)?;

        let delete = file_delete(&file_pk, now);
        let _ = take_untracked_file_locator_read_profile();
        commit_deltas(&storage, branch_id, &[delete], &[false]).await?;
        let profile = take_untracked_file_locator_read_profile();
        assert_eq!(profile.root_point_reads, 1);
        assert_eq!(profile.locator_scans, 1);
        assert_eq!(profile.locator_rows, 2);
        assert_eq!(profile.authoritative_exact_reads, 2);

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
    #[ignore = "legacy full-authority pagination fixture; locator pagination is covered by the strict cursor tests"]
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
        let _ = take_untracked_file_locator_read_profile();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let mut writes = storage.new_write_set();
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
        let profile = take_untracked_file_locator_read_profile();
        assert_eq!(profile.root_point_reads, 1);
        assert_eq!(profile.locator_scans, 1);
        assert_eq!(profile.locator_rows, 0);
        assert_eq!(profile.authoritative_exact_reads, 0);
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
        let key = locator_entry_key(branch_id, file_id, "schema", &row_pk)?;
        let entry = StorageReadEntry {
            key,
            value: StorageProjectedValue::FullValue(Bytes::from_static(LOCATOR_ENTRY_MARKER)),
        };
        let read = ScriptedScanRead::new([
            StorageScanChunk {
                entries: Vec::new(),
                has_more: false,
            },
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
        let _ = take_untracked_file_locator_read_profile();
        let mut writes = StorageWriteSet::new();
        assert!(
            stage_untracked_deltas(
                &read,
                &mut writes,
                branch_id,
                test_control(branch_id),
                &[delete],
                &[true],
            )
            .await
            .is_err()
        );
        assert!(writes.is_empty());
        let profile = take_untracked_file_locator_read_profile();
        assert_eq!(profile.root_point_reads, 1);
        assert_eq!(profile.locator_scans, 2);
        assert_eq!(profile.locator_rows, 1);
        assert_eq!(profile.authoritative_exact_reads, 0);
        Ok(())
    }

    #[tokio::test]
    async fn file_cascade_empty_has_more_page_fails_with_zero_staged_writes() -> Result<(), LixError>
    {
        let branch_id = "branch-empty-page";
        let file_pk = EntityPk::single("file-target");
        let read = ScriptedScanRead::new([
            StorageScanChunk {
                entries: Vec::new(),
                has_more: false,
            },
            StorageScanChunk {
                entries: Vec::new(),
                has_more: true,
            },
        ]);
        let delete = file_delete(&file_pk, timestamp());
        let _ = take_untracked_file_locator_read_profile();
        let mut writes = StorageWriteSet::new();
        assert!(
            stage_untracked_deltas(
                &read,
                &mut writes,
                branch_id,
                test_control(branch_id),
                &[delete],
                &[true],
            )
            .await
            .is_err()
        );
        assert!(writes.is_empty());
        let profile = take_untracked_file_locator_read_profile();
        assert_eq!(profile.root_point_reads, 1);
        assert_eq!(profile.locator_scans, 2);
        assert_eq!(profile.locator_rows, 0);
        assert_eq!(profile.authoritative_exact_reads, 0);
        Ok(())
    }

    #[tokio::test]
    async fn file_cascade_late_locator_page_corruption_fails_with_zero_staged_writes()
    -> Result<(), LixError> {
        let branch_id = "branch-late-locator-corruption";
        let file_id = "file-target";
        let first_pk = EntityPk::single("first");
        let second_pk = EntityPk::single("second");
        let first = StorageReadEntry {
            key: locator_entry_key(branch_id, file_id, "schema", &first_pk)?,
            value: StorageProjectedValue::FullValue(Bytes::from_static(LOCATOR_ENTRY_MARKER)),
        };
        let second = StorageReadEntry {
            key: locator_entry_key(branch_id, file_id, "schema", &second_pk)?,
            value: StorageProjectedValue::FullValue(Bytes::from_static(b"malformed-marker")),
        };
        let read = ScriptedScanRead::new([
            StorageScanChunk {
                entries: Vec::new(),
                has_more: false,
            },
            StorageScanChunk {
                entries: vec![first],
                has_more: true,
            },
            StorageScanChunk {
                entries: vec![second],
                has_more: false,
            },
        ]);
        let file_pk = EntityPk::single(file_id);
        let delete = file_delete(&file_pk, timestamp());
        let _ = take_untracked_file_locator_read_profile();
        let mut writes = StorageWriteSet::new();
        assert!(
            stage_untracked_deltas(
                &read,
                &mut writes,
                branch_id,
                test_control(branch_id),
                &[delete],
                &[true],
            )
            .await
            .is_err()
        );
        assert!(writes.is_empty());
        let profile = take_untracked_file_locator_read_profile();
        assert_eq!(profile.root_point_reads, 1);
        assert_eq!(profile.locator_scans, 2);
        assert_eq!(profile.locator_rows, 1);
        assert_eq!(profile.authoritative_exact_reads, 0);
        Ok(())
    }

    #[tokio::test]
    async fn locator_cascade_is_file_bounded_and_exact_reads_are_batched() -> Result<(), LixError> {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "locator-bounded";
        let file_id = "file-target";
        let first = EntityPk::single("first");
        let second = EntityPk::single("second");
        let rows = [
            untracked("schema-a", Some(file_id), &first, r#"{"v":1}"#, timestamp()),
            untracked(
                "schema-b",
                Some(file_id),
                &second,
                r#"{"v":1}"#,
                timestamp(),
            ),
        ];
        commit_deltas(&storage, branch_id, &rows, &[true, true]).await?;
        let _ = take_untracked_file_locator_read_profile();
        let file_entity = EntityPk::single(file_id);
        let file_delete = file_delete(&file_entity, timestamp());
        commit_deltas(&storage, branch_id, &[file_delete], &[false]).await?;
        let profile = take_untracked_file_locator_read_profile();
        assert_eq!(profile.root_point_reads, 1);
        assert_eq!(profile.locator_scans, 1);
        assert_eq!(profile.locator_rows, 2);
        assert_eq!(profile.authoritative_exact_reads, 2);
        assert_eq!(profile.rebuilt_rows, 0);
        Ok(())
    }

    #[tokio::test]
    async fn explicit_rebuild_retains_tracked_zero_member_anchor() -> Result<(), LixError> {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "locator-rebuild-zero";
        let file_id = "file-rebuild-zero";
        let file_entity = EntityPk::single(file_id);
        let descriptor = file_descriptor(&file_entity, timestamp());
        commit_deltas(&storage, branch_id, &[descriptor], &[true]).await?;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let control = crate::branch::BranchHeadControlContext::new()
            .reader(&read)
            .load(branch_id)
            .await
            .map_err(LixError::from)?
            .expect("zero-member rebuild fixture control");
        let mut writes = storage.new_write_set();
        let live_file_ids = BTreeSet::from([file_id.to_owned()]);
        let rebuilt_control =
            rebuild_untracked_file_locator(&read, &mut writes, branch_id, control, &live_file_ids)
                .await?;
        crate::branch::stage_branch_head_control(&mut writes, branch_id, rebuilt_control)?;
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .map_err(LixError::from)?;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let summary = PointReadPlan::new(
            UNTRACKED_FILE_LOCATOR_SPACE,
            &[locator_summary_key(branch_id, file_id)?],
        )
        .materialize(&read, StorageGetOptions::default())
        .await?
        .value;
        let Some(Some(StorageProjectedValue::FullValue(bytes))) = summary.into_iter().next() else {
            return Err(codec_error(
                "rebuild dropped the tracked zero-member anchor",
            ));
        };
        assert_eq!(decode_locator_summary(&bytes)?.count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn malformed_locator_marker_fails_closed_without_staging() -> Result<(), LixError> {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "locator-corrupt";
        let file_id = "file-corrupt";
        let entity = EntityPk::single("member");
        let row = untracked("schema", Some(file_id), &entity, r#"{"v":1}"#, timestamp());
        commit_deltas(&storage, branch_id, &[row], &[true]).await?;
        let locator_key = locator_entry_key(branch_id, file_id, "schema", &entity)?;
        let mut corrupt = storage.new_write_set();
        corrupt.put(
            UNTRACKED_FILE_LOCATOR_SPACE,
            locator_key,
            StorageValue {
                bytes: Bytes::from_static(b"not-a-marker"),
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
        let file_entity = EntityPk::single(file_id);
        let delete = file_delete(&file_entity, timestamp());
        assert!(
            stage_untracked_deltas(
                &read,
                &mut writes,
                branch_id,
                crate::branch::BranchHeadControlContext::new()
                    .reader(&read)
                    .load(branch_id)
                    .await
                    .map_err(LixError::from)?
                    .expect("locator corruption fixture control"),
                &[delete],
                &[false],
            )
            .await
            .is_err()
        );
        assert!(writes.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn explicit_locator_rebuild_replaces_projection_from_authority() -> Result<(), LixError> {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "locator-rebuild";
        let file_id = "file-rebuild";
        let entity = EntityPk::single("member");
        let row = untracked("schema", Some(file_id), &entity, r#"{"v":1}"#, timestamp());
        commit_deltas(&storage, branch_id, &[row], &[true]).await?;
        let stale_key = locator_entry_key(branch_id, "stale", "schema", &entity)?;
        let mut stale_writes = storage.new_write_set();
        stale_writes.put(
            UNTRACKED_FILE_LOCATOR_SPACE,
            stale_key,
            StorageValue {
                bytes: Bytes::from_static(LOCATOR_ENTRY_MARKER),
            },
        );
        storage
            .commit_write_set(stale_writes, StorageWriteOptions::default())
            .await
            .map_err(LixError::from)?;
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .map_err(LixError::from)?;
        let control = crate::branch::BranchHeadControlContext::new()
            .reader(&read)
            .load(branch_id)
            .await
            .map_err(LixError::from)?
            .expect("rebuild fixture control");
        let mut writes = storage.new_write_set();
        let live_file_ids = BTreeSet::from([file_id.to_owned()]);
        let rebuilt_control =
            rebuild_untracked_file_locator(&read, &mut writes, branch_id, control, &live_file_ids)
                .await?;
        assert_eq!(rebuilt_control.untracked_locator_count, 1);
        assert_eq!(take_untracked_file_locator_read_profile().rebuilt_rows, 1);
        assert!(!writes.is_empty());
        Ok(())
    }
}
