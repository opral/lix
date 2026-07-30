//! V18 row-addressable current state with schema-level file membership.
//!
//! V12 packed every file member of one logical entity into a group. That made
//! a logical-PK lookup cheap, but it also made every normal commit read,
//! decode, merge, and rewrite each predecessor group. V17 keeps the fixed row
//! value codec and branch-control publication fence, makes a full row identity
//! the physical mutation unit, and stores each value only in the authoritative
//! file-first row index.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::Range;
use std::sync::Arc;

use crate::wasm::WasmCreateContext;
use base64::Engine as _;
use bytes::Bytes;
use smallvec::SmallVec;
use tracing::Instrument as _;

use crate::storage_adapter::{
    BufferRange, DeferredFinalPutPage, DeferredFinalPutSource, EncodedMutationBatch, EncodedPut,
    PutBatch, PutEntry,
};
use crate::tracked_state::TrackedStateReadColumns;
use crate::wasm::WasmCertifiedEntityBatch;

use super::*;

pub(crate) const HOT_ROW_NAMESPACE: &str = "live_state.hot_row.v17";
pub(crate) const HOT_FILE_NAMESPACE: &str = "live_state.hot_file_schema.v18";
pub(crate) const HOT_DIFF_NAMESPACE: &str = "live_state.hot_diff.v17";
pub(crate) const HOT_COLLECTION_CONTROL_NAMESPACE: &str = "live_state.hot_collection_control.v1";
pub(crate) const HOT_ROW_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0004_001b), HOT_ROW_NAMESPACE);
/// Conservative `(branch, generation, schema)` file-membership markers.
///
/// The authoritative hot row owns every value and file identity. Markers are
/// never removed within a generation, so they may produce a harmless false
/// positive after the last file member is deleted but cannot hide live rows.
pub(crate) const HOT_FILE_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0004_001c), HOT_FILE_NAMESPACE);
/// Reserved for the row-level first-before working-diff index.
pub(crate) const HOT_DIFF_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0004_001d), HOT_DIFF_NAMESPACE);
pub(crate) const HOT_COLLECTION_CONTROL_SPACE: StorageSpace = StorageSpace::new(
    StorageSpaceId(0x0004_0023),
    HOT_COLLECTION_CONTROL_NAMESPACE,
);
const HOT_DENSE_SCAN_MIN_IDENTITIES: usize = 64;
const HOT_DENSE_SCAN_MAX_OVERREAD: usize = 2;
const HOT_DIFF_SEGMENT_VERSION: u8 = 1;
const HOT_DIFF_SEGMENT_HEADER_BYTES: usize = 5;
const HOT_DIFF_SEGMENT_MAX_BYTES: usize = 256 * 1024;
const HOT_DIFF_SEGMENT_MAX_IDENTITIES: u32 = 4_096;
// Real-repository profiling showed that smaller batches contribute negligible
// storage amplification, so retain their allocation-free direct-key path.
const HOT_DIFF_PACK_MIN_IDENTITIES: usize = 64;
const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const CERTIFIED_ENTITY_BATCH_MAGIC_V1: &[u8; 4] = b"CEB1";
const CERTIFIED_ENTITY_BATCH_MAGIC_V2: &[u8; 4] = b"CEB2";
pub(crate) const CERTIFIED_ENTITY_BATCH_SPACE: StorageSpace = StorageSpace::new(
    StorageSpaceId(0x0004_001f),
    "live_state.certified_entity_batch.v1",
);
pub(crate) const CERTIFIED_ENTITY_BATCH_MANIFEST_SPACE: StorageSpace = StorageSpace::new(
    StorageSpaceId(0x0004_0021),
    "live_state.certified_entity_batch_manifest.v1",
);
pub(crate) const CERTIFIED_ENTITY_BATCH_PAGE_SPACE: StorageSpace = StorageSpace::new(
    StorageSpaceId(0x0004_0022),
    "live_state.certified_entity_batch_page.v1",
);
const DEFERRED_FRESH_HOT_ROWS_PER_PAGE: usize = 4_096;
const DEFERRED_FRESH_HOT_SPACES: [StorageSpace; 3] =
    [HOT_ROW_SPACE, HOT_FILE_SPACE, HOT_DIFF_SPACE];

pub(crate) struct DeferredFreshHotRowRef<'a> {
    pub(crate) branch_id: &'a str,
    pub(crate) delta: CurrentStateDeltaRef<'a>,
}

pub(crate) trait DeferredFreshHotRows: Send + Sync {
    fn row(&self, index: usize) -> DeferredFreshHotRowRef<'_>;
}

pub(crate) struct CertifiedEntityBatchFileRef<'a> {
    pub(crate) branch_id: &'a str,
    pub(crate) file_id: &'a str,
    pub(crate) batches: &'a [WasmCertifiedEntityBatch],
}

/// A certified fresh-file hot-state publication whose row owner is attached
/// only after every other commit materializer has finished reading it.
///
/// Keeping this as row ordinals avoids manufacturing expanded keys and values
/// while the prepared transaction batch is still live.
pub(crate) struct DeferredFreshHotPlan {
    branch_id: String,
    generation: CommitId,
    checkpoint_commit_id: Option<CommitId>,
    row_indices: Vec<usize>,
    file_schema_keys: Vec<String>,
    put_count: u64,
    written_bytes: u64,
    backend_capacity_hint_bytes: usize,
}

impl DeferredFreshHotPlan {
    pub(crate) fn new(
        branch_id: &str,
        generation: CommitId,
        state_rows: &dyn DeferredFreshHotRows,
        row_indices: &[usize],
        certified_file_id: &str,
        validated_absence_guards: &[TrackedStateKeyRef<'_>],
        checkpoint_commit_id: Option<CommitId>,
        coverage: &mut WorkingDiffIndexCoverage,
    ) -> Result<Self, LixError> {
        if row_indices.is_empty() {
            return Err(head_value_error(
                "deferred fresh hot publication has no rows",
            ));
        }
        let scope = hot_scope_prefix(branch_id, generation);
        let diff_scope = checkpoint_commit_id.map(|checkpoint_commit_id| {
            encode_working_diff_scope_prefix(branch_id, checkpoint_commit_id, generation)
        });
        let mut next_coverage = *coverage;
        let mut written_bytes = 0_u64;
        let mut backend_capacity_hint_bytes = 0_usize;
        let mut put_count = 0_u64;
        let mut coverage_key = Vec::new();
        let mut file_schema_keys = BTreeSet::new();
        for &row_index in row_indices {
            let row = state_rows.row(row_index);
            let delta =
                deferred_fresh_delta(row, branch_id, certified_file_id, validated_absence_guards)?;
            delta.validate()?;
            if delta.deleted {
                return Err(head_value_error(
                    "deferred fresh hot publication requires live rows",
                ));
            }
            let key_len = encoded_hot_identity_key_len(
                scope.len(),
                delta.schema_key,
                delta.entity_pk,
                delta.file_id,
            )
            .ok_or_else(|| head_value_error("deferred fresh hot key length overflowed"))?;
            let value_len =
                checked_add_hot_next_value_capacity(0, &delta, checkpoint_commit_id.is_some())
                    .ok_or_else(|| {
                        head_value_error("deferred fresh hot value length overflowed")
                    })?;
            if delta.file_id.is_some() {
                file_schema_keys.insert(delta.schema_key.to_owned());
            }
            written_bytes = written_bytes
                .checked_add(u64::try_from(value_len).unwrap_or(u64::MAX))
                .ok_or_else(|| head_value_error("deferred fresh hot written bytes overflowed"))?;
            backend_capacity_hint_bytes = backend_capacity_hint_bytes
                .checked_add(key_len)
                .and_then(|capacity| capacity.checked_add(value_len))
                .and_then(|capacity| capacity.checked_add(16))
                .ok_or_else(|| {
                    head_value_error("deferred fresh hot backend capacity overflowed")
                })?;
            put_count = put_count
                .checked_add(1)
                .ok_or_else(|| head_value_error("deferred fresh hot put count overflowed"))?;
            if let Some(diff_scope) = diff_scope.as_deref() {
                coverage_key.clear();
                let range = append_hot_diff_key_parts(
                    &mut coverage_key,
                    diff_scope,
                    delta.schema_key,
                    delta.entity_pk,
                    delta.file_id,
                );
                next_coverage
                    .add_encoded_group_key(&coverage_key[range])
                    .ok_or_else(|| {
                        head_value_error("deferred fresh hot working-diff count overflowed")
                    })?;
                backend_capacity_hint_bytes = backend_capacity_hint_bytes
                    .checked_add(coverage_key.len())
                    .and_then(|capacity| capacity.checked_add(16))
                    .ok_or_else(|| {
                        head_value_error("deferred fresh hot diff capacity overflowed")
                    })?;
                put_count = put_count
                    .checked_add(1)
                    .ok_or_else(|| head_value_error("deferred fresh hot put count overflowed"))?;
            }
        }
        for schema_key in &file_schema_keys {
            let marker_len = scope
                .len()
                .checked_add(encoded_key_bytes_len(schema_key.as_bytes()).ok_or_else(|| {
                    head_value_error("deferred fresh hot schema marker length overflowed")
                })?)
                .ok_or_else(|| {
                    head_value_error("deferred fresh hot schema marker length overflowed")
                })?;
            backend_capacity_hint_bytes = backend_capacity_hint_bytes
                .checked_add(marker_len)
                .and_then(|capacity| capacity.checked_add(16))
                .ok_or_else(|| {
                    head_value_error("deferred fresh hot backend capacity overflowed")
                })?;
            put_count = put_count
                .checked_add(1)
                .ok_or_else(|| head_value_error("deferred fresh hot put count overflowed"))?;
        }
        *coverage = next_coverage;
        Ok(Self {
            branch_id: branch_id.to_owned(),
            generation,
            checkpoint_commit_id,
            row_indices: row_indices.to_vec(),
            file_schema_keys: file_schema_keys.into_iter().collect(),
            put_count,
            written_bytes,
            backend_capacity_hint_bytes,
        })
    }

    pub(crate) fn into_source(
        self,
        state_rows: Arc<dyn DeferredFreshHotRows>,
    ) -> Box<dyn DeferredFinalPutSource> {
        Box::new(DeferredFreshHotSource {
            plan: self,
            state_rows,
            cursor: 0,
            schema_markers_emitted: false,
            pending_pages: VecDeque::new(),
        })
    }
}

struct DeferredFreshHotSource {
    plan: DeferredFreshHotPlan,
    state_rows: Arc<dyn DeferredFreshHotRows>,
    cursor: usize,
    schema_markers_emitted: bool,
    pending_pages: VecDeque<DeferredFinalPutPage>,
}

impl DeferredFinalPutSource for DeferredFreshHotSource {
    fn target_spaces(&self) -> &[StorageSpace] {
        &DEFERRED_FRESH_HOT_SPACES
    }

    fn put_count(&self) -> u64 {
        self.plan.put_count
    }

    fn written_bytes(&self) -> u64 {
        self.plan.written_bytes
    }

    fn backend_capacity_hint_bytes(&self) -> usize {
        self.plan.backend_capacity_hint_bytes
    }

    fn next_page(&mut self) -> Option<DeferredFinalPutPage> {
        if let Some(page) = self.pending_pages.pop_front() {
            return Some(page);
        }
        if !self.schema_markers_emitted {
            self.schema_markers_emitted = true;
            if !self.plan.file_schema_keys.is_empty() {
                let scope = hot_scope_prefix(&self.plan.branch_id, self.plan.generation);
                return Some(DeferredFinalPutPage {
                    space: HOT_FILE_SPACE,
                    entries: PutBatch {
                        entries: self
                            .plan
                            .file_schema_keys
                            .iter()
                            .map(|schema_key| PutEntry {
                                key: StorageKey(Bytes::from(encode_hot_file_schema_key(
                                    &scope, schema_key,
                                ))),
                                value: StorageValue {
                                    bytes: Bytes::new(),
                                },
                            })
                            .collect(),
                    },
                });
            }
        }
        if self.cursor == self.plan.row_indices.len() {
            return None;
        }
        let end = self
            .cursor
            .saturating_add(DEFERRED_FRESH_HOT_ROWS_PER_PAGE)
            .min(self.plan.row_indices.len());
        let indices = &self.plan.row_indices[self.cursor..end];
        self.cursor = end;
        let scope = hot_scope_prefix(&self.plan.branch_id, self.plan.generation);
        let diff_scope = self.plan.checkpoint_commit_id.map(|checkpoint_commit_id| {
            encode_working_diff_scope_prefix(
                &self.plan.branch_id,
                checkpoint_commit_id,
                self.plan.generation,
            )
        });
        let mut key_capacity = 0_usize;
        let mut value_capacity = 0_usize;
        for &row_index in indices {
            let delta = self.state_rows.row(row_index).delta;
            key_capacity = key_capacity.saturating_add(
                encoded_hot_identity_key_len(
                    scope.len(),
                    delta.schema_key,
                    delta.entity_pk,
                    delta.file_id,
                )
                .unwrap_or(0),
            );
            if let Some(diff_scope) = diff_scope.as_deref() {
                key_capacity = key_capacity.saturating_add(
                    encoded_hot_identity_key_len(
                        diff_scope.len(),
                        delta.schema_key,
                        delta.entity_pk,
                        delta.file_id,
                    )
                    .unwrap_or(0),
                );
            }
            value_capacity = checked_add_hot_next_value_capacity(
                value_capacity,
                &delta,
                self.plan.checkpoint_commit_id.is_some(),
            )
            .unwrap_or(0);
        }
        let mut key_bytes = Vec::with_capacity(key_capacity);
        let mut value_bytes = Vec::with_capacity(value_capacity);
        let mut key_ranges = Vec::with_capacity(indices.len());
        let mut value_ranges = Vec::with_capacity(indices.len());
        let mut diff_key_ranges =
            Vec::with_capacity(diff_scope.as_ref().map_or(0, |_| indices.len()));
        for &row_index in indices {
            let delta = self.state_rows.row(row_index).delta;
            key_ranges.push(append_hot_mutation_identity(&mut key_bytes, &scope, &delta));
            value_ranges.push(
                append_head_value(
                    &mut value_bytes,
                    &delta.value_ref(
                        delta.created_at,
                        if self.plan.checkpoint_commit_id.is_some() {
                            WorkingDiffBaseline::BeforeAbsent
                        } else {
                            WorkingDiffBaseline::Disabled
                        },
                    ),
                )
                .expect("deferred fresh hot rows were validated before staging"),
            );
            if let Some(diff_scope) = diff_scope.as_deref() {
                diff_key_ranges.push(append_hot_diff_key_parts(
                    &mut key_bytes,
                    diff_scope,
                    delta.schema_key,
                    delta.entity_pk,
                    delta.file_id,
                ));
            }
        }
        let key_bytes = Bytes::from(key_bytes);
        let value_bytes = Bytes::from(value_bytes);
        let mut row_entries = Vec::with_capacity(indices.len());
        for (identity, value) in key_ranges.into_iter().zip(value_ranges) {
            let value = StorageValue {
                bytes: value_bytes.slice(value.clone()),
            };
            row_entries.push(PutEntry {
                key: StorageKey(key_bytes.slice(
                    identity.row_key.offset()..identity.row_key.offset() + identity.row_key.len(),
                )),
                value: value.clone(),
            });
        }
        if !diff_key_ranges.is_empty() {
            self.pending_pages.push_back(DeferredFinalPutPage {
                space: HOT_DIFF_SPACE,
                entries: PutBatch {
                    entries: diff_key_ranges
                        .into_iter()
                        .map(|key| PutEntry {
                            key: StorageKey(key_bytes.slice(key)),
                            value: StorageValue {
                                bytes: Bytes::new(),
                            },
                        })
                        .collect(),
                },
            });
        }
        Some(DeferredFinalPutPage {
            space: HOT_ROW_SPACE,
            entries: PutBatch {
                entries: row_entries,
            },
        })
    }
}

fn deferred_fresh_delta<'a>(
    row: DeferredFreshHotRowRef<'a>,
    branch_id: &str,
    certified_file_id: &str,
    validated_absence_guards: &[TrackedStateKeyRef<'_>],
) -> Result<CurrentStateDeltaRef<'a>, LixError> {
    let delta = row.delta;
    let file_is_certified = delta.file_id == Some(certified_file_id);
    let identity_is_guarded = validated_absence_guards
        .binary_search_by(|guard| {
            guard
                .schema_key
                .cmp(delta.schema_key)
                .then_with(|| guard.entity_pk.cmp(delta.entity_pk))
                .then_with(|| guard.file_id.cmp(&delta.file_id))
        })
        .is_ok();
    if row.branch_id != branch_id || (!file_is_certified && !identity_is_guarded) || delta.untracked
    {
        return Err(head_value_error(
            "deferred fresh hot row escaped its certified or guarded identity scope",
        ));
    }
    if delta.commit_id.is_none() || delta.change_id.is_none() {
        return Err(head_value_error(
            "deferred fresh hot tracked row is missing commit identity",
        ));
    }
    Ok(delta)
}

/// Publishes one immutable semantic owner per certified file batch.
///
/// The storage value is the authority for the batch rows. Current-state and
/// history readers decode rows lazily; later sparse mutations remain ordinary
/// hot rows and therefore form overlays instead of rewriting this value.
pub(crate) async fn stage_certified_entity_batches(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    file_writes: &[CertifiedEntityBatchFileRef<'_>],
    controls: &BTreeMap<String, BranchHeadControl>,
    observations: &BTreeMap<String, crate::branch::BranchHeadControlObservation>,
    commit_created_at: &BTreeMap<CommitId, LixTimestamp>,
) -> Result<(), LixError> {
    let mut complete_manifest_suffixes = BTreeMap::<String, Vec<Vec<u8>>>::new();
    for file in file_writes {
        for batch in file
            .batches
            .iter()
            .filter(|batch| batch.complete_file_state)
        {
            let mut suffix = Vec::new();
            append_batch_text(&mut suffix, file.file_id)?;
            suffix.extend_from_slice(&batch.format.to_le_bytes());
            complete_manifest_suffixes
                .entry(file.branch_id.to_owned())
                .or_default()
                .push(suffix);
        }
    }

    let needs_branch_creation_donor = controls.keys().any(|branch_id| {
        observations
            .get(branch_id)
            .is_none_or(|observation| observation.control.is_none())
    });
    let durable_controls = if needs_branch_creation_donor {
        BranchHeadControlContext::new().reader(read).scan().await?
    } else {
        Vec::new()
    };

    let mut inherited_manifests = BTreeMap::new();
    for (branch_id, control) in controls {
        let source_generations = observations
            .get(branch_id)
            .and_then(|observation| observation.control)
            .map(|previous| BTreeSet::from([previous.generation]))
            .unwrap_or_else(|| {
                durable_controls
                    .iter()
                    .filter(|(_, candidate)| candidate.head_commit_id == control.head_commit_id)
                    .map(|(_, candidate)| candidate.generation)
                    .collect()
            });
        for source_generation in source_generations
            .into_iter()
            .filter(|generation| *generation != control.generation)
        {
            let previous_prefix = source_generation.as_uuid().as_bytes().to_vec();
            let manifests = ScanPlan::prefix(
                CERTIFIED_ENTITY_BATCH_MANIFEST_SPACE,
                StoragePrefix {
                    bytes: Bytes::from(previous_prefix.clone()),
                },
            )
            .collect(read, StorageScanOptions::default())
            .await?;
            for entry in manifests.value.entries {
                let suffix = entry
                    .key
                    .0
                    .get(previous_prefix.len()..)
                    .ok_or_else(|| head_value_error("truncated certified manifest key"))?;
                if complete_manifest_suffixes
                    .get(branch_id)
                    .is_some_and(|prefixes| {
                        prefixes.iter().any(|prefix| suffix.starts_with(prefix))
                    })
                {
                    continue;
                }
                let mut key = control.generation.as_uuid().as_bytes().to_vec();
                key.extend_from_slice(suffix);
                let value = full_value_bytes(entry.value)?;
                match inherited_manifests.entry(key) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(value);
                    }
                    std::collections::btree_map::Entry::Occupied(entry)
                        if entry.get() == &value => {}
                    std::collections::btree_map::Entry::Occupied(_) => {
                        return Err(head_value_error(
                            "certified manifests disagree for the same inherited key",
                        ));
                    }
                }
            }
        }
    }
    for (key, value) in inherited_manifests {
        writes.put(
            CERTIFIED_ENTITY_BATCH_MANIFEST_SPACE,
            StorageKey(Bytes::from(key)),
            StorageValue { bytes: value },
        );
    }

    for file in file_writes {
        if file.batches.is_empty() {
            continue;
        }
        let control = controls.get(file.branch_id).ok_or_else(|| {
            head_value_error("certified entity batch has no published branch control")
        })?;
        let created_at = commit_created_at
            .get(&control.head_commit_id)
            .copied()
            .ok_or_else(|| head_value_error("certified entity batch has no commit timestamp"))?;
        for batch in file.batches {
            if batch.complete_file_state {
                let mut manifest_prefix = control.generation.as_uuid().as_bytes().to_vec();
                append_batch_text(&mut manifest_prefix, file.file_id)?;
                manifest_prefix.extend_from_slice(&batch.format.to_le_bytes());
                let prior_manifests = ScanPlan::prefix(
                    CERTIFIED_ENTITY_BATCH_MANIFEST_SPACE,
                    StoragePrefix {
                        bytes: Bytes::from(manifest_prefix),
                    },
                )
                .collect(read, StorageScanOptions::default())
                .await?;
                for entry in prior_manifests.value.entries {
                    writes.delete(CERTIFIED_ENTITY_BATCH_MANIFEST_SPACE, entry.key);
                }
            }
            let mut content_key = control.head_commit_id.as_uuid().as_bytes().to_vec();
            append_batch_text(&mut content_key, file.file_id)?;
            content_key.extend_from_slice(&batch.format.to_le_bytes());

            let schema_bytes = batch
                .schema_keys
                .iter()
                .try_fold(2usize, |total, schema| total.checked_add(2 + schema.len()))
                .ok_or_else(|| head_value_error("certified schema list size overflowed"))?;
            let mut value = Vec::with_capacity(
                4 + schema_bytes
                    + 2
                    + file.file_id.len()
                    + 16
                    + 2
                    + created_at.to_string().len()
                    + 2
                    + 8
                    + 8
                    + 4
                    + 4
                    + batch.pages.len().saturating_mul(12),
            );
            value.extend_from_slice(CERTIFIED_ENTITY_BATCH_MAGIC_V2);
            value.extend_from_slice(
                &u16::try_from(batch.schema_keys.len())
                    .map_err(|_| head_value_error("certified batch has too many schemas"))?
                    .to_le_bytes(),
            );
            for schema_key in &batch.schema_keys {
                append_batch_text(&mut value, schema_key)?;
            }
            append_batch_text(&mut value, file.file_id)?;
            value.extend_from_slice(control.head_commit_id.as_uuid().as_bytes());
            append_batch_text(&mut value, &created_at.to_string())?;
            value.extend_from_slice(&batch.format.to_le_bytes());
            value.extend_from_slice(&batch.row_count.to_le_bytes());
            value.extend_from_slice(&batch.creates.high.to_le_bytes());
            value.extend_from_slice(&batch.creates.low.to_le_bytes());
            value.extend_from_slice(
                &u32::try_from(batch.pages.len())
                    .map_err(|_| head_value_error("certified entity batch has too many pages"))?
                    .to_le_bytes(),
            );
            for (page_index, page) in batch.pages.iter().enumerate() {
                let (first_local_ref, last_local_ref) = match batch.format {
                    1 => certified_csv_page_local_ref_range(page)?,
                    2 | crate::wasm::HOST_CERTIFIED_PACKET_FORMAT => {
                        certified_packet_page_local_ref_range(page)?.unwrap_or((0, u32::MAX))
                    }
                    crate::wasm::HOST_CERTIFIED_ZSTD_PACKET_FORMAT => {
                        certified_zstd_packet_page_header(page)?.0
                    }
                    _ => (0, u32::MAX),
                };
                value.extend_from_slice(&first_local_ref.to_le_bytes());
                value.extend_from_slice(&last_local_ref.to_le_bytes());
                value.extend_from_slice(
                    &u32::try_from(page.len())
                        .map_err(|_| head_value_error("certified entity batch page exceeds 4GiB"))?
                        .to_le_bytes(),
                );
                writes.put(
                    CERTIFIED_ENTITY_BATCH_PAGE_SPACE,
                    certified_entity_batch_page_key(
                        &content_key,
                        u32::try_from(page_index).map_err(|_| {
                            head_value_error("certified entity batch has too many pages")
                        })?,
                    ),
                    StorageValue {
                        bytes: page.clone(),
                    },
                );
            }
            writes.put(
                CERTIFIED_ENTITY_BATCH_SPACE,
                StorageKey(Bytes::from(content_key.clone())),
                StorageValue {
                    bytes: Bytes::from(value),
                },
            );
            let mut manifest_key = control.generation.as_uuid().as_bytes().to_vec();
            append_batch_text(&mut manifest_key, file.file_id)?;
            manifest_key.extend_from_slice(&batch.format.to_le_bytes());
            manifest_key.extend_from_slice(control.head_commit_id.as_uuid().as_bytes());
            writes.put(
                CERTIFIED_ENTITY_BATCH_MANIFEST_SPACE,
                StorageKey(Bytes::from(manifest_key)),
                StorageValue {
                    bytes: Bytes::from(content_key),
                },
            );
        }
    }
    Ok(())
}

fn append_batch_text(output: &mut Vec<u8>, value: &str) -> Result<(), LixError> {
    output.extend_from_slice(
        &u16::try_from(value.len())
            .map_err(|_| head_value_error("certified entity batch text exceeds 64KiB"))?
            .to_le_bytes(),
    );
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn certified_entity_batch_page_key(content_key: &[u8], page_index: u32) -> StorageKey {
    let mut key = Vec::with_capacity(content_key.len() + 4);
    key.extend_from_slice(content_key);
    key.extend_from_slice(&page_index.to_be_bytes());
    StorageKey(Bytes::from(key))
}

fn certified_csv_page_local_ref_range(page: &[u8]) -> Result<(u32, u32), LixError> {
    let mut rows = CertifiedCsvReader {
        bytes: page,
        offset: 0,
    };
    let mut first = None;
    let mut last = None;
    while rows.offset < rows.bytes.len() {
        let local_ref = rows.u32()?;
        if last.is_some_and(|previous| previous >= local_ref) {
            return Err(head_value_error(
                "certified CSV page local refs are not strictly increasing",
            ));
        }
        first.get_or_insert(local_ref);
        last = Some(local_ref);
        let _order_rank = rows.u64()?;
        let _ending = rows.u8()?;
        let quote_layout_len = rows.u32()? as usize;
        let _quote_layout = rows.bytes(quote_layout_len)?;
        let field_count = rows.u16()?;
        for _ in 0..field_count {
            let cell_len = rows.u32()? as usize;
            let _cell = rows.bytes(cell_len)?;
        }
    }
    first
        .zip(last)
        .ok_or_else(|| head_value_error("certified CSV page is empty"))
}

/// Returns an ordinal range only when every packet row is a keyless create
/// and those local references are strictly increasing. A keyed or mixed page
/// remains conservatively unindexed.
fn certified_packet_page_local_ref_range(page: &[u8]) -> Result<Option<(u32, u32)>, LixError> {
    let mut rows = CertifiedCsvReader {
        bytes: page,
        offset: 0,
    };
    let mut first = None;
    let mut last = None;
    while rows.offset < rows.bytes.len() {
        let record_len = rows.u32()? as usize;
        let record_bytes = rows.bytes(record_len)?;
        let mut record = CertifiedCsvReader {
            bytes: record_bytes,
            offset: 0,
        };
        if record.u8()? != 2 {
            return Ok(None);
        }
        let schema_len = record.u32()? as usize;
        let _schema = record.bytes(schema_len)?;
        let local_ref = u32::try_from(record.u64()?)
            .map_err(|_| head_value_error("certified packet local reference exceeds u32"))?;
        if last.is_some_and(|previous| previous >= local_ref) {
            return Ok(None);
        }
        first.get_or_insert(local_ref);
        last = Some(local_ref);
    }
    Ok(first.zip(last))
}

fn certified_zstd_packet_page_header(page: &[u8]) -> Result<((u32, u32), usize, &[u8]), LixError> {
    let (header, compressed) = page
        .split_at_checked(12)
        .ok_or_else(|| head_value_error("compressed certified packet page is truncated"))?;
    let first_local_ref = u32::from_le_bytes(
        header[..4]
            .try_into()
            .expect("compressed packet first local ref"),
    );
    let last_local_ref = u32::from_le_bytes(
        header[4..8]
            .try_into()
            .expect("compressed packet last local ref"),
    );
    if first_local_ref > last_local_ref {
        return Err(head_value_error(
            "compressed certified packet page has an inverted local-ref range",
        ));
    }
    let uncompressed_len = u32::from_le_bytes(
        header[8..12]
            .try_into()
            .expect("compressed packet uncompressed length"),
    ) as usize;
    if uncompressed_len == 0 || uncompressed_len > 64 * 1024 * 1024 {
        return Err(head_value_error(
            "compressed certified packet page has an invalid uncompressed length",
        ));
    }
    Ok((
        (first_local_ref, last_local_ref),
        uncompressed_len,
        compressed,
    ))
}

fn decode_certified_zstd_packet_page(page: &[u8]) -> Result<Vec<u8>, LixError> {
    let (_, uncompressed_len, compressed) = certified_zstd_packet_page_header(page)?;
    let decoded =
        crate::compression::decompress_zstd(compressed, uncompressed_len).map_err(|error| {
            head_value_error(format!(
                "compressed certified packet page failed to decode: {error}"
            ))
        })?;
    if decoded.len() != uncompressed_len {
        return Err(head_value_error(format!(
            "compressed certified packet page decoded to {} bytes, expected {uncompressed_len}",
            decoded.len(),
        )));
    }
    Ok(decoded)
}

fn certified_external_page_plan(
    bytes: &[u8],
    content_key: &[u8],
    request: &TrackedStateScanRequest,
) -> Result<Option<Vec<(u32, StorageKey)>>, LixError> {
    let mut input = CertifiedBatchReader::new(bytes)?;
    if !input.external_pages {
        return Ok(None);
    }
    let schema_count = input.u16()? as usize;
    for _ in 0..schema_count {
        let _schema = input.text()?;
    }
    let _file_id = input.text()?;
    let _commit_id = input.bytes(16)?;
    let _timestamp = input.text()?;
    let format = input.u16()?;
    let _declared_rows = input.u64()?;
    let creates = WasmCreateContext {
        high: input.u64()?,
        low: input.u32()?,
    };
    let selected_local_refs = ((format == 1
        || format == 2
        || format == crate::wasm::HOST_CERTIFIED_PACKET_FORMAT
        || format == crate::wasm::HOST_CERTIFIED_ZSTD_PACKET_FORMAT)
        && !request.filter.entity_pks.is_empty())
    .then(|| {
        let high = creates.high.to_be_bytes();
        let low = creates.low.to_be_bytes();
        request
            .filter
            .entity_pks
            .iter()
            .map(|entity_pk| match entity_pk.components.as_slice() {
                [crate::entity_pk::EntityPkComponent::Uuid(bytes)]
                    if bytes[..8] == high && bytes[8..12] == low =>
                {
                    Ok(u32::from_be_bytes(
                        bytes[12..]
                            .try_into()
                            .expect("UUID local-reference suffix is four bytes"),
                    ))
                }
                _ => Err(()),
            })
            .collect::<Result<BTreeSet<_>, _>>()
            .ok()
    })
    .flatten();
    let page_count = input.u32()?;
    let mut pages = Vec::with_capacity(page_count as usize);
    for page_index in 0..page_count {
        let first_local_ref = input.u32()?;
        let last_local_ref = input.u32()?;
        let _page_len = input.u32()?;
        let selected = selected_local_refs.as_ref().is_none_or(|local_refs| {
            local_refs
                .range(first_local_ref..=last_local_ref)
                .next()
                .is_some()
        });
        if selected {
            pages.push((
                page_index,
                certified_entity_batch_page_key(content_key, page_index),
            ));
        }
    }
    if input.offset != input.bytes.len() {
        return Err(head_value_error(
            "certified entity batch header has trailing bytes",
        ));
    }
    Ok(Some(pages))
}

async fn scan_certified_entity_batch_rows(
    store: &impl StorageAdapterRead,
    branch_id: &str,
    generation: CommitId,
    request: &TrackedStateScanRequest,
    limit: Option<usize>,
) -> Result<MaterializedLiveStateBatch, LixError> {
    if matches!(limit, Some(0)) {
        return Ok(MaterializedLiveStateBatch::default());
    }
    let exact_file_ids = (!request.filter.file_ids.is_empty()
        && !request
            .filter
            .file_ids
            .iter()
            .any(|file_id| matches!(file_id, NullableKeyFilter::Any)))
    .then(|| {
        request
            .filter
            .file_ids
            .iter()
            .filter_map(|file_id| match file_id {
                NullableKeyFilter::Value(file_id) => Some(file_id.as_str()),
                NullableKeyFilter::Any | NullableKeyFilter::Null => None,
            })
            .collect::<BTreeSet<_>>()
    });
    let mut manifest_entries = Vec::new();
    if let Some(file_ids) = exact_file_ids {
        for file_id in file_ids {
            let mut prefix = generation.as_uuid().as_bytes().to_vec();
            append_batch_text(&mut prefix, file_id)?;
            let manifests = ScanPlan::prefix(
                CERTIFIED_ENTITY_BATCH_MANIFEST_SPACE,
                StoragePrefix {
                    bytes: Bytes::from(prefix),
                },
            )
            .collect(store, StorageScanOptions::default())
            .await?;
            manifest_entries.extend(manifests.value.entries);
        }
    } else {
        let manifests = ScanPlan::prefix(
            CERTIFIED_ENTITY_BATCH_MANIFEST_SPACE,
            StoragePrefix {
                bytes: Bytes::copy_from_slice(generation.as_uuid().as_bytes()),
            },
        )
        .collect(store, StorageScanOptions::default())
        .await?;
        manifest_entries = manifests.value.entries;
    }
    let content_keys = manifest_entries
        .into_iter()
        .map(|entry| full_value_bytes(entry.value).map(StorageKey))
        .collect::<Result<Vec<_>, _>>()?;
    let contents = PointReadPlan::new(CERTIFIED_ENTITY_BATCH_SPACE, &content_keys)
        .materialize(store, StorageGetOptions::default())
        .await?
        .value;
    let content_count = contents.iter().flatten().count();
    let decode_limit = (content_count <= 1).then_some(limit).flatten();
    let needs_snapshot = request.read_columns.columns.is_empty()
        || request
            .read_columns
            .columns
            .iter()
            .any(|column| column == "snapshot_content");
    let mut decode_inputs = Vec::with_capacity(content_keys.len());
    let mut page_routes = Vec::new();
    let mut page_keys = Vec::new();
    for (content_key, value) in content_keys.into_iter().zip(contents) {
        let Some(value) = value else {
            continue;
        };
        let value = full_value_bytes(value)?;
        let external_plan = certified_external_page_plan(&value, content_key.0.as_ref(), request)?;
        let input_index = decode_inputs.len();
        let external_pages = external_plan
            .as_ref()
            .map(|plan| Vec::with_capacity(plan.len()));
        if let Some(plan) = external_plan {
            for (page_index, key) in plan {
                page_routes.push((input_index, page_index));
                page_keys.push(key);
            }
        }
        decode_inputs.push((value, external_pages));
    }
    if !page_keys.is_empty() {
        let page_values = PointReadPlan::new(CERTIFIED_ENTITY_BATCH_PAGE_SPACE, &page_keys)
            .materialize(store, StorageGetOptions::default())
            .await?
            .value;
        for ((input_index, page_index), value) in page_routes.into_iter().zip(page_values) {
            let value =
                value.ok_or_else(|| head_value_error("certified entity batch page is missing"))?;
            decode_inputs[input_index]
                .1
                .as_mut()
                .expect("external page route belongs to an external batch")
                .push((page_index, full_value_bytes(value)?));
        }
    }
    let mut builder = MaterializedLiveStateBatchBuilder::with_capacity(
        limit.unwrap_or_else(|| decode_inputs.len().saturating_mul(1024)),
    );
    for (value, external_pages) in decode_inputs {
        decode_certified_entity_batch_rows(
            &value,
            external_pages.as_deref(),
            branch_id,
            request,
            needs_snapshot,
            decode_limit,
            &mut builder,
        )?;
        if decode_limit.is_some_and(|limit| builder.len() >= limit) {
            break;
        }
    }
    let batch = builder.finish();
    if content_count <= 1 {
        return Ok(batch);
    }
    let mut winners = BTreeMap::new();
    for row in batch.into_rows() {
        let key = (
            row.schema_key.clone(),
            row.entity_pk.clone(),
            row.file_id.clone(),
        );
        match winners.entry(key) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(row);
            }
            std::collections::btree_map::Entry::Occupied(mut entry)
                if entry.get().updated_at <= row.updated_at =>
            {
                entry.insert(row);
            }
            std::collections::btree_map::Entry::Occupied(_) => {}
        }
    }
    let mut rows = MaterializedLiveStateBatch::from_rows(winners.into_values().collect());
    if let Some(limit) = limit {
        rows = rows.filter(|_| true, Some(limit));
    }
    Ok(rows)
}

pub(crate) async fn scan_certified_history_rows(
    store: &impl StorageAdapterRead,
    commit_ids: &BTreeSet<CommitId>,
    request: &TrackedStateScanRequest,
) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
    if commit_ids.is_empty() {
        return Ok(Vec::new());
    }
    let needs_snapshot = request
        .read_columns
        .columns
        .iter()
        .any(|column| column == "snapshot_content");
    let mut builder = MaterializedLiveStateBatchBuilder::with_capacity(commit_ids.len() * 1024);
    for commit_id in commit_ids {
        let page = ScanPlan::prefix(
            CERTIFIED_ENTITY_BATCH_SPACE,
            StoragePrefix {
                bytes: Bytes::copy_from_slice(commit_id.as_uuid().as_bytes()),
            },
        )
        .collect(store, StorageScanOptions::default())
        .await?;
        for entry in page.value.entries {
            let value = full_value_bytes(entry.value)?;
            if certified_batch_commit_id(&value)? != *commit_id {
                continue;
            }
            let external_plan =
                certified_external_page_plan(&value, entry.key.0.as_ref(), request)?;
            let external_pages = if let Some(plan) = &external_plan {
                let keys = plan.iter().map(|(_, key)| key.clone()).collect::<Vec<_>>();
                let values = PointReadPlan::new(CERTIFIED_ENTITY_BATCH_PAGE_SPACE, &keys)
                    .materialize(store, StorageGetOptions::default())
                    .await?
                    .value;
                Some(
                    plan.iter()
                        .zip(values)
                        .map(|((page_index, _), value)| {
                            let value = value.ok_or_else(|| {
                                head_value_error("certified history batch page is missing")
                            })?;
                            Ok((*page_index, full_value_bytes(value)?))
                        })
                        .collect::<Result<Vec<_>, LixError>>()?,
                )
            } else {
                None
            };
            decode_certified_entity_batch_rows(
                &value,
                external_pages.as_deref(),
                "",
                request,
                needs_snapshot,
                None,
                &mut builder,
            )?;
        }
    }
    Ok(builder.finish().into_rows())
}

fn certified_batch_commit_id(bytes: &[u8]) -> Result<CommitId, LixError> {
    let mut input = CertifiedBatchReader::new(bytes)?;
    let schema_count = input.u16()? as usize;
    for _ in 0..schema_count {
        let _ = input.text()?;
    }
    let _ = input.text()?;
    Ok(CommitId::new(
        uuid::Uuid::from_slice(input.bytes(16)?)
            .map_err(|error| head_value_error(format!("invalid certified commit id: {error}")))?,
    ))
}

fn decode_certified_entity_batch_rows(
    bytes: &[u8],
    external_pages: Option<&[(u32, Bytes)]>,
    branch_id: &str,
    request: &TrackedStateScanRequest,
    needs_snapshot: bool,
    limit: Option<usize>,
    builder: &mut MaterializedLiveStateBatchBuilder,
) -> Result<(), LixError> {
    let mut input = CertifiedBatchReader::new(bytes)?;
    let schema_count = input.u16()? as usize;
    if schema_count == 0 {
        return Err(head_value_error("certified entity batch has no schemas"));
    }
    let mut schema_keys = Vec::with_capacity(schema_count);
    for _ in 0..schema_count {
        schema_keys.push(input.text()?);
    }
    let file_id = input.text()?;
    let commit_id = CommitId::new(
        uuid::Uuid::from_slice(input.bytes(16)?)
            .map_err(|error| head_value_error(format!("invalid certified commit id: {error}")))?,
    );
    let timestamp = LixTimestamp::parse(input.text()?).map_err(head_value_error)?;
    let format = input.u16()?;
    if format != 1
        && format != 2
        && format != crate::wasm::HOST_CERTIFIED_PACKET_FORMAT
        && format != crate::wasm::HOST_CERTIFIED_ZSTD_PACKET_FORMAT
    {
        return Err(head_value_error(format!(
            "unsupported certified entity batch format {format}"
        )));
    }
    let declared_rows = input.u64()?;
    let creates = WasmCreateContext {
        high: input.u64()?,
        low: input.u32()?,
    };
    let page_count = input.u32()?;
    if !request.filter.schema_keys.is_empty()
        && !request
            .filter
            .schema_keys
            .iter()
            .any(|candidate| schema_keys.contains(&candidate.as_str()))
    {
        return Ok(());
    }
    if !request.filter.file_ids.is_empty()
        && !request
            .filter
            .file_ids
            .iter()
            .any(|candidate| match candidate {
                NullableKeyFilter::Any => true,
                NullableKeyFilter::Null => false,
                NullableKeyFilter::Value(candidate) => candidate == file_id,
            })
    {
        return Ok(());
    }

    let complete_pages = !input.external_pages
        || external_pages.is_some_and(|pages| pages.len() == page_count as usize);
    let mut decoded_rows = 0_u64;
    for page_index in 0..page_count {
        let page = if input.external_pages {
            let _first_local_ref = input.u32()?;
            let _last_local_ref = input.u32()?;
            let page_len = input.u32()? as usize;
            let Some((_, page)) = external_pages.and_then(|pages| {
                pages
                    .binary_search_by_key(&page_index, |(page_index, _)| *page_index)
                    .ok()
                    .map(|index| &pages[index])
            }) else {
                continue;
            };
            if page.len() != page_len {
                return Err(head_value_error(
                    "certified entity batch page length does not match its header",
                ));
            }
            page.as_ref()
        } else {
            let page_len = input.u32()? as usize;
            input.bytes(page_len)?
        };
        let decoded_page;
        let page = if format == crate::wasm::HOST_CERTIFIED_ZSTD_PACKET_FORMAT {
            decoded_page = decode_certified_zstd_packet_page(page)?;
            decoded_page.as_slice()
        } else {
            page
        };
        if format == 2
            || format == crate::wasm::HOST_CERTIFIED_PACKET_FORMAT
            || format == crate::wasm::HOST_CERTIFIED_ZSTD_PACKET_FORMAT
        {
            decoded_rows = decoded_rows.saturating_add(decode_certified_packet_rows(
                page,
                &creates,
                commit_id,
                timestamp,
                branch_id,
                file_id,
                request,
                needs_snapshot,
                limit,
                decoded_rows,
                builder,
            )?);
            if limit.is_some_and(|limit| builder.len() >= limit) {
                return Ok(());
            }
            continue;
        }
        let mut rows = CertifiedCsvReader {
            bytes: page,
            offset: 0,
        };
        while rows.offset < rows.bytes.len() {
            let local_ref = rows.u32()?;
            let order_rank = rows.u64()?;
            let ending = rows.u8()?;
            let quote_layout_len = rows.u32()? as usize;
            let quote_layout = rows.bytes(quote_layout_len)?;
            let field_count = rows.u16()?;
            let id = creates
                .component_uuid_bytes(u64::from(local_ref))
                .map_err(|error| head_value_error(error.to_string()))?;
            let entity_pk = EntityPk::uuid_from_bytes(id);
            let selected = request.filter.entity_pks.is_empty()
                || request
                    .filter
                    .entity_pks
                    .iter()
                    .any(|candidate| candidate == &entity_pk);
            let mut cells = needs_snapshot.then(|| Vec::with_capacity(field_count as usize));
            for _ in 0..field_count {
                let cell_len = rows.u32()? as usize;
                let cell = std::str::from_utf8(rows.bytes(cell_len)?).map_err(|error| {
                    head_value_error(format!("certified CSV cell is not UTF-8: {error}"))
                })?;
                if let Some(cells) = &mut cells {
                    cells.push(cell);
                }
            }
            decoded_rows = decoded_rows.saturating_add(1);
            if !selected {
                continue;
            }
            let snapshot = cells
                .map(|cells| {
                    let mut object = serde_json::Map::new();
                    object.insert(
                        "cells".to_owned(),
                        serde_json::Value::Array(
                            cells
                                .into_iter()
                                .map(|cell| serde_json::Value::String(cell.to_owned()))
                                .collect(),
                        ),
                    );
                    object.insert(
                        "id".to_owned(),
                        serde_json::Value::String(uuid::Uuid::from_bytes(id).to_string()),
                    );
                    if !quote_layout.is_empty() || ending != 0 {
                        let mut layout = serde_json::Map::new();
                        if !quote_layout.is_empty() {
                            layout.insert(
                                "force_quote".to_owned(),
                                serde_json::Value::String(
                                    base64::engine::general_purpose::URL_SAFE_NO_PAD
                                        .encode(quote_layout),
                                ),
                            );
                        }
                        if ending != 0 {
                            let terminator = match ending {
                                1 => "",
                                2 => "\n",
                                3 => "\r\n",
                                4 => "\r",
                                _ => {
                                    return Err(head_value_error(
                                        "certified CSV row has invalid terminator",
                                    ));
                                }
                            };
                            layout.insert(
                                "terminator".to_owned(),
                                serde_json::Value::String(terminator.to_owned()),
                            );
                        }
                        object.insert("layout".to_owned(), serde_json::Value::Object(layout));
                    }
                    object.insert(
                        "order_key".to_owned(),
                        serde_json::Value::String(format!("{order_rank:016x}")),
                    );
                    let json = serde_json::to_vec(&serde_json::Value::Object(object))
                        .map_err(|error| head_value_error(error.to_string()))?;
                    SharedStr::from_utf8(Bytes::from(json))
                        .map_err(|error| head_value_error(error.to_string()))
                })
                .transpose()?;
            builder.push_materialized(
                entity_pk,
                schema_keys[0].to_owned(),
                Some(file_id.to_owned()),
                snapshot,
                None,
                false,
                timestamp,
                timestamp,
                false,
                Some(ChangeId::new(uuid::Uuid::from_bytes(id))),
                Some(commit_id),
                false,
                branch_id,
            );
            if limit.is_some_and(|limit| builder.len() >= limit) {
                return Ok(());
            }
        }
    }
    if complete_pages && decoded_rows != declared_rows {
        return Err(head_value_error(format!(
            "certified entity batch declared {declared_rows} rows but decoded {decoded_rows}"
        )));
    }
    if input.offset != input.bytes.len() {
        return Err(head_value_error(
            "certified entity batch has trailing storage bytes",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn decode_certified_packet_rows(
    page: &[u8],
    creates: &WasmCreateContext,
    commit_id: CommitId,
    timestamp: LixTimestamp,
    branch_id: &str,
    file_id: &str,
    request: &TrackedStateScanRequest,
    needs_snapshot: bool,
    limit: Option<usize>,
    base_ordinal: u64,
    builder: &mut MaterializedLiveStateBatchBuilder,
) -> Result<u64, LixError> {
    let mut input = CertifiedCsvReader {
        bytes: page,
        offset: 0,
    };
    let mut decoded = 0_u64;
    while input.offset < input.bytes.len() {
        let record_len = input.u32()? as usize;
        let record_bytes = input.bytes(record_len)?;
        let mut record = CertifiedCsvReader {
            bytes: record_bytes,
            offset: 0,
        };
        let tag = record.u8()?;
        let schema_len = record.u32()? as usize;
        let schema_key = std::str::from_utf8(record.bytes(schema_len)?)
            .map_err(|error| head_value_error(format!("invalid packet schema: {error}")))?;
        let (entity_pk, created_id) = match tag {
            0 => {
                let component_count = record.u32()? as usize;
                let mut components = Vec::with_capacity(component_count);
                for _ in 0..component_count {
                    let component_len = record.u32()? as usize;
                    let component =
                        std::str::from_utf8(record.bytes(component_len)?).map_err(|error| {
                            head_value_error(format!("invalid packet key: {error}"))
                        })?;
                    components.push(
                        SharedStr::from_utf8(Bytes::copy_from_slice(component.as_bytes()))
                            .map_err(|error| head_value_error(error.to_string()))?,
                    );
                }
                if record.u8()? > 1 {
                    return Err(head_value_error(
                        "certified packet upsert has invalid effect",
                    ));
                }
                (
                    EntityPk::from_shared_parts(components)
                        .map_err(|error| head_value_error(error.to_string()))?,
                    None,
                )
            }
            2 => {
                let local_ref = record.u64()?;
                let id = creates
                    .component_uuid_bytes(local_ref)
                    .map_err(|error| head_value_error(error.to_string()))?;
                (EntityPk::uuid_from_bytes(id), Some(id))
            }
            _ => {
                return Err(head_value_error(
                    "certified packet contains a non-snapshot record",
                ));
            }
        };
        if record.u8()? != 0 {
            return Err(head_value_error(
                "certified create packet snapshot is not inline",
            ));
        }
        let snapshot_len = record.u32()? as usize;
        let snapshot_bytes = record.bytes(snapshot_len)?;
        if record.offset != record.bytes.len() {
            return Err(head_value_error(
                "certified create packet record has trailing bytes",
            ));
        }
        decoded = decoded.saturating_add(1);
        let selected = (request.filter.schema_keys.is_empty()
            || request
                .filter
                .schema_keys
                .iter()
                .any(|candidate| candidate == schema_key))
            && (request.filter.entity_pks.is_empty()
                || request
                    .filter
                    .entity_pks
                    .iter()
                    .any(|candidate| candidate == &entity_pk));
        if !selected {
            continue;
        }
        let snapshot = if needs_snapshot {
            if let Some(id) = &created_id {
                let json = insert_created_id_into_canonical_object(
                    snapshot_bytes,
                    &uuid::Uuid::from_bytes(*id).to_string(),
                )?;
                Some(
                    SharedStr::from_utf8(Bytes::from(json))
                        .map_err(|error| head_value_error(error.to_string()))?,
                )
            } else {
                Some(
                    SharedStr::from_utf8(Bytes::copy_from_slice(snapshot_bytes))
                        .map_err(|error| head_value_error(error.to_string()))?,
                )
            }
        } else {
            None
        };
        let change_id = if let Some(id) = &created_id {
            ChangeId::new(uuid::Uuid::from_bytes(*id))
        } else {
            let mut bytes = *commit_id.as_uuid().as_bytes();
            let ordinal = base_ordinal.saturating_add(decoded).to_be_bytes();
            for (target, source) in bytes[8..].iter_mut().zip(ordinal) {
                *target ^= source;
            }
            ChangeId::new(uuid::Uuid::from_bytes(bytes))
        };
        builder.push_materialized(
            entity_pk,
            schema_key.to_owned(),
            Some(file_id.to_owned()),
            snapshot,
            None,
            false,
            timestamp,
            timestamp,
            false,
            Some(change_id),
            Some(commit_id),
            false,
            branch_id,
        );
        if limit.is_some_and(|limit| builder.len() >= limit) {
            break;
        }
    }
    Ok(decoded)
}

/// Inserts the generated `id` into an already validated canonical JSON object
/// without materializing its value tree. Keys emitted by serde_json are
/// lexicographically ordered, so locating the top-level insertion boundary is
/// sufficient to reproduce the exact canonical snapshot.
fn insert_created_id_into_canonical_object(snapshot: &[u8], id: &str) -> Result<Vec<u8>, LixError> {
    if snapshot.first() != Some(&b'{') || snapshot.last() != Some(&b'}') {
        return Err(head_value_error(
            "certified create snapshot is not a JSON object",
        ));
    }
    let field = format!("\"id\":\"{id}\"");
    if snapshot == b"{}" {
        let mut output = Vec::with_capacity(field.len() + 2);
        output.push(b'{');
        output.extend_from_slice(field.as_bytes());
        output.push(b'}');
        return Ok(output);
    }

    let mut entry_start = 1;
    loop {
        if snapshot.get(entry_start) != Some(&b'"') {
            return Err(head_value_error(
                "certified canonical object has an invalid key",
            ));
        }
        let key_end = json_string_end(snapshot, entry_start)?;
        let encoded_key = &snapshot[entry_start..key_end];
        let key = if encoded_key[1..encoded_key.len() - 1].contains(&b'\\') {
            serde_json::from_slice::<String>(encoded_key)
                .map_err(|error| head_value_error(format!("invalid canonical key: {error}")))?
        } else {
            std::str::from_utf8(&encoded_key[1..encoded_key.len() - 1])
                .map_err(|error| head_value_error(format!("invalid canonical key: {error}")))?
                .to_owned()
        };
        if key.as_str() >= "id" {
            if key == "id" {
                return Err(head_value_error(
                    "certified keyless create snapshot already contains id",
                ));
            }
            let mut output = Vec::with_capacity(snapshot.len() + field.len() + 1);
            output.extend_from_slice(&snapshot[..entry_start]);
            output.extend_from_slice(field.as_bytes());
            output.push(b',');
            output.extend_from_slice(&snapshot[entry_start..]);
            return Ok(output);
        }

        let colon = snapshot
            .get(key_end..)
            .and_then(|tail| tail.iter().position(|byte| *byte == b':'))
            .map(|offset| key_end + offset)
            .ok_or_else(|| head_value_error("certified canonical object key has no value"))?;
        match json_top_level_value_end(snapshot, colon + 1)? {
            JsonObjectBoundary::Next(next) => entry_start = next,
            JsonObjectBoundary::End(end) => {
                let mut output = Vec::with_capacity(snapshot.len() + field.len() + 1);
                output.extend_from_slice(&snapshot[..end]);
                output.push(b',');
                output.extend_from_slice(field.as_bytes());
                output.extend_from_slice(&snapshot[end..]);
                return Ok(output);
            }
        }
    }
}

fn json_string_end(bytes: &[u8], start: usize) -> Result<usize, LixError> {
    let mut escaped = false;
    for (offset, byte) in bytes[start + 1..].iter().enumerate() {
        if escaped {
            escaped = false;
        } else if *byte == b'\\' {
            escaped = true;
        } else if *byte == b'"' {
            return Ok(start + offset + 2);
        }
    }
    Err(head_value_error("unterminated canonical JSON string"))
}

enum JsonObjectBoundary {
    Next(usize),
    End(usize),
}

fn json_top_level_value_end(bytes: &[u8], start: usize) -> Result<JsonObjectBoundary, LixError> {
    let mut depth = 0_u32;
    let mut in_string = false;
    let mut escaped = false;
    for (offset, byte) in bytes[start..].iter().enumerate() {
        let index = start + offset;
        if in_string {
            if escaped {
                escaped = false;
            } else if *byte == b'\\' {
                escaped = true;
            } else if *byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match *byte {
            b'"' => in_string = true,
            b'[' | b'{' => {
                depth = depth
                    .checked_add(1)
                    .ok_or_else(|| head_value_error("canonical JSON nesting overflowed"))?;
            }
            b']' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| head_value_error("invalid canonical JSON array"))?;
            }
            b'}' if depth > 0 => depth -= 1,
            b',' if depth == 0 => return Ok(JsonObjectBoundary::Next(index + 1)),
            b'}' if depth == 0 => return Ok(JsonObjectBoundary::End(index)),
            _ => {}
        }
    }
    Err(head_value_error(
        "canonical JSON object has no closing boundary",
    ))
}

struct CertifiedBatchReader<'a> {
    bytes: &'a [u8],
    offset: usize,
    external_pages: bool,
}

impl<'a> CertifiedBatchReader<'a> {
    fn new(bytes: &'a [u8]) -> Result<Self, LixError> {
        let external_pages = if bytes.starts_with(CERTIFIED_ENTITY_BATCH_MAGIC_V1) {
            false
        } else if bytes.starts_with(CERTIFIED_ENTITY_BATCH_MAGIC_V2) {
            true
        } else {
            return Err(head_value_error("invalid certified entity batch magic"));
        };
        Ok(Self {
            bytes,
            offset: 4,
            external_pages,
        })
    }

    fn bytes(&mut self, length: usize) -> Result<&'a [u8], LixError> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| head_value_error("truncated certified entity batch"))?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn text(&mut self) -> Result<&'a str, LixError> {
        let length = self.u16()? as usize;
        std::str::from_utf8(self.bytes(length)?)
            .map_err(|error| head_value_error(format!("invalid certified batch text: {error}")))
    }

    fn u16(&mut self) -> Result<u16, LixError> {
        Ok(u16::from_le_bytes(
            self.bytes(2)?.try_into().expect("fixed batch u16 width"),
        ))
    }

    fn u32(&mut self) -> Result<u32, LixError> {
        Ok(u32::from_le_bytes(
            self.bytes(4)?.try_into().expect("fixed batch u32 width"),
        ))
    }

    fn u64(&mut self) -> Result<u64, LixError> {
        Ok(u64::from_le_bytes(
            self.bytes(8)?.try_into().expect("fixed batch u64 width"),
        ))
    }
}

struct CertifiedCsvReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> CertifiedCsvReader<'a> {
    fn bytes(&mut self, length: usize) -> Result<&'a [u8], LixError> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| head_value_error("truncated certified CSV page"))?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, LixError> {
        Ok(self.bytes(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, LixError> {
        Ok(u16::from_le_bytes(
            self.bytes(2)?.try_into().expect("fixed CSV u16 width"),
        ))
    }

    fn u32(&mut self) -> Result<u32, LixError> {
        Ok(u32::from_le_bytes(
            self.bytes(4)?.try_into().expect("fixed CSV u32 width"),
        ))
    }

    fn u64(&mut self) -> Result<u64, LixError> {
        Ok(u64::from_le_bytes(
            self.bytes(8)?.try_into().expect("fixed CSV u64 width"),
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
struct HotCollectionControl {
    active_generation: CommitId,
    live_count: u64,
}

fn hot_collection_control_key(
    branch_id: &str,
    branch_generation: CommitId,
    scope: crate::collection_generation::CollectionScopeRef<'_>,
) -> Vec<u8> {
    let mut key = hot_scope_prefix(branch_id, branch_generation);
    write_key_string(&mut key, scope.schema_key, KEY_PART_FINAL);
    write_file_id(&mut key, scope.file_id);
    key
}

async fn load_hot_collection_control(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    branch_generation: CommitId,
    scope: crate::collection_generation::CollectionScopeRef<'_>,
) -> Result<HotCollectionControl, LixError> {
    let key = StorageKey(Bytes::from(hot_collection_control_key(
        branch_id,
        branch_generation,
        scope,
    )));
    let value = PointReadPlan::new(HOT_COLLECTION_CONTROL_SPACE, &[key])
        .materialize(store, StorageGetOptions::default())
        .await?
        .value
        .into_iter()
        .next()
        .flatten();
    value.map_or(
        Ok(HotCollectionControl {
            active_generation: branch_generation,
            live_count: 0,
        }),
        |value| {
            let StorageProjectedValue::FullValue(bytes) = value else {
                return Err(head_value_error(
                    "hot collection-control read unexpectedly omitted its value",
                ));
            };
            storage_codec::decode("hot collection control", &bytes)
        },
    )
}

async fn load_hot_collection_controls(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    branch_generation: CommitId,
    scopes: &[crate::collection_generation::CollectionScopeRef<'_>],
) -> Result<Vec<HotCollectionControl>, LixError> {
    if scopes.is_empty() {
        return Ok(Vec::new());
    }
    let keys = scopes
        .iter()
        .copied()
        .map(|scope| {
            StorageKey(Bytes::from(hot_collection_control_key(
                branch_id,
                branch_generation,
                scope,
            )))
        })
        .collect::<Vec<_>>();
    PointReadPlan::new(HOT_COLLECTION_CONTROL_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?
        .value
        .into_iter()
        .map(|value| {
            value.map_or(
                Ok(HotCollectionControl {
                    active_generation: branch_generation,
                    live_count: 0,
                }),
                |value| {
                    let StorageProjectedValue::FullValue(bytes) = value else {
                        return Err(head_value_error(
                            "hot collection-control batch read unexpectedly omitted its value",
                        ));
                    };
                    storage_codec::decode("hot collection control", &bytes)
                },
            )
        })
        .collect()
}

fn stage_hot_collection_control(
    writes: &mut StorageWriteSet,
    branch_id: &str,
    branch_generation: CommitId,
    scope: crate::collection_generation::CollectionScopeRef<'_>,
    control: HotCollectionControl,
) -> Result<(), LixError> {
    writes.put(
        HOT_COLLECTION_CONTROL_SPACE,
        StorageKey(Bytes::from(hot_collection_control_key(
            branch_id,
            branch_generation,
            scope,
        ))),
        StorageValue {
            bytes: Bytes::from(storage_codec::encode("hot collection control", &control)?),
        },
    );
    Ok(())
}

async fn load_incremental_collection_controls(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    branch_generation: CommitId,
    deltas: &[&CurrentStateDeltaRef<'_>],
) -> Result<BTreeMap<(String, Option<String>), HotCollectionControl>, LixError> {
    use crate::collection_generation::{
        COLLECTION_GENERATION_SCHEMA_KEY, CollectionScopeRef, collection_scope_from_entity_pk,
    };

    let mut owned_scopes = BTreeSet::<(String, Option<String>)>::new();
    for delta in deltas {
        if delta.schema_key == COLLECTION_GENERATION_SCHEMA_KEY {
            owned_scopes.insert(collection_scope_from_entity_pk(delta.entity_pk)?);
            continue;
        }
        owned_scopes.insert((delta.schema_key.to_string(), None));
        if let Some(file_id) = delta.file_id {
            owned_scopes.insert((delta.schema_key.to_string(), Some(file_id.to_string())));
        }
    }
    if owned_scopes.is_empty() {
        return Ok(BTreeMap::new());
    }
    let scopes = owned_scopes
        .iter()
        .map(|(schema_key, file_id)| CollectionScopeRef {
            schema_key,
            file_id: file_id.as_deref(),
        })
        .collect::<Vec<_>>();
    let controls =
        load_hot_collection_controls(store, branch_id, branch_generation, &scopes).await?;
    Ok(scopes
        .iter()
        .copied()
        .zip(controls)
        .map(|(scope, control)| {
            (
                (
                    scope.schema_key.to_string(),
                    scope.file_id.map(str::to_string),
                ),
                control,
            )
        })
        .collect())
}

fn stage_incremental_collection_controls(
    writes: &mut StorageWriteSet,
    branch_id: &str,
    branch_generation: CommitId,
    deltas: &[&CurrentStateDeltaRef<'_>],
    previous_values: &[Option<Bytes>],
    mut controls: BTreeMap<(String, Option<String>), HotCollectionControl>,
    certified_live_increments: &BTreeMap<(String, Option<String>), u64>,
) -> Result<(), LixError> {
    use crate::collection_generation::{
        COLLECTION_GENERATION_SCHEMA_KEY, CollectionScopeRef, collection_scope_from_entity_pk,
    };
    for (delta, previous) in deltas.iter().zip(previous_values) {
        if delta.schema_key == COLLECTION_GENERATION_SCHEMA_KEY {
            let scope = collection_scope_from_entity_pk(delta.entity_pk)?;
            let control = controls
                .get_mut(&scope)
                .expect("collection marker target was loaded above");
            if delta.deleted {
                return Err(head_value_error(
                    "collection-generation controls cannot be tombstoned",
                ));
            }
            control.active_generation = delta.commit_id.ok_or_else(|| {
                head_value_error("tracked collection-generation row lacks commit_id")
            })?;
            control.live_count = 0;
            continue;
        }

        let previous_live = previous
            .as_deref()
            .map(decode_head_value)
            .transpose()?
            .is_some_and(|value| !value.deleted);
        let schema_control = controls
            .get(&(delta.schema_key.to_string(), None))
            .expect("row schema collection scope was loaded above");
        let belongs_to_active_generation = schema_control.active_generation == branch_generation
            || delta
                .commit_id
                .is_some_and(|commit_id| commit_id > schema_control.active_generation);
        let next_live = !delta.deleted && belongs_to_active_generation;
        if previous_live == next_live {
            continue;
        }
        for scope in [
            Some((delta.schema_key.to_string(), None)),
            delta
                .file_id
                .map(|file_id| (delta.schema_key.to_string(), Some(file_id.to_string()))),
        ]
        .into_iter()
        .flatten()
        {
            let control = controls
                .get_mut(&scope)
                .expect("row collection scope was loaded above");
            control.live_count = if next_live {
                control
                    .live_count
                    .checked_add(1)
                    .ok_or_else(|| head_value_error("hot collection live count exceeds u64"))?
            } else {
                control
                    .live_count
                    .checked_sub(1)
                    .ok_or_else(|| head_value_error("hot collection live count underflow"))?
            };
        }
    }

    for (scope, increment) in certified_live_increments {
        let control = controls
            .get_mut(scope)
            .expect("certified collection scope was loaded above");
        control.live_count = control
            .live_count
            .checked_add(*increment)
            .ok_or_else(|| head_value_error("hot collection live count exceeds u64"))?;
    }

    for ((schema_key, file_id), control) in controls {
        stage_hot_collection_control(
            writes,
            branch_id,
            branch_generation,
            CollectionScopeRef {
                schema_key: &schema_key,
                file_id: file_id.as_deref(),
            },
            control,
        )?;
    }
    Ok(())
}

fn stage_complete_collection_controls(
    writes: &mut StorageWriteSet,
    branch_id: &str,
    branch_generation: CommitId,
    rows: &HotRowMap,
) -> Result<(), LixError> {
    use crate::collection_generation::{
        COLLECTION_GENERATION_SCHEMA_KEY, CollectionScopeRef, collection_scope_from_entity_pk,
    };

    let mut controls = BTreeMap::<(String, Option<String>), HotCollectionControl>::new();
    for (identity, bytes) in rows {
        if identity.schema_key == COLLECTION_GENERATION_SCHEMA_KEY {
            let target = collection_scope_from_entity_pk(&identity.entity_pk)?;
            let marker = decode_head_value(bytes)?;
            let active_generation = marker.commit_id.ok_or_else(|| {
                head_value_error("tracked collection-generation row lacks commit_id")
            })?;
            controls.insert(
                target,
                HotCollectionControl {
                    active_generation,
                    live_count: 0,
                },
            );
            continue;
        }
        controls
            .entry((identity.schema_key.clone(), None))
            .or_insert(HotCollectionControl {
                active_generation: branch_generation,
                live_count: 0,
            });
        if let Some(file_id) = &identity.file_id {
            controls
                .entry((identity.schema_key.clone(), Some(file_id.clone())))
                .or_insert(HotCollectionControl {
                    active_generation: branch_generation,
                    live_count: 0,
                });
        }
    }

    for (identity, bytes) in rows {
        if identity.schema_key == COLLECTION_GENERATION_SCHEMA_KEY {
            continue;
        }
        let value = decode_head_value(bytes)?;
        if value.deleted {
            continue;
        }
        let schema_scope = (identity.schema_key.clone(), None);
        let schema_control = controls
            .get(&schema_scope)
            .expect("complete row schema control was initialized above");
        let visible_after_schema_generation = schema_control.active_generation == branch_generation
            || value
                .commit_id
                .is_some_and(|commit_id| commit_id > schema_control.active_generation);
        let file_scope = identity
            .file_id
            .as_ref()
            .map(|file_id| (identity.schema_key.clone(), Some(file_id.clone())));
        let visible_after_file_generation = file_scope
            .as_ref()
            .and_then(|scope| controls.get(scope))
            .is_none_or(|control| {
                control.active_generation == branch_generation
                    || value
                        .commit_id
                        .is_some_and(|commit_id| commit_id > control.active_generation)
            });
        if !visible_after_schema_generation || !visible_after_file_generation {
            continue;
        }
        for scope in [Some(schema_scope), file_scope].into_iter().flatten() {
            let control = controls
                .get_mut(&scope)
                .expect("complete row collection control was initialized above");
            control.live_count = control
                .live_count
                .checked_add(1)
                .ok_or_else(|| head_value_error("hot collection live count exceeds u64"))?;
        }
    }

    for ((schema_key, file_id), control) in controls {
        stage_hot_collection_control(
            writes,
            branch_id,
            branch_generation,
            CollectionScopeRef {
                schema_key: &schema_key,
                file_id: file_id.as_deref(),
            },
            control,
        )?;
    }
    Ok(())
}

/// Direct reader for one published hot generation.
pub(crate) struct HotStateStoreReader<S> {
    pub(super) store: S,
}

impl<S> HotStateStoreReader<S>
where
    S: StorageAdapterRead,
{
    pub(crate) async fn collection_generation(
        &self,
        branch_id: &str,
        branch_generation: CommitId,
        scope: crate::collection_generation::CollectionScopeRef<'_>,
    ) -> Result<crate::collection_generation::CollectionGeneration, LixError> {
        load_hot_collection_control(&self.store, branch_id, branch_generation, scope)
            .await
            .map(
                |control| crate::collection_generation::CollectionGeneration {
                    active_generation: control.active_generation,
                    live_count: control.live_count,
                },
            )
    }

    pub(crate) async fn scan_live_batch(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        request: &TrackedStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        self.scan_live_batch_for_generation(branch_id, control.generation, request)
            .await
    }

    pub(crate) async fn scan_live_rows(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        request: &TrackedStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        self.scan_live_batch(branch_id, control, request)
            .await
            .map(MaterializedLiveStateBatch::into_rows)
    }

    pub(crate) async fn scan_live_batches_for_controls(
        &self,
        controls: &[(String, BranchHeadControl)],
        request: &TrackedStateScanRequest,
    ) -> Result<Vec<(String, MaterializedLiveStateBatch)>, LixError> {
        let mut rows = Vec::with_capacity(controls.len());
        for (branch_id, control) in controls {
            let branch_rows = self.scan_live_batch(branch_id, *control, request).await?;
            rows.push((branch_id.clone(), branch_rows));
        }
        Ok(rows)
    }

    pub(crate) async fn has_schema_rows(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        schema_key: &str,
    ) -> Result<bool, LixError> {
        let mut prefix = hot_scope_prefix(branch_id, control.generation);
        write_key_string(&mut prefix, schema_key, KEY_PART_FINAL);
        let page = ScanPlan::prefix(
            HOT_ROW_SPACE,
            StoragePrefix {
                bytes: Bytes::from(prefix),
            },
        )
        .collect(
            &self.store,
            StorageScanOptions {
                projection: StorageCoreProjection::KeyOnly,
                limit_rows: 1,
                ..StorageScanOptions::default()
            },
        )
        .await?;
        if !page.value.entries.is_empty() {
            return Ok(true);
        }
        // Format plugins cannot publish engine-owned schemas. Avoid probing
        // certified plugin segments when validation asks about a missing
        // system schema; durable-function setup relies on this point-read-only
        // path staying free of unrelated storage scans.
        if schema_key.starts_with("lix_") {
            return Ok(false);
        }
        let certified = scan_certified_entity_batch_rows(
            &self.store,
            branch_id,
            control.generation,
            &TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    schema_keys: vec![schema_key.to_owned()],
                    ..TrackedStateFilter::default()
                },
                read_columns: Default::default(),
                limit: Some(1),
            },
            Some(1),
        )
        .await?;
        Ok(!certified.is_empty())
    }

    pub(crate) async fn scan_entity_snapshots(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        schema_key: &str,
        entity_pks: &[EntityPk],
        limit: Option<usize>,
    ) -> Result<Vec<Option<Bytes>>, LixError> {
        self.scan_entity_snapshots_for_generation(
            branch_id,
            control.generation,
            schema_key,
            entity_pks,
            limit,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn scan_live_rows_if_current(
        &self,
        branch_id: &str,
        expected_head: &str,
        request: &TrackedStateScanRequest,
    ) -> Result<Option<Vec<MaterializedLiveStateRow>>, LixError> {
        let expected_head = CommitId::parse_lix(expected_head, "hot-state expected commit")?;
        let control = BranchHeadControlContext::new()
            .reader(&self.store)
            .load(branch_id)
            .await?;
        let Some(control) = control.filter(|control| control.head_commit_id == expected_head)
        else {
            return Ok(None);
        };
        Ok(Some(
            self.scan_live_batch_for_generation(branch_id, control.generation, request)
                .await?
                .into_rows(),
        ))
    }

    async fn scan_live_batch_for_generation(
        &self,
        branch_id: &str,
        generation: CommitId,
        request: &TrackedStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        let collection_control = match request.filter.schema_keys.as_slice() {
            [schema_key]
                if schema_key != crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY =>
            {
                Some(
                    load_hot_collection_control(
                        &self.store,
                        branch_id,
                        generation,
                        crate::collection_generation::CollectionScopeRef {
                            schema_key,
                            file_id: None,
                        },
                    )
                    .await?,
                )
            }
            _ => None,
        };
        let replaced_generation =
            collection_control.filter(|control| control.active_generation != generation);
        if replaced_generation.is_some_and(|control| control.live_count == 0) {
            return Ok(MaterializedLiveStateBatch::default());
        }
        // A storage prefix is ordered by identity, but tombstones are filtered
        // only after decoding the value. Applying SQL LIMIT to the raw scan
        // would therefore let one tombstone hide a later live row.
        let entries =
            hot_scan_entries(&self.store, branch_id, generation, &request.filter, None).await?;
        let projection = ChangeRecordProjection::from_columns(&request.read_columns.columns);
        let rows =
            materialize_hot_scan_entries(&self.store, entries, projection, branch_id).await?;
        let overlay_identities = rows
            .iter()
            .map(|row| {
                (
                    row.schema_key().to_owned(),
                    row.entity_pk().clone(),
                    row.file_id().map(str::to_owned),
                )
            })
            .collect::<BTreeSet<_>>();
        let certified_rows = scan_certified_entity_batch_rows(
            &self.store,
            branch_id,
            generation,
            request,
            if overlay_identities.is_empty() {
                request.limit.map(|limit| limit.saturating_sub(rows.len()))
            } else {
                None
            },
        )
        .await?
        .filter(
            |row| {
                !overlay_identities
                    .iter()
                    .any(|(schema_key, entity_pk, file_id)| {
                        schema_key == row.schema_key()
                            && entity_pk == row.entity_pk()
                            && file_id.as_deref() == row.file_id()
                    })
            },
            None,
        );
        let rows = if certified_rows.is_empty() {
            rows
        } else if rows.is_empty() {
            certified_rows
        } else {
            let mut combined = rows.into_rows();
            combined.extend(certified_rows.into_rows());
            MaterializedLiveStateBatch::from_rows(combined)
        };
        if request.filter.include_tombstones
            && request.limit.is_none()
            && replaced_generation.is_none()
        {
            return Ok(rows);
        }
        Ok(rows.filter(
            |row| {
                let in_active_generation = replaced_generation.is_none_or(|control| {
                    row.commit_id()
                        .is_some_and(|commit_id| commit_id > control.active_generation)
                });
                in_active_generation && (request.filter.include_tombstones || !row.deleted())
            },
            request.limit,
        ))
    }

    #[cfg(test)]
    pub(crate) async fn load_projected_live_rows_if_current(
        &self,
        branch_id: &str,
        expected_head: &str,
        keys: &[TrackedStateKey],
        projection: &ChangeRecordProjection,
    ) -> Result<Option<Vec<Option<MaterializedLiveStateRow>>>, LixError> {
        let expected_head = CommitId::parse_lix(expected_head, "hot-state expected commit")?;
        let control = BranchHeadControlContext::new()
            .reader(&self.store)
            .load(branch_id)
            .await?;
        let Some(control) = control.filter(|control| control.head_commit_id == expected_head)
        else {
            return Ok(None);
        };
        Ok(Some(
            self.load_projected_live_batch(branch_id, control, keys, projection)
                .await?
                .into_rows(),
        ))
    }

    pub(crate) async fn load_projected_live_rows(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        keys: &[TrackedStateKey],
        projection: &ChangeRecordProjection,
    ) -> Result<Vec<Option<MaterializedLiveStateRow>>, LixError> {
        self.load_projected_live_batch(branch_id, control, keys, projection)
            .await
            .map(MaterializedLiveStateExactBatch::into_rows)
    }

    pub(crate) async fn load_projected_live_batch(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        keys: &[TrackedStateKey],
        projection: &ChangeRecordProjection,
    ) -> Result<MaterializedLiveStateExactBatch, LixError> {
        let keys = keys
            .iter()
            .map(|key| TrackedStateKeyRef {
                schema_key: key.schema_key.as_str(),
                file_id: key.file_id.as_deref(),
                entity_pk: &key.entity_pk,
            })
            .collect::<Vec<_>>();
        self.load_projected_live_batch_refs(branch_id, control, &keys, projection)
            .await
    }

    pub(crate) async fn load_projected_live_batch_refs(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        keys: &[TrackedStateKeyRef<'_>],
        projection: &ChangeRecordProjection,
    ) -> Result<MaterializedLiveStateExactBatch, LixError> {
        self.load_projected_live_batch_for_generation_refs(
            branch_id,
            control.generation,
            keys,
            projection,
        )
        .await
    }

    async fn load_projected_live_batch_for_generation_refs(
        &self,
        branch_id: &str,
        generation: CommitId,
        keys: &[TrackedStateKeyRef<'_>],
        projection: &ChangeRecordProjection,
    ) -> Result<MaterializedLiveStateExactBatch, LixError> {
        if keys.is_empty() {
            return Ok(MaterializedLiveStateExactBatch::default());
        }
        let replaced_generation = keys
            .first()
            .filter(|first| keys.iter().all(|key| key.schema_key == first.schema_key))
            .filter(|first| {
                first.schema_key != crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
            })
            .map(|first| async {
                load_hot_collection_control(
                    &self.store,
                    branch_id,
                    generation,
                    crate::collection_generation::CollectionScopeRef {
                        schema_key: first.schema_key,
                        file_id: None,
                    },
                )
                .await
            });
        let replaced_generation = match replaced_generation {
            Some(control) => {
                let control = control.await?;
                (control.active_generation != generation).then_some(control)
            }
            None => None,
        };
        if replaced_generation.is_some_and(|control| control.live_count == 0) {
            return MaterializedLiveStateExactBatch::new(
                MaterializedLiveStateBatch::default(),
                vec![None; keys.len()],
            );
        }
        let mut values =
            hot_load_identity_ref_bytes(&self.store, branch_id, generation, keys).await?;
        if let Some(control) = replaced_generation {
            for value in &mut values {
                let visible = value
                    .as_deref()
                    .map(decode_head_value)
                    .transpose()?
                    .and_then(|value| value.commit_id)
                    .is_some_and(|commit_id| commit_id > control.active_generation);
                if !visible {
                    *value = None;
                }
            }
        }
        let mut slots = Vec::with_capacity(values.len());
        let mut entries = Vec::with_capacity(values.iter().flatten().count());
        for (identity, value) in keys.iter().copied().zip(values) {
            slots.push(value.map(|value| {
                let ordinal =
                    u32::try_from(entries.len()).expect("live-state exact batch exceeds u32 rows");
                entries.push((identity, value));
                ordinal
            }));
        }
        let rows = materialize_live_entries(&self.store, entries, *projection, branch_id).await?;
        if slots.iter().all(Option::is_some) {
            return MaterializedLiveStateExactBatch::new(rows, slots);
        }
        if replaced_generation.is_some()
            || keys.iter().all(|key| key.schema_key.starts_with("lix_"))
        {
            return MaterializedLiveStateExactBatch::new(rows, slots);
        }

        // Certified immutable segments deliberately do not manufacture one
        // HOT_ROW entry per semantic row. Exact validation reads still need
        // to observe those identities—for example, a sparse plugin successor
        // proving that a keyed update or foreign-key target already exists.
        // Decode the matching segment rows only when the ordinary point-read
        // index misses, then preserve the original request alignment.
        let certified_request = TrackedStateScanRequest {
            filter: TrackedStateFilter {
                schema_keys: keys.iter().map(|key| key.schema_key.to_owned()).collect(),
                entity_pks: keys.iter().map(|key| key.entity_pk.clone()).collect(),
                file_ids: keys
                    .iter()
                    .map(|key| {
                        key.file_id.map_or(NullableKeyFilter::Null, |file_id| {
                            NullableKeyFilter::Value(file_id.to_owned())
                        })
                    })
                    .collect(),
                include_tombstones: true,
            },
            read_columns: TrackedStateReadColumns {
                columns: [
                    projection.snapshot_content.then_some("snapshot_content"),
                    projection.metadata.then_some("metadata"),
                ]
                .into_iter()
                .flatten()
                .map(str::to_owned)
                .collect(),
            },
            limit: None,
        };
        let certified = scan_certified_entity_batch_rows(
            &self.store,
            branch_id,
            generation,
            &certified_request,
            None,
        )
        .await?;
        if certified.is_empty() {
            return MaterializedLiveStateExactBatch::new(rows, slots);
        }

        let mut builder = MaterializedLiveStateBatchBuilder::with_capacity(keys.len());
        let mut combined_slots = Vec::with_capacity(keys.len());
        for (key, slot) in keys.iter().zip(slots) {
            let row = slot.and_then(|slot| rows.get(slot as usize)).or_else(|| {
                certified.iter().find(|row| {
                    row.schema_key() == key.schema_key
                        && row.entity_pk() == key.entity_pk
                        && row.file_id() == key.file_id
                })
            });
            combined_slots.push(
                row.map(|row| {
                    u32::try_from(builder.push_ref(row, None)).map_err(|_| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "certified exact live-state result exceeds u32 rows",
                        )
                    })
                })
                .transpose()?,
            );
        }
        MaterializedLiveStateExactBatch::new(builder.finish(), combined_slots)
    }

    pub(crate) async fn working_diff_epoch(
        &self,
        branch_id: &str,
    ) -> Result<Option<TrackedWorkingDiffEpoch>, LixError> {
        load_tracked_working_diff_epoch(&self.store, branch_id).await
    }

    pub(crate) async fn untracked_json_refs(
        &self,
        controls: &[(String, BranchHeadControl)],
    ) -> Result<Vec<JsonRef>, LixError> {
        let mut refs = BTreeSet::new();
        for (branch_id, control) in controls {
            let scope = hot_scope_prefix(branch_id, control.generation);
            let plan = ScanPlan::prefix(
                HOT_ROW_SPACE,
                StoragePrefix {
                    bytes: Bytes::from(scope),
                },
            );
            let mut resume_after = None;
            loop {
                let page = plan
                    .collect(
                        &self.store,
                        StorageScanOptions {
                            resume_after: resume_after.clone(),
                            ..StorageScanOptions::default()
                        },
                    )
                    .await?;
                resume_after = page.value.entries.last().map(|entry| entry.key.clone());
                for entry in page.value.entries {
                    let bytes = full_value_bytes(entry.value)?;
                    let value = decode_head_value(&bytes)?;
                    collect_hot_untracked_refs(value, &mut refs);
                }
                if !page.value.has_more || resume_after.is_none() {
                    break;
                }
            }
        }
        Ok(refs.into_iter().map(JsonRef::from_hash_bytes).collect())
    }

    pub(crate) async fn working_diff_for_control(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        request: &TrackedStateDiffRequest,
    ) -> Result<Option<TrackedWorkingDiff>, LixError> {
        let Ok(Some(epoch)) = self.working_diff_epoch(branch_id).await else {
            return Ok(None);
        };
        let generation = epoch.generation;
        if generation != control.generation
            || control.working_diff_checkpoint_commit_id != Some(epoch.checkpoint_commit_id)
        {
            return Ok(None);
        }
        let Some(entries) = hot_working_diff_entries(
            &self.store,
            branch_id,
            epoch.checkpoint_commit_id,
            generation,
            epoch.coverage,
            &request.filter,
        )
        .await?
        else {
            return Ok(None);
        };
        Ok(Some(TrackedWorkingDiff {
            checkpoint_commit_id: epoch.checkpoint_commit_id,
            diff: TrackedStateDiff::from_entries(entries),
        }))
    }

    async fn scan_entity_snapshots_for_generation(
        &self,
        branch_id: &str,
        generation: CommitId,
        schema_key: &str,
        entity_pks: &[EntityPk],
        limit: Option<usize>,
    ) -> Result<Vec<Option<Bytes>>, LixError> {
        if matches!(limit, Some(0)) {
            return Ok(Vec::new());
        }
        let collection_control = load_hot_collection_control(
            &self.store,
            branch_id,
            generation,
            crate::collection_generation::CollectionScopeRef {
                schema_key,
                file_id: None,
            },
        )
        .await?;
        let replaced_generation =
            (collection_control.active_generation != generation).then_some(collection_control);
        if replaced_generation.is_some_and(|control| control.live_count == 0) {
            return Ok(Vec::new());
        }
        let use_finite_batch = !entity_pks.is_empty()
            && !hot_schema_has_file_member(&self.store, branch_id, generation, schema_key).await?;
        let fallback_filter = (!use_finite_batch).then(|| TrackedStateFilter {
            schema_keys: vec![schema_key.to_string()],
            entity_pks: entity_pks.to_vec(),
            include_tombstones: false,
            ..TrackedStateFilter::default()
        });
        let entries = if use_finite_batch {
            let identities = FiniteHotIdentityBatchRef::new(
                branch_id,
                generation,
                schema_key,
                entity_pks.iter().collect(),
                vec![None],
            )
            .expect("a borrowed entity slice has a representable finite identity count");
            HotScanEntries::Finite(
                hot_scan_finite_identity_batches(&self.store, vec![identities], None).await?,
            )
        } else {
            hot_scan_entries(
                &self.store,
                branch_id,
                generation,
                fallback_filter
                    .as_ref()
                    .expect("non-finite snapshots retain their fallback filter"),
                None,
            )
            .await?
        };
        let mut snapshots = Vec::new();
        let mut json_refs = Vec::new();
        let mut deferred = Vec::new();
        for bytes in hot_scan_values_in_logical_order(entries) {
            let value = decode_head_value(&bytes)?;
            if value.deleted
                || replaced_generation.is_some_and(|control| {
                    value
                        .commit_id
                        .is_none_or(|commit_id| commit_id <= control.active_generation)
                })
            {
                continue;
            }
            let row_index = snapshots.len();
            match value.snapshot {
                HeadSlotView::None => snapshots.push(None),
                HeadSlotView::Inline(snapshot) => {
                    snapshots.push(Some(bytes.slice_ref(snapshot.as_bytes())));
                }
                HeadSlotView::Ref(json_ref) => {
                    snapshots.push(None);
                    json_refs.push(json_ref);
                    deferred.push((row_index, json_ref));
                }
            }
            if limit.is_some_and(|limit| snapshots.len() >= limit) {
                break;
            }
        }
        materialize_entity_snapshot_refs(&self.store, snapshots, json_refs, deferred).await
    }
}

type HotRowMap = BTreeMap<HeadRowIdentity, Bytes>;

/// An owned tracked snapshot used only while a lifecycle publication is being
/// staged. Normal commits mutate the published hot generation in place; a
/// checkpoint, merge, or branch move instead builds one complete replacement
/// generation before the branch control makes it visible.
///
/// The snapshot deliberately stores the already encoded row values. That
/// keeps large JSON slots as refs/inline values and makes a root staged in
/// this write set usable as the parent of another root without reading the
/// uncommitted write set back through storage.
#[derive(Clone, Default)]
pub(crate) struct HotTrackedSnapshot {
    rows: HotRowMap,
}

impl HotTrackedSnapshot {
    pub(crate) fn from_materialized_rows(
        tracked_rows: Vec<MaterializedTrackedStateRow>,
    ) -> Result<Self, LixError> {
        let mut rows = BTreeMap::new();
        for row in tracked_rows {
            let identity = HeadRowIdentity {
                schema_key: row.schema_key,
                entity_pk: row.entity_pk,
                file_id: row.file_id,
            };
            let value = HeadValueRef {
                change_id: Some(row.change_id),
                commit_id: Some(row.commit_id),
                untracked: false,
                deleted: row.deleted,
                created_at: LixTimestamp::expect_parse(
                    "hot tracked snapshot created_at",
                    &row.created_at,
                ),
                updated_at: LixTimestamp::expect_parse(
                    "hot tracked snapshot updated_at",
                    &row.updated_at,
                ),
                snapshot: row
                    .snapshot_content
                    .as_deref()
                    .map_or(JsonSlotRef::None, JsonSlotRef::Inline),
                metadata: row
                    .metadata
                    .as_deref()
                    .map_or(JsonSlotRef::None, JsonSlotRef::Inline),
                working_diff_baseline: WorkingDiffBaseline::Disabled,
            };
            if rows
                .insert(identity, Bytes::from(encode_head_value(&value)?))
                .is_some()
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked hot snapshot contains duplicate row identity",
                ));
            }
        }
        Ok(Self { rows })
    }
}

/// Writer for row-addressable current state.
pub(crate) struct HotStateWriter<'a, S: ?Sized> {
    pub(super) store: &'a S,
    pub(super) writes: &'a mut StorageWriteSet,
}

impl<S> HotStateWriter<'_, S>
where
    S: StorageAdapterRead + ?Sized,
{
    #[cfg(any(test, feature = "storage-benches"))]
    pub(crate) async fn stage_commit(
        &mut self,
        branch_id: &str,
        parent_generation: Option<CommitId>,
        new_head: CommitId,
        deltas: &[TrackedHeadDeltaRef<'_>],
        absence_guards: &BTreeSet<TrackedStateKey>,
        parent_rows: Option<Vec<MaterializedTrackedStateRow>>,
    ) -> Result<CommitId, LixError> {
        let deltas = deltas
            .iter()
            .map(TrackedHeadDeltaRef::as_current)
            .collect::<Vec<_>>();
        let mut coverage = WorkingDiffIndexCoverage::default();
        let generation = self
            .stage_current_state_with_working_diff(
                branch_id,
                parent_generation,
                new_head,
                &deltas,
                absence_guards,
                parent_rows,
                None,
                None,
                &mut coverage,
            )
            .await?;
        #[cfg(test)]
        stage_test_current_control(self.writes, branch_id, new_head, generation, None)?;
        Ok(generation)
    }

    #[cfg(test)]
    pub(crate) async fn stage_commit_with_working_diff(
        &mut self,
        branch_id: &str,
        parent_generation: Option<CommitId>,
        new_head: CommitId,
        deltas: &[TrackedHeadDeltaRef<'_>],
        absence_guards: &BTreeSet<TrackedStateKey>,
        parent_rows: Option<Vec<MaterializedTrackedStateRow>>,
        working_diff_capture_checkpoint_commit_id: Option<CommitId>,
        coverage: &mut WorkingDiffIndexCoverage,
    ) -> Result<CommitId, LixError> {
        let deltas = deltas
            .iter()
            .map(TrackedHeadDeltaRef::as_current)
            .collect::<Vec<_>>();
        self.stage_current_state_with_working_diff(
            branch_id,
            parent_generation,
            new_head,
            &deltas,
            absence_guards,
            parent_rows,
            None,
            working_diff_capture_checkpoint_commit_id,
            coverage,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn stage_current_state_with_working_diff(
        &mut self,
        branch_id: &str,
        parent_generation: Option<CommitId>,
        new_head: CommitId,
        deltas: &[CurrentStateDeltaRef<'_>],
        absence_guards: &BTreeSet<TrackedStateKey>,
        parent_rows: Option<Vec<MaterializedTrackedStateRow>>,
        preserved_untracked_rows: Option<Vec<MaterializedLiveStateRow>>,
        working_diff_capture_checkpoint_commit_id: Option<CommitId>,
        coverage: &mut WorkingDiffIndexCoverage,
    ) -> Result<CommitId, LixError> {
        self.stage_current_state_with_working_diff_inner(
            branch_id,
            parent_generation,
            new_head,
            deltas,
            absence_guards,
            parent_rows,
            preserved_untracked_rows,
            working_diff_capture_checkpoint_commit_id,
            coverage,
            false,
            None,
            None,
            false,
            &BTreeMap::new(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn stage_current_state_with_certified_counts(
        &mut self,
        branch_id: &str,
        parent_generation: Option<CommitId>,
        new_head: CommitId,
        deltas: &[CurrentStateDeltaRef<'_>],
        absence_guards: &BTreeSet<TrackedStateKey>,
        working_diff_capture_checkpoint_commit_id: Option<CommitId>,
        coverage: &mut WorkingDiffIndexCoverage,
        certified_live_increments: &BTreeMap<(String, Option<String>), u64>,
    ) -> Result<CommitId, LixError> {
        self.stage_current_state_with_working_diff_inner(
            branch_id,
            parent_generation,
            new_head,
            deltas,
            absence_guards,
            None,
            None,
            working_diff_capture_checkpoint_commit_id,
            coverage,
            false,
            None,
            None,
            false,
            certified_live_increments,
        )
        .await
    }

    /// Publishes a checkpoint into the already-visible generation.
    ///
    /// The checkpoint selected refs are the complete dirty set for the
    /// interval. Rewriting only those rows to `Clean` starts the next epoch
    /// without copying every unchanged HOT row into a fresh generation.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn stage_checkpoint_current_state(
        &mut self,
        branch_id: &str,
        generation: CommitId,
        new_head: CommitId,
        deltas: &[CurrentStateDeltaRef<'_>],
        absence_guards: &BTreeSet<TrackedStateKey>,
        checkpoint_commit_id: CommitId,
        coverage: &mut WorkingDiffIndexCoverage,
    ) -> Result<CommitId, LixError> {
        self.stage_current_state_with_working_diff_inner(
            branch_id,
            Some(generation),
            new_head,
            deltas,
            absence_guards,
            None,
            None,
            Some(checkpoint_commit_id),
            coverage,
            false,
            None,
            None,
            true,
            &BTreeMap::new(),
        )
        .await
    }

    /// Stages deltas whose absence was already validated against the coherent
    /// transaction snapshot. The caller must publish the corresponding branch
    /// control with a compare-and-swap precondition from that same snapshot.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn stage_validated_insert_current_state_with_working_diff(
        &mut self,
        branch_id: &str,
        parent_generation: Option<CommitId>,
        new_head: CommitId,
        deltas: &[CurrentStateDeltaRef<'_>],
        absence_guards: &[TrackedStateKeyRef<'_>],
        parent_rows: Option<Vec<MaterializedTrackedStateRow>>,
        preserved_untracked_rows: Option<Vec<MaterializedLiveStateRow>>,
        working_diff_capture_checkpoint_commit_id: Option<CommitId>,
        coverage: &mut WorkingDiffIndexCoverage,
        validated_absent_file_id: Option<&str>,
    ) -> Result<CommitId, LixError> {
        if parent_generation.is_none() {
            let owned_guards = absence_guards
                .iter()
                .map(|guard| TrackedStateKey {
                    schema_key: guard.schema_key.to_string(),
                    file_id: guard.file_id.map(str::to_string),
                    entity_pk: guard.entity_pk.clone(),
                })
                .collect::<BTreeSet<_>>();
            return self
                .stage_current_state_with_working_diff_inner(
                    branch_id,
                    parent_generation,
                    new_head,
                    deltas,
                    &owned_guards,
                    parent_rows,
                    preserved_untracked_rows,
                    working_diff_capture_checkpoint_commit_id,
                    coverage,
                    true,
                    validated_absent_file_id,
                    None,
                    false,
                    &BTreeMap::new(),
                )
                .await;
        }
        let no_owned_guards = BTreeSet::new();
        self.stage_current_state_with_working_diff_inner(
            branch_id,
            parent_generation,
            new_head,
            deltas,
            &no_owned_guards,
            parent_rows,
            preserved_untracked_rows,
            working_diff_capture_checkpoint_commit_id,
            coverage,
            true,
            validated_absent_file_id,
            Some(absence_guards),
            false,
            &BTreeMap::new(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn stage_current_state_with_working_diff_inner(
        &mut self,
        branch_id: &str,
        parent_generation: Option<CommitId>,
        new_head: CommitId,
        deltas: &[CurrentStateDeltaRef<'_>],
        absence_guards: &BTreeSet<TrackedStateKey>,
        parent_rows: Option<Vec<MaterializedTrackedStateRow>>,
        preserved_untracked_rows: Option<Vec<MaterializedLiveStateRow>>,
        working_diff_capture_checkpoint_commit_id: Option<CommitId>,
        coverage: &mut WorkingDiffIndexCoverage,
        absence_guards_validated: bool,
        validated_absent_file_id: Option<&str>,
        borrowed_absence_guards: Option<&[TrackedStateKeyRef<'_>]>,
        reset_working_diff_baselines: bool,
        certified_live_increments: &BTreeMap<(String, Option<String>), u64>,
    ) -> Result<CommitId, LixError> {
        let generation = parent_generation.unwrap_or(new_head);
        let sorted = {
            let _span = tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.materialization.hot.sort"
            )
            .entered();
            let mut sorted = deltas.iter().collect::<Vec<_>>();
            for delta in &sorted {
                delta.validate()?;
            }
            let mut already_strictly_sorted = true;
            for pair in sorted.windows(2) {
                match compare_hot_deltas(pair[0], pair[1]) {
                    Ordering::Less => {}
                    Ordering::Equal => {
                        return Err(current_state_duplicate_delta_error(pair[1]));
                    }
                    Ordering::Greater => {
                        already_strictly_sorted = false;
                        break;
                    }
                }
            }
            if !already_strictly_sorted {
                sorted.sort_unstable_by(|left, right| compare_hot_deltas(left, right));
                for pair in sorted.windows(2) {
                    if compare_hot_deltas(pair[0], pair[1]).is_eq() {
                        return Err(current_state_duplicate_delta_error(pair[1]));
                    }
                }
            }
            sorted
        };

        if parent_generation.is_none() {
            stage_hot_bootstrap(
                self.writes,
                branch_id,
                generation,
                parent_rows.unwrap_or_default(),
                preserved_untracked_rows.unwrap_or_default(),
                &sorted,
                absence_guards,
                working_diff_capture_checkpoint_commit_id,
                coverage,
            )?;
            return Ok(generation);
        }

        let identities = {
            let _span = tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.materialization.hot.identities"
            )
            .entered();
            encode_hot_mutation_identities(branch_id, generation, &sorted)
        };
        // Mutation validation must use primary rows rather than the file-id
        // projection. The projection is an equally-valued read accelerator,
        // not an ownership record.
        let loaded_previous_values = hot_load_primary_mutation_identity_refs(
            self.store,
            &identities,
            &sorted,
            absence_guards_validated,
            validated_absent_file_id,
        )
        .instrument(tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.materialization.hot.previous"
        ))
        .await?;
        let mut loaded_previous_values = loaded_previous_values.into_iter();
        let mut previous_values = sorted
            .iter()
            .map(|delta| {
                if hot_delta_is_guarded_by_absent_file(
                    delta,
                    absence_guards_validated,
                    validated_absent_file_id,
                ) {
                    None
                } else {
                    loaded_previous_values
                        .next()
                        .expect("every unguarded hot delta has one loaded previous value")
                }
            })
            .collect::<Vec<_>>();
        debug_assert_eq!(loaded_previous_values.len(), 0);
        let mut collection_controls =
            load_incremental_collection_controls(self.store, branch_id, generation, &sorted)
                .await?;
        let missing_certified_scopes = certified_live_increments
            .keys()
            .filter(|scope| !collection_controls.contains_key(*scope))
            .map(
                |(schema_key, file_id)| crate::collection_generation::CollectionScopeRef {
                    schema_key,
                    file_id: file_id.as_deref(),
                },
            )
            .collect::<Vec<_>>();
        let missing_certified_controls = load_hot_collection_controls(
            self.store,
            branch_id,
            generation,
            &missing_certified_scopes,
        )
        .await?;
        collection_controls.extend(
            missing_certified_scopes
                .into_iter()
                .zip(missing_certified_controls)
                .map(|(scope, control)| {
                    (
                        (
                            scope.schema_key.to_owned(),
                            scope.file_id.map(str::to_owned),
                        ),
                        control,
                    )
                }),
        );
        for (delta, previous) in sorted.iter().zip(&mut previous_values) {
            if delta.schema_key == crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY {
                continue;
            }
            let Some(control) = collection_controls.get(&(delta.schema_key.to_string(), None))
            else {
                continue;
            };
            if control.active_generation == generation {
                continue;
            }
            let belongs_to_retired_generation = previous
                .as_deref()
                .map(decode_head_value)
                .transpose()?
                .and_then(|value| value.commit_id)
                .is_none_or(|commit_id| commit_id <= control.active_generation);
            if belongs_to_retired_generation {
                *previous = None;
            }
        }
        let mut created_ats = Vec::with_capacity(sorted.len());
        let mut retired_untracked_json_refs = BTreeSet::new();
        for (delta, previous) in sorted.iter().zip(&previous_values) {
            let Some(previous) = previous else {
                created_ats.push(delta.created_at);
                continue;
            };
            let existing = decode_head_value(previous)?;
            if let Some(borrowed_absence_guards) = borrowed_absence_guards {
                reject_borrowed_guarded_live_member(borrowed_absence_guards, delta, existing)?;
            } else {
                reject_guarded_live_member(absence_guards, delta, existing)?;
            }
            reject_retention_change(delta, existing)?;
            if existing.untracked {
                collect_retired_untracked_json_refs(
                    existing,
                    delta,
                    &mut retired_untracked_json_refs,
                );
            }
            created_ats.push(if reset_working_diff_baselines && !delta.untracked {
                // Checkpoint selection canonicalizes newly added rows to the
                // changelog timestamp and preserves the original timestamp
                // for modified/removed rows.
                delta.created_at
            } else {
                existing.created_at
            });
        }
        let unmatched_guards = if absence_guards_validated || absence_guards.is_empty() {
            BTreeSet::new()
        } else {
            let delta_keys = sorted
                .iter()
                .map(|delta| TrackedStateKey {
                    schema_key: delta.schema_key.to_string(),
                    entity_pk: delta.entity_pk.clone(),
                    file_id: delta.file_id.map(str::to_string),
                })
                .collect::<BTreeSet<_>>();
            absence_guards
                .iter()
                .filter(|key| !delta_keys.contains(*key))
                .cloned()
                .collect::<BTreeSet<_>>()
        };
        reject_hot_absence_guards(self.store, branch_id, generation, &unmatched_guards).await?;

        // Build every fallible output before mutating the write set. The
        // required primary-row batch above now supplies both mutation
        // validation and the checkpoint first-before decision.  `HOT_DIFF`
        // is an empty dirty-key index, so there is deliberately no second
        // point-read batch against it here.
        let mut next_coverage = *coverage;
        let diff_scope = working_diff_capture_checkpoint_commit_id.map(|checkpoint_commit_id| {
            encode_working_diff_scope_prefix(branch_id, checkpoint_commit_id, generation)
        });
        let diff_key_capacity = diff_scope.as_deref().map_or(0, |scope| {
            sorted
                .iter()
                .try_fold(0_usize, |total, delta| {
                    total.checked_add(encoded_hot_identity_key_len(
                        scope.len(),
                        delta.schema_key,
                        delta.entity_pk,
                        delta.file_id,
                    )?)
                })
                .unwrap_or(0)
        });
        let mut diff_key_bytes = Vec::with_capacity(diff_key_capacity);
        let mut diff_puts = Vec::with_capacity(diff_scope.as_ref().map_or(0, |_| sorted.len()));
        let next_value_capacity = sorted
            .iter()
            .try_fold(0_usize, |total, delta| {
                checked_add_hot_next_value_capacity(
                    total,
                    delta,
                    working_diff_capture_checkpoint_commit_id.is_some(),
                )
            })
            // Preserve the encoder's fallible behavior for impossible input:
            // overflow must not turn into an attempted `usize::MAX`
            // allocation. The row encoder will report its normal length
            // error while this arena falls back to ordinary growth.
            .unwrap_or(0);
        let mut next_value_ranges = Vec::with_capacity(sorted.len());
        let mut next_value_bytes = Vec::with_capacity(next_value_capacity);
        {
            let _span = tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.materialization.hot.values"
            )
            .entered();
            for (delta, (created_at, previous)) in
                sorted.iter().zip(created_ats.iter().zip(&previous_values))
            {
                // Ordinary commits have no active checkpoint, so their baseline
                // is always disabled. Do not decode the row a second time merely
                // to rediscover that fact; the first decode above already handled
                // retention validation and `created_at` preservation.
                let (working_diff_baseline, newly_dirty) = if reset_working_diff_baselines
                    && !delta.untracked
                {
                    (WorkingDiffBaseline::Clean, false)
                } else if working_diff_capture_checkpoint_commit_id.is_some() && !delta.untracked {
                    let previous = previous.as_deref().map(decode_head_value).transpose()?;
                    next_hot_working_diff_baseline(
                        working_diff_capture_checkpoint_commit_id,
                        delta,
                        previous,
                    )?
                } else {
                    (WorkingDiffBaseline::Disabled, false)
                };
                if newly_dirty {
                    let key = append_hot_diff_key_parts(
                        &mut diff_key_bytes,
                        diff_scope
                            .as_deref()
                            .expect("a newly dirty hot row requires an active checkpoint"),
                        delta.schema_key,
                        delta.entity_pk,
                        delta.file_id,
                    );
                    next_coverage
                        .add_encoded_group_key(&diff_key_bytes[key.clone()])
                        .ok_or_else(|| {
                            head_value_error("hot working-diff index count exceeds u64")
                        })?;
                    diff_puts.push(EncodedPut {
                        key: buffer_range(&key),
                        value: BufferRange::default(),
                    });
                }
                next_value_ranges.push(if delta.physically_deletes() {
                    None
                } else {
                    Some(append_head_value(
                        &mut next_value_bytes,
                        &delta.value_ref(*created_at, working_diff_baseline),
                    )?)
                });
            }
        }
        let next_value_bytes = Bytes::from(next_value_bytes);
        stage_incremental_collection_controls(
            self.writes,
            branch_id,
            generation,
            &sorted,
            &previous_values,
            collection_controls,
            certified_live_increments,
        )?;

        let _stage_span = tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.materialization.hot.stage"
        )
        .entered();
        stage_hot_diff_batch(
            self.writes,
            diff_scope.as_deref().unwrap_or_default(),
            diff_key_bytes,
            diff_puts,
        )?;
        stage_hot_mutation_batch(self.writes, identities, next_value_bytes, next_value_ranges);
        stage_incremental_file_delete_cascades(
            self.store,
            self.writes,
            branch_id,
            generation,
            &sorted,
            working_diff_capture_checkpoint_commit_id,
            reset_working_diff_baselines,
            &mut next_coverage,
            &mut retired_untracked_json_refs,
        )
        .await?;
        JsonStoreWriter::stage_untracked_reclaim_candidates(
            self.writes,
            retired_untracked_json_refs
                .into_iter()
                .map(JsonRef::from_hash_bytes),
        );
        *coverage = next_coverage;
        Ok(generation)
    }

    /// Publishes a complete replacement generation for a lifecycle event.
    ///
    /// The supplied snapshot is the target commit's tracked portion.  Any
    /// branch-local untracked rows are copied from the previous generation,
    /// then this transaction's untracked mutations are applied before its
    /// tracked mutations.  That order admits the one legitimate mixed case:
    /// deleting an untracked row and selecting a tracked row with the same
    /// identity in the same atomic publication.  Every other retention
    /// collision fails before the new control can become visible.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn stage_complete_current_state_with_working_diff(
        &mut self,
        branch_id: &str,
        generation: CommitId,
        parent_tracked: HotTrackedSnapshot,
        preserved_untracked_generation: Option<CommitId>,
        tracked_deltas: &[CurrentStateDeltaRef<'_>],
        untracked_deltas: &[CurrentStateDeltaRef<'_>],
        absence_guards: &BTreeSet<TrackedStateKey>,
        working_diff_capture_checkpoint_commit_id: Option<CommitId>,
        coverage: &mut WorkingDiffIndexCoverage,
    ) -> Result<(HotTrackedSnapshot, BTreeSet<String>), LixError> {
        let mut rows = parent_tracked.rows;
        let mut untracked_rows = match preserved_untracked_generation {
            Some(previous_generation) => {
                load_hot_untracked_generation(self.store, branch_id, previous_generation).await?
            }
            None => BTreeMap::new(),
        };

        let sorted_untracked = sorted_lifecycle_hot_deltas(untracked_deltas, true)?;
        let sorted_tracked = sorted_lifecycle_hot_deltas(tracked_deltas, false)?;
        reject_lifecycle_retention_collisions(&sorted_untracked, &sorted_tracked)?;

        let mut retired_untracked_json_refs = BTreeSet::new();
        for delta in &sorted_untracked {
            apply_complete_hot_snapshot_delta(
                &mut untracked_rows,
                delta,
                absence_guards,
                &mut retired_untracked_json_refs,
            )?;
        }
        merge_final_untracked_rows(&mut rows, untracked_rows)?;
        for delta in &sorted_tracked {
            apply_complete_hot_snapshot_delta(
                &mut rows,
                delta,
                absence_guards,
                &mut retired_untracked_json_refs,
            )?;
        }

        // A replacement generation cannot inherit a checkpoint baseline from
        // its source: that before image belongs to the retired generation.
        // The one exception is publishing the checkpoint itself, where every
        // final tracked row is the clean baseline by definition.
        let tracked_baseline = if working_diff_capture_checkpoint_commit_id.is_some() {
            WorkingDiffBaseline::Clean
        } else {
            WorkingDiffBaseline::Disabled
        };
        normalize_complete_hot_snapshot_baselines(&mut rows, tracked_baseline)?;

        let mut final_tracked = BTreeMap::new();
        let mut schema_keys = BTreeSet::new();
        for (identity, bytes) in &rows {
            schema_keys.insert(identity.schema_key.clone());
            if !decode_head_value(bytes.as_ref())?.untracked {
                final_tracked.insert(identity.clone(), bytes.clone());
            }
        }

        stage_complete_collection_controls(self.writes, branch_id, generation, &rows)?;
        stage_complete_hot_rows(self.writes, branch_id, generation, rows);
        JsonStoreWriter::stage_untracked_reclaim_candidates(
            self.writes,
            retired_untracked_json_refs
                .into_iter()
                .map(JsonRef::from_hash_bytes),
        );
        *coverage = WorkingDiffIndexCoverage::default();
        Ok((
            HotTrackedSnapshot {
                rows: final_tracked,
            },
            schema_keys,
        ))
    }
}

#[allow(clippy::too_many_arguments)]
async fn stage_incremental_file_delete_cascades(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    branch_id: &str,
    generation: CommitId,
    deltas: &[&CurrentStateDeltaRef<'_>],
    working_diff_capture_checkpoint_commit_id: Option<CommitId>,
    reset_working_diff_baselines: bool,
    coverage: &mut WorkingDiffIndexCoverage,
    retired_untracked_json_refs: &mut BTreeSet<[u8; JSON_REF_BYTES]>,
) -> Result<(), LixError> {
    let mut cascades = BTreeMap::<String, &CurrentStateDeltaRef<'_>>::new();
    for cascade in deltas {
        let Some(file_id) = file_delete_cascade_id(cascade)? else {
            continue;
        };
        cascades.insert(file_id, cascade);
    }
    if cascades.is_empty() {
        return Ok(());
    }
    #[cfg(test)]
    INCREMENTAL_CASCADE_EXPLICIT_INDEX_BUILDS.with(|builds| {
        builds.set(builds.get().saturating_add(1));
    });
    let explicit = deltas
        .iter()
        .map(|delta| HeadRowIdentity {
            schema_key: delta.schema_key.to_string(),
            entity_pk: delta.entity_pk.clone(),
            file_id: delta.file_id.map(str::to_string),
        })
        .collect::<BTreeSet<_>>();
    let identities =
        hot_load_file_scope_identities(store, branch_id, generation, &cascades).await?;
    let values = hot_load_primary_identity_bytes(store, &identities).await?;
    let scope = hot_scope_prefix(branch_id, generation);
    let key_capacity = identities
        .iter()
        .try_fold(0_usize, |total, identity| {
            let key_len = encoded_hot_identity_key_len(
                scope.len(),
                &identity.schema_key,
                &identity.entity_pk,
                identity.file_id.as_deref(),
            )?;
            total.checked_add(key_len)
        })
        .unwrap_or(0);
    let mut mutations = HotCascadeMutationBuffers::with_capacity(
        identities.len(),
        key_capacity,
        working_diff_capture_checkpoint_commit_id.is_some(),
    );
    let diff_scope = working_diff_capture_checkpoint_commit_id.map(|checkpoint_commit_id| {
        encode_working_diff_scope_prefix(branch_id, checkpoint_commit_id, generation)
    });
    let diff_key_capacity = diff_scope.as_deref().map_or(0, |scope| {
        identities
            .iter()
            .try_fold(0_usize, |total, identity| {
                total.checked_add(encoded_hot_identity_key_len(
                    scope.len(),
                    &identity.schema_key,
                    &identity.entity_pk,
                    identity.file_id.as_deref(),
                )?)
            })
            .unwrap_or(0)
    });
    let mut diff_key_bytes = Vec::with_capacity(diff_key_capacity);
    let mut diff_puts = Vec::with_capacity(diff_scope.as_ref().map_or(0, |_| identities.len()));
    for (identity, previous) in identities.into_iter().zip(values) {
        let row_identity = identity.clone().into_row_identity();
        if explicit.contains(&row_identity) {
            continue;
        }
        let cascade = cascades
            .get(
                identity
                    .file_id
                    .as_deref()
                    .expect("file-backed identity requires file id"),
            )
            .expect("file scan only returns requested cascade ids");
        let Some(previous) = previous else {
            return Err(head_value_error(
                "hot file-backed identity has no authoritative primary row",
            ));
        };
        let existing = decode_head_value(&previous)?;
        if (cascade.untracked && !existing.untracked) || existing.deleted {
            continue;
        }
        let row_start = mutations.key_bytes.len();
        mutations.key_bytes.extend_from_slice(&scope);
        write_key_string(
            &mut mutations.key_bytes,
            &identity.schema_key,
            KEY_PART_FINAL,
        );
        write_file_id(&mut mutations.key_bytes, identity.file_id.as_deref());
        write_entity_pk(&mut mutations.key_bytes, &identity.entity_pk);
        let row_key = BufferRange::new(row_start, mutations.key_bytes.len() - row_start);
        if existing.untracked {
            collect_hot_untracked_refs(existing, retired_untracked_json_refs);
            mutations.row_deletes.push(row_key);
            continue;
        }
        let (baseline, newly_dirty) = if reset_working_diff_baselines {
            (WorkingDiffBaseline::Clean, false)
        } else {
            next_cascade_working_diff_baseline(working_diff_capture_checkpoint_commit_id, existing)?
        };
        if newly_dirty {
            let key = append_hot_diff_key_parts(
                &mut diff_key_bytes,
                diff_scope
                    .as_deref()
                    .expect("new cascade dirty row requires active checkpoint"),
                &identity.schema_key,
                &identity.entity_pk,
                identity.file_id.as_deref(),
            );
            coverage
                .add_encoded_group_key(&diff_key_bytes[key.clone()])
                .ok_or_else(|| head_value_error("hot working-diff index count exceeds u64"))?;
            diff_puts.push(EncodedPut {
                key: buffer_range(&key),
                value: BufferRange::default(),
            });
        }
        let value = append_head_value(
            &mut mutations.value_bytes,
            &HeadValueRef {
                change_id: cascade.change_id,
                commit_id: cascade.commit_id,
                untracked: false,
                deleted: true,
                created_at: existing.created_at,
                updated_at: cascade.updated_at,
                snapshot: JsonSlotRef::None,
                metadata: JsonSlotRef::None,
                working_diff_baseline: baseline,
            },
        )?;
        let value = buffer_range(&value);
        mutations.row_puts.push(EncodedPut {
            key: row_key,
            value,
        });
    }
    stage_hot_diff_batch(
        writes,
        diff_scope.as_deref().unwrap_or_default(),
        diff_key_bytes,
        diff_puts,
    )?;
    if !mutations.row_puts.is_empty() || !mutations.row_deletes.is_empty() {
        stage_hot_encoded_mutation_ranges(
            writes,
            Bytes::from(mutations.key_bytes),
            Bytes::from(mutations.value_bytes),
            mutations.row_puts,
            mutations.row_deletes,
            Vec::new(),
        );
    }
    Ok(())
}

struct HotCascadeMutationBuffers {
    key_bytes: Vec<u8>,
    value_bytes: Vec<u8>,
    row_puts: Vec<EncodedPut>,
    row_deletes: Vec<BufferRange>,
}

impl HotCascadeMutationBuffers {
    fn with_capacity(row_capacity: usize, key_capacity: usize, active_checkpoint: bool) -> Self {
        let checkpoint_bytes = if active_checkpoint {
            WORKING_DIFF_VERSION_BYTES
        } else {
            0
        };
        let value_bytes_per_row = HEAD_VALUE_HEADER_BYTES.checked_add(checkpoint_bytes);
        let value_capacity = value_bytes_per_row
            .and_then(|value_bytes| row_capacity.checked_mul(value_bytes))
            .unwrap_or(0);
        Self {
            key_bytes: Vec::with_capacity(key_capacity),
            value_bytes: Vec::with_capacity(value_capacity),
            row_puts: Vec::with_capacity(row_capacity),
            row_deletes: Vec::with_capacity(row_capacity),
        }
    }
}

#[cfg(test)]
std::thread_local! {
    static INCREMENTAL_CASCADE_EXPLICIT_INDEX_BUILDS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn incremental_cascade_explicit_index_builds() -> usize {
    INCREMENTAL_CASCADE_EXPLICIT_INDEX_BUILDS.with(std::cell::Cell::get)
}

fn next_cascade_working_diff_baseline(
    active_checkpoint_commit_id: Option<CommitId>,
    previous: HeadValueView<'_>,
) -> Result<(WorkingDiffBaseline, bool), LixError> {
    if active_checkpoint_commit_id.is_none() {
        return Ok((WorkingDiffBaseline::Disabled, false));
    }
    match previous.working_diff_baseline {
        WorkingDiffBaseline::Clean => {
            let before = previous
                .working_diff_version()
                .ok_or_else(|| head_value_error("tracked cascade member has no version"))?;
            Ok((WorkingDiffBaseline::BeforePresent(before), true))
        }
        WorkingDiffBaseline::BeforeAbsent => Ok((WorkingDiffBaseline::BeforeAbsent, false)),
        WorkingDiffBaseline::BeforePresent(before) => {
            Ok((WorkingDiffBaseline::BeforePresent(before), false))
        }
        WorkingDiffBaseline::Disabled => Err(head_value_error(
            "active checkpoint generation contains a cascade member without a baseline",
        )),
    }
}

/// Selects the baseline for one primary-row mutation and whether the sparse
/// dirty-key index needs its first entry. The current primary row is already
/// loaded for retention and insert validation, so this must remain purely
/// in-memory: adding a `HOT_DIFF` lookup here would recreate the avoidable
/// second read batch this layout removes.
fn next_hot_working_diff_baseline(
    active_checkpoint_commit_id: Option<CommitId>,
    delta: &CurrentStateDeltaRef<'_>,
    previous: Option<HeadValueView<'_>>,
) -> Result<(WorkingDiffBaseline, bool), LixError> {
    if delta.untracked || active_checkpoint_commit_id.is_none() {
        return Ok((WorkingDiffBaseline::Disabled, false));
    }
    let Some(previous) = previous else {
        return Ok((WorkingDiffBaseline::BeforeAbsent, true));
    };
    if previous.untracked {
        return Err(head_value_error(
            "tracked mutation has an untracked primary before image",
        ));
    }
    match previous.working_diff_baseline {
        WorkingDiffBaseline::Clean => {
            let before = previous
                .working_diff_version()
                .ok_or_else(|| head_value_error("tracked mutation has no working-diff version"))?;
            Ok((WorkingDiffBaseline::BeforePresent(before), true))
        }
        WorkingDiffBaseline::BeforeAbsent => Ok((WorkingDiffBaseline::BeforeAbsent, false)),
        WorkingDiffBaseline::BeforePresent(before) => {
            Ok((WorkingDiffBaseline::BeforePresent(before), false))
        }
        WorkingDiffBaseline::Disabled => Err(head_value_error(
            "active checkpoint generation contains a tracked row without a baseline",
        )),
    }
}

async fn load_hot_untracked_generation(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
) -> Result<HotRowMap, LixError> {
    let filter = TrackedStateFilter {
        include_tombstones: true,
        ..TrackedStateFilter::default()
    };
    let HotScanEntries::Decoded(entries) =
        hot_scan_entries(store, branch_id, generation, &filter, None).await?
    else {
        unreachable!("an unconstrained HOT scan cannot select the finite point-read route");
    };
    let mut rows = BTreeMap::new();
    for (identity, bytes) in entries {
        let value = decode_head_value(bytes.as_ref())?;
        if !value.untracked {
            continue;
        }
        if value.deleted {
            return Err(head_value_error(
                "untracked hot row must be physically removed rather than tombstoned",
            ));
        }
        match rows.entry(identity.into_row_identity()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(bytes);
            }
            std::collections::btree_map::Entry::Occupied(entry) => {
                let identity = entry.key();
                return Err(LixError::new(
                    LixError::CODE_UNIQUE,
                    format!(
                        "hot generation contains duplicate untracked identity in schema '{}' entity_pk {:?}",
                        identity.schema_key, identity.entity_pk
                    ),
                ));
            }
        }
    }
    Ok(rows)
}

fn merge_final_untracked_rows(
    rows: &mut HotRowMap,
    untracked_rows: HotRowMap,
) -> Result<(), LixError> {
    for (identity, bytes) in untracked_rows {
        match rows.entry(identity) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(bytes);
            }
            std::collections::btree_map::Entry::Occupied(entry) => {
                let identity = entry.key();
                return Err(LixError::new(
                    LixError::CODE_UNIQUE,
                    format!(
                        "cannot materialize tracked and untracked hot rows with the same identity in schema '{}' entity_pk {:?}",
                        identity.schema_key, identity.entity_pk
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn sorted_lifecycle_hot_deltas<'a>(
    deltas: &'a [CurrentStateDeltaRef<'a>],
    expect_untracked: bool,
) -> Result<Vec<&'a CurrentStateDeltaRef<'a>>, LixError> {
    let mut sorted = Vec::with_capacity(deltas.len());
    for delta in deltas {
        delta.validate()?;
        if delta.untracked != expect_untracked {
            return Err(head_value_error(if expect_untracked {
                "untracked lifecycle delta was marked tracked"
            } else {
                "tracked lifecycle delta was marked untracked"
            }));
        }
        sorted.push(delta);
    }
    sorted.sort_unstable_by(|left, right| compare_hot_deltas(left, right));
    for pair in sorted.windows(2) {
        if compare_hot_deltas(pair[0], pair[1]).is_eq() {
            return Err(current_state_duplicate_delta_error(pair[1]));
        }
    }
    Ok(sorted)
}

fn reject_lifecycle_retention_collisions(
    untracked: &[&CurrentStateDeltaRef<'_>],
    tracked: &[&CurrentStateDeltaRef<'_>],
) -> Result<(), LixError> {
    let mut untracked_index = 0;
    let mut tracked_index = 0;
    while untracked_index < untracked.len() && tracked_index < tracked.len() {
        match compare_hot_deltas(untracked[untracked_index], tracked[tracked_index]) {
            Ordering::Less => untracked_index += 1,
            Ordering::Greater => tracked_index += 1,
            Ordering::Equal => {
                if !untracked[untracked_index].physically_deletes() {
                    return Err(current_state_duplicate_delta_error(tracked[tracked_index]));
                }
                untracked_index += 1;
                tracked_index += 1;
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_complete_hot_snapshot_delta(
    rows: &mut HotRowMap,
    delta: &CurrentStateDeltaRef<'_>,
    absence_guards: &BTreeSet<TrackedStateKey>,
    retired_untracked_json_refs: &mut BTreeSet<[u8; JSON_REF_BYTES]>,
) -> Result<(), LixError> {
    apply_complete_file_delete_cascade(rows, delta, retired_untracked_json_refs)?;
    let identity = HeadRowIdentity {
        schema_key: delta.schema_key.to_string(),
        entity_pk: delta.entity_pk.clone(),
        file_id: delta.file_id.map(str::to_string),
    };
    let previous = rows.get(&identity).map(|bytes| bytes.as_ref());
    if let Some(previous) = previous {
        let existing = decode_head_value(previous)?;
        reject_guarded_live_member(absence_guards, delta, existing)?;
        reject_retention_change(delta, existing)?;
        if existing.untracked {
            collect_retired_untracked_json_refs(existing, delta, retired_untracked_json_refs);
        }
    }
    if delta.physically_deletes() {
        rows.remove(&identity);
    } else {
        let created_at = previous
            .map(decode_head_value)
            .transpose()?
            .map_or(delta.created_at, |value| value.created_at);
        rows.insert(
            identity,
            Bytes::from(encode_head_value(
                &delta.value_ref(created_at, WorkingDiffBaseline::Disabled),
            )?),
        );
    }
    Ok(())
}

fn apply_complete_file_delete_cascade(
    rows: &mut HotRowMap,
    delta: &CurrentStateDeltaRef<'_>,
    retired_untracked_json_refs: &mut BTreeSet<[u8; JSON_REF_BYTES]>,
) -> Result<(), LixError> {
    let Some(file_id) = file_delete_cascade_id(delta)? else {
        return Ok(());
    };
    let identities = rows
        .keys()
        .filter(|identity| identity.file_id.as_deref() == Some(file_id.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    for identity in identities {
        let Some(previous) = rows.get(&identity) else {
            continue;
        };
        let existing = decode_head_value(previous.as_ref())?;
        if (delta.untracked && !existing.untracked) || existing.deleted {
            continue;
        }
        if existing.untracked {
            collect_hot_untracked_refs(existing, retired_untracked_json_refs);
            rows.remove(&identity);
            continue;
        }
        rows.insert(
            identity,
            Bytes::from(encode_head_value(&HeadValueRef {
                change_id: delta.change_id,
                commit_id: delta.commit_id,
                untracked: false,
                deleted: true,
                created_at: existing.created_at,
                updated_at: delta.updated_at,
                snapshot: JsonSlotRef::None,
                metadata: JsonSlotRef::None,
                working_diff_baseline: WorkingDiffBaseline::Disabled,
            })?),
        );
    }
    Ok(())
}

fn file_delete_cascade_id(delta: &CurrentStateDeltaRef<'_>) -> Result<Option<String>, LixError> {
    if delta.schema_key != FILE_DESCRIPTOR_SCHEMA_KEY || !delta.deleted {
        return Ok(None);
    }
    delta
        .entity_pk
        .as_single_string_owned()
        .map(Some)
        .map_err(|error| {
            head_value_error(&format!(
                "file descriptor tombstone has invalid identity: {error}"
            ))
        })
}

fn normalize_complete_hot_snapshot_baselines(
    rows: &mut HotRowMap,
    tracked_baseline: WorkingDiffBaseline,
) -> Result<(), LixError> {
    for bytes in rows.values_mut() {
        let value = decode_head_value(bytes.as_ref())?;
        if value.untracked {
            continue;
        }
        *bytes = Bytes::from(reencode_head_value_with_baseline(value, tracked_baseline)?);
    }
    Ok(())
}

fn compare_hot_deltas(
    left: &CurrentStateDeltaRef<'_>,
    right: &CurrentStateDeltaRef<'_>,
) -> Ordering {
    left.schema_key
        .cmp(right.schema_key)
        .then_with(|| left.entity_pk.cmp(right.entity_pk))
        .then_with(|| left.file_id.cmp(&right.file_id))
}

fn hot_identity(
    branch_id: &str,
    generation: CommitId,
    schema_key: &str,
    entity_pk: &EntityPk,
    file_id: Option<&str>,
) -> HeadIdentity {
    HeadIdentity {
        branch_id: branch_id.to_string(),
        generation,
        schema_key: schema_key.to_string(),
        entity_pk: entity_pk.clone(),
        file_id: file_id.map(str::to_string),
    }
}

struct EncodedHotMutationIdentities {
    key_bytes: Bytes,
    key_ranges: Vec<EncodedHotMutationIdentityRanges>,
}

#[derive(Clone, Copy)]
struct EncodedHotMutationIdentityRanges {
    row_key: BufferRange,
    file_schema_key: Option<BufferRange>,
}

fn encode_hot_mutation_identities(
    branch_id: &str,
    generation: CommitId,
    deltas: &[&CurrentStateDeltaRef<'_>],
) -> EncodedHotMutationIdentities {
    let scope = hot_scope_prefix(branch_id, generation);
    let encoded_capacity = encoded_hot_mutation_identity_capacity(scope.len(), deltas).unwrap_or(0);
    let mut encoded = Vec::with_capacity(encoded_capacity);
    let mut key_ranges = Vec::with_capacity(deltas.len());
    for delta in deltas {
        key_ranges.push(append_hot_mutation_identity(&mut encoded, &scope, delta));
    }
    EncodedHotMutationIdentities {
        key_bytes: Bytes::from(encoded),
        key_ranges,
    }
}

fn encoded_hot_mutation_identity_capacity(
    scope_len: usize,
    deltas: &[&CurrentStateDeltaRef<'_>],
) -> Option<usize> {
    deltas.iter().try_fold(0_usize, |total, delta| {
        let key_len = encoded_hot_identity_key_len(
            scope_len,
            delta.schema_key,
            delta.entity_pk,
            delta.file_id,
        )?;
        let marker_len = if delta.file_id.is_some() {
            scope_len.checked_add(encoded_key_bytes_len(delta.schema_key.as_bytes())?)?
        } else {
            0
        };
        total.checked_add(key_len)?.checked_add(marker_len)
    })
}

fn append_hot_mutation_identity(
    encoded: &mut Vec<u8>,
    scope: &[u8],
    delta: &CurrentStateDeltaRef<'_>,
) -> EncodedHotMutationIdentityRanges {
    let row_start = encoded.len();
    encoded.extend_from_slice(scope);
    write_key_string(encoded, delta.schema_key, KEY_PART_FINAL);
    write_file_id(encoded, delta.file_id);
    write_entity_pk(encoded, delta.entity_pk);
    let row_key = BufferRange::new(row_start, encoded.len() - row_start);

    let file_schema_key = delta.file_id.map(|_| {
        let marker_start = encoded.len();
        encoded.extend_from_slice(scope);
        write_key_string(encoded, delta.schema_key, KEY_PART_FINAL);
        BufferRange::new(marker_start, encoded.len() - marker_start)
    });
    EncodedHotMutationIdentityRanges {
        row_key,
        file_schema_key,
    }
}

async fn hot_load_primary_mutation_identity_refs(
    store: &(impl StorageAdapterRead + ?Sized),
    identities: &EncodedHotMutationIdentities,
    deltas: &[&CurrentStateDeltaRef<'_>],
    absence_guards_validated: bool,
    validated_absent_file_id: Option<&str>,
) -> Result<Vec<Option<Bytes>>, LixError> {
    assert_eq!(
        identities.key_ranges.len(),
        deltas.len(),
        "every hot mutation identity must have one source delta"
    );
    let read_count = deltas
        .iter()
        .filter(|delta| {
            !hot_delta_is_guarded_by_absent_file(
                delta,
                absence_guards_validated,
                validated_absent_file_id,
            )
        })
        .count();
    if read_count == 0 {
        return Ok(Vec::new());
    }
    if read_count == deltas.len()
        && let Some(values) = hot_scan_dense_mutation_identity_range(store, identities).await?
    {
        return Ok(values);
    }
    let mut keys = Vec::with_capacity(read_count);
    for (identity, delta) in identities.key_ranges.iter().zip(deltas) {
        if hot_delta_is_guarded_by_absent_file(
            delta,
            absence_guards_validated,
            validated_absent_file_id,
        ) {
            continue;
        }
        let start = identity.row_key.offset();
        keys.push(StorageKey(
            identities
                .key_bytes
                .slice(start..start + identity.row_key.len()),
        ));
    }
    PointReadPlan::new(HOT_ROW_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?
        .value
        .into_iter()
        .map(|value| value.map(full_value_bytes).transpose())
        .collect()
}

async fn hot_scan_dense_mutation_identity_range(
    store: &(impl StorageAdapterRead + ?Sized),
    identities: &EncodedHotMutationIdentities,
) -> Result<Option<Vec<Option<Bytes>>>, LixError> {
    hot_scan_dense_encoded_key_range(store, identities.key_ranges.len(), |index| {
        let range = identities.key_ranges[index].row_key;
        let start = range.offset();
        &identities.key_bytes[start..start.saturating_add(range.len())]
    })
    .await
}

fn hot_delta_is_guarded_by_absent_file(
    delta: &CurrentStateDeltaRef<'_>,
    absence_guards_validated: bool,
    validated_absent_file_id: Option<&str>,
) -> bool {
    absence_guards_validated
        && validated_absent_file_id.is_some_and(|file_id| delta.file_id == Some(file_id))
}

/// Adds one encoded current-state value to the shared arena's capacity plan.
///
/// Ordinary commits are exact. During an active checkpoint, a tracked row
/// may carry a fixed-size first-before image, so the plan reserves that upper
/// bound without decoding every predecessor a third time. Physical untracked
/// deletes produce no value. Every operation is checked so an impossible
/// batch can safely use a zero-capacity growth fallback and reach the normal
/// fallible encoder instead of attempting an overflowing allocation.
fn checked_add_hot_next_value_capacity(
    total: usize,
    delta: &CurrentStateDeltaRef<'_>,
    active_checkpoint: bool,
) -> Option<usize> {
    if delta.physically_deletes() {
        return Some(total);
    }
    let (snapshot_len, metadata_len) = if delta.deleted {
        (0, 0)
    } else {
        (
            encoded_hot_slot_len(delta.snapshot),
            encoded_hot_slot_len(delta.metadata),
        )
    };
    // Keep the plan bounded by the same on-disk u32 fields the encoder checks.
    u32::try_from(snapshot_len).ok()?;
    u32::try_from(metadata_len).ok()?;
    let baseline_len = if active_checkpoint && !delta.untracked {
        WORKING_DIFF_VERSION_BYTES
    } else {
        0
    };
    let encoded_len = HEAD_VALUE_HEADER_BYTES
        .checked_add(snapshot_len)?
        .checked_add(metadata_len)?
        .checked_add(baseline_len)?;
    total.checked_add(encoded_len)
}

fn encoded_hot_slot_len(slot: JsonSlotRef<'_>) -> usize {
    match slot {
        JsonSlotRef::None => 0,
        JsonSlotRef::Ref(_) => JSON_REF_BYTES,
        JsonSlotRef::Inline(json) => json.len(),
    }
}

fn stage_hot_mutation_batch(
    writes: &mut StorageWriteSet,
    identities: EncodedHotMutationIdentities,
    value_bytes: Bytes,
    value_ranges: Vec<Option<Range<usize>>>,
) {
    assert_eq!(
        identities.key_ranges.len(),
        value_ranges.len(),
        "every hot mutation identity must have one staged value"
    );
    let put_count = value_ranges.iter().flatten().count();
    let delete_count = value_ranges.len() - put_count;
    let file_count = identities
        .key_ranges
        .iter()
        .filter(|identity| identity.file_schema_key.is_some())
        .count();
    let mut row_puts = Vec::with_capacity(put_count);
    let mut row_deletes = Vec::with_capacity(delete_count);
    let mut file_schema_puts = Vec::with_capacity(file_count);
    for (identity, value) in identities.key_ranges.iter().zip(&value_ranges) {
        if let Some(value) = value {
            let value = buffer_range(value);
            row_puts.push(EncodedPut {
                key: identity.row_key,
                value,
            });
        } else {
            row_deletes.push(identity.row_key);
        }
        if let Some(key) = identity.file_schema_key {
            file_schema_puts.push(key);
        }
    }
    file_schema_puts.sort_unstable_by(|left, right| {
        let left = &identities.key_bytes[left.offset()..left.offset().saturating_add(left.len())];
        let right =
            &identities.key_bytes[right.offset()..right.offset().saturating_add(right.len())];
        left.cmp(right)
    });
    file_schema_puts.dedup_by(|left, right| {
        identities.key_bytes[left.offset()..left.offset().saturating_add(left.len())]
            == identities.key_bytes[right.offset()..right.offset().saturating_add(right.len())]
    });

    stage_hot_encoded_mutation_ranges(
        writes,
        identities.key_bytes,
        value_bytes,
        row_puts,
        row_deletes,
        file_schema_puts,
    );
}

fn stage_hot_encoded_mutation_ranges(
    writes: &mut StorageWriteSet,
    key_bytes: Bytes,
    value_bytes: Bytes,
    row_puts: Vec<EncodedPut>,
    row_deletes: Vec<BufferRange>,
    mut file_schema_puts: Vec<BufferRange>,
) {
    let row_batch = EncodedMutationBatch::try_new(
        key_bytes.clone(),
        value_bytes.clone(),
        row_puts,
        row_deletes,
    )
    .expect("hot row ranges originate in the supplied encoded buffers");
    writes.stage_encoded_batch(HOT_ROW_SPACE, row_batch);
    file_schema_puts.retain(|key| {
        !writes.contains_put(
            HOT_FILE_SPACE,
            &key_bytes[key.offset()..key.offset().saturating_add(key.len())],
        )
    });
    if !file_schema_puts.is_empty() {
        let file_puts = file_schema_puts
            .into_iter()
            .map(|key| EncodedPut {
                key,
                value: BufferRange::new(0, 0),
            })
            .collect();
        let file_batch =
            EncodedMutationBatch::try_new(key_bytes, Bytes::new(), file_puts, Vec::new())
                .expect("hot file schema ranges originate in the supplied encoded buffers");
        writes.stage_encoded_batch(HOT_FILE_SPACE, file_batch);
    }
}

fn stage_hot_diff_batch(
    writes: &mut StorageWriteSet,
    scope: &[u8],
    identity_key_bytes: Vec<u8>,
    identity_puts: Vec<EncodedPut>,
) -> Result<(), LixError> {
    if identity_puts.is_empty() {
        return Ok(());
    }
    if scope.is_empty() {
        return Err(head_value_error(
            "hot diff identities require a checkpoint scope",
        ));
    }
    if identity_puts.len() < HOT_DIFF_PACK_MIN_IDENTITIES {
        let batch = EncodedMutationBatch::try_new(
            Bytes::from(identity_key_bytes),
            Bytes::new(),
            identity_puts,
            Vec::new(),
        )
        .expect("direct hot diff ranges originate in the supplied encoded buffer");
        writes.stage_encoded_batch(HOT_DIFF_SPACE, batch);
        return Ok(());
    }

    // HOT_DIFF is a set of eagerly persisted identities. Store bounded
    // segments instead of repeating the checkpoint scope in one LSM entry per
    // identity. The content digest makes each segment key deterministic while
    // the value retains every identity suffix in the same atomic write set.
    let value_capacity = identity_key_bytes
        .len()
        .saturating_sub(scope.len().saturating_mul(identity_puts.len()))
        .saturating_add(identity_puts.len().saturating_mul(4))
        .saturating_add(
            identity_puts
                .len()
                .div_ceil(HOT_DIFF_SEGMENT_MAX_IDENTITIES as usize)
                .saturating_mul(HOT_DIFF_SEGMENT_HEADER_BYTES),
        );
    let mut value_bytes = Vec::with_capacity(value_capacity);
    let mut value_ranges = Vec::<Range<usize>>::new();
    let mut segment_start = value_bytes.len();
    value_bytes.push(HOT_DIFF_SEGMENT_VERSION);
    value_bytes.extend_from_slice(&0_u32.to_le_bytes());
    let mut segment_count = 0_u32;

    for put in identity_puts {
        let key_start = put.key.offset();
        let key_end = key_start
            .checked_add(put.key.len())
            .ok_or_else(|| head_value_error("hot diff key range overflow"))?;
        let full_key = identity_key_bytes
            .get(key_start..key_end)
            .ok_or_else(|| head_value_error("hot diff key range escapes its arena"))?;
        let suffix = full_key
            .strip_prefix(scope)
            .ok_or_else(|| head_value_error("hot diff identity key is outside its scope"))?;
        let suffix_len = u32::try_from(suffix.len())
            .map_err(|_| head_value_error("hot diff identity suffix exceeds u32"))?;
        let encoded_len = 4_usize.saturating_add(suffix.len());
        if HOT_DIFF_SEGMENT_HEADER_BYTES.saturating_add(encoded_len) > HOT_DIFF_SEGMENT_MAX_BYTES {
            return Err(head_value_error(
                "hot diff identity exceeds the segment size limit",
            ));
        }
        let current_len = value_bytes.len() - segment_start;
        if segment_count > 0
            && (segment_count == HOT_DIFF_SEGMENT_MAX_IDENTITIES
                || current_len.saturating_add(encoded_len) > HOT_DIFF_SEGMENT_MAX_BYTES)
        {
            value_bytes[segment_start + 1..segment_start + HOT_DIFF_SEGMENT_HEADER_BYTES]
                .copy_from_slice(&segment_count.to_le_bytes());
            value_ranges.push(segment_start..value_bytes.len());
            segment_start = value_bytes.len();
            value_bytes.push(HOT_DIFF_SEGMENT_VERSION);
            value_bytes.extend_from_slice(&0_u32.to_le_bytes());
            segment_count = 0;
        }
        value_bytes.extend_from_slice(&suffix_len.to_le_bytes());
        value_bytes.extend_from_slice(suffix);
        segment_count += 1;
    }
    value_bytes[segment_start + 1..segment_start + HOT_DIFF_SEGMENT_HEADER_BYTES]
        .copy_from_slice(&segment_count.to_le_bytes());
    value_ranges.push(segment_start..value_bytes.len());

    let mut key_bytes = Vec::with_capacity(value_ranges.len().saturating_mul(scope.len() + 32));
    let mut puts = Vec::with_capacity(value_ranges.len());
    for value in value_ranges {
        let key_start = key_bytes.len();
        key_bytes.extend_from_slice(scope);
        key_bytes.extend_from_slice(blake3::hash(&value_bytes[value.clone()]).as_bytes());
        puts.push(EncodedPut {
            key: BufferRange::new(key_start, key_bytes.len() - key_start),
            value: buffer_range(&value),
        });
    }
    let batch = EncodedMutationBatch::try_new(
        Bytes::from(key_bytes),
        Bytes::from(value_bytes),
        puts,
        Vec::new(),
    )
    .expect("hot diff segment ranges originate in the supplied encoded buffers");
    writes.stage_encoded_batch(HOT_DIFF_SPACE, batch);
    Ok(())
}

fn buffer_range(range: &Range<usize>) -> BufferRange {
    BufferRange::new(range.start, range.end - range.start)
}

fn stage_complete_hot_rows(
    writes: &mut StorageWriteSet,
    branch_id: &str,
    generation: CommitId,
    rows: HotRowMap,
) {
    if rows.is_empty() {
        return;
    }
    let scope = hot_scope_prefix(branch_id, generation);
    let file_schema_keys = rows
        .keys()
        .filter(|identity| identity.file_id.is_some())
        .map(|identity| identity.schema_key.clone())
        .collect::<BTreeSet<_>>();
    let value_capacity = rows.values().map(Bytes::len).sum();
    let marker_key_capacity = file_schema_keys
        .iter()
        .map(|schema_key| {
            scope
                .len()
                .saturating_add(encoded_key_bytes_len(schema_key.as_bytes()).unwrap_or(0))
        })
        .sum::<usize>();
    let key_capacity = rows
        .len()
        .saturating_mul(scope.len() + 32)
        .saturating_add(
            rows.keys()
                .map(|identity| {
                    identity
                        .schema_key
                        .len()
                        .saturating_add(identity.entity_pk.estimated_heap_bytes())
                        .saturating_add(
                            identity
                                .file_id
                                .as_ref()
                                .map_or(0, |file_id| file_id.len().saturating_mul(2)),
                        )
                })
                .sum(),
        )
        .saturating_add(marker_key_capacity);
    let mut key_bytes = Vec::with_capacity(key_capacity);
    let mut value_bytes = Vec::with_capacity(value_capacity);
    let mut row_puts = Vec::with_capacity(rows.len());
    let mut file_puts = Vec::with_capacity(file_schema_keys.len());
    for (identity, value) in rows {
        let value_start = value_bytes.len();
        value_bytes.extend_from_slice(value.as_ref());
        let value = BufferRange::new(value_start, value_bytes.len() - value_start);

        let row_start = key_bytes.len();
        key_bytes.extend_from_slice(&scope);
        write_key_string(&mut key_bytes, &identity.schema_key, KEY_PART_FINAL);
        write_file_id(&mut key_bytes, identity.file_id.as_deref());
        write_entity_pk(&mut key_bytes, &identity.entity_pk);
        row_puts.push(EncodedPut {
            key: BufferRange::new(row_start, key_bytes.len() - row_start),
            value,
        });
    }
    for schema_key in file_schema_keys {
        let file_start = key_bytes.len();
        key_bytes.extend_from_slice(&scope);
        write_key_string(&mut key_bytes, &schema_key, KEY_PART_FINAL);
        file_puts.push(EncodedPut {
            key: BufferRange::new(file_start, key_bytes.len() - file_start),
            value: BufferRange::new(0, 0),
        });
    }
    let key_bytes = Bytes::from(key_bytes);
    let value_bytes = Bytes::from(value_bytes);
    let row_batch =
        EncodedMutationBatch::try_new(key_bytes.clone(), value_bytes.clone(), row_puts, Vec::new())
            .expect("complete hot row ranges originate in the supplied encoded buffers");
    writes.stage_encoded_batch(HOT_ROW_SPACE, row_batch);
    file_puts.retain(|put| {
        !writes.contains_put(
            HOT_FILE_SPACE,
            &key_bytes[put.key.offset()..put.key.offset().saturating_add(put.key.len())],
        )
    });
    if !file_puts.is_empty() {
        let file_batch =
            EncodedMutationBatch::try_new(key_bytes, Bytes::new(), file_puts, Vec::new())
                .expect("complete hot file ranges originate in the supplied encoded buffers");
        writes.stage_encoded_batch(HOT_FILE_SPACE, file_batch);
    }
}

#[cfg(test)]
pub(super) fn stage_test_hot_value(
    writes: &mut StorageWriteSet,
    identity: &HeadIdentity,
    value: &HeadValue,
) -> Result<(), LixError> {
    let rows = BTreeMap::from([(
        HeadRowIdentity {
            schema_key: identity.schema_key.clone(),
            entity_pk: identity.entity_pk.clone(),
            file_id: identity.file_id.clone(),
        },
        Bytes::from(encode_head_value(&value.as_ref())?),
    )]);
    stage_complete_hot_rows(writes, &identity.branch_id, identity.generation, rows);
    Ok(())
}

fn stage_hot_bootstrap(
    writes: &mut StorageWriteSet,
    branch_id: &str,
    generation: CommitId,
    parent_rows: Vec<MaterializedTrackedStateRow>,
    preserved_untracked_rows: Vec<MaterializedLiveStateRow>,
    deltas: &[&CurrentStateDeltaRef<'_>],
    absence_guards: &BTreeSet<TrackedStateKey>,
    working_diff_capture_checkpoint_commit_id: Option<CommitId>,
    coverage: &mut WorkingDiffIndexCoverage,
) -> Result<(), LixError> {
    let mut rows = HotRowMap::new();
    let tracked_baseline = if working_diff_capture_checkpoint_commit_id.is_some() {
        WorkingDiffBaseline::Clean
    } else {
        WorkingDiffBaseline::Disabled
    };
    for row in parent_rows {
        let key = TrackedStateKey {
            schema_key: row.schema_key.clone(),
            entity_pk: row.entity_pk.clone(),
            file_id: row.file_id.clone(),
        };
        if absence_guards.contains(&key) && !row.deleted {
            return Err(tracked_head_duplicate_insert_error(&key));
        }
        let identity = HeadRowIdentity {
            schema_key: row.schema_key,
            entity_pk: row.entity_pk,
            file_id: row.file_id,
        };
        let value = HeadValueRef {
            change_id: Some(row.change_id),
            commit_id: Some(row.commit_id),
            untracked: false,
            deleted: row.deleted,
            created_at: LixTimestamp::expect_parse("hot bootstrap created_at", &row.created_at),
            updated_at: LixTimestamp::expect_parse("hot bootstrap updated_at", &row.updated_at),
            snapshot: row
                .snapshot_content
                .as_deref()
                .map_or(JsonSlotRef::None, JsonSlotRef::Inline),
            metadata: row
                .metadata
                .as_deref()
                .map_or(JsonSlotRef::None, JsonSlotRef::Inline),
            working_diff_baseline: tracked_baseline,
        };
        if rows
            .insert(identity, Bytes::from(encode_head_value(&value)?))
            .is_some()
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "hot bootstrap contains duplicate tracked row identity",
            ));
        }
    }
    for row in preserved_untracked_rows {
        if !row.untracked || row.deleted {
            return Err(head_value_error(
                "hot bootstrap preserved state must contain only live untracked rows",
            ));
        }
        let key = TrackedStateKey {
            schema_key: row.schema_key.clone(),
            entity_pk: row.entity_pk.clone(),
            file_id: row.file_id.clone(),
        };
        if absence_guards.contains(&key) {
            return Err(tracked_head_duplicate_insert_error(&key));
        }
        let identity = HeadRowIdentity {
            schema_key: row.schema_key,
            entity_pk: row.entity_pk,
            file_id: row.file_id,
        };
        let value = HeadValueRef {
            change_id: None,
            commit_id: None,
            untracked: true,
            deleted: false,
            created_at: row.created_at,
            updated_at: row.updated_at,
            snapshot: row
                .snapshot_content
                .as_deref()
                .map_or(JsonSlotRef::None, JsonSlotRef::Inline),
            metadata: row
                .metadata
                .as_deref()
                .map_or(JsonSlotRef::None, JsonSlotRef::Inline),
            working_diff_baseline: WorkingDiffBaseline::Disabled,
        };
        if rows
            .insert(identity, Bytes::from(encode_head_value(&value)?))
            .is_some()
        {
            return Err(LixError::new(
                LixError::CODE_UNIQUE,
                "cannot materialize tracked and untracked hot rows with the same identity",
            ));
        }
    }
    let mut retired_untracked_json_refs = BTreeSet::new();
    for delta in deltas {
        apply_complete_file_delete_cascade(&mut rows, delta, &mut retired_untracked_json_refs)?;
        let identity = HeadRowIdentity {
            schema_key: delta.schema_key.to_string(),
            entity_pk: delta.entity_pk.clone(),
            file_id: delta.file_id.map(str::to_string),
        };
        let previous = rows.get(&identity).map(|bytes| bytes.as_ref());
        if let Some(previous) = previous {
            let existing = decode_head_value(previous)?;
            reject_guarded_live_member(absence_guards, delta, existing)?;
            reject_retention_change(delta, existing)?;
            if existing.untracked {
                collect_retired_untracked_json_refs(
                    existing,
                    delta,
                    &mut retired_untracked_json_refs,
                );
            }
        }
        if delta.physically_deletes() {
            rows.remove(&identity);
        } else {
            let created_at = previous
                .map(decode_head_value)
                .transpose()?
                .map_or(delta.created_at, |value| value.created_at);
            rows.insert(
                identity,
                Bytes::from(encode_head_value(&delta.value_ref(
                    created_at,
                    if delta.untracked {
                        WorkingDiffBaseline::Disabled
                    } else {
                        tracked_baseline
                    },
                ))?),
            );
        }
    }
    stage_complete_collection_controls(writes, branch_id, generation, &rows)?;
    stage_complete_hot_rows(writes, branch_id, generation, rows);
    JsonStoreWriter::stage_untracked_reclaim_candidates(
        writes,
        retired_untracked_json_refs
            .into_iter()
            .map(JsonRef::from_hash_bytes),
    );
    *coverage = WorkingDiffIndexCoverage::default();
    Ok(())
}

async fn reject_hot_absence_guards(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    guards: &BTreeSet<TrackedStateKey>,
) -> Result<(), LixError> {
    if guards.is_empty() {
        return Ok(());
    }
    let identities = guards
        .iter()
        .map(|key| {
            hot_identity(
                branch_id,
                generation,
                &key.schema_key,
                &key.entity_pk,
                key.file_id.as_deref(),
            )
        })
        .collect::<Vec<_>>();
    for (identity, value) in identities
        .iter()
        .zip(hot_load_primary_identity_bytes(store, &identities).await?)
    {
        let Some(value) = value else {
            continue;
        };
        let value = decode_head_value(&value)?;
        if !value.deleted {
            return Err(tracked_head_duplicate_insert_error(&TrackedStateKey {
                schema_key: identity.schema_key.clone(),
                entity_pk: identity.entity_pk.clone(),
                file_id: identity.file_id.clone(),
            }));
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct EncodedHotPointKeyRanges {
    primary: BufferRange,
}

struct EncodedHotPointKeys {
    bytes: Bytes,
    ranges: Vec<EncodedHotPointKeyRanges>,
}

impl EncodedHotPointKeys {
    fn primary_key(&self, index: usize) -> StorageKey {
        self.key_for_range(self.ranges[index].primary)
    }

    fn primary_key_bytes(&self, index: usize) -> &[u8] {
        let range = self.ranges[index].primary;
        &self.bytes[range.offset()..range.offset() + range.len()]
    }

    fn key_for_range(&self, range: BufferRange) -> StorageKey {
        let start = range.offset();
        StorageKey(self.bytes.slice(start..start + range.len()))
    }
}

fn encode_hot_point_keys(
    branch_id: &str,
    generation: CommitId,
    keys: &[TrackedStateKeyRef<'_>],
) -> EncodedHotPointKeys {
    encode_hot_point_keys_with(branch_id, generation, keys.len(), |index| keys[index])
}

fn encode_hot_point_keys_with<'a>(
    branch_id: &str,
    generation: CommitId,
    key_count: usize,
    mut key_at: impl FnMut(usize) -> TrackedStateKeyRef<'a>,
) -> EncodedHotPointKeys {
    let scope = hot_scope_prefix(branch_id, generation);
    let planned_capacity = (0..key_count).try_fold(0_usize, |total, index| {
        let key = key_at(index);
        let primary_len =
            encoded_hot_identity_key_len(scope.len(), key.schema_key, key.entity_pk, key.file_id)?;
        total.checked_add(primary_len)
    });
    let capacity = planned_capacity.unwrap_or(0);
    let mut bytes = Vec::with_capacity(capacity);
    let mut ranges = Vec::with_capacity(key_count);
    for index in 0..key_count {
        let key = key_at(index);
        let primary_start = bytes.len();
        bytes.extend_from_slice(&scope);
        write_key_string(&mut bytes, key.schema_key, KEY_PART_FINAL);
        write_file_id(&mut bytes, key.file_id);
        write_entity_pk(&mut bytes, key.entity_pk);
        let primary = BufferRange::new(primary_start, bytes.len() - primary_start);

        ranges.push(EncodedHotPointKeyRanges { primary });
    }
    debug_assert!(planned_capacity.is_none() || bytes.len() == capacity);
    EncodedHotPointKeys {
        bytes: Bytes::from(bytes),
        ranges,
    }
}

#[derive(Clone, Copy)]
struct FiniteHotIdentityRef<'a> {
    entity_pk: &'a EntityPk,
    file_id: Option<&'a str>,
}

/// One exact schema partition whose invariant identity components are retained
/// once for the entire point-read batch.
///
/// Entity and file descriptors borrow the caller's filter. Physical primary
/// keys share one immutable arena, so a dense-range probe can reuse the exact
/// same ranges before falling back to MultiGet.
struct FiniteHotIdentityBatchRef<'a> {
    branch_id: &'a str,
    generation: CommitId,
    schema_key: &'a str,
    identities: Vec<FiniteHotIdentityRef<'a>>,
    encoded: EncodedHotPointKeys,
}

impl<'a> FiniteHotIdentityBatchRef<'a> {
    fn new(
        branch_id: &'a str,
        generation: CommitId,
        schema_key: &'a str,
        mut entity_pks: Vec<&'a EntityPk>,
        mut file_ids: Vec<Option<&'a str>>,
    ) -> Option<Self> {
        entity_pks.sort_unstable();
        entity_pks.dedup();
        file_ids.sort_unstable();
        file_ids.dedup();
        let identity_count = entity_pks.len().checked_mul(file_ids.len())?;
        let mut identities = Vec::with_capacity(identity_count);
        for entity_pk in entity_pks {
            for &file_id in &file_ids {
                identities.push(FiniteHotIdentityRef { entity_pk, file_id });
            }
        }
        let encoded =
            encode_hot_point_keys_with(branch_id, generation, identities.len(), |index| {
                TrackedStateKeyRef {
                    schema_key,
                    entity_pk: identities[index].entity_pk,
                    file_id: identities[index].file_id,
                }
            });
        Some(Self {
            branch_id,
            generation,
            schema_key,
            identities,
            encoded,
        })
    }

    fn len(&self) -> usize {
        self.identities.len()
    }

    fn key_ref(&self, index: usize) -> TrackedStateKeyRef<'a> {
        let identity = self.identities[index];
        TrackedStateKeyRef {
            schema_key: self.schema_key,
            entity_pk: identity.entity_pk,
            file_id: identity.file_id,
        }
    }
}

struct FiniteHotEntryBatchRef<'a> {
    identities: FiniteHotIdentityBatchRef<'a>,
    values: Vec<Option<Bytes>>,
}

/// Identity decoded from a storage scan without constructing row-owned
/// `String` buffers for key metadata.
///
/// The immutable storage key is retained once. Schema and file ids normally
/// remain compact ranges into that buffer; only an escaped-NUL key part uses
/// the owned fallback. String and byte entity-PK components retain `Bytes`
/// slices of the same key allocation.
#[derive(Debug)]
struct HotScanIdentity {
    key: Bytes,
    schema_key: HotScanString,
    entity_pk: EntityPk,
    file_id: Option<HotScanString>,
}

#[derive(Debug)]
enum HotScanString {
    Borrowed(Range<u32>),
    Owned(String),
}

impl HotScanString {
    fn as_str<'a>(&'a self, key: &'a Bytes) -> &'a str {
        match self {
            Self::Borrowed(range) => {
                let range = range.start as usize..range.end as usize;
                // SAFETY: `read_hot_scan_key_string` validates this exact
                // range as UTF-8 before constructing the descriptor.
                unsafe { std::str::from_utf8_unchecked(&key[range]) }
            }
            Self::Owned(value) => value,
        }
    }

    fn into_shared_str(self, key: &Bytes) -> SharedStr {
        match self {
            Self::Borrowed(range) => {
                let range = range.start as usize..range.end as usize;
                let value = {
                    // SAFETY: the decoder validated this exact range.
                    unsafe { std::str::from_utf8_unchecked(&key[range]) }
                };
                SharedStr::from_utf8_slice(key.clone(), value)
                    .expect("decoded key string remains inside its retained key")
            }
            Self::Owned(value) => SharedStr::from(value),
        }
    }

    fn into_string(self, key: &Bytes) -> String {
        match self {
            Self::Borrowed(range) => {
                let range = range.start as usize..range.end as usize;
                // SAFETY: the decoder validated this exact range.
                unsafe { std::str::from_utf8_unchecked(&key[range]) }.to_owned()
            }
            Self::Owned(value) => value,
        }
    }

    #[cfg(test)]
    fn owns_fallback_buffer(&self) -> bool {
        matches!(self, Self::Owned(_))
    }
}

impl HotScanIdentity {
    fn schema_key(&self) -> &str {
        self.schema_key.as_str(&self.key)
    }

    fn file_id(&self) -> Option<&str> {
        self.file_id
            .as_ref()
            .map(|file_id| file_id.as_str(&self.key))
    }

    fn matches_filter(&self, filter: &TrackedStateFilter) -> bool {
        (filter.schema_keys.is_empty()
            || filter
                .schema_keys
                .iter()
                .any(|schema_key| schema_key == self.schema_key()))
            && (filter.entity_pks.is_empty() || filter.entity_pks.contains(&self.entity_pk))
            && (filter.file_ids.is_empty()
                || filter.file_ids.iter().any(|filter| match filter {
                    NullableKeyFilter::Any => true,
                    NullableKeyFilter::Null => self.file_id().is_none(),
                    NullableKeyFilter::Value(value) => self.file_id() == Some(value.as_str()),
                }))
    }

    fn into_row_identity(self) -> HeadRowIdentity {
        let Self {
            key,
            schema_key,
            entity_pk,
            file_id,
        } = self;
        HeadRowIdentity {
            schema_key: schema_key.into_string(&key),
            entity_pk,
            file_id: file_id.map(|file_id| file_id.into_string(&key)),
        }
    }

    #[cfg(test)]
    fn owned_metadata_buffer_count(&self) -> usize {
        usize::from(self.schema_key.owns_fallback_buffer())
            + usize::from(
                self.file_id
                    .as_ref()
                    .is_some_and(HotScanString::owns_fallback_buffer),
            )
    }
}

impl PartialEq for HotScanIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.schema_key() == other.schema_key()
            && self.entity_pk == other.entity_pk
            && self.file_id() == other.file_id()
    }
}

impl Eq for HotScanIdentity {}

impl PartialOrd for HotScanIdentity {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HotScanIdentity {
    fn cmp(&self, other: &Self) -> Ordering {
        self.schema_key()
            .cmp(other.schema_key())
            .then_with(|| self.entity_pk.cmp(&other.entity_pk))
            .then_with(|| self.file_id().cmp(&other.file_id()))
    }
}

impl LiveMaterializationIdentity for HotScanIdentity {
    fn push_materialized(
        self,
        rows: &mut MaterializedLiveStateBatchBuilder,
        snapshot_content: Option<SharedStr>,
        metadata: Option<SharedStr>,
        deleted: bool,
        created_at: LixTimestamp,
        updated_at: LixTimestamp,
        global: bool,
        change_id: Option<ChangeId>,
        commit_id: Option<CommitId>,
        untracked: bool,
        branch_id: &str,
    ) {
        rows.push_materialized_ref(
            &self.entity_pk,
            self.schema_key(),
            self.file_id(),
            snapshot_content,
            metadata,
            deleted,
            created_at,
            updated_at,
            global,
            change_id,
            commit_id,
            untracked,
            branch_id,
        );
    }
}

enum HotScanEntries<'a> {
    Finite(Vec<FiniteHotEntryBatchRef<'a>>),
    Decoded(Vec<(HotScanIdentity, Bytes)>),
}

fn hot_exact_identity_batches<'a>(
    branch_id: &'a str,
    generation: CommitId,
    filter: &'a TrackedStateFilter,
) -> Option<Vec<FiniteHotIdentityBatchRef<'a>>> {
    if filter.schema_keys.is_empty() || filter.entity_pks.is_empty() {
        return None;
    }
    let mut schema_keys = filter
        .schema_keys
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    schema_keys.sort_unstable();
    schema_keys.dedup();
    let entity_pks = filter.entity_pks.iter().collect::<Vec<_>>();
    let file_ids = if filter.file_ids.is_empty() {
        vec![None]
    } else {
        filter
            .file_ids
            .iter()
            .map(|file_id| match file_id {
                NullableKeyFilter::Null => Some(None),
                NullableKeyFilter::Value(value) => Some(Some(value.as_str())),
                NullableKeyFilter::Any => None,
            })
            .collect::<Option<Vec<_>>>()?
    };
    schema_keys
        .into_iter()
        .map(|schema_key| {
            FiniteHotIdentityBatchRef::new(
                branch_id,
                generation,
                schema_key,
                entity_pks.clone(),
                file_ids.clone(),
            )
        })
        .collect()
}

async fn hot_load_finite_identity_bytes(
    store: &(impl StorageAdapterRead + ?Sized),
    batch: &FiniteHotIdentityBatchRef<'_>,
) -> Result<Vec<Option<Bytes>>, LixError> {
    if batch.identities.is_empty() {
        return Ok(Vec::new());
    }
    let keys = (0..batch.len())
        .map(|index| batch.encoded.primary_key(index))
        .collect::<Vec<_>>();
    PointReadPlan::new(HOT_ROW_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?
        .value
        .into_iter()
        .map(|value| value.map(full_value_bytes).transpose())
        .collect()
}

async fn hot_scan_finite_identity_batches<'a>(
    store: &(impl StorageAdapterRead + ?Sized),
    batches: Vec<FiniteHotIdentityBatchRef<'a>>,
    limit: Option<usize>,
) -> Result<Vec<FiniteHotEntryBatchRef<'a>>, LixError> {
    let expected_generation = batches.first().map(|batch| batch.generation);
    let mut remaining = limit.unwrap_or(usize::MAX);
    let mut entries = Vec::with_capacity(batches.len());
    for identities in batches {
        debug_assert_eq!(Some(identities.generation), expected_generation);
        if remaining == 0 {
            break;
        }
        let mut values = if limit.is_none()
            && let Some(values) = hot_scan_dense_identity_range(store, &identities).await?
        {
            values
        } else {
            hot_load_finite_identity_bytes(store, &identities).await?
        };
        if limit.is_some() {
            for value in &mut values {
                if value.is_none() {
                    continue;
                }
                if remaining == 0 {
                    *value = None;
                } else {
                    remaining -= 1;
                }
            }
        }
        entries.push(FiniteHotEntryBatchRef { identities, values });
    }
    Ok(entries)
}

async fn materialize_hot_scan_entries(
    store: &(impl StorageAdapterRead + ?Sized),
    entries: HotScanEntries<'_>,
    projection: ChangeRecordProjection,
    branch_id: &str,
) -> Result<MaterializedLiveStateBatch, LixError> {
    match entries {
        HotScanEntries::Decoded(entries) => {
            materialize_live_entries(store, entries, projection, branch_id).await
        }
        HotScanEntries::Finite(batches) => {
            let row_count = batches
                .iter()
                .map(|batch| batch.values.iter().flatten().count())
                .sum();
            let mut entries = Vec::with_capacity(row_count);
            for batch in batches {
                debug_assert_eq!(batch.identities.branch_id, branch_id);
                for (index, value) in batch.values.into_iter().enumerate() {
                    let Some(value) = value else {
                        continue;
                    };
                    entries.push((batch.identities.key_ref(index), value));
                }
            }
            materialize_live_entries(store, entries, projection, branch_id).await
        }
    }
}

fn hot_scan_values_in_logical_order(entries: HotScanEntries<'_>) -> Vec<Bytes> {
    match entries {
        HotScanEntries::Decoded(mut entries) => {
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            entries.into_iter().map(|(_, value)| value).collect()
        }
        HotScanEntries::Finite(batches) => batches
            .into_iter()
            .flat_map(|batch| batch.values.into_iter().flatten())
            .collect(),
    }
}

async fn hot_load_identity_ref_bytes(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    identities: &[TrackedStateKeyRef<'_>],
) -> Result<Vec<Option<Bytes>>, LixError> {
    if identities.is_empty() {
        return Ok(Vec::new());
    }
    let encoded = encode_hot_point_keys(branch_id, generation, identities);
    let keys = (0..identities.len())
        .map(|index| encoded.primary_key(index))
        .collect::<Vec<_>>();
    PointReadPlan::new(HOT_ROW_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?
        .value
        .into_iter()
        .map(|value| value.map(full_value_bytes).transpose())
        .collect()
}

/// Loads the authoritative primary row for every identity.
async fn hot_load_primary_identity_bytes(
    store: &(impl StorageAdapterRead + ?Sized),
    identities: &[HeadIdentity],
) -> Result<Vec<Option<Bytes>>, LixError> {
    if identities.is_empty() {
        return Ok(Vec::new());
    }
    let scope = &identities[0];
    debug_assert!(identities.iter().all(|identity| {
        identity.branch_id == scope.branch_id && identity.generation == scope.generation
    }));
    let identities = identities
        .iter()
        .map(|identity| TrackedStateKeyRef {
            schema_key: identity.schema_key.as_str(),
            file_id: identity.file_id.as_deref(),
            entity_pk: &identity.entity_pk,
        })
        .collect::<Vec<_>>();
    let encoded = encode_hot_point_keys(scope.branch_id.as_str(), scope.generation, &identities);
    let keys = (0..identities.len())
        .map(|index| encoded.primary_key(index))
        .collect::<Vec<_>>();
    PointReadPlan::new(HOT_ROW_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?
        .value
        .into_iter()
        .map(|value| value.map(full_value_bytes).transpose())
        .collect()
}

async fn hot_load_file_scope_identities(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    cascades: &BTreeMap<String, &CurrentStateDeltaRef<'_>>,
) -> Result<Vec<HeadIdentity>, LixError> {
    let scope = hot_scope_prefix(branch_id, generation);
    let plan = ScanPlan::prefix(
        HOT_ROW_SPACE,
        StoragePrefix {
            bytes: Bytes::from(scope.clone()),
        },
    );
    let mut identities = Vec::new();
    let mut resume_after = None;
    loop {
        let page = plan
            .collect(
                store,
                StorageScanOptions {
                    projection: StorageCoreProjection::KeyOnly,
                    resume_after: resume_after.clone(),
                    ..StorageScanOptions::default()
                },
            )
            .await?;
        resume_after = page.value.entries.last().map(|entry| entry.key.clone());
        for entry in page.value.entries {
            let row = decode_hot_row_key_in_scope(entry.key.0.as_ref(), &scope)?;
            if !row
                .file_id
                .as_ref()
                .is_some_and(|file_id| cascades.contains_key(file_id))
            {
                continue;
            }
            identities.push(HeadIdentity {
                branch_id: branch_id.to_string(),
                generation,
                schema_key: row.schema_key,
                entity_pk: row.entity_pk,
                file_id: row.file_id,
            });
        }
        if !page.value.has_more || resume_after.is_none() {
            break;
        }
    }
    Ok(identities)
}

fn working_diff_baseline_before(
    baseline: WorkingDiffBaseline,
) -> Option<Option<WorkingDiffVersion>> {
    match baseline {
        WorkingDiffBaseline::BeforeAbsent => Some(None),
        WorkingDiffBaseline::BeforePresent(version) => Some(Some(version)),
        WorkingDiffBaseline::Disabled | WorkingDiffBaseline::Clean => None,
    }
}

/// Resolves a checkpoint diff from row-local first-before images. Broad diffs
/// enumerate the sparse dirty-key index; finite PK queries read only the
/// primary rows that can answer the request.
async fn hot_working_diff_entries(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    checkpoint_commit_id: CommitId,
    generation: CommitId,
    expected_coverage: WorkingDiffIndexCoverage,
    filter: &TrackedStateFilter,
) -> Result<Option<Vec<TrackedStateDiffEntry>>, LixError> {
    if !filter.schema_keys.is_empty() && !filter.entity_pks.is_empty() {
        return hot_working_diff_entries_for_finite_filter(store, branch_id, generation, filter)
            .await;
    }

    let scope = encode_working_diff_scope_prefix(branch_id, checkpoint_commit_id, generation);
    let plan = ScanPlan::prefix(
        HOT_DIFF_SPACE,
        StoragePrefix {
            bytes: Bytes::from(scope.clone()),
        },
    );
    let mut actual_coverage = WorkingDiffIndexCoverage::default();
    let mut selected = Vec::<HeadIdentity>::new();
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
            let Ok(bytes) = full_value_bytes(entry.value) else {
                return Ok(None);
            };
            if bytes.is_empty() {
                if actual_coverage
                    .add_encoded_group_key(entry.key.0.as_ref())
                    .is_none()
                {
                    return Ok(None);
                }
                let Ok(identity) = decode_hot_diff_key_in_scope(entry.key.0.as_ref(), &scope)
                else {
                    return Ok(None);
                };
                if matches_filter(&identity, filter) {
                    selected.push(HeadIdentity {
                        branch_id: branch_id.to_string(),
                        generation,
                        schema_key: identity.schema_key,
                        entity_pk: identity.entity_pk,
                        file_id: identity.file_id,
                    });
                }
                continue;
            }
            let Ok(segment_scope) = decode_hot_diff_segment_key(entry.key.0.as_ref()) else {
                return Ok(None);
            };
            if segment_scope.digest != *blake3::hash(&bytes).as_bytes() {
                return Ok(None);
            }
            let decoded =
                visit_hot_diff_segment(&bytes, &scope, &mut actual_coverage, |identity| {
                    if matches_filter(&identity, filter) {
                        selected.push(HeadIdentity {
                            branch_id: branch_id.to_string(),
                            generation,
                            schema_key: identity.schema_key,
                            entity_pk: identity.entity_pk,
                            file_id: identity.file_id,
                        });
                    }
                });
            if decoded.is_err() {
                return Ok(None);
            }
        }
        if !page.value.has_more || resume_after.is_none() {
            break;
        }
    }
    if actual_coverage != expected_coverage {
        return Ok(None);
    }
    let after_values = hot_load_primary_identity_bytes(store, &selected).await?;
    let mut candidates = Vec::with_capacity(selected.len());
    for (identity, after) in selected.into_iter().zip(after_values) {
        let Some(after) = after else {
            return Ok(None);
        };
        let Ok(after) = decode_head_value(&after) else {
            return Ok(None);
        };
        if after.untracked {
            return Ok(None);
        }
        let Some(before) = working_diff_baseline_before(after.working_diff_baseline) else {
            return Ok(None);
        };
        let Some(after) = after.working_diff_version() else {
            return Ok(None);
        };
        let identity = identity.into_row_identity();
        candidates.push((
            TrackedStateKey {
                schema_key: identity.schema_key,
                entity_pk: identity.entity_pk,
                file_id: identity.file_id,
            },
            before,
            after,
        ));
    }
    Ok(Some(classify_hot_working_diff_entries(candidates)?))
}

async fn hot_working_diff_entries_for_finite_filter(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    filter: &TrackedStateFilter,
) -> Result<Option<Vec<TrackedStateDiffEntry>>, LixError> {
    let rows = hot_scan_entries(store, branch_id, generation, filter, None).await?;
    match rows {
        HotScanEntries::Decoded(rows) => {
            let mut candidates = Vec::with_capacity(rows.len());
            for (identity, bytes) in rows {
                let Some(versions) = finite_working_diff_versions(&bytes) else {
                    return Ok(None);
                };
                let Some((before, after)) = versions else {
                    continue;
                };
                candidates.push((identity, before, after));
            }
            Ok(Some(classify_hot_working_diff_scan_entries(candidates)?))
        }
        HotScanEntries::Finite(batches) => {
            let row_count = batches
                .iter()
                .map(|batch| batch.values.iter().flatten().count())
                .sum();
            let mut candidates = Vec::with_capacity(row_count);
            for batch in batches {
                for (index, bytes) in batch.values.into_iter().enumerate() {
                    let Some(bytes) = bytes else {
                        continue;
                    };
                    let Some(versions) = finite_working_diff_versions(&bytes) else {
                        return Ok(None);
                    };
                    let Some((before, after)) = versions else {
                        continue;
                    };
                    candidates.push((batch.identities.key_ref(index), before, after));
                }
            }
            Ok(Some(classify_hot_working_diff_entry_refs(candidates)?))
        }
    }
}

fn finite_working_diff_versions(
    bytes: &Bytes,
) -> Option<Option<(Option<WorkingDiffVersion>, WorkingDiffVersion)>> {
    let after = decode_head_value(bytes).ok()?;
    if after.untracked || after.working_diff_baseline == WorkingDiffBaseline::Clean {
        return Some(None);
    }
    let before = working_diff_baseline_before(after.working_diff_baseline)?;
    let after = after.working_diff_version()?;
    Some(Some((before, after)))
}

fn classify_hot_working_diff_entries(
    candidates: Vec<(
        TrackedStateKey,
        Option<WorkingDiffVersion>,
        WorkingDiffVersion,
    )>,
) -> Result<Vec<TrackedStateDiffEntry>, LixError> {
    let row_count = candidates.len();
    let mut keys = Vec::with_capacity(row_count);
    let mut versions = Vec::with_capacity(row_count);
    for (key, before, after) in candidates {
        keys.push(key);
        versions.push((before, after));
    }
    let identities = TrackedStateDiffIdentity::from_key_batch(keys)?;
    let mut entries = Vec::with_capacity(row_count);
    for (identity, (before, after)) in identities.into_iter().zip(versions) {
        if let Some(entry) = classify_hot_working_diff_entry(identity, before, after) {
            entries.push(entry);
        }
    }
    Ok(entries)
}

fn classify_hot_working_diff_entry_refs(
    candidates: Vec<(
        TrackedStateKeyRef<'_>,
        Option<WorkingDiffVersion>,
        WorkingDiffVersion,
    )>,
) -> Result<Vec<TrackedStateDiffEntry>, LixError> {
    let row_count = candidates.len();
    let identities =
        TrackedStateDiffIdentity::from_key_refs(row_count, |index| candidates[index].0)?;
    let mut entries = Vec::with_capacity(row_count);
    for (identity, (_, before, after)) in identities.into_iter().zip(candidates) {
        if let Some(entry) = classify_hot_working_diff_entry(identity, before, after) {
            entries.push(entry);
        }
    }
    Ok(entries)
}

fn classify_hot_working_diff_scan_entries(
    candidates: Vec<(
        HotScanIdentity,
        Option<WorkingDiffVersion>,
        WorkingDiffVersion,
    )>,
) -> Result<Vec<TrackedStateDiffEntry>, LixError> {
    let row_count = candidates.len();
    let identities = TrackedStateDiffIdentity::from_key_refs(row_count, |index| {
        let identity = &candidates[index].0;
        TrackedStateKeyRef {
            schema_key: identity.schema_key(),
            file_id: identity.file_id(),
            entity_pk: &identity.entity_pk,
        }
    })?;
    let mut entries = Vec::with_capacity(row_count);
    for (identity, (_, before, after)) in identities.into_iter().zip(candidates) {
        if let Some(entry) = classify_hot_working_diff_entry(identity, before, after) {
            entries.push(entry);
        }
    }
    Ok(entries)
}

fn classify_hot_working_diff_entry(
    diff_identity: TrackedStateDiffIdentity,
    before: Option<WorkingDiffVersion>,
    after: WorkingDiffVersion,
) -> Option<TrackedStateDiffEntry> {
    let before_row = before.map(|version| version.into_diff_row(diff_identity.clone()));
    let after_row = after.into_diff_row(diff_identity.clone());
    match (
        before_row.as_ref().filter(|row| !row.deleted),
        (!after_row.deleted).then_some(&after_row),
    ) {
        (None, None) => None,
        (None, Some(_)) => Some(TrackedStateDiffEntry {
            identity: diff_identity,
            kind: TrackedStateDiffKind::Added,
            before: before_row,
            after: Some(after_row),
        }),
        (Some(_), None) => Some(TrackedStateDiffEntry {
            identity: diff_identity,
            kind: TrackedStateDiffKind::Removed,
            before: before_row,
            after: Some(after_row),
        }),
        (Some(_), Some(_)) if before.is_some_and(|version| version.payload_eq(after)) => None,
        (Some(_), Some(_)) => Some(TrackedStateDiffEntry {
            identity: diff_identity,
            kind: TrackedStateDiffKind::Modified,
            before: before_row,
            after: Some(after_row),
        }),
    }
}

async fn hot_scan_entries<'a>(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &'a str,
    generation: CommitId,
    filter: &'a TrackedStateFilter,
    limit: Option<usize>,
) -> Result<HotScanEntries<'a>, LixError> {
    // The null-file member is a true point key. A logical-PK scan can use a
    // single MultiGet only when this schema has no file-backed members; if it
    // does, fall through to the complete primary-prefix route so UPDATE and
    // DELETE still see every candidate member.
    if let Some(identities) = hot_exact_identity_batches(branch_id, generation, filter) {
        let may_use_null_point_batch = !filter.file_ids.is_empty()
            || !hot_schema_has_file_members(store, branch_id, generation, &filter.schema_keys)
                .await?;
        if may_use_null_point_batch {
            return hot_scan_finite_identity_batches(store, identities, limit)
                .await
                .map(HotScanEntries::Finite);
        }
    }

    // The authoritative hot index is file-first, so filesystem queries such as
    // `WHERE file_id = ?` read one contiguous hydrated range without a second
    // value projection or random point-read hydration.
    if let Some(prefixes) = hot_file_scan_prefixes(branch_id, generation, filter) {
        return scan_hot_file_entries(store, branch_id, generation, prefixes, filter, limit)
            .await
            .map(HotScanEntries::Decoded);
    }

    let scope = hot_scope_prefix(branch_id, generation);
    let mut prefixes = hot_row_scan_prefixes(&scope, filter);
    prefixes.sort();
    prefixes.dedup();
    let mut rows = Vec::new();
    for prefix in prefixes {
        let plan = ScanPlan::prefix(
            HOT_ROW_SPACE,
            StoragePrefix {
                bytes: Bytes::from(prefix),
            },
        );
        let mut resume_after = None;
        loop {
            let remaining = limit.map(|limit| limit.saturating_sub(rows.len()));
            if matches!(remaining, Some(0)) {
                return Ok(HotScanEntries::Decoded(rows));
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
                let identity = decode_hot_scan_row_key_in_scope(entry.key.0, &scope)?;
                if identity.matches_filter(filter) {
                    rows.push((identity, full_value_bytes(entry.value)?));
                    if limit.is_some_and(|limit| rows.len() >= limit) {
                        return Ok(HotScanEntries::Decoded(rows));
                    }
                }
            }
            if !page.value.has_more || resume_after.is_none() {
                break;
            }
        }
    }
    Ok(HotScanEntries::Decoded(rows))
}

async fn hot_scan_dense_identity_range(
    store: &(impl StorageAdapterRead + ?Sized),
    identities: &FiniteHotIdentityBatchRef<'_>,
) -> Result<Option<Vec<Option<Bytes>>>, LixError> {
    hot_scan_dense_encoded_key_range(store, identities.len(), |index| {
        identities.encoded.primary_key_bytes(index)
    })
    .await
}

async fn hot_scan_dense_encoded_key_range<'a>(
    store: &(impl StorageAdapterRead + ?Sized),
    key_count: usize,
    key_at: impl Fn(usize) -> &'a [u8],
) -> Result<Option<Vec<Option<Bytes>>>, LixError> {
    if key_count < HOT_DENSE_SCAN_MIN_IDENTITIES {
        return Ok(None);
    }
    if key_count == 0 {
        return Ok(Some(Vec::new()));
    }
    if (1..key_count).any(|index| key_at(index - 1) > key_at(index)) {
        return Ok(None);
    }
    let first_key = StorageKey(Bytes::copy_from_slice(key_at(0)));
    let last_key = StorageKey(Bytes::copy_from_slice(key_at(key_count - 1)));
    let plan = ScanPlan::range(
        HOT_ROW_SPACE,
        crate::storage_adapter::StorageKeyRange {
            lower: std::ops::Bound::Included(first_key),
            upper: std::ops::Bound::Included(last_key),
        },
    );
    let scan_budget = key_count.saturating_mul(HOT_DENSE_SCAN_MAX_OVERREAD);
    let mut scanned = 0;
    let mut requested_index = 0;
    let mut resume_after = None;
    let mut values = vec![None; key_count];
    loop {
        let remaining_budget = scan_budget.saturating_sub(scanned);
        if remaining_budget == 0 {
            return Ok(None);
        }
        let page = plan
            .collect(
                store,
                StorageScanOptions {
                    resume_after: resume_after.clone(),
                    limit_rows: remaining_budget.min(StorageScanOptions::default().limit_rows),
                    ..StorageScanOptions::default()
                },
            )
            .await?;
        resume_after = page.value.entries.last().map(|entry| entry.key.clone());
        scanned += page.value.entries.len();
        for entry in page.value.entries {
            while requested_index < key_count && key_at(requested_index) < entry.key.0.as_ref() {
                requested_index += 1;
            }
            if requested_index < key_count && key_at(requested_index) == entry.key.0.as_ref() {
                values[requested_index] = Some(full_value_bytes(entry.value)?);
                requested_index += 1;
            }
        }
        if requested_index == key_count || !page.value.has_more || resume_after.is_none() {
            return Ok(Some(values));
        }
    }
}

fn hot_row_scan_prefixes(scope: &[u8], filter: &TrackedStateFilter) -> Vec<Vec<u8>> {
    if filter.schema_keys.is_empty() {
        return vec![scope.to_vec()];
    }
    filter
        .schema_keys
        .iter()
        .map(|schema_key| {
            let mut prefix = scope.to_vec();
            write_key_string(&mut prefix, schema_key, KEY_PART_FINAL);
            prefix
        })
        .collect()
}

fn hot_file_scan_prefixes(
    branch_id: &str,
    generation: CommitId,
    filter: &TrackedStateFilter,
) -> Option<Vec<Vec<u8>>> {
    if filter.schema_keys.is_empty()
        || filter.file_ids.is_empty()
        || !filter.entity_pks.is_empty()
        || filter
            .file_ids
            .iter()
            .any(|file_id| !matches!(file_id, NullableKeyFilter::Value(_)))
    {
        return None;
    }
    let mut prefixes = Vec::with_capacity(filter.schema_keys.len() * filter.file_ids.len());
    for schema_key in &filter.schema_keys {
        for file_id in &filter.file_ids {
            let NullableKeyFilter::Value(file_id) = file_id else {
                unreachable!("file-id projection predicate was checked above");
            };
            let mut prefix = hot_scope_prefix(branch_id, generation);
            write_key_string(&mut prefix, schema_key, KEY_PART_FINAL);
            write_file_id(&mut prefix, Some(file_id));
            prefixes.push(prefix);
        }
    }
    prefixes.sort();
    prefixes.dedup();
    Some(prefixes)
}

async fn scan_hot_file_entries(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    prefixes: Vec<Vec<u8>>,
    filter: &TrackedStateFilter,
    limit: Option<usize>,
) -> Result<Vec<(HotScanIdentity, Bytes)>, LixError> {
    let scope = hot_scope_prefix(branch_id, generation);
    let mut rows = Vec::new();
    for prefix in prefixes {
        let plan = ScanPlan::prefix(
            HOT_ROW_SPACE,
            StoragePrefix {
                bytes: Bytes::from(prefix),
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
                let identity = decode_hot_scan_row_key_in_scope(entry.key.0, &scope)?;
                if identity.matches_filter(filter) {
                    rows.push((identity, full_value_bytes(entry.value)?));
                }
            }
            if !page.value.has_more || resume_after.is_none() {
                break;
            }
        }
    }
    // Physical rows are ordered `(schema, file_id, entity_pk)`, while SQL rows
    // are ordered `(schema, entity_pk, file_id)`. Restore the public order
    // after multi-file scans and defend against repeated predicates.
    rows.sort_by(|left, right| left.0.cmp(&right.0));
    rows.dedup_by(|left, right| left.0 == right.0);
    if let Some(limit) = limit {
        rows.truncate(limit);
    }
    Ok(rows)
}

async fn hot_schema_has_file_members(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    schema_keys: &[String],
) -> Result<bool, LixError> {
    // Exact identity batches always name their schema. Broad scans deliberately
    // take the primary-prefix route, which already sees all file members.
    if schema_keys.is_empty() {
        return Ok(true);
    }
    for schema_key in schema_keys {
        if hot_schema_has_file_member(store, branch_id, generation, schema_key).await? {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn hot_schema_has_file_member(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    schema_key: &str,
) -> Result<bool, LixError> {
    let scope = hot_scope_prefix(branch_id, generation);
    let key = StorageKey(Bytes::from(encode_hot_file_schema_key(&scope, schema_key)));
    let values = PointReadPlan::new(HOT_FILE_SPACE, &[key])
        .materialize(
            store,
            StorageGetOptions {
                projection: StorageCoreProjection::KeyOnly,
            },
        )
        .await?;
    Ok(values.value.into_iter().next().flatten().is_some())
}

fn hot_scope_prefix(branch_id: &str, generation: CommitId) -> Vec<u8> {
    encode_scope_prefix(branch_id, generation)
}

#[cfg(test)]
fn encode_hot_row_key(identity: &HeadIdentity) -> Vec<u8> {
    encode_hot_row_key_parts(
        &identity.branch_id,
        identity.generation,
        &identity.schema_key,
        &identity.entity_pk,
        identity.file_id.as_deref(),
    )
}

#[cfg(test)]
fn encode_hot_row_key_parts(
    branch_id: &str,
    generation: CommitId,
    schema_key: &str,
    entity_pk: &EntityPk,
    file_id: Option<&str>,
) -> Vec<u8> {
    let mut key = hot_scope_prefix(branch_id, generation);
    write_key_string(&mut key, schema_key, KEY_PART_FINAL);
    write_file_id(&mut key, file_id);
    write_entity_pk(&mut key, entity_pk);
    key
}

#[cfg(test)]
fn encode_hot_diff_key(checkpoint_commit_id: CommitId, identity: &HeadIdentity) -> Vec<u8> {
    encode_hot_diff_key_parts(
        &identity.branch_id,
        checkpoint_commit_id,
        identity.generation,
        &identity.schema_key,
        &identity.entity_pk,
        identity.file_id.as_deref(),
    )
}

#[cfg(test)]
fn encode_hot_diff_key_parts(
    branch_id: &str,
    checkpoint_commit_id: CommitId,
    generation: CommitId,
    schema_key: &str,
    entity_pk: &EntityPk,
    file_id: Option<&str>,
) -> Vec<u8> {
    let scope = encode_working_diff_scope_prefix(branch_id, checkpoint_commit_id, generation);
    let mut key = Vec::with_capacity(
        encoded_hot_identity_key_len(scope.len(), schema_key, entity_pk, file_id).unwrap_or(0),
    );
    append_hot_diff_key_parts(&mut key, &scope, schema_key, entity_pk, file_id);
    key
}

fn append_hot_diff_key_parts(
    key_bytes: &mut Vec<u8>,
    scope: &[u8],
    schema_key: &str,
    entity_pk: &EntityPk,
    file_id: Option<&str>,
) -> Range<usize> {
    let start = key_bytes.len();
    key_bytes.extend_from_slice(scope);
    write_key_string(key_bytes, schema_key, KEY_PART_FINAL);
    write_entity_pk(key_bytes, entity_pk);
    write_file_id(key_bytes, file_id);
    start..key_bytes.len()
}

fn encoded_hot_identity_key_len(
    scope_len: usize,
    schema_key: &str,
    entity_pk: &EntityPk,
    file_id: Option<&str>,
) -> Option<usize> {
    let file_id_len = match file_id {
        Some(file_id) => encoded_key_bytes_len(file_id.as_bytes())?,
        None => 0,
    };
    scope_len
        .checked_add(encoded_key_bytes_len(schema_key.as_bytes())?)?
        .checked_add(encoded_entity_pk_len(entity_pk)?)?
        .checked_add(1)?
        .checked_add(file_id_len)
}

fn encoded_entity_pk_len(entity_pk: &EntityPk) -> Option<usize> {
    entity_pk
        .components
        .iter()
        .try_fold(1_usize, |total, component| {
            let payload_len = match component {
                crate::entity_pk::EntityPkComponent::Uuid(_) => Some(16 + 1),
                crate::entity_pk::EntityPkComponent::Integer(_) => Some(8 + 1),
                crate::entity_pk::EntityPkComponent::String(value) => {
                    encoded_key_bytes_len(value.as_bytes())
                }
                crate::entity_pk::EntityPkComponent::Bytes(value) => {
                    encoded_key_bytes_len(value.as_ref())
                }
            }?;
            total.checked_add(1)?.checked_add(payload_len)
        })
}

fn encoded_key_bytes_len(value: &[u8]) -> Option<usize> {
    value
        .len()
        .checked_add(memchr::memchr_iter(KEY_PART_FINAL, value).count())?
        .checked_add(2)
}

fn decode_hot_scan_row_key_in_scope(key: Bytes, scope: &[u8]) -> Result<HotScanIdentity, LixError> {
    if !key.starts_with(scope) {
        return Err(key_codec_error(
            "hot row does not begin with its scanned scope",
        ));
    }
    let mut offset = scope.len();
    let (schema_key, schema_terminator) =
        read_hot_scan_key_string(&key, &mut offset, "schema key")?;
    if schema_terminator != KEY_PART_FINAL {
        return Err(key_codec_error(
            "hot row schema key has an invalid terminator",
        ));
    }
    let file_id = read_hot_scan_file_id(&key, &mut offset)?;
    let entity_pk = read_hot_scan_entity_pk(&key, &mut offset)?;
    if offset != key.len() {
        return Err(key_codec_error("hot row key has trailing bytes"));
    }
    Ok(HotScanIdentity {
        key,
        schema_key,
        entity_pk,
        file_id,
    })
}

fn read_hot_scan_entity_pk(bytes: &Bytes, offset: &mut usize) -> Result<EntityPk, LixError> {
    let version = bytes
        .get(*offset)
        .copied()
        .ok_or_else(|| key_codec_error("is truncated before entity primary key version"))?;
    *offset += 1;
    if version != ENTITY_PK_CODEC_V1 {
        return Err(key_codec_error(&format!(
            "has unsupported entity primary key codec version {version}"
        )));
    }
    let mut components = SmallVec::new();
    loop {
        let (part, terminator) = read_hot_scan_entity_pk_part(bytes, offset)?;
        components.push(part);
        match terminator {
            KEY_PART_FINAL => break,
            KEY_PART_MORE => {}
            _ => {
                return Err(key_codec_error(
                    "entity primary key has an invalid terminator",
                ));
            }
        }
    }
    EntityPk::from_components(components).map_err(|error| {
        key_codec_error(&format!("contains an invalid entity primary key: {error}"))
    })
}

fn read_hot_scan_entity_pk_part(
    bytes: &Bytes,
    offset: &mut usize,
) -> Result<(crate::entity_pk::EntityPkComponent, u8), LixError> {
    let tag = bytes
        .get(*offset)
        .copied()
        .ok_or_else(|| key_codec_error("is truncated before entity primary key part tag"))?;
    *offset += 1;
    match tag {
        ENTITY_PK_STRING => {
            let (value, terminator) =
                read_hot_scan_key_string(bytes, offset, "entity primary key")?;
            Ok((
                crate::entity_pk::EntityPkComponent::String(value.into_shared_str(bytes)),
                terminator,
            ))
        }
        ENTITY_PK_BYTES => {
            let (value, terminator) =
                read_hot_scan_shared_bytes(bytes, offset, "entity primary key bytes")?;
            Ok((
                crate::entity_pk::EntityPkComponent::Bytes(value),
                terminator,
            ))
        }
        ENTITY_PK_UUID => {
            let uuid_end = offset
                .checked_add(16)
                .ok_or_else(|| key_codec_error("UUIDv7 entity primary key offset overflow"))?;
            let uuid_bytes: [u8; 16] = bytes
                .get(*offset..uuid_end)
                .ok_or_else(|| key_codec_error("is truncated in UUIDv7 entity primary key"))?
                .try_into()
                .expect("UUIDv7 slice has fixed length");
            let terminator = bytes
                .get(uuid_end)
                .copied()
                .ok_or_else(|| key_codec_error("is truncated after UUIDv7 entity primary key"))?;
            if !matches!(terminator, KEY_PART_FINAL | KEY_PART_MORE) {
                return Err(key_codec_error(
                    "UUIDv7 entity primary key has an invalid terminator",
                ));
            }
            *offset = uuid_end + 1;
            Ok((
                crate::entity_pk::EntityPkComponent::Uuid(uuid_bytes),
                terminator,
            ))
        }
        ENTITY_PK_INTEGER => {
            let integer_end = offset
                .checked_add(8)
                .ok_or_else(|| key_codec_error("integer entity primary key offset overflow"))?;
            let ordered = u64::from_be_bytes(
                bytes
                    .get(*offset..integer_end)
                    .ok_or_else(|| key_codec_error("is truncated in integer entity primary key"))?
                    .try_into()
                    .expect("integer slice has fixed length"),
            );
            let terminator = bytes
                .get(integer_end)
                .copied()
                .ok_or_else(|| key_codec_error("is truncated after integer entity primary key"))?;
            if !matches!(terminator, KEY_PART_FINAL | KEY_PART_MORE) {
                return Err(key_codec_error(
                    "integer entity primary key has an invalid terminator",
                ));
            }
            *offset = integer_end + 1;
            Ok((
                crate::entity_pk::EntityPkComponent::Integer(i64::from_be_bytes(
                    (ordered ^ (1_u64 << 63)).to_be_bytes(),
                )),
                terminator,
            ))
        }
        _ => Err(key_codec_error(
            "has an unknown entity primary key part tag",
        )),
    }
}

fn read_hot_scan_file_id(
    bytes: &Bytes,
    offset: &mut usize,
) -> Result<Option<HotScanString>, LixError> {
    let tag = *bytes
        .get(*offset)
        .ok_or_else(|| key_codec_error("is truncated before file id"))?;
    *offset += 1;
    match tag {
        FILE_ID_NONE => Ok(None),
        FILE_ID_SOME => {
            let (file_id, terminator) = read_hot_scan_key_string(bytes, offset, "file id")?;
            if terminator != KEY_PART_FINAL {
                return Err(key_codec_error("file id has an invalid terminator"));
            }
            Ok(Some(file_id))
        }
        _ => Err(key_codec_error("has an invalid file id tag")),
    }
}

fn read_hot_scan_key_string(
    bytes: &Bytes,
    offset: &mut usize,
    field: &str,
) -> Result<(HotScanString, u8), LixError> {
    let start = *offset;
    let mut cursor = start;
    loop {
        let byte = *bytes
            .get(cursor)
            .ok_or_else(|| key_codec_error(&format!("is truncated in {field}")))?;
        cursor += 1;
        if byte != KEY_PART_FINAL {
            continue;
        }
        let terminator = *bytes
            .get(cursor)
            .ok_or_else(|| key_codec_error(&format!("is truncated after {field}")))?;
        cursor += 1;
        if terminator != KEY_ESCAPE {
            let end = cursor - 2;
            std::str::from_utf8(&bytes[start..end])
                .map_err(|error| key_codec_error(&format!("{field} is not UTF-8: {error}")))?;
            let start = u32::try_from(start)
                .map_err(|_| key_codec_error(&format!("{field} offset exceeds u32")))?;
            let end = u32::try_from(end)
                .map_err(|_| key_codec_error(&format!("{field} offset exceeds u32")))?;
            *offset = cursor;
            return Ok((HotScanString::Borrowed(start..end), terminator));
        }
        break;
    }

    // Embedded NULs require unescaping. Preserve that uncommon case without
    // imposing an owned buffer on generated schema and file identifiers.
    *offset = start;
    read_key_string(bytes.as_ref(), offset, field)
        .map(|(value, terminator)| (HotScanString::Owned(value), terminator))
}

fn read_hot_scan_shared_bytes(
    bytes: &Bytes,
    offset: &mut usize,
    field: &str,
) -> Result<(Bytes, u8), LixError> {
    let start = *offset;
    let mut cursor = start;
    loop {
        let byte = *bytes
            .get(cursor)
            .ok_or_else(|| key_codec_error(&format!("is truncated in {field}")))?;
        cursor += 1;
        if byte != KEY_PART_FINAL {
            continue;
        }
        let terminator = *bytes
            .get(cursor)
            .ok_or_else(|| key_codec_error(&format!("is truncated after {field}")))?;
        cursor += 1;
        if terminator != KEY_ESCAPE {
            *offset = cursor;
            return Ok((bytes.slice(start..cursor - 2), terminator));
        }
        break;
    }

    *offset = start;
    read_key_bytes(bytes.as_ref(), offset, field)
        .map(|(value, terminator)| (Bytes::from(value), terminator))
}

fn decode_hot_row_key_in_scope(bytes: &[u8], scope: &[u8]) -> Result<HeadRowIdentity, LixError> {
    if !bytes.starts_with(scope) {
        return Err(key_codec_error(
            "hot row does not begin with its scanned scope",
        ));
    }
    let mut offset = scope.len();
    let (schema_key, schema_terminator) = read_key_string(bytes, &mut offset, "schema key")?;
    if schema_terminator != KEY_PART_FINAL {
        return Err(key_codec_error(
            "hot row schema key has an invalid terminator",
        ));
    }
    let file_id = read_file_id(bytes, &mut offset)?;
    let entity_pk = read_entity_pk(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(key_codec_error("hot row key has trailing bytes"));
    }
    Ok(HeadRowIdentity {
        schema_key,
        entity_pk,
        file_id,
    })
}

fn decode_hot_row_scope(bytes: &[u8]) -> Result<(String, CommitId), LixError> {
    let mut offset = 0;
    let (branch_id, branch_terminator) = read_key_string(bytes, &mut offset, "branch id")?;
    if branch_terminator != KEY_PART_FINAL {
        return Err(key_codec_error(
            "hot row branch id has an invalid terminator",
        ));
    }
    let generation = read_generation(bytes, &mut offset)?;
    let (schema_key, schema_terminator) = read_key_string(bytes, &mut offset, "schema key")?;
    if schema_terminator != KEY_PART_FINAL {
        return Err(key_codec_error(
            "hot row schema key has an invalid terminator",
        ));
    }
    let file_id = read_file_id(bytes, &mut offset)?;
    let entity_pk = read_entity_pk(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(key_codec_error("hot row key has trailing bytes"));
    }
    let _ = (schema_key, entity_pk, file_id);
    Ok((branch_id, generation))
}

fn encode_hot_file_schema_key(scope: &[u8], schema_key: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(
        scope
            .len()
            .saturating_add(encoded_key_bytes_len(schema_key.as_bytes()).unwrap_or(0)),
    );
    key.extend_from_slice(scope);
    write_key_string(&mut key, schema_key, KEY_PART_FINAL);
    key
}

fn decode_hot_file_scope(bytes: &[u8]) -> Result<(String, CommitId), LixError> {
    let mut offset = 0;
    let (branch_id, branch_terminator) = read_key_string(bytes, &mut offset, "branch id")?;
    if branch_terminator != KEY_PART_FINAL {
        return Err(key_codec_error(
            "hot file schema branch id has an invalid terminator",
        ));
    }
    let generation = read_generation(bytes, &mut offset)?;
    let (schema_key, schema_terminator) = read_key_string(bytes, &mut offset, "schema key")?;
    if schema_terminator != KEY_PART_FINAL {
        return Err(key_codec_error(
            "hot file schema key has an invalid terminator",
        ));
    }
    if offset != bytes.len() {
        return Err(key_codec_error("hot file schema key has trailing bytes"));
    }
    let _ = schema_key;
    Ok((branch_id, generation))
}

struct HotDiffSegmentScope {
    branch_id: String,
    checkpoint_commit_id: CommitId,
    generation: CommitId,
    digest: [u8; 32],
}

fn decode_hot_diff_key_in_scope(bytes: &[u8], scope: &[u8]) -> Result<HeadRowIdentity, LixError> {
    if !bytes.starts_with(scope) {
        return Err(key_codec_error(
            "hot diff row does not begin with its scanned scope",
        ));
    }
    let mut offset = scope.len();
    let (schema_key, schema_terminator) = read_key_string(bytes, &mut offset, "schema key")?;
    if schema_terminator != KEY_PART_FINAL {
        return Err(key_codec_error(
            "hot diff row schema key has an invalid terminator",
        ));
    }
    let entity_pk = read_entity_pk(bytes, &mut offset)?;
    let file_id = read_file_id(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(key_codec_error("hot diff row key has trailing bytes"));
    }
    Ok(HeadRowIdentity {
        schema_key,
        entity_pk,
        file_id,
    })
}

fn decode_hot_diff_segment_key(bytes: &[u8]) -> Result<HotDiffSegmentScope, LixError> {
    let mut offset = 0;
    let (branch_id, branch_terminator) = read_key_string(bytes, &mut offset, "branch id")?;
    if branch_terminator != KEY_PART_FINAL {
        return Err(key_codec_error(
            "hot diff branch id has an invalid terminator",
        ));
    }
    let checkpoint_commit_id = read_generation(bytes, &mut offset)?;
    let generation = read_generation(bytes, &mut offset)?;
    let digest_bytes = bytes
        .get(offset..)
        .ok_or_else(|| key_codec_error("hot diff segment key is truncated before its digest"))?;
    let digest = <[u8; 32]>::try_from(digest_bytes)
        .map_err(|_| key_codec_error("hot diff segment key has an invalid digest length"))?;
    Ok(HotDiffSegmentScope {
        branch_id,
        checkpoint_commit_id,
        generation,
        digest,
    })
}

fn visit_hot_diff_segment(
    bytes: &[u8],
    scope: &[u8],
    coverage: &mut WorkingDiffIndexCoverage,
    mut visit: impl FnMut(HeadRowIdentity),
) -> Result<(), LixError> {
    if bytes.len() < HOT_DIFF_SEGMENT_HEADER_BYTES {
        return Err(key_codec_error("hot diff segment is truncated"));
    }
    if bytes[0] != HOT_DIFF_SEGMENT_VERSION {
        return Err(key_codec_error("hot diff segment has an unknown version"));
    }
    let count = u32::from_le_bytes(
        bytes[1..HOT_DIFF_SEGMENT_HEADER_BYTES]
            .try_into()
            .expect("hot diff segment header has a fixed count width"),
    );
    if count == 0 || count > HOT_DIFF_SEGMENT_MAX_IDENTITIES {
        return Err(key_codec_error(
            "hot diff segment has an invalid identity count",
        ));
    }
    let mut offset = HOT_DIFF_SEGMENT_HEADER_BYTES;
    let mut full_key = Vec::with_capacity(scope.len() + 128);
    full_key.extend_from_slice(scope);
    for _ in 0..count {
        let length_end = offset
            .checked_add(4)
            .ok_or_else(|| key_codec_error("hot diff segment length offset overflow"))?;
        let encoded_length = bytes.get(offset..length_end).ok_or_else(|| {
            key_codec_error("hot diff segment is truncated before identity length")
        })?;
        let suffix_len = u32::from_le_bytes(
            encoded_length
                .try_into()
                .expect("hot diff identity length has a fixed width"),
        ) as usize;
        if suffix_len == 0 {
            return Err(key_codec_error("hot diff segment has an empty identity"));
        }
        let suffix_end = length_end
            .checked_add(suffix_len)
            .ok_or_else(|| key_codec_error("hot diff segment identity offset overflow"))?;
        let suffix = bytes
            .get(length_end..suffix_end)
            .ok_or_else(|| key_codec_error("hot diff segment is truncated in an identity"))?;
        full_key.truncate(scope.len());
        full_key.extend_from_slice(suffix);
        coverage
            .add_encoded_group_key(&full_key)
            .ok_or_else(|| head_value_error("hot working-diff index count exceeds u64"))?;
        visit(decode_hot_diff_key_in_scope(&full_key, scope)?);
        offset = suffix_end;
    }
    if offset != bytes.len() {
        return Err(key_codec_error("hot diff segment has trailing bytes"));
    }
    Ok(())
}

fn decode_hot_diff_key(bytes: &[u8]) -> Result<(CommitId, HeadIdentity), LixError> {
    let mut offset = 0;
    let (branch_id, branch_terminator) = read_key_string(bytes, &mut offset, "branch id")?;
    if branch_terminator != KEY_PART_FINAL {
        return Err(key_codec_error(
            "hot diff branch id has an invalid terminator",
        ));
    }
    let checkpoint_commit_id = read_generation(bytes, &mut offset)?;
    let generation = read_generation(bytes, &mut offset)?;
    let (schema_key, schema_terminator) = read_key_string(bytes, &mut offset, "schema key")?;
    if schema_terminator != KEY_PART_FINAL {
        return Err(key_codec_error(
            "hot diff schema key has an invalid terminator",
        ));
    }
    let entity_pk = read_entity_pk(bytes, &mut offset)?;
    let file_id = read_file_id(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(key_codec_error("hot diff key has trailing bytes"));
    }
    Ok((
        checkpoint_commit_id,
        HeadIdentity {
            branch_id,
            generation,
            schema_key,
            entity_pk,
            file_id,
        },
    ))
}

fn collect_hot_untracked_refs(value: HeadValueView<'_>, refs: &mut BTreeSet<[u8; JSON_REF_BYTES]>) {
    if !value.untracked {
        return;
    }
    for slot in [value.snapshot, value.metadata] {
        if let HeadSlotView::Ref(json_ref) = slot {
            refs.insert(*json_ref.as_hash_array());
        }
    }
}

pub(crate) async fn stage_collect_stale_hot_generations<S>(
    store: &S,
    writes: &mut StorageWriteSet,
    controls: &[(String, BranchHeadControl)],
) -> Result<Vec<JsonRef>, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let active = active_current_state_generations(controls);
    let mut stale_untracked_refs = BTreeSet::new();
    stage_collect_stale_hot_space(
        store,
        writes,
        HOT_ROW_SPACE,
        decode_hot_row_scope,
        &active,
        &mut stale_untracked_refs,
    )
    .await?;
    // Sweep schema membership markers independently so orphaned generations
    // cannot retain conservative file-membership hints.
    stage_collect_stale_hot_space(
        store,
        writes,
        HOT_FILE_SPACE,
        decode_hot_file_scope,
        &active,
        &mut stale_untracked_refs,
    )
    .await?;
    stage_collect_stale_hot_collection_controls(store, writes, &active).await?;
    Ok(stale_untracked_refs
        .into_iter()
        .map(JsonRef::from_hash_bytes)
        .collect())
}

fn decode_hot_collection_control_scope(bytes: &[u8]) -> Result<(String, CommitId), LixError> {
    let mut offset = 0;
    let (branch_id, branch_terminator) = read_key_string(bytes, &mut offset, "branch id")?;
    if branch_terminator != KEY_PART_FINAL {
        return Err(key_codec_error(
            "hot collection-control branch id has an invalid terminator",
        ));
    }
    let generation = read_generation(bytes, &mut offset)?;
    Ok((branch_id, generation))
}

async fn stage_collect_stale_hot_collection_controls(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    active: &BTreeSet<(String, CommitId)>,
) -> Result<(), LixError> {
    let plan = ScanPlan::prefix(
        HOT_COLLECTION_CONTROL_SPACE,
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
        resume_after = page.value.entries.last().map(|entry| entry.key.clone());
        for entry in page.value.entries {
            let keep = decode_hot_collection_control_scope(entry.key.0.as_ref())
                .is_ok_and(|scope| active.contains(&scope));
            if !keep {
                writes.delete(HOT_COLLECTION_CONTROL_SPACE, entry.key);
            }
        }
        if !page.value.has_more || resume_after.is_none() {
            break;
        }
    }
    Ok(())
}

async fn stage_collect_stale_hot_space(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    space: StorageSpace,
    decode_key: fn(&[u8]) -> Result<(String, CommitId), LixError>,
    active: &BTreeSet<(String, CommitId)>,
    stale_untracked_refs: &mut BTreeSet<[u8; JSON_REF_BYTES]>,
) -> Result<(), LixError> {
    let plan = ScanPlan::prefix(
        space,
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
        resume_after = page.value.entries.last().map(|entry| entry.key.clone());
        for entry in page.value.entries {
            let active_generation =
                decode_key(entry.key.0.as_ref()).is_ok_and(|identity| active.contains(&identity));
            if active_generation {
                continue;
            }
            if let StorageProjectedValue::FullValue(bytes) = &entry.value
                && let Ok(value) = decode_head_value(bytes)
            {
                collect_hot_untracked_refs(value, stale_untracked_refs);
            }
            writes.delete(space, entry.key);
        }
        if !page.value.has_more || resume_after.is_none() {
            break;
        }
    }
    Ok(())
}

pub(crate) async fn stage_collect_stale_hot_diff_records<S>(
    store: &S,
    writes: &mut StorageWriteSet,
    active: &BTreeMap<String, ActiveWorkingDiffScope>,
) -> Result<(), LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let plan = ScanPlan::prefix(
        HOT_DIFF_SPACE,
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
        resume_after = page.value.entries.last().map(|entry| entry.key.clone());
        for entry in page.value.entries {
            let keep = match full_value_bytes(entry.value) {
                Ok(bytes) if bytes.is_empty() => decode_hot_diff_key(entry.key.0.as_ref())
                    .is_ok_and(|(checkpoint_commit_id, identity)| {
                        active.get(&identity.branch_id).is_some_and(|scope| {
                            scope.checkpoint_commit_id == checkpoint_commit_id
                                && scope.generation == identity.generation
                        })
                    }),
                Ok(bytes) => {
                    decode_hot_diff_segment_key(entry.key.0.as_ref()).is_ok_and(|segment_scope| {
                        if !active.get(&segment_scope.branch_id).is_some_and(|scope| {
                            scope.checkpoint_commit_id == segment_scope.checkpoint_commit_id
                                && scope.generation == segment_scope.generation
                        }) {
                            return false;
                        }
                        let scope = encode_working_diff_scope_prefix(
                            &segment_scope.branch_id,
                            segment_scope.checkpoint_commit_id,
                            segment_scope.generation,
                        );
                        let mut coverage = WorkingDiffIndexCoverage::default();
                        segment_scope.digest == *blake3::hash(&bytes).as_bytes()
                            && visit_hot_diff_segment(&bytes, &scope, &mut coverage, |_| {}).is_ok()
                    })
                }
                _ => false,
            };
            if !keep {
                writes.delete(HOT_DIFF_SPACE, entry.key);
            }
        }
        if !page.value.has_more || resume_after.is_none() {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;

    use super::*;
    use crate::branch::{BranchHeadControl, stage_branch_head_control};
    use crate::storage_adapter::{
        Memory, StorageAdapter, StorageGetManyRequest, StorageGetManyResult, StorageKeyRange,
        StorageReadOptions, StorageScanChunk, StorageScanOptions, StorageSpaceId,
        StorageWriteOptions,
    };

    struct CountingRead<R> {
        inner: R,
        get_many_calls: Arc<AtomicUsize>,
    }

    impl<R: StorageAdapterRead> StorageAdapterRead for CountingRead<R> {
        fn snapshot_cache_key(&self) -> Option<u128> {
            self.inner.snapshot_cache_key()
        }

        async fn get_many(
            &self,
            requests: &[StorageGetManyRequest<'_>],
        ) -> Result<StorageGetManyResult, crate::storage_adapter::StorageError> {
            self.get_many_calls.fetch_add(1, Ordering::Relaxed);
            self.inner.get_many(requests).await
        }

        async fn scan(
            &self,
            space: StorageSpaceId,
            range: StorageKeyRange,
            opts: StorageScanOptions,
        ) -> Result<StorageScanChunk, crate::storage_adapter::StorageError> {
            self.inner.scan(space, range, opts).await
        }
    }

    fn timestamp() -> LixTimestamp {
        LixTimestamp::expect_parse("hot working-diff test timestamp", "2026-01-01T00:00:00Z")
    }

    #[tokio::test]
    async fn certified_history_scan_ignores_malformed_unrequested_commit() {
        let storage = StorageAdapter::new(Memory::new());
        let requested_commit = CommitId::for_test_label("requested-certified-history");
        let unrelated_commit = CommitId::for_test_label("malformed-certified-history");
        let mut writes = storage.new_write_set();
        writes.put(
            CERTIFIED_ENTITY_BATCH_SPACE,
            StorageKey(Bytes::copy_from_slice(
                unrelated_commit.as_uuid().as_bytes(),
            )),
            StorageValue {
                bytes: Bytes::from_static(b"malformed"),
            },
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("malformed unrelated certified batch should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("certified history read should open");
        let rows = scan_certified_history_rows(
            &read,
            &BTreeSet::from([requested_commit]),
            &TrackedStateScanRequest::default(),
        )
        .await
        .expect("unrelated malformed batch must not affect the requested commit");

        assert!(rows.is_empty());
    }

    #[test]
    fn created_id_is_inserted_without_materializing_canonical_json() {
        let id = "018f47a2-cafe-7000-8000-000000000001";
        assert_eq!(
            insert_created_id_into_canonical_object(b"{}", id).unwrap(),
            format!(r#"{{"id":"{id}"}}"#).as_bytes()
        );
        assert_eq!(
            insert_created_id_into_canonical_object(
                br#"{"format":{"nested":"},\\\""},"kind":"paragraph","parent_id":null}"#,
                id,
            )
            .unwrap(),
            format!(
                r#"{{"format":{{"nested":"}},\\\""}},"id":"{id}","kind":"paragraph","parent_id":null}}"#
            )
            .as_bytes()
        );
        assert_eq!(
            insert_created_id_into_canonical_object(br#"{"alpha":1}"#, id).unwrap(),
            format!(r#"{{"alpha":1,"id":"{id}"}}"#).as_bytes()
        );
    }

    #[test]
    fn create_only_packet_pages_expose_their_local_ref_range() {
        fn create_record(local_ref: u64) -> Vec<u8> {
            let mut record = vec![2];
            record.extend_from_slice(&6_u32.to_le_bytes());
            record.extend_from_slice(b"schema");
            record.extend_from_slice(&local_ref.to_le_bytes());
            let mut framed = Vec::new();
            framed.extend_from_slice(&(record.len() as u32).to_le_bytes());
            framed.extend_from_slice(&record);
            framed
        }

        let mut page = create_record(7);
        page.extend_from_slice(&create_record(11));
        assert_eq!(
            certified_packet_page_local_ref_range(&page).unwrap(),
            Some((7, 11))
        );

        let mut keyed = create_record(7);
        keyed[4] = 0;
        assert_eq!(certified_packet_page_local_ref_range(&keyed).unwrap(), None);
    }

    #[test]
    fn compressed_certified_page_rejects_invalid_bounds_and_corruption() {
        let mut inverted = Vec::new();
        inverted.extend_from_slice(&2_u32.to_le_bytes());
        inverted.extend_from_slice(&1_u32.to_le_bytes());
        inverted.extend_from_slice(&1_u32.to_le_bytes());
        inverted.push(0);
        assert!(certified_zstd_packet_page_header(&inverted).is_err());

        let mut oversized = Vec::new();
        oversized.extend_from_slice(&1_u32.to_le_bytes());
        oversized.extend_from_slice(&2_u32.to_le_bytes());
        oversized.extend_from_slice(&(64_u32 * 1024 * 1024 + 1).to_le_bytes());
        oversized.push(0);
        assert!(certified_zstd_packet_page_header(&oversized).is_err());

        let mut corrupt = Vec::new();
        corrupt.extend_from_slice(&1_u32.to_le_bytes());
        corrupt.extend_from_slice(&2_u32.to_le_bytes());
        corrupt.extend_from_slice(&16_u32.to_le_bytes());
        corrupt.extend_from_slice(b"not a zstd frame");
        assert!(decode_certified_zstd_packet_page(&corrupt).is_err());
    }

    #[tokio::test]
    async fn exact_file_certified_scan_does_not_read_unrelated_manifest() {
        let storage = StorageAdapter::new(Memory::new());
        let generation = CommitId::for_test_label("exact-certified-file-generation");
        let malformed_content_key = StorageKey(Bytes::from_static(b"malformed-content"));
        let mut manifest_key = generation.as_uuid().as_bytes().to_vec();
        append_batch_text(&mut manifest_key, "unrelated.md").unwrap();
        manifest_key
            .extend_from_slice(&crate::wasm::HOST_CERTIFIED_ZSTD_PACKET_FORMAT.to_le_bytes());
        manifest_key.extend_from_slice(
            CommitId::for_test_label("unrelated-certified-commit")
                .as_uuid()
                .as_bytes(),
        );
        let mut writes = storage.new_write_set();
        writes.put(
            CERTIFIED_ENTITY_BATCH_MANIFEST_SPACE,
            StorageKey(Bytes::from(manifest_key)),
            StorageValue {
                bytes: malformed_content_key.0.clone(),
            },
        );
        writes.put(
            CERTIFIED_ENTITY_BATCH_SPACE,
            malformed_content_key,
            StorageValue {
                bytes: Bytes::from_static(b"malformed"),
            },
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .unwrap();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let rows = scan_certified_entity_batch_rows(
            &read,
            "main",
            generation,
            &TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    file_ids: vec![NullableKeyFilter::Value("requested.md".to_owned())],
                    ..TrackedStateFilter::default()
                },
                ..TrackedStateScanRequest::default()
            },
            None,
        )
        .await
        .expect("exact-file scan must not decode unrelated certified content");
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn branch_creation_inherits_certified_manifests_from_same_head() {
        const EMPTY_BRANCH: &str = "a-empty";
        const DONOR_BRANCH: &str = "donor";
        const SECOND_DONOR_BRANCH: &str = "donor-two";
        const CREATED_BRANCH: &str = "created";
        const FILE_ID: &str = "inherited.csv";
        const SECOND_FILE_ID: &str = "inherited-two.csv";
        const SCHEMA_KEY: &str = "inherited_row";

        let storage = StorageAdapter::new(Memory::new());
        let head_commit_id = CommitId::for_test_label("certified-inherited-head");
        let donor_generation = CommitId::for_test_label("certified-inherited-donor");
        let created_generation = CommitId::for_test_label("certified-inherited-created");
        let created_at = timestamp();
        let donor_control = BranchHeadControl {
            head_commit_id,
            generation: donor_generation,
            current_state_revision: 0,
            schema_presence_bloom: [u64::MAX; 4],
            working_diff_checkpoint_commit_id: None,
            created_at,
            updated_at: created_at,
            ref_change_id: ChangeId::for_test_label("certified-inherited-donor-ref"),
        };
        let empty_control = BranchHeadControl {
            generation: CommitId::for_test_label("certified-inherited-empty"),
            ref_change_id: ChangeId::for_test_label("certified-inherited-empty-ref"),
            ..donor_control
        };
        let second_donor_control = BranchHeadControl {
            generation: CommitId::for_test_label("certified-inherited-donor-two"),
            ref_change_id: ChangeId::for_test_label("certified-inherited-donor-two-ref"),
            ..donor_control
        };
        let creates = WasmCreateContext {
            high: 0x0192_0000_0000_7000,
            low: 0x8000_0000,
        };
        let mut page = Vec::new();
        page.extend_from_slice(&0_u32.to_le_bytes());
        page.extend_from_slice(&1_u64.to_le_bytes());
        page.push(0);
        page.extend_from_slice(&0_u32.to_le_bytes());
        page.extend_from_slice(&1_u16.to_le_bytes());
        page.extend_from_slice(&5_u32.to_le_bytes());
        page.extend_from_slice(b"value");
        let batch = WasmCertifiedEntityBatch {
            format: 1,
            schema_keys: vec![SCHEMA_KEY.to_owned()],
            row_count: 1,
            creates,
            complete_file_state: true,
            pages: vec![Bytes::from(page)],
        };
        let batches = [batch.clone()];
        let second_batches = [WasmCertifiedEntityBatch {
            creates: WasmCreateContext {
                low: creates.low + 1,
                ..creates
            },
            ..batch
        }];
        let files = [
            CertifiedEntityBatchFileRef {
                branch_id: DONOR_BRANCH,
                file_id: FILE_ID,
                batches: &batches,
            },
            CertifiedEntityBatchFileRef {
                branch_id: DONOR_BRANCH,
                file_id: SECOND_FILE_ID,
                batches: &second_batches,
            },
        ];

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("donor certified read should open");
        let mut writes = StorageWriteSet::new();
        stage_branch_head_control(&mut writes, EMPTY_BRANCH, empty_control)
            .expect("empty same-head control should stage");
        stage_branch_head_control(&mut writes, DONOR_BRANCH, donor_control)
            .expect("donor control should stage");
        stage_certified_entity_batches(
            &read,
            &mut writes,
            &files,
            &BTreeMap::from([(DONOR_BRANCH.to_owned(), donor_control)]),
            &BTreeMap::from([(
                DONOR_BRANCH.to_owned(),
                crate::branch::BranchHeadControlObservation {
                    control: Some(donor_control),
                    raw_token: None,
                },
            )]),
            &BTreeMap::from([(head_commit_id, created_at)]),
        )
        .await
        .expect("donor certified batch should stage");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("donor certified batch should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("second donor read should open");
        let mut writes = StorageWriteSet::new();
        stage_branch_head_control(&mut writes, SECOND_DONOR_BRANCH, second_donor_control)
            .expect("second donor control should stage");
        stage_certified_entity_batches(
            &read,
            &mut writes,
            &[],
            &BTreeMap::from([(SECOND_DONOR_BRANCH.to_owned(), second_donor_control)]),
            &BTreeMap::from([(
                SECOND_DONOR_BRANCH.to_owned(),
                crate::branch::BranchHeadControlObservation {
                    control: None,
                    raw_token: None,
                },
            )]),
            &BTreeMap::new(),
        )
        .await
        .expect("second donor should inherit certified manifests");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("second donor certified manifests should commit");

        let created_control = BranchHeadControl {
            generation: created_generation,
            ref_change_id: ChangeId::for_test_label("certified-inherited-created-ref"),
            ..donor_control
        };
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("created branch read should open");
        let mut writes = StorageWriteSet::new();
        stage_certified_entity_batches(
            &read,
            &mut writes,
            &[],
            &BTreeMap::from([(CREATED_BRANCH.to_owned(), created_control)]),
            &BTreeMap::from([(
                CREATED_BRANCH.to_owned(),
                crate::branch::BranchHeadControlObservation {
                    control: None,
                    raw_token: None,
                },
            )]),
            &BTreeMap::new(),
        )
        .await
        .expect("created branch should inherit certified manifests");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("created branch certified manifests should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("created branch verification read should open");
        let get_many_calls = Arc::new(AtomicUsize::new(0));
        let read = CountingRead {
            inner: read,
            get_many_calls: Arc::clone(&get_many_calls),
        };
        let rows = scan_certified_entity_batch_rows(
            &read,
            CREATED_BRANCH,
            created_generation,
            &TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    schema_keys: vec![SCHEMA_KEY.to_owned()],
                    ..TrackedStateFilter::default()
                },
                read_columns: TrackedStateReadColumns {
                    columns: vec!["snapshot_content".to_owned()],
                },
                limit: None,
            },
            None,
        )
        .await
        .expect("created branch certified rows should scan");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows.row(0).schema_key(), SCHEMA_KEY);
        assert_eq!(rows.row(0).file_id(), Some(FILE_ID));
        assert_eq!(rows.row(1).file_id(), Some(SECOND_FILE_ID));
        assert_eq!(
            get_many_calls.load(Ordering::Relaxed),
            2,
            "one content read and one page read must serve every certified batch"
        );
    }

    fn diff_identity(branch_id: &str, generation: CommitId, entity: &str) -> HeadIdentity {
        HeadIdentity {
            branch_id: branch_id.to_string(),
            generation,
            schema_key: "schema".to_string(),
            entity_pk: EntityPk::single(entity),
            file_id: None,
        }
    }

    fn working_diff_version(label: &str) -> WorkingDiffVersion {
        WorkingDiffVersion {
            change_id: ChangeId::for_test_label(&format!("{label}-change")),
            commit_id: CommitId::for_test_label(&format!("{label}-commit")),
            deleted: false,
            created_at: timestamp(),
            updated_at: timestamp(),
            snapshot: WorkingDiffSlotFingerprint {
                kind: WORKING_DIFF_SLOT_NONE,
                hash: [0; JSON_REF_BYTES],
            },
            metadata: WorkingDiffSlotFingerprint {
                kind: WORKING_DIFF_SLOT_NONE,
                hash: [0; JSON_REF_BYTES],
            },
        }
    }

    fn single_hot_diff_segment(
        checkpoint_commit_id: CommitId,
        identity: &HeadIdentity,
    ) -> (Vec<u8>, Vec<u8>) {
        let scope = encode_working_diff_scope_prefix(
            &identity.branch_id,
            checkpoint_commit_id,
            identity.generation,
        );
        let full_key = encode_hot_diff_key(checkpoint_commit_id, identity);
        let suffix = full_key
            .strip_prefix(scope.as_slice())
            .expect("encoded hot diff identity starts with its scope");
        let mut value = Vec::with_capacity(HOT_DIFF_SEGMENT_HEADER_BYTES + 4 + suffix.len());
        value.push(HOT_DIFF_SEGMENT_VERSION);
        value.extend_from_slice(&1_u32.to_le_bytes());
        value.extend_from_slice(
            &u32::try_from(suffix.len())
                .expect("test identity suffix fits u32")
                .to_le_bytes(),
        );
        value.extend_from_slice(suffix);
        let mut key = scope;
        key.extend_from_slice(blake3::hash(&value).as_bytes());
        (key, value)
    }

    #[test]
    fn hot_working_diff_entries_share_one_identity_batch() {
        let candidates = ["first", "second"]
            .into_iter()
            .map(|entity| {
                (
                    TrackedStateKey {
                        schema_key: "schema".to_owned(),
                        entity_pk: EntityPk::single(entity),
                        file_id: Some("file".to_owned()),
                    },
                    None,
                    working_diff_version(entity),
                )
            })
            .collect();

        let entries =
            classify_hot_working_diff_entries(candidates).expect("valid working diff batch");

        assert_eq!(entries.len(), 2);
        assert!(entries[0].identity.shares_batch_with(&entries[1].identity));
        for entry in &entries {
            assert!(
                entry
                    .identity
                    .shares_key_with(&entry.after.as_ref().expect("after row").identity)
            );
        }
    }

    #[test]
    fn finite_hot_working_diff_borrows_keys_into_one_identity_batch() {
        let schema_key = String::from("schema");
        let file_id = String::from("file");
        let entity_pks = [EntityPk::single("first"), EntityPk::single("second")];
        let candidates = entity_pks
            .iter()
            .enumerate()
            .map(|(index, entity_pk)| {
                (
                    TrackedStateKeyRef {
                        schema_key: &schema_key,
                        entity_pk,
                        file_id: Some(&file_id),
                    },
                    None,
                    working_diff_version(if index == 0 { "first" } else { "second" }),
                )
            })
            .collect();

        let entries =
            classify_hot_working_diff_entry_refs(candidates).expect("valid borrowed diff batch");

        assert_eq!(entries.len(), 2);
        assert!(entries[0].identity.shares_batch_with(&entries[1].identity));
        assert_eq!(entries[0].identity.schema_key(), schema_key);
        assert_eq!(entries[1].identity.file_id(), Some(file_id.as_str()));
        for entry in &entries {
            assert!(
                entry
                    .identity
                    .shares_key_with(&entry.after.as_ref().expect("after row").identity)
            );
        }
    }

    #[test]
    fn ten_thousand_finite_hot_identities_share_one_primary_key_arena() {
        let generation = CommitId::for_test_label("point-key-generation");
        let entity_pks = (0..5_000)
            .map(|index| EntityPk::single(format!("entity-{index:05}")))
            .collect::<Vec<_>>();
        let branch_id = String::from("branch");
        let schema_key = String::from("schema");
        let batch = FiniteHotIdentityBatchRef::new(
            &branch_id,
            generation,
            &schema_key,
            entity_pks.iter().collect(),
            vec![None, Some("file")],
        )
        .expect("test identity count is representable");

        assert_eq!(batch.len(), 10_000);
        assert_eq!(batch.encoded.ranges.len(), batch.len());
        assert_eq!(batch.encoded.ranges.capacity(), batch.len());
        assert_eq!(
            batch
                .encoded
                .ranges
                .last()
                .map(|ranges| ranges.primary.offset() + ranges.primary.len()),
            Some(batch.encoded.bytes.len())
        );
        for index in [0, 1, 9_999] {
            let identity = batch.key_ref(index);
            assert_eq!(batch.branch_id.as_ptr(), branch_id.as_ptr());
            assert_eq!(identity.schema_key.as_ptr(), schema_key.as_ptr());
            let primary = batch.encoded.ranges[index].primary;
            let key = batch.encoded.primary_key(index);
            assert_eq!(
                key.0.as_ptr(),
                batch.encoded.bytes[primary.offset()..].as_ptr(),
                "primary point key {index} must remain a slice of the batch arena"
            );
        }
    }

    #[test]
    fn ten_thousand_hot_scan_identities_borrow_repeated_metadata() {
        const ROW_COUNT: usize = 10_000;
        let generation = CommitId::for_test_label("borrowed-scan-generation");
        let scope = hot_scope_prefix("branch", generation);
        let schema_key = "shared_schema";
        let file_id = "shared_file";
        let entity_pks = (0..ROW_COUNT)
            .map(|index| EntityPk::single(format!("entity-{index:05}")))
            .collect::<Vec<_>>();
        let capacity = entity_pks
            .iter()
            .try_fold(0_usize, |total, entity_pk| {
                total.checked_add(
                    encoded_hot_identity_key_len(scope.len(), schema_key, entity_pk, Some(file_id))
                        .expect("test key size is representable"),
                )
            })
            .expect("test key arena size is representable");
        let mut key_bytes = Vec::with_capacity(capacity);
        let ranges = entity_pks
            .iter()
            .map(|entity_pk| {
                let start = key_bytes.len();
                key_bytes.extend_from_slice(&scope);
                write_key_string(&mut key_bytes, schema_key, KEY_PART_FINAL);
                write_file_id(&mut key_bytes, Some(file_id));
                write_entity_pk(&mut key_bytes, entity_pk);
                start..key_bytes.len()
            })
            .collect::<Vec<_>>();
        assert_eq!(key_bytes.len(), capacity);
        let key_bytes = Bytes::from(key_bytes);
        let identities = ranges
            .into_iter()
            .map(|range| {
                decode_hot_scan_row_key_in_scope(key_bytes.slice(range), &scope)
                    .expect("decode borrowed hot scan key")
            })
            .collect::<Vec<_>>();

        assert_eq!(identities.len(), ROW_COUNT);
        assert_eq!(
            identities
                .iter()
                .map(HotScanIdentity::owned_metadata_buffer_count)
                .sum::<usize>(),
            0,
            "normal schema and file ids must remain ranges over storage keys"
        );
        assert!(identities.iter().all(|identity| {
            identity.schema_key() == schema_key && identity.file_id() == Some(file_id)
        }));

        let mut rows = MaterializedLiveStateBatchBuilder::with_capacity(ROW_COUNT);
        for identity in identities {
            identity.push_materialized(
                &mut rows,
                None,
                None,
                false,
                timestamp(),
                timestamp(),
                false,
                None,
                None,
                true,
                "branch",
            );
        }
        let rows = rows.finish();

        assert_eq!(rows.len(), ROW_COUNT);
        assert_eq!(rows.dictionary_entry_count(), 3);
        assert_eq!(rows.dictionary_arena_buffer_count(), 1);
        assert_eq!(
            rows.dictionary_arena_allocation_count(),
            1,
            "materialization should allocate one small identity arena, not per-row buffers"
        );
        assert_eq!(rows.dictionary_arena_large_allocation_count(), 0);
        assert_eq!(
            rows.row(0).schema_key().as_ptr(),
            rows.row(ROW_COUNT - 1).schema_key().as_ptr()
        );
        assert_eq!(
            rows.row(0).file_id().expect("file").as_ptr(),
            rows.row(ROW_COUNT - 1).file_id().expect("file").as_ptr()
        );
    }

    #[test]
    fn hot_diff_keys_append_into_one_exact_arena() {
        let checkpoint = CommitId::for_test_label("shared-hot-diff-checkpoint");
        let generation = CommitId::for_test_label("shared-hot-diff-generation");
        let scope = encode_working_diff_scope_prefix("branch", checkpoint, generation);
        let identities = [
            HeadRowIdentity {
                schema_key: "schema".to_string(),
                entity_pk: EntityPk::single("first"),
                file_id: None,
            },
            HeadRowIdentity {
                schema_key: "schema\0escaped".to_string(),
                entity_pk: EntityPk::single("second\0escaped"),
                file_id: Some("file\0id".to_string()),
            },
        ];
        let capacity = identities
            .iter()
            .try_fold(0_usize, |total, identity| {
                total.checked_add(encoded_hot_identity_key_len(
                    scope.len(),
                    &identity.schema_key,
                    &identity.entity_pk,
                    identity.file_id.as_deref(),
                )?)
            })
            .expect("test identities have a representable encoded size");
        let mut key_bytes = Vec::with_capacity(capacity);
        let allocation = key_bytes.as_ptr();
        let ranges = identities
            .iter()
            .map(|identity| {
                append_hot_diff_key_parts(
                    &mut key_bytes,
                    &scope,
                    &identity.schema_key,
                    &identity.entity_pk,
                    identity.file_id.as_deref(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(key_bytes.len(), capacity);
        assert_eq!(key_bytes.capacity(), capacity);
        assert_eq!(key_bytes.as_ptr(), allocation);
        assert_eq!(ranges[0].end, ranges[1].start);
        for (range, expected) in ranges.into_iter().zip(identities) {
            let (decoded_checkpoint, decoded_identity) =
                decode_hot_diff_key(&key_bytes[range]).expect("decode appended hot-diff key");
            assert_eq!(decoded_checkpoint, checkpoint);
            assert_eq!(decoded_identity.branch_id, "branch");
            assert_eq!(decoded_identity.generation, generation);
            assert_eq!(decoded_identity.schema_key, expected.schema_key);
            assert_eq!(decoded_identity.entity_pk, expected.entity_pk);
            assert_eq!(decoded_identity.file_id, expected.file_id);
        }
    }

    #[tokio::test]
    async fn hot_diff_segments_preserve_identity_coverage_with_bounded_puts() {
        const IDENTITY_COUNT: usize = 10_000;

        let checkpoint = CommitId::for_test_label("segmented-hot-diff-checkpoint");
        let generation = CommitId::for_test_label("segmented-hot-diff-generation");
        let scope = encode_working_diff_scope_prefix("branch", checkpoint, generation);
        let mut identity_key_bytes = Vec::new();
        let mut identity_puts = Vec::with_capacity(IDENTITY_COUNT);
        let mut expected_coverage = WorkingDiffIndexCoverage::default();
        for index in 0..IDENTITY_COUNT {
            let entity_pk = EntityPk::single(format!("entity-{index:05}"));
            let key = append_hot_diff_key_parts(
                &mut identity_key_bytes,
                &scope,
                "schema",
                &entity_pk,
                Some("file.md"),
            );
            expected_coverage
                .add_encoded_group_key(&identity_key_bytes[key.clone()])
                .expect("test coverage count fits u64");
            identity_puts.push(EncodedPut {
                key: buffer_range(&key),
                value: BufferRange::default(),
            });
        }

        let mut writes = StorageWriteSet::new();
        stage_hot_diff_batch(&mut writes, &scope, identity_key_bytes, identity_puts)
            .expect("stage segmented hot diff");
        assert!(
            writes.stats().staged_puts <= 3,
            "ten thousand short identities should require at most three bounded segments"
        );

        let storage = StorageAdapter::new(Memory::new());
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit segmented hot diff");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open segmented hot diff read");
        let page = ScanPlan::prefix(
            HOT_DIFF_SPACE,
            StoragePrefix {
                bytes: Bytes::from(scope.clone()),
            },
        )
        .collect(&read, StorageScanOptions::default())
        .await
        .expect("scan segmented hot diff");
        assert!(!page.value.has_more);

        let mut actual_coverage = WorkingDiffIndexCoverage::default();
        let mut decoded = 0_usize;
        for entry in page.value.entries {
            let bytes = full_value_bytes(entry.value).expect("full segment value");
            let segment_scope =
                decode_hot_diff_segment_key(entry.key.0.as_ref()).expect("segment key");
            assert_eq!(segment_scope.digest, *blake3::hash(&bytes).as_bytes());
            visit_hot_diff_segment(&bytes, &scope, &mut actual_coverage, |_| decoded += 1)
                .expect("decode hot diff segment");
        }
        assert_eq!(decoded, IDENTITY_COUNT);
        assert_eq!(actual_coverage, expected_coverage);
    }

    #[test]
    fn hot_mutation_keys_append_into_one_exact_arena() {
        let generation = CommitId::for_test_label("shared-hot-mutation-generation");
        let scope = hot_scope_prefix("branch", generation);
        let first_pk = EntityPk::single("first\0entity");
        let second_pk = EntityPk::single("second");
        let first = CurrentStateDeltaRef {
            schema_key: "schema\0escaped",
            file_id: Some("file\0id"),
            entity_pk: &first_pk,
            change_id: None,
            commit_id: None,
            untracked: true,
            deleted: false,
            created_at: timestamp(),
            updated_at: timestamp(),
            snapshot: JsonSlotRef::Inline("{}"),
            metadata: JsonSlotRef::None,
        };
        let second = CurrentStateDeltaRef {
            schema_key: "schema_without_file",
            file_id: None,
            entity_pk: &second_pk,
            change_id: None,
            commit_id: None,
            untracked: true,
            deleted: false,
            created_at: timestamp(),
            updated_at: timestamp(),
            snapshot: JsonSlotRef::Inline("{}"),
            metadata: JsonSlotRef::None,
        };
        let deltas = [&first, &second];
        let capacity = encoded_hot_mutation_identity_capacity(scope.len(), &deltas)
            .expect("test identities have a representable encoded size");
        let mut key_bytes = Vec::with_capacity(capacity);
        let allocation = key_bytes.as_ptr();
        let ranges = deltas
            .iter()
            .map(|delta| append_hot_mutation_identity(&mut key_bytes, &scope, delta))
            .collect::<Vec<_>>();

        assert_eq!(key_bytes.len(), capacity);
        assert_eq!(key_bytes.capacity(), capacity);
        assert_eq!(key_bytes.as_ptr(), allocation);
        assert_eq!(ranges[0].row_key.offset(), 0);
        assert_eq!(
            ranges[0]
                .file_schema_key
                .expect("first identity has a file schema marker")
                .offset()
                + ranges[0]
                    .file_schema_key
                    .expect("first identity has a file schema marker")
                    .len(),
            ranges[1].row_key.offset()
        );
        for (range, delta) in ranges.iter().zip(deltas) {
            let row_start = range.row_key.offset();
            let row = decode_hot_row_key_in_scope(
                &key_bytes[row_start..row_start + range.row_key.len()],
                &scope,
            )
            .expect("decode shared row key");
            assert_eq!(row.schema_key, delta.schema_key);
            assert_eq!(row.entity_pk, *delta.entity_pk);
            assert_eq!(row.file_id.as_deref(), delta.file_id);

            let scan_row = decode_hot_scan_row_key_in_scope(
                Bytes::copy_from_slice(&key_bytes[row_start..row_start + range.row_key.len()]),
                &scope,
            )
            .expect("decode shared row key for direct scan");
            assert_eq!(scan_row.schema_key(), delta.schema_key);
            assert_eq!(scan_row.entity_pk, *delta.entity_pk);
            assert_eq!(scan_row.file_id(), delta.file_id);
            assert_eq!(
                scan_row.owned_metadata_buffer_count(),
                usize::from(delta.schema_key.contains('\0'))
                    + usize::from(delta.file_id.is_some_and(|file_id| file_id.contains('\0'))),
                "only escaped metadata should take an owned fallback"
            );

            if let Some(marker) = range.file_schema_key {
                let marker_start = marker.offset();
                assert_eq!(
                    &key_bytes[marker_start..marker_start + marker.len()],
                    encode_hot_file_schema_key(&scope, delta.schema_key)
                );
            }
        }

        let encoded = encode_hot_mutation_identities("branch", generation, &deltas);
        assert_eq!(encoded.key_bytes.as_ref(), key_bytes);
        assert_eq!(encoded.key_ranges.len(), ranges.len());
        for (encoded, expected) in encoded.key_ranges.iter().zip(ranges) {
            assert_eq!(encoded.row_key, expected.row_key);
            assert_eq!(encoded.file_schema_key, expected.file_schema_key);
        }
    }

    #[test]
    fn hot_next_values_append_into_one_planned_arena() {
        let tracked_pk = EntityPk::single("tracked");
        let tombstone_pk = EntityPk::single("tombstone");
        let untracked_pk = EntityPk::single("untracked");
        let removed_pk = EntityPk::single("removed");
        let snapshot_ref = JsonRef::for_content(b"{\"large\":\"snapshot\"}");
        let tracked = CurrentStateDeltaRef {
            schema_key: "tracked_schema",
            file_id: Some("tracked.json"),
            entity_pk: &tracked_pk,
            change_id: Some(ChangeId::for_test_label("planned-value-change")),
            commit_id: Some(CommitId::for_test_label("planned-value-commit")),
            untracked: false,
            deleted: false,
            created_at: timestamp(),
            updated_at: timestamp(),
            snapshot: JsonSlotRef::Inline("{\"tracked\":true}"),
            metadata: JsonSlotRef::Inline("{\"source\":\"test\"}"),
        };
        let tombstone = CurrentStateDeltaRef {
            schema_key: "tracked_schema",
            file_id: None,
            entity_pk: &tombstone_pk,
            change_id: Some(ChangeId::for_test_label("planned-tombstone-change")),
            commit_id: Some(CommitId::for_test_label("planned-tombstone-commit")),
            untracked: false,
            deleted: true,
            created_at: timestamp(),
            updated_at: timestamp(),
            // Deleted values deliberately ignore both supplied slots.
            snapshot: JsonSlotRef::Inline("{\"ignored\":true}"),
            metadata: JsonSlotRef::Ref(&snapshot_ref),
        };
        let untracked = CurrentStateDeltaRef {
            schema_key: "untracked_schema",
            file_id: Some("untracked.json"),
            entity_pk: &untracked_pk,
            change_id: None,
            commit_id: None,
            untracked: true,
            deleted: false,
            created_at: timestamp(),
            updated_at: timestamp(),
            snapshot: JsonSlotRef::Ref(&snapshot_ref),
            metadata: JsonSlotRef::None,
        };
        let removed = CurrentStateDeltaRef {
            schema_key: "untracked_schema",
            file_id: Some("removed.json"),
            entity_pk: &removed_pk,
            change_id: None,
            commit_id: None,
            untracked: true,
            deleted: true,
            created_at: timestamp(),
            updated_at: timestamp(),
            snapshot: JsonSlotRef::None,
            metadata: JsonSlotRef::None,
        };
        let deltas = [&tracked, &tombstone, &untracked, &removed];

        let ordinary_capacity = deltas
            .iter()
            .try_fold(0_usize, |total, delta| {
                checked_add_hot_next_value_capacity(total, delta, false)
            })
            .expect("ordinary test values have a representable encoded size");
        let mut ordinary = Vec::with_capacity(ordinary_capacity);
        let ordinary_allocation = ordinary.as_ptr();
        let mut ordinary_expected = Vec::new();
        let mut ordinary_ranges = Vec::new();
        for delta in deltas {
            if delta.physically_deletes() {
                continue;
            }
            let value = delta.value_ref(delta.created_at, WorkingDiffBaseline::Disabled);
            ordinary_expected.extend_from_slice(
                &encode_head_value(&value).expect("encode ordinary expected value"),
            );
            ordinary_ranges.push(
                append_head_value(&mut ordinary, &value).expect("append ordinary planned value"),
            );
        }
        assert_eq!(ordinary, ordinary_expected);
        assert_eq!(ordinary.len(), ordinary_capacity);
        assert_eq!(ordinary.capacity(), ordinary_capacity);
        assert_eq!(ordinary.as_ptr(), ordinary_allocation);
        for range in ordinary_ranges {
            assert_eq!(
                decode_head_value(&ordinary[range])
                    .expect("decode ordinary planned value")
                    .working_diff_baseline,
                WorkingDiffBaseline::Disabled
            );
        }

        let before = WorkingDiffVersion {
            change_id: ChangeId::for_test_label("planned-before-change"),
            commit_id: CommitId::for_test_label("planned-before-commit"),
            deleted: false,
            created_at: timestamp(),
            updated_at: timestamp(),
            snapshot: WorkingDiffSlotFingerprint {
                kind: WORKING_DIFF_SLOT_NONE,
                hash: [0; JSON_REF_BYTES],
            },
            metadata: WorkingDiffSlotFingerprint {
                kind: WORKING_DIFF_SLOT_NONE,
                hash: [0; JSON_REF_BYTES],
            },
        };
        let checkpoint_capacity = [&tracked, &tombstone, &untracked, &removed]
            .iter()
            .try_fold(0_usize, |total, delta| {
                checked_add_hot_next_value_capacity(total, delta, true)
            })
            .expect("checkpoint test values have a representable encoded size");
        let checkpoint_baselines = [
            WorkingDiffBaseline::BeforePresent(before),
            WorkingDiffBaseline::BeforePresent(before),
            WorkingDiffBaseline::Disabled,
            WorkingDiffBaseline::Disabled,
        ];
        let mut checkpoint = Vec::with_capacity(checkpoint_capacity);
        let checkpoint_allocation = checkpoint.as_ptr();
        let mut checkpoint_expected = Vec::new();
        for (delta, baseline) in [&tracked, &tombstone, &untracked, &removed]
            .into_iter()
            .zip(checkpoint_baselines)
        {
            if delta.physically_deletes() {
                continue;
            }
            let value = delta.value_ref(delta.created_at, baseline);
            checkpoint_expected.extend_from_slice(
                &encode_head_value(&value).expect("encode checkpoint expected value"),
            );
            append_head_value(&mut checkpoint, &value).expect("append checkpoint planned value");
        }
        assert_eq!(checkpoint, checkpoint_expected);
        assert_eq!(checkpoint.len(), checkpoint_capacity);
        assert_eq!(checkpoint.capacity(), checkpoint_capacity);
        assert_eq!(checkpoint.as_ptr(), checkpoint_allocation);

        let before_absent =
            tracked.value_ref(tracked.created_at, WorkingDiffBaseline::BeforeAbsent);
        let before_absent_bytes =
            encode_head_value(&before_absent).expect("encode before-absent checkpoint value");
        let tracked_checkpoint_capacity = checked_add_hot_next_value_capacity(0, &tracked, true)
            .expect("tracked checkpoint value has a representable size");
        assert_eq!(
            tracked_checkpoint_capacity,
            before_absent_bytes.len() + WORKING_DIFF_VERSION_BYTES,
            "new checkpoint rows use the same safe fixed-size upper bound"
        );

        assert!(
            checked_add_hot_next_value_capacity(usize::MAX, &tracked, false).is_none(),
            "overflow must select the caller's zero-capacity fallback"
        );
        assert_eq!(
            checked_add_hot_next_value_capacity(usize::MAX, &tracked, false).unwrap_or(0),
            0
        );
    }

    #[test]
    fn hot_tracked_snapshot_clones_share_encoded_row_values() {
        let snapshot =
            HotTrackedSnapshot::from_materialized_rows(vec![MaterializedTrackedStateRow {
                entity_pk: EntityPk::single("entity"),
                schema_key: "schema".to_string(),
                file_id: None,
                snapshot_content: None,
                metadata: None,
                deleted: false,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                updated_at: "2026-01-01T00:00:00Z".to_string(),
                change_id: ChangeId::for_test_label("hot-shared-value-change"),
                commit_id: CommitId::for_test_label("hot-shared-value-commit"),
            }])
            .expect("encode tracked snapshot");
        let cloned = snapshot.clone();
        let source = snapshot.rows.values().next().expect("source encoded row");
        let retained = cloned.rows.values().next().expect("cloned encoded row");

        assert_eq!(source.as_ptr(), retained.as_ptr());
        assert_eq!(source.len(), retained.len());
    }

    #[test]
    fn hot_batch_staging_retains_encoded_key_and_value_arenas() {
        let key_bytes = Bytes::from_static(b"row-keymarker");
        let value_bytes = Bytes::from_static(b"value");
        let identities = EncodedHotMutationIdentities {
            key_bytes: key_bytes.clone(),
            key_ranges: vec![EncodedHotMutationIdentityRanges {
                row_key: BufferRange::new(0, 7),
                file_schema_key: Some(BufferRange::new(7, 6)),
            }],
        };
        let mut writes = StorageWriteSet::new();

        stage_hot_mutation_batch(&mut writes, identities, value_bytes, vec![Some(0..5)]);

        let stats = writes.arena_stats();
        assert_eq!(stats.spaces, 2);
        assert_eq!(stats.put_descriptors, 2);
        assert_eq!(stats.key_inline_bytes, 0);
        assert_eq!(stats.value_inline_bytes, 0);
        assert_eq!(stats.key_shared_buffers, 2);
        assert_eq!(stats.value_shared_buffers, 2);
    }

    #[test]
    fn ten_thousand_file_cascade_mutations_reserve_shared_buffers_once() {
        const ROW_COUNT: usize = 10_000;

        let tombstone = HeadValueRef {
            change_id: Some(ChangeId::for_test_label("cascade-reserve-change")),
            commit_id: Some(CommitId::for_test_label("cascade-reserve-commit")),
            untracked: false,
            deleted: true,
            created_at: timestamp(),
            updated_at: timestamp(),
            snapshot: JsonSlotRef::None,
            metadata: JsonSlotRef::None,
            working_diff_baseline: WorkingDiffBaseline::BeforePresent(working_diff_version(
                "cascade-reserve-before",
            )),
        };
        let encoded_tombstone =
            encode_head_value(&tombstone).expect("encode maximum cascade tombstone");
        assert_eq!(
            encoded_tombstone.len(),
            HEAD_VALUE_HEADER_BYTES + WORKING_DIFF_VERSION_BYTES,
            "the cascade value reservation must cover the largest checkpoint tombstone"
        );

        let mut buffers = HotCascadeMutationBuffers::with_capacity(ROW_COUNT, 0, true);
        let value_allocation = buffers.value_bytes.as_ptr();
        let row_put_allocation = buffers.row_puts.as_ptr();
        let row_delete_allocation = buffers.row_deletes.as_ptr();
        let descriptor = EncodedPut {
            key: BufferRange::default(),
            value: BufferRange::default(),
        };
        for _ in 0..ROW_COUNT {
            append_head_value(&mut buffers.value_bytes, &tombstone)
                .expect("append planned cascade tombstone");
            buffers.row_puts.push(descriptor);
            buffers.row_deletes.push(BufferRange::default());
        }

        assert_eq!(
            buffers.value_bytes.len(),
            ROW_COUNT * (HEAD_VALUE_HEADER_BYTES + WORKING_DIFF_VERSION_BYTES)
        );
        assert_eq!(buffers.value_bytes.as_ptr(), value_allocation);
        assert_eq!(buffers.row_puts.as_ptr(), row_put_allocation);
        assert_eq!(buffers.row_deletes.as_ptr(), row_delete_allocation);
        assert!(buffers.row_puts.capacity() >= ROW_COUNT);
        assert!(buffers.row_deletes.capacity() >= ROW_COUNT);
    }

    #[tokio::test]
    async fn ordinary_incremental_import_skips_file_cascade_identity_index() {
        const DELTAS: usize = 4096;

        let storage = StorageAdapter::new(Memory::new());
        let read = crate::storage_adapter::SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("open ordinary incremental import read"),
        );
        let entity_pk = EntityPk::single("ordinary");
        let timestamp = timestamp();
        let delta = CurrentStateDeltaRef {
            schema_key: "ordinary_schema",
            file_id: Some("ordinary.json"),
            entity_pk: &entity_pk,
            change_id: None,
            commit_id: None,
            untracked: true,
            deleted: false,
            created_at: timestamp,
            updated_at: timestamp,
            snapshot: JsonSlotRef::Inline("{}"),
            metadata: JsonSlotRef::None,
        };
        let deltas = vec![&delta; DELTAS];
        let generation = CommitId::for_test_label("ordinary-import-generation");
        let mut writes = StorageWriteSet::new();
        let mut coverage = WorkingDiffIndexCoverage::default();
        let mut retired_untracked_json_refs = BTreeSet::new();
        let explicit_index_builds = incremental_cascade_explicit_index_builds();

        stage_incremental_file_delete_cascades(
            &read,
            &mut writes,
            "ordinary-import",
            generation,
            &deltas,
            None,
            false,
            &mut coverage,
            &mut retired_untracked_json_refs,
        )
        .await
        .expect("ordinary imports do not need file-delete cascade staging");

        assert_eq!(
            incremental_cascade_explicit_index_builds(),
            explicit_index_builds,
            "ordinary imports must return before allocating the batch-sized explicit identity index"
        );
        assert!(writes.is_empty());
    }

    #[tokio::test]
    async fn dense_mutation_identity_range_scan_matches_point_reads() {
        let storage = StorageAdapter::new(Memory::new());
        let generation = CommitId::for_test_label("dense-mutation-generation");
        let entity_pks = (0..HOT_DENSE_SCAN_MIN_IDENTITIES)
            .map(|index| EntityPk::single(format!("{index:04}")))
            .collect::<Vec<_>>();
        let timestamp = timestamp();
        let deltas = entity_pks
            .iter()
            .map(|entity_pk| CurrentStateDeltaRef {
                schema_key: "schema",
                file_id: None,
                entity_pk,
                change_id: None,
                commit_id: None,
                untracked: true,
                deleted: false,
                created_at: timestamp,
                updated_at: timestamp,
                snapshot: JsonSlotRef::Inline("{}"),
                metadata: JsonSlotRef::None,
            })
            .collect::<Vec<_>>();
        let delta_refs = deltas.iter().collect::<Vec<_>>();
        let encoded = encode_hot_mutation_identities("branch", generation, &delta_refs);
        let keys = encoded
            .key_ranges
            .iter()
            .map(|ranges| {
                let start = ranges.row_key.offset();
                StorageKey(
                    encoded
                        .key_bytes
                        .slice(start..start.saturating_add(ranges.row_key.len())),
                )
            })
            .collect::<Vec<_>>();
        let mut writes = StorageWriteSet::new();
        for key in &keys {
            writes.put(
                HOT_ROW_SPACE,
                key.clone(),
                StorageValue {
                    bytes: Bytes::from_static(b"row"),
                },
            );
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit dense mutation fixture");
        let read = crate::storage_adapter::SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("open dense mutation read"),
        );

        let dense = hot_scan_dense_mutation_identity_range(&read, &encoded)
            .await
            .expect("scan dense mutation range")
            .expect("dense mutation range should stay on the scan path");
        let point = PointReadPlan::new(HOT_ROW_SPACE, &keys)
            .materialize(&read, StorageGetOptions::default())
            .await
            .expect("point-read dense mutation identities")
            .value
            .into_iter()
            .map(|value| value.map(full_value_bytes).transpose())
            .collect::<Result<Vec<_>, _>>()
            .expect("decode dense mutation point reads");

        assert_eq!(dense, point);
        assert_eq!(
            dense.iter().flatten().count(),
            HOT_DENSE_SCAN_MIN_IDENTITIES
        );
    }

    #[tokio::test]
    async fn dense_identity_range_scan_returns_requested_rows() {
        let storage = StorageAdapter::new(Memory::new());
        let generation = CommitId::for_test_label("dense-range-generation");
        let all_identities = (0..HOT_DENSE_SCAN_MIN_IDENTITIES * 2)
            .map(|index| diff_identity("branch", generation, &format!("{index:04}")))
            .collect::<Vec<_>>();
        let requested = all_identities
            .iter()
            .step_by(2)
            .cloned()
            .collect::<Vec<_>>();
        let requested_batch = FiniteHotIdentityBatchRef::new(
            "branch",
            generation,
            "schema",
            requested
                .iter()
                .map(|identity| &identity.entity_pk)
                .collect(),
            vec![None],
        )
        .expect("dense identity count is representable");
        let mut writes = StorageWriteSet::new();
        for identity in &all_identities {
            writes.put(
                HOT_ROW_SPACE,
                StorageKey(Bytes::from(encode_hot_row_key(identity))),
                StorageValue {
                    bytes: Bytes::from_static(b"row"),
                },
            );
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit dense-range fixture");
        let read = crate::storage_adapter::SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("open dense-range read"),
        );

        let dense = hot_scan_dense_identity_range(&read, &requested_batch)
            .await
            .expect("scan dense identity range")
            .expect("dense range should stay on the scan path");
        let point = hot_load_finite_identity_bytes(&read, &requested_batch)
            .await
            .expect("point-read the same dense identities");

        assert_eq!(dense, point);
        assert_eq!(dense.iter().flatten().count(), requested.len());
    }

    #[tokio::test]
    async fn sparse_identity_range_scan_returns_to_point_reads() {
        let storage = StorageAdapter::new(Memory::new());
        let generation = CommitId::for_test_label("sparse-range-generation");
        let all_identities = (0..HOT_DENSE_SCAN_MIN_IDENTITIES * 4)
            .map(|index| diff_identity("branch", generation, &format!("{index:04}")))
            .collect::<Vec<_>>();
        let requested = all_identities
            .iter()
            .step_by(4)
            .cloned()
            .collect::<Vec<_>>();
        let requested_batch = FiniteHotIdentityBatchRef::new(
            "branch",
            generation,
            "schema",
            requested
                .iter()
                .map(|identity| &identity.entity_pk)
                .collect(),
            vec![None],
        )
        .expect("sparse identity count is representable");
        let mut writes = StorageWriteSet::new();
        for identity in &all_identities {
            writes.put(
                HOT_ROW_SPACE,
                StorageKey(Bytes::from(encode_hot_row_key(identity))),
                StorageValue {
                    bytes: Bytes::from_static(b"row"),
                },
            );
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit sparse-range fixture");
        let read = crate::storage_adapter::SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("open sparse-range read"),
        );

        let rows = hot_scan_dense_identity_range(&read, &requested_batch)
            .await
            .expect("probe sparse identity range");

        assert!(
            rows.is_none(),
            "sparse ranges must return to the exact point-read path"
        );
        let point = hot_load_finite_identity_bytes(&read, &requested_batch)
            .await
            .expect("point-read sparse identities after dense fallback");
        assert_eq!(point.iter().flatten().count(), requested.len());
    }

    #[tokio::test]
    async fn working_diff_gc_keeps_only_control_bound_hot_records() {
        let storage = StorageAdapter::new(Memory::new());
        let active_generation = CommitId::for_test_label("active-generation");
        let active_checkpoint = CommitId::for_test_label("active-checkpoint");
        let stale_generation = CommitId::for_test_label("stale-generation");
        let stale_checkpoint = CommitId::for_test_label("stale-checkpoint");
        let orphan_generation = CommitId::for_test_label("orphan-generation");
        let orphan_checkpoint = CommitId::for_test_label("orphan-checkpoint");

        let active_identity = diff_identity("active", active_generation, "active-row");
        let stale_identity = diff_identity("stale", stale_generation, "stale-row");
        let orphan_identity = diff_identity("deleted", orphan_generation, "orphan-row");
        let (active_key, active_value) =
            single_hot_diff_segment(active_checkpoint, &active_identity);
        let (stale_key, stale_value) = single_hot_diff_segment(stale_checkpoint, &stale_identity);
        let (orphan_key, orphan_value) =
            single_hot_diff_segment(orphan_checkpoint, &orphan_identity);

        let mut writes = StorageWriteSet::new();
        stage_tracked_working_diff_epoch(
            &mut writes,
            "active",
            TrackedWorkingDiffEpoch {
                checkpoint_commit_id: active_checkpoint,
                generation: active_generation,
                coverage: WorkingDiffIndexCoverage::default(),
            },
        )
        .expect("stage active epoch");
        stage_branch_head_control(
            &mut writes,
            "active",
            BranchHeadControl {
                head_commit_id: active_generation,
                generation: active_generation,
                current_state_revision: 0,
                schema_presence_bloom: [u64::MAX; 4],
                working_diff_checkpoint_commit_id: Some(active_checkpoint),
                created_at: timestamp(),
                updated_at: timestamp(),
                ref_change_id: ChangeId::for_test_label("active-ref"),
            },
        )
        .expect("stage active control");

        stage_tracked_working_diff_epoch(
            &mut writes,
            "stale",
            TrackedWorkingDiffEpoch {
                checkpoint_commit_id: stale_checkpoint,
                generation: stale_generation,
                coverage: WorkingDiffIndexCoverage::default(),
            },
        )
        .expect("stage stale epoch");
        stage_branch_head_control(
            &mut writes,
            "stale",
            BranchHeadControl {
                head_commit_id: stale_generation,
                generation: stale_generation,
                current_state_revision: 0,
                schema_presence_bloom: [u64::MAX; 4],
                working_diff_checkpoint_commit_id: None,
                created_at: timestamp(),
                updated_at: timestamp(),
                ref_change_id: ChangeId::for_test_label("stale-ref"),
            },
        )
        .expect("stage stale control");

        stage_tracked_working_diff_epoch(
            &mut writes,
            "deleted",
            TrackedWorkingDiffEpoch {
                checkpoint_commit_id: orphan_checkpoint,
                generation: orphan_generation,
                coverage: WorkingDiffIndexCoverage::default(),
            },
        )
        .expect("stage orphan epoch");

        for (key, value) in [
            (&active_key, active_value),
            (&stale_key, stale_value),
            (&orphan_key, orphan_value),
        ] {
            writes.put(
                HOT_DIFF_SPACE,
                StorageKey(Bytes::copy_from_slice(key)),
                StorageValue {
                    bytes: Bytes::from(value),
                },
            );
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit hot working-diff GC fixture");

        let read = crate::storage_adapter::SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("open hot working-diff GC read"),
        );
        let mut gc_writes = StorageWriteSet::new();
        stage_collect_stale_working_diff_indexes(&read, &mut gc_writes)
            .await
            .expect("stage hot working-diff GC");
        drop(read);
        storage
            .commit_write_set(gc_writes, StorageWriteOptions::default())
            .await
            .expect("commit hot working-diff GC");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open hot working-diff GC verification read");
        let active_epoch = PointReadPlan::new(
            TRACKED_WORKING_DIFF_MARKER_SPACE,
            &[StorageKey(Bytes::from(
                working_diff_marker_key("active").expect("active working-diff marker key"),
            ))],
        )
        .materialize(&read, StorageGetOptions::default())
        .await
        .expect("read active epoch")
        .value
        .into_iter()
        .next()
        .flatten();
        assert!(active_epoch.is_some(), "active epoch must survive GC");

        for (branch_id, key) in [("stale", stale_key), ("deleted", orphan_key)] {
            let epoch = PointReadPlan::new(
                TRACKED_WORKING_DIFF_MARKER_SPACE,
                &[StorageKey(Bytes::from(
                    working_diff_marker_key(branch_id).expect("working-diff marker key"),
                ))],
            )
            .materialize(&read, StorageGetOptions::default())
            .await
            .expect("read stale epoch")
            .value
            .into_iter()
            .next()
            .flatten();
            assert!(epoch.is_none(), "inactive epoch must be reclaimed");
            let record = PointReadPlan::new(HOT_DIFF_SPACE, &[StorageKey(Bytes::from(key))])
                .materialize(&read, StorageGetOptions::default())
                .await
                .expect("read stale hot record")
                .value
                .into_iter()
                .next()
                .flatten();
            assert!(record.is_none(), "inactive hot record must be reclaimed");
        }

        let active_record =
            PointReadPlan::new(HOT_DIFF_SPACE, &[StorageKey(Bytes::from(active_key))])
                .materialize(&read, StorageGetOptions::default())
                .await
                .expect("read active hot record")
                .value
                .into_iter()
                .next()
                .flatten();
        assert!(active_record.is_some(), "active hot record must survive GC");
    }
}
