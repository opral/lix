#![allow(
    clippy::cast_possible_truncation,
    clippy::clone_on_copy,
    clippy::match_same_arms,
    clippy::needless_pass_by_ref_mut,
    clippy::redundant_closure_for_method_calls,
    clippy::unnecessary_mut_passed,
    clippy::unnecessary_wraps
)]

use std::collections::{BTreeMap, BTreeSet, HashSet};

use crate::changelog::CommitId;
use crate::changelog::{ChangeId, ChangeRecordProjection};
use crate::storage_adapter::StorageAdapterRead;
use crate::tracked_state::TrackedStateScanRequest;
use crate::tracked_state::codec::{TrackedStateKeyBatchBuilder, decode_key_shared};
use crate::tracked_state::diff::{
    TrackedStateArrowDiffBatch, TrackedStateArrowDiffBatchBuilder, TrackedStateDiff,
    TrackedStateDiffIdentity, TrackedStateDiffRequest, TrackedStatePayloadBatch, diff_commits,
};
use crate::tracked_state::storage;
use crate::tracked_state::types::{
    TrackedStateIndexValue, TrackedStateKey, TrackedStateKeyRef, TrackedStatePhysicalScanRequest,
};
use crate::tracked_state::{
    MaterializedTrackedStateBatch, MaterializedTrackedStateExactBatch,
    materialize_batch_from_arrow_rows,
};
use crate::{LixError, NullableKeyFilter};
use bytes::Bytes;

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";
/// Factory for commit-addressed Arrow state readers.
///
/// Tracked state is stored as content-addressed roots. Branch refs
/// choose which commit/root to read; this context only owns root operations.
#[derive(Clone, Default)]
pub(crate) struct TrackedStateContext;

impl TrackedStateContext {
    pub(crate) fn new() -> Self {
        Self
    }

    /// Creates a commit-id-addressed tracked-state reader.
    pub(crate) fn reader<S>(&self, store: S) -> TrackedStateStoreReader<S>
    where
        S: StorageAdapterRead,
    {
        TrackedStateStoreReader {
            store,
            commit_delta_point_cache: storage::CommitDeltaPointReadCache::default(),
            decoded_columns: None,
        }
    }

    pub(crate) fn reader_with_decoded_columns<S>(
        &self,
        store: S,
        decoded_columns: crate::live_state::EntityDecodedColumnCache,
    ) -> TrackedStateStoreReader<S>
    where
        S: StorageAdapterRead,
    {
        TrackedStateStoreReader {
            store,
            commit_delta_point_cache: storage::CommitDeltaPointReadCache::default(),
            decoded_columns: Some(decoded_columns),
        }
    }
}

/// Store-backed tracked-state reader created by `TrackedStateContext`.
pub(crate) struct TrackedStateStoreReader<S> {
    store: S,
    /// Shares immutable event manifests across repeated history validation.
    commit_delta_point_cache: storage::CommitDeltaPointReadCache,
    decoded_columns: Option<crate::live_state::EntityDecodedColumnCache>,
}

impl<S> TrackedStateStoreReader<S>
where
    S: StorageAdapterRead,
{
    pub(crate) async fn scan_batch_at_commit(
        &mut self,
        commit_id: &str,
        request: &TrackedStateScanRequest,
    ) -> Result<MaterializedTrackedStateBatch, LixError> {
        let tree_request = physical_scan_request_from_tracked(request);
        let materialization = ChangeRecordProjection::from_columns(&request.read_columns.columns);
        let parsed_commit_id =
            CommitId::parse_lix(commit_id, "Arrow state historical scan commit_id")?;
        let manifest = storage::load_commit_state_manifest(&self.store, parsed_commit_id)
            .await?
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("tracked_state commit authority is missing for commit '{commit_id}'"),
                )
            })?;
        let rows = storage::scan_complete_current_state_rows(&self.store, &manifest, &tree_request)
            .await?;
        materialize_batch_from_arrow_rows(&self.store, rows, &materialization).await
    }

    pub(crate) async fn load_projected_batch_at_commit(
        &mut self,
        commit_id: &str,
        keys: &[TrackedStateKey],
        projection: &ChangeRecordProjection,
    ) -> Result<MaterializedTrackedStateExactBatch, LixError> {
        let key_refs = keys
            .iter()
            .map(|key| TrackedStateKeyRef {
                schema_key: key.schema_key.as_str(),
                file_id: key.file_id.as_deref(),
                entity_pk: &key.entity_pk,
            })
            .collect::<Vec<_>>();
        self.load_projected_batch_at_commit_refs(commit_id, &key_refs, projection)
            .await
    }

    pub(crate) async fn load_projected_batch_at_commit_refs(
        &mut self,
        commit_id: &str,
        keys: &[TrackedStateKeyRef<'_>],
        projection: &ChangeRecordProjection,
    ) -> Result<MaterializedTrackedStateExactBatch, LixError> {
        if keys.is_empty() {
            return Ok(MaterializedTrackedStateExactBatch::default());
        }
        if u32::try_from(keys.len()).is_err() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "exact tracked-state request exceeds the batch ordinal range",
            ));
        }

        // Sort compact input ordinals instead of building one heap-owned
        // `Vec<usize>` per distinct key. Duplicates retain their original
        // positions through `unique_ordinal_by_input`.
        let mut ordered_indices = (0..keys.len()).collect::<Vec<_>>();
        ordered_indices.sort_unstable_by(|left, right| {
            compare_tracked_state_key_refs(keys[*left], keys[*right])
        });
        let unique_key_count = 1 + ordered_indices
            .windows(2)
            .filter(|pair| {
                compare_tracked_state_key_refs(keys[pair[0]], keys[pair[1]])
                    != std::cmp::Ordering::Equal
            })
            .count();
        let mut unique_keys = Vec::with_capacity(unique_key_count);
        let mut unique_ordinal_by_input = vec![0_u32; keys.len()];
        let mut offset = 0;
        while offset < ordered_indices.len() {
            let first_index = ordered_indices[offset];
            let unique_ordinal =
                u32::try_from(unique_keys.len()).expect("request row count was bounded to u32");
            unique_keys.push(keys[first_index]);
            let mut end = offset + 1;
            while end < ordered_indices.len()
                && compare_tracked_state_key_refs(keys[ordered_indices[end]], keys[first_index])
                    == std::cmp::Ordering::Equal
            {
                end += 1;
            }
            for &input_index in &ordered_indices[offset..end] {
                unique_ordinal_by_input[input_index] = unique_ordinal;
            }
            offset = end;
        }
        debug_assert_eq!(unique_keys.len(), unique_key_count);

        let mut encoded_key_batch =
            TrackedStateKeyBatchBuilder::with_row_capacity(unique_keys.len());
        for &key in &unique_keys {
            encoded_key_batch.push(key);
        }
        let encoded_keys = encoded_key_batch.finish();
        let parsed_commit_id = CommitId::parse_lix(commit_id, "Arrow state exact-read commit_id")?;
        let manifest = storage::load_commit_state_manifest(&self.store, parsed_commit_id)
            .await?
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("tracked_state commit authority is missing for commit '{commit_id}'"),
                )
            })?;
        let arrow_rows = storage::load_complete_current_state_rows_with_coordinates_encoded_cached(
            &self.store,
            &manifest,
            &encoded_keys,
            self.decoded_columns.as_ref(),
        )
        .await?;
        if arrow_rows.len() != unique_keys.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "exact tracked-state read returned {} Arrow rows for {} unique keys",
                    arrow_rows.len(),
                    unique_keys.len()
                ),
            ));
        }
        let mut present_rows = Vec::with_capacity(arrow_rows.iter().flatten().count());
        let mut present_ordinal_by_unique = vec![None; unique_keys.len()];
        for (unique_ordinal, row) in arrow_rows.into_iter().enumerate() {
            if let Some((row, _coordinate)) = row {
                present_ordinal_by_unique[unique_ordinal] = Some(
                    u32::try_from(present_rows.len())
                        .expect("request row count was bounded to u32"),
                );
                present_rows.push(row);
            }
        }
        let slots = unique_ordinal_by_input
            .into_iter()
            .map(|unique_ordinal| present_ordinal_by_unique[unique_ordinal as usize])
            .collect();
        let batch =
            materialize_batch_from_arrow_rows(&self.store, present_rows, projection).await?;
        MaterializedTrackedStateExactBatch::new(batch, slots)
    }

    #[cfg(any(test, feature = "storage-benches"))]
    #[cfg(test)]
    pub(crate) async fn load_batch_at_commit(
        &mut self,
        commit_id: &str,
        keys: &[TrackedStateKey],
    ) -> Result<MaterializedTrackedStateExactBatch, LixError> {
        self.load_projected_batch_at_commit(commit_id, keys, &ChangeRecordProjection::full())
            .await
    }

    pub(crate) async fn diff_commits(
        &mut self,
        left_commit_id: &str,
        right_commit_id: &str,
        request: &TrackedStateDiffRequest,
    ) -> Result<TrackedStateDiff, LixError> {
        diff_commits(self, left_commit_id, right_commit_id, request).await
    }

    /// Resolves the exact tracked identities affected by descriptor cascades.
    ///
    /// File-descriptor tombstones implicitly delete every tracked row owned by
    /// that file, but those cascade members are not authored commit-delta
    /// rows. Historical keys are ordered by schema before file ID, so a
    /// file-only scan would still walk the whole root. Build the schema
    /// inventory from the visible schema catalog and scan only the resulting
    /// `(schema_key, file_id)` prefixes at the endpoint where the file is
    /// live. If the target changes the catalog, also include both historical
    /// endpoint inventories.
    pub(crate) async fn descriptor_dependency_closure(
        &mut self,
        current_commit_id: &str,
        desired_commit_id: &str,
        dependency_commit_id: &str,
        target_delta: &[(TrackedStateKey, TrackedStateIndexValue)],
        file_ids: &[String],
        visible_schema_keys: &[String],
    ) -> Result<Vec<TrackedStateKey>, LixError> {
        let mut keys = target_delta
            .iter()
            .map(|(key, _)| key.clone())
            .collect::<BTreeSet<_>>();

        let mut schema_keys = visible_schema_keys.iter().cloned().collect::<BTreeSet<_>>();
        if target_delta
            .iter()
            .any(|(key, _)| key.schema_key == REGISTERED_SCHEMA_KEY)
        {
            schema_keys.extend(
                self.registered_schema_keys_at_commit(desired_commit_id)
                    .await?,
            );
            schema_keys.extend(
                self.registered_schema_keys_at_commit(current_commit_id)
                    .await?,
            );
        }
        let request = TrackedStateScanRequest {
            filter: crate::tracked_state::TrackedStateFilter {
                schema_keys: schema_keys.into_iter().collect(),
                file_ids: file_ids
                    .iter()
                    .cloned()
                    .map(NullableKeyFilter::Value)
                    .collect(),
                ..crate::tracked_state::TrackedStateFilter::default()
            },
            read_columns: crate::tracked_state::TrackedStateReadColumns {
                columns: vec!["change_id".to_string()],
            },
            limit: None,
        };
        let rows = self
            .scan_batch_at_commit(dependency_commit_id, &request)
            .await?;
        keys.extend(rows.iter().map(|row| TrackedStateKey {
            schema_key: row.schema_key().to_owned(),
            file_id: row.file_id().map(str::to_owned),
            entity_pk: row.entity_pk().clone(),
        }));
        Ok(keys.into_iter().collect())
    }

    async fn registered_schema_keys_at_commit(
        &mut self,
        commit_id: &str,
    ) -> Result<BTreeSet<String>, LixError> {
        let rows = self
            .scan_batch_at_commit(
                commit_id,
                &TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec![REGISTERED_SCHEMA_KEY.to_string()],
                        file_ids: vec![NullableKeyFilter::Null],
                        ..crate::tracked_state::TrackedStateFilter::default()
                    },
                    read_columns: crate::tracked_state::TrackedStateReadColumns {
                        columns: vec!["change_id".to_string()],
                    },
                    limit: None,
                },
            )
            .await?;
        rows.iter()
            .map(|row| {
                row.entity_pk().as_single_string_owned().map_err(|error| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("registered schema dependency identity is invalid: {error}"),
                    )
                })
            })
            .collect()
    }

    /// Loads the identities and index values authored or selected by exactly
    /// one commit, without resolving inherited tracked state.
    pub(crate) async fn commit_delta_members(
        &mut self,
        commit_id: CommitId,
    ) -> Result<Vec<(TrackedStateKey, TrackedStateIndexValue)>, LixError> {
        storage::scan_commit_delta_members(&self.store, commit_id).await
    }

    /// Loads only the requested schema ranges from one commit delta. Semantic
    /// classification uses this to avoid hydrating unrelated commit members.
    pub(crate) async fn commit_delta_values_for_schemas(
        &mut self,
        commit_id: CommitId,
        schema_keys: &[String],
    ) -> Result<storage::DecodedCommitDeltaBatch, LixError> {
        self.scan_commit_event_values(commit_id, schema_keys).await
    }

    pub(crate) async fn diff_arrow_entries_at_commits(
        &mut self,
        left_commit_id: &str,
        right_commit_id: &str,
        request: &TrackedStatePhysicalScanRequest,
    ) -> Result<TrackedStateArrowDiffBatch, LixError> {
        if left_commit_id == right_commit_id {
            return Ok(TrackedStateArrowDiffBatch::default());
        }
        let left_parsed = CommitId::parse_lix(left_commit_id, "Arrow diff left commit_id")?;
        let left_manifest = storage::load_commit_state_manifest(&self.store, left_parsed)
            .await?
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "tracked_state commit authority is missing for commit '{left_commit_id}'"
                    ),
                )
            })?;
        let right_parsed = CommitId::parse_lix(right_commit_id, "Arrow diff right commit_id")?;
        let right_manifest = storage::load_commit_state_manifest(&self.store, right_parsed)
            .await?
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "tracked_state commit authority is missing for commit '{right_commit_id}'"
                    ),
                )
            })?;
        if left_manifest.current_state_catalog.root_id
            == right_manifest.current_state_catalog.root_id
        {
            return Ok(TrackedStateArrowDiffBatch::default());
        }
        let (left, right) = storage::diff_complete_current_state_leaf_rows(
            &self.store,
            &left_manifest,
            &right_manifest,
            request,
        )
        .await?;
        let mut entries = TrackedStateArrowDiffBatchBuilder::with_row_capacity(
            left.len().saturating_add(right.len()),
        );
        let mut payloads =
            BTreeMap::<ChangeId, (crate::json_store::JsonSlot, crate::json_store::JsonSlot)>::new();
        let mut left_ordinal = 0usize;
        let mut right_ordinal = 0usize;
        while left_ordinal < left.len() || right_ordinal < right.len() {
            let ordering = match (left_ordinal < left.len(), right_ordinal < right.len()) {
                (true, true) => left[left_ordinal]
                    .encoded_key
                    .cmp(&right[right_ordinal].encoded_key),
                (true, false) => std::cmp::Ordering::Less,
                (false, true) => std::cmp::Ordering::Greater,
                (false, false) => break,
            };
            let (encoded_key, before, after, before_payload, after_payload) = match ordering {
                std::cmp::Ordering::Less => {
                    let ordinal = left_ordinal;
                    left_ordinal += 1;
                    (
                        Bytes::from(left[ordinal].encoded_key.clone()),
                        Some(left[ordinal].value.clone()),
                        None,
                        Some((
                            left[ordinal].snapshot.clone(),
                            left[ordinal].metadata.clone(),
                        )),
                        None,
                    )
                }
                std::cmp::Ordering::Greater => {
                    let ordinal = right_ordinal;
                    right_ordinal += 1;
                    (
                        Bytes::from(right[ordinal].encoded_key.clone()),
                        None,
                        Some(right[ordinal].value.clone()),
                        None,
                        Some((
                            right[ordinal].snapshot.clone(),
                            right[ordinal].metadata.clone(),
                        )),
                    )
                }
                std::cmp::Ordering::Equal => {
                    let left_value = left[left_ordinal].value.clone();
                    let right_value = right[right_ordinal].value.clone();
                    let encoded_key = Bytes::from(left[left_ordinal].encoded_key.clone());
                    let left_payload = (
                        left[left_ordinal].snapshot.clone(),
                        left[left_ordinal].metadata.clone(),
                    );
                    let right_payload = (
                        right[right_ordinal].snapshot.clone(),
                        right[right_ordinal].metadata.clone(),
                    );
                    left_ordinal += 1;
                    right_ordinal += 1;
                    (
                        encoded_key,
                        Some(left_value),
                        Some(right_value),
                        Some(left_payload),
                        Some(right_payload),
                    )
                }
            };
            if before == after {
                continue;
            }
            let key = decode_key_shared(encoded_key)?;
            if request.matches_key_ref(key.as_ref()) {
                for (value, payload) in [
                    (before.as_ref(), before_payload),
                    (after.as_ref(), after_payload),
                ] {
                    let (Some(value), Some(payload)) = (value, payload) else {
                        continue;
                    };
                    if let Some(existing) = payloads.insert(value.change_id, payload.clone())
                        && existing != payload
                    {
                        return Err(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!(
                                "Arrow state change '{}' resolves to conflicting leaf payloads",
                                value.change_id
                            ),
                        ));
                    }
                }
                entries.push_shared(key, before, after);
            }
        }
        let payloads = TrackedStatePayloadBatch::from_payloads(
            payloads
                .into_iter()
                .map(|(change_id, (snapshot, metadata))| (change_id, snapshot, metadata)),
        )?;
        Ok(entries.finish()?.with_payloads(payloads))
    }

    /// Returns the compact write-set union for an ancestor/descendant
    /// first-parent interval.
    ///
    /// Stale transaction admission needs to know whether a prepared identity
    /// was touched, not the endpoint payload or before value. Reading the
    /// immutable per-commit delta generations directly avoids tree diffing,
    /// ancestor point reads, and changelog payload hydration. `None` means the
    /// pair is not representable by the compact first-parent event journal and
    /// the caller must use the general structural Arrow diff.
    pub(crate) async fn changed_identities_in_first_parent_interval(
        &mut self,
        ancestor_commit_id: &str,
        descendant_commit_id: &str,
    ) -> Result<Option<Vec<TrackedStateDiffIdentity>>, LixError> {
        let Some(interval) = self
            .first_parent_interval_between(ancestor_commit_id, descendant_commit_id)
            .await?
        else {
            return Ok(None);
        };
        let mut batches = Vec::with_capacity(interval.len());
        for commit_id in interval {
            batches.push(self.scan_commit_event_values(commit_id, &[]).await?);
        }
        let row_count = batches
            .iter()
            .try_fold(0usize, |count, batch| count.checked_add(batch.len()))
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "first-parent write-set row count overflows usize",
                )
            })?;
        let mut seen = HashSet::with_capacity(row_count);
        let mut keys = Vec::with_capacity(row_count);
        for batch in &batches {
            for row in batch.iter() {
                let key = row.key_ref();
                if seen.insert(key) {
                    keys.push(key);
                }
            }
        }
        keys.sort_unstable();
        Ok(Some(TrackedStateDiffIdentity::from_key_refs(
            keys.len(),
            |index| keys[index],
        )?))
    }

    /// Diffs an ancestor/descendant pair from immutable per-commit deltas.
    ///
    /// Merge always compares its merge base with each head. Walking only that
    /// first-parent interval makes the common branch case proportional to the
    /// commits and identities changed since the base, rather than every entity
    /// inherited by both commits.
    async fn first_parent_interval_between(
        &mut self,
        ancestor_commit_id: &str,
        descendant_commit_id: &str,
    ) -> Result<Option<Vec<CommitId>>, LixError> {
        let ancestor =
            CommitId::parse_lix(ancestor_commit_id, "tracked-state diff ancestor commit_id")?;
        let mut current = CommitId::parse_lix(
            descendant_commit_id,
            "tracked-state diff descendant commit_id",
        )?;
        let mut interval = Vec::new();
        let mut seen = HashSet::new();
        loop {
            if current == ancestor {
                return Ok(Some(interval));
            }
            if !seen.insert(current) {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "cannot diff tracked_state commits: first-parent cycle includes '{current}'"
                    ),
                ));
            }
            interval.push(current);
            let manifest = storage::load_commit_state_manifest(&self.store, current)
                .await?
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("tracked_state commit authority is missing for commit '{current}'"),
                    )
                })?;
            let Some(parent_commit_id) = manifest.parent_commit_ids.first().copied() else {
                return Ok(None);
            };
            current = parent_commit_id;
        }
    }

    async fn scan_commit_event_values(
        &mut self,
        commit_id: CommitId,
        schema_keys: &[String],
    ) -> Result<storage::DecodedCommitDeltaBatch, LixError> {
        storage::scan_commit_delta_values_with_cache(
            &self.store,
            commit_id,
            schema_keys,
            Some(&self.commit_delta_point_cache),
        )
        .await
    }
}

pub(crate) fn physical_scan_request_from_tracked(
    request: &TrackedStateScanRequest,
) -> TrackedStatePhysicalScanRequest {
    TrackedStatePhysicalScanRequest {
        schema_keys: request.filter.schema_keys.clone(),
        entity_pks: request.filter.entity_pks.clone(),
        file_ids: request.filter.file_ids.clone(),
        include_tombstones: request.filter.include_tombstones,
        // User limits belong above delta overlay and tombstone visibility.
        // Pushing them into the physical tree can stop on rows that are later
        // hidden, returning too few live rows.
        limit: None,
    }
}

fn compare_tracked_state_key_refs(
    left: TrackedStateKeyRef<'_>,
    right: TrackedStateKeyRef<'_>,
) -> std::cmp::Ordering {
    left.schema_key
        .cmp(right.schema_key)
        .then_with(|| left.file_id.cmp(&right.file_id))
        .then_with(|| left.entity_pk.cmp(right.entity_pk))
}

fn file_delete_cascade(
    key: &TrackedStateKey,
    value: &TrackedStateIndexValue,
) -> Result<Option<String>, LixError> {
    file_delete_cascade_ref(
        TrackedStateKeyRef {
            schema_key: &key.schema_key,
            file_id: key.file_id.as_deref(),
            entity_pk: &key.entity_pk,
        },
        value,
    )
}

pub(crate) fn descriptor_dependency_cascade_file_ids(
    target_delta: &[(TrackedStateKey, TrackedStateIndexValue)],
) -> Result<Vec<String>, LixError> {
    let mut file_ids = BTreeSet::new();
    for (key, value) in target_delta {
        if let Some(file_id) = file_delete_cascade(key, value)? {
            file_ids.insert(file_id);
        }
    }
    Ok(file_ids.into_iter().collect())
}

fn file_delete_cascade_ref(
    key: TrackedStateKeyRef<'_>,
    value: &TrackedStateIndexValue,
) -> Result<Option<String>, LixError> {
    if key.schema_key != FILE_DESCRIPTOR_SCHEMA_KEY || !value.deleted {
        return Ok(None);
    }
    key.entity_pk
        .as_single_string_owned()
        .map(Some)
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state commit_delta file descriptor tombstone has invalid identity: {error}"
                ),
            )
        })
}
