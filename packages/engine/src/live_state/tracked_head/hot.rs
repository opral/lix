//! V15 row-addressable current state.
//!
//! V12 packed every file member of one logical entity into a group. That made
//! a logical-PK lookup cheap, but it also made every normal commit read,
//! decode, merge, and rewrite each predecessor group. V15 keeps the same
//! fixed row value codec and branch-control publication fence, while making a
//! full row identity the physical mutation unit.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use tracing::Instrument as _;

use super::*;

pub(crate) const HOT_ROW_NAMESPACE: &str = "live_state.hot_row.v15";
pub(crate) const HOT_FILE_NAMESPACE: &str = "live_state.hot_file.v15";
pub(crate) const HOT_DIFF_NAMESPACE: &str = "live_state.hot_diff.v15";
pub(crate) const HOT_ROW_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0004_001b), HOT_ROW_NAMESPACE);
/// File-id-first projection. The primary hot row remains authoritative.
pub(crate) const HOT_FILE_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0004_001c), HOT_FILE_NAMESPACE);
/// Reserved for the row-level first-before working-diff index.
pub(crate) const HOT_DIFF_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0004_001d), HOT_DIFF_NAMESPACE);
const HOT_DENSE_SCAN_MIN_IDENTITIES: usize = 64;
const HOT_DENSE_SCAN_MAX_OVERREAD: usize = 2;
const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";

/// Direct reader for one published hot generation.
pub(crate) struct HotStateStoreReader<S> {
    pub(super) store: S,
}

impl<S> HotStateStoreReader<S>
where
    S: StorageAdapterRead,
{
    pub(crate) async fn scan_live_rows(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        request: &TrackedStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        self.scan_live_rows_for_generation(branch_id, control.generation, request)
            .await
    }

    pub(crate) async fn scan_live_rows_for_controls(
        &self,
        controls: &[(String, BranchHeadControl)],
        request: &TrackedStateScanRequest,
    ) -> Result<Vec<(String, Vec<MaterializedLiveStateRow>)>, LixError> {
        let mut rows = Vec::with_capacity(controls.len());
        for (branch_id, control) in controls {
            let branch_rows = self
                .scan_live_rows_for_generation(branch_id, control.generation, request)
                .await?;
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
        Ok(!page.value.entries.is_empty())
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
            self.scan_live_rows_for_generation(branch_id, control.generation, request)
                .await?,
        ))
    }

    async fn scan_live_rows_for_generation(
        &self,
        branch_id: &str,
        generation: CommitId,
        request: &TrackedStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        // A storage prefix is ordered by identity, but tombstones are filtered
        // only after decoding the value. Applying SQL LIMIT to the raw scan
        // would therefore let one tombstone hide a later live row.
        let entries =
            hot_scan_entries(&self.store, branch_id, generation, &request.filter, None).await?;
        let projection = ChangeRecordProjection::from_columns(&request.read_columns.columns);
        let mut rows =
            materialize_live_entries(&self.store, entries, projection, branch_id).await?;
        if !request.filter.include_tombstones {
            rows.retain(|row| !row.deleted);
        }
        if let Some(limit) = request.limit {
            rows.truncate(limit);
        }
        Ok(rows)
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
            self.load_projected_live_rows_for_generation(
                branch_id,
                control.generation,
                keys,
                projection,
            )
            .await?,
        ))
    }

    pub(crate) async fn load_projected_live_rows(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        keys: &[TrackedStateKey],
        projection: &ChangeRecordProjection,
    ) -> Result<Vec<Option<MaterializedLiveStateRow>>, LixError> {
        self.load_projected_live_rows_for_generation(
            branch_id,
            control.generation,
            keys,
            projection,
        )
        .await
    }

    async fn load_projected_live_rows_for_generation(
        &self,
        branch_id: &str,
        generation: CommitId,
        keys: &[TrackedStateKey],
        projection: &ChangeRecordProjection,
    ) -> Result<Vec<Option<MaterializedLiveStateRow>>, LixError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let identities = keys
            .iter()
            .map(|key| HeadIdentity {
                branch_id: branch_id.to_string(),
                generation,
                schema_key: key.schema_key.clone(),
                entity_pk: key.entity_pk.clone(),
                file_id: key.file_id.clone(),
            })
            .collect::<Vec<_>>();
        let values = hot_load_identity_bytes(&self.store, &identities).await?;
        let entries = identities
            .iter()
            .cloned()
            .zip(values)
            .filter_map(|(identity, value)| {
                value.map(|value| (identity.into_row_identity(), value))
            })
            .collect::<Vec<_>>();
        let rows = materialize_live_entries(&self.store, entries, *projection, branch_id).await?;
        let rows_by_identity = rows
            .into_iter()
            .map(|row| {
                (
                    HeadRowIdentity {
                        schema_key: row.schema_key.clone(),
                        entity_pk: row.entity_pk.clone(),
                        file_id: row.file_id.clone(),
                    },
                    row,
                )
            })
            .collect::<BTreeMap<_, _>>();
        Ok(identities
            .iter()
            .map(|identity| {
                rows_by_identity
                    .get(&identity.clone().into_row_identity())
                    .cloned()
            })
            .collect())
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
            diff: TrackedStateDiff { entries },
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
        let entries = hot_scan_entries(
            &self.store,
            branch_id,
            generation,
            &TrackedStateFilter {
                schema_keys: vec![schema_key.to_string()],
                entity_pks: entity_pks.to_vec(),
                include_tombstones: false,
                ..TrackedStateFilter::default()
            },
            None,
        )
        .await?;
        let mut snapshots = Vec::new();
        let mut json_refs = Vec::new();
        let mut deferred = Vec::new();
        for (_, bytes) in entries {
            let value = decode_head_value(&bytes)?;
            if value.deleted {
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
    rows: BTreeMap<HeadRowIdentity, Vec<u8>>,
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
            if rows.insert(identity, encode_head_value(&value)?).is_some() {
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
        absence_guards: &BTreeSet<TrackedStateKey>,
        parent_rows: Option<Vec<MaterializedTrackedStateRow>>,
        preserved_untracked_rows: Option<Vec<MaterializedLiveStateRow>>,
        working_diff_capture_checkpoint_commit_id: Option<CommitId>,
        coverage: &mut WorkingDiffIndexCoverage,
        validated_absent_file_id: Option<&str>,
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
            true,
            validated_absent_file_id,
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
        let guarded_deltas = if absence_guards_validated {
            sorted
                .iter()
                .map(|delta| {
                    validated_absent_file_id.is_some_and(|file_id| delta.file_id == Some(file_id))
                })
                .collect::<Vec<_>>()
        } else {
            vec![false; sorted.len()]
        };
        let identities_requiring_reads = identities
            .iter()
            .zip(&guarded_deltas)
            .filter(|(_, guarded)| !**guarded)
            .map(|(identity, _)| identity)
            .collect::<Vec<_>>();
        let loaded_previous_values =
            hot_load_primary_mutation_identity_refs(self.store, &identities_requiring_reads)
                .instrument(tracing::debug_span!(
                    target: "lix_perf",
                    "lix.perf.materialization.hot.previous"
                ))
                .await?;
        let mut loaded_previous_values = loaded_previous_values.into_iter();
        let previous_values = guarded_deltas
            .iter()
            .map(|guarded| {
                if *guarded {
                    None
                } else {
                    loaded_previous_values
                        .next()
                        .expect("every unguarded hot delta has one loaded previous value")
                }
            })
            .collect::<Vec<_>>();
        debug_assert_eq!(loaded_previous_values.len(), 0);
        let mut created_ats = Vec::with_capacity(sorted.len());
        let mut retired_untracked_json_refs = BTreeSet::new();
        for (delta, previous) in sorted.iter().zip(&previous_values) {
            let Some(previous) = previous else {
                created_ats.push(delta.created_at);
                continue;
            };
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
            created_ats.push(existing.created_at);
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
        let mut diff_keys = Vec::new();
        let mut next_value_ranges = Vec::with_capacity(sorted.len());
        let mut next_value_bytes = Vec::new();
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
                let (working_diff_baseline, newly_dirty) =
                    if working_diff_capture_checkpoint_commit_id.is_some() && !delta.untracked {
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
                    let checkpoint_commit_id = working_diff_capture_checkpoint_commit_id
                        .expect("a newly dirty hot row requires an active checkpoint");
                    let key = encode_hot_diff_key_parts(
                        branch_id,
                        checkpoint_commit_id,
                        generation,
                        delta.schema_key,
                        delta.entity_pk,
                        delta.file_id,
                    );
                    next_coverage.add_encoded_group_key(&key).ok_or_else(|| {
                        head_value_error("hot working-diff index count exceeds u64")
                    })?;
                    diff_keys.push(key);
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
        let next_values = next_value_ranges
            .into_iter()
            .map(|range| range.map(|range| next_value_bytes.slice(range)))
            .collect::<Vec<_>>();

        let _stage_span = tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.materialization.hot.stage"
        )
        .entered();
        self.writes.reserve_space(
            HOT_ROW_SPACE,
            next_values.iter().filter(|value| value.is_some()).count(),
            next_values.iter().filter(|value| value.is_none()).count(),
        );
        self.writes.reserve_space(
            HOT_FILE_SPACE,
            identities
                .iter()
                .zip(&next_values)
                .filter(|(identity, value)| identity.file_key.is_some() && value.is_some())
                .count(),
            identities
                .iter()
                .zip(&next_values)
                .filter(|(identity, value)| identity.file_key.is_some() && value.is_none())
                .count(),
        );
        self.writes
            .reserve_space(HOT_DIFF_SPACE, diff_keys.len(), 0);
        for key in diff_keys {
            self.writes.put(
                HOT_DIFF_SPACE,
                StorageKey(Bytes::from(key)),
                StorageValue {
                    bytes: Bytes::new(),
                },
            );
        }
        for (identity, value) in identities.iter().zip(next_values) {
            stage_hot_mutation_value(self.writes, identity, value);
        }
        stage_incremental_file_delete_cascades(
            self.store,
            self.writes,
            branch_id,
            generation,
            &sorted,
            working_diff_capture_checkpoint_commit_id,
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
            if !decode_head_value(bytes)?.untracked {
                final_tracked.insert(identity.clone(), bytes.clone());
            }
        }

        self.writes.reserve_space(HOT_ROW_SPACE, rows.len(), 0);
        self.writes.reserve_space(
            HOT_FILE_SPACE,
            rows.keys()
                .filter(|identity| identity.file_id.is_some())
                .count(),
            0,
        );
        for (identity, bytes) in rows {
            let full = HeadIdentity {
                branch_id: branch_id.to_string(),
                generation,
                schema_key: identity.schema_key,
                entity_pk: identity.entity_pk,
                file_id: identity.file_id,
            };
            stage_hot_value(self.writes, &full, Some(bytes));
        }
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
    coverage: &mut WorkingDiffIndexCoverage,
    retired_untracked_json_refs: &mut BTreeSet<[u8; JSON_REF_BYTES]>,
) -> Result<(), LixError> {
    let explicit = deltas
        .iter()
        .map(|delta| HeadRowIdentity {
            schema_key: delta.schema_key.to_string(),
            entity_pk: delta.entity_pk.clone(),
            file_id: delta.file_id.map(str::to_string),
        })
        .collect::<BTreeSet<_>>();
    let mut cascades = BTreeMap::<String, &CurrentStateDeltaRef<'_>>::new();
    for cascade in deltas {
        let Some(file_id) = file_delete_cascade_id(cascade)? else {
            continue;
        };
        cascades.insert(file_id.to_string(), cascade);
    }
    if cascades.is_empty() {
        return Ok(());
    }
    let identities =
        hot_load_file_scope_identities(store, branch_id, generation, &cascades).await?;
    let values = hot_load_primary_identity_bytes(store, &identities).await?;
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
                    .expect("file projection identity requires file id"),
            )
            .expect("file scan only returns requested cascade ids");
        let Some(previous) = previous else {
            return Err(head_value_error(
                "hot file projection has no authoritative primary row",
            ));
        };
        let existing = decode_head_value(&previous)?;
        if (cascade.untracked && !existing.untracked) || existing.deleted {
            continue;
        }
        let encoded = encode_hot_mutation_identity(
            branch_id,
            generation,
            &identity.schema_key,
            &identity.entity_pk,
            identity.file_id.as_deref(),
        );
        if existing.untracked {
            collect_hot_untracked_refs(existing, retired_untracked_json_refs);
            stage_hot_mutation_value(writes, &encoded, None);
            continue;
        }
        let (baseline, newly_dirty) = next_cascade_working_diff_baseline(
            working_diff_capture_checkpoint_commit_id,
            existing,
        )?;
        if newly_dirty {
            let checkpoint_commit_id = working_diff_capture_checkpoint_commit_id
                .expect("new cascade dirty row requires active checkpoint");
            let key = encode_hot_diff_key_parts(
                branch_id,
                checkpoint_commit_id,
                generation,
                &identity.schema_key,
                &identity.entity_pk,
                identity.file_id.as_deref(),
            );
            coverage
                .add_encoded_group_key(&key)
                .ok_or_else(|| head_value_error("hot working-diff index count exceeds u64"))?;
            writes.put(
                HOT_DIFF_SPACE,
                StorageKey(Bytes::from(key)),
                StorageValue {
                    bytes: Bytes::new(),
                },
            );
        }
        let value = encode_head_value(&HeadValueRef {
            change_id: cascade.change_id,
            commit_id: cascade.commit_id,
            untracked: false,
            deleted: true,
            created_at: existing.created_at,
            updated_at: cascade.updated_at,
            snapshot: JsonSlotRef::None,
            metadata: JsonSlotRef::None,
            working_diff_baseline: baseline,
        })?;
        stage_hot_mutation_value(writes, &encoded, Some(value.into()));
    }
    Ok(())
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
) -> Result<BTreeMap<HeadRowIdentity, Vec<u8>>, LixError> {
    let entries = hot_scan_entries(
        store,
        branch_id,
        generation,
        &TrackedStateFilter {
            include_tombstones: true,
            ..TrackedStateFilter::default()
        },
        None,
    )
    .await?;
    let mut rows = BTreeMap::new();
    for (identity, bytes) in entries {
        let value = decode_head_value(&bytes)?;
        if !value.untracked {
            continue;
        }
        if value.deleted {
            return Err(head_value_error(
                "untracked hot row must be physically removed rather than tombstoned",
            ));
        }
        if rows.insert(identity.clone(), bytes.to_vec()).is_some() {
            return Err(LixError::new(
                LixError::CODE_UNIQUE,
                format!(
                    "hot generation contains duplicate untracked identity in schema '{}' entity_pk {:?}",
                    identity.schema_key, identity.entity_pk
                ),
            ));
        }
    }
    Ok(rows)
}

fn merge_final_untracked_rows(
    rows: &mut BTreeMap<HeadRowIdentity, Vec<u8>>,
    untracked_rows: BTreeMap<HeadRowIdentity, Vec<u8>>,
) -> Result<(), LixError> {
    for (identity, bytes) in untracked_rows {
        if rows.insert(identity.clone(), bytes).is_some() {
            return Err(LixError::new(
                LixError::CODE_UNIQUE,
                format!(
                    "cannot materialize tracked and untracked hot rows with the same identity in schema '{}' entity_pk {:?}",
                    identity.schema_key, identity.entity_pk
                ),
            ));
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
    rows: &mut BTreeMap<HeadRowIdentity, Vec<u8>>,
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
    let previous = rows.get(&identity).map(Vec::as_slice);
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
            encode_head_value(&delta.value_ref(created_at, WorkingDiffBaseline::Disabled))?,
        );
    }
    Ok(())
}

fn apply_complete_file_delete_cascade(
    rows: &mut BTreeMap<HeadRowIdentity, Vec<u8>>,
    delta: &CurrentStateDeltaRef<'_>,
    retired_untracked_json_refs: &mut BTreeSet<[u8; JSON_REF_BYTES]>,
) -> Result<(), LixError> {
    let Some(file_id) = file_delete_cascade_id(delta)? else {
        return Ok(());
    };
    let identities = rows
        .keys()
        .filter(|identity| identity.file_id.as_deref() == Some(file_id))
        .cloned()
        .collect::<Vec<_>>();
    for identity in identities {
        let Some(previous) = rows.get(&identity) else {
            continue;
        };
        let existing = decode_head_value(previous)?;
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
            encode_head_value(&HeadValueRef {
                change_id: delta.change_id,
                commit_id: delta.commit_id,
                untracked: false,
                deleted: true,
                created_at: existing.created_at,
                updated_at: delta.updated_at,
                snapshot: JsonSlotRef::None,
                metadata: JsonSlotRef::None,
                working_diff_baseline: WorkingDiffBaseline::Disabled,
            })?,
        );
    }
    Ok(())
}

fn file_delete_cascade_id<'a>(
    delta: &'a CurrentStateDeltaRef<'_>,
) -> Result<Option<&'a str>, LixError> {
    if delta.schema_key != FILE_DESCRIPTOR_SCHEMA_KEY || delta.file_id.is_some() || !delta.deleted {
        return Ok(None);
    }
    delta
        .entity_pk
        .as_single_string()
        .map(Some)
        .map_err(|error| {
            head_value_error(&format!(
                "file descriptor tombstone has invalid identity: {error}"
            ))
        })
}

fn normalize_complete_hot_snapshot_baselines(
    rows: &mut BTreeMap<HeadRowIdentity, Vec<u8>>,
    tracked_baseline: WorkingDiffBaseline,
) -> Result<(), LixError> {
    for bytes in rows.values_mut() {
        let value = decode_head_value(bytes)?;
        if value.untracked {
            continue;
        }
        *bytes = reencode_head_value_with_baseline(value, tracked_baseline)?;
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

struct EncodedHotMutationIdentity {
    row_key: Bytes,
    file_key: Option<Bytes>,
}

fn encode_hot_mutation_identity(
    branch_id: &str,
    generation: CommitId,
    schema_key: &str,
    entity_pk: &EntityPk,
    file_id: Option<&str>,
) -> EncodedHotMutationIdentity {
    EncodedHotMutationIdentity {
        row_key: Bytes::from(encode_hot_row_key_parts(
            branch_id, generation, schema_key, entity_pk, file_id,
        )),
        file_key: file_id.map(|_| {
            Bytes::from(encode_hot_file_key_parts(
                branch_id, generation, schema_key, entity_pk, file_id,
            ))
        }),
    }
}

fn encode_hot_mutation_identities(
    branch_id: &str,
    generation: CommitId,
    deltas: &[&CurrentStateDeltaRef<'_>],
) -> Vec<EncodedHotMutationIdentity> {
    let scope = hot_scope_prefix(branch_id, generation);
    let mut encoded = Vec::new();
    let mut ranges = Vec::with_capacity(deltas.len());
    for delta in deltas {
        let row_start = encoded.len();
        encoded.extend_from_slice(&scope);
        write_key_string(&mut encoded, delta.schema_key, KEY_PART_FINAL);
        write_entity_pk(&mut encoded, delta.entity_pk);
        write_file_id(&mut encoded, delta.file_id);
        let row_range = row_start..encoded.len();

        let file_range = delta.file_id.map(|file_id| {
            let file_start = encoded.len();
            encoded.extend_from_slice(&scope);
            write_key_string(&mut encoded, delta.schema_key, KEY_PART_FINAL);
            write_file_id(&mut encoded, Some(file_id));
            write_entity_pk(&mut encoded, delta.entity_pk);
            file_start..encoded.len()
        });
        ranges.push((row_range, file_range));
    }
    let encoded = Bytes::from(encoded);
    ranges
        .into_iter()
        .map(|(row_range, file_range)| EncodedHotMutationIdentity {
            row_key: encoded.slice(row_range),
            file_key: file_range.map(|range| encoded.slice(range)),
        })
        .collect()
}

async fn hot_load_primary_mutation_identity_refs(
    store: &(impl StorageAdapterRead + ?Sized),
    identities: &[&EncodedHotMutationIdentity],
) -> Result<Vec<Option<Bytes>>, LixError> {
    if identities.is_empty() {
        return Ok(Vec::new());
    }
    let keys = identities
        .iter()
        .map(|identity| StorageKey(identity.row_key.clone()))
        .collect::<Vec<_>>();
    PointReadPlan::new(HOT_ROW_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?
        .value
        .into_iter()
        .map(|value| value.map(full_value_bytes).transpose())
        .collect()
}

fn stage_hot_mutation_value(
    writes: &mut StorageWriteSet,
    identity: &EncodedHotMutationIdentity,
    value: Option<Bytes>,
) {
    let Some(value) = value else {
        writes.delete(HOT_ROW_SPACE, StorageKey(identity.row_key.clone()));
        if let Some(file_key) = &identity.file_key {
            writes.delete(HOT_FILE_SPACE, StorageKey(file_key.clone()));
        }
        return;
    };
    if let Some(file_key) = &identity.file_key {
        writes.put(
            HOT_ROW_SPACE,
            StorageKey(identity.row_key.clone()),
            StorageValue {
                bytes: value.clone(),
            },
        );
        writes.put(
            HOT_FILE_SPACE,
            StorageKey(file_key.clone()),
            StorageValue { bytes: value },
        );
    } else {
        writes.put(
            HOT_ROW_SPACE,
            StorageKey(identity.row_key.clone()),
            StorageValue { bytes: value },
        );
    }
}

fn stage_hot_value(writes: &mut StorageWriteSet, identity: &HeadIdentity, value: Option<Vec<u8>>) {
    let Some(value) = value else {
        writes.delete(
            HOT_ROW_SPACE,
            StorageKey(Bytes::from(encode_hot_row_key(identity))),
        );
        if identity.file_id.is_some() {
            writes.delete(
                HOT_FILE_SPACE,
                StorageKey(Bytes::from(encode_hot_file_key(identity))),
            );
        }
        return;
    };
    if identity.file_id.is_some() {
        let value = Bytes::from(value);
        writes.put(
            HOT_ROW_SPACE,
            StorageKey(Bytes::from(encode_hot_row_key(identity))),
            StorageValue {
                bytes: value.clone(),
            },
        );
        writes.put(
            HOT_FILE_SPACE,
            StorageKey(Bytes::from(encode_hot_file_key(identity))),
            StorageValue { bytes: value },
        );
    } else {
        writes.put(
            HOT_ROW_SPACE,
            StorageKey(Bytes::from(encode_hot_row_key(identity))),
            StorageValue {
                bytes: Bytes::from(value),
            },
        );
    }
}

#[cfg(test)]
pub(super) fn stage_test_hot_value(
    writes: &mut StorageWriteSet,
    identity: &HeadIdentity,
    value: &HeadValue,
) -> Result<(), LixError> {
    stage_hot_value(writes, identity, Some(encode_head_value(&value.as_ref())?));
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
    let mut rows = BTreeMap::<HeadRowIdentity, Vec<u8>>::new();
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
        if rows.insert(identity, encode_head_value(&value)?).is_some() {
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
        if rows.insert(identity, encode_head_value(&value)?).is_some() {
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
        let previous = rows.get(&identity).map(Vec::as_slice);
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
                encode_head_value(&delta.value_ref(
                    created_at,
                    if delta.untracked {
                        WorkingDiffBaseline::Disabled
                    } else {
                        tracked_baseline
                    },
                ))?,
            );
        }
    }
    writes.reserve_space(HOT_ROW_SPACE, rows.len(), 0);
    writes.reserve_space(
        HOT_FILE_SPACE,
        rows.keys()
            .filter(|identity| identity.file_id.is_some())
            .count(),
        0,
    );
    for (identity, bytes) in rows {
        let full = HeadIdentity {
            branch_id: branch_id.to_string(),
            generation,
            schema_key: identity.schema_key,
            entity_pk: identity.entity_pk,
            file_id: identity.file_id,
        };
        stage_hot_value(writes, &full, Some(bytes));
    }
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

async fn hot_load_identity_bytes(
    store: &(impl StorageAdapterRead + ?Sized),
    identities: &[HeadIdentity],
) -> Result<Vec<Option<Bytes>>, LixError> {
    if identities.is_empty() {
        return Ok(Vec::new());
    }
    let mut output = vec![None; identities.len()];
    for (is_file, space) in [(false, HOT_ROW_SPACE), (true, HOT_FILE_SPACE)] {
        let indices = identities
            .iter()
            .enumerate()
            .filter_map(|(index, identity)| {
                (identity.file_id.is_some() == is_file).then_some(index)
            })
            .collect::<Vec<_>>();
        if indices.is_empty() {
            continue;
        }
        let keys = indices
            .iter()
            .map(|&index| {
                let identity = &identities[index];
                StorageKey(Bytes::from(if is_file {
                    encode_hot_file_key(identity)
                } else {
                    encode_hot_row_key(identity)
                }))
            })
            .collect::<Vec<_>>();
        let values = PointReadPlan::new(space, &keys)
            .materialize(store, StorageGetOptions::default())
            .await?
            .value;
        for (index, value) in indices.into_iter().zip(values) {
            output[index] = value.map(full_value_bytes).transpose()?;
        }
    }
    Ok(output)
}

/// Loads the authoritative primary row for every identity. File-backed
/// identities normally read through `HOT_FILE_SPACE`, but mutation validation
/// and diff capture must never treat that duplicate projection as ownership.
async fn hot_load_primary_identity_bytes(
    store: &(impl StorageAdapterRead + ?Sized),
    identities: &[HeadIdentity],
) -> Result<Vec<Option<Bytes>>, LixError> {
    if identities.is_empty() {
        return Ok(Vec::new());
    }
    let keys = identities
        .iter()
        .map(|identity| StorageKey(Bytes::from(encode_hot_row_key(identity))))
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
        HOT_FILE_SPACE,
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
            let row = decode_hot_file_key_in_scope(entry.key.0.as_ref(), &scope)?;
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
            if !bytes.is_empty() {
                return Ok(None);
            }
            if actual_coverage
                .add_encoded_group_key(entry.key.0.as_ref())
                .is_none()
            {
                return Ok(None);
            }
            let Ok(identity) = decode_hot_diff_key_in_scope(entry.key.0.as_ref(), &scope) else {
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
        }
        if !page.value.has_more || resume_after.is_none() {
            break;
        }
    }
    if actual_coverage != expected_coverage {
        return Ok(None);
    }
    let identities = selected.clone();
    let after_values = hot_load_primary_identity_bytes(store, &identities).await?;
    let mut entries = Vec::new();
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
        if let Some(entry) =
            classify_hot_working_diff_entry(identity.into_row_identity(), before, after)
        {
            entries.push(entry);
        }
    }
    Ok(Some(entries))
}

async fn hot_working_diff_entries_for_finite_filter(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    filter: &TrackedStateFilter,
) -> Result<Option<Vec<TrackedStateDiffEntry>>, LixError> {
    let rows = hot_scan_entries(store, branch_id, generation, filter, None).await?;
    let mut entries = Vec::new();
    for (identity, bytes) in rows {
        let Ok(after) = decode_head_value(&bytes) else {
            return Ok(None);
        };
        if after.untracked {
            continue;
        }
        let baseline = after.working_diff_baseline;
        if baseline == WorkingDiffBaseline::Clean {
            continue;
        }
        let Some(before) = working_diff_baseline_before(baseline) else {
            return Ok(None);
        };
        let Some(after) = after.working_diff_version() else {
            return Ok(None);
        };
        if let Some(entry) = classify_hot_working_diff_entry(identity, before, after) {
            entries.push(entry);
        }
    }
    Ok(Some(entries))
}

fn classify_hot_working_diff_entry(
    identity: HeadRowIdentity,
    before: Option<WorkingDiffVersion>,
    after: WorkingDiffVersion,
) -> Option<TrackedStateDiffEntry> {
    let before_row = before.map(|version| version.into_diff_row(&identity));
    let after_row = after.into_diff_row(&identity);
    let diff_identity = TrackedStateDiffIdentity {
        schema_key: identity.schema_key,
        entity_pk: identity.entity_pk,
        file_id: identity.file_id,
    };
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

async fn hot_scan_entries(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    filter: &TrackedStateFilter,
    limit: Option<usize>,
) -> Result<Vec<(HeadRowIdentity, Bytes)>, LixError> {
    // The null-file member is a true point key. A logical-PK scan can use a
    // single MultiGet only when this schema has no file-backed members; if it
    // does, fall through to the complete primary-prefix route so UPDATE and
    // DELETE still see every candidate member.
    if let Some(identities) = hot_exact_identities(branch_id, generation, filter) {
        let may_use_null_point_batch = !filter.file_ids.is_empty()
            || !hot_schema_has_file_members(store, branch_id, generation, &filter.schema_keys)
                .await?;
        if may_use_null_point_batch {
            if limit.is_none()
                && let Some(entries) = hot_scan_dense_identity_range(store, &identities).await?
            {
                return Ok(entries);
            }
            let values = hot_load_identity_bytes(store, &identities).await?;
            return Ok(identities
                .into_iter()
                .zip(values)
                .filter_map(|(identity, value)| {
                    value.map(|value| (identity.into_row_identity(), value))
                })
                .take(limit.unwrap_or(usize::MAX))
                .collect());
        }
    }

    // The primary hot index is ordered by logical PK. Filesystem queries such
    // as `WHERE file_id = ?` need the inverse order, otherwise one matching
    // file would force a scan of every entity in its schema. Keep the full
    // value in the file projection so this is a direct serving route rather
    // than an index lookup followed by a second primary read.
    if let Some(prefixes) = hot_file_scan_prefixes(branch_id, generation, filter) {
        return scan_hot_file_entries(store, branch_id, generation, prefixes, filter, limit).await;
    }

    let scope = hot_scope_prefix(branch_id, generation);
    let mut prefixes = scan_prefixes(&scope, filter);
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
                let identity = decode_hot_row_key_in_scope(entry.key.0.as_ref(), &scope)?;
                if matches_filter(&identity, filter) {
                    rows.push((identity, full_value_bytes(entry.value)?));
                    if limit.is_some_and(|limit| rows.len() >= limit) {
                        return Ok(rows);
                    }
                }
            }
            if !page.value.has_more || resume_after.is_none() {
                break;
            }
        }
    }
    Ok(rows)
}

async fn hot_scan_dense_identity_range(
    store: &(impl StorageAdapterRead + ?Sized),
    identities: &[HeadIdentity],
) -> Result<Option<Vec<(HeadRowIdentity, Bytes)>>, LixError> {
    if identities.len() < HOT_DENSE_SCAN_MIN_IDENTITIES
        || identities
            .windows(2)
            .any(|pair| pair[0].schema_key != pair[1].schema_key)
    {
        return Ok(None);
    }

    let requested_keys = identities
        .iter()
        .map(encode_hot_row_key)
        .collect::<Vec<_>>();
    let Some(first_key) = requested_keys.first() else {
        return Ok(Some(Vec::new()));
    };
    let Some(last_key) = requested_keys.last() else {
        return Ok(Some(Vec::new()));
    };
    let plan = ScanPlan::range(
        HOT_ROW_SPACE,
        crate::storage_adapter::StorageKeyRange {
            lower: std::ops::Bound::Included(StorageKey(Bytes::copy_from_slice(first_key))),
            upper: std::ops::Bound::Included(StorageKey(Bytes::copy_from_slice(last_key))),
        },
    );
    let scan_budget = identities.len().saturating_mul(HOT_DENSE_SCAN_MAX_OVERREAD);
    let mut scanned = 0;
    let mut requested_index = 0;
    let mut resume_after = None;
    let mut rows = Vec::with_capacity(identities.len());
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
            while requested_index < requested_keys.len()
                && requested_keys[requested_index].as_slice() < entry.key.0.as_ref()
            {
                requested_index += 1;
            }
            if requested_index < requested_keys.len()
                && requested_keys[requested_index].as_slice() == entry.key.0.as_ref()
            {
                rows.push((
                    identities[requested_index].clone().into_row_identity(),
                    full_value_bytes(entry.value)?,
                ));
                requested_index += 1;
            }
        }
        if requested_index == requested_keys.len() || !page.value.has_more || resume_after.is_none()
        {
            return Ok(Some(rows));
        }
    }
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
) -> Result<Vec<(HeadRowIdentity, Bytes)>, LixError> {
    let scope = hot_scope_prefix(branch_id, generation);
    let mut rows = Vec::new();
    for prefix in prefixes {
        let plan = ScanPlan::prefix(
            HOT_FILE_SPACE,
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
                let identity = decode_hot_file_key_in_scope(entry.key.0.as_ref(), &scope)?;
                if matches_filter(&identity, filter) {
                    rows.push((identity, full_value_bytes(entry.value)?));
                }
            }
            if !page.value.has_more || resume_after.is_none() {
                break;
            }
        }
    }
    // A file projection is ordered `(schema, file_id, entity_pk)`, while SQL
    // rows are ordered `(schema, entity_pk, file_id)`. Restore the public
    // order after multi-file scans and defend against repeated predicates.
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
        let mut prefix = hot_scope_prefix(branch_id, generation);
        write_key_string(&mut prefix, schema_key, KEY_PART_FINAL);
        let page = ScanPlan::prefix(
            HOT_FILE_SPACE,
            StoragePrefix {
                bytes: Bytes::from(prefix),
            },
        )
        .collect(
            store,
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
    }
    Ok(false)
}

fn hot_exact_identities(
    branch_id: &str,
    generation: CommitId,
    filter: &TrackedStateFilter,
) -> Option<Vec<HeadIdentity>> {
    if filter.schema_keys.is_empty() || filter.entity_pks.is_empty() {
        return None;
    }
    let file_ids = if filter.file_ids.is_empty() {
        // A full logical-PK lookup can contain file-backed variants. It is
        // still a direct point batch for the overwhelmingly common null-file
        // schemas; schemas with a file projection fall back to the prefix
        // route below.
        vec![None]
    } else {
        filter
            .file_ids
            .iter()
            .map(|file_id| match file_id {
                NullableKeyFilter::Null => Some(None),
                NullableKeyFilter::Value(value) => Some(Some(value.clone())),
                NullableKeyFilter::Any => None,
            })
            .collect::<Option<Vec<_>>>()?
    };
    let mut identities = Vec::new();
    for schema_key in &filter.schema_keys {
        for entity_pk in &filter.entity_pks {
            for file_id in &file_ids {
                identities.push(HeadIdentity {
                    branch_id: branch_id.to_string(),
                    generation,
                    schema_key: schema_key.clone(),
                    entity_pk: entity_pk.clone(),
                    file_id: file_id.clone(),
                });
            }
        }
    }
    identities.sort();
    identities.dedup();
    Some(identities)
}

fn hot_scope_prefix(branch_id: &str, generation: CommitId) -> Vec<u8> {
    encode_scope_prefix(branch_id, generation)
}

fn encode_hot_row_key(identity: &HeadIdentity) -> Vec<u8> {
    encode_hot_row_key_parts(
        &identity.branch_id,
        identity.generation,
        &identity.schema_key,
        &identity.entity_pk,
        identity.file_id.as_deref(),
    )
}

fn encode_hot_row_key_parts(
    branch_id: &str,
    generation: CommitId,
    schema_key: &str,
    entity_pk: &EntityPk,
    file_id: Option<&str>,
) -> Vec<u8> {
    let mut key = hot_scope_prefix(branch_id, generation);
    write_key_string(&mut key, schema_key, KEY_PART_FINAL);
    write_entity_pk(&mut key, entity_pk);
    write_file_id(&mut key, file_id);
    key
}

fn encode_hot_file_key(identity: &HeadIdentity) -> Vec<u8> {
    debug_assert!(identity.file_id.is_some());
    encode_hot_file_key_parts(
        &identity.branch_id,
        identity.generation,
        &identity.schema_key,
        &identity.entity_pk,
        identity.file_id.as_deref(),
    )
}

fn encode_hot_file_key_parts(
    branch_id: &str,
    generation: CommitId,
    schema_key: &str,
    entity_pk: &EntityPk,
    file_id: Option<&str>,
) -> Vec<u8> {
    debug_assert!(file_id.is_some());
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

fn encode_hot_diff_key_parts(
    branch_id: &str,
    checkpoint_commit_id: CommitId,
    generation: CommitId,
    schema_key: &str,
    entity_pk: &EntityPk,
    file_id: Option<&str>,
) -> Vec<u8> {
    let mut key = encode_working_diff_scope_prefix(branch_id, checkpoint_commit_id, generation);
    write_key_string(&mut key, schema_key, KEY_PART_FINAL);
    write_entity_pk(&mut key, entity_pk);
    write_file_id(&mut key, file_id);
    key
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
    let entity_pk = read_entity_pk(bytes, &mut offset)?;
    let file_id = read_file_id(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(key_codec_error("hot row key has trailing bytes"));
    }
    Ok(HeadRowIdentity {
        schema_key,
        entity_pk,
        file_id,
    })
}

fn decode_hot_row_key(bytes: &[u8]) -> Result<HeadIdentity, LixError> {
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
    let entity_pk = read_entity_pk(bytes, &mut offset)?;
    let file_id = read_file_id(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(key_codec_error("hot row key has trailing bytes"));
    }
    Ok(HeadIdentity {
        branch_id,
        generation,
        schema_key,
        entity_pk,
        file_id,
    })
}

fn decode_hot_file_key_in_scope(bytes: &[u8], scope: &[u8]) -> Result<HeadRowIdentity, LixError> {
    if !bytes.starts_with(scope) {
        return Err(key_codec_error(
            "hot file row does not begin with its scanned scope",
        ));
    }
    let mut offset = scope.len();
    let (schema_key, schema_terminator) = read_key_string(bytes, &mut offset, "schema key")?;
    if schema_terminator != KEY_PART_FINAL {
        return Err(key_codec_error(
            "hot file row schema key has an invalid terminator",
        ));
    }
    let Some(file_id) = read_file_id(bytes, &mut offset)? else {
        return Err(key_codec_error("hot file row is missing its file id"));
    };
    let entity_pk = read_entity_pk(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(key_codec_error("hot file row key has trailing bytes"));
    }
    Ok(HeadRowIdentity {
        schema_key,
        entity_pk,
        file_id: Some(file_id),
    })
}

fn decode_hot_file_key(bytes: &[u8]) -> Result<HeadIdentity, LixError> {
    let mut offset = 0;
    let (branch_id, branch_terminator) = read_key_string(bytes, &mut offset, "branch id")?;
    if branch_terminator != KEY_PART_FINAL {
        return Err(key_codec_error(
            "hot file row branch id has an invalid terminator",
        ));
    }
    let generation = read_generation(bytes, &mut offset)?;
    let (schema_key, schema_terminator) = read_key_string(bytes, &mut offset, "schema key")?;
    if schema_terminator != KEY_PART_FINAL {
        return Err(key_codec_error(
            "hot file row schema key has an invalid terminator",
        ));
    }
    let Some(file_id) = read_file_id(bytes, &mut offset)? else {
        return Err(key_codec_error("hot file row is missing its file id"));
    };
    let entity_pk = read_entity_pk(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(key_codec_error("hot file row key has trailing bytes"));
    }
    Ok(HeadIdentity {
        branch_id,
        generation,
        schema_key,
        entity_pk,
        file_id: Some(file_id),
    })
}

fn decode_hot_diff_key_in_scope(bytes: &[u8], scope: &[u8]) -> Result<HeadRowIdentity, LixError> {
    decode_hot_row_key_in_scope(bytes, scope)
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
        decode_hot_row_key,
        &active,
        &mut stale_untracked_refs,
    )
    .await?;
    // The file projection duplicates primary values. Sweep it independently
    // so an orphaned projection cannot keep a history-free JSON payload alive
    // after its branch generation becomes unreachable.
    stage_collect_stale_hot_space(
        store,
        writes,
        HOT_FILE_SPACE,
        decode_hot_file_key,
        &active,
        &mut stale_untracked_refs,
    )
    .await?;
    Ok(stale_untracked_refs
        .into_iter()
        .map(JsonRef::from_hash_bytes)
        .collect())
}

async fn stage_collect_stale_hot_space(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    space: StorageSpace,
    decode_key: fn(&[u8]) -> Result<HeadIdentity, LixError>,
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
            let active_generation = decode_key(entry.key.0.as_ref())
                .is_ok_and(|identity| active.contains(&(identity.branch_id, identity.generation)));
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
            let keep = match (
                decode_hot_diff_key(entry.key.0.as_ref()),
                full_value_bytes(entry.value),
            ) {
                (Ok((checkpoint_commit_id, identity)), Ok(bytes))
                    if active.get(&identity.branch_id).is_some_and(|scope| {
                        scope.checkpoint_commit_id == checkpoint_commit_id
                            && scope.generation == identity.generation
                    }) =>
                {
                    bytes.is_empty()
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
    use bytes::Bytes;

    use super::*;
    use crate::branch::{BranchHeadControl, stage_branch_head_control};
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};

    fn timestamp() -> LixTimestamp {
        LixTimestamp::expect_parse("hot working-diff test timestamp", "2026-01-01T00:00:00Z")
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

        let rows = hot_scan_dense_identity_range(&read, &requested)
            .await
            .expect("scan dense identity range")
            .expect("dense range should stay on the scan path");

        assert_eq!(
            rows.into_iter()
                .map(|(identity, _)| identity.entity_pk)
                .collect::<Vec<_>>(),
            requested
                .into_iter()
                .map(|identity| identity.entity_pk)
                .collect::<Vec<_>>()
        );
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

        let rows = hot_scan_dense_identity_range(&read, &requested)
            .await
            .expect("probe sparse identity range");

        assert!(
            rows.is_none(),
            "sparse ranges must return to the exact point-read path"
        );
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
        let active_key = encode_hot_diff_key(active_checkpoint, &active_identity);
        let stale_key = encode_hot_diff_key(stale_checkpoint, &stale_identity);
        let orphan_key = encode_hot_diff_key(orphan_checkpoint, &orphan_identity);

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

        for key in [&active_key, &stale_key, &orphan_key] {
            writes.put(
                HOT_DIFF_SPACE,
                StorageKey(Bytes::copy_from_slice(key)),
                StorageValue {
                    bytes: Bytes::new(),
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
