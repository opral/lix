//! Filesystem ownership changes that accompany native incoming row application.
use super::*;
use crate::filesystem::{FileDeleteInput, plan_file_delete};

impl<S: Storage + Clone + Send + Sync + 'static> Transaction<S> {
    /// Apply deletion events to the resolved candidate, including target-only
    /// tombstones which are absent from the incoming pick list.
    pub(crate) async fn stage_merge_delete_actions(
        &mut self,
        analysis: &crate::session::MergeAnalysis,
        picks: &[crate::tracked_state::TrackedStateMergePick],
        resolved_deletes: &BTreeSet<TrackedStateKey>,
    ) -> Result<BTreeSet<TrackedStateKey>, LixError> {
        const MAX_FILE_DELETE_ROWS: usize = 65_536;
        let branch = self.active_branch_id().to_owned();
        let mut file_delete_ids = analysis
            .target_diff
            .entries
            .iter()
            .chain(analysis.source_diff.entries.iter())
            .filter(|entry| {
                entry.identity.schema_key() == "lix_file_descriptor"
                    && entry.after.as_ref().is_some_and(|row| row.deleted)
            })
            .filter_map(|entry| entry.identity.file_id().map(str::to_owned))
            .collect::<BTreeSet<_>>();
        let resurrected_file_ids = picks
            .iter()
            .filter(|pick| {
                pick.identity.schema_key() == "lix_file_descriptor" && !pick.selected_row.deleted
            })
            .filter_map(|pick| pick.identity.file_id().map(str::to_owned))
            .collect::<BTreeSet<_>>();
        file_delete_ids.retain(|id| !resurrected_file_ids.contains(id));
        let historical_file_schema_keys = if file_delete_ids.is_empty() {
            BTreeSet::new()
        } else {
            let source = self
                .tracked_catalog_at_commit(&analysis.commits.source_commit_id.to_string())
                .await?;
            let base = self
                .tracked_catalog_at_commit(&analysis.commits.base_commit_id.to_string())
                .await?;
            source
                .plans()
                .chain(base.plans())
                .map(|plan| plan.key.schema_key.to_string())
                .collect()
        };
        let read = self.opening_read();
        let base = self.hot_state.reader(&read);
        let staged = self.staged_writes.staging_overlay()?;
        let incoming_schemas = picks
            .iter()
            .any(|pick| pick.identity.schema_key() == REGISTERED_SCHEMA_KEY)
            || self
                .staged_writes
                .has_staged_schema_catalog_change(&Domain::schema_catalog(branch.clone(), true))?;
        // Inspect only incoming schema declarations to bound generation
        // expansion. An unrelated registration must not expand an unrelated
        // collection delete into individual row tombstones.
        let mut incoming_fk_targets = BTreeSet::new();
        let mut incoming_row_refs = false;
        let mut incoming_schema_keys = BTreeSet::new();
        let mut schema_groups = BTreeMap::<CommitId, Vec<TrackedStateKey>>::new();
        for pick in picks.iter().filter(|pick| {
            pick.identity.schema_key() == REGISTERED_SCHEMA_KEY && !pick.selected_row.deleted
        }) {
            schema_groups
                .entry(pick.selected_row.commit_id)
                .or_default()
                .push(TrackedStateKey {
                    schema_key: REGISTERED_SCHEMA_KEY.into(),
                    file_id: pick.identity.file_id().map(str::to_owned),
                    row_pk: pick.identity.row_pk().clone(),
                });
        }
        if !schema_groups.is_empty() {
            let mut reader = self.tracked_state_reader().await?;
            for (commit, keys) in schema_groups {
                let rows = reader
                    .load_projected_batch_at_commit(
                        &commit.to_string(),
                        &keys,
                        &ChangeRecordProjection::full(),
                    )
                    .await?;
                for slot in 0..keys.len() {
                    let row = rows
                        .row(slot)
                        .ok_or_else(|| LixError::unknown("incoming schema selection is missing"))?;
                    let snapshot = native_file_descriptor_json(row)?;
                    incoming_row_refs |= snapshot
                        .get("value")
                        .and_then(|schema| schema.get("columns"))
                        .and_then(JsonValue::as_array)
                        .is_some_and(|columns| {
                            columns.iter().any(|column| {
                                column.get("type").and_then(JsonValue::as_str) == Some("row_ref")
                            })
                        });
                    if let Some(schema_key) = snapshot
                        .get("schema_key")
                        .or_else(|| snapshot.get("value").and_then(|schema| schema.get("key")))
                        .and_then(JsonValue::as_str)
                    {
                        incoming_schema_keys.insert(schema_key.to_owned());
                    }
                    if let Some(foreign_keys) = snapshot
                        .get("value")
                        .and_then(|schema| schema.get("foreign_keys"))
                        .and_then(JsonValue::as_array)
                    {
                        for foreign_key in foreign_keys {
                            if let Some(target) = foreign_key
                                .get("references")
                                .and_then(|reference| reference.get("schema_key"))
                                .and_then(JsonValue::as_str)
                            {
                                incoming_fk_targets.insert(target.to_owned());
                            }
                        }
                    }
                }
            }
        }
        if incoming_schemas {
            self.schema_resolver.clear_cached_catalogs();
        }
        let (_, catalog) = self
            .schema_resolver
            .catalogs_for_validation(
                &base,
                &staged,
                &Domain::schema_catalog(branch.clone(), true),
            )
            .await?;
        let row_ref_source_schema_keys = catalog
            .row_ref_references()
            .iter()
            .map(|reference| reference.source_key.schema_key.as_str())
            .collect::<BTreeSet<_>>();
        // A collection-generation marker is safe to apply without member
        // expansion when the catalog merely declares a row-ref source but
        // there are no source rows participating in this merge. A source row
        // selected by the merge, or one visible in the destination overlay,
        // changes that: the marker must be expanded so the shared delete
        // planner can cascade it. The overlay probe covers unchanged
        // destination rows and staged rows without expanding any source
        // collection. Incoming picks carry no scope, so compare candidate
        // picks against exact rows in the global domain by identity and
        // change id. This distinguishes a projected global pick from a local
        // row with the same schema/key without hydrating document payloads.
        let expected_global = branch == GLOBAL_BRANCH_ID;
        let incoming_row_ref_picks = picks
            .iter()
            .filter(|pick| {
                !pick.selected_row.deleted
                    && row_ref_source_schema_keys.contains(pick.identity.schema_key())
            })
            .collect::<Vec<_>>();
        let incoming_row_ref_sources = if incoming_row_ref_picks.is_empty() {
            false
        } else if expected_global {
            true
        } else {
            let global_rows = base
                .load_exact_batch(&HotStateExactBatchRequest {
                    rows: incoming_row_ref_picks
                        .iter()
                        .map(|pick| HotStateExactRowRequest {
                            schema_key: pick.identity.schema_key().to_owned(),
                            file_id: pick.identity.file_id().map(str::to_owned),
                            row_pk: pick.identity.row_pk().clone(),
                            branch_id: GLOBAL_BRANCH_ID.to_owned(),
                        })
                        .collect(),
                    projection: Default::default(),
                    untracked: Some(false),
                    include_tombstones: false,
                })
                .await?;
            incoming_row_ref_picks
                .iter()
                .enumerate()
                .any(|(slot, pick)| {
                    !global_rows.row(slot).is_some_and(|row| {
                        row.global() && row.change_id() == Some(pick.selected_row.change_id)
                    })
                })
        };
        let live_row_ref_sources = if row_ref_source_schema_keys.is_empty() {
            false
        } else {
            let mut found = false;
            for schema_key in &row_ref_source_schema_keys {
                let rows = overlay_scan_batch(
                    &base,
                    &staged,
                    &HotStateScanRequest {
                        filter: HotStateFilter {
                            schema_keys: vec![(*schema_key).to_owned()],
                            branch_ids: vec![branch.clone()],
                            global: Some(expected_global),
                            include_tombstones: false,
                            ..Default::default()
                        },
                        limit: Some(1),
                        ..Default::default()
                    },
                )
                .await?;
                if !rows.is_empty() {
                    found = true;
                    break;
                }
            }
            found
        };
        let row_ref_sources_present = incoming_row_ref_sources || live_row_ref_sources;
        let has_action = |schema: &str| {
            catalog.has_row_ref_cascades()
                || catalog
                    .delete_plan_for_key(schema)
                    .foreign_key_references
                    .iter()
                    .any(|reference| {
                        reference.foreign_key.on_delete == lix_schema::DeleteAction::Cascade
                    })
        };
        let file_delete_scopes_enabled = !file_delete_ids.is_empty()
            && (incoming_row_refs
                || !catalog.row_ref_references().is_empty()
                || catalog.has_row_ref_cascades());
        let mut file_delete_schema_keys = self
            .sql_schema_snapshot
            .plans()
            .map(|plan| plan.key.schema_key.to_string())
            .chain(historical_file_schema_keys)
            .chain(incoming_schema_keys)
            .collect::<BTreeSet<_>>();
        file_delete_schema_keys.extend([
            "lix_file_descriptor".to_owned(),
            "lix_binary_blob_ref".to_owned(),
            "lix_key_value".to_owned(),
        ]);
        let mut seed_keys = analysis
            .target_diff
            .entries
            .iter()
            .chain(analysis.source_diff.entries.iter())
            .filter(|entry| {
                // Collection-generation deletion can remove an identity without
                // retaining an individual after-row tombstone in the diff.
                entry.after.as_ref().is_none_or(|row| row.deleted)
                    && (incoming_schemas || has_action(entry.identity.schema_key()))
            })
            .map(|entry| TrackedStateKey {
                schema_key: entry.identity.schema_key().into(),
                file_id: entry.identity.file_id().map(str::to_owned),
                row_pk: entry.identity.row_pk().clone(),
            })
            .collect::<BTreeSet<_>>();
        seed_keys.extend(
            resolved_deletes
                .iter()
                .filter(|key| incoming_schemas || has_action(&key.schema_key))
                .cloned(),
        );

        // `diff_commit_members` deliberately suppresses the physical member
        // tombstones produced by a collection-generation delete. The marker
        // itself remains in the merge diff, so use those marker identities as
        // a finite index into the ordinary effective diff. This recovers the
        // deleted parent identities needed by a newly arriving FK without
        // scanning unrelated schemas, files, or the whole database.
        let mut generation_scopes = BTreeMap::<String, BTreeSet<Option<String>>>::new();
        for entry in analysis
            .target_diff
            .entries
            .iter()
            .chain(analysis.source_diff.entries.iter())
            .filter(|entry| {
                entry.identity.schema_key()
                    == crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
            })
        {
            let (schema_key, file_id) = crate::collection_generation::collection_scope_from_row_pk(
                entry.identity.row_pk(),
            )?;
            if incoming_row_refs
                || row_ref_sources_present
                || incoming_fk_targets.contains(&schema_key)
                || !catalog
                    .delete_plan_for_key(&schema_key)
                    .foreign_key_references
                    .is_empty()
            {
                generation_scopes
                    .entry(schema_key)
                    .or_default()
                    .insert(file_id);
            }
        }
        let selected_generation_scopes = picks
            .iter()
            .filter(|pick| {
                pick.identity.schema_key()
                    == crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
                    && !pick.selected_row.deleted
                    && analysis.source_diff.entries.iter().any(|entry| {
                        entry.identity == pick.identity
                            && entry
                                .after
                                .as_ref()
                                .is_some_and(|row| row.change_id == pick.selected_row.change_id)
                    })
            })
            .map(|pick| {
                crate::collection_generation::collection_scope_from_row_pk(pick.identity.row_pk())
            })
            .collect::<Result<BTreeSet<_>, _>>()?;
        let mut incoming_generation_deletes = BTreeSet::new();
        if seed_keys.is_empty()
            && generation_scopes.is_empty()
            && !incoming_schemas
            && !file_delete_scopes_enabled
        {
            return Ok(BTreeSet::new());
        }
        let mut relevant_schemas = seed_keys
            .iter()
            .map(|key| key.schema_key.clone())
            .chain(generation_scopes.keys().cloned())
            .collect::<BTreeSet<_>>();
        if incoming_schemas {
            relevant_schemas.extend(
                picks
                    .iter()
                    .map(|pick| pick.identity.schema_key().to_owned()),
            );
        }
        // Dynamic references can point at any winning deletion, including a
        // target in another file. Hydrate their incoming source selections so
        // the shared planner sees the complete candidate, not only local rows.
        relevant_schemas.extend(
            catalog
                .row_ref_references()
                .iter()
                .filter(|reference| {
                    reference.row_ref.on_delete == lix_schema::DeleteAction::Cascade
                })
                .map(|reference| reference.source_key.schema_key.clone()),
        );
        let mut schema_frontier = relevant_schemas.iter().cloned().collect::<Vec<_>>();
        while let Some(schema) = schema_frontier.pop() {
            for reference in catalog.delete_plan_for_key(&schema).foreign_key_references {
                if reference.foreign_key.on_delete == lix_schema::DeleteAction::Cascade
                    && relevant_schemas.insert(reference.source_key.schema_key.clone())
                {
                    schema_frontier.push(reference.source_key.schema_key.clone());
                }
            }
        }
        if !generation_scopes.is_empty() {
            let mut reader = self.tracked_state_reader().await?;
            let base_commit = analysis.commits.base_commit_id.to_string();
            for (schema_key, file_ids) in generation_scopes {
                let request = TrackedStateDiffRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec![schema_key],
                        file_ids: file_ids
                            .into_iter()
                            .map(|file_id| {
                                file_id.map_or(NullableKeyFilter::Null, NullableKeyFilter::Value)
                            })
                            .collect(),
                        include_tombstones: true,
                        ..TrackedStateFilter::default()
                    },
                    retain_payloads: false,
                };
                for head_commit in [
                    analysis.commits.target_commit_id,
                    analysis.commits.source_commit_id,
                ] {
                    let diff = reader
                        .diff_commits(&base_commit, &head_commit.to_string(), &request)
                        .await?;
                    for entry in diff.entries.iter().filter(|entry| {
                        entry.before.as_ref().is_some_and(|row| !row.deleted)
                            && entry.after.as_ref().is_none_or(|row| row.deleted)
                    }) {
                        let key = TrackedStateKey {
                            schema_key: entry.identity.schema_key().into(),
                            file_id: entry.identity.file_id().map(str::to_owned),
                            row_pk: entry.identity.row_pk().clone(),
                        };
                        if head_commit == analysis.commits.source_commit_id
                            && selected_generation_scopes
                                .contains(&(key.schema_key.clone(), key.file_id.clone()))
                        {
                            incoming_generation_deletes.insert(key.clone());
                        }
                        seed_keys.insert(key);
                    }
                }
            }
        }
        let mut file_delete_keys = BTreeSet::new();
        if file_delete_scopes_enabled {
            let request = TrackedStateDiffRequest {
                filter: TrackedStateFilter {
                    schema_keys: file_delete_schema_keys.iter().cloned().collect(),
                    file_ids: file_delete_ids
                        .iter()
                        .cloned()
                        .map(NullableKeyFilter::Value)
                        .collect(),
                    include_tombstones: true,
                    ..TrackedStateFilter::default()
                },
                retain_payloads: false,
            };
            let base_commit = analysis.commits.base_commit_id.to_string();
            let mut reader = self.tracked_state_reader().await?;
            let mut rows_per_file = BTreeMap::<String, usize>::new();
            for head_commit in [
                analysis.commits.target_commit_id,
                analysis.commits.source_commit_id,
            ] {
                let diff = reader
                    .diff_commits(&base_commit, &head_commit.to_string(), &request)
                    .await?;
                for entry in diff.entries {
                    let Some(file_id) = entry.identity.file_id() else {
                        continue;
                    };
                    if !file_delete_ids.contains(file_id)
                        || matches!(
                            entry.identity.schema_key(),
                            "lix_file_descriptor"
                                | "lix_binary_blob_ref"
                                | "lix_collection_generation"
                                | REGISTERED_SCHEMA_KEY
                        )
                        || entry.before.as_ref().is_none_or(|row| row.deleted)
                            && entry.after.as_ref().is_none_or(|row| row.deleted)
                    {
                        continue;
                    }
                    let key = TrackedStateKey {
                        schema_key: entry.identity.schema_key().into(),
                        file_id: Some(file_id.to_owned()),
                        row_pk: entry.identity.row_pk().clone(),
                    };
                    if file_delete_keys.insert(key) {
                        let count = rows_per_file.entry(file_id.to_owned()).or_default();
                        *count += 1;
                        if *count > MAX_FILE_DELETE_ROWS {
                            return Err(LixError::new(
                                "LIX_PARTIAL_MERGE_PREPARATION_LIMIT",
                                "deleted file semantic closure exceeds the bounded row closure",
                            ));
                        }
                    }
                }
            }
            // The endpoint diff only reports identities whose values changed
            // after the merge base. A row that was already present at the
            // base and unchanged on both endpoints is still retired by the
            // winning file delete. Recover those identities with bounded
            // exact (schema,file) key scans; payloads are deliberately not
            // requested because the historical point reads below hydrate
            // only the proven seeds that the planner needs.
            for file_id in &file_delete_ids {
                for schema_key in &file_delete_schema_keys {
                    if matches!(
                        schema_key.as_str(),
                        "lix_file_descriptor"
                            | "lix_binary_blob_ref"
                            | "lix_collection_generation"
                            | REGISTERED_SCHEMA_KEY
                    ) {
                        continue;
                    }
                    let rows = reader
                        .scan_batch_at_commit_page(
                            &base_commit,
                            &TrackedStateScanRequest {
                                filter: TrackedStateFilter {
                                    schema_keys: vec![schema_key.clone()],
                                    file_ids: vec![NullableKeyFilter::Value(file_id.clone())],
                                    include_tombstones: false,
                                    ..TrackedStateFilter::default()
                                },
                                read_columns: crate::tracked_state::TrackedStateReadColumns {
                                    columns: Vec::new(),
                                },
                                limit: Some(MAX_FILE_DELETE_ROWS + 1),
                            },
                            None,
                        )
                        .await?;
                    if rows.len() > MAX_FILE_DELETE_ROWS {
                        return Err(LixError::new(
                            "LIX_PARTIAL_MERGE_PREPARATION_LIMIT",
                            "deleted file semantic closure exceeds the bounded row closure",
                        ));
                    }
                    for slot in 0..rows.len() {
                        let row = rows.row(slot);
                        let key = TrackedStateKey {
                            schema_key: row.schema_key().into(),
                            file_id: row.file_id().map(str::to_owned),
                            row_pk: row.row_pk().clone(),
                        };
                        if file_delete_keys.insert(key) {
                            let count = rows_per_file.entry(file_id.clone()).or_default();
                            *count += 1;
                            if *count > MAX_FILE_DELETE_ROWS {
                                return Err(LixError::new(
                                    "LIX_PARTIAL_MERGE_PREPARATION_LIMIT",
                                    "deleted file semantic closure exceeds the bounded row closure",
                                ));
                            }
                        }
                    }
                }
            }
        }
        seed_keys.extend(file_delete_keys.iter().cloned());
        let seed_keys = seed_keys.into_iter().collect::<Vec<_>>();
        // Hydrate sparse picks in one batch per historical commit, not one
        // storage read per row. The staged semantic resolutions take precedence.
        let mut groups = BTreeMap::<CommitId, Vec<TrackedStateKey>>::new();
        for pick in picks.iter().filter(|pick| {
            incoming_schemas || relevant_schemas.contains(pick.identity.schema_key())
        }) {
            groups
                .entry(pick.selected_row.commit_id)
                .or_default()
                .push(TrackedStateKey {
                    schema_key: pick.identity.schema_key().into(),
                    file_id: pick.identity.file_id().map(str::to_owned),
                    row_pk: pick.identity.row_pk().clone(),
                });
        }
        let mut raw = RawWriteBatch::new();
        // A selected generation marker retires these proven predecessors, but
        // the row overlay itself does not interpret markers. Project explicit
        // tombstones before selected rows; a selected/staged resurrection wins.
        for key in &incoming_generation_deletes {
            raw.push_parts(
                Some(key.row_pk.clone()),
                key.schema_key.as_str().into(),
                key.file_id.as_deref().map(Into::into),
                None,
                None,
                None,
                None,
                None,
                branch == GLOBAL_BRANCH_ID,
                None,
                None,
                false,
                branch.as_str().into(),
            );
        }
        let mut reader = self.tracked_state_reader().await?;
        for (commit, keys) in groups {
            let rows = reader
                .load_projected_batch_at_commit(
                    &commit.to_string(),
                    &keys,
                    &ChangeRecordProjection::full(),
                )
                .await?;
            for slot in 0..keys.len() {
                let row = rows.row(slot).ok_or_else(|| {
                    LixError::unknown("cascade candidate selected row is missing")
                })?;
                raw.push_parts(
                    Some(row.row_pk().clone()),
                    row.schema_key_shared(),
                    row.file_id_shared(),
                    if row.deleted() {
                        None
                    } else {
                        row.snapshot_content()
                            .cloned()
                            .map(TransactionJson::from_unvalidated_shared_normalized_content)
                    },
                    row.metadata()
                        .cloned()
                        .map(TransactionJson::from_unvalidated_shared_normalized_content),
                    None,
                    Some(row.created_at().to_string().into()),
                    Some(row.updated_at().to_string().into()),
                    branch == GLOBAL_BRANCH_ID,
                    None,
                    None,
                    false,
                    branch.as_str().into(),
                );
                if !row.deleted()
                    && let Some(snapshot) = row.decoded_snapshot()
                {
                    raw.set_decoded_snapshot(raw.len() - 1, Some(snapshot.clone()));
                }
            }
        }
        // A winning descriptor deletion retires every proven semantic row in
        // its file.  Append these after selected rows so a target-side edit
        // cannot resurrect a row whose owner file was deleted on source.
        let mut file_delete_deletes = RawWriteBatch::new();
        for key in &file_delete_keys {
            raw.push_parts(
                Some(key.row_pk.clone()),
                key.schema_key.as_str().into(),
                key.file_id.as_deref().map(Into::into),
                None,
                None,
                None,
                None,
                None,
                branch == GLOBAL_BRANCH_ID,
                None,
                None,
                false,
                branch.as_str().into(),
            );
            file_delete_deletes.push_parts(
                Some(key.row_pk.clone()),
                key.schema_key.as_str().into(),
                key.file_id.as_deref().map(Into::into),
                None,
                None,
                None,
                None,
                None,
                branch == GLOBAL_BRANCH_ID,
                None,
                None,
                false,
                branch.as_str().into(),
            );
        }
        let previous = reader
            .load_projected_batch_at_commit(
                &analysis.commits.base_commit_id.to_string(),
                &seed_keys,
                &ChangeRecordProjection::full(),
            )
            .await?;
        let target_previous = reader
            .load_projected_batch_at_commit(
                &analysis.commits.target_commit_id.to_string(),
                &seed_keys,
                &ChangeRecordProjection::full(),
            )
            .await?;
        let source_previous = reader
            .load_projected_batch_at_commit(
                &analysis.commits.source_commit_id.to_string(),
                &seed_keys,
                &ChangeRecordProjection::full(),
            )
            .await?;
        drop(reader);
        let prepared = self.prepare_transaction_rows(raw).await?;
        let selected = super::super::staging::PreparedSchemaOverlay::all_rows(&prepared);
        let base = self.hot_state.reader(&read);
        let selected_candidate = super::super::schema_resolver::TransactionSchemaHotStateReader {
            base: &base,
            staged: &selected,
        };
        let candidate = super::super::schema_resolver::TransactionSchemaHotStateReader {
            base: &selected_candidate,
            staged: &staged,
        };
        let current = candidate
            .load_exact_batch(&HotStateExactBatchRequest {
                rows: seed_keys
                    .iter()
                    .map(|key| HotStateExactRowRequest {
                        schema_key: key.schema_key.clone(),
                        file_id: key.file_id.clone(),
                        row_pk: key.row_pk.clone(),
                        branch_id: branch.clone(),
                    })
                    .collect(),
                projection: Default::default(),
                untracked: Some(false),
                include_tombstones: false,
            })
            .await?;
        let mut seeds = Vec::new();
        let mut generation_deletes = RawWriteBatch::new();
        for slot in 0..seed_keys.len() {
            // A winning resurrection cancels the historical deletion event.
            if current.row(slot).is_some() {
                continue;
            }
            let key = &seed_keys[slot];
            if incoming_generation_deletes.contains(key) {
                generation_deletes.push_parts(
                    Some(key.row_pk.clone()),
                    key.schema_key.as_str().into(),
                    key.file_id.as_deref().map(Into::into),
                    None,
                    None,
                    None,
                    None,
                    None,
                    branch == GLOBAL_BRANCH_ID,
                    None,
                    None,
                    false,
                    branch.as_str().into(),
                );
            }
            // A winning delete can conflict with a changed referenced unique
            // key. Both branch-live images can have valid incoming dependents.
            let live = [target_previous.row(slot), source_previous.row(slot)]
                .into_iter()
                .flatten()
                .filter(|row| !row.deleted())
                .collect::<Vec<_>>();
            let images = if live.is_empty() {
                previous
                    .row(slot)
                    .filter(|row| !row.deleted())
                    .into_iter()
                    .collect::<Vec<_>>()
            } else {
                live
            };
            for row in images {
                seeds.push(MaterializedHotStateRow {
                    row_pk: row.row_pk().clone(),
                    schema_key: row.schema_key().into(),
                    file_id: row.file_id().map(str::to_owned),
                    snapshot_content: Some(native_file_descriptor_json(row)?.to_string().into()),
                    metadata: None,
                    deleted: false,
                    created_at: row.created_at(),
                    updated_at: row.updated_at(),
                    global: branch == GLOBAL_BRANCH_ID,
                    change_id: None,
                    commit_id: None,
                    untracked: false,
                    branch_id: branch.as_str().into(),
                });
            }
        }
        // Normal staging uses the tracked catalog; load both durability
        // catalogs from the candidate so selected registrations remain native
        // historical references while generated rows can resolve their schemas.
        if incoming_schemas {
            self.schema_resolver.clear_cached_catalogs();
        }
        let (_, catalog) = self
            .schema_resolver
            .catalogs_for_validation(
                &selected_candidate,
                &staged,
                &Domain::schema_catalog(branch.clone(), false),
            )
            .await?;
        let mut deletes =
            super::super::validation::plan_delete_actions(&candidate, catalog, seeds).await?;
        deletes.append(generation_deletes);
        // Current state has one physical key across durability modes. A
        // cascaded untracked delete cannot also install a tracked selection at
        // that key in one publication. Preserve the ordinary merge conflict
        // contract instead of discarding the selection or emitting duplicate
        // physical mutations.
        let untracked_deletes = deletes
            .iter()
            .filter(|row| row.untracked)
            .map(|row| TrackedStateKey {
                schema_key: row.schema_key.to_string(),
                file_id: row.file_id.map(ToString::to_string),
                row_pk: row.row_pk.expect("cascade has an identity").clone(),
            })
            .collect::<BTreeSet<_>>();
        if !untracked_deletes.is_empty() {
            for pick in picks {
                let identity = TrackedStateKey {
                    schema_key: pick.identity.schema_key().into(),
                    file_id: pick.identity.file_id().map(str::to_owned),
                    row_pk: pick.identity.row_pk().clone(),
                };
                if untracked_deletes.contains(&identity) {
                    return Err(commit::selected_tracked_ref_untracked_collision_error(
                        &branch, &identity,
                    ));
                }
            }
        }
        // Untracked cleanup does not replace historical picks or count as
        // tracked merge changes.
        let identities = deletes
            .iter()
            .chain(file_delete_deletes.iter())
            .filter(|row| !row.untracked)
            .map(|row| TrackedStateKey {
                schema_key: row.schema_key.to_string(),
                file_id: row.file_id.map(ToString::to_string),
                row_pk: row.row_pk.expect("cascade has an identity").clone(),
            })
            .collect();
        if !file_delete_deletes.is_empty() {
            self.stage_file_delete_semantic_tombstones(file_delete_deletes)
                .await?;
        }
        if !deletes.is_empty() {
            self.stage_planned_cascade_deletes(deletes).await?;
        }
        Ok(identities)
    }
    /// Native file lifecycle is resolved before ordinary row reconciliation.
    /// A later file edit revives its complete captured source incarnation;
    /// a later delete uses the current target's ordinary deletion closure.
    pub(crate) async fn prepare_incoming_file_lifecycle(
        &mut self,
        analysis: &crate::session::MergeAnalysis,
    ) -> Result<Option<crate::session::MergeAnalysis>, LixError> {
        const MAX_ROWS: usize = 65_536;
        let affected = analysis
            .source_diff
            .entries
            .iter()
            .filter_map(|entry| entry.identity.file_id().map(str::to_owned))
            .collect::<BTreeSet<_>>();
        if affected.is_empty() {
            return Ok(None);
        }
        let keys = affected
            .iter()
            .map(|id| {
                Ok(TrackedStateKey {
                    schema_key: "lix_file_descriptor".into(),
                    file_id: Some(id.clone()),
                    row_pk: native_file_uuid_key(id)?,
                })
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        let source_id = analysis.commits.source_commit_id.to_string();
        let target_id = analysis.commits.target_commit_id.to_string();
        // The immutable native tree orders schema before file identity. Use
        // the already validated transaction catalog to issue finite
        // (schema,file) prefixes, never an all-schema file-only tree walk.
        let mut source_schemas = self
            .sql_schema_snapshot
            .plans()
            .map(|plan| plan.key.schema_key.to_string())
            .collect::<Vec<_>>();
        // SQL catalog plans need not expose canonical storage-only schemas.
        // These exact prefixes carry the file descriptor, bytes, and plugin
        // incarnation/reservation metadata required by a restored file.
        source_schemas.extend([
            "lix_file_descriptor".to_owned(),
            "lix_binary_blob_ref".to_owned(),
            "lix_key_value".to_owned(),
        ]);
        source_schemas.sort();
        source_schemas.dedup();
        let mut reader = self.tracked_state_reader().await?;
        let source = reader
            .load_projected_batch_at_commit(&source_id, &keys, &ChangeRecordProjection::full())
            .await?;
        let target = reader
            .load_projected_batch_at_commit(&target_id, &keys, &ChangeRecordProjection::full())
            .await?;
        let mut deletes = BTreeSet::new();
        let mut restores = BTreeSet::new();
        for (slot, id) in affected.iter().enumerate() {
            let source_row = source.row(slot);
            let target_row = target.row(slot);
            let incoming_delete = analysis.source_diff.entries.iter().any(|entry| {
                entry.identity.schema_key() == "lix_file_descriptor"
                    && entry.identity.file_id() == Some(id.as_str())
                    && entry.after.as_ref().is_some_and(|row| row.deleted)
            });
            if incoming_delete {
                deletes.insert(id.clone());
            } else if source_row.is_some_and(|row| !row.deleted())
                && target_row.is_none_or(|row| row.deleted())
            {
                restores.insert(id.clone());
            }
        }
        if deletes.is_empty() && restores.is_empty() {
            return Ok(None);
        }

        let handled = deletes.union(&restores).cloned().collect::<BTreeSet<_>>();
        let plan = analysis
            .merge_plan()
            .expect("file lifecycle requires native merge plan");
        let incoming_directory_keys = plan
            .picks
            .iter()
            .map(|pick| &pick.identity)
            .chain(plan.conflicts.iter().map(|conflict| &conflict.identity))
            .filter(|identity| {
                identity.schema_key() == "lix_directory_descriptor" && identity.file_id().is_none()
            })
            .map(|identity| identity.row_pk().clone())
            .collect::<BTreeSet<_>>();
        let mut restored = BTreeMap::new();
        for id in &restores {
            let rows = reader
                .scan_batch_at_commit_page(
                    &source_id,
                    &TrackedStateScanRequest {
                        filter: TrackedStateFilter {
                            schema_keys: source_schemas.clone(),
                            file_ids: vec![NullableKeyFilter::Value(id.clone())],
                            include_tombstones: true,
                            ..Default::default()
                        },
                        read_columns: crate::tracked_state::TrackedStateReadColumns {
                            columns: Vec::new(),
                        },
                        limit: Some(MAX_ROWS + 1),
                    },
                    None,
                )
                .await?;
            if rows.len() > MAX_ROWS || restored.len().saturating_add(rows.len()) > MAX_ROWS {
                return Err(LixError::new(
                    "LIX_PARTIAL_MERGE_PREPARATION_LIMIT",
                    "captured file resurrection exceeds the bounded row closure",
                ));
            }
            for slot in 0..rows.len() {
                let row = rows.row(slot);
                let pick = native_file_lifecycle_pick(row)?;
                restored.insert(
                    TrackedStateKey {
                        schema_key: row.schema_key().into(),
                        file_id: row.file_id().map(str::to_owned),
                        row_pk: row.row_pk().clone(),
                    },
                    pick,
                );
            }
            let descriptor = source
                .row(
                    affected
                        .iter()
                        .position(|candidate| candidate == id)
                        .expect("restored file is affected"),
                )
                .expect("restore requires source descriptor");
            let payload = native_file_descriptor_json(descriptor)?;
            let mut ancestor = payload
                .get("directory_id")
                .and_then(JsonValue::as_str)
                .map(str::to_owned);
            let mut visited = BTreeSet::new();
            while let Some(directory) = ancestor {
                if visited.len() >= 256 || !visited.insert(directory.clone()) {
                    return Err(LixError::new(
                        LixError::CODE_CONSTRAINT_VIOLATION,
                        "resurrected file has an invalid ancestor path",
                    ));
                }
                let key = TrackedStateKey {
                    schema_key: "lix_directory_descriptor".into(),
                    file_id: None,
                    row_pk: native_file_uuid_key(&directory)?,
                };
                let source_directory = reader
                    .load_projected_batch_at_commit(
                        &source_id,
                        std::slice::from_ref(&key),
                        &ChangeRecordProjection::full(),
                    )
                    .await?;
                let row = source_directory
                    .row(0)
                    .filter(|row| !row.deleted())
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_CONSTRAINT_VIOLATION,
                            "resurrected file source ancestor is absent",
                        )
                    })?;
                let payload = native_file_descriptor_json(row)?;
                let target_directory = reader
                    .load_projected_batch_at_commit(
                        &target_id,
                        std::slice::from_ref(&key),
                        &ChangeRecordProjection::identity_only(),
                    )
                    .await?;
                let incoming_directory_change = incoming_directory_keys.contains(&key.row_pk);
                if target_directory.row(0).is_some_and(|row| !row.deleted())
                    && !incoming_directory_change
                {
                    // This unchanged effective ancestor is already validated
                    // in the target root. Its current parent may differ from
                    // the source's old parent; do not revive that obsolete
                    // source chain. Incoming directory selections still need
                    // their own captured source dependency closure.
                    break;
                }
                if target_directory.row(0).is_none_or(|row| row.deleted()) {
                    if restored.len() >= MAX_ROWS && !restored.contains_key(&key) {
                        return Err(LixError::new(
                            "LIX_PARTIAL_MERGE_PREPARATION_LIMIT",
                            "file resurrection structural closure exceeds row budget",
                        ));
                    }
                    restored.insert(key, native_file_lifecycle_pick(row)?);
                }
                ancestor = payload
                    .get("parent_id")
                    .and_then(JsonValue::as_str)
                    .map(str::to_owned);
            }
        }
        drop(reader);
        let mut next = analysis.clone();
        let plan = next
            .merge_plan
            .as_mut()
            .expect("native file lifecycle requires merge plan");
        let keep = |identity: &crate::tracked_state::TrackedStateDiffIdentity| {
            identity.file_id().is_none_or(|id| !handled.contains(id))
        };
        let mut picks = plan
            .picks
            .iter()
            .filter(|pick| keep(&pick.identity))
            .cloned()
            .map(|pick| {
                (
                    TrackedStateKey {
                        schema_key: pick.identity.schema_key().into(),
                        file_id: pick.identity.file_id().map(str::to_owned),
                        row_pk: pick.identity.row_pk().clone(),
                    },
                    pick,
                )
            })
            .collect::<BTreeMap<_, _>>();
        // Incoming explicit directory changes remain authoritative over an
        // ancestor restored only as a structural dependency.
        for (key, pick) in restored {
            picks.entry(key).or_insert(pick);
        }
        plan.picks = picks.into_values().collect();
        plan.conflicts = plan
            .conflicts
            .iter()
            .filter(|conflict| keep(&conflict.identity))
            .cloned()
            .collect();
        next.source_diff.entries.retain(|entry| {
            keep(&entry.identity)
                || (entry.identity.schema_key() == "lix_file_descriptor"
                    && entry
                        .identity
                        .file_id()
                        .is_some_and(|id| deletes.contains(id))
                    && entry.after.as_ref().is_some_and(|row| row.deleted))
        });
        next.target_diff
            .entries
            .retain(|entry| keep(&entry.identity));
        if !deletes.is_empty() {
            let branch = self.active_branch_id().to_owned();
            let index = self
                .filesystem_path_index(&FilesystemPathIndexRequest::new(vec![branch.clone()]))
                .await?;
            let mut rows = RawWriteBatch::new();
            for id in deletes {
                if index
                    .exact_file_id_entries(&id)
                    .iter()
                    .any(|entry| is_plugin_storage_path(&entry.path))
                {
                    return Err(LixError::new(
                        LixError::CODE_CONSTRAINT_VIOLATION,
                        "native file deletion cannot implicitly uninstall a plugin archive",
                    ));
                }
                let blob_key = TrackedStateKey {
                    schema_key: "lix_binary_blob_ref".into(),
                    file_id: Some(id.clone()),
                    row_pk: native_file_uuid_key(&id)?,
                };
                let mut reader = self.tracked_state_reader().await?;
                let blob = reader
                    .load_projected_batch_at_commit(
                        &target_id,
                        &[blob_key],
                        &ChangeRecordProjection::identity_only(),
                    )
                    .await?;
                let has_blob_ref = blob.row(0).is_some_and(|row| !row.deleted());
                drop(reader);
                rows.append(
                    plan_file_delete(FileDeleteInput {
                        file_id: id.clone(),
                        has_blob_ref,
                        context: FilesystemRowContext {
                            branch_id: branch.clone(),
                            global: false,
                            untracked: false,
                            file_id: Some(id),
                            metadata: None,
                        },
                    })
                    .rows,
                );
            }
            self.stage_write(TransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows,
            })
            .await?;
        }
        Ok(Some(next))
    }

    /// Retire an existing file when an accepted incoming descriptor claims its
    /// name. This uses the ordinary deletion owner so plugin rows and blobs are
    /// retired together. Directory occupants and plugin archives retain their
    /// existing validation rules; neither is implicitly recursively deleted.
    pub(crate) async fn retire_incoming_file_path_occupants(
        &mut self,
        analysis: &crate::session::MergeAnalysis,
        semantic_rows: &RawWriteBatch,
    ) -> Result<(), LixError> {
        let plan = analysis
            .merge_plan()
            .expect("native application has a plan");
        let mut changes = BTreeMap::<String, Option<(Option<String>, String)>>::new();
        let mut keys = Vec::new();
        for pick in &plan.picks {
            if pick.identity.schema_key() == "lix_file_descriptor" {
                keys.push(TrackedStateKey {
                    schema_key: pick.identity.schema_key().into(),
                    file_id: pick.identity.file_id().map(str::to_owned),
                    row_pk: pick.identity.row_pk().clone(),
                });
            }
        }
        if !keys.is_empty() {
            let mut reader = self.tracked_state_reader().await?;
            let rows = reader
                .load_projected_batch_at_commit(
                    &analysis.commits.source_commit_id.to_string(),
                    &keys,
                    &ChangeRecordProjection::full(),
                )
                .await?;
            for (index, key) in keys.iter().enumerate() {
                let row = rows.row(index).ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "incoming descriptor selection is absent",
                    )
                })?;
                let value = if row.deleted() {
                    None
                } else {
                    let json = if let Some(typed) = row.decoded_snapshot() {
                        typed.to_json_shared()?
                    } else {
                        row.snapshot_content().cloned().ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                "incoming descriptor has no payload",
                            )
                        })?
                    };
                    Some(
                        serde_json::from_str::<JsonValue>(&json)
                            .map_err(|e| LixError::unknown(e.to_string()))?,
                    )
                };
                changes.insert(
                    key.row_pk.as_single_string_owned()?,
                    descriptor_name(value.as_ref())?,
                );
            }
        }
        for row in semantic_rows
            .iter()
            .filter(|row| row.schema_key.as_str() == "lix_file_descriptor")
        {
            let id = row
                .row_pk
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "resolved descriptor has no identity",
                    )
                })?
                .as_single_string_owned()?;
            let value = row.snapshot_json();
            changes.insert(id, descriptor_name(value.map(|value| &**value))?);
        }
        if !changes.values().any(Option::is_some) {
            return Ok(());
        }
        // This is the existing namespace metadata dependency, shared with
        // descriptor write validation. It does not load file contents. A cold
        // namespace index currently materializes branch descriptor metadata.
        let branch = self.active_branch_id().to_owned();
        let index = self
            .filesystem_path_index(&FilesystemPathIndexRequest::new(vec![branch.clone()]))
            .await?;
        let incoming_names = changes.values().flatten().collect::<BTreeSet<_>>();
        let mut deletes = RawWriteBatch::new();
        for entry in index.entries() {
            if entry.kind != FilesystemPathKind::File
                || entry.key.global()
                || entry.key.is_untracked()
                || entry.key.branch_id() != branch
                || changes.contains_key(entry.id())
                || !incoming_names.contains(&(entry.parent_id.clone(), entry.name.clone()))
            {
                continue;
            }
            if is_plugin_storage_path(&entry.path) {
                return Err(LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    "incoming file path cannot implicitly uninstall a plugin archive",
                ));
            }
            let key = TrackedStateKey {
                schema_key: "lix_binary_blob_ref".into(),
                file_id: Some(entry.id().into()),
                row_pk: native_file_uuid_key(entry.id())?,
            };
            let has_blob_ref = {
                let mut reader = self.tracked_state_reader().await?;
                let rows = reader
                    .load_projected_batch_at_commit(
                        &analysis.commits.target_commit_id.to_string(),
                        &[key],
                        &ChangeRecordProjection::full(),
                    )
                    .await?;
                rows.row(0).is_some_and(|row| !row.deleted())
            };
            deletes.append(
                plan_file_delete(FileDeleteInput {
                    file_id: entry.id().into(),
                    has_blob_ref,
                    context: FilesystemRowContext {
                        branch_id: branch.clone(),
                        global: false,
                        untracked: false,
                        file_id: Some(entry.id().into()),
                        metadata: None,
                    },
                })
                .rows,
            );
        }
        if !deletes.is_empty() {
            self.stage_write(TransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: deletes,
            })
            .await?;
        }
        Ok(())
    }
}

fn descriptor_name(
    value: Option<&JsonValue>,
) -> Result<Option<(Option<String>, String)>, LixError> {
    value
        .map(|value| {
            let name = value
                .get("name")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_SCHEMA_VALIDATION,
                        "file descriptor has no name",
                    )
                })?;
            let directory = match value.get("directory_id") {
                None | Some(JsonValue::Null) => None,
                Some(JsonValue::String(id)) => Some(id.clone()),
                _ => {
                    return Err(LixError::new(
                        LixError::CODE_SCHEMA_VALIDATION,
                        "invalid directory identity",
                    ));
                }
            };
            Ok((directory, name.into()))
        })
        .transpose()
}

fn native_file_descriptor_json(
    row: crate::tracked_state::MaterializedTrackedStateRowRef<'_>,
) -> Result<JsonValue, LixError> {
    let json = if let Some(typed) = row.decoded_snapshot() {
        typed.to_json_shared()?
    } else {
        row.snapshot_content().cloned().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "native file descriptor payload is absent",
            )
        })?
    };
    serde_json::from_str(&json).map_err(|error| LixError::unknown(error.to_string()))
}
fn native_file_lifecycle_pick(
    row: crate::tracked_state::MaterializedTrackedStateRowRef<'_>,
) -> Result<crate::tracked_state::TrackedStateMergePick, LixError> {
    let identity =
        crate::tracked_state::TrackedStateDiffIdentity::from_key_batch(vec![TrackedStateKey {
            schema_key: row.schema_key().into(),
            file_id: row.file_id().map(str::to_owned),
            row_pk: row.row_pk().clone(),
        }])?
        .pop()
        .expect("one native identity");
    Ok(crate::tracked_state::TrackedStateMergePick {
        identity: identity.clone(),
        change_id: row.change_id(),
        selected_row: crate::tracked_state::TrackedStateDiffRow {
            identity,
            change_id: row.change_id(),
            commit_id: row.commit_id(),
            deleted: row.deleted(),
            created_at: row.created_at(),
            updated_at: row.updated_at(),
        },
    })
}

// Native built-in structural primary keys are typed UUIDs, not string keys.
fn native_file_uuid_key(id: &str) -> Result<RowPk, LixError> {
    RowPk::uuid_from_canonical(id).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("native file lifecycle contains a noncanonical structural ID: {error}"),
        )
    })
}

#[cfg(test)]
mod ancestor_restore_tests {
    use crate::{CreateBranchOptions, MergeBranchOptions, open_lix};

    #[tokio::test]
    async fn file_restore_stops_at_unchanged_effective_target_directory() {
        let target = open_lix().await.unwrap();
        target.execute("INSERT INTO lix_file(path,content) VALUES('/old/child/note.txt',CAST('base' AS BYTEA))", &[]).await.unwrap();
        let branch = target
            .create_branch(CreateBranchOptions {
                id: None,
                name: "incoming".into(),
                from_commit_id: None,
            })
            .await
            .unwrap();
        let source = target
            .open_another_session()
            .with_branch(branch.id.clone())
            .await
            .unwrap();
        source.execute("UPDATE lix_file SET content=CAST('restored' AS BYTEA) WHERE path='/old/child/note.txt'", &[]).await.unwrap();
        for sql in [
            "DELETE FROM lix_file WHERE path='/old/child/note.txt'",
            "UPDATE lix_directory SET path='/moved' WHERE path='/old/child'",
            "DELETE FROM lix_directory WHERE path='/old'",
        ] {
            target.execute(sql, &[]).await.unwrap();
        }
        target
            .merge_branch(MergeBranchOptions {
                source_branch_id: branch.id,
            })
            .await
            .unwrap();
        let restored = target.execute("SELECT id FROM lix_file WHERE path='/moved/note.txt' AND content=CAST('restored' AS BYTEA)", &[]).await.unwrap();
        assert_eq!(restored.rows().len(), 1);
        let obsolete = target
            .execute("SELECT id FROM lix_directory WHERE path='/old'", &[])
            .await
            .unwrap();
        assert!(
            obsolete.rows().is_empty(),
            "restoration must not revive the moved directory's obsolete parent"
        );
    }

    #[tokio::test]
    async fn incoming_directory_pick_retains_its_required_source_parent() {
        let target = open_lix().await.unwrap();
        target.execute("INSERT INTO lix_file(path,content) VALUES('/old/child/note.txt',CAST('base' AS BYTEA))", &[]).await.unwrap();
        target
            .execute("INSERT INTO lix_directory(path) VALUES('/new')", &[])
            .await
            .unwrap();
        let branch = target
            .create_branch(CreateBranchOptions {
                id: None,
                name: "incoming".into(),
                from_commit_id: None,
            })
            .await
            .unwrap();
        let source = target
            .open_another_session()
            .with_branch(branch.id.clone())
            .await
            .unwrap();
        source
            .execute(
                "UPDATE lix_directory SET path='/new/child' WHERE path='/old/child'",
                &[],
            )
            .await
            .unwrap();
        source.execute("UPDATE lix_file SET content=CAST('restored' AS BYTEA) WHERE path='/new/child/note.txt'", &[]).await.unwrap();
        target
            .execute("DELETE FROM lix_file WHERE path='/old/child/note.txt'", &[])
            .await
            .unwrap();
        target
            .execute("DELETE FROM lix_directory WHERE path='/new'", &[])
            .await
            .unwrap();
        target
            .merge_branch(MergeBranchOptions {
                source_branch_id: branch.id,
            })
            .await
            .unwrap();
        let restored = target.execute("SELECT id FROM lix_file WHERE path='/new/child/note.txt' AND content=CAST('restored' AS BYTEA)", &[]).await.unwrap();
        assert_eq!(restored.rows().len(), 1);
    }
}
