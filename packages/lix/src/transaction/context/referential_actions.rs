//! Statement and publication adapters for the shared referential-action planner.
use super::*;
use crate::transaction::{schema_resolver, staging, validation};
use crate::transaction_types::PreparedStateRowRef;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct FileDeleteDescriptor {
    pub(super) branch_id: String,
    pub(super) global: bool,
    pub(super) untracked: bool,
    pub(super) file_id: String,
}

pub(super) struct FileDeleteSemanticClosure {
    pub(super) tombstones: RawWriteBatch,
    pub(super) seeds: Vec<MaterializedHotStateRow>,
}

impl<S: Storage + Clone + Send + Sync + 'static> Transaction<S> {
    /// Reconcile actions with the coherent publication snapshot. Only newly
    /// committed dependents are eligible here: a later pending insert/update
    /// must still fail its FK check instead of being silently erased at commit.
    pub(super) async fn reconcile_delete_actions(
        &mut self,
        prepared: &mut PreparedWriteSet,
    ) -> Result<(), LixError> {
        if !prepared.state_rows.iter().any(|row| row.is_deleted()) {
            return Ok(());
        }
        if prepared
            .state_rows
            .iter()
            .any(|row| row.schema_key.as_str() == REGISTERED_SCHEMA_KEY)
        {
            self.schema_resolver.clear_cached_catalogs();
        }
        let file_deletes = prepared
            .state_rows
            .iter()
            .filter_map(file_delete_descriptor_from_prepared_row)
            .collect::<BTreeSet<_>>();
        let file_closure = self
            .capture_file_delete_semantic_closure(
                &file_deletes,
                &prepared.state_rows,
                Some(&prepared.state_rows),
            )
            .await?;
        let staged_file_closure = !file_closure.tombstones.is_empty();
        if staged_file_closure {
            self.stage_file_delete_semantic_tombstones(file_closure.tombstones)
                .await?;
        }
        let seeds = self
            .capture_delete_action_seeds(&prepared.state_rows, Some(&prepared.state_rows))
            .await?
            .into_iter()
            .chain(file_closure.seeds)
            .collect::<Vec<_>>();
        if seeds.is_empty() && !staged_file_closure {
            return Ok(());
        }
        let mut by_branch = BTreeMap::<String, Vec<MaterializedHotStateRow>>::new();
        for seed in seeds {
            by_branch
                .entry(seed.branch_id.to_string())
                .or_default()
                .push(seed);
        }
        let mut deletes = RawWriteBatch::new();
        {
            let read = self.opening_read();
            let base = self.hot_state.reader(&read);
            let staged = staging::PreparedSchemaOverlay::all_rows(&prepared.state_rows);
            let candidate = schema_resolver::TransactionSchemaHotStateReader {
                base: &base,
                staged: &staged,
            };
            for (branch, seeds) in by_branch {
                let (_, catalog) = self
                    .schema_resolver
                    .catalogs_for_validation(&base, &staged, &Domain::schema_catalog(branch, true))
                    .await?;
                deletes.append(
                    validation::plan_delete_actions(&candidate, catalog, seeds, None).await?,
                );
            }
        }
        let pending = prepared
            .state_rows
            .iter()
            .map(|row| {
                (
                    row.branch_id.to_string(),
                    row.untracked,
                    row.schema_key.to_string(),
                    row.file_id.map(ToString::to_string),
                    row.row_pk.clone(),
                )
            })
            .collect::<BTreeSet<_>>();
        let keep = deletes
            .iter()
            .enumerate()
            .filter_map(|(index, row)| {
                (row.snapshot.is_some()
                    || !pending.contains(&(
                        row.branch_id.to_string(),
                        row.untracked,
                        row.schema_key.to_string(),
                        row.file_id.map(ToString::to_string),
                        row.row_pk.expect("cascade identity").clone(),
                    )))
                .then_some(index)
            })
            .collect::<Vec<_>>();
        deletes = deletes.take_rows(&keep);
        if !deletes.is_empty() {
            self.stage_planned_cascade_deletes(deletes).await?;
        } else if !staged_file_closure {
            return Ok(());
        }
        let mut generated = self.staged_writes.drain()?;
        // Plugin materialization reads the coherent durable file, while the
        // original prepared rows are outside the mutable staging buffer. If
        // reconciliation touches pending work, retry rather than overwrite it
        // with a file projection that omitted that work.
        let pending_files = prepared
            .state_rows
            .iter()
            .filter_map(|row| row.file_id.map(ToString::to_string))
            .chain(
                prepared
                    .file_content_writes
                    .iter()
                    .map(|write| write.file_id.clone()),
            )
            .collect::<BTreeSet<_>>();
        let deleted_file_ids = file_deletes
            .iter()
            .map(|descriptor| descriptor.file_id.as_str())
            .collect::<BTreeSet<_>>();
        if generated.state_rows.iter().any(|row| {
            (row.is_deleted()
                && pending.contains(&(
                    row.branch_id.to_string(),
                    row.untracked,
                    row.schema_key.to_string(),
                    row.file_id.map(ToString::to_string),
                    row.row_pk.clone(),
                )))
                || row.file_id.is_some_and(|file| {
                    pending_files.contains(file.as_str())
                        && !(row.is_deleted() && deleted_file_ids.contains(file.as_str()))
                })
        }) || generated
            .file_content_writes
            .iter()
            .any(|write| pending_files.contains(&write.file_id))
        {
            return Err(LixError::new(
                LixError::CODE_TRANSACTION_CONFLICT,
                "concurrent cascade requires reconciliation with pending file or row writes",
            )
            .with_hint("Retry the transaction against the latest committed state."));
        }
        for index in 0..generated.state_rows.len() {
            let row = generated.state_rows.row(index);
            if !row.untracked {
                let commit_id = prepared
                    .commit_change_refs_by_branch
                    .get(row.branch_id.as_str())
                    .ok_or_else(|| LixError::unknown("cascade branch has no publication commit"))?
                    .commit_id;
                generated.state_rows.set_commit_id(index, Some(commit_id));
            }
        }
        prepared.replace_reconciled_writes(generated, &BTreeSet::new());
        Ok(())
    }

    /// File deletion is represented durably by the descriptor tombstone. HOT
    /// state applies that tombstone as a collection-wide hide, so row-reference
    /// validation needs a finite semantic preimage of the same file. Keep this
    /// expansion here, beside the ordinary planner, so SQL deletes and native
    /// merge deletes share exactly the same target identities.
    pub(super) async fn capture_file_delete_semantic_closure(
        &mut self,
        descriptors: &BTreeSet<FileDeleteDescriptor>,
        skip_rows: &PreparedStateBatch,
        prepared_schema_rows: Option<&PreparedStateBatch>,
    ) -> Result<FileDeleteSemanticClosure, LixError> {
        if descriptors.is_empty() {
            return Ok(FileDeleteSemanticClosure {
                tombstones: RawWriteBatch::new(),
                seeds: Vec::new(),
            });
        }
        let read = self.opening_read();
        let base = self.hot_state.reader(&read);
        let staged = self.staged_writes.staging_overlay()?;
        let prepared_schemas = prepared_schema_rows.map(staging::PreparedSchemaOverlay::new);
        let catalog_overlay: &(dyn StagedHotStateRows + Sync) = match &prepared_schemas {
            Some(overlay) => overlay,
            None => &staged,
        };
        let mut skip = skip_rows
            .iter()
            .filter(|row| row.is_deleted())
            .map(|row| {
                (
                    row.branch_id.to_string(),
                    row.untracked,
                    row.schema_key.to_string(),
                    row.file_id.map(ToString::to_string),
                    row.row_pk.clone(),
                )
            })
            .collect::<BTreeSet<_>>();
        let mut tombstones = RawWriteBatch::new();
        let mut seeds = Vec::new();
        for descriptor in descriptors {
            // Catalog declarations are branch scoped. Use the tracked schema
            // catalog even when the deleted target is in an untracked lane;
            // row-reference sources can be fileless or live in either lane.
            let catalog_branch = if descriptor.global {
                self.active_branch_id().to_owned()
            } else {
                descriptor.branch_id.clone()
            };
            let catalog_domain = Domain::schema_catalog(catalog_branch, true);
            let (_, catalog) = self
                .schema_resolver
                .catalogs_for_validation(&base, catalog_overlay, &catalog_domain)
                .await?;
            // A file without row-reference declarations has no dynamic target
            // relation to protect. Preserve the descriptor-only fast path.
            if catalog.row_ref_references().is_empty() {
                continue;
            }
            // The catalog lookup deliberately uses the tracked schema lane,
            // but the rows being expanded belong to the descriptor's exact
            // branch/lane/file. Keep those domains separate: using the
            // catalog domain here would silently discard tracked rows (and
            // admit only untracked rows) before the semantic tombstones are
            // handed to the referential-action planner.
            let data_domain = Domain::exact_file(
                descriptor.branch_id.clone(),
                descriptor.untracked,
                Some(descriptor.file_id.clone()),
            );
            let batch = overlay_scan_batch(
                &base,
                &staged,
                &HotStateScanRequest {
                    filter: HotStateFilter {
                        branch_ids: vec![descriptor.branch_id.clone()],
                        file_ids: vec![NullableKeyFilter::Value(descriptor.file_id.clone())],
                        untracked: Some(descriptor.untracked),
                        include_tombstones: false,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await?;
            for row in batch.iter() {
                if !data_domain.contains_ref(row) {
                    continue;
                }
                if is_file_delete_control_schema(row.schema_key()) {
                    continue;
                }
                let identity = (
                    row.branch_id().to_string(),
                    row.untracked(),
                    row.schema_key().to_string(),
                    row.file_id().map(ToString::to_string),
                    row.row_pk().clone(),
                );
                if !skip.insert(identity) {
                    continue;
                }
                seeds.push(validation::delete_action_seed(row)?);
                tombstones.push_parts(
                    Some(row.row_pk().clone()),
                    row.schema_key().into(),
                    row.file_id().map(Into::into),
                    None,
                    None,
                    None,
                    None,
                    None,
                    row.global(),
                    None,
                    None,
                    row.untracked(),
                    row.branch_id().into(),
                );
            }
        }
        Ok(FileDeleteSemanticClosure { tombstones, seeds })
    }

    pub(super) async fn stage_file_delete_semantic_rows_for_descriptors(
        &mut self,
        descriptors: &BTreeSet<FileDeleteDescriptor>,
    ) -> Result<Vec<MaterializedHotStateRow>, LixError> {
        let closure = self
            .capture_file_delete_semantic_closure(descriptors, &PreparedStateBatch::new(), None)
            .await?;
        if closure.tombstones.is_empty() {
            return Ok(closure.seeds);
        }
        let seeds = closure.seeds;
        self.stage_file_delete_semantic_tombstones(closure.tombstones)
            .await?;
        Ok(seeds)
    }

    pub(super) async fn stage_file_delete_semantic_tombstones(
        &mut self,
        rows: RawWriteBatch,
    ) -> Result<(), LixError> {
        let previous = std::mem::take(&mut self.planned_cascade_deletes);
        self.planned_cascade_deletes = rows
            .iter()
            .map(|row| {
                DomainRowIdentity::new(
                    Domain::exact_file(
                        row.branch_id.to_string(),
                        row.untracked,
                        row.file_id.map(ToString::to_string),
                    ),
                    row.schema_key.to_string(),
                    row.row_pk.expect("file delete semantic identity").clone(),
                )
            })
            .collect();
        // These rows are already proven members of a file whose descriptor is
        // being deleted. Bypass plugin semantic reconciliation: its actor and
        // materialization are intentionally being retired by the descriptor
        // lifecycle, and invoking it would try to reopen an absent file.
        let result = self
            .stage_write_inner_with_recovery(
                TransactionWrite::Rows {
                    mode: TransactionWriteMode::Replace,
                    rows,
                },
                None,
                true,
                BTreeSet::new(),
            )
            .await;
        self.planned_cascade_deletes = previous;
        result.map(|_| ())
    }

    pub(super) async fn capture_delete_action_seeds(
        &mut self,
        rows: &PreparedStateBatch,
        prepared_schema_rows: Option<&PreparedStateBatch>,
    ) -> Result<Vec<MaterializedHotStateRow>, LixError> {
        if !rows.iter().any(|row| row.is_deleted()) {
            return Ok(Vec::new());
        }
        let read = self.opening_read();
        let base = self.hot_state.reader(&read);
        let staged = self.staged_writes.staging_overlay()?;
        let prepared_schemas = prepared_schema_rows.map(staging::PreparedSchemaOverlay::new);
        let catalog_overlay: &(dyn StagedHotStateRows + Sync) = match &prepared_schemas {
            Some(overlay) => overlay,
            None => &staged,
        };

        let mut requests = BTreeMap::<bool, Vec<HotStateExactRowRequest>>::new();
        for row in rows.iter().filter(|row| row.is_deleted()) {
            let domain = Domain::exact_file(
                row.branch_id.to_string(),
                row.untracked,
                row.file_id.map(ToString::to_string),
            );
            if self
                .planned_cascade_deletes
                .contains(&DomainRowIdentity::new(
                    domain.clone(),
                    row.schema_key.to_string(),
                    row.row_pk.clone(),
                ))
            {
                continue;
            }
            let (_, catalog) = self
                .schema_resolver
                .catalogs_for_validation(&base, catalog_overlay, &domain)
                .await?;
            if catalog.has_row_ref_delete_actions()
                || catalog
                    .delete_plan_for_key(row.schema_key.as_str())
                    .foreign_key_references
                    .iter()
                    .any(|reference| {
                        reference.foreign_key.on_delete == lix_schema::DeleteAction::Cascade
                    })
            {
                requests
                    .entry(row.untracked)
                    .or_default()
                    .push(HotStateExactRowRequest {
                        schema_key: row.schema_key.to_string(),
                        branch_id: row.branch_id.to_string(),
                        row_pk: row.row_pk.clone(),
                        file_id: row.file_id.map(ToString::to_string),
                    });
            }
        }
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        // Read the preimage, excluding prepared tombstones at publication.
        let mut seeds = Vec::new();
        for (untracked, requests) in requests {
            let batch = overlay_load_exact_batch(
                &base,
                &staged,
                &HotStateExactBatchRequest {
                    rows: requests,
                    projection: Default::default(),
                    untracked: Some(untracked),
                    include_tombstones: false,
                },
            )
            .await?
            .into_present_batch();
            seeds.extend(
                batch
                    .iter()
                    .map(validation::delete_action_seed)
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        Ok(seeds)
    }

    pub(crate) async fn stage_planned_cascade_deletes(
        &mut self,
        rows: RawWriteBatch,
    ) -> Result<(), LixError> {
        let previous = std::mem::take(&mut self.planned_cascade_deletes);
        self.planned_cascade_deletes = rows
            .iter()
            .map(|row| {
                DomainRowIdentity::new(
                    Domain::exact_file(
                        row.branch_id.to_string(),
                        row.untracked,
                        row.file_id.map(ToString::to_string),
                    ),
                    row.schema_key.to_string(),
                    row.row_pk.expect("cascade identity").clone(),
                )
            })
            .collect();
        // Reconciliation can itself produce new plugin deletes. Those still
        // enter the planner; only the closure already computed is suppressed.
        let result = Box::pin(self.stage_write_inner(
            TransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows,
            },
            None,
        ))
        .await;
        self.planned_cascade_deletes = previous;
        result.map(|_| ())
    }

    pub(super) async fn stage_delete_actions(
        &mut self,
        seeds: Vec<MaterializedHotStateRow>,
    ) -> Result<(), LixError> {
        let mut by_branch = BTreeMap::<String, Vec<MaterializedHotStateRow>>::new();
        for row in seeds {
            by_branch
                .entry(row.branch_id.to_string())
                .or_default()
                .push(row);
        }
        let mut deletes = RawWriteBatch::new();
        {
            let read = self.opening_read();
            let base = self.hot_state.reader(&read);
            let staged = self.staged_writes.staging_overlay()?;
            let candidate = schema_resolver::TransactionSchemaHotStateReader {
                base: &base,
                staged: &staged,
            };
            for (branch, seeds) in by_branch {
                let (_, catalog) = self
                    .schema_resolver
                    .catalogs_for_validation(&base, &staged, &Domain::schema_catalog(branch, true))
                    .await?;
                deletes.append(
                    validation::plan_delete_actions(&candidate, catalog, seeds, None).await?,
                );
            }
        }
        if !deletes.is_empty() {
            self.stage_planned_cascade_deletes(deletes).await?;
        }
        Ok(())
    }
}

pub(super) fn file_delete_descriptors_from_prepared(
    rows: &PreparedStateBatch,
) -> BTreeSet<FileDeleteDescriptor> {
    rows.iter()
        .filter_map(file_delete_descriptor_from_prepared_row)
        .collect()
}

fn file_delete_descriptor_from_prepared_row(
    row: PreparedStateRowRef<'_>,
) -> Option<FileDeleteDescriptor> {
    if row.schema_key.as_str() != FILE_DESCRIPTOR_SCHEMA_KEY || !row.is_deleted() {
        return None;
    }
    Some(FileDeleteDescriptor {
        branch_id: row.branch_id.to_string(),
        global: row.global,
        untracked: row.untracked,
        file_id: row.file_id?.to_string(),
    })
}

fn is_file_delete_control_schema(schema_key: &str) -> bool {
    matches!(
        schema_key,
        FILE_DESCRIPTOR_SCHEMA_KEY
            | BLOB_REF_SCHEMA_KEY
            | REGISTERED_SCHEMA_KEY
            | BRANCH_REF_SCHEMA_KEY
            | "lix_directory_descriptor"
            | crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
    )
}
