//! Statement and publication adapters for the shared referential-action planner.
use super::*;
use crate::transaction::{schema_resolver, staging, validation};

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
        let seeds = self
            .capture_delete_action_seeds(&prepared.state_rows, Some(&prepared.state_rows))
            .await?;
        if seeds.is_empty() {
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
                deletes.append(validation::plan_delete_actions(&candidate, catalog, seeds).await?);
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
                (!pending.contains(&(
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
        if deletes.is_empty() {
            return Ok(());
        }
        self.stage_planned_cascade_deletes(deletes).await?;
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
        if generated.state_rows.iter().any(|row| {
            pending.contains(&(
                row.branch_id.to_string(),
                row.untracked,
                row.schema_key.to_string(),
                row.file_id.map(ToString::to_string),
                row.row_pk.clone(),
            )) || row
                .file_id
                .is_some_and(|file| pending_files.contains(file.as_str()))
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
            if catalog
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
                deletes.append(validation::plan_delete_actions(&candidate, catalog, seeds).await?);
            }
        }
        if !deletes.is_empty() {
            self.stage_planned_cascade_deletes(deletes).await?;
        }
        Ok(())
    }
}
