#![allow(
    clippy::manual_let_else,
    clippy::option_if_let_else,
    clippy::redundant_closure,
    clippy::unnecessary_literal_bound,
    clippy::unnecessary_wraps
)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, BooleanArray, RecordBatchOptions, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, DataFusionError, Result, ScalarValue};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::{PhysicalExpr, create_physical_expr};
use datafusion::prelude::SessionContext;
use futures_util::FutureExt;
use serde::Deserialize;

use crate::branch::BranchRefReader;
use crate::filesystem::{
    FilesystemPathIndexReader, FilesystemPathIndexRequest, FilesystemPathKind,
    FilesystemPathSelection,
};
use crate::functions::FunctionProviderHandle;
use crate::live_state::MaterializedLiveStateRow;
use crate::live_state::{
    LiveStateFilter, LiveStateProjection, LiveStateReader, LiveStateScanRequest,
};
use crate::plugin::{is_plugin_storage_path, reject_normal_plugin_storage_mutation};
use crate::sql2::branch_scope::{
    BranchBinding, explicit_branch_ids_from_dml_filters, resolve_provider_branch_ids,
    resolve_write_branch_scope,
};
use crate::sql2::predicate_typecheck::{
    canonicalize_json_identity_text_filters, validate_json_predicate_filters,
};
use crate::sql2::write_normalization::{InsertCell, SqlCell, UpdateAssignmentValues};
use crate::transaction::types::{
    LogicalPrimaryKey, TransactionJson, TransactionWriteOperation, TransactionWriteOrigin,
    TransactionWriteRow,
};
use crate::{GLOBAL_BRANCH_ID, LixError, parse_row_metadata_value, serialize_row_metadata};

use crate::filesystem::{
    DirectoryDescriptorWriteIntent, DirectoryPathRecord, DirectoryPathResolver,
    FilesystemDeletePlan, FilesystemDescriptorKey, FilesystemRowContext, VisibleFilesystem,
    create_directory_path_with_leaf_id_with_resolvers, derive_directory_paths,
    directory_descriptor_write_row, directory_path_resolvers_from_live_state,
    filesystem_storage_scope_key, plan_parsed_directory_path_update_with_resolvers,
    plan_recursive_directory_delete,
};
use crate::sql2::result_metadata::json_field;
use crate::sql2::{SqlWriteContext, WriteAccess, WriteContextLiveStateReader};
use crate::transaction::types::{TransactionWrite, TransactionWriteMode};

use super::file::{FilePathPredicate, file_path_predicate_from_filters, indexed_path_matches};
use super::spec::{
    PlannedDml, PlannedScan, RowSource, TableSpec, finish_scan_batch, projected_schema,
    register_spec_table, row_source,
};
use super::upsert::{
    StagedUpsert, UpsertConflictKind, UpsertConflictTarget, UpsertSupport, validate_target_columns,
};
use crate::entity_pk::EntityPk;

const DIRECTORY_SCHEMA_KEY: &str = "lix_directory_descriptor";

/// Physical-identity column the upsert driver matches conflicting rows on.
/// A directory's identity is its `id`; the underlying live state keys on the
/// directory id as a single-element entity primary key.
const LIX_DIRECTORY_IDENTITY: &[&str] = &["id"];
const LIX_DIRECTORY_PATH_IDENTITY: &[&str] = &["path"];
const LIX_DIRECTORY_BY_BRANCH_PATH_IDENTITY: &[&str] = &["path", "lixcol_branch_id"];

pub(super) async fn register_lix_directory_active_provider(
    session: &SessionContext,
    surface_name: &str,
    active_branch_id: &str,
    live_state: Arc<dyn LiveStateReader>,
    filesystem_path_index: Arc<dyn FilesystemPathIndexReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    functions: FunctionProviderHandle,
) -> Result<(), LixError> {
    register_spec_table(
        session,
        surface_name,
        Arc::new(LixDirectorySpec::active_branch(
            active_branch_id,
            live_state,
            filesystem_path_index,
            branch_ref,
            functions,
        )),
        WriteAccess::read_only(),
    )
}

pub(super) async fn register_lix_directory_by_branch_provider(
    session: &SessionContext,
    surface_name: &str,
    live_state: Arc<dyn LiveStateReader>,
    filesystem_path_index: Arc<dyn FilesystemPathIndexReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    functions: FunctionProviderHandle,
) -> Result<(), LixError> {
    register_spec_table(
        session,
        surface_name,
        Arc::new(LixDirectorySpec::by_branch(
            live_state,
            filesystem_path_index,
            branch_ref,
            functions,
        )),
        WriteAccess::read_only(),
    )
}

pub(super) async fn register_by_branch_write_provider(
    session: &SessionContext,
    surface_name: &str,
    write_ctx: SqlWriteContext,
    branch_ref: Arc<dyn BranchRefReader>,
) -> Result<(), LixError> {
    let functions = write_ctx.functions();
    let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx.clone()));
    let filesystem_path_index: Arc<dyn FilesystemPathIndexReader> = live_state.clone();
    register_spec_table(
        session,
        surface_name,
        Arc::new(LixDirectorySpec::by_branch(
            live_state,
            filesystem_path_index,
            branch_ref,
            functions,
        )),
        WriteAccess::write(write_ctx),
    )
}

pub(super) async fn register_active_write_provider(
    session: &SessionContext,
    surface_name: &str,
    write_ctx: SqlWriteContext,
    branch_ref: Arc<dyn BranchRefReader>,
) -> Result<(), LixError> {
    let active_branch_id = write_ctx.active_branch_id();
    let functions = write_ctx.functions();
    let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx.clone()));
    let filesystem_path_index: Arc<dyn FilesystemPathIndexReader> = live_state.clone();
    register_spec_table(
        session,
        surface_name,
        Arc::new(LixDirectorySpec::active_branch(
            active_branch_id,
            live_state,
            filesystem_path_index,
            branch_ref,
            functions,
        )),
        WriteAccess::write(write_ctx),
    )
}

struct LixDirectorySpec {
    schema: SchemaRef,
    live_state: Arc<dyn LiveStateReader>,
    filesystem_path_index: Arc<dyn FilesystemPathIndexReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    functions: FunctionProviderHandle,
    branch_binding: BranchBinding,
}

impl LixDirectorySpec {
    async fn indexed_path_matches(
        &self,
        request: &LiveStateScanRequest,
        filters: &[Expr],
    ) -> Result<Option<(FilesystemPathSelection, FilesystemPathSelection)>> {
        let predicate = file_path_predicate_from_filters(filters);
        if predicate == FilePathPredicate::All {
            return Ok(None);
        }
        let index = self
            .filesystem_path_index
            .path_index(&FilesystemPathIndexRequest::new(
                request.filter.branch_ids.clone(),
            ))
            .await
            .map_err(lix_error_to_datafusion_error)?;
        let selected = indexed_path_matches(
            Arc::clone(&index),
            &predicate,
            FilesystemPathKind::Directory,
        );
        let all = indexed_path_matches(
            index,
            &FilePathPredicate::All,
            FilesystemPathKind::Directory,
        );
        Ok(Some((selected, all)))
    }

    fn active_branch(
        active_branch_id: impl Into<String>,
        live_state: Arc<dyn LiveStateReader>,
        filesystem_path_index: Arc<dyn FilesystemPathIndexReader>,
        branch_ref: Arc<dyn BranchRefReader>,
        functions: FunctionProviderHandle,
    ) -> Self {
        Self {
            schema: lix_directory_schema(),
            live_state,
            filesystem_path_index,
            branch_ref,
            functions,
            branch_binding: BranchBinding::active(active_branch_id),
        }
    }

    fn by_branch(
        live_state: Arc<dyn LiveStateReader>,
        filesystem_path_index: Arc<dyn FilesystemPathIndexReader>,
        branch_ref: Arc<dyn BranchRefReader>,
        functions: FunctionProviderHandle,
    ) -> Self {
        Self {
            schema: lix_directory_by_branch_schema(),
            live_state,
            filesystem_path_index,
            branch_ref,
            functions,
            branch_binding: BranchBinding::explicit(),
        }
    }

    /// Resolve the candidate-row scan request for an UPDATE/DELETE, scoped by
    /// the explicit branch ids from the statement filters.
    async fn dml_scan_request(&self, filters: &[Expr]) -> Result<LiveStateScanRequest> {
        let mut request =
            lix_directory_scan_request(self.branch_binding.active_branch_id(), None, None);
        request.filter.branch_ids = explicit_branch_ids_from_dml_filters(filters);
        request.filter.branch_ids = resolve_provider_branch_ids(
            self.branch_ref.as_ref(),
            &self.branch_binding,
            request.filter.branch_ids,
        )
        .await
        .map_err(lix_error_to_datafusion_error)?;
        Ok(request)
    }

    /// Unprojected candidate-row source for UPDATE/DELETE. The full source
    /// batch is stashed into `captured` so DELETE's plugin-storage rejection
    /// can inspect every directory path, not just the filter-matched rows.
    fn dml_source(
        &self,
        write_ctx: &SqlWriteContext,
        request: LiveStateScanRequest,
        indexed_matches: Option<(FilesystemPathSelection, FilesystemPathSelection)>,
        captured: Arc<Mutex<Option<RecordBatch>>>,
    ) -> RowSource {
        row_source(
            (
                write_ctx.clone(),
                request,
                indexed_matches,
                Arc::clone(&self.schema),
                captured,
            ),
            |(write_ctx, request, indexed_matches, table_schema, captured)| async move {
                let (source_batch, all_directories_batch) =
                    if let Some((selected, all)) = indexed_matches.as_ref() {
                        (
                            indexed_lix_directory_record_batch(&table_schema, selected)
                                .map_err(lix_error_to_datafusion_error)?,
                            indexed_lix_directory_record_batch(&table_schema, all)
                                .map_err(lix_error_to_datafusion_error)?,
                        )
                    } else {
                        let rows = write_ctx
                            .scan_live_state(&request)
                            .await
                            .map_err(lix_error_to_datafusion_error)?;
                        let batch = lix_directory_record_batch(&table_schema, rows)
                            .map_err(lix_error_to_datafusion_error)?;
                        (batch.clone(), batch)
                    };
                *captured.lock().expect("dml source mutex poisoned") = Some(all_directories_batch);
                Ok(source_batch)
            },
        )
    }
}

#[async_trait]
impl TableSpec for LixDirectorySpec {
    fn table_name(&self) -> &str {
        "lix_directory"
    }

    fn upsert_support(&self) -> Option<&dyn UpsertSupport> {
        Some(self)
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn filter_pushdown(&self, _filter: &Expr) -> TableProviderFilterPushDown {
        TableProviderFilterPushDown::Exact
    }

    async fn plan_scan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        props: &ExecutionProps,
    ) -> Result<PlannedScan> {
        let output_schema = projected_schema(&self.schema, projection);
        let scan_limit = if filters.is_empty() { limit } else { None };
        let mut request = lix_directory_scan_request(
            self.branch_binding.active_branch_id(),
            Some(output_schema.as_ref()),
            scan_limit,
        );
        request.filter.branch_ids = resolve_provider_branch_ids(
            self.branch_ref.as_ref(),
            &self.branch_binding,
            request.filter.branch_ids,
        )
        .await
        .map_err(lix_error_to_datafusion_error)?;
        let filters = filters.to_vec();
        let mut indexed_matches = self
            .indexed_path_matches(&request, &filters)
            .await?
            .map(|(selected, _)| selected);
        if indexed_matches.is_none() && filters.is_empty() && output_schema.index_of("path").is_ok()
        {
            let index = self
                .filesystem_path_index
                .path_index(&FilesystemPathIndexRequest::new(
                    request.filter.branch_ids.clone(),
                ))
                .await
                .map_err(lix_error_to_datafusion_error)?;
            indexed_matches = Some(indexed_path_matches(
                index,
                &FilePathPredicate::All,
                FilesystemPathKind::Directory,
            ));
        }
        let df_schema = DFSchema::try_from(Arc::clone(&self.schema))?;
        validate_json_predicate_filters(self.schema.as_ref(), &filters)?;
        let physical_filters = filters
            .iter()
            .map(|expr| create_physical_expr(expr, &df_schema, props))
            .collect::<Result<Vec<_>>>()?;

        let ordering = indexed_matches.as_ref().map(|_| "path".to_string());
        Ok(PlannedScan {
            schema: Arc::clone(&output_schema),
            ordering,
            load: row_source(
                (
                    Arc::clone(&self.live_state),
                    Arc::clone(&self.schema),
                    output_schema,
                    projection.cloned(),
                    request,
                    indexed_matches,
                    physical_filters,
                    limit,
                ),
                |(
                    live_state,
                    batch_schema,
                    _output_schema,
                    projection,
                    request,
                    indexed_matches,
                    physical_filters,
                    limit,
                )| async move {
                    let batch = if let Some(indexed_matches) = indexed_matches.as_ref() {
                        indexed_lix_directory_record_batch(&batch_schema, indexed_matches)
                    } else {
                        let rows = live_state.scan_rows(&request).await.map_err(|error| {
                            DataFusionError::Execution(format!(
                                "sql2 lix_directory scan failed: {error}"
                            ))
                        })?;
                        lix_directory_record_batch(&batch_schema, rows)
                    }
                    .map_err(|error| {
                        DataFusionError::Execution(format!(
                            "sql2 lix_directory batch build failed: {error}"
                        ))
                    })?;
                    finish_scan_batch(
                        batch,
                        &physical_filters,
                        projection.as_deref(),
                        limit,
                        "lix_directory",
                    )
                },
            ),
        })
    }

    async fn stage_insert(
        &self,
        write_ctx: &SqlWriteContext,
        batches: Vec<RecordBatch>,
    ) -> Result<u64> {
        let surface_name = lix_directory_surface_name(&self.branch_binding);
        let mut path_resolvers = None;
        let mut rows = Vec::new();
        let mut count = 0_u64;
        for batch in batches {
            if path_resolvers.is_none() {
                path_resolvers = Some(
                    directory_path_resolvers_from_live_state(
                        Arc::new(WriteContextLiveStateReader::new(write_ctx.clone())),
                        self.branch_binding.active_branch_id(),
                    )
                    .await
                    .map_err(lix_error_to_datafusion_error)?,
                );
            }
            count = count
                .checked_add(u64::try_from(batch.num_rows()).map_err(|_| {
                    DataFusionError::Execution("lix_directory INSERT row count overflow".into())
                })?)
                .ok_or_else(|| {
                    DataFusionError::Execution("lix_directory INSERT row count overflow".into())
                })?;
            if record_batch_has_non_null_column(&batch, "path")? {
                rows.extend(lix_directory_write_rows_from_batch_with_path_resolvers(
                    &batch,
                    self.branch_binding.active_branch_id(),
                    surface_name,
                    path_resolvers
                        .as_mut()
                        .expect("path resolver should be initialized"),
                    &mut || self.functions.call_uuid_v7().to_string(),
                )?);
            } else {
                rows.extend(
                    lix_directory_write_rows_from_batch_with_options_and_path_resolvers(
                        &batch,
                        self.branch_binding.active_branch_id(),
                        surface_name,
                        true,
                        path_resolvers.as_mut(),
                        None,
                    )?,
                );
            }
        }

        write_ctx
            .stage_write(TransactionWrite::Rows {
                mode: TransactionWriteMode::Insert,
                rows,
            })
            .await
            .map_err(lix_error_to_datafusion_error)?;

        Ok(count)
    }

    fn validate_update_assignments(&self, assignments: &[(String, Expr)]) -> Result<()> {
        validate_lix_directory_update_assignments(&self.schema, assignments)
    }

    fn prepare_write_filters(&self, filters: Vec<Expr>) -> Result<Vec<Expr>> {
        let filters = canonicalize_json_identity_text_filters(self.schema.as_ref(), &filters)?;
        validate_json_predicate_filters(self.schema.as_ref(), &filters)?;
        Ok(filters)
    }

    async fn plan_delete(
        &self,
        write_ctx: SqlWriteContext,
        filters: &[Expr],
    ) -> Result<PlannedDml> {
        let request = self.dml_scan_request(filters).await?;
        let indexed_matches = self.indexed_path_matches(&request, filters).await?;
        let captured: Arc<Mutex<Option<RecordBatch>>> = Arc::new(Mutex::new(None));
        let branch_binding = self.branch_binding.clone();
        Ok(PlannedDml {
            source: self.dml_source(&write_ctx, request, indexed_matches, Arc::clone(&captured)),
            apply: Arc::new(move |matched_batch| {
                let write_ctx = write_ctx.clone();
                let branch_binding = branch_binding.clone();
                let captured = Arc::clone(&captured);
                async move {
                    let source_batch = captured
                        .lock()
                        .expect("dml source mutex poisoned")
                        .clone()
                        .ok_or_else(|| {
                            DataFusionError::Execution(
                                "lix_directory DELETE source batch missing".to_string(),
                            )
                        })?;
                    reject_lix_directory_delete_plugin_storage_paths(&matched_batch, &source_batch)
                        .map_err(lix_error_to_datafusion_error)?;
                    let branch_ids = directory_branch_ids_from_batch(
                        &matched_batch,
                        branch_binding.active_branch_id(),
                    )?;
                    let mut visible_filesystems = BTreeMap::new();
                    for branch_id in branch_ids {
                        visible_filesystems.insert(
                            branch_id.clone(),
                            VisibleFilesystem::load(
                                Arc::new(WriteContextLiveStateReader::new(write_ctx.clone())),
                                &branch_id,
                            )
                            .await
                            .map_err(lix_error_to_datafusion_error)?,
                        );
                    }
                    let (write_rows, count) = lix_directory_recursive_delete_rows_from_batch(
                        &matched_batch,
                        branch_binding.active_branch_id(),
                        &visible_filesystems,
                    )?;

                    if count > 0 {
                        write_ctx
                            .stage_write(TransactionWrite::Rows {
                                mode: TransactionWriteMode::Replace,
                                rows: write_rows,
                            })
                            .await
                            .map_err(lix_error_to_datafusion_error)?;
                    }

                    Ok(count)
                }
                .boxed()
            }),
        })
    }

    async fn plan_update(
        &self,
        write_ctx: SqlWriteContext,
        assignments: Vec<(String, Arc<dyn PhysicalExpr>)>,
        filters: &[Expr],
    ) -> Result<PlannedDml> {
        let request = self.dml_scan_request(filters).await?;
        let indexed_matches = self.indexed_path_matches(&request, filters).await?;
        let captured: Arc<Mutex<Option<RecordBatch>>> = Arc::new(Mutex::new(None));
        let branch_binding = self.branch_binding.clone();
        let functions = self.functions.clone();
        Ok(PlannedDml {
            source: self.dml_source(&write_ctx, request, indexed_matches, captured),
            apply: Arc::new(move |matched_batch| {
                let write_ctx = write_ctx.clone();
                let branch_binding = branch_binding.clone();
                let functions = functions.clone();
                let assignments = assignments.clone();
                async move {
                    let mut path_resolvers = directory_path_resolvers_from_live_state(
                        Arc::new(WriteContextLiveStateReader::new(write_ctx.clone())),
                        branch_binding.active_branch_id(),
                    )
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
                    let write_rows = lix_directory_update_write_rows_from_batch(
                        &matched_batch,
                        &assignments,
                        branch_binding.active_branch_id(),
                        &mut path_resolvers,
                        &mut || functions.call_uuid_v7().to_string(),
                    )?;
                    let count = u64::try_from(write_rows.len()).map_err(|_| {
                        DataFusionError::Execution("lix_directory UPDATE row count overflow".into())
                    })?;

                    if count > 0 {
                        write_ctx
                            .stage_write(TransactionWrite::Rows {
                                mode: TransactionWriteMode::Replace,
                                rows: write_rows,
                            })
                            .await
                            .map_err(lix_error_to_datafusion_error)?;
                    }

                    Ok(count)
                }
                .boxed()
            }),
        })
    }
}

#[async_trait]
impl UpsertSupport for LixDirectorySpec {
    fn conflict_identity_columns(&self) -> &[&'static str] {
        LIX_DIRECTORY_IDENTITY
    }

    fn resolve_conflict_target(
        &self,
        table_name: &str,
        target_columns: &[String],
    ) -> Result<UpsertConflictTarget> {
        if validate_target_columns(
            table_name,
            target_columns,
            LIX_DIRECTORY_IDENTITY,
            "conflict identity columns",
        )
        .is_ok()
        {
            return Ok(UpsertConflictTarget::id(LIX_DIRECTORY_IDENTITY));
        }

        let path_identity = match self.branch_binding {
            BranchBinding::Active { .. } => LIX_DIRECTORY_PATH_IDENTITY,
            BranchBinding::Explicit => LIX_DIRECTORY_BY_BRANCH_PATH_IDENTITY,
        };
        validate_target_columns(
            table_name,
            target_columns,
            path_identity,
            "path identity columns",
        )?;
        Ok(UpsertConflictTarget::path(path_identity))
    }

    /// Produce the staged INSERT rows for the non-conflicting proposed rows,
    /// replicating `stage_insert`'s row production exactly: seed the directory
    /// path resolvers from live state, then branch on whether the batch carries
    /// a non-null `path` column. Directories have no file data, so the result
    /// is plain state rows.
    async fn insert_staged_rows(
        &self,
        write_ctx: &SqlWriteContext,
        batch: &RecordBatch,
    ) -> Result<StagedUpsert> {
        let surface_name = lix_directory_surface_name(&self.branch_binding);
        let mut path_resolvers = directory_path_resolvers_from_live_state(
            Arc::new(WriteContextLiveStateReader::new(write_ctx.clone())),
            self.branch_binding.active_branch_id(),
        )
        .await
        .map_err(lix_error_to_datafusion_error)?;

        let rows = if record_batch_has_non_null_column(batch, "path")? {
            lix_directory_write_rows_from_batch_with_path_resolvers(
                batch,
                self.branch_binding.active_branch_id(),
                surface_name,
                &mut path_resolvers,
                &mut || self.functions.call_uuid_v7().to_string(),
            )?
        } else {
            lix_directory_write_rows_from_batch_with_options_and_path_resolvers(
                batch,
                self.branch_binding.active_branch_id(),
                surface_name,
                true,
                Some(&mut path_resolvers),
                None,
            )?
        };

        Ok(StagedUpsert::rows(rows))
    }

    /// Scan the existing directories that could conflict with `proposed`,
    /// scoped to the active/explicit branch and narrowed to the proposed
    /// directory ids, returned as a batch in this table's column schema (the
    /// same builder the scan path uses).
    async fn scan_conflict_candidates(
        &self,
        write_ctx: &SqlWriteContext,
        proposed: &RecordBatch,
        target: &UpsertConflictTarget,
    ) -> Result<RecordBatch> {
        let mut request =
            lix_directory_scan_request(self.branch_binding.active_branch_id(), None, None);
        if matches!(self.branch_binding, BranchBinding::Explicit) {
            request.filter.branch_ids = match target.kind() {
                UpsertConflictKind::Id => proposed_branch_ids(proposed)?,
                UpsertConflictKind::Path => {
                    required_proposed_branch_ids(proposed, "lix_directory")?
                }
            };
        }
        request.filter.branch_ids = resolve_provider_branch_ids(
            self.branch_ref.as_ref(),
            &self.branch_binding,
            request.filter.branch_ids,
        )
        .await
        .map_err(lix_error_to_datafusion_error)?;
        request.filter.entity_pks = match target.kind() {
            UpsertConflictKind::Id => proposed_directory_entity_pks(proposed)?,
            UpsertConflictKind::Path => {
                validate_required_paths(proposed, "lix_directory")?;
                Vec::new()
            }
        };

        let rows = write_ctx
            .scan_live_state(&request)
            .await
            .map_err(lix_error_to_datafusion_error)?;
        lix_directory_record_batch(&self.schema, rows).map_err(lix_error_to_datafusion_error)
    }

    fn validate_conflict_pair(
        &self,
        existing: &RecordBatch,
        existing_row: usize,
        proposed: &RecordBatch,
        proposed_row: usize,
        target: &UpsertConflictTarget,
    ) -> Result<()> {
        if target.kind() != UpsertConflictKind::Path {
            return Ok(());
        }
        let existing_untracked =
            optional_bool_value(existing, existing_row, "lixcol_untracked")?.unwrap_or(false);
        let proposed_untracked =
            optional_bool_value(proposed, proposed_row, "lixcol_untracked")?.unwrap_or(false);
        if existing_untracked == proposed_untracked {
            return Ok(());
        }
        let path = required_string_value(proposed, proposed_row, "path")?;
        Err(lix_error_to_datafusion_error(LixError::new(
            LixError::CODE_CONSTRAINT_VIOLATION,
            format!(
                "INSERT ON CONFLICT (path) on lix_directory cannot write {} path {path:?} over existing {} directory",
                lane_name(proposed_untracked),
                lane_name(existing_untracked)
            ),
        )))
    }

    /// Apply the `DO UPDATE` assignments to the augmented batch (existing
    /// directory columns plus `excluded.*` proposed columns), reusing the
    /// directory UPDATE row builder with the same path-resolver/uuid-generator
    /// threading `plan_update` uses. This supports every assignment shape the
    /// plain UPDATE supports — `path` (recursive), `parent_id`, `name`, and
    /// `lixcol_metadata` — because the augmented batch carries the existing
    /// directory's `id`, `path`, and context columns.
    async fn apply_conflict_update(
        &self,
        write_ctx: &SqlWriteContext,
        augmented: &RecordBatch,
        assignments: &[(String, Arc<dyn PhysicalExpr>)],
    ) -> Result<StagedUpsert> {
        let mut path_resolvers = directory_path_resolvers_from_live_state(
            Arc::new(WriteContextLiveStateReader::new(write_ctx.clone())),
            self.branch_binding.active_branch_id(),
        )
        .await
        .map_err(lix_error_to_datafusion_error)?;
        let rows = lix_directory_update_write_rows_from_batch(
            augmented,
            assignments,
            self.branch_binding.active_branch_id(),
            &mut path_resolvers,
            &mut || self.functions.call_uuid_v7().to_string(),
        )?;
        Ok(StagedUpsert::rows(rows))
    }
}

/// The proposed directory ids as single-element entity primary keys, used to
/// narrow the conflict-candidate live-state scan. Rows without an explicit
/// `id` (defaulted ids) contribute nothing — a generated id cannot collide
/// with an existing row.
fn proposed_directory_entity_pks(proposed: &RecordBatch) -> Result<Vec<EntityPk>> {
    let mut entity_pks = Vec::new();
    for row_index in 0..proposed.num_rows() {
        if let Some(id) = optional_string_value(proposed, row_index, "id")? {
            entity_pks.push(EntityPk::single(&id));
        }
    }
    Ok(entity_pks)
}

fn proposed_branch_ids(batch: &RecordBatch) -> Result<Vec<String>> {
    let mut branch_ids = BTreeSet::new();
    for row_index in 0..batch.num_rows() {
        if let Some(branch_id) = optional_string_value(batch, row_index, "lixcol_branch_id")? {
            branch_ids.insert(branch_id);
        }
    }
    Ok(branch_ids.into_iter().collect())
}

fn required_proposed_branch_ids(batch: &RecordBatch, table_name: &str) -> Result<Vec<String>> {
    let mut branch_ids = BTreeSet::new();
    for row_index in 0..batch.num_rows() {
        let branch_id = optional_string_value(batch, row_index, "lixcol_branch_id")?.ok_or_else(
            || {
                DataFusionError::Execution(format!(
                    "INSERT ON CONFLICT (path, lixcol_branch_id) on {table_name} requires non-null lixcol_branch_id"
                ))
            },
        )?;
        branch_ids.insert(branch_id);
    }
    Ok(branch_ids.into_iter().collect())
}

fn validate_required_paths(batch: &RecordBatch, table_name: &str) -> Result<()> {
    for row_index in 0..batch.num_rows() {
        if optional_string_value(batch, row_index, "path")?.is_none() {
            return Err(DataFusionError::Execution(format!(
                "INSERT ON CONFLICT (path) on {table_name} requires non-null path"
            )));
        }
    }
    Ok(())
}

fn lane_name(untracked: bool) -> &'static str {
    if untracked { "untracked" } else { "tracked" }
}

fn lix_directory_surface_name(branch_binding: &BranchBinding) -> &'static str {
    match branch_binding {
        BranchBinding::Active { .. } => "lix_directory",
        BranchBinding::Explicit => "lix_directory_by_branch",
    }
}

#[derive(Debug, Clone)]
struct DirectoryDescriptorRecord {
    id: String,
    parent_id: Option<String>,
    name: String,
    key: FilesystemDescriptorKey,
    live: MaterializedLiveStateRow,
}

impl DirectoryPathRecord for DirectoryDescriptorRecord {
    type Key = FilesystemDescriptorKey;

    fn parent_key(&self, key: &Self::Key) -> Option<Self::Key> {
        self.parent_id
            .as_deref()
            .map(|parent_id| key.in_same_scope(parent_id))
    }

    fn parent_keys(&self, key: &Self::Key) -> Vec<Self::Key> {
        let Some(parent_id) = self.parent_id.as_deref() else {
            return Vec::new();
        };
        let mut keys = vec![key.in_same_scope(parent_id)];
        if key.is_untracked() {
            keys.push(key.in_tracked_scope(parent_id));
        }
        keys
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Deserialize)]
struct DirectoryDescriptorSnapshot {
    id: String,
    parent_id: Option<String>,
    name: String,
}

#[cfg(test)]
fn lix_directory_write_rows_from_batch(
    batch: &RecordBatch,
    branch_binding: Option<&str>,
) -> Result<Vec<TransactionWriteRow>> {
    lix_directory_write_rows_from_batch_with_options(batch, branch_binding, "lix_directory", true)
}

fn lix_directory_write_rows_from_batch_with_path_resolvers(
    batch: &RecordBatch,
    branch_binding: Option<&str>,
    surface_name: &str,
    path_resolvers: &mut BTreeMap<String, DirectoryPathResolver>,
    generate_directory_id: &mut dyn FnMut() -> String,
) -> Result<Vec<TransactionWriteRow>> {
    lix_directory_write_rows_from_batch_with_options_and_path_resolvers(
        batch,
        branch_binding,
        surface_name,
        true,
        Some(path_resolvers),
        Some(generate_directory_id),
    )
}

fn lix_directory_update_write_rows_from_batch(
    batch: &RecordBatch,
    assignments: &[(String, Arc<dyn PhysicalExpr>)],
    branch_binding: Option<&str>,
    path_resolvers: &mut BTreeMap<String, DirectoryPathResolver>,
    generate_directory_id: &mut dyn FnMut() -> String,
) -> Result<Vec<TransactionWriteRow>> {
    let assignment_values = UpdateAssignmentValues::evaluate(batch, assignments)?;
    let updates_path = assignments
        .iter()
        .any(|(column_name, _)| column_name == "path");
    let mut rows = Vec::new();
    for row_index in 0..batch.num_rows() {
        let id = optional_string_value(batch, row_index, "id")?;
        let context = directory_row_context_from_update(
            batch,
            &assignment_values,
            row_index,
            branch_binding,
        )?;
        if updates_path {
            let directory_id = id.ok_or_else(|| {
                DataFusionError::Execution(
                    "UPDATE lix_directory path requires existing directory id".to_string(),
                )
            })?;
            let path = update_required_string_value(batch, &assignment_values, row_index, "path")?;
            let parsed = crate::common::LixPath::try_from_directory_path(&path)
                .map_err(lix_error_to_datafusion_error)?;
            rows.extend(
                plan_parsed_directory_path_update_with_resolvers(
                    path_resolvers,
                    parsed,
                    directory_id,
                    context,
                    generate_directory_id,
                )
                .map_err(lix_error_to_datafusion_error)?,
            );
            continue;
        }
        let parent_id =
            update_optional_string_value(batch, &assignment_values, row_index, "parent_id")?;
        let name = update_required_string_value(batch, &assignment_values, row_index, "name")?;
        if let Some(directory_id) = id.as_ref() {
            let resolver = path_resolvers
                .entry(directory_path_resolver_key(&context))
                .or_default();
            resolver
                .update_directory(parent_id.clone(), name.clone(), directory_id.clone())
                .map_err(lix_error_to_datafusion_error)?;
        }
        rows.push(directory_descriptor_write_row(
            DirectoryDescriptorWriteIntent {
                id,
                parent_id,
                name,
                context,
            },
        ));
    }
    Ok(rows)
}

fn directory_branch_ids_from_batch(
    batch: &RecordBatch,
    branch_binding: Option<&str>,
) -> Result<BTreeSet<String>> {
    let mut branch_ids = BTreeSet::new();
    for row_index in 0..batch.num_rows() {
        branch_ids
            .insert(directory_row_context_from_batch(batch, row_index, branch_binding)?.branch_id);
    }
    Ok(branch_ids)
}

fn lix_directory_recursive_delete_rows_from_batch(
    batch: &RecordBatch,
    branch_binding: Option<&str>,
    visible_filesystems: &BTreeMap<String, VisibleFilesystem>,
) -> Result<(Vec<TransactionWriteRow>, u64)> {
    let mut rows = Vec::new();
    let mut seen = BTreeSet::new();
    let mut count = 0u64;
    for row_index in 0..batch.num_rows() {
        let directory_id = required_string_value(batch, row_index, "id")?;
        let context = directory_row_context_from_batch(batch, row_index, branch_binding)?;
        let visible_filesystem = visible_filesystems.get(&context.branch_id).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "DELETE FROM lix_directory missing visible filesystem for branch '{}'",
                context.branch_id
            ))
        })?;
        append_deduped_delete_plan(
            &mut rows,
            &mut seen,
            plan_recursive_directory_delete(&directory_id, visible_filesystem, context),
            &mut count,
        );
    }
    Ok((rows, count))
}

fn reject_lix_directory_delete_plugin_storage_paths(
    matched_batch: &RecordBatch,
    all_directories_batch: &RecordBatch,
) -> std::result::Result<(), LixError> {
    let mut all_directory_paths = Vec::new();
    for row_index in 0..all_directories_batch.num_rows() {
        if let Some(path) = optional_string_value(all_directories_batch, row_index, "path")
            .map_err(|error| LixError::unknown(error.to_string()))?
        {
            all_directory_paths.push(path);
        }
    }

    for row_index in 0..matched_batch.num_rows() {
        let Some(path) = optional_string_value(matched_batch, row_index, "path")
            .map_err(|error| LixError::unknown(error.to_string()))?
        else {
            continue;
        };
        reject_normal_plugin_storage_mutation(&path, "DELETE FROM lix_directory")?;
        if all_directory_paths.iter().any(|candidate| {
            path_is_inside_directory(candidate, &path) && is_plugin_storage_path(candidate)
        }) {
            reject_normal_plugin_storage_mutation(
                "/.lix/plugins/",
                "DELETE FROM lix_directory recursive directory delete",
            )?;
        }
    }
    Ok(())
}

fn path_is_inside_directory(path: &str, directory_path: &str) -> bool {
    directory_path == "/" || path.starts_with(directory_path)
}

fn append_deduped_delete_plan(
    rows: &mut Vec<TransactionWriteRow>,
    seen: &mut BTreeSet<StateRowDedupeKey>,
    plan: FilesystemDeletePlan,
    count: &mut u64,
) {
    for row in plan.rows {
        if seen.insert(StateRowDedupeKey::from(&row)) {
            if is_user_visible_filesystem_delete_row(&row) {
                *count += 1;
            }
            rows.push(row);
        }
    }
}

fn is_user_visible_filesystem_delete_row(row: &TransactionWriteRow) -> bool {
    matches!(
        row.schema_key.as_str(),
        "lix_directory_descriptor" | "lix_file_descriptor"
    )
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct StateRowDedupeKey {
    entity_pk: String,
    schema_key: String,
    file_id: Option<String>,
    branch_id: String,
    global: bool,
    untracked: bool,
}

impl From<&TransactionWriteRow> for StateRowDedupeKey {
    fn from(row: &TransactionWriteRow) -> Self {
        Self {
            entity_pk: row
                .entity_pk
                .as_ref()
                .expect("directory provider staged row should carry entity_pk")
                .as_single_string_owned()
                .expect("directory provider staged row entity primary key should project"),
            schema_key: row.schema_key.clone(),
            file_id: row.file_id.clone(),
            branch_id: row.branch_id.clone(),
            global: row.global,
            untracked: row.untracked,
        }
    }
}

#[cfg(test)]
fn lix_directory_write_rows_from_batch_with_options(
    batch: &RecordBatch,
    branch_binding: Option<&str>,
    surface_name: &str,
    reject_read_only_fields: bool,
) -> Result<Vec<TransactionWriteRow>> {
    lix_directory_write_rows_from_batch_with_options_and_path_resolvers(
        batch,
        branch_binding,
        surface_name,
        reject_read_only_fields,
        None,
        None,
    )
}

fn lix_directory_write_rows_from_batch_with_options_and_path_resolvers(
    batch: &RecordBatch,
    branch_binding: Option<&str>,
    surface_name: &str,
    reject_read_only_fields: bool,
    mut path_resolvers: Option<&mut BTreeMap<String, DirectoryPathResolver>>,
    mut generate_directory_id: Option<&mut dyn FnMut() -> String>,
) -> Result<Vec<TransactionWriteRow>> {
    let mut rows = Vec::new();
    for row_index in 0..batch.num_rows() {
        if reject_read_only_fields {
            reject_read_only_lix_directory_insert_field(batch, row_index, "lixcol_entity_pk")?;
            reject_read_only_lix_directory_insert_field(batch, row_index, "lixcol_schema_key")?;
            reject_read_only_lix_directory_insert_field(batch, row_index, "lixcol_change_id")?;
            reject_read_only_lix_directory_insert_field(batch, row_index, "lixcol_created_at")?;
            reject_read_only_lix_directory_insert_field(batch, row_index, "lixcol_updated_at")?;
            reject_read_only_lix_directory_insert_field(batch, row_index, "lixcol_commit_id")?;
        }

        let path = optional_string_value(batch, row_index, "path")?;
        let id = optional_string_value(batch, row_index, "id")?;
        let context = directory_row_context_from_batch(batch, row_index, branch_binding)?;

        if let Some(path) = path.filter(|_| reject_read_only_fields) {
            reject_read_only_lix_directory_insert_field(batch, row_index, "parent_id")?;
            reject_read_only_lix_directory_insert_field(batch, row_index, "name")?;

            let Some(path_resolvers) = path_resolvers.as_deref_mut() else {
                return Err(DataFusionError::Execution(
                    "INSERT into lix_directory with path requires directory path resolver"
                        .to_string(),
                ));
            };
            let Some(generate_directory_id) = generate_directory_id.as_deref_mut() else {
                return Err(DataFusionError::Execution(
                    "INSERT into lix_directory with path requires directory id generator"
                        .to_string(),
                ));
            };
            let explicit_directory_id = id.clone();
            let parsed = crate::common::LixPath::try_from_directory_path(&path)
                .map_err(lix_error_to_datafusion_error)?;
            let plan = create_directory_path_with_leaf_id_with_resolvers(
                path_resolvers,
                parsed,
                explicit_directory_id,
                context,
                generate_directory_id,
            )
            .map_err(|error| map_lix_directory_insert_error(error, surface_name, id.as_deref()))
            .map_err(lix_error_to_datafusion_error)?;
            let directory_id = plan.directory_id;
            let mut planned_rows = plan.rows;
            attach_lix_directory_insert_origin(&mut planned_rows, surface_name, &directory_id);
            rows.extend(planned_rows);
            continue;
        }

        let parent_id = optional_string_value(batch, row_index, "parent_id")?;
        let name = required_string_value(batch, row_index, "name")?;
        if let Some(path_resolvers) = path_resolvers.as_deref_mut() {
            if let Some(directory_id) = id.as_ref() {
                let resolver = path_resolvers
                    .entry(directory_path_resolver_key(&context))
                    .or_insert_with(DirectoryPathResolver::default);
                resolver
                    .reserve_directory(parent_id.clone(), name.clone(), directory_id.clone())
                    .map_err(|error| {
                        map_lix_directory_insert_error(error, surface_name, Some(directory_id))
                    })
                    .map_err(lix_error_to_datafusion_error)?;
            }
        }
        let mut row = directory_descriptor_write_row(DirectoryDescriptorWriteIntent {
            id: id.clone(),
            parent_id,
            name,
            context,
        });
        if let Some(directory_id) = id.as_ref() {
            row.origin = Some(lix_directory_insert_origin(surface_name, directory_id));
        }
        rows.push(row);
    }
    Ok(rows)
}

fn map_lix_directory_insert_error(
    error: LixError,
    surface_name: &str,
    directory_id: Option<&str>,
) -> LixError {
    let Some(directory_id) = directory_id else {
        return error;
    };
    let directory_id_conflict =
        format!("unique constraint violation on lix_directory.id for value {directory_id:?}");
    if error.code == LixError::CODE_UNIQUE && error.message == directory_id_conflict {
        return LixError::new(
            LixError::CODE_UNIQUE,
            format!(
                "primary-key constraint violation on table '{surface_name}': INSERT would duplicate id '{directory_id}'"
            ),
        );
    }
    error
}

fn attach_lix_directory_insert_origin(
    rows: &mut [TransactionWriteRow],
    surface_name: &str,
    directory_id: &str,
) {
    let origin = lix_directory_insert_origin(surface_name, directory_id);
    for row in rows {
        if row.schema_key != DIRECTORY_SCHEMA_KEY {
            continue;
        }
        let Some(entity_pk) = row
            .entity_pk
            .as_ref()
            .and_then(|entity_pk| entity_pk.as_single_string_owned().ok())
        else {
            continue;
        };
        if entity_pk == directory_id {
            row.origin = Some(origin.clone());
        }
    }
}

fn lix_directory_insert_origin(surface_name: &str, directory_id: &str) -> TransactionWriteOrigin {
    TransactionWriteOrigin {
        surface: surface_name.to_string(),
        operation: TransactionWriteOperation::Insert,
        primary_key: Some(LogicalPrimaryKey {
            columns: vec!["id".to_string()],
            values: vec![directory_id.to_string()],
        }),
    }
}

fn directory_row_context_from_batch(
    batch: &RecordBatch,
    row_index: usize,
    branch_binding: Option<&str>,
) -> Result<FilesystemRowContext> {
    let scope = resolve_write_branch_scope(
        optional_bool_value(batch, row_index, "lixcol_global")?,
        optional_string_value(batch, row_index, "lixcol_branch_id")?,
        branch_binding,
        "INSERT into lix_directory_by_branch",
        "lix_directory",
    )?;

    Ok(FilesystemRowContext {
        branch_id: scope.branch_id,
        global: scope.global,
        untracked: optional_bool_value(batch, row_index, "lixcol_untracked")?.unwrap_or(false),
        file_id: optional_string_value(batch, row_index, "lixcol_file_id")?,
        metadata: optional_metadata_value(batch, row_index, "lixcol_metadata", "lix_directory")?,
    })
}

fn directory_row_context_from_update(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    row_index: usize,
    branch_binding: Option<&str>,
) -> Result<FilesystemRowContext> {
    let explicit_global = optional_bool_value(batch, row_index, "lixcol_global")?;
    let explicit_branch_id = if explicit_global == Some(true) {
        Some(GLOBAL_BRANCH_ID.to_string())
    } else {
        optional_string_value(batch, row_index, "lixcol_branch_id")?
    };
    let scope = resolve_write_branch_scope(
        explicit_global,
        explicit_branch_id,
        branch_binding,
        "UPDATE into lix_directory_by_branch",
        "lix_directory",
    )?;

    Ok(FilesystemRowContext {
        branch_id: scope.branch_id,
        global: scope.global,
        untracked: optional_bool_value(batch, row_index, "lixcol_untracked")?.unwrap_or(false),
        file_id: optional_string_value(batch, row_index, "lixcol_file_id")?,
        metadata: update_optional_metadata_value(
            batch,
            assignment_values,
            row_index,
            "lixcol_metadata",
            "lix_directory",
        )?,
    })
}

fn directory_path_resolver_key(context: &FilesystemRowContext) -> String {
    filesystem_storage_scope_key(
        &context.branch_id,
        context.global,
        context.untracked,
        context.file_id.as_deref(),
    )
}

fn lix_directory_record_batch(
    schema: &SchemaRef,
    rows: Vec<MaterializedLiveStateRow>,
) -> Result<RecordBatch, LixError> {
    let mut directory_rows = Vec::<DirectoryDescriptorRecord>::new();

    for row in rows {
        if row.schema_key != DIRECTORY_SCHEMA_KEY {
            continue;
        }
        let Some(snapshot_content) = row.snapshot_content.as_deref() else {
            continue;
        };
        let snapshot: DirectoryDescriptorSnapshot = serde_json::from_str(snapshot_content)
            .map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("invalid lix_directory_descriptor snapshot JSON: {error}"),
                )
            })?;
        let key = FilesystemDescriptorKey::from_live_row(&row, snapshot.id.clone());
        directory_rows.push(DirectoryDescriptorRecord {
            id: snapshot.id,
            parent_id: snapshot.parent_id,
            name: snapshot.name,
            key,
            live: row,
        });
    }

    let directory_paths =
        derive_directory_paths(directory_rows.iter().map(|row| (row.key.clone(), row)))?;
    let directory_rows = directory_rows
        .into_iter()
        .map(|row| {
            let path = directory_paths.get(&row.key).cloned();
            (row, path)
        })
        .collect();
    lix_directory_record_batch_from_rendered(schema, directory_rows)
}

fn indexed_lix_directory_record_batch(
    schema: &SchemaRef,
    matches: &FilesystemPathSelection,
) -> Result<RecordBatch, LixError> {
    let rows = matches
        .entries()
        .filter(|entry| entry.kind == FilesystemPathKind::Directory)
        .map(|entry| {
            (
                DirectoryDescriptorRecord {
                    id: entry.id().to_string(),
                    parent_id: entry.parent_id.clone(),
                    name: entry.name.clone(),
                    key: entry.key.clone(),
                    live: entry.live_row(),
                },
                Some(entry.path.clone()),
            )
        })
        .collect();
    lix_directory_record_batch_from_rendered(schema, rows)
}

fn lix_directory_record_batch_from_rendered(
    schema: &SchemaRef,
    directory_rows: Vec<(DirectoryDescriptorRecord, Option<String>)>,
) -> Result<RecordBatch, LixError> {
    let mut ids = Vec::new();
    let mut paths = Vec::new();
    let mut parent_ids = Vec::new();
    let mut names = Vec::new();
    let mut entity_pks = Vec::new();
    let mut schema_keys = Vec::new();
    let mut file_ids = Vec::new();
    let mut globals = Vec::new();
    let mut change_ids = Vec::new();
    let mut created_ats = Vec::new();
    let mut updated_ats = Vec::new();
    let mut commit_ids = Vec::new();
    let mut untracked_values = Vec::new();
    let mut metadata_values = Vec::new();
    let mut branch_ids = Vec::new();

    for (directory, path) in directory_rows {
        ids.push(Some(directory.id.clone()));
        paths.push(path);
        parent_ids.push(directory.parent_id);
        names.push(Some(directory.name));
        entity_pks.push(Some(directory.live.entity_pk.as_json_array_text()?));
        schema_keys.push(Some(directory.live.schema_key));
        file_ids.push(directory.live.file_id);
        globals.push(Some(directory.live.global));
        change_ids.push(directory.live.change_id.map(|id| id.to_string()));
        created_ats.push(directory.live.created_at);
        updated_ats.push(directory.live.updated_at);
        commit_ids.push(directory.live.commit_id.map(|id| id.to_string()));
        untracked_values.push(Some(directory.live.untracked));
        metadata_values.push(
            directory
                .live
                .metadata
                .as_deref()
                .map(serialize_row_metadata),
        );
        branch_ids.push(Some(directory.live.branch_id));
    }

    let mut columns = Vec::<ArrayRef>::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let array: ArrayRef = match field.name().as_str() {
            "id" => Arc::new(StringArray::from(ids.clone())),
            "path" => Arc::new(StringArray::from(paths.clone())),
            "parent_id" => Arc::new(StringArray::from(parent_ids.clone())),
            "name" => Arc::new(StringArray::from(names.clone())),
            "lixcol_entity_pk" => Arc::new(StringArray::from(entity_pks.clone())),
            "lixcol_schema_key" => Arc::new(StringArray::from(schema_keys.clone())),
            "lixcol_file_id" => Arc::new(StringArray::from(file_ids.clone())),
            "lixcol_global" => Arc::new(BooleanArray::from(globals.clone())),
            "lixcol_change_id" => Arc::new(StringArray::from(change_ids.clone())),
            "lixcol_created_at" => Arc::new(StringArray::from(created_ats.clone())),
            "lixcol_updated_at" => Arc::new(StringArray::from(updated_ats.clone())),
            "lixcol_commit_id" => Arc::new(StringArray::from(commit_ids.clone())),
            "lixcol_untracked" => Arc::new(BooleanArray::from(untracked_values.clone())),
            "lixcol_metadata" => Arc::new(StringArray::from(metadata_values.clone())),
            "lixcol_branch_id" => Arc::new(StringArray::from(branch_ids.clone())),
            other => {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "sql2 lix_directory provider does not support projected column '{other}'"
                    ),
                ));
            }
        };
        columns.push(array);
    }

    let options = RecordBatchOptions::new().with_row_count(Some(ids.len()));
    RecordBatch::try_new_with_options(Arc::clone(schema), columns, &options).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("sql2 failed to build lix_directory record batch: {error}"),
        )
    })
}

fn lix_directory_scan_request(
    branch_binding: Option<&str>,
    projected_schema: Option<&Schema>,
    limit: Option<usize>,
) -> LiveStateScanRequest {
    LiveStateScanRequest {
        filter: LiveStateFilter {
            schema_keys: vec![DIRECTORY_SCHEMA_KEY.to_string()],
            branch_ids: branch_binding
                .map(|branch_id| vec![branch_id.to_string()])
                .unwrap_or_default(),
            ..LiveStateFilter::default()
        },
        projection: lix_directory_live_state_projection(projected_schema),
        limit,
    }
}

fn lix_directory_live_state_projection(projected_schema: Option<&Schema>) -> LiveStateProjection {
    let Some(schema) = projected_schema else {
        return LiveStateProjection::default();
    };
    let mut columns = vec!["snapshot_content".to_string()];
    if schema
        .fields()
        .iter()
        .any(|field| field.name() == "lixcol_metadata")
    {
        columns.push("metadata".to_string());
    }
    LiveStateProjection { columns }
}

fn validate_lix_directory_update_assignments(
    schema: &SchemaRef,
    assignments: &[(String, Expr)],
) -> Result<()> {
    let updates_path = assignments
        .iter()
        .any(|(column_name, _)| column_name == "path");
    for (column_name, _) in assignments {
        schema.field_with_name(column_name).map_err(|_| {
            DataFusionError::Plan(format!(
                "UPDATE lix_directory failed: column '{column_name}' does not exist"
            ))
        })?;
        if !matches!(
            column_name.as_str(),
            "path" | "parent_id" | "name" | "lixcol_metadata"
        ) {
            return Err(DataFusionError::Execution(format!(
                "UPDATE lix_directory cannot stage read-only column '{column_name}'"
            )));
        }
        if updates_path && matches!(column_name.as_str(), "parent_id" | "name") {
            return Err(DataFusionError::Execution(
                "UPDATE lix_directory cannot mix path with parent_id or name assignments"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

fn record_batch_has_non_null_column(batch: &RecordBatch, column_name: &str) -> Result<bool> {
    for row_index in 0..batch.num_rows() {
        if optional_scalar_value(batch, row_index, column_name)?
            .is_some_and(|value| !value.is_null())
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn reject_read_only_lix_directory_insert_field(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<()> {
    if optional_scalar_value(batch, row_index, column_name)?.is_some_and(|value| !value.is_null()) {
        return Err(DataFusionError::Execution(format!(
            "INSERT into lix_directory cannot stage read-only column '{column_name}'"
        )));
    }
    Ok(())
}

fn required_string_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<String> {
    optional_string_value(batch, row_index, column_name)?.ok_or_else(|| {
        DataFusionError::Execution(format!(
            "INSERT into lix_directory requires non-null text column '{column_name}'"
        ))
    })
}

fn update_required_string_value(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    row_index: usize,
    column_name: &str,
) -> Result<String> {
    update_optional_string_value(batch, assignment_values, row_index, column_name)?.ok_or_else(
        || {
            DataFusionError::Execution(format!(
                "UPDATE lix_directory requires non-null text column '{column_name}'"
            ))
        },
    )
}

fn update_optional_string_value(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    row_index: usize,
    column_name: &str,
) -> Result<Option<String>> {
    match assignment_values.assigned_or_existing_cell(batch, row_index, column_name)? {
        InsertCell::Omitted | InsertCell::Provided(SqlCell::Null) => Ok(None),
        InsertCell::Provided(SqlCell::Value(
            ScalarValue::Utf8(Some(value))
            | ScalarValue::Utf8View(Some(value))
            | ScalarValue::LargeUtf8(Some(value)),
        )) => Ok(Some(value)),
        InsertCell::Provided(SqlCell::Value(other)) => Err(DataFusionError::Execution(format!(
            "UPDATE lix_directory expected text-compatible column '{column_name}', got {other:?}"
        ))),
    }
}

fn update_optional_metadata_value(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    row_index: usize,
    column_name: &str,
    context: &str,
) -> Result<Option<TransactionJson>> {
    update_optional_string_value(batch, assignment_values, row_index, column_name)?
        .map(|value| {
            let metadata = parse_row_metadata_value(&value, context)
                .map_err(crate::sql2::error::lix_error_to_datafusion_error)?;
            TransactionJson::from_value(metadata, &format!("{context} metadata"))
                .map_err(crate::sql2::error::lix_error_to_datafusion_error)
        })
        .transpose()
}

fn optional_string_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<Option<String>> {
    match optional_scalar_value(batch, row_index, column_name)? {
        None
        | Some(
            ScalarValue::Null
            | ScalarValue::Utf8(None)
            | ScalarValue::Utf8View(None)
            | ScalarValue::LargeUtf8(None),
        ) => Ok(None),
        Some(
            ScalarValue::Utf8(Some(value))
            | ScalarValue::Utf8View(Some(value))
            | ScalarValue::LargeUtf8(Some(value)),
        ) => Ok(Some(value)),
        Some(other) => Err(DataFusionError::Execution(format!(
            "INSERT into lix_directory expected text-compatible column '{column_name}', got {other:?}"
        ))),
    }
}

fn optional_metadata_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
    context: &str,
) -> Result<Option<TransactionJson>> {
    optional_string_value(batch, row_index, column_name)?
        .map(|value| {
            let metadata = parse_row_metadata_value(&value, context)
                .map_err(crate::sql2::error::lix_error_to_datafusion_error)?;
            TransactionJson::from_value(metadata, &format!("{context} metadata"))
                .map_err(crate::sql2::error::lix_error_to_datafusion_error)
        })
        .transpose()
}

fn optional_bool_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<Option<bool>> {
    match optional_scalar_value(batch, row_index, column_name)? {
        None | Some(ScalarValue::Null | ScalarValue::Boolean(None)) => Ok(None),
        Some(ScalarValue::Boolean(Some(value))) => Ok(Some(value)),
        Some(other) => Err(DataFusionError::Execution(format!(
            "INSERT into lix_directory expected boolean column '{column_name}', got {other:?}"
        ))),
    }
}

fn optional_scalar_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<Option<ScalarValue>> {
    let schema = batch.schema();
    let column_index = match schema.index_of(column_name) {
        Ok(column_index) => column_index,
        Err(_) => return Ok(None),
    };
    if row_index >= batch.num_rows() {
        return Err(DataFusionError::Execution(format!(
            "row index {row_index} out of bounds for lix_directory batch with {} rows",
            batch.num_rows()
        )));
    }
    ScalarValue::try_from_array(batch.column(column_index).as_ref(), row_index)
        .map(Some)
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "failed to decode lix_directory column '{column_name}' at row {row_index}: {error}"
            ))
        })
}

pub(super) fn lix_directory_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("path", DataType::Utf8, true),
        Field::new("parent_id", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, false),
        json_field("lixcol_entity_pk", false),
        Field::new("lixcol_schema_key", DataType::Utf8, false),
        Field::new("lixcol_file_id", DataType::Utf8, true),
        Field::new("lixcol_global", DataType::Boolean, true),
        Field::new("lixcol_change_id", DataType::Utf8, true),
        Field::new("lixcol_created_at", DataType::Utf8, true),
        Field::new("lixcol_updated_at", DataType::Utf8, true),
        Field::new("lixcol_commit_id", DataType::Utf8, true),
        Field::new("lixcol_untracked", DataType::Boolean, true),
        json_field("lixcol_metadata", true),
    ]))
}

pub(super) fn lix_directory_by_branch_schema() -> SchemaRef {
    let mut fields = lix_directory_schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(Field::new("lixcol_branch_id", DataType::Utf8, false));
    Arc::new(Schema::new(fields))
}

fn lix_error_to_datafusion_error(error: LixError) -> DataFusionError {
    crate::sql2::error::lix_error_to_datafusion_error(error)
}

#[cfg(test)]
#[expect(trivial_casts)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    use async_trait::async_trait;
    use datafusion::arrow::array::{ArrayRef, BooleanArray, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use serde_json::json;

    use crate::LixError;
    use crate::binary_cas::BlobDataReader;
    use crate::changelog::{ChangeId, CommitId};
    use crate::functions::FunctionProviderHandle;
    use crate::live_state::{LiveStateScanRequest, MaterializedLiveStateRow};
    use crate::sql2::{SqlWriteContext, SqlWriteExecutionContext};
    use crate::transaction::types::{
        TransactionJson, TransactionWrite, TransactionWriteMode, TransactionWriteOutcome,
        TransactionWriteRow,
    };

    use super::super::spec::{SpecTableProvider, TableSpec};
    use super::{
        BranchBinding, DirectoryDescriptorRecord, LixDirectorySpec, WriteAccess,
        derive_directory_paths, lix_directory_by_branch_schema, lix_directory_insert_origin,
        lix_directory_record_batch, lix_directory_recursive_delete_rows_from_batch,
        lix_directory_write_rows_from_batch,
        lix_directory_write_rows_from_batch_with_path_resolvers,
    };
    use crate::filesystem::{
        FilesystemDescriptorKey, VisibleFilesystem, directory_path_resolvers_from_state_rows,
    };

    fn test_id_generator(ids: &'static [&'static str]) -> impl FnMut() -> String {
        let mut ids = ids.iter();
        move || ids.next().expect("test id should exist").to_string()
    }

    fn test_functions() -> FunctionProviderHandle {
        FunctionProviderHandle::system()
    }

    /// Stage a single INSERT batch through the directory spec, exercising the
    /// same `stage_insert` path the writable provider uses.
    async fn stage_directory_insert(
        write_ctx: SqlWriteContext,
        branch_binding: BranchBinding,
        batch: RecordBatch,
    ) -> Result<u64, datafusion::common::DataFusionError> {
        let live_state = Arc::new(crate::sql2::WriteContextLiveStateReader::new(
            write_ctx.clone(),
        ));
        let branch_ref = Arc::new(crate::sql2::WriteContextBranchRefReader::new(
            write_ctx.clone(),
        ));
        let filesystem_path_index: Arc<dyn crate::filesystem::FilesystemPathIndexReader> =
            live_state.clone();
        let spec = match branch_binding {
            BranchBinding::Active { .. } => LixDirectorySpec::active_branch(
                write_ctx.active_branch_id(),
                live_state,
                filesystem_path_index,
                branch_ref,
                test_functions(),
            ),
            BranchBinding::Explicit => LixDirectorySpec::by_branch(
                live_state,
                filesystem_path_index,
                branch_ref,
                test_functions(),
            ),
        };
        spec.stage_insert(&write_ctx, vec![batch]).await
    }

    #[derive(Default)]
    struct CapturingWriteContext {
        rows: Vec<MaterializedLiveStateRow>,
        writes: Vec<TransactionWrite>,
    }

    #[async_trait]
    impl BlobDataReader for CapturingWriteContext {
        async fn load_bytes_many(
            &self,
            hashes: &[crate::binary_cas::BlobHash],
        ) -> Result<crate::binary_cas::BlobBytesBatch, LixError> {
            Ok(crate::binary_cas::BlobBytesBatch::new(vec![
                None;
                hashes.len()
            ]))
        }
    }

    #[async_trait]
    impl SqlWriteExecutionContext for CapturingWriteContext {
        fn active_branch_id(&self) -> &str {
            "branch-a"
        }

        fn functions(&self) -> FunctionProviderHandle {
            test_functions()
        }

        fn list_visible_schemas(&self) -> Result<Vec<serde_json::Value>, LixError> {
            Ok(Vec::new())
        }

        async fn load_bytes_many(
            &mut self,
            hashes: &[crate::binary_cas::BlobHash],
        ) -> Result<crate::binary_cas::BlobBytesBatch, LixError> {
            BlobDataReader::load_bytes_many(self, hashes).await
        }

        async fn scan_live_state(
            &mut self,
            _request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            Ok(self.rows.clone())
        }

        async fn load_branch_head(
            &mut self,
            branch_id: &str,
        ) -> Result<Option<CommitId>, LixError> {
            if branch_id == "ghost-branch" {
                return Ok(None);
            }
            Ok(Some(CommitId::for_test_label(&format!(
                "commit-{branch_id}"
            ))))
        }

        async fn stage_write(
            &mut self,
            write: TransactionWrite,
        ) -> Result<TransactionWriteOutcome, LixError> {
            self.writes.push(write);
            Ok(TransactionWriteOutcome { count: 0 })
        }
    }

    fn live_row(
        entity_pk: &str,
        branch_id: &str,
        snapshot_content: &str,
    ) -> MaterializedLiveStateRow {
        live_filesystem_row(
            entity_pk,
            super::DIRECTORY_SCHEMA_KEY,
            None,
            branch_id,
            snapshot_content,
        )
    }

    fn live_filesystem_row(
        entity_pk: &str,
        schema_key: &str,
        file_id: Option<&str>,
        branch_id: &str,
        snapshot_content: &str,
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_pk: crate::entity_pk::EntityPk::single(entity_pk),
            schema_key: schema_key.to_string(),
            file_id: file_id.map(ToOwned::to_owned),
            snapshot_content: Some(snapshot_content.to_string()),
            metadata: Some(json!({"source": "test"}).to_string()),
            deleted: false,
            branch_id: branch_id.to_string(),
            change_id: Some(ChangeId::for_test_label(&format!("change-{entity_pk}"))),
            commit_id: Some(CommitId::for_test_label(&format!("commit-{entity_pk}"))),
            global: false,
            untracked: false,
            created_at: "2026-04-23T00:00:00Z".to_string(),
            updated_at: "2026-04-23T01:00:00Z".to_string(),
        }
    }

    fn filesystem_rows() -> Vec<MaterializedLiveStateRow> {
        vec![
            live_filesystem_row(
                "dir-docs",
                "lix_directory_descriptor",
                None,
                "branch-a",
                r#"{"id":"dir-docs","parent_id":null,"name":"docs"}"#,
            ),
            live_filesystem_row(
                "dir-guides",
                "lix_directory_descriptor",
                None,
                "branch-a",
                r#"{"id":"dir-guides","parent_id":"dir-docs","name":"guides"}"#,
            ),
            live_filesystem_row(
                "file-index",
                "lix_file_descriptor",
                None,
                "branch-a",
                r#"{"id":"file-index","directory_id":"dir-docs","name":"index.md"}"#,
            ),
            live_filesystem_row(
                "file-readme",
                "lix_file_descriptor",
                None,
                "branch-a",
                r#"{"id":"file-readme","directory_id":"dir-guides","name":"readme.md"}"#,
            ),
            live_filesystem_row(
                "file-readme",
                "lix_binary_blob_ref",
                Some("file-readme"),
                "branch-a",
                r#"{"id":"file-readme","blob_hash":"abc123","size_bytes":5}"#,
            ),
        ]
    }

    fn string_column(values: Vec<Option<&str>>) -> ArrayRef {
        Arc::new(StringArray::from(values)) as ArrayRef
    }

    fn directory_insert_batch(include_branch: bool, global: bool) -> RecordBatch {
        let mut fields = vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("parent_id", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, false),
            Field::new("lixcol_global", DataType::Boolean, false),
            Field::new("lixcol_metadata", DataType::Utf8, true),
        ];
        let mut columns = vec![
            string_column(vec![Some("dir-docs")]),
            string_column(vec![None]),
            string_column(vec![Some("docs")]),
            Arc::new(BooleanArray::from(vec![global])) as ArrayRef,
            string_column(vec![Some("{\"source\":\"directory\"}")]),
        ];
        if include_branch {
            fields.push(Field::new("lixcol_branch_id", DataType::Utf8, false));
            columns.push(string_column(vec![Some("branch-a")]));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("directory insert batch should build")
    }

    fn directory_path_insert_batch(path: &str) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("path", DataType::Utf8, true),
                Field::new("lixcol_branch_id", DataType::Utf8, false),
            ])),
            vec![
                string_column(vec![Some("dir-nested")]),
                string_column(vec![Some(path)]),
                string_column(vec![Some("branch-a")]),
            ],
        )
        .expect("directory path insert batch should build")
    }

    fn directory_delete_batch(ids: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("lixcol_branch_id", DataType::Utf8, false),
            ])),
            vec![
                string_column(ids.iter().copied().map(Some).collect::<Vec<_>>()),
                string_column(vec![Some("branch-a"); ids.len()]),
            ],
        )
        .expect("directory delete batch should build")
    }

    #[test]
    fn derives_nested_directory_paths() {
        let root_live = live_row(
            "dir-docs",
            "branch-a",
            "{\"id\":\"dir-docs\",\"parent_id\":null,\"name\":\"docs\"}",
        );
        let child_live = live_row(
            "dir-guides",
            "branch-a",
            "{\"id\":\"dir-guides\",\"parent_id\":\"dir-docs\",\"name\":\"guides\"}",
        );
        let root = DirectoryDescriptorRecord {
            id: "dir-docs".to_string(),
            parent_id: None,
            name: "docs".to_string(),
            key: FilesystemDescriptorKey::from_live_row(&root_live, "dir-docs"),
            live: root_live,
        };
        let child = DirectoryDescriptorRecord {
            id: "dir-guides".to_string(),
            parent_id: Some("dir-docs".to_string()),
            name: "guides".to_string(),
            key: FilesystemDescriptorKey::from_live_row(&child_live, "dir-guides"),
            live: child_live,
        };
        let child_key = child.key.clone();
        let records = [root, child];
        let paths = derive_directory_paths(records.iter().map(|row| (row.key.clone(), row)))
            .expect("path derivation should succeed");

        assert_eq!(paths.get(&child_key), Some(&"/docs/guides/".to_string()));
    }

    #[test]
    fn record_batch_projects_directory_columns() {
        let rows = vec![
            live_row(
                "dir-docs",
                "branch-a",
                "{\"id\":\"dir-docs\",\"parent_id\":null,\"name\":\"docs\"}",
            ),
            live_row(
                "dir-guides",
                "branch-a",
                "{\"id\":\"dir-guides\",\"parent_id\":\"dir-docs\",\"name\":\"guides\"}",
            ),
        ];

        let batch = lix_directory_record_batch(&lix_directory_by_branch_schema(), rows)
            .expect("directory batch should build");

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(
            batch
                .column_by_name("path")
                .expect("path column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("path is string")
                .value(1),
            "/docs/guides/"
        );
        assert_eq!(
            batch
                .column_by_name("lixcol_branch_id")
                .expect("branch column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("branch is string")
                .value(1),
            "branch-a"
        );
    }

    #[test]
    fn decodes_directory_insert_into_lix_state_write_row() {
        let rows = lix_directory_write_rows_from_batch(&directory_insert_batch(true, false), None)
            .expect("directory batch should decode");

        assert_eq!(
            rows,
            vec![TransactionWriteRow {
                entity_pk: Some(crate::entity_pk::EntityPk::single("dir-docs")),
                schema_key: super::DIRECTORY_SCHEMA_KEY.to_string(),
                file_id: None,
                snapshot: Some(TransactionJson::from_value_for_test(
                    json!({"id":"dir-docs","name":"docs","parent_id":null})
                )),
                metadata: Some(TransactionJson::from_value_for_test(
                    json!({"source": "directory"})
                )),
                origin: Some(lix_directory_insert_origin("lix_directory", "dir-docs")),
                created_at: None,
                updated_at: None,
                global: false,
                change_id: None,
                commit_id: None,
                untracked: false,
                branch_id: "branch-a".to_string(),
            }]
        );
    }

    #[test]
    fn active_directory_insert_defaults_branch_id() {
        let rows = lix_directory_write_rows_from_batch(
            &directory_insert_batch(false, false),
            Some("branch-active"),
        )
        .expect("active directory batch should decode");

        assert_eq!(rows[0].branch_id, "branch-active");
    }

    #[test]
    fn by_branch_directory_insert_requires_branch_id_for_non_global_rows() {
        let error =
            lix_directory_write_rows_from_batch(&directory_insert_batch(false, false), None)
                .expect_err("by-branch insert should require branch id");

        assert!(
            error.to_string().contains("requires lixcol_branch_id"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn directory_insert_rejects_global_with_non_global_branch_id() {
        let error = lix_directory_write_rows_from_batch(&directory_insert_batch(true, true), None)
            .expect_err("global directory write should reject conflicting branch id");

        assert!(
            error
                .to_string()
                .contains("cannot set lixcol_global=true with non-global lixcol_branch_id"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn directory_path_insert_reuses_existing_parent_descriptor() {
        let existing_rows = vec![live_row(
            "dir-docs",
            "branch-a",
            "{\"id\":\"dir-docs\",\"parent_id\":null,\"name\":\"docs\"}",
        )];
        let mut resolvers = directory_path_resolvers_from_state_rows(existing_rows)
            .expect("existing directory rows should seed paths");

        let rows = lix_directory_write_rows_from_batch_with_path_resolvers(
            &directory_path_insert_batch("/docs/nested/"),
            None,
            "lix_directory",
            &mut resolvers,
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect("directory path batch should decode");

        assert_eq!(rows.len(), 1);
        let snapshot = rows[0].snapshot.as_ref().unwrap();
        assert_eq!(snapshot["id"], "dir-nested");
        assert_eq!(snapshot["parent_id"], "dir-docs");
        assert_eq!(snapshot["name"], "nested");
    }

    #[test]
    fn recursive_directory_delete_deletes_nested_dirs_files_and_blob_refs() {
        let visible_filesystem = VisibleFilesystem::from_live_rows(filesystem_rows())
            .expect("visible filesystem should build");
        let mut visible_filesystems = BTreeMap::new();
        visible_filesystems.insert("branch-a".to_string(), visible_filesystem);

        let (rows, count) = lix_directory_recursive_delete_rows_from_batch(
            &directory_delete_batch(&["dir-docs"]),
            None,
            &visible_filesystems,
        )
        .expect("recursive directory delete should plan");

        assert_eq!(count, 4);
        assert_eq!(
            rows.iter()
                .map(|row| {
                    (
                        row.schema_key.as_str(),
                        row.entity_pk
                            .as_ref()
                            .expect("planned delete row should carry entity_pk")
                            .as_single_string_owned()
                            .expect("planned delete row should project entity_pk"),
                    )
                })
                .collect::<Vec<_>>(),
            vec![
                ("lix_file_descriptor", "file-readme".to_string()),
                ("lix_binary_blob_ref", "file-readme".to_string()),
                ("lix_directory_descriptor", "dir-guides".to_string()),
                ("lix_file_descriptor", "file-index".to_string()),
                ("lix_directory_descriptor", "dir-docs".to_string()),
            ]
        );
        assert!(rows.iter().all(|row| row.snapshot.is_none()));
    }

    #[test]
    fn recursive_directory_delete_dedupes_overlapping_parent_and_child() {
        let visible_filesystem = VisibleFilesystem::from_live_rows(filesystem_rows())
            .expect("visible filesystem should build");
        let mut visible_filesystems = BTreeMap::new();
        visible_filesystems.insert("branch-a".to_string(), visible_filesystem);

        let (rows, count) = lix_directory_recursive_delete_rows_from_batch(
            &directory_delete_batch(&["dir-docs", "dir-guides"]),
            None,
            &visible_filesystems,
        )
        .expect("recursive directory delete should plan");

        assert_eq!(count, 4);
        let identities = rows
            .iter()
            .map(|row| {
                (
                    row.schema_key.clone(),
                    row.entity_pk.clone(),
                    row.file_id.clone(),
                    row.branch_id.clone(),
                )
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(identities.len(), rows.len());
        assert_eq!(rows.len(), 5);
    }

    #[tokio::test]
    async fn directory_insert_sink_stages_decoded_lix_state_rows() {
        let mut write_context = CapturingWriteContext::default();
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let batch = directory_insert_batch(true, false);
        let count = stage_directory_insert(write_ctx, BranchBinding::explicit(), batch)
            .await
            .expect("directory spec should stage write");

        assert_eq!(count, 1);
        assert_eq!(
            write_context.writes.as_slice(),
            &[TransactionWrite::Rows {
                mode: TransactionWriteMode::Insert,
                rows: vec![TransactionWriteRow {
                    entity_pk: Some(crate::entity_pk::EntityPk::single("dir-docs")),
                    schema_key: super::DIRECTORY_SCHEMA_KEY.to_string(),
                    file_id: None,
                    snapshot: Some(TransactionJson::from_value_for_test(
                        json!({"id":"dir-docs","name":"docs","parent_id":null})
                    )),
                    metadata: Some(TransactionJson::from_value_for_test(
                        json!({"source": "directory"})
                    )),
                    origin: Some(lix_directory_insert_origin(
                        "lix_directory_by_branch",
                        "dir-docs"
                    )),
                    created_at: None,
                    updated_at: None,
                    global: false,
                    change_id: None,
                    commit_id: None,
                    untracked: false,
                    branch_id: "branch-a".to_string(),
                }]
            }]
        );
    }

    #[tokio::test]
    async fn directory_insert_sink_seeds_path_resolver_from_live_state() {
        let mut write_context = CapturingWriteContext {
            rows: vec![live_row(
                "dir-docs",
                "branch-a",
                "{\"id\":\"dir-docs\",\"parent_id\":null,\"name\":\"docs\"}",
            )],
            writes: Vec::new(),
        };
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let batch = directory_path_insert_batch("/docs/nested/");
        let count = stage_directory_insert(write_ctx, BranchBinding::explicit(), batch)
            .await
            .expect("directory spec should stage path write");

        assert_eq!(count, 1);
        let [TransactionWrite::Rows { rows, .. }] = write_context.writes.as_slice() else {
            panic!("expected one directory staged write");
        };
        assert_eq!(rows.len(), 1);
        let snapshot = rows[0].snapshot.as_ref().unwrap();
        assert_eq!(snapshot["id"], "dir-nested");
        assert_eq!(snapshot["parent_id"], "dir-docs");
        assert_eq!(snapshot["name"], "nested");
    }

    #[test]
    fn directory_provider_is_writable_when_given_write_access() {
        let mut write_context = CapturingWriteContext::default();
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let live_state = Arc::new(crate::sql2::WriteContextLiveStateReader::new(
            write_ctx.clone(),
        ));
        let branch_ref = Arc::new(crate::sql2::WriteContextBranchRefReader::new(
            write_ctx.clone(),
        ));
        let filesystem_path_index: Arc<dyn crate::filesystem::FilesystemPathIndexReader> =
            live_state.clone();
        let provider = SpecTableProvider::new(
            Arc::new(LixDirectorySpec::active_branch(
                write_ctx.active_branch_id(),
                live_state,
                filesystem_path_index,
                branch_ref,
                test_functions(),
            )),
            WriteAccess::write(write_ctx),
        );
        assert!(provider.is_write());
    }
}
