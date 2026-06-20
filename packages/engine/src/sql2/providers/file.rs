#![allow(
    clippy::manual_let_else,
    clippy::match_wildcard_for_single_variants,
    clippy::needless_collect,
    clippy::option_if_let_else,
    clippy::redundant_closure,
    clippy::unnecessary_literal_bound,
    clippy::unnecessary_wraps,
    clippy::unused_self,
    clippy::useless_let_if_seq
)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, RecordBatchOptions, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, DataFusionError, Result, ScalarValue};
use datafusion::execution::TaskContext;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::{PhysicalExpr, create_physical_expr};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion::prelude::SessionContext;
use futures_util::FutureExt;
use serde::Deserialize;

use crate::binary_cas::{BlobDataReader, BlobHash};
use crate::branch::BranchRefReader;
use crate::common::{LixPath, compose_file_path};
use crate::entity_pk::EntityPk;
use crate::filesystem::{FilesystemIndex, filesystem_schema_keys};
use crate::functions::FunctionProviderHandle;
use crate::live_state::{
    LiveStateFileScanRequest, LiveStateFilter, LiveStateProjection, LiveStateReader,
    LiveStateScanRequest, MaterializedLiveStateRow,
};
use crate::plugin::{
    InstalledPlugin, PluginRuntimeHost, is_plugin_storage_path,
    load_installed_plugins_from_filesystem, plugin_key_from_archive_path,
    plugin_state_live_state_projection, plugin_storage_archive_file_id,
    reject_normal_plugin_storage_mutation, render_materialized_plugin_file,
    retain_plugin_state_rows, select_plugin_for_path,
};
use crate::sql2::branch_scope::{
    BranchBinding, explicit_branch_ids_from_dml_filters, resolve_provider_branch_ids,
    resolve_write_branch_scope,
};
use crate::sql2::dml::InsertSink;
use crate::sql2::predicate_typecheck::{
    canonicalize_json_identity_text_filters, validate_json_predicate_filters,
};
use crate::sql2::write_normalization::{
    InsertCell, InsertColumnIntents, SqlCell, UpdateAssignmentValues, UpdateCell, is_binary_type,
    lix_file_data_type_error, lix_file_data_type_error_with_value, logical_expr_is_binary_or_null,
    reject_non_binary_casts_for_insert_column, scalar_is_binary_or_null,
};
use crate::transaction::types::{TransactionJson, TransactionWriteRow};
use crate::{GLOBAL_BRANCH_ID, LixError, parse_row_metadata_value, serialize_row_metadata};

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";

use crate::filesystem::{
    BlobRefRowInput, DirectoryPathRecord, DirectoryPathResolver, FileDeleteInput,
    FileDescriptorRowInput, FileDescriptorWriteInput, FileDescriptorWriteIntent,
    FilesystemBlobRefKey, FilesystemDeletePlan, FilesystemDescriptorKey, FilesystemRowContext,
    blob_ref_row, blob_ref_tombstone_row, derive_directory_paths,
    directory_path_resolvers_from_live_state, directory_path_resolvers_from_state_rows,
    file_descriptor_row, file_descriptor_write_row, filesystem_storage_scope_key, plan_file_delete,
    plan_file_descriptor_write, plan_parsed_file_path_update_with_resolvers,
    plan_parsed_file_path_write_with_resolvers,
};
use crate::sql2::result_metadata::json_field;
use crate::sql2::session::SqlWriteSessionOptions;
use crate::sql2::{
    SqlWriteContext, SqlWriteExecutionContext, WriteAccess, WriteContextBranchRefReader,
    WriteContextLiveStateReader,
};
use crate::transaction::types::{
    LogicalPrimaryKey, TransactionFileData, TransactionWrite, TransactionWriteMode,
    TransactionWriteOperation, TransactionWriteOrigin,
};

use super::spec::{
    DmlApply, InsertApply, PlannedDml, PlannedScan, RowSource, TableSpec, finish_scan_batch,
    register_spec_table, row_source,
};
use super::upsert::{
    StagedUpsert, UpsertConflictKind, UpsertConflictTarget, UpsertSupport, validate_target_columns,
};

pub(super) async fn register_lix_file_active_provider(
    session: &SessionContext,
    surface_name: &str,
    active_branch_id: &str,
    live_state: Arc<dyn LiveStateReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    blob_reader: Arc<dyn BlobDataReader>,
    plugin_host: PluginRuntimeHost,
    functions: FunctionProviderHandle,
) -> Result<(), LixError> {
    register_spec_table(
        session,
        surface_name,
        Arc::new(LixFileSpec::active_branch(
            active_branch_id,
            live_state,
            branch_ref,
            blob_reader,
            plugin_host,
            functions,
        )),
        WriteAccess::read_only(),
    )
}

pub(super) async fn register_lix_file_by_branch_provider(
    session: &SessionContext,
    surface_name: &str,
    live_state: Arc<dyn LiveStateReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    blob_reader: Arc<dyn BlobDataReader>,
    plugin_host: PluginRuntimeHost,
    functions: FunctionProviderHandle,
) -> Result<(), LixError> {
    register_spec_table(
        session,
        surface_name,
        Arc::new(LixFileSpec::by_branch(
            live_state,
            branch_ref,
            blob_reader,
            plugin_host,
            functions,
        )),
        WriteAccess::read_only(),
    )
}

pub(super) async fn register_by_branch_write_provider(
    session: &SessionContext,
    surface_name: &str,
    write_ctx: SqlWriteContext,
    options: SqlWriteSessionOptions,
) -> Result<(), LixError> {
    register_spec_table(
        session,
        surface_name,
        Arc::new(LixFileSpec::by_branch_with_write(
            write_ctx.clone(),
            options,
        )),
        WriteAccess::write(write_ctx),
    )
}

pub(super) async fn register_active_write_provider(
    session: &SessionContext,
    surface_name: &str,
    write_ctx: SqlWriteContext,
    options: SqlWriteSessionOptions,
) -> Result<(), LixError> {
    register_spec_table(
        session,
        surface_name,
        Arc::new(LixFileSpec::active_branch_with_write(
            write_ctx.clone(),
            options,
        )),
        WriteAccess::write(write_ctx),
    )
}

struct LixFileSpec {
    schema: SchemaRef,
    live_state: Arc<dyn LiveStateReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    blob_reader: Arc<dyn BlobDataReader>,
    plugin_host: PluginRuntimeHost,
    functions: FunctionProviderHandle,
    branch_binding: BranchBinding,
    options: SqlWriteSessionOptions,
}

impl LixFileSpec {
    fn active_branch(
        active_branch_id: impl Into<String>,
        live_state: Arc<dyn LiveStateReader>,
        branch_ref: Arc<dyn BranchRefReader>,
        blob_reader: Arc<dyn BlobDataReader>,
        plugin_host: PluginRuntimeHost,
        functions: FunctionProviderHandle,
    ) -> Self {
        Self {
            schema: lix_file_schema(),
            live_state,
            branch_ref,
            blob_reader,
            plugin_host,
            functions,
            branch_binding: BranchBinding::active(active_branch_id),
            options: SqlWriteSessionOptions::default(),
        }
    }

    fn active_branch_with_write(
        write_ctx: SqlWriteContext,
        options: SqlWriteSessionOptions,
    ) -> Self {
        let active_branch_id = write_ctx.active_branch_id();
        let functions = write_ctx.functions();
        let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx.clone()));
        let branch_ref = Arc::new(WriteContextBranchRefReader::new(write_ctx.clone()));
        let blob_reader = write_ctx.blob_reader();
        let plugin_host = write_ctx.plugin_host();
        Self {
            schema: lix_file_schema(),
            live_state,
            branch_ref,
            blob_reader,
            plugin_host,
            functions,
            branch_binding: BranchBinding::active(active_branch_id),
            options,
        }
    }

    fn by_branch(
        live_state: Arc<dyn LiveStateReader>,
        branch_ref: Arc<dyn BranchRefReader>,
        blob_reader: Arc<dyn BlobDataReader>,
        plugin_host: PluginRuntimeHost,
        functions: FunctionProviderHandle,
    ) -> Self {
        Self {
            schema: lix_file_by_branch_schema(),
            live_state,
            branch_ref,
            blob_reader,
            plugin_host,
            functions,
            branch_binding: BranchBinding::explicit(),
            options: SqlWriteSessionOptions::default(),
        }
    }

    fn by_branch_with_write(write_ctx: SqlWriteContext, options: SqlWriteSessionOptions) -> Self {
        let functions = write_ctx.functions();
        let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx.clone()));
        let branch_ref = Arc::new(WriteContextBranchRefReader::new(write_ctx.clone()));
        let blob_reader = write_ctx.blob_reader();
        let plugin_host = write_ctx.plugin_host();
        Self {
            schema: lix_file_by_branch_schema(),
            live_state,
            branch_ref,
            blob_reader,
            plugin_host,
            functions,
            branch_binding: BranchBinding::explicit(),
            options,
        }
    }

    /// Build the unprojected candidate-row source for UPDATE/DELETE: scan the
    /// scoped live-state rows, then render the full `lix_file` batch the
    /// statement filters run against.
    fn dml_source(
        &self,
        write_ctx: &SqlWriteContext,
        request: LiveStateScanRequest,
        target_file_ids: FileIdConstraint,
        needs_data: bool,
    ) -> RowSource {
        row_source(
            (
                write_ctx.clone(),
                Arc::clone(&self.blob_reader),
                self.plugin_host.clone(),
                Arc::clone(&self.schema),
                request,
                target_file_ids,
                needs_data,
            ),
            |(
                write_ctx,
                blob_reader,
                plugin_host,
                table_schema,
                request,
                target_file_ids,
                needs_data,
            )| async move {
                let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx.clone()));
                let rows = scan_lix_file_live_rows(live_state.clone(), &request, &target_file_ids)
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
                let plugin_render = plugin_render_context_for_lix_file_scan(
                    live_state,
                    &blob_reader,
                    &request,
                    plugin_host,
                    needs_data,
                )
                .await
                .map_err(|error| {
                    DataFusionError::Execution(format!(
                        "sql2 lix_file plugin discovery failed: {error}"
                    ))
                })?;
                lix_file_record_batch(&table_schema, &blob_reader, plugin_render, needs_data, rows)
                    .await
                    .map_err(lix_error_to_datafusion_error)
            },
        )
    }
}

#[async_trait]
impl TableSpec for LixFileSpec {
    fn table_name(&self) -> &str {
        "lix_file"
    }

    fn upsert_support(&self) -> Option<&dyn UpsertSupport> {
        Some(self)
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn filter_pushdown(&self, filter: &Expr) -> TableProviderFilterPushDown {
        let analyzer = LixFileIdFilterAnalyzer;
        if ExactStringColumnFilterAnalyzer::new("lixcol_branch_id").supports(filter)
            || analyzer.supports(filter)
            || contains_column(filter, "path")
        {
            TableProviderFilterPushDown::Exact
        } else {
            TableProviderFilterPushDown::Unsupported
        }
    }

    async fn plan_scan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        props: &ExecutionProps,
    ) -> Result<PlannedScan> {
        let projected_schema = projected_schema(&self.schema, projection)?;
        let scan_limit = if filters.is_empty() { limit } else { None };
        let mut request = lix_file_scan_request(
            self.branch_binding.active_branch_id(),
            Some(projected_schema.as_ref()),
            scan_limit,
        );
        let filters = filters.to_vec();
        if matches!(self.branch_binding, BranchBinding::Explicit) {
            request.filter.branch_ids = explicit_branch_ids_from_dml_filters(&filters);
        }
        request.filter.branch_ids = resolve_provider_branch_ids(
            self.branch_ref.as_ref(),
            &self.branch_binding,
            request.filter.branch_ids,
        )
        .await
        .map_err(lix_error_to_datafusion_error)?;
        let needs_data = scan_needs_data(&self.schema, projection, &filters);
        let target_file_ids = file_id_constraint_from_filters(&filters)?;
        let df_schema = DFSchema::try_from(Arc::clone(&self.schema))?;
        validate_json_predicate_filters(self.schema.as_ref(), &filters)?;
        let physical_filters = filters
            .iter()
            .map(|expr| create_physical_expr(expr, &df_schema, props))
            .collect::<Result<Vec<_>>>()?;
        Ok(PlannedScan {
            schema: Arc::clone(&projected_schema),
            load: row_source(
                (
                    Arc::clone(&self.live_state),
                    Arc::clone(&self.blob_reader),
                    self.plugin_host.clone(),
                    Arc::clone(&self.schema),
                    projection.cloned(),
                    request,
                    target_file_ids,
                    physical_filters,
                    needs_data,
                    limit,
                ),
                |(
                    live_state,
                    blob_reader,
                    plugin_host,
                    batch_schema,
                    projection,
                    request,
                    target_file_ids,
                    filters,
                    needs_data,
                    limit,
                )| async move {
                    let rows = scan_lix_file_live_rows(
                        Arc::clone(&live_state),
                        &request,
                        &target_file_ids,
                    )
                    .await
                    .map_err(|error| {
                        DataFusionError::Execution(format!("sql2 lix_file scan failed: {error}"))
                    })?;
                    let plugin_render = plugin_render_context_for_lix_file_scan(
                        Arc::clone(&live_state),
                        &blob_reader,
                        &request,
                        plugin_host,
                        needs_data,
                    )
                    .await
                    .map_err(|error| {
                        DataFusionError::Execution(format!(
                            "sql2 lix_file plugin discovery failed: {error}"
                        ))
                    })?;
                    let batch = lix_file_record_batch(
                        &batch_schema,
                        &blob_reader,
                        plugin_render,
                        needs_data,
                        rows,
                    )
                    .await
                    .map_err(|error| {
                        DataFusionError::Execution(format!(
                            "sql2 lix_file batch build failed: {error}"
                        ))
                    })?;
                    finish_scan_batch(batch, &filters, projection.as_deref(), limit, "lix_file")
                },
            ),
        })
    }

    async fn plan_insert(
        &self,
        write_ctx: SqlWriteContext,
        input: &Arc<dyn ExecutionPlan>,
    ) -> Result<Option<InsertApply>> {
        let insert_intents = InsertColumnIntents::from_input(input);
        let include_data_writes = self.schema.field_with_name("data").is_ok()
            && insert_intents.includes_column("data")
            && !self.options.omitted_insert_columns.contains("data");
        if include_data_writes {
            reject_non_binary_casts_for_insert_column(input, "data", "INSERT into lix_file")?;
        }
        let sink = Arc::new(LixFileInsertSink::new(
            write_ctx,
            self.functions.clone(),
            self.branch_binding.clone(),
            include_data_writes,
        ));
        let apply: InsertApply = Arc::new(move |batches| {
            let sink = Arc::clone(&sink);
            async move {
                sink.write_batches(batches, &Arc::new(TaskContext::default()))
                    .await
            }
            .boxed()
        });
        Ok(Some(apply))
    }

    fn validate_update_assignments(&self, assignments: &[(String, Expr)]) -> Result<()> {
        validate_lix_file_update_assignments(&self.schema, assignments)
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
        let needs_data = filters.iter().any(|filter| contains_column(filter, "data"));
        let target_file_ids = file_id_constraint_from_filters(filters)?;
        let mut request = lix_file_scan_request(self.branch_binding.active_branch_id(), None, None);
        request.filter.branch_ids = explicit_branch_ids_from_dml_filters(filters);
        request.filter.branch_ids = resolve_provider_branch_ids(
            self.branch_ref.as_ref(),
            &self.branch_binding,
            request.filter.branch_ids,
        )
        .await
        .map_err(lix_error_to_datafusion_error)?;

        let source = self.dml_source(
            &write_ctx,
            request.clone(),
            target_file_ids.clone(),
            needs_data,
        );
        let branch_binding = self.branch_binding.clone();
        let apply: DmlApply = Arc::new(move |matched_batch| {
            let write_ctx = write_ctx.clone();
            let branch_binding = branch_binding.clone();
            let request = request.clone();
            let target_file_ids = target_file_ids.clone();
            async move {
                let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx.clone()));
                let rows = scan_lix_file_live_rows(live_state, &request, &target_file_ids)
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
                let blob_ref_keys =
                    blob_ref_keys_from_live_rows(&rows).map_err(lix_error_to_datafusion_error)?;
                let staged = lix_file_delete_stage_from_batch(
                    &matched_batch,
                    branch_binding.active_branch_id(),
                    &blob_ref_keys,
                )?;
                let count = staged.count;

                if count > 0 {
                    write_ctx
                        .stage_write(TransactionWrite::Rows {
                            mode: TransactionWriteMode::Replace,
                            rows: staged.state_rows,
                        })
                        .await
                        .map_err(lix_error_to_datafusion_error)?;
                }

                Ok(count)
            }
            .boxed()
        });
        Ok(PlannedDml { source, apply })
    }

    async fn plan_update(
        &self,
        write_ctx: SqlWriteContext,
        assignments: Vec<(String, Arc<dyn PhysicalExpr>)>,
        filters: &[Expr],
    ) -> Result<PlannedDml> {
        let needs_data = filters.iter().any(|filter| contains_column(filter, "data"))
            || assignments.iter().any(|(column_name, expr)| {
                column_name == "path" || physical_expr_contains_column(expr, "data")
            });
        let target_file_ids = file_id_constraint_from_filters(filters)?;
        let mut request = lix_file_scan_request(self.branch_binding.active_branch_id(), None, None);
        request.filter.branch_ids = explicit_branch_ids_from_dml_filters(filters);
        request.filter.branch_ids = resolve_provider_branch_ids(
            self.branch_ref.as_ref(),
            &self.branch_binding,
            request.filter.branch_ids,
        )
        .await
        .map_err(lix_error_to_datafusion_error)?;

        let source = self.dml_source(
            &write_ctx,
            request.clone(),
            target_file_ids.clone(),
            needs_data,
        );
        let branch_binding = self.branch_binding.clone();
        let functions = self.functions.clone();
        let blob_reader = Arc::clone(&self.blob_reader);
        let plugin_host = self.plugin_host.clone();
        let apply: DmlApply = Arc::new(move |matched_batch| {
            let write_ctx = write_ctx.clone();
            let branch_binding = branch_binding.clone();
            let functions = functions.clone();
            let blob_reader = Arc::clone(&blob_reader);
            let plugin_host = plugin_host.clone();
            let request = request.clone();
            let target_file_ids = target_file_ids.clone();
            let assignments = assignments.clone();
            async move {
                let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx.clone()));
                let rows = scan_lix_file_live_rows(live_state.clone(), &request, &target_file_ids)
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
                let blob_ref_keys =
                    blob_ref_keys_from_live_rows(&rows).map_err(lix_error_to_datafusion_error)?;
                let plugin_render = plugin_render_context_for_lix_file_scan(
                    live_state,
                    &blob_reader,
                    &request,
                    plugin_host,
                    needs_data,
                )
                .await
                .map_err(|error| {
                    DataFusionError::Execution(format!(
                        "sql2 lix_file plugin discovery failed: {error}"
                    ))
                })?;
                let assignment_values =
                    UpdateAssignmentValues::evaluate(&matched_batch, &assignments)?;
                let update_columns = LixFileUpdateColumns::from_assignments(&assignments);
                let plugin_rewrite_file_ids = if update_columns.path && !update_columns.data {
                    path_update_plugin_rewrite_file_ids(
                        plugin_render.as_ref(),
                        &matched_batch,
                        &assignment_values,
                        branch_binding.active_branch_id(),
                    )?
                } else {
                    BTreeSet::new()
                };
                let mut path_resolvers = None;
                if update_columns.path || update_columns.descriptor {
                    path_resolvers = Some(
                        directory_path_resolvers_from_live_state(
                            Arc::new(WriteContextLiveStateReader::new(write_ctx.clone())),
                            branch_binding.active_branch_id(),
                        )
                        .await
                        .map_err(lix_error_to_datafusion_error)?,
                    );
                }
                let staged = lix_file_update_stage_from_batch(
                    &matched_batch,
                    &assignment_values,
                    branch_binding.active_branch_id(),
                    update_columns,
                    &blob_ref_keys,
                    &plugin_rewrite_file_ids,
                    path_resolvers.as_mut(),
                    &mut || functions.call_uuid_v7().to_string(),
                )?;
                let count = staged.count;

                if count > 0 {
                    let intent = if staged.file_data_writes.is_empty() {
                        TransactionWrite::Rows {
                            mode: TransactionWriteMode::Replace,
                            rows: staged.state_rows,
                        }
                    } else {
                        TransactionWrite::RowsWithFileData {
                            mode: TransactionWriteMode::Replace,
                            rows: staged.state_rows,
                            file_data: staged.file_data_writes,
                            count,
                        }
                    };
                    write_ctx
                        .stage_write(intent)
                        .await
                        .map_err(lix_error_to_datafusion_error)?;
                }

                Ok(count)
            }
            .boxed()
        });
        Ok(PlannedDml { source, apply })
    }
}

/// Physical and path identities the upsert driver can match `lix_file` rows
/// on. Path targets model the visible filesystem identity for active and
/// by-branch surfaces.
const LIX_FILE_IDENTITY: &[&str] = &["id"];
const LIX_FILE_PATH_IDENTITY: &[&str] = &["path"];
const LIX_FILE_BY_BRANCH_PATH_IDENTITY: &[&str] = &["path", "lixcol_branch_id"];

#[async_trait]
impl UpsertSupport for LixFileSpec {
    fn conflict_identity_columns(&self) -> &[&'static str] {
        LIX_FILE_IDENTITY
    }

    fn resolve_conflict_target(
        &self,
        table_name: &str,
        target_columns: &[String],
    ) -> Result<UpsertConflictTarget> {
        if validate_target_columns(
            table_name,
            target_columns,
            LIX_FILE_IDENTITY,
            "conflict identity columns",
        )
        .is_ok()
        {
            return Ok(UpsertConflictTarget::id(LIX_FILE_IDENTITY));
        }

        let path_identity = match self.branch_binding {
            BranchBinding::Active { .. } => LIX_FILE_PATH_IDENTITY,
            BranchBinding::Explicit => LIX_FILE_BY_BRANCH_PATH_IDENTITY,
        };
        validate_target_columns(
            table_name,
            target_columns,
            path_identity,
            "path identity columns",
        )?;
        Ok(UpsertConflictTarget::path(path_identity))
    }

    async fn insert_staged_rows(
        &self,
        write_ctx: &SqlWriteContext,
        batch: &RecordBatch,
    ) -> Result<StagedUpsert> {
        // Reuse the plain-INSERT staging the file insert sink performs, for a
        // single proposed batch. The collected proposed batch has lost the
        // per-column insert intent metadata, so `include_data_writes` is
        // derived from whether the materialized `data` column carries a value.
        let surface_name = lix_file_surface_name(&self.branch_binding);
        let branch_binding = self.branch_binding.active_branch_id();
        let include_data_writes = record_batch_has_non_null_column(batch, "data")?;

        let mut path_resolvers = directory_path_resolvers_from_live_state(
            Arc::new(WriteContextLiveStateReader::new(write_ctx.clone())),
            branch_binding,
        )
        .await
        .map_err(lix_error_to_datafusion_error)?;

        let staged = if record_batch_has_non_null_column(batch, "path")? {
            lix_file_insert_stage_from_batch_with_path_resolvers(
                batch,
                branch_binding,
                surface_name,
                &mut path_resolvers,
                &mut || self.functions.call_uuid_v7().to_string(),
                include_data_writes,
            )?
        } else {
            lix_file_insert_stage_from_batch_with_id_generator_and_path_resolvers(
                batch,
                branch_binding,
                surface_name,
                &mut path_resolvers,
                &mut || self.functions.call_uuid_v7().to_string(),
                include_data_writes,
            )?
        };

        Ok(StagedUpsert::with_file_data(
            staged.state_rows,
            staged.file_data_writes,
        ))
    }

    async fn scan_conflict_candidates(
        &self,
        write_ctx: &SqlWriteContext,
        proposed: &RecordBatch,
        target: &UpsertConflictTarget,
    ) -> Result<RecordBatch> {
        // Existing rows whose `id` matches a proposed row, rendered as a full
        // `lix_file` batch (with materialized `data`) so the driver can build
        // the augmented `excluded.*` batch the conflict assignments run over.
        let target_file_ids = match target.kind() {
            UpsertConflictKind::Id => proposed_file_id_constraint(proposed)?,
            UpsertConflictKind::Path => {
                validate_required_paths(proposed, "lix_file")?;
                FileIdConstraint::All
            }
        };
        let mut request = lix_file_scan_request(self.branch_binding.active_branch_id(), None, None);
        if matches!(self.branch_binding, BranchBinding::Explicit) {
            request.filter.branch_ids = match target.kind() {
                UpsertConflictKind::Id => proposed_branch_ids(proposed)?,
                UpsertConflictKind::Path => required_proposed_branch_ids(proposed, "lix_file")?,
            };
        }
        request.filter.branch_ids = resolve_provider_branch_ids(
            self.branch_ref.as_ref(),
            &self.branch_binding,
            request.filter.branch_ids,
        )
        .await
        .map_err(lix_error_to_datafusion_error)?;

        let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx.clone()));
        let rows = scan_lix_file_live_rows(live_state.clone(), &request, &target_file_ids)
            .await
            .map_err(lix_error_to_datafusion_error)?;
        let plugin_render = plugin_render_context_for_lix_file_scan(
            live_state,
            &self.blob_reader,
            &request,
            self.plugin_host.clone(),
            true,
        )
        .await
        .map_err(|error| {
            DataFusionError::Execution(format!("sql2 lix_file plugin discovery failed: {error}"))
        })?;
        lix_file_record_batch(&self.schema, &self.blob_reader, plugin_render, true, rows)
            .await
            .map_err(lix_error_to_datafusion_error)
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
                "INSERT ON CONFLICT (path) on lix_file cannot write {} path {path:?} over existing {} file",
                lane_name(proposed_untracked),
                lane_name(existing_untracked)
            ),
        )))
    }

    async fn apply_conflict_update(
        &self,
        write_ctx: &SqlWriteContext,
        augmented: &RecordBatch,
        assignments: &[(String, Arc<dyn PhysicalExpr>)],
    ) -> Result<StagedUpsert> {
        // Reuse the plain-UPDATE staging. `augmented` carries the existing file
        // columns plus `excluded.*`; the physical assignments reference both.
        let branch_binding = self.branch_binding.active_branch_id();
        let assignment_values = UpdateAssignmentValues::evaluate(augmented, assignments)?;
        let update_columns = LixFileUpdateColumns::from_assignments(assignments);

        // Re-scan the conflicting files' live rows to recover their blob refs
        // (needed to tombstone the old blob when `data` is replaced) and any
        // plugins installed for path-move rewrites.
        let target_file_ids = augmented_file_id_constraint(augmented)?;
        let mut request = lix_file_scan_request(self.branch_binding.active_branch_id(), None, None);
        if matches!(self.branch_binding, BranchBinding::Explicit) {
            request.filter.branch_ids = augmented_branch_ids(augmented)?;
        }
        request.filter.branch_ids = resolve_provider_branch_ids(
            self.branch_ref.as_ref(),
            &self.branch_binding,
            request.filter.branch_ids,
        )
        .await
        .map_err(lix_error_to_datafusion_error)?;

        let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx.clone()));
        let rows = scan_lix_file_live_rows(live_state.clone(), &request, &target_file_ids)
            .await
            .map_err(lix_error_to_datafusion_error)?;
        let blob_ref_keys =
            blob_ref_keys_from_live_rows(&rows).map_err(lix_error_to_datafusion_error)?;

        let plugin_rewrite_file_ids = if update_columns.path && !update_columns.data {
            let plugin_render = plugin_render_context_for_lix_file_scan(
                live_state.clone(),
                &self.blob_reader,
                &request,
                self.plugin_host.clone(),
                true,
            )
            .await
            .map_err(|error| {
                DataFusionError::Execution(format!(
                    "sql2 lix_file plugin discovery failed: {error}"
                ))
            })?;
            path_update_plugin_rewrite_file_ids(
                plugin_render.as_ref(),
                augmented,
                &assignment_values,
                branch_binding,
            )?
        } else {
            BTreeSet::new()
        };

        let mut path_resolvers = None;
        if update_columns.path || update_columns.descriptor {
            path_resolvers = Some(
                directory_path_resolvers_from_live_state(
                    Arc::new(WriteContextLiveStateReader::new(write_ctx.clone())),
                    branch_binding,
                )
                .await
                .map_err(lix_error_to_datafusion_error)?,
            );
        }

        let staged = lix_file_update_stage_from_batch(
            augmented,
            &assignment_values,
            branch_binding,
            update_columns,
            &blob_ref_keys,
            &plugin_rewrite_file_ids,
            path_resolvers.as_mut(),
            &mut || self.functions.call_uuid_v7().to_string(),
        )?;

        Ok(StagedUpsert::with_file_data(
            staged.state_rows,
            staged.file_data_writes,
        ))
    }
}

/// The conflict-identity (`id`) constraint of the proposed insert batch: the
/// distinct file ids whose existing rows must be scanned for conflicts.
fn proposed_file_id_constraint(batch: &RecordBatch) -> Result<FileIdConstraint> {
    let mut ids = Vec::new();
    for row_index in 0..batch.num_rows() {
        if let Some(id) = optional_string_value(batch, row_index, "id")? {
            ids.push(id);
        }
    }
    if ids.is_empty() {
        // No explicit ids: nothing can conflict (every inserted row will be
        // assigned a fresh id), so the candidate scan should match nothing.
        return Ok(FileIdConstraint::None);
    }
    Ok(FileIdConstraint::from_ids(ids))
}

/// The `id` constraint of an augmented conflict batch (existing-row columns).
fn augmented_file_id_constraint(batch: &RecordBatch) -> Result<FileIdConstraint> {
    let mut ids = Vec::new();
    for row_index in 0..batch.num_rows() {
        ids.push(required_string_value(batch, row_index, "id")?);
    }
    Ok(FileIdConstraint::from_ids(ids))
}

/// Distinct explicit `lixcol_branch_id` values in a proposed insert batch
/// (by-branch surface). Empty when the column is absent or all-null.
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

/// Distinct `lixcol_branch_id` values carried by an augmented conflict batch.
fn augmented_branch_ids(batch: &RecordBatch) -> Result<Vec<String>> {
    proposed_branch_ids(batch)
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

struct LixFileInsertSink {
    write_ctx: SqlWriteContext,
    functions: FunctionProviderHandle,
    branch_binding: BranchBinding,
    surface_name: &'static str,
    include_data_writes: bool,
}

impl std::fmt::Debug for LixFileInsertSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixFileInsertSink").finish()
    }
}

impl LixFileInsertSink {
    fn new(
        write_ctx: SqlWriteContext,
        functions: FunctionProviderHandle,
        branch_binding: BranchBinding,
        include_data_writes: bool,
    ) -> Self {
        let surface_name = lix_file_surface_name(&branch_binding);
        Self {
            write_ctx,
            functions,
            branch_binding,
            surface_name,
            include_data_writes,
        }
    }
}

impl DisplayAs for LixFileInsertSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "LixFileInsertSink")
            }
            DisplayFormatType::TreeRender => write!(f, "LixFileInsertSink"),
        }
    }
}

#[async_trait]
impl InsertSink for LixFileInsertSink {
    async fn write_batches(
        &self,
        batches: Vec<RecordBatch>,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let mut staged = LixFileStagedBatch::default();
        let mut path_resolvers = None;
        for batch in batches {
            if path_resolvers.is_none() {
                path_resolvers = Some(
                    directory_path_resolvers_from_live_state(
                        Arc::new(WriteContextLiveStateReader::new(self.write_ctx.clone())),
                        self.branch_binding.active_branch_id(),
                    )
                    .await
                    .map_err(lix_error_to_datafusion_error)?,
                );
            }
            if record_batch_has_non_null_column(&batch, "path")? {
                staged.extend(lix_file_insert_stage_from_batch_with_path_resolvers(
                    &batch,
                    self.branch_binding.active_branch_id(),
                    self.surface_name,
                    path_resolvers
                        .as_mut()
                        .expect("path resolver should be initialized"),
                    &mut || self.functions.call_uuid_v7().to_string(),
                    self.include_data_writes,
                )?);
            } else {
                staged.extend(
                    lix_file_insert_stage_from_batch_with_id_generator_and_path_resolvers(
                        &batch,
                        self.branch_binding.active_branch_id(),
                        self.surface_name,
                        path_resolvers
                            .as_mut()
                            .expect("path resolver should be initialized"),
                        &mut || self.functions.call_uuid_v7().to_string(),
                        self.include_data_writes,
                    )?,
                );
            }
        }

        if !staged.state_rows.is_empty() || !staged.file_data_writes.is_empty() {
            let intent = if staged.file_data_writes.is_empty() {
                TransactionWrite::Rows {
                    mode: TransactionWriteMode::Insert,
                    rows: staged.state_rows,
                }
            } else {
                TransactionWrite::RowsWithFileData {
                    mode: TransactionWriteMode::Insert,
                    rows: staged.state_rows,
                    file_data: staged.file_data_writes,
                    count: staged.count,
                }
            };
            self.write_ctx
                .stage_write(intent)
                .await
                .map_err(lix_error_to_datafusion_error)?;
        }

        Ok(staged.count)
    }
}

fn lix_file_surface_name(branch_binding: &BranchBinding) -> &'static str {
    match branch_binding {
        BranchBinding::Active { .. } => "lix_file",
        BranchBinding::Explicit => "lix_file_by_branch",
    }
}

#[derive(Debug, Clone)]
struct FileDescriptorRecord {
    id: String,
    directory_id: Option<String>,
    name: String,
    key: FilesystemDescriptorKey,
    live: MaterializedLiveStateRow,
}

impl FileDescriptorRecord {
    fn directory_parent_keys(&self, directory_id: &str) -> Vec<FilesystemDescriptorKey> {
        let mut keys = vec![self.key.in_same_scope(directory_id)];
        if self.key.is_untracked() {
            keys.push(self.key.in_tracked_scope(directory_id));
        }
        keys
    }
}

#[derive(Clone)]
struct PluginRenderContext {
    live_state: Arc<dyn LiveStateReader>,
    host: PluginRuntimeHost,
    installed_plugins_by_branch: BTreeMap<String, Vec<InstalledPlugin>>,
}

impl PluginRenderContext {
    fn installed_plugins_for_branch(&self, branch_id: &str) -> &[InstalledPlugin] {
        self.installed_plugins_by_branch
            .get(branch_id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }
}

#[derive(Debug, Clone)]
struct BlobRefRecord {
    blob_hash: String,
}

#[derive(Debug, Clone)]
struct DirectoryDescriptorRecord {
    parent_id: Option<String>,
    name: String,
    key: FilesystemDescriptorKey,
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
struct FileDescriptorSnapshot {
    id: String,
    directory_id: Option<String>,
    name: String,
}

#[derive(Debug, Deserialize)]
struct BlobRefSnapshot {
    id: String,
    blob_hash: String,
}

#[derive(Debug, Deserialize)]
struct DirectoryDescriptorSnapshot {
    id: String,
    parent_id: Option<String>,
    name: String,
}

#[derive(Debug, Default)]
struct LixFileStagedBatch {
    state_rows: Vec<TransactionWriteRow>,
    file_data_writes: Vec<TransactionFileData>,
    count: u64,
}

impl LixFileStagedBatch {
    fn extend(&mut self, other: Self) {
        self.state_rows.extend(other.state_rows);
        self.file_data_writes.extend(other.file_data_writes);
        self.count += other.count;
    }

    fn extend_filesystem_plan(&mut self, plan: crate::filesystem::FilesystemWritePlan) {
        self.state_rows.extend(plan.rows);
        self.file_data_writes.extend(plan.file_data);
        self.count += plan.count;
    }

    fn extend_filesystem_delete_plan(&mut self, plan: FilesystemDeletePlan) {
        self.state_rows.extend(plan.rows);
        self.count += plan.count;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FastLixFilePathWriteConflict {
    None,
    DoNothing,
    UpdateData,
}

pub(crate) async fn execute_fast_lix_file_path_write(
    ctx: &mut dyn SqlWriteExecutionContext,
    path: String,
    data: Vec<u8>,
    conflict: FastLixFilePathWriteConflict,
) -> Result<u64, LixError> {
    let active_branch_id = ctx.active_branch_id().to_string();
    let parsed = parse_file_upsert_path(&path, TransactionWriteOperation::Insert)
        .map_err(crate::sql2::error::datafusion_error_to_lix_error)?;

    let live_rows = ctx
        .scan_live_state(&LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: filesystem_schema_keys(),
                branch_ids: vec![active_branch_id.clone()],
                include_tombstones: false,
                ..LiveStateFilter::default()
            },
            ..LiveStateScanRequest::default()
        })
        .await?;
    let filesystem = FilesystemIndex::from_live_rows(live_rows.clone())?;

    if let Some(existing) = filesystem.file_entry(&parsed.path).cloned() {
        if conflict != FastLixFilePathWriteConflict::None {
            validate_fast_lix_file_path_conflict_pair(existing.scope.untracked, &parsed.path)?;
        }
        return match conflict {
            FastLixFilePathWriteConflict::None => {
                let mut path_resolvers = directory_path_resolvers_from_state_rows(live_rows)?;
                let context = FilesystemRowContext {
                    branch_id: active_branch_id.clone(),
                    global: false,
                    untracked: false,
                    file_id: None,
                    metadata: None,
                };
                let plan = plan_parsed_file_path_write_with_resolvers(
                    &mut path_resolvers,
                    parsed.parsed_path,
                    Some(
                        parsed
                            .plugin_key
                            .as_deref()
                            .map(plugin_storage_archive_file_id)
                            .unwrap_or_else(|| ctx.functions().call_uuid_v7().to_string()),
                    ),
                    Some(data),
                    context,
                    &mut || ctx.functions().call_uuid_v7().to_string(),
                )?;
                let mut staged = LixFileStagedBatch::default();
                staged.extend_filesystem_plan(plan);
                stage_lix_file_fast_batch(ctx, TransactionWriteMode::Insert, staged).await
            }
            FastLixFilePathWriteConflict::DoNothing => Ok(0),
            FastLixFilePathWriteConflict::UpdateData => {
                let mut staged = LixFileStagedBatch::default();
                let mut context = existing.scope.context(Some(existing.id.clone()));
                if context.global {
                    context.branch_id = GLOBAL_BRANCH_ID.to_string();
                }
                stage_lix_file_data_update_write(
                    &mut staged,
                    existing.id.clone(),
                    Some(parsed.path),
                    Some(existing.name.clone()),
                    data,
                    context,
                    existing.blob_hash.is_some(),
                    None,
                )
                .map_err(crate::sql2::error::datafusion_error_to_lix_error)?;
                staged.count = 1;
                stage_lix_file_fast_batch(ctx, TransactionWriteMode::Replace, staged).await
            }
        };
    }

    let mut path_resolvers = directory_path_resolvers_from_state_rows(live_rows)?;
    let resolver_key = filesystem_storage_scope_key(&active_branch_id, false, false, None);
    path_resolvers.entry(resolver_key).or_default();
    let context = FilesystemRowContext {
        branch_id: active_branch_id,
        global: false,
        untracked: false,
        file_id: None,
        metadata: None,
    };
    let file_id = parsed
        .plugin_key
        .as_deref()
        .map(plugin_storage_archive_file_id)
        .unwrap_or_else(|| ctx.functions().call_uuid_v7().to_string());
    let mut plan = plan_parsed_file_path_write_with_resolvers(
        &mut path_resolvers,
        parsed.parsed_path,
        Some(file_id.clone()),
        Some(data),
        context,
        &mut || ctx.functions().call_uuid_v7().to_string(),
    )?;
    attach_lix_file_insert_origin(&mut plan.rows, "lix_file", &file_id);
    let mut staged = LixFileStagedBatch::default();
    staged.extend_filesystem_plan(plan);
    let mode = match conflict {
        FastLixFilePathWriteConflict::None => TransactionWriteMode::Insert,
        FastLixFilePathWriteConflict::DoNothing | FastLixFilePathWriteConflict::UpdateData => {
            TransactionWriteMode::Replace
        }
    };
    stage_lix_file_fast_batch(ctx, mode, staged).await
}

fn validate_fast_lix_file_path_conflict_pair(
    existing_untracked: bool,
    path: &str,
) -> Result<(), LixError> {
    let proposed_untracked = false;
    if existing_untracked == proposed_untracked {
        return Ok(());
    }
    Err(LixError::new(
        LixError::CODE_CONSTRAINT_VIOLATION,
        format!(
            "INSERT ON CONFLICT (path) on lix_file cannot write {} path {path:?} over existing {} file",
            lane_name(proposed_untracked),
            lane_name(existing_untracked)
        ),
    ))
}

async fn stage_lix_file_fast_batch(
    ctx: &mut dyn SqlWriteExecutionContext,
    mode: TransactionWriteMode,
    staged: LixFileStagedBatch,
) -> Result<u64, LixError> {
    let count = staged.count;
    if staged.state_rows.is_empty() && staged.file_data_writes.is_empty() {
        return Ok(count);
    }
    let write = if staged.file_data_writes.is_empty() {
        TransactionWrite::Rows {
            mode,
            rows: staged.state_rows,
        }
    } else {
        TransactionWrite::RowsWithFileData {
            mode,
            rows: staged.state_rows,
            file_data: staged.file_data_writes,
            count,
        }
    };
    let outcome = ctx.stage_write(write).await?;
    Ok(outcome.count)
}

#[cfg(test)]
fn lix_file_write_rows_from_batch(
    batch: &RecordBatch,
    branch_binding: Option<&str>,
) -> Result<Vec<TransactionWriteRow>> {
    Ok(lix_file_insert_stage_from_batch(batch, branch_binding)?.state_rows)
}

fn lix_file_delete_stage_from_batch(
    batch: &RecordBatch,
    branch_binding: Option<&str>,
    blob_ref_keys: &BTreeSet<FilesystemBlobRefKey>,
) -> Result<LixFileStagedBatch> {
    let mut staged = LixFileStagedBatch::default();
    for row_index in 0..batch.num_rows() {
        if let Some(path) = optional_string_value(batch, row_index, "path")? {
            parse_normal_write_file_path(&path, TransactionWriteOperation::Delete)?;
        }
        let file_id = required_string_value(batch, row_index, "id")?;
        let context = file_row_context_from_batch(batch, row_index, branch_binding)?;
        staged.extend_filesystem_delete_plan(plan_file_delete(FileDeleteInput {
            file_id: file_id.clone(),
            has_blob_ref: blob_ref_keys
                .contains(&FilesystemBlobRefKey::from_context(&context, &file_id)),
            context,
        }));
    }
    Ok(staged)
}

fn blob_ref_keys_from_live_rows(
    rows: &[MaterializedLiveStateRow],
) -> std::result::Result<BTreeSet<FilesystemBlobRefKey>, LixError> {
    let mut keys = BTreeSet::new();
    for row in rows {
        if row.schema_key != BLOB_REF_SCHEMA_KEY {
            continue;
        }
        let Some(snapshot_content) = row.snapshot_content.as_deref() else {
            continue;
        };
        let snapshot: BlobRefSnapshot =
            serde_json::from_str(snapshot_content).map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("invalid lix_binary_blob_ref snapshot JSON: {error}"),
                )
            })?;
        keys.insert(FilesystemBlobRefKey::from_live_row(row, snapshot.id));
    }
    Ok(keys)
}

#[cfg(test)]
fn lix_file_insert_stage_from_batch(
    batch: &RecordBatch,
    branch_binding: Option<&str>,
) -> Result<LixFileStagedBatch> {
    lix_file_stage_from_batch_with_options(batch, branch_binding, "lix_file", true, true, true)
}

fn lix_file_insert_stage_from_batch_with_id_generator_and_path_resolvers(
    batch: &RecordBatch,
    branch_binding: Option<&str>,
    surface_name: &str,
    path_resolvers: &mut BTreeMap<String, DirectoryPathResolver>,
    generate_id: &mut dyn FnMut() -> String,
    include_data_writes: bool,
) -> Result<LixFileStagedBatch> {
    lix_file_stage_from_batch_with_options_and_path_resolvers(
        batch,
        branch_binding,
        surface_name,
        true,
        true,
        include_data_writes,
        Some(path_resolvers),
        Some(generate_id),
    )
}

fn lix_file_insert_stage_from_batch_with_path_resolvers(
    batch: &RecordBatch,
    branch_binding: Option<&str>,
    surface_name: &str,
    path_resolvers: &mut BTreeMap<String, DirectoryPathResolver>,
    generate_directory_id: &mut dyn FnMut() -> String,
    include_data_writes: bool,
) -> Result<LixFileStagedBatch> {
    lix_file_stage_from_batch_with_options_and_path_resolvers(
        batch,
        branch_binding,
        surface_name,
        true,
        true,
        include_data_writes,
        Some(path_resolvers),
        Some(generate_directory_id),
    )
}

fn lix_file_existing_update_stage_from_batch(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    branch_binding: Option<&str>,
    include_descriptor_writes: bool,
    include_data_writes: bool,
    blob_ref_keys: &BTreeSet<FilesystemBlobRefKey>,
    path_resolvers: Option<&mut BTreeMap<String, DirectoryPathResolver>>,
) -> Result<LixFileStagedBatch> {
    let mut staged = LixFileStagedBatch::default();
    let mut path_resolvers = path_resolvers;

    for row_index in 0..batch.num_rows() {
        let id = required_string_value(batch, row_index, "id")?;
        let context =
            file_row_context_from_update(batch, assignment_values, row_index, branch_binding)?;
        let mut data_path = None;
        let mut data_filename = None;
        if include_descriptor_writes {
            let directory_id =
                update_optional_string_value(batch, assignment_values, row_index, "directory_id")?;
            let name = update_required_string_value(batch, assignment_values, row_index, "name")?;
            data_filename = Some(name.clone());
            if let Some(path_resolvers) = path_resolvers.as_deref_mut() {
                let resolver = path_resolvers
                    .entry(file_path_resolver_key(&context))
                    .or_insert_with(DirectoryPathResolver::default);
                resolver
                    .reserve_file(directory_id.clone(), name.clone(), id.clone())
                    .map_err(lix_error_to_datafusion_error)?;
                data_path = resolver
                    .file_path(directory_id.as_deref(), &name)
                    .map_err(lix_error_to_datafusion_error)?;
            }
            staged
                .state_rows
                .push(file_descriptor_row(FileDescriptorRowInput {
                    id: id.clone(),
                    directory_id,
                    name,
                    context: context.clone(),
                }));
        }

        if include_data_writes {
            let data = update_required_binary_value(batch, assignment_values, row_index, "data")?;
            let path = if include_descriptor_writes {
                data_path
            } else {
                optional_string_value(batch, row_index, "path")?
            };
            let has_blob_ref =
                blob_ref_keys.contains(&FilesystemBlobRefKey::from_context(&context, &id));
            stage_lix_file_data_update_write(
                &mut staged,
                id.clone(),
                path,
                data_filename,
                data,
                context,
                has_blob_ref,
                None,
            )?;
        }

        staged.count = staged
            .count
            .checked_add(1)
            .ok_or_else(|| DataFusionError::Execution("lix_file row count overflow".into()))?;
    }

    Ok(staged)
}

#[derive(Debug, Clone, Copy)]
struct LixFileUpdateColumns {
    path: bool,
    data: bool,
    descriptor: bool,
}

impl LixFileUpdateColumns {
    fn from_assignments(assignments: &[(String, Arc<dyn PhysicalExpr>)]) -> Self {
        let path = assignments
            .iter()
            .any(|(column_name, _)| column_name == "path");
        let data = assignments
            .iter()
            .any(|(column_name, _)| column_name == "data");
        let descriptor = assignments
            .iter()
            .any(|(column_name, _)| column_name != "path" && column_name != "data");
        Self {
            path,
            data,
            descriptor,
        }
    }
}

fn reject_lix_file_update_plugin_storage_paths(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    update_columns: LixFileUpdateColumns,
) -> Result<()> {
    for row_index in 0..batch.num_rows() {
        if let Some(existing_path) = optional_string_value(batch, row_index, "path")? {
            let parsed = parse_file_upsert_path(&existing_path, TransactionWriteOperation::Update)?;
            if parsed.plugin_key.is_some() {
                if update_columns.path {
                    return Err(lix_error_to_datafusion_error(LixError::new(
                        LixError::CODE_CONSTRAINT_VIOLATION,
                        "UPDATE lix_file cannot modify plugin archive paths".to_string(),
                    )));
                }
                if !update_columns.data {
                    return Err(lix_error_to_datafusion_error(LixError::new(
                        LixError::CODE_CONSTRAINT_VIOLATION,
                        "UPDATE lix_file for plugin archive paths requires data".to_string(),
                    )));
                }
            }
        }
        if update_columns.path {
            let assigned_path =
                update_required_string_value(batch, assignment_values, row_index, "path")?;
            let parsed = parse_file_upsert_path(&assigned_path, TransactionWriteOperation::Update)?;
            if parsed.plugin_key.is_some() {
                return Err(lix_error_to_datafusion_error(LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    "UPDATE lix_file cannot move files into plugin archive paths".to_string(),
                )));
            }
        }
    }
    Ok(())
}

fn lix_file_update_stage_from_batch(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    branch_binding: Option<&str>,
    update_columns: LixFileUpdateColumns,
    blob_ref_keys: &BTreeSet<FilesystemBlobRefKey>,
    plugin_rewrite_file_ids: &BTreeSet<String>,
    path_resolvers: Option<&mut BTreeMap<String, DirectoryPathResolver>>,
    generate_directory_id: &mut dyn FnMut() -> String,
) -> Result<LixFileStagedBatch> {
    reject_lix_file_update_plugin_storage_paths(batch, assignment_values, update_columns)?;

    if update_columns.path || update_columns.descriptor {
        let Some(path_resolvers) = path_resolvers else {
            return Err(DataFusionError::Execution(
                "UPDATE lix_file requires filesystem path resolver".to_string(),
            ));
        };
        return if update_columns.path {
            lix_file_path_update_stage_from_batch(
                batch,
                assignment_values,
                branch_binding,
                update_columns,
                blob_ref_keys,
                plugin_rewrite_file_ids,
                path_resolvers,
                generate_directory_id,
            )
        } else {
            lix_file_existing_update_stage_from_batch(
                batch,
                assignment_values,
                branch_binding,
                update_columns.descriptor,
                update_columns.data,
                blob_ref_keys,
                Some(path_resolvers),
            )
        };
    }

    lix_file_existing_update_stage_from_batch(
        batch,
        assignment_values,
        branch_binding,
        update_columns.descriptor,
        update_columns.data,
        blob_ref_keys,
        None,
    )
}

fn lix_file_path_update_stage_from_batch(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    branch_binding: Option<&str>,
    update_columns: LixFileUpdateColumns,
    blob_ref_keys: &BTreeSet<FilesystemBlobRefKey>,
    plugin_rewrite_file_ids: &BTreeSet<String>,
    path_resolvers: &mut BTreeMap<String, DirectoryPathResolver>,
    generate_directory_id: &mut dyn FnMut() -> String,
) -> Result<LixFileStagedBatch> {
    let mut staged = LixFileStagedBatch::default();

    for row_index in 0..batch.num_rows() {
        let id = required_string_value(batch, row_index, "id")?;
        let path = update_required_string_value(batch, assignment_values, row_index, "path")?;
        let ParsedFileWritePath {
            path,
            filename,
            parsed_path,
            ..
        } = parse_file_upsert_path(&path, TransactionWriteOperation::Update)?;
        let context =
            file_row_context_from_update(batch, assignment_values, row_index, branch_binding)?;
        let assigned_data = if update_columns.data {
            Some(update_required_binary_value(
                batch,
                assignment_values,
                row_index,
                "data",
            )?)
        } else {
            None
        };

        let plan = plan_parsed_file_path_update_with_resolvers(
            path_resolvers,
            id.clone(),
            parsed_path,
            context.clone(),
            generate_directory_id,
        )
        .map_err(lix_error_to_datafusion_error)?;
        staged.extend_filesystem_plan(plan);

        if let Some(data) = assigned_data {
            let has_blob_ref =
                blob_ref_keys.contains(&FilesystemBlobRefKey::from_context(&context, &id));
            stage_lix_file_data_update_write(
                &mut staged,
                id.clone(),
                Some(path),
                Some(filename),
                data,
                context,
                has_blob_ref,
                None,
            )?;
        } else if plugin_rewrite_file_ids.contains(&id) {
            let data = required_binary_value(batch, row_index, "data")?;
            let has_blob_ref =
                blob_ref_keys.contains(&FilesystemBlobRefKey::from_context(&context, &id));
            stage_lix_file_data_update_write(
                &mut staged,
                id.clone(),
                Some(path),
                Some(filename),
                data,
                context,
                has_blob_ref,
                None,
            )?;
        }
    }

    Ok(staged)
}

fn path_update_plugin_rewrite_file_ids(
    plugin_render: Option<&PluginRenderContext>,
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    branch_binding: Option<&str>,
) -> Result<BTreeSet<String>> {
    let Some(plugin_render) = plugin_render else {
        return Ok(BTreeSet::new());
    };
    let mut file_ids = BTreeSet::new();
    for row_index in 0..batch.num_rows() {
        let file_id = required_string_value(batch, row_index, "id")?;
        let existing_path = required_string_value(batch, row_index, "path")?;
        let assigned_path =
            update_required_string_value(batch, assignment_values, row_index, "path")?;
        let assigned_path =
            parse_file_upsert_path(&assigned_path, TransactionWriteOperation::Update)?.path;
        if existing_path == assigned_path {
            continue;
        }

        let context =
            file_row_context_from_update(batch, assignment_values, row_index, branch_binding)?;
        let plugins = plugin_render.installed_plugins_for_branch(&context.branch_id);
        if plugins.is_empty() {
            continue;
        }
        let existing_plugin = select_plugin_for_path(plugins, &existing_path);
        let assigned_plugin = select_plugin_for_path(plugins, &assigned_path);
        let existing_plugin_key = existing_plugin.map(|plugin| plugin.key.as_str());
        let assigned_plugin_key = assigned_plugin.map(|plugin| plugin.key.as_str());
        if existing_plugin_key != assigned_plugin_key {
            file_ids.insert(file_id);
        }
    }
    Ok(file_ids)
}

#[cfg(test)]
fn lix_file_stage_from_batch_with_options(
    batch: &RecordBatch,
    branch_binding: Option<&str>,
    surface_name: &str,
    reject_read_only_fields: bool,
    include_descriptor_writes: bool,
    include_data_writes: bool,
) -> Result<LixFileStagedBatch> {
    lix_file_stage_from_batch_with_options_and_path_resolvers(
        batch,
        branch_binding,
        surface_name,
        reject_read_only_fields,
        include_descriptor_writes,
        include_data_writes,
        None,
        None,
    )
}

fn lix_file_stage_from_batch_with_options_and_path_resolvers(
    batch: &RecordBatch,
    branch_binding: Option<&str>,
    surface_name: &str,
    reject_read_only_fields: bool,
    include_descriptor_writes: bool,
    include_data_writes: bool,
    mut path_resolvers: Option<&mut BTreeMap<String, DirectoryPathResolver>>,
    mut generate_directory_id: Option<&mut dyn FnMut() -> String>,
) -> Result<LixFileStagedBatch> {
    let mut staged = LixFileStagedBatch::default();

    for row_index in 0..batch.num_rows() {
        if reject_read_only_fields {
            reject_read_only_lix_file_insert_field(batch, row_index, "lixcol_entity_pk")?;
            reject_read_only_lix_file_insert_field(batch, row_index, "lixcol_schema_key")?;
            reject_read_only_lix_file_insert_field(batch, row_index, "lixcol_change_id")?;
            reject_read_only_lix_file_insert_field(batch, row_index, "lixcol_created_at")?;
            reject_read_only_lix_file_insert_field(batch, row_index, "lixcol_updated_at")?;
            reject_read_only_lix_file_insert_field(batch, row_index, "lixcol_commit_id")?;
        }

        let path = optional_string_value(batch, row_index, "path")?;
        let id = optional_string_value(batch, row_index, "id")?;
        let context = file_row_context_from_batch(batch, row_index, branch_binding)?;
        let data = if include_data_writes {
            insert_optional_binary_value(batch, row_index, "data")?
        } else {
            None
        };

        if let Some(path) = path {
            let ParsedFileWritePath {
                parsed_path,
                plugin_key,
                ..
            } = parse_file_upsert_path(&path, TransactionWriteOperation::Insert)?;
            if plugin_key.is_some() && data.is_none() {
                return Err(lix_error_to_datafusion_error(LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    "INSERT into lix_file for plugin archive paths requires data".to_string(),
                )));
            }
            reject_read_only_lix_file_insert_field(batch, row_index, "directory_id")?;
            reject_read_only_lix_file_insert_field(batch, row_index, "name")?;

            let Some(path_resolvers) = path_resolvers.as_deref_mut() else {
                return Err(DataFusionError::Execution(
                    "INSERT into lix_file with path requires directory path resolver".to_string(),
                ));
            };
            let Some(generate_directory_id) = generate_directory_id.as_deref_mut() else {
                return Err(DataFusionError::Execution(
                    "INSERT into lix_file with path requires directory id generator".to_string(),
                ));
            };
            let file_id = id.unwrap_or_else(|| {
                plugin_key
                    .as_deref()
                    .map(plugin_storage_archive_file_id)
                    .unwrap_or_else(|| generate_directory_id())
            });
            let mut plan = plan_parsed_file_path_write_with_resolvers(
                path_resolvers,
                parsed_path,
                Some(file_id.clone()),
                data,
                context,
                generate_directory_id,
            )
            .map_err(lix_error_to_datafusion_error)?;
            attach_lix_file_insert_origin(&mut plan.rows, surface_name, &file_id);
            staged.extend_filesystem_plan(plan);
            continue;
        }

        let directory_id = optional_string_value(batch, row_index, "directory_id")?;
        let name = required_string_value(batch, row_index, "name")?;
        let mut data_path = None;

        let id = if data.is_some() {
            match id {
                Some(id) => Some(id),
                None => {
                    let Some(generate_id) = generate_directory_id.as_deref_mut() else {
                        return Err(DataFusionError::Execution(
                            "INSERT into lix_file with data requires id generator".to_string(),
                        ));
                    };
                    Some(generate_id())
                }
            }
        } else {
            id
        };

        if include_descriptor_writes && data.is_some() {
            if let (Some(file_id), Some(path_resolvers)) =
                (id.clone(), path_resolvers.as_deref_mut())
            {
                let resolver = path_resolvers
                    .entry(file_path_resolver_key(&context))
                    .or_insert_with(DirectoryPathResolver::default);
                let mut plan = plan_file_descriptor_write(
                    resolver,
                    FileDescriptorWriteInput {
                        id: Some(file_id.clone()),
                        directory_id,
                        name,
                        data,
                        context,
                    },
                    &mut || unreachable!("file id is supplied for descriptor insert data writes"),
                )
                .map_err(lix_error_to_datafusion_error)?;
                attach_lix_file_insert_origin(&mut plan.rows, surface_name, &file_id);
                staged.extend_filesystem_plan(plan);
                continue;
            }
        }

        if include_descriptor_writes {
            if let Some(path_resolvers) = path_resolvers.as_deref_mut() {
                if let Some(file_id) = id.as_ref() {
                    let resolver = path_resolvers
                        .entry(file_path_resolver_key(&context))
                        .or_insert_with(DirectoryPathResolver::default);
                    resolver
                        .reserve_file(directory_id.clone(), name.clone(), file_id.clone())
                        .map_err(lix_error_to_datafusion_error)?;
                    data_path = resolver
                        .file_path(directory_id.as_deref(), &name)
                        .map_err(lix_error_to_datafusion_error)?;
                }
            }
            let mut row = file_descriptor_write_row(FileDescriptorWriteIntent {
                id: id.clone(),
                directory_id: directory_id.clone(),
                name: name.clone(),
                context: context.clone(),
            });
            if let Some(file_id) = id.as_ref() {
                row.origin = Some(lix_file_insert_origin(surface_name, file_id));
            }
            staged.state_rows.push(row);
        }

        if let (Some(id), Some(data)) = (id, data) {
            let origin = Some(lix_file_insert_origin(surface_name, &id));
            let path = match data_path {
                Some(path) => Some(path),
                None if directory_id.is_none() => {
                    Some(compose_file_path(None, &name).map_err(lix_error_to_datafusion_error)?)
                }
                None => None,
            };
            stage_lix_file_data_insert_write(
                &mut staged,
                id,
                path,
                Some(name),
                data,
                context,
                origin,
            )?;
        }
        staged.count = staged
            .count
            .checked_add(1)
            .ok_or_else(|| DataFusionError::Execution("lix_file row count overflow".into()))?;
    }

    Ok(staged)
}

fn stage_lix_file_data_insert_write(
    staged: &mut LixFileStagedBatch,
    file_id: String,
    path: Option<String>,
    filename: Option<String>,
    data: Vec<u8>,
    context: FilesystemRowContext,
    origin: Option<TransactionWriteOrigin>,
) -> Result<()> {
    if !data.is_empty() {
        stage_lix_file_data_blob_ref_write(
            staged,
            file_id.clone(),
            data.clone(),
            &context,
            origin,
        )?;
    }
    stage_lix_file_data_payload_write(staged, file_id, path, filename, data, context);
    Ok(())
}

fn stage_lix_file_data_update_write(
    staged: &mut LixFileStagedBatch,
    file_id: String,
    path: Option<String>,
    filename: Option<String>,
    data: Vec<u8>,
    context: FilesystemRowContext,
    has_blob_ref: bool,
    origin: Option<TransactionWriteOrigin>,
) -> Result<()> {
    if data.is_empty() {
        if has_blob_ref {
            let mut row = blob_ref_tombstone_row(file_id.clone(), context.clone());
            row.origin = origin;
            staged.state_rows.push(row);
        }
        stage_lix_file_data_payload_write(staged, file_id, path, filename, data, context);
        return Ok(());
    }
    stage_lix_file_data_blob_ref_write(staged, file_id.clone(), data.clone(), &context, origin)?;
    stage_lix_file_data_payload_write(staged, file_id, path, filename, data, context);
    Ok(())
}

fn stage_lix_file_data_blob_ref_write(
    staged: &mut LixFileStagedBatch,
    file_id: String,
    data: Vec<u8>,
    context: &FilesystemRowContext,
    origin: Option<TransactionWriteOrigin>,
) -> Result<()> {
    let mut row = blob_ref_row(BlobRefRowInput {
        file_id,
        data,
        context: FilesystemRowContext {
            file_id: None,
            metadata: None,
            ..context.clone()
        },
    })
    .map_err(lix_error_to_datafusion_error)?;
    row.origin = origin;
    staged.state_rows.push(row);
    Ok(())
}

fn stage_lix_file_data_payload_write(
    staged: &mut LixFileStagedBatch,
    file_id: String,
    path: Option<String>,
    filename: Option<String>,
    data: Vec<u8>,
    context: FilesystemRowContext,
) {
    staged.file_data_writes.push(TransactionFileData {
        file_id,
        path,
        filename,
        branch_id: context.branch_id,
        global: context.global,
        untracked: context.untracked,
        data,
    });
}

fn attach_lix_file_insert_origin(
    rows: &mut [TransactionWriteRow],
    surface_name: &str,
    file_id: &str,
) {
    let origin = lix_file_insert_origin(surface_name, file_id);
    for row in rows {
        if row.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY || row.schema_key == BLOB_REF_SCHEMA_KEY {
            row.origin = Some(origin.clone());
        }
    }
}

fn lix_file_insert_origin(surface_name: &str, file_id: &str) -> TransactionWriteOrigin {
    TransactionWriteOrigin {
        surface: surface_name.to_string(),
        operation: TransactionWriteOperation::Insert,
        primary_key: Some(LogicalPrimaryKey {
            columns: vec!["id".to_string()],
            values: vec![file_id.to_string()],
        }),
    }
}

fn file_row_context_from_batch(
    batch: &RecordBatch,
    row_index: usize,
    branch_binding: Option<&str>,
) -> Result<FilesystemRowContext> {
    let explicit_branch_id = optional_string_value(batch, row_index, "lixcol_branch_id")?;
    let scope = resolve_write_branch_scope(
        optional_bool_value(batch, row_index, "lixcol_global")?,
        explicit_branch_id,
        branch_binding,
        "INSERT into lix_file_by_branch",
        "lix_file",
    )?;

    Ok(FilesystemRowContext {
        branch_id: scope.branch_id,
        global: scope.global,
        untracked: optional_bool_value(batch, row_index, "lixcol_untracked")?.unwrap_or(false),
        file_id: optional_string_value(batch, row_index, "lixcol_file_id")?,
        metadata: optional_metadata_value(batch, row_index, "lixcol_metadata", "lix_file")?,
    })
}

fn file_row_context_from_update(
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
        "UPDATE into lix_file_by_branch",
        "lix_file",
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
            "lix_file",
        )?,
    })
}

fn file_path_resolver_key(context: &FilesystemRowContext) -> String {
    filesystem_storage_scope_key(
        &context.branch_id,
        context.global,
        context.untracked,
        context.file_id.as_deref(),
    )
}

async fn lix_file_record_batch(
    schema: &SchemaRef,
    blob_reader: &Arc<dyn BlobDataReader>,
    plugin_render: Option<PluginRenderContext>,
    load_data: bool,
    rows: Vec<MaterializedLiveStateRow>,
) -> Result<RecordBatch, LixError> {
    let projected_columns = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    let needs_data = load_data && projected_columns.contains(&"data");

    let mut file_rows = BTreeMap::<FilesystemDescriptorKey, FileDescriptorRecord>::new();
    let mut blob_rows = BTreeMap::<FilesystemBlobRefKey, BlobRefRecord>::new();
    let mut directory_rows = Vec::<DirectoryDescriptorRecord>::new();

    for row in rows {
        match row.schema_key.as_str() {
            FILE_DESCRIPTOR_SCHEMA_KEY => {
                let Some(snapshot_content) = row.snapshot_content.as_deref() else {
                    continue;
                };
                let snapshot: FileDescriptorSnapshot = serde_json::from_str(snapshot_content)
                    .map_err(|error| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!("invalid lix_file_descriptor snapshot JSON: {error}"),
                        )
                    })?;
                let key = FilesystemDescriptorKey::from_live_row(&row, snapshot.id.clone());
                file_rows.insert(
                    key.clone(),
                    FileDescriptorRecord {
                        id: snapshot.id,
                        directory_id: snapshot.directory_id,
                        name: snapshot.name,
                        key,
                        live: row,
                    },
                );
            }
            BLOB_REF_SCHEMA_KEY => {
                let Some(snapshot_content) = row.snapshot_content.as_deref() else {
                    continue;
                };
                let snapshot: BlobRefSnapshot =
                    serde_json::from_str(snapshot_content).map_err(|error| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!("invalid lix_binary_blob_ref snapshot JSON: {error}"),
                        )
                    })?;
                blob_rows.insert(
                    FilesystemBlobRefKey::from_live_row(&row, snapshot.id),
                    BlobRefRecord {
                        blob_hash: snapshot.blob_hash,
                    },
                );
            }
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY => {
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
                directory_rows.push(DirectoryDescriptorRecord {
                    key: FilesystemDescriptorKey::from_live_row(&row, snapshot.id.clone()),
                    parent_id: snapshot.parent_id,
                    name: snapshot.name,
                });
            }
            _ => {}
        }
    }

    let directory_paths =
        derive_directory_paths(directory_rows.iter().map(|row| (row.key.clone(), row)))?;
    let mut ids = Vec::new();
    let mut paths = Vec::new();
    let mut directory_ids = Vec::new();
    let mut names = Vec::new();
    let mut data_values = Vec::new();
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

    for (_, file) in file_rows {
        let directory_path = match file.directory_id.as_ref() {
            Some(directory_id) => {
                let parent_key = file
                    .directory_parent_keys(directory_id)
                    .into_iter()
                    .find(|key| directory_paths.contains_key(key));
                let Some(path) = parent_key
                    .as_ref()
                    .and_then(|key| directory_paths.get(key))
                    .cloned()
                else {
                    return Err(LixError::new(
                        LixError::CODE_FOREIGN_KEY,
                        format!(
                            "lix_file_descriptor '{}' references missing directory_id '{}' in branch '{}'",
                            file.id, directory_id, file.live.branch_id
                        ),
                    ));
                };
                Some(path)
            }
            None => None,
        };
        let path = compose_file_path(directory_path.as_deref(), &file.name)?;
        let data = if needs_data {
            let context = FilesystemRowContext {
                branch_id: file.live.branch_id.clone(),
                global: file.live.global,
                untracked: file.live.untracked,
                file_id: file.live.file_id.clone(),
                metadata: None,
            };
            match blob_rows.get(&FilesystemBlobRefKey::from_context(&context, &file.id)) {
                Some(blob_ref) => load_single_blob_bytes(blob_reader, &blob_ref.blob_hash).await?,
                None => {
                    let rendered = match &plugin_render {
                        Some(plugin_render) => {
                            render_plugin_file_for_sql(plugin_render, &file, &path).await?
                        }
                        None => None,
                    };
                    Some(rendered.unwrap_or_default())
                }
            }
        } else {
            Some(Vec::new())
        };

        ids.push(Some(file.id));
        paths.push(Some(path));
        directory_ids.push(file.directory_id);
        names.push(Some(file.name));
        data_values.push(data);
        entity_pks.push(Some(file.live.entity_pk.as_json_array_text()?));
        schema_keys.push(Some(file.live.schema_key));
        file_ids.push(file.live.file_id);
        globals.push(Some(file.live.global));
        change_ids.push(file.live.change_id.map(|id| id.to_string()));
        created_ats.push(file.live.created_at);
        updated_ats.push(file.live.updated_at);
        commit_ids.push(file.live.commit_id.map(|id| id.to_string()));
        untracked_values.push(Some(file.live.untracked));
        metadata_values.push(file.live.metadata.as_deref().map(serialize_row_metadata));
        branch_ids.push(Some(file.live.branch_id));
    }

    let mut columns = Vec::<ArrayRef>::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let array: ArrayRef = match field.name().as_str() {
            "id" => Arc::new(StringArray::from(ids.clone())),
            "path" => Arc::new(StringArray::from(paths.clone())),
            "directory_id" => Arc::new(StringArray::from(directory_ids.clone())),
            "name" => Arc::new(StringArray::from(names.clone())),
            "data" => Arc::new(BinaryArray::from(
                data_values
                    .iter()
                    .map(|value| value.as_deref())
                    .collect::<Vec<_>>(),
            )),
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
                    format!("sql2 lix_file provider does not support projected column '{other}'"),
                ));
            }
        };
        columns.push(array);
    }

    let options = RecordBatchOptions::new().with_row_count(Some(ids.len()));
    RecordBatch::try_new_with_options(Arc::clone(schema), columns, &options).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("sql2 failed to build lix_file record batch: {error}"),
        )
    })
}

async fn render_plugin_file_for_sql(
    plugin_render: &PluginRenderContext,
    file: &FileDescriptorRecord,
    path: &str,
) -> Result<Option<Vec<u8>>, LixError> {
    let installed_plugins = plugin_render.installed_plugins_for_branch(&file.live.branch_id);
    let Some(plugin) = select_plugin_for_path(installed_plugins, path) else {
        return Ok(None);
    };
    let rows = plugin_render
        .live_state
        .scan_file_rows(&LiveStateFileScanRequest {
            branch_ids: vec![file.live.branch_id.clone()],
            file_id: file.id.clone(),
            schema_keys: plugin.schema_keys.clone(),
            projection: plugin_state_live_state_projection(),
            ..Default::default()
        })
        .await?;
    let active_state = retain_plugin_state_rows(plugin, rows);
    render_materialized_plugin_file(&plugin_render.host, plugin, &active_state).await
}

async fn load_installed_plugins_for_lix_file_scan(
    live_state: Arc<dyn LiveStateReader>,
    blob_reader: &Arc<dyn BlobDataReader>,
    request: &LiveStateScanRequest,
) -> Result<BTreeMap<String, Vec<InstalledPlugin>>, LixError> {
    let mut plugin_request = request.clone();
    plugin_request.filter.schema_keys = vec![
        FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
        BLOB_REF_SCHEMA_KEY.to_string(),
        DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
    ];
    plugin_request.filter.entity_pks.clear();
    plugin_request.filter.file_ids.clear();
    plugin_request.projection = LiveStateProjection::default();
    plugin_request.limit = None;

    let rows = live_state.scan_rows(&plugin_request).await?;
    let mut rows_by_branch = BTreeMap::<String, Vec<MaterializedLiveStateRow>>::new();
    for row in rows {
        rows_by_branch
            .entry(row.branch_id.clone())
            .or_default()
            .push(row);
    }

    let mut installed_plugins_by_branch = BTreeMap::new();
    for (branch_id, rows) in rows_by_branch {
        let filesystem = FilesystemIndex::from_live_rows(rows)?;
        let installed_plugins =
            load_installed_plugins_from_filesystem(&filesystem, blob_reader.as_ref()).await?;
        installed_plugins_by_branch.insert(branch_id, installed_plugins);
    }
    Ok(installed_plugins_by_branch)
}

async fn plugin_render_context_for_lix_file_scan(
    live_state: Arc<dyn LiveStateReader>,
    blob_reader: &Arc<dyn BlobDataReader>,
    request: &LiveStateScanRequest,
    host: PluginRuntimeHost,
    needs_data: bool,
) -> Result<Option<PluginRenderContext>, LixError> {
    if !needs_data {
        return Ok(None);
    }

    let installed_plugins_by_branch =
        load_installed_plugins_for_lix_file_scan(Arc::clone(&live_state), blob_reader, request)
            .await?;
    Ok(Some(PluginRenderContext {
        live_state,
        host,
        installed_plugins_by_branch,
    }))
}

pub(super) async fn load_single_blob_bytes(
    blob_reader: &Arc<dyn BlobDataReader>,
    blob_hash: &str,
) -> Result<Option<Vec<u8>>, LixError> {
    let hash = BlobHash::from_hex(blob_hash)?;
    Ok(blob_reader
        .load_bytes_many(&[hash])
        .await?
        .into_vec()
        .into_iter()
        .next()
        .flatten())
}

fn projected_schema(base_schema: &SchemaRef, projection: Option<&Vec<usize>>) -> Result<SchemaRef> {
    let fields = match projection {
        Some(indices) => indices
            .iter()
            .map(|index| base_schema.field(*index).as_ref().clone())
            .collect::<Vec<_>>(),
        None => base_schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>(),
    };
    Ok(Arc::new(Schema::new(fields)))
}

fn scan_needs_data(
    base_schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
) -> bool {
    let projected_needs_data = match projection {
        Some(indices) => indices
            .iter()
            .any(|index| base_schema.field(*index).name() == "data"),
        None => true,
    };
    projected_needs_data || filters.iter().any(|filter| contains_column(filter, "data"))
}

fn lix_file_scan_request(
    branch_binding: Option<&str>,
    projected_schema: Option<&Schema>,
    limit: Option<usize>,
) -> LiveStateScanRequest {
    LiveStateScanRequest {
        filter: LiveStateFilter {
            schema_keys: vec![
                FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                BLOB_REF_SCHEMA_KEY.to_string(),
                DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
            ],
            branch_ids: branch_binding
                .map(|branch_id| vec![branch_id.to_string()])
                .unwrap_or_default(),
            ..LiveStateFilter::default()
        },
        projection: lix_file_live_state_projection(projected_schema),
        limit,
    }
}

fn lix_file_live_state_projection(projected_schema: Option<&Schema>) -> LiveStateProjection {
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

async fn scan_lix_file_live_rows(
    live_state: Arc<dyn LiveStateReader>,
    request: &LiveStateScanRequest,
    target_file_ids: &FileIdConstraint,
) -> std::result::Result<Vec<MaterializedLiveStateRow>, LixError> {
    let target_file_ids = match target_file_ids {
        FileIdConstraint::All => return live_state.scan_rows(request).await,
        FileIdConstraint::None => return Ok(Vec::new()),
        FileIdConstraint::Ids(target_file_ids) => target_file_ids,
    };

    let mut file_request = request.clone();
    file_request.filter.schema_keys = vec![
        FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
        BLOB_REF_SCHEMA_KEY.to_string(),
    ];
    file_request.filter.entity_pks = target_file_ids
        .iter()
        .map(|file_id| EntityPk::single(file_id.clone()))
        .collect();

    let mut rows = live_state.scan_rows(&file_request).await?;

    let mut directory_request = request.clone();
    directory_request.filter.schema_keys = vec![DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string()];
    directory_request.filter.entity_pks.clear();
    directory_request.limit = None;
    rows.extend(live_state.scan_rows(&directory_request).await?);

    Ok(rows)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FileIdConstraint {
    All,
    None,
    Ids(BTreeSet<String>),
}

impl FileIdConstraint {
    fn from_ids(ids: Vec<String>) -> Self {
        let ids = ids.into_iter().collect::<BTreeSet<_>>();
        if ids.is_empty() {
            Self::None
        } else {
            Self::Ids(ids)
        }
    }

    fn intersect(self, other: Self) -> Self {
        match (self, other) {
            (Self::None, _) | (_, Self::None) => Self::None,
            (Self::All, constraint) | (constraint, Self::All) => constraint,
            (Self::Ids(left), Self::Ids(right)) => {
                let ids = left.intersection(&right).cloned().collect::<BTreeSet<_>>();
                if ids.is_empty() {
                    Self::None
                } else {
                    Self::Ids(ids)
                }
            }
        }
    }

    fn union(self, other: Self) -> Self {
        match (self, other) {
            (Self::All, _) | (_, Self::All) => Self::All,
            (Self::None, constraint) | (constraint, Self::None) => constraint,
            (Self::Ids(mut left), Self::Ids(right)) => {
                left.extend(right);
                Self::Ids(left)
            }
        }
    }
}

fn file_id_constraint_from_filters(filters: &[Expr]) -> Result<FileIdConstraint> {
    let analyzer = LixFileIdFilterAnalyzer;
    let mut constraint = FileIdConstraint::All;
    for filter in filters {
        if let Some(filter_constraint) = analyzer.analyze(filter)? {
            constraint = constraint.intersect(filter_constraint);
        }
    }
    Ok(constraint)
}

struct LixFileIdFilterAnalyzer;

impl LixFileIdFilterAnalyzer {
    fn supports(&self, expr: &Expr) -> bool {
        self.analyze(expr)
            .is_ok_and(|constraint| constraint.is_some())
    }

    fn analyze(&self, expr: &Expr) -> Result<Option<FileIdConstraint>> {
        ExactStringColumnFilterAnalyzer::new("id").analyze(expr)
    }
}

struct ExactStringColumnFilterAnalyzer {
    column_name: &'static str,
}

impl ExactStringColumnFilterAnalyzer {
    fn new(column_name: &'static str) -> Self {
        Self { column_name }
    }

    fn supports(&self, expr: &Expr) -> bool {
        self.analyze(expr)
            .is_ok_and(|constraint| constraint.is_some())
    }

    fn analyze(&self, expr: &Expr) -> Result<Option<FileIdConstraint>> {
        match expr {
            Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::And => {
                let Some(left) = self.analyze(&binary_expr.left)? else {
                    return Ok(None);
                };
                let Some(right) = self.analyze(&binary_expr.right)? else {
                    return Ok(None);
                };
                Ok(Some(left.intersect(right)))
            }
            Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::Or => {
                let Some(left) = self.analyze(&binary_expr.left)? else {
                    return Ok(None);
                };
                let Some(right) = self.analyze(&binary_expr.right)? else {
                    return Ok(None);
                };
                Ok(Some(left.union(right)))
            }
            Expr::BinaryExpr(binary_expr) => Ok(self
                .value_from_binary_filter(binary_expr)
                .map(|value| FileIdConstraint::Ids(BTreeSet::from([value])))),
            Expr::InList(in_list) => Ok(self
                .values_from_in_list_filter(in_list)
                .map(FileIdConstraint::from_ids)),
            _ => Ok(None),
        }
    }

    fn value_from_binary_filter(&self, binary_expr: &BinaryExpr) -> Option<String> {
        if binary_expr.op != Operator::Eq {
            return None;
        }
        self.value_from_column_literal_filter(&binary_expr.left, &binary_expr.right)
            .or_else(|| {
                self.value_from_column_literal_filter(&binary_expr.right, &binary_expr.left)
            })
    }

    fn values_from_in_list_filter(&self, in_list: &InList) -> Option<Vec<String>> {
        if in_list.negated {
            return None;
        }
        let Expr::Column(column) = in_list.expr.as_ref() else {
            return None;
        };
        if column.name != self.column_name {
            return None;
        }
        let values = in_list
            .list
            .iter()
            .map(string_expr_literal)
            .collect::<Option<Vec<_>>>()?;
        Some(values)
    }

    fn value_from_column_literal_filter(
        &self,
        column_expr: &Expr,
        literal_expr: &Expr,
    ) -> Option<String> {
        let Expr::Column(column) = column_expr else {
            return None;
        };
        if column.name != self.column_name {
            return None;
        }
        string_expr_literal(literal_expr)
    }
}

fn string_expr_literal(expr: &Expr) -> Option<String> {
    let Expr::Literal(literal, _) = expr else {
        return None;
    };
    match literal {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Some(value.clone()),
        _ => None,
    }
}

fn contains_column(expr: &Expr, column_name: &str) -> bool {
    expr.column_refs()
        .iter()
        .any(|column| column.name.as_str() == column_name)
}

/// Whether a physical assignment expression references `column_name`. Mirrors
/// the logical `contains_column` check `main` applied to UPDATE assignment
/// right-hand sides before physical conversion (the framework hands `plan_update`
/// the already-compiled physical assignments).
fn physical_expr_contains_column(expr: &Arc<dyn PhysicalExpr>, column_name: &str) -> bool {
    if let Some(column) = expr
        .as_any()
        .downcast_ref::<datafusion::physical_expr::expressions::Column>()
    {
        if column.name() == column_name {
            return true;
        }
    }
    expr.children()
        .into_iter()
        .any(|child| physical_expr_contains_column(child, column_name))
}

fn parse_normal_write_file_path(
    path: &str,
    operation: TransactionWriteOperation,
) -> Result<String> {
    LixPath::try_from_file_path(path).map_err(lix_error_to_datafusion_error)?;
    reject_normal_plugin_storage_mutation(path, lix_file_write_operation_label(operation))
        .map_err(lix_error_to_datafusion_error)?;
    Ok(path.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedFileWritePath {
    path: String,
    filename: String,
    parsed_path: LixPath,
    plugin_key: Option<String>,
}

fn parse_file_upsert_path(
    path: &str,
    operation: TransactionWriteOperation,
) -> Result<ParsedFileWritePath> {
    let parsed = LixPath::try_from_file_path(path).map_err(lix_error_to_datafusion_error)?;
    let filename = parsed
        .segments()
        .last()
        .expect("parsed file path should have a leaf segment")
        .to_string();
    let path = path.to_string();
    let plugin_key = if is_plugin_storage_path(&path) {
        Some(plugin_key_from_archive_path(&path).ok_or_else(|| {
            lix_error_to_datafusion_error(LixError::new(
                LixError::CODE_CONSTRAINT_VIOLATION,
                format!(
                    "{} cannot modify reserved plugin storage path {:?}",
                    lix_file_write_operation_label(operation),
                    path
                ),
            ))
        })?)
    } else {
        None
    };
    Ok(ParsedFileWritePath {
        path,
        filename,
        parsed_path: parsed,
        plugin_key,
    })
}

fn lix_file_write_operation_label(operation: TransactionWriteOperation) -> &'static str {
    match operation {
        TransactionWriteOperation::Insert => "INSERT into lix_file",
        TransactionWriteOperation::Update => "UPDATE lix_file",
        TransactionWriteOperation::Delete => "DELETE FROM lix_file",
    }
}

fn validate_lix_file_update_assignments(
    schema: &SchemaRef,
    assignments: &[(String, Expr)],
) -> Result<()> {
    for (column_name, expr) in assignments {
        schema.field_with_name(column_name).map_err(|_| {
            DataFusionError::Plan(format!(
                "UPDATE lix_file failed: column '{column_name}' does not exist"
            ))
        })?;
        if !matches!(
            column_name.as_str(),
            "path" | "directory_id" | "name" | "data" | "lixcol_metadata"
        ) {
            return Err(DataFusionError::Execution(format!(
                "UPDATE lix_file cannot stage read-only column '{column_name}'"
            )));
        }
        if column_name == "data" {
            reject_non_binary_lix_file_data_assignment(expr)?;
        }
    }
    Ok(())
}

fn reject_non_binary_lix_file_data_assignment(expr: &Expr) -> Result<()> {
    match expr {
        Expr::Literal(value, _) => {
            if !scalar_is_binary_or_null(value) {
                return Err(non_binary_lix_file_data_assignment_error());
            }
        }
        Expr::Cast(cast)
            if is_binary_type(&cast.data_type) && !logical_expr_is_binary_or_null(&cast.expr) =>
        {
            return Err(non_binary_lix_file_data_assignment_error());
        }
        _ => {}
    }

    Ok(())
}

fn non_binary_lix_file_data_assignment_error() -> DataFusionError {
    lix_file_data_type_error(
        "UPDATE lix_file",
        "data",
        "use X'...' or a binary parameter for file contents",
    )
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

fn reject_read_only_lix_file_insert_field(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<()> {
    if optional_scalar_value(batch, row_index, column_name)?.is_some_and(|value| !value.is_null()) {
        return Err(DataFusionError::Execution(format!(
            "INSERT into lix_file cannot stage read-only column '{column_name}'"
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
            "INSERT into lix_file requires non-null text column '{column_name}'"
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
                "UPDATE lix_file requires non-null text column '{column_name}'"
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
            "UPDATE lix_file expected text-compatible column '{column_name}', got {other:?}"
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

fn update_required_binary_value(
    _batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    row_index: usize,
    column_name: &str,
) -> Result<Vec<u8>> {
    match assignment_values.assigned_cell(row_index, column_name)? {
        UpdateCell::Unassigned | UpdateCell::Assigned(SqlCell::Null) => {
            Err(lix_file_data_type_error(
                "UPDATE lix_file",
                column_name,
                "use X'' for an empty file or omit data to leave contents unchanged",
            ))
        }
        UpdateCell::Assigned(SqlCell::Value(
            ScalarValue::Binary(Some(value)) | ScalarValue::LargeBinary(Some(value)),
        )) => Ok(value),
        UpdateCell::Assigned(SqlCell::Value(ScalarValue::FixedSizeBinary(_, Some(value)))) => {
            Ok(value)
        }
        UpdateCell::Assigned(SqlCell::Value(other)) => Err(lix_file_data_type_error_with_value(
            "UPDATE lix_file",
            column_name,
            &other,
            "use X'...' or a binary parameter for file contents",
        )),
    }
}

fn required_binary_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<Vec<u8>> {
    match optional_scalar_value(batch, row_index, column_name)? {
        Some(ScalarValue::Binary(Some(value)) | ScalarValue::LargeBinary(Some(value))) => Ok(value),
        Some(ScalarValue::FixedSizeBinary(_, Some(value))) => Ok(value),
        Some(other) => Err(lix_file_data_type_error_with_value(
            "UPDATE lix_file",
            column_name,
            &other,
            "expected materialized binary file contents",
        )),
        None => Err(DataFusionError::Execution(format!(
            "UPDATE lix_file requires materialized column '{column_name}'"
        ))),
    }
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
            "INSERT into lix_file expected text-compatible column '{column_name}', got {other:?}"
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
            "INSERT into lix_file expected boolean column '{column_name}', got {other:?}"
        ))),
    }
}

fn insert_optional_binary_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<Option<Vec<u8>>> {
    match optional_scalar_value(batch, row_index, column_name)? {
        None => Ok(None),
        Some(
            ScalarValue::Null
            | ScalarValue::Binary(None)
            | ScalarValue::LargeBinary(None)
            | ScalarValue::FixedSizeBinary(_, None),
        ) => Err(lix_file_data_type_error(
            "INSERT into lix_file",
            column_name,
            "use X'' for an empty file or omit data to create an empty file",
        )),
        Some(ScalarValue::Binary(Some(value)) | ScalarValue::LargeBinary(Some(value))) => {
            Ok(Some(value))
        }
        Some(ScalarValue::FixedSizeBinary(_, Some(value))) => Ok(Some(value)),
        Some(other) => Err(lix_file_data_type_error_with_value(
            "INSERT into lix_file",
            column_name,
            &other,
            "use X'...' or a binary parameter for file contents",
        )),
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
            "row index {row_index} out of bounds for lix_file batch with {} rows",
            batch.num_rows()
        )));
    }
    ScalarValue::try_from_array(batch.column(column_index).as_ref(), row_index)
        .map(Some)
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "failed to decode lix_file column '{column_name}' at row {row_index}: {error}"
            ))
        })
}

pub(super) fn lix_file_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("path", DataType::Utf8, false),
        Field::new("directory_id", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, false),
        Field::new("data", DataType::Binary, false),
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

pub(super) fn lix_file_by_branch_schema() -> SchemaRef {
    let mut fields = lix_file_schema()
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
    use std::io::{Cursor, Write};
    use std::sync::Arc;

    use async_trait::async_trait;
    use datafusion::arrow::array::{Array, ArrayRef, BinaryArray, BooleanArray, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::{Column, ScalarValue};
    use datafusion::execution::TaskContext;
    use datafusion::logical_expr::expr::{Cast, InList, ScalarFunction};
    use datafusion::logical_expr::lit;
    use datafusion::logical_expr::{
        BinaryExpr, ColumnarValue, Expr, Operator, Volatility, create_udf,
    };
    use serde_json::Value as JsonValue;

    use crate::LixError;
    use crate::binary_cas::{BlobBytesBatch, BlobDataReader, BlobHash};
    use crate::changelog::{ChangeId, CommitId};
    use crate::filesystem::{FilesystemBlobRefKey, FilesystemDescriptorKey, FilesystemRowContext};
    use crate::functions::FunctionProviderHandle;
    use crate::live_state::{LiveStateFilter, MaterializedLiveStateRow};
    use crate::live_state::{LiveStateReader, LiveStateRowRequest, LiveStateScanRequest};
    use crate::sql2::dml::InsertSink;
    use crate::sql2::{SqlWriteContext, SqlWriteExecutionContext};
    use crate::transaction::types::{
        TransactionJson, TransactionWrite, TransactionWriteMode, TransactionWriteOutcome,
    };

    use super::{
        BranchBinding, DirectoryDescriptorRecord, LixFileInsertSink, derive_directory_paths,
        lix_file_delete_stage_from_batch, lix_file_insert_stage_from_batch,
        lix_file_insert_stage_from_batch_with_path_resolvers, lix_file_write_rows_from_batch,
    };

    fn test_id_generator(ids: &'static [&'static str]) -> impl FnMut() -> String {
        let mut ids = ids.iter();
        move || ids.next().expect("test id should exist").to_string()
    }

    fn test_functions() -> FunctionProviderHandle {
        FunctionProviderHandle::system()
    }

    fn string_literal(value: &str) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some(value.to_string())), None)
    }

    fn column(name: &str) -> Expr {
        Expr::Column(Column::from_name(name))
    }

    fn eq_filter(column_name: &str, value: &str) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(column(column_name)),
            Operator::Eq,
            Box::new(string_literal(value)),
        ))
    }

    #[test]
    fn file_id_filters_support_string_id_predicates() {
        let analyzer = super::LixFileIdFilterAnalyzer;
        let constraint = analyzer
            .analyze(&Expr::InList(InList::new(
                Box::new(column("id")),
                vec![string_literal("file-b"), string_literal("file-a")],
                false,
            )))
            .unwrap()
            .unwrap();

        assert_eq!(
            constraint,
            super::FileIdConstraint::Ids(BTreeSet::from([
                "file-a".to_string(),
                "file-b".to_string()
            ]))
        );
        assert!(analyzer.supports(&eq_filter("id", "file-a")));
        assert!(analyzer.supports(&Expr::BinaryExpr(BinaryExpr::new(
            Box::new(string_literal("file-a")),
            Operator::Eq,
            Box::new(column("id")),
        ))));
    }

    #[test]
    fn file_id_filters_intersect_and_union_boolean_predicates() {
        let analyzer = super::LixFileIdFilterAnalyzer;
        let left = Expr::InList(InList::new(
            Box::new(column("id")),
            vec![string_literal("file-a"), string_literal("file-b")],
            false,
        ));
        let right = Expr::InList(InList::new(
            Box::new(column("id")),
            vec![string_literal("file-b"), string_literal("file-c")],
            false,
        ));

        let and_constraint = analyzer
            .analyze(&Expr::BinaryExpr(BinaryExpr::new(
                Box::new(left.clone()),
                Operator::And,
                Box::new(right.clone()),
            )))
            .unwrap()
            .unwrap();
        assert_eq!(
            and_constraint,
            super::FileIdConstraint::Ids(BTreeSet::from(["file-b".to_string()]))
        );

        let or_constraint = analyzer
            .analyze(&Expr::BinaryExpr(BinaryExpr::new(
                Box::new(left),
                Operator::Or,
                Box::new(right),
            )))
            .unwrap()
            .unwrap();
        assert_eq!(
            or_constraint,
            super::FileIdConstraint::Ids(BTreeSet::from([
                "file-a".to_string(),
                "file-b".to_string(),
                "file-c".to_string()
            ]))
        );
    }

    #[test]
    fn file_id_filters_detect_contradictions() {
        let filters = vec![Expr::BinaryExpr(BinaryExpr::new(
            Box::new(eq_filter("id", "file-a")),
            Operator::And,
            Box::new(eq_filter("id", "file-b")),
        ))];

        assert_eq!(
            super::file_id_constraint_from_filters(&filters).unwrap(),
            super::FileIdConstraint::None
        );
    }

    #[test]
    fn file_id_filters_ignore_non_id_and_negated_predicates() {
        let analyzer = super::LixFileIdFilterAnalyzer;

        assert!(!analyzer.supports(&eq_filter("name", "readme.md")));
        assert!(!analyzer.supports(&Expr::InList(InList::new(
            Box::new(column("id")),
            vec![string_literal("file-a")],
            true,
        ))));
    }

    #[test]
    fn contains_column_finds_nested_cast_and_function_references() {
        let cast_data = Expr::Cast(Cast::new(Box::new(column("data")), DataType::Utf8));
        let function_data = scalar_function_expr("some_fn", vec![cast_data.clone()]);

        assert!(super::contains_column(&cast_data, "data"));
        assert!(super::contains_column(&function_data, "data"));
        assert!(!super::contains_column(&function_data, "path"));
    }

    #[test]
    fn scan_needs_data_finds_data_inside_filter_functions() {
        let schema = super::lix_file_schema();
        let projection = vec![schema.index_of("id").expect("id column")];
        let filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(scalar_function_expr("octet_length", vec![column("data")])),
            Operator::Gt,
            Box::new(Expr::Literal(ScalarValue::Int64(Some(0)), None)),
        ));

        assert!(super::scan_needs_data(
            &schema,
            Some(&projection),
            &[filter]
        ));
    }

    fn scalar_function_expr(name: &str, args: Vec<Expr>) -> Expr {
        let udf = create_udf(
            name,
            vec![DataType::Binary],
            DataType::Int64,
            Volatility::Immutable,
            Arc::new(|_: &[ColumnarValue]| Ok(ColumnarValue::Scalar(ScalarValue::Null))),
        );
        Expr::ScalarFunction(ScalarFunction::new_udf(Arc::new(udf), args))
    }

    fn lix_file_update_stage_from_batch_for_test(
        batch: &RecordBatch,
        branch_binding: Option<&str>,
        update_columns: super::LixFileUpdateColumns,
        path_resolvers: Option<&mut BTreeMap<String, super::DirectoryPathResolver>>,
        generate_directory_id: &mut dyn FnMut() -> String,
    ) -> datafusion::common::Result<super::LixFileStagedBatch> {
        lix_file_update_stage_from_batch_with_blob_keys_for_test(
            batch,
            branch_binding,
            update_columns,
            path_resolvers,
            generate_directory_id,
            &BTreeSet::from([blob_ref_key("branch-b", false, false, "file-readme")]),
        )
    }

    fn lix_file_update_stage_from_batch_with_blob_keys_for_test(
        batch: &RecordBatch,
        branch_binding: Option<&str>,
        update_columns: super::LixFileUpdateColumns,
        path_resolvers: Option<&mut BTreeMap<String, super::DirectoryPathResolver>>,
        generate_directory_id: &mut dyn FnMut() -> String,
        blob_ref_keys: &BTreeSet<FilesystemBlobRefKey>,
    ) -> datafusion::common::Result<super::LixFileStagedBatch> {
        let mut columns = Vec::new();
        if update_columns.path {
            columns.push("path");
        }
        if update_columns.data {
            columns.push("data");
        }
        if update_columns.descriptor {
            columns.extend(["directory_id", "name"]);
        }
        let assignment_values = super::UpdateAssignmentValues::from_batch_columns(batch, &columns);
        super::lix_file_update_stage_from_batch(
            batch,
            &assignment_values,
            branch_binding,
            update_columns,
            blob_ref_keys,
            &BTreeSet::new(),
            path_resolvers,
            generate_directory_id,
        )
    }

    fn blob_ref_key(
        branch_id: &str,
        global: bool,
        untracked: bool,
        file_id: &str,
    ) -> FilesystemBlobRefKey {
        FilesystemBlobRefKey::from_context(
            &FilesystemRowContext {
                branch_id: branch_id.to_string(),
                global,
                untracked,
                file_id: None,
                metadata: None,
            },
            file_id,
        )
    }

    #[derive(Default)]
    struct CapturingWriteContext {
        rows: Vec<MaterializedLiveStateRow>,
        writes: Vec<TransactionWrite>,
    }

    struct StaticBlobReader {
        bytes_by_hash: BTreeMap<BlobHash, Vec<u8>>,
    }

    impl StaticBlobReader {
        fn from_blobs(blobs: impl IntoIterator<Item = Vec<u8>>) -> Self {
            Self {
                bytes_by_hash: blobs
                    .into_iter()
                    .map(|bytes| (BlobHash::from_content(&bytes), bytes))
                    .collect(),
            }
        }
    }

    #[async_trait]
    impl BlobDataReader for CapturingWriteContext {
        async fn load_bytes_many(&self, hashes: &[BlobHash]) -> Result<BlobBytesBatch, LixError> {
            Ok(BlobBytesBatch::new(vec![None; hashes.len()]))
        }
    }

    #[async_trait]
    impl BlobDataReader for StaticBlobReader {
        async fn load_bytes_many(&self, hashes: &[BlobHash]) -> Result<BlobBytesBatch, LixError> {
            Ok(BlobBytesBatch::new(
                hashes
                    .iter()
                    .map(|hash| self.bytes_by_hash.get(hash).cloned())
                    .collect(),
            ))
        }
    }

    #[async_trait]
    impl SqlWriteExecutionContext for CapturingWriteContext {
        fn active_branch_id(&self) -> &str {
            "branch-b"
        }

        fn functions(&self) -> FunctionProviderHandle {
            test_functions()
        }

        fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
            Ok(Vec::new())
        }

        async fn load_bytes_many(
            &mut self,
            hashes: &[BlobHash],
        ) -> Result<BlobBytesBatch, LixError> {
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

    #[derive(Default)]
    struct RowsLiveStateReader {
        rows: Vec<MaterializedLiveStateRow>,
    }

    #[async_trait]
    impl LiveStateReader for RowsLiveStateReader {
        async fn scan_rows(
            &self,
            _request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            Ok(self.rows.clone())
        }

        async fn load_row(
            &self,
            _request: &LiveStateRowRequest,
        ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
            Ok(None)
        }
    }

    fn live_directory_row(
        entity_pk: &str,
        branch_id: &str,
        snapshot_content: &str,
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_pk: crate::entity_pk::EntityPk::single(entity_pk),
            schema_key: super::DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
            file_id: None,
            snapshot_content: Some(snapshot_content.to_string()),
            metadata: None,
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

    fn live_file_row(
        entity_pk: &str,
        branch_id: &str,
        snapshot_content: &str,
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_pk: crate::entity_pk::EntityPk::single(entity_pk),
            schema_key: super::FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
            file_id: None,
            snapshot_content: Some(snapshot_content.to_string()),
            metadata: None,
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

    fn live_blob_ref_row(
        entity_pk: &str,
        branch_id: &str,
        file_id: &str,
        blob_hash: &str,
        size_bytes: usize,
    ) -> MaterializedLiveStateRow {
        let mut row = live_file_row(
            entity_pk,
            branch_id,
            &format!(r#"{{"id":"{file_id}","blob_hash":"{blob_hash}","size_bytes":{size_bytes}}}"#),
        );
        row.schema_key = super::BLOB_REF_SCHEMA_KEY.to_string();
        row.file_id = Some(file_id.to_string());
        row
    }

    fn plugin_archive(path_glob: &str, schema_key: &str) -> Vec<u8> {
        const WASM_HEADER: &[u8] = b"\0asm\x01\0\0\0";
        let manifest_json = format!(
            r#"{{
                "key": "plugin_sentinel",
                "runtime": "wasm-component-v1",
                "api_version": "0.1.0",
                "match": {{ "path_glob": "{path_glob}" }},
                "entry": "plugin.wasm",
                "schemas": ["schema/plugin_note.json"]
            }}"#
        );
        let schema_json = format!(
            r#"{{
                "x-lix-key": "{schema_key}",
                "x-lix-primary-key": ["/id"],
                "type": "object",
                "properties": {{
                    "id": {{ "type": "string" }},
                    "value": {{ "type": "string" }}
                }},
                "required": ["id", "value"],
                "additionalProperties": false
            }}"#
        );

        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        for (path, bytes) in [
            ("manifest.json", manifest_json.as_bytes()),
            ("schema/plugin_note.json", schema_json.as_bytes()),
            ("plugin.wasm", WASM_HEADER),
        ] {
            writer.start_file(path, options).unwrap();
            writer.write_all(bytes).unwrap();
        }
        writer.finish().unwrap().into_inner()
    }

    fn plugin_install_rows(branch_id: &str, archive_bytes: &[u8]) -> Vec<MaterializedLiveStateRow> {
        let archive_file_id = "lix_plugin_archive::plugin_sentinel";
        let blob_hash = BlobHash::from_content(archive_bytes).to_hex();
        vec![
            live_directory_row(
                "dir-lix-system",
                branch_id,
                r#"{"id":"dir-lix-system","parent_id":null,"name":".lix"}"#,
            ),
            live_directory_row(
                "dir-lix-system-plugins",
                branch_id,
                r#"{"id":"dir-lix-system-plugins","parent_id":"dir-lix-system","name":"plugins"}"#,
            ),
            live_file_row(
                archive_file_id,
                branch_id,
                r#"{"id":"lix_plugin_archive::plugin_sentinel","directory_id":"dir-lix-system-plugins","name":"plugin_sentinel.lixplugin"}"#,
            ),
            live_blob_ref_row(
                archive_file_id,
                branch_id,
                archive_file_id,
                &blob_hash,
                archive_bytes.len(),
            ),
        ]
    }

    fn string_column(values: Vec<Option<&str>>) -> ArrayRef {
        Arc::new(StringArray::from(values)) as ArrayRef
    }

    fn file_insert_batch(include_branch: bool, global: bool) -> RecordBatch {
        let mut fields = vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("directory_id", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, false),
            Field::new("lixcol_global", DataType::Boolean, false),
            Field::new("lixcol_metadata", DataType::Utf8, true),
        ];
        let mut columns = vec![
            string_column(vec![Some("file-readme")]),
            string_column(vec![Some("dir-docs")]),
            string_column(vec![Some("readme.md")]),
            Arc::new(BooleanArray::from(vec![global])) as ArrayRef,
            string_column(vec![Some("{\"source\":\"file\"}")]),
        ];
        if include_branch {
            fields.push(Field::new("lixcol_branch_id", DataType::Utf8, false));
            columns.push(string_column(vec![Some("branch-b")]));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("file insert batch")
    }

    fn data_insert_batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("directory_id", DataType::Utf8, true),
                Field::new("name", DataType::Utf8, false),
                Field::new("data", DataType::Binary, true),
                Field::new("lixcol_branch_id", DataType::Utf8, false),
            ])),
            vec![
                string_column(vec![Some("file-readme")]),
                string_column(vec![Some("dir-docs")]),
                string_column(vec![Some("readme.md")]),
                Arc::new(BinaryArray::from_vec(vec![b"hello"])) as ArrayRef,
                string_column(vec![Some("branch-b")]),
            ],
        )
        .expect("file data batch")
    }

    fn path_data_insert_batch() -> RecordBatch {
        path_data_insert_batch_with_path("/docs/guides/readme.md")
    }

    fn path_data_insert_batch_with_path(path: &str) -> RecordBatch {
        path_data_insert_batch_with_path_and_data(path, b"hello".to_vec())
    }

    fn path_data_insert_batch_with_path_and_data(path: &str, data: Vec<u8>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("path", DataType::Utf8, false),
                Field::new("data", DataType::Binary, true),
                Field::new("lixcol_branch_id", DataType::Utf8, false),
            ])),
            vec![
                string_column(vec![Some("file-readme")]),
                string_column(vec![Some(path)]),
                Arc::new(BinaryArray::from_vec(vec![data.as_slice()])) as ArrayRef,
                string_column(vec![Some("branch-b")]),
            ],
        )
        .expect("file path data batch")
    }

    fn path_update_batch() -> RecordBatch {
        path_update_batch_with_path("/docs/renamed.md")
    }

    fn path_update_batch_with_path(path: &str) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("path", DataType::Utf8, false),
                Field::new("data", DataType::Binary, true),
                Field::new("lixcol_branch_id", DataType::Utf8, false),
            ])),
            vec![
                string_column(vec![Some("file-readme")]),
                string_column(vec![Some(path)]),
                Arc::new(BinaryArray::from_vec(vec![b"hello"])) as ArrayRef,
                string_column(vec![Some("branch-b")]),
            ],
        )
        .expect("file path update batch")
    }

    fn data_update_batch_with_path(path: &str) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("path", DataType::Utf8, false),
                Field::new("data", DataType::Binary, true),
                Field::new("lixcol_branch_id", DataType::Utf8, false),
            ])),
            vec![
                string_column(vec![Some("file-readme")]),
                string_column(vec![Some(path)]),
                Arc::new(BinaryArray::from_vec(vec![b"hello"])) as ArrayRef,
                string_column(vec![Some("branch-b")]),
            ],
        )
        .expect("file data update batch")
    }

    fn descriptor_data_update_batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("path", DataType::Utf8, false),
                Field::new("directory_id", DataType::Utf8, true),
                Field::new("name", DataType::Utf8, false),
                Field::new("data", DataType::Binary, true),
                Field::new("lixcol_branch_id", DataType::Utf8, false),
            ])),
            vec![
                string_column(vec![Some("file-readme")]),
                string_column(vec![Some("/old.raw")]),
                string_column(vec![Some("dir-docs")]),
                string_column(vec![Some("readme.md")]),
                Arc::new(BinaryArray::from_vec(vec![b"hello"])) as ArrayRef,
                string_column(vec![Some("branch-b")]),
            ],
        )
        .expect("file descriptor data update batch")
    }

    fn empty_data_update_batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("data", DataType::Binary, true),
                Field::new("lixcol_branch_id", DataType::Utf8, false),
            ])),
            vec![
                string_column(vec![Some("file-readme")]),
                Arc::new(BinaryArray::from_vec(vec![b""])) as ArrayRef,
                string_column(vec![Some("branch-b")]),
            ],
        )
        .expect("empty file data update batch")
    }

    fn file_delete_batch() -> RecordBatch {
        file_delete_batch_with_path(None)
    }

    fn file_delete_batch_with_path(path: Option<&str>) -> RecordBatch {
        let mut fields = vec![Field::new("id", DataType::Utf8, false)];
        let mut columns = vec![string_column(vec![Some("file-readme")])];
        if let Some(path) = path {
            fields.push(Field::new("path", DataType::Utf8, false));
            columns.push(string_column(vec![Some(path)]));
        }
        fields.push(Field::new("lixcol_branch_id", DataType::Utf8, false));
        columns.push(string_column(vec![Some("branch-b")]));

        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("file delete batch")
    }

    #[test]
    fn derives_nested_directory_paths() {
        let context = FilesystemRowContext::active_branch("branch-a");
        let root = DirectoryDescriptorRecord {
            parent_id: None,
            name: "docs".to_string(),
            key: FilesystemDescriptorKey::from_context(&context, "dir-docs"),
        };
        let child = DirectoryDescriptorRecord {
            parent_id: Some("dir-docs".to_string()),
            name: "guides".to_string(),
            key: FilesystemDescriptorKey::from_context(&context, "dir-guides"),
        };
        let child_key = child.key.clone();
        let records = [root, child];
        let paths = derive_directory_paths(records.iter().map(|row| (row.key.clone(), row)))
            .expect("path derivation should succeed");

        assert_eq!(paths.get(&child_key), Some(&"/docs/guides/".to_string()));
    }

    #[tokio::test]
    async fn file_projection_rejects_unresolved_non_root_directory_id() {
        let blob_reader = Arc::new(CapturingWriteContext::default()) as Arc<dyn BlobDataReader>;
        let error = super::lix_file_record_batch(
            &super::lix_file_schema(),
            &blob_reader,
            None,
            true,
            vec![live_file_row(
                "file-readme",
                "branch-b",
                "{\"id\":\"file-readme\",\"directory_id\":\"missing-dir\",\"name\":\"readme.md\"}",
            )],
        )
        .await
        .expect_err("unresolved non-root directory_id should not project as root path");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
        assert!(error.message.contains("missing-dir"));
    }

    #[tokio::test]
    async fn file_projection_keeps_same_id_descriptors_in_distinct_file_scopes() {
        let blob_reader = Arc::new(CapturingWriteContext::default()) as Arc<dyn BlobDataReader>;
        let mut scoped_file = live_file_row(
            "file-readme",
            "branch-b",
            "{\"id\":\"file-readme\",\"directory_id\":null,\"name\":\"scoped.md\"}",
        );
        scoped_file.file_id = Some("owner-file".to_string());
        let batch = super::lix_file_record_batch(
            &super::lix_file_schema(),
            &blob_reader,
            None,
            true,
            vec![
                live_file_row(
                    "file-readme",
                    "branch-b",
                    "{\"id\":\"file-readme\",\"directory_id\":null,\"name\":\"root.md\"}",
                ),
                scoped_file,
            ],
        )
        .await
        .expect("same descriptor id in different file scopes should project");

        assert_eq!(batch.num_rows(), 2);
        let file_id_column = batch
            .column(batch.schema().index_of("lixcol_file_id").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("lixcol_file_id should be string array");
        let values = (0..batch.num_rows())
            .map(|index| {
                if file_id_column.is_null(index) {
                    None
                } else {
                    Some(file_id_column.value(index).to_string())
                }
            })
            .collect::<Vec<_>>();
        assert!(values.contains(&None));
        assert!(values.contains(&Some("owner-file".to_string())));
    }

    #[tokio::test]
    async fn plugin_discovery_keeps_duplicate_archive_paths_branch_scoped() {
        let archive_a = plugin_archive("*.branch-a", "plugin_note_a");
        let archive_b = plugin_archive("*.branch-b", "plugin_note_b");
        let mut rows = plugin_install_rows("branch-a", &archive_a);
        rows.extend(plugin_install_rows("branch-b", &archive_b));
        let blob_reader = Arc::new(StaticBlobReader::from_blobs(vec![
            archive_a.clone(),
            archive_b.clone(),
        ])) as Arc<dyn BlobDataReader>;

        let installed_plugins = super::load_installed_plugins_for_lix_file_scan(
            Arc::new(RowsLiveStateReader { rows }) as Arc<dyn LiveStateReader>,
            &blob_reader,
            &LiveStateScanRequest {
                filter: LiveStateFilter {
                    branch_ids: vec!["branch-a".to_string(), "branch-b".to_string()],
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .expect("duplicate plugin paths in different branches should not collide");

        let branch_a_plugins = installed_plugins
            .get("branch-a")
            .expect("branch-a plugins should load");
        let branch_b_plugins = installed_plugins
            .get("branch-b")
            .expect("branch-b plugins should load");
        assert_eq!(branch_a_plugins.len(), 1);
        assert_eq!(branch_b_plugins.len(), 1);
        assert_eq!(branch_a_plugins[0].path_glob, "*.branch-a");
        assert_eq!(branch_b_plugins[0].path_glob, "*.branch-b");
        assert_eq!(branch_a_plugins[0].schema_keys, vec!["plugin_note_a"]);
        assert_eq!(branch_b_plugins[0].schema_keys, vec!["plugin_note_b"]);
    }

    #[test]
    fn decodes_file_insert_into_lix_state_write_row() {
        let batch = file_insert_batch(true, false);

        let rows = lix_file_write_rows_from_batch(&batch, None).expect("decode file insert");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].entity_pk.as_ref(),
            Some(&crate::entity_pk::EntityPk::single("file-readme"))
        );
        assert_eq!(rows[0].schema_key, "lix_file_descriptor");
        assert_eq!(rows[0].branch_id, "branch-b");
        assert_eq!(
            rows[0].metadata.as_ref(),
            Some(&TransactionJson::from_value_for_test(
                serde_json::json!({"source": "file"})
            ))
        );
        let snapshot = rows[0].snapshot.as_ref().expect("descriptor snapshot JSON");
        assert_eq!(snapshot["id"], "file-readme");
        assert_eq!(snapshot["directory_id"], "dir-docs");
        assert_eq!(snapshot["name"], "readme.md");
    }

    #[test]
    fn active_file_insert_defaults_branch_id() {
        let batch = file_insert_batch(false, false);

        let rows =
            lix_file_write_rows_from_batch(&batch, Some("branch-a")).expect("decode file insert");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].branch_id, "branch-a");
    }

    #[test]
    fn by_branch_file_insert_requires_branch_id_for_non_global_rows() {
        let batch = file_insert_batch(false, false);

        let error =
            lix_file_write_rows_from_batch(&batch, None).expect_err("branch id is required");

        assert!(
            error.to_string().contains("requires lixcol_branch_id"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn file_insert_rejects_global_with_non_global_branch_id() {
        let error = lix_file_write_rows_from_batch(&file_insert_batch(true, true), None)
            .expect_err("global file write should reject conflicting branch id");

        assert!(
            error
                .to_string()
                .contains("cannot set lixcol_global=true with non-global lixcol_branch_id"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn file_update_accepts_path_assignment() {
        super::validate_lix_file_update_assignments(
            &super::lix_file_schema(),
            &[("path".to_string(), lit("/docs/renamed.md"))],
        )
        .expect("path should be writable for update");
    }

    #[test]
    fn file_path_insert_rejects_invalid_plugin_storage_path() {
        let mut resolvers = BTreeMap::new();

        let error = lix_file_insert_stage_from_batch_with_path_resolvers(
            &path_data_insert_batch_with_path_and_data(
                "/.lix/plugins/nested/plugin_sentinel.lixplugin",
                plugin_archive("*.sentinel", "plugin_note"),
            ),
            None,
            "lix_file",
            &mut resolvers,
            &mut test_id_generator(&["should-not-be-used"]),
            true,
        )
        .expect_err("normal file insert should reject plugin storage path");

        assert!(
            error.to_string().contains("reserved plugin storage path"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn file_path_update_rejects_invalid_plugin_storage_path() {
        let mut resolvers = BTreeMap::new();

        let error = lix_file_update_stage_from_batch_for_test(
            &path_update_batch_with_path("/.lix/plugins/nested/plugin_sentinel.lixplugin"),
            None,
            super::LixFileUpdateColumns {
                path: true,
                data: false,
                descriptor: false,
            },
            Some(&mut resolvers),
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect_err("normal file update should reject plugin storage path");

        assert!(
            error.to_string().contains("reserved plugin storage path")
                || error.to_string().contains("plugin archive paths"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn file_path_update_rejects_valid_plugin_storage_path() {
        let mut resolvers = BTreeMap::new();

        let error = lix_file_update_stage_from_batch_for_test(
            &path_update_batch_with_path("/.lix/plugins/plugin_sentinel.lixplugin"),
            None,
            super::LixFileUpdateColumns {
                path: true,
                data: false,
                descriptor: false,
            },
            Some(&mut resolvers),
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect_err("normal file path update should reject plugin archive path");

        assert!(
            error.to_string().contains("plugin archive paths"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn file_data_update_rejects_invalid_existing_plugin_storage_path() {
        let error = lix_file_update_stage_from_batch_for_test(
            &data_update_batch_with_path("/.lix/plugins/nested/plugin_sentinel.lixplugin"),
            None,
            super::LixFileUpdateColumns {
                path: false,
                data: true,
                descriptor: false,
            },
            None,
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect_err("normal file data update should reject installed archive path");

        assert!(
            error.to_string().contains("reserved plugin storage path")
                || error.to_string().contains("valid zip file"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn file_delete_rejects_plugin_storage_path() {
        let error = lix_file_delete_stage_from_batch(
            &file_delete_batch_with_path(Some("/.lix/plugins/plugin_sentinel.lixplugin")),
            None,
            &BTreeSet::new(),
        )
        .expect_err("normal file delete should reject installed archive path");

        assert!(
            error.to_string().contains("reserved plugin storage path"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn file_path_update_stages_descriptor_from_new_path() {
        let mut resolvers = BTreeMap::new();
        resolvers.insert(
            super::filesystem_storage_scope_key("branch-b", false, false, None),
            super::DirectoryPathResolver::from_existing([(
                "/docs/".to_string(),
                "dir-docs".to_string(),
            )])
            .expect("directory resolver should seed"),
        );

        let staged = lix_file_update_stage_from_batch_for_test(
            &path_update_batch(),
            None,
            super::LixFileUpdateColumns {
                path: true,
                data: false,
                descriptor: false,
            },
            Some(&mut resolvers),
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect("decode file path update");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.file_data_writes.len(), 0);
        assert_eq!(staged.state_rows.len(), 1);
        let descriptor = staged
            .state_rows
            .iter()
            .find(|row| row.schema_key == "lix_file_descriptor")
            .expect("file descriptor row should be staged");
        let snapshot: JsonValue = descriptor.snapshot.as_ref().unwrap().value().clone();
        assert_eq!(snapshot["id"], "file-readme");
        assert_eq!(snapshot["directory_id"], "dir-docs");
        assert_eq!(snapshot["name"], "renamed.md");
    }

    #[test]
    fn file_path_update_preserves_existing_data_unless_data_is_assigned() {
        let mut resolvers = BTreeMap::new();
        resolvers.insert(
            super::filesystem_storage_scope_key("branch-b", false, false, None),
            super::DirectoryPathResolver::from_existing([(
                "/docs/".to_string(),
                "dir-docs".to_string(),
            )])
            .expect("directory resolver should seed"),
        );

        let staged = lix_file_update_stage_from_batch_for_test(
            &path_update_batch(),
            None,
            super::LixFileUpdateColumns {
                path: true,
                data: false,
                descriptor: false,
            },
            Some(&mut resolvers),
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect("decode file path update");

        assert!(
            staged.file_data_writes.is_empty(),
            "path-only update should not rewrite file data"
        );
        assert!(
            staged
                .state_rows
                .iter()
                .all(|row| row.schema_key != "lix_binary_blob_ref"),
            "path-only update should not rewrite the blob ref"
        );
    }

    #[tokio::test]
    async fn file_path_update_seeds_resolver_from_visible_directory_state() {
        let mut resolvers = super::directory_path_resolvers_from_live_state(
            Arc::new(RowsLiveStateReader {
                rows: vec![live_directory_row(
                    "dir-docs",
                    "branch-b",
                    "{\"id\":\"dir-docs\",\"parent_id\":null,\"name\":\"docs\"}",
                )],
            }) as Arc<dyn LiveStateReader>,
            Some("branch-b"),
        )
        .await
        .expect("directory state should seed path resolver");

        let staged = lix_file_update_stage_from_batch_for_test(
            &path_update_batch(),
            None,
            super::LixFileUpdateColumns {
                path: true,
                data: false,
                descriptor: false,
            },
            Some(&mut resolvers),
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect("decode file path update");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.state_rows.len(), 1);
        assert!(
            staged
                .state_rows
                .iter()
                .all(|row| row.schema_key != "lix_directory_descriptor")
        );

        let snapshot: JsonValue = staged.state_rows[0]
            .snapshot
            .as_ref()
            .unwrap()
            .value()
            .clone();
        assert_eq!(snapshot["directory_id"], "dir-docs");
        assert_eq!(snapshot["name"], "renamed.md");
    }

    #[tokio::test]
    async fn file_path_update_stages_only_missing_parent_directories() {
        let mut resolvers = super::directory_path_resolvers_from_live_state(
            Arc::new(RowsLiveStateReader::default()) as Arc<dyn LiveStateReader>,
            Some("branch-b"),
        )
        .await
        .expect("empty directory state should seed path resolver");

        let staged = lix_file_update_stage_from_batch_for_test(
            &path_update_batch(),
            None,
            super::LixFileUpdateColumns {
                path: true,
                data: false,
                descriptor: false,
            },
            Some(&mut resolvers),
            &mut test_id_generator(&["dir-generated-docs"]),
        )
        .expect("decode file path update");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.state_rows.len(), 2);
        assert_eq!(
            staged
                .state_rows
                .iter()
                .filter(|row| row.schema_key == "lix_directory_descriptor")
                .count(),
            1
        );

        let directory = staged
            .state_rows
            .iter()
            .find(|row| row.schema_key == "lix_directory_descriptor")
            .expect("missing /docs/ directory should be staged");
        assert_eq!(
            directory.entity_pk.as_ref(),
            Some(&crate::entity_pk::EntityPk::single("dir-generated-docs"))
        );

        let descriptor = staged
            .state_rows
            .iter()
            .find(|row| row.schema_key == "lix_file_descriptor")
            .expect("file descriptor should be staged");
        let snapshot: JsonValue = descriptor.snapshot.as_ref().unwrap().value().clone();
        assert_eq!(snapshot["directory_id"], "dir-generated-docs");
    }

    #[test]
    fn file_path_update_with_data_assignment_stages_blob_ref_and_payload() {
        let mut resolvers = BTreeMap::new();
        resolvers.insert(
            super::filesystem_storage_scope_key("branch-b", false, false, None),
            super::DirectoryPathResolver::from_existing([(
                "/docs/".to_string(),
                "dir-docs".to_string(),
            )])
            .expect("directory resolver should seed"),
        );

        let staged = lix_file_update_stage_from_batch_for_test(
            &path_update_batch(),
            None,
            super::LixFileUpdateColumns {
                path: true,
                data: true,
                descriptor: false,
            },
            Some(&mut resolvers),
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect("decode file path and data update");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.file_data_writes.len(), 1);
        assert_eq!(staged.file_data_writes[0].file_id, "file-readme");
        assert_eq!(staged.file_data_writes[0].data, b"hello");
        assert!(
            staged
                .state_rows
                .iter()
                .any(|row| row.schema_key == "lix_file_descriptor")
        );
        assert!(
            staged
                .state_rows
                .iter()
                .any(|row| row.schema_key == "lix_binary_blob_ref")
        );
    }

    #[test]
    fn file_descriptor_update_with_data_stages_payload_at_assigned_path() {
        let mut resolvers = BTreeMap::new();
        resolvers.insert(
            super::filesystem_storage_scope_key("branch-b", false, false, None),
            super::DirectoryPathResolver::from_existing([(
                "/docs/".to_string(),
                "dir-docs".to_string(),
            )])
            .expect("directory resolver should seed"),
        );

        let staged = lix_file_update_stage_from_batch_for_test(
            &descriptor_data_update_batch(),
            None,
            super::LixFileUpdateColumns {
                path: false,
                data: true,
                descriptor: true,
            },
            Some(&mut resolvers),
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect("decode file descriptor and data update");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.file_data_writes.len(), 1);
        assert_eq!(staged.file_data_writes[0].file_id, "file-readme");
        assert_eq!(
            staged.file_data_writes[0].path.as_deref(),
            Some("/docs/readme.md")
        );
        assert_eq!(staged.file_data_writes[0].data, b"hello");
    }

    #[test]
    fn file_data_update_without_path_ignores_materialized_path_column() {
        let staged = lix_file_update_stage_from_batch_for_test(
            &path_update_batch(),
            None,
            super::LixFileUpdateColumns {
                path: false,
                data: true,
                descriptor: false,
            },
            None,
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect("decode file data update");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.file_data_writes.len(), 1);
        assert_eq!(staged.file_data_writes[0].file_id, "file-readme");
        assert_eq!(staged.state_rows.len(), 1);
        assert_eq!(staged.state_rows[0].schema_key, "lix_binary_blob_ref");
    }

    #[test]
    fn file_data_update_to_empty_ignores_blob_ref_in_other_scope() {
        let staged = lix_file_update_stage_from_batch_with_blob_keys_for_test(
            &empty_data_update_batch(),
            None,
            super::LixFileUpdateColumns {
                path: false,
                data: true,
                descriptor: false,
            },
            None,
            &mut test_id_generator(&["should-not-be-used"]),
            &BTreeSet::from([blob_ref_key("branch-a", false, false, "file-readme")]),
        )
        .expect("decode empty file data update");

        assert_eq!(staged.count, 1);
        assert!(
            staged.state_rows.is_empty(),
            "blob ref from another branch must not produce a tombstone"
        );
    }

    #[test]
    fn file_insert_stages_non_null_data() {
        let batch = data_insert_batch();

        let staged = lix_file_insert_stage_from_batch(&batch, None).expect("decode file data");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.state_rows.len(), 2);
        assert!(
            staged
                .state_rows
                .iter()
                .any(|row| row.schema_key == "lix_file_descriptor")
        );
        let blob_ref_row = staged
            .state_rows
            .iter()
            .find(|row| row.schema_key == "lix_binary_blob_ref")
            .expect("data insert should stage blob ref row");
        assert_eq!(
            blob_ref_row.entity_pk.as_ref(),
            Some(&crate::entity_pk::EntityPk::single("file-readme"))
        );
        assert_eq!(blob_ref_row.file_id.as_deref(), Some("file-readme"));
        assert_eq!(staged.file_data_writes.len(), 1);
        assert_eq!(staged.file_data_writes[0].file_id, "file-readme");
        assert_eq!(staged.file_data_writes[0].branch_id, "branch-b");
        assert_eq!(staged.file_data_writes[0].data, b"hello");
    }

    #[test]
    fn file_delete_with_blob_ref_stages_descriptor_and_blob_ref_tombstones() {
        let batch = file_delete_batch();
        let staged = lix_file_delete_stage_from_batch(
            &batch,
            None,
            &BTreeSet::from([blob_ref_key("branch-b", false, false, "file-readme")]),
        )
        .expect("decode file delete");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.state_rows.len(), 2);
        let descriptor = staged
            .state_rows
            .iter()
            .find(|row| row.schema_key == "lix_file_descriptor")
            .expect("file descriptor tombstone should be staged");
        assert_eq!(
            descriptor.entity_pk.as_ref(),
            Some(&crate::entity_pk::EntityPk::single("file-readme"))
        );
        assert_eq!(descriptor.file_id, None);
        assert_eq!(descriptor.snapshot, None);

        let blob_ref = staged
            .state_rows
            .iter()
            .find(|row| row.schema_key == "lix_binary_blob_ref")
            .expect("blob ref tombstone should be staged");
        assert_eq!(
            blob_ref.entity_pk.as_ref(),
            Some(&crate::entity_pk::EntityPk::single("file-readme"))
        );
        assert_eq!(blob_ref.file_id.as_deref(), Some("file-readme"));
        assert_eq!(blob_ref.snapshot, None);
    }

    #[test]
    fn file_delete_without_blob_ref_stages_only_descriptor_tombstone() {
        let batch = file_delete_batch();
        let staged = lix_file_delete_stage_from_batch(&batch, None, &BTreeSet::new())
            .expect("decode file delete");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.state_rows.len(), 1);
        assert_eq!(staged.state_rows[0].schema_key, "lix_file_descriptor");
        assert_eq!(
            staged.state_rows[0].entity_pk.as_ref(),
            Some(&crate::entity_pk::EntityPk::single("file-readme"))
        );
        assert_eq!(staged.state_rows[0].snapshot, None);
    }

    #[test]
    fn file_delete_ignores_blob_ref_in_other_scope() {
        let batch = file_delete_batch();
        let staged = lix_file_delete_stage_from_batch(
            &batch,
            None,
            &BTreeSet::from([blob_ref_key("branch-a", false, false, "file-readme")]),
        )
        .expect("decode file delete");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.state_rows.len(), 1);
        assert_eq!(staged.state_rows[0].schema_key, "lix_file_descriptor");
    }

    #[test]
    fn file_path_insert_reuses_existing_parent_directory() {
        let mut resolvers = BTreeMap::new();
        resolvers.insert(
            super::filesystem_storage_scope_key("branch-b", false, false, None),
            super::DirectoryPathResolver::from_existing([
                ("/docs/".to_string(), "dir-docs".to_string()),
                ("/docs/guides/".to_string(), "dir-guides".to_string()),
            ])
            .expect("directory resolver should seed"),
        );

        let staged = lix_file_insert_stage_from_batch_with_path_resolvers(
            &path_data_insert_batch(),
            None,
            "lix_file",
            &mut resolvers,
            &mut test_id_generator(&["should-not-be-used"]),
            true,
        )
        .expect("decode file path data");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.file_data_writes.len(), 1);
        assert_eq!(staged.file_data_writes[0].file_id, "file-readme");
        assert_eq!(staged.state_rows.len(), 2);
        let descriptor = staged
            .state_rows
            .iter()
            .find(|row| row.schema_key == "lix_file_descriptor")
            .expect("file descriptor row should be staged");
        let snapshot: JsonValue = descriptor.snapshot.as_ref().unwrap().value().clone();
        assert_eq!(snapshot["id"], "file-readme");
        assert_eq!(snapshot["directory_id"], "dir-guides");
        assert_eq!(snapshot["name"], "readme.md");
    }

    #[test]
    fn file_path_insert_stages_missing_parent_directories_once() {
        let mut resolvers = BTreeMap::new();

        let staged = lix_file_insert_stage_from_batch_with_path_resolvers(
            &path_data_insert_batch(),
            None,
            "lix_file",
            &mut resolvers,
            &mut test_id_generator(&["dir-generated-docs", "dir-generated-guides"]),
            true,
        )
        .expect("decode file path data");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.state_rows.len(), 4);
        let directory_rows = staged
            .state_rows
            .iter()
            .filter(|row| row.schema_key == "lix_directory_descriptor")
            .collect::<Vec<_>>();
        assert_eq!(directory_rows.len(), 2);

        let descriptor = staged
            .state_rows
            .iter()
            .find(|row| row.schema_key == "lix_file_descriptor")
            .expect("file descriptor row should be staged");
        let snapshot: JsonValue = descriptor.snapshot.as_ref().unwrap().value().clone();
        assert_eq!(snapshot["directory_id"], "dir-generated-guides");
    }

    #[tokio::test]
    async fn file_insert_sink_stages_decoded_lix_state_rows() {
        let batch = file_insert_batch(true, false);
        let mut write_context = CapturingWriteContext::default();
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let sink = LixFileInsertSink::new(
            write_ctx,
            test_functions(),
            BranchBinding::explicit(),
            false,
        );

        let count = sink
            .write_batches(vec![batch], &Arc::new(TaskContext::default()))
            .await
            .expect("file insert sink should stage");

        assert_eq!(count, 1);
        let writes = &write_context.writes;
        assert_eq!(writes.len(), 1);
        match &writes[0] {
            TransactionWrite::Rows { mode, rows } => {
                assert_eq!(*mode, TransactionWriteMode::Insert);
                assert_eq!(rows.len(), 1);
                assert_eq!(
                    rows[0].entity_pk.as_ref(),
                    Some(&crate::entity_pk::EntityPk::single("file-readme"))
                );
                assert_eq!(rows[0].schema_key, "lix_file_descriptor");
            }
            other => panic!("expected insert staged write, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn file_insert_sink_stages_file_data_writes() {
        let batch = data_insert_batch();
        let mut write_context = CapturingWriteContext {
            rows: vec![live_directory_row(
                "dir-docs",
                "branch-b",
                "{\"id\":\"dir-docs\",\"parent_id\":null,\"name\":\"docs\"}",
            )],
            writes: Vec::new(),
        };
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let sink =
            LixFileInsertSink::new(write_ctx, test_functions(), BranchBinding::explicit(), true);

        let count = sink
            .write_batches(vec![batch], &Arc::new(TaskContext::default()))
            .await
            .expect("file insert sink should stage data");

        assert_eq!(count, 1);
        let writes = &write_context.writes;
        assert_eq!(writes.len(), 1);
        match &writes[0] {
            TransactionWrite::RowsWithFileData {
                mode,
                rows,
                file_data,
                count,
                ..
            } => {
                assert_eq!(*mode, TransactionWriteMode::Insert);
                assert_eq!(*count, 1);
                assert_eq!(rows.len(), 2);
                assert!(
                    rows.iter()
                        .any(|row| row.schema_key == "lix_file_descriptor")
                );
                assert!(
                    rows.iter()
                        .any(|row| row.schema_key == "lix_binary_blob_ref")
                );
                assert_eq!(file_data.len(), 1);
                assert_eq!(file_data[0].file_id, "file-readme");
                assert_eq!(file_data[0].path.as_deref(), Some("/docs/readme.md"));
                assert_eq!(file_data[0].data, b"hello");
            }
            other => panic!("expected insert with file data staged write, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn file_insert_sink_seeds_path_resolver_from_live_state() {
        let batch = path_data_insert_batch();
        let mut write_context = CapturingWriteContext {
            rows: vec![
                live_directory_row(
                    "dir-docs",
                    "branch-b",
                    "{\"id\":\"dir-docs\",\"parent_id\":null,\"name\":\"docs\"}",
                ),
                live_directory_row(
                    "dir-guides",
                    "branch-b",
                    "{\"id\":\"dir-guides\",\"parent_id\":\"dir-docs\",\"name\":\"guides\"}",
                ),
            ],
            writes: Vec::new(),
        };
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let sink =
            LixFileInsertSink::new(write_ctx, test_functions(), BranchBinding::explicit(), true);

        let count = sink
            .write_batches(vec![batch], &Arc::new(TaskContext::default()))
            .await
            .expect("file insert sink should stage path data");

        assert_eq!(count, 1);
        let writes = &write_context.writes;
        assert_eq!(writes.len(), 1);
        match &writes[0] {
            TransactionWrite::RowsWithFileData {
                rows,
                file_data,
                count,
                ..
            } => {
                assert_eq!(*count, 1);
                assert_eq!(file_data.len(), 1);
                assert_eq!(file_data[0].file_id, "file-readme");
                let descriptor = rows
                    .iter()
                    .find(|row| row.schema_key == "lix_file_descriptor")
                    .expect("file descriptor row should be staged");
                let snapshot: JsonValue = descriptor.snapshot.as_ref().unwrap().value().clone();
                assert_eq!(snapshot["directory_id"], "dir-guides");
            }
            other => panic!("expected insert with file data staged write, got {other:?}"),
        }
    }
}
