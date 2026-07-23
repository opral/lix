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
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, LargeBinaryArray, RecordBatchOptions, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, DataFusionError, Result, ScalarValue};
use datafusion::execution::TaskContext;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::expr::{InList, Like};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::{PhysicalExpr, create_physical_expr};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion::prelude::SessionContext;
use futures_util::{FutureExt, future::try_join_all};
use serde::Deserialize;

use crate::binary_cas::{BlobDataReader, BlobHash};
use crate::branch::BranchRefReader;
use crate::common::{LixPath, MutationIdentity, RequestBlobSpliceProvenance, compose_file_path};
use crate::entity_pk::EntityPk;
use crate::filesystem::{FilesystemIndex, filesystem_schema_keys};
use crate::filesystem::{
    FilesystemPathEntry, FilesystemPathIndexReader, FilesystemPathIndexRequest, FilesystemPathKind,
    FilesystemPathSelection,
};
use crate::functions::FunctionProviderHandle;
use crate::live_state::{
    LiveStateExactBatchRequest, LiveStateExactRowRequest, LiveStateFilter, LiveStateProjection,
    LiveStateReader, LiveStateScanRequest, MaterializedLiveStateRow,
};
use crate::plugin::{
    CompiledPluginCatalog, PLUGIN_OWNER_KEY, PLUGIN_REGISTRY_KEY, PluginActorColdInstall,
    PluginActorColdOpen, PluginActorKey, PluginFileOwner, PluginRegistry, PluginRegistryEntry,
    PluginRuntime, PluginRuntimeHost, VecEntitySource, drain_entity_transition_edits,
    inferred_media_type_for_path, is_plugin_storage_path, plugin_key_from_archive_file_id,
    plugin_key_from_archive_path, plugin_state_live_state_projection,
    plugin_storage_archive_file_id, render_plugin_state_with_component_instance,
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
    InsertCell, InsertColumnIntents, SqlCell, UpdateAssignmentValues, UpdateCell,
    lix_file_data_type_error, lix_file_data_type_error_with_value, scalar_is_binary_or_null,
};
use crate::sql2::{SessionFileViewKey, SessionFileViews, SessionPluginFileView};
use crate::transaction::types::{TransactionJson, TransactionWriteRow};
use crate::wasm::WasmComponentInstance;
use crate::wasm::{
    WasmComponentV2Factory, WasmFileDescriptor, WasmHostBytes, WasmHostEntity,
    WasmOpenEntitiesInput, WasmPluginSelection, WasmTransitionLimits,
};
use crate::{
    GLOBAL_BRANCH_ID, LixError, SqlQueryResult, Value, parse_row_metadata_value,
    serialize_row_metadata,
};

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";

use crate::filesystem::{
    BlobRefRowInput, DirectoryPathRecord, DirectoryPathResolver, FileDeleteInput,
    FileDescriptorRowInput, FileDescriptorWriteInput, FileDescriptorWriteIntent,
    FilesystemBlobRefKey, FilesystemDeletePlan, FilesystemDescriptorKey, FilesystemRowContext,
    blob_ref_row, blob_ref_tombstone_row, derive_directory_paths,
    directory_path_resolvers_from_live_state, directory_path_resolvers_from_path_index,
    directory_path_resolvers_from_state_rows, file_descriptor_row, file_descriptor_write_row,
    filesystem_storage_scope_key, plan_file_delete, plan_file_descriptor_write,
    plan_parsed_file_path_update_with_resolvers, plan_parsed_file_path_write_with_resolvers,
};
use crate::sql2::result_metadata::json_field;
use crate::sql2::session::SqlWriteSessionOptions;
use crate::sql2::{
    SqlWriteContext, SqlWriteExecutionContext, WriteAccess, WriteContextLiveStateReader,
};
use crate::transaction::types::{
    LogicalPrimaryKey, TransactionFileData, TransactionWrite, TransactionWriteMode,
    TransactionWriteOperation, TransactionWriteOrigin,
};

use super::spec::{
    DmlApply, DmlPlanOptions, InsertApply, PlannedDml, PlannedScan, RowSource, TableSpec,
    finish_scan_batch, register_spec_table, row_source,
};
use super::upsert::{
    StagedUpsert, UpsertConflictKind, UpsertConflictTarget, UpsertSupport, validate_target_columns,
};

pub(super) async fn register_lix_file_active_provider(
    session: &SessionContext,
    surface_name: &str,
    active_branch_id: &str,
    live_state: Arc<dyn LiveStateReader>,
    filesystem_path_index: Arc<dyn FilesystemPathIndexReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    blob_reader: Arc<dyn BlobDataReader>,
    plugin_host: PluginRuntimeHost,
    functions: FunctionProviderHandle,
    session_file_views: Option<SessionFileViews>,
) -> Result<(), LixError> {
    register_spec_table(
        session,
        surface_name,
        Arc::new(
            LixFileSpec::active_branch(
                active_branch_id,
                live_state,
                filesystem_path_index,
                branch_ref,
                blob_reader,
                plugin_host,
                functions,
            )
            .with_session_file_views(session_file_views),
        ),
        WriteAccess::read_only(),
    )
}

pub(super) async fn register_lix_file_by_branch_provider(
    session: &SessionContext,
    surface_name: &str,
    live_state: Arc<dyn LiveStateReader>,
    filesystem_path_index: Arc<dyn FilesystemPathIndexReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    blob_reader: Arc<dyn BlobDataReader>,
    plugin_host: PluginRuntimeHost,
    functions: FunctionProviderHandle,
    session_file_views: Option<SessionFileViews>,
) -> Result<(), LixError> {
    register_spec_table(
        session,
        surface_name,
        Arc::new(
            LixFileSpec::by_branch(
                live_state,
                filesystem_path_index,
                branch_ref,
                blob_reader,
                plugin_host,
                functions,
            )
            .with_session_file_views(session_file_views),
        ),
        WriteAccess::read_only(),
    )
}

pub(super) async fn register_by_branch_write_provider(
    session: &SessionContext,
    surface_name: &str,
    write_ctx: SqlWriteContext,
    branch_ref: Arc<dyn BranchRefReader>,
    options: SqlWriteSessionOptions,
) -> Result<(), LixError> {
    register_spec_table(
        session,
        surface_name,
        Arc::new(LixFileSpec::by_branch_with_write(
            write_ctx.clone(),
            branch_ref,
            options,
        )),
        WriteAccess::write(write_ctx),
    )
}

pub(super) async fn register_active_write_provider(
    session: &SessionContext,
    surface_name: &str,
    write_ctx: SqlWriteContext,
    branch_ref: Arc<dyn BranchRefReader>,
    options: SqlWriteSessionOptions,
) -> Result<(), LixError> {
    register_spec_table(
        session,
        surface_name,
        Arc::new(LixFileSpec::active_branch_with_write(
            write_ctx.clone(),
            branch_ref,
            options,
        )),
        WriteAccess::write(write_ctx),
    )
}

struct LixFileSpec {
    schema: SchemaRef,
    live_state: Arc<dyn LiveStateReader>,
    filesystem_path_index: Arc<dyn FilesystemPathIndexReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    blob_reader: Arc<dyn BlobDataReader>,
    plugin_host: PluginRuntimeHost,
    functions: FunctionProviderHandle,
    branch_binding: BranchBinding,
    options: SqlWriteSessionOptions,
    session_file_views: Option<SessionFileViews>,
}

struct LixFileDmlSourceState {
    blob_ref_keys: BTreeSet<FilesystemBlobRefKey>,
    plugin_render: Option<PluginRenderContext>,
    path_resolver_rows: Option<Vec<MaterializedLiveStateRow>>,
    path_index: Option<FilesystemPathSelection>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExactLixFileReadColumn {
    Data,
    ChangeId,
}

impl ExactLixFileReadColumn {
    fn name(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::ChangeId => "lixcol_change_id",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ExactLixFileReadSelector {
    Id(String),
    Path(String),
}

type SharedLixFileDmlSourceState = Arc<Mutex<Option<LixFileDmlSourceState>>>;

impl LixFileSpec {
    async fn indexed_dml_matches(
        &self,
        request: &LiveStateScanRequest,
        filters: &[Expr],
        target_file_ids: &FileIdConstraint,
    ) -> Result<Option<FilesystemPathSelection>> {
        let predicate = file_path_predicate_from_filters(filters);
        match target_file_ids {
            // Preserve the generic source's allocation-free contradiction
            // short circuit instead of loading the path index for no rows.
            FileIdConstraint::None => return Ok(None),
            FileIdConstraint::All if predicate == FilePathPredicate::All => return Ok(None),
            FileIdConstraint::All | FileIdConstraint::Ids(_) => {}
        }
        let index = self
            .filesystem_path_index
            .path_index(&FilesystemPathIndexRequest::new(
                request.filter.branch_ids.clone(),
            ))
            .await
            .map_err(lix_error_to_datafusion_error)?;
        Ok(Some(match target_file_ids {
            FileIdConstraint::Ids(file_ids) => indexed_file_id_matches(index, file_ids, &predicate),
            FileIdConstraint::All => indexed_file_matches(index, &predicate),
            FileIdConstraint::None => unreachable!("handled before loading the path index"),
        }))
    }

    fn active_branch(
        active_branch_id: impl Into<String>,
        live_state: Arc<dyn LiveStateReader>,
        filesystem_path_index: Arc<dyn FilesystemPathIndexReader>,
        branch_ref: Arc<dyn BranchRefReader>,
        blob_reader: Arc<dyn BlobDataReader>,
        plugin_host: PluginRuntimeHost,
        functions: FunctionProviderHandle,
    ) -> Self {
        Self {
            schema: lix_file_schema(),
            live_state,
            filesystem_path_index,
            branch_ref,
            blob_reader,
            plugin_host,
            functions,
            branch_binding: BranchBinding::active(active_branch_id),
            options: SqlWriteSessionOptions::default(),
            session_file_views: None,
        }
    }

    fn active_branch_with_write(
        write_ctx: SqlWriteContext,
        branch_ref: Arc<dyn BranchRefReader>,
        options: SqlWriteSessionOptions,
    ) -> Self {
        let active_branch_id = write_ctx.active_branch_id();
        let functions = write_ctx.functions();
        let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx.clone()));
        let filesystem_path_index: Arc<dyn FilesystemPathIndexReader> = live_state.clone();
        let blob_reader = write_ctx.blob_reader();
        let plugin_host = write_ctx.plugin_host();
        let session_file_views = write_ctx.session_file_views();
        Self {
            schema: lix_file_schema(),
            live_state,
            filesystem_path_index,
            branch_ref,
            blob_reader,
            plugin_host,
            functions,
            branch_binding: BranchBinding::active(active_branch_id),
            options,
            session_file_views,
        }
    }

    fn by_branch(
        live_state: Arc<dyn LiveStateReader>,
        filesystem_path_index: Arc<dyn FilesystemPathIndexReader>,
        branch_ref: Arc<dyn BranchRefReader>,
        blob_reader: Arc<dyn BlobDataReader>,
        plugin_host: PluginRuntimeHost,
        functions: FunctionProviderHandle,
    ) -> Self {
        Self {
            schema: lix_file_by_branch_schema(),
            live_state,
            filesystem_path_index,
            branch_ref,
            blob_reader,
            plugin_host,
            functions,
            branch_binding: BranchBinding::explicit(),
            options: SqlWriteSessionOptions::default(),
            session_file_views: None,
        }
    }

    fn by_branch_with_write(
        write_ctx: SqlWriteContext,
        branch_ref: Arc<dyn BranchRefReader>,
        options: SqlWriteSessionOptions,
    ) -> Self {
        let functions = write_ctx.functions();
        let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx.clone()));
        let filesystem_path_index: Arc<dyn FilesystemPathIndexReader> = live_state.clone();
        let blob_reader = write_ctx.blob_reader();
        let plugin_host = write_ctx.plugin_host();
        let session_file_views = write_ctx.session_file_views();
        Self {
            schema: lix_file_by_branch_schema(),
            live_state,
            filesystem_path_index,
            branch_ref,
            blob_reader,
            plugin_host,
            functions,
            branch_binding: BranchBinding::explicit(),
            options,
            session_file_views,
        }
    }

    fn with_session_file_views(mut self, session_file_views: Option<SessionFileViews>) -> Self {
        self.session_file_views = session_file_views;
        self
    }

    /// Build the unprojected candidate-row source for UPDATE/DELETE: scan the
    /// scoped live-state rows, then render the full `lix_file` batch the
    /// statement filters run against.
    fn dml_source(
        &self,
        write_ctx: &SqlWriteContext,
        request: LiveStateScanRequest,
        target_file_ids: FileIdConstraint,
        indexed_matches: Option<FilesystemPathSelection>,
        needs_data: bool,
        needs_plugin_ownership: bool,
        capture_path_resolver_rows: bool,
        captured: SharedLixFileDmlSourceState,
    ) -> RowSource {
        row_source(
            (
                write_ctx.clone(),
                Arc::clone(&self.blob_reader),
                self.plugin_host.clone(),
                Arc::clone(&self.schema),
                request,
                target_file_ids,
                indexed_matches,
                needs_data,
                needs_plugin_ownership,
                capture_path_resolver_rows,
                self.session_file_views.clone(),
                captured,
            ),
            |(
                write_ctx,
                blob_reader,
                plugin_host,
                table_schema,
                request,
                target_file_ids,
                indexed_matches,
                needs_data,
                needs_plugin_ownership,
                capture_path_resolver_rows,
                session_file_views,
                captured,
            )| async move {
                *captured.lock().expect("lix_file DML source mutex poisoned") = None;
                let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx.clone()));
                let (prepared, path_resolver_rows, path_index) = if let Some(indexed_matches) =
                    indexed_matches.as_ref()
                {
                    let rows = match &target_file_ids {
                        // Exact DML must still validate a targeted blob ref
                        // when its descriptor is missing from the path index.
                        FileIdConstraint::Ids(file_ids) => {
                            scan_exact_file_blob_rows(live_state.clone(), &request, file_ids).await
                        }
                        FileIdConstraint::All | FileIdConstraint::None => {
                            scan_indexed_file_rows(
                                live_state.clone(),
                                &request,
                                indexed_matches,
                                true,
                            )
                            .await
                        }
                    }
                    .map_err(lix_error_to_datafusion_error)?;
                    (
                        prepare_indexed_lix_file_rows(indexed_matches, rows),
                        None,
                        capture_path_resolver_rows.then(|| indexed_matches.clone()),
                    )
                } else {
                    let rows =
                        scan_lix_file_live_rows(live_state.clone(), &request, &target_file_ids)
                            .await
                            .map_err(lix_error_to_datafusion_error)?;
                    let path_resolver_rows = capture_path_resolver_rows.then(|| rows.clone());
                    (
                        prepare_lix_file_rows(rows, &FilePathPredicate::All),
                        path_resolver_rows,
                        None,
                    )
                };
                let prepared = prepared.map_err(lix_error_to_datafusion_error)?;
                let plugin_render = if prepared.needs_plugin_render(needs_data)
                    || (needs_plugin_ownership && !prepared.file_rows.is_empty())
                {
                    plugin_render_context_for_lix_file_scan(
                        live_state,
                        &request,
                        plugin_host,
                        &prepared,
                        needs_plugin_ownership,
                    )
                    .await
                    .map_err(|error| {
                        DataFusionError::Execution(format!(
                            "sql2 lix_file plugin discovery failed: {error}"
                        ))
                    })?
                    .map(|context| context.with_session_file_views(session_file_views))
                } else {
                    None
                };
                let blob_ref_keys = prepared.blob_rows.keys().cloned().collect();
                let source_batch = lix_file_record_batch_from_prepared(
                    &table_schema,
                    &blob_reader,
                    plugin_render.clone(),
                    needs_data,
                    prepared,
                )
                .await
                .map_err(lix_error_to_datafusion_error)?;
                *captured.lock().expect("lix_file DML source mutex poisoned") =
                    Some(LixFileDmlSourceState {
                        blob_ref_keys,
                        plugin_render,
                        path_resolver_rows,
                        path_index,
                    });
                Ok(source_batch)
            },
        )
    }
}

/// Executes the narrow active-branch point-read shape without constructing a
/// DataFusion catalog and plan. Row selection, branch visibility, blob
/// loading, plugin rendering, and session acknowledgement all stay on the
/// regular `lix_file` provider helpers below.
pub(crate) async fn execute_exact_lix_file_read(
    active_branch_id: &str,
    live_state: Arc<dyn LiveStateReader>,
    filesystem_path_index: Arc<dyn FilesystemPathIndexReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    blob_reader: Arc<dyn BlobDataReader>,
    plugin_host: PluginRuntimeHost,
    session_file_views: Option<SessionFileViews>,
    selector: &ExactLixFileReadSelector,
    column: ExactLixFileReadColumn,
) -> Result<SqlQueryResult, LixError> {
    let base_schema = lix_file_schema();
    let column_index = base_schema.index_of(column.name()).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("exact lix_file projection is missing: {error}"),
        )
    })?;
    let schema = Arc::new(Schema::new(vec![
        base_schema.field(column_index).as_ref().clone(),
    ]));
    let mut request = lix_file_scan_request(Some(active_branch_id), Some(schema.as_ref()), None);
    let branch_binding = BranchBinding::active(active_branch_id);
    request.filter.branch_ids = resolve_provider_branch_ids(
        branch_ref.as_ref(),
        &branch_binding,
        request.filter.branch_ids,
    )
    .await?;

    let index = filesystem_path_index
        .path_index(&FilesystemPathIndexRequest::new(
            request.filter.branch_ids.clone(),
        ))
        .await?;
    let matches = match selector {
        ExactLixFileReadSelector::Id(file_id) => indexed_file_id_matches(
            index,
            &BTreeSet::from([file_id.clone()]),
            &FilePathPredicate::All,
        ),
        ExactLixFileReadSelector::Path(path) => indexed_file_matches(
            index,
            &FilePathPredicate::Comparison {
                operation: FilePathComparison::Equal,
                value: path.clone(),
            },
        ),
    };
    let rows = scan_indexed_file_rows(Arc::clone(&live_state), &request, &matches, true).await?;
    let prepared = prepare_indexed_lix_file_rows(&matches, rows)?;
    let load_data = column == ExactLixFileReadColumn::Data;
    let acknowledge_plugin_data = load_data && session_file_views.is_some();
    let plugin_render = if prepared.needs_plugin_render(load_data) || acknowledge_plugin_data {
        plugin_render_context_for_lix_file_scan(
            live_state,
            &request,
            plugin_host,
            &prepared,
            acknowledge_plugin_data,
        )
        .await?
        .map(|context| context.with_session_file_views(session_file_views))
    } else {
        None
    };
    let batch = lix_file_record_batch_from_prepared(
        &schema,
        &blob_reader,
        plugin_render,
        load_data,
        prepared,
    )
    .await?;
    crate::sql2::exec::datafusion::query_result_from_batches(
        &schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>(),
        &[batch],
    )
}

/// Executes Lixray's exact active-branch file batch without constructing a
/// DataFusion catalog, plan, or Arrow result batch. Keep this separate from
/// the established point-read path so unrelated file queries remain
/// byte-for-byte on their existing implementation.
pub(crate) async fn execute_exact_lix_file_batch_read(
    active_branch_id: &str,
    live_state: Arc<dyn LiveStateReader>,
    filesystem_path_index: Arc<dyn FilesystemPathIndexReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    blob_reader: Arc<dyn BlobDataReader>,
    plugin_host: PluginRuntimeHost,
    session_file_views: Option<SessionFileViews>,
    paths: &BTreeSet<String>,
) -> Result<SqlQueryResult, LixError> {
    let base_schema = lix_file_schema();
    let schema = Arc::new(Schema::new(vec![
        base_schema
            .field_with_name("path")
            .expect("lix_file schema should have path")
            .clone(),
        base_schema
            .field_with_name("data")
            .expect("lix_file schema should have data")
            .clone(),
    ]));
    let mut request = lix_file_scan_request(Some(active_branch_id), Some(schema.as_ref()), None);
    let branch_binding = BranchBinding::active(active_branch_id);
    request.filter.branch_ids = resolve_provider_branch_ids(
        branch_ref.as_ref(),
        &branch_binding,
        request.filter.branch_ids,
    )
    .await?;

    let index = filesystem_path_index
        .path_index(&FilesystemPathIndexRequest::new(
            request.filter.branch_ids.clone(),
        ))
        .await?;
    let matches = indexed_file_matches(index, &FilePathPredicate::In(paths.clone()));
    let rows = scan_indexed_file_rows(Arc::clone(&live_state), &request, &matches, true).await?;
    let prepared = prepare_indexed_lix_file_rows(&matches, rows)?;
    let acknowledge_plugin_data = session_file_views.is_some();
    let plugin_render = if prepared.needs_plugin_render(true) || acknowledge_plugin_data {
        plugin_render_context_for_lix_file_scan(
            live_state,
            &request,
            plugin_host,
            &prepared,
            acknowledge_plugin_data,
        )
        .await?
        .map(|context| context.with_session_file_views(session_file_views))
    } else {
        None
    };

    // No relational operators remain after exact path selection. Move owned
    // blobs into the result instead of packing them into Arrow only for
    // DataFusion to copy them back into row values.
    let rows = exact_path_data_rows_from_prepared(&blob_reader, plugin_render, prepared).await?;
    Ok(SqlQueryResult {
        columns: vec!["path".to_string(), "data".to_string()],
        rows,
        notices: Vec::new(),
    })
}

fn lix_file_dml_source_state(
    captured: &SharedLixFileDmlSourceState,
    action: &str,
) -> Result<LixFileDmlSourceState> {
    captured
        .lock()
        .expect("lix_file DML source mutex poisoned")
        .take()
        .ok_or_else(|| {
            DataFusionError::Execution(format!("lix_file {action} source state missing"))
        })
}

fn path_resolvers_from_dml_source_rows(
    rows: Vec<MaterializedLiveStateRow>,
    active_branch_id: &str,
) -> std::result::Result<BTreeMap<String, DirectoryPathResolver>, LixError> {
    let mut path_resolvers = directory_path_resolvers_from_state_rows(rows)?;
    let resolver_key = filesystem_storage_scope_key(active_branch_id, false, false, None);
    path_resolvers.entry(resolver_key).or_default();
    Ok(path_resolvers)
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
            || ExactStringColumnFilterAnalyzer::new("directory_id").supports(filter)
            || is_null_column_filter(filter, "directory_id")
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
        let needs_blob_rows = scan_needs_blob_rows(&self.schema, projection, &filters);
        let target_file_ids = file_id_constraint_from_filters(&filters)?;
        let target_directory_ids =
            exact_string_column_constraint_from_filters(&filters, "directory_id")?;
        let root_directory_filter = filters
            .iter()
            .any(|filter| is_null_column_filter(filter, "directory_id"));
        let path_predicate = file_path_predicate_from_filters(&filters);
        let indexed_path_predicate = path_predicate
            .clone()
            .and(lower_path_contains_predicate_from_filters(&filters));
        // The path index carries every descriptor column, not just `path`.
        // Prefer it for all descriptor-only scans so queries such as
        // `SELECT id FROM lix_file` and `COUNT(*)` do not materialize the
        // complete descriptor/directory live-state domain on every request.
        // Scans that need file data or the blob revision still load blob rows.
        // A path predicate or exact file/directory ids can narrow those loads
        // to matching cached descriptors instead of scanning complete state.
        let use_path_index = should_use_path_index(&indexed_path_predicate, needs_blob_rows)
            || matches!(&target_file_ids, FileIdConstraint::Ids(_))
            || matches!(&target_directory_ids, FileIdConstraint::Ids(_))
            || root_directory_filter;
        let indexed_matches = if !use_path_index {
            None
        } else {
            let index = self
                .filesystem_path_index
                .path_index(&FilesystemPathIndexRequest::new(
                    request.filter.branch_ids.clone(),
                ))
                .await
                .map_err(lix_error_to_datafusion_error)?;
            let matches = if root_directory_filter {
                indexed_file_root_matches(
                    Arc::clone(&index),
                    &target_file_ids,
                    &indexed_path_predicate,
                )
            } else {
                match (&target_file_ids, &target_directory_ids) {
                    (FileIdConstraint::Ids(file_ids), FileIdConstraint::Ids(directory_ids)) => {
                        indexed_file_directory_matches(
                            Arc::clone(&index),
                            directory_ids,
                            Some(file_ids),
                            &indexed_path_predicate,
                        )
                    }
                    (_, FileIdConstraint::Ids(directory_ids)) => indexed_file_directory_matches(
                        Arc::clone(&index),
                        directory_ids,
                        None,
                        &indexed_path_predicate,
                    ),
                    (FileIdConstraint::Ids(file_ids), _) => indexed_file_id_matches(
                        Arc::clone(&index),
                        file_ids,
                        &indexed_path_predicate,
                    ),
                    (FileIdConstraint::All | FileIdConstraint::None, _) => {
                        indexed_file_matches(Arc::clone(&index), &indexed_path_predicate)
                    }
                }
            };
            let index_scan_threshold =
                2_048_usize.max(index.kind_count(FilesystemPathKind::File) / 1_000);
            if needs_blob_rows && matches.len() > index_scan_threshold {
                None
            } else {
                Some(matches)
            }
        };
        let df_schema = DFSchema::try_from(Arc::clone(&self.schema))?;
        validate_json_predicate_filters(self.schema.as_ref(), &filters)?;
        let physical_filters = filters
            .iter()
            .map(|expr| create_physical_expr(expr, &df_schema, props))
            .collect::<Result<Vec<_>>>()?;
        let ordering = indexed_matches.as_ref().map(|_| "path".to_string());
        Ok(PlannedScan {
            schema: Arc::clone(&projected_schema),
            ordering,
            load: row_source(
                (
                    Arc::clone(&self.live_state),
                    Arc::clone(&self.blob_reader),
                    self.plugin_host.clone(),
                    Arc::clone(&self.schema),
                    Arc::clone(&projected_schema),
                    projection.cloned(),
                    request,
                    target_file_ids,
                    path_predicate,
                    indexed_matches,
                    physical_filters,
                    self.session_file_views.clone(),
                    needs_data,
                    needs_blob_rows,
                    limit,
                ),
                |(
                    live_state,
                    blob_reader,
                    plugin_host,
                    batch_schema,
                    projected_schema,
                    projection,
                    request,
                    target_file_ids,
                    path_predicate,
                    indexed_matches,
                    filters,
                    session_file_views,
                    needs_data,
                    needs_blob_rows,
                    limit,
                )| async move {
                    if let Some(indexed_matches) = indexed_matches.as_ref()
                        && !needs_blob_rows
                    {
                        // Without residual filters, the path-index order is the
                        // output order. Materialize only projected columns and
                        // stop at the scan limit before building Arrow arrays.
                        // Filtered scans still need their full input schema and
                        // must apply LIMIT after evaluating every predicate.
                        let (materialization_schema, materialization_limit) = if filters.is_empty()
                        {
                            (&projected_schema, limit)
                        } else {
                            (&batch_schema, None)
                        };
                        let batch = lix_file_record_batch_from_path_selection(
                            materialization_schema,
                            indexed_matches,
                            materialization_limit,
                        )
                        .map_err(|error| {
                            DataFusionError::Execution(format!(
                                "sql2 indexed lix_file batch build failed: {error}"
                            ))
                        })?;
                        return finish_scan_batch(
                            batch,
                            &filters,
                            if filters.is_empty() {
                                None
                            } else {
                                projection.as_deref()
                            },
                            if filters.is_empty() { None } else { limit },
                            "lix_file",
                        );
                    }
                    let prepared = if let Some(indexed_matches) = indexed_matches.as_ref() {
                        let rows = scan_indexed_file_rows(
                            Arc::clone(&live_state),
                            &request,
                            indexed_matches,
                            needs_blob_rows,
                        )
                        .await
                        .map_err(|error| {
                            DataFusionError::Execution(format!(
                                "sql2 indexed lix_file scan failed: {error}"
                            ))
                        })?;
                        prepare_indexed_lix_file_rows(indexed_matches, rows)
                    } else {
                        let rows = scan_lix_file_live_rows(
                            Arc::clone(&live_state),
                            &request,
                            &target_file_ids,
                        )
                        .await
                        .map_err(|error| {
                            DataFusionError::Execution(format!(
                                "sql2 lix_file scan failed: {error}"
                            ))
                        })?;
                        prepare_lix_file_rows(rows, &path_predicate)
                    }
                    .map_err(|error| {
                        DataFusionError::Execution(format!(
                            "sql2 lix_file row preparation failed: {error}"
                        ))
                    })?;
                    let plugin_render = if prepared.needs_plugin_render(needs_data) {
                        plugin_render_context_for_lix_file_scan(
                            Arc::clone(&live_state),
                            &request,
                            plugin_host,
                            &prepared,
                            false,
                        )
                        .await
                        .map_err(|error| {
                            DataFusionError::Context(
                                "sql2 lix_file plugin discovery failed".to_string(),
                                Box::new(lix_error_to_datafusion_error(error)),
                            )
                        })?
                        .map(|context| context.with_session_file_views(session_file_views))
                    } else {
                        None
                    };
                    let batch = lix_file_record_batch_from_prepared(
                        &batch_schema,
                        &blob_reader,
                        plugin_render,
                        needs_data,
                        prepared,
                    )
                    .await
                    .map_err(|error| {
                        DataFusionError::Context(
                            "sql2 lix_file batch build failed".to_string(),
                            Box::new(lix_error_to_datafusion_error(error)),
                        )
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
        self.plan_delete_with_options(write_ctx, filters, DmlPlanOptions::default())
            .await
    }

    async fn plan_delete_with_options(
        &self,
        write_ctx: SqlWriteContext,
        filters: &[Expr],
        options: DmlPlanOptions,
    ) -> Result<PlannedDml> {
        let plugin_archive_delete_target =
            exact_plugin_archive_delete_target_from_filters(filters)?;
        let needs_data = filters.iter().any(|filter| contains_column(filter, "data"))
            || options.returning_columns.contains("data");
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
        let indexed_matches = self
            .indexed_dml_matches(&request, filters, &target_file_ids)
            .await?;

        let captured: SharedLixFileDmlSourceState = Arc::new(Mutex::new(None));
        let source = self.dml_source(
            &write_ctx,
            request,
            target_file_ids,
            indexed_matches,
            needs_data,
            false,
            false,
            Arc::clone(&captured),
        );
        let branch_binding = self.branch_binding.clone();
        let apply: DmlApply = Arc::new(move |matched_batch| {
            let write_ctx = write_ctx.clone();
            let branch_binding = branch_binding.clone();
            let captured = Arc::clone(&captured);
            let plugin_archive_delete_target = plugin_archive_delete_target.clone();
            async move {
                let source_state = lix_file_dml_source_state(&captured, "DELETE")?;
                let staged = lix_file_delete_stage_from_batch(
                    &matched_batch,
                    branch_binding.active_branch_id(),
                    &source_state.blob_ref_keys,
                    plugin_archive_delete_target.as_deref(),
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
        let indexed_matches = self
            .indexed_dml_matches(&request, filters, &target_file_ids)
            .await?;

        let update_columns = LixFileUpdateColumns::from_assignments(&assignments);
        let capture_path_resolver_rows = update_columns.requires_path_resolver()
            && matches!(
                (&self.branch_binding, &target_file_ids),
                (BranchBinding::Active { .. }, FileIdConstraint::All)
            );
        let captured: SharedLixFileDmlSourceState = Arc::new(Mutex::new(None));
        let source = self.dml_source(
            &write_ctx,
            request,
            target_file_ids,
            indexed_matches,
            needs_data,
            update_columns.updates_path() && !update_columns.data,
            capture_path_resolver_rows,
            Arc::clone(&captured),
        );
        let branch_binding = self.branch_binding.clone();
        let functions = self.functions.clone();
        let apply: DmlApply = Arc::new(move |matched_batch| {
            let write_ctx = write_ctx.clone();
            let branch_binding = branch_binding.clone();
            let functions = functions.clone();
            let assignments = assignments.clone();
            let captured = Arc::clone(&captured);
            async move {
                let LixFileDmlSourceState {
                    blob_ref_keys,
                    plugin_render,
                    path_resolver_rows,
                    path_index,
                } = lix_file_dml_source_state(&captured, "UPDATE")?;
                let assignment_values =
                    UpdateAssignmentValues::evaluate(&matched_batch, &assignments)?;
                let plugin_rewrite_file_ids =
                    if update_columns.updates_path() && !update_columns.data {
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
                if update_columns.requires_path_resolver() {
                    path_resolvers = Some(if let Some(path_index) = path_index {
                        directory_path_resolvers_from_path_index(
                            path_index.index(),
                            branch_binding.active_branch_id(),
                        )
                        .map_err(lix_error_to_datafusion_error)?
                    } else if let (Some(rows), Some(active_branch_id)) =
                        (path_resolver_rows, branch_binding.active_branch_id())
                    {
                        path_resolvers_from_dml_source_rows(rows, active_branch_id)
                            .map_err(lix_error_to_datafusion_error)?
                    } else {
                        directory_path_resolvers_from_live_state(
                            Arc::new(WriteContextLiveStateReader::new(write_ctx.clone())),
                            branch_binding.active_branch_id(),
                        )
                        .await
                        .map_err(lix_error_to_datafusion_error)?
                    });
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
        // Existing rows matching the proposed conflict identity, rendered as
        // a full `lix_file` batch (with materialized `data`) so the driver can
        // build the augmented `excluded.*` batch the conflict assignments run
        // over.
        let (target_file_ids, path_predicate) = match target.kind() {
            UpsertConflictKind::Id => (
                proposed_file_id_constraint(proposed)?,
                FilePathPredicate::All,
            ),
            UpsertConflictKind::Path => (
                FileIdConstraint::All,
                proposed_file_path_predicate(proposed)?,
            ),
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

        let indexed_matches = if target.kind() == UpsertConflictKind::Path {
            let index = self
                .filesystem_path_index
                .path_index(&FilesystemPathIndexRequest::new(
                    request.filter.branch_ids.clone(),
                ))
                .await
                .map_err(lix_error_to_datafusion_error)?;
            Some(indexed_file_matches(index, &path_predicate))
        } else {
            self.indexed_dml_matches(&request, &[], &target_file_ids)
                .await?
        };

        let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx.clone()));
        let prepared = if let Some(indexed_matches) = indexed_matches.as_ref() {
            // Conflict probes only need the proposed exact IDs or paths. Use
            // the visible filesystem index for descriptor matching, then fetch
            // correlated blob refs solely for those files.
            let rows = match &target_file_ids {
                FileIdConstraint::Ids(file_ids) => {
                    scan_exact_file_blob_rows(live_state.clone(), &request, file_ids).await
                }
                FileIdConstraint::All | FileIdConstraint::None => {
                    scan_indexed_file_rows(live_state.clone(), &request, indexed_matches, true)
                        .await
                }
            }
            .map_err(lix_error_to_datafusion_error)?;
            prepare_indexed_lix_file_rows(indexed_matches, rows)
        } else {
            let rows = scan_lix_file_live_rows(live_state.clone(), &request, &target_file_ids)
                .await
                .map_err(lix_error_to_datafusion_error)?;
            prepare_lix_file_rows(rows, &FilePathPredicate::All)
        }
        .map_err(lix_error_to_datafusion_error)?;
        let plugin_render = if prepared.needs_plugin_render(true) {
            plugin_render_context_for_lix_file_scan(
                live_state,
                &request,
                self.plugin_host.clone(),
                &prepared,
                false,
            )
            .await
            .map_err(|error| {
                DataFusionError::Execution(format!(
                    "sql2 lix_file plugin discovery failed: {error}"
                ))
            })?
        } else {
            None
        };
        lix_file_record_batch_from_prepared(
            &self.schema,
            &self.blob_reader,
            plugin_render,
            true,
            prepared,
        )
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

        let live_state: Arc<dyn LiveStateReader> =
            Arc::new(WriteContextLiveStateReader::new(write_ctx.clone()));
        // The augmented conflict batch already carries the selected
        // descriptors. Recover only their correlated blob refs; rebuilding
        // the path index here would duplicate the conflict probe's topology
        // read, especially for path-based upserts.
        let rows = match &target_file_ids {
            FileIdConstraint::Ids(file_ids) => {
                scan_exact_file_blob_rows(live_state.clone(), &request, file_ids).await
            }
            FileIdConstraint::All | FileIdConstraint::None => {
                scan_lix_file_live_rows(live_state.clone(), &request, &target_file_ids).await
            }
        }
        .map_err(lix_error_to_datafusion_error)?;
        let blob_ref_keys =
            blob_ref_keys_from_live_rows(&rows).map_err(lix_error_to_datafusion_error)?;

        let plugin_rewrite_file_ids = if update_columns.updates_path() && !update_columns.data {
            let plugin_host = self.plugin_host.clone();
            let branches =
                load_plugin_render_branches(Arc::clone(&live_state), &request, &plugin_host)
                    .await
                    .map_err(|error| {
                        DataFusionError::Execution(format!(
                            "sql2 lix_file plugin discovery failed: {error}"
                        ))
                    })?;
            let plugin_render = if branches.is_empty() {
                None
            } else {
                plugin_render_context_with_branches(
                    live_state.clone(),
                    plugin_host,
                    branches,
                    plugin_owner_candidates_from_batch(augmented, branch_binding)?,
                    true,
                )
                .await
                .map_err(|error| {
                    DataFusionError::Execution(format!(
                        "sql2 lix_file plugin discovery failed: {error}"
                    ))
                })?
            };
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
        if update_columns.requires_path_resolver() {
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

/// The exact paths whose existing rows can conflict with a proposed
/// `INSERT .. ON CONFLICT (path)` batch.
fn proposed_file_path_predicate(batch: &RecordBatch) -> Result<FilePathPredicate> {
    validate_required_paths(batch, "lix_file")?;
    let paths = (0..batch.num_rows())
        .map(|row_index| required_string_value(batch, row_index, "path"))
        .collect::<Result<BTreeSet<_>>>()?;
    Ok(FilePathPredicate::In(paths))
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
                staged
                    .extend(lix_file_insert_stage_from_batch_with_path_resolvers(
                        &batch,
                        self.branch_binding.active_branch_id(),
                        self.surface_name,
                        path_resolvers
                            .as_mut()
                            .expect("path resolver should be initialized"),
                        &mut || self.functions.call_uuid_v7().to_string(),
                        self.include_data_writes,
                    )?)
                    .map_err(lix_error_to_datafusion_error)?;
            } else {
                staged
                    .extend(
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
                    )
                    .map_err(lix_error_to_datafusion_error)?;
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
    fn row_context(&self) -> FilesystemRowContext {
        FilesystemRowContext {
            branch_id: self.live.branch_id.clone(),
            global: self.live.global,
            untracked: self.live.untracked,
            file_id: self.live.file_id.clone(),
            metadata: None,
        }
    }

    fn directory_parent_keys(&self, directory_id: &str) -> Vec<FilesystemDescriptorKey> {
        let mut keys = vec![self.key.in_same_scope(directory_id)];
        if self.key.is_untracked() {
            keys.push(self.key.in_tracked_scope(directory_id));
        }
        keys
    }

    fn blob_ref_key(&self) -> FilesystemBlobRefKey {
        FilesystemBlobRefKey::from_context(&self.row_context(), &self.id)
    }
}

#[derive(Clone)]
struct PluginRenderContext {
    live_state: Arc<dyn LiveStateReader>,
    host: PluginRuntimeHost,
    branches: BTreeMap<String, BranchPluginRenderContext>,
    owners_by_file: BTreeMap<FilesystemDescriptorKey, PluginFileOwner>,
    owner_change_ids_by_file: BTreeMap<FilesystemDescriptorKey, String>,
    session_file_views: Option<SessionFileViews>,
}

#[derive(Clone)]
struct BranchPluginRenderContext {
    registry: PluginRegistry,
    catalog: Arc<CompiledPluginCatalog>,
}

impl PluginRenderContext {
    fn branch(&self, branch_id: &str) -> Option<&BranchPluginRenderContext> {
        self.branches.get(branch_id)
    }

    fn owner_for_file(&self, key: &FilesystemDescriptorKey) -> Option<&PluginFileOwner> {
        self.owners_by_file.get(key)
    }

    fn owner_change_id_for_file(&self, key: &FilesystemDescriptorKey) -> Option<&str> {
        self.owner_change_ids_by_file.get(key).map(String::as_str)
    }

    fn with_session_file_views(mut self, session_file_views: Option<SessionFileViews>) -> Self {
        self.session_file_views = session_file_views;
        self
    }
}

#[derive(Debug, Clone)]
struct BlobRefRecord {
    blob_hash: String,
    live: MaterializedLiveStateRow,
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
    fn extend(&mut self, other: Self) -> std::result::Result<(), LixError> {
        self.state_rows.extend(other.state_rows);
        self.file_data_writes.extend(other.file_data_writes);
        self.add_count(other.count)
    }

    fn extend_filesystem_plan(
        &mut self,
        plan: crate::filesystem::FilesystemWritePlan,
    ) -> std::result::Result<(), LixError> {
        self.state_rows.extend(plan.rows);
        self.file_data_writes.extend(plan.file_data);
        self.add_count(plan.count)
    }

    fn extend_filesystem_delete_plan(
        &mut self,
        plan: FilesystemDeletePlan,
    ) -> std::result::Result<(), LixError> {
        self.state_rows.extend(plan.rows);
        self.add_count(plan.count)
    }

    fn add_count(&mut self, count: u64) -> std::result::Result<(), LixError> {
        self.count = self.count.checked_add(count).ok_or_else(|| {
            LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "lix_file fast write row count overflow",
            )
        })?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FastLixFilePathWriteConflict {
    None,
    DoNothing,
    UpdateData,
    UpdateDataAndMetadata,
}

pub(crate) async fn execute_fast_lix_file_path_writes(
    ctx: &mut dyn SqlWriteExecutionContext,
    writes: Vec<(
        String,
        Vec<u8>,
        Option<TransactionJson>,
        Option<RequestBlobSpliceProvenance>,
    )>,
    conflict: FastLixFilePathWriteConflict,
    mutation_identity: Option<MutationIdentity>,
) -> Result<Option<u64>, LixError> {
    if writes.is_empty() {
        return Ok(Some(0));
    }

    let active_branch_id = ctx.active_branch_id().to_string();
    let parsed_writes = parse_fast_lix_file_path_writes(writes)?;

    if matches!(
        conflict,
        FastLixFilePathWriteConflict::UpdateData
            | FastLixFilePathWriteConflict::UpdateDataAndMetadata
    ) && let Some(indexed) =
        indexed_file_path_writes(ctx, &active_branch_id, &parsed_writes).await?
    {
        return stage_indexed_file_path_writes(
            ctx,
            &active_branch_id,
            parsed_writes,
            indexed,
            conflict,
            mutation_identity,
        )
        .await
        .map(Some);
    }

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
    let filesystem = match FilesystemIndex::from_live_rows(live_rows.clone()) {
        Ok(filesystem) => filesystem,
        // The legacy write index intentionally rejects visible path collisions
        // across storage scopes, while the general provider can disambiguate
        // them. No writes have been staged yet, so decline the fast route and
        // let the caller execute the original DataFusion plan.
        Err(error) if error.code == LixError::CODE_CONSTRAINT_VIOLATION => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut path_resolvers = directory_path_resolvers_from_state_rows(live_rows)?;
    let resolver_key = filesystem_storage_scope_key(&active_branch_id, false, false, None);
    path_resolvers.entry(resolver_key).or_default();
    let mut staged = LixFileStagedBatch::default();

    for write in parsed_writes {
        if let Some(existing) = filesystem.file_entry(&write.parsed.path).cloned() {
            if conflict != FastLixFilePathWriteConflict::None {
                validate_fast_lix_file_path_conflict_pair(
                    existing.scope.untracked,
                    &write.parsed.path,
                )?;
            }
            match conflict {
                FastLixFilePathWriteConflict::None => {
                    let context = FilesystemRowContext {
                        branch_id: active_branch_id.clone(),
                        global: false,
                        untracked: false,
                        file_id: None,
                        metadata: write.metadata,
                    };
                    let plan = plan_parsed_file_path_write_with_resolvers(
                        &mut path_resolvers,
                        write.parsed.parsed_path,
                        Some(
                            write
                                .parsed
                                .plugin_key
                                .as_deref()
                                .map(plugin_storage_archive_file_id)
                                .unwrap_or_else(|| ctx.functions().call_uuid_v7().to_string()),
                        ),
                        Some(write.data),
                        context,
                        &mut || ctx.functions().call_uuid_v7().to_string(),
                    )?;
                    staged.extend_filesystem_plan(plan)?;
                }
                FastLixFilePathWriteConflict::DoNothing => {}
                FastLixFilePathWriteConflict::UpdateData => {
                    let mut context = existing.scope.context(Some(existing.id.clone()));
                    if context.global {
                        context.branch_id = GLOBAL_BRANCH_ID.to_string();
                    }
                    let file_data_start = staged.file_data_writes.len();
                    stage_lix_file_data_update_write(
                        &mut staged,
                        existing.id.clone(),
                        Some(write.parsed.path),
                        Some(existing.name.clone()),
                        write.data,
                        context,
                        existing.blob_hash.is_some(),
                        None,
                    )
                    .map_err(crate::sql2::error::datafusion_error_to_lix_error)?;
                    attach_fast_file_write_metadata(
                        &mut staged.file_data_writes[file_data_start..],
                        write.splice_provenance,
                        mutation_identity,
                    );
                    staged.add_count(1)?;
                }
                FastLixFilePathWriteConflict::UpdateDataAndMetadata => {
                    let mut context = existing.scope.context(None);
                    if context.global {
                        context.branch_id = GLOBAL_BRANCH_ID.to_string();
                    }
                    context.metadata = write.metadata;
                    staged
                        .state_rows
                        .push(file_descriptor_row(FileDescriptorRowInput {
                            id: existing.id.clone(),
                            directory_id: existing.directory_id.clone(),
                            name: existing.name.clone(),
                            context: context.clone(),
                        }));
                    let file_data_start = staged.file_data_writes.len();
                    stage_lix_file_data_update_write(
                        &mut staged,
                        existing.id.clone(),
                        Some(write.parsed.path),
                        Some(existing.name.clone()),
                        write.data,
                        context,
                        existing.blob_hash.is_some(),
                        None,
                    )
                    .map_err(crate::sql2::error::datafusion_error_to_lix_error)?;
                    attach_fast_file_write_metadata(
                        &mut staged.file_data_writes[file_data_start..],
                        write.splice_provenance,
                        mutation_identity,
                    );
                    staged.add_count(1)?;
                }
            }
        } else {
            let context = FilesystemRowContext {
                branch_id: active_branch_id.clone(),
                global: false,
                untracked: false,
                file_id: None,
                metadata: write.metadata,
            };
            let file_id = write
                .parsed
                .plugin_key
                .as_deref()
                .map(plugin_storage_archive_file_id)
                .unwrap_or_else(|| ctx.functions().call_uuid_v7().to_string());
            let mut plan = plan_parsed_file_path_write_with_resolvers(
                &mut path_resolvers,
                write.parsed.parsed_path,
                Some(file_id.clone()),
                Some(write.data),
                context,
                &mut || ctx.functions().call_uuid_v7().to_string(),
            )?;
            attach_fast_file_write_metadata(
                &mut plan.file_data,
                write.splice_provenance,
                mutation_identity,
            );
            attach_lix_file_insert_origin(&mut plan.rows, "lix_file", &file_id);
            staged.extend_filesystem_plan(plan)?;
        }
    }

    let mode = match conflict {
        FastLixFilePathWriteConflict::None => TransactionWriteMode::Insert,
        FastLixFilePathWriteConflict::DoNothing
        | FastLixFilePathWriteConflict::UpdateData
        | FastLixFilePathWriteConflict::UpdateDataAndMetadata => TransactionWriteMode::Replace,
    };
    stage_lix_file_fast_batch(ctx, mode, staged).await.map(Some)
}

struct IndexedFilePathWrites {
    existing: Vec<Option<Arc<FilesystemPathEntry>>>,
    path_resolvers: Option<BTreeMap<String, DirectoryPathResolver>>,
}

async fn indexed_file_path_writes(
    ctx: &mut dyn SqlWriteExecutionContext,
    active_branch_id: &str,
    writes: &[FastLixFilePathWrite],
) -> Result<Option<IndexedFilePathWrites>, LixError> {
    let index = ctx
        .filesystem_path_index(&FilesystemPathIndexRequest::new(vec![
            active_branch_id.to_string(),
        ]))
        .await?;
    let mut existing = Vec::with_capacity(writes.len());
    for write in writes {
        let entries = index.exact_entries(&write.parsed.path);
        match entries.as_slice() {
            [] => {
                if write.parsed.parsed_path.segments().count() == 1
                    && !index
                        .exact_entries(&format!("{}/", write.parsed.path))
                        .is_empty()
                {
                    return Ok(None);
                }
                existing.push(None);
            }
            [entry] if entry.kind == FilesystemPathKind::File => {
                existing.push(Some(Arc::clone(entry)));
            }
            _ => return Ok(None),
        }
    }
    let has_missing = existing.iter().any(Option::is_none);
    let has_missing_nested = writes
        .iter()
        .zip(&existing)
        .any(|(write, entry)| entry.is_none() && write.parsed.parsed_path.segments().count() > 1);
    let path_resolvers = if has_missing_nested {
        match directory_path_resolvers_from_path_index(&index, Some(active_branch_id)) {
            Ok(resolvers) => Some(resolvers),
            Err(error) if error.code == LixError::CODE_CONSTRAINT_VIOLATION => return Ok(None),
            Err(error) => return Err(error),
        }
    } else if has_missing {
        Some(BTreeMap::from([(
            filesystem_storage_scope_key(active_branch_id, false, false, None),
            DirectoryPathResolver::default(),
        )]))
    } else {
        None
    };
    Ok(Some(IndexedFilePathWrites {
        existing,
        path_resolvers,
    }))
}

async fn stage_indexed_file_path_writes(
    ctx: &mut dyn SqlWriteExecutionContext,
    active_branch_id: &str,
    writes: Vec<FastLixFilePathWrite>,
    mut indexed: IndexedFilePathWrites,
    conflict: FastLixFilePathWriteConflict,
    mutation_identity: Option<MutationIdentity>,
) -> Result<u64, LixError> {
    debug_assert_eq!(writes.len(), indexed.existing.len());
    debug_assert!(matches!(
        conflict,
        FastLixFilePathWriteConflict::UpdateData
            | FastLixFilePathWriteConflict::UpdateDataAndMetadata
    ));
    for (write, entry) in writes.iter().zip(&indexed.existing) {
        if let Some(entry) = entry {
            validate_fast_lix_file_path_conflict_pair(
                entry.key.is_untracked(),
                &write.parsed.path,
            )?;
        }
    }
    let existing = indexed
        .existing
        .iter()
        .filter_map(|entry| entry.as_ref().map(Arc::clone))
        .collect::<Vec<_>>();
    let blob_backed = load_exact_existing_blob_keys(ctx, &existing).await?;
    let mut staged = LixFileStagedBatch::default();

    for (write, entry) in writes.into_iter().zip(indexed.existing) {
        if let Some(entry) = entry {
            let has_blob_ref = blob_backed.contains(&entry.key);
            let mut context = FilesystemRowContext {
                branch_id: entry.key.branch_id().to_string(),
                global: entry.key.global(),
                untracked: entry.key.is_untracked(),
                file_id: entry.key.file_id().map(str::to_string),
                metadata: None,
            };
            if context.global {
                context.branch_id = GLOBAL_BRANCH_ID.to_string();
            }
            match conflict {
                FastLixFilePathWriteConflict::UpdateData => {
                    context.file_id = Some(entry.id().to_string());
                }
                FastLixFilePathWriteConflict::UpdateDataAndMetadata => {
                    let metadata_changed = entry.metadata()
                        != write.metadata.as_ref().map(TransactionJson::normalized);
                    context.metadata = write.metadata;
                    if metadata_changed {
                        staged
                            .state_rows
                            .push(file_descriptor_row(FileDescriptorRowInput {
                                id: entry.id().to_string(),
                                directory_id: entry.parent_id.clone(),
                                name: entry.name.clone(),
                                context: context.clone(),
                            }));
                    }
                }
                FastLixFilePathWriteConflict::None | FastLixFilePathWriteConflict::DoNothing => {
                    unreachable!("indexed path route only handles conflict updates")
                }
            }
            let file_data_start = staged.file_data_writes.len();
            stage_lix_file_data_update_write(
                &mut staged,
                entry.id().to_string(),
                Some(write.parsed.path),
                Some(entry.name.clone()),
                write.data,
                context,
                has_blob_ref,
                None,
            )
            .map_err(crate::sql2::error::datafusion_error_to_lix_error)?;
            attach_fast_file_write_metadata(
                &mut staged.file_data_writes[file_data_start..],
                write.splice_provenance,
                mutation_identity,
            );
            staged.add_count(1)?;
        } else {
            let context = FilesystemRowContext {
                branch_id: active_branch_id.to_string(),
                global: false,
                untracked: false,
                file_id: None,
                metadata: write.metadata,
            };
            let file_id = write
                .parsed
                .plugin_key
                .as_deref()
                .map(plugin_storage_archive_file_id)
                .unwrap_or_else(|| ctx.functions().call_uuid_v7().to_string());
            let mut plan = plan_parsed_file_path_write_with_resolvers(
                indexed
                    .path_resolvers
                    .as_mut()
                    .expect("missing indexed path should have directory resolvers"),
                write.parsed.parsed_path,
                Some(file_id.clone()),
                Some(write.data),
                context,
                &mut || ctx.functions().call_uuid_v7().to_string(),
            )?;
            attach_fast_file_write_metadata(
                &mut plan.file_data,
                write.splice_provenance,
                mutation_identity,
            );
            attach_lix_file_insert_origin(&mut plan.rows, "lix_file", &file_id);
            staged.extend_filesystem_plan(plan)?;
        }
    }

    stage_lix_file_fast_batch(ctx, TransactionWriteMode::Replace, staged).await
}

async fn load_exact_existing_blob_keys(
    ctx: &mut dyn SqlWriteExecutionContext,
    entries: &[Arc<FilesystemPathEntry>],
) -> Result<BTreeSet<FilesystemDescriptorKey>, LixError> {
    if entries.is_empty() {
        return Ok(BTreeSet::new());
    }
    let unique = entries
        .iter()
        .map(|entry| (entry.key.clone(), Arc::clone(entry)))
        .collect::<BTreeMap<_, _>>();
    let request = LiveStateExactBatchRequest {
        rows: unique
            .values()
            .map(|entry| LiveStateExactRowRequest {
                branch_id: entry.key.branch_id().to_string(),
                schema_key: BLOB_REF_SCHEMA_KEY.to_string(),
                entity_pk: EntityPk::single(entry.id()),
                file_id: Some(entry.id().to_string()),
            })
            .collect(),
        projection: LiveStateProjection::default(),
        untracked: Some(false),
        include_tombstones: false,
    };
    let rows = ctx.load_exact_live_state_rows(&request).await?;
    Ok(unique
        .into_keys()
        .zip(rows)
        .filter_map(|(key, row)| {
            row.filter(|row| {
                row.branch_id == key.branch_id()
                    && row.global == key.global()
                    && row.untracked == key.is_untracked()
            })
            .map(|_| key)
        })
        .collect())
}

pub(crate) async fn execute_fast_lix_file_data_update_by_id(
    ctx: &mut dyn SqlWriteExecutionContext,
    file_id: Option<String>,
    data: Vec<u8>,
    splice_provenance: Option<RequestBlobSpliceProvenance>,
    mutation_identity: Option<MutationIdentity>,
) -> Result<u64, LixError> {
    let active_branch_id = ctx.active_branch_id().to_string();
    ctx.load_branch_head(&active_branch_id)
        .await?
        .ok_or_else(|| {
            LixError::branch_not_found(
                active_branch_id.clone(),
                "execute bound public write",
                "active branch",
            )
        })?;
    let Some(file_id) = file_id else {
        return Ok(0);
    };
    // The revisioned path index contains every visible descriptor together with
    // its already-derived path. Reuse it instead of scanning every directory
    // descriptor just to reconstruct this one file's path.
    let index = ctx
        .filesystem_path_index(&FilesystemPathIndexRequest::new(vec![
            active_branch_id.clone(),
        ]))
        .await?;
    let target_file_ids = BTreeSet::from([file_id.clone()]);
    let indexed_matches = indexed_file_id_matches(index, &target_file_ids, &FilePathPredicate::All);

    // Blob references are not part of the descriptor index and can change
    // without a path-index revision, so load only this file's current blobs.
    let mut blob_request = lix_file_scan_request(Some(&active_branch_id), None, None);
    blob_request.filter.schema_keys = vec![BLOB_REF_SCHEMA_KEY.to_string()];
    blob_request.filter.entity_pks = vec![EntityPk::single(file_id.clone())];
    let rows = ctx.scan_live_state(&blob_request).await?;

    let PreparedLixFileRows {
        file_rows,
        blob_rows,
        file_paths,
        ..
    } = prepare_indexed_lix_file_rows(&indexed_matches, rows)?;
    let existing = file_rows
        .into_iter()
        .filter(|(_, file)| file.id == file_id)
        .map(|(key, file)| {
            let path = file_paths
                .get(&key)
                .cloned()
                .expect("prepared lix_file descriptor should have a path");
            let has_blob_ref = blob_rows.contains_key(&file.blob_ref_key());
            (path, file, has_blob_ref)
        })
        .collect::<Vec<_>>();
    if existing.is_empty() {
        return Ok(0);
    }

    let mut staged = LixFileStagedBatch::default();
    for (path, existing, has_blob_ref) in existing {
        parse_file_upsert_path(&path, TransactionWriteOperation::Update)
            .map_err(crate::sql2::error::datafusion_error_to_lix_error)?;
        let mut context = existing.row_context();
        if context.global {
            context.branch_id = GLOBAL_BRANCH_ID.to_string();
        }
        stage_lix_file_data_update_write(
            &mut staged,
            existing.id,
            Some(path),
            Some(existing.name),
            data.clone(),
            context,
            has_blob_ref,
            None,
        )
        .map_err(crate::sql2::error::datafusion_error_to_lix_error)?;
        if let Some(file_data) = staged.file_data_writes.last_mut() {
            file_data.set_splice_provenance(splice_provenance.clone());
            file_data.set_mutation_identity(mutation_identity);
        }
        staged.add_count(1)?;
    }
    stage_lix_file_fast_batch(ctx, TransactionWriteMode::Replace, staged).await
}

struct FastLixFilePathWrite {
    parsed: ParsedFileWritePath,
    data: Vec<u8>,
    metadata: Option<TransactionJson>,
    splice_provenance: Option<RequestBlobSpliceProvenance>,
}

fn parse_fast_lix_file_path_writes(
    writes: Vec<(
        String,
        Vec<u8>,
        Option<TransactionJson>,
        Option<RequestBlobSpliceProvenance>,
    )>,
) -> std::result::Result<Vec<FastLixFilePathWrite>, LixError> {
    writes
        .into_iter()
        .map(|(path, data, metadata, splice_provenance)| {
            Ok(FastLixFilePathWrite {
                parsed: parse_file_upsert_path(&path, TransactionWriteOperation::Insert)
                    .map_err(crate::sql2::error::datafusion_error_to_lix_error)?,
                data,
                metadata,
                splice_provenance,
            })
        })
        .collect()
}

fn attach_fast_file_write_metadata(
    file_data: &mut [TransactionFileData],
    splice_provenance: Option<RequestBlobSpliceProvenance>,
    mutation_identity: Option<MutationIdentity>,
) {
    for file_data in file_data {
        file_data.set_splice_provenance(splice_provenance.clone());
        file_data.set_mutation_identity(mutation_identity);
    }
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
    plugin_archive_delete_target: Option<&str>,
) -> Result<LixFileStagedBatch> {
    let mut staged = LixFileStagedBatch::default();
    for row_index in 0..batch.num_rows() {
        let file_id = required_string_value(batch, row_index, "id")?;
        let path = optional_string_value(batch, row_index, "path")?;
        validate_lix_file_delete_target(path.as_deref(), &file_id, plugin_archive_delete_target)?;
        let context = file_row_context_from_batch(batch, row_index, branch_binding)?;
        staged
            .extend_filesystem_delete_plan(plan_file_delete(FileDeleteInput {
                file_id: file_id.clone(),
                has_blob_ref: blob_ref_keys
                    .contains(&FilesystemBlobRefKey::from_context(&context, &file_id)),
                context,
            }))
            .map_err(lix_error_to_datafusion_error)?;
    }
    Ok(staged)
}

fn validate_lix_file_delete_target(
    path: Option<&str>,
    file_id: &str,
    plugin_archive_delete_target: Option<&str>,
) -> Result<()> {
    let archive_id_plugin_key = plugin_key_from_archive_file_id(file_id);
    let Some(path) = path else {
        if archive_id_plugin_key.is_some() {
            return Err(rejected_plugin_archive_delete_error(None, file_id));
        }
        return Ok(());
    };
    LixPath::try_from_file_path(path).map_err(lix_error_to_datafusion_error)?;
    let archive_path_plugin_key = plugin_key_from_archive_path(path);
    if !is_plugin_storage_path(path) && archive_id_plugin_key.is_none() {
        return Ok(());
    }

    match (
        archive_path_plugin_key.as_deref(),
        archive_id_plugin_key.as_deref(),
        plugin_archive_delete_target,
    ) {
        (Some(path_key), Some(id_key), Some(target_key))
            if path_key == id_key && id_key == target_key =>
        {
            Ok(())
        }
        _ => Err(rejected_plugin_archive_delete_error(Some(path), file_id)),
    }
}

fn rejected_plugin_archive_delete_error(path: Option<&str>, file_id: &str) -> DataFusionError {
    lix_error_to_datafusion_error(
        LixError::new(
            LixError::CODE_CONSTRAINT_VIOLATION,
            format!(
                "DELETE FROM lix_file may only uninstall one exact canonical plugin archive; got path {path:?} and file id {file_id:?}"
            ),
        )
        .with_hint(
            "Delete one canonical /.lix/plugins/<key>.lixplugin path or its deterministic archive file ID.",
        ),
    )
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
    // Descriptor attributes retain the existing materialized public path. A
    // resolver is only needed when a write can alter the directory graph. In
    // particular, metadata is stored on the descriptor but cannot change its
    // path, so a content-and-metadata overwrite can use the matched row's
    // already materialized path for downstream file-data handling.
    let reuse_materialized_path = include_descriptor_writes && path_resolvers.is_none();
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
            let data_filename = match data_filename {
                Some(filename) => Some(filename),
                None if batch.schema().index_of("name").is_ok() => {
                    optional_string_value(batch, row_index, "name")?
                }
                None => None,
            };
            let path = if include_descriptor_writes {
                match data_path {
                    Some(path) => Some(path),
                    None if reuse_materialized_path => {
                        Some(required_string_value(batch, row_index, "path")?)
                    }
                    None => None,
                }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LixFileDescriptorUpdate {
    None,
    Attributes,
    Topology,
    Path,
}

#[derive(Debug, Clone, Copy)]
struct LixFileUpdateColumns {
    data: bool,
    descriptor: LixFileDescriptorUpdate,
}

impl LixFileUpdateColumns {
    fn from_assignments(assignments: &[(String, Arc<dyn PhysicalExpr>)]) -> Self {
        let mut impact = Self {
            data: false,
            descriptor: LixFileDescriptorUpdate::None,
        };
        for (column_name, _) in assignments {
            let descriptor = match column_name.as_str() {
                // These fields determine the visible filesystem graph and
                // therefore require collision checks and path resolution.
                "path" => LixFileDescriptorUpdate::Path,
                "directory_id" | "name" => LixFileDescriptorUpdate::Topology,
                // Payload and descriptor attributes retain the current path.
                "data" => {
                    impact.data = true;
                    continue;
                }
                "lixcol_metadata" => LixFileDescriptorUpdate::Attributes,
                // Assignment validation rejects every other writable target.
                // Treating an unexpected target as topology-changing keeps a
                // future surface extension conservative until it is classified.
                _ => LixFileDescriptorUpdate::Topology,
            };
            impact.descriptor = match (impact.descriptor, descriptor) {
                (LixFileDescriptorUpdate::Path, _) | (_, LixFileDescriptorUpdate::Path) => {
                    LixFileDescriptorUpdate::Path
                }
                (LixFileDescriptorUpdate::Topology, _) | (_, LixFileDescriptorUpdate::Topology) => {
                    LixFileDescriptorUpdate::Topology
                }
                (LixFileDescriptorUpdate::Attributes, _)
                | (_, LixFileDescriptorUpdate::Attributes) => LixFileDescriptorUpdate::Attributes,
                (LixFileDescriptorUpdate::None, LixFileDescriptorUpdate::None) => {
                    LixFileDescriptorUpdate::None
                }
            };
        }
        impact
    }

    fn updates_path(self) -> bool {
        self.descriptor == LixFileDescriptorUpdate::Path
    }

    fn writes_descriptor(self) -> bool {
        self.descriptor != LixFileDescriptorUpdate::None
    }

    fn requires_path_resolver(self) -> bool {
        matches!(
            self.descriptor,
            LixFileDescriptorUpdate::Topology | LixFileDescriptorUpdate::Path
        )
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
                if update_columns.updates_path() {
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
        if update_columns.updates_path() {
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

    if update_columns.requires_path_resolver() {
        let Some(path_resolvers) = path_resolvers else {
            return Err(DataFusionError::Execution(
                "UPDATE lix_file requires filesystem path resolver".to_string(),
            ));
        };
        return if update_columns.updates_path() {
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
                update_columns.writes_descriptor(),
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
        update_columns.writes_descriptor(),
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
        staged
            .extend_filesystem_plan(plan)
            .map_err(lix_error_to_datafusion_error)?;

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
        // Path-only UPDATE sources already materialize `data` so a plugin
        // handoff can restage the file. Use those bytes for typed matching;
        // this adds no storage or rendering work beyond the existing source.
        let data = required_binary_value(batch, row_index, "data")?;

        let context =
            file_row_context_from_update(batch, assignment_values, row_index, branch_binding)?;
        let file_key = FilesystemDescriptorKey::from_context(&context, &file_id);
        let existing_plugin_key = plugin_render
            .owner_for_file(&file_key)
            .map(PluginFileOwner::plugin_key);
        let assigned_plugin = plugin_render
            .branch(&context.branch_id)
            .and_then(|branch| branch.catalog.select_for_bytes(&assigned_path, &data));
        let assigned_plugin_key = assigned_plugin.map(PluginRegistryEntry::key);
        let same_v2_owner = assigned_plugin.is_some_and(|plugin| {
            plugin.runtime() == PluginRuntime::WasmComponentV2
                && existing_plugin_key == Some(plugin.key())
        });
        if existing_plugin_key != assigned_plugin_key || same_v2_owner {
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
            staged
                .extend_filesystem_plan(plan)
                .map_err(lix_error_to_datafusion_error)?;
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
                staged
                    .extend_filesystem_plan(plan)
                    .map_err(lix_error_to_datafusion_error)?;
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
    let file_payload = TransactionFileData::new(
        file_id,
        path,
        filename,
        context.branch_id.clone(),
        context.global,
        context.untracked,
        data,
    );
    if !file_payload.is_empty() {
        stage_lix_file_data_blob_ref_write(staged, &file_payload, &context, origin)?;
    }
    staged.file_data_writes.push(file_payload);
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
    let file_payload = TransactionFileData::new(
        file_id.clone(),
        path,
        filename,
        context.branch_id.clone(),
        context.global,
        context.untracked,
        data,
    )
    .with_had_blob_ref(has_blob_ref);
    if file_payload.is_empty() {
        if has_blob_ref {
            let mut row = blob_ref_tombstone_row(file_id, context.clone());
            row.origin = origin;
            staged.state_rows.push(row);
        }
        staged.file_data_writes.push(file_payload);
        return Ok(());
    }
    stage_lix_file_data_blob_ref_write(staged, &file_payload, &context, origin)?;
    staged.file_data_writes.push(file_payload);
    Ok(())
}

fn stage_lix_file_data_blob_ref_write(
    staged: &mut LixFileStagedBatch,
    file_data: &TransactionFileData,
    context: &FilesystemRowContext,
    origin: Option<TransactionWriteOrigin>,
) -> Result<()> {
    let mut row = blob_ref_row(BlobRefRowInput {
        file_id: file_data.file_id.clone(),
        blob_hash: file_data
            .blob_hash()
            .expect("non-empty payload should have blob hash"),
        size_bytes: file_data.len(),
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

#[cfg(test)]
async fn lix_file_record_batch(
    schema: &SchemaRef,
    blob_reader: &Arc<dyn BlobDataReader>,
    plugin_render: Option<PluginRenderContext>,
    load_data: bool,
    rows: Vec<MaterializedLiveStateRow>,
) -> Result<RecordBatch, LixError> {
    let prepared = prepare_lix_file_rows(rows, &FilePathPredicate::All)?;
    lix_file_record_batch_from_prepared(schema, blob_reader, plugin_render, load_data, prepared)
        .await
}

struct PreparedLixFileRows {
    file_rows: BTreeMap<FilesystemDescriptorKey, FileDescriptorRecord>,
    blob_rows: BTreeMap<FilesystemBlobRefKey, BlobRefRecord>,
    file_paths: BTreeMap<FilesystemDescriptorKey, String>,
    path_ordered_file_keys: Option<Vec<FilesystemDescriptorKey>>,
}

impl PreparedLixFileRows {
    fn needs_plugin_render(&self, needs_data: bool) -> bool {
        needs_data
            && self.file_rows.values().any(|file| {
                plugin_file_can_have_durable_owner(file)
                    && !self.blob_rows.contains_key(&file.blob_ref_key())
            })
    }

    fn plugin_owner_candidates(&self, include_blob_backed: bool) -> Vec<FilesystemDescriptorKey> {
        self.file_rows
            .values()
            .filter(|file| {
                plugin_file_can_have_durable_owner(file)
                    && (include_blob_backed || !self.blob_rows.contains_key(&file.blob_ref_key()))
            })
            .map(|file| file.key.clone())
            .collect()
    }
}

fn plugin_file_can_have_durable_owner(file: &FileDescriptorRecord) -> bool {
    plugin_descriptor_key_can_have_durable_owner(&file.key)
}

fn plugin_descriptor_key_can_have_durable_owner(key: &FilesystemDescriptorKey) -> bool {
    !key.global() && !key.is_untracked() && key.file_id().is_none()
}

fn plugin_owner_candidates_from_batch(
    batch: &RecordBatch,
    branch_binding: Option<&str>,
) -> Result<Vec<FilesystemDescriptorKey>> {
    let mut candidates = Vec::new();
    for row_index in 0..batch.num_rows() {
        let file_id = required_string_value(batch, row_index, "id")?;
        let context = file_row_context_from_batch(batch, row_index, branch_binding)?;
        let key = FilesystemDescriptorKey::from_context(&context, &file_id);
        if plugin_descriptor_key_can_have_durable_owner(&key) {
            candidates.push(key);
        }
    }
    Ok(candidates)
}

fn prepare_lix_file_rows(
    rows: Vec<MaterializedLiveStateRow>,
    path_predicate: &FilePathPredicate,
) -> Result<PreparedLixFileRows, LixError> {
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
                        live: row,
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
    let mut file_paths = BTreeMap::<FilesystemDescriptorKey, String>::new();
    for (key, file) in &file_rows {
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
        if path_predicate.matches(&path) {
            file_paths.insert(key.clone(), path);
        }
    }
    file_rows.retain(|key, _| file_paths.contains_key(key));

    Ok(PreparedLixFileRows {
        file_rows,
        blob_rows,
        file_paths,
        path_ordered_file_keys: None,
    })
}

fn prepare_indexed_lix_file_rows(
    matches: &FilesystemPathSelection,
    rows: Vec<MaterializedLiveStateRow>,
) -> Result<PreparedLixFileRows, LixError> {
    let mut file_rows = BTreeMap::<FilesystemDescriptorKey, FileDescriptorRecord>::new();
    let mut file_paths = BTreeMap::<FilesystemDescriptorKey, String>::new();
    let mut path_ordered_file_keys = Vec::with_capacity(matches.len());
    for entry in matches.entries() {
        if entry.kind != FilesystemPathKind::File {
            continue;
        }
        let key = entry.key.clone();
        path_ordered_file_keys.push(key.clone());
        file_paths.insert(key.clone(), entry.path.clone());
        file_rows.insert(
            key.clone(),
            FileDescriptorRecord {
                id: entry.id().to_string(),
                directory_id: entry.parent_id.clone(),
                name: entry.name.clone(),
                key,
                live: entry.live_row(),
            },
        );
    }

    let mut blob_rows = BTreeMap::<FilesystemBlobRefKey, BlobRefRecord>::new();
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
        blob_rows.insert(
            FilesystemBlobRefKey::from_live_row(&row, snapshot.id),
            BlobRefRecord {
                blob_hash: snapshot.blob_hash,
                live: row,
            },
        );
    }

    Ok(PreparedLixFileRows {
        file_rows,
        blob_rows,
        file_paths,
        path_ordered_file_keys: Some(path_ordered_file_keys),
    })
}

fn lix_file_record_batch_from_path_selection(
    schema: &SchemaRef,
    matches: &FilesystemPathSelection,
    limit: Option<usize>,
) -> Result<RecordBatch, LixError> {
    let entries = matches
        .entries_of_kind_with_limit(FilesystemPathKind::File, limit)
        .collect::<Vec<_>>();
    let row_count = entries.len();
    let mut columns = Vec::<ArrayRef>::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let array: ArrayRef = match field.name().as_str() {
            "id" => Arc::new(StringArray::from(
                entries
                    .iter()
                    .map(|entry| Some(entry.id()))
                    .collect::<Vec<_>>(),
            )),
            "path" => Arc::new(StringArray::from(
                entries
                    .iter()
                    .map(|entry| Some(entry.path.as_str()))
                    .collect::<Vec<_>>(),
            )),
            "directory_id" => Arc::new(StringArray::from(
                entries
                    .iter()
                    .map(|entry| entry.parent_id.as_deref())
                    .collect::<Vec<_>>(),
            )),
            "name" => Arc::new(StringArray::from(
                entries
                    .iter()
                    .map(|entry| Some(entry.name.as_str()))
                    .collect::<Vec<_>>(),
            )),
            "data" => Arc::new(LargeBinaryArray::from(
                entries.iter().map(|_| Some(&[][..])).collect::<Vec<_>>(),
            )),
            "lixcol_entity_pk" => Arc::new(StringArray::from(
                entries
                    .iter()
                    .map(|entry| EntityPk::single(entry.id()).as_json_array_text().map(Some))
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            "lixcol_schema_key" => {
                Arc::new(StringArray::from(vec![
                    Some(FILE_DESCRIPTOR_SCHEMA_KEY);
                    row_count
                ]))
            }
            "lixcol_file_id" => Arc::new(StringArray::from(
                entries
                    .iter()
                    .map(|entry| entry.key.file_id())
                    .collect::<Vec<_>>(),
            )),
            "lixcol_global" => Arc::new(BooleanArray::from(
                entries
                    .iter()
                    .map(|entry| Some(entry.key.global()))
                    .collect::<Vec<_>>(),
            )),
            "lixcol_change_id" => Arc::new(StringArray::from(
                entries
                    .iter()
                    .map(|entry| entry.change_id().map(|id| id.to_string()))
                    .collect::<Vec<_>>(),
            )),
            "lixcol_created_at" => Arc::new(StringArray::from(
                entries
                    .iter()
                    .map(|entry| Some(entry.created_at()))
                    .collect::<Vec<_>>(),
            )),
            "lixcol_updated_at" => Arc::new(StringArray::from(
                entries
                    .iter()
                    .map(|entry| Some(entry.updated_at()))
                    .collect::<Vec<_>>(),
            )),
            "lixcol_commit_id" => Arc::new(StringArray::from(
                entries
                    .iter()
                    .map(|entry| entry.commit_id().map(|id| id.to_string()))
                    .collect::<Vec<_>>(),
            )),
            "lixcol_untracked" => Arc::new(BooleanArray::from(
                entries
                    .iter()
                    .map(|entry| Some(entry.key.is_untracked()))
                    .collect::<Vec<_>>(),
            )),
            "lixcol_metadata" => Arc::new(StringArray::from(
                entries
                    .iter()
                    .map(|entry| entry.metadata())
                    .collect::<Vec<_>>(),
            )),
            "lixcol_branch_id" => Arc::new(StringArray::from(
                entries
                    .iter()
                    .map(|entry| Some(entry.key.branch_id()))
                    .collect::<Vec<_>>(),
            )),
            other => {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("sql2 lix_file provider does not support projected column '{other}'"),
                ));
            }
        };
        columns.push(array);
    }
    let options = RecordBatchOptions::new().with_row_count(Some(row_count));
    RecordBatch::try_new_with_options(Arc::clone(schema), columns, &options).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("sql2 failed to build indexed lix_file record batch: {error}"),
        )
    })
}

struct LixFileRecordBatchRow {
    id: String,
    path: String,
    directory_id: Option<String>,
    name: String,
    data: Option<Vec<u8>>,
    entity_pk: String,
    file_id: Option<String>,
    global: bool,
    change_id: Option<String>,
    created_at: String,
    updated_at: String,
    commit_id: Option<String>,
    untracked: bool,
    metadata: Option<String>,
    branch_id: String,
}

#[derive(Default)]
struct LixFileRecordBatchColumns {
    ids: Vec<Option<String>>,
    paths: Vec<Option<String>>,
    directory_ids: Vec<Option<String>>,
    names: Vec<Option<String>>,
    data_values: Vec<Option<Vec<u8>>>,
    entity_pks: Vec<Option<String>>,
    schema_keys: Vec<Option<String>>,
    file_ids: Vec<Option<String>>,
    globals: Vec<Option<bool>>,
    change_ids: Vec<Option<String>>,
    created_ats: Vec<Option<String>>,
    updated_ats: Vec<Option<String>>,
    commit_ids: Vec<Option<String>>,
    untracked_values: Vec<Option<bool>>,
    metadata_values: Vec<Option<String>>,
    branch_ids: Vec<Option<String>>,
}

impl LixFileRecordBatchColumns {
    fn push(&mut self, row: LixFileRecordBatchRow) {
        self.ids.push(Some(row.id));
        self.paths.push(Some(row.path));
        self.directory_ids.push(row.directory_id);
        self.names.push(Some(row.name));
        self.data_values.push(row.data);
        self.entity_pks.push(Some(row.entity_pk));
        self.schema_keys
            .push(Some(FILE_DESCRIPTOR_SCHEMA_KEY.to_string()));
        self.file_ids.push(row.file_id);
        self.globals.push(Some(row.global));
        self.change_ids.push(row.change_id);
        self.created_ats.push(Some(row.created_at));
        self.updated_ats.push(Some(row.updated_at));
        self.commit_ids.push(row.commit_id);
        self.untracked_values.push(Some(row.untracked));
        self.metadata_values.push(row.metadata);
        self.branch_ids.push(Some(row.branch_id));
    }

    fn into_record_batch(self, schema: &SchemaRef) -> Result<RecordBatch, LixError> {
        let row_count = self.ids.len();
        let Self {
            ids,
            paths,
            directory_ids,
            names,
            data_values,
            entity_pks,
            schema_keys,
            file_ids,
            globals,
            change_ids,
            created_ats,
            updated_ats,
            commit_ids,
            untracked_values,
            metadata_values,
            branch_ids,
        } = self;
        let ids: ArrayRef = Arc::new(StringArray::from(ids));
        let paths: ArrayRef = Arc::new(StringArray::from(paths));
        let directory_ids: ArrayRef = Arc::new(StringArray::from(directory_ids));
        let names: ArrayRef = Arc::new(StringArray::from(names));
        let data_values: ArrayRef = Arc::new(LargeBinaryArray::from(
            data_values
                .iter()
                .map(|value| value.as_deref())
                .collect::<Vec<_>>(),
        ));
        let entity_pks: ArrayRef = Arc::new(StringArray::from(entity_pks));
        let schema_keys: ArrayRef = Arc::new(StringArray::from(schema_keys));
        let file_ids: ArrayRef = Arc::new(StringArray::from(file_ids));
        let globals: ArrayRef = Arc::new(BooleanArray::from(globals));
        let change_ids: ArrayRef = Arc::new(StringArray::from(change_ids));
        let created_ats: ArrayRef = Arc::new(StringArray::from(created_ats));
        let updated_ats: ArrayRef = Arc::new(StringArray::from(updated_ats));
        let commit_ids: ArrayRef = Arc::new(StringArray::from(commit_ids));
        let untracked_values: ArrayRef = Arc::new(BooleanArray::from(untracked_values));
        let metadata_values: ArrayRef = Arc::new(StringArray::from(metadata_values));
        let branch_ids: ArrayRef = Arc::new(StringArray::from(branch_ids));

        let mut columns = Vec::<ArrayRef>::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let array = match field.name().as_str() {
                "id" => Arc::clone(&ids),
                "path" => Arc::clone(&paths),
                "directory_id" => Arc::clone(&directory_ids),
                "name" => Arc::clone(&names),
                "data" => Arc::clone(&data_values),
                "lixcol_entity_pk" => Arc::clone(&entity_pks),
                "lixcol_schema_key" => Arc::clone(&schema_keys),
                "lixcol_file_id" => Arc::clone(&file_ids),
                "lixcol_global" => Arc::clone(&globals),
                "lixcol_change_id" => Arc::clone(&change_ids),
                "lixcol_created_at" => Arc::clone(&created_ats),
                "lixcol_updated_at" => Arc::clone(&updated_ats),
                "lixcol_commit_id" => Arc::clone(&commit_ids),
                "lixcol_untracked" => Arc::clone(&untracked_values),
                "lixcol_metadata" => Arc::clone(&metadata_values),
                "lixcol_branch_id" => Arc::clone(&branch_ids),
                other => {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "sql2 lix_file provider does not support projected column '{other}'"
                        ),
                    ));
                }
            };
            columns.push(array);
        }

        let options = RecordBatchOptions::new().with_row_count(Some(row_count));
        RecordBatch::try_new_with_options(Arc::clone(schema), columns, &options).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("sql2 failed to build lix_file record batch: {error}"),
            )
        })
    }
}

async fn lix_file_record_batch_from_prepared(
    schema: &SchemaRef,
    blob_reader: &Arc<dyn BlobDataReader>,
    plugin_render: Option<PluginRenderContext>,
    load_data: bool,
    prepared: PreparedLixFileRows,
) -> Result<RecordBatch, LixError> {
    let projected_columns = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    let needs_data = load_data && projected_columns.contains(&"data");
    let PreparedLixFileRows {
        mut file_rows,
        blob_rows,
        mut file_paths,
        path_ordered_file_keys,
    } = prepared;
    let mut columns = LixFileRecordBatchColumns::default();
    let mut blob_bytes = if needs_data {
        load_blob_bytes_for_files(blob_reader, &file_rows, &blob_rows).await?
    } else {
        LoadedBlobBytes::default()
    };

    let file_keys =
        path_ordered_file_keys.unwrap_or_else(|| file_rows.keys().cloned().collect::<Vec<_>>());
    let mut rendered_plugin_bytes = match &plugin_render {
        Some(plugin_render) if needs_data => {
            render_plugin_files_for_sql(
                plugin_render,
                blob_reader,
                &file_keys,
                &file_rows,
                &blob_rows,
                &file_paths,
            )
            .await?
        }
        _ => BTreeMap::new(),
    };
    for key in file_keys {
        let file = file_rows
            .remove(&key)
            .expect("prepared lix_file order should reference a descriptor");
        let path = file_paths
            .remove(&key)
            .expect("prepared lix_file descriptor should have a path");
        let blob_key = file.blob_ref_key();
        let data = if needs_data {
            match blob_bytes.take(&blob_key) {
                Some(data) => data,
                None => Some(rendered_plugin_bytes.remove(&key).unwrap_or_default()),
            }
        } else {
            Some(Vec::new())
        };
        let blob_ref = blob_rows.get(&blob_key);
        let projected_change_id = blob_ref
            .and_then(|blob_ref| blob_ref.live.change_id)
            .or(file.live.change_id);
        let FileDescriptorRecord {
            id,
            directory_id,
            name,
            live,
            ..
        } = file;
        columns.push(LixFileRecordBatchRow {
            id,
            path,
            directory_id,
            name,
            data,
            entity_pk: live.entity_pk.as_json_array_text()?,
            file_id: live.file_id,
            global: live.global,
            change_id: projected_change_id.map(|id| id.to_string()),
            created_at: live.created_at,
            updated_at: live.updated_at,
            commit_id: live.commit_id.map(|id| id.to_string()),
            untracked: live.untracked,
            metadata: live.metadata.as_deref().map(serialize_row_metadata),
            branch_id: live.branch_id,
        });
    }

    columns.into_record_batch(schema)
}

async fn exact_path_data_rows_from_prepared(
    blob_reader: &Arc<dyn BlobDataReader>,
    plugin_render: Option<PluginRenderContext>,
    prepared: PreparedLixFileRows,
) -> Result<Vec<Vec<Value>>, LixError> {
    let PreparedLixFileRows {
        mut file_rows,
        blob_rows,
        mut file_paths,
        path_ordered_file_keys,
    } = prepared;
    let mut blob_bytes = load_blob_bytes_for_files(blob_reader, &file_rows, &blob_rows).await?;
    let file_keys =
        path_ordered_file_keys.unwrap_or_else(|| file_rows.keys().cloned().collect::<Vec<_>>());
    let mut rows = Vec::with_capacity(file_keys.len());
    let mut rendered_plugin_bytes = match &plugin_render {
        Some(plugin_render) => {
            render_plugin_files_for_sql(
                plugin_render,
                blob_reader,
                &file_keys,
                &file_rows,
                &blob_rows,
                &file_paths,
            )
            .await?
        }
        None => BTreeMap::new(),
    };
    for key in file_keys {
        let file = file_rows
            .remove(&key)
            .expect("prepared lix_file order should reference a descriptor");
        let path = file_paths
            .remove(&key)
            .expect("prepared lix_file descriptor should have a path");
        let blob_key = file.blob_ref_key();
        let data = match blob_bytes.take(&blob_key) {
            Some(data) => data,
            None => Some(rendered_plugin_bytes.remove(&key).unwrap_or_default()),
        };
        rows.push(vec![
            Value::Text(path),
            data.map_or(Value::Null, Value::Blob),
        ]);
    }

    Ok(rows)
}

#[derive(Default)]
struct LoadedBlobBytes {
    bytes_by_key: BTreeMap<FilesystemBlobRefKey, Option<Vec<u8>>>,
    remaining_by_key: BTreeMap<FilesystemBlobRefKey, usize>,
}

impl LoadedBlobBytes {
    fn take(&mut self, key: &FilesystemBlobRefKey) -> Option<Option<Vec<u8>>> {
        match self.remaining_by_key.get_mut(key) {
            Some(remaining) if *remaining > 1 => {
                *remaining -= 1;
                self.bytes_by_key.get(key).cloned()
            }
            Some(_) => {
                self.remaining_by_key.remove(key);
                self.bytes_by_key.remove(key)
            }
            None => None,
        }
    }
}

async fn load_blob_bytes_for_files(
    blob_reader: &Arc<dyn BlobDataReader>,
    file_rows: &BTreeMap<FilesystemDescriptorKey, FileDescriptorRecord>,
    blob_rows: &BTreeMap<FilesystemBlobRefKey, BlobRefRecord>,
) -> Result<LoadedBlobBytes, LixError> {
    if file_rows.is_empty() || blob_rows.is_empty() {
        return Ok(LoadedBlobBytes::default());
    }
    let mut keys = Vec::new();
    let mut hashes = Vec::new();
    let mut remaining_by_key = BTreeMap::<FilesystemBlobRefKey, usize>::new();
    for file in file_rows.values() {
        let key = file.blob_ref_key();
        if let Some(row) = blob_rows.get(&key) {
            let remaining = remaining_by_key.entry(key.clone()).or_insert(0);
            if *remaining == 0 {
                keys.push(key);
                hashes.push(BlobHash::from_hex(&row.blob_hash)?);
            }
            *remaining += 1;
        }
    }
    if keys.is_empty() {
        return Ok(LoadedBlobBytes::default());
    }
    let values = blob_reader.load_bytes_many(&hashes).await?.into_vec();
    if values.len() != keys.len() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "blob reader returned {} values for {} requested hashes",
                values.len(),
                keys.len()
            ),
        ));
    }
    Ok(LoadedBlobBytes {
        bytes_by_key: keys.into_iter().zip(values).collect(),
        remaining_by_key,
    })
}

async fn render_plugin_files_for_sql(
    plugin_render: &PluginRenderContext,
    blob_reader: &Arc<dyn BlobDataReader>,
    file_keys: &[FilesystemDescriptorKey],
    file_rows: &BTreeMap<FilesystemDescriptorKey, FileDescriptorRecord>,
    blob_rows: &BTreeMap<FilesystemBlobRefKey, BlobRefRecord>,
    file_paths: &BTreeMap<FilesystemDescriptorKey, String>,
) -> Result<BTreeMap<FilesystemDescriptorKey, Vec<u8>>, LixError> {
    let mut owned_file_keys = Vec::new();
    let mut materialized_v2_file_keys = Vec::new();
    for key in file_keys {
        let file = file_rows
            .get(key)
            .expect("prepared lix_file order should reference a descriptor");
        let Some(owner) = plugin_render.owner_for_file(key) else {
            continue;
        };
        let path = file_paths
            .get(key)
            .expect("prepared lix_file descriptor should have a path");
        let Some(branch) = plugin_render.branch(key.branch_id()) else {
            return Err(plugin_unavailable_error(file, path, owner));
        };
        let Some(plugin) = branch.registry.get(owner.plugin_key()) else {
            return Err(plugin_unavailable_error(file, path, owner));
        };
        if branch.catalog.matches_plugin(owner.plugin_key(), path) {
            match (
                plugin.runtime(),
                blob_rows.contains_key(&file.blob_ref_key()),
            ) {
                (PluginRuntime::WasmComponentV2, true)
                    if plugin_render.session_file_views.is_some() =>
                {
                    materialized_v2_file_keys.push(key.clone());
                }
                (PluginRuntime::WasmComponentV2, true) => {}
                (PluginRuntime::WasmComponentV2, false) => {
                    return Err(invalid_plugin_read_state(format!(
                        "v2 plugin-owned file '{}' is missing its durable materialized blob",
                        file.id
                    )));
                }
                (PluginRuntime::WasmComponentV1, false) => owned_file_keys.push(key.clone()),
                (PluginRuntime::WasmComponentV1, true) => {}
            }
        }
    }
    for file_key in materialized_v2_file_keys {
        acknowledge_materialized_v2_file(
            plugin_render,
            blob_reader,
            &file_key,
            file_rows,
            blob_rows,
            file_paths,
        )
        .await?;
    }
    if owned_file_keys.is_empty() {
        return Ok(BTreeMap::new());
    }

    let mut execution_keys = BTreeSet::<(String, String)>::new();
    for file_key in &owned_file_keys {
        let owner = plugin_render
            .owner_for_file(file_key)
            .expect("owned file key was selected above");
        execution_keys.insert((
            file_key.branch_id().to_string(),
            owner.plugin_key().to_string(),
        ));
    }

    let mut component_instances =
        BTreeMap::<(String, String), Arc<dyn WasmComponentInstance>>::new();
    let mut cold_entries = BTreeMap::<(String, String), PluginRegistryEntry>::new();
    for (branch_id, plugin_key) in &execution_keys {
        let entry = plugin_render
            .branch(branch_id)
            .and_then(|branch| branch.registry.get(plugin_key))
            .ok_or_else(|| invalid_plugin_read_state(format!(
                "owner references plugin '{plugin_key}' absent from branch '{branch_id}' registry"
            )))?;
        let hash = BlobHash::from_hex(entry.wasm_blob_hash())?;
        let key = (branch_id.clone(), plugin_key.clone());
        if let Some(instance) = plugin_render
            .host
            .cached_plugin_component(entry.key(), hash)?
        {
            component_instances.insert(key, instance);
        } else {
            cold_entries.insert(key, entry.clone());
        }
    }

    let wasm_hash_hexes = cold_entries
        .values()
        .map(|entry| entry.wasm_blob_hash().to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let wasm_hashes = wasm_hash_hexes
        .iter()
        .map(|hash| BlobHash::from_hex(hash))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let wasm_values = if wasm_hashes.is_empty() {
        Vec::new()
    } else {
        blob_reader.load_bytes_many(&wasm_hashes).await?.into_vec()
    };
    if wasm_values.len() != wasm_hashes.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "blob reader returned {} values for {} plugin WASM hashes",
                wasm_values.len(),
                wasm_hashes.len()
            ),
        ));
    }
    let mut wasm_by_hash = BTreeMap::<String, Vec<u8>>::new();
    for (hash, bytes) in wasm_hash_hexes.into_iter().zip(wasm_values) {
        let bytes = bytes.ok_or_else(|| {
            invalid_plugin_read_state(format!(
                "plugin registry references missing WASM blob '{hash}'"
            ))
        })?;
        wasm_by_hash.insert(hash, bytes);
    }

    for (key, entry) in cold_entries {
        let wasm = wasm_by_hash
            .get(entry.wasm_blob_hash())
            .expect("distinct plugin WASM hash was loaded")
            .clone();
        let plugin = entry.to_installed_plugin(wasm)?;
        let instance =
            crate::plugin::load_or_init_plugin_component(&plugin_render.host, &plugin).await?;
        component_instances.insert(key, instance);
    }

    let mut file_ids_by_plugin = BTreeMap::<(String, String), BTreeSet<String>>::new();
    let mut file_key_by_branch_and_id =
        BTreeMap::<(String, String), FilesystemDescriptorKey>::new();
    for file_key in &owned_file_keys {
        let owner = plugin_render
            .owner_for_file(file_key)
            .expect("owned file key was selected above");
        let branch_id = file_key.branch_id().to_string();
        let file_id = file_key.descriptor_id().to_string();
        file_ids_by_plugin
            .entry((branch_id.clone(), owner.plugin_key().to_string()))
            .or_default()
            .insert(file_id.clone());
        if file_key_by_branch_and_id
            .insert((branch_id.clone(), file_id.clone()), file_key.clone())
            .is_some()
        {
            return Err(invalid_plugin_read_state(format!(
                "branch '{branch_id}' has multiple plugin-render candidates for file id '{file_id}'"
            )));
        }
    }

    let state_scans = file_ids_by_plugin
        .into_iter()
        .map(|(execution_key, file_ids)| {
            let live_state = Arc::clone(&plugin_render.live_state);
            let schema_keys = plugin_render
                .branch(&execution_key.0)
                .and_then(|branch| branch.registry.get(&execution_key.1))
                .expect("registry entry exists for every state group")
                .schema_keys()
                .to_vec();
            async move {
                let branch_id = execution_key.0.clone();
                let mut rows = live_state
                    .scan_tracked_rows(&LiveStateScanRequest {
                        filter: LiveStateFilter {
                            schema_keys: schema_keys.clone(),
                            branch_ids: vec![branch_id.clone()],
                            file_ids: file_ids
                                .iter()
                                .cloned()
                                .map(crate::NullableKeyFilter::Value)
                                .collect(),
                            untracked: Some(false),
                            ..LiveStateFilter::default()
                        },
                        projection: plugin_state_live_state_projection(),
                        limit: None,
                    })
                    .await?;
                rows.retain(|row| {
                    row.branch_id == branch_id
                        && !row.global
                        && !row.untracked
                        && row
                            .file_id
                            .as_ref()
                            .is_some_and(|file_id| file_ids.contains(file_id))
                        && row.snapshot_content.is_some()
                        && schema_keys.binary_search(&row.schema_key).is_ok()
                });
                Ok::<_, LixError>((execution_key, rows))
            }
        });
    let state_groups = try_join_all(state_scans).await?;
    let mut state_by_file = owned_file_keys
        .iter()
        .cloned()
        .map(|key| (key, Vec::<MaterializedLiveStateRow>::new()))
        .collect::<BTreeMap<_, _>>();
    for ((branch_id, _plugin_key), rows) in state_groups {
        for row in rows {
            let Some(file_id) = row.file_id.as_deref() else {
                continue;
            };
            let key = file_key_by_branch_and_id
                .get(&(branch_id.clone(), file_id.to_string()))
                .ok_or_else(|| {
                    invalid_plugin_read_state(format!(
                        "plugin state scan returned unexpected file id '{file_id}' in branch '{branch_id}'"
                    ))
                })?;
            state_by_file
                .get_mut(key)
                .expect("owned file state bucket exists")
                .push(row);
        }
    }
    for rows in state_by_file.values_mut() {
        rows.sort_by(|left, right| {
            (&left.schema_key, &left.entity_pk).cmp(&(&right.schema_key, &right.entity_pk))
        });
    }

    let mut rendered = BTreeMap::new();
    for file_key in owned_file_keys {
        let owner = plugin_render
            .owner_for_file(&file_key)
            .expect("owned file key was selected above");
        let execution_key = (
            file_key.branch_id().to_string(),
            owner.plugin_key().to_string(),
        );
        let component = component_instances
            .get(&execution_key)
            .expect("resolved component exists for owned file");
        let active_state = state_by_file.remove(&file_key).unwrap_or_default();
        // The owner row, not the presence of plugin state rows, is the durable
        // materialization signal. Plugins that intentionally emit zero state
        // must still receive render([]).
        let bytes = render_plugin_state_with_component_instance(component, &active_state).await?;
        if let Some(session_file_views) = &plugin_render.session_file_views {
            let plugin = plugin_render
                .branch(file_key.branch_id())
                .and_then(|branch| branch.registry.get(owner.plugin_key()))
                .expect("rendered plugin should remain in the branch registry");
            session_file_views.remember_plugin_file_view(
                SessionFileViewKey::new(file_key.branch_id(), file_key.descriptor_id()),
                SessionPluginFileView {
                    plugin_key: owner.plugin_key().to_string(),
                    plugin_generation: plugin.archive_blob_hash().to_string(),
                    owner_change_id: plugin_render
                        .owner_change_id_for_file(&file_key)
                        .expect("rendered plugin owner should have a change id")
                        .to_string(),
                    observation: None,
                    // Rendering only borrows the materialized state and this
                    // is its final consumer. Move the row graph into the
                    // session acknowledgement instead of deep-cloning every
                    // entity on each exact blob read.
                    rows: active_state.into(),
                },
            );
        }
        rendered.insert(file_key, bytes);
    }
    Ok(rendered)
}

async fn acknowledge_materialized_v2_file(
    plugin_render: &PluginRenderContext,
    blob_reader: &Arc<dyn BlobDataReader>,
    file_key: &FilesystemDescriptorKey,
    file_rows: &BTreeMap<FilesystemDescriptorKey, FileDescriptorRecord>,
    blob_rows: &BTreeMap<FilesystemBlobRefKey, BlobRefRecord>,
    file_paths: &BTreeMap<FilesystemDescriptorKey, String>,
) -> Result<(), LixError> {
    let file = file_rows
        .get(file_key)
        .expect("v2 materialization candidate has a descriptor");
    let blob = blob_rows
        .get(&file.blob_ref_key())
        .expect("v2 materialization candidate has a blob reference");
    let owner = plugin_render
        .owner_for_file(file_key)
        .expect("v2 materialization candidate has an owner");
    let owner_change_id = plugin_render
        .owner_change_id_for_file(file_key)
        .ok_or_else(|| invalid_plugin_read_state("v2 plugin owner is missing change_id"))?;
    let path = file_paths
        .get(file_key)
        .expect("v2 materialization candidate has a path");
    let plugin = plugin_render
        .branch(file_key.branch_id())
        .and_then(|branch| branch.registry.get(owner.plugin_key()))
        .ok_or_else(|| plugin_unavailable_error(file, path, owner))?;
    if plugin.runtime() != PluginRuntime::WasmComponentV2 {
        return Err(invalid_plugin_read_state(
            "materialized v2 acknowledgement selected a non-v2 plugin",
        ));
    }
    let semantic_root = blob
        .live
        .change_id
        .as_ref()
        .map(ToString::to_string)
        .ok_or_else(|| {
            invalid_plugin_read_state("materialized v2 blob reference is missing its semantic root")
        })?;
    let actor_key = PluginActorKey {
        branch_id: file_key.branch_id().to_string(),
        file_id: file_key.descriptor_id().to_string(),
        path: path.clone(),
        owner_change_id: owner_change_id.to_string(),
        plugin_key: plugin.key().to_string(),
        plugin_generation: plugin.archive_blob_hash().to_string(),
    };
    let cache = plugin_render.host.actor_cache();
    let observation = match cache.observe(&actor_key, &semantic_root).await {
        Ok(observation) => observation,
        Err(error) if error.code == LixError::CODE_PLUGIN_OBSERVATION_STALE => {
            cold_open_materialized_v2_actor(
                plugin_render,
                blob_reader,
                plugin,
                &actor_key,
                path,
                blob,
                &semantic_root,
            )
            .await?
        }
        Err(error) => return Err(error),
    };
    if let Some(session_file_views) = &plugin_render.session_file_views {
        session_file_views.remember_plugin_file_view(
            SessionFileViewKey::new(file_key.branch_id(), file_key.descriptor_id()),
            SessionPluginFileView {
                plugin_key: plugin.key().to_string(),
                plugin_generation: plugin.archive_blob_hash().to_string(),
                owner_change_id: owner_change_id.to_string(),
                observation: Some(observation),
                rows: Arc::from([]),
            },
        );
    }
    Ok(())
}

async fn cold_open_materialized_v2_actor(
    plugin_render: &PluginRenderContext,
    blob_reader: &Arc<dyn BlobDataReader>,
    plugin: &PluginRegistryEntry,
    actor_key: &PluginActorKey,
    path: &str,
    blob: &BlobRefRecord,
    semantic_root: &str,
) -> Result<crate::plugin::PluginObservation, LixError> {
    let cache = plugin_render.host.actor_cache();
    let _cold_open_guard = cache.cold_open_guard().await;
    // Another reader may have populated this actor while we waited. Recheck
    // under the shared cold gate before scanning full semantic state or
    // instantiating another Store.
    let cold_install: PluginActorColdInstall =
        match cache.prepare_cold_open(actor_key, semantic_root).await? {
            PluginActorColdOpen::Ready(observation) => return Ok(observation),
            PluginActorColdOpen::Build(cold_install) => cold_install,
        };
    let limits = WasmTransitionLimits::default();
    let factory = resolve_v2_factory(&plugin_render.host, blob_reader, plugin).await?;
    let mut rows = plugin_render
        .live_state
        .scan_tracked_rows(&LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: plugin.schema_keys().to_vec(),
                branch_ids: vec![actor_key.branch_id.clone()],
                file_ids: vec![crate::NullableKeyFilter::Value(actor_key.file_id.clone())],
                untracked: Some(false),
                ..LiveStateFilter::default()
            },
            projection: plugin_state_live_state_projection(),
            limit: None,
        })
        .await?;
    rows.retain(|row| {
        row.branch_id == actor_key.branch_id
            && row.file_id.as_deref() == Some(actor_key.file_id.as_str())
            && !row.global
            && !row.untracked
            && row.snapshot_content.is_some()
            && plugin.schema_keys().binary_search(&row.schema_key).is_ok()
    });
    let mut entities = rows
        .into_iter()
        .map(|row| WasmHostEntity {
            key: crate::wasm::WasmEntityKey {
                schema_key: row.schema_key,
                entity_pk: row.entity_pk.into_parts(),
            },
            snapshot_content: WasmHostBytes::Inline(
                row.snapshot_content
                    .expect("cold-open v2 rows retained only live snapshots")
                    .into_bytes(),
            ),
        })
        .collect::<Vec<_>>();
    entities.sort_by(|left, right| left.key.cmp(&right.key));
    let entity_count = entities.len();
    let source = VecEntitySource::new(entities, limits)?;
    let materialized_hash = BlobHash::from_hex(&blob.blob_hash)?;
    let values = blob_reader
        .load_bytes_many(&[materialized_hash])
        .await?
        .into_vec();
    let materialized_bytes: Arc<[u8]> = values
        .into_iter()
        .next()
        .flatten()
        .ok_or_else(|| {
            invalid_plugin_read_state(format!(
                "materialized v2 blob '{}' is missing",
                blob.blob_hash
            ))
        })?
        .into();
    let mut actor = factory.instantiate_actor().await?;
    let transition = match actor
        .open_entities(
            limits,
            WasmOpenEntitiesInput {
                descriptor: v2_read_descriptor(path, plugin),
                entities: Box::new(source),
            },
        )
        .await
    {
        Ok(transition) => transition,
        Err(error) => {
            let _ = actor.retire().await;
            return Err(error);
        }
    };
    let validated = match drain_entity_transition_edits(
        actor.as_mut(),
        transition,
        &[],
        Some(Arc::clone(&materialized_bytes)),
        None,
        limits,
    )
    .await
    {
        Ok(validated) => validated,
        Err(error) => {
            let _ = actor.retire().await;
            return Err(error);
        }
    };
    let mut counters = validated.counters;
    counters.full_state_semantic_rows_materialized =
        u64::try_from(entity_count).unwrap_or(u64::MAX);
    counters.full_document_reparses = 1;
    counters.full_renderer_invocations = 1;
    plugin_render.host.record_v2_transition_counters(counters);
    cache
        .install_cold_if_absent(
            cold_install,
            actor_key.clone(),
            actor,
            validated.document,
            Arc::clone(&materialized_bytes),
            Arc::<str>::from(semantic_root),
        )
        .await
}

async fn resolve_v2_factory(
    host: &PluginRuntimeHost,
    blob_reader: &Arc<dyn BlobDataReader>,
    plugin: &PluginRegistryEntry,
) -> Result<Arc<dyn WasmComponentV2Factory>, LixError> {
    let wasm_hash = BlobHash::from_hex(plugin.wasm_blob_hash())?;
    if let Some(factory) = host.cached_plugin_v2_factory(plugin.key(), wasm_hash)? {
        return Ok(factory);
    }
    let wasm = blob_reader
        .load_bytes_many(&[wasm_hash])
        .await?
        .into_vec()
        .into_iter()
        .next()
        .flatten()
        .ok_or_else(|| {
            invalid_plugin_read_state(format!(
                "plugin registry references missing WASM blob '{}'",
                plugin.wasm_blob_hash()
            ))
        })?;
    host.load_or_compile_v2_factory(&plugin.to_installed_plugin(wasm)?)
        .await
}

fn v2_read_descriptor(path: &str, plugin: &PluginRegistryEntry) -> WasmFileDescriptor {
    WasmFileDescriptor {
        path: Some(path.to_string()),
        media_type: inferred_media_type_for_path(Some(path)).map(str::to_owned),
        plugin: WasmPluginSelection {
            plugin_key: plugin.key().to_string(),
            generation: plugin.archive_blob_hash().to_string(),
        },
    }
}

async fn plugin_render_context_for_lix_file_scan(
    live_state: Arc<dyn LiveStateReader>,
    request: &LiveStateScanRequest,
    host: PluginRuntimeHost,
    prepared: &PreparedLixFileRows,
    include_blob_backed_candidates: bool,
) -> Result<Option<PluginRenderContext>, LixError> {
    let candidates = prepared.plugin_owner_candidates(include_blob_backed_candidates);
    if candidates.is_empty() {
        return Ok(None);
    }
    let branches = load_plugin_render_branches(Arc::clone(&live_state), request, &host).await?;
    plugin_render_context_with_branches(
        live_state,
        host,
        branches,
        candidates,
        include_blob_backed_candidates,
    )
    .await
}

async fn load_plugin_render_branches(
    live_state: Arc<dyn LiveStateReader>,
    request: &LiveStateScanRequest,
    host: &PluginRuntimeHost,
) -> Result<BTreeMap<String, BranchPluginRenderContext>, LixError> {
    let branch_ids = request
        .filter
        .branch_ids
        .iter()
        .filter(|branch_id| branch_id.as_str() != GLOBAL_BRANCH_ID)
        .cloned()
        .collect::<BTreeSet<_>>();
    let registry_reads = branch_ids.into_iter().map(|branch_id| {
        let live_state = Arc::clone(&live_state);
        async move {
            let rows = live_state
                .scan_tracked_rows(&LiveStateScanRequest {
                    filter: LiveStateFilter {
                        schema_keys: vec!["lix_key_value".to_string()],
                        entity_pks: vec![EntityPk::single(PLUGIN_REGISTRY_KEY)],
                        branch_ids: vec![branch_id.clone()],
                        file_ids: vec![crate::NullableKeyFilter::Null],
                        untracked: Some(false),
                        ..LiveStateFilter::default()
                    },
                    projection: plugin_control_live_state_projection(),
                    limit: Some(1),
                })
                .await?;
            let row = rows.into_iter().find(|row| {
                row.schema_key == "lix_key_value"
                    && row.entity_pk.as_single_string().ok() == Some(PLUGIN_REGISTRY_KEY)
                    && row.file_id.is_none()
                    && row.branch_id == branch_id
                    && !row.global
                    && !row.untracked
            });
            let registry = PluginRegistry::from_optional_live_state_row(row.as_ref(), &branch_id)?;
            Ok::<_, LixError>((branch_id, registry))
        }
    });

    let mut branches = BTreeMap::<String, BranchPluginRenderContext>::new();
    for (branch_id, registry) in try_join_all(registry_reads).await? {
        if registry.is_empty() {
            continue;
        }
        let catalog = host.compiled_plugin_catalog(&registry)?;
        branches.insert(branch_id, BranchPluginRenderContext { registry, catalog });
    }
    // This is the O(1)-shape no-plugin path: exact registry lookups above are
    // the only reads. No owner/state scan, CAS read, matcher compilation, or
    // WASM work is reachable when every requested registry is absent/empty.
    Ok(branches)
}

async fn plugin_render_context_with_branches(
    live_state: Arc<dyn LiveStateReader>,
    host: PluginRuntimeHost,
    branches: BTreeMap<String, BranchPluginRenderContext>,
    candidates: Vec<FilesystemDescriptorKey>,
    keep_catalog_without_owners: bool,
) -> Result<Option<PluginRenderContext>, LixError> {
    if candidates.is_empty() {
        return Ok(None);
    }

    let mut candidate_keys_by_branch =
        BTreeMap::<String, BTreeMap<String, FilesystemDescriptorKey>>::new();
    for candidate in candidates {
        let branch_id = candidate.branch_id().to_string();
        let file_id = candidate.descriptor_id().to_string();
        if candidate_keys_by_branch
            .entry(branch_id.clone())
            .or_default()
            .insert(file_id.clone(), candidate)
            .is_some()
        {
            return Err(invalid_plugin_read_state(format!(
                "branch '{branch_id}' has multiple plugin-owner candidates for file id '{file_id}'"
            )));
        }
    }

    let owner_reads = candidate_keys_by_branch
        .iter()
        .map(|(branch_id, candidate_keys)| {
            let live_state = Arc::clone(&live_state);
            let branch_id = branch_id.clone();
            let file_ids = candidate_keys.keys().cloned().collect::<BTreeSet<_>>();
            async move {
                let rows = live_state
                    .scan_tracked_rows(&LiveStateScanRequest {
                        filter: LiveStateFilter {
                            schema_keys: vec!["lix_key_value".to_string()],
                            entity_pks: vec![EntityPk::single(PLUGIN_OWNER_KEY)],
                            branch_ids: vec![branch_id.clone()],
                            file_ids: file_ids
                                .iter()
                                .cloned()
                                .map(crate::NullableKeyFilter::Value)
                                .collect(),
                            untracked: Some(false),
                            ..LiveStateFilter::default()
                        },
                        projection: plugin_control_live_state_projection(),
                        limit: None,
                    })
                    .await?;
                Ok::<_, LixError>((branch_id, file_ids, rows))
            }
        });
    let mut owners_by_file = BTreeMap::new();
    let mut owner_change_ids_by_file = BTreeMap::new();
    for (branch_id, file_ids, rows) in try_join_all(owner_reads).await? {
        for row in rows {
            let Some(file_id) = row.file_id.as_deref() else {
                continue;
            };
            if row.schema_key != "lix_key_value"
                || row.entity_pk.as_single_string().ok() != Some(PLUGIN_OWNER_KEY)
                || row.branch_id != branch_id
                || row.global
                || row.untracked
                || !file_ids.contains(file_id)
            {
                continue;
            }
            let Some(owner) = PluginFileOwner::from_live_state_row(&row, &branch_id)? else {
                continue;
            };
            let candidate_key = candidate_keys_by_branch
                .get(&branch_id)
                .and_then(|candidate_keys| candidate_keys.get(file_id))
                .expect("owner row was filtered to candidate file ids")
                .clone();
            let owner_change_id = row.change_id.ok_or_else(|| {
                invalid_plugin_read_state(format!(
                    "branch '{branch_id}' plugin owner for file id '{file_id}' is missing change_id"
                ))
            })?;
            // Keep a well-formed stale owner even when its plugin is currently
            // absent. Rendering checks the current registry, while path moves
            // still need the old key to force reconciliation; reinstall can
            // then resume from the durable owner.
            if owners_by_file
                .insert(candidate_key.clone(), owner)
                .is_some()
            {
                return Err(invalid_plugin_read_state(format!(
                    "branch '{branch_id}' returned duplicate plugin owners for file id '{file_id}'"
                )));
            }
            owner_change_ids_by_file.insert(candidate_key, owner_change_id.to_string());
        }
    }

    if owners_by_file.is_empty() && !keep_catalog_without_owners {
        return Ok(None);
    }

    Ok(Some(PluginRenderContext {
        live_state,
        host,
        branches,
        owners_by_file,
        owner_change_ids_by_file,
        session_file_views: None,
    }))
}

fn invalid_plugin_read_state(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INTERNAL_ERROR, message)
}

fn plugin_unavailable_error(
    file: &FileDescriptorRecord,
    path: &str,
    owner: &PluginFileOwner,
) -> LixError {
    LixError::new(
        LixError::CODE_PLUGIN_UNAVAILABLE,
        format!(
            "file '{path}' requires unavailable plugin '{}'",
            owner.plugin_key()
        ),
    )
    .with_hint(format!(
        "Add a valid .lixplugin archive for '{}' to /.lix/plugins/ to render the file again.",
        owner.plugin_key()
    ))
    .with_details(serde_json::json!({
        "branch_id": file.live.branch_id,
        "file_id": file.id,
        "path": path,
        "plugin_key": owner.plugin_key(),
    }))
}

fn plugin_control_live_state_projection() -> LiveStateProjection {
    LiveStateProjection {
        columns: vec!["snapshot_content".to_string()],
    }
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

fn scan_needs_blob_rows(
    base_schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
) -> bool {
    let projects_blob_column = match projection {
        Some(indices) => indices.iter().any(|index| {
            matches!(
                base_schema.field(*index).name().as_str(),
                "data" | "lixcol_change_id"
            )
        }),
        None => true,
    };
    projects_blob_column
        || filters.iter().any(|filter| {
            contains_column(filter, "data") || contains_column(filter, "lixcol_change_id")
        })
}

fn should_use_path_index(path_predicate: &FilePathPredicate, needs_blob_rows: bool) -> bool {
    path_predicate != &FilePathPredicate::All || !needs_blob_rows
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

async fn scan_indexed_file_rows(
    live_state: Arc<dyn LiveStateReader>,
    request: &LiveStateScanRequest,
    matches: &FilesystemPathSelection,
    needs_blob_rows: bool,
) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
    if matches.is_empty() || !needs_blob_rows {
        return Ok(Vec::new());
    }
    let file_ids = matches
        .entries()
        .filter(|entry| entry.kind == FilesystemPathKind::File)
        .map(|entry| entry.id().to_string())
        .collect::<BTreeSet<_>>();
    if file_ids.is_empty() {
        return Ok(Vec::new());
    }
    scan_exact_file_blob_rows(live_state, request, &file_ids).await
}

async fn scan_exact_file_blob_rows(
    live_state: Arc<dyn LiveStateReader>,
    request: &LiveStateScanRequest,
    file_ids: &BTreeSet<String>,
) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
    if file_ids.is_empty() {
        return Ok(Vec::new());
    }
    if request.filter.branch_ids.is_empty() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "exact lix_file blob reads require resolved branch IDs",
        ));
    }

    let exact_rows = request
        .filter
        .branch_ids
        .iter()
        .flat_map(|branch_id| {
            file_ids
                .iter()
                .map(move |file_id| LiveStateExactRowRequest {
                    branch_id: branch_id.clone(),
                    schema_key: BLOB_REF_SCHEMA_KEY.to_string(),
                    entity_pk: EntityPk::single(file_id.clone()),
                    file_id: Some(file_id.clone()),
                })
        })
        .collect::<Vec<_>>();
    let rows = live_state
        .load_exact_rows(&LiveStateExactBatchRequest {
            rows: exact_rows,
            projection: request.projection.clone(),
            untracked: request.filter.untracked,
            include_tombstones: request.filter.include_tombstones,
        })
        .await?;
    Ok(rows.into_iter().flatten().collect())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum FileIdConstraint {
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

    fn allows(&self, file_id: &str) -> bool {
        match self {
            Self::All => true,
            Self::None => false,
            Self::Ids(file_ids) => file_ids.contains(file_id),
        }
    }
}

fn file_id_constraint_from_filters(filters: &[Expr]) -> Result<FileIdConstraint> {
    exact_string_column_constraint_from_filters(filters, "id")
}

fn exact_plugin_archive_delete_target_from_filters(filters: &[Expr]) -> Result<Option<String>> {
    let path_plugin_key = single_exact_string_constraint(
        exact_string_column_constraint_from_filters(filters, "path")?,
    )
    .as_deref()
    .and_then(plugin_key_from_archive_path);
    let id_plugin_key = single_exact_string_constraint(file_id_constraint_from_filters(filters)?)
        .as_deref()
        .and_then(plugin_key_from_archive_file_id);

    match (path_plugin_key, id_plugin_key) {
        (Some(path_key), Some(id_key)) if path_key != id_key => Ok(None),
        (Some(path_key), _) => Ok(Some(path_key)),
        (_, Some(id_key)) => Ok(Some(id_key)),
        (None, None) => Ok(None),
    }
}

fn single_exact_string_constraint(constraint: FileIdConstraint) -> Option<String> {
    let FileIdConstraint::Ids(values) = constraint else {
        return None;
    };
    if values.len() != 1 {
        return None;
    }
    values.into_iter().next()
}

pub(super) fn exact_string_column_constraint_from_filters(
    filters: &[Expr],
    column_name: &'static str,
) -> Result<FileIdConstraint> {
    let analyzer = ExactStringColumnFilterAnalyzer::new(column_name);
    let mut constraint = FileIdConstraint::All;
    for filter in filters {
        if let Some(filter_constraint) = analyzer.analyze(filter)? {
            constraint = constraint.intersect(filter_constraint);
        }
    }
    Ok(constraint)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum FilePathPredicate {
    All,
    Comparison {
        operation: FilePathComparison,
        value: String,
    },
    In(BTreeSet<String>),
    /// A conservative fast path for the MCP file search shape:
    /// `LOWER(path) LIKE '%ascii-lowercase-literal%'`. Other LIKE forms retain the
    /// regular residual-filter scan so SQL pattern semantics stay unchanged.
    LowercaseContains(String),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
}

impl FilePathPredicate {
    fn matches(&self, path: &str) -> bool {
        match self {
            Self::All => true,
            Self::Comparison { operation, value } => operation.matches(path, value),
            Self::In(values) => values.contains(path),
            Self::LowercaseContains(value) => path.to_lowercase().contains(value),
            Self::And(left, right) => left.matches(path) && right.matches(path),
            Self::Or(left, right) => left.matches(path) || right.matches(path),
        }
    }

    fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::All, predicate) | (predicate, Self::All) => predicate,
            (left, right) => Self::And(Box::new(left), Box::new(right)),
        }
    }

    fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::All, _) | (_, Self::All) => Self::All,
            (left, right) => Self::Or(Box::new(left), Box::new(right)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FilePathComparison {
    Equal,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

impl FilePathComparison {
    fn matches(self, path: &str, value: &str) -> bool {
        match self {
            Self::Equal => path == value,
            Self::LessThan => path < value,
            Self::LessThanOrEqual => path <= value,
            Self::GreaterThan => path > value,
            Self::GreaterThanOrEqual => path >= value,
        }
    }

    fn reversed(self) -> Self {
        match self {
            Self::Equal => Self::Equal,
            Self::LessThan => Self::GreaterThan,
            Self::LessThanOrEqual => Self::GreaterThanOrEqual,
            Self::GreaterThan => Self::LessThan,
            Self::GreaterThanOrEqual => Self::LessThanOrEqual,
        }
    }
}

pub(super) fn file_path_predicate_from_filters(filters: &[Expr]) -> FilePathPredicate {
    filters
        .iter()
        .fold(FilePathPredicate::All, |predicate, filter| {
            predicate.and(file_path_predicate_from_expr(filter))
        })
}

fn file_path_predicate_from_expr(expr: &Expr) -> FilePathPredicate {
    match expr {
        Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::And => {
            file_path_predicate_from_expr(&binary_expr.left)
                .and(file_path_predicate_from_expr(&binary_expr.right))
        }
        Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::Or => {
            file_path_predicate_from_expr(&binary_expr.left)
                .or(file_path_predicate_from_expr(&binary_expr.right))
        }
        Expr::BinaryExpr(binary_expr) => {
            file_path_comparison_from_binary_filter(binary_expr).unwrap_or(FilePathPredicate::All)
        }
        Expr::InList(in_list) => file_path_in_predicate(in_list),
        _ => FilePathPredicate::All,
    }
}

/// Extract only AND-connected `LOWER(path) LIKE '%literal%'` terms for read
/// scans. DML keeps using [`file_path_predicate_from_filters`] unchanged.
fn lower_path_contains_predicate_from_filters(filters: &[Expr]) -> FilePathPredicate {
    filters
        .iter()
        .fold(FilePathPredicate::All, |predicate, filter| {
            predicate.and(lower_path_contains_predicate_from_expr(filter))
        })
}

fn lower_path_contains_predicate_from_expr(expr: &Expr) -> FilePathPredicate {
    match expr {
        Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::And => {
            lower_path_contains_predicate_from_expr(&binary_expr.left)
                .and(lower_path_contains_predicate_from_expr(&binary_expr.right))
        }
        Expr::Like(like) => lower_path_contains_predicate(like).unwrap_or(FilePathPredicate::All),
        _ => FilePathPredicate::All,
    }
}

pub(super) fn indexed_path_matches(
    index: Arc<crate::filesystem::FilesystemPathIndex>,
    predicate: &FilePathPredicate,
    kind: FilesystemPathKind,
) -> FilesystemPathSelection {
    fn entries(
        index: &crate::filesystem::FilesystemPathIndex,
        predicate: &FilePathPredicate,
        kind: FilesystemPathKind,
    ) -> BTreeMap<(FilesystemPathKind, FilesystemDescriptorKey), Arc<FilesystemPathEntry>> {
        let candidates = match predicate {
            FilePathPredicate::All => index.entries(),
            FilePathPredicate::Comparison { operation, value } => match operation {
                FilePathComparison::Equal => index.exact_entries(value),
                FilePathComparison::LessThan => index.range_entries(
                    std::ops::Bound::Unbounded,
                    std::ops::Bound::Excluded(value.as_str()),
                ),
                FilePathComparison::LessThanOrEqual => index.range_entries(
                    std::ops::Bound::Unbounded,
                    std::ops::Bound::Included(value.as_str()),
                ),
                FilePathComparison::GreaterThan => index.range_entries(
                    std::ops::Bound::Excluded(value.as_str()),
                    std::ops::Bound::Unbounded,
                ),
                FilePathComparison::GreaterThanOrEqual => index.range_entries(
                    std::ops::Bound::Included(value.as_str()),
                    std::ops::Bound::Unbounded,
                ),
            },
            FilePathPredicate::In(values) => values
                .iter()
                .flat_map(|value| index.exact_entries(value))
                .collect(),
            FilePathPredicate::LowercaseContains(_) => index.entries(),
            FilePathPredicate::And(left, right) => {
                let left = entries(index, left, kind);
                let right = entries(index, right, kind);
                return left
                    .into_iter()
                    .filter(|(identity, _)| right.contains_key(identity))
                    .collect();
            }
            FilePathPredicate::Or(left, right) => {
                let mut matches = entries(index, left, kind);
                matches.extend(entries(index, right, kind));
                return matches;
            }
        };
        candidates
            .into_iter()
            .filter(|entry| entry.kind == kind && predicate.matches(&entry.path))
            .map(|entry| ((entry.kind, entry.key.clone()), entry))
            .collect()
    }

    let mut entries = entries(&index, predicate, kind)
        .into_values()
        .collect::<Vec<_>>();
    entries.sort_unstable_by(|left, right| {
        left.path
            .cmp(&right.path)
            .then_with(|| left.key.cmp(&right.key))
            .then_with(|| left.kind.cmp(&right.kind))
    });
    FilesystemPathSelection::new(index, entries)
}

fn indexed_file_matches(
    index: Arc<crate::filesystem::FilesystemPathIndex>,
    predicate: &FilePathPredicate,
) -> FilesystemPathSelection {
    indexed_path_matches(index, predicate, FilesystemPathKind::File)
}

fn indexed_file_id_matches(
    index: Arc<crate::filesystem::FilesystemPathIndex>,
    file_ids: &BTreeSet<String>,
    path_predicate: &FilePathPredicate,
) -> FilesystemPathSelection {
    let mut entries = file_ids
        .iter()
        .flat_map(|file_id| index.exact_file_id_entries(file_id))
        .filter(|entry| {
            debug_assert_eq!(entry.kind, FilesystemPathKind::File);
            path_predicate.matches(&entry.path)
        })
        .collect::<Vec<_>>();
    // Each equal-ID range is path-ordered, but multiple ranges arrive in ID
    // order. Restore the primary index order promised to DataFusion before
    // LIMIT is applied.
    entries.sort_unstable_by(|left, right| {
        left.path
            .cmp(&right.path)
            .then_with(|| left.key.cmp(&right.key))
    });
    FilesystemPathSelection::new(index, entries)
}

fn indexed_file_directory_matches(
    index: Arc<crate::filesystem::FilesystemPathIndex>,
    directory_ids: &BTreeSet<String>,
    file_ids: Option<&BTreeSet<String>>,
    path_predicate: &FilePathPredicate,
) -> FilesystemPathSelection {
    let entries = index
        .entries()
        .into_iter()
        .filter(|entry| {
            entry.kind == FilesystemPathKind::File
                && entry
                    .parent_id
                    .as_ref()
                    .is_some_and(|directory_id| directory_ids.contains(directory_id))
                && file_ids.is_none_or(|file_ids| file_ids.contains(entry.id()))
                && path_predicate.matches(&entry.path)
        })
        .collect();
    FilesystemPathSelection::new(index, entries)
}

fn indexed_file_root_matches(
    index: Arc<crate::filesystem::FilesystemPathIndex>,
    file_ids: &FileIdConstraint,
    path_predicate: &FilePathPredicate,
) -> FilesystemPathSelection {
    let entries = index
        .entries()
        .into_iter()
        .filter(|entry| {
            entry.kind == FilesystemPathKind::File
                && entry.parent_id.is_none()
                && file_ids.allows(entry.id())
                && path_predicate.matches(&entry.path)
        })
        .collect();
    FilesystemPathSelection::new(index, entries)
}

fn is_null_column_filter(expr: &Expr, column_name: &str) -> bool {
    matches!(
        expr,
        Expr::IsNull(inner)
            if matches!(inner.as_ref(), Expr::Column(column) if column.name == column_name)
    )
}

fn file_path_comparison_from_binary_filter(binary_expr: &BinaryExpr) -> Option<FilePathPredicate> {
    let operation = match binary_expr.op {
        Operator::Eq => FilePathComparison::Equal,
        Operator::Lt => FilePathComparison::LessThan,
        Operator::LtEq => FilePathComparison::LessThanOrEqual,
        Operator::Gt => FilePathComparison::GreaterThan,
        Operator::GtEq => FilePathComparison::GreaterThanOrEqual,
        _ => return None,
    };
    let direct = string_column_literal_filter(&binary_expr.left, &binary_expr.right, "path")
        .map(|value| (operation, value));
    let (operation, value) = direct.or_else(|| {
        string_column_literal_filter(&binary_expr.right, &binary_expr.left, "path")
            .map(|value| (operation.reversed(), value))
    })?;
    Some(FilePathPredicate::Comparison { operation, value })
}

fn file_path_in_predicate(in_list: &InList) -> FilePathPredicate {
    if in_list.negated {
        return FilePathPredicate::All;
    }
    let Expr::Column(column) = in_list.expr.as_ref() else {
        return FilePathPredicate::All;
    };
    if column.name != "path" {
        return FilePathPredicate::All;
    }
    let Some(values) = in_list
        .list
        .iter()
        .map(string_expr_literal)
        .collect::<Option<BTreeSet<_>>>()
    else {
        return FilePathPredicate::All;
    };
    FilePathPredicate::In(values)
}

fn lower_path_contains_predicate(like: &Like) -> Option<FilePathPredicate> {
    if like.negated || like.case_insensitive || like.escape_char.is_some() {
        return None;
    }
    let Expr::ScalarFunction(function) = like.expr.as_ref() else {
        return None;
    };
    if function.name() != "lower"
        || !matches!(function.args.as_slice(), [Expr::Column(column)] if column.name == "path")
    {
        return None;
    }
    let pattern = string_expr_literal(&like.pattern)?;
    let literal = pattern.strip_prefix('%')?.strip_suffix('%')?;
    if literal.is_empty()
        || !literal.is_ascii()
        || literal.bytes().any(|byte| byte.is_ascii_uppercase())
        || literal.contains('%')
        || literal.contains('_')
        || literal.contains('\\')
    {
        return None;
    }
    Some(FilePathPredicate::LowercaseContains(literal.to_string()))
}

fn string_column_literal_filter(
    column_expr: &Expr,
    literal_expr: &Expr,
    column_name: &str,
) -> Option<String> {
    let Expr::Column(column) = column_expr else {
        return None;
    };
    if column.name != column_name {
        return None;
    }
    string_expr_literal(literal_expr)
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
    if let Expr::Literal(value, _) = expr {
        if !scalar_is_binary_or_null(value) {
            return Err(non_binary_lix_file_data_assignment_error());
        }
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
        Field::new("data", DataType::LargeBinary, false),
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
    use std::hint::black_box;
    use std::io::{Cursor, Write};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use async_trait::async_trait;
    use datafusion::arrow::array::{
        Array, ArrayRef, BinaryArray, BooleanArray, LargeBinaryArray, StringArray,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::{Column, ScalarValue};
    use datafusion::execution::TaskContext;
    use datafusion::execution::context::ExecutionProps;
    use datafusion::logical_expr::expr::{Cast, InList, Like, ScalarFunction};
    use datafusion::logical_expr::lit;
    use datafusion::logical_expr::{
        BinaryExpr, ColumnarValue, Expr, Operator, TableProviderFilterPushDown, Volatility,
        create_udf,
    };
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::Literal;
    use serde_json::Value as JsonValue;

    use crate::binary_cas::{BlobBytesBatch, BlobDataReader, BlobHash};
    use crate::branch::{BranchHead, BranchRefReader};
    use crate::changelog::{ChangeId, CommitId};
    use crate::filesystem::{
        FilesystemBlobRefKey, FilesystemDescriptorKey, FilesystemPathIndex,
        FilesystemPathIndexReader, FilesystemPathIndexRequest, FilesystemRowContext,
    };
    use crate::functions::FunctionProviderHandle;
    use crate::live_state::{
        LiveStateExactBatchRequest, LiveStateFilter, LiveStateReader, LiveStateRowRequest,
        LiveStateScanRequest, MaterializedLiveStateRow,
    };
    use crate::plugin::{
        PLUGIN_OWNER_KEY, PLUGIN_REGISTRY_KEY, PluginContentType, PluginFileOwner, PluginRegistry,
        PluginRegistryEntry, PluginRegistryEntryInput, PluginRuntime, PluginRuntimeHost,
        plugin_storage_archive_file_id, plugin_storage_archive_path,
    };
    use crate::sql2::dml::InsertSink;
    use crate::sql2::providers::upsert::UpsertConflictTarget;
    use crate::sql2::{SqlWriteContext, SqlWriteExecutionContext, WriteContextBranchRefReader};
    use crate::transaction::types::{
        TransactionJson, TransactionWrite, TransactionWriteMode, TransactionWriteOutcome,
    };
    use crate::wasm::{
        UnsupportedWasmRuntime, WasmComponentInstance, WasmLimits, WasmPluginDetectedChange,
        WasmPluginEntityState, WasmPluginFile, WasmRuntime,
    };
    use crate::{LixError, NullableKeyFilter};

    use super::{
        BranchBinding, DirectoryDescriptorRecord, LixFileInsertSink, LixFileSpec, TableSpec,
        UpsertSupport, derive_directory_paths, lix_file_delete_stage_from_batch,
        lix_file_insert_stage_from_batch, lix_file_insert_stage_from_batch_with_path_resolvers,
        lix_file_write_rows_from_batch,
    };

    fn test_id_generator(ids: &'static [&'static str]) -> impl FnMut() -> String {
        let mut ids = ids.iter();
        move || ids.next().expect("test id should exist").to_string()
    }

    fn test_functions() -> FunctionProviderHandle {
        FunctionProviderHandle::system()
    }

    #[test]
    #[ignore = "filesystem path index exact-ID lifecycle benchmark probe"]
    fn filesystem_path_id_lookup_benchmark_probe() {
        let file_count = benchmark_env_usize("LIX_PATH_INDEX_BENCH_FILES", 10_000);
        let operation = std::env::var("LIX_PATH_INDEX_BENCH_OPERATION")
            .unwrap_or_else(|_| "lookup".to_string());
        let default_rounds = if operation == "build" { 20 } else { 2_000 };
        let default_warmups = if operation == "build" { 3 } else { 100 };
        let rounds = benchmark_env_usize("LIX_PATH_INDEX_BENCH_ROUNDS", default_rounds);
        let warmups = benchmark_env_usize("LIX_PATH_INDEX_BENCH_WARMUPS", default_warmups);
        assert!(file_count > 0, "benchmark needs at least one file");
        assert!(rounds > 0, "benchmark needs at least one measured round");

        let id_order = benchmark_shuffled_indices(file_count);
        let target_id = format!("file-{:08}", id_order[file_count - 1]);
        let rows = id_order
            .into_iter()
            .enumerate()
            .map(|(path_index, id_index)| {
                let id = format!("file-{id_index:08}");
                let snapshot = serde_json::json!({
                    "id": id,
                    "directory_id": JsonValue::Null,
                    "name": format!("path-{path_index:08}.txt"),
                })
                .to_string();
                live_file_row(&id, "branch-b", &snapshot)
            })
            .collect::<Vec<_>>();

        let mut samples = Vec::with_capacity(rounds);
        let mut heap_bytes = 0;
        match operation.as_str() {
            "lookup" => {
                let index = Arc::new(
                    FilesystemPathIndex::from_live_rows(rows)
                        .expect("benchmark path index should build"),
                );
                heap_bytes = index.estimated_heap_bytes();
                let target_ids = BTreeSet::from([target_id]);
                for iteration in 0..warmups.saturating_add(rounds) {
                    let started = Instant::now();
                    let selection = super::indexed_file_id_matches(
                        Arc::clone(&index),
                        &target_ids,
                        &super::FilePathPredicate::All,
                    );
                    let elapsed = started.elapsed();
                    assert_eq!(black_box(selection.len()), 1);
                    if iteration >= warmups {
                        samples.push(elapsed);
                    }
                }
            }
            "build" => {
                for iteration in 0..warmups.saturating_add(rounds) {
                    let input = rows.clone();
                    let started = Instant::now();
                    let index = FilesystemPathIndex::from_live_rows(input)
                        .expect("benchmark path index should build");
                    let elapsed = started.elapsed();
                    heap_bytes = index.estimated_heap_bytes();
                    assert_eq!(
                        black_box(index.kind_count(super::FilesystemPathKind::File)),
                        file_count
                    );
                    if iteration >= warmups {
                        samples.push(elapsed);
                    }
                }
            }
            other => {
                panic!("LIX_PATH_INDEX_BENCH_OPERATION must be lookup or build; got {other:?}")
            }
        }

        samples.sort_unstable();
        let p50 = benchmark_percentile(&samples, 50);
        let p95 = benchmark_percentile(&samples, 95);
        eprintln!(
            "filesystem_path_id_probe operation={operation} files={file_count} rounds={rounds} p50_ns={} p95_ns={} heap_bytes={heap_bytes} heap_bytes_per_file={}",
            p50.as_nanos(),
            p95.as_nanos(),
            heap_bytes / file_count,
        );
    }

    fn benchmark_env_usize(name: &str, default: usize) -> usize {
        std::env::var(name).map_or(default, |value| {
            value
                .parse::<usize>()
                .unwrap_or_else(|error| panic!("{name} must be an integer: {error}"))
        })
    }

    fn benchmark_percentile(samples: &[Duration], percentile: usize) -> Duration {
        samples[(samples.len() - 1) * percentile / 100]
    }

    fn benchmark_shuffled_indices(len: usize) -> Vec<usize> {
        let mut indices = (0..len).collect::<Vec<_>>();
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        for upper in (1..len).rev() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let modulus = u64::try_from(upper + 1).expect("benchmark size should fit u64");
            let selected = usize::try_from(state % modulus)
                .expect("shuffled benchmark index should fit usize");
            indices.swap(upper, selected);
        }
        indices
    }

    #[test]
    fn indexed_file_id_matches_restores_path_order_and_applies_path_predicate() {
        let index = Arc::new(
            FilesystemPathIndex::from_live_rows(vec![
                live_file_row(
                    "file-z",
                    "branch-b",
                    r#"{"id":"file-z","directory_id":null,"name":"a.txt"}"#,
                ),
                live_file_row(
                    "file-a",
                    "branch-b",
                    r#"{"id":"file-a","directory_id":null,"name":"z.txt"}"#,
                ),
                live_file_row(
                    "file-middle",
                    "branch-b",
                    r#"{"id":"file-middle","directory_id":null,"name":"middle.txt"}"#,
                ),
            ])
            .expect("filesystem path index should build"),
        );
        let ids = BTreeSet::from(["file-a".to_string(), "file-z".to_string()]);

        let matches = super::indexed_file_id_matches(
            Arc::clone(&index),
            &ids,
            &super::FilePathPredicate::All,
        );
        assert_eq!(
            matches
                .entries()
                .map(|entry| entry.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/a.txt", "/z.txt"]
        );

        let filtered = super::indexed_file_id_matches(
            index,
            &ids,
            &super::FilePathPredicate::Comparison {
                operation: super::FilePathComparison::GreaterThan,
                value: "/middle.txt".to_string(),
            },
        );
        assert_eq!(
            filtered
                .entries()
                .map(|entry| entry.path.as_str())
                .collect::<Vec<_>>(),
            vec!["/z.txt"]
        );
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
    fn plugin_archive_delete_target_requires_one_exact_canonical_path_or_id() {
        let path = "/.lix/plugins/plugin_sentinel.lixplugin";
        let file_id = "lix_plugin_archive::plugin_sentinel";

        for filters in [
            vec![eq_filter("path", path)],
            vec![eq_filter("id", file_id)],
        ] {
            assert_eq!(
                super::exact_plugin_archive_delete_target_from_filters(&filters).unwrap(),
                Some("plugin_sentinel".to_string())
            );
        }

        for filters in [
            Vec::new(),
            vec![lower_path_contains_filter("%/.lix/plugins/%")],
            vec![Expr::InList(InList::new(
                Box::new(column("path")),
                vec![
                    string_literal(path),
                    string_literal("/.lix/plugins/plugin_other.lixplugin"),
                ],
                false,
            ))],
        ] {
            assert_eq!(
                super::exact_plugin_archive_delete_target_from_filters(&filters).unwrap(),
                None
            );
        }
    }

    #[test]
    fn plugin_archive_delete_target_rejects_conflicting_exact_identities() {
        assert_eq!(
            super::exact_plugin_archive_delete_target_from_filters(&[
                eq_filter("path", "/.lix/plugins/plugin_sentinel.lixplugin"),
                eq_filter("id", "lix_plugin_archive::plugin_other"),
            ])
            .unwrap(),
            None
        );
    }

    #[test]
    fn file_path_predicates_support_atelier_equality_and_range_filters() {
        let predicate = super::file_path_predicate_from_filters(&[
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(column("path")),
                Operator::GtEq,
                Box::new(string_literal("/extensions/")),
            )),
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(column("path")),
                Operator::Lt,
                Box::new(string_literal("/extensions0")),
            )),
        ]);

        assert!(predicate.matches("/extensions/example.js"));
        assert!(!predicate.matches("/extension.txt"));
        assert!(!predicate.matches("/extensions0"));

        let reversed_equality =
            super::file_path_predicate_from_filters(&[Expr::BinaryExpr(BinaryExpr::new(
                Box::new(string_literal("/readme.md")),
                Operator::Eq,
                Box::new(column("path")),
            ))]);
        assert!(reversed_equality.matches("/readme.md"));
        assert!(!reversed_equality.matches("/other.md"));
    }

    #[test]
    fn file_path_predicates_select_ascii_lower_path_contains_searches() {
        let predicate = super::file_path_predicate_from_filters(&[
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(column("path")),
                Operator::GtEq,
                Box::new(string_literal("/docs/")),
            )),
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(column("path")),
                Operator::Lt,
                Box::new(string_literal("/docs0")),
            )),
        ]);
        let indexed_predicate =
            predicate.and(super::lower_path_contains_predicate_from_filters(&[
                lower_path_contains_filter("%readme%"),
            ]));

        assert!(indexed_predicate.matches("/docs/README.md"));
        assert!(!indexed_predicate.matches("/docs/changelog.md"));
        assert!(!indexed_predicate.matches("/other/readme.md"));

        assert_eq!(
            super::file_path_predicate_from_filters(&[lower_path_contains_filter("%readme%")]),
            super::FilePathPredicate::All,
            "DML path predicates should not gain a LOWER LIKE fast path",
        );

        for filter in [
            lower_path_contains_filter("%read_me%"),
            lower_path_contains_filter("%read\\me%"),
            lower_path_contains_filter("%résumé%"),
            lower_path_contains_filter("%ReadMe%"),
            lower_path_contains_filter("%README%"),
            lower_path_contains_filter("readme%"),
            Expr::Not(Box::new(lower_path_contains_filter("%readme%"))),
            lower_path_contains_filter_with_options("%readme%", true, None, false),
            lower_path_contains_filter_with_options("%readme%", false, None, true),
            lower_path_contains_filter_with_options("%readme%", false, Some('\\'), false),
        ] {
            assert_eq!(
                super::lower_path_contains_predicate_from_filters(&[filter]),
                super::FilePathPredicate::All,
                "unsupported LIKE shape should retain the residual scan",
            );
        }

        let disjunction = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(lower_path_contains_filter("%readme%")),
            Operator::Or,
            Box::new(eq_filter("path", "/docs/readme.md")),
        ));
        assert_eq!(
            super::lower_path_contains_predicate_from_filters(&[disjunction]),
            super::FilePathPredicate::All,
            "OR-connected LIKE terms must retain the residual scan",
        );

        assert!(
            super::lower_path_contains_predicate_from_filters(&[lower_path_contains_filter(
                "%docs/readme.md%"
            ),])
            .matches("/docs/readme.md")
        );
    }

    #[test]
    fn file_path_predicates_stay_conservative_across_boolean_filters() {
        let path_filter = eq_filter("path", "/readme.md");
        let id_filter = eq_filter("id", "file-other");
        let conjunction =
            super::file_path_predicate_from_filters(&[Expr::BinaryExpr(BinaryExpr::new(
                Box::new(path_filter.clone()),
                Operator::And,
                Box::new(id_filter.clone()),
            ))]);
        assert!(conjunction.matches("/readme.md"));
        assert!(!conjunction.matches("/other.md"));

        let disjunction = super::file_path_predicate_from_filters(&[Expr::BinaryExpr(
            BinaryExpr::new(Box::new(path_filter), Operator::Or, Box::new(id_filter)),
        )]);
        assert!(disjunction.matches("/readme.md"));
        assert!(disjunction.matches("/other.md"));
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

    #[test]
    fn descriptor_only_scans_use_the_filesystem_path_index() {
        assert!(super::should_use_path_index(
            &super::FilePathPredicate::All,
            false,
        ));
        assert!(!super::should_use_path_index(
            &super::FilePathPredicate::All,
            true,
        ));
        assert!(super::should_use_path_index(
            &super::FilePathPredicate::Comparison {
                operation: super::FilePathComparison::Equal,
                value: "/readme.md".to_string(),
            },
            true,
        ));
    }

    #[tokio::test]
    async fn descriptor_only_scan_materializes_index_columns_without_live_rows() {
        let live_state_scans = Arc::new(AtomicUsize::new(0));
        let path_index_requests = Arc::new(AtomicUsize::new(0));
        let mut file = live_file_row(
            "file-readme",
            "branch-b",
            r#"{"id":"file-readme","directory_id":"dir-docs","name":"readme.md"}"#,
        );
        file.metadata = Some(r#"{"source":"index"}"#.to_string());
        let index = Arc::new(
            FilesystemPathIndex::from_live_rows(vec![
                live_directory_row(
                    "dir-docs",
                    "branch-b",
                    r#"{"id":"dir-docs","parent_id":null,"name":"docs"}"#,
                ),
                file,
            ])
            .expect("filesystem path index should build"),
        );
        let spec = LixFileSpec::active_branch(
            "branch-b",
            Arc::new(RejectingLiveStateReader {
                scan_count: Arc::clone(&live_state_scans),
            }),
            Arc::new(StaticFilesystemPathIndexReader {
                index,
                request_count: Arc::clone(&path_index_requests),
            }),
            Arc::new(TestBranchRefReader),
            Arc::new(StaticBlobReader::from_blobs(Vec::new())),
            PluginRuntimeHost::new(Arc::new(UnsupportedWasmRuntime)),
            test_functions(),
        );
        let projection = [
            "id",
            "path",
            "directory_id",
            "name",
            "lixcol_entity_pk",
            "lixcol_schema_key",
            "lixcol_commit_id",
            "lixcol_metadata",
        ]
        .into_iter()
        .map(|column_name| {
            spec.schema()
                .index_of(column_name)
                .expect("descriptor column should exist")
        })
        .collect::<Vec<_>>();

        let planned = spec
            .plan_scan(Some(&projection), &[], None, &ExecutionProps::new())
            .await
            .expect("descriptor-only scan should plan");
        let batch = (planned.load)()
            .await
            .expect("descriptor-only scan should load");

        assert_eq!(batch.num_rows(), 1);
        let string_value = |column_name: &str| {
            batch
                .column(batch.schema().index_of(column_name).unwrap())
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("descriptor column should be string data")
                .value(0)
        };
        assert_eq!(string_value("id"), "file-readme");
        assert_eq!(string_value("path"), "/docs/readme.md");
        assert_eq!(string_value("directory_id"), "dir-docs");
        assert_eq!(string_value("name"), "readme.md");
        assert_eq!(string_value("lixcol_entity_pk"), "[\"file-readme\"]");
        assert_eq!(
            string_value("lixcol_schema_key"),
            super::FILE_DESCRIPTOR_SCHEMA_KEY
        );
        assert_eq!(
            string_value("lixcol_commit_id"),
            CommitId::for_test_label("commit-file-readme").to_string()
        );
        assert_eq!(string_value("lixcol_metadata"), r#"{"source":"index"}"#);
        assert_eq!(path_index_requests.load(Ordering::SeqCst), 1);
        assert_eq!(live_state_scans.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn filter_free_descriptor_scan_pushes_projection_and_limit_into_path_selection() {
        let live_state_scans = Arc::new(AtomicUsize::new(0));
        let path_index_requests = Arc::new(AtomicUsize::new(0));
        let index = Arc::new(
            FilesystemPathIndex::from_live_rows(vec![
                live_file_row(
                    "file-c",
                    "branch-b",
                    r#"{"id":"file-c","directory_id":null,"name":"c.txt"}"#,
                ),
                live_file_row(
                    "file-a",
                    "branch-b",
                    r#"{"id":"file-a","directory_id":null,"name":"a.txt"}"#,
                ),
                live_file_row(
                    "file-b",
                    "branch-b",
                    r#"{"id":"file-b","directory_id":null,"name":"b.txt"}"#,
                ),
            ])
            .expect("filesystem path index should build"),
        );
        let spec = LixFileSpec::active_branch(
            "branch-b",
            Arc::new(RejectingLiveStateReader {
                scan_count: Arc::clone(&live_state_scans),
            }),
            Arc::new(StaticFilesystemPathIndexReader {
                index,
                request_count: Arc::clone(&path_index_requests),
            }),
            Arc::new(TestBranchRefReader),
            Arc::new(StaticBlobReader::from_blobs(Vec::new())),
            PluginRuntimeHost::new(Arc::new(UnsupportedWasmRuntime)),
            test_functions(),
        );
        let id_projection = vec![spec.schema().index_of("id").expect("id column")];

        let planned = spec
            .plan_scan(Some(&id_projection), &[], Some(1), &ExecutionProps::new())
            .await
            .expect("limited descriptor-only scan should plan");
        assert_eq!(planned.ordering.as_deref(), Some("path"));
        let batch = (planned.load)()
            .await
            .expect("limited descriptor-only scan should load");

        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.schema().field(0).name(), "id");
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("id should be string data")
                .value(0),
            "file-a"
        );

        let empty_projection = Vec::new();
        let planned = spec
            .plan_scan(
                Some(&empty_projection),
                &[],
                Some(2),
                &ExecutionProps::new(),
            )
            .await
            .expect("count-style descriptor scan should plan");
        let batch = (planned.load)()
            .await
            .expect("count-style descriptor scan should load");
        assert_eq!(batch.num_columns(), 0);
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(path_index_requests.load(Ordering::SeqCst), 2);
        assert_eq!(live_state_scans.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn by_branch_descriptor_scan_keeps_scope_columns_and_residual_filtering() {
        let live_state_scans = Arc::new(AtomicUsize::new(0));
        let path_index_requests = Arc::new(AtomicUsize::new(0));
        let mut target = live_file_row(
            "file-target",
            "branch-b",
            r#"{"id":"file-target","directory_id":null,"name":"readme.md"}"#,
        );
        target.file_id = Some("remote-file-target".to_string());
        target.untracked = true;
        let index = Arc::new(
            FilesystemPathIndex::from_live_rows(vec![
                live_file_row(
                    "file-other",
                    "branch-a",
                    r#"{"id":"file-other","directory_id":null,"name":"readme.md"}"#,
                ),
                target,
            ])
            .expect("filesystem path index should build"),
        );
        let spec = LixFileSpec::by_branch(
            Arc::new(RejectingLiveStateReader {
                scan_count: Arc::clone(&live_state_scans),
            }),
            Arc::new(StaticFilesystemPathIndexReader {
                index,
                request_count: Arc::clone(&path_index_requests),
            }),
            Arc::new(TestBranchRefReader),
            Arc::new(StaticBlobReader::from_blobs(Vec::new())),
            PluginRuntimeHost::new(Arc::new(UnsupportedWasmRuntime)),
            test_functions(),
        );
        let projection = [
            "id",
            "lixcol_file_id",
            "lixcol_global",
            "lixcol_untracked",
            "lixcol_created_at",
            "lixcol_updated_at",
            "lixcol_branch_id",
        ]
        .into_iter()
        .map(|column_name| {
            spec.schema()
                .index_of(column_name)
                .expect("by-branch descriptor column should exist")
        })
        .collect::<Vec<_>>();
        let filters = vec![eq_filter("lixcol_branch_id", "branch-b")];

        let planned = spec
            .plan_scan(Some(&projection), &filters, Some(1), &ExecutionProps::new())
            .await
            .expect("by-branch descriptor scan should plan");
        let batch = (planned.load)()
            .await
            .expect("by-branch descriptor scan should load");

        assert_eq!(batch.num_rows(), 1);
        let string_value = |column_name: &str| {
            batch
                .column(batch.schema().index_of(column_name).unwrap())
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("descriptor column should be string data")
                .value(0)
        };
        let boolean_value = |column_name: &str| {
            batch
                .column(batch.schema().index_of(column_name).unwrap())
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("descriptor column should be boolean data")
                .value(0)
        };
        assert_eq!(string_value("id"), "file-target");
        assert_eq!(string_value("lixcol_file_id"), "remote-file-target");
        assert!(!boolean_value("lixcol_global"));
        assert!(boolean_value("lixcol_untracked"));
        assert_eq!(string_value("lixcol_created_at"), "2026-04-23T00:00:00Z");
        assert_eq!(string_value("lixcol_updated_at"), "2026-04-23T01:00:00Z");
        assert_eq!(string_value("lixcol_branch_id"), "branch-b");
        assert_eq!(path_index_requests.load(Ordering::SeqCst), 1);
        assert_eq!(live_state_scans.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn file_id_data_scan_uses_indexed_descriptor_and_only_scans_blob_rows() {
        let data = b"readme contents".to_vec();
        let blob_hash = BlobHash::from_content(&data).to_hex();
        let live_state_requests = Arc::new(Mutex::new(Vec::new()));
        let path_index_requests = Arc::new(AtomicUsize::new(0));
        let index = Arc::new(
            FilesystemPathIndex::from_live_rows(vec![live_file_row(
                "file-readme",
                "branch-b",
                r#"{"id":"file-readme","directory_id":null,"name":"readme.md"}"#,
            )])
            .expect("filesystem path index should build"),
        );
        let spec = LixFileSpec::active_branch(
            "branch-b",
            Arc::new(RecordingLiveStateReader {
                rows: vec![live_blob_ref_row(
                    "file-readme",
                    "branch-b",
                    "file-readme",
                    &blob_hash,
                    data.len(),
                )],
                scan_requests: Arc::clone(&live_state_requests),
            }),
            Arc::new(StaticFilesystemPathIndexReader {
                index,
                request_count: Arc::clone(&path_index_requests),
            }),
            Arc::new(TestBranchRefReader),
            Arc::new(StaticBlobReader::from_blobs(vec![data.clone()])),
            PluginRuntimeHost::new(Arc::new(UnsupportedWasmRuntime)),
            test_functions(),
        );
        let projection = vec![spec.schema().index_of("data").expect("data column")];
        let filters = vec![eq_filter("id", "file-readme")];

        let planned = spec
            .plan_scan(Some(&projection), &filters, None, &ExecutionProps::new())
            .await
            .expect("file-id data scan should plan");
        let batch = (planned.load)()
            .await
            .expect("file-id data scan should load");

        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .expect("data column should be binary data");
        assert_eq!(values.value(0), data.as_slice());
        assert_eq!(path_index_requests.load(Ordering::SeqCst), 1);
        let requests = live_state_requests
            .lock()
            .expect("live-state request mutex should not be poisoned");
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].filter.schema_keys,
            vec![super::BLOB_REF_SCHEMA_KEY.to_string()]
        );
    }

    #[tokio::test]
    async fn lower_path_contains_scan_loads_blob_rows_only_for_the_matching_find_files_projection()
    {
        let selected_data = b"selected contents".to_vec();
        let selected_blob_hash = BlobHash::from_content(&selected_data).to_hex();
        let changelog_data = b"changelog contents".to_vec();
        let changelog_blob_hash = BlobHash::from_content(&changelog_data).to_hex();
        let outside_data = b"outside contents".to_vec();
        let outside_blob_hash = BlobHash::from_content(&outside_data).to_hex();
        let selected_change_id = ChangeId::for_test_label("selected-search-blob");
        let live_state_requests = Arc::new(Mutex::new(Vec::new()));
        let index = Arc::new(
            FilesystemPathIndex::from_live_rows(vec![
                live_directory_row(
                    "dir-docs",
                    "branch-b",
                    r#"{"id":"dir-docs","parent_id":null,"name":"Docs"}"#,
                ),
                live_directory_row(
                    "dir-other",
                    "branch-b",
                    r#"{"id":"dir-other","parent_id":null,"name":"Other"}"#,
                ),
                live_file_row(
                    "file-selected",
                    "branch-b",
                    r#"{"id":"file-selected","directory_id":"dir-docs","name":"README.md"}"#,
                ),
                live_file_row(
                    "file-changelog",
                    "branch-b",
                    r#"{"id":"file-changelog","directory_id":"dir-docs","name":"changelog.md"}"#,
                ),
                live_file_row(
                    "file-outside",
                    "branch-b",
                    r#"{"id":"file-outside","directory_id":"dir-other","name":"README.md"}"#,
                ),
            ])
            .expect("filesystem path index should build"),
        );
        let path_predicate = super::file_path_predicate_from_filters(&[
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(column("path")),
                Operator::GtEq,
                Box::new(string_literal("/Docs/")),
            )),
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(column("path")),
                Operator::Lt,
                Box::new(string_literal("/Docs0")),
            )),
        ]);
        let indexed_path_predicate =
            path_predicate.and(super::lower_path_contains_predicate_from_filters(&[
                lower_path_contains_filter("%readme%"),
            ]));
        let matches = super::indexed_file_matches(Arc::clone(&index), &indexed_path_predicate);
        assert_eq!(
            matches
                .entries()
                .map(|entry| entry.id().to_owned())
                .collect::<Vec<_>>(),
            vec!["file-selected".to_string()],
            "the range and contains predicates should exclude both the local non-match and outside root",
        );

        let mut selected_blob = live_blob_ref_row(
            "file-selected",
            "branch-b",
            "file-selected",
            &selected_blob_hash,
            selected_data.len(),
        );
        selected_blob.change_id = Some(selected_change_id);
        let live_state: Arc<dyn LiveStateReader> = Arc::new(RecordingLiveStateReader {
            rows: vec![
                selected_blob,
                live_blob_ref_row(
                    "file-changelog",
                    "branch-b",
                    "file-changelog",
                    &changelog_blob_hash,
                    changelog_data.len(),
                ),
                live_blob_ref_row(
                    "file-outside",
                    "branch-b",
                    "file-outside",
                    &outside_blob_hash,
                    outside_data.len(),
                ),
            ],
            scan_requests: Arc::clone(&live_state_requests),
        });
        let base_schema = super::lix_file_schema();
        let find_files_projection = vec![
            base_schema.index_of("path").expect("path column"),
            base_schema.index_of("name").expect("name column"),
            base_schema
                .index_of("lixcol_metadata")
                .expect("metadata column"),
            base_schema
                .index_of("lixcol_change_id")
                .expect("change-id column"),
            base_schema
                .index_of("lixcol_updated_at")
                .expect("updated-at column"),
        ];
        let projected_schema = super::projected_schema(&base_schema, Some(&find_files_projection))
            .expect("findFiles projection should be valid");
        let request = super::lix_file_scan_request(Some("branch-b"), Some(&projected_schema), None);
        let rows = super::scan_indexed_file_rows(Arc::clone(&live_state), &request, &matches, true)
            .await
            .expect("matching blob rows should load");
        let prepared = super::prepare_indexed_lix_file_rows(&matches, rows)
            .expect("indexed rows should prepare");
        let blob_reader: Arc<dyn BlobDataReader> =
            Arc::new(StaticBlobReader::from_blobs(Vec::new()));
        let batch = super::lix_file_record_batch_from_prepared(
            &projected_schema,
            &blob_reader,
            None,
            false,
            prepared,
        )
        .await
        .expect("findFiles projection should render");

        let paths = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("path column should be string data");
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column should be string data");
        let change_ids = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("change-id column should be string data");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(paths.value(0), "/Docs/README.md");
        assert_eq!(names.value(0), "README.md");
        assert_eq!(change_ids.value(0), selected_change_id.to_string());

        let requests = live_state_requests
            .lock()
            .expect("live-state request mutex should not be poisoned");
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].filter.schema_keys,
            vec![super::BLOB_REF_SCHEMA_KEY.to_string()]
        );
        assert_eq!(
            requests[0].filter.entity_pks,
            vec![crate::entity_pk::EntityPk::single("file-selected")]
        );
        assert_eq!(
            requests[0].filter.file_ids,
            vec![NullableKeyFilter::Value("file-selected".to_string())]
        );
    }

    #[tokio::test]
    async fn file_directory_id_scan_uses_indexed_descriptors_and_only_scans_blob_rows() {
        let data = b"docs contents".to_vec();
        let blob_hash = BlobHash::from_content(&data).to_hex();
        let other_data = b"other contents".to_vec();
        let other_blob_hash = BlobHash::from_content(&other_data).to_hex();
        let live_state_requests = Arc::new(Mutex::new(Vec::new()));
        let path_index_requests = Arc::new(AtomicUsize::new(0));
        let index = Arc::new(
            FilesystemPathIndex::from_live_rows(vec![
                live_directory_row(
                    "dir-docs",
                    "branch-b",
                    r#"{"id":"dir-docs","parent_id":null,"name":"docs"}"#,
                ),
                live_file_row(
                    "file-docs",
                    "branch-b",
                    r#"{"id":"file-docs","directory_id":"dir-docs","name":"readme.md"}"#,
                ),
                live_file_row(
                    "file-other-doc",
                    "branch-b",
                    r#"{"id":"file-other-doc","directory_id":"dir-docs","name":"other.md"}"#,
                ),
                live_file_row(
                    "file-root",
                    "branch-b",
                    r#"{"id":"file-root","directory_id":null,"name":"root.md"}"#,
                ),
            ])
            .expect("filesystem path index should build"),
        );
        let spec = LixFileSpec::active_branch(
            "branch-b",
            Arc::new(RecordingLiveStateReader {
                rows: vec![
                    live_blob_ref_row("file-docs", "branch-b", "file-docs", &blob_hash, data.len()),
                    live_blob_ref_row(
                        "file-other-doc",
                        "branch-b",
                        "file-other-doc",
                        &other_blob_hash,
                        other_data.len(),
                    ),
                ],
                scan_requests: Arc::clone(&live_state_requests),
            }),
            Arc::new(StaticFilesystemPathIndexReader {
                index,
                request_count: Arc::clone(&path_index_requests),
            }),
            Arc::new(TestBranchRefReader),
            Arc::new(StaticBlobReader::from_blobs(Vec::new())),
            PluginRuntimeHost::new(Arc::new(UnsupportedWasmRuntime)),
            test_functions(),
        );
        let projection = vec![
            spec.schema().index_of("path").expect("path column"),
            spec.schema()
                .index_of("lixcol_change_id")
                .expect("change-id column"),
        ];
        let filters = vec![eq_filter("directory_id", "dir-docs")];

        assert_eq!(
            spec.filter_pushdown(&filters[0]),
            TableProviderFilterPushDown::Exact
        );
        let planned = spec
            .plan_scan(Some(&projection), &filters, None, &ExecutionProps::new())
            .await
            .expect("directory-id scan should plan");
        let batch = (planned.load)()
            .await
            .expect("directory-id scan should load");

        let paths = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("path column should be string data");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(
            paths
                .iter()
                .map(|path| path.expect("path should not be null").to_string())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["/docs/other.md".to_string(), "/docs/readme.md".to_string()])
        );
        assert_eq!(path_index_requests.load(Ordering::SeqCst), 1);
        let requests = live_state_requests
            .lock()
            .expect("live-state request mutex should not be poisoned");
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].filter.schema_keys,
            vec![super::BLOB_REF_SCHEMA_KEY.to_string()]
        );
        assert_eq!(
            requests[0].filter.entity_pks,
            vec![
                crate::entity_pk::EntityPk::single("file-docs"),
                crate::entity_pk::EntityPk::single("file-other-doc"),
            ]
        );
        assert_eq!(
            requests[0].filter.file_ids,
            vec![
                NullableKeyFilter::Value("file-docs".to_string()),
                NullableKeyFilter::Value("file-other-doc".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn exact_blob_batch_requires_resolved_branch_ids_without_scanning() {
        let scan_count = Arc::new(AtomicUsize::new(0));
        let live_state: Arc<dyn LiveStateReader> = Arc::new(RejectingLiveStateReader {
            scan_count: Arc::clone(&scan_count),
        });
        let error = super::scan_exact_file_blob_rows(
            live_state,
            &LiveStateScanRequest::default(),
            &BTreeSet::from(["file-a".to_string()]),
        )
        .await
        .expect_err("branchless exact reads should be rejected");

        assert_eq!(error.code, LixError::CODE_INTERNAL_ERROR);
        assert!(error.message.contains("require resolved branch IDs"));
        assert_eq!(scan_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn indexed_blob_exact_batch_preserves_lanes_and_rejects_cross_pairs() {
        let tracked_data = b"tracked".to_vec();
        let global_data = b"global".to_vec();
        let untracked_data = b"untracked".to_vec();
        let misplaced_data = b"misplaced".to_vec();

        let mut global_file = live_file_row(
            "file-global",
            // Path-index rows are already projected into the requested branch.
            "branch-b",
            r#"{"id":"file-global","directory_id":null,"name":"global.md"}"#,
        );
        global_file.global = true;
        let mut untracked_file = live_file_row(
            "file-untracked",
            "branch-b",
            r#"{"id":"file-untracked","directory_id":null,"name":"untracked.md"}"#,
        );
        untracked_file.untracked = true;
        let mut index_rows = vec![
            live_file_row(
                "file-tracked",
                "branch-b",
                r#"{"id":"file-tracked","directory_id":null,"name":"tracked.md"}"#,
            ),
            global_file,
            untracked_file,
        ];
        index_rows.extend((0..30).map(|index| {
            live_file_row(
                &format!("file-padding-{index}"),
                "branch-b",
                &format!(
                    r#"{{"id":"file-padding-{index}","directory_id":null,"name":"padding-{index}.md"}}"#
                ),
            )
        }));
        let index = Arc::new(
            FilesystemPathIndex::from_live_rows(index_rows)
                .expect("filesystem path index should build"),
        );
        let matches =
            super::indexed_file_matches(Arc::clone(&index), &super::FilePathPredicate::All);

        let mut tracked_blob = live_blob_ref_row(
            "file-tracked",
            "branch-b",
            "file-tracked",
            &BlobHash::from_content(&tracked_data).to_hex(),
            tracked_data.len(),
        );
        tracked_blob.change_id = Some(ChangeId::for_test_label("tracked-blob"));
        let mut global_blob = live_blob_ref_row(
            "file-global",
            crate::GLOBAL_BRANCH_ID,
            "file-global",
            &BlobHash::from_content(&global_data).to_hex(),
            global_data.len(),
        );
        global_blob.global = true;
        global_blob.change_id = Some(ChangeId::for_test_label("global-blob"));
        let mut untracked_blob = live_blob_ref_row(
            "file-untracked",
            "branch-b",
            "file-untracked",
            &BlobHash::from_content(&untracked_data).to_hex(),
            untracked_data.len(),
        );
        untracked_blob.untracked = true;
        untracked_blob.change_id = Some(ChangeId::for_test_label("untracked-blob"));
        // A malformed `(entity=file-tracked, file=different-file-id)` row must
        // never be fetched for the exact descriptor identity.
        let mut misplaced_blob = live_blob_ref_row(
            "file-tracked",
            "branch-b",
            "different-file-id",
            &BlobHash::from_content(&misplaced_data).to_hex(),
            misplaced_data.len(),
        );
        misplaced_blob.change_id = Some(ChangeId::for_test_label("misplaced-blob"));
        let live_state_requests = Arc::new(Mutex::new(Vec::new()));
        let live_state: Arc<dyn LiveStateReader> = Arc::new(RecordingLiveStateReader {
            rows: vec![tracked_blob, global_blob, untracked_blob, misplaced_blob],
            scan_requests: Arc::clone(&live_state_requests),
        });
        let base_schema = super::lix_file_schema();
        let projection = vec![
            base_schema.index_of("path").expect("path column"),
            base_schema
                .index_of("lixcol_change_id")
                .expect("change id column"),
        ];
        let projected_schema = super::projected_schema(&base_schema, Some(&projection))
            .expect("projection should be valid");
        let request = super::lix_file_scan_request(Some("branch-b"), Some(&projected_schema), None);
        let rows = super::scan_indexed_file_rows(Arc::clone(&live_state), &request, &matches, true)
            .await
            .expect("matching blob rows should load");
        let prepared = super::prepare_indexed_lix_file_rows(&matches, rows)
            .expect("indexed rows should prepare");
        let blob_reader: Arc<dyn BlobDataReader> =
            Arc::new(StaticBlobReader::from_blobs(Vec::new()));
        let batch = super::lix_file_record_batch_from_prepared(
            &projected_schema,
            &blob_reader,
            None,
            false,
            prepared,
        )
        .await
        .expect("prefix-selected blobs should render");

        let paths = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("path column should be string data");
        let change_ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("change id column should be string data");
        let changes_by_path = paths
            .iter()
            .zip(change_ids.iter())
            .filter_map(|(path, change_id)| {
                let path = path?;
                if !matches!(path, "/global.md" | "/tracked.md" | "/untracked.md") {
                    return None;
                }
                change_id.map(|change_id| (path.to_string(), change_id.to_string()))
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            changes_by_path,
            BTreeMap::from([
                (
                    "/global.md".to_string(),
                    ChangeId::for_test_label("global-blob").to_string(),
                ),
                (
                    "/tracked.md".to_string(),
                    ChangeId::for_test_label("tracked-blob").to_string(),
                ),
                (
                    "/untracked.md".to_string(),
                    ChangeId::for_test_label("untracked-blob").to_string(),
                ),
            ])
        );
        assert_ne!(
            changes_by_path.get("/tracked.md"),
            Some(&ChangeId::for_test_label("misplaced-blob").to_string()),
            "the exact live-state tuple must reject a mismatched file-id"
        );
        let requests = live_state_requests
            .lock()
            .expect("live-state request mutex should not be poisoned");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].filter.entity_pks.len(), 33);
        assert!(
            requests[0]
                .filter
                .entity_pks
                .contains(&crate::entity_pk::EntityPk::single("file-global"))
        );
        assert!(
            requests[0]
                .filter
                .entity_pks
                .contains(&crate::entity_pk::EntityPk::single("file-tracked"))
        );
        assert!(
            requests[0]
                .filter
                .entity_pks
                .contains(&crate::entity_pk::EntityPk::single("file-untracked"))
        );
        assert_eq!(requests[0].filter.file_ids.len(), 33);
        assert!(
            requests[0]
                .filter
                .file_ids
                .contains(&NullableKeyFilter::Value("file-tracked".to_string()))
        );
    }

    #[tokio::test]
    async fn file_root_directory_scan_uses_indexed_descriptors_and_only_scans_root_blob_rows() {
        let root_data = b"root contents".to_vec();
        let root_blob_hash = BlobHash::from_content(&root_data).to_hex();
        let nested_data = b"nested contents".to_vec();
        let nested_blob_hash = BlobHash::from_content(&nested_data).to_hex();
        let live_state_requests = Arc::new(Mutex::new(Vec::new()));
        let path_index_requests = Arc::new(AtomicUsize::new(0));
        let index = Arc::new(
            FilesystemPathIndex::from_live_rows(vec![
                live_directory_row(
                    "dir-docs",
                    "branch-b",
                    r#"{"id":"dir-docs","parent_id":null,"name":"docs"}"#,
                ),
                live_file_row(
                    "file-nested",
                    "branch-b",
                    r#"{"id":"file-nested","directory_id":"dir-docs","name":"readme.md"}"#,
                ),
                live_file_row(
                    "file-root",
                    "branch-b",
                    r#"{"id":"file-root","directory_id":null,"name":"root.md"}"#,
                ),
            ])
            .expect("filesystem path index should build"),
        );
        let spec = LixFileSpec::active_branch(
            "branch-b",
            Arc::new(RecordingLiveStateReader {
                rows: vec![
                    live_blob_ref_row(
                        "file-root",
                        "branch-b",
                        "file-root",
                        &root_blob_hash,
                        root_data.len(),
                    ),
                    live_blob_ref_row(
                        "file-nested",
                        "branch-b",
                        "file-nested",
                        &nested_blob_hash,
                        nested_data.len(),
                    ),
                ],
                scan_requests: Arc::clone(&live_state_requests),
            }),
            Arc::new(StaticFilesystemPathIndexReader {
                index,
                request_count: Arc::clone(&path_index_requests),
            }),
            Arc::new(TestBranchRefReader),
            Arc::new(StaticBlobReader::from_blobs(Vec::new())),
            PluginRuntimeHost::new(Arc::new(UnsupportedWasmRuntime)),
            test_functions(),
        );
        let projection = vec![
            spec.schema().index_of("path").expect("path column"),
            spec.schema().index_of("name").expect("name column"),
            spec.schema()
                .index_of("lixcol_metadata")
                .expect("metadata column"),
            spec.schema()
                .index_of("lixcol_change_id")
                .expect("change-id column"),
            spec.schema()
                .index_of("lixcol_updated_at")
                .expect("updated-at column"),
        ];
        let filters = vec![Expr::IsNull(Box::new(column("directory_id")))];

        assert_eq!(
            spec.filter_pushdown(&filters[0]),
            TableProviderFilterPushDown::Exact
        );
        let planned = spec
            .plan_scan(Some(&projection), &filters, None, &ExecutionProps::new())
            .await
            .expect("root-directory scan should plan");
        let batch = (planned.load)()
            .await
            .expect("root-directory scan should load");

        let paths = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("path column should be string data");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(paths.value(0), "/root.md");
        assert_eq!(path_index_requests.load(Ordering::SeqCst), 1);
        let requests = live_state_requests
            .lock()
            .expect("live-state request mutex should not be poisoned");
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].filter.schema_keys,
            vec![super::BLOB_REF_SCHEMA_KEY.to_string()]
        );
        assert_eq!(
            requests[0].filter.entity_pks,
            vec![crate::entity_pk::EntityPk::single("file-root")]
        );
        assert_eq!(
            requests[0].filter.file_ids,
            vec![NullableKeyFilter::Value("file-root".to_string())]
        );
    }

    fn scalar_function_expr(name: &str, args: Vec<Expr>) -> Expr {
        let udf = create_udf(
            name,
            vec![DataType::LargeBinary],
            DataType::Int64,
            Volatility::Immutable,
            Arc::new(|_: &[ColumnarValue]| Ok(ColumnarValue::Scalar(ScalarValue::Null))),
        );
        Expr::ScalarFunction(ScalarFunction::new_udf(Arc::new(udf), args))
    }

    fn lower_path_contains_filter(pattern: &str) -> Expr {
        lower_path_contains_filter_with_options(pattern, false, None, false)
    }

    fn lower_path_contains_filter_with_options(
        pattern: &str,
        negated: bool,
        escape_char: Option<char>,
        case_insensitive: bool,
    ) -> Expr {
        Expr::Like(Like::new(
            negated,
            Box::new(scalar_function_expr("lower", vec![column("path")])),
            Box::new(string_literal(pattern)),
            escape_char,
            case_insensitive,
        ))
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
        if update_columns.updates_path() {
            columns.push("path");
        }
        if update_columns.data {
            columns.push("data");
        }
        if update_columns.writes_descriptor() {
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
        blob_bytes_by_hash: BTreeMap<BlobHash, Vec<u8>>,
        writes: Vec<TransactionWrite>,
        scan_count: usize,
        path_index_count: usize,
        exact_load_requests: Vec<LiveStateExactBatchRequest>,
    }

    struct IndexedFileDataUpdateWriteContext {
        index: Arc<FilesystemPathIndex>,
        blob_rows: Vec<MaterializedLiveStateRow>,
        writes: Vec<TransactionWrite>,
        scan_requests: Arc<Mutex<Vec<LiveStateScanRequest>>>,
        path_index_requests: Arc<AtomicUsize>,
    }

    struct StaticBlobReader {
        bytes_by_hash: BTreeMap<BlobHash, Vec<u8>>,
    }

    struct ExactBlobReader {
        expected_hashes: Vec<BlobHash>,
        bytes_by_hash: BTreeMap<BlobHash, Vec<u8>>,
    }

    struct RecordingBlobReader {
        bytes_by_hash: BTreeMap<BlobHash, Vec<u8>>,
        requests: Arc<Mutex<Vec<Vec<BlobHash>>>>,
    }

    struct RecordingRenderRuntime {
        rendered_states: Arc<Mutex<Vec<Vec<WasmPluginEntityState>>>>,
        rendered_bytes: Vec<u8>,
    }

    struct RecordingRenderComponent {
        rendered_states: Arc<Mutex<Vec<Vec<WasmPluginEntityState>>>>,
        rendered_bytes: Vec<u8>,
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
            Ok(BlobBytesBatch::new(
                hashes
                    .iter()
                    .map(|hash| self.blob_bytes_by_hash.get(hash).cloned())
                    .collect(),
            ))
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
    impl BlobDataReader for ExactBlobReader {
        async fn load_bytes_many(&self, hashes: &[BlobHash]) -> Result<BlobBytesBatch, LixError> {
            assert_eq!(hashes, self.expected_hashes.as_slice());
            Ok(BlobBytesBatch::new(
                hashes
                    .iter()
                    .map(|hash| self.bytes_by_hash.get(hash).cloned())
                    .collect(),
            ))
        }
    }

    #[async_trait]
    impl BlobDataReader for RecordingBlobReader {
        async fn load_bytes_many(&self, hashes: &[BlobHash]) -> Result<BlobBytesBatch, LixError> {
            self.requests
                .lock()
                .expect("blob request mutex should not be poisoned")
                .push(hashes.to_vec());
            Ok(BlobBytesBatch::new(
                hashes
                    .iter()
                    .map(|hash| self.bytes_by_hash.get(hash).cloned())
                    .collect(),
            ))
        }
    }

    #[async_trait]
    impl WasmRuntime for RecordingRenderRuntime {
        async fn init_component(
            &self,
            _bytes: Vec<u8>,
            _limits: WasmLimits,
        ) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
            Ok(Arc::new(RecordingRenderComponent {
                rendered_states: Arc::clone(&self.rendered_states),
                rendered_bytes: self.rendered_bytes.clone(),
            }))
        }
    }

    #[async_trait]
    impl WasmComponentInstance for RecordingRenderComponent {
        async fn detect_changes(
            &self,
            _state: Vec<WasmPluginEntityState>,
            _file: WasmPluginFile,
        ) -> Result<Vec<WasmPluginDetectedChange>, LixError> {
            Ok(Vec::new())
        }

        async fn render(&self, state: Vec<WasmPluginEntityState>) -> Result<Vec<u8>, LixError> {
            self.rendered_states
                .lock()
                .expect("rendered state mutex should not be poisoned")
                .push(state);
            Ok(self.rendered_bytes.clone())
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
            self.scan_count += 1;
            Ok(self.rows.clone())
        }

        async fn load_exact_live_state_rows(
            &mut self,
            request: &LiveStateExactBatchRequest,
        ) -> Result<Vec<Option<MaterializedLiveStateRow>>, LixError> {
            self.exact_load_requests.push(request.clone());
            Ok(request
                .rows
                .iter()
                .map(|requested| {
                    let matches = |row: &&MaterializedLiveStateRow| {
                        row.schema_key == requested.schema_key
                            && row.entity_pk == requested.entity_pk
                            && row.file_id == requested.file_id
                            && request
                                .untracked
                                .is_none_or(|untracked| row.untracked == untracked)
                    };
                    let mut row = self
                        .rows
                        .iter()
                        .filter(matches)
                        .find(|row| row.branch_id == requested.branch_id)
                        .or_else(|| {
                            self.rows
                                .iter()
                                .filter(matches)
                                .find(|row| row.branch_id == crate::GLOBAL_BRANCH_ID)
                        })?
                        .clone();
                    if row.branch_id == crate::GLOBAL_BRANCH_ID
                        && requested.branch_id != crate::GLOBAL_BRANCH_ID
                    {
                        row.branch_id.clone_from(&requested.branch_id);
                        row.global = true;
                    }
                    if row.deleted && !request.include_tombstones {
                        None
                    } else {
                        Some(row)
                    }
                })
                .collect())
        }

        async fn filesystem_path_index(
            &mut self,
            _request: &FilesystemPathIndexRequest,
        ) -> Result<Arc<FilesystemPathIndex>, LixError> {
            self.path_index_count += 1;
            Ok(Arc::new(FilesystemPathIndex::from_live_rows(
                self.rows.clone(),
            )?))
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

    #[async_trait]
    impl SqlWriteExecutionContext for IndexedFileDataUpdateWriteContext {
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
            Ok(BlobBytesBatch::new(vec![None; hashes.len()]))
        }

        async fn scan_live_state(
            &mut self,
            request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            self.scan_requests
                .lock()
                .expect("scan request mutex should not be poisoned")
                .push(request.clone());
            Ok(self.blob_rows.clone())
        }

        async fn filesystem_path_index(
            &mut self,
            request: &FilesystemPathIndexRequest,
        ) -> Result<Arc<FilesystemPathIndex>, LixError> {
            assert_eq!(request.branch_ids, vec!["branch-b".to_string()]);
            self.path_index_requests.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::clone(&self.index))
        }

        async fn load_branch_head(
            &mut self,
            branch_id: &str,
        ) -> Result<Option<CommitId>, LixError> {
            Ok(Some(CommitId::for_test_label(&format!(
                "commit-{branch_id}"
            ))))
        }

        async fn stage_write(
            &mut self,
            write: TransactionWrite,
        ) -> Result<TransactionWriteOutcome, LixError> {
            let count = match &write {
                TransactionWrite::Rows { rows, .. } => rows.len() as u64,
                TransactionWrite::RowsWithFileData { count, .. } => *count,
            };
            self.writes.push(write);
            Ok(TransactionWriteOutcome { count })
        }
    }

    #[derive(Default)]
    struct RowsLiveStateReader {
        rows: Vec<MaterializedLiveStateRow>,
    }

    struct RejectingLiveStateReader {
        scan_count: Arc<AtomicUsize>,
    }

    struct RecordingLiveStateReader {
        rows: Vec<MaterializedLiveStateRow>,
        scan_requests: Arc<Mutex<Vec<LiveStateScanRequest>>>,
    }

    #[async_trait]
    impl LiveStateReader for RecordingLiveStateReader {
        async fn scan_rows(
            &self,
            request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            self.scan_requests
                .lock()
                .expect("live-state request mutex should not be poisoned")
                .push(request.clone());
            Ok(self.rows.clone())
        }

        async fn load_row(
            &self,
            _request: &LiveStateRowRequest,
        ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
            Ok(None)
        }

        async fn load_exact_rows(
            &self,
            request: &LiveStateExactBatchRequest,
        ) -> Result<Vec<Option<MaterializedLiveStateRow>>, LixError> {
            let mut recorded = LiveStateScanRequest {
                filter: LiveStateFilter {
                    branch_ids: request
                        .rows
                        .iter()
                        .map(|row| row.branch_id.clone())
                        .collect(),
                    schema_keys: request
                        .rows
                        .iter()
                        .map(|row| row.schema_key.clone())
                        .collect(),
                    entity_pks: request
                        .rows
                        .iter()
                        .map(|row| row.entity_pk.clone())
                        .collect(),
                    file_ids: request
                        .rows
                        .iter()
                        .map(|row| match &row.file_id {
                            Some(file_id) => NullableKeyFilter::Value(file_id.clone()),
                            None => NullableKeyFilter::Null,
                        })
                        .collect(),
                    untracked: request.untracked,
                    include_tombstones: request.include_tombstones,
                    ..LiveStateFilter::default()
                },
                projection: request.projection.clone(),
                limit: None,
            };
            recorded.filter.branch_ids.sort();
            recorded.filter.branch_ids.dedup();
            recorded.filter.schema_keys.sort();
            recorded.filter.schema_keys.dedup();
            recorded.filter.entity_pks.sort();
            recorded.filter.entity_pks.dedup();
            recorded
                .filter
                .file_ids
                .sort_by_key(|file_id| format!("{file_id:?}"));
            recorded.filter.file_ids.dedup();
            self.scan_requests
                .lock()
                .expect("live-state request mutex should not be poisoned")
                .push(recorded);

            Ok(request
                .rows
                .iter()
                .map(|requested| {
                    let exact_match = |row: &&MaterializedLiveStateRow| {
                        row.schema_key == requested.schema_key
                            && row.entity_pk == requested.entity_pk
                            && row.file_id == requested.file_id
                            && request
                                .untracked
                                .is_none_or(|untracked| row.untracked == untracked)
                    };
                    let mut row = self
                        .rows
                        .iter()
                        .filter(exact_match)
                        .find(|row| row.branch_id == requested.branch_id)
                        .or_else(|| {
                            self.rows
                                .iter()
                                .filter(exact_match)
                                .find(|row| row.branch_id == crate::GLOBAL_BRANCH_ID)
                        })?
                        .clone();
                    if row.branch_id == crate::GLOBAL_BRANCH_ID
                        && requested.branch_id != crate::GLOBAL_BRANCH_ID
                    {
                        row.branch_id.clone_from(&requested.branch_id);
                        row.global = true;
                    }
                    if row.deleted && !request.include_tombstones {
                        None
                    } else {
                        Some(row)
                    }
                })
                .collect())
        }
    }

    #[async_trait]
    impl LiveStateReader for RejectingLiveStateReader {
        async fn load_exact_rows(
            &self,
            request: &LiveStateExactBatchRequest,
        ) -> Result<Vec<Option<MaterializedLiveStateRow>>, LixError> {
            crate::live_state::load_exact_rows_via_scan_for_test(self, request).await
        }

        async fn scan_rows(
            &self,
            _request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            self.scan_count.fetch_add(1, Ordering::SeqCst);
            Err(LixError::unknown(
                "descriptor-only scan should not read live state",
            ))
        }

        async fn load_row(
            &self,
            _request: &LiveStateRowRequest,
        ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
            Err(LixError::unknown(
                "descriptor-only scan should not load live-state rows",
            ))
        }
    }

    struct StaticFilesystemPathIndexReader {
        index: Arc<FilesystemPathIndex>,
        request_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl FilesystemPathIndexReader for StaticFilesystemPathIndexReader {
        async fn path_index(
            &self,
            _request: &FilesystemPathIndexRequest,
        ) -> Result<Arc<FilesystemPathIndex>, LixError> {
            self.request_count.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::clone(&self.index))
        }
    }

    struct TestBranchRefReader;

    #[async_trait]
    impl BranchRefReader for TestBranchRefReader {
        async fn load_head(&self, branch_id: &str) -> Result<Option<BranchHead>, LixError> {
            Ok(Some(BranchHead {
                branch_id: branch_id.to_string(),
                commit_id: CommitId::for_test_label(&format!("commit-{branch_id}")),
            }))
        }

        async fn scan_heads(&self) -> Result<Vec<BranchHead>, LixError> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl LiveStateReader for RowsLiveStateReader {
        async fn load_exact_rows(
            &self,
            request: &LiveStateExactBatchRequest,
        ) -> Result<Vec<Option<MaterializedLiveStateRow>>, LixError> {
            crate::live_state::load_exact_rows_via_scan_for_test(self, request).await
        }

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

    fn file_dml_rows() -> Vec<MaterializedLiveStateRow> {
        vec![
            live_file_row(
                "file-readme",
                "branch-b",
                r#"{"id":"file-readme","directory_id":null,"name":"readme.md"}"#,
            ),
            live_blob_ref_row("file-readme", "branch-b", "file-readme", &"0".repeat(64), 5),
        ]
    }

    fn file_dml_spec(write_ctx: SqlWriteContext) -> LixFileSpec {
        let branch_ref = Arc::new(WriteContextBranchRefReader::new(write_ctx.clone()));
        LixFileSpec::active_branch_with_write(
            write_ctx,
            branch_ref,
            super::SqlWriteSessionOptions::default(),
        )
    }

    fn literal_assignment(
        column_name: &str,
        value: ScalarValue,
    ) -> (String, Arc<dyn PhysicalExpr>) {
        (
            column_name.to_string(),
            Arc::new(Literal::new(value)) as Arc<dyn PhysicalExpr>,
        )
    }

    fn test_plugin_registry_entry(
        key: &str,
        path_glob: &str,
        schema_key: &str,
        wasm: &[u8],
    ) -> PluginRegistryEntry {
        test_plugin_registry_entry_with_content_type(key, path_glob, None, schema_key, wasm)
    }

    fn test_plugin_registry_entry_with_content_type(
        key: &str,
        path_glob: &str,
        content_type: Option<PluginContentType>,
        schema_key: &str,
        wasm: &[u8],
    ) -> PluginRegistryEntry {
        let mut manifest = serde_json::json!({
            "api_version": "0.1.0",
            "entry": "plugin.wasm",
            "key": key,
            "match": { "path_glob": path_glob },
            "runtime": "wasm-component-v1",
            "schemas": ["schema/plugin.json"],
        });
        if let Some(content_type) = content_type {
            manifest["match"]["content_type"] =
                serde_json::to_value(content_type).expect("plugin content type should serialize");
        }
        let manifest_json = manifest.to_string();
        PluginRegistryEntry::new(PluginRegistryEntryInput {
            key: key.to_string(),
            runtime: PluginRuntime::WasmComponentV1,
            api_version: "0.1.0".to_string(),
            path_glob: path_glob.to_string(),
            content_type,
            entry: "plugin.wasm".to_string(),
            schema_keys: vec![schema_key.to_string()],
            host_allocated_schema_keys: Vec::new(),
            manifest_json,
            archive_file_id: plugin_storage_archive_file_id(key),
            archive_path: plugin_storage_archive_path(key),
            archive_blob_hash: BlobHash::from_content(format!("archive-{key}").as_bytes()).to_hex(),
            wasm_blob_hash: BlobHash::from_content(wasm).to_hex(),
        })
        .expect("test plugin registry entry should be valid")
    }

    fn test_v2_plugin_registry_entry(
        key: &str,
        path_glob: &str,
        schema_key: &str,
        wasm: &[u8],
    ) -> PluginRegistryEntry {
        let manifest_json = serde_json::json!({
            "api_version": "2.0.0",
            "entry": "plugin.wasm",
            "key": key,
            "match": { "path_glob": path_glob },
            "runtime": "wasm-component-v2",
            "schemas": ["schema/plugin.json"],
        })
        .to_string();
        PluginRegistryEntry::new(PluginRegistryEntryInput {
            key: key.to_string(),
            runtime: PluginRuntime::WasmComponentV2,
            api_version: "2.0.0".to_string(),
            path_glob: path_glob.to_string(),
            content_type: None,
            entry: "plugin.wasm".to_string(),
            schema_keys: vec![schema_key.to_string()],
            host_allocated_schema_keys: Vec::new(),
            manifest_json,
            archive_file_id: plugin_storage_archive_file_id(key),
            archive_path: plugin_storage_archive_path(key),
            archive_blob_hash: BlobHash::from_content(format!("archive-{key}").as_bytes()).to_hex(),
            wasm_blob_hash: BlobHash::from_content(wasm).to_hex(),
        })
        .expect("test v2 plugin registry entry should be valid")
    }

    fn live_plugin_registry_row(
        branch_id: &str,
        entries: Vec<PluginRegistryEntry>,
    ) -> MaterializedLiveStateRow {
        let registry = PluginRegistry::new(entries).expect("test plugin registry should be valid");
        let mut row = live_file_row(
            PLUGIN_REGISTRY_KEY,
            branch_id,
            &registry
                .to_snapshot()
                .expect("registry snapshot should serialize")
                .to_string(),
        );
        row.schema_key = "lix_key_value".to_string();
        row
    }

    fn live_plugin_owner_row(
        branch_id: &str,
        file_id: &str,
        plugin_key: &str,
        schema_keys: Vec<String>,
    ) -> MaterializedLiveStateRow {
        let owner = PluginFileOwner::new(file_id, plugin_key, schema_keys)
            .expect("test plugin owner should be valid");
        let mut row = live_file_row(
            PLUGIN_OWNER_KEY,
            branch_id,
            &owner
                .to_snapshot()
                .expect("owner snapshot should serialize")
                .to_string(),
        );
        row.schema_key = "lix_key_value".to_string();
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
        path_update_batch_with_path_and_data(path, b"hello")
    }

    fn path_update_batch_with_path_and_data(path: &str, data: &[u8]) -> RecordBatch {
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
                Arc::new(BinaryArray::from_vec(vec![data])) as ArrayRef,
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

    fn metadata_data_update_batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("path", DataType::Utf8, false),
                Field::new("directory_id", DataType::Utf8, true),
                Field::new("name", DataType::Utf8, false),
                Field::new("data", DataType::Binary, true),
                Field::new("lixcol_metadata", DataType::Utf8, true),
                Field::new("lixcol_branch_id", DataType::Utf8, false),
            ])),
            vec![
                string_column(vec![Some("file-readme")]),
                string_column(vec![Some("/docs/readme.md")]),
                string_column(vec![Some("dir-docs")]),
                string_column(vec![Some("readme.md")]),
                Arc::new(BinaryArray::from_vec(vec![b"updated"])) as ArrayRef,
                string_column(vec![Some(r#"{"source":"upload"}"#)]),
                string_column(vec![Some("branch-b")]),
            ],
        )
        .expect("file metadata data update batch")
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
        file_delete_batch_with_id_and_path("file-readme", path)
    }

    fn file_delete_batch_with_id_and_path(file_id: &str, path: Option<&str>) -> RecordBatch {
        let mut fields = vec![Field::new("id", DataType::Utf8, false)];
        let mut columns = vec![string_column(vec![Some(file_id)])];
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
    async fn file_path_predicate_filters_before_blob_and_plugin_hydration() {
        let selected_data = b"selected".to_vec();
        let other_data = b"other".to_vec();
        let selected_hash = BlobHash::from_content(&selected_data);
        let other_hash = BlobHash::from_content(&other_data);
        let rows = vec![
            live_file_row(
                "file-selected",
                "branch-b",
                r#"{"id":"file-selected","directory_id":null,"name":"selected.md"}"#,
            ),
            live_blob_ref_row(
                "file-selected",
                "branch-b",
                "file-selected",
                &selected_hash.to_hex(),
                selected_data.len(),
            ),
            live_file_row(
                "file-other",
                "branch-b",
                r#"{"id":"file-other","directory_id":null,"name":"other.md"}"#,
            ),
            live_blob_ref_row(
                "file-other",
                "branch-b",
                "file-other",
                &other_hash.to_hex(),
                other_data.len(),
            ),
        ];
        let predicate =
            super::file_path_predicate_from_filters(&[eq_filter("path", "/selected.md")]);
        let prepared = super::prepare_lix_file_rows(rows, &predicate)
            .expect("path-filtered rows should prepare");
        assert_eq!(prepared.file_rows.len(), 1);
        assert!(!prepared.needs_plugin_render(true));

        let blob_reader = Arc::new(ExactBlobReader {
            expected_hashes: vec![selected_hash],
            bytes_by_hash: BTreeMap::from([
                (selected_hash, selected_data.clone()),
                (other_hash, other_data),
            ]),
        }) as Arc<dyn BlobDataReader>;
        let batch = super::lix_file_record_batch_from_prepared(
            &super::lix_file_schema(),
            &blob_reader,
            None,
            true,
            prepared,
        )
        .await
        .expect("path-filtered batch should build");

        assert_eq!(batch.num_rows(), 1);
        let path_column = batch
            .column(batch.schema().index_of("path").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let data_column = batch
            .column(batch.schema().index_of("data").unwrap())
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert_eq!(path_column.value(0), "/selected.md");
        assert_eq!(data_column.value(0), selected_data.as_slice());
    }

    #[test]
    fn file_path_predicate_only_discovers_plugins_for_selected_blobless_files() {
        let blob_data = b"stored".to_vec();
        let blob_hash = BlobHash::from_content(&blob_data);
        let rows = vec![
            live_file_row(
                "file-stored",
                "branch-b",
                r#"{"id":"file-stored","directory_id":null,"name":"stored.md"}"#,
            ),
            live_blob_ref_row(
                "file-stored",
                "branch-b",
                "file-stored",
                &blob_hash.to_hex(),
                blob_data.len(),
            ),
            live_file_row(
                "file-rendered",
                "branch-b",
                r#"{"id":"file-rendered","directory_id":null,"name":"rendered.md"}"#,
            ),
        ];

        let stored = super::prepare_lix_file_rows(
            rows.clone(),
            &super::file_path_predicate_from_filters(&[eq_filter("path", "/stored.md")]),
        )
        .unwrap();
        assert!(!stored.needs_plugin_render(true));

        let rendered = super::prepare_lix_file_rows(
            rows,
            &super::file_path_predicate_from_filters(&[eq_filter("path", "/rendered.md")]),
        )
        .unwrap();
        assert!(rendered.needs_plugin_render(true));
        assert!(!rendered.needs_plugin_render(false));
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
    async fn file_projection_reuses_loaded_blob_for_duplicate_blob_ref_keys() {
        let data = b"shared data".to_vec();
        let blob_hash = BlobHash::from_content(&data).to_hex();
        let blob_reader =
            Arc::new(StaticBlobReader::from_blobs(vec![data.clone()])) as Arc<dyn BlobDataReader>;
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
                live_blob_ref_row(
                    "file-readme",
                    "branch-b",
                    "file-readme",
                    &blob_hash,
                    data.len(),
                ),
            ],
        )
        .await
        .expect("duplicate blob-ref keys should project data for every descriptor");

        let data_column = batch
            .column(batch.schema().index_of("data").unwrap())
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .expect("data should be large binary array");
        assert_eq!(batch.num_rows(), 2);
        for index in 0..batch.num_rows() {
            assert!(!data_column.is_null(index));
            assert_eq!(data_column.value(index), data.as_slice());
        }
    }

    #[tokio::test]
    async fn plugin_registry_catalogs_remain_branch_scoped() {
        let wasm = b"test wasm";
        let rows = vec![
            live_plugin_registry_row(
                "branch-a",
                vec![test_plugin_registry_entry(
                    "plugin_sentinel",
                    "*.branch-a",
                    "plugin_note_a",
                    wasm,
                )],
            ),
            live_plugin_owner_row(
                "branch-a",
                "file-a",
                "plugin_sentinel",
                vec!["plugin_note_a".to_string()],
            ),
            live_plugin_registry_row(
                "branch-b",
                vec![test_plugin_registry_entry(
                    "plugin_sentinel",
                    "*.branch-b",
                    "plugin_note_b",
                    wasm,
                )],
            ),
            live_plugin_owner_row(
                "branch-b",
                "file-b",
                "plugin_sentinel",
                vec!["plugin_note_b".to_string()],
            ),
        ];
        let prepared = super::prepare_lix_file_rows(
            vec![
                live_file_row(
                    "file-a",
                    "branch-a",
                    r#"{"id":"file-a","directory_id":null,"name":"note.branch-a"}"#,
                ),
                live_file_row(
                    "file-b",
                    "branch-b",
                    r#"{"id":"file-b","directory_id":null,"name":"note.branch-b"}"#,
                ),
            ],
            &super::FilePathPredicate::All,
        )
        .expect("plugin candidates should prepare");
        let context = super::plugin_render_context_for_lix_file_scan(
            Arc::new(RowsLiveStateReader { rows }) as Arc<dyn LiveStateReader>,
            &LiveStateScanRequest {
                filter: LiveStateFilter {
                    branch_ids: vec!["branch-a".to_string(), "branch-b".to_string()],
                    ..Default::default()
                },
                ..Default::default()
            },
            PluginRuntimeHost::new(Arc::new(UnsupportedWasmRuntime)),
            &prepared,
            false,
        )
        .await
        .expect("branch registries should load")
        .expect("non-empty registries should create a render context");

        assert_eq!(
            context
                .branch("branch-a")
                .and_then(|branch| branch.catalog.select_for_bytes("/note.branch-a", b""))
                .map(PluginRegistryEntry::key),
            Some("plugin_sentinel")
        );
        assert!(
            context
                .branch("branch-a")
                .and_then(|branch| branch.catalog.select_for_bytes("/note.branch-b", b""))
                .is_none()
        );
        assert_eq!(
            context
                .branch("branch-b")
                .and_then(|branch| branch.catalog.select_for_bytes("/note.branch-b", b""))
                .map(PluginRegistryEntry::key),
            Some("plugin_sentinel")
        );
    }

    #[tokio::test]
    async fn missing_plugin_registry_checks_blobless_file_ownership() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let prepared = super::prepare_lix_file_rows(
            vec![live_file_row(
                "file-note",
                "branch-b",
                r#"{"id":"file-note","directory_id":null,"name":"note.sentinel"}"#,
            )],
            &super::FilePathPredicate::All,
        )
        .expect("blobless file should prepare");
        let context = super::plugin_render_context_for_lix_file_scan(
            Arc::new(RecordingLiveStateReader {
                rows: Vec::new(),
                scan_requests: Arc::clone(&requests),
            }) as Arc<dyn LiveStateReader>,
            &LiveStateScanRequest {
                filter: LiveStateFilter {
                    branch_ids: vec!["branch-b".to_string()],
                    ..Default::default()
                },
                ..Default::default()
            },
            PluginRuntimeHost::new(Arc::new(UnsupportedWasmRuntime)),
            &prepared,
            false,
        )
        .await
        .expect("missing registry is the empty registry");

        assert!(context.is_none());
        let requests = requests.lock().expect("scan request mutex");
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].filter.schema_keys, vec!["lix_key_value"]);
        assert_eq!(
            requests[0].filter.entity_pks,
            vec![crate::entity_pk::EntityPk::single(PLUGIN_REGISTRY_KEY)]
        );
        assert_eq!(requests[0].filter.branch_ids, vec!["branch-b"]);
        assert_eq!(requests[0].filter.file_ids, vec![NullableKeyFilter::Null]);
        assert_eq!(requests[0].filter.untracked, Some(false));
        assert_eq!(requests[0].limit, Some(1));
        assert_eq!(
            requests[1].filter.entity_pks,
            vec![crate::entity_pk::EntityPk::single(PLUGIN_OWNER_KEY)]
        );
        assert_eq!(
            requests[1].filter.file_ids,
            vec![NullableKeyFilter::Value("file-note".to_string())]
        );
    }

    #[tokio::test]
    async fn installed_nonmatching_plugin_checks_blobless_file_ownership() {
        let wasm = b"test wasm";
        let requests = Arc::new(Mutex::new(Vec::new()));
        let prepared = super::prepare_lix_file_rows(
            vec![live_file_row(
                "file-note",
                "branch-b",
                r#"{"id":"file-note","directory_id":null,"name":"note.txt"}"#,
            )],
            &super::FilePathPredicate::All,
        )
        .expect("blobless raw file should prepare");
        let context = super::plugin_render_context_for_lix_file_scan(
            Arc::new(RecordingLiveStateReader {
                rows: vec![live_plugin_registry_row(
                    "branch-b",
                    vec![test_plugin_registry_entry(
                        "plugin_sentinel",
                        "*.sentinel",
                        "plugin_note",
                        wasm,
                    )],
                )],
                scan_requests: Arc::clone(&requests),
            }) as Arc<dyn LiveStateReader>,
            &LiveStateScanRequest {
                filter: LiveStateFilter {
                    branch_ids: vec!["branch-b".to_string()],
                    ..Default::default()
                },
                ..Default::default()
            },
            PluginRuntimeHost::new(Arc::new(UnsupportedWasmRuntime)),
            &prepared,
            false,
        )
        .await
        .expect("nonmatching registry lookup should succeed");

        assert!(context.is_none());
        let requests = requests.lock().expect("scan request mutex");
        assert_eq!(requests.len(), 2, "registry and exact owner rows are read");
        assert_eq!(
            requests[0].filter.entity_pks,
            vec![crate::entity_pk::EntityPk::single(PLUGIN_REGISTRY_KEY)]
        );
        assert_eq!(
            requests[1].filter.entity_pks,
            vec![crate::entity_pk::EntityPk::single(PLUGIN_OWNER_KEY)]
        );
    }

    #[tokio::test]
    async fn blobless_owned_file_requires_its_installed_plugin() {
        let prepared = super::prepare_lix_file_rows(
            vec![live_file_row(
                "file-note",
                "branch-b",
                r#"{"id":"file-note","directory_id":null,"name":"note.sentinel"}"#,
            )],
            &super::FilePathPredicate::All,
        )
        .expect("owned blobless file should prepare");
        let context = super::plugin_render_context_for_lix_file_scan(
            Arc::new(RowsLiveStateReader {
                rows: vec![live_plugin_owner_row(
                    "branch-b",
                    "file-note",
                    "plugin_sentinel",
                    vec!["plugin_note".to_string()],
                )],
            }) as Arc<dyn LiveStateReader>,
            &LiveStateScanRequest {
                filter: LiveStateFilter {
                    branch_ids: vec!["branch-b".to_string()],
                    ..Default::default()
                },
                ..Default::default()
            },
            PluginRuntimeHost::new(Arc::new(UnsupportedWasmRuntime)),
            &prepared,
            false,
        )
        .await
        .expect("durable owner lookup should succeed")
        .expect("durable owner should create a render context");
        let blob_reader = Arc::new(StaticBlobReader {
            bytes_by_hash: BTreeMap::new(),
        }) as Arc<dyn BlobDataReader>;

        let error = super::lix_file_record_batch_from_prepared(
            &super::lix_file_schema(),
            &blob_reader,
            Some(context),
            true,
            prepared,
        )
        .await
        .expect_err("missing plugin must not silently render empty bytes");

        assert_eq!(error.code, LixError::CODE_PLUGIN_UNAVAILABLE);
        assert!(error.message.contains("plugin_sentinel"));
        assert!(error.message.contains("/note.sentinel"));
    }

    #[tokio::test]
    async fn plugin_owner_renders_empty_state_with_one_wasm_batch() {
        let wasm = b"test wasm".to_vec();
        let entry =
            test_plugin_registry_entry("plugin_sentinel", "*.sentinel", "plugin_note", &wasm);
        let scan_requests = Arc::new(Mutex::new(Vec::new()));
        let live_state = Arc::new(RecordingLiveStateReader {
            rows: vec![
                live_plugin_registry_row("branch-b", vec![entry]),
                live_plugin_owner_row(
                    "branch-b",
                    "file-note",
                    "plugin_sentinel",
                    vec!["plugin_note".to_string()],
                ),
            ],
            scan_requests: Arc::clone(&scan_requests),
        }) as Arc<dyn LiveStateReader>;
        let prepared = super::prepare_lix_file_rows(
            vec![live_file_row(
                "file-note",
                "branch-b",
                r#"{"id":"file-note","directory_id":null,"name":"note.sentinel"}"#,
            )],
            &super::FilePathPredicate::All,
        )
        .expect("owned blobless file should prepare");
        let rendered_states = Arc::new(Mutex::new(Vec::new()));
        let context = super::plugin_render_context_for_lix_file_scan(
            Arc::clone(&live_state),
            &LiveStateScanRequest {
                filter: LiveStateFilter {
                    branch_ids: vec!["branch-b".to_string()],
                    ..Default::default()
                },
                ..Default::default()
            },
            PluginRuntimeHost::new(Arc::new(RecordingRenderRuntime {
                rendered_states: Arc::clone(&rendered_states),
                rendered_bytes: b"rendered empty state".to_vec(),
            })),
            &prepared,
            false,
        )
        .await
        .expect("plugin render context should load")
        .expect("installed plugin should create a render context");
        let blob_requests = Arc::new(Mutex::new(Vec::new()));
        let blob_reader = Arc::new(RecordingBlobReader {
            bytes_by_hash: BTreeMap::from([(BlobHash::from_content(&wasm), wasm)]),
            requests: Arc::clone(&blob_requests),
        }) as Arc<dyn BlobDataReader>;

        let batch = super::lix_file_record_batch_from_prepared(
            &super::lix_file_schema(),
            &blob_reader,
            Some(context.clone()),
            true,
            prepared,
        )
        .await
        .expect("owned file should render");

        let data = batch
            .column(batch.schema().index_of("data").unwrap())
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .expect("data should be a large binary array");
        assert_eq!(data.value(0), b"rendered empty state");
        assert_eq!(
            rendered_states
                .lock()
                .expect("rendered state mutex")
                .as_slice(),
            &[Vec::<WasmPluginEntityState>::new()]
        );
        assert_eq!(
            blob_requests.lock().expect("blob request mutex").as_slice(),
            &[vec![BlobHash::from_content(b"test wasm")]]
        );

        let warm_prepared = super::prepare_lix_file_rows(
            vec![live_file_row(
                "file-note",
                "branch-b",
                r#"{"id":"file-note","directory_id":null,"name":"note.sentinel"}"#,
            )],
            &super::FilePathPredicate::All,
        )
        .expect("warm owned file should prepare");
        super::lix_file_record_batch_from_prepared(
            &super::lix_file_schema(),
            &blob_reader,
            Some(context),
            true,
            warm_prepared,
        )
        .await
        .expect("warm owned file should render without loading component bytes");
        assert_eq!(
            blob_requests.lock().expect("blob request mutex").as_slice(),
            &[vec![BlobHash::from_content(b"test wasm")]],
            "warm render must hit the hash cache before CAS"
        );
        assert_eq!(
            rendered_states.lock().expect("rendered state mutex").len(),
            2
        );
        let requests = scan_requests.lock().expect("scan request mutex");
        assert_eq!(
            requests.len(),
            4,
            "registry, owners, then one grouped state scan per render"
        );
        assert_eq!(
            requests[1].filter.entity_pks,
            vec![crate::entity_pk::EntityPk::single(PLUGIN_OWNER_KEY)]
        );
        assert_eq!(
            requests[2].filter.file_ids,
            vec![NullableKeyFilter::Value("file-note".to_string())]
        );
    }

    #[tokio::test]
    async fn path_update_uses_stale_owner_and_current_catalog() {
        let wasm = b"test wasm";
        let prepared = super::prepare_lix_file_rows(
            vec![live_file_row(
                "file-readme",
                "branch-b",
                r#"{"id":"file-readme","directory_id":null,"name":"note.removed"}"#,
            )],
            &super::FilePathPredicate::All,
        )
        .expect("blobless file should prepare");
        let context = super::plugin_render_context_for_lix_file_scan(
            Arc::new(RowsLiveStateReader {
                rows: vec![
                    live_plugin_registry_row(
                        "branch-b",
                        vec![test_plugin_registry_entry(
                            "plugin_active",
                            "*.active",
                            "plugin_active_state",
                            wasm,
                        )],
                    ),
                    live_plugin_owner_row(
                        "branch-b",
                        "file-readme",
                        "plugin_removed",
                        vec!["plugin_removed_state".to_string()],
                    ),
                ],
            }) as Arc<dyn LiveStateReader>,
            &LiveStateScanRequest {
                filter: LiveStateFilter {
                    branch_ids: vec!["branch-b".to_string()],
                    ..Default::default()
                },
                ..Default::default()
            },
            PluginRuntimeHost::new(Arc::new(UnsupportedWasmRuntime)),
            &prepared,
            true,
        )
        .await
        .expect("plugin ownership should load")
        .expect("the active registry should create a context");
        let batch = path_update_batch_with_path("/note.removed");
        let assignments = vec![literal_assignment(
            "path",
            ScalarValue::Utf8(Some("/note.active".to_string())),
        )];
        let assignment_values = super::UpdateAssignmentValues::evaluate(&batch, &assignments)
            .expect("path assignment should evaluate");

        let rewritten = super::path_update_plugin_rewrite_file_ids(
            Some(&context),
            &batch,
            &assignment_values,
            Some("branch-b"),
        )
        .expect("path ownership comparison should succeed");

        assert_eq!(rewritten, BTreeSet::from(["file-readme".to_string()]));
        assert_eq!(
            context
                .owners_by_file
                .values()
                .next()
                .map(PluginFileOwner::plugin_key),
            Some("plugin_removed")
        );
        assert_eq!(
            context
                .branch("branch-b")
                .and_then(|branch| branch.catalog.select_for_bytes("/note.active", b""))
                .map(PluginRegistryEntry::key),
            Some("plugin_active")
        );
    }

    #[tokio::test]
    async fn path_update_restages_same_owner_v2_for_descriptor_transition() {
        let wasm = b"test v2 wasm";
        let prepared = super::prepare_lix_file_rows(
            vec![live_file_row(
                "file-readme",
                "branch-b",
                r#"{"id":"file-readme","directory_id":null,"name":"before.csv"}"#,
            )],
            &super::FilePathPredicate::All,
        )
        .expect("blobless file should prepare");
        let context = super::plugin_render_context_for_lix_file_scan(
            Arc::new(RowsLiveStateReader {
                rows: vec![
                    live_plugin_registry_row(
                        "branch-b",
                        vec![test_v2_plugin_registry_entry(
                            "plugin_csv_v2",
                            "*.csv",
                            "csv_row",
                            wasm,
                        )],
                    ),
                    live_plugin_owner_row(
                        "branch-b",
                        "file-readme",
                        "plugin_csv_v2",
                        vec!["csv_row".to_string()],
                    ),
                ],
            }) as Arc<dyn LiveStateReader>,
            &LiveStateScanRequest {
                filter: LiveStateFilter {
                    branch_ids: vec!["branch-b".to_string()],
                    ..Default::default()
                },
                ..Default::default()
            },
            PluginRuntimeHost::new(Arc::new(UnsupportedWasmRuntime)),
            &prepared,
            true,
        )
        .await
        .expect("plugin ownership should load")
        .expect("the active registry should create a context");
        let batch = path_update_batch_with_path("/before.csv");
        let assignments = vec![literal_assignment(
            "path",
            ScalarValue::Utf8(Some("/after.csv".to_string())),
        )];
        let assignment_values = super::UpdateAssignmentValues::evaluate(&batch, &assignments)
            .expect("path assignment should evaluate");

        let rewritten = super::path_update_plugin_rewrite_file_ids(
            Some(&context),
            &batch,
            &assignment_values,
            Some("branch-b"),
        )
        .expect("v2 descriptor transition should be selected");

        assert_eq!(rewritten, BTreeSet::from(["file-readme".to_string()]));
    }

    #[tokio::test]
    async fn path_update_uses_materialized_data_for_content_type_matching() {
        let wasm = b"test wasm";
        let prepared = super::prepare_lix_file_rows(
            vec![live_file_row(
                "file-readme",
                "branch-b",
                r#"{"id":"file-readme","directory_id":null,"name":"note.raw"}"#,
            )],
            &super::FilePathPredicate::All,
        )
        .expect("blobless file should prepare");
        let context = super::plugin_render_context_for_lix_file_scan(
            Arc::new(RowsLiveStateReader {
                rows: vec![live_plugin_registry_row(
                    "branch-b",
                    vec![test_plugin_registry_entry_with_content_type(
                        "plugin_text",
                        "*.active",
                        Some(PluginContentType::Text),
                        "plugin_text_state",
                        wasm,
                    )],
                )],
            }) as Arc<dyn LiveStateReader>,
            &LiveStateScanRequest {
                filter: LiveStateFilter {
                    branch_ids: vec!["branch-b".to_string()],
                    ..Default::default()
                },
                ..Default::default()
            },
            PluginRuntimeHost::new(Arc::new(UnsupportedWasmRuntime)),
            &prepared,
            true,
        )
        .await
        .expect("plugin registry should load")
        .expect("the active registry should create a context");
        let assignments = vec![literal_assignment(
            "path",
            ScalarValue::Utf8(Some("/note.active".to_string())),
        )];

        for (data, expected) in [
            (
                b"hello".as_slice(),
                BTreeSet::from(["file-readme".to_string()]),
            ),
            ([0xff, 0xfe].as_slice(), BTreeSet::new()),
        ] {
            let batch = path_update_batch_with_path_and_data("/note.raw", data);
            let assignment_values = super::UpdateAssignmentValues::evaluate(&batch, &assignments)
                .expect("path assignment should evaluate");
            let rewritten = super::path_update_plugin_rewrite_file_ids(
                Some(&context),
                &batch,
                &assignment_values,
                Some("branch-b"),
            )
            .expect("typed path ownership comparison should succeed");
            assert_eq!(rewritten, expected);
        }
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
                data: false,
                descriptor: super::LixFileDescriptorUpdate::Path,
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
                data: false,
                descriptor: super::LixFileDescriptorUpdate::Path,
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
                data: true,
                descriptor: super::LixFileDescriptorUpdate::None,
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
    fn file_delete_rejects_plugin_storage_path_without_exact_target() {
        let error = lix_file_delete_stage_from_batch(
            &file_delete_batch_with_id_and_path(
                "lix_plugin_archive::plugin_sentinel",
                Some("/.lix/plugins/plugin_sentinel.lixplugin"),
            ),
            None,
            &BTreeSet::new(),
            None,
        )
        .expect_err("non-exact file delete should reject installed archive path");

        assert!(
            error
                .to_string()
                .contains("one exact canonical plugin archive"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn file_delete_allows_exact_canonical_plugin_archive_target() {
        let staged = lix_file_delete_stage_from_batch(
            &file_delete_batch_with_id_and_path(
                "lix_plugin_archive::plugin_sentinel",
                Some("/.lix/plugins/plugin_sentinel.lixplugin"),
            ),
            None,
            &BTreeSet::new(),
            Some("plugin_sentinel"),
        )
        .expect("exact canonical plugin archive delete should stage");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.state_rows.len(), 1);
        assert_eq!(staged.state_rows[0].schema_key, "lix_file_descriptor");
        assert_eq!(staged.state_rows[0].snapshot, None);
    }

    #[test]
    fn file_delete_rejects_noncanonical_plugin_archive_identity() {
        for (file_id, path) in [
            ("file-arbitrary", "/.lix/plugins/plugin_sentinel.lixplugin"),
            (
                "lix_plugin_archive::plugin_other",
                "/.lix/plugins/plugin_sentinel.lixplugin",
            ),
            (
                "lix_plugin_archive::plugin_sentinel",
                "/.lix/plugins/nested/plugin_sentinel.lixplugin",
            ),
        ] {
            let error = lix_file_delete_stage_from_batch(
                &file_delete_batch_with_id_and_path(file_id, Some(path)),
                None,
                &BTreeSet::new(),
                Some("plugin_sentinel"),
            )
            .expect_err("noncanonical plugin archive delete should fail");

            assert!(
                error
                    .to_string()
                    .contains("one exact canonical plugin archive"),
                "unexpected error for {file_id} at {path}: {error}"
            );
        }
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
                data: false,
                descriptor: super::LixFileDescriptorUpdate::Path,
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
                data: false,
                descriptor: super::LixFileDescriptorUpdate::Path,
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
                data: false,
                descriptor: super::LixFileDescriptorUpdate::Path,
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
                data: false,
                descriptor: super::LixFileDescriptorUpdate::Path,
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
                data: true,
                descriptor: super::LixFileDescriptorUpdate::Path,
            },
            Some(&mut resolvers),
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect("decode file path and data update");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.file_data_writes.len(), 1);
        assert_eq!(staged.file_data_writes[0].file_id, "file-readme");
        assert_eq!(staged.file_data_writes[0].data(), b"hello");
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
                data: true,
                descriptor: super::LixFileDescriptorUpdate::Topology,
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
        assert_eq!(staged.file_data_writes[0].data(), b"hello");
        let blob_ref_row = staged
            .state_rows
            .iter()
            .find(|row| row.schema_key == "lix_binary_blob_ref")
            .expect("data update should stage blob ref row");
        let snapshot: serde_json::Value = blob_ref_row
            .snapshot
            .as_ref()
            .expect("blob ref should carry snapshot")
            .value()
            .clone();
        assert_eq!(
            snapshot["blob_hash"].as_str(),
            staged.file_data_writes[0]
                .blob_hash()
                .map(BlobHash::to_hex)
                .as_deref()
        );
    }

    #[test]
    fn file_metadata_data_update_reuses_materialized_path_without_resolver() {
        let batch = metadata_data_update_batch();
        let assignments = vec![
            literal_assignment("data", ScalarValue::Binary(Some(b"updated".to_vec()))),
            literal_assignment(
                "lixcol_metadata",
                ScalarValue::Utf8(Some(r#"{"source":"upload"}"#.to_string())),
            ),
        ];
        let update_columns = super::LixFileUpdateColumns::from_assignments(&assignments);
        assert!(update_columns.data);
        assert!(update_columns.writes_descriptor());
        assert!(
            !update_columns.requires_path_resolver(),
            "metadata must not be treated as a filesystem topology mutation"
        );

        let structural_columns =
            super::LixFileUpdateColumns::from_assignments(&[literal_assignment(
                "directory_id",
                ScalarValue::Utf8(Some("dir-other".to_string())),
            )]);
        assert!(
            structural_columns.requires_path_resolver(),
            "directory moves must retain resolver validation"
        );

        let assignment_values =
            super::UpdateAssignmentValues::from_batch_columns(&batch, &["data", "lixcol_metadata"]);
        let staged = super::lix_file_update_stage_from_batch(
            &batch,
            &assignment_values,
            None,
            update_columns,
            &BTreeSet::from([blob_ref_key("branch-b", false, false, "file-readme")]),
            &BTreeSet::new(),
            None,
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect("metadata/data update should not need a path resolver");

        let descriptor = staged
            .state_rows
            .iter()
            .find(|row| row.schema_key == "lix_file_descriptor")
            .expect("metadata update should stage a descriptor row");
        assert_eq!(
            descriptor.metadata.as_ref().map(TransactionJson::value),
            Some(&serde_json::json!({"source": "upload"}))
        );
        assert_eq!(staged.file_data_writes.len(), 1);
        assert_eq!(
            staged.file_data_writes[0].path.as_deref(),
            Some("/docs/readme.md")
        );
        assert_eq!(staged.file_data_writes[0].data(), b"updated");
    }

    #[test]
    fn file_data_update_without_path_ignores_materialized_path_column() {
        let staged = lix_file_update_stage_from_batch_for_test(
            &path_update_batch(),
            None,
            super::LixFileUpdateColumns {
                data: true,
                descriptor: super::LixFileDescriptorUpdate::None,
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
                data: true,
                descriptor: super::LixFileDescriptorUpdate::None,
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
        assert_eq!(staged.file_data_writes[0].data(), b"hello");
        let snapshot: serde_json::Value = blob_ref_row
            .snapshot
            .as_ref()
            .expect("blob ref should carry snapshot")
            .value()
            .clone();
        assert_eq!(
            snapshot["blob_hash"].as_str(),
            staged.file_data_writes[0]
                .blob_hash()
                .map(BlobHash::to_hex)
                .as_deref()
        );
    }

    #[test]
    fn file_delete_with_blob_ref_stages_descriptor_and_blob_ref_tombstones() {
        let batch = file_delete_batch();
        let staged = lix_file_delete_stage_from_batch(
            &batch,
            None,
            &BTreeSet::from([blob_ref_key("branch-b", false, false, "file-readme")]),
            None,
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
        let staged = lix_file_delete_stage_from_batch(&batch, None, &BTreeSet::new(), None)
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
            None,
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
    async fn file_delete_reuses_single_candidate_scan() {
        let mut write_context = CapturingWriteContext {
            rows: file_dml_rows(),
            ..CapturingWriteContext::default()
        };
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let spec = file_dml_spec(write_ctx.clone());
        let planned = spec
            .plan_delete(write_ctx, &[])
            .await
            .expect("plan file delete");

        let source_batch = (planned.source)().await.expect("load delete candidates");
        assert_eq!(write_context.scan_count, 1);
        let count = (planned.apply)(source_batch)
            .await
            .expect("apply file delete");

        assert_eq!(count, 1);
        assert_eq!(write_context.scan_count, 1);
        let TransactionWrite::Rows { rows, .. } = &write_context.writes[0] else {
            panic!("delete should stage state rows");
        };
        assert!(
            rows.iter()
                .any(|row| row.schema_key == super::FILE_DESCRIPTOR_SCHEMA_KEY)
        );
        assert!(
            rows.iter()
                .any(|row| row.schema_key == super::BLOB_REF_SCHEMA_KEY)
        );
    }

    #[tokio::test]
    async fn file_delete_by_exact_id_uses_path_index_and_exact_blob_batch() {
        let mut rows = file_dml_rows();
        rows.extend([
            live_file_row(
                "file-other",
                "branch-b",
                r#"{"id":"file-other","directory_id":null,"name":"other.md"}"#,
            ),
            live_blob_ref_row("file-other", "branch-b", "file-other", &"1".repeat(64), 7),
        ]);
        let mut write_context = CapturingWriteContext {
            rows,
            ..CapturingWriteContext::default()
        };
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let spec = file_dml_spec(write_ctx.clone());
        let planned = spec
            .plan_delete(write_ctx, &[eq_filter("id", "file-readme")])
            .await
            .expect("plan exact-id file delete");

        let source_batch = (planned.source)()
            .await
            .expect("load exact delete candidates");
        assert_eq!(source_batch.num_rows(), 1);
        assert_eq!(write_context.path_index_count, 1);
        assert_eq!(write_context.scan_count, 0);
        assert_eq!(write_context.exact_load_requests.len(), 1);
        assert_eq!(write_context.exact_load_requests[0].rows.len(), 1);
        assert_eq!(
            write_context.exact_load_requests[0].rows[0].entity_pk,
            crate::entity_pk::EntityPk::single("file-readme")
        );
        assert_eq!(
            write_context.exact_load_requests[0].rows[0]
                .file_id
                .as_deref(),
            Some("file-readme")
        );
    }

    #[tokio::test]
    async fn file_update_reuses_source_rows_for_blob_and_path_state() {
        let mut write_context = CapturingWriteContext {
            rows: file_dml_rows(),
            ..CapturingWriteContext::default()
        };
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let spec = file_dml_spec(write_ctx.clone());
        let planned = spec
            .plan_update(
                write_ctx,
                vec![literal_assignment(
                    "name",
                    ScalarValue::Utf8(Some("README.md".to_string())),
                )],
                &[],
            )
            .await
            .expect("plan file update");

        let source_batch = (planned.source)().await.expect("load update candidates");
        assert_eq!(write_context.scan_count, 1);
        let count = (planned.apply)(source_batch)
            .await
            .expect("apply file update");

        assert_eq!(count, 1);
        assert_eq!(write_context.scan_count, 1);
        let TransactionWrite::Rows { rows, .. } = &write_context.writes[0] else {
            panic!("descriptor update should stage state rows");
        };
        let snapshot = rows[0].snapshot.as_ref().expect("updated snapshot").value();
        assert_eq!(snapshot["name"], "README.md");
    }

    #[tokio::test]
    async fn file_update_exact_id_intersects_path_before_exact_blob_batch() {
        let mut rows = file_dml_rows();
        rows.push(live_file_row(
            "file-other",
            "branch-b",
            r#"{"id":"file-other","directory_id":null,"name":"other.md"}"#,
        ));
        let mut write_context = CapturingWriteContext {
            rows,
            ..CapturingWriteContext::default()
        };
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let spec = file_dml_spec(write_ctx.clone());
        let planned = spec
            .plan_update(
                write_ctx,
                vec![literal_assignment(
                    "name",
                    ScalarValue::Utf8(Some("README.md".to_string())),
                )],
                &[
                    eq_filter("id", "file-readme"),
                    eq_filter("path", "/other.md"),
                ],
            )
            .await
            .expect("plan exact-id/path file update");

        let source_batch = (planned.source)()
            .await
            .expect("load exact update candidates");
        assert_eq!(source_batch.num_rows(), 0);
        assert_eq!(write_context.path_index_count, 1);
        assert_eq!(write_context.scan_count, 0);
        assert_eq!(
            write_context.exact_load_requests.len(),
            1,
            "exact DML validates the requested blob identity even when the path predicate rejects its descriptor"
        );
    }

    #[tokio::test]
    async fn file_data_update_reuses_source_blob_ref_keys() {
        let mut write_context = CapturingWriteContext {
            rows: file_dml_rows(),
            ..CapturingWriteContext::default()
        };
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let spec = file_dml_spec(write_ctx.clone());
        let planned = spec
            .plan_update(
                write_ctx,
                vec![literal_assignment(
                    "data",
                    ScalarValue::LargeBinary(Some(Vec::new())),
                )],
                &[],
            )
            .await
            .expect("plan file data update");

        let source_batch = (planned.source)().await.expect("load update candidates");
        let count = (planned.apply)(source_batch)
            .await
            .expect("apply file data update");

        assert_eq!(count, 1);
        assert_eq!(write_context.scan_count, 1);
        let TransactionWrite::RowsWithFileData {
            rows, file_data, ..
        } = &write_context.writes[0]
        else {
            panic!("data update should stage rows and file data");
        };
        assert!(file_data[0].data().is_empty());
        assert!(file_data[0].had_blob_ref);
        assert!(
            rows.iter().any(|row| {
                row.schema_key == super::BLOB_REF_SCHEMA_KEY && row.snapshot.is_none()
            })
        );
    }

    #[tokio::test]
    async fn file_upsert_attribute_update_uses_exact_id_blob_lookup() {
        let mut write_context = CapturingWriteContext {
            rows: file_dml_rows(),
            ..CapturingWriteContext::default()
        };
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let spec = file_dml_spec(write_ctx.clone());
        let assignments = vec![
            literal_assignment("data", ScalarValue::Binary(Some(b"updated".to_vec()))),
            literal_assignment(
                "lixcol_metadata",
                ScalarValue::Utf8(Some(r#"{"source":"upload"}"#.to_string())),
            ),
        ];

        let staged = spec
            .apply_conflict_update(&write_ctx, &metadata_data_update_batch(), &assignments)
            .await
            .expect("attribute-only conflict update should stage");

        assert_eq!(write_context.path_index_count, 0);
        assert_eq!(write_context.exact_load_requests.len(), 1);
        assert_eq!(write_context.scan_count, 0);
        assert_eq!(staged.file_data.len(), 1);
        assert_eq!(staged.file_data[0].path.as_deref(), Some("/docs/readme.md"));
    }

    #[tokio::test]
    async fn file_id_conflict_probe_uses_path_index_and_exact_blob_batch() {
        let data = b"hello".to_vec();
        let blob_hash = BlobHash::from_content(&data);
        let rows = vec![
            live_file_row(
                "file-readme",
                "branch-b",
                r#"{"id":"file-readme","directory_id":null,"name":"readme.md"}"#,
            ),
            live_blob_ref_row(
                "file-readme",
                "branch-b",
                "file-readme",
                &blob_hash.to_hex(),
                data.len(),
            ),
        ];
        let mut write_context = CapturingWriteContext {
            rows,
            blob_bytes_by_hash: BTreeMap::from([(blob_hash, data)]),
            ..CapturingWriteContext::default()
        };
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let spec = file_dml_spec(write_ctx.clone());

        let candidates = spec
            .scan_conflict_candidates(
                &write_ctx,
                &file_insert_batch(false, false),
                &UpsertConflictTarget::id(super::LIX_FILE_IDENTITY),
            )
            .await
            .expect("scan exact ID conflict candidates");

        assert_eq!(candidates.num_rows(), 1);
        assert_eq!(write_context.path_index_count, 1);
        assert_eq!(write_context.exact_load_requests.len(), 1);
        assert_eq!(write_context.exact_load_requests[0].rows.len(), 1);
        assert_eq!(write_context.scan_count, 0);
    }

    #[tokio::test]
    async fn fast_file_data_update_uses_path_index_and_target_blob_scan() {
        let path_index_requests = Arc::new(AtomicUsize::new(0));
        let scan_requests = Arc::new(Mutex::new(Vec::new()));
        let index = Arc::new(
            FilesystemPathIndex::from_live_rows(vec![
                live_directory_row(
                    "dir-docs",
                    "branch-b",
                    r#"{"id":"dir-docs","parent_id":null,"name":"docs"}"#,
                ),
                live_file_row(
                    "file-readme",
                    "branch-b",
                    r#"{"id":"file-readme","directory_id":"dir-docs","name":"readme.md"}"#,
                ),
            ])
            .expect("filesystem path index should build"),
        );
        let old_data = b"old";
        let mut write_context = IndexedFileDataUpdateWriteContext {
            index,
            blob_rows: vec![live_blob_ref_row(
                "file-readme",
                "branch-b",
                "file-readme",
                &BlobHash::from_content(old_data).to_hex(),
                old_data.len(),
            )],
            writes: Vec::new(),
            scan_requests: Arc::clone(&scan_requests),
            path_index_requests: Arc::clone(&path_index_requests),
        };

        let count = super::execute_fast_lix_file_data_update_by_id(
            &mut write_context,
            Some("file-readme".to_string()),
            b"new".to_vec(),
            None,
            None,
        )
        .await
        .expect("fast data update should stage");

        assert_eq!(count, 1);
        assert_eq!(path_index_requests.load(Ordering::SeqCst), 1);
        {
            let requests = scan_requests.lock().expect("scan request mutex");
            assert_eq!(requests.len(), 1);
            assert_eq!(
                requests[0].filter.schema_keys,
                vec![super::BLOB_REF_SCHEMA_KEY.to_string()]
            );
            assert_eq!(
                requests[0].filter.entity_pks,
                vec![crate::entity_pk::EntityPk::single("file-readme")]
            );
        }

        let TransactionWrite::RowsWithFileData { file_data, .. } = &write_context.writes[0] else {
            panic!("data update should stage file data");
        };
        assert_eq!(file_data.len(), 1);
        assert_eq!(file_data[0].path.as_deref(), Some("/docs/readme.md"));
        assert_eq!(file_data[0].data(), b"new");
        assert!(file_data[0].had_blob_ref);
        assert!(file_data[0].splice_provenance().is_none());

        let provenance = crate::common::RequestBlobSpliceProvenance {
            base_sha256: "a".repeat(64),
            result_sha256: "b".repeat(64),
            prefix_bytes: 1,
            suffix_bytes: 1,
            insert: b"e".to_vec(),
        };
        let count = super::execute_fast_lix_file_data_update_by_id(
            &mut write_context,
            Some("file-readme".to_string()),
            b"next".to_vec(),
            Some(provenance.clone()),
            Some(crate::common::MutationIdentity {
                namespace_seed: [7; 16],
                operation_proof: [17; 32],
            }),
        )
        .await
        .expect("fast spliced data update should stage");
        assert_eq!(count, 1);
        let TransactionWrite::RowsWithFileData { file_data, .. } = &write_context.writes[1] else {
            panic!("spliced data update should stage file data");
        };
        assert_eq!(file_data[0].file_id, "file-readme");
        assert_eq!(file_data[0].data(), b"next");
        assert_eq!(file_data[0].splice_provenance(), Some(&provenance));
        assert_eq!(
            file_data[0].mutation_identity(),
            Some(crate::common::MutationIdentity {
                namespace_seed: [7; 16],
                operation_proof: [17; 32],
            })
        );
    }

    #[tokio::test]
    async fn fast_file_path_upsert_uses_exact_index_without_scanning_blob_state() {
        let old_data = b"old";
        let rows = vec![
            live_file_row(
                "file-readme",
                "branch-b",
                r#"{"id":"file-readme","directory_id":null,"name":"readme.md"}"#,
            ),
            live_blob_ref_row(
                "file-readme",
                "branch-b",
                "file-readme",
                &BlobHash::from_content(old_data).to_hex(),
                old_data.len(),
            ),
        ];
        let mut write_context = CapturingWriteContext {
            rows,
            ..CapturingWriteContext::default()
        };

        let outcome = super::execute_fast_lix_file_path_writes(
            &mut write_context,
            vec![(
                "/readme.md".to_string(),
                b"new".to_vec(),
                Some(TransactionJson::from_value_for_test(
                    serde_json::json!({"source": "upload"}),
                )),
                None,
            )],
            super::FastLixFilePathWriteConflict::UpdateDataAndMetadata,
            None,
        )
        .await
        .expect("existing path upsert should stage");

        assert!(outcome.is_some());
        assert_eq!(write_context.path_index_count, 1);
        assert_eq!(write_context.exact_load_requests.len(), 1);
        assert_eq!(write_context.exact_load_requests[0].rows.len(), 1);
        assert_eq!(write_context.scan_count, 0);
        let TransactionWrite::RowsWithFileData {
            rows, file_data, ..
        } = &write_context.writes[0]
        else {
            panic!("path upsert should stage descriptor, blob, and file data");
        };
        assert_eq!(file_data.len(), 1);
        assert_eq!(file_data[0].path.as_deref(), Some("/readme.md"));
        assert_eq!(file_data[0].data(), b"new");
        assert!(file_data[0].had_blob_ref);
        let descriptor = rows
            .iter()
            .find(|row| row.schema_key == super::FILE_DESCRIPTOR_SCHEMA_KEY)
            .expect("metadata upsert should rewrite the descriptor");
        assert_eq!(
            descriptor.metadata.as_ref(),
            Some(&TransactionJson::from_value_for_test(
                serde_json::json!({"source": "upload"})
            ))
        );
    }

    #[tokio::test]
    async fn fast_file_path_upsert_mixes_existing_and_missing_without_full_scan() {
        let old_data = b"old";
        let existing_provenance = crate::common::RequestBlobSpliceProvenance {
            base_sha256: "a".repeat(64),
            result_sha256: "b".repeat(64),
            prefix_bytes: 1,
            suffix_bytes: 1,
            insert: b"existing".to_vec(),
        };
        let missing_provenance = crate::common::RequestBlobSpliceProvenance {
            base_sha256: "c".repeat(64),
            result_sha256: "d".repeat(64),
            prefix_bytes: 2,
            suffix_bytes: 3,
            insert: b"missing".to_vec(),
        };
        let rows = vec![
            live_file_row(
                "file-readme",
                "branch-b",
                r#"{"id":"file-readme","directory_id":null,"name":"readme.md"}"#,
            ),
            live_blob_ref_row(
                "file-readme",
                "branch-b",
                "file-readme",
                &BlobHash::from_content(old_data).to_hex(),
                old_data.len(),
            ),
        ];
        let mut write_context = CapturingWriteContext {
            rows,
            ..CapturingWriteContext::default()
        };

        let outcome = super::execute_fast_lix_file_path_writes(
            &mut write_context,
            vec![
                (
                    "/readme.md".to_string(),
                    b"updated".to_vec(),
                    None,
                    Some(existing_provenance.clone()),
                ),
                (
                    "/new.md".to_string(),
                    b"new".to_vec(),
                    None,
                    Some(missing_provenance.clone()),
                ),
            ],
            super::FastLixFilePathWriteConflict::UpdateData,
            Some(crate::common::MutationIdentity {
                namespace_seed: [8; 16],
                operation_proof: [18; 32],
            }),
        )
        .await
        .expect("mixed path upsert should stage");

        assert!(outcome.is_some());
        assert_eq!(write_context.path_index_count, 1);
        assert_eq!(write_context.exact_load_requests.len(), 1);
        assert_eq!(write_context.exact_load_requests[0].rows.len(), 1);
        assert_eq!(write_context.scan_count, 0);
        let TransactionWrite::RowsWithFileData {
            rows, file_data, ..
        } = &write_context.writes[0]
        else {
            panic!("mixed path upsert should stage file data");
        };
        assert_eq!(file_data.len(), 2);
        assert!(file_data[0].had_blob_ref);
        assert!(!file_data[1].had_blob_ref);
        assert_eq!(file_data[0].splice_provenance(), Some(&existing_provenance));
        assert_eq!(file_data[1].splice_provenance(), Some(&missing_provenance));
        assert!(file_data.iter().all(|file_data| {
            file_data.mutation_identity()
                == Some(crate::common::MutationIdentity {
                    namespace_seed: [8; 16],
                    operation_proof: [18; 32],
                })
        }));
        assert!(rows.iter().any(|row| {
            row.schema_key == super::FILE_DESCRIPTOR_SCHEMA_KEY
                && row.origin.as_ref().is_some_and(|origin| {
                    origin.operation == super::TransactionWriteOperation::Insert
                })
        }));
    }

    #[tokio::test]
    async fn fast_file_path_upsert_creates_nested_directories_from_index() {
        let mut write_context = CapturingWriteContext::default();

        let outcome = super::execute_fast_lix_file_path_writes(
            &mut write_context,
            vec![(
                "/new/nested/file.md".to_string(),
                b"new".to_vec(),
                None,
                None,
            )],
            super::FastLixFilePathWriteConflict::UpdateDataAndMetadata,
            None,
        )
        .await
        .expect("nested missing path upsert should stage");

        assert!(outcome.is_some());
        assert_eq!(write_context.path_index_count, 1);
        assert!(write_context.exact_load_requests.is_empty());
        assert_eq!(write_context.scan_count, 0);
        let TransactionWrite::RowsWithFileData {
            rows, file_data, ..
        } = &write_context.writes[0]
        else {
            panic!("nested path upsert should stage descriptors and file data");
        };
        assert_eq!(file_data.len(), 1);
        assert_eq!(
            rows.iter()
                .filter(|row| row.schema_key == super::DIRECTORY_DESCRIPTOR_SCHEMA_KEY)
                .count(),
            2
        );
        assert_eq!(
            rows.iter()
                .filter(|row| row.schema_key == super::FILE_DESCRIPTOR_SCHEMA_KEY)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn fast_file_path_upsert_rejects_duplicate_missing_paths_before_staging() {
        let mut write_context = CapturingWriteContext::default();

        let error = super::execute_fast_lix_file_path_writes(
            &mut write_context,
            vec![
                ("/duplicate.md".to_string(), b"first".to_vec(), None, None),
                ("/duplicate.md".to_string(), b"second".to_vec(), None, None),
            ],
            super::FastLixFilePathWriteConflict::UpdateData,
            None,
        )
        .await
        .expect_err("duplicate missing path should be rejected");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
        assert_eq!(write_context.path_index_count, 1);
        assert_eq!(write_context.scan_count, 0);
        assert!(write_context.writes.is_empty());
    }

    #[tokio::test]
    async fn fast_file_path_upsert_preserves_root_directory_namespace_collision() {
        let mut write_context = CapturingWriteContext {
            rows: vec![live_directory_row(
                "dir-docs",
                "branch-b",
                r#"{"id":"dir-docs","parent_id":null,"name":"docs"}"#,
            )],
            ..CapturingWriteContext::default()
        };

        let error = super::execute_fast_lix_file_path_writes(
            &mut write_context,
            vec![("/docs".to_string(), b"file".to_vec(), None, None)],
            super::FastLixFilePathWriteConflict::UpdateData,
            None,
        )
        .await
        .expect_err("file should not overwrite a same-name root directory");

        assert_eq!(error.code, LixError::CODE_UNIQUE);
        assert_eq!(write_context.path_index_count, 1);
        assert_eq!(write_context.scan_count, 1);
        assert!(write_context.writes.is_empty());
    }

    #[tokio::test]
    async fn fast_file_path_upsert_does_not_cross_blob_ref_scope_lanes() {
        let local_descriptor = live_file_row(
            "file-local",
            "branch-b",
            r#"{"id":"file-local","directory_id":null,"name":"local.md"}"#,
        );
        let mut global_fallback = live_blob_ref_row(
            "file-local",
            crate::GLOBAL_BRANCH_ID,
            "file-local",
            &BlobHash::from_content(b"global").to_hex(),
            6,
        );
        global_fallback.global = true;

        let mut global_descriptor = live_file_row(
            "file-global",
            "branch-b",
            r#"{"id":"file-global","directory_id":null,"name":"global.md"}"#,
        );
        global_descriptor.global = true;
        let branch_override = live_blob_ref_row(
            "file-global",
            "branch-b",
            "file-global",
            &BlobHash::from_content(b"branch").to_hex(),
            6,
        );
        let mut write_context = CapturingWriteContext {
            rows: vec![
                local_descriptor,
                global_fallback,
                global_descriptor,
                branch_override,
            ],
            ..CapturingWriteContext::default()
        };

        let outcome = super::execute_fast_lix_file_path_writes(
            &mut write_context,
            vec![
                ("/local.md".to_string(), b"new-local".to_vec(), None, None),
                ("/global.md".to_string(), b"new-global".to_vec(), None, None),
            ],
            super::FastLixFilePathWriteConflict::UpdateData,
            None,
        )
        .await
        .expect("scope-isolated path upsert should stage");

        assert!(outcome.is_some());
        assert_eq!(write_context.path_index_count, 1);
        assert_eq!(write_context.exact_load_requests.len(), 1);
        assert_eq!(write_context.exact_load_requests[0].rows.len(), 2);
        assert_eq!(write_context.scan_count, 0);
        let TransactionWrite::RowsWithFileData {
            rows, file_data, ..
        } = &write_context.writes[0]
        else {
            panic!("scope-isolated path upsert should stage file data");
        };
        assert_eq!(file_data.len(), 2);
        assert!(file_data.iter().all(|write| !write.had_blob_ref));
        assert!(rows.iter().all(|row| row.snapshot.is_some()));
    }

    #[tokio::test]
    async fn fast_empty_file_path_upsert_loads_exact_prior_blob() {
        let old_data = b"old";
        let rows = vec![
            live_file_row(
                "file-readme",
                "branch-b",
                r#"{"id":"file-readme","directory_id":null,"name":"readme.md"}"#,
            ),
            live_blob_ref_row(
                "file-readme",
                "branch-b",
                "file-readme",
                &BlobHash::from_content(old_data).to_hex(),
                old_data.len(),
            ),
        ];
        let mut write_context = CapturingWriteContext {
            rows,
            ..CapturingWriteContext::default()
        };

        let outcome = super::execute_fast_lix_file_path_writes(
            &mut write_context,
            vec![("/readme.md".to_string(), Vec::new(), None, None)],
            super::FastLixFilePathWriteConflict::UpdateData,
            None,
        )
        .await
        .expect("empty existing path upsert should stage");

        assert!(outcome.is_some());
        assert_eq!(write_context.path_index_count, 1);
        assert_eq!(write_context.exact_load_requests.len(), 1);
        assert_eq!(write_context.exact_load_requests[0].rows.len(), 1);
        assert_eq!(write_context.scan_count, 0);
        let TransactionWrite::RowsWithFileData {
            rows, file_data, ..
        } = &write_context.writes[0]
        else {
            panic!("empty path upsert should stage a blob tombstone and file data");
        };
        assert!(file_data[0].data().is_empty());
        assert!(file_data[0].had_blob_ref);
        assert!(
            rows.iter().any(|row| {
                row.schema_key == super::BLOB_REF_SCHEMA_KEY && row.snapshot.is_none()
            })
        );
    }

    #[tokio::test]
    async fn fast_file_path_write_declines_ambiguous_cross_scope_paths() {
        let tracked = live_file_row(
            "file-tracked",
            "branch-b",
            r#"{"id":"file-tracked","directory_id":null,"name":"shared.md"}"#,
        );
        let mut untracked = live_file_row(
            "file-untracked",
            "branch-b",
            r#"{"id":"file-untracked","directory_id":null,"name":"shared.md"}"#,
        );
        untracked.untracked = true;
        let mut write_context = CapturingWriteContext {
            rows: vec![tracked, untracked],
            ..CapturingWriteContext::default()
        };

        let outcome = super::execute_fast_lix_file_path_writes(
            &mut write_context,
            vec![("/shared.md".to_string(), b"new".to_vec(), None, None)],
            super::FastLixFilePathWriteConflict::UpdateDataAndMetadata,
            None,
        )
        .await
        .expect("ambiguous legacy topology should decline the fast path");

        assert_eq!(outcome, None);
        assert_eq!(write_context.path_index_count, 1);
        assert_eq!(write_context.scan_count, 1);
        assert!(write_context.writes.is_empty());
    }

    #[tokio::test]
    async fn file_dml_apply_without_source_state_errors() {
        let mut write_context = CapturingWriteContext::default();
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let spec = file_dml_spec(write_ctx.clone());
        let planned = spec
            .plan_delete(write_ctx, &[])
            .await
            .expect("plan file delete");

        let error = (planned.apply)(RecordBatch::new_empty(super::lix_file_schema()))
            .await
            .expect_err("apply without source should fail");

        assert!(
            error
                .to_string()
                .contains("lix_file DELETE source state missing")
        );
        assert_eq!(write_context.scan_count, 0);
        assert!(write_context.writes.is_empty());
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
            ..CapturingWriteContext::default()
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
                assert_eq!(file_data[0].data(), b"hello");
                assert!(!file_data[0].had_blob_ref);
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
            ..CapturingWriteContext::default()
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
