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
use std::ops::Range;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, LargeBinaryArray, RecordBatchOptions, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, DataFusionError, Result, ScalarValue};
use datafusion::datasource::TableType;
use datafusion::execution::TaskContext;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::expr::{InList, Like};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::{PhysicalExpr, create_physical_expr};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion::prelude::SessionContext;
use futures_util::{FutureExt, future::try_join_all};
use serde::Deserialize;

use crate::binary_cas::{BlobDataReader, BlobId, BlobRangeBytes};
use crate::branch::BranchRefReader;
use crate::common::{LixPath, MutationIdentity, RequestBlobSpliceProvenance, compose_file_path};
use crate::filesystem::{FilesystemIndex, filesystem_schema_keys};
use crate::filesystem::{
    FilesystemPathEntry, FilesystemPathIndexReader, FilesystemPathIndexRequest, FilesystemPathKind,
    FilesystemPathSelection,
};
use crate::functions::FunctionProviderHandle;
use crate::hot_state::MaterializedHotStateRow;
use crate::hot_state::{
    HotStateExactBatchRequest, HotStateExactRowRequest, HotStateFilter, HotStateProjection,
    HotStateReader, HotStateScanRequest, MaterializedHotStateBatch,
    MaterializedHotStateBatchBuilder, MaterializedHotStateRowRef,
};
use crate::plugin::runtime::{
    CompiledPluginCatalog, PLUGIN_OWNER_KEY, PLUGIN_REGISTRY_KEY, PluginActorKey, PluginFileOwner,
    PluginRegistry, PluginRegistryEntry, PluginRuntimeHost, is_plugin_storage_path,
    plugin_archive_delete_origin, plugin_archive_file_id_matches, plugin_key_from_archive_path,
    plugin_storage_archive_file_id,
};
use crate::row_pk::RowPk;
use crate::sql2::branch_scope::{
    BranchBinding, resolve_provider_branch_ids, resolve_write_branch_scope,
};
use crate::sql2::dml::InsertSink;
use crate::sql2::predicate_typecheck::{
    canonicalize_json_identity_text_filters, validate_json_predicate_filters,
};
use crate::sql2::write_normalization::{
    InsertCell, InsertColumnIntents, LIX_FILE_CONTENT_CAST_HINT, SqlCell, UpdateAssignmentValues,
    UpdateCell, defaultable_bool_insert_value, defaultable_text_insert_value,
    insert_column_is_omitted, lix_file_content_type_error, lix_file_content_type_error_with_value,
    scalar_is_binary_or_null,
};
use crate::sql2::{SessionFileViewKey, SessionFileViews, SessionPluginFileView};
#[cfg(test)]
use crate::transaction_types::TransactionWriteRow;
use crate::transaction_types::{RawWriteBatch, TransactionJson};
use crate::{
    GLOBAL_BRANCH_ID, LixError, SqlQueryResult, Value, parse_row_metadata_value,
    serialize_row_metadata,
};

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";

use crate::filesystem::{
    BlobRefRowInput, DirectoryPathRecord, DirectoryPathResolver, FileDeleteInput,
    FileDescriptorWriteInput, FileDescriptorWriteIntent, FilesystemBlobRefKey,
    FilesystemDeletePlan, FilesystemDescriptorKey, FilesystemRowContext,
    append_blob_ref_tombstone_row, derive_directory_paths, directory_path_resolvers_for_paths,
    directory_path_resolvers_from_hot_state, directory_path_resolvers_from_path_index,
    directory_path_resolvers_from_state_batch, filesystem_storage_scope_key, plan_file_delete,
    plan_file_descriptor_write, plan_parsed_file_path_update_with_resolvers,
    plan_parsed_file_path_write_with_resolvers,
};
use crate::sql2::result_metadata::json_field;
use crate::sql2::session::SqlWriteSessionOptions;
use crate::sql2::{
    SqlWriteContext, SqlWriteExecutionContext, WriteAccess, WriteContextHotStateReader,
};
use crate::transaction_types::{
    FileContent, LogicalPrimaryKey, TransactionFileContent, TransactionWrite, TransactionWriteMode,
    TransactionWriteOperation, TransactionWriteOrigin,
};

use super::spec::{
    DmlApply, DmlPlanOptions, DmlReturning, InsertApply, PlannedDml, PlannedScan, RowSource,
    TableSpec, finish_scan_batch, register_spec_table, row_source, scan_row_source,
    take_record_batch_rows,
};
use super::upsert::{
    StagedUpsert, UpsertConflictKind, UpsertConflictTarget, UpsertReturningRow, UpsertSupport,
    materialize_omitted_column, materialize_omitted_insert_default, validate_target_columns,
};

pub(super) async fn register_lix_file_active_provider(
    session: &SessionContext,
    surface_name: &str,
    active_branch_id: &str,
    hot_state: Arc<dyn HotStateReader>,
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
                hot_state,
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

#[derive(Clone)]
struct LixFileSpec {
    schema: SchemaRef,
    hot_state: Arc<dyn HotStateReader>,
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
    path_resolvers: Option<BTreeMap<String, DirectoryPathResolver>>,
    path_index: Option<FilesystemPathSelection>,
}

/// Stable public identity for a file post-image. The by-branch surface needs
/// both components; the active surface exposes one visible branch scope and
/// uses an empty branch discriminator.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct FileReturningKey {
    id: String,
    branch_id: String,
}

#[derive(Clone, Copy)]
struct LixFileDmlSourceOptions {
    needs_data: bool,
    needs_plugin_ownership: bool,
    capture_path_resolver_rows: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExactLixFileReadColumn {
    Content,
    ChangeId,
}

impl ExactLixFileReadColumn {
    fn name(self) -> &'static str {
        match self {
            Self::Content => "content",
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
        request: &HotStateScanRequest,
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
            .path_index(
                &FilesystemPathIndexRequest::new(request.filter.branch_ids.clone())
                    .with_blob_refs(true),
            )
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
        hot_state: Arc<dyn HotStateReader>,
        filesystem_path_index: Arc<dyn FilesystemPathIndexReader>,
        branch_ref: Arc<dyn BranchRefReader>,
        blob_reader: Arc<dyn BlobDataReader>,
        plugin_host: PluginRuntimeHost,
        functions: FunctionProviderHandle,
    ) -> Self {
        Self {
            schema: lix_file_schema(),
            hot_state,
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
        let hot_state = Arc::new(WriteContextHotStateReader::new(write_ctx.clone()));
        let filesystem_path_index: Arc<dyn FilesystemPathIndexReader> = hot_state.clone();
        let blob_reader = write_ctx.blob_reader();
        let plugin_host = write_ctx.plugin_host();
        let session_file_views = write_ctx.session_file_views();
        Self {
            schema: lix_file_schema(),
            hot_state,
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
        request: HotStateScanRequest,
        target_file_ids: FileIdConstraint,
        indexed_matches: Option<FilesystemPathSelection>,
        options: LixFileDmlSourceOptions,
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
                options,
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
                options,
                session_file_views,
                captured,
            )| async move {
                *captured.lock().expect("lix_file DML source mutex poisoned") = None;
                let hot_state: Arc<dyn HotStateReader> =
                    Arc::new(WriteContextHotStateReader::new(write_ctx.clone()));
                let (prepared, path_resolvers, path_index) = if let Some(indexed_matches) =
                    indexed_matches.as_ref()
                {
                    let rows = match &target_file_ids {
                        // Exact DML must still validate a targeted blob ref
                        // when its descriptor is missing from the path index.
                        FileIdConstraint::Ids(file_ids) => {
                            scan_exact_file_blob_batch(hot_state.clone(), &request, file_ids).await
                        }
                        FileIdConstraint::All | FileIdConstraint::None => {
                            scan_indexed_file_batch(indexed_matches, true)
                        }
                    }
                    .map_err(lix_error_to_datafusion_error)?;
                    (
                        prepare_indexed_lix_file_rows(indexed_matches, rows),
                        None,
                        options
                            .capture_path_resolver_rows
                            .then(|| indexed_matches.clone()),
                    )
                } else {
                    let rows =
                        scan_lix_file_live_batch(hot_state.clone(), &request, &target_file_ids)
                            .await
                            .map_err(lix_error_to_datafusion_error)?;
                    let path_resolvers = options
                        .capture_path_resolver_rows
                        .then(|| directory_path_resolvers_from_state_batch(&rows))
                        .transpose()
                        .map_err(lix_error_to_datafusion_error)?;
                    (
                        prepare_lix_file_rows(rows, &FilePathPredicate::All),
                        path_resolvers,
                        None,
                    )
                };
                let prepared = prepared.map_err(lix_error_to_datafusion_error)?;
                let plugin_render = if prepared.needs_plugin_render(options.needs_data)
                    || (options.needs_plugin_ownership && !prepared.file_rows.is_empty())
                {
                    plugin_render_context_for_lix_file_scan(
                        Arc::clone(&hot_state),
                        &request,
                        plugin_host,
                        &prepared,
                        options.needs_plugin_ownership,
                    )
                    .await
                    .map_err(|error| {
                        DataFusionError::Execution(format!(
                            "sql2 lix_file plugin discovery failed: {error}"
                        ))
                    })?
                    .map(|context| context.with_session_file_views(session_file_views.clone()))
                } else {
                    None
                };
                let blob_ref_keys = prepared.blob_rows.keys().cloned().collect();
                let source_batch = lix_file_record_batch_from_prepared(
                    &table_schema,
                    &blob_reader,
                    plugin_render.clone(),
                    options.needs_data,
                    prepared,
                )
                .await
                .map_err(lix_error_to_datafusion_error)?;
                *captured.lock().expect("lix_file DML source mutex poisoned") =
                    Some(LixFileDmlSourceState {
                        blob_ref_keys,
                        plugin_render,
                        path_resolvers,
                        path_index,
                    });
                Ok(source_batch)
            },
        )
    }

    fn returning_key_from_batch(
        &self,
        batch: &RecordBatch,
        row_index: usize,
    ) -> Result<FileReturningKey> {
        let id = required_string_value(batch, row_index, "id")?;
        let branch_id = String::new();
        Ok(FileReturningKey { id, branch_id })
    }

    fn materialize_returning_insert_defaults(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        if !insert_column_is_omitted(batch, "id") {
            return Ok(batch.clone());
        }
        let ids = (0..batch.num_rows())
            .map(|row_index| {
                let plugin_archive_id = optional_string_value(batch, row_index, "path")?
                    .and_then(|path| plugin_key_from_archive_path(&path))
                    .map(|plugin_key| plugin_storage_archive_file_id(&plugin_key));
                Ok(Some(plugin_archive_id.unwrap_or_else(|| {
                    self.functions.call_uuid_v7().to_string()
                })))
            })
            .collect::<Result<Vec<_>>>()?;
        let ids = StringArray::from(ids);
        materialize_omitted_insert_default(batch, "id", Arc::new(ids))
    }

    /// Reload the just-staged rows from the transaction overlay. Filesystem
    /// writes derive paths, blob references, and audit fields during staging;
    /// projecting the pre-image with assignments applied would make
    /// `RETURNING *` stale or incomplete.
    async fn returning_post_image(
        &self,
        write_ctx: &SqlWriteContext,
        keys: &[FileReturningKey],
        needs_data: bool,
    ) -> Result<RecordBatch> {
        if keys.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }
        let request = lix_file_scan_request(
            self.branch_binding.active_branch_id(),
            Some(self.schema.as_ref()),
            None,
        );
        let file_ids = keys
            .iter()
            .map(|key| key.id.clone())
            .collect::<BTreeSet<_>>();
        let captured: SharedLixFileDmlSourceState = Arc::new(Mutex::new(None));
        let source = self.dml_source(
            write_ctx,
            request,
            FileIdConstraint::Ids(file_ids),
            None,
            LixFileDmlSourceOptions {
                needs_data,
                needs_plugin_ownership: false,
                capture_path_resolver_rows: false,
            },
            captured,
        );
        let batch = source().await?;
        let mut post_rows = BTreeMap::new();
        for row_index in 0..batch.num_rows() {
            let key = self.returning_key_from_batch(&batch, row_index)?;
            let index = u32::try_from(row_index).map_err(|_| {
                DataFusionError::Execution("lix_file RETURNING row index overflow".into())
            })?;
            if post_rows.insert(key.clone(), index).is_some() {
                return Err(DataFusionError::Execution(format!(
                    "lix_file RETURNING post-image contains duplicate row for id '{}'",
                    key.id
                )));
            }
        }
        let indices = keys
            .iter()
            .map(|key| {
                post_rows.get(key).copied().ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "lix_file RETURNING post-image is missing inserted or updated row '{}'",
                        key.id
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        take_record_batch_rows(&batch, &indices)
    }
}

/// Executes the narrow active-branch point-read shape without constructing a
/// DataFusion catalog and plan. Row selection, branch visibility, blob
/// loading and plugin rendering stay on the regular `lix_file` provider
/// helpers below. Durable materialized reads do not hydrate a Wasm actor;
/// mutation state is opened lazily by the first write.
pub(crate) async fn execute_exact_lix_file_read(
    active_branch_id: &str,
    hot_state: Arc<dyn HotStateReader>,
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
        .path_index(
            &FilesystemPathIndexRequest::new(request.filter.branch_ids.clone())
                .with_blob_refs(true)
                .with_cached_blob_data(column == ExactLixFileReadColumn::Content),
        )
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
    let rows = scan_indexed_file_batch(&matches, true)?;
    let prepared = prepare_indexed_lix_file_rows(&matches, rows)?;
    let load_data = column == ExactLixFileReadColumn::Content;
    let acknowledge_plugin_data = load_data && session_file_views.is_some();
    let plugin_render = if prepared.needs_plugin_render(true) || acknowledge_plugin_data {
        plugin_render_context_for_lix_file_scan(
            Arc::clone(&hot_state),
            &request,
            plugin_host,
            &prepared,
            acknowledge_plugin_data,
        )
        .await?
        .map(|context| context.with_session_file_views(session_file_views.clone()))
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

/// Executes the exact root-file listing used by the filesystem API directly
/// from the shared path index. The recognized SQL shape has no joins,
/// grouping, computed values, or residual predicates, so constructing a
/// DataFusion catalog, logical plan, physical plan and Arrow batch adds no
/// semantics.
pub(crate) async fn execute_exact_lix_file_root_listing(
    active_branch_id: &str,
    filesystem_path_index: Arc<dyn FilesystemPathIndexReader>,
    branch_ref: Arc<dyn BranchRefReader>,
) -> Result<SqlQueryResult, LixError> {
    let branch_binding = BranchBinding::active(active_branch_id);
    let branch_ids = resolve_provider_branch_ids(
        branch_ref.as_ref(),
        &branch_binding,
        vec![active_branch_id.to_string()],
    )
    .await?;
    let index = filesystem_path_index
        .path_index(&FilesystemPathIndexRequest::new(branch_ids))
        .await?;
    let matches = indexed_file_matches(index, &FilePathPredicate::All);
    let mut entries = matches
        .entries()
        .filter(|entry| entry.parent_id.is_none())
        .collect::<Vec<_>>();
    entries.sort_unstable_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.path.cmp(&right.path))
            .then_with(|| left.key.cmp(&right.key))
    });
    let rows = entries
        .into_iter()
        .map(|entry| {
            let metadata = entry
                .metadata()
                .map(|metadata| parse_row_metadata_value(metadata, "lix_file"))
                .transpose()?;
            Ok(vec![
                Value::Text(entry.id().to_string()),
                Value::Text(entry.path.clone()),
                Value::Text(entry.name.clone()),
                metadata.map_or(Value::Null, |metadata| Value::Jsonb(metadata.into())),
                Value::Text(entry.updated_at().to_string()),
            ])
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    Ok(SqlQueryResult {
        columns: vec![
            "id".to_string(),
            "path".to_string(),
            "name".to_string(),
            "lixcol_metadata".to_string(),
            "lixcol_updated_at".to_string(),
        ],
        column_types: vec![
            crate::ResultColumnType::Text,
            crate::ResultColumnType::Text,
            crate::ResultColumnType::Text,
            crate::ResultColumnType::Jsonb,
            crate::ResultColumnType::Text,
        ],
        rows,
        notices: Vec::new(),
    })
}

/// Executes Lixray's exact active-branch file batch without constructing a
/// DataFusion catalog, plan, or Arrow result batch. Keep this separate from
/// the established point-read path so unrelated file queries remain
/// byte-for-byte on their existing implementation.
pub(crate) async fn execute_exact_lix_file_batch_read(
    active_branch_id: &str,
    hot_state: Arc<dyn HotStateReader>,
    filesystem_path_index: Arc<dyn FilesystemPathIndexReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    blob_reader: Arc<dyn BlobDataReader>,
    plugin_host: PluginRuntimeHost,
    session_file_views: Option<SessionFileViews>,
    plugin_cache_snapshot: Option<u128>,
    paths: &BTreeSet<String>,
    data_range: Option<Range<u64>>,
) -> Result<SqlQueryResult, LixError> {
    let base_schema = lix_file_schema();
    let schema = Arc::new(Schema::new(vec![
        base_schema
            .field_with_name("path")
            .expect("lix_file schema should have path")
            .clone(),
        base_schema
            .field_with_name("content")
            .expect("lix_file schema should have content")
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
        .path_index(
            &FilesystemPathIndexRequest::new(request.filter.branch_ids.clone())
                .with_blob_refs(true)
                .with_cached_blob_data(data_range.is_none()),
        )
        .await?;
    let matches = indexed_file_matches(index, &FilePathPredicate::In(paths.clone()));
    let rows = scan_indexed_file_batch(&matches, true)?;
    let prepared = prepare_indexed_lix_file_rows(&matches, rows)?;
    let acknowledge_plugin_data = session_file_views.is_some();
    let plugin_render = if prepared.needs_plugin_render(true) || acknowledge_plugin_data {
        plugin_render_context_for_lix_file_scan_cached(
            Arc::clone(&hot_state),
            &request,
            plugin_host,
            &prepared,
            acknowledge_plugin_data,
            plugin_cache_snapshot,
        )
        .await?
        .map(|context| context.with_session_file_views(session_file_views.clone()))
    } else {
        None
    };

    // No relational operators remain after exact path selection. Move owned
    // blobs into the result instead of packing them into Arrow only for
    // DataFusion to copy them back into row values.
    let rows = exact_path_data_rows_from_prepared(
        &blob_reader,
        plugin_render,
        prepared,
        data_range.as_ref(),
    )
    .await?;
    let columns = if data_range.is_some() {
        vec![
            "path".to_string(),
            "content".to_string(),
            "total_size".to_string(),
            "range_start".to_string(),
            "range_end".to_string(),
            "content_identity".to_string(),
        ]
    } else {
        vec!["path".to_string(), "content".to_string()]
    };
    let column_types = if data_range.is_some() {
        vec![
            crate::ResultColumnType::Text,
            crate::ResultColumnType::Blob,
            crate::ResultColumnType::Integer,
            crate::ResultColumnType::Integer,
            crate::ResultColumnType::Integer,
            crate::ResultColumnType::Text,
        ]
    } else {
        vec![crate::ResultColumnType::Text, crate::ResultColumnType::Blob]
    };
    Ok(SqlQueryResult {
        columns,
        column_types,
        rows,
        notices: Vec::new(),
    })
}

/// Executes an exact active-branch manifest batch selected by file id without
/// constructing a DataFusion catalog or plan. This is the multi-row analogue
/// of [`execute_exact_lix_file_read`] for callers that need to verify durable
/// file identity, bytes, and metadata together.
pub(crate) async fn execute_exact_lix_file_id_manifest_batch_read(
    active_branch_id: &str,
    hot_state: Arc<dyn HotStateReader>,
    filesystem_path_index: Arc<dyn FilesystemPathIndexReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    blob_reader: Arc<dyn BlobDataReader>,
    plugin_host: PluginRuntimeHost,
    session_file_views: Option<SessionFileViews>,
    file_ids: &BTreeSet<String>,
) -> Result<SqlQueryResult, LixError> {
    let base_schema = lix_file_schema();
    let schema = Arc::new(Schema::new(
        ["id", "path", "content", "lixcol_metadata"]
            .into_iter()
            .map(|name| {
                base_schema
                    .field_with_name(name)
                    .expect("lix_file manifest column should exist")
                    .clone()
            })
            .collect::<Vec<_>>(),
    ));
    let mut request = lix_file_scan_request(Some(active_branch_id), Some(schema.as_ref()), None);
    let branch_binding = BranchBinding::active(active_branch_id);
    request.filter.branch_ids = resolve_provider_branch_ids(
        branch_ref.as_ref(),
        &branch_binding,
        request.filter.branch_ids,
    )
    .await?;

    let index = filesystem_path_index
        .path_index(
            &FilesystemPathIndexRequest::new(request.filter.branch_ids.clone())
                .with_blob_refs(true)
                .with_cached_blob_data(true),
        )
        .await?;
    let matches = indexed_file_id_matches(index, file_ids, &FilePathPredicate::All);
    let rows = scan_indexed_file_batch(&matches, true)?;
    let prepared = prepare_indexed_lix_file_rows(&matches, rows)?;
    let acknowledge_plugin_data = session_file_views.is_some();
    let plugin_render = if prepared.needs_plugin_render(true) || acknowledge_plugin_data {
        plugin_render_context_for_lix_file_scan(
            Arc::clone(&hot_state),
            &request,
            plugin_host,
            &prepared,
            acknowledge_plugin_data,
        )
        .await?
        .map(|context| context.with_session_file_views(session_file_views.clone()))
    } else {
        None
    };
    let batch =
        lix_file_record_batch_from_prepared(&schema, &blob_reader, plugin_render, true, prepared)
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

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn filter_pushdown(&self, filter: &Expr) -> TableProviderFilterPushDown {
        let analyzer = LixFileIdFilterAnalyzer;
        if analyzer.supports(filter)
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
        request.filter.branch_ids = resolve_provider_branch_ids(
            self.branch_ref.as_ref(),
            &self.branch_binding,
            request.filter.branch_ids,
        )
        .await
        .map_err(lix_error_to_datafusion_error)?;
        let needs_data = scan_needs_data(&self.schema, projection, &filters);
        let needs_required_blob_rows =
            scan_needs_required_blob_rows(&self.schema, projection, &filters);
        let needs_blob_rows = needs_required_blob_rows
            || scan_needs_content_updated_at(&self.schema, projection, &filters);
        let needs_file_timestamps = scan_needs_file_timestamps(&self.schema, projection, &filters);
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
        // Scans that need file content or the blob revision still load blob rows.
        // A path predicate or exact file/directory ids can narrow those loads
        // to matching cached descriptors instead of scanning complete state.
        let use_path_index = should_use_path_index(&indexed_path_predicate, needs_blob_rows)
            || needs_file_timestamps
            || matches!(&target_file_ids, FileIdConstraint::Ids(_))
            || matches!(&target_directory_ids, FileIdConstraint::Ids(_))
            || root_directory_filter;
        let indexed_matches = if !use_path_index {
            None
        } else {
            let index = self
                .filesystem_path_index
                .path_index(
                    &FilesystemPathIndexRequest::new(request.filter.branch_ids.clone())
                        .with_blob_refs(needs_blob_rows)
                        .with_cached_blob_data(needs_data),
                )
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
            source: scan_row_source(
                Arc::clone(&projected_schema),
                (
                    Arc::clone(&self.hot_state),
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
                    needs_required_blob_rows,
                    limit,
                ),
                |(
                    hot_state,
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
                    needs_required_blob_rows,
                    limit,
                )| async move {
                    if let Some(indexed_matches) = indexed_matches.as_ref()
                        && !needs_required_blob_rows
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
                        let rows = scan_indexed_file_batch(indexed_matches, needs_blob_rows)
                            .map_err(|error| {
                                DataFusionError::Execution(format!(
                                    "sql2 indexed lix_file scan failed: {error}"
                                ))
                            })?;
                        prepare_indexed_lix_file_rows(indexed_matches, rows)
                    } else {
                        let rows = scan_lix_file_live_batch(
                            Arc::clone(&hot_state),
                            &request,
                            &target_file_ids,
                        )
                        .await
                        .map_err(|error| {
                            DataFusionError::Context(
                                "sql2 lix_file scan failed".to_string(),
                                Box::new(lix_error_to_datafusion_error(error)),
                            )
                        })?;
                        prepare_lix_file_rows(rows, &path_predicate)
                    }
                    .map_err(|error| {
                        DataFusionError::Execution(format!(
                            "sql2 lix_file row preparation failed: {error}"
                        ))
                    })?;
                    let acknowledge_plugin_data = needs_data && session_file_views.is_some();
                    let plugin_render = if prepared.needs_plugin_render(needs_required_blob_rows)
                        || acknowledge_plugin_data
                    {
                        plugin_render_context_for_lix_file_scan(
                            Arc::clone(&hot_state),
                            &request,
                            plugin_host,
                            &prepared,
                            acknowledge_plugin_data,
                        )
                        .await
                        .map_err(|error| {
                            DataFusionError::Context(
                                "sql2 lix_file plugin discovery failed".to_string(),
                                Box::new(lix_error_to_datafusion_error(error)),
                            )
                        })?
                        .map(|context| context.with_session_file_views(session_file_views.clone()))
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
        let data_is_explicit = write_ctx.explicit_insert_columns().map_or_else(
            || {
                insert_intents.includes_column("content")
                    && !self.options.omitted_insert_columns.contains("content")
            },
            |columns| columns.contains("content"),
        );
        let include_data_writes =
            self.schema.field_with_name("content").is_ok() && data_is_explicit;
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

    async fn plan_insert_with_returning(
        &self,
        write_ctx: SqlWriteContext,
        input: &Arc<dyn ExecutionPlan>,
        returning: DmlReturning,
    ) -> Result<InsertApply> {
        let insert_intents = InsertColumnIntents::from_input(input);
        let data_is_explicit = write_ctx.explicit_insert_columns().map_or_else(
            || {
                insert_intents.includes_column("content")
                    && !self.options.omitted_insert_columns.contains("content")
            },
            |columns| columns.contains("content"),
        );
        let include_data_writes =
            self.schema.field_with_name("content").is_ok() && data_is_explicit;
        let spec = self.clone();
        Ok(Arc::new(move |batches| {
            let write_ctx = write_ctx.clone();
            let spec = spec.clone();
            let returning = returning.clone();
            async move {
                let row_capacity = batches
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>()
                    .saturating_mul(3);
                let mut staged = LixFileStagedBatch::with_row_capacity(row_capacity);
                let mut path_resolvers = None;
                let mut keys = Vec::new();
                for batch in batches {
                    let batch = spec.materialize_returning_insert_defaults(&batch)?;
                    for row_index in 0..batch.num_rows() {
                        keys.push(spec.returning_key_from_batch(&batch, row_index)?);
                    }
                    if path_resolvers.is_none() {
                        path_resolvers = Some(
                            directory_path_resolvers_from_hot_state(
                                Arc::new(WriteContextHotStateReader::new(write_ctx.clone())),
                                spec.branch_binding.active_branch_id(),
                            )
                            .await
                            .map_err(lix_error_to_datafusion_error)?,
                        );
                    }
                    if record_batch_has_non_null_column(&batch, "path")? {
                        staged
                            .extend(lix_file_insert_stage_from_batch_with_path_resolvers(
                                &batch,
                                spec.branch_binding.active_branch_id(),
                                lix_file_surface_name(&spec.branch_binding),
                                path_resolvers
                                    .as_mut()
                                    .expect("path resolver should be initialized"),
                                &mut || spec.functions.call_uuid_v7().to_string(),
                                include_data_writes,
                            )?)
                            .map_err(lix_error_to_datafusion_error)?;
                    } else {
                        staged
                            .extend(
                                lix_file_insert_stage_from_batch_with_id_generator_and_path_resolvers(
                                    &batch,
                                    spec.branch_binding.active_branch_id(),
                                    lix_file_surface_name(&spec.branch_binding),
                                    path_resolvers
                                        .as_mut()
                                        .expect("path resolver should be initialized"),
                                    &mut || spec.functions.call_uuid_v7().to_string(),
                                    include_data_writes,
                                )?,
                            )
                            .map_err(lix_error_to_datafusion_error)?;
                    }
                }

                let count = staged.count;
                if !staged.state_rows.is_empty() || !staged.file_content_writes.is_empty() {
                    let intent = if staged.file_content_writes.is_empty() {
                        TransactionWrite::Rows {
                            mode: TransactionWriteMode::Insert,
                            rows: staged.state_rows,
                        }
                    } else {
                        TransactionWrite::RowsWithFileContent {
                            mode: TransactionWriteMode::Insert,
                            rows: staged.state_rows,
                            file_content: staged.file_content_writes,
                            count,
                        }
                    };
                    write_ctx
                        .stage_write(intent)
                        .await
                        .map_err(lix_error_to_datafusion_error)?;
                }

                let post_image = spec
                    .returning_post_image(
                        &write_ctx,
                        &keys,
                        returning.required_columns().contains("content"),
                    )
                    .await?;
                returning.capture(returning.project(&post_image)?);
                Ok(count)
            }
            .boxed()
        }))
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
        let needs_data = filters
            .iter()
            .any(|filter| contains_column(filter, "content"))
            || options.returning_columns.contains("content");
        let target_file_ids = file_id_constraint_from_filters(filters)?;
        let mut request = lix_file_scan_request(self.branch_binding.active_branch_id(), None, None);
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
            LixFileDmlSourceOptions {
                needs_data,
                needs_plugin_ownership: false,
                capture_path_resolver_rows: false,
            },
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
        self.plan_update_with_post_image(write_ctx, assignments, filters, None)
            .await
    }

    async fn plan_update_with_returning(
        &self,
        write_ctx: SqlWriteContext,
        assignments: Vec<(String, Arc<dyn PhysicalExpr>)>,
        filters: &[Expr],
        returning: DmlReturning,
    ) -> Result<PlannedDml> {
        self.plan_update_with_post_image(write_ctx, assignments, filters, Some(returning))
            .await
    }
}

impl LixFileSpec {
    async fn plan_update_with_post_image(
        &self,
        write_ctx: SqlWriteContext,
        assignments: Vec<(String, Arc<dyn PhysicalExpr>)>,
        filters: &[Expr],
        returning: Option<DmlReturning>,
    ) -> Result<PlannedDml> {
        let needs_data = filters
            .iter()
            .any(|filter| contains_column(filter, "content"))
            || assignments.iter().any(|(column_name, expr)| {
                column_name == "path" || physical_expr_contains_column(expr, "content")
            })
            || returning
                .as_ref()
                .is_some_and(|returning| returning.required_columns().contains("content"));
        let target_file_ids = file_id_constraint_from_filters(filters)?;
        let mut request = lix_file_scan_request(self.branch_binding.active_branch_id(), None, None);
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
            LixFileDmlSourceOptions {
                needs_data,
                needs_plugin_ownership: update_columns.updates_path() && !update_columns.data,
                capture_path_resolver_rows,
            },
            Arc::clone(&captured),
        );
        let branch_binding = self.branch_binding.clone();
        let functions = self.functions.clone();
        let returning_spec = self.clone();
        let apply: DmlApply = Arc::new(move |matched_batch| {
            let write_ctx = write_ctx.clone();
            let branch_binding = branch_binding.clone();
            let functions = functions.clone();
            let assignments = assignments.clone();
            let captured = Arc::clone(&captured);
            let returning = returning.clone();
            let returning_spec = returning_spec.clone();
            async move {
                let keys = returning
                    .as_ref()
                    .map(|_| {
                        (0..matched_batch.num_rows())
                            .map(|row_index| {
                                returning_spec.returning_key_from_batch(&matched_batch, row_index)
                            })
                            .collect::<Result<Vec<_>>>()
                    })
                    .transpose()?;
                let LixFileDmlSourceState {
                    blob_ref_keys,
                    plugin_render,
                    path_resolvers: captured_path_resolvers,
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
                    } else if let Some(mut path_resolvers) = captured_path_resolvers {
                        if let Some(active_branch_id) = branch_binding.active_branch_id() {
                            let resolver_key =
                                filesystem_storage_scope_key(active_branch_id, false, false, None);
                            path_resolvers.entry(resolver_key).or_default();
                        }
                        path_resolvers
                    } else {
                        directory_path_resolvers_from_hot_state(
                            Arc::new(WriteContextHotStateReader::new(write_ctx.clone())),
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
                    let intent = if staged.file_content_writes.is_empty() {
                        TransactionWrite::Rows {
                            mode: TransactionWriteMode::Replace,
                            rows: staged.state_rows,
                        }
                    } else {
                        TransactionWrite::RowsWithFileContent {
                            mode: TransactionWriteMode::Replace,
                            rows: staged.state_rows,
                            file_content: staged.file_content_writes,
                            count,
                        }
                    };
                    write_ctx
                        .stage_write(intent)
                        .await
                        .map_err(lix_error_to_datafusion_error)?;
                }

                if let (Some(returning), Some(keys)) = (returning, keys) {
                    let post_image = returning_spec
                        .returning_post_image(
                            &write_ctx,
                            &keys,
                            returning.required_columns().contains("content"),
                        )
                        .await?;
                    returning.capture(returning.project(&post_image)?);
                }
                Ok(count)
            }
            .boxed()
        });
        Ok(PlannedDml { source, apply })
    }
}

/// Physical and path identities the upsert driver can match `lix_file` rows
/// on.
const LIX_FILE_IDENTITY: &[&str] = &["id"];
const LIX_FILE_PATH_IDENTITY: &[&str] = &["path"];

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

        let path_identity = LIX_FILE_PATH_IDENTITY;
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
        let include_data_writes = record_batch_has_non_null_column(batch, "content")?;

        let mut path_resolvers = directory_path_resolvers_from_hot_state(
            Arc::new(WriteContextHotStateReader::new(write_ctx.clone())),
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

        Ok(StagedUpsert::with_file_content(
            staged.state_rows,
            staged.file_content_writes,
        ))
    }

    fn validate_proposed_batch(&self, batch: &RecordBatch) -> Result<()> {
        for row_index in 0..batch.num_rows() {
            defaultable_text_insert_value(batch, row_index, "id", "INSERT into lix_file")?;
            defaultable_bool_insert_value(
                batch,
                row_index,
                "lixcol_global",
                "INSERT into lix_file",
            )?;
            defaultable_bool_insert_value(
                batch,
                row_index,
                "lixcol_untracked",
                "INSERT into lix_file",
            )?;
            if !insert_column_is_omitted(batch, "content") {
                insert_optional_binary_value(batch, row_index, "content")?;
            }
        }
        Ok(())
    }

    async fn materialize_excluded_defaults(
        &self,
        _write_ctx: &SqlWriteContext,
        proposed: &RecordBatch,
    ) -> Result<RecordBatch> {
        let materialized = if insert_column_is_omitted(proposed, "id") {
            let ids = (0..proposed.num_rows())
                .map(|_| Some(self.functions.call_uuid_v7().to_string()))
                .collect::<StringArray>();
            materialize_omitted_column(proposed, "id", Arc::new(ids))?
        } else {
            proposed.clone()
        };
        let materialized = materialize_omitted_column(
            &materialized,
            "content",
            Arc::new(LargeBinaryArray::from(vec![
                Some(&[][..]);
                proposed.num_rows()
            ])),
        )?;
        let materialized = materialize_omitted_column(
            &materialized,
            "lixcol_global",
            Arc::new(BooleanArray::from(vec![false; proposed.num_rows()])),
        )?;
        materialize_omitted_column(
            &materialized,
            "lixcol_untracked",
            Arc::new(BooleanArray::from(vec![false; proposed.num_rows()])),
        )
    }

    async fn materialize_returning_insert_defaults(
        &self,
        _write_ctx: &SqlWriteContext,
        proposed: &RecordBatch,
    ) -> Result<RecordBatch> {
        LixFileSpec::materialize_returning_insert_defaults(self, proposed)
    }

    async fn capture_upsert_returning(
        &self,
        write_ctx: &SqlWriteContext,
        affected_rows: Vec<UpsertReturningRow>,
        returning: DmlReturning,
    ) -> Result<()> {
        let keys = affected_rows
            .iter()
            .map(|row| self.returning_key_from_batch(row.batch(), row.row_index()))
            .collect::<Result<Vec<_>>>()?;
        let post_image = self
            .returning_post_image(
                write_ctx,
                &keys,
                returning.required_columns().contains("content"),
            )
            .await?;
        returning.capture(returning.project(&post_image)?);
        Ok(())
    }

    async fn scan_conflict_candidates(
        &self,
        write_ctx: &SqlWriteContext,
        proposed: &RecordBatch,
        target: &UpsertConflictTarget,
    ) -> Result<RecordBatch> {
        // Existing rows matching the proposed conflict identity, rendered as
        // a full `lix_file` batch (with materialized `content`) so the driver can
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
                .path_index(
                    &FilesystemPathIndexRequest::new(request.filter.branch_ids.clone())
                        .with_blob_refs(true),
                )
                .await
                .map_err(lix_error_to_datafusion_error)?;
            Some(indexed_file_matches(index, &path_predicate))
        } else {
            self.indexed_dml_matches(&request, &[], &target_file_ids)
                .await?
        };

        let hot_state: Arc<dyn HotStateReader> =
            Arc::new(WriteContextHotStateReader::new(write_ctx.clone()));
        let prepared = if let Some(indexed_matches) = indexed_matches.as_ref() {
            // Conflict probes only need the proposed exact IDs or paths. Use
            // the visible filesystem index for descriptor matching, then fetch
            // correlated blob refs solely for those files.
            let rows = match &target_file_ids {
                FileIdConstraint::Ids(file_ids) => {
                    scan_exact_file_blob_batch(hot_state.clone(), &request, file_ids).await
                }
                FileIdConstraint::All | FileIdConstraint::None => {
                    scan_indexed_file_batch(indexed_matches, true)
                }
            }
            .map_err(lix_error_to_datafusion_error)?;
            prepare_indexed_lix_file_rows(indexed_matches, rows)
        } else {
            let rows = scan_lix_file_live_batch(hot_state.clone(), &request, &target_file_ids)
                .await
                .map_err(lix_error_to_datafusion_error)?;
            prepare_lix_file_rows(rows, &FilePathPredicate::All)
        }
        .map_err(lix_error_to_datafusion_error)?;
        let plugin_render = if prepared.needs_plugin_render(true) {
            plugin_render_context_for_lix_file_scan(
                Arc::clone(&hot_state),
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
        request.filter.branch_ids = resolve_provider_branch_ids(
            self.branch_ref.as_ref(),
            &self.branch_binding,
            request.filter.branch_ids,
        )
        .await
        .map_err(lix_error_to_datafusion_error)?;

        let hot_state: Arc<dyn HotStateReader> =
            Arc::new(WriteContextHotStateReader::new(write_ctx.clone()));
        // The augmented conflict batch already carries the selected
        // descriptors. Recover only their correlated blob refs; rebuilding
        // the path index here would duplicate the conflict probe's topology
        // read, especially for path-based upserts.
        let rows = match &target_file_ids {
            FileIdConstraint::Ids(file_ids) => {
                scan_exact_file_blob_batch(hot_state.clone(), &request, file_ids).await
            }
            FileIdConstraint::All | FileIdConstraint::None => {
                scan_lix_file_live_batch(hot_state.clone(), &request, &target_file_ids).await
            }
        }
        .map_err(lix_error_to_datafusion_error)?;
        let blob_ref_keys =
            blob_ref_keys_from_live_rows(&rows).map_err(lix_error_to_datafusion_error)?;

        let plugin_rewrite_file_ids = if update_columns.updates_path() && !update_columns.data {
            let plugin_host = self.plugin_host.clone();
            let branches =
                load_plugin_render_branches(Arc::clone(&hot_state), &request, &plugin_host, None)
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
                    hot_state.clone(),
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
                directory_path_resolvers_from_hot_state(
                    Arc::new(WriteContextHotStateReader::new(write_ctx.clone())),
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

        Ok(StagedUpsert::with_file_content(
            staged.state_rows,
            staged.file_content_writes,
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
        let row_capacity = batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>()
            .saturating_mul(3);
        let mut staged = LixFileStagedBatch::with_row_capacity(row_capacity);
        let mut path_resolvers = None;
        for batch in batches {
            if path_resolvers.is_none() {
                path_resolvers = Some(
                    directory_path_resolvers_from_hot_state(
                        Arc::new(WriteContextHotStateReader::new(self.write_ctx.clone())),
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

        if !staged.state_rows.is_empty() || !staged.file_content_writes.is_empty() {
            let intent = if staged.file_content_writes.is_empty() {
                TransactionWrite::Rows {
                    mode: TransactionWriteMode::Insert,
                    rows: staged.state_rows,
                }
            } else {
                TransactionWrite::RowsWithFileContent {
                    mode: TransactionWriteMode::Insert,
                    rows: staged.state_rows,
                    file_content: staged.file_content_writes,
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
    let _ = branch_binding;
    "lix_file"
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HotStateRowHandle {
    batch: u32,
    row: u32,
}

#[derive(Debug, Default)]
struct HotStateBatchOwners {
    batches: Vec<MaterializedHotStateBatch>,
}

impl HotStateBatchOwners {
    fn push(&mut self, batch: MaterializedHotStateBatch) -> u32 {
        let ordinal =
            u32::try_from(self.batches.len()).expect("lix_file live batch count exceeds u32");
        self.batches.push(batch);
        ordinal
    }

    fn row(&self, handle: HotStateRowHandle) -> MaterializedHotStateRowRef<'_> {
        self.batches[handle.batch as usize].row(handle.row as usize)
    }

    fn batch(&self, ordinal: u32) -> &MaterializedHotStateBatch {
        &self.batches[ordinal as usize]
    }
}

fn hot_state_row_handle(batch: u32, row: usize) -> HotStateRowHandle {
    HotStateRowHandle {
        batch,
        row: u32::try_from(row).expect("lix_file live batch row count exceeds u32"),
    }
}

#[derive(Debug, Clone)]
struct FileDescriptorRecord {
    id: String,
    directory_id: Option<String>,
    name: String,
    key: FilesystemDescriptorKey,
    live: HotStateRowHandle,
}

impl FileDescriptorRecord {
    fn row_context(&self, owners: &HotStateBatchOwners) -> FilesystemRowContext {
        let live = owners.row(self.live);
        FilesystemRowContext {
            branch_id: live.branch_id().to_owned(),
            global: live.global(),
            untracked: live.untracked(),
            file_id: live.file_id().map(str::to_owned),
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

    fn blob_ref_key(&self, owners: &HotStateBatchOwners) -> FilesystemBlobRefKey {
        FilesystemBlobRefKey::from_context(&self.row_context(owners), &self.id)
    }
}

#[derive(Clone)]
struct PluginRenderContext {
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
    inline_data: Option<Vec<u8>>,
    live: HotStateRowHandle,
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

fn typed_row_string(
    row: &lix_schema::Row,
    schema_key: &str,
    field: &str,
) -> Result<String, LixError> {
    match row.get(field) {
        Some(lix_schema::Value::Text(value)) => Ok(value.clone()),
        Some(lix_schema::Value::Uuid(value)) => Ok(value.to_string()),
        _ => Err(LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!("{schema_key} typed payload field '{field}' must be a string"),
        )),
    }
}

fn optional_typed_row_string(
    row: &lix_schema::Row,
    schema_key: &str,
    field: &str,
) -> Result<Option<String>, LixError> {
    match row.get(field) {
        None | Some(lix_schema::Value::Null) => Ok(None),
        Some(lix_schema::Value::Text(value)) => Ok(Some(value.clone())),
        Some(lix_schema::Value::Uuid(value)) => Ok(Some(value.to_string())),
        _ => Err(LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!("{schema_key} typed payload field '{field}' must be a string or null"),
        )),
    }
}

fn blob_ref_snapshot_from_live_row(
    row: MaterializedHotStateRowRef<'_>,
) -> Result<Option<BlobRefSnapshot>, LixError> {
    if let Some(typed) = row.decoded_snapshot() {
        return Ok(Some(BlobRefSnapshot {
            id: typed_row_string(&typed.row, BLOB_REF_SCHEMA_KEY, "id")?,
            blob_hash: typed_row_string(&typed.row, BLOB_REF_SCHEMA_KEY, "blob_hash")?,
        }));
    }
    row.snapshot_json_value()?
        .map(|snapshot| {
            serde_json::from_value(snapshot).map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("invalid lix_binary_blob_ref snapshot: {error}"),
                )
            })
        })
        .transpose()
}

fn blob_ref_record_from_live_row(
    row: MaterializedHotStateRowRef<'_>,
    handle: HotStateRowHandle,
) -> Result<Option<(FilesystemBlobRefKey, BlobRefRecord)>, LixError> {
    if row.schema_key() != BLOB_REF_SCHEMA_KEY {
        return Ok(None);
    }
    let Some(snapshot) = blob_ref_snapshot_from_live_row(row)? else {
        return Ok(None);
    };
    let key = FilesystemBlobRefKey::from_live_row_ref(row, snapshot.id);
    Ok(Some((
        key,
        BlobRefRecord {
            blob_hash: snapshot.blob_hash,
            inline_data: None,
            live: handle,
        },
    )))
}

#[derive(Debug, Deserialize)]
struct DirectoryDescriptorSnapshot {
    id: String,
    parent_id: Option<String>,
    name: String,
}

fn file_descriptor_snapshot_from_live_row(
    row: MaterializedHotStateRowRef<'_>,
) -> Result<Option<FileDescriptorSnapshot>, LixError> {
    if let Some(typed) = row.decoded_snapshot() {
        return Ok(Some(FileDescriptorSnapshot {
            id: typed_row_string(&typed.row, FILE_DESCRIPTOR_SCHEMA_KEY, "id")?,
            directory_id: optional_typed_row_string(
                &typed.row,
                FILE_DESCRIPTOR_SCHEMA_KEY,
                "directory_id",
            )?,
            name: typed_row_string(&typed.row, FILE_DESCRIPTOR_SCHEMA_KEY, "name")?,
        }));
    }
    row.snapshot_json_value()?
        .map(|snapshot| {
            serde_json::from_value(snapshot).map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("invalid lix_file_descriptor snapshot: {error}"),
                )
            })
        })
        .transpose()
}

fn directory_descriptor_snapshot_from_live_row(
    row: MaterializedHotStateRowRef<'_>,
) -> Result<Option<DirectoryDescriptorSnapshot>, LixError> {
    if let Some(typed) = row.decoded_snapshot() {
        return Ok(Some(DirectoryDescriptorSnapshot {
            id: typed_row_string(&typed.row, DIRECTORY_DESCRIPTOR_SCHEMA_KEY, "id")?,
            parent_id: optional_typed_row_string(
                &typed.row,
                DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
                "parent_id",
            )?,
            name: typed_row_string(&typed.row, DIRECTORY_DESCRIPTOR_SCHEMA_KEY, "name")?,
        }));
    }
    row.snapshot_json_value()?
        .map(|snapshot| {
            serde_json::from_value(snapshot).map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("invalid lix_directory_descriptor snapshot: {error}"),
                )
            })
        })
        .transpose()
}

#[derive(Debug, Default)]
struct LixFileStagedBatch {
    state_rows: RawWriteBatch,
    file_content_writes: Vec<TransactionFileContent>,
    count: u64,
}

impl LixFileStagedBatch {
    fn with_row_capacity(row_capacity: usize) -> Self {
        Self {
            state_rows: RawWriteBatch::with_capacity(row_capacity),
            file_content_writes: Vec::with_capacity(row_capacity),
            count: 0,
        }
    }

    fn extend(&mut self, other: Self) -> std::result::Result<(), LixError> {
        self.state_rows.append(other.state_rows);
        self.file_content_writes.extend(other.file_content_writes);
        self.add_count(other.count)
    }

    fn extend_filesystem_plan(
        &mut self,
        plan: crate::filesystem::FilesystemWritePlan,
    ) -> std::result::Result<(), LixError> {
        self.state_rows.append(plan.rows);
        self.file_content_writes.extend(plan.file_content);
        self.add_count(plan.count)
    }

    fn extend_filesystem_delete_plan(
        &mut self,
        plan: FilesystemDeletePlan,
    ) -> std::result::Result<(), LixError> {
        self.state_rows.append(plan.rows);
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
    UpdateContent,
    UpdateContentAndMetadata,
    IdDoNothing,
    IdUpdateContent,
    IdUpdateContentAndMetadata,
}

impl FastLixFilePathWriteConflict {
    fn targets_id(self) -> bool {
        matches!(
            self,
            Self::IdDoNothing | Self::IdUpdateContent | Self::IdUpdateContentAndMetadata
        )
    }

    fn updates_existing(self) -> bool {
        matches!(
            self,
            Self::UpdateContent
                | Self::UpdateContentAndMetadata
                | Self::IdUpdateContent
                | Self::IdUpdateContentAndMetadata
        )
    }

    pub(crate) fn updates_metadata(self) -> bool {
        matches!(
            self,
            Self::UpdateContentAndMetadata | Self::IdUpdateContentAndMetadata
        )
    }

    pub(crate) fn updates_data_only(self) -> bool {
        matches!(self, Self::UpdateContent | Self::IdUpdateContent)
    }
}

pub(crate) async fn execute_fast_lix_file_path_writes(
    ctx: &mut dyn SqlWriteExecutionContext,
    writes: Vec<(
        String,
        crate::Blob,
        Option<TransactionJson>,
        Option<RequestBlobSpliceProvenance>,
    )>,
    conflict: FastLixFilePathWriteConflict,
    mutation_identity: Option<MutationIdentity>,
) -> Result<Option<u64>, LixError> {
    execute_fast_lix_file_id_path_writes_inner(
        ctx,
        writes
            .into_iter()
            .map(|(path, data, metadata, splice)| {
                (None, path, FileContent::inline(data), metadata, splice)
            })
            .collect(),
        conflict,
        mutation_identity,
    )
    .await
}

pub(crate) async fn execute_fast_lix_file_id_path_writes(
    ctx: &mut dyn SqlWriteExecutionContext,
    writes: Vec<(
        Option<String>,
        String,
        crate::Blob,
        Option<TransactionJson>,
        Option<RequestBlobSpliceProvenance>,
    )>,
    conflict: FastLixFilePathWriteConflict,
    mutation_identity: Option<MutationIdentity>,
) -> Result<Option<u64>, LixError> {
    execute_fast_lix_file_id_path_writes_inner(
        ctx,
        writes
            .into_iter()
            .map(|(id, path, data, metadata, splice)| {
                (id, path, FileContent::inline(data), metadata, splice)
            })
            .collect(),
        conflict,
        mutation_identity,
    )
    .await
}

pub(crate) async fn execute_fast_lix_file_prepared_path_write(
    ctx: &mut dyn SqlWriteExecutionContext,
    path: String,
    receipt: crate::binary_cas::BlobWriteReceipt,
) -> Result<Option<u64>, LixError> {
    execute_fast_lix_file_id_path_writes_inner(
        ctx,
        vec![(None, path, FileContent::PreparedCas(receipt), None, None)],
        FastLixFilePathWriteConflict::UpdateContent,
        None,
    )
    .await
}

async fn execute_fast_lix_file_id_path_writes_inner(
    ctx: &mut dyn SqlWriteExecutionContext,
    writes: Vec<(
        Option<String>,
        String,
        FileContent,
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

    // Boxed: this function keeps the whole-branch fallback below live in the
    // same state machine, so inlining the indexed route's futures here makes
    // one `lix_file` write frame large enough to overflow libtest's 2 MiB
    // worker stack in a debug build.
    let indexed = if conflict.targets_id() {
        Box::pin(indexed_file_id_writes(
            ctx,
            &active_branch_id,
            &parsed_writes,
        ))
        .await?
    } else if conflict.updates_existing() || conflict == FastLixFilePathWriteConflict::None {
        // A plain INSERT is a create; it needs the same path lookup an upsert
        // does and nothing more. Routing it through the path index removes the
        // whole-branch descriptor scan the fallback below performs.
        Box::pin(indexed_file_path_writes(
            ctx,
            &active_branch_id,
            &parsed_writes,
            conflict,
        ))
        .await?
    } else {
        None
    };
    if let Some(indexed) = indexed {
        return Box::pin(stage_indexed_file_path_writes(
            ctx,
            &active_branch_id,
            parsed_writes,
            indexed,
            conflict,
            mutation_identity,
        ))
        .await
        .map(Some);
    }

    // Extracted and boxed so the whole-branch fallback's locals are not part of
    // this function's poll frame. At `opt-level = 0` a single async fn keeps a
    // stack slot for every local across every branch, and holding both routes
    // in one frame overflows libtest's 2 MiB worker stack.
    Box::pin(stage_scanning_file_path_writes(
        ctx,
        &active_branch_id,
        parsed_writes,
        conflict,
        mutation_identity,
    ))
    .await
}

/// The canonical fallback: rebuild the branch's filesystem view from a
/// whole-branch descriptor scan. Taken only when the path index cannot answer
/// the write (occupied path on a plain INSERT, ambiguous multi-scope entry,
/// constraint violation while seeding resolvers).
async fn stage_scanning_file_path_writes(
    ctx: &mut dyn SqlWriteExecutionContext,
    active_branch_id: &str,
    parsed_writes: Vec<FastLixFilePathWrite>,
    conflict: FastLixFilePathWriteConflict,
    mutation_identity: Option<MutationIdentity>,
) -> Result<Option<u64>, LixError> {
    let live_rows = ctx
        .scan_hot_state_batch(&HotStateScanRequest {
            filter: HotStateFilter {
                schema_keys: filesystem_schema_keys(),
                branch_ids: vec![active_branch_id.to_string()],
                include_tombstones: false,
                ..HotStateFilter::default()
            },
            ..HotStateScanRequest::default()
        })
        .await?;
    let filesystem = match FilesystemIndex::from_live_batch(&live_rows) {
        Ok(filesystem) => filesystem,
        // The legacy write index intentionally rejects visible path collisions
        // across storage scopes, while the general provider can disambiguate
        // them. No writes have been staged yet, so decline the fast route and
        // let the caller execute the original DataFusion plan.
        Err(error) if error.code == LixError::CODE_CONSTRAINT_VIOLATION => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut path_resolvers = directory_path_resolvers_from_state_batch(&live_rows)?;
    let resolver_key = filesystem_storage_scope_key(active_branch_id, false, false, None);
    path_resolvers.entry(resolver_key).or_default();
    let mut staged = LixFileStagedBatch::with_row_capacity(parsed_writes.len().saturating_mul(3));

    for write in parsed_writes {
        let content = write.data.clone();
        if let Some(existing) = filesystem.file_entry(&write.parsed.path).cloned() {
            let base_blob_hash = existing
                .blob_hash
                .as_deref()
                .and_then(|hash| BlobId::from_hex(hash).ok());
            if conflict != FastLixFilePathWriteConflict::None {
                validate_fast_lix_file_path_conflict_pair(
                    existing.scope.untracked,
                    &write.parsed.path,
                )?;
            }
            match conflict {
                FastLixFilePathWriteConflict::None => {
                    let file_id = fast_file_write_id(&write, ctx);
                    let context = FilesystemRowContext {
                        branch_id: active_branch_id.to_string(),
                        global: false,
                        untracked: false,
                        file_id: None,
                        metadata: write.metadata,
                    };
                    let plan = plan_parsed_file_path_write_with_resolvers(
                        &mut path_resolvers,
                        write.parsed.parsed_path,
                        Some(file_id),
                        Some(content.clone()),
                        context,
                        &mut || ctx.functions().call_uuid_v7().to_string(),
                    )?;
                    staged.extend_filesystem_plan(plan)?;
                }
                FastLixFilePathWriteConflict::DoNothing
                | FastLixFilePathWriteConflict::IdDoNothing => {}
                FastLixFilePathWriteConflict::UpdateContent
                | FastLixFilePathWriteConflict::IdUpdateContent => {
                    let mut context = existing.scope.context(Some(existing.id.clone()));
                    if context.global {
                        context.branch_id = GLOBAL_BRANCH_ID.to_string();
                    }
                    let file_content_start = staged.file_content_writes.len();
                    stage_lix_file_content_update_write(
                        &mut staged,
                        existing.id.clone(),
                        Some(write.parsed.path),
                        Some(existing.name.clone()),
                        content.clone(),
                        context,
                        existing.blob_hash.is_some(),
                        base_blob_hash,
                        None,
                    )
                    .map_err(crate::sql2::error::datafusion_error_to_lix_error)?;
                    attach_fast_file_write_metadata(
                        &mut staged.file_content_writes[file_content_start..],
                        write.splice_provenance,
                        mutation_identity,
                    );
                    staged.add_count(1)?;
                }
                FastLixFilePathWriteConflict::UpdateContentAndMetadata
                | FastLixFilePathWriteConflict::IdUpdateContentAndMetadata => {
                    let mut context = existing.scope.context(None);
                    if context.global {
                        context.branch_id = GLOBAL_BRANCH_ID.to_string();
                    }
                    context.metadata = write.metadata;
                    FileDescriptorWriteIntent {
                        id: Some(existing.id.clone()),
                        directory_id: existing.directory_id.clone(),
                        name: existing.name.clone(),
                        context: context.clone(),
                    }
                    .append_to(&mut staged.state_rows);
                    let file_content_start = staged.file_content_writes.len();
                    stage_lix_file_content_update_write(
                        &mut staged,
                        existing.id.clone(),
                        Some(write.parsed.path),
                        Some(existing.name.clone()),
                        content.clone(),
                        context,
                        existing.blob_hash.is_some(),
                        base_blob_hash,
                        None,
                    )
                    .map_err(crate::sql2::error::datafusion_error_to_lix_error)?;
                    attach_fast_file_write_metadata(
                        &mut staged.file_content_writes[file_content_start..],
                        write.splice_provenance,
                        mutation_identity,
                    );
                    staged.add_count(1)?;
                }
            }
        } else {
            let file_id = fast_file_write_id(&write, ctx);
            let context = FilesystemRowContext {
                branch_id: active_branch_id.to_string(),
                global: false,
                untracked: false,
                file_id: None,
                metadata: write.metadata,
            };
            let mut plan = plan_parsed_file_path_write_with_resolvers(
                &mut path_resolvers,
                write.parsed.parsed_path,
                Some(file_id.clone()),
                Some(content),
                context,
                &mut || ctx.functions().call_uuid_v7().to_string(),
            )?;
            attach_fast_file_write_metadata(
                &mut plan.file_content,
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
        | FastLixFilePathWriteConflict::UpdateContent
        | FastLixFilePathWriteConflict::UpdateContentAndMetadata
        | FastLixFilePathWriteConflict::IdDoNothing
        | FastLixFilePathWriteConflict::IdUpdateContent
        | FastLixFilePathWriteConflict::IdUpdateContentAndMetadata => TransactionWriteMode::Replace,
    };
    Box::pin(stage_lix_file_fast_batch(ctx, mode, staged))
        .await
        .map(Some)
}

struct IndexedFilePathWrites {
    existing: Vec<Option<Arc<FilesystemPathEntry>>>,
    path_resolvers: Option<BTreeMap<String, DirectoryPathResolver>>,
}

async fn indexed_file_path_writes(
    ctx: &mut dyn SqlWriteExecutionContext,
    active_branch_id: &str,
    writes: &[FastLixFilePathWrite],
    conflict: FastLixFilePathWriteConflict,
) -> Result<Option<IndexedFilePathWrites>, LixError> {
    let index = Box::pin(
        ctx.filesystem_path_index(&FilesystemPathIndexRequest::new(vec![
            active_branch_id.to_string(),
        ])),
    )
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
                // A plain INSERT onto an occupied path must raise the unique
                // violation the whole-branch route already produces. Decline
                // rather than duplicate that error construction here.
                if conflict == FastLixFilePathWriteConflict::None {
                    return Ok(None);
                }
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
        let missing_paths = writes
            .iter()
            .zip(&existing)
            .filter(|(_, entry)| entry.is_none())
            .map(|(write, _)| &write.parsed.parsed_path);
        match directory_path_resolvers_for_paths(&index, missing_paths, Some(active_branch_id)) {
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

async fn indexed_file_id_writes(
    ctx: &mut dyn SqlWriteExecutionContext,
    active_branch_id: &str,
    writes: &[FastLixFilePathWrite],
) -> Result<Option<IndexedFilePathWrites>, LixError> {
    let index = Box::pin(
        ctx.filesystem_path_index(&FilesystemPathIndexRequest::new(vec![
            active_branch_id.to_string(),
        ])),
    )
    .await?;
    let mut unique_ids = BTreeSet::new();
    let mut existing = Vec::with_capacity(writes.len());
    for write in writes {
        let Some(id) = write.id.as_deref() else {
            return Ok(None);
        };
        if !unique_ids.insert(id) {
            return Ok(None);
        }
        match index.exact_file_id_entries(id).as_slice() {
            [] => existing.push(None),
            [entry] if entry.kind == FilesystemPathKind::File => {
                existing.push(Some(Arc::clone(entry)));
            }
            _ => return Ok(None),
        }
    }
    let has_missing = existing.iter().any(Option::is_none);
    // An ID miss says nothing about path availability. Seed the resolver with
    // the proposed paths' own leaves even at root level, so a different file
    // already at a proposed path still raises the normal uniqueness error.
    let path_resolvers = if has_missing {
        let missing_paths = writes
            .iter()
            .zip(&existing)
            .filter(|(_, entry)| entry.is_none())
            .map(|(write, _)| &write.parsed.parsed_path);
        match directory_path_resolvers_for_paths(&index, missing_paths, Some(active_branch_id)) {
            Ok(resolvers) => Some(resolvers),
            Err(error) if error.code == LixError::CODE_CONSTRAINT_VIOLATION => return Ok(None),
            Err(error) => return Err(error),
        }
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
    debug_assert!(
        conflict.updates_existing()
            || conflict == FastLixFilePathWriteConflict::IdDoNothing
            || conflict == FastLixFilePathWriteConflict::None
    );
    // `indexed_file_path_writes` declines every plain INSERT that would collide,
    // so a `None` conflict reaching here is a pure create and stages as one.
    debug_assert!(
        conflict != FastLixFilePathWriteConflict::None
            || indexed.existing.iter().all(Option::is_none)
    );
    let write_mode = if conflict == FastLixFilePathWriteConflict::None {
        TransactionWriteMode::Insert
    } else {
        TransactionWriteMode::Replace
    };
    for (write, entry) in writes.iter().zip(&indexed.existing) {
        if !conflict.targets_id()
            && let Some(entry) = entry
        {
            validate_fast_lix_file_path_conflict_pair(
                entry.key.is_untracked(),
                &write.parsed.path,
            )?;
        }
    }
    let existing = if conflict.updates_existing() {
        indexed
            .existing
            .iter()
            .filter_map(|entry| entry.as_ref().map(Arc::clone))
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let existing_materializations =
        Box::pin(load_exact_existing_materializations(ctx, &existing)).await?;
    let mut staged = LixFileStagedBatch::with_row_capacity(writes.len().saturating_mul(3));

    for (write, entry) in writes.into_iter().zip(indexed.existing) {
        let content = write.data.clone();
        if let Some(entry) = entry {
            if conflict == FastLixFilePathWriteConflict::IdDoNothing {
                continue;
            }
            let materialization = existing_materializations
                .get(&entry.key)
                .copied()
                .unwrap_or_default();
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
                FastLixFilePathWriteConflict::UpdateContent
                | FastLixFilePathWriteConflict::IdUpdateContent => {
                    context.file_id = Some(entry.id().to_string());
                }
                FastLixFilePathWriteConflict::UpdateContentAndMetadata
                | FastLixFilePathWriteConflict::IdUpdateContentAndMetadata => {
                    let metadata_changed = entry.metadata()
                        != write.metadata.as_ref().map(TransactionJson::normalized);
                    context.metadata = write.metadata;
                    if metadata_changed {
                        FileDescriptorWriteIntent {
                            id: Some(entry.id().to_string()),
                            directory_id: entry.parent_id.clone(),
                            name: entry.name.clone(),
                            context: context.clone(),
                        }
                        .append_to(&mut staged.state_rows);
                    }
                }
                FastLixFilePathWriteConflict::None
                | FastLixFilePathWriteConflict::DoNothing
                | FastLixFilePathWriteConflict::IdDoNothing => {
                    unreachable!("indexed path route only handles conflict updates")
                }
            }
            let update_path = if conflict.targets_id() {
                entry.path.clone()
            } else {
                write.parsed.path
            };
            let file_content_start = staged.file_content_writes.len();
            stage_lix_file_content_update_write(
                &mut staged,
                entry.id().to_string(),
                Some(update_path),
                Some(entry.name.clone()),
                content,
                context,
                materialization.has_blob_ref,
                materialization.blob_hash,
                None,
            )
            .map_err(crate::sql2::error::datafusion_error_to_lix_error)?;
            attach_fast_file_write_metadata(
                &mut staged.file_content_writes[file_content_start..],
                write.splice_provenance,
                mutation_identity,
            );
            staged.add_count(1)?;
        } else {
            let file_id = fast_file_write_id(&write, ctx);
            let context = FilesystemRowContext {
                branch_id: active_branch_id.to_string(),
                global: false,
                untracked: false,
                file_id: None,
                metadata: write.metadata,
            };
            let mut plan = plan_parsed_file_path_write_with_resolvers(
                indexed
                    .path_resolvers
                    .as_mut()
                    .expect("missing indexed path should have directory resolvers"),
                write.parsed.parsed_path,
                Some(file_id.clone()),
                Some(content),
                context,
                &mut || ctx.functions().call_uuid_v7().to_string(),
            )?;
            attach_fast_file_write_metadata(
                &mut plan.file_content,
                write.splice_provenance,
                mutation_identity,
            );
            attach_lix_file_insert_origin(&mut plan.rows, "lix_file", &file_id);
            staged.extend_filesystem_plan(plan)?;
        }
    }

    Box::pin(stage_lix_file_fast_batch(ctx, write_mode, staged)).await
}

#[derive(Debug, Clone, Copy, Default)]
struct ExistingFileMaterialization {
    has_blob_ref: bool,
    blob_hash: Option<BlobId>,
}

async fn load_exact_existing_materializations(
    ctx: &mut dyn SqlWriteExecutionContext,
    entries: &[Arc<FilesystemPathEntry>],
) -> Result<BTreeMap<FilesystemDescriptorKey, ExistingFileMaterialization>, LixError> {
    if entries.is_empty() {
        return Ok(BTreeMap::new());
    }
    let unique = entries
        .iter()
        .map(|entry| (entry.key.clone(), Arc::clone(entry)))
        .collect::<BTreeMap<_, _>>();
    let blob_requests = unique
        .iter()
        .map(|(key, entry)| {
            Ok((
                key.clone(),
                HotStateExactRowRequest {
                    branch_id: entry.key.branch_id().to_string(),
                    schema_key: BLOB_REF_SCHEMA_KEY.to_string(),
                    row_pk: file_id_row_pk(entry.id())?,
                    file_id: Some(entry.id().to_string()),
                },
            ))
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let request = HotStateExactBatchRequest {
        rows: blob_requests
            .iter()
            .map(|(_, request)| request.clone())
            .collect(),
        projection: HotStateProjection::default(),
        untracked: Some(false),
        include_tombstones: false,
    };
    let rows = ctx.load_exact_hot_state_batch(&request).await?;
    let mut materializations =
        BTreeMap::<FilesystemDescriptorKey, ExistingFileMaterialization>::new();
    for (row_index, (key, request)) in blob_requests.into_iter().enumerate() {
        let Some(row) = rows.row(row_index) else {
            continue;
        };
        let tracking_mismatch = row.untracked() ^ key.is_untracked();
        if row.branch_id() != key.branch_id() || row.global() != key.global() || tracking_mismatch {
            continue;
        }
        let materialization = materializations.entry(key).or_default();
        // A malformed old snapshot still proves that a blob row must be
        // replaced; only the optional CAS-reuse shortcut is lost.
        materialization.has_blob_ref = true;
        materialization.blob_hash = blob_ref_snapshot_from_live_row(row)?
            .filter(|snapshot| snapshot.id == request.file_id.as_deref().unwrap_or_default())
            .and_then(|snapshot| BlobId::from_hex(&snapshot.blob_hash).ok());
    }

    Ok(materializations)
}

pub(crate) async fn execute_fast_lix_file_content_update_by_id(
    ctx: &mut dyn SqlWriteExecutionContext,
    file_id: Option<String>,
    data: crate::Blob,
    splice_provenance: Option<RequestBlobSpliceProvenance>,
    mutation_identity: Option<MutationIdentity>,
) -> Result<u64, LixError> {
    execute_fast_lix_file_content_update_by_id_impl(
        ctx,
        file_id,
        data,
        None,
        splice_provenance,
        mutation_identity,
    )
    .await
}

pub(crate) async fn execute_fast_lix_file_content_update_by_id_with_metadata(
    ctx: &mut dyn SqlWriteExecutionContext,
    file_id: Option<String>,
    data: crate::Blob,
    metadata: Option<TransactionJson>,
    splice_provenance: Option<RequestBlobSpliceProvenance>,
    mutation_identity: Option<MutationIdentity>,
) -> Result<u64, LixError> {
    execute_fast_lix_file_content_update_by_id_impl(
        ctx,
        file_id,
        data,
        Some(metadata),
        splice_provenance,
        mutation_identity,
    )
    .await
}

async fn execute_fast_lix_file_content_update_by_id_impl(
    ctx: &mut dyn SqlWriteExecutionContext,
    file_id: Option<String>,
    data: crate::Blob,
    metadata_update: Option<Option<TransactionJson>>,
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
    // without a path-index revision.
    let mut blob_request = lix_file_scan_request(Some(&active_branch_id), None, None);
    blob_request.filter.schema_keys = vec![BLOB_REF_SCHEMA_KEY.to_string()];
    blob_request.filter.row_pks = vec![file_id_row_pk(&file_id)?];
    // Pin the file id too. The hot index key is file-first, so a row PK on
    // its own cannot form a point key: `hot_scan_entries` refuses the MultiGet
    // route for any schema with file-backed members -- a null-file point key
    // would miss the real row -- and `hot_file_scan_prefixes` requires a file
    // id. Without this the request fell through to walking every
    // `lix_binary_blob_ref` row in the branch and filtering in memory, which is
    // O(files) *per statement* and cost ~0.20 us per file in the branch.
    //
    // The pin cannot reject the row it is looking for, because for this schema
    // the file id and the row PK are the same value by construction: both
    // producers in `filesystem::planner` (`BlobRefRowInput::append_to` and
    // `append_blob_ref_tombstone_row`, tombstones included) set the row's
    // row PK and its `file_id` from one variable, and `lix_binary_blob_ref`
    // is a read-only public surface, so no caller can supply a divergent pair.
    // The exact-batch route below already pairs them the same way.
    blob_request.filter.file_ids = vec![crate::NullableKeyFilter::Value(file_id.clone())];
    let rows = ctx.scan_hot_state_batch(&blob_request).await?;

    let prepared = prepare_indexed_lix_file_rows(&indexed_matches, rows)?;

    let PreparedLixFileRows {
        live_rows,
        file_rows,
        blob_rows,
        file_paths,
        ..
    } = prepared;
    let existing = file_rows
        .into_iter()
        .filter(|(_, file)| file.id == file_id)
        .map(|(key, file)| {
            let blob_ref_key = file.blob_ref_key(&live_rows);
            let path = file_paths
                .get(&key)
                .cloned()
                .expect("prepared lix_file descriptor should have a path");
            let base_blob_hash = blob_rows
                .get(&blob_ref_key)
                .and_then(|row| BlobId::from_hex(&row.blob_hash).ok());
            let has_blob_ref = blob_rows.contains_key(&blob_ref_key);
            (path, file, has_blob_ref, base_blob_hash)
        })
        .collect::<Vec<_>>();
    if existing.is_empty() {
        return Ok(0);
    }

    let mut staged = LixFileStagedBatch::with_row_capacity(existing.len().saturating_mul(3));
    for (path, existing, has_blob_ref, base_blob_hash) in existing {
        parse_file_upsert_path(&path, TransactionWriteOperation::Update)
            .map_err(crate::sql2::error::datafusion_error_to_lix_error)?;
        let mut context = existing.row_context(&live_rows);
        if context.global {
            context.branch_id = GLOBAL_BRANCH_ID.to_string();
        }
        if let Some(metadata) = &metadata_update {
            context.metadata.clone_from(metadata);
            FileDescriptorWriteIntent {
                id: Some(existing.id.clone()),
                directory_id: existing.directory_id.clone(),
                name: existing.name.clone(),
                context: context.clone(),
            }
            .append_to(&mut staged.state_rows);
        }
        stage_lix_file_content_update_write(
            &mut staged,
            existing.id,
            Some(path),
            Some(existing.name),
            data.clone(),
            context,
            has_blob_ref,
            base_blob_hash,
            None,
        )
        .map_err(crate::sql2::error::datafusion_error_to_lix_error)?;
        if let Some(file_content) = staged.file_content_writes.last_mut() {
            file_content.set_splice_provenance(splice_provenance.clone());
            file_content.set_mutation_identity(mutation_identity);
        }
        staged.add_count(1)?;
    }
    stage_lix_file_fast_batch(ctx, TransactionWriteMode::Replace, staged).await
}

struct FastLixFilePathWrite {
    id: Option<String>,
    parsed: ParsedFileWritePath,
    data: FileContent,
    metadata: Option<TransactionJson>,
    splice_provenance: Option<RequestBlobSpliceProvenance>,
}

fn parse_fast_lix_file_path_writes(
    writes: Vec<(
        Option<String>,
        String,
        FileContent,
        Option<TransactionJson>,
        Option<RequestBlobSpliceProvenance>,
    )>,
) -> std::result::Result<Vec<FastLixFilePathWrite>, LixError> {
    writes
        .into_iter()
        .map(|(id, path, data, metadata, splice_provenance)| {
            let parsed = parse_file_upsert_path(&path, TransactionWriteOperation::Insert)
                .map_err(crate::sql2::error::datafusion_error_to_lix_error)?;
            if let (Some(id), Some(plugin_key)) = (id.as_deref(), parsed.plugin_key.as_deref())
                && id != plugin_storage_archive_file_id(plugin_key)
            {
                return Err(LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    "plugin archive file id must match its reserved storage identity",
                ));
            }
            Ok(FastLixFilePathWrite {
                id,
                parsed,
                data,
                metadata,
                splice_provenance,
            })
        })
        .collect()
}

fn fast_file_write_id(write: &FastLixFilePathWrite, ctx: &dyn SqlWriteExecutionContext) -> String {
    write
        .id
        .clone()
        .or_else(|| {
            write
                .parsed
                .plugin_key
                .as_deref()
                .map(plugin_storage_archive_file_id)
        })
        .unwrap_or_else(|| ctx.functions().call_uuid_v7().to_string())
}

fn attach_fast_file_write_metadata(
    file_content: &mut [TransactionFileContent],
    splice_provenance: Option<RequestBlobSpliceProvenance>,
    mutation_identity: Option<MutationIdentity>,
) {
    for file_content in file_content {
        file_content.set_splice_provenance(splice_provenance.clone());
        file_content.set_mutation_identity(mutation_identity);
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
    if staged.state_rows.is_empty() && staged.file_content_writes.is_empty() {
        return Ok(count);
    }
    let write = if staged.file_content_writes.is_empty() {
        TransactionWrite::Rows {
            mode,
            rows: staged.state_rows,
        }
    } else {
        TransactionWrite::RowsWithFileContent {
            mode,
            rows: staged.state_rows,
            file_content: staged.file_content_writes,
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
    Ok(lix_file_insert_stage_from_batch(batch, branch_binding)?
        .state_rows
        .into_rows())
}

fn lix_file_delete_stage_from_batch(
    batch: &RecordBatch,
    branch_binding: Option<&str>,
    blob_ref_keys: &BTreeSet<FilesystemBlobRefKey>,
    plugin_archive_delete_target: Option<&str>,
) -> Result<LixFileStagedBatch> {
    let mut staged = LixFileStagedBatch::with_row_capacity(batch.num_rows().saturating_mul(3));
    for row_index in 0..batch.num_rows() {
        let file_id = required_string_value(batch, row_index, "id")?;
        let path = optional_string_value(batch, row_index, "path")?;
        validate_lix_file_delete_target(path.as_deref(), &file_id, plugin_archive_delete_target)?;
        let context = file_row_context_from_batch(batch, row_index, branch_binding)?;
        let mut plan = plan_file_delete(FileDeleteInput {
            file_id: file_id.clone(),
            has_blob_ref: blob_ref_keys
                .contains(&FilesystemBlobRefKey::from_context(&context, &file_id)),
            context,
        });
        if let Some(plugin_key) = plugin_archive_delete_target {
            for index in 0..plan.rows.len() {
                if plan.rows.row(index).schema_key == FILE_DESCRIPTOR_SCHEMA_KEY {
                    plan.rows.set_origin(
                        index,
                        Some(TransactionWriteOrigin {
                            surface: plugin_archive_delete_origin(plugin_key).into(),
                            operation: TransactionWriteOperation::Delete,
                            primary_key: Some(Arc::new(LogicalPrimaryKey::single_id(
                                file_id.clone(),
                            ))),
                        }),
                    );
                }
            }
        }
        staged
            .extend_filesystem_delete_plan(plan)
            .map_err(lix_error_to_datafusion_error)?;
    }
    Ok(staged)
}

fn validate_lix_file_delete_target(
    path: Option<&str>,
    file_id: &str,
    plugin_archive_delete_target: Option<&str>,
) -> Result<()> {
    let archive_target_matches_id = plugin_archive_delete_target
        .is_some_and(|plugin_key| plugin_archive_file_id_matches(file_id, plugin_key));
    let Some(path) = path else {
        if plugin_archive_delete_target.is_none() {
            return Ok(());
        }
        if archive_target_matches_id {
            return Ok(());
        }
        return Err(rejected_plugin_archive_delete_error(None, file_id));
    };
    LixPath::try_from_file_path(path).map_err(lix_error_to_datafusion_error)?;
    let archive_path_plugin_key = plugin_key_from_archive_path(path);
    if !is_plugin_storage_path(path) && !archive_target_matches_id {
        return Ok(());
    }

    match (
        archive_path_plugin_key.as_deref(),
        plugin_archive_delete_target,
    ) {
        (Some(path_key), Some(target_key))
            if path_key == target_key && plugin_archive_file_id_matches(file_id, target_key) =>
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
    rows: &MaterializedHotStateBatch,
) -> std::result::Result<BTreeSet<FilesystemBlobRefKey>, LixError> {
    let mut keys = BTreeSet::new();
    for row in rows.iter() {
        if row.schema_key() != BLOB_REF_SCHEMA_KEY {
            continue;
        }
        let Some(snapshot) = blob_ref_snapshot_from_live_row(row)? else {
            continue;
        };
        keys.insert(FilesystemBlobRefKey::from_live_row_ref(row, snapshot.id));
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
    let mut staged = LixFileStagedBatch::with_row_capacity(batch.num_rows().saturating_mul(3));
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
            crate::common::validate_lix_path_segment(&name)
                .map_err(lix_error_to_datafusion_error)?;
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
            FileDescriptorWriteIntent {
                id: Some(id.clone()),
                directory_id,
                name,
                context: context.clone(),
            }
            .append_to(&mut staged.state_rows);
        }

        if include_data_writes {
            let data =
                update_required_binary_value(batch, assignment_values, row_index, "content")?;
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
            stage_lix_file_content_update_write(
                &mut staged,
                id.clone(),
                path,
                data_filename,
                data,
                context,
                has_blob_ref,
                None,
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
                "content" => {
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
                        "UPDATE lix_file for plugin archive paths requires content".to_string(),
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
    let mut staged = LixFileStagedBatch::with_row_capacity(batch.num_rows().saturating_mul(3));

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
                "content",
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
            stage_lix_file_content_update_write(
                &mut staged,
                id.clone(),
                Some(path),
                Some(filename),
                data,
                context,
                has_blob_ref,
                None,
                None,
            )?;
        } else if plugin_rewrite_file_ids.contains(&id) {
            let data = required_binary_value(batch, row_index, "content")?;
            let has_blob_ref =
                blob_ref_keys.contains(&FilesystemBlobRefKey::from_context(&context, &id));
            stage_lix_file_content_update_write(
                &mut staged,
                id.clone(),
                Some(path),
                Some(filename),
                data,
                context,
                has_blob_ref,
                None,
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
        let data = required_binary_value(batch, row_index, "content")?;

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
        let same_plugin_owner =
            assigned_plugin.is_some_and(|plugin| existing_plugin_key == Some(plugin.key()));
        if existing_plugin_key != assigned_plugin_key || same_plugin_owner {
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
    let mut staged = LixFileStagedBatch::with_row_capacity(batch.num_rows().saturating_mul(3));

    for row_index in 0..batch.num_rows() {
        if reject_read_only_fields {
            reject_read_only_lix_file_insert_field(batch, row_index, "lixcol_schema_key")?;
            reject_read_only_lix_file_insert_field(batch, row_index, "lixcol_change_id")?;
            reject_read_only_lix_file_insert_field(batch, row_index, "lixcol_created_at")?;
            reject_read_only_lix_file_insert_field(batch, row_index, "lixcol_updated_at")?;
            reject_read_only_lix_file_insert_field(batch, row_index, "lixcol_commit_id")?;
        }

        let path = optional_string_value(batch, row_index, "path")?;
        let id = defaultable_text_insert_value(batch, row_index, "id", "INSERT into lix_file")?;
        let context = file_row_context_from_batch(batch, row_index, branch_binding)?;
        let data = if include_data_writes {
            insert_optional_binary_value(batch, row_index, "content")?
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
                    "INSERT into lix_file for plugin archive paths requires content".to_string(),
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
                data.map(Into::into),
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
        crate::common::validate_lix_path_segment(&name).map_err(lix_error_to_datafusion_error)?;
        let mut data_path = None;

        let id = if data.is_some() {
            match id {
                Some(id) => Some(id),
                None => {
                    let Some(generate_id) = generate_directory_id.as_deref_mut() else {
                        return Err(DataFusionError::Execution(
                            "INSERT into lix_file with content requires id generator".to_string(),
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
            let row_index = staged.state_rows.len();
            FileDescriptorWriteIntent {
                id: id.clone(),
                directory_id: directory_id.clone(),
                name: name.clone(),
                context: context.clone(),
            }
            .append_to(&mut staged.state_rows);
            if let Some(file_id) = id.as_ref() {
                staged.state_rows.set_origin(
                    row_index,
                    Some(lix_file_insert_origin(surface_name, file_id)),
                );
            }
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
            stage_lix_file_content_insert_write(
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

fn stage_lix_file_content_insert_write(
    staged: &mut LixFileStagedBatch,
    file_id: String,
    path: Option<String>,
    filename: Option<String>,
    content: impl Into<FileContent>,
    context: FilesystemRowContext,
    origin: Option<TransactionWriteOrigin>,
) -> Result<()> {
    let file_payload = TransactionFileContent::new(
        file_id,
        path,
        filename,
        context.branch_id.clone(),
        context.global,
        context.untracked,
        content,
    );
    if !file_payload.is_empty() {
        stage_lix_file_content_blob_ref_write(staged, &file_payload, &context, origin)?;
    }
    staged.file_content_writes.push(file_payload);
    Ok(())
}

fn stage_lix_file_content_update_write(
    staged: &mut LixFileStagedBatch,
    file_id: String,
    path: Option<String>,
    filename: Option<String>,
    content: impl Into<FileContent>,
    context: FilesystemRowContext,
    has_blob_ref: bool,
    base_blob_hash: Option<BlobId>,
    origin: Option<TransactionWriteOrigin>,
) -> Result<()> {
    let file_payload = TransactionFileContent::new(
        file_id.clone(),
        path,
        filename,
        context.branch_id.clone(),
        context.global,
        context.untracked,
        content,
    )
    .with_had_blob_ref(has_blob_ref)
    .with_base_blob_hash(base_blob_hash);
    if file_payload.is_empty() {
        if has_blob_ref {
            let row_index = staged.state_rows.len();
            append_blob_ref_tombstone_row(&mut staged.state_rows, file_id, context.clone());
            staged.state_rows.set_origin(row_index, origin.clone());
        }
        staged.file_content_writes.push(file_payload);
        return Ok(());
    }
    stage_lix_file_content_blob_ref_write(staged, &file_payload, &context, origin.clone())?;
    staged.file_content_writes.push(file_payload);
    Ok(())
}

fn stage_lix_file_content_blob_ref_write(
    staged: &mut LixFileStagedBatch,
    file_content: &TransactionFileContent,
    context: &FilesystemRowContext,
    origin: Option<TransactionWriteOrigin>,
) -> Result<()> {
    let row_index = staged.state_rows.len();
    BlobRefRowInput {
        file_id: file_content.file_id.clone(),
        blob_hash: file_content
            .blob_hash()
            .expect("non-empty payload should have blob hash"),
        size_bytes: file_content.len(),
        context: FilesystemRowContext {
            file_id: None,
            metadata: None,
            ..context.clone()
        },
    }
    .append_to(&mut staged.state_rows)
    .map_err(lix_error_to_datafusion_error)?;
    staged.state_rows.set_origin(row_index, origin);
    Ok(())
}

fn attach_lix_file_insert_origin(rows: &mut RawWriteBatch, surface_name: &str, file_id: &str) {
    let origin = lix_file_insert_origin(surface_name, file_id);
    for index in 0..rows.len() {
        let schema_key = rows.row(index).schema_key;
        if schema_key == FILE_DESCRIPTOR_SCHEMA_KEY || schema_key == BLOB_REF_SCHEMA_KEY {
            rows.set_origin(index, Some(origin.clone()));
        }
    }
}

fn lix_file_insert_origin(surface_name: &str, file_id: &str) -> TransactionWriteOrigin {
    TransactionWriteOrigin {
        surface: crate::transaction_types::shared_origin_surface(surface_name),
        operation: TransactionWriteOperation::Insert,
        primary_key: Some(Arc::new(LogicalPrimaryKey::single_id(file_id))),
    }
}

fn file_row_context_from_batch(
    batch: &RecordBatch,
    row_index: usize,
    branch_binding: Option<&str>,
) -> Result<FilesystemRowContext> {
    let scope = resolve_write_branch_scope(
        defaultable_bool_insert_value(batch, row_index, "lixcol_global", "INSERT into lix_file")?,
        branch_binding,
        "INSERT into lix_file",
    )?;

    Ok(FilesystemRowContext {
        branch_id: scope.branch_id,
        global: scope.global,
        untracked: defaultable_bool_insert_value(
            batch,
            row_index,
            "lixcol_untracked",
            "INSERT into lix_file",
        )?
        .unwrap_or(false),
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
    let scope = resolve_write_branch_scope(explicit_global, branch_binding, "UPDATE lix_file")?;

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
        // `file_id` is descriptor ownership, not filesystem namespace scope.
        // Directory resolvers are shared by every file in the same durability
        // and branch lane.
        None,
    )
}

async fn lix_file_record_batch(
    schema: &SchemaRef,
    blob_reader: &Arc<dyn BlobDataReader>,
    plugin_render: Option<PluginRenderContext>,
    load_data: bool,
    rows: Vec<MaterializedHotStateRow>,
) -> Result<RecordBatch, LixError> {
    let prepared = prepare_lix_file_rows(
        MaterializedHotStateBatch::from_rows(rows),
        &FilePathPredicate::All,
    )?;
    lix_file_record_batch_from_prepared(schema, blob_reader, plugin_render, load_data, prepared)
        .await
}

pub(super) async fn lix_file_state_record_batch(
    schema: &SchemaRef,
    blob_reader: &Arc<dyn BlobDataReader>,
    load_data: bool,
    rows: Vec<MaterializedHotStateRow>,
) -> Result<RecordBatch, LixError> {
    let prepared = prepare_lix_file_rows(
        MaterializedHotStateBatch::from_rows(rows),
        &FilePathPredicate::All,
    )?;
    if prepared.needs_plugin_render(load_data) {
        return Err(invalid_plugin_read_state(
            "historical plugin-owned file is missing its durable materialization",
        ));
    }
    lix_file_record_batch_from_prepared(schema, blob_reader, None, load_data, prepared).await
}

struct PreparedLixFileRows {
    live_rows: HotStateBatchOwners,
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
                    && !self
                        .blob_rows
                        .contains_key(&file.blob_ref_key(&self.live_rows))
            })
    }

    fn plugin_owner_candidates(&self, include_blob_backed: bool) -> Vec<FilesystemDescriptorKey> {
        self.file_rows
            .values()
            .filter(|file| {
                plugin_file_can_have_durable_owner(file)
                    && (include_blob_backed
                        || !self
                            .blob_rows
                            .contains_key(&file.blob_ref_key(&self.live_rows)))
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
    rows: impl Into<MaterializedHotStateBatch>,
    path_predicate: &FilePathPredicate,
) -> Result<PreparedLixFileRows, LixError> {
    let mut live_rows = HotStateBatchOwners::default();
    let batch = live_rows.push(rows.into());
    let mut file_rows = BTreeMap::<FilesystemDescriptorKey, FileDescriptorRecord>::new();
    let mut blob_rows = BTreeMap::<FilesystemBlobRefKey, BlobRefRecord>::new();
    let mut directory_rows = Vec::<DirectoryDescriptorRecord>::new();

    for row_index in 0..live_rows.batch(batch).len() {
        let handle = hot_state_row_handle(batch, row_index);
        let row = live_rows.row(handle);
        match row.schema_key() {
            FILE_DESCRIPTOR_SCHEMA_KEY => {
                let Some(snapshot) = file_descriptor_snapshot_from_live_row(row)? else {
                    continue;
                };
                let key = FilesystemDescriptorKey::from_file_descriptor_live_row_ref(
                    row,
                    snapshot.id.clone(),
                );
                file_rows.insert(
                    key.clone(),
                    FileDescriptorRecord {
                        id: snapshot.id,
                        directory_id: snapshot.directory_id,
                        name: snapshot.name,
                        key,
                        live: handle,
                    },
                );
            }
            BLOB_REF_SCHEMA_KEY => {
                if let Some((key, record)) = blob_ref_record_from_live_row(row, handle)? {
                    blob_rows.insert(key, record);
                }
            }
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY => {
                let Some(snapshot) = directory_descriptor_snapshot_from_live_row(row)? else {
                    continue;
                };
                directory_rows.push(DirectoryDescriptorRecord {
                    key: FilesystemDescriptorKey::from_live_row_ref(row, snapshot.id.clone()),
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
                            file.id,
                            directory_id,
                            live_rows.row(file.live).branch_id()
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
        live_rows,
        file_rows,
        blob_rows,
        file_paths,
        path_ordered_file_keys: None,
    })
}

fn prepare_indexed_lix_file_rows(
    matches: &FilesystemPathSelection,
    rows: impl Into<MaterializedHotStateBatch>,
) -> Result<PreparedLixFileRows, LixError> {
    let mut live_rows = HotStateBatchOwners::default();
    let scanned_batch = live_rows.push(rows.into());
    let indexed_batch =
        u32::try_from(live_rows.batches.len()).expect("lix_file live batch count exceeds u32");
    let mut indexed_builder =
        MaterializedHotStateBatchBuilder::with_capacity(matches.len().saturating_mul(2));
    let mut file_rows = BTreeMap::<FilesystemDescriptorKey, FileDescriptorRecord>::new();
    let mut blob_rows = BTreeMap::<FilesystemBlobRefKey, BlobRefRecord>::new();
    let mut file_paths = BTreeMap::<FilesystemDescriptorKey, String>::new();
    let mut path_ordered_file_keys = Vec::with_capacity(matches.len());
    let mut inline_data_by_row = BTreeMap::<u32, Vec<u8>>::new();
    for entry in matches.entries() {
        if entry.kind != FilesystemPathKind::File {
            continue;
        }
        let descriptor_row = entry.live_row();
        let descriptor_row_index = indexed_builder.len();
        indexed_builder.push_owned(descriptor_row);
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
                live: hot_state_row_handle(indexed_batch, descriptor_row_index),
            },
        );
        if let Some(blob_ref) = entry.blob_ref_live_row() {
            let row_index = indexed_builder.push_materialized_ref(
                &blob_ref.row_pk,
                &blob_ref.schema_key,
                blob_ref.file_id.as_deref(),
                blob_ref.snapshot_content.clone(),
                blob_ref.metadata.clone(),
                blob_ref.deleted,
                blob_ref.created_at,
                blob_ref.updated_at,
                blob_ref.global,
                blob_ref.change_id,
                blob_ref.commit_id,
                blob_ref.untracked,
                &blob_ref.branch_id,
            );
            if let Some(data) = entry.cached_blob_data() {
                inline_data_by_row.insert(
                    u32::try_from(row_index).expect("indexed lix_file row count exceeds u32"),
                    data.as_ref().to_vec(),
                );
            }
        }
    }
    let pushed_indexed_batch = live_rows.push(indexed_builder.finish());
    debug_assert_eq!(pushed_indexed_batch, indexed_batch);

    for batch in [indexed_batch, scanned_batch] {
        for row_index in 0..live_rows.batch(batch).len() {
            let handle = hot_state_row_handle(batch, row_index);
            let row = live_rows.row(handle);
            match row.schema_key() {
                BLOB_REF_SCHEMA_KEY => {
                    if let Some((key, mut record)) = blob_ref_record_from_live_row(row, handle)? {
                        if batch == indexed_batch {
                            record.inline_data = inline_data_by_row.remove(&handle.row);
                        }
                        blob_rows.entry(key).or_insert(record);
                    }
                }
                _ => {}
            }
        }
    }

    Ok(PreparedLixFileRows {
        live_rows,
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
            "content" => Arc::new(LargeBinaryArray::from(
                entries.iter().map(|_| Some(&[][..])).collect::<Vec<_>>(),
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
                    .map(|entry| Some(entry.id()))
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
    file_id: Option<String>,
    global: bool,
    change_id: Option<String>,
    created_at: String,
    updated_at: String,
    commit_id: Option<String>,
    untracked: bool,
    metadata: Option<String>,
}

#[derive(Default)]
struct LixFileRecordBatchColumns {
    ids: Vec<Option<String>>,
    paths: Vec<Option<String>>,
    directory_ids: Vec<Option<String>>,
    names: Vec<Option<String>>,
    data_values: Vec<Option<Vec<u8>>>,
    schema_keys: Vec<Option<String>>,
    file_ids: Vec<Option<String>>,
    globals: Vec<Option<bool>>,
    change_ids: Vec<Option<String>>,
    created_ats: Vec<Option<String>>,
    updated_ats: Vec<Option<String>>,
    commit_ids: Vec<Option<String>>,
    untracked_values: Vec<Option<bool>>,
    metadata_values: Vec<Option<String>>,
}

impl LixFileRecordBatchColumns {
    fn push(&mut self, row: LixFileRecordBatchRow) {
        self.ids.push(Some(row.id));
        self.paths.push(Some(row.path));
        self.directory_ids.push(row.directory_id);
        self.names.push(Some(row.name));
        self.data_values.push(row.data);
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
    }

    fn into_record_batch(self, schema: &SchemaRef) -> Result<RecordBatch, LixError> {
        let row_count = self.ids.len();
        let Self {
            ids,
            paths,
            directory_ids,
            names,
            data_values,
            schema_keys,
            file_ids,
            globals,
            change_ids,
            created_ats,
            updated_ats,
            commit_ids,
            untracked_values,
            metadata_values,
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
        let schema_keys: ArrayRef = Arc::new(StringArray::from(schema_keys));
        let file_ids: ArrayRef = Arc::new(StringArray::from(file_ids));
        let globals: ArrayRef = Arc::new(BooleanArray::from(globals));
        let change_ids: ArrayRef = Arc::new(StringArray::from(change_ids));
        let created_ats: ArrayRef = Arc::new(StringArray::from(created_ats));
        let updated_ats: ArrayRef = Arc::new(StringArray::from(updated_ats));
        let commit_ids: ArrayRef = Arc::new(StringArray::from(commit_ids));
        let untracked_values: ArrayRef = Arc::new(BooleanArray::from(untracked_values));
        let metadata_values: ArrayRef = Arc::new(StringArray::from(metadata_values));

        let mut columns = Vec::<ArrayRef>::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let array = match field.name().as_str() {
                "id" => Arc::clone(&ids),
                "path" => Arc::clone(&paths),
                "directory_id" => Arc::clone(&directory_ids),
                "name" => Arc::clone(&names),
                "content" => Arc::clone(&data_values),
                "lixcol_schema_key" => Arc::clone(&schema_keys),
                "lixcol_file_id" => Arc::clone(&file_ids),
                "lixcol_global" => Arc::clone(&globals),
                "lixcol_change_id" => Arc::clone(&change_ids),
                "lixcol_created_at" => Arc::clone(&created_ats),
                "lixcol_updated_at" => Arc::clone(&updated_ats),
                "lixcol_commit_id" => Arc::clone(&commit_ids),
                "lixcol_untracked" => Arc::clone(&untracked_values),
                "lixcol_metadata" => Arc::clone(&metadata_values),
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
    let needs_data = load_data && projected_columns.contains(&"content");
    let PreparedLixFileRows {
        live_rows,
        mut file_rows,
        blob_rows,
        mut file_paths,
        path_ordered_file_keys,
    } = prepared;
    let mut columns = LixFileRecordBatchColumns::default();
    let mut blob_bytes = if needs_data {
        load_blob_bytes_for_files(blob_reader, &live_rows, &file_rows, &blob_rows).await?
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
                &live_rows,
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
        let blob_key = file.blob_ref_key(&live_rows);
        let data = if needs_data {
            match blob_bytes.take(&blob_key).flatten() {
                Some(data) => Some(data),
                None => match rendered_plugin_bytes.remove(&key) {
                    Some(data) => Some(data),
                    None => Some(Vec::new()),
                },
            }
        } else {
            Some(Vec::new())
        };
        let projected_change_id = blob_rows
            .get(&blob_key)
            .and_then(|blob_ref| live_rows.row(blob_ref.live).change_id())
            .or_else(|| live_rows.row(file.live).change_id());
        let live = live_rows.row(file.live);
        let content_live = blob_rows
            .get(&blob_key)
            .map(|blob_ref| live_rows.row(blob_ref.live));
        let FileDescriptorRecord {
            id,
            directory_id,
            name,
            ..
        } = file;
        columns.push(LixFileRecordBatchRow {
            id,
            path,
            directory_id,
            name,
            data,
            file_id: live.file_id().map(str::to_owned),
            global: live.global(),
            change_id: projected_change_id.map(|id| id.to_string()),
            created_at: live.created_at().to_string(),
            updated_at: content_live.unwrap_or(live).updated_at().to_string(),
            commit_id: live.commit_id().map(|id| id.to_string()),
            untracked: live.untracked(),
            metadata: live.metadata().map(|value| serialize_row_metadata(value)),
        });
    }

    columns.into_record_batch(schema)
}

async fn exact_path_data_rows_from_prepared(
    blob_reader: &Arc<dyn BlobDataReader>,
    plugin_render: Option<PluginRenderContext>,
    prepared: PreparedLixFileRows,
    data_range: Option<&Range<u64>>,
) -> Result<Vec<Vec<Value>>, LixError> {
    let PreparedLixFileRows {
        live_rows,
        mut file_rows,
        blob_rows,
        mut file_paths,
        path_ordered_file_keys,
    } = prepared;
    let mut blob_bytes = if data_range.is_none() {
        load_blob_bytes_for_files(blob_reader, &live_rows, &file_rows, &blob_rows).await?
    } else {
        LoadedBlobBytes::default()
    };
    let mut blob_ranges = match data_range {
        Some(range) => {
            load_blob_ranges_for_files(
                blob_reader,
                &live_rows,
                &file_rows,
                &blob_rows,
                range.clone(),
            )
            .await?
        }
        None => LoadedBlobRanges::default(),
    };
    let file_keys =
        path_ordered_file_keys.unwrap_or_else(|| file_rows.keys().cloned().collect::<Vec<_>>());
    let mut rows = Vec::with_capacity(file_keys.len());
    let mut rendered_plugin_bytes = match &plugin_render {
        Some(plugin_render) => {
            render_plugin_files_for_sql(
                plugin_render,
                blob_reader,
                &live_rows,
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
        let blob_key = file.blob_ref_key(&live_rows);
        if let Some(range) = data_range {
            let content_identity = blob_rows
                .get(&blob_key)
                .map(|blob_ref| blob_ref.blob_hash.clone())
                .or_else(|| {
                    live_rows
                        .row(file.live)
                        .change_id()
                        .map(|change_id| change_id.to_string())
                })
                .unwrap_or_else(|| file.id.clone());
            let selected = match blob_ranges.take(&blob_key).flatten() {
                Some(data) => data,
                None => match rendered_plugin_bytes.remove(&key) {
                    Some(data) => materialize_vec_range(data, range.clone())?,
                    None => materialize_vec_range(Vec::new(), range.clone())?,
                },
            };
            rows.push(vec![
                Value::Text(path),
                Value::Blob(selected.bytes.into()),
                Value::Integer(i64::try_from(selected.total_size).map_err(|_| {
                    LixError::new(LixError::CODE_INTERNAL_ERROR, "file size exceeds SQL i64")
                })?),
                Value::Integer(i64::try_from(selected.range.start).map_err(|_| {
                    LixError::new(LixError::CODE_INTERNAL_ERROR, "file range exceeds SQL i64")
                })?),
                Value::Integer(i64::try_from(selected.range.end).map_err(|_| {
                    LixError::new(LixError::CODE_INTERNAL_ERROR, "file range exceeds SQL i64")
                })?),
                Value::Text(content_identity),
            ]);
        } else {
            let data = match blob_bytes.take(&blob_key).flatten() {
                Some(data) => Some(data),
                None => match rendered_plugin_bytes.remove(&key) {
                    Some(data) => Some(data),
                    None => Some(Vec::new()),
                },
            };
            rows.push(vec![
                Value::Text(path),
                data.map_or(Value::Null, |data| Value::Blob(data.into())),
            ]);
        }
    }

    Ok(rows)
}

fn materialize_vec_range(data: Vec<u8>, requested: Range<u64>) -> Result<BlobRangeBytes, LixError> {
    let total_size = u64::try_from(data.len())
        .map_err(|_| LixError::new(LixError::CODE_INTERNAL_ERROR, "file size exceeds u64"))?;
    // A zero-width result is the metadata-bearing representation of a
    // present empty file. It lets bounded download callers distinguish that
    // file from a missing path without materializing an unbounded read first.
    if total_size == 0 && requested.start == 0 {
        return Ok(BlobRangeBytes {
            bytes: Vec::new(),
            total_size,
            range: 0..0,
        });
    }
    if requested.start >= requested.end || requested.start >= total_size {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "file read range is not satisfiable",
        ));
    }
    let range = requested.start..requested.end.min(total_size);
    let start = usize::try_from(range.start)
        .map_err(|_| LixError::new(LixError::CODE_INVALID_PARAM, "file range is too large"))?;
    let end = usize::try_from(range.end)
        .map_err(|_| LixError::new(LixError::CODE_INVALID_PARAM, "file range is too large"))?;
    Ok(BlobRangeBytes {
        bytes: data[start..end].to_vec(),
        total_size,
        range,
    })
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

#[derive(Default)]
struct LoadedBlobRanges {
    bytes_by_key: BTreeMap<FilesystemBlobRefKey, Option<BlobRangeBytes>>,
    remaining_by_key: BTreeMap<FilesystemBlobRefKey, usize>,
}

impl LoadedBlobRanges {
    fn take(&mut self, key: &FilesystemBlobRefKey) -> Option<Option<BlobRangeBytes>> {
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

async fn load_blob_ranges_for_files(
    blob_reader: &Arc<dyn BlobDataReader>,
    live_rows: &HotStateBatchOwners,
    file_rows: &BTreeMap<FilesystemDescriptorKey, FileDescriptorRecord>,
    blob_rows: &BTreeMap<FilesystemBlobRefKey, BlobRefRecord>,
    range: Range<u64>,
) -> Result<LoadedBlobRanges, LixError> {
    if file_rows.is_empty() || blob_rows.is_empty() {
        return Ok(LoadedBlobRanges::default());
    }
    let mut keys = Vec::new();
    let mut requests = Vec::new();
    let mut bytes_by_key = BTreeMap::new();
    let mut remaining_by_key = BTreeMap::<FilesystemBlobRefKey, usize>::new();
    for file in file_rows.values() {
        let key = file.blob_ref_key(live_rows);
        if let Some(row) = blob_rows.get(&key) {
            let remaining = remaining_by_key.entry(key.clone()).or_insert(0);
            if *remaining == 0 {
                if let Some(data) = &row.inline_data {
                    bytes_by_key.insert(
                        key.clone(),
                        Some(materialize_vec_range(data.clone(), range.clone())?),
                    );
                } else {
                    keys.push(key);
                    requests.push((BlobId::from_hex(&row.blob_hash)?, range.clone()));
                }
            }
            *remaining += 1;
        }
    }
    if !keys.is_empty() {
        let values = blob_reader.load_ranges_many(&requests).await?.into_vec();
        if values.len() != keys.len() {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "blob range reader returned {} values for {} requested hashes",
                    values.len(),
                    keys.len()
                ),
            ));
        }
        bytes_by_key.extend(keys.into_iter().zip(values));
    }
    Ok(LoadedBlobRanges {
        bytes_by_key,
        remaining_by_key,
    })
}

async fn load_blob_bytes_for_files(
    blob_reader: &Arc<dyn BlobDataReader>,
    live_rows: &HotStateBatchOwners,
    file_rows: &BTreeMap<FilesystemDescriptorKey, FileDescriptorRecord>,
    blob_rows: &BTreeMap<FilesystemBlobRefKey, BlobRefRecord>,
) -> Result<LoadedBlobBytes, LixError> {
    if file_rows.is_empty() || blob_rows.is_empty() {
        return Ok(LoadedBlobBytes::default());
    }
    let mut keys = Vec::new();
    let mut hashes = Vec::new();
    let mut bytes_by_key = BTreeMap::new();
    let mut remaining_by_key = BTreeMap::<FilesystemBlobRefKey, usize>::new();
    for file in file_rows.values() {
        let key = file.blob_ref_key(live_rows);
        if let Some(row) = blob_rows.get(&key) {
            let remaining = remaining_by_key.entry(key.clone()).or_insert(0);
            if *remaining == 0 {
                if let Some(data) = &row.inline_data {
                    bytes_by_key.insert(key.clone(), Some(data.clone()));
                } else {
                    keys.push(key);
                    hashes.push(BlobId::from_hex(&row.blob_hash)?);
                }
            }
            *remaining += 1;
        }
    }
    if !keys.is_empty() {
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
        bytes_by_key.extend(keys.into_iter().zip(values));
    }
    Ok(LoadedBlobBytes {
        bytes_by_key,
        remaining_by_key,
    })
}

async fn render_plugin_files_for_sql(
    plugin_render: &PluginRenderContext,
    blob_reader: &Arc<dyn BlobDataReader>,
    live_rows: &HotStateBatchOwners,
    file_keys: &[FilesystemDescriptorKey],
    file_rows: &BTreeMap<FilesystemDescriptorKey, FileDescriptorRecord>,
    blob_rows: &BTreeMap<FilesystemBlobRefKey, BlobRefRecord>,
    file_paths: &BTreeMap<FilesystemDescriptorKey, String>,
) -> Result<BTreeMap<FilesystemDescriptorKey, Vec<u8>>, LixError> {
    let mut materialized_file_keys = Vec::new();
    let rendered = BTreeMap::new();
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
        let Some(_plugin) = branch.registry.get(owner.plugin_key()) else {
            return Err(plugin_unavailable_error(file, path, owner));
        };
        let blob_key = file.blob_ref_key(live_rows);
        let blob = blob_rows.get(&blob_key);
        if !branch.catalog.matches_plugin(owner.plugin_key(), path) {
            continue;
        }
        match blob {
            Some(_) => {
                if plugin_render.session_file_views.is_some() {
                    materialized_file_keys.push(key.clone());
                }
            }
            None => {
                return Err(invalid_plugin_read_state(format!(
                    "plugin-owned file '{}' is missing its durable materialization",
                    file.id
                )));
            }
        }
    }
    for file_key in materialized_file_keys {
        acknowledge_materialized_file(
            plugin_render,
            blob_reader,
            live_rows,
            &file_key,
            file_rows,
            blob_rows,
            file_paths,
        )
        .await?;
    }
    Ok(rendered)
}

async fn acknowledge_materialized_file(
    plugin_render: &PluginRenderContext,
    _blob_reader: &Arc<dyn BlobDataReader>,
    live_rows: &HotStateBatchOwners,
    file_key: &FilesystemDescriptorKey,
    file_rows: &BTreeMap<FilesystemDescriptorKey, FileDescriptorRecord>,
    blob_rows: &BTreeMap<FilesystemBlobRefKey, BlobRefRecord>,
    file_paths: &BTreeMap<FilesystemDescriptorKey, String>,
) -> Result<(), LixError> {
    let file = file_rows
        .get(file_key)
        .expect("v2 materialization candidate has a descriptor");
    let blob = blob_rows
        .get(&file.blob_ref_key(live_rows))
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
    let semantic_root = live_rows
        .row(blob.live)
        .change_id()
        .map(|id| id.to_string())
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
        Ok(observation) => Some(observation),
        Err(error) if error.code == LixError::CODE_PLUGIN_OBSERVATION_STALE => None,
        Err(error) => return Err(error),
    };
    if let Some(session_file_views) = &plugin_render.session_file_views {
        session_file_views.remember_plugin_file_view(
            SessionFileViewKey::new(file_key.branch_id(), file_key.descriptor_id()),
            SessionPluginFileView {
                path: path.clone(),
                plugin_key: plugin.key().to_string(),
                plugin_generation: plugin.archive_blob_hash().to_string(),
                owner_change_id: owner_change_id.to_string(),
                observation,
            },
        );
    }
    Ok(())
}

async fn plugin_render_context_for_lix_file_scan(
    hot_state: Arc<dyn HotStateReader>,
    request: &HotStateScanRequest,
    host: PluginRuntimeHost,
    prepared: &PreparedLixFileRows,
    include_blob_backed_candidates: bool,
) -> Result<Option<PluginRenderContext>, LixError> {
    plugin_render_context_for_lix_file_scan_cached(
        hot_state,
        request,
        host,
        prepared,
        include_blob_backed_candidates,
        None,
    )
    .await
}

async fn plugin_render_context_for_lix_file_scan_cached(
    hot_state: Arc<dyn HotStateReader>,
    request: &HotStateScanRequest,
    host: PluginRuntimeHost,
    prepared: &PreparedLixFileRows,
    include_blob_backed_candidates: bool,
    cache_snapshot: Option<u128>,
) -> Result<Option<PluginRenderContext>, LixError> {
    let candidates = prepared.plugin_owner_candidates(include_blob_backed_candidates);
    if candidates.is_empty() {
        return Ok(None);
    }
    let branches =
        load_plugin_render_branches(Arc::clone(&hot_state), request, &host, cache_snapshot).await?;
    plugin_render_context_with_branches(
        hot_state,
        host,
        branches,
        candidates,
        include_blob_backed_candidates,
    )
    .await
}

async fn load_plugin_render_branches(
    hot_state: Arc<dyn HotStateReader>,
    request: &HotStateScanRequest,
    host: &PluginRuntimeHost,
    cache_snapshot: Option<u128>,
) -> Result<BTreeMap<String, BranchPluginRenderContext>, LixError> {
    let branch_ids = request
        .filter
        .branch_ids
        .iter()
        .filter(|branch_id| branch_id.as_str() != GLOBAL_BRANCH_ID)
        .cloned()
        .collect::<BTreeSet<_>>();
    let cached = match cache_snapshot {
        Some(snapshot) => host.cached_plugin_registries(snapshot, &branch_ids)?,
        None => None,
    };
    let registries = match cached {
        Some(registries) => registries,
        None => {
            let registry_reads = branch_ids.iter().cloned().map(|branch_id| {
                let hot_state = Arc::clone(&hot_state);
                async move {
                    let rows = hot_state
                        .scan_tracked_batch(&HotStateScanRequest {
                            filter: HotStateFilter {
                                schema_keys: vec!["lix_key_value".to_string()],
                                row_pks: vec![RowPk::single(PLUGIN_REGISTRY_KEY)],
                                branch_ids: vec![branch_id.clone()],
                                file_ids: vec![crate::NullableKeyFilter::Null],
                                untracked: Some(false),
                                ..HotStateFilter::default()
                            },
                            projection: plugin_control_hot_state_projection(),
                            limit: Some(1),
                        })
                        .await?;
                    let row = rows.iter().find(|row| {
                        row.schema_key() == "lix_key_value"
                            && row.row_pk().as_single_string().ok() == Some(PLUGIN_REGISTRY_KEY)
                            && row.file_id().is_none()
                            && row.branch_id() == branch_id.as_str()
                            && !row.global()
                            && !row.untracked()
                    });
                    let registry = PluginRegistry::from_optional_hot_state_row(row, &branch_id)?;
                    Ok::<_, LixError>((branch_id, registry))
                }
            });
            let registries = try_join_all(registry_reads)
                .await?
                .into_iter()
                .collect::<BTreeMap<_, _>>();
            if let Some(snapshot) = cache_snapshot {
                host.cache_plugin_registries(snapshot, &registries)?;
            }
            registries
        }
    };

    let mut branches = BTreeMap::<String, BranchPluginRenderContext>::new();
    for (branch_id, registry) in registries {
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
    hot_state: Arc<dyn HotStateReader>,
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
            let hot_state = Arc::clone(&hot_state);
            let branch_id = branch_id.clone();
            let file_ids = candidate_keys.keys().cloned().collect::<BTreeSet<_>>();
            async move {
                let rows = hot_state
                    .scan_tracked_batch(&HotStateScanRequest {
                        filter: HotStateFilter {
                            schema_keys: vec!["lix_key_value".to_string()],
                            row_pks: vec![RowPk::single(PLUGIN_OWNER_KEY)],
                            branch_ids: vec![branch_id.clone()],
                            file_ids: file_ids
                                .iter()
                                .cloned()
                                .map(crate::NullableKeyFilter::Value)
                                .collect(),
                            untracked: Some(false),
                            ..HotStateFilter::default()
                        },
                        projection: plugin_control_hot_state_projection(),
                        limit: None,
                    })
                    .await?;
                Ok::<_, LixError>((branch_id, file_ids, rows))
            }
        });
    let mut owners_by_file = BTreeMap::new();
    let mut owner_change_ids_by_file = BTreeMap::new();
    let owner_rows = try_join_all(owner_reads).await?;
    for (branch_id, file_ids, rows) in owner_rows {
        for row in rows.iter() {
            let Some(file_id) = row.file_id() else {
                continue;
            };
            if row.schema_key() != "lix_key_value"
                || row.row_pk().as_single_string().ok() != Some(PLUGIN_OWNER_KEY)
                || row.branch_id() != branch_id.as_str()
                || row.global()
                || row.untracked()
                || !file_ids.contains(file_id)
            {
                continue;
            }
            let owned_row = row.to_owned();
            // KNOWN LANE GAP: this render context resolves owners through
            // `scan_tracked_batch`, a tracked-only reader, so untracked
            // plugin-owned files are not rendered from rows here. They do
            // not need to be - an untracked file's bytes round-trip through its
            // stored content blob, which is asserted by the lane-parity tests.
            // Extending this to both lanes means changing the reader and
            // belongs with the read-path work, not the unskip.
            let Some(owner) = PluginFileOwner::from_hot_state_row(&owned_row, &branch_id, false)?
            else {
                continue;
            };
            let candidate_key = candidate_keys_by_branch
                .get(&branch_id)
                .and_then(|candidate_keys| candidate_keys.get(file_id))
                .expect("owner row was filtered to candidate file ids")
                .clone();
            let owner_change_id = row.change_id().ok_or_else(|| {
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
        "branch_id": file.key.branch_id(),
        "file_id": file.id,
        "path": path,
        "plugin_key": owner.plugin_key(),
    }))
}

fn plugin_control_hot_state_projection() -> HotStateProjection {
    HotStateProjection {
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
            .any(|index| base_schema.field(*index).name() == "content"),
        None => true,
    };
    projected_needs_data
        || filters
            .iter()
            .any(|filter| contains_column(filter, "content"))
}

fn scan_needs_required_blob_rows(
    base_schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
) -> bool {
    let projects_blob_column = match projection {
        Some(indices) => indices.iter().any(|index| {
            matches!(
                base_schema.field(*index).name().as_str(),
                "content" | "lixcol_change_id"
            )
        }),
        None => true,
    };
    projects_blob_column
        || filters.iter().any(|filter| {
            contains_column(filter, "content") || contains_column(filter, "lixcol_change_id")
        })
}

fn scan_needs_content_updated_at(
    base_schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
) -> bool {
    let projects_updated_at = match projection {
        Some(indices) => indices
            .iter()
            .any(|index| base_schema.field(*index).name() == "lixcol_updated_at"),
        None => true,
    };
    projects_updated_at
        || filters
            .iter()
            .any(|filter| contains_column(filter, "lixcol_updated_at"))
}

fn should_use_path_index(path_predicate: &FilePathPredicate, needs_blob_rows: bool) -> bool {
    path_predicate != &FilePathPredicate::All || !needs_blob_rows
}

fn scan_needs_file_timestamps(
    base_schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
) -> bool {
    let projects_timestamp = match projection {
        Some(indices) => indices.iter().any(|index| {
            matches!(
                base_schema.field(*index).name().as_str(),
                "lixcol_created_at" | "lixcol_updated_at"
            )
        }),
        None => true,
    };
    projects_timestamp
        || filters.iter().any(|filter| {
            contains_column(filter, "lixcol_created_at")
                || contains_column(filter, "lixcol_updated_at")
        })
}

fn lix_file_scan_request(
    branch_binding: Option<&str>,
    projected_schema: Option<&Schema>,
    limit: Option<usize>,
) -> HotStateScanRequest {
    HotStateScanRequest {
        filter: HotStateFilter {
            schema_keys: vec![
                FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                BLOB_REF_SCHEMA_KEY.to_string(),
                DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
            ],
            branch_ids: branch_binding
                .map(|branch_id| vec![branch_id.to_string()])
                .unwrap_or_default(),
            ..HotStateFilter::default()
        },
        projection: lix_file_hot_state_projection(projected_schema),
        limit,
    }
}

fn lix_file_hot_state_projection(projected_schema: Option<&Schema>) -> HotStateProjection {
    let Some(schema) = projected_schema else {
        return HotStateProjection::default();
    };
    let mut columns = vec!["snapshot_content".to_string()];
    if schema
        .fields()
        .iter()
        .any(|field| field.name() == "lixcol_metadata")
    {
        columns.push("metadata".to_string());
    }
    HotStateProjection { columns }
}

async fn scan_lix_file_live_batch(
    hot_state: Arc<dyn HotStateReader>,
    request: &HotStateScanRequest,
    target_file_ids: &FileIdConstraint,
) -> std::result::Result<MaterializedHotStateBatch, LixError> {
    let target_file_ids = match target_file_ids {
        FileIdConstraint::All => return hot_state.scan_batch(request).await,
        FileIdConstraint::None => return Ok(MaterializedHotStateBatch::default()),
        FileIdConstraint::Ids(target_file_ids) => target_file_ids,
    };

    let mut file_request = request.clone();
    file_request.filter.schema_keys = vec![
        FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
        BLOB_REF_SCHEMA_KEY.to_string(),
    ];
    // Bound this read on the file id, not the row PK.
    //
    // The hot index key is file-first -- `scope(branch, generation) ++
    // schema_key ++ file_id ++ row_pk` -- so a row PK on its own names
    // neither a point key nor a prefix. With `row_pks` set and `file_ids`
    // empty, `hot_exact_identity_batches` returns a batch but
    // `may_use_null_point_batch` refuses it (both schema keys here have
    // file-backed members, and a null-file point key would miss the real row),
    // `hot_file_scan_prefixes` declines for want of a file id, and the request
    // lands on the widest arm: a full walk of every `lix_file_descriptor` and
    // every `lix_binary_blob_ref` row in the branch, filtered in memory. That
    // is O(files in branch) per statement.
    //
    // Pinning `file_ids` *instead of* `row_pks` -- not in addition to --
    // routes to `hot_file_scan_prefixes`, one tight `(branch, generation,
    // schema_key, file_id)` prefix seek per target file. Adding the pin while
    // keeping `row_pks` would be much worse than the walk it replaces:
    // `FiniteHotIdentityBatchRef::new` builds the *cross product*
    // `row_pks.len() * file_ids.len()`, so N target files would encode N^2
    // point keys per schema key.
    //
    // Dropping `row_pks` cannot widen the answer, because `file_id ==
    // row_pk` holds by construction for both of these schema keys:
    // `transaction::normalization::canonicalize_descriptor_file_id` *derives*
    // the descriptor row's `file_id` from its row PK on every normalized
    // write, and both `lix_binary_blob_ref` producers in `filesystem::planner`
    // (`BlobRefRowInput::append_to` and `append_blob_ref_tombstone_row`,
    // tombstones included) set the pair from one variable. Both surfaces are
    // read-only, so no caller can supply a divergent pair.
    // `scan_exact_file_blob_batch` below already pairs them the same way.
    file_request.filter.row_pks.clear();
    file_request.filter.file_ids = target_file_ids
        .iter()
        .map(|file_id| crate::NullableKeyFilter::Value(file_id.clone()))
        .collect();
    // Preserve the canonical-UUID validation the row-PK projection used to
    // perform, so a malformed file id stays an error instead of becoming a
    // silently empty answer.
    for file_id in target_file_ids {
        file_id_row_pk(file_id)?;
    }

    let file_rows = hot_state.scan_batch(&file_request).await?;

    let mut directory_request = request.clone();
    directory_request.filter.schema_keys = vec![DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string()];
    directory_request.filter.row_pks.clear();
    directory_request.limit = None;
    let directory_rows = hot_state.scan_batch(&directory_request).await?;
    Ok(concatenate_hot_state_batches([file_rows, directory_rows]))
}

fn concatenate_hot_state_batches(
    batches: impl IntoIterator<Item = MaterializedHotStateBatch>,
) -> MaterializedHotStateBatch {
    let batches = batches.into_iter().collect::<Vec<_>>();
    let row_count = batches.iter().map(MaterializedHotStateBatch::len).sum();
    let mut builder = MaterializedHotStateBatchBuilder::with_capacity(row_count);
    for batch in &batches {
        for row in batch.iter() {
            builder.push_ref(row, None);
        }
    }
    builder.finish()
}

fn scan_indexed_file_batch(
    matches: &FilesystemPathSelection,
    needs_blob_rows: bool,
) -> Result<MaterializedHotStateBatch, LixError> {
    if matches.is_empty() || !needs_blob_rows {
        return Ok(MaterializedHotStateBatch::default());
    }
    let mut builder = MaterializedHotStateBatchBuilder::with_capacity(matches.len());
    for row in matches
        .entries()
        .filter(|entry| entry.kind == FilesystemPathKind::File)
        .filter_map(FilesystemPathEntry::blob_ref_live_row)
    {
        builder.push_materialized_ref(
            &row.row_pk,
            &row.schema_key,
            row.file_id.as_deref(),
            row.snapshot_content.clone(),
            row.metadata.clone(),
            row.deleted,
            row.created_at,
            row.updated_at,
            row.global,
            row.change_id,
            row.commit_id,
            row.untracked,
            &row.branch_id,
        );
    }
    Ok(builder.finish())
}

async fn scan_exact_file_blob_batch(
    hot_state: Arc<dyn HotStateReader>,
    request: &HotStateScanRequest,
    file_ids: &BTreeSet<String>,
) -> Result<MaterializedHotStateBatch, LixError> {
    if file_ids.is_empty() {
        return Ok(MaterializedHotStateBatch::default());
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
            file_ids.iter().map(move |file_id| {
                Ok(HotStateExactRowRequest {
                    branch_id: branch_id.clone(),
                    schema_key: BLOB_REF_SCHEMA_KEY.to_string(),
                    row_pk: file_id_row_pk(file_id)?,
                    file_id: Some(file_id.clone()),
                })
            })
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let rows = hot_state
        .load_exact_batch(&HotStateExactBatchRequest {
            rows: exact_rows,
            projection: request.projection.clone(),
            untracked: request.filter.untracked,
            include_tombstones: request.filter.include_tombstones,
        })
        .await?;
    Ok(rows.into_present_batch())
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
    let id = single_exact_string_constraint(file_id_constraint_from_filters(filters)?);

    match (path_plugin_key, id) {
        (Some(path_key), Some(id)) if plugin_archive_file_id_matches(&id, &path_key) => {
            Ok(Some(path_key))
        }
        (Some(path_key), None) => Ok(Some(path_key)),
        _ => Ok(None),
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
            "path" | "directory_id" | "name" | "content" | "lixcol_metadata"
        ) {
            return Err(DataFusionError::Execution(format!(
                "UPDATE lix_file cannot stage read-only column '{column_name}'"
            )));
        }
        if column_name == "content" {
            reject_non_binary_lix_file_content_assignment(expr)?;
        }
    }
    Ok(())
}

fn reject_non_binary_lix_file_content_assignment(expr: &Expr) -> Result<()> {
    if let Expr::Literal(value, _) = expr {
        if !scalar_is_binary_or_null(value) {
            return Err(non_binary_lix_file_content_assignment_error());
        }
    }

    Ok(())
}

fn non_binary_lix_file_content_assignment_error() -> DataFusionError {
    lix_file_content_type_error("UPDATE lix_file", "content", LIX_FILE_CONTENT_CAST_HINT)
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
            Err(lix_file_content_type_error(
                "UPDATE lix_file",
                column_name,
                "Use CAST('' AS BYTEA) for an empty file or omit content to leave contents unchanged.",
            ))
        }
        UpdateCell::Assigned(SqlCell::Value(
            ScalarValue::Binary(Some(value)) | ScalarValue::LargeBinary(Some(value)),
        )) => Ok(value),
        UpdateCell::Assigned(SqlCell::Value(ScalarValue::FixedSizeBinary(_, Some(value)))) => {
            Ok(value)
        }
        UpdateCell::Assigned(SqlCell::Value(other)) => Err(lix_file_content_type_error_with_value(
            "UPDATE lix_file",
            column_name,
            &other,
            LIX_FILE_CONTENT_CAST_HINT,
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
        Some(other) => Err(lix_file_content_type_error_with_value(
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
        ) => Err(lix_file_content_type_error(
            "INSERT into lix_file",
            column_name,
            "Use CAST('' AS BYTEA) for an empty file or omit content to create an empty file.",
        )),
        Some(ScalarValue::Binary(Some(value)) | ScalarValue::LargeBinary(Some(value))) => {
            Ok(Some(value))
        }
        Some(ScalarValue::FixedSizeBinary(_, Some(value))) => Ok(Some(value)),
        Some(other) => Err(lix_file_content_type_error_with_value(
            "INSERT into lix_file",
            column_name,
            &other,
            LIX_FILE_CONTENT_CAST_HINT,
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
        Field::new("content", DataType::LargeBinary, false),
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

fn lix_error_to_datafusion_error(error: LixError) -> DataFusionError {
    crate::sql2::error::lix_error_to_datafusion_error(error)
}

fn file_id_row_pk(file_id: &str) -> Result<RowPk, LixError> {
    RowPk::uuid_from_canonical(file_id).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("validated file ID is not a canonical UUID: {error}"),
        )
    })
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
    use wasm_encoder::{ComponentBuilder, ComponentExportKind};

    use crate::binary_cas::{
        BlobBytesBatch, BlobDataReader, BlobId, BlobLayout, BlobWriteReceipt, ChunkHash,
    };
    use crate::branch::{BranchHead, BranchRefReader};
    use crate::changelog::{ChangeId, CommitId};
    use crate::common::LixTimestamp;
    use crate::filesystem::{
        FilesystemBlobRefKey, FilesystemDescriptorKey, FilesystemPathIndex,
        FilesystemPathIndexReader, FilesystemPathIndexRequest, FilesystemRowContext,
    };
    use crate::functions::FunctionProviderHandle;
    use crate::hot_state::{
        HotStateExactBatchRequest, HotStateFilter, HotStateReader, HotStateScanRequest,
        MaterializedHotStateBatch, MaterializedHotStateBatchBuilder, MaterializedHotStateRow,
    };
    use crate::plugin::runtime::UnsupportedWasmRuntime;
    use crate::plugin::runtime::{
        PLUGIN_OWNER_KEY, PLUGIN_REGISTRY_KEY, PluginContentMatcher, PluginFileOwner,
        PluginRegistry, PluginRegistryEntry, PluginRegistryEntryInput, PluginRuntime,
        PluginRuntimeHost, plugin_storage_archive_file_id, plugin_storage_archive_path,
    };
    use crate::sql2::dml::InsertSink;
    use crate::sql2::providers::upsert::UpsertConflictTarget;
    use crate::sql2::{SqlWriteContext, SqlWriteExecutionContext, WriteContextBranchRefReader};
    use crate::transaction_types::{
        TransactionJson, TransactionWrite, TransactionWriteMode, TransactionWriteOutcome,
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

    fn uuid_pk(value: &str) -> crate::row_pk::RowPk {
        crate::row_pk::RowPk::uuid_from_canonical(value)
            .expect("fixture ID should be a canonical UUID")
    }

    fn path_index_from_rows(
        rows: Vec<MaterializedHotStateRow>,
    ) -> Result<FilesystemPathIndex, LixError> {
        FilesystemPathIndex::from_live_batch(&MaterializedHotStateBatch::from_rows(rows))
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
                live_file_row(&id, "01920000-0000-7000-8000-0000000000b1", &snapshot)
            })
            .collect::<Vec<_>>();

        let mut samples = Vec::with_capacity(rounds);
        let mut heap_bytes = 0;
        match operation.as_str() {
            "lookup" => {
                let index = Arc::new(
                    path_index_from_rows(rows).expect("benchmark path index should build"),
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
                    let index =
                        path_index_from_rows(input).expect("benchmark path index should build");
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
            path_index_from_rows(vec![
                live_file_row(
                    "01920000-0000-7000-8000-000000000532",
                    "01920000-0000-7000-8000-0000000000b1",
                    r#"{"id":"01920000-0000-7000-8000-000000000532","directory_id":null,"name":"a.txt"}"#,
                ),
                live_file_row(
                    "01920000-0000-7000-8000-0000000000a2",
                    "01920000-0000-7000-8000-0000000000b1",
                    r#"{"id":"01920000-0000-7000-8000-0000000000a2","directory_id":null,"name":"z.txt"}"#,
                ),
                live_file_row(
                    "01920000-0000-7000-8000-000000000452",
                    "01920000-0000-7000-8000-0000000000b1",
                    r#"{"id":"01920000-0000-7000-8000-000000000452","directory_id":null,"name":"middle.txt"}"#,
                ),
            ])
            .expect("filesystem path index should build"),
        );
        let ids = BTreeSet::from([
            "01920000-0000-7000-8000-0000000000a2".to_string(),
            "01920000-0000-7000-8000-000000000532".to_string(),
        ]);

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
                vec![
                    string_literal("01920000-0000-7000-8000-0000000000b2"),
                    string_literal("01920000-0000-7000-8000-0000000000a2"),
                ],
                false,
            )))
            .unwrap()
            .unwrap();

        assert_eq!(
            constraint,
            super::FileIdConstraint::Ids(BTreeSet::from([
                "01920000-0000-7000-8000-0000000000a2".to_string(),
                "01920000-0000-7000-8000-0000000000b2".to_string()
            ]))
        );
        assert!(analyzer.supports(&eq_filter("id", "01920000-0000-7000-8000-0000000000a2")));
        assert!(analyzer.supports(&Expr::BinaryExpr(BinaryExpr::new(
            Box::new(string_literal("01920000-0000-7000-8000-0000000000a2")),
            Operator::Eq,
            Box::new(column("id")),
        ))));
    }

    #[test]
    fn file_id_filters_intersect_and_union_boolean_predicates() {
        let analyzer = super::LixFileIdFilterAnalyzer;
        let left = Expr::InList(InList::new(
            Box::new(column("id")),
            vec![
                string_literal("01920000-0000-7000-8000-0000000000a2"),
                string_literal("01920000-0000-7000-8000-0000000000b2"),
            ],
            false,
        ));
        let right = Expr::InList(InList::new(
            Box::new(column("id")),
            vec![
                string_literal("01920000-0000-7000-8000-0000000000b2"),
                string_literal("01920000-0000-7000-8000-0000000000c2"),
            ],
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
            super::FileIdConstraint::Ids(BTreeSet::from([
                "01920000-0000-7000-8000-0000000000b2".to_string()
            ]))
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
                "01920000-0000-7000-8000-0000000000a2".to_string(),
                "01920000-0000-7000-8000-0000000000b2".to_string(),
                "01920000-0000-7000-8000-0000000000c2".to_string()
            ]))
        );
    }

    #[test]
    fn file_id_filters_detect_contradictions() {
        let filters = vec![Expr::BinaryExpr(BinaryExpr::new(
            Box::new(eq_filter("id", "01920000-0000-7000-8000-0000000000a2")),
            Operator::And,
            Box::new(eq_filter("id", "01920000-0000-7000-8000-0000000000b2")),
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
            vec![string_literal("01920000-0000-7000-8000-0000000000a2")],
            true,
        ))));
    }

    #[test]
    fn plugin_archive_delete_target_requires_one_exact_canonical_path() {
        let path = "/.lix/plugins/plugin_sentinel.lixplugin";
        let file_id = plugin_storage_archive_file_id("plugin_sentinel");

        assert_eq!(
            super::exact_plugin_archive_delete_target_from_filters(&[eq_filter("path", path)])
                .unwrap(),
            Some("plugin_sentinel".to_string())
        );
        assert_eq!(
            super::exact_plugin_archive_delete_target_from_filters(&[eq_filter("id", &file_id)])
                .unwrap(),
            None,
            "a UUID does not embed the plugin key; uninstall routing requires the canonical path"
        );

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
        let id_filter = eq_filter("id", "01920000-0000-7000-8000-000000000482");
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
        let cast_data = Expr::Cast(Cast::new(Box::new(column("content")), DataType::Utf8));
        let function_data = scalar_function_expr("some_fn", vec![cast_data.clone()]);

        assert!(super::contains_column(&cast_data, "content"));
        assert!(super::contains_column(&function_data, "content"));
        assert!(!super::contains_column(&function_data, "path"));
    }

    #[test]
    fn scan_needs_data_finds_data_inside_filter_functions() {
        let schema = super::lix_file_schema();
        let projection = vec![schema.index_of("id").expect("id column")];
        let filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(scalar_function_expr(
                "octet_length",
                vec![column("content")],
            )),
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
        let hot_state_scans = Arc::new(AtomicUsize::new(0));
        let path_index_requests = Arc::new(AtomicUsize::new(0));
        let mut file = live_file_row(
            "01920000-0000-7000-8000-0000000000d2",
            "01920000-0000-7000-8000-0000000000b1",
            r#"{"id":"01920000-0000-7000-8000-0000000000d2","directory_id":"01920000-0000-7000-8000-0000000000d3","name":"readme.md"}"#,
        );
        file.metadata = Some(r#"{"source":"index"}"#.into());
        let index = Arc::new(
            path_index_from_rows(vec![
                live_directory_row(
                    "01920000-0000-7000-8000-0000000000d3",
                    "01920000-0000-7000-8000-0000000000b1",
                    r#"{"id":"01920000-0000-7000-8000-0000000000d3","parent_id":null,"name":"docs"}"#,
                ),
                file,
            ])
            .expect("filesystem path index should build"),
        );
        let spec = LixFileSpec::active_branch(
            "01920000-0000-7000-8000-0000000000b1",
            Arc::new(RejectingHotStateReader {
                scan_count: Arc::clone(&hot_state_scans),
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
        let batch = planned
            .source
            .load_single_batch()
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
        assert_eq!(string_value("id"), "01920000-0000-7000-8000-0000000000d2");
        assert_eq!(string_value("path"), "/docs/readme.md");
        assert_eq!(
            string_value("directory_id"),
            "01920000-0000-7000-8000-0000000000d3"
        );
        assert_eq!(string_value("name"), "readme.md");
        assert_eq!(
            string_value("lixcol_schema_key"),
            super::FILE_DESCRIPTOR_SCHEMA_KEY
        );
        assert_eq!(
            string_value("lixcol_commit_id"),
            CommitId::for_test_label("commit-01920000-0000-7000-8000-0000000000d2").to_string()
        );
        assert_eq!(string_value("lixcol_metadata"), r#"{"source":"index"}"#);
        assert_eq!(path_index_requests.load(Ordering::SeqCst), 1);
        assert_eq!(hot_state_scans.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn filter_free_descriptor_scan_pushes_projection_and_limit_into_path_selection() {
        let hot_state_scans = Arc::new(AtomicUsize::new(0));
        let path_index_requests = Arc::new(AtomicUsize::new(0));
        let index = Arc::new(
            path_index_from_rows(vec![
                live_file_row(
                    "01920000-0000-7000-8000-0000000000c2",
                    "01920000-0000-7000-8000-0000000000b1",
                    r#"{"id":"01920000-0000-7000-8000-0000000000c2","directory_id":null,"name":"c.txt"}"#,
                ),
                live_file_row(
                    "01920000-0000-7000-8000-0000000000a2",
                    "01920000-0000-7000-8000-0000000000b1",
                    r#"{"id":"01920000-0000-7000-8000-0000000000a2","directory_id":null,"name":"a.txt"}"#,
                ),
                live_file_row(
                    "01920000-0000-7000-8000-0000000000b2",
                    "01920000-0000-7000-8000-0000000000b1",
                    r#"{"id":"01920000-0000-7000-8000-0000000000b2","directory_id":null,"name":"b.txt"}"#,
                ),
            ])
            .expect("filesystem path index should build"),
        );
        let spec = LixFileSpec::active_branch(
            "01920000-0000-7000-8000-0000000000b1",
            Arc::new(RejectingHotStateReader {
                scan_count: Arc::clone(&hot_state_scans),
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
        let batch = planned
            .source
            .load_single_batch()
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
            "01920000-0000-7000-8000-0000000000a2"
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
        let batch = planned
            .source
            .load_single_batch()
            .await
            .expect("count-style descriptor scan should load");
        assert_eq!(batch.num_columns(), 0);
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(path_index_requests.load(Ordering::SeqCst), 2);
        assert_eq!(hot_state_scans.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn file_id_data_scan_uses_indexed_descriptor_and_blob_rows() {
        let data = b"readme contents".to_vec();
        let hot_state_requests = Arc::new(Mutex::new(Vec::new()));
        let path_index_requests = Arc::new(AtomicUsize::new(0));
        let index = Arc::new(
            path_index_from_rows(vec![
                live_file_row(
                    "01920000-0000-7000-8000-0000000000d2",
                    "01920000-0000-7000-8000-0000000000b1",
                    r#"{"id":"01920000-0000-7000-8000-0000000000d2","directory_id":null,"name":"readme.md"}"#,
                ),
                live_blob_ref_row(
                    "01920000-0000-7000-8000-0000000000d2",
                    "01920000-0000-7000-8000-0000000000b1",
                    "01920000-0000-7000-8000-0000000000d2",
                    &BlobId::from_content(&data).to_hex(),
                    data.len(),
                ),
            ])
            .expect("filesystem path index should build"),
        );
        let spec = LixFileSpec::active_branch(
            "01920000-0000-7000-8000-0000000000b1",
            Arc::new(RecordingHotStateReader {
                rows: Vec::new(),
                scan_requests: Arc::clone(&hot_state_requests),
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
        let projection = vec![spec.schema().index_of("content").expect("data column")];
        let filters = vec![eq_filter("id", "01920000-0000-7000-8000-0000000000d2")];

        let planned = spec
            .plan_scan(Some(&projection), &filters, None, &ExecutionProps::new())
            .await
            .expect("file-id data scan should plan");
        let batch = planned
            .source
            .load_single_batch()
            .await
            .expect("file-id data scan should load");

        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .expect("data column should be binary data");
        assert_eq!(values.value(0), data.as_slice());
        assert_eq!(path_index_requests.load(Ordering::SeqCst), 1);
        let requests = hot_state_requests
            .lock()
            .expect("live-state request mutex should not be poisoned");
        assert!(requests.is_empty());
    }

    #[tokio::test]
    async fn lower_path_contains_scan_loads_blob_rows_only_for_the_matching_find_files_projection()
    {
        let selected_data = b"selected contents".to_vec();
        let selected_blob_hash = BlobId::from_content(&selected_data).to_hex();
        let changelog_data = b"changelog contents".to_vec();
        let changelog_blob_hash = BlobId::from_content(&changelog_data).to_hex();
        let outside_data = b"outside contents".to_vec();
        let outside_blob_hash = BlobId::from_content(&outside_data).to_hex();
        let selected_change_id = ChangeId::for_test_label("selected-search-blob");
        let hot_state_requests = Arc::new(Mutex::new(Vec::new()));
        let mut selected_blob = live_blob_ref_row(
            "01920000-0000-7000-8000-0000000000e2",
            "01920000-0000-7000-8000-0000000000b1",
            "01920000-0000-7000-8000-0000000000e2",
            &selected_blob_hash,
            selected_data.len(),
        );
        selected_blob.change_id = Some(selected_change_id);
        let index = Arc::new(
            path_index_from_rows(vec![
                live_directory_row(
                    "01920000-0000-7000-8000-0000000000d3",
                    "01920000-0000-7000-8000-0000000000b1",
                    r#"{"id":"01920000-0000-7000-8000-0000000000d3","parent_id":null,"name":"Docs"}"#,
                ),
                live_directory_row(
                    "01920000-0000-7000-8000-000000000383",
                    "01920000-0000-7000-8000-0000000000b1",
                    r#"{"id":"01920000-0000-7000-8000-000000000383","parent_id":null,"name":"Other"}"#,
                ),
                live_file_row(
                    "01920000-0000-7000-8000-0000000000e2",
                    "01920000-0000-7000-8000-0000000000b1",
                    r#"{"id":"01920000-0000-7000-8000-0000000000e2","directory_id":"01920000-0000-7000-8000-0000000000d3","name":"README.md"}"#,
                ),
                live_file_row(
                    "01920000-0000-7000-8000-000000000432",
                    "01920000-0000-7000-8000-0000000000b1",
                    r#"{"id":"01920000-0000-7000-8000-000000000432","directory_id":"01920000-0000-7000-8000-0000000000d3","name":"changelog.md"}"#,
                ),
                live_file_row(
                    "01920000-0000-7000-8000-000000000492",
                    "01920000-0000-7000-8000-0000000000b1",
                    r#"{"id":"01920000-0000-7000-8000-000000000492","directory_id":"01920000-0000-7000-8000-000000000383","name":"README.md"}"#,
                ),
                selected_blob.clone(),
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
            vec!["01920000-0000-7000-8000-0000000000e2".to_string()],
            "the range and contains predicates should exclude both the local non-match and outside root",
        );

        let _hot_state: Arc<dyn HotStateReader> = Arc::new(RecordingHotStateReader {
            rows: vec![
                selected_blob,
                live_blob_ref_row(
                    "01920000-0000-7000-8000-000000000432",
                    "01920000-0000-7000-8000-0000000000b1",
                    "01920000-0000-7000-8000-000000000432",
                    &changelog_blob_hash,
                    changelog_data.len(),
                ),
                live_blob_ref_row(
                    "01920000-0000-7000-8000-000000000492",
                    "01920000-0000-7000-8000-0000000000b1",
                    "01920000-0000-7000-8000-000000000492",
                    &outside_blob_hash,
                    outside_data.len(),
                ),
            ],
            scan_requests: Arc::clone(&hot_state_requests),
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
        let _request = super::lix_file_scan_request(
            Some("01920000-0000-7000-8000-0000000000b1"),
            Some(&projected_schema),
            None,
        );
        let rows =
            super::scan_indexed_file_batch(&matches, true).expect("matching blob rows should load");
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

        let requests = hot_state_requests
            .lock()
            .expect("live-state request mutex should not be poisoned");
        assert!(requests.is_empty());
    }

    #[tokio::test]
    async fn file_directory_id_scan_uses_indexed_descriptors_and_blob_rows() {
        let data = b"docs contents".to_vec();
        let blob_hash = BlobId::from_content(&data).to_hex();
        let other_data = b"other contents".to_vec();
        let other_blob_hash = BlobId::from_content(&other_data).to_hex();
        let hot_state_requests = Arc::new(Mutex::new(Vec::new()));
        let path_index_requests = Arc::new(AtomicUsize::new(0));
        let index = Arc::new(
            path_index_from_rows(vec![
                live_directory_row(
                    "01920000-0000-7000-8000-0000000000d3",
                    "01920000-0000-7000-8000-0000000000b1",
                    r#"{"id":"01920000-0000-7000-8000-0000000000d3","parent_id":null,"name":"docs"}"#,
                ),
                live_file_row(
                    "01920000-0000-7000-8000-0000000000f2",
                    "01920000-0000-7000-8000-0000000000b1",
                    r#"{"id":"01920000-0000-7000-8000-0000000000f2","directory_id":"01920000-0000-7000-8000-0000000000d3","name":"readme.md"}"#,
                ),
                live_file_row(
                    "01920000-0000-7000-8000-000000000102",
                    "01920000-0000-7000-8000-0000000000b1",
                    r#"{"id":"01920000-0000-7000-8000-000000000102","directory_id":"01920000-0000-7000-8000-0000000000d3","name":"other.md"}"#,
                ),
                live_file_row(
                    "01920000-0000-7000-8000-000000000142",
                    "01920000-0000-7000-8000-0000000000b1",
                    r#"{"id":"01920000-0000-7000-8000-000000000142","directory_id":null,"name":"root.md"}"#,
                ),
                live_blob_ref_row(
                    "01920000-0000-7000-8000-0000000000f2",
                    "01920000-0000-7000-8000-0000000000b1",
                    "01920000-0000-7000-8000-0000000000f2",
                    &blob_hash,
                    data.len(),
                ),
                live_blob_ref_row(
                    "01920000-0000-7000-8000-000000000102",
                    "01920000-0000-7000-8000-0000000000b1",
                    "01920000-0000-7000-8000-000000000102",
                    &other_blob_hash,
                    other_data.len(),
                ),
            ])
            .expect("filesystem path index should build"),
        );
        let spec = LixFileSpec::active_branch(
            "01920000-0000-7000-8000-0000000000b1",
            Arc::new(RecordingHotStateReader {
                rows: Vec::new(),
                scan_requests: Arc::clone(&hot_state_requests),
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
        let filters = vec![eq_filter(
            "directory_id",
            "01920000-0000-7000-8000-0000000000d3",
        )];

        assert_eq!(
            spec.filter_pushdown(&filters[0]),
            TableProviderFilterPushDown::Exact
        );
        let planned = spec
            .plan_scan(Some(&projection), &filters, None, &ExecutionProps::new())
            .await
            .expect("directory-id scan should plan");
        let batch = planned
            .source
            .load_single_batch()
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
        let requests = hot_state_requests
            .lock()
            .expect("live-state request mutex should not be poisoned");
        assert!(requests.is_empty());
    }

    #[tokio::test]
    async fn exact_blob_batch_requires_resolved_branch_ids_without_scanning() {
        let scan_count = Arc::new(AtomicUsize::new(0));
        let hot_state: Arc<dyn HotStateReader> = Arc::new(RejectingHotStateReader {
            scan_count: Arc::clone(&scan_count),
        });
        let error = super::scan_exact_file_blob_batch(
            hot_state,
            &HotStateScanRequest::default(),
            &BTreeSet::from(["01920000-0000-7000-8000-0000000000a2".to_string()]),
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
            "01920000-0000-7000-8000-000000000112",
            // Path-index rows are already projected into the requested branch.
            "01920000-0000-7000-8000-0000000000b1",
            r#"{"id":"01920000-0000-7000-8000-000000000112","directory_id":null,"name":"global.md"}"#,
        );
        global_file.global = true;
        let mut untracked_file = live_file_row(
            "01920000-0000-7000-8000-000000000132",
            "01920000-0000-7000-8000-0000000000b1",
            r#"{"id":"01920000-0000-7000-8000-000000000132","directory_id":null,"name":"untracked.md"}"#,
        );
        untracked_file.untracked = true;
        let mut index_rows = vec![
            live_file_row(
                "01920000-0000-7000-8000-000000000122",
                "01920000-0000-7000-8000-0000000000b1",
                r#"{"id":"01920000-0000-7000-8000-000000000122","directory_id":null,"name":"tracked.md"}"#,
            ),
            global_file,
            untracked_file,
        ];
        index_rows.extend((0..30).map(|index| {
            let file_id = format!("01920000-0000-7000-8000-{index:012x}");
            live_file_row(
                &file_id,
                "01920000-0000-7000-8000-0000000000b1",
                &format!(r#"{{"id":"{file_id}","directory_id":null,"name":"padding-{index}.md"}}"#),
            )
        }));
        let mut tracked_blob = live_blob_ref_row(
            "01920000-0000-7000-8000-000000000122",
            "01920000-0000-7000-8000-0000000000b1",
            "01920000-0000-7000-8000-000000000122",
            &BlobId::from_content(&tracked_data).to_hex(),
            tracked_data.len(),
        );
        tracked_blob.change_id = Some(ChangeId::for_test_label("tracked-blob"));
        let mut global_blob = live_blob_ref_row(
            "01920000-0000-7000-8000-000000000112",
            // Path-index rows are already projected into the requested branch.
            "01920000-0000-7000-8000-0000000000b1",
            "01920000-0000-7000-8000-000000000112",
            &BlobId::from_content(&global_data).to_hex(),
            global_data.len(),
        );
        global_blob.global = true;
        global_blob.change_id = Some(ChangeId::for_test_label("global-blob"));
        let mut untracked_blob = live_blob_ref_row(
            "01920000-0000-7000-8000-000000000132",
            "01920000-0000-7000-8000-0000000000b1",
            "01920000-0000-7000-8000-000000000132",
            &BlobId::from_content(&untracked_data).to_hex(),
            untracked_data.len(),
        );
        untracked_blob.untracked = true;
        untracked_blob.change_id = Some(ChangeId::for_test_label("untracked-blob"));
        // A malformed `(row=01920000-0000-7000-8000-000000000122, file=different-file-id)` row must
        // never be fetched for the exact descriptor identity.
        let mut misplaced_blob = live_blob_ref_row(
            "01920000-0000-7000-8000-000000000122",
            "01920000-0000-7000-8000-0000000000b1",
            "different-file-id",
            &BlobId::from_content(&misplaced_data).to_hex(),
            misplaced_data.len(),
        );
        misplaced_blob.change_id = Some(ChangeId::for_test_label("misplaced-blob"));
        index_rows.extend([
            tracked_blob.clone(),
            global_blob.clone(),
            untracked_blob.clone(),
            misplaced_blob.clone(),
        ]);
        let index =
            Arc::new(path_index_from_rows(index_rows).expect("filesystem path index should build"));
        let matches =
            super::indexed_file_matches(Arc::clone(&index), &super::FilePathPredicate::All);
        let hot_state_requests = Arc::new(Mutex::new(Vec::new()));
        let _hot_state: Arc<dyn HotStateReader> = Arc::new(RecordingHotStateReader {
            rows: Vec::new(),
            scan_requests: Arc::clone(&hot_state_requests),
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
        let _request = super::lix_file_scan_request(
            Some("01920000-0000-7000-8000-0000000000b1"),
            Some(&projected_schema),
            None,
        );
        let rows =
            super::scan_indexed_file_batch(&matches, true).expect("matching blob rows should load");
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
        let requests = hot_state_requests
            .lock()
            .expect("live-state request mutex should not be poisoned");
        assert!(requests.is_empty());
    }

    #[tokio::test]
    async fn file_root_directory_scan_uses_indexed_descriptors_and_root_blob_rows() {
        let root_data = b"root contents".to_vec();
        let root_blob_hash = BlobId::from_content(&root_data).to_hex();
        let nested_data = b"nested contents".to_vec();
        let nested_blob_hash = BlobId::from_content(&nested_data).to_hex();
        let hot_state_requests = Arc::new(Mutex::new(Vec::new()));
        let path_index_requests = Arc::new(AtomicUsize::new(0));
        let index = Arc::new(
            path_index_from_rows(vec![
                live_directory_row(
                    "01920000-0000-7000-8000-0000000000d3",
                    "01920000-0000-7000-8000-0000000000b1",
                    r#"{"id":"01920000-0000-7000-8000-0000000000d3","parent_id":null,"name":"docs"}"#,
                ),
                live_file_row(
                    "01920000-0000-7000-8000-000000000462",
                    "01920000-0000-7000-8000-0000000000b1",
                    r#"{"id":"01920000-0000-7000-8000-000000000462","directory_id":"01920000-0000-7000-8000-0000000000d3","name":"readme.md"}"#,
                ),
                live_file_row(
                    "01920000-0000-7000-8000-000000000142",
                    "01920000-0000-7000-8000-0000000000b1",
                    r#"{"id":"01920000-0000-7000-8000-000000000142","directory_id":null,"name":"root.md"}"#,
                ),
                live_blob_ref_row(
                    "01920000-0000-7000-8000-000000000142",
                    "01920000-0000-7000-8000-0000000000b1",
                    "01920000-0000-7000-8000-000000000142",
                    &root_blob_hash,
                    root_data.len(),
                ),
                live_blob_ref_row(
                    "01920000-0000-7000-8000-000000000462",
                    "01920000-0000-7000-8000-0000000000b1",
                    "01920000-0000-7000-8000-000000000462",
                    &nested_blob_hash,
                    nested_data.len(),
                ),
            ])
            .expect("filesystem path index should build"),
        );
        let spec = LixFileSpec::active_branch(
            "01920000-0000-7000-8000-0000000000b1",
            Arc::new(RecordingHotStateReader {
                rows: Vec::new(),
                scan_requests: Arc::clone(&hot_state_requests),
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
        let batch = planned
            .source
            .load_single_batch()
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
        let requests = hot_state_requests
            .lock()
            .expect("live-state request mutex should not be poisoned");
        assert!(requests.is_empty());
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
            &BTreeSet::from([blob_ref_key(
                "01920000-0000-7000-8000-0000000000b1",
                false,
                false,
                "01920000-0000-7000-8000-0000000000d2",
            )]),
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
            columns.push("content");
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
        rows: Vec<MaterializedHotStateRow>,
        blob_bytes_by_hash: BTreeMap<BlobId, Vec<u8>>,
        writes: Vec<TransactionWrite>,
        scan_count: usize,
        path_index_count: usize,
        exact_load_requests: Vec<HotStateExactBatchRequest>,
    }

    struct IndexedFileContentUpdateWriteContext {
        index: Arc<FilesystemPathIndex>,
        blob_rows: Vec<MaterializedHotStateRow>,
        writes: Vec<TransactionWrite>,
        scan_requests: Arc<Mutex<Vec<HotStateScanRequest>>>,
        path_index_requests: Arc<AtomicUsize>,
    }

    struct StaticBlobReader {
        bytes_by_hash: BTreeMap<BlobId, Vec<u8>>,
    }

    struct ExactBlobReader {
        expected_hashes: Vec<BlobId>,
        bytes_by_hash: BTreeMap<BlobId, Vec<u8>>,
    }

    impl StaticBlobReader {
        fn from_blobs(blobs: impl IntoIterator<Item = Vec<u8>>) -> Self {
            Self {
                bytes_by_hash: blobs
                    .into_iter()
                    .map(|bytes| (BlobId::from_content(&bytes), bytes))
                    .collect(),
            }
        }
    }

    #[async_trait]
    impl BlobDataReader for CapturingWriteContext {
        async fn load_bytes_many(&self, hashes: &[BlobId]) -> Result<BlobBytesBatch, LixError> {
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
        async fn load_bytes_many(&self, hashes: &[BlobId]) -> Result<BlobBytesBatch, LixError> {
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
        async fn load_bytes_many(&self, hashes: &[BlobId]) -> Result<BlobBytesBatch, LixError> {
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
    impl SqlWriteExecutionContext for CapturingWriteContext {
        fn active_branch_id(&self) -> &str {
            "01920000-0000-7000-8000-0000000000b1"
        }

        fn functions(&self) -> FunctionProviderHandle {
            test_functions()
        }

        fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
            Ok(Vec::new().into())
        }

        async fn load_bytes_many(&mut self, hashes: &[BlobId]) -> Result<BlobBytesBatch, LixError> {
            BlobDataReader::load_bytes_many(self, hashes).await
        }

        async fn scan_hot_state_batch(
            &mut self,
            _request: &HotStateScanRequest,
        ) -> Result<MaterializedHotStateBatch, LixError> {
            self.scan_count += 1;
            Ok(MaterializedHotStateBatch::from_rows(self.rows.clone()))
        }

        async fn load_exact_hot_state_batch(
            &mut self,
            request: &HotStateExactBatchRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateExactBatch, LixError> {
            self.exact_load_requests.push(request.clone());
            Ok(crate::hot_state::MaterializedHotStateExactBatch::from_rows(
                request
                    .rows
                    .iter()
                    .map(|requested| {
                        let matches = |row: &&MaterializedHotStateRow| {
                            row.schema_key == requested.schema_key
                                && row.row_pk == requested.row_pk
                                && row.file_id == requested.file_id
                                && request
                                    .untracked
                                    .is_none_or(|untracked| row.untracked == untracked)
                        };
                        let mut row =
                            self.rows
                                .iter()
                                .filter(matches)
                                .find(|row| row.branch_id.as_ref() == requested.branch_id.as_str())
                                .or_else(|| {
                                    self.rows.iter().filter(matches).find(|row| {
                                        row.branch_id.as_ref() == crate::GLOBAL_BRANCH_ID
                                    })
                                })?
                                .clone();
                        if row.branch_id.as_ref() == crate::GLOBAL_BRANCH_ID
                            && requested.branch_id != crate::GLOBAL_BRANCH_ID
                        {
                            row.branch_id = requested.branch_id.clone().into();
                            row.global = true;
                        }
                        if row.deleted && !request.include_tombstones {
                            None
                        } else {
                            Some(row)
                        }
                    })
                    .collect(),
            ))
        }

        async fn filesystem_path_index(
            &mut self,
            _request: &FilesystemPathIndexRequest,
        ) -> Result<Arc<FilesystemPathIndex>, LixError> {
            self.path_index_count += 1;
            Ok(Arc::new(path_index_from_rows(self.rows.clone())?))
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

        async fn stage_typed_mutation_journal_replace(
            &mut self,
            _rows: crate::transaction_types::TypedMutationJournalBatch,
        ) -> Result<TransactionWriteOutcome, LixError> {
            Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "file provider test context does not stage transaction journals",
            ))
        }

        async fn can_stage_typed_mutation_journal_replace(
            &mut self,
            _schema_key: &str,
            _live_count: u64,
            _ordered_identity_digest: [u8; 32],
        ) -> Result<bool, LixError> {
            Ok(false)
        }
    }

    #[async_trait]
    impl SqlWriteExecutionContext for IndexedFileContentUpdateWriteContext {
        fn active_branch_id(&self) -> &str {
            "01920000-0000-7000-8000-0000000000b1"
        }

        fn functions(&self) -> FunctionProviderHandle {
            test_functions()
        }

        fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
            Ok(Vec::new().into())
        }

        async fn load_bytes_many(&mut self, hashes: &[BlobId]) -> Result<BlobBytesBatch, LixError> {
            Ok(BlobBytesBatch::new(vec![None; hashes.len()]))
        }

        async fn scan_hot_state_batch(
            &mut self,
            request: &HotStateScanRequest,
        ) -> Result<MaterializedHotStateBatch, LixError> {
            self.scan_requests
                .lock()
                .expect("scan request mutex should not be poisoned")
                .push(request.clone());
            Ok(MaterializedHotStateBatch::from_rows(self.blob_rows.clone()))
        }

        async fn load_exact_hot_state_batch(
            &mut self,
            request: &HotStateExactBatchRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateExactBatch, LixError> {
            Ok(crate::hot_state::MaterializedHotStateExactBatch::from_rows(
                request
                    .rows
                    .iter()
                    .map(|requested| {
                        self.blob_rows
                            .iter()
                            .find(|row| {
                                row.schema_key == requested.schema_key
                                    && row.row_pk == requested.row_pk
                                    && row.file_id == requested.file_id
                                    && row.branch_id.as_ref() == requested.branch_id.as_str()
                            })
                            .cloned()
                    })
                    .collect(),
            ))
        }

        async fn filesystem_path_index(
            &mut self,
            request: &FilesystemPathIndexRequest,
        ) -> Result<Arc<FilesystemPathIndex>, LixError> {
            assert_eq!(
                request.branch_ids,
                vec!["01920000-0000-7000-8000-0000000000b1".to_string()]
            );
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
                TransactionWrite::RowsWithFileContent { count, .. } => *count,
            };
            self.writes.push(write);
            Ok(TransactionWriteOutcome { count })
        }

        async fn stage_typed_mutation_journal_replace(
            &mut self,
            _rows: crate::transaction_types::TypedMutationJournalBatch,
        ) -> Result<TransactionWriteOutcome, LixError> {
            Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "indexed file test context does not stage transaction journals",
            ))
        }

        async fn can_stage_typed_mutation_journal_replace(
            &mut self,
            _schema_key: &str,
            _live_count: u64,
            _ordered_identity_digest: [u8; 32],
        ) -> Result<bool, LixError> {
            Ok(false)
        }
    }

    #[derive(Default)]
    struct RowsHotStateReader {
        rows: Vec<MaterializedHotStateRow>,
    }

    struct RejectingHotStateReader {
        scan_count: Arc<AtomicUsize>,
    }

    struct RecordingHotStateReader {
        rows: Vec<MaterializedHotStateRow>,
        scan_requests: Arc<Mutex<Vec<HotStateScanRequest>>>,
    }

    fn typed_fixture_batch(
        rows: impl IntoIterator<Item = MaterializedHotStateRow>,
    ) -> Result<MaterializedHotStateBatch, LixError> {
        let rows = rows.into_iter().collect::<Vec<_>>();
        let mut builder = MaterializedHotStateBatchBuilder::with_capacity(rows.len());
        for row in rows {
            let typed = match row.snapshot_content.as_deref() {
                Some(snapshot) if !row.deleted => {
                    let value: serde_json::Value =
                        serde_json::from_str(snapshot).map_err(|error| {
                            LixError::unknown(format!("invalid test live-row JSON: {error}"))
                        })?;
                    crate::plugin::runtime::WasmTypedRow::from_builtin_json(
                        &row.schema_key,
                        &row.row_pk,
                        &value,
                    )
                    .ok()
                    .map(Arc::new)
                }
                _ => None,
            };
            let ordinal = builder.len();
            builder.push_owned(row);
            builder.set_decoded_snapshot(ordinal, typed);
        }
        Ok(builder.finish())
    }

    #[async_trait]
    impl HotStateReader for RecordingHotStateReader {
        async fn scan_batch(
            &self,
            request: &HotStateScanRequest,
        ) -> Result<MaterializedHotStateBatch, LixError> {
            self.scan_requests
                .lock()
                .expect("live-state request mutex should not be poisoned")
                .push(request.clone());
            typed_fixture_batch(self.rows.clone())
        }

        async fn load_exact_batch(
            &self,
            request: &HotStateExactBatchRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateExactBatch, LixError> {
            let mut recorded = HotStateScanRequest {
                filter: HotStateFilter {
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
                    row_pks: request.rows.iter().map(|row| row.row_pk.clone()).collect(),
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
                    ..HotStateFilter::default()
                },
                projection: request.projection.clone(),
                limit: None,
            };
            recorded.filter.branch_ids.sort();
            recorded.filter.branch_ids.dedup();
            recorded.filter.schema_keys.sort();
            recorded.filter.schema_keys.dedup();
            recorded.filter.row_pks.sort();
            recorded.filter.row_pks.dedup();
            recorded
                .filter
                .file_ids
                .sort_by_key(|file_id| format!("{file_id:?}"));
            recorded.filter.file_ids.dedup();
            self.scan_requests
                .lock()
                .expect("live-state request mutex should not be poisoned")
                .push(recorded);

            Ok(crate::hot_state::MaterializedHotStateExactBatch::from_rows(
                request
                    .rows
                    .iter()
                    .map(|requested| {
                        let exact_match = |row: &&MaterializedHotStateRow| {
                            row.schema_key == requested.schema_key
                                && row.row_pk == requested.row_pk
                                && row.file_id == requested.file_id
                                && request
                                    .untracked
                                    .is_none_or(|untracked| row.untracked == untracked)
                        };
                        let mut row =
                            self.rows
                                .iter()
                                .filter(exact_match)
                                .find(|row| row.branch_id.as_ref() == requested.branch_id.as_str())
                                .or_else(|| {
                                    self.rows.iter().filter(exact_match).find(|row| {
                                        row.branch_id.as_ref() == crate::GLOBAL_BRANCH_ID
                                    })
                                })?
                                .clone();
                        if row.branch_id.as_ref() == crate::GLOBAL_BRANCH_ID
                            && requested.branch_id != crate::GLOBAL_BRANCH_ID
                        {
                            row.branch_id = requested.branch_id.clone().into();
                            row.global = true;
                        }
                        if row.deleted && !request.include_tombstones {
                            None
                        } else {
                            Some(row)
                        }
                    })
                    .collect(),
            ))
        }
    }

    #[async_trait]
    impl HotStateReader for RejectingHotStateReader {
        async fn load_exact_batch(
            &self,
            request: &HotStateExactBatchRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateExactBatch, LixError> {
            crate::hot_state::load_exact_batch_via_scan_for_test(self, request).await
        }

        async fn scan_batch(
            &self,
            _request: &HotStateScanRequest,
        ) -> Result<MaterializedHotStateBatch, LixError> {
            self.scan_count.fetch_add(1, Ordering::SeqCst);
            Err(LixError::unknown(
                "descriptor-only scan should not read live state",
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
            Ok(Vec::new().into())
        }
    }

    #[async_trait]
    impl HotStateReader for RowsHotStateReader {
        async fn load_exact_batch(
            &self,
            request: &HotStateExactBatchRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateExactBatch, LixError> {
            crate::hot_state::load_exact_batch_via_scan_for_test(self, request).await
        }

        async fn scan_batch(
            &self,
            _request: &HotStateScanRequest,
        ) -> Result<MaterializedHotStateBatch, LixError> {
            typed_fixture_batch(self.rows.clone())
        }
    }

    fn live_directory_row(
        row_pk: &str,
        branch_id: &str,
        snapshot_content: &str,
    ) -> MaterializedHotStateRow {
        MaterializedHotStateRow {
            row_pk: crate::row_pk::RowPk::uuid_from_canonical(row_pk)
                .expect("fixture directory ID should be a UUID"),
            schema_key: super::DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
            file_id: None,
            snapshot_content: Some(snapshot_content.into()),
            metadata: None,
            deleted: false,
            branch_id: branch_id.into(),
            change_id: Some(ChangeId::for_test_label(&format!("change-{row_pk}"))),
            commit_id: Some(CommitId::for_test_label(&format!("commit-{row_pk}"))),
            global: false,
            untracked: false,
            created_at: LixTimestamp::expect_parse("test created_at", "2026-04-23T00:00:00Z"),
            updated_at: LixTimestamp::expect_parse("test updated_at", "2026-04-23T01:00:00Z"),
        }
    }

    fn live_file_row(
        row_pk: &str,
        branch_id: &str,
        snapshot_content: &str,
    ) -> MaterializedHotStateRow {
        let typed_row_pk = if matches!(row_pk, PLUGIN_REGISTRY_KEY | PLUGIN_OWNER_KEY) {
            crate::row_pk::RowPk::single(row_pk)
        } else {
            crate::row_pk::RowPk::uuid_from_canonical(row_pk)
                .expect("fixture file ID should be a UUID")
        };
        MaterializedHotStateRow {
            row_pk: typed_row_pk,
            schema_key: super::FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
            file_id: Some(row_pk.to_string()),
            snapshot_content: Some(snapshot_content.into()),
            metadata: None,
            deleted: false,
            branch_id: branch_id.into(),
            change_id: Some(ChangeId::for_test_label(&format!("change-{row_pk}"))),
            commit_id: Some(CommitId::for_test_label(&format!("commit-{row_pk}"))),
            global: false,
            untracked: false,
            created_at: LixTimestamp::expect_parse("test created_at", "2026-04-23T00:00:00Z"),
            updated_at: LixTimestamp::expect_parse("test updated_at", "2026-04-23T01:00:00Z"),
        }
    }

    fn live_blob_ref_row(
        row_pk: &str,
        branch_id: &str,
        file_id: &str,
        blob_hash: &str,
        size_bytes: usize,
    ) -> MaterializedHotStateRow {
        let mut row = live_file_row(
            row_pk,
            branch_id,
            &format!(r#"{{"id":"{file_id}","blob_hash":"{blob_hash}","size_bytes":{size_bytes}}}"#),
        );
        row.schema_key = super::BLOB_REF_SCHEMA_KEY.to_string();
        row.file_id = Some(file_id.to_string());
        row
    }

    fn file_dml_rows() -> Vec<MaterializedHotStateRow> {
        vec![
            live_file_row(
                "01920000-0000-7000-8000-0000000000d2",
                "01920000-0000-7000-8000-0000000000b1",
                r#"{"id":"01920000-0000-7000-8000-0000000000d2","directory_id":null,"name":"readme.md"}"#,
            ),
            live_blob_ref_row(
                "01920000-0000-7000-8000-0000000000d2",
                "01920000-0000-7000-8000-0000000000b1",
                "01920000-0000-7000-8000-0000000000d2",
                &"0".repeat(64),
                5,
            ),
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
        test_plugin_registry_entry_with_content(key, path_glob, None, schema_key, wasm)
    }

    fn test_plugin_registry_entry_with_content(
        key: &str,
        path_glob: &str,
        content: Option<PluginContentMatcher>,
        schema_key: &str,
        wasm: &[u8],
    ) -> PluginRegistryEntry {
        let mut manifest = serde_json::json!({
            "entry": "plugin.wasm",
            "key": key,
            "file_match": { "path_glob": path_glob },
            "schemas": ["schema/plugin.json"],
        });
        if let Some(content) = content {
            manifest["file_match"]["content"] =
                serde_json::to_value(content).expect("plugin content type should serialize");
        }
        let manifest_json = manifest.to_string();
        PluginRegistryEntry::new(PluginRegistryEntryInput {
            key: key.to_string(),
            runtime: PluginRuntime::WasmComponent,
            api_version: "2.0.0".to_string(),
            capabilities: crate::plugin::runtime::PluginCapabilities {
                column_merger: false,
                file_projection: true,
            },
            path_glob: Some(path_glob.to_string()),
            content,
            entry: Some("plugin.wasm".to_string()),
            schema_keys: vec![schema_key.to_string()],
            create_schema_keys: Vec::new(),
            manifest_json,
            archive_file_id: plugin_storage_archive_file_id(key),
            archive_path: plugin_storage_archive_path(key),
            archive_blob_hash: BlobId::from_content(format!("archive-{key}").as_bytes()).to_hex(),
            wasm_blob_hash: Some(BlobId::from_content(wasm).to_hex()),
        })
        .expect("test plugin registry entry should be valid")
    }

    fn live_plugin_registry_row(
        branch_id: &str,
        entries: Vec<PluginRegistryEntry>,
    ) -> MaterializedHotStateRow {
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
        row.file_id = None;
        row
    }

    fn live_plugin_owner_row(
        branch_id: &str,
        file_id: &str,
        plugin_key: &str,
        schema_keys: Vec<String>,
    ) -> MaterializedHotStateRow {
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
        let wasm = file_projection_component();
        let manifest_json = format!(
            r#"{{
                "key": "plugin_sentinel",
                "file_match": {{ "path_glob": "{path_glob}" }},
                "entry": "plugin.wasm",
                "schemas": ["schema/plugin_note.json"]
            }}"#
        );
        let schema_json = format!(
            r#"{{
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "{schema_key}",
                "columns": [
                    {{ "name": "id", "type": "text", "nullable": false }},
                    {{ "name": "value", "type": "text", "nullable": false }}
                ],
                "primary_key": ["id"]
            }}"#
        );

        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        for (path, bytes) in [
            ("manifest.json", manifest_json.as_bytes()),
            ("schema/plugin_note.json", schema_json.as_bytes()),
            ("plugin.wasm", wasm.as_slice()),
        ] {
            writer.start_file(path, options).unwrap();
            writer.write_all(bytes).unwrap();
        }
        writer.finish().unwrap().into_inner()
    }

    fn file_projection_component() -> Vec<u8> {
        let mut component = ComponentBuilder::default();
        let empty = component.component(Some("capability"), ComponentBuilder::default());
        let instance = component.instantiate(
            Some("file-projection"),
            empty,
            std::iter::empty::<(&str, ComponentExportKind, u32)>(),
        );
        component.export(
            "lix:plugin/file-projection@2.0.0",
            ComponentExportKind::Instance,
            instance,
            None,
        );
        component.finish()
    }

    fn string_column(values: Vec<Option<&str>>) -> ArrayRef {
        Arc::new(StringArray::from(values)) as ArrayRef
    }

    fn file_insert_batch(global: bool) -> RecordBatch {
        let fields = vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("directory_id", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, false),
            Field::new("lixcol_global", DataType::Boolean, false),
            Field::new("lixcol_metadata", DataType::Utf8, true),
        ];
        let columns = vec![
            string_column(vec![Some("01920000-0000-7000-8000-0000000000d2")]),
            string_column(vec![Some("01920000-0000-7000-8000-0000000000d3")]),
            string_column(vec![Some("readme.md")]),
            Arc::new(BooleanArray::from(vec![global])) as ArrayRef,
            string_column(vec![Some("{\"source\":\"file\"}")]),
        ];
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("file insert batch")
    }

    fn data_insert_batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("directory_id", DataType::Utf8, true),
                Field::new("name", DataType::Utf8, false),
                Field::new("content", DataType::Binary, true),
            ])),
            vec![
                string_column(vec![Some("01920000-0000-7000-8000-0000000000d2")]),
                string_column(vec![Some("01920000-0000-7000-8000-0000000000d3")]),
                string_column(vec![Some("readme.md")]),
                Arc::new(BinaryArray::from_vec(vec![b"hello"])) as ArrayRef,
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
                Field::new("content", DataType::Binary, true),
            ])),
            vec![
                string_column(vec![Some("01920000-0000-7000-8000-0000000000d2")]),
                string_column(vec![Some(path)]),
                Arc::new(BinaryArray::from_vec(vec![data.as_slice()])) as ArrayRef,
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
                Field::new("content", DataType::Binary, true),
            ])),
            vec![
                string_column(vec![Some("01920000-0000-7000-8000-0000000000d2")]),
                string_column(vec![Some(path)]),
                Arc::new(BinaryArray::from_vec(vec![data])) as ArrayRef,
            ],
        )
        .expect("file path update batch")
    }

    fn data_update_batch_with_path(path: &str) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("path", DataType::Utf8, false),
                Field::new("content", DataType::Binary, true),
            ])),
            vec![
                string_column(vec![Some("01920000-0000-7000-8000-0000000000d2")]),
                string_column(vec![Some(path)]),
                Arc::new(BinaryArray::from_vec(vec![b"hello"])) as ArrayRef,
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
                Field::new("content", DataType::Binary, true),
            ])),
            vec![
                string_column(vec![Some("01920000-0000-7000-8000-0000000000d2")]),
                string_column(vec![Some("/old.raw")]),
                string_column(vec![Some("01920000-0000-7000-8000-0000000000d3")]),
                string_column(vec![Some("readme.md")]),
                Arc::new(BinaryArray::from_vec(vec![b"hello"])) as ArrayRef,
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
                Field::new("content", DataType::Binary, true),
                Field::new("lixcol_metadata", DataType::Utf8, true),
            ])),
            vec![
                string_column(vec![Some("01920000-0000-7000-8000-0000000000d2")]),
                string_column(vec![Some("/docs/readme.md")]),
                string_column(vec![Some("01920000-0000-7000-8000-0000000000d3")]),
                string_column(vec![Some("readme.md")]),
                Arc::new(BinaryArray::from_vec(vec![b"updated"])) as ArrayRef,
                string_column(vec![Some(r#"{"source":"upload"}"#)]),
            ],
        )
        .expect("file metadata data update batch")
    }

    fn empty_data_update_batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("content", DataType::Binary, true),
            ])),
            vec![
                string_column(vec![Some("01920000-0000-7000-8000-0000000000d2")]),
                Arc::new(BinaryArray::from_vec(vec![b""])) as ArrayRef,
            ],
        )
        .expect("empty file data update batch")
    }

    fn file_delete_batch() -> RecordBatch {
        file_delete_batch_with_path(None)
    }

    fn file_delete_batch_with_path(path: Option<&str>) -> RecordBatch {
        file_delete_batch_with_id_and_path("01920000-0000-7000-8000-0000000000d2", path)
    }

    fn file_delete_batch_with_id_and_path(file_id: &str, path: Option<&str>) -> RecordBatch {
        let mut fields = vec![Field::new("id", DataType::Utf8, false)];
        let mut columns = vec![string_column(vec![Some(file_id)])];
        if let Some(path) = path {
            fields.push(Field::new("path", DataType::Utf8, false));
            columns.push(string_column(vec![Some(path)]));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("file delete batch")
    }

    #[test]
    fn derives_nested_directory_paths() {
        let context = FilesystemRowContext::active_branch("01920000-0000-7000-8000-0000000000a1");
        let root = DirectoryDescriptorRecord {
            parent_id: None,
            name: "docs".to_string(),
            key: FilesystemDescriptorKey::from_context(
                &context,
                "01920000-0000-7000-8000-0000000000d3",
            ),
        };
        let child = DirectoryDescriptorRecord {
            parent_id: Some("01920000-0000-7000-8000-0000000000d3".to_string()),
            name: "guides".to_string(),
            key: FilesystemDescriptorKey::from_context(
                &context,
                "01920000-0000-7000-8000-000000000313",
            ),
        };
        let child_key = child.key.clone();
        let records = [root, child];
        let paths = derive_directory_paths(records.iter().map(|row| (row.key.clone(), row)))
            .expect("path derivation should succeed");

        assert_eq!(paths.get(&child_key), Some(&"/docs/guides".to_string()));
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
                "01920000-0000-7000-8000-0000000000d2",
                "01920000-0000-7000-8000-0000000000b1",
                "{\"id\":\"01920000-0000-7000-8000-0000000000d2\",\"directory_id\":\"missing-dir\",\"name\":\"readme.md\"}",
            )],
        )
        .await
        .expect_err("unresolved non-root directory_id should not project as root path");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
        assert!(error.message.contains("missing-dir"));
    }

    #[test]
    fn ten_thousand_file_payloads_append_into_one_aligned_write_batch() {
        const FILE_COUNT: usize = 10_000;
        let mut staged = super::LixFileStagedBatch::with_row_capacity(FILE_COUNT);
        let owner_pointers = staged.state_rows.aligned_owner_allocation_ptrs();
        let owner_capacities = staged.state_rows.aligned_owner_capacities();
        let file_content_pointer = staged.file_content_writes.as_ptr();
        let file_content_capacity = staged.file_content_writes.capacity();
        let context = FilesystemRowContext::active_branch("01920000-0000-7000-8000-000000002710");

        for index in 0..FILE_COUNT {
            let file_id = format!("01920000-0000-7000-8000-{index:012x}");
            super::stage_lix_file_content_insert_write(
                &mut staged,
                file_id.clone(),
                None,
                None,
                vec![u8::try_from(index % 251).expect("fixture byte")],
                context.clone(),
                Some(super::lix_file_insert_origin("lix_file", &file_id)),
            )
            .expect("file payload should append");
        }

        assert_eq!(staged.state_rows.len(), FILE_COUNT);
        assert_eq!(staged.file_content_writes.len(), FILE_COUNT);
        assert_eq!(
            staged.state_rows.aligned_owner_allocation_ptrs(),
            owner_pointers
        );
        assert_eq!(
            staged.state_rows.aligned_owner_capacities(),
            owner_capacities
        );
        assert_eq!(staged.file_content_writes.as_ptr(), file_content_pointer);
        assert_eq!(staged.file_content_writes.capacity(), file_content_capacity);
        assert_eq!(
            staged.state_rows.shared_string_count(),
            FILE_COUNT + 2,
            "row-cardinal file ids plus one schema and branch value should be dictionary-backed"
        );
        assert_eq!(staged.state_rows.shared_origin_count(), FILE_COUNT);
        assert_eq!(
            staged.state_rows.dictionary_promotion_counts(),
            [1, 1],
            "string and origin dictionaries should each promote only once"
        );
        let first = staged.state_rows.row(0);
        let last = staged.state_rows.row(FILE_COUNT - 1);
        assert_eq!(
            first.file_id.map(crate::common::SharedStr::as_str),
            Some("01920000-0000-7000-8000-000000000000")
        );
        assert_eq!(
            last.file_id.map(crate::common::SharedStr::as_str),
            Some("01920000-0000-7000-8000-00000000270f")
        );
        assert_eq!(
            first.origin.expect("first write origin").surface,
            "lix_file"
        );
        assert_eq!(last.origin.expect("last write origin").surface, "lix_file");
    }

    #[test]
    fn bulk_file_preparation_retains_one_batch_owner_and_compact_row_handles() {
        const FILE_COUNT: usize = 10_000;
        const ZERO_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";
        let branch_id = "01920000-0000-7000-8000-0000000000b1";
        let created_at = LixTimestamp::expect_parse("test created_at", "2026-04-23T00:00:00Z");
        let updated_at = LixTimestamp::expect_parse("test updated_at", "2026-04-23T01:00:00Z");
        let mut builder =
            MaterializedHotStateBatchBuilder::with_capacity(FILE_COUNT.saturating_mul(2));

        for index in 0..FILE_COUNT {
            let file_id = format!("01920000-0000-7000-8000-{index:012x}");
            let row_pk = uuid_pk(&file_id);
            builder.push_materialized_ref(
                &row_pk,
                super::FILE_DESCRIPTOR_SCHEMA_KEY,
                None,
                Some(
                    format!(
                        r#"{{"id":"{file_id}","directory_id":null,"name":"file-{index}.json"}}"#
                    )
                    .into(),
                ),
                None,
                false,
                created_at,
                updated_at,
                false,
                None,
                None,
                false,
                branch_id,
            );
            builder.push_materialized_ref(
                &row_pk,
                super::BLOB_REF_SCHEMA_KEY,
                Some(&file_id),
                Some(
                    format!(r#"{{"id":"{file_id}","blob_hash":"{ZERO_HASH}","size_bytes":1}}"#)
                        .into(),
                ),
                None,
                false,
                created_at,
                updated_at,
                false,
                None,
                None,
                false,
                branch_id,
            );
        }

        let batch = builder.finish();
        let row_column = batch.row_column_ptr();
        let prepared = super::prepare_lix_file_rows(batch, &super::FilePathPredicate::All)
            .expect("bulk descriptor/blob batch should prepare");

        assert_eq!(prepared.file_rows.len(), FILE_COUNT);
        assert_eq!(prepared.blob_rows.len(), FILE_COUNT);
        assert_eq!(prepared.live_rows.batches.len(), 1);
        assert_eq!(
            prepared.live_rows.batch(0).row_column_ptr(),
            row_column,
            "preparation should retain the source batch instead of rebuilding row DTOs"
        );
        assert!(
            prepared
                .file_rows
                .values()
                .all(|record| record.live.batch == 0)
        );
        assert!(
            prepared
                .blob_rows
                .values()
                .all(|record| record.live.batch == 0)
        );
    }

    #[test]
    fn indexed_file_preparation_retains_the_indexed_batch_in_all_profiles() {
        let file_id = "01920000-0000-7000-8000-0000000000e2";
        let branch_id = "01920000-0000-7000-8000-0000000000b1";
        let data = b"indexed contents";
        let blob_hash = BlobId::from_content(data);
        let index = Arc::new(
            path_index_from_rows(vec![
                live_file_row(
                    file_id,
                    branch_id,
                    r#"{"id":"01920000-0000-7000-8000-0000000000e2","directory_id":null,"name":"indexed.md"}"#,
                ),
                live_blob_ref_row(
                    file_id,
                    branch_id,
                    file_id,
                    &blob_hash.to_hex(),
                    data.len(),
                ),
            ])
            .expect("indexed file fixture should build"),
        );
        let matches =
            super::indexed_file_matches(Arc::clone(&index), &super::FilePathPredicate::All);
        let scanned =
            super::scan_indexed_file_batch(&matches, true).expect("indexed blob rows should scan");
        let prepared = super::prepare_indexed_lix_file_rows(&matches, scanned)
            .expect("indexed rows should prepare");

        assert_eq!(
            prepared.live_rows.batches.len(),
            2,
            "the scanned and rebuilt indexed batches must both be retained"
        );
        let file = prepared
            .file_rows
            .values()
            .next()
            .expect("indexed descriptor should be retained");
        let blob = prepared
            .blob_rows
            .values()
            .next()
            .expect("indexed blob reference should be retained");
        assert_eq!(file.live.batch, 1);
        assert_eq!(blob.live.batch, 1);
        assert_eq!(
            prepared.live_rows.row(file.live).schema_key(),
            super::FILE_DESCRIPTOR_SCHEMA_KEY
        );
        assert_eq!(
            prepared.live_rows.row(blob.live).schema_key(),
            super::BLOB_REF_SCHEMA_KEY
        );
    }

    #[tokio::test]
    async fn file_path_predicate_filters_before_blob_and_plugin_hydration() {
        let selected_data = b"selected".to_vec();
        let other_data = b"other".to_vec();
        let selected_hash = BlobId::from_content(&selected_data);
        let other_hash = BlobId::from_content(&other_data);
        let rows = vec![
            live_file_row(
                "01920000-0000-7000-8000-0000000000e2",
                "01920000-0000-7000-8000-0000000000b1",
                r#"{"id":"01920000-0000-7000-8000-0000000000e2","directory_id":null,"name":"selected.md"}"#,
            ),
            live_blob_ref_row(
                "01920000-0000-7000-8000-0000000000e2",
                "01920000-0000-7000-8000-0000000000b1",
                "01920000-0000-7000-8000-0000000000e2",
                &selected_hash.to_hex(),
                selected_data.len(),
            ),
            live_file_row(
                "01920000-0000-7000-8000-000000000482",
                "01920000-0000-7000-8000-0000000000b1",
                r#"{"id":"01920000-0000-7000-8000-000000000482","directory_id":null,"name":"other.md"}"#,
            ),
            live_blob_ref_row(
                "01920000-0000-7000-8000-000000000482",
                "01920000-0000-7000-8000-0000000000b1",
                "01920000-0000-7000-8000-000000000482",
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
            .column(batch.schema().index_of("content").unwrap())
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert_eq!(path_column.value(0), "/selected.md");
        assert_eq!(data_column.value(0), selected_data.as_slice());
    }

    #[test]
    fn file_path_predicate_only_discovers_plugins_for_selected_blobless_files() {
        let blob_data = b"stored".to_vec();
        let blob_hash = BlobId::from_content(&blob_data);
        let rows = vec![
            live_file_row(
                "01920000-0000-7000-8000-000000000512",
                "01920000-0000-7000-8000-0000000000b1",
                r#"{"id":"01920000-0000-7000-8000-000000000512","directory_id":null,"name":"stored.md"}"#,
            ),
            live_blob_ref_row(
                "01920000-0000-7000-8000-000000000512",
                "01920000-0000-7000-8000-0000000000b1",
                "01920000-0000-7000-8000-000000000512",
                &blob_hash.to_hex(),
                blob_data.len(),
            ),
            live_file_row(
                "01920000-0000-7000-8000-000000000502",
                "01920000-0000-7000-8000-0000000000b1",
                r#"{"id":"01920000-0000-7000-8000-000000000502","directory_id":null,"name":"rendered.md"}"#,
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
    async fn file_projection_sets_descriptor_file_id_to_own_id() {
        let blob_reader = Arc::new(CapturingWriteContext::default()) as Arc<dyn BlobDataReader>;
        let batch = super::lix_file_record_batch(
            &super::lix_file_schema(),
            &blob_reader,
            None,
            true,
            vec![live_file_row(
                "01920000-0000-7000-8000-0000000000d2",
                "01920000-0000-7000-8000-0000000000b1",
                "{\"id\":\"01920000-0000-7000-8000-0000000000d2\",\"directory_id\":null,\"name\":\"root.md\"}",
            )],
        )
        .await
        .expect("file descriptor should project");

        assert_eq!(batch.num_rows(), 1);
        let file_id_column = batch
            .column(batch.schema().index_of("lixcol_file_id").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("lixcol_file_id should be string array");
        assert_eq!(
            file_id_column.value(0),
            "01920000-0000-7000-8000-0000000000d2"
        );
    }

    #[tokio::test]
    async fn file_projection_matches_blob_ref_by_descriptor_file_id() {
        let data = b"shared data".to_vec();
        let blob_hash = BlobId::from_content(&data).to_hex();
        let blob_reader =
            Arc::new(StaticBlobReader::from_blobs(vec![data.clone()])) as Arc<dyn BlobDataReader>;
        let batch = super::lix_file_record_batch(
            &super::lix_file_schema(),
            &blob_reader,
            None,
            true,
            vec![
                live_file_row(
                    "01920000-0000-7000-8000-0000000000d2",
                    "01920000-0000-7000-8000-0000000000b1",
                    "{\"id\":\"01920000-0000-7000-8000-0000000000d2\",\"directory_id\":null,\"name\":\"root.md\"}",
                ),
                live_blob_ref_row(
                    "01920000-0000-7000-8000-0000000000d2",
                    "01920000-0000-7000-8000-0000000000b1",
                    "01920000-0000-7000-8000-0000000000d2",
                    &blob_hash,
                    data.len(),
                ),
            ],
        )
        .await
        .expect("blob ref should project data for its descriptor");

        let data_column = batch
            .column(batch.schema().index_of("content").unwrap())
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .expect("data should be large binary array");
        assert_eq!(batch.num_rows(), 1);
        for index in 0..batch.num_rows() {
            assert!(!data_column.is_null(index));
            assert_eq!(data_column.value(index), data.as_slice());
        }
    }

    #[tokio::test]
    async fn plugin_registry_catalogs_remain_branch_scoped() {
        let wasm = file_projection_component();
        let rows = vec![
            live_plugin_registry_row(
                "01920000-0000-7000-8000-0000000000a1",
                vec![test_plugin_registry_entry(
                    "plugin_sentinel",
                    "*.01920000-0000-7000-8000-0000000000a1",
                    "plugin_note_a",
                    &wasm,
                )],
            ),
            live_plugin_owner_row(
                "01920000-0000-7000-8000-0000000000a1",
                "01920000-0000-7000-8000-0000000000a2",
                "plugin_sentinel",
                vec!["plugin_note_a".to_string()],
            ),
            live_plugin_registry_row(
                "01920000-0000-7000-8000-0000000000b1",
                vec![test_plugin_registry_entry(
                    "plugin_sentinel",
                    "*.01920000-0000-7000-8000-0000000000b1",
                    "plugin_note_b",
                    &wasm,
                )],
            ),
            live_plugin_owner_row(
                "01920000-0000-7000-8000-0000000000b1",
                "01920000-0000-7000-8000-0000000000b2",
                "plugin_sentinel",
                vec!["plugin_note_b".to_string()],
            ),
        ];
        let prepared = super::prepare_lix_file_rows(
            vec![
                live_file_row(
                    "01920000-0000-7000-8000-0000000000a2",
                    "01920000-0000-7000-8000-0000000000a1",
                    r#"{"id":"01920000-0000-7000-8000-0000000000a2","directory_id":null,"name":"note.01920000-0000-7000-8000-0000000000a1"}"#,
                ),
                live_file_row(
                    "01920000-0000-7000-8000-0000000000b2",
                    "01920000-0000-7000-8000-0000000000b1",
                    r#"{"id":"01920000-0000-7000-8000-0000000000b2","directory_id":null,"name":"note.01920000-0000-7000-8000-0000000000b1"}"#,
                ),
            ],
            &super::FilePathPredicate::All,
        )
        .expect("plugin candidates should prepare");
        let context = super::plugin_render_context_for_lix_file_scan(
            Arc::new(RowsHotStateReader { rows }) as Arc<dyn HotStateReader>,
            &HotStateScanRequest {
                filter: HotStateFilter {
                    branch_ids: vec![
                        "01920000-0000-7000-8000-0000000000a1".to_string(),
                        "01920000-0000-7000-8000-0000000000b1".to_string(),
                    ],
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
                .branch("01920000-0000-7000-8000-0000000000a1")
                .and_then(|branch| branch
                    .catalog
                    .select_for_bytes("/note.01920000-0000-7000-8000-0000000000a1", b""))
                .map(PluginRegistryEntry::key),
            Some("plugin_sentinel")
        );
        assert!(
            context
                .branch("01920000-0000-7000-8000-0000000000a1")
                .and_then(|branch| branch
                    .catalog
                    .select_for_bytes("/note.01920000-0000-7000-8000-0000000000b1", b""))
                .is_none()
        );
        assert_eq!(
            context
                .branch("01920000-0000-7000-8000-0000000000b1")
                .and_then(|branch| branch
                    .catalog
                    .select_for_bytes("/note.01920000-0000-7000-8000-0000000000b1", b""))
                .map(PluginRegistryEntry::key),
            Some("plugin_sentinel")
        );
    }

    #[tokio::test]
    async fn missing_plugin_registry_checks_blobless_file_ownership() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let prepared = super::prepare_lix_file_rows(
            vec![live_file_row(
                "01920000-0000-7000-8000-000000000472",
                "01920000-0000-7000-8000-0000000000b1",
                r#"{"id":"01920000-0000-7000-8000-000000000472","directory_id":null,"name":"note.sentinel"}"#,
            )],
            &super::FilePathPredicate::All,
        )
        .expect("blobless file should prepare");
        let context = super::plugin_render_context_for_lix_file_scan(
            Arc::new(RecordingHotStateReader {
                rows: Vec::new(),
                scan_requests: Arc::clone(&requests),
            }) as Arc<dyn HotStateReader>,
            &HotStateScanRequest {
                filter: HotStateFilter {
                    branch_ids: vec!["01920000-0000-7000-8000-0000000000b1".to_string()],
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
            requests[0].filter.row_pks,
            vec![crate::row_pk::RowPk::single(PLUGIN_REGISTRY_KEY)]
        );
        assert_eq!(
            requests[0].filter.branch_ids,
            vec!["01920000-0000-7000-8000-0000000000b1"]
        );
        assert_eq!(requests[0].filter.file_ids, vec![NullableKeyFilter::Null]);
        assert_eq!(requests[0].filter.untracked, Some(false));
        assert_eq!(requests[0].limit, Some(1));
        assert_eq!(
            requests[1].filter.row_pks,
            vec![crate::row_pk::RowPk::single(PLUGIN_OWNER_KEY)]
        );
        assert_eq!(
            requests[1].filter.file_ids,
            vec![NullableKeyFilter::Value(
                "01920000-0000-7000-8000-000000000472".to_string()
            )]
        );
    }

    #[tokio::test]
    async fn installed_nonmatching_plugin_checks_blobless_file_ownership() {
        let wasm = file_projection_component();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let prepared = super::prepare_lix_file_rows(
            vec![live_file_row(
                "01920000-0000-7000-8000-000000000472",
                "01920000-0000-7000-8000-0000000000b1",
                r#"{"id":"01920000-0000-7000-8000-000000000472","directory_id":null,"name":"note.txt"}"#,
            )],
            &super::FilePathPredicate::All,
        )
        .expect("blobless raw file should prepare");
        let context = super::plugin_render_context_for_lix_file_scan(
            Arc::new(RecordingHotStateReader {
                rows: vec![live_plugin_registry_row(
                    "01920000-0000-7000-8000-0000000000b1",
                    vec![test_plugin_registry_entry(
                        "plugin_sentinel",
                        "*.sentinel",
                        "plugin_note",
                        &wasm,
                    )],
                )],
                scan_requests: Arc::clone(&requests),
            }) as Arc<dyn HotStateReader>,
            &HotStateScanRequest {
                filter: HotStateFilter {
                    branch_ids: vec!["01920000-0000-7000-8000-0000000000b1".to_string()],
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
            requests[0].filter.row_pks,
            vec![crate::row_pk::RowPk::single(PLUGIN_REGISTRY_KEY)]
        );
        assert_eq!(
            requests[1].filter.row_pks,
            vec![crate::row_pk::RowPk::single(PLUGIN_OWNER_KEY)]
        );
    }

    #[tokio::test]
    async fn blobless_owned_file_requires_its_installed_plugin() {
        let prepared = super::prepare_lix_file_rows(
            vec![live_file_row(
                "01920000-0000-7000-8000-000000000472",
                "01920000-0000-7000-8000-0000000000b1",
                r#"{"id":"01920000-0000-7000-8000-000000000472","directory_id":null,"name":"note.sentinel"}"#,
            )],
            &super::FilePathPredicate::All,
        )
        .expect("owned blobless file should prepare");
        let context = super::plugin_render_context_for_lix_file_scan(
            Arc::new(RowsHotStateReader {
                rows: vec![live_plugin_owner_row(
                    "01920000-0000-7000-8000-0000000000b1",
                    "01920000-0000-7000-8000-000000000472",
                    "plugin_sentinel",
                    vec!["plugin_note".to_string()],
                )],
            }) as Arc<dyn HotStateReader>,
            &HotStateScanRequest {
                filter: HotStateFilter {
                    branch_ids: vec!["01920000-0000-7000-8000-0000000000b1".to_string()],
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
    async fn path_update_uses_stale_owner_and_current_catalog() {
        let wasm = file_projection_component();
        let prepared = super::prepare_lix_file_rows(
            vec![live_file_row(
                "01920000-0000-7000-8000-0000000000d2",
                "01920000-0000-7000-8000-0000000000b1",
                r#"{"id":"01920000-0000-7000-8000-0000000000d2","directory_id":null,"name":"note.removed"}"#,
            )],
            &super::FilePathPredicate::All,
        )
        .expect("blobless file should prepare");
        let context = super::plugin_render_context_for_lix_file_scan(
            Arc::new(RowsHotStateReader {
                rows: vec![
                    live_plugin_registry_row(
                        "01920000-0000-7000-8000-0000000000b1",
                        vec![test_plugin_registry_entry(
                            "plugin_active",
                            "*.active",
                            "plugin_active_state",
                            &wasm,
                        )],
                    ),
                    live_plugin_owner_row(
                        "01920000-0000-7000-8000-0000000000b1",
                        "01920000-0000-7000-8000-0000000000d2",
                        "plugin_removed",
                        vec!["plugin_removed_state".to_string()],
                    ),
                ],
            }) as Arc<dyn HotStateReader>,
            &HotStateScanRequest {
                filter: HotStateFilter {
                    branch_ids: vec!["01920000-0000-7000-8000-0000000000b1".to_string()],
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
            Some("01920000-0000-7000-8000-0000000000b1"),
        )
        .expect("path ownership comparison should succeed");

        assert_eq!(
            rewritten,
            BTreeSet::from(["01920000-0000-7000-8000-0000000000d2".to_string()])
        );
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
                .branch("01920000-0000-7000-8000-0000000000b1")
                .and_then(|branch| branch.catalog.select_for_bytes("/note.active", b""))
                .map(PluginRegistryEntry::key),
            Some("plugin_active")
        );
    }

    #[tokio::test]
    async fn path_update_restages_same_owner_v2_for_descriptor_transition() {
        let wasm = file_projection_component();
        let prepared = super::prepare_lix_file_rows(
            vec![live_file_row(
                "01920000-0000-7000-8000-0000000000d2",
                "01920000-0000-7000-8000-0000000000b1",
                r#"{"id":"01920000-0000-7000-8000-0000000000d2","directory_id":null,"name":"before.csv"}"#,
            )],
            &super::FilePathPredicate::All,
        )
        .expect("blobless file should prepare");
        let context = super::plugin_render_context_for_lix_file_scan(
            Arc::new(RowsHotStateReader {
                rows: vec![
                    live_plugin_registry_row(
                        "01920000-0000-7000-8000-0000000000b1",
                        vec![test_plugin_registry_entry(
                            "plugin_csv",
                            "*.csv",
                            "csv_row",
                            &wasm,
                        )],
                    ),
                    live_plugin_owner_row(
                        "01920000-0000-7000-8000-0000000000b1",
                        "01920000-0000-7000-8000-0000000000d2",
                        "plugin_csv",
                        vec!["csv_row".to_string()],
                    ),
                ],
            }) as Arc<dyn HotStateReader>,
            &HotStateScanRequest {
                filter: HotStateFilter {
                    branch_ids: vec!["01920000-0000-7000-8000-0000000000b1".to_string()],
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
            Some("01920000-0000-7000-8000-0000000000b1"),
        )
        .expect("v2 descriptor transition should be selected");

        assert_eq!(
            rewritten,
            BTreeSet::from(["01920000-0000-7000-8000-0000000000d2".to_string()])
        );
    }

    #[tokio::test]
    async fn path_update_uses_materialized_data_for_content_matching() {
        let wasm = file_projection_component();
        let prepared = super::prepare_lix_file_rows(
            vec![live_file_row(
                "01920000-0000-7000-8000-0000000000d2",
                "01920000-0000-7000-8000-0000000000b1",
                r#"{"id":"01920000-0000-7000-8000-0000000000d2","directory_id":null,"name":"note.raw"}"#,
            )],
            &super::FilePathPredicate::All,
        )
        .expect("blobless file should prepare");
        let context = super::plugin_render_context_for_lix_file_scan(
            Arc::new(RowsHotStateReader {
                rows: vec![live_plugin_registry_row(
                    "01920000-0000-7000-8000-0000000000b1",
                    vec![test_plugin_registry_entry_with_content(
                        "plugin_text",
                        "*.active",
                        Some(PluginContentMatcher::Text),
                        "plugin_text_state",
                        &wasm,
                    )],
                )],
            }) as Arc<dyn HotStateReader>,
            &HotStateScanRequest {
                filter: HotStateFilter {
                    branch_ids: vec!["01920000-0000-7000-8000-0000000000b1".to_string()],
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
                BTreeSet::from(["01920000-0000-7000-8000-0000000000d2".to_string()]),
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
                Some("01920000-0000-7000-8000-0000000000b1"),
            )
            .expect("typed path ownership comparison should succeed");
            assert_eq!(rewritten, expected);
        }
    }

    #[test]
    fn decodes_file_insert_into_transaction_write_row() {
        let batch = file_insert_batch(false);

        let rows =
            lix_file_write_rows_from_batch(&batch, Some("01920000-0000-7000-8000-0000000000b1"))
                .expect("decode file insert");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].row_pk.as_ref(),
            Some(&uuid_pk("01920000-0000-7000-8000-0000000000d2"))
        );
        assert_eq!(rows[0].schema_key, "lix_file_descriptor");
        assert_eq!(rows[0].branch_id, "01920000-0000-7000-8000-0000000000b1");
        assert_eq!(
            rows[0].metadata.as_ref(),
            Some(&TransactionJson::from_value_for_test(
                serde_json::json!({"source": "file"})
            ))
        );
        let snapshot = rows[0].snapshot.as_ref().expect("descriptor snapshot JSON");
        assert_eq!(snapshot["id"], "01920000-0000-7000-8000-0000000000d2");
        assert_eq!(
            snapshot["directory_id"],
            "01920000-0000-7000-8000-0000000000d3"
        );
        assert_eq!(snapshot["name"], "readme.md");
    }

    #[test]
    fn active_file_insert_defaults_branch_id() {
        let batch = file_insert_batch(false);

        let rows =
            lix_file_write_rows_from_batch(&batch, Some("01920000-0000-7000-8000-0000000000a1"))
                .expect("decode file insert");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].branch_id, "01920000-0000-7000-8000-0000000000a1");
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
            Some("01920000-0000-7000-8000-0000000000b1"),
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
            Some("01920000-0000-7000-8000-0000000000b1"),
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
            Some("01920000-0000-7000-8000-0000000000b1"),
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
    fn file_content_update_rejects_invalid_existing_plugin_storage_path() {
        let error = lix_file_update_stage_from_batch_for_test(
            &data_update_batch_with_path("/.lix/plugins/nested/plugin_sentinel.lixplugin"),
            Some("01920000-0000-7000-8000-0000000000b1"),
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
            Some("01920000-0000-7000-8000-0000000000b1"),
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
                &plugin_storage_archive_file_id("plugin_sentinel"),
                Some("/.lix/plugins/plugin_sentinel.lixplugin"),
            ),
            Some("01920000-0000-7000-8000-0000000000b1"),
            &BTreeSet::new(),
            Some("plugin_sentinel"),
        )
        .expect("exact canonical plugin archive delete should stage");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.state_rows.len(), 1);
        let row = staged.state_rows.row(0);
        assert_eq!(row.schema_key, "lix_file_descriptor");
        assert_eq!(row.snapshot, None);
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
                Some("01920000-0000-7000-8000-0000000000b1"),
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
            super::filesystem_storage_scope_key(
                "01920000-0000-7000-8000-0000000000b1",
                false,
                false,
                None,
            ),
            super::DirectoryPathResolver::from_existing([(
                "/docs".to_string(),
                "01920000-0000-7000-8000-0000000000d3".to_string(),
            )])
            .expect("directory resolver should seed"),
        );

        let staged = lix_file_update_stage_from_batch_for_test(
            &path_update_batch(),
            Some("01920000-0000-7000-8000-0000000000b1"),
            super::LixFileUpdateColumns {
                data: false,
                descriptor: super::LixFileDescriptorUpdate::Path,
            },
            Some(&mut resolvers),
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect("decode file path update");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.file_content_writes.len(), 0);
        assert_eq!(staged.state_rows.len(), 1);
        let descriptor = staged
            .state_rows
            .iter()
            .find(|row| row.schema_key == "lix_file_descriptor")
            .expect("file descriptor row should be staged");
        let snapshot: JsonValue = descriptor
            .snapshot_json()
            .expect("file descriptor should carry JSON")
            .value()
            .clone();
        assert_eq!(snapshot["id"], "01920000-0000-7000-8000-0000000000d2");
        assert_eq!(
            snapshot["directory_id"],
            "01920000-0000-7000-8000-0000000000d3"
        );
        assert_eq!(snapshot["name"], "renamed.md");
    }

    #[test]
    fn file_path_update_preserves_existing_data_unless_data_is_assigned() {
        let mut resolvers = BTreeMap::new();
        resolvers.insert(
            super::filesystem_storage_scope_key(
                "01920000-0000-7000-8000-0000000000b1",
                false,
                false,
                None,
            ),
            super::DirectoryPathResolver::from_existing([(
                "/docs".to_string(),
                "01920000-0000-7000-8000-0000000000d3".to_string(),
            )])
            .expect("directory resolver should seed"),
        );

        let staged = lix_file_update_stage_from_batch_for_test(
            &path_update_batch(),
            Some("01920000-0000-7000-8000-0000000000b1"),
            super::LixFileUpdateColumns {
                data: false,
                descriptor: super::LixFileDescriptorUpdate::Path,
            },
            Some(&mut resolvers),
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect("decode file path update");

        assert!(
            staged.file_content_writes.is_empty(),
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
        let mut resolvers = super::directory_path_resolvers_from_hot_state(
            Arc::new(RowsHotStateReader {
                rows: vec![live_directory_row(
                    "01920000-0000-7000-8000-0000000000d3",
                    "01920000-0000-7000-8000-0000000000b1",
                    "{\"id\":\"01920000-0000-7000-8000-0000000000d3\",\"parent_id\":null,\"name\":\"docs\"}",
                )],
            }) as Arc<dyn HotStateReader>,
            Some("01920000-0000-7000-8000-0000000000b1"),
        )
        .await
        .expect("directory state should seed path resolver");

        let staged = lix_file_update_stage_from_batch_for_test(
            &path_update_batch(),
            Some("01920000-0000-7000-8000-0000000000b1"),
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

        let snapshot: JsonValue = staged
            .state_rows
            .row(0)
            .snapshot_json()
            .expect("file descriptor should carry JSON")
            .value()
            .clone();
        assert_eq!(
            snapshot["directory_id"],
            "01920000-0000-7000-8000-0000000000d3"
        );
        assert_eq!(snapshot["name"], "renamed.md");
    }

    #[tokio::test]
    async fn file_path_update_stages_only_missing_parent_directories() {
        let mut resolvers = super::directory_path_resolvers_from_hot_state(
            Arc::new(RowsHotStateReader::default()) as Arc<dyn HotStateReader>,
            Some("01920000-0000-7000-8000-0000000000b1"),
        )
        .await
        .expect("empty directory state should seed path resolver");

        let staged = lix_file_update_stage_from_batch_for_test(
            &path_update_batch(),
            Some("01920000-0000-7000-8000-0000000000b1"),
            super::LixFileUpdateColumns {
                data: false,
                descriptor: super::LixFileDescriptorUpdate::Path,
            },
            Some(&mut resolvers),
            &mut test_id_generator(&["01920000-0000-7000-8000-000000000353"]),
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
            directory.row_pk,
            Some(&uuid_pk("01920000-0000-7000-8000-000000000353"))
        );

        let descriptor = staged
            .state_rows
            .iter()
            .find(|row| row.schema_key == "lix_file_descriptor")
            .expect("file descriptor should be staged");
        let snapshot: JsonValue = descriptor
            .snapshot_json()
            .expect("file descriptor should carry JSON")
            .value()
            .clone();
        assert_eq!(
            snapshot["directory_id"],
            "01920000-0000-7000-8000-000000000353"
        );
    }

    #[test]
    fn file_path_update_with_data_assignment_stages_blob_ref_and_payload() {
        let mut resolvers = BTreeMap::new();
        resolvers.insert(
            super::filesystem_storage_scope_key(
                "01920000-0000-7000-8000-0000000000b1",
                false,
                false,
                None,
            ),
            super::DirectoryPathResolver::from_existing([(
                "/docs".to_string(),
                "01920000-0000-7000-8000-0000000000d3".to_string(),
            )])
            .expect("directory resolver should seed"),
        );

        let staged = lix_file_update_stage_from_batch_for_test(
            &path_update_batch(),
            Some("01920000-0000-7000-8000-0000000000b1"),
            super::LixFileUpdateColumns {
                data: true,
                descriptor: super::LixFileDescriptorUpdate::Path,
            },
            Some(&mut resolvers),
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect("decode file path and data update");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.file_content_writes.len(), 1);
        assert_eq!(
            staged.file_content_writes[0].file_id,
            "01920000-0000-7000-8000-0000000000d2"
        );
        assert_eq!(staged.file_content_writes[0].content(), b"hello");
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
            super::filesystem_storage_scope_key(
                "01920000-0000-7000-8000-0000000000b1",
                false,
                false,
                None,
            ),
            super::DirectoryPathResolver::from_existing([(
                "/docs".to_string(),
                "01920000-0000-7000-8000-0000000000d3".to_string(),
            )])
            .expect("directory resolver should seed"),
        );

        let staged = lix_file_update_stage_from_batch_for_test(
            &descriptor_data_update_batch(),
            Some("01920000-0000-7000-8000-0000000000b1"),
            super::LixFileUpdateColumns {
                data: true,
                descriptor: super::LixFileDescriptorUpdate::Topology,
            },
            Some(&mut resolvers),
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect("decode file descriptor and data update");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.file_content_writes.len(), 1);
        assert_eq!(
            staged.file_content_writes[0].file_id,
            "01920000-0000-7000-8000-0000000000d2"
        );
        assert_eq!(
            staged.file_content_writes[0].path.as_deref(),
            Some("/docs/readme.md")
        );
        assert_eq!(staged.file_content_writes[0].content(), b"hello");
        let blob_ref_row = staged
            .state_rows
            .iter()
            .find(|row| row.schema_key == "lix_binary_blob_ref")
            .expect("data update should stage blob ref row");
        let snapshot: serde_json::Value = blob_ref_row
            .snapshot_json()
            .expect("blob ref should carry snapshot")
            .value()
            .clone();
        assert_eq!(
            snapshot["blob_hash"].as_str(),
            staged.file_content_writes[0]
                .blob_hash()
                .map(BlobId::to_hex)
                .as_deref()
        );
    }

    #[test]
    fn file_metadata_data_update_reuses_materialized_path_without_resolver() {
        let batch = metadata_data_update_batch();
        let assignments = vec![
            literal_assignment("content", ScalarValue::Binary(Some(b"updated".to_vec()))),
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
                ScalarValue::Utf8(Some("01920000-0000-7000-8000-000000000383".to_string())),
            )]);
        assert!(
            structural_columns.requires_path_resolver(),
            "directory moves must retain resolver validation"
        );

        let assignment_values = super::UpdateAssignmentValues::from_batch_columns(
            &batch,
            &["content", "lixcol_metadata"],
        );
        let staged = super::lix_file_update_stage_from_batch(
            &batch,
            &assignment_values,
            Some("01920000-0000-7000-8000-0000000000b1"),
            update_columns,
            &BTreeSet::from([blob_ref_key(
                "01920000-0000-7000-8000-0000000000b1",
                false,
                false,
                "01920000-0000-7000-8000-0000000000d2",
            )]),
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
            descriptor.metadata.map(TransactionJson::value),
            Some(&serde_json::json!({"source": "upload"}))
        );
        assert_eq!(staged.file_content_writes.len(), 1);
        assert_eq!(
            staged.file_content_writes[0].path.as_deref(),
            Some("/docs/readme.md")
        );
        assert_eq!(staged.file_content_writes[0].content(), b"updated");
    }

    #[test]
    fn file_content_update_without_path_ignores_materialized_path_column() {
        let staged = lix_file_update_stage_from_batch_for_test(
            &path_update_batch(),
            Some("01920000-0000-7000-8000-0000000000b1"),
            super::LixFileUpdateColumns {
                data: true,
                descriptor: super::LixFileDescriptorUpdate::None,
            },
            None,
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect("decode file data update");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.file_content_writes.len(), 1);
        assert_eq!(
            staged.file_content_writes[0].file_id,
            "01920000-0000-7000-8000-0000000000d2"
        );
        assert_eq!(staged.state_rows.len(), 1);
        assert_eq!(staged.state_rows.row(0).schema_key, "lix_binary_blob_ref");
    }

    #[test]
    fn file_content_update_to_empty_ignores_blob_ref_in_other_scope() {
        let staged = lix_file_update_stage_from_batch_with_blob_keys_for_test(
            &empty_data_update_batch(),
            Some("01920000-0000-7000-8000-0000000000b1"),
            super::LixFileUpdateColumns {
                data: true,
                descriptor: super::LixFileDescriptorUpdate::None,
            },
            None,
            &mut test_id_generator(&["should-not-be-used"]),
            &BTreeSet::from([blob_ref_key(
                "01920000-0000-7000-8000-0000000000a1",
                false,
                false,
                "01920000-0000-7000-8000-0000000000d2",
            )]),
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

        let staged =
            lix_file_insert_stage_from_batch(&batch, Some("01920000-0000-7000-8000-0000000000b1"))
                .expect("decode file content");

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
            blob_ref_row.row_pk,
            Some(&uuid_pk("01920000-0000-7000-8000-0000000000d2"))
        );
        assert_eq!(
            blob_ref_row.file_id.map(|file_id| file_id.as_str()),
            Some("01920000-0000-7000-8000-0000000000d2")
        );
        assert_eq!(staged.file_content_writes.len(), 1);
        assert_eq!(
            staged.file_content_writes[0].file_id,
            "01920000-0000-7000-8000-0000000000d2"
        );
        assert_eq!(
            staged.file_content_writes[0].branch_id,
            "01920000-0000-7000-8000-0000000000b1"
        );
        assert_eq!(staged.file_content_writes[0].content(), b"hello");
        let snapshot: serde_json::Value = blob_ref_row
            .snapshot_json()
            .expect("blob ref should carry snapshot")
            .value()
            .clone();
        assert_eq!(
            snapshot["blob_hash"].as_str(),
            staged.file_content_writes[0]
                .blob_hash()
                .map(BlobId::to_hex)
                .as_deref()
        );
    }

    #[test]
    fn file_delete_with_blob_ref_stages_descriptor_and_blob_ref_tombstones() {
        let batch = file_delete_batch();
        let staged = lix_file_delete_stage_from_batch(
            &batch,
            Some("01920000-0000-7000-8000-0000000000b1"),
            &BTreeSet::from([blob_ref_key(
                "01920000-0000-7000-8000-0000000000b1",
                false,
                false,
                "01920000-0000-7000-8000-0000000000d2",
            )]),
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
            descriptor.row_pk,
            Some(&uuid_pk("01920000-0000-7000-8000-0000000000d2"))
        );
        assert_eq!(
            descriptor.file_id.map(crate::common::SharedStr::as_str),
            Some("01920000-0000-7000-8000-0000000000d2")
        );
        assert_eq!(descriptor.snapshot, None);

        let blob_ref = staged
            .state_rows
            .iter()
            .find(|row| row.schema_key == "lix_binary_blob_ref")
            .expect("blob ref tombstone should be staged");
        assert_eq!(
            blob_ref.row_pk,
            Some(&uuid_pk("01920000-0000-7000-8000-0000000000d2"))
        );
        assert_eq!(
            blob_ref.file_id.map(|file_id| file_id.as_str()),
            Some("01920000-0000-7000-8000-0000000000d2")
        );
        assert_eq!(blob_ref.snapshot, None);
    }

    #[test]
    fn file_delete_without_blob_ref_stages_only_descriptor_tombstone() {
        let batch = file_delete_batch();
        let staged = lix_file_delete_stage_from_batch(
            &batch,
            Some("01920000-0000-7000-8000-0000000000b1"),
            &BTreeSet::new(),
            None,
        )
        .expect("decode file delete");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.state_rows.len(), 1);
        let row = staged.state_rows.row(0);
        assert_eq!(row.schema_key, "lix_file_descriptor");
        assert_eq!(
            row.row_pk,
            Some(&uuid_pk("01920000-0000-7000-8000-0000000000d2"))
        );
        assert_eq!(row.snapshot, None);
    }

    #[test]
    fn file_delete_ignores_blob_ref_in_other_scope() {
        let batch = file_delete_batch();
        let staged = lix_file_delete_stage_from_batch(
            &batch,
            Some("01920000-0000-7000-8000-0000000000b1"),
            &BTreeSet::from([blob_ref_key(
                "01920000-0000-7000-8000-0000000000a1",
                false,
                false,
                "01920000-0000-7000-8000-0000000000d2",
            )]),
            None,
        )
        .expect("decode file delete");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.state_rows.len(), 1);
        assert_eq!(staged.state_rows.row(0).schema_key, "lix_file_descriptor");
    }

    #[test]
    fn file_path_insert_reuses_existing_parent_directory() {
        let mut resolvers = BTreeMap::new();
        resolvers.insert(
            super::filesystem_storage_scope_key(
                "01920000-0000-7000-8000-0000000000b1",
                false,
                false,
                None,
            ),
            super::DirectoryPathResolver::from_existing([
                (
                    "/docs".to_string(),
                    "01920000-0000-7000-8000-0000000000d3".to_string(),
                ),
                (
                    "/docs/guides".to_string(),
                    "01920000-0000-7000-8000-000000000313".to_string(),
                ),
            ])
            .expect("directory resolver should seed"),
        );

        let staged = lix_file_insert_stage_from_batch_with_path_resolvers(
            &path_data_insert_batch(),
            Some("01920000-0000-7000-8000-0000000000b1"),
            "lix_file",
            &mut resolvers,
            &mut test_id_generator(&["should-not-be-used"]),
            true,
        )
        .expect("decode file path data");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.file_content_writes.len(), 1);
        assert_eq!(
            staged.file_content_writes[0].file_id,
            "01920000-0000-7000-8000-0000000000d2"
        );
        assert_eq!(staged.state_rows.len(), 2);
        let descriptor = staged
            .state_rows
            .iter()
            .find(|row| row.schema_key == "lix_file_descriptor")
            .expect("file descriptor row should be staged");
        let snapshot: JsonValue = descriptor
            .snapshot_json()
            .expect("file descriptor should carry JSON")
            .value()
            .clone();
        assert_eq!(snapshot["id"], "01920000-0000-7000-8000-0000000000d2");
        assert_eq!(
            snapshot["directory_id"],
            "01920000-0000-7000-8000-000000000313"
        );
        assert_eq!(snapshot["name"], "readme.md");
    }

    #[test]
    fn file_path_insert_stages_missing_parent_directories_once() {
        let mut resolvers = BTreeMap::new();

        let staged = lix_file_insert_stage_from_batch_with_path_resolvers(
            &path_data_insert_batch(),
            Some("01920000-0000-7000-8000-0000000000b1"),
            "lix_file",
            &mut resolvers,
            &mut test_id_generator(&[
                "01920000-0000-7000-8000-000000000353",
                "dir-generated-guides",
            ]),
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
        let snapshot: JsonValue = descriptor
            .snapshot_json()
            .expect("file descriptor should carry JSON")
            .value()
            .clone();
        assert_eq!(snapshot["directory_id"], "dir-generated-guides");
    }

    #[tokio::test]
    async fn file_insert_sink_stages_decoded_transaction_rows() {
        let batch = file_insert_batch(false);
        let mut write_context = CapturingWriteContext::default();
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let sink = LixFileInsertSink::new(
            write_ctx,
            test_functions(),
            BranchBinding::active("01920000-0000-7000-8000-0000000000b1"),
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
                    rows.row(0).row_pk,
                    Some(&uuid_pk("01920000-0000-7000-8000-0000000000d2"))
                );
                assert_eq!(rows.row(0).schema_key, "lix_file_descriptor");
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
                "01920000-0000-7000-8000-000000000482",
                "01920000-0000-7000-8000-0000000000b1",
                r#"{"id":"01920000-0000-7000-8000-000000000482","directory_id":null,"name":"other.md"}"#,
            ),
            live_blob_ref_row(
                "01920000-0000-7000-8000-000000000482",
                "01920000-0000-7000-8000-0000000000b1",
                "01920000-0000-7000-8000-000000000482",
                &"1".repeat(64),
                7,
            ),
        ]);
        let mut write_context = CapturingWriteContext {
            rows,
            ..CapturingWriteContext::default()
        };
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let spec = file_dml_spec(write_ctx.clone());
        let planned = spec
            .plan_delete(
                write_ctx,
                &[eq_filter("id", "01920000-0000-7000-8000-0000000000d2")],
            )
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
            write_context.exact_load_requests[0].rows[0].row_pk,
            crate::row_pk::RowPk::uuid_from_canonical("01920000-0000-7000-8000-0000000000d2",)
                .expect("fixture file ID")
        );
        assert_eq!(
            write_context.exact_load_requests[0].rows[0]
                .file_id
                .as_deref(),
            Some("01920000-0000-7000-8000-0000000000d2")
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
        let row = rows.row(0);
        let snapshot = row
            .snapshot_json()
            .expect("updated snapshot should remain JSON")
            .value();
        assert_eq!(snapshot["name"], "README.md");
    }

    #[tokio::test]
    async fn file_update_exact_id_intersects_path_before_exact_blob_batch() {
        let mut rows = file_dml_rows();
        rows.push(live_file_row(
            "01920000-0000-7000-8000-000000000482",
            "01920000-0000-7000-8000-0000000000b1",
            r#"{"id":"01920000-0000-7000-8000-000000000482","directory_id":null,"name":"other.md"}"#,
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
                    eq_filter("id", "01920000-0000-7000-8000-0000000000d2"),
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
    async fn file_content_update_reuses_source_blob_ref_keys() {
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
                    "content",
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
        let TransactionWrite::RowsWithFileContent {
            rows, file_content, ..
        } = &write_context.writes[0]
        else {
            panic!("data update should stage rows and file data");
        };
        assert!(file_content[0].content().is_empty());
        assert!(file_content[0].had_blob_ref);
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
            literal_assignment("content", ScalarValue::Binary(Some(b"updated".to_vec()))),
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
        assert_eq!(staged.file_content.len(), 1);
        assert_eq!(
            staged.file_content[0].path.as_deref(),
            Some("/docs/readme.md")
        );
    }

    #[tokio::test]
    async fn file_id_conflict_probe_uses_path_index_and_exact_blob_batch() {
        let data = b"hello".to_vec();
        let blob_hash = BlobId::from_content(&data);
        let rows = vec![
            live_file_row(
                "01920000-0000-7000-8000-0000000000d2",
                "01920000-0000-7000-8000-0000000000b1",
                r#"{"id":"01920000-0000-7000-8000-0000000000d2","directory_id":null,"name":"readme.md"}"#,
            ),
            live_blob_ref_row(
                "01920000-0000-7000-8000-0000000000d2",
                "01920000-0000-7000-8000-0000000000b1",
                "01920000-0000-7000-8000-0000000000d2",
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
                &file_insert_batch(false),
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
    async fn fast_file_content_update_uses_path_index_and_target_blob_scan() {
        let path_index_requests = Arc::new(AtomicUsize::new(0));
        let scan_requests = Arc::new(Mutex::new(Vec::new()));
        let index = Arc::new(
            path_index_from_rows(vec![
                live_directory_row(
                    "01920000-0000-7000-8000-0000000000d3",
                    "01920000-0000-7000-8000-0000000000b1",
                    r#"{"id":"01920000-0000-7000-8000-0000000000d3","parent_id":null,"name":"docs"}"#,
                ),
                live_file_row(
                    "01920000-0000-7000-8000-0000000000d2",
                    "01920000-0000-7000-8000-0000000000b1",
                    r#"{"id":"01920000-0000-7000-8000-0000000000d2","directory_id":"01920000-0000-7000-8000-0000000000d3","name":"readme.md"}"#,
                ),
            ])
            .expect("filesystem path index should build"),
        );
        let old_data = b"old";
        let mut write_context = IndexedFileContentUpdateWriteContext {
            index,
            blob_rows: vec![live_blob_ref_row(
                "01920000-0000-7000-8000-0000000000d2",
                "01920000-0000-7000-8000-0000000000b1",
                "01920000-0000-7000-8000-0000000000d2",
                &BlobId::from_content(old_data).to_hex(),
                old_data.len(),
            )],
            writes: Vec::new(),
            scan_requests: Arc::clone(&scan_requests),
            path_index_requests: Arc::clone(&path_index_requests),
        };

        let count = super::execute_fast_lix_file_content_update_by_id(
            &mut write_context,
            Some("01920000-0000-7000-8000-0000000000d2".to_string()),
            b"new".to_vec().into(),
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
                requests[0].filter.row_pks,
                vec![uuid_pk("01920000-0000-7000-8000-0000000000d2")]
            );
        }

        let TransactionWrite::RowsWithFileContent { file_content, .. } = &write_context.writes[0]
        else {
            panic!("data update should stage file data");
        };
        assert_eq!(file_content.len(), 1);
        assert_eq!(file_content[0].path.as_deref(), Some("/docs/readme.md"));
        assert_eq!(file_content[0].content(), b"new");
        assert!(file_content[0].had_blob_ref);
        assert!(file_content[0].splice_provenance().is_none());

        let next: crate::Blob = b"next".as_slice().into();
        let provenance = crate::common::RequestBlobSpliceProvenance::new_validated_for_test(
            b"not",
            &next,
            1,
            1,
            b"ex".to_vec(),
        );
        let count = super::execute_fast_lix_file_content_update_by_id(
            &mut write_context,
            Some("01920000-0000-7000-8000-0000000000d2".to_string()),
            next,
            Some(provenance.clone()),
            Some(crate::common::MutationIdentity {
                namespace_seed: [7; 16],
                operation_proof: [17; 32],
            }),
        )
        .await
        .expect("fast spliced data update should stage");
        assert_eq!(count, 1);
        let TransactionWrite::RowsWithFileContent { file_content, .. } = &write_context.writes[1]
        else {
            panic!("spliced data update should stage file data");
        };
        assert_eq!(
            file_content[0].file_id,
            "01920000-0000-7000-8000-0000000000d2"
        );
        assert_eq!(file_content[0].content(), b"next");
        assert_eq!(file_content[0].splice_provenance(), Some(&provenance));
        assert_eq!(
            file_content[0].mutation_identity(),
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
                "01920000-0000-7000-8000-0000000000d2",
                "01920000-0000-7000-8000-0000000000b1",
                r#"{"id":"01920000-0000-7000-8000-0000000000d2","directory_id":null,"name":"readme.md"}"#,
            ),
            live_blob_ref_row(
                "01920000-0000-7000-8000-0000000000d2",
                "01920000-0000-7000-8000-0000000000b1",
                "01920000-0000-7000-8000-0000000000d2",
                &BlobId::from_content(old_data).to_hex(),
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
                b"new".to_vec().into(),
                Some(TransactionJson::from_value_for_test(
                    serde_json::json!({"source": "upload"}),
                )),
                None,
            )],
            super::FastLixFilePathWriteConflict::UpdateContentAndMetadata,
            None,
        )
        .await
        .expect("existing path upsert should stage");

        assert!(outcome.is_some());
        assert_eq!(write_context.path_index_count, 1);
        assert_eq!(write_context.exact_load_requests.len(), 1);
        assert_eq!(write_context.exact_load_requests[0].rows.len(), 1);
        assert_eq!(write_context.scan_count, 0);
        let TransactionWrite::RowsWithFileContent {
            rows, file_content, ..
        } = &write_context.writes[0]
        else {
            panic!("path upsert should stage descriptor, blob, and file data");
        };
        assert_eq!(file_content.len(), 1);
        assert_eq!(file_content[0].path.as_deref(), Some("/readme.md"));
        assert_eq!(file_content[0].content(), b"new");
        assert!(file_content[0].had_blob_ref);
        assert_eq!(
            file_content[0].base_blob_hash(),
            Some(BlobId::from_content(old_data)),
            "the exact indexed blob hash should be retained for optional CAS reuse",
        );
        let descriptor = rows
            .iter()
            .find(|row| row.schema_key == super::FILE_DESCRIPTOR_SCHEMA_KEY)
            .expect("metadata upsert should rewrite the descriptor");
        assert_eq!(
            descriptor.metadata,
            Some(&TransactionJson::from_value_for_test(
                serde_json::json!({"source": "upload"})
            ))
        );
    }

    #[tokio::test]
    async fn prepared_cas_path_write_is_first_class_transaction_content() {
        let old_payload = b"old media payload";
        let rows = vec![
            live_file_row(
                "01920000-0000-7000-8000-0000000000d2",
                "01920000-0000-7000-8000-0000000000b1",
                r#"{"id":"01920000-0000-7000-8000-0000000000d2","directory_id":null,"name":"proxy.mov"}"#,
            ),
            live_blob_ref_row(
                "01920000-0000-7000-8000-0000000000d2",
                "01920000-0000-7000-8000-0000000000b1",
                "01920000-0000-7000-8000-0000000000d2",
                &BlobId::from_content(old_payload).to_hex(),
                old_payload.len(),
            ),
        ];
        let mut write_context = CapturingWriteContext {
            rows,
            ..CapturingWriteContext::default()
        };
        let chunk_hash = ChunkHash::from_content(b"durable media chunk");
        let chunk_count = 4_096;
        let chunk_size = 1024 * 1024;
        let size_bytes = u64::from(chunk_count) * chunk_size;
        let blob_id = BlobId::from_chunks(
            size_bytes,
            (0..chunk_count).map(|_| (chunk_hash, chunk_size)),
        );
        let receipt = BlobWriteReceipt {
            hash: blob_id,
            size_bytes,
            layout: BlobLayout::Chunked { chunk_count },
        };

        let outcome = super::execute_fast_lix_file_prepared_path_write(
            &mut write_context,
            "/proxy.mov".to_string(),
            receipt,
        )
        .await
        .expect("prepared path write should stage");

        assert!(outcome.is_some());
        let TransactionWrite::RowsWithFileContent {
            rows, file_content, ..
        } = &write_context.writes[0]
        else {
            panic!("prepared path write should use ordinary file transaction rows");
        };
        assert_eq!(file_content.len(), 1);
        assert!(file_content[0].inline_data().is_none());
        assert_eq!(file_content[0].blob_hash(), Some(blob_id));
        assert_eq!(file_content[0].len(), size_bytes);
        let blob_ref = rows
            .iter()
            .find(|row| row.schema_key == super::BLOB_REF_SCHEMA_KEY)
            .expect("prepared path write should stage its final blob reference directly");
        let snapshot = blob_ref
            .snapshot_json()
            .expect("prepared blob ref should have a snapshot")
            .value();
        let blob_id_hex = blob_id.to_hex();
        assert_eq!(snapshot["blob_hash"].as_str(), Some(blob_id_hex.as_str()));
        assert_eq!(snapshot["size_bytes"].as_u64(), Some(size_bytes));
    }

    #[tokio::test]
    async fn fast_file_path_upsert_mixes_existing_and_missing_without_full_scan() {
        let old_data = b"old";
        let updated: crate::Blob = b"updated".as_slice().into();
        let existing_provenance =
            crate::common::RequestBlobSpliceProvenance::new_validated_for_test(
                old_data,
                &updated,
                0,
                0,
                updated.to_vec(),
            );
        let new_data: crate::Blob = b"new".as_slice().into();
        let missing_provenance = crate::common::RequestBlobSpliceProvenance::new_validated_for_test(
            b"",
            &new_data,
            0,
            0,
            new_data.to_vec(),
        );
        let rows = vec![
            live_file_row(
                "01920000-0000-7000-8000-0000000000d2",
                "01920000-0000-7000-8000-0000000000b1",
                r#"{"id":"01920000-0000-7000-8000-0000000000d2","directory_id":null,"name":"readme.md"}"#,
            ),
            live_blob_ref_row(
                "01920000-0000-7000-8000-0000000000d2",
                "01920000-0000-7000-8000-0000000000b1",
                "01920000-0000-7000-8000-0000000000d2",
                &BlobId::from_content(old_data).to_hex(),
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
                    updated,
                    None,
                    Some(existing_provenance.clone()),
                ),
                (
                    "/new.md".to_string(),
                    new_data,
                    None,
                    Some(missing_provenance.clone()),
                ),
            ],
            super::FastLixFilePathWriteConflict::UpdateContent,
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
        let TransactionWrite::RowsWithFileContent {
            rows, file_content, ..
        } = &write_context.writes[0]
        else {
            panic!("mixed path upsert should stage file data");
        };
        assert_eq!(file_content.len(), 2);
        assert!(file_content[0].had_blob_ref);
        assert!(!file_content[1].had_blob_ref);
        assert_eq!(
            file_content[0].splice_provenance(),
            Some(&existing_provenance)
        );
        assert_eq!(
            file_content[1].splice_provenance(),
            Some(&missing_provenance)
        );
        assert!(file_content.iter().all(|file_content| {
            file_content.mutation_identity()
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
                b"new".to_vec().into(),
                None,
                None,
            )],
            super::FastLixFilePathWriteConflict::UpdateContentAndMetadata,
            None,
        )
        .await
        .expect("nested missing path upsert should stage");

        assert!(outcome.is_some());
        assert_eq!(write_context.path_index_count, 1);
        assert!(write_context.exact_load_requests.is_empty());
        assert_eq!(write_context.scan_count, 0);
        let TransactionWrite::RowsWithFileContent {
            rows, file_content, ..
        } = &write_context.writes[0]
        else {
            panic!("nested path upsert should stage descriptors and file data");
        };
        assert_eq!(file_content.len(), 1);
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
                (
                    "/duplicate.md".to_string(),
                    b"first".to_vec().into(),
                    None,
                    None,
                ),
                (
                    "/duplicate.md".to_string(),
                    b"second".to_vec().into(),
                    None,
                    None,
                ),
            ],
            super::FastLixFilePathWriteConflict::UpdateContent,
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
                "01920000-0000-7000-8000-0000000000d3",
                "01920000-0000-7000-8000-0000000000b1",
                r#"{"id":"01920000-0000-7000-8000-0000000000d3","parent_id":null,"name":"docs"}"#,
            )],
            ..CapturingWriteContext::default()
        };

        let error = super::execute_fast_lix_file_path_writes(
            &mut write_context,
            vec![("/docs".to_string(), b"file".to_vec().into(), None, None)],
            super::FastLixFilePathWriteConflict::UpdateContent,
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
            "01920000-0000-7000-8000-000000000442",
            "01920000-0000-7000-8000-0000000000b1",
            r#"{"id":"01920000-0000-7000-8000-000000000442","directory_id":null,"name":"local.md"}"#,
        );
        let mut global_fallback = live_blob_ref_row(
            "01920000-0000-7000-8000-000000000442",
            crate::GLOBAL_BRANCH_ID,
            "01920000-0000-7000-8000-000000000442",
            &BlobId::from_content(b"global").to_hex(),
            6,
        );
        global_fallback.global = true;

        let mut global_descriptor = live_file_row(
            "01920000-0000-7000-8000-000000000112",
            "01920000-0000-7000-8000-0000000000b1",
            r#"{"id":"01920000-0000-7000-8000-000000000112","directory_id":null,"name":"global.md"}"#,
        );
        global_descriptor.global = true;
        let branch_override = live_blob_ref_row(
            "01920000-0000-7000-8000-000000000112",
            "01920000-0000-7000-8000-0000000000b1",
            "01920000-0000-7000-8000-000000000112",
            &BlobId::from_content(b"branch").to_hex(),
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
                (
                    "/local.md".to_string(),
                    b"new-local".to_vec().into(),
                    None,
                    None,
                ),
                (
                    "/global.md".to_string(),
                    b"new-global".to_vec().into(),
                    None,
                    None,
                ),
            ],
            super::FastLixFilePathWriteConflict::UpdateContent,
            None,
        )
        .await
        .expect("scope-isolated path upsert should stage");

        assert!(outcome.is_some());
        assert_eq!(write_context.path_index_count, 1);
        assert_eq!(
            write_context.exact_load_requests.len(),
            1,
            "blob-less scoped descriptors need only the blob-reference probe"
        );
        assert_eq!(write_context.exact_load_requests[0].rows.len(), 2);
        assert_eq!(write_context.scan_count, 0);
        let TransactionWrite::RowsWithFileContent {
            rows, file_content, ..
        } = &write_context.writes[0]
        else {
            panic!("scope-isolated path upsert should stage file data");
        };
        assert_eq!(file_content.len(), 2);
        assert!(file_content.iter().all(|write| !write.had_blob_ref));
        assert!(rows.iter().all(|row| row.snapshot.is_some()));
    }

    #[tokio::test]
    async fn fast_empty_file_path_upsert_loads_exact_prior_blob() {
        let old_data = b"old";
        let rows = vec![
            live_file_row(
                "01920000-0000-7000-8000-0000000000d2",
                "01920000-0000-7000-8000-0000000000b1",
                r#"{"id":"01920000-0000-7000-8000-0000000000d2","directory_id":null,"name":"readme.md"}"#,
            ),
            live_blob_ref_row(
                "01920000-0000-7000-8000-0000000000d2",
                "01920000-0000-7000-8000-0000000000b1",
                "01920000-0000-7000-8000-0000000000d2",
                &BlobId::from_content(old_data).to_hex(),
                old_data.len(),
            ),
        ];
        let mut write_context = CapturingWriteContext {
            rows,
            ..CapturingWriteContext::default()
        };

        let outcome = super::execute_fast_lix_file_path_writes(
            &mut write_context,
            vec![("/readme.md".to_string(), Vec::new().into(), None, None)],
            super::FastLixFilePathWriteConflict::UpdateContent,
            None,
        )
        .await
        .expect("empty existing path upsert should stage");

        assert!(outcome.is_some());
        assert_eq!(write_context.path_index_count, 1);
        assert_eq!(write_context.exact_load_requests.len(), 1);
        assert_eq!(write_context.exact_load_requests[0].rows.len(), 1);
        assert_eq!(write_context.scan_count, 0);
        let TransactionWrite::RowsWithFileContent {
            rows, file_content, ..
        } = &write_context.writes[0]
        else {
            panic!("empty path upsert should stage a blob tombstone and file data");
        };
        assert!(file_content[0].content().is_empty());
        assert!(file_content[0].had_blob_ref);
        assert!(
            rows.iter().any(|row| {
                row.schema_key == super::BLOB_REF_SCHEMA_KEY && row.snapshot.is_none()
            })
        );
    }

    #[tokio::test]
    async fn fast_file_path_write_declines_ambiguous_cross_scope_paths() {
        let tracked = live_file_row(
            "01920000-0000-7000-8000-000000000122",
            "01920000-0000-7000-8000-0000000000b1",
            r#"{"id":"01920000-0000-7000-8000-000000000122","directory_id":null,"name":"shared.md"}"#,
        );
        let mut untracked = live_file_row(
            "01920000-0000-7000-8000-000000000132",
            "01920000-0000-7000-8000-0000000000b1",
            r#"{"id":"01920000-0000-7000-8000-000000000132","directory_id":null,"name":"shared.md"}"#,
        );
        untracked.untracked = true;
        let mut write_context = CapturingWriteContext {
            rows: vec![tracked, untracked],
            ..CapturingWriteContext::default()
        };

        let outcome = super::execute_fast_lix_file_path_writes(
            &mut write_context,
            vec![("/shared.md".to_string(), b"new".to_vec().into(), None, None)],
            super::FastLixFilePathWriteConflict::UpdateContentAndMetadata,
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
    async fn file_insert_sink_stages_file_content_writes() {
        let batch = data_insert_batch();
        let mut write_context = CapturingWriteContext {
            rows: vec![live_directory_row(
                "01920000-0000-7000-8000-0000000000d3",
                "01920000-0000-7000-8000-0000000000b1",
                "{\"id\":\"01920000-0000-7000-8000-0000000000d3\",\"parent_id\":null,\"name\":\"docs\"}",
            )],
            ..CapturingWriteContext::default()
        };
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let sink = LixFileInsertSink::new(
            write_ctx,
            test_functions(),
            BranchBinding::active("01920000-0000-7000-8000-0000000000b1"),
            true,
        );

        let count = sink
            .write_batches(vec![batch], &Arc::new(TaskContext::default()))
            .await
            .expect("file insert sink should stage data");

        assert_eq!(count, 1);
        let writes = &write_context.writes;
        assert_eq!(writes.len(), 1);
        match &writes[0] {
            TransactionWrite::RowsWithFileContent {
                mode,
                rows,
                file_content,
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
                assert_eq!(file_content.len(), 1);
                assert_eq!(
                    file_content[0].file_id,
                    "01920000-0000-7000-8000-0000000000d2"
                );
                assert_eq!(file_content[0].path.as_deref(), Some("/docs/readme.md"));
                assert_eq!(file_content[0].content(), b"hello");
                assert!(!file_content[0].had_blob_ref);
            }
            other => panic!("expected insert with file data staged write, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn file_insert_sink_seeds_path_resolver_from_hot_state() {
        let batch = path_data_insert_batch();
        let mut write_context = CapturingWriteContext {
            rows: vec![
                live_directory_row(
                    "01920000-0000-7000-8000-0000000000d3",
                    "01920000-0000-7000-8000-0000000000b1",
                    "{\"id\":\"01920000-0000-7000-8000-0000000000d3\",\"parent_id\":null,\"name\":\"docs\"}",
                ),
                live_directory_row(
                    "01920000-0000-7000-8000-000000000313",
                    "01920000-0000-7000-8000-0000000000b1",
                    "{\"id\":\"01920000-0000-7000-8000-000000000313\",\"parent_id\":\"01920000-0000-7000-8000-0000000000d3\",\"name\":\"guides\"}",
                ),
            ],
            ..CapturingWriteContext::default()
        };
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let sink = LixFileInsertSink::new(
            write_ctx,
            test_functions(),
            BranchBinding::active("01920000-0000-7000-8000-0000000000b1"),
            true,
        );

        let count = sink
            .write_batches(vec![batch], &Arc::new(TaskContext::default()))
            .await
            .expect("file insert sink should stage path data");

        assert_eq!(count, 1);
        let writes = &write_context.writes;
        assert_eq!(writes.len(), 1);
        match &writes[0] {
            TransactionWrite::RowsWithFileContent {
                rows,
                file_content,
                count,
                ..
            } => {
                assert_eq!(*count, 1);
                assert_eq!(file_content.len(), 1);
                assert_eq!(
                    file_content[0].file_id,
                    "01920000-0000-7000-8000-0000000000d2"
                );
                let descriptor = rows
                    .iter()
                    .find(|row| row.schema_key == "lix_file_descriptor")
                    .expect("file descriptor row should be staged");
                let snapshot: JsonValue = descriptor
                    .snapshot_json()
                    .expect("file descriptor should carry JSON")
                    .value()
                    .clone();
                assert_eq!(
                    snapshot["directory_id"],
                    "01920000-0000-7000-8000-000000000313"
                );
            }
            other => panic!("expected insert with file data staged write, got {other:?}"),
        }
    }
}
