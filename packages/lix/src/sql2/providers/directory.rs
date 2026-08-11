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
use crate::hot_state::{HotStateFilter, HotStateProjection, HotStateReader, HotStateScanRequest};
use crate::hot_state::{
    MaterializedHotStateBatch, MaterializedHotStateRow, MaterializedHotStateRowRef,
};
use crate::plugin::runtime::{is_plugin_storage_path, reject_normal_plugin_storage_mutation};
use crate::sql2::branch_scope::{
    BranchBinding, explicit_branch_ids_from_dml_filters, resolve_provider_branch_ids,
    resolve_write_branch_scope,
};
use crate::sql2::predicate_typecheck::{
    canonicalize_json_identity_text_filters, validate_json_predicate_filters,
};
use crate::sql2::write_normalization::{
    InsertCell, SqlCell, UpdateAssignmentValues, defaultable_bool_insert_value,
    defaultable_text_insert_value, insert_column_is_omitted,
};
#[cfg(test)]
use crate::transaction_types::TransactionWriteRow;
use crate::transaction_types::{
    LogicalPrimaryKey, RawWriteBatch, RawWriteRowRef, TransactionJson, TransactionWriteOperation,
    TransactionWriteOrigin,
};
use crate::{
    GLOBAL_BRANCH_ID, LixError, SqlQueryResult, Value, parse_row_metadata_value,
    serialize_row_metadata,
};

use crate::filesystem::{
    DirectoryDescriptorWriteIntent, DirectoryPathRecord, DirectoryPathResolver,
    FilesystemDeletePlan, FilesystemDescriptorKey, FilesystemRowContext, VisibleFilesystem,
    create_directory_path_with_leaf_id_with_resolvers, derive_directory_paths,
    directory_path_resolvers_from_hot_state, directory_path_resolvers_from_path_index,
    filesystem_storage_scope_key, plan_parsed_directory_path_update_with_resolvers,
    plan_recursive_directory_delete,
};
use crate::sql2::result_metadata::json_field;
use crate::sql2::{SqlWriteContext, WriteAccess, WriteContextHotStateReader};
use crate::transaction_types::{TransactionWrite, TransactionWriteMode};

use super::file::{
    FileIdConstraint, FilePathPredicate, exact_string_column_constraint_from_filters,
    file_path_predicate_from_filters, indexed_path_matches,
};
use super::spec::{
    DmlReturning, InsertApply, PlannedDml, PlannedScan, RowSource, TableSpec, finish_scan_batch,
    projected_schema, register_spec_table, row_source, scan_row_source, take_record_batch_rows,
};
use super::upsert::{
    StagedUpsert, UpsertConflictKind, UpsertConflictTarget, UpsertReturningRow, UpsertSupport,
    materialize_omitted_column, materialize_omitted_insert_default, validate_target_columns,
};
use crate::row_pk::RowPk;

const DIRECTORY_SCHEMA_KEY: &str = "lix_directory_descriptor";

/// Physical-identity column the upsert driver matches conflicting rows on.
/// A directory's identity is its `id`; the underlying live state keys on the
/// directory id as a single-element row primary key.
const LIX_DIRECTORY_IDENTITY: &[&str] = &["id"];
const LIX_DIRECTORY_PATH_IDENTITY: &[&str] = &["path"];
const LIX_DIRECTORY_BY_BRANCH_PATH_IDENTITY: &[&str] = &["path", "lixcol_branch_id"];

/// Executes the exact root-directory listing used by the filesystem API
/// directly from the shared path index.
pub(crate) async fn execute_exact_lix_directory_root_listing(
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
    let matches = indexed_path_matches(
        index,
        &FilePathPredicate::All,
        FilesystemPathKind::Directory,
    );
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
            vec![
                Value::Text(entry.id().to_string()),
                Value::Text(entry.path.clone()),
                Value::Text(entry.name.clone()),
                Value::Text(entry.updated_at().to_string()),
            ]
        })
        .collect();
    Ok(SqlQueryResult {
        columns: vec![
            "id".to_string(),
            "path".to_string(),
            "name".to_string(),
            "lixcol_updated_at".to_string(),
        ],
        rows,
        notices: Vec::new(),
    })
}

pub(super) async fn register_lix_directory_active_provider(
    session: &SessionContext,
    surface_name: &str,
    active_branch_id: &str,
    hot_state: Arc<dyn HotStateReader>,
    filesystem_path_index: Arc<dyn FilesystemPathIndexReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    functions: FunctionProviderHandle,
) -> Result<(), LixError> {
    register_spec_table(
        session,
        surface_name,
        Arc::new(LixDirectorySpec::active_branch(
            active_branch_id,
            hot_state,
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
    hot_state: Arc<dyn HotStateReader>,
    filesystem_path_index: Arc<dyn FilesystemPathIndexReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    functions: FunctionProviderHandle,
) -> Result<(), LixError> {
    register_spec_table(
        session,
        surface_name,
        Arc::new(LixDirectorySpec::by_branch(
            hot_state,
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
    let hot_state = Arc::new(WriteContextHotStateReader::new(write_ctx.clone()));
    let filesystem_path_index: Arc<dyn FilesystemPathIndexReader> = hot_state.clone();
    register_spec_table(
        session,
        surface_name,
        Arc::new(LixDirectorySpec::by_branch(
            hot_state,
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
    let hot_state = Arc::new(WriteContextHotStateReader::new(write_ctx.clone()));
    let filesystem_path_index: Arc<dyn FilesystemPathIndexReader> = hot_state.clone();
    register_spec_table(
        session,
        surface_name,
        Arc::new(LixDirectorySpec::active_branch(
            active_branch_id,
            hot_state,
            filesystem_path_index,
            branch_ref,
            functions,
        )),
        WriteAccess::write(write_ctx),
    )
}

#[derive(Clone)]
struct LixDirectorySpec {
    schema: SchemaRef,
    hot_state: Arc<dyn HotStateReader>,
    filesystem_path_index: Arc<dyn FilesystemPathIndexReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    functions: FunctionProviderHandle,
    branch_binding: BranchBinding,
}

/// Stable public identity for a directory post-image. By-branch surfaces need
/// both components; active surfaces operate on one visible branch scope and
/// intentionally use an empty branch discriminator.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DirectoryReturningKey {
    id: String,
    branch_id: String,
}

impl LixDirectorySpec {
    async fn indexed_path_matches(
        &self,
        request: &HotStateScanRequest,
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
        hot_state: Arc<dyn HotStateReader>,
        filesystem_path_index: Arc<dyn FilesystemPathIndexReader>,
        branch_ref: Arc<dyn BranchRefReader>,
        functions: FunctionProviderHandle,
    ) -> Self {
        Self {
            schema: lix_directory_schema(),
            hot_state,
            filesystem_path_index,
            branch_ref,
            functions,
            branch_binding: BranchBinding::active(active_branch_id),
        }
    }

    fn by_branch(
        hot_state: Arc<dyn HotStateReader>,
        filesystem_path_index: Arc<dyn FilesystemPathIndexReader>,
        branch_ref: Arc<dyn BranchRefReader>,
        functions: FunctionProviderHandle,
    ) -> Self {
        Self {
            schema: lix_directory_by_branch_schema(),
            hot_state,
            filesystem_path_index,
            branch_ref,
            functions,
            branch_binding: BranchBinding::explicit(),
        }
    }

    /// Build the resolver set used by path-based directory writes from the
    /// descriptor index.  The transaction write context serves its revisioned
    /// cache until filesystem descriptors are staged, then safely rebuilds an
    /// overlay-aware index, so this has the same transaction visibility as the
    /// previous live-state scan.
    async fn path_resolvers_for_write(
        &self,
        write_ctx: &SqlWriteContext,
    ) -> Result<BTreeMap<String, DirectoryPathResolver>> {
        let branch_ids = self
            .branch_binding
            .active_branch_id()
            .map(|branch_id| vec![branch_id.to_string()])
            .unwrap_or_default();
        let index = self
            .filesystem_path_index
            .path_index(&FilesystemPathIndexRequest::new(branch_ids))
            .await;
        match index {
            Ok(index) => directory_path_resolvers_from_path_index(
                index.as_ref(),
                self.branch_binding.active_branch_id(),
            )
            .map_err(lix_error_to_datafusion_error),
            // The index includes files as well as directories. If building it
            // fails for an unrelated malformed file descriptor, retain the
            // previous directory-write behavior rather than failing a write
            // that the directory-only live-state resolver can still plan.
            Err(_) => directory_path_resolvers_from_hot_state(
                Arc::new(WriteContextHotStateReader::new(write_ctx.clone())),
                self.branch_binding.active_branch_id(),
            )
            .await
            .map_err(lix_error_to_datafusion_error),
        }
    }

    /// Resolve the candidate-row scan request for an UPDATE/DELETE, scoped by
    /// the explicit branch ids from the statement filters.
    async fn dml_scan_request(&self, filters: &[Expr]) -> Result<HotStateScanRequest> {
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
        request: HotStateScanRequest,
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
                            .scan_hot_state_batch(&request)
                            .await
                            .map_err(lix_error_to_datafusion_error)?;
                        let batch = lix_directory_record_batch(&table_schema, &rows)
                            .map_err(lix_error_to_datafusion_error)?;
                        (batch.clone(), batch)
                    };
                *captured.lock().expect("dml source mutex poisoned") = Some(all_directories_batch);
                Ok(source_batch)
            },
        )
    }

    fn returning_key_from_batch(
        &self,
        batch: &RecordBatch,
        row_index: usize,
    ) -> Result<DirectoryReturningKey> {
        Ok(DirectoryReturningKey {
            id: required_string_value(batch, row_index, "id")?,
            branch_id: match self.branch_binding {
                BranchBinding::Active { .. } => String::new(),
                BranchBinding::Explicit => {
                    required_string_value(batch, row_index, "lixcol_branch_id")?
                }
            },
        })
    }

    fn materialize_returning_insert_defaults(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        if !insert_column_is_omitted(batch, "id") {
            return Ok(batch.clone());
        }
        let ids = (0..batch.num_rows())
            .map(|_| Some(self.functions.call_uuid_v7().to_string()))
            .collect::<StringArray>();
        materialize_omitted_insert_default(batch, "id", Arc::new(ids))
    }

    /// Reload the just-staged rows from the transaction overlay. Filesystem
    /// paths and audit fields are derived during staging, so applying SQL
    /// assignments to the pre-image would produce stale `RETURNING *` values.
    /// Scan the complete directory graph for the relevant branch scope, then
    /// select the requested identities in write-row order.
    async fn returning_post_image(
        &self,
        write_ctx: &SqlWriteContext,
        keys: &[DirectoryReturningKey],
    ) -> Result<RecordBatch> {
        if keys.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }
        let mut request = lix_directory_scan_request(
            self.branch_binding.active_branch_id(),
            Some(self.schema.as_ref()),
            None,
        );
        if matches!(self.branch_binding, BranchBinding::Explicit) {
            request.filter.branch_ids = keys
                .iter()
                .map(|key| key.branch_id.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
        }
        let rows = write_ctx
            .scan_hot_state_batch(&request)
            .await
            .map_err(lix_error_to_datafusion_error)?;
        let batch = lix_directory_record_batch(&self.schema, &rows)
            .map_err(lix_error_to_datafusion_error)?;
        let mut post_rows = BTreeMap::new();
        for row_index in 0..batch.num_rows() {
            let key = self.returning_key_from_batch(&batch, row_index)?;
            let index = u32::try_from(row_index).map_err(|_| {
                DataFusionError::Execution("lix_directory RETURNING row index overflow".into())
            })?;
            if post_rows.insert(key.clone(), index).is_some() {
                return Err(DataFusionError::Execution(format!(
                    "lix_directory RETURNING post-image contains duplicate row for id '{}'",
                    key.id
                )));
            }
        }
        let indices = keys
            .iter()
            .map(|key| {
                post_rows.get(key).copied().ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "lix_directory RETURNING post-image is missing inserted or updated row '{}'",
                        key.id
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        take_record_batch_rows(&batch, &indices)
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
        let target_parent_ids = exact_string_column_constraint_from_filters(&filters, "parent_id")?;
        let root_parent_filter = filters
            .iter()
            .any(|filter| is_null_column_filter(filter, "parent_id"));
        let mut indexed_matches = self
            .indexed_path_matches(&request, &filters)
            .await?
            .map(|(selected, _)| selected);
        if indexed_matches.is_none() {
            if root_parent_filter {
                let index = self
                    .filesystem_path_index
                    .path_index(&FilesystemPathIndexRequest::new(
                        request.filter.branch_ids.clone(),
                    ))
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
                indexed_matches = Some(indexed_directory_root_matches(index));
            } else if let FileIdConstraint::Ids(parent_ids) = &target_parent_ids {
                let index = self
                    .filesystem_path_index
                    .path_index(&FilesystemPathIndexRequest::new(
                        request.filter.branch_ids.clone(),
                    ))
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
                indexed_matches = Some(indexed_directory_parent_matches(index, parent_ids));
            }
        }
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
            source: scan_row_source(
                Arc::clone(&output_schema),
                (
                    Arc::clone(&self.hot_state),
                    Arc::clone(&self.schema),
                    output_schema,
                    projection.cloned(),
                    request,
                    indexed_matches,
                    physical_filters,
                    limit,
                ),
                |(
                    hot_state,
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
                        let rows = hot_state.scan_batch(&request).await.map_err(|error| {
                            DataFusionError::Execution(format!(
                                "sql2 lix_directory scan failed: {error}"
                            ))
                        })?;
                        lix_directory_record_batch(&batch_schema, &rows)
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
        let row_capacity = batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>()
            .saturating_mul(3);
        let mut rows = RawWriteBatch::with_capacity(row_capacity);
        let mut count = 0_u64;
        for batch in batches {
            if path_resolvers.is_none() {
                path_resolvers = Some(self.path_resolvers_for_write(write_ctx).await?);
            }
            count = count
                .checked_add(u64::try_from(batch.num_rows()).map_err(|_| {
                    DataFusionError::Execution("lix_directory INSERT row count overflow".into())
                })?)
                .ok_or_else(|| {
                    DataFusionError::Execution("lix_directory INSERT row count overflow".into())
                })?;
            if record_batch_has_non_null_column(&batch, "path")? {
                rows.append(lix_directory_write_rows_from_batch_with_path_resolvers(
                    &batch,
                    self.branch_binding.active_branch_id(),
                    surface_name,
                    path_resolvers
                        .as_mut()
                        .expect("path resolver should be initialized"),
                    &mut || self.functions.call_uuid_v7().to_string(),
                )?);
            } else {
                rows.append(
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

    async fn plan_insert_with_returning(
        &self,
        write_ctx: SqlWriteContext,
        _input: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        returning: DmlReturning,
    ) -> Result<InsertApply> {
        let spec = self.clone();
        Ok(Arc::new(move |batches| {
            let write_ctx = write_ctx.clone();
            let spec = spec.clone();
            let returning = returning.clone();
            async move {
                let surface_name = lix_directory_surface_name(&spec.branch_binding);
                let row_capacity = batches
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>()
                    .saturating_mul(3);
                let mut rows = RawWriteBatch::with_capacity(row_capacity);
                let mut path_resolvers = None;
                let mut keys = Vec::new();
                let mut count = 0_u64;
                for batch in batches {
                    let batch = spec.materialize_returning_insert_defaults(&batch)?;
                    for row_index in 0..batch.num_rows() {
                        keys.push(spec.returning_key_from_batch(&batch, row_index)?);
                    }
                    if path_resolvers.is_none() {
                        path_resolvers = Some(spec.path_resolvers_for_write(&write_ctx).await?);
                    }
                    count = count
                        .checked_add(u64::try_from(batch.num_rows()).map_err(|_| {
                            DataFusionError::Execution(
                                "lix_directory INSERT row count overflow".into(),
                            )
                        })?)
                        .ok_or_else(|| {
                            DataFusionError::Execution(
                                "lix_directory INSERT row count overflow".into(),
                            )
                        })?;
                    if record_batch_has_non_null_column(&batch, "path")? {
                        rows.append(lix_directory_write_rows_from_batch_with_path_resolvers(
                            &batch,
                            spec.branch_binding.active_branch_id(),
                            surface_name,
                            path_resolvers
                                .as_mut()
                                .expect("path resolver should be initialized"),
                            &mut || spec.functions.call_uuid_v7().to_string(),
                        )?);
                    } else {
                        rows.append(
                            lix_directory_write_rows_from_batch_with_options_and_path_resolvers(
                                &batch,
                                spec.branch_binding.active_branch_id(),
                                surface_name,
                                true,
                                path_resolvers.as_mut(),
                                None,
                            )?,
                        );
                    }
                }

                if !rows.is_empty() {
                    write_ctx
                        .stage_write(TransactionWrite::Rows {
                            mode: TransactionWriteMode::Insert,
                            rows,
                        })
                        .await
                        .map_err(lix_error_to_datafusion_error)?;
                }

                let post_image = spec.returning_post_image(&write_ctx, &keys).await?;
                returning.capture(returning.project(&post_image)?);
                Ok(count)
            }
            .boxed()
        }))
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
                                Arc::new(WriteContextHotStateReader::new(write_ctx.clone())),
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
                    let mut path_resolvers = directory_path_resolvers_from_hot_state(
                        Arc::new(WriteContextHotStateReader::new(write_ctx.clone())),
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

    async fn plan_update_with_returning(
        &self,
        write_ctx: SqlWriteContext,
        assignments: Vec<(String, Arc<dyn PhysicalExpr>)>,
        filters: &[Expr],
        returning: DmlReturning,
    ) -> Result<PlannedDml> {
        let request = self.dml_scan_request(filters).await?;
        let indexed_matches = self.indexed_path_matches(&request, filters).await?;
        let captured: Arc<Mutex<Option<RecordBatch>>> = Arc::new(Mutex::new(None));
        let branch_binding = self.branch_binding.clone();
        let functions = self.functions.clone();
        let returning_spec = self.clone();
        Ok(PlannedDml {
            source: self.dml_source(&write_ctx, request, indexed_matches, captured),
            apply: Arc::new(move |matched_batch| {
                let write_ctx = write_ctx.clone();
                let branch_binding = branch_binding.clone();
                let functions = functions.clone();
                let assignments = assignments.clone();
                let returning = returning.clone();
                let returning_spec = returning_spec.clone();
                async move {
                    let keys = (0..matched_batch.num_rows())
                        .map(|row_index| {
                            returning_spec.returning_key_from_batch(&matched_batch, row_index)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let mut path_resolvers = directory_path_resolvers_from_hot_state(
                        Arc::new(WriteContextHotStateReader::new(write_ctx.clone())),
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

                    let post_image = returning_spec
                        .returning_post_image(&write_ctx, &keys)
                        .await?;
                    returning.capture(returning.project(&post_image)?);
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
    /// path resolvers from the transaction-visible filesystem index, then
    /// branch on whether the batch carries a non-null `path` column.
    /// Directories have no file data, so the result is plain state rows.
    async fn insert_staged_rows(
        &self,
        write_ctx: &SqlWriteContext,
        batch: &RecordBatch,
    ) -> Result<StagedUpsert> {
        let surface_name = lix_directory_surface_name(&self.branch_binding);
        let mut path_resolvers = self.path_resolvers_for_write(write_ctx).await?;

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

    fn validate_proposed_batch(&self, batch: &RecordBatch) -> Result<()> {
        for row_index in 0..batch.num_rows() {
            defaultable_text_insert_value(batch, row_index, "id", "INSERT into lix_directory")?;
            defaultable_bool_insert_value(
                batch,
                row_index,
                "lixcol_global",
                "INSERT into lix_directory",
            )?;
            defaultable_bool_insert_value(
                batch,
                row_index,
                "lixcol_untracked",
                "INSERT into lix_directory",
            )?;
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
        LixDirectorySpec::materialize_returning_insert_defaults(self, proposed)
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
        let post_image = self.returning_post_image(write_ctx, &keys).await?;
        returning.capture(returning.project(&post_image)?);
        Ok(())
    }

    /// Scan the existing directories that could conflict with `proposed`,
    /// scoped to the active/explicit branch and narrowed to the proposed
    /// directory ids or exact paths, returned as a batch in this table's
    /// column schema (the same builder the scan path uses).
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
        request.filter.row_pks = match target.kind() {
            UpsertConflictKind::Id => proposed_directory_row_pks(proposed)?,
            UpsertConflictKind::Path => {
                validate_required_paths(proposed, "lix_directory")?;
                Vec::new()
            }
        };

        if target.kind() == UpsertConflictKind::Path {
            // `ON CONFLICT (path)` has a finite, exact set of proposed
            // paths. The filesystem index preserves every visible directory
            // lane for each path (tracked, untracked, and global), so retain
            // those rows for the generic matcher and its lane validation.
            // Primary-key conflict targets intentionally retain the generic
            // row-PK scan below. An index-build failure also falls through
            // to that generic directory-only scan.
            let index = self
                .filesystem_path_index
                .path_index(&FilesystemPathIndexRequest::new(
                    request.filter.branch_ids.clone(),
                ))
                .await;
            if let Ok(index) = index {
                let matches = indexed_path_matches(
                    index,
                    &proposed_directory_path_predicate(proposed)?,
                    FilesystemPathKind::Directory,
                );
                return indexed_lix_directory_record_batch(&self.schema, &matches)
                    .map_err(lix_error_to_datafusion_error);
            }
        }

        let rows = write_ctx
            .scan_hot_state_batch(&request)
            .await
            .map_err(lix_error_to_datafusion_error)?;
        lix_directory_record_batch(&self.schema, &rows).map_err(lix_error_to_datafusion_error)
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
        let mut path_resolvers = directory_path_resolvers_from_hot_state(
            Arc::new(WriteContextHotStateReader::new(write_ctx.clone())),
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

/// The proposed directory ids as single-element row primary keys, used to
/// narrow the conflict-candidate live-state scan. Rows without an explicit
/// `id` (defaulted ids) contribute nothing — a generated id cannot collide
/// with an existing row.
fn proposed_directory_row_pks(proposed: &RecordBatch) -> Result<Vec<RowPk>> {
    let mut row_pks = Vec::new();
    for row_index in 0..proposed.num_rows() {
        if let Some(id) = optional_string_value(proposed, row_index, "id")? {
            row_pks.push(RowPk::uuid_from_canonical(&id).map_err(|error| {
                DataFusionError::Execution(format!(
                    "lix_directory id must be a canonical UUID: {error}"
                ))
            })?);
        }
    }
    Ok(row_pks)
}

/// The finite, exact directory paths whose existing rows can conflict with a
/// proposed `INSERT .. ON CONFLICT (path)` batch.
fn proposed_directory_path_predicate(batch: &RecordBatch) -> Result<FilePathPredicate> {
    validate_required_paths(batch, "lix_directory")?;
    let paths = (0..batch.num_rows())
        .map(|row_index| required_string_value(batch, row_index, "path"))
        .collect::<Result<BTreeSet<_>>>()?;
    Ok(FilePathPredicate::In(paths))
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

trait DirectoryLiveRow {
    fn row_pk_json(&self) -> Result<String, LixError>;
    fn schema_key(&self) -> &str;
    fn file_id(&self) -> Option<&str>;
    fn global(&self) -> bool;
    fn change_id(&self) -> Option<String>;
    fn created_at(&self) -> String;
    fn updated_at(&self) -> String;
    fn commit_id(&self) -> Option<String>;
    fn untracked(&self) -> bool;
    fn metadata(&self) -> Option<String>;
    fn branch_id(&self) -> &str;
}

impl DirectoryLiveRow for MaterializedHotStateRow {
    fn row_pk_json(&self) -> Result<String, LixError> {
        self.row_pk.as_json_array_text()
    }

    fn schema_key(&self) -> &str {
        &self.schema_key
    }

    fn file_id(&self) -> Option<&str> {
        self.file_id.as_deref()
    }

    fn global(&self) -> bool {
        self.global
    }

    fn change_id(&self) -> Option<String> {
        self.change_id.map(|id| id.to_string())
    }

    fn created_at(&self) -> String {
        self.created_at.to_string()
    }

    fn updated_at(&self) -> String {
        self.updated_at.to_string()
    }

    fn commit_id(&self) -> Option<String> {
        self.commit_id.map(|id| id.to_string())
    }

    fn untracked(&self) -> bool {
        self.untracked
    }

    fn metadata(&self) -> Option<String> {
        self.metadata.as_deref().map(serialize_row_metadata)
    }

    fn branch_id(&self) -> &str {
        &self.branch_id
    }
}

impl DirectoryLiveRow for MaterializedHotStateRowRef<'_> {
    fn row_pk_json(&self) -> Result<String, LixError> {
        (*self).row_pk().as_json_array_text()
    }

    fn schema_key(&self) -> &str {
        (*self).schema_key()
    }

    fn file_id(&self) -> Option<&str> {
        (*self).file_id()
    }

    fn global(&self) -> bool {
        (*self).global()
    }

    fn change_id(&self) -> Option<String> {
        (*self).change_id().map(|id| id.to_string())
    }

    fn created_at(&self) -> String {
        (*self).created_at().to_string()
    }

    fn updated_at(&self) -> String {
        (*self).updated_at().to_string()
    }

    fn commit_id(&self) -> Option<String> {
        (*self).commit_id().map(|id| id.to_string())
    }

    fn untracked(&self) -> bool {
        (*self).untracked()
    }

    fn metadata(&self) -> Option<String> {
        (*self)
            .metadata()
            .map(|value| serialize_row_metadata(value))
    }

    fn branch_id(&self) -> &str {
        (*self).branch_id()
    }
}

#[derive(Debug, Clone)]
struct DirectoryDescriptorRecord<L = MaterializedHotStateRow> {
    id: String,
    parent_id: Option<String>,
    name: String,
    key: FilesystemDescriptorKey,
    live: L,
}

impl<L> DirectoryPathRecord for DirectoryDescriptorRecord<L> {
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
) -> Result<RawWriteBatch> {
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
) -> Result<RawWriteBatch> {
    let assignment_values = UpdateAssignmentValues::evaluate(batch, assignments)?;
    let updates_path = assignments
        .iter()
        .any(|(column_name, _)| column_name == "path");
    let mut rows = RawWriteBatch::with_capacity(batch.num_rows().saturating_mul(3));
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
            rows.append(
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
        crate::common::validate_lix_path_segment(&name)
            .map_err(lix_error_to_datafusion_error)?;
        if let Some(directory_id) = id.as_ref() {
            let resolver = path_resolvers
                .entry(directory_path_resolver_key(&context))
                .or_default();
            resolver
                .update_directory(parent_id.clone(), name.clone(), directory_id.clone())
                .map_err(lix_error_to_datafusion_error)?;
        }
        DirectoryDescriptorWriteIntent {
            id,
            parent_id,
            name,
            context,
        }
        .append_to(&mut rows);
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
) -> Result<(RawWriteBatch, u64)> {
    let mut rows = RawWriteBatch::with_capacity(batch.num_rows().saturating_mul(3));
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
    directory_path == "/"
        || path == directory_path
        || path
            .strip_prefix(directory_path)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn append_deduped_delete_plan(
    rows: &mut RawWriteBatch,
    seen: &mut BTreeSet<StateRowDedupeKey>,
    mut plan: FilesystemDeletePlan,
    count: &mut u64,
) {
    plan.rows.retain(|row| {
        if !seen.insert(StateRowDedupeKey::from(row)) {
            return false;
        }
        if is_user_visible_filesystem_delete_row(row) {
            *count += 1;
        }
        true
    });
    if !plan.rows.is_empty() {
        rows.append(plan.rows);
    }
}

fn is_user_visible_filesystem_delete_row(row: RawWriteRowRef<'_>) -> bool {
    matches!(
        row.schema_key.as_str(),
        "lix_directory_descriptor" | "lix_file_descriptor"
    )
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct StateRowDedupeKey {
    row_pk: String,
    schema_key: String,
    file_id: Option<String>,
    branch_id: String,
    global: bool,
    untracked: bool,
}

impl From<RawWriteRowRef<'_>> for StateRowDedupeKey {
    fn from(row: RawWriteRowRef<'_>) -> Self {
        Self {
            row_pk: row
                .row_pk
                .expect("directory provider staged row should carry row_pk")
                .as_single_string_owned()
                .expect("directory provider staged row primary key should project"),
            schema_key: row.schema_key.to_string(),
            file_id: row.file_id.map(ToString::to_string),
            branch_id: row.branch_id.to_string(),
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
    Ok(
        lix_directory_write_rows_from_batch_with_options_and_path_resolvers(
            batch,
            branch_binding,
            surface_name,
            reject_read_only_fields,
            None,
            None,
        )?
        .into_rows(),
    )
}

fn lix_directory_write_rows_from_batch_with_options_and_path_resolvers(
    batch: &RecordBatch,
    branch_binding: Option<&str>,
    surface_name: &str,
    reject_read_only_fields: bool,
    mut path_resolvers: Option<&mut BTreeMap<String, DirectoryPathResolver>>,
    mut generate_directory_id: Option<&mut dyn FnMut() -> String>,
) -> Result<RawWriteBatch> {
    let mut rows = RawWriteBatch::with_capacity(batch.num_rows().saturating_mul(3));
    for row_index in 0..batch.num_rows() {
        if reject_read_only_fields {
            reject_read_only_lix_directory_insert_field(batch, row_index, "lixcol_row_pk")?;
            reject_read_only_lix_directory_insert_field(batch, row_index, "lixcol_schema_key")?;
            reject_read_only_lix_directory_insert_field(batch, row_index, "lixcol_change_id")?;
            reject_read_only_lix_directory_insert_field(batch, row_index, "lixcol_created_at")?;
            reject_read_only_lix_directory_insert_field(batch, row_index, "lixcol_updated_at")?;
            reject_read_only_lix_directory_insert_field(batch, row_index, "lixcol_commit_id")?;
        }

        let path = optional_string_value(batch, row_index, "path")?;
        let id =
            defaultable_text_insert_value(batch, row_index, "id", "INSERT into lix_directory")?;
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
            rows.append(planned_rows);
            continue;
        }

        let parent_id = optional_string_value(batch, row_index, "parent_id")?;
        let name = required_string_value(batch, row_index, "name")?;
        crate::common::validate_lix_path_segment(&name)
            .map_err(lix_error_to_datafusion_error)?;
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
        let row_index = rows.len();
        DirectoryDescriptorWriteIntent {
            id: id.clone(),
            parent_id,
            name,
            context,
        }
        .append_to(&mut rows);
        if let Some(directory_id) = id.as_ref() {
            rows.set_origin(
                row_index,
                Some(lix_directory_insert_origin(surface_name, directory_id)),
            );
        }
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
    rows: &mut RawWriteBatch,
    surface_name: &str,
    directory_id: &str,
) {
    let origin = lix_directory_insert_origin(surface_name, directory_id);
    for index in 0..rows.len() {
        let matches = {
            let row = rows.row(index);
            row.schema_key == DIRECTORY_SCHEMA_KEY
                && row
                    .row_pk
                    .and_then(|row_pk| row_pk.as_single_string().ok())
                    == Some(directory_id)
        };
        if matches {
            rows.set_origin(index, Some(origin.clone()));
        }
    }
}

fn lix_directory_insert_origin(surface_name: &str, directory_id: &str) -> TransactionWriteOrigin {
    TransactionWriteOrigin {
        surface: crate::transaction_types::shared_origin_surface(surface_name),
        operation: TransactionWriteOperation::Insert,
        primary_key: Some(Arc::new(LogicalPrimaryKey::single_id(directory_id))),
    }
}

fn directory_row_context_from_batch(
    batch: &RecordBatch,
    row_index: usize,
    branch_binding: Option<&str>,
) -> Result<FilesystemRowContext> {
    let scope = resolve_write_branch_scope(
        defaultable_bool_insert_value(
            batch,
            row_index,
            "lixcol_global",
            "INSERT into lix_directory",
        )?,
        optional_string_value(batch, row_index, "lixcol_branch_id")?,
        branch_binding,
        "INSERT into lix_directory_by_branch",
        "lix_directory",
    )?;

    Ok(FilesystemRowContext {
        branch_id: scope.branch_id,
        global: scope.global,
        untracked: defaultable_bool_insert_value(
            batch,
            row_index,
            "lixcol_untracked",
            "INSERT into lix_directory",
        )?
        .unwrap_or(false),
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
    rows: &MaterializedHotStateBatch,
) -> Result<RecordBatch, LixError> {
    let mut directory_rows = Vec::new();

    for row in rows.iter() {
        if row.schema_key() != DIRECTORY_SCHEMA_KEY {
            continue;
        }
        let Some(snapshot_content) = row.snapshot_content().map(|value| value.as_str()) else {
            continue;
        };
        let snapshot: DirectoryDescriptorSnapshot = serde_json::from_str(snapshot_content)
            .map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("invalid lix_directory_descriptor snapshot JSON: {error}"),
                )
            })?;
        let key = FilesystemDescriptorKey::from_live_row_ref(row, snapshot.id.clone());
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

fn indexed_directory_parent_matches(
    index: Arc<crate::filesystem::FilesystemPathIndex>,
    parent_ids: &BTreeSet<String>,
) -> FilesystemPathSelection {
    let entries = index
        .entries()
        .into_iter()
        .filter(|entry| {
            entry.kind == FilesystemPathKind::Directory
                && entry
                    .parent_id
                    .as_ref()
                    .is_some_and(|parent_id| parent_ids.contains(parent_id))
        })
        .collect();
    FilesystemPathSelection::new(index, entries)
}

fn indexed_directory_root_matches(
    index: Arc<crate::filesystem::FilesystemPathIndex>,
) -> FilesystemPathSelection {
    let entries = index
        .entries()
        .into_iter()
        .filter(|entry| entry.kind == FilesystemPathKind::Directory && entry.parent_id.is_none())
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

fn lix_directory_record_batch_from_rendered<L>(
    schema: &SchemaRef,
    directory_rows: Vec<(DirectoryDescriptorRecord<L>, Option<String>)>,
) -> Result<RecordBatch, LixError>
where
    L: DirectoryLiveRow,
{
    let mut ids = Vec::new();
    let mut paths = Vec::new();
    let mut parent_ids = Vec::new();
    let mut names = Vec::new();
    let mut row_pks = Vec::new();
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
        ids.push(Some(directory.id));
        paths.push(path);
        parent_ids.push(directory.parent_id);
        names.push(Some(directory.name));
        row_pks.push(Some(directory.live.row_pk_json()?));
        schema_keys.push(Some(directory.live.schema_key().to_owned()));
        file_ids.push(directory.live.file_id().map(str::to_owned));
        globals.push(Some(directory.live.global()));
        change_ids.push(directory.live.change_id());
        created_ats.push(directory.live.created_at());
        updated_ats.push(directory.live.updated_at());
        commit_ids.push(directory.live.commit_id());
        untracked_values.push(Some(directory.live.untracked()));
        metadata_values.push(directory.live.metadata());
        branch_ids.push(Some(directory.live.branch_id().to_owned()));
    }

    let mut columns = Vec::<ArrayRef>::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let array: ArrayRef = match field.name().as_str() {
            "id" => Arc::new(StringArray::from(ids.clone())),
            "path" => Arc::new(StringArray::from(paths.clone())),
            "parent_id" => Arc::new(StringArray::from(parent_ids.clone())),
            "name" => Arc::new(StringArray::from(names.clone())),
            "lixcol_row_pk" => Arc::new(StringArray::from(row_pks.clone())),
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
) -> HotStateScanRequest {
    HotStateScanRequest {
        filter: HotStateFilter {
            schema_keys: vec![DIRECTORY_SCHEMA_KEY.to_string()],
            branch_ids: branch_binding
                .map(|branch_id| vec![branch_id.to_string()])
                .unwrap_or_default(),
            ..HotStateFilter::default()
        },
        projection: lix_directory_hot_state_projection(projected_schema),
        limit,
    }
}

fn lix_directory_hot_state_projection(projected_schema: Option<&Schema>) -> HotStateProjection {
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
        json_field("lixcol_row_pk", false),
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use datafusion::arrow::array::{ArrayRef, BooleanArray, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::{Column, ScalarValue};
    use datafusion::datasource::TableProvider;
    use datafusion::execution::context::ExecutionProps;
    use datafusion::logical_expr::expr::BinaryExpr;
    use datafusion::logical_expr::{Expr, Operator};
    use serde_json::json;

    use crate::LixError;
    use crate::binary_cas::BlobDataReader;
    use crate::branch::{BranchHead, BranchRefReader};
    use crate::changelog::{ChangeId, CommitId};
    use crate::common::LixTimestamp;
    use crate::filesystem::{
        FilesystemPathIndex, FilesystemPathIndexReader, FilesystemPathIndexRequest,
    };
    use crate::functions::FunctionProviderHandle;
    use crate::hot_state::{
        HotStateReader, HotStateScanRequest, MaterializedHotStateBatch, MaterializedHotStateRow,
    };
    use crate::sql2::{SqlWriteContext, SqlWriteExecutionContext};
    use crate::transaction_types::{
        RawWriteBatch, TransactionJson, TransactionWrite, TransactionWriteMode,
        TransactionWriteOutcome, TransactionWriteRow,
    };

    use super::super::spec::{SpecTableProvider, TableSpec};
    use super::{
        BranchBinding, DirectoryDescriptorRecord, LixDirectorySpec, UpsertConflictTarget,
        UpsertSupport, derive_directory_paths, lix_directory_by_branch_schema,
        lix_directory_insert_origin, lix_directory_record_batch,
        lix_directory_recursive_delete_rows_from_batch, lix_directory_write_rows_from_batch,
        lix_directory_write_rows_from_batch_with_path_resolvers,
    };
    use crate::filesystem::{
        FilesystemDescriptorKey, VisibleFilesystem, directory_path_resolvers_from_state_batch,
    };

    fn path_index_from_rows(
        rows: Vec<MaterializedHotStateRow>,
    ) -> Result<FilesystemPathIndex, LixError> {
        FilesystemPathIndex::from_live_batch(&MaterializedHotStateBatch::from_rows(rows))
    }

    fn visible_filesystem_from_rows(
        rows: Vec<MaterializedHotStateRow>,
    ) -> Result<VisibleFilesystem, LixError> {
        VisibleFilesystem::from_live_batch(&MaterializedHotStateBatch::from_rows(rows))
    }

    fn test_id_generator(ids: &'static [&'static str]) -> impl FnMut() -> String {
        let mut ids = ids.iter();
        move || ids.next().expect("test id should exist").to_string()
    }

    fn test_functions() -> FunctionProviderHandle {
        FunctionProviderHandle::system()
    }

    fn eq_filter(column_name: &str, value: &str) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(Column::from_name(column_name))),
            Operator::Eq,
            Box::new(Expr::Literal(
                ScalarValue::Utf8(Some(value.to_string())),
                None,
            )),
        ))
    }

    struct RejectingHotStateReader {
        scan_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl HotStateReader for RejectingHotStateReader {
        async fn load_exact_batch(
            &self,
            request: &crate::hot_state::HotStateExactBatchRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateExactBatch, LixError> {
            crate::hot_state::load_exact_batch_via_scan_for_test(self, request).await
        }

        async fn scan_batch(
            &self,
            _request: &HotStateScanRequest,
        ) -> Result<MaterializedHotStateBatch, LixError> {
            self.scan_count.fetch_add(1, Ordering::SeqCst);
            Err(LixError::unknown(
                "directory parent-id scan should not read live state",
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

    struct FailingFilesystemPathIndexReader;

    #[async_trait]
    impl FilesystemPathIndexReader for FailingFilesystemPathIndexReader {
        async fn path_index(
            &self,
            _request: &FilesystemPathIndexRequest,
        ) -> Result<Arc<FilesystemPathIndex>, LixError> {
            Err(LixError::unknown(
                "unrelated malformed file descriptor prevented path-index construction",
            ))
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

    /// Stage a single INSERT batch through the directory spec, exercising the
    /// same `stage_insert` path the writable provider uses.
    async fn stage_directory_insert(
        write_ctx: SqlWriteContext,
        branch_binding: BranchBinding,
        batch: RecordBatch,
    ) -> Result<u64, datafusion::common::DataFusionError> {
        let hot_state = Arc::new(crate::sql2::WriteContextHotStateReader::new(
            write_ctx.clone(),
        ));
        let branch_ref = Arc::new(crate::sql2::WriteContextBranchRefReader::new(
            write_ctx.clone(),
        ));
        let filesystem_path_index: Arc<dyn FilesystemPathIndexReader> = hot_state.clone();
        let spec = match branch_binding {
            BranchBinding::Active { .. } => LixDirectorySpec::active_branch(
                write_ctx.active_branch_id(),
                hot_state,
                filesystem_path_index,
                branch_ref,
                test_functions(),
            ),
            BranchBinding::Explicit => LixDirectorySpec::by_branch(
                hot_state,
                filesystem_path_index,
                branch_ref,
                test_functions(),
            ),
        };
        spec.stage_insert(&write_ctx, vec![batch]).await
    }

    /// Stage one active-branch INSERT with an injected path-index reader so a
    /// routing test can prove resolver seeding does not fall back to a
    /// live-state scan.
    async fn stage_active_directory_insert_with_path_index(
        write_ctx: SqlWriteContext,
        filesystem_path_index: Arc<dyn FilesystemPathIndexReader>,
        batch: RecordBatch,
    ) -> Result<u64, datafusion::common::DataFusionError> {
        let hot_state = Arc::new(crate::sql2::WriteContextHotStateReader::new(
            write_ctx.clone(),
        ));
        let branch_ref = Arc::new(crate::sql2::WriteContextBranchRefReader::new(
            write_ctx.clone(),
        ));
        let spec = LixDirectorySpec::active_branch(
            write_ctx.active_branch_id(),
            hot_state,
            filesystem_path_index,
            branch_ref,
            test_functions(),
        );
        spec.stage_insert(&write_ctx, vec![batch]).await
    }

    #[derive(Default)]
    struct CapturingWriteContext {
        rows: Vec<MaterializedHotStateRow>,
        writes: Vec<TransactionWrite>,
        reject_scans: bool,
    }

    #[async_trait]
    impl BlobDataReader for CapturingWriteContext {
        async fn load_bytes_many(
            &self,
            hashes: &[crate::binary_cas::BlobId],
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
            "01920000-0000-7000-8000-0000000000a1"
        }

        fn functions(&self) -> FunctionProviderHandle {
            test_functions()
        }

        fn list_visible_schemas(&self) -> Result<Vec<serde_json::Value>, LixError> {
            Ok(Vec::new())
        }

        async fn load_bytes_many(
            &mut self,
            hashes: &[crate::binary_cas::BlobId],
        ) -> Result<crate::binary_cas::BlobBytesBatch, LixError> {
            BlobDataReader::load_bytes_many(self, hashes).await
        }

        async fn scan_hot_state_batch(
            &mut self,
            _request: &HotStateScanRequest,
        ) -> Result<MaterializedHotStateBatch, LixError> {
            if self.reject_scans {
                return Err(LixError::unknown(
                    "directory index routing should not scan live state",
                ));
            }
            Ok(MaterializedHotStateBatch::from_rows(self.rows.clone()))
        }

        async fn load_exact_hot_state_batch(
            &mut self,
            request: &crate::hot_state::HotStateExactBatchRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateExactBatch, LixError> {
            Ok(crate::hot_state::MaterializedHotStateExactBatch::from_rows(
                request
                    .rows
                    .iter()
                    .map(|requested| {
                        self.rows
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
                "directory provider test context does not stage transaction journals",
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

    fn live_row(
        row_pk: &str,
        branch_id: &str,
        snapshot_content: &str,
    ) -> MaterializedHotStateRow {
        live_filesystem_row(
            row_pk,
            super::DIRECTORY_SCHEMA_KEY,
            None,
            branch_id,
            snapshot_content,
        )
    }

    fn live_filesystem_row(
        row_pk: &str,
        schema_key: &str,
        file_id: Option<&str>,
        branch_id: &str,
        snapshot_content: &str,
    ) -> MaterializedHotStateRow {
        MaterializedHotStateRow {
            row_pk: crate::row_pk::RowPk::uuid_from_canonical(row_pk)
                .expect("fixture filesystem ID should be a UUID"),
            schema_key: schema_key.to_string(),
            file_id: file_id.map(ToOwned::to_owned),
            snapshot_content: Some(snapshot_content.into()),
            metadata: Some(json!({"source": "test"}).to_string().into()),
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

    fn filesystem_rows() -> Vec<MaterializedHotStateRow> {
        vec![
            live_filesystem_row(
                "01920000-0000-7000-8000-0000000000d3",
                "lix_directory_descriptor",
                None,
                "01920000-0000-7000-8000-0000000000a1",
                r#"{"id":"01920000-0000-7000-8000-0000000000d3","parent_id":null,"name":"docs"}"#,
            ),
            live_filesystem_row(
                "01920000-0000-7000-8000-000000000313",
                "lix_directory_descriptor",
                None,
                "01920000-0000-7000-8000-0000000000a1",
                r#"{"id":"01920000-0000-7000-8000-000000000313","parent_id":"01920000-0000-7000-8000-0000000000d3","name":"guides"}"#,
            ),
            live_filesystem_row(
                "01920000-0000-7000-8000-000000000372",
                "lix_file_descriptor",
                None,
                "01920000-0000-7000-8000-0000000000a1",
                r#"{"id":"01920000-0000-7000-8000-000000000372","directory_id":"01920000-0000-7000-8000-0000000000d3","name":"index.md"}"#,
            ),
            live_filesystem_row(
                "01920000-0000-7000-8000-0000000000d2",
                "lix_file_descriptor",
                None,
                "01920000-0000-7000-8000-0000000000a1",
                r#"{"id":"01920000-0000-7000-8000-0000000000d2","directory_id":"01920000-0000-7000-8000-000000000313","name":"readme.md"}"#,
            ),
            live_filesystem_row(
                "01920000-0000-7000-8000-0000000000d2",
                "lix_binary_blob_ref",
                Some("01920000-0000-7000-8000-0000000000d2"),
                "01920000-0000-7000-8000-0000000000a1",
                r#"{"id":"01920000-0000-7000-8000-0000000000d2","blob_hash":"abc123","size_bytes":5}"#,
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
            string_column(vec![Some("01920000-0000-7000-8000-0000000000d3")]),
            string_column(vec![None]),
            string_column(vec![Some("docs")]),
            Arc::new(BooleanArray::from(vec![global])) as ArrayRef,
            string_column(vec![Some("{\"source\":\"directory\"}")]),
        ];
        if include_branch {
            fields.push(Field::new("lixcol_branch_id", DataType::Utf8, false));
            columns.push(string_column(vec![Some(
                "01920000-0000-7000-8000-0000000000a1",
            )]));
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
                string_column(vec![Some("01920000-0000-7000-8000-000000000343")]),
                string_column(vec![Some(path)]),
                string_column(vec![Some("01920000-0000-7000-8000-0000000000a1")]),
            ],
        )
        .expect("directory path insert batch should build")
    }

    fn active_directory_path_insert_batch(path: &str) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("path", DataType::Utf8, true),
            ])),
            vec![
                string_column(vec![Some("01920000-0000-7000-8000-000000000343")]),
                string_column(vec![Some(path)]),
            ],
        )
        .expect("active directory path insert batch should build")
    }

    fn directory_delete_batch(ids: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("lixcol_branch_id", DataType::Utf8, false),
            ])),
            vec![
                string_column(ids.iter().copied().map(Some).collect::<Vec<_>>()),
                string_column(vec![
                    Some("01920000-0000-7000-8000-0000000000a1");
                    ids.len()
                ]),
            ],
        )
        .expect("directory delete batch should build")
    }

    #[test]
    fn derives_nested_directory_paths() {
        let root_live = live_row(
            "01920000-0000-7000-8000-0000000000d3",
            "01920000-0000-7000-8000-0000000000a1",
            "{\"id\":\"01920000-0000-7000-8000-0000000000d3\",\"parent_id\":null,\"name\":\"docs\"}",
        );
        let child_live = live_row(
            "01920000-0000-7000-8000-000000000313",
            "01920000-0000-7000-8000-0000000000a1",
            "{\"id\":\"01920000-0000-7000-8000-000000000313\",\"parent_id\":\"01920000-0000-7000-8000-0000000000d3\",\"name\":\"guides\"}",
        );
        let root = DirectoryDescriptorRecord {
            id: "01920000-0000-7000-8000-0000000000d3".to_string(),
            parent_id: None,
            name: "docs".to_string(),
            key: FilesystemDescriptorKey::from_live_row(
                &root_live,
                "01920000-0000-7000-8000-0000000000d3",
            ),
            live: root_live,
        };
        let child = DirectoryDescriptorRecord {
            id: "01920000-0000-7000-8000-000000000313".to_string(),
            parent_id: Some("01920000-0000-7000-8000-0000000000d3".to_string()),
            name: "guides".to_string(),
            key: FilesystemDescriptorKey::from_live_row(
                &child_live,
                "01920000-0000-7000-8000-000000000313",
            ),
            live: child_live,
        };
        let child_key = child.key.clone();
        let records = [root, child];
        let paths = derive_directory_paths(records.iter().map(|row| (row.key.clone(), row)))
            .expect("path derivation should succeed");

        assert_eq!(paths.get(&child_key), Some(&"/docs/guides".to_string()));
    }

    #[test]
    fn record_batch_projects_directory_columns() {
        let rows = vec![
            live_row(
                "01920000-0000-7000-8000-0000000000d3",
                "01920000-0000-7000-8000-0000000000a1",
                "{\"id\":\"01920000-0000-7000-8000-0000000000d3\",\"parent_id\":null,\"name\":\"docs\"}",
            ),
            live_row(
                "01920000-0000-7000-8000-000000000313",
                "01920000-0000-7000-8000-0000000000a1",
                "{\"id\":\"01920000-0000-7000-8000-000000000313\",\"parent_id\":\"01920000-0000-7000-8000-0000000000d3\",\"name\":\"guides\"}",
            ),
        ];

        let rows = MaterializedHotStateBatch::from_rows(rows);
        let batch = lix_directory_record_batch(&lix_directory_by_branch_schema(), &rows)
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
            "/docs/guides"
        );
        assert_eq!(
            batch
                .column_by_name("lixcol_branch_id")
                .expect("branch column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("branch is string")
                .value(1),
            "01920000-0000-7000-8000-0000000000a1"
        );
    }

    #[tokio::test]
    async fn directory_parent_id_scan_uses_indexed_descriptors() {
        let hot_state_scans = Arc::new(AtomicUsize::new(0));
        let path_index_requests = Arc::new(AtomicUsize::new(0));
        let index = Arc::new(
            path_index_from_rows(vec![
                live_row(
                    "01920000-0000-7000-8000-0000000000d3",
                    "01920000-0000-7000-8000-0000000000a1",
                    r#"{"id":"01920000-0000-7000-8000-0000000000d3","parent_id":null,"name":"docs"}"#,
                ),
                live_row(
                    "01920000-0000-7000-8000-000000000313",
                    "01920000-0000-7000-8000-0000000000a1",
                    r#"{"id":"01920000-0000-7000-8000-000000000313","parent_id":"01920000-0000-7000-8000-0000000000d3","name":"guides"}"#,
                ),
                live_row(
                    "01920000-0000-7000-8000-000000000393",
                    "01920000-0000-7000-8000-0000000000a1",
                    r#"{"id":"01920000-0000-7000-8000-000000000393","parent_id":"01920000-0000-7000-8000-0000000000d3","name":"reference"}"#,
                ),
                live_row(
                    "01920000-0000-7000-8000-000000000383",
                    "01920000-0000-7000-8000-0000000000a1",
                    r#"{"id":"01920000-0000-7000-8000-000000000383","parent_id":null,"name":"other"}"#,
                ),
            ])
            .expect("filesystem path index should build"),
        );
        let spec = LixDirectorySpec::active_branch(
            "01920000-0000-7000-8000-0000000000a1",
            Arc::new(RejectingHotStateReader {
                scan_count: Arc::clone(&hot_state_scans),
            }),
            Arc::new(StaticFilesystemPathIndexReader {
                index,
                request_count: Arc::clone(&path_index_requests),
            }),
            Arc::new(TestBranchRefReader),
            test_functions(),
        );
        let projection = vec![
            spec.schema().index_of("path").expect("path column"),
            spec.schema().index_of("name").expect("name column"),
            spec.schema()
                .index_of("lixcol_change_id")
                .expect("change-id column"),
            spec.schema()
                .index_of("lixcol_updated_at")
                .expect("updated-at column"),
        ];
        let filters = vec![eq_filter(
            "parent_id",
            "01920000-0000-7000-8000-0000000000d3",
        )];

        let planned = spec
            .plan_scan(Some(&projection), &filters, None, &ExecutionProps::new())
            .await
            .expect("parent-id scan should plan");
        let batch = planned
            .source
            .load_single_batch()
            .await
            .expect("parent-id scan should load");

        let paths = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("path column should be string data");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(paths.value(0), "/docs/guides");
        assert_eq!(paths.value(1), "/docs/reference");
        assert_eq!(path_index_requests.load(Ordering::SeqCst), 1);
        assert_eq!(hot_state_scans.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn directory_root_scan_uses_indexed_descriptors() {
        let hot_state_scans = Arc::new(AtomicUsize::new(0));
        let path_index_requests = Arc::new(AtomicUsize::new(0));
        let index = Arc::new(
            path_index_from_rows(vec![
                live_row(
                    "01920000-0000-7000-8000-0000000000d3",
                    "01920000-0000-7000-8000-0000000000a1",
                    r#"{"id":"01920000-0000-7000-8000-0000000000d3","parent_id":null,"name":"docs"}"#,
                ),
                live_row(
                    "01920000-0000-7000-8000-000000000313",
                    "01920000-0000-7000-8000-0000000000a1",
                    r#"{"id":"01920000-0000-7000-8000-000000000313","parent_id":"01920000-0000-7000-8000-0000000000d3","name":"guides"}"#,
                ),
                live_row(
                    "01920000-0000-7000-8000-000000000383",
                    "01920000-0000-7000-8000-0000000000a1",
                    r#"{"id":"01920000-0000-7000-8000-000000000383","parent_id":null,"name":"other"}"#,
                ),
            ])
            .expect("filesystem path index should build"),
        );
        let spec = LixDirectorySpec::active_branch(
            "01920000-0000-7000-8000-0000000000a1",
            Arc::new(RejectingHotStateReader {
                scan_count: Arc::clone(&hot_state_scans),
            }),
            Arc::new(StaticFilesystemPathIndexReader {
                index,
                request_count: Arc::clone(&path_index_requests),
            }),
            Arc::new(TestBranchRefReader),
            test_functions(),
        );
        let projection = vec![
            spec.schema().index_of("path").expect("path column"),
            spec.schema().index_of("name").expect("name column"),
            spec.schema()
                .index_of("lixcol_change_id")
                .expect("change-id column"),
            spec.schema()
                .index_of("lixcol_updated_at")
                .expect("updated-at column"),
        ];
        let filters = vec![Expr::IsNull(Box::new(Expr::Column(Column::from_name(
            "parent_id",
        ))))];

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
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(paths.value(0), "/docs");
        assert_eq!(paths.value(1), "/other");
        assert_eq!(path_index_requests.load(Ordering::SeqCst), 1);
        assert_eq!(hot_state_scans.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn decodes_directory_insert_into_transaction_write_row() {
        let rows = lix_directory_write_rows_from_batch(&directory_insert_batch(true, false), None)
            .expect("directory batch should decode");

        assert_eq!(
            rows,
            vec![TransactionWriteRow {
                row_pk: Some(
                    crate::row_pk::RowPk::uuid_from_canonical(
                        "01920000-0000-7000-8000-0000000000d3",
                    )
                    .expect("fixture directory ID"),
                ),
                schema_key: super::DIRECTORY_SCHEMA_KEY.into(),
                file_id: None,
                snapshot: Some(TransactionJson::from_value_for_test(
                    json!({"id":"01920000-0000-7000-8000-0000000000d3","name":"docs","parent_id":null})
                )),
                metadata: Some(TransactionJson::from_value_for_test(
                    json!({"source": "directory"})
                )),
                origin: Some(lix_directory_insert_origin(
                    "lix_directory",
                    "01920000-0000-7000-8000-0000000000d3"
                )),
                created_at: None,
                updated_at: None,
                global: false,
                change_id: None,
                commit_id: None,
                untracked: false,
                branch_id: "01920000-0000-7000-8000-0000000000a1".into(),
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
            "01920000-0000-7000-8000-0000000000d3",
            "01920000-0000-7000-8000-0000000000a1",
            "{\"id\":\"01920000-0000-7000-8000-0000000000d3\",\"parent_id\":null,\"name\":\"docs\"}",
        )];
        let existing_rows = MaterializedHotStateBatch::from_rows(existing_rows);
        let mut resolvers = directory_path_resolvers_from_state_batch(&existing_rows)
            .expect("existing directory rows should seed paths");

        let rows = lix_directory_write_rows_from_batch_with_path_resolvers(
            &directory_path_insert_batch("/docs/nested"),
            None,
            "lix_directory",
            &mut resolvers,
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect("directory path batch should decode");

        assert_eq!(rows.len(), 1);
        let snapshot = rows.row(0).snapshot.unwrap();
        assert_eq!(snapshot["id"], "01920000-0000-7000-8000-000000000343");
        assert_eq!(
            snapshot["parent_id"],
            "01920000-0000-7000-8000-0000000000d3"
        );
        assert_eq!(snapshot["name"], "nested");
    }

    #[test]
    fn recursive_directory_delete_deletes_nested_dirs_files_and_blob_refs() {
        let visible_filesystem = visible_filesystem_from_rows(filesystem_rows())
            .expect("visible filesystem should build");
        let mut visible_filesystems = BTreeMap::new();
        visible_filesystems.insert(
            "01920000-0000-7000-8000-0000000000a1".to_string(),
            visible_filesystem,
        );

        let (rows, count) = lix_directory_recursive_delete_rows_from_batch(
            &directory_delete_batch(&["01920000-0000-7000-8000-0000000000d3"]),
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
                        row.row_pk
                            .as_ref()
                            .expect("planned delete row should carry row_pk")
                            .as_single_string_owned()
                            .expect("planned delete row should project row_pk"),
                    )
                })
                .collect::<Vec<_>>(),
            vec![
                (
                    "lix_file_descriptor",
                    "01920000-0000-7000-8000-0000000000d2".to_string()
                ),
                (
                    "lix_binary_blob_ref",
                    "01920000-0000-7000-8000-0000000000d2".to_string()
                ),
                (
                    "lix_directory_descriptor",
                    "01920000-0000-7000-8000-000000000313".to_string()
                ),
                (
                    "lix_file_descriptor",
                    "01920000-0000-7000-8000-000000000372".to_string()
                ),
                (
                    "lix_directory_descriptor",
                    "01920000-0000-7000-8000-0000000000d3".to_string()
                ),
            ]
        );
        assert!(rows.iter().all(|row| row.snapshot.is_none()));
    }

    #[test]
    fn recursive_directory_delete_dedupes_overlapping_parent_and_child() {
        let visible_filesystem = visible_filesystem_from_rows(filesystem_rows())
            .expect("visible filesystem should build");
        let mut visible_filesystems = BTreeMap::new();
        visible_filesystems.insert(
            "01920000-0000-7000-8000-0000000000a1".to_string(),
            visible_filesystem,
        );

        let (rows, count) = lix_directory_recursive_delete_rows_from_batch(
            &directory_delete_batch(&[
                "01920000-0000-7000-8000-0000000000d3",
                "01920000-0000-7000-8000-000000000313",
            ]),
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
                    row.row_pk.clone(),
                    row.file_id.clone(),
                    row.branch_id.clone(),
                )
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(identities.len(), rows.len());
        assert_eq!(rows.len(), 5);
    }

    #[tokio::test]
    async fn directory_insert_sink_stages_decoded_transaction_rows() {
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
                rows: RawWriteBatch::from_test_rows(vec![TransactionWriteRow {
                    row_pk: Some(
                        crate::row_pk::RowPk::uuid_from_canonical(
                            "01920000-0000-7000-8000-0000000000d3",
                        )
                        .expect("fixture directory ID"),
                    ),
                    schema_key: super::DIRECTORY_SCHEMA_KEY.into(),
                    file_id: None,
                    snapshot: Some(TransactionJson::from_value_for_test(
                        json!({"id":"01920000-0000-7000-8000-0000000000d3","name":"docs","parent_id":null})
                    )),
                    metadata: Some(TransactionJson::from_value_for_test(
                        json!({"source": "directory"})
                    )),
                    origin: Some(lix_directory_insert_origin(
                        "lix_directory_by_branch",
                        "01920000-0000-7000-8000-0000000000d3"
                    )),
                    created_at: None,
                    updated_at: None,
                    global: false,
                    change_id: None,
                    commit_id: None,
                    untracked: false,
                    branch_id: "01920000-0000-7000-8000-0000000000a1".into(),
                }])
            }]
        );
    }

    #[tokio::test]
    async fn directory_insert_sink_seeds_path_resolver_from_filesystem_index() {
        let mut write_context = CapturingWriteContext {
            rows: vec![live_row(
                "01920000-0000-7000-8000-0000000000d3",
                "01920000-0000-7000-8000-0000000000a1",
                "{\"id\":\"01920000-0000-7000-8000-0000000000d3\",\"parent_id\":null,\"name\":\"docs\"}",
            )],
            writes: Vec::new(),
            reject_scans: false,
        };
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let batch = directory_path_insert_batch("/docs/nested");
        let count = stage_directory_insert(write_ctx, BranchBinding::explicit(), batch)
            .await
            .expect("directory spec should stage path write");

        assert_eq!(count, 1);
        let [TransactionWrite::Rows { rows, .. }] = write_context.writes.as_slice() else {
            panic!("expected one directory staged write");
        };
        assert_eq!(rows.len(), 1);
        let snapshot = rows.row(0).snapshot.unwrap();
        assert_eq!(snapshot["id"], "01920000-0000-7000-8000-000000000343");
        assert_eq!(
            snapshot["parent_id"],
            "01920000-0000-7000-8000-0000000000d3"
        );
        assert_eq!(snapshot["name"], "nested");
    }

    #[tokio::test]
    async fn directory_path_insert_uses_indexed_resolver_without_hot_state_fallback() {
        let index = Arc::new(
            path_index_from_rows(vec![live_row(
                "01920000-0000-7000-8000-0000000000d3",
                "01920000-0000-7000-8000-0000000000a1",
                "{\"id\":\"01920000-0000-7000-8000-0000000000d3\",\"parent_id\":null,\"name\":\"docs\"}",
            )])
            .expect("filesystem path index should build"),
        );
        let filesystem_path_index: Arc<dyn FilesystemPathIndexReader> =
            Arc::new(StaticFilesystemPathIndexReader {
                index,
                request_count: Arc::new(AtomicUsize::new(0)),
            });
        let mut write_context = CapturingWriteContext {
            rows: Vec::new(),
            writes: Vec::new(),
            reject_scans: true,
        };

        let count = {
            let write_ctx = SqlWriteContext::new(&mut write_context);
            stage_active_directory_insert_with_path_index(
                write_ctx,
                filesystem_path_index,
                active_directory_path_insert_batch("/docs/nested"),
            )
            .await
            .expect("indexed directory path insert should stage without a live-state scan")
        };

        assert_eq!(count, 1);
        let [TransactionWrite::Rows { rows, .. }] = write_context.writes.as_slice() else {
            panic!("expected one directory staged write");
        };
        assert_eq!(rows.len(), 1);
        let snapshot = rows.row(0).snapshot.expect("staged descriptor snapshot");
        assert_eq!(snapshot["id"], "01920000-0000-7000-8000-000000000343");
        assert_eq!(
            snapshot["parent_id"],
            "01920000-0000-7000-8000-0000000000d3"
        );
        assert_eq!(snapshot["name"], "nested");
    }

    #[tokio::test]
    async fn directory_path_insert_falls_back_when_path_index_build_fails() {
        let mut write_context = CapturingWriteContext {
            rows: vec![live_row(
                "01920000-0000-7000-8000-0000000000d3",
                "01920000-0000-7000-8000-0000000000a1",
                "{\"id\":\"01920000-0000-7000-8000-0000000000d3\",\"parent_id\":null,\"name\":\"docs\"}",
            )],
            writes: Vec::new(),
            reject_scans: false,
        };

        let count = {
            let write_ctx = SqlWriteContext::new(&mut write_context);
            stage_active_directory_insert_with_path_index(
                write_ctx,
                Arc::new(FailingFilesystemPathIndexReader),
                active_directory_path_insert_batch("/docs/nested"),
            )
            .await
            .expect("directory path insert should retain its live-state fallback")
        };

        assert_eq!(count, 1);
        let [TransactionWrite::Rows { rows, .. }] = write_context.writes.as_slice() else {
            panic!("expected one directory staged write");
        };
        assert_eq!(rows.len(), 1);
        let snapshot = rows.row(0).snapshot.expect("staged descriptor snapshot");
        assert_eq!(
            snapshot["parent_id"],
            "01920000-0000-7000-8000-0000000000d3"
        );
        assert_eq!(snapshot["name"], "nested");
    }

    #[tokio::test]
    async fn directory_path_conflict_candidates_use_index_and_retain_visible_lanes() {
        let tracked = live_row(
            "01920000-0000-7000-8000-000000000403",
            "01920000-0000-7000-8000-0000000000a1",
            "{\"id\":\"01920000-0000-7000-8000-000000000403\",\"parent_id\":null,\"name\":\"docs\"}",
        );
        let mut untracked = live_row(
            "01920000-0000-7000-8000-000000000413",
            "01920000-0000-7000-8000-0000000000a1",
            "{\"id\":\"01920000-0000-7000-8000-000000000413\",\"parent_id\":null,\"name\":\"docs\"}",
        );
        untracked.untracked = true;
        let mut global = live_row(
            "01920000-0000-7000-8000-000000000363",
            "ffffffff-ffff-7fff-bfff-ffffffffffff",
            "{\"id\":\"01920000-0000-7000-8000-000000000363\",\"parent_id\":null,\"name\":\"docs\"}",
        );
        global.global = true;
        let other = live_row(
            "01920000-0000-7000-8000-000000000383",
            "01920000-0000-7000-8000-0000000000a1",
            "{\"id\":\"01920000-0000-7000-8000-000000000383\",\"parent_id\":null,\"name\":\"other\"}",
        );
        let index = Arc::new(
            path_index_from_rows(vec![tracked, untracked, global, other])
                .expect("filesystem path index should build"),
        );
        let filesystem_path_index: Arc<dyn FilesystemPathIndexReader> =
            Arc::new(StaticFilesystemPathIndexReader {
                index,
                request_count: Arc::new(AtomicUsize::new(0)),
            });
        let mut write_context = CapturingWriteContext {
            rows: Vec::new(),
            writes: Vec::new(),
            reject_scans: true,
        };
        let candidates = {
            let write_ctx = SqlWriteContext::new(&mut write_context);
            let spec = LixDirectorySpec::active_branch(
                "01920000-0000-7000-8000-0000000000a1",
                Arc::new(RejectingHotStateReader {
                    scan_count: Arc::new(AtomicUsize::new(0)),
                }),
                filesystem_path_index,
                Arc::new(TestBranchRefReader),
                test_functions(),
            );
            spec.scan_conflict_candidates(
                &write_ctx,
                &active_directory_path_insert_batch("/docs"),
                &UpsertConflictTarget::path(&["path"]),
            )
            .await
            .expect("path conflicts should route through the filesystem index")
        };

        let ids = candidates
            .column_by_name("id")
            .expect("candidate id column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("candidate id column should be string");
        let globals = candidates
            .column_by_name("lixcol_global")
            .expect("candidate global column")
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("candidate global column should be boolean");
        let untracked = candidates
            .column_by_name("lixcol_untracked")
            .expect("candidate untracked column")
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("candidate untracked column should be boolean");
        let lanes = (0..candidates.num_rows())
            .map(|row| {
                (
                    ids.value(row).to_string(),
                    globals.value(row),
                    untracked.value(row),
                )
            })
            .collect::<BTreeSet<_>>();

        assert_eq!(
            lanes,
            [
                (
                    "01920000-0000-7000-8000-000000000363".to_string(),
                    true,
                    false
                ),
                (
                    "01920000-0000-7000-8000-000000000403".to_string(),
                    false,
                    false
                ),
                (
                    "01920000-0000-7000-8000-000000000413".to_string(),
                    false,
                    true
                ),
            ]
            .into_iter()
            .collect(),
        );
    }

    #[tokio::test]
    async fn directory_path_conflict_candidates_fall_back_when_index_build_fails() {
        let mut write_context = CapturingWriteContext {
            rows: vec![live_row(
                "01920000-0000-7000-8000-0000000000d3",
                "01920000-0000-7000-8000-0000000000a1",
                "{\"id\":\"01920000-0000-7000-8000-0000000000d3\",\"parent_id\":null,\"name\":\"docs\"}",
            )],
            writes: Vec::new(),
            reject_scans: false,
        };
        let candidates = {
            let write_ctx = SqlWriteContext::new(&mut write_context);
            let spec = LixDirectorySpec::active_branch(
                "01920000-0000-7000-8000-0000000000a1",
                Arc::new(RejectingHotStateReader {
                    scan_count: Arc::new(AtomicUsize::new(0)),
                }),
                Arc::new(FailingFilesystemPathIndexReader),
                Arc::new(TestBranchRefReader),
                test_functions(),
            );
            spec.scan_conflict_candidates(
                &write_ctx,
                &active_directory_path_insert_batch("/docs"),
                &UpsertConflictTarget::path(&["path"]),
            )
            .await
            .expect("path conflicts should retain their generic directory scan fallback")
        };

        let ids = candidates
            .column_by_name("id")
            .expect("candidate id column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("candidate id column should be string");
        assert_eq!(candidates.num_rows(), 1);
        assert_eq!(ids.value(0), "01920000-0000-7000-8000-0000000000d3");
    }

    #[tokio::test]
    async fn directory_id_conflict_candidates_keep_generic_row_pk_scan() {
        let indexed = live_row(
            "01920000-0000-7000-8000-000000000373",
            "01920000-0000-7000-8000-0000000000a1",
            "{\"id\":\"01920000-0000-7000-8000-000000000373\",\"parent_id\":null,\"name\":\"docs\"}",
        );
        let filesystem_path_index: Arc<dyn FilesystemPathIndexReader> =
            Arc::new(StaticFilesystemPathIndexReader {
                index: Arc::new(
                    path_index_from_rows(vec![indexed])
                        .expect("filesystem path index should build"),
                ),
                request_count: Arc::new(AtomicUsize::new(0)),
            });
        let mut write_context = CapturingWriteContext {
            rows: vec![live_row(
                "01920000-0000-7000-8000-0000000000d3",
                "01920000-0000-7000-8000-0000000000a1",
                "{\"id\":\"01920000-0000-7000-8000-0000000000d3\",\"parent_id\":null,\"name\":\"docs\"}",
            )],
            writes: Vec::new(),
            reject_scans: false,
        };
        let candidates = {
            let write_ctx = SqlWriteContext::new(&mut write_context);
            let spec = LixDirectorySpec::active_branch(
                "01920000-0000-7000-8000-0000000000a1",
                Arc::new(RejectingHotStateReader {
                    scan_count: Arc::new(AtomicUsize::new(0)),
                }),
                filesystem_path_index,
                Arc::new(TestBranchRefReader),
                test_functions(),
            );
            spec.scan_conflict_candidates(
                &write_ctx,
                &directory_insert_batch(false, false),
                &UpsertConflictTarget::id(&["id"]),
            )
            .await
            .expect("id conflicts should use the row-primary-key scan")
        };

        let ids = candidates
            .column_by_name("id")
            .expect("candidate id column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("candidate id column should be string");
        assert_eq!(candidates.num_rows(), 1);
        assert_eq!(ids.value(0), "01920000-0000-7000-8000-0000000000d3");
    }

    #[test]
    fn directory_provider_keeps_no_write_authority() {
        let mut write_context = CapturingWriteContext::default();
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let hot_state = Arc::new(crate::sql2::WriteContextHotStateReader::new(
            write_ctx.clone(),
        ));
        let branch_ref = Arc::new(crate::sql2::WriteContextBranchRefReader::new(
            write_ctx.clone(),
        ));
        let filesystem_path_index: Arc<dyn FilesystemPathIndexReader> = hot_state.clone();
        let provider = SpecTableProvider::new(Arc::new(LixDirectorySpec::active_branch(
            write_ctx.active_branch_id(),
            hot_state,
            filesystem_path_index,
            branch_ref,
            test_functions(),
        )));
        assert_eq!(
            provider.table_type(),
            datafusion::datasource::TableType::Base
        );
    }
}
