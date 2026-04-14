use async_trait::async_trait;
use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, Ident, ObjectName, ObjectNamePart, Query, Select,
    SelectFlavor, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Value as SqlValue,
    ValueWithSpan,
};

use crate::catalog::{
    CatalogProjectionRegistry, CatalogReadTimeProjectionRequest, SurfaceReadFreshness,
    SurfaceRegistry,
};
use crate::execution::{
    execute_prepared_public_read_artifact_with_backend, ReadExecutionHost,
    ReadTimeProjectionIdentity, ReadTimeProjectionRow,
};
use crate::functions::DynFunctionProvider;
use crate::sql::load_sql_compiler_metadata;
use crate::sql::{
    prepare_public_read, prepare_public_read_artifact, public_selector_column_name,
    public_selector_version_column, CanonicalStateRowKey, PlannedWrite, PreparedPublicRead,
    PublicReadSource, ScopeProof, SqlCompilerMetadata, SqlPreparationMetadataReader,
};
use crate::transaction::{PendingOverlay, WriteResolveError, WriteSelectorResolver};
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackend, LixBackendTransaction, LixError, QueryResult, Value};

use super::state_write_target_resolver::try_resolve_state_write_targets_with_backend;

pub(crate) struct TransactionWriteSelectorResolver<'a> {
    backend: &'a dyn LixBackend,
    projection_registry: &'a CatalogProjectionRegistry,
    pending_overlay: Option<&'a dyn PendingOverlay>,
    preparation_context: ReadPreparationContext,
}

impl<'a> TransactionWriteSelectorResolver<'a> {
    pub(crate) async fn new(
        backend: &'a dyn LixBackend,
        projection_registry: &'a CatalogProjectionRegistry,
        pending_overlay: Option<&'a dyn PendingOverlay>,
        functions: &DynFunctionProvider,
    ) -> Result<Self, LixError> {
        let preparation_context =
            build_read_preparation_context(backend, pending_overlay, functions).await?;
        Ok(Self {
            backend,
            projection_registry,
            pending_overlay,
            preparation_context,
        })
    }

    async fn execute_public_selector_query(
        &self,
        planned_write: &PlannedWrite,
        selector_columns: &[&str],
    ) -> Result<QueryResult, LixError> {
        let statement = Statement::Query(Box::new(build_public_selector_query(
            planned_write,
            selector_columns,
        )));
        let active_version_id = selector_read_preparation_version_id(planned_write);
        let artifact = prepare_required_active_public_read_artifact_with_backend(
            self.backend,
            &self.preparation_context,
            &[statement],
            &planned_write.command.bound_parameters,
            active_version_id,
            planned_write
                .command
                .statement_context
                .writer_key
                .as_deref(),
        )
        .await?;
        execute_pending_overlay_public_read_with_backend(
            self.backend,
            self,
            self.pending_overlay,
            &artifact,
        )
        .await
    }
}

#[async_trait(?Send)]
impl ReadExecutionHost for TransactionWriteSelectorResolver<'_> {
    async fn derive_read_time_projection_rows(
        &self,
        backend: &dyn LixBackend,
        request: &CatalogReadTimeProjectionRequest,
    ) -> Result<Vec<ReadTimeProjectionRow>, LixError> {
        derive_read_time_projection_rows_with_registry(self.projection_registry, backend, request)
            .await
    }

    async fn ensure_projection_freshness_with_backend(
        &self,
        backend: &dyn LixBackend,
        freshness_contract: SurfaceReadFreshness,
        resolved_relations: &[String],
    ) -> Result<(), LixError> {
        crate::live_state::ensure_projection_read_freshness_with_backend(
            backend,
            freshness_contract,
            resolved_relations,
        )
        .await
    }

    async fn ensure_projection_freshness_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        freshness_contract: SurfaceReadFreshness,
        resolved_relations: &[String],
    ) -> Result<(), LixError> {
        crate::live_state::ensure_projection_read_freshness_in_transaction(
            transaction,
            freshness_contract,
            resolved_relations,
        )
        .await
    }
}

fn selector_read_preparation_version_id(planned_write: &PlannedWrite) -> &str {
    match &planned_write.scope_proof {
        ScopeProof::SingleVersion(version_id) => version_id,
        ScopeProof::FiniteVersionSet(version_ids) if version_ids.len() == 1 => version_ids
            .iter()
            .next()
            .expect("singleton finite version-set proof"),
        ScopeProof::GlobalAdmin => GLOBAL_VERSION_ID,
        ScopeProof::ActiveVersion
        | ScopeProof::FiniteVersionSet(_)
        | ScopeProof::Unknown
        | ScopeProof::Unbounded => planned_write
            .command
            .statement_context
            .requested_version_id
            .as_deref()
            .unwrap_or(GLOBAL_VERSION_ID),
    }
}

#[async_trait(?Send)]
impl WriteSelectorResolver for TransactionWriteSelectorResolver<'_> {
    async fn load_text_selector_values(
        &self,
        planned_write: &PlannedWrite,
        selector_column: &str,
        error_message: &str,
    ) -> Result<Vec<String>, WriteResolveError> {
        let query_result = self
            .execute_public_selector_query(planned_write, &[selector_column])
            .await
            .map_err(write_resolve_backend_error)?;
        let mut values = Vec::new();
        for row in query_result.rows {
            let Some(value) = row.first().and_then(text_from_value) else {
                return Err(WriteResolveError {
                    message: error_message.to_string(),
                });
            };
            if !values.iter().any(|existing| existing == &value) {
                values.push(value);
            }
        }
        Ok(values)
    }

    async fn load_entity_selector_rows(
        &self,
        planned_write: &PlannedWrite,
    ) -> Result<Vec<CanonicalStateRowKey>, WriteResolveError> {
        let selector = canonical_state_selector(planned_write);
        let mut selector_columns = vec!["lixcol_entity_id"];
        if let Some(version_column) = selector.version_column.as_deref() {
            selector_columns.push(version_column);
        }
        let query_result = self
            .execute_public_selector_query(planned_write, &selector_columns)
            .await
            .map_err(write_resolve_backend_error)?;

        let mut selector_rows = Vec::new();
        for row in query_result.rows {
            let selector_row = CanonicalStateRowKey {
                entity_id: required_text_value_index(&row, 0, "lixcol_entity_id")?,
                file_id: None,
                plugin_key: None,
                schema_version: None,
                version_id: selector
                    .version_column
                    .as_deref()
                    .map(|version_column| required_text_value_index(&row, 1, version_column))
                    .transpose()?,
                global: None,
                untracked: None,
                writer_key: None,
            };
            if !selector_rows
                .iter()
                .any(|existing| existing == &selector_row)
            {
                selector_rows.push(selector_row);
            }
        }
        Ok(selector_rows)
    }

    async fn load_state_selector_rows(
        &self,
        planned_write: &PlannedWrite,
    ) -> Result<Vec<CanonicalStateRowKey>, WriteResolveError> {
        if let Some(rows) = try_resolve_state_write_targets_with_backend(
            self.backend,
            self.pending_overlay,
            planned_write,
        )
        .await
        .map_err(write_resolve_backend_error)?
        {
            return Ok(rows);
        }

        let selector = canonical_state_selector(planned_write);
        let mut selector_columns = vec!["entity_id", "file_id", "plugin_key", "schema_version"];
        if let Some(version_column) = selector.version_column.as_deref() {
            selector_columns.push(version_column);
        }
        selector_columns.push("global");
        selector_columns.push("untracked");
        let query_result = self
            .execute_public_selector_query(planned_write, &selector_columns)
            .await
            .map_err(write_resolve_backend_error)?;

        let mut selector_rows = Vec::new();
        for row in query_result.rows {
            let version_offset = usize::from(selector.version_column.is_some());
            let selector_row = CanonicalStateRowKey {
                entity_id: required_text_value_index(&row, 0, "entity_id")?,
                file_id: Some(required_text_value_index(&row, 1, "file_id")?),
                plugin_key: Some(required_text_value_index(&row, 2, "plugin_key")?),
                schema_version: Some(required_text_value_index(&row, 3, "schema_version")?),
                version_id: selector
                    .version_column
                    .as_deref()
                    .map(|version_column| required_text_value_index(&row, 4, version_column))
                    .transpose()?,
                global: Some(required_bool_value_index(
                    &row,
                    4 + version_offset,
                    "global",
                )?),
                untracked: Some(required_bool_value_index(
                    &row,
                    5 + version_offset,
                    "untracked",
                )?),
                writer_key: None,
            };
            if !selector_rows
                .iter()
                .any(|existing| existing == &selector_row)
            {
                selector_rows.push(selector_row);
            }
        }
        Ok(selector_rows)
    }
}

struct ReadPreparationContext {
    registry: SurfaceRegistry,
    compiler_metadata: SqlCompilerMetadata,
}

async fn build_read_preparation_context(
    backend: &dyn LixBackend,
    pending_overlay: Option<&dyn PendingOverlay>,
    functions: &DynFunctionProvider,
) -> Result<ReadPreparationContext, LixError> {
    let registry = crate::transaction::build_public_read_surface_registry_with_pending_overlay(
        backend,
        pending_overlay,
        functions,
    )
    .await?;
    let compiler_metadata = load_sql_compiler_metadata(backend, &registry).await?;
    Ok(ReadPreparationContext {
        registry,
        compiler_metadata,
    })
}

async fn prepare_required_active_public_read_artifact_with_backend(
    backend: &dyn LixBackend,
    preparation_context: &ReadPreparationContext,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
) -> Result<PreparedPublicRead, LixError> {
    let mut metadata_reader = backend;
    prepare_required_active_public_read_artifact_with_reader(
        &mut metadata_reader,
        backend.dialect(),
        preparation_context,
        parsed_statements,
        params,
        active_version_id,
        writer_key,
    )
    .await
}

async fn prepare_required_active_public_read_artifact_with_reader(
    metadata_reader: &mut dyn SqlPreparationMetadataReader,
    dialect: crate::SqlDialect,
    preparation_context: &ReadPreparationContext,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
) -> Result<PreparedPublicRead, LixError> {
    let active_history_root_commit_id = metadata_reader
        .load_active_history_root_commit_id_for_preparation(active_version_id)
        .await?;
    let prepared = prepare_public_read(
        dialect,
        &preparation_context.registry,
        &preparation_context.compiler_metadata,
        parsed_statements,
        params,
        active_version_id,
        active_history_root_commit_id.as_deref(),
        writer_key,
        false,
        None,
    )
    .await?;
    let Some(public_read) = prepared else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "public write selector resolver expected a public read plan",
        ));
    };
    prepare_public_read_artifact(&public_read, dialect)
}

async fn derive_read_time_projection_rows_with_registry(
    projection_registry: &CatalogProjectionRegistry,
    backend: &dyn LixBackend,
    request: &CatalogReadTimeProjectionRequest,
) -> Result<Vec<ReadTimeProjectionRow>, LixError> {
    Ok(
        crate::live_state::derive_read_time_surface_rows(backend, projection_registry, request)
            .await?
            .into_iter()
            .map(|row| ReadTimeProjectionRow {
                surface_name: row.surface_name,
                identity: row.identity.map(|identity| ReadTimeProjectionIdentity {
                    schema_key: identity.schema_key,
                    version_id: identity.version_id,
                    entity_id: identity.entity_id,
                    file_id: identity.file_id,
                }),
                values: row.values,
            })
            .collect(),
    )
}

async fn execute_pending_overlay_public_read_with_backend(
    backend: &dyn LixBackend,
    host: &dyn ReadExecutionHost,
    pending_overlay: Option<&dyn PendingOverlay>,
    public_read: &PreparedPublicRead,
) -> Result<QueryResult, LixError> {
    match public_read.contract.source() {
        PublicReadSource::PendingOverlay => {
            crate::transaction::execute_pending_overlay_public_read(
                backend,
                pending_overlay,
                public_read,
            )
            .await
        }
        PublicReadSource::Committed(_) => {
            execute_prepared_public_read_artifact_with_backend(backend, host, public_read).await
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct SelectorQuery {
    predicates: Vec<Expr>,
    version_column: Option<String>,
}

fn canonical_state_selector(planned_write: &PlannedWrite) -> SelectorQuery {
    let predicates = if planned_write.command.selector.exact_only {
        exact_selector_predicates(planned_write)
            .unwrap_or_else(|| planned_write.command.selector.residual_predicates.clone())
    } else {
        planned_write.command.selector.residual_predicates.clone()
    };
    let version_column = planned_write
        .command
        .target
        .implicit_overrides
        .expose_version_id
        .then(|| {
            public_selector_version_column(planned_write.command.target.descriptor.surface_family)
                .to_string()
        });
    SelectorQuery {
        predicates,
        version_column,
    }
}

fn exact_selector_predicates(planned_write: &PlannedWrite) -> Option<Vec<Expr>> {
    let mut predicates = Vec::with_capacity(planned_write.command.selector.exact_filters.len());
    for (column, value) in &planned_write.command.selector.exact_filters {
        let public_column = public_selector_column_name(
            planned_write.command.target.descriptor.surface_family,
            column,
        )?;
        predicates.push(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new(public_column))),
            op: BinaryOperator::Eq,
            right: Box::new(engine_value_to_sql_expr(value)),
        });
    }
    Some(predicates)
}

fn engine_value_to_sql_expr(value: &Value) -> Expr {
    match value {
        Value::Null => Expr::Value(ValueWithSpan::from(SqlValue::Null)),
        Value::Boolean(value) => Expr::Value(ValueWithSpan::from(SqlValue::Boolean(*value))),
        Value::Text(value) => Expr::Value(ValueWithSpan::from(SqlValue::SingleQuotedString(
            value.clone(),
        ))),
        Value::Json(value) => Expr::Value(ValueWithSpan::from(SqlValue::SingleQuotedString(
            value.to_string(),
        ))),
        Value::Integer(value) => Expr::Value(ValueWithSpan::from(SqlValue::Number(
            value.to_string(),
            false,
        ))),
        Value::Real(value) => Expr::Value(ValueWithSpan::from(SqlValue::Number(
            value.to_string(),
            false,
        ))),
        Value::Blob(value) => Expr::Value(ValueWithSpan::from(
            SqlValue::SingleQuotedByteStringLiteral(String::from_utf8_lossy(value).to_string()),
        )),
    }
}

fn build_public_selector_query(planned_write: &PlannedWrite, selector_columns: &[&str]) -> Query {
    let selector = canonical_state_selector(planned_write);
    let selection = selector
        .predicates
        .iter()
        .cloned()
        .reduce(|left, right| Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::And,
            right: Box::new(right),
        });

    Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(Select {
            select_token: AttachedToken::empty(),
            distinct: None,
            top: None,
            top_before_distinct: false,
            projection: selector_columns
                .iter()
                .map(|column| SelectItem::UnnamedExpr(Expr::Identifier(Ident::new(*column))))
                .collect(),
            exclude: None,
            into: None,
            from: vec![TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
                        &planned_write.command.target.descriptor.public_name,
                    ))]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    with_ordinality: false,
                    partitions: vec![],
                    json_path: None,
                    sample: None,
                    index_hints: vec![],
                },
                joins: Vec::new(),
            }],
            lateral_views: Vec::new(),
            prewhere: None,
            selection,
            group_by: GroupByExpr::Expressions(Vec::new(), Vec::new()),
            cluster_by: Vec::new(),
            distribute_by: Vec::new(),
            sort_by: Vec::new(),
            having: None,
            named_window: Vec::new(),
            qualify: None,
            window_before_qualify: false,
            value_table_mode: None,
            connect_by: None,
            flavor: SelectFlavor::Standard,
        }))),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: Vec::new(),
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: Vec::new(),
    }
}

fn required_text_value_index(
    row: &[Value],
    index: usize,
    label: &str,
) -> Result<String, WriteResolveError> {
    row.get(index)
        .and_then(text_from_value)
        .ok_or_else(|| WriteResolveError {
            message: format!("public selector resolver expected text {}", label),
        })
}

fn required_bool_value_index(
    row: &[Value],
    index: usize,
    label: &str,
) -> Result<bool, WriteResolveError> {
    row.get(index)
        .and_then(bool_from_value)
        .ok_or_else(|| WriteResolveError {
            message: format!("public selector resolver expected bool {}", label),
        })
}

fn text_from_value(value: &Value) -> Option<String> {
    match value {
        Value::Text(value) => Some(value.clone()),
        Value::Integer(value) => Some(value.to_string()),
        Value::Boolean(value) => Some(value.to_string()),
        Value::Real(value) => Some(value.to_string()),
        _ => None,
    }
}

fn bool_from_value(value: &Value) -> Option<bool> {
    match value {
        Value::Boolean(value) => Some(*value),
        Value::Integer(value) => Some(*value != 0),
        Value::Text(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" => Some(true),
            "0" | "false" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn write_resolve_backend_error(error: LixError) -> WriteResolveError {
    WriteResolveError {
        message: error.description,
    }
}
