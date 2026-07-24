use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::prelude::SessionContext;
use tokio::sync::Mutex;

use crate::commit_graph::CommitGraphReader;
use crate::{LixError, serialize_row_metadata};

use crate::sql2::SqlHistoryQuerySource;
use crate::sql2::WriteAccess;
use crate::sql2::error::lix_error_to_datafusion_error;
use crate::sql2::history_route::{
    HISTORY_COL_AS_OF_COMMIT_ID, HISTORY_COL_CHANGE_CREATED_AT, HISTORY_COL_CHANGE_ID,
    HISTORY_COL_COMMIT_CREATED_AT, HISTORY_COL_DEPTH, HISTORY_COL_ENTITY_PK, HISTORY_COL_FILE_ID,
    HISTORY_COL_IS_DELETED, HISTORY_COL_METADATA, HISTORY_COL_OBSERVED_COMMIT_ID,
    HISTORY_COL_ORIGIN_KEY, HISTORY_COL_SCHEMA_KEY, HISTORY_COL_SNAPSHOT_CONTENT,
    HistoryMetadataProjection, HistoryRoute, HistoryViewDescriptor, load_history_entries,
    parse_history_filter, validate_history_anchor_filter,
};
use crate::sql2::result_metadata::json_field;
use crate::storage_adapter::StorageAdapterRead;

use super::columns::{Col, ColumnTable, ColumnTableError};
use super::spec::{PlannedScan, TableSpec, projected_schema, register_spec_table, row_source};

pub(super) async fn register_history_provider<S>(
    session: &SessionContext,
    surface_name: &str,
    commit_graph: Box<dyn CommitGraphReader>,
    query_source: SqlHistoryQuerySource<S>,
) -> Result<(), LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    register_spec_table(
        session,
        surface_name,
        Arc::new(StateHistorySpec {
            commit_graph: Arc::new(Mutex::new(commit_graph)),
            query_source,
        }),
        WriteAccess::read_only(),
    )
}

/// SQL spec for `lix_state_history`.
///
/// The reachability-aware history surface over canonical state changes: rows
/// are loaded by walking the commit graph from the routed anchor commits.
struct StateHistorySpec<S> {
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlHistoryQuerySource<S>,
}

#[async_trait]
impl<S> TableSpec for StateHistorySpec<S>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    #[expect(clippy::unnecessary_literal_bound)]
    fn table_name(&self) -> &str {
        "lix_state_history"
    }

    fn schema(&self) -> SchemaRef {
        lix_state_history_schema()
    }

    fn history_anchor_column(&self) -> Option<&'static str> {
        Some(HISTORY_COL_AS_OF_COMMIT_ID)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn filter_pushdown(&self, filter: &Expr) -> TableProviderFilterPushDown {
        if parse_history_filter(filter).is_some() {
            TableProviderFilterPushDown::Exact
        } else {
            TableProviderFilterPushDown::Unsupported
        }
    }

    fn validate_filter_pushdown(&self, filter: &Expr) -> Result<()> {
        validate_history_anchor_filter(filter).map_err(lix_error_to_datafusion_error)
    }

    async fn plan_scan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        _props: &ExecutionProps,
    ) -> Result<PlannedScan> {
        let schema = projected_schema(&lix_state_history_schema(), projection);
        let mut route = HistoryRoute::from_filters(filters);
        route.default_to_as_of_commit_id(&self.query_source.default_as_of_commit_id);
        let metadata_projection = HistoryMetadataProjection::from_scan(&schema, filters);
        Ok(PlannedScan {
            schema: Arc::clone(&schema),
            ordering: None,
            load: row_source(
                (
                    Arc::clone(&self.commit_graph),
                    self.query_source.clone(),
                    route,
                    schema,
                    metadata_projection,
                ),
                move |(commit_graph, query_source, route, schema, metadata_projection)| async move {
                    let rows = if route.is_contradictory() {
                        Vec::new()
                    } else {
                        load_state_history_rows(
                            commit_graph,
                            query_source,
                            &route,
                            metadata_projection,
                        )
                        .await
                        .map_err(lix_error_to_datafusion_error)?
                    };
                    let rows = if let Some(limit) = limit {
                        rows.into_iter().take(limit).collect::<Vec<_>>()
                    } else {
                        rows
                    };
                    LIX_STATE_HISTORY_COLS
                        .build(schema, &rows)
                        .map_err(state_history_batch_error)
                },
            ),
        })
    }
}

pub(super) fn lix_state_history_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        json_field(HISTORY_COL_ENTITY_PK, false),
        Field::new(HISTORY_COL_SCHEMA_KEY, DataType::Utf8, false),
        Field::new(HISTORY_COL_FILE_ID, DataType::Utf8, true),
        json_field(HISTORY_COL_SNAPSHOT_CONTENT, true),
        json_field(HISTORY_COL_METADATA, true),
        Field::new(HISTORY_COL_CHANGE_ID, DataType::Utf8, false),
        Field::new(HISTORY_COL_CHANGE_CREATED_AT, DataType::Utf8, false),
        Field::new(HISTORY_COL_ORIGIN_KEY, DataType::Utf8, true),
        Field::new(HISTORY_COL_OBSERVED_COMMIT_ID, DataType::Utf8, false),
        Field::new(HISTORY_COL_COMMIT_CREATED_AT, DataType::Utf8, false),
        Field::new(HISTORY_COL_AS_OF_COMMIT_ID, DataType::Utf8, false),
        Field::new(HISTORY_COL_DEPTH, DataType::Int64, false),
        Field::new(HISTORY_COL_IS_DELETED, DataType::Boolean, false),
    ]))
}

/// Project a single-string history entity pk as the canonical JSON array
/// text the `lixcol_entity_pk` column exposes. Shared by the file and
/// directory history surfaces.
pub(super) fn entity_pk_json_array(entity_pk: &str) -> Result<String, LixError> {
    serde_json::to_string(&[entity_pk]).map_err(|error| {
        LixError::unknown(format!(
            "failed to encode history entity pk as JSON: {error}"
        ))
    })
}

#[derive(Debug, Clone)]
struct StateHistorySqlRow {
    entity_pk: String,
    schema_key: String,
    file_id: Option<String>,
    snapshot_content: Option<String>,
    metadata: Option<String>,
    change_id: String,
    change_created_at: String,
    origin_key: Option<String>,
    observed_commit_id: String,
    commit_created_at: Option<String>,
    as_of_commit_id: String,
    depth: i64,
    is_deleted: bool,
}

static LIX_STATE_HISTORY_COLS: ColumnTable<StateHistorySqlRow> = ColumnTable {
    columns: &[
        (
            HISTORY_COL_ENTITY_PK,
            Col::Utf8(|row| Some(row.entity_pk.as_str())),
        ),
        (
            HISTORY_COL_SCHEMA_KEY,
            Col::Utf8(|row| Some(row.schema_key.as_str())),
        ),
        (HISTORY_COL_FILE_ID, Col::Utf8(|row| row.file_id.as_deref())),
        (
            HISTORY_COL_SNAPSHOT_CONTENT,
            Col::Utf8(|row| row.snapshot_content.as_deref()),
        ),
        (
            HISTORY_COL_METADATA,
            Col::Utf8Owned(|row| row.metadata.as_deref().map(serialize_row_metadata)),
        ),
        (
            HISTORY_COL_CHANGE_ID,
            Col::Utf8(|row| Some(row.change_id.as_str())),
        ),
        (
            HISTORY_COL_CHANGE_CREATED_AT,
            Col::Utf8(|row| Some(row.change_created_at.as_str())),
        ),
        (
            HISTORY_COL_ORIGIN_KEY,
            Col::Utf8(|row| row.origin_key.as_deref()),
        ),
        (
            HISTORY_COL_OBSERVED_COMMIT_ID,
            Col::Utf8(|row| Some(row.observed_commit_id.as_str())),
        ),
        (
            HISTORY_COL_COMMIT_CREATED_AT,
            Col::Utf8(|row| row.commit_created_at.as_deref()),
        ),
        (
            HISTORY_COL_AS_OF_COMMIT_ID,
            Col::Utf8(|row| Some(row.as_of_commit_id.as_str())),
        ),
        (HISTORY_COL_DEPTH, Col::I64(|row| Some(row.depth))),
        (
            HISTORY_COL_IS_DELETED,
            Col::Bool(|row| Some(row.is_deleted)),
        ),
    ],
};

fn state_history_batch_error(error: ColumnTableError) -> DataFusionError {
    match error {
        ColumnTableError::UnsupportedColumn(column) => DataFusionError::Execution(format!(
            "lix_state_history provider does not support projected column '{column}'"
        )),
        ColumnTableError::Arrow(error) => DataFusionError::from(error),
        ColumnTableError::ArrowZeroColumn(error) => DataFusionError::Execution(format!(
            "failed to build zero-column lix_state_history batch: {error}"
        )),
        ColumnTableError::Row(error) => lix_error_to_datafusion_error(error),
    }
}

async fn load_state_history_rows<S>(
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlHistoryQuerySource<S>,
    route: &HistoryRoute,
    metadata_projection: HistoryMetadataProjection,
) -> Result<Vec<StateHistorySqlRow>, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    let entries = load_history_entries(
        HistoryViewDescriptor {
            view_name: "lix_state_history",
            as_of_commit_column: HISTORY_COL_AS_OF_COMMIT_ID,
        },
        commit_graph,
        query_source,
        route,
        Vec::new(),
        metadata_projection,
    )
    .await?;
    let mut rows = entries
        .into_iter()
        .map(|entry| -> Result<StateHistorySqlRow, LixError> {
            Ok(StateHistorySqlRow {
                entity_pk: entry.change.entity_pk.as_json_array_text()?,
                schema_key: entry.change.schema_key,
                file_id: entry.change.file_id,
                is_deleted: entry.change.snapshot_content.is_none(),
                snapshot_content: entry.change.snapshot_content,
                metadata: entry.change.metadata,
                change_id: entry.change.id,
                change_created_at: entry.change.created_at,
                origin_key: entry.change.origin_key,
                observed_commit_id: entry.observed_commit_id,
                commit_created_at: entry.commit_created_at,
                as_of_commit_id: entry.as_of_commit_id,
                depth: i64::from(entry.depth),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    rows.sort_by(|left, right| {
        left.entity_pk
            .cmp(&right.entity_pk)
            .then(left.file_id.cmp(&right.file_id))
            .then(left.schema_key.cmp(&right.schema_key))
            .then(left.depth.cmp(&right.depth))
            .then(left.change_id.cmp(&right.change_id))
    });
    Ok(rows)
}
