use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result;
use datafusion::datasource::TableType;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::LixError;
use crate::commit_graph::CommitGraphReader;
use crate::serialize_row_metadata;

use crate::sql2::SqlHistoryQuerySource;
use crate::sql2::WriteAccess;
use crate::sql2::change_materialization::MaterializedChange;
use crate::sql2::error::lix_error_to_datafusion_error;
use crate::sql2::history_projection::{HistoryIdentityProjection, tombstone_identity_column_value};
use crate::sql2::history_route::{
    HISTORY_COL_CHANGE_ID, HISTORY_COL_COMMIT_CREATED_AT, HISTORY_COL_DEPTH, HISTORY_COL_ENTITY_PK,
    HISTORY_COL_FILE_ID, HISTORY_COL_METADATA, HISTORY_COL_OBSERVED_COMMIT_ID,
    HISTORY_COL_ORIGIN_KEY, HISTORY_COL_SCHEMA_KEY, HISTORY_COL_SNAPSHOT_CONTENT,
    HISTORY_COL_START_COMMIT_ID, HistoryColumnStyle, HistoryEntry, HistoryMetadataProjection,
    HistoryRoute, HistoryViewDescriptor, history_descriptor_event_matches, load_history_entries,
    parse_history_filter,
};
use crate::sql2::providers::filesystem_history_path::{
    HistoryDirectoryPathRecord, resolve_history_directory_path,
};
use crate::sql2::result_metadata::json_field;
use crate::storage_adapter::StorageAdapterRead;

use super::columns::{Col, ColumnTable, ColumnTableError};
use super::history::entity_pk_json_array;
use super::spec::{PlannedScan, TableSpec, projected_schema, register_spec_table, row_source};

const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";

pub(super) async fn register_lix_directory_history_surface<S>(
    session: &datafusion::prelude::SessionContext,
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
        Arc::new(LixDirectoryHistorySpec {
            schema: lix_directory_history_schema(),
            commit_graph: Arc::new(Mutex::new(commit_graph)),
            query_source,
        }),
        WriteAccess::read_only(),
    )
}

struct LixDirectoryHistorySpec<S> {
    schema: SchemaRef,
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlHistoryQuerySource<S>,
}

#[async_trait]
impl<S> TableSpec for LixDirectoryHistorySpec<S>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    #[expect(clippy::unnecessary_literal_bound)]
    fn table_name(&self) -> &str {
        "lix_directory_history"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn filter_pushdown(&self, filter: &Expr) -> TableProviderFilterPushDown {
        if parse_history_filter(filter, HistoryColumnStyle::Prefixed).is_some() {
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
        _props: &ExecutionProps,
    ) -> Result<PlannedScan> {
        let schema = projected_schema(&self.schema, projection);
        let route = HistoryRoute::from_filters(filters, HistoryColumnStyle::Prefixed);
        let metadata_projection =
            HistoryMetadataProjection::from_scan(&schema, filters, HistoryColumnStyle::Prefixed);
        Ok(PlannedScan {
            schema: Arc::clone(&schema),
            ordering: None,
            load: row_source(
                (
                    Arc::clone(&self.commit_graph),
                    self.query_source.clone(),
                    schema,
                    route,
                    limit,
                    metadata_projection,
                ),
                |(commit_graph, query_source, schema, route, limit, metadata_projection)| async move {
                    let mut rows = load_directory_history_rows(
                        commit_graph,
                        query_source,
                        &route,
                        metadata_projection,
                    )
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
                    if let Some(limit) = limit {
                        rows.truncate(limit);
                    }
                    LIX_DIRECTORY_HISTORY_COLS
                        .build(schema, &rows)
                        .map_err(directory_history_batch_error)
                        .map_err(lix_error_to_datafusion_error)
                },
            ),
        })
    }
}

#[derive(Debug, Clone)]
struct DirectoryHistoryRecord {
    id: String,
    parent_id: Option<String>,
    name: Option<String>,
    entry: HistoryEntry,
}

impl HistoryDirectoryPathRecord for DirectoryHistoryRecord {
    fn id(&self) -> &str {
        &self.id
    }

    fn parent_id(&self) -> Option<&str> {
        self.parent_id.as_deref()
    }

    fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    fn entry(&self) -> &HistoryEntry {
        &self.entry
    }
}

#[derive(Debug, Clone)]
struct DirectoryHistoryOutputRow {
    entity_pk: String,
    id: String,
    path: Option<String>,
    parent_id: Option<String>,
    name: Option<String>,
    descriptor_change: MaterializedChange,
    event: DirectoryHistoryEvent,
}

#[derive(Debug, Clone)]
struct DirectoryHistoryEvent {
    directory_id: String,
    start_commit_id: String,
    depth: u32,
    change: MaterializedChange,
    observed_commit_id: String,
    commit_created_at: String,
}

#[derive(Debug, Deserialize)]
struct DirectoryDescriptorSnapshot {
    id: String,
    parent_id: Option<String>,
    name: String,
}

async fn load_directory_history_rows<S>(
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlHistoryQuerySource<S>,
    route: &HistoryRoute,
    metadata_projection: HistoryMetadataProjection,
) -> Result<Vec<DirectoryHistoryOutputRow>, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    let event_route = route.traversal_only();
    let context_route = route.starts_only();
    let (event_entries, context_entries) =
        load_directory_history_entry_sets(&event_route, &context_route, move |route| {
            let commit_graph = Arc::clone(&commit_graph);
            let json_reader = query_source.json_reader.clone();
            async move {
                load_history_entries(
                    HistoryViewDescriptor {
                        view_name: "lix_directory_history",
                        start_commit_column: HISTORY_COL_START_COMMIT_ID,
                    },
                    commit_graph,
                    json_reader,
                    &route,
                    vec![DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string()],
                    metadata_projection,
                )
                .await
            }
        })
        .await?;
    let event_descriptors = parse_directory_history_records(&event_entries)?;
    let descriptors = parse_directory_history_records(&context_entries)?;
    let mut output = Vec::new();

    for descriptor in &event_descriptors {
        let event = directory_history_event_from_entry(&descriptor.id, &descriptor.entry);
        let Some(visible_descriptor) = nearest_directory_descriptor(&descriptors, &event) else {
            continue;
        };
        let path = if visible_descriptor.name.is_some() {
            resolve_history_directory_path(
                &visible_descriptor.id,
                &event.start_commit_id,
                event.depth,
                &descriptors,
                &mut BTreeMap::new(),
                &mut BTreeSet::new(),
            )
        } else {
            None
        };
        let id = tombstone_identity_column_value(
            "id",
            &visible_descriptor.id,
            HistoryIdentityProjection::SingleColumn { column: "id" },
        )?
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| visible_descriptor.id.clone());
        output.push(DirectoryHistoryOutputRow {
            entity_pk: visible_descriptor.id.clone(),
            id,
            path,
            parent_id: visible_descriptor.parent_id.clone(),
            name: visible_descriptor.name.clone(),
            descriptor_change: visible_descriptor.entry.change.clone(),
            event,
        });
    }
    output.retain(|row| {
        let entity_pk = entity_pk_json_array(&row.entity_pk).ok();
        route.matches_surface_row(
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
            entity_pk.as_deref().unwrap_or(&row.entity_pk),
            None,
            row.event.depth,
        )
    });

    output.sort_by(|left, right| {
        left.entity_pk
            .cmp(&right.entity_pk)
            .then(left.event.start_commit_id.cmp(&right.event.start_commit_id))
            .then(left.event.depth.cmp(&right.event.depth))
            .then(
                left.event
                    .observed_commit_id
                    .cmp(&right.event.observed_commit_id),
            )
            .then(left.event.change.id.cmp(&right.event.change.id))
    });
    Ok(output)
}

async fn load_directory_history_entry_sets<Load, LoadFuture>(
    event_route: &HistoryRoute,
    context_route: &HistoryRoute,
    load: Load,
) -> Result<(Vec<HistoryEntry>, Vec<HistoryEntry>), LixError>
where
    Load: Fn(HistoryRoute) -> LoadFuture,
    LoadFuture: Future<Output = Result<Vec<HistoryEntry>, LixError>>,
{
    let event_entries = load(event_route.clone()).await?;
    let context_entries = if event_route == context_route {
        event_entries.clone()
    } else {
        load(context_route.clone()).await?
    };
    Ok((event_entries, context_entries))
}

fn parse_directory_history_records(
    entries: &[HistoryEntry],
) -> Result<Vec<DirectoryHistoryRecord>, LixError> {
    entries
        .iter()
        .filter(|entry| entry.change.schema_key == DIRECTORY_DESCRIPTOR_SCHEMA_KEY)
        .map(|entry| {
            let Some(snapshot_content) = entry.change.snapshot_content.as_deref() else {
                return Ok(DirectoryHistoryRecord {
                    id: entry.change.entity_pk.as_single_string_owned()?,
                    parent_id: None,
                    name: None,
                    entry: entry.clone(),
                });
            };
            let snapshot: DirectoryDescriptorSnapshot = serde_json::from_str(snapshot_content)
                .map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid lix_directory_descriptor history snapshot JSON: {error}"),
                    )
                })?;
            Ok(DirectoryHistoryRecord {
                id: snapshot.id,
                parent_id: snapshot.parent_id,
                name: Some(snapshot.name),
                entry: entry.clone(),
            })
        })
        .collect()
}

fn directory_history_event_from_entry(
    directory_id: &str,
    entry: &HistoryEntry,
) -> DirectoryHistoryEvent {
    DirectoryHistoryEvent {
        directory_id: directory_id.to_string(),
        start_commit_id: entry.start_commit_id.clone(),
        depth: entry.depth,
        change: entry.change.clone(),
        observed_commit_id: entry.observed_commit_id.clone(),
        commit_created_at: entry.commit_created_at.clone(),
    }
}

fn nearest_directory_descriptor<'a>(
    descriptors: &'a [DirectoryHistoryRecord],
    event: &DirectoryHistoryEvent,
) -> Option<&'a DirectoryHistoryRecord> {
    descriptors
        .iter()
        .filter(|descriptor| {
            let exact_descriptor_event =
                history_descriptor_event_matches(&descriptor.entry, event.depth, &event.change.id);
            (exact_descriptor_event || descriptor.name.is_some())
                && descriptor.id == event.directory_id
                && descriptor.entry.start_commit_id == event.start_commit_id
                && descriptor.entry.depth >= event.depth
        })
        .min_by(|left, right| {
            left.entry
                .depth
                .cmp(&right.entry.depth)
                .then(left.entry.change.id.cmp(&right.entry.change.id))
        })
}

static LIX_DIRECTORY_HISTORY_COLS: ColumnTable<DirectoryHistoryOutputRow> = ColumnTable {
    columns: &[
        ("id", Col::Utf8(|row| Some(row.id.as_str()))),
        ("path", Col::Utf8(|row| row.path.as_deref())),
        ("parent_id", Col::Utf8(|row| row.parent_id.as_deref())),
        ("name", Col::Utf8(|row| row.name.as_deref())),
        (
            HISTORY_COL_ENTITY_PK,
            Col::Utf8Fallible(|row| entity_pk_json_array(&row.entity_pk).map(Some)),
        ),
        (
            HISTORY_COL_SCHEMA_KEY,
            Col::Utf8(|_row| Some(DIRECTORY_DESCRIPTOR_SCHEMA_KEY)),
        ),
        (HISTORY_COL_FILE_ID, Col::Utf8(|_row| None)),
        (
            HISTORY_COL_CHANGE_ID,
            Col::Utf8(|row| Some(row.event.change.id.as_str())),
        ),
        (
            HISTORY_COL_ORIGIN_KEY,
            Col::Utf8(|row| row.event.change.origin_key.as_deref()),
        ),
        (
            HISTORY_COL_SNAPSHOT_CONTENT,
            Col::Utf8(|row| row.descriptor_change.snapshot_content.as_deref()),
        ),
        (
            HISTORY_COL_METADATA,
            Col::Utf8Owned(|row| {
                row.descriptor_change
                    .metadata
                    .as_deref()
                    .map(serialize_row_metadata)
            }),
        ),
        (
            HISTORY_COL_OBSERVED_COMMIT_ID,
            Col::Utf8(|row| Some(row.event.observed_commit_id.as_str())),
        ),
        (
            HISTORY_COL_COMMIT_CREATED_AT,
            Col::Utf8(|row| Some(row.event.commit_created_at.as_str())),
        ),
        (
            HISTORY_COL_START_COMMIT_ID,
            Col::Utf8(|row| Some(row.event.start_commit_id.as_str())),
        ),
        (
            HISTORY_COL_DEPTH,
            Col::I64(|row| Some(i64::from(row.event.depth))),
        ),
    ],
};

fn directory_history_batch_error(error: ColumnTableError) -> LixError {
    match error {
        ColumnTableError::UnsupportedColumn(column) => LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "sql2 lix_directory_history provider does not support projected column '{column}'"
            ),
        ),
        ColumnTableError::Arrow(error) | ColumnTableError::ArrowZeroColumn(error) => LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("sql2 failed to build lix_directory_history record batch: {error}"),
        ),
        ColumnTableError::Row(error) => error,
    }
}

pub(super) fn lix_directory_history_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("path", DataType::Utf8, true),
        Field::new("parent_id", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        json_field(HISTORY_COL_ENTITY_PK, false),
        Field::new(HISTORY_COL_SCHEMA_KEY, DataType::Utf8, false),
        Field::new(HISTORY_COL_FILE_ID, DataType::Utf8, true),
        json_field(HISTORY_COL_SNAPSHOT_CONTENT, true),
        Field::new(HISTORY_COL_CHANGE_ID, DataType::Utf8, false),
        Field::new(HISTORY_COL_ORIGIN_KEY, DataType::Utf8, true),
        json_field(HISTORY_COL_METADATA, true),
        Field::new(HISTORY_COL_OBSERVED_COMMIT_ID, DataType::Utf8, false),
        Field::new(HISTORY_COL_COMMIT_CREATED_AT, DataType::Utf8, false),
        Field::new(HISTORY_COL_START_COMMIT_ID, DataType::Utf8, false),
        Field::new(HISTORY_COL_DEPTH, DataType::Int64, false),
    ]))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{HistoryRoute, load_directory_history_entry_sets};

    #[tokio::test]
    async fn identical_event_and_context_routes_load_history_once() {
        let route = HistoryRoute {
            start_commit_ids: vec!["cid-start".to_string()],
            ..HistoryRoute::default()
        };
        let event_route = route.traversal_only();
        let context_route = route.starts_only();
        assert_eq!(event_route, context_route);

        let loads = Arc::new(AtomicUsize::new(0));
        let counted_loads = Arc::clone(&loads);
        let (event_entries, context_entries) =
            load_directory_history_entry_sets(&event_route, &context_route, move |_| {
                counted_loads.fetch_add(1, Ordering::SeqCst);
                async { Ok(Vec::new()) }
            })
            .await
            .expect("identical routes should load");

        assert!(event_entries.is_empty());
        assert!(context_entries.is_empty());
        assert_eq!(loads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn differing_depth_routes_load_history_twice() {
        let route = HistoryRoute {
            start_commit_ids: vec!["cid-start".to_string()],
            max_depth: Some(3),
            ..HistoryRoute::default()
        };
        let event_route = route.traversal_only();
        let context_route = route.starts_only();
        assert_ne!(event_route, context_route);

        let loads = Arc::new(AtomicUsize::new(0));
        let counted_loads = Arc::clone(&loads);
        let (event_entries, context_entries) =
            load_directory_history_entry_sets(&event_route, &context_route, move |_| {
                counted_loads.fetch_add(1, Ordering::SeqCst);
                async { Ok(Vec::new()) }
            })
            .await
            .expect("distinct routes should load");

        assert!(event_entries.is_empty());
        assert!(context_entries.is_empty());
        assert_eq!(loads.load(Ordering::SeqCst), 2);
    }
}
