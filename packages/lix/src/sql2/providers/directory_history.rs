use std::collections::{BTreeMap, BTreeSet};
#[cfg(test)]
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
use crate::changelog::CommitId;
use crate::commit_graph::CommitGraphReader;
use crate::common::SharedStr;
use crate::forktree::ForkTreeReadFacade;

use crate::sql2::SqlChangelogQuerySource;
use crate::sql2::WriteAccess;
use crate::sql2::change_materialization::MaterializedChange;
use crate::sql2::error::lix_error_to_datafusion_error;
use crate::sql2::history_projection::{HistoryIdentityProjection, tombstone_identity_column_value};
use crate::sql2::history_route::{
    HISTORY_COL_AS_OF_COMMIT_ID, HISTORY_COL_COMMIT_CREATED_AT, HISTORY_COL_DEPTH,
    HISTORY_COL_ENTITY_PK, HISTORY_COL_IS_DELETED, HISTORY_COL_OBSERVED_COMMIT_ID,
    HISTORY_COL_SOURCE_CHANGES, HistoryEntry, HistoryMetadataProjection, HistoryRoute,
    HistoryViewDescriptor, load_history_entries, parse_history_filter,
    serialize_history_source_changes, validate_history_anchor_filter,
};
use crate::sql2::providers::filesystem_history_path::{
    DirectoryPathRecord, HistoryDirectoryTree, load_history_commit_parents,
    resolve_observed_directory_path,
};
use crate::sql2::result_metadata::json_field;
use crate::storage_adapter::StorageAdapterRead;

use super::columns::{Col, ColumnTable, ColumnTableError};
use super::history_util::{ObservedStateOrdinal, ObservedStateRows, entity_pk_json_array};
use super::spec::{PlannedScan, TableSpec, projected_schema, register_spec_table, scan_row_source};

const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";

pub(super) async fn register_lix_directory_history_surface<S>(
    session: &datafusion::prelude::SessionContext,
    surface_name: &str,
    commit_graph: super::SharedCommitGraph,
    query_source: SqlChangelogQuerySource<S>,
    default_as_of_commit_id: String,
) -> Result<(), LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    register_spec_table(
        session,
        surface_name,
        Arc::new(LixDirectoryHistorySpec {
            schema: lix_directory_history_schema(),
            commit_graph,
            query_source,
            default_as_of_commit_id,
        }),
        WriteAccess::read_only(),
    )
}

struct LixDirectoryHistorySpec<S> {
    schema: SchemaRef,
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlChangelogQuerySource<S>,
    default_as_of_commit_id: String,
}

#[async_trait]
impl<S> TableSpec<S> for LixDirectoryHistorySpec<S>
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
        let schema = projected_schema(&self.schema, projection);
        let mut route = HistoryRoute::from_filters(filters);
        route.default_to_as_of_commit_id(&self.default_as_of_commit_id);
        let metadata_projection = HistoryMetadataProjection::from_scan(&schema, filters);
        Ok(PlannedScan {
            schema: Arc::clone(&schema),
            ordering: None,
            source: scan_row_source(
                Arc::clone(&schema),
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

impl DirectoryPathRecord for DirectoryHistoryRecord {
    fn id(&self) -> &str {
        &self.id
    }

    fn parent_id(&self) -> Option<&str> {
        self.parent_id.as_deref()
    }

    fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
}

#[derive(Debug)]
struct DirectoryHistoryObservedState {
    rows: ObservedStateRows,
    descriptors: Vec<DirectoryHistoryObservedRecord>,
}

#[derive(Debug)]
struct DirectoryHistoryObservedRecord {
    id: String,
    parent_id: Option<String>,
    name: Option<String>,
    row: ObservedStateOrdinal,
}

impl DirectoryPathRecord for DirectoryHistoryObservedRecord {
    fn id(&self) -> &str {
        &self.id
    }

    fn parent_id(&self) -> Option<&str> {
        self.parent_id.as_deref()
    }

    fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
}

#[derive(Debug)]
struct DirectoryHistoryOutputRow {
    observed_state: Arc<DirectoryHistoryObservedState>,
    descriptor_ordinal: u32,
    id: String,
    path: Option<String>,
    event: DirectoryHistoryEvent,
}

impl DirectoryHistoryOutputRow {
    fn descriptor(&self) -> &DirectoryHistoryObservedRecord {
        let descriptor = &self.observed_state.descriptors[self.descriptor_ordinal as usize];
        let _ = self.observed_state.rows.row(descriptor.row);
        descriptor
    }
}

#[derive(Debug, Clone)]
struct DirectoryHistoryEvent {
    directory_id: String,
    as_of_commit_id: String,
    depth: u32,
    source_changes: Vec<MaterializedChange>,
    observed_commit_id: String,
    commit_created_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DirectoryDescriptorSnapshot {
    id: String,
    parent_id: Option<String>,
    name: String,
}

async fn load_directory_history_rows<S>(
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlChangelogQuerySource<S>,
    route: &HistoryRoute,
    metadata_projection: HistoryMetadataProjection,
) -> Result<Vec<DirectoryHistoryOutputRow>, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    let event_route = route.traversal_only();
    let event_entries = load_history_entries(
        HistoryViewDescriptor {
            view_name: "lix_directory_history",
            as_of_commit_column: HISTORY_COL_AS_OF_COMMIT_ID,
        },
        Arc::clone(&commit_graph),
        query_source.clone(),
        &event_route,
        vec![DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string()],
        metadata_projection,
    )
    .await?;
    let event_descriptors = parse_directory_history_records(&event_entries)?;
    let parent_commit_ids_by_commit =
        load_history_commit_parents(&commit_graph, &event_route.as_of_commit_ids).await?;
    let mut observed_commit_ids = event_descriptors
        .iter()
        .map(|record| record.entry.observed_commit_id.clone())
        .collect::<BTreeSet<_>>();
    let direct_parent_commit_ids = observed_commit_ids
        .iter()
        .flat_map(|observed_commit_id| {
            parent_commit_ids_by_commit
                .get(observed_commit_id)
                .into_iter()
                .flatten()
                .cloned()
        })
        .collect::<Vec<_>>();
    observed_commit_ids.extend(direct_parent_commit_ids);
    let historical = query_source.forktree_reader.clone();
    let observed_states =
        load_directory_history_observed_states(&historical, observed_commit_ids).await?;
    let events = grouped_directory_history_events(
        &event_descriptors,
        &observed_states,
        &parent_commit_ids_by_commit,
    )?;
    let mut output = Vec::new();

    for event in events {
        let Some(observed_state) = observed_states.get(&event.observed_commit_id) else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "lix_directory_history did not load observed commit '{}'",
                    event.observed_commit_id
                ),
            ));
        };
        let Some((descriptor_ordinal, visible_descriptor)) = observed_state
            .descriptors
            .iter()
            .enumerate()
            .find(|(_, descriptor)| descriptor.id == event.directory_id)
        else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "lix_directory_history event for '{}' at commit '{}' has no authenticated descriptor",
                    event.directory_id, event.observed_commit_id
                ),
            ));
        };
        let path = if visible_descriptor.name.is_some() {
            resolve_observed_directory_path(
                &visible_descriptor.id,
                &observed_state.descriptors,
                &mut BTreeMap::new(),
                &mut BTreeSet::new(),
            )?
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
            observed_state: Arc::clone(observed_state),
            descriptor_ordinal: u32::try_from(descriptor_ordinal).map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "lix_directory_history observed descriptor ordinal exceeds u32",
                )
            })?,
            id,
            path,
            event,
        });
    }
    output.retain(|row| {
        let entity_pk = entity_pk_json_array(&row.descriptor().id).ok();
        route.matches_surface_row(
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
            entity_pk.as_deref().unwrap_or(&row.descriptor().id),
            None,
            row.event.depth,
        )
    });

    output.sort_by(|left, right| {
        left.descriptor()
            .id
            .cmp(&right.descriptor().id)
            .then(left.event.as_of_commit_id.cmp(&right.event.as_of_commit_id))
            .then(left.event.depth.cmp(&right.event.depth))
            .then(
                left.event
                    .observed_commit_id
                    .cmp(&right.event.observed_commit_id),
            )
            .then(
                left.event
                    .source_changes
                    .first()
                    .map(|change| &change.id)
                    .cmp(&right.event.source_changes.first().map(|change| &change.id)),
            )
    });
    Ok(output)
}

#[cfg(test)]
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

async fn load_directory_history_observed_states<S>(
    historical: &ForkTreeReadFacade<S>,
    observed_commit_ids: BTreeSet<String>,
) -> Result<BTreeMap<String, Arc<DirectoryHistoryObservedState>>, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    let mut states = BTreeMap::new();
    for observed_commit_id in observed_commit_ids {
        let commit_id =
            CommitId::parse_lix(&observed_commit_id, "directory history observed commit")?;
        let batch = historical.scan_state_rows_at_commit(commit_id).await?;
        let batch = batch
            .into_iter()
            .filter(|row| row.key.schema_key == DIRECTORY_DESCRIPTOR_SCHEMA_KEY)
            .collect();
        let rows =
            ObservedStateRows::from_rows(SharedStr::from(observed_commit_id.as_str()), batch)?;
        let descriptors = parse_directory_history_observed_records(&rows)?;
        states.insert(
            observed_commit_id,
            Arc::new(DirectoryHistoryObservedState { rows, descriptors }),
        );
    }
    Ok(states)
}

fn parse_directory_history_observed_records(
    rows: &ObservedStateRows,
) -> Result<Vec<DirectoryHistoryObservedRecord>, LixError> {
    rows.iter()
        .filter(|observed| observed.row().schema_key() == DIRECTORY_DESCRIPTOR_SCHEMA_KEY)
        .map(|observed| {
            let _ = observed.observed_commit_id();
            let row = observed.row();
            let row_id = row.entity_pk().as_single_string_owned()?;
            let Some(snapshot_content) = row.snapshot_content() else {
                if row.deleted() {
                    return Ok(DirectoryHistoryObservedRecord {
                        id: row_id,
                        parent_id: None,
                        name: None,
                        row: observed.ordinal(),
                    });
                }
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("directory descriptor row '{row_id}' has no authenticated payload"),
                ));
            };
            let snapshot_content = Some(snapshot_content).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("directory descriptor row '{row_id}' has no authenticated payload"),
                )
            })?;
            let snapshot: DirectoryDescriptorSnapshot = serde_json::from_str(snapshot_content)
                .map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid lix_directory_descriptor history snapshot JSON: {error}"),
                    )
                })?;
            if snapshot.id != row_id {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "observed directory descriptor payload identity '{}' does not match authenticated row key '{}'",
                        snapshot.id, row_id
                    ),
                ));
            }
            Ok(DirectoryHistoryObservedRecord {
                id: row_id,
                parent_id: snapshot.parent_id,
                name: Some(snapshot.name),
                row: observed.ordinal(),
            })
        })
        .collect()
}

fn parse_directory_history_records(
    entries: &[HistoryEntry],
) -> Result<Vec<DirectoryHistoryRecord>, LixError> {
    entries
        .iter()
        .filter(|entry| entry.change.schema_key == DIRECTORY_DESCRIPTOR_SCHEMA_KEY)
        .map(|entry| {
            let row_id = entry.change.entity_pk.as_single_string_owned()?;
            if entry.change.snapshot_content.is_none() {
                return Ok(DirectoryHistoryRecord {
                    id: row_id,
                    parent_id: None,
                    name: None,
                    entry: entry.clone(),
                });
            }
            let snapshot_content = entry.change.snapshot_content.as_deref().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("directory descriptor history row '{row_id}' has no authenticated payload"),
                )
            })?;
            let snapshot: DirectoryDescriptorSnapshot = serde_json::from_str(snapshot_content)
                .map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid lix_directory_descriptor history snapshot JSON: {error}"),
                    )
                })?;
            if snapshot.id != row_id {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "directory descriptor payload identity '{}' does not match authenticated row key '{}'",
                        snapshot.id, row_id
                    ),
                ));
            }
            Ok(DirectoryHistoryRecord {
                id: row_id,
                parent_id: snapshot.parent_id,
                name: Some(snapshot.name),
                entry: entry.clone(),
            })
        })
        .collect()
}

fn grouped_directory_history_events(
    descriptors: &[DirectoryHistoryRecord],
    observed_states: &BTreeMap<String, Arc<DirectoryHistoryObservedState>>,
    parent_commit_ids_by_commit: &BTreeMap<String, Vec<String>>,
) -> Result<Vec<DirectoryHistoryEvent>, LixError> {
    let mut grouped = BTreeMap::<(String, String, String), DirectoryHistoryEvent>::new();
    let directory_trees = observed_states
        .iter()
        .map(|(commit_id, state)| {
            (
                commit_id.as_str(),
                HistoryDirectoryTree::from_records(&state.descriptors),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for descriptor in descriptors {
        let state_commit_ids = std::iter::once(descriptor.entry.observed_commit_id.as_str()).chain(
            parent_commit_ids_by_commit
                .get(&descriptor.entry.observed_commit_id)
                .into_iter()
                .flatten()
                .map(String::as_str),
        );
        let mut affected_directory_ids = BTreeSet::from([descriptor.id.clone()]);
        for state_commit_id in state_commit_ids {
            if observed_states.contains_key(state_commit_id) {
                affected_directory_ids.extend(
                    directory_trees
                        .get(state_commit_id)
                        .expect("every observed directory state should have a directory tree")
                        .descendants_including(&descriptor.id)?,
                );
            }
        }
        for affected_directory_id in affected_directory_ids {
            let mut event =
                directory_history_event_from_entry(&affected_directory_id, &descriptor.entry);
            let key = (
                event.directory_id.clone(),
                event.as_of_commit_id.clone(),
                event.observed_commit_id.clone(),
            );
            match grouped.entry(key) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(event);
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    let grouped_event = entry.get_mut();
                    debug_assert_eq!(grouped_event.depth, event.depth);
                    grouped_event
                        .source_changes
                        .append(&mut event.source_changes);
                }
            }
        }
    }
    let mut events = grouped.into_values().collect::<Vec<_>>();
    for event in &mut events {
        event
            .source_changes
            .sort_by(|left, right| left.id.cmp(&right.id));
        event
            .source_changes
            .dedup_by(|left, right| left.id == right.id);
    }
    events.sort_by(|left, right| {
        left.directory_id
            .cmp(&right.directory_id)
            .then(left.as_of_commit_id.cmp(&right.as_of_commit_id))
            .then(left.depth.cmp(&right.depth))
            .then(left.observed_commit_id.cmp(&right.observed_commit_id))
    });
    Ok(events)
}

fn directory_history_event_from_entry(
    directory_id: &str,
    entry: &HistoryEntry,
) -> DirectoryHistoryEvent {
    DirectoryHistoryEvent {
        directory_id: directory_id.to_string(),
        as_of_commit_id: entry.as_of_commit_id.clone(),
        depth: entry.depth,
        source_changes: vec![entry.change.clone()],
        observed_commit_id: entry.observed_commit_id.clone(),
        commit_created_at: entry.commit_created_at.clone(),
    }
}

static LIX_DIRECTORY_HISTORY_COLS: ColumnTable<DirectoryHistoryOutputRow> = ColumnTable {
    columns: &[
        ("id", Col::Utf8(|row| Some(row.id.as_str()))),
        ("path", Col::Utf8(|row| row.path.as_deref())),
        (
            "parent_id",
            Col::Utf8(|row| row.descriptor().parent_id.as_deref()),
        ),
        ("name", Col::Utf8(|row| row.descriptor().name.as_deref())),
        (
            HISTORY_COL_ENTITY_PK,
            Col::Utf8Fallible(|row| entity_pk_json_array(&row.descriptor().id).map(Some)),
        ),
        (
            HISTORY_COL_SOURCE_CHANGES,
            Col::Utf8Fallible(|row| {
                serialize_history_source_changes(&row.event.source_changes, "lix_directory_history")
                    .map(Some)
            }),
        ),
        (
            HISTORY_COL_OBSERVED_COMMIT_ID,
            Col::Utf8(|row| Some(row.event.observed_commit_id.as_str())),
        ),
        (
            HISTORY_COL_COMMIT_CREATED_AT,
            Col::Utf8(|row| row.event.commit_created_at.as_deref()),
        ),
        (
            HISTORY_COL_AS_OF_COMMIT_ID,
            Col::Utf8(|row| Some(row.event.as_of_commit_id.as_str())),
        ),
        (
            HISTORY_COL_DEPTH,
            Col::I64(|row| Some(i64::from(row.event.depth))),
        ),
        (
            HISTORY_COL_IS_DELETED,
            Col::Bool(|row| Some(row.descriptor().name.is_none())),
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
        json_field(HISTORY_COL_SOURCE_CHANGES, false),
        Field::new(HISTORY_COL_OBSERVED_COMMIT_ID, DataType::Utf8, false),
        Field::new(HISTORY_COL_COMMIT_CREATED_AT, DataType::Utf8, false),
        Field::new(HISTORY_COL_AS_OF_COMMIT_ID, DataType::Utf8, false),
        Field::new(HISTORY_COL_DEPTH, DataType::Int64, false),
        Field::new(HISTORY_COL_IS_DELETED, DataType::Boolean, false),
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
            as_of_commit_ids: vec!["cid-start".to_string()],
            ..HistoryRoute::default()
        };
        let event_route = route.traversal_only();
        let context_route = route.anchors_only();
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
            as_of_commit_ids: vec!["cid-start".to_string()],
            max_depth: Some(3),
            ..HistoryRoute::default()
        };
        let event_route = route.traversal_only();
        let context_route = route.anchors_only();
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
