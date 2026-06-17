use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::GLOBAL_BRANCH_ID;
use crate::LixError;
use crate::binary_cas::{BlobDataReader, BlobHash};
use crate::commit_graph::CommitGraphReader;
use crate::common::compose_file_path;
use crate::filesystem::FilesystemIndex;
use crate::live_state::MaterializedLiveStateRow;
use crate::plugin::{
    InstalledPlugin, InstalledPluginMetadata, PluginContentType, PluginRuntimeHost,
    load_installed_plugin_from_archive_bytes, load_installed_plugin_metadata_from_archive_bytes,
    plugin_key_from_archive_path, render_materialized_plugin_file, retain_plugin_state_rows,
    select_best_glob_match,
};
use crate::serialize_row_metadata;

use crate::sql2::SqlHistoryQuerySource;
use crate::sql2::WriteAccess;
use crate::sql2::change_materialization::MaterializedChange;
use crate::sql2::history_projection::{HistoryIdentityProjection, tombstone_identity_column_value};
use crate::sql2::history_route::{
    HISTORY_COL_CHANGE_ID, HISTORY_COL_COMMIT_CREATED_AT, HISTORY_COL_DEPTH, HISTORY_COL_ENTITY_PK,
    HISTORY_COL_FILE_ID, HISTORY_COL_METADATA, HISTORY_COL_OBSERVED_COMMIT_ID,
    HISTORY_COL_SCHEMA_KEY, HISTORY_COL_SNAPSHOT_CONTENT, HISTORY_COL_START_COMMIT_ID,
    HistoryColumnStyle, HistoryEntry, HistoryRoute, HistoryViewDescriptor,
    history_descriptor_event_matches, load_history_entries, parse_history_filter,
};
use crate::sql2::providers::filesystem_history_path::{
    HistoryDirectoryPathRecord, resolve_history_directory_path,
};
use crate::sql2::result_metadata::json_field;
use crate::storage::StorageRead;

use super::columns::{Col, ColumnTable, ColumnTableError};
use super::file::load_single_blob_bytes;
use super::history::entity_pk_json_array;
use super::spec::{PlannedScan, TableSpec, projected_schema, register_spec_table, row_source};

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";
const BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";

pub(super) async fn register_lix_file_history_surface<S>(
    session: &datafusion::prelude::SessionContext,
    surface_name: &str,
    commit_graph: Box<dyn CommitGraphReader>,
    query_source: SqlHistoryQuerySource<S>,
    blob_reader: Arc<dyn BlobDataReader>,
    plugin_host: PluginRuntimeHost,
) -> Result<(), LixError>
where
    S: StorageRead + Clone + Send + Sync + 'static,
{
    register_spec_table(
        session,
        surface_name,
        Arc::new(LixFileHistorySpec {
            commit_graph: Arc::new(Mutex::new(commit_graph)),
            query_source,
            blob_reader,
            plugin_host,
        }),
        WriteAccess::read_only(),
    )
}

/// SQL spec for `lix_file_history`.
///
/// The reachability-aware file history surface: rows are reconstructed by
/// walking the commit graph from the routed start commits, resolving the
/// nearest descriptor/blob/directory events per file.
struct LixFileHistorySpec<S> {
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlHistoryQuerySource<S>,
    blob_reader: Arc<dyn BlobDataReader>,
    plugin_host: PluginRuntimeHost,
}

#[async_trait]
impl<S> TableSpec for LixFileHistorySpec<S>
where
    S: StorageRead + Clone + Send + Sync + 'static,
{
    #[expect(clippy::unnecessary_literal_bound)]
    fn table_name(&self) -> &str {
        "lix_file_history"
    }

    fn schema(&self) -> SchemaRef {
        lix_file_history_schema()
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
        let full_schema = lix_file_history_schema();
        let schema = projected_schema(&full_schema, projection);
        let needs_data = projection.is_none_or(|projection| {
            projection.iter().any(|index| {
                full_schema
                    .field(*index)
                    .name()
                    .as_str()
                    .eq_ignore_ascii_case("data")
            })
        });
        let route = HistoryRoute::from_filters(filters, HistoryColumnStyle::Prefixed);
        Ok(PlannedScan {
            schema: Arc::clone(&schema),
            load: row_source(
                (
                    Arc::clone(&self.commit_graph),
                    self.query_source.clone(),
                    Arc::clone(&self.blob_reader),
                    self.plugin_host.clone(),
                    route,
                    schema,
                ),
                move |(commit_graph, query_source, blob_reader, plugin_host, route, schema)| async move {
                    let mut rows = load_file_history_rows(
                        commit_graph,
                        query_source,
                        &blob_reader,
                        plugin_host,
                        &route,
                        needs_data,
                    )
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
                    if let Some(limit) = limit {
                        rows.truncate(limit);
                    }
                    LIX_FILE_HISTORY_COLS
                        .build(schema, &rows)
                        .map_err(file_history_batch_error)
                        .map_err(lix_error_to_datafusion_error)
                },
            ),
        })
    }
}

#[derive(Debug, Clone)]
struct FileHistoryDescriptorRecord {
    id: String,
    directory_id: Option<String>,
    name: Option<String>,
    entry: HistoryEntry,
}

#[derive(Debug, Clone)]
struct FileHistoryDirectoryRecord {
    id: String,
    parent_id: Option<String>,
    name: String,
    entry: HistoryEntry,
}

impl HistoryDirectoryPathRecord for FileHistoryDirectoryRecord {
    fn id(&self) -> &str {
        &self.id
    }

    fn parent_id(&self) -> Option<&str> {
        self.parent_id.as_deref()
    }

    fn name(&self) -> Option<&str> {
        Some(&self.name)
    }

    fn entry(&self) -> &HistoryEntry {
        &self.entry
    }
}

#[derive(Debug, Clone)]
struct FileHistoryBlobRecord {
    file_id: String,
    blob_hash: Option<String>,
    entry: HistoryEntry,
}

#[derive(Debug, Clone)]
struct FileHistoryPluginStateRecord {
    file_id: String,
    entry: HistoryEntry,
}

#[derive(Debug, Clone)]
struct FileHistoryEvent {
    file_id: String,
    start_commit_id: String,
    depth: u32,
    priority: u8,
    change: MaterializedChange,
    observed_commit_id: String,
    commit_created_at: String,
}

#[derive(Debug, Clone)]
struct FileHistoryOutputRow {
    entity_pk: String,
    id: String,
    path: Option<String>,
    directory_id: Option<String>,
    name: Option<String>,
    data: Option<Vec<u8>>,
    descriptor_change: MaterializedChange,
    event: FileHistoryEvent,
}

#[derive(Debug, Deserialize)]
struct FileDescriptorSnapshot {
    id: String,
    directory_id: Option<String>,
    name: String,
}

#[derive(Debug, Deserialize)]
struct DirectoryDescriptorSnapshot {
    id: String,
    parent_id: Option<String>,
    name: String,
}

#[derive(Debug, Deserialize)]
struct BlobRefSnapshot {
    id: String,
    blob_hash: String,
}

struct FileHistoryFilesystemContext {
    event_descriptors: Vec<FileHistoryDescriptorRecord>,
    event_directories: Vec<FileHistoryDirectoryRecord>,
    event_blobs: Vec<FileHistoryBlobRecord>,
    descriptors: Vec<FileHistoryDescriptorRecord>,
    directories: Vec<FileHistoryDirectoryRecord>,
    blobs: Vec<FileHistoryBlobRecord>,
}

async fn load_file_history_rows<S>(
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlHistoryQuerySource<S>,
    blob_reader: &Arc<dyn BlobDataReader>,
    plugin_host: PluginRuntimeHost,
    route: &HistoryRoute,
    needs_data: bool,
) -> Result<Vec<FileHistoryOutputRow>, LixError>
where
    S: StorageRead + Clone + Send + Sync + 'static,
{
    if !route.schema_keys.is_empty()
        && !route
            .schema_keys
            .iter()
            .any(|schema_key| schema_key == FILE_DESCRIPTOR_SCHEMA_KEY)
    {
        return Ok(Vec::new());
    }

    let event_route = route.traversal_only();
    let context_route = route.starts_only();
    let filesystem_context = load_file_history_filesystem_context(
        Arc::clone(&commit_graph),
        query_source.clone(),
        &event_route,
        &context_route,
    )
    .await?;
    let mut installed_plugins_cache = BTreeMap::<(String, u32, String), InstalledPlugin>::new();
    let mut installed_plugin_metadata_cache =
        BTreeMap::<(String, u32), Vec<InstalledPluginMetadata>>::new();
    let events = file_history_events(
        &filesystem_context.event_descriptors,
        &filesystem_context.event_directories,
        &filesystem_context.event_blobs,
        &filesystem_context.descriptors,
    );
    let plugin_schema_keys = discover_file_history_plugin_schema_keys(
        blob_reader,
        &mut installed_plugin_metadata_cache,
        &filesystem_context,
        &events,
    )
    .await?;
    let (event_plugin_state, plugin_state) = if plugin_schema_keys.is_empty() {
        (Vec::new(), Vec::new())
    } else {
        load_file_history_plugin_state(
            commit_graph,
            query_source,
            &event_route,
            &context_route,
            plugin_schema_keys,
        )
        .await?
    };
    cache_installed_plugins_for_plugin_state_depths(
        blob_reader,
        &mut installed_plugin_metadata_cache,
        &filesystem_context,
        &event_plugin_state,
    )
    .await?;
    let plugin_events = file_history_plugin_events(
        &installed_plugin_metadata_cache,
        &event_plugin_state,
        &filesystem_context.descriptors,
        &filesystem_context.directories,
    );
    let events = sorted_deduped_file_history_events(events.into_iter().chain(plugin_events));

    let mut output = Vec::new();
    for event in events {
        let Some(descriptor) = nearest_file_descriptor(&filesystem_context.descriptors, &event)
        else {
            continue;
        };
        let blob = nearest_blob_ref(&filesystem_context.blobs, &event);
        let path =
            resolve_file_history_path(descriptor, &filesystem_context.directories, event.depth);
        let data = if needs_data && descriptor.name.is_some() {
            match blob.and_then(|blob| blob.blob_hash.as_deref()) {
                Some(blob_hash) => load_single_blob_bytes(blob_reader, blob_hash).await?,
                None => match path.as_deref() {
                    Some(path) => {
                        let rendered = render_plugin_file_history_data(
                            &plugin_host,
                            blob_reader,
                            &mut installed_plugins_cache,
                            &installed_plugin_metadata_cache,
                            &plugin_state,
                            descriptor,
                            &event,
                            path,
                        )
                        .await?;
                        Some(rendered.unwrap_or_default())
                    }
                    None => Some(Vec::new()),
                },
            }
        } else {
            None
        };
        let id = tombstone_identity_column_value(
            "id",
            &descriptor.id,
            HistoryIdentityProjection::SingleColumn { column: "id" },
        )?
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| descriptor.id.clone());

        output.push(FileHistoryOutputRow {
            entity_pk: descriptor.id.clone(),
            id,
            path,
            directory_id: descriptor.directory_id.clone(),
            name: descriptor.name.clone(),
            data,
            descriptor_change: descriptor.entry.change.clone(),
            event,
        });
    }
    output.retain(|row| {
        let entity_pk = entity_pk_json_array(&row.entity_pk).ok();
        route.matches_surface_row(
            FILE_DESCRIPTOR_SCHEMA_KEY,
            entity_pk.as_deref().unwrap_or(&row.entity_pk),
            Some(&row.entity_pk),
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

async fn load_file_history_filesystem_context<S>(
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlHistoryQuerySource<S>,
    event_route: &HistoryRoute,
    context_route: &HistoryRoute,
) -> Result<FileHistoryFilesystemContext, LixError>
where
    S: StorageRead + Clone + Send + Sync + 'static,
{
    let filesystem_schema_keys = file_history_filesystem_schema_keys();
    let event_entries = load_history_entries(
        HistoryViewDescriptor {
            view_name: "lix_file_history",
            start_commit_column: HISTORY_COL_START_COMMIT_ID,
        },
        Arc::clone(&commit_graph),
        query_source.json_reader.clone(),
        event_route,
        filesystem_schema_keys.clone(),
    )
    .await?;
    let context_entries = load_history_entries(
        HistoryViewDescriptor {
            view_name: "lix_file_history",
            start_commit_column: HISTORY_COL_START_COMMIT_ID,
        },
        commit_graph,
        query_source.json_reader,
        context_route,
        filesystem_schema_keys,
    )
    .await?;

    Ok(FileHistoryFilesystemContext {
        event_descriptors: parse_file_history_descriptors(&event_entries)?,
        event_directories: parse_file_history_directories(&event_entries)?,
        event_blobs: parse_file_history_blobs(&event_entries)?,
        descriptors: parse_file_history_descriptors(&context_entries)?,
        directories: parse_file_history_directories(&context_entries)?,
        blobs: parse_file_history_blobs(&context_entries)?,
    })
}

async fn load_file_history_plugin_state<S>(
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlHistoryQuerySource<S>,
    event_route: &HistoryRoute,
    context_route: &HistoryRoute,
    plugin_schema_keys: Vec<String>,
) -> Result<
    (
        Vec<FileHistoryPluginStateRecord>,
        Vec<FileHistoryPluginStateRecord>,
    ),
    LixError,
>
where
    S: StorageRead + Clone + Send + Sync + 'static,
{
    let event_entries = load_history_entries(
        HistoryViewDescriptor {
            view_name: "lix_file_history",
            start_commit_column: HISTORY_COL_START_COMMIT_ID,
        },
        Arc::clone(&commit_graph),
        query_source.json_reader.clone(),
        event_route,
        plugin_schema_keys.clone(),
    )
    .await?;
    let context_entries = load_history_entries(
        HistoryViewDescriptor {
            view_name: "lix_file_history",
            start_commit_column: HISTORY_COL_START_COMMIT_ID,
        },
        commit_graph,
        query_source.json_reader,
        context_route,
        plugin_schema_keys,
    )
    .await?;
    Ok((
        parse_file_history_plugin_state(&event_entries),
        parse_file_history_plugin_state(&context_entries),
    ))
}

fn file_history_filesystem_schema_keys() -> Vec<String> {
    vec![
        FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
        DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
        BLOB_REF_SCHEMA_KEY.to_string(),
    ]
}

fn file_history_events(
    event_descriptors: &[FileHistoryDescriptorRecord],
    event_directories: &[FileHistoryDirectoryRecord],
    event_blobs: &[FileHistoryBlobRecord],
    context_descriptors: &[FileHistoryDescriptorRecord],
) -> Vec<FileHistoryEvent> {
    let mut descriptor_ids_by_start = BTreeSet::<(String, String)>::new();
    let mut directory_ids_by_file_start = BTreeMap::<(String, String), BTreeSet<String>>::new();

    for descriptor in context_descriptors {
        let key = (
            descriptor.id.clone(),
            descriptor.entry.start_commit_id.clone(),
        );
        descriptor_ids_by_start.insert(key.clone());
        if let Some(directory_id) = &descriptor.directory_id {
            directory_ids_by_file_start
                .entry(key)
                .or_default()
                .insert(directory_id.clone());
        }
    }

    let mut candidates = Vec::new();
    for descriptor in event_descriptors {
        candidates.push(file_history_event_from_entry(
            descriptor.id.clone(),
            &descriptor.entry,
            1,
        ));
    }
    for directory in event_directories {
        for ((file_id, start_commit_id), directory_ids) in &directory_ids_by_file_start {
            if start_commit_id == &directory.entry.start_commit_id
                && directory_ids.contains(&directory.id)
            {
                candidates.push(file_history_event_from_entry(
                    file_id.clone(),
                    &directory.entry,
                    2,
                ));
            }
        }
    }
    for blob in event_blobs {
        if descriptor_ids_by_start
            .contains(&(blob.file_id.clone(), blob.entry.start_commit_id.clone()))
        {
            candidates.push(file_history_event_from_entry(
                blob.file_id.clone(),
                &blob.entry,
                3,
            ));
        }
    }
    sorted_deduped_file_history_events(candidates)
}

fn sorted_deduped_file_history_events<I>(events: I) -> Vec<FileHistoryEvent>
where
    I: IntoIterator<Item = FileHistoryEvent>,
{
    let mut candidates = events.into_iter().collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        left.file_id
            .cmp(&right.file_id)
            .then(left.start_commit_id.cmp(&right.start_commit_id))
            .then(left.depth.cmp(&right.depth))
            .then(left.priority.cmp(&right.priority))
            .then(left.change.id.cmp(&right.change.id))
    });
    candidates.dedup_by(|left, right| {
        left.file_id == right.file_id
            && left.start_commit_id == right.start_commit_id
            && left.depth == right.depth
    });
    candidates
}

async fn discover_file_history_plugin_schema_keys(
    blob_reader: &Arc<dyn BlobDataReader>,
    installed_plugin_metadata_cache: &mut BTreeMap<(String, u32), Vec<InstalledPluginMetadata>>,
    filesystem_context: &FileHistoryFilesystemContext,
    events: &[FileHistoryEvent],
) -> Result<Vec<String>, LixError> {
    let mut depths = BTreeSet::<(String, u32)>::new();
    for event in events {
        depths.insert((event.start_commit_id.clone(), event.depth));
    }
    collect_record_depths(&filesystem_context.descriptors, &mut depths);
    collect_record_depths(&filesystem_context.directories, &mut depths);
    collect_record_depths(&filesystem_context.blobs, &mut depths);

    let mut schema_keys = BTreeSet::new();
    for (start_commit_id, depth) in depths {
        let paths = file_paths_at_history_depth(
            &filesystem_context.descriptors,
            &filesystem_context.directories,
            &start_commit_id,
            depth,
        );
        let plugins = installed_plugins_at_history_depth(
            blob_reader,
            installed_plugin_metadata_cache,
            &filesystem_context.descriptors,
            &filesystem_context.directories,
            &filesystem_context.blobs,
            &start_commit_id,
            depth,
        )
        .await?;
        for plugin in plugins {
            if paths.iter().any(|path| {
                select_plugin_metadata_for_path(plugins, path)
                    .is_some_and(|candidate| candidate.key == plugin.key)
            }) {
                schema_keys.extend(plugin.schema_keys.iter().cloned());
            }
        }
    }

    Ok(schema_keys.into_iter().collect())
}

fn file_paths_at_history_depth(
    descriptors: &[FileHistoryDescriptorRecord],
    directories: &[FileHistoryDirectoryRecord],
    start_commit_id: &str,
    depth: u32,
) -> Vec<String> {
    let file_ids = descriptors
        .iter()
        .filter(|record| record.entry.start_commit_id == start_commit_id)
        .map(|record| record.id.clone())
        .collect::<BTreeSet<_>>();
    file_ids
        .into_iter()
        .filter_map(|file_id| {
            let descriptor = nearest_history_record(
                descriptors.iter().filter(|record| {
                    record.id == file_id && record.entry.start_commit_id == start_commit_id
                }),
                depth,
            )?;
            resolve_file_history_path(descriptor, directories, depth)
        })
        .collect()
}

fn collect_record_depths<T>(records: &[T], depths: &mut BTreeSet<(String, u32)>)
where
    T: FileHistoryRecord,
{
    depths.extend(
        records
            .iter()
            .map(|record| (record.entry().start_commit_id.clone(), record.entry().depth)),
    );
}

async fn cache_installed_plugins_for_plugin_state_depths(
    blob_reader: &Arc<dyn BlobDataReader>,
    installed_plugin_metadata_cache: &mut BTreeMap<(String, u32), Vec<InstalledPluginMetadata>>,
    filesystem_context: &FileHistoryFilesystemContext,
    event_plugin_state: &[FileHistoryPluginStateRecord],
) -> Result<(), LixError> {
    let mut depths = BTreeSet::<(String, u32)>::new();
    for record in event_plugin_state {
        depths.insert((record.entry.start_commit_id.clone(), record.entry.depth));
    }
    for (start_commit_id, depth) in depths {
        installed_plugins_at_history_depth(
            blob_reader,
            installed_plugin_metadata_cache,
            &filesystem_context.descriptors,
            &filesystem_context.directories,
            &filesystem_context.blobs,
            &start_commit_id,
            depth,
        )
        .await?;
    }
    Ok(())
}

fn file_history_plugin_events(
    installed_plugin_metadata_cache: &BTreeMap<(String, u32), Vec<InstalledPluginMetadata>>,
    event_plugin_state: &[FileHistoryPluginStateRecord],
    descriptors: &[FileHistoryDescriptorRecord],
    directories: &[FileHistoryDirectoryRecord],
) -> Vec<FileHistoryEvent> {
    let mut events = Vec::new();
    for plugin_state in event_plugin_state {
        let event =
            file_history_event_from_entry(plugin_state.file_id.clone(), &plugin_state.entry, 4);
        let Some(descriptor) = nearest_file_descriptor(descriptors, &event) else {
            continue;
        };
        let Some(path) = resolve_file_history_path(descriptor, directories, event.depth) else {
            continue;
        };
        let plugins = installed_plugin_metadata_cache
            .get(&(event.start_commit_id.clone(), event.depth))
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let Some(plugin) = select_plugin_metadata_for_path(plugins, &path) else {
            continue;
        };
        if plugin
            .schema_keys
            .iter()
            .any(|schema_key| schema_key == &plugin_state.entry.change.schema_key)
        {
            events.push(event);
        }
    }
    events
}

fn file_history_event_from_entry(
    file_id: String,
    entry: &HistoryEntry,
    priority: u8,
) -> FileHistoryEvent {
    FileHistoryEvent {
        file_id,
        start_commit_id: entry.start_commit_id.clone(),
        depth: entry.depth,
        priority,
        change: entry.change.clone(),
        observed_commit_id: entry.observed_commit_id.clone(),
        commit_created_at: entry.commit_created_at.clone(),
    }
}

fn parse_file_history_descriptors(
    entries: &[HistoryEntry],
) -> Result<Vec<FileHistoryDescriptorRecord>, LixError> {
    entries
        .iter()
        .filter(|entry| entry.change.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY)
        .map(|entry| {
            let Some(snapshot_content) = entry.change.snapshot_content.as_deref() else {
                return Ok(FileHistoryDescriptorRecord {
                    id: entry.change.entity_pk.as_single_string_owned()?,
                    directory_id: None,
                    name: None,
                    entry: entry.clone(),
                });
            };
            let snapshot: FileDescriptorSnapshot =
                serde_json::from_str(snapshot_content).map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid lix_file_descriptor history snapshot JSON: {error}"),
                    )
                })?;
            Ok(FileHistoryDescriptorRecord {
                id: snapshot.id,
                directory_id: snapshot.directory_id,
                name: Some(snapshot.name),
                entry: entry.clone(),
            })
        })
        .collect()
}

fn parse_file_history_directories(
    entries: &[HistoryEntry],
) -> Result<Vec<FileHistoryDirectoryRecord>, LixError> {
    entries
        .iter()
        .filter(|entry| entry.change.schema_key == DIRECTORY_DESCRIPTOR_SCHEMA_KEY)
        .filter_map(|entry| {
            let snapshot_content = entry.change.snapshot_content.clone()?;
            Some((entry, snapshot_content))
        })
        .map(|(entry, snapshot_content)| {
            let snapshot: DirectoryDescriptorSnapshot = serde_json::from_str(&snapshot_content)
                .map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid lix_directory_descriptor history snapshot JSON: {error}"),
                    )
                })?;
            Ok(FileHistoryDirectoryRecord {
                id: snapshot.id,
                parent_id: snapshot.parent_id,
                name: snapshot.name,
                entry: entry.clone(),
            })
        })
        .collect()
}

fn parse_file_history_blobs(
    entries: &[HistoryEntry],
) -> Result<Vec<FileHistoryBlobRecord>, LixError> {
    entries
        .iter()
        .filter(|entry| entry.change.schema_key == BLOB_REF_SCHEMA_KEY)
        .map(|entry| {
            let Some(snapshot_content) = entry.change.snapshot_content.as_deref() else {
                return Ok(FileHistoryBlobRecord {
                    file_id: entry.change.file_id.clone().unwrap_or_else(|| {
                        entry
                            .change
                            .entity_pk
                            .as_single_string_owned()
                            .expect("canonical change entity primary key should project")
                    }),
                    blob_hash: None,
                    entry: entry.clone(),
                });
            };
            let snapshot: BlobRefSnapshot =
                serde_json::from_str(snapshot_content).map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid lix_binary_blob_ref history snapshot JSON: {error}"),
                    )
                })?;
            Ok(FileHistoryBlobRecord {
                file_id: entry.change.file_id.clone().unwrap_or(snapshot.id),
                blob_hash: Some(snapshot.blob_hash),
                entry: entry.clone(),
            })
        })
        .collect()
}

fn parse_file_history_plugin_state(entries: &[HistoryEntry]) -> Vec<FileHistoryPluginStateRecord> {
    entries
        .iter()
        .filter(|entry| {
            !matches!(
                entry.change.schema_key.as_str(),
                FILE_DESCRIPTOR_SCHEMA_KEY | DIRECTORY_DESCRIPTOR_SCHEMA_KEY | BLOB_REF_SCHEMA_KEY
            )
        })
        .filter_map(|entry| {
            Some(FileHistoryPluginStateRecord {
                file_id: entry.change.file_id.clone()?,
                entry: entry.clone(),
            })
        })
        .collect()
}

fn nearest_file_descriptor<'a>(
    descriptors: &'a [FileHistoryDescriptorRecord],
    event: &FileHistoryEvent,
) -> Option<&'a FileHistoryDescriptorRecord> {
    descriptors
        .iter()
        .filter(|descriptor| {
            let exact_descriptor_event =
                history_descriptor_event_matches(&descriptor.entry, event.depth, &event.change.id);
            (exact_descriptor_event || descriptor.name.is_some())
                && descriptor.id == event.file_id
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

fn nearest_blob_ref<'a>(
    blobs: &'a [FileHistoryBlobRecord],
    event: &FileHistoryEvent,
) -> Option<&'a FileHistoryBlobRecord> {
    blobs
        .iter()
        .filter(|blob| {
            blob.file_id == event.file_id
                && blob.entry.start_commit_id == event.start_commit_id
                && blob.entry.depth >= event.depth
        })
        .min_by(|left, right| {
            left.entry
                .depth
                .cmp(&right.entry.depth)
                .then(left.entry.change.id.cmp(&right.entry.change.id))
        })
}

fn resolve_file_history_path(
    descriptor: &FileHistoryDescriptorRecord,
    directories: &[FileHistoryDirectoryRecord],
    target_depth: u32,
) -> Option<String> {
    let name = descriptor.name.as_ref()?;
    let Some(directory_id) = descriptor.directory_id.as_deref() else {
        return compose_file_path(None, name).ok();
    };
    let directory_path = resolve_history_directory_path(
        directory_id,
        &descriptor.entry.start_commit_id,
        target_depth,
        directories,
        &mut BTreeMap::new(),
        &mut BTreeSet::new(),
    )?;
    compose_file_path(Some(&directory_path), name).ok()
}

async fn render_plugin_file_history_data(
    plugin_host: &PluginRuntimeHost,
    blob_reader: &Arc<dyn BlobDataReader>,
    installed_plugins_cache: &mut BTreeMap<(String, u32, String), InstalledPlugin>,
    installed_plugin_metadata_cache: &BTreeMap<(String, u32), Vec<InstalledPluginMetadata>>,
    plugin_state: &[FileHistoryPluginStateRecord],
    descriptor: &FileHistoryDescriptorRecord,
    event: &FileHistoryEvent,
    path: &str,
) -> Result<Option<Vec<u8>>, LixError> {
    let plugins = installed_plugin_metadata_cache
        .get(&(event.start_commit_id.clone(), event.depth))
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let Some(plugin_metadata) = select_plugin_metadata_for_path(plugins, path) else {
        return Ok(None);
    };
    let plugin = installed_plugin_at_history_depth(
        blob_reader,
        installed_plugins_cache,
        plugin_metadata,
        &event.start_commit_id,
        event.depth,
    )
    .await?;
    let rows = plugin_state_live_rows_at_depth(plugin_state, plugin, descriptor, event);
    let active_state = retain_plugin_state_rows(plugin, rows);
    render_materialized_plugin_file(plugin_host, plugin, &active_state).await
}

fn select_plugin_metadata_for_path<'a>(
    plugins: &'a [InstalledPluginMetadata],
    path: &str,
) -> Option<&'a InstalledPluginMetadata> {
    select_best_glob_match(
        path,
        None::<PluginContentType>,
        plugins,
        |plugin| plugin.path_glob.as_str(),
        |plugin| plugin.content_type,
    )
}

async fn installed_plugins_at_history_depth<'a>(
    blob_reader: &Arc<dyn BlobDataReader>,
    installed_plugin_metadata_cache: &'a mut BTreeMap<(String, u32), Vec<InstalledPluginMetadata>>,
    descriptors: &[FileHistoryDescriptorRecord],
    directories: &[FileHistoryDirectoryRecord],
    blobs: &[FileHistoryBlobRecord],
    start_commit_id: &str,
    depth: u32,
) -> Result<&'a [InstalledPluginMetadata], LixError> {
    let key = (start_commit_id.to_string(), depth);
    if !installed_plugin_metadata_cache.contains_key(&key) {
        let rows = filesystem_live_rows_at_history_depth(
            descriptors,
            directories,
            blobs,
            start_commit_id,
            depth,
        );
        let filesystem = FilesystemIndex::from_live_rows(rows)?;
        let plugins =
            load_installed_plugin_metadata_from_filesystem(&filesystem, blob_reader.as_ref())
                .await?;
        installed_plugin_metadata_cache.insert(key.clone(), plugins);
    }
    Ok(installed_plugin_metadata_cache
        .get(&key)
        .map(Vec::as_slice)
        .unwrap_or(&[]))
}

async fn load_installed_plugin_metadata_from_filesystem(
    filesystem: &FilesystemIndex,
    blob_reader: &dyn BlobDataReader,
) -> Result<Vec<InstalledPluginMetadata>, LixError> {
    let mut plugins = Vec::new();
    for (path, file) in filesystem.file_entries() {
        let Some(plugin_key) = plugin_key_from_archive_path(path) else {
            continue;
        };
        let Some(blob_hash) = file.blob_hash.as_deref() else {
            continue;
        };
        let Ok(hash) = BlobHash::from_hex(blob_hash) else {
            continue;
        };
        let mut batch = blob_reader.load_bytes_many(&[hash]).await?.into_vec();
        let Some(archive_bytes) = batch.pop().flatten() else {
            continue;
        };
        let Ok(plugin) = load_installed_plugin_metadata_from_archive_bytes(
            &plugin_key,
            path,
            blob_hash,
            &archive_bytes,
        ) else {
            continue;
        };
        plugins.push(plugin);
    }
    Ok(plugins)
}

async fn installed_plugin_at_history_depth<'a>(
    blob_reader: &Arc<dyn BlobDataReader>,
    installed_plugins_cache: &'a mut BTreeMap<(String, u32, String), InstalledPlugin>,
    plugin_metadata: &InstalledPluginMetadata,
    start_commit_id: &str,
    depth: u32,
) -> Result<&'a InstalledPlugin, LixError> {
    let cache_key = (
        start_commit_id.to_string(),
        depth,
        plugin_metadata.key.clone(),
    );
    if !installed_plugins_cache.contains_key(&cache_key) {
        let Some(plugin) =
            load_installed_plugin_at_history_depth(blob_reader, plugin_metadata).await?
        else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "installed plugin archive '{}' is unavailable at history depth {}",
                    plugin_metadata.key, depth
                ),
            ));
        };
        installed_plugins_cache.insert(cache_key.clone(), plugin);
    }
    Ok(installed_plugins_cache
        .get(&cache_key)
        .expect("plugin should be cached after load"))
}

async fn load_installed_plugin_at_history_depth(
    blob_reader: &Arc<dyn BlobDataReader>,
    plugin_metadata: &InstalledPluginMetadata,
) -> Result<Option<InstalledPlugin>, LixError> {
    let hash = BlobHash::from_hex(&plugin_metadata.archive_blob_hash)?;
    let mut batch = blob_reader.load_bytes_many(&[hash]).await?.into_vec();
    let Some(archive_bytes) = batch.pop().flatten() else {
        return Ok(None);
    };
    Ok(Some(load_installed_plugin_from_archive_bytes(
        &plugin_metadata.key,
        &plugin_metadata.archive_path,
        &archive_bytes,
    )?))
}

fn filesystem_live_rows_at_history_depth(
    descriptors: &[FileHistoryDescriptorRecord],
    directories: &[FileHistoryDirectoryRecord],
    blobs: &[FileHistoryBlobRecord],
    start_commit_id: &str,
    depth: u32,
) -> Vec<MaterializedLiveStateRow> {
    let mut rows = Vec::new();
    let mut descriptor_ids = descriptors
        .iter()
        .filter(|record| record.entry.start_commit_id == start_commit_id)
        .map(|record| record.id.clone())
        .collect::<BTreeSet<_>>();
    for descriptor_id in std::mem::take(&mut descriptor_ids) {
        if let Some(record) = nearest_history_record(
            descriptors.iter().filter(|record| {
                record.id == descriptor_id && record.entry.start_commit_id == start_commit_id
            }),
            depth,
        ) {
            rows.push(history_entry_to_live_row(&record.entry));
        }
    }

    let mut directory_ids = directories
        .iter()
        .filter(|record| record.entry.start_commit_id == start_commit_id)
        .map(|record| record.id.clone())
        .collect::<BTreeSet<_>>();
    for directory_id in std::mem::take(&mut directory_ids) {
        if let Some(record) = nearest_history_record(
            directories.iter().filter(|record| {
                record.id == directory_id && record.entry.start_commit_id == start_commit_id
            }),
            depth,
        ) {
            rows.push(history_entry_to_live_row(&record.entry));
        }
    }

    let mut blob_file_ids = blobs
        .iter()
        .filter(|record| record.entry.start_commit_id == start_commit_id)
        .map(|record| record.file_id.clone())
        .collect::<BTreeSet<_>>();
    for file_id in std::mem::take(&mut blob_file_ids) {
        if let Some(record) = nearest_history_record(
            blobs.iter().filter(|record| {
                record.file_id == file_id && record.entry.start_commit_id == start_commit_id
            }),
            depth,
        ) {
            rows.push(history_entry_to_live_row(&record.entry));
        }
    }

    rows
}

fn plugin_state_live_rows_at_depth(
    plugin_state: &[FileHistoryPluginStateRecord],
    plugin: &InstalledPlugin,
    descriptor: &FileHistoryDescriptorRecord,
    event: &FileHistoryEvent,
) -> Vec<MaterializedLiveStateRow> {
    let plugin_schema_keys = plugin.schema_keys.iter().collect::<BTreeSet<_>>();
    let mut identities = plugin_state
        .iter()
        .filter(|record| {
            record.file_id == descriptor.id
                && record.entry.start_commit_id == event.start_commit_id
                && plugin_schema_keys.contains(&record.entry.change.schema_key)
        })
        .map(|record| {
            (
                record.entry.change.schema_key.clone(),
                record
                    .entry
                    .change
                    .entity_pk
                    .as_json_array_text()
                    .unwrap_or_default(),
            )
        })
        .collect::<BTreeSet<_>>();

    let mut rows = Vec::new();
    for (schema_key, entity_pk) in std::mem::take(&mut identities) {
        if let Some(record) = nearest_history_record(
            plugin_state.iter().filter(|record| {
                record.file_id == descriptor.id
                    && record.entry.start_commit_id == event.start_commit_id
                    && record.entry.change.schema_key == schema_key
                    && record
                        .entry
                        .change
                        .entity_pk
                        .as_json_array_text()
                        .is_ok_and(|candidate| candidate == entity_pk)
            }),
            event.depth,
        ) {
            rows.push(history_entry_to_live_row(&record.entry));
        }
    }
    rows
}

trait FileHistoryRecord {
    fn entry(&self) -> &HistoryEntry;
}

impl FileHistoryRecord for FileHistoryDescriptorRecord {
    fn entry(&self) -> &HistoryEntry {
        &self.entry
    }
}

impl FileHistoryRecord for FileHistoryDirectoryRecord {
    fn entry(&self) -> &HistoryEntry {
        &self.entry
    }
}

impl FileHistoryRecord for FileHistoryBlobRecord {
    fn entry(&self) -> &HistoryEntry {
        &self.entry
    }
}

impl FileHistoryRecord for FileHistoryPluginStateRecord {
    fn entry(&self) -> &HistoryEntry {
        &self.entry
    }
}

fn nearest_history_record<'a, T, I>(records: I, depth: u32) -> Option<&'a T>
where
    T: FileHistoryRecord + 'a,
    I: Iterator<Item = &'a T>,
{
    records
        .filter(|record| record.entry().depth >= depth)
        .min_by(|left, right| {
            left.entry()
                .depth
                .cmp(&right.entry().depth)
                .then(left.entry().change.id.cmp(&right.entry().change.id))
        })
}

fn history_entry_to_live_row(entry: &HistoryEntry) -> MaterializedLiveStateRow {
    MaterializedLiveStateRow {
        entity_pk: entry.change.entity_pk.clone(),
        schema_key: entry.change.schema_key.clone(),
        file_id: entry.change.file_id.clone(),
        snapshot_content: entry.change.snapshot_content.clone(),
        metadata: entry.change.metadata.clone(),
        deleted: entry.change.snapshot_content.is_none(),
        created_at: entry.change.created_at.clone(),
        updated_at: entry.change.created_at.clone(),
        global: false,
        change_id: None,
        commit_id: None,
        untracked: false,
        branch_id: GLOBAL_BRANCH_ID.to_string(),
    }
}

static LIX_FILE_HISTORY_COLS: ColumnTable<FileHistoryOutputRow> = ColumnTable {
    columns: &[
        ("id", Col::Utf8(|row| Some(row.id.as_str()))),
        ("path", Col::Utf8(|row| row.path.as_deref())),
        ("directory_id", Col::Utf8(|row| row.directory_id.as_deref())),
        ("name", Col::Utf8(|row| row.name.as_deref())),
        ("data", Col::Binary(|row| row.data.clone())),
        (
            HISTORY_COL_ENTITY_PK,
            Col::Utf8Fallible(|row| entity_pk_json_array(&row.entity_pk).map(Some)),
        ),
        (
            HISTORY_COL_SCHEMA_KEY,
            Col::Utf8(|_| Some(FILE_DESCRIPTOR_SCHEMA_KEY)),
        ),
        (
            HISTORY_COL_FILE_ID,
            Col::Utf8(|row| Some(row.entity_pk.as_str())),
        ),
        (
            HISTORY_COL_CHANGE_ID,
            Col::Utf8(|row| Some(row.event.change.id.as_str())),
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

/// Map [`ColumnTableError`] from [`LIX_FILE_HISTORY_COLS`] builds onto the exact
/// error messages the hand-written `file_history_record_batch` produced.
fn file_history_batch_error(error: ColumnTableError) -> LixError {
    match error {
        ColumnTableError::UnsupportedColumn(other) => LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("sql2 lix_file_history provider does not support projected column '{other}'"),
        ),
        ColumnTableError::Arrow(error) | ColumnTableError::ArrowZeroColumn(error) => LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("sql2 failed to build lix_file_history record batch: {error}"),
        ),
        ColumnTableError::Row(error) => error,
    }
}

pub(super) fn lix_file_history_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("path", DataType::Utf8, true),
        Field::new("directory_id", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("data", DataType::Binary, true),
        json_field(HISTORY_COL_ENTITY_PK, false),
        Field::new(HISTORY_COL_SCHEMA_KEY, DataType::Utf8, false),
        Field::new(HISTORY_COL_FILE_ID, DataType::Utf8, true),
        json_field(HISTORY_COL_SNAPSHOT_CONTENT, true),
        Field::new(HISTORY_COL_CHANGE_ID, DataType::Utf8, false),
        json_field(HISTORY_COL_METADATA, true),
        Field::new(HISTORY_COL_OBSERVED_COMMIT_ID, DataType::Utf8, false),
        Field::new(HISTORY_COL_COMMIT_CREATED_AT, DataType::Utf8, false),
        Field::new(HISTORY_COL_START_COMMIT_ID, DataType::Utf8, false),
        Field::new(HISTORY_COL_DEPTH, DataType::Int64, false),
    ]))
}

fn lix_error_to_datafusion_error(error: LixError) -> DataFusionError {
    crate::sql2::error::lix_error_to_datafusion_error(error)
}
