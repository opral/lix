use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::datasource::TableType;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
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
    HISTORY_COL_ORIGIN_KEY, HISTORY_COL_SCHEMA_KEY, HISTORY_COL_SNAPSHOT_CONTENT,
    HISTORY_COL_START_COMMIT_ID, HistoryColumnStyle, HistoryEntry, HistoryMetadataProjection,
    HistoryRoute, HistoryViewDescriptor, history_descriptor_event_matches, load_history_entries,
    parse_history_filter,
};
use crate::sql2::providers::filesystem_history_path::{
    HistoryDirectoryPathRecord, resolve_history_directory_path,
};
use crate::sql2::result_metadata::json_field;
use crate::storage::StorageRead;

use super::columns::{Col, ColumnTable, ColumnTableError};
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
        if parse_history_filter(filter, HistoryColumnStyle::Prefixed).is_some()
            || FileHistoryPublicPredicate::parse_exact(filter).is_some()
        {
            TableProviderFilterPushDown::Exact
        } else if !FileHistoryPublicPredicate::extract_conjuncts(filter).is_all() {
            // A mixed conjunction can be pruned by its public id/path terms,
            // but DataFusion must still evaluate the complete expression.
            TableProviderFilterPushDown::Inexact
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
        let metadata_projection =
            HistoryMetadataProjection::from_scan(&schema, filters, HistoryColumnStyle::Prefixed);
        let public_predicate = FileHistoryPublicPredicate::from_filters(filters);
        Ok(PlannedScan {
            schema: Arc::clone(&schema),
            ordering: None,
            load: row_source(
                (
                    Arc::clone(&self.commit_graph),
                    self.query_source.clone(),
                    Arc::clone(&self.blob_reader),
                    self.plugin_host.clone(),
                    route,
                    public_predicate,
                    schema,
                    metadata_projection,
                ),
                move |(
                    commit_graph,
                    query_source,
                    blob_reader,
                    plugin_host,
                    route,
                    public_predicate,
                    schema,
                    metadata_projection,
                )| async move {
                    let mut rows = load_file_history_rows(
                        commit_graph,
                        query_source,
                        &blob_reader,
                        plugin_host,
                        &route,
                        &public_predicate,
                        needs_data,
                        metadata_projection,
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

#[derive(Debug, Clone)]
struct PreparedFileHistoryRow {
    id: String,
    path: Option<String>,
    descriptor: FileHistoryDescriptorRecord,
    blob_hash: Option<String>,
    event: FileHistoryEvent,
}

/// Conservative early predicate for the public columns Atelier uses to point
/// lookup file history. `All` means that no safe public predicate was found;
/// unsupported expressions are always left to DataFusion.
#[derive(Debug, Clone, PartialEq, Eq)]
enum FileHistoryPublicPredicate {
    All,
    Ids(BTreeSet<String>),
    Paths(BTreeSet<String>),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
}

impl FileHistoryPublicPredicate {
    fn from_filters(filters: &[Expr]) -> Self {
        filters.iter().fold(Self::All, |predicate, filter| {
            predicate.and(Self::extract_conjuncts(filter))
        })
    }

    /// Extract only predicates that are guaranteed conjuncts. In particular,
    /// one supported side of an OR is not enough to prune the whole OR.
    fn extract_conjuncts(expr: &Expr) -> Self {
        match expr {
            Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::And => {
                Self::extract_conjuncts(&binary_expr.left)
                    .and(Self::extract_conjuncts(&binary_expr.right))
            }
            _ => Self::parse_exact(expr).unwrap_or(Self::All),
        }
    }

    fn parse_exact(expr: &Expr) -> Option<Self> {
        match expr {
            Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::And => Some(
                Self::parse_exact(&binary_expr.left)?.and(Self::parse_exact(&binary_expr.right)?),
            ),
            Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::Or => Some(
                Self::parse_exact(&binary_expr.left)?.or(Self::parse_exact(&binary_expr.right)?),
            ),
            Expr::BinaryExpr(binary_expr) => Self::from_binary_filter(binary_expr),
            Expr::InList(in_list) => Self::from_in_list(in_list),
            _ => None,
        }
    }

    fn from_binary_filter(binary_expr: &BinaryExpr) -> Option<Self> {
        if binary_expr.op != Operator::Eq {
            return None;
        }
        Self::from_column_literal(&binary_expr.left, &binary_expr.right)
            .or_else(|| Self::from_column_literal(&binary_expr.right, &binary_expr.left))
    }

    fn from_column_literal(column_expr: &Expr, literal_expr: &Expr) -> Option<Self> {
        let Expr::Column(column) = column_expr else {
            return None;
        };
        let value = string_literal(literal_expr)?;
        match column.name.as_str() {
            "id" => Some(Self::Ids(BTreeSet::from([value]))),
            "path" => Some(Self::Paths(BTreeSet::from([value]))),
            _ => None,
        }
    }

    fn from_in_list(in_list: &InList) -> Option<Self> {
        if in_list.negated {
            return None;
        }
        let Expr::Column(column) = in_list.expr.as_ref() else {
            return None;
        };
        let values = in_list
            .list
            .iter()
            .map(string_literal)
            .collect::<Option<BTreeSet<_>>>()?;
        if values.is_empty() {
            return None;
        }
        match column.name.as_str() {
            "id" => Some(Self::Ids(values)),
            "path" => Some(Self::Paths(values)),
            _ => None,
        }
    }

    fn matches(&self, id: &str, path: Option<&str>) -> bool {
        match self {
            Self::All => true,
            Self::Ids(ids) => ids.contains(id),
            // SQL equality/IN does not select a NULL path.
            Self::Paths(paths) => path.is_some_and(|path| paths.contains(path)),
            Self::And(left, right) => left.matches(id, path) && right.matches(id, path),
            Self::Or(left, right) => left.matches(id, path) || right.matches(id, path),
        }
    }

    fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::All, predicate) | (predicate, Self::All) => predicate,
            (left, right) => Self::And(Box::new(left), Box::new(right)),
        }
    }

    fn or(self, other: Self) -> Self {
        Self::Or(Box::new(self), Box::new(other))
    }

    fn is_all(&self) -> bool {
        matches!(self, Self::All)
    }
}

fn string_literal(expr: &Expr) -> Option<String> {
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
    public_predicate: &FileHistoryPublicPredicate,
    needs_data: bool,
    metadata_projection: HistoryMetadataProjection,
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
        metadata_projection,
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
        public_predicate,
    )
    .await?;
    let (mut event_plugin_state, plugin_state) = if plugin_schema_keys.is_empty() {
        (Vec::new(), Vec::new())
    } else {
        load_file_history_plugin_state(
            commit_graph,
            query_source,
            &event_route,
            &context_route,
            plugin_schema_keys,
            metadata_projection,
        )
        .await?
    };
    retain_matching_plugin_events(
        &mut event_plugin_state,
        &filesystem_context.descriptors,
        &filesystem_context.directories,
        public_predicate,
    );
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
    let prepared = prepare_file_history_rows(&filesystem_context, events, route, public_predicate)?;
    let blob_bytes = if needs_data {
        load_file_history_blob_bytes(blob_reader, &prepared).await?
    } else {
        BTreeMap::new()
    };

    let mut output = Vec::with_capacity(prepared.len());
    for prepared_row in prepared {
        let data = if needs_data && prepared_row.descriptor.name.is_some() {
            match prepared_row.blob_hash.as_deref() {
                Some(blob_hash) => blob_bytes.get(blob_hash).cloned().flatten(),
                None => match prepared_row.path.as_deref() {
                    Some(path) => {
                        let rendered = render_plugin_file_history_data(
                            &plugin_host,
                            blob_reader,
                            &mut installed_plugins_cache,
                            &installed_plugin_metadata_cache,
                            &plugin_state,
                            &prepared_row.descriptor,
                            &prepared_row.event,
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

        output.push(FileHistoryOutputRow {
            entity_pk: prepared_row.descriptor.id.clone(),
            id: prepared_row.id,
            path: prepared_row.path,
            directory_id: prepared_row.descriptor.directory_id.clone(),
            name: prepared_row.descriptor.name.clone(),
            data,
            descriptor_change: prepared_row.descriptor.entry.change.clone(),
            event: prepared_row.event,
        });
    }

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

fn prepare_file_history_rows(
    filesystem_context: &FileHistoryFilesystemContext,
    events: Vec<FileHistoryEvent>,
    route: &HistoryRoute,
    public_predicate: &FileHistoryPublicPredicate,
) -> Result<Vec<PreparedFileHistoryRow>, LixError> {
    let mut prepared = Vec::new();
    for event in events {
        let Some(descriptor) = nearest_file_descriptor(&filesystem_context.descriptors, &event)
        else {
            continue;
        };
        let path =
            resolve_file_history_path(descriptor, &filesystem_context.directories, event.depth);
        let id = tombstone_identity_column_value(
            "id",
            &descriptor.id,
            HistoryIdentityProjection::SingleColumn { column: "id" },
        )?
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| descriptor.id.clone());
        if !public_predicate.matches(&id, path.as_deref()) {
            continue;
        }
        let entity_pk = entity_pk_json_array(&descriptor.id).ok();
        if !route.matches_surface_row(
            FILE_DESCRIPTOR_SCHEMA_KEY,
            entity_pk.as_deref().unwrap_or(&descriptor.id),
            Some(&descriptor.id),
            event.depth,
        ) {
            continue;
        }
        prepared.push(PreparedFileHistoryRow {
            id,
            path,
            descriptor: descriptor.clone(),
            blob_hash: nearest_blob_ref(&filesystem_context.blobs, &event)
                .and_then(|blob| blob.blob_hash.clone()),
            event,
        });
    }
    Ok(prepared)
}

async fn load_file_history_blob_bytes(
    blob_reader: &Arc<dyn BlobDataReader>,
    rows: &[PreparedFileHistoryRow],
) -> Result<BTreeMap<String, Option<Vec<u8>>>, LixError> {
    let mut hashes = BTreeMap::<BlobHash, BTreeSet<String>>::new();
    for hash in rows
        .iter()
        .filter(|row| row.descriptor.name.is_some())
        .filter_map(|row| row.blob_hash.as_deref())
    {
        hashes
            .entry(BlobHash::from_hex(hash)?)
            .or_default()
            .insert(hash.to_string());
    }
    if hashes.is_empty() {
        return Ok(BTreeMap::new());
    }
    let request = hashes.keys().copied().collect::<Vec<_>>();
    let loaded = blob_reader.load_bytes_many(&request).await?.into_vec();
    if loaded.len() != request.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "file history blob batch returned {} values for {} requested hashes",
                loaded.len(),
                request.len()
            ),
        ));
    }
    let mut by_encoded_hash = BTreeMap::new();
    for ((_, encoded_hashes), bytes) in hashes.into_iter().zip(loaded) {
        for encoded_hash in encoded_hashes {
            by_encoded_hash.insert(encoded_hash, bytes.clone());
        }
    }
    Ok(by_encoded_hash)
}

async fn load_file_history_filesystem_context<S>(
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlHistoryQuerySource<S>,
    event_route: &HistoryRoute,
    context_route: &HistoryRoute,
    metadata_projection: HistoryMetadataProjection,
) -> Result<FileHistoryFilesystemContext, LixError>
where
    S: StorageRead + Clone + Send + Sync + 'static,
{
    let filesystem_schema_keys = file_history_filesystem_schema_keys();
    let (event_entries, context_entries) =
        load_file_history_entry_sets(event_route, context_route, move |route| {
            let commit_graph = Arc::clone(&commit_graph);
            let json_reader = query_source.json_reader.clone();
            let schema_keys = filesystem_schema_keys.clone();
            async move {
                load_history_entries(
                    HistoryViewDescriptor {
                        view_name: "lix_file_history",
                        start_commit_column: HISTORY_COL_START_COMMIT_ID,
                    },
                    commit_graph,
                    json_reader,
                    &route,
                    schema_keys,
                    metadata_projection,
                )
                .await
            }
        })
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
    metadata_projection: HistoryMetadataProjection,
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
    let (event_entries, context_entries) =
        load_file_history_entry_sets(event_route, context_route, move |route| {
            let commit_graph = Arc::clone(&commit_graph);
            let json_reader = query_source.json_reader.clone();
            let schema_keys = plugin_schema_keys.clone();
            async move {
                load_history_entries(
                    HistoryViewDescriptor {
                        view_name: "lix_file_history",
                        start_commit_column: HISTORY_COL_START_COMMIT_ID,
                    },
                    commit_graph,
                    json_reader,
                    &route,
                    schema_keys,
                    metadata_projection,
                )
                .await
            }
        })
        .await?;
    Ok((
        parse_file_history_plugin_state(&event_entries),
        parse_file_history_plugin_state(&context_entries),
    ))
}

async fn load_file_history_entry_sets<Load, LoadFuture>(
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
    public_predicate: &FileHistoryPublicPredicate,
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
        let paths = file_identity_paths_at_history_depth(
            &filesystem_context.descriptors,
            &filesystem_context.directories,
            &start_commit_id,
            depth,
        )
        .into_iter()
        .filter_map(|(id, path)| public_predicate.matches(&id, Some(&path)).then_some(path))
        .collect::<Vec<_>>();
        if paths.is_empty() {
            continue;
        }
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

fn file_identity_paths_at_history_depth(
    descriptors: &[FileHistoryDescriptorRecord],
    directories: &[FileHistoryDirectoryRecord],
    start_commit_id: &str,
    depth: u32,
) -> Vec<(String, String)> {
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
            let path = resolve_file_history_path(descriptor, directories, depth)?;
            Some((file_id, path))
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

fn retain_matching_plugin_events(
    plugin_state: &mut Vec<FileHistoryPluginStateRecord>,
    descriptors: &[FileHistoryDescriptorRecord],
    directories: &[FileHistoryDirectoryRecord],
    public_predicate: &FileHistoryPublicPredicate,
) {
    plugin_state.retain(|record| {
        let event = file_history_event_from_entry(record.file_id.clone(), &record.entry, 4);
        let Some(descriptor) = nearest_file_descriptor(descriptors, &event) else {
            return false;
        };
        let path = resolve_file_history_path(descriptor, directories, event.depth);
        public_predicate.matches(&descriptor.id, path.as_deref())
    });
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
        Field::new("data", DataType::LargeBinary, true),
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

fn lix_error_to_datafusion_error(error: LixError) -> DataFusionError {
    crate::sql2::error::lix_error_to_datafusion_error(error)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use datafusion::common::{Column, ScalarValue};
    use datafusion::logical_expr::{BinaryExpr, Expr, Operator};

    use crate::LixError;
    use crate::binary_cas::{BlobBytesBatch, BlobDataReader, BlobHash};
    use crate::entity_pk::EntityPk;
    use crate::sql2::change_materialization::MaterializedChange;
    use crate::sql2::history_route::HistoryEntry;

    use super::{
        FileHistoryBlobRecord, FileHistoryDescriptorRecord, FileHistoryFilesystemContext,
        FileHistoryPublicPredicate, HistoryRoute, PreparedFileHistoryRow,
        file_history_event_from_entry, load_file_history_blob_bytes, load_file_history_entry_sets,
        prepare_file_history_rows,
    };

    fn history_entry(file_id: &str, depth: u32, snapshot_content: Option<String>) -> HistoryEntry {
        HistoryEntry {
            change: MaterializedChange {
                id: format!("change-{file_id}-{depth}"),
                entity_pk: EntityPk::single(file_id),
                schema_key: super::FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                file_id: Some(file_id.to_string()),
                snapshot_content,
                metadata: None,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                origin_key: None,
            },
            observed_commit_id: format!("commit-{depth}"),
            commit_created_at: "2026-01-01T00:00:00Z".to_string(),
            start_commit_id: "start".to_string(),
            depth,
        }
    }

    fn descriptor(file_id: &str, name: Option<&str>, depth: u32) -> FileHistoryDescriptorRecord {
        let snapshot = name.map(|name| {
            serde_json::json!({
                "id": file_id,
                "directory_id": null,
                "name": name,
            })
            .to_string()
        });
        FileHistoryDescriptorRecord {
            id: file_id.to_string(),
            directory_id: None,
            name: name.map(str::to_string),
            entry: history_entry(file_id, depth, snapshot),
        }
    }

    fn blob_record(file_id: &str, hash: BlobHash, depth: u32) -> FileHistoryBlobRecord {
        let mut entry = history_entry(file_id, depth, None);
        entry.change.id = format!("blob-{file_id}-{depth}");
        entry.change.schema_key = super::BLOB_REF_SCHEMA_KEY.to_string();
        FileHistoryBlobRecord {
            file_id: file_id.to_string(),
            blob_hash: Some(hash.to_hex()),
            entry,
        }
    }

    fn filesystem_context(
        descriptors: Vec<FileHistoryDescriptorRecord>,
        blobs: Vec<FileHistoryBlobRecord>,
    ) -> FileHistoryFilesystemContext {
        FileHistoryFilesystemContext {
            event_descriptors: descriptors.clone(),
            event_directories: Vec::new(),
            event_blobs: blobs.clone(),
            descriptors,
            directories: Vec::new(),
            blobs,
        }
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

    #[derive(Default)]
    struct RecordingBlobReader {
        calls: StdMutex<Vec<Vec<BlobHash>>>,
        values: BTreeMap<BlobHash, Option<Vec<u8>>>,
    }

    #[async_trait]
    impl BlobDataReader for RecordingBlobReader {
        async fn load_bytes_many(&self, hashes: &[BlobHash]) -> Result<BlobBytesBatch, LixError> {
            self.calls.lock().unwrap().push(hashes.to_vec());
            Ok(BlobBytesBatch::new(
                hashes
                    .iter()
                    .map(|hash| self.values.get(hash).cloned().flatten())
                    .collect(),
            ))
        }
    }

    struct FixedBatchBlobReader(Vec<Option<Vec<u8>>>);

    #[async_trait]
    impl BlobDataReader for FixedBatchBlobReader {
        async fn load_bytes_many(&self, _hashes: &[BlobHash]) -> Result<BlobBytesBatch, LixError> {
            Ok(BlobBytesBatch::new(self.0.clone()))
        }
    }

    #[test]
    fn public_id_and_path_filters_prune_before_hydration() {
        let hash = BlobHash::from_content(b"content");
        let live_a = descriptor("file-a", Some("a.md"), 0);
        let live_b = descriptor("file-b", Some("b.md"), 0);
        let tombstone = descriptor("file-deleted", None, 0);
        let events = [&live_a, &live_b, &tombstone]
            .into_iter()
            .map(|descriptor| {
                file_history_event_from_entry(descriptor.id.clone(), &descriptor.entry, 1)
            })
            .collect::<Vec<_>>();
        let context = filesystem_context(
            vec![live_a, live_b, tombstone],
            vec![
                blob_record("file-a", hash, 0),
                blob_record("file-b", hash, 0),
            ],
        );

        let id_predicate = FileHistoryPublicPredicate::from_filters(&[eq_filter("id", "file-a")]);
        let by_id = prepare_file_history_rows(
            &context,
            events.clone(),
            &HistoryRoute::default(),
            &id_predicate,
        )
        .unwrap();
        assert_eq!(by_id.len(), 1);
        assert_eq!(by_id[0].id, "file-a");

        let path_predicate =
            FileHistoryPublicPredicate::from_filters(&[eq_filter("path", "/b.md")]);
        let by_path = prepare_file_history_rows(
            &context,
            events.clone(),
            &HistoryRoute::default(),
            &path_predicate,
        )
        .unwrap();
        assert_eq!(by_path.len(), 1);
        assert_eq!(by_path[0].id, "file-b");

        let tombstone_predicate =
            FileHistoryPublicPredicate::from_filters(&[eq_filter("id", "file-deleted")]);
        let deleted = prepare_file_history_rows(
            &context,
            events,
            &HistoryRoute::default(),
            &tombstone_predicate,
        )
        .unwrap();
        assert_eq!(deleted.len(), 1);
        assert_eq!(deleted[0].id, "file-deleted");
        assert_eq!(deleted[0].path, None);

        let unsafe_or = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(eq_filter("id", "file-a")),
            Operator::Or,
            Box::new(eq_filter("name", "b.md")),
        ));
        assert!(
            FileHistoryPublicPredicate::extract_conjuncts(&unsafe_or).is_all(),
            "one supported OR arm must not prune rows needed by the other arm"
        );
        let mixed_and = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(eq_filter("id", "file-a")),
            Operator::And,
            Box::new(eq_filter("name", "a.md")),
        ));
        assert!(
            FileHistoryPublicPredicate::extract_conjuncts(&mixed_and)
                .matches("file-a", Some("/a.md")),
            "a guaranteed public conjunct remains safe for early pruning"
        );
    }

    #[tokio::test]
    async fn blob_hydration_batches_deduplicates_and_preserves_missing_values() {
        let present_hash = BlobHash::from_content(b"present");
        let missing_hash = BlobHash::from_content(b"missing");
        let descriptor = descriptor("file-a", Some("a.md"), 0);
        let event = file_history_event_from_entry("file-a".to_string(), &descriptor.entry, 1);
        let row = |id: &str, hash: BlobHash| PreparedFileHistoryRow {
            id: id.to_string(),
            path: Some(format!("/{id}.md")),
            descriptor: FileHistoryDescriptorRecord {
                id: id.to_string(),
                ..descriptor.clone()
            },
            blob_hash: Some(hash.to_hex()),
            event: event.clone(),
        };
        let rows = vec![
            row("file-a", present_hash),
            row("file-b", present_hash),
            row("file-c", missing_hash),
        ];
        let reader = Arc::new(RecordingBlobReader {
            calls: StdMutex::new(Vec::new()),
            values: BTreeMap::from([(present_hash, Some(b"present".to_vec()))]),
        });
        let blob_reader: Arc<dyn BlobDataReader> = reader.clone();

        let loaded = load_file_history_blob_bytes(&blob_reader, &rows)
            .await
            .unwrap();

        let calls = reader.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].iter().copied().collect::<BTreeSet<_>>().len(), 2);
        assert_eq!(
            loaded.get(&present_hash.to_hex()),
            Some(&Some(b"present".to_vec()))
        );
        assert_eq!(loaded.get(&missing_hash.to_hex()), Some(&None));
    }

    #[tokio::test]
    async fn blob_hydration_rejects_malformed_batch_lengths() {
        let descriptor = descriptor("file-a", Some("a.md"), 0);
        let event = file_history_event_from_entry("file-a".to_string(), &descriptor.entry, 1);
        let row = |id: &str, hash: BlobHash| PreparedFileHistoryRow {
            id: id.to_string(),
            path: Some(format!("/{id}.md")),
            descriptor: FileHistoryDescriptorRecord {
                id: id.to_string(),
                ..descriptor.clone()
            },
            blob_hash: Some(hash.to_hex()),
            event: event.clone(),
        };
        let rows = vec![
            row("file-a", BlobHash::from_content(b"first")),
            row("file-b", BlobHash::from_content(b"second")),
        ];

        for malformed in [
            vec![Some(b"only-one".to_vec())],
            vec![None, None, Some(b"extra".to_vec())],
        ] {
            let reader: Arc<dyn BlobDataReader> = Arc::new(FixedBatchBlobReader(malformed));
            let error = load_file_history_blob_bytes(&reader, &rows)
                .await
                .expect_err("mismatched positional batch must fail");
            assert_eq!(error.code, LixError::CODE_INTERNAL_ERROR);
            assert!(error.message.contains("values for 2 requested hashes"));
        }
    }

    #[tokio::test]
    async fn unfiltered_bulk_history_keeps_all_rows_and_uses_one_blob_batch() {
        let first_hash = BlobHash::from_content(b"first");
        let second_hash = BlobHash::from_content(b"second");
        let descriptors = vec![
            descriptor("file-a", Some("a.md"), 0),
            descriptor("file-b", Some("b.md"), 0),
        ];
        let events = descriptors
            .iter()
            .map(|descriptor| {
                file_history_event_from_entry(descriptor.id.clone(), &descriptor.entry, 1)
            })
            .collect::<Vec<_>>();
        let context = filesystem_context(
            descriptors,
            vec![
                blob_record("file-a", first_hash, 0),
                blob_record("file-b", second_hash, 0),
            ],
        );
        let rows = prepare_file_history_rows(
            &context,
            events,
            &HistoryRoute::default(),
            &FileHistoryPublicPredicate::All,
        )
        .unwrap();
        assert_eq!(rows.len(), 2);

        let reader = Arc::new(RecordingBlobReader {
            calls: StdMutex::new(Vec::new()),
            values: BTreeMap::from([
                (first_hash, Some(b"first".to_vec())),
                (second_hash, Some(b"second".to_vec())),
            ]),
        });
        let blob_reader: Arc<dyn BlobDataReader> = reader.clone();
        let loaded = load_file_history_blob_bytes(&blob_reader, &rows)
            .await
            .unwrap();

        assert_eq!(reader.calls.lock().unwrap().len(), 1);
        assert_eq!(loaded.len(), 2);
    }

    #[tokio::test]
    async fn identical_event_and_context_routes_load_history_once() {
        let route = HistoryRoute {
            start_commit_ids: vec!["cid-start".to_string()],
            file_ids: vec!["file-a".to_string()],
            ..HistoryRoute::default()
        };
        let event_route = route.traversal_only();
        let context_route = route.starts_only();
        assert_eq!(event_route, context_route);

        let loads = Arc::new(AtomicUsize::new(0));
        let counted_loads = Arc::clone(&loads);
        let (event_entries, context_entries) =
            load_file_history_entry_sets(&event_route, &context_route, move |_| {
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
            load_file_history_entry_sets(&event_route, &context_route, move |_| {
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
