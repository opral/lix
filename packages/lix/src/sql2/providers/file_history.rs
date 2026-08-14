use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::datasource::TableType;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::LixError;
use crate::NullableKeyFilter;
use crate::binary_cas::BlobId;
use crate::changelog::CommitId;
use crate::commit_graph::CommitGraphReader;
use crate::common::{SharedStr, compose_file_path};
use crate::row_pk::RowPk;
use crate::forktree::{ForkTreeReadFacade, StateKey, encode_state_row_prefix_bounds};
use crate::plugin::{
    PLUGIN_OWNER_KEY, PLUGIN_REGISTRY_KEY, PluginFileOwner, PluginRegistry, PluginRuntimeHost,
};

use super::columns::{Col, ColumnTable, ColumnTableError};
use super::history_util::{
    ObservedStateOrdinal, ObservedStateRows, StateFilter, row_pk_json_array,
};
use super::spec::{PlannedScan, TableSpec, projected_schema, register_spec_table, scan_row_source};
use crate::sql2::SqlChangelogQuerySource;
use crate::sql2::WriteAccess;
use crate::sql2::change_materialization::MaterializedChange;
use crate::sql2::history_projection::{HistoryIdentityProjection, tombstone_identity_column_value};
use crate::sql2::history_route::{
    HISTORY_COL_AS_OF_COMMIT_ID, HISTORY_COL_COMMIT_CREATED_AT, HISTORY_COL_DEPTH,
    HISTORY_COL_ROW_PK, HISTORY_COL_IS_DELETED, HISTORY_COL_OBSERVED_COMMIT_ID,
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

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";
const BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
const KEY_VALUE_SCHEMA_KEY: &str = "lix_key_value";

fn file_history_owner_schema_keys(
    state: &FileHistoryObservedState,
    owner: &PluginFileOwner,
) -> Result<Vec<String>, LixError> {
    state
        .plugin_registry
        .get(owner.plugin_key())
        .map(crate::plugin::PluginRegistryEntry::schema_keys)
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            invalid_file_history_state(format!(
                "plugin owner '{}' references missing authenticated registry entry '{}'",
                owner.file_id(),
                owner.plugin_key(),
            ))
        })
}

pub(super) async fn register_lix_file_history_surface<S>(
    session: &datafusion::prelude::SessionContext,
    surface_name: &str,
    commit_graph: super::SharedCommitGraph,
    query_source: SqlChangelogQuerySource<S>,
    default_as_of_commit_id: String,
    plugin_host: PluginRuntimeHost,
) -> Result<(), LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    register_spec_table(
        session,
        surface_name,
        Arc::new(LixFileHistorySpec {
            commit_graph,
            query_source,
            default_as_of_commit_id,
            plugin_host,
        }),
        WriteAccess::read_only(),
    )
}

/// SQL spec for `lix_file_history`.
///
/// The reachability-aware file history surface: rows are reconstructed by
/// walking the commit graph from the routed anchor commits, resolving the
/// nearest descriptor/blob/directory events per file.
struct LixFileHistorySpec<S> {
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlChangelogQuerySource<S>,
    default_as_of_commit_id: String,
    plugin_host: PluginRuntimeHost,
}

#[async_trait]
impl<S> TableSpec<S> for LixFileHistorySpec<S>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    #[expect(clippy::unnecessary_literal_bound)]
    fn table_name(&self) -> &str {
        "lix_file_history"
    }

    fn schema(&self) -> SchemaRef {
        lix_file_history_schema()
    }

    fn history_anchor_column(&self) -> Option<&'static str> {
        Some(HISTORY_COL_AS_OF_COMMIT_ID)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn filter_pushdown(&self, filter: &Expr) -> TableProviderFilterPushDown {
        if parse_history_filter(filter).is_some()
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
        let full_schema = lix_file_history_schema();
        let schema = projected_schema(&full_schema, projection);
        let needs_data = projection.is_none_or(|projection| {
            projection.iter().any(|index| {
                full_schema
                    .field(*index)
                    .name()
                    .as_str()
                    .eq_ignore_ascii_case("content")
            })
        });
        let mut route = HistoryRoute::from_filters(filters);
        route.default_to_as_of_commit_id(&self.default_as_of_commit_id);
        let metadata_projection = HistoryMetadataProjection::from_scan(&schema, filters);
        let public_predicate = FileHistoryPublicPredicate::from_filters(filters);
        let lookup_ids = FileHistoryLookupIds::from_public_predicate(&public_predicate);
        Ok(PlannedScan {
            schema: Arc::clone(&schema),
            ordering: None,
            source: scan_row_source(
                Arc::clone(&schema),
                (
                    Arc::clone(&self.commit_graph),
                    self.query_source.clone(),
                    self.plugin_host.clone(),
                    route,
                    public_predicate,
                    lookup_ids,
                    schema,
                    metadata_projection,
                ),
                move |(
                    commit_graph,
                    query_source,
                    plugin_host,
                    route,
                    public_predicate,
                    lookup_ids,
                    schema,
                    metadata_projection,
                )| async move {
                    let mut rows = load_file_history_rows(
                        commit_graph,
                        query_source,
                        &plugin_host,
                        &route,
                        &public_predicate,
                        lookup_ids.as_ref(),
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
    entry: HistoryEntry,
}

#[derive(Debug, Clone)]
struct FileHistoryDirectoryRecord {
    id: String,
    parent_id: Option<String>,
    name: Option<String>,
    entry: HistoryEntry,
}

impl DirectoryPathRecord for FileHistoryDirectoryRecord {
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

#[derive(Debug, Clone)]
struct FileHistoryBlobRecord {
    file_id: String,
    entry: HistoryEntry,
}

#[derive(Debug, Clone)]
struct FileHistoryPluginStateRecord {
    file_id: String,
    entry: HistoryEntry,
}

#[derive(Debug, Clone)]
struct FileHistoryPluginOwnerRecord {
    file_id: String,
    entry: HistoryEntry,
}

#[derive(Debug, Clone)]
struct FileHistoryEvent {
    file_id: String,
    as_of_commit_id: String,
    depth: u32,
    source_changes: Vec<MaterializedChange>,
    observed_commit_id: String,
    commit_created_at: Option<String>,
}

#[derive(Debug)]
struct FileHistoryOutputRow {
    observed_state: Arc<FileHistoryObservedState>,
    descriptor_ordinal: u32,
    id: String,
    path: Option<String>,
    data: Option<Vec<u8>>,
    event: FileHistoryEvent,
}

impl FileHistoryOutputRow {
    fn descriptor(&self) -> &FileHistoryObservedDescriptorRecord {
        let descriptor = &self.observed_state.descriptors[self.descriptor_ordinal as usize];
        let _ = self.observed_state.rows.row(descriptor.row);
        descriptor
    }
}

#[derive(Debug)]
struct PreparedFileHistoryRow {
    id: String,
    path: Option<String>,
    observed_state: Arc<FileHistoryObservedState>,
    descriptor_ordinal: u32,
    blob_hash: Option<String>,
    event: FileHistoryEvent,
}

impl PreparedFileHistoryRow {
    fn descriptor(&self) -> &FileHistoryObservedDescriptorRecord {
        let descriptor = &self.observed_state.descriptors[self.descriptor_ordinal as usize];
        let _ = self.observed_state.rows.row(descriptor.row);
        descriptor
    }
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

    fn exact_ids_for_lookup(&self) -> Option<BTreeSet<String>> {
        match self {
            Self::Ids(ids) => Some(ids.clone()),
            Self::And(left, right) => {
                match (left.exact_ids_for_lookup(), right.exact_ids_for_lookup()) {
                    (Some(left), Some(right)) => Some(left.intersection(&right).cloned().collect()),
                    (Some(ids), None) | (None, Some(ids)) => Some(ids),
                    (None, None) => None,
                }
            }
            Self::All | Self::Paths(_) | Self::Or(_, _) => None,
        }
    }
}

/// A conservative exact public `id` constraint that can be translated to the
/// canonical descriptor/blob row primary key. Unlike
/// [`FileHistoryPublicPredicate`], this deliberately declines disjunctions and
/// non-literal `id` expressions: those retain the existing complete traversal
/// and DataFusion residual evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FileHistoryLookupIds(BTreeSet<String>);

impl FileHistoryLookupIds {
    fn from_public_predicate(predicate: &FileHistoryPublicPredicate) -> Option<Self> {
        predicate
            .exact_ids_for_lookup()
            .filter(|ids| !ids.is_empty())
            .map(Self)
    }

    fn row_pks(&self) -> Result<Vec<String>, LixError> {
        self.0.iter().map(|id| row_pk_json_array(id)).collect()
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
    size_bytes: u64,
}

struct FileHistoryFilesystemContext {
    event_descriptors: Vec<FileHistoryDescriptorRecord>,
    event_directories: Vec<FileHistoryDirectoryRecord>,
    event_blobs: Vec<FileHistoryBlobRecord>,
    descriptors: Vec<FileHistoryDescriptorRecord>,
}

#[derive(Debug, Clone)]
struct FileHistoryObservedDescriptorRecord {
    id: String,
    directory_id: Option<String>,
    name: Option<String>,
    row: ObservedStateOrdinal,
}

#[derive(Debug, Clone)]
struct FileHistoryObservedDirectoryRecord {
    id: String,
    parent_id: Option<String>,
    name: Option<String>,
    row: ObservedStateOrdinal,
}

impl DirectoryPathRecord for FileHistoryObservedDirectoryRecord {
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

#[derive(Debug, Clone)]
struct FileHistoryObservedBlobRecord {
    file_id: String,
    blob_hash: Option<String>,
    size_bytes: Option<u64>,
    deleted: bool,
    row: ObservedStateOrdinal,
}

#[derive(Debug, Clone)]
struct FileHistoryObservedPluginOwnerRecord {
    file_id: String,
    owner: Option<PluginFileOwner>,
    row: ObservedStateOrdinal,
}

#[derive(Debug)]
struct FileHistoryObservedState {
    rows: ObservedStateRows,
    descriptors: Vec<FileHistoryObservedDescriptorRecord>,
    directories: Vec<FileHistoryObservedDirectoryRecord>,
    blobs: Vec<FileHistoryObservedBlobRecord>,
    plugin_owners: Vec<FileHistoryObservedPluginOwnerRecord>,
    plugin_registry: PluginRegistry,
}

struct FileHistoryDirectoryIndex {
    tree: HistoryDirectoryTree,
    file_ids_by_directory: BTreeMap<String, BTreeSet<String>>,
}

impl FileHistoryDirectoryIndex {
    fn from_state(state: &FileHistoryObservedState) -> Self {
        let mut file_ids_by_directory = BTreeMap::<String, BTreeSet<String>>::new();
        for descriptor in &state.descriptors {
            let _ = state.rows.row(descriptor.row);
            if let Some(directory_id) = &descriptor.directory_id {
                file_ids_by_directory
                    .entry(directory_id.clone())
                    .or_default()
                    .insert(descriptor.id.clone());
            }
        }
        for directory in &state.directories {
            let _ = state.rows.row(directory.row);
        }
        Self {
            tree: HistoryDirectoryTree::from_records(&state.directories),
            file_ids_by_directory,
        }
    }

    fn affected_file_ids(&self, changed_directory_id: &str) -> Result<BTreeSet<String>, LixError> {
        let mut file_ids = BTreeSet::new();
        self.visit_affected_file_buckets(changed_directory_id, |bucket| {
            file_ids.extend(bucket.iter().cloned());
        })?;
        Ok(file_ids)
    }

    fn visit_affected_file_buckets(
        &self,
        changed_directory_id: &str,
        mut visit: impl FnMut(&BTreeSet<String>),
    ) -> Result<(), LixError> {
        for directory_id in self.tree.descendants_including(changed_directory_id)? {
            if let Some(bucket) = self.file_ids_by_directory.get(&directory_id) {
                visit(bucket);
            }
        }
        Ok(())
    }
}

struct FileHistoryPluginDiscovery {
    schema_keys: Vec<String>,
    registries_by_commit: BTreeMap<String, PluginRegistry>,
    parent_commit_ids_by_commit: BTreeMap<String, Vec<String>>,
    registry_events: Vec<HistoryEntry>,
}

async fn load_file_history_rows<S>(
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlChangelogQuerySource<S>,
    _plugin_host: &PluginRuntimeHost,
    route: &HistoryRoute,
    public_predicate: &FileHistoryPublicPredicate,
    lookup_ids: Option<&FileHistoryLookupIds>,
    needs_data: bool,
    metadata_projection: HistoryMetadataProjection,
) -> Result<Vec<FileHistoryOutputRow>, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
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
    let context_route = route.anchors_only();
    let filesystem_context = load_file_history_filesystem_context(
        Arc::clone(&commit_graph),
        query_source.clone(),
        &event_route,
        &context_route,
        lookup_ids,
        metadata_projection,
    )
    .await?;
    let parent_commit_ids_by_commit =
        load_history_commit_parents(&commit_graph, &context_route.as_of_commit_ids).await?;
    let historical = query_source.forktree_reader.clone();
    let plugin_discovery = discover_file_history_plugins(
        Arc::clone(&commit_graph),
        query_source.clone(),
        &historical,
        &event_route,
        &parent_commit_ids_by_commit,
        metadata_projection,
    )
    .await?;
    let plugin_schema_keys = plugin_discovery.schema_keys.clone();
    let event_plugin_state = if plugin_schema_keys.is_empty() {
        Vec::new()
    } else {
        let (events, _) = load_file_history_plugin_state(
            Arc::clone(&commit_graph),
            query_source.clone(),
            &event_route,
            &context_route,
            plugin_schema_keys.clone(),
            lookup_ids,
            metadata_projection,
        )
        .await?;
        events
    };
    let event_plugin_owners = load_file_history_plugin_owner_events(
        Arc::clone(&commit_graph),
        query_source.clone(),
        &event_route,
        lookup_ids,
        metadata_projection,
    )
    .await?;
    // Ownership replacement and deletion can tombstone the prior owner's
    // plugin state in the same commit. The exact observed root contains only
    // the new owner (or its tombstone), so retain direct-parent roots as the
    // ownership evidence for those cleanup changes.
    let mut observed_commit_ids = filesystem_context
        .event_descriptors
        .iter()
        .map(|record| record.entry.observed_commit_id.clone())
        .chain(
            filesystem_context
                .event_directories
                .iter()
                .map(|record| record.entry.observed_commit_id.clone()),
        )
        .chain(
            filesystem_context
                .event_blobs
                .iter()
                .map(|record| record.entry.observed_commit_id.clone()),
        )
        .chain(
            event_plugin_state
                .iter()
                .map(|record| record.entry.observed_commit_id.clone()),
        )
        .chain(
            event_plugin_owners
                .iter()
                .map(|record| record.entry.observed_commit_id.clone()),
        )
        .chain(
            plugin_discovery
                .registry_events
                .iter()
                .map(|entry| entry.observed_commit_id.clone()),
        )
        .collect::<BTreeSet<_>>();
    let parent_evidence_commit_ids = filesystem_context
        .event_directories
        .iter()
        .map(|record| record.entry.observed_commit_id.as_str())
        .chain(
            event_plugin_owners
                .iter()
                .map(|record| record.entry.observed_commit_id.as_str()),
        );
    let direct_parent_commit_ids = parent_evidence_commit_ids
        .flat_map(|observed_commit_id| {
            parent_commit_ids_by_commit
                .get(observed_commit_id)
                .into_iter()
                .flatten()
                .cloned()
        })
        .collect::<Vec<_>>();
    observed_commit_ids.extend(direct_parent_commit_ids);
    let observed_states =
        load_file_history_observed_states(&historical, observed_commit_ids, lookup_ids).await?;
    let filesystem_events = file_history_events(
        &filesystem_context.event_descriptors,
        &filesystem_context.event_directories,
        &filesystem_context.event_blobs,
        &filesystem_context.descriptors,
        &observed_states,
        &parent_commit_ids_by_commit,
    )?;
    let plugin_state_events = file_history_plugin_events(
        &event_plugin_state,
        &event_plugin_owners,
        &observed_states,
        &plugin_discovery.parent_commit_ids_by_commit,
    )?;
    let plugin_owner_events = event_plugin_owners
        .iter()
        .map(|record| file_history_event_from_entry(record.file_id.clone(), &record.entry));
    let plugin_registry_events = file_history_plugin_registry_events(
        &plugin_discovery.registry_events,
        &observed_states,
        &plugin_discovery.registries_by_commit,
        &plugin_discovery.parent_commit_ids_by_commit,
    )?;
    let events = sorted_grouped_file_history_events(
        filesystem_events
            .into_iter()
            .chain(plugin_state_events)
            .chain(plugin_owner_events)
            .chain(plugin_registry_events),
    )?;
    let prepared = prepare_file_history_rows(&observed_states, events, route, public_predicate)?;
    let blob_bytes = load_file_history_blob_bytes(&historical, &prepared).await?;

    let mut output = Vec::with_capacity(prepared.len());
    for prepared_row in prepared {
        let data = if prepared_row.descriptor().name.is_some() {
            let blob_hash = validate_exactly_one_blob_ref(
                &prepared_row.observed_state,
                &prepared_row.event,
                true,
            )?
            .and_then(|reference| reference.blob_hash.as_deref());
            let bytes = blob_hash
                .and_then(|blob_hash| blob_bytes.get(blob_hash))
                .and_then(Option::as_deref);
            validate_file_history_materialization(&prepared_row, bytes)?;
            if needs_data {
                Some(bytes.unwrap_or_default().to_vec())
            } else {
                None
            }
        } else {
            None
        };

        output.push(FileHistoryOutputRow {
            observed_state: Arc::clone(&prepared_row.observed_state),
            descriptor_ordinal: prepared_row.descriptor_ordinal,
            id: prepared_row.id,
            path: prepared_row.path,
            data,
            event: prepared_row.event,
        });
    }

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
    });
    Ok(output)
}

fn prepare_file_history_rows(
    observed_states: &BTreeMap<String, Arc<FileHistoryObservedState>>,
    events: Vec<FileHistoryEvent>,
    route: &HistoryRoute,
    public_predicate: &FileHistoryPublicPredicate,
) -> Result<Vec<PreparedFileHistoryRow>, LixError> {
    let directory_indexes = observed_states
        .iter()
        .map(|(commit_id, state)| {
            (
                commit_id.as_str(),
                FileHistoryDirectoryIndex::from_state(state.as_ref()),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut prepared = Vec::new();
    for event in events {
        let Some(state) = observed_states.get(&event.observed_commit_id) else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "lix_file_history did not load observed commit '{}'",
                    event.observed_commit_id
                ),
            ));
        };
        let Some((descriptor_ordinal, descriptor)) = state
            .descriptors
            .iter()
            .enumerate()
            .find(|(_, descriptor)| descriptor.id == event.file_id)
        else {
            return Err(invalid_file_history_state(format!(
                "file history event for '{}' at commit '{}' has no authenticated descriptor",
                event.file_id, event.observed_commit_id
            )));
        };
        let directory_index = directory_indexes
            .get(event.observed_commit_id.as_str())
            .expect("every observed file state should have a directory index");
        if !file_history_event_affects_observed_file(&event, descriptor, &directory_index.tree)? {
            continue;
        }
        let path = resolve_observed_file_history_path(descriptor, &state.directories)?;
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
        let row_pk = row_pk_json_array(&descriptor.id).ok();
        if !route.matches_surface_row(
            FILE_DESCRIPTOR_SCHEMA_KEY,
            row_pk.as_deref().unwrap_or(&descriptor.id),
            Some(&descriptor.id),
            event.depth,
        ) {
            continue;
        }
        let blob_hash = validate_exactly_one_blob_ref(state, &event, descriptor.name.is_some())?
            .and_then(|blob| blob.blob_hash.clone());
        prepared.push(PreparedFileHistoryRow {
            id,
            path,
            observed_state: Arc::clone(state),
            descriptor_ordinal: u32::try_from(descriptor_ordinal).map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "lix_file_history observed descriptor ordinal exceeds u32",
                )
            })?,
            blob_hash,
            event,
        });
    }
    Ok(prepared)
}

async fn load_file_history_blob_bytes<S>(
    historical: &ForkTreeReadFacade<S>,
    rows: &[PreparedFileHistoryRow],
) -> Result<BTreeMap<String, Option<Bytes>>, LixError>
where
    S: StorageAdapterRead,
{
    let mut requests = Vec::new();
    let mut hashes = Vec::new();
    for row in rows.iter().filter(|row| row.descriptor().name.is_some()) {
        let Some(reference) = validate_exactly_one_blob_ref(&row.observed_state, &row.event, true)?
        else {
            continue;
        };
        let observed = row.observed_state.rows.row(reference.row);
        requests.push((
            observed.observed_commit_id().to_owned(),
            observed.row().key().clone(),
        ));
        hashes.push(row.blob_hash.clone().ok_or_else(|| {
            invalid_file_history_state(format!(
                "file '{}' at commit '{}' has no authenticated BlobId",
                row.id, row.event.observed_commit_id
            ))
        })?);
    }
    if requests.is_empty() {
        return Ok(BTreeMap::new());
    }
    let loaded = historical
        .load_historical_blob_bytes_for_rows(&requests)
        .await?
        .into_shared_vec();
    if loaded.len() != hashes.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "file history blob batch returned {} values for {} requested hashes",
                loaded.len(),
                hashes.len()
            ),
        ));
    }
    let mut by_encoded_hash = BTreeMap::new();
    for (encoded_hash, bytes) in hashes.into_iter().zip(loaded) {
        if let Some(previous) = by_encoded_hash.insert(encoded_hash.clone(), bytes.clone())
            && previous != bytes
        {
            return Err(invalid_file_history_state(format!(
                "file history BlobId '{}' resolves to conflicting authenticated payloads",
                encoded_hash
            )));
        }
    }
    Ok(by_encoded_hash)
}

fn invalid_file_history_state(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INVALID_PLUGIN, message)
}

/// Validate a historical file materialization. A live descriptor without a
/// BlobRef is the canonical descriptor-only representation of an empty file.
/// Once a BlobRef exists, its identity, size, and payload remain strict.
fn validate_file_history_materialization(
    prepared: &PreparedFileHistoryRow,
    payload: Option<&[u8]>,
) -> Result<(), LixError> {
    let observed_commit_id = prepared.event.observed_commit_id.as_str();
    let state = prepared.observed_state.as_ref();
    let descriptor = prepared.descriptor();
    let plugin = live_file_history_plugin_owner(state, &descriptor.id)
        .map(|owner| {
            state
                .plugin_registry
                .get(owner.plugin_key())
                .ok_or_else(|| {
                    invalid_file_history_state(format!(
                        "plugin-owned file '{}' at commit '{observed_commit_id}' names unavailable plugin '{}'",
                        descriptor.id,
                        owner.plugin_key(),
                    ))
                })
        })
        .transpose()?;
    let Some(blob) = validate_exactly_one_blob_ref(state, &prepared.event, true)? else {
        if payload.is_some_and(|payload| !payload.is_empty()) {
            return Err(invalid_file_history_state(format!(
                "descriptor-only file '{}' at commit '{observed_commit_id}' has an unexpected payload",
                descriptor.id
            )));
        }
        let _ = plugin;
        return Ok(());
    };
    let blob_hash = blob.blob_hash.as_deref().ok_or_else(|| {
        invalid_file_history_state(format!(
            "file '{}' at commit '{observed_commit_id}' has no live blob identity",
            descriptor.id
        ))
    })?;
    let expected_blob_id = BlobId::from_hex(blob_hash).map_err(|error| {
        invalid_file_history_state(format!(
            "file '{}' at commit '{observed_commit_id}' has an invalid BlobId: {error}",
            descriptor.id
        ))
    })?;
    let payload = payload.ok_or_else(|| {
        invalid_file_history_state(format!(
            "file '{}' blob payload '{}' is missing at observed commit '{observed_commit_id}'",
            descriptor.id, blob_hash
        ))
    })?;
    let declared_size = blob.size_bytes.ok_or_else(|| {
        invalid_file_history_state(format!(
            "file '{}' blob reference '{}' has no declared size at observed commit '{observed_commit_id}'",
            descriptor.id, blob_hash
        ))
    })?;
    let payload_size = u64::try_from(payload.len()).map_err(|_| {
        invalid_file_history_state(format!(
            "file '{}' blob payload '{}' exceeds the supported size at observed commit '{observed_commit_id}'",
            descriptor.id, blob_hash
        ))
    })?;
    if declared_size != payload_size || BlobId::from_canonical_content(payload) != expected_blob_id
    {
        return Err(invalid_file_history_state(format!(
            "file '{}' blob reference '{}' does not authenticate its payload at observed commit '{observed_commit_id}'",
            descriptor.id, blob_hash
        )));
    }
    let _ = plugin;
    Ok(())
}

fn validate_exactly_one_blob_ref<'a>(
    state: &'a FileHistoryObservedState,
    event: &FileHistoryEvent,
    require_live: bool,
) -> Result<Option<&'a FileHistoryObservedBlobRecord>, LixError> {
    let refs = state
        .blobs
        .iter()
        .filter(|blob| blob.file_id == event.file_id)
        .collect::<Vec<_>>();
    if refs.len() > 1 {
        return Err(invalid_file_history_state(format!(
            "file '{}' at commit '{}' has duplicate or conflicting BlobRef rows",
            event.file_id, event.observed_commit_id
        )));
    }
    if refs.is_empty() {
        return absent_blob_ref();
    }
    let blob = refs[0];
    let _ = state.rows.row(blob.row);
    match (blob.deleted, require_live) {
        (true, true) => Err(invalid_file_history_state(format!(
            "file '{}' at commit '{}' has a tombstoned BlobRef",
            event.file_id, event.observed_commit_id
        ))),
        (true, false) => Ok(None),
        (false, false) => Err(invalid_file_history_state(format!(
            "file '{}' at commit '{}' has a live BlobRef for a deleted descriptor",
            event.file_id, event.observed_commit_id
        ))),
        (false, true) => {
            if blob.blob_hash.is_none() || blob.size_bytes.is_none() {
                return Err(invalid_file_history_state(format!(
                    "file '{}' at commit '{}' has an incomplete authenticated BlobRef",
                    event.file_id, event.observed_commit_id
                )));
            }
            Ok(Some(blob))
        }
    }
}

fn absent_blob_ref<'a>() -> Result<Option<&'a FileHistoryObservedBlobRecord>, LixError> {
    Ok(None)
}

async fn load_file_history_filesystem_context<S>(
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlChangelogQuerySource<S>,
    event_route: &HistoryRoute,
    context_route: &HistoryRoute,
    lookup_ids: Option<&FileHistoryLookupIds>,
    metadata_projection: HistoryMetadataProjection,
) -> Result<FileHistoryFilesystemContext, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    let lookup_ids = lookup_ids.cloned();
    let context_entries = load_file_history_filesystem_entries(
        Arc::clone(&commit_graph),
        query_source.clone(),
        context_route,
        lookup_ids.as_ref(),
        metadata_projection,
        None,
    )
    .await?;
    let directory_seed_ids = parse_file_history_directories(&context_entries)?
        .into_iter()
        .map(|directory| directory.id)
        .collect::<BTreeSet<_>>();
    let event_entries = load_file_history_filesystem_entries(
        commit_graph,
        query_source,
        event_route,
        lookup_ids.as_ref(),
        metadata_projection,
        Some(&directory_seed_ids),
    )
    .await?;

    Ok(FileHistoryFilesystemContext {
        event_descriptors: parse_file_history_descriptors(&event_entries)?,
        event_directories: parse_file_history_directories(&event_entries)?,
        event_blobs: parse_file_history_blobs(&event_entries)?,
        descriptors: parse_file_history_descriptors(&context_entries)?,
    })
}

async fn load_file_history_observed_states<S>(
    historical: &ForkTreeReadFacade<S>,
    observed_commit_ids: BTreeSet<String>,
    lookup_ids: Option<&FileHistoryLookupIds>,
) -> Result<BTreeMap<String, Arc<FileHistoryObservedState>>, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    let mut states = BTreeMap::new();
    for observed_commit_id in observed_commit_ids {
        let state =
            load_file_history_observed_state(historical, &observed_commit_id, lookup_ids).await?;
        states.insert(observed_commit_id, Arc::new(state));
    }
    Ok(states)
}

async fn load_file_history_observed_state<S>(
    historical: &ForkTreeReadFacade<S>,
    observed_commit_id: &str,
    lookup_ids: Option<&FileHistoryLookupIds>,
) -> Result<FileHistoryObservedState, LixError>
where
    S: StorageAdapterRead,
{
    let observed_commit: SharedStr = observed_commit_id.into();
    let plugin_registry =
        load_plugin_registry_at_observed_commit(historical, &observed_commit).await?;
    let plugin_owner_rows = load_file_history_plugin_owner_rows_at_observed_commit(
        historical,
        &observed_commit,
        lookup_ids,
    )
    .await?;
    let mut rows = if let Some(lookup_ids) = lookup_ids {
        load_selected_file_history_observed_rows(historical, &observed_commit, lookup_ids).await?
    } else {
        scan_file_history_observed_rows(
            historical,
            &observed_commit,
            StateFilter {
                schema_keys: file_history_filesystem_schema_keys(),
                include_tombstones: true,
                ..StateFilter::default()
            },
        )
        .await?
    };
    rows.append(plugin_owner_rows)?;
    let descriptors = parse_file_history_observed_descriptors(&rows)?;
    let directories = parse_file_history_observed_directories(&rows)?;
    let blobs = parse_file_history_observed_blobs(&rows)?;
    let plugin_owners = parse_file_history_observed_plugin_owners(&rows)?;
    Ok(FileHistoryObservedState {
        rows,
        descriptors,
        directories,
        blobs,
        plugin_owners,
        plugin_registry,
    })
}

async fn load_file_history_plugin_owner_rows_at_observed_commit<S>(
    historical: &ForkTreeReadFacade<S>,
    observed_commit_id: &SharedStr,
    lookup_ids: Option<&FileHistoryLookupIds>,
) -> Result<ObservedStateRows, LixError>
where
    S: StorageAdapterRead,
{
    scan_file_history_observed_rows(
        historical,
        observed_commit_id,
        StateFilter {
            schema_keys: vec![KEY_VALUE_SCHEMA_KEY.to_string()],
            row_pks: vec![RowPk::single(PLUGIN_OWNER_KEY)],
            file_ids: lookup_ids
                .map(|lookup_ids| {
                    lookup_ids
                        .0
                        .iter()
                        .cloned()
                        .map(NullableKeyFilter::Value)
                        .collect()
                })
                .unwrap_or_default(),
            include_tombstones: true,
        },
    )
    .await
}

async fn load_selected_file_history_observed_rows<S>(
    historical: &ForkTreeReadFacade<S>,
    observed_commit_id: &SharedStr,
    lookup_ids: &FileHistoryLookupIds,
) -> Result<ObservedStateRows, LixError>
where
    S: StorageAdapterRead,
{
    let row_pks = lookup_ids
        .0
        .iter()
        .map(|file_id| {
            RowPk::uuid_from_canonical(file_id).map_err(|error| {
                LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!("file history id must be a canonical UUID: {error}"),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let file_ids = selected_file_id_filters(lookup_ids);
    let mut rows = scan_file_history_observed_rows(
        historical,
        observed_commit_id,
        StateFilter {
            schema_keys: vec![
                FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                BLOB_REF_SCHEMA_KEY.to_string(),
            ],
            row_pks,
            file_ids: file_ids.clone(),
            include_tombstones: true,
        },
    )
    .await?;
    let descriptors = parse_file_history_observed_descriptors(&rows)?;
    rows.append(
        load_file_history_ancestor_directory_rows(
            historical,
            observed_commit_id,
            &descriptors,
            file_ids.clone(),
        )
        .await?,
    )?;
    Ok(rows)
}

fn selected_file_id_filters(lookup_ids: &FileHistoryLookupIds) -> Vec<NullableKeyFilter<String>> {
    std::iter::once(NullableKeyFilter::Null)
        .chain(lookup_ids.0.iter().cloned().map(NullableKeyFilter::Value))
        .collect()
}

async fn load_file_history_ancestor_directory_rows<S>(
    historical: &ForkTreeReadFacade<S>,
    observed_commit_id: &SharedStr,
    descriptors: &[FileHistoryObservedDescriptorRecord],
    file_ids: Vec<NullableKeyFilter<String>>,
) -> Result<ObservedStateRows, LixError>
where
    S: StorageAdapterRead,
{
    let mut pending = descriptors
        .iter()
        .filter_map(|descriptor| descriptor.directory_id.clone())
        .collect::<BTreeSet<_>>();
    let mut requested = BTreeSet::new();
    let mut rows = ObservedStateRows::default();
    while !pending.is_empty() {
        let ids = std::mem::take(&mut pending)
            .into_iter()
            .filter(|id| requested.insert(id.clone()))
            .collect::<Vec<_>>();
        if ids.is_empty() {
            break;
        }
        let loaded = scan_file_history_observed_rows(
            historical,
            observed_commit_id,
            StateFilter {
                schema_keys: vec![DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string()],
                row_pks: ids
                    .iter()
                    .map(|directory_id| {
                        RowPk::uuid_from_canonical(directory_id).map_err(|error| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                format!(
                                    "file history directory ID is not a canonical UUID: {error}"
                                ),
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                file_ids: file_ids.clone(),
                include_tombstones: true,
            },
        )
        .await?;
        let directories = parse_file_history_observed_directories(&loaded)?;
        pending.extend(
            directories
                .iter()
                .filter_map(|directory| directory.parent_id.clone())
                .filter(|id| !requested.contains(id)),
        );
        rows.append(loaded)?;
    }
    Ok(rows)
}

async fn scan_file_history_observed_rows<S>(
    historical: &ForkTreeReadFacade<S>,
    observed_commit_id: &SharedStr,
    filter: StateFilter,
) -> Result<ObservedStateRows, LixError>
where
    S: StorageAdapterRead,
{
    let commit_id = CommitId::parse_lix(observed_commit_id, "file history observed commit")?;
    let rows = if !filter.schema_keys.is_empty()
        && !filter.row_pks.is_empty()
        && let Some(file_ids) = exact_file_id_values(&filter.file_ids)
    {
        let keys = filter
            .schema_keys
            .iter()
            .flat_map(|schema_key| {
                filter.row_pks.iter().flat_map(|row_pk| {
                    file_ids.iter().map(|file_id| StateKey {
                        schema_key: schema_key.clone(),
                        file_id: file_id.clone(),
                        row_pk: row_pk.clone(),
                    })
                })
            })
            .collect::<Vec<_>>();
        historical
            .load_state_rows_at_commit(observed_commit_id, &keys)
            .await?
            .into_iter()
            .flatten()
            .collect()
    } else if !filter.schema_keys.is_empty() && !filter.row_pks.is_empty() {
        let mut rows = Vec::new();
        for schema_key in &filter.schema_keys {
            for row_pk in &filter.row_pks {
                let bounds = encode_state_row_prefix_bounds(schema_key, row_pk);
                rows.extend(
                    historical
                        .scan_state_rows_at_commit_range(
                            commit_id,
                            &bounds.lower,
                            bounds.upper.as_deref(),
                        )
                        .await?,
                );
            }
        }
        rows
    } else {
        historical.scan_state_rows_at_commit(commit_id).await?
    };
    let rows = rows
        .into_iter()
        .filter(|row| {
            (filter.schema_keys.is_empty() || filter.schema_keys.contains(&row.key.schema_key))
                && (filter.row_pks.is_empty() || filter.row_pks.contains(&row.key.row_pk))
                && (filter.file_ids.is_empty()
                    || filter.file_ids.iter().any(|file_id| match file_id {
                        NullableKeyFilter::Any => true,
                        NullableKeyFilter::Null => row.key.file_id.is_none(),
                        NullableKeyFilter::Value(file_id) => {
                            row.key.file_id.as_deref() == Some(file_id.as_str())
                        }
                    }))
                && (filter.include_tombstones || !row.deleted)
        })
        .collect();
    ObservedStateRows::from_rows(observed_commit_id.clone(), rows)
}

fn exact_file_id_values(file_ids: &[NullableKeyFilter<String>]) -> Option<Vec<Option<String>>> {
    if file_ids.is_empty() {
        return None;
    }
    let mut values = Vec::with_capacity(file_ids.len());
    for file_id in file_ids {
        match file_id {
            NullableKeyFilter::Any => return None,
            NullableKeyFilter::Null => values.push(None),
            NullableKeyFilter::Value(file_id) => values.push(Some(file_id.clone())),
        }
    }
    Some(values)
}

async fn load_plugin_registry_at_observed_commit<S>(
    historical: &ForkTreeReadFacade<S>,
    observed_commit_id: &SharedStr,
) -> Result<PluginRegistry, LixError>
where
    S: StorageAdapterRead,
{
    let rows = scan_file_history_observed_rows(
        historical,
        observed_commit_id,
        StateFilter {
            schema_keys: vec![KEY_VALUE_SCHEMA_KEY.to_string()],
            row_pks: vec![RowPk::single(PLUGIN_REGISTRY_KEY)],
            file_ids: vec![NullableKeyFilter::Null],
            include_tombstones: true,
        },
    )
    .await?;
    let observed = rows.iter().next().ok_or_else(|| {
        invalid_file_history_state(format!(
            "lix_file_history plugin registry row is missing at observed commit '{observed_commit_id}'"
        ))
    })?;
    if rows.iter().nth(1).is_some() {
        return Err(invalid_file_history_state(format!(
            "lix_file_history plugin registry has duplicate authenticated rows at observed commit '{observed_commit_id}'"
        )));
    }
    let row = observed.row();
    if row.deleted() {
        return Err(invalid_file_history_state(format!(
            "lix_file_history plugin registry row is deleted at observed commit '{observed_commit_id}'"
        )));
    }
    let snapshot = row.snapshot_content().ok_or_else(|| {
        invalid_file_history_state(format!(
            "lix_file_history plugin registry row is not an authenticated value at observed commit '{observed_commit_id}'"
        ))
    })?;
    let snapshot = serde_json::from_str::<serde_json::Value>(snapshot).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "lix_file_history plugin registry snapshot is invalid JSON at observed commit '{observed_commit_id}': {error}"
            ),
        )
    })?;
    PluginRegistry::from_optional_snapshot(Some(&snapshot))
}

async fn load_file_history_filesystem_entries<S>(
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlChangelogQuerySource<S>,
    route: &HistoryRoute,
    lookup_ids: Option<&FileHistoryLookupIds>,
    metadata_projection: HistoryMetadataProjection,
    directory_seed_ids: Option<&BTreeSet<String>>,
) -> Result<Vec<HistoryEntry>, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    let Some(lookup_ids) = lookup_ids else {
        return load_history_entries(
            HistoryViewDescriptor {
                view_name: "lix_file_history",
                as_of_commit_column: HISTORY_COL_AS_OF_COMMIT_ID,
            },
            commit_graph,
            query_source,
            route,
            file_history_filesystem_schema_keys(),
            metadata_projection,
        )
        .await;
    };

    let descriptor_and_blob_route = file_history_descriptor_blob_route(route, lookup_ids)?;
    let mut entries = load_history_entries(
        HistoryViewDescriptor {
            view_name: "lix_file_history",
            as_of_commit_column: HISTORY_COL_AS_OF_COMMIT_ID,
        },
        Arc::clone(&commit_graph),
        query_source.clone(),
        &descriptor_and_blob_route,
        vec![
            FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
            BLOB_REF_SCHEMA_KEY.to_string(),
        ],
        metadata_projection,
    )
    .await?;
    // Directory changes can rename or move a selected file, but only its
    // authenticated descriptor ancestors can affect that file. Derive those
    // directory IDs from the selected descriptor history and walk their
    // authenticated parent chain. The old route scanned every directory in
    // every reachable commit, which violated the point-history bound.
    let mut pending_directory_ids = match directory_seed_ids {
        Some(directory_seed_ids) => directory_seed_ids.clone(),
        None => directory_ids_from_file_history_descriptors(&entries)?,
    };
    let mut requested_directory_ids = BTreeSet::new();
    while !pending_directory_ids.is_empty() {
        let directory_ids = std::mem::take(&mut pending_directory_ids)
            .into_iter()
            .filter(|directory_id| requested_directory_ids.insert(directory_id.clone()))
            .collect::<Vec<_>>();
        if directory_ids.is_empty() {
            break;
        }
        let directory_route = file_history_directory_route(route, &directory_ids)?;
        let directories = load_history_entries(
            HistoryViewDescriptor {
                view_name: "lix_file_history",
                as_of_commit_column: HISTORY_COL_AS_OF_COMMIT_ID,
            },
            Arc::clone(&commit_graph),
            query_source.clone(),
            &directory_route,
            vec![DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string()],
            metadata_projection,
        )
        .await?;
        pending_directory_ids.extend(
            parse_file_history_directories(&directories)?
                .into_iter()
                .filter_map(|directory| directory.parent_id),
        );
        entries.extend(directories);
    }
    Ok(entries)
}

fn directory_ids_from_file_history_descriptors(
    entries: &[HistoryEntry],
) -> Result<BTreeSet<String>, LixError> {
    let mut directory_ids = BTreeSet::new();
    for descriptor in parse_file_history_descriptors(entries)? {
        let Some(snapshot) = descriptor.entry.change.snapshot_content.as_deref() else {
            continue;
        };
        let snapshot =
            serde_json::from_str::<FileDescriptorSnapshot>(snapshot).map_err(|error| {
                invalid_file_history_state(format!(
                    "invalid selected file descriptor history snapshot JSON: {error}"
                ))
            })?;
        if let Some(directory_id) = snapshot.directory_id {
            directory_ids.insert(directory_id);
        }
    }
    Ok(directory_ids)
}

fn file_history_directory_route(
    route: &HistoryRoute,
    directory_ids: &[String],
) -> Result<HistoryRoute, LixError> {
    let mut route = route.clone();
    route.file_ids.clear();
    route.row_pks = directory_ids
        .iter()
        .map(|directory_id| {
            let row_pk = RowPk::uuid_from_canonical(directory_id).map_err(|error| {
                LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!("file history directory ID must be a canonical UUID: {error}"),
                )
            })?;
            row_pk.as_json_array_text()
        })
        .collect::<Result<Vec<_>, _>>()?;
    route.resolved_row_pks = directory_ids
        .iter()
        .map(|directory_id| {
            RowPk::uuid_from_canonical(directory_id).map_err(|error| {
                LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!("file history directory ID must be a canonical UUID: {error}"),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(route)
}

fn file_history_descriptor_blob_route(
    route: &HistoryRoute,
    lookup_ids: &FileHistoryLookupIds,
) -> Result<HistoryRoute, LixError> {
    let mut route = route.clone();
    route.file_ids = lookup_ids.0.iter().cloned().collect();
    route.row_pks = lookup_ids.row_pks()?;
    route.resolved_row_pks = lookup_ids
        .0
        .iter()
        .map(|file_id| {
            RowPk::uuid_from_canonical(file_id).map_err(|error| {
                LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!("file history id must be a canonical UUID: {error}"),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(route)
}

async fn load_file_history_plugin_state<S>(
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlChangelogQuerySource<S>,
    event_route: &HistoryRoute,
    context_route: &HistoryRoute,
    plugin_schema_keys: Vec<String>,
    lookup_ids: Option<&FileHistoryLookupIds>,
    metadata_projection: HistoryMetadataProjection,
) -> Result<
    (
        Vec<FileHistoryPluginStateRecord>,
        Vec<FileHistoryPluginStateRecord>,
    ),
    LixError,
>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    let event_route = file_history_plugin_route(event_route, lookup_ids);
    let context_route = file_history_plugin_route(context_route, lookup_ids);
    let (event_entries, context_entries) =
        load_file_history_entry_sets(&event_route, &context_route, move |route| {
            let commit_graph = Arc::clone(&commit_graph);
            let query_source = query_source.clone();
            let schema_keys = plugin_schema_keys.clone();
            async move {
                load_history_entries(
                    HistoryViewDescriptor {
                        view_name: "lix_file_history",
                        as_of_commit_column: HISTORY_COL_AS_OF_COMMIT_ID,
                    },
                    commit_graph,
                    query_source,
                    &route,
                    schema_keys,
                    metadata_projection,
                )
                .await
            }
        })
        .await?;
    Ok((
        parse_file_history_plugin_state(&event_entries)?,
        parse_file_history_plugin_state(&context_entries)?,
    ))
}

async fn load_file_history_plugin_owner_events<S>(
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlChangelogQuerySource<S>,
    event_route: &HistoryRoute,
    lookup_ids: Option<&FileHistoryLookupIds>,
    metadata_projection: HistoryMetadataProjection,
) -> Result<Vec<FileHistoryPluginOwnerRecord>, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    let mut owner_route = file_history_plugin_route(event_route, lookup_ids);
    let owner_pk = RowPk::single(PLUGIN_OWNER_KEY);
    owner_route.row_pks = vec![owner_pk.as_json_array_text()?];
    owner_route.resolved_row_pks = vec![owner_pk];
    let entries = load_history_entries(
        HistoryViewDescriptor {
            view_name: "lix_file_history",
            as_of_commit_column: HISTORY_COL_AS_OF_COMMIT_ID,
        },
        commit_graph,
        query_source,
        &owner_route,
        vec![KEY_VALUE_SCHEMA_KEY.to_string()],
        metadata_projection,
    )
    .await?;
    parse_file_history_plugin_owners(&entries)
}

fn file_history_plugin_route(
    route: &HistoryRoute,
    lookup_ids: Option<&FileHistoryLookupIds>,
) -> HistoryRoute {
    let mut route = route.clone();
    if let Some(lookup_ids) = lookup_ids {
        route.file_ids = lookup_ids.0.iter().cloned().collect();
    }
    route
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
    observed_states: &BTreeMap<String, Arc<FileHistoryObservedState>>,
    parent_commit_ids_by_commit: &BTreeMap<String, Vec<String>>,
) -> Result<Vec<FileHistoryEvent>, LixError> {
    let mut descriptor_ids_by_as_of = BTreeSet::<(String, String)>::new();

    for descriptor in context_descriptors {
        let key = (
            descriptor.id.clone(),
            descriptor.entry.as_of_commit_id.clone(),
        );
        descriptor_ids_by_as_of.insert(key);
    }

    let mut candidates = Vec::new();
    let directory_indexes = observed_states
        .iter()
        .map(|(commit_id, state)| {
            (
                commit_id.as_str(),
                FileHistoryDirectoryIndex::from_state(state),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for descriptor in event_descriptors {
        candidates.push(file_history_event_from_entry(
            descriptor.id.clone(),
            &descriptor.entry,
        ));
    }
    for directory in event_directories {
        let state_commit_ids = std::iter::once(directory.entry.observed_commit_id.as_str()).chain(
            parent_commit_ids_by_commit
                .get(&directory.entry.observed_commit_id)
                .into_iter()
                .flatten()
                .map(String::as_str),
        );
        let mut affected_file_ids = BTreeSet::new();
        for state_commit_id in state_commit_ids {
            if let Some(directory_index) = directory_indexes.get(state_commit_id) {
                affected_file_ids.extend(directory_index.affected_file_ids(&directory.id)?);
            }
        }
        for file_id in affected_file_ids {
            candidates.push(file_history_event_from_entry(file_id, &directory.entry));
        }
    }
    for blob in event_blobs {
        if descriptor_ids_by_as_of
            .contains(&(blob.file_id.clone(), blob.entry.as_of_commit_id.clone()))
        {
            candidates.push(file_history_event_from_entry(
                blob.file_id.clone(),
                &blob.entry,
            ));
        }
    }
    sorted_grouped_file_history_events(candidates)
}

fn sorted_grouped_file_history_events<I>(events: I) -> Result<Vec<FileHistoryEvent>, LixError>
where
    I: IntoIterator<Item = FileHistoryEvent>,
{
    let mut grouped = BTreeMap::<(String, String, String), FileHistoryEvent>::new();
    for mut event in events {
        let key = (
            event.file_id.clone(),
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
    let mut events = grouped.into_values().collect::<Vec<_>>();
    for event in &mut events {
        let validate_source_changes: Result<(), LixError> = (|| {
            let mut source_changes_by_id = BTreeMap::<String, MaterializedChange>::new();
            for change in &event.source_changes {
                if let Some(previous) = source_changes_by_id.get(&change.id) {
                    if previous != change {
                        return Err(invalid_file_history_state(format!(
                            "file '{}' at observed commit '{}' has conflicting source-change ID '{}'",
                            event.file_id, event.observed_commit_id, change.id
                        )));
                    }
                } else {
                    source_changes_by_id.insert(change.id.clone(), change.clone());
                }
            }
            Ok(())
        })();
        validate_source_changes?;
        event
            .source_changes
            .sort_by(|left, right| left.id.cmp(&right.id));
        event
            .source_changes
            .dedup_by(|left, right| left.id == right.id);
    }
    events.sort_by(|left, right| {
        left.file_id
            .cmp(&right.file_id)
            .then(left.as_of_commit_id.cmp(&right.as_of_commit_id))
            .then(left.depth.cmp(&right.depth))
            .then(left.observed_commit_id.cmp(&right.observed_commit_id))
    });
    Ok(events)
}

async fn discover_file_history_plugins<S>(
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlChangelogQuerySource<S>,
    historical: &ForkTreeReadFacade<S>,
    event_route: &HistoryRoute,
    parent_commit_ids_by_commit: &BTreeMap<String, Vec<String>>,
    metadata_projection: HistoryMetadataProjection,
) -> Result<FileHistoryPluginDiscovery, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    // The durable registry snapshot is already the complete plugin set at its
    // observed commit. Read that exact root identity for every reachable commit
    // instead of inventing a filesystem state from `(anchor, depth)`, which
    // conflates equal-depth siblings in a DAG.
    let observed_commit_ids = parent_commit_ids_by_commit
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut schema_keys = BTreeSet::new();
    let mut registries_by_commit = BTreeMap::new();
    for observed_commit_id in observed_commit_ids {
        let shared_observed_commit: SharedStr = observed_commit_id.as_str().into();
        let registry =
            load_plugin_registry_at_observed_commit(historical, &shared_observed_commit).await?;
        schema_keys.extend(
            registry
                .plugins()
                .iter()
                .flat_map(|plugin| plugin.schema_keys().iter().cloned()),
        );
        registries_by_commit.insert(observed_commit_id, registry);
    }

    let mut registry_route = event_route.clone();
    let registry_pk = RowPk::single(PLUGIN_REGISTRY_KEY);
    registry_route.row_pks = vec![registry_pk.as_json_array_text()?];
    registry_route.resolved_row_pks = vec![registry_pk];
    let registry_events = load_history_entries(
        HistoryViewDescriptor {
            view_name: "lix_file_history",
            as_of_commit_column: HISTORY_COL_AS_OF_COMMIT_ID,
        },
        commit_graph,
        query_source,
        &registry_route,
        vec![KEY_VALUE_SCHEMA_KEY.to_string()],
        metadata_projection,
    )
    .await?;

    Ok(FileHistoryPluginDiscovery {
        schema_keys: schema_keys.into_iter().collect(),
        registries_by_commit,
        parent_commit_ids_by_commit: parent_commit_ids_by_commit.clone(),
        registry_events,
    })
}

fn file_history_plugin_events(
    event_plugin_state: &[FileHistoryPluginStateRecord],
    event_plugin_owners: &[FileHistoryPluginOwnerRecord],
    observed_states: &BTreeMap<String, Arc<FileHistoryObservedState>>,
    parent_commit_ids_by_commit: &BTreeMap<String, Vec<String>>,
) -> Result<Vec<FileHistoryEvent>, LixError> {
    let owner_changes = event_plugin_owners
        .iter()
        .map(|record| {
            (
                record.entry.observed_commit_id.as_str(),
                record.file_id.as_str(),
            )
        })
        .collect::<BTreeSet<_>>();

    let mut events = Vec::new();
    for plugin_state in event_plugin_state {
        let observed_commit_id = plugin_state.entry.observed_commit_id.as_str();
        let schema_key = plugin_state.entry.change.schema_key.as_str();
        let live_owner_matches = observed_states
            .get(observed_commit_id)
            .and_then(|state| {
                live_file_history_plugin_owner(state, &plugin_state.file_id)
                    .map(|owner| (state, owner))
            })
            .map_or(Ok::<bool, LixError>(false), |(state, owner)| {
                Ok(file_history_owner_schema_keys(state, owner)?
                    .iter()
                    .any(|owner_schema_key| owner_schema_key == schema_key))
            })?;
        if live_owner_matches {
            events.push(file_history_event_from_entry(
                plugin_state.file_id.clone(),
                &plugin_state.entry,
            ));
            continue;
        }

        // A non-owner live row cannot change the public file projection.
        // A tombstone remains relevant only when this commit also changes
        // the durable owner and a direct parent proves that the schema was
        // part of the prior owner's rendering contract. This covers owner
        // deletion, A -> B replacement, and same-key contract updates.
        if plugin_state.entry.change.snapshot_content.is_some()
            || !owner_changes.contains(&(observed_commit_id, plugin_state.file_id.as_str()))
        {
            continue;
        }
        let prior_owner_matches = parent_commit_ids_by_commit
            .get(observed_commit_id)
            .into_iter()
            .flatten()
            .try_fold(
                false,
                |matched, parent_commit_id| -> Result<bool, LixError> {
                    if matched {
                        return Ok(true);
                    }
                    observed_states
                        .get(parent_commit_id)
                        .and_then(|state| {
                            live_file_history_plugin_owner(state, &plugin_state.file_id)
                                .map(|owner| (state, owner))
                        })
                        .map_or(Ok(false), |(state, owner)| {
                            Ok(file_history_owner_schema_keys(state, owner)?
                                .iter()
                                .any(|owner_schema_key| owner_schema_key == schema_key))
                        })
                },
            )?;
        if prior_owner_matches {
            events.push(file_history_event_from_entry(
                plugin_state.file_id.clone(),
                &plugin_state.entry,
            ));
        }
    }
    Ok(events)
}

fn live_file_history_plugin_owner<'a>(
    state: &'a FileHistoryObservedState,
    file_id: &str,
) -> Option<&'a PluginFileOwner> {
    state
        .plugin_owners
        .iter()
        .find(|record| record.file_id == file_id)
        .and_then(|record| {
            let _ = state.rows.row(record.row);
            record.owner.as_ref()
        })
}

fn file_history_plugin_registry_events(
    registry_events: &[HistoryEntry],
    observed_states: &BTreeMap<String, Arc<FileHistoryObservedState>>,
    registries_by_commit: &BTreeMap<String, PluginRegistry>,
    parent_commit_ids_by_commit: &BTreeMap<String, Vec<String>>,
) -> Result<Vec<FileHistoryEvent>, LixError> {
    let mut events = Vec::new();
    for registry_event in registry_events {
        let observed_commit_id = &registry_event.observed_commit_id;
        let state = observed_states.get(observed_commit_id).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("lix_file_history did not load observed commit '{observed_commit_id}'"),
            )
        })?;
        let registry = registries_by_commit.get(observed_commit_id).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "lix_file_history did not load plugin registry at observed commit '{observed_commit_id}'"
                ),
            )
        })?;
        let parent_commit_ids = parent_commit_ids_by_commit
            .get(observed_commit_id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        for owner_record in state
            .plugin_owners
            .iter()
            .filter(|record| record.owner.is_some())
        {
            let owner = owner_record
                .owner
                .as_ref()
                .expect("filtered plugin owner should exist");
            let current_entry = registry.get(owner.plugin_key());
            let owner_contract_changed = parent_commit_ids.iter().any(|parent_commit_id| {
                registries_by_commit
                    .get(parent_commit_id)
                    .and_then(|parent| parent.get(owner.plugin_key()))
                    != current_entry
            });
            if owner_contract_changed {
                events.push(file_history_event_from_entry(
                    owner_record.file_id.clone(),
                    registry_event,
                ));
            }
        }
    }
    Ok(events)
}

fn file_history_event_from_entry(file_id: String, entry: &HistoryEntry) -> FileHistoryEvent {
    FileHistoryEvent {
        file_id,
        as_of_commit_id: entry.as_of_commit_id.clone(),
        depth: entry.depth,
        source_changes: vec![entry.change.clone()],
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
            let row_id = entry.change.row_pk.as_single_string_owned()?;
            if entry.change.file_id.as_deref() != Some(row_id.as_str()) {
                return Err(invalid_file_history_state(format!(
                    "file descriptor row file_id does not match authenticated row key '{}'",
                    row_id
                )));
            }
            if entry.change.snapshot_content.is_none() {
                return Ok(FileHistoryDescriptorRecord {
                    id: row_id,
                    entry: entry.clone(),
                });
            }
            let snapshot_content = entry.change.snapshot_content.as_deref().ok_or_else(|| {
                invalid_file_history_state(format!(
                    "file descriptor history row '{row_id}' has no authenticated payload"
                ))
            })?;
            let snapshot: FileDescriptorSnapshot = serde_json::from_str(snapshot_content)
                .map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid lix_file_descriptor history snapshot JSON: {error}"),
                    )
                })?;
            if snapshot.id != row_id {
                return Err(invalid_file_history_state(format!(
                    "file descriptor payload identity '{}' does not match authenticated row key '{}'",
                    snapshot.id, row_id
                )));
            }
            Ok(FileHistoryDescriptorRecord {
                id: row_id,
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
        .map(|entry| {
            let row_id = entry.change.row_pk.as_single_string_owned()?;
            if entry.change.file_id.is_some() {
                return Err(invalid_file_history_state(format!(
                    "directory descriptor row '{}' has a non-NULL file_id",
                    row_id
                )));
            }
            if entry.change.snapshot_content.is_none() {
                return Ok(FileHistoryDirectoryRecord {
                    id: row_id,
                    parent_id: None,
                    name: None,
                    entry: entry.clone(),
                });
            }
            let snapshot_content = entry.change.snapshot_content.as_deref().ok_or_else(|| {
                invalid_file_history_state(format!(
                    "directory descriptor history row '{row_id}' has no authenticated payload"
                ))
            })?;
            let snapshot: DirectoryDescriptorSnapshot = serde_json::from_str(snapshot_content)
                .map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid lix_directory_descriptor history snapshot JSON: {error}"),
                    )
                })?;
            if snapshot.id != row_id {
                return Err(invalid_file_history_state(format!(
                    "directory descriptor payload identity '{}' does not match authenticated row key '{}'",
                    snapshot.id, row_id
                )));
            }
            Ok(FileHistoryDirectoryRecord {
                id: row_id,
                parent_id: snapshot.parent_id,
                name: Some(snapshot.name),
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
            let row_id = entry.change.row_pk.as_single_string_owned()?;
            let file_id = entry.change.file_id.clone().ok_or_else(|| {
                invalid_file_history_state(format!(
                    "blob reference history row '{row_id}' is missing file_id"
                ))
            })?;
            if entry.change.snapshot_content.is_none() {
                if file_id != row_id {
                    return Err(invalid_file_history_state(format!(
                        "blob reference tombstone file identity '{}' does not match authenticated row key '{}'",
                        file_id, row_id
                    )));
                }
                return Ok(FileHistoryBlobRecord {
                    file_id,
                    entry: entry.clone(),
                });
            }
            let snapshot_content = entry.change.snapshot_content.as_deref().ok_or_else(|| {
                invalid_file_history_state(format!(
                    "blob reference history row '{row_id}' has no authenticated payload"
                ))
            })?;
            let snapshot: BlobRefSnapshot = serde_json::from_str(snapshot_content)
                .map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid lix_binary_blob_ref history snapshot JSON: {error}"),
                    )
                })?;
            BlobId::from_hex(&snapshot.blob_hash).map_err(|error| {
                invalid_file_history_state(format!(
                    "blob reference history row '{row_id}' has an invalid BlobId: {error}"
                ))
            })?;
            if snapshot.id != row_id
                || file_id != snapshot.id
            {
                return Err(invalid_file_history_state(format!(
                    "blob reference payload identity '{}' does not match authenticated row key '{}'",
                    snapshot.id, row_id
                )));
            }
            Ok(FileHistoryBlobRecord {
                file_id,
                entry: entry.clone(),
            })
        })
        .collect()
}

fn parse_file_history_plugin_state(
    entries: &[HistoryEntry],
) -> Result<Vec<FileHistoryPluginStateRecord>, LixError> {
    entries
        .iter()
        .filter(|entry| {
            !matches!(
                entry.change.schema_key.as_str(),
                FILE_DESCRIPTOR_SCHEMA_KEY | DIRECTORY_DESCRIPTOR_SCHEMA_KEY | BLOB_REF_SCHEMA_KEY
            )
        })
        .map(|entry| {
            let file_id = entry.change.file_id.clone().ok_or_else(|| {
                invalid_file_history_state(format!(
                    "plugin history row at commit '{}' is missing file_id",
                    entry.observed_commit_id
                ))
            })?;
            Ok(FileHistoryPluginStateRecord {
                file_id,
                entry: entry.clone(),
            })
        })
        .collect()
}

fn parse_file_history_plugin_owners(
    entries: &[HistoryEntry],
) -> Result<Vec<FileHistoryPluginOwnerRecord>, LixError> {
    entries
        .iter()
        .filter(|entry| {
            entry.change.schema_key == KEY_VALUE_SCHEMA_KEY
                && entry.change.row_pk.as_single_string().ok() == Some(PLUGIN_OWNER_KEY)
        })
        .map(|entry| {
            let file_id = entry.change.file_id.clone().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "lix_file_history plugin owner row is missing file_id",
                )
            })?;
            Ok(FileHistoryPluginOwnerRecord {
                file_id,
                entry: entry.clone(),
            })
        })
        .collect()
}

fn parse_file_history_observed_descriptors(
    rows: &ObservedStateRows,
) -> Result<Vec<FileHistoryObservedDescriptorRecord>, LixError> {
    rows.iter()
        .filter(|observed| observed.row().schema_key() == FILE_DESCRIPTOR_SCHEMA_KEY)
        .map(|observed| {
            let _ = observed.observed_commit_id();
            let row = observed.row();
            let row_id = row.row_pk().as_single_string_owned()?;
            if row.file_id().as_deref() != Some(row_id.as_str()) {
                return Err(invalid_file_history_state(format!(
                    "observed file descriptor row file_id does not match authenticated row key '{}'",
                    row_id
                )));
            }
            if row.deleted() && row.snapshot_content().is_some() {
                return Err(invalid_file_history_state(format!(
                    "observed file descriptor row '{}' tombstone has a payload",
                    row_id
                )));
            }
            let Some(snapshot_content) = row.snapshot_content() else {
                if row.deleted() {
                    return Ok(FileHistoryObservedDescriptorRecord {
                        id: row_id,
                        directory_id: None,
                        name: None,
                        row: observed.ordinal(),
                    });
                }
                return Err(invalid_file_history_state(format!(
                    "file descriptor row '{row_id}' has no authenticated payload"
                )));
            };
            let snapshot: FileDescriptorSnapshot =
                serde_json::from_str(snapshot_content).map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid lix_file_descriptor history snapshot JSON: {error}"),
                    )
                })?;
            if snapshot.id != row_id {
                return Err(invalid_file_history_state(format!(
                    "observed file descriptor payload identity '{}' does not match authenticated row key '{}'",
                    snapshot.id, row_id
                )));
            }
            Ok(FileHistoryObservedDescriptorRecord {
                id: row_id,
                directory_id: snapshot.directory_id,
                name: Some(snapshot.name),
                row: observed.ordinal(),
            })
        })
        .collect()
}

fn parse_file_history_observed_directories(
    rows: &ObservedStateRows,
) -> Result<Vec<FileHistoryObservedDirectoryRecord>, LixError> {
    rows.iter()
        .filter(|observed| observed.row().schema_key() == DIRECTORY_DESCRIPTOR_SCHEMA_KEY)
        .map(|observed| {
            let row = observed.row();
            let row_id = row.row_pk().as_single_string_owned()?;
            if row.file_id().is_some() {
                return Err(invalid_file_history_state(format!(
                    "observed directory descriptor row '{}' has a non-NULL file_id",
                    row_id
                )));
            }
            if row.deleted() && row.snapshot_content().is_some() {
                return Err(invalid_file_history_state(format!(
                    "observed directory descriptor row '{}' tombstone has a payload",
                    row_id
                )));
            }
            let Some(snapshot_content) = row.snapshot_content() else {
                if row.deleted() {
                    return Ok(FileHistoryObservedDirectoryRecord {
                        id: row_id,
                        parent_id: None,
                        name: None,
                        row: observed.ordinal(),
                    });
                }
                return Err(invalid_file_history_state(format!(
                    "directory descriptor row '{row_id}' has no authenticated payload"
                )));
            };
            let snapshot: DirectoryDescriptorSnapshot = serde_json::from_str(snapshot_content)
                .map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid lix_directory_descriptor history snapshot JSON: {error}"),
                    )
                })?;
            if snapshot.id != row_id {
                return Err(invalid_file_history_state(format!(
                    "observed directory descriptor payload identity '{}' does not match authenticated row key '{}'",
                    snapshot.id, row_id
                )));
            }
            Ok(FileHistoryObservedDirectoryRecord {
                id: row_id,
                parent_id: snapshot.parent_id,
                name: Some(snapshot.name),
                row: observed.ordinal(),
            })
        })
        .collect()
}

fn parse_file_history_observed_blobs(
    rows: &ObservedStateRows,
) -> Result<Vec<FileHistoryObservedBlobRecord>, LixError> {
    rows.iter()
        .filter(|observed| observed.row().schema_key() == BLOB_REF_SCHEMA_KEY)
        .map(|observed| {
            let row = observed.row();
            let row_id = row.row_pk().as_single_string_owned()?;
            let file_id = row.file_id().map(str::to_owned).ok_or_else(|| {
                invalid_file_history_state(format!(
                    "observed blob reference row '{row_id}' is missing file_id"
                ))
            })?;
            if file_id != row_id {
                return Err(invalid_file_history_state(format!(
                    "observed blob reference file identity '{}' does not match authenticated row key '{}'",
                    file_id, row_id
                )));
            }
            let Some(snapshot_content) = row.snapshot_content() else {
                if row.deleted() {
                    return Ok(FileHistoryObservedBlobRecord {
                        file_id,
                        blob_hash: None,
                        size_bytes: None,
                        deleted: true,
                        row: observed.ordinal(),
                    });
                }
                return Err(invalid_file_history_state(format!(
                    "blob reference row '{row_id}' has no authenticated payload"
                )));
            };
            let snapshot: BlobRefSnapshot =
                serde_json::from_str(snapshot_content).map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid lix_binary_blob_ref history snapshot JSON: {error}"),
                    )
                })?;
            BlobId::from_hex(&snapshot.blob_hash).map_err(|error| {
                invalid_file_history_state(format!(
                    "observed blob reference row '{row_id}' has an invalid BlobId: {error}"
                ))
            })?;
            if row.deleted() || snapshot.id != row_id || file_id != snapshot.id {
                return Err(invalid_file_history_state(format!(
                    "observed blob reference payload identity '{}' does not match authenticated row key '{}'",
                    snapshot.id, row_id
                )));
            }
            Ok(FileHistoryObservedBlobRecord {
                file_id,
                blob_hash: Some(snapshot.blob_hash),
                size_bytes: Some(snapshot.size_bytes),
                deleted: false,
                row: observed.ordinal(),
            })
        })
        .collect()
}

fn parse_file_history_observed_plugin_owners(
    rows: &ObservedStateRows,
) -> Result<Vec<FileHistoryObservedPluginOwnerRecord>, LixError> {
    rows.iter()
        .filter(|observed| {
            let row = observed.row();
            row.schema_key() == KEY_VALUE_SCHEMA_KEY
                && row.row_pk().as_single_string().ok() == Some(PLUGIN_OWNER_KEY)
        })
        .map(|observed| {
            let row = observed.row();
            let file_id = row.file_id().map(str::to_owned).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "lix_file_history plugin owner row is missing file_id",
                )
            })?;
            if row.deleted() && row.snapshot_content().is_some() {
                return Err(invalid_file_history_state(format!(
                    "observed plugin owner row for file '{}' tombstone has a payload",
                    file_id
                )));
            }
            if row.deleted() {
                return Ok(FileHistoryObservedPluginOwnerRecord {
                    file_id,
                    owner: None,
                    row: observed.ordinal(),
                });
            }
            let snapshot = row.snapshot_content().ok_or_else(|| {
                invalid_file_history_state(format!(
                    "plugin owner row for file '{file_id}' has no authenticated payload"
                ))
            })?;
            let owner = serde_json::from_str::<serde_json::Value>(snapshot)
                .map_err(|error| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "lix_file_history plugin owner snapshot is invalid JSON for file '{file_id}': {error}"
                        ),
                    )
                })
                .and_then(|snapshot| PluginFileOwner::from_snapshot(&file_id, &snapshot))?;
            Ok(FileHistoryObservedPluginOwnerRecord {
                file_id,
                owner: Some(owner),
                row: observed.ordinal(),
            })
        })
        .collect()
}

fn file_history_event_affects_observed_file(
    event: &FileHistoryEvent,
    descriptor: &FileHistoryObservedDescriptorRecord,
    directory_tree: &HistoryDirectoryTree,
) -> Result<bool, LixError> {
    event
        .source_changes
        .iter()
        .try_fold(false, |matched, change| {
            if matched {
                return Ok(true);
            }
            let matched = match change.schema_key.as_str() {
                FILE_DESCRIPTOR_SCHEMA_KEY | BLOB_REF_SCHEMA_KEY => {
                    change
                        .file_id
                        .as_deref()
                        .is_some_and(|file_id| file_id == descriptor.id)
                        || change
                            .row_pk
                            .as_single_string_owned()
                            .is_ok_and(|row_id| row_id == descriptor.id)
                }
                DIRECTORY_DESCRIPTOR_SCHEMA_KEY => {
                    let Ok(changed_directory_id) = change.row_pk.as_single_string_owned() else {
                        return Ok(false);
                    };
                    let Some(directory_id) = descriptor.directory_id.as_deref() else {
                        return Ok(false);
                    };
                    directory_tree.has_ancestor_including(directory_id, &changed_directory_id)?
                }
                KEY_VALUE_SCHEMA_KEY
                    if change.row_pk.as_single_string().ok() == Some(PLUGIN_REGISTRY_KEY) =>
                {
                    event.file_id == descriptor.id
                }
                _ => change
                    .file_id
                    .as_deref()
                    .is_some_and(|file_id| file_id == descriptor.id),
            };
            Ok(matched)
        })
}

fn resolve_observed_file_history_path(
    descriptor: &FileHistoryObservedDescriptorRecord,
    directories: &[FileHistoryObservedDirectoryRecord],
) -> Result<Option<String>, LixError> {
    let Some(name) = descriptor.name.as_ref() else {
        return Ok(None);
    };
    let Some(directory_id) = descriptor.directory_id.as_deref() else {
        return compose_file_path(None, name).map(Some);
    };
    let directory_path = resolve_observed_directory_path(
        directory_id,
        directories,
        &mut BTreeMap::new(),
        &mut BTreeSet::new(),
    )?
    .ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("directory '{directory_id}' has no authenticated path"),
        )
    })?;
    compose_file_path(Some(&directory_path), name).map(Some)
}

static LIX_FILE_HISTORY_COLS: ColumnTable<FileHistoryOutputRow> = ColumnTable {
    columns: &[
        ("id", Col::Utf8(|row| Some(row.id.as_str()))),
        ("path", Col::Utf8(|row| row.path.as_deref())),
        (
            "directory_id",
            Col::Utf8(|row| row.descriptor().directory_id.as_deref()),
        ),
        ("name", Col::Utf8(|row| row.descriptor().name.as_deref())),
        ("content", Col::Binary(|row| row.data.clone())),
        (
            HISTORY_COL_ROW_PK,
            Col::Utf8Fallible(|row| row_pk_json_array(&row.descriptor().id).map(Some)),
        ),
        (
            HISTORY_COL_SOURCE_CHANGES,
            Col::Utf8Fallible(|row| {
                serialize_history_source_changes(&row.event.source_changes, "lix_file_history")
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
        Field::new("content", DataType::LargeBinary, true),
        json_field(HISTORY_COL_ROW_PK, false),
        json_field(HISTORY_COL_SOURCE_CHANGES, false),
        Field::new(HISTORY_COL_OBSERVED_COMMIT_ID, DataType::Utf8, false),
        Field::new(HISTORY_COL_COMMIT_CREATED_AT, DataType::Utf8, false),
        Field::new(HISTORY_COL_AS_OF_COMMIT_ID, DataType::Utf8, false),
        Field::new(HISTORY_COL_DEPTH, DataType::Int64, false),
        Field::new(HISTORY_COL_IS_DELETED, DataType::Boolean, false),
    ]))
}

fn lix_error_to_datafusion_error(error: LixError) -> DataFusionError {
    crate::sql2::error::lix_error_to_datafusion_error(error)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion::common::{Column, ScalarValue};
    use datafusion::logical_expr::expr::InList;
    use datafusion::logical_expr::{BinaryExpr, Expr, Operator};

    use crate::binary_cas::BlobId;
    use crate::changelog::{ChangeId, CommitId};
    use crate::common::{LixTimestamp, SharedStr};
    use crate::row_pk::RowPk;
    use crate::forktree::{HistoricalStateRow, StateKey};
    use crate::plugin::{
        PluginFileOwner, PluginRegistryEntry, PluginRegistryEntryInput, PluginRuntime,
        plugin_storage_archive_file_id, plugin_storage_archive_path,
    };
    use crate::sql2::change_materialization::MaterializedChange;
    use crate::sql2::history_route::HistoryEntry;

    use super::ObservedStateRows;
    use super::{
        FileHistoryBlobRecord, FileHistoryDescriptorRecord, FileHistoryDirectoryIndex,
        FileHistoryDirectoryRecord, FileHistoryFilesystemContext, FileHistoryLookupIds,
        FileHistoryObservedState, FileHistoryPluginOwnerRecord, FileHistoryPluginStateRecord,
        FileHistoryPublicPredicate, HistoryRoute, PluginRegistry, PreparedFileHistoryRow,
        file_history_descriptor_blob_route, file_history_event_from_entry, file_history_events,
        file_history_plugin_events, load_file_history_entry_sets,
        parse_file_history_observed_blobs, parse_file_history_observed_descriptors,
        parse_file_history_observed_directories, parse_file_history_observed_plugin_owners,
        prepare_file_history_rows, sorted_grouped_file_history_events,
    };

    fn history_entry(file_id: &str, depth: u32, snapshot_content: Option<String>) -> HistoryEntry {
        HistoryEntry {
            change: MaterializedChange {
                id: format!("change-{file_id}-{depth}"),
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                row_pk: RowPk::single(file_id),
                schema_key: super::FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                file_id: Some(file_id.to_string()),
                snapshot_content: snapshot_content.map(Into::into),
                metadata: None,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                origin_key: None,
            },
            native_row: None,
            observed_commit_id: format!("commit-{depth}"),
            commit_created_at: Some("2026-01-01T00:00:00Z".to_string()),
            as_of_commit_id: "start".to_string(),
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
            entry: history_entry(file_id, depth, snapshot),
        }
    }

    fn observed_row(
        schema_key: &str,
        row_pk: RowPk,
        file_id: Option<&str>,
        deleted: bool,
        ordinal: u128,
    ) -> HistoricalStateRow {
        let timestamp =
            LixTimestamp::expect_parse("test historical row timestamp", "2026-01-01T00:00:00Z");
        HistoricalStateRow {
            key: StateKey {
                row_pk,
                schema_key: schema_key.to_string(),
                file_id: file_id.map(str::to_owned),
            },
            global: false,
            cell: if deleted {
                crate::forktree::StateCell::Tombstone
            } else {
                crate::forktree::StateCell::Null
            },
            metadata: None,
            deleted,
            blob_manifest_object_ids: Vec::new(),
            created_at: timestamp,
            updated_at: timestamp,
            change_id: ChangeId::new(uuid::Uuid::from_u128(ordinal)),
            commit_id: CommitId::new(uuid::Uuid::from_u128(1)),
        }
    }

    fn descriptor_in_directory(file_id: &str, directory_id: &str) -> FileHistoryDescriptorRecord {
        let mut descriptor = descriptor(file_id, Some("file.txt"), 0);
        descriptor.entry.change.snapshot_content = Some(
            serde_json::json!({
                "id": file_id,
                "directory_id": directory_id,
                "name": "file.txt",
            })
            .to_string()
            .into(),
        );
        descriptor
    }

    fn directory_record(directory_id: &str) -> FileHistoryDirectoryRecord {
        let mut entry = history_entry(directory_id, 0, None);
        entry.change.id = format!("change-{directory_id}");
        entry.change.schema_key = super::DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string();
        entry.change.file_id = None;
        entry.change.snapshot_content = Some(
            serde_json::json!({
                "id": directory_id,
                "parent_id": null,
                "name": directory_id,
            })
            .to_string()
            .into(),
        );
        FileHistoryDirectoryRecord {
            id: directory_id.to_string(),
            parent_id: None,
            name: Some(directory_id.to_string()),
            entry,
        }
    }

    fn blob_record(file_id: &str, hash: BlobId, depth: u32) -> FileHistoryBlobRecord {
        let mut entry = history_entry(file_id, depth, None);
        entry.change.id = format!("blob-{file_id}-{depth}");
        entry.change.schema_key = super::BLOB_REF_SCHEMA_KEY.to_string();
        entry.change.snapshot_content = Some(
            serde_json::json!({
                "id": file_id,
                "blob_hash": hash.to_hex(),
                "size_bytes": 0,
            })
            .to_string()
            .into(),
        );
        FileHistoryBlobRecord {
            file_id: file_id.to_string(),
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
            event_blobs: blobs,
            descriptors,
        }
    }

    fn observed_state_from_entries(
        entries: impl IntoIterator<Item = HistoryEntry>,
        plugin_registry: PluginRegistry,
    ) -> FileHistoryObservedState {
        let rows = entries
            .into_iter()
            .enumerate()
            .map(|(index, entry)| {
                let change = entry.change;
                let deleted = change.snapshot_content.is_none();
                let created_at =
                    LixTimestamp::expect_parse("test history created_at", &change.created_at);
                let updated_at =
                    LixTimestamp::expect_parse("test history updated_at", &change.created_at);
                HistoricalStateRow {
                    key: StateKey {
                        row_pk: change.row_pk,
                        schema_key: change.schema_key,
                        file_id: change.file_id,
                    },
                    global: false,
                    cell: change.snapshot_content.map_or(
                        crate::forktree::StateCell::Tombstone,
                        crate::forktree::StateCell::Value,
                    ),
                    metadata: change.metadata,
                    deleted,
                    blob_manifest_object_ids: Vec::new(),
                    created_at,
                    updated_at,
                    change_id: ChangeId::new(uuid::Uuid::from_u128(index as u128 + 1)),
                    commit_id: CommitId::new(uuid::Uuid::from_u128(1)),
                }
            })
            .collect::<Vec<_>>();
        let rows = ObservedStateRows::from_rows(SharedStr::from_static("commit-0"), rows)
            .expect("test observed rows");
        FileHistoryObservedState {
            descriptors: parse_file_history_observed_descriptors(&rows).expect("test descriptors"),
            directories: parse_file_history_observed_directories(&rows).expect("test directories"),
            blobs: parse_file_history_observed_blobs(&rows).expect("test blobs"),
            plugin_owners: parse_file_history_observed_plugin_owners(&rows)
                .expect("test plugin owners"),
            plugin_registry,
            rows,
        }
    }

    fn observed_states(
        context: &FileHistoryFilesystemContext,
    ) -> BTreeMap<String, Arc<FileHistoryObservedState>> {
        let entries = context
            .descriptors
            .iter()
            .map(|record| record.entry.clone())
            .chain(
                context
                    .event_directories
                    .iter()
                    .map(|record| record.entry.clone()),
            )
            .chain(
                context
                    .event_blobs
                    .iter()
                    .map(|record| record.entry.clone()),
            );
        BTreeMap::from([(
            "commit-0".to_string(),
            Arc::new(observed_state_from_entries(
                entries,
                PluginRegistry::empty(),
            )),
        )])
    }

    #[test]
    fn ten_thousand_observed_rows_retain_one_batch_and_one_commit_buffer() {
        const ROW_COUNT: usize = 10_000;
        let entries = (0..ROW_COUNT)
            .map(|index| history_entry(&format!("01920000-0000-7000-8000-{index:012x}"), 0, None));
        let state = observed_state_from_entries(entries, PluginRegistry::empty());

        assert_eq!(state.descriptors.len(), ROW_COUNT);
        assert_eq!(state.rows.iter().len(), ROW_COUNT);
        assert_eq!(state.rows.retained_batch_count(), 1);
        let commit_buffers = state.rows.observed_commit_buffer_identitys();
        assert_eq!(commit_buffers.len(), 1);
        let first_commit_ptr = state
            .rows
            .iter()
            .next()
            .expect("non-empty observed batch")
            .observed_commit_id()
            .as_ptr();
        assert!(state.rows.iter().all(|row| {
            row.observed_commit_id().as_ptr() == first_commit_ptr
                && row.observed_commit_id() == "commit-0"
        }));
    }

    fn plugin_owner_record(
        file_id: &str,
        owner: PluginFileOwner,
        observed_commit_id: &str,
    ) -> FileHistoryPluginOwnerRecord {
        let snapshot_content = owner
            .to_snapshot()
            .expect("test owner should serialize")
            .to_string();
        let mut entry = history_entry(file_id, 0, Some(snapshot_content));
        entry.observed_commit_id = observed_commit_id.to_string();
        entry.change.id = format!("owner-{file_id}-{observed_commit_id}");
        entry.change.row_pk = RowPk::single(super::PLUGIN_OWNER_KEY);
        entry.change.schema_key = super::KEY_VALUE_SCHEMA_KEY.to_string();
        FileHistoryPluginOwnerRecord {
            file_id: file_id.to_string(),
            entry,
        }
    }

    fn plugin_state_tombstone(
        file_id: &str,
        schema_key: &str,
        observed_commit_id: &str,
    ) -> FileHistoryPluginStateRecord {
        let mut entry = history_entry(file_id, 0, None);
        entry.observed_commit_id = observed_commit_id.to_string();
        entry.change.id = format!("plugin-state-{schema_key}-{observed_commit_id}");
        entry.change.row_pk = RowPk::single("plugin-state");
        entry.change.schema_key = schema_key.to_string();
        FileHistoryPluginStateRecord {
            file_id: file_id.to_string(),
            entry,
        }
    }

    fn plugin_observed_state(
        owner_record: FileHistoryPluginOwnerRecord,
    ) -> FileHistoryObservedState {
        observed_state_from_entries([owner_record.entry], PluginRegistry::empty())
    }

    fn plugin_registry(plugin_key: &str, schema_keys: &[&str]) -> PluginRegistry {
        let wasm = b"test wasm";
        let manifest_json = serde_json::json!({
            "entry": "plugin.wasm",
            "key": plugin_key,
            "match": { "path_glob": "*.plugin-test" },
            "schemas": ["schema/plugin.json"],
        })
        .to_string();
        let entry = PluginRegistryEntry::new(PluginRegistryEntryInput {
            key: plugin_key.to_string(),
            runtime: PluginRuntime::WasmComponent,
            api_version: "1.0.0".to_string(),
            path_glob: "*.plugin-test".to_string(),
            content: None,
            entry: "plugin.wasm".to_string(),
            schema_keys: schema_keys
                .iter()
                .map(|schema_key| (*schema_key).to_string())
                .collect(),
            create_schema_keys: Vec::new(),
            manifest_json,
            archive_file_id: plugin_storage_archive_file_id(plugin_key),
            archive_path: plugin_storage_archive_path(plugin_key),
            archive_blob_hash: BlobId::from_canonical_content(
                format!("archive-{plugin_key}").as_bytes(),
            )
            .to_hex(),
            wasm_blob_hash: BlobId::from_canonical_content(wasm).to_hex(),
        })
        .expect("test plugin registry entry should be valid");
        PluginRegistry::new(vec![entry]).expect("test plugin registry should be valid")
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

    fn in_filter(column_name: &str, values: &[&str]) -> Expr {
        Expr::InList(InList::new(
            Box::new(Expr::Column(Column::from_name(column_name))),
            values
                .iter()
                .map(|value| Expr::Literal(ScalarValue::Utf8(Some((*value).to_string())), None))
                .collect(),
            false,
        ))
    }

    #[test]
    fn public_id_and_path_filters_prune_before_hydration() {
        let hash = BlobId::from_canonical_content(b"content");
        let live_a = descriptor("01920000-0000-7000-8000-0000000000a2", Some("a.md"), 0);
        let live_b = descriptor("01920000-0000-7000-8000-0000000000b2", Some("b.md"), 0);
        let tombstone = descriptor("file-deleted", None, 0);
        let events = [&live_a, &live_b, &tombstone]
            .into_iter()
            .map(|descriptor| {
                file_history_event_from_entry(descriptor.id.clone(), &descriptor.entry)
            })
            .collect::<Vec<_>>();
        let context = filesystem_context(
            vec![live_a, live_b, tombstone],
            vec![
                blob_record("01920000-0000-7000-8000-0000000000a2", hash, 0),
                blob_record("01920000-0000-7000-8000-0000000000b2", hash, 0),
            ],
        );

        let id_predicate = FileHistoryPublicPredicate::from_filters(&[eq_filter(
            "id",
            "01920000-0000-7000-8000-0000000000a2",
        )]);
        let by_id = prepare_file_history_rows(
            &observed_states(&context),
            events.clone(),
            &HistoryRoute::default(),
            &id_predicate,
        )
        .unwrap();
        assert_eq!(by_id.len(), 1);
        assert_eq!(by_id[0].id, "01920000-0000-7000-8000-0000000000a2");

        let path_predicate =
            FileHistoryPublicPredicate::from_filters(&[eq_filter("path", "/b.md")]);
        let by_path = prepare_file_history_rows(
            &observed_states(&context),
            events.clone(),
            &HistoryRoute::default(),
            &path_predicate,
        )
        .unwrap();
        assert_eq!(by_path.len(), 1);
        assert_eq!(by_path[0].id, "01920000-0000-7000-8000-0000000000b2");

        let tombstone_predicate =
            FileHistoryPublicPredicate::from_filters(&[eq_filter("id", "file-deleted")]);
        let deleted = prepare_file_history_rows(
            &observed_states(&context),
            events,
            &HistoryRoute::default(),
            &tombstone_predicate,
        )
        .unwrap();
        assert_eq!(deleted.len(), 1);
        assert_eq!(deleted[0].id, "file-deleted");
        assert_eq!(deleted[0].path, None);

        let unsafe_or = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(eq_filter("id", "01920000-0000-7000-8000-0000000000a2")),
            Operator::Or,
            Box::new(eq_filter("name", "b.md")),
        ));
        assert!(
            FileHistoryPublicPredicate::extract_conjuncts(&unsafe_or).is_all(),
            "one supported OR arm must not prune rows needed by the other arm"
        );
        let mixed_and = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(eq_filter("id", "01920000-0000-7000-8000-0000000000a2")),
            Operator::And,
            Box::new(eq_filter("name", "a.md")),
        ));
        assert!(
            FileHistoryPublicPredicate::extract_conjuncts(&mixed_and)
                .matches("01920000-0000-7000-8000-0000000000a2", Some("/a.md")),
            "a guaranteed public conjunct remains safe for early pruning"
        );
    }

    #[test]
    fn authenticated_tombstones_remain_history_rows_but_nulls_fail_closed() {
        let file_id = "01920000-0000-7000-8000-0000000000a2";
        let descriptor_tombstone = ObservedStateRows::from_rows(
            SharedStr::from_static("commit-0"),
            vec![observed_row(
                super::FILE_DESCRIPTOR_SCHEMA_KEY,
                RowPk::single(file_id),
                Some(file_id),
                true,
                1,
            )],
        )
        .unwrap();
        let descriptor = parse_file_history_observed_descriptors(&descriptor_tombstone).unwrap();
        assert_eq!(descriptor.len(), 1);
        assert_eq!(descriptor[0].name, None);

        let directory_id = "01920000-0000-7000-8000-0000000000b2";
        let directory_tombstone = ObservedStateRows::from_rows(
            SharedStr::from_static("commit-0"),
            vec![observed_row(
                super::DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
                RowPk::single(directory_id),
                None,
                true,
                2,
            )],
        )
        .unwrap();
        let directory = parse_file_history_observed_directories(&directory_tombstone).unwrap();
        assert_eq!(directory.len(), 1);
        assert_eq!(directory[0].name, None);

        let blob_tombstone = ObservedStateRows::from_rows(
            SharedStr::from_static("commit-0"),
            vec![observed_row(
                super::BLOB_REF_SCHEMA_KEY,
                RowPk::single(file_id),
                Some(file_id),
                true,
                3,
            )],
        )
        .unwrap();
        let blob = parse_file_history_observed_blobs(&blob_tombstone).unwrap();
        assert_eq!(blob.len(), 1);
        assert_eq!(blob[0].blob_hash, None);

        let owner_tombstone = ObservedStateRows::from_rows(
            SharedStr::from_static("commit-0"),
            vec![observed_row(
                super::KEY_VALUE_SCHEMA_KEY,
                RowPk::single(super::PLUGIN_OWNER_KEY),
                Some(file_id),
                true,
                4,
            )],
        )
        .unwrap();
        let owner = parse_file_history_observed_plugin_owners(&owner_tombstone).unwrap();
        assert_eq!(owner.len(), 1);
        assert_eq!(owner[0].owner, None);

        let authenticated_null = ObservedStateRows::from_rows(
            SharedStr::from_static("commit-0"),
            vec![observed_row(
                super::FILE_DESCRIPTOR_SCHEMA_KEY,
                RowPk::single(file_id),
                Some(file_id),
                false,
                5,
            )],
        )
        .unwrap();
        assert!(parse_file_history_observed_descriptors(&authenticated_null).is_err());
    }

    #[test]
    fn observed_descriptor_tombstones_with_payload_fail_closed() {
        let file_id = "01920000-0000-7000-8000-0000000000a2";
        let mut file_tombstone = observed_row(
            super::FILE_DESCRIPTOR_SCHEMA_KEY,
            RowPk::single(file_id),
            Some(file_id),
            true,
            6,
        );
        file_tombstone.snapshot_content = Some(
            serde_json::json!({
                "id": file_id,
                "directory_id": null,
                "name": "file.txt",
            })
            .to_string()
            .into(),
        );
        let mut directory_tombstone = observed_row(
            super::DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
            RowPk::single("01920000-0000-7000-8000-0000000000b2"),
            None,
            true,
            7,
        );
        directory_tombstone.snapshot_content = Some(
            serde_json::json!({
                "id": "01920000-0000-7000-8000-0000000000b2",
                "parent_id": null,
                "name": "directory",
            })
            .to_string()
            .into(),
        );
        let mut owner_tombstone = observed_row(
            super::KEY_VALUE_SCHEMA_KEY,
            RowPk::single(super::PLUGIN_OWNER_KEY),
            Some(file_id),
            true,
            8,
        );
        owner_tombstone.snapshot_content = Some("{}".into());

        let rows = ObservedStateRows::from_rows(
            SharedStr::from_static("commit-0"),
            vec![file_tombstone, directory_tombstone, owner_tombstone],
        )
        .unwrap();

        assert!(parse_file_history_observed_descriptors(&rows).is_err());
        assert!(parse_file_history_observed_directories(&rows).is_err());
        assert!(parse_file_history_observed_plugin_owners(&rows).is_err());
    }

    #[test]
    fn live_descriptor_without_blob_ref_materializes_as_empty_file() {
        let file_id = "01920000-0000-7000-8000-0000000000a2";
        let descriptor = descriptor(file_id, Some("empty.txt"), 0);
        let event = file_history_event_from_entry(file_id.to_string(), &descriptor.entry);
        let missing = Arc::new(observed_state_from_entries(
            [descriptor.entry.clone()],
            PluginRegistry::empty(),
        ));
        assert!(
            super::validate_exactly_one_blob_ref(missing.as_ref(), &event, true)
                .unwrap()
                .is_none(),
            "a descriptor-only live file should not synthesize a BlobRef"
        );
        let missing_prepared = PreparedFileHistoryRow {
            id: file_id.to_string(),
            path: Some("/empty.txt".to_string()),
            observed_state: Arc::clone(&missing),
            descriptor_ordinal: 0,
            blob_hash: None,
            event: event.clone(),
        };
        super::validate_file_history_materialization(&missing_prepared, None)
            .expect("descriptor-only live file should materialize as empty content");

        let empty_hash = BlobId::from_canonical_content(b"");
        let blob = blob_record(file_id, empty_hash, 0);
        let state = Arc::new(observed_state_from_entries(
            [descriptor.entry.clone(), blob.entry],
            PluginRegistry::empty(),
        ));
        let reference = super::validate_exactly_one_blob_ref(state.as_ref(), &event, true)
            .unwrap()
            .expect("zero-length BlobRef should be live and authenticated");
        assert_eq!(reference.size_bytes, Some(0));

        let prepared = PreparedFileHistoryRow {
            id: file_id.to_string(),
            path: Some("/empty.txt".to_string()),
            observed_state: state,
            descriptor_ordinal: 0,
            blob_hash: Some(empty_hash.to_hex()),
            event,
        };
        super::validate_file_history_materialization(&prepared, Some(&[]))
            .expect("authenticated zero-length payload should remain valid");
    }

    #[test]
    fn historical_descriptor_and_directory_file_identity_bindings_fail_closed() {
        let file_id = "01920000-0000-7000-8000-0000000000a2";
        let mut file = history_entry(
            file_id,
            0,
            Some(
                serde_json::json!({
                    "id": file_id,
                    "directory_id": null,
                    "name": "file.txt",
                })
                .to_string(),
            ),
        );
        file.change.file_id = Some("01920000-0000-7000-8000-0000000000b2".to_string());
        assert!(super::parse_file_history_descriptors(&[file]).is_err());

        let directory_id = "01920000-0000-7000-8000-0000000000b2";
        let mut directory = history_entry(
            directory_id,
            0,
            Some(
                serde_json::json!({
                    "id": directory_id,
                    "parent_id": null,
                    "name": "directory",
                })
                .to_string(),
            ),
        );
        directory.change.schema_key = super::DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string();
        directory.change.file_id = Some(file_id.to_string());
        assert!(super::parse_file_history_directories(&[directory]).is_err());
    }

    #[test]
    fn equal_depth_sibling_revisions_are_not_deduplicated() {
        let mut left = history_entry("01920000-0000-7000-8000-0000000000a2", 1, None);
        left.observed_commit_id = "commit-left".to_string();
        left.change.id = "change-left".to_string();
        let mut right = history_entry("01920000-0000-7000-8000-0000000000a2", 1, None);
        right.observed_commit_id = "commit-right".to_string();
        right.change.id = "change-right".to_string();

        let events = sorted_grouped_file_history_events([
            file_history_event_from_entry(
                "01920000-0000-7000-8000-0000000000a2".to_string(),
                &left,
            ),
            file_history_event_from_entry(
                "01920000-0000-7000-8000-0000000000a2".to_string(),
                &right,
            ),
        ])
        .unwrap();

        assert_eq!(events.len(), 2);
        assert_eq!(
            events
                .iter()
                .map(|event| event.observed_commit_id.as_str())
                .collect::<Vec<_>>(),
            vec!["commit-left", "commit-right"]
        );
    }

    #[test]
    fn same_commit_sources_form_one_logical_revision() {
        let descriptor = history_entry("01920000-0000-7000-8000-0000000000a2", 0, None);
        let mut blob = descriptor.clone();
        blob.change.id = "change-01920000-0000-7000-8000-0000000000a2-blob".to_string();
        blob.change.schema_key = super::BLOB_REF_SCHEMA_KEY.to_string();

        let events = sorted_grouped_file_history_events([
            file_history_event_from_entry(
                "01920000-0000-7000-8000-0000000000a2".to_string(),
                &descriptor,
            ),
            file_history_event_from_entry(
                "01920000-0000-7000-8000-0000000000a2".to_string(),
                &blob,
            ),
        ])
        .unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].source_changes.len(), 2);
        assert_eq!(
            events[0]
                .source_changes
                .iter()
                .map(|change| change.schema_key.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([
                super::BLOB_REF_SCHEMA_KEY,
                super::FILE_DESCRIPTOR_SCHEMA_KEY,
            ])
        );
    }

    #[test]
    fn same_commit_sources_deduplicate_ids_after_grouping_and_sort() {
        let mut first = history_entry("01920000-0000-7000-8000-0000000000a2", 0, None);
        first.change.id = "source-b".to_string();
        let mut second = first.clone();
        second.change.id = "source-a".to_string();
        let duplicate = second.clone();

        let events = sorted_grouped_file_history_events([
            file_history_event_from_entry(
                "01920000-0000-7000-8000-0000000000a2".to_string(),
                &first,
            ),
            file_history_event_from_entry(
                "01920000-0000-7000-8000-0000000000a2".to_string(),
                &second,
            ),
            file_history_event_from_entry(
                "01920000-0000-7000-8000-0000000000a2".to_string(),
                &duplicate,
            ),
        ])
        .unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0]
                .source_changes
                .iter()
                .map(|change| change.id.as_str())
                .collect::<Vec<_>>(),
            vec!["source-a", "source-b"]
        );
    }

    #[test]
    fn conflicting_source_change_ids_fail_before_projection_limit() {
        let mut first = history_entry("01920000-0000-7000-8000-0000000000a2", 0, None);
        first.change.id = "source-conflict".to_string();
        let mut conflicting = first.clone();
        conflicting.change.schema_key = super::BLOB_REF_SCHEMA_KEY.to_string();

        let result = sorted_grouped_file_history_events([
            file_history_event_from_entry(
                "01920000-0000-7000-8000-0000000000a2".to_string(),
                &first,
            ),
            file_history_event_from_entry(
                "01920000-0000-7000-8000-0000000000a2".to_string(),
                &conflicting,
            ),
        ]);

        assert!(result.is_err());
    }

    #[test]
    fn unfiltered_sibling_directory_fanout_uses_directory_file_buckets() {
        const SIBLING_COUNT: usize = 512;

        let directories = (0..SIBLING_COUNT)
            .map(|index| directory_record(&format!("directory-{index:04}")))
            .collect::<Vec<_>>();
        let descriptors = (0..SIBLING_COUNT)
            .map(|index| {
                descriptor_in_directory(
                    &format!("file-{index:04}"),
                    &format!("directory-{index:04}"),
                )
            })
            .collect::<Vec<_>>();
        let observed_state = Arc::new(observed_state_from_entries(
            descriptors
                .iter()
                .map(|record| record.entry.clone())
                .chain(directories.iter().map(|record| record.entry.clone())),
            PluginRegistry::empty(),
        ));
        let directory_index = FileHistoryDirectoryIndex::from_state(observed_state.as_ref());
        let observed_states = BTreeMap::from([("commit-0".to_string(), observed_state)]);

        let events = file_history_events(
            &[],
            &directories,
            &[],
            &descriptors,
            &observed_states,
            &BTreeMap::new(),
        )
        .expect("valid directory history should fan out");

        assert_eq!(events.len(), SIBLING_COUNT);
        for (index, event) in events.iter().enumerate() {
            assert_eq!(event.file_id, format!("file-{index:04}"));
            assert_eq!(event.source_changes.len(), 1);
            assert_eq!(
                event.source_changes[0].row_pk,
                RowPk::single(format!("directory-{index:04}"))
            );
        }

        let mut visited_buckets = 0;
        let mut visited_file_candidates = 0;
        for directory in &directories {
            directory_index
                .visit_affected_file_buckets(&directory.id, |bucket| {
                    visited_buckets += 1;
                    visited_file_candidates += bucket.len();
                })
                .expect("valid directory tree should have no cycle");
        }
        assert_eq!(visited_buckets, SIBLING_COUNT);
        assert_eq!(visited_file_candidates, SIBLING_COUNT);
    }

    #[test]
    fn owner_replacement_retains_prior_owner_state_tombstones() {
        let file_id = "plugin-file";
        let parent_commit_id = "commit-parent";
        let replacement_commit_id = "commit-replacement";
        let parent_owner = plugin_owner_record(
            file_id,
            PluginFileOwner::new(file_id, "plugin-a", vec!["plugin_a_state".to_string()]).unwrap(),
            parent_commit_id,
        );
        let replacement_owner = plugin_owner_record(
            file_id,
            PluginFileOwner::new(file_id, "plugin-b", vec!["plugin_b_state".to_string()]).unwrap(),
            replacement_commit_id,
        );
        let old_state_tombstone =
            plugin_state_tombstone(file_id, "plugin_a_state", replacement_commit_id);
        let mut parent_state = plugin_observed_state(parent_owner);
        parent_state.plugin_registry = plugin_registry("plugin-a", &["plugin_a_state"]);
        let mut replacement_state = plugin_observed_state(replacement_owner.clone());
        replacement_state.plugin_registry = plugin_registry("plugin-b", &["plugin_b_state"]);
        let observed_states = BTreeMap::from([
            (parent_commit_id.to_string(), Arc::new(parent_state)),
            (
                replacement_commit_id.to_string(),
                Arc::new(replacement_state),
            ),
        ]);
        let parents = BTreeMap::from([(
            replacement_commit_id.to_string(),
            vec![parent_commit_id.to_string()],
        )]);

        assert!(
            file_history_plugin_events(
                std::slice::from_ref(&old_state_tombstone),
                &[],
                &observed_states,
                &parents,
            )
            .unwrap()
            .is_empty(),
            "a prior-owner tombstone needs a durable owner change in the same commit"
        );
        let events = file_history_plugin_events(
            &[old_state_tombstone],
            &[replacement_owner],
            &observed_states,
            &parents,
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].source_changes[0].schema_key, "plugin_a_state");
    }

    #[test]
    fn owner_contract_update_retains_removed_schema_tombstones() {
        let file_id = "plugin-file";
        let parent_commit_id = "commit-parent";
        let update_commit_id = "commit-contract-update";
        let parent_owner = plugin_owner_record(
            file_id,
            PluginFileOwner::new(
                file_id,
                "plugin-a",
                vec![
                    "plugin_a_removed".to_string(),
                    "plugin_a_retained".to_string(),
                ],
            )
            .unwrap(),
            parent_commit_id,
        );
        let updated_owner = plugin_owner_record(
            file_id,
            PluginFileOwner::new(file_id, "plugin-a", vec!["plugin_a_retained".to_string()])
                .unwrap(),
            update_commit_id,
        );
        let removed_schema_tombstone =
            plugin_state_tombstone(file_id, "plugin_a_removed", update_commit_id);
        let mut parent_state = plugin_observed_state(parent_owner);
        parent_state.plugin_registry =
            plugin_registry("plugin-a", &["plugin_a_removed", "plugin_a_retained"]);
        let mut updated_state = plugin_observed_state(updated_owner.clone());
        updated_state.plugin_registry = plugin_registry("plugin-a", &["plugin_a_retained"]);
        let observed_states = BTreeMap::from([
            (parent_commit_id.to_string(), Arc::new(parent_state)),
            (update_commit_id.to_string(), Arc::new(updated_state)),
        ]);
        let parents = BTreeMap::from([(
            update_commit_id.to_string(),
            vec![parent_commit_id.to_string()],
        )]);

        let events = file_history_plugin_events(
            &[removed_schema_tombstone],
            &[updated_owner],
            &observed_states,
            &parents,
        )
        .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].source_changes[0].schema_key, "plugin_a_removed");
    }

    #[test]
    fn exact_public_ids_route_only_descriptor_and_blob_history() {
        let predicate = FileHistoryPublicPredicate::from_filters(&[in_filter(
            "id",
            &[
                "01920000-0000-7000-8000-0000000000b2",
                "01920000-0000-7000-8000-0000000000a2",
            ],
        )]);
        let ids = FileHistoryLookupIds::from_public_predicate(&predicate)
            .expect("literal public ID IN filter should be routable");
        let route = file_history_descriptor_blob_route(
            &HistoryRoute {
                as_of_commit_ids: vec!["commit-start".to_string()],
                ..HistoryRoute::default()
            },
            &ids,
        )
        .expect("file IDs should encode as canonical row keys");

        assert_eq!(
            route.row_pks,
            vec![
                r#"["01920000-0000-7000-8000-0000000000a2"]"#.to_string(),
                r#"["01920000-0000-7000-8000-0000000000b2"]"#.to_string()
            ]
        );
        assert_eq!(route.as_of_commit_ids, vec!["commit-start".to_string()]);
    }

    #[test]
    fn public_id_pushdown_declines_or_nonliteral_and_mixed_predicates() {
        let disjunction = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(eq_filter("id", "01920000-0000-7000-8000-0000000000a2")),
            Operator::Or,
            Box::new(eq_filter("id", "01920000-0000-7000-8000-0000000000b2")),
        ));
        assert!(
            FileHistoryLookupIds::from_public_predicate(&FileHistoryPublicPredicate::from_filters(
                &[disjunction]
            ))
            .is_none(),
            "OR must retain the existing complete traversal"
        );

        let nonliteral = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(Column::from_name("id"))),
            Operator::Eq,
            Box::new(Expr::Column(Column::from_name("other_id"))),
        ));
        assert!(
            FileHistoryLookupIds::from_public_predicate(&FileHistoryPublicPredicate::from_filters(
                &[nonliteral]
            ))
            .is_none(),
            "non-literal IDs cannot become storage keys"
        );

        let mixed_conjunction = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(eq_filter("id", "01920000-0000-7000-8000-0000000000a2")),
            Operator::And,
            Box::new(eq_filter("path", "/a.md")),
        ));
        let mixed_ids = FileHistoryLookupIds::from_public_predicate(
            &FileHistoryPublicPredicate::from_filters(&[mixed_conjunction]),
        )
        .expect("an exact ID conjunct remains safe with a residual path predicate");
        assert_eq!(
            mixed_ids.0,
            BTreeSet::from(["01920000-0000-7000-8000-0000000000a2".to_owned()])
        );
    }

    #[tokio::test]
    async fn identical_event_and_context_routes_load_history_once() {
        let route = HistoryRoute {
            as_of_commit_ids: vec!["cid-start".to_string()],
            file_ids: vec!["01920000-0000-7000-8000-0000000000a2".to_string()],
            ..HistoryRoute::default()
        };
        let event_route = route.traversal_only();
        let context_route = route.anchors_only();
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
