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

use crate::LixError;
use crate::NullableKeyFilter;
use crate::binary_cas::{BlobDataReader, BlobId};
use crate::commit_graph::CommitGraphReader;
use crate::common::{SharedStr, compose_file_path};
use crate::entity_pk::EntityPk;
use crate::plugin::{
    PLUGIN_OWNER_KEY, PLUGIN_REGISTRY_KEY, PluginFileOwner, PluginRegistry, PluginRuntimeHost,
};
use crate::tracked_state::{
    TrackedStateContext, TrackedStateFilter, TrackedStateReadColumns, TrackedStateScanRequest,
    TrackedStateStoreReader,
};

use super::columns::{Col, ColumnTable, ColumnTableError};
use super::history_util::{
    ObservedTrackedStateOrdinal, ObservedTrackedStateRows, entity_pk_json_array,
};
use super::spec::{PlannedScan, TableSpec, projected_schema, register_spec_table, scan_row_source};
use crate::sql2::SqlHistoryQuerySource;
use crate::sql2::WriteAccess;
use crate::sql2::change_materialization::MaterializedChange;
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

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";
const BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
const KEY_VALUE_SCHEMA_KEY: &str = "lix_key_value";

pub(super) async fn register_lix_file_history_surface<S>(
    session: &datafusion::prelude::SessionContext,
    surface_name: &str,
    commit_graph: Box<dyn CommitGraphReader>,
    query_source: SqlHistoryQuerySource<S>,
    blob_reader: Arc<dyn BlobDataReader>,
    plugin_host: PluginRuntimeHost,
) -> Result<(), LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
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
/// walking the commit graph from the routed anchor commits, resolving the
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
        route.default_to_as_of_commit_id(&self.query_source.default_as_of_commit_id);
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
                    Arc::clone(&self.blob_reader),
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
                    blob_reader,
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
                        &blob_reader,
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

    fn exact_ids(&self) -> Option<&BTreeSet<String>> {
        match self {
            Self::Ids(ids) => Some(ids),
            _ => None,
        }
    }

    /// The finite set of `path` literals every matching row must render, when
    /// one exists.
    ///
    /// A conjunction narrows (either side alone is already a sound bound); a
    /// disjunction only bounds the whole expression when both sides do.
    fn exact_paths(&self) -> Option<BTreeSet<String>> {
        match self {
            Self::All | Self::Ids(_) => None,
            Self::Paths(paths) => Some(paths.clone()),
            Self::And(left, right) => match (left.exact_paths(), right.exact_paths()) {
                (Some(left), Some(right)) => Some(left.intersection(&right).cloned().collect()),
                (bound, None) | (None, bound) => bound,
            },
            Self::Or(left, right) => {
                let mut left = left.exact_paths()?;
                left.extend(right.exact_paths()?);
                Some(left)
            }
        }
    }
}

/// A conservative exact public `id` constraint that can be translated to the
/// canonical descriptor/blob entity primary key. Unlike
/// [`FileHistoryPublicPredicate`], this deliberately declines disjunctions and
/// non-literal `id` expressions: those retain the existing complete traversal
/// and DataFusion residual evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FileHistoryLookupIds(BTreeSet<String>);

impl FileHistoryLookupIds {
    fn from_public_predicate(predicate: &FileHistoryPublicPredicate) -> Option<Self> {
        predicate.exact_ids().cloned().map(Self)
    }

    fn entity_pks(&self) -> Result<Vec<String>, LixError> {
        self.0.iter().map(|id| entity_pk_json_array(id)).collect()
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

#[derive(Debug, Deserialize)]
struct HistoryIdentitySnapshot {
    id: String,
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
    row: ObservedTrackedStateOrdinal,
}

#[derive(Debug, Clone)]
struct FileHistoryObservedDirectoryRecord {
    id: String,
    parent_id: Option<String>,
    name: Option<String>,
    row: ObservedTrackedStateOrdinal,
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
    row: ObservedTrackedStateOrdinal,
}

#[derive(Debug, Clone)]
struct FileHistoryObservedPluginOwnerRecord {
    file_id: String,
    owner: Option<PluginFileOwner>,
    row: ObservedTrackedStateOrdinal,
}

#[derive(Debug)]
struct FileHistoryObservedState {
    rows: ObservedTrackedStateRows,
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

    fn affected_file_ids(&self, changed_directory_id: &str) -> BTreeSet<String> {
        let mut file_ids = BTreeSet::new();
        self.visit_affected_file_buckets(changed_directory_id, |bucket| {
            file_ids.extend(bucket.iter().cloned());
        });
        file_ids
    }

    fn visit_affected_file_buckets(
        &self,
        changed_directory_id: &str,
        mut visit: impl FnMut(&BTreeSet<String>),
    ) {
        for directory_id in self.tree.descendants_including(changed_directory_id) {
            if let Some(bucket) = self.file_ids_by_directory.get(&directory_id) {
                visit(bucket);
            }
        }
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
    query_source: SqlHistoryQuerySource<S>,
    blob_reader: &Arc<dyn BlobDataReader>,
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
    // A `path` predicate prunes exactly like an `id` predicate once it is
    // resolved to the file IDs that could render that path.
    let resolved_lookup_ids = match lookup_ids {
        Some(lookup_ids) => Some(lookup_ids.clone()),
        None => {
            resolve_file_history_path_lookup_ids(
                Arc::clone(&commit_graph),
                query_source.clone(),
                &context_route,
                public_predicate,
            )
            .await?
        }
    };
    if resolved_lookup_ids
        .as_ref()
        .is_some_and(|lookup_ids| lookup_ids.0.is_empty())
    {
        return Ok(Vec::new());
    }
    let lookup_ids = resolved_lookup_ids.as_ref();

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
    let plugin_discovery = discover_file_history_plugins(
        Arc::clone(&commit_graph),
        query_source.clone(),
        &event_route,
        &context_route,
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
        load_file_history_observed_states(query_source, observed_commit_ids, lookup_ids).await?;
    let filesystem_events = file_history_events(
        &filesystem_context.event_descriptors,
        &filesystem_context.event_directories,
        &filesystem_context.event_blobs,
        &filesystem_context.descriptors,
        &observed_states,
        &parent_commit_ids_by_commit,
    );
    let plugin_state_events = file_history_plugin_events(
        &event_plugin_state,
        &event_plugin_owners,
        &observed_states,
        &plugin_discovery.parent_commit_ids_by_commit,
    );
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
    );
    let prepared = prepare_file_history_rows(&observed_states, events, route, public_predicate)?;
    let blob_bytes = if needs_data {
        load_file_history_blob_bytes(blob_reader, &prepared).await?
    } else {
        BTreeMap::new()
    };

    let mut output = Vec::with_capacity(prepared.len());
    for prepared_row in prepared {
        let data = if needs_data && prepared_row.descriptor().name.is_some() {
            validate_file_history_materialization(&prepared_row)?;
            prepared_row.blob_hash.as_deref().map_or_else(
                || Some(Vec::new()),
                |blob_hash| blob_bytes.get(blob_hash).cloned().flatten(),
            )
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
            continue;
        };
        let directory_index = directory_indexes
            .get(event.observed_commit_id.as_str())
            .expect("every observed file state should have a directory index");
        if !file_history_event_affects_observed_file(&event, descriptor, &directory_index.tree) {
            continue;
        }
        let path = resolve_observed_file_history_path(descriptor, &state.directories);
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
        let blob_hash = state
            .blobs
            .iter()
            .find(|blob| blob.file_id == event.file_id)
            .and_then(|blob| {
                let _ = state.rows.row(blob.row);
                blob.blob_hash.clone()
            });
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

async fn load_file_history_blob_bytes(
    blob_reader: &Arc<dyn BlobDataReader>,
    rows: &[PreparedFileHistoryRow],
) -> Result<BTreeMap<String, Option<Vec<u8>>>, LixError> {
    let mut hashes = BTreeMap::<BlobId, BTreeSet<String>>::new();
    for hash in rows
        .iter()
        .filter(|row| row.descriptor().name.is_some())
        .filter_map(|row| row.blob_hash.as_deref())
    {
        hashes
            .entry(BlobId::from_hex(hash)?)
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

fn invalid_file_history_state(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INVALID_PLUGIN, message)
}

/// A plugin owner commits a materialization contract alongside semantic
/// state. Never silently serve an owned historical file from an absent or
/// opposite representation: that would turn a damaged history root into empty
/// bytes instead of surfacing the corruption to the caller.
fn validate_file_history_materialization(
    prepared: &PreparedFileHistoryRow,
) -> Result<(), LixError> {
    let observed_commit_id = prepared.event.observed_commit_id.as_str();
    let state = prepared.observed_state.as_ref();
    let descriptor = prepared.descriptor();
    let has_blob = prepared.blob_hash.is_some();
    let Some(owner) = live_file_history_plugin_owner(state, &descriptor.id) else {
        return Ok(());
    };
    let plugin = state
        .plugin_registry
        .get(owner.plugin_key())
        .ok_or_else(|| {
            invalid_file_history_state(format!(
                "plugin-owned file '{}' at commit '{observed_commit_id}' names unavailable plugin '{}'",
                descriptor.id,
                owner.plugin_key(),
            ))
        })?;
    if has_blob {
        Ok(())
    } else {
        Err(invalid_file_history_state(format!(
            "plugin '{}' owns file '{}' at commit '{observed_commit_id}' without exactly one blob reference",
            plugin.key(),
            descriptor.id,
        )))
    }
}

/// Resolves an exact `path` predicate to the file IDs that can render it, so a
/// path lookup prunes the traversal the same way an ID lookup already does.
///
/// A row's path is `<directory path>/<descriptor name>` and a path segment can
/// never contain `/`, so a row renders `path` only when its descriptor name
/// equals the final segment of `path` at that commit. Every name a file has
/// ever carried was written by a descriptor change, and every commit a row can
/// be observed at is reachable from the anchors — so descriptor changes read
/// from the anchors are a superset of the matching file IDs. The residual
/// [`FileHistoryPublicPredicate::matches`] still decides every row, so this
/// only narrows the traversal; it never decides the answer.
///
/// Returns `None` when the predicate cannot be bounded to a finite set of
/// paths, which keeps the complete traversal.
async fn resolve_file_history_path_lookup_ids<S>(
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlHistoryQuerySource<S>,
    anchor_route: &HistoryRoute,
    public_predicate: &FileHistoryPublicPredicate,
) -> Result<Option<FileHistoryLookupIds>, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    let Some(paths) = public_predicate.exact_paths() else {
        return Ok(None);
    };
    let mut names = BTreeSet::new();
    for path in &paths {
        match path.rsplit_once('/') {
            Some((_, name)) if !name.is_empty() => {
                names.insert(name.to_string());
            }
            // Not a composable Lix path. Rather than reason about a literal no
            // composed path can equal, leave the traversal unpruned.
            _ => return Ok(None),
        }
    }

    let entries = load_history_entries(
        HistoryViewDescriptor {
            view_name: "lix_file_history",
            as_of_commit_column: HISTORY_COL_AS_OF_COMMIT_ID,
        },
        commit_graph,
        query_source,
        anchor_route,
        vec![FILE_DESCRIPTOR_SCHEMA_KEY.to_string()],
        HistoryMetadataProjection::default(),
        None,
    )
    .await?;

    let mut file_ids = BTreeSet::new();
    for entry in &entries {
        // A tombstone carries no name, and the name it retired was written by
        // the live descriptor change this scan already saw.
        let Some(snapshot_content) = entry.change.snapshot_content.as_deref() else {
            continue;
        };
        let snapshot: FileDescriptorSnapshot =
            serde_json::from_str(snapshot_content).map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("invalid lix_file_descriptor history snapshot JSON: {error}"),
                )
            })?;
        if !names.contains(&snapshot.name) {
            continue;
        }
        if EntityPk::uuid_from_canonical(&snapshot.id).is_err() {
            // The pruned readers key on a canonical UUID. A file ID that is not
            // one cannot be routed, so keep the complete traversal.
            return Ok(None);
        }
        file_ids.insert(snapshot.id);
    }
    Ok(Some(FileHistoryLookupIds(file_ids)))
}

async fn load_file_history_filesystem_context<S>(
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlHistoryQuerySource<S>,
    event_route: &HistoryRoute,
    context_route: &HistoryRoute,
    lookup_ids: Option<&FileHistoryLookupIds>,
    metadata_projection: HistoryMetadataProjection,
) -> Result<FileHistoryFilesystemContext, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    let lookup_ids = lookup_ids.cloned();
    let (event_entries, context_entries) =
        load_file_history_entry_sets(event_route, context_route, move |route| {
            let commit_graph = Arc::clone(&commit_graph);
            let query_source = query_source.clone();
            let lookup_ids = lookup_ids.clone();
            async move {
                load_file_history_filesystem_entries(
                    commit_graph,
                    query_source,
                    &route,
                    lookup_ids.as_ref(),
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
    })
}

async fn load_file_history_observed_states<S>(
    query_source: SqlHistoryQuerySource<S>,
    observed_commit_ids: BTreeSet<String>,
    lookup_ids: Option<&FileHistoryLookupIds>,
) -> Result<BTreeMap<String, Arc<FileHistoryObservedState>>, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    // Traversal depth is only a distance from the requested start commit. In a
    // DAG, an equal-depth row may belong to a sibling and must not shape this
    // revision. Commit roots are the canonical state as of each observed
    // commit, so use them directly instead of inferring ancestry from depth.
    let mut reader = TrackedStateContext::new().reader(query_source.store);
    let mut states = BTreeMap::new();
    for observed_commit_id in observed_commit_ids {
        let state =
            load_file_history_observed_state(&mut reader, &observed_commit_id, lookup_ids).await?;
        states.insert(observed_commit_id, Arc::new(state));
    }
    Ok(states)
}

async fn load_file_history_observed_state<S>(
    reader: &mut TrackedStateStoreReader<S>,
    observed_commit_id: &str,
    lookup_ids: Option<&FileHistoryLookupIds>,
) -> Result<FileHistoryObservedState, LixError>
where
    S: StorageAdapterRead,
{
    let observed_commit: SharedStr = observed_commit_id.into();
    let plugin_registry = load_plugin_registry_at_observed_commit(reader, &observed_commit).await?;
    let plugin_owner_rows = load_file_history_plugin_owner_rows_at_observed_commit(
        reader,
        &observed_commit,
        lookup_ids,
    )
    .await?;
    let mut rows = if let Some(lookup_ids) = lookup_ids {
        load_selected_file_history_observed_rows(reader, &observed_commit, lookup_ids).await?
    } else {
        scan_file_history_observed_rows(
            reader,
            &observed_commit,
            TrackedStateFilter {
                schema_keys: file_history_filesystem_schema_keys(),
                include_tombstones: true,
                ..TrackedStateFilter::default()
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
    reader: &mut TrackedStateStoreReader<S>,
    observed_commit_id: &SharedStr,
    lookup_ids: Option<&FileHistoryLookupIds>,
) -> Result<ObservedTrackedStateRows, LixError>
where
    S: StorageAdapterRead,
{
    scan_file_history_observed_rows(
        reader,
        observed_commit_id,
        TrackedStateFilter {
            schema_keys: vec![KEY_VALUE_SCHEMA_KEY.to_string()],
            entity_pks: vec![EntityPk::single(PLUGIN_OWNER_KEY)],
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
    reader: &mut TrackedStateStoreReader<S>,
    observed_commit_id: &SharedStr,
    lookup_ids: &FileHistoryLookupIds,
) -> Result<ObservedTrackedStateRows, LixError>
where
    S: StorageAdapterRead,
{
    let entity_pks = lookup_ids
        .0
        .iter()
        .map(|file_id| {
            EntityPk::uuid_from_canonical(file_id).map_err(|error| {
                LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!("file history id must be a canonical UUID: {error}"),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let file_ids = selected_file_id_filters(lookup_ids);
    let mut rows = scan_file_history_observed_rows(
        reader,
        observed_commit_id,
        TrackedStateFilter {
            schema_keys: vec![
                FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                BLOB_REF_SCHEMA_KEY.to_string(),
            ],
            entity_pks,
            file_ids: file_ids.clone(),
            include_tombstones: true,
        },
    )
    .await?;
    let descriptors = parse_file_history_observed_descriptors(&rows)?;
    rows.append(
        load_file_history_ancestor_directory_rows(
            reader,
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
    reader: &mut TrackedStateStoreReader<S>,
    observed_commit_id: &SharedStr,
    descriptors: &[FileHistoryObservedDescriptorRecord],
    file_ids: Vec<NullableKeyFilter<String>>,
) -> Result<ObservedTrackedStateRows, LixError>
where
    S: StorageAdapterRead,
{
    let mut pending = descriptors
        .iter()
        .filter_map(|descriptor| descriptor.directory_id.clone())
        .collect::<BTreeSet<_>>();
    let mut requested = BTreeSet::new();
    let mut rows = ObservedTrackedStateRows::default();
    while !pending.is_empty() {
        let ids = std::mem::take(&mut pending)
            .into_iter()
            .filter(|id| requested.insert(id.clone()))
            .collect::<Vec<_>>();
        if ids.is_empty() {
            break;
        }
        let loaded = scan_file_history_observed_rows(
            reader,
            observed_commit_id,
            TrackedStateFilter {
                schema_keys: vec![DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string()],
                entity_pks: ids
                    .iter()
                    .map(|directory_id| {
                        EntityPk::uuid_from_canonical(directory_id).map_err(|error| {
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
    reader: &mut TrackedStateStoreReader<S>,
    observed_commit_id: &SharedStr,
    filter: TrackedStateFilter,
) -> Result<ObservedTrackedStateRows, LixError>
where
    S: StorageAdapterRead,
{
    let rows = reader
        .scan_batch_at_commit(
            observed_commit_id.as_str(),
            &TrackedStateScanRequest {
                filter,
                read_columns: TrackedStateReadColumns {
                    columns: vec!["snapshot_content".to_string(), "metadata".to_string()],
                },
                ..TrackedStateScanRequest::default()
            },
        )
        .await?;
    ObservedTrackedStateRows::from_batch(observed_commit_id.clone(), rows)
}

async fn load_plugin_registry_at_observed_commit<S>(
    reader: &mut TrackedStateStoreReader<S>,
    observed_commit_id: &SharedStr,
) -> Result<PluginRegistry, LixError>
where
    S: StorageAdapterRead,
{
    let rows = reader
        .scan_batch_at_commit(
            observed_commit_id.as_str(),
            &TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    schema_keys: vec![KEY_VALUE_SCHEMA_KEY.to_string()],
                    entity_pks: vec![EntityPk::single(PLUGIN_REGISTRY_KEY)],
                    file_ids: vec![NullableKeyFilter::Null],
                    include_tombstones: true,
                },
                read_columns: TrackedStateReadColumns {
                    columns: vec!["snapshot_content".to_string()],
                },
                ..TrackedStateScanRequest::default()
            },
        )
        .await?;
    let snapshot = (rows.len() != 0)
        .then(|| rows.row(rows.len() - 1))
        .and_then(|row| (!row.deleted()).then_some(row.snapshot_content()))
        .flatten();
    parse_plugin_registry_snapshot(snapshot.map(|snapshot| snapshot.as_str()), observed_commit_id.as_str())
}

/// Builds the plugin registry a snapshot describes.
///
/// A `None` snapshot is an absent or tombstoned registry, i.e. no plugins.
fn parse_plugin_registry_snapshot(
    snapshot_content: Option<&str>,
    observed_commit_id: &str,
) -> Result<PluginRegistry, LixError> {
    let snapshot = snapshot_content
        .map(|snapshot| {
            serde_json::from_str::<serde_json::Value>(snapshot).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "lix_file_history plugin registry snapshot is invalid JSON at observed commit '{observed_commit_id}': {error}"
                    ),
                )
            })
        })
        .transpose()?;
    PluginRegistry::from_optional_snapshot(snapshot.as_ref())
}

async fn load_file_history_filesystem_entries<S>(
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlHistoryQuerySource<S>,
    route: &HistoryRoute,
    lookup_ids: Option<&FileHistoryLookupIds>,
    metadata_projection: HistoryMetadataProjection,
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
            None,
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
        None,
    )
    .await?;
    // Directory changes can rename or move a selected file. Their entity keys
    // are unrelated to the public file ID, so retain the complete directory
    // history and let `file_history_events` join only relevant directories.
    let directories = load_history_entries(
        HistoryViewDescriptor {
            view_name: "lix_file_history",
            as_of_commit_column: HISTORY_COL_AS_OF_COMMIT_ID,
        },
        commit_graph,
        query_source,
        route,
        vec![DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string()],
        metadata_projection,
        None,
    )
    .await?;
    entries.extend(directories);
    Ok(entries)
}

/// Routes the descriptor + blob-ref traversal for a known set of file IDs.
///
/// # Why this pins `file_ids` as well as the entity primary keys
///
/// `lix_binary_blob_ref` is touched by *every* content commit, so the
/// touched-scope digest's schema-family test can never prove this projection
/// absent: some commit in almost every workload has a blob-ref member. The
/// digest also carries a `(schema_key, file_id)` **pair** token, which is
/// exactly the discriminator this traversal needs — but `scope_digest_outcome`
/// only reaches the pair probe when the request pins `file_ids`, and returns
/// `LoadedPresent` otherwise. Leaving `file_ids` empty therefore loaded one
/// commit delta per reachable commit to discover that its blob-ref member
/// belongs to a different file.
///
/// # Why pinning cannot drop a history row
///
/// `change_matches_history_request` treats a non-empty `file_ids` as requiring
/// a non-null `file_id` that is in the set, so pinning would be a correctness
/// bug if any matching row could carry a null or different `file_id`. It
/// cannot, for either of this route's two schema keys:
///
/// * `lix_file_descriptor` — `canonicalize_descriptor_file_id` rewrites
///   `file_id` to the row's own `entity_pk` on **every** normalized descriptor
///   row, on all three exits of the row normalizer and for tombstones as well
///   as live rows, and errors if that identity is not a single value. The
///   planner really can hand it a descriptor row with `file_id: None` (an
///   `INSERT` that leaves the file ID to the engine), and normalization is what
///   closes that hole. `delete_restriction_source_domains` already depends on
///   the same invariant.
/// * `lix_binary_blob_ref` — every producer (`BlobRefRowInput::append_to` and
///   `append_blob_ref_tombstone_row`) sets `entity_pk` and `file_id` from the
///   same file ID, and both schemas are read-only public surfaces, so no caller
///   can supply a divergent pair.
///
/// Because `file_id == entity_pk` holds for both, and this route already pins
/// `resolved_entity_pks` to the same IDs, the added predicate is implied by the
/// one already applied: it cannot reject a change the existing entity-PK test
/// accepts. The identities compare byte-for-byte rather than merely
/// semantically because `EntityPk::uuid_from_canonical` accepts only the exact
/// lowercase hyphenated form — a file ID that is not canonical already fails
/// this function before any pinning happens.
fn file_history_descriptor_blob_route(
    route: &HistoryRoute,
    lookup_ids: &FileHistoryLookupIds,
) -> Result<HistoryRoute, LixError> {
    let mut route = route.clone();
    route.entity_pks = lookup_ids.entity_pks()?;
    route.file_ids = lookup_ids.0.iter().cloned().collect();
    route.resolved_entity_pks = lookup_ids
        .0
        .iter()
        .map(|file_id| {
            EntityPk::uuid_from_canonical(file_id).map_err(|error| {
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
    query_source: SqlHistoryQuerySource<S>,
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
                    None,
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

async fn load_file_history_plugin_owner_events<S>(
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlHistoryQuerySource<S>,
    event_route: &HistoryRoute,
    lookup_ids: Option<&FileHistoryLookupIds>,
    metadata_projection: HistoryMetadataProjection,
) -> Result<Vec<FileHistoryPluginOwnerRecord>, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    let mut owner_route = file_history_plugin_route(event_route, lookup_ids);
    let owner_pk = EntityPk::single(PLUGIN_OWNER_KEY);
    owner_route.entity_pks = vec![owner_pk.as_json_array_text()?];
    owner_route.resolved_entity_pks = vec![owner_pk];
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
        None,
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
) -> Vec<FileHistoryEvent> {
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
                affected_file_ids.extend(directory_index.affected_file_ids(&directory.id));
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

fn sorted_grouped_file_history_events<I>(events: I) -> Vec<FileHistoryEvent>
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
    events
}

async fn discover_file_history_plugins<S>(
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlHistoryQuerySource<S>,
    event_route: &HistoryRoute,
    context_route: &HistoryRoute,
    parent_commit_ids_by_commit: &BTreeMap<String, Vec<String>>,
    metadata_projection: HistoryMetadataProjection,
) -> Result<FileHistoryPluginDiscovery, LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    let registry_pk = EntityPk::single(PLUGIN_REGISTRY_KEY);
    let registry_pk_text = registry_pk.as_json_array_text()?;

    let mut registry_route = event_route.clone();
    registry_route.entity_pks = vec![registry_pk_text.clone()];
    registry_route.resolved_entity_pks = vec![registry_pk.clone()];
    let registry_events = load_history_entries(
        HistoryViewDescriptor {
            view_name: "lix_file_history",
            as_of_commit_column: HISTORY_COL_AS_OF_COMMIT_ID,
        },
        Arc::clone(&commit_graph),
        query_source.clone(),
        &registry_route,
        vec![KEY_VALUE_SCHEMA_KEY.to_string()],
        metadata_projection,
        None,
    )
    .await?;

    // Schema-key discovery reads the registry's change history once instead of
    // reconstructing the registry at every reachable commit.
    //
    // The registry at a reachable commit is whatever the nearest reachable
    // registry change wrote, so the reachable registry changes carry every
    // registry value any reachable commit can observe. Taking the union over
    // those changes is therefore the same schema-key set the per-commit
    // reconstruction produced, for one traversal instead of one tracked-state
    // scan per commit. The anchors-only route keeps this independent of a depth
    // bound, which restricts the rows that are emitted, not which plugins
    // existed.
    let mut schema_key_route = context_route.clone();
    schema_key_route.entity_pks = vec![registry_pk_text];
    schema_key_route.resolved_entity_pks = vec![registry_pk];
    let schema_key_entries = if schema_key_route == registry_route {
        registry_events.clone()
    } else {
        load_history_entries(
            HistoryViewDescriptor {
                view_name: "lix_file_history",
                as_of_commit_column: HISTORY_COL_AS_OF_COMMIT_ID,
            },
            Arc::clone(&commit_graph),
            query_source.clone(),
            &schema_key_route,
            vec![KEY_VALUE_SCHEMA_KEY.to_string()],
            HistoryMetadataProjection::default(),
            None,
        )
        .await?
    };
    let mut schema_keys = BTreeSet::new();
    for entry in &schema_key_entries {
        let registry = parse_plugin_registry_snapshot(
            entry.change.snapshot_content.as_deref(),
            &entry.observed_commit_id,
        )?;
        schema_keys.extend(
            registry
                .plugins()
                .iter()
                .flat_map(|plugin| plugin.schema_keys().iter().cloned()),
        );
    }

    // A registry is only ever compared at a commit whose registry changed and
    // at that commit's direct parents, so reconstruct exactly those.
    let mut registry_commit_ids = BTreeSet::new();
    for entry in &registry_events {
        registry_commit_ids.insert(entry.observed_commit_id.clone());
        registry_commit_ids.extend(
            parent_commit_ids_by_commit
                .get(&entry.observed_commit_id)
                .into_iter()
                .flatten()
                .cloned(),
        );
    }
    let mut reader = TrackedStateContext::new().reader(query_source.store.clone());
    let mut registries_by_commit = BTreeMap::new();
    for observed_commit_id in registry_commit_ids {
        let shared_observed_commit: SharedStr = observed_commit_id.as_str().into();
        let registry =
            load_plugin_registry_at_observed_commit(&mut reader, &shared_observed_commit).await?;
        registries_by_commit.insert(observed_commit_id, registry);
    }

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
) -> Vec<FileHistoryEvent> {
    let owner_changes = event_plugin_owners
        .iter()
        .map(|record| {
            (
                record.entry.observed_commit_id.as_str(),
                record.file_id.as_str(),
            )
        })
        .collect::<BTreeSet<_>>();

    event_plugin_state
        .iter()
        .filter(|plugin_state| {
            let observed_commit_id = plugin_state.entry.observed_commit_id.as_str();
            let schema_key = plugin_state.entry.change.schema_key.as_str();
            let live_owner_matches = observed_states
                .get(observed_commit_id)
                .and_then(|state| {
                    live_file_history_plugin_owner(state, &plugin_state.file_id)
                        .map(|owner| (state, owner))
                })
                .is_some_and(|(state, owner)| {
                    file_history_owner_schema_keys(state, owner)
                        .iter()
                        .any(|owner_schema_key| owner_schema_key == schema_key)
                });
            if live_owner_matches {
                return true;
            }

            // A non-owner live row cannot change the public file projection.
            // A tombstone remains relevant only when this commit also changes
            // the durable owner and a direct parent proves that the schema was
            // part of the prior owner's rendering contract. This covers owner
            // deletion, A -> B replacement, and same-key contract updates.
            if plugin_state.entry.change.snapshot_content.is_some()
                || !owner_changes.contains(&(observed_commit_id, plugin_state.file_id.as_str()))
            {
                return false;
            }
            parent_commit_ids_by_commit
                .get(observed_commit_id)
                .into_iter()
                .flatten()
                .any(|parent_commit_id| {
                    observed_states
                        .get(parent_commit_id)
                        .and_then(|state| {
                            live_file_history_plugin_owner(state, &plugin_state.file_id)
                                .map(|owner| (state, owner))
                        })
                        .is_some_and(|(_state, owner)| {
                            owner
                                .schema_keys()
                                .iter()
                                .any(|owner_schema_key| owner_schema_key == schema_key)
                        })
                })
        })
        .map(|plugin_state| {
            file_history_event_from_entry(plugin_state.file_id.clone(), &plugin_state.entry)
        })
        .collect()
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

fn file_history_owner_schema_keys<'a>(
    state: &'a FileHistoryObservedState,
    owner: &'a PluginFileOwner,
) -> &'a [String] {
    state
        .plugin_registry
        .get(owner.plugin_key())
        .map(crate::plugin::PluginRegistryEntry::schema_keys)
        .unwrap_or_else(|| owner.schema_keys())
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
            let Some(snapshot_content) = entry.change.snapshot_content.as_deref() else {
                return Ok(FileHistoryDescriptorRecord {
                    id: entry.change.entity_pk.as_single_string_owned()?,
                    entry: entry.clone(),
                });
            };
            let snapshot: HistoryIdentitySnapshot = serde_json::from_str(snapshot_content)
                .map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid lix_file_descriptor history snapshot JSON: {error}"),
                    )
                })?;
            Ok(FileHistoryDescriptorRecord {
                id: snapshot.id,
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
            let Some(snapshot_content) = entry.change.snapshot_content.as_deref() else {
                return Ok(FileHistoryDirectoryRecord {
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
            Ok(FileHistoryDirectoryRecord {
                id: snapshot.id,
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
            let Some(snapshot_content) = entry.change.snapshot_content.as_deref() else {
                return Ok(FileHistoryBlobRecord {
                    file_id: entry.change.file_id.clone().unwrap_or_else(|| {
                        entry
                            .change
                            .entity_pk
                            .as_single_string_owned()
                            .expect("canonical change entity primary key should project")
                    }),
                    entry: entry.clone(),
                });
            };
            let snapshot: HistoryIdentitySnapshot = serde_json::from_str(snapshot_content)
                .map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid lix_binary_blob_ref history snapshot JSON: {error}"),
                    )
                })?;
            Ok(FileHistoryBlobRecord {
                file_id: entry.change.file_id.clone().unwrap_or(snapshot.id),
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

fn parse_file_history_plugin_owners(
    entries: &[HistoryEntry],
) -> Result<Vec<FileHistoryPluginOwnerRecord>, LixError> {
    entries
        .iter()
        .filter(|entry| {
            entry.change.schema_key == KEY_VALUE_SCHEMA_KEY
                && entry.change.entity_pk.as_single_string().ok() == Some(PLUGIN_OWNER_KEY)
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
    rows: &ObservedTrackedStateRows,
) -> Result<Vec<FileHistoryObservedDescriptorRecord>, LixError> {
    rows.iter()
        .filter(|observed| observed.row().schema_key() == FILE_DESCRIPTOR_SCHEMA_KEY)
        .map(|observed| {
            let _ = observed.observed_commit_id();
            let row = observed.row();
            let Some(snapshot_content) = row.snapshot_content() else {
                return Ok(FileHistoryObservedDescriptorRecord {
                    id: row.entity_pk().as_single_string_owned()?,
                    directory_id: None,
                    name: None,
                    row: observed.ordinal(),
                });
            };
            let snapshot: FileDescriptorSnapshot =
                serde_json::from_str(snapshot_content).map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid lix_file_descriptor history snapshot JSON: {error}"),
                    )
                })?;
            Ok(FileHistoryObservedDescriptorRecord {
                id: snapshot.id,
                directory_id: snapshot.directory_id,
                name: Some(snapshot.name),
                row: observed.ordinal(),
            })
        })
        .collect()
}

fn parse_file_history_observed_directories(
    rows: &ObservedTrackedStateRows,
) -> Result<Vec<FileHistoryObservedDirectoryRecord>, LixError> {
    rows.iter()
        .filter(|observed| observed.row().schema_key() == DIRECTORY_DESCRIPTOR_SCHEMA_KEY)
        .map(|observed| {
            let row = observed.row();
            let Some(snapshot_content) = row.snapshot_content() else {
                return Ok(FileHistoryObservedDirectoryRecord {
                    id: row.entity_pk().as_single_string_owned()?,
                    parent_id: None,
                    name: None,
                    row: observed.ordinal(),
                });
            };
            let snapshot: DirectoryDescriptorSnapshot = serde_json::from_str(snapshot_content)
                .map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid lix_directory_descriptor history snapshot JSON: {error}"),
                    )
                })?;
            Ok(FileHistoryObservedDirectoryRecord {
                id: snapshot.id,
                parent_id: snapshot.parent_id,
                name: Some(snapshot.name),
                row: observed.ordinal(),
            })
        })
        .collect()
}

fn parse_file_history_observed_blobs(
    rows: &ObservedTrackedStateRows,
) -> Result<Vec<FileHistoryObservedBlobRecord>, LixError> {
    rows.iter()
        .filter(|observed| observed.row().schema_key() == BLOB_REF_SCHEMA_KEY)
        .map(|observed| {
            let row = observed.row();
            let fallback_file_id = || {
                row.file_id().map(str::to_owned).unwrap_or_else(|| {
                    row.entity_pk()
                        .as_single_string_owned()
                        .expect("canonical change entity primary key should project")
                })
            };
            let Some(snapshot_content) = row.snapshot_content() else {
                return Ok(FileHistoryObservedBlobRecord {
                    file_id: fallback_file_id(),
                    blob_hash: None,
                    row: observed.ordinal(),
                });
            };
            let snapshot: BlobRefSnapshot =
                serde_json::from_str(snapshot_content).map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid lix_binary_blob_ref history snapshot JSON: {error}"),
                    )
                })?;
            Ok(FileHistoryObservedBlobRecord {
                file_id: row.file_id().map(str::to_owned).unwrap_or(snapshot.id),
                blob_hash: Some(snapshot.blob_hash),
                row: observed.ordinal(),
            })
        })
        .collect()
}

fn parse_file_history_observed_plugin_owners(
    rows: &ObservedTrackedStateRows,
) -> Result<Vec<FileHistoryObservedPluginOwnerRecord>, LixError> {
    rows.iter()
        .filter(|observed| {
            let row = observed.row();
            row.schema_key() == KEY_VALUE_SCHEMA_KEY
                && row.entity_pk().as_single_string().ok() == Some(PLUGIN_OWNER_KEY)
        })
        .map(|observed| {
            let row = observed.row();
            let file_id = row.file_id().map(str::to_owned).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "lix_file_history plugin owner row is missing file_id",
                )
            })?;
            let owner = row
                .snapshot_content()
                .map(|snapshot| {
                    serde_json::from_str::<serde_json::Value>(snapshot)
                        .map_err(|error| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                format!(
                                    "lix_file_history plugin owner snapshot is invalid JSON for file '{file_id}': {error}"
                                ),
                            )
                        })
                        .and_then(|snapshot| PluginFileOwner::from_snapshot(&file_id, &snapshot))
                })
                .transpose()?;
            Ok(FileHistoryObservedPluginOwnerRecord {
                file_id,
                owner,
                row: observed.ordinal(),
            })
        })
        .collect()
}

fn file_history_event_affects_observed_file(
    event: &FileHistoryEvent,
    descriptor: &FileHistoryObservedDescriptorRecord,
    directory_tree: &HistoryDirectoryTree,
) -> bool {
    event
        .source_changes
        .iter()
        .any(|change| match change.schema_key.as_str() {
            FILE_DESCRIPTOR_SCHEMA_KEY | BLOB_REF_SCHEMA_KEY => {
                change
                    .file_id
                    .as_deref()
                    .is_some_and(|file_id| file_id == descriptor.id)
                    || change
                        .entity_pk
                        .as_single_string_owned()
                        .is_ok_and(|entity_id| entity_id == descriptor.id)
            }
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY => {
                let Ok(changed_directory_id) = change.entity_pk.as_single_string_owned() else {
                    return false;
                };
                let Some(directory_id) = descriptor.directory_id.as_deref() else {
                    return false;
                };
                directory_tree.has_ancestor_including(directory_id, &changed_directory_id)
            }
            KEY_VALUE_SCHEMA_KEY
                if change.entity_pk.as_single_string().ok() == Some(PLUGIN_REGISTRY_KEY) =>
            {
                event.file_id == descriptor.id
            }
            _ => change
                .file_id
                .as_deref()
                .is_some_and(|file_id| file_id == descriptor.id),
        })
}

fn resolve_observed_file_history_path(
    descriptor: &FileHistoryObservedDescriptorRecord,
    directories: &[FileHistoryObservedDirectoryRecord],
) -> Option<String> {
    let name = descriptor.name.as_ref()?;
    let Some(directory_id) = descriptor.directory_id.as_deref() else {
        return compose_file_path(None, name).ok();
    };
    let directory_path = resolve_observed_directory_path(
        directory_id,
        directories,
        &mut BTreeMap::new(),
        &mut BTreeSet::new(),
    )?;
    compose_file_path(Some(&directory_path), name).ok()
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
            HISTORY_COL_ENTITY_PK,
            Col::Utf8Fallible(|row| entity_pk_json_array(&row.descriptor().id).map(Some)),
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
        json_field(HISTORY_COL_ENTITY_PK, false),
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
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use datafusion::common::{Column, ScalarValue};
    use datafusion::logical_expr::expr::InList;
    use datafusion::logical_expr::{BinaryExpr, Expr, Operator};

    use crate::LixError;
    use crate::binary_cas::{BlobBytesBatch, BlobDataReader, BlobId};
    use crate::changelog::{ChangeId, CommitId};
    use crate::common::SharedStr;
    use crate::entity_pk::EntityPk;
    use crate::plugin::{
        PluginFileOwner, PluginRegistryEntry, PluginRegistryEntryInput, PluginRuntime,
        plugin_storage_archive_file_id, plugin_storage_archive_path,
    };
    use crate::sql2::change_materialization::MaterializedChange;
    use crate::sql2::history_route::HistoryEntry;
    use crate::tracked_state::{MaterializedTrackedStateBatch, MaterializedTrackedStateRow};

    use super::ObservedTrackedStateRows;
    use super::{
        FileHistoryBlobRecord, FileHistoryDescriptorRecord, FileHistoryDirectoryIndex,
        FileHistoryDirectoryRecord, FileHistoryFilesystemContext, FileHistoryLookupIds,
        FileHistoryObservedState, FileHistoryPluginOwnerRecord, FileHistoryPluginStateRecord,
        FileHistoryPublicPredicate, HistoryRoute, PluginRegistry, PreparedFileHistoryRow,
        file_history_descriptor_blob_route, file_history_event_from_entry, file_history_events,
        file_history_plugin_events, load_file_history_blob_bytes, load_file_history_entry_sets,
        parse_file_history_observed_blobs, parse_file_history_observed_descriptors,
        parse_file_history_observed_directories, parse_file_history_observed_plugin_owners,
        prepare_file_history_rows, sorted_grouped_file_history_events,
    };

    fn history_entry(file_id: &str, depth: u32, snapshot_content: Option<String>) -> HistoryEntry {
        HistoryEntry {
            change: MaterializedChange {
                id: format!("change-{file_id}-{depth}"),
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                entity_pk: EntityPk::single(file_id),
                schema_key: super::FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                file_id: Some(file_id.to_string()),
                snapshot_content: snapshot_content.map(Into::into),
                metadata: None,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                origin_key: None,
            },
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
        let materialized = entries
            .into_iter()
            .enumerate()
            .map(|(index, entry)| {
                let change = entry.change;
                let deleted = change.snapshot_content.is_none();
                MaterializedTrackedStateRow {
                    entity_pk: change.entity_pk,
                    schema_key: change.schema_key,
                    file_id: change.file_id,
                    snapshot_content: change.snapshot_content,
                    metadata: change.metadata,
                    deleted,
                    created_at: change.created_at.clone(),
                    updated_at: change.created_at,
                    change_id: ChangeId::new(uuid::Uuid::from_u128(index as u128 + 1)),
                    commit_id: CommitId::new(uuid::Uuid::from_u128(1)),
                }
            })
            .collect::<Vec<_>>();
        let batch =
            MaterializedTrackedStateBatch::from_rows(materialized).expect("test history batch");
        let rows = ObservedTrackedStateRows::from_batch(SharedStr::from_static("commit-0"), batch)
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
        let commit_buffers = state.rows.observed_commit_buffer_identities();
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
        entry.change.entity_pk = EntityPk::single(super::PLUGIN_OWNER_KEY);
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
        entry.change.entity_pk = EntityPk::single("plugin-state");
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
            archive_blob_hash: BlobId::from_content(format!("archive-{plugin_key}").as_bytes())
                .to_hex(),
            wasm_blob_hash: BlobId::from_content(wasm).to_hex(),
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

    #[derive(Default)]
    struct RecordingBlobReader {
        calls: StdMutex<Vec<Vec<BlobId>>>,
        values: BTreeMap<BlobId, Option<Vec<u8>>>,
    }

    #[async_trait]
    impl BlobDataReader for RecordingBlobReader {
        async fn load_bytes_many(&self, hashes: &[BlobId]) -> Result<BlobBytesBatch, LixError> {
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
        async fn load_bytes_many(&self, _hashes: &[BlobId]) -> Result<BlobBytesBatch, LixError> {
            Ok(BlobBytesBatch::new(self.0.clone()))
        }
    }

    #[test]
    fn public_id_and_path_filters_prune_before_hydration() {
        let hash = BlobId::from_content(b"content");
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
        ]);

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
        ]);

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
        );

        assert_eq!(events.len(), SIBLING_COUNT);
        for (index, event) in events.iter().enumerate() {
            assert_eq!(event.file_id, format!("file-{index:04}"));
            assert_eq!(event.source_changes.len(), 1);
            assert_eq!(
                event.source_changes[0].entity_pk,
                EntityPk::single(format!("directory-{index:04}"))
            );
        }

        let mut visited_buckets = 0;
        let mut visited_file_candidates = 0;
        for directory in &directories {
            directory_index.visit_affected_file_buckets(&directory.id, |bucket| {
                visited_buckets += 1;
                visited_file_candidates += bucket.len();
            });
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
        let observed_states = BTreeMap::from([
            (
                parent_commit_id.to_string(),
                Arc::new(plugin_observed_state(parent_owner)),
            ),
            (
                replacement_commit_id.to_string(),
                Arc::new(plugin_observed_state(replacement_owner.clone())),
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
            .is_empty(),
            "a prior-owner tombstone needs a durable owner change in the same commit"
        );
        let events = file_history_plugin_events(
            &[old_state_tombstone],
            &[replacement_owner],
            &observed_states,
            &parents,
        );
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
        parent_state.plugin_registry = plugin_registry("plugin-a", &["plugin_a_retained"]);
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
        );
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
        .expect("file IDs should encode as canonical entity keys");

        assert_eq!(
            route.entity_pks,
            vec![
                r#"["01920000-0000-7000-8000-0000000000a2"]"#.to_string(),
                r#"["01920000-0000-7000-8000-0000000000b2"]"#.to_string()
            ]
        );
        // The digest's `(schema_key, file_id)` pair probe is only reached when
        // the request pins `file_ids`; without this the blob-ref projection
        // loads one commit delta per reachable content commit.
        assert_eq!(
            route.file_ids,
            vec![
                "01920000-0000-7000-8000-0000000000a2".to_string(),
                "01920000-0000-7000-8000-0000000000b2".to_string()
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
        assert!(
            FileHistoryLookupIds::from_public_predicate(&FileHistoryPublicPredicate::from_filters(
                &[mixed_conjunction]
            ))
            .is_none(),
            "mixed public predicates retain the existing complete traversal"
        );
    }

    #[test]
    fn exact_public_paths_bound_conjunctions_and_complete_disjunctions() {
        let single = FileHistoryPublicPredicate::from_filters(&[eq_filter("path", "/docs/a.md")]);
        assert_eq!(
            single.exact_paths(),
            Some(BTreeSet::from(["/docs/a.md".to_string()]))
        );

        // Either side of a conjunction is already a sound bound.
        let mixed_conjunction = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(eq_filter("id", "01920000-0000-7000-8000-0000000000a2")),
            Operator::And,
            Box::new(eq_filter("path", "/docs/a.md")),
        ));
        assert_eq!(
            FileHistoryPublicPredicate::from_filters(&[mixed_conjunction]).exact_paths(),
            Some(BTreeSet::from(["/docs/a.md".to_string()]))
        );

        let path_disjunction = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(eq_filter("path", "/docs/a.md")),
            Operator::Or,
            Box::new(eq_filter("path", "/docs/b.md")),
        ));
        assert_eq!(
            FileHistoryPublicPredicate::from_filters(&[path_disjunction]).exact_paths(),
            Some(BTreeSet::from([
                "/docs/a.md".to_string(),
                "/docs/b.md".to_string()
            ]))
        );

        // One unbounded side leaves the whole disjunction unbounded.
        let mixed_disjunction = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(eq_filter("path", "/docs/a.md")),
            Operator::Or,
            Box::new(eq_filter("id", "01920000-0000-7000-8000-0000000000a2")),
        ));
        assert!(
            FileHistoryPublicPredicate::from_filters(&[mixed_disjunction])
                .exact_paths()
                .is_none(),
            "an unbounded disjunct must retain the complete traversal"
        );

        assert!(FileHistoryPublicPredicate::All.exact_paths().is_none());
    }

    #[tokio::test]
    async fn blob_hydration_batches_deduplicates_and_preserves_missing_values() {
        let present_hash = BlobId::from_content(b"present");
        let missing_hash = BlobId::from_content(b"missing");
        let descriptor = descriptor("01920000-0000-7000-8000-0000000000a2", Some("a.md"), 0);
        let event = file_history_event_from_entry(
            "01920000-0000-7000-8000-0000000000a2".to_string(),
            &descriptor.entry,
        );
        let observed_state = Arc::new(observed_state_from_entries(
            [descriptor.entry.clone()],
            PluginRegistry::empty(),
        ));
        let row = |id: &str, hash: BlobId| PreparedFileHistoryRow {
            id: id.to_string(),
            path: Some(format!("/{id}.md")),
            observed_state: Arc::clone(&observed_state),
            descriptor_ordinal: 0,
            blob_hash: Some(hash.to_hex()),
            event: event.clone(),
        };
        let rows = vec![
            row("01920000-0000-7000-8000-0000000000a2", present_hash),
            row("01920000-0000-7000-8000-0000000000b2", present_hash),
            row("01920000-0000-7000-8000-0000000000c2", missing_hash),
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
        let descriptor = descriptor("01920000-0000-7000-8000-0000000000a2", Some("a.md"), 0);
        let event = file_history_event_from_entry(
            "01920000-0000-7000-8000-0000000000a2".to_string(),
            &descriptor.entry,
        );
        let observed_state = Arc::new(observed_state_from_entries(
            [descriptor.entry.clone()],
            PluginRegistry::empty(),
        ));
        let row = |id: &str, hash: BlobId| PreparedFileHistoryRow {
            id: id.to_string(),
            path: Some(format!("/{id}.md")),
            observed_state: Arc::clone(&observed_state),
            descriptor_ordinal: 0,
            blob_hash: Some(hash.to_hex()),
            event: event.clone(),
        };
        let rows = vec![
            row(
                "01920000-0000-7000-8000-0000000000a2",
                BlobId::from_content(b"first"),
            ),
            row(
                "01920000-0000-7000-8000-0000000000b2",
                BlobId::from_content(b"second"),
            ),
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
        let first_hash = BlobId::from_content(b"first");
        let second_hash = BlobId::from_content(b"second");
        let descriptors = vec![
            descriptor("01920000-0000-7000-8000-0000000000a2", Some("a.md"), 0),
            descriptor("01920000-0000-7000-8000-0000000000b2", Some("b.md"), 0),
        ];
        let events = descriptors
            .iter()
            .map(|descriptor| {
                file_history_event_from_entry(descriptor.id.clone(), &descriptor.entry)
            })
            .collect::<Vec<_>>();
        let context = filesystem_context(
            descriptors,
            vec![
                blob_record("01920000-0000-7000-8000-0000000000a2", first_hash, 0),
                blob_record("01920000-0000-7000-8000-0000000000b2", second_hash, 0),
            ],
        );
        let rows = prepare_file_history_rows(
            &observed_states(&context),
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
