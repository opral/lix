use std::collections::{BTreeMap, BTreeSet};
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
use crate::branch::BranchRefReader;
use crate::checkpoint::latest_checkpoint_for_branch;
use crate::commit_graph::CommitGraphReader;
use crate::common::{compose_directory_path, compose_file_path};
use crate::entity_pk::EntityPk;
use crate::sql2::{SqlChangelogQuerySource, WriteAccess};
use crate::storage_adapter::StorageAdapterRead;
use crate::tracked_state::{
    TrackedStateContext, TrackedStateDiffRequest, TrackedStateFilter, TrackedStateReadColumns,
    TrackedStateScanRequest, TrackedStateStoreReader,
};

use super::checkpoint::{filter_conjuncts, selected_heads};
use super::columns::{Col, ColumnTable, ColumnTableError};
use super::file::{FileIdConstraint, exact_string_column_constraint_from_filters};
use super::spec::{PlannedScan, TableSpec, projected_schema, register_spec_table, row_source};
use crate::sql2::error::lix_error_to_datafusion_error;

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";

pub(super) async fn register_filesystem_working_change_provider<S>(
    session: &datafusion::prelude::SessionContext,
    surface_name: &str,
    active_branch_id: Option<String>,
    branch_ref: Arc<dyn BranchRefReader>,
    commit_graph: Box<dyn CommitGraphReader>,
    query_source: SqlChangelogQuerySource<S>,
    kind: FilesystemWorkingChangeKind,
) -> Result<(), LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    register_spec_table(
        session,
        surface_name,
        Arc::new(FilesystemWorkingChangeSpec {
            by_branch: active_branch_id.is_none(),
            active_branch_id,
            branch_ref,
            commit_graph: Arc::new(Mutex::new(commit_graph)),
            store: query_source.store,
            kind,
        }),
        WriteAccess::read_only(),
    )
}

#[derive(Clone, Copy)]
pub(super) enum FilesystemWorkingChangeKind {
    File,
    Directory,
}

struct FilesystemWorkingChangeSpec<S> {
    by_branch: bool,
    active_branch_id: Option<String>,
    branch_ref: Arc<dyn BranchRefReader>,
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    store: S,
    kind: FilesystemWorkingChangeKind,
}

#[async_trait]
impl<S> TableSpec for FilesystemWorkingChangeSpec<S>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    fn table_name(&self) -> &str {
        match (self.kind, self.by_branch) {
            (FilesystemWorkingChangeKind::File, false) => "lix_file_working_change",
            (FilesystemWorkingChangeKind::File, true) => "lix_file_working_change_by_branch",
            (FilesystemWorkingChangeKind::Directory, false) => "lix_directory_working_change",
            (FilesystemWorkingChangeKind::Directory, true) => {
                "lix_directory_working_change_by_branch"
            }
        }
    }

    fn schema(&self) -> SchemaRef {
        filesystem_working_change_schema(self.by_branch)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn filter_pushdown(&self, filter: &Expr) -> TableProviderFilterPushDown {
        if filter
            .column_refs()
            .iter()
            .any(|column| matches!(column.name.as_str(), "id" | "lixcol_branch_id"))
        {
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
        let schema = projected_schema(&self.schema(), projection);
        let route = FilesystemWorkingChangeRoute::from_filters(filters)?;
        Ok(PlannedScan {
            schema: Arc::clone(&schema),
            ordering: None,
            load: row_source(
                (
                    self.active_branch_id.clone(),
                    Arc::clone(&self.branch_ref),
                    Arc::clone(&self.commit_graph),
                    self.store.clone(),
                    schema,
                    route,
                    self.kind,
                ),
                move |(active_branch_id, branch_ref, commit_graph, store, schema, route, kind)| async move {
                    if route.contradictory {
                        return FILESYSTEM_WORKING_CHANGE_COLS
                            .build(schema, &[])
                            .map_err(batch_error);
                    }
                    let heads = selected_heads(
                        branch_ref.as_ref(),
                        active_branch_id.as_deref(),
                        &route.branch_ids,
                    )
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
                    let mut graph = commit_graph.lock().await;
                    let mut tracked = TrackedStateContext::new().reader(store);
                    let mut rows = Vec::new();
                    for head in heads {
                        let checkpoint_id = latest_checkpoint_for_branch(
                            graph.as_mut(),
                            &mut tracked,
                            &head.commit_id,
                            &head.branch_id,
                        )
                        .await
                        .map_err(lix_error_to_datafusion_error)?
                        .ok_or_else(|| {
                            datafusion::common::DataFusionError::Execution(format!(
                                "branch '{}' has no checkpoint baseline",
                                head.branch_id
                            ))
                        })?;
                        let mut branch_rows = load_rows(
                            &mut tracked,
                            &checkpoint_id.to_string(),
                            &head.commit_id.to_string(),
                            &head.branch_id,
                            kind,
                        )
                        .await
                        .map_err(lix_error_to_datafusion_error)?;
                        if let FileIdConstraint::Ids(ids) = &route.ids {
                            branch_rows.retain(|row| ids.contains(&row.id));
                        }
                        rows.extend(branch_rows);
                        if limit.is_some_and(|limit| rows.len() >= limit) {
                            rows.truncate(limit.unwrap_or(0));
                            break;
                        }
                    }
                    FILESYSTEM_WORKING_CHANGE_COLS
                        .build(schema, &rows)
                        .map_err(batch_error)
                },
            ),
        })
    }
}

#[derive(Clone)]
struct FilesystemWorkingChangeRoute {
    branch_ids: FileIdConstraint,
    ids: FileIdConstraint,
    contradictory: bool,
}

impl FilesystemWorkingChangeRoute {
    fn from_filters(filters: &[Expr]) -> Result<Self> {
        let conjuncts = filter_conjuncts(filters);
        let branch_ids =
            exact_string_column_constraint_from_filters(&conjuncts, "lixcol_branch_id")?;
        let ids = exact_string_column_constraint_from_filters(&conjuncts, "id")?;
        let contradictory =
            matches!(branch_ids, FileIdConstraint::None) || matches!(ids, FileIdConstraint::None);
        Ok(Self {
            branch_ids,
            ids,
            contradictory,
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
struct FileDescriptor {
    id: String,
    directory_id: Option<String>,
    name: String,
}

#[derive(Debug, Clone, Deserialize)]
struct DirectoryDescriptor {
    id: String,
    parent_id: Option<String>,
    name: String,
}

#[derive(Debug, Clone)]
struct LogicalSnapshot {
    files: BTreeMap<String, String>,
    directories: BTreeMap<String, String>,
}

async fn load_rows<S>(
    tracked: &mut TrackedStateStoreReader<S>,
    checkpoint_id: &str,
    head_id: &str,
    branch_id: &str,
    kind: FilesystemWorkingChangeKind,
) -> Result<Vec<FilesystemWorkingChangeSqlRow>, LixError>
where
    S: StorageAdapterRead,
{
    let diff = tracked
        .diff_commits(
            checkpoint_id,
            head_id,
            &TrackedStateDiffRequest {
                filter: TrackedStateFilter {
                    include_tombstones: true,
                    ..TrackedStateFilter::default()
                },
            },
        )
        .await?;
    let mut file_ids = BTreeSet::new();
    let mut directory_ids = BTreeSet::new();
    for entry in &diff.entries {
        if let Some(file_id) = &entry.identity.file_id {
            file_ids.insert(file_id.clone());
        }
        match entry.identity.schema_key.as_str() {
            FILE_DESCRIPTOR_SCHEMA_KEY => {
                if let Some(id) = single_entity_pk_value(&entry.identity.entity_pk) {
                    file_ids.insert(id);
                }
            }
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY => {
                if let Some(id) = single_entity_pk_value(&entry.identity.entity_pk) {
                    directory_ids.insert(id);
                }
            }
            _ => {}
        }
    }
    if diff.entries.is_empty() {
        return Ok(Vec::new());
    }
    if matches!(kind, FilesystemWorkingChangeKind::Directory) && directory_ids.is_empty() {
        return Ok(Vec::new());
    }

    let load_all_files = !directory_ids.is_empty();
    let before = load_logical_snapshot(tracked, checkpoint_id, &file_ids, load_all_files).await?;
    let after = load_logical_snapshot(tracked, head_id, &file_ids, load_all_files).await?;
    let (before_entries, after_entries) = match kind {
        FilesystemWorkingChangeKind::File => (&before.files, &after.files),
        FilesystemWorkingChangeKind::Directory => (&before.directories, &after.directories),
    };
    let ids = before_entries
        .keys()
        .chain(after_entries.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut rows = Vec::new();
    for id in ids {
        let previous_path = before_entries.get(&id).cloned();
        let path = after_entries.get(&id).cloned();
        let directly_changed = match kind {
            FilesystemWorkingChangeKind::File => file_ids.contains(&id),
            FilesystemWorkingChangeKind::Directory => directory_ids.contains(&id),
        };
        if previous_path == path && !directly_changed {
            continue;
        }
        let change_kind = match (
            before_entries.contains_key(&id),
            after_entries.contains_key(&id),
        ) {
            (false, true) => "added",
            (true, false) => "removed",
            _ => "modified",
        };
        rows.push(FilesystemWorkingChangeSqlRow {
            id,
            path,
            previous_path,
            change_kind,
            branch_id: branch_id.to_string(),
        });
    }
    Ok(rows)
}

fn single_entity_pk_value(entity_pk: &EntityPk) -> Option<String> {
    serde_json::from_str::<Vec<String>>(&entity_pk.as_json_array_text().ok()?)
        .ok()?
        .into_iter()
        .next()
}

async fn load_logical_snapshot<S>(
    tracked: &mut TrackedStateStoreReader<S>,
    commit_id: &str,
    selected_file_ids: &BTreeSet<String>,
    load_all_files: bool,
) -> Result<LogicalSnapshot, LixError>
where
    S: StorageAdapterRead,
{
    let file_entity_pks = if load_all_files {
        Vec::new()
    } else {
        selected_file_ids.iter().map(EntityPk::single).collect()
    };
    let files = if !load_all_files && selected_file_ids.is_empty() {
        Vec::new()
    } else {
        scan_descriptors::<S, FileDescriptor>(
            tracked,
            commit_id,
            FILE_DESCRIPTOR_SCHEMA_KEY,
            file_entity_pks,
        )
        .await?
    };
    let directories = if load_all_files {
        scan_descriptors::<S, DirectoryDescriptor>(
            tracked,
            commit_id,
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
            Vec::new(),
        )
        .await?
    } else {
        load_ancestor_directories(tracked, commit_id, &files).await?
    };

    let directory_by_id = directories
        .into_iter()
        .map(|directory| (directory.id.clone(), directory))
        .collect::<BTreeMap<_, _>>();
    let mut directory_paths = BTreeMap::new();
    for id in directory_by_id.keys() {
        resolve_directory_path(
            id,
            &directory_by_id,
            &mut directory_paths,
            &mut BTreeSet::new(),
        )?;
    }
    let mut file_paths = BTreeMap::new();
    for file in files {
        let directory_path = file
            .directory_id
            .as_ref()
            .and_then(|directory_id| directory_paths.get(directory_id))
            .map(String::as_str);
        if file.directory_id.is_some() && directory_path.is_none() {
            continue;
        }
        file_paths.insert(file.id, compose_file_path(directory_path, &file.name)?);
    }
    Ok(LogicalSnapshot {
        files: file_paths,
        directories: directory_paths,
    })
}

async fn load_ancestor_directories<S>(
    tracked: &mut TrackedStateStoreReader<S>,
    commit_id: &str,
    files: &[FileDescriptor],
) -> Result<Vec<DirectoryDescriptor>, LixError>
where
    S: StorageAdapterRead,
{
    let mut pending = files
        .iter()
        .filter_map(|file| file.directory_id.clone())
        .collect::<BTreeSet<_>>();
    let mut requested = BTreeSet::new();
    let mut directories = Vec::new();
    while !pending.is_empty() {
        let ids = std::mem::take(&mut pending)
            .into_iter()
            .filter(|id| requested.insert(id.clone()))
            .collect::<Vec<_>>();
        if ids.is_empty() {
            break;
        }
        let loaded = scan_descriptors::<S, DirectoryDescriptor>(
            tracked,
            commit_id,
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
            ids.into_iter().map(EntityPk::single).collect(),
        )
        .await?;
        pending.extend(
            loaded
                .iter()
                .filter_map(|directory| directory.parent_id.clone())
                .filter(|id| !requested.contains(id)),
        );
        directories.extend(loaded);
    }
    Ok(directories)
}

async fn scan_descriptors<S, T>(
    tracked: &mut TrackedStateStoreReader<S>,
    commit_id: &str,
    schema_key: &str,
    entity_pks: Vec<EntityPk>,
) -> Result<Vec<T>, LixError>
where
    S: StorageAdapterRead,
    T: for<'de> Deserialize<'de>,
{
    tracked
        .scan_rows_at_commit(
            commit_id,
            &TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    schema_keys: vec![schema_key.to_string()],
                    entity_pks,
                    include_tombstones: false,
                    ..TrackedStateFilter::default()
                },
                read_columns: TrackedStateReadColumns {
                    columns: vec!["snapshot_content".to_string()],
                },
                ..TrackedStateScanRequest::default()
            },
        )
        .await?
        .into_iter()
        .filter_map(|row| row.snapshot_content)
        .map(|snapshot| {
            serde_json::from_str(&snapshot).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("invalid {schema_key} snapshot JSON: {error}"),
                )
            })
        })
        .collect()
}

fn resolve_directory_path(
    id: &str,
    directories: &BTreeMap<String, DirectoryDescriptor>,
    paths: &mut BTreeMap<String, String>,
    visiting: &mut BTreeSet<String>,
) -> Result<Option<String>, LixError> {
    if let Some(path) = paths.get(id) {
        return Ok(Some(path.clone()));
    }
    let Some(directory) = directories.get(id) else {
        return Ok(None);
    };
    if !visiting.insert(id.to_string()) {
        return Err(LixError::new(
            LixError::CODE_CONSTRAINT_VIOLATION,
            format!("directory parent cycle while resolving {id:?}"),
        ));
    }
    let parent_path = match directory.parent_id.as_deref() {
        Some(parent_id) => {
            let Some(path) = resolve_directory_path(parent_id, directories, paths, visiting)?
            else {
                visiting.remove(id);
                return Ok(None);
            };
            Some(path)
        }
        None => None,
    };
    let path = compose_directory_path(parent_path.as_deref(), &directory.name)?;
    visiting.remove(id);
    paths.insert(id.to_string(), path.clone());
    Ok(Some(path))
}

#[derive(Debug)]
struct FilesystemWorkingChangeSqlRow {
    id: String,
    path: Option<String>,
    previous_path: Option<String>,
    change_kind: &'static str,
    branch_id: String,
}

static FILESYSTEM_WORKING_CHANGE_COLS: ColumnTable<FilesystemWorkingChangeSqlRow> = ColumnTable {
    columns: &[
        ("id", Col::Utf8(|row| Some(row.id.as_str()))),
        ("path", Col::Utf8(|row| row.path.as_deref())),
        (
            "previous_path",
            Col::Utf8(|row| row.previous_path.as_deref()),
        ),
        ("change_kind", Col::Utf8(|row| Some(row.change_kind))),
        (
            "lixcol_branch_id",
            Col::Utf8(|row| Some(row.branch_id.as_str())),
        ),
    ],
};

pub(crate) fn filesystem_working_change_schema(by_branch: bool) -> SchemaRef {
    let mut fields = vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("path", DataType::Utf8, true),
        Field::new("previous_path", DataType::Utf8, true),
        Field::new("change_kind", DataType::Utf8, false),
    ];
    if by_branch {
        fields.push(Field::new("lixcol_branch_id", DataType::Utf8, false));
    }
    Arc::new(Schema::new(fields))
}

fn batch_error(error: ColumnTableError) -> datafusion::common::DataFusionError {
    match error {
        ColumnTableError::UnsupportedColumn(column) => {
            datafusion::common::DataFusionError::Execution(format!(
                "unsupported filesystem working-change column '{column}'"
            ))
        }
        ColumnTableError::Arrow(error) | ColumnTableError::ArrowZeroColumn(error) => {
            datafusion::common::DataFusionError::from(error)
        }
        ColumnTableError::Row(error) => lix_error_to_datafusion_error(error),
    }
}
