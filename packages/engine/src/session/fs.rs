use serde_json::Value as JsonValue;

use crate::LixError;
use crate::NullableKeyFilter;
use crate::binary_cas::BlobDataReader;
use crate::common::{ParsedFilePath, directory_ancestor_paths, normalize_directory_path};
use crate::filesystem::{
    BlobRefRowInput, DirectoryDeleteInput, DirectoryPathResolver, FileDeleteInput,
    FileDescriptorRowInput, FilePathWriteInput, FilesystemDeletePlan, FilesystemDirEntryKind,
    FilesystemEntry, FilesystemIndex, FilesystemRowContext, blob_ref_row, blob_ref_tombstone_row,
    directory_path_resolvers_from_state_rows, file_descriptor_row, filesystem_conflict_error,
    filesystem_schema_keys, filesystem_storage_scope_key, load_filesystem_index,
    plan_directory_delete, plan_file_delete, plan_file_path_write, plan_recursive_directory_delete,
    wrong_kind_error,
};
use crate::live_state::{
    LiveStateFilter, LiveStateReader, LiveStateScanRequest, MaterializedLiveStateRow,
};
use crate::plugin::{
    InstalledPlugin, is_plugin_storage_path, load_installed_plugins_from_filesystem,
    plugin_state_rows, reject_normal_plugin_storage_mutation, render_materialized_plugin_file,
    select_plugin_for_path,
};
use crate::sql2::SqlWriteExecutionContext;
use crate::storage::{SharedStorageRead, StorageBackend, StorageReadOptions};
use crate::transaction::types::{
    TransactionFileData, TransactionJson, TransactionWrite, TransactionWriteMode,
};

use super::context::SessionContext;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FsWriteOptions {
    pub metadata: Option<JsonValue>,
    pub untracked: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FsMkdirOptions {
    pub metadata: Option<JsonValue>,
    pub untracked: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FsRmOptions {
    pub recursive: bool,
    pub metadata: Option<JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsDirEntry {
    pub name: String,
    pub path: String,
    pub kind: FsDirEntryKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsDirEntryKind {
    File,
    Directory,
}

#[derive(Clone)]
#[expect(missing_debug_implementations)]
pub struct SessionFs<B: StorageBackend = crate::storage::InMemoryStorageBackend> {
    session: SessionContext<B>,
}

impl<B> SessionContext<B>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    pub fn fs(&self) -> SessionFs<B> {
        SessionFs {
            session: self.clone(),
        }
    }
}

impl<B> SessionFs<B>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    pub async fn write_file(
        &self,
        path: &str,
        data: Vec<u8>,
        options: FsWriteOptions,
    ) -> Result<(), LixError> {
        let path = ParsedFilePath::try_from_path(path)?
            .normalized_path
            .to_string();
        reject_normal_plugin_storage_mutation(&path, "fs.write_file")?;
        self.session.with_write_transaction(|transaction| {
            Box::pin(async move {
                let branch_id = transaction.active_branch_id().to_string();
                let rows = scan_filesystem_rows(transaction, &branch_id).await?;
                let filesystem = FilesystemIndex::from_live_rows(rows.clone())?;
                let metadata = transaction_metadata(options.metadata, "fs.write_file metadata")?;

                if let Some(existing) = filesystem.entry(&path) {
                    match existing {
                        FilesystemEntry::Directory(_) => {
                            Err(wrong_kind_error(&path, "file", "directory"))
                        }
                        FilesystemEntry::File(file)
                            if file.scope.untracked != options.untracked =>
                        {
                            Err(filesystem_conflict_error(format!(
                                "fs.write_file cannot write {} path {path:?} over existing {} file",
                                lane_name(options.untracked),
                                lane_name(file.scope.untracked)
                            )))
                        }
                        FilesystemEntry::File(file) => {
                            let mut rows = Vec::new();
                            let context = file.context_with_metadata(metadata);
                            let mut file_data = Vec::new();
                            if data.is_empty() {
                                if file.blob_hash.is_some() {
                                    rows.push(blob_ref_tombstone_row(
                                        file.id.clone(),
                                        context.clone(),
                                    ));
                                }
                            } else {
                                rows.push(blob_ref_row(BlobRefRowInput {
                                    file_id: file.id.clone(),
                                    data: data.clone(),
                                    context: FilesystemRowContext {
                                        file_id: None,
                                        metadata: None,
                                        ..context.clone()
                                    },
                                })?);
                            }
                            file_data.push(TransactionFileData {
                                file_id: file.id.clone(),
                                path: path.clone(),
                                branch_id: context.branch_id.clone(),
                                global: context.global,
                                untracked: context.untracked,
                                data,
                            });
                            if context.metadata.is_some() {
                                rows.push(file_descriptor_row(FileDescriptorRowInput {
                                    id: file.id.clone(),
                                    directory_id: file.directory_id.clone(),
                                    name: file.name.clone(),
                                    context: context.clone(),
                                }));
                            }
                            if rows.is_empty() && file_data.is_empty() {
                                return Ok(());
                            }
                            transaction
                                .stage_write(TransactionWrite::RowsWithFileData {
                                    mode: TransactionWriteMode::Replace,
                                    rows,
                                    file_data,
                                    count: 1,
                                })
                                .await?;
                            Ok(())
                        }
                    }
                } else {
                    if options.untracked {
                        filesystem.reject_tracked_path_collision(&path, "fs.write_file")?;
                    }
                    filesystem.reject_cross_lane_namespace_collision(
                        namespace_paths_for_file_write(&path),
                        options.untracked,
                        "fs.write_file",
                    )?;
                    let mut resolvers = directory_path_resolvers_from_state_rows(rows)?;
                    let context = FilesystemRowContext {
                        branch_id: branch_id.clone(),
                        global: false,
                        untracked: options.untracked,
                        file_id: None,
                        metadata,
                    };
                    let key =
                        filesystem_storage_scope_key(&branch_id, false, options.untracked, None);
                    let resolver = resolvers
                        .entry(key)
                        .or_insert_with(DirectoryPathResolver::default);
                    let plan = plan_file_path_write(
                        resolver,
                        FilePathWriteInput {
                            id: None,
                            path,
                            data: Some(data),
                            context,
                        },
                        &mut || transaction.functions().call_uuid_v7().to_string(),
                    )?;
                    transaction
                        .stage_write(TransactionWrite::RowsWithFileData {
                            mode: TransactionWriteMode::Replace,
                            rows: plan.rows,
                            file_data: plan.file_data,
                            count: plan.count,
                        })
                        .await?;
                    Ok(())
                }
            })
        })
        .await
    }

    pub async fn read_file(&self, path: &str) -> Result<Option<Vec<u8>>, LixError> {
        let path = ParsedFilePath::try_from_path(path)?
            .normalized_path
            .to_string();
        let _operation_guard = self.session.begin_session_operation()?;
        let read = SharedStorageRead::new(
            self.session
                .storage
                .begin_read(StorageReadOptions::default())?,
        );
        let active_branch_id = self.session.active_branch_id_from_reader(&read).await?;
        let live_state = self.session.live_state.reader(&read);
        let filesystem_rows = live_state
            .scan_rows(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: filesystem_schema_keys(),
                    branch_ids: vec![active_branch_id.clone()],
                    ..Default::default()
                },
                ..Default::default()
            })
            .await?;
        let index = FilesystemIndex::from_live_rows(filesystem_rows)?;
        let blob_reader = self.session.binary_cas.reader(read.clone());
        if let Some(bytes) = read_plugin_file_bytes(
            &self.session,
            &path,
            &active_branch_id,
            &index,
            &live_state,
            &blob_reader,
        )
        .await?
        {
            return Ok(Some(bytes));
        }
        index.read_file_bytes(&path, &blob_reader).await
    }

    pub async fn mkdir(&self, path: &str, options: FsMkdirOptions) -> Result<(), LixError> {
        let path = normalize_directory_path(path)?;
        self.session.with_write_transaction(|transaction| {
            Box::pin(async move {
                if path == "/" {
                    return Ok(());
                }
                let branch_id = transaction.active_branch_id().to_string();
                let rows = scan_filesystem_rows(transaction, &branch_id).await?;
                let filesystem = FilesystemIndex::from_live_rows(rows.clone())?;
                if let Some(existing) = filesystem.entry(&path) {
                    return match existing {
                        FilesystemEntry::Directory(directory)
                            if directory.scope.untracked == options.untracked =>
                        {
                            Ok(())
                        }
                        FilesystemEntry::Directory(directory) => Err(filesystem_conflict_error(format!(
                            "fs.mkdir cannot write {} path {path:?} over existing {} directory",
                            lane_name(options.untracked),
                            lane_name(directory.scope.untracked)
                        ))),
                        FilesystemEntry::File(_) => Err(wrong_kind_error(&path, "directory", "file")),
                    };
                }
                if options.untracked {
                    filesystem.reject_tracked_path_collision(&path, "fs.mkdir")?;
                }
                filesystem.reject_cross_lane_namespace_collision(
                    namespace_paths_for_directory_write(&path),
                    options.untracked,
                    "fs.mkdir",
                )?;
                let metadata = transaction_metadata(options.metadata, "fs.mkdir metadata")?;
                let context = FilesystemRowContext {
                    branch_id: branch_id.clone(),
                    global: false,
                    untracked: options.untracked,
                    file_id: None,
                    metadata,
                };
                let mut resolvers = directory_path_resolvers_from_state_rows(rows)?;
                let key = filesystem_storage_scope_key(&branch_id, false, options.untracked, None);
                let resolver = resolvers
                    .entry(key)
                    .or_insert_with(DirectoryPathResolver::default);
                let rows = resolver.create_directory_path_with_leaf_id(
                    &path,
                    None,
                    context,
                    &mut || transaction.functions().call_uuid_v7().to_string(),
                )?;
                if !rows.is_empty() {
                    transaction
                        .stage_write(TransactionWrite::Rows {
                            mode: TransactionWriteMode::Replace,
                            rows,
                        })
                        .await?;
                }
                Ok(())
            })
        })
        .await
    }

    pub async fn readdir(&self, path: &str) -> Result<Option<Vec<FsDirEntry>>, LixError> {
        let path = normalize_directory_path(path)?;
        let _operation_guard = self.session.begin_session_operation()?;
        let read = SharedStorageRead::new(
            self.session
                .storage
                .begin_read(StorageReadOptions::default())?,
        );
        let active_branch_id = self.session.active_branch_id_from_reader(&read).await?;
        let live_state = self.session.live_state.reader(&read);
        let index = load_filesystem_index(&live_state, &active_branch_id).await?;
        index.readdir(&path).map(|entries| {
            entries.map(|entries| {
                entries
                    .into_iter()
                    .map(|entry| FsDirEntry {
                        name: entry.name,
                        path: entry.path,
                        kind: match entry.kind {
                            FilesystemDirEntryKind::File => FsDirEntryKind::File,
                            FilesystemDirEntryKind::Directory => FsDirEntryKind::Directory,
                        },
                    })
                    .collect()
            })
        })
    }

    pub async fn rm(&self, path: &str, options: FsRmOptions) -> Result<(), LixError> {
        let rm_path = normalize_rm_path(path)?;
        if matches!(rm_path, RmPath::Directory(ref path) if path == "/") {
            return Err(LixError::new(
                LixError::CODE_CONSTRAINT_VIOLATION,
                "fs.rm cannot remove the root directory",
            ));
        }
        self.session.with_write_transaction(|transaction| {
            Box::pin(async move {
                let branch_id = transaction.active_branch_id().to_string();
                let rows = scan_filesystem_rows(transaction, &branch_id).await?;
                let filesystem = FilesystemIndex::from_live_rows(rows)?;
                let Some((normalized_path, entry)) = resolve_rm_entry(&filesystem, &rm_path)? else {
                    return Ok(());
                };
                reject_plugin_storage_rm(&normalized_path, entry, &filesystem, options.recursive)?;
                let metadata = transaction_metadata(options.metadata, "fs.rm metadata")?;
                let plan = match entry {
                    FilesystemEntry::File(file) => {
                        let mut context = file.context();
                        context.metadata = metadata;
                        plan_file_delete(FileDeleteInput {
                            file_id: file.id.clone(),
                            has_blob_ref: file.blob_hash.is_some(),
                            context,
                        })
                    }
                    FilesystemEntry::Directory(directory) => {
                        let has_children = filesystem.has_children(directory);
                        if has_children && !options.recursive {
                            return Err(LixError::new(
                                LixError::CODE_CONSTRAINT_VIOLATION,
                                format!("fs.rm cannot remove non-empty directory {normalized_path:?} without recursive=true"),
                            ));
                        }
                        let mut context = directory.context();
                        context.metadata = metadata;
                        if options.recursive {
                            plan_recursive_directory_delete(&directory.id, &filesystem.visible_filesystem(), context)
                        } else {
                            plan_directory_delete(DirectoryDeleteInput {
                                directory_id: directory.id.clone(),
                                context,
                            })
                        }
                    }
                };
                stage_delete_plan(transaction, plan).await
            })
        })
        .await
    }
}

fn reject_plugin_storage_rm(
    normalized_path: &str,
    entry: &FilesystemEntry,
    filesystem: &FilesystemIndex,
    recursive: bool,
) -> Result<(), LixError> {
    reject_normal_plugin_storage_mutation(normalized_path, "fs.rm")?;
    if !recursive || !matches!(entry, FilesystemEntry::Directory(_)) {
        return Ok(());
    }
    if filesystem.file_entries().any(|(path, _)| {
        is_plugin_storage_path(path) && path_is_inside_directory(path, normalized_path)
    }) {
        return reject_normal_plugin_storage_mutation(
            "/.lix/plugins/",
            "fs.rm recursive directory delete",
        );
    }
    Ok(())
}

fn path_is_inside_directory(path: &str, directory_path: &str) -> bool {
    directory_path == "/" || path.starts_with(directory_path)
}

pub(crate) async fn scan_filesystem_rows<B>(
    transaction: &mut crate::transaction::Transaction<B>,
    branch_id: &str,
) -> Result<Vec<MaterializedLiveStateRow>, LixError>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    transaction
        .scan_live_state(&LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: filesystem_schema_keys(),
                branch_ids: vec![branch_id.to_string()],
                ..Default::default()
            },
            ..Default::default()
        })
        .await
}

async fn scan_plugin_state_rows_from_reader(
    live_state: &dyn LiveStateReader,
    branch_id: &str,
    file_id: &str,
    plugin: &InstalledPlugin,
) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
    let rows = live_state
        .scan_rows(&LiveStateScanRequest {
            filter: plugin_state_filter(branch_id, file_id, plugin),
            ..Default::default()
        })
        .await?;
    Ok(plugin_state_rows(plugin, rows.iter()))
}

fn plugin_state_filter(
    branch_id: &str,
    file_id: &str,
    plugin: &InstalledPlugin,
) -> LiveStateFilter {
    LiveStateFilter {
        schema_keys: plugin.schema_keys.clone(),
        branch_ids: vec![branch_id.to_string()],
        file_ids: vec![NullableKeyFilter::Value(file_id.to_string())],
        ..Default::default()
    }
}

async fn read_plugin_file_bytes(
    host: &impl crate::plugin::PluginComponentHost,
    path: &str,
    branch_id: &str,
    index: &FilesystemIndex,
    live_state: &dyn LiveStateReader,
    blob_reader: &dyn BlobDataReader,
) -> Result<Option<Vec<u8>>, LixError> {
    let Some(FilesystemEntry::File(file)) = index.entry(path) else {
        return Ok(None);
    };
    if file.blob_hash.is_some() {
        return Ok(None);
    }

    let installed_plugins = load_installed_plugins_from_filesystem(index, blob_reader).await?;
    let Some(plugin) = select_plugin_for_path(&installed_plugins, path, None) else {
        return Ok(None);
    };

    let active_state =
        scan_plugin_state_rows_from_reader(live_state, branch_id, &file.id, plugin).await?;
    render_materialized_plugin_file(host, plugin, &active_state).await
}

async fn stage_delete_plan<B>(
    transaction: &mut crate::transaction::Transaction<B>,
    plan: FilesystemDeletePlan,
) -> Result<(), LixError>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    if plan.rows.is_empty() {
        return Ok(());
    }
    transaction
        .stage_write(TransactionWrite::Rows {
            mode: TransactionWriteMode::Replace,
            rows: plan.rows,
        })
        .await?;
    Ok(())
}

fn transaction_metadata(
    value: Option<JsonValue>,
    context: &str,
) -> Result<Option<TransactionJson>, LixError> {
    value
        .map(|value| TransactionJson::from_value(value, context))
        .transpose()
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RmPath {
    Directory(String),
    FileOrDirectory(String),
}

fn normalize_rm_path(path: &str) -> Result<RmPath, LixError> {
    if path.ends_with('/') {
        Ok(RmPath::Directory(normalize_directory_path(path)?))
    } else {
        Ok(RmPath::FileOrDirectory(
            ParsedFilePath::try_from_path(path)?
                .normalized_path
                .to_string(),
        ))
    }
}

fn resolve_rm_entry<'a>(
    filesystem: &'a FilesystemIndex,
    path: &RmPath,
) -> Result<Option<(String, &'a FilesystemEntry)>, LixError> {
    match path {
        RmPath::Directory(directory_path) => {
            if let Some(entry @ FilesystemEntry::Directory(_)) = filesystem.entry(directory_path) {
                return Ok(Some((directory_path.clone(), entry)));
            }
            let file_path = directory_path.trim_end_matches('/');
            if matches!(filesystem.entry(file_path), Some(FilesystemEntry::File(_))) {
                return Err(wrong_kind_error(file_path, "directory", "file"));
            }
            Ok(None)
        }
        RmPath::FileOrDirectory(file_path) => {
            if let Some(entry @ FilesystemEntry::File(_)) = filesystem.entry(file_path) {
                return Ok(Some((file_path.clone(), entry)));
            }
            let directory_path = format!("{file_path}/");
            if let Some(entry @ FilesystemEntry::Directory(_)) = filesystem.entry(&directory_path) {
                return Ok(Some((directory_path, entry)));
            }
            Ok(None)
        }
    }
}

fn lane_name(untracked: bool) -> &'static str {
    if untracked { "untracked" } else { "tracked" }
}

fn namespace_paths_for_file_write(path: &str) -> Vec<String> {
    let mut paths = directory_ancestor_paths(path);
    paths.push(path.to_string());
    paths
}

fn namespace_paths_for_directory_write(path: &str) -> Vec<String> {
    let mut paths = directory_ancestor_paths(path);
    paths.push(path.to_string());
    paths
}
