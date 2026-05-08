#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;
use serde_json::{json, Map as JsonMap, Value as JsonValue};

use crate::common::{
    directory_ancestor_paths, directory_name_from_path, normalize_directory_path,
    parent_directory_path, stable_content_fingerprint_hex, ParsedFilePath,
};
use crate::entity_identity::EntityIdentity;
use crate::live_state::MaterializedLiveStateRow;
use crate::LixError;

use super::filesystem_visibility::VisibleFilesystem;
use crate::transaction::types::{TransactionFileData, TransactionJson, TransactionWriteRow};

pub(crate) const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
pub(crate) const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";
pub(crate) const BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";

/// Planned filesystem write output after SQL surface columns have been lowered
/// into state rows and optional file payload writes.
///
/// Providers should emit this shape; transaction/commit code should not need
/// to know whether a row came from `lix_file`, `lix_directory`, or a future
/// filesystem write surface.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct FilesystemWritePlan {
    pub(crate) rows: Vec<TransactionWriteRow>,
    pub(crate) file_data: Vec<TransactionFileData>,
    pub(crate) count: u64,
}

/// Planned filesystem delete output after SQL predicates have selected rows
/// and the surface delete has been lowered into tombstone state rows.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct FilesystemDeletePlan {
    pub(crate) rows: Vec<TransactionWriteRow>,
    pub(crate) count: u64,
}

/// Common state-row lane fields shared by filesystem descriptor/blob rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FilesystemRowContext {
    pub(crate) version_id: String,
    pub(crate) global: bool,
    pub(crate) untracked: bool,
    pub(crate) file_id: Option<String>,
    pub(crate) metadata: Option<TransactionJson>,
}

impl FilesystemRowContext {
    pub(crate) fn active_version(version_id: impl Into<String>) -> Self {
        Self {
            version_id: version_id.into(),
            global: false,
            untracked: false,
            file_id: None,
            metadata: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirectoryDescriptorRowInput {
    pub(crate) id: String,
    pub(crate) parent_id: Option<String>,
    pub(crate) name: String,
    pub(crate) hidden: bool,
    pub(crate) context: FilesystemRowContext,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileDescriptorRowInput {
    pub(crate) id: String,
    pub(crate) directory_id: Option<String>,
    pub(crate) name: String,
    pub(crate) hidden: bool,
    pub(crate) context: FilesystemRowContext,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirectoryDescriptorWriteIntent {
    pub(crate) id: Option<String>,
    pub(crate) parent_id: Option<String>,
    pub(crate) name: String,
    pub(crate) hidden: Option<bool>,
    pub(crate) context: FilesystemRowContext,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileDescriptorWriteIntent {
    pub(crate) id: Option<String>,
    pub(crate) directory_id: Option<String>,
    pub(crate) name: String,
    pub(crate) hidden: Option<bool>,
    pub(crate) context: FilesystemRowContext,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlobRefRowInput {
    pub(crate) file_id: String,
    pub(crate) data: Vec<u8>,
    pub(crate) context: FilesystemRowContext,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FilePathWriteInput {
    pub(crate) id: Option<String>,
    pub(crate) path: String,
    pub(crate) data: Option<Vec<u8>>,
    pub(crate) hidden: Option<bool>,
    pub(crate) context: FilesystemRowContext,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileDeleteInput {
    pub(crate) file_id: String,
    pub(crate) has_blob_ref: bool,
    pub(crate) context: FilesystemRowContext,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirectoryDeleteInput {
    pub(crate) directory_id: String,
    pub(crate) context: FilesystemRowContext,
}

#[derive(Debug, Deserialize)]
struct DirectoryDescriptorSnapshot {
    id: String,
    parent_id: Option<String>,
    name: String,
}

#[derive(Debug, Deserialize)]
struct FileDescriptorSnapshot {
    id: String,
    directory_id: Option<String>,
    name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FilesystemNamespaceEntry {
    Directory(String),
    File(String),
}

/// Resolves directory paths while planning filesystem writes.
///
/// The resolver is seeded from the transaction-visible filesystem state and is
/// then updated as the current statement stages implicit directories. That is
/// what prevents path inserts from restaging committed ancestors or duplicating
/// an ancestor created earlier in the same SQL batch.
#[derive(Debug, Clone, Default)]
pub(crate) struct DirectoryPathResolver {
    directory_ids_by_path: BTreeMap<String, String>,
    entries_by_parent_and_name: BTreeMap<(Option<String>, String), FilesystemNamespaceEntry>,
}

impl DirectoryPathResolver {
    pub(crate) fn from_existing(
        existing_directories: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, LixError> {
        Self::from_existing_filesystem(existing_directories, std::iter::empty())
    }

    pub(crate) fn from_existing_filesystem(
        existing_directories: impl IntoIterator<Item = (String, String)>,
        existing_files: impl IntoIterator<Item = (Option<String>, String, String)>,
    ) -> Result<Self, LixError> {
        let mut directory_ids_by_path = BTreeMap::new();
        for (path, id) in existing_directories {
            directory_ids_by_path.insert(normalize_directory_path(&path)?, id);
        }

        let mut resolver = Self {
            directory_ids_by_path,
            entries_by_parent_and_name: BTreeMap::new(),
        };
        let mut paths = resolver
            .directory_ids_by_path
            .iter()
            .map(|(path, id)| (path.clone(), id.clone()))
            .collect::<Vec<_>>();
        paths.sort_by_key(|(path, _)| path.len());
        for (path, id) in paths {
            let parent_id = parent_directory_path(&path)
                .and_then(|parent_path| resolver.directory_ids_by_path.get(&parent_path).cloned());
            let name = directory_name_from_path(&path).ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("directory path '{path}' does not contain a directory name"),
                )
            })?;
            resolver.reserve_directory(parent_id, name, id)?;
        }
        for (directory_id, entry_name, file_id) in existing_files {
            resolver.reserve_file(directory_id, entry_name, file_id)?;
        }
        Ok(resolver)
    }

    pub(crate) fn directory_id(&self, path: &str) -> Result<Option<&str>, LixError> {
        Ok(self
            .directory_ids_by_path
            .get(&normalize_directory_path(path)?)
            .map(String::as_str))
    }

    /// Stages only the missing descriptors needed for `directory_path`.
    ///
    /// Existing directories keep their original ids. Missing directories receive
    /// deterministic ids so repeated planning of the same transaction-visible
    /// path resolves to the same descriptor identity.
    pub(crate) fn ensure_directory_path(
        &mut self,
        directory_path: &str,
        context: FilesystemRowContext,
        hidden: bool,
        generate_directory_id: &mut dyn FnMut() -> String,
    ) -> Result<Vec<TransactionWriteRow>, LixError> {
        self.ensure_directory_path_with_leaf_id(
            directory_path,
            None,
            context,
            hidden,
            generate_directory_id,
        )
    }

    pub(crate) fn ensure_directory_path_with_leaf_id(
        &mut self,
        directory_path: &str,
        leaf_id: Option<String>,
        context: FilesystemRowContext,
        hidden: bool,
        generate_directory_id: &mut dyn FnMut() -> String,
    ) -> Result<Vec<TransactionWriteRow>, LixError> {
        self.plan_directory_path(
            directory_path,
            leaf_id,
            context,
            hidden,
            generate_directory_id,
            false,
        )
    }

    pub(crate) fn create_directory_path_with_leaf_id(
        &mut self,
        directory_path: &str,
        leaf_id: Option<String>,
        context: FilesystemRowContext,
        hidden: bool,
        generate_directory_id: &mut dyn FnMut() -> String,
    ) -> Result<Vec<TransactionWriteRow>, LixError> {
        self.plan_directory_path(
            directory_path,
            leaf_id,
            context,
            hidden,
            generate_directory_id,
            true,
        )
    }

    fn plan_directory_path(
        &mut self,
        directory_path: &str,
        leaf_id: Option<String>,
        context: FilesystemRowContext,
        hidden: bool,
        generate_directory_id: &mut dyn FnMut() -> String,
        reject_existing_leaf: bool,
    ) -> Result<Vec<TransactionWriteRow>, LixError> {
        let directory_path = normalize_directory_path(directory_path)?;
        if directory_path == "/" {
            if reject_existing_leaf {
                return Err(duplicate_directory_path_error(&directory_path));
            }
            return Ok(Vec::new());
        }

        let mut paths = directory_ancestor_paths(&directory_path);
        paths.push(directory_path.clone());

        let mut rows = Vec::new();
        for path in paths {
            if self.directory_ids_by_path.contains_key(&path) {
                if reject_existing_leaf && path == directory_path {
                    return Err(duplicate_directory_path_error(&directory_path));
                }
                continue;
            }

            let id = if path == directory_path {
                leaf_id.clone().unwrap_or_else(&mut *generate_directory_id)
            } else {
                generate_directory_id()
            };
            let parent_id = parent_directory_path(&path)
                .and_then(|parent_path| self.directory_ids_by_path.get(&parent_path).cloned());
            let name = directory_name_from_path(&path).ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("directory path '{path}' does not contain a directory name"),
                )
            })?;
            self.reserve_directory(parent_id.clone(), name.clone(), id.clone())?;

            rows.push(directory_descriptor_row(DirectoryDescriptorRowInput {
                id: id.clone(),
                parent_id,
                name,
                hidden,
                context: FilesystemRowContext {
                    // Directory descriptors are their own filesystem state row,
                    // even when they are implicitly planned from a file insert.
                    file_id: None,
                    ..context.clone()
                },
            }));
            self.directory_ids_by_path.insert(path, id);
        }

        Ok(rows)
    }

    pub(crate) fn reserve_directory(
        &mut self,
        parent_id: Option<String>,
        name: String,
        directory_id: String,
    ) -> Result<(), LixError> {
        let key = (parent_id, name);
        match self.entries_by_parent_and_name.get(&key) {
            Some(FilesystemNamespaceEntry::Directory(existing_id))
                if existing_id == &directory_id =>
            {
                Ok(())
            }
            Some(existing) => Err(filesystem_namespace_conflict_error(
                &key.0, &key.1, existing,
            )),
            None => {
                self.entries_by_parent_and_name
                    .insert(key, FilesystemNamespaceEntry::Directory(directory_id));
                Ok(())
            }
        }
    }

    pub(crate) fn reserve_file(
        &mut self,
        directory_id: Option<String>,
        entry_name: String,
        file_id: String,
    ) -> Result<(), LixError> {
        let key = (directory_id, entry_name);
        match self.entries_by_parent_and_name.get(&key) {
            Some(FilesystemNamespaceEntry::File(existing_id)) if existing_id == &file_id => Ok(()),
            Some(existing) => Err(filesystem_namespace_conflict_error(
                &key.0, &key.1, existing,
            )),
            None => {
                self.entries_by_parent_and_name
                    .insert(key, FilesystemNamespaceEntry::File(file_id));
                Ok(())
            }
        }
    }
}

fn duplicate_directory_path_error(path: &str) -> LixError {
    LixError::new(
        LixError::CODE_UNIQUE,
        format!("unique constraint violation on lix_directory.path for value {path:?}"),
    )
}

fn filesystem_namespace_conflict_error(
    parent_id: &Option<String>,
    entry_name: &str,
    existing: &FilesystemNamespaceEntry,
) -> LixError {
    let parent = parent_id.as_deref().unwrap_or("<root>");
    let existing_kind = match existing {
        FilesystemNamespaceEntry::Directory(_) => "directory",
        FilesystemNamespaceEntry::File(_) => "file",
    };
    LixError::new(
        LixError::CODE_UNIQUE,
        format!(
            "filesystem namespace conflict: parent {parent:?} already contains {existing_kind} entry {entry_name:?}"
        ),
    )
}

pub(crate) fn directory_descriptor_row(input: DirectoryDescriptorRowInput) -> TransactionWriteRow {
    directory_descriptor_write_row(DirectoryDescriptorWriteIntent {
        id: Some(input.id),
        parent_id: input.parent_id,
        name: input.name,
        hidden: Some(input.hidden),
        context: input.context,
    })
}

pub(crate) fn file_descriptor_row(input: FileDescriptorRowInput) -> TransactionWriteRow {
    file_descriptor_write_row(FileDescriptorWriteIntent {
        id: Some(input.id),
        directory_id: input.directory_id,
        name: input.name,
        hidden: Some(input.hidden),
        context: input.context,
    })
}

pub(crate) fn directory_descriptor_write_row(
    input: DirectoryDescriptorWriteIntent,
) -> TransactionWriteRow {
    let mut snapshot = JsonMap::new();
    if let Some(id) = input.id.as_ref() {
        snapshot.insert("id".to_string(), JsonValue::String(id.clone()));
    }
    snapshot.insert(
        "parent_id".to_string(),
        input
            .parent_id
            .clone()
            .map(JsonValue::String)
            .unwrap_or(JsonValue::Null),
    );
    snapshot.insert("name".to_string(), JsonValue::String(input.name));
    if let Some(hidden) = input.hidden {
        snapshot.insert("hidden".to_string(), JsonValue::Bool(hidden));
    }

    partial_state_row(
        input.id,
        DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
        Some(JsonValue::Object(snapshot)),
        input.context,
    )
}

pub(crate) fn file_descriptor_write_row(input: FileDescriptorWriteIntent) -> TransactionWriteRow {
    let mut snapshot = JsonMap::new();
    if let Some(id) = input.id.as_ref() {
        snapshot.insert("id".to_string(), JsonValue::String(id.clone()));
    }
    snapshot.insert(
        "directory_id".to_string(),
        input
            .directory_id
            .clone()
            .map(JsonValue::String)
            .unwrap_or(JsonValue::Null),
    );
    snapshot.insert("name".to_string(), JsonValue::String(input.name));
    if let Some(hidden) = input.hidden {
        snapshot.insert("hidden".to_string(), JsonValue::Bool(hidden));
    }

    partial_state_row(
        input.id,
        FILE_DESCRIPTOR_SCHEMA_KEY,
        Some(JsonValue::Object(snapshot)),
        input.context,
    )
}

pub(crate) fn blob_ref_row(input: BlobRefRowInput) -> Result<TransactionWriteRow, LixError> {
    let size_bytes = u64::try_from(input.data.len()).map_err(|_| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "binary blob size exceeds supported range for file '{}' version '{}'",
                input.file_id, input.context.version_id
            ),
        )
    })?;
    let snapshot = json!({
        "id": input.file_id.clone(),
        "blob_hash": stable_content_fingerprint_hex(&input.data),
        "size_bytes": size_bytes,
    });

    Ok(state_row(
        input.file_id.clone(),
        BLOB_REF_SCHEMA_KEY,
        Some(snapshot),
        FilesystemRowContext {
            file_id: Some(input.file_id),
            ..input.context
        },
    ))
}

pub(crate) fn plan_file_path_write(
    resolver: &mut DirectoryPathResolver,
    input: FilePathWriteInput,
    generate_directory_id: &mut dyn FnMut() -> String,
) -> Result<FilesystemWritePlan, LixError> {
    let parsed = ParsedFilePath::try_from_path(&input.path)?;
    let mut rows = Vec::new();
    let file_id = input.id.unwrap_or_else(&mut *generate_directory_id);

    let directory_id = match parsed.directory_path.as_ref() {
        Some(directory_path) => {
            rows.extend(resolver.ensure_directory_path(
                directory_path.as_str(),
                input.context.clone(),
                false,
                generate_directory_id,
            )?);
            resolver
                .directory_id(directory_path.as_str())?
                .map(ToOwned::to_owned)
        }
        None => None,
    };

    resolver.reserve_file(directory_id.clone(), parsed.name.clone(), file_id.clone())?;
    rows.push(file_descriptor_row(FileDescriptorRowInput {
        id: file_id.clone(),
        directory_id,
        name: parsed.name.clone(),
        hidden: input.hidden.unwrap_or(false),
        context: input.context.clone(),
    }));

    let mut file_data = Vec::new();
    if let Some(data) = input.data {
        rows.push(blob_ref_row(BlobRefRowInput {
            file_id: file_id.clone(),
            data: data.clone(),
            context: FilesystemRowContext {
                file_id: None,
                metadata: None,
                ..input.context.clone()
            },
        })?);
        file_data.push(TransactionFileData {
            file_id,
            version_id: input.context.version_id,
            untracked: input.context.untracked,
            data,
        });
    }

    Ok(FilesystemWritePlan {
        rows,
        file_data,
        count: 1,
    })
}

pub(crate) fn plan_file_path_update(
    resolver: &mut DirectoryPathResolver,
    existing_file_id: String,
    new_path: String,
    existing_hidden: bool,
    _existing_data: Option<Vec<u8>>,
    context: FilesystemRowContext,
    generate_directory_id: &mut dyn FnMut() -> String,
) -> Result<FilesystemWritePlan, LixError> {
    let parsed = ParsedFilePath::try_from_path(&new_path)?;
    let mut rows = Vec::new();

    let directory_id = match parsed.directory_path.as_ref() {
        Some(directory_path) => {
            rows.extend(resolver.ensure_directory_path(
                directory_path.as_str(),
                context.clone(),
                false,
                generate_directory_id,
            )?);
            resolver
                .directory_id(directory_path.as_str())?
                .map(ToOwned::to_owned)
        }
        None => None,
    };

    resolver.reserve_file(
        directory_id.clone(),
        parsed.name.clone(),
        existing_file_id.clone(),
    )?;
    rows.push(file_descriptor_row(FileDescriptorRowInput {
        id: existing_file_id,
        directory_id,
        name: parsed.name.clone(),
        hidden: existing_hidden,
        context,
    }));

    // Data/blob-ref state is intentionally left untouched for path-only
    // updates. A provider should plan blob rows only when `data` is assigned.
    Ok(FilesystemWritePlan {
        rows,
        file_data: Vec::new(),
        count: 1,
    })
}

pub(crate) fn plan_file_delete(input: FileDeleteInput) -> FilesystemDeletePlan {
    let mut rows = vec![tombstone_row(
        input.file_id.clone(),
        FILE_DESCRIPTOR_SCHEMA_KEY,
        FilesystemRowContext {
            file_id: None,
            ..input.context.clone()
        },
    )];

    if input.has_blob_ref {
        rows.push(tombstone_row(
            input.file_id.clone(),
            BLOB_REF_SCHEMA_KEY,
            FilesystemRowContext {
                file_id: Some(input.file_id),
                metadata: None,
                ..input.context
            },
        ));
    }

    FilesystemDeletePlan { rows, count: 1 }
}

pub(crate) fn plan_directory_delete(input: DirectoryDeleteInput) -> FilesystemDeletePlan {
    FilesystemDeletePlan {
        rows: vec![tombstone_row(
            input.directory_id,
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
            FilesystemRowContext {
                file_id: None,
                ..input.context
            },
        )],
        count: 1,
    }
}

pub(crate) fn plan_recursive_directory_delete(
    root_directory_id: &str,
    visible_filesystem: &VisibleFilesystem,
    context: FilesystemRowContext,
) -> FilesystemDeletePlan {
    let mut rows = Vec::new();
    let mut count = 0;

    collect_recursive_directory_delete(
        root_directory_id,
        visible_filesystem,
        &context,
        &mut rows,
        &mut count,
    );

    FilesystemDeletePlan { rows, count }
}

pub(crate) fn directory_path_resolvers_from_state_rows(
    rows: Vec<MaterializedLiveStateRow>,
) -> Result<BTreeMap<String, DirectoryPathResolver>, LixError> {
    let mut directory_rows = BTreeMap::<String, BTreeMap<String, DirectoryDescriptorSeed>>::new();
    let mut file_rows = BTreeMap::<String, Vec<(Option<String>, String, String)>>::new();
    for row in rows {
        let Some(snapshot_content) = row.snapshot_content.as_deref() else {
            continue;
        };
        let resolver_key = filesystem_storage_scope_key(
            &row.version_id,
            row.global,
            row.untracked,
            row.file_id.as_deref(),
        );
        match row.schema_key.as_str() {
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY => {
                let snapshot: DirectoryDescriptorSnapshot = serde_json::from_str(snapshot_content)
                    .map_err(|error| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!("invalid lix_directory_descriptor snapshot JSON: {error}"),
                        )
                    })?;
                directory_rows.entry(resolver_key).or_default().insert(
                    snapshot.id.clone(),
                    DirectoryDescriptorSeed {
                        id: snapshot.id,
                        parent_id: snapshot.parent_id,
                        name: snapshot.name,
                    },
                );
            }
            FILE_DESCRIPTOR_SCHEMA_KEY => {
                let snapshot: FileDescriptorSnapshot = serde_json::from_str(snapshot_content)
                    .map_err(|error| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!("invalid lix_file_descriptor snapshot JSON: {error}"),
                        )
                    })?;
                file_rows.entry(resolver_key).or_default().push((
                    snapshot.directory_id,
                    snapshot.name,
                    snapshot.id,
                ));
            }
            _ => {}
        }
    }

    let mut resolvers = BTreeMap::new();
    for (version_id, records) in directory_rows {
        let mut paths = BTreeMap::<String, String>::new();
        for directory_id in records.keys() {
            resolve_directory_seed_path(directory_id, &records, &mut paths, &mut BTreeSet::new())?;
        }
        let seeds = paths
            .into_iter()
            .map(|(directory_id, path)| (path, directory_id))
            .collect::<Vec<_>>();
        let files = file_rows.remove(&version_id).unwrap_or_default();
        resolvers.insert(
            version_id,
            DirectoryPathResolver::from_existing_filesystem(seeds, files)?,
        );
    }
    for (version_id, files) in file_rows {
        resolvers.insert(
            version_id,
            DirectoryPathResolver::from_existing_filesystem(std::iter::empty(), files)?,
        );
    }
    Ok(resolvers)
}

pub(crate) fn filesystem_storage_scope_key(
    version_id: &str,
    global: bool,
    untracked: bool,
    file_id: Option<&str>,
) -> String {
    format!(
        "version={version_id}\0global={global}\0untracked={untracked}\0file_id={}",
        file_id.unwrap_or("<null>")
    )
}

#[derive(Debug, Clone)]
struct DirectoryDescriptorSeed {
    id: String,
    parent_id: Option<String>,
    name: String,
}

fn resolve_directory_seed_path(
    directory_id: &str,
    records: &BTreeMap<String, DirectoryDescriptorSeed>,
    paths: &mut BTreeMap<String, String>,
    visiting: &mut BTreeSet<String>,
) -> Result<Option<String>, LixError> {
    if let Some(path) = paths.get(directory_id) {
        return Ok(Some(path.clone()));
    }
    if !visiting.insert(directory_id.to_string()) {
        return Err(directory_parent_cycle_error(directory_id));
    }
    let Some(row) = records.get(directory_id) else {
        visiting.remove(directory_id);
        return Ok(None);
    };
    let path = match row.parent_id.as_deref() {
        Some(parent_id) => {
            let Some(parent_path) =
                resolve_directory_seed_path(parent_id, records, paths, visiting)?
            else {
                visiting.remove(directory_id);
                return Ok(None);
            };
            format!("{parent_path}{}/", row.name)
        }
        None => format!("/{}/", row.name),
    };
    visiting.remove(directory_id);
    paths.insert(row.id.clone(), path.clone());
    Ok(Some(path))
}

fn directory_parent_cycle_error(directory_id: &str) -> LixError {
    LixError::new(
        LixError::CODE_CONSTRAINT_VIOLATION,
        format!(
            "lix_directory_descriptor parent_id cycle detected while resolving directory '{directory_id}'"
        ),
    )
}

fn state_row(
    entity_id: String,
    schema_key: &str,
    snapshot: Option<JsonValue>,
    context: FilesystemRowContext,
) -> TransactionWriteRow {
    partial_state_row(Some(entity_id), schema_key, snapshot, context)
}

fn partial_state_row(
    entity_id: Option<String>,
    schema_key: &str,
    snapshot: Option<JsonValue>,
    context: FilesystemRowContext,
) -> TransactionWriteRow {
    let snapshot = snapshot.map(TransactionJson::from_value_unchecked);
    TransactionWriteRow {
        entity_id: entity_id.map(EntityIdentity::single),
        schema_key: schema_key.to_string(),
        file_id: context.file_id,
        snapshot,
        metadata: context.metadata,
        origin: None,
        created_at: None,
        updated_at: None,
        global: context.global,
        change_id: None,
        commit_id: None,
        untracked: context.untracked,
        version_id: context.version_id,
    }
}

fn tombstone_row(
    entity_id: String,
    schema_key: &str,
    context: FilesystemRowContext,
) -> TransactionWriteRow {
    state_row(entity_id, schema_key, None, context)
}

fn collect_recursive_directory_delete(
    directory_id: &str,
    visible_filesystem: &VisibleFilesystem,
    context: &FilesystemRowContext,
    rows: &mut Vec<TransactionWriteRow>,
    count: &mut u64,
) {
    if let Some(child_ids) = visible_filesystem
        .directory_children_by_parent_id
        .get(&Some(directory_id.to_string()))
    {
        for child_id in child_ids {
            collect_recursive_directory_delete(child_id, visible_filesystem, context, rows, count);
        }
    }

    if let Some(files) = visible_filesystem
        .files_by_directory_id
        .get(&Some(directory_id.to_string()))
    {
        for file_id in files.keys() {
            let plan = plan_file_delete(FileDeleteInput {
                file_id: file_id.clone(),
                has_blob_ref: visible_filesystem
                    .blob_refs_by_file_id
                    .contains_key(file_id),
                context: context.clone(),
            });
            rows.extend(plan.rows);
            *count += plan.count;
        }
    }

    let plan = plan_directory_delete(DirectoryDeleteInput {
        directory_id: directory_id.to_string(),
        context: context.clone(),
    });
    rows.extend(plan.rows);
    *count += plan.count;
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use serde_json::Value as JsonValue;

    use super::{
        blob_ref_row, directory_descriptor_row, file_descriptor_row, plan_file_path_update,
        plan_file_path_write, BlobRefRowInput, DirectoryDeleteInput, DirectoryDescriptorRowInput,
        DirectoryPathResolver, FileDeleteInput, FileDescriptorRowInput, FilePathWriteInput,
        FilesystemRowContext,
    };
    use crate::sql2::filesystem_visibility::{
        VisibleBlobRef, VisibleDirectory, VisibleFile, VisibleFilesystem,
    };
    use crate::{entity_identity::EntityIdentity, live_state::MaterializedLiveStateRow};

    fn test_id_generator(ids: &'static [&'static str]) -> impl FnMut() -> String {
        let mut ids = ids.iter();
        move || ids.next().expect("test id should exist").to_string()
    }

    #[test]
    fn directory_descriptor_row_builds_state_row() {
        let row = directory_descriptor_row(DirectoryDescriptorRowInput {
            id: "dir-docs".to_string(),
            parent_id: None,
            name: "docs".to_string(),
            hidden: false,
            context: FilesystemRowContext::active_version("version-a"),
        });

        assert_eq!(
            row.entity_id.as_ref(),
            Some(&crate::entity_identity::EntityIdentity::single("dir-docs"))
        );
        assert_eq!(row.schema_key, "lix_directory_descriptor");
        assert_eq!(row.version_id, "version-a");
        let snapshot: JsonValue = row.snapshot.as_ref().unwrap().value().clone();
        assert_eq!(snapshot["id"], "dir-docs");
        assert_eq!(snapshot["parent_id"], JsonValue::Null);
        assert_eq!(snapshot["name"], "docs");
        assert_eq!(snapshot["hidden"], false);
    }

    #[test]
    fn file_descriptor_row_builds_state_row() {
        let row = file_descriptor_row(FileDescriptorRowInput {
            id: "file-readme".to_string(),
            directory_id: Some("dir-docs".to_string()),
            name: "readme.md".to_string(),
            hidden: false,
            context: FilesystemRowContext::active_version("version-a"),
        });

        assert_eq!(
            row.entity_id.as_ref(),
            Some(&crate::entity_identity::EntityIdentity::single(
                "file-readme"
            ))
        );
        assert_eq!(row.schema_key, "lix_file_descriptor");
        let snapshot: JsonValue = row.snapshot.as_ref().unwrap().value().clone();
        assert_eq!(snapshot["directory_id"], "dir-docs");
        assert_eq!(snapshot["name"], "readme.md");
    }

    #[test]
    fn blob_ref_row_builds_state_row() {
        let row = blob_ref_row(BlobRefRowInput {
            file_id: "file-readme".to_string(),
            data: b"Hello".to_vec(),
            context: FilesystemRowContext::active_version("version-a"),
        })
        .expect("blob ref row should build");

        assert_eq!(
            row.entity_id.as_ref(),
            Some(&crate::entity_identity::EntityIdentity::single(
                "file-readme"
            ))
        );
        assert_eq!(row.file_id.as_deref(), Some("file-readme"));
        assert_eq!(row.schema_key, "lix_binary_blob_ref");
        let snapshot: JsonValue = row.snapshot.as_ref().unwrap().value().clone();
        assert_eq!(snapshot["id"], "file-readme");
        assert_eq!(snapshot["size_bytes"], 5);
        assert!(snapshot["blob_hash"]
            .as_str()
            .is_some_and(|hash| !hash.is_empty()));
    }

    #[test]
    fn directory_path_resolver_reuses_existing_ancestor() {
        let mut resolver =
            DirectoryPathResolver::from_existing([("/docs/".to_string(), "dir-docs".to_string())])
                .expect("existing directories should normalize");

        let rows = resolver
            .ensure_directory_path(
                "/docs/nested/",
                FilesystemRowContext::active_version("version-a"),
                false,
                &mut test_id_generator(&["dir-generated-nested"]),
            )
            .expect("directory path should plan");

        assert_eq!(rows.len(), 1);
        assert_eq!(resolver.directory_id("/docs/").unwrap(), Some("dir-docs"));
        assert_eq!(
            resolver.directory_id("/docs/nested/").unwrap(),
            Some("dir-generated-nested")
        );

        let snapshot: JsonValue = rows[0].snapshot.as_ref().unwrap().value().clone();
        assert_eq!(snapshot["id"], "dir-generated-nested");
        assert_eq!(snapshot["parent_id"], "dir-docs");
        assert_eq!(snapshot["name"], "nested");
    }

    #[test]
    fn directory_path_resolver_reuses_ancestor_staged_in_same_batch() {
        let mut resolver =
            DirectoryPathResolver::from_existing([]).expect("empty resolver should build");

        let docs_rows = resolver
            .ensure_directory_path(
                "/docs/",
                FilesystemRowContext::active_version("version-a"),
                false,
                &mut test_id_generator(&["dir-generated-docs"]),
            )
            .expect("top-level directory should plan");
        assert_eq!(docs_rows.len(), 1);

        let nested_rows = resolver
            .ensure_directory_path(
                "/docs/nested/",
                FilesystemRowContext::active_version("version-a"),
                false,
                &mut test_id_generator(&["dir-generated-nested"]),
            )
            .expect("nested directory should plan");

        assert_eq!(nested_rows.len(), 1);
        let snapshot: JsonValue = nested_rows[0].snapshot.as_ref().unwrap().value().clone();
        assert_eq!(snapshot["id"], "dir-generated-nested");
        assert_eq!(snapshot["parent_id"], "dir-generated-docs");
        assert_eq!(snapshot["name"], "nested");
    }

    #[test]
    fn directory_path_resolver_uses_explicit_leaf_id() {
        let mut resolver =
            DirectoryPathResolver::from_existing([]).expect("empty resolver should build");

        let rows = resolver
            .ensure_directory_path_with_leaf_id(
                "/docs/nested/",
                Some("dir-nested".to_string()),
                FilesystemRowContext::active_version("version-a"),
                false,
                &mut test_id_generator(&["dir-generated-docs"]),
            )
            .expect("directory path should plan");

        assert_eq!(rows.len(), 2);
        assert_eq!(
            resolver.directory_id("/docs/").unwrap(),
            Some("dir-generated-docs")
        );
        assert_eq!(
            resolver.directory_id("/docs/nested/").unwrap(),
            Some("dir-nested")
        );

        let snapshot: JsonValue = rows[1].snapshot.as_ref().unwrap().value().clone();
        assert_eq!(snapshot["id"], "dir-nested");
        assert_eq!(snapshot["parent_id"], "dir-generated-docs");
        assert_eq!(snapshot["name"], "nested");
    }

    #[test]
    fn directory_path_resolver_does_not_restage_same_path() {
        let mut resolver =
            DirectoryPathResolver::from_existing([]).expect("empty resolver should build");

        let rows = resolver
            .ensure_directory_path(
                "/docs/nested/",
                FilesystemRowContext::active_version("version-a"),
                false,
                &mut test_id_generator(&["dir-generated-docs", "dir-generated-nested"]),
            )
            .expect("directory path should plan");
        assert_eq!(rows.len(), 2);

        let rows = resolver
            .ensure_directory_path(
                "/docs/nested/",
                FilesystemRowContext::active_version("version-a"),
                false,
                &mut test_id_generator(&["should-not-be-used"]),
            )
            .expect("directory path should plan");
        assert!(rows.is_empty());
    }

    #[test]
    fn file_path_write_stages_missing_directories_file_blob_and_payload() {
        let mut resolver =
            DirectoryPathResolver::from_existing([]).expect("empty resolver should build");

        let plan = plan_file_path_write(
            &mut resolver,
            FilePathWriteInput {
                id: Some("file-readme".to_string()),
                path: "/docs/guides/readme.md".to_string(),
                data: Some(b"hello".to_vec()),
                hidden: Some(false),
                context: FilesystemRowContext::active_version("version-a"),
            },
            &mut test_id_generator(&["dir-generated-docs", "dir-generated-guides"]),
        )
        .expect("file path write should plan");

        assert_eq!(plan.count, 1);
        assert_eq!(plan.file_data.len(), 1);
        assert_eq!(plan.file_data[0].file_id, "file-readme");
        assert_eq!(plan.file_data[0].version_id, "version-a");
        assert_eq!(plan.file_data[0].data, b"hello");
        assert_eq!(plan.rows.len(), 4);
        assert_eq!(
            plan.rows
                .iter()
                .filter(|row| row.schema_key == "lix_directory_descriptor")
                .count(),
            2
        );
        assert!(plan
            .rows
            .iter()
            .any(|row| row.schema_key == "lix_binary_blob_ref"));

        let file_row = plan
            .rows
            .iter()
            .find(|row| row.schema_key == "lix_file_descriptor")
            .expect("file descriptor row should be planned");
        let snapshot: JsonValue = file_row.snapshot.as_ref().unwrap().value().clone();
        assert_eq!(snapshot["id"], "file-readme");
        assert_eq!(snapshot["directory_id"], "dir-generated-guides");
        assert_eq!(snapshot["name"], "readme.md");
    }

    #[test]
    fn file_path_write_reuses_existing_parent_directory() {
        let mut resolver = DirectoryPathResolver::from_existing([
            ("/docs/".to_string(), "dir-docs".to_string()),
            ("/docs/guides/".to_string(), "dir-guides".to_string()),
        ])
        .expect("existing directories should seed");

        let plan = plan_file_path_write(
            &mut resolver,
            FilePathWriteInput {
                id: Some("file-readme".to_string()),
                path: "/docs/guides/readme.md".to_string(),
                data: Some(b"hello".to_vec()),
                hidden: Some(false),
                context: FilesystemRowContext::active_version("version-a"),
            },
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect("file path write should plan");

        assert_eq!(plan.rows.len(), 2);
        assert_eq!(
            plan.rows
                .iter()
                .filter(|row| row.schema_key == "lix_directory_descriptor")
                .count(),
            0
        );
        let file_row = plan
            .rows
            .iter()
            .find(|row| row.schema_key == "lix_file_descriptor")
            .expect("file descriptor row should be planned");
        let snapshot: JsonValue = file_row.snapshot.as_ref().unwrap().value().clone();
        assert_eq!(snapshot["directory_id"], "dir-guides");
    }

    #[test]
    fn file_path_update_reuses_existing_parent_and_preserves_data() {
        let mut resolver =
            DirectoryPathResolver::from_existing([("/docs/".to_string(), "dir-docs".to_string())])
                .expect("existing directories should seed");

        let plan = plan_file_path_update(
            &mut resolver,
            "file-readme".to_string(),
            "/docs/renamed.md".to_string(),
            false,
            Some(b"hello".to_vec()),
            FilesystemRowContext::active_version("version-a"),
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect("file path update should plan");

        assert_eq!(plan.count, 1);
        assert!(plan.file_data.is_empty());
        assert_eq!(plan.rows.len(), 1);
        assert!(plan
            .rows
            .iter()
            .all(|row| row.schema_key != "lix_binary_blob_ref"));

        let snapshot: JsonValue = plan.rows[0].snapshot.as_ref().unwrap().value().clone();
        assert_eq!(snapshot["id"], "file-readme");
        assert_eq!(snapshot["directory_id"], "dir-docs");
        assert_eq!(snapshot["name"], "renamed.md");
        assert_eq!(snapshot["hidden"], false);
    }

    #[test]
    fn file_path_update_stages_missing_parent_directories() {
        let mut resolver =
            DirectoryPathResolver::from_existing([]).expect("empty resolver should build");

        let plan = plan_file_path_update(
            &mut resolver,
            "file-readme".to_string(),
            "/docs/guides/readme.md".to_string(),
            true,
            Some(b"hello".to_vec()),
            FilesystemRowContext::active_version("version-a"),
            &mut test_id_generator(&["dir-generated-docs", "dir-generated-guides"]),
        )
        .expect("file path update should plan");

        assert_eq!(plan.count, 1);
        assert!(plan.file_data.is_empty());
        assert_eq!(plan.rows.len(), 3);
        assert_eq!(
            plan.rows
                .iter()
                .filter(|row| row.schema_key == "lix_directory_descriptor")
                .count(),
            2
        );
        assert!(plan
            .rows
            .iter()
            .all(|row| row.schema_key != "lix_binary_blob_ref"));

        let file_row = plan
            .rows
            .iter()
            .find(|row| row.schema_key == "lix_file_descriptor")
            .expect("file descriptor row should be planned");
        let snapshot: JsonValue = file_row.snapshot.as_ref().unwrap().value().clone();
        assert_eq!(snapshot["directory_id"], "dir-generated-guides");
        assert_eq!(snapshot["name"], "readme.md");
        assert_eq!(snapshot["hidden"], true);
    }

    #[test]
    fn directory_path_resolvers_from_state_rows_derives_nested_paths() {
        let resolvers = super::directory_path_resolvers_from_state_rows(vec![
            live_directory_row(
                "dir-docs",
                "version-a",
                "{\"id\":\"dir-docs\",\"parent_id\":null,\"name\":\"docs\"}",
            ),
            live_directory_row(
                "dir-guides",
                "version-a",
                "{\"id\":\"dir-guides\",\"parent_id\":\"dir-docs\",\"name\":\"guides\"}",
            ),
        ])
        .expect("state rows should seed directory resolvers");

        let resolver = resolvers
            .get(&super::filesystem_storage_scope_key(
                "version-a",
                false,
                false,
                None,
            ))
            .expect("storage-scope resolver should exist");
        assert_eq!(resolver.directory_id("/docs/").unwrap(), Some("dir-docs"));
        assert_eq!(
            resolver.directory_id("/docs/guides/").unwrap(),
            Some("dir-guides")
        );
    }

    #[test]
    fn file_delete_plans_descriptor_and_blob_ref_tombstones() {
        let plan = super::plan_file_delete(FileDeleteInput {
            file_id: "file-readme".to_string(),
            has_blob_ref: true,
            context: FilesystemRowContext::active_version("version-a"),
        });

        assert_eq!(plan.count, 1);
        assert_eq!(plan.rows.len(), 2);
        let descriptor = plan
            .rows
            .iter()
            .find(|row| row.schema_key == "lix_file_descriptor")
            .expect("file descriptor tombstone should be planned");
        assert_eq!(
            descriptor.entity_id.as_ref(),
            Some(&crate::entity_identity::EntityIdentity::single(
                "file-readme"
            ))
        );
        assert_eq!(descriptor.file_id, None);
        assert_eq!(descriptor.snapshot, None);

        let blob_ref = plan
            .rows
            .iter()
            .find(|row| row.schema_key == "lix_binary_blob_ref")
            .expect("blob ref tombstone should be planned");
        assert_eq!(
            blob_ref.entity_id.as_ref(),
            Some(&crate::entity_identity::EntityIdentity::single(
                "file-readme"
            ))
        );
        assert_eq!(blob_ref.file_id.as_deref(), Some("file-readme"));
        assert_eq!(blob_ref.snapshot, None);
    }

    #[test]
    fn file_delete_without_blob_ref_plans_only_descriptor_tombstone() {
        let plan = super::plan_file_delete(FileDeleteInput {
            file_id: "file-readme".to_string(),
            has_blob_ref: false,
            context: FilesystemRowContext::active_version("version-a"),
        });

        assert_eq!(plan.count, 1);
        assert_eq!(plan.rows.len(), 1);
        assert_eq!(plan.rows[0].schema_key, "lix_file_descriptor");
        assert_eq!(plan.rows[0].snapshot, None);
    }

    #[test]
    fn directory_delete_plans_descriptor_tombstone() {
        let plan = super::plan_directory_delete(DirectoryDeleteInput {
            directory_id: "dir-docs".to_string(),
            context: FilesystemRowContext::active_version("version-a"),
        });

        assert_eq!(plan.count, 1);
        assert_eq!(plan.rows.len(), 1);
        assert_eq!(
            plan.rows[0].entity_id.as_ref(),
            Some(&crate::entity_identity::EntityIdentity::single("dir-docs"))
        );
        assert_eq!(plan.rows[0].schema_key, "lix_directory_descriptor");
        assert_eq!(plan.rows[0].file_id, None);
        assert_eq!(plan.rows[0].snapshot, None);
    }

    #[test]
    fn recursive_directory_delete_plans_files_blobs_and_deepest_directories_first() {
        let context = FilesystemRowContext::active_version("version-a");
        let mut directories_by_id = BTreeMap::new();
        directories_by_id.insert(
            "dir-docs".to_string(),
            visible_directory("dir-docs", None, "docs", context.clone()),
        );
        directories_by_id.insert(
            "dir-guides".to_string(),
            visible_directory("dir-guides", Some("dir-docs"), "guides", context.clone()),
        );

        let mut directory_children_by_parent_id = BTreeMap::new();
        directory_children_by_parent_id.insert(
            Some("dir-docs".to_string()),
            BTreeSet::from(["dir-guides".to_string()]),
        );

        let mut files_by_directory_id = BTreeMap::new();
        files_by_directory_id.insert(
            Some("dir-guides".to_string()),
            BTreeMap::from([(
                "file-readme".to_string(),
                visible_file("file-readme", Some("dir-guides"), "readme", context.clone()),
            )]),
        );
        files_by_directory_id.insert(
            Some("dir-docs".to_string()),
            BTreeMap::from([(
                "file-index".to_string(),
                visible_file("file-index", Some("dir-docs"), "index", context.clone()),
            )]),
        );

        let visible_filesystem = VisibleFilesystem {
            directories_by_id,
            directory_children_by_parent_id,
            files_by_directory_id,
            blob_refs_by_file_id: BTreeMap::from([(
                "file-readme".to_string(),
                visible_blob_ref("file-readme", context.clone()),
            )]),
        };

        let plan = super::plan_recursive_directory_delete("dir-docs", &visible_filesystem, context);

        assert_eq!(plan.count, 4);
        assert_eq!(
            plan.rows
                .iter()
                .map(|row| {
                    (
                        row.schema_key.as_str(),
                        row.entity_id
                            .as_ref()
                            .expect("planned recursive delete row should carry entity_id")
                            .as_single_string_owned()
                            .expect("planned recursive delete row should project entity_id"),
                    )
                })
                .collect::<Vec<_>>(),
            vec![
                ("lix_file_descriptor", "file-readme".to_string()),
                ("lix_binary_blob_ref", "file-readme".to_string()),
                ("lix_directory_descriptor", "dir-guides".to_string()),
                ("lix_file_descriptor", "file-index".to_string()),
                ("lix_directory_descriptor", "dir-docs".to_string()),
            ]
        );
        assert!(plan.rows.iter().all(|row| row.snapshot.is_none()));
    }

    fn visible_directory(
        id: &str,
        parent_id: Option<&str>,
        name: &str,
        context: FilesystemRowContext,
    ) -> VisibleDirectory {
        VisibleDirectory {
            id: id.to_string(),
            parent_id: parent_id.map(ToOwned::to_owned),
            name: name.to_string(),
            hidden: false,
            context,
        }
    }

    fn visible_file(
        id: &str,
        directory_id: Option<&str>,
        name: &str,
        context: FilesystemRowContext,
    ) -> VisibleFile {
        VisibleFile {
            id: id.to_string(),
            directory_id: directory_id.map(ToOwned::to_owned),
            name: name.to_string(),
            hidden: false,
            context,
        }
    }

    fn visible_blob_ref(file_id: &str, context: FilesystemRowContext) -> VisibleBlobRef {
        VisibleBlobRef {
            file_id: file_id.to_string(),
            blob_hash: format!("hash-{file_id}"),
            size_bytes: Some(1),
            context,
        }
    }

    fn live_directory_row(
        entity_id: &str,
        version_id: &str,
        snapshot_content: &str,
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_id: EntityIdentity::single(entity_id),
            schema_key: "lix_directory_descriptor".to_string(),
            file_id: None,
            snapshot_content: Some(snapshot_content.to_string()),
            metadata: None,
            version_id: version_id.to_string(),
            change_id: Some(format!("change-{entity_id}")),
            commit_id: Some(format!("commit-{entity_id}")),
            global: false,
            untracked: false,
            created_at: "2026-04-23T00:00:00Z".to_string(),
            updated_at: "2026-04-23T01:00:00Z".to_string(),
        }
    }
}
