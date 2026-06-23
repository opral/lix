use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use serde::Deserialize;
use serde_json::{Map as JsonMap, Value as JsonValue, json};

use crate::GLOBAL_BRANCH_ID;
use crate::LixError;
use crate::binary_cas::BlobHash;
use crate::common::{LixPath, compose_file_path};
use crate::entity_pk::EntityPk;
use crate::live_state::{
    LiveStateFilter, LiveStateReader, LiveStateScanRequest, MaterializedLiveStateRow,
};

use super::keys::{
    BLOB_REF_SCHEMA_KEY, DIRECTORY_DESCRIPTOR_SCHEMA_KEY, FILE_DESCRIPTOR_SCHEMA_KEY,
};
use super::visibility::VisibleFilesystem;
use super::{DirectoryPathRecord, derive_directory_paths};
use crate::transaction::types::{TransactionFileData, TransactionJson, TransactionWriteRow};

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

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct DirectoryPathCreatePlan {
    pub(crate) rows: Vec<TransactionWriteRow>,
    pub(crate) directory_id: String,
}

/// Common state-row lane fields shared by filesystem descriptor/blob rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FilesystemRowContext {
    pub(crate) branch_id: String,
    pub(crate) global: bool,
    pub(crate) untracked: bool,
    pub(crate) file_id: Option<String>,
    pub(crate) metadata: Option<TransactionJson>,
}

impl FilesystemRowContext {
    #[cfg(test)]
    pub(crate) fn active_branch(branch_id: impl Into<String>) -> Self {
        Self {
            branch_id: branch_id.into(),
            global: false,
            untracked: false,
            file_id: None,
            metadata: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct FilesystemDescriptorKey {
    branch_id: String,
    global: bool,
    untracked: bool,
    file_id: Option<String>,
    descriptor_id: String,
}

impl FilesystemDescriptorKey {
    pub(crate) fn from_context(context: &FilesystemRowContext, descriptor_id: &str) -> Self {
        Self {
            branch_id: context.branch_id.clone(),
            global: context.global,
            untracked: context.untracked,
            file_id: context.file_id.clone(),
            descriptor_id: descriptor_id.to_string(),
        }
    }

    pub(crate) fn from_live_row(
        row: &MaterializedLiveStateRow,
        descriptor_id: impl Into<String>,
    ) -> Self {
        Self {
            branch_id: row.branch_id.clone(),
            global: row.global,
            untracked: row.untracked,
            file_id: row.file_id.clone(),
            descriptor_id: descriptor_id.into(),
        }
    }

    pub(crate) fn in_same_scope(&self, descriptor_id: &str) -> Self {
        Self {
            branch_id: self.branch_id.clone(),
            global: self.global,
            untracked: self.untracked,
            file_id: self.file_id.clone(),
            descriptor_id: descriptor_id.to_string(),
        }
    }

    pub(crate) fn in_tracked_scope(&self, descriptor_id: &str) -> Self {
        Self {
            branch_id: self.branch_id.clone(),
            global: self.global,
            untracked: false,
            file_id: self.file_id.clone(),
            descriptor_id: descriptor_id.to_string(),
        }
    }

    pub(crate) fn is_untracked(&self) -> bool {
        self.untracked
    }

    pub(crate) fn context(&self) -> FilesystemRowContext {
        FilesystemRowContext {
            branch_id: self.branch_id.clone(),
            global: self.global,
            untracked: self.untracked,
            file_id: self.file_id.clone(),
            metadata: None,
        }
    }
}

/// Storage identity of a `lix_binary_blob_ref` row for a filesystem file.
///
/// File descriptors and their blob refs do not have identical row scopes:
/// blob refs are file-scoped to the file they describe. Callers should derive
/// this key from the descriptor lane plus the descriptor id instead of joining
/// blob refs by id alone.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct FilesystemBlobRefKey(FilesystemDescriptorKey);

impl FilesystemBlobRefKey {
    pub(crate) fn from_context(context: &FilesystemRowContext, file_id: &str) -> Self {
        Self(FilesystemDescriptorKey {
            branch_id: context.branch_id.clone(),
            global: context.global,
            untracked: context.untracked,
            file_id: Some(file_id.to_string()),
            descriptor_id: file_id.to_string(),
        })
    }

    pub(crate) fn from_live_row(
        row: &MaterializedLiveStateRow,
        blob_ref_id: impl Into<String>,
    ) -> Self {
        Self(FilesystemDescriptorKey::from_live_row(row, blob_ref_id))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirectoryDescriptorRowInput {
    pub(crate) id: String,
    pub(crate) parent_id: Option<String>,
    pub(crate) name: String,
    pub(crate) context: FilesystemRowContext,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileDescriptorRowInput {
    pub(crate) id: String,
    pub(crate) directory_id: Option<String>,
    pub(crate) name: String,
    pub(crate) context: FilesystemRowContext,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirectoryDescriptorWriteIntent {
    pub(crate) id: Option<String>,
    pub(crate) parent_id: Option<String>,
    pub(crate) name: String,
    pub(crate) context: FilesystemRowContext,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileDescriptorWriteIntent {
    pub(crate) id: Option<String>,
    pub(crate) directory_id: Option<String>,
    pub(crate) name: String,
    pub(crate) context: FilesystemRowContext,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlobRefRowInput {
    pub(crate) file_id: String,
    pub(crate) blob_hash: BlobHash,
    pub(crate) size_bytes: usize,
    pub(crate) context: FilesystemRowContext,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileDescriptorWriteInput {
    pub(crate) id: Option<String>,
    pub(crate) directory_id: Option<String>,
    pub(crate) name: String,
    pub(crate) data: Option<Vec<u8>>,
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
    directories_by_id: BTreeMap<String, DirectoryDescriptorSeed>,
    entries_by_parent_and_name: BTreeMap<(Option<String>, String), FilesystemNamespaceEntry>,
    promoted_directory_ids: BTreeSet<String>,
}

impl DirectoryPathResolver {
    #[cfg(test)]
    pub(crate) fn from_existing(
        existing_directories: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, LixError> {
        Self::from_existing_filesystem(existing_directories, std::iter::empty())
    }

    #[cfg(test)]
    pub(crate) fn from_existing_filesystem(
        existing_directories: impl IntoIterator<Item = (String, String)>,
        existing_files: impl IntoIterator<Item = (Option<String>, String, String)>,
    ) -> Result<Self, LixError> {
        let mut directory_paths = Vec::new();
        for (path, id) in existing_directories {
            let parsed = LixPath::try_from_directory_path(&path)?;
            directory_paths.push((
                parsed.segments().map(ToOwned::to_owned).collect::<Vec<_>>(),
                id,
            ));
        }
        directory_paths.sort_by_key(|(segments, _)| segments.len());
        let mut resolver = Self::default();
        let mut ids_by_segments = BTreeMap::<Vec<String>, String>::new();
        for (segments, id) in directory_paths {
            if segments.is_empty() {
                continue;
            }
            let parent_id = ids_by_segments
                .get(&segments[..segments.len() - 1])
                .cloned();
            let name = segments
                .last()
                .expect("non-root directory path should have a leaf segment")
                .clone();
            resolver.reserve_directory(parent_id, name, id.clone())?;
            ids_by_segments.insert(segments, id);
        }
        for (directory_id, entry_name, file_id) in existing_files {
            resolver.reserve_file(directory_id, entry_name, file_id)?;
        }
        Ok(resolver)
    }

    fn from_existing_descriptors(
        existing_directories: impl IntoIterator<Item = DirectoryDescriptorSeed>,
        existing_files: impl IntoIterator<Item = (Option<String>, String, String)>,
    ) -> Result<Self, LixError> {
        let mut resolver = Self::default();
        for directory in existing_directories {
            resolver.reserve_directory(directory.parent_id, directory.name, directory.id)?;
        }
        resolver.validate_directory_parent_graph()?;
        for (directory_id, entry_name, file_id) in existing_files {
            resolver.reserve_file(directory_id, entry_name, file_id)?;
        }
        Ok(resolver)
    }

    #[cfg(test)]
    pub(crate) fn directory_id(&self, path: &str) -> Result<Option<&str>, LixError> {
        let parsed = LixPath::try_from_directory_path(path)?;
        Ok(self.directory_id_from_segments(
            &parsed.segments().map(ToOwned::to_owned).collect::<Vec<_>>(),
        ))
    }

    fn directory_id_from_segments(&self, segments: &[String]) -> Option<&str> {
        let mut directory_id = None::<&str>;
        for segment in segments {
            let key = (directory_id.map(ToOwned::to_owned), segment.clone());
            let entry = self.entries_by_parent_and_name.get(&key)?;
            match entry {
                FilesystemNamespaceEntry::Directory(id) => directory_id = Some(id.as_str()),
                FilesystemNamespaceEntry::File(_) => return None,
            }
        }
        directory_id
    }

    pub(crate) fn file_path(
        &self,
        directory_id: Option<&str>,
        name: &str,
    ) -> Result<Option<String>, LixError> {
        let Some(directory_id) = directory_id else {
            return Ok(Some(compose_file_path(None, name)?));
        };
        let directory_paths = self.directory_paths_by_id()?;
        let Some(directory_path) = directory_paths.get(directory_id) else {
            return Ok(None);
        };
        Ok(Some(compose_file_path(Some(directory_path), name)?))
    }

    pub(crate) fn require_file_path(
        &self,
        directory_id: Option<&str>,
        name: &str,
    ) -> Result<String, LixError> {
        self.file_path(directory_id, name)?.ok_or_else(|| {
            LixError::new(
                LixError::CODE_CONSTRAINT_VIOLATION,
                format!(
                    "filesystem descriptor references missing directory_id {:?}",
                    directory_id.unwrap_or("<root>")
                ),
            )
        })
    }

    /// Stages only the missing descriptors needed for `directory_path`.
    ///
    /// Existing directories keep their original ids. Missing directories receive
    /// deterministic ids so repeated planning of the same transaction-visible
    /// path resolves to the same descriptor identity.
    #[cfg(test)]
    pub(crate) fn ensure_directory_path(
        &mut self,
        directory_path: &str,
        context: FilesystemRowContext,
        generate_directory_id: &mut dyn FnMut() -> String,
    ) -> Result<Vec<TransactionWriteRow>, LixError> {
        let parsed = LixPath::try_from_directory_path(directory_path)?;
        self.plan_directory_segments_with_fallback(
            None,
            parsed.segments().map(ToOwned::to_owned).collect::<Vec<_>>(),
            None,
            context,
            generate_directory_id,
            None,
        )
    }

    fn plan_directory_segments_with_fallback(
        &mut self,
        fallback: Option<&Self>,
        segments: Vec<String>,
        leaf_id: Option<String>,
        context: FilesystemRowContext,
        generate_directory_id: &mut dyn FnMut() -> String,
        duplicate_directory_path: Option<&str>,
    ) -> Result<Vec<TransactionWriteRow>, LixError> {
        if segments.is_empty() {
            if let Some(directory_path) = duplicate_directory_path {
                return Err(duplicate_directory_path_error(directory_path));
            }
            return Ok(Vec::new());
        }

        let mut rows = Vec::new();
        let mut parent_id = None::<String>;
        let leaf_index = segments.len() - 1;
        for (index, name) in segments.into_iter().enumerate() {
            let is_leaf = index == leaf_index;
            let key = (parent_id.clone(), name.clone());
            let fallback_entry = fallback
                .and_then(|resolver| resolver.entries_by_parent_and_name.get(&key))
                .cloned();
            match self.entries_by_parent_and_name.get(&key).cloned() {
                Some(FilesystemNamespaceEntry::Directory(existing_id)) => {
                    if is_leaf && let Some(directory_path) = duplicate_directory_path {
                        return Err(duplicate_directory_path_error(directory_path));
                    }
                    if let Some(fallback_entry) = fallback_entry.as_ref() {
                        Self::reject_cross_scope_directory_conflict(
                            key.0.as_deref(),
                            &key.1,
                            &existing_id,
                            fallback_entry,
                        )?;
                        if !context.untracked
                            && let FilesystemNamespaceEntry::Directory(fallback_id) = fallback_entry
                            && fallback_id == &existing_id
                        {
                            self.stage_promoted_directory_once(&existing_id, &context, &mut rows)?;
                        }
                    }
                    parent_id = Some(existing_id.clone());
                    continue;
                }
                Some(existing @ FilesystemNamespaceEntry::File(_)) => {
                    return Err(filesystem_namespace_conflict_error(
                        &key.0, &key.1, &existing,
                    ));
                }
                None => {}
            }

            if let Some(fallback_entry) = fallback_entry {
                match fallback_entry {
                    FilesystemNamespaceEntry::Directory(existing_id) => {
                        if is_leaf
                            && let Some(leaf_id) = leaf_id.as_ref()
                            && leaf_id != &existing_id
                        {
                            return Err(directory_id_conflict_error(&existing_id));
                        }
                        let fallback_resolver =
                            fallback.expect("fallback entry came from resolver");
                        let seed = fallback_resolver
                            .directories_by_id
                            .get(&existing_id)
                            .cloned()
                            .ok_or_else(|| {
                                LixError::new(
                                    "LIX_ERROR_UNKNOWN",
                                    format!(
                                        "directory namespace entry references missing directory descriptor {existing_id:?}"
                                    ),
                                )
                            })?;
                        self.reserve_directory(
                            seed.parent_id.clone(),
                            seed.name.clone(),
                            seed.id.clone(),
                        )?;
                        if !context.untracked {
                            self.stage_promoted_directory_seed_once(seed, &context, &mut rows);
                        }
                        parent_id = Some(existing_id);
                        continue;
                    }
                    existing @ FilesystemNamespaceEntry::File(_) => {
                        return Err(filesystem_namespace_conflict_error(
                            &key.0, &key.1, &existing,
                        ));
                    }
                }
            }

            let id = if is_leaf {
                leaf_id.clone().unwrap_or_else(&mut *generate_directory_id)
            } else {
                generate_directory_id()
            };
            self.reserve_directory(parent_id.clone(), name.clone(), id.clone())?;

            rows.push(directory_descriptor_row(DirectoryDescriptorRowInput {
                id: id.clone(),
                parent_id: parent_id.clone(),
                name,
                context: FilesystemRowContext {
                    // Directory descriptors are their own filesystem state row,
                    // even when they are implicitly planned from a file insert.
                    file_id: None,
                    ..context.clone()
                },
            }));
            parent_id = Some(id);
        }

        Ok(rows)
    }

    fn reject_cross_scope_directory_conflict(
        parent_id: Option<&str>,
        entry_name: &str,
        existing_id: &str,
        fallback_entry: &FilesystemNamespaceEntry,
    ) -> Result<(), LixError> {
        match fallback_entry {
            FilesystemNamespaceEntry::Directory(fallback_id) if fallback_id == existing_id => {
                Ok(())
            }
            existing => {
                let parent_id = parent_id.map(str::to_string);
                Err(filesystem_namespace_conflict_error(
                    &parent_id, entry_name, existing,
                ))
            }
        }
    }

    fn stage_promoted_directory_once(
        &mut self,
        directory_id: &str,
        context: &FilesystemRowContext,
        rows: &mut Vec<TransactionWriteRow>,
    ) -> Result<(), LixError> {
        let seed = self.directories_by_id.get(directory_id).cloned().ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "directory namespace entry references missing directory descriptor {directory_id:?}"
                ),
            )
        })?;
        self.stage_promoted_directory_seed_once(seed, context, rows);
        Ok(())
    }

    fn stage_promoted_directory_seed_once(
        &mut self,
        seed: DirectoryDescriptorSeed,
        context: &FilesystemRowContext,
        rows: &mut Vec<TransactionWriteRow>,
    ) {
        if !self.promoted_directory_ids.insert(seed.id.clone()) {
            return;
        }
        rows.push(directory_descriptor_row(DirectoryDescriptorRowInput {
            id: seed.id,
            parent_id: seed.parent_id,
            name: seed.name,
            context: FilesystemRowContext {
                file_id: None,
                untracked: false,
                ..context.clone()
            },
        }));
    }

    fn validate_directory_parent_graph(&self) -> Result<(), LixError> {
        self.directory_paths_by_id().map(|_| ())
    }

    fn directory_paths_by_id(&self) -> Result<BTreeMap<String, String>, LixError> {
        derive_directory_paths(
            self.directories_by_id
                .iter()
                .map(|(directory_id, directory)| (directory_id.clone(), directory)),
        )
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
                let existing_descriptor =
                    self.directories_by_id
                        .get(&directory_id)
                        .ok_or_else(|| {
                            LixError::new(
                                "LIX_ERROR_UNKNOWN",
                                format!(
                                    "directory namespace entry references missing directory descriptor {directory_id:?}"
                                ),
                            )
                        })?;
                if existing_descriptor.parent_id == key.0 && existing_descriptor.name == key.1 {
                    return Ok(());
                }
                Err(directory_id_conflict_error(&directory_id))
            }
            Some(existing) => Err(filesystem_namespace_conflict_error(
                &key.0, &key.1, existing,
            )),
            None => {
                match self.directories_by_id.get(&directory_id) {
                    Some(existing) if existing.parent_id == key.0 && existing.name == key.1 => {}
                    Some(_) => return Err(directory_id_conflict_error(&directory_id)),
                    None => {
                        self.directories_by_id.insert(
                            directory_id.clone(),
                            DirectoryDescriptorSeed {
                                id: directory_id.clone(),
                                parent_id: key.0.clone(),
                                name: key.1.clone(),
                            },
                        );
                    }
                }
                self.entries_by_parent_and_name
                    .insert(key, FilesystemNamespaceEntry::Directory(directory_id));
                Ok(())
            }
        }
    }

    pub(crate) fn update_directory(
        &mut self,
        parent_id: Option<String>,
        name: String,
        directory_id: String,
    ) -> Result<(), LixError> {
        let mut next = self.clone();
        let new_key = (parent_id.clone(), name.clone());
        if let Some(existing) = next.entries_by_parent_and_name.get(&new_key) {
            match existing {
                FilesystemNamespaceEntry::Directory(existing_id)
                    if existing_id == &directory_id => {}
                existing => {
                    return Err(filesystem_namespace_conflict_error(
                        &new_key.0, &new_key.1, existing,
                    ));
                }
            }
        }

        let Some(existing_descriptor) = next.directories_by_id.get(&directory_id).cloned() else {
            next.reserve_directory(parent_id, name, directory_id)?;
            next.validate_directory_parent_graph()?;
            *self = next;
            return Ok(());
        };
        let old_key = (
            existing_descriptor.parent_id.clone(),
            existing_descriptor.name,
        );
        if old_key != new_key {
            if matches!(
                next.entries_by_parent_and_name.get(&old_key),
                Some(FilesystemNamespaceEntry::Directory(existing_id))
                    if existing_id == &directory_id
            ) {
                next.entries_by_parent_and_name.remove(&old_key);
            }
            next.entries_by_parent_and_name.insert(
                new_key.clone(),
                FilesystemNamespaceEntry::Directory(directory_id.clone()),
            );
        }
        next.directories_by_id.insert(
            directory_id.clone(),
            DirectoryDescriptorSeed {
                id: directory_id,
                parent_id: new_key.0,
                name: new_key.1,
            },
        );
        next.validate_directory_parent_graph()?;
        *self = next;
        Ok(())
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

fn directory_id_conflict_error(directory_id: &str) -> LixError {
    LixError::new(
        LixError::CODE_UNIQUE,
        format!("unique constraint violation on lix_directory.id for value {directory_id:?}"),
    )
}

fn duplicate_directory_path_error(path: &str) -> LixError {
    LixError::new(
        LixError::CODE_UNIQUE,
        format!("unique constraint violation on lix_directory.path for value {path:?}"),
    )
}

#[expect(clippy::ref_option)]
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
        context: input.context,
    })
}

pub(crate) fn file_descriptor_row(input: FileDescriptorRowInput) -> TransactionWriteRow {
    file_descriptor_write_row(FileDescriptorWriteIntent {
        id: Some(input.id),
        directory_id: input.directory_id,
        name: input.name,
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

    partial_state_row(
        input.id,
        FILE_DESCRIPTOR_SCHEMA_KEY,
        Some(JsonValue::Object(snapshot)),
        input.context,
    )
}

pub(crate) fn blob_ref_row(input: BlobRefRowInput) -> Result<TransactionWriteRow, LixError> {
    let size_bytes = u64::try_from(input.size_bytes).map_err(|_| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "binary blob size exceeds supported range for file '{}' branch '{}'",
                input.file_id, input.context.branch_id
            ),
        )
    })?;
    let snapshot = json!({
        "id": input.file_id,
        "blob_hash": input.blob_hash.to_hex(),
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

pub(crate) fn blob_ref_tombstone_row(
    file_id: String,
    context: FilesystemRowContext,
) -> TransactionWriteRow {
    tombstone_row(
        file_id.clone(),
        BLOB_REF_SCHEMA_KEY,
        FilesystemRowContext {
            file_id: Some(file_id),
            metadata: None,
            ..context
        },
    )
}

pub(crate) fn plan_parsed_file_path_write_with_resolvers(
    resolvers: &mut BTreeMap<String, DirectoryPathResolver>,
    parsed: LixPath,
    id: Option<String>,
    data: Option<Vec<u8>>,
    context: FilesystemRowContext,
    generate_directory_id: &mut dyn FnMut() -> String,
) -> Result<FilesystemWritePlan, LixError> {
    let fallback = fallback_path_resolver(resolvers, &context);
    let resolver = resolvers.entry(path_resolver_key(&context)).or_default();
    plan_parsed_file_path_write_with_fallback(
        resolver,
        fallback.as_ref(),
        parsed,
        id,
        data,
        context,
        generate_directory_id,
    )
}

fn plan_parsed_file_path_write_with_fallback(
    resolver: &mut DirectoryPathResolver,
    fallback: Option<&DirectoryPathResolver>,
    parsed: LixPath,
    id: Option<String>,
    data: Option<Vec<u8>>,
    context: FilesystemRowContext,
    generate_directory_id: &mut dyn FnMut() -> String,
) -> Result<FilesystemWritePlan, LixError> {
    let mut rows = Vec::new();
    let file_id = id.unwrap_or_else(&mut *generate_directory_id);
    let segments = parsed.segments().map(ToOwned::to_owned).collect::<Vec<_>>();
    let filename = segments
        .last()
        .expect("parsed file path should have a leaf segment")
        .clone();
    let file_path = file_path_from_segments(&segments);

    let directory_segments = file_directory_segments(&segments);
    let directory_id = if directory_segments.is_empty() {
        None
    } else {
        rows.extend(resolver.plan_directory_segments_with_fallback(
            fallback,
            directory_segments.to_vec(),
            None,
            context.clone(),
            generate_directory_id,
            None,
        )?);
        resolver
            .directory_id_from_segments(directory_segments)
            .map(ToOwned::to_owned)
    };

    resolver.reserve_file(directory_id.clone(), filename.clone(), file_id.clone())?;
    rows.push(file_descriptor_row(FileDescriptorRowInput {
        id: file_id.clone(),
        directory_id,
        name: filename.clone(),
        context: context.clone(),
    }));

    let mut file_data = Vec::new();
    if let Some(data) = data {
        let file_payload = TransactionFileData::new(
            file_id.clone(),
            Some(file_path),
            Some(filename),
            context.branch_id.clone(),
            context.global,
            context.untracked,
            data,
        );
        if let Some(blob_hash) = file_payload.blob_hash() {
            rows.push(blob_ref_row(BlobRefRowInput {
                file_id,
                blob_hash,
                size_bytes: file_payload.len(),
                context: FilesystemRowContext {
                    file_id: None,
                    metadata: None,
                    ..context
                },
            })?);
        }
        file_data.push(file_payload);
    }

    Ok(FilesystemWritePlan {
        rows,
        file_data,
        count: 1,
    })
}

pub(crate) fn plan_file_descriptor_write(
    resolver: &mut DirectoryPathResolver,
    input: FileDescriptorWriteInput,
    generate_file_id: &mut dyn FnMut() -> String,
) -> Result<FilesystemWritePlan, LixError> {
    let file_path = resolver.require_file_path(input.directory_id.as_deref(), &input.name)?;
    let file_id = input.id.unwrap_or_else(&mut *generate_file_id);
    let filename = input.name.clone();
    resolver.reserve_file(
        input.directory_id.clone(),
        input.name.clone(),
        file_id.clone(),
    )?;
    let mut rows = vec![file_descriptor_row(FileDescriptorRowInput {
        id: file_id.clone(),
        directory_id: input.directory_id,
        name: input.name,
        context: input.context.clone(),
    })];

    let mut file_data = Vec::new();
    if let Some(data) = input.data {
        let file_payload = TransactionFileData::new(
            file_id.clone(),
            Some(file_path),
            Some(filename),
            input.context.branch_id.clone(),
            input.context.global,
            input.context.untracked,
            data,
        );
        if let Some(blob_hash) = file_payload.blob_hash() {
            rows.push(blob_ref_row(BlobRefRowInput {
                file_id,
                blob_hash,
                size_bytes: file_payload.len(),
                context: FilesystemRowContext {
                    file_id: None,
                    metadata: None,
                    ..input.context.clone()
                },
            })?);
        }
        file_data.push(file_payload);
    }

    Ok(FilesystemWritePlan {
        rows,
        file_data,
        count: 1,
    })
}

pub(crate) fn plan_parsed_file_path_update_with_resolvers(
    resolvers: &mut BTreeMap<String, DirectoryPathResolver>,
    existing_file_id: String,
    parsed: LixPath,
    context: FilesystemRowContext,
    generate_directory_id: &mut dyn FnMut() -> String,
) -> Result<FilesystemWritePlan, LixError> {
    let fallback = fallback_path_resolver(resolvers, &context);
    let resolver = resolvers.entry(path_resolver_key(&context)).or_default();
    plan_parsed_file_path_update_with_fallback(
        resolver,
        fallback.as_ref(),
        existing_file_id,
        parsed,
        context,
        generate_directory_id,
    )
}

fn plan_parsed_file_path_update_with_fallback(
    resolver: &mut DirectoryPathResolver,
    fallback: Option<&DirectoryPathResolver>,
    existing_file_id: String,
    parsed: LixPath,
    context: FilesystemRowContext,
    generate_directory_id: &mut dyn FnMut() -> String,
) -> Result<FilesystemWritePlan, LixError> {
    let mut rows = Vec::new();
    let segments = parsed.segments().map(ToOwned::to_owned).collect::<Vec<_>>();
    let filename = segments
        .last()
        .expect("parsed file path should have a leaf segment")
        .clone();

    let directory_segments = file_directory_segments(&segments);
    let directory_id = if directory_segments.is_empty() {
        None
    } else {
        rows.extend(resolver.plan_directory_segments_with_fallback(
            fallback,
            directory_segments.to_vec(),
            None,
            context.clone(),
            generate_directory_id,
            None,
        )?);
        resolver
            .directory_id_from_segments(directory_segments)
            .map(ToOwned::to_owned)
    };

    resolver.reserve_file(
        directory_id.clone(),
        filename.clone(),
        existing_file_id.clone(),
    )?;
    rows.push(file_descriptor_row(FileDescriptorRowInput {
        id: existing_file_id,
        directory_id,
        name: filename,
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

pub(crate) fn create_directory_path_with_leaf_id_with_resolvers(
    resolvers: &mut BTreeMap<String, DirectoryPathResolver>,
    parsed: LixPath,
    leaf_id: Option<String>,
    context: FilesystemRowContext,
    generate_directory_id: &mut dyn FnMut() -> String,
) -> Result<DirectoryPathCreatePlan, LixError> {
    let segments = parsed.segments().map(ToOwned::to_owned).collect::<Vec<_>>();
    let duplicate_directory_path = directory_path_from_segments(&segments);
    let fallback = fallback_path_resolver(resolvers, &context);
    let resolver = resolvers.entry(path_resolver_key(&context)).or_default();
    let rows = resolver.plan_directory_segments_with_fallback(
        fallback.as_ref(),
        segments.clone(),
        leaf_id,
        context,
        generate_directory_id,
        Some(duplicate_directory_path.as_str()),
    )?;
    let directory_id = resolver
        .directory_id_from_segments(&segments)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("directory path {duplicate_directory_path:?} did not resolve after create"),
            )
        })?
        .to_string();
    Ok(DirectoryPathCreatePlan { rows, directory_id })
}

pub(crate) fn plan_parsed_directory_path_update_with_resolvers(
    resolvers: &mut BTreeMap<String, DirectoryPathResolver>,
    parsed: LixPath,
    directory_id: String,
    context: FilesystemRowContext,
    generate_directory_id: &mut dyn FnMut() -> String,
) -> Result<Vec<TransactionWriteRow>, LixError> {
    let segments = parsed.segments().map(ToOwned::to_owned).collect::<Vec<_>>();
    if segments.is_empty() {
        return Err(duplicate_directory_path_error("/"));
    }
    let leaf_name = segments
        .last()
        .expect("parsed directory path should have a leaf segment")
        .clone();
    let parent_segments = &segments[..segments.len() - 1];
    let fallback = fallback_path_resolver(resolvers, &context);
    let resolver = resolvers.entry(path_resolver_key(&context)).or_default();
    let mut rows = resolver.plan_directory_segments_with_fallback(
        fallback.as_ref(),
        parent_segments.to_vec(),
        None,
        context.clone(),
        generate_directory_id,
        None,
    )?;
    let parent_id = if parent_segments.is_empty() {
        None
    } else {
        resolver
            .directory_id_from_segments(parent_segments)
            .map(ToOwned::to_owned)
    };
    resolver.update_directory(parent_id.clone(), leaf_name.clone(), directory_id.clone())?;
    rows.push(directory_descriptor_row(DirectoryDescriptorRowInput {
        id: directory_id,
        parent_id,
        name: leaf_name,
        context: FilesystemRowContext {
            file_id: None,
            ..context
        },
    }));
    Ok(rows)
}

fn fallback_path_resolver(
    resolvers: &BTreeMap<String, DirectoryPathResolver>,
    context: &FilesystemRowContext,
) -> Option<DirectoryPathResolver> {
    let fallback_key = filesystem_storage_scope_key(
        &context.branch_id,
        context.global,
        !context.untracked,
        context.file_id.as_deref(),
    );
    resolvers.get(&fallback_key).cloned()
}

fn path_resolver_key(context: &FilesystemRowContext) -> String {
    filesystem_storage_scope_key(
        &context.branch_id,
        context.global,
        context.untracked,
        context.file_id.as_deref(),
    )
}

fn file_directory_segments(segments: &[String]) -> &[String] {
    &segments[..segments.len() - 1]
}

fn file_path_from_segments(segments: &[String]) -> String {
    format!("/{}", segments.join("/"))
}

fn directory_path_from_segments(segments: &[String]) -> String {
    if segments.is_empty() {
        "/".to_string()
    } else {
        format!("/{}/", segments.join("/"))
    }
}

pub(crate) fn plan_file_delete(input: FileDeleteInput) -> FilesystemDeletePlan {
    let mut rows = vec![tombstone_row(
        input.file_id.clone(),
        FILE_DESCRIPTOR_SCHEMA_KEY,
        input.context.clone(),
    )];

    if input.has_blob_ref {
        rows.push(blob_ref_tombstone_row(input.file_id.clone(), input.context));
    }

    FilesystemDeletePlan { rows, count: 1 }
}

pub(crate) fn plan_directory_delete(input: DirectoryDeleteInput) -> FilesystemDeletePlan {
    FilesystemDeletePlan {
        rows: vec![tombstone_row(
            input.directory_id,
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
            input.context,
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
        let storage_branch_id = if row.global {
            GLOBAL_BRANCH_ID
        } else {
            row.branch_id.as_str()
        };
        let resolver_key = filesystem_storage_scope_key(
            storage_branch_id,
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
    for (branch_id, records) in directory_rows {
        let files = file_rows.remove(&branch_id).unwrap_or_default();
        resolvers.insert(
            branch_id,
            DirectoryPathResolver::from_existing_descriptors(records.into_values(), files)?,
        );
    }
    for (branch_id, files) in file_rows {
        resolvers.insert(
            branch_id,
            DirectoryPathResolver::from_existing_descriptors(std::iter::empty(), files)?,
        );
    }
    Ok(resolvers)
}

pub(crate) async fn directory_path_resolvers_from_live_state(
    live_state: Arc<dyn LiveStateReader>,
    branch_binding: Option<&str>,
) -> Result<BTreeMap<String, DirectoryPathResolver>, LixError> {
    let rows = live_state
        .scan_rows(&LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec![
                    DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
                    FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                ],
                branch_ids: branch_binding
                    .map(|branch_id| vec![branch_id.to_string()])
                    .unwrap_or_default(),
                ..Default::default()
            },
            ..Default::default()
        })
        .await?;
    let mut resolvers = directory_path_resolvers_from_state_rows(rows)?;
    if let Some(branch_id) = branch_binding {
        let key = filesystem_storage_scope_key(branch_id, false, false, None);
        resolvers.entry(key).or_default();
    }
    Ok(resolvers)
}

pub(crate) fn filesystem_storage_scope_key(
    branch_id: &str,
    global: bool,
    untracked: bool,
    file_id: Option<&str>,
) -> String {
    format!(
        "branch={branch_id}\0global={global}\0untracked={untracked}\0file_id_present={}\0file_id={}",
        file_id.is_some(),
        file_id.unwrap_or("")
    )
}

#[derive(Debug, Clone)]
struct DirectoryDescriptorSeed {
    id: String,
    parent_id: Option<String>,
    name: String,
}

impl DirectoryPathRecord for DirectoryDescriptorSeed {
    type Key = String;

    fn parent_key(&self, _key: &Self::Key) -> Option<Self::Key> {
        self.parent_id.clone()
    }

    fn name(&self) -> &str {
        &self.name
    }
}

fn state_row(
    entity_pk: String,
    schema_key: &str,
    snapshot: Option<JsonValue>,
    context: FilesystemRowContext,
) -> TransactionWriteRow {
    partial_state_row(Some(entity_pk), schema_key, snapshot, context)
}

fn partial_state_row(
    entity_pk: Option<String>,
    schema_key: &str,
    snapshot: Option<JsonValue>,
    context: FilesystemRowContext,
) -> TransactionWriteRow {
    let snapshot = snapshot.map(TransactionJson::from_value_unchecked);
    TransactionWriteRow {
        entity_pk: entity_pk.map(EntityPk::single),
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
        branch_id: context.branch_id,
    }
}

fn tombstone_row(
    entity_pk: String,
    schema_key: &str,
    context: FilesystemRowContext,
) -> TransactionWriteRow {
    state_row(entity_pk, schema_key, None, context)
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
        .get(&Some(FilesystemDescriptorKey::from_context(
            context,
            directory_id,
        )))
    {
        for child_id in child_ids {
            collect_recursive_directory_delete(child_id, visible_filesystem, context, rows, count);
        }
    }

    if let Some(files) =
        visible_filesystem
            .files_by_directory_id
            .get(&Some(FilesystemDescriptorKey::from_context(
                context,
                directory_id,
            )))
    {
        for file_id in files {
            let plan = plan_file_delete(FileDeleteInput {
                file_id: file_id.clone(),
                has_blob_ref: visible_filesystem.has_blob_ref(context, file_id),
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

    use serde_json::{Value as JsonValue, json};

    use crate::GLOBAL_BRANCH_ID;
    use crate::binary_cas::BlobHash;
    use crate::changelog::{ChangeId, CommitId};
    use crate::filesystem::{FilesystemBlobRefKey, FilesystemDescriptorKey};
    use crate::transaction::types::TransactionJson;

    use super::{
        BlobRefRowInput, DirectoryDeleteInput, DirectoryDescriptorRowInput,
        DirectoryDescriptorWriteIntent, DirectoryPathResolver, FileDeleteInput,
        FileDescriptorRowInput, FileDescriptorWriteInput, FileDescriptorWriteIntent,
        FilesystemRowContext, blob_ref_row, directory_descriptor_row,
        directory_descriptor_write_row, file_descriptor_row, file_descriptor_write_row,
        plan_file_descriptor_write,
    };
    use crate::common::LixPath;
    use crate::filesystem::VisibleFilesystem;
    use crate::{entity_pk::EntityPk, live_state::MaterializedLiveStateRow};

    fn test_id_generator(ids: &'static [&'static str]) -> impl FnMut() -> String {
        let mut ids = ids.iter();
        move || ids.next().expect("test id should exist").to_string()
    }

    fn parsed_file_path(path: &str) -> LixPath {
        LixPath::try_from_file_path(path).expect("test file path should parse")
    }

    fn with_resolver_map<T>(
        resolver: &mut DirectoryPathResolver,
        key: String,
        run: impl FnOnce(&mut BTreeMap<String, DirectoryPathResolver>) -> Result<T, crate::LixError>,
    ) -> Result<T, crate::LixError> {
        let mut resolvers = BTreeMap::from([(key.clone(), std::mem::take(resolver))]);
        let result = run(&mut resolvers);
        *resolver = resolvers.remove(&key).unwrap_or_default();
        result
    }

    fn create_directory_path_with_leaf_id(
        resolver: &mut DirectoryPathResolver,
        directory_path: &str,
        leaf_id: Option<String>,
        context: FilesystemRowContext,
        generate_directory_id: &mut dyn FnMut() -> String,
    ) -> Result<Vec<crate::transaction::types::TransactionWriteRow>, crate::LixError> {
        let parsed = LixPath::try_from_directory_path(directory_path)?;
        let key = super::path_resolver_key(&context);
        with_resolver_map(resolver, key, |resolvers| {
            super::create_directory_path_with_leaf_id_with_resolvers(
                resolvers,
                parsed,
                leaf_id,
                context,
                generate_directory_id,
            )
            .map(|plan| plan.rows)
        })
    }

    fn plan_parsed_file_path_write(
        resolver: &mut DirectoryPathResolver,
        parsed: LixPath,
        id: Option<String>,
        data: Option<Vec<u8>>,
        context: FilesystemRowContext,
        generate_directory_id: &mut dyn FnMut() -> String,
    ) -> Result<super::FilesystemWritePlan, crate::LixError> {
        let key = super::path_resolver_key(&context);
        with_resolver_map(resolver, key, |resolvers| {
            super::plan_parsed_file_path_write_with_resolvers(
                resolvers,
                parsed,
                id,
                data,
                context,
                generate_directory_id,
            )
        })
    }

    fn plan_parsed_file_path_update(
        resolver: &mut DirectoryPathResolver,
        existing_file_id: String,
        parsed: LixPath,
        context: FilesystemRowContext,
        generate_directory_id: &mut dyn FnMut() -> String,
    ) -> Result<super::FilesystemWritePlan, crate::LixError> {
        let key = super::path_resolver_key(&context);
        with_resolver_map(resolver, key, |resolvers| {
            super::plan_parsed_file_path_update_with_resolvers(
                resolvers,
                existing_file_id,
                parsed,
                context,
                generate_directory_id,
            )
        })
    }

    #[test]
    fn directory_descriptor_row_builds_state_row() {
        let row = directory_descriptor_row(DirectoryDescriptorRowInput {
            id: "dir-docs".to_string(),
            parent_id: None,
            name: "docs".to_string(),
            context: FilesystemRowContext::active_branch("branch-a"),
        });

        assert_eq!(row.entity_pk.as_ref(), Some(&EntityPk::single("dir-docs")));
        assert_eq!(row.schema_key, "lix_directory_descriptor");
        assert_eq!(row.branch_id, "branch-a");
        let snapshot: JsonValue = row.snapshot.as_ref().unwrap().value().clone();
        assert_eq!(snapshot["id"], "dir-docs");
        assert_eq!(snapshot["parent_id"], JsonValue::Null);
        assert_eq!(snapshot["name"], "docs");
    }

    #[test]
    fn file_descriptor_row_builds_state_row() {
        let row = file_descriptor_row(FileDescriptorRowInput {
            id: "file-readme".to_string(),
            directory_id: Some("dir-docs".to_string()),
            name: "readme.md".to_string(),
            context: FilesystemRowContext::active_branch("branch-a"),
        });

        assert_eq!(
            row.entity_pk.as_ref(),
            Some(&EntityPk::single("file-readme"))
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
            blob_hash: BlobHash::from_content(b"Hello"),
            size_bytes: 5,
            context: FilesystemRowContext::active_branch("branch-a"),
        })
        .expect("blob ref row should build");

        assert_eq!(
            row.entity_pk.as_ref(),
            Some(&EntityPk::single("file-readme"))
        );
        assert_eq!(row.file_id.as_deref(), Some("file-readme"));
        assert_eq!(row.schema_key, "lix_binary_blob_ref");
        let snapshot: JsonValue = row.snapshot.as_ref().unwrap().value().clone();
        assert_eq!(snapshot["id"], "file-readme");
        assert_eq!(snapshot["size_bytes"], 5);
        assert_eq!(
            snapshot["blob_hash"].as_str(),
            Some(BlobHash::from_content(b"Hello").to_hex().as_str())
        );
    }

    #[test]
    fn directory_path_resolver_handles_root_directory() {
        let mut resolver =
            DirectoryPathResolver::from_existing([]).expect("empty resolver should build");

        let rows = resolver
            .ensure_directory_path(
                "/",
                FilesystemRowContext::active_branch("branch-a"),
                &mut test_id_generator(&["should-not-be-used"]),
            )
            .expect("root directory ensure should be a no-op");
        assert!(rows.is_empty());

        let error = create_directory_path_with_leaf_id(
            &mut resolver,
            "/",
            Some("dir-root".to_string()),
            FilesystemRowContext::active_branch("branch-a"),
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect_err("explicit root directory create should be rejected");
        assert_eq!(error.code, crate::LixError::CODE_UNIQUE);
    }

    #[test]
    fn directory_path_resolver_reuses_existing_ancestor() {
        let mut resolver =
            DirectoryPathResolver::from_existing([("/docs/".to_string(), "dir-docs".to_string())])
                .expect("existing directories should parse");

        let rows = resolver
            .ensure_directory_path(
                "/docs/nested/",
                FilesystemRowContext::active_branch("branch-a"),
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
                FilesystemRowContext::active_branch("branch-a"),
                &mut test_id_generator(&["dir-generated-docs"]),
            )
            .expect("top-level directory should plan");
        assert_eq!(docs_rows.len(), 1);

        let nested_rows = resolver
            .ensure_directory_path(
                "/docs/nested/",
                FilesystemRowContext::active_branch("branch-a"),
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

        let rows = create_directory_path_with_leaf_id(
            &mut resolver,
            "/docs/nested/",
            Some("dir-nested".to_string()),
            FilesystemRowContext::active_branch("branch-a"),
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
                FilesystemRowContext::active_branch("branch-a"),
                &mut test_id_generator(&["dir-generated-docs", "dir-generated-nested"]),
            )
            .expect("directory path should plan");
        assert_eq!(rows.len(), 2);

        let rows = resolver
            .ensure_directory_path(
                "/docs/nested/",
                FilesystemRowContext::active_branch("branch-a"),
                &mut test_id_generator(&["should-not-be-used"]),
            )
            .expect("directory path should plan");
        assert!(rows.is_empty());
    }

    #[test]
    fn file_path_write_supports_root_file_generated_id_and_no_data() {
        let mut resolver =
            DirectoryPathResolver::from_existing([]).expect("empty resolver should build");

        let plan = plan_parsed_file_path_write(
            &mut resolver,
            parsed_file_path("/readme.md"),
            None,
            None,
            FilesystemRowContext::active_branch("branch-a"),
            &mut test_id_generator(&["file-generated-readme"]),
        )
        .expect("root file path write should plan");

        assert_eq!(plan.count, 1);
        assert!(plan.file_data.is_empty());
        assert_eq!(plan.rows.len(), 1);
        assert_eq!(plan.rows[0].schema_key, "lix_file_descriptor");
        let snapshot: JsonValue = plan.rows[0].snapshot.as_ref().unwrap().value().clone();
        assert_eq!(snapshot["id"], "file-generated-readme");
        assert_eq!(snapshot["directory_id"], JsonValue::Null);
        assert_eq!(snapshot["name"], "readme.md");
    }

    #[test]
    fn file_path_write_stages_missing_directories_file_blob_and_payload() {
        let mut resolver =
            DirectoryPathResolver::from_existing([]).expect("empty resolver should build");

        let plan = plan_parsed_file_path_write(
            &mut resolver,
            parsed_file_path("/docs/guides/readme.md"),
            Some("file-readme".to_string()),
            Some(b"hello".to_vec()),
            FilesystemRowContext::active_branch("branch-a"),
            &mut test_id_generator(&["dir-generated-docs", "dir-generated-guides"]),
        )
        .expect("file path write should plan");

        assert_eq!(plan.count, 1);
        assert_eq!(plan.file_data.len(), 1);
        assert_eq!(plan.file_data[0].file_id, "file-readme");
        assert_eq!(plan.file_data[0].branch_id, "branch-a");
        assert_eq!(plan.file_data[0].data(), b"hello");
        assert_eq!(plan.rows.len(), 4);
        assert_eq!(
            plan.rows
                .iter()
                .filter(|row| row.schema_key == "lix_directory_descriptor")
                .count(),
            2
        );
        assert!(
            plan.rows
                .iter()
                .any(|row| row.schema_key == "lix_binary_blob_ref")
        );

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

        let plan = plan_parsed_file_path_write(
            &mut resolver,
            parsed_file_path("/docs/guides/readme.md"),
            Some("file-readme".to_string()),
            Some(b"hello".to_vec()),
            FilesystemRowContext::active_branch("branch-a"),
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
    fn file_descriptor_write_renders_payload_path_from_parent_descriptor() {
        let mut resolver =
            DirectoryPathResolver::from_existing([("/docs/".to_string(), "dir-docs".to_string())])
                .expect("resolver should build");

        let plan = plan_file_descriptor_write(
            &mut resolver,
            FileDescriptorWriteInput {
                id: Some("file-readme".to_string()),
                directory_id: Some("dir-docs".to_string()),
                name: "readme.md".to_string(),
                data: Some(b"hello".to_vec()),
                context: FilesystemRowContext::active_branch("branch-a"),
            },
            &mut test_id_generator(&[]),
        )
        .expect("file descriptor write should plan");

        assert_eq!(plan.count, 1);
        assert_eq!(plan.file_data.len(), 1);
        assert_eq!(plan.file_data[0].file_id, "file-readme");
        assert_eq!(plan.file_data[0].path.as_deref(), Some("/docs/readme.md"));
        assert_eq!(plan.file_data[0].data(), b"hello");
        assert_eq!(plan.rows.len(), 2);
        let file_row = plan
            .rows
            .iter()
            .find(|row| row.schema_key == "lix_file_descriptor")
            .expect("file descriptor row should be planned");
        let snapshot: JsonValue = file_row.snapshot.as_ref().unwrap().value().clone();
        assert_eq!(snapshot["id"], "file-readme");
        assert_eq!(snapshot["directory_id"], "dir-docs");
        assert_eq!(snapshot["name"], "readme.md");
    }

    #[test]
    fn file_path_planners_reject_representative_invalid_paths_before_staging() {
        let file_error = LixPath::try_from_file_path("/docs/")
            .expect_err("directory-looking path should not parse as a file");
        assert!(!file_error.message.is_empty());

        let mut directory_resolver =
            DirectoryPathResolver::from_existing([]).expect("empty resolver should build");
        let directory_error = directory_resolver
            .ensure_directory_path(
                "/docs",
                FilesystemRowContext::active_branch("branch-a"),
                &mut test_id_generator(&["should-not-be-used"]),
            )
            .expect_err("file-looking path should not plan as a directory");
        assert!(!directory_error.message.is_empty());
        assert_eq!(directory_resolver.directory_id("/docs/").unwrap(), None);
    }

    #[test]
    fn directory_path_resolver_rejects_namespace_conflicts() {
        let mut existing_file_resolver = DirectoryPathResolver::from_existing_filesystem(
            std::iter::empty(),
            [(None, "docs".to_string(), "file-docs".to_string())],
        )
        .expect("resolver should seed existing file");
        let error = existing_file_resolver
            .ensure_directory_path(
                "/docs/",
                FilesystemRowContext::active_branch("branch-a"),
                &mut test_id_generator(&["dir-docs"]),
            )
            .expect_err("existing file should block directory with same name");
        assert_eq!(error.code, crate::LixError::CODE_UNIQUE);

        let mut existing_directory_resolver =
            DirectoryPathResolver::from_existing([("/docs/".to_string(), "dir-docs".to_string())])
                .expect("resolver should seed existing directory");
        let error = plan_parsed_file_path_write(
            &mut existing_directory_resolver,
            parsed_file_path("/docs"),
            Some("file-docs".to_string()),
            None,
            FilesystemRowContext::active_branch("branch-a"),
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect_err("existing directory should block file with same name");
        assert_eq!(error.code, crate::LixError::CODE_UNIQUE);

        let mut duplicate_file_resolver =
            DirectoryPathResolver::from_existing([]).expect("empty resolver should build");
        plan_parsed_file_path_write(
            &mut duplicate_file_resolver,
            parsed_file_path("/readme.md"),
            Some("file-first".to_string()),
            None,
            FilesystemRowContext::active_branch("branch-a"),
            &mut test_id_generator(&[]),
        )
        .expect("first file should plan");
        let error = plan_parsed_file_path_write(
            &mut duplicate_file_resolver,
            parsed_file_path("/readme.md"),
            Some("file-second".to_string()),
            None,
            FilesystemRowContext::active_branch("branch-a"),
            &mut test_id_generator(&[]),
        )
        .expect_err("same path with different file id should conflict");
        assert_eq!(error.code, crate::LixError::CODE_UNIQUE);
    }

    #[test]
    fn directory_path_resolver_rejects_duplicate_explicit_directory_create() {
        let mut resolver =
            DirectoryPathResolver::from_existing([]).expect("empty resolver should build");

        create_directory_path_with_leaf_id(
            &mut resolver,
            "/docs/",
            Some("dir-docs".to_string()),
            FilesystemRowContext::active_branch("branch-a"),
            &mut test_id_generator(&[]),
        )
        .expect("first explicit directory create should plan");

        let error = create_directory_path_with_leaf_id(
            &mut resolver,
            "/docs/",
            Some("dir-docs-again".to_string()),
            FilesystemRowContext::active_branch("branch-a"),
            &mut test_id_generator(&[]),
        )
        .expect_err("duplicate explicit directory create should be rejected");
        assert_eq!(error.code, crate::LixError::CODE_UNIQUE);
    }

    #[test]
    fn file_path_update_reuses_existing_parent_and_preserves_data() {
        let mut resolver =
            DirectoryPathResolver::from_existing([("/docs/".to_string(), "dir-docs".to_string())])
                .expect("existing directories should seed");

        let plan = plan_parsed_file_path_update(
            &mut resolver,
            "file-readme".to_string(),
            parsed_file_path("/docs/renamed.md"),
            FilesystemRowContext::active_branch("branch-a"),
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect("file path update should plan");

        assert_eq!(plan.count, 1);
        assert!(plan.file_data.is_empty());
        assert_eq!(plan.rows.len(), 1);
        assert!(
            plan.rows
                .iter()
                .all(|row| row.schema_key != "lix_binary_blob_ref")
        );

        let snapshot: JsonValue = plan.rows[0].snapshot.as_ref().unwrap().value().clone();
        assert_eq!(snapshot["id"], "file-readme");
        assert_eq!(snapshot["directory_id"], "dir-docs");
        assert_eq!(snapshot["name"], "renamed.md");
    }

    #[test]
    fn file_path_update_stages_missing_parent_directories() {
        let mut resolver =
            DirectoryPathResolver::from_existing([]).expect("empty resolver should build");

        let plan = plan_parsed_file_path_update(
            &mut resolver,
            "file-readme".to_string(),
            parsed_file_path("/docs/guides/readme.md"),
            FilesystemRowContext::active_branch("branch-a"),
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
        assert!(
            plan.rows
                .iter()
                .all(|row| row.schema_key != "lix_binary_blob_ref")
        );

        let file_row = plan
            .rows
            .iter()
            .find(|row| row.schema_key == "lix_file_descriptor")
            .expect("file descriptor row should be planned");
        let snapshot: JsonValue = file_row.snapshot.as_ref().unwrap().value().clone();
        assert_eq!(snapshot["directory_id"], "dir-generated-guides");
        assert_eq!(snapshot["name"], "readme.md");
    }

    #[test]
    fn filesystem_rows_propagate_partial_ids_and_context() {
        let metadata = TransactionJson::from_value_for_test(json!({"source":"filesystem-test"}));
        let context = FilesystemRowContext {
            branch_id: "branch-a".to_string(),
            global: true,
            untracked: true,
            file_id: Some("context-file".to_string()),
            metadata: Some(metadata.clone()),
        };

        let directory_row = directory_descriptor_write_row(DirectoryDescriptorWriteIntent {
            id: None,
            parent_id: Some("dir-parent".to_string()),
            name: "docs".to_string(),
            context: context.clone(),
        });
        assert_eq!(directory_row.entity_pk, None);
        assert_eq!(directory_row.schema_key, "lix_directory_descriptor");
        assert!(
            directory_row
                .snapshot
                .as_ref()
                .unwrap()
                .value()
                .get("id")
                .is_none()
        );
        assert_eq!(directory_row.global, true);
        assert_eq!(directory_row.untracked, true);
        assert_eq!(directory_row.file_id.as_deref(), Some("context-file"));
        assert_eq!(directory_row.metadata.as_ref(), Some(&metadata));

        let file_row = file_descriptor_write_row(FileDescriptorWriteIntent {
            id: None,
            directory_id: Some("dir-parent".to_string()),
            name: "readme.md".to_string(),
            context: context.clone(),
        });
        assert_eq!(file_row.entity_pk, None);
        assert_eq!(file_row.schema_key, "lix_file_descriptor");
        assert!(
            file_row
                .snapshot
                .as_ref()
                .unwrap()
                .value()
                .get("id")
                .is_none()
        );
        assert_eq!(file_row.file_id.as_deref(), Some("context-file"));
        assert_eq!(file_row.metadata.as_ref(), Some(&metadata));

        let mut resolver =
            DirectoryPathResolver::from_existing([]).expect("empty resolver should build");
        let plan = plan_parsed_file_path_write(
            &mut resolver,
            parsed_file_path("/docs/readme.md"),
            Some("file-readme".to_string()),
            Some(b"hello".to_vec()),
            context,
            &mut test_id_generator(&["dir-docs"]),
        )
        .expect("file path write should plan");

        let directory = plan
            .rows
            .iter()
            .find(|row| row.schema_key == "lix_directory_descriptor")
            .expect("directory descriptor should be planned");
        assert_eq!(directory.global, true);
        assert_eq!(directory.untracked, true);
        assert_eq!(directory.file_id, None);
        assert_eq!(directory.metadata.as_ref(), Some(&metadata));

        let descriptor = plan
            .rows
            .iter()
            .find(|row| row.schema_key == "lix_file_descriptor")
            .expect("file descriptor should be planned");
        assert_eq!(descriptor.global, true);
        assert_eq!(descriptor.untracked, true);
        assert_eq!(descriptor.file_id.as_deref(), Some("context-file"));
        assert_eq!(descriptor.metadata.as_ref(), Some(&metadata));

        let blob = plan
            .rows
            .iter()
            .find(|row| row.schema_key == "lix_binary_blob_ref")
            .expect("blob ref should be planned");
        assert_eq!(blob.global, true);
        assert_eq!(blob.untracked, true);
        assert_eq!(blob.file_id.as_deref(), Some("file-readme"));
        assert_eq!(blob.metadata, None);

        assert_eq!(plan.file_data.len(), 1);
        assert_eq!(plan.file_data[0].file_id, "file-readme");
        assert_eq!(plan.file_data[0].branch_id, "branch-a");
        assert_eq!(plan.file_data[0].untracked, true);
        assert_eq!(plan.file_data[0].data(), b"hello");
    }

    #[test]
    fn file_path_write_carries_empty_payload_without_blob_ref() {
        let mut resolver =
            DirectoryPathResolver::from_existing([]).expect("empty resolver should build");
        let plan = plan_parsed_file_path_write(
            &mut resolver,
            parsed_file_path("/empty.txt"),
            Some("file-empty".to_string()),
            Some(Vec::new()),
            FilesystemRowContext::active_branch("branch-a"),
            &mut test_id_generator(&[]),
        )
        .expect("empty file path write should plan");

        assert_eq!(plan.count, 1);
        assert_eq!(plan.file_data.len(), 1);
        assert_eq!(plan.file_data[0].file_id, "file-empty");
        assert_eq!(plan.file_data[0].data(), b"");
        assert!(
            plan.rows
                .iter()
                .any(|row| row.schema_key == "lix_file_descriptor")
        );
        assert!(
            plan.rows
                .iter()
                .any(|row| row.schema_key == "lix_binary_blob_ref")
        );
    }

    #[test]
    fn directory_path_resolvers_from_state_rows_derives_nested_paths() {
        let resolvers = super::directory_path_resolvers_from_state_rows(vec![
            live_directory_row(
                "dir-docs",
                "branch-a",
                "{\"id\":\"dir-docs\",\"parent_id\":null,\"name\":\"docs\"}",
            ),
            live_directory_row(
                "dir-guides",
                "branch-a",
                "{\"id\":\"dir-guides\",\"parent_id\":\"dir-docs\",\"name\":\"guides\"}",
            ),
        ])
        .expect("state rows should seed directory resolvers");

        let resolver = resolvers
            .get(&super::filesystem_storage_scope_key(
                "branch-a", false, false, None,
            ))
            .expect("storage-scope resolver should exist");
        assert_eq!(resolver.directory_id("/docs/").unwrap(), Some("dir-docs"));
        assert_eq!(
            resolver.directory_id("/docs/guides/").unwrap(),
            Some("dir-guides")
        );
    }

    #[test]
    fn directory_path_resolvers_from_state_rows_handles_parent_cycles() {
        let error = super::directory_path_resolvers_from_state_rows(vec![
            live_directory_row(
                "dir-a",
                "branch-a",
                "{\"id\":\"dir-a\",\"parent_id\":\"dir-b\",\"name\":\"a\"}",
            ),
            live_directory_row(
                "dir-b",
                "branch-a",
                "{\"id\":\"dir-b\",\"parent_id\":\"dir-a\",\"name\":\"b\"}",
            ),
        ])
        .expect_err("cyclic directory parent graph should be rejected");

        assert_eq!(error.code, crate::LixError::CODE_CONSTRAINT_VIOLATION);
        assert!(error.message.contains("parent_id cycle"));
    }

    #[test]
    fn directory_path_resolvers_from_state_rows_separates_storage_scopes() {
        let rows = vec![
            live_directory_row_with_scope(
                "dir-branch-a",
                "branch-a",
                false,
                false,
                None,
                "{\"id\":\"dir-branch-a\",\"parent_id\":null,\"name\":\"docs\"}",
            ),
            live_directory_row_with_scope(
                "dir-branch-b",
                "branch-b",
                false,
                false,
                None,
                "{\"id\":\"dir-branch-b\",\"parent_id\":null,\"name\":\"docs\"}",
            ),
            live_directory_row_with_scope(
                "dir-global",
                "branch-a",
                true,
                false,
                None,
                "{\"id\":\"dir-global\",\"parent_id\":null,\"name\":\"docs\"}",
            ),
            live_directory_row_with_scope(
                "dir-untracked",
                "branch-a",
                false,
                true,
                None,
                "{\"id\":\"dir-untracked\",\"parent_id\":null,\"name\":\"docs\"}",
            ),
            live_directory_row_with_scope(
                "dir-file-scoped",
                "branch-a",
                false,
                false,
                Some("scope-file".to_string()),
                "{\"id\":\"dir-file-scoped\",\"parent_id\":null,\"name\":\"docs\"}",
            ),
        ];

        let resolvers = super::directory_path_resolvers_from_state_rows(rows)
            .expect("scoped rows should seed distinct resolvers");

        let branch_a_key = super::filesystem_storage_scope_key("branch-a", false, false, None);
        let branch_b_key = super::filesystem_storage_scope_key("branch-b", false, false, None);
        let global_key = super::filesystem_storage_scope_key(GLOBAL_BRANCH_ID, true, false, None);
        let untracked_key = super::filesystem_storage_scope_key("branch-a", false, true, None);
        let file_scoped_key =
            super::filesystem_storage_scope_key("branch-a", false, false, Some("scope-file"));
        let literal_null_file_id_key =
            super::filesystem_storage_scope_key("branch-a", false, false, Some("<null>"));

        assert_ne!(branch_a_key, branch_b_key);
        assert_ne!(branch_a_key, global_key);
        assert_ne!(branch_a_key, untracked_key);
        assert_ne!(branch_a_key, file_scoped_key);
        assert_ne!(branch_a_key, literal_null_file_id_key);

        assert_eq!(
            resolvers
                .get(&branch_a_key)
                .unwrap()
                .directory_id("/docs/")
                .unwrap(),
            Some("dir-branch-a")
        );
        assert_eq!(
            resolvers
                .get(&branch_b_key)
                .unwrap()
                .directory_id("/docs/")
                .unwrap(),
            Some("dir-branch-b")
        );
        assert_eq!(
            resolvers
                .get(&global_key)
                .unwrap()
                .directory_id("/docs/")
                .unwrap(),
            Some("dir-global")
        );
        assert_eq!(
            resolvers
                .get(&untracked_key)
                .unwrap()
                .directory_id("/docs/")
                .unwrap(),
            Some("dir-untracked")
        );
        assert_eq!(
            resolvers
                .get(&file_scoped_key)
                .unwrap()
                .directory_id("/docs/")
                .unwrap(),
            Some("dir-file-scoped")
        );
    }

    #[test]
    fn file_delete_plans_descriptor_and_blob_ref_tombstones() {
        let plan = super::plan_file_delete(FileDeleteInput {
            file_id: "file-readme".to_string(),
            has_blob_ref: true,
            context: FilesystemRowContext::active_branch("branch-a"),
        });

        assert_eq!(plan.count, 1);
        assert_eq!(plan.rows.len(), 2);
        let descriptor = plan
            .rows
            .iter()
            .find(|row| row.schema_key == "lix_file_descriptor")
            .expect("file descriptor tombstone should be planned");
        assert_eq!(
            descriptor.entity_pk.as_ref(),
            Some(&EntityPk::single("file-readme"))
        );
        assert_eq!(descriptor.file_id, None);
        assert_eq!(descriptor.snapshot, None);

        let blob_ref = plan
            .rows
            .iter()
            .find(|row| row.schema_key == "lix_binary_blob_ref")
            .expect("blob ref tombstone should be planned");
        assert_eq!(
            blob_ref.entity_pk.as_ref(),
            Some(&EntityPk::single("file-readme"))
        );
        assert_eq!(blob_ref.file_id.as_deref(), Some("file-readme"));
        assert_eq!(blob_ref.snapshot, None);
    }

    #[test]
    fn file_delete_without_blob_ref_plans_only_descriptor_tombstone() {
        let plan = super::plan_file_delete(FileDeleteInput {
            file_id: "file-readme".to_string(),
            has_blob_ref: false,
            context: FilesystemRowContext::active_branch("branch-a"),
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
            context: FilesystemRowContext::active_branch("branch-a"),
        });

        assert_eq!(plan.count, 1);
        assert_eq!(plan.rows.len(), 1);
        assert_eq!(
            plan.rows[0].entity_pk.as_ref(),
            Some(&EntityPk::single("dir-docs"))
        );
        assert_eq!(plan.rows[0].schema_key, "lix_directory_descriptor");
        assert_eq!(plan.rows[0].file_id, None);
        assert_eq!(plan.rows[0].snapshot, None);
    }

    #[test]
    fn recursive_directory_delete_handles_empty_directory() {
        let plan = super::plan_recursive_directory_delete(
            "dir-empty",
            &VisibleFilesystem::default(),
            FilesystemRowContext::active_branch("branch-a"),
        );

        assert_eq!(plan.count, 1);
        assert_eq!(plan.rows.len(), 1);
        assert_eq!(plan.rows[0].schema_key, "lix_directory_descriptor");
        assert_eq!(
            plan.rows[0].entity_pk.as_ref(),
            Some(&EntityPk::single("dir-empty"))
        );
        assert_eq!(plan.rows[0].snapshot, None);
    }

    #[test]
    fn recursive_directory_delete_plans_files_blobs_and_deepest_directories_first() {
        let context = FilesystemRowContext::active_branch("branch-a");
        let mut directory_children_by_parent_id = BTreeMap::new();
        directory_children_by_parent_id.insert(
            Some(FilesystemDescriptorKey::from_context(&context, "dir-docs")),
            BTreeSet::from(["dir-guides".to_string()]),
        );

        let mut files_by_directory_id = BTreeMap::new();
        files_by_directory_id.insert(
            Some(FilesystemDescriptorKey::from_context(
                &context,
                "dir-guides",
            )),
            BTreeSet::from(["file-readme".to_string()]),
        );
        files_by_directory_id.insert(
            Some(FilesystemDescriptorKey::from_context(&context, "dir-docs")),
            BTreeSet::from(["file-index".to_string()]),
        );

        let visible_filesystem = VisibleFilesystem {
            directory_children_by_parent_id,
            files_by_directory_id,
            blob_refs_by_key: BTreeSet::from([FilesystemBlobRefKey::from_context(
                &context,
                "file-readme",
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
                        row.entity_pk
                            .as_ref()
                            .expect("planned recursive delete row should carry entity_pk")
                            .as_single_string_owned()
                            .expect("planned recursive delete row should project entity_pk"),
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

    fn live_directory_row(
        entity_pk: &str,
        branch_id: &str,
        snapshot_content: &str,
    ) -> MaterializedLiveStateRow {
        live_directory_row_with_scope(entity_pk, branch_id, false, false, None, snapshot_content)
    }

    fn live_directory_row_with_scope(
        entity_pk: &str,
        branch_id: &str,
        global: bool,
        untracked: bool,
        file_id: Option<String>,
        snapshot_content: &str,
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_pk: EntityPk::single(entity_pk),
            schema_key: "lix_directory_descriptor".to_string(),
            file_id,
            snapshot_content: Some(snapshot_content.to_string()),
            metadata: None,
            deleted: false,
            branch_id: branch_id.to_string(),
            change_id: Some(ChangeId::for_test_label(&format!("change-{entity_pk}"))),
            commit_id: Some(CommitId::for_test_label(&format!("commit-{entity_pk}"))),
            global,
            untracked,
            created_at: "2026-04-23T00:00:00Z".to_string(),
            updated_at: "2026-04-23T01:00:00Z".to_string(),
        }
    }
}
