use crate::common::paths::filesystem::{
    compose_directory_path, directory_ancestor_paths, directory_name_from_path,
    parent_directory_path, NormalizedDirectoryPath, ParsedFilePath,
};
use crate::contracts::artifacts::FilesystemProjectionScope;
use crate::contracts::traits::PendingView;
use crate::execution::write::filesystem::query::{
    load_directory_descriptors_by_parent_name_pairs,
    load_file_descriptors_by_directory_name_extension_triplets,
    lookup_directory_id_by_path_with_pending_transaction_view,
    lookup_directory_path_by_id_with_pending_transaction_view,
    lookup_file_id_by_path_with_pending_transaction_view, FilesystemQueryError,
};
use crate::session::write_resolution::prepared_artifacts::{
    DirectoryInsertAssignments, FileInsertAssignments,
};
use crate::version_state::GLOBAL_VERSION_ID;
use crate::LixBackend;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FilesystemPlanningError {
    pub(super) message: String,
}

impl From<FilesystemQueryError> for FilesystemPlanningError {
    fn from(error: FilesystemQueryError) -> Self {
        Self {
            message: error.message,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct PlannedDirectoryInsertTarget {
    pub(super) id: String,
    pub(super) parent_id: Option<String>,
    pub(super) name: String,
    pub(super) hidden: bool,
    pub(super) metadata: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) struct PlannedFileInsertTarget {
    pub(super) id: String,
    pub(super) directory_id: Option<String>,
    pub(super) name: String,
    pub(super) extension: Option<String>,
    pub(super) hidden: bool,
    pub(super) metadata: Option<String>,
    pub(super) data: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub(super) struct PlannedDirectoryInsertBatch {
    pub(super) directories: Vec<PlannedDirectoryInsertTarget>,
}

#[derive(Debug, Clone)]
pub(super) struct PlannedFileInsertBatch {
    pub(super) directories: Vec<PlannedDirectoryInsertTarget>,
    pub(super) files: Vec<PlannedFileInsertTarget>,
}

#[derive(Debug, Default, Clone)]
pub(super) struct FilesystemInsertSnapshot {
    existing_directory_ids_by_path: BTreeMap<String, String>,
    existing_directory_paths_by_id: BTreeMap<String, String>,
    existing_file_ids_by_path: BTreeMap<String, String>,
}

impl FilesystemInsertSnapshot {
    fn directory_id_by_path(&self, path: &str) -> Option<String> {
        self.existing_directory_ids_by_path.get(path).cloned()
    }

    fn directory_path_by_id(&self, directory_id: &str) -> Option<String> {
        self.existing_directory_paths_by_id
            .get(directory_id)
            .cloned()
    }

    fn file_id_by_path(&self, path: &str) -> Option<String> {
        self.existing_file_ids_by_path.get(path).cloned()
    }
}

#[derive(Debug, Clone)]
struct ResolvedFileInsertTarget {
    id: String,
    path: String,
    directory_path: Option<String>,
    name: String,
    extension: Option<String>,
    hidden: bool,
    metadata: Option<String>,
}

#[derive(Debug, Clone)]
struct ResolvedDirectoryInsertTarget {
    id: String,
    path: String,
    parent_path: Option<String>,
    name: String,
    hidden: bool,
    metadata: Option<String>,
}

#[derive(Debug, Default)]
struct PendingFilesystemInsertBatch {
    directories_by_path: BTreeMap<String, PendingDirectoryInsert>,
    files_by_path: BTreeMap<String, PendingFileInsert>,
}

#[derive(Debug, Clone)]
struct PendingDirectoryInsert {
    explicit: bool,
    target: ResolvedDirectoryInsertTarget,
}

#[derive(Debug, Clone)]
struct PendingFileInsert {
    target: ResolvedFileInsertTarget,
    data: Option<Vec<u8>>,
}

pub(super) async fn build_directory_insert_snapshot(
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&dyn PendingView>,
    assignments: &[DirectoryInsertAssignments],
    version_id: &str,
    lookup_scope: FilesystemProjectionScope,
) -> Result<FilesystemInsertSnapshot, FilesystemPlanningError> {
    let requested_directory_ids = collect_requested_parent_directory_ids(assignments);
    let existing_directory_paths_by_id = resolve_visible_directory_paths_by_id(
        backend,
        pending_transaction_view,
        version_id,
        &requested_directory_ids,
        lookup_scope,
    )
    .await?;
    let (requested_directory_paths, requested_file_paths) =
        collect_directory_insert_requests(assignments, &existing_directory_paths_by_id)?;
    let mut snapshot = build_insert_snapshot(
        backend,
        pending_transaction_view,
        version_id,
        &requested_directory_paths,
        &requested_file_paths,
        lookup_scope,
    )
    .await?;
    snapshot.existing_directory_paths_by_id = existing_directory_paths_by_id;
    Ok(snapshot)
}

pub(super) async fn build_file_insert_snapshot(
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&dyn PendingView>,
    assignments: &[FileInsertAssignments],
    version_id: &str,
    lookup_scope: FilesystemProjectionScope,
) -> Result<FilesystemInsertSnapshot, FilesystemPlanningError> {
    let (requested_directory_paths, requested_file_paths) =
        collect_file_insert_requests(assignments);
    build_insert_snapshot(
        backend,
        pending_transaction_view,
        version_id,
        &requested_directory_paths,
        &requested_file_paths,
        lookup_scope,
    )
    .await
}

pub(super) fn plan_directory_insert_batch(
    snapshot: &FilesystemInsertSnapshot,
    assignments: &[DirectoryInsertAssignments],
    version_id: &str,
) -> Result<PlannedDirectoryInsertBatch, FilesystemPlanningError> {
    let mut batch = PendingFilesystemInsertBatch::default();
    for assignments in assignments {
        let computed =
            resolve_directory_insert_target(snapshot, assignments, version_id, &mut batch)?;
        batch.register_directory_target(computed)?;
    }
    Ok(PlannedDirectoryInsertBatch {
        directories: finalize_pending_directory_insert_batch(snapshot, &batch),
    })
}

pub(super) fn plan_file_insert_batch(
    snapshot: &FilesystemInsertSnapshot,
    assignments: &[FileInsertAssignments],
    version_id: &str,
) -> Result<PlannedFileInsertBatch, FilesystemPlanningError> {
    let mut batch = PendingFilesystemInsertBatch::default();
    for assignments in assignments {
        let computed = resolve_file_insert_target(snapshot, assignments, version_id, &mut batch)?;
        batch.register_file_target(computed, assignments.data.clone())?;
    }
    finalize_pending_file_insert_batch(snapshot, &batch)
}

impl PendingFilesystemInsertBatch {
    fn pending_directory_id_by_path(&self, path: &str) -> Option<String> {
        self.directories_by_path
            .get(path)
            .map(|pending| pending.target.id.clone())
    }

    fn pending_directory_path_by_id(&self, directory_id: &str) -> Option<String> {
        self.directories_by_path.values().find_map(|pending| {
            (pending.target.id == directory_id).then(|| pending.target.path.clone())
        })
    }

    fn pending_file_id_by_path(&self, path: &str) -> Option<String> {
        self.files_by_path
            .get(path)
            .map(|pending| pending.target.id.clone())
    }

    fn directory_is_explicit(&self, path: &str) -> bool {
        self.directories_by_path
            .get(path)
            .is_some_and(|pending| pending.explicit)
    }

    fn register_implicit_directory(
        &mut self,
        version_id: &str,
        path: &str,
    ) -> Result<(), FilesystemPlanningError> {
        if self.directories_by_path.contains_key(path) {
            return Ok(());
        }
        let target = ResolvedDirectoryInsertTarget {
            id: auto_directory_id(version_id, path),
            path: path.to_string(),
            parent_path: parent_directory_path(path),
            name: directory_name_from_path(path).unwrap_or_default(),
            hidden: false,
            metadata: None,
        };
        self.directories_by_path.insert(
            path.to_string(),
            PendingDirectoryInsert {
                explicit: false,
                target,
            },
        );
        self.ensure_unique_directory_ids()
    }

    fn register_directory_target(
        &mut self,
        target: ResolvedDirectoryInsertTarget,
    ) -> Result<(), FilesystemPlanningError> {
        let file_collision_path = target.path.trim_end_matches('/').to_string();
        if self.files_by_path.contains_key(&file_collision_path) {
            return Err(FilesystemPlanningError {
                message: format!(
                    "Directory path collides with file path already inserted in this statement: {}",
                    file_collision_path
                ),
            });
        }

        match self.directories_by_path.entry(target.path.clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(PendingDirectoryInsert {
                    explicit: true,
                    target,
                });
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                if entry.get().explicit {
                    return Err(FilesystemPlanningError {
                        message: format!(
                            "Unique constraint violation: directory path '{}' already exists in this INSERT",
                            target.path
                        ),
                    });
                }
                entry.insert(PendingDirectoryInsert {
                    explicit: true,
                    target,
                });
            }
        }
        self.ensure_unique_directory_ids()
    }

    fn register_file_target(
        &mut self,
        target: ResolvedFileInsertTarget,
        data: Option<Vec<u8>>,
    ) -> Result<(), FilesystemPlanningError> {
        let directory_collision_path = format!("{}/", target.path.trim_end_matches('/'));
        if self
            .directories_by_path
            .contains_key(&directory_collision_path)
        {
            return Err(FilesystemPlanningError {
                message: format!(
                    "File path collides with directory path already inserted in this statement: {}",
                    directory_collision_path
                ),
            });
        }
        if self.files_by_path.contains_key(&target.path) {
            return Err(FilesystemPlanningError {
                message: format!(
                    "Unique constraint violation: file path '{}' already exists in this INSERT",
                    target.path
                ),
            });
        }
        self.files_by_path
            .insert(target.path.clone(), PendingFileInsert { target, data });
        self.ensure_unique_file_ids()
    }

    fn ensure_unique_directory_ids(&self) -> Result<(), FilesystemPlanningError> {
        let mut ids = BTreeMap::<String, String>::new();
        for pending in self.directories_by_path.values() {
            if let Some(existing_path) =
                ids.insert(pending.target.id.clone(), pending.target.path.clone())
            {
                if existing_path != pending.target.path {
                    return Err(FilesystemPlanningError {
                        message: format!(
                            "public filesystem directory insert produced duplicate id '{}' for paths '{}' and '{}'",
                            pending.target.id, existing_path, pending.target.path
                        ),
                    });
                }
            }
        }
        Ok(())
    }

    fn ensure_unique_file_ids(&self) -> Result<(), FilesystemPlanningError> {
        let mut ids = BTreeMap::<String, String>::new();
        for pending in self.files_by_path.values() {
            if let Some(existing_path) =
                ids.insert(pending.target.id.clone(), pending.target.path.clone())
            {
                if existing_path != pending.target.path {
                    return Err(FilesystemPlanningError {
                        message: format!(
                            "public filesystem file insert produced duplicate id '{}' for paths '{}' and '{}'",
                            pending.target.id, existing_path, pending.target.path
                        ),
                    });
                }
            }
        }
        Ok(())
    }
}

fn collect_requested_parent_directory_ids(
    assignments: &[DirectoryInsertAssignments],
) -> BTreeSet<String> {
    assignments
        .iter()
        .filter_map(|assignments| assignments.parent_id.clone())
        .collect()
}

fn collect_directory_insert_requests(
    assignments: &[DirectoryInsertAssignments],
    existing_directory_paths_by_id: &BTreeMap<String, String>,
) -> Result<(BTreeSet<String>, BTreeSet<String>), FilesystemPlanningError> {
    let mut requested_directory_paths = BTreeSet::new();
    let mut requested_file_paths = BTreeSet::new();

    for assignments in assignments {
        if let Some(path) = assignments.path.as_ref() {
            register_directory_path_with_ancestors(
                &mut requested_directory_paths,
                &mut requested_file_paths,
                path.as_str(),
            );
            continue;
        }

        let Some(name) = assignments.name.as_deref() else {
            continue;
        };
        let parent_path = match assignments.parent_id.as_deref() {
            Some(parent_id) => existing_directory_paths_by_id.get(parent_id).cloned(),
            None => Some("/".to_string()),
        };
        let Some(parent_path) = parent_path else {
            continue;
        };

        if assignments.parent_id.is_some() {
            register_directory_path_request(
                &mut requested_directory_paths,
                &mut requested_file_paths,
                &parent_path,
            );
        }

        let candidate_path =
            compose_directory_path(parent_path.as_str(), name).map_err(filesystem_path_error)?;
        register_directory_path_request(
            &mut requested_directory_paths,
            &mut requested_file_paths,
            &candidate_path,
        );
    }

    Ok((requested_directory_paths, requested_file_paths))
}

fn collect_file_insert_requests(
    assignments: &[FileInsertAssignments],
) -> (BTreeSet<String>, BTreeSet<String>) {
    let mut requested_directory_paths = BTreeSet::new();
    let mut requested_file_paths = BTreeSet::new();

    for assignments in assignments {
        let file_path = assignments.path.normalized_path.as_str();
        requested_file_paths.insert(file_path.to_string());
        requested_directory_paths.insert(format!("{}/", file_path.trim_end_matches('/')));

        if let Some(directory_path) = assignments.path.directory_path.as_ref() {
            for ancestor in directory_ancestor_paths(directory_path.as_str()) {
                requested_file_paths.insert(ancestor.trim_end_matches('/').to_string());
                requested_directory_paths.insert(ancestor);
            }
            let directory_path = directory_path.as_str().to_string();
            requested_file_paths.insert(directory_path.trim_end_matches('/').to_string());
            requested_directory_paths.insert(directory_path);
        }
    }

    (requested_directory_paths, requested_file_paths)
}

fn register_directory_path_with_ancestors(
    requested_directory_paths: &mut BTreeSet<String>,
    requested_file_paths: &mut BTreeSet<String>,
    path: &str,
) {
    for ancestor in directory_ancestor_paths(path) {
        register_directory_path_request(requested_directory_paths, requested_file_paths, &ancestor);
    }
    register_directory_path_request(requested_directory_paths, requested_file_paths, path);
}

fn register_directory_path_request(
    requested_directory_paths: &mut BTreeSet<String>,
    requested_file_paths: &mut BTreeSet<String>,
    path: &str,
) {
    requested_directory_paths.insert(path.to_string());
    requested_file_paths.insert(path.trim_end_matches('/').to_string());
}

async fn build_insert_snapshot(
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&dyn PendingView>,
    version_id: &str,
    requested_directory_paths: &BTreeSet<String>,
    requested_file_paths: &BTreeSet<String>,
    lookup_scope: FilesystemProjectionScope,
) -> Result<FilesystemInsertSnapshot, FilesystemPlanningError> {
    let committed_directory_ids_by_path = resolve_committed_directory_ids_by_path(
        backend,
        version_id,
        requested_directory_paths,
        lookup_scope,
    )
    .await?;
    let committed_file_ids_by_path = resolve_committed_file_ids_by_path(
        backend,
        version_id,
        requested_file_paths,
        &committed_directory_ids_by_path,
        lookup_scope,
    )
    .await?;

    Ok(FilesystemInsertSnapshot {
        existing_directory_ids_by_path: merge_visible_directory_ids_by_path(
            backend,
            pending_transaction_view,
            version_id,
            requested_directory_paths,
            lookup_scope,
            committed_directory_ids_by_path,
        )
        .await?,
        existing_directory_paths_by_id: BTreeMap::new(),
        existing_file_ids_by_path: merge_visible_file_ids_by_path(
            backend,
            pending_transaction_view,
            version_id,
            requested_file_paths,
            lookup_scope,
            committed_file_ids_by_path,
        )
        .await?,
    })
}

async fn resolve_visible_directory_paths_by_id(
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&dyn PendingView>,
    version_id: &str,
    directory_ids: &BTreeSet<String>,
    lookup_scope: FilesystemProjectionScope,
) -> Result<BTreeMap<String, String>, FilesystemPlanningError> {
    let mut resolved = BTreeMap::new();
    for directory_id in directory_ids {
        if let Some(path) = lookup_directory_path_by_id_with_pending_transaction_view(
            backend,
            pending_transaction_view,
            version_id,
            directory_id,
            lookup_scope,
        )
        .await?
        {
            resolved.insert(directory_id.clone(), path);
        }
    }
    Ok(resolved)
}

async fn merge_visible_directory_ids_by_path(
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&dyn PendingView>,
    version_id: &str,
    requested_paths: &BTreeSet<String>,
    lookup_scope: FilesystemProjectionScope,
    committed_directory_ids_by_path: BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, FilesystemPlanningError> {
    if !pending_transaction_view.is_some_and(|view| view.has_overlays()) {
        return Ok(committed_directory_ids_by_path);
    }

    let mut resolved = BTreeMap::new();
    for path in requested_paths {
        if let Some(directory_id) = lookup_directory_id_by_path_with_pending_transaction_view(
            backend,
            pending_transaction_view,
            version_id,
            &NormalizedDirectoryPath::from_normalized(path.clone()),
            lookup_scope,
        )
        .await?
        {
            resolved.insert(path.clone(), directory_id);
        }
    }
    Ok(resolved)
}

async fn merge_visible_file_ids_by_path(
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&dyn PendingView>,
    version_id: &str,
    requested_paths: &BTreeSet<String>,
    lookup_scope: FilesystemProjectionScope,
    committed_file_ids_by_path: BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, FilesystemPlanningError> {
    if !pending_transaction_view.is_some_and(|view| view.has_overlays()) {
        return Ok(committed_file_ids_by_path);
    }

    let mut resolved = BTreeMap::new();
    for path in requested_paths {
        let parsed =
            ParsedFilePath::from_normalized_path(path.clone()).map_err(filesystem_path_error)?;
        if let Some(file_id) = lookup_file_id_by_path_with_pending_transaction_view(
            backend,
            pending_transaction_view,
            version_id,
            &parsed,
            lookup_scope,
        )
        .await?
        {
            resolved.insert(path.clone(), file_id);
        }
    }
    Ok(resolved)
}

fn resolve_file_insert_target(
    snapshot: &FilesystemInsertSnapshot,
    assignments: &FileInsertAssignments,
    version_id: &str,
    batch: &mut PendingFilesystemInsertBatch,
) -> Result<ResolvedFileInsertTarget, FilesystemPlanningError> {
    let parsed = &assignments.path;
    let explicit_id = assignments.id.as_deref();
    ensure_no_directory_at_file_path_in_snapshot(parsed.normalized_path.as_str(), snapshot, batch)?;
    let directory_path = ensure_parent_directories_for_file_insert_batch(
        version_id,
        parsed.directory_path.as_ref(),
        snapshot,
        batch,
    )?;

    if let Some(existing_id) = batch.pending_file_id_by_path(parsed.normalized_path.as_str()) {
        if explicit_id != Some(existing_id.as_str()) {
            return Err(FilesystemPlanningError {
                message: format!(
                    "Unique constraint violation: file path '{}' already exists in this INSERT",
                    parsed.normalized_path.as_str()
                ),
            });
        }
    } else if let Some(existing_id) = snapshot.file_id_by_path(parsed.normalized_path.as_str()) {
        let same_id = explicit_id
            .map(|value| value == existing_id.as_str())
            .unwrap_or(false);
        if !same_id {
            return Err(FilesystemPlanningError {
                message: format!(
                    "Unique constraint violation: file path '{}' already exists in version '{}'",
                    parsed.normalized_path.as_str(),
                    version_id
                ),
            });
        }
    }

    Ok(ResolvedFileInsertTarget {
        id: assignments
            .id
            .clone()
            .unwrap_or_else(|| auto_file_id(version_id, parsed.normalized_path.as_str())),
        path: parsed.normalized_path.as_str().to_string(),
        directory_path: directory_path.map(|path| path.as_str().to_string()),
        name: parsed.name.clone(),
        extension: parsed.extension.clone(),
        hidden: assignments.hidden,
        metadata: assignments.metadata.clone(),
    })
}

fn resolve_directory_insert_target(
    snapshot: &FilesystemInsertSnapshot,
    assignments: &DirectoryInsertAssignments,
    version_id: &str,
    batch: &mut PendingFilesystemInsertBatch,
) -> Result<ResolvedDirectoryInsertTarget, FilesystemPlanningError> {
    let explicit_id = assignments.id.as_deref();
    let explicit_parent_id = assignments.parent_id.as_deref();
    let explicit_name = assignments.name.as_deref();
    let explicit_path = assignments.path.as_ref();

    let (parent_path, name, normalized_path) = if let Some(raw_path) = explicit_path {
        let normalized_path = raw_path.as_str().to_string();
        let derived_name =
            directory_name_from_path(&normalized_path).ok_or_else(|| FilesystemPlanningError {
                message: "Directory name must be provided".to_string(),
            })?;
        let derived_parent_path = ensure_parent_directories_for_insert_batch(
            version_id,
            parent_directory_path(&normalized_path)
                .map(NormalizedDirectoryPath::from_normalized)
                .as_ref(),
            snapshot,
            batch,
        )?;
        let derived_parent_id = match derived_parent_path.as_deref() {
            Some(parent_path) => {
                lookup_directory_id_by_path_in_snapshot(parent_path, snapshot, batch)
                    .or_else(|| Some(auto_directory_id(version_id, parent_path)))
            }
            None => None,
        };

        if explicit_parent_id != derived_parent_id.as_deref() && explicit_parent_id.is_some() {
            return Err(FilesystemPlanningError {
                message: format!(
                    "Provided parent_id does not match parent derived from path {}",
                    normalized_path
                ),
            });
        }
        if let Some(name) = explicit_name {
            if name != derived_name {
                return Err(FilesystemPlanningError {
                    message: format!(
                        "Provided directory name '{}' does not match path '{}'",
                        name, normalized_path
                    ),
                });
            }
        }
        (derived_parent_path, derived_name, normalized_path)
    } else {
        let name = explicit_name
            .ok_or_else(|| FilesystemPlanningError {
                message: "Directory name must be provided".to_string(),
            })?
            .to_string();
        let parent_path = match explicit_parent_id {
            Some(parent_id) => lookup_directory_path_by_id_in_snapshot(parent_id, snapshot, batch)
                .ok_or_else(|| FilesystemPlanningError {
                    message: format!("Parent directory does not exist for id {parent_id}"),
                })?,
            None => "/".to_string(),
        };
        let computed_path =
            compose_directory_path(parent_path.as_str(), &name).map_err(filesystem_path_error)?;
        (explicit_parent_id.map(|_| parent_path), name, computed_path)
    };

    if let Some(existing_id) = batch.pending_directory_id_by_path(&normalized_path) {
        if batch.directory_is_explicit(&normalized_path)
            && explicit_id != Some(existing_id.as_str())
        {
            return Err(FilesystemPlanningError {
                message: format!(
                    "Unique constraint violation: directory path '{}' already exists in this INSERT",
                    normalized_path
                ),
            });
        }
    } else if let Some(existing_id) = snapshot.directory_id_by_path(&normalized_path) {
        let same_id = explicit_id
            .map(|value| value == existing_id.as_str())
            .unwrap_or(false);
        if !same_id {
            return Err(FilesystemPlanningError {
                message: format!(
                    "Unique constraint violation: directory path '{}' already exists in version '{}'",
                    normalized_path, version_id
                ),
            });
        }
    }
    ensure_no_file_at_directory_path_in_snapshot(&normalized_path, snapshot, batch)?;

    Ok(ResolvedDirectoryInsertTarget {
        id: assignments
            .id
            .clone()
            .unwrap_or_else(|| auto_directory_id(version_id, &normalized_path)),
        path: normalized_path,
        parent_path,
        name,
        hidden: assignments.hidden,
        metadata: assignments.metadata.clone(),
    })
}

fn ensure_parent_directories_for_insert_batch(
    version_id: &str,
    directory_path: Option<&NormalizedDirectoryPath>,
    snapshot: &FilesystemInsertSnapshot,
    batch: &mut PendingFilesystemInsertBatch,
) -> Result<Option<String>, FilesystemPlanningError> {
    let Some(directory_path) = directory_path else {
        return Ok(None);
    };

    let mut paths = directory_ancestor_paths(directory_path.as_str());
    paths.push(directory_path.as_str().to_string());

    for candidate_path in paths {
        if batch
            .pending_directory_id_by_path(&candidate_path)
            .is_some()
        {
            continue;
        }
        if snapshot.directory_id_by_path(&candidate_path).is_some() {
            continue;
        }
        ensure_no_file_at_directory_path_in_snapshot(&candidate_path, snapshot, batch)?;
        batch.register_implicit_directory(version_id, &candidate_path)?;
    }

    Ok(Some(directory_path.as_str().to_string()))
}

fn finalize_pending_directory_insert_batch(
    snapshot: &FilesystemInsertSnapshot,
    batch: &PendingFilesystemInsertBatch,
) -> Vec<PlannedDirectoryInsertTarget> {
    let mut pending_directories: Vec<_> = batch.directories_by_path.values().cloned().collect();
    pending_directories
        .sort_by_key(|pending| pending_directory_insert_sort_key(&pending.target.path));

    let mut directories = Vec::new();
    for pending in pending_directories {
        let parent_id = match pending.target.parent_path.as_deref() {
            Some(parent_path) => {
                lookup_directory_id_by_path_in_snapshot(parent_path, snapshot, batch)
            }
            None => None,
        };
        directories.push(PlannedDirectoryInsertTarget {
            id: pending.target.id,
            parent_id,
            name: pending.target.name,
            hidden: pending.target.hidden,
            metadata: pending.target.metadata,
        });
    }
    directories
}

fn ensure_parent_directories_for_file_insert_batch(
    version_id: &str,
    directory_path: Option<&NormalizedDirectoryPath>,
    snapshot: &FilesystemInsertSnapshot,
    batch: &mut PendingFilesystemInsertBatch,
) -> Result<Option<String>, FilesystemPlanningError> {
    let Some(directory_path) = directory_path else {
        return Ok(None);
    };

    let mut paths = directory_ancestor_paths(directory_path.as_str());
    paths.push(directory_path.as_str().to_string());

    for candidate_path in paths {
        if batch
            .pending_directory_id_by_path(&candidate_path)
            .is_some()
        {
            continue;
        }
        if snapshot.directory_id_by_path(&candidate_path).is_some() {
            continue;
        }
        ensure_no_file_at_directory_path_in_snapshot(&candidate_path, snapshot, batch)?;
        batch.register_implicit_directory(version_id, &candidate_path)?;
    }

    Ok(Some(directory_path.as_str().to_string()))
}

fn finalize_pending_file_insert_batch(
    snapshot: &FilesystemInsertSnapshot,
    batch: &PendingFilesystemInsertBatch,
) -> Result<PlannedFileInsertBatch, FilesystemPlanningError> {
    Ok(PlannedFileInsertBatch {
        directories: finalize_file_insert_directories(snapshot, batch),
        files: finalize_file_insert_files(snapshot, batch),
    })
}

fn finalize_file_insert_directories(
    snapshot: &FilesystemInsertSnapshot,
    batch: &PendingFilesystemInsertBatch,
) -> Vec<PlannedDirectoryInsertTarget> {
    let mut pending_directories: Vec<_> = batch.directories_by_path.values().cloned().collect();
    pending_directories
        .sort_by_key(|pending| pending_directory_insert_sort_key(&pending.target.path));

    let mut directories = Vec::new();
    for pending in pending_directories {
        let parent_id = match pending.target.parent_path.as_deref() {
            Some(parent_path) => {
                lookup_directory_id_by_path_in_snapshot(parent_path, snapshot, batch)
            }
            None => None,
        };
        directories.push(PlannedDirectoryInsertTarget {
            id: pending.target.id,
            parent_id,
            name: pending.target.name,
            hidden: pending.target.hidden,
            metadata: pending.target.metadata,
        });
    }
    directories
}

fn finalize_file_insert_files(
    snapshot: &FilesystemInsertSnapshot,
    batch: &PendingFilesystemInsertBatch,
) -> Vec<PlannedFileInsertTarget> {
    let mut pending_files: Vec<_> = batch.files_by_path.values().cloned().collect();
    pending_files.sort_by_key(|pending| pending.target.path.clone());

    let mut files = Vec::new();
    for pending in pending_files {
        let directory_id = match pending.target.directory_path.as_deref() {
            Some(directory_path) => {
                lookup_directory_id_by_path_in_snapshot(directory_path, snapshot, batch)
            }
            None => None,
        };
        files.push(PlannedFileInsertTarget {
            id: pending.target.id,
            directory_id,
            name: pending.target.name,
            extension: pending.target.extension,
            hidden: pending.target.hidden,
            metadata: pending.target.metadata,
            data: pending.data,
        });
    }
    files
}

async fn resolve_committed_directory_ids_by_path(
    backend: &dyn LixBackend,
    version_id: &str,
    requested_paths: &BTreeSet<String>,
    lookup_scope: FilesystemProjectionScope,
) -> Result<BTreeMap<String, String>, FilesystemPlanningError> {
    let mut resolved: BTreeMap<String, String> = BTreeMap::new();
    let mut by_depth = BTreeMap::<usize, Vec<String>>::new();
    for path in requested_paths {
        by_depth
            .entry(path.matches('/').count())
            .or_default()
            .push(path.clone());
    }

    for paths in by_depth.into_values() {
        let mut requests = BTreeSet::new();
        let mut request_paths: BTreeMap<(Option<String>, String), String> = BTreeMap::new();
        for path in paths {
            let parent_path = parent_directory_path(&path);
            let Some(name) = directory_name_from_path(&path) else {
                continue;
            };
            let parent_id = match parent_path.as_deref() {
                Some(parent_path) => match resolved.get(parent_path) {
                    Some(parent_id) => Some(parent_id.clone()),
                    None => continue,
                },
                None => None,
            };
            requests.insert((parent_id.clone(), name.to_string()));
            request_paths.insert((parent_id, name.to_string()), path);
        }
        if requests.is_empty() {
            continue;
        }
        for descriptor in load_directory_descriptors_by_parent_name_pairs(
            backend,
            version_id,
            &requests,
            lookup_scope,
        )
        .await?
        {
            if let Some(path) =
                request_paths.get(&(descriptor.parent_id.clone(), descriptor.name.clone()))
            {
                resolved.insert(path.clone(), descriptor.id);
            }
        }
    }

    Ok(resolved)
}

async fn resolve_committed_file_ids_by_path(
    backend: &dyn LixBackend,
    version_id: &str,
    requested_paths: &BTreeSet<String>,
    directory_ids_by_path: &BTreeMap<String, String>,
    lookup_scope: FilesystemProjectionScope,
) -> Result<BTreeMap<String, String>, FilesystemPlanningError> {
    let mut requests = BTreeSet::new();
    let mut request_paths = BTreeMap::new();
    for path in requested_paths {
        let parsed =
            ParsedFilePath::from_normalized_path(path.clone()).map_err(filesystem_path_error)?;
        let directory_id = match parsed.directory_path.as_deref() {
            Some(directory_path) => match directory_ids_by_path.get(directory_path) {
                Some(directory_id) => Some(directory_id.clone()),
                None => continue,
            },
            None => None,
        };
        let key = (
            directory_id,
            parsed.name.clone(),
            parsed.extension.clone().filter(|value| !value.is_empty()),
        );
        requests.insert(key.clone());
        request_paths.insert(key, path.clone());
    }

    let mut resolved = BTreeMap::new();
    for descriptor in load_file_descriptors_by_directory_name_extension_triplets(
        backend,
        version_id,
        &requests,
        lookup_scope,
    )
    .await?
    {
        let key = (
            descriptor.directory_id.clone(),
            descriptor.name.clone(),
            descriptor
                .extension
                .clone()
                .filter(|value| !value.is_empty()),
        );
        if let Some(path) = request_paths.get(&key) {
            resolved.insert(path.clone(), descriptor.id);
        }
    }
    Ok(resolved)
}

fn lookup_directory_id_by_path_in_snapshot(
    path: &str,
    snapshot: &FilesystemInsertSnapshot,
    batch: &PendingFilesystemInsertBatch,
) -> Option<String> {
    batch
        .pending_directory_id_by_path(path)
        .or_else(|| snapshot.directory_id_by_path(path))
}

fn lookup_directory_path_by_id_in_snapshot(
    directory_id: &str,
    snapshot: &FilesystemInsertSnapshot,
    batch: &PendingFilesystemInsertBatch,
) -> Option<String> {
    batch
        .pending_directory_path_by_id(directory_id)
        .or_else(|| snapshot.directory_path_by_id(directory_id))
}

fn ensure_no_file_at_directory_path_in_snapshot(
    directory_path: &str,
    snapshot: &FilesystemInsertSnapshot,
    batch: &PendingFilesystemInsertBatch,
) -> Result<(), FilesystemPlanningError> {
    let file_path =
        ParsedFilePath::from_normalized_path(directory_path.trim_end_matches('/').to_string())
            .map_err(filesystem_path_error)?;
    if batch
        .pending_file_id_by_path(file_path.normalized_path.as_str())
        .is_some()
    {
        return Err(FilesystemPlanningError {
            message: format!(
                "Directory path collides with existing file path: {}",
                file_path.normalized_path.as_str()
            ),
        });
    }
    if snapshot
        .file_id_by_path(file_path.normalized_path.as_str())
        .is_some()
    {
        return Err(FilesystemPlanningError {
            message: format!(
                "Directory path collides with existing file path: {}",
                file_path.normalized_path.as_str()
            ),
        });
    }
    Ok(())
}

fn ensure_no_directory_at_file_path_in_snapshot(
    file_path: &str,
    snapshot: &FilesystemInsertSnapshot,
    batch: &PendingFilesystemInsertBatch,
) -> Result<(), FilesystemPlanningError> {
    let file_path = ParsedFilePath::from_normalized_path(file_path.to_string())
        .map_err(filesystem_path_error)?;
    let directory_path = NormalizedDirectoryPath::from_normalized(format!(
        "{}/",
        file_path.normalized_path.as_str().trim_end_matches('/')
    ));
    if batch
        .pending_directory_id_by_path(directory_path.as_str())
        .is_some()
    {
        return Err(FilesystemPlanningError {
            message: format!(
                "File path collides with existing directory path: {}",
                directory_path.as_str()
            ),
        });
    }
    if snapshot
        .directory_id_by_path(directory_path.as_str())
        .is_some()
    {
        return Err(FilesystemPlanningError {
            message: format!(
                "File path collides with existing directory path: {}",
                directory_path.as_str()
            ),
        });
    }
    Ok(())
}

fn pending_directory_insert_sort_key(path: &str) -> (usize, String) {
    (path.matches('/').count(), path.to_string())
}

fn auto_directory_id(version_id: &str, path: &str) -> String {
    if version_id == GLOBAL_VERSION_ID {
        format!("dir:auto::{path}")
    } else {
        format!("dir:auto::{version_id}::{path}")
    }
}

fn auto_file_id(version_id: &str, path: &str) -> String {
    if version_id == GLOBAL_VERSION_ID {
        format!("file:auto::{path}")
    } else {
        format!("file:auto::{version_id}::{path}")
    }
}

fn filesystem_path_error(error: crate::LixError) -> FilesystemPlanningError {
    FilesystemPlanningError {
        message: error.description,
    }
}
