use crate::contracts::BlobDataReader;
use crate::contracts::CommittedStateHistoryReader;
use crate::contracts::{
    DirectoryHistoryRequest, DirectoryHistoryRow, FileHistoryContentMode, FileHistoryLineageScope,
    FileHistoryRequest, FileHistoryRootScope, FileHistoryRow, FileHistoryVersionScope,
    StateHistoryContentMode, StateHistoryLineageScope, StateHistoryRequest, StateHistoryRootScope,
    StateHistoryVersionScope,
};
use crate::{LixBackend, LixError};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";
const BINARY_BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";

#[derive(Debug, Clone)]
struct FileDescriptorHistoryRow {
    id: String,
    directory_id: Option<String>,
    name: String,
    extension: Option<String>,
    metadata: Option<String>,
    hidden: Option<bool>,
    lixcol_schema_key: String,
    lixcol_file_id: String,
    lixcol_version_id: String,
    lixcol_plugin_key: String,
    lixcol_schema_version: String,
    lixcol_change_id: String,
    lixcol_origin_commit_id: String,
    lixcol_commit_created_at: String,
    lixcol_metadata: Option<String>,
    lixcol_root_commit_id: String,
    lixcol_depth: i64,
}

#[derive(Debug, Clone)]
struct DirectoryDescriptorHistoryRow {
    id: String,
    parent_id: Option<String>,
    name: String,
    hidden: Option<bool>,
    lixcol_entity_id: String,
    lixcol_schema_key: String,
    lixcol_file_id: String,
    lixcol_version_id: String,
    lixcol_plugin_key: String,
    lixcol_schema_version: String,
    lixcol_change_id: String,
    lixcol_metadata: Option<String>,
    lixcol_commit_id: String,
    lixcol_commit_created_at: String,
    lixcol_root_commit_id: String,
    lixcol_depth: i64,
}

#[derive(Debug, Clone)]
struct BinaryBlobRefHistoryRow {
    id: String,
    lixcol_change_id: String,
    lixcol_commit_id: String,
    lixcol_commit_created_at: String,
    lixcol_root_commit_id: String,
    lixcol_depth: i64,
    blob_hash: String,
}

#[derive(Debug, Clone)]
struct FileCheckpointCandidate {
    id: String,
    lixcol_root_commit_id: String,
    lixcol_raw_depth: i64,
    lixcol_change_id: String,
    lixcol_commit_id: String,
    lixcol_commit_created_at: String,
}

#[derive(Debug, Clone)]
struct FileCheckpointRow {
    id: String,
    lixcol_root_commit_id: String,
    lixcol_raw_depth: i64,
    lixcol_change_id: String,
    lixcol_commit_id: String,
    lixcol_commit_created_at: String,
    lixcol_depth: i64,
}

#[derive(Debug, Deserialize)]
struct FileDescriptorSnapshot {
    id: String,
    directory_id: Option<String>,
    name: String,
    extension: Option<String>,
    metadata: Option<JsonValue>,
    hidden: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct DirectoryDescriptorSnapshot {
    id: String,
    parent_id: Option<String>,
    name: String,
    hidden: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct BinaryBlobRefSnapshot {
    blob_hash: String,
}

pub(crate) async fn load_file_history_rows(
    backend: &dyn LixBackend,
    request: &FileHistoryRequest,
) -> Result<Vec<FileHistoryRow>, LixError> {
    let state_rows = backend
        .load_committed_state_history_rows(&StateHistoryRequest {
            lineage_scope: match request.lineage_scope {
                FileHistoryLineageScope::ActiveVersion => StateHistoryLineageScope::ActiveVersion,
                FileHistoryLineageScope::Standard => StateHistoryLineageScope::Standard,
            },
            active_version_id: request.active_version_id.clone(),
            root_scope: match &request.root_scope {
                FileHistoryRootScope::AllRoots => StateHistoryRootScope::AllRoots,
                FileHistoryRootScope::RequestedRoots(root_commit_ids) => {
                    StateHistoryRootScope::RequestedRoots(root_commit_ids.clone())
                }
            },
            version_scope: match &request.version_scope {
                FileHistoryVersionScope::Any => StateHistoryVersionScope::Any,
                FileHistoryVersionScope::RequestedVersions(version_ids) => {
                    StateHistoryVersionScope::RequestedVersions(version_ids.clone())
                }
            },
            file_ids: request.file_ids.clone(),
            schema_keys: vec![
                FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
                BINARY_BLOB_REF_SCHEMA_KEY.to_string(),
            ],
            content_mode: StateHistoryContentMode::IncludeSnapshotContent,
            ..StateHistoryRequest::default()
        })
        .await?;

    let mut file_descriptors = Vec::new();
    let mut directory_rows = Vec::new();
    let mut blob_rows = Vec::new();

    for row in state_rows {
        match row.schema_key.as_str() {
            FILE_DESCRIPTOR_SCHEMA_KEY => {
                let Some(snapshot_content) = row.snapshot_content.as_deref() else {
                    continue;
                };
                let snapshot: FileDescriptorSnapshot =
                    serde_json::from_str(snapshot_content).map_err(|error| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!(
                                "filesystem history: invalid lix_file_descriptor snapshot_content: {error}"
                            ),
                        )
                    })?;
                file_descriptors.push(FileDescriptorHistoryRow {
                    id: snapshot.id,
                    directory_id: snapshot.directory_id,
                    name: snapshot.name,
                    extension: snapshot.extension.filter(|value| !value.is_empty()),
                    metadata: snapshot.metadata.map(|value| value.to_string()),
                    hidden: snapshot.hidden,
                    lixcol_schema_key: row.schema_key,
                    lixcol_file_id: row.file_id,
                    lixcol_version_id: row.version_id,
                    lixcol_plugin_key: row.plugin_key,
                    lixcol_schema_version: row.schema_version,
                    lixcol_change_id: row.change_id,
                    lixcol_origin_commit_id: row.commit_id,
                    lixcol_commit_created_at: row.commit_created_at,
                    lixcol_metadata: row.metadata,
                    lixcol_root_commit_id: row.root_commit_id,
                    lixcol_depth: row.depth,
                });
            }
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY => {
                let Some(snapshot_content) = row.snapshot_content.as_deref() else {
                    continue;
                };
                let snapshot: DirectoryDescriptorSnapshot =
                    serde_json::from_str(snapshot_content).map_err(|error| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!(
                                "filesystem history: invalid lix_directory_descriptor snapshot_content: {error}"
                            ),
                        )
                    })?;
                directory_rows.push(DirectoryDescriptorHistoryRow {
                    id: snapshot.id,
                    parent_id: snapshot.parent_id,
                    name: snapshot.name,
                    hidden: snapshot.hidden,
                    lixcol_entity_id: row.entity_id,
                    lixcol_schema_key: row.schema_key,
                    lixcol_file_id: row.file_id,
                    lixcol_version_id: row.version_id,
                    lixcol_plugin_key: row.plugin_key,
                    lixcol_schema_version: row.schema_version,
                    lixcol_change_id: row.change_id,
                    lixcol_metadata: row.metadata,
                    lixcol_commit_id: row.commit_id,
                    lixcol_commit_created_at: row.commit_created_at,
                    lixcol_root_commit_id: row.root_commit_id,
                    lixcol_depth: row.depth,
                });
            }
            BINARY_BLOB_REF_SCHEMA_KEY => {
                let Some(snapshot_content) = row.snapshot_content.as_deref() else {
                    continue;
                };
                let snapshot: BinaryBlobRefSnapshot =
                    serde_json::from_str(snapshot_content).map_err(|error| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!(
                                "filesystem history: invalid lix_binary_blob_ref snapshot_content: {error}"
                            ),
                        )
                    })?;
                blob_rows.push(BinaryBlobRefHistoryRow {
                    id: row.file_id,
                    lixcol_change_id: row.change_id,
                    lixcol_commit_id: row.commit_id,
                    lixcol_commit_created_at: row.commit_created_at,
                    lixcol_root_commit_id: row.root_commit_id,
                    lixcol_depth: row.depth,
                    blob_hash: snapshot.blob_hash,
                });
            }
            _ => {}
        }
    }

    let mut file_descriptors_by_key: BTreeMap<(String, String), Vec<FileDescriptorHistoryRow>> =
        BTreeMap::new();
    for descriptor in file_descriptors {
        file_descriptors_by_key
            .entry((
                descriptor.lixcol_root_commit_id.clone(),
                descriptor.id.clone(),
            ))
            .or_default()
            .push(descriptor);
    }
    for rows in file_descriptors_by_key.values_mut() {
        rows.sort_by_key(|row| row.lixcol_depth);
    }

    let mut directory_rows_by_key: BTreeMap<(String, String), Vec<DirectoryDescriptorHistoryRow>> =
        BTreeMap::new();
    for row in directory_rows {
        directory_rows_by_key
            .entry((row.lixcol_root_commit_id.clone(), row.id.clone()))
            .or_default()
            .push(row);
    }
    for rows in directory_rows_by_key.values_mut() {
        rows.sort_by_key(|row| row.lixcol_depth);
    }

    let mut blob_rows_by_key: BTreeMap<(String, String), Vec<BinaryBlobRefHistoryRow>> =
        BTreeMap::new();
    let mut blob_max_depth_by_key: BTreeMap<(String, String), i64> = BTreeMap::new();
    for row in blob_rows {
        let key = (row.lixcol_root_commit_id.clone(), row.id.clone());
        blob_max_depth_by_key
            .entry(key.clone())
            .and_modify(|current| *current = (*current).max(row.lixcol_depth))
            .or_insert(row.lixcol_depth);
        blob_rows_by_key.entry(key).or_default().push(row);
    }
    for rows in blob_rows_by_key.values_mut() {
        rows.sort_by_key(|row| row.lixcol_depth);
    }

    let mut deduped_candidates: BTreeMap<(String, String, i64), FileCheckpointCandidate> =
        BTreeMap::new();
    for ((root_commit_id, file_id), descriptors) in &file_descriptors_by_key {
        let max_blob_depth = blob_max_depth_by_key
            .get(&(root_commit_id.clone(), file_id.clone()))
            .copied();
        for descriptor in descriptors {
            if max_blob_depth.is_none_or(|depth| descriptor.lixcol_depth <= depth) {
                insert_checkpoint_candidate(
                    &mut deduped_candidates,
                    FileCheckpointCandidate {
                        id: descriptor.id.clone(),
                        lixcol_root_commit_id: root_commit_id.clone(),
                        lixcol_raw_depth: descriptor.lixcol_depth,
                        lixcol_change_id: descriptor.lixcol_change_id.clone(),
                        lixcol_commit_id: descriptor.lixcol_origin_commit_id.clone(),
                        lixcol_commit_created_at: descriptor.lixcol_commit_created_at.clone(),
                    },
                );
            }
        }

        let mut visited_directory_ids = BTreeSet::new();
        for directory_id in descriptors
            .iter()
            .filter_map(|descriptor| descriptor.directory_id.as_ref())
        {
            if !visited_directory_ids.insert(directory_id.clone()) {
                continue;
            }
            let Some(directory_history) =
                directory_rows_by_key.get(&(root_commit_id.clone(), directory_id.clone()))
            else {
                continue;
            };
            for directory in directory_history {
                if max_blob_depth.is_none_or(|depth| directory.lixcol_depth <= depth) {
                    insert_checkpoint_candidate(
                        &mut deduped_candidates,
                        FileCheckpointCandidate {
                            id: file_id.clone(),
                            lixcol_root_commit_id: root_commit_id.clone(),
                            lixcol_raw_depth: directory.lixcol_depth,
                            lixcol_change_id: directory.lixcol_change_id.clone(),
                            lixcol_commit_id: directory.lixcol_commit_id.clone(),
                            lixcol_commit_created_at: directory.lixcol_commit_created_at.clone(),
                        },
                    );
                }
            }
        }
    }

    for rows in blob_rows_by_key.values() {
        for blob in rows {
            insert_checkpoint_candidate(
                &mut deduped_candidates,
                FileCheckpointCandidate {
                    id: blob.id.clone(),
                    lixcol_root_commit_id: blob.lixcol_root_commit_id.clone(),
                    lixcol_raw_depth: blob.lixcol_depth,
                    lixcol_change_id: blob.lixcol_change_id.clone(),
                    lixcol_commit_id: blob.lixcol_commit_id.clone(),
                    lixcol_commit_created_at: blob.lixcol_commit_created_at.clone(),
                },
            );
        }
    }

    let mut checkpoints_by_file_root: BTreeMap<(String, String), Vec<FileCheckpointRow>> =
        BTreeMap::new();
    for candidate in deduped_candidates.into_values() {
        checkpoints_by_file_root
            .entry((
                candidate.lixcol_root_commit_id.clone(),
                candidate.id.clone(),
            ))
            .or_default()
            .push(FileCheckpointRow {
                id: candidate.id,
                lixcol_root_commit_id: candidate.lixcol_root_commit_id,
                lixcol_raw_depth: candidate.lixcol_raw_depth,
                lixcol_change_id: candidate.lixcol_change_id,
                lixcol_commit_id: candidate.lixcol_commit_id,
                lixcol_commit_created_at: candidate.lixcol_commit_created_at,
                lixcol_depth: 0,
            });
    }

    for rows in checkpoints_by_file_root.values_mut() {
        rows.sort_by(|left, right| {
            left.lixcol_raw_depth
                .cmp(&right.lixcol_raw_depth)
                .then_with(|| right.lixcol_commit_id.cmp(&left.lixcol_commit_id))
                .then_with(|| right.lixcol_change_id.cmp(&left.lixcol_change_id))
        });
        for (index, row) in rows.iter_mut().enumerate() {
            row.lixcol_depth = index as i64;
        }
    }

    let mut directory_path_cache: BTreeMap<(String, String, i64), Option<String>> = BTreeMap::new();
    let mut rows = Vec::new();
    let mut required_blob_hashes = BTreeSet::new();
    let mut pending_blob_hashes = Vec::new();

    for ((root_commit_id, file_id), checkpoints) in checkpoints_by_file_root {
        let Some(descriptor_rows) =
            file_descriptors_by_key.get(&(root_commit_id.clone(), file_id.clone()))
        else {
            continue;
        };
        let blob_history = blob_rows_by_key.get(&(root_commit_id.clone(), file_id.clone()));

        for checkpoint in checkpoints {
            let Some(descriptor) = descriptor_rows
                .iter()
                .find(|descriptor| descriptor.lixcol_depth >= checkpoint.lixcol_raw_depth)
            else {
                continue;
            };

            let path = match descriptor.directory_id.as_deref() {
                None => Some(render_file_name(
                    &descriptor.name,
                    descriptor.extension.as_deref(),
                )),
                Some(directory_id) => resolve_directory_path_at_depth(
                    &directory_rows_by_key,
                    &mut directory_path_cache,
                    &root_commit_id,
                    directory_id,
                    descriptor.lixcol_depth,
                )?
                .map(|directory_path| {
                    format!(
                        "{}{}",
                        directory_path,
                        render_file_name_segment(&descriptor.name, descriptor.extension.as_deref())
                    )
                }),
            };

            let blob_hash = blob_history.and_then(|rows| {
                rows.iter()
                    .find(|row| row.lixcol_depth >= checkpoint.lixcol_raw_depth)
                    .map(|row| row.blob_hash.clone())
            });
            if let Some(blob_hash) = &blob_hash {
                if request.content_mode == FileHistoryContentMode::IncludeData {
                    required_blob_hashes.insert(blob_hash.clone());
                }
            }

            pending_blob_hashes.push(blob_hash);
            rows.push(FileHistoryRow {
                id: checkpoint.id.clone(),
                path,
                data: None,
                metadata: descriptor.metadata.clone(),
                hidden: descriptor.hidden,
                lixcol_entity_id: checkpoint.id.clone(),
                lixcol_schema_key: descriptor.lixcol_schema_key.clone(),
                lixcol_file_id: if descriptor.lixcol_change_id == checkpoint.lixcol_change_id {
                    descriptor.lixcol_file_id.clone()
                } else {
                    checkpoint.id.clone()
                },
                lixcol_version_id: descriptor.lixcol_version_id.clone(),
                lixcol_plugin_key: descriptor.lixcol_plugin_key.clone(),
                lixcol_schema_version: descriptor.lixcol_schema_version.clone(),
                lixcol_change_id: checkpoint.lixcol_change_id.clone(),
                lixcol_metadata: descriptor.lixcol_metadata.clone(),
                lixcol_commit_id: checkpoint.lixcol_commit_id.clone(),
                lixcol_commit_created_at: checkpoint.lixcol_commit_created_at.clone(),
                lixcol_root_commit_id: checkpoint.lixcol_root_commit_id.clone(),
                lixcol_depth: checkpoint.lixcol_depth,
            });
        }
    }

    if request.content_mode == FileHistoryContentMode::IncludeData
        && !required_blob_hashes.is_empty()
    {
        let mut blob_data_by_hash = BTreeMap::new();
        for blob_hash in required_blob_hashes {
            blob_data_by_hash.insert(
                blob_hash.clone(),
                backend.load_blob_data_by_hash(&blob_hash).await?,
            );
        }
        for (row, blob_hash) in rows.iter_mut().zip(pending_blob_hashes.into_iter()) {
            if let Some(blob_hash) = blob_hash {
                row.data = blob_data_by_hash.get(&blob_hash).cloned().unwrap_or(None);
            }
        }
    }

    Ok(rows)
}

fn insert_checkpoint_candidate(
    deduped_candidates: &mut BTreeMap<(String, String, i64), FileCheckpointCandidate>,
    candidate: FileCheckpointCandidate,
) {
    let key = (
        candidate.id.clone(),
        candidate.lixcol_root_commit_id.clone(),
        candidate.lixcol_raw_depth,
    );
    match deduped_candidates.get(&key) {
        Some(current)
            if checkpoint_candidate_ordering(&candidate, current) != Ordering::Greater => {}
        _ => {
            deduped_candidates.insert(key, candidate);
        }
    }
}

fn checkpoint_candidate_ordering(
    left: &FileCheckpointCandidate,
    right: &FileCheckpointCandidate,
) -> Ordering {
    left.lixcol_commit_created_at
        .cmp(&right.lixcol_commit_created_at)
        .then_with(|| left.lixcol_commit_id.cmp(&right.lixcol_commit_id))
        .then_with(|| left.lixcol_change_id.cmp(&right.lixcol_change_id))
}

fn resolve_directory_path_at_depth(
    directory_rows_by_key: &BTreeMap<(String, String), Vec<DirectoryDescriptorHistoryRow>>,
    cache: &mut BTreeMap<(String, String, i64), Option<String>>,
    root_commit_id: &str,
    directory_id: &str,
    target_depth: i64,
) -> Result<Option<String>, LixError> {
    let cache_key = (
        root_commit_id.to_string(),
        directory_id.to_string(),
        target_depth,
    );
    if let Some(path) = cache.get(&cache_key) {
        return Ok(path.clone());
    }

    let Some(rows) =
        directory_rows_by_key.get(&(root_commit_id.to_string(), directory_id.to_string()))
    else {
        cache.insert(cache_key, None);
        return Ok(None);
    };
    let Some(row) = rows.iter().find(|row| row.lixcol_depth >= target_depth) else {
        cache.insert(cache_key, None);
        return Ok(None);
    };

    let path = if let Some(parent_id) = row.parent_id.as_deref() {
        resolve_directory_path_at_depth(
            directory_rows_by_key,
            cache,
            root_commit_id,
            parent_id,
            target_depth,
        )?
        .map(|parent_path| format!("{parent_path}{}/", row.name))
    } else {
        Some(format!("/{}/", row.name))
    };

    cache.insert(cache_key, path.clone());
    Ok(path)
}

fn render_file_name(name: &str, extension: Option<&str>) -> String {
    format!("/{}", render_file_name_segment(name, extension))
}

fn render_file_name_segment(name: &str, extension: Option<&str>) -> String {
    match extension {
        Some(extension) if !extension.is_empty() => format!("{name}.{extension}"),
        _ => name.to_string(),
    }
}

pub(crate) async fn load_directory_history_rows(
    backend: &dyn LixBackend,
    request: &DirectoryHistoryRequest,
) -> Result<Vec<DirectoryHistoryRow>, LixError> {
    let state_rows = backend
        .load_committed_state_history_rows(&StateHistoryRequest {
            lineage_scope: match request.lineage_scope {
                FileHistoryLineageScope::ActiveVersion => StateHistoryLineageScope::ActiveVersion,
                FileHistoryLineageScope::Standard => StateHistoryLineageScope::Standard,
            },
            active_version_id: request.active_version_id.clone(),
            root_scope: match &request.root_scope {
                FileHistoryRootScope::AllRoots => StateHistoryRootScope::AllRoots,
                FileHistoryRootScope::RequestedRoots(root_commit_ids) => {
                    StateHistoryRootScope::RequestedRoots(root_commit_ids.clone())
                }
            },
            version_scope: match &request.version_scope {
                FileHistoryVersionScope::Any => StateHistoryVersionScope::Any,
                FileHistoryVersionScope::RequestedVersions(version_ids) => {
                    StateHistoryVersionScope::RequestedVersions(version_ids.clone())
                }
            },
            schema_keys: vec![DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string()],
            content_mode: StateHistoryContentMode::IncludeSnapshotContent,
            ..StateHistoryRequest::default()
        })
        .await?;

    let mut directory_rows = Vec::new();
    for row in state_rows {
        if row.schema_key != DIRECTORY_DESCRIPTOR_SCHEMA_KEY {
            continue;
        }
        let Some(snapshot_content) = row.snapshot_content.as_deref() else {
            continue;
        };
        let snapshot: DirectoryDescriptorSnapshot =
            serde_json::from_str(snapshot_content).map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "filesystem history: invalid lix_directory_descriptor snapshot_content: {error}"
                    ),
                )
            })?;
        directory_rows.push(DirectoryDescriptorHistoryRow {
            id: snapshot.id,
            parent_id: snapshot.parent_id,
            name: snapshot.name,
            hidden: snapshot.hidden,
            lixcol_entity_id: row.entity_id,
            lixcol_schema_key: row.schema_key,
            lixcol_file_id: row.file_id,
            lixcol_version_id: row.version_id,
            lixcol_plugin_key: row.plugin_key,
            lixcol_schema_version: row.schema_version,
            lixcol_change_id: row.change_id,
            lixcol_metadata: row.metadata,
            lixcol_commit_id: row.commit_id,
            lixcol_commit_created_at: row.commit_created_at,
            lixcol_root_commit_id: row.root_commit_id,
            lixcol_depth: row.depth,
        });
    }

    let mut directory_rows_by_key: BTreeMap<(String, String), Vec<DirectoryDescriptorHistoryRow>> =
        BTreeMap::new();
    for row in directory_rows {
        directory_rows_by_key
            .entry((row.lixcol_root_commit_id.clone(), row.id.clone()))
            .or_default()
            .push(row);
    }
    for rows in directory_rows_by_key.values_mut() {
        rows.sort_by_key(|row| row.lixcol_depth);
    }

    let mut directory_path_cache: BTreeMap<(String, String, i64), Option<String>> = BTreeMap::new();
    let mut out = Vec::new();
    let requested_directory_ids = request
        .directory_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();

    for ((root_commit_id, directory_id), rows) in &directory_rows_by_key {
        if !requested_directory_ids.is_empty() && !requested_directory_ids.contains(directory_id) {
            continue;
        }
        for row in rows {
            let path = resolve_directory_path_at_depth(
                &directory_rows_by_key,
                &mut directory_path_cache,
                root_commit_id,
                directory_id,
                row.lixcol_depth,
            )?;
            out.push(DirectoryHistoryRow {
                id: row.id.clone(),
                parent_id: row.parent_id.clone(),
                name: row.name.clone(),
                path,
                hidden: row.hidden,
                lixcol_entity_id: row.lixcol_entity_id.clone(),
                lixcol_schema_key: row.lixcol_schema_key.clone(),
                lixcol_file_id: row.lixcol_file_id.clone(),
                lixcol_version_id: row.lixcol_version_id.clone(),
                lixcol_plugin_key: row.lixcol_plugin_key.clone(),
                lixcol_schema_version: row.lixcol_schema_version.clone(),
                lixcol_change_id: row.lixcol_change_id.clone(),
                lixcol_metadata: row.lixcol_metadata.clone(),
                lixcol_commit_id: row.lixcol_commit_id.clone(),
                lixcol_commit_created_at: row.lixcol_commit_created_at.clone(),
                lixcol_root_commit_id: row.lixcol_root_commit_id.clone(),
                lixcol_depth: row.lixcol_depth,
            });
        }
    }

    Ok(out)
}
