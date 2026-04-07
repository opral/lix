mod insert_planning;

use self::insert_planning::{
    build_directory_insert_snapshot, build_file_insert_snapshot, plan_directory_insert_batch,
    plan_file_insert_batch,
};
use super::*;
use crate::contracts::artifacts::FilesystemProjectionScope;
use crate::contracts::traits::PendingView;
use crate::paths::filesystem::{
    compose_directory_path, directory_ancestor_paths, directory_name_from_path,
    parent_directory_path, NormalizedDirectoryPath, ParsedFilePath,
};
use crate::sql::parser::placeholders::{resolve_placeholder_index, PlaceholderState};
use crate::sql::semantic_ir::semantics::filesystem_assignments::{
    DirectoryInsertAssignments, DirectoryUpdateAssignments, FileInsertAssignments,
    FileUpdateAssignments, FilesystemWriteIntent,
};
use crate::write_runtime::filesystem::query::{
    ensure_no_directory_at_file_path, ensure_no_file_at_directory_path, load_directory_row_by_id,
    load_directory_row_by_id_with_pending_transaction_view, load_directory_rows_under_path,
    load_file_row_by_id_with_pending_transaction_view,
    load_file_row_by_id_without_path_with_pending_transaction_view,
    load_file_row_by_path_with_pending_transaction_view, load_file_rows_under_path,
    lookup_directory_id_by_path, lookup_directory_id_by_path_with_pending_transaction_view,
    lookup_directory_path_by_id, lookup_file_id_by_path_with_pending_transaction_view,
    DirectoryFilesystemRow, FileFilesystemRow,
};
use serde_json::json;
use sqlparser::ast::{BinaryOperator, Expr, Value as SqlValue, ValueWithSpan};
use std::collections::{BTreeMap, BTreeSet};

use self::insert_planning::FilesystemPlanningError;

const FILESYSTEM_DESCRIPTOR_FILE_ID: &str = "lix";
const FILESYSTEM_DESCRIPTOR_PLUGIN_KEY: &str = "lix";
const FILESYSTEM_DIRECTORY_SCHEMA_KEY: &str = "lix_directory_descriptor";
const FILESYSTEM_DIRECTORY_SCHEMA_VERSION: &str = "1";
const FILESYSTEM_FILE_SCHEMA_KEY: &str = "lix_file_descriptor";
const FILESYSTEM_FILE_SCHEMA_VERSION: &str = "1";
const FILESYSTEM_BINARY_BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
const FILESYSTEM_BINARY_BLOB_REF_SCHEMA_VERSION: &str = "1";

fn write_resolve_filesystem_planning_error(error: FilesystemPlanningError) -> WriteResolveError {
    WriteResolveError {
        message: error.message,
    }
}

pub(super) async fn resolve_filesystem_write(
    hydrator: &mut PublicWriteHydrator<'_>,
    projection_registry: &ProjectionRegistry,
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    match planned_write.command.target.descriptor.public_name.as_str() {
        "lix_file" | "lix_file_by_version" => match planned_write.command.operation_kind {
            WriteOperationKind::Insert => {
                resolve_file_insert_write_plan(hydrator, planned_write).await
            }
            WriteOperationKind::Update | WriteOperationKind::Delete => {
                resolve_existing_file_write(
                    hydrator.backend(),
                    projection_registry,
                    planned_write,
                    hydrator
                        .pending_state_overlay()
                        .map(|overlay| overlay.as_pending_view()),
                )
                .await
            }
        },
        "lix_directory" | "lix_directory_by_version" => {
            match planned_write.command.operation_kind {
                WriteOperationKind::Insert => {
                    resolve_directory_insert_write_plan(hydrator, planned_write).await
                }
                WriteOperationKind::Update | WriteOperationKind::Delete => {
                    resolve_existing_directory_write(
                        hydrator.backend(),
                        projection_registry,
                        planned_write,
                        hydrator
                            .pending_state_overlay()
                            .map(|overlay| overlay.as_pending_view()),
                    )
                    .await
                }
            }
        }
        other => Err(WriteResolveError {
            message: format!(
                "public filesystem live slice does not yet support '{}' writes",
                other
            ),
        }),
    }
}

fn filesystem_write_lookup_scope(planned_write: &PlannedWrite) -> FilesystemProjectionScope {
    match planned_write.command.target.descriptor.public_name.as_str() {
        "lix_file" | "lix_directory" => FilesystemProjectionScope::ActiveVersion,
        "lix_file_by_version" | "lix_directory_by_version" => {
            FilesystemProjectionScope::ExplicitVersion
        }
        _ => FilesystemProjectionScope::ExplicitVersion,
    }
}

async fn resolved_filesystem_version_id(
    planned_write: &PlannedWrite,
) -> Result<String, WriteResolveError> {
    resolved_version_id(planned_write)?.ok_or_else(|| WriteResolveError {
        message: "public filesystem write requires a concrete version_id".to_string(),
    })
}

fn filesystem_write_intent(
    planned_write: &PlannedWrite,
) -> Result<&FilesystemWriteIntent, WriteResolveError> {
    planned_write
        .filesystem_write_intent
        .as_ref()
        .ok_or_else(|| WriteResolveError {
            message: format!(
                "public filesystem write '{}' is missing compiler-owned filesystem intent",
                planned_write.command.target.descriptor.public_name
            ),
        })
}

fn directory_insert_assignments_batch(
    planned_write: &PlannedWrite,
) -> Result<&[DirectoryInsertAssignments], WriteResolveError> {
    match filesystem_write_intent(planned_write)? {
        FilesystemWriteIntent::DirectoryInsert(rows) => Ok(rows.as_slice()),
        _ => Err(WriteResolveError {
            message: "public filesystem directory insert expected typed directory-insert intent"
                .to_string(),
        }),
    }
}

fn file_insert_assignments_batch(
    planned_write: &PlannedWrite,
) -> Result<&[FileInsertAssignments], WriteResolveError> {
    match filesystem_write_intent(planned_write)? {
        FilesystemWriteIntent::FileInsert(rows) => Ok(rows.as_slice()),
        _ => Err(WriteResolveError {
            message: "public filesystem file insert expected typed file-insert intent".to_string(),
        }),
    }
}

fn directory_update_assignments(
    planned_write: &PlannedWrite,
) -> Result<&DirectoryUpdateAssignments, WriteResolveError> {
    match filesystem_write_intent(planned_write)? {
        FilesystemWriteIntent::DirectoryUpdate(assignments) => Ok(assignments),
        _ => Err(WriteResolveError {
            message: "public filesystem directory update expected typed directory-update intent"
                .to_string(),
        }),
    }
}

fn file_update_assignments(
    planned_write: &PlannedWrite,
) -> Result<&FileUpdateAssignments, WriteResolveError> {
    match filesystem_write_intent(planned_write)? {
        FilesystemWriteIntent::FileUpdate(assignments) => Ok(assignments),
        _ => Err(WriteResolveError {
            message: "public filesystem file update expected typed file-update intent".to_string(),
        }),
    }
}

async fn resolve_directory_insert_write_plan(
    hydrator: &mut PublicWriteHydrator<'_>,
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let lookup_scope = filesystem_write_lookup_scope(planned_write);
    let payloads = payload_maps(planned_write)?;
    let assignments_rows = directory_insert_assignments_batch(planned_write)?;
    if payloads.len() != assignments_rows.len() {
        return Err(WriteResolveError {
            message: "public filesystem directory insert compiler/runtime row count mismatch"
                .to_string(),
        });
    }
    let row_version_ids = resolved_insert_version_ids(hydrator, planned_write).await?;
    if payloads.len() != row_version_ids.len() {
        return Err(WriteResolveError {
            message:
                "public filesystem directory insert requires one version target per payload row"
                    .to_string(),
        });
    }

    let mut grouped_batches = Vec::<(WriteMode, String, Vec<DirectoryInsertAssignments>)>::new();
    for ((payload, version_id), assignments) in payloads
        .into_iter()
        .zip(row_version_ids.into_iter())
        .zip(assignments_rows.iter().cloned())
    {
        let version_id = version_id.ok_or_else(|| WriteResolveError {
            message: "public filesystem write requires a concrete version_id".to_string(),
        })?;
        let execution_mode = default_execution_mode_for_request(
            write_mode_request_for_insert_payload(planned_write, &payload),
        );
        if let Some((_, _, batch)) =
            grouped_batches
                .iter_mut()
                .find(|(group_mode, group_version_id, _)| {
                    *group_mode == execution_mode && *group_version_id == version_id
                })
        {
            batch.push(assignments);
        } else {
            grouped_batches.push((execution_mode, version_id, vec![assignments]));
        }
    }

    let mut partitions = ResolvedWritePlanBuilder::default();
    for (execution_mode, version_id, assignments_batch) in grouped_batches {
        let snapshot = build_directory_insert_snapshot(
            hydrator.backend(),
            hydrator
                .pending_state_overlay()
                .map(|overlay| overlay.as_pending_view()),
            &assignments_batch,
            &version_id,
            lookup_scope,
        )
        .await
        .map_err(write_resolve_filesystem_planning_error)?;
        let planned_batch = plan_directory_insert_batch(&snapshot, &assignments_batch, &version_id)
            .map_err(write_resolve_filesystem_planning_error)?;
        let target_write_lane = target_write_lane_for_version(
            planned_write,
            execution_mode,
            Some(version_id.as_str()),
        )?;
        let partition = partitions.partition_mut(execution_mode, target_write_lane);
        for directory in planned_batch.directories {
            partition.intended_post_state.push(directory_descriptor_row(
                &directory.id,
                directory.parent_id.as_deref(),
                &directory.name,
                directory.hidden,
                &version_id,
                directory.metadata.as_deref(),
            ));
            partition.lineage.push(RowLineage {
                entity_id: directory.id,
                source_change_id: None,
                source_commit_id: None,
            });
        }
    }

    Ok(partitions.into_resolved_write_plan(planned_write.command.requested_mode))
}

async fn resolve_existing_directory_write(
    backend: &dyn LixBackend,
    projection_registry: &ProjectionRegistry,
    planned_write: &PlannedWrite,
    pending_transaction_view: Option<&dyn PendingView>,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let version_id = resolved_filesystem_version_id(planned_write).await?;
    let lookup_scope = filesystem_write_lookup_scope(planned_write);
    let current_rows = load_target_directory_rows_for_selector(
        backend,
        projection_registry,
        planned_write,
        pending_transaction_view,
        &version_id,
        lookup_scope,
    )
    .await?;
    if current_rows.is_empty() {
        return Ok(noop_resolved_write_plan(
            default_execution_mode_for_request(planned_write.command.requested_mode),
        ));
    }
    match planned_write.command.operation_kind {
        WriteOperationKind::Update => {
            let assignments = directory_update_assignments(planned_write)?;

            let mut partitions = ResolvedWritePlanBuilder::default();
            let next_rows = if current_rows.len() > 1 && assignments.changes_structure() {
                resolve_directory_update_targets_batch(
                    backend,
                    &current_rows,
                    &assignments,
                    &version_id,
                    lookup_scope,
                )
                .await?
            } else {
                let mut next_rows = Vec::with_capacity(current_rows.len());
                for current_row in &current_rows {
                    next_rows.push(
                        resolve_directory_update_target(
                            backend,
                            current_row,
                            &assignments,
                            &version_id,
                            lookup_scope,
                        )
                        .await?,
                    );
                }
                next_rows
            };

            for (current_row, next_row) in current_rows.into_iter().zip(next_rows.into_iter()) {
                let execution_mode = resolve_execution_mode_for_untracked_flag(
                    planned_write.command.requested_mode,
                    current_row.untracked,
                    "public tracked filesystem update requires tracked visible rows",
                    "public untracked filesystem update requires an untracked visible row",
                )?;
                let row_ref = ResolvedRowRef {
                    entity_id: current_row.id.clone(),
                    schema_key: FILESYSTEM_DIRECTORY_SCHEMA_KEY.to_string(),
                    version_id: Some(version_id.clone()),
                    source_change_id: current_row.change_id.clone(),
                    source_commit_id: None,
                };
                let target_write_lane = target_write_lane_for_version(
                    planned_write,
                    execution_mode,
                    Some(version_id.as_str()),
                )?;
                let partition = partitions.partition_mut(execution_mode, target_write_lane);
                partition.authoritative_pre_state.push(row_ref.clone());
                partition
                    .authoritative_pre_state_rows
                    .push(directory_descriptor_row(
                        &current_row.id,
                        current_row.parent_id.as_deref(),
                        &current_row.name,
                        current_row.hidden,
                        &version_id,
                        current_row.metadata.as_deref(),
                    ));
                partition.intended_post_state.push(directory_descriptor_row(
                    &current_row.id,
                    next_row.parent_id.as_deref(),
                    &next_row.name,
                    next_row.hidden,
                    &version_id,
                    next_row.metadata.as_deref(),
                ));
                partition.lineage.push(RowLineage {
                    entity_id: current_row.id,
                    source_change_id: row_ref.source_change_id,
                    source_commit_id: None,
                });
            }

            Ok(partitions.into_resolved_write_plan(planned_write.command.requested_mode))
        }
        WriteOperationKind::Delete => {
            let mut descendant_directories = BTreeMap::new();
            let mut descendant_files = BTreeMap::new();
            for current_row in current_rows {
                for row in
                    load_directory_rows_under_path(backend, &version_id, &current_row.path).await?
                {
                    descendant_directories.entry(row.id.clone()).or_insert(row);
                }
                for row in
                    load_file_rows_under_path(backend, &version_id, &current_row.path).await?
                {
                    descendant_files.entry(row.id.clone()).or_insert(row);
                }
            }

            let mut partitions = ResolvedWritePlanBuilder::default();

            for row in descendant_directories.values() {
                let execution_mode = resolve_execution_mode_for_untracked_flag(
                    planned_write.command.requested_mode,
                    row.untracked,
                    "public tracked filesystem directory delete requires tracked visible rows throughout the cascade",
                    "public untracked filesystem directory delete requires untracked visible rows throughout the cascade",
                )?;
                let row_ref = ResolvedRowRef {
                    entity_id: row.id.clone(),
                    schema_key: FILESYSTEM_DIRECTORY_SCHEMA_KEY.to_string(),
                    version_id: Some(version_id.clone()),
                    source_change_id: row.change_id.clone(),
                    source_commit_id: None,
                };
                let target_write_lane = target_write_lane_for_version(
                    planned_write,
                    execution_mode,
                    Some(version_id.as_str()),
                )?;
                let partition = partitions.partition_mut(execution_mode, target_write_lane);
                partition.authoritative_pre_state.push(row_ref.clone());
                partition
                    .intended_post_state
                    .push(directory_descriptor_tombstone_row(
                        &row.id,
                        row.parent_id.as_deref(),
                        &row.name,
                        row.hidden,
                        &version_id,
                        row.metadata.as_deref(),
                    ));
                partition.tombstones.push(row_ref);
                partition.lineage.push(RowLineage {
                    entity_id: row.id.clone(),
                    source_change_id: row.change_id.clone(),
                    source_commit_id: None,
                });
            }

            for row in descendant_files.values() {
                let execution_mode = resolve_execution_mode_for_untracked_flag(
                    planned_write.command.requested_mode,
                    row.untracked,
                    "public tracked filesystem directory delete requires tracked visible rows throughout the cascade",
                    "public untracked filesystem directory delete requires untracked visible rows throughout the cascade",
                )?;
                let file_ref = ResolvedRowRef {
                    entity_id: row.id.clone(),
                    schema_key: FILESYSTEM_FILE_SCHEMA_KEY.to_string(),
                    version_id: Some(version_id.clone()),
                    source_change_id: row.change_id.clone(),
                    source_commit_id: None,
                };
                let blob_ref = ResolvedRowRef {
                    entity_id: row.id.clone(),
                    schema_key: FILESYSTEM_BINARY_BLOB_REF_SCHEMA_KEY.to_string(),
                    version_id: Some(version_id.clone()),
                    source_change_id: None,
                    source_commit_id: None,
                };
                let target_write_lane = target_write_lane_for_version(
                    planned_write,
                    execution_mode,
                    Some(version_id.as_str()),
                )?;
                let partition = partitions.partition_mut(execution_mode, target_write_lane);
                partition.authoritative_pre_state.push(file_ref.clone());
                partition.authoritative_pre_state.push(blob_ref.clone());
                partition
                    .intended_post_state
                    .push(file_descriptor_tombstone_row(
                        &row.id,
                        row.directory_id.as_deref(),
                        &row.name,
                        row.extension.as_deref(),
                        row.hidden,
                        &version_id,
                        row.metadata.as_deref(),
                    ));
                partition
                    .intended_post_state
                    .push(binary_blob_ref_tombstone_row(&row.id, &version_id));
                partition.tombstones.push(file_ref);
                partition.tombstones.push(blob_ref);
                set_filesystem_deleted_state(
                    &mut partition.filesystem_state,
                    &row.id,
                    &version_id,
                    execution_mode == WriteMode::Untracked,
                );
                partition.lineage.push(RowLineage {
                    entity_id: row.id.clone(),
                    source_change_id: row.change_id.clone(),
                    source_commit_id: None,
                });
            }

            Ok(partitions.into_resolved_write_plan(planned_write.command.requested_mode))
        }
        WriteOperationKind::Insert => Err(WriteResolveError {
            message: "public filesystem directory existing-row resolver does not handle inserts"
                .to_string(),
        }),
    }
}

async fn resolve_file_insert_write_plan(
    hydrator: &mut PublicWriteHydrator<'_>,
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let lookup_scope = filesystem_write_lookup_scope(planned_write);
    let payloads = payload_maps(planned_write)?;
    let assignments_rows = file_insert_assignments_batch(planned_write)?;
    if payloads.len() != assignments_rows.len() {
        return Err(WriteResolveError {
            message: "public filesystem file insert compiler/runtime row count mismatch"
                .to_string(),
        });
    }
    let row_version_ids = resolved_insert_version_ids(hydrator, planned_write).await?;
    if payloads.len() != row_version_ids.len() {
        return Err(WriteResolveError {
            message: "public filesystem file insert requires one version target per payload row"
                .to_string(),
        });
    }

    let mut grouped_batches = Vec::<(WriteMode, String, Vec<FileInsertAssignments>)>::new();
    for ((payload, version_id), assignments) in payloads
        .into_iter()
        .zip(row_version_ids.into_iter())
        .zip(assignments_rows.iter().cloned())
    {
        let version_id = version_id.ok_or_else(|| WriteResolveError {
            message: "public filesystem write requires a concrete version_id".to_string(),
        })?;
        let execution_mode = default_execution_mode_for_request(
            write_mode_request_for_insert_payload(planned_write, &payload),
        );
        if let Some((_, _, batch)) =
            grouped_batches
                .iter_mut()
                .find(|(group_mode, group_version_id, _)| {
                    *group_mode == execution_mode && *group_version_id == version_id
                })
        {
            batch.push(assignments);
        } else {
            grouped_batches.push((execution_mode, version_id, vec![assignments]));
        }
    }

    let mut partitions = ResolvedWritePlanBuilder::default();
    for (execution_mode, version_id, assignments_batch) in grouped_batches {
        let snapshot = build_file_insert_snapshot(
            hydrator.backend(),
            hydrator
                .pending_state_overlay()
                .map(|overlay| overlay.as_pending_view()),
            &assignments_batch,
            &version_id,
            lookup_scope,
        )
        .await
        .map_err(write_resolve_filesystem_planning_error)?;
        let planned_batch = plan_file_insert_batch(&snapshot, &assignments_batch, &version_id)
            .map_err(write_resolve_filesystem_planning_error)?;
        let target_write_lane = target_write_lane_for_version(
            planned_write,
            execution_mode,
            Some(version_id.as_str()),
        )?;
        let partition = partitions.partition_mut(execution_mode, target_write_lane);
        for directory in planned_batch.directories {
            partition.intended_post_state.push(directory_descriptor_row(
                &directory.id,
                directory.parent_id.as_deref(),
                &directory.name,
                directory.hidden,
                &version_id,
                directory.metadata.as_deref(),
            ));
            partition.lineage.push(RowLineage {
                entity_id: directory.id,
                source_change_id: None,
                source_commit_id: None,
            });
        }
        for file in planned_batch.files {
            partition.intended_post_state.push(file_descriptor_row(
                &file.id,
                file.directory_id.as_deref(),
                &file.name,
                file.extension.as_deref(),
                file.hidden,
                &version_id,
                file.metadata.as_deref(),
            ));
            let descriptor = filesystem_descriptor_state_from_file_row(
                file.directory_id.as_deref(),
                &file.name,
                file.extension.as_deref(),
                file.hidden,
                file.metadata.as_deref(),
            );
            set_filesystem_descriptor_state(
                &mut partition.filesystem_state,
                &file.id,
                &version_id,
                execution_mode == WriteMode::Untracked,
                descriptor,
            );
            if let Some(bytes) = file.data.as_deref() {
                partition.intended_post_state.push(binary_blob_ref_row(
                    &file.id,
                    &version_id,
                    bytes,
                )?);
                set_filesystem_data_state(
                    &mut partition.filesystem_state,
                    &file.id,
                    &version_id,
                    execution_mode == WriteMode::Untracked,
                    bytes.to_vec(),
                );
            }
            partition.lineage.push(RowLineage {
                entity_id: file.id,
                source_change_id: None,
                source_commit_id: None,
            });
        }
    }

    Ok(partitions.into_resolved_write_plan(planned_write.command.requested_mode))
}

async fn resolve_existing_file_write(
    backend: &dyn LixBackend,
    projection_registry: &ProjectionRegistry,
    planned_write: &PlannedWrite,
    pending_transaction_view: Option<&dyn PendingView>,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let version_id = resolved_filesystem_version_id(planned_write).await?;
    let lookup_scope = filesystem_write_lookup_scope(planned_write);
    match planned_write.command.operation_kind {
        WriteOperationKind::Update => {
            let assignments = file_update_assignments(planned_write)?;
            let current_rows = load_target_file_rows_for_selector(
                backend,
                projection_registry,
                planned_write,
                pending_transaction_view,
                &version_id,
                lookup_scope,
                assignments.path.is_some(),
            )
            .await?;
            if current_rows.is_empty() {
                return Ok(noop_resolved_write_plan(
                    default_execution_mode_for_request(planned_write.command.requested_mode),
                ));
            }
            if current_rows.len() > 1 && assignments.path.is_some() {
                return Err(WriteResolveError {
                    message: format!(
                        "Unique constraint violation: file path '{}' would be assigned to multiple rows",
                        assignments
                            .path
                            .as_ref()
                            .expect("path checked above")
                            .normalized_path
                    ),
                });
            }

            let mut partitions = ResolvedWritePlanBuilder::default();
            for current_row in current_rows {
                let execution_mode = resolve_execution_mode_for_untracked_flag(
                    planned_write.command.requested_mode,
                    current_row.untracked,
                    "public tracked filesystem update requires tracked visible rows",
                    "public untracked filesystem update requires an untracked visible row",
                )?;

                let row_ref = ResolvedRowRef {
                    entity_id: current_row.id.clone(),
                    schema_key: FILESYSTEM_FILE_SCHEMA_KEY.to_string(),
                    version_id: Some(version_id.clone()),
                    source_change_id: current_row.change_id.clone(),
                    source_commit_id: None,
                };
                let target_write_lane = target_write_lane_for_version(
                    planned_write,
                    execution_mode,
                    Some(version_id.as_str()),
                )?;
                let partition = partitions.partition_mut(execution_mode, target_write_lane);
                partition.authoritative_pre_state.push(row_ref.clone());
                partition
                    .authoritative_pre_state_rows
                    .push(file_descriptor_row(
                        &current_row.id,
                        current_row.directory_id.as_deref(),
                        &current_row.name,
                        current_row.extension.as_deref(),
                        current_row.hidden,
                        &version_id,
                        current_row.metadata.as_deref(),
                    ));

                let (next_row, ancestor_rows) = resolve_file_update_target(
                    backend,
                    pending_transaction_view,
                    &current_row,
                    &assignments,
                    &version_id,
                    lookup_scope,
                )
                .await?;
                let descriptor_changed =
                    file_descriptor_changed(&current_row, &next_row) || !ancestor_rows.is_empty();
                partition.lineage.push(RowLineage {
                    entity_id: current_row.id.clone(),
                    source_change_id: row_ref.source_change_id.clone(),
                    source_commit_id: None,
                });

                for ancestor in &ancestor_rows {
                    partition.intended_post_state.push(directory_descriptor_row(
                        &ancestor.id,
                        ancestor.parent_id.as_deref(),
                        &ancestor.name,
                        ancestor.hidden,
                        &ancestor.version_id,
                        ancestor.metadata.as_deref(),
                    ));
                    partition.lineage.push(RowLineage {
                        entity_id: ancestor.id.clone(),
                        source_change_id: None,
                        source_commit_id: None,
                    });
                }

                if descriptor_changed {
                    partition.intended_post_state.push(file_descriptor_row(
                        &current_row.id,
                        next_row.directory_id.as_deref(),
                        &next_row.name,
                        next_row.extension.as_deref(),
                        next_row.hidden,
                        &version_id,
                        next_row.metadata.as_deref(),
                    ));
                    set_filesystem_descriptor_state(
                        &mut partition.filesystem_state,
                        &current_row.id,
                        &version_id,
                        execution_mode == WriteMode::Untracked,
                        filesystem_descriptor_state_from_file_row(
                            next_row.directory_id.as_deref(),
                            &next_row.name,
                            next_row.extension.as_deref(),
                            next_row.hidden,
                            next_row.metadata.as_deref(),
                        ),
                    );
                }

                if let Some(bytes) = assignments.data.bytes() {
                    partition.authoritative_pre_state.push(ResolvedRowRef {
                        entity_id: current_row.id.clone(),
                        schema_key: FILESYSTEM_BINARY_BLOB_REF_SCHEMA_KEY.to_string(),
                        version_id: Some(version_id.clone()),
                        source_change_id: None,
                        source_commit_id: None,
                    });
                    partition.intended_post_state.push(binary_blob_ref_row(
                        &current_row.id,
                        &version_id,
                        bytes,
                    )?);
                    set_filesystem_data_state(
                        &mut partition.filesystem_state,
                        &current_row.id,
                        &version_id,
                        execution_mode == WriteMode::Untracked,
                        bytes.to_vec(),
                    );
                }
            }

            Ok(partitions.into_resolved_write_plan(planned_write.command.requested_mode))
        }
        WriteOperationKind::Delete => {
            let current_rows = load_target_file_rows_for_selector(
                backend,
                projection_registry,
                planned_write,
                pending_transaction_view,
                &version_id,
                lookup_scope,
                true,
            )
            .await?;
            if current_rows.is_empty() {
                return Ok(noop_resolved_write_plan(
                    default_execution_mode_for_request(planned_write.command.requested_mode),
                ));
            }
            let mut partitions = ResolvedWritePlanBuilder::default();
            for current_row in current_rows {
                let execution_mode = resolve_execution_mode_for_untracked_flag(
                    planned_write.command.requested_mode,
                    current_row.untracked,
                    "public tracked filesystem delete requires tracked visible rows",
                    "public untracked filesystem delete requires an untracked visible row",
                )?;

                let row_ref = ResolvedRowRef {
                    entity_id: current_row.id.clone(),
                    schema_key: FILESYSTEM_FILE_SCHEMA_KEY.to_string(),
                    version_id: Some(version_id.clone()),
                    source_change_id: current_row.change_id.clone(),
                    source_commit_id: None,
                };
                let blob_ref = ResolvedRowRef {
                    entity_id: current_row.id.clone(),
                    schema_key: FILESYSTEM_BINARY_BLOB_REF_SCHEMA_KEY.to_string(),
                    version_id: Some(version_id.clone()),
                    source_change_id: None,
                    source_commit_id: None,
                };
                let target_write_lane = target_write_lane_for_version(
                    planned_write,
                    execution_mode,
                    Some(version_id.as_str()),
                )?;
                let partition = partitions.partition_mut(execution_mode, target_write_lane);
                partition.authoritative_pre_state.push(row_ref.clone());
                partition.authoritative_pre_state.push(blob_ref.clone());
                partition
                    .intended_post_state
                    .push(file_descriptor_tombstone_row(
                        &current_row.id,
                        current_row.directory_id.as_deref(),
                        &current_row.name,
                        current_row.extension.as_deref(),
                        current_row.hidden,
                        &version_id,
                        current_row.metadata.as_deref(),
                    ));
                partition
                    .intended_post_state
                    .push(binary_blob_ref_tombstone_row(&current_row.id, &version_id));
                partition.tombstones.push(row_ref);
                partition.tombstones.push(blob_ref);
                set_filesystem_deleted_state(
                    &mut partition.filesystem_state,
                    &current_row.id,
                    &version_id,
                    execution_mode == WriteMode::Untracked,
                );
                partition.lineage.push(RowLineage {
                    entity_id: current_row.id,
                    source_change_id: current_row.change_id,
                    source_commit_id: None,
                });
            }
            Ok(partitions.into_resolved_write_plan(planned_write.command.requested_mode))
        }
        WriteOperationKind::Insert => Err(WriteResolveError {
            message: "public filesystem existing-row resolver does not handle inserts".to_string(),
        }),
    }
}

async fn resolve_parent_directory_target(
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&dyn PendingView>,
    version_id: &str,
    directory_path: Option<&str>,
    untracked: bool,
    lookup_scope: FilesystemProjectionScope,
) -> Result<(Option<String>, Vec<DirectoryFilesystemRow>), WriteResolveError> {
    let Some(directory_path) = directory_path else {
        return Ok((None, Vec::new()));
    };
    let missing_rows = resolve_missing_directory_rows(
        backend,
        pending_transaction_view,
        version_id,
        directory_path,
        untracked,
        lookup_scope,
    )
    .await?;
    let directory_id = if let Some(last_row) = missing_rows.last() {
        Some(last_row.id.clone())
    } else {
        lookup_directory_id_by_path_with_pending_transaction_view(
            backend,
            pending_transaction_view,
            version_id,
            &NormalizedDirectoryPath::from_normalized(directory_path.to_string()),
            lookup_scope,
        )
        .await?
    };
    Ok((directory_id, missing_rows))
}

fn filesystem_descriptor_state_from_file_row(
    directory_id: Option<&str>,
    name: &str,
    extension: Option<&str>,
    hidden: bool,
    metadata: Option<&str>,
) -> PlannedFilesystemDescriptor {
    PlannedFilesystemDescriptor {
        directory_id: directory_id.unwrap_or("").to_string(),
        name: name.to_string(),
        extension: extension.map(ToString::to_string),
        metadata: metadata.map(ToString::to_string),
        hidden,
    }
}

fn ensure_file_state_entry<'a>(
    state: &'a mut PlannedFilesystemState,
    file_id: &str,
    version_id: &str,
    untracked: bool,
) -> &'a mut PlannedFilesystemFile {
    state
        .files
        .entry((file_id.to_string(), version_id.to_string()))
        .or_insert_with(|| PlannedFilesystemFile {
            file_id: file_id.to_string(),
            version_id: version_id.to_string(),
            untracked,
            descriptor: None,
            metadata_patch: crate::sql::logical_plan::public_ir::OptionalTextPatch::Unchanged,
            data: None,
            deleted: false,
        })
}

fn set_filesystem_descriptor_state(
    state: &mut PlannedFilesystemState,
    file_id: &str,
    version_id: &str,
    untracked: bool,
    descriptor: PlannedFilesystemDescriptor,
) {
    let entry = ensure_file_state_entry(state, file_id, version_id, untracked);
    entry.untracked = untracked;
    entry.deleted = false;
    entry.descriptor = Some(descriptor);
    entry.metadata_patch = crate::sql::logical_plan::public_ir::OptionalTextPatch::Unchanged;
}

fn set_filesystem_data_state(
    state: &mut PlannedFilesystemState,
    file_id: &str,
    version_id: &str,
    untracked: bool,
    data: Vec<u8>,
) {
    let entry = ensure_file_state_entry(state, file_id, version_id, untracked);
    entry.untracked = untracked;
    entry.deleted = false;
    entry.data = Some(data);
}

fn set_filesystem_deleted_state(
    state: &mut PlannedFilesystemState,
    file_id: &str,
    version_id: &str,
    untracked: bool,
) {
    let entry = ensure_file_state_entry(state, file_id, version_id, untracked);
    entry.untracked = untracked;
    entry.deleted = true;
    entry.descriptor = None;
    entry.data = None;
    entry.metadata_patch = crate::sql::logical_plan::public_ir::OptionalTextPatch::Unchanged;
}

async fn resolve_missing_directory_rows(
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&dyn PendingView>,
    version_id: &str,
    directory_path: &str,
    untracked: bool,
    lookup_scope: FilesystemProjectionScope,
) -> Result<Vec<DirectoryFilesystemRow>, WriteResolveError> {
    let mut missing = Vec::new();
    let mut known_ids = BTreeMap::<String, String>::new();
    let mut paths = directory_ancestor_paths(directory_path);
    paths.push(directory_path.to_string());

    for candidate_path in paths {
        if let Some(existing_id) = lookup_directory_id_by_path_with_pending_transaction_view(
            backend,
            pending_transaction_view,
            version_id,
            &NormalizedDirectoryPath::from_normalized(candidate_path.clone()),
            lookup_scope,
        )
        .await?
        {
            known_ids.insert(candidate_path, existing_id);
            continue;
        }
        ensure_no_file_at_directory_path(
            backend,
            version_id,
            &NormalizedDirectoryPath::from_normalized(candidate_path.clone()),
            lookup_scope,
        )
        .await?;
        let parent_id = match parent_directory_path(&candidate_path) {
            Some(parent_path) => {
                if let Some(parent_id) = known_ids.get(&parent_path).cloned() {
                    Some(parent_id)
                } else if let Some(existing_parent_id) =
                    lookup_directory_id_by_path_with_pending_transaction_view(
                        backend,
                        pending_transaction_view,
                        version_id,
                        &NormalizedDirectoryPath::from_normalized(parent_path.clone()),
                        lookup_scope,
                    )
                    .await?
                {
                    Some(existing_parent_id)
                } else {
                    Some(auto_directory_id(version_id, &parent_path))
                }
            }
            None => None,
        };
        let id = auto_directory_id(version_id, &candidate_path);
        missing.push(DirectoryFilesystemRow {
            id: id.clone(),
            parent_id,
            name: directory_name_from_path(&candidate_path).unwrap_or_default(),
            path: candidate_path.clone(),
            hidden: false,
            version_id: version_id.to_string(),
            untracked,
            metadata: None,
            change_id: None,
        });
        known_ids.insert(candidate_path, id);
    }

    Ok(missing)
}

async fn resolve_file_update_target(
    backend: &dyn LixBackend,
    pending_transaction_view: Option<&dyn PendingView>,
    current_row: &FileFilesystemRow,
    assignments: &FileUpdateAssignments,
    version_id: &str,
    lookup_scope: FilesystemProjectionScope,
) -> Result<(FileFilesystemRow, Vec<DirectoryFilesystemRow>), WriteResolveError> {
    let next_hidden = assignments.hidden.unwrap_or(current_row.hidden);
    let next_metadata = assignments.metadata.apply(current_row.metadata.clone());

    let mut ancestor_rows = Vec::new();
    let (next_directory_id, next_name, next_extension, next_path) =
        if let Some(parsed) = assignments.path.as_ref() {
            ensure_no_directory_at_file_path(backend, version_id, parsed, lookup_scope).await?;
            let (directory_id, missing_ancestors) = resolve_parent_directory_target(
                backend,
                pending_transaction_view,
                version_id,
                parsed.directory_path.as_deref(),
                current_row.untracked,
                lookup_scope,
            )
            .await?;
            ancestor_rows = missing_ancestors;
            (
                directory_id,
                parsed.name.clone(),
                parsed.extension.clone(),
                Some(parsed.normalized_path.as_str().to_string()),
            )
        } else {
            (
                current_row.directory_id.clone(),
                current_row.name.clone(),
                current_row.extension.clone(),
                None,
            )
        };

    if let Some(next_path) = next_path.as_ref() {
        if let Some(existing_id) = lookup_file_id_by_path_with_pending_transaction_view(
            backend,
            pending_transaction_view,
            version_id,
            &ParsedFilePath::from_normalized_path(next_path.clone())
                .map_err(write_resolve_backend_error)?,
            lookup_scope,
        )
        .await?
        {
            if existing_id != current_row.id {
                return Err(WriteResolveError {
                    message: format!(
                        "Unique constraint violation: file path '{}' already exists in version '{}'",
                        next_path, version_id
                    ),
                });
            }
        }
    }

    Ok((
        FileFilesystemRow {
            id: current_row.id.clone(),
            directory_id: next_directory_id,
            name: next_name,
            extension: next_extension,
            path: next_path.unwrap_or_default(),
            hidden: next_hidden,
            untracked: current_row.untracked,
            metadata: next_metadata,
            change_id: current_row.change_id.clone(),
        },
        ancestor_rows,
    ))
}

fn file_descriptor_changed(current_row: &FileFilesystemRow, next_row: &FileFilesystemRow) -> bool {
    current_row.directory_id != next_row.directory_id
        || current_row.name != next_row.name
        || current_row.extension != next_row.extension
        || current_row.hidden != next_row.hidden
        || current_row.metadata != next_row.metadata
}

async fn resolve_directory_update_target(
    backend: &dyn LixBackend,
    current_row: &DirectoryFilesystemRow,
    assignments: &DirectoryUpdateAssignments,
    version_id: &str,
    lookup_scope: FilesystemProjectionScope,
) -> Result<DirectoryFilesystemRow, WriteResolveError> {
    let next_hidden = assignments.hidden.unwrap_or(current_row.hidden);
    let next_metadata = assignments.metadata.apply(current_row.metadata.clone());

    let (resolved_parent_id, resolved_name, resolved_path) = if let Some(normalized_path) =
        assignments.path.as_ref()
    {
        let name = directory_name_from_path(normalized_path).ok_or_else(|| WriteResolveError {
            message: "Directory name must be provided".to_string(),
        })?;
        let parent_id = match parent_directory_path(normalized_path) {
            Some(parent_path) => lookup_directory_id_by_path(
                backend,
                version_id,
                &NormalizedDirectoryPath::from_normalized(parent_path.clone()),
                lookup_scope,
            )
            .await?
            .ok_or_else(|| WriteResolveError {
                message: format!("Parent directory does not exist for path {}", parent_path),
            })?,
            None => String::new(),
        };
        let parent_id_opt = if parent_id.is_empty() {
            None
        } else {
            Some(parent_id)
        };
        (parent_id_opt, name, normalized_path.as_str().to_string())
    } else {
        let parent_id = assignments
            .parent_id
            .clone()
            .or_else(|| current_row.parent_id.clone());
        let name = assignments
            .name
            .clone()
            .unwrap_or_else(|| current_row.name.clone());
        let parent_path = match parent_id.as_deref() {
            Some(parent_id) => {
                lookup_directory_path_by_id(backend, version_id, parent_id, lookup_scope)
                    .await?
                    .ok_or_else(|| WriteResolveError {
                        message: format!("Parent directory does not exist for id {}", parent_id),
                    })?
            }
            None => "/".to_string(),
        };
        let path = compose_directory_path(parent_path.as_str(), &name)
            .map_err(write_resolve_backend_error)?;
        (parent_id, name, path)
    };

    if resolved_parent_id.as_deref() == Some(current_row.id.as_str()) {
        return Err(WriteResolveError {
            message: "Directory cannot be its own parent".to_string(),
        });
    }
    if let Some(parent_id) = resolved_parent_id.as_deref() {
        assert_no_directory_cycle(
            backend,
            version_id,
            current_row.id.as_str(),
            parent_id,
            lookup_scope,
        )
        .await?;
    }
    if let Some(existing_id) = lookup_directory_id_by_path(
        backend,
        version_id,
        &NormalizedDirectoryPath::from_normalized(resolved_path.clone()),
        lookup_scope,
    )
    .await?
    {
        if existing_id != current_row.id {
            return Err(WriteResolveError {
                message: format!(
                    "Unique constraint violation: directory path '{}' already exists in version '{}'",
                    resolved_path, version_id
                ),
            });
        }
    }
    ensure_no_file_at_directory_path(
        backend,
        version_id,
        &NormalizedDirectoryPath::from_normalized(resolved_path.clone()),
        lookup_scope,
    )
    .await?;

    Ok(DirectoryFilesystemRow {
        id: current_row.id.clone(),
        parent_id: resolved_parent_id,
        name: resolved_name,
        path: resolved_path,
        hidden: next_hidden,
        version_id: version_id.to_string(),
        untracked: current_row.untracked,
        metadata: next_metadata,
        change_id: current_row.change_id.clone(),
    })
}

#[derive(Debug, Clone)]
struct ProposedDirectoryUpdate {
    id: String,
    parent_id: Option<String>,
    name: String,
    hidden: bool,
    metadata: Option<String>,
}

async fn resolve_directory_update_targets_batch(
    backend: &dyn LixBackend,
    current_rows: &[DirectoryFilesystemRow],
    assignments: &DirectoryUpdateAssignments,
    version_id: &str,
    lookup_scope: FilesystemProjectionScope,
) -> Result<Vec<DirectoryFilesystemRow>, WriteResolveError> {
    if let Some(normalized_path) = assignments.path.as_ref() {
        return Err(WriteResolveError {
            message: format!(
                "Unique constraint violation: directory path '{}' would be assigned to multiple rows",
                normalized_path
            ),
        });
    }

    let mut proposed_by_id = BTreeMap::new();
    for row in current_rows {
        let parent_id = assignments
            .parent_id
            .clone()
            .or_else(|| row.parent_id.clone());
        if parent_id.as_deref() == Some(row.id.as_str()) {
            return Err(WriteResolveError {
                message: "Directory cannot be its own parent".to_string(),
            });
        }
        let name = assignments.name.clone().unwrap_or_else(|| row.name.clone());
        let hidden = assignments.hidden.unwrap_or(row.hidden);
        let metadata = assignments.metadata.apply(row.metadata.clone());
        proposed_by_id.insert(
            row.id.clone(),
            ProposedDirectoryUpdate {
                id: row.id.clone(),
                parent_id,
                name,
                hidden,
                metadata,
            },
        );
    }

    let mut external_parent_paths = BTreeMap::<String, String>::new();
    for proposal in proposed_by_id.values() {
        let Some(parent_id) = proposal.parent_id.as_deref() else {
            continue;
        };
        if proposed_by_id.contains_key(parent_id) {
            continue;
        }
        assert_no_directory_cycle(backend, version_id, &proposal.id, parent_id, lookup_scope)
            .await?;
        let parent_path = lookup_directory_path_by_id(backend, version_id, parent_id, lookup_scope)
            .await?
            .ok_or_else(|| WriteResolveError {
                message: format!("Parent directory does not exist for id {}", parent_id),
            })?;
        external_parent_paths.insert(parent_id.to_string(), parent_path);
    }

    for proposal in proposed_by_id.values() {
        let mut seen = BTreeSet::new();
        let mut cursor = proposal.parent_id.clone();
        while let Some(parent_id) = cursor {
            if !seen.insert(parent_id.clone()) {
                return Err(WriteResolveError {
                    message: "Directory parent would create a cycle".to_string(),
                });
            }
            cursor = proposed_by_id
                .get(&parent_id)
                .and_then(|parent| parent.parent_id.clone());
        }
    }

    let mut resolved_paths = BTreeMap::<String, String>::new();
    for row in current_rows {
        let path = resolve_proposed_directory_path(
            &row.id,
            &proposed_by_id,
            &external_parent_paths,
            &mut resolved_paths,
        )?;
        ensure_no_file_at_directory_path(
            backend,
            version_id,
            &NormalizedDirectoryPath::from_normalized(path.clone()),
            lookup_scope,
        )
        .await?;
    }

    let mut path_to_id = BTreeMap::<String, String>::new();
    for row in current_rows {
        let path = resolved_paths
            .get(&row.id)
            .expect("resolved batch path should exist")
            .clone();
        if let Some(existing_id) = path_to_id.insert(path.clone(), row.id.clone()) {
            if existing_id != row.id {
                return Err(WriteResolveError {
                    message: format!(
                        "Unique constraint violation: directory path '{}' would be assigned to multiple rows",
                        path
                    ),
                });
            }
        }
        if let Some(existing_id) = lookup_directory_id_by_path(
            backend,
            version_id,
            &NormalizedDirectoryPath::from_normalized(path.clone()),
            lookup_scope,
        )
        .await?
        {
            if existing_id == row.id {
                continue;
            }
            let Some(other_path) = resolved_paths.get(&existing_id) else {
                return Err(WriteResolveError {
                    message: format!(
                        "Unique constraint violation: directory path '{}' already exists in version '{}'",
                        path, version_id
                    ),
                });
            };
            if other_path != &path {
                continue;
            }
            return Err(WriteResolveError {
                message: format!(
                    "Unique constraint violation: directory path '{}' would be assigned to multiple rows",
                    path
                ),
            });
        }
    }

    let mut next_rows = Vec::with_capacity(current_rows.len());
    for row in current_rows {
        let proposal = proposed_by_id
            .get(&row.id)
            .expect("proposed directory update should exist");
        next_rows.push(DirectoryFilesystemRow {
            id: row.id.clone(),
            parent_id: proposal.parent_id.clone(),
            name: proposal.name.clone(),
            path: resolved_paths
                .get(&row.id)
                .expect("resolved batch path should exist")
                .clone(),
            hidden: proposal.hidden,
            version_id: version_id.to_string(),
            untracked: row.untracked,
            metadata: proposal.metadata.clone(),
            change_id: row.change_id.clone(),
        });
    }
    Ok(next_rows)
}

fn resolve_proposed_directory_path(
    directory_id: &str,
    proposed_by_id: &BTreeMap<String, ProposedDirectoryUpdate>,
    external_parent_paths: &BTreeMap<String, String>,
    resolved_paths: &mut BTreeMap<String, String>,
) -> Result<String, WriteResolveError> {
    if let Some(path) = resolved_paths.get(directory_id) {
        return Ok(path.clone());
    }
    let proposal = proposed_by_id
        .get(directory_id)
        .expect("proposed directory update should exist");
    let parent_path = match proposal.parent_id.as_deref() {
        Some(parent_id) if proposed_by_id.contains_key(parent_id) => {
            resolve_proposed_directory_path(
                parent_id,
                proposed_by_id,
                external_parent_paths,
                resolved_paths,
            )?
        }
        Some(parent_id) => external_parent_paths
            .get(parent_id)
            .cloned()
            .ok_or_else(|| WriteResolveError {
                message: format!("Parent directory does not exist for id {}", parent_id),
            })?,
        None => "/".to_string(),
    };
    let path = compose_directory_path(parent_path.as_str(), &proposal.name)
        .map_err(write_resolve_backend_error)?;
    resolved_paths.insert(directory_id.to_string(), path.clone());
    Ok(path)
}

async fn load_target_directory_rows_for_selector(
    backend: &dyn LixBackend,
    projection_registry: &ProjectionRegistry,
    planned_write: &PlannedWrite,
    pending_transaction_view: Option<&dyn PendingView>,
    version_id: &str,
    lookup_scope: FilesystemProjectionScope,
) -> Result<Vec<DirectoryFilesystemRow>, WriteResolveError> {
    if let Some(directory_id) = exact_id_selector_value(planned_write) {
        return Ok(load_directory_row_by_id_with_pending_transaction_view(
            backend,
            pending_transaction_view,
            version_id,
            &directory_id,
            lookup_scope,
        )
        .await?
        .into_iter()
        .collect());
    }
    let directory_ids = query_text_selector_values_for_write_selector(
        backend,
        projection_registry,
        planned_write,
        pending_transaction_view,
        "id",
        "public filesystem directory selector resolver expected id text rows",
    )
    .await?;
    let mut rows = Vec::new();
    for directory_id in directory_ids {
        if let Some(row) =
            load_directory_row_by_id(backend, version_id, &directory_id, lookup_scope).await?
        {
            rows.push(row);
        }
    }
    Ok(rows)
}

async fn load_target_file_rows_for_selector(
    backend: &dyn LixBackend,
    projection_registry: &ProjectionRegistry,
    planned_write: &PlannedWrite,
    pending_transaction_view: Option<&dyn PendingView>,
    version_id: &str,
    lookup_scope: FilesystemProjectionScope,
    require_paths: bool,
) -> Result<Vec<FileFilesystemRow>, WriteResolveError> {
    if let Some(file_id) = exact_id_selector_value(planned_write) {
        let row = if require_paths {
            load_file_row_by_id_with_pending_transaction_view(
                backend,
                pending_transaction_view,
                version_id,
                &file_id,
                lookup_scope,
            )
            .await?
        } else {
            load_file_row_by_id_without_path_with_pending_transaction_view(
                backend,
                pending_transaction_view,
                version_id,
                &file_id,
                lookup_scope,
            )
            .await?
        };
        return Ok(row.into_iter().collect());
    }
    if let Some(paths) = exact_path_selector_values(planned_write) {
        let mut rows = Vec::new();
        for path in paths {
            let parsed =
                ParsedFilePath::from_normalized_path(path).map_err(write_resolve_backend_error)?;
            let row = load_file_row_by_path_with_pending_transaction_view(
                backend,
                pending_transaction_view,
                version_id,
                &parsed,
                lookup_scope,
            )
            .await?;
            if let Some(row) = row {
                rows.push(row);
            }
        }
        return Ok(rows);
    }
    let file_ids = query_text_selector_values_for_write_selector(
        backend,
        projection_registry,
        planned_write,
        pending_transaction_view,
        "id",
        "public filesystem file selector resolver expected id text rows",
    )
    .await?;
    let mut rows = Vec::new();
    for file_id in file_ids {
        let row = if require_paths {
            load_file_row_by_id_with_pending_transaction_view(
                backend,
                pending_transaction_view,
                version_id,
                &file_id,
                lookup_scope,
            )
            .await?
        } else {
            load_file_row_by_id_without_path_with_pending_transaction_view(
                backend,
                pending_transaction_view,
                version_id,
                &file_id,
                lookup_scope,
            )
            .await?
        };
        if let Some(row) = row {
            rows.push(row);
        }
    }
    Ok(rows)
}

fn exact_id_selector_value(planned_write: &PlannedWrite) -> Option<String> {
    exact_id_selector_values(planned_write)
        .filter(|ids| ids.len() == 1)
        .and_then(|ids| ids.into_iter().next())
}

fn exact_path_selector_values(planned_write: &PlannedWrite) -> Option<Vec<String>> {
    exact_selector_values_for_column(planned_write, "path")
}

fn exact_id_selector_values(planned_write: &PlannedWrite) -> Option<Vec<String>> {
    exact_selector_values_for_column(planned_write, "id")
}

fn exact_selector_values_for_column(
    planned_write: &PlannedWrite,
    column_name: &str,
) -> Option<Vec<String>> {
    if !planned_write.command.selector.exact_only {
        return exact_selector_values_from_residuals(planned_write, column_name);
    }
    if !planned_write
        .command
        .selector
        .exact_filters
        .keys()
        .all(|key| {
            matches!(
                key.as_str(),
                "id" | "path" | "version_id" | "lixcol_version_id"
            )
        })
    {
        return None;
    }
    planned_write
        .command
        .selector
        .exact_filters
        .get(column_name)
        .and_then(text_from_value)
        .map(|value| vec![value])
}

fn exact_selector_values_from_residuals(
    planned_write: &PlannedWrite,
    column_name: &str,
) -> Option<Vec<String>> {
    let [predicate] = planned_write
        .command
        .selector
        .residual_predicates
        .as_slice()
    else {
        return None;
    };
    let mut values = BTreeSet::new();
    let mut placeholder_state = PlaceholderState::new();
    if !collect_exact_selector_values(
        predicate,
        &planned_write.command.bound_parameters,
        &mut placeholder_state,
        column_name,
        &mut values,
    ) {
        return None;
    }
    if values.is_empty() {
        None
    } else {
        Some(values.into_iter().collect())
    }
}

fn collect_exact_selector_values(
    expr: &Expr,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
    column_name: &str,
    values: &mut BTreeSet<String>,
) -> bool {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect_exact_selector_values(left, params, placeholder_state, column_name, values)
                && collect_exact_selector_values(
                    right,
                    params,
                    placeholder_state,
                    column_name,
                    values,
                )
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            let Some(column) =
                exact_selector_column_name(left).or_else(|| exact_selector_column_name(right))
            else {
                return false;
            };
            if column == column_name {
                let value_expr = if exact_selector_column_name(left).is_some() {
                    right
                } else {
                    left
                };
                let value = exact_expr_text_value(value_expr, params, placeholder_state);
                if let Some(value) = value {
                    values.insert(value);
                    true
                } else {
                    false
                }
            } else {
                matches!(column.as_str(), "version_id" | "lixcol_version_id")
            }
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            let Some(column) = exact_selector_column_name(expr) else {
                return false;
            };
            if column != column_name {
                return matches!(column.as_str(), "version_id" | "lixcol_version_id");
            }
            let mut local = Vec::with_capacity(list.len());
            for value_expr in list {
                let Some(value) = exact_expr_text_value(value_expr, params, placeholder_state)
                else {
                    return false;
                };
                local.push(value);
            }
            values.extend(local);
            true
        }
        Expr::Nested(inner) => {
            collect_exact_selector_values(inner, params, placeholder_state, column_name, values)
        }
        _ => false,
    }
}

fn exact_selector_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(identifier) => Some(identifier.value.to_ascii_lowercase()),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|identifier| identifier.value.to_ascii_lowercase()),
        Expr::Nested(inner) => exact_selector_column_name(inner),
        _ => None,
    }
}

fn exact_expr_text_value(
    expr: &Expr,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Option<String> {
    match expr {
        Expr::Value(ValueWithSpan {
            value:
                SqlValue::SingleQuotedString(value)
                | SqlValue::DoubleQuotedString(value)
                | SqlValue::TripleSingleQuotedString(value)
                | SqlValue::TripleDoubleQuotedString(value)
                | SqlValue::EscapedStringLiteral(value)
                | SqlValue::SingleQuotedByteStringLiteral(value)
                | SqlValue::DoubleQuotedByteStringLiteral(value),
            ..
        }) => Some(value.clone()),
        Expr::Value(ValueWithSpan {
            value: SqlValue::Number(value, _),
            ..
        }) => Some(value.clone()),
        Expr::Value(ValueWithSpan {
            value: SqlValue::Boolean(value),
            ..
        }) => Some(value.to_string()),
        Expr::Value(ValueWithSpan {
            value: SqlValue::Placeholder(token),
            ..
        }) => {
            let index = resolve_placeholder_index(token, params.len(), placeholder_state).ok()?;
            match params.get(index)? {
                Value::Text(value) => Some(value.clone()),
                Value::Json(value) => Some(value.to_string()),
                Value::Integer(value) => Some(value.to_string()),
                Value::Real(value) => Some(value.to_string()),
                Value::Boolean(value) => Some(value.to_string()),
                Value::Null | Value::Blob(_) => None,
            }
        }
        Expr::Nested(inner) => exact_expr_text_value(inner, params, placeholder_state),
        _ => None,
    }
}

async fn assert_no_directory_cycle(
    backend: &dyn LixBackend,
    version_id: &str,
    directory_id: &str,
    parent_id: &str,
    lookup_scope: FilesystemProjectionScope,
) -> Result<(), WriteResolveError> {
    let mut safety = 0usize;
    let mut current_parent: Option<String> = Some(parent_id.to_string());
    while let Some(parent_id) = current_parent {
        if parent_id == directory_id {
            return Err(WriteResolveError {
                message: "Directory parent would create a cycle".to_string(),
            });
        }
        if safety > 1024 {
            return Err(WriteResolveError {
                message: "Directory hierarchy appears to be cyclic".to_string(),
            });
        }
        safety += 1;
        let Some(parent_row) =
            load_directory_row_by_id(backend, version_id, &parent_id, lookup_scope).await?
        else {
            return Err(WriteResolveError {
                message: format!("Parent directory does not exist for id {}", parent_id),
            });
        };
        current_parent = parent_row.parent_id;
    }
    Ok(())
}

fn directory_descriptor_row(
    entity_id: &str,
    parent_id: Option<&str>,
    name: &str,
    hidden: bool,
    version_id: &str,
    metadata: Option<&str>,
) -> PlannedStateRow {
    let snapshot_content = json!({
        "id": entity_id,
        "parent_id": parent_id,
        "name": name,
        "hidden": hidden,
    })
    .to_string();
    let mut values = BTreeMap::new();
    values.insert("entity_id".to_string(), Value::Text(entity_id.to_string()));
    values.insert(
        "schema_key".to_string(),
        Value::Text(FILESYSTEM_DIRECTORY_SCHEMA_KEY.to_string()),
    );
    values.insert(
        "file_id".to_string(),
        Value::Text(FILESYSTEM_DESCRIPTOR_FILE_ID.to_string()),
    );
    values.insert(
        "plugin_key".to_string(),
        Value::Text(FILESYSTEM_DESCRIPTOR_PLUGIN_KEY.to_string()),
    );
    values.insert(
        "schema_version".to_string(),
        Value::Text(FILESYSTEM_DIRECTORY_SCHEMA_VERSION.to_string()),
    );
    values.insert(
        "snapshot_content".to_string(),
        Value::Text(snapshot_content),
    );
    values.insert(
        "version_id".to_string(),
        Value::Text(version_id.to_string()),
    );
    if let Some(metadata) = metadata {
        values.insert("metadata".to_string(), Value::Text(metadata.to_string()));
    }
    PlannedStateRow {
        entity_id: entity_id.to_string(),
        schema_key: FILESYSTEM_DIRECTORY_SCHEMA_KEY.to_string(),
        version_id: Some(version_id.to_string()),
        values,
        writer_key: None,
        tombstone: false,
    }
}

fn file_descriptor_row(
    entity_id: &str,
    directory_id: Option<&str>,
    name: &str,
    extension: Option<&str>,
    hidden: bool,
    version_id: &str,
    metadata: Option<&str>,
) -> PlannedStateRow {
    let snapshot_content = json!({
        "id": entity_id,
        "directory_id": directory_id,
        "name": name,
        "extension": extension,
        "metadata": metadata,
        "hidden": hidden,
    })
    .to_string();
    let mut values = BTreeMap::new();
    values.insert("entity_id".to_string(), Value::Text(entity_id.to_string()));
    values.insert(
        "schema_key".to_string(),
        Value::Text(FILESYSTEM_FILE_SCHEMA_KEY.to_string()),
    );
    values.insert(
        "file_id".to_string(),
        Value::Text(FILESYSTEM_DESCRIPTOR_FILE_ID.to_string()),
    );
    values.insert(
        "plugin_key".to_string(),
        Value::Text(FILESYSTEM_DESCRIPTOR_PLUGIN_KEY.to_string()),
    );
    values.insert(
        "schema_version".to_string(),
        Value::Text(FILESYSTEM_FILE_SCHEMA_VERSION.to_string()),
    );
    values.insert(
        "snapshot_content".to_string(),
        Value::Text(snapshot_content),
    );
    values.insert(
        "version_id".to_string(),
        Value::Text(version_id.to_string()),
    );
    if let Some(metadata) = metadata {
        values.insert("metadata".to_string(), Value::Text(metadata.to_string()));
    }
    PlannedStateRow {
        entity_id: entity_id.to_string(),
        schema_key: FILESYSTEM_FILE_SCHEMA_KEY.to_string(),
        version_id: Some(version_id.to_string()),
        values,
        writer_key: None,
        tombstone: false,
    }
}

fn file_descriptor_tombstone_row(
    entity_id: &str,
    directory_id: Option<&str>,
    name: &str,
    extension: Option<&str>,
    hidden: bool,
    version_id: &str,
    metadata: Option<&str>,
) -> PlannedStateRow {
    let mut row = file_descriptor_row(
        entity_id,
        directory_id,
        name,
        extension,
        hidden,
        version_id,
        metadata,
    );
    row.values.remove("snapshot_content");
    row.tombstone = true;
    row
}

fn directory_descriptor_tombstone_row(
    entity_id: &str,
    parent_id: Option<&str>,
    name: &str,
    hidden: bool,
    version_id: &str,
    metadata: Option<&str>,
) -> PlannedStateRow {
    let mut row =
        directory_descriptor_row(entity_id, parent_id, name, hidden, version_id, metadata);
    row.values.remove("snapshot_content");
    row.tombstone = true;
    row
}

fn binary_blob_ref_row(
    file_id: &str,
    version_id: &str,
    data: &[u8],
) -> Result<PlannedStateRow, WriteResolveError> {
    let size_bytes = u64::try_from(data.len()).map_err(|_| WriteResolveError {
        message: format!(
            "binary blob size exceeds supported range for file '{}' version '{}'",
            file_id, version_id
        ),
    })?;
    let snapshot_content = json!({
        "id": file_id,
        "blob_hash": crate::binary_cas::codec::binary_blob_hash_hex(data),
        "size_bytes": size_bytes,
    })
    .to_string();
    let mut values = BTreeMap::new();
    values.insert("entity_id".to_string(), Value::Text(file_id.to_string()));
    values.insert(
        "schema_key".to_string(),
        Value::Text(FILESYSTEM_BINARY_BLOB_REF_SCHEMA_KEY.to_string()),
    );
    values.insert("file_id".to_string(), Value::Text(file_id.to_string()));
    values.insert(
        "plugin_key".to_string(),
        Value::Text(FILESYSTEM_DESCRIPTOR_PLUGIN_KEY.to_string()),
    );
    values.insert(
        "schema_version".to_string(),
        Value::Text(FILESYSTEM_BINARY_BLOB_REF_SCHEMA_VERSION.to_string()),
    );
    values.insert(
        "snapshot_content".to_string(),
        Value::Text(snapshot_content),
    );
    values.insert(
        "version_id".to_string(),
        Value::Text(version_id.to_string()),
    );
    Ok(PlannedStateRow {
        entity_id: file_id.to_string(),
        schema_key: FILESYSTEM_BINARY_BLOB_REF_SCHEMA_KEY.to_string(),
        version_id: Some(version_id.to_string()),
        values,
        writer_key: None,
        tombstone: false,
    })
}

fn binary_blob_ref_tombstone_row(file_id: &str, version_id: &str) -> PlannedStateRow {
    let mut values = BTreeMap::new();
    values.insert("entity_id".to_string(), Value::Text(file_id.to_string()));
    values.insert(
        "schema_key".to_string(),
        Value::Text(FILESYSTEM_BINARY_BLOB_REF_SCHEMA_KEY.to_string()),
    );
    values.insert("file_id".to_string(), Value::Text(file_id.to_string()));
    values.insert(
        "plugin_key".to_string(),
        Value::Text(FILESYSTEM_DESCRIPTOR_PLUGIN_KEY.to_string()),
    );
    values.insert(
        "schema_version".to_string(),
        Value::Text(FILESYSTEM_BINARY_BLOB_REF_SCHEMA_VERSION.to_string()),
    );
    values.insert(
        "version_id".to_string(),
        Value::Text(version_id.to_string()),
    );
    PlannedStateRow {
        entity_id: file_id.to_string(),
        schema_key: FILESYSTEM_BINARY_BLOB_REF_SCHEMA_KEY.to_string(),
        version_id: Some(version_id.to_string()),
        values,
        writer_key: None,
        tombstone: true,
    }
}

fn auto_directory_id(version_id: &str, path: &str) -> String {
    format!("lix-auto-dir:{}:{}", version_id, path)
}
