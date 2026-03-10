use crate::account::{
    active_account_file_id, active_account_plugin_key, active_account_schema_key,
    active_account_schema_version, active_account_snapshot_content,
    active_account_storage_version_id, parse_active_account_snapshot,
};
use crate::builtin_schema::builtin_schema_definition;
use crate::engine::query_storage::sql_text::escape_sql_string;
use crate::filesystem::live_projection::{
    build_filesystem_directory_projection_sql, build_filesystem_file_projection_sql,
    FilesystemProjectionScope,
};
use crate::filesystem::path::{
    compose_directory_path, directory_ancestor_paths, directory_name_from_path,
    normalize_directory_path, normalize_path_segment, parent_directory_path, parse_file_path,
};
use crate::schema::{SchemaProvider, SqlStoredSchemaProvider};
use crate::sql2::catalog::SurfaceFamily;
use crate::sql2::planner::ir::{
    MutationPayload, PlannedStateRow, PlannedWrite, ResolvedRowRef, ResolvedWritePlan, RowLineage,
    SchemaProof, ScopeProof, TargetSetProof, WriteLane, WriteMode, WriteOperationKind,
};
use crate::sql2::planner::semantics::effective_state_resolver::{
    resolve_exact_effective_state_row, ExactEffectiveStateRow, ExactEffectiveStateRowRequest,
    OverlayLane,
};
use crate::sql_shared::ast::{lower_statement, parse_sql_statements};
use crate::version::{
    active_version_file_id, active_version_plugin_key, active_version_schema_key,
    active_version_schema_version, active_version_snapshot_content,
    active_version_storage_version_id, parse_active_version_snapshot, version_descriptor_file_id,
    version_descriptor_plugin_key, version_descriptor_schema_key,
    version_descriptor_schema_version, version_descriptor_snapshot_content,
    version_descriptor_storage_version_id, version_pointer_file_id, version_pointer_plugin_key,
    version_pointer_schema_key, version_pointer_schema_version, version_pointer_snapshot_content,
    version_pointer_storage_version_id, GLOBAL_VERSION_ID,
};
use crate::{LixBackend, Value};
use serde_json::{json, Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use std::collections::BTreeMap;

const FILESYSTEM_DESCRIPTOR_FILE_ID: &str = "lix";
const FILESYSTEM_DESCRIPTOR_PLUGIN_KEY: &str = "lix";
const FILESYSTEM_DIRECTORY_SCHEMA_KEY: &str = "lix_directory_descriptor";
const FILESYSTEM_DIRECTORY_SCHEMA_VERSION: &str = "1";
const FILESYSTEM_FILE_SCHEMA_KEY: &str = "lix_file_descriptor";
const FILESYSTEM_FILE_SCHEMA_VERSION: &str = "1";
const FILESYSTEM_BINARY_BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
const FILESYSTEM_BINARY_BLOB_REF_SCHEMA_VERSION: &str = "1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WriteResolveError {
    pub(crate) message: String,
}

pub(crate) async fn resolve_write_plan(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let target_write_lane = match planned_write.command.mode {
        WriteMode::Tracked => Some(write_lane_from_scope(&planned_write.scope_proof)?),
        WriteMode::Untracked => None,
    };

    match planned_write.command.target.descriptor.surface_family {
        SurfaceFamily::State => match planned_write.command.operation_kind {
            WriteOperationKind::Insert => {
                resolve_state_insert_write_plan(planned_write, target_write_lane)
            }
            WriteOperationKind::Update | WriteOperationKind::Delete => {
                resolve_existing_state_write(backend, planned_write, target_write_lane).await
            }
        },
        SurfaceFamily::Entity => {
            let mut provider = SqlStoredSchemaProvider::new(backend);
            let entity_schema = load_entity_schema(&mut provider, planned_write)
                .await
                .map_err(write_resolve_backend_error)?;
            match planned_write.command.operation_kind {
                WriteOperationKind::Insert => resolve_entity_insert_write_plan(
                    planned_write,
                    target_write_lane,
                    &entity_schema,
                ),
                WriteOperationKind::Update | WriteOperationKind::Delete => {
                    resolve_existing_entity_write(
                        backend,
                        planned_write,
                        target_write_lane,
                        &entity_schema,
                    )
                    .await
                }
            }
        }
        SurfaceFamily::Admin => {
            resolve_admin_write(backend, planned_write, target_write_lane).await
        }
        SurfaceFamily::Filesystem => {
            resolve_filesystem_write(backend, planned_write, target_write_lane).await
        }
        SurfaceFamily::Change => Err(WriteResolveError {
            message: format!(
                "sql2 write resolver does not support '{}' writes",
                planned_write.command.target.descriptor.public_name
            ),
        }),
    }
}

fn resolve_state_insert_write_plan(
    planned_write: &PlannedWrite,
    target_write_lane: Option<WriteLane>,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let entity_id = resolved_entity_id(planned_write)?;
    let schema_key = resolved_schema_key(planned_write)?;
    let version_id = resolved_version_id(planned_write)?;

    Ok(ResolvedWritePlan {
        authoritative_pre_state: Vec::new(),
        intended_post_state: vec![PlannedStateRow {
            entity_id: entity_id.clone(),
            schema_key,
            version_id,
            values: payload_map(planned_write)?,
            tombstone: false,
        }],
        tombstones: Vec::new(),
        lineage: vec![RowLineage {
            entity_id,
            source_change_id: None,
            source_commit_id: None,
        }],
        target_write_lane,
    })
}

async fn resolve_existing_state_write(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    target_write_lane: Option<WriteLane>,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    ensure_exact_selector(planned_write)?;
    let schema_key = resolved_schema_key(planned_write)?;
    let version_id = resolved_version_id(planned_write)?.ok_or_else(|| WriteResolveError {
        message: "sql2 existing-row write resolver requires a concrete version_id".to_string(),
    })?;
    let Some(current_row) = resolve_exact_effective_state_row(
        backend,
        &ExactEffectiveStateRowRequest {
            schema_key: schema_key.clone(),
            version_id,
            exact_filters: planned_write.command.selector.exact_filters.clone(),
            include_global_overlay: true,
            include_untracked_overlay: true,
        },
    )
    .await
    .map_err(write_resolve_backend_error)?
    else {
        return Ok(ResolvedWritePlan {
            authoritative_pre_state: Vec::new(),
            intended_post_state: Vec::new(),
            tombstones: Vec::new(),
            lineage: Vec::new(),
            target_write_lane,
        });
    };
    ensure_local_tracked_overlay(&current_row)?;

    let row_ref = ResolvedRowRef {
        entity_id: current_row.entity_id.clone(),
        schema_key: current_row.schema_key.clone(),
        version_id: Some(current_row.version_id.clone()),
        source_change_id: current_row.source_change_id.clone(),
        source_commit_id: None,
    };
    let lineage = vec![RowLineage {
        entity_id: current_row.entity_id.clone(),
        source_change_id: current_row.source_change_id.clone(),
        source_commit_id: None,
    }];

    match planned_write.command.operation_kind {
        WriteOperationKind::Update => {
            let values = merged_update_values(&current_row.values, planned_write)?;
            ensure_identity_columns_preserved(
                &current_row.entity_id,
                &current_row.schema_key,
                &current_row.file_id,
                &current_row.version_id,
                &values,
            )?;

            Ok(ResolvedWritePlan {
                authoritative_pre_state: vec![row_ref],
                intended_post_state: vec![PlannedStateRow {
                    entity_id: current_row.entity_id.clone(),
                    schema_key: current_row.schema_key.clone(),
                    version_id: Some(current_row.version_id.clone()),
                    values,
                    tombstone: false,
                }],
                tombstones: Vec::new(),
                lineage,
                target_write_lane,
            })
        }
        WriteOperationKind::Delete => Ok(ResolvedWritePlan {
            authoritative_pre_state: vec![row_ref.clone()],
            intended_post_state: vec![PlannedStateRow {
                entity_id: current_row.entity_id,
                schema_key: current_row.schema_key,
                version_id: Some(current_row.version_id),
                values: current_row.values,
                tombstone: true,
            }],
            tombstones: vec![row_ref],
            lineage,
            target_write_lane,
        }),
        WriteOperationKind::Insert => Err(WriteResolveError {
            message: "sql2 existing-row resolver does not handle inserts".to_string(),
        }),
    }
}

async fn resolve_existing_entity_write(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    target_write_lane: Option<WriteLane>,
    entity_schema: &EntityWriteSchema,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    ensure_exact_selector(planned_write)?;
    reject_unsupported_entity_overrides(entity_schema)?;
    let version_id = resolved_version_id(planned_write)?.ok_or_else(|| WriteResolveError {
        message: "sql2 entity write resolver requires a concrete version_id".to_string(),
    })?;
    let entity_id = resolved_entity_id_for_entity(planned_write, entity_schema)?;
    let exact_filters = entity_state_exact_filters(planned_write, entity_schema, &entity_id)?;
    let Some(current_row) = resolve_exact_effective_state_row(
        backend,
        &ExactEffectiveStateRowRequest {
            schema_key: entity_schema.schema_key.clone(),
            version_id,
            exact_filters,
            include_global_overlay: true,
            include_untracked_overlay: true,
        },
    )
    .await
    .map_err(write_resolve_backend_error)?
    else {
        return Ok(ResolvedWritePlan {
            authoritative_pre_state: Vec::new(),
            intended_post_state: Vec::new(),
            tombstones: Vec::new(),
            lineage: Vec::new(),
            target_write_lane,
        });
    };
    ensure_local_tracked_overlay(&current_row)?;
    ensure_entity_selector_matches_current_row(entity_schema, planned_write, &current_row)?;

    let row_ref = ResolvedRowRef {
        entity_id: current_row.entity_id.clone(),
        schema_key: current_row.schema_key.clone(),
        version_id: Some(current_row.version_id.clone()),
        source_change_id: current_row.source_change_id.clone(),
        source_commit_id: None,
    };
    let lineage = vec![RowLineage {
        entity_id: current_row.entity_id.clone(),
        source_change_id: current_row.source_change_id.clone(),
        source_commit_id: None,
    }];

    match planned_write.command.operation_kind {
        WriteOperationKind::Update => {
            let values = merged_entity_update_values(planned_write, entity_schema, &current_row)?;
            Ok(ResolvedWritePlan {
                authoritative_pre_state: vec![row_ref],
                intended_post_state: vec![PlannedStateRow {
                    entity_id: current_row.entity_id.clone(),
                    schema_key: current_row.schema_key.clone(),
                    version_id: Some(current_row.version_id.clone()),
                    values,
                    tombstone: false,
                }],
                tombstones: Vec::new(),
                lineage,
                target_write_lane,
            })
        }
        WriteOperationKind::Delete => Ok(ResolvedWritePlan {
            authoritative_pre_state: vec![row_ref.clone()],
            intended_post_state: vec![PlannedStateRow {
                entity_id: current_row.entity_id,
                schema_key: current_row.schema_key,
                version_id: Some(current_row.version_id),
                values: current_row.values,
                tombstone: true,
            }],
            tombstones: vec![row_ref],
            lineage,
            target_write_lane,
        }),
        WriteOperationKind::Insert => Err(WriteResolveError {
            message: "sql2 entity existing-row resolver does not handle inserts".to_string(),
        }),
    }
}

fn resolve_entity_insert_write_plan(
    planned_write: &PlannedWrite,
    target_write_lane: Option<WriteLane>,
    entity_schema: &EntityWriteSchema,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    reject_unsupported_entity_overrides(entity_schema)?;
    let row = build_entity_insert_row(planned_write, entity_schema)?;
    Ok(ResolvedWritePlan {
        authoritative_pre_state: Vec::new(),
        intended_post_state: vec![row.clone()],
        tombstones: Vec::new(),
        lineage: vec![RowLineage {
            entity_id: row.entity_id,
            source_change_id: None,
            source_commit_id: None,
        }],
        target_write_lane,
    })
}

async fn resolve_filesystem_write(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    target_write_lane: Option<WriteLane>,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    match planned_write.command.target.descriptor.public_name.as_str() {
        "lix_file" | "lix_file_by_version" => match planned_write.command.operation_kind {
            WriteOperationKind::Insert => {
                resolve_file_insert_write_plan(backend, planned_write, target_write_lane).await
            }
            WriteOperationKind::Update | WriteOperationKind::Delete => {
                resolve_existing_file_write(backend, planned_write, target_write_lane).await
            }
        },
        "lix_directory" | "lix_directory_by_version" => {
            match planned_write.command.operation_kind {
                WriteOperationKind::Insert => {
                    resolve_directory_insert_write_plan(backend, planned_write, target_write_lane)
                        .await
                }
                WriteOperationKind::Update | WriteOperationKind::Delete => {
                    resolve_existing_directory_write(backend, planned_write, target_write_lane)
                        .await
                }
            }
        }
        other => Err(WriteResolveError {
            message: format!(
                "sql2 filesystem live slice does not yet support '{}' writes",
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
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<String, WriteResolveError> {
    match planned_write.command.target.descriptor.public_name.as_str() {
        "lix_file" | "lix_directory" => load_active_filesystem_version_id(backend).await,
        _ => resolved_version_id(planned_write)?.ok_or_else(|| WriteResolveError {
            message: "sql2 filesystem write requires a concrete version_id".to_string(),
        }),
    }
}

async fn load_active_filesystem_version_id(
    backend: &dyn LixBackend,
) -> Result<String, WriteResolveError> {
    let result = backend
        .execute(
            "SELECT snapshot_content \
             FROM lix_internal_state_untracked \
             WHERE schema_key = $1 \
               AND file_id = $2 \
               AND version_id = $3 \
               AND snapshot_content IS NOT NULL \
             ORDER BY updated_at DESC \
             LIMIT 1",
            &[
                Value::Text(active_version_schema_key().to_string()),
                Value::Text(active_version_file_id().to_string()),
                Value::Text(active_version_storage_version_id().to_string()),
            ],
        )
        .await
        .map_err(write_resolve_backend_error)?;
    let Some(row) = result.rows.first() else {
        return Err(WriteResolveError {
            message: "sql2 filesystem write requires an active version".to_string(),
        });
    };
    let Some(snapshot_content) = row.first().and_then(text_from_value) else {
        return Err(WriteResolveError {
            message: "sql2 filesystem active-version lookup expected snapshot_content text"
                .to_string(),
        });
    };
    parse_active_version_snapshot(&snapshot_content).map_err(write_resolve_backend_error)
}

async fn resolve_directory_insert_write_plan(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    target_write_lane: Option<WriteLane>,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let MutationPayload::FullSnapshot(payload) = &planned_write.command.payload else {
        return Err(WriteResolveError {
            message: "sql2 filesystem directory insert requires a full snapshot payload"
                .to_string(),
        });
    };
    let version_id = resolved_filesystem_version_id(backend, planned_write).await?;
    let lookup_scope = filesystem_write_lookup_scope(planned_write);

    let computed =
        resolve_directory_insert_target(backend, planned_write, payload, &version_id, lookup_scope)
            .await?;
    let mut intended_post_state = Vec::new();
    let mut lineage = Vec::new();

    for ancestor in &computed.ancestor_rows {
        intended_post_state.push(directory_descriptor_row(
            &ancestor.id,
            ancestor.parent_id.as_deref(),
            &ancestor.name,
            ancestor.hidden,
            &ancestor.version_id,
            ancestor.metadata.as_deref(),
        ));
        lineage.push(RowLineage {
            entity_id: ancestor.id.clone(),
            source_change_id: None,
            source_commit_id: None,
        });
    }

    intended_post_state.push(directory_descriptor_row(
        &computed.id,
        computed.parent_id.as_deref(),
        &computed.name,
        computed.hidden,
        &version_id,
        computed.metadata.as_deref(),
    ));
    lineage.push(RowLineage {
        entity_id: computed.id.clone(),
        source_change_id: None,
        source_commit_id: None,
    });

    Ok(ResolvedWritePlan {
        authoritative_pre_state: Vec::new(),
        intended_post_state,
        tombstones: Vec::new(),
        lineage,
        target_write_lane,
    })
}

async fn resolve_existing_directory_write(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    target_write_lane: Option<WriteLane>,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    ensure_exact_selector(planned_write)?;
    let version_id = resolved_filesystem_version_id(backend, planned_write).await?;
    let lookup_scope = filesystem_write_lookup_scope(planned_write);
    let Some(current_row) =
        load_target_directory_row(backend, planned_write, &version_id, lookup_scope).await?
    else {
        return Ok(ResolvedWritePlan {
            authoritative_pre_state: Vec::new(),
            intended_post_state: Vec::new(),
            tombstones: Vec::new(),
            lineage: Vec::new(),
            target_write_lane,
        });
    };
    if !directory_row_matches_exact_filters(
        &current_row,
        &planned_write.command.selector.exact_filters,
    ) {
        return Ok(ResolvedWritePlan {
            authoritative_pre_state: Vec::new(),
            intended_post_state: Vec::new(),
            tombstones: Vec::new(),
            lineage: Vec::new(),
            target_write_lane,
        });
    }

    match planned_write.command.mode {
        WriteMode::Tracked if current_row.untracked => {
            return Err(WriteResolveError {
                message: "sql2 live tracked filesystem writes do not yet support untracked winners"
                    .to_string(),
            })
        }
        WriteMode::Untracked if !current_row.untracked => {
            return Err(WriteResolveError {
                message: "sql2 untracked filesystem update requires an untracked visible row"
                    .to_string(),
            })
        }
        _ => {}
    }

    match planned_write.command.operation_kind {
        WriteOperationKind::Update => {
            let MutationPayload::Patch(payload) = &planned_write.command.payload else {
                return Err(WriteResolveError {
                    message: "sql2 filesystem directory update requires a patch payload"
                        .to_string(),
                });
            };
            if payload.contains_key("id") {
                return Err(WriteResolveError {
                    message:
                        "lix_directory id is immutable; create a new row and delete the old row instead"
                            .to_string(),
                });
            }

            let next_row = resolve_directory_update_target(
                backend,
                &current_row,
                payload,
                &version_id,
                lookup_scope,
            )
            .await?;
            let row_ref = ResolvedRowRef {
                entity_id: current_row.id.clone(),
                schema_key: FILESYSTEM_DIRECTORY_SCHEMA_KEY.to_string(),
                version_id: Some(version_id.clone()),
                source_change_id: current_row.change_id.clone(),
                source_commit_id: None,
            };

            Ok(ResolvedWritePlan {
                authoritative_pre_state: vec![row_ref.clone()],
                intended_post_state: vec![directory_descriptor_row(
                    &current_row.id,
                    next_row.parent_id.as_deref(),
                    &next_row.name,
                    next_row.hidden,
                    &version_id,
                    next_row.metadata.as_deref(),
                )],
                tombstones: Vec::new(),
                lineage: vec![RowLineage {
                    entity_id: current_row.id,
                    source_change_id: row_ref.source_change_id,
                    source_commit_id: None,
                }],
                target_write_lane,
            })
        }
        WriteOperationKind::Delete => {
            let descendant_directories =
                load_directory_rows_under_path(backend, &version_id, &current_row.path).await?;
            let descendant_files =
                load_file_rows_under_path(backend, &version_id, &current_row.path).await?;

            match planned_write.command.mode {
                WriteMode::Tracked
                    if descendant_directories.iter().any(|row| row.untracked)
                        || descendant_files.iter().any(|row| row.untracked) =>
                {
                    return Err(WriteResolveError {
                        message: "sql2 live tracked filesystem directory deletes do not yet support untracked winners in the cascade".to_string(),
                    });
                }
                WriteMode::Untracked
                    if descendant_directories.iter().any(|row| !row.untracked)
                        || descendant_files.iter().any(|row| !row.untracked) =>
                {
                    return Err(WriteResolveError {
                        message: "sql2 untracked filesystem directory delete requires untracked visible rows throughout the cascade".to_string(),
                    });
                }
                _ => {}
            }

            let mut authoritative_pre_state = Vec::new();
            let mut intended_post_state = Vec::new();
            let mut tombstones = Vec::new();
            let mut lineage = Vec::new();

            for row in &descendant_directories {
                let row_ref = ResolvedRowRef {
                    entity_id: row.id.clone(),
                    schema_key: FILESYSTEM_DIRECTORY_SCHEMA_KEY.to_string(),
                    version_id: Some(version_id.clone()),
                    source_change_id: row.change_id.clone(),
                    source_commit_id: None,
                };
                authoritative_pre_state.push(row_ref.clone());
                intended_post_state.push(directory_descriptor_tombstone_row(
                    &row.id,
                    row.parent_id.as_deref(),
                    &row.name,
                    row.hidden,
                    &version_id,
                    row.metadata.as_deref(),
                ));
                tombstones.push(row_ref);
                lineage.push(RowLineage {
                    entity_id: row.id.clone(),
                    source_change_id: row.change_id.clone(),
                    source_commit_id: None,
                });
            }

            for row in &descendant_files {
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
                authoritative_pre_state.push(file_ref.clone());
                authoritative_pre_state.push(blob_ref.clone());
                intended_post_state.push(file_descriptor_tombstone_row(
                    &row.id,
                    row.directory_id.as_deref(),
                    &row.name,
                    row.extension.as_deref(),
                    row.hidden,
                    &version_id,
                    row.metadata.as_deref(),
                ));
                intended_post_state.push(binary_blob_ref_tombstone_row(&row.id, &version_id));
                tombstones.push(file_ref);
                tombstones.push(blob_ref);
                lineage.push(RowLineage {
                    entity_id: row.id.clone(),
                    source_change_id: row.change_id.clone(),
                    source_commit_id: None,
                });
            }

            Ok(ResolvedWritePlan {
                authoritative_pre_state,
                intended_post_state,
                tombstones,
                lineage,
                target_write_lane,
            })
        }
        WriteOperationKind::Insert => Err(WriteResolveError {
            message: "sql2 filesystem directory existing-row resolver does not handle inserts"
                .to_string(),
        }),
    }
}

async fn resolve_file_insert_write_plan(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    target_write_lane: Option<WriteLane>,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let MutationPayload::FullSnapshot(payload) = &planned_write.command.payload else {
        return Err(WriteResolveError {
            message: "sql2 filesystem file insert requires a full snapshot payload".to_string(),
        });
    };
    let version_id = resolved_filesystem_version_id(backend, planned_write).await?;
    let lookup_scope = filesystem_write_lookup_scope(planned_write);
    let computed =
        resolve_file_insert_target(backend, planned_write, payload, &version_id, lookup_scope)
            .await?;
    let payload_bytes = payload_binary_value(payload, "data")?;

    let mut intended_post_state = Vec::new();
    let mut lineage = Vec::new();

    for ancestor in &computed.ancestor_rows {
        intended_post_state.push(directory_descriptor_row(
            &ancestor.id,
            ancestor.parent_id.as_deref(),
            &ancestor.name,
            ancestor.hidden,
            &ancestor.version_id,
            ancestor.metadata.as_deref(),
        ));
        lineage.push(RowLineage {
            entity_id: ancestor.id.clone(),
            source_change_id: None,
            source_commit_id: None,
        });
    }

    intended_post_state.push(file_descriptor_row(
        &computed.id,
        computed.directory_id.as_deref(),
        &computed.name,
        computed.extension.as_deref(),
        computed.hidden,
        &version_id,
        computed.metadata.as_deref(),
    ));
    lineage.push(RowLineage {
        entity_id: computed.id.clone(),
        source_change_id: None,
        source_commit_id: None,
    });

    if let Some(bytes) = payload_bytes {
        intended_post_state.push(binary_blob_ref_row(&computed.id, &version_id, &bytes)?);
    }

    Ok(ResolvedWritePlan {
        authoritative_pre_state: Vec::new(),
        intended_post_state,
        tombstones: Vec::new(),
        lineage,
        target_write_lane,
    })
}

async fn resolve_existing_file_write(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    target_write_lane: Option<WriteLane>,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    ensure_exact_selector(planned_write)?;
    let version_id = resolved_filesystem_version_id(backend, planned_write).await?;
    let lookup_scope = filesystem_write_lookup_scope(planned_write);
    let Some(current_row) =
        load_target_file_row(backend, planned_write, &version_id, lookup_scope).await?
    else {
        return Ok(ResolvedWritePlan {
            authoritative_pre_state: Vec::new(),
            intended_post_state: Vec::new(),
            tombstones: Vec::new(),
            lineage: Vec::new(),
            target_write_lane,
        });
    };
    if !file_row_matches_exact_filters(&current_row, &planned_write.command.selector.exact_filters)
    {
        return Ok(ResolvedWritePlan {
            authoritative_pre_state: Vec::new(),
            intended_post_state: Vec::new(),
            tombstones: Vec::new(),
            lineage: Vec::new(),
            target_write_lane,
        });
    }

    match planned_write.command.mode {
        WriteMode::Tracked if current_row.untracked => {
            return Err(WriteResolveError {
                message: "sql2 live tracked filesystem writes do not yet support untracked winners"
                    .to_string(),
            })
        }
        WriteMode::Untracked if !current_row.untracked => {
            return Err(WriteResolveError {
                message: "sql2 untracked filesystem update requires an untracked visible row"
                    .to_string(),
            })
        }
        _ => {}
    }

    let row_ref = ResolvedRowRef {
        entity_id: current_row.id.clone(),
        schema_key: FILESYSTEM_FILE_SCHEMA_KEY.to_string(),
        version_id: Some(version_id.clone()),
        source_change_id: current_row.change_id.clone(),
        source_commit_id: None,
    };

    match planned_write.command.operation_kind {
        WriteOperationKind::Update => {
            let MutationPayload::Patch(payload) = &planned_write.command.payload else {
                return Err(WriteResolveError {
                    message: "sql2 filesystem file update requires a patch payload".to_string(),
                });
            };
            if payload.contains_key("id") {
                return Err(WriteResolveError {
                    message:
                        "lix_file id is immutable; create a new row and delete the old row instead"
                            .to_string(),
                });
            }

            let payload_bytes = payload_binary_value(payload, "data")?;
            let (next_row, ancestor_rows) = resolve_file_update_target(
                backend,
                &current_row,
                payload,
                &version_id,
                lookup_scope,
            )
            .await?;
            let descriptor_changed =
                file_descriptor_changed(&current_row, &next_row) || !ancestor_rows.is_empty();
            let mut intended_post_state = Vec::new();
            let mut lineage = vec![RowLineage {
                entity_id: current_row.id.clone(),
                source_change_id: row_ref.source_change_id.clone(),
                source_commit_id: None,
            }];

            for ancestor in &ancestor_rows {
                intended_post_state.push(directory_descriptor_row(
                    &ancestor.id,
                    ancestor.parent_id.as_deref(),
                    &ancestor.name,
                    ancestor.hidden,
                    &ancestor.version_id,
                    ancestor.metadata.as_deref(),
                ));
                lineage.push(RowLineage {
                    entity_id: ancestor.id.clone(),
                    source_change_id: None,
                    source_commit_id: None,
                });
            }

            if descriptor_changed {
                intended_post_state.push(file_descriptor_row(
                    &current_row.id,
                    next_row.directory_id.as_deref(),
                    &next_row.name,
                    next_row.extension.as_deref(),
                    next_row.hidden,
                    &version_id,
                    next_row.metadata.as_deref(),
                ));
            }

            if let Some(bytes) = payload_bytes {
                intended_post_state.push(binary_blob_ref_row(
                    &current_row.id,
                    &version_id,
                    &bytes,
                )?);
            }

            Ok(ResolvedWritePlan {
                authoritative_pre_state: vec![row_ref],
                intended_post_state,
                tombstones: Vec::new(),
                lineage,
                target_write_lane,
            })
        }
        WriteOperationKind::Delete => {
            let blob_ref = ResolvedRowRef {
                entity_id: current_row.id.clone(),
                schema_key: FILESYSTEM_BINARY_BLOB_REF_SCHEMA_KEY.to_string(),
                version_id: Some(version_id.clone()),
                source_change_id: None,
                source_commit_id: None,
            };
            Ok(ResolvedWritePlan {
                authoritative_pre_state: vec![row_ref.clone(), blob_ref.clone()],
                intended_post_state: vec![
                    file_descriptor_tombstone_row(
                        &current_row.id,
                        current_row.directory_id.as_deref(),
                        &current_row.name,
                        current_row.extension.as_deref(),
                        current_row.hidden,
                        &version_id,
                        current_row.metadata.as_deref(),
                    ),
                    binary_blob_ref_tombstone_row(&current_row.id, &version_id),
                ],
                tombstones: vec![row_ref, blob_ref],
                lineage: vec![RowLineage {
                    entity_id: current_row.id,
                    source_change_id: current_row.change_id,
                    source_commit_id: None,
                }],
                target_write_lane,
            })
        }
        WriteOperationKind::Insert => Err(WriteResolveError {
            message: "sql2 filesystem existing-row resolver does not handle inserts".to_string(),
        }),
    }
}

#[derive(Debug, Clone)]
struct ResolvedFileInsertTarget {
    id: String,
    directory_id: Option<String>,
    name: String,
    extension: Option<String>,
    hidden: bool,
    metadata: Option<String>,
    ancestor_rows: Vec<DirectoryFilesystemRow>,
}

async fn resolve_file_insert_target(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    payload: &BTreeMap<String, Value>,
    version_id: &str,
    lookup_scope: FilesystemProjectionScope,
) -> Result<ResolvedFileInsertTarget, WriteResolveError> {
    if !payload
        .keys()
        .any(|key| !matches!(key.as_str(), "data" | "version_id" | "untracked"))
    {
        return Err(WriteResolveError {
            message: "file insert requires at least one non-data column".to_string(),
        });
    }
    let explicit_path = payload_text_required(payload, "path", "sql2 filesystem file insert")?;
    let parsed = parse_file_path(&explicit_path).map_err(write_resolve_backend_error)?;
    let explicit_id = payload.get("id").and_then(text_from_value);
    let hidden = payload
        .get("hidden")
        .and_then(value_as_bool)
        .unwrap_or(false);
    let metadata = payload_optional_text(payload, "metadata")?;
    ensure_no_directory_at_file_path(backend, version_id, &parsed.normalized_path, lookup_scope)
        .await?;

    let (directory_id, ancestor_rows) = resolve_parent_directory_target(
        backend,
        version_id,
        parsed.directory_path.as_deref(),
        planned_write.command.mode == WriteMode::Untracked,
        lookup_scope,
    )
    .await?;

    if let Some(existing_id) =
        lookup_file_id_by_path(backend, version_id, &parsed.normalized_path, lookup_scope).await?
    {
        let same_id = explicit_id
            .as_deref()
            .map(|value| value == existing_id.as_str())
            .unwrap_or(false);
        if !same_id {
            return Err(WriteResolveError {
                message: format!(
                    "Unique constraint violation: file path '{}' already exists in version '{}'",
                    parsed.normalized_path, version_id
                ),
            });
        }
    }

    Ok(ResolvedFileInsertTarget {
        id: explicit_id.unwrap_or_else(|| auto_file_id(version_id, &parsed.normalized_path)),
        directory_id,
        name: parsed.name,
        extension: parsed.extension,
        hidden,
        metadata,
        ancestor_rows,
    })
}

async fn resolve_file_update_target(
    backend: &dyn LixBackend,
    current_row: &FileFilesystemRow,
    payload: &BTreeMap<String, Value>,
    version_id: &str,
    lookup_scope: FilesystemProjectionScope,
) -> Result<(FileFilesystemRow, Vec<DirectoryFilesystemRow>), WriteResolveError> {
    let next_hidden = payload
        .get("hidden")
        .and_then(value_as_bool)
        .unwrap_or(current_row.hidden);
    let next_metadata = if payload.contains_key("metadata") {
        payload_optional_text(payload, "metadata")?
    } else {
        current_row.metadata.clone()
    };

    let mut ancestor_rows = Vec::new();
    let (next_directory_id, next_name, next_extension, next_path) =
        if let Some(raw_path) = payload.get("path").and_then(text_from_value) {
            let parsed = parse_file_path(&raw_path).map_err(write_resolve_backend_error)?;
            ensure_no_directory_at_file_path(
                backend,
                version_id,
                &parsed.normalized_path,
                lookup_scope,
            )
            .await?;
            let (directory_id, missing_ancestors) = resolve_parent_directory_target(
                backend,
                version_id,
                parsed.directory_path.as_deref(),
                current_row.untracked,
                lookup_scope,
            )
            .await?;
            ancestor_rows = missing_ancestors;
            (
                directory_id,
                parsed.name,
                parsed.extension,
                parsed.normalized_path,
            )
        } else {
            (
                current_row.directory_id.clone(),
                current_row.name.clone(),
                current_row.extension.clone(),
                current_row.path.clone(),
            )
        };

    if let Some(existing_id) =
        lookup_file_id_by_path(backend, version_id, &next_path, lookup_scope).await?
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

    Ok((
        FileFilesystemRow {
            id: current_row.id.clone(),
            directory_id: next_directory_id,
            name: next_name,
            extension: next_extension,
            path: next_path,
            hidden: next_hidden,
            version_id: version_id.to_string(),
            untracked: current_row.untracked,
            metadata: next_metadata,
            change_id: current_row.change_id.clone(),
        },
        ancestor_rows,
    ))
}

async fn resolve_parent_directory_target(
    backend: &dyn LixBackend,
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
        version_id,
        directory_path,
        untracked,
        lookup_scope,
    )
    .await?;
    let directory_id = if let Some(last_row) = missing_rows.last() {
        Some(last_row.id.clone())
    } else {
        lookup_directory_id_by_path(backend, version_id, directory_path, lookup_scope).await?
    };
    Ok((directory_id, missing_rows))
}

async fn resolve_missing_directory_rows(
    backend: &dyn LixBackend,
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
        if let Some(existing_id) =
            lookup_directory_id_by_path(backend, version_id, &candidate_path, lookup_scope).await?
        {
            known_ids.insert(candidate_path, existing_id);
            continue;
        }
        ensure_no_file_at_directory_path(backend, version_id, &candidate_path, lookup_scope)
            .await?;
        let parent_id = match parent_directory_path(&candidate_path) {
            Some(parent_path) => {
                if let Some(parent_id) = known_ids.get(&parent_path).cloned() {
                    Some(parent_id)
                } else if let Some(existing_parent_id) =
                    lookup_directory_id_by_path(backend, version_id, &parent_path, lookup_scope)
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

async fn ensure_no_directory_at_file_path(
    backend: &dyn LixBackend,
    version_id: &str,
    file_path: &str,
    lookup_scope: FilesystemProjectionScope,
) -> Result<(), WriteResolveError> {
    let directory_path = format!("{}/", file_path.trim_end_matches('/'));
    if lookup_directory_id_by_path(backend, version_id, &directory_path, lookup_scope)
        .await?
        .is_none()
    {
        return Ok(());
    }
    Err(WriteResolveError {
        message: format!("File path collides with existing directory path: {directory_path}"),
    })
}

fn payload_text_required(
    payload: &BTreeMap<String, Value>,
    key: &str,
    context: &str,
) -> Result<String, WriteResolveError> {
    payload
        .get(key)
        .and_then(text_from_value)
        .ok_or_else(|| WriteResolveError {
            message: format!("{context} requires column '{key}'"),
        })
}

fn payload_optional_text(
    payload: &BTreeMap<String, Value>,
    key: &str,
) -> Result<Option<String>, WriteResolveError> {
    match payload.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Text(value)) => Ok(Some(value.clone())),
        Some(other) => Err(WriteResolveError {
            message: format!("sql2 filesystem resolver expected text/null {key}, got {other:?}"),
        }),
    }
}

fn payload_binary_value(
    payload: &BTreeMap<String, Value>,
    key: &str,
) -> Result<Option<Vec<u8>>, WriteResolveError> {
    match payload.get(key) {
        None => Ok(None),
        Some(Value::Blob(bytes)) => Ok(Some(bytes.clone())),
        Some(Value::Text(_)) => Err(WriteResolveError {
            message:
                "data expects bytes; use lix_text_encode('...') for text, X'HEX', or a blob parameter"
                    .to_string(),
        }),
        Some(other) => Err(WriteResolveError {
            message: format!("sql2 filesystem resolver expected blob {key}, got {other:?}"),
        }),
    }
}

fn file_descriptor_changed(current_row: &FileFilesystemRow, next_row: &FileFilesystemRow) -> bool {
    current_row.directory_id != next_row.directory_id
        || current_row.name != next_row.name
        || current_row.extension != next_row.extension
        || current_row.hidden != next_row.hidden
        || current_row.metadata != next_row.metadata
}

#[derive(Debug, Clone)]
struct ResolvedDirectoryInsertTarget {
    id: String,
    parent_id: Option<String>,
    name: String,
    hidden: bool,
    metadata: Option<String>,
    ancestor_rows: Vec<DirectoryFilesystemRow>,
}

async fn resolve_directory_insert_target(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    payload: &BTreeMap<String, Value>,
    version_id: &str,
    lookup_scope: FilesystemProjectionScope,
) -> Result<ResolvedDirectoryInsertTarget, WriteResolveError> {
    let explicit_id = payload.get("id").and_then(text_from_value);
    let explicit_parent_id = payload.get("parent_id").and_then(text_from_value);
    let explicit_name = payload.get("name").and_then(text_from_value);
    let explicit_path = payload.get("path").and_then(text_from_value);
    let hidden = payload
        .get("hidden")
        .and_then(value_as_bool)
        .unwrap_or(false);
    let metadata = payload.get("metadata").and_then(text_from_value);

    let (parent_id, name, normalized_path) = if let Some(raw_path) = explicit_path {
        let normalized_path =
            normalize_directory_path(&raw_path).map_err(write_resolve_backend_error)?;
        let derived_name =
            directory_name_from_path(&normalized_path).ok_or_else(|| WriteResolveError {
                message: "Directory name must be provided".to_string(),
            })?;
        let derived_parent_id = match parent_directory_path(&normalized_path) {
            Some(parent_path) => {
                lookup_directory_id_by_path(backend, version_id, &parent_path, lookup_scope)
                    .await?
                    .or_else(|| Some(auto_directory_id(version_id, &parent_path)))
            }
            None => None,
        };

        if explicit_parent_id.as_deref() != derived_parent_id.as_deref()
            && explicit_parent_id.is_some()
        {
            return Err(WriteResolveError {
                message: format!(
                    "Provided parent_id does not match parent derived from path {}",
                    normalized_path
                ),
            });
        }
        if let Some(name) = explicit_name {
            if normalize_path_segment(&name).map_err(write_resolve_backend_error)? != derived_name {
                return Err(WriteResolveError {
                    message: format!(
                        "Provided directory name '{}' does not match path '{}'",
                        name, normalized_path
                    ),
                });
            }
        }
        (derived_parent_id, derived_name, normalized_path)
    } else {
        let raw_name = explicit_name.ok_or_else(|| WriteResolveError {
            message: "Directory name must be provided".to_string(),
        })?;
        let name = normalize_path_segment(&raw_name).map_err(write_resolve_backend_error)?;
        let parent_path = match explicit_parent_id.as_deref() {
            Some(parent_id) => {
                lookup_directory_path_by_id(backend, version_id, parent_id, lookup_scope)
                    .await?
                    .ok_or_else(|| WriteResolveError {
                        message: format!("Parent directory does not exist for id {parent_id}"),
                    })?
            }
            None => "/".to_string(),
        };
        let computed_path = compose_directory_path(parent_path.as_str(), &name)
            .map_err(write_resolve_backend_error)?;
        (explicit_parent_id, name, computed_path)
    };

    if let Some(existing_id) =
        lookup_directory_id_by_path(backend, version_id, &normalized_path, lookup_scope).await?
    {
        let same_id = explicit_id
            .as_deref()
            .map(|value| value == existing_id.as_str())
            .unwrap_or(false);
        if !same_id {
            return Err(WriteResolveError {
                message: format!(
                    "Unique constraint violation: directory path '{}' already exists in version '{}'",
                    normalized_path, version_id
                ),
            });
        }
    }
    ensure_no_file_at_directory_path(backend, version_id, &normalized_path, lookup_scope).await?;

    let mut ancestor_rows = Vec::new();
    for ancestor_path in directory_ancestor_paths(&normalized_path) {
        if lookup_directory_id_by_path(backend, version_id, &ancestor_path, lookup_scope)
            .await?
            .is_some()
        {
            continue;
        }
        let ancestor_parent_id = match parent_directory_path(&ancestor_path) {
            Some(path) => {
                Some(lookup_or_auto_directory_id(backend, version_id, &path, lookup_scope).await?)
            }
            None => None,
        };
        ancestor_rows.push(DirectoryFilesystemRow {
            id: auto_directory_id(version_id, &ancestor_path),
            parent_id: ancestor_parent_id,
            name: directory_name_from_path(&ancestor_path).unwrap_or_default(),
            path: ancestor_path,
            hidden: false,
            version_id: version_id.to_string(),
            untracked: planned_write.command.mode == WriteMode::Untracked,
            metadata: None,
            change_id: None,
        });
    }

    let id = explicit_id.unwrap_or_else(|| auto_directory_id(version_id, &normalized_path));
    Ok(ResolvedDirectoryInsertTarget {
        id,
        parent_id,
        name,
        hidden,
        metadata,
        ancestor_rows,
    })
}

async fn resolve_directory_update_target(
    backend: &dyn LixBackend,
    current_row: &DirectoryFilesystemRow,
    payload: &BTreeMap<String, Value>,
    version_id: &str,
    lookup_scope: FilesystemProjectionScope,
) -> Result<DirectoryFilesystemRow, WriteResolveError> {
    let next_path = payload.get("path").and_then(text_from_value);
    let next_parent_id = payload.get("parent_id").and_then(text_from_value);
    let next_name = payload.get("name").and_then(text_from_value);
    let next_hidden = payload
        .get("hidden")
        .and_then(value_as_bool)
        .unwrap_or(current_row.hidden);
    let next_metadata = payload
        .get("metadata")
        .and_then(text_from_value)
        .or_else(|| current_row.metadata.clone());

    let (resolved_parent_id, resolved_name, resolved_path) = if let Some(raw_path) = next_path {
        let normalized_path =
            normalize_directory_path(&raw_path).map_err(write_resolve_backend_error)?;
        let name = directory_name_from_path(&normalized_path).ok_or_else(|| WriteResolveError {
            message: "Directory name must be provided".to_string(),
        })?;
        let parent_id = match parent_directory_path(&normalized_path) {
            Some(parent_path) => {
                lookup_directory_id_by_path(backend, version_id, &parent_path, lookup_scope)
                    .await?
                    .ok_or_else(|| WriteResolveError {
                        message: format!(
                            "Parent directory does not exist for path {}",
                            parent_path
                        ),
                    })?
            }
            None => String::new(),
        };
        let parent_id_opt = if parent_id.is_empty() {
            None
        } else {
            Some(parent_id)
        };
        (parent_id_opt, name, normalized_path)
    } else {
        let parent_id = next_parent_id.or_else(|| current_row.parent_id.clone());
        let name_raw = next_name.unwrap_or_else(|| current_row.name.clone());
        let name = normalize_path_segment(&name_raw).map_err(write_resolve_backend_error)?;
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
    if let Some(existing_id) =
        lookup_directory_id_by_path(backend, version_id, &resolved_path, lookup_scope).await?
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
    ensure_no_file_at_directory_path(backend, version_id, &resolved_path, lookup_scope).await?;

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

async fn load_target_directory_row(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    version_id: &str,
    lookup_scope: FilesystemProjectionScope,
) -> Result<Option<DirectoryFilesystemRow>, WriteResolveError> {
    if let Some(id) = planned_write
        .command
        .selector
        .exact_filters
        .get("id")
        .and_then(text_from_value)
    {
        return load_directory_row_by_id(backend, version_id, &id, lookup_scope).await;
    }
    if let Some(path) = planned_write
        .command
        .selector
        .exact_filters
        .get("path")
        .and_then(text_from_value)
    {
        let normalized = normalize_directory_path(&path).map_err(write_resolve_backend_error)?;
        let Some(directory_id) =
            lookup_directory_id_by_path(backend, version_id, &normalized, lookup_scope).await?
        else {
            return Ok(None);
        };
        return load_directory_row_by_id(backend, version_id, &directory_id, lookup_scope).await;
    }
    Err(WriteResolveError {
        message: "sql2 filesystem directory update requires an exact id or path selector"
            .to_string(),
    })
}

async fn load_target_file_row(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    version_id: &str,
    lookup_scope: FilesystemProjectionScope,
) -> Result<Option<FileFilesystemRow>, WriteResolveError> {
    if let Some(id) = planned_write
        .command
        .selector
        .exact_filters
        .get("id")
        .and_then(text_from_value)
    {
        return load_file_row_by_id(backend, version_id, &id, lookup_scope).await;
    }
    if let Some(path) = planned_write
        .command
        .selector
        .exact_filters
        .get("path")
        .and_then(text_from_value)
    {
        let normalized = parse_file_path(&path)
            .map_err(write_resolve_backend_error)?
            .normalized_path;
        let Some(file_id) =
            lookup_file_id_by_path(backend, version_id, &normalized, lookup_scope).await?
        else {
            return Ok(None);
        };
        return load_file_row_by_id(backend, version_id, &file_id, lookup_scope).await;
    }
    Err(WriteResolveError {
        message: "sql2 filesystem file write requires an exact id or path selector".to_string(),
    })
}

async fn lookup_or_auto_directory_id(
    backend: &dyn LixBackend,
    version_id: &str,
    path: &str,
    lookup_scope: FilesystemProjectionScope,
) -> Result<String, WriteResolveError> {
    Ok(
        lookup_directory_id_by_path(backend, version_id, path, lookup_scope)
            .await?
            .unwrap_or_else(|| auto_directory_id(version_id, path)),
    )
}

async fn lookup_directory_id_by_path(
    backend: &dyn LixBackend,
    version_id: &str,
    path: &str,
    lookup_scope: FilesystemProjectionScope,
) -> Result<Option<String>, WriteResolveError> {
    Ok(
        load_directory_row_by_path(backend, version_id, path, lookup_scope)
            .await?
            .map(|row| row.id),
    )
}

async fn lookup_file_id_by_path(
    backend: &dyn LixBackend,
    version_id: &str,
    path: &str,
    lookup_scope: FilesystemProjectionScope,
) -> Result<Option<String>, WriteResolveError> {
    Ok(
        load_file_row_by_path(backend, version_id, path, lookup_scope)
            .await?
            .map(|row| row.id),
    )
}

async fn lookup_directory_path_by_id(
    backend: &dyn LixBackend,
    version_id: &str,
    directory_id: &str,
    lookup_scope: FilesystemProjectionScope,
) -> Result<Option<String>, WriteResolveError> {
    Ok(
        load_directory_row_by_id(backend, version_id, directory_id, lookup_scope)
            .await?
            .map(|row| row.path),
    )
}

async fn ensure_no_file_at_directory_path(
    backend: &dyn LixBackend,
    version_id: &str,
    directory_path: &str,
    lookup_scope: FilesystemProjectionScope,
) -> Result<(), WriteResolveError> {
    let file_path = directory_path.trim_end_matches('/').to_string();
    if lookup_file_id_by_path(backend, version_id, &file_path, lookup_scope)
        .await?
        .is_none()
    {
        return Ok(());
    }
    Err(WriteResolveError {
        message: format!("Directory path collides with existing file path: {file_path}"),
    })
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

async fn load_directory_row_by_id(
    backend: &dyn LixBackend,
    version_id: &str,
    directory_id: &str,
    scope: FilesystemProjectionScope,
) -> Result<Option<DirectoryFilesystemRow>, WriteResolveError> {
    let sql = format!(
        "SELECT id, parent_id, name, path, hidden, lixcol_version_id, lixcol_untracked, lixcol_metadata, lixcol_change_id \
         FROM ({projection_sql}) directories \
         WHERE lixcol_version_id = '{version_id}' \
           AND id = '{directory_id}' \
         LIMIT 1",
        projection_sql = build_filesystem_directory_projection_sql(scope),
        version_id = escape_sql_string(version_id),
        directory_id = escape_sql_string(directory_id),
    );
    load_directory_row_from_sql(backend, &sql).await
}

async fn load_directory_row_by_path(
    backend: &dyn LixBackend,
    version_id: &str,
    path: &str,
    scope: FilesystemProjectionScope,
) -> Result<Option<DirectoryFilesystemRow>, WriteResolveError> {
    let sql = format!(
        "SELECT id, parent_id, name, path, hidden, lixcol_version_id, lixcol_untracked, lixcol_metadata, lixcol_change_id \
         FROM ({projection_sql}) directories \
         WHERE lixcol_version_id = '{version_id}' \
           AND path = '{path}' \
         LIMIT 1",
        projection_sql = build_filesystem_directory_projection_sql(scope),
        version_id = escape_sql_string(version_id),
        path = escape_sql_string(path),
    );
    load_directory_row_from_sql(backend, &sql).await
}

async fn load_directory_row_from_sql(
    backend: &dyn LixBackend,
    sql: &str,
) -> Result<Option<DirectoryFilesystemRow>, WriteResolveError> {
    Ok(load_directory_rows_from_sql(backend, sql)
        .await?
        .into_iter()
        .next())
}

async fn load_directory_rows_from_sql(
    backend: &dyn LixBackend,
    sql: &str,
) -> Result<Vec<DirectoryFilesystemRow>, WriteResolveError> {
    let lowered_sql = lower_internal_sql_for_backend(backend, sql)?;
    let result = backend
        .execute(&lowered_sql, &[])
        .await
        .map_err(write_resolve_backend_error)?;
    result
        .rows
        .iter()
        .map(|row| {
            Ok(DirectoryFilesystemRow {
                id: required_text_value(row, "id")?,
                parent_id: optional_text_value(row.get(1)),
                name: required_text_value_index(row, 2, "name")?,
                path: required_text_value_index(row, 3, "path")?,
                hidden: row.get(4).and_then(value_as_bool).unwrap_or(false),
                version_id: required_text_value_index(row, 5, "lixcol_version_id")?,
                untracked: row.get(6).and_then(value_as_bool).unwrap_or(false),
                metadata: row.get(7).and_then(text_from_value),
                change_id: row.get(8).and_then(text_from_value),
            })
        })
        .collect()
}

async fn load_file_row_by_path(
    backend: &dyn LixBackend,
    version_id: &str,
    path: &str,
    scope: FilesystemProjectionScope,
) -> Result<Option<FileFilesystemRow>, WriteResolveError> {
    let sql = format!(
        "SELECT id, directory_id, name, extension, path, hidden, lixcol_version_id, lixcol_untracked, metadata, lixcol_change_id \
         FROM ({projection_sql}) files \
         WHERE lixcol_version_id = '{version_id}' \
           AND path = '{path}' \
         LIMIT 1",
        projection_sql = build_filesystem_file_projection_sql(scope, false),
        version_id = escape_sql_string(version_id),
        path = escape_sql_string(path),
    );
    load_file_row_from_sql(backend, &sql).await
}

async fn load_file_row_by_id(
    backend: &dyn LixBackend,
    version_id: &str,
    file_id: &str,
    scope: FilesystemProjectionScope,
) -> Result<Option<FileFilesystemRow>, WriteResolveError> {
    let sql = format!(
        "SELECT id, directory_id, name, extension, path, hidden, lixcol_version_id, lixcol_untracked, metadata, lixcol_change_id \
         FROM ({projection_sql}) files \
         WHERE lixcol_version_id = '{version_id}' \
           AND id = '{file_id}' \
         LIMIT 1",
        projection_sql = build_filesystem_file_projection_sql(scope, false),
        version_id = escape_sql_string(version_id),
        file_id = escape_sql_string(file_id),
    );
    load_file_row_from_sql(backend, &sql).await
}

async fn load_file_row_from_sql(
    backend: &dyn LixBackend,
    sql: &str,
) -> Result<Option<FileFilesystemRow>, WriteResolveError> {
    Ok(load_file_rows_from_sql(backend, sql)
        .await?
        .into_iter()
        .next())
}

async fn load_file_rows_from_sql(
    backend: &dyn LixBackend,
    sql: &str,
) -> Result<Vec<FileFilesystemRow>, WriteResolveError> {
    let lowered_sql = lower_internal_sql_for_backend(backend, sql)?;
    let result = backend
        .execute(&lowered_sql, &[])
        .await
        .map_err(write_resolve_backend_error)?;
    result
        .rows
        .iter()
        .map(|row| {
            Ok(FileFilesystemRow {
                id: required_text_value(row, "id")?,
                directory_id: optional_text_value(row.get(1)),
                name: required_text_value_index(row, 2, "name")?,
                extension: optional_text_value(row.get(3)),
                path: required_text_value_index(row, 4, "path")?,
                hidden: row.get(5).and_then(value_as_bool).unwrap_or(false),
                version_id: required_text_value_index(row, 6, "lixcol_version_id")?,
                untracked: row.get(7).and_then(value_as_bool).unwrap_or(false),
                metadata: row.get(8).and_then(text_from_value),
                change_id: row.get(9).and_then(text_from_value),
            })
        })
        .collect()
}

async fn load_directory_rows_under_path(
    backend: &dyn LixBackend,
    version_id: &str,
    root_path: &str,
) -> Result<Vec<DirectoryFilesystemRow>, WriteResolveError> {
    let prefix_length = root_path.chars().count();
    let sql = format!(
        "SELECT id, parent_id, name, path, hidden, lixcol_version_id, lixcol_untracked, lixcol_metadata, lixcol_change_id \
         FROM ({projection_sql}) directories \
         WHERE lixcol_version_id = '{version_id}' \
           AND substr(path, 1, {prefix_length}) = '{root_path}' \
         ORDER BY path ASC, id ASC",
        projection_sql =
            build_filesystem_directory_projection_sql(FilesystemProjectionScope::ExplicitVersion),
        version_id = escape_sql_string(version_id),
        prefix_length = prefix_length,
        root_path = escape_sql_string(root_path),
    );
    load_directory_rows_from_sql(backend, &sql).await
}

async fn load_file_rows_under_path(
    backend: &dyn LixBackend,
    version_id: &str,
    root_path: &str,
) -> Result<Vec<FileFilesystemRow>, WriteResolveError> {
    let prefix_length = root_path.chars().count();
    let sql = format!(
        "SELECT id, directory_id, name, extension, path, hidden, lixcol_version_id, lixcol_untracked, metadata, lixcol_change_id \
         FROM ({projection_sql}) files \
         WHERE lixcol_version_id = '{version_id}' \
           AND substr(path, 1, {prefix_length}) = '{root_path}' \
         ORDER BY path ASC, id ASC",
        projection_sql =
            build_filesystem_file_projection_sql(FilesystemProjectionScope::ExplicitVersion, false),
        version_id = escape_sql_string(version_id),
        prefix_length = prefix_length,
        root_path = escape_sql_string(root_path),
    );
    load_file_rows_from_sql(backend, &sql).await
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
        "blob_hash": crate::plugin::runtime::binary_blob_hash_hex(data),
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
        tombstone: true,
    }
}

fn auto_directory_id(version_id: &str, path: &str) -> String {
    format!("lix-auto-dir:{}:{}", version_id, path)
}

fn auto_file_id(version_id: &str, path: &str) -> String {
    format!("lix-auto-file:{}:{}", version_id, path)
}

#[derive(Debug, Clone)]
struct VersionAdminRow {
    id: String,
    name: String,
    hidden: bool,
    commit_id: String,
    descriptor_change_id: Option<String>,
    pointer_change_id: Option<String>,
}

#[derive(Debug, Clone)]
struct ActiveVersionAdminRow {
    id: String,
    version_id: String,
}

#[derive(Debug, Clone)]
struct ActiveAccountAdminRow {
    account_id: String,
}

#[derive(Debug, Clone)]
struct DirectoryFilesystemRow {
    id: String,
    parent_id: Option<String>,
    name: String,
    path: String,
    hidden: bool,
    version_id: String,
    untracked: bool,
    metadata: Option<String>,
    change_id: Option<String>,
}

#[derive(Debug, Clone)]
struct FileFilesystemRow {
    id: String,
    directory_id: Option<String>,
    name: String,
    extension: Option<String>,
    path: String,
    hidden: bool,
    version_id: String,
    untracked: bool,
    metadata: Option<String>,
    change_id: Option<String>,
}

async fn resolve_admin_write(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    target_write_lane: Option<WriteLane>,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    match planned_write.command.target.descriptor.public_name.as_str() {
        "lix_active_version" => match planned_write.command.operation_kind {
            WriteOperationKind::Update => {
                resolve_active_version_update_write_plan(backend, planned_write, target_write_lane)
                    .await
            }
            _ => Err(WriteResolveError {
                message: "sql2 write resolver only supports UPDATE for 'lix_active_version'"
                    .to_string(),
            }),
        },
        "lix_active_account" => match planned_write.command.operation_kind {
            WriteOperationKind::Insert => {
                resolve_active_account_insert_write_plan(planned_write, target_write_lane)
            }
            WriteOperationKind::Delete => {
                resolve_active_account_delete_write_plan(backend, planned_write, target_write_lane)
                    .await
            }
            WriteOperationKind::Update => Err(WriteResolveError {
                message: "sql2 write resolver does not support UPDATE for 'lix_active_account'"
                    .to_string(),
            }),
        },
        "lix_version" => match planned_write.command.operation_kind {
            WriteOperationKind::Insert => {
                resolve_version_insert_write_plan(backend, planned_write, target_write_lane).await
            }
            WriteOperationKind::Update | WriteOperationKind::Delete => {
                resolve_existing_version_write(backend, planned_write, target_write_lane).await
            }
        },
        other => Err(WriteResolveError {
            message: format!(
                "sql2 write resolver does not yet support '{}' writes",
                other
            ),
        }),
    }
}

async fn resolve_active_version_update_write_plan(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    target_write_lane: Option<WriteLane>,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    ensure_exact_or_unfiltered_selector(planned_write)?;
    let MutationPayload::Patch(payload) = &planned_write.command.payload else {
        return Err(WriteResolveError {
            message: "sql2 active-version update resolver requires a patch payload".to_string(),
        });
    };
    if payload.contains_key("id") {
        return Err(WriteResolveError {
            message: "sql2 active-version update cannot modify id".to_string(),
        });
    }
    if payload.keys().any(|key| key != "version_id") {
        return Err(WriteResolveError {
            message: "sql2 active-version update only supports version_id assignments".to_string(),
        });
    }
    let next_version_id =
        payload_text_value(planned_write, "version_id").ok_or_else(|| WriteResolveError {
            message: "sql2 active-version update must set version_id".to_string(),
        })?;
    if next_version_id.is_empty() {
        return Err(WriteResolveError {
            message: "sql2 active-version update cannot set empty version_id".to_string(),
        });
    }
    let version_exists = load_version_admin_row(backend, &next_version_id)
        .await
        .map_err(write_resolve_backend_error)?
        .is_some();
    if !version_exists {
        return Err(WriteResolveError {
            message: format!(
                "Foreign key constraint violation: lix_active_version.version_id '{}' references missing lix_version_descriptor.id",
                next_version_id
            ),
        });
    }

    let current_rows = load_active_version_admin_rows(backend)
        .await
        .map_err(write_resolve_backend_error)?;
    let matching_rows = current_rows
        .into_iter()
        .filter(|row| {
            active_version_row_matches_exact_filters(
                row,
                &planned_write.command.selector.exact_filters,
            )
        })
        .collect::<Vec<_>>();
    if matching_rows.is_empty() {
        return Ok(ResolvedWritePlan {
            authoritative_pre_state: Vec::new(),
            intended_post_state: Vec::new(),
            tombstones: Vec::new(),
            lineage: Vec::new(),
            target_write_lane,
        });
    }

    let authoritative_pre_state = matching_rows
        .iter()
        .map(active_version_admin_pre_state_ref)
        .collect::<Vec<_>>();
    let intended_post_state = matching_rows
        .iter()
        .map(|row| active_version_admin_row(&row.id, &next_version_id))
        .collect::<Vec<_>>();
    let lineage = matching_rows
        .into_iter()
        .map(|row| RowLineage {
            entity_id: row.id,
            source_change_id: None,
            source_commit_id: None,
        })
        .collect::<Vec<_>>();

    Ok(ResolvedWritePlan {
        authoritative_pre_state,
        intended_post_state,
        tombstones: Vec::new(),
        lineage,
        target_write_lane,
    })
}

fn resolve_active_account_insert_write_plan(
    planned_write: &PlannedWrite,
    target_write_lane: Option<WriteLane>,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let MutationPayload::FullSnapshot(payload) = &planned_write.command.payload else {
        return Err(WriteResolveError {
            message: "sql2 active-account insert resolver requires a full payload".to_string(),
        });
    };
    if payload.keys().any(|key| key != "account_id") {
        return Err(WriteResolveError {
            message: "sql2 active-account insert only supports the account_id column".to_string(),
        });
    }
    let account_id =
        payload_text_value(planned_write, "account_id").ok_or_else(|| WriteResolveError {
            message: "sql2 active-account insert requires column 'account_id'".to_string(),
        })?;
    if account_id.is_empty() {
        return Err(WriteResolveError {
            message: "sql2 active-account insert requires non-empty account_id".to_string(),
        });
    }

    Ok(ResolvedWritePlan {
        authoritative_pre_state: Vec::new(),
        intended_post_state: vec![active_account_admin_row(&account_id)],
        tombstones: Vec::new(),
        lineage: vec![RowLineage {
            entity_id: account_id,
            source_change_id: None,
            source_commit_id: None,
        }],
        target_write_lane,
    })
}

async fn resolve_active_account_delete_write_plan(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    target_write_lane: Option<WriteLane>,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    ensure_exact_or_unfiltered_selector(planned_write)?;
    let current_rows = load_active_account_admin_rows(backend)
        .await
        .map_err(write_resolve_backend_error)?;
    let matching_rows = current_rows
        .into_iter()
        .filter(|row| {
            active_account_row_matches_exact_filters(
                row,
                &planned_write.command.selector.exact_filters,
            )
        })
        .collect::<Vec<_>>();
    if matching_rows.is_empty() {
        return Ok(ResolvedWritePlan {
            authoritative_pre_state: Vec::new(),
            intended_post_state: Vec::new(),
            tombstones: Vec::new(),
            lineage: Vec::new(),
            target_write_lane,
        });
    }

    let authoritative_pre_state = matching_rows
        .iter()
        .map(active_account_admin_pre_state_ref)
        .collect::<Vec<_>>();
    let intended_post_state = matching_rows
        .iter()
        .map(|row| active_account_admin_tombstone_row(&row.account_id))
        .collect::<Vec<_>>();
    let tombstones = matching_rows
        .iter()
        .map(active_account_admin_pre_state_ref)
        .collect::<Vec<_>>();
    let lineage = matching_rows
        .into_iter()
        .map(|row| RowLineage {
            entity_id: row.account_id,
            source_change_id: None,
            source_commit_id: None,
        })
        .collect::<Vec<_>>();

    Ok(ResolvedWritePlan {
        authoritative_pre_state,
        intended_post_state,
        tombstones,
        lineage,
        target_write_lane,
    })
}

async fn load_active_version_admin_rows(
    backend: &dyn LixBackend,
) -> Result<Vec<ActiveVersionAdminRow>, crate::LixError> {
    let sql = format!(
        "SELECT entity_id, snapshot_content \
         FROM lix_internal_state_untracked \
         WHERE schema_key = '{schema_key}' \
           AND file_id = '{file_id}' \
           AND version_id = '{storage_version_id}' \
           AND snapshot_content IS NOT NULL \
         ORDER BY updated_at DESC, entity_id ASC",
        schema_key = active_version_schema_key(),
        file_id = active_version_file_id(),
        storage_version_id = active_version_storage_version_id(),
    );
    let result = backend.execute(&sql, &[]).await?;
    let mut rows = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        let Some(id) = row.first().and_then(text_from_value) else {
            continue;
        };
        let Some(snapshot_content) = row.get(1).and_then(text_from_value) else {
            continue;
        };
        let version_id = parse_active_version_snapshot(&snapshot_content)?;
        rows.push(ActiveVersionAdminRow { id, version_id });
    }
    Ok(rows)
}

fn active_version_row_matches_exact_filters(
    row: &ActiveVersionAdminRow,
    exact_filters: &BTreeMap<String, Value>,
) -> bool {
    exact_filters.iter().all(|(key, value)| match key.as_str() {
        "id" => text_from_value(value).is_some_and(|expected| expected == row.id),
        "version_id" => text_from_value(value).is_some_and(|expected| expected == row.version_id),
        _ => false,
    })
}

fn active_version_admin_pre_state_ref(row: &ActiveVersionAdminRow) -> ResolvedRowRef {
    ResolvedRowRef {
        entity_id: row.id.clone(),
        schema_key: active_version_schema_key().to_string(),
        version_id: Some(active_version_storage_version_id().to_string()),
        source_change_id: None,
        source_commit_id: None,
    }
}

fn active_version_admin_row(id: &str, version_id: &str) -> PlannedStateRow {
    let mut values = BTreeMap::new();
    values.insert("entity_id".to_string(), Value::Text(id.to_string()));
    values.insert(
        "schema_key".to_string(),
        Value::Text(active_version_schema_key().to_string()),
    );
    values.insert(
        "file_id".to_string(),
        Value::Text(active_version_file_id().to_string()),
    );
    values.insert(
        "plugin_key".to_string(),
        Value::Text(active_version_plugin_key().to_string()),
    );
    values.insert(
        "schema_version".to_string(),
        Value::Text(active_version_schema_version().to_string()),
    );
    values.insert(
        "snapshot_content".to_string(),
        Value::Text(active_version_snapshot_content(id, version_id)),
    );
    values.insert(
        "version_id".to_string(),
        Value::Text(active_version_storage_version_id().to_string()),
    );
    values.insert("global".to_string(), Value::Boolean(true));
    PlannedStateRow {
        entity_id: id.to_string(),
        schema_key: active_version_schema_key().to_string(),
        version_id: Some(active_version_storage_version_id().to_string()),
        values,
        tombstone: false,
    }
}

async fn load_active_account_admin_rows(
    backend: &dyn LixBackend,
) -> Result<Vec<ActiveAccountAdminRow>, crate::LixError> {
    let sql = format!(
        "SELECT entity_id, snapshot_content \
         FROM lix_internal_state_untracked \
         WHERE schema_key = '{schema_key}' \
           AND file_id = '{file_id}' \
           AND version_id = '{storage_version_id}' \
           AND snapshot_content IS NOT NULL \
         ORDER BY updated_at DESC, entity_id ASC",
        schema_key = active_account_schema_key(),
        file_id = active_account_file_id(),
        storage_version_id = active_account_storage_version_id(),
    );
    let result = backend.execute(&sql, &[]).await?;
    let mut rows = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        let Some(snapshot_content) = row.get(1).and_then(text_from_value) else {
            continue;
        };
        let account_id = parse_active_account_snapshot(&snapshot_content)?;
        rows.push(ActiveAccountAdminRow { account_id });
    }
    Ok(rows)
}

fn active_account_row_matches_exact_filters(
    row: &ActiveAccountAdminRow,
    exact_filters: &BTreeMap<String, Value>,
) -> bool {
    exact_filters.iter().all(|(key, value)| match key.as_str() {
        "id" | "account_id" => {
            text_from_value(value).is_some_and(|expected| expected == row.account_id)
        }
        _ => false,
    })
}

fn active_account_admin_pre_state_ref(row: &ActiveAccountAdminRow) -> ResolvedRowRef {
    ResolvedRowRef {
        entity_id: row.account_id.clone(),
        schema_key: active_account_schema_key().to_string(),
        version_id: Some(active_account_storage_version_id().to_string()),
        source_change_id: None,
        source_commit_id: None,
    }
}

fn active_account_admin_row(account_id: &str) -> PlannedStateRow {
    let mut values = BTreeMap::new();
    values.insert("entity_id".to_string(), Value::Text(account_id.to_string()));
    values.insert(
        "schema_key".to_string(),
        Value::Text(active_account_schema_key().to_string()),
    );
    values.insert(
        "file_id".to_string(),
        Value::Text(active_account_file_id().to_string()),
    );
    values.insert(
        "plugin_key".to_string(),
        Value::Text(active_account_plugin_key().to_string()),
    );
    values.insert(
        "schema_version".to_string(),
        Value::Text(active_account_schema_version().to_string()),
    );
    values.insert(
        "snapshot_content".to_string(),
        Value::Text(active_account_snapshot_content(account_id)),
    );
    values.insert(
        "version_id".to_string(),
        Value::Text(active_account_storage_version_id().to_string()),
    );
    values.insert("global".to_string(), Value::Boolean(true));
    PlannedStateRow {
        entity_id: account_id.to_string(),
        schema_key: active_account_schema_key().to_string(),
        version_id: Some(active_account_storage_version_id().to_string()),
        values,
        tombstone: false,
    }
}

fn active_account_admin_tombstone_row(account_id: &str) -> PlannedStateRow {
    let mut row = active_account_admin_row(account_id);
    row.values.remove("snapshot_content");
    row.tombstone = true;
    row
}

async fn resolve_version_insert_write_plan(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    target_write_lane: Option<WriteLane>,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let version_id = version_admin_id_from_payload(planned_write)?;
    let name = version_admin_required_text(planned_write, "name")?;
    let commit_id = version_admin_required_text(planned_write, "commit_id")?;
    let hidden = version_admin_hidden_from_payload(planned_write)?;
    let existing = load_version_admin_row(backend, &version_id)
        .await
        .map_err(write_resolve_backend_error)?;

    let authoritative_pre_state = existing
        .as_ref()
        .map(version_admin_pre_state_refs)
        .unwrap_or_default();

    Ok(ResolvedWritePlan {
        authoritative_pre_state,
        intended_post_state: vec![
            version_descriptor_row(&version_id, &name, hidden),
            version_pointer_row(&version_id, &commit_id),
        ],
        tombstones: Vec::new(),
        lineage: vec![RowLineage {
            entity_id: version_id,
            source_change_id: None,
            source_commit_id: None,
        }],
        target_write_lane,
    })
}

async fn resolve_existing_version_write(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    target_write_lane: Option<WriteLane>,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    ensure_exact_selector(planned_write)?;
    let version_id = version_admin_target_id(planned_write)?;
    let Some(current_row) = load_version_admin_row(backend, &version_id)
        .await
        .map_err(write_resolve_backend_error)?
    else {
        return Ok(ResolvedWritePlan {
            authoritative_pre_state: Vec::new(),
            intended_post_state: Vec::new(),
            tombstones: Vec::new(),
            lineage: Vec::new(),
            target_write_lane,
        });
    };

    let authoritative_pre_state = version_admin_pre_state_refs(&current_row);
    let lineage = vec![RowLineage {
        entity_id: current_row.id.clone(),
        source_change_id: current_row
            .descriptor_change_id
            .clone()
            .or_else(|| current_row.pointer_change_id.clone()),
        source_commit_id: None,
    }];

    match planned_write.command.operation_kind {
        WriteOperationKind::Update => {
            let MutationPayload::Patch(payload) = &planned_write.command.payload else {
                return Err(WriteResolveError {
                    message: "sql2 version update resolver requires a patch payload".to_string(),
                });
            };
            if payload.contains_key("id") {
                return Err(WriteResolveError {
                    message: "sql2 version update cannot modify id".to_string(),
                });
            }

            let next_name = payload
                .get("name")
                .and_then(text_from_value)
                .unwrap_or_else(|| current_row.name.clone());
            if next_name.is_empty() {
                return Err(WriteResolveError {
                    message: "sql2 version update cannot set empty name".to_string(),
                });
            }
            let next_hidden = payload
                .get("hidden")
                .and_then(value_as_bool)
                .unwrap_or(current_row.hidden);
            let next_commit_id = payload
                .get("commit_id")
                .and_then(text_from_value)
                .unwrap_or_else(|| current_row.commit_id.clone());
            if next_commit_id.is_empty() {
                return Err(WriteResolveError {
                    message: "sql2 version update cannot set empty commit_id".to_string(),
                });
            }

            let mut intended_post_state = Vec::new();
            if payload.contains_key("name") || payload.contains_key("hidden") {
                intended_post_state.push(version_descriptor_row(
                    &current_row.id,
                    &next_name,
                    next_hidden,
                ));
            }
            if payload.contains_key("commit_id") {
                intended_post_state.push(version_pointer_row(&current_row.id, &next_commit_id));
            }

            Ok(ResolvedWritePlan {
                authoritative_pre_state,
                intended_post_state,
                tombstones: Vec::new(),
                lineage,
                target_write_lane,
            })
        }
        WriteOperationKind::Delete => {
            let tombstones = version_admin_tombstone_refs(&current_row);
            Ok(ResolvedWritePlan {
                authoritative_pre_state,
                intended_post_state: vec![
                    version_descriptor_tombstone_row(&current_row.id),
                    version_pointer_tombstone_row(&current_row.id),
                ],
                tombstones,
                lineage,
                target_write_lane,
            })
        }
        WriteOperationKind::Insert => Err(WriteResolveError {
            message: "sql2 version existing-row resolver does not handle inserts".to_string(),
        }),
    }
}

async fn load_version_admin_row(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<Option<VersionAdminRow>, crate::LixError> {
    let descriptor_sql = format!(
        "SELECT snapshot_content, change_id \
         FROM lix_internal_state_materialized_v1_lix_version_descriptor \
         WHERE schema_key = '{schema_key}' \
           AND entity_id = '{entity_id}' \
           AND file_id = '{file_id}' \
           AND plugin_key = '{plugin_key}' \
           AND version_id = '{storage_version_id}' \
           AND is_tombstone = 0 \
           AND snapshot_content IS NOT NULL \
         LIMIT 1",
        schema_key = version_descriptor_schema_key(),
        entity_id = version_id.replace('\'', "''"),
        file_id = version_descriptor_file_id(),
        plugin_key = version_descriptor_plugin_key(),
        storage_version_id = version_descriptor_storage_version_id(),
    );
    let descriptor_result = backend.execute(&descriptor_sql, &[]).await?;
    let Some(descriptor_row) = descriptor_result.rows.first() else {
        return Ok(None);
    };
    let pointer_sql = format!(
        "SELECT snapshot_content, change_id \
         FROM lix_internal_state_materialized_v1_lix_version_pointer \
         WHERE schema_key = '{schema_key}' \
           AND entity_id = '{entity_id}' \
           AND file_id = '{file_id}' \
           AND plugin_key = '{plugin_key}' \
           AND version_id = '{storage_version_id}' \
           AND is_tombstone = 0 \
           AND snapshot_content IS NOT NULL \
         LIMIT 1",
        schema_key = version_pointer_schema_key(),
        entity_id = version_id.replace('\'', "''"),
        file_id = version_pointer_file_id(),
        plugin_key = version_pointer_plugin_key(),
        storage_version_id = version_pointer_storage_version_id(),
    );
    let pointer_result = backend.execute(&pointer_sql, &[]).await?;
    let pointer_row = pointer_result.rows.first();
    Ok(Some(VersionAdminRow {
        id: version_id.to_string(),
        name: row_snapshot_name(descriptor_row).unwrap_or_default(),
        hidden: row_snapshot_hidden(descriptor_row).unwrap_or(false),
        commit_id: pointer_row
            .and_then(|row| row_snapshot_commit_id(row))
            .unwrap_or_default(),
        descriptor_change_id: descriptor_row.get(1).and_then(text_from_value),
        pointer_change_id: pointer_row
            .and_then(|row| row.get(1))
            .and_then(text_from_value),
    }))
}

fn version_admin_pre_state_refs(row: &VersionAdminRow) -> Vec<ResolvedRowRef> {
    vec![
        ResolvedRowRef {
            entity_id: row.id.clone(),
            schema_key: version_descriptor_schema_key().to_string(),
            version_id: Some(GLOBAL_VERSION_ID.to_string()),
            source_change_id: row.descriptor_change_id.clone(),
            source_commit_id: None,
        },
        ResolvedRowRef {
            entity_id: row.id.clone(),
            schema_key: version_pointer_schema_key().to_string(),
            version_id: Some(GLOBAL_VERSION_ID.to_string()),
            source_change_id: row.pointer_change_id.clone(),
            source_commit_id: None,
        },
    ]
}

fn version_admin_tombstone_refs(row: &VersionAdminRow) -> Vec<ResolvedRowRef> {
    version_admin_pre_state_refs(row)
}

fn version_admin_id_from_payload(
    planned_write: &PlannedWrite,
) -> Result<String, WriteResolveError> {
    payload_text_value(planned_write, "id").ok_or_else(|| WriteResolveError {
        message: "sql2 version insert requires column 'id'".to_string(),
    })
}

fn version_admin_target_id(planned_write: &PlannedWrite) -> Result<String, WriteResolveError> {
    if let Some(value) = planned_write
        .command
        .selector
        .exact_filters
        .get("id")
        .and_then(text_from_value)
    {
        return Ok(value);
    }
    payload_text_value(planned_write, "id").ok_or_else(|| WriteResolveError {
        message: "sql2 version write resolver requires an exact 'id' target".to_string(),
    })
}

fn version_admin_required_text(
    planned_write: &PlannedWrite,
    key: &str,
) -> Result<String, WriteResolveError> {
    let value = payload_text_value(planned_write, key).ok_or_else(|| WriteResolveError {
        message: format!("sql2 version insert requires column '{key}'"),
    })?;
    if value.is_empty() {
        return Err(WriteResolveError {
            message: format!("sql2 version insert cannot set empty {key}"),
        });
    }
    Ok(value)
}

fn version_admin_hidden_from_payload(
    planned_write: &PlannedWrite,
) -> Result<bool, WriteResolveError> {
    let (MutationPayload::FullSnapshot(payload) | MutationPayload::Patch(payload)) =
        &planned_write.command.payload
    else {
        return Ok(false);
    };
    Ok(payload
        .get("hidden")
        .and_then(value_as_bool)
        .unwrap_or(false))
}

fn version_descriptor_row(id: &str, name: &str, hidden: bool) -> PlannedStateRow {
    let mut values = BTreeMap::new();
    values.insert("entity_id".to_string(), Value::Text(id.to_string()));
    values.insert(
        "schema_key".to_string(),
        Value::Text(version_descriptor_schema_key().to_string()),
    );
    values.insert(
        "file_id".to_string(),
        Value::Text(version_descriptor_file_id().to_string()),
    );
    values.insert(
        "plugin_key".to_string(),
        Value::Text(version_descriptor_plugin_key().to_string()),
    );
    values.insert(
        "schema_version".to_string(),
        Value::Text(version_descriptor_schema_version().to_string()),
    );
    values.insert(
        "snapshot_content".to_string(),
        Value::Text(version_descriptor_snapshot_content(id, name, hidden)),
    );
    values.insert(
        "version_id".to_string(),
        Value::Text(GLOBAL_VERSION_ID.to_string()),
    );
    PlannedStateRow {
        entity_id: id.to_string(),
        schema_key: version_descriptor_schema_key().to_string(),
        version_id: Some(GLOBAL_VERSION_ID.to_string()),
        values,
        tombstone: false,
    }
}

fn version_pointer_row(id: &str, commit_id: &str) -> PlannedStateRow {
    let mut values = BTreeMap::new();
    values.insert("entity_id".to_string(), Value::Text(id.to_string()));
    values.insert(
        "schema_key".to_string(),
        Value::Text(version_pointer_schema_key().to_string()),
    );
    values.insert(
        "file_id".to_string(),
        Value::Text(version_pointer_file_id().to_string()),
    );
    values.insert(
        "plugin_key".to_string(),
        Value::Text(version_pointer_plugin_key().to_string()),
    );
    values.insert(
        "schema_version".to_string(),
        Value::Text(version_pointer_schema_version().to_string()),
    );
    values.insert(
        "snapshot_content".to_string(),
        Value::Text(version_pointer_snapshot_content(id, commit_id)),
    );
    values.insert(
        "version_id".to_string(),
        Value::Text(GLOBAL_VERSION_ID.to_string()),
    );
    PlannedStateRow {
        entity_id: id.to_string(),
        schema_key: version_pointer_schema_key().to_string(),
        version_id: Some(GLOBAL_VERSION_ID.to_string()),
        values,
        tombstone: false,
    }
}

fn version_descriptor_tombstone_row(id: &str) -> PlannedStateRow {
    let mut row = version_descriptor_row(id, id, false);
    row.values.remove("snapshot_content");
    row.tombstone = true;
    row
}

fn version_pointer_tombstone_row(id: &str) -> PlannedStateRow {
    let mut row = version_pointer_row(id, "deleted");
    row.values.remove("snapshot_content");
    row.tombstone = true;
    row
}

fn merged_update_values(
    current_values: &BTreeMap<String, Value>,
    planned_write: &PlannedWrite,
) -> Result<BTreeMap<String, Value>, WriteResolveError> {
    let MutationPayload::Patch(payload) = &planned_write.command.payload else {
        return Err(WriteResolveError {
            message: "sql2 update resolver requires a patch payload".to_string(),
        });
    };

    let mut values = current_values.clone();
    for (key, value) in payload {
        values.insert(key.clone(), value.clone());
    }
    Ok(values)
}

fn ensure_identity_columns_preserved(
    entity_id: &str,
    schema_key: &str,
    file_id: &str,
    version_id: &str,
    values: &BTreeMap<String, Value>,
) -> Result<(), WriteResolveError> {
    for (column, expected) in [
        ("entity_id", entity_id),
        ("schema_key", schema_key),
        ("file_id", file_id),
        ("version_id", version_id),
    ] {
        let Some(actual) = values.get(column).and_then(text_from_value) else {
            return Err(WriteResolveError {
                message: format!("sql2 update resolver requires '{column}' in authoritative row"),
            });
        };
        if actual != expected {
            return Err(WriteResolveError {
                message: format!("sql2 day-1 update resolver does not support changing '{column}'"),
            });
        }
    }

    Ok(())
}

fn ensure_exact_selector(planned_write: &PlannedWrite) -> Result<(), WriteResolveError> {
    if !planned_write.command.selector.exact_only {
        return Err(WriteResolveError {
            message: "sql2 day-1 update/delete resolver only supports exact conjunctive selectors"
                .to_string(),
        });
    }
    Ok(())
}

fn ensure_exact_or_unfiltered_selector(
    planned_write: &PlannedWrite,
) -> Result<(), WriteResolveError> {
    if planned_write
        .command
        .selector
        .residual_predicates
        .is_empty()
        || planned_write.command.selector.exact_only
    {
        return Ok(());
    }

    Err(WriteResolveError {
        message:
            "sql2 admin write resolver only supports exact conjunctive selectors or implicit singleton/all-row selection"
                .to_string(),
    })
}

fn ensure_local_tracked_overlay(
    current_row: &ExactEffectiveStateRow,
) -> Result<(), WriteResolveError> {
    if current_row.overlay_lane != OverlayLane::LocalTracked {
        return Err(WriteResolveError {
            message: format!(
                "sql2 live tracked writes do not yet support {:?} effective-state winners",
                current_row.overlay_lane
            ),
        });
    }
    Ok(())
}

fn directory_row_matches_exact_filters(
    row: &DirectoryFilesystemRow,
    exact_filters: &BTreeMap<String, Value>,
) -> bool {
    exact_filters.iter().all(|(key, value)| match key.as_str() {
        "id" => text_from_value(value).is_some_and(|expected| expected == row.id),
        "path" => text_from_value(value)
            .map(|expected| normalize_directory_path(&expected))
            .transpose()
            .ok()
            .flatten()
            .is_some_and(|expected| expected == row.path),
        "parent_id" => optional_text_matches(value, row.parent_id.as_deref()),
        "name" => text_from_value(value)
            .map(|expected| normalize_path_segment(&expected))
            .transpose()
            .ok()
            .flatten()
            .is_some_and(|expected| expected == row.name),
        "hidden" => value_as_bool(value).is_some_and(|expected| expected == row.hidden),
        "version_id" => text_from_value(value).is_some_and(|expected| expected == row.version_id),
        "untracked" => value_as_bool(value).is_some_and(|expected| expected == row.untracked),
        "metadata" => optional_text_matches(value, row.metadata.as_deref()),
        _ => false,
    })
}

fn file_row_matches_exact_filters(
    row: &FileFilesystemRow,
    exact_filters: &BTreeMap<String, Value>,
) -> bool {
    exact_filters.iter().all(|(key, value)| match key.as_str() {
        "id" => text_from_value(value).is_some_and(|expected| expected == row.id),
        "path" => text_from_value(value)
            .map(|expected| parse_file_path(&expected).map(|parsed| parsed.normalized_path))
            .transpose()
            .ok()
            .flatten()
            .is_some_and(|expected| expected == row.path),
        "hidden" => value_as_bool(value).is_some_and(|expected| expected == row.hidden),
        "version_id" => text_from_value(value).is_some_and(|expected| expected == row.version_id),
        "untracked" => value_as_bool(value).is_some_and(|expected| expected == row.untracked),
        "metadata" => optional_text_matches(value, row.metadata.as_deref()),
        _ => false,
    })
}

fn optional_text_matches(value: &Value, actual: Option<&str>) -> bool {
    match value {
        Value::Null => actual.is_none(),
        _ => text_from_value(value).is_some_and(|expected| Some(expected.as_str()) == actual),
    }
}

#[derive(Debug, Clone)]
struct EntityWriteSchema {
    schema_key: String,
    schema_version: String,
    property_columns: Vec<String>,
    primary_key_paths: Vec<Vec<String>>,
    state_defaults: BTreeMap<String, Value>,
}

async fn load_entity_schema(
    provider: &mut dyn SchemaProvider,
    planned_write: &PlannedWrite,
) -> Result<EntityWriteSchema, crate::LixError> {
    let schema_key = resolved_schema_key(planned_write).map_err(write_resolve_to_lix_error)?;
    let schema = if let Some(schema) = builtin_schema_definition(&schema_key) {
        schema.clone()
    } else {
        provider.load_latest_schema(&schema_key).await?
    };
    let schema_version = schema
        .get("x-lix-version")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| crate::LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("schema '{}' is missing string x-lix-version", schema_key),
        })?
        .to_string();
    let mut property_columns = schema
        .get("properties")
        .and_then(JsonValue::as_object)
        .map(|properties| {
            properties
                .keys()
                .filter(|key| !key.starts_with("lixcol_"))
                .cloned()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    property_columns.sort();
    property_columns.dedup();

    let primary_key_paths = schema
        .get("x-lix-primary-key")
        .and_then(JsonValue::as_array)
        .map(|entries| {
            entries
                .iter()
                .map(|entry| {
                    let pointer = entry.as_str().ok_or_else(|| crate::LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        description: format!(
                            "schema '{}' has non-string x-lix-primary-key entry",
                            schema_key
                        ),
                    })?;
                    parse_json_pointer(pointer)
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();

    let mut state_defaults = BTreeMap::new();
    state_defaults.insert(
        "schema_version".to_string(),
        Value::Text(schema_version.clone()),
    );
    if let Some(overrides) = schema
        .get("x-lix-override-lixcols")
        .and_then(JsonValue::as_object)
    {
        for (raw_key, expr) in overrides {
            let Some(expr) = expr.as_str() else {
                continue;
            };
            let Some(key) = entity_state_column_name(raw_key) else {
                continue;
            };
            state_defaults.insert(key.to_string(), parse_literal_override(expr)?);
        }
    }

    Ok(EntityWriteSchema {
        schema_key,
        schema_version,
        property_columns,
        primary_key_paths,
        state_defaults,
    })
}

fn reject_unsupported_entity_overrides(
    entity_schema: &EntityWriteSchema,
) -> Result<(), WriteResolveError> {
    if entity_schema
        .state_defaults
        .get("global")
        .and_then(value_as_bool)
        == Some(true)
    {
        return Err(WriteResolveError {
            message: "sql2 entity live slice does not yet support lixcol_global write overrides"
                .to_string(),
        });
    }
    if entity_schema
        .state_defaults
        .get("untracked")
        .and_then(value_as_bool)
        == Some(true)
    {
        return Err(WriteResolveError {
            message: "sql2 entity live slice does not yet support lixcol_untracked write overrides"
                .to_string(),
        });
    }
    Ok(())
}

fn build_entity_insert_row(
    planned_write: &PlannedWrite,
    entity_schema: &EntityWriteSchema,
) -> Result<PlannedStateRow, WriteResolveError> {
    let MutationPayload::FullSnapshot(payload) = &planned_write.command.payload else {
        return Err(WriteResolveError {
            message: "sql2 entity insert resolver requires a full snapshot payload".to_string(),
        });
    };
    let version_id = resolved_version_id(planned_write)?;
    let snapshot = snapshot_from_entity_payload(payload, entity_schema)?;
    let entity_id = payload
        .get("entity_id")
        .and_then(text_from_value)
        .map(|value| value.to_string())
        .or_else(|| {
            derive_entity_id_from_snapshot(&snapshot, &entity_schema.primary_key_paths).ok()
        })
        .ok_or_else(|| WriteResolveError {
            message: "sql2 entity insert resolver requires an exact primary-key-derived entity_id"
                .to_string(),
        })?;
    let file_id = resolved_entity_state_text(payload, entity_schema, "file_id")?;
    let plugin_key = resolved_entity_state_text(payload, entity_schema, "plugin_key")?;
    let schema_version = resolved_entity_state_text(payload, entity_schema, "schema_version")?;
    let mut values = BTreeMap::new();
    values.insert("entity_id".to_string(), Value::Text(entity_id.clone()));
    values.insert(
        "schema_key".to_string(),
        Value::Text(entity_schema.schema_key.clone()),
    );
    values.insert("file_id".to_string(), Value::Text(file_id));
    values.insert("plugin_key".to_string(), Value::Text(plugin_key));
    values.insert("schema_version".to_string(), Value::Text(schema_version));
    values.insert(
        "snapshot_content".to_string(),
        Value::Text(
            serde_json::to_string(&JsonValue::Object(snapshot)).map_err(|error| {
                WriteResolveError {
                    message: format!(
                        "sql2 entity insert resolver could not serialize snapshot: {error}"
                    ),
                }
            })?,
        ),
    );
    if let Some(version_id) = version_id.clone() {
        values.insert("version_id".to_string(), Value::Text(version_id));
    }
    if let Some(metadata) = resolved_entity_state_value(payload, entity_schema, "metadata") {
        if metadata != Value::Null {
            values.insert("metadata".to_string(), metadata);
        }
    }

    Ok(PlannedStateRow {
        entity_id,
        schema_key: entity_schema.schema_key.clone(),
        version_id,
        values,
        tombstone: false,
    })
}

fn resolved_entity_id_for_entity(
    planned_write: &PlannedWrite,
    entity_schema: &EntityWriteSchema,
) -> Result<String, WriteResolveError> {
    if let Some(entity_id) = planned_write
        .command
        .selector
        .exact_filters
        .get("entity_id")
        .and_then(text_from_value)
    {
        return Ok(entity_id.to_string());
    }
    if let Some(entity_id) = payload_text_value(planned_write, "entity_id") {
        return Ok(entity_id);
    }

    let snapshot = snapshot_from_exact_filters(
        &planned_write.command.selector.exact_filters,
        &entity_schema.property_columns,
    );
    derive_entity_id_from_snapshot(&snapshot, &entity_schema.primary_key_paths).map_err(|_| {
        WriteResolveError {
            message:
                "sql2 entity write resolver requires an exact selector over the entity primary key"
                    .to_string(),
        }
    })
}

fn entity_state_exact_filters(
    planned_write: &PlannedWrite,
    entity_schema: &EntityWriteSchema,
    entity_id: &str,
) -> Result<BTreeMap<String, Value>, WriteResolveError> {
    let mut filters = BTreeMap::new();
    filters.insert("entity_id".to_string(), Value::Text(entity_id.to_string()));
    for key in [
        "file_id",
        "plugin_key",
        "schema_version",
        "global",
        "untracked",
    ] {
        if let Some(value) = planned_write.command.selector.exact_filters.get(key) {
            filters.insert(key.to_string(), value.clone());
            continue;
        }
        if let Some(default) = entity_schema.state_defaults.get(key) {
            filters.insert(key.to_string(), default.clone());
        }
    }
    Ok(filters)
}

fn ensure_entity_selector_matches_current_row(
    entity_schema: &EntityWriteSchema,
    planned_write: &PlannedWrite,
    current_row: &ExactEffectiveStateRow,
) -> Result<(), WriteResolveError> {
    let snapshot = parse_snapshot_object(&current_row.values)?;
    for (key, value) in &planned_write.command.selector.exact_filters {
        if !entity_schema
            .property_columns
            .iter()
            .any(|column| column == key)
        {
            continue;
        }
        if !json_value_matches_engine_value(snapshot.get(key), value) {
            return Err(WriteResolveError {
                message: format!(
                    "sql2 entity live slice requires exact property filters to match the visible row for '{}'",
                    key
                ),
            });
        }
    }
    Ok(())
}

fn merged_entity_update_values(
    planned_write: &PlannedWrite,
    entity_schema: &EntityWriteSchema,
    current_row: &ExactEffectiveStateRow,
) -> Result<BTreeMap<String, Value>, WriteResolveError> {
    let MutationPayload::Patch(payload) = &planned_write.command.payload else {
        return Err(WriteResolveError {
            message: "sql2 entity update resolver requires a patch payload".to_string(),
        });
    };

    let mut snapshot = parse_snapshot_object(&current_row.values)?;
    let mut values = current_row.values.clone();
    for (key, value) in payload {
        if entity_schema
            .primary_key_paths
            .iter()
            .any(|path| path.len() == 1 && path[0] == *key)
        {
            return Err(WriteResolveError {
                message: "sql2 entity live slice does not yet support primary-key property updates"
                    .to_string(),
            });
        }
        if entity_schema
            .property_columns
            .iter()
            .any(|column| column == key)
        {
            snapshot.insert(key.clone(), engine_value_to_json_value(value)?);
            continue;
        }
        return Err(WriteResolveError {
            message: format!(
                "sql2 entity live slice does not yet support updating state column '{}'",
                key
            ),
        });
    }

    let expected_entity_id = derive_entity_id_from_snapshot(
        &snapshot,
        &entity_schema.primary_key_paths,
    )
    .map_err(|_| WriteResolveError {
        message: "sql2 entity update resolver requires a stable primary-key-derived entity_id"
            .to_string(),
    })?;
    if expected_entity_id != current_row.entity_id {
        return Err(WriteResolveError {
            message:
                "sql2 entity live slice does not yet support updates that change entity identity"
                    .to_string(),
        });
    }

    values.insert(
        "snapshot_content".to_string(),
        Value::Text(
            serde_json::to_string(&JsonValue::Object(snapshot)).map_err(|error| {
                WriteResolveError {
                    message: format!(
                        "sql2 entity update resolver could not serialize snapshot: {error}"
                    ),
                }
            })?,
        ),
    );
    ensure_identity_columns_preserved(
        &current_row.entity_id,
        &current_row.schema_key,
        &current_row.file_id,
        &current_row.version_id,
        &values,
    )?;
    Ok(values)
}

fn resolved_entity_id(planned_write: &PlannedWrite) -> Result<String, WriteResolveError> {
    if let Some(TargetSetProof::Exact(entity_ids)) = &planned_write.target_set_proof {
        if entity_ids.len() == 1 {
            return Ok(entity_ids
                .iter()
                .next()
                .expect("singleton exact target-set proof")
                .clone());
        }
    }

    payload_text_value(planned_write, "entity_id").ok_or_else(|| WriteResolveError {
        message: "sql2 day-1 write resolver requires an exact entity target".to_string(),
    })
}

fn resolved_schema_key(planned_write: &PlannedWrite) -> Result<String, WriteResolveError> {
    match &planned_write.schema_proof {
        SchemaProof::Exact(schema_keys) if schema_keys.len() == 1 => Ok(schema_keys
            .iter()
            .next()
            .expect("singleton exact schema proof")
            .clone()),
        _ => payload_text_value(planned_write, "schema_key").ok_or_else(|| WriteResolveError {
            message: "sql2 write resolver requires an exact schema proof or schema_key literal"
                .to_string(),
        }),
    }
}

fn resolved_version_id(planned_write: &PlannedWrite) -> Result<Option<String>, WriteResolveError> {
    match &planned_write.scope_proof {
        ScopeProof::ActiveVersion => planned_write
            .command
            .execution_context
            .requested_version_id
            .clone()
            .map(Some)
            .ok_or_else(|| WriteResolveError {
                message:
                    "sql2 write resolver requires requested_version_id for ActiveVersion writes"
                        .to_string(),
            }),
        ScopeProof::SingleVersion(version_id) => Ok(Some(version_id.clone())),
        ScopeProof::FiniteVersionSet(version_ids) if version_ids.len() == 1 => {
            Ok(version_ids.iter().next().cloned())
        }
        ScopeProof::FiniteVersionSet(_) => Err(WriteResolveError {
            message: "sql2 day-1 write resolver cannot resolve multi-version writes".to_string(),
        }),
        ScopeProof::GlobalAdmin => Ok(Some(GLOBAL_VERSION_ID.to_string())),
        ScopeProof::Unknown | ScopeProof::Unbounded => Err(WriteResolveError {
            message: "sql2 day-1 write resolver requires a bounded scope proof".to_string(),
        }),
    }
}

fn write_lane_from_scope(scope_proof: &ScopeProof) -> Result<WriteLane, WriteResolveError> {
    match scope_proof {
        ScopeProof::ActiveVersion => Ok(WriteLane::ActiveVersion),
        ScopeProof::SingleVersion(version_id) => Ok(WriteLane::SingleVersion(version_id.clone())),
        ScopeProof::FiniteVersionSet(version_ids) if version_ids.len() == 1 => {
            Ok(WriteLane::SingleVersion(
                version_ids
                    .iter()
                    .next()
                    .expect("singleton version set")
                    .clone(),
            ))
        }
        ScopeProof::FiniteVersionSet(_) => Err(WriteResolveError {
            message: "sql2 day-1 tracked writes require exactly one write lane".to_string(),
        }),
        ScopeProof::GlobalAdmin => Ok(WriteLane::GlobalAdmin),
        ScopeProof::Unknown | ScopeProof::Unbounded => Err(WriteResolveError {
            message: "sql2 day-1 tracked writes require a bounded write lane".to_string(),
        }),
    }
}

fn row_snapshot_name(row: &[Value]) -> Option<String> {
    row.first()
        .and_then(text_from_value)
        .and_then(|snapshot| serde_json::from_str::<JsonValue>(&snapshot).ok())
        .and_then(|snapshot| {
            snapshot
                .get("name")
                .and_then(JsonValue::as_str)
                .map(ToString::to_string)
        })
}

fn row_snapshot_hidden(row: &[Value]) -> Option<bool> {
    row.first()
        .and_then(text_from_value)
        .and_then(|snapshot| serde_json::from_str::<JsonValue>(&snapshot).ok())
        .and_then(|snapshot| snapshot.get("hidden").and_then(JsonValue::as_bool))
}

fn row_snapshot_commit_id(row: &[Value]) -> Option<String> {
    row.first()
        .and_then(text_from_value)
        .and_then(|snapshot| serde_json::from_str::<JsonValue>(&snapshot).ok())
        .and_then(|snapshot| {
            snapshot
                .get("commit_id")
                .and_then(JsonValue::as_str)
                .map(ToString::to_string)
        })
}

fn snapshot_from_entity_payload(
    payload: &BTreeMap<String, Value>,
    entity_schema: &EntityWriteSchema,
) -> Result<JsonMap<String, JsonValue>, WriteResolveError> {
    let mut snapshot = JsonMap::new();
    for key in &entity_schema.property_columns {
        if let Some(value) = payload.get(key) {
            snapshot.insert(key.clone(), engine_value_to_json_value(value)?);
        }
    }
    Ok(snapshot)
}

fn snapshot_from_exact_filters(
    exact_filters: &BTreeMap<String, Value>,
    property_columns: &[String],
) -> JsonMap<String, JsonValue> {
    let mut snapshot = JsonMap::new();
    for key in property_columns {
        if let Some(value) = exact_filters.get(key) {
            if let Ok(json_value) = engine_value_to_json_value(value) {
                snapshot.insert(key.clone(), json_value);
            }
        }
    }
    snapshot
}

fn parse_snapshot_object(
    values: &BTreeMap<String, Value>,
) -> Result<JsonMap<String, JsonValue>, WriteResolveError> {
    let Some(snapshot_text) = values.get("snapshot_content").and_then(text_from_value) else {
        return Err(WriteResolveError {
            message: "sql2 entity resolver requires snapshot_content in authoritative pre-state"
                .to_string(),
        });
    };
    let JsonValue::Object(object) =
        serde_json::from_str::<JsonValue>(&snapshot_text).map_err(|error| WriteResolveError {
            message: format!("sql2 entity resolver could not parse snapshot_content JSON: {error}"),
        })?
    else {
        return Err(WriteResolveError {
            message: "sql2 entity resolver requires object snapshot_content".to_string(),
        });
    };
    Ok(object)
}

fn derive_entity_id_from_snapshot(
    snapshot: &JsonMap<String, JsonValue>,
    primary_key_paths: &[Vec<String>],
) -> Result<String, WriteResolveError> {
    if primary_key_paths.is_empty() {
        return Err(WriteResolveError {
            message: "sql2 entity resolver requires x-lix-primary-key for entity writes"
                .to_string(),
        });
    }

    let snapshot = JsonValue::Object(snapshot.clone());
    let mut parts = Vec::with_capacity(primary_key_paths.len());
    for path in primary_key_paths {
        if path.is_empty() {
            return Err(WriteResolveError {
                message: "sql2 entity resolver does not support empty primary-key pointers"
                    .to_string(),
            });
        }
        let value = json_pointer_get(&snapshot, path).ok_or_else(|| WriteResolveError {
            message: "sql2 entity resolver could not derive entity_id from the primary-key fields"
                .to_string(),
        })?;
        parts.push(entity_id_component_from_json_value(value)?);
    }

    Ok(if parts.len() == 1 {
        parts.into_iter().next().expect("single primary key part")
    } else {
        parts.join("~")
    })
}

fn resolved_entity_state_text(
    payload: &BTreeMap<String, Value>,
    entity_schema: &EntityWriteSchema,
    key: &str,
) -> Result<String, WriteResolveError> {
    resolved_entity_state_value(payload, entity_schema, key)
        .and_then(|value| text_from_value(&value))
        .map(|value| value.to_string())
        .ok_or_else(|| WriteResolveError {
            message: format!(
                "sql2 entity resolver requires a concrete '{}' value or schema override",
                key
            ),
        })
}

fn resolved_entity_state_value(
    payload: &BTreeMap<String, Value>,
    entity_schema: &EntityWriteSchema,
    key: &str,
) -> Option<Value> {
    payload
        .get(key)
        .cloned()
        .or_else(|| entity_schema.state_defaults.get(key).cloned())
}

fn entity_state_column_name(column: &str) -> Option<&'static str> {
    match column.to_ascii_lowercase().as_str() {
        "lixcol_entity_id" => Some("entity_id"),
        "lixcol_schema_key" => Some("schema_key"),
        "lixcol_file_id" => Some("file_id"),
        "lixcol_version_id" => Some("version_id"),
        "lixcol_plugin_key" => Some("plugin_key"),
        "lixcol_schema_version" => Some("schema_version"),
        "lixcol_global" => Some("global"),
        "lixcol_writer_key" => Some("writer_key"),
        "lixcol_untracked" => Some("untracked"),
        "lixcol_metadata" => Some("metadata"),
        _ => None,
    }
}

fn parse_literal_override(expr: &str) -> Result<Value, crate::LixError> {
    let parsed: JsonValue = serde_json::from_str(expr).map_err(|error| crate::LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("sql2 entity resolver requires literal lixcol overrides: {error}"),
    })?;
    Ok(json_value_to_engine_value(&parsed))
}

fn engine_value_to_json_value(value: &Value) -> Result<JsonValue, WriteResolveError> {
    match value {
        Value::Null => Ok(JsonValue::Null),
        Value::Text(value) => Ok(JsonValue::String(value.clone())),
        Value::Boolean(value) => Ok(JsonValue::Bool(*value)),
        Value::Integer(value) => Ok(JsonValue::Number((*value).into())),
        Value::Real(value) => JsonNumber::from_f64(*value)
            .map(JsonValue::Number)
            .ok_or_else(|| WriteResolveError {
                message: "sql2 entity resolver cannot represent NaN/inf JSON numbers".to_string(),
            }),
        Value::Blob(_) => Err(WriteResolveError {
            message: "sql2 entity resolver does not support blob entity properties".to_string(),
        }),
    }
}

fn json_value_to_engine_value(value: &JsonValue) -> Value {
    match value {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(value) => Value::Boolean(*value),
        JsonValue::String(value) => Value::Text(value.clone()),
        JsonValue::Number(number) => number
            .as_i64()
            .map(Value::Integer)
            .or_else(|| number.as_f64().map(Value::Real))
            .unwrap_or_else(|| Value::Text(number.to_string())),
        JsonValue::Array(_) | JsonValue::Object(_) => Value::Text(value.to_string()),
    }
}

fn json_value_matches_engine_value(actual: Option<&JsonValue>, expected: &Value) -> bool {
    match (actual, expected) {
        (Some(JsonValue::Null), Value::Null) => true,
        (Some(JsonValue::Bool(actual)), Value::Boolean(expected)) => actual == expected,
        (Some(JsonValue::String(actual)), Value::Text(expected)) => actual == expected,
        (Some(JsonValue::Number(actual)), Value::Integer(expected)) => {
            actual.as_i64() == Some(*expected)
        }
        (Some(JsonValue::Number(actual)), Value::Real(expected)) => {
            actual.as_f64().is_some_and(|value| value == *expected)
        }
        (None, Value::Null) => true,
        _ => false,
    }
}

fn entity_id_component_from_json_value(value: &JsonValue) -> Result<String, WriteResolveError> {
    match value {
        JsonValue::Null => Err(WriteResolveError {
            message: "sql2 entity resolver cannot derive entity_id from null primary-key values"
                .to_string(),
        }),
        JsonValue::String(text) => Ok(text.clone()),
        JsonValue::Bool(flag) => Ok(flag.to_string()),
        JsonValue::Number(number) => Ok(number.to_string()),
        JsonValue::Array(_) | JsonValue::Object(_) => Ok(value.to_string()),
    }
}

fn parse_json_pointer(pointer: &str) -> Result<Vec<String>, crate::LixError> {
    if pointer.is_empty() {
        return Ok(Vec::new());
    }
    if !pointer.starts_with('/') {
        return Err(crate::LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("invalid JSON pointer '{pointer}'"),
        });
    }
    pointer[1..]
        .split('/')
        .map(decode_json_pointer_segment)
        .collect()
}

fn decode_json_pointer_segment(segment: &str) -> Result<String, crate::LixError> {
    let mut out = String::new();
    let mut chars = segment.chars();
    while let Some(ch) = chars.next() {
        if ch == '~' {
            match chars.next() {
                Some('0') => out.push('~'),
                Some('1') => out.push('/'),
                _ => {
                    return Err(crate::LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        description: format!("invalid JSON pointer segment '{segment}'"),
                    })
                }
            }
        } else {
            out.push(ch);
        }
    }
    Ok(out)
}

fn json_pointer_get<'a>(value: &'a JsonValue, pointer: &[String]) -> Option<&'a JsonValue> {
    let mut current = value;
    for segment in pointer {
        match current {
            JsonValue::Object(object) => current = object.get(segment)?,
            JsonValue::Array(array) => {
                let index = segment.parse::<usize>().ok()?;
                current = array.get(index)?;
            }
            _ => return None,
        }
    }
    Some(current)
}

fn required_text_value(row: &[Value], label: &str) -> Result<String, WriteResolveError> {
    required_text_value_index(row, 0, label)
}

fn required_text_value_index(
    row: &[Value],
    index: usize,
    label: &str,
) -> Result<String, WriteResolveError> {
    row.get(index)
        .and_then(text_from_value)
        .ok_or_else(|| WriteResolveError {
            message: format!("sql2 filesystem resolver expected text {}", label),
        })
}

fn optional_text_value(value: Option<&Value>) -> Option<String> {
    value.and_then(text_from_value)
}

fn value_as_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Boolean(value) => Some(*value),
        Value::Integer(value) => Some(*value != 0),
        Value::Text(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" => Some(true),
            "0" | "false" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn write_resolve_to_lix_error(error: WriteResolveError) -> crate::LixError {
    crate::LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: error.message,
    }
}

fn payload_map(planned_write: &PlannedWrite) -> Result<BTreeMap<String, Value>, WriteResolveError> {
    match &planned_write.command.payload {
        MutationPayload::FullSnapshot(payload) | MutationPayload::Patch(payload) => {
            Ok(payload.clone())
        }
        MutationPayload::Tombstone => Ok(Default::default()),
    }
}

fn payload_text_value(planned_write: &PlannedWrite, key: &str) -> Option<String> {
    let (MutationPayload::FullSnapshot(payload) | MutationPayload::Patch(payload)) =
        &planned_write.command.payload
    else {
        return None;
    };

    match payload.get(key) {
        Some(Value::Text(value)) => Some(value.clone()),
        _ => None,
    }
}

fn text_from_value(value: &Value) -> Option<String> {
    match value {
        Value::Text(value) => Some(value.clone()),
        Value::Integer(value) => Some(value.to_string()),
        Value::Boolean(value) => Some(value.to_string()),
        Value::Real(value) => Some(value.to_string()),
        _ => None,
    }
}

fn write_resolve_backend_error(error: crate::LixError) -> WriteResolveError {
    WriteResolveError {
        message: error.description,
    }
}

fn lower_internal_sql_for_backend(
    backend: &dyn LixBackend,
    sql: &str,
) -> Result<String, WriteResolveError> {
    let mut statements = parse_sql_statements(sql).map_err(write_resolve_backend_error)?;
    if statements.len() != 1 {
        return Err(WriteResolveError {
            message: "sql2 filesystem resolver expected a single helper statement".to_string(),
        });
    }
    let statement = statements.remove(0);
    let lowered =
        lower_statement(statement, backend.dialect()).map_err(write_resolve_backend_error)?;
    Ok(lowered.to_string())
}

#[cfg(test)]
mod tests {
    use super::resolve_write_plan;
    use crate::sql2::catalog::SurfaceRegistry;
    use crate::sql2::core::contracts::{BoundStatement, ExecutionContext};
    use crate::sql2::core::parser::parse_sql_script;
    use crate::sql2::planner::canonicalize::canonicalize_write;
    use crate::sql2::planner::ir::WriteLane;
    use crate::sql2::planner::semantics::proof_engine::prove_write;
    use crate::{LixBackend, LixError, QueryResult, SqlDialect, Value};
    use async_trait::async_trait;

    #[derive(Default)]
    struct FakeBackend {
        change_rows: Vec<Vec<Value>>,
        version_descriptor_rows: Vec<Vec<Value>>,
        version_pointer_rows: Vec<Vec<Value>>,
    }

    #[async_trait(?Send)]
    impl LixBackend for FakeBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("FROM lix_internal_change c")
                && sql.contains("c.schema_key = 'lix_version_descriptor'")
            {
                return Ok(QueryResult {
                    rows: self.version_descriptor_rows.clone(),
                    columns: vec!["snapshot_content".to_string(), "change_id".to_string()],
                });
            }
            if sql.contains("FROM lix_internal_change c")
                && sql.contains("c.schema_key = 'lix_version_pointer'")
                && !sql.contains("SELECT c.id, c.entity_id, c.schema_key, c.schema_version")
            {
                return Ok(QueryResult {
                    rows: self.version_pointer_rows.clone(),
                    columns: vec!["snapshot_content".to_string(), "change_id".to_string()],
                });
            }
            if sql.contains("SELECT c.id, c.entity_id, c.schema_key, c.schema_version, c.file_id, c.plugin_key, s.content AS snapshot_content, c.metadata, c.created_at")
                && sql.contains("FROM lix_internal_change c")
            {
                return Ok(QueryResult {
                    rows: self.change_rows.clone(),
                    columns: vec![
                        "id".to_string(),
                        "entity_id".to_string(),
                        "schema_key".to_string(),
                        "schema_version".to_string(),
                        "file_id".to_string(),
                        "plugin_key".to_string(),
                        "snapshot_content".to_string(),
                        "metadata".to_string(),
                        "created_at".to_string(),
                    ],
                });
            }

            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn begin_transaction(&self) -> Result<Box<dyn crate::LixTransaction + '_>, LixError> {
            Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "transactions are not needed in this test backend".to_string(),
            })
        }
    }

    fn extract_sql_string_filter(sql: &str, column: &str) -> Option<String> {
        let marker = format!("{column} = '");
        let start = sql.find(&marker)? + marker.len();
        let rest = &sql[start..];
        let end = rest.find('\'')?;
        Some(rest[..end].to_string())
    }

    fn build_committed_state_change_rows(
        entity_id: &str,
        version_id: &str,
        file_id: &str,
        plugin_key: &str,
        snapshot_content: &str,
        metadata: Option<&str>,
        change_id: &str,
        commit_id: &str,
    ) -> Vec<Vec<Value>> {
        let commit_snapshot = serde_json::json!({
            "id": commit_id,
            "change_set_id": format!("change-set-{commit_id}"),
            "change_ids": [change_id],
            "author_account_ids": [],
            "parent_commit_ids": [],
            "meta_change_ids": []
        })
        .to_string();
        let pointer_snapshot = serde_json::json!({
            "id": version_id,
            "commit_id": commit_id
        })
        .to_string();
        vec![
            vec![
                Value::Text(change_id.to_string()),
                Value::Text(entity_id.to_string()),
                Value::Text("lix_key_value".to_string()),
                Value::Text("1".to_string()),
                Value::Text(file_id.to_string()),
                Value::Text(plugin_key.to_string()),
                Value::Text(snapshot_content.to_string()),
                metadata
                    .map(|value| Value::Text(value.to_string()))
                    .unwrap_or(Value::Null),
                Value::Text("2026-03-06T18:00:00Z".to_string()),
            ],
            vec![
                Value::Text(format!("commit-change-{commit_id}")),
                Value::Text(commit_id.to_string()),
                Value::Text("lix_commit".to_string()),
                Value::Text("1".to_string()),
                Value::Text("lix".to_string()),
                Value::Text("lix".to_string()),
                Value::Text(commit_snapshot),
                Value::Null,
                Value::Text("2026-03-06T18:00:01Z".to_string()),
            ],
            vec![
                Value::Text(format!("pointer-change-{version_id}")),
                Value::Text(version_id.to_string()),
                Value::Text("lix_version_pointer".to_string()),
                Value::Text("1".to_string()),
                Value::Text(crate::version::version_pointer_file_id().to_string()),
                Value::Text(crate::version::version_pointer_plugin_key().to_string()),
                Value::Text(pointer_snapshot),
                Value::Null,
                Value::Text("2026-03-06T18:00:02Z".to_string()),
            ],
        ]
    }

    fn planned_write(
        sql: &str,
        requested_version_id: &str,
    ) -> crate::sql2::planner::ir::PlannedWrite {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let mut statements = parse_sql_script(sql).expect("SQL should parse");
        let statement = statements.pop().expect("single statement");
        let bound = BoundStatement::from_statement(
            statement,
            Vec::new(),
            ExecutionContext {
                requested_version_id: Some(requested_version_id.to_string()),
                ..ExecutionContext::default()
            },
        );
        let canonicalized =
            canonicalize_write(bound, &registry).expect("write should canonicalize");
        prove_write(&canonicalized).expect("proofs should succeed")
    }

    #[tokio::test]
    async fn resolves_active_version_insert_with_active_lane() {
        let backend = FakeBackend::default();
        let resolved = resolve_write_plan(
            &backend,
            &planned_write(
                "INSERT INTO lix_state (entity_id, schema_key, file_id, plugin_key, snapshot_content, schema_version) \
                 VALUES ('entity-1', 'lix_key_value', 'lix', 'lix', '{\"key\":\"hello\"}', '1')",
                "main",
            ),
        )
        .await
        .expect("write should resolve");

        assert_eq!(
            resolved.intended_post_state[0].version_id.as_deref(),
            Some("main")
        );
        assert_eq!(resolved.target_write_lane, Some(WriteLane::ActiveVersion));
    }

    #[tokio::test]
    async fn resolves_explicit_version_insert_with_single_version_lane() {
        let backend = FakeBackend::default();
        let resolved = resolve_write_plan(
            &backend,
            &planned_write(
                "INSERT INTO lix_state_by_version (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version) \
                 VALUES ('entity-1', 'lix_key_value', 'lix', 'version-a', 'lix', '{\"key\":\"hello\"}', '1')",
                "main",
            ),
        )
        .await
        .expect("write should resolve");

        assert_eq!(
            resolved.target_write_lane,
            Some(WriteLane::SingleVersion("version-a".to_string()))
        );
        assert_eq!(
            resolved.intended_post_state[0].version_id.as_deref(),
            Some("version-a")
        );
    }

    #[tokio::test]
    async fn resolves_lix_version_insert_with_global_admin_lane() {
        let backend = FakeBackend::default();
        let resolved = resolve_write_plan(
            &backend,
            &planned_write(
                "INSERT INTO lix_version (id, name, hidden, commit_id) \
                 VALUES ('version-a', 'Version A', false, 'commit-a')",
                "main",
            ),
        )
        .await
        .expect("version write should resolve");

        assert_eq!(resolved.target_write_lane, Some(WriteLane::GlobalAdmin));
        assert_eq!(resolved.intended_post_state.len(), 2);
        assert!(resolved
            .intended_post_state
            .iter()
            .any(|row| row.schema_key == crate::version::version_descriptor_schema_key()));
        assert!(resolved
            .intended_post_state
            .iter()
            .any(|row| row.schema_key == crate::version::version_pointer_schema_key()));
    }

    #[tokio::test]
    async fn resolves_update_from_authoritative_pre_state() {
        let backend = FakeBackend {
            change_rows: build_committed_state_change_rows(
                "entity-1",
                "version-a",
                "lix",
                "lix",
                "{\"value\":\"before\"}",
                Some("{\"m\":1}"),
                "change-1",
                "commit-1",
            ),
            ..FakeBackend::default()
        };
        let resolved = resolve_write_plan(
            &backend,
            &planned_write(
                "UPDATE lix_state_by_version \
                 SET snapshot_content = '{\"value\":\"after\"}' \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'entity-1' \
                   AND version_id = 'version-a'",
                "main",
            ),
        )
        .await
        .expect("write should resolve");

        assert_eq!(resolved.authoritative_pre_state.len(), 1);
        assert_eq!(
            resolved.intended_post_state[0]
                .values
                .get("file_id")
                .and_then(super::text_from_value)
                .as_deref(),
            Some("lix")
        );
        assert_eq!(
            resolved.intended_post_state[0]
                .values
                .get("snapshot_content")
                .and_then(super::text_from_value)
                .as_deref(),
            Some("{\"value\":\"after\"}")
        );
    }

    #[tokio::test]
    async fn resolves_delete_from_authoritative_pre_state() {
        let backend = FakeBackend {
            change_rows: build_committed_state_change_rows(
                "entity-1",
                "version-a",
                "lix",
                "lix",
                "{\"value\":\"before\"}",
                None,
                "change-1",
                "commit-1",
            ),
            ..FakeBackend::default()
        };
        let resolved = resolve_write_plan(
            &backend,
            &planned_write(
                "DELETE FROM lix_state_by_version \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'entity-1' \
                   AND version_id = 'version-a'",
                "main",
            ),
        )
        .await
        .expect("write should resolve");

        assert_eq!(resolved.authoritative_pre_state.len(), 1);
        assert_eq!(resolved.tombstones.len(), 1);
        assert!(resolved.intended_post_state[0].tombstone);
    }

    #[tokio::test]
    async fn leaves_noop_update_with_no_rows_to_append() {
        let backend = FakeBackend::default();
        let resolved = resolve_write_plan(
            &backend,
            &planned_write(
                "UPDATE lix_state_by_version \
                 SET snapshot_content = '{\"value\":\"after\"}' \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'entity-missing' \
                   AND version_id = 'version-a'",
                "main",
            ),
        )
        .await
        .expect("missing rows should resolve as a no-op");

        assert_eq!(
            resolved.target_write_lane,
            Some(WriteLane::SingleVersion("version-a".into()))
        );
        assert!(resolved.authoritative_pre_state.is_empty());
        assert!(resolved.intended_post_state.is_empty());
    }

    #[tokio::test]
    async fn rejects_update_that_changes_identity_columns() {
        let backend = FakeBackend {
            change_rows: build_committed_state_change_rows(
                "entity-1",
                "version-a",
                "lix",
                "lix",
                "{\"value\":\"before\"}",
                None,
                "change-1",
                "commit-1",
            ),
            ..FakeBackend::default()
        };
        let error = resolve_write_plan(
            &backend,
            &planned_write(
                "UPDATE lix_state_by_version \
                 SET file_id = 'other-file' \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'entity-1' \
                   AND version_id = 'version-a'",
                "main",
            ),
        )
        .await
        .expect_err("identity-changing update should stay off the sql2 live slice");

        assert!(error
            .message
            .contains("does not support changing 'file_id'"));
    }

    #[tokio::test]
    async fn exact_file_filter_prevents_mismatched_updates() {
        let backend = FakeBackend {
            change_rows: build_committed_state_change_rows(
                "entity-1",
                "version-a",
                "lix",
                "lix",
                "{\"value\":\"before\"}",
                None,
                "change-1",
                "commit-1",
            ),
            ..FakeBackend::default()
        };
        let resolved = resolve_write_plan(
            &backend,
            &planned_write(
                "UPDATE lix_state_by_version \
                 SET snapshot_content = '{\"value\":\"after\"}' \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'entity-1' \
                   AND file_id = 'other-file' \
                   AND version_id = 'version-a'",
                "main",
            ),
        )
        .await
        .expect("mismatched exact filters should resolve as a no-op");

        assert!(resolved.intended_post_state.is_empty());
    }

    #[tokio::test]
    async fn rejects_non_exact_or_selectors() {
        let backend = FakeBackend {
            change_rows: build_committed_state_change_rows(
                "entity-1",
                "version-a",
                "lix",
                "lix",
                "{\"value\":\"before\"}",
                None,
                "change-1",
                "commit-1",
            ),
            ..FakeBackend::default()
        };
        let error = resolve_write_plan(
            &backend,
            &planned_write(
                "UPDATE lix_state_by_version \
                 SET snapshot_content = '{\"value\":\"after\"}' \
                 WHERE schema_key = 'lix_key_value' \
                   AND (entity_id = 'entity-1' OR entity_id = 'entity-2') \
                   AND version_id = 'version-a'",
                "main",
            ),
        )
        .await
        .expect_err("unsupported selectors should stay off the live sql2 slice");

        assert!(error.message.contains("exact conjunctive selectors"));
    }

    #[tokio::test]
    async fn resolves_directory_delete_cascade_from_authoritative_pre_state() {
        #[derive(Default)]
        struct FilesystemDeleteBackend;

        #[async_trait(?Send)]
        impl LixBackend for FilesystemDeleteBackend {
            fn dialect(&self) -> SqlDialect {
                SqlDialect::Sqlite
            }

            async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
                let sql_lower = sql.to_ascii_lowercase();

                if sql_lower.contains("id = 'dir-root'") {
                    return Ok(QueryResult {
                        rows: vec![vec![
                            Value::Text("dir-root".to_string()),
                            Value::Null,
                            Value::Text("root".to_string()),
                            Value::Text("/root/".to_string()),
                            Value::Boolean(false),
                            Value::Text("version-a".to_string()),
                            Value::Boolean(false),
                            Value::Null,
                            Value::Text("change-dir-root".to_string()),
                        ]],
                        columns: vec![
                            "id".to_string(),
                            "parent_id".to_string(),
                            "name".to_string(),
                            "path".to_string(),
                            "hidden".to_string(),
                            "lixcol_version_id".to_string(),
                            "lixcol_untracked".to_string(),
                            "lixcol_metadata".to_string(),
                            "lixcol_change_id".to_string(),
                        ],
                    });
                }

                if sql_lower.contains("substr(path, 1, 6) = '/root/'") {
                    if sql_lower.contains("parent_id") {
                        return Ok(QueryResult {
                            rows: vec![
                                vec![
                                    Value::Text("dir-root".to_string()),
                                    Value::Null,
                                    Value::Text("root".to_string()),
                                    Value::Text("/root/".to_string()),
                                    Value::Boolean(false),
                                    Value::Text("version-a".to_string()),
                                    Value::Boolean(false),
                                    Value::Null,
                                    Value::Text("change-dir-root".to_string()),
                                ],
                                vec![
                                    Value::Text("dir-child".to_string()),
                                    Value::Text("dir-root".to_string()),
                                    Value::Text("child".to_string()),
                                    Value::Text("/root/child/".to_string()),
                                    Value::Boolean(false),
                                    Value::Text("version-a".to_string()),
                                    Value::Boolean(false),
                                    Value::Null,
                                    Value::Text("change-dir-child".to_string()),
                                ],
                            ],
                            columns: vec![
                                "id".to_string(),
                                "parent_id".to_string(),
                                "name".to_string(),
                                "path".to_string(),
                                "hidden".to_string(),
                                "lixcol_version_id".to_string(),
                                "lixcol_untracked".to_string(),
                                "lixcol_metadata".to_string(),
                                "lixcol_change_id".to_string(),
                            ],
                        });
                    }

                    if sql_lower.contains("directory_id") {
                        return Ok(QueryResult {
                            rows: vec![vec![
                                Value::Text("file-1".to_string()),
                                Value::Text("dir-child".to_string()),
                                Value::Text("note".to_string()),
                                Value::Text("txt".to_string()),
                                Value::Text("/root/child/note.txt".to_string()),
                                Value::Boolean(false),
                                Value::Text("version-a".to_string()),
                                Value::Boolean(false),
                                Value::Null,
                                Value::Text("change-file-1".to_string()),
                            ]],
                            columns: vec![
                                "id".to_string(),
                                "directory_id".to_string(),
                                "name".to_string(),
                                "extension".to_string(),
                                "path".to_string(),
                                "hidden".to_string(),
                                "lixcol_version_id".to_string(),
                                "lixcol_untracked".to_string(),
                                "metadata".to_string(),
                                "lixcol_change_id".to_string(),
                            ],
                        });
                    }
                }

                if sql.contains(
                    "SELECT id, parent_id, name, path, hidden, lixcol_version_id, lixcol_untracked, lixcol_metadata, lixcol_change_id",
                ) {
                    let rows = if sql_lower.contains("id = 'dir-root'") {
                        vec![vec![
                            Value::Text("dir-root".to_string()),
                            Value::Null,
                            Value::Text("root".to_string()),
                            Value::Text("/root/".to_string()),
                            Value::Boolean(false),
                            Value::Text("version-a".to_string()),
                            Value::Boolean(false),
                            Value::Null,
                            Value::Text("change-dir-root".to_string()),
                        ]]
                    } else if sql_lower.contains("substr(path, 1, 6) = '/root/'") {
                        vec![
                            vec![
                                Value::Text("dir-root".to_string()),
                                Value::Null,
                                Value::Text("root".to_string()),
                                Value::Text("/root/".to_string()),
                                Value::Boolean(false),
                                Value::Text("version-a".to_string()),
                                Value::Boolean(false),
                                Value::Null,
                                Value::Text("change-dir-root".to_string()),
                            ],
                            vec![
                                Value::Text("dir-child".to_string()),
                                Value::Text("dir-root".to_string()),
                                Value::Text("child".to_string()),
                                Value::Text("/root/child/".to_string()),
                                Value::Boolean(false),
                                Value::Text("version-a".to_string()),
                                Value::Boolean(false),
                                Value::Null,
                                Value::Text("change-dir-child".to_string()),
                            ],
                        ]
                    } else {
                        Vec::new()
                    };
                    return Ok(QueryResult {
                        rows,
                        columns: vec![
                            "id".to_string(),
                            "parent_id".to_string(),
                            "name".to_string(),
                            "path".to_string(),
                            "hidden".to_string(),
                            "lixcol_version_id".to_string(),
                            "lixcol_untracked".to_string(),
                            "lixcol_metadata".to_string(),
                            "lixcol_change_id".to_string(),
                        ],
                    });
                }

                if sql.contains(
                    "SELECT id, directory_id, name, extension, path, hidden, lixcol_version_id, lixcol_untracked, metadata, lixcol_change_id",
                ) {
                    let rows = if sql_lower.contains("substr(path, 1, 6) = '/root/'") {
                        vec![vec![
                            Value::Text("file-1".to_string()),
                            Value::Text("dir-child".to_string()),
                            Value::Text("note".to_string()),
                            Value::Text("txt".to_string()),
                            Value::Text("/root/child/note.txt".to_string()),
                            Value::Boolean(false),
                            Value::Text("version-a".to_string()),
                            Value::Boolean(false),
                            Value::Null,
                            Value::Text("change-file-1".to_string()),
                        ]]
                    } else {
                        Vec::new()
                    };
                    return Ok(QueryResult {
                        rows,
                        columns: vec![
                            "id".to_string(),
                            "directory_id".to_string(),
                            "name".to_string(),
                            "extension".to_string(),
                            "path".to_string(),
                            "hidden".to_string(),
                            "lixcol_version_id".to_string(),
                            "lixcol_untracked".to_string(),
                            "metadata".to_string(),
                            "lixcol_change_id".to_string(),
                        ],
                    });
                }

                Ok(QueryResult {
                    rows: Vec::new(),
                    columns: Vec::new(),
                })
            }

            async fn begin_transaction(
                &self,
            ) -> Result<Box<dyn crate::LixTransaction + '_>, LixError> {
                Err(LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: "transactions are not needed in this test backend".to_string(),
                })
            }
        }

        let backend = FilesystemDeleteBackend;
        let resolved = resolve_write_plan(
            &backend,
            &planned_write(
                "DELETE FROM lix_directory_by_version \
                 WHERE id = 'dir-root' \
                   AND lixcol_version_id = 'version-a'",
                "main",
            ),
        )
        .await
        .expect("directory delete should resolve through sql2");

        assert_eq!(
            resolved.target_write_lane,
            Some(WriteLane::SingleVersion("version-a".into()))
        );
        assert!(resolved.authoritative_pre_state.len() >= 4);
        assert!(resolved.intended_post_state.len() >= 4);
        assert!(resolved.tombstones.len() >= 4);
        assert!(resolved.intended_post_state.iter().all(|row| row.tombstone));
        assert!(resolved
            .intended_post_state
            .iter()
            .any(|row| row.schema_key == "lix_directory_descriptor"));
        assert!(resolved
            .intended_post_state
            .iter()
            .any(|row| row.schema_key == "lix_file_descriptor"));
        assert!(resolved
            .intended_post_state
            .iter()
            .any(|row| row.schema_key == "lix_binary_blob_ref"));
    }
}
