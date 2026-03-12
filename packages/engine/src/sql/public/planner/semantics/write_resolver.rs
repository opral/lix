use crate::account::{
    active_account_file_id, active_account_plugin_key, active_account_schema_key,
    active_account_schema_version, active_account_snapshot_content,
    active_account_storage_version_id, parse_active_account_snapshot,
};
use crate::filesystem::live_projection::{
    build_filesystem_directory_projection_sql, build_filesystem_file_projection_sql,
    FilesystemProjectionScope,
};
use crate::filesystem::path::{
    compose_directory_path, directory_ancestor_paths, directory_name_from_path,
    normalize_directory_path, normalize_path_segment, parent_directory_path, parse_file_path,
};
use crate::schema::builtin::builtin_schema_definition;
use crate::schema::defaults::apply_schema_defaults_with_system_functions;
use crate::schema::{SchemaProvider, SqlStoredSchemaProvider};
use crate::sql::ast::utils::bind_statement_ast;
use crate::sql::common::ast::{lower_statement, parse_sql_statements};
use crate::sql::public::catalog::SurfaceFamily;
use crate::sql::public::planner::ir::{
    InsertOnConflictAction, MutationPayload, PlannedStateRow, PlannedWrite, ResolvedRowRef,
    ResolvedWritePartition, ResolvedWritePlan, RowLineage, SchemaProof, ScopeProof,
    TargetSetProof, WriteLane, WriteMode, WriteModeRequest, WriteOperationKind,
};
use crate::sql::public::planner::semantics::effective_state_resolver::{
    resolve_exact_effective_state_row, ExactEffectiveStateRow, ExactEffectiveStateRowRequest,
    OverlayLane,
};
use crate::sql::public::runtime::execute_public_read_query_strict;
use crate::sql::storage::sql_text::escape_sql_string;
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
use crate::{LixBackend, LixError, QueryResult, Value};
use serde_json::{json, Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, Ident, ObjectName, ObjectNamePart, Query, Select,
    SelectFlavor, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
    Value as SqlValue, ValueWithSpan,
};
use std::collections::{BTreeMap, BTreeSet};

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

#[derive(Default, Clone)]
struct ResolvedWritePartitionBuilder {
    authoritative_pre_state: Vec<ResolvedRowRef>,
    intended_post_state: Vec<PlannedStateRow>,
    tombstones: Vec<ResolvedRowRef>,
    lineage: Vec<RowLineage>,
}

impl ResolvedWritePartitionBuilder {
    fn is_empty(&self) -> bool {
        self.authoritative_pre_state.is_empty()
            && self.intended_post_state.is_empty()
            && self.tombstones.is_empty()
            && self.lineage.is_empty()
    }

    fn into_partition(self, execution_mode: WriteMode) -> Option<ResolvedWritePartition> {
        (!self.is_empty()).then_some(ResolvedWritePartition {
            execution_mode,
            authoritative_pre_state: self.authoritative_pre_state,
            intended_post_state: self.intended_post_state,
            tombstones: self.tombstones,
            lineage: self.lineage,
            target_write_lane: None,
        })
    }
}

#[derive(Default, Clone)]
struct ResolvedWritePlanBuilder {
    partitions: Vec<ResolvedWritePlanEntry>,
}

#[derive(Clone)]
struct ResolvedWritePlanEntry {
    execution_mode: WriteMode,
    target_write_lane: Option<WriteLane>,
    partition: ResolvedWritePartitionBuilder,
}

impl ResolvedWritePlanBuilder {
    fn partition_mut(
        &mut self,
        execution_mode: WriteMode,
        target_write_lane: Option<WriteLane>,
    ) -> &mut ResolvedWritePartitionBuilder {
        if let Some(index) = self.partitions.iter().position(|entry| {
            entry.execution_mode == execution_mode && entry.target_write_lane == target_write_lane
        }) {
            return &mut self.partitions[index].partition;
        }
        self.partitions.push(ResolvedWritePlanEntry {
            execution_mode,
            target_write_lane,
            partition: ResolvedWritePartitionBuilder::default(),
        });
        &mut self
            .partitions
            .last_mut()
            .expect("partition entry was just pushed")
            .partition
    }

    fn into_resolved_write_plan(self, requested_mode: WriteModeRequest) -> ResolvedWritePlan {
        let mut partitions = Vec::new();
        for entry in self.partitions {
            if let Some(mut partition) = entry.partition.into_partition(entry.execution_mode) {
                partition.target_write_lane = entry.target_write_lane;
                partitions.push(partition);
            }
        }
        if partitions.is_empty() {
            return noop_resolved_write_plan(default_execution_mode_for_request(requested_mode));
        }
        ResolvedWritePlan::from_partitions(partitions)
    }
}

pub(crate) async fn resolve_write_plan(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let resolved = match planned_write.command.target.descriptor.surface_family {
        SurfaceFamily::State => match planned_write.command.operation_kind {
            WriteOperationKind::Insert => {
                resolve_state_insert_write_plan(backend, planned_write).await
            }
            WriteOperationKind::Update | WriteOperationKind::Delete => {
                resolve_existing_state_write(backend, planned_write).await
            }
        },
        SurfaceFamily::Entity => {
            let mut provider = SqlStoredSchemaProvider::new(backend);
            let entity_schema = load_entity_schema(&mut provider, planned_write)
                .await
                .map_err(write_resolve_backend_error)?;
            match planned_write.command.operation_kind {
                WriteOperationKind::Insert => {
                    resolve_entity_insert_write_plan(backend, planned_write, &entity_schema).await
                }
                WriteOperationKind::Update | WriteOperationKind::Delete => {
                    resolve_existing_entity_write(backend, planned_write, &entity_schema).await
                }
            }
        }
        SurfaceFamily::Admin => resolve_admin_write(backend, planned_write).await,
        SurfaceFamily::Filesystem => resolve_filesystem_write(backend, planned_write).await,
        SurfaceFamily::Change => Err(WriteResolveError {
            message: format!(
                "public write resolver does not support '{}' writes",
                planned_write.command.target.descriptor.public_name
            ),
        }),
    }?;

    finalize_resolved_write_plan(planned_write, resolved)
}

async fn resolve_state_insert_write_plan(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let entity_id = resolved_entity_id(planned_write)?;
    let schema_key = resolved_schema_key(planned_write)?;
    let version_id = resolved_version_id(planned_write)?;
    let row = PlannedStateRow {
        entity_id: entity_id.clone(),
        schema_key,
        version_id,
        values: payload_map(planned_write)?,
        tombstone: false,
    };

    if insert_on_conflict_action(planned_write) == Some(InsertOnConflictAction::DoNothing)
        && state_insert_conflicts_with_existing_row(backend, &row).await?
    {
        return Ok(noop_resolved_write_plan(
            default_execution_mode_for_request(planned_write.command.requested_mode),
        ));
    }

    Ok(single_partition_write_plan(
        default_execution_mode_for_request(planned_write.command.requested_mode),
        Vec::new(),
        vec![row],
        Vec::new(),
        vec![RowLineage {
            entity_id,
            source_change_id: None,
            source_commit_id: None,
        }],
    ))
}

async fn resolve_existing_state_write(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    if planned_write.command.selector.exact_only
        && state_selector_targets_single_effective_row(planned_write)
    {
        return resolve_existing_state_write_exact(backend, planned_write).await;
    }
    if !supports_selector_driven_state_resolution(planned_write) {
        return Err(WriteResolveError {
            message: "public update/delete resolver only supports exact conjunctive selectors"
                .to_string(),
        });
    }
    let current_rows = resolve_target_state_rows(backend, planned_write).await?;
    resolve_existing_state_write_from_rows(planned_write, current_rows)
}

fn state_selector_targets_single_effective_row(planned_write: &PlannedWrite) -> bool {
    let exact_filters = &planned_write.command.selector.exact_filters;
    let mut required_columns = vec![
        "entity_id",
        "file_id",
        "plugin_key",
        "schema_version",
        "global",
        "untracked",
    ];
    if state_selector_exposes_version_id(planned_write) {
        required_columns.push("version_id");
    }
    required_columns
        .into_iter()
        .all(|column| exact_filters.contains_key(column))
}

fn state_selector_exposes_version_id(planned_write: &PlannedWrite) -> bool {
    planned_write.command.target.implicit_overrides.expose_version_id
}

async fn resolve_existing_state_write_exact(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let schema_key = resolved_schema_key(planned_write)?;
    let version_id = resolved_version_id(planned_write)?.ok_or_else(|| WriteResolveError {
        message: "public existing-row write resolver requires a concrete version_id".to_string(),
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
        return Ok(noop_resolved_write_plan(
            default_execution_mode_for_request(planned_write.command.requested_mode),
        ));
    };
    resolve_existing_state_write_from_rows(planned_write, vec![current_row])
}

fn resolve_existing_state_write_from_rows(
    planned_write: &PlannedWrite,
    current_rows: Vec<ExactEffectiveStateRow>,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    if current_rows.is_empty() {
        return Ok(noop_resolved_write_plan(
            default_execution_mode_for_request(planned_write.command.requested_mode),
        ));
    }
    let mut partitions = ResolvedWritePlanBuilder::default();

    match planned_write.command.operation_kind {
        WriteOperationKind::Update => {
            for current_row in current_rows {
                let execution_mode = resolve_execution_mode_for_effective_row(
                    planned_write.command.requested_mode,
                    &current_row,
                )?;
                let values = merged_update_values(&current_row.values, planned_write)?;
                ensure_identity_columns_preserved(
                    &current_row.entity_id,
                    &current_row.schema_key,
                    &current_row.file_id,
                    &current_row.version_id,
                    &values,
                )?;
                let row_ref = ResolvedRowRef {
                    entity_id: current_row.entity_id.clone(),
                    schema_key: current_row.schema_key.clone(),
                    version_id: Some(current_row.version_id.clone()),
                    source_change_id: current_row.source_change_id.clone(),
                    source_commit_id: None,
                };
                let target_write_lane = target_write_lane_for_effective_row(
                    planned_write,
                    execution_mode,
                    &current_row,
                )?;
                let partition = partitions.partition_mut(execution_mode, target_write_lane);
                partition.authoritative_pre_state.push(row_ref.clone());
                partition.intended_post_state.push(PlannedStateRow {
                    entity_id: current_row.entity_id.clone(),
                    schema_key: current_row.schema_key.clone(),
                    version_id: Some(current_row.version_id.clone()),
                    values,
                    tombstone: false,
                });
                partition.lineage.push(RowLineage {
                    entity_id: current_row.entity_id,
                    source_change_id: row_ref.source_change_id,
                    source_commit_id: None,
                });
            }
            Ok(partitions.into_resolved_write_plan(
                planned_write.command.requested_mode,
            ))
        }
        WriteOperationKind::Delete => {
            for current_row in current_rows {
                let execution_mode = resolve_execution_mode_for_effective_row(
                    planned_write.command.requested_mode,
                    &current_row,
                )?;
                let row_ref = ResolvedRowRef {
                    entity_id: current_row.entity_id.clone(),
                    schema_key: current_row.schema_key.clone(),
                    version_id: Some(current_row.version_id.clone()),
                    source_change_id: current_row.source_change_id.clone(),
                    source_commit_id: None,
                };
                let target_write_lane = target_write_lane_for_effective_row(
                    planned_write,
                    execution_mode,
                    &current_row,
                )?;
                let partition = partitions.partition_mut(execution_mode, target_write_lane);
                partition.authoritative_pre_state.push(row_ref.clone());
                partition.intended_post_state.push(PlannedStateRow {
                    entity_id: current_row.entity_id.clone(),
                    schema_key: current_row.schema_key.clone(),
                    version_id: Some(current_row.version_id.clone()),
                    values: current_row.values,
                    tombstone: true,
                });
                partition.tombstones.push(row_ref.clone());
                partition.lineage.push(RowLineage {
                    entity_id: current_row.entity_id,
                    source_change_id: row_ref.source_change_id,
                    source_commit_id: None,
                });
            }
            Ok(partitions.into_resolved_write_plan(
                planned_write.command.requested_mode,
            ))
        }
        WriteOperationKind::Insert => Err(WriteResolveError {
            message: "public existing-row resolver does not handle inserts".to_string(),
        }),
    }
}

async fn resolve_existing_entity_write(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    entity_schema: &EntityWriteSchema,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    reject_unsupported_entity_overrides(planned_write, entity_schema)?;
    let current_rows = resolve_target_entity_rows(backend, planned_write, entity_schema).await?;
    if current_rows.is_empty() {
        return Ok(noop_resolved_write_plan(
            default_execution_mode_for_request(planned_write.command.requested_mode),
        ));
    }
    let mut partitions = ResolvedWritePlanBuilder::default();

    match planned_write.command.operation_kind {
        WriteOperationKind::Update => {
            for current_row in current_rows {
                let execution_mode = resolve_execution_mode_for_effective_row(
                    planned_write.command.requested_mode,
                    &current_row,
                )?;
                let row_ref = ResolvedRowRef {
                    entity_id: current_row.entity_id.clone(),
                    schema_key: current_row.schema_key.clone(),
                    version_id: Some(current_row.version_id.clone()),
                    source_change_id: current_row.source_change_id.clone(),
                    source_commit_id: None,
                };
                let values =
                    merged_entity_update_values(planned_write, entity_schema, &current_row)?;
                let target_write_lane = target_write_lane_for_effective_row(
                    planned_write,
                    execution_mode,
                    &current_row,
                )?;
                let partition = partitions.partition_mut(execution_mode, target_write_lane);
                partition.authoritative_pre_state.push(row_ref.clone());
                partition.intended_post_state.push(PlannedStateRow {
                    entity_id: current_row.entity_id.clone(),
                    schema_key: current_row.schema_key.clone(),
                    version_id: Some(current_row.version_id.clone()),
                    values,
                    tombstone: false,
                });
                partition.lineage.push(RowLineage {
                    entity_id: current_row.entity_id,
                    source_change_id: row_ref.source_change_id,
                    source_commit_id: None,
                });
            }
            Ok(partitions.into_resolved_write_plan(
                planned_write.command.requested_mode,
            ))
        }
        WriteOperationKind::Delete => {
            for current_row in current_rows {
                let execution_mode = resolve_execution_mode_for_effective_row(
                    planned_write.command.requested_mode,
                    &current_row,
                )?;
                let row_ref = ResolvedRowRef {
                    entity_id: current_row.entity_id.clone(),
                    schema_key: current_row.schema_key.clone(),
                    version_id: Some(current_row.version_id.clone()),
                    source_change_id: current_row.source_change_id.clone(),
                    source_commit_id: None,
                };
                let target_write_lane = target_write_lane_for_effective_row(
                    planned_write,
                    execution_mode,
                    &current_row,
                )?;
                let partition = partitions.partition_mut(execution_mode, target_write_lane);
                partition.authoritative_pre_state.push(row_ref.clone());
                partition.intended_post_state.push(PlannedStateRow {
                    entity_id: current_row.entity_id.clone(),
                    schema_key: current_row.schema_key.clone(),
                    version_id: Some(current_row.version_id.clone()),
                    values: current_row.values,
                    tombstone: true,
                });
                partition.tombstones.push(row_ref.clone());
                partition.lineage.push(RowLineage {
                    entity_id: current_row.entity_id,
                    source_change_id: row_ref.source_change_id,
                    source_commit_id: None,
                });
            }
            Ok(partitions.into_resolved_write_plan(
                planned_write.command.requested_mode,
            ))
        }
        WriteOperationKind::Insert => Err(WriteResolveError {
            message: "public entity existing-row resolver does not handle inserts".to_string(),
        }),
    }
}

async fn resolve_entity_insert_write_plan(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    entity_schema: &EntityWriteSchema,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    reject_unsupported_entity_overrides(planned_write, entity_schema)?;
    let rows = build_entity_insert_rows(planned_write, entity_schema)?;
    let mut partitions = ResolvedWritePlanBuilder::default();
    let default_execution_mode =
        default_execution_mode_for_request(planned_write.command.requested_mode);

    for row in rows {
        if let Some(conflict) = planned_write.command.on_conflict.as_ref() {
            let exact_filters = entity_insert_exact_filters(entity_schema, &row)?;
            if let Some(current_row) = resolve_exact_effective_state_row(
                backend,
                &ExactEffectiveStateRowRequest {
                    schema_key: entity_schema.schema_key.clone(),
                    version_id: row.version_id.clone().ok_or_else(|| WriteResolveError {
                        message: "public entity insert resolver requires a concrete version_id"
                            .to_string(),
                    })?,
                    exact_filters,
                    include_global_overlay: true,
                    include_untracked_overlay: true,
                },
            )
            .await
            .map_err(write_resolve_backend_error)?
            {
                if conflict.action == InsertOnConflictAction::DoNothing {
                    continue;
                }
                let row_execution_mode = resolve_execution_mode_for_effective_row(
                    planned_write.command.requested_mode,
                    &current_row,
                )?;
                let row_ref = ResolvedRowRef {
                    entity_id: current_row.entity_id.clone(),
                    schema_key: current_row.schema_key.clone(),
                    version_id: Some(current_row.version_id.clone()),
                    source_change_id: current_row.source_change_id.clone(),
                    source_commit_id: None,
                };
                let target_write_lane = target_write_lane_for_effective_row(
                    planned_write,
                    row_execution_mode,
                    &current_row,
                )?;
                let partition =
                    partitions.partition_mut(row_execution_mode, target_write_lane);
                partition.authoritative_pre_state.push(row_ref.clone());
                partition.lineage.push(RowLineage {
                    entity_id: row_ref.entity_id,
                    source_change_id: row_ref.source_change_id,
                    source_commit_id: None,
                });
                partition.intended_post_state.push(row);
                continue;
            }
        }
        let target_write_lane = target_write_lane_for_planned_row(
            planned_write,
            default_execution_mode,
            row.version_id.as_deref(),
        )?;
        let partition = partitions.partition_mut(default_execution_mode, target_write_lane);
        partition.lineage.push(RowLineage {
            entity_id: row.entity_id.clone(),
            source_change_id: None,
            source_commit_id: None,
        });
        partition.intended_post_state.push(row);
    }

    Ok(partitions.into_resolved_write_plan(
        planned_write.command.requested_mode,
    ))
}

async fn resolve_filesystem_write(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    match planned_write.command.target.descriptor.public_name.as_str() {
        "lix_file" | "lix_file_by_version" => match planned_write.command.operation_kind {
            WriteOperationKind::Insert => {
                resolve_file_insert_write_plan(backend, planned_write).await
            }
            WriteOperationKind::Update | WriteOperationKind::Delete => {
                resolve_existing_file_write(backend, planned_write).await
            }
        },
        "lix_directory" | "lix_directory_by_version" => {
            match planned_write.command.operation_kind {
                WriteOperationKind::Insert => {
                    resolve_directory_insert_write_plan(backend, planned_write).await
                }
                WriteOperationKind::Update | WriteOperationKind::Delete => {
                    resolve_existing_directory_write(backend, planned_write).await
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
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<String, WriteResolveError> {
    match planned_write.command.target.descriptor.public_name.as_str() {
        "lix_file" | "lix_directory" => load_active_filesystem_version_id(backend).await,
        _ => resolved_version_id(planned_write)?.ok_or_else(|| WriteResolveError {
            message: "public filesystem write requires a concrete version_id".to_string(),
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
            message: "public filesystem write requires an active version".to_string(),
        });
    };
    let Some(snapshot_content) = row.first().and_then(text_from_value) else {
        return Err(WriteResolveError {
            message: "public filesystem active-version lookup expected snapshot_content text"
                .to_string(),
        });
    };
    parse_active_version_snapshot(&snapshot_content).map_err(write_resolve_backend_error)
}

async fn resolve_directory_insert_write_plan(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let version_id = resolved_filesystem_version_id(backend, planned_write).await?;
    let lookup_scope = filesystem_write_lookup_scope(planned_write);
    let mut batch = PendingFilesystemInsertBatch::default();
    for payload in payload_maps(planned_write)? {
        let computed = resolve_directory_insert_target(
            backend,
            planned_write,
            &payload,
            &version_id,
            lookup_scope,
            &mut batch,
        )
        .await?;
        batch.register_directory_target(computed)?;
    }
    let (intended_post_state, lineage) =
        finalize_pending_directory_insert_batch(backend, &batch, &version_id, lookup_scope)
            .await?;

    Ok(single_partition_write_plan(
        default_execution_mode_for_request(planned_write.command.requested_mode),
        Vec::new(),
        intended_post_state,
        Vec::new(),
        lineage,
    ))
}

async fn resolve_existing_directory_write(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let version_id = resolved_filesystem_version_id(backend, planned_write).await?;
    let lookup_scope = filesystem_write_lookup_scope(planned_write);
    let current_rows =
        load_target_directory_rows_for_selector(backend, planned_write, &version_id, lookup_scope)
            .await?;
    if current_rows.is_empty() {
        return Ok(noop_resolved_write_plan(
            default_execution_mode_for_request(planned_write.command.requested_mode),
        ));
    }
    match planned_write.command.operation_kind {
        WriteOperationKind::Update => {
            let MutationPayload::Patch(payload) = &planned_write.command.payload else {
                return Err(WriteResolveError {
                    message: "public filesystem directory update requires a patch payload"
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

            let mut partitions = ResolvedWritePlanBuilder::default();
            let next_rows = if current_rows.len() > 1 && directory_update_changes_structure(payload)
            {
                resolve_directory_update_targets_batch(
                    backend,
                    &current_rows,
                    payload,
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
                            payload,
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

            Ok(partitions.into_resolved_write_plan(
                planned_write.command.requested_mode,
            ))
        }
        WriteOperationKind::Delete => {
            let mut descendant_directories = BTreeMap::new();
            let mut descendant_files = BTreeMap::new();
            for current_row in current_rows {
                for row in load_directory_rows_under_path(backend, &version_id, &current_row.path)
                    .await?
                {
                    descendant_directories.entry(row.id.clone()).or_insert(row);
                }
                for row in load_file_rows_under_path(backend, &version_id, &current_row.path).await?
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
                partition.intended_post_state.push(directory_descriptor_tombstone_row(
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
                partition.intended_post_state.push(file_descriptor_tombstone_row(
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
                partition.lineage.push(RowLineage {
                    entity_id: row.id.clone(),
                    source_change_id: row.change_id.clone(),
                    source_commit_id: None,
                });
            }

            Ok(partitions.into_resolved_write_plan(
                planned_write.command.requested_mode,
            ))
        }
        WriteOperationKind::Insert => Err(WriteResolveError {
            message: "public filesystem directory existing-row resolver does not handle inserts"
                .to_string(),
        }),
    }
}

async fn resolve_file_insert_write_plan(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let version_id = resolved_filesystem_version_id(backend, planned_write).await?;
    let lookup_scope = filesystem_write_lookup_scope(planned_write);
    let mut batch = PendingFilesystemInsertBatch::default();
    for payload in payload_maps(planned_write)? {
        let computed = resolve_file_insert_target(
            backend,
            planned_write,
            &payload,
            &version_id,
            lookup_scope,
            &mut batch,
        )
        .await?;
        let payload_bytes = payload_binary_value(&payload, "data")?;
        batch.register_file_target(computed, payload_bytes)?;
    }
    let (intended_post_state, lineage) =
        finalize_pending_file_insert_batch(backend, &batch, &version_id, lookup_scope).await?;

    Ok(single_partition_write_plan(
        default_execution_mode_for_request(planned_write.command.requested_mode),
        Vec::new(),
        intended_post_state,
        Vec::new(),
        lineage,
    ))
}

async fn resolve_existing_file_write(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let version_id = resolved_filesystem_version_id(backend, planned_write).await?;
    let lookup_scope = filesystem_write_lookup_scope(planned_write);
    let current_rows =
        load_target_file_rows_for_selector(backend, planned_write, &version_id, lookup_scope)
            .await?;
    if current_rows.is_empty() {
        return Ok(noop_resolved_write_plan(
            default_execution_mode_for_request(planned_write.command.requested_mode),
        ));
    }
    match planned_write.command.operation_kind {
        WriteOperationKind::Update => {
            let MutationPayload::Patch(payload) = &planned_write.command.payload else {
                return Err(WriteResolveError {
                    message: "public filesystem file update requires a patch payload".to_string(),
                });
            };
            if payload.contains_key("id") {
                return Err(WriteResolveError {
                    message:
                        "lix_file id is immutable; create a new row and delete the old row instead"
                            .to_string(),
                });
            }
            if current_rows.len() > 1 && payload.contains_key("path") {
                let next_path =
                    payload_text_required(payload, "path", "public filesystem file update")?;
                let normalized_path = parse_file_path(&next_path)
                    .map_err(write_resolve_backend_error)?
                    .normalized_path;
                return Err(WriteResolveError {
                    message: format!(
                        "Unique constraint violation: file path '{}' would be assigned to multiple rows",
                        normalized_path
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
                }

                if let Some(bytes) = payload_bytes {
                    partition.intended_post_state.push(binary_blob_ref_row(
                        &current_row.id,
                        &version_id,
                        &bytes,
                    )?);
                }
            }

            Ok(partitions.into_resolved_write_plan(
                planned_write.command.requested_mode,
            ))
        }
        WriteOperationKind::Delete => {
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
                partition.intended_post_state.push(file_descriptor_tombstone_row(
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
                partition.lineage.push(RowLineage {
                    entity_id: current_row.id,
                    source_change_id: current_row.change_id,
                    source_commit_id: None,
                });
            }
            Ok(partitions.into_resolved_write_plan(
                planned_write.command.requested_mode,
            ))
        }
        WriteOperationKind::Insert => Err(WriteResolveError {
            message: "public filesystem existing-row resolver does not handle inserts".to_string(),
        }),
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

impl PendingFilesystemInsertBatch {
    fn pending_directory_id_by_path(&self, path: &str) -> Option<String> {
        self.directories_by_path
            .get(path)
            .map(|pending| pending.target.id.clone())
    }

    fn pending_directory_path_by_id(&self, directory_id: &str) -> Option<String> {
        self.directories_by_path
            .values()
            .find_map(|pending| (pending.target.id == directory_id).then(|| pending.target.path.clone()))
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
    ) -> Result<(), WriteResolveError> {
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
    ) -> Result<(), WriteResolveError> {
        let file_collision_path = target.path.trim_end_matches('/').to_string();
        if self.files_by_path.contains_key(&file_collision_path) {
            return Err(WriteResolveError {
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
                    return Err(WriteResolveError {
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
    ) -> Result<(), WriteResolveError> {
        let directory_collision_path = format!("{}/", target.path.trim_end_matches('/'));
        if self.directories_by_path.contains_key(&directory_collision_path) {
            return Err(WriteResolveError {
                message: format!(
                    "File path collides with directory path already inserted in this statement: {}",
                    directory_collision_path
                ),
            });
        }
        if self.files_by_path.contains_key(&target.path) {
            return Err(WriteResolveError {
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

    fn ensure_unique_directory_ids(&self) -> Result<(), WriteResolveError> {
        let mut ids = BTreeMap::<String, String>::new();
        for pending in self.directories_by_path.values() {
            if let Some(existing_path) =
                ids.insert(pending.target.id.clone(), pending.target.path.clone())
            {
                if existing_path != pending.target.path {
                    return Err(WriteResolveError {
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

    fn ensure_unique_file_ids(&self) -> Result<(), WriteResolveError> {
        let mut ids = BTreeMap::<String, String>::new();
        for pending in self.files_by_path.values() {
            if let Some(existing_path) =
                ids.insert(pending.target.id.clone(), pending.target.path.clone())
            {
                if existing_path != pending.target.path {
                    return Err(WriteResolveError {
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

async fn resolve_file_insert_target(
    backend: &dyn LixBackend,
    _planned_write: &PlannedWrite,
    payload: &BTreeMap<String, Value>,
    version_id: &str,
    lookup_scope: FilesystemProjectionScope,
    batch: &mut PendingFilesystemInsertBatch,
) -> Result<ResolvedFileInsertTarget, WriteResolveError> {
    if !payload
        .keys()
        .any(|key| !matches!(key.as_str(), "data" | "version_id" | "untracked"))
    {
        return Err(WriteResolveError {
            message: "file insert requires at least one non-data column".to_string(),
        });
    }
    let explicit_path = payload_text_required(payload, "path", "public filesystem file insert")?;
    let parsed = parse_file_path(&explicit_path).map_err(write_resolve_backend_error)?;
    let explicit_id = payload.get("id").and_then(text_from_value);
    let hidden = payload
        .get("hidden")
        .and_then(value_as_bool)
        .unwrap_or(false);
    let metadata = payload_optional_text(payload, "metadata")?;
    ensure_no_directory_at_file_path_in_insert_batch(
        backend,
        version_id,
        &parsed.normalized_path,
        lookup_scope,
        batch,
    )
    .await?;
    let directory_path = ensure_parent_directories_for_insert_batch(
        backend,
        version_id,
        parsed.directory_path.as_deref(),
        lookup_scope,
        batch,
    )
    .await?;

    if let Some(existing_id) = batch.pending_file_id_by_path(&parsed.normalized_path) {
        if explicit_id.as_deref() != Some(existing_id.as_str()) {
            return Err(WriteResolveError {
                message: format!(
                    "Unique constraint violation: file path '{}' already exists in this INSERT",
                    parsed.normalized_path
                ),
            });
        }
    } else if let Some(existing_id) =
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
        path: parsed.normalized_path,
        directory_path,
        name: parsed.name,
        extension: parsed.extension,
        hidden,
        metadata,
    })
}

async fn ensure_parent_directories_for_insert_batch(
    backend: &dyn LixBackend,
    version_id: &str,
    directory_path: Option<&str>,
    lookup_scope: FilesystemProjectionScope,
    batch: &mut PendingFilesystemInsertBatch,
) -> Result<Option<String>, WriteResolveError> {
    let Some(directory_path) = directory_path else {
        return Ok(None);
    };

    let mut paths = directory_ancestor_paths(directory_path);
    paths.push(directory_path.to_string());

    for candidate_path in paths {
        if batch.pending_directory_id_by_path(&candidate_path).is_some() {
            continue;
        }
        if lookup_directory_id_by_path(backend, version_id, &candidate_path, lookup_scope)
            .await?
            .is_some()
        {
            continue;
        }
        ensure_no_file_at_directory_path_in_insert_batch(
            backend,
            version_id,
            &candidate_path,
            lookup_scope,
            batch,
        )
        .await?;
        batch.register_implicit_directory(version_id, &candidate_path)?;
    }

    Ok(Some(directory_path.to_string()))
}

async fn lookup_directory_id_by_path_in_insert_batch(
    backend: &dyn LixBackend,
    version_id: &str,
    path: &str,
    lookup_scope: FilesystemProjectionScope,
    batch: &PendingFilesystemInsertBatch,
) -> Result<Option<String>, WriteResolveError> {
    if let Some(directory_id) = batch.pending_directory_id_by_path(path) {
        return Ok(Some(directory_id));
    }
    lookup_directory_id_by_path(backend, version_id, path, lookup_scope).await
}

async fn lookup_directory_path_by_id_in_insert_batch(
    backend: &dyn LixBackend,
    version_id: &str,
    directory_id: &str,
    lookup_scope: FilesystemProjectionScope,
    batch: &PendingFilesystemInsertBatch,
) -> Result<Option<String>, WriteResolveError> {
    if let Some(path) = batch.pending_directory_path_by_id(directory_id) {
        return Ok(Some(path));
    }
    lookup_directory_path_by_id(backend, version_id, directory_id, lookup_scope).await
}

async fn ensure_no_file_at_directory_path_in_insert_batch(
    backend: &dyn LixBackend,
    version_id: &str,
    directory_path: &str,
    lookup_scope: FilesystemProjectionScope,
    batch: &PendingFilesystemInsertBatch,
) -> Result<(), WriteResolveError> {
    let file_path = directory_path.trim_end_matches('/').to_string();
    if batch.pending_file_id_by_path(&file_path).is_some() {
        return Err(WriteResolveError {
            message: format!("Directory path collides with existing file path: {file_path}"),
        });
    }
    ensure_no_file_at_directory_path(backend, version_id, directory_path, lookup_scope).await
}

async fn ensure_no_directory_at_file_path_in_insert_batch(
    backend: &dyn LixBackend,
    version_id: &str,
    file_path: &str,
    lookup_scope: FilesystemProjectionScope,
    batch: &PendingFilesystemInsertBatch,
) -> Result<(), WriteResolveError> {
    let directory_path = format!("{}/", file_path.trim_end_matches('/'));
    if batch.pending_directory_id_by_path(&directory_path).is_some() {
        return Err(WriteResolveError {
            message: format!("File path collides with existing directory path: {directory_path}"),
        });
    }
    ensure_no_directory_at_file_path(backend, version_id, file_path, lookup_scope).await
}

fn pending_directory_insert_sort_key(path: &str) -> (usize, String) {
    (path.matches('/').count(), path.to_string())
}

async fn finalize_pending_directory_insert_batch(
    backend: &dyn LixBackend,
    batch: &PendingFilesystemInsertBatch,
    version_id: &str,
    lookup_scope: FilesystemProjectionScope,
) -> Result<(Vec<PlannedStateRow>, Vec<RowLineage>), WriteResolveError> {
    let mut pending_directories: Vec<_> = batch.directories_by_path.values().cloned().collect();
    pending_directories.sort_by_key(|pending| pending_directory_insert_sort_key(&pending.target.path));

    let mut intended_post_state = Vec::new();
    let mut lineage = Vec::new();

    for pending in pending_directories {
        let parent_id = match pending.target.parent_path.as_deref() {
            Some(parent_path) => lookup_directory_id_by_path_in_insert_batch(
                backend,
                version_id,
                parent_path,
                lookup_scope,
                batch,
            )
            .await?,
            None => None,
        };
        intended_post_state.push(directory_descriptor_row(
            &pending.target.id,
            parent_id.as_deref(),
            &pending.target.name,
            pending.target.hidden,
            version_id,
            pending.target.metadata.as_deref(),
        ));
        lineage.push(RowLineage {
            entity_id: pending.target.id,
            source_change_id: None,
            source_commit_id: None,
        });
    }

    Ok((intended_post_state, lineage))
}

async fn finalize_pending_file_insert_batch(
    backend: &dyn LixBackend,
    batch: &PendingFilesystemInsertBatch,
    version_id: &str,
    lookup_scope: FilesystemProjectionScope,
) -> Result<(Vec<PlannedStateRow>, Vec<RowLineage>), WriteResolveError> {
    let (mut intended_post_state, mut lineage) =
        finalize_pending_directory_insert_batch(backend, batch, version_id, lookup_scope).await?;
    let mut pending_files: Vec<_> = batch.files_by_path.values().cloned().collect();
    pending_files.sort_by_key(|pending| pending.target.path.clone());

    for pending in pending_files {
        let directory_id = match pending.target.directory_path.as_deref() {
            Some(directory_path) => lookup_directory_id_by_path_in_insert_batch(
                backend,
                version_id,
                directory_path,
                lookup_scope,
                batch,
            )
            .await?,
            None => None,
        };
        intended_post_state.push(file_descriptor_row(
            &pending.target.id,
            directory_id.as_deref(),
            &pending.target.name,
            pending.target.extension.as_deref(),
            pending.target.hidden,
            version_id,
            pending.target.metadata.as_deref(),
        ));
        if let Some(bytes) = pending.data.as_deref() {
            intended_post_state.push(binary_blob_ref_row(&pending.target.id, version_id, bytes)?);
        }
        lineage.push(RowLineage {
            entity_id: pending.target.id,
            source_change_id: None,
            source_commit_id: None,
        });
    }

    Ok((intended_post_state, lineage))
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
            message: format!("public filesystem resolver expected text/null {key}, got {other:?}"),
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
            message: format!("public filesystem resolver expected blob {key}, got {other:?}"),
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
    path: String,
    parent_path: Option<String>,
    name: String,
    hidden: bool,
    metadata: Option<String>,
}

async fn resolve_directory_insert_target(
    backend: &dyn LixBackend,
    _planned_write: &PlannedWrite,
    payload: &BTreeMap<String, Value>,
    version_id: &str,
    lookup_scope: FilesystemProjectionScope,
    batch: &mut PendingFilesystemInsertBatch,
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

    let (parent_path, name, normalized_path) = if let Some(raw_path) = explicit_path {
        let normalized_path =
            normalize_directory_path(&raw_path).map_err(write_resolve_backend_error)?;
        let derived_name =
            directory_name_from_path(&normalized_path).ok_or_else(|| WriteResolveError {
                message: "Directory name must be provided".to_string(),
            })?;
        let derived_parent_path =
            ensure_parent_directories_for_insert_batch(
                backend,
                version_id,
                parent_directory_path(&normalized_path).as_deref(),
                lookup_scope,
                batch,
            )
            .await?;
        let derived_parent_id = match derived_parent_path.as_deref() {
            Some(parent_path) => {
                lookup_directory_id_by_path_in_insert_batch(
                    backend,
                    version_id,
                    parent_path,
                    lookup_scope,
                    batch,
                )
                    .await?
                    .or_else(|| Some(auto_directory_id(version_id, parent_path)))
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
        (derived_parent_path, derived_name, normalized_path)
    } else {
        let raw_name = explicit_name.ok_or_else(|| WriteResolveError {
            message: "Directory name must be provided".to_string(),
        })?;
        let name = normalize_path_segment(&raw_name).map_err(write_resolve_backend_error)?;
        let parent_path = match explicit_parent_id.as_deref() {
            Some(parent_id) => {
                lookup_directory_path_by_id_in_insert_batch(
                    backend,
                    version_id,
                    parent_id,
                    lookup_scope,
                    batch,
                )
                    .await?
                    .ok_or_else(|| WriteResolveError {
                        message: format!("Parent directory does not exist for id {parent_id}"),
                    })?
            }
            None => "/".to_string(),
        };
        let computed_path = compose_directory_path(parent_path.as_str(), &name)
            .map_err(write_resolve_backend_error)?;
        (explicit_parent_id.as_deref().map(|_| parent_path), name, computed_path)
    };

    if let Some(existing_id) = batch.pending_directory_id_by_path(&normalized_path) {
        if batch.directory_is_explicit(&normalized_path)
            && explicit_id.as_deref() != Some(existing_id.as_str())
        {
            return Err(WriteResolveError {
                message: format!(
                    "Unique constraint violation: directory path '{}' already exists in this INSERT",
                    normalized_path
                ),
            });
        }
    } else if let Some(existing_id) =
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
    ensure_no_file_at_directory_path_in_insert_batch(
        backend,
        version_id,
        &normalized_path,
        lookup_scope,
        batch,
    )
    .await?;

    let id = explicit_id.unwrap_or_else(|| auto_directory_id(version_id, &normalized_path));
    Ok(ResolvedDirectoryInsertTarget {
        id,
        path: normalized_path,
        parent_path,
        name,
        hidden,
        metadata,
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
    payload: &BTreeMap<String, Value>,
    version_id: &str,
    lookup_scope: FilesystemProjectionScope,
) -> Result<Vec<DirectoryFilesystemRow>, WriteResolveError> {
    if let Some(raw_path) = payload.get("path").and_then(text_from_value) {
        let normalized_path = normalize_directory_path(&raw_path).map_err(write_resolve_backend_error)?;
        return Err(WriteResolveError {
            message: format!(
                "Unique constraint violation: directory path '{}' would be assigned to multiple rows",
                normalized_path
            ),
        });
    }

    let mut proposed_by_id = BTreeMap::new();
    for row in current_rows {
        let parent_id = payload
            .get("parent_id")
            .and_then(text_from_value)
            .or_else(|| row.parent_id.clone());
        if parent_id.as_deref() == Some(row.id.as_str()) {
            return Err(WriteResolveError {
                message: "Directory cannot be its own parent".to_string(),
            });
        }
        let name = match payload.get("name").and_then(text_from_value) {
            Some(raw_name) => normalize_path_segment(&raw_name).map_err(write_resolve_backend_error)?,
            None => row.name.clone(),
        };
        let hidden = payload
            .get("hidden")
            .and_then(value_as_bool)
            .unwrap_or(row.hidden);
        let metadata = if payload.contains_key("metadata") {
            payload_optional_text(payload, "metadata")?
        } else {
            row.metadata.clone()
        };
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
        assert_no_directory_cycle(backend, version_id, &proposal.id, parent_id, lookup_scope).await?;
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
        ensure_no_file_at_directory_path(backend, version_id, &path, lookup_scope).await?;
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
        if let Some(existing_id) =
            lookup_directory_id_by_path(backend, version_id, &path, lookup_scope).await?
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
        Some(parent_id) if proposed_by_id.contains_key(parent_id) => resolve_proposed_directory_path(
            parent_id,
            proposed_by_id,
            external_parent_paths,
            resolved_paths,
        )?,
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
    planned_write: &PlannedWrite,
    version_id: &str,
    lookup_scope: FilesystemProjectionScope,
) -> Result<Vec<DirectoryFilesystemRow>, WriteResolveError> {
    let directory_ids = query_text_selector_values_for_write_selector(
        backend,
        planned_write,
        "id",
        "public filesystem directory selector resolver expected id text rows",
    )
    .await?;
    let mut rows = Vec::new();
    for directory_id in directory_ids {
        if let Some(row) = load_directory_row_by_id(backend, version_id, &directory_id, lookup_scope)
            .await?
        {
            rows.push(row);
        }
    }
    Ok(rows)
}

async fn load_target_file_rows_for_selector(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    version_id: &str,
    lookup_scope: FilesystemProjectionScope,
) -> Result<Vec<FileFilesystemRow>, WriteResolveError> {
    let file_ids = query_text_selector_values_for_write_selector(
        backend,
        planned_write,
        "id",
        "public filesystem file selector resolver expected id text rows",
    )
    .await?;
    let mut rows = Vec::new();
    for file_id in file_ids {
        if let Some(row) = load_file_row_by_id(backend, version_id, &file_id, lookup_scope).await? {
            rows.push(row);
        }
    }
    Ok(rows)
}

fn directory_update_changes_structure(payload: &BTreeMap<String, Value>) -> bool {
    payload
        .keys()
        .any(|key| matches!(key.as_str(), "path" | "name" | "parent_id"))
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
) -> Result<ResolvedWritePlan, WriteResolveError> {
    match planned_write.command.target.descriptor.public_name.as_str() {
        "lix_active_version" => match planned_write.command.operation_kind {
            WriteOperationKind::Update => {
                resolve_active_version_update_write_plan(backend, planned_write).await
            }
            _ => Err(WriteResolveError {
                message: "public write resolver only supports UPDATE for 'lix_active_version'"
                    .to_string(),
            }),
        },
        "lix_active_account" => match planned_write.command.operation_kind {
            WriteOperationKind::Insert => resolve_active_account_insert_write_plan(planned_write),
            WriteOperationKind::Delete => {
                resolve_active_account_delete_write_plan(backend, planned_write).await
            }
            WriteOperationKind::Update => Err(WriteResolveError {
                message: "public write resolver does not support UPDATE for 'lix_active_account'"
                    .to_string(),
            }),
        },
        "lix_version" => match planned_write.command.operation_kind {
            WriteOperationKind::Insert => {
                resolve_version_insert_write_plan(backend, planned_write).await
            }
            WriteOperationKind::Update | WriteOperationKind::Delete => {
                resolve_existing_version_write(backend, planned_write).await
            }
        },
        other => Err(WriteResolveError {
            message: format!(
                "public write resolver does not yet support '{}' writes",
                other
            ),
        }),
    }
}

async fn resolve_active_version_update_write_plan(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let MutationPayload::Patch(payload) = &planned_write.command.payload else {
        return Err(WriteResolveError {
            message: "public active-version update resolver requires a patch payload".to_string(),
        });
    };
    if payload.contains_key("id") {
        return Err(WriteResolveError {
            message: "public active-version update cannot modify id".to_string(),
        });
    }
    if payload.keys().any(|key| key != "version_id") {
        return Err(WriteResolveError {
            message: "public active-version update only supports version_id assignments".to_string(),
        });
    }
    let next_version_id =
        payload_text_value(planned_write, "version_id").ok_or_else(|| WriteResolveError {
            message: "public active-version update must set version_id".to_string(),
        })?;
    if next_version_id.is_empty() {
        return Err(WriteResolveError {
            message: "public active-version update cannot set empty version_id".to_string(),
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

    let matching_ids = query_text_selector_values_for_write_selector(
        backend,
        planned_write,
        "id",
        "public active-version selector resolver expected id text rows",
    )
    .await?;
    let current_rows = load_active_version_admin_rows(backend)
        .await
        .map_err(write_resolve_backend_error)?;
    let matching_rows = current_rows
        .into_iter()
        .filter(|row| matching_ids.iter().any(|id| id == &row.id))
        .collect::<Vec<_>>();
    if matching_rows.is_empty() {
        return Ok(noop_resolved_write_plan(
            default_execution_mode_for_request(planned_write.command.requested_mode),
        ));
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

    Ok(single_partition_write_plan(
        default_execution_mode_for_request(planned_write.command.requested_mode),
        authoritative_pre_state,
        intended_post_state,
        Vec::new(),
        lineage,
    ))
}

fn resolve_active_account_insert_write_plan(
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let MutationPayload::FullSnapshot(payload) = &planned_write.command.payload else {
        return Err(WriteResolveError {
            message: "public active-account insert resolver requires a full payload".to_string(),
        });
    };
    if payload.keys().any(|key| key != "account_id") {
        return Err(WriteResolveError {
            message: "public active-account insert only supports the account_id column".to_string(),
        });
    }
    let account_id =
        payload_text_value(planned_write, "account_id").ok_or_else(|| WriteResolveError {
            message: "public active-account insert requires column 'account_id'".to_string(),
        })?;
    if account_id.is_empty() {
        return Err(WriteResolveError {
            message: "public active-account insert requires non-empty account_id".to_string(),
        });
    }

    Ok(single_partition_write_plan(
        default_execution_mode_for_request(planned_write.command.requested_mode),
        Vec::new(),
        vec![active_account_admin_row(&account_id)],
        Vec::new(),
        vec![RowLineage {
            entity_id: account_id,
            source_change_id: None,
            source_commit_id: None,
        }],
    ))
}

async fn resolve_active_account_delete_write_plan(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let matching_account_ids = query_text_selector_values_for_write_selector(
        backend,
        planned_write,
        "account_id",
        "public active-account selector resolver expected account_id text rows",
    )
    .await?;
    let current_rows = load_active_account_admin_rows(backend)
        .await
        .map_err(write_resolve_backend_error)?;
    let matching_rows = current_rows
        .into_iter()
        .filter(|row| {
            matching_account_ids
                .iter()
                .any(|account_id| account_id == &row.account_id)
        })
        .collect::<Vec<_>>();
    if matching_rows.is_empty() {
        return Ok(noop_resolved_write_plan(
            default_execution_mode_for_request(planned_write.command.requested_mode),
        ));
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

    Ok(single_partition_write_plan(
        default_execution_mode_for_request(planned_write.command.requested_mode),
        authoritative_pre_state,
        intended_post_state,
        tombstones,
        lineage,
    ))
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
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let rows = payload_maps(planned_write)?;
    let mut authoritative_pre_state = Vec::new();
    let mut intended_post_state = Vec::new();
    let mut lineage = Vec::new();

    for row in rows {
        let version_id = version_admin_id_from_payload_map(&row)?;
        let name = version_admin_required_text_from_payload_map(&row, "name")?;
        let commit_id = version_admin_required_text_from_payload_map(&row, "commit_id")?;
        let hidden = version_admin_hidden_from_payload_map(&row)?;
        let existing = load_version_admin_row(backend, &version_id)
            .await
            .map_err(write_resolve_backend_error)?;

        if let Some(existing) = existing.as_ref() {
            authoritative_pre_state.extend(version_admin_pre_state_refs(existing));
        }
        intended_post_state.push(version_descriptor_row(&version_id, &name, hidden));
        intended_post_state.push(version_pointer_row(&version_id, &commit_id));
        lineage.push(RowLineage {
            entity_id: version_id,
            source_change_id: None,
            source_commit_id: None,
        });
    }

    Ok(single_partition_write_plan(
        default_execution_mode_for_request(planned_write.command.requested_mode),
        authoritative_pre_state,
        intended_post_state,
        Vec::new(),
        lineage,
    ))
}

async fn resolve_existing_version_write(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let version_ids = query_text_selector_values_for_write_selector(
        backend,
        planned_write,
        "id",
        "public version selector resolver expected id text rows",
    )
    .await?;
    if version_ids.is_empty() {
        return Ok(noop_resolved_write_plan(
            default_execution_mode_for_request(planned_write.command.requested_mode),
        ));
    }

    let mut current_rows = Vec::new();
    for version_id in version_ids {
        let Some(current_row) = load_version_admin_row(backend, &version_id)
            .await
            .map_err(write_resolve_backend_error)?
        else {
            continue;
        };
        current_rows.push(current_row);
    }
    if current_rows.is_empty() {
        return Ok(noop_resolved_write_plan(
            default_execution_mode_for_request(planned_write.command.requested_mode),
        ));
    }

    match planned_write.command.operation_kind {
        WriteOperationKind::Update => {
            let MutationPayload::Patch(payload) = &planned_write.command.payload else {
                return Err(WriteResolveError {
                    message: "public version update resolver requires a patch payload".to_string(),
                });
            };
            if payload.contains_key("id") {
                return Err(WriteResolveError {
                    message: "public version update cannot modify id".to_string(),
                });
            }
            let mut authoritative_pre_state = Vec::new();
            let mut intended_post_state = Vec::new();
            let mut lineage = Vec::new();

            for current_row in current_rows {
                let next_name = payload
                    .get("name")
                    .and_then(text_from_value)
                    .unwrap_or_else(|| current_row.name.clone());
                if next_name.is_empty() {
                    return Err(WriteResolveError {
                        message: "public version update cannot set empty name".to_string(),
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
                        message: "public version update cannot set empty commit_id".to_string(),
                    });
                }

                authoritative_pre_state.extend(version_admin_pre_state_refs(&current_row));
                lineage.push(RowLineage {
                    entity_id: current_row.id.clone(),
                    source_change_id: current_row
                        .descriptor_change_id
                        .clone()
                        .or_else(|| current_row.pointer_change_id.clone()),
                    source_commit_id: None,
                });
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
            }

            Ok(single_partition_write_plan(
                default_execution_mode_for_request(planned_write.command.requested_mode),
                authoritative_pre_state,
                intended_post_state,
                Vec::new(),
                lineage,
            ))
        }
        WriteOperationKind::Delete => {
            let mut authoritative_pre_state = Vec::new();
            let mut intended_post_state = Vec::new();
            let mut tombstones = Vec::new();
            let mut lineage = Vec::new();
            for current_row in current_rows {
                authoritative_pre_state.extend(version_admin_pre_state_refs(&current_row));
                intended_post_state.push(version_descriptor_tombstone_row(&current_row.id));
                intended_post_state.push(version_pointer_tombstone_row(&current_row.id));
                tombstones.extend(version_admin_tombstone_refs(&current_row));
                lineage.push(RowLineage {
                    entity_id: current_row.id.clone(),
                    source_change_id: current_row
                        .descriptor_change_id
                        .clone()
                        .or_else(|| current_row.pointer_change_id.clone()),
                    source_commit_id: None,
                });
            }
            Ok(single_partition_write_plan(
                default_execution_mode_for_request(planned_write.command.requested_mode),
                authoritative_pre_state,
                intended_post_state,
                tombstones,
                lineage,
            ))
        }
        WriteOperationKind::Insert => Err(WriteResolveError {
            message: "public version existing-row resolver does not handle inserts".to_string(),
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

fn version_admin_id_from_payload_map(
    payload: &BTreeMap<String, Value>,
) -> Result<String, WriteResolveError> {
    payload.get("id").and_then(text_from_value).ok_or_else(|| WriteResolveError {
        message: "public version insert requires column 'id'".to_string(),
    })
}

fn version_admin_required_text_from_payload_map(
    payload: &BTreeMap<String, Value>,
    key: &str,
) -> Result<String, WriteResolveError> {
    let value = payload.get(key).and_then(text_from_value).ok_or_else(|| WriteResolveError {
        message: format!("public version insert requires column '{key}'"),
    })?;
    if value.is_empty() {
        return Err(WriteResolveError {
            message: format!("public version insert cannot set empty {key}"),
        });
    }
    Ok(value)
}

fn version_admin_hidden_from_payload_map(
    payload: &BTreeMap<String, Value>,
) -> Result<bool, WriteResolveError> {
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
            message: "public update resolver requires a patch payload".to_string(),
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
                message: format!("public update resolver requires '{column}' in authoritative row"),
            });
        };
        if actual != expected {
            return Err(WriteResolveError {
                message: format!("public update resolver does not support changing '{column}'"),
            });
        }
    }

    Ok(())
}

fn ensure_exact_selector(planned_write: &PlannedWrite) -> Result<(), WriteResolveError> {
    if !planned_write.command.selector.exact_only {
        return Err(WriteResolveError {
            message: "public update/delete resolver only supports exact conjunctive selectors"
                .to_string(),
        });
    }
    Ok(())
}

fn default_execution_mode_for_request(requested_mode: WriteModeRequest) -> WriteMode {
    match requested_mode {
        WriteModeRequest::Auto | WriteModeRequest::ForceTracked => WriteMode::Tracked,
        WriteModeRequest::ForceUntracked => WriteMode::Untracked,
    }
}

fn single_partition_write_plan(
    execution_mode: WriteMode,
    authoritative_pre_state: Vec<ResolvedRowRef>,
    intended_post_state: Vec<PlannedStateRow>,
    tombstones: Vec<ResolvedRowRef>,
    lineage: Vec<RowLineage>,
) -> ResolvedWritePlan {
    ResolvedWritePlan::from_partition(ResolvedWritePartition {
        execution_mode,
        authoritative_pre_state,
        intended_post_state,
        tombstones,
        lineage,
        target_write_lane: None,
    })
}

fn execution_mode_for_overlay_lane(overlay_lane: OverlayLane) -> WriteMode {
    match overlay_lane {
        OverlayLane::LocalTracked | OverlayLane::GlobalTracked => WriteMode::Tracked,
        OverlayLane::LocalUntracked | OverlayLane::GlobalUntracked => WriteMode::Untracked,
    }
}

fn resolve_execution_mode_for_untracked_flag(
    requested_mode: WriteModeRequest,
    untracked: bool,
    tracked_error: &str,
    untracked_error: &str,
) -> Result<WriteMode, WriteResolveError> {
    let execution_mode = if untracked {
        WriteMode::Untracked
    } else {
        WriteMode::Tracked
    };
    match (requested_mode, execution_mode) {
        (WriteModeRequest::ForceTracked, WriteMode::Untracked) => Err(WriteResolveError {
            message: tracked_error.to_string(),
        }),
        (WriteModeRequest::ForceUntracked, WriteMode::Tracked) => Err(WriteResolveError {
            message: untracked_error.to_string(),
        }),
        _ => Ok(execution_mode),
    }
}

fn resolve_execution_mode_for_effective_row(
    requested_mode: WriteModeRequest,
    current_row: &ExactEffectiveStateRow,
) -> Result<WriteMode, WriteResolveError> {
    let execution_mode = execution_mode_for_overlay_lane(current_row.overlay_lane);
    match (requested_mode, execution_mode) {
        (WriteModeRequest::ForceTracked, WriteMode::Untracked) => Err(WriteResolveError {
            message: format!(
                "public tracked write requires a tracked effective-state winner, found {:?}",
                current_row.overlay_lane
            ),
        }),
        (WriteModeRequest::ForceUntracked, WriteMode::Tracked) => Err(WriteResolveError {
            message: format!(
                "public untracked write requires an untracked effective-state winner, found {:?}",
                current_row.overlay_lane
            ),
        }),
        _ => Ok(execution_mode),
    }
}

fn selector_row_version_id(
    planned_write: &PlannedWrite,
    selector_version_id: Option<&str>,
) -> Result<String, WriteResolveError> {
    if let Some(version_id) = selector_version_id {
        return Ok(version_id.to_string());
    }
    resolved_version_id(planned_write)?.ok_or_else(|| WriteResolveError {
        message: "public write resolver requires a concrete version_id for the selected row"
            .to_string(),
    })
}

fn target_write_lane_for_effective_row(
    planned_write: &PlannedWrite,
    execution_mode: WriteMode,
    current_row: &ExactEffectiveStateRow,
) -> Result<Option<WriteLane>, WriteResolveError> {
    target_write_lane_for_version(
        planned_write,
        execution_mode,
        Some(current_row.version_id.as_str()),
    )
}

fn target_write_lane_for_planned_row(
    planned_write: &PlannedWrite,
    execution_mode: WriteMode,
    version_id: Option<&str>,
) -> Result<Option<WriteLane>, WriteResolveError> {
    target_write_lane_for_version(planned_write, execution_mode, version_id)
}

fn target_write_lane_for_version(
    planned_write: &PlannedWrite,
    execution_mode: WriteMode,
    version_id: Option<&str>,
) -> Result<Option<WriteLane>, WriteResolveError> {
    if execution_mode == WriteMode::Untracked {
        return Ok(None);
    }
    match &planned_write.scope_proof {
        ScopeProof::ActiveVersion => Ok(Some(WriteLane::ActiveVersion)),
        ScopeProof::SingleVersion(version_id) => Ok(Some(WriteLane::SingleVersion(
            version_id.clone(),
        ))),
        ScopeProof::FiniteVersionSet(_) => version_id
            .map(|version_id| Some(WriteLane::SingleVersion(version_id.to_string())))
            .ok_or_else(|| WriteResolveError {
                message:
                    "public tracked write could not determine a concrete version lane for a selected row"
                        .to_string(),
            }),
        ScopeProof::GlobalAdmin => Ok(Some(WriteLane::GlobalAdmin)),
        ScopeProof::Unknown | ScopeProof::Unbounded => version_id
            .map(|version_id| Some(WriteLane::SingleVersion(version_id.to_string())))
            .ok_or_else(|| WriteResolveError {
                message: "public tracked write requires a bounded version lane".to_string(),
            }),
    }
}

fn resolve_execution_mode_for_effective_rows(
    requested_mode: WriteModeRequest,
    current_rows: &[ExactEffectiveStateRow],
) -> Result<WriteMode, WriteResolveError> {
    let mut resolved_mode = None;
    for current_row in current_rows {
        let row_mode = resolve_execution_mode_for_effective_row(requested_mode, current_row)?;
        ensure_consistent_execution_mode(
            &mut resolved_mode,
            row_mode,
            "public write resolver does not yet support mixing tracked and untracked effective-state winners",
        )?;
    }
    Ok(resolved_mode.unwrap_or(default_execution_mode_for_request(requested_mode)))
}

fn ensure_consistent_execution_mode(
    resolved_mode: &mut Option<WriteMode>,
    row_mode: WriteMode,
    mixed_mode_error: &str,
) -> Result<(), WriteResolveError> {
    match resolved_mode {
        Some(existing) if *existing != row_mode => Err(WriteResolveError {
            message: mixed_mode_error.to_string(),
        }),
        Some(_) => Ok(()),
        None => {
            *resolved_mode = Some(row_mode);
            Ok(())
        }
    }
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
    schema: JsonValue,
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
        schema,
        schema_key,
        schema_version,
        property_columns,
        primary_key_paths,
        state_defaults,
    })
}

fn reject_unsupported_entity_overrides(
    planned_write: &PlannedWrite,
    entity_schema: &EntityWriteSchema,
) -> Result<(), WriteResolveError> {
    if entity_schema
        .state_defaults
        .get("global")
        .and_then(value_as_bool)
        == Some(true)
    {
        let version_id = resolved_version_id(planned_write)?;
        if version_id.as_deref() != Some(GLOBAL_VERSION_ID) {
            return Err(WriteResolveError {
                message:
                    "public entity write resolver requires a concrete global version_id for lixcol_global write overrides"
                        .to_string(),
            });
        }
    }
    if entity_schema
        .state_defaults
        .get("untracked")
        .and_then(value_as_bool)
        == Some(true)
    {
        return Err(WriteResolveError {
            message: "public entity live slice does not yet support lixcol_untracked write overrides"
                .to_string(),
        });
    }
    Ok(())
}

fn build_entity_insert_rows(
    planned_write: &PlannedWrite,
    entity_schema: &EntityWriteSchema,
) -> Result<Vec<PlannedStateRow>, WriteResolveError> {
    let version_id = resolved_version_id(planned_write)?;
    let payloads = payload_maps(planned_write)?;
    let mut rows = Vec::with_capacity(payloads.len());
    for payload in payloads {
        let snapshot = snapshot_from_entity_payload(&payload, entity_schema)?;
        let entity_id = payload
            .get("entity_id")
            .and_then(text_from_value)
            .map(|value| value.to_string())
            .or_else(|| {
                derive_entity_id_from_snapshot(&snapshot, &entity_schema.primary_key_paths).ok()
            })
            .ok_or_else(|| WriteResolveError {
                message:
                    "public entity insert resolver requires an exact primary-key-derived entity_id"
                        .to_string(),
            })?;
        let file_id = resolved_entity_state_text(&payload, entity_schema, "file_id")?;
        let plugin_key = resolved_entity_state_text(&payload, entity_schema, "plugin_key")?;
        let schema_version = resolved_entity_state_text(&payload, entity_schema, "schema_version")?;
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
                            "public entity insert resolver could not serialize snapshot: {error}"
                        ),
                    }
                })?,
            ),
        );
        if let Some(version_id) = version_id.clone() {
            values.insert("version_id".to_string(), Value::Text(version_id));
        }
        if let Some(metadata) = resolved_entity_state_value(&payload, entity_schema, "metadata") {
            if metadata != Value::Null {
                values.insert("metadata".to_string(), metadata);
            }
        }
        for key in ["global", "untracked"] {
            if let Some(value) = resolved_entity_state_value(&payload, entity_schema, key) {
                if value != Value::Null {
                    values.insert(key.to_string(), value);
                }
            }
        }
        rows.push(PlannedStateRow {
            entity_id,
            schema_key: entity_schema.schema_key.clone(),
            version_id: version_id.clone(),
            values,
            tombstone: false,
        });
    }

    Ok(rows)
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

fn entity_insert_exact_filters(
    entity_schema: &EntityWriteSchema,
    row: &PlannedStateRow,
) -> Result<BTreeMap<String, Value>, WriteResolveError> {
    let mut filters = BTreeMap::new();
    filters.insert("entity_id".to_string(), Value::Text(row.entity_id.clone()));
    for key in [
        "file_id",
        "plugin_key",
        "schema_version",
        "global",
        "untracked",
    ] {
        if let Some(value) = row.values.get(key) {
            filters.insert(key.to_string(), value.clone());
            continue;
        }
        if let Some(default) = entity_schema.state_defaults.get(key) {
            filters.insert(key.to_string(), default.clone());
        }
    }
    Ok(filters)
}

async fn resolve_target_entity_rows(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    entity_schema: &EntityWriteSchema,
) -> Result<Vec<ExactEffectiveStateRow>, WriteResolveError> {
    let selector_rows = query_entity_selector_rows(backend, planned_write).await?;
    let mut rows = Vec::new();
    for selector_row in selector_rows {
        let version_id = selector_row_version_id(planned_write, selector_row.version_id.as_deref())?;
        let exact_filters =
            entity_state_exact_filters(planned_write, entity_schema, &selector_row.entity_id)?;
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
            continue;
        };
        rows.push(current_row);
    }
    Ok(rows)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EntitySelectorRow {
    entity_id: String,
    version_id: Option<String>,
}

async fn query_entity_selector_rows(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<Vec<EntitySelectorRow>, WriteResolveError> {
    let mut selector_columns = vec!["lixcol_entity_id"];
    let version_index = if state_selector_exposes_version_id(planned_write) {
        selector_columns.push(public_selector_version_column(planned_write));
        Some(selector_columns.len() - 1)
    } else {
        None
    };
    let query_result = execute_public_selector_query_strict(
        backend,
        planned_write,
        build_public_selector_query(
            &planned_write.command.target.descriptor.public_name,
            &selector_columns,
            &selector_query_predicates(planned_write),
        ),
    )
    .await
    .map_err(write_resolve_backend_error)?;

    let mut selector_rows = Vec::new();
    for row in query_result.rows {
        let selector_row = EntitySelectorRow {
            entity_id: required_text_value_index(&row, 0, "lixcol_entity_id")?,
            version_id: version_index
                .map(|index| {
                    required_text_value_index(
                        &row,
                        index,
                        public_selector_version_column(planned_write),
                    )
                })
                .transpose()?,
        };
        if !selector_rows.iter().any(|existing| existing == &selector_row) {
            selector_rows.push(selector_row);
        }
    }
    Ok(selector_rows)
}

async fn query_text_selector_values_for_write_selector(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    selector_column: &str,
    error_message: &str,
) -> Result<Vec<String>, WriteResolveError> {
    let query_result = execute_public_selector_query_strict(
        backend,
        planned_write,
        build_public_selector_query(
            &planned_write.command.target.descriptor.public_name,
            &[selector_column],
            &selector_query_predicates(planned_write),
        ),
    )
    .await
    .map_err(write_resolve_backend_error)?;

    let mut values = Vec::new();
    for row in query_result.rows {
        let Some(value) = row.first().and_then(text_from_value) else {
            return Err(WriteResolveError {
                message: error_message.to_string(),
            });
        };
        if !values.iter().any(|existing| existing == &value) {
            values.push(value);
        }
    }
    Ok(values)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StateSelectorRow {
    entity_id: String,
    file_id: String,
    plugin_key: String,
    schema_version: String,
    version_id: Option<String>,
    global: bool,
    untracked: bool,
}

async fn query_state_selector_rows(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<Vec<StateSelectorRow>, WriteResolveError> {
    let mut selector_columns = vec![
        "entity_id",
        "file_id",
        "plugin_key",
        "schema_version",
    ];
    let version_index = if state_selector_exposes_version_id(planned_write) {
        selector_columns.push(public_selector_version_column(planned_write));
        Some(selector_columns.len() - 1)
    } else {
        None
    };
    selector_columns.push("global");
    selector_columns.push("untracked");
    let query_result = execute_public_selector_query_strict(
        backend,
        planned_write,
        build_public_selector_query(
            &planned_write.command.target.descriptor.public_name,
            &selector_columns,
            &selector_query_predicates(planned_write),
        ),
    )
    .await
    .map_err(write_resolve_backend_error)?;

    let mut selector_rows = Vec::new();
    for row in query_result.rows {
        let selector_row = StateSelectorRow {
            entity_id: required_text_value_index(&row, 0, "entity_id")?,
            file_id: required_text_value_index(&row, 1, "file_id")?,
            plugin_key: required_text_value_index(&row, 2, "plugin_key")?,
            schema_version: required_text_value_index(&row, 3, "schema_version")?,
            version_id: version_index
                .map(|index| {
                    required_text_value_index(
                        &row,
                        index,
                        public_selector_version_column(planned_write),
                    )
                })
                .transpose()?,
            global: required_bool_value_index(&row, 4 + version_index.is_some() as usize, "global")?,
            untracked: required_bool_value_index(
                &row,
                5 + version_index.is_some() as usize,
                "untracked",
            )?,
        };
        if !selector_rows.iter().any(|existing| existing == &selector_row) {
            selector_rows.push(selector_row);
        }
    }
    Ok(selector_rows)
}

fn public_selector_version_column(planned_write: &PlannedWrite) -> &'static str {
    match planned_write.command.target.descriptor.surface_family {
        SurfaceFamily::State => "version_id",
        SurfaceFamily::Entity | SurfaceFamily::Filesystem => "lixcol_version_id",
        SurfaceFamily::Admin | SurfaceFamily::Change => "version_id",
    }
}

fn selector_query_predicates(planned_write: &PlannedWrite) -> Vec<Expr> {
    if planned_write.command.selector.exact_only {
        if let Some(predicates) = exact_selector_predicates(planned_write) {
            return predicates;
        }
    }
    planned_write.command.selector.residual_predicates.clone()
}

fn exact_selector_predicates(planned_write: &PlannedWrite) -> Option<Vec<Expr>> {
    let mut predicates = Vec::with_capacity(planned_write.command.selector.exact_filters.len());
    for (column, value) in &planned_write.command.selector.exact_filters {
        let public_column = public_selector_column_name(planned_write, column)?;
        predicates.push(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new(public_column))),
            op: BinaryOperator::Eq,
            right: Box::new(engine_value_to_sql_expr(value)),
        });
    }
    Some(predicates)
}

fn public_selector_column_name(planned_write: &PlannedWrite, canonical_column: &str) -> Option<String> {
    match planned_write.command.target.descriptor.surface_family {
        SurfaceFamily::State => match canonical_column {
            "entity_id" => Some("entity_id".to_string()),
            "schema_key" => Some("schema_key".to_string()),
            "file_id" => Some("file_id".to_string()),
            "plugin_key" => Some("plugin_key".to_string()),
            "schema_version" => Some("schema_version".to_string()),
            "version_id" => Some("version_id".to_string()),
            "global" => Some("global".to_string()),
            "untracked" => Some("untracked".to_string()),
            _ => None,
        },
        SurfaceFamily::Entity => match canonical_column {
            "entity_id" => Some("lixcol_entity_id".to_string()),
            "schema_key" => Some("lixcol_schema_key".to_string()),
            "file_id" => Some("lixcol_file_id".to_string()),
            "plugin_key" => Some("lixcol_plugin_key".to_string()),
            "schema_version" => Some("lixcol_schema_version".to_string()),
            "version_id" => Some("lixcol_version_id".to_string()),
            "global" => Some("lixcol_global".to_string()),
            "untracked" => Some("lixcol_untracked".to_string()),
            "metadata" => Some("lixcol_metadata".to_string()),
            _ => Some(canonical_column.to_string()),
        },
        SurfaceFamily::Filesystem => match canonical_column {
            "id" => Some("id".to_string()),
            "path" => Some("path".to_string()),
            "name" => Some("name".to_string()),
            "parent_id" => Some("parent_id".to_string()),
            "directory_id" => Some("directory_id".to_string()),
            "hidden" => Some("hidden".to_string()),
            "entity_id" => Some("lixcol_entity_id".to_string()),
            "schema_key" => Some("lixcol_schema_key".to_string()),
            "schema_version" => Some("lixcol_schema_version".to_string()),
            "version_id" => Some("lixcol_version_id".to_string()),
            "global" => Some("lixcol_global".to_string()),
            "untracked" => Some("lixcol_untracked".to_string()),
            "metadata" => Some("lixcol_metadata".to_string()),
            _ => None,
        },
        SurfaceFamily::Admin => match canonical_column {
            "id" => Some("id".to_string()),
            "name" => Some("name".to_string()),
            "hidden" => Some("hidden".to_string()),
            "commit_id" => Some("commit_id".to_string()),
            "version_id" => Some("version_id".to_string()),
            "account_id" => Some("account_id".to_string()),
            _ => None,
        },
        SurfaceFamily::Change => None,
    }
}

fn engine_value_to_sql_expr(value: &Value) -> Expr {
    match value {
        Value::Null => Expr::Value(ValueWithSpan::from(SqlValue::Null)),
        Value::Boolean(value) => Expr::Value(ValueWithSpan::from(SqlValue::Boolean(*value))),
        Value::Text(value) => {
            Expr::Value(ValueWithSpan::from(SqlValue::SingleQuotedString(value.clone())))
        }
        Value::Json(value) => Expr::Value(ValueWithSpan::from(SqlValue::SingleQuotedString(
            value.to_string(),
        ))),
        Value::Integer(value) => {
            Expr::Value(ValueWithSpan::from(SqlValue::Number(value.to_string(), false)))
        }
        Value::Real(value) => {
            Expr::Value(ValueWithSpan::from(SqlValue::Number(value.to_string(), false)))
        }
        Value::Blob(value) => Expr::Value(ValueWithSpan::from(
            SqlValue::SingleQuotedByteStringLiteral(String::from_utf8_lossy(value).to_string()),
        )),
    }
}

fn build_public_selector_query(
    surface_name: &str,
    selector_columns: &[&str],
    residual_predicates: &[Expr],
) -> Query {
    let selection = residual_predicates
        .iter()
        .cloned()
        .reduce(|left, right| Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::And,
            right: Box::new(right),
        });

    Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(Select {
            select_token: AttachedToken::empty(),
            distinct: None,
            top: None,
            top_before_distinct: false,
            projection: selector_columns
                .iter()
                .map(|column| SelectItem::UnnamedExpr(Expr::Identifier(Ident::new(*column))))
                .collect(),
            exclude: None,
            into: None,
            from: vec![TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
                        surface_name,
                    ))]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    with_ordinality: false,
                    partitions: vec![],
                    json_path: None,
                    sample: None,
                    index_hints: vec![],
                },
                joins: Vec::new(),
            }],
            lateral_views: Vec::new(),
            prewhere: None,
            selection,
            group_by: GroupByExpr::Expressions(Vec::new(), Vec::new()),
            cluster_by: Vec::new(),
            distribute_by: Vec::new(),
            sort_by: Vec::new(),
            having: None,
            named_window: Vec::new(),
            qualify: None,
            window_before_qualify: false,
            value_table_mode: None,
            connect_by: None,
            flavor: SelectFlavor::Standard,
        }))),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: Vec::new(),
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: Vec::new(),
    }
}

async fn execute_public_selector_query_strict(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
    query: Query,
) -> Result<QueryResult, LixError> {
    let mut statement = Statement::Query(Box::new(query));
    let rebound_params = bind_statement_ast(
        &mut statement,
        &planned_write.command.bound_parameters,
        backend.dialect(),
    )?;
    let Statement::Query(query) = statement else {
        unreachable!("selector query binding should preserve statement kind");
    };
    execute_public_read_query_strict(backend, *query, &rebound_params).await
}

async fn resolve_target_state_rows(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<Vec<ExactEffectiveStateRow>, WriteResolveError> {
    let schema_key = resolved_schema_key(planned_write)?;
    let selector_rows = query_state_selector_rows(backend, planned_write).await?;
    let mut rows = Vec::new();
    for selector_row in selector_rows {
        let version_id = selector_row_version_id(planned_write, selector_row.version_id.as_deref())?;
        let exact_filters = state_selector_exact_filters(planned_write, &selector_row);
        let Some(current_row) = resolve_exact_effective_state_row(
            backend,
            &ExactEffectiveStateRowRequest {
                schema_key: schema_key.clone(),
                version_id,
                exact_filters,
                include_global_overlay: true,
                include_untracked_overlay: true,
            },
        )
        .await
        .map_err(write_resolve_backend_error)?
        else {
            continue;
        };
        rows.push(current_row);
    }
    Ok(rows)
}

fn state_selector_exact_filters(
    planned_write: &PlannedWrite,
    selector_row: &StateSelectorRow,
) -> BTreeMap<String, Value> {
    let mut exact_filters = planned_write.command.selector.exact_filters.clone();
    exact_filters.insert(
        "entity_id".to_string(),
        Value::Text(selector_row.entity_id.clone()),
    );
    exact_filters.insert(
        "file_id".to_string(),
        Value::Text(selector_row.file_id.clone()),
    );
    exact_filters.insert(
        "plugin_key".to_string(),
        Value::Text(selector_row.plugin_key.clone()),
    );
    exact_filters.insert(
        "schema_version".to_string(),
        Value::Text(selector_row.schema_version.clone()),
    );
    if let Some(version_id) = selector_row.version_id.as_ref() {
        exact_filters.insert("version_id".to_string(), Value::Text(version_id.clone()));
    }
    exact_filters.insert("global".to_string(), Value::Boolean(selector_row.global));
    exact_filters.insert(
        "untracked".to_string(),
        Value::Boolean(selector_row.untracked),
    );
    exact_filters
}

fn supports_selector_driven_state_resolution(planned_write: &PlannedWrite) -> bool {
    planned_write.command.target.descriptor.surface_family == SurfaceFamily::State
}

fn merged_entity_update_values(
    planned_write: &PlannedWrite,
    entity_schema: &EntityWriteSchema,
    current_row: &ExactEffectiveStateRow,
) -> Result<BTreeMap<String, Value>, WriteResolveError> {
    let MutationPayload::Patch(payload) = &planned_write.command.payload else {
        return Err(WriteResolveError {
            message: "public entity update resolver requires a patch payload".to_string(),
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
                message: "public entity live slice does not yet support primary-key property updates"
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
        if apply_entity_state_column_update(&mut values, key, value)? {
            continue;
        }
        return Err(WriteResolveError {
            message: format!(
                "public entity live slice does not yet support updating state column '{}'",
                key
            ),
        });
    }

    let expected_entity_id = derive_entity_id_from_snapshot(
        &snapshot,
        &entity_schema.primary_key_paths,
    )
    .map_err(|_| WriteResolveError {
        message: "public entity update resolver requires a stable primary-key-derived entity_id"
            .to_string(),
    })?;
    if expected_entity_id != current_row.entity_id {
        return Err(WriteResolveError {
            message:
                "public entity live slice does not yet support updates that change entity identity"
                    .to_string(),
        });
    }

    values.insert(
        "snapshot_content".to_string(),
        Value::Text(
            serde_json::to_string(&JsonValue::Object(snapshot)).map_err(|error| {
                WriteResolveError {
                    message: format!(
                        "public entity update resolver could not serialize snapshot: {error}"
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

fn apply_entity_state_column_update(
    values: &mut BTreeMap<String, Value>,
    key: &str,
    value: &Value,
) -> Result<bool, WriteResolveError> {
    match key {
        "entity_id" | "schema_key" | "file_id" | "version_id" | "plugin_key"
        | "schema_version" => {
            let Some(text) = text_from_value(value) else {
                return Err(WriteResolveError {
                    message: format!("public entity resolver expected text {key}, got {value:?}"),
                });
            };
            values.insert(key.to_string(), Value::Text(text.to_string()));
            Ok(true)
        }
        "metadata" => match value {
            Value::Null => {
                values.remove(key);
                Ok(true)
            }
            Value::Text(text) => {
                values.insert(key.to_string(), Value::Text(text.clone()));
                Ok(true)
            }
            other => Err(WriteResolveError {
                message: format!("public entity resolver expected text/null {key}, got {other:?}"),
            }),
        },
        _ => Ok(false),
    }
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
        message: "public write resolver requires an exact entity target".to_string(),
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
            message: "public write resolver requires an exact schema proof or schema_key literal"
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
                    "public write resolver requires requested_version_id for ActiveVersion writes"
                        .to_string(),
            }),
        ScopeProof::SingleVersion(version_id) => Ok(Some(version_id.clone())),
        ScopeProof::FiniteVersionSet(version_ids) if version_ids.len() == 1 => {
            Ok(version_ids.iter().next().cloned())
        }
        ScopeProof::FiniteVersionSet(version_ids) if version_ids.is_empty() => Err(
            WriteResolveError {
                message: "public write resolver requires a concrete version_id".to_string(),
            },
        ),
        ScopeProof::FiniteVersionSet(_) => Err(WriteResolveError {
            message: "public write resolver cannot resolve multi-version writes".to_string(),
        }),
        ScopeProof::GlobalAdmin => Ok(Some(GLOBAL_VERSION_ID.to_string())),
        ScopeProof::Unknown | ScopeProof::Unbounded => Err(WriteResolveError {
            message: "public write resolver requires a bounded scope proof".to_string(),
        }),
    }
}

fn insert_on_conflict_action(planned_write: &PlannedWrite) -> Option<InsertOnConflictAction> {
    planned_write
        .command
        .on_conflict
        .as_ref()
        .map(|conflict| conflict.action)
}

fn finalize_resolved_write_plan(
    planned_write: &PlannedWrite,
    mut resolved: ResolvedWritePlan,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    resolved
        .partitions
        .retain(|partition| !partition.intended_post_state.is_empty());
    for partition in &mut resolved.partitions {
        if partition.execution_mode == WriteMode::Untracked {
            partition.target_write_lane = None;
            continue;
        }
        if partition.target_write_lane.is_none() {
            partition.target_write_lane = Some(write_lane_from_scope(&planned_write.scope_proof)?);
        }
    }
    Ok(resolved)
}

fn noop_resolved_write_plan(execution_mode: WriteMode) -> ResolvedWritePlan {
    single_partition_write_plan(
        execution_mode,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
}

async fn state_insert_conflicts_with_existing_row(
    backend: &dyn LixBackend,
    row: &PlannedStateRow,
) -> Result<bool, WriteResolveError> {
    let version_id = row.version_id.clone().ok_or_else(|| WriteResolveError {
        message: "public state insert resolver requires a concrete version_id".to_string(),
    })?;
    let current_row = resolve_exact_effective_state_row(
        backend,
        &ExactEffectiveStateRowRequest {
            schema_key: row.schema_key.clone(),
            version_id,
            exact_filters: state_insert_exact_filters(row),
            include_global_overlay: true,
            include_untracked_overlay: true,
        },
    )
    .await
    .map_err(write_resolve_backend_error)?;
    Ok(current_row.is_some())
}

fn state_insert_exact_filters(row: &PlannedStateRow) -> BTreeMap<String, Value> {
    let mut filters = BTreeMap::new();
    filters.insert("entity_id".to_string(), Value::Text(row.entity_id.clone()));
    for key in [
        "file_id",
        "plugin_key",
        "schema_version",
        "global",
        "untracked",
    ] {
        if let Some(value) = row.values.get(key) {
            filters.insert(key.to_string(), value.clone());
        }
    }
    filters
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
            message: "public tracked writes require exactly one write lane".to_string(),
        }),
        ScopeProof::GlobalAdmin => Ok(WriteLane::GlobalAdmin),
        ScopeProof::Unknown | ScopeProof::Unbounded => Err(WriteResolveError {
            message: "public tracked writes require a bounded write lane".to_string(),
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
    apply_schema_defaults_with_system_functions(
        &mut snapshot,
        &entity_schema.schema,
        &entity_schema.schema_key,
        &entity_schema.schema_version,
    )
    .map_err(|error| WriteResolveError {
        message: error.description,
    })?;
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
            message: "public entity resolver requires snapshot_content in authoritative pre-state"
                .to_string(),
        });
    };
    let JsonValue::Object(object) =
        serde_json::from_str::<JsonValue>(&snapshot_text).map_err(|error| WriteResolveError {
            message: format!("public entity resolver could not parse snapshot_content JSON: {error}"),
        })?
    else {
        return Err(WriteResolveError {
            message: "public entity resolver requires object snapshot_content".to_string(),
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
            message: "public entity resolver requires x-lix-primary-key for entity writes"
                .to_string(),
        });
    }

    let snapshot = JsonValue::Object(snapshot.clone());
    let mut parts = Vec::with_capacity(primary_key_paths.len());
    for path in primary_key_paths {
        if path.is_empty() {
            return Err(WriteResolveError {
                message: "public entity resolver does not support empty primary-key pointers"
                    .to_string(),
            });
        }
        let value = json_pointer_get(&snapshot, path).ok_or_else(|| WriteResolveError {
            message: "public entity resolver could not derive entity_id from the primary-key fields"
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
                "public entity resolver requires a concrete '{}' value or schema override",
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
        description: format!("public entity resolver requires literal lixcol overrides: {error}"),
    })?;
    Ok(json_value_to_engine_value(&parsed))
}

fn engine_value_to_json_value(value: &Value) -> Result<JsonValue, WriteResolveError> {
    match value {
        Value::Null => Ok(JsonValue::Null),
        Value::Text(value) => Ok(JsonValue::String(value.clone())),
        Value::Json(value) => Ok(value.clone()),
        Value::Boolean(value) => Ok(JsonValue::Bool(*value)),
        Value::Integer(value) => Ok(JsonValue::Number((*value).into())),
        Value::Real(value) => JsonNumber::from_f64(*value)
            .map(JsonValue::Number)
            .ok_or_else(|| WriteResolveError {
                message: "public entity resolver cannot represent NaN/inf JSON numbers".to_string(),
            }),
        Value::Blob(_) => Err(WriteResolveError {
            message: "public entity resolver does not support blob entity properties".to_string(),
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
        JsonValue::Array(_) | JsonValue::Object(_) => Value::Json(value.clone()),
    }
}

fn json_value_matches_engine_value(actual: Option<&JsonValue>, expected: &Value) -> bool {
    match (actual, expected) {
        (Some(JsonValue::Null), Value::Null) => true,
        (Some(JsonValue::Bool(actual)), Value::Boolean(expected)) => actual == expected,
        (Some(JsonValue::String(actual)), Value::Text(expected)) => actual == expected,
        (Some(actual), Value::Json(expected)) => actual == expected,
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
            message: "public entity resolver cannot derive entity_id from null primary-key values"
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
            message: format!("public filesystem resolver expected text {}", label),
        })
}

fn required_bool_value_index(
    row: &[Value],
    index: usize,
    label: &str,
) -> Result<bool, WriteResolveError> {
    row.get(index)
        .and_then(value_as_bool)
        .ok_or_else(|| WriteResolveError {
            message: format!("public selector resolver expected bool {}", label),
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
        MutationPayload::BulkFullSnapshot(_) => Err(WriteResolveError {
            message: "public resolver expected a single-row payload".to_string(),
        }),
        MutationPayload::Tombstone => Ok(Default::default()),
    }
}

fn payload_text_value(planned_write: &PlannedWrite, key: &str) -> Option<String> {
    match &planned_write.command.payload {
        MutationPayload::FullSnapshot(payload) | MutationPayload::Patch(payload) => {
            match payload.get(key) {
                Some(Value::Text(value)) => Some(value.clone()),
                _ => None,
            }
        }
        MutationPayload::BulkFullSnapshot(payloads) => {
            let mut values = payloads
                .iter()
                .filter_map(|payload| match payload.get(key) {
                    Some(Value::Text(value)) => Some(value.clone()),
                    _ => None,
                });
            let first = values.next()?;
            values.all(|candidate| candidate == first).then_some(first)
        }
        MutationPayload::Tombstone => None,
    }
}

fn payload_maps(
    planned_write: &PlannedWrite,
) -> Result<Vec<BTreeMap<String, Value>>, WriteResolveError> {
    match &planned_write.command.payload {
        MutationPayload::FullSnapshot(payload) => Ok(vec![payload.clone()]),
        MutationPayload::BulkFullSnapshot(payloads) => Ok(payloads.clone()),
        MutationPayload::Patch(_) | MutationPayload::Tombstone => Err(WriteResolveError {
            message: "public resolver expected insert payload rows".to_string(),
        }),
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
            message: "public filesystem resolver expected a single helper statement".to_string(),
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
    use crate::sql::public::catalog::SurfaceRegistry;
    use crate::sql::public::core::contracts::{BoundStatement, ExecutionContext};
    use crate::sql::public::core::parser::parse_sql_script;
    use crate::sql::public::planner::canonicalize::canonicalize_write;
    use crate::sql::public::planner::ir::{WriteLane, WriteMode};
    use crate::sql::public::planner::semantics::write_analysis::analyze_write;
    use crate::{LixBackend, LixError, QueryResult, SqlDialect, Value};
    use async_trait::async_trait;

    #[derive(Default)]
    struct FakeBackend {
        change_rows: Vec<Vec<Value>>,
        untracked_rows: Vec<Vec<Value>>,
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
            if sql.contains("FROM lix_internal_state_untracked") {
                return Ok(QueryResult {
                    rows: self.untracked_rows.clone(),
                    columns: vec![
                        "entity_id".to_string(),
                        "schema_key".to_string(),
                        "schema_version".to_string(),
                        "file_id".to_string(),
                        "version_id".to_string(),
                        "plugin_key".to_string(),
                        "snapshot_content".to_string(),
                        "metadata".to_string(),
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

    fn build_untracked_state_rows(
        entity_id: &str,
        version_id: &str,
        file_id: &str,
        plugin_key: &str,
        snapshot_content: &str,
        metadata: Option<&str>,
    ) -> Vec<Vec<Value>> {
        vec![vec![
            Value::Text(entity_id.to_string()),
            Value::Text("lix_key_value".to_string()),
            Value::Text("1".to_string()),
            Value::Text(file_id.to_string()),
            Value::Text(version_id.to_string()),
            Value::Text(plugin_key.to_string()),
            Value::Text(snapshot_content.to_string()),
            metadata
                .map(|value| Value::Text(value.to_string()))
                .unwrap_or(Value::Null),
        ]]
    }

    fn planned_write(
        sql: &str,
        requested_version_id: &str,
    ) -> crate::sql::public::planner::ir::PlannedWrite {
        planned_write_with_params(sql, &[], requested_version_id)
    }

    fn planned_write_with_params(
        sql: &str,
        params: &[Value],
        requested_version_id: &str,
    ) -> crate::sql::public::planner::ir::PlannedWrite {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let mut statements = parse_sql_script(sql).expect("SQL should parse");
        let statement = statements.pop().expect("single statement");
        let bound = BoundStatement::from_statement(
            statement,
            params.to_vec(),
            ExecutionContext {
                requested_version_id: Some(requested_version_id.to_string()),
                ..ExecutionContext::default()
            },
        );
        let canonicalized =
            canonicalize_write(bound, &registry).expect("write should canonicalize");
        analyze_write(&canonicalized).expect("write analysis should succeed")
    }

    fn tracked_write_lane(
        resolved: &crate::sql::public::planner::ir::ResolvedWritePlan,
    ) -> Option<WriteLane> {
        resolved
            .tracked_partitions()
            .next()
            .and_then(|partition| partition.target_write_lane.clone())
    }

    fn single_execution_mode(
        resolved: &crate::sql::public::planner::ir::ResolvedWritePlan,
    ) -> Option<WriteMode> {
        resolved.single_partition().map(|partition| partition.execution_mode)
    }

    fn intended_post_state<'a>(
        resolved: &'a crate::sql::public::planner::ir::ResolvedWritePlan,
    ) -> Vec<&'a crate::sql::public::planner::ir::PlannedStateRow> {
        resolved.intended_post_state().collect()
    }

    fn authoritative_pre_state<'a>(
        resolved: &'a crate::sql::public::planner::ir::ResolvedWritePlan,
    ) -> Vec<&'a crate::sql::public::planner::ir::ResolvedRowRef> {
        resolved.authoritative_pre_state().collect()
    }

    fn tombstones<'a>(
        resolved: &'a crate::sql::public::planner::ir::ResolvedWritePlan,
    ) -> Vec<&'a crate::sql::public::planner::ir::ResolvedRowRef> {
        resolved.tombstones().collect()
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

        let rows = intended_post_state(&resolved);
        assert_eq!(rows[0].version_id.as_deref(), Some("main"));
        assert_eq!(tracked_write_lane(&resolved), Some(WriteLane::ActiveVersion));
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

        let rows = intended_post_state(&resolved);
        assert_eq!(
            tracked_write_lane(&resolved),
            Some(WriteLane::SingleVersion("version-a".to_string()))
        );
        assert_eq!(rows[0].version_id.as_deref(), Some("version-a"));
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

        let rows = intended_post_state(&resolved);
        assert_eq!(tracked_write_lane(&resolved), Some(WriteLane::GlobalAdmin));
        assert_eq!(rows.len(), 2);
        assert!(rows
            .iter()
            .any(|row| row.schema_key == crate::version::version_descriptor_schema_key()));
        assert!(rows
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

        let authoritative = authoritative_pre_state(&resolved);
        let rows = intended_post_state(&resolved);
        assert_eq!(authoritative.len(), 1);
        assert_eq!(
            rows[0]
                .values
                .get("file_id")
                .and_then(super::text_from_value)
                .as_deref(),
            Some("lix")
        );
        assert_eq!(
            rows[0]
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

        assert_eq!(authoritative_pre_state(&resolved).len(), 1);
        assert_eq!(tombstones(&resolved).len(), 1);
        assert!(intended_post_state(&resolved)[0].tombstone);
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
            tracked_write_lane(&resolved),
            Some(WriteLane::SingleVersion("version-a".into()))
        );
        assert!(authoritative_pre_state(&resolved).is_empty());
        assert!(intended_post_state(&resolved).is_empty());
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
        .expect_err("identity-changing update should stay off the public live slice");

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

        assert!(intended_post_state(&resolved).is_empty());
    }

    #[tokio::test]
    async fn resolves_state_update_against_explicit_untracked_winner() {
        let backend = FakeBackend {
            untracked_rows: build_untracked_state_rows(
                "entity-1",
                "version-a",
                "lix",
                "lix",
                "{\"value\":\"before\"}",
                None,
            ),
            ..FakeBackend::default()
        };
        let resolved = resolve_write_plan(
            &backend,
            &planned_write(
                "UPDATE lix_state \
                 SET snapshot_content = '{\"value\":\"after\"}' \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'entity-1' \
                   AND file_id = 'lix' \
                   AND untracked = true",
                "version-a",
            ),
        )
        .await
        .expect("explicit untracked winner should resolve");

        let rows = intended_post_state(&resolved);
        assert_eq!(single_execution_mode(&resolved), Some(WriteMode::Untracked));
        assert_eq!(tracked_write_lane(&resolved), None);
        assert_eq!(
            rows[0]
                .values
                .get("snapshot_content")
                .and_then(super::text_from_value)
                .as_deref(),
            Some("{\"value\":\"after\"}")
        );
    }

    #[tokio::test]
    async fn resolves_state_update_against_visible_untracked_winner_without_selector_hint() {
        let backend = FakeBackend {
            change_rows: build_committed_state_change_rows(
                "entity-1",
                "version-a",
                "lix",
                "lix",
                "{\"value\":\"tracked-before\"}",
                None,
                "change-1",
                "commit-1",
            ),
            untracked_rows: build_untracked_state_rows(
                "entity-1",
                "version-a",
                "lix",
                "lix",
                "{\"value\":\"untracked-before\"}",
                None,
            ),
            ..FakeBackend::default()
        };
        let resolved = resolve_write_plan(
            &backend,
            &planned_write(
                "UPDATE lix_state \
                 SET snapshot_content = '{\"value\":\"after\"}' \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'entity-1' \
                   AND file_id = 'lix'",
                "version-a",
            ),
        )
        .await
        .expect("visible untracked winner should resolve");

        let rows = intended_post_state(&resolved);
        assert_eq!(single_execution_mode(&resolved), Some(WriteMode::Untracked));
        assert_eq!(tracked_write_lane(&resolved), None);
        assert_eq!(
            rows[0]
                .values
                .get("snapshot_content")
                .and_then(super::text_from_value)
                .as_deref(),
            Some("{\"value\":\"after\"}")
        );
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
        .expect("directory delete should resolve through public lowering");

        let authoritative = authoritative_pre_state(&resolved);
        let rows = intended_post_state(&resolved);
        let deleted = tombstones(&resolved);
        assert_eq!(
            tracked_write_lane(&resolved),
            Some(WriteLane::SingleVersion("version-a".into()))
        );
        assert!(authoritative.len() >= 4);
        assert!(rows.len() >= 4);
        assert!(deleted.len() >= 4);
        assert!(rows.iter().all(|row| row.tombstone));
        assert!(rows
            .iter()
            .any(|row| row.schema_key == "lix_directory_descriptor"));
        assert!(rows
            .iter()
            .any(|row| row.schema_key == "lix_file_descriptor"));
        assert!(rows
            .iter()
            .any(|row| row.schema_key == "lix_binary_blob_ref"));
    }
}
