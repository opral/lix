use crate::account::{
    active_account_file_id, active_account_plugin_key, active_account_schema_key,
    active_account_schema_version, active_account_snapshot_content,
    active_account_storage_version_id,
};
use crate::schema::live_store::{
    load_exact_live_row_with_executor, load_untracked_live_rows_by_property_with_executor,
    LiveRowScope,
};
use crate::sql::public::catalog::SurfaceFamily;
use crate::sql::public::planner::ir::{
    InsertOnConflictAction, MutationPayload, PlannedStateRow, PlannedWrite, ResolvedRowRef,
    ResolvedWritePartition, ResolvedWritePlan, RowLineage, SchemaProof, ScopeProof, TargetSetProof,
    WriteLane, WriteMode, WriteModeRequest, WriteOperationKind,
};
use crate::sql::public::planner::semantics::effective_state_resolver::{
    resolve_exact_effective_state_row, ExactEffectiveStateRow, ExactEffectiveStateRowRequest,
};
use crate::sql::public::planner::semantics::filesystem_assignments::FilesystemAssignmentsError;
use crate::sql::public::planner::semantics::filesystem_planning::FilesystemPlanningError;
use crate::sql::public::planner::semantics::filesystem_queries::FilesystemQueryError;
use crate::sql::public::planner::semantics::state_assignments::StateAssignmentsError;
use crate::sql::public::planner::semantics::surface_semantics::OverlayLane;
use crate::version::{
    active_version_file_id, active_version_plugin_key, active_version_schema_key,
    active_version_schema_version, active_version_snapshot_content,
    active_version_storage_version_id, version_descriptor_file_id, version_descriptor_plugin_key,
    version_descriptor_schema_key, version_descriptor_schema_version,
    version_descriptor_snapshot_content, version_descriptor_storage_version_id,
    version_ref_file_id, version_ref_plugin_key, version_ref_schema_key,
    version_ref_schema_version, version_ref_snapshot_content, version_ref_storage_version_id,
    GLOBAL_VERSION_ID,
};
use crate::{LixBackend, Value};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

mod filesystem_writes;
mod selector_queries;
mod state_backed_writes;

use filesystem_writes::resolve_filesystem_write;
use selector_queries::query_text_selector_values_for_write_selector;
use state_backed_writes::{resolve_entity_write, resolve_state_write};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WriteResolveError {
    pub(crate) message: String,
}

impl From<FilesystemQueryError> for WriteResolveError {
    fn from(error: FilesystemQueryError) -> Self {
        Self {
            message: error.message,
        }
    }
}

#[derive(Default, Clone)]
struct ResolvedWritePartitionBuilder {
    authoritative_pre_state: Vec<ResolvedRowRef>,
    authoritative_pre_state_rows: Vec<PlannedStateRow>,
    intended_post_state: Vec<PlannedStateRow>,
    tombstones: Vec<ResolvedRowRef>,
    lineage: Vec<RowLineage>,
    filesystem_state: crate::sql::execution::runtime_effects::FilesystemTransactionState,
}

impl ResolvedWritePartitionBuilder {
    fn is_empty(&self) -> bool {
        self.authoritative_pre_state.is_empty()
            && self.intended_post_state.is_empty()
            && self.tombstones.is_empty()
            && self.lineage.is_empty()
    }

    fn into_partition(mut self, execution_mode: WriteMode) -> Option<ResolvedWritePartition> {
        self.normalize_semantic_noops();
        (!self.is_empty()).then_some(ResolvedWritePartition {
            execution_mode,
            authoritative_pre_state: self.authoritative_pre_state,
            authoritative_pre_state_rows: self.authoritative_pre_state_rows,
            intended_post_state: self.intended_post_state,
            tombstones: self.tombstones,
            lineage: self.lineage,
            target_write_lane: None,
            filesystem_state: self.filesystem_state,
        })
    }

    fn normalize_semantic_noops(&mut self) {
        if self.authoritative_pre_state_rows.is_empty() || self.intended_post_state.is_empty() {
            return;
        }

        let authoritative_by_identity = self
            .authoritative_pre_state_rows
            .iter()
            .map(|row| (planned_state_row_identity(row), row))
            .collect::<BTreeMap<_, _>>();
        let mut dropped_blob_rows = std::collections::BTreeSet::new();

        self.intended_post_state.retain(|row| {
            let Some(authoritative) =
                authoritative_by_identity.get(&planned_state_row_identity(row))
            else {
                return true;
            };
            let unchanged = !row.tombstone && planned_state_rows_equivalent(authoritative, row);
            if unchanged && row.schema_key == "lix_binary_blob_ref" && row.version_id.is_some() {
                dropped_blob_rows.insert((
                    row.entity_id.clone(),
                    row.version_id
                        .clone()
                        .expect("checked version_id presence above"),
                ));
            }
            !unchanged
        });

        if !dropped_blob_rows.is_empty() {
            for file in self.filesystem_state.files.values_mut() {
                if dropped_blob_rows.contains(&(file.file_id.clone(), file.version_id.clone())) {
                    file.data = None;
                }
            }
            self.filesystem_state.files.retain(|_, file| {
                file.deleted
                    || file.descriptor.is_some()
                    || file.data.is_some()
                    || !matches!(
                        file.metadata_patch,
                        crate::sql::public::planner::ir::OptionalTextPatch::Unchanged
                    )
            });
        }
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
        SurfaceFamily::State => resolve_state_write(backend, planned_write).await,
        SurfaceFamily::Entity => resolve_entity_write(backend, planned_write).await,
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
            WriteOperationKind::Insert => {
                resolve_active_account_insert_write_plan(backend, planned_write).await
            }
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
    let MutationPayload::UpdatePatch(payload) = &planned_write.command.payload else {
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
            message: "public active-version update only supports version_id assignments"
                .to_string(),
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
    let authoritative_pre_state_rows = matching_rows
        .iter()
        .map(|row| active_version_admin_row(&row.id, &row.version_id))
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
        authoritative_pre_state_rows,
        intended_post_state,
        Vec::new(),
        lineage,
    ))
}

async fn resolve_active_account_insert_write_plan(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let MutationPayload::InsertRows(payloads) = &planned_write.command.payload else {
        return Err(WriteResolveError {
            message: "public active-account insert resolver requires a full payload".to_string(),
        });
    };
    let [payload] = payloads.as_slice() else {
        return Err(WriteResolveError {
            message: "public active-account insert resolver requires exactly one payload row"
                .to_string(),
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

    let current_rows = load_active_account_admin_rows(backend)
        .await
        .map_err(write_resolve_backend_error)?;
    if current_rows.len() == 1 && current_rows[0].account_id == account_id {
        return Ok(noop_resolved_write_plan(
            default_execution_mode_for_request(planned_write.command.requested_mode),
        ));
    }

    let authoritative_pre_state = current_rows
        .iter()
        .map(active_account_admin_pre_state_ref)
        .collect::<Vec<_>>();
    let mut intended_post_state = current_rows
        .iter()
        .filter(|row| row.account_id != account_id)
        .map(|row| active_account_admin_tombstone_row(&row.account_id))
        .collect::<Vec<_>>();
    let tombstones = current_rows
        .iter()
        .filter(|row| row.account_id != account_id)
        .map(active_account_admin_pre_state_ref)
        .collect::<Vec<_>>();
    let mut lineage = current_rows
        .iter()
        .filter(|row| row.account_id != account_id)
        .map(|row| RowLineage {
            entity_id: row.account_id.clone(),
            source_change_id: None,
            source_commit_id: None,
        })
        .collect::<Vec<_>>();

    if !current_rows.iter().any(|row| row.account_id == account_id) {
        intended_post_state.push(active_account_admin_row(&account_id));
        lineage.push(RowLineage {
            entity_id: account_id.clone(),
            source_change_id: None,
            source_commit_id: None,
        });
    }

    Ok(single_partition_write_plan(
        default_execution_mode_for_request(planned_write.command.requested_mode),
        authoritative_pre_state,
        current_rows
            .iter()
            .map(|row| active_account_admin_row(&row.account_id))
            .collect(),
        intended_post_state,
        tombstones,
        lineage,
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
        .iter()
        .map(|row| RowLineage {
            entity_id: row.account_id.clone(),
            source_change_id: None,
            source_commit_id: None,
        })
        .collect::<Vec<_>>();

    Ok(single_partition_write_plan(
        default_execution_mode_for_request(planned_write.command.requested_mode),
        authoritative_pre_state,
        matching_rows
            .iter()
            .map(|row| active_account_admin_row(&row.account_id))
            .collect(),
        intended_post_state,
        tombstones,
        lineage,
    ))
}

async fn load_active_version_admin_rows(
    backend: &dyn LixBackend,
) -> Result<Vec<ActiveVersionAdminRow>, crate::LixError> {
    let mut executor = backend;
    let filters = BTreeMap::from([
        ("file_id", active_version_file_id().to_string()),
        (
            "version_id",
            active_version_storage_version_id().to_string(),
        ),
    ]);
    let rows = load_untracked_live_rows_by_property_with_executor(
        &mut executor,
        active_version_schema_key(),
        "version_id",
        &filters,
        true,
        &["updated_at", "entity_id"],
    )
    .await?;
    Ok(rows
        .into_iter()
        .filter_map(|row| {
            row.property_text("version_id")
                .map(|version_id| ActiveVersionAdminRow {
                    id: row.entity_id,
                    version_id,
                })
        })
        .collect())
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
    let mut executor = backend;
    let filters = BTreeMap::from([
        ("file_id", active_account_file_id().to_string()),
        (
            "version_id",
            active_account_storage_version_id().to_string(),
        ),
    ]);
    let rows = load_untracked_live_rows_by_property_with_executor(
        &mut executor,
        active_account_schema_key(),
        "account_id",
        &filters,
        true,
        &["updated_at", "entity_id"],
    )
    .await?;
    Ok(rows
        .into_iter()
        .filter_map(|row| {
            row.property_text("account_id")
                .map(|account_id| ActiveAccountAdminRow { account_id })
        })
        .collect())
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

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

async fn resolve_version_insert_write_plan(
    backend: &dyn LixBackend,
    planned_write: &PlannedWrite,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let rows = payload_maps(planned_write)?;
    let mut partitions = ResolvedWritePlanBuilder::default();

    for row in rows {
        let version_id = version_admin_id_from_payload_map(&row)?;
        let name = version_admin_required_text_from_payload_map(&row, "name")?;
        let commit_id = version_admin_required_text_from_payload_map(&row, "commit_id")?;
        let hidden = version_admin_hidden_from_payload_map(&row)?;
        let existing = load_version_admin_row(backend, &version_id)
            .await
            .map_err(write_resolve_backend_error)?;

        if let Some(existing) = existing.as_ref() {
            partitions
                .partition_mut(WriteMode::Tracked, None)
                .authoritative_pre_state
                .extend(version_descriptor_pre_state_refs(existing));
            partitions
                .partition_mut(WriteMode::Untracked, None)
                .authoritative_pre_state
                .extend(version_ref_pre_state_refs(existing));
        }
        partitions
            .partition_mut(WriteMode::Tracked, None)
            .intended_post_state
            .push(version_descriptor_row(&version_id, &name, hidden));
        partitions
            .partition_mut(WriteMode::Tracked, None)
            .lineage
            .push(RowLineage {
                entity_id: version_id.clone(),
                source_change_id: existing.and_then(|row| row.descriptor_change_id),
                source_commit_id: None,
            });
        partitions
            .partition_mut(WriteMode::Untracked, None)
            .intended_post_state
            .push(version_ref_row(&version_id, &commit_id));
        partitions
            .partition_mut(WriteMode::Untracked, None)
            .lineage
            .push(RowLineage {
                entity_id: version_id,
                source_change_id: None,
                source_commit_id: None,
            });
    }

    Ok(partitions.into_resolved_write_plan(planned_write.command.requested_mode))
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
            let MutationPayload::UpdatePatch(payload) = &planned_write.command.payload else {
                return Err(WriteResolveError {
                    message: "public version update resolver requires a patch payload".to_string(),
                });
            };
            if payload.contains_key("id") {
                return Err(WriteResolveError {
                    message: "public version update cannot modify id".to_string(),
                });
            }
            let mut partitions = ResolvedWritePlanBuilder::default();

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

                if payload.contains_key("name") || payload.contains_key("hidden") {
                    let tracked = partitions.partition_mut(WriteMode::Tracked, None);
                    tracked
                        .authoritative_pre_state
                        .extend(version_descriptor_pre_state_refs(&current_row));
                    tracked
                        .authoritative_pre_state_rows
                        .push(version_descriptor_row(
                            &current_row.id,
                            &current_row.name,
                            current_row.hidden,
                        ));
                    tracked.lineage.push(RowLineage {
                        entity_id: current_row.id.clone(),
                        source_change_id: current_row.descriptor_change_id.clone(),
                        source_commit_id: None,
                    });
                    tracked.intended_post_state.push(version_descriptor_row(
                        &current_row.id,
                        &next_name,
                        next_hidden,
                    ));
                }
                if payload.contains_key("commit_id") {
                    let untracked = partitions.partition_mut(WriteMode::Untracked, None);
                    untracked
                        .authoritative_pre_state
                        .extend(version_ref_pre_state_refs(&current_row));
                    untracked
                        .authoritative_pre_state_rows
                        .push(version_ref_row(&current_row.id, &current_row.commit_id));
                    untracked.lineage.push(RowLineage {
                        entity_id: current_row.id.clone(),
                        source_change_id: None,
                        source_commit_id: None,
                    });
                    untracked
                        .intended_post_state
                        .push(version_ref_row(&current_row.id, &next_commit_id));
                }
            }

            Ok(partitions.into_resolved_write_plan(planned_write.command.requested_mode))
        }
        WriteOperationKind::Delete => {
            let mut partitions = ResolvedWritePlanBuilder::default();
            for current_row in current_rows {
                let tracked = partitions.partition_mut(WriteMode::Tracked, None);
                tracked
                    .authoritative_pre_state
                    .extend(version_descriptor_pre_state_refs(&current_row));
                tracked
                    .intended_post_state
                    .push(version_descriptor_tombstone_row(&current_row.id));
                tracked
                    .tombstones
                    .extend(version_descriptor_tombstone_refs(&current_row));
                tracked.lineage.push(RowLineage {
                    entity_id: current_row.id.clone(),
                    source_change_id: current_row.descriptor_change_id.clone(),
                    source_commit_id: None,
                });

                let untracked = partitions.partition_mut(WriteMode::Untracked, None);
                untracked
                    .authoritative_pre_state
                    .extend(version_ref_pre_state_refs(&current_row));
                untracked
                    .intended_post_state
                    .push(version_ref_tombstone_row(&current_row.id));
                untracked
                    .tombstones
                    .extend(version_ref_tombstone_refs(&current_row));
                untracked.lineage.push(RowLineage {
                    entity_id: current_row.id.clone(),
                    source_change_id: None,
                    source_commit_id: None,
                });
            }
            Ok(partitions.into_resolved_write_plan(planned_write.command.requested_mode))
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
    let mut executor = backend;
    let descriptor_filters = BTreeMap::from([
        ("entity_id", version_id.to_string()),
        ("file_id", version_descriptor_file_id().to_string()),
        ("plugin_key", version_descriptor_plugin_key().to_string()),
        (
            "version_id",
            version_descriptor_storage_version_id().to_string(),
        ),
    ]);
    let Some(descriptor_row) = load_exact_live_row_with_executor(
        &mut executor,
        LiveRowScope::Tracked,
        version_descriptor_schema_key(),
        &descriptor_filters,
    )
    .await?
    else {
        return Ok(None);
    };
    let pointer_filters = BTreeMap::from([
        ("entity_id", version_id.to_string()),
        ("file_id", version_ref_file_id().to_string()),
        ("plugin_key", version_ref_plugin_key().to_string()),
        ("version_id", version_ref_storage_version_id().to_string()),
    ]);
    let pointer_row = load_exact_live_row_with_executor(
        &mut executor,
        LiveRowScope::Untracked,
        version_ref_schema_key(),
        &pointer_filters,
    )
    .await?;
    Ok(Some(VersionAdminRow {
        id: version_id.to_string(),
        name: descriptor_row.property_text("name").unwrap_or_default(),
        hidden: descriptor_row
            .values
            .get("hidden")
            .and_then(value_as_bool)
            .unwrap_or(false),
        commit_id: pointer_row
            .as_ref()
            .and_then(|row| row.property_text("commit_id"))
            .unwrap_or_default(),
        descriptor_change_id: descriptor_row.change_id,
        pointer_change_id: None,
    }))
}

fn version_descriptor_pre_state_refs(row: &VersionAdminRow) -> Vec<ResolvedRowRef> {
    vec![ResolvedRowRef {
        entity_id: row.id.clone(),
        schema_key: version_descriptor_schema_key().to_string(),
        version_id: Some(GLOBAL_VERSION_ID.to_string()),
        source_change_id: row.descriptor_change_id.clone(),
        source_commit_id: None,
    }]
}

fn version_ref_pre_state_refs(row: &VersionAdminRow) -> Vec<ResolvedRowRef> {
    vec![ResolvedRowRef {
        entity_id: row.id.clone(),
        schema_key: version_ref_schema_key().to_string(),
        version_id: Some(GLOBAL_VERSION_ID.to_string()),
        source_change_id: None,
        source_commit_id: None,
    }]
}

fn version_descriptor_tombstone_refs(row: &VersionAdminRow) -> Vec<ResolvedRowRef> {
    version_descriptor_pre_state_refs(row)
}

fn version_ref_tombstone_refs(row: &VersionAdminRow) -> Vec<ResolvedRowRef> {
    version_ref_pre_state_refs(row)
}

fn version_admin_id_from_payload_map(
    payload: &BTreeMap<String, Value>,
) -> Result<String, WriteResolveError> {
    payload
        .get("id")
        .and_then(text_from_value)
        .ok_or_else(|| WriteResolveError {
            message: "public version insert requires column 'id'".to_string(),
        })
}

fn version_admin_required_text_from_payload_map(
    payload: &BTreeMap<String, Value>,
    key: &str,
) -> Result<String, WriteResolveError> {
    let value = payload
        .get(key)
        .and_then(text_from_value)
        .ok_or_else(|| WriteResolveError {
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

fn version_ref_row(id: &str, commit_id: &str) -> PlannedStateRow {
    let mut values = BTreeMap::new();
    values.insert("entity_id".to_string(), Value::Text(id.to_string()));
    values.insert(
        "schema_key".to_string(),
        Value::Text(version_ref_schema_key().to_string()),
    );
    values.insert(
        "file_id".to_string(),
        Value::Text(version_ref_file_id().to_string()),
    );
    values.insert(
        "plugin_key".to_string(),
        Value::Text(version_ref_plugin_key().to_string()),
    );
    values.insert(
        "schema_version".to_string(),
        Value::Text(version_ref_schema_version().to_string()),
    );
    values.insert(
        "snapshot_content".to_string(),
        Value::Text(version_ref_snapshot_content(id, commit_id)),
    );
    values.insert(
        "version_id".to_string(),
        Value::Text(GLOBAL_VERSION_ID.to_string()),
    );
    PlannedStateRow {
        entity_id: id.to_string(),
        schema_key: version_ref_schema_key().to_string(),
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

fn version_ref_tombstone_row(id: &str) -> PlannedStateRow {
    let mut row = version_ref_row(id, "deleted");
    row.values.remove("snapshot_content");
    row.tombstone = true;
    row
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
    authoritative_pre_state_rows: Vec<PlannedStateRow>,
    intended_post_state: Vec<PlannedStateRow>,
    tombstones: Vec<ResolvedRowRef>,
    lineage: Vec<RowLineage>,
) -> ResolvedWritePlan {
    let builder = ResolvedWritePartitionBuilder {
        authoritative_pre_state,
        authoritative_pre_state_rows,
        intended_post_state,
        tombstones,
        lineage,
        filesystem_state: Default::default(),
    };
    builder
        .into_partition(execution_mode)
        .map(ResolvedWritePlan::from_partition)
        .unwrap_or_else(|| ResolvedWritePlan::from_partitions(Vec::new()))
}

fn planned_state_row_identity(row: &PlannedStateRow) -> (String, String, Option<String>) {
    (
        row.entity_id.clone(),
        row.schema_key.clone(),
        row.version_id.clone(),
    )
}

fn planned_state_rows_equivalent(left: &PlannedStateRow, right: &PlannedStateRow) -> bool {
    left.entity_id == right.entity_id
        && left.schema_key == right.schema_key
        && left.version_id == right.version_id
        && left.tombstone == right.tombstone
        && left.values == right.values
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
        ScopeProof::FiniteVersionSet(version_ids) if version_ids.is_empty() => {
            Err(WriteResolveError {
                message: "public write resolver requires a concrete version_id".to_string(),
            })
        }
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
    resolved.partitions.retain(|partition| {
        !partition.intended_post_state.is_empty() || !partition.filesystem_state.files.is_empty()
    });
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
        Vec::new(),
    )
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
        MutationPayload::InsertRows(payloads) => match payloads.as_slice() {
            [payload] => Ok(payload.clone()),
            _ => Err(WriteResolveError {
                message: "public resolver expected a single-row payload".to_string(),
            }),
        },
        MutationPayload::UpdatePatch(payload) => Ok(payload.clone()),
        MutationPayload::Tombstone => Ok(Default::default()),
    }
}

fn payload_text_value(planned_write: &PlannedWrite, key: &str) -> Option<String> {
    match &planned_write.command.payload {
        MutationPayload::InsertRows(payloads) => {
            let mut values = payloads
                .iter()
                .filter_map(|payload| match payload.get(key) {
                    Some(Value::Text(value)) => Some(value.clone()),
                    _ => None,
                });
            let first = values.next()?;
            values.all(|candidate| candidate == first).then_some(first)
        }
        MutationPayload::UpdatePatch(payload) => match payload.get(key) {
            Some(Value::Text(value)) => Some(value.clone()),
            _ => None,
        },
        MutationPayload::Tombstone => None,
    }
}

fn payload_maps(
    planned_write: &PlannedWrite,
) -> Result<Vec<BTreeMap<String, Value>>, WriteResolveError> {
    match &planned_write.command.payload {
        MutationPayload::InsertRows(payloads) => Ok(payloads.clone()),
        MutationPayload::UpdatePatch(_) | MutationPayload::Tombstone => Err(WriteResolveError {
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

fn bool_from_value(value: &Value) -> Option<bool> {
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

fn write_resolve_backend_error(error: crate::LixError) -> WriteResolveError {
    WriteResolveError {
        message: error.description,
    }
}

fn write_resolve_state_assignments_error(error: StateAssignmentsError) -> WriteResolveError {
    WriteResolveError {
        message: error.message,
    }
}

fn write_resolve_filesystem_assignments_error(
    error: FilesystemAssignmentsError,
) -> WriteResolveError {
    WriteResolveError {
        message: error.message,
    }
}

fn write_resolve_filesystem_planning_error(error: FilesystemPlanningError) -> WriteResolveError {
    WriteResolveError {
        message: error.message,
    }
}
