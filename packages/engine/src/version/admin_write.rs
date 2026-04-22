use std::collections::{BTreeMap, BTreeSet};

use crate::catalog::ResolvedRelation;
use crate::sql::{
    build_change_batches, build_public_write_execution, derive_commit_preconditions, ChangeError,
    MutationPayload, PlannedStateRow, PlannedWrite, PlannedWriteCommand,
    PreparedWriteOperationKind, PreparedWriteStatementKind, ResultContract, RowLineage,
    SchemaProof, ScopeProof, StateSourceKind, StatementContext, TargetSetProof,
    WriteDiagnosticContext, WriteMode, WriteModeRequest, WriteOperationKind, WriteSelector,
};
use crate::sql::{ResolvedWritePartition, ResolvedWritePlan};
use crate::transaction::{
    ensure_function_bindings_for_write_scope, prepared_write_function_bindings_for_execution,
    stage_prepared_write_statement, PreparedPublicSurfaceRegistryEffect, PreparedPublicWrite,
    PreparedPublicWriteContract, PreparedPublicWriteExecution, PreparedPublicWriteMaterialization,
    PreparedPublicWritePlanArtifact, PreparedResolvedWritePartition, PreparedResolvedWritePlan,
    PreparedWriteArtifact, PreparedWriteStatement, WriteCommand,
};
use crate::version::{
    load_version_descriptor_with_pending_overlay, load_version_head_commit_id_with_pending_overlay,
    version_descriptor_file_id, version_descriptor_plugin_key, version_descriptor_schema_key,
    version_descriptor_schema_version, version_descriptor_snapshot_content, version_ref_file_id,
    version_ref_plugin_key, version_ref_schema_key, version_ref_schema_version,
    version_ref_snapshot_content, GLOBAL_VERSION_ID,
};
use crate::{LixError, SessionTransaction, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExistingVersionAdminState {
    descriptor_change_id: Option<String>,
    head_commit_id: Option<String>,
}

pub(crate) async fn stage_create_version_insert(
    tx: &mut SessionTransaction<'_>,
    version_id: &str,
    name: &str,
    hidden: bool,
    commit_id: &str,
) -> Result<(), LixError> {
    let existing = load_existing_version_admin_state(tx, version_id).await?;
    let payload = BTreeMap::from([
        ("id".to_string(), Value::Text(version_id.to_string())),
        ("name".to_string(), Value::Text(name.to_string())),
        ("hidden".to_string(), Value::Boolean(hidden)),
        ("commit_id".to_string(), Value::Text(commit_id.to_string())),
    ]);
    let mut tracked_partition = tracked_version_descriptor_partition(version_id, name, hidden);
    let mut untracked_partition = untracked_version_ref_partition(version_id, commit_id);
    apply_existing_version_pre_state(
        &mut tracked_partition,
        &mut untracked_partition,
        existing.as_ref(),
        version_id,
    );
    stage_version_admin_write(
        tx,
        WriteOperationKind::Insert,
        WriteSelector::default(),
        MutationPayload::InsertRows(vec![payload]),
        ResolvedWritePlan::from_partitions(vec![tracked_partition, untracked_partition]),
        version_target_proof(version_id),
    )
    .await
}

pub(crate) async fn stage_update_version_head(
    tx: &mut SessionTransaction<'_>,
    version_id: &str,
    commit_id: &str,
) -> Result<(), LixError> {
    let existing = load_existing_version_admin_state(tx, version_id).await?;
    let mut exact_filters = BTreeMap::new();
    exact_filters.insert("id".to_string(), Value::Text(version_id.to_string()));
    let payload = BTreeMap::from([("commit_id".to_string(), Value::Text(commit_id.to_string()))]);
    let mut partition = untracked_version_ref_partition(version_id, commit_id);
    if let Some(existing) = existing.as_ref() {
        partition
            .authoritative_pre_state
            .extend(version_ref_pre_state_refs(
                version_id,
                existing.head_commit_id.as_ref(),
            ));
    }
    stage_version_admin_write(
        tx,
        WriteOperationKind::Update,
        WriteSelector {
            exact_filters,
            exact_only: true,
            ..WriteSelector::default()
        },
        MutationPayload::UpdatePatch(payload),
        ResolvedWritePlan::from_partitions(vec![partition]),
        version_target_proof(version_id),
    )
    .await
}

async fn stage_version_admin_write(
    tx: &mut SessionTransaction<'_>,
    operation_kind: WriteOperationKind,
    selector: WriteSelector,
    payload: MutationPayload,
    resolved_write_plan: ResolvedWritePlan,
    target_set_proof: TargetSetProof,
) -> Result<(), LixError> {
    ensure_version_admin_function_bindings(tx).await?;
    let target = version_relation(&tx.context.public_surface_registry)?;
    let statement_context = StatementContext {
        origin_key: tx.context.origin_key.clone(),
        requested_version_id: Some(tx.context.active_version_id.clone()),
        active_account_ids: tx.context.active_account_ids.clone(),
        ..StatementContext::default()
    };
    let planned_write = PlannedWrite {
        command: PlannedWriteCommand {
            operation_kind,
            target: target.clone(),
            selector,
            payload,
            on_conflict: None,
            requested_mode: WriteModeRequest::Auto,
            bound_parameters: Vec::new(),
            statement_context,
        },
        filesystem_write_intent: None,
        scope_proof: ScopeProof::GlobalAdmin,
        schema_proof: SchemaProof::Exact(BTreeSet::from([
            version_descriptor_schema_key().to_string(),
            version_ref_schema_key().to_string(),
        ])),
        target_set_proof: Some(target_set_proof),
        state_source: StateSourceKind::AuthoritativeCommitted,
        resolved_write_plan: Some(resolved_write_plan.clone()),
        commit_preconditions: Vec::new(),
        residual_execution_predicates: Vec::new(),
        backend_rejections: Vec::new(),
    };
    let change_batches = build_change_batches(&planned_write).map_err(change_error)?;
    let commit_preconditions = derive_commit_preconditions(&planned_write).map_err(change_error)?;
    let execution =
        build_public_write_execution(&planned_write, &change_batches, &commit_preconditions)?
            .ok_or_else(|| LixError::unknown("typed version admin write did not materialize"))?;
    let prepared = PreparedWriteStatement {
        statement_kind: PreparedWriteStatementKind::Write,
        result_contract: ResultContract::DmlNoReturning,
        artifact: PreparedWriteArtifact::PublicWrite(PreparedPublicWrite {
            contract: PreparedPublicWriteContract {
                operation_kind: prepared_operation_kind(operation_kind),
                target,
                on_conflict_action: None,
                requested_version_id: Some(tx.context.active_version_id.clone()),
                active_account_ids: tx.context.active_account_ids.clone(),
                origin_key: tx.context.origin_key.clone(),
                resolved_write_plan: Some(prepared_resolved_write_plan(&resolved_write_plan)),
            },
            execution: prepared_public_write_execution(execution),
        }),
        diagnostic_context: WriteDiagnosticContext::new(vec!["lix_version".to_string()]),
        public_surface_registry_effect: PreparedPublicSurfaceRegistryEffect::None,
    };
    let function_bindings = prepared_write_function_bindings_for_execution(
        tx.context
            .function_bindings()
            .expect("version admin write should prepare function bindings"),
    );
    let command = WriteCommand::build(prepared, &function_bindings)?;
    stage_prepared_write_statement(tx.write_transaction_mut()?, command)
}

async fn ensure_version_admin_function_bindings(
    tx: &mut SessionTransaction<'_>,
) -> Result<(), LixError> {
    let session_host = tx.session_host();
    let execution_context = crate::session::SessionExecutionContext::new(session_host);
    let write_transaction = tx
        .write_transaction
        .as_mut()
        .ok_or_else(|| LixError::unknown("transaction is no longer active"))?;
    ensure_function_bindings_for_write_scope(
        &execution_context,
        write_transaction.backend_transaction_mut()?,
        &mut tx.context,
    )
    .await
}

async fn load_existing_version_admin_state(
    tx: &mut SessionTransaction<'_>,
    version_id: &str,
) -> Result<Option<ExistingVersionAdminState>, LixError> {
    let pending_overlay = tx
        .write_transaction
        .as_ref()
        .map(|write_transaction| write_transaction.buffered_write_pending_write_overlay())
        .transpose()?
        .flatten();
    let pending_overlay = pending_overlay
        .as_ref()
        .map(|overlay| overlay as &dyn crate::transaction::PendingOverlay);
    let mut executor = crate::backend::transaction_backend_view(tx.backend_transaction_mut()?);
    let descriptor =
        load_version_descriptor_with_pending_overlay(&mut executor, pending_overlay, version_id)
            .await?;
    let Some(descriptor) = descriptor else {
        return Ok(None);
    };
    let head_commit_id = load_version_head_commit_id_with_pending_overlay(
        &mut executor,
        pending_overlay,
        version_id,
    )
    .await?;
    Ok(Some(ExistingVersionAdminState {
        descriptor_change_id: descriptor.change_id,
        head_commit_id,
    }))
}

fn version_relation(
    registry: &crate::catalog::SurfaceRegistry,
) -> Result<ResolvedRelation, LixError> {
    registry
        .bind_relation_name("lix_version")
        .ok_or_else(|| LixError::unknown("lix_version surface should resolve"))
}

fn version_target_proof(version_id: &str) -> TargetSetProof {
    TargetSetProof::Exact(BTreeSet::from([version_id.to_string()]))
}

fn tracked_version_descriptor_partition(
    version_id: &str,
    name: &str,
    hidden: bool,
) -> ResolvedWritePartition {
    ResolvedWritePartition {
        execution_mode: WriteMode::Tracked,
        authoritative_pre_state: Vec::new(),
        authoritative_pre_state_rows: Vec::new(),
        intended_post_state: vec![version_descriptor_row(version_id, name, hidden)],
        tombstones: Vec::new(),
        lineage: vec![RowLineage {
            entity_id: version_id.to_string(),
            source_change_id: None,
            source_commit_id: None,
        }],
        target_write_lane: Some(crate::sql::WriteLane::GlobalAdmin),
        filesystem_state: crate::sql::PlannedFilesystemState::default(),
    }
}

fn untracked_version_ref_partition(version_id: &str, commit_id: &str) -> ResolvedWritePartition {
    ResolvedWritePartition {
        execution_mode: WriteMode::Untracked,
        authoritative_pre_state: Vec::new(),
        authoritative_pre_state_rows: Vec::new(),
        intended_post_state: vec![version_ref_row(version_id, commit_id)],
        tombstones: Vec::new(),
        lineage: vec![RowLineage {
            entity_id: version_id.to_string(),
            source_change_id: None,
            source_commit_id: None,
        }],
        target_write_lane: None,
        filesystem_state: crate::sql::PlannedFilesystemState::default(),
    }
}

fn apply_existing_version_pre_state(
    tracked_partition: &mut ResolvedWritePartition,
    untracked_partition: &mut ResolvedWritePartition,
    existing: Option<&ExistingVersionAdminState>,
    version_id: &str,
) {
    let Some(existing) = existing else {
        return;
    };
    tracked_partition
        .authoritative_pre_state
        .extend(version_descriptor_pre_state_refs(
            version_id,
            existing.descriptor_change_id.as_ref(),
        ));
    tracked_partition.lineage[0].source_change_id = existing.descriptor_change_id.clone();
    untracked_partition
        .authoritative_pre_state
        .extend(version_ref_pre_state_refs(
            version_id,
            existing.head_commit_id.as_ref(),
        ));
}

fn version_descriptor_pre_state_refs(
    version_id: &str,
    descriptor_change_id: Option<&String>,
) -> Vec<crate::sql::ResolvedRowRef> {
    vec![crate::sql::ResolvedRowRef {
        entity_id: version_id.to_string(),
        schema_key: version_descriptor_schema_key().to_string(),
        version_id: Some(GLOBAL_VERSION_ID.to_string()),
        source_change_id: descriptor_change_id.cloned(),
        source_commit_id: None,
    }]
}

fn version_ref_pre_state_refs(
    version_id: &str,
    head_commit_id: Option<&String>,
) -> Vec<crate::sql::ResolvedRowRef> {
    vec![crate::sql::ResolvedRowRef {
        entity_id: version_id.to_string(),
        schema_key: version_ref_schema_key().to_string(),
        version_id: Some(GLOBAL_VERSION_ID.to_string()),
        source_change_id: None,
        source_commit_id: head_commit_id.cloned(),
    }]
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
        version_descriptor_file_id()
            .map(|value| Value::Text(value.to_string()))
            .unwrap_or(Value::Null),
    );
    values.insert(
        "plugin_key".to_string(),
        version_descriptor_plugin_key()
            .map(|value| Value::Text(value.to_string()))
            .unwrap_or(Value::Null),
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
        origin_key: None,
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
        version_ref_file_id()
            .map(|value| Value::Text(value.to_string()))
            .unwrap_or(Value::Null),
    );
    values.insert(
        "plugin_key".to_string(),
        version_ref_plugin_key()
            .map(|value| Value::Text(value.to_string()))
            .unwrap_or(Value::Null),
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
        origin_key: None,
        tombstone: false,
    }
}

fn prepared_operation_kind(kind: WriteOperationKind) -> PreparedWriteOperationKind {
    match kind {
        WriteOperationKind::Insert => PreparedWriteOperationKind::Insert,
        WriteOperationKind::Update => PreparedWriteOperationKind::Update,
        WriteOperationKind::Delete => PreparedWriteOperationKind::Delete,
    }
}

fn prepared_public_write_execution(
    execution: crate::sql::PublicWritePhysicalPlan,
) -> PreparedPublicWritePlanArtifact {
    match execution {
        crate::sql::PublicWritePhysicalPlan::Noop => PreparedPublicWritePlanArtifact::Noop,
        crate::sql::PublicWritePhysicalPlan::Materialize(materialization) => {
            PreparedPublicWritePlanArtifact::Materialize(PreparedPublicWriteMaterialization {
                partitions: materialization
                    .partitions
                    .iter()
                    .map(|partition| match partition {
                        crate::sql::PublicWriteExecutionPartition::Tracked(tracked) => {
                            PreparedPublicWriteExecution {
                                execution_mode: WriteMode::Tracked,
                                intended_post_state: Vec::new(),
                                schema_live_table_requirements: tracked
                                    .schema_live_table_requirements
                                    .clone(),
                                change_batch: tracked.change_batch.clone(),
                                create_preconditions: Some(tracked.create_preconditions.clone()),
                                semantic_effects: tracked.semantic_effects.clone(),
                                persist_filesystem_payloads_before_write: false,
                            }
                        }
                        crate::sql::PublicWriteExecutionPartition::Untracked(untracked) => {
                            PreparedPublicWriteExecution {
                                execution_mode: WriteMode::Untracked,
                                intended_post_state: untracked.intended_post_state.clone(),
                                schema_live_table_requirements: Vec::new(),
                                change_batch: None,
                                create_preconditions: None,
                                semantic_effects: untracked.semantic_effects.clone(),
                                persist_filesystem_payloads_before_write: untracked
                                    .persist_filesystem_payloads_before_write,
                            }
                        }
                    })
                    .collect(),
            })
        }
    }
}

fn prepared_resolved_write_plan(resolved: &ResolvedWritePlan) -> PreparedResolvedWritePlan {
    PreparedResolvedWritePlan {
        partitions: resolved
            .partitions
            .iter()
            .map(|partition| PreparedResolvedWritePartition {
                execution_mode: partition.execution_mode,
                authoritative_pre_state_rows: partition.authoritative_pre_state_rows.clone(),
                intended_post_state: partition.intended_post_state.clone(),
                filesystem_state: partition.filesystem_state.clone(),
            })
            .collect(),
    }
}

fn change_error(error: ChangeError) -> LixError {
    LixError::new("LIX_ERROR_UNKNOWN", error.message)
}
