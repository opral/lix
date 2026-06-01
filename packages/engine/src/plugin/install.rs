//! Plugin install write helpers.
//!
//! This module owns plugin archive parsing, registered-schema staging, and the
//! prepared write construction needed to install a plugin into the engine.

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde_json::{json, Value as JsonValue};

use crate::catalog::{ResolvedRelation, SurfaceRegistry};
use crate::common::stable_content_fingerprint_hex;
use crate::common::{NormalizedDirectoryPath, ParsedFilePath};
use crate::plugin::{
    parse_plugin_archive_for_install, plugin_storage_archive_file_id, plugin_storage_archive_path,
    ParsedPluginArchive, PLUGIN_STORAGE_ROOT_DIRECTORY_PATH,
};
use crate::schema::{schema_key_from_definition, validate_lix_schema_definition};
use crate::sql::{
    ChangeBatch, CommitPreconditions, ExpectedHead, IdempotencyKey, OptionalTextPatch, PlanEffects,
    PlannedFilesystemDescriptor, PlannedFilesystemFile, PlannedFilesystemState, PlannedStateRow,
    PreparedWriteOperationKind, PreparedWriteStatementKind, PublicChange, ResultContract,
    SchemaLiveTableRequirement, SemanticEffect, WriteDiagnosticContext, WriteLane, WriteMode,
};
use crate::streams::{
    state_commit_stream_changes_from_changes, StateCommitStreamOperation,
    StateCommitStreamRuntimeMetadata,
};
use crate::transaction::{
    PreparedPublicSurfaceRegistryEffect, PreparedPublicSurfaceRegistryMutation,
    PreparedPublicWrite, PreparedPublicWriteContract, PreparedPublicWriteExecution,
    PreparedPublicWriteMaterialization, PreparedPublicWritePlanArtifact,
    PreparedResolvedWritePartition, PreparedResolvedWritePlan, PreparedWriteArtifact,
    PreparedWriteFunctionBindings, PreparedWriteStatement,
};
use crate::{LixError, Value};

use crate::transaction::WriteCommand;
const REGISTERED_SCHEMA_STORAGE_SCHEMA_KEY: &str = "lix_registered_schema";
const FILESYSTEM_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const FILESYSTEM_BINARY_BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";

#[derive(Clone)]
pub(crate) struct PluginInstallWriteContext {
    function_bindings: PreparedWriteFunctionBindings,
    public_surface_registry: SurfaceRegistry,
    target_branch_id: String,
    active_account_ids: Vec<String>,
    origin_key: Option<String>,
}

impl PluginInstallWriteContext {
    pub(crate) fn new(
        function_bindings: PreparedWriteFunctionBindings,
        public_surface_registry: SurfaceRegistry,
        target_branch_id: impl Into<String>,
        active_account_ids: Vec<String>,
        origin_key: Option<String>,
    ) -> Self {
        Self {
            function_bindings,
            public_surface_registry,
            target_branch_id: target_branch_id.into(),
            active_account_ids,
            origin_key,
        }
    }

    fn target_branch_id(&self) -> &str {
        &self.target_branch_id
    }
}

#[async_trait(?Send)]
pub(crate) trait PluginInstallWriteExecutor {
    fn plugin_install_write_context(&self) -> PluginInstallWriteContext;

    fn stage_prepared_write_statement(&mut self, statement: WriteCommand) -> Result<(), LixError>;

    async fn resolve_directory_id(
        &mut self,
        path: &NormalizedDirectoryPath,
    ) -> Result<Option<String>, LixError>;
}

pub(crate) async fn install_plugin_archive_with_writer(
    archive_bytes: &[u8],
    executor: &mut dyn PluginInstallWriteExecutor,
) -> Result<(), LixError> {
    let parsed = parse_plugin_archive_for_install(archive_bytes)?;
    install_plugin_with_writer(executor, &parsed, archive_bytes).await
}

pub(crate) fn prepare_registered_schema_write_statement(
    schema: &JsonValue,
    context: &PluginInstallWriteContext,
) -> Result<WriteCommand, LixError> {
    prepare_registered_schema_write_statement_from_schemas(std::slice::from_ref(schema), context)
}

async fn install_plugin_with_writer(
    executor: &mut dyn PluginInstallWriteExecutor,
    parsed: &ParsedPluginArchive,
    archive_bytes: &[u8],
) -> Result<(), LixError> {
    let plugin_install_context = executor.plugin_install_write_context();

    if !parsed.schemas.is_empty() {
        executor.stage_prepared_write_statement(
            prepare_registered_schema_write_statement_from_schemas(
                &parsed.schemas,
                &plugin_install_context,
            )?,
        )?;
    }

    let plugin_root =
        NormalizedDirectoryPath::from_normalized(PLUGIN_STORAGE_ROOT_DIRECTORY_PATH.to_string());
    let plugin_directory_id = executor
        .resolve_directory_id(&plugin_root)
        .await?
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "plugin storage directory '{}' is missing",
                    PLUGIN_STORAGE_ROOT_DIRECTORY_PATH
                ),
            )
        })?;
    executor.stage_prepared_write_statement(prepare_plugin_archive_write_statement(
        parsed,
        archive_bytes,
        &plugin_directory_id,
        &plugin_install_context,
    )?)?;

    Ok(())
}

#[derive(Clone)]
struct RegisteredSchemaRowSpec {
    entity_pk: String,
    registered_schema_key: String,
    snapshot: JsonValue,
    schema_json: JsonValue,
}

fn prepare_registered_schema_write_statement_from_schemas(
    schemas: &[JsonValue],
    context: &PluginInstallWriteContext,
) -> Result<WriteCommand, LixError> {
    let target = require_resolved_surface(
        &context.public_surface_registry,
        "lix_registered_schema_by_branch",
    )?;
    let schema_rows = schemas
        .iter()
        .map(registered_schema_row_spec_from_json)
        .collect::<Result<Vec<_>, _>>()?;
    let intended_post_state = schema_rows
        .iter()
        .map(|row| registered_schema_planned_row(row, context.target_branch_id()))
        .collect::<Vec<_>>();
    let changes = schema_rows
        .iter()
        .map(|row| PublicChange {
            entity_pk: row.entity_pk.clone(),
            schema_key: REGISTERED_SCHEMA_STORAGE_SCHEMA_KEY.to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some(row.snapshot.to_string()),
            metadata: None,
            branch_id: context.target_branch_id().to_string(),
            origin_key: context.origin_key.clone(),
        })
        .collect::<Vec<_>>();
    let schema_live_table_requirements = schema_rows
        .iter()
        .map(|row| SchemaLiveTableRequirement {
            schema_key: row.registered_schema_key.clone(),
            schema_definition: Some(row.schema_json.clone()),
        })
        .collect::<Vec<_>>();

    prepare_public_tracked_write_statement(
        context,
        target,
        "lix_registered_schema_by_branch",
        intended_post_state,
        PlannedFilesystemState::default(),
        changes,
        schema_live_table_requirements,
        PreparedPublicSurfaceRegistryEffect::ApplyMutations(
            schema_rows
                .iter()
                .map(
                    |row| PreparedPublicSurfaceRegistryMutation::UpsertRegisteredSchemaSnapshot {
                        snapshot: row.snapshot.clone(),
                    },
                )
                .collect(),
        ),
        "semantic.register_schema",
    )
}

fn prepare_plugin_archive_write_statement(
    parsed: &ParsedPluginArchive,
    archive_bytes: &[u8],
    plugin_directory_id: &str,
    context: &PluginInstallWriteContext,
) -> Result<WriteCommand, LixError> {
    let target = require_resolved_surface(&context.public_surface_registry, "lix_file_by_branch")?;
    let archive_id = plugin_storage_archive_file_id(parsed.manifest.key.as_str());
    let archive_path = plugin_storage_archive_path(parsed.manifest.key.as_str())?;
    let parsed_path = ParsedFilePath::try_from_path(&archive_path)?;
    let descriptor = PlannedFilesystemDescriptor {
        directory_id: plugin_directory_id.to_string(),
        name: parsed_path.name.clone(),
        metadata: None,
    };
    let target_branch_id = context.target_branch_id();
    let filesystem_state = PlannedFilesystemState {
        files: [(
            (archive_id.clone(), target_branch_id.to_string()),
            PlannedFilesystemFile {
                file_id: archive_id.clone(),
                branch_id: target_branch_id.to_string(),
                untracked: false,
                descriptor: Some(descriptor.clone()),
                metadata_patch: OptionalTextPatch::Unchanged,
                data: Some(archive_bytes.to_vec()),
                deleted: false,
            },
        )]
        .into_iter()
        .collect(),
    };
    let intended_post_state = vec![
        plugin_archive_file_descriptor_row(&archive_id, target_branch_id, &descriptor),
        plugin_archive_binary_blob_ref_row(&archive_id, target_branch_id, archive_bytes)?,
    ];
    let changes = intended_post_state
        .iter()
        .map(planned_row_to_public_change)
        .collect::<Result<Vec<_>, _>>()?;

    prepare_public_tracked_write_statement(
        context,
        target,
        "lix_file_by_branch",
        intended_post_state,
        filesystem_state,
        changes,
        Vec::new(),
        PreparedPublicSurfaceRegistryEffect::None,
        "semantic.install_plugin_archive",
    )
}

fn registered_schema_row_spec_from_json(
    schema: &JsonValue,
) -> Result<RegisteredSchemaRowSpec, LixError> {
    validate_lix_schema_definition(schema)?;
    let schema_key = schema_key_from_definition(schema)?;
    Ok(RegisteredSchemaRowSpec {
        entity_pk: schema_key.entity_pk(),
        registered_schema_key: schema_key.schema_key,
        snapshot: json!({ "value": schema }),
        schema_json: schema.clone(),
    })
}

fn registered_schema_planned_row(
    row: &RegisteredSchemaRowSpec,
    target_branch_id: &str,
) -> PlannedStateRow {
    let mut values = BTreeMap::new();
    values.insert("entity_pk".to_string(), Value::Text(row.entity_pk.clone()));
    values.insert(
        "schema_key".to_string(),
        Value::Text(REGISTERED_SCHEMA_STORAGE_SCHEMA_KEY.to_string()),
    );
    values.insert("file_id".to_string(), Value::Null);
    values.insert("plugin_key".to_string(), Value::Null);
    values.insert(
        "snapshot_content".to_string(),
        Value::Json(row.snapshot.clone()),
    );
    values.insert(
        "branch_id".to_string(),
        Value::Text(target_branch_id.to_string()),
    );
    PlannedStateRow {
        entity_pk: row.entity_pk.clone(),
        schema_key: REGISTERED_SCHEMA_STORAGE_SCHEMA_KEY.to_string(),
        branch_id: Some(target_branch_id.to_string()),
        values,
        origin_key: None,
        tombstone: false,
    }
}

fn plugin_archive_file_descriptor_row(
    archive_id: &str,
    target_branch_id: &str,
    descriptor: &PlannedFilesystemDescriptor,
) -> PlannedStateRow {
    let snapshot_content = json!({
        "id": archive_id,
        "directory_id": descriptor.directory_id,
        "name": descriptor.name,
    })
    .to_string();
    let mut values = BTreeMap::new();
    values.insert("entity_pk".to_string(), Value::Text(archive_id.to_string()));
    values.insert(
        "schema_key".to_string(),
        Value::Text(FILESYSTEM_DESCRIPTOR_SCHEMA_KEY.to_string()),
    );
    values.insert("file_id".to_string(), Value::Null);
    values.insert("plugin_key".to_string(), Value::Null);
    values.insert(
        "snapshot_content".to_string(),
        Value::Text(snapshot_content),
    );
    values.insert(
        "branch_id".to_string(),
        Value::Text(target_branch_id.to_string()),
    );
    PlannedStateRow {
        entity_pk: archive_id.to_string(),
        schema_key: FILESYSTEM_DESCRIPTOR_SCHEMA_KEY.to_string(),
        branch_id: Some(target_branch_id.to_string()),
        values,
        origin_key: None,
        tombstone: false,
    }
}

fn plugin_archive_binary_blob_ref_row(
    archive_id: &str,
    target_branch_id: &str,
    archive_bytes: &[u8],
) -> Result<PlannedStateRow, LixError> {
    let size_bytes = u64::try_from(archive_bytes.len()).map_err(|_| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "plugin archive '{}' exceeds supported size range",
                archive_id
            ),
        )
    })?;
    let snapshot_content = json!({
        "id": archive_id,
        "blob_hash": stable_content_fingerprint_hex(archive_bytes),
        "size_bytes": size_bytes,
    })
    .to_string();
    let mut values = BTreeMap::new();
    values.insert("entity_pk".to_string(), Value::Text(archive_id.to_string()));
    values.insert(
        "schema_key".to_string(),
        Value::Text(FILESYSTEM_BINARY_BLOB_REF_SCHEMA_KEY.to_string()),
    );
    values.insert("file_id".to_string(), Value::Text(archive_id.to_string()));
    values.insert("plugin_key".to_string(), Value::Null);
    values.insert(
        "snapshot_content".to_string(),
        Value::Text(snapshot_content),
    );
    values.insert(
        "branch_id".to_string(),
        Value::Text(target_branch_id.to_string()),
    );
    Ok(PlannedStateRow {
        entity_pk: archive_id.to_string(),
        schema_key: FILESYSTEM_BINARY_BLOB_REF_SCHEMA_KEY.to_string(),
        branch_id: Some(target_branch_id.to_string()),
        values,
        origin_key: None,
        tombstone: false,
    })
}

fn prepare_public_tracked_write_statement(
    context: &PluginInstallWriteContext,
    target: ResolvedRelation,
    relation_name: &str,
    intended_post_state: Vec<PlannedStateRow>,
    filesystem_state: PlannedFilesystemState,
    changes: Vec<PublicChange>,
    schema_live_table_requirements: Vec<SchemaLiveTableRequirement>,
    public_surface_registry_effect: PreparedPublicSurfaceRegistryEffect,
    idempotency_purpose: &str,
) -> Result<WriteCommand, LixError> {
    let semantic_effects =
        semantic_plan_effects_from_changes(&changes, context.origin_key.as_deref())?;
    let write_payload = json!({
        "rows": intended_post_state.iter().map(summarize_planned_row).collect::<Vec<_>>(),
        "changes": changes.iter().map(summarize_change).collect::<Vec<_>>(),
        "filesystem_files": filesystem_state.files.keys().cloned().collect::<Vec<_>>(),
    });
    WriteCommand::build(
        PreparedWriteStatement {
            statement_kind: PreparedWriteStatementKind::Write,
            result_contract: ResultContract::DmlNoReturning,
            artifact: PreparedWriteArtifact::PublicWrite(PreparedPublicWrite {
                contract: PreparedPublicWriteContract {
                    operation_kind: PreparedWriteOperationKind::Insert,
                    target,
                    on_conflict_action: None,
                    requested_branch_id: Some(context.target_branch_id().to_string()),
                    active_account_ids: context.active_account_ids.clone(),
                    origin_key: context.origin_key.clone(),
                    resolved_write_plan: Some(PreparedResolvedWritePlan {
                        partitions: vec![PreparedResolvedWritePartition {
                            execution_mode: WriteMode::Tracked,
                            authoritative_pre_state_rows: Vec::new(),
                            intended_post_state,
                            filesystem_state,
                        }],
                    }),
                },
                execution: PreparedPublicWritePlanArtifact::Materialize(
                    PreparedPublicWriteMaterialization {
                        partitions: vec![PreparedPublicWriteExecution {
                            execution_mode: WriteMode::Tracked,
                            intended_post_state: Vec::new(),
                            schema_live_table_requirements,
                            change_batch: Some(ChangeBatch {
                                changes: changes.clone(),
                                write_lane: WriteLane::GlobalAdmin,
                                origin_key: context.origin_key.clone(),
                                semantic_effects: semantic_effect_markers_from_changes(&changes),
                            }),
                            create_preconditions: Some(CommitPreconditions {
                                write_lane: WriteLane::GlobalAdmin,
                                expected_head: ExpectedHead::CurrentHead,
                                idempotency_key: semantic_idempotency_key(
                                    idempotency_purpose,
                                    &write_payload,
                                )?,
                            }),
                            semantic_effects,
                            persist_filesystem_payloads_before_write: false,
                        }],
                    },
                ),
            }),
            diagnostic_context: WriteDiagnosticContext::new(vec![relation_name.to_string()]),
            public_surface_registry_effect,
        },
        &context.function_bindings,
    )
}

fn semantic_plan_effects_from_changes(
    changes: &[PublicChange],
    origin_key: Option<&str>,
) -> Result<PlanEffects, LixError> {
    Ok(PlanEffects {
        state_commit_stream_changes: state_commit_stream_changes_from_changes(
            changes,
            StateCommitStreamOperation::Insert,
            StateCommitStreamRuntimeMetadata::from_runtime_origin_key(origin_key),
        )?,
        ..PlanEffects::default()
    })
}

fn semantic_effect_markers_from_changes(changes: &[PublicChange]) -> Vec<SemanticEffect> {
    changes
        .iter()
        .map(|change| SemanticEffect {
            effect_key: "state.upsert".to_string(),
            target: format!(
                "{}:{}@{}",
                change.schema_key, change.entity_pk, change.branch_id
            ),
        })
        .collect()
}

fn planned_row_to_public_change(row: &PlannedStateRow) -> Result<PublicChange, LixError> {
    Ok(PublicChange {
        entity_pk: row.entity_pk.clone(),
        schema_key: row.schema_key.clone(),
        file_id: planned_row_text_value(row, "file_id"),
        plugin_key: planned_row_text_value(row, "plugin_key"),
        snapshot_content: if row.tombstone {
            None
        } else {
            planned_row_json_text_value(row, "snapshot_content")
        },
        metadata: planned_row_json_text_value(row, "metadata"),
        branch_id: row
            .branch_id
            .clone()
            .or_else(|| planned_row_text_value(row, "branch_id"))
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "semantic tracked write requires a concrete branch_id",
                )
            })?,
        origin_key: row.origin_key.clone(),
    })
}

fn planned_row_text_value(row: &PlannedStateRow, key: &str) -> Option<String> {
    match row.values.get(key) {
        Some(Value::Text(value)) => Some(value.clone()),
        Some(Value::Integer(value)) => Some(value.to_string()),
        Some(Value::Boolean(value)) => Some(value.to_string()),
        Some(Value::Real(value)) => Some(value.to_string()),
        _ => None,
    }
}

fn planned_row_json_text_value(row: &PlannedStateRow, key: &str) -> Option<String> {
    match row.values.get(key) {
        Some(Value::Json(value)) => Some(value.to_string()),
        _ => planned_row_text_value(row, key),
    }
}

fn semantic_idempotency_key(
    purpose: &str,
    payload: &JsonValue,
) -> Result<IdempotencyKey, LixError> {
    let bytes = serde_json::to_vec(payload).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("semantic idempotency payload serialization failed: {error}"),
        )
    })?;
    Ok(IdempotencyKey(
        json!({
            "purpose": purpose,
            "fingerprint": stable_content_fingerprint_hex(&bytes),
        })
        .to_string(),
    ))
}

fn summarize_change(change: &PublicChange) -> JsonValue {
    json!({
        "entity_pk": change.entity_pk,
        "schema_key": change.schema_key,
        "file_id": change.file_id,
        "plugin_key": change.plugin_key,
        "branch_id": change.branch_id,
        "origin_key": change.origin_key,
        "snapshot_content": change.snapshot_content.as_ref().map(|snapshot| {
            stable_content_fingerprint_hex(snapshot.as_bytes())
        }),
    })
}

fn summarize_planned_row(row: &PlannedStateRow) -> JsonValue {
    json!({
        "entity_pk": row.entity_pk,
        "schema_key": row.schema_key,
        "branch_id": row.branch_id,
        "tombstone": row.tombstone,
        "values": row
            .values
            .iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    match value {
                        Value::Null => json!({ "kind": "null" }),
                        Value::Text(text) => json!({
                            "kind": "text",
                            "sha256": stable_content_fingerprint_hex(text.as_bytes()),
                            "len": text.len(),
                        }),
                        Value::Json(value) => {
                            let encoded = value.to_string();
                            json!({
                                "kind": "json",
                                "sha256": stable_content_fingerprint_hex(encoded.as_bytes()),
                                "len": encoded.len(),
                            })
                        }
                        Value::Blob(bytes) => json!({
                            "kind": "blob",
                            "sha256": stable_content_fingerprint_hex(bytes),
                            "len": bytes.len(),
                        }),
                        Value::Integer(value) => json!({ "kind": "integer", "value": value }),
                        Value::Real(value) => json!({ "kind": "real", "value": value }),
                        Value::Boolean(value) => json!({ "kind": "boolean", "value": value }),
                    },
                )
            })
            .collect::<serde_json::Map<_, _>>(),
    })
}

fn require_resolved_surface(
    public_surface_registry: &SurfaceRegistry,
    relation_name: &str,
) -> Result<ResolvedRelation, LixError> {
    public_surface_registry
        .bind_relation_name(relation_name)
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("public surface '{relation_name}' is not registered"),
            )
        })
}
