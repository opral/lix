use std::collections::{BTreeMap, BTreeSet};
use std::io::{Cursor, Read};
use std::path::{Component, Path};

use async_trait::async_trait;
use serde_json::{json, Value as JsonValue};
use zip::read::ZipArchive;

use crate::content_fingerprint::stable_content_fingerprint_hex;
use crate::contracts::artifacts::{
    CommitPreconditions, DomainChangeBatch, IdempotencyKey, OptionalTextPatch, PlanEffects,
    PlannedFilesystemDescriptor, PlannedFilesystemFile, PlannedFilesystemState, PlannedStateRow,
    PreparedPublicSurfaceRegistryEffect, PreparedPublicSurfaceRegistryMutation,
    PreparedPublicWriteArtifact, PreparedPublicWriteContract, PreparedPublicWriteExecutionArtifact,
    PreparedPublicWriteExecutionPartition, PreparedPublicWriteMaterialization,
    PreparedResolvedWritePartition, PreparedResolvedWritePlan, PreparedTrackedWriteExecution,
    PreparedWriteArtifact, PreparedWriteDiagnosticContext, PreparedWriteOperationKind,
    PreparedWriteStatementKind, PreparedWriteStep, PublicDomainChange, ResultContract,
    SchemaLiveTableRequirement, SemanticEffect, StateCommitStreamOperation, WriteLane, WriteMode,
};
use crate::contracts::plugin::{
    parse_plugin_manifest_json, plugin_storage_archive_file_id, plugin_storage_archive_path,
    PluginManifest, PLUGIN_STORAGE_ROOT_DIRECTORY_PATH,
};
use crate::contracts::state_commit_stream::{
    state_commit_stream_changes_from_domain_changes, StateCommitStreamRuntimeMetadata,
};
use crate::contracts::surface::{SurfaceBinding, SurfaceRegistry};
use crate::paths::filesystem::{NormalizedDirectoryPath, ParsedFilePath};
use crate::schema::{schema_key_from_definition, validate_lix_schema_definition};
use crate::version_artifacts::GLOBAL_VERSION_ID;
use crate::write_runtime::sql_adapter::PreparedWriteExecutionStep;
use crate::write_runtime::{PreparedWriteRuntimeState, WriteTransaction};
use crate::{LixError, Value};

const REGISTERED_SCHEMA_STORAGE_SCHEMA_KEY: &str = "lix_registered_schema";
const REGISTERED_SCHEMA_STORAGE_SCHEMA_VERSION: &str = "1";
const REGISTERED_SCHEMA_STORAGE_FILE_ID: &str = "lix";
const REGISTERED_SCHEMA_STORAGE_PLUGIN_KEY: &str = "lix";
const FILESYSTEM_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const FILESYSTEM_DESCRIPTOR_SCHEMA_VERSION: &str = "1";
const FILESYSTEM_DESCRIPTOR_FILE_ID: &str = "lix";
const FILESYSTEM_DESCRIPTOR_PLUGIN_KEY: &str = "lix";
const FILESYSTEM_BINARY_BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
const FILESYSTEM_BINARY_BLOB_REF_SCHEMA_VERSION: &str = "1";

struct ParsedPluginArchive {
    manifest: PluginManifest,
    wasm_bytes: Vec<u8>,
    schemas: Vec<ParsedSchema>,
}

struct ParsedSchema {
    schema_json: JsonValue,
}

#[derive(Clone)]
pub(crate) struct SemanticWriteContext {
    runtime_state: PreparedWriteRuntimeState,
    public_surface_registry: SurfaceRegistry,
    active_account_ids: Vec<String>,
    writer_key: Option<String>,
}

impl SemanticWriteContext {
    pub(crate) fn new(
        runtime_state: PreparedWriteRuntimeState,
        public_surface_registry: SurfaceRegistry,
        active_account_ids: Vec<String>,
        writer_key: Option<String>,
    ) -> Self {
        Self {
            runtime_state,
            public_surface_registry,
            active_account_ids,
            writer_key,
        }
    }
}

#[async_trait(?Send)]
pub(crate) trait PluginInstallWriteExecutor {
    fn semantic_write_context(&self) -> SemanticWriteContext;

    fn stage_prepared_write_step(
        &mut self,
        step: PreparedWriteExecutionStep,
    ) -> Result<(), LixError>;

    async fn resolve_directory_id(
        &mut self,
        path: &NormalizedDirectoryPath,
    ) -> Result<Option<String>, LixError>;
}

pub(crate) async fn install_plugin_archive_with_writer(
    archive_bytes: &[u8],
    executor: &mut dyn PluginInstallWriteExecutor,
) -> Result<(), LixError> {
    let parsed = parse_plugin_archive(archive_bytes)?;
    ensure_valid_wasm_binary(&parsed.wasm_bytes)?;
    install_plugin_with_writer(executor, &parsed, archive_bytes).await
}

pub(crate) fn prepare_registered_schema_write_step(
    schema: &JsonValue,
    context: &SemanticWriteContext,
) -> Result<PreparedWriteExecutionStep, LixError> {
    let parsed_schema = parsed_schema_from_json(schema)?;
    prepare_registered_schema_write_step_from_schemas(&[parsed_schema], context)
}

pub(crate) fn stage_prepared_write_step(
    transaction: &mut WriteTransaction<'_>,
    step: PreparedWriteExecutionStep,
) -> Result<(), LixError> {
    if !step.prepared().public_surface_registry_effect.is_none() {
        transaction.mark_public_surface_registry_refresh_pending();
    }
    let planned_write_delta = step.planned_write_delta().cloned().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "semantic write step must materialize a planned write delta",
        )
    })?;
    transaction.stage_planned_write_delta(planned_write_delta)
}

async fn install_plugin_with_writer(
    executor: &mut dyn PluginInstallWriteExecutor,
    parsed: &ParsedPluginArchive,
    archive_bytes: &[u8],
) -> Result<(), LixError> {
    let semantic_context = executor.semantic_write_context();

    if !parsed.schemas.is_empty() {
        executor.stage_prepared_write_step(prepare_registered_schema_write_step_from_schemas(
            &parsed.schemas,
            &semantic_context,
        )?)?;
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
    executor.stage_prepared_write_step(prepare_plugin_archive_write_step(
        parsed,
        archive_bytes,
        &plugin_directory_id,
        &semantic_context,
    )?)?;

    Ok(())
}

#[derive(Clone)]
struct RegisteredSchemaRowSpec {
    entity_id: String,
    registered_schema_key: String,
    snapshot: JsonValue,
    schema_json: JsonValue,
}

fn prepare_registered_schema_write_step_from_schemas(
    schemas: &[ParsedSchema],
    context: &SemanticWriteContext,
) -> Result<PreparedWriteExecutionStep, LixError> {
    let target = require_surface_binding(
        &context.public_surface_registry,
        "lix_registered_schema_by_version",
    )?;
    let schema_rows = schemas
        .iter()
        .map(registered_schema_row_spec_from_parsed)
        .collect::<Result<Vec<_>, _>>()?;
    let intended_post_state = schema_rows
        .iter()
        .map(registered_schema_planned_row)
        .collect::<Vec<_>>();
    let domain_changes = schema_rows
        .iter()
        .map(|row| PublicDomainChange {
            entity_id: row.entity_id.clone(),
            schema_key: REGISTERED_SCHEMA_STORAGE_SCHEMA_KEY.to_string(),
            schema_version: Some(REGISTERED_SCHEMA_STORAGE_SCHEMA_VERSION.to_string()),
            file_id: Some(REGISTERED_SCHEMA_STORAGE_FILE_ID.to_string()),
            plugin_key: Some(REGISTERED_SCHEMA_STORAGE_PLUGIN_KEY.to_string()),
            snapshot_content: Some(row.snapshot.to_string()),
            metadata: None,
            version_id: GLOBAL_VERSION_ID.to_string(),
            writer_key: context.writer_key.clone(),
        })
        .collect::<Vec<_>>();
    let schema_live_table_requirements = schema_rows
        .iter()
        .map(|row| SchemaLiveTableRequirement {
            schema_key: row.registered_schema_key.clone(),
            schema_definition: Some(row.schema_json.clone()),
        })
        .collect::<Vec<_>>();

    prepare_public_tracked_write_step(
        context,
        target,
        "lix_registered_schema_by_version",
        intended_post_state,
        PlannedFilesystemState::default(),
        domain_changes,
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

fn prepare_plugin_archive_write_step(
    parsed: &ParsedPluginArchive,
    archive_bytes: &[u8],
    plugin_directory_id: &str,
    context: &SemanticWriteContext,
) -> Result<PreparedWriteExecutionStep, LixError> {
    let target = require_surface_binding(&context.public_surface_registry, "lix_file_by_version")?;
    let archive_id = plugin_storage_archive_file_id(parsed.manifest.key.as_str());
    let archive_path = plugin_storage_archive_path(parsed.manifest.key.as_str())?;
    let parsed_path = ParsedFilePath::try_from_path(&archive_path)?;
    let descriptor = PlannedFilesystemDescriptor {
        directory_id: plugin_directory_id.to_string(),
        name: parsed_path.name.clone(),
        extension: parsed_path.extension.clone(),
        metadata: None,
        hidden: false,
    };
    let filesystem_state = PlannedFilesystemState {
        files: [(
            (archive_id.clone(), GLOBAL_VERSION_ID.to_string()),
            PlannedFilesystemFile {
                file_id: archive_id.clone(),
                version_id: GLOBAL_VERSION_ID.to_string(),
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
        plugin_archive_file_descriptor_row(&archive_id, &descriptor),
        plugin_archive_binary_blob_ref_row(&archive_id, archive_bytes)?,
    ];
    let domain_changes = intended_post_state
        .iter()
        .map(planned_row_to_public_domain_change)
        .collect::<Result<Vec<_>, _>>()?;

    prepare_public_tracked_write_step(
        context,
        target,
        "lix_file_by_version",
        intended_post_state,
        filesystem_state,
        domain_changes,
        Vec::new(),
        PreparedPublicSurfaceRegistryEffect::None,
        "semantic.install_plugin_archive",
    )
}

fn parsed_schema_from_json(schema: &JsonValue) -> Result<ParsedSchema, LixError> {
    validate_lix_schema_definition(schema)?;
    Ok(ParsedSchema {
        schema_json: schema.clone(),
    })
}

fn registered_schema_row_spec_from_parsed(
    schema: &ParsedSchema,
) -> Result<RegisteredSchemaRowSpec, LixError> {
    let schema_key = schema_key_from_definition(&schema.schema_json)?;
    Ok(RegisteredSchemaRowSpec {
        entity_id: schema_key.entity_id(),
        registered_schema_key: schema_key.schema_key,
        snapshot: json!({ "value": schema.schema_json }),
        schema_json: schema.schema_json.clone(),
    })
}

fn registered_schema_planned_row(row: &RegisteredSchemaRowSpec) -> PlannedStateRow {
    let mut values = BTreeMap::new();
    values.insert("entity_id".to_string(), Value::Text(row.entity_id.clone()));
    values.insert(
        "schema_key".to_string(),
        Value::Text(REGISTERED_SCHEMA_STORAGE_SCHEMA_KEY.to_string()),
    );
    values.insert(
        "file_id".to_string(),
        Value::Text(REGISTERED_SCHEMA_STORAGE_FILE_ID.to_string()),
    );
    values.insert(
        "plugin_key".to_string(),
        Value::Text(REGISTERED_SCHEMA_STORAGE_PLUGIN_KEY.to_string()),
    );
    values.insert(
        "schema_version".to_string(),
        Value::Text(REGISTERED_SCHEMA_STORAGE_SCHEMA_VERSION.to_string()),
    );
    values.insert(
        "snapshot_content".to_string(),
        Value::Json(row.snapshot.clone()),
    );
    values.insert(
        "version_id".to_string(),
        Value::Text(GLOBAL_VERSION_ID.to_string()),
    );
    PlannedStateRow {
        entity_id: row.entity_id.clone(),
        schema_key: REGISTERED_SCHEMA_STORAGE_SCHEMA_KEY.to_string(),
        version_id: Some(GLOBAL_VERSION_ID.to_string()),
        values,
        writer_key: None,
        tombstone: false,
    }
}

fn plugin_archive_file_descriptor_row(
    archive_id: &str,
    descriptor: &PlannedFilesystemDescriptor,
) -> PlannedStateRow {
    let snapshot_content = json!({
        "id": archive_id,
        "directory_id": descriptor.directory_id,
        "name": descriptor.name,
        "extension": descriptor.extension,
        "metadata": descriptor.metadata,
        "hidden": descriptor.hidden,
    })
    .to_string();
    let mut values = BTreeMap::new();
    values.insert("entity_id".to_string(), Value::Text(archive_id.to_string()));
    values.insert(
        "schema_key".to_string(),
        Value::Text(FILESYSTEM_DESCRIPTOR_SCHEMA_KEY.to_string()),
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
        Value::Text(FILESYSTEM_DESCRIPTOR_SCHEMA_VERSION.to_string()),
    );
    values.insert(
        "snapshot_content".to_string(),
        Value::Text(snapshot_content),
    );
    values.insert(
        "version_id".to_string(),
        Value::Text(GLOBAL_VERSION_ID.to_string()),
    );
    PlannedStateRow {
        entity_id: archive_id.to_string(),
        schema_key: FILESYSTEM_DESCRIPTOR_SCHEMA_KEY.to_string(),
        version_id: Some(GLOBAL_VERSION_ID.to_string()),
        values,
        writer_key: None,
        tombstone: false,
    }
}

fn plugin_archive_binary_blob_ref_row(
    archive_id: &str,
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
    values.insert("entity_id".to_string(), Value::Text(archive_id.to_string()));
    values.insert(
        "schema_key".to_string(),
        Value::Text(FILESYSTEM_BINARY_BLOB_REF_SCHEMA_KEY.to_string()),
    );
    values.insert("file_id".to_string(), Value::Text(archive_id.to_string()));
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
        Value::Text(GLOBAL_VERSION_ID.to_string()),
    );
    Ok(PlannedStateRow {
        entity_id: archive_id.to_string(),
        schema_key: FILESYSTEM_BINARY_BLOB_REF_SCHEMA_KEY.to_string(),
        version_id: Some(GLOBAL_VERSION_ID.to_string()),
        values,
        writer_key: None,
        tombstone: false,
    })
}

fn prepare_public_tracked_write_step(
    context: &SemanticWriteContext,
    target: SurfaceBinding,
    relation_name: &str,
    intended_post_state: Vec<PlannedStateRow>,
    filesystem_state: PlannedFilesystemState,
    domain_changes: Vec<PublicDomainChange>,
    schema_live_table_requirements: Vec<SchemaLiveTableRequirement>,
    public_surface_registry_effect: PreparedPublicSurfaceRegistryEffect,
    idempotency_purpose: &str,
) -> Result<PreparedWriteExecutionStep, LixError> {
    let semantic_effects =
        semantic_plan_effects_from_domain_changes(&domain_changes, context.writer_key.as_deref())?;
    let write_payload = json!({
        "rows": intended_post_state.iter().map(summarize_planned_row).collect::<Vec<_>>(),
        "domain_changes": domain_changes.iter().map(summarize_domain_change).collect::<Vec<_>>(),
        "filesystem_files": filesystem_state.files.keys().cloned().collect::<Vec<_>>(),
    });
    PreparedWriteExecutionStep::build(
        PreparedWriteStep {
            statement_kind: PreparedWriteStatementKind::Other,
            result_contract: ResultContract::DmlNoReturning,
            artifact: PreparedWriteArtifact::PublicWrite(PreparedPublicWriteArtifact {
                contract: PreparedPublicWriteContract {
                    operation_kind: PreparedWriteOperationKind::Insert,
                    target,
                    on_conflict_action: None,
                    requested_version_id: Some(GLOBAL_VERSION_ID.to_string()),
                    active_account_ids: context.active_account_ids.clone(),
                    writer_key: context.writer_key.clone(),
                    resolved_write_plan: Some(PreparedResolvedWritePlan {
                        partitions: vec![PreparedResolvedWritePartition {
                            execution_mode: WriteMode::Tracked,
                            authoritative_pre_state_rows: Vec::new(),
                            intended_post_state,
                            workspace_writer_key_updates: BTreeMap::new(),
                            filesystem_state,
                        }],
                    }),
                },
                execution: PreparedPublicWriteExecutionArtifact::Materialize(
                    PreparedPublicWriteMaterialization {
                        partitions: vec![PreparedPublicWriteExecutionPartition::Tracked(
                            PreparedTrackedWriteExecution {
                                schema_live_table_requirements,
                                domain_change_batch: Some(DomainChangeBatch {
                                    changes: domain_changes.clone(),
                                    write_lane: WriteLane::GlobalAdmin,
                                    writer_key: context.writer_key.clone(),
                                    semantic_effects: semantic_effect_markers_from_domain_changes(
                                        &domain_changes,
                                    ),
                                }),
                                create_preconditions: CommitPreconditions {
                                    write_lane: WriteLane::GlobalAdmin,
                                    expected_head:
                                        crate::contracts::artifacts::ExpectedHead::CurrentHead,
                                    idempotency_key: semantic_idempotency_key(
                                        idempotency_purpose,
                                        &write_payload,
                                    )?,
                                },
                                semantic_effects,
                            },
                        )],
                    },
                ),
            }),
            diagnostic_context: PreparedWriteDiagnosticContext::new(
                vec![relation_name.to_string()],
            ),
            public_surface_registry_effect,
        },
        &context.runtime_state,
    )
}

fn semantic_plan_effects_from_domain_changes(
    changes: &[PublicDomainChange],
    writer_key: Option<&str>,
) -> Result<PlanEffects, LixError> {
    Ok(PlanEffects {
        state_commit_stream_changes: state_commit_stream_changes_from_domain_changes(
            changes,
            StateCommitStreamOperation::Insert,
            StateCommitStreamRuntimeMetadata::from_runtime_writer_key(writer_key),
        )?,
        ..PlanEffects::default()
    })
}

fn semantic_effect_markers_from_domain_changes(
    changes: &[PublicDomainChange],
) -> Vec<SemanticEffect> {
    changes
        .iter()
        .map(|change| SemanticEffect {
            effect_key: "state.upsert".to_string(),
            target: format!(
                "{}:{}@{}",
                change.schema_key, change.entity_id, change.version_id
            ),
        })
        .collect()
}

fn planned_row_to_public_domain_change(
    row: &PlannedStateRow,
) -> Result<PublicDomainChange, LixError> {
    Ok(PublicDomainChange {
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        schema_version: planned_row_text_value(row, "schema_version"),
        file_id: planned_row_text_value(row, "file_id"),
        plugin_key: planned_row_text_value(row, "plugin_key"),
        snapshot_content: if row.tombstone {
            None
        } else {
            planned_row_json_text_value(row, "snapshot_content")
        },
        metadata: planned_row_json_text_value(row, "metadata"),
        version_id: row
            .version_id
            .clone()
            .or_else(|| planned_row_text_value(row, "version_id"))
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "semantic tracked write requires a concrete version_id",
                )
            })?,
        writer_key: row.writer_key.clone(),
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

fn summarize_domain_change(change: &PublicDomainChange) -> JsonValue {
    json!({
        "entity_id": change.entity_id,
        "schema_key": change.schema_key,
        "schema_version": change.schema_version,
        "file_id": change.file_id,
        "plugin_key": change.plugin_key,
        "version_id": change.version_id,
        "writer_key": change.writer_key,
        "snapshot_content": change.snapshot_content.as_ref().map(|snapshot| {
            stable_content_fingerprint_hex(snapshot.as_bytes())
        }),
    })
}

fn summarize_planned_row(row: &PlannedStateRow) -> JsonValue {
    json!({
        "entity_id": row.entity_id,
        "schema_key": row.schema_key,
        "version_id": row.version_id,
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

fn require_surface_binding(
    public_surface_registry: &SurfaceRegistry,
    relation_name: &str,
) -> Result<SurfaceBinding, LixError> {
    public_surface_registry
        .bind_relation_name(relation_name)
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("public surface '{relation_name}' is not registered"),
            )
        })
}

fn parse_plugin_archive(archive_bytes: &[u8]) -> Result<ParsedPluginArchive, LixError> {
    let files = read_archive_files(archive_bytes)?;

    let manifest_bytes = files.get("manifest.json").ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: "Plugin archive must contain manifest.json".to_string(),
    })?;
    let manifest_raw = std::str::from_utf8(manifest_bytes).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("Plugin archive manifest.json must be UTF-8: {error}"),
    })?;
    let validated_manifest = parse_plugin_manifest_json(manifest_raw)?;

    let entry_path = normalize_archive_path(&validated_manifest.manifest.entry)?;
    let wasm_bytes = files
        .get(&entry_path)
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "Plugin archive is missing manifest entry file '{}'",
                validated_manifest.manifest.entry
            ),
        })?
        .clone();

    let mut schemas = Vec::with_capacity(validated_manifest.manifest.schemas.len());
    let mut seen_schema_keys = BTreeSet::<(String, String)>::new();
    for schema_path in &validated_manifest.manifest.schemas {
        let normalized_schema_path = normalize_archive_path(schema_path)?;
        let schema_bytes = files.get(&normalized_schema_path).ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("Plugin archive is missing schema file '{schema_path}'"),
        })?;
        let schema_json: JsonValue =
            serde_json::from_slice(schema_bytes).map_err(|error| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "Plugin archive schema '{schema_path}' is invalid JSON: {error}"
                ),
            })?;
        validate_lix_schema_definition(&schema_json)?;
        let schema_key = schema_key_from_definition(&schema_json)?;
        if !seen_schema_keys.insert((
            schema_key.schema_key.clone(),
            schema_key.schema_version.clone(),
        )) {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "Plugin archive declares duplicate schema '{}~{}'",
                    schema_key.schema_key, schema_key.schema_version
                ),
            });
        }
        schemas.push(ParsedSchema { schema_json });
    }

    Ok(ParsedPluginArchive {
        manifest: validated_manifest.manifest,
        wasm_bytes,
        schemas,
    })
}

fn read_archive_files(archive_bytes: &[u8]) -> Result<BTreeMap<String, Vec<u8>>, LixError> {
    if archive_bytes.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "Plugin archive bytes must not be empty".to_string(),
        });
    }

    let mut archive = ZipArchive::new(Cursor::new(archive_bytes)).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("Plugin archive is not a valid zip file: {error}"),
    })?;
    let mut files = BTreeMap::<String, Vec<u8>>::new();

    for index in 0..archive.len() {
        let mut entry = archive.by_index(index).map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("Failed to read plugin archive entry at index {index}: {error}"),
        })?;
        let raw_name = entry.name().to_string();

        if entry.is_dir() {
            continue;
        }
        if is_symlink_mode(entry.unix_mode()) {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!("Plugin archive entry '{raw_name}' must not be a symlink"),
            });
        }

        let normalized_path = normalize_archive_path(&raw_name)?;
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes).map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("Failed to read plugin archive entry '{raw_name}': {error}"),
        })?;
        if files.insert(normalized_path.clone(), bytes).is_some() {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!("Plugin archive contains duplicate entry '{normalized_path}'"),
            });
        }
    }

    Ok(files)
}

fn is_symlink_mode(mode: Option<u32>) -> bool {
    const MODE_FILE_TYPE_MASK: u32 = 0o170000;
    const MODE_SYMLINK: u32 = 0o120000;
    mode.is_some_and(|value| (value & MODE_FILE_TYPE_MASK) == MODE_SYMLINK)
}

fn normalize_archive_path(path: &str) -> Result<String, LixError> {
    if path.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "Plugin archive path must not be empty".to_string(),
        });
    }
    if path.starts_with('/') || path.starts_with('\\') {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("Plugin archive path '{path}' must be relative"),
        });
    }
    if path.contains('\\') {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("Plugin archive path '{path}' must use forward slash separators"),
        });
    }

    let mut segments = Vec::<String>::new();
    for component in Path::new(path).components() {
        match component {
            Component::Normal(value) => {
                let segment = value.to_str().ok_or_else(|| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!(
                        "Plugin archive path '{path}' contains non-UTF-8 components"
                    ),
                })?;
                if segment.is_empty() {
                    return Err(LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        description: format!("Plugin archive path '{path}' is invalid"),
                    });
                }
                segments.push(segment.to_string());
            }
            Component::CurDir | Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!(
                        "Plugin archive path '{path}' must not contain traversal or absolute components"
                    ),
                })
            }
        }
    }

    if segments.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("Plugin archive path '{path}' is invalid"),
        });
    }

    Ok(segments.join("/"))
}

fn ensure_valid_wasm_binary(wasm_bytes: &[u8]) -> Result<(), LixError> {
    if wasm_bytes.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "Plugin wasm bytes must not be empty".to_string(),
        });
    }
    if wasm_bytes.len() < 8 || !wasm_bytes.starts_with(&[0x00, 0x61, 0x73, 0x6d]) {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "Plugin wasm bytes must start with a valid wasm header".to_string(),
        });
    }
    Ok(())
}
