use std::collections::{BTreeMap, BTreeSet};

use crate::backend::program_runner::execute_write_program_with_transaction;
use crate::canonical_json::CanonicalJson;
use crate::deterministic_mode::{
    build_ensure_runtime_sequence_row_sql, build_update_runtime_sequence_highest_sql,
};
use crate::filesystem::runtime::{
    build_binary_blob_fastcdc_write_program, compile_filesystem_transaction_state_from_state,
    filesystem_transaction_state_needs_exact_descriptors, with_exact_filesystem_descriptors,
    BinaryBlobWrite, ExactFilesystemDescriptorState, FilesystemDescriptorState,
    FilesystemSemanticChange, FilesystemTransactionState, MutationRow, OptionalTextPatch,
    FILESYSTEM_DESCRIPTOR_FILE_ID, FILESYSTEM_DESCRIPTOR_PLUGIN_KEY, FILESYSTEM_FILE_SCHEMA_KEY,
    FILESYSTEM_FILE_SCHEMA_VERSION,
};
use crate::functions::LixFunctionProvider;
use crate::version::version_ref_snapshot_content;
use crate::version::GLOBAL_VERSION_ID;
use crate::SqlDialect;
use crate::{CanonicalSchemaKey, LixBackendTransaction, LixError, QueryResult, Value};
use async_trait::async_trait;

use super::change_log::{
    build_prepared_batch_from_canonical_output, load_next_change_ordinal_with_executor,
};
use super::create_commit_preflight::{
    load_create_commit_deterministic_sequence_start as load_create_commit_deterministic_sequence_start_impl,
    load_untracked_file_descriptor as load_untracked_file_descriptor_impl,
};
use super::generate_commit::generate_commit;
use super::graph_index::{
    build_commit_graph_node_prepared_batch, resolve_commit_graph_node_write_rows_with_executor,
};
use super::receipt::{
    latest_canonical_watermark_from_change_rows, CanonicalCommitReceipt, UpdatedVersionRef,
};
use super::roots::load_committed_version_head_commit_id;
use super::state_source::{
    load_exact_committed_state_row_at_version_head_with_executor, load_version_info_for_versions,
    CommitQueryExecutor, ExactCommittedStateRowRequest,
};
use super::types::{
    CanonicalCommitOutput, DomainChangeInput, GenerateCommitArgs, GenerateCommitResult,
    ProposedDomainChange, VersionInfo, VersionSnapshot,
};

const COMMIT_IDEMPOTENCY_TABLE: &str = "lix_internal_commit_idempotency";
const BINARY_BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
const BINARY_BLOB_REF_SCHEMA_VERSION: &str = "1";
const INTERNAL_FILESYSTEM_PLUGIN_KEY: &str = "lix";
const IDEMPOTENCY_KIND_EXACT: &str = "exact";
const IDEMPOTENCY_KIND_CURRENT_HEAD_FINGERPRINT: &str = "current_head_fingerprint";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CreateCommitWriteLane {
    Version(String),
    GlobalAdmin,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CreateCommitExpectedHead {
    CurrentHead,
    CommitId(String),
    CreateIfMissing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CreateCommitIdempotencyKey {
    Exact(String),
    CurrentHeadFingerprint(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreateCommitPreconditions {
    pub(crate) write_lane: CreateCommitWriteLane,
    pub(crate) expected_head: CreateCommitExpectedHead,
    pub(crate) idempotency_key: CreateCommitIdempotencyKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreateCommitArgs {
    pub(crate) timestamp: Option<String>,
    pub(crate) changes: Vec<ProposedDomainChange>,
    pub(crate) filesystem_state: FilesystemTransactionState,
    pub(crate) preconditions: CreateCommitPreconditions,
    pub(crate) active_account_ids: Vec<String>,
    pub(crate) lane_parent_commit_ids_override: Option<Vec<String>>,
    pub(crate) allow_empty_commit: bool,
    pub(crate) should_emit_observe_tick: bool,
    pub(crate) observe_tick_writer_key: Option<String>,
    pub(crate) writer_key: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CreateCommitDisposition {
    Applied,
    Replay,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreateCommitResult {
    pub(crate) disposition: CreateCommitDisposition,
    pub(crate) committed_head: String,
    pub(crate) receipt: Option<CanonicalCommitReceipt>,
    pub(crate) applied_output: Option<CreateCommitAppliedOutput>,
    pub(crate) applied_domain_changes: Vec<ProposedDomainChange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitIdempotencyWrite {
    pub(crate) write_lane: String,
    pub(crate) idempotency_key: String,
    pub(crate) idempotency_kind: String,
    pub(crate) idempotency_value: String,
    pub(crate) parent_head_snapshot_content: String,
    pub(crate) commit_id: String,
    pub(crate) created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObserveTickWrite {
    pub(crate) writer_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OperationalCommitApplyInput {
    pub(crate) idempotency_write: CommitIdempotencyWrite,
    pub(crate) deterministic_sequence_highest_seen: Option<i64>,
    pub(crate) observe_tick: Option<ObserveTickWrite>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreateCommitAppliedOutput {
    pub(crate) canonical_output: CanonicalCommitOutput,
    pub(crate) operational_apply_input: OperationalCommitApplyInput,
    pub(crate) pending_public_commit_seed: Option<PendingPublicCommitSeed>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingPublicCommitSeed {
    pub(crate) commit_id: String,
    pub(crate) commit_change_id: String,
    pub(crate) commit_snapshot_content: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CreateCommitErrorKind {
    EmptyBatch,
    MissingDomainField,
    MissingWriteLane,
    TipDrift,
    UnsupportedWriteLane,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreateCommitError {
    pub(crate) kind: CreateCommitErrorKind,
    pub(crate) message: String,
}

#[async_trait(?Send)]
pub(crate) trait CreateCommitInvariantChecker {
    async fn recheck_invariants(
        &mut self,
        transaction: &mut dyn LixBackendTransaction,
    ) -> Result<(), CreateCommitError>;
}

pub(crate) async fn create_commit(
    transaction: &mut dyn LixBackendTransaction,
    args: CreateCommitArgs,
    functions: &mut dyn LixFunctionProvider,
    invariant_checker: Option<&mut dyn CreateCommitInvariantChecker>,
) -> Result<CreateCommitResult, CreateCommitError> {
    if args.changes.is_empty() && args.filesystem_state.files.is_empty() && !args.allow_empty_commit
    {
        return Err(CreateCommitError {
            kind: CreateCommitErrorKind::EmptyBatch,
            message: "create_commit requires at least one change".to_string(),
        });
    }

    let concrete_lane = concrete_lane(&args.preconditions)?;
    validate_change_versions(&args.changes, &args.filesystem_state, &concrete_lane)?;

    let needs_deterministic_sequence = functions.deterministic_sequence_enabled()
        && !functions.deterministic_sequence_initialized();
    let preflight = {
        let mut executor = TransactionCommitExecutor { transaction };
        load_create_commit_preflight_state(
            &mut executor,
            &concrete_lane,
            &args.preconditions,
            &args.filesystem_state,
            needs_deterministic_sequence,
            &args.active_account_ids,
        )
        .await?
    };
    if let Some(sequence_start) = preflight.deterministic_sequence_start {
        functions.initialize_deterministic_sequence(sequence_start);
    }
    let resolved_idempotency = resolve_idempotency_state(&args.preconditions, &preflight);
    let current_head = preflight.current_head.clone();
    let existing_replay = preflight.existing_replay.clone();
    let timestamp = args
        .timestamp
        .clone()
        .unwrap_or_else(|| functions.timestamp());

    match (&args.preconditions.expected_head, current_head.as_deref()) {
        (CreateCommitExpectedHead::CurrentHead, Some(_)) => {}
        (CreateCommitExpectedHead::CurrentHead, None) => {
            return Err(CreateCommitError {
                kind: CreateCommitErrorKind::MissingWriteLane,
                message: format!(
                    "create commit precondition failed for '{}': version ref is missing",
                    lane_storage_key(&concrete_lane)
                ),
            });
        }
        (CreateCommitExpectedHead::CommitId(expected), Some(current)) if current != expected => {
            if existing_replay.as_deref() == Some(current) {
                return Ok(CreateCommitResult {
                    disposition: CreateCommitDisposition::Replay,
                    committed_head: current.to_string(),
                    receipt: None,
                    applied_output: None,
                    applied_domain_changes: Vec::new(),
                });
            }
            return Err(CreateCommitError {
                kind: CreateCommitErrorKind::TipDrift,
                message: format!(
                    "create commit precondition failed for '{}': expected head '{}', found '{}'",
                    lane_storage_key(&concrete_lane),
                    expected,
                    current
                ),
            });
        }
        (CreateCommitExpectedHead::CommitId(_), None) => {
            return Err(CreateCommitError {
                kind: CreateCommitErrorKind::MissingWriteLane,
                message: format!(
                    "create commit precondition failed for '{}': version ref is missing",
                    lane_storage_key(&concrete_lane)
                ),
            });
        }
        (CreateCommitExpectedHead::CreateIfMissing, Some(current)) => {
            if existing_replay.as_deref() == Some(current) {
                return Ok(CreateCommitResult {
                    disposition: CreateCommitDisposition::Replay,
                    committed_head: current.to_string(),
                    receipt: None,
                    applied_output: None,
                    applied_domain_changes: Vec::new(),
                });
            }
            return Err(CreateCommitError {
                kind: CreateCommitErrorKind::TipDrift,
                message: format!(
                    "create commit precondition failed for '{}': lane already exists at '{}'",
                    lane_storage_key(&concrete_lane),
                    current
                ),
            });
        }
        (CreateCommitExpectedHead::CreateIfMissing, None)
        | (CreateCommitExpectedHead::CommitId(_), Some(_)) => {}
    }

    if let Some(commit_id) = existing_replay {
        return Ok(CreateCommitResult {
            disposition: CreateCommitDisposition::Replay,
            committed_head: commit_id,
            receipt: None,
            applied_output: None,
            applied_domain_changes: Vec::new(),
        });
    }

    if let Some(invariant_checker) = invariant_checker {
        invariant_checker.recheck_invariants(transaction).await?;
    }

    let (applied_domain_changes, mut compiled_filesystem_state) = resolve_proposed_domain_changes(
        &args.changes,
        &preflight,
        &args.filesystem_state,
        args.writer_key.as_deref(),
    )?;
    let applied_domain_changes = {
        let mut executor = TransactionCommitExecutor { transaction };
        normalize_proposed_domain_changes(&mut executor, &applied_domain_changes).await?
    };
    // Binary CAS writes are only meaningful when a surviving semantic change still
    // points at them. If normalization removed every referencing domain change,
    // drop the unreachable payload writes before deciding whether this is a noop.
    let applied_change_identities = applied_domain_changes
        .iter()
        .map(proposed_domain_change_identity)
        .collect::<BTreeSet<_>>();
    compiled_filesystem_state
        .binary_blob_writes
        .retain(|write| binary_blob_write_still_needed(write, &applied_change_identities));
    if applied_domain_changes.is_empty()
        && compiled_filesystem_state.binary_blob_writes.is_empty()
        && !args.allow_empty_commit
    {
        return Ok(CreateCommitResult {
            disposition: CreateCommitDisposition::Replay,
            committed_head: current_head.unwrap_or_default(),
            receipt: None,
            applied_output: None,
            applied_domain_changes: Vec::new(),
        });
    }
    let domain_changes =
        materialize_domain_changes(&timestamp, &applied_domain_changes, functions)?;
    let affected_versions = domain_changes
        .iter()
        .map(|change| change.version_id.to_string())
        .collect::<BTreeSet<_>>();
    let lane_version_id = match &concrete_lane {
        ConcreteWriteLane::Version { version_id } => Some(version_id.clone()),
        ConcreteWriteLane::GlobalAdmin => None,
    };
    let versions_to_load = affected_versions
        .iter()
        .filter(|version_id| Some((*version_id).clone()) != lane_version_id)
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut versions = {
        let mut executor = TransactionCommitExecutor { transaction };
        load_version_info_for_versions(&mut executor, &versions_to_load)
            .await
            .map_err(backend_error)?
    };
    if let Some(version_id) = lane_version_id.as_ref() {
        versions.insert(
            version_id.clone(),
            VersionInfo {
                parent_commit_ids: args
                    .lane_parent_commit_ids_override
                    .clone()
                    .unwrap_or_else(|| current_head.clone().into_iter().collect()),
                snapshot: VersionSnapshot {
                    id: try_identity(version_id.clone(), "lane version snapshot id")?,
                },
            },
        );
    }
    if matches!(concrete_lane, ConcreteWriteLane::GlobalAdmin) {
        let global_snapshot_id =
            try_identity(GLOBAL_VERSION_ID.to_string(), "global version snapshot id")?;
        let global_version = versions
            .entry(GLOBAL_VERSION_ID.to_string())
            .or_insert(VersionInfo {
                parent_commit_ids: Vec::new(),
                snapshot: VersionSnapshot {
                    id: global_snapshot_id,
                },
            });
        global_version.parent_commit_ids = current_head.clone().into_iter().collect();
    }
    let generated_commit = generate_commit(
        GenerateCommitArgs {
            timestamp: timestamp.clone(),
            active_accounts: preflight.active_accounts,
            changes: domain_changes,
            versions,
            force_commit_versions: if args.allow_empty_commit {
                lane_version_id
                    .clone()
                    .into_iter()
                    .collect::<BTreeSet<String>>()
            } else {
                BTreeSet::new()
            },
        },
        || functions.uuid_v7(),
    )
    .map_err(backend_error)?;
    let committed_head = extract_committed_head_id(&generated_commit, &concrete_lane)?;
    let canonical_output = generated_commit.canonical_output;
    let updated_version_refs = generated_commit.updated_version_refs;
    let next_change_ordinal = {
        let mut executor = TransactionCommitExecutor { transaction };
        load_next_change_ordinal_with_executor(&mut executor)
            .await
            .map_err(backend_error)?
    };
    let receipt = build_canonical_commit_receipt(
        committed_head.clone(),
        &canonical_output,
        &updated_version_refs,
        next_change_ordinal,
    )?;
    let mut executor = &mut *transaction;
    let commit_graph_rows =
        resolve_commit_graph_node_write_rows_with_executor(&mut executor, &canonical_output)
            .await
            .map_err(backend_error)?;
    let idempotency_write = CommitIdempotencyWrite {
        write_lane: lane_storage_key(&concrete_lane),
        idempotency_key: resolved_idempotency.legacy_key.clone(),
        idempotency_kind: resolved_idempotency.kind.to_string(),
        idempotency_value: resolved_idempotency.value.clone(),
        parent_head_snapshot_content: resolved_idempotency.parent_head_snapshot_content.clone(),
        commit_id: committed_head.clone(),
        created_at: timestamp.clone(),
    };
    let observe_tick = args.should_emit_observe_tick.then(|| ObserveTickWrite {
        writer_key: args.observe_tick_writer_key.clone(),
    });
    let pending_public_commit_seed = build_pending_public_commit_seed(&canonical_output)?;
    let mut prepared_batch = build_prepared_batch_from_canonical_output(
        &canonical_output,
        functions,
        transaction.dialect(),
        next_change_ordinal,
    )
    .map_err(backend_error)?;
    let deterministic_sequence_highest_seen =
        functions.deterministic_sequence_persist_highest_seen();
    prepared_batch.extend(
        build_commit_graph_node_prepared_batch(&commit_graph_rows, transaction.dialect())
            .map_err(backend_error)?,
    );
    prepared_batch.append_sql(insert_idempotency_row_sql(&idempotency_write));
    if let Some(highest_seen) = deterministic_sequence_highest_seen {
        prepared_batch.append_sql(build_ensure_runtime_sequence_row_sql(
            highest_seen,
            transaction.dialect(),
        ));
        prepared_batch.append_sql(build_update_runtime_sequence_highest_sql(
            highest_seen,
            transaction.dialect(),
        ));
    }
    if let Some(observe_tick) = observe_tick.as_ref() {
        prepared_batch.append_sql(build_observe_tick_insert_sql(
            observe_tick.writer_key.as_deref(),
        ));
    }
    let applied_output = CreateCommitAppliedOutput {
        canonical_output,
        operational_apply_input: OperationalCommitApplyInput {
            idempotency_write,
            deterministic_sequence_highest_seen,
            observe_tick,
        },
        pending_public_commit_seed,
    };
    // NOTE: watermark is intentionally NOT written here. It is written once
    // at transaction-commit time by the caller, so that multi-statement
    // transactions (including merged commits) always end with a consistent
    // watermark pointing to the latest canonical change.

    let payloads = compiled_filesystem_state
        .binary_blob_writes
        .iter()
        .map(BinaryBlobWrite::as_input)
        .collect::<Vec<_>>();
    let mut write_program =
        build_binary_blob_fastcdc_write_program(transaction.dialect(), &payloads)
            .map_err(backend_error)?;
    write_program.push_batch(prepared_batch);
    execute_write_program_with_transaction(transaction, write_program)
        .await
        .map_err(backend_error)?;
    Ok(CreateCommitResult {
        disposition: CreateCommitDisposition::Applied,
        committed_head,
        receipt: Some(receipt),
        applied_output: Some(applied_output),
        applied_domain_changes,
    })
}

fn build_pending_public_commit_seed(
    canonical_output: &CanonicalCommitOutput,
) -> Result<Option<PendingPublicCommitSeed>, CreateCommitError> {
    let Some(commit_row) = canonical_output
        .changes
        .iter()
        .find(|row| row.schema_key == "lix_commit")
    else {
        return Ok(None);
    };
    let commit_snapshot_content =
        commit_row
            .snapshot_content
            .as_deref()
            .ok_or_else(|| CreateCommitError {
                kind: CreateCommitErrorKind::Internal,
                message: "public commit seed requires commit snapshot_content".to_string(),
            })?;
    let commit_snapshot: serde_json::Value = serde_json::from_str(commit_snapshot_content)
        .map_err(|error| CreateCommitError {
            kind: CreateCommitErrorKind::Internal,
            message: format!("public commit seed snapshot is invalid JSON: {error}"),
        })?;
    commit_snapshot
        .get("change_set_id")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| CreateCommitError {
            kind: CreateCommitErrorKind::Internal,
            message: "public commit seed snapshot is missing change_set_id".to_string(),
        })?;
    let commit_change_id = canonical_output
        .changes
        .iter()
        .find(|row| row.schema_key == "lix_commit" && row.entity_id == commit_row.entity_id)
        .map(|row| row.id.clone())
        .ok_or_else(|| CreateCommitError {
            kind: CreateCommitErrorKind::Internal,
            message: "public commit seed requires a lix_commit change row".to_string(),
        })?;

    Ok(Some(PendingPublicCommitSeed {
        commit_id: commit_row.entity_id.to_string(),
        commit_change_id,
        commit_snapshot_content: commit_snapshot_content.to_string(),
    }))
}

fn build_observe_tick_insert_sql(writer_key: Option<&str>) -> String {
    match writer_key {
        Some(writer_key) => format!(
            "INSERT INTO lix_internal_observe_tick (created_at, writer_key) \
             VALUES (CURRENT_TIMESTAMP, '{}')",
            escape_sql_string(writer_key)
        ),
        None => "INSERT INTO lix_internal_observe_tick (created_at, writer_key) \
                  VALUES (CURRENT_TIMESTAMP, NULL)"
            .to_string(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ConcreteWriteLane {
    Version { version_id: String },
    GlobalAdmin,
}

struct TransactionCommitExecutor<'a> {
    transaction: &'a mut dyn LixBackendTransaction,
}

#[async_trait(?Send)]
impl CommitQueryExecutor for TransactionCommitExecutor<'_> {
    fn dialect(&self) -> SqlDialect {
        self.transaction.dialect()
    }

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        self.transaction.execute(sql, params).await
    }
}

fn concrete_lane(
    preconditions: &CreateCommitPreconditions,
) -> Result<ConcreteWriteLane, CreateCommitError> {
    match &preconditions.write_lane {
        CreateCommitWriteLane::Version(version_id) => Ok(ConcreteWriteLane::Version {
            version_id: version_id.clone(),
        }),
        CreateCommitWriteLane::GlobalAdmin => Ok(ConcreteWriteLane::GlobalAdmin),
    }
}

fn resolve_proposed_domain_changes(
    changes: &[ProposedDomainChange],
    preflight: &CreateCommitPreflightState,
    filesystem_state: &FilesystemTransactionState,
    writer_key: Option<&str>,
) -> Result<(Vec<ProposedDomainChange>, CompiledTrackedFilesystemState), CreateCommitError> {
    let hydrated = with_exact_filesystem_descriptors(filesystem_state, &preflight.file_descriptors);
    let compiled_filesystem = compile_filesystem_transaction_state_from_state(
        &hydrated,
        writer_key,
        &[] as &[MutationRow],
    )
    .map_err(backend_error)?;

    let mut resolved = changes.to_vec();
    let mut index_by_identity = resolved
        .iter()
        .enumerate()
        .map(|(index, change)| (proposed_domain_change_identity(change), index))
        .collect::<BTreeMap<_, _>>();
    for compiled_change in compiled_filesystem
        .semantic_changes
        .iter()
        .map(proposed_domain_change_from_filesystem_semantic_change)
    {
        let compiled_change = compiled_change?;
        let identity = proposed_domain_change_identity(&compiled_change);
        if let Some(index) = index_by_identity.get(&identity).copied() {
            let mut merged = compiled_change;
            if merged.writer_key.is_none() {
                merged.writer_key = resolved[index].writer_key.clone();
            }
            resolved[index] = merged;
        } else {
            index_by_identity.insert(identity, resolved.len());
            resolved.push(compiled_change);
        }
    }
    Ok((
        resolved,
        CompiledTrackedFilesystemState {
            binary_blob_writes: compiled_filesystem.binary_blob_writes,
        },
    ))
}

async fn normalize_proposed_domain_changes(
    executor: &mut dyn CommitQueryExecutor,
    changes: &[ProposedDomainChange],
) -> Result<Vec<ProposedDomainChange>, CreateCommitError> {
    let mut normalized = Vec::with_capacity(changes.len());
    for change in changes {
        if proposed_domain_change_is_noop(executor, change).await? {
            continue;
        }
        normalized.push(change.clone());
    }
    Ok(normalized)
}

async fn proposed_domain_change_is_noop(
    executor: &mut dyn CommitQueryExecutor,
    change: &ProposedDomainChange,
) -> Result<bool, CreateCommitError> {
    let Some(file_id) = change.file_id.clone() else {
        return Ok(false);
    };
    let Some(plugin_key) = change.plugin_key.clone() else {
        return Ok(false);
    };
    let Some(schema_version) = change.schema_version.clone() else {
        return Ok(false);
    };
    let current = load_exact_committed_state_row_at_version_head_with_executor(
        executor,
        &ExactCommittedStateRowRequest {
            entity_id: change.entity_id.to_string(),
            schema_key: change.schema_key.to_string(),
            version_id: change.version_id.to_string(),
            exact_filters: BTreeMap::from([
                ("file_id".to_string(), Value::Text(file_id.to_string())),
                (
                    "plugin_key".to_string(),
                    Value::Text(plugin_key.to_string()),
                ),
                (
                    "schema_version".to_string(),
                    Value::Text(schema_version.to_string()),
                ),
            ]),
        },
    )
    .await
    .map_err(backend_error)?;

    match current {
        None => {
            if change.snapshot_content.is_some() {
                return Ok(false);
            }
            if change.version_id.as_str() == GLOBAL_VERSION_ID {
                return Ok(true);
            }

            let global_current = load_exact_committed_state_row_at_version_head_with_executor(
                executor,
                &ExactCommittedStateRowRequest {
                    entity_id: change.entity_id.to_string(),
                    schema_key: change.schema_key.to_string(),
                    version_id: GLOBAL_VERSION_ID.to_string(),
                    exact_filters: BTreeMap::from([
                        ("file_id".to_string(), Value::Text(file_id.to_string())),
                        (
                            "plugin_key".to_string(),
                            Value::Text(plugin_key.to_string()),
                        ),
                        (
                            "schema_version".to_string(),
                            Value::Text(schema_version.to_string()),
                        ),
                    ]),
                },
            )
            .await
            .map_err(backend_error)?;

            Ok(global_current.is_none())
        }
        Some(current) => {
            let current_snapshot = current
                .values
                .get("snapshot_content")
                .and_then(value_as_text);
            let current_metadata = current.values.get("metadata").and_then(value_as_text);
            let current_writer_key = current.writer_key.as_deref();
            Ok(canonicalize_change_payload(
                change.snapshot_content.as_deref(),
                &change.schema_key,
                "snapshot_content",
            )? == canonicalize_change_payload(
                current_snapshot.as_deref(),
                &change.schema_key,
                "snapshot_content",
            )? && canonicalize_change_payload(
                change.metadata.as_deref(),
                &change.schema_key,
                "metadata",
            )? == canonicalize_change_payload(
                current_metadata.as_deref(),
                &change.schema_key,
                "metadata",
            )? && change.writer_key.as_deref() == current_writer_key)
        }
    }
}

struct CompiledTrackedFilesystemState {
    binary_blob_writes: Vec<BinaryBlobWrite>,
}

fn proposed_domain_change_from_filesystem_semantic_change(
    change: &FilesystemSemanticChange,
) -> Result<ProposedDomainChange, CreateCommitError> {
    Ok(ProposedDomainChange {
        entity_id: try_identity(
            change.entity_id.clone(),
            "filesystem semantic change entity_id",
        )?,
        schema_key: try_identity(
            change.schema_key.clone(),
            "filesystem semantic change schema_key",
        )?,
        schema_version: Some(try_identity(
            change.schema_version.clone(),
            "filesystem semantic change schema_version",
        )?),
        file_id: Some(try_identity(
            change.file_id.clone(),
            "filesystem semantic change file_id",
        )?),
        plugin_key: Some(try_identity(
            change.plugin_key.clone(),
            "filesystem semantic change plugin_key",
        )?),
        snapshot_content: change.snapshot_content.clone(),
        metadata: change.metadata.clone(),
        version_id: try_identity(
            change.version_id.clone(),
            "filesystem semantic change version_id",
        )?,
        writer_key: change.writer_key.clone(),
    })
}

fn proposed_domain_change_identity(
    change: &ProposedDomainChange,
) -> (String, String, String, String, String, Option<String>) {
    (
        change.entity_id.to_string(),
        change.schema_key.to_string(),
        change.version_id.to_string(),
        change
            .file_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default(),
        change
            .plugin_key
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default(),
        change.schema_version.as_ref().map(ToString::to_string),
    )
}

fn binary_blob_write_still_needed(
    write: &BinaryBlobWrite,
    applied_change_identities: &BTreeSet<(String, String, String, String, String, Option<String>)>,
) -> bool {
    let Some(file_id) = write.file_id.as_ref() else {
        return true;
    };
    applied_change_identities.contains(&(
        file_id.clone(),
        BINARY_BLOB_REF_SCHEMA_KEY.to_string(),
        write.version_id.clone(),
        file_id.clone(),
        INTERNAL_FILESYSTEM_PLUGIN_KEY.to_string(),
        Some(BINARY_BLOB_REF_SCHEMA_VERSION.to_string()),
    ))
}

fn materialize_domain_changes(
    timestamp: &str,
    changes: &[ProposedDomainChange],
    functions: &mut dyn LixFunctionProvider,
) -> Result<Vec<DomainChangeInput>, CreateCommitError> {
    changes
        .iter()
        .map(|change| {
            Ok(DomainChangeInput {
                id: functions.uuid_v7(),
                entity_id: change.entity_id.clone(),
                schema_key: change.schema_key.clone(),
                schema_version: require_change_field(
                    change.schema_version.clone(),
                    &change.schema_key,
                    "schema_version",
                )?,
                file_id: require_change_field(
                    change.file_id.clone(),
                    &change.schema_key,
                    "file_id",
                )?,
                version_id: change.version_id.clone(),
                plugin_key: require_change_field(
                    change.plugin_key.clone(),
                    &change.schema_key,
                    "plugin_key",
                )?,
                snapshot_content: canonicalize_change_payload(
                    change.snapshot_content.as_deref(),
                    &change.schema_key,
                    "snapshot_content",
                )?,
                metadata: canonicalize_change_payload(
                    change.metadata.as_deref(),
                    &change.schema_key,
                    "metadata",
                )?,
                created_at: timestamp.to_string(),
                writer_key: change.writer_key.clone(),
            })
        })
        .collect()
}

fn canonicalize_change_payload(
    value: Option<&str>,
    schema_key: &str,
    field_name: &str,
) -> Result<Option<CanonicalJson>, CreateCommitError> {
    value
        .map(CanonicalJson::from_text)
        .transpose()
        .map_err(|error| CreateCommitError {
            kind: CreateCommitErrorKind::Internal,
            message: format!(
                "create commit batch requires valid canonical JSON for '{field_name}' in schema '{}': {}",
                schema_key, error.description
            ),
        })
}

fn require_change_field<T>(
    value: Option<T>,
    schema_key: &CanonicalSchemaKey,
    field_name: &str,
) -> Result<T, CreateCommitError> {
    value.ok_or_else(|| CreateCommitError {
        kind: CreateCommitErrorKind::MissingDomainField,
        message: format!(
            "create commit batch requires '{field_name}' for schema '{}'",
            schema_key
        ),
    })
}

struct CreateCommitPreflightState {
    current_head: Option<String>,
    current_head_snapshot: Option<String>,
    existing_replay: Option<String>,
    deterministic_sequence_start: Option<i64>,
    active_accounts: Vec<String>,
    file_descriptors: BTreeMap<String, ExactFilesystemDescriptorState>,
}

async fn load_create_commit_preflight_state(
    executor: &mut dyn CommitQueryExecutor,
    concrete_lane: &ConcreteWriteLane,
    preconditions: &CreateCommitPreconditions,
    filesystem_state: &FilesystemTransactionState,
    include_deterministic_sequence: bool,
    active_account_ids: &[String],
) -> Result<CreateCommitPreflightState, CreateCommitError> {
    let lane_entity_id = match concrete_lane {
        ConcreteWriteLane::Version { version_id } => version_id.as_str(),
        ConcreteWriteLane::GlobalAdmin => GLOBAL_VERSION_ID,
    };
    let current_head = load_committed_version_head_commit_id(executor, lane_entity_id)
        .await
        .map_err(backend_error)?;
    let current_head_snapshot = current_head
        .as_ref()
        .map(|commit_id| version_ref_snapshot_content(lane_entity_id, commit_id));
    let existing_replay = load_create_commit_existing_replay(
        executor,
        concrete_lane,
        preconditions,
        current_head_snapshot.as_deref(),
    )
    .await?;
    let deterministic_sequence_start = if include_deterministic_sequence {
        load_create_commit_deterministic_sequence_start(executor).await?
    } else {
        None
    };
    let active_accounts = active_account_ids.to_vec();
    let file_descriptors = if filesystem_transaction_state_needs_exact_descriptors(filesystem_state)
    {
        load_create_commit_file_descriptors(executor, filesystem_state, lane_entity_id).await?
    } else {
        BTreeMap::new()
    };

    Ok(CreateCommitPreflightState {
        current_head,
        current_head_snapshot,
        existing_replay,
        deterministic_sequence_start,
        active_accounts,
        file_descriptors,
    })
}

fn parse_file_descriptor_preflight_row(
    snapshot_content: &str,
    metadata: Option<String>,
    untracked: bool,
) -> Result<ExactFilesystemDescriptorState, CreateCommitError> {
    let parsed: serde_json::Value =
        serde_json::from_str(snapshot_content).map_err(|error| CreateCommitError {
            kind: CreateCommitErrorKind::Internal,
            message: format!(
                "create commit preflight file descriptor snapshot could not be parsed: {error}"
            ),
        })?;
    Ok(ExactFilesystemDescriptorState {
        descriptor: FilesystemDescriptorState {
            directory_id: parsed
                .get("directory_id")
                .and_then(|value| match value {
                    serde_json::Value::Null => None,
                    serde_json::Value::String(text) => Some(text.clone()),
                    _ => None,
                })
                .unwrap_or_default(),
            name: parsed
                .get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string(),
            extension: parsed.get("extension").and_then(|value| match value {
                serde_json::Value::Null => None,
                serde_json::Value::String(text) if text.is_empty() => None,
                serde_json::Value::String(text) => Some(text.clone()),
                _ => None,
            }),
            hidden: parsed
                .get("hidden")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
            metadata,
        },
        untracked,
    })
}

async fn load_create_commit_existing_replay(
    executor: &mut dyn CommitQueryExecutor,
    concrete_lane: &ConcreteWriteLane,
    preconditions: &CreateCommitPreconditions,
    current_head_snapshot: Option<&str>,
) -> Result<Option<String>, CreateCommitError> {
    let (kind, value, parent_head_snapshot_content) = match &preconditions.idempotency_key {
        CreateCommitIdempotencyKey::Exact(value) => (IDEMPOTENCY_KIND_EXACT, value.as_str(), ""),
        CreateCommitIdempotencyKey::CurrentHeadFingerprint(fingerprint) => {
            let Some(current_head_snapshot) = current_head_snapshot else {
                return Ok(None);
            };
            (
                IDEMPOTENCY_KIND_CURRENT_HEAD_FINGERPRINT,
                fingerprint.as_str(),
                current_head_snapshot,
            )
        }
    };
    let sql = format!(
        "SELECT commit_id \
         FROM {table_name} \
         WHERE write_lane = '{write_lane}' \
           AND idempotency_kind = '{kind}' \
           AND idempotency_value = '{value}' \
           AND parent_head_snapshot_content = '{parent_head_snapshot_content}' \
         LIMIT 1",
        table_name = COMMIT_IDEMPOTENCY_TABLE,
        write_lane = escape_sql_string(&lane_storage_key(concrete_lane)),
        kind = escape_sql_string(kind),
        value = escape_sql_string(value),
        parent_head_snapshot_content = escape_sql_string(parent_head_snapshot_content),
    );
    let result = executor.execute(&sql, &[]).await.map_err(backend_error)?;
    Ok(result
        .rows
        .first()
        .and_then(|row| row.first())
        .and_then(value_as_text)
        .filter(|commit_id| !commit_id.is_empty()))
}

async fn load_create_commit_deterministic_sequence_start(
    executor: &mut dyn CommitQueryExecutor,
) -> Result<Option<i64>, CreateCommitError> {
    load_create_commit_deterministic_sequence_start_impl(executor)
        .await
        .map_err(backend_error)
}

async fn load_create_commit_file_descriptors(
    executor: &mut dyn CommitQueryExecutor,
    filesystem_state: &FilesystemTransactionState,
    lane_entity_id: &str,
) -> Result<BTreeMap<String, ExactFilesystemDescriptorState>, CreateCommitError> {
    let exact_file_ids = filesystem_state
        .files
        .values()
        .filter(|file| {
            !file.deleted
                && file.descriptor.is_none()
                && !matches!(file.metadata_patch, OptionalTextPatch::Unchanged)
        })
        .map(|file| file.file_id.as_str())
        .collect::<BTreeSet<_>>();
    let mut file_descriptors = BTreeMap::new();
    for file_id in exact_file_ids {
        let Some(descriptor) =
            load_create_commit_file_descriptor(executor, file_id, lane_entity_id).await?
        else {
            continue;
        };
        file_descriptors.insert(file_id.to_string(), descriptor);
    }
    Ok(file_descriptors)
}

async fn load_create_commit_file_descriptor(
    executor: &mut dyn CommitQueryExecutor,
    file_id: &str,
    lane_entity_id: &str,
) -> Result<Option<ExactFilesystemDescriptorState>, CreateCommitError> {
    if let Some(descriptor) =
        load_untracked_file_descriptor(executor, file_id, lane_entity_id).await?
    {
        return Ok(Some(descriptor));
    }
    if let Some(descriptor) =
        load_tracked_file_descriptor(executor, file_id, lane_entity_id).await?
    {
        return Ok(Some(descriptor));
    }
    if let Some(descriptor) =
        load_untracked_file_descriptor(executor, file_id, GLOBAL_VERSION_ID).await?
    {
        return Ok(Some(descriptor));
    }
    load_tracked_file_descriptor(executor, file_id, GLOBAL_VERSION_ID).await
}

async fn load_untracked_file_descriptor(
    executor: &mut dyn CommitQueryExecutor,
    file_id: &str,
    version_id: &str,
) -> Result<Option<ExactFilesystemDescriptorState>, CreateCommitError> {
    load_untracked_file_descriptor_impl(executor, file_id, version_id)
        .await
        .map_err(backend_error)
}

async fn load_tracked_file_descriptor(
    executor: &mut dyn CommitQueryExecutor,
    file_id: &str,
    version_id: &str,
) -> Result<Option<ExactFilesystemDescriptorState>, CreateCommitError> {
    let row = load_exact_committed_state_row_at_version_head_with_executor(
        executor,
        &ExactCommittedStateRowRequest {
            entity_id: file_id.to_string(),
            schema_key: FILESYSTEM_FILE_SCHEMA_KEY.to_string(),
            version_id: version_id.to_string(),
            exact_filters: BTreeMap::from([
                (
                    "file_id".to_string(),
                    Value::Text(FILESYSTEM_DESCRIPTOR_FILE_ID.to_string()),
                ),
                (
                    "plugin_key".to_string(),
                    Value::Text(FILESYSTEM_DESCRIPTOR_PLUGIN_KEY.to_string()),
                ),
                (
                    "schema_version".to_string(),
                    Value::Text(FILESYSTEM_FILE_SCHEMA_VERSION.to_string()),
                ),
            ]),
        },
    )
    .await
    .map_err(backend_error)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let Some(snapshot_content) = row.values.get("snapshot_content").and_then(value_as_text) else {
        return Ok(None);
    };
    let metadata = row.values.get("metadata").and_then(value_as_text);
    parse_file_descriptor_preflight_row(&snapshot_content, metadata, false).map(Some)
}

fn parse_deterministic_sequence_snapshot(snapshot_content: &str) -> Result<i64, CreateCommitError> {
    let parsed: serde_json::Value =
        serde_json::from_str(snapshot_content).map_err(|error| CreateCommitError {
            kind: CreateCommitErrorKind::Internal,
            message: format!(
                "create commit preflight deterministic sequence snapshot could not be parsed: {error}"
            ),
        })?;
    let value = parsed
        .get("value")
        .and_then(|value| match value {
            serde_json::Value::Number(number) => number.as_i64(),
            serde_json::Value::String(text) => text.parse::<i64>().ok(),
            _ => None,
        })
        .unwrap_or(-1);
    Ok(value + 1)
}

fn validate_change_versions(
    changes: &[ProposedDomainChange],
    filesystem_state: &FilesystemTransactionState,
    concrete_lane: &ConcreteWriteLane,
) -> Result<(), CreateCommitError> {
    if !filesystem_state.files.is_empty() {
        let expected_version_id = match concrete_lane {
            ConcreteWriteLane::Version { version_id } => version_id,
            ConcreteWriteLane::GlobalAdmin => GLOBAL_VERSION_ID,
        };
        if filesystem_state
            .files
            .values()
            .any(|file| file.version_id != *expected_version_id)
        {
            return Err(CreateCommitError {
                kind: CreateCommitErrorKind::Internal,
                message: format!(
                    "create commit batch must target exactly one version lane '{}'",
                    expected_version_id
                ),
            });
        }
        if changes.is_empty() {
            return Ok(());
        }
    }
    validate_change_versions_without_lazy(changes, concrete_lane)
}

fn validate_change_versions_without_lazy(
    changes: &[ProposedDomainChange],
    concrete_lane: &ConcreteWriteLane,
) -> Result<(), CreateCommitError> {
    let version_ids = changes
        .iter()
        .map(|change| change.version_id.as_str())
        .collect::<BTreeSet<_>>();
    match concrete_lane {
        ConcreteWriteLane::Version { version_id } => {
            if version_ids.len() != 1 || !version_ids.contains(version_id.as_str()) {
                return Err(CreateCommitError {
                    kind: CreateCommitErrorKind::Internal,
                    message: format!(
                        "create commit batch must target exactly one version lane '{}'",
                        version_id
                    ),
                });
            }
        }
        ConcreteWriteLane::GlobalAdmin => {
            if version_ids.len() != 1 || !version_ids.contains(GLOBAL_VERSION_ID) {
                return Err(CreateCommitError {
                    kind: CreateCommitErrorKind::Internal,
                    message: "create commit batch must target exactly the global admin lane"
                        .to_string(),
                });
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedIdempotencyState {
    legacy_key: String,
    kind: &'static str,
    value: String,
    parent_head_snapshot_content: String,
}

fn resolve_idempotency_key(
    preconditions: &CreateCommitPreconditions,
    current_head: Option<&str>,
) -> String {
    match &preconditions.idempotency_key {
        CreateCommitIdempotencyKey::Exact(value) => value.clone(),
        CreateCommitIdempotencyKey::CurrentHeadFingerprint(fingerprint) => serde_json::json!({
            "head": current_head,
            "fingerprint": fingerprint,
        })
        .to_string(),
    }
}

fn resolve_idempotency_state(
    preconditions: &CreateCommitPreconditions,
    preflight: &CreateCommitPreflightState,
) -> ResolvedIdempotencyState {
    let legacy_key = resolve_idempotency_key(preconditions, preflight.current_head.as_deref());
    match &preconditions.idempotency_key {
        CreateCommitIdempotencyKey::Exact(value) => ResolvedIdempotencyState {
            legacy_key,
            kind: IDEMPOTENCY_KIND_EXACT,
            value: value.clone(),
            parent_head_snapshot_content: String::new(),
        },
        CreateCommitIdempotencyKey::CurrentHeadFingerprint(fingerprint) => {
            ResolvedIdempotencyState {
                legacy_key,
                kind: IDEMPOTENCY_KIND_CURRENT_HEAD_FINGERPRINT,
                value: fingerprint.clone(),
                parent_head_snapshot_content: preflight
                    .current_head_snapshot
                    .clone()
                    .unwrap_or_default(),
            }
        }
    }
}

fn extract_committed_head_id(
    commit_result: &GenerateCommitResult,
    concrete_lane: &ConcreteWriteLane,
) -> Result<String, CreateCommitError> {
    let version_id = match concrete_lane {
        ConcreteWriteLane::Version { version_id } => version_id.as_str(),
        ConcreteWriteLane::GlobalAdmin => GLOBAL_VERSION_ID,
    };
    let update = commit_result
        .updated_version_refs
        .iter()
        .find(|update| update.version_id.as_str() == version_id)
        .ok_or_else(|| CreateCommitError {
            kind: CreateCommitErrorKind::Internal,
            message: format!(
                "generated commit result did not include a version ref for '{}'",
                version_id
            ),
        })?;
    if update.commit_id.is_empty() {
        return Err(CreateCommitError {
            kind: CreateCommitErrorKind::Internal,
            message: format!(
                "generated version ref for '{}' contained an empty commit_id",
                version_id
            ),
        });
    }
    Ok(update.commit_id.clone())
}

fn build_canonical_commit_receipt(
    commit_id: String,
    canonical_output: &CanonicalCommitOutput,
    updated_version_refs: &[UpdatedVersionRef],
    starting_change_ordinal: i64,
) -> Result<CanonicalCommitReceipt, CreateCommitError> {
    let canonical_watermark = latest_canonical_watermark_from_change_rows(
        &canonical_output.changes,
        starting_change_ordinal,
    )
    .ok_or_else(|| CreateCommitError {
        kind: CreateCommitErrorKind::Internal,
        message: "canonical commit receipt requires at least one canonical change row".to_string(),
    })?;
    let affected_versions = updated_version_refs
        .iter()
        .map(|update| update.version_id.to_string())
        .collect();
    Ok(CanonicalCommitReceipt {
        commit_id,
        canonical_watermark,
        updated_version_refs: updated_version_refs.to_vec(),
        affected_versions,
    })
}

fn insert_idempotency_row_sql(idempotency: &CommitIdempotencyWrite) -> String {
    format!(
        "INSERT INTO {table_name} \
         (write_lane, idempotency_key, idempotency_kind, idempotency_value, parent_head_snapshot_content, commit_id, created_at) \
         VALUES ('{write_lane}', '{idempotency_key}', '{idempotency_kind}', '{idempotency_value}', '{parent_head_snapshot_content}', '{commit_id}', '{created_at}')",
        table_name = COMMIT_IDEMPOTENCY_TABLE,
        write_lane = escape_sql_string(&idempotency.write_lane),
        idempotency_key = escape_sql_string(&idempotency.idempotency_key),
        idempotency_kind = escape_sql_string(&idempotency.idempotency_kind),
        idempotency_value = escape_sql_string(&idempotency.idempotency_value),
        parent_head_snapshot_content =
            escape_sql_string(&idempotency.parent_head_snapshot_content),
        commit_id = escape_sql_string(&idempotency.commit_id),
        created_at = escape_sql_string(&idempotency.created_at),
    )
}

fn lane_storage_key(concrete_lane: &ConcreteWriteLane) -> String {
    match concrete_lane {
        ConcreteWriteLane::Version { version_id } => format!("version:{version_id}"),
        ConcreteWriteLane::GlobalAdmin => "global-admin".to_string(),
    }
}

fn backend_error(error: LixError) -> CreateCommitError {
    CreateCommitError {
        kind: CreateCommitErrorKind::Internal,
        message: error.description,
    }
}

fn value_as_text(value: &Value) -> Option<String> {
    match value {
        Value::Text(text) => Some(text.clone()),
        Value::Integer(integer) => Some(integer.to_string()),
        Value::Boolean(boolean) => Some(boolean.to_string()),
        Value::Real(real) => Some(real.to_string()),
        _ => None,
    }
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

fn try_identity<T>(value: impl Into<String>, context: &str) -> Result<T, CreateCommitError>
where
    T: TryFrom<String, Error = LixError>,
{
    T::try_from(value.into()).map_err(|error| CreateCommitError {
        kind: CreateCommitErrorKind::Internal,
        message: format!("{context}: {}", error.description),
    })
}

#[cfg(test)]
mod tests {
    use super::{
        build_canonical_commit_receipt, create_commit, CreateCommitArgs, CreateCommitDisposition,
        CreateCommitError, CreateCommitErrorKind, CreateCommitExpectedHead,
        CreateCommitIdempotencyKey, CreateCommitInvariantChecker, CreateCommitPreconditions,
        CreateCommitWriteLane,
    };
    use crate::backend::prepared::{collapse_prepared_batch_for_dialect, PreparedBatch};
    use crate::canonical::receipt::UpdatedVersionRef;
    use crate::canonical::types::{CanonicalCommitOutput, ChangeRow};
    use crate::filesystem::runtime::{
        FilesystemTransactionFileState, FilesystemTransactionState, OptionalTextPatch,
    };
    use crate::functions::LixFunctionProvider;
    use crate::live_state::{
        live_schema_column_names, schema_access::normalized_values_for_schema,
    };
    use crate::version::GLOBAL_VERSION_ID;
    use crate::{
        CanonicalPluginKey, CanonicalSchemaKey, CanonicalSchemaVersion, EntityId, FileId,
        LixBackendTransaction, LixError, QueryResult, SqlDialect, Value, VersionId,
    };
    use async_trait::async_trait;
    use sqlparser::ast::{BinaryOperator, Expr, Query, SetExpr, Statement, TableFactor};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use std::collections::HashMap;

    fn fake_version_ref_live_row(version_id: &str, commit_id: &str) -> Vec<Value> {
        let snapshot = crate::version::version_ref_snapshot_content(version_id, commit_id);
        let normalized = normalized_values_for_schema(
            crate::version::version_ref_schema_key(),
            None,
            Some(&snapshot),
        )
        .expect("snapshot should normalize");
        let mut row = vec![
            Value::Text(version_id.to_string()),
            Value::Text(crate::version::version_ref_schema_key().to_string()),
            Value::Text(crate::version::version_ref_schema_version().to_string()),
            Value::Text(crate::version::version_ref_file_id().to_string()),
            Value::Text(crate::version::version_ref_storage_version_id().to_string()),
            Value::Boolean(true),
            Value::Text(crate::version::version_ref_plugin_key().to_string()),
            Value::Null,
            Value::Null,
            Value::Text("2026-03-06T14:22:00.000Z".to_string()),
            Value::Text("2026-03-06T14:22:00.000Z".to_string()),
        ];
        for column_name in live_schema_column_names(crate::version::version_ref_schema_key(), None)
            .expect("version ref schema should expose column names")
        {
            row.push(normalized.get(&column_name).cloned().unwrap_or(Value::Null));
        }
        row
    }

    fn fake_file_descriptor_live_row() -> Vec<Value> {
        let snapshot = serde_json::json!({
            "id": "file-1",
            "directory_id": serde_json::Value::Null,
            "name": "contract",
            "extension": "txt",
            "hidden": false,
            "metadata": serde_json::Value::Null,
        })
        .to_string();
        let normalized = normalized_values_for_schema("lix_file_descriptor", None, Some(&snapshot))
            .expect("snapshot should normalize");
        let mut row = vec![
            Value::Text("file-1".to_string()),
            Value::Text("lix_file_descriptor".to_string()),
            Value::Text("1".to_string()),
            Value::Text("lix".to_string()),
            Value::Text("version-a".to_string()),
            Value::Boolean(false),
            Value::Text("lix".to_string()),
            Value::Null,
            Value::Text("change-file-1".to_string()),
            Value::Null,
            Value::Text("2026-03-06T14:22:00.000Z".to_string()),
            Value::Text("2026-03-06T14:22:00.000Z".to_string()),
        ];
        for column_name in live_schema_column_names("lix_file_descriptor", None)
            .expect("file descriptor schema should expose column names")
        {
            row.push(normalized.get(&column_name).cloned().unwrap_or(Value::Null));
        }
        row
    }

    struct CountingFunctionProvider {
        next_uuid: usize,
    }

    impl Default for CountingFunctionProvider {
        fn default() -> Self {
            Self { next_uuid: 1 }
        }
    }

    impl LixFunctionProvider for CountingFunctionProvider {
        fn uuid_v7(&mut self) -> String {
            let value = format!("uuid-{}", self.next_uuid);
            self.next_uuid += 1;
            value
        }

        fn timestamp(&mut self) -> String {
            "2026-03-06T14:22:00.000Z".to_string()
        }
    }

    #[derive(Default)]
    struct FakeTransaction {
        version_heads: HashMap<String, String>,
        idempotency_rows: HashMap<(String, String, String, String), String>,
        executed_sql: Vec<String>,
        live_state_mode: Option<String>,
        latest_watermark: Option<(i64, String, String)>,
        fail_projected_live_state_writes: bool,
    }

    #[async_trait(?Send)]
    impl LixBackendTransaction for FakeTransaction {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        fn mode(&self) -> crate::TransactionMode {
            crate::TransactionMode::Write
        }

        async fn execute(&mut self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            self.executed_sql.push(sql.to_string());

            if sql.contains("FROM lix_internal_live_state_status") {
                return Ok(QueryResult {
                    rows: vec![vec![
                        Value::Text(
                            self.live_state_mode
                                .clone()
                                .unwrap_or_else(|| "ready".to_string()),
                        ),
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Text(crate::live_state::LIVE_STATE_SCHEMA_EPOCH.to_string()),
                    ]],
                    columns: vec![
                        "mode".to_string(),
                        "latest_change_ordinal".to_string(),
                        "latest_change_id".to_string(),
                        "latest_change_created_at".to_string(),
                        "schema_epoch".to_string(),
                    ],
                });
            }
            if sql.contains("FROM lix_internal_change")
                && sql.contains("ORDER BY change_ordinal DESC")
            {
                return Ok(QueryResult {
                    rows: self
                        .latest_watermark
                        .clone()
                        .into_iter()
                        .map(|(ordinal, id, created_at)| {
                            vec![
                                Value::Integer(ordinal),
                                Value::Text(id),
                                Value::Text(created_at),
                            ]
                        })
                        .collect(),
                    columns: vec![
                        "change_ordinal".to_string(),
                        "id".to_string(),
                        "created_at".to_string(),
                    ],
                });
            }

            if query_targets_table(sql, "lix_internal_live_v1_lix_version_ref") {
                let rows = self
                    .version_heads
                    .iter()
                    .filter(|(version_id, _)| query_has_text_equality(sql, "entity_id", version_id))
                    .map(|(version_id, commit_id)| fake_version_ref_live_row(version_id, commit_id))
                    .collect::<Vec<_>>();
                return Ok(QueryResult {
                    rows,
                    columns: vec![
                        "entity_id".to_string(),
                        "schema_key".to_string(),
                        "schema_version".to_string(),
                        "file_id".to_string(),
                        "version_id".to_string(),
                        "global".to_string(),
                        "plugin_key".to_string(),
                        "metadata".to_string(),
                        "writer_key".to_string(),
                        "created_at".to_string(),
                        "updated_at".to_string(),
                        "commit_id".to_string(),
                        "id".to_string(),
                    ],
                });
            }
            if query_targets_table(sql, "lix_internal_live_v1_lix_file_descriptor") {
                return Ok(QueryResult {
                    rows: vec![fake_file_descriptor_live_row()],
                    columns: vec![
                        "entity_id".to_string(),
                        "schema_key".to_string(),
                        "schema_version".to_string(),
                        "file_id".to_string(),
                        "version_id".to_string(),
                        "global".to_string(),
                        "plugin_key".to_string(),
                        "metadata".to_string(),
                        "change_id".to_string(),
                        "writer_key".to_string(),
                        "created_at".to_string(),
                        "updated_at".to_string(),
                        "directory_id".to_string(),
                        "extension".to_string(),
                        "hidden".to_string(),
                        "id".to_string(),
                        "metadata_json".to_string(),
                        "name".to_string(),
                    ],
                });
            }
            if self.fail_projected_live_state_writes
                && sql.contains("lix_internal_live_v1_")
                && !sql.contains("lix_internal_live_v1_lix_version_ref")
                && !sql.contains("FROM lix_internal_live_state_status")
            {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "simulated projected live-state write failure",
                ));
            }
            if sql.contains("FROM lix_internal_change c")
                && sql.contains("c.schema_key = 'lix_version_ref'")
            {
                let rows = self
                    .version_heads
                    .iter()
                    .filter(|(version_id, _)| {
                        sql.contains(&format!("c.entity_id = '{}'", version_id))
                            || sql.contains(&format!("'{}'", version_id))
                    })
                    .map(|(version_id, commit_id)| {
                        let snapshot = Value::Text(
                            serde_json::json!({
                                "id": version_id,
                                "commit_id": commit_id,
                            })
                            .to_string(),
                        );
                        if sql.contains("SELECT c.entity_id, s.content AS snapshot_content") {
                            vec![Value::Text(version_id.clone()), snapshot]
                        } else {
                            vec![snapshot]
                        }
                    })
                    .collect::<Vec<_>>();
                return Ok(QueryResult {
                    rows,
                    columns: if sql.contains("SELECT c.entity_id, s.content AS snapshot_content") {
                        vec!["entity_id".to_string(), "snapshot_content".to_string()]
                    } else {
                        vec!["snapshot_content".to_string()]
                    },
                });
            }

            if sql.contains("lix_internal_commit_idempotency") {
                let rows = self
                    .idempotency_rows
                    .iter()
                    .filter(|((lane, kind, value, parent_head_snapshot_content), _)| {
                        sql.contains(&format!("write_lane = '{}'", lane))
                            && sql.contains(&format!("idempotency_kind = '{}'", kind))
                            && sql.contains(&format!("idempotency_value = '{}'", value))
                            && sql.contains(&format!(
                                "parent_head_snapshot_content = '{}'",
                                parent_head_snapshot_content
                            ))
                    })
                    .map(|(_, commit_id)| vec![Value::Text(commit_id.clone())])
                    .collect::<Vec<_>>();
                return Ok(QueryResult {
                    rows,
                    columns: vec!["commit_id".to_string()],
                });
            }

            if let Some(idempotency_sql) =
                extract_statement_from_batch(sql, "INSERT INTO lix_internal_commit_idempotency ")
            {
                let lane = extract_single_quoted_value(idempotency_sql, "VALUES ('")
                    .expect("lane should be present");
                let kind = extract_nth_single_quoted_value(idempotency_sql, 2)
                    .expect("kind should be present");
                let value = extract_nth_single_quoted_value(idempotency_sql, 3)
                    .expect("value should be present");
                let parent_head_snapshot_content =
                    extract_nth_single_quoted_value(idempotency_sql, 4)
                        .expect("parent head snapshot content should be present");
                let commit_id = extract_nth_single_quoted_value(idempotency_sql, 5)
                    .expect("commit id should be present");
                self.idempotency_rows
                    .insert((lane, kind, value, parent_head_snapshot_content), commit_id);
            }
            if extract_statement_from_batch(sql, "INSERT INTO lix_internal_change ").is_some() {
                self.latest_watermark = Some((
                    0,
                    "change-watermark".to_string(),
                    "2026-03-06T14:22:00.000Z".to_string(),
                ));
            }
            if let Some(status_sql) =
                extract_statement_from_batch(sql, "INSERT INTO lix_internal_live_state_status ")
            {
                let mode = extract_nth_single_quoted_value(status_sql, 0)
                    .expect("live state mode should be present");
                self.live_state_mode = Some(mode);
            }

            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn execute_batch(&mut self, batch: &PreparedBatch) -> Result<QueryResult, LixError> {
            let collapsed = collapse_prepared_batch_for_dialect(batch, self.dialect())?;
            self.execute(&collapsed.sql, &collapsed.params).await
        }

        async fn commit(self: Box<Self>) -> Result<(), LixError> {
            Ok(())
        }

        async fn rollback(self: Box<Self>) -> Result<(), LixError> {
            Ok(())
        }
    }

    fn sample_change() -> crate::canonical::ProposedDomainChange {
        crate::canonical::ProposedDomainChange {
            entity_id: "entity-1".try_into().unwrap(),
            schema_key: "lix_key_value".try_into().unwrap(),
            schema_version: Some("1".try_into().unwrap()),
            file_id: Some("lix".try_into().unwrap()),
            plugin_key: Some("lix".try_into().unwrap()),
            snapshot_content: Some("{\"key\":\"hello\"}".to_string()),
            metadata: None,
            version_id: "version-a".try_into().unwrap(),
            writer_key: Some("writer-a".to_string()),
        }
    }

    fn sample_global_change() -> crate::canonical::ProposedDomainChange {
        crate::canonical::ProposedDomainChange {
            entity_id: "version-a".try_into().unwrap(),
            schema_key: "lix_version_descriptor".try_into().unwrap(),
            schema_version: Some("1".try_into().unwrap()),
            file_id: Some(
                crate::version::version_descriptor_file_id()
                    .to_string()
                    .try_into()
                    .unwrap(),
            ),
            plugin_key: Some(
                crate::version::version_descriptor_plugin_key()
                    .to_string()
                    .try_into()
                    .unwrap(),
            ),
            snapshot_content: Some(crate::version::version_descriptor_snapshot_content(
                "version-a",
                "Version A",
                false,
            )),
            metadata: None,
            version_id: GLOBAL_VERSION_ID.try_into().unwrap(),
            writer_key: Some("writer-a".to_string()),
        }
    }

    #[derive(Default)]
    struct RecordingInvariantChecker {
        calls: usize,
        failure: Option<CreateCommitError>,
    }

    #[async_trait(?Send)]
    impl CreateCommitInvariantChecker for RecordingInvariantChecker {
        async fn recheck_invariants(
            &mut self,
            _transaction: &mut dyn LixBackendTransaction,
        ) -> Result<(), CreateCommitError> {
            self.calls += 1;
            if let Some(error) = self.failure.clone() {
                return Err(error);
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn applies_commit_when_head_matches_expected() {
        let mut transaction = FakeTransaction::default();
        transaction
            .version_heads
            .insert("version-a".to_string(), "commit-123".to_string());
        let mut functions = CountingFunctionProvider::default();
        let mut checker = RecordingInvariantChecker::default();

        let result = create_commit(
            &mut transaction,
            CreateCommitArgs {
                timestamp: Some("2026-03-06T14:22:00.000Z".to_string()),
                changes: vec![sample_change()],
                filesystem_state: Default::default(),
                preconditions: CreateCommitPreconditions {
                    write_lane: CreateCommitWriteLane::Version("version-a".to_string()),
                    expected_head: CreateCommitExpectedHead::CommitId("commit-123".to_string()),
                    idempotency_key: CreateCommitIdempotencyKey::Exact("idem-1".to_string()),
                },
                active_account_ids: Vec::new(),
                lane_parent_commit_ids_override: None,
                allow_empty_commit: false,
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
                writer_key: None,
            },
            &mut functions,
            Some(&mut checker),
        )
        .await
        .expect("create_commit should succeed");

        assert_eq!(result.disposition, CreateCommitDisposition::Applied);
        assert!(result.applied_output.is_some());
        assert_eq!(checker.calls, 1);
        let generated_commit_batches = transaction
            .executed_sql
            .iter()
            .filter(|sql| sql.contains("INSERT INTO lix_internal_change "))
            .collect::<Vec<_>>();
        assert_eq!(
            generated_commit_batches.len(),
            1,
            "generated commit work should execute as one SQL batch"
        );
        assert!(
            !generated_commit_batches[0].contains("lix_internal_live_v1_"),
            "canonical commit batch should not inline live-state writes after apply isolation"
        );
        assert!(
            result.receipt.is_some(),
            "create_commit should emit a canonical receipt for projection replay instead of applying derived rows inline"
        );
        assert!(
            transaction
                .executed_sql
                .iter()
                .any(|sql| sql.contains("INSERT INTO lix_internal_commit_idempotency ")),
            "create_commit should persist idempotency state in the executed batch"
        );
        assert!(
            !transaction
                .executed_sql
                .iter()
                .any(|sql| sql.contains("INSERT INTO lix_internal_live_state_status ")),
            "create_commit must NOT write the watermark — the caller stamps it at commit time"
        );
    }

    #[tokio::test]
    async fn create_commit_keeps_canonical_commit_when_projected_live_state_apply_fails() {
        let mut transaction = FakeTransaction {
            version_heads: HashMap::from([("version-a".to_string(), "commit-123".to_string())]),
            latest_watermark: Some((
                0,
                "change-watermark".to_string(),
                "2026-03-06T14:22:00.000Z".to_string(),
            )),
            fail_projected_live_state_writes: true,
            ..Default::default()
        };
        let mut functions = CountingFunctionProvider::default();

        let result = create_commit(
            &mut transaction,
            CreateCommitArgs {
                timestamp: Some("2026-03-06T14:22:00.000Z".to_string()),
                changes: vec![sample_change()],
                filesystem_state: Default::default(),
                preconditions: CreateCommitPreconditions {
                    write_lane: CreateCommitWriteLane::Version("version-a".to_string()),
                    expected_head: CreateCommitExpectedHead::CommitId("commit-123".to_string()),
                    idempotency_key: CreateCommitIdempotencyKey::Exact(
                        "idem-projection-failure".to_string(),
                    ),
                },
                active_account_ids: Vec::new(),
                lane_parent_commit_ids_override: None,
                allow_empty_commit: false,
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
                writer_key: None,
            },
            &mut functions,
            None,
        )
        .await
        .expect("canonical commit should succeed without touching projected live-state writes");

        assert_eq!(result.disposition, CreateCommitDisposition::Applied);
        let receipt = result.receipt.expect("canonical receipt should be present");
        assert!(
            receipt
                .updated_version_refs
                .iter()
                .any(|update| update.version_id.as_str() == "version-a"),
            "canonical receipt should carry committed version-ref updates",
        );
        assert_eq!(
            transaction.live_state_mode, None,
            "canonical commit should not mutate projection readiness directly",
        );
    }

    #[tokio::test]
    async fn create_commit_uses_provided_active_account_ids_without_live_state_fallback() {
        let mut transaction = FakeTransaction::default();
        transaction
            .version_heads
            .insert("version-a".to_string(), "commit-123".to_string());
        let mut functions = CountingFunctionProvider::default();

        let result = create_commit(
            &mut transaction,
            CreateCommitArgs {
                timestamp: Some("2026-03-06T14:22:00.000Z".to_string()),
                changes: vec![sample_change()],
                filesystem_state: Default::default(),
                preconditions: CreateCommitPreconditions {
                    write_lane: CreateCommitWriteLane::Version("version-a".to_string()),
                    expected_head: CreateCommitExpectedHead::CommitId("commit-123".to_string()),
                    idempotency_key: CreateCommitIdempotencyKey::Exact(
                        "idem-active-accounts".to_string(),
                    ),
                },
                active_account_ids: vec!["acct-session".to_string(), "acct-shadow".to_string()],
                lane_parent_commit_ids_override: None,
                allow_empty_commit: false,
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
                writer_key: None,
            },
            &mut functions,
            None,
        )
        .await
        .expect("create_commit should succeed with explicit active accounts");

        let seed = result
            .applied_output
            .and_then(|output| output.pending_public_commit_seed)
            .expect("applied create_commit should produce a pending public commit seed");
        let commit_snapshot: serde_json::Value =
            serde_json::from_str(&seed.commit_snapshot_content).expect("commit snapshot JSON");
        assert_eq!(
            commit_snapshot.get("author_account_ids"),
            Some(&serde_json::json!(["acct-session", "acct-shadow"])),
            "commit attribution should come from typed session-owned active account ids",
        );
        assert!(
            !transaction
                .executed_sql
                .iter()
                .any(|sql| sql.contains("lix_internal_live_v1_lix_active_account")),
            "create_commit should not read shared active-account live-state rows anymore",
        );
    }

    #[tokio::test]
    async fn replays_when_same_idempotency_key_already_committed() {
        let mut transaction = FakeTransaction::default();
        transaction
            .version_heads
            .insert("version-a".to_string(), "commit-456".to_string());
        transaction.idempotency_rows.insert(
            (
                "version:version-a".to_string(),
                "exact".to_string(),
                "idem-1".to_string(),
                String::new(),
            ),
            "commit-456".to_string(),
        );
        let mut functions = CountingFunctionProvider::default();
        let mut checker = RecordingInvariantChecker::default();

        let result = create_commit(
            &mut transaction,
            CreateCommitArgs {
                timestamp: Some("2026-03-06T14:22:00.000Z".to_string()),
                changes: vec![sample_change()],
                filesystem_state: Default::default(),
                preconditions: CreateCommitPreconditions {
                    write_lane: CreateCommitWriteLane::Version("version-a".to_string()),
                    expected_head: CreateCommitExpectedHead::CommitId("commit-123".to_string()),
                    idempotency_key: CreateCommitIdempotencyKey::Exact("idem-1".to_string()),
                },
                active_account_ids: Vec::new(),
                lane_parent_commit_ids_override: None,
                allow_empty_commit: false,
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
                writer_key: None,
            },
            &mut functions,
            Some(&mut checker),
        )
        .await
        .expect("replay should succeed");

        assert_eq!(result.disposition, CreateCommitDisposition::Replay);
        assert_eq!(result.committed_head, "commit-456");
        assert!(result.applied_output.is_none());
        assert_eq!(checker.calls, 0);
    }

    #[tokio::test]
    async fn replays_when_same_current_head_fingerprint_already_committed() {
        let mut transaction = FakeTransaction::default();
        transaction
            .version_heads
            .insert("version-a".to_string(), "commit-456".to_string());
        transaction.idempotency_rows.insert(
            (
                "version:version-a".to_string(),
                "current_head_fingerprint".to_string(),
                "fp-1".to_string(),
                crate::version::version_ref_snapshot_content("version-a", "commit-456"),
            ),
            "commit-456".to_string(),
        );
        let mut functions = CountingFunctionProvider::default();

        let result = create_commit(
            &mut transaction,
            CreateCommitArgs {
                timestamp: Some("2026-03-06T14:22:00.000Z".to_string()),
                changes: vec![sample_change()],
                filesystem_state: Default::default(),
                preconditions: CreateCommitPreconditions {
                    write_lane: CreateCommitWriteLane::Version("version-a".to_string()),
                    expected_head: CreateCommitExpectedHead::CurrentHead,
                    idempotency_key: CreateCommitIdempotencyKey::CurrentHeadFingerprint(
                        "fp-1".to_string(),
                    ),
                },
                active_account_ids: Vec::new(),
                lane_parent_commit_ids_override: None,
                allow_empty_commit: false,
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
                writer_key: None,
            },
            &mut functions,
            None,
        )
        .await
        .expect("fingerprint replay should succeed");

        assert_eq!(result.disposition, CreateCommitDisposition::Replay);
        assert_eq!(result.committed_head, "commit-456");
        assert!(result.applied_output.is_none());
    }

    #[tokio::test]
    async fn rejects_head_drift_without_matching_idempotency_row() {
        let mut transaction = FakeTransaction::default();
        transaction
            .version_heads
            .insert("version-a".to_string(), "commit-456".to_string());
        let mut functions = CountingFunctionProvider::default();
        let mut checker = RecordingInvariantChecker::default();

        let error = create_commit(
            &mut transaction,
            CreateCommitArgs {
                timestamp: Some("2026-03-06T14:22:00.000Z".to_string()),
                changes: vec![sample_change()],
                filesystem_state: Default::default(),
                preconditions: CreateCommitPreconditions {
                    write_lane: CreateCommitWriteLane::Version("version-a".to_string()),
                    expected_head: CreateCommitExpectedHead::CommitId("commit-123".to_string()),
                    idempotency_key: CreateCommitIdempotencyKey::Exact("idem-1".to_string()),
                },
                active_account_ids: Vec::new(),
                lane_parent_commit_ids_override: None,
                allow_empty_commit: false,
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
                writer_key: None,
            },
            &mut functions,
            Some(&mut checker),
        )
        .await
        .expect_err("head drift should fail");

        assert_eq!(error.kind, CreateCommitErrorKind::TipDrift);
        assert_eq!(checker.calls, 0);
    }

    #[tokio::test]
    async fn rejects_missing_lane_without_create_if_missing() {
        let mut transaction = FakeTransaction::default();
        let mut functions = CountingFunctionProvider::default();

        let error = create_commit(
            &mut transaction,
            CreateCommitArgs {
                timestamp: Some("2026-03-06T14:22:00.000Z".to_string()),
                changes: vec![sample_change()],
                filesystem_state: Default::default(),
                preconditions: CreateCommitPreconditions {
                    write_lane: CreateCommitWriteLane::Version("version-a".to_string()),
                    expected_head: CreateCommitExpectedHead::CommitId("commit-123".to_string()),
                    idempotency_key: CreateCommitIdempotencyKey::Exact("idem-1".to_string()),
                },
                active_account_ids: Vec::new(),
                lane_parent_commit_ids_override: None,
                allow_empty_commit: false,
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
                writer_key: None,
            },
            &mut functions,
            None,
        )
        .await
        .expect_err("missing lane should fail");

        assert_eq!(error.kind, CreateCommitErrorKind::MissingWriteLane);
    }

    #[tokio::test]
    async fn allows_create_if_missing_for_new_version_lane() {
        let mut transaction = FakeTransaction::default();
        let mut functions = CountingFunctionProvider::default();

        let result = create_commit(
            &mut transaction,
            CreateCommitArgs {
                timestamp: Some("2026-03-06T14:22:00.000Z".to_string()),
                changes: vec![sample_change()],
                filesystem_state: Default::default(),
                preconditions: CreateCommitPreconditions {
                    write_lane: CreateCommitWriteLane::Version("version-a".to_string()),
                    expected_head: CreateCommitExpectedHead::CreateIfMissing,
                    idempotency_key: CreateCommitIdempotencyKey::Exact("idem-create".to_string()),
                },
                active_account_ids: Vec::new(),
                lane_parent_commit_ids_override: None,
                allow_empty_commit: false,
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
                writer_key: None,
            },
            &mut functions,
            None,
        )
        .await
        .expect("create-if-missing should succeed");

        assert_eq!(result.disposition, CreateCommitDisposition::Applied);
    }

    #[tokio::test]
    async fn applies_global_admin_lane_when_head_matches_expected() {
        let mut transaction = FakeTransaction::default();
        transaction.version_heads.insert(
            GLOBAL_VERSION_ID.to_string(),
            "commit-global-123".to_string(),
        );
        let mut functions = CountingFunctionProvider::default();

        let result = create_commit(
            &mut transaction,
            CreateCommitArgs {
                timestamp: Some("2026-03-06T14:22:00.000Z".to_string()),
                changes: vec![sample_global_change()],
                filesystem_state: Default::default(),
                preconditions: CreateCommitPreconditions {
                    write_lane: CreateCommitWriteLane::GlobalAdmin,
                    expected_head: CreateCommitExpectedHead::CommitId(
                        "commit-global-123".to_string(),
                    ),
                    idempotency_key: CreateCommitIdempotencyKey::Exact("idem-global".to_string()),
                },
                active_account_ids: Vec::new(),
                lane_parent_commit_ids_override: None,
                allow_empty_commit: false,
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
                writer_key: None,
            },
            &mut functions,
            None,
        )
        .await
        .expect("global admin create_commit should succeed");

        assert_eq!(result.disposition, CreateCommitDisposition::Applied);
        assert!(result.applied_output.is_some());
    }

    #[tokio::test]
    async fn exact_file_data_update_avoids_descriptor_preflight_lookup() {
        let mut transaction = FakeTransaction::default();
        transaction
            .version_heads
            .insert("version-a".to_string(), "commit-123".to_string());
        let mut functions = CountingFunctionProvider::default();

        let result = create_commit(
            &mut transaction,
            CreateCommitArgs {
                timestamp: Some("2026-03-06T14:22:00.000Z".to_string()),
                changes: Vec::new(),
                filesystem_state: FilesystemTransactionState {
                    files: std::iter::once((
                        ("file-1".to_string(), "version-a".to_string()),
                        FilesystemTransactionFileState {
                            file_id: "file-1".to_string(),
                            version_id: "version-a".to_string(),
                            untracked: false,
                            descriptor: None,
                            metadata_patch: OptionalTextPatch::Unchanged,
                            data: Some(vec![1, 2, 3]),
                            deleted: false,
                        },
                    ))
                    .collect(),
                },
                preconditions: CreateCommitPreconditions {
                    write_lane: CreateCommitWriteLane::Version("version-a".to_string()),
                    expected_head: CreateCommitExpectedHead::CommitId("commit-123".to_string()),
                    idempotency_key: CreateCommitIdempotencyKey::Exact(
                        "idem-file-data".to_string(),
                    ),
                },
                active_account_ids: Vec::new(),
                lane_parent_commit_ids_override: None,
                allow_empty_commit: false,
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
                writer_key: None,
            },
            &mut functions,
            None,
        )
        .await
        .expect("exact file data update should succeed");

        assert_eq!(result.disposition, CreateCommitDisposition::Applied);
        assert!(
            !transaction
                .executed_sql
                .iter()
                .any(|sql| sql.contains("FROM \"lix_internal_live_v1_lix_file_descriptor\"")),
            "data-only filesystem ops should not require descriptor preflight reads"
        );
    }

    #[tokio::test]
    async fn invariant_recheck_failure_aborts_create_commit_before_generation() {
        let mut transaction = FakeTransaction::default();
        transaction
            .version_heads
            .insert("version-a".to_string(), "commit-123".to_string());
        let mut functions = CountingFunctionProvider::default();
        let mut checker = RecordingInvariantChecker {
            calls: 0,
            failure: Some(CreateCommitError {
                kind: CreateCommitErrorKind::Internal,
                message: "create commit invariant failed".to_string(),
            }),
        };

        let error = create_commit(
            &mut transaction,
            CreateCommitArgs {
                timestamp: Some("2026-03-06T14:22:00.000Z".to_string()),
                changes: vec![sample_change()],
                filesystem_state: Default::default(),
                preconditions: CreateCommitPreconditions {
                    write_lane: CreateCommitWriteLane::Version("version-a".to_string()),
                    expected_head: CreateCommitExpectedHead::CommitId("commit-123".to_string()),
                    idempotency_key: CreateCommitIdempotencyKey::Exact("idem-1".to_string()),
                },
                active_account_ids: Vec::new(),
                lane_parent_commit_ids_override: None,
                allow_empty_commit: false,
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
                writer_key: None,
            },
            &mut functions,
            Some(&mut checker),
        )
        .await
        .expect_err("create commit invariant failure should abort");

        assert_eq!(checker.calls, 1);
        assert_eq!(error.message, "create commit invariant failed");
        assert!(
            !transaction
                .executed_sql
                .iter()
                .any(|sql| sql.contains("INSERT INTO lix_internal_commit_idempotency ")),
            "create_commit should abort before persisting idempotency state"
        );
    }

    fn extract_single_quoted_value(sql: &str, prefix: &str) -> Option<String> {
        let start = sql.find(prefix)? + prefix.len();
        let rest = &sql[start..];
        let end = rest.find('\'')?;
        Some(rest[..end].to_string())
    }

    fn extract_statement_from_batch<'a>(sql: &'a str, prefix: &str) -> Option<&'a str> {
        let start = sql.find(prefix)?;
        let statement = &sql[start..];
        Some(statement.split("; ").next().unwrap_or(statement))
    }

    fn extract_nth_single_quoted_value(sql: &str, index: usize) -> Option<String> {
        let mut remaining = sql;
        for current in 0..=index {
            let start = remaining.find('\'')? + 1;
            remaining = &remaining[start..];
            let end = remaining.find('\'')?;
            if current == index {
                return Some(remaining[..end].to_string());
            }
            remaining = &remaining[end + 1..];
        }
        None
    }

    fn query_targets_table(sql: &str, table_name: &str) -> bool {
        let Ok(statements) = Parser::parse_sql(&GenericDialect {}, sql) else {
            return false;
        };
        statements
            .iter()
            .any(|statement| statement_targets_table(statement, table_name))
    }

    fn statement_targets_table(statement: &Statement, table_name: &str) -> bool {
        match statement {
            Statement::Query(query) => query_targets_table_name(query, table_name),
            _ => false,
        }
    }

    fn query_targets_table_name(query: &Query, table_name: &str) -> bool {
        match query.body.as_ref() {
            SetExpr::Select(select) => select.from.iter().any(|table_with_joins| {
                table_factor_targets_table(&table_with_joins.relation, table_name)
                    || table_with_joins
                        .joins
                        .iter()
                        .any(|join| table_factor_targets_table(&join.relation, table_name))
            }),
            SetExpr::Query(query) => query_targets_table_name(query, table_name),
            _ => false,
        }
    }

    fn table_factor_targets_table(table_factor: &TableFactor, table_name: &str) -> bool {
        match table_factor {
            TableFactor::Table { name, .. } => name
                .0
                .last()
                .and_then(|part| part.as_ident())
                .map(|ident| ident.value.eq_ignore_ascii_case(table_name))
                .unwrap_or(false),
            TableFactor::Derived { subquery, .. } => query_targets_table_name(subquery, table_name),
            _ => false,
        }
    }

    fn query_has_text_equality(sql: &str, column_name: &str, expected: &str) -> bool {
        let Ok(statements) = Parser::parse_sql(&GenericDialect {}, sql) else {
            return false;
        };
        statements
            .iter()
            .any(|statement| statement_has_text_equality(statement, column_name, expected))
    }

    fn statement_has_text_equality(
        statement: &Statement,
        column_name: &str,
        expected: &str,
    ) -> bool {
        match statement {
            Statement::Query(query) => query_has_where_text_equality(query, column_name, expected),
            _ => false,
        }
    }

    fn query_has_where_text_equality(query: &Query, column_name: &str, expected: &str) -> bool {
        match query.body.as_ref() {
            SetExpr::Select(select) => select
                .selection
                .as_ref()
                .is_some_and(|expr| expr_has_text_equality(expr, column_name, expected)),
            SetExpr::Query(query) => query_has_where_text_equality(query, column_name, expected),
            _ => false,
        }
    }

    fn expr_has_text_equality(expr: &Expr, column_name: &str, expected: &str) -> bool {
        match expr {
            Expr::BinaryOp { left, op, right } if *op == BinaryOperator::Eq => {
                expr_identifier_name(left)
                    .is_some_and(|name| name.eq_ignore_ascii_case(column_name))
                    && expr_single_quoted_text(right).is_some_and(|value| value == expected)
                    || expr_identifier_name(right)
                        .is_some_and(|name| name.eq_ignore_ascii_case(column_name))
                        && expr_single_quoted_text(left).is_some_and(|value| value == expected)
            }
            Expr::BinaryOp { left, right, .. } => {
                expr_has_text_equality(left, column_name, expected)
                    || expr_has_text_equality(right, column_name, expected)
            }
            Expr::Nested(inner) => expr_has_text_equality(inner, column_name, expected),
            _ => false,
        }
    }

    fn expr_identifier_name(expr: &Expr) -> Option<&str> {
        match expr {
            Expr::Identifier(ident) => Some(ident.value.as_str()),
            Expr::CompoundIdentifier(parts) => parts.last().map(|ident| ident.value.as_str()),
            _ => None,
        }
    }

    fn expr_single_quoted_text(expr: &Expr) -> Option<&str> {
        match expr {
            Expr::Value(sqlparser::ast::ValueWithSpan {
                value: sqlparser::ast::Value::SingleQuotedString(text),
                ..
            }) => Some(text.as_str()),
            _ => None,
        }
    }

    #[test]
    fn canonical_commit_receipt_uses_latest_change_as_watermark() {
        let canonical_output = CanonicalCommitOutput {
            changes: vec![
                ChangeRow {
                    id: "change-1".to_string(),
                    entity_id: EntityId::new("entity-1").expect("valid entity id"),
                    schema_key: CanonicalSchemaKey::new("lix_key_value").expect("valid schema key"),
                    schema_version: CanonicalSchemaVersion::new("1").expect("valid schema version"),
                    file_id: FileId::new("lix").expect("valid file id"),
                    plugin_key: CanonicalPluginKey::new("lix").expect("valid plugin key"),
                    snapshot_content: None,
                    metadata: None,
                    created_at: "2026-03-06T14:22:00.000Z".to_string(),
                },
                ChangeRow {
                    id: "change-2".to_string(),
                    entity_id: EntityId::new("entity-2").expect("valid entity id"),
                    schema_key: CanonicalSchemaKey::new("lix_key_value").expect("valid schema key"),
                    schema_version: CanonicalSchemaVersion::new("1").expect("valid schema version"),
                    file_id: FileId::new("lix").expect("valid file id"),
                    plugin_key: CanonicalPluginKey::new("lix").expect("valid plugin key"),
                    snapshot_content: None,
                    metadata: None,
                    created_at: "2026-03-06T14:22:01.000Z".to_string(),
                },
            ],
        };
        let updated_version_refs = vec![UpdatedVersionRef {
            version_id: VersionId::new("version-a").expect("valid version id"),
            commit_id: "commit-123".to_string(),
            created_at: "2026-03-06T14:22:01.000Z".to_string(),
        }];

        let receipt = build_canonical_commit_receipt(
            "commit-123".to_string(),
            &canonical_output,
            &updated_version_refs,
            0,
        )
        .expect("receipt should build");

        assert_eq!(receipt.commit_id, "commit-123");
        assert_eq!(receipt.canonical_watermark.change_ordinal, 1);
        assert_eq!(receipt.canonical_watermark.change_id, "change-2");
        assert_eq!(
            receipt.canonical_watermark.created_at,
            "2026-03-06T14:22:01.000Z"
        );
        assert_eq!(receipt.updated_version_refs, updated_version_refs);
        assert_eq!(receipt.affected_versions, vec!["version-a".to_string()]);
    }
}
