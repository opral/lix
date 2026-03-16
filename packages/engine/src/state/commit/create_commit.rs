use std::collections::{BTreeMap, BTreeSet};

use crate::account::{
    active_account_file_id, active_account_schema_key, active_account_storage_version_id,
    parse_active_account_snapshot,
};
use crate::canonical_json::CanonicalJson;
use crate::deterministic_mode::build_persist_sequence_highest_sql;
use crate::functions::LixFunctionProvider;
use crate::schema::builtin::types::LixVersionRef;
use crate::sql::execution::runtime_effects::{
    build_binary_blob_fastcdc_write_program, BinaryBlobWriteInput,
};
use crate::sql::execution::write_program_runner::execute_write_program_with_transaction;
use crate::sql::public::planner::ir::LazyExactFileUpdate;
use crate::state::live_state::{
    build_mark_live_state_ready_sql, ensure_live_state_ready_in_transaction, CanonicalWatermark,
};
use crate::version::version_ref_snapshot_content;
use crate::version::GLOBAL_VERSION_ID;
#[cfg(test)]
use crate::SqlDialect;
use crate::{LixError, LixTransaction, QueryResult, Value};
use async_trait::async_trait;

use super::generate_commit::generate_commit;
use super::runtime::{
    bind_statement_batch_for_dialect, build_statement_batch_from_generate_commit_result,
};
use super::state_source::{
    load_committed_version_head_commit_id_from_live_state,
    load_exact_committed_state_row_from_live_state_with_executor, load_version_info_for_versions,
    CommitQueryExecutor, ExactCommittedStateRowRequest,
};
use super::types::{
    CanonicalCommitOutput, DerivedCommitApplyInput, DomainChangeInput, GenerateCommitArgs,
    GenerateCommitResult, ProposedDomainChange, VersionInfo, VersionSnapshot,
};

const COMMIT_IDEMPOTENCY_TABLE: &str = "lix_internal_commit_idempotency";
const LIVE_UNTRACKED_TABLE: &str = "lix_internal_live_untracked_v1";
const VERSION_REF_SCHEMA_KEY: &str = "lix_version_ref";
const CHANGE_AUTHOR_SCHEMA_KEY: &str = "lix_change_author";
const IDEMPOTENCY_KIND_EXACT: &str = "exact";
const IDEMPOTENCY_KIND_CURRENT_HEAD_FINGERPRINT: &str = "current_head_fingerprint";
const FILESYSTEM_DESCRIPTOR_FILE_ID: &str = "lix";
const FILESYSTEM_DESCRIPTOR_PLUGIN_KEY: &str = "lix";
const FILESYSTEM_FILE_SCHEMA_KEY: &str = "lix_file_descriptor";
const FILESYSTEM_FILE_SCHEMA_VERSION: &str = "1";

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
    pub(crate) lazy_exact_file_updates: Vec<LazyExactFileUpdate>,
    pub(crate) additional_binary_blob_payloads: Vec<Vec<u8>>,
    pub(crate) preconditions: CreateCommitPreconditions,
    pub(crate) should_emit_observe_tick: bool,
    pub(crate) observe_tick_writer_key: Option<String>,
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
    pub(crate) derived_apply_input: DerivedCommitApplyInput,
    pub(crate) operational_apply_input: OperationalCommitApplyInput,
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
        transaction: &mut dyn LixTransaction,
    ) -> Result<(), CreateCommitError>;
}

pub(crate) async fn create_commit(
    transaction: &mut dyn LixTransaction,
    args: CreateCommitArgs,
    functions: &mut dyn LixFunctionProvider,
    invariant_checker: Option<&mut dyn CreateCommitInvariantChecker>,
) -> Result<CreateCommitResult, CreateCommitError> {
    if args.changes.is_empty() {
        if args.lazy_exact_file_updates.is_empty() {
            return Err(CreateCommitError {
                kind: CreateCommitErrorKind::EmptyBatch,
                message: "create_commit requires at least one change".to_string(),
            });
        }
    }

    let concrete_lane = concrete_lane(&args.preconditions)?;
    validate_change_versions(&args.changes, &args.lazy_exact_file_updates, &concrete_lane)?;
    ensure_live_state_ready_in_transaction(transaction)
        .await
        .map_err(backend_error)?;

    let needs_active_accounts = !args
        .changes
        .iter()
        .all(|change| change.schema_key == CHANGE_AUTHOR_SCHEMA_KEY);
    let needs_deterministic_sequence = functions.deterministic_sequence_enabled()
        && !functions.deterministic_sequence_initialized();
    let preflight = {
        let mut executor = TransactionCommitExecutor { transaction };
        load_create_commit_preflight_state_with_active_accounts(
            &mut executor,
            &concrete_lane,
            &args.preconditions,
            &args.lazy_exact_file_updates,
            needs_deterministic_sequence,
            needs_active_accounts,
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
            applied_output: None,
            applied_domain_changes: Vec::new(),
        });
    }

    if let Some(invariant_checker) = invariant_checker {
        invariant_checker.recheck_invariants(transaction).await?;
    }

    let applied_domain_changes =
        resolve_proposed_domain_changes(&args.changes, &args.lazy_exact_file_updates, &preflight)?;
    if applied_domain_changes.is_empty() {
        return Ok(CreateCommitResult {
            disposition: CreateCommitDisposition::Replay,
            committed_head: current_head.unwrap_or_default(),
            applied_output: None,
            applied_domain_changes: Vec::new(),
        });
    }
    let domain_changes =
        materialize_domain_changes(&timestamp, &applied_domain_changes, functions)?;
    let affected_versions = domain_changes
        .iter()
        .map(|change| change.version_id.clone())
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
                parent_commit_ids: current_head.clone().into_iter().collect(),
                snapshot: VersionSnapshot {
                    id: version_id.clone(),
                },
            },
        );
    }
    if matches!(concrete_lane, ConcreteWriteLane::GlobalAdmin) {
        let global_version = versions
            .entry(GLOBAL_VERSION_ID.to_string())
            .or_insert_with(|| VersionInfo {
                parent_commit_ids: Vec::new(),
                snapshot: VersionSnapshot {
                    id: GLOBAL_VERSION_ID.to_string(),
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
        },
        || functions.uuid_v7(),
    )
    .map_err(backend_error)?;
    let committed_head = extract_committed_head_id(&generated_commit, &concrete_lane)?;
    let operational_apply_input = OperationalCommitApplyInput {
        idempotency_write: CommitIdempotencyWrite {
            write_lane: lane_storage_key(&concrete_lane),
            idempotency_key: resolved_idempotency.legacy_key.clone(),
            idempotency_kind: resolved_idempotency.kind.to_string(),
            idempotency_value: resolved_idempotency.value.clone(),
            parent_head_snapshot_content: resolved_idempotency.parent_head_snapshot_content.clone(),
            commit_id: committed_head.clone(),
            created_at: timestamp.clone(),
        },
        deterministic_sequence_highest_seen: functions
            .deterministic_sequence_persist_highest_seen(),
        observe_tick: args.should_emit_observe_tick.then(|| ObserveTickWrite {
            writer_key: args.observe_tick_writer_key.clone(),
        }),
    };
    let applied_output = CreateCommitAppliedOutput {
        canonical_output: generated_commit.canonical_output.clone(),
        derived_apply_input: generated_commit.derived_apply_input.clone(),
        operational_apply_input,
    };
    let live_state_watermark =
        canonical_watermark(&applied_output.canonical_output).ok_or_else(|| CreateCommitError {
            kind: CreateCommitErrorKind::Internal,
            message: "generated commit did not produce a canonical watermark".to_string(),
        })?;

    let mut prepared_batch = bind_statement_batch_for_dialect(
        build_statement_batch_from_generate_commit_result(
            GenerateCommitResult {
                canonical_output: applied_output.canonical_output.clone(),
                derived_apply_input: applied_output.derived_apply_input.clone(),
            },
            functions,
            0,
            transaction.dialect(),
        )
        .map_err(backend_error)?,
        transaction.dialect(),
    )
    .map_err(backend_error)?;
    prepared_batch.append_sql(insert_idempotency_row_sql(
        &applied_output.operational_apply_input.idempotency_write,
    ));
    if let Some(highest_seen) = applied_output
        .operational_apply_input
        .deterministic_sequence_highest_seen
    {
        prepared_batch.append_sql(build_persist_sequence_highest_sql(highest_seen));
    }
    if let Some(observe_tick) = applied_output.operational_apply_input.observe_tick.as_ref() {
        prepared_batch.append_sql(build_observe_tick_insert_sql(
            observe_tick.writer_key.as_deref(),
        ));
    }
    prepared_batch.append_sql(build_mark_live_state_ready_sql(&live_state_watermark));

    let payloads = args
        .lazy_exact_file_updates
        .iter()
        .filter_map(|lazy| match lazy {
            LazyExactFileUpdate::Data(lazy) => Some(BinaryBlobWriteInput {
                file_id: &lazy.file_id,
                version_id: &lazy.version_id,
                data: &lazy.data,
            }),
            _ => None,
        })
        .chain(
            args.additional_binary_blob_payloads
                .iter()
                .map(|data| BinaryBlobWriteInput {
                    file_id: "",
                    version_id: "",
                    data,
                }),
        )
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
        applied_output: Some(applied_output),
        applied_domain_changes,
    })
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
    transaction: &'a mut dyn LixTransaction,
}

#[async_trait(?Send)]
impl CommitQueryExecutor for TransactionCommitExecutor<'_> {
    #[cfg(test)]
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
    lazy_exact_file_updates: &[LazyExactFileUpdate],
    preflight: &CreateCommitPreflightState,
) -> Result<Vec<ProposedDomainChange>, CreateCommitError> {
    if lazy_exact_file_updates.is_empty() {
        return Ok(changes.to_vec());
    }
    let mut resolved = changes.to_vec();
    for lazy in lazy_exact_file_updates {
        match lazy {
            LazyExactFileUpdate::Metadata(lazy) => {
                let current = required_exact_file_descriptor(preflight, &lazy.file_id)?;
                resolved.push(ProposedDomainChange {
                    entity_id: lazy.file_id.clone(),
                    schema_key: FILESYSTEM_FILE_SCHEMA_KEY.to_string(),
                    schema_version: Some(FILESYSTEM_FILE_SCHEMA_VERSION.to_string()),
                    file_id: Some(FILESYSTEM_DESCRIPTOR_FILE_ID.to_string()),
                    version_id: lazy.version_id.clone(),
                    plugin_key: Some(FILESYSTEM_DESCRIPTOR_PLUGIN_KEY.to_string()),
                    snapshot_content: Some(
                        serde_json::json!({
                            "id": lazy.file_id,
                            "directory_id": current.directory_id,
                            "name": current.name,
                            "extension": current.extension,
                            "metadata": lazy.metadata.apply(current.metadata.clone()),
                            "hidden": current.hidden,
                        })
                        .to_string(),
                    ),
                    metadata: lazy.metadata.apply(current.metadata.clone()),
                    writer_key: None,
                });
            }
            LazyExactFileUpdate::Data(lazy) => {
                let _current = required_exact_file_descriptor(preflight, &lazy.file_id)?;
                resolved.push(ProposedDomainChange {
                    entity_id: lazy.file_id.clone(),
                    schema_key: "lix_binary_blob_ref".to_string(),
                    schema_version: Some("1".to_string()),
                    file_id: Some(lazy.file_id.clone()),
                    version_id: lazy.version_id.clone(),
                    plugin_key: Some(FILESYSTEM_DESCRIPTOR_PLUGIN_KEY.to_string()),
                    snapshot_content: Some(
                        serde_json::json!({
                            "id": lazy.file_id,
                            "blob_hash": crate::plugin::runtime::binary_blob_hash_hex(&lazy.data),
                            "size_bytes": u64::try_from(lazy.data.len()).map_err(|_| CreateCommitError {
                                kind: CreateCommitErrorKind::Internal,
                                message: format!(
                                    "exact file data update exceeds supported size for '{}'",
                                    lazy.file_id
                                ),
                            })?,
                        })
                        .to_string(),
                    ),
                    metadata: None,
                    writer_key: None,
                });
            }
            LazyExactFileUpdate::Delete(lazy) => {
                for file_id in &lazy.file_ids {
                    let Some(current) = preflight.file_descriptors.get(file_id) else {
                        continue;
                    };
                    if current.untracked {
                        return Err(CreateCommitError {
                            kind: CreateCommitErrorKind::Internal,
                            message:
                                "lazy exact file update does not support untracked visible rows"
                                    .to_string(),
                        });
                    }
                    resolved.push(ProposedDomainChange {
                        entity_id: file_id.clone(),
                        schema_key: FILESYSTEM_FILE_SCHEMA_KEY.to_string(),
                        schema_version: Some(FILESYSTEM_FILE_SCHEMA_VERSION.to_string()),
                        file_id: Some(FILESYSTEM_DESCRIPTOR_FILE_ID.to_string()),
                        version_id: lazy.version_id.clone(),
                        plugin_key: Some(FILESYSTEM_DESCRIPTOR_PLUGIN_KEY.to_string()),
                        snapshot_content: None,
                        metadata: None,
                        writer_key: None,
                    });
                    resolved.push(ProposedDomainChange {
                        entity_id: file_id.clone(),
                        schema_key: "lix_binary_blob_ref".to_string(),
                        schema_version: Some("1".to_string()),
                        file_id: Some(file_id.clone()),
                        version_id: lazy.version_id.clone(),
                        plugin_key: Some(FILESYSTEM_DESCRIPTOR_PLUGIN_KEY.to_string()),
                        snapshot_content: None,
                        metadata: None,
                        writer_key: None,
                    });
                }
            }
        }
    }
    Ok(resolved)
}

fn required_exact_file_descriptor<'a>(
    preflight: &'a CreateCommitPreflightState,
    file_id: &str,
) -> Result<&'a CreateCommitPreflightFileDescriptor, CreateCommitError> {
    let current = preflight
        .file_descriptors
        .get(file_id)
        .ok_or_else(|| CreateCommitError {
            kind: CreateCommitErrorKind::Internal,
            message: format!(
                "create commit preflight did not load the exact file descriptor row for '{}'",
                file_id
            ),
        })?;
    if current.untracked {
        return Err(CreateCommitError {
            kind: CreateCommitErrorKind::Internal,
            message: "lazy exact file update does not support untracked visible rows".to_string(),
        });
    }
    Ok(current)
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

fn require_change_field(
    value: Option<String>,
    schema_key: &str,
    field_name: &str,
) -> Result<String, CreateCommitError> {
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
    file_descriptors: BTreeMap<String, CreateCommitPreflightFileDescriptor>,
}

struct CreateCommitPreflightFileDescriptor {
    directory_id: Option<String>,
    name: String,
    extension: Option<String>,
    hidden: bool,
    metadata: Option<String>,
    untracked: bool,
}

async fn load_create_commit_preflight_state_with_active_accounts(
    executor: &mut dyn CommitQueryExecutor,
    concrete_lane: &ConcreteWriteLane,
    preconditions: &CreateCommitPreconditions,
    lazy_exact_file_updates: &[LazyExactFileUpdate],
    include_deterministic_sequence: bool,
    include_active_accounts: bool,
) -> Result<CreateCommitPreflightState, CreateCommitError> {
    let lane_entity_id = match concrete_lane {
        ConcreteWriteLane::Version { version_id } => version_id.as_str(),
        ConcreteWriteLane::GlobalAdmin => GLOBAL_VERSION_ID,
    };
    let current_head =
        load_committed_version_head_commit_id_from_live_state(executor, lane_entity_id)
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
    let active_accounts = if include_active_accounts {
        load_create_commit_active_accounts(executor).await?
    } else {
        Vec::new()
    };
    let file_descriptors =
        load_create_commit_file_descriptors(executor, lazy_exact_file_updates, lane_entity_id)
            .await?;

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
) -> Result<CreateCommitPreflightFileDescriptor, CreateCommitError> {
    let parsed: serde_json::Value =
        serde_json::from_str(snapshot_content).map_err(|error| CreateCommitError {
            kind: CreateCommitErrorKind::Internal,
            message: format!(
                "create commit preflight file descriptor snapshot could not be parsed: {error}"
            ),
        })?;
    Ok(CreateCommitPreflightFileDescriptor {
        directory_id: parsed.get("directory_id").and_then(|value| match value {
            serde_json::Value::Null => None,
            serde_json::Value::String(text) => Some(text.clone()),
            _ => None,
        }),
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
    let sql = format!(
        "SELECT snapshot_content \
         FROM {table_name} \
         WHERE schema_key = 'lix_key_value' \
           AND entity_id = 'lix_deterministic_sequence_number' \
           AND version_id = '{version_id}' \
           AND snapshot_content IS NOT NULL \
         ORDER BY updated_at DESC \
         LIMIT 1",
        table_name = LIVE_UNTRACKED_TABLE,
        version_id = GLOBAL_VERSION_ID,
    );
    let result = executor.execute(&sql, &[]).await.map_err(backend_error)?;
    if let Some(snapshot_content) = result
        .rows
        .first()
        .and_then(|row| row.first())
        .and_then(value_as_text)
    {
        return parse_deterministic_sequence_snapshot(&snapshot_content).map(Some);
    }

    let tracked = load_exact_committed_state_row_from_live_state_with_executor(
        executor,
        &ExactCommittedStateRowRequest {
            entity_id: "lix_deterministic_sequence_number".to_string(),
            schema_key: "lix_key_value".to_string(),
            version_id: GLOBAL_VERSION_ID.to_string(),
            exact_filters: BTreeMap::from([
                (
                    "file_id".to_string(),
                    Value::Text(FILESYSTEM_DESCRIPTOR_FILE_ID.to_string()),
                ),
                (
                    "plugin_key".to_string(),
                    Value::Text(FILESYSTEM_DESCRIPTOR_PLUGIN_KEY.to_string()),
                ),
            ]),
        },
    )
    .await
    .map_err(backend_error)?;
    let Some(snapshot_content) = tracked
        .as_ref()
        .and_then(|row| row.values.get("snapshot_content"))
        .and_then(value_as_text)
    else {
        return Ok(Some(0));
    };
    parse_deterministic_sequence_snapshot(&snapshot_content).map(Some)
}

async fn load_create_commit_active_accounts(
    executor: &mut dyn CommitQueryExecutor,
) -> Result<Vec<String>, CreateCommitError> {
    let sql = format!(
        "SELECT snapshot_content \
         FROM {table_name} \
         WHERE schema_key = '{schema_key}' \
           AND file_id = '{file_id}' \
           AND version_id = '{version_id}' \
           AND snapshot_content IS NOT NULL",
        table_name = LIVE_UNTRACKED_TABLE,
        schema_key = escape_sql_string(active_account_schema_key()),
        file_id = escape_sql_string(active_account_file_id()),
        version_id = escape_sql_string(active_account_storage_version_id()),
    );
    let result = executor.execute(&sql, &[]).await.map_err(backend_error)?;
    let mut active_accounts = BTreeSet::new();
    for row in result.rows {
        let Some(snapshot_content) = row.first().and_then(value_as_text) else {
            continue;
        };
        let account_id = parse_active_account_snapshot(&snapshot_content).map_err(backend_error)?;
        active_accounts.insert(account_id);
    }
    Ok(active_accounts.into_iter().collect())
}

async fn load_create_commit_file_descriptors(
    executor: &mut dyn CommitQueryExecutor,
    lazy_exact_file_updates: &[LazyExactFileUpdate],
    lane_entity_id: &str,
) -> Result<BTreeMap<String, CreateCommitPreflightFileDescriptor>, CreateCommitError> {
    let exact_file_ids = lazy_exact_file_updates
        .iter()
        .flat_map(LazyExactFileUpdate::file_ids)
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
) -> Result<Option<CreateCommitPreflightFileDescriptor>, CreateCommitError> {
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
) -> Result<Option<CreateCommitPreflightFileDescriptor>, CreateCommitError> {
    let sql = format!(
        "SELECT snapshot_content, metadata \
         FROM {table_name} \
         WHERE entity_id = '{entity_id}' \
           AND schema_key = '{schema_key}' \
           AND file_id = '{file_id}' \
           AND version_id = '{version_id}' \
           AND snapshot_content IS NOT NULL \
         ORDER BY updated_at DESC \
         LIMIT 1",
        table_name = LIVE_UNTRACKED_TABLE,
        entity_id = escape_sql_string(file_id),
        schema_key = escape_sql_string(FILESYSTEM_FILE_SCHEMA_KEY),
        file_id = escape_sql_string(FILESYSTEM_DESCRIPTOR_FILE_ID),
        version_id = escape_sql_string(version_id),
    );
    let result = executor.execute(&sql, &[]).await.map_err(backend_error)?;
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    let Some(snapshot_content) = row.first().and_then(value_as_text) else {
        return Ok(None);
    };
    let metadata = row.get(1).and_then(value_as_text);
    parse_file_descriptor_preflight_row(&snapshot_content, metadata, true).map(Some)
}

async fn load_tracked_file_descriptor(
    executor: &mut dyn CommitQueryExecutor,
    file_id: &str,
    version_id: &str,
) -> Result<Option<CreateCommitPreflightFileDescriptor>, CreateCommitError> {
    let row = load_exact_committed_state_row_from_live_state_with_executor(
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
    lazy_exact_file_updates: &[LazyExactFileUpdate],
    concrete_lane: &ConcreteWriteLane,
) -> Result<(), CreateCommitError> {
    if !lazy_exact_file_updates.is_empty() {
        let expected_version_id = match concrete_lane {
            ConcreteWriteLane::Version { version_id } => version_id,
            ConcreteWriteLane::GlobalAdmin => GLOBAL_VERSION_ID,
        };
        if lazy_exact_file_updates
            .iter()
            .any(|lazy| lazy.version_id() != expected_version_id)
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
    let pointer_change = commit_result
        .canonical_output
        .changes
        .iter()
        .find(|change| {
            change.schema_key == VERSION_REF_SCHEMA_KEY && change.entity_id == version_id
        })
        .ok_or_else(|| CreateCommitError {
            kind: CreateCommitErrorKind::Internal,
            message: format!(
                "generated commit result did not include a version ref for '{}'",
                version_id
            ),
        })?;
    let snapshot_content =
        pointer_change
            .snapshot_content
            .as_ref()
            .ok_or_else(|| CreateCommitError {
                kind: CreateCommitErrorKind::Internal,
                message: format!(
                    "generated version ref for '{}' is missing snapshot_content",
                    version_id
                ),
            })?;
    let pointer: LixVersionRef =
        serde_json::from_str(snapshot_content).map_err(|error| CreateCommitError {
            kind: CreateCommitErrorKind::Internal,
            message: format!(
                "generated version ref for '{}' could not be parsed: {error}",
                version_id
            ),
        })?;
    if pointer.commit_id.is_empty() {
        return Err(CreateCommitError {
            kind: CreateCommitErrorKind::Internal,
            message: format!(
                "generated version ref for '{}' contained an empty commit_id",
                version_id
            ),
        });
    }
    Ok(pointer.commit_id)
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

fn canonical_watermark(canonical_output: &CanonicalCommitOutput) -> Option<CanonicalWatermark> {
    canonical_output
        .changes
        .iter()
        .max_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.cmp(&right.id))
        })
        .map(|change| CanonicalWatermark {
            change_id: change.id.clone(),
            created_at: change.created_at.clone(),
        })
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

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::{
        create_commit, CreateCommitArgs, CreateCommitDisposition, CreateCommitError,
        CreateCommitErrorKind, CreateCommitExpectedHead, CreateCommitIdempotencyKey,
        CreateCommitInvariantChecker, CreateCommitPreconditions, CreateCommitWriteLane,
    };
    use crate::functions::LixFunctionProvider;
    use crate::sql::public::planner::ir::{LazyExactFileDataUpdate, LazyExactFileUpdate};
    use crate::version::GLOBAL_VERSION_ID;
    use crate::{LixError, LixTransaction, QueryResult, SqlDialect, Value};
    use async_trait::async_trait;
    use std::collections::HashMap;

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
    }

    #[async_trait(?Send)]
    impl LixTransaction for FakeTransaction {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
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
                        Value::Text(crate::state::live_state::LIVE_STATE_SCHEMA_EPOCH.to_string()),
                    ]],
                    columns: vec![
                        "mode".to_string(),
                        "latest_change_id".to_string(),
                        "latest_change_created_at".to_string(),
                        "schema_epoch".to_string(),
                    ],
                });
            }

            if sql.contains("FROM lix_internal_live_v1_lix_version_ref") {
                let rows = self
                    .version_heads
                    .iter()
                    .filter(|(version_id, _)| {
                        sql.contains(&format!("entity_id = '{}'", version_id))
                    })
                    .map(|(version_id, commit_id)| {
                        vec![Value::Text(crate::version::version_ref_snapshot_content(
                            version_id, commit_id,
                        ))]
                    })
                    .collect::<Vec<_>>();
                return Ok(QueryResult {
                    rows,
                    columns: vec!["snapshot_content".to_string()],
                });
            }
            if sql.contains("FROM \"lix_internal_live_v1_lix_file_descriptor\"") {
                return Ok(QueryResult {
                    rows: vec![vec![
                        Value::Text("file-1".to_string()),
                        Value::Text("lix_file_descriptor".to_string()),
                        Value::Text("1".to_string()),
                        Value::Text("lix".to_string()),
                        Value::Text("version-a".to_string()),
                        Value::Text("lix".to_string()),
                        Value::Text(
                            serde_json::json!({
                                "id": "file-1",
                                "directory_id": serde_json::Value::Null,
                                "name": "contract",
                                "extension": "txt",
                                "hidden": false,
                                "metadata": serde_json::Value::Null,
                            })
                            .to_string(),
                        ),
                        Value::Null,
                        Value::Text("change-file-1".to_string()),
                    ]],
                    columns: vec![
                        "entity_id".to_string(),
                        "schema_key".to_string(),
                        "schema_version".to_string(),
                        "file_id".to_string(),
                        "version_id".to_string(),
                        "plugin_key".to_string(),
                        "snapshot_content".to_string(),
                        "metadata".to_string(),
                        "change_id".to_string(),
                    ],
                });
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

        async fn execute_batch(
            &mut self,
            batch: &crate::sql::execution::contracts::prepared_statement::PreparedBatch,
        ) -> Result<QueryResult, LixError> {
            let collapsed =
                crate::sql::execution::contracts::prepared_statement::collapse_prepared_batch_for_dialect(
                    batch,
                    self.dialect(),
                )?;
            self.execute(&collapsed.sql, &collapsed.params).await
        }

        async fn commit(self: Box<Self>) -> Result<(), LixError> {
            Ok(())
        }

        async fn rollback(self: Box<Self>) -> Result<(), LixError> {
            Ok(())
        }
    }

    fn sample_change() -> crate::state::commit::ProposedDomainChange {
        crate::state::commit::ProposedDomainChange {
            entity_id: "entity-1".to_string(),
            schema_key: "lix_key_value".to_string(),
            schema_version: Some("1".to_string()),
            file_id: Some("lix".to_string()),
            plugin_key: Some("lix".to_string()),
            snapshot_content: Some("{\"key\":\"hello\"}".to_string()),
            metadata: None,
            version_id: "version-a".to_string(),
            writer_key: Some("writer-a".to_string()),
        }
    }

    fn sample_global_change() -> crate::state::commit::ProposedDomainChange {
        crate::state::commit::ProposedDomainChange {
            entity_id: "version-a".to_string(),
            schema_key: "lix_version_descriptor".to_string(),
            schema_version: Some("1".to_string()),
            file_id: Some(crate::version::version_descriptor_file_id().to_string()),
            plugin_key: Some(crate::version::version_descriptor_plugin_key().to_string()),
            snapshot_content: Some(crate::version::version_descriptor_snapshot_content(
                "version-a",
                "Version A",
                false,
            )),
            metadata: None,
            version_id: GLOBAL_VERSION_ID.to_string(),
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
            _transaction: &mut dyn LixTransaction,
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
                lazy_exact_file_updates: Vec::new(),
                additional_binary_blob_payloads: Vec::new(),
                preconditions: CreateCommitPreconditions {
                    write_lane: CreateCommitWriteLane::Version("version-a".to_string()),
                    expected_head: CreateCommitExpectedHead::CommitId("commit-123".to_string()),
                    idempotency_key: CreateCommitIdempotencyKey::Exact("idem-1".to_string()),
                },
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
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
            generated_commit_batches[0].contains("; INSERT INTO lix_internal_live_v1_"),
            "generated commit batch should include live-state writes in the same execute call"
        );
        assert!(
            transaction
                .executed_sql
                .iter()
                .any(|sql| sql.contains("INSERT INTO lix_internal_commit_idempotency ")),
            "create_commit should persist idempotency state in the executed batch"
        );
        assert!(
            transaction
                .executed_sql
                .iter()
                .any(|sql| sql.contains("INSERT INTO lix_internal_live_state_status ")),
            "create_commit should update live-state readiness in the executed batch"
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
                lazy_exact_file_updates: Vec::new(),
                additional_binary_blob_payloads: Vec::new(),
                preconditions: CreateCommitPreconditions {
                    write_lane: CreateCommitWriteLane::Version("version-a".to_string()),
                    expected_head: CreateCommitExpectedHead::CommitId("commit-123".to_string()),
                    idempotency_key: CreateCommitIdempotencyKey::Exact("idem-1".to_string()),
                },
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
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
                lazy_exact_file_updates: Vec::new(),
                additional_binary_blob_payloads: Vec::new(),
                preconditions: CreateCommitPreconditions {
                    write_lane: CreateCommitWriteLane::Version("version-a".to_string()),
                    expected_head: CreateCommitExpectedHead::CurrentHead,
                    idempotency_key: CreateCommitIdempotencyKey::CurrentHeadFingerprint(
                        "fp-1".to_string(),
                    ),
                },
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
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
                lazy_exact_file_updates: Vec::new(),
                additional_binary_blob_payloads: Vec::new(),
                preconditions: CreateCommitPreconditions {
                    write_lane: CreateCommitWriteLane::Version("version-a".to_string()),
                    expected_head: CreateCommitExpectedHead::CommitId("commit-123".to_string()),
                    idempotency_key: CreateCommitIdempotencyKey::Exact("idem-1".to_string()),
                },
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
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
                lazy_exact_file_updates: Vec::new(),
                additional_binary_blob_payloads: Vec::new(),
                preconditions: CreateCommitPreconditions {
                    write_lane: CreateCommitWriteLane::Version("version-a".to_string()),
                    expected_head: CreateCommitExpectedHead::CommitId("commit-123".to_string()),
                    idempotency_key: CreateCommitIdempotencyKey::Exact("idem-1".to_string()),
                },
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
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
                lazy_exact_file_updates: Vec::new(),
                additional_binary_blob_payloads: Vec::new(),
                preconditions: CreateCommitPreconditions {
                    write_lane: CreateCommitWriteLane::Version("version-a".to_string()),
                    expected_head: CreateCommitExpectedHead::CreateIfMissing,
                    idempotency_key: CreateCommitIdempotencyKey::Exact("idem-create".to_string()),
                },
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
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
                lazy_exact_file_updates: Vec::new(),
                additional_binary_blob_payloads: Vec::new(),
                preconditions: CreateCommitPreconditions {
                    write_lane: CreateCommitWriteLane::GlobalAdmin,
                    expected_head: CreateCommitExpectedHead::CommitId(
                        "commit-global-123".to_string(),
                    ),
                    idempotency_key: CreateCommitIdempotencyKey::Exact("idem-global".to_string()),
                },
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
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
    async fn lazy_exact_file_update_uses_live_descriptor_lookup() {
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
                lazy_exact_file_updates: vec![LazyExactFileUpdate::Data(LazyExactFileDataUpdate {
                    file_id: "file-1".to_string(),
                    version_id: "version-a".to_string(),
                    data: vec![1, 2, 3],
                })],
                additional_binary_blob_payloads: Vec::new(),
                preconditions: CreateCommitPreconditions {
                    write_lane: CreateCommitWriteLane::Version("version-a".to_string()),
                    expected_head: CreateCommitExpectedHead::CommitId("commit-123".to_string()),
                    idempotency_key: CreateCommitIdempotencyKey::Exact(
                        "idem-file-data".to_string(),
                    ),
                },
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
            },
            &mut functions,
            None,
        )
        .await
        .expect("lazy exact file update should succeed");

        assert_eq!(result.disposition, CreateCommitDisposition::Applied);
        assert!(
            transaction
                .executed_sql
                .iter()
                .any(|sql| { sql.contains("FROM \"lix_internal_live_v1_lix_file_descriptor\"") }),
            "create_commit should read lazy exact file descriptors from live state"
        );
    }

    #[tokio::test]
    async fn rejects_create_commit_when_live_state_is_not_ready() {
        let mut transaction = FakeTransaction {
            live_state_mode: Some("needs_rebuild".to_string()),
            ..FakeTransaction::default()
        };
        let mut functions = CountingFunctionProvider::default();

        let error = create_commit(
            &mut transaction,
            CreateCommitArgs {
                timestamp: Some("2026-03-06T14:22:00.000Z".to_string()),
                changes: vec![sample_change()],
                lazy_exact_file_updates: Vec::new(),
                additional_binary_blob_payloads: Vec::new(),
                preconditions: CreateCommitPreconditions {
                    write_lane: CreateCommitWriteLane::Version("version-a".to_string()),
                    expected_head: CreateCommitExpectedHead::CommitId("commit-123".to_string()),
                    idempotency_key: CreateCommitIdempotencyKey::Exact("idem-1".to_string()),
                },
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
            },
            &mut functions,
            None,
        )
        .await
        .expect_err("live-state readiness should gate create_commit");

        assert_eq!(error.kind, CreateCommitErrorKind::Internal);
        assert!(
            error.message.contains("live state is not ready"),
            "unexpected error: {}",
            error.message
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
                lazy_exact_file_updates: Vec::new(),
                additional_binary_blob_payloads: Vec::new(),
                preconditions: CreateCommitPreconditions {
                    write_lane: CreateCommitWriteLane::Version("version-a".to_string()),
                    expected_head: CreateCommitExpectedHead::CommitId("commit-123".to_string()),
                    idempotency_key: CreateCommitIdempotencyKey::Exact("idem-1".to_string()),
                },
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
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
}
