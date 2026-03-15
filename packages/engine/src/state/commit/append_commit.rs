use std::collections::{BTreeMap, BTreeSet};

use crate::account::{
    active_account_file_id, active_account_schema_key, active_account_storage_version_id,
    parse_active_account_snapshot,
};
use crate::deterministic_mode::build_persist_sequence_highest_sql;
use crate::functions::LixFunctionProvider;
use crate::schema::builtin::types::LixVersionRef;
use crate::sql::execution::runtime_effects::{
    build_binary_blob_fastcdc_write_program, BinaryBlobWriteInput,
};
use crate::sql::execution::write_program_runner::execute_write_program_with_transaction;
use crate::sql::public::planner::ir::LazyExactFileUpdate;
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixError, LixTransaction, QueryResult, Value};
use async_trait::async_trait;

use super::generate_commit::generate_commit;
use super::runtime::{
    bind_statement_batch_for_dialect, build_statement_batch_from_generate_commit_result,
};
use super::state_source::{load_version_info_for_versions, CommitQueryExecutor};
use super::types::{
    DomainChangeInput, GenerateCommitArgs, GenerateCommitResult, ProposedDomainChange, VersionInfo,
    VersionSnapshot,
};

const COMMIT_IDEMPOTENCY_TABLE: &str = "lix_internal_commit_idempotency";
const LIVE_VERSION_REF_TABLE: &str = "lix_internal_live_v1_lix_version_ref";
const LIVE_UNTRACKED_TABLE: &str = "lix_internal_live_untracked_v1";
const VERSION_REF_SCHEMA_KEY: &str = "lix_version_ref";
const CHANGE_AUTHOR_SCHEMA_KEY: &str = "lix_change_author";
const IDEMPOTENCY_KIND_EXACT: &str = "exact";
const IDEMPOTENCY_KIND_CURRENT_TIP_FINGERPRINT: &str = "current_tip_fingerprint";
const FILESYSTEM_DESCRIPTOR_FILE_ID: &str = "lix";
const FILESYSTEM_DESCRIPTOR_PLUGIN_KEY: &str = "lix";
const FILESYSTEM_FILE_SCHEMA_KEY: &str = "lix_file_descriptor";
const FILESYSTEM_FILE_SCHEMA_VERSION: &str = "1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AppendWriteLane {
    Version(String),
    GlobalAdmin,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AppendExpectedTip {
    CurrentTip,
    CommitId(String),
    CreateIfMissing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AppendIdempotencyKey {
    Exact(String),
    CurrentTipFingerprint(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AppendCommitPreconditions {
    pub(crate) write_lane: AppendWriteLane,
    pub(crate) expected_tip: AppendExpectedTip,
    pub(crate) idempotency_key: AppendIdempotencyKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AppendCommitArgs {
    pub(crate) timestamp: Option<String>,
    pub(crate) changes: Vec<ProposedDomainChange>,
    pub(crate) lazy_exact_file_updates: Vec<LazyExactFileUpdate>,
    pub(crate) preconditions: AppendCommitPreconditions,
    pub(crate) should_emit_observe_tick: bool,
    pub(crate) observe_tick_writer_key: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppendCommitDisposition {
    Applied,
    Replay,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AppendCommitResult {
    pub(crate) disposition: AppendCommitDisposition,
    pub(crate) committed_tip: String,
    pub(crate) commit_result: Option<GenerateCommitResult>,
    pub(crate) applied_domain_changes: Vec<ProposedDomainChange>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppendCommitErrorKind {
    EmptyBatch,
    MissingDomainField,
    MissingWriteLane,
    TipDrift,
    UnsupportedWriteLane,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AppendCommitError {
    pub(crate) kind: AppendCommitErrorKind,
    pub(crate) message: String,
}

#[async_trait(?Send)]
pub(crate) trait AppendCommitInvariantChecker {
    async fn recheck_invariants(
        &mut self,
        transaction: &mut dyn LixTransaction,
    ) -> Result<(), AppendCommitError>;
}

pub(crate) async fn append_commit_if_preconditions_hold(
    transaction: &mut dyn LixTransaction,
    args: AppendCommitArgs,
    functions: &mut dyn LixFunctionProvider,
    invariant_checker: Option<&mut dyn AppendCommitInvariantChecker>,
) -> Result<AppendCommitResult, AppendCommitError> {
    if args.changes.is_empty() {
        if args.lazy_exact_file_updates.is_empty() {
            return Err(AppendCommitError {
                kind: AppendCommitErrorKind::EmptyBatch,
                message: "append_commit_if_preconditions_hold requires at least one change"
                    .to_string(),
            });
        }
    }

    let concrete_lane = concrete_lane(&args.preconditions)?;
    validate_change_versions(&args.changes, &args.lazy_exact_file_updates, &concrete_lane)?;

    let needs_active_accounts = !args
        .changes
        .iter()
        .all(|change| change.schema_key == CHANGE_AUTHOR_SCHEMA_KEY);
    let needs_deterministic_sequence = functions.deterministic_sequence_enabled()
        && !functions.deterministic_sequence_initialized();
    let preflight = {
        let mut executor = TransactionCommitExecutor { transaction };
        load_append_preflight_state_with_active_accounts(
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
    let current_tip = preflight.current_tip.clone();
    let existing_replay = preflight.existing_replay.clone();
    let timestamp = args
        .timestamp
        .clone()
        .unwrap_or_else(|| functions.timestamp());

    match (&args.preconditions.expected_tip, current_tip.as_deref()) {
        (AppendExpectedTip::CurrentTip, Some(_)) => {}
        (AppendExpectedTip::CurrentTip, None) => {
            return Err(AppendCommitError {
                kind: AppendCommitErrorKind::MissingWriteLane,
                message: format!(
                    "append precondition failed for '{}': version ref is missing",
                    lane_storage_key(&concrete_lane)
                ),
            });
        }
        (AppendExpectedTip::CommitId(expected), Some(current)) if current != expected => {
            if existing_replay.as_deref() == Some(current) {
                return Ok(AppendCommitResult {
                    disposition: AppendCommitDisposition::Replay,
                    committed_tip: current.to_string(),
                    commit_result: None,
                    applied_domain_changes: Vec::new(),
                });
            }
            return Err(AppendCommitError {
                kind: AppendCommitErrorKind::TipDrift,
                message: format!(
                    "append precondition failed for '{}': expected tip '{}', found '{}'",
                    lane_storage_key(&concrete_lane),
                    expected,
                    current
                ),
            });
        }
        (AppendExpectedTip::CommitId(_), None) => {
            return Err(AppendCommitError {
                kind: AppendCommitErrorKind::MissingWriteLane,
                message: format!(
                    "append precondition failed for '{}': version ref is missing",
                    lane_storage_key(&concrete_lane)
                ),
            });
        }
        (AppendExpectedTip::CreateIfMissing, Some(current)) => {
            if existing_replay.as_deref() == Some(current) {
                return Ok(AppendCommitResult {
                    disposition: AppendCommitDisposition::Replay,
                    committed_tip: current.to_string(),
                    commit_result: None,
                    applied_domain_changes: Vec::new(),
                });
            }
            return Err(AppendCommitError {
                kind: AppendCommitErrorKind::TipDrift,
                message: format!(
                    "append precondition failed for '{}': lane already exists at '{}'",
                    lane_storage_key(&concrete_lane),
                    current
                ),
            });
        }
        (AppendExpectedTip::CreateIfMissing, None) | (AppendExpectedTip::CommitId(_), Some(_)) => {}
    }

    if let Some(commit_id) = existing_replay {
        return Ok(AppendCommitResult {
            disposition: AppendCommitDisposition::Replay,
            committed_tip: commit_id,
            commit_result: None,
            applied_domain_changes: Vec::new(),
        });
    }

    if let Some(invariant_checker) = invariant_checker {
        invariant_checker.recheck_invariants(transaction).await?;
    }

    let applied_domain_changes =
        resolve_proposed_domain_changes(&args.changes, &args.lazy_exact_file_updates, &preflight)?;
    if applied_domain_changes.is_empty() {
        return Ok(AppendCommitResult {
            disposition: AppendCommitDisposition::Replay,
            committed_tip: current_tip.unwrap_or_default(),
            commit_result: None,
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
                parent_commit_ids: current_tip.clone().into_iter().collect(),
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
        global_version.parent_commit_ids = current_tip.clone().into_iter().collect();
    }
    let commit_result = generate_commit(
        GenerateCommitArgs {
            timestamp: timestamp.clone(),
            active_accounts: preflight.active_accounts,
            changes: domain_changes,
            versions,
        },
        || functions.uuid_v7(),
    )
    .map_err(backend_error)?;
    let committed_tip = extract_committed_tip_id(&commit_result, &concrete_lane)?;

    let mut prepared_batch = bind_statement_batch_for_dialect(
        build_statement_batch_from_generate_commit_result(
            commit_result.clone(),
            functions,
            0,
            transaction.dialect(),
        )
        .map_err(backend_error)?,
        transaction.dialect(),
    )
    .map_err(backend_error)?;
    prepared_batch.append_sql(insert_idempotency_row_sql(
        &concrete_lane,
        &resolved_idempotency,
        &committed_tip,
        &timestamp,
    ));
    if let Some(highest_seen) = functions.deterministic_sequence_persist_highest_seen() {
        prepared_batch.append_sql(build_persist_sequence_highest_sql(highest_seen));
    }
    if args.should_emit_observe_tick {
        prepared_batch.append_sql(build_observe_tick_insert_sql(
            args.observe_tick_writer_key.as_deref(),
        ));
    }

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
        .collect::<Vec<_>>();
    let mut write_program =
        build_binary_blob_fastcdc_write_program(transaction.dialect(), &payloads)
            .map_err(backend_error)?;
    write_program.push_batch(prepared_batch);
    execute_write_program_with_transaction(transaction, write_program)
        .await
        .map_err(backend_error)?;

    Ok(AppendCommitResult {
        disposition: AppendCommitDisposition::Applied,
        committed_tip,
        commit_result: Some(commit_result),
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
    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        self.transaction.execute(sql, params).await
    }
}

fn concrete_lane(
    preconditions: &AppendCommitPreconditions,
) -> Result<ConcreteWriteLane, AppendCommitError> {
    match &preconditions.write_lane {
        AppendWriteLane::Version(version_id) => Ok(ConcreteWriteLane::Version {
            version_id: version_id.clone(),
        }),
        AppendWriteLane::GlobalAdmin => Ok(ConcreteWriteLane::GlobalAdmin),
    }
}

fn resolve_proposed_domain_changes(
    changes: &[ProposedDomainChange],
    lazy_exact_file_updates: &[LazyExactFileUpdate],
    preflight: &AppendPreflightState,
) -> Result<Vec<ProposedDomainChange>, AppendCommitError> {
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
                            "size_bytes": u64::try_from(lazy.data.len()).map_err(|_| AppendCommitError {
                                kind: AppendCommitErrorKind::Internal,
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
                        return Err(AppendCommitError {
                            kind: AppendCommitErrorKind::Internal,
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
    preflight: &'a AppendPreflightState,
    file_id: &str,
) -> Result<&'a AppendPreflightFileDescriptor, AppendCommitError> {
    let current = preflight
        .file_descriptors
        .get(file_id)
        .ok_or_else(|| AppendCommitError {
            kind: AppendCommitErrorKind::Internal,
            message: format!(
                "append preflight did not load the exact file descriptor row for '{}'",
                file_id
            ),
        })?;
    if current.untracked {
        return Err(AppendCommitError {
            kind: AppendCommitErrorKind::Internal,
            message: "lazy exact file update does not support untracked visible rows".to_string(),
        });
    }
    Ok(current)
}

fn materialize_domain_changes(
    timestamp: &str,
    changes: &[ProposedDomainChange],
    functions: &mut dyn LixFunctionProvider,
) -> Result<Vec<DomainChangeInput>, AppendCommitError> {
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
                snapshot_content: change.snapshot_content.clone(),
                metadata: change.metadata.clone(),
                created_at: timestamp.to_string(),
                writer_key: change.writer_key.clone(),
            })
        })
        .collect()
}

fn require_change_field(
    value: Option<String>,
    schema_key: &str,
    field_name: &str,
) -> Result<String, AppendCommitError> {
    value.ok_or_else(|| AppendCommitError {
        kind: AppendCommitErrorKind::MissingDomainField,
        message: format!(
            "append batch requires '{field_name}' for schema '{}'",
            schema_key
        ),
    })
}

struct AppendPreflightState {
    current_tip: Option<String>,
    current_tip_snapshot: Option<String>,
    existing_replay: Option<String>,
    deterministic_sequence_start: Option<i64>,
    active_accounts: Vec<String>,
    file_descriptors: BTreeMap<String, AppendPreflightFileDescriptor>,
}

struct AppendPreflightFileDescriptor {
    directory_id: Option<String>,
    name: String,
    extension: Option<String>,
    hidden: bool,
    metadata: Option<String>,
    untracked: bool,
}

async fn load_append_preflight_state_with_active_accounts(
    executor: &mut dyn CommitQueryExecutor,
    concrete_lane: &ConcreteWriteLane,
    preconditions: &AppendCommitPreconditions,
    lazy_exact_file_updates: &[LazyExactFileUpdate],
    include_deterministic_sequence: bool,
    include_active_accounts: bool,
) -> Result<AppendPreflightState, AppendCommitError> {
    let lane_entity_id = match concrete_lane {
        ConcreteWriteLane::Version { version_id } => version_id.as_str(),
        ConcreteWriteLane::GlobalAdmin => GLOBAL_VERSION_ID,
    };
    let current_tip_source_sql = format!(
        "FROM {version_ref_table} \
         WHERE schema_key = '{schema_key}' \
           AND entity_id = '{entity_id}' \
           AND file_id = 'lix' \
           AND plugin_key = 'lix' \
           AND version_id = '{version_id}' \
           AND snapshot_content IS NOT NULL",
        version_ref_table = LIVE_VERSION_REF_TABLE,
        schema_key = VERSION_REF_SCHEMA_KEY,
        entity_id = escape_sql_string(lane_entity_id),
        version_id = escape_sql_string(GLOBAL_VERSION_ID),
    );
    let existing_replay_sql = match &preconditions.idempotency_key {
        AppendIdempotencyKey::Exact(value) => format!(
            " UNION ALL \
              SELECT CAST('existing_replay' AS TEXT) AS row_kind, CAST(commit_id AS TEXT) AS value, CAST(NULL AS TEXT) AS metadata_value, CAST(NULL AS INTEGER) AS untracked_value, CAST(NULL AS TEXT) AS entity_id \
              FROM {table_name} \
              WHERE write_lane = '{write_lane}' \
                AND idempotency_kind = '{kind}' \
                AND idempotency_value = '{value}' \
                AND parent_tip_snapshot_content = ''",
            table_name = COMMIT_IDEMPOTENCY_TABLE,
            write_lane = escape_sql_string(&lane_storage_key(concrete_lane)),
            kind = IDEMPOTENCY_KIND_EXACT,
            value = escape_sql_string(value),
        ),
        AppendIdempotencyKey::CurrentTipFingerprint(fingerprint) => format!(
            " UNION ALL \
              SELECT CAST('existing_replay' AS TEXT) AS row_kind, CAST(idempotency.commit_id AS TEXT) AS value, CAST(NULL AS TEXT) AS metadata_value, CAST(NULL AS INTEGER) AS untracked_value, CAST(NULL AS TEXT) AS entity_id \
              FROM (SELECT snapshot_content {current_tip_source_sql}) current_tip \
              JOIN {table_name} idempotency \
                ON idempotency.write_lane = '{write_lane}' \
               AND idempotency.idempotency_kind = '{kind}' \
               AND idempotency.idempotency_value = '{value}' \
               AND idempotency.parent_tip_snapshot_content = current_tip.snapshot_content",
            current_tip_source_sql = current_tip_source_sql,
            table_name = COMMIT_IDEMPOTENCY_TABLE,
            write_lane = escape_sql_string(&lane_storage_key(concrete_lane)),
            kind = IDEMPOTENCY_KIND_CURRENT_TIP_FINGERPRINT,
            value = escape_sql_string(fingerprint),
        ),
    };
    let active_account_sql = if include_active_accounts {
        format!(
            " UNION ALL \
              SELECT CAST('active_account' AS TEXT) AS row_kind, CAST(snapshot_content AS TEXT) AS value, CAST(NULL AS TEXT) AS metadata_value, CAST(NULL AS INTEGER) AS untracked_value, CAST(NULL AS TEXT) AS entity_id \
              FROM {untracked_table} \
              WHERE schema_key = '{schema_key}' \
                AND file_id = '{file_id}' \
                AND version_id = '{version_id}' \
                AND snapshot_content IS NOT NULL",
            untracked_table = LIVE_UNTRACKED_TABLE,
            schema_key = escape_sql_string(active_account_schema_key()),
            file_id = escape_sql_string(active_account_file_id()),
            version_id = escape_sql_string(active_account_storage_version_id()),
        )
    } else {
        String::new()
    };
    let deterministic_sequence_sql = if include_deterministic_sequence {
        " UNION ALL \
           SELECT CAST('deterministic_sequence' AS TEXT) AS row_kind, CAST(deterministic_sequence.value AS TEXT) AS value, CAST(NULL AS TEXT) AS metadata_value, CAST(NULL AS INTEGER) AS untracked_value, CAST(NULL AS TEXT) AS entity_id \
           FROM (\
             SELECT value \
             FROM (\
               SELECT CAST(snapshot_content AS TEXT) AS value, 0 AS precedence \
               FROM lix_internal_live_untracked_v1 \
               WHERE schema_key = 'lix_key_value' \
                 AND entity_id = 'lix_deterministic_sequence_number' \
                 AND version_id = 'global' \
                 AND snapshot_content IS NOT NULL \
               UNION ALL \
               SELECT CAST(snapshot_content AS TEXT) AS value, 1 AS precedence \
               FROM lix_internal_live_v1_lix_key_value \
               WHERE entity_id = 'lix_deterministic_sequence_number' \
                 AND version_id = 'global' \
                 AND snapshot_content IS NOT NULL \
                 AND is_tombstone = 0\
             ) deterministic_sequence_candidates \
             ORDER BY precedence ASC \
             LIMIT 1\
           ) deterministic_sequence \
"
            .to_string()
    } else {
        String::new()
    };
    let exact_file_ids = lazy_exact_file_updates
        .iter()
        .flat_map(LazyExactFileUpdate::file_ids)
        .collect::<BTreeSet<_>>();
    let file_descriptor_sql = if exact_file_ids.is_empty() {
        String::new()
    } else {
        format!(
            " UNION ALL \
              SELECT CAST('file_descriptor' AS TEXT) AS row_kind, CAST(snapshot_content AS TEXT) AS value, CAST(metadata AS TEXT) AS metadata_value, CAST(untracked AS INTEGER) AS untracked_value, CAST(entity_id AS TEXT) AS entity_id \
              FROM ({descriptor_sql}) file_descriptor",
            descriptor_sql = exact_file_descriptor_preflight_sql(
                &exact_file_ids.into_iter().map(str::to_string).collect::<Vec<_>>(),
                lane_entity_id,
            ),
        )
    };
    let sql = format!(
        "SELECT row_kind, value, metadata_value, untracked_value, entity_id \
         FROM (\
           SELECT CAST('current_tip' AS TEXT) AS row_kind, CAST(snapshot_content AS TEXT) AS value, CAST(NULL AS TEXT) AS metadata_value, CAST(NULL AS INTEGER) AS untracked_value, CAST(NULL AS TEXT) AS entity_id \
           {current_tip_source_sql}{existing_replay_sql}{deterministic_sequence_sql}{active_account_sql}{file_descriptor_sql}\
         ) append_preflight",
        current_tip_source_sql = current_tip_source_sql,
        existing_replay_sql = existing_replay_sql,
        deterministic_sequence_sql = deterministic_sequence_sql,
        active_account_sql = active_account_sql,
        file_descriptor_sql = file_descriptor_sql,
    );
    let result = executor.execute(&sql, &[]).await.map_err(backend_error)?;
    let mut current_tip = None;
    let mut current_tip_snapshot = None;
    let mut existing_replay = None;
    let mut deterministic_sequence_start = None;
    let mut active_accounts = BTreeSet::new();
    let mut file_descriptors = BTreeMap::new();
    for row in result.rows {
        let Some(kind) = row.first() else {
            continue;
        };
        let Some(value) = row.get(1) else {
            continue;
        };
        let kind = match kind {
            Value::Text(text) => text.as_str(),
            Value::Null => continue,
            other => {
                return Err(AppendCommitError {
                    kind: AppendCommitErrorKind::Internal,
                    message: format!("append preflight returned unexpected kind value {other:?}"),
                })
            }
        };
        match (kind, value) {
            ("current_tip", Value::Text(snapshot_content)) => {
                current_tip_snapshot = Some(snapshot_content.clone());
                let pointer: LixVersionRef =
                    serde_json::from_str(snapshot_content).map_err(|error| AppendCommitError {
                        kind: AppendCommitErrorKind::Internal,
                        message: format!(
                            "append preflight version ref snapshot could not be parsed: {error}"
                        ),
                    })?;
                if !pointer.commit_id.is_empty() {
                    current_tip = Some(pointer.commit_id);
                }
            }
            ("existing_replay", Value::Text(commit_id)) => {
                if !commit_id.is_empty() {
                    existing_replay = Some(commit_id.clone());
                }
            }
            ("deterministic_sequence", Value::Text(snapshot_content)) => {
                deterministic_sequence_start =
                    parse_deterministic_sequence_snapshot(snapshot_content).map(Some)?;
            }
            ("active_account", Value::Text(snapshot_content)) => {
                let account_id =
                    parse_active_account_snapshot(snapshot_content).map_err(backend_error)?;
                active_accounts.insert(account_id);
            }
            ("file_descriptor", Value::Text(snapshot_content)) => {
                let Some(entity_id) = row.get(4).and_then(append_preflight_text_from_value) else {
                    return Err(AppendCommitError {
                        kind: AppendCommitErrorKind::Internal,
                        message:
                            "append preflight returned a file descriptor row without entity_id"
                                .to_string(),
                    });
                };
                file_descriptors.insert(
                    entity_id,
                    parse_file_descriptor_preflight_row(
                        snapshot_content,
                        row.get(2).and_then(append_preflight_text_from_value),
                        row.get(3)
                            .and_then(append_preflight_value_as_bool)
                            .unwrap_or(false),
                    )?,
                );
            }
            (_, Value::Null) => {}
            ("current_tip", other)
            | ("existing_replay", other)
            | ("deterministic_sequence", other)
            | ("active_account", other)
            | ("file_descriptor", other) => {
                return Err(AppendCommitError {
                    kind: AppendCommitErrorKind::Internal,
                    message: format!(
                        "append preflight returned unexpected '{kind}' value {other:?}"
                    ),
                })
            }
            _ => {}
        }
    }

    if include_deterministic_sequence && deterministic_sequence_start.is_none() {
        deterministic_sequence_start = Some(0);
    }

    Ok(AppendPreflightState {
        current_tip,
        current_tip_snapshot,
        existing_replay,
        deterministic_sequence_start,
        active_accounts: active_accounts.into_iter().collect(),
        file_descriptors,
    })
}

fn parse_file_descriptor_preflight_row(
    snapshot_content: &str,
    metadata: Option<String>,
    untracked: bool,
) -> Result<AppendPreflightFileDescriptor, AppendCommitError> {
    let parsed: serde_json::Value =
        serde_json::from_str(snapshot_content).map_err(|error| AppendCommitError {
            kind: AppendCommitErrorKind::Internal,
            message: format!(
                "append preflight file descriptor snapshot could not be parsed: {error}"
            ),
        })?;
    Ok(AppendPreflightFileDescriptor {
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

fn exact_file_descriptor_preflight_sql(file_ids: &[String], version_id: &str) -> String {
    let requested_rows = file_ids
        .iter()
        .map(|file_id| format!("('{}')", escape_sql_string(file_id)))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "WITH requested(entity_id) AS (VALUES {requested_rows}), \
         descriptor_candidates AS (\
           SELECT requested.entity_id, CAST(untracked.snapshot_content AS TEXT) AS snapshot_content, CAST(untracked.metadata AS TEXT) AS metadata, CAST(1 AS INTEGER) AS untracked, CAST(1 AS INTEGER) AS precedence \
           FROM requested \
           JOIN {untracked_table} untracked \
             ON untracked.entity_id = requested.entity_id \
            AND untracked.version_id = '{version_id}' \
            AND untracked.schema_key = '{schema_key}' \
            AND untracked.file_id = '{file_id_value}' \
           UNION ALL \
           SELECT requested.entity_id, CAST(tracked.snapshot_content AS TEXT) AS snapshot_content, CAST(tracked.metadata AS TEXT) AS metadata, CAST(0 AS INTEGER) AS untracked, CAST(2 AS INTEGER) AS precedence \
           FROM requested \
           JOIN {tracked_table} tracked \
             ON tracked.entity_id = requested.entity_id \
            AND tracked.version_id = '{version_id}' \
            AND tracked.file_id = '{file_id_value}' \
           UNION ALL \
           SELECT requested.entity_id, CAST(untracked.snapshot_content AS TEXT) AS snapshot_content, CAST(untracked.metadata AS TEXT) AS metadata, CAST(1 AS INTEGER) AS untracked, CAST(3 AS INTEGER) AS precedence \
           FROM requested \
           JOIN {untracked_table} untracked \
             ON untracked.entity_id = requested.entity_id \
            AND untracked.version_id = '{global_version_id}' \
            AND untracked.schema_key = '{schema_key}' \
            AND untracked.file_id = '{file_id_value}' \
           UNION ALL \
           SELECT requested.entity_id, CAST(tracked.snapshot_content AS TEXT) AS snapshot_content, CAST(tracked.metadata AS TEXT) AS metadata, CAST(0 AS INTEGER) AS untracked, CAST(4 AS INTEGER) AS precedence \
           FROM requested \
           JOIN {tracked_table} tracked \
             ON tracked.entity_id = requested.entity_id \
            AND tracked.version_id = '{global_version_id}' \
            AND tracked.file_id = '{file_id_value}' \
         ), ranked AS (\
           SELECT entity_id, snapshot_content, metadata, untracked, precedence, \
                  ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY precedence ASC) AS rn \
           FROM descriptor_candidates \
           WHERE snapshot_content IS NOT NULL\
         ) \
         SELECT entity_id, snapshot_content, metadata, untracked \
         FROM ranked \
         WHERE rn = 1",
        requested_rows = requested_rows,
        tracked_table = "lix_internal_live_v1_lix_file_descriptor",
        untracked_table = LIVE_UNTRACKED_TABLE,
        version_id = escape_sql_string(version_id),
        global_version_id = escape_sql_string(GLOBAL_VERSION_ID),
        schema_key = escape_sql_string(FILESYSTEM_FILE_SCHEMA_KEY),
        file_id_value = escape_sql_string(FILESYSTEM_DESCRIPTOR_FILE_ID),
    )
}

fn parse_deterministic_sequence_snapshot(snapshot_content: &str) -> Result<i64, AppendCommitError> {
    let parsed: serde_json::Value =
        serde_json::from_str(snapshot_content).map_err(|error| AppendCommitError {
            kind: AppendCommitErrorKind::Internal,
            message: format!(
                "append preflight deterministic sequence snapshot could not be parsed: {error}"
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

fn append_preflight_text_from_value(value: &Value) -> Option<String> {
    match value {
        Value::Text(text) => Some(text.clone()),
        _ => None,
    }
}

fn append_preflight_value_as_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Boolean(flag) => Some(*flag),
        Value::Integer(integer) => Some(*integer != 0),
        _ => None,
    }
}

fn validate_change_versions(
    changes: &[ProposedDomainChange],
    lazy_exact_file_updates: &[LazyExactFileUpdate],
    concrete_lane: &ConcreteWriteLane,
) -> Result<(), AppendCommitError> {
    if !lazy_exact_file_updates.is_empty() {
        let expected_version_id = match concrete_lane {
            ConcreteWriteLane::Version { version_id } => version_id,
            ConcreteWriteLane::GlobalAdmin => GLOBAL_VERSION_ID,
        };
        if lazy_exact_file_updates
            .iter()
            .any(|lazy| lazy.version_id() != expected_version_id)
        {
            return Err(AppendCommitError {
                kind: AppendCommitErrorKind::Internal,
                message: format!(
                    "append batch must target exactly one version lane '{}'",
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
) -> Result<(), AppendCommitError> {
    let version_ids = changes
        .iter()
        .map(|change| change.version_id.as_str())
        .collect::<BTreeSet<_>>();
    match concrete_lane {
        ConcreteWriteLane::Version { version_id } => {
            if version_ids.len() != 1 || !version_ids.contains(version_id.as_str()) {
                return Err(AppendCommitError {
                    kind: AppendCommitErrorKind::Internal,
                    message: format!(
                        "append batch must target exactly one version lane '{}'",
                        version_id
                    ),
                });
            }
        }
        ConcreteWriteLane::GlobalAdmin => {
            if version_ids.len() != 1 || !version_ids.contains(GLOBAL_VERSION_ID) {
                return Err(AppendCommitError {
                    kind: AppendCommitErrorKind::Internal,
                    message: "append batch must target exactly the global admin lane".to_string(),
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
    parent_tip_snapshot_content: String,
}

fn resolve_idempotency_key(
    preconditions: &AppendCommitPreconditions,
    current_tip: Option<&str>,
) -> String {
    match &preconditions.idempotency_key {
        AppendIdempotencyKey::Exact(value) => value.clone(),
        AppendIdempotencyKey::CurrentTipFingerprint(fingerprint) => serde_json::json!({
            "tip": current_tip,
            "fingerprint": fingerprint,
        })
        .to_string(),
    }
}

fn resolve_idempotency_state(
    preconditions: &AppendCommitPreconditions,
    preflight: &AppendPreflightState,
) -> ResolvedIdempotencyState {
    let legacy_key = resolve_idempotency_key(preconditions, preflight.current_tip.as_deref());
    match &preconditions.idempotency_key {
        AppendIdempotencyKey::Exact(value) => ResolvedIdempotencyState {
            legacy_key,
            kind: IDEMPOTENCY_KIND_EXACT,
            value: value.clone(),
            parent_tip_snapshot_content: String::new(),
        },
        AppendIdempotencyKey::CurrentTipFingerprint(fingerprint) => ResolvedIdempotencyState {
            legacy_key,
            kind: IDEMPOTENCY_KIND_CURRENT_TIP_FINGERPRINT,
            value: fingerprint.clone(),
            parent_tip_snapshot_content: preflight.current_tip_snapshot.clone().unwrap_or_default(),
        },
    }
}

fn extract_committed_tip_id(
    commit_result: &GenerateCommitResult,
    concrete_lane: &ConcreteWriteLane,
) -> Result<String, AppendCommitError> {
    let version_id = match concrete_lane {
        ConcreteWriteLane::Version { version_id } => version_id.as_str(),
        ConcreteWriteLane::GlobalAdmin => GLOBAL_VERSION_ID,
    };
    let pointer_change = commit_result
        .changes
        .iter()
        .find(|change| {
            change.schema_key == VERSION_REF_SCHEMA_KEY && change.entity_id == version_id
        })
        .ok_or_else(|| AppendCommitError {
            kind: AppendCommitErrorKind::Internal,
            message: format!(
                "generated commit result did not include a version ref for '{}'",
                version_id
            ),
        })?;
    let snapshot_content =
        pointer_change
            .snapshot_content
            .as_ref()
            .ok_or_else(|| AppendCommitError {
                kind: AppendCommitErrorKind::Internal,
                message: format!(
                    "generated version ref for '{}' is missing snapshot_content",
                    version_id
                ),
            })?;
    let pointer: LixVersionRef =
        serde_json::from_str(snapshot_content).map_err(|error| AppendCommitError {
            kind: AppendCommitErrorKind::Internal,
            message: format!(
                "generated version ref for '{}' could not be parsed: {error}",
                version_id
            ),
        })?;
    if pointer.commit_id.is_empty() {
        return Err(AppendCommitError {
            kind: AppendCommitErrorKind::Internal,
            message: format!(
                "generated version ref for '{}' contained an empty commit_id",
                version_id
            ),
        });
    }
    Ok(pointer.commit_id)
}

fn insert_idempotency_row_sql(
    concrete_lane: &ConcreteWriteLane,
    idempotency: &ResolvedIdempotencyState,
    commit_id: &str,
    created_at: &str,
) -> String {
    format!(
        "INSERT INTO {table_name} \
         (write_lane, idempotency_key, idempotency_kind, idempotency_value, parent_tip_snapshot_content, commit_id, created_at) \
         VALUES ('{write_lane}', '{idempotency_key}', '{idempotency_kind}', '{idempotency_value}', '{parent_tip_snapshot_content}', '{commit_id}', '{created_at}')",
        table_name = COMMIT_IDEMPOTENCY_TABLE,
        write_lane = escape_sql_string(&lane_storage_key(concrete_lane)),
        idempotency_key = escape_sql_string(&idempotency.legacy_key),
        idempotency_kind = escape_sql_string(idempotency.kind),
        idempotency_value = escape_sql_string(&idempotency.value),
        parent_tip_snapshot_content = escape_sql_string(&idempotency.parent_tip_snapshot_content),
        commit_id = escape_sql_string(commit_id),
        created_at = escape_sql_string(created_at),
    )
}

fn lane_storage_key(concrete_lane: &ConcreteWriteLane) -> String {
    match concrete_lane {
        ConcreteWriteLane::Version { version_id } => format!("version:{version_id}"),
        ConcreteWriteLane::GlobalAdmin => "global-admin".to_string(),
    }
}

fn backend_error(error: LixError) -> AppendCommitError {
    AppendCommitError {
        kind: AppendCommitErrorKind::Internal,
        message: error.description,
    }
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::{
        append_commit_if_preconditions_hold, AppendCommitArgs, AppendCommitDisposition,
        AppendCommitError, AppendCommitErrorKind, AppendCommitInvariantChecker,
        AppendCommitPreconditions, AppendExpectedTip, AppendIdempotencyKey, AppendWriteLane,
    };
    use crate::functions::LixFunctionProvider;
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
    }

    #[async_trait(?Send)]
    impl LixTransaction for FakeTransaction {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&mut self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            self.executed_sql.push(sql.to_string());

            if sql.contains("SELECT row_kind, value")
                && sql.contains("FROM lix_internal_live_v1_lix_version_ref")
            {
                let mut rows = self
                    .version_heads
                    .iter()
                    .filter(|(version_id, _)| {
                        sql.contains(&format!("entity_id = '{}'", version_id))
                    })
                    .map(|(version_id, commit_id)| {
                        vec![
                            Value::Text("current_tip".to_string()),
                            Value::Text(crate::version::version_ref_snapshot_content(
                                version_id, commit_id,
                            )),
                        ]
                    })
                    .collect::<Vec<_>>();
                if sql.contains("lix_internal_commit_idempotency") {
                    rows.extend(
                        self.idempotency_rows
                            .iter()
                            .filter(|((lane, kind, value, parent_tip_snapshot_content), _)| {
                                sql.contains(&format!("write_lane = '{}'", lane))
                                    && sql.contains(&format!("idempotency_kind = '{}'", kind))
                                    && sql.contains(&format!("idempotency_value = '{}'", value))
                                    && if sql.contains(
                                        "parent_tip_snapshot_content = current_tip.snapshot_content",
                                    ) {
                                        !parent_tip_snapshot_content.is_empty()
                                    } else {
                                        sql.contains(&format!(
                                            "parent_tip_snapshot_content = '{}'",
                                            parent_tip_snapshot_content
                                        ))
                                    }
                            })
                            .map(|(_, commit_id)| {
                                vec![
                                    Value::Text("existing_replay".to_string()),
                                    Value::Text(commit_id.clone()),
                                ]
                            }),
                    );
                }
                return Ok(QueryResult {
                    rows,
                    columns: vec!["row_kind".to_string(), "value".to_string()],
                });
            }

            if sql.contains("FROM lix_internal_live_v1_lix_version_ref")
                && sql.contains("entity_id = 'global'")
            {
                let rows = self
                    .version_heads
                    .get(GLOBAL_VERSION_ID)
                    .map(|commit_id| {
                        vec![Value::Text(crate::version::version_ref_snapshot_content(
                            GLOBAL_VERSION_ID,
                            commit_id,
                        ))]
                    })
                    .into_iter()
                    .collect::<Vec<_>>();
                return Ok(QueryResult {
                    rows,
                    columns: vec!["snapshot_content".to_string()],
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
                    .filter(|((lane, kind, value, parent_tip_snapshot_content), _)| {
                        sql.contains(&format!("write_lane = '{}'", lane))
                            && sql.contains(&format!("idempotency_kind = '{}'", kind))
                            && sql.contains(&format!("idempotency_value = '{}'", value))
                            && sql.contains(&format!(
                                "parent_tip_snapshot_content = '{}'",
                                parent_tip_snapshot_content
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
                let parent_tip_snapshot_content =
                    extract_nth_single_quoted_value(idempotency_sql, 4)
                        .expect("parent tip snapshot content should be present");
                let commit_id = extract_nth_single_quoted_value(idempotency_sql, 5)
                    .expect("commit id should be present");
                self.idempotency_rows
                    .insert((lane, kind, value, parent_tip_snapshot_content), commit_id);
            }

            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
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
        failure: Option<AppendCommitError>,
    }

    #[async_trait(?Send)]
    impl AppendCommitInvariantChecker for RecordingInvariantChecker {
        async fn recheck_invariants(
            &mut self,
            _transaction: &mut dyn LixTransaction,
        ) -> Result<(), AppendCommitError> {
            self.calls += 1;
            if let Some(error) = self.failure.clone() {
                return Err(error);
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn applies_commit_when_tip_matches_expected() {
        let mut transaction = FakeTransaction::default();
        transaction
            .version_heads
            .insert("version-a".to_string(), "commit-123".to_string());
        let mut functions = CountingFunctionProvider::default();
        let mut checker = RecordingInvariantChecker::default();

        let result = append_commit_if_preconditions_hold(
            &mut transaction,
            AppendCommitArgs {
                timestamp: Some("2026-03-06T14:22:00.000Z".to_string()),
                changes: vec![sample_change()],
                lazy_exact_file_updates: Vec::new(),
                preconditions: AppendCommitPreconditions {
                    write_lane: AppendWriteLane::Version("version-a".to_string()),
                    expected_tip: AppendExpectedTip::CommitId("commit-123".to_string()),
                    idempotency_key: AppendIdempotencyKey::Exact("idem-1".to_string()),
                },
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
            },
            &mut functions,
            Some(&mut checker),
        )
        .await
        .expect("append should succeed");

        assert_eq!(result.disposition, AppendCommitDisposition::Applied);
        assert!(result.commit_result.is_some());
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
            "append should persist idempotency state in the executed batch"
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

        let result = append_commit_if_preconditions_hold(
            &mut transaction,
            AppendCommitArgs {
                timestamp: Some("2026-03-06T14:22:00.000Z".to_string()),
                changes: vec![sample_change()],
                lazy_exact_file_updates: Vec::new(),
                preconditions: AppendCommitPreconditions {
                    write_lane: AppendWriteLane::Version("version-a".to_string()),
                    expected_tip: AppendExpectedTip::CommitId("commit-123".to_string()),
                    idempotency_key: AppendIdempotencyKey::Exact("idem-1".to_string()),
                },
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
            },
            &mut functions,
            Some(&mut checker),
        )
        .await
        .expect("replay should succeed");

        assert_eq!(result.disposition, AppendCommitDisposition::Replay);
        assert_eq!(result.committed_tip, "commit-456");
        assert!(result.commit_result.is_none());
        assert_eq!(checker.calls, 0);
    }

    #[tokio::test]
    async fn replays_when_same_current_tip_fingerprint_already_committed() {
        let mut transaction = FakeTransaction::default();
        transaction
            .version_heads
            .insert("version-a".to_string(), "commit-456".to_string());
        transaction.idempotency_rows.insert(
            (
                "version:version-a".to_string(),
                "current_tip_fingerprint".to_string(),
                "fp-1".to_string(),
                crate::version::version_ref_snapshot_content("version-a", "commit-456"),
            ),
            "commit-456".to_string(),
        );
        let mut functions = CountingFunctionProvider::default();

        let result = append_commit_if_preconditions_hold(
            &mut transaction,
            AppendCommitArgs {
                timestamp: Some("2026-03-06T14:22:00.000Z".to_string()),
                changes: vec![sample_change()],
                lazy_exact_file_updates: Vec::new(),
                preconditions: AppendCommitPreconditions {
                    write_lane: AppendWriteLane::Version("version-a".to_string()),
                    expected_tip: AppendExpectedTip::CurrentTip,
                    idempotency_key: AppendIdempotencyKey::CurrentTipFingerprint(
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

        assert_eq!(result.disposition, AppendCommitDisposition::Replay);
        assert_eq!(result.committed_tip, "commit-456");
        assert!(result.commit_result.is_none());
    }

    #[tokio::test]
    async fn rejects_tip_drift_without_matching_idempotency_row() {
        let mut transaction = FakeTransaction::default();
        transaction
            .version_heads
            .insert("version-a".to_string(), "commit-456".to_string());
        let mut functions = CountingFunctionProvider::default();
        let mut checker = RecordingInvariantChecker::default();

        let error = append_commit_if_preconditions_hold(
            &mut transaction,
            AppendCommitArgs {
                timestamp: Some("2026-03-06T14:22:00.000Z".to_string()),
                changes: vec![sample_change()],
                lazy_exact_file_updates: Vec::new(),
                preconditions: AppendCommitPreconditions {
                    write_lane: AppendWriteLane::Version("version-a".to_string()),
                    expected_tip: AppendExpectedTip::CommitId("commit-123".to_string()),
                    idempotency_key: AppendIdempotencyKey::Exact("idem-1".to_string()),
                },
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
            },
            &mut functions,
            Some(&mut checker),
        )
        .await
        .expect_err("tip drift should fail");

        assert_eq!(error.kind, AppendCommitErrorKind::TipDrift);
        assert_eq!(checker.calls, 0);
    }

    #[tokio::test]
    async fn rejects_missing_lane_without_create_if_missing() {
        let mut transaction = FakeTransaction::default();
        let mut functions = CountingFunctionProvider::default();

        let error = append_commit_if_preconditions_hold(
            &mut transaction,
            AppendCommitArgs {
                timestamp: Some("2026-03-06T14:22:00.000Z".to_string()),
                changes: vec![sample_change()],
                lazy_exact_file_updates: Vec::new(),
                preconditions: AppendCommitPreconditions {
                    write_lane: AppendWriteLane::Version("version-a".to_string()),
                    expected_tip: AppendExpectedTip::CommitId("commit-123".to_string()),
                    idempotency_key: AppendIdempotencyKey::Exact("idem-1".to_string()),
                },
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
            },
            &mut functions,
            None,
        )
        .await
        .expect_err("missing lane should fail");

        assert_eq!(error.kind, AppendCommitErrorKind::MissingWriteLane);
    }

    #[tokio::test]
    async fn allows_create_if_missing_for_new_version_lane() {
        let mut transaction = FakeTransaction::default();
        let mut functions = CountingFunctionProvider::default();

        let result = append_commit_if_preconditions_hold(
            &mut transaction,
            AppendCommitArgs {
                timestamp: Some("2026-03-06T14:22:00.000Z".to_string()),
                changes: vec![sample_change()],
                lazy_exact_file_updates: Vec::new(),
                preconditions: AppendCommitPreconditions {
                    write_lane: AppendWriteLane::Version("version-a".to_string()),
                    expected_tip: AppendExpectedTip::CreateIfMissing,
                    idempotency_key: AppendIdempotencyKey::Exact("idem-create".to_string()),
                },
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
            },
            &mut functions,
            None,
        )
        .await
        .expect("create-if-missing should succeed");

        assert_eq!(result.disposition, AppendCommitDisposition::Applied);
    }

    #[tokio::test]
    async fn applies_global_admin_lane_when_tip_matches_expected() {
        let mut transaction = FakeTransaction::default();
        transaction.version_heads.insert(
            GLOBAL_VERSION_ID.to_string(),
            "commit-global-123".to_string(),
        );
        let mut functions = CountingFunctionProvider::default();

        let result = append_commit_if_preconditions_hold(
            &mut transaction,
            AppendCommitArgs {
                timestamp: Some("2026-03-06T14:22:00.000Z".to_string()),
                changes: vec![sample_global_change()],
                lazy_exact_file_updates: Vec::new(),
                preconditions: AppendCommitPreconditions {
                    write_lane: AppendWriteLane::GlobalAdmin,
                    expected_tip: AppendExpectedTip::CommitId("commit-global-123".to_string()),
                    idempotency_key: AppendIdempotencyKey::Exact("idem-global".to_string()),
                },
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
            },
            &mut functions,
            None,
        )
        .await
        .expect("global admin append should succeed");

        assert_eq!(result.disposition, AppendCommitDisposition::Applied);
        assert!(result.commit_result.is_some());
    }

    #[tokio::test]
    async fn invariant_recheck_failure_aborts_append_before_commit_generation() {
        let mut transaction = FakeTransaction::default();
        transaction
            .version_heads
            .insert("version-a".to_string(), "commit-123".to_string());
        let mut functions = CountingFunctionProvider::default();
        let mut checker = RecordingInvariantChecker {
            calls: 0,
            failure: Some(AppendCommitError {
                kind: AppendCommitErrorKind::Internal,
                message: "append invariant failed".to_string(),
            }),
        };

        let error = append_commit_if_preconditions_hold(
            &mut transaction,
            AppendCommitArgs {
                timestamp: Some("2026-03-06T14:22:00.000Z".to_string()),
                changes: vec![sample_change()],
                lazy_exact_file_updates: Vec::new(),
                preconditions: AppendCommitPreconditions {
                    write_lane: AppendWriteLane::Version("version-a".to_string()),
                    expected_tip: AppendExpectedTip::CommitId("commit-123".to_string()),
                    idempotency_key: AppendIdempotencyKey::Exact("idem-1".to_string()),
                },
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
            },
            &mut functions,
            Some(&mut checker),
        )
        .await
        .expect_err("append invariant failure should abort");

        assert_eq!(checker.calls, 1);
        assert_eq!(error.message, "append invariant failed");
        assert!(
            !transaction
                .executed_sql
                .iter()
                .any(|sql| sql.contains("INSERT INTO lix_internal_commit_idempotency ")),
            "append should abort before persisting idempotency state"
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
