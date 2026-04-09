use std::collections::{BTreeMap, BTreeSet};

use crate::binary_cas::support::build_binary_blob_fastcdc_write_program;
use crate::canonical::graph::{
    build_commit_graph_node_prepared_batch, resolve_commit_graph_node_write_rows_with_executor,
};
use crate::canonical::journal::{
    build_prepared_batch_from_canonical_output, CanonicalCommitOutput,
};
use crate::canonical::json::CanonicalJson;
use crate::canonical::read::CommitQueryExecutor;
use crate::contracts::artifacts::{PendingPublicCommitLane, PendingPublicCommitSession};
use crate::contracts::functions::LixFunctionProvider;
use crate::execution::write::filesystem::runtime::BinaryBlobWrite;
use crate::execution::write::transaction::{execute_write_program_with_transaction, WriteProgram};
use crate::session::version_ops::{load_version_info_for_versions, VersionInfo};
use crate::{
    CanonicalPluginKey, CanonicalSchemaKey, CanonicalSchemaVersion, EntityId, FileId,
    LixBackendTransaction, LixError, QueryResult, Value, VersionId,
};
use serde_json::{json, Value as JsonValue};

use super::create::{
    CreateCommitAppliedOutput, CreateCommitError, CreateCommitExpectedHead,
    CreateCommitPreconditions, CreateCommitWriteLane,
};
use super::generate::generate_commit;
use super::receipt::latest_replay_cursor_from_change_rows;
use super::types::{GenerateCommitArgs, GenerateCommitResult, StagedChange};
use super::{CanonicalCommitReceipt, UpdatedVersionRef};

struct TransactionCommitExecutor<'a> {
    transaction: &'a mut dyn LixBackendTransaction,
}

#[async_trait::async_trait(?Send)]
impl CommitQueryExecutor for TransactionCommitExecutor<'_> {
    fn dialect(&self) -> crate::SqlDialect {
        self.transaction.dialect()
    }

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        self.transaction.execute(sql, params).await
    }
}

pub(crate) fn pending_session_matches_create_commit(
    session: &PendingPublicCommitSession,
    preconditions: &CreateCommitPreconditions,
) -> bool {
    pending_public_commit_lane_matches_write_lane(&session.lane, &preconditions.write_lane)
        && match &preconditions.expected_head {
            CreateCommitExpectedHead::CurrentHead => true,
            CreateCommitExpectedHead::CommitId(commit_id) => commit_id == &session.commit_id,
            CreateCommitExpectedHead::CreateIfMissing => false,
        }
}

pub(crate) async fn build_pending_public_commit_session(
    transaction: &mut dyn LixBackendTransaction,
    lane: CreateCommitWriteLane,
    applied_output: &CreateCommitAppliedOutput,
) -> Result<PendingPublicCommitSession, LixError> {
    let seed = applied_output
        .pending_public_commit_seed
        .as_ref()
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "public commit session requires a pending public commit seed",
            )
        })?;
    let commit_snapshot: JsonValue =
        serde_json::from_str(&seed.commit_snapshot_content).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("public commit session commit snapshot is invalid JSON: {error}"),
            )
        })?;
    let snapshot_id_result = transaction
        .execute(
            "SELECT snapshot_id \
             FROM lix_internal_change \
             WHERE id = $1 \
               AND schema_key = 'lix_commit' \
               AND entity_id = $2 \
             LIMIT 1",
            &[
                Value::Text(seed.commit_change_id.clone()),
                Value::Text(seed.commit_id.clone()),
            ],
        )
        .await?;
    let commit_change_snapshot_id = snapshot_id_result
        .rows
        .first()
        .and_then(|row| row.first())
        .and_then(|value| match value {
            Value::Text(text) => Some(text.clone()),
            _ => None,
        })
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "public commit session could not load commit snapshot_id",
            )
        })?;

    Ok(PendingPublicCommitSession {
        lane: pending_public_commit_lane_from_write_lane(&lane),
        commit_id: seed.commit_id.clone(),
        commit_change_snapshot_id,
        commit_snapshot,
    })
}

fn pending_public_commit_lane_matches_write_lane(
    pending_lane: &PendingPublicCommitLane,
    write_lane: &CreateCommitWriteLane,
) -> bool {
    match (pending_lane, write_lane) {
        (PendingPublicCommitLane::Version(pending), CreateCommitWriteLane::Version(current)) => {
            pending == current
        }
        (PendingPublicCommitLane::GlobalAdmin, CreateCommitWriteLane::GlobalAdmin) => true,
        _ => false,
    }
}

fn pending_public_commit_lane_from_write_lane(
    lane: &CreateCommitWriteLane,
) -> PendingPublicCommitLane {
    match lane {
        CreateCommitWriteLane::Version(version_id) => {
            PendingPublicCommitLane::Version(version_id.clone())
        }
        CreateCommitWriteLane::GlobalAdmin => PendingPublicCommitLane::GlobalAdmin,
    }
}

pub(crate) async fn merge_public_change_batch_into_pending_commit(
    transaction: &mut dyn LixBackendTransaction,
    session: &mut PendingPublicCommitSession,
    changes: &[StagedChange],
    binary_blob_writes: &[BinaryBlobWrite],
    active_account_ids: &[String],
    writer_key: Option<&str>,
    functions: &mut dyn LixFunctionProvider,
    timestamp: &str,
) -> Result<CanonicalCommitReceipt, LixError> {
    let staged_changes = changes
        .iter()
        .map(|change| {
            Ok::<StagedChange, LixError>(StagedChange {
                id: Some(functions.uuid_v7()),
                entity_id: EntityId::new(change.entity_id.to_string())?,
                schema_key: CanonicalSchemaKey::new(change.schema_key.to_string())?,
                schema_version: Some(CanonicalSchemaVersion::new(
                    change
                        .schema_version
                        .as_ref()
                        .map(ToString::to_string)
                        .ok_or_else(|| {
                            LixError::new(
                                "LIX_ERROR_UNKNOWN",
                                format!(
                                    "public merge requires schema_version for '{}:{}'",
                                    change.schema_key, change.entity_id
                                ),
                            )
                        })?,
                )?),
                file_id: Some(FileId::new(
                    change
                        .file_id
                        .as_ref()
                        .map(ToString::to_string)
                        .ok_or_else(|| {
                            LixError::new(
                                "LIX_ERROR_UNKNOWN",
                                format!(
                                    "public merge requires file_id for '{}:{}'",
                                    change.schema_key, change.entity_id
                                ),
                            )
                        })?,
                )?),
                plugin_key: Some(CanonicalPluginKey::new(
                    change
                        .plugin_key
                        .as_ref()
                        .map(ToString::to_string)
                        .ok_or_else(|| {
                            LixError::new(
                                "LIX_ERROR_UNKNOWN",
                                format!(
                                    "public merge requires plugin_key for '{}:{}'",
                                    change.schema_key, change.entity_id
                                ),
                            )
                        })?,
                )?),
                snapshot_content: canonicalize_optional_json_text(
                    change.snapshot_content.as_deref(),
                    "snapshot_content",
                    change.schema_key.as_str(),
                    change.entity_id.as_str(),
                )?
                .map(|value| value.as_str().to_string()),
                metadata: canonicalize_optional_json_text(
                    change.metadata.as_deref(),
                    "metadata",
                    change.schema_key.as_str(),
                    change.entity_id.as_str(),
                )?
                .map(|value| value.as_str().to_string()),
                version_id: VersionId::new(change.version_id.to_string())?,
                writer_key: change.writer_key.clone(),
                created_at: Some(timestamp.to_string()),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let active_accounts = active_account_ids.to_vec();
    let versions = load_version_info_for_staged_changes(transaction, &staged_changes).await?;
    let generated = generate_commit(
        GenerateCommitArgs {
            timestamp: timestamp.to_string(),
            active_accounts: active_accounts.clone(),
            changes: staged_changes.clone(),
            versions,
            force_commit_versions: BTreeSet::new(),
        },
        || functions.uuid_v7(),
    )?;

    extend_json_array_strings(
        &mut session.commit_snapshot,
        "change_ids",
        staged_changes.iter().map(|change| {
            change
                .id
                .clone()
                .expect("pending merge staged changes must have ids")
        }),
    );
    extend_json_array_strings(
        &mut session.commit_snapshot,
        "author_account_ids",
        active_accounts.iter().cloned(),
    );

    transaction
        .execute(
            "UPDATE lix_internal_snapshot \
             SET content = $1 \
             WHERE id = $2",
            &[
                Value::Text(session.commit_snapshot.to_string()),
                Value::Text(session.commit_change_snapshot_id.clone()),
            ],
        )
        .await?;

    let rewritten = rewrite_generated_commit_result_for_pending_session(
        session,
        generated,
        staged_changes.len(),
        timestamp,
    )?;
    execute_generated_commit_result(
        transaction,
        rewritten,
        binary_blob_writes,
        functions,
        changes,
        writer_key,
    )
    .await
}

fn canonicalize_optional_json_text(
    value: Option<&str>,
    field_name: &str,
    schema_key: &str,
    entity_id: &str,
) -> Result<Option<CanonicalJson>, LixError> {
    value
        .map(CanonicalJson::from_text)
        .transpose()
        .map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "public merge requires valid JSON {field_name} for '{schema_key}:{entity_id}': {}",
                    error.description
                ),
            )
        })
}

async fn load_version_info_for_staged_changes(
    transaction: &mut dyn LixBackendTransaction,
    staged_changes: &[StagedChange],
) -> Result<BTreeMap<String, VersionInfo>, LixError> {
    let affected_versions = staged_changes
        .iter()
        .map(|change| change.version_id.to_string())
        .collect::<BTreeSet<_>>();
    let mut executor = TransactionCommitExecutor { transaction };
    load_version_info_for_versions(&mut executor, &affected_versions).await
}

fn rewrite_generated_commit_result_for_pending_session(
    session: &PendingPublicCommitSession,
    generated: GenerateCommitResult,
    change_count: usize,
    timestamp: &str,
) -> Result<GenerateCommitResult, LixError> {
    let updated_version_refs = generated
        .updated_version_refs
        .into_iter()
        .map(|update| UpdatedVersionRef {
            version_id: update.version_id,
            commit_id: session.commit_id.clone(),
            created_at: timestamp.to_string(),
        })
        .collect();

    Ok(GenerateCommitResult {
        canonical_output: CanonicalCommitOutput {
            changes: generated
                .canonical_output
                .changes
                .into_iter()
                .take(change_count)
                .collect(),
        },
        updated_version_refs,
        affected_versions: generated.affected_versions,
    })
}

fn extend_json_array_strings<I>(snapshot: &mut JsonValue, key: &str, values: I)
where
    I: IntoIterator<Item = String>,
{
    if !snapshot.is_object() {
        *snapshot = json!({});
    }
    let JsonValue::Object(map) = snapshot else {
        return;
    };
    let entry = map
        .entry(key.to_string())
        .or_insert_with(|| JsonValue::Array(Vec::new()));
    if !entry.is_array() {
        *entry = JsonValue::Array(Vec::new());
    }
    let JsonValue::Array(array) = entry else {
        return;
    };
    let mut seen = array
        .iter()
        .filter_map(|value| value.as_str().map(ToString::to_string))
        .collect::<BTreeSet<_>>();
    for value in values {
        if seen.insert(value.clone()) {
            array.push(JsonValue::String(value));
        }
    }
}

async fn execute_generated_commit_result(
    transaction: &mut dyn LixBackendTransaction,
    result: GenerateCommitResult,
    binary_blob_writes: &[BinaryBlobWrite],
    functions: &mut dyn LixFunctionProvider,
    changes: &[StagedChange],
    writer_key: Option<&str>,
) -> Result<CanonicalCommitReceipt, LixError> {
    let mut executor = &mut *transaction;
    let commit_graph_rows =
        resolve_commit_graph_node_write_rows_with_executor(&mut executor, &result.canonical_output)
            .await?;
    let mut prepared = build_prepared_batch_from_canonical_output(
        &result.canonical_output,
        functions,
        transaction.dialect(),
    )?;
    prepared.extend(build_commit_graph_node_prepared_batch(
        &commit_graph_rows,
        transaction.dialect(),
    )?);
    let mut program = WriteProgram::new();
    if !binary_blob_writes.is_empty() {
        let payloads = binary_blob_writes
            .iter()
            .map(BinaryBlobWrite::as_input)
            .map(|payload| crate::binary_cas::support::BinaryBlobWriteInput {
                file_id: payload.file_id,
                version_id: payload.version_id,
                data: payload.data,
            })
            .collect::<Vec<_>>();
        program.extend(build_binary_blob_fastcdc_write_program(
            transaction.dialect(),
            &payloads,
        )?);
    }
    program.push_batch(prepared);
    execute_write_program_with_transaction(transaction, program).await?;
    let receipt = canonical_commit_receipt_from_generated_result(&result)?;
    crate::live_state::apply_tracked_commit_effects_in_transaction(
        transaction,
        &receipt,
        changes,
        writer_key,
    )
    .await?;
    Ok(receipt)
}

fn canonical_commit_receipt_from_generated_result(
    result: &GenerateCommitResult,
) -> Result<CanonicalCommitReceipt, LixError> {
    let replay_cursor = latest_replay_cursor_from_change_rows(&result.canonical_output.changes)
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "pending public commit execution requires at least one canonical change row",
            )
        })?;
    let commit_id = result
        .updated_version_refs
        .first()
        .map(|update| update.commit_id.clone())
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "pending public commit execution requires at least one committed version ref update",
            )
        })?;
    Ok(CanonicalCommitReceipt {
        commit_id,
        replay_cursor,
        updated_version_refs: result.updated_version_refs.clone(),
        affected_versions: result.affected_versions.clone(),
    })
}

pub(crate) fn create_commit_error_to_lix_error(error: CreateCommitError) -> LixError {
    LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: error.message,
    }
}
