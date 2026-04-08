use std::collections::{BTreeMap, BTreeSet};

use crate::binary_cas::support::build_binary_blob_fastcdc_write_program;
use crate::canonical::graph::{
    build_commit_graph_node_prepared_batch, resolve_commit_graph_node_write_rows_with_executor,
};
use crate::canonical::journal::{
    build_prepared_batch_from_canonical_output, CanonicalCommitOutput,
};
use crate::canonical::read::CommitQueryExecutor;
use crate::canonical::json::CanonicalJson;
use crate::contracts::functions::LixFunctionProvider;
use crate::session::version_ops::{load_version_info_for_versions, VersionInfo};
use crate::transaction_execution::{execute_write_program_with_transaction, WriteProgram};
use crate::write_runtime::filesystem::runtime::BinaryBlobWrite;
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
use super::types::{
    DomainChangeInput, GenerateCommitArgs, GenerateCommitResult, ProposedDomainChange,
};
use super::{CanonicalCommitReceipt, UpdatedVersionRef};

#[derive(Debug, Clone)]
pub(crate) struct PendingPublicCommitSession {
    pub(crate) lane: CreateCommitWriteLane,
    pub(crate) commit_id: String,
    pub(crate) commit_change_snapshot_id: String,
    pub(crate) commit_snapshot: JsonValue,
}

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
    session.lane == preconditions.write_lane
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
        lane,
        commit_id: seed.commit_id.clone(),
        commit_change_snapshot_id,
        commit_snapshot,
    })
}

pub(crate) async fn merge_public_domain_change_batch_into_pending_commit(
    transaction: &mut dyn LixBackendTransaction,
    session: &mut PendingPublicCommitSession,
    changes: &[ProposedDomainChange],
    binary_blob_writes: &[BinaryBlobWrite],
    active_account_ids: &[String],
    writer_key: Option<&str>,
    functions: &mut dyn LixFunctionProvider,
    timestamp: &str,
) -> Result<CanonicalCommitReceipt, LixError> {
    let tracked_writer_key_annotations =
        crate::schema::annotations::writer_key::tracked_writer_key_annotations_from_changes(
            changes, writer_key,
        );
    let domain_changes = changes
        .iter()
        .map(|change| {
            Ok::<DomainChangeInput, LixError>(DomainChangeInput {
                id: functions.uuid_v7(),
                entity_id: EntityId::new(change.entity_id.to_string())?,
                schema_key: CanonicalSchemaKey::new(change.schema_key.to_string())?,
                schema_version: CanonicalSchemaVersion::new(
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
                )?,
                file_id: FileId::new(
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
                )?,
                plugin_key: CanonicalPluginKey::new(
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
                )?,
                snapshot_content: canonicalize_optional_json_text(
                    change.snapshot_content.as_deref(),
                    "snapshot_content",
                    change.schema_key.as_str(),
                    change.entity_id.as_str(),
                )?,
                metadata: canonicalize_optional_json_text(
                    change.metadata.as_deref(),
                    "metadata",
                    change.schema_key.as_str(),
                    change.entity_id.as_str(),
                )?,
                created_at: timestamp.to_string(),
                version_id: VersionId::new(change.version_id.to_string())?,
                writer_key: change.writer_key.clone(),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let active_accounts = active_account_ids.to_vec();
    let versions = load_version_info_for_domain_changes(transaction, &domain_changes).await?;
    let generated = generate_commit(
        GenerateCommitArgs {
            timestamp: timestamp.to_string(),
            active_accounts: active_accounts.clone(),
            changes: domain_changes.clone(),
            versions,
            force_commit_versions: BTreeSet::new(),
        },
        || functions.uuid_v7(),
    )?;

    extend_json_array_strings(
        &mut session.commit_snapshot,
        "change_ids",
        domain_changes.iter().map(|change| change.id.clone()),
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
        domain_changes.len(),
        timestamp,
    )?;
    execute_generated_commit_result(
        transaction,
        rewritten,
        binary_blob_writes,
        functions,
        &tracked_writer_key_annotations,
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

async fn load_version_info_for_domain_changes(
    transaction: &mut dyn LixBackendTransaction,
    domain_changes: &[DomainChangeInput],
) -> Result<BTreeMap<String, VersionInfo>, LixError> {
    let affected_versions = domain_changes
        .iter()
        .map(|change| change.version_id.to_string())
        .collect::<BTreeSet<_>>();
    let mut executor = TransactionCommitExecutor { transaction };
    load_version_info_for_versions(&mut executor, &affected_versions).await
}

fn rewrite_generated_commit_result_for_pending_session(
    session: &PendingPublicCommitSession,
    generated: GenerateCommitResult,
    domain_change_count: usize,
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
                .take(domain_change_count)
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
    tracked_writer_key_annotations: &std::collections::BTreeMap<
        crate::live_state::RowIdentity,
        Option<String>,
    >,
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
            .collect::<Vec<_>>();
        program.extend(build_binary_blob_fastcdc_write_program(
            transaction.dialect(),
            &payloads,
        )?);
    }
    program.push_batch(prepared);
    execute_write_program_with_transaction(transaction, program).await?;
    let mut executor = &mut *transaction;
    crate::schema::annotations::writer_key::apply_workspace_writer_key_annotations_with_executor(
        &mut executor,
        tracked_writer_key_annotations,
    )
    .await?;
    let receipt = canonical_commit_receipt_from_generated_result(&result)?;
    crate::live_state::apply_commit_projections_best_effort_in_transaction(
        transaction,
        &receipt,
        tracked_writer_key_annotations,
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
