use std::collections::BTreeSet;

use crate::functions::LixFunctionProvider;
use crate::schema::builtin::types::LixVersionPointer;
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixError, LixTransaction, QueryResult, Value};
use async_trait::async_trait;

use super::generate_commit::generate_commit;
use super::runtime::{
    bind_statement_batch_for_dialect, build_statement_batch_from_generate_commit_result,
    load_commit_active_accounts,
};
use super::state_source::{
    load_committed_global_tip_commit_id, load_committed_version_tip_commit_id,
    load_version_info_for_versions, CommitQueryExecutor,
};
use super::types::{
    DomainChangeInput, GenerateCommitArgs, GenerateCommitResult, ProposedDomainChange, VersionInfo,
    VersionSnapshot,
};

const COMMIT_IDEMPOTENCY_TABLE: &str = "lix_internal_commit_idempotency";
const VERSION_POINTER_SCHEMA_KEY: &str = "lix_version_pointer";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AppendWriteLane {
    Version(String),
    GlobalAdmin,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AppendExpectedTip {
    CommitId(String),
    CreateIfMissing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AppendCommitPreconditions {
    pub(crate) write_lane: AppendWriteLane,
    pub(crate) expected_tip: AppendExpectedTip,
    pub(crate) idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AppendCommitArgs {
    pub(crate) timestamp: String,
    pub(crate) changes: Vec<ProposedDomainChange>,
    pub(crate) preconditions: AppendCommitPreconditions,
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
        return Err(AppendCommitError {
            kind: AppendCommitErrorKind::EmptyBatch,
            message: "append_commit_if_preconditions_hold requires at least one change".to_string(),
        });
    }

    let concrete_lane = concrete_lane(&args.preconditions)?;
    validate_change_versions(&args.changes, &concrete_lane)?;

    let current_tip = {
        let mut executor = TransactionCommitExecutor { transaction };
        load_current_tip_commit_id(&mut executor, &concrete_lane).await?
    };
    let existing_replay = {
        let mut executor = TransactionCommitExecutor { transaction };
        load_existing_idempotency_commit_id(
            &mut executor,
            &concrete_lane,
            &args.preconditions.idempotency_key,
        )
        .await?
    };

    match (&args.preconditions.expected_tip, current_tip.as_deref()) {
        (AppendExpectedTip::CommitId(expected), Some(current)) if current != expected => {
            if existing_replay.as_deref() == Some(current) {
                return Ok(AppendCommitResult {
                    disposition: AppendCommitDisposition::Replay,
                    committed_tip: current.to_string(),
                    commit_result: None,
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
                    "append precondition failed for '{}': version pointer is missing",
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
        });
    }

    if let Some(invariant_checker) = invariant_checker {
        invariant_checker.recheck_invariants(transaction).await?;
    }

    let domain_changes = materialize_domain_changes(&args.timestamp, &args.changes, functions)?;
    let affected_versions = domain_changes
        .iter()
        .map(|change| change.version_id.clone())
        .collect::<BTreeSet<_>>();
    let mut versions = {
        let mut executor = TransactionCommitExecutor { transaction };
        load_version_info_for_versions(&mut executor, &affected_versions)
            .await
            .map_err(backend_error)?
    };
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
    let active_accounts = {
        let mut executor = TransactionCommitExecutor { transaction };
        load_commit_active_accounts(&mut executor, &domain_changes)
            .await
            .map_err(backend_error)?
    };
    let commit_result = generate_commit(
        GenerateCommitArgs {
            timestamp: args.timestamp.clone(),
            active_accounts,
            changes: domain_changes,
            versions,
        },
        || functions.uuid_v7(),
    )
    .map_err(backend_error)?;
    let committed_tip = extract_committed_tip_id(&commit_result, &concrete_lane)?;

    let prepared_statements = bind_statement_batch_for_dialect(
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

    for statement in prepared_statements {
        transaction
            .execute(&statement.sql, &statement.params)
            .await
            .map_err(backend_error)?;
    }
    insert_idempotency_row(
        transaction,
        &concrete_lane,
        &args.preconditions.idempotency_key,
        &committed_tip,
        &args.timestamp,
    )
    .await?;

    Ok(AppendCommitResult {
        disposition: AppendCommitDisposition::Applied,
        committed_tip,
        commit_result: Some(commit_result),
    })
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

fn validate_change_versions(
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

async fn load_current_tip_commit_id(
    executor: &mut dyn CommitQueryExecutor,
    concrete_lane: &ConcreteWriteLane,
) -> Result<Option<String>, AppendCommitError> {
    match concrete_lane {
        ConcreteWriteLane::Version { version_id } => {
            load_committed_version_tip_commit_id(executor, version_id)
                .await
                .map_err(backend_error)
        }
        ConcreteWriteLane::GlobalAdmin => load_committed_global_tip_commit_id(executor)
            .await
            .map_err(backend_error),
    }
}

async fn load_existing_idempotency_commit_id(
    executor: &mut dyn CommitQueryExecutor,
    concrete_lane: &ConcreteWriteLane,
    idempotency_key: &str,
) -> Result<Option<String>, AppendCommitError> {
    let sql = format!(
        "SELECT commit_id \
         FROM {table_name} \
         WHERE write_lane = '{write_lane}' \
           AND idempotency_key = '{idempotency_key}' \
         LIMIT 1",
        table_name = COMMIT_IDEMPOTENCY_TABLE,
        write_lane = escape_sql_string(&lane_storage_key(concrete_lane)),
        idempotency_key = escape_sql_string(idempotency_key),
    );
    let result = executor.execute(&sql, &[]).await.map_err(backend_error)?;
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    match row.first() {
        Some(Value::Text(commit_id)) if !commit_id.is_empty() => Ok(Some(commit_id.clone())),
        Some(Value::Null) | None => Ok(None),
        Some(other) => Err(AppendCommitError {
            kind: AppendCommitErrorKind::Internal,
            message: format!("idempotency lookup returned unexpected value {other:?}"),
        }),
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
            change.schema_key == VERSION_POINTER_SCHEMA_KEY && change.entity_id == version_id
        })
        .ok_or_else(|| AppendCommitError {
            kind: AppendCommitErrorKind::Internal,
            message: format!(
                "generated commit result did not include a version pointer for '{}'",
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
                    "generated version pointer for '{}' is missing snapshot_content",
                    version_id
                ),
            })?;
    let pointer: LixVersionPointer =
        serde_json::from_str(snapshot_content).map_err(|error| AppendCommitError {
            kind: AppendCommitErrorKind::Internal,
            message: format!(
                "generated version pointer for '{}' could not be parsed: {error}",
                version_id
            ),
        })?;
    if pointer.commit_id.is_empty() {
        return Err(AppendCommitError {
            kind: AppendCommitErrorKind::Internal,
            message: format!(
                "generated version pointer for '{}' contained an empty commit_id",
                version_id
            ),
        });
    }
    Ok(pointer.commit_id)
}

async fn insert_idempotency_row(
    transaction: &mut dyn LixTransaction,
    concrete_lane: &ConcreteWriteLane,
    idempotency_key: &str,
    commit_id: &str,
    created_at: &str,
) -> Result<(), AppendCommitError> {
    let sql = format!(
        "INSERT INTO {table_name} (write_lane, idempotency_key, commit_id, created_at) \
         VALUES ('{write_lane}', '{idempotency_key}', '{commit_id}', '{created_at}')",
        table_name = COMMIT_IDEMPOTENCY_TABLE,
        write_lane = escape_sql_string(&lane_storage_key(concrete_lane)),
        idempotency_key = escape_sql_string(idempotency_key),
        commit_id = escape_sql_string(commit_id),
        created_at = escape_sql_string(created_at),
    );
    transaction
        .execute(&sql, &[])
        .await
        .map_err(backend_error)?;
    Ok(())
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
        AppendCommitPreconditions, AppendExpectedTip, AppendWriteLane,
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
        version_tips: HashMap<String, String>,
        idempotency_rows: HashMap<(String, String), String>,
        executed_sql: Vec<String>,
    }

    #[async_trait(?Send)]
    impl LixTransaction for FakeTransaction {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&mut self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            self.executed_sql.push(sql.to_string());

            if sql.contains("FROM lix_internal_live_v1_lix_version_pointer")
                && sql.contains("entity_id = 'global'")
            {
                let rows = self
                    .version_tips
                    .get(GLOBAL_VERSION_ID)
                    .map(|commit_id| {
                        vec![Value::Text(
                            crate::version::version_pointer_snapshot_content(
                                GLOBAL_VERSION_ID,
                                commit_id,
                            ),
                        )]
                    })
                    .into_iter()
                    .collect::<Vec<_>>();
                return Ok(QueryResult {
                    rows,
                    columns: vec!["snapshot_content".to_string()],
                });
            }
            if sql.contains("FROM lix_internal_change c")
                && sql.contains("c.schema_key = 'lix_version_pointer'")
            {
                let rows = self
                    .version_tips
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

            if sql.contains("FROM lix_internal_commit_idempotency") {
                let rows = self
                    .idempotency_rows
                    .iter()
                    .filter(|((lane, key), _)| {
                        sql.contains(&format!("write_lane = '{}'", lane))
                            && sql.contains(&format!("idempotency_key = '{}'", key))
                    })
                    .map(|(_, commit_id)| vec![Value::Text(commit_id.clone())])
                    .collect::<Vec<_>>();
                return Ok(QueryResult {
                    rows,
                    columns: vec!["commit_id".to_string()],
                });
            }

            if sql.starts_with("INSERT INTO lix_internal_commit_idempotency ") {
                let lane =
                    extract_single_quoted_value(sql, "VALUES ('").expect("lane should be present");
                let key = extract_nth_single_quoted_value(sql, 1).expect("key should be present");
                let commit_id =
                    extract_nth_single_quoted_value(sql, 2).expect("commit id should be present");
                self.idempotency_rows.insert((lane, key), commit_id);
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
            .version_tips
            .insert("version-a".to_string(), "commit-123".to_string());
        let mut functions = CountingFunctionProvider::default();
        let mut checker = RecordingInvariantChecker::default();

        let result = append_commit_if_preconditions_hold(
            &mut transaction,
            AppendCommitArgs {
                timestamp: "2026-03-06T14:22:00.000Z".to_string(),
                changes: vec![sample_change()],
                preconditions: AppendCommitPreconditions {
                    write_lane: AppendWriteLane::Version("version-a".to_string()),
                    expected_tip: AppendExpectedTip::CommitId("commit-123".to_string()),
                    idempotency_key: "idem-1".to_string(),
                },
            },
            &mut functions,
            Some(&mut checker),
        )
        .await
        .expect("append should succeed");

        assert_eq!(result.disposition, AppendCommitDisposition::Applied);
        assert!(result.commit_result.is_some());
        assert_eq!(checker.calls, 1);
        assert!(
            transaction
                .executed_sql
                .iter()
                .any(|sql| sql.starts_with("INSERT INTO lix_internal_commit_idempotency ")),
            "append should persist an idempotency row"
        );
    }

    #[tokio::test]
    async fn replays_when_same_idempotency_key_already_committed() {
        let mut transaction = FakeTransaction::default();
        transaction
            .version_tips
            .insert("version-a".to_string(), "commit-456".to_string());
        transaction.idempotency_rows.insert(
            ("version:version-a".to_string(), "idem-1".to_string()),
            "commit-456".to_string(),
        );
        let mut functions = CountingFunctionProvider::default();
        let mut checker = RecordingInvariantChecker::default();

        let result = append_commit_if_preconditions_hold(
            &mut transaction,
            AppendCommitArgs {
                timestamp: "2026-03-06T14:22:00.000Z".to_string(),
                changes: vec![sample_change()],
                preconditions: AppendCommitPreconditions {
                    write_lane: AppendWriteLane::Version("version-a".to_string()),
                    expected_tip: AppendExpectedTip::CommitId("commit-123".to_string()),
                    idempotency_key: "idem-1".to_string(),
                },
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
    async fn rejects_tip_drift_without_matching_idempotency_row() {
        let mut transaction = FakeTransaction::default();
        transaction
            .version_tips
            .insert("version-a".to_string(), "commit-456".to_string());
        let mut functions = CountingFunctionProvider::default();
        let mut checker = RecordingInvariantChecker::default();

        let error = append_commit_if_preconditions_hold(
            &mut transaction,
            AppendCommitArgs {
                timestamp: "2026-03-06T14:22:00.000Z".to_string(),
                changes: vec![sample_change()],
                preconditions: AppendCommitPreconditions {
                    write_lane: AppendWriteLane::Version("version-a".to_string()),
                    expected_tip: AppendExpectedTip::CommitId("commit-123".to_string()),
                    idempotency_key: "idem-1".to_string(),
                },
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
                timestamp: "2026-03-06T14:22:00.000Z".to_string(),
                changes: vec![sample_change()],
                preconditions: AppendCommitPreconditions {
                    write_lane: AppendWriteLane::Version("version-a".to_string()),
                    expected_tip: AppendExpectedTip::CommitId("commit-123".to_string()),
                    idempotency_key: "idem-1".to_string(),
                },
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
                timestamp: "2026-03-06T14:22:00.000Z".to_string(),
                changes: vec![sample_change()],
                preconditions: AppendCommitPreconditions {
                    write_lane: AppendWriteLane::Version("version-a".to_string()),
                    expected_tip: AppendExpectedTip::CreateIfMissing,
                    idempotency_key: "idem-create".to_string(),
                },
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
        transaction.version_tips.insert(
            GLOBAL_VERSION_ID.to_string(),
            "commit-global-123".to_string(),
        );
        let mut functions = CountingFunctionProvider::default();

        let result = append_commit_if_preconditions_hold(
            &mut transaction,
            AppendCommitArgs {
                timestamp: "2026-03-06T14:22:00.000Z".to_string(),
                changes: vec![sample_global_change()],
                preconditions: AppendCommitPreconditions {
                    write_lane: AppendWriteLane::GlobalAdmin,
                    expected_tip: AppendExpectedTip::CommitId("commit-global-123".to_string()),
                    idempotency_key: "idem-global".to_string(),
                },
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
            .version_tips
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
                timestamp: "2026-03-06T14:22:00.000Z".to_string(),
                changes: vec![sample_change()],
                preconditions: AppendCommitPreconditions {
                    write_lane: AppendWriteLane::Version("version-a".to_string()),
                    expected_tip: AppendExpectedTip::CommitId("commit-123".to_string()),
                    idempotency_key: "idem-1".to_string(),
                },
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
                .any(|sql| sql.starts_with("INSERT INTO lix_internal_commit_idempotency ")),
            "append should abort before persisting idempotency state"
        );
    }

    fn extract_single_quoted_value(sql: &str, prefix: &str) -> Option<String> {
        let start = sql.find(prefix)? + prefix.len();
        let rest = &sql[start..];
        let end = rest.find('\'')?;
        Some(rest[..end].to_string())
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
