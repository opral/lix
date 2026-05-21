//! Differential test harness target for fast and normal sql2 write execution.

#[cfg(test)]
mod tests {
    use crate::common::serialize_row_metadata;
    use crate::entity_pk::EntityPk;
    use crate::live_state::{LiveStateFilter, LiveStateScanRequest, MaterializedLiveStateRow};
    use crate::session::CreateVersionOptions;
    use crate::sql2::test_support::generators::{
        deterministic_repro_cases, generated_dml_cases, DifferentialExpectation, DifferentialParam,
        DifferentialProbe, DifferentialSqlCase, ExpectedExecution, ACTIVE_VERSION_PROBE_ID,
    };
    use crate::sql2::{WriteExecutorMode, WriteExecutorPath};
    use crate::storage::InMemoryStorageBackend;
    use crate::{Engine, ExecuteResult, LixError, Value};

    #[derive(Debug, Clone)]
    struct DifferentialOutcome {
        execution: ExecutionSignature,
        executor_path: Option<WriteExecutorPath>,
        staged_rows: Vec<ProbeSnapshot>,
        final_rows: Vec<ProbeSnapshot>,
    }

    impl PartialEq for DifferentialOutcome {
        fn eq(&self, other: &Self) -> bool {
            self.execution == other.execution
                && self.staged_rows == other.staged_rows
                && self.final_rows == other.final_rows
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum ExecutionSignature {
        Ok { rows_affected: u64 },
        Err { code: String, message: String },
    }

    #[derive(Debug, Clone, PartialEq)]
    struct ProbeSnapshot {
        name: String,
        rows: Vec<Vec<Value>>,
    }

    struct ProbeQuery {
        name: String,
        sql: String,
        params: Vec<Value>,
        version_column_indexes: &'static [usize],
    }

    #[tokio::test]
    async fn deterministic_known_repros_match_reference_writer() {
        for case in deterministic_repro_cases() {
            assert_case_matches_reference(&case).await;
        }
    }

    #[tokio::test]
    async fn generated_dml_cases_match_reference_writer() {
        for case in generated_dml_cases() {
            assert_case_matches_reference(&case).await;
        }
    }

    async fn assert_case_matches_reference(case: &DifferentialSqlCase) {
        let reference = run_case(case, WriteExecutorMode::ForceDataFusion).await;
        let candidate_mode = match case.expectation {
            DifferentialExpectation::SemanticParityMayFallback => WriteExecutorMode::Auto,
            DifferentialExpectation::FastRequiredParity => WriteExecutorMode::ForceFast,
        };
        let candidate = run_case(case, candidate_mode).await;
        assert_expected_execution(case, &reference.execution);
        assert_eq!(
            candidate, reference,
            "differential SQL seed '{}' diverged under {:?}\nSQL: {}",
            case.seed, candidate_mode, case.sql
        );
        if matches!(case.expected_execution, ExpectedExecution::Err { .. }) {
            assert_independent_no_mutation(case, &reference).await;
        } else if case.expectation == DifferentialExpectation::FastRequiredParity {
            assert_independent_no_mutation(case, &reference).await;
            assert_eq!(
                candidate.executor_path,
                Some(WriteExecutorPath::Fast),
                "differential SQL seed '{}' did not execute through the fast writer\nSQL: {}",
                case.seed,
                case.sql
            );
        } else if matches!(
            reference.execution,
            ExecutionSignature::Ok { rows_affected: 0 }
        ) {
            assert_independent_no_mutation(case, &reference).await;
        }
    }

    async fn assert_independent_no_mutation(
        case: &DifferentialSqlCase,
        reference: &DifferentialOutcome,
    ) {
        let baseline = run_baseline(case).await;
        assert_eq!(
            reference.staged_rows, baseline.staged_rows,
            "differential SQL seed '{}' changed staged rows in the independent no-mutation check\nSQL: {}",
            case.seed, case.sql
        );
        assert_eq!(
            reference.final_rows, baseline.final_rows,
            "differential SQL seed '{}' changed final rows in the independent no-mutation check\nSQL: {}",
            case.seed, case.sql
        );
    }

    async fn run_case(case: &DifferentialSqlCase, mode: WriteExecutorMode) -> DifferentialOutcome {
        let engine = open_initialized_engine().await;
        let session = engine
            .open_workspace_session()
            .await
            .expect("workspace session should open");
        create_probe_versions(&session).await;
        let active_version_id = session
            .active_version_id()
            .await
            .expect("differential session should have an active version");

        for setup_sql in case.setup_sql {
            session
                .execute_with_write_executor_mode(
                    setup_sql,
                    &[],
                    WriteExecutorMode::ForceDataFusion,
                )
                .await
                .unwrap_or_else(|error| {
                    panic!(
                        "differential SQL seed '{}' setup failed\nSQL: {}\nerror: {:?}",
                        case.seed, setup_sql, error
                    )
                });
        }

        let params = differential_params(case.params);
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("differential transaction should open");
        for setup_sql in case.transaction_setup_sql {
            transaction
                .execute_with_write_executor_mode(
                    setup_sql,
                    &[],
                    WriteExecutorMode::ForceDataFusion,
                )
                .await
                .unwrap_or_else(|error| {
                    panic!(
                        "differential SQL seed '{}' transaction setup failed\nSQL: {}\nerror: {:?}",
                        case.seed, setup_sql, error
                    )
                });
        }
        let execution_result = transaction
            .execute_with_write_executor_mode_and_trace(case.sql.as_ref(), &params, mode)
            .await;
        let execution = execution_signature(&execution_result);
        let executor_path = execution_result
            .as_ref()
            .ok()
            .and_then(|(_result, path)| *path);
        let staged_rows =
            probe_transaction_state(&mut transaction, case.probes, &active_version_id).await;

        match execution_result {
            Ok(_) => transaction
                .commit()
                .await
                .expect("successful differential case should commit"),
            Err(_) => transaction
                .rollback()
                .await
                .expect("failed differential case should rollback"),
        }

        let final_rows = probe_session_state(&session, case.probes, &active_version_id).await;
        DifferentialOutcome {
            execution,
            executor_path,
            staged_rows,
            final_rows,
        }
    }

    async fn run_baseline(case: &DifferentialSqlCase) -> DifferentialOutcome {
        let engine = open_initialized_engine().await;
        let session = engine
            .open_workspace_session()
            .await
            .expect("workspace session should open");
        create_probe_versions(&session).await;
        let active_version_id = session
            .active_version_id()
            .await
            .expect("differential session should have an active version");

        for setup_sql in case.setup_sql {
            session
                .execute_with_write_executor_mode(
                    setup_sql,
                    &[],
                    WriteExecutorMode::ForceDataFusion,
                )
                .await
                .unwrap_or_else(|error| {
                    panic!(
                        "differential SQL seed '{}' baseline setup failed\nSQL: {}\nerror: {:?}",
                        case.seed, setup_sql, error
                    )
                });
        }

        let mut transaction = session
            .begin_transaction()
            .await
            .expect("differential baseline transaction should open");
        for setup_sql in case.transaction_setup_sql {
            transaction
                .execute_with_write_executor_mode(
                    setup_sql,
                    &[],
                    WriteExecutorMode::ForceDataFusion,
                )
                .await
                .unwrap_or_else(|error| {
                    panic!(
                        "differential SQL seed '{}' baseline transaction setup failed\nSQL: {}\nerror: {:?}",
                        case.seed, setup_sql, error
                    )
                });
        }

        let staged_rows =
            probe_transaction_state(&mut transaction, case.probes, &active_version_id).await;
        transaction
            .commit()
            .await
            .expect("baseline differential case should commit setup");
        let final_rows = probe_session_state(&session, case.probes, &active_version_id).await;

        DifferentialOutcome {
            execution: ExecutionSignature::Ok { rows_affected: 0 },
            executor_path: None,
            staged_rows,
            final_rows,
        }
    }

    async fn open_initialized_engine() -> Engine {
        let backend = InMemoryStorageBackend::new();
        Engine::initialize(backend.clone())
            .await
            .expect("unit backend should initialize");
        Engine::new(backend)
            .await
            .expect("engine should open over initialized unit backend")
    }

    async fn create_probe_versions(session: &crate::SessionContext) {
        for id in ["version-a", "version-b"] {
            session
                .create_version(CreateVersionOptions {
                    id: Some(id.to_string()),
                    name: id.to_string(),
                    from_commit_id: None,
                })
                .await
                .unwrap_or_else(|error| panic!("failed to create probe version {id}: {error:?}"));
        }
    }

    fn execution_signature(
        result: &Result<(ExecuteResult, Option<WriteExecutorPath>), LixError>,
    ) -> ExecutionSignature {
        match result {
            Ok((result, _path)) => ExecutionSignature::Ok {
                rows_affected: result.rows_affected(),
            },
            Err(error) => ExecutionSignature::Err {
                code: error.code.clone(),
                message: error.message.clone(),
            },
        }
    }

    fn assert_expected_execution(case: &DifferentialSqlCase, execution: &ExecutionSignature) {
        match (case.expected_execution, execution) {
            (ExpectedExecution::Ok, ExecutionSignature::Ok { .. })
            | (ExpectedExecution::Err { .. }, ExecutionSignature::Err { .. }) => {}
            (ExpectedExecution::Ok, ExecutionSignature::Err { code, message }) => {
                panic!(
                    "differential SQL seed '{}' should succeed but failed with {code}: {message}\nSQL: {}",
                    case.seed, case.sql
                );
            }
            (ExpectedExecution::Err { code }, ExecutionSignature::Ok { rows_affected }) => {
                panic!(
                    "differential SQL seed '{}' should fail with {code} but succeeded with {rows_affected} rows affected\nSQL: {}",
                    case.seed, case.sql
                );
            }
        }
        if let (
            ExpectedExecution::Err {
                code: expected_code,
            },
            ExecutionSignature::Err { code, message },
        ) = (case.expected_execution, execution)
        {
            assert_eq!(
                code, expected_code,
                "differential SQL seed '{}' failed with the wrong error code: {code}: {message}\nSQL: {}",
                case.seed, case.sql
            );
        }
    }

    fn differential_params(params: &[DifferentialParam]) -> Vec<Value> {
        params
            .iter()
            .map(|param| match param {
                DifferentialParam::Json(value) => {
                    let value =
                        serde_json::from_str(value).expect("differential JSON param should parse");
                    Value::Json(value)
                }
            })
            .collect()
    }

    async fn probe_session_state(
        session: &crate::SessionContext,
        probes: &[DifferentialProbe],
        active_version_id: &str,
    ) -> Vec<ProbeSnapshot> {
        let mut snapshots = Vec::with_capacity(probes.len());
        for probe in probes {
            let query = probe_query(probe, active_version_id);
            snapshots.push(ProbeSnapshot {
                name: query.name,
                rows: session
                    .execute(&query.sql, &query.params)
                    .await
                    .unwrap_or_else(|error| {
                        panic!(
                            "final differential probe failed\nSQL: {}\nerror: {error:?}",
                            query.sql
                        )
                    })
                    .rows()
                    .iter()
                    .map(|row| {
                        canonical_probe_values(
                            row.values(),
                            active_version_id,
                            query.version_column_indexes,
                        )
                    })
                    .collect(),
            });
        }
        snapshots
    }

    async fn probe_transaction_state(
        transaction: &mut crate::session::SessionTransaction,
        probes: &[DifferentialProbe],
        active_version_id: &str,
    ) -> Vec<ProbeSnapshot> {
        let mut snapshots = Vec::with_capacity(probes.len());
        for probe in probes {
            if let Some(snapshot) =
                synthetic_staged_by_version_probe(transaction, probe, active_version_id).await
            {
                snapshots.push(snapshot);
                continue;
            }
            let query = probe_query(probe, active_version_id);
            snapshots.push(ProbeSnapshot {
                name: query.name,
                rows: transaction
                    .execute(&query.sql, &query.params)
                    .await
                    .unwrap_or_else(|error| {
                        panic!(
                            "staged differential probe failed\nSQL: {}\nerror: {error:?}",
                            query.sql
                        )
                    })
                    .rows()
                    .iter()
                    .map(|row| {
                        canonical_probe_values(
                            row.values(),
                            active_version_id,
                            query.version_column_indexes,
                        )
                    })
                    .collect(),
            });
        }
        snapshots
    }

    fn probe_query(probe: &DifferentialProbe, active_version_id: &str) -> ProbeQuery {
        match probe {
            DifferentialProbe::LixStateActive {
                schema_key,
                entity_pks,
            } => {
                let mut params = Vec::with_capacity(entity_pks.len() + 1);
                params.push(Value::Text((*schema_key).to_string()));
                let placeholders = entity_pks
                    .iter()
                    .enumerate()
                    .map(|(index, entity_pk)| {
                        params.push(Value::Json(serde_json::json!([*entity_pk])));
                        format!("${}", index + 2)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                ProbeQuery {
                    name: format!("lix_state:{schema_key}:{entity_pks:?}"),
                    sql: format!(
                        "SELECT entity_pk, schema_key, file_id, snapshot_content, metadata, global, untracked \
                         FROM lix_state \
                         WHERE schema_key = $1 AND entity_pk IN ({placeholders}) \
                         ORDER BY schema_key, entity_pk, file_id"
                    ),
                    params,
                    version_column_indexes: &[],
                }
            }
            DifferentialProbe::LixStateByVersion {
                schema_key,
                entity_pks,
                version_ids,
            } => {
                let mut params = Vec::with_capacity(entity_pks.len() + version_ids.len() + 1);
                params.push(Value::Text((*schema_key).to_string()));
                let entity_placeholders = entity_pks
                    .iter()
                    .enumerate()
                    .map(|(index, entity_pk)| {
                        params.push(Value::Json(serde_json::json!([*entity_pk])));
                        format!("${}", index + 2)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                let version_offset = params.len();
                let version_placeholders = version_ids
                    .iter()
                    .enumerate()
                    .map(|(index, version_id)| {
                        params.push(Value::Text(resolve_probe_version_id(
                            version_id,
                            active_version_id,
                        )));
                        format!("${}", version_offset + index + 1)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                ProbeQuery {
                    name: format!(
                        "lix_state_by_version:{schema_key}:{entity_pks:?}:{version_ids:?}"
                    ),
                    sql: format!(
                        "SELECT entity_pk, schema_key, file_id, version_id, snapshot_content, metadata, global, untracked \
                         FROM lix_state_by_version \
                         WHERE schema_key = $1 \
                           AND entity_pk IN ({entity_placeholders}) \
                           AND version_id IN ({version_placeholders}) \
                         ORDER BY schema_key, entity_pk, file_id, version_id"
                    ),
                    params,
                    version_column_indexes: &[3],
                }
            }
            DifferentialProbe::RegisteredSchemaActive => ProbeQuery {
                name: "lix_registered_schema".to_string(),
                sql: "SELECT lixcol_entity_pk, value, lixcol_metadata, lixcol_global, lixcol_untracked \
                 FROM lix_registered_schema \
                 ORDER BY lixcol_entity_pk"
                    .to_string(),
                params: Vec::new(),
                version_column_indexes: &[],
            },
            DifferentialProbe::RegisteredSchemaByVersion { version_ids } => {
                let mut params = Vec::with_capacity(version_ids.len());
                let placeholders = version_ids
                    .iter()
                    .enumerate()
                    .map(|(index, version_id)| {
                        params.push(Value::Text(resolve_probe_version_id(
                            version_id,
                            active_version_id,
                        )));
                        format!("${}", index + 1)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                ProbeQuery {
                    name: format!("lix_registered_schema_by_version:{version_ids:?}"),
                    sql: format!(
                        "SELECT lixcol_entity_pk, value, lixcol_version_id, lixcol_metadata, lixcol_global, lixcol_untracked \
                         FROM lix_registered_schema_by_version \
                         WHERE lixcol_version_id IN ({placeholders}) \
                         ORDER BY lixcol_entity_pk, lixcol_version_id"
                    ),
                    params,
                    version_column_indexes: &[2],
                }
            }
        }
    }

    async fn synthetic_staged_by_version_probe(
        transaction: &mut crate::session::SessionTransaction,
        probe: &DifferentialProbe,
        active_version_id: &str,
    ) -> Option<ProbeSnapshot> {
        match probe {
            DifferentialProbe::LixStateByVersion {
                schema_key,
                entity_pks,
                version_ids,
            } => {
                let rows = scan_transaction_live_state(
                    transaction,
                    schema_key,
                    *entity_pks,
                    *version_ids,
                    active_version_id,
                )
                .await;
                Some(ProbeSnapshot {
                    name: format!(
                        "lix_state_by_version_staged:{schema_key}:{entity_pks:?}:{version_ids:?}"
                    ),
                    rows: lix_state_by_version_rows(rows, active_version_id),
                })
            }
            DifferentialProbe::RegisteredSchemaByVersion { version_ids } => {
                let rows = scan_transaction_live_state(
                    transaction,
                    "lix_registered_schema",
                    &[],
                    *version_ids,
                    active_version_id,
                )
                .await;
                Some(ProbeSnapshot {
                    name: format!("lix_registered_schema_by_version_staged:{version_ids:?}"),
                    rows: registered_schema_by_version_rows(rows, active_version_id),
                })
            }
            _ => None,
        }
    }

    async fn scan_transaction_live_state(
        transaction: &mut crate::session::SessionTransaction,
        schema_key: &str,
        entity_pks: &[&str],
        version_ids: &[&str],
        active_version_id: &str,
    ) -> Vec<MaterializedLiveStateRow> {
        let rows = transaction
            .scan_live_state_for_test(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec![schema_key.to_string()],
                    entity_pks: entity_pks
                        .iter()
                        .map(|entity_pk| EntityPk::single(*entity_pk))
                        .collect(),
                    version_ids: version_ids
                        .iter()
                        .map(|version_id| resolve_probe_version_id(version_id, active_version_id))
                        .collect(),
                    ..LiveStateFilter::default()
                },
                ..LiveStateScanRequest::default()
            })
            .await
            .unwrap_or_else(|error| {
                panic!(
                "staged live-state differential probe failed for schema '{schema_key}': {error:?}"
            )
            });
        rows
    }

    fn lix_state_by_version_rows(
        mut rows: Vec<MaterializedLiveStateRow>,
        active_version_id: &str,
    ) -> Vec<Vec<Value>> {
        rows.sort_by_key(|row| {
            (
                row.schema_key.clone(),
                row.entity_pk.clone(),
                row.file_id.clone(),
                row.version_id.clone(),
            )
        });
        rows.iter()
            .map(|row| {
                canonical_probe_values(
                    &[
                        entity_pk_value(row),
                        Value::Text(row.schema_key.clone()),
                        optional_text_value(row.file_id.clone()),
                        Value::Text(row.version_id.clone()),
                        optional_text_value(row.snapshot_content.clone()),
                        row.metadata
                            .as_ref()
                            .map(serialize_row_metadata)
                            .map(Value::Text)
                            .unwrap_or(Value::Null),
                        Value::Boolean(row.global),
                        Value::Boolean(row.untracked),
                    ],
                    active_version_id,
                    &[3],
                )
            })
            .collect()
    }

    fn registered_schema_by_version_rows(
        mut rows: Vec<MaterializedLiveStateRow>,
        active_version_id: &str,
    ) -> Vec<Vec<Value>> {
        rows.sort_by_key(|row| (row.entity_pk.clone(), row.version_id.clone()));
        rows.iter()
            .map(|row| {
                let value = row
                    .snapshot_content
                    .as_deref()
                    .and_then(|snapshot| serde_json::from_str::<serde_json::Value>(snapshot).ok())
                    .and_then(|snapshot| snapshot.get("value").cloned())
                    .map(|value| {
                        Value::Text(serde_json::to_string(&value).expect("JSON serializes"))
                    })
                    .unwrap_or(Value::Null);
                canonical_probe_values(
                    &[
                        entity_pk_value(row),
                        value,
                        Value::Text(row.version_id.clone()),
                        row.metadata
                            .as_ref()
                            .map(serialize_row_metadata)
                            .map(Value::Text)
                            .unwrap_or(Value::Null),
                        Value::Boolean(row.global),
                        Value::Boolean(row.untracked),
                    ],
                    active_version_id,
                    &[2],
                )
            })
            .collect()
    }

    fn entity_pk_value(row: &MaterializedLiveStateRow) -> Value {
        Value::Text(
            row.entity_pk
                .as_json_array_text()
                .expect("materialized entity pk should encode"),
        )
    }

    fn optional_text_value(value: Option<String>) -> Value {
        value.map(Value::Text).unwrap_or(Value::Null)
    }

    fn resolve_probe_version_id(version_id: &str, active_version_id: &str) -> String {
        if version_id == ACTIVE_VERSION_PROBE_ID {
            active_version_id.to_string()
        } else {
            version_id.to_string()
        }
    }

    fn canonical_probe_values(
        values: &[Value],
        active_version_id: &str,
        version_column_indexes: &[usize],
    ) -> Vec<Value> {
        values
            .iter()
            .enumerate()
            .map(|(index, value)| match value {
                Value::Text(text)
                    if text == active_version_id && version_column_indexes.contains(&index) =>
                {
                    Value::Text(ACTIVE_VERSION_PROBE_ID.to_string())
                }
                other => other.clone(),
            })
            .collect()
    }
}
