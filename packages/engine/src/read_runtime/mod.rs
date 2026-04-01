use crate::contracts::artifacts::{
    DerivedRow, PendingViewFilter, PendingViewOrderClause, PendingViewProjection,
    ReadTimeProjectionRead, ResultContract, SessionExecutionMode,
};
use crate::live_state::projection::dispatch::derive_read_time_projection_rows_with_backend;
use crate::runtime::execution_state::ExecutionRuntimeState;
use crate::runtime::{
    normalize_sql_execution_error_with_backend, RuntimeHost, TransactionBackendAdapter,
};
use crate::sql::executor::execute_prepared::execute_prepared_with_transaction;
use crate::sql::executor::execution_program::{
    BoundStatementTemplateInstance, ExecutionContext, ExecutionProgram,
};
use crate::sql::executor::{
    compile_execution_from_template_instance_with_backend, execute_prepared_public_read,
    CompiledExecution, PreparationPolicy,
};
use crate::{
    ExecuteResult, LixBackend, LixBackendTransaction, LixError, QueryResult, TransactionMode, Value,
};
use sqlparser::ast::Statement;
use std::time::Instant;

pub(crate) struct PreparedCommittedReadProgram {
    pub(crate) transaction_mode: TransactionMode,
    steps: Vec<PreparedCommittedReadStep>,
}

struct PreparedCommittedReadStep {
    bound_statement_template: BoundStatementTemplateInstance,
    compiled: Option<CompiledExecution>,
    source_statement: Statement,
}

pub(crate) async fn prepare_committed_read_program(
    runtime_host: &dyn RuntimeHost,
    backend: &dyn LixBackend,
    program: &ExecutionProgram,
    allow_internal_tables: bool,
    context: &ExecutionContext,
    execution_mode: SessionExecutionMode,
) -> Result<PreparedCommittedReadProgram, LixError> {
    let runtime_state = context.execution_runtime_state().expect(
        "committed execution should install an execution runtime state before step preparation",
    );
    let precompile_steps = !matches!(
        execution_mode,
        SessionExecutionMode::CommittedRuntimeMutation
    ) || !runtime_state.settings().enabled;
    let mut mode =
        baseline_transaction_mode_for_committed_read_program(execution_mode, runtime_state);
    let mut steps = Vec::new();

    for step in program.steps() {
        let compiled = if precompile_steps {
            let compiled = compile_bound_statement_template_instance_for_committed_read(
                runtime_host,
                backend,
                step,
                allow_internal_tables,
                context,
                runtime_state,
            )
            .await?;
            mode = merge_committed_read_transaction_mode(
                mode,
                transaction_mode_for_committed_read_execution(&compiled)?,
            );
            Some(compiled)
        } else {
            None
        };

        steps.push(PreparedCommittedReadStep {
            bound_statement_template: step.clone(),
            compiled,
            source_statement: step.statement().clone(),
        });
    }

    Ok(PreparedCommittedReadProgram {
        transaction_mode: mode,
        steps,
    })
}

async fn compile_bound_statement_template_instance_for_committed_read(
    runtime_host: &dyn RuntimeHost,
    backend: &dyn LixBackend,
    bound_statement_template: &BoundStatementTemplateInstance,
    allow_internal_tables: bool,
    context: &ExecutionContext,
    runtime_state: &ExecutionRuntimeState,
) -> Result<CompiledExecution, LixError> {
    let parsed_statements = std::slice::from_ref(bound_statement_template.statement());
    match compile_execution_from_template_instance_with_backend(
        runtime_host,
        backend,
        None,
        bound_statement_template,
        context.active_version_id.as_str(),
        &context.active_account_ids,
        context.options.writer_key.as_deref(),
        allow_internal_tables,
        Some(&context.public_surface_registry),
        Some(runtime_state),
        PreparationPolicy {
            skip_side_effect_collection: false,
        },
    )
    .await
    {
        Ok(compiled) => Ok(compiled),
        Err(error) => {
            Err(normalize_sql_execution_error_with_backend(backend, error, parsed_statements).await)
        }
    }
}

fn baseline_transaction_mode_for_committed_read_program(
    execution_mode: SessionExecutionMode,
    runtime_state: &ExecutionRuntimeState,
) -> TransactionMode {
    match execution_mode {
        SessionExecutionMode::CommittedRead => TransactionMode::Read,
        SessionExecutionMode::CommittedRuntimeMutation => {
            if runtime_state.settings().enabled {
                TransactionMode::Write
            } else {
                TransactionMode::Read
            }
        }
        SessionExecutionMode::WriteTransaction => TransactionMode::Write,
    }
}

fn merge_committed_read_transaction_mode(
    current: TransactionMode,
    next: TransactionMode,
) -> TransactionMode {
    match (current, next) {
        (TransactionMode::Write, _) | (_, TransactionMode::Write) => TransactionMode::Write,
        (TransactionMode::Deferred, _) | (_, TransactionMode::Deferred) => {
            TransactionMode::Deferred
        }
        _ => TransactionMode::Read,
    }
}

fn transaction_mode_for_committed_read_execution(
    compiled: &CompiledExecution,
) -> Result<TransactionMode, LixError> {
    if compiled.plain_explain().is_some() {
        return Ok(TransactionMode::Read);
    }
    if let Some(public_read) = compiled.public_read() {
        return Ok(public_read.committed_read_mode().transaction_mode());
    }
    if compiled.internal_execution().is_some() {
        return if compiled.read_only_query {
            Ok(TransactionMode::Read)
        } else {
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "committed read routing compiled a non-read internal step unexpectedly",
            ))
        };
    }
    Err(LixError::new(
        "LIX_ERROR_UNKNOWN",
        "committed read routing compiled a public write unexpectedly",
    ))
}

fn public_result_from_contract(
    contract: ResultContract,
    internal_result: &QueryResult,
) -> QueryResult {
    match contract {
        ResultContract::DmlNoReturning => QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        },
        ResultContract::Select | ResultContract::DmlReturning | ResultContract::Other => {
            internal_result.clone()
        }
    }
}

#[allow(dead_code)]
pub(crate) async fn execute_read_time_projection_read_with_backend(
    backend: &dyn LixBackend,
    artifact: &ReadTimeProjectionRead,
) -> Result<QueryResult, LixError> {
    let mut rows = derive_read_time_projection_rows_with_backend(backend)
        .await?
        .into_iter()
        .filter(|row| row.surface_name == artifact.surface.public_name())
        .filter(|row| {
            artifact
                .query
                .filters
                .iter()
                .all(|filter| read_time_projection_filter_matches_row(filter, row))
        })
        .collect::<Vec<_>>();

    rows.sort_by(|left, right| {
        compare_read_time_projection_rows(left, right, &artifact.query.order_by)
    });

    if let Some(limit) = artifact.query.limit {
        rows.truncate(limit);
    }

    let columns = artifact
        .query
        .projections
        .iter()
        .map(read_time_projection_output_column)
        .collect::<Vec<_>>();

    if artifact
        .query
        .projections
        .iter()
        .all(|projection| matches!(projection, PendingViewProjection::CountAll { .. }))
    {
        return Ok(QueryResult {
            columns,
            rows: vec![artifact
                .query
                .projections
                .iter()
                .map(|_| Value::Integer(rows.len() as i64))
                .collect()],
        });
    }

    Ok(QueryResult {
        columns,
        rows: rows
            .into_iter()
            .map(|row| {
                artifact
                    .query
                    .projections
                    .iter()
                    .map(|projection| read_time_projection_value(&row, projection))
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?,
    })
}

#[allow(dead_code)]
fn read_time_projection_output_column(projection: &PendingViewProjection) -> String {
    match projection {
        PendingViewProjection::Column { output_column, .. }
        | PendingViewProjection::CountAll { output_column } => output_column.clone(),
    }
}

#[allow(dead_code)]
fn read_time_projection_filter_matches_row(filter: &PendingViewFilter, row: &DerivedRow) -> bool {
    match filter {
        PendingViewFilter::And(filters) => filters
            .iter()
            .all(|filter| read_time_projection_filter_matches_row(filter, row)),
        PendingViewFilter::Or(filters) => filters
            .iter()
            .any(|filter| read_time_projection_filter_matches_row(filter, row)),
        PendingViewFilter::Equals(column, expected) => {
            read_time_projection_row_value(row, column).is_some_and(|actual| actual == *expected)
        }
        PendingViewFilter::In(column, expected) => read_time_projection_row_value(row, column)
            .is_some_and(|actual| expected.iter().any(|candidate| candidate == &actual)),
        PendingViewFilter::IsNull(column) => {
            matches!(
                read_time_projection_row_value(row, column),
                Some(Value::Null) | None
            )
        }
        PendingViewFilter::IsNotNull(column) => !matches!(
            read_time_projection_row_value(row, column),
            Some(Value::Null) | None
        ),
        PendingViewFilter::Like {
            column,
            pattern,
            case_insensitive,
        } => read_time_projection_row_value(row, column)
            .and_then(|actual| projection_filter_text(&actual))
            .is_some_and(|actual| sql_like_matches(&actual, pattern, *case_insensitive)),
    }
}

#[allow(dead_code)]
fn read_time_projection_row_value(row: &DerivedRow, column: &str) -> Option<Value> {
    row.values.get(column).cloned()
}

#[allow(dead_code)]
fn read_time_projection_value(
    row: &DerivedRow,
    projection: &PendingViewProjection,
) -> Result<Value, LixError> {
    match projection {
        PendingViewProjection::Column { source_column, .. } => {
            read_time_projection_row_value(row, source_column).ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "read-time projection query requested unsupported column '{source_column}'"
                    ),
                )
            })
        }
        PendingViewProjection::CountAll { .. } => Ok(Value::Integer(1)),
    }
}

#[allow(dead_code)]
fn compare_read_time_projection_rows(
    left: &DerivedRow,
    right: &DerivedRow,
    order_by: &[PendingViewOrderClause],
) -> std::cmp::Ordering {
    for clause in order_by {
        let ordering = compare_projection_values(
            &read_time_projection_row_value(left, &clause.column),
            &read_time_projection_row_value(right, &clause.column),
        );
        if ordering != std::cmp::Ordering::Equal {
            return if clause.descending {
                ordering.reverse()
            } else {
                ordering
            };
        }
    }

    let identity_ordering = left.identity.cmp(&right.identity);
    if identity_ordering != std::cmp::Ordering::Equal {
        return identity_ordering;
    }

    format!("{:?}", left.values).cmp(&format!("{:?}", right.values))
}

#[allow(dead_code)]
fn compare_projection_values(left: &Option<Value>, right: &Option<Value>) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(left), Some(right)) => format!("{left:?}").cmp(&format!("{right:?}")),
    }
}

#[allow(dead_code)]
fn projection_filter_text(value: &Value) -> Option<String> {
    match value {
        Value::Text(text) => Some(text.clone()),
        Value::Integer(value) => Some(value.to_string()),
        Value::Boolean(value) => Some(if *value { "1" } else { "0" }.to_string()),
        Value::Real(value) => Some(value.to_string()),
        Value::Json(value) => Some(value.to_string()),
        Value::Null | Value::Blob(_) => None,
    }
}

#[allow(dead_code)]
fn sql_like_matches(actual: &str, pattern: &str, case_insensitive: bool) -> bool {
    let actual_chars = if case_insensitive {
        actual.to_ascii_lowercase().chars().collect::<Vec<_>>()
    } else {
        actual.chars().collect::<Vec<_>>()
    };
    let pattern_chars = if case_insensitive {
        pattern.to_ascii_lowercase().chars().collect::<Vec<_>>()
    } else {
        pattern.chars().collect::<Vec<_>>()
    };

    let mut dp = vec![false; actual_chars.len() + 1];
    dp[0] = true;

    for pattern_char in pattern_chars {
        let mut next = vec![false; actual_chars.len() + 1];
        match pattern_char {
            '%' => {
                let mut seen = false;
                for index in 0..=actual_chars.len() {
                    seen |= dp[index];
                    next[index] = seen;
                }
            }
            '_' => {
                for index in 0..actual_chars.len() {
                    if dp[index] {
                        next[index + 1] = true;
                    }
                }
            }
            literal => {
                for index in 0..actual_chars.len() {
                    if dp[index] && actual_chars[index] == literal {
                        next[index + 1] = true;
                    }
                }
            }
        }
        dp = next;
    }

    dp[actual_chars.len()]
}

pub(crate) async fn execute_execution_program_in_committed_read_transaction(
    runtime_host: &dyn RuntimeHost,
    transaction: &mut dyn LixBackendTransaction,
    prepared: &PreparedCommittedReadProgram,
    allow_internal_tables: bool,
    context: &ExecutionContext,
) -> Result<ExecuteResult, LixError> {
    let mut results = Vec::new();

    for step in &prepared.steps {
        let compiled_on_demand = if step.compiled.is_some() {
            None
        } else {
            let runtime_state = context.execution_runtime_state().expect(
                "committed execution should install an execution runtime state before step compilation",
            );
            if runtime_state.settings().enabled && transaction.mode() == TransactionMode::Write {
                runtime_state
                    .ensure_sequence_initialized_in_transaction(runtime_host, transaction)
                    .await?;
            }

            let backend = TransactionBackendAdapter::new(transaction);
            Some(
                compile_bound_statement_template_instance_for_committed_read(
                    runtime_host,
                    &backend,
                    &step.bound_statement_template,
                    allow_internal_tables,
                    context,
                    runtime_state,
                )
                .await?,
            )
        };
        let compiled = step
            .compiled
            .as_ref()
            .or(compiled_on_demand.as_ref())
            .expect(
            "compiled committed read step should be available after eager or on-demand preparation",
        );

        let result = execute_compiled_committed_read_in_transaction(
            transaction,
            compiled,
            &step.source_statement,
        )
        .await?;
        results.push(result);
    }

    context
        .execution_runtime_state()
        .expect("committed execution should retain its runtime state until flush")
        .flush_in_transaction(runtime_host, transaction)
        .await?;

    Ok(ExecuteResult {
        statements: results,
    })
}

async fn execute_compiled_committed_read_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    compiled: &CompiledExecution,
    source_statement: &Statement,
) -> Result<QueryResult, LixError> {
    let parsed_statements = std::slice::from_ref(source_statement);
    if let Some(explain) = compiled.plain_explain() {
        return explain.render_query_result();
    }
    if let Some(public_read) = compiled.public_read() {
        let backend = TransactionBackendAdapter::new(transaction);
        let execution_started = Instant::now();
        return match execute_prepared_public_read(&backend, public_read).await {
            Ok(result) => {
                if let Some(explain) = compiled.analyzed_explain() {
                    explain.render_analyzed_query_result(&result, execution_started.elapsed())
                } else {
                    Ok(result)
                }
            }
            Err(error) => {
                Err(
                    normalize_sql_execution_error_with_backend(&backend, error, parsed_statements)
                        .await,
                )
            }
        };
    }
    if let Some(internal) = compiled.internal_execution() {
        let execution_started = Instant::now();
        let internal_result =
            execute_prepared_with_transaction(transaction, &internal.prepared_statements)
                .await
                .map_err(LixError::from);
        let internal_result = match internal_result {
            Ok(result) => result,
            Err(error) => {
                let backend = TransactionBackendAdapter::new(transaction);
                return Err(normalize_sql_execution_error_with_backend(
                    &backend,
                    error,
                    parsed_statements,
                )
                .await);
            }
        };
        let public_result = public_result_from_contract(compiled.result_contract, &internal_result);
        if let Some(explain) = compiled.analyzed_explain() {
            return explain
                .render_analyzed_query_result(&public_result, execution_started.elapsed());
        }
        return Ok(public_result);
    }
    Err(LixError::new(
        "LIX_ERROR_UNKNOWN",
        "committed read execution compiled a write-routed step unexpectedly",
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::execute_read_time_projection_read_with_backend;
    use crate::canonical::read::build_admin_version_source_sql_with_current_heads;
    use crate::contracts::artifacts::{
        PendingViewFilter, PendingViewOrderClause, PendingViewProjection, ReadTimeProjectionRead,
        ReadTimeProjectionReadQuery, ReadTimeProjectionSurface,
    };
    use crate::live_state;
    use crate::schema::builtin::types::LixCommit;
    use crate::test_support::{
        init_test_backend_core, seed_canonical_change_row, CanonicalChangeSeed, TestSqliteBackend,
    };
    use crate::version::{
        version_descriptor_file_id, version_descriptor_plugin_key, version_descriptor_schema_key,
        version_descriptor_schema_version, version_descriptor_snapshot_content,
        version_ref_file_id, version_ref_plugin_key, version_ref_schema_key,
        version_ref_schema_version, version_ref_snapshot_content,
    };
    use crate::{LixBackend, LixError, QueryResult, SqlDialect, TransactionMode, Value};

    #[derive(Debug, Clone)]
    struct VersionCaseDescriptor {
        id: &'static str,
        name: Option<&'static str>,
        hidden: bool,
        current_commit_id: Option<&'static str>,
    }

    #[tokio::test]
    async fn executes_lix_version_read_time_projection_query_like_current_admin_sql() {
        let backend = TestSqliteBackend::new();
        let current_heads = seed_version_projection_case(
            &backend,
            &[
                VersionCaseDescriptor {
                    id: "version-main",
                    name: Some(crate::version::DEFAULT_ACTIVE_VERSION_NAME),
                    hidden: false,
                    current_commit_id: Some("commit-main"),
                },
                VersionCaseDescriptor {
                    id: "version-hidden",
                    name: Some("main-hidden"),
                    hidden: true,
                    current_commit_id: Some("commit-hidden"),
                },
            ],
        )
        .await
        .expect("version projection case should seed");

        let artifact = ReadTimeProjectionRead {
            surface: ReadTimeProjectionSurface::LixVersion,
            query: ReadTimeProjectionReadQuery {
                projections: vec![
                    PendingViewProjection::Column {
                        source_column: "id".into(),
                        output_column: "id".into(),
                    },
                    PendingViewProjection::Column {
                        source_column: "name".into(),
                        output_column: "name".into(),
                    },
                    PendingViewProjection::Column {
                        source_column: "hidden".into(),
                        output_column: "hidden".into(),
                    },
                    PendingViewProjection::Column {
                        source_column: "commit_id".into(),
                        output_column: "commit_id".into(),
                    },
                ],
                filters: vec![PendingViewFilter::Like {
                    column: "name".into(),
                    pattern: "main%".into(),
                    case_insensitive: false,
                }],
                order_by: vec![PendingViewOrderClause {
                    column: "id".into(),
                    descending: false,
                }],
                limit: Some(1),
            },
        };

        let actual = execute_read_time_projection_read_with_backend(&backend, &artifact)
            .await
            .expect("read-time projection query should execute");
        let expected = current_admin_sql_query_result(
            &backend,
            &current_heads,
            "SELECT id, name, hidden, commit_id \
             FROM ({source_sql}) versions \
             WHERE name LIKE 'main%' \
             ORDER BY id \
             LIMIT 1",
        )
        .await
        .expect("current admin sql should execute");

        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn counts_lix_version_rows_through_read_time_projection_query() {
        let backend = TestSqliteBackend::new();
        let current_heads = seed_version_projection_case(
            &backend,
            &[
                VersionCaseDescriptor {
                    id: "version-main",
                    name: Some(crate::version::DEFAULT_ACTIVE_VERSION_NAME),
                    hidden: false,
                    current_commit_id: Some("commit-main"),
                },
                VersionCaseDescriptor {
                    id: "version-dev",
                    name: Some("dev"),
                    hidden: false,
                    current_commit_id: None,
                },
                VersionCaseDescriptor {
                    id: "version-hidden",
                    name: Some("hidden"),
                    hidden: true,
                    current_commit_id: Some("commit-hidden"),
                },
            ],
        )
        .await
        .expect("version projection case should seed");

        let artifact = ReadTimeProjectionRead {
            surface: ReadTimeProjectionSurface::LixVersion,
            query: ReadTimeProjectionReadQuery {
                projections: vec![PendingViewProjection::CountAll {
                    output_column: "count".into(),
                }],
                filters: vec![PendingViewFilter::IsNotNull("commit_id".into())],
                order_by: Vec::new(),
                limit: None,
            },
        };

        let actual = execute_read_time_projection_read_with_backend(&backend, &artifact)
            .await
            .expect("read-time projection count query should execute");
        let expected = current_admin_sql_query_result(
            &backend,
            &current_heads,
            "SELECT COUNT(*) AS count \
             FROM ({source_sql}) versions \
             WHERE commit_id IS NOT NULL",
        )
        .await
        .expect("current admin sql count should execute");

        assert_eq!(actual, expected);
    }

    async fn current_admin_sql_query_result(
        backend: &TestSqliteBackend,
        current_heads: &BTreeMap<String, String>,
        template: &str,
    ) -> Result<QueryResult, LixError> {
        let source_sql = build_admin_version_source_sql_with_current_heads(
            SqlDialect::Sqlite,
            Some(current_heads),
        );
        let sql = template.replace("{source_sql}", &source_sql);
        Ok(normalize_sqlite_version_hidden(
            backend.execute(&sql, &[]).await?,
        ))
    }

    fn normalize_sqlite_version_hidden(mut result: QueryResult) -> QueryResult {
        if result
            .columns
            .iter()
            .all(|column| !column.eq_ignore_ascii_case("hidden"))
        {
            return result;
        }

        let hidden_indexes = result
            .columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| column.eq_ignore_ascii_case("hidden").then_some(index))
            .collect::<Vec<_>>();

        for row in &mut result.rows {
            for hidden_index in &hidden_indexes {
                if let Some(value) = row.get_mut(*hidden_index) {
                    match value {
                        Value::Integer(0) => *value = Value::Boolean(false),
                        Value::Integer(1) => *value = Value::Boolean(true),
                        _ => {}
                    }
                }
            }
        }

        result
    }

    async fn seed_version_projection_case(
        backend: &TestSqliteBackend,
        descriptors: &[VersionCaseDescriptor],
    ) -> Result<BTreeMap<String, String>, LixError> {
        init_test_backend_core(backend).await?;
        live_state::register_schema(backend, version_descriptor_schema_key()).await?;
        live_state::register_schema(backend, version_ref_schema_key()).await?;

        let global_head_commit_id = "commit-global-head";
        let mut all_descriptors = vec![VersionCaseDescriptor {
            id: crate::version::GLOBAL_VERSION_ID,
            name: Some(crate::version::GLOBAL_VERSION_ID),
            hidden: true,
            current_commit_id: Some(global_head_commit_id),
        }];
        all_descriptors.extend(descriptors.iter().cloned());

        let mut transaction = backend.begin_transaction(TransactionMode::Write).await?;
        for (index, descriptor) in all_descriptors.iter().enumerate() {
            let timestamp = format!("2026-04-01T00:00:0{}Z", index);
            live_state::upsert_bootstrap_tracked_row_in_transaction(
                transaction.as_mut(),
                descriptor.id,
                version_descriptor_schema_key(),
                version_descriptor_schema_version(),
                version_descriptor_file_id(),
                crate::version::GLOBAL_VERSION_ID,
                version_descriptor_plugin_key(),
                &format!("change-{}", descriptor.id),
                &descriptor_snapshot_json(descriptor),
                &timestamp,
            )
            .await?;

            if let Some(commit_id) = descriptor.current_commit_id {
                live_state::upsert_bootstrap_untracked_row_in_transaction(
                    transaction.as_mut(),
                    descriptor.id,
                    version_ref_schema_key(),
                    version_ref_schema_version(),
                    version_ref_file_id(),
                    crate::version::GLOBAL_VERSION_ID,
                    version_ref_plugin_key(),
                    &version_ref_snapshot_content(descriptor.id, commit_id),
                    &format!("2026-04-01T00:00:1{}Z", index),
                )
                .await?;
            }
        }
        transaction.commit().await?;

        let mut current_heads = BTreeMap::from([(
            crate::version::GLOBAL_VERSION_ID.to_string(),
            global_head_commit_id.to_string(),
        )]);
        for descriptor in descriptors {
            if let Some(commit_id) = descriptor.current_commit_id {
                current_heads.insert(descriptor.id.to_string(), commit_id.to_string());
            }
        }

        let mut change_ids = Vec::new();
        for (index, descriptor) in all_descriptors.iter().enumerate() {
            let change_id = format!("change-{}", descriptor.id);
            let snapshot_id = format!("snapshot-{}", descriptor.id);
            change_ids.push(change_id.clone());
            let snapshot_content = descriptor_snapshot_json(descriptor);
            seed_canonical_change_row(
                backend,
                CanonicalChangeSeed {
                    id: &change_id,
                    entity_id: descriptor.id,
                    schema_key: version_descriptor_schema_key(),
                    schema_version: version_descriptor_schema_version(),
                    file_id: version_descriptor_file_id(),
                    plugin_key: version_descriptor_plugin_key(),
                    snapshot_id: &snapshot_id,
                    snapshot_content: Some(snapshot_content.as_str()),
                    metadata: None,
                    created_at: match index {
                        0 => "2026-04-01T01:00:00Z",
                        1 => "2026-04-01T01:00:01Z",
                        2 => "2026-04-01T01:00:02Z",
                        _ => "2026-04-01T01:00:03Z",
                    },
                },
            )
            .await?;
        }

        let commit_snapshot = serde_json::to_string(&LixCommit {
            id: global_head_commit_id.to_string(),
            change_set_id: Some("cs-global-head".to_string()),
            change_ids,
            author_account_ids: Vec::new(),
            parent_commit_ids: Vec::new(),
        })
        .expect("commit snapshot should serialize");
        seed_canonical_change_row(
            backend,
            CanonicalChangeSeed {
                id: "change-commit-global-head",
                entity_id: global_head_commit_id,
                schema_key: "lix_commit",
                schema_version: "1",
                file_id: "lix",
                plugin_key: "lix",
                snapshot_id: "snapshot-commit-global-head",
                snapshot_content: Some(commit_snapshot.as_str()),
                metadata: None,
                created_at: "2026-04-01T01:10:00Z",
            },
        )
        .await?;

        Ok(current_heads)
    }

    fn descriptor_snapshot_json(descriptor: &VersionCaseDescriptor) -> String {
        match descriptor.name {
            Some(name) => {
                version_descriptor_snapshot_content(descriptor.id, name, descriptor.hidden)
            }
            None => serde_json::json!({
                "id": descriptor.id,
                "hidden": descriptor.hidden,
            })
            .to_string(),
        }
    }
}
