use crate::sql2::backend::PushdownDecision;
use crate::sql2::catalog::SurfaceRegistry;
use crate::sql2::core::contracts::{BoundStatement, ExecutionContext};
use crate::sql2::planner::backend::lowerer::{lower_read_for_execution, LoweredReadProgram};
use crate::sql2::planner::canonicalize::{
    canonicalize_read, canonicalize_write, CanonicalizedRead, CanonicalizedWrite,
};
use crate::sql2::planner::ir::{
    CommitPreconditions, PlannedWrite, ResolvedWritePlan, SchemaProof, ScopeProof, TargetSetProof,
    WriteCommand,
};
use crate::sql2::planner::semantics::dependency_spec::derive_dependency_spec_from_canonicalized_read;
use crate::sql2::planner::semantics::domain_changes::{
    build_domain_change_batch, derive_commit_preconditions, DomainChangeBatch,
};
use crate::sql2::planner::semantics::effective_state_resolver::{
    build_effective_state, EffectiveStatePlan, EffectiveStateRequest,
};
use crate::sql2::planner::semantics::proof_engine::prove_write;
use crate::sql2::planner::semantics::write_resolver::resolve_write_plan;
use crate::sql_shared::dependency_spec::DependencySpec;
use crate::{LixBackend, Value};
use sqlparser::ast::Statement;

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct Sql2DebugTrace {
    pub(crate) bound_statements: Vec<BoundStatement>,
    pub(crate) surface_bindings: Vec<String>,
    pub(crate) dependency_spec: Option<DependencySpec>,
    pub(crate) effective_state_request: Option<EffectiveStateRequest>,
    pub(crate) effective_state_plan: Option<EffectiveStatePlan>,
    pub(crate) pushdown_decision: Option<PushdownDecision>,
    pub(crate) write_command: Option<WriteCommand>,
    pub(crate) scope_proof: Option<ScopeProof>,
    pub(crate) schema_proof: Option<SchemaProof>,
    pub(crate) target_set_proof: Option<TargetSetProof>,
    pub(crate) resolved_write_plan: Option<ResolvedWritePlan>,
    pub(crate) domain_change_batch: Option<DomainChangeBatch>,
    pub(crate) commit_preconditions: Option<CommitPreconditions>,
    pub(crate) invariant_trace: Option<Sql2InvariantTrace>,
    pub(crate) write_phase_trace: Vec<String>,
    pub(crate) lowered_sql: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Sql2InvariantTrace {
    pub(crate) batch_local_checks: Vec<String>,
    pub(crate) append_time_checks: Vec<String>,
    pub(crate) physical_checks: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Sql2PreparedRead {
    pub(crate) canonicalized: CanonicalizedRead,
    pub(crate) dependency_spec: Option<DependencySpec>,
    pub(crate) effective_state_request: Option<EffectiveStateRequest>,
    pub(crate) effective_state_plan: Option<EffectiveStatePlan>,
    pub(crate) lowered_read: Option<LoweredReadProgram>,
    pub(crate) debug_trace: Sql2DebugTrace,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Sql2PreparedWrite {
    pub(crate) canonicalized: CanonicalizedWrite,
    pub(crate) planned_write: PlannedWrite,
    pub(crate) domain_change_batch: Option<DomainChangeBatch>,
    pub(crate) debug_trace: Sql2DebugTrace,
}

pub(crate) async fn prepare_sql2_read(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
) -> Option<Sql2PreparedRead> {
    if parsed_statements.len() != 1 {
        return None;
    }

    let registry = SurfaceRegistry::bootstrap_with_backend(backend)
        .await
        .ok()?;
    let statement = parsed_statements[0].clone();
    let bound_statement = BoundStatement::from_statement(
        statement,
        params.to_vec(),
        ExecutionContext {
            dialect: Some(backend.dialect()),
            writer_key: writer_key.map(ToString::to_string),
            requested_version_id: Some(active_version_id.to_string()),
        },
    );
    let canonicalized = canonicalize_read(bound_statement.clone(), &registry).ok()?;
    let dependency_spec = derive_dependency_spec_from_canonicalized_read(&canonicalized);
    let (effective_state_request, effective_state_plan) =
        build_effective_state(&canonicalized, dependency_spec.as_ref())?;
    let lowered_read = lower_read_for_execution(
        &canonicalized,
        &effective_state_request,
        &effective_state_plan,
    )
    .ok()
    .flatten();
    let lowered_sql = lowered_read
        .as_ref()
        .map(|program| {
            program
                .statements
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    Some(Sql2PreparedRead {
        debug_trace: Sql2DebugTrace {
            bound_statements: vec![bound_statement],
            surface_bindings: vec![canonicalized.surface_binding.descriptor.public_name.clone()],
            dependency_spec: dependency_spec.clone(),
            effective_state_request: Some(effective_state_request.clone()),
            effective_state_plan: Some(effective_state_plan.clone()),
            pushdown_decision: lowered_read
                .as_ref()
                .map(|program| program.pushdown_decision.clone()),
            write_command: None,
            scope_proof: None,
            schema_proof: None,
            target_set_proof: None,
            resolved_write_plan: None,
            domain_change_batch: None,
            commit_preconditions: None,
            invariant_trace: None,
            write_phase_trace: Vec::new(),
            lowered_sql,
        },
        dependency_spec,
        effective_state_request: Some(effective_state_request),
        effective_state_plan: Some(effective_state_plan),
        lowered_read,
        canonicalized,
    })
}

pub(crate) async fn prepare_sql2_write(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
) -> Option<Sql2PreparedWrite> {
    if parsed_statements.len() != 1 {
        return None;
    }

    let registry = SurfaceRegistry::bootstrap_with_backend(backend)
        .await
        .ok()?;
    let statement = parsed_statements[0].clone();
    let bound_statement = BoundStatement::from_statement(
        statement,
        params.to_vec(),
        ExecutionContext {
            dialect: Some(backend.dialect()),
            writer_key: writer_key.map(ToString::to_string),
            requested_version_id: Some(active_version_id.to_string()),
        },
    );
    let canonicalized = canonicalize_write(bound_statement.clone(), &registry).ok()?;
    let mut planned_write = prove_write(&canonicalized).ok()?;
    let resolved_write_plan = resolve_write_plan(backend, &planned_write).await.ok()?;
    planned_write.resolved_write_plan = Some(resolved_write_plan.clone());
    let domain_change_batch = build_domain_change_batch(&planned_write).ok()?;
    let commit_preconditions = derive_commit_preconditions(backend, &planned_write)
        .await
        .ok()?;
    planned_write.commit_preconditions = commit_preconditions.clone();
    let invariant_trace = Some(build_sql2_invariant_trace(&planned_write));

    Some(Sql2PreparedWrite {
        debug_trace: Sql2DebugTrace {
            bound_statements: vec![bound_statement],
            surface_bindings: vec![canonicalized.surface_binding.descriptor.public_name.clone()],
            dependency_spec: None,
            effective_state_request: None,
            effective_state_plan: None,
            pushdown_decision: None,
            write_command: Some(canonicalized.write_command.clone()),
            scope_proof: Some(planned_write.scope_proof.clone()),
            schema_proof: Some(planned_write.schema_proof.clone()),
            target_set_proof: planned_write.target_set_proof.clone(),
            resolved_write_plan: Some(resolved_write_plan),
            domain_change_batch: domain_change_batch.clone(),
            commit_preconditions: commit_preconditions.clone(),
            invariant_trace,
            write_phase_trace: sql2_write_phase_trace(),
            lowered_sql: Vec::new(),
        },
        planned_write,
        domain_change_batch,
        canonicalized,
    })
}

fn sql2_write_phase_trace() -> Vec<String> {
    vec![
        "canonicalize_write".to_string(),
        "prove_write".to_string(),
        "resolve_authoritative_pre_state".to_string(),
        "build_domain_change_batch".to_string(),
        "derive_commit_preconditions".to_string(),
        "validate_batch_local_write".to_string(),
        "append_time_invariant_recheck".to_string(),
        "append_commit_if_preconditions_hold".to_string(),
    ]
}

fn build_sql2_invariant_trace(planned_write: &PlannedWrite) -> Sql2InvariantTrace {
    let mut batch_local_checks = Vec::new();
    let mut append_time_checks = vec![
        "write_lane.tip_precondition".to_string(),
        "idempotency_key.recheck".to_string(),
    ];
    let mut physical_checks = Vec::new();

    if planned_write.command.operation_kind == crate::sql2::planner::ir::WriteOperationKind::Update
    {
        append_time_checks.push("schema_mutability.recheck".to_string());
    }

    if let Some(resolved) = planned_write.resolved_write_plan.as_ref() {
        let mut saw_snapshot_validation = false;
        let mut saw_primary_key_consistency = false;
        let mut saw_stored_schema_definition = false;
        let mut saw_stored_schema_bootstrap_identity = false;

        for row in &resolved.intended_post_state {
            if row.tombstone {
                continue;
            }

            if !saw_snapshot_validation {
                batch_local_checks.push("snapshot_content.schema_validation".to_string());
                saw_snapshot_validation = true;
            }
            if !saw_primary_key_consistency {
                batch_local_checks.push("entity_id.primary_key_consistency".to_string());
                saw_primary_key_consistency = true;
            }
            if row.schema_key == "lix_stored_schema" {
                if !saw_stored_schema_definition {
                    batch_local_checks.push("stored_schema.definition_validation".to_string());
                    saw_stored_schema_definition = true;
                }
                if !saw_stored_schema_bootstrap_identity {
                    batch_local_checks.push("stored_schema.bootstrap_identity".to_string());
                    saw_stored_schema_bootstrap_identity = true;
                }
            }
        }
    }

    if !planned_write
        .command
        .mode
        .eq(&crate::sql2::planner::ir::WriteMode::Untracked)
    {
        physical_checks.push("backend_constraints.defense_in_depth".to_string());
    }

    Sql2InvariantTrace {
        batch_local_checks,
        append_time_checks,
        physical_checks,
    }
}

#[cfg(test)]
mod tests {
    use super::{prepare_sql2_read, prepare_sql2_write};
    use crate::sql2::catalog::SurfaceRegistry;
    use crate::sql2::core::contracts::{BoundStatement, ExecutionContext};
    use crate::sql2::planner::canonicalize::canonicalize_write;
    use crate::sql2::planner::ir::{ExpectedTip, ScopeProof, WriteLane, WriteMode};
    use crate::sql2::planner::semantics::domain_changes::{
        build_domain_change_batch, derive_commit_preconditions,
    };
    use crate::sql2::planner::semantics::proof_engine::prove_write;
    use crate::sql2::planner::semantics::write_resolver::resolve_write_plan;
    use crate::{LixBackend, LixError, QueryResult, SqlDialect, Value};
    use async_trait::async_trait;
    use serde_json::{json, to_string};
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use std::collections::HashMap;

    #[derive(Default)]
    struct FakeBackend {
        stored_schema_rows: HashMap<String, String>,
        version_pointer_rows: HashMap<String, String>,
        state_rows: HashMap<String, Vec<Vec<Value>>>,
    }

    #[async_trait(?Send)]
    impl LixBackend for FakeBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("FROM lix_internal_stored_schema_bootstrap") {
                let rows = self
                    .stored_schema_rows
                    .iter()
                    .map(|(schema_key, snapshot)| {
                        if sql.contains("SELECT schema_version, snapshot_content") {
                            let schema_version =
                                serde_json::from_str::<serde_json::Value>(snapshot)
                                    .ok()
                                    .and_then(|value| {
                                        value
                                            .get("value")
                                            .and_then(|value| value.get("x-lix-version"))
                                            .and_then(serde_json::Value::as_str)
                                            .map(ToString::to_string)
                                    })
                                    .unwrap_or_else(|| "1".to_string());
                            vec![Value::Text(schema_version), Value::Text(snapshot.clone())]
                        } else if sql.contains("substr(entity_id, 1,") {
                            if sql.contains(schema_key) {
                                vec![Value::Text(snapshot.clone())]
                            } else {
                                Vec::new()
                            }
                        } else {
                            vec![Value::Text(snapshot.clone())]
                        }
                    })
                    .filter(|row| !row.is_empty())
                    .collect::<Vec<_>>();
                return Ok(QueryResult {
                    rows,
                    columns: if sql.contains("SELECT schema_version, snapshot_content") {
                        vec!["schema_version".to_string(), "snapshot_content".to_string()]
                    } else {
                        vec!["snapshot_content".to_string()]
                    },
                });
            }
            if sql.contains("FROM lix_internal_change c")
                && sql.contains("c.schema_key = 'lix_version_pointer'")
            {
                let rows = self
                    .version_pointer_rows
                    .iter()
                    .filter(|(version_id, _)| {
                        sql.contains(&format!("c.entity_id = '{}'", version_id))
                            || sql.contains(&format!("'{}'", version_id))
                    })
                    .map(|(version_id, snapshot)| {
                        if sql.contains("SELECT c.entity_id, s.content AS snapshot_content") {
                            vec![
                                Value::Text(version_id.clone()),
                                Value::Text(snapshot.clone()),
                            ]
                        } else {
                            vec![Value::Text(snapshot.clone())]
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
            if sql.contains("FROM \"lix_internal_state_materialized_v1_") {
                if let Some((_table_name, rows)) = self
                    .state_rows
                    .iter()
                    .find(|(table_name, _)| sql.contains(table_name.as_str()))
                {
                    let entity_filter = extract_sql_string_filter(sql, "entity_id");
                    let version_filter = extract_sql_string_filter(sql, "version_id");
                    let filtered_rows = rows
                        .iter()
                        .filter(|row| {
                            let entity_matches = match entity_filter.as_ref() {
                                Some(entity_id) => {
                                    matches!(row.first(), Some(Value::Text(value)) if value == entity_id)
                                }
                                None => true,
                            };
                            let version_matches = match version_filter.as_ref() {
                                Some(version_id) => {
                                    matches!(row.get(4), Some(Value::Text(value)) if value == version_id)
                                }
                                None => true,
                            };
                            entity_matches && version_matches
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    return Ok(QueryResult {
                        rows: filtered_rows,
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
            }
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn begin_transaction(&self) -> Result<Box<dyn crate::LixTransaction + '_>, LixError> {
            Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "transactions are not needed in this test backend".to_string(),
            })
        }
    }

    fn parse_one(sql: &str) -> Vec<Statement> {
        Parser::parse_sql(&GenericDialect {}, sql).expect("SQL should parse")
    }

    fn extract_sql_string_filter(sql: &str, column: &str) -> Option<String> {
        let marker = format!("{column} = '");
        let start = sql.find(&marker)? + marker.len();
        let rest = &sql[start..];
        let end = rest.find('\'')?;
        Some(rest[..end].to_string())
    }

    #[tokio::test]
    async fn prepares_builtin_schema_derived_entity_reads() {
        let backend = FakeBackend::default();
        let prepared = prepare_sql2_read(
            &backend,
            &parse_one("SELECT key, value FROM lix_key_value WHERE key = 'hello'"),
            &[],
            "main",
            None,
        )
        .await
        .expect("builtin entity read should canonicalize");

        assert_eq!(prepared.debug_trace.surface_bindings, vec!["lix_key_value"]);
        assert_eq!(
            prepared
                .dependency_spec
                .expect("dependency spec should be derived")
                .schema_keys
                .into_iter()
                .collect::<Vec<_>>(),
            vec![
                "lix_active_version".to_string(),
                "lix_key_value".to_string()
            ]
        );
        assert_eq!(
            prepared
                .effective_state_request
                .expect("effective-state request should be built")
                .schema_set
                .into_iter()
                .collect::<Vec<_>>(),
            vec!["lix_key_value".to_string()]
        );
        assert_eq!(
            prepared
                .debug_trace
                .pushdown_decision
                .as_ref()
                .expect("pushdown decision should be recorded")
                .residual_predicates,
            vec!["key = 'hello'".to_string()]
        );
        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("live sql2 entity read should lower");
        assert!(lowered_sql.contains("FROM (SELECT"));
        assert_eq!(
            extract_sql_string_filter(lowered_sql, "schema_key").as_deref(),
            Some("lix_key_value")
        );
        assert_eq!(
            extract_sql_string_filter(lowered_sql, "file_id").as_deref(),
            Some("lix")
        );
        assert_eq!(
            extract_sql_string_filter(lowered_sql, "plugin_key").as_deref(),
            Some("lix")
        );
    }

    #[tokio::test]
    async fn prepares_stored_schema_derived_entity_reads() {
        let mut backend = FakeBackend::default();
        backend.stored_schema_rows.insert(
            "message".to_string(),
            json!({
                "value": {
                    "x-lix-key": "message",
                    "x-lix-version": "1",
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" },
                        "body": { "type": "string" }
                    }
                }
            })
            .to_string(),
        );

        let prepared = prepare_sql2_read(
            &backend,
            &parse_one("SELECT body FROM message WHERE id = 'm1'"),
            &[],
            "main",
            None,
        )
        .await
        .expect("stored-schema entity read should canonicalize");

        assert_eq!(prepared.debug_trace.surface_bindings, vec!["message"]);
        assert_eq!(
            prepared
                .canonicalized
                .surface_binding
                .implicit_overrides
                .fixed_schema_key
                .as_deref(),
            Some("message")
        );
        assert!(prepared.dependency_spec.is_some());
        assert!(prepared.effective_state_plan.is_some());
        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("stored-schema entity read should lower");
        assert!(lowered_sql.contains("FROM (SELECT"));
        assert_eq!(
            extract_sql_string_filter(lowered_sql, "schema_key").as_deref(),
            Some("message")
        );
    }

    #[tokio::test]
    async fn returns_none_for_unsupported_day_one_query_shapes() {
        let backend = FakeBackend::default();
        let prepared = prepare_sql2_read(
            &backend,
            &parse_one(
                "SELECT * FROM lix_state s JOIN lix_state_by_version b ON s.entity_id = b.entity_id",
            ),
            &[],
            "main",
            None,
        )
        .await;

        assert!(prepared.is_none());
    }

    #[tokio::test]
    async fn prepares_state_reads_with_explicit_residual_pushdown_trace() {
        let backend = FakeBackend::default();
        let prepared = prepare_sql2_read(
            &backend,
            &parse_one("SELECT entity_id FROM lix_state WHERE schema_key = 'lix_key_value'"),
            &[],
            "main",
            None,
        )
        .await
        .expect("state read should canonicalize");

        assert_eq!(
            prepared
                .debug_trace
                .pushdown_decision
                .as_ref()
                .expect("pushdown decision should be recorded")
                .accepted_predicates,
            vec!["schema_key = 'lix_key_value'".to_string()]
        );
        assert_eq!(
            prepared
                .debug_trace
                .pushdown_decision
                .as_ref()
                .expect("pushdown decision should be recorded")
                .residual_predicates,
            Vec::<String>::new()
        );
        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("state read should lower");
        assert!(lowered_sql
            .contains("FROM (SELECT * FROM lix_state WHERE schema_key = 'lix_key_value')"));
    }

    #[tokio::test]
    async fn prepares_state_by_version_reads_with_version_pushdown_trace() {
        let backend = FakeBackend::default();
        let prepared = prepare_sql2_read(
            &backend,
            &parse_one(
                "SELECT entity_id FROM lix_state_by_version \
                 WHERE version_id = 'v1' AND schema_key = 'lix_key_value'",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("state-by-version read should canonicalize");

        assert_eq!(
            prepared
                .debug_trace
                .pushdown_decision
                .as_ref()
                .expect("pushdown decision should be recorded")
                .accepted_predicates,
            vec![
                "version_id = 'v1'".to_string(),
                "schema_key = 'lix_key_value'".to_string()
            ]
        );
        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("state-by-version read should lower");
        assert!(lowered_sql.contains(
            "FROM (SELECT * FROM lix_state_by_version WHERE version_id = 'v1' AND schema_key = 'lix_key_value')"
        ));
    }

    #[tokio::test]
    async fn prepares_state_history_reads_with_root_commit_pushdown_trace() {
        let backend = FakeBackend::default();
        let prepared = prepare_sql2_read(
            &backend,
            &parse_one(
                "SELECT snapshot_content, root_commit_id, depth \
                 FROM lix_state_history \
                 WHERE entity_id = 'entity1' AND root_commit_id = 'commit-1' \
                 ORDER BY depth ASC",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("state-history read should canonicalize");

        assert_eq!(
            prepared
                .debug_trace
                .pushdown_decision
                .as_ref()
                .expect("pushdown decision should be recorded")
                .accepted_predicates,
            vec![
                "entity_id = 'entity1'".to_string(),
                "root_commit_id = 'commit-1'".to_string()
            ]
        );
        let lowered_sql = prepared
            .debug_trace
            .lowered_sql
            .first()
            .expect("state-history read should lower");
        assert!(lowered_sql.contains(
            "FROM (SELECT * FROM lix_state_history WHERE entity_id = 'entity1' AND root_commit_id = 'commit-1')"
        ));
    }

    #[tokio::test]
    async fn returns_none_for_nested_subqueries_that_stay_on_legacy_path() {
        let backend = FakeBackend::default();
        let prepared = prepare_sql2_read(
            &backend,
            &parse_one(
                "SELECT entity_id FROM lix_state WHERE entity_id IN (SELECT entity_id FROM lix_state_by_version)",
            ),
            &[],
            "main",
            None,
        )
        .await;

        assert!(prepared.is_none());
    }

    #[tokio::test]
    async fn prepares_state_by_version_inserts_into_planned_writes() {
        let mut backend = FakeBackend::default();
        backend.version_pointer_rows.insert(
            "version-a".to_string(),
            to_string(&crate::builtin_schema::types::LixVersionPointer {
                id: "version-a".to_string(),
                commit_id: "commit-123".to_string(),
            })
            .expect("version pointer JSON"),
        );
        let prepared = prepare_sql2_write(
            &backend,
            &parse_one(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'entity-1', 'lix_key_value', 'lix', 'version-a', 'lix', '{\"key\":\"hello\"}', '1'\
                 )",
            ),
            &[],
            "main",
            Some("writer-a"),
        )
        .await
        .expect("state insert should prepare through sql2");

        assert_eq!(
            prepared.debug_trace.surface_bindings,
            vec!["lix_state_by_version"]
        );
        assert_eq!(
            prepared.planned_write.scope_proof,
            ScopeProof::SingleVersion("version-a".to_string())
        );
        assert_eq!(prepared.planned_write.command.mode, WriteMode::Tracked);
        assert_eq!(
            prepared
                .planned_write
                .commit_preconditions
                .as_ref()
                .expect("tracked write should include commit preconditions")
                .expected_tip,
            ExpectedTip::CommitId("commit-123".to_string())
        );
        assert_eq!(
            prepared
                .planned_write
                .resolved_write_plan
                .as_ref()
                .expect("resolved write plan should exist")
                .target_write_lane,
            Some(WriteLane::SingleVersion("version-a".to_string()))
        );
        assert_eq!(
            prepared
                .domain_change_batch
                .as_ref()
                .expect("tracked write should include a domain change batch")
                .write_lane,
            WriteLane::SingleVersion("version-a".to_string())
        );
        assert_eq!(
            prepared
                .debug_trace
                .invariant_trace
                .as_ref()
                .expect("write debug trace should include invariant checks")
                .batch_local_checks,
            vec![
                "snapshot_content.schema_validation".to_string(),
                "entity_id.primary_key_consistency".to_string()
            ]
        );
        assert_eq!(
            prepared.debug_trace.write_phase_trace,
            vec![
                "canonicalize_write".to_string(),
                "prove_write".to_string(),
                "resolve_authoritative_pre_state".to_string(),
                "build_domain_change_batch".to_string(),
                "derive_commit_preconditions".to_string(),
                "validate_batch_local_write".to_string(),
                "append_time_invariant_recheck".to_string(),
                "append_commit_if_preconditions_hold".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn prepares_active_version_state_inserts_with_active_lane() {
        let mut backend = FakeBackend::default();
        backend.version_pointer_rows.insert(
            "main".to_string(),
            to_string(&crate::builtin_schema::types::LixVersionPointer {
                id: "main".to_string(),
                commit_id: "commit-main".to_string(),
            })
            .expect("version pointer JSON"),
        );
        let prepared = prepare_sql2_write(
            &backend,
            &parse_one(
                "INSERT INTO lix_state (\
                 entity_id, schema_key, file_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'entity-1', 'lix_key_value', 'lix', 'lix', '{\"key\":\"hello\"}', '1'\
                 )",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("active-version state insert should prepare through sql2");

        assert_eq!(
            prepared.planned_write.scope_proof,
            ScopeProof::ActiveVersion
        );
        assert_eq!(
            prepared
                .planned_write
                .resolved_write_plan
                .as_ref()
                .expect("resolved write plan should exist")
                .target_write_lane,
            Some(WriteLane::ActiveVersion)
        );
        assert_eq!(
            prepared
                .planned_write
                .commit_preconditions
                .as_ref()
                .expect("tracked write should include commit preconditions")
                .expected_tip,
            ExpectedTip::CommitId("commit-main".to_string())
        );
    }

    #[tokio::test]
    async fn prepares_stored_schema_invariant_trace_for_sql2_writes() {
        let mut backend = FakeBackend::default();
        backend.version_pointer_rows.insert(
            "global".to_string(),
            to_string(&crate::builtin_schema::types::LixVersionPointer {
                id: "global".to_string(),
                commit_id: "commit-global".to_string(),
            })
            .expect("version pointer JSON"),
        );

        let prepared = prepare_sql2_write(
            &backend,
            &parse_one(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'schema-a~1', 'lix_stored_schema', 'lix', 'global', 'lix', '{\"value\":{\"x-lix-key\":\"schema-a\",\"x-lix-version\":\"1\",\"type\":\"object\"}}', '1'\
                 )",
            ),
            &[],
            "main",
            None,
        )
        .await
        .expect("stored schema write should prepare through sql2");

        let invariant_trace = prepared
            .debug_trace
            .invariant_trace
            .as_ref()
            .expect("stored schema write should expose invariant trace");
        assert!(invariant_trace
            .batch_local_checks
            .contains(&"stored_schema.definition_validation".to_string()));
        assert!(invariant_trace
            .batch_local_checks
            .contains(&"stored_schema.bootstrap_identity".to_string()));
        assert!(invariant_trace
            .append_time_checks
            .contains(&"write_lane.tip_precondition".to_string()));
        assert_eq!(
            invariant_trace.physical_checks,
            vec!["backend_constraints.defense_in_depth".to_string()]
        );
    }

    #[tokio::test]
    async fn prepares_builtin_entity_inserts_into_tracked_write_artifacts() {
        let mut backend = FakeBackend::default();
        backend.version_pointer_rows.insert(
            "main".to_string(),
            to_string(&crate::builtin_schema::types::LixVersionPointer {
                id: "main".to_string(),
                commit_id: "commit-main".to_string(),
            })
            .expect("version pointer JSON"),
        );
        let registry = SurfaceRegistry::bootstrap_with_backend(&backend)
            .await
            .expect("registry should bootstrap");
        let bound = BoundStatement::from_statement(
            parse_one("INSERT INTO lix_key_value (key, value) VALUES ('k', 'v')")
                .into_iter()
                .next()
                .expect("single statement"),
            Vec::new(),
            ExecutionContext {
                dialect: Some(SqlDialect::Sqlite),
                requested_version_id: Some("main".to_string()),
                ..ExecutionContext::default()
            },
        );
        let canonicalized =
            canonicalize_write(bound, &registry).expect("entity insert should canonicalize");
        let mut planned_write = prove_write(&canonicalized).expect("entity insert should prove");
        let resolved_write_plan = resolve_write_plan(&backend, &planned_write)
            .await
            .expect("entity insert should resolve");
        planned_write.resolved_write_plan = Some(resolved_write_plan);
        let _ = build_domain_change_batch(&planned_write)
            .expect("domain-change batch should build")
            .expect("tracked entity insert should produce a batch");
        let _ = derive_commit_preconditions(&backend, &planned_write)
            .await
            .expect("commit preconditions should derive")
            .expect("tracked entity insert should produce commit preconditions");

        let prepared = prepare_sql2_write(
            &backend,
            &parse_one("INSERT INTO lix_key_value (key, value) VALUES ('k', 'v')"),
            &[],
            "main",
            None,
        )
        .await
        .expect("builtin entity insert should prepare through sql2");

        assert_eq!(
            prepared.debug_trace.surface_bindings,
            vec!["lix_key_value".to_string()]
        );
        assert_eq!(
            prepared
                .planned_write
                .resolved_write_plan
                .as_ref()
                .expect("resolved write plan should exist")
                .intended_post_state[0]
                .values
                .get("snapshot_content"),
            Some(&Value::Text("{\"key\":\"k\",\"value\":\"v\"}".to_string()))
        );
    }

    #[tokio::test]
    async fn returns_none_for_entity_writes_that_need_legacy_global_override_handling() {
        let mut backend = FakeBackend::default();
        backend.version_pointer_rows.insert(
            "main".to_string(),
            to_string(&crate::builtin_schema::types::LixVersionPointer {
                id: "main".to_string(),
                commit_id: "commit-main".to_string(),
            })
            .expect("version pointer JSON"),
        );
        backend.stored_schema_rows.insert(
            "message".to_string(),
            json!({
                "value": {
                    "x-lix-key": "message",
                    "x-lix-version": "1",
                    "x-lix-primary-key": ["/id"],
                    "x-lix-override-lixcols": {
                        "lixcol_file_id": "\"lix\"",
                        "lixcol_plugin_key": "\"lix\"",
                        "lixcol_global": "true"
                    },
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" },
                        "body": { "type": "string" }
                    }
                }
            })
            .to_string(),
        );

        let prepared = prepare_sql2_write(
            &backend,
            &parse_one("INSERT INTO message (id, body) VALUES ('m1', 'hello')"),
            &[],
            "main",
            None,
        )
        .await;

        assert!(prepared.is_none());
    }

    #[tokio::test]
    async fn prepares_state_by_version_updates_into_tracked_write_artifacts() {
        let mut backend = FakeBackend::default();
        backend.version_pointer_rows.insert(
            "version-a".to_string(),
            to_string(&crate::builtin_schema::types::LixVersionPointer {
                id: "version-a".to_string(),
                commit_id: "commit-456".to_string(),
            })
            .expect("version pointer JSON"),
        );
        backend.state_rows.insert(
            "lix_internal_state_materialized_v1_lix_key_value".to_string(),
            vec![vec![
                Value::Text("entity-1".to_string()),
                Value::Text("lix_key_value".to_string()),
                Value::Text("1".to_string()),
                Value::Text("lix".to_string()),
                Value::Text("version-a".to_string()),
                Value::Text("lix".to_string()),
                Value::Text("{\"value\":\"before\"}".to_string()),
                Value::Text("{\"m\":1}".to_string()),
                Value::Text("change-1".to_string()),
            ]],
        );
        let prepared = prepare_sql2_write(
            &backend,
            &parse_one(
                "UPDATE lix_state_by_version \
                 SET snapshot_content = '{\"value\":\"after\"}' \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'entity-1' \
                   AND version_id = 'version-a'",
            ),
            &[],
            "main",
            Some("writer-a"),
        )
        .await
        .expect("state update should prepare through sql2");

        assert_eq!(
            prepared.planned_write.scope_proof,
            ScopeProof::SingleVersion("version-a".to_string())
        );
        assert_eq!(
            prepared
                .planned_write
                .resolved_write_plan
                .as_ref()
                .expect("resolved write plan should exist")
                .target_write_lane,
            Some(WriteLane::SingleVersion("version-a".to_string()))
        );
        assert_eq!(
            prepared
                .planned_write
                .resolved_write_plan
                .as_ref()
                .expect("resolved write plan should exist")
                .intended_post_state[0]
                .values
                .get("file_id"),
            Some(&Value::Text("lix".to_string()))
        );
        assert_eq!(
            prepared
                .planned_write
                .commit_preconditions
                .as_ref()
                .expect("tracked write should include commit preconditions")
                .expected_tip,
            ExpectedTip::CommitId("commit-456".to_string())
        );
    }

    #[tokio::test]
    async fn prepares_state_by_version_deletes_into_tracked_write_artifacts() {
        let mut backend = FakeBackend::default();
        backend.version_pointer_rows.insert(
            "version-a".to_string(),
            to_string(&crate::builtin_schema::types::LixVersionPointer {
                id: "version-a".to_string(),
                commit_id: "commit-789".to_string(),
            })
            .expect("version pointer JSON"),
        );
        backend.state_rows.insert(
            "lix_internal_state_materialized_v1_lix_key_value".to_string(),
            vec![vec![
                Value::Text("entity-1".to_string()),
                Value::Text("lix_key_value".to_string()),
                Value::Text("1".to_string()),
                Value::Text("lix".to_string()),
                Value::Text("version-a".to_string()),
                Value::Text("lix".to_string()),
                Value::Text("{\"value\":\"before\"}".to_string()),
                Value::Null,
                Value::Text("change-1".to_string()),
            ]],
        );
        let prepared = prepare_sql2_write(
            &backend,
            &parse_one(
                "DELETE FROM lix_state_by_version \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'entity-1' \
                   AND version_id = 'version-a'",
            ),
            &[],
            "main",
            Some("writer-a"),
        )
        .await
        .expect("state delete should prepare through sql2");

        assert_eq!(
            prepared.planned_write.scope_proof,
            ScopeProof::SingleVersion("version-a".to_string())
        );
        assert_eq!(
            prepared
                .planned_write
                .resolved_write_plan
                .as_ref()
                .expect("resolved write plan should exist")
                .intended_post_state[0]
                .tombstone,
            true
        );
        assert_eq!(
            prepared
                .planned_write
                .resolved_write_plan
                .as_ref()
                .expect("resolved write plan should exist")
                .tombstones
                .len(),
            1
        );
        assert_eq!(
            prepared
                .planned_write
                .commit_preconditions
                .as_ref()
                .expect("tracked write should include commit preconditions")
                .expected_tip,
            ExpectedTip::CommitId("commit-789".to_string())
        );
    }
}
