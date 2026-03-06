use crate::sql2::catalog::SurfaceRegistry;
use crate::sql2::core::contracts::{BoundStatement, ExecutionContext};
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
    pub(crate) write_command: Option<WriteCommand>,
    pub(crate) scope_proof: Option<ScopeProof>,
    pub(crate) schema_proof: Option<SchemaProof>,
    pub(crate) target_set_proof: Option<TargetSetProof>,
    pub(crate) resolved_write_plan: Option<ResolvedWritePlan>,
    pub(crate) domain_change_batch: Option<DomainChangeBatch>,
    pub(crate) commit_preconditions: Option<CommitPreconditions>,
    pub(crate) lowered_sql: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Sql2PreparedRead {
    pub(crate) canonicalized: CanonicalizedRead,
    pub(crate) dependency_spec: Option<DependencySpec>,
    pub(crate) effective_state_request: Option<EffectiveStateRequest>,
    pub(crate) effective_state_plan: Option<EffectiveStatePlan>,
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

    Some(Sql2PreparedRead {
        debug_trace: Sql2DebugTrace {
            bound_statements: vec![bound_statement],
            surface_bindings: vec![canonicalized.surface_binding.descriptor.public_name.clone()],
            dependency_spec: dependency_spec.clone(),
            effective_state_request: Some(effective_state_request.clone()),
            effective_state_plan: Some(effective_state_plan.clone()),
            write_command: None,
            scope_proof: None,
            schema_proof: None,
            target_set_proof: None,
            resolved_write_plan: None,
            domain_change_batch: None,
            commit_preconditions: None,
            lowered_sql: Vec::new(),
        },
        dependency_spec,
        effective_state_request: Some(effective_state_request),
        effective_state_plan: Some(effective_state_plan),
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
    let resolved_write_plan = resolve_write_plan(&planned_write).ok()?;
    planned_write.resolved_write_plan = Some(resolved_write_plan.clone());
    let domain_change_batch = build_domain_change_batch(&planned_write).ok()?;
    let commit_preconditions = derive_commit_preconditions(backend, &planned_write)
        .await
        .ok()?;
    planned_write.commit_preconditions = commit_preconditions.clone();

    Some(Sql2PreparedWrite {
        debug_trace: Sql2DebugTrace {
            bound_statements: vec![bound_statement],
            surface_bindings: vec![canonicalized.surface_binding.descriptor.public_name.clone()],
            dependency_spec: None,
            effective_state_request: None,
            effective_state_plan: None,
            write_command: Some(canonicalized.write_command.clone()),
            scope_proof: Some(planned_write.scope_proof.clone()),
            schema_proof: Some(planned_write.schema_proof.clone()),
            target_set_proof: planned_write.target_set_proof.clone(),
            resolved_write_plan: Some(resolved_write_plan),
            domain_change_batch: domain_change_batch.clone(),
            commit_preconditions: commit_preconditions.clone(),
            lowered_sql: Vec::new(),
        },
        planned_write,
        domain_change_batch,
        canonicalized,
    })
}

#[cfg(test)]
mod tests {
    use super::{prepare_sql2_read, prepare_sql2_write};
    use crate::sql2::planner::ir::{ExpectedTip, ScopeProof, WriteLane, WriteMode};
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
    }

    #[async_trait(?Send)]
    impl LixBackend for FakeBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("FROM lix_internal_stored_schema_bootstrap") {
                return Ok(QueryResult {
                    rows: self
                        .stored_schema_rows
                        .values()
                        .cloned()
                        .map(|snapshot| vec![Value::Text(snapshot)])
                        .collect(),
                    columns: vec!["snapshot_content".to_string()],
                });
            }
            if sql.contains("FROM lix_internal_state_materialized_v1_lix_version_pointer") {
                let rows = self
                    .version_pointer_rows
                    .iter()
                    .filter(|(version_id, _)| {
                        sql.contains(&format!("entity_id = '{}'", version_id))
                    })
                    .map(|(_, snapshot)| vec![Value::Text(snapshot.clone())])
                    .collect::<Vec<_>>();
                return Ok(QueryResult {
                    rows,
                    columns: vec!["snapshot_content".to_string()],
                });
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
    async fn returns_none_for_entity_writes_that_stay_on_legacy_path() {
        let backend = FakeBackend::default();
        let prepared = prepare_sql2_write(
            &backend,
            &parse_one("INSERT INTO lix_key_value (key, value) VALUES ('k', 'v')"),
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
                .commit_preconditions
                .as_ref()
                .expect("tracked write should include commit preconditions")
                .expected_tip,
            ExpectedTip::CommitId("commit-789".to_string())
        );
    }
}
