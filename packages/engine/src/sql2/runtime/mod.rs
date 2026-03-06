use crate::sql2::catalog::SurfaceRegistry;
use crate::sql2::core::contracts::{BoundStatement, ExecutionContext};
use crate::sql2::planner::canonicalize::{canonicalize_read, CanonicalizedRead};
use crate::sql2::planner::semantics::dependency_spec::derive_dependency_spec_from_canonicalized_read;
use crate::sql2::planner::semantics::effective_state_resolver::{
    build_effective_state, EffectiveStatePlan, EffectiveStateRequest,
};
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
            lowered_sql: Vec::new(),
        },
        dependency_spec,
        effective_state_request: Some(effective_state_request),
        effective_state_plan: Some(effective_state_plan),
        canonicalized,
    })
}

#[cfg(test)]
mod tests {
    use super::prepare_sql2_read;
    use crate::{LixBackend, LixError, QueryResult, SqlDialect, Value};
    use async_trait::async_trait;
    use serde_json::json;
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use std::collections::HashMap;

    #[derive(Default)]
    struct FakeBackend {
        stored_schema_rows: HashMap<String, String>,
    }

    #[async_trait(?Send)]
    impl LixBackend for FakeBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("FROM lix_internal_state_materialized_v1_lix_stored_schema") {
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
}
