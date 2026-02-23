use std::collections::BTreeSet;

use sqlparser::ast::{Query, Statement};

use crate::sql::analysis::file_history_read_materialization_required_for_statements;
use crate::sql::entity_views::read as entity_view_read;
use crate::sql::steps::filesystem_step;
use crate::sql::steps::lix_state_history_view_read;
use crate::{LixBackend, LixError, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HistoryRequirements {
    pub requested_root_commit_ids: BTreeSet<String>,
    pub required_max_depth: i64,
    pub requires_file_history_data_materialization: bool,
}

impl Default for HistoryRequirements {
    fn default() -> Self {
        Self {
            requested_root_commit_ids: BTreeSet::new(),
            required_max_depth: 0,
            requires_file_history_data_materialization: false,
        }
    }
}

pub(crate) fn collect_history_requirements_for_statements(
    statements: &[Statement],
) -> HistoryRequirements {
    HistoryRequirements {
        requires_file_history_data_materialization:
            file_history_read_materialization_required_for_statements(statements),
        ..HistoryRequirements::default()
    }
}

pub(crate) async fn collect_history_requirements_for_statements_with_backend(
    backend: &dyn LixBackend,
    statements: &[Statement],
    params: &[Value],
) -> Result<HistoryRequirements, LixError> {
    let mut requirements = collect_history_requirements_for_statements(statements);
    for statement in statements {
        if let Some(query) = query_from_statement(statement) {
            let history_query =
                rewrite_query_for_history_requirements(backend, query, params).await?;
            let state_requirements =
                lix_state_history_view_read::collect_history_requirements_with_backend(
                    backend,
                    &history_query,
                    params,
                )
                .await?;
            for state_requirement in state_requirements {
                requirements.required_max_depth = requirements
                    .required_max_depth
                    .max(state_requirement.required_max_depth);
                requirements
                    .requested_root_commit_ids
                    .extend(state_requirement.requested_root_commit_ids);
            }
        }
    }
    Ok(requirements)
}

fn query_from_statement(statement: &Statement) -> Option<&Query> {
    match statement {
        Statement::Query(query) => Some(query.as_ref()),
        Statement::Insert(insert) => insert.source.as_deref(),
        _ => None,
    }
}

async fn rewrite_query_for_history_requirements(
    backend: &dyn LixBackend,
    query: &Query,
    params: &[Value],
) -> Result<Query, LixError> {
    let filesystem_rewritten = if params.is_empty() {
        filesystem_step::rewrite_query(query.clone())?
    } else {
        filesystem_step::rewrite_query_with_params(query.clone(), params)?
    };
    let mut current = filesystem_rewritten.unwrap_or_else(|| query.clone());
    if let Some(entity_rewritten) =
        entity_view_read::rewrite_query_with_backend(backend, current.clone()).await?
    {
        current = entity_rewritten;
    }
    Ok(current)
}

#[cfg(test)]
mod tests {
    use super::{
        collect_history_requirements_for_statements,
        collect_history_requirements_for_statements_with_backend,
    };
    use crate::backend::{LixBackend, LixTransaction, SqlDialect};
    use crate::sql::parse_sql_statements;
    use crate::{LixError, QueryResult, Value};
    use async_trait::async_trait;

    struct RequirementTestBackend {
        roots: Vec<String>,
    }

    struct RequirementTestTransaction;

    #[async_trait(?Send)]
    impl LixBackend for RequirementTestBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("SELECT DISTINCT c.id") && sql.contains("commit_by_version") {
                return Ok(QueryResult {
                    rows: self
                        .roots
                        .iter()
                        .map(|root| vec![Value::Text(root.clone())])
                        .collect(),
                });
            }
            Ok(QueryResult { rows: Vec::new() })
        }

        async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError> {
            Ok(Box::new(RequirementTestTransaction))
        }
    }

    #[async_trait(?Send)]
    impl LixTransaction for RequirementTestTransaction {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(
            &mut self,
            _sql: &str,
            _params: &[Value],
        ) -> Result<QueryResult, LixError> {
            Ok(QueryResult { rows: Vec::new() })
        }

        async fn commit(self: Box<Self>) -> Result<(), LixError> {
            Ok(())
        }

        async fn rollback(self: Box<Self>) -> Result<(), LixError> {
            Ok(())
        }
    }

    #[test]
    fn sync_collection_marks_file_history_requirement() {
        let statements = parse_sql_statements(
            "SELECT id FROM lix_file_history WHERE id = 'file-a' AND lixcol_depth = 0",
        )
        .expect("parse");
        let requirements = collect_history_requirements_for_statements(&statements);
        assert!(requirements.requires_file_history_data_materialization);
        assert!(requirements.requested_root_commit_ids.is_empty());
        assert_eq!(requirements.required_max_depth, 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn backend_collection_resolves_state_history_roots_and_depth() {
        let backend = RequirementTestBackend {
            roots: vec!["root-a".to_string(), "root-b".to_string()],
        };
        let statements = parse_sql_statements(
            "SELECT id \
             FROM lix_state_history \
             WHERE root_commit_id = 'root-a' \
                OR root_commit_id = 'root-b'",
        )
        .expect("parse");

        let requirements =
            collect_history_requirements_for_statements_with_backend(&backend, &statements, &[])
                .await
                .expect("collect requirements");

        assert_eq!(
            requirements
                .requested_root_commit_ids
                .into_iter()
                .collect::<Vec<_>>(),
            vec!["root-a".to_string(), "root-b".to_string()]
        );
        assert_eq!(requirements.required_max_depth, 512);
        assert!(!requirements.requires_file_history_data_materialization);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn backend_collection_resolves_file_history_roots_after_filesystem_rewrite() {
        let backend = RequirementTestBackend {
            roots: vec!["root-a".to_string()],
        };
        let statements = parse_sql_statements(
            "SELECT id \
             FROM lix_file_history \
             WHERE root_commit_id = 'root-a' \
               AND id = 'file-a'",
        )
        .expect("parse");

        let requirements =
            collect_history_requirements_for_statements_with_backend(&backend, &statements, &[])
                .await
                .expect("collect requirements");

        assert_eq!(
            requirements
                .requested_root_commit_ids
                .into_iter()
                .collect::<Vec<_>>(),
            vec!["root-a".to_string()]
        );
        assert_eq!(requirements.required_max_depth, 512);
        assert!(requirements.requires_file_history_data_materialization);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn backend_collection_resolves_entity_history_roots_after_entity_view_rewrite() {
        let backend = RequirementTestBackend {
            roots: vec!["root-a".to_string()],
        };
        let statements = parse_sql_statements(
            "SELECT key \
             FROM lix_key_value_history \
             WHERE key = 'history-key' \
               AND lixcol_root_commit_id = 'root-a'",
        )
        .expect("parse");

        let requirements =
            collect_history_requirements_for_statements_with_backend(&backend, &statements, &[])
                .await
                .expect("collect requirements");

        assert_eq!(
            requirements
                .requested_root_commit_ids
                .into_iter()
                .collect::<Vec<_>>(),
            vec!["root-a".to_string()]
        );
        assert_eq!(requirements.required_max_depth, 512);
        assert!(!requirements.requires_file_history_data_materialization);
    }
}
