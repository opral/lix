use std::collections::BTreeMap;

use sqlparser::ast::{Query, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::cel::CelEvaluator;
use crate::{LixBackend, LixError, Value};

pub(crate) mod directory_history;
pub(crate) mod file_history;
pub(crate) mod predicates;
pub(crate) mod state_history;

#[derive(Debug, Default, Clone)]
pub(crate) struct ReadRewriteSession {
    version_chain_cache: BTreeMap<String, Vec<String>>,
}

impl ReadRewriteSession {
    pub(crate) fn cached_version_chain(&self, version_id: &str) -> Option<&[String]> {
        self.version_chain_cache.get(version_id).map(Vec::as_slice)
    }

    pub(crate) fn cache_version_chain(&mut self, version_id: String, chain: Vec<String>) {
        self.version_chain_cache.insert(version_id, chain);
    }
}

pub(crate) async fn rewrite_read_query_with_backend_and_params_in_session(
    backend: &dyn LixBackend,
    query: Query,
    params: &[Value],
    _session: &mut ReadRewriteSession,
) -> Result<Query, LixError> {
    let query_sql = query.to_string();
    let planned = super::super::planning::preprocess::preprocess_sql_to_plan(
        backend,
        &CelEvaluator::new(),
        &query_sql,
        params,
    )
    .await?;
    if planned.prepared_statements.len() != 1 {
        return Err(LixError {
            message: format!(
                "read rewrite helper expected one prepared statement, got {}",
                planned.prepared_statements.len()
            ),
        });
    }

    let rewritten_sql = &planned.prepared_statements[0].sql;
    let mut statements = Parser::parse_sql(&GenericDialect {}, rewritten_sql).map_err(|error| {
        LixError {
            message: format!("failed to parse rewritten helper SQL: {error}"),
        }
    })?;
    if statements.len() != 1 {
        return Err(LixError {
            message: "read rewrite helper expected a single statement after rewrite".to_string(),
        });
    }

    match statements.remove(0) {
        Statement::Query(query) => Ok(*query),
        _ => Err(LixError {
            message: "read rewrite helper expected a query statement after rewrite".to_string(),
        }),
    }
}
