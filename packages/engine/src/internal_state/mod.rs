pub(crate) mod stored_schema;
pub(crate) mod vtable_read;
pub(crate) mod vtable_write;
pub(crate) mod materialize;
pub(crate) mod script;
#[path = "canonical_write.rs"]
mod canonical;
pub(crate) mod followup;
pub(crate) mod postprocess;

use crate::functions::LixFunctionProvider;
use crate::engine::sql::contracts::planned_statement::PlannedStatementSet;
use crate::sql_shared::ast::parse_sql_statements;
use crate::{LixBackend, LixError, Value};
use sqlparser::ast::{ObjectNamePart, Query, Statement, TableFactor, Visit, Visitor};
use std::collections::BTreeSet;
use std::ops::ControlFlow;

pub(crate) use crate::engine::sql::ast::walk::object_name_matches;
pub(crate) use crate::engine::sql::ast::utils::PlaceholderState;
pub(crate) use crate::engine::sql::ast::utils::{
    resolve_expr_cell_with_state, ResolvedCell, RowSourceResolver,
};
pub(crate) type SchemaRegistration =
    crate::engine::sql::contracts::planned_statement::SchemaRegistration;
pub(crate) type MutationOperation =
    crate::engine::sql::contracts::planned_statement::MutationOperation;
pub(crate) type MutationRow = crate::engine::sql::contracts::planned_statement::MutationRow;
pub(crate) type UpdateValidationPlan =
    crate::engine::sql::contracts::planned_statement::UpdateValidationPlan;
pub(crate) type PreparedStatement =
    crate::engine::sql::contracts::prepared_statement::PreparedStatement;
pub(crate) use postprocess::{PostprocessPlan, VtableDeletePlan, VtableUpdatePlan};

#[derive(Debug, Clone)]
pub(crate) struct InternalStatePlan {
    pub(crate) postprocess: Option<PostprocessPlan>,
}

#[derive(Debug, Clone)]
pub(crate) struct RewriteOutput {
    pub(crate) statements: Vec<Statement>,
    pub(crate) effect_only: bool,
    pub(crate) params: Vec<Value>,
    pub(crate) registrations: Vec<SchemaRegistration>,
    pub(crate) postprocess: Option<PostprocessPlan>,
    pub(crate) mutations: Vec<MutationRow>,
    pub(crate) update_validations: Vec<UpdateValidationPlan>,
}

#[derive(Debug, Clone)]
pub(crate) struct PreprocessOutput {
    pub(crate) sql: String,
    pub(crate) prepared_statements: Vec<PreparedStatement>,
    pub(crate) registrations: Vec<SchemaRegistration>,
    pub(crate) internal_state: Option<InternalStatePlan>,
    pub(crate) mutations: Vec<MutationRow>,
    pub(crate) update_validations: Vec<UpdateValidationPlan>,
}

impl From<PreprocessOutput> for PlannedStatementSet {
    fn from(output: PreprocessOutput) -> Self {
        Self {
            sql: output.sql,
            prepared_statements: output.prepared_statements,
            registrations: output.registrations,
            internal_state: output.internal_state,
            mutations: output.mutations,
            update_validations: output.update_validations,
        }
    }
}

pub(crate) fn internal_state_plan_from_postprocess(
    postprocess: Option<PostprocessPlan>,
) -> Option<InternalStatePlan> {
    postprocess.map(|postprocess| InternalStatePlan {
        postprocess: Some(postprocess),
    })
}

pub(crate) fn parse_single_query(sql: &str) -> Result<sqlparser::ast::Query, LixError> {
    let mut statements = parse_sql_statements(sql)?;
    if statements.len() != 1 {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "expected a single SELECT statement".to_string(),
        });
    }
    match statements.remove(0) {
        Statement::Query(query) => Ok(*query),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "expected SELECT statement".to_string(),
        }),
    }
}

pub(crate) fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

pub(crate) fn rewrite_internal_state_query_read(
    query: Query,
    params: &[Value],
) -> Result<Query, LixError> {
    let original = query.clone();
    Ok(vtable_read::rewrite_query(query, params)?.unwrap_or(original))
}

pub(crate) async fn rewrite_internal_state_query_read_with_backend(
    backend: &dyn LixBackend,
    query: Query,
    params: &[Value],
) -> Result<Query, LixError> {
    let original = query.clone();
    Ok(vtable_read::rewrite_query_with_backend(backend, query, params)
        .await?
        .unwrap_or(original))
}

pub(crate) fn statement_references_internal_state_vtable(statement: &Statement) -> bool {
    match statement {
        Statement::Query(query) => collect_query_relation_names(query).contains("lix_internal_state_vtable"),
        Statement::Explain { statement, .. } => {
            statement_references_internal_state_vtable(statement)
        }
        _ => false,
    }
}

pub(crate) fn requires_single_statement_postprocess(plan: Option<&PostprocessPlan>) -> bool {
    matches!(plan, Some(PostprocessPlan::VtableDelete(_)))
}

pub(crate) fn requires_single_statement_internal_state_plan(
    plan: Option<&InternalStatePlan>,
) -> bool {
    requires_single_statement_postprocess(plan.and_then(|plan| plan.postprocess.as_ref()))
}

pub(crate) fn validate_internal_state_plan(plan: Option<&InternalStatePlan>) -> Result<(), LixError> {
    let Some(plan) = plan else {
        return Ok(());
    };
    let Some(postprocess) = plan.postprocess.as_ref() else {
        return Ok(());
    };
    let schema_key = match postprocess {
        PostprocessPlan::VtableUpdate(update) => &update.schema_key,
        PostprocessPlan::VtableDelete(delete) => &delete.schema_key,
    };
    if !schema_key.trim().is_empty()
        && !schema_key.contains(char::is_whitespace)
        && !schema_key.contains('\'')
    {
        return Ok(());
    }
    Err(LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: "vtable postprocess plan requires a valid schema_key".to_string(),
    })
}

fn collect_query_relation_names(query: &Query) -> BTreeSet<String> {
    struct Collector {
        relation_names: BTreeSet<String>,
    }

    impl Visitor for Collector {
        type Break = ();

        fn pre_visit_table_factor(
            &mut self,
            table_factor: &TableFactor,
        ) -> ControlFlow<Self::Break> {
            if let TableFactor::Table { name, .. } = table_factor {
                let relation_name = name
                    .0
                    .iter()
                    .map(|part| match part {
                        ObjectNamePart::Identifier(identifier) => identifier.value.clone(),
                        ObjectNamePart::Function(function) => function.to_string(),
                    })
                    .collect::<Vec<_>>()
                    .join(".");
                self.relation_names.insert(relation_name);
            }
            ControlFlow::Continue(())
        }
    }

    let mut collector = Collector {
        relation_names: BTreeSet::new(),
    };
    let _ = query.visit(&mut collector);
    collector.relation_names
}

pub(crate) fn rewrite_statement<P: LixFunctionProvider>(
    statement: Statement,
    params: &[Value],
    writer_key: Option<&str>,
    provider: &mut P,
) -> Result<RewriteOutput, LixError> {
    let output = if let Some(output) =
        canonical::rewrite_sync_statement(statement.clone(), params, writer_key, provider)?
    {
        output
    } else {
        passthrough_output(statement)
    };
    validate_statement_output(&output)?;
    Ok(output)
}

pub(crate) async fn rewrite_statement_with_backend<P>(
    backend: &dyn LixBackend,
    statement: Statement,
    params: &[Value],
    writer_key: Option<&str>,
    provider: &mut P,
) -> Result<RewriteOutput, LixError>
where
    P: LixFunctionProvider + Clone + Send + 'static,
{
    let output = if let Some(output) =
        canonical::rewrite_backend_statement(
            backend,
            statement.clone(),
            params,
            writer_key,
            provider,
        )
        .await?
    {
        output
    } else {
        passthrough_output(statement)
    };
    validate_statement_output(&output)?;
    Ok(output)
}

fn passthrough_output(statement: Statement) -> RewriteOutput {
    RewriteOutput {
        statements: vec![statement],
        effect_only: false,
        params: Vec::new(),
        registrations: Vec::new(),
        postprocess: None,
        mutations: Vec::new(),
        update_validations: Vec::new(),
    }
}

fn validate_statement_output(output: &RewriteOutput) -> Result<(), LixError> {
    if output.statements.is_empty()
        && !(output.effect_only
            && output.postprocess.is_none()
            && output.mutations.is_empty()
            && output.update_validations.is_empty())
    {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "statement rewrite produced no statements".to_string(),
        });
    }
    if requires_single_statement_postprocess(output.postprocess.as_ref())
        && output.statements.len() != 1
    {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "postprocess rewrites require a single statement".to_string(),
        });
    }
    if output.postprocess.is_some() && !output.mutations.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "postprocess rewrites cannot emit mutation rows".to_string(),
        });
    }
    if !output.mutations.is_empty() && !output.update_validations.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "mutation rewrites cannot emit update validations".to_string(),
        });
    }
    if !output.update_validations.is_empty()
        && !output
            .statements
            .iter()
            .all(|statement| matches!(statement, sqlparser::ast::Statement::Update(_)))
    {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "update validations require an UPDATE statement output".to_string(),
        });
    }
    if let Some(postprocess) = &output.postprocess {
        match postprocess {
            PostprocessPlan::VtableUpdate(_) => {
                if !matches!(
                    output.statements.last(),
                    Some(sqlparser::ast::Statement::Update(_))
                ) {
                    return Err(LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        description: "vtable update postprocess requires an UPDATE statement"
                            .to_string(),
                    });
                }
            }
            PostprocessPlan::VtableDelete(_) => {
                if !matches!(
                    output.statements[0],
                    sqlparser::ast::Statement::Update(_) | sqlparser::ast::Statement::Delete(_)
                ) {
                    return Err(LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        description:
                            "vtable delete postprocess requires an UPDATE or DELETE statement"
                                .to_string(),
                    });
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use super::*;

    fn empty_statement() -> Statement {
        let mut statements = Parser::parse_sql(&GenericDialect {}, "SELECT 1").expect("parse SQL");
        statements.remove(0)
    }

    fn empty_update_statement() -> Statement {
        let mut statements = Parser::parse_sql(
            &GenericDialect {},
            "UPDATE lix_internal_state_vtable SET updated_at = updated_at WHERE schema_key = 'schema'",
        )
        .expect("parse SQL");
        statements.remove(0)
    }

    #[test]
    fn statement_validator_rejects_postprocess_with_mutations() {
        let output = RewriteOutput {
            statements: vec![empty_statement()],
            effect_only: false,
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: Some(PostprocessPlan::VtableDelete(VtableDeletePlan {
                schema_key: "schema".to_string(),
                effective_scope_fallback: false,
                effective_scope_selection_sql: None,
                effective_scope_untracked_selection_sql: None,
            })),
            mutations: vec![MutationRow {
                operation: MutationOperation::Insert,
                entity_id: "e".to_string(),
                schema_key: "s".to_string(),
                schema_version: "1".to_string(),
                file_id: "f".to_string(),
                version_id: "v".to_string(),
                plugin_key: "p".to_string(),
                snapshot_content: None,
                untracked: false,
            }],
            update_validations: Vec::new(),
        };

        let err = validate_statement_output(&output)
            .expect_err("postprocess output with mutations should be rejected");
        assert!(err.description.contains("cannot emit mutation rows"));
    }

    #[test]
    fn statement_validator_rejects_update_validation_with_non_update_statement() {
        let output = RewriteOutput {
            statements: vec![empty_statement(), empty_update_statement()],
            effect_only: false,
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: None,
            mutations: Vec::new(),
            update_validations: vec![UpdateValidationPlan {
                table: "t".to_string(),
                where_clause: None,
                snapshot_content: None,
                snapshot_patch: None,
            }],
        };

        let err = validate_statement_output(&output)
            .expect_err("update validations with non-update statement should be rejected");
        assert!(err
            .description
            .contains("update validations require an UPDATE statement output"));
    }

    #[test]
    fn statement_validator_rejects_update_validation_on_non_update_statement() {
        let output = RewriteOutput {
            statements: vec![empty_statement()],
            effect_only: false,
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: None,
            mutations: Vec::new(),
            update_validations: vec![UpdateValidationPlan {
                table: "t".to_string(),
                where_clause: None,
                snapshot_content: None,
                snapshot_patch: None,
            }],
        };

        let err = validate_statement_output(&output)
            .expect_err("update validation on query statement should be rejected");
        assert!(err
            .description
            .contains("update validations require an UPDATE statement output"));
    }

    #[test]
    fn statement_validator_rejects_vtable_update_postprocess_on_non_update_statement() {
        let output = RewriteOutput {
            statements: vec![empty_statement()],
            effect_only: false,
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: Some(PostprocessPlan::VtableUpdate(VtableUpdatePlan {
                schema_key: "schema".to_string(),
                explicit_writer_key: None,
                writer_key_assignment_present: false,
            })),
            mutations: Vec::new(),
            update_validations: Vec::new(),
        };

        let err = validate_statement_output(&output)
            .expect_err("vtable update postprocess on query statement should be rejected");
        assert!(err
            .description
            .contains("vtable update postprocess requires an UPDATE statement"));
    }

    #[test]
    fn statement_validator_allows_multi_statement_vtable_update_postprocess() {
        let output = RewriteOutput {
            statements: vec![empty_update_statement(), empty_update_statement()],
            effect_only: false,
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: Some(PostprocessPlan::VtableUpdate(VtableUpdatePlan {
                schema_key: "schema".to_string(),
                explicit_writer_key: None,
                writer_key_assignment_present: false,
            })),
            mutations: Vec::new(),
            update_validations: Vec::new(),
        };

        validate_statement_output(&output)
            .expect("multi-statement vtable update postprocess should remain valid");
    }
}
