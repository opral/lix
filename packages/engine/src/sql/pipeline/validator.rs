use std::collections::BTreeSet;
use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Expr, JoinConstraint, JoinOperator, ObjectName, Query, Select, TableFactor,
    TableWithJoins, Value as AstValue, ValueWithSpan, Visit, Visitor,
};

use crate::sql::types::RewriteOutput;
use crate::sql::PostprocessPlan;
use crate::sql::{object_name_matches, visit_query_selects, visit_table_factors_in_select};
use crate::LixError;

use super::context::AnalysisContext;
use super::registry::RewritePhase;

const LOGICAL_READ_VIEW_NAMES: &[&str] = &[
    "lix_active_account",
    "lix_active_version",
    "lix_state",
    "lix_state_by_version",
    "lix_state_history",
    "lix_version",
    "lix_file",
    "lix_file_by_version",
    "lix_file_history",
    "lix_directory",
    "lix_directory_by_version",
    "lix_directory_history",
];
const MATERIALIZED_STATE_TABLE_PREFIX: &str = "lix_internal_state_materialized_v1_";

pub(crate) fn validate_final_read_query(query: &Query) -> Result<(), LixError> {
    validate_no_unresolved_logical_read_views(query)?;
    validate_unique_explicit_relation_aliases(query)?;
    validate_materialized_state_live_filters(query)
}

pub(crate) fn validate_phase_invariants(
    phase: RewritePhase,
    query: &Query,
    _context: &AnalysisContext,
) -> Result<(), LixError> {
    match phase {
        RewritePhase::Analyze => Ok(()),
        RewritePhase::Canonicalize => {
            validate_no_unresolved_logical_read_views(query)?;
            validate_unique_explicit_relation_aliases(query)?;
            validate_materialized_state_live_filters(query)
        }
        RewritePhase::Optimize => {
            validate_no_unresolved_logical_read_views(query)?;
            validate_unique_explicit_relation_aliases(query)?;
            validate_materialized_state_live_filters(query)
        }
        // Lower can expand SQL substantially; final invariant check covers output.
        RewritePhase::Lower => {
            validate_no_unresolved_logical_read_views(query)?;
            validate_unique_explicit_relation_aliases(query)?;
            validate_materialized_state_live_filters(query)
        }
    }
}

pub(crate) fn validate_statement_output(output: &RewriteOutput) -> Result<(), LixError> {
    if output.statements.is_empty() {
        return Err(LixError {
            message: "statement rewrite produced no statements".to_string(),
        });
    }
    if output.postprocess.is_some() && output.statements.len() != 1 {
        return Err(LixError {
            message: "postprocess rewrites require a single statement".to_string(),
        });
    }
    if output.postprocess.is_some() && !output.mutations.is_empty() {
        return Err(LixError {
            message: "postprocess rewrites cannot emit mutation rows".to_string(),
        });
    }
    if !output.update_validations.is_empty() && output.statements.len() != 1 {
        return Err(LixError {
            message: "update validation rewrites require a single statement".to_string(),
        });
    }
    if !output.mutations.is_empty() && !output.update_validations.is_empty() {
        return Err(LixError {
            message: "mutation rewrites cannot emit update validations".to_string(),
        });
    }
    if !output.update_validations.is_empty()
        && !matches!(output.statements[0], sqlparser::ast::Statement::Update(_))
    {
        return Err(LixError {
            message: "update validations require an UPDATE statement output".to_string(),
        });
    }
    for registration in &output.registrations {
        if registration.schema_key.trim().is_empty() {
            return Err(LixError {
                message: "schema registration cannot have an empty schema_key".to_string(),
            });
        }
    }
    for mutation in &output.mutations {
        validate_non_empty_field("mutation entity_id", &mutation.entity_id)?;
        validate_non_empty_field("mutation schema_key", &mutation.schema_key)?;
        validate_non_empty_field("mutation schema_version", &mutation.schema_version)?;
        validate_non_empty_field("mutation file_id", &mutation.file_id)?;
        validate_non_empty_field("mutation version_id", &mutation.version_id)?;
        validate_non_empty_field("mutation plugin_key", &mutation.plugin_key)?;
    }
    for validation in &output.update_validations {
        validate_non_empty_field("update validation table", &validation.table)?;
        if validation.snapshot_content.is_some() && validation.snapshot_patch.is_some() {
            return Err(LixError {
                message:
                    "update validations cannot define both snapshot_content and snapshot_patch"
                        .to_string(),
            });
        }
    }
    if let Some(postprocess) = &output.postprocess {
        match postprocess {
            PostprocessPlan::VtableUpdate(plan) => {
                validate_non_empty_field("vtable update schema_key", &plan.schema_key)?;
                if !matches!(output.statements[0], sqlparser::ast::Statement::Update(_)) {
                    return Err(LixError {
                        message: "vtable update postprocess requires an UPDATE statement"
                            .to_string(),
                    });
                }
            }
            PostprocessPlan::VtableDelete(plan) => {
                validate_non_empty_field("vtable delete schema_key", &plan.schema_key)?;
                if !plan.effective_scope_fallback && plan.effective_scope_selection_sql.is_some() {
                    return Err(LixError {
                        message: "vtable delete postprocess cannot emit effective scope selection SQL without fallback"
                            .to_string(),
                    });
                }
                if !matches!(
                    output.statements[0],
                    sqlparser::ast::Statement::Update(_) | sqlparser::ast::Statement::Delete(_)
                ) {
                    return Err(LixError {
                        message: "vtable delete postprocess requires an UPDATE or DELETE statement"
                            .to_string(),
                    });
                }
            }
        }
    }
    Ok(())
}

fn validate_non_empty_field(field: &str, value: &str) -> Result<(), LixError> {
    if value.trim().is_empty() {
        return Err(LixError {
            message: format!("{field} cannot be empty"),
        });
    }
    Ok(())
}

pub(crate) fn validate_no_unresolved_logical_read_views(query: &Query) -> Result<(), LixError> {
    validate_no_unresolved_logical_read_views_except(query, &[])
}

fn validate_unique_explicit_relation_aliases(query: &Query) -> Result<(), LixError> {
    visit_query_selects(query, &mut |select| {
        let mut aliases = BTreeSet::new();
        visit_table_factors_in_select(select, &mut |relation| {
            let alias = match relation {
                sqlparser::ast::TableFactor::Table {
                    alias: Some(alias), ..
                }
                | sqlparser::ast::TableFactor::Derived {
                    alias: Some(alias), ..
                } => alias.name.value.to_ascii_lowercase(),
                _ => return Ok(()),
            };
            if !aliases.insert(alias.clone()) {
                return Err(LixError {
                    message: format!(
                        "read rewrite produced duplicate explicit relation alias '{alias}'"
                    ),
                });
            }
            Ok(())
        })
    })
}

fn validate_materialized_state_live_filters(query: &Query) -> Result<(), LixError> {
    visit_query_selects(query, &mut |select| {
        let materialized_relations = collect_materialized_relations(select);
        if materialized_relations.is_empty() {
            return Ok(());
        }

        let predicates = collect_select_predicates(select);
        for relation in &materialized_relations {
            let allow_unqualified = materialized_relations.len() == 1;
            let has_live_filter = predicates.iter().any(|predicate| {
                expr_contains_live_tombstone_filter(predicate, Some(&relation.qualifier))
                    || (allow_unqualified && expr_contains_live_tombstone_filter(predicate, None))
            });
            if !has_live_filter {
                return Err(LixError {
                    message: format!(
                        "read rewrite produced materialized relation '{}' without live-row tombstone filter",
                        relation.display_name
                    ),
                });
            }
        }

        Ok(())
    })
}

#[derive(Debug)]
struct MaterializedRelation {
    display_name: String,
    qualifier: String,
}

fn collect_materialized_relations(select: &Select) -> Vec<MaterializedRelation> {
    let mut relations = Vec::new();
    for table in &select.from {
        collect_materialized_relations_from_table_with_joins(table, &mut relations);
    }
    relations
}

fn collect_materialized_relations_from_table_with_joins(
    table: &TableWithJoins,
    relations: &mut Vec<MaterializedRelation>,
) {
    collect_materialized_relations_from_table_factor(&table.relation, relations);
    for join in &table.joins {
        collect_materialized_relations_from_table_factor(&join.relation, relations);
    }
}

fn collect_materialized_relations_from_table_factor(
    table: &TableFactor,
    relations: &mut Vec<MaterializedRelation>,
) {
    let TableFactor::Table { name, alias, .. } = table else {
        return;
    };
    let Some(base_name) = object_name_last_identifier(name) else {
        return;
    };
    if !base_name
        .to_ascii_lowercase()
        .starts_with(MATERIALIZED_STATE_TABLE_PREFIX)
    {
        return;
    }

    let qualifier = alias
        .as_ref()
        .map(|alias| alias.name.value.clone())
        .unwrap_or_else(|| base_name.clone());
    relations.push(MaterializedRelation {
        display_name: base_name,
        qualifier,
    });
}

fn object_name_last_identifier(name: &ObjectName) -> Option<String> {
    let last = name.0.last()?;
    match last {
        sqlparser::ast::ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
        _ => Some(last.to_string()),
    }
}

fn collect_select_predicates(select: &Select) -> Vec<&Expr> {
    let mut predicates = Vec::new();
    if let Some(selection) = &select.selection {
        predicates.push(selection);
    }
    for table in &select.from {
        for join in &table.joins {
            collect_join_operator_predicates(&join.join_operator, &mut predicates);
        }
    }
    predicates
}

fn collect_join_operator_predicates<'a>(
    operator: &'a JoinOperator,
    predicates: &mut Vec<&'a Expr>,
) {
    match operator {
        JoinOperator::AsOf {
            match_condition,
            constraint,
        } => {
            predicates.push(match_condition);
            if let JoinConstraint::On(expr) = constraint {
                predicates.push(expr);
            }
        }
        JoinOperator::Join(constraint)
        | JoinOperator::Inner(constraint)
        | JoinOperator::Left(constraint)
        | JoinOperator::LeftOuter(constraint)
        | JoinOperator::Right(constraint)
        | JoinOperator::RightOuter(constraint)
        | JoinOperator::FullOuter(constraint)
        | JoinOperator::CrossJoin(constraint)
        | JoinOperator::Semi(constraint)
        | JoinOperator::LeftSemi(constraint)
        | JoinOperator::RightSemi(constraint)
        | JoinOperator::Anti(constraint)
        | JoinOperator::LeftAnti(constraint)
        | JoinOperator::RightAnti(constraint)
        | JoinOperator::StraightJoin(constraint) => {
            if let JoinConstraint::On(expr) = constraint {
                predicates.push(expr);
            }
        }
        JoinOperator::CrossApply | JoinOperator::OuterApply => {}
    }
}

fn expr_contains_live_tombstone_filter(expr: &Expr, qualifier: Option<&str>) -> bool {
    struct LiveTombstoneFilterVisitor<'a> {
        qualifier: Option<&'a str>,
        found: bool,
    }

    impl Visitor for LiveTombstoneFilterVisitor<'_> {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            if self.found {
                return ControlFlow::Break(());
            }
            if let Expr::BinaryOp { left, op, right } = expr {
                if *op == BinaryOperator::Eq
                    && ((expr_is_tombstone_column(left, self.qualifier)
                        && expr_is_numeric_zero(right))
                        || (expr_is_tombstone_column(right, self.qualifier)
                            && expr_is_numeric_zero(left)))
                {
                    self.found = true;
                    return ControlFlow::Break(());
                }
            }
            ControlFlow::Continue(())
        }
    }

    let mut visitor = LiveTombstoneFilterVisitor {
        qualifier,
        found: false,
    };
    let _ = expr.visit(&mut visitor);
    visitor.found
}

fn expr_is_tombstone_column(expr: &Expr, qualifier: Option<&str>) -> bool {
    match expr {
        Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case("is_tombstone"),
        Expr::CompoundIdentifier(identifiers) => {
            let Some(last) = identifiers.last() else {
                return false;
            };
            if !last.value.eq_ignore_ascii_case("is_tombstone") {
                return false;
            }
            let Some(qualifier) = qualifier else {
                return true;
            };
            identifiers.len() < 2
                || identifiers[identifiers.len() - 2]
                    .value
                    .eq_ignore_ascii_case(qualifier)
        }
        _ => false,
    }
}

fn expr_is_numeric_zero(expr: &Expr) -> bool {
    match expr {
        Expr::Value(ValueWithSpan {
            value: AstValue::Number(number, _),
            ..
        }) => number.parse::<i64>().ok() == Some(0),
        Expr::Cast { expr, .. } => expr_is_numeric_zero(expr),
        _ => false,
    }
}

pub(crate) fn validate_no_unresolved_logical_read_views_except(
    query: &Query,
    allowed: &[&str],
) -> Result<(), LixError> {
    let allowed: BTreeSet<&str> = allowed.iter().copied().collect();
    let mut unresolved = BTreeSet::new();
    visit_query_selects(query, &mut |select| {
        visit_table_factors_in_select(select, &mut |relation| {
            let sqlparser::ast::TableFactor::Table { name, .. } = relation else {
                return Ok(());
            };
            for candidate in LOGICAL_READ_VIEW_NAMES {
                if object_name_matches(name, candidate) {
                    if allowed.contains(candidate) {
                        continue;
                    }
                    unresolved.insert((*candidate).to_string());
                }
            }
            Ok(())
        })
    })?;

    if unresolved.is_empty() {
        return Ok(());
    }

    Err(LixError {
        message: format!(
            "read rewrite left unresolved logical views: {}",
            unresolved.into_iter().collect::<Vec<_>>().join(", ")
        ),
    })
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use crate::sql::pipeline::context::AnalysisContext;
    use crate::sql::pipeline::registry::RewritePhase;
    use crate::sql::types::{PostprocessPlan, RewriteOutput, VtableDeletePlan, VtableUpdatePlan};

    use super::{validate_phase_invariants, validate_statement_output};

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse SQL");
        assert_eq!(statements.len(), 1);
        match statements.remove(0) {
            Statement::Query(query) => *query,
            other => panic!("expected query, got {other:?}"),
        }
    }

    fn parse_statement(sql: &str) -> Statement {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse SQL");
        assert_eq!(statements.len(), 1);
        statements.remove(0)
    }

    fn empty_statement() -> Statement {
        Statement::Query(Box::new(parse_query("SELECT 1")))
    }

    #[test]
    fn canonical_phase_rejects_unresolved_state_views() {
        let query = parse_query("SELECT * FROM lix_state_by_version");
        let context = AnalysisContext::from_query(&query);
        let err = validate_phase_invariants(RewritePhase::Canonicalize, &query, &context)
            .expect_err("canonical phase should reject unresolved state views");
        assert!(err.message.contains("lix_state_by_version"));
    }

    #[test]
    fn canonical_phase_rejects_duplicate_relation_aliases() {
        let query = parse_query("SELECT * FROM some_table AS t JOIN other_table AS t ON 1 = 1");
        let context = AnalysisContext::from_query(&query);
        let err = validate_phase_invariants(RewritePhase::Canonicalize, &query, &context)
            .expect_err("canonical phase should reject duplicate explicit aliases");
        assert!(err.message.contains("duplicate explicit relation alias"));
    }

    #[test]
    fn canonical_phase_rejects_materialized_state_without_tombstone_filter() {
        let query = parse_query(
            "SELECT * \
             FROM lix_internal_state_materialized_v1_example AS s \
             WHERE s.schema_key = 'example'",
        );
        let context = AnalysisContext::from_query(&query);
        let err = validate_phase_invariants(RewritePhase::Canonicalize, &query, &context)
            .expect_err(
                "canonical phase should reject materialized state reads without tombstone filter",
            );
        assert!(err.message.contains("without live-row tombstone filter"));
    }

    #[test]
    fn canonical_phase_accepts_materialized_state_with_tombstone_filter() {
        let query = parse_query(
            "SELECT * \
             FROM lix_internal_state_materialized_v1_example AS s \
             WHERE s.schema_key = 'example' \
               AND s.is_tombstone = 0",
        );
        let context = AnalysisContext::from_query(&query);
        validate_phase_invariants(RewritePhase::Canonicalize, &query, &context)
            .expect("canonical phase should accept materialized state reads with tombstone filter");
    }

    #[test]
    fn optimize_phase_rejects_unresolved_state_views() {
        let query = parse_query("SELECT * FROM lix_state_by_version");
        let context = AnalysisContext::from_query(&query);
        let err = validate_phase_invariants(RewritePhase::Optimize, &query, &context)
            .expect_err("optimize phase should reject unresolved state views");
        assert!(err.message.contains("lix_state_by_version"));
    }

    #[test]
    fn statement_validator_rejects_postprocess_with_mutations() {
        let output = RewriteOutput {
            statements: vec![empty_statement()],
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: Some(PostprocessPlan::VtableDelete(VtableDeletePlan {
                schema_key: "schema".to_string(),
                effective_scope_fallback: false,
                effective_scope_selection_sql: None,
            })),
            mutations: vec![crate::sql::types::MutationRow {
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
        assert!(err.message.contains("cannot emit mutation rows"));
    }

    #[test]
    fn statement_validator_rejects_multi_statement_update_validation() {
        let output = RewriteOutput {
            statements: vec![empty_statement(), empty_statement()],
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: None,
            mutations: Vec::new(),
            update_validations: vec![crate::sql::types::UpdateValidationPlan {
                table: "t".to_string(),
                where_clause: None,
                snapshot_content: None,
                snapshot_patch: None,
            }],
        };

        let err = validate_statement_output(&output)
            .expect_err("update validations with multiple statements should be rejected");
        assert!(err
            .message
            .contains("update validation rewrites require a single statement"));
    }

    #[test]
    fn statement_validator_rejects_update_validation_on_non_update_statement() {
        let output = RewriteOutput {
            statements: vec![empty_statement()],
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: None,
            mutations: Vec::new(),
            update_validations: vec![crate::sql::types::UpdateValidationPlan {
                table: "t".to_string(),
                where_clause: None,
                snapshot_content: None,
                snapshot_patch: None,
            }],
        };

        let err = validate_statement_output(&output)
            .expect_err("update validation on query statement should be rejected");
        assert!(err
            .message
            .contains("update validations require an UPDATE statement output"));
    }

    #[test]
    fn statement_validator_rejects_vtable_update_postprocess_on_non_update_statement() {
        let output = RewriteOutput {
            statements: vec![empty_statement()],
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: Some(PostprocessPlan::VtableUpdate(VtableUpdatePlan {
                schema_key: "schema".to_string(),
                explicit_writer_key: None,
                writer_key_assignment_present: false,
                file_data_assignment: None,
            })),
            mutations: Vec::new(),
            update_validations: Vec::new(),
        };

        let err = validate_statement_output(&output)
            .expect_err("vtable update postprocess on query statement should be rejected");
        assert!(err
            .message
            .contains("vtable update postprocess requires an UPDATE statement"));
    }

    #[test]
    fn statement_validator_rejects_vtable_delete_postprocess_on_non_delete_or_update_statement() {
        let output = RewriteOutput {
            statements: vec![empty_statement()],
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: Some(PostprocessPlan::VtableDelete(VtableDeletePlan {
                schema_key: "schema".to_string(),
                effective_scope_fallback: false,
                effective_scope_selection_sql: None,
            })),
            mutations: Vec::new(),
            update_validations: Vec::new(),
        };

        let err = validate_statement_output(&output)
            .expect_err("vtable delete postprocess on query statement should be rejected");
        assert!(err
            .message
            .contains("vtable delete postprocess requires an UPDATE or DELETE statement"));
    }

    #[test]
    fn statement_validator_rejects_empty_mutation_identity_fields() {
        let output = RewriteOutput {
            statements: vec![empty_statement()],
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: None,
            mutations: vec![crate::sql::types::MutationRow {
                entity_id: String::new(),
                schema_key: "schema".to_string(),
                schema_version: "1".to_string(),
                file_id: "file".to_string(),
                version_id: "version".to_string(),
                plugin_key: "plugin".to_string(),
                snapshot_content: None,
                untracked: false,
            }],
            update_validations: Vec::new(),
        };

        let err = validate_statement_output(&output)
            .expect_err("mutations with empty identity fields should be rejected");
        assert!(err.message.contains("mutation entity_id cannot be empty"));
    }

    #[test]
    fn statement_validator_rejects_update_validation_with_conflicting_snapshot_fields() {
        let output = RewriteOutput {
            statements: vec![parse_statement("UPDATE table_name SET value = 1")],
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: None,
            mutations: Vec::new(),
            update_validations: vec![crate::sql::types::UpdateValidationPlan {
                table: "table".to_string(),
                where_clause: None,
                snapshot_content: Some(serde_json::json!({"id":"value"})),
                snapshot_patch: Some(std::collections::BTreeMap::new()),
            }],
        };

        let err = validate_statement_output(&output)
            .expect_err("conflicting update validation snapshot fields should fail");
        assert!(err
            .message
            .contains("cannot define both snapshot_content and snapshot_patch"));
    }

    #[test]
    fn statement_validator_rejects_delete_postprocess_scope_sql_without_fallback() {
        let output = RewriteOutput {
            statements: vec![parse_statement("DELETE FROM table_name")],
            params: Vec::new(),
            registrations: Vec::new(),
            postprocess: Some(PostprocessPlan::VtableDelete(VtableDeletePlan {
                schema_key: "schema".to_string(),
                effective_scope_fallback: false,
                effective_scope_selection_sql: Some("schema_key = 'schema'".to_string()),
            })),
            mutations: Vec::new(),
            update_validations: Vec::new(),
        };

        let err = validate_statement_output(&output).expect_err(
            "delete postprocess effective scope SQL without fallback should be rejected",
        );
        assert!(err
            .message
            .contains("cannot emit effective scope selection SQL without fallback"));
    }
}
