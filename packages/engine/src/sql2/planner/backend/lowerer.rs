use crate::sql2::backend::{PushdownDecision, PushdownSupport, RejectedPredicate};
use crate::sql2::catalog::{
    SurfaceBinding, SurfaceFamily, SurfaceOverridePredicate, SurfaceOverrideValue, SurfaceVariant,
};
use crate::sql2::core::parser::parse_sql_script;
use crate::sql2::planner::canonicalize::CanonicalizedRead;
use crate::sql2::planner::semantics::effective_state_resolver::{
    EffectiveStatePlan, EffectiveStateRequest,
};
use crate::LixError;
use sqlparser::ast::{
    BinaryOperator, Expr, Ident, Query, Select, SelectItem, SetExpr, Statement, TableAlias,
    TableFactor,
};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LoweredReadProgram {
    pub(crate) statements: Vec<Statement>,
    pub(crate) pushdown_decision: PushdownDecision,
}

pub(crate) fn lower_read_for_execution(
    canonicalized: &CanonicalizedRead,
    effective_state_request: Option<&EffectiveStateRequest>,
    effective_state_plan: Option<&EffectiveStatePlan>,
) -> Result<Option<LoweredReadProgram>, LixError> {
    match canonicalized.surface_binding.descriptor.surface_family {
        SurfaceFamily::State => {
            let Some(effective_state_request) = effective_state_request else {
                return Ok(None);
            };
            let Some(effective_state_plan) = effective_state_plan else {
                return Ok(None);
            };
            lower_state_read_for_execution(
                canonicalized,
                effective_state_request,
                effective_state_plan,
            )
            .map(|statement| {
                statement.map(|statement| LoweredReadProgram {
                    statements: vec![statement],
                    pushdown_decision: build_pushdown_decision(effective_state_plan),
                })
            })
        }
        SurfaceFamily::Entity => {
            let Some(effective_state_request) = effective_state_request else {
                return Ok(None);
            };
            let Some(effective_state_plan) = effective_state_plan else {
                return Ok(None);
            };
            lower_entity_read_for_execution(canonicalized, effective_state_request).map(
                |statement| {
                    statement.map(|statement| LoweredReadProgram {
                        statements: vec![statement],
                        pushdown_decision: build_pushdown_decision(effective_state_plan),
                    })
                },
            )
        }
        SurfaceFamily::Change => lower_change_read_for_execution(canonicalized).map(|statement| {
            statement.map(|statement| LoweredReadProgram {
                statements: vec![statement],
                pushdown_decision: change_pushdown_decision(canonicalized),
            })
        }),
        SurfaceFamily::Filesystem | SurfaceFamily::Admin => Ok(None),
    }
}

fn lower_state_read_for_execution(
    canonicalized: &CanonicalizedRead,
    effective_state_request: &EffectiveStateRequest,
    effective_state_plan: &EffectiveStatePlan,
) -> Result<Option<Statement>, LixError> {
    if !state_read_references_exposed_columns(
        &canonicalized.surface_binding,
        effective_state_request,
    ) {
        return Ok(None);
    }

    let Statement::Query(mut query) = canonicalized.bound_statement.statement.clone() else {
        return Ok(None);
    };
    let select = select_mut(query.as_mut())?;
    let TableFactor::Table { alias, .. } = &mut select.from[0].relation else {
        return Ok(None);
    };

    let (pushdown_predicates, residual_selection) =
        split_state_selection_for_pushdown(select.selection.as_ref(), effective_state_plan);
    let derived_query =
        build_state_source_query(&canonicalized.surface_binding, &pushdown_predicates)?;
    let derived_alias = alias.clone().or_else(|| {
        Some(TableAlias {
            explicit: false,
            name: Ident::new(&canonicalized.surface_binding.descriptor.public_name),
            columns: Vec::new(),
        })
    });
    select.from[0].relation = TableFactor::Derived {
        lateral: false,
        subquery: Box::new(derived_query),
        alias: derived_alias,
    };
    select.selection = residual_selection;

    Ok(Some(Statement::Query(query)))
}

fn state_read_references_exposed_columns(
    surface_binding: &SurfaceBinding,
    effective_state_request: &EffectiveStateRequest,
) -> bool {
    let exposed = surface_binding
        .exposed_columns
        .iter()
        .map(|column| column.to_ascii_lowercase())
        .collect::<std::collections::BTreeSet<_>>();
    effective_state_request
        .required_columns
        .iter()
        .all(|column| exposed.contains(&column.to_ascii_lowercase()))
}

fn lower_entity_read_for_execution(
    canonicalized: &CanonicalizedRead,
    effective_state_request: &EffectiveStateRequest,
) -> Result<Option<Statement>, LixError> {
    if query_uses_wildcard_projection(&canonicalized.bound_statement.statement) {
        return Ok(None);
    }

    let Statement::Query(mut query) = canonicalized.bound_statement.statement.clone() else {
        return Ok(None);
    };
    let select = select_mut(query.as_mut())?;
    let TableFactor::Table { alias, .. } = &mut select.from[0].relation else {
        return Ok(None);
    };

    let derived_query =
        build_entity_source_query(&canonicalized.surface_binding, effective_state_request)?;
    let derived_alias = alias.clone().or_else(|| {
        Some(TableAlias {
            explicit: false,
            name: Ident::new(&canonicalized.surface_binding.descriptor.public_name),
            columns: Vec::new(),
        })
    });
    select.from[0].relation = TableFactor::Derived {
        lateral: false,
        subquery: Box::new(derived_query),
        alias: derived_alias,
    };

    Ok(Some(Statement::Query(query)))
}

fn lower_change_read_for_execution(
    canonicalized: &CanonicalizedRead,
) -> Result<Option<Statement>, LixError> {
    let Statement::Query(mut query) = canonicalized.bound_statement.statement.clone() else {
        return Ok(None);
    };
    let select = select_mut(query.as_mut())?;
    let TableFactor::Table { alias, .. } = &mut select.from[0].relation else {
        return Ok(None);
    };

    let derived_query = build_change_source_query()?;
    let derived_alias = alias.clone().or_else(|| {
        Some(TableAlias {
            explicit: false,
            name: Ident::new(&canonicalized.surface_binding.descriptor.public_name),
            columns: Vec::new(),
        })
    });
    select.from[0].relation = TableFactor::Derived {
        lateral: false,
        subquery: Box::new(derived_query),
        alias: derived_alias,
    };

    Ok(Some(Statement::Query(query)))
}

fn build_state_source_query(
    surface_binding: &SurfaceBinding,
    pushdown_predicates: &[String],
) -> Result<Query, LixError> {
    let source = render_identifier(&surface_binding.descriptor.public_name);
    let sql = if pushdown_predicates.is_empty() {
        format!("SELECT * FROM {source}")
    } else {
        format!(
            "SELECT * FROM {source} WHERE {}",
            pushdown_predicates.join(" AND ")
        )
    };
    parse_single_query(&sql)
}

fn build_entity_source_query(
    surface_binding: &SurfaceBinding,
    effective_state_request: &EffectiveStateRequest,
) -> Result<Query, LixError> {
    let Some(schema_key) = surface_binding
        .implicit_overrides
        .fixed_schema_key
        .as_deref()
    else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "sql2 entity read lowerer requires fixed schema binding for '{}'",
                surface_binding.descriptor.public_name
            ),
        });
    };

    let projection = entity_projection_sql(surface_binding, effective_state_request);
    let projection = if projection.is_empty() {
        "entity_id AS lixcol_entity_id".to_string()
    } else {
        projection.join(", ")
    };

    let (source_table, mut predicates) = entity_source_predicates(surface_binding, schema_key);
    for predicate in &surface_binding.implicit_overrides.predicate_overrides {
        predicates.push(render_override_predicate(predicate));
    }

    let sql = if predicates.is_empty() {
        format!("SELECT {projection} FROM {source_table}")
    } else {
        format!(
            "SELECT {projection} FROM {source_table} WHERE {}",
            predicates.join(" AND ")
        )
    };
    parse_single_query(&sql)
}

fn build_change_source_query() -> Result<Query, LixError> {
    parse_single_query(
        "SELECT \
            ch.id AS id, \
            ch.entity_id AS entity_id, \
            ch.schema_key AS schema_key, \
            ch.schema_version AS schema_version, \
            ch.file_id AS file_id, \
            ch.plugin_key AS plugin_key, \
            ch.metadata AS metadata, \
            ch.created_at AS created_at, \
            CASE \
                WHEN ch.snapshot_id = 'no-content' THEN NULL \
                ELSE s.content \
            END AS snapshot_content \
         FROM lix_internal_change ch \
         LEFT JOIN lix_internal_snapshot s \
            ON s.id = ch.snapshot_id",
    )
}

fn entity_projection_sql(
    surface_binding: &SurfaceBinding,
    effective_state_request: &EffectiveStateRequest,
) -> Vec<String> {
    let mut projections = Vec::new();
    for column in &effective_state_request.required_columns {
        let Some(expression) = entity_projection_sql_for_column(surface_binding, column) else {
            continue;
        };
        if !projections.iter().any(|existing| existing == &expression) {
            projections.push(expression);
        }
    }
    projections
}

fn entity_projection_sql_for_column(
    surface_binding: &SurfaceBinding,
    column: &str,
) -> Option<String> {
    if let Some(source_column) =
        entity_hidden_alias_source_column(column, surface_binding.descriptor.surface_variant)
    {
        let alias = render_identifier(column);
        return Some(format!("{source_column} AS {alias}"));
    }

    if surface_binding
        .exposed_columns
        .iter()
        .any(|candidate| candidate.eq_ignore_ascii_case(column))
    {
        let alias = render_identifier(column);
        let path = escape_sql_string(column);
        return Some(format!(
            "lix_json_extract(snapshot_content, '{path}') AS {alias}"
        ));
    }

    None
}

fn entity_hidden_alias_source_column(alias: &str, variant: SurfaceVariant) -> Option<&'static str> {
    match alias.to_ascii_lowercase().as_str() {
        "lixcol_entity_id" => Some("entity_id"),
        "lixcol_schema_key" => Some("schema_key"),
        "lixcol_file_id" => Some("file_id"),
        "lixcol_plugin_key" => Some("plugin_key"),
        "lixcol_schema_version" => Some("schema_version"),
        "lixcol_change_id" => Some("change_id"),
        "lixcol_created_at" => Some("created_at"),
        "lixcol_updated_at" => Some("updated_at"),
        "lixcol_global" => Some("global"),
        "lixcol_writer_key" => Some("writer_key"),
        "lixcol_untracked" => Some("untracked"),
        "lixcol_metadata" => Some("metadata"),
        "lixcol_version_id" if variant != SurfaceVariant::Default => Some("version_id"),
        "lixcol_commit_id" if variant == SurfaceVariant::History => Some("commit_id"),
        "lixcol_root_commit_id" if variant == SurfaceVariant::History => Some("root_commit_id"),
        "lixcol_depth" if variant == SurfaceVariant::History => Some("depth"),
        _ => None,
    }
}

fn entity_source_predicates(
    surface_binding: &SurfaceBinding,
    schema_key: &str,
) -> (String, Vec<String>) {
    let mut predicates = vec![format!(
        "{} = '{}'",
        render_identifier("schema_key"),
        escape_sql_string(schema_key)
    )];

    let source_table = match surface_binding.descriptor.surface_variant {
        SurfaceVariant::Default => {
            if let Some(version_id) = surface_binding
                .implicit_overrides
                .fixed_version_id
                .as_deref()
            {
                predicates.push(format!(
                    "{} = '{}'",
                    render_identifier("version_id"),
                    escape_sql_string(version_id)
                ));
                "lix_state_by_version".to_string()
            } else {
                "lix_state".to_string()
            }
        }
        SurfaceVariant::ByVersion => "lix_state_by_version".to_string(),
        SurfaceVariant::History => "lix_state_history".to_string(),
        SurfaceVariant::Active | SurfaceVariant::WorkingChanges => {
            surface_binding.descriptor.public_name.clone()
        }
    };

    (source_table, predicates)
}

fn render_override_predicate(predicate: &SurfaceOverridePredicate) -> String {
    match &predicate.value {
        SurfaceOverrideValue::Null => {
            format!("{} IS NULL", render_identifier(&predicate.column))
        }
        value => format!(
            "{} = {}",
            render_identifier(&predicate.column),
            render_override_value(value)
        ),
    }
}

fn render_override_value(value: &SurfaceOverrideValue) -> String {
    match value {
        SurfaceOverrideValue::Null => "NULL".to_string(),
        SurfaceOverrideValue::Boolean(value) => value.to_string(),
        SurfaceOverrideValue::Number(value) => value.clone(),
        SurfaceOverrideValue::String(value) => format!("'{}'", escape_sql_string(value)),
    }
}

fn build_pushdown_decision(effective_state_plan: &EffectiveStatePlan) -> PushdownDecision {
    PushdownDecision {
        accepted_predicates: effective_state_plan.pushdown_safe_predicates.clone(),
        rejected_predicates: effective_state_plan
            .residual_predicates
            .iter()
            .map(|predicate| RejectedPredicate {
                predicate: predicate.clone(),
                reason:
                    "day-1 sql2 read lowering keeps this predicate above effective-state resolution"
                        .to_string(),
                support: PushdownSupport::Unsupported,
            })
            .collect(),
        residual_predicates: effective_state_plan.residual_predicates.clone(),
    }
}

fn change_pushdown_decision(canonicalized: &CanonicalizedRead) -> PushdownDecision {
    let residual_predicates = read_predicates_from_query(canonicalized);
    PushdownDecision {
        accepted_predicates: Vec::new(),
        rejected_predicates: residual_predicates
            .iter()
            .map(|predicate| RejectedPredicate {
                predicate: predicate.clone(),
                reason: "sql2 change-scan lowering keeps change predicates above the derived change source".to_string(),
                support: PushdownSupport::Unsupported,
            })
            .collect(),
        residual_predicates,
    }
}

fn split_state_selection_for_pushdown(
    selection: Option<&Expr>,
    effective_state_plan: &EffectiveStatePlan,
) -> (Vec<String>, Option<Expr>) {
    let accepted = effective_state_plan
        .pushdown_safe_predicates
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let Some(selection) = selection else {
        return (Vec::new(), None);
    };

    let mut pushdown = Vec::new();
    let mut residual = Vec::new();
    for predicate in split_conjunctive_predicates(selection) {
        if accepted.contains(&predicate.to_string()) {
            pushdown.push(predicate.to_string());
        } else {
            residual.push(predicate);
        }
    }

    (pushdown, combine_conjunctive_predicates(residual))
}

fn split_conjunctive_predicates(expr: &Expr) -> Vec<Expr> {
    let mut predicates = Vec::new();
    collect_conjunctive_predicates(expr, &mut predicates);
    predicates
}

fn collect_conjunctive_predicates(expr: &Expr, predicates: &mut Vec<Expr>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect_conjunctive_predicates(left, predicates);
            collect_conjunctive_predicates(right, predicates);
        }
        Expr::Nested(inner) => collect_conjunctive_predicates(inner, predicates),
        _ => predicates.push(expr.clone()),
    }
}

fn combine_conjunctive_predicates(predicates: Vec<Expr>) -> Option<Expr> {
    let mut predicates = predicates.into_iter();
    let first = predicates.next()?;
    Some(predicates.fold(first, |left, right| Expr::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::And,
        right: Box::new(right),
    }))
}

fn read_predicates_from_query(canonicalized: &CanonicalizedRead) -> Vec<String> {
    let Statement::Query(query) = &canonicalized.bound_statement.statement else {
        return Vec::new();
    };
    let Some(select) = select_ref(query.as_ref()) else {
        return Vec::new();
    };
    let Some(selection) = &select.selection else {
        return Vec::new();
    };

    split_conjunctive_predicates(selection)
        .into_iter()
        .map(|predicate| predicate.to_string())
        .collect()
}

fn parse_single_query(sql: &str) -> Result<Query, LixError> {
    let mut statements = parse_sql_script(sql).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: error.to_string(),
    })?;
    if statements.len() != 1 {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "expected a single lowered sql2 read statement".to_string(),
        });
    }
    let Statement::Query(query) = statements.remove(0) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "expected lowered sql2 read to parse as a query".to_string(),
        });
    };
    Ok(*query)
}

fn query_uses_wildcard_projection(statement: &Statement) -> bool {
    let Statement::Query(query) = statement else {
        return false;
    };
    let Some(select) = select_query(query.as_ref()) else {
        return false;
    };
    select.projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
        )
    })
}

fn select_query(query: &Query) -> Option<&Select> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    Some(select.as_ref())
}

fn select_ref(query: &Query) -> Option<&Select> {
    select_query(query)
}

fn select_mut(query: &mut Query) -> Result<&mut Select, LixError> {
    let SetExpr::Select(select) = query.body.as_mut() else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "sql2 live read lowering requires a SELECT query".to_string(),
        });
    };
    Ok(select.as_mut())
}

fn render_identifier(value: &str) -> String {
    Ident::new(value).to_string()
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::{lower_read_for_execution, LoweredReadProgram};
    use crate::sql2::catalog::SurfaceRegistry;
    use crate::sql2::core::contracts::{BoundStatement, ExecutionContext};
    use crate::sql2::planner::canonicalize::canonicalize_read;
    use crate::sql2::planner::semantics::dependency_spec::derive_dependency_spec_from_canonicalized_read;
    use crate::sql2::planner::semantics::effective_state_resolver::build_effective_state;
    use crate::{SqlDialect, Value};

    fn lowered_program(registry: &SurfaceRegistry, sql: &str) -> Option<LoweredReadProgram> {
        let mut statements =
            crate::sql2::core::parser::parse_sql_script(sql).expect("SQL should parse");
        let statement = statements.pop().expect("single statement");
        let bound = BoundStatement::from_statement(
            statement,
            Vec::<Value>::new(),
            ExecutionContext::with_dialect(SqlDialect::Sqlite),
        );
        let canonicalized = canonicalize_read(bound, registry).expect("query should canonicalize");
        let dependency_spec = derive_dependency_spec_from_canonicalized_read(&canonicalized);
        let effective_state = build_effective_state(&canonicalized, dependency_spec.as_ref());
        lower_read_for_execution(
            &canonicalized,
            effective_state.as_ref().map(|(request, _)| request),
            effective_state.as_ref().map(|(_, plan)| plan),
        )
        .expect("lowering should succeed")
    }

    #[test]
    fn lowers_builtin_entity_reads_through_state_surfaces() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let lowered = lowered_program(
            &registry,
            "SELECT key, value FROM lix_key_value WHERE key = 'hello'",
        )
        .expect("builtin entity read should lower");
        let lowered_sql = lowered.statements[0].to_string();

        assert!(lowered_sql.contains("FROM (SELECT"));
        assert!(lowered_sql.contains("FROM lix_state"));
        assert!(lowered_sql.contains("schema_key = 'lix_key_value'"));
        assert!(lowered_sql.contains("file_id = 'lix'"));
        assert!(lowered_sql.contains("plugin_key = 'lix'"));
        assert_eq!(
            lowered.pushdown_decision.residual_predicates,
            vec!["key = 'hello'".to_string()]
        );
        assert_eq!(
            lowered.pushdown_decision.accepted_predicates,
            Vec::<String>::new()
        );
    }

    #[test]
    fn lowers_dynamic_entity_reads_with_scalar_override_predicates() {
        let mut registry = SurfaceRegistry::with_builtin_surfaces();
        registry.register_dynamic_entity_surfaces(crate::sql2::catalog::DynamicEntitySurfaceSpec {
            schema_key: "message".to_string(),
            visible_columns: vec!["body".to_string(), "id".to_string()],
            fixed_version_id: None,
            predicate_overrides: vec![
                crate::sql2::catalog::SurfaceOverridePredicate {
                    column: "file_id".to_string(),
                    value: crate::sql2::catalog::SurfaceOverrideValue::String("inlang".to_string()),
                },
                crate::sql2::catalog::SurfaceOverridePredicate {
                    column: "plugin_key".to_string(),
                    value: crate::sql2::catalog::SurfaceOverrideValue::String(
                        "inlang_sdk".to_string(),
                    ),
                },
                crate::sql2::catalog::SurfaceOverridePredicate {
                    column: "global".to_string(),
                    value: crate::sql2::catalog::SurfaceOverrideValue::Boolean(true),
                },
            ],
        });

        let lowered = lowered_program(
            &registry,
            "SELECT body, lixcol_global FROM message WHERE id = 'm1'",
        )
        .expect("dynamic entity read should lower");
        let lowered_sql = lowered.statements[0].to_string();

        assert!(lowered_sql.contains("schema_key = 'message'"));
        assert!(lowered_sql.contains("file_id = 'inlang'"));
        assert!(lowered_sql.contains("plugin_key = 'inlang_sdk'"));
        assert!(lowered_sql.contains("global = true"));
    }

    #[test]
    fn rejects_entity_wildcard_reads_for_live_lowering() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        assert_eq!(
            lowered_program(&registry, "SELECT * FROM lix_key_value"),
            None
        );
    }

    #[test]
    fn lowers_state_reads_through_explicit_source_boundary() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let lowered = lowered_program(
            &registry,
            "SELECT entity_id FROM lix_state WHERE schema_key = 'lix_key_value'",
        )
        .expect("state read should lower");
        let lowered_sql = lowered.statements[0].to_string();

        assert!(lowered_sql
            .contains("FROM (SELECT * FROM lix_state WHERE schema_key = 'lix_key_value')"));
        assert!(!lowered_sql.contains(") WHERE schema_key = 'lix_key_value'"));
        assert_eq!(
            lowered.pushdown_decision.accepted_predicates,
            vec!["schema_key = 'lix_key_value'".to_string()]
        );
        assert_eq!(
            lowered.pushdown_decision.residual_predicates,
            Vec::<String>::new()
        );
    }

    #[test]
    fn lowers_change_reads_through_internal_change_sources() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let lowered = lowered_program(
            &registry,
            "SELECT id, schema_key, snapshot_content FROM lix_change WHERE entity_id = 'entity-1'",
        )
        .expect("change read should lower");
        let lowered_sql = lowered.statements[0].to_string();

        assert!(lowered_sql.contains("FROM (SELECT ch.id AS id"));
        assert!(lowered_sql.contains("FROM lix_internal_change ch"));
        assert!(lowered_sql.contains("LEFT JOIN lix_internal_snapshot s"));
        assert_eq!(
            lowered.pushdown_decision.residual_predicates,
            vec!["entity_id = 'entity-1'".to_string()]
        );
    }
}
