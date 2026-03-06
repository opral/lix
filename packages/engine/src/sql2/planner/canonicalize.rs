use crate::sql2::catalog::{SurfaceBinding, SurfaceRegistry};
use crate::sql2::core::contracts::{BoundStatement, StatementKind};
use crate::sql2::planner::ir::{
    CanonicalStateScan, PredicateSpec, ProjectionExpr, ReadCommand, ReadContract, ReadPlan, SortKey,
};
use sqlparser::ast::{
    Expr, GroupByExpr, LimitClause, ObjectNamePart, OrderBy, OrderByKind, Query, Select,
    SelectItem, SetExpr, Statement, TableFactor,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalizeError {
    pub(crate) message: String,
}

impl CanonicalizeError {
    fn unsupported(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CanonicalizedRead {
    pub(crate) bound_statement: BoundStatement,
    pub(crate) surface_binding: SurfaceBinding,
    pub(crate) read_command: ReadCommand,
}

pub(crate) fn canonicalize_read(
    bound_statement: BoundStatement,
    registry: &SurfaceRegistry,
) -> Result<CanonicalizedRead, CanonicalizeError> {
    if bound_statement.statement_kind != StatementKind::Query {
        return Err(CanonicalizeError::unsupported(
            "sql2 day-1 canonicalizer only supports query statements",
        ));
    }

    let Statement::Query(query) = &bound_statement.statement else {
        return Err(CanonicalizeError::unsupported(
            "sql2 day-1 canonicalizer requires a top-level query",
        ));
    };

    let select = extract_supported_select(query)?;
    let surface_binding = bind_single_surface(select, registry)?;
    if !surface_binding.resolution_capabilities.canonical_state_scan {
        return Err(CanonicalizeError::unsupported(format!(
            "surface '{}' does not yet canonicalize through CanonicalStateScan",
            surface_binding.descriptor.public_name
        )));
    }

    let scan =
        CanonicalStateScan::from_surface_binding(surface_binding.clone()).ok_or_else(|| {
            CanonicalizeError::unsupported(format!(
                "surface '{}' did not produce a canonical state scan",
                surface_binding.descriptor.public_name
            ))
        })?;

    let mut root = ReadPlan::scan(scan);

    if let Some(predicate) = select.selection.as_ref() {
        root = ReadPlan::Filter {
            input: Box::new(root),
            predicate: PredicateSpec {
                sql: predicate.to_string(),
            },
        };
    }

    if let Some(expressions) = projection_expressions(&select.projection)? {
        root = ReadPlan::Project {
            input: Box::new(root),
            expressions,
        };
    }

    if let Some(ordering) = sort_keys(query.order_by.as_ref())? {
        root = ReadPlan::Sort {
            input: Box::new(root),
            ordering,
        };
    }

    if let Some((limit, offset)) = limit_values(query.limit_clause.as_ref())? {
        root = ReadPlan::Limit {
            input: Box::new(root),
            limit,
            offset,
        };
    }

    Ok(CanonicalizedRead {
        bound_statement,
        surface_binding,
        read_command: ReadCommand {
            root,
            contract: ReadContract::CommittedAtStart,
            requested_commit_mapping: None,
        },
    })
}

fn extract_supported_select(query: &Query) -> Result<&Select, CanonicalizeError> {
    if query.with.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
    {
        return Err(CanonicalizeError::unsupported(
            "sql2 day-1 canonicalizer does not support WITH/FETCH/LOCK/FOR/SETTINGS/FORMAT clauses",
        ));
    }

    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(CanonicalizeError::unsupported(
            "sql2 day-1 canonicalizer only supports SELECT bodies",
        ));
    };

    if select.distinct.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || select.connect_by.is_some()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
    {
        return Err(CanonicalizeError::unsupported(
            "sql2 day-1 canonicalizer only supports Scan->Filter->Project->Sort->Limit read shapes",
        ));
    }

    match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers) if exprs.is_empty() && modifiers.is_empty() => {}
        GroupByExpr::Expressions(_, _) | GroupByExpr::All(_) => {
            return Err(CanonicalizeError::unsupported(
                "sql2 day-1 canonicalizer does not support GROUP BY",
            ));
        }
    }

    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Err(CanonicalizeError::unsupported(
            "sql2 day-1 canonicalizer requires a single surface scan without joins",
        ));
    }

    Ok(select)
}

fn bind_single_surface(
    select: &Select,
    registry: &SurfaceRegistry,
) -> Result<SurfaceBinding, CanonicalizeError> {
    let relation = &select.from[0].relation;
    let TableFactor::Table { name, .. } = relation else {
        return Err(CanonicalizeError::unsupported(
            "sql2 day-1 canonicalizer only supports direct table references",
        ));
    };

    registry.bind_object_name(name).ok_or_else(|| {
        let surface_name = name
            .0
            .last()
            .and_then(ObjectNamePart::as_ident)
            .map(|ident| ident.value.clone())
            .unwrap_or_else(|| name.to_string());
        CanonicalizeError::unsupported(format!(
            "surface '{surface_name}' is not registered in sql2 SurfaceRegistry"
        ))
    })
}

fn projection_expressions(
    projection: &[SelectItem],
) -> Result<Option<Vec<ProjectionExpr>>, CanonicalizeError> {
    if projection.len() == 1 && matches!(projection[0], SelectItem::Wildcard(_)) {
        return Ok(None);
    }
    if projection.len() == 1 && matches!(projection[0], SelectItem::QualifiedWildcard(_, _)) {
        return Ok(None);
    }

    let mut expressions = Vec::with_capacity(projection.len());
    for item in projection {
        match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                return Err(CanonicalizeError::unsupported(
                    "mixed wildcard projections are not supported by the sql2 day-1 canonicalizer",
                ));
            }
            SelectItem::UnnamedExpr(expr) => expressions.push(ProjectionExpr {
                output_name: expr_output_name(expr),
                source_name: expr.to_string(),
            }),
            SelectItem::ExprWithAlias { expr, alias } => expressions.push(ProjectionExpr {
                output_name: alias.value.clone(),
                source_name: expr.to_string(),
            }),
        }
    }

    Ok(Some(expressions))
}

fn sort_keys(order_by: Option<&OrderBy>) -> Result<Option<Vec<SortKey>>, CanonicalizeError> {
    let Some(order_by) = order_by else {
        return Ok(None);
    };

    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return Err(CanonicalizeError::unsupported(
            "ORDER BY ALL is not supported by the sql2 day-1 canonicalizer",
        ));
    };

    Ok(Some(
        expressions
            .iter()
            .map(|expr| SortKey {
                column_name: expr_output_name(&expr.expr),
                descending: matches!(expr.options.asc, Some(false)),
            })
            .collect(),
    ))
}

fn limit_values(
    limit_clause: Option<&LimitClause>,
) -> Result<Option<(Option<u64>, u64)>, CanonicalizeError> {
    let Some(limit_clause) = limit_clause else {
        return Ok(None);
    };

    match limit_clause {
        LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            if !limit_by.is_empty() {
                return Err(CanonicalizeError::unsupported(
                    "LIMIT BY is not supported by the sql2 day-1 canonicalizer",
                ));
            }

            let limit = limit.as_ref().map(expr_to_u64).transpose()?;
            let offset = offset
                .as_ref()
                .map(|offset| expr_to_u64(&offset.value))
                .transpose()?
                .unwrap_or(0);
            Ok(Some((limit, offset)))
        }
        LimitClause::OffsetCommaLimit { offset, limit } => {
            Ok(Some((Some(expr_to_u64(limit)?), expr_to_u64(offset)?)))
        }
    }
}

fn expr_output_name(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|ident| ident.value.clone())
            .unwrap_or_else(|| expr.to_string()),
        Expr::Nested(inner) => expr_output_name(inner),
        _ => expr.to_string(),
    }
}

fn expr_to_u64(expr: &Expr) -> Result<u64, CanonicalizeError> {
    let Expr::Value(value) = expr else {
        return Err(CanonicalizeError::unsupported(
            "sql2 day-1 canonicalizer only supports literal LIMIT/OFFSET values",
        ));
    };

    let sqlparser::ast::Value::Number(raw, _) = &value.value else {
        return Err(CanonicalizeError::unsupported(
            "sql2 day-1 canonicalizer only supports numeric LIMIT/OFFSET values",
        ));
    };

    raw.parse::<u64>().map_err(|_| {
        CanonicalizeError::unsupported(format!(
            "sql2 day-1 canonicalizer could not parse numeric LIMIT/OFFSET value '{raw}'"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::canonicalize_read;
    use crate::sql2::catalog::{DynamicEntitySurfaceSpec, SurfaceRegistry};
    use crate::sql2::core::contracts::{BoundStatement, ExecutionContext};
    use crate::sql2::core::parser::parse_sql_script;
    use crate::sql2::planner::ir::{ReadContract, ReadPlan, VersionScope};

    fn bound_statement(sql: &str) -> BoundStatement {
        let mut statements = parse_sql_script(sql).expect("SQL should parse");
        let statement = statements.pop().expect("single statement");
        BoundStatement::from_statement(statement, Vec::new(), ExecutionContext::default())
    }

    #[test]
    fn canonicalizes_state_surface_into_day_one_read_plan_shell() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let canonicalized = canonicalize_read(
            bound_statement(
                "SELECT entity_id, schema_key \
                 FROM lix_state_by_version \
                 WHERE version_id = 'v1' \
                 ORDER BY entity_id DESC \
                 LIMIT 5 OFFSET 2",
            ),
            &registry,
        )
        .expect("state surface should canonicalize");

        assert_eq!(
            canonicalized.surface_binding.descriptor.public_name,
            "lix_state_by_version"
        );
        assert_eq!(
            canonicalized.read_command.contract,
            ReadContract::CommittedAtStart
        );

        let ReadPlan::Limit {
            input,
            limit,
            offset,
        } = &canonicalized.read_command.root
        else {
            panic!("expected limit root");
        };
        assert_eq!(*limit, Some(5));
        assert_eq!(*offset, 2);

        let ReadPlan::Sort { input, ordering } = input.as_ref() else {
            panic!("expected sort node");
        };
        assert_eq!(ordering.len(), 1);
        assert_eq!(ordering[0].column_name, "entity_id");
        assert!(ordering[0].descending);

        let ReadPlan::Project { input, expressions } = input.as_ref() else {
            panic!("expected project node");
        };
        assert_eq!(expressions.len(), 2);
        assert_eq!(expressions[0].output_name, "entity_id");
        assert_eq!(expressions[1].output_name, "schema_key");

        let ReadPlan::Filter { input, predicate } = input.as_ref() else {
            panic!("expected filter node");
        };
        assert_eq!(predicate.sql, "version_id = 'v1'");

        let ReadPlan::Scan(scan) = input.as_ref() else {
            panic!("expected scan node");
        };
        assert_eq!(scan.version_scope, VersionScope::ExplicitVersion);
        assert!(scan.expose_version_id);
    }

    #[test]
    fn canonicalizes_dynamic_entity_surface_into_canonical_state_scan() {
        let mut registry = SurfaceRegistry::with_builtin_surfaces();
        registry.register_dynamic_entity_surfaces(DynamicEntitySurfaceSpec {
            schema_key: "lix_key_value".to_string(),
            visible_columns: vec!["key".to_string(), "value".to_string()],
        });

        let canonicalized = canonicalize_read(
            bound_statement("SELECT key, value FROM lix_key_value WHERE key = 'hello'"),
            &registry,
        )
        .expect("entity surface should canonicalize");

        let ReadPlan::Project { input, expressions } = &canonicalized.read_command.root else {
            panic!("expected project root");
        };
        assert_eq!(expressions.len(), 2);
        assert_eq!(expressions[0].output_name, "key");
        assert_eq!(expressions[1].output_name, "value");

        let ReadPlan::Filter { input, predicate } = input.as_ref() else {
            panic!("expected filter node");
        };
        assert_eq!(predicate.sql, "key = 'hello'");

        let ReadPlan::Scan(scan) = input.as_ref() else {
            panic!("expected scan node");
        };
        let projection = scan
            .entity_projection
            .as_ref()
            .expect("entity surface should carry projection");
        assert_eq!(projection.schema_key, "lix_key_value");
        assert!(projection.hide_version_columns_by_default);
        assert_eq!(scan.version_scope, VersionScope::ActiveVersion);
    }

    #[test]
    fn rejects_join_reads_for_day_one_shell() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let error = canonicalize_read(
            bound_statement(
                "SELECT * FROM lix_state s JOIN lix_state_by_version b ON s.entity_id = b.entity_id",
            ),
            &registry,
        )
        .expect_err("joins should be rejected");

        assert!(
            error
                .message
                .contains("requires a single surface scan without joins"),
            "unexpected error: {}",
            error.message
        );
    }
}
