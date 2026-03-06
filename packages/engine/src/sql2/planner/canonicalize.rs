use crate::sql2::catalog::{SurfaceBinding, SurfaceFamily, SurfaceRegistry};
use crate::sql2::core::contracts::{BoundStatement, StatementKind};
use crate::sql2::planner::ir::{
    CanonicalStateScan, MutationPayload, PredicateSpec, ProjectionExpr, ReadCommand, ReadContract,
    ReadPlan, SortKey, WriteCommand, WriteMode, WriteOperationKind, WriteSelector,
};
use crate::sql_shared::placeholders::{resolve_placeholder_index, PlaceholderState};
use crate::Value;
use sqlparser::ast::{
    Expr, GroupByExpr, Insert, LimitClause, ObjectNamePart, OrderBy, OrderByKind, Query, Select,
    SelectItem, SetExpr, Statement, TableFactor, Value as SqlValue, Visit, Visitor,
};
use std::collections::BTreeMap;
use std::ops::ControlFlow;

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

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CanonicalizedWrite {
    pub(crate) bound_statement: BoundStatement,
    pub(crate) surface_binding: SurfaceBinding,
    pub(crate) write_command: WriteCommand,
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

pub(crate) fn canonicalize_write(
    bound_statement: BoundStatement,
    registry: &SurfaceRegistry,
) -> Result<CanonicalizedWrite, CanonicalizeError> {
    if bound_statement.statement_kind != StatementKind::Insert {
        return Err(CanonicalizeError::unsupported(
            "sql2 day-1 write canonicalizer only supports INSERT statements",
        ));
    }

    let Statement::Insert(insert) = &bound_statement.statement else {
        return Err(CanonicalizeError::unsupported(
            "sql2 day-1 write canonicalizer requires a top-level INSERT",
        ));
    };
    let surface_binding = bind_insert_surface(insert, registry)?;
    if !surface_binding.resolution_capabilities.semantic_write {
        return Err(CanonicalizeError::unsupported(format!(
            "surface '{}' is not writable in sql2",
            surface_binding.descriptor.public_name
        )));
    }
    if surface_binding.descriptor.surface_family != SurfaceFamily::State {
        return Err(CanonicalizeError::unsupported(
            "sql2 day-1 write canonicalizer only supports lix_state* surfaces",
        ));
    }
    if !insert.assignments.is_empty() || insert.on.is_some() || insert.returning.is_some() {
        return Err(CanonicalizeError::unsupported(
            "sql2 day-1 write canonicalizer only supports plain VALUES inserts without ON CONFLICT or RETURNING",
        ));
    }

    let payload = insert_payload(insert, &bound_statement.bound_parameters)?;
    let mode = write_mode_from_payload(&payload);

    Ok(CanonicalizedWrite {
        bound_statement: bound_statement.clone(),
        surface_binding: surface_binding.clone(),
        write_command: WriteCommand {
            operation_kind: WriteOperationKind::Insert,
            target: surface_binding,
            selector: WriteSelector::default(),
            payload: MutationPayload::FullSnapshot(payload),
            mode,
            execution_context: bound_statement.execution_context,
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
    if query_contains_nested_query_shape(query) {
        return Err(CanonicalizeError::unsupported(
            "sql2 day-1 canonicalizer does not support subqueries or derived tables",
        ));
    }

    Ok(select)
}

fn query_contains_nested_query_shape(query: &Query) -> bool {
    struct Collector {
        query_count: usize,
        has_derived_tables: bool,
        has_expression_subqueries: bool,
    }

    impl Visitor for Collector {
        type Break = ();

        fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
            self.query_count += 1;
            ControlFlow::Continue(())
        }

        fn pre_visit_table_factor(
            &mut self,
            table_factor: &TableFactor,
        ) -> ControlFlow<Self::Break> {
            if matches!(table_factor, TableFactor::Derived { .. }) {
                self.has_derived_tables = true;
            }
            ControlFlow::Continue(())
        }

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            if matches!(
                expr,
                Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. }
            ) {
                self.has_expression_subqueries = true;
            }
            ControlFlow::Continue(())
        }
    }

    let mut collector = Collector {
        query_count: 0,
        has_derived_tables: false,
        has_expression_subqueries: false,
    };
    let _ = query.visit(&mut collector);
    collector.query_count > 1 || collector.has_derived_tables || collector.has_expression_subqueries
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

fn bind_insert_surface(
    insert: &Insert,
    registry: &SurfaceRegistry,
) -> Result<SurfaceBinding, CanonicalizeError> {
    let sqlparser::ast::TableObject::TableName(name) = &insert.table else {
        return Err(CanonicalizeError::unsupported(
            "sql2 day-1 write canonicalizer only supports direct table targets",
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

fn insert_payload(
    insert: &Insert,
    params: &[Value],
) -> Result<BTreeMap<String, Value>, CanonicalizeError> {
    let Some(source) = &insert.source else {
        return Err(CanonicalizeError::unsupported(
            "sql2 day-1 write canonicalizer requires VALUES inserts",
        ));
    };
    let SetExpr::Values(values) = source.body.as_ref() else {
        return Err(CanonicalizeError::unsupported(
            "sql2 day-1 write canonicalizer requires VALUES inserts",
        ));
    };
    if values.rows.len() != 1 {
        return Err(CanonicalizeError::unsupported(
            "sql2 day-1 write canonicalizer only supports single-row inserts",
        ));
    }

    let row = &values.rows[0];
    if row.len() != insert.columns.len() {
        return Err(CanonicalizeError::unsupported(
            "sql2 day-1 write canonicalizer requires one value per inserted column",
        ));
    }

    let mut placeholder_state = PlaceholderState::new();
    let mut payload = BTreeMap::new();
    for (column, expr) in insert.columns.iter().zip(row.iter()) {
        let value = expr_to_value(expr, params, &mut placeholder_state)?;
        payload.insert(column.value.to_ascii_lowercase(), value);
    }
    Ok(payload)
}

fn expr_to_value(
    expr: &Expr,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Result<Value, CanonicalizeError> {
    match expr {
        Expr::Value(value) => sql_value_to_engine_value(&value.value, params, placeholder_state),
        Expr::Nested(inner) => expr_to_value(inner, params, placeholder_state),
        Expr::UnaryOp { op, expr } if op.to_string() == "-" => {
            match expr_to_value(expr, params, placeholder_state)? {
                Value::Integer(value) => Ok(Value::Integer(-value)),
                Value::Real(value) => Ok(Value::Real(-value)),
                _ => Err(CanonicalizeError::unsupported(
                    "sql2 day-1 write canonicalizer only supports numeric unary minus literals",
                )),
            }
        }
        _ => Err(CanonicalizeError::unsupported(
            "sql2 day-1 write canonicalizer only supports literal and placeholder VALUES",
        )),
    }
}

fn sql_value_to_engine_value(
    value: &SqlValue,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Result<Value, CanonicalizeError> {
    match value {
        SqlValue::SingleQuotedString(value) | SqlValue::DoubleQuotedString(value) => {
            Ok(Value::Text(value.clone()))
        }
        SqlValue::Number(raw, _) => {
            if let Ok(integer) = raw.parse::<i64>() {
                Ok(Value::Integer(integer))
            } else if let Ok(real) = raw.parse::<f64>() {
                Ok(Value::Real(real))
            } else {
                Err(CanonicalizeError::unsupported(format!(
                    "sql2 day-1 write canonicalizer could not parse numeric literal '{raw}'"
                )))
            }
        }
        SqlValue::Boolean(value) => Ok(Value::Boolean(*value)),
        SqlValue::Null => Ok(Value::Null),
        SqlValue::SingleQuotedByteStringLiteral(value) => Ok(Value::Blob(value.clone().into_bytes())),
        SqlValue::Placeholder(token) => {
            let index = resolve_placeholder_index(token, params.len(), placeholder_state).map_err(
                |err| CanonicalizeError::unsupported(format!(
                    "sql2 day-1 write canonicalizer could not bind placeholder: {}",
                    err.description
                )),
            )?;
            params.get(index).cloned().ok_or_else(|| {
                CanonicalizeError::unsupported(format!(
                    "sql2 day-1 write canonicalizer placeholder index {} was out of bounds",
                    index + 1
                ))
            })
        }
        _ => Err(CanonicalizeError::unsupported(
            "sql2 day-1 write canonicalizer only supports string, numeric, boolean, null, blob, and placeholder VALUES",
        )),
    }
}

fn write_mode_from_payload(payload: &BTreeMap<String, Value>) -> WriteMode {
    match payload
        .get("untracked")
        .or_else(|| payload.get("lixcol_untracked"))
    {
        Some(Value::Boolean(true)) => WriteMode::Untracked,
        Some(Value::Integer(value)) if *value != 0 => WriteMode::Untracked,
        Some(Value::Text(value))
            if matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true") =>
        {
            WriteMode::Untracked
        }
        _ => WriteMode::Tracked,
    }
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
    use super::{canonicalize_read, canonicalize_write};
    use crate::sql2::catalog::{DynamicEntitySurfaceSpec, SurfaceRegistry};
    use crate::sql2::core::contracts::{BoundStatement, ExecutionContext};
    use crate::sql2::core::parser::parse_sql_script;
    use crate::sql2::planner::ir::{
        MutationPayload, ReadContract, ReadPlan, VersionScope, WriteMode, WriteOperationKind,
    };
    use crate::Value;

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

    #[test]
    fn rejects_nested_subqueries_for_day_one_shell() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let error = canonicalize_read(
            bound_statement(
                "SELECT entity_id FROM lix_state WHERE entity_id IN (SELECT entity_id FROM lix_state_by_version)",
            ),
            &registry,
        )
        .expect_err("subqueries should be rejected");

        assert!(
            error
                .message
                .contains("does not support subqueries or derived tables"),
            "unexpected error: {}",
            error.message
        );
    }

    #[test]
    fn canonicalizes_state_insert_into_write_command() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let canonicalized = canonicalize_write(
            bound_statement(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'entity-1', 'lix_key_value', 'lix', 'version-a', 'lix', '{\"key\":\"hello\"}', '1'\
                 )",
            ),
            &registry,
        )
        .expect("state insert should canonicalize");

        assert_eq!(
            canonicalized.surface_binding.descriptor.public_name,
            "lix_state_by_version"
        );
        assert_eq!(
            canonicalized.write_command.operation_kind,
            WriteOperationKind::Insert
        );
        assert_eq!(canonicalized.write_command.mode, WriteMode::Tracked);
        let MutationPayload::FullSnapshot(payload) = &canonicalized.write_command.payload else {
            panic!("expected full snapshot payload");
        };
        assert_eq!(
            payload.get("entity_id"),
            Some(&Value::Text("entity-1".to_string()))
        );
        assert_eq!(
            payload.get("version_id"),
            Some(&Value::Text("version-a".to_string()))
        );
    }

    #[test]
    fn rejects_entity_writes_for_day_one_write_shell() {
        let mut registry = SurfaceRegistry::with_builtin_surfaces();
        registry.register_dynamic_entity_surfaces(DynamicEntitySurfaceSpec {
            schema_key: "lix_key_value".to_string(),
            visible_columns: vec!["key".to_string(), "value".to_string()],
        });

        let error = canonicalize_write(
            bound_statement("INSERT INTO lix_key_value (key, value) VALUES ('k', 'v')"),
            &registry,
        )
        .expect_err("entity writes should stay on the legacy path for now");

        assert!(
            error.message.contains("only supports lix_state* surfaces"),
            "unexpected error: {}",
            error.message
        );
    }
}
