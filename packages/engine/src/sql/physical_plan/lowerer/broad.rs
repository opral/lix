use super::*;
use crate::schema::builtin::builtin_schema_definition;
use crate::sql::logical_plan::public_ir::{
    BroadPublicReadJoin, BroadPublicReadQuery, BroadPublicReadRelation, BroadPublicReadSelect,
    BroadPublicReadSetExpr, BroadPublicReadStatement, BroadPublicReadTableFactor,
    BroadPublicReadTableWithJoins, BroadPublicReadWith,
};
use serde_json::Value as JsonValue;
use sqlparser::ast::With;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RenderedBroadPublicReadStatement {
    pub(crate) shell_statement: Statement,
    pub(crate) relation_render_nodes: Vec<TerminalRelationRenderNode>,
}

impl RenderedBroadPublicReadStatement {
    #[cfg(test)]
    pub(crate) fn render_sql(&self, dialect: SqlDialect) -> Result<String, LixError> {
        let statement =
            crate::sql::ast::lowering::lower_statement(self.shell_statement.clone(), dialect)?;
        let mut sql = statement.to_string();
        for render_node in &self.relation_render_nodes {
            sql = sql.replace(
                &crate::sql::physical_plan::plan::placeholder_table_factor_sql(render_node),
                &render_node.rendered_factor_sql,
            );
        }
        Ok(sql)
    }
}

#[cfg(test)]
pub(crate) fn rewrite_supported_public_read_surfaces_in_statement(
    statement: &mut Statement,
    dialect: SqlDialect,
) -> Result<(), LixError> {
    let rendered =
        rewrite_supported_public_read_surfaces_in_statement_with_registry_and_active_version_id(
            statement,
            &SurfaceRegistry::with_builtin_surfaces(),
            dialect,
            None,
            &BTreeMap::new(),
        )?;
    let mut parsed = crate::sql::parser::parse_sql_script(&rendered.render_sql(dialect)?)
        .map_err(|error| LixError::new("LIX_ERROR_UNKNOWN", error.to_string()))?;
    *statement = parsed
        .pop()
        .ok_or_else(|| LixError::new("LIX_ERROR_UNKNOWN", "expected rewritten statement"))?;
    Ok(())
}

#[cfg(test)]
pub(crate) fn rewrite_supported_public_read_surfaces_in_statement_with_registry_and_active_version_id(
    statement: &Statement,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<RenderedBroadPublicReadStatement, LixError> {
    rewrite_supported_public_read_surfaces_in_statement_with_registry(
        statement,
        registry,
        dialect,
        active_version_id,
        known_live_layouts,
    )
}

#[cfg(test)]
pub(crate) fn rewrite_supported_public_read_surfaces_in_statement_with_registry(
    statement: &Statement,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<RenderedBroadPublicReadStatement, LixError> {
    let Some(bound_statement) =
        bind_broad_public_read_statement_with_registry(statement, registry)?
    else {
        return Ok(RenderedBroadPublicReadStatement {
            shell_statement: statement.clone(),
            relation_render_nodes: Vec::new(),
        });
    };
    lower_broad_public_read_statement(
        &bound_statement,
        registry,
        dialect,
        active_version_id,
        known_live_layouts,
    )
}

pub(crate) fn broad_public_relation_supports_terminal_render(
    binding: &SurfaceBinding,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<bool, LixError> {
    build_supported_public_read_surface_sql(
        &binding.descriptor.public_name,
        registry,
        false,
        dialect,
        active_version_id,
        known_live_layouts,
    )
    .map(|sql| sql.is_some())
}

pub(crate) fn render_broad_public_read_statement_with_registry_and_active_version_id(
    statement: &BroadPublicReadStatement,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<RenderedBroadPublicReadStatement, LixError> {
    lower_broad_public_read_statement(
        statement,
        registry,
        dialect,
        active_version_id,
        known_live_layouts,
    )
}

pub(crate) fn bind_broad_public_read_statement_with_registry(
    statement: &Statement,
    registry: &SurfaceRegistry,
) -> Result<Option<BroadPublicReadStatement>, LixError> {
    match statement {
        Statement::Query(query) => Ok(Some(BroadPublicReadStatement::Query(
            bind_broad_public_read_query_scoped(query, registry, &BTreeSet::new())?,
        ))),
        Statement::Explain {
            statement: inner, ..
        } => {
            let Some(bound_inner) =
                bind_broad_public_read_statement_with_registry(inner, registry)?
            else {
                return Ok(None);
            };
            Ok(Some(BroadPublicReadStatement::Explain {
                original: statement.clone(),
                statement: Box::new(bound_inner),
            }))
        }
        _ => Ok(None),
    }
}

fn bind_broad_public_read_query_scoped(
    query: &Query,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<BroadPublicReadQuery, LixError> {
    let mut scoped_ctes = visible_ctes.clone();
    let with = if let Some(with) = &query.with {
        let mut cte_scope = visible_ctes.clone();
        let mut cte_tables = Vec::with_capacity(with.cte_tables.len());
        for cte in &with.cte_tables {
            cte_tables.push(bind_broad_public_read_query_scoped(
                &cte.query, registry, &cte_scope,
            )?);
            cte_scope.insert(cte.alias.name.value.to_ascii_lowercase());
        }
        scoped_ctes = cte_scope;
        Some(BroadPublicReadWith {
            original: with.clone(),
            cte_tables,
        })
    } else {
        None
    };

    Ok(BroadPublicReadQuery {
        original: query.clone(),
        with,
        body: bind_broad_public_read_set_expr(query.body.as_ref(), registry, &scoped_ctes)?,
    })
}

fn bind_broad_public_read_set_expr(
    expr: &SetExpr,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<BroadPublicReadSetExpr, LixError> {
    match expr {
        SetExpr::Select(select) => Ok(BroadPublicReadSetExpr::Select(BroadPublicReadSelect {
            original: select.as_ref().clone(),
            from: select
                .from
                .iter()
                .map(|table| bind_broad_public_read_table_with_joins(table, registry, visible_ctes))
                .collect::<Result<_, _>>()?,
        })),
        SetExpr::Query(query) => Ok(BroadPublicReadSetExpr::Query(Box::new(
            bind_broad_public_read_query_scoped(query, registry, visible_ctes)?,
        ))),
        SetExpr::SetOperation { left, right, .. } => Ok(BroadPublicReadSetExpr::SetOperation {
            original: expr.clone(),
            left: Box::new(bind_broad_public_read_set_expr(
                left,
                registry,
                visible_ctes,
            )?),
            right: Box::new(bind_broad_public_read_set_expr(
                right,
                registry,
                visible_ctes,
            )?),
        }),
        SetExpr::Table(table) => {
            let Some(table_name) = table.table_name.as_deref() else {
                return Ok(BroadPublicReadSetExpr::Other(expr.clone()));
            };
            Ok(BroadPublicReadSetExpr::Table {
                original: expr.clone(),
                relation: classify_broad_public_read_relation(table_name, registry, visible_ctes),
            })
        }
        _ => Ok(BroadPublicReadSetExpr::Other(expr.clone())),
    }
}

fn bind_broad_public_read_table_with_joins(
    table: &TableWithJoins,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<BroadPublicReadTableWithJoins, LixError> {
    Ok(BroadPublicReadTableWithJoins {
        original: table.clone(),
        relation: bind_broad_public_read_table_factor(&table.relation, registry, visible_ctes)?,
        joins: table
            .joins
            .iter()
            .map(|join| bind_broad_public_read_join(join, registry, visible_ctes))
            .collect::<Result<_, _>>()?,
    })
}

fn bind_broad_public_read_join(
    join: &sqlparser::ast::Join,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<BroadPublicReadJoin, LixError> {
    Ok(BroadPublicReadJoin {
        original: join.clone(),
        relation: bind_broad_public_read_table_factor(&join.relation, registry, visible_ctes)?,
    })
}

fn bind_broad_public_read_table_factor(
    relation: &TableFactor,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> Result<BroadPublicReadTableFactor, LixError> {
    match relation {
        TableFactor::Table { name, .. } => {
            let Some(relation_name) = table_name_terminal(name) else {
                return Ok(BroadPublicReadTableFactor::Other(relation.clone()));
            };
            Ok(BroadPublicReadTableFactor::Table {
                original: relation.clone(),
                relation: classify_broad_public_read_relation(
                    relation_name,
                    registry,
                    visible_ctes,
                ),
            })
        }
        TableFactor::Derived { subquery, .. } => Ok(BroadPublicReadTableFactor::Derived {
            original: relation.clone(),
            subquery: Box::new(bind_broad_public_read_query_scoped(
                subquery,
                registry,
                visible_ctes,
            )?),
        }),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => Ok(BroadPublicReadTableFactor::NestedJoin {
            original: relation.clone(),
            table_with_joins: Box::new(bind_broad_public_read_table_with_joins(
                table_with_joins,
                registry,
                visible_ctes,
            )?),
        }),
        _ => Ok(BroadPublicReadTableFactor::Other(relation.clone())),
    }
}

fn classify_broad_public_read_relation(
    relation_name: &str,
    registry: &SurfaceRegistry,
    visible_ctes: &BTreeSet<String>,
) -> BroadPublicReadRelation {
    let normalized = relation_name.to_ascii_lowercase();
    if visible_ctes.contains(&normalized) {
        return BroadPublicReadRelation::Cte(normalized);
    }
    if let Some(binding) = registry.bind_relation_name(relation_name) {
        return BroadPublicReadRelation::Public(binding);
    }
    if normalized.starts_with("lix_internal_") {
        return BroadPublicReadRelation::Internal(normalized);
    }
    BroadPublicReadRelation::External(normalized)
}

fn lower_broad_public_read_statement(
    statement: &BroadPublicReadStatement,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<RenderedBroadPublicReadStatement, LixError> {
    let mut substitutions = RenderRelationSubstitutionCollector::default();
    let shell_statement = lower_broad_public_read_statement_into_shell(
        statement,
        registry,
        dialect,
        active_version_id,
        known_live_layouts,
        &mut substitutions,
    )?;
    Ok(RenderedBroadPublicReadStatement {
        shell_statement,
        relation_render_nodes: substitutions.into_substitutions(),
    })
}

fn lower_broad_public_read_statement_into_shell(
    statement: &BroadPublicReadStatement,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<Statement, LixError> {
    match statement {
        BroadPublicReadStatement::Query(query) => {
            Ok(Statement::Query(Box::new(lower_broad_public_read_query(
                query,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?)))
        }
        BroadPublicReadStatement::Explain {
            original,
            statement: bound_statement,
        } => {
            let mut lowered = original.clone();
            if let Statement::Explain {
                statement: lowered_statement,
                ..
            } = &mut lowered
            {
                **lowered_statement = lower_broad_public_read_statement_into_shell(
                    bound_statement.as_ref(),
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?;
            }
            Ok(lowered)
        }
    }
}

fn lower_broad_public_read_query(
    query: &BroadPublicReadQuery,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<Query, LixError> {
    let mut lowered = query.original.clone();
    lowered.with = query
        .with
        .as_ref()
        .map(|with| {
            lower_broad_public_read_with(
                with,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )
        })
        .transpose()?;
    lowered.body = Box::new(lower_broad_public_read_set_expr(
        &query.body,
        registry,
        dialect,
        active_version_id,
        known_live_layouts,
        substitutions,
    )?);
    lower_nested_public_surfaces_in_query_expressions(
        &mut lowered,
        registry,
        dialect,
        active_version_id,
        known_live_layouts,
        substitutions,
    )?;
    Ok(lowered)
}

fn lower_broad_public_read_with(
    with: &BroadPublicReadWith,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<With, LixError> {
    let mut lowered = with.original.clone();
    for (cte, bound_query) in lowered.cte_tables.iter_mut().zip(&with.cte_tables) {
        cte.query = Box::new(lower_broad_public_read_query(
            bound_query,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
            substitutions,
        )?);
    }
    Ok(lowered)
}

fn lower_broad_public_read_set_expr(
    expr: &BroadPublicReadSetExpr,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<SetExpr, LixError> {
    match expr {
        BroadPublicReadSetExpr::Select(select) => {
            Ok(SetExpr::Select(Box::new(lower_broad_public_read_select(
                select,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?)))
        }
        BroadPublicReadSetExpr::Query(query) => {
            Ok(SetExpr::Query(Box::new(lower_broad_public_read_query(
                query,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?)))
        }
        BroadPublicReadSetExpr::SetOperation {
            original,
            left,
            right,
        } => {
            let mut lowered = original.clone();
            if let SetExpr::SetOperation {
                left: lowered_left,
                right: lowered_right,
                ..
            } = &mut lowered
            {
                *lowered_left = Box::new(lower_broad_public_read_set_expr(
                    left.as_ref(),
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?);
                *lowered_right = Box::new(lower_broad_public_read_set_expr(
                    right.as_ref(),
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?);
            }
            Ok(lowered)
        }
        BroadPublicReadSetExpr::Table { original, relation } => {
            lower_broad_public_read_table_relation(
                relation,
                original,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )
        }
        BroadPublicReadSetExpr::Other(expr) => {
            let mut lowered = expr.clone();
            lower_nested_public_surfaces_in_set_expr_expressions(
                &mut lowered,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?;
            Ok(lowered)
        }
    }
}

fn lower_broad_public_read_select(
    select: &BroadPublicReadSelect,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<Select, LixError> {
    let mut lowered = select.original.clone();
    lowered.from = select
        .from
        .iter()
        .map(|table| {
            lower_broad_public_read_table_with_joins(
                table,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )
        })
        .collect::<Result<_, _>>()?;
    lower_nested_public_surfaces_in_select_expressions(
        &mut lowered,
        registry,
        dialect,
        active_version_id,
        known_live_layouts,
        substitutions,
    )?;
    Ok(lowered)
}

fn lower_broad_public_read_table_with_joins(
    table: &BroadPublicReadTableWithJoins,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<TableWithJoins, LixError> {
    let mut lowered = table.original.clone();
    lowered.relation = lower_broad_public_read_table_factor(
        &table.relation,
        registry,
        dialect,
        active_version_id,
        known_live_layouts,
        substitutions,
    )?;
    lowered.joins = table
        .joins
        .iter()
        .map(|join| {
            lower_broad_public_read_join(
                join,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )
        })
        .collect::<Result<_, _>>()?;
    Ok(lowered)
}

fn lower_broad_public_read_join(
    join: &BroadPublicReadJoin,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<sqlparser::ast::Join, LixError> {
    let mut lowered = join.original.clone();
    lowered.relation = lower_broad_public_read_table_factor(
        &join.relation,
        registry,
        dialect,
        active_version_id,
        known_live_layouts,
        substitutions,
    )?;
    lower_nested_public_surfaces_in_join_operator(
        &mut lowered.join_operator,
        registry,
        dialect,
        active_version_id,
        known_live_layouts,
        substitutions,
    )?;
    Ok(lowered)
}

fn lower_broad_public_read_table_factor(
    relation: &BroadPublicReadTableFactor,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<TableFactor, LixError> {
    match relation {
        BroadPublicReadTableFactor::Table { original, relation } => {
            lower_broad_public_read_relation(
                relation,
                original,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )
        }
        BroadPublicReadTableFactor::Derived { original, subquery } => {
            let mut lowered = original.clone();
            if let TableFactor::Derived {
                subquery: lowered_subquery,
                ..
            } = &mut lowered
            {
                *lowered_subquery = Box::new(lower_broad_public_read_query(
                    subquery.as_ref(),
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?);
            }
            Ok(lowered)
        }
        BroadPublicReadTableFactor::NestedJoin {
            original,
            table_with_joins,
        } => {
            let mut lowered = original.clone();
            if let TableFactor::NestedJoin {
                table_with_joins: lowered_table_with_joins,
                ..
            } = &mut lowered
            {
                *lowered_table_with_joins = Box::new(lower_broad_public_read_table_with_joins(
                    table_with_joins.as_ref(),
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?);
            }
            Ok(lowered)
        }
        BroadPublicReadTableFactor::Other(relation) => Ok(relation.clone()),
    }
}

fn lower_broad_public_read_relation(
    relation: &BroadPublicReadRelation,
    original: &TableFactor,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<TableFactor, LixError> {
    match relation {
        BroadPublicReadRelation::Public(binding)
        | BroadPublicReadRelation::LoweredPublic(binding) => {
            let Some(source_sql) = build_supported_public_read_surface_sql(
                &binding.descriptor.public_name,
                registry,
                false,
                dialect,
                active_version_id,
                known_live_layouts,
            )?
            else {
                return Ok(original.clone());
            };
            let TableFactor::Table { alias, .. } = original else {
                return Ok(original.clone());
            };
            Ok(substitutions.replacement_table_factor(
                &binding.descriptor.public_name,
                alias.clone().or_else(|| {
                    Some(TableAlias {
                        explicit: true,
                        name: Ident::new(&binding.descriptor.public_name),
                        columns: Vec::new(),
                    })
                }),
                source_sql,
            ))
        }
        BroadPublicReadRelation::Internal(_)
        | BroadPublicReadRelation::External(_)
        | BroadPublicReadRelation::Cte(_) => Ok(original.clone()),
    }
}

fn lower_broad_public_read_table_relation(
    relation: &BroadPublicReadRelation,
    original: &SetExpr,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<SetExpr, LixError> {
    match relation {
        BroadPublicReadRelation::Public(binding)
        | BroadPublicReadRelation::LoweredPublic(binding) => {
            let Some(source_sql) = build_supported_public_read_surface_sql(
                &binding.descriptor.public_name,
                registry,
                true,
                dialect,
                active_version_id,
                known_live_layouts,
            )?
            else {
                return Ok(original.clone());
            };
            Ok(SetExpr::Query(Box::new(Query {
                with: None,
                body: Box::new(SetExpr::Select(Box::new(Select {
                    select_token: AttachedToken::empty(),
                    distinct: None,
                    top: None,
                    top_before_distinct: false,
                    projection: vec![SelectItem::Wildcard(Default::default())],
                    exclude: None,
                    into: None,
                    from: vec![TableWithJoins {
                        relation: substitutions.replacement_table_factor(
                            &binding.descriptor.public_name,
                            Some(TableAlias {
                                explicit: true,
                                name: Ident::new(&binding.descriptor.public_name),
                                columns: Vec::new(),
                            }),
                            source_sql,
                        ),
                        joins: Vec::new(),
                    }],
                    lateral_views: Vec::new(),
                    prewhere: None,
                    selection: None,
                    group_by: GroupByExpr::Expressions(Vec::new(), Vec::new()),
                    cluster_by: Vec::new(),
                    distribute_by: Vec::new(),
                    sort_by: Vec::new(),
                    having: None,
                    named_window: Vec::new(),
                    qualify: None,
                    window_before_qualify: false,
                    value_table_mode: None,
                    connect_by: None,
                    flavor: sqlparser::ast::SelectFlavor::Standard,
                }))),
                order_by: None,
                limit_clause: None,
                fetch: None,
                locks: Vec::new(),
                for_clause: None,
                settings: None,
                format_clause: None,
                pipe_operators: Vec::new(),
            })))
        }
        BroadPublicReadRelation::Internal(_)
        | BroadPublicReadRelation::External(_)
        | BroadPublicReadRelation::Cte(_) => Ok(original.clone()),
    }
}

fn lower_nested_public_surfaces_in_query_expressions(
    query: &mut Query,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<(), LixError> {
    if let Some(order_by) = &mut query.order_by {
        lower_nested_public_surfaces_in_order_by(
            order_by,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
            substitutions,
        )?;
    }
    if let Some(limit_clause) = &mut query.limit_clause {
        lower_nested_public_surfaces_in_limit_clause(
            limit_clause,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
            substitutions,
        )?;
    }
    if let Some(quantity) = query
        .fetch
        .as_mut()
        .and_then(|fetch| fetch.quantity.as_mut())
    {
        lower_nested_public_surfaces_in_expr(
            quantity,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
            substitutions,
        )?;
    }
    Ok(())
}

fn lower_nested_public_surfaces_in_set_expr_expressions(
    expr: &mut SetExpr,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<(), LixError> {
    match expr {
        SetExpr::Select(select) => lower_nested_public_surfaces_in_select_expressions(
            select,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
            substitutions,
        ),
        SetExpr::Query(query) => {
            *query = Box::new(lower_query_via_broad_binding(
                query.as_ref(),
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?);
            Ok(())
        }
        SetExpr::SetOperation { left, right, .. } => {
            lower_nested_public_surfaces_in_set_expr_expressions(
                left,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?;
            lower_nested_public_surfaces_in_set_expr_expressions(
                right,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )
        }
        SetExpr::Values(values) => {
            for row in &mut values.rows {
                for expr in row {
                    lower_nested_public_surfaces_in_expr(
                        expr,
                        registry,
                        dialect,
                        active_version_id,
                        known_live_layouts,
                        substitutions,
                    )?;
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn lower_nested_public_surfaces_in_select_expressions(
    select: &mut Select,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<(), LixError> {
    if let Some(prewhere) = &mut select.prewhere {
        lower_nested_public_surfaces_in_expr(
            prewhere,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
            substitutions,
        )?;
    }
    if let Some(selection) = &mut select.selection {
        lower_nested_public_surfaces_in_expr(
            selection,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
            substitutions,
        )?;
    }
    for item in &mut select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                lower_nested_public_surfaces_in_expr(
                    expr,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?;
            }
            SelectItem::QualifiedWildcard(
                sqlparser::ast::SelectItemQualifiedWildcardKind::Expr(expr),
                _,
            ) => lower_nested_public_surfaces_in_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?,
            _ => {}
        }
    }
    match &mut select.group_by {
        GroupByExpr::All(_) => {}
        GroupByExpr::Expressions(expressions, _) => {
            for expr in expressions {
                lower_nested_public_surfaces_in_expr(
                    expr,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?;
            }
        }
    }
    for expr in &mut select.cluster_by {
        lower_nested_public_surfaces_in_expr(
            expr,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
            substitutions,
        )?;
    }
    for expr in &mut select.distribute_by {
        lower_nested_public_surfaces_in_expr(
            expr,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
            substitutions,
        )?;
    }
    for expr in &mut select.sort_by {
        lower_nested_public_surfaces_in_order_by_expr(
            expr,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
            substitutions,
        )?;
    }
    if let Some(having) = &mut select.having {
        lower_nested_public_surfaces_in_expr(
            having,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
            substitutions,
        )?;
    }
    if let Some(qualify) = &mut select.qualify {
        lower_nested_public_surfaces_in_expr(
            qualify,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
            substitutions,
        )?;
    }
    if let Some(connect_by) = &mut select.connect_by {
        lower_nested_public_surfaces_in_expr(
            &mut connect_by.condition,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
            substitutions,
        )?;
        for expr in &mut connect_by.relationships {
            lower_nested_public_surfaces_in_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?;
        }
    }
    Ok(())
}

fn lower_nested_public_surfaces_in_order_by(
    order_by: &mut OrderBy,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<(), LixError> {
    match &mut order_by.kind {
        sqlparser::ast::OrderByKind::All(_) => Ok(()),
        sqlparser::ast::OrderByKind::Expressions(expressions) => {
            for expr in expressions {
                lower_nested_public_surfaces_in_order_by_expr(
                    expr,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?;
            }
            Ok(())
        }
    }
}

fn lower_nested_public_surfaces_in_order_by_expr(
    order_by_expr: &mut OrderByExpr,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<(), LixError> {
    lower_nested_public_surfaces_in_expr(
        &mut order_by_expr.expr,
        registry,
        dialect,
        active_version_id,
        known_live_layouts,
        substitutions,
    )?;
    if let Some(with_fill) = &mut order_by_expr.with_fill {
        if let Some(from) = &mut with_fill.from {
            lower_nested_public_surfaces_in_expr(
                from,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?;
        }
        if let Some(to) = &mut with_fill.to {
            lower_nested_public_surfaces_in_expr(
                to,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?;
        }
        if let Some(step) = &mut with_fill.step {
            lower_nested_public_surfaces_in_expr(
                step,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?;
        }
    }
    Ok(())
}

fn lower_nested_public_surfaces_in_limit_clause(
    limit_clause: &mut LimitClause,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<(), LixError> {
    match limit_clause {
        LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            if let Some(limit) = limit {
                lower_nested_public_surfaces_in_expr(
                    limit,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?;
            }
            if let Some(offset) = offset {
                lower_nested_public_surfaces_in_expr(
                    &mut offset.value,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?;
            }
            for expr in limit_by {
                lower_nested_public_surfaces_in_expr(
                    expr,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?;
            }
            Ok(())
        }
        LimitClause::OffsetCommaLimit { offset, limit } => {
            lower_nested_public_surfaces_in_expr(
                offset,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?;
            lower_nested_public_surfaces_in_expr(
                limit,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )
        }
    }
}

fn lower_nested_public_surfaces_in_join_operator(
    join_operator: &mut JoinOperator,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<(), LixError> {
    let (match_condition, constraint) = match join_operator {
        JoinOperator::AsOf {
            match_condition,
            constraint,
        } => (Some(match_condition), Some(constraint)),
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
        | JoinOperator::StraightJoin(constraint) => (None, Some(constraint)),
        JoinOperator::CrossApply | JoinOperator::OuterApply => (None, None),
    };
    if let Some(expr) = match_condition {
        lower_nested_public_surfaces_in_expr(
            expr,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
            substitutions,
        )?;
    }
    if let Some(constraint) = constraint {
        lower_nested_public_surfaces_in_join_constraint(
            constraint,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
            substitutions,
        )?;
    }
    Ok(())
}

fn lower_nested_public_surfaces_in_join_constraint(
    constraint: &mut JoinConstraint,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<(), LixError> {
    match constraint {
        JoinConstraint::On(expr) => lower_nested_public_surfaces_in_expr(
            expr,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
            substitutions,
        ),
        _ => Ok(()),
    }
}

fn lower_nested_public_surfaces_in_expr(
    expr: &mut Expr,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<(), LixError> {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            lower_nested_public_surfaces_in_expr(
                left,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?;
            lower_nested_public_surfaces_in_expr(
                right,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. } => lower_nested_public_surfaces_in_expr(
            expr,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
            substitutions,
        ),
        Expr::InList { expr, list, .. } => {
            lower_nested_public_surfaces_in_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?;
            for item in list {
                lower_nested_public_surfaces_in_expr(
                    item,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?;
            }
            Ok(())
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            lower_nested_public_surfaces_in_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?;
            lower_nested_public_surfaces_in_expr(
                low,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?;
            lower_nested_public_surfaces_in_expr(
                high,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            lower_nested_public_surfaces_in_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?;
            lower_nested_public_surfaces_in_expr(
                pattern,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )
        }
        Expr::Subquery(query) => {
            *query = Box::new(lower_query_via_broad_binding(
                query.as_ref(),
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?);
            Ok(())
        }
        Expr::Exists { subquery, .. } => {
            *subquery = Box::new(lower_query_via_broad_binding(
                subquery.as_ref(),
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?);
            Ok(())
        }
        Expr::InSubquery { expr, subquery, .. } => {
            lower_nested_public_surfaces_in_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?;
            *subquery = Box::new(lower_query_via_broad_binding(
                subquery.as_ref(),
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?);
            Ok(())
        }
        Expr::InUnnest {
            expr, array_expr, ..
        } => {
            lower_nested_public_surfaces_in_expr(
                expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?;
            lower_nested_public_surfaces_in_expr(
                array_expr,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )
        }
        Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
            lower_nested_public_surfaces_in_expr(
                left,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )?;
            lower_nested_public_surfaces_in_expr(
                right,
                registry,
                dialect,
                active_version_id,
                known_live_layouts,
                substitutions,
            )
        }
        Expr::Function(function) => lower_nested_public_surfaces_in_function_args(
            &mut function.args,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
            substitutions,
        ),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                lower_nested_public_surfaces_in_expr(
                    operand,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?;
            }
            for condition in conditions {
                lower_nested_public_surfaces_in_expr(
                    &mut condition.condition,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?;
                lower_nested_public_surfaces_in_expr(
                    &mut condition.result,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?;
            }
            if let Some(else_result) = else_result {
                lower_nested_public_surfaces_in_expr(
                    else_result,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?;
            }
            Ok(())
        }
        Expr::Tuple(items) => {
            for item in items {
                lower_nested_public_surfaces_in_expr(
                    item,
                    registry,
                    dialect,
                    active_version_id,
                    known_live_layouts,
                    substitutions,
                )?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn lower_nested_public_surfaces_in_function_args(
    args: &mut FunctionArguments,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<(), LixError> {
    match args {
        FunctionArguments::List(list) => {
            for arg in &mut list.args {
                match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                        lower_nested_public_surfaces_in_expr(
                            expr,
                            registry,
                            dialect,
                            active_version_id,
                            known_live_layouts,
                            substitutions,
                        )?;
                    }
                    FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => {
                        if let FunctionArgExpr::Expr(expr) = arg {
                            lower_nested_public_surfaces_in_expr(
                                expr,
                                registry,
                                dialect,
                                active_version_id,
                                known_live_layouts,
                                substitutions,
                            )?;
                        }
                    }
                    _ => {}
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn lower_query_via_broad_binding(
    query: &Query,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<Query, LixError> {
    let bound = bind_broad_public_read_query_scoped(query, registry, &BTreeSet::new())?;
    lower_broad_public_read_query(
        &bound,
        registry,
        dialect,
        active_version_id,
        known_live_layouts,
        substitutions,
    )
}

fn build_supported_public_read_surface_sql(
    surface_name: &str,
    registry: &SurfaceRegistry,
    _top_level: bool,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<Option<String>, LixError> {
    let Some(surface_binding) = registry.bind_relation_name(surface_name) else {
        return Ok(None);
    };

    match surface_binding.descriptor.surface_family {
        SurfaceFamily::State => build_public_state_surface_sql(
            &surface_binding,
            registry,
            dialect,
            active_version_id,
            known_live_layouts,
        ),
        SurfaceFamily::Entity => build_entity_surface_sql_for_broad_lowering(
            dialect,
            &surface_binding,
            active_version_id,
            known_live_layouts,
        ),
        SurfaceFamily::Filesystem => build_nested_filesystem_surface_sql(
            dialect,
            active_version_id,
            &surface_binding.descriptor.public_name,
        ),
        SurfaceFamily::Admin => build_public_admin_surface_sql(dialect, &surface_binding),
        SurfaceFamily::Change => {
            build_public_change_surface_sql(&surface_binding, active_version_id)
        }
    }
}

fn build_public_state_surface_sql(
    surface_binding: &SurfaceBinding,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<Option<String>, LixError> {
    let Some(state_scan) = CanonicalStateScan::from_surface_binding(surface_binding.clone()) else {
        return Ok(None);
    };
    let schema_set: BTreeSet<String> = registry
        .registered_state_surface_schema_keys()
        .into_iter()
        .collect();
    let request = EffectiveStateRequest {
        schema_set,
        version_scope: state_scan.version_scope,
        include_global_overlay: true,
        include_untracked_overlay: true,
        include_tombstones: state_scan.include_tombstones,
        predicate_classes: Vec::new(),
        required_columns: surface_binding
            .descriptor
            .visible_columns
            .iter()
            .chain(surface_binding.descriptor.hidden_columns.iter())
            .cloned()
            .collect(),
    };
    if state_scan.version_scope == VersionScope::ActiveVersion && active_version_id.is_none() {
        return Ok(None);
    }
    build_state_source_sql(
        dialect,
        active_version_id,
        surface_binding,
        &request,
        &[],
        known_live_layouts,
    )
}

fn build_public_admin_surface_sql(
    dialect: SqlDialect,
    surface_binding: &SurfaceBinding,
) -> Result<Option<String>, LixError> {
    let Some(admin_scan) = CanonicalAdminScan::from_surface_binding(surface_binding.clone()) else {
        return Ok(None);
    };
    build_admin_source_sql(admin_scan.kind, dialect).map(Some)
}

fn build_public_change_surface_sql(
    surface_binding: &SurfaceBinding,
    active_version_id: Option<&str>,
) -> Result<Option<String>, LixError> {
    if CanonicalWorkingChangesScan::from_surface_binding(surface_binding.clone()).is_some() {
        let Some(active_version_id) = active_version_id else {
            return Ok(None);
        };
        return Ok(Some(build_working_changes_source_sql(active_version_id)));
    }
    if CanonicalChangeScan::from_surface_binding(surface_binding.clone()).is_some() {
        return Ok(Some(build_change_source_sql()));
    }
    Ok(None)
}

fn build_entity_surface_sql_for_broad_lowering(
    dialect: SqlDialect,
    surface_binding: &SurfaceBinding,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<Option<String>, LixError> {
    let Some(schema_key) = surface_binding.implicit_overrides.fixed_schema_key.clone() else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "public-surface lowering requires fixed schema binding for '{}'",
                surface_binding.descriptor.public_name
            ),
        });
    };
    if builtin_schema_definition(&schema_key).is_none()
        && !known_live_layouts.contains_key(&schema_key)
    {
        return Ok(None);
    }
    let Some(state_scan) = CanonicalStateScan::from_surface_binding(surface_binding.clone()) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "public-surface lowering could not build canonical state scan for '{}'",
                surface_binding.descriptor.public_name
            ),
        });
    };
    let request = EffectiveStateRequest {
        schema_set: BTreeSet::from([schema_key]),
        version_scope: state_scan.version_scope,
        include_global_overlay: true,
        include_untracked_overlay: true,
        include_tombstones: state_scan.include_tombstones,
        predicate_classes: Vec::new(),
        required_columns: surface_binding
            .descriptor
            .visible_columns
            .iter()
            .chain(surface_binding.descriptor.hidden_columns.iter())
            .cloned()
            .collect(),
    };
    if state_scan.version_scope == VersionScope::ActiveVersion && active_version_id.is_none() {
        return Ok(None);
    }
    Ok(Some(
        build_entity_source_sql(
            dialect,
            active_version_id,
            surface_binding,
            &request,
            &[],
            known_live_layouts,
        )?
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "public-surface lowering could not lower entity surface '{}'",
                surface_binding.descriptor.public_name
            ),
        })?,
    ))
}
