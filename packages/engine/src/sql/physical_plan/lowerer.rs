use crate::common::errors::sql_unknown_column_error;
use crate::contracts::artifacts::EffectiveStateRequest;
use crate::contracts::surface::{
    SurfaceBinding, SurfaceColumnType, SurfaceFamily, SurfaceOverridePredicate,
    SurfaceOverrideValue, SurfaceRegistry, SurfaceVariant,
};
use crate::sql::common::pushdown::{PushdownDecision, PushdownSupport, RejectedPredicate};
use crate::sql::logical_plan::public_ir::{
    BroadPublicReadStatement, CanonicalAdminKind, CanonicalAdminScan, CanonicalChangeScan,
    CanonicalStateScan, CanonicalWorkingChangesScan, FilesystemKind, ReadPlan,
    StructuredPublicRead, VersionScope,
};
use crate::sql::physical_plan::plan::{
    compile_final_read_statement, compile_lowered_read_statement,
    compile_terminal_read_statement_from_template, FilesystemPublicSurface, LoweredReadProgram,
    LoweredResultColumn, LoweredResultColumns, TerminalRelationRenderNode,
};
use crate::sql::physical_plan::public_surface_sql_support::{
    entity_surface_has_live_payload_collisions, entity_surface_payload_alias,
    entity_surface_uses_payload_alias, escape_sql_string, render_identifier,
};
use crate::sql::physical_plan::source_sql::{
    build_effective_public_read_source_sql, build_working_changes_public_read_source_sql,
};
use crate::sql::semantic_ir::semantics::effective_state_resolver::EffectiveStatePlan;
use crate::surface_sql::filesystem::{
    build_filesystem_directory_projection_sql, build_filesystem_file_projection_sql,
};
use crate::surface_sql::version::{
    build_admin_version_source_sql, build_admin_version_source_sql_with_current_heads,
};
use crate::{LixError, SqlDialect};
use serde_json::Value as JsonValue;
use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, Ident, OrderBy, OrderByKind, Query, Select, SelectItem,
    SetExpr, Statement, TableAlias, TableFactor, TableWithJoins,
};
use sqlparser::ast::{ObjectName, ObjectNamePart};
use sqlparser::ast::{Value as SqlValue, Visit, Visitor};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RenderRelationSubstitutionCollector {
    next_relation_id: usize,
    substitutions: Vec<TerminalRelationRenderNode>,
}

impl RenderRelationSubstitutionCollector {
    fn into_substitutions(self) -> Vec<TerminalRelationRenderNode> {
        self.substitutions
    }

    fn replacement_table_factor(
        &mut self,
        relation_name: &str,
        alias: Option<TableAlias>,
        replacement_sql: String,
    ) -> TableFactor {
        let alias = alias.unwrap_or_else(|| TableAlias {
            explicit: true,
            name: Ident::new(relation_name),
            columns: Vec::new(),
        });
        let placeholder_name = format!("__lix_lowered_relation_{}", self.next_relation_id);
        self.next_relation_id += 1;

        let placeholder_factor = TableFactor::Table {
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
                &placeholder_name,
            ))]),
            alias: Some(alias.clone()),
            args: None,
            with_hints: vec![],
            version: None,
            with_ordinality: false,
            partitions: vec![],
            json_path: None,
            sample: None,
            index_hints: vec![],
        };

        self.substitutions.push(TerminalRelationRenderNode {
            placeholder_relation_name: placeholder_name,
            alias: alias.clone(),
            rendered_factor_sql: format!("({replacement_sql}) {}", alias),
        });

        placeholder_factor
    }
}

mod broad;

pub(crate) fn broad_public_relation_supports_terminal_render(
    binding: &SurfaceBinding,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<bool, LixError> {
    broad::broad_public_relation_supports_terminal_render(
        binding,
        registry,
        dialect,
        active_version_id,
        known_live_layouts,
    )
}

pub(crate) fn lower_broad_public_read_for_execution_with_layouts(
    statement: &BroadPublicReadStatement,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    params_len: usize,
    active_version_id: Option<&str>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<Option<LoweredReadProgram>, LixError> {
    broad::lower_broad_public_read_for_execution(
        statement,
        registry,
        dialect,
        params_len,
        active_version_id,
        known_live_layouts,
    )
}

pub(crate) fn lower_read_for_execution_with_layouts(
    dialect: SqlDialect,
    structured_read: &StructuredPublicRead,
    effective_state_request: Option<&EffectiveStateRequest>,
    effective_state_plan: Option<&EffectiveStatePlan>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    current_version_heads: &BTreeMap<String, String>,
) -> Result<Option<LoweredReadProgram>, LixError> {
    let result_columns = lowered_result_columns(structured_read);
    match structured_read.surface_binding.descriptor.surface_family {
        SurfaceFamily::State => {
            let Some(effective_state_request) = effective_state_request else {
                return Ok(None);
            };
            let Some(effective_state_plan) = effective_state_plan else {
                return Ok(None);
            };
            lower_state_read_for_execution(
                dialect,
                structured_read,
                effective_state_request,
                effective_state_plan,
                known_live_layouts,
            )
            .map(|program| {
                program.map(|mut program| {
                    program.pushdown_decision = build_pushdown_decision(effective_state_plan);
                    program.result_columns = result_columns.clone();
                    program
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
            lower_entity_read_for_execution(
                dialect,
                structured_read,
                effective_state_request,
                effective_state_plan,
                known_live_layouts,
            )
            .map(|program| {
                program.map(|mut program| {
                    program.pushdown_decision = build_pushdown_decision(effective_state_plan);
                    program.result_columns = result_columns.clone();
                    program
                })
            })
        }
        SurfaceFamily::Change => {
            lower_change_read_for_execution(dialect, structured_read).map(|program| {
                let pushdown_decision =
                    if canonical_working_changes_scan(&structured_read.read_command.root).is_some()
                    {
                        working_changes_pushdown_decision(structured_read)
                    } else {
                        change_pushdown_decision(structured_read)
                    };
                program.map(|mut program| {
                    program.pushdown_decision = pushdown_decision;
                    program.result_columns = result_columns.clone();
                    program
                })
            })
        }
        SurfaceFamily::Admin => {
            lower_admin_read_for_execution(dialect, structured_read, current_version_heads).map(
                |program| {
                    program.map(|mut program| {
                        program.pushdown_decision = admin_pushdown_decision(structured_read);
                        program.result_columns = result_columns.clone();
                        program
                    })
                },
            )
        }
        SurfaceFamily::Filesystem => lower_filesystem_read_for_execution(dialect, structured_read)
            .map(|program| {
                program.map(|mut program| {
                    program.pushdown_decision = filesystem_pushdown_decision(structured_read);
                    program.result_columns = result_columns.clone();
                    program
                })
            }),
    }
}

fn lower_state_read_for_execution(
    dialect: SqlDialect,
    canonicalized: &StructuredPublicRead,
    effective_state_request: &EffectiveStateRequest,
    effective_state_plan: &EffectiveStatePlan,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<Option<LoweredReadProgram>, LixError> {
    if let Some(error) =
        state_read_exposed_column_error(&canonicalized.surface_binding, effective_state_request)
    {
        return Err(error);
    }

    let (pushdown_predicates, residual_selection) =
        if entity_surface_has_live_payload_collisions(&canonicalized.surface_binding) {
            (Vec::new(), canonicalized.query.selection.clone())
        } else {
            split_state_selection_for_pushdown(
                canonicalized.query.selection.as_ref(),
                effective_state_plan,
            )
        };
    let Some(source_sql) = build_state_source_sql(
        dialect,
        canonicalized.requested_version_id.as_deref(),
        &canonicalized.surface_binding,
        effective_state_request,
        &pushdown_predicates,
        known_live_layouts,
    )?
    else {
        return Ok(None);
    };
    build_lowered_read_program(dialect, canonicalized, source_sql, residual_selection).map(Some)
}

fn build_lowered_read_program(
    dialect: SqlDialect,
    structured_read: &StructuredPublicRead,
    source_sql: String,
    selection: Option<Expr>,
) -> Result<LoweredReadProgram, LixError> {
    let mut projection = structured_read.query.projection.clone();
    let active_version_id = structured_read.requested_version_id.as_deref();
    let mut substitutions = RenderRelationSubstitutionCollector::default();
    rewrite_nested_filesystem_surfaces_in_select_items(
        dialect,
        active_version_id,
        &mut projection,
        &mut substitutions,
    )?;

    let mut selection = selection;
    if let Some(selection) = &mut selection {
        rewrite_nested_filesystem_surfaces_in_expr(
            dialect,
            active_version_id,
            selection,
            &mut substitutions,
        )?;
    }

    let mut order_by = structured_read.query.order_by.clone();
    rewrite_nested_filesystem_surfaces_in_order_by(
        dialect,
        active_version_id,
        order_by.as_mut(),
        &mut substitutions,
    )?;

    let derived_alias = structured_read.query.source_alias.clone().or_else(|| {
        Some(TableAlias {
            explicit: true,
            name: Ident::new(&structured_read.surface_binding.descriptor.public_name),
            columns: Vec::new(),
        })
    });

    let relation = substitutions.replacement_table_factor(
        &structured_read.surface_binding.descriptor.public_name,
        derived_alias,
        source_sql,
    );

    let statement = Statement::Query(Box::new(Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(Select {
            select_token: AttachedToken::empty(),
            distinct: None,
            top: None,
            top_before_distinct: false,
            projection,
            exclude: None,
            into: None,
            from: vec![TableWithJoins {
                relation,
                joins: Vec::new(),
            }],
            lateral_views: Vec::new(),
            prewhere: None,
            selection,
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
        order_by,
        limit_clause: structured_read.query.limit_clause.clone(),
        fetch: None,
        locks: Vec::new(),
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: Vec::new(),
    }));
    Ok(LoweredReadProgram {
        statements: vec![compile_lowered_read_statement(
            dialect,
            structured_read.bound_parameters.len(),
            statement,
            substitutions.into_substitutions(),
        )?],
        pushdown_decision: PushdownDecision::default(),
        result_columns: LoweredResultColumns::Static(Vec::new()),
    })
}

fn lowered_result_columns(structured_read: &StructuredPublicRead) -> LoweredResultColumns {
    if structured_read.query.uses_wildcard_projection() {
        let columns = structured_read
            .surface_binding
            .column_types
            .iter()
            .map(|(name, column_type)| {
                (
                    name.clone(),
                    lowered_result_column_from_surface_type(*column_type),
                )
            })
            .collect();
        return LoweredResultColumns::ByColumnName(columns);
    }

    LoweredResultColumns::Static(
        structured_read
            .query
            .projection
            .iter()
            .map(|item| {
                lowered_result_column_for_projection_item(item, &structured_read.surface_binding)
            })
            .collect(),
    )
}

fn lowered_result_column_for_projection_item(
    item: &SelectItem,
    surface_binding: &SurfaceBinding,
) -> LoweredResultColumn {
    let expr = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
            return LoweredResultColumn::Untyped;
        }
    };

    let column = match expr {
        Expr::Identifier(ident) => ident.value.as_str(),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|part| part.value.as_str())
            .unwrap_or_default(),
        _ => return LoweredResultColumn::Untyped,
    };

    surface_column_type(surface_binding, column)
        .map(lowered_result_column_from_surface_type)
        .unwrap_or(LoweredResultColumn::Untyped)
}

fn lowered_result_column_from_surface_type(column_type: SurfaceColumnType) -> LoweredResultColumn {
    match column_type {
        SurfaceColumnType::Boolean => LoweredResultColumn::Boolean,
        SurfaceColumnType::String
        | SurfaceColumnType::Integer
        | SurfaceColumnType::Number
        | SurfaceColumnType::Json => LoweredResultColumn::Untyped,
    }
}

fn surface_column_type(
    surface_binding: &SurfaceBinding,
    column: &str,
) -> Option<SurfaceColumnType> {
    surface_binding
        .column_types
        .iter()
        .find_map(|(candidate, kind)| candidate.eq_ignore_ascii_case(column).then_some(*kind))
}

fn state_read_exposed_column_error(
    surface_binding: &SurfaceBinding,
    effective_state_request: &EffectiveStateRequest,
) -> Option<LixError> {
    let exposed = surface_binding
        .exposed_columns
        .iter()
        .map(|column| column.to_ascii_lowercase())
        .collect::<std::collections::BTreeSet<_>>();
    let missing = effective_state_request
        .required_columns
        .iter()
        .filter(|column| !exposed.contains(&column.to_ascii_lowercase()))
        .cloned()
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return None;
    }
    if surface_binding.descriptor.public_name == "lix_state"
        && missing
            .iter()
            .any(|column| matches!(column.as_str(), "version_id" | "lixcol_version_id"))
    {
        return Some(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description:
                "lix_state does not expose version_id; use lix_state_by_version for explicit version filters"
                    .to_string(),
        });
    }
    let column = missing[0].clone();
    let available = surface_binding
        .exposed_columns
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    Some(sql_unknown_column_error(
        &column,
        Some(&surface_binding.descriptor.public_name),
        &available,
        None,
    ))
}

fn lower_entity_read_for_execution(
    dialect: SqlDialect,
    canonicalized: &StructuredPublicRead,
    effective_state_request: &EffectiveStateRequest,
    effective_state_plan: &EffectiveStatePlan,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<Option<LoweredReadProgram>, LixError> {
    if canonicalized.query.uses_wildcard_projection() {
        return Ok(None);
    }

    let (pushdown_predicates, residual_selection) = split_state_selection_for_pushdown(
        canonicalized.query.selection.as_ref(),
        effective_state_plan,
    );
    let Some(source_sql) = build_entity_source_sql(
        dialect,
        canonicalized.requested_version_id.as_deref(),
        &canonicalized.surface_binding,
        effective_state_request,
        &pushdown_predicates,
        known_live_layouts,
    )?
    else {
        return Ok(None);
    };
    build_lowered_read_program(dialect, canonicalized, source_sql, residual_selection).map(Some)
}

fn lower_change_read_for_execution(
    dialect: SqlDialect,
    canonicalized: &StructuredPublicRead,
) -> Result<Option<LoweredReadProgram>, LixError> {
    if canonical_working_changes_scan(&canonicalized.read_command.root).is_some() {
        return lower_working_changes_read_for_execution(dialect, canonicalized);
    }

    build_lowered_read_program(
        dialect,
        canonicalized,
        build_change_source_sql(),
        canonicalized.query.selection.clone(),
    )
    .map(Some)
}

fn lower_working_changes_read_for_execution(
    dialect: SqlDialect,
    canonicalized: &StructuredPublicRead,
) -> Result<Option<LoweredReadProgram>, LixError> {
    let active_version_id = canonicalized
        .requested_version_id
        .as_deref()
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "public read '{}' requires a session-requested active version id",
                    canonicalized.surface_binding.descriptor.public_name
                ),
            )
        })?;
    build_lowered_read_program(
        dialect,
        canonicalized,
        build_working_changes_public_read_source_sql(dialect, active_version_id),
        canonicalized.query.selection.clone(),
    )
    .map(Some)
}

fn rewrite_nested_filesystem_surfaces_in_query(
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    query: &mut Query,
    top_level: bool,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<(), LixError> {
    rewrite_nested_filesystem_surfaces_in_set_expr(
        dialect,
        active_version_id,
        query.body.as_mut(),
        top_level,
        substitutions,
    )?;
    Ok(())
}

fn rewrite_nested_filesystem_surfaces_in_set_expr(
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    expr: &mut SetExpr,
    top_level: bool,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<(), LixError> {
    match expr {
        SetExpr::Select(select) => rewrite_nested_filesystem_surfaces_in_select(
            dialect,
            active_version_id,
            select,
            top_level,
            substitutions,
        ),
        SetExpr::Query(query) => rewrite_nested_filesystem_surfaces_in_query(
            dialect,
            active_version_id,
            query,
            false,
            substitutions,
        ),
        SetExpr::SetOperation { left, right, .. } => {
            rewrite_nested_filesystem_surfaces_in_set_expr(
                dialect,
                active_version_id,
                left.as_mut(),
                top_level,
                substitutions,
            )?;
            rewrite_nested_filesystem_surfaces_in_set_expr(
                dialect,
                active_version_id,
                right.as_mut(),
                top_level,
                substitutions,
            )
        }
        _ => Ok(()),
    }
}

fn rewrite_nested_filesystem_surfaces_in_select(
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    select: &mut Select,
    top_level: bool,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<(), LixError> {
    for table in &mut select.from {
        rewrite_nested_filesystem_surfaces_in_table_with_joins(
            dialect,
            active_version_id,
            table,
            top_level,
            substitutions,
        )?;
    }
    if let Some(selection) = &mut select.selection {
        rewrite_nested_filesystem_surfaces_in_expr(
            dialect,
            active_version_id,
            selection,
            substitutions,
        )?;
    }
    for item in &mut select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                rewrite_nested_filesystem_surfaces_in_expr(
                    dialect,
                    active_version_id,
                    expr,
                    substitutions,
                )?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn rewrite_nested_filesystem_surfaces_in_select_items(
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    projection: &mut [SelectItem],
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<(), LixError> {
    for item in projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                rewrite_nested_filesystem_surfaces_in_expr(
                    dialect,
                    active_version_id,
                    expr,
                    substitutions,
                )?;
            }
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {}
        }
    }
    Ok(())
}

fn rewrite_nested_filesystem_surfaces_in_order_by(
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    order_by: Option<&mut OrderBy>,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<(), LixError> {
    let Some(order_by) = order_by else {
        return Ok(());
    };
    let OrderByKind::Expressions(ordering) = &mut order_by.kind else {
        return Ok(());
    };
    for item in ordering {
        rewrite_nested_filesystem_surfaces_in_expr(
            dialect,
            active_version_id,
            &mut item.expr,
            substitutions,
        )?;
    }
    Ok(())
}

fn rewrite_nested_filesystem_surfaces_in_table_with_joins(
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    table: &mut TableWithJoins,
    top_level: bool,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<(), LixError> {
    rewrite_nested_filesystem_surfaces_in_table_factor(
        dialect,
        active_version_id,
        &mut table.relation,
        top_level,
        substitutions,
    )?;
    for join in &mut table.joins {
        rewrite_nested_filesystem_surfaces_in_table_factor(
            dialect,
            active_version_id,
            &mut join.relation,
            top_level,
            substitutions,
        )?;
    }
    Ok(())
}

fn rewrite_nested_filesystem_surfaces_in_table_factor(
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    relation: &mut TableFactor,
    top_level: bool,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<(), LixError> {
    match relation {
        TableFactor::Table { name, alias, .. } => {
            let Some(surface_name) = table_name_terminal(name) else {
                return Ok(());
            };
            if top_level || !is_rewriteable_filesystem_public_surface_name(surface_name) {
                return Ok(());
            }
            let Some(source_sql) =
                build_nested_filesystem_surface_sql(dialect, active_version_id, surface_name)?
            else {
                return Ok(());
            };
            *relation = substitutions.replacement_table_factor(
                surface_name,
                alias.clone().or_else(|| {
                    Some(TableAlias {
                        explicit: true,
                        name: Ident::new(surface_name),
                        columns: Vec::new(),
                    })
                }),
                source_sql,
            );
            Ok(())
        }
        TableFactor::Derived { subquery, .. } => rewrite_nested_filesystem_surfaces_in_query(
            dialect,
            active_version_id,
            subquery,
            false,
            substitutions,
        ),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => rewrite_nested_filesystem_surfaces_in_table_with_joins(
            dialect,
            active_version_id,
            table_with_joins,
            false,
            substitutions,
        ),
        _ => Ok(()),
    }
}

fn rewrite_nested_filesystem_surfaces_in_expr(
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    expr: &mut Expr,
    substitutions: &mut RenderRelationSubstitutionCollector,
) -> Result<(), LixError> {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            rewrite_nested_filesystem_surfaces_in_expr(
                dialect,
                active_version_id,
                left,
                substitutions,
            )?;
            rewrite_nested_filesystem_surfaces_in_expr(
                dialect,
                active_version_id,
                right,
                substitutions,
            )
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. } => rewrite_nested_filesystem_surfaces_in_expr(
            dialect,
            active_version_id,
            expr,
            substitutions,
        ),
        Expr::InList { expr, list, .. } => {
            rewrite_nested_filesystem_surfaces_in_expr(
                dialect,
                active_version_id,
                expr,
                substitutions,
            )?;
            for item in list {
                rewrite_nested_filesystem_surfaces_in_expr(
                    dialect,
                    active_version_id,
                    item,
                    substitutions,
                )?;
            }
            Ok(())
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            rewrite_nested_filesystem_surfaces_in_expr(
                dialect,
                active_version_id,
                expr,
                substitutions,
            )?;
            rewrite_nested_filesystem_surfaces_in_expr(
                dialect,
                active_version_id,
                low,
                substitutions,
            )?;
            rewrite_nested_filesystem_surfaces_in_expr(
                dialect,
                active_version_id,
                high,
                substitutions,
            )
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            rewrite_nested_filesystem_surfaces_in_expr(
                dialect,
                active_version_id,
                expr,
                substitutions,
            )?;
            rewrite_nested_filesystem_surfaces_in_expr(
                dialect,
                active_version_id,
                pattern,
                substitutions,
            )
        }
        Expr::Subquery(query) => rewrite_nested_filesystem_surfaces_in_query(
            dialect,
            active_version_id,
            query,
            false,
            substitutions,
        ),
        Expr::Exists { subquery, .. } => rewrite_nested_filesystem_surfaces_in_query(
            dialect,
            active_version_id,
            subquery,
            false,
            substitutions,
        ),
        Expr::InSubquery { expr, subquery, .. } => {
            rewrite_nested_filesystem_surfaces_in_expr(
                dialect,
                active_version_id,
                expr,
                substitutions,
            )?;
            rewrite_nested_filesystem_surfaces_in_query(
                dialect,
                active_version_id,
                subquery,
                false,
                substitutions,
            )
        }
        _ => Ok(()),
    }
}

fn build_nested_filesystem_surface_sql(
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    surface_name: &str,
) -> Result<Option<String>, LixError> {
    let Some(surface) = FilesystemPublicSurface::from_public_name(surface_name) else {
        return Ok(None);
    };
    Ok(Some(build_filesystem_surface_sql(
        dialect,
        active_version_id,
        surface,
    )?))
}

fn table_name_terminal(name: &sqlparser::ast::ObjectName) -> Option<&str> {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .map(|ident| ident.value.as_str())
}

fn is_rewriteable_filesystem_public_surface_name(name: &str) -> bool {
    FilesystemPublicSurface::from_public_name(name).is_some()
}

fn lower_admin_read_for_execution(
    dialect: SqlDialect,
    canonicalized: &StructuredPublicRead,
    current_version_heads: &BTreeMap<String, String>,
) -> Result<Option<LoweredReadProgram>, LixError> {
    let Some(admin_scan) = canonical_admin_scan(&canonicalized.read_command.root) else {
        return Ok(None);
    };

    build_lowered_read_program(
        dialect,
        canonicalized,
        build_admin_source_sql_with_current_heads(admin_scan.kind, dialect, current_version_heads)?,
        canonicalized.query.selection.clone(),
    )
    .map(Some)
}

fn lower_filesystem_read_for_execution(
    dialect: SqlDialect,
    canonicalized: &StructuredPublicRead,
) -> Result<Option<LoweredReadProgram>, LixError> {
    let Some(filesystem_scan) = canonical_filesystem_scan(&canonicalized.read_command.root) else {
        return Ok(None);
    };
    let active_version_id = canonicalized.requested_version_id.as_deref();
    let Some(surface) = FilesystemPublicSurface::from_filesystem_read(
        &canonicalized.surface_binding,
        filesystem_scan.kind,
        filesystem_scan.version_scope,
    ) else {
        return Ok(None);
    };

    build_lowered_read_program(
        dialect,
        canonicalized,
        build_filesystem_surface_sql(dialect, active_version_id, surface)?,
        canonicalized.query.selection.clone(),
    )
    .map(Some)
}

fn build_filesystem_surface_sql(
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    surface: FilesystemPublicSurface,
) -> Result<String, LixError> {
    let scoped_active_version_id = if surface.needs_active_version_id() {
        active_version_id
    } else {
        None
    };
    match surface.kind() {
        FilesystemKind::File => build_filesystem_file_projection_sql(
            surface.projection_scope(),
            scoped_active_version_id,
            false,
            dialect,
        ),
        FilesystemKind::Directory => build_filesystem_directory_projection_sql(
            surface.projection_scope(),
            scoped_active_version_id,
            dialect,
        ),
    }
}

fn build_state_source_sql(
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    surface_binding: &SurfaceBinding,
    effective_state_request: &EffectiveStateRequest,
    pushdown_predicates: &[Expr],
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<Option<String>, LixError> {
    let sql = match surface_binding.descriptor.surface_variant {
        SurfaceVariant::Default | SurfaceVariant::ByVersion => build_effective_state_source_sql(
            dialect,
            active_version_id,
            effective_state_request,
            surface_binding,
            pushdown_predicates,
            known_live_layouts,
        )?,
        SurfaceVariant::History => return Ok(None),
        SurfaceVariant::WorkingChanges => return Ok(None),
    };
    Ok(Some(sql))
}

fn build_admin_source_sql(
    kind: CanonicalAdminKind,
    dialect: SqlDialect,
) -> Result<String, LixError> {
    Ok(match kind {
        CanonicalAdminKind::Version => build_admin_version_source_sql(dialect),
    })
}

fn build_admin_source_sql_with_current_heads(
    kind: CanonicalAdminKind,
    dialect: SqlDialect,
    current_version_heads: &BTreeMap<String, String>,
) -> Result<String, LixError> {
    Ok(match kind {
        CanonicalAdminKind::Version => {
            build_admin_version_source_sql_with_current_heads(dialect, Some(current_version_heads))
        }
    })
}

fn build_entity_source_sql(
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    surface_binding: &SurfaceBinding,
    effective_state_request: &EffectiveStateRequest,
    pushdown_predicates: &[Expr],
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<Option<String>, LixError> {
    let projection = entity_projection_sql(surface_binding, effective_state_request);
    let projection = if projection.is_empty() {
        "entity_id AS lixcol_entity_id".to_string()
    } else {
        projection.join(", ")
    };

    let state_source_sql = match surface_binding.descriptor.surface_variant {
        SurfaceVariant::Default | SurfaceVariant::ByVersion => {
            Some(build_effective_public_read_source_sql(
                dialect,
                active_version_id,
                effective_state_request,
                surface_binding,
                pushdown_predicates,
                known_live_layouts,
                false,
            )?)
        }
        SurfaceVariant::History => None,
        SurfaceVariant::WorkingChanges => None,
    };
    let Some(state_source_sql) = state_source_sql else {
        return Ok(None);
    };
    let mut predicates = Vec::new();
    for predicate in &surface_binding.implicit_overrides.predicate_overrides {
        predicates.push(render_override_predicate(predicate));
    }

    let sql = if predicates.is_empty() {
        format!("SELECT {projection} FROM ({state_source_sql}) AS state_source")
    } else {
        format!(
            "SELECT {projection} FROM ({state_source_sql}) AS state_source WHERE {}",
            predicates.join(" AND ")
        )
    };
    Ok(Some(sql))
}

fn build_effective_state_source_sql(
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    effective_state_request: &EffectiveStateRequest,
    surface_binding: &SurfaceBinding,
    pushdown_predicates: &[Expr],
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<String, LixError> {
    let include_snapshot_content = effective_state_request
        .required_columns
        .iter()
        .any(|column| column.eq_ignore_ascii_case("snapshot_content"));
    build_effective_public_read_source_sql(
        dialect,
        active_version_id,
        effective_state_request,
        surface_binding,
        pushdown_predicates,
        known_live_layouts,
        include_snapshot_content,
    )
}

fn build_change_source_sql() -> String {
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
        ON s.id = ch.snapshot_id"
        .to_string()
}

fn canonical_admin_scan(
    read_plan: &ReadPlan,
) -> Option<&crate::sql::logical_plan::public_ir::CanonicalAdminScan> {
    match read_plan {
        ReadPlan::AdminScan(scan) => Some(scan),
        ReadPlan::Scan(_)
        | ReadPlan::FilesystemScan(_)
        | ReadPlan::ChangeScan(_)
        | ReadPlan::WorkingChangesScan(_) => None,
        ReadPlan::Filter { input, .. }
        | ReadPlan::Project { input, .. }
        | ReadPlan::Sort { input, .. }
        | ReadPlan::Limit { input, .. } => canonical_admin_scan(input),
    }
}

fn canonical_working_changes_scan(
    read_plan: &ReadPlan,
) -> Option<&crate::sql::logical_plan::public_ir::CanonicalWorkingChangesScan> {
    match read_plan {
        ReadPlan::WorkingChangesScan(scan) => Some(scan),
        ReadPlan::Scan(_)
        | ReadPlan::FilesystemScan(_)
        | ReadPlan::AdminScan(_)
        | ReadPlan::ChangeScan(_) => None,
        ReadPlan::Filter { input, .. }
        | ReadPlan::Project { input, .. }
        | ReadPlan::Sort { input, .. }
        | ReadPlan::Limit { input, .. } => canonical_working_changes_scan(input),
    }
}

fn canonical_filesystem_scan(
    read_plan: &ReadPlan,
) -> Option<&crate::sql::logical_plan::public_ir::CanonicalFilesystemScan> {
    match read_plan {
        ReadPlan::FilesystemScan(scan) => Some(scan),
        ReadPlan::Scan(_)
        | ReadPlan::AdminScan(_)
        | ReadPlan::ChangeScan(_)
        | ReadPlan::WorkingChangesScan(_) => None,
        ReadPlan::Filter { input, .. }
        | ReadPlan::Project { input, .. }
        | ReadPlan::Sort { input, .. }
        | ReadPlan::Limit { input, .. } => canonical_filesystem_scan(input),
    }
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
        let expression = if entity_surface_uses_payload_alias(surface_binding, column) {
            render_identifier(&entity_surface_payload_alias(column))
        } else {
            render_identifier(column)
        };
        return Some(format!("{expression} AS {alias}"));
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
        "lixcol_version_id" if variant == SurfaceVariant::ByVersion => Some("version_id"),
        _ => None,
    }
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
                    "day-1 public read lowering keeps this predicate above effective-state resolution"
                        .to_string(),
                support: PushdownSupport::Unsupported,
            })
            .collect(),
        residual_predicates: effective_state_plan.residual_predicates.clone(),
    }
}

fn change_pushdown_decision(canonicalized: &StructuredPublicRead) -> PushdownDecision {
    let residual_predicates = read_predicates_from_query(canonicalized);
    PushdownDecision {
        accepted_predicates: Vec::new(),
        rejected_predicates: residual_predicates
            .iter()
            .map(|predicate| RejectedPredicate {
                predicate: predicate.clone(),
                reason: "public change-scan lowering keeps change predicates above the derived change source".to_string(),
                support: PushdownSupport::Unsupported,
            })
            .collect(),
        residual_predicates,
    }
}

fn working_changes_pushdown_decision(canonicalized: &StructuredPublicRead) -> PushdownDecision {
    let residual_predicates = read_predicates_from_query(canonicalized);
    PushdownDecision {
        accepted_predicates: Vec::new(),
        rejected_predicates: residual_predicates
            .iter()
            .map(|predicate| RejectedPredicate {
                predicate: predicate.clone(),
                reason: "public working-changes lowering keeps predicates above the derived working-changes source".to_string(),
                support: PushdownSupport::Unsupported,
            })
            .collect(),
        residual_predicates,
    }
}

fn admin_pushdown_decision(canonicalized: &StructuredPublicRead) -> PushdownDecision {
    let residual_predicates = read_predicates_from_query(canonicalized);
    PushdownDecision {
        accepted_predicates: Vec::new(),
        rejected_predicates: residual_predicates
            .iter()
            .map(|predicate| RejectedPredicate {
                predicate: predicate.clone(),
                reason:
                    "public admin-scan lowering keeps admin predicates above the derived admin source"
                        .to_string(),
                support: PushdownSupport::Unsupported,
            })
            .collect(),
        residual_predicates,
    }
}

fn filesystem_pushdown_decision(canonicalized: &StructuredPublicRead) -> PushdownDecision {
    let residual_predicates = read_predicates_from_query(canonicalized);
    PushdownDecision {
        accepted_predicates: Vec::new(),
        rejected_predicates: residual_predicates
            .iter()
            .map(|predicate| RejectedPredicate {
                predicate: predicate.clone(),
                reason:
                    "public filesystem lowering keeps filesystem predicates above the derived source"
                        .to_string(),
                support: PushdownSupport::Unsupported,
            })
            .collect(),
        residual_predicates,
    }
}

fn split_state_selection_for_pushdown(
    selection: Option<&Expr>,
    effective_state_plan: &EffectiveStatePlan,
) -> (Vec<Expr>, Option<Expr>) {
    let Some(selection) = selection else {
        return (Vec::new(), None);
    };

    let mut pushdown = Vec::new();
    let mut residual = Vec::new();
    for predicate in split_conjunctive_predicates(selection) {
        if effective_state_plan
            .pushdown_safe_predicates
            .iter()
            .any(|accepted| accepted == &predicate)
            && !expr_contains_placeholder(&predicate)
        {
            pushdown.push(predicate);
        } else {
            residual.push(predicate);
        }
    }

    (pushdown, combine_conjunctive_predicates(residual))
}

fn expr_contains_placeholder(expr: &Expr) -> bool {
    struct PlaceholderVisitor {
        found: bool,
    }

    impl Visitor for PlaceholderVisitor {
        type Break = ();

        fn pre_visit_value(&mut self, value: &SqlValue) -> ControlFlow<Self::Break> {
            if matches!(value, SqlValue::Placeholder(_)) {
                self.found = true;
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }
    }

    let mut visitor = PlaceholderVisitor { found: false };
    let _ = expr.visit(&mut visitor);
    visitor.found
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

fn read_predicates_from_query(canonicalized: &StructuredPublicRead) -> Vec<Expr> {
    canonicalized.query.selection_predicates.clone()
}

#[cfg(test)]
mod tests {
    use super::{
        lower_broad_public_read_for_execution_with_layouts, lower_read_for_execution_with_layouts,
        LoweredReadProgram,
    };
    use crate::contracts::surface::SurfaceRegistry;
    use crate::sql::binder::{
        bind_broad_public_read_statement_with_registry, bind_statement,
        forbid_broad_binding_for_test,
    };
    use crate::sql::logical_plan::public_ir::{
        BroadPublicReadDistinct, BroadPublicReadGroupBy, BroadPublicReadJoin,
        BroadPublicReadLimitClause, BroadPublicReadOrderBy, BroadPublicReadQuery,
        BroadPublicReadSelect, BroadPublicReadSetExpr, BroadPublicReadStatement,
        BroadPublicReadTableFactor, BroadPublicReadTableWithJoins, BroadPublicReadWith,
        BroadSqlCaseWhen, BroadSqlExpr, BroadSqlExprKind, BroadSqlFunction, BroadSqlFunctionArg,
        BroadSqlFunctionArgExpr, BroadSqlFunctionArguments, BroadSqlProvenance,
    };
    use crate::sql::physical_plan::plan::LoweredReadStatementShape;
    use crate::sql::prepare::public_surface::routing::{
        forbid_broad_routing_for_test, route_broad_public_read_statement_with_known_live_layouts,
    };
    use crate::sql::semantic_ir::canonicalize::canonicalize_read;
    use crate::sql::semantic_ir::semantics::dependency_spec::derive_dependency_spec_from_structured_public_read;
    use crate::sql::semantic_ir::semantics::effective_state_resolver::build_effective_state;
    use crate::sql::semantic_ir::ExecutionContext;
    use crate::{SqlDialect, Value};
    use serde_json::{json, Value as JsonValue};
    use std::collections::BTreeMap;

    fn lowered_program(registry: &SurfaceRegistry, sql: &str) -> Option<LoweredReadProgram> {
        lowered_program_with_layouts(registry, sql, &BTreeMap::new())
    }

    fn lowered_program_with_layouts(
        registry: &SurfaceRegistry,
        sql: &str,
        known_live_layouts: &BTreeMap<String, JsonValue>,
    ) -> Option<LoweredReadProgram> {
        let mut statements = crate::sql::parser::parse_sql_script(sql).expect("SQL should parse");
        let statement = statements.pop().expect("single statement");
        let mut execution_context = ExecutionContext::with_dialect(SqlDialect::Sqlite);
        execution_context.requested_version_id = Some("main".to_string());
        let bound = bind_statement(statement, Vec::<Value>::new(), execution_context);
        let structured_read = canonicalize_read(bound, registry)
            .expect("query should canonicalize")
            .structured_read();
        let dependency_spec = derive_dependency_spec_from_structured_public_read(&structured_read);
        let effective_state = build_effective_state(&structured_read, dependency_spec.as_ref());
        lower_read_for_execution_with_layouts(
            SqlDialect::Sqlite,
            &structured_read,
            effective_state.as_ref().map(|(request, _)| request),
            effective_state.as_ref().map(|(_, plan)| plan),
            known_live_layouts,
            &BTreeMap::new(),
        )
        .expect("lowering should succeed")
    }

    fn bound_broad_statement(registry: &SurfaceRegistry, sql: &str) -> BroadPublicReadStatement {
        let mut statements = crate::sql::parser::parse_sql_script(sql).expect("SQL should parse");
        let statement = statements.pop().expect("single statement");
        bind_broad_public_read_statement_with_registry(&statement, registry)
            .expect("broad binding should succeed")
            .expect("query should bind as broad public read")
    }

    fn lowered_broad_program(registry: &SurfaceRegistry, sql: &str) -> Option<LoweredReadProgram> {
        lowered_broad_program_with_layouts(registry, sql, &BTreeMap::new())
    }

    fn lowered_broad_program_with_layouts(
        registry: &SurfaceRegistry,
        sql: &str,
        known_live_layouts: &BTreeMap<String, JsonValue>,
    ) -> Option<LoweredReadProgram> {
        let broad_statement = bound_broad_statement(registry, sql);
        let optimized = route_broad_public_read_statement_with_known_live_layouts(
            &broad_statement,
            registry,
            SqlDialect::Sqlite,
            Some("main"),
            known_live_layouts,
        )
        .expect("broad optimization should succeed");
        lower_broad_public_read_for_execution_with_layouts(
            &optimized.broad_statement,
            registry,
            SqlDialect::Sqlite,
            0,
            Some("main"),
            known_live_layouts,
        )
        .expect("broad lowering should succeed")
    }

    fn strip_provenance_from_broad_statement(statement: &mut BroadPublicReadStatement) {
        match statement {
            BroadPublicReadStatement::Query(query) => strip_provenance_from_broad_query(query),
            BroadPublicReadStatement::Explain {
                statement: inner, ..
            } => strip_provenance_from_broad_statement(inner),
        }
    }

    fn strip_provenance_from_broad_query(query: &mut BroadPublicReadQuery) {
        query.provenance = BroadSqlProvenance::default();
        if let Some(with) = &mut query.with {
            strip_provenance_from_broad_with(with);
        }
        strip_provenance_from_broad_set_expr(&mut query.body);
        if let Some(order_by) = &mut query.order_by {
            strip_provenance_from_broad_order_by(order_by);
        }
        if let Some(limit_clause) = &mut query.limit_clause {
            strip_provenance_from_broad_limit_clause(limit_clause);
        }
    }

    fn strip_provenance_from_broad_with(with: &mut BroadPublicReadWith) {
        with.provenance = BroadSqlProvenance::default();
        for cte in &mut with.cte_tables {
            strip_provenance_from_broad_query(&mut cte.query);
        }
    }

    fn strip_provenance_from_broad_set_expr(expr: &mut BroadPublicReadSetExpr) {
        match expr {
            BroadPublicReadSetExpr::Select(select) => strip_provenance_from_broad_select(select),
            BroadPublicReadSetExpr::Query(query) => strip_provenance_from_broad_query(query),
            BroadPublicReadSetExpr::SetOperation {
                provenance,
                left,
                right,
                ..
            } => {
                *provenance = BroadSqlProvenance::default();
                strip_provenance_from_broad_set_expr(left);
                strip_provenance_from_broad_set_expr(right);
            }
            BroadPublicReadSetExpr::Table { provenance, .. } => {
                *provenance = BroadSqlProvenance::default();
            }
        }
    }

    fn strip_provenance_from_broad_select(select: &mut BroadPublicReadSelect) {
        select.provenance = BroadSqlProvenance::default();
        if let Some(distinct) = &mut select.distinct {
            strip_provenance_from_broad_distinct(distinct);
        }
        for projection in &mut select.projection {
            projection.provenance = BroadSqlProvenance::default();
            if let crate::sql::logical_plan::public_ir::BroadPublicReadProjectionItemKind::Expr {
                expr,
                ..
            } = &mut projection.kind
            {
                strip_provenance_from_broad_sql_expr(expr);
            }
        }
        for table in &mut select.from {
            strip_provenance_from_broad_table_with_joins(table);
        }
        if let Some(selection) = &mut select.selection {
            strip_provenance_from_broad_sql_expr(selection);
        }
        strip_provenance_from_broad_group_by(&mut select.group_by);
        if let Some(having) = &mut select.having {
            strip_provenance_from_broad_sql_expr(having);
        }
    }

    fn strip_provenance_from_broad_distinct(distinct: &mut BroadPublicReadDistinct) {
        if let BroadPublicReadDistinct::On(expressions) = distinct {
            for expr in expressions {
                strip_provenance_from_broad_sql_expr(expr);
            }
        }
    }

    fn strip_provenance_from_broad_group_by(group_by: &mut BroadPublicReadGroupBy) {
        group_by.provenance = BroadSqlProvenance::default();
        if let crate::sql::logical_plan::public_ir::BroadPublicReadGroupByKind::Expressions(
            expressions,
        ) = &mut group_by.kind
        {
            for expr in expressions {
                strip_provenance_from_broad_sql_expr(expr);
            }
        }
    }

    fn strip_provenance_from_broad_order_by(order_by: &mut BroadPublicReadOrderBy) {
        order_by.provenance = BroadSqlProvenance::default();
        if let crate::sql::logical_plan::public_ir::BroadPublicReadOrderByKind::Expressions(
            expressions,
        ) = &mut order_by.kind
        {
            for expr in expressions {
                expr.provenance = BroadSqlProvenance::default();
                strip_provenance_from_broad_sql_expr(&mut expr.expr);
            }
        }
    }

    fn strip_provenance_from_broad_limit_clause(limit_clause: &mut BroadPublicReadLimitClause) {
        limit_clause.provenance = BroadSqlProvenance::default();
        match &mut limit_clause.kind {
            crate::sql::logical_plan::public_ir::BroadPublicReadLimitClauseKind::LimitOffset {
                limit,
                offset,
                limit_by,
            } => {
                if let Some(limit) = limit {
                    strip_provenance_from_broad_sql_expr(limit);
                }
                if let Some(offset) = offset {
                    strip_provenance_from_broad_sql_expr(&mut offset.value);
                }
                for expr in limit_by {
                    strip_provenance_from_broad_sql_expr(expr);
                }
            }
            crate::sql::logical_plan::public_ir::BroadPublicReadLimitClauseKind::OffsetCommaLimit {
                offset,
                limit,
            } => {
                strip_provenance_from_broad_sql_expr(offset);
                strip_provenance_from_broad_sql_expr(limit);
            }
        }
    }

    fn strip_provenance_from_broad_table_with_joins(table: &mut BroadPublicReadTableWithJoins) {
        table.provenance = BroadSqlProvenance::default();
        strip_provenance_from_broad_table_factor(&mut table.relation);
        for join in &mut table.joins {
            strip_provenance_from_broad_join(join);
        }
    }

    fn strip_provenance_from_broad_join(join: &mut BroadPublicReadJoin) {
        join.provenance = BroadSqlProvenance::default();
        strip_provenance_from_broad_table_factor(&mut join.relation);
        strip_provenance_from_broad_join_kind(&mut join.kind);
    }

    fn strip_provenance_from_broad_join_kind(
        kind: &mut crate::sql::logical_plan::public_ir::BroadPublicReadJoinKind,
    ) {
        use crate::sql::logical_plan::public_ir::BroadPublicReadJoinConstraint;
        use crate::sql::logical_plan::public_ir::BroadPublicReadJoinKind::*;

        match kind {
            Join(constraint)
            | Inner(constraint)
            | Left(constraint)
            | LeftOuter(constraint)
            | Right(constraint)
            | RightOuter(constraint)
            | FullOuter(constraint)
            | CrossJoin(constraint)
            | Semi(constraint)
            | LeftSemi(constraint)
            | RightSemi(constraint)
            | Anti(constraint)
            | LeftAnti(constraint)
            | RightAnti(constraint)
            | StraightJoin(constraint) => {
                if let BroadPublicReadJoinConstraint::On(expr) = constraint {
                    strip_provenance_from_broad_sql_expr(expr);
                }
            }
            AsOf {
                match_condition,
                constraint,
            } => {
                strip_provenance_from_broad_sql_expr(match_condition);
                if let BroadPublicReadJoinConstraint::On(expr) = constraint {
                    strip_provenance_from_broad_sql_expr(expr);
                }
            }
            CrossApply | OuterApply => {}
        }
    }

    fn strip_provenance_from_broad_table_factor(factor: &mut BroadPublicReadTableFactor) {
        match factor {
            BroadPublicReadTableFactor::Table { provenance, .. } => {
                *provenance = BroadSqlProvenance::default();
            }
            BroadPublicReadTableFactor::Derived {
                provenance,
                subquery,
                ..
            } => {
                *provenance = BroadSqlProvenance::default();
                strip_provenance_from_broad_query(subquery);
            }
            BroadPublicReadTableFactor::NestedJoin {
                provenance,
                table_with_joins,
                ..
            } => {
                *provenance = BroadSqlProvenance::default();
                strip_provenance_from_broad_table_with_joins(table_with_joins);
            }
        }
    }

    fn strip_provenance_from_broad_sql_expr(expr: &mut BroadSqlExpr) {
        match &mut expr.kind {
            BroadSqlExprKind::Identifier(_)
            | BroadSqlExprKind::CompoundIdentifier(_)
            | BroadSqlExprKind::Value(_)
            | BroadSqlExprKind::TypedString { .. }
            | BroadSqlExprKind::Unsupported { .. } => {}
            BroadSqlExprKind::BinaryOp { left, right, .. }
            | BroadSqlExprKind::AnyOp { left, right, .. }
            | BroadSqlExprKind::AllOp { left, right, .. }
            | BroadSqlExprKind::IsDistinctFrom { left, right }
            | BroadSqlExprKind::IsNotDistinctFrom { left, right } => {
                strip_provenance_from_broad_sql_expr(left);
                strip_provenance_from_broad_sql_expr(right);
            }
            BroadSqlExprKind::UnaryOp { expr, .. }
            | BroadSqlExprKind::Nested(expr)
            | BroadSqlExprKind::IsNull(expr)
            | BroadSqlExprKind::IsNotNull(expr)
            | BroadSqlExprKind::IsTrue(expr)
            | BroadSqlExprKind::IsNotTrue(expr)
            | BroadSqlExprKind::IsFalse(expr)
            | BroadSqlExprKind::IsNotFalse(expr)
            | BroadSqlExprKind::IsUnknown(expr)
            | BroadSqlExprKind::IsNotUnknown(expr)
            | BroadSqlExprKind::Cast { expr, .. } => {
                strip_provenance_from_broad_sql_expr(expr);
            }
            BroadSqlExprKind::InList { expr, list, .. } => {
                strip_provenance_from_broad_sql_expr(expr);
                for item in list {
                    strip_provenance_from_broad_sql_expr(item);
                }
            }
            BroadSqlExprKind::InSubquery { expr, subquery, .. } => {
                strip_provenance_from_broad_sql_expr(expr);
                strip_provenance_from_broad_query(subquery);
            }
            BroadSqlExprKind::InUnnest {
                expr, array_expr, ..
            } => {
                strip_provenance_from_broad_sql_expr(expr);
                strip_provenance_from_broad_sql_expr(array_expr);
            }
            BroadSqlExprKind::Between {
                expr, low, high, ..
            } => {
                strip_provenance_from_broad_sql_expr(expr);
                strip_provenance_from_broad_sql_expr(low);
                strip_provenance_from_broad_sql_expr(high);
            }
            BroadSqlExprKind::Like { expr, pattern, .. }
            | BroadSqlExprKind::ILike { expr, pattern, .. } => {
                strip_provenance_from_broad_sql_expr(expr);
                strip_provenance_from_broad_sql_expr(pattern);
            }
            BroadSqlExprKind::Function(function) => {
                strip_provenance_from_broad_sql_function(function);
            }
            BroadSqlExprKind::Case {
                operand,
                conditions,
                else_result,
            } => {
                if let Some(operand) = operand {
                    strip_provenance_from_broad_sql_expr(operand);
                }
                for when in conditions {
                    strip_provenance_from_broad_case_when(when);
                }
                if let Some(else_result) = else_result {
                    strip_provenance_from_broad_sql_expr(else_result);
                }
            }
            BroadSqlExprKind::Exists { subquery, .. }
            | BroadSqlExprKind::ScalarSubquery(subquery) => {
                strip_provenance_from_broad_query(subquery);
            }
            BroadSqlExprKind::Tuple(items) => {
                for item in items {
                    strip_provenance_from_broad_sql_expr(item);
                }
            }
        }
    }

    fn strip_provenance_from_broad_case_when(when: &mut BroadSqlCaseWhen) {
        strip_provenance_from_broad_sql_expr(&mut when.condition);
        strip_provenance_from_broad_sql_expr(&mut when.result);
    }

    fn strip_provenance_from_broad_sql_function(function: &mut BroadSqlFunction) {
        strip_provenance_from_broad_sql_function_arguments(&mut function.parameters);
        strip_provenance_from_broad_sql_function_arguments(&mut function.args);
        if let Some(filter) = &mut function.filter {
            strip_provenance_from_broad_sql_expr(filter);
        }
        for expr in &mut function.within_group {
            expr.provenance = BroadSqlProvenance::default();
            strip_provenance_from_broad_sql_expr(&mut expr.expr);
        }
    }

    fn strip_provenance_from_broad_sql_function_arguments(
        arguments: &mut BroadSqlFunctionArguments,
    ) {
        match arguments {
            BroadSqlFunctionArguments::None => {}
            BroadSqlFunctionArguments::Subquery(query) => {
                strip_provenance_from_broad_query(query);
            }
            BroadSqlFunctionArguments::List(list) => {
                for arg in &mut list.args {
                    strip_provenance_from_broad_sql_function_arg(arg);
                }
            }
        }
    }

    fn strip_provenance_from_broad_sql_function_arg(arg: &mut BroadSqlFunctionArg) {
        match arg {
            BroadSqlFunctionArg::Named { arg, .. } => {
                strip_provenance_from_broad_sql_function_arg_expr(arg);
            }
            BroadSqlFunctionArg::ExprNamed { name, arg, .. } => {
                strip_provenance_from_broad_sql_expr(name);
                strip_provenance_from_broad_sql_function_arg_expr(arg);
            }
            BroadSqlFunctionArg::Unnamed(arg) => {
                strip_provenance_from_broad_sql_function_arg_expr(arg);
            }
        }
    }

    fn strip_provenance_from_broad_sql_function_arg_expr(arg: &mut BroadSqlFunctionArgExpr) {
        if let BroadSqlFunctionArgExpr::Expr(expr) = arg {
            strip_provenance_from_broad_sql_expr(expr);
        }
    }

    #[test]
    fn lowers_builtin_entity_reads_through_state_surfaces() {
        let registry = crate::schema::build_builtin_surface_registry();
        let lowered = lowered_program(
            &registry,
            "SELECT key, value FROM lix_key_value WHERE key = 'hello'",
        )
        .expect("builtin entity read should lower");
        let lowered_sql = lowered.statements[0]
            .render_sql(SqlDialect::Sqlite)
            .expect("lowered statement should render");

        assert!(lowered_sql.contains("FROM (SELECT"));
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_key_value"));
        assert!(lowered_sql.contains("untracked = false"));
        assert!(lowered_sql.contains("untracked = true"));
        assert!(lowered_sql.contains("file_id = 'lix'"));
        assert!(lowered_sql.contains("plugin_key = 'lix'"));
        assert_eq!(
            lowered.pushdown_decision.residual_predicate_sql(),
            vec!["key = 'hello'".to_string()]
        );
        assert_eq!(
            lowered.pushdown_decision.accepted_predicate_sql(),
            Vec::<String>::new()
        );
    }

    #[test]
    fn broad_physical_lowering_requires_routing_lowered_relations() {
        let registry = crate::schema::build_builtin_surface_registry();
        let broad_statement = bound_broad_statement(
            &registry,
            "SELECT s.schema_key, COUNT(*) \
             FROM lix_state s \
             JOIN lix_state_by_version sv ON sv.entity_id = s.entity_id \
             WHERE s.schema_key = 'lix_key_value' AND sv.lixcol_version_id = 'main' \
             GROUP BY s.schema_key \
             ORDER BY s.schema_key",
        );

        let lowered = lower_broad_public_read_for_execution_with_layouts(
            &broad_statement,
            &registry,
            SqlDialect::Sqlite,
            0,
            Some("main"),
            &BTreeMap::new(),
        )
        .expect("broad lowering should not error");

        assert_eq!(
            lowered, None,
            "physical lowering should decline broad statements until routing marks renderable relations as lowered_public"
        );
    }

    #[test]
    fn lowers_optimized_cte_and_joined_state_surfaces_to_internal_queries() {
        let lowered = lowered_broad_program(
            &crate::schema::build_builtin_surface_registry(),
            "WITH keyed AS ( \
               SELECT entity_id, schema_key \
               FROM lix_state_by_version \
               WHERE schema_key = 'lix_key_value' \
                 AND lixcol_version_id = 'main' \
             ) \
             SELECT keyed.schema_key, COUNT(*) \
             FROM keyed \
             JOIN lix_state_by_version sv \
               ON sv.entity_id = keyed.entity_id \
             WHERE sv.lixcol_version_id = 'main' \
             GROUP BY keyed.schema_key \
             ORDER BY keyed.schema_key",
        )
        .expect("optimized broad state query should lower");
        let lowered_sql = lowered.statements[0]
            .render_sql(SqlDialect::Sqlite)
            .expect("lowered statement should render");

        assert!(!lowered_sql.contains("FROM lix_state "));
        assert!(!lowered_sql.contains("JOIN lix_state_by_version"));
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_key_value"));
        assert!(lowered_sql.contains("all_target_versions AS"));
    }

    #[test]
    fn cte_names_that_shadow_public_surfaces_do_not_lower_through_broad_physical_planning() {
        let lowered = lowered_broad_program(
            &crate::schema::build_builtin_surface_registry(),
            "WITH lix_state AS (SELECT 'shadow' AS entity_id) \
             SELECT entity_id FROM lix_state",
        );

        assert_eq!(
            lowered, None,
            "physical broad lowering should decline cte-shadowed names instead of rendering or reparsing shell SQL"
        );
    }

    #[test]
    fn lowers_dynamic_entity_reads_with_scalar_override_predicates() {
        let mut registry = crate::schema::build_builtin_surface_registry();
        crate::schema::public_surfaces::register_dynamic_entity_surface_spec(
            &mut registry,
            crate::contracts::surface::DynamicEntitySurfaceSpec {
                schema_key: "message".to_string(),
                visible_columns: vec!["body".to_string(), "id".to_string()],
                column_types: BTreeMap::new(),
                predicate_overrides: vec![
                    crate::contracts::surface::SurfaceOverridePredicate {
                        column: "file_id".to_string(),
                        value: crate::contracts::surface::SurfaceOverrideValue::String(
                            "inlang".to_string(),
                        ),
                    },
                    crate::contracts::surface::SurfaceOverridePredicate {
                        column: "plugin_key".to_string(),
                        value: crate::contracts::surface::SurfaceOverrideValue::String(
                            "inlang_sdk".to_string(),
                        ),
                    },
                    crate::contracts::surface::SurfaceOverridePredicate {
                        column: "global".to_string(),
                        value: crate::contracts::surface::SurfaceOverrideValue::Boolean(true),
                    },
                ],
            },
        );

        let mut known_live_layouts = BTreeMap::new();
        known_live_layouts.insert(
            "message".to_string(),
            json!({
                "x-lix-key": "message",
                "x-lix-version": "1",
                "type": "object",
                "properties": {
                    "body": { "type": "string" },
                    "id": { "type": "string" }
                },
                "required": ["id"]
            }),
        );

        let lowered = lowered_program_with_layouts(
            &registry,
            "SELECT body, lixcol_global FROM message WHERE id = 'm1'",
            &known_live_layouts,
        )
        .expect("dynamic entity read should lower");
        let lowered_sql = lowered.statements[0]
            .render_sql(SqlDialect::Sqlite)
            .expect("lowered statement should render");

        assert!(lowered_sql.contains("lix_internal_live_v1_message"));
        assert!(lowered_sql.contains("file_id = 'inlang'"));
        assert!(lowered_sql.contains("plugin_key = 'inlang_sdk'"));
        assert!(lowered_sql.contains("global = true"));
    }

    #[test]
    fn rejects_entity_wildcard_reads_for_live_lowering() {
        let registry = crate::schema::build_builtin_surface_registry();
        assert_eq!(
            lowered_program(&registry, "SELECT * FROM lix_key_value"),
            None
        );
    }

    #[test]
    fn lowers_state_reads_through_explicit_source_boundary() {
        let registry = crate::schema::build_builtin_surface_registry();
        let lowered = lowered_program(
            &registry,
            "SELECT entity_id FROM lix_state WHERE schema_key = 'lix_key_value'",
        )
        .expect("state read should lower");
        let lowered_sql = lowered.statements[0]
            .render_sql(SqlDialect::Sqlite)
            .expect("lowered statement should render");

        assert!(lowered_sql.contains("lix_internal_live_v1_lix_key_value"));
        assert!(!lowered_sql.contains("FROM lix_state"));
        assert!(!lowered_sql.contains(") WHERE schema_key = 'lix_key_value'"));
        assert_eq!(
            lowered.pushdown_decision.accepted_predicate_sql(),
            vec!["schema_key = 'lix_key_value'".to_string()]
        );
        assert_eq!(
            lowered.pushdown_decision.residual_predicate_sql(),
            Vec::<String>::new()
        );
    }

    #[test]
    fn lowers_change_reads_through_internal_change_sources() {
        let registry = crate::schema::build_builtin_surface_registry();
        let lowered = lowered_program(
            &registry,
            "SELECT id, schema_key, snapshot_content FROM lix_change WHERE entity_id = 'entity-1'",
        )
        .expect("change read should lower");
        let lowered_sql = lowered.statements[0]
            .render_sql(SqlDialect::Sqlite)
            .expect("lowered statement should render");

        assert!(lowered_sql.contains("FROM (SELECT ch.id AS id"));
        assert!(lowered_sql.contains("FROM lix_internal_change ch"));
        assert!(lowered_sql.contains("LEFT JOIN lix_internal_snapshot s"));
        assert_eq!(
            lowered.pushdown_decision.residual_predicate_sql(),
            vec!["entity_id = 'entity-1'".to_string()]
        );
    }

    #[test]
    fn projects_entity_string_payloads_without_json_wrappers() {
        let registry = crate::schema::build_builtin_surface_registry();
        let lowered = lowered_program(
            &registry,
            "SELECT change_id FROM lix_change_set_element \
             WHERE entity_id = 'entity-1' AND schema_key = 'lix_file_descriptor'",
        )
        .expect("change-set element read should lower");
        let lowered_sql = lowered.statements[0]
            .render_sql(SqlDialect::Sqlite)
            .expect("lowered statement should render");

        assert!(
            !lowered_sql.contains("json_extract(\"ranked\".\"change_id\""),
            "string payload columns should not be wrapped in sqlite JSON extraction: {lowered_sql}"
        );
        assert!(
            !lowered_sql.contains("json_extract(\"ranked\".\"entity_id\""),
            "string payload predicates should not rely on sqlite JSON extraction: {lowered_sql}"
        );
    }

    #[test]
    fn lowers_working_changes_reads_through_canonical_commit_sources() {
        let registry = crate::schema::build_builtin_surface_registry();
        let lowered = lowered_program(
            &registry,
            "SELECT status, before_change_id, after_change_id \
             FROM lix_working_changes \
             WHERE schema_key = 'lix_key_value'",
        )
        .expect("working changes read should lower");
        let lowered_sql = lowered.statements[0]
            .render_sql(SqlDialect::Sqlite)
            .expect("lowered statement should render");

        assert!(lowered_sql.contains("FROM lix_internal_change commit_change"));
        assert!(lowered_sql.contains("LEFT JOIN lix_internal_snapshot commit_snapshot"));
        assert!(lowered_sql.contains(
            "JOIN json_each(commit_rows.commit_snapshot_content, '$.parent_commit_ids')"
        ));
        assert!(lowered_sql
            .contains("JOIN json_each(commit_rows.commit_snapshot_content, '$.change_ids')"));
        assert!(
            !lowered_sql.contains("lix_internal_live_v1_lix_commit"),
            "working changes lowering should not use live commit mirrors: {lowered_sql}"
        );
        assert!(
            !lowered_sql.contains("lix_internal_live_v1_lix_change_set_element"),
            "working changes lowering should not use live change-set element mirrors: {lowered_sql}"
        );
        assert!(
            !lowered_sql.contains("lix_internal_live_v1_lix_commit_edge"),
            "working changes lowering should not use live commit-edge mirrors: {lowered_sql}"
        );
    }

    #[test]
    fn lowers_working_changes_reads_with_nested_filesystem_subqueries() {
        let registry = crate::schema::build_builtin_surface_registry();
        let lowered = lowered_program(
            &registry,
            "SELECT COUNT(*) \
             FROM lix_working_changes wc \
             WHERE wc.file_id IN (SELECT f.id FROM lix_file f WHERE f.path = '/hello.txt')",
        )
        .expect("working changes read with nested filesystem subquery should lower");
        let lowered_sql = lowered.statements[0]
            .render_sql(SqlDialect::Sqlite)
            .expect("lowered statement should render");

        assert!(
            !lowered_sql.contains("FROM lix_file"),
            "lowered sql still contains public lix_file: {lowered_sql}"
        );
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_file_descriptor"));
        assert!(lowered_sql.contains("FROM (WITH RECURSIVE"));
    }

    #[test]
    fn lowers_broad_working_changes_reads_with_nested_filesystem_subqueries() {
        let registry = crate::schema::build_builtin_surface_registry();
        let lowered = lowered_broad_program(
            &registry,
            "SELECT COUNT(*) \
             FROM lix_working_changes wc \
             WHERE wc.file_id IN (SELECT f.id FROM lix_file f WHERE f.path = '/hello.txt')",
        )
        .expect(
            "optimized broad working changes read with nested filesystem subquery should lower",
        );
        let lowered_sql = lowered.statements[0]
            .render_sql(SqlDialect::Sqlite)
            .expect("lowered broad statement should render");

        assert!(
            !lowered_sql.contains("FROM lix_file"),
            "lowered broad sql still contains public lix_file: {lowered_sql}"
        );
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_file_descriptor"));
        assert!(lowered_sql.contains("FROM (WITH RECURSIVE"));
    }

    #[test]
    fn lowers_broad_scalar_subqueries_without_top_level_surface_relations() {
        let registry = crate::schema::build_builtin_surface_registry();
        let lowered = lowered_broad_program(
            &registry,
            "SELECT \
                (SELECT lixcol_change_id FROM lix_directory WHERE id = 'dir-stable-parent'), \
                (SELECT lixcol_change_id FROM lix_directory WHERE id = 'dir-stable-child'), \
                (SELECT lixcol_change_id FROM lix_file WHERE id = 'file-stable-child')",
        )
        .expect("optimized broad scalar subqueries over public surfaces should lower");
        let lowered_sql = lowered.statements[0]
            .render_sql(SqlDialect::Sqlite)
            .expect("lowered broad scalar subquery statement should render");

        assert!(!lowered_sql.contains("FROM lix_directory"));
        assert!(!lowered_sql.contains("FROM lix_file"));
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_directory_descriptor"));
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_file_descriptor"));
    }

    #[test]
    fn broad_physical_lowering_requires_routing_lowered_nested_relations() {
        let registry = crate::schema::build_builtin_surface_registry();
        let broad_statement = bound_broad_statement(
            &registry,
            "SELECT (SELECT lixcol_change_id FROM lix_directory WHERE id = 'dir-stable-parent')",
        );

        let lowered = lower_broad_public_read_for_execution_with_layouts(
            &broad_statement,
            &registry,
            SqlDialect::Sqlite,
            0,
            Some("main"),
            &BTreeMap::new(),
        )
        .expect("broad lowering should not error");

        assert_eq!(
            lowered, None,
            "physical lowering should decline nested public relations until routing lowers them"
        );

        let optimized = route_broad_public_read_statement_with_known_live_layouts(
            &broad_statement,
            &registry,
            SqlDialect::Sqlite,
            Some("main"),
            &BTreeMap::new(),
        )
        .expect("broad optimization should succeed");
        let lowered = lower_broad_public_read_for_execution_with_layouts(
            &optimized.broad_statement,
            &registry,
            SqlDialect::Sqlite,
            0,
            Some("main"),
            &BTreeMap::new(),
        )
        .expect("optimized broad lowering should not error")
        .expect("optimized nested public relation should lower");
        let lowered_sql = lowered.statements[0]
            .render_sql(SqlDialect::Sqlite)
            .expect("lowered statement should render");

        assert!(!lowered_sql.contains("FROM lix_directory"));
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_directory_descriptor"));
    }

    #[test]
    fn broad_physical_lowering_does_not_call_back_into_binding_or_routing() {
        let registry = crate::schema::build_builtin_surface_registry();
        let broad_statement = bound_broad_statement(
            &registry,
            "SELECT \
               (SELECT lixcol_change_id FROM lix_directory WHERE id = 'dir-stable-parent') AS parent_change_id, \
               EXISTS (SELECT 1 FROM lix_directory WHERE id = 'dir-stable-child') AS has_child_dir, \
               'file-stable-child' IN (SELECT id FROM lix_file WHERE path = '/hello.txt') AS has_file",
        );
        let optimized = route_broad_public_read_statement_with_known_live_layouts(
            &broad_statement,
            &registry,
            SqlDialect::Sqlite,
            Some("main"),
            &BTreeMap::new(),
        )
        .expect("broad routing should succeed");

        let _binding_guard = forbid_broad_binding_for_test();
        let _routing_guard = forbid_broad_routing_for_test();
        let lowered = lower_broad_public_read_for_execution_with_layouts(
            &optimized.broad_statement,
            &registry,
            SqlDialect::Sqlite,
            0,
            Some("main"),
            &BTreeMap::new(),
        )
        .expect("broad lowering should not error")
        .expect("optimized broad nested query should lower without callback");
        let lowered_sql = lowered.statements[0]
            .render_sql(SqlDialect::Sqlite)
            .expect("lowered statement should render");

        assert!(!lowered_sql.contains("FROM lix_directory"));
        assert!(!lowered_sql.contains("FROM lix_file"));
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_directory_descriptor"));
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_file_descriptor"));
    }

    #[test]
    fn broad_physical_lowering_accepts_provenance_free_typed_ir() {
        let registry = crate::schema::build_builtin_surface_registry();
        let mut broad_statement = bound_broad_statement(
            &registry,
            "WITH keyed AS ( \
               SELECT entity_id, schema_key \
               FROM lix_state \
               WHERE schema_key = 'lix_key_value' \
             ) \
             SELECT keyed.schema_key, COUNT(*) \
             FROM keyed \
             JOIN lix_state_by_version sv \
               ON sv.entity_id = keyed.entity_id \
             WHERE sv.lixcol_version_id = 'main' \
             GROUP BY keyed.schema_key \
             ORDER BY keyed.schema_key",
        );
        strip_provenance_from_broad_statement(&mut broad_statement);

        let optimized = route_broad_public_read_statement_with_known_live_layouts(
            &broad_statement,
            &registry,
            SqlDialect::Sqlite,
            Some("main"),
            &BTreeMap::new(),
        )
        .expect("broad routing should not require stored provenance for accepted typed IR");

        let lowered = lower_broad_public_read_for_execution_with_layouts(
            &optimized.broad_statement,
            &registry,
            SqlDialect::Sqlite,
            0,
            Some("main"),
            &BTreeMap::new(),
        )
        .expect("broad lowering should not require stored provenance for accepted typed IR")
        .expect("provenance-free typed broad IR should still lower");

        let lowered_sql = lowered.statements[0]
            .render_sql(SqlDialect::Sqlite)
            .expect("provenance-free typed broad statement should render");
        assert!(!lowered_sql.contains("FROM lix_state "));
        assert!(!lowered_sql.contains("JOIN lix_state_by_version"));
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_key_value"));
        assert!(lowered_sql.contains("all_target_versions AS"));
    }

    #[test]
    fn broad_physical_lowering_emits_final_terminal_statement_artifacts() {
        let registry = crate::schema::build_builtin_surface_registry();
        let lowered = lowered_broad_program(
            &registry,
            "SELECT COUNT(*) \
             FROM lix_working_changes wc \
             WHERE wc.file_id IN (SELECT f.id FROM lix_file f WHERE f.path = '/hello.txt')",
        )
        .expect("optimized broad read should lower");

        let statement = lowered
            .statements
            .first()
            .expect("broad lowered program should contain a statement");
        match &statement.shape {
            LoweredReadStatementShape::Final { statement_sql } => {
                assert!(
                    !statement_sql.contains("__lix_lowered_relation_"),
                    "broad physical artifacts must not fall back to placeholder shell SQL identity"
                );
                assert!(
                    statement_sql.contains("lix_internal_live_v1_lix_file_descriptor"),
                    "broad final artifact should already embed the terminal lowered relation SQL"
                );
            }
            LoweredReadStatementShape::Template { .. } => {
                panic!(
                    "broad physical artifacts must remain terminal final statements, not shell-SQL templates"
                );
            }
        }
    }

    #[test]
    fn broad_physical_lowerer_source_has_no_fallback_defense_branches() {
        let lowerer_broad_src = include_str!("lowerer/broad.rs");

        for forbidden in [
            "BroadPublicReadSetExpr::Other",
            "BroadPublicReadTableFactor::Other",
            "legacy set-expression fallback",
            "legacy table-factor fallback",
        ] {
            assert!(
                !lowerer_broad_src.contains(forbidden),
                "broad physical lowering must not reintroduce fallback-defense branch {forbidden}"
            );
        }
    }

    #[test]
    fn lowers_filesystem_current_and_versioned_reads_through_internal_sources() {
        let registry = crate::schema::build_builtin_surface_registry();

        let current = lowered_program(
            &registry,
            "SELECT id, path, data FROM lix_file WHERE id = 'file-1'",
        )
        .expect("filesystem current read should lower");
        let current_sql = current.statements[0]
            .render_sql(SqlDialect::Sqlite)
            .expect("lowered statement should render");

        assert!(current_sql.contains("lix_internal_live_v1_lix_file_descriptor"));
        assert!(current_sql.contains("lix_internal_live_v1_lix_directory_descriptor"));
        assert!(current_sql.contains("lix_internal_binary_blob_store"));
        assert!(!current_sql.contains("FROM lix_file_by_version"));
        assert_eq!(
            current.pushdown_decision.residual_predicate_sql(),
            vec!["id = 'file-1'".to_string()]
        );

        let by_version = lowered_program(
            &registry,
            "SELECT id, path FROM lix_directory_by_version \
             WHERE id = 'dir-1' AND lixcol_version_id = 'version-a'",
        )
        .expect("filesystem by-version read should lower");
        let by_version_sql = by_version.statements[0]
            .render_sql(SqlDialect::Sqlite)
            .expect("lowered statement should render");

        assert!(by_version_sql.contains("lix_internal_live_v1_lix_directory_descriptor"));
        assert!(by_version_sql.contains("all_target_versions AS"));
        assert!(!by_version_sql.contains("FROM lix_directory_by_version"));
        assert_eq!(
            by_version.pushdown_decision.residual_predicate_sql(),
            vec![
                "id = 'dir-1'".to_string(),
                "lixcol_version_id = 'version-a'".to_string()
            ]
        );
    }

    #[test]
    fn rejects_removed_active_version_surface() {
        let registry = crate::schema::build_builtin_surface_registry();
        assert!(registry.bind_relation_name("lix_active_version").is_none());
    }

    #[test]
    fn rejects_removed_active_account_surface() {
        let registry = crate::schema::build_builtin_surface_registry();
        assert!(registry.bind_relation_name("lix_active_account").is_none());
    }

    #[test]
    fn lowers_version_reads_through_canonical_descriptor_and_pointer_sources() {
        let registry = crate::schema::build_builtin_surface_registry();
        let lowered = lowered_program(
            &registry,
            "SELECT id, name, hidden, commit_id FROM lix_version WHERE name = 'main'",
        )
        .expect("version read should lower");
        let lowered_sql = lowered.statements[0]
            .render_sql(SqlDialect::Sqlite)
            .expect("lowered statement should render");

        assert!(lowered_sql.contains("FROM lix_internal_change c"));
        assert!(lowered_sql.contains("lix_version_descriptor"));
        assert!(lowered_sql.contains("current_refs"));
        assert!(!lowered_sql.contains("FROM lix_state_by_version"));
        assert!(!lowered_sql.contains("FROM lix_version"));
        assert!(!lowered_sql.contains("lix_json_extract("));
        assert!(!lowered_sql.contains("lix_json_extract_boolean("));
        assert_eq!(
            lowered.pushdown_decision.accepted_predicate_sql(),
            Vec::<String>::new()
        );
        assert_eq!(
            lowered.pushdown_decision.residual_predicate_sql(),
            vec!["name = 'main'".to_string()]
        );

        let lowered_postgres_sql = lowered.statements[0]
            .render_sql(SqlDialect::Postgres)
            .expect("postgres lowered statement should render");
        crate::sql::parser::parse_sql_script(&lowered_postgres_sql).unwrap_or_else(|error| {
            panic!("postgres lix_version lowered sql should parse: {error}\n{lowered_postgres_sql}")
        });
    }

    #[test]
    fn broad_lowering_keeps_version_commit_join_on_internal_sources() {
        let registry = crate::schema::build_builtin_surface_registry();
        let lowered = lowered_broad_program(
            &registry,
            "SELECT c.change_set_id \
             FROM lix_version v \
             JOIN lix_commit c ON c.id = v.commit_id \
             WHERE v.id = lix_active_version_id() \
             LIMIT 1",
        )
        .expect("version/commit join should lower");
        let lowered_sql = lowered.statements[0]
            .render_sql(SqlDialect::Sqlite)
            .expect("lowered statement should render");

        assert!(
            !lowered_sql.contains("FROM lix_version"),
            "lowered sql still contains public lix_version: {lowered_sql}"
        );
        assert!(
            !lowered_sql.contains("JOIN lix_commit"),
            "lowered sql still contains public lix_commit: {lowered_sql}"
        );
        assert!(
            !lowered_sql.contains("lix_state_by_version"),
            "lowered sql still contains lix_state_by_version: {lowered_sql}"
        );
        assert!(lowered_sql.contains("lix_version_descriptor"));
        assert!(lowered_sql.contains("lix_internal_change"));
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_version_ref"));
    }

    #[test]
    fn parameterized_version_reads_render_parseable_postgres_sql() {
        let registry = crate::schema::build_builtin_surface_registry();
        let mut statements = crate::sql::parser::parse_sql_script(
            "SELECT id, commit_id FROM lix_version WHERE id = $1 LIMIT 1",
        )
        .expect("SQL should parse");
        let statement = statements.pop().expect("single statement");
        let mut execution_context = ExecutionContext::with_dialect(SqlDialect::Postgres);
        execution_context.requested_version_id = Some("main".to_string());
        let bound = bind_statement(
            statement,
            vec![Value::Text("main".to_string())],
            execution_context,
        );
        let structured_read = canonicalize_read(bound, &registry)
            .expect("query should canonicalize")
            .structured_read();
        let dependency_spec = derive_dependency_spec_from_structured_public_read(&structured_read);
        let effective_state = build_effective_state(&structured_read, dependency_spec.as_ref());
        let lowered = lower_read_for_execution_with_layouts(
            SqlDialect::Postgres,
            &structured_read,
            effective_state.as_ref().map(|(request, _)| request),
            effective_state.as_ref().map(|(_, plan)| plan),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .expect("parameterized version read should lower")
        .expect("parameterized version read should produce lowered sql");
        let runtime_bindings = crate::sql::binder::RuntimeBindingValues {
            active_version_id: "main".to_string(),
            active_account_ids_json: "[]".to_string(),
        };
        let (sql, bound_params) = lowered.statements[0]
            .bind_and_render_sql(
                &[Value::Text("main".to_string())],
                &runtime_bindings,
                SqlDialect::Postgres,
            )
            .expect("postgres parameterized lowered statement should render");

        assert_eq!(bound_params, vec![Value::Text("main".to_string())]);
        assert!(sql.contains("WITH RECURSIVE canonical_commit_headers AS"));
        assert!(sql.contains("FROM descriptor_state d"));
        assert!(sql.contains("WHERE id = $1"));
    }

    #[test]
    fn lowers_registered_schema_reads_through_entity_surface() {
        let registry = crate::schema::build_builtin_surface_registry();
        let lowered = lowered_program(
            &registry,
            "SELECT value, lixcol_entity_id FROM lix_registered_schema WHERE lixcol_entity_id = 'x~1'",
        )
        .expect("registered schema read should lower");
        let lowered_sql = lowered.statements[0]
            .render_sql(SqlDialect::Sqlite)
            .expect("lowered statement should render");

        assert!(lowered_sql.contains("lix_internal_live_v1_lix_registered_schema"));
        assert!(lowered_sql.contains("file_id = 'lix'"));
        assert!(lowered_sql.contains("plugin_key = 'lix'"));
        assert!(lowered_sql.contains("global = true"));
        assert!(!lowered_sql.contains("FROM lix_registered_schema"));
        assert_eq!(
            lowered.pushdown_decision.residual_predicate_sql(),
            vec!["lixcol_entity_id = 'x~1'".to_string()]
        );
    }

    #[test]
    fn broad_lowering_preserves_select_distinct() {
        let registry = crate::schema::build_builtin_surface_registry();
        let lowered = lowered_broad_program(
            &registry,
            "SELECT DISTINCT schema_key \
             FROM lix_state_by_version \
             WHERE entity_id = 'version-a' \
               AND schema_key IN ('lix_version_descriptor', 'lix_version_ref') \
               AND snapshot_content IS NOT NULL \
             ORDER BY schema_key",
        )
        .expect("broad distinct read should lower");
        let lowered_sql = lowered.statements[0]
            .render_sql(SqlDialect::Sqlite)
            .expect("lowered statement should render");

        assert!(
            lowered_sql.contains("SELECT DISTINCT"),
            "broad lowered SQL must preserve DISTINCT: {lowered_sql}"
        );
    }
}
