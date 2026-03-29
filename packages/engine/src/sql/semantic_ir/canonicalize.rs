use crate::sql::catalog::{SurfaceBinding, SurfaceFamily, SurfaceRegistry};
use crate::sql::logical_plan::public_ir::{
    CanonicalAdminScan, CanonicalChangeScan, CanonicalFilesystemScan, CanonicalStateScan,
    CanonicalWorkingChangesScan, InsertOnConflict, InsertOnConflictAction, MutationPayload,
    NormalizedPublicReadQuery, PredicateSpec, ProjectionExpr, ReadCommand, ReadContract, ReadPlan,
    SortKey, StructuredPublicRead, WriteCommand, WriteModeRequest, WriteOperationKind,
    WriteSelector,
};
use crate::sql::parser::placeholders::{resolve_placeholder_index, PlaceholderState};
use crate::sql::semantic_ir::{BoundStatement, ExecutionContext, StatementKind};
use crate::Value;
use sqlparser::ast::{
    AssignmentTarget, BinaryOperator, ConflictTarget, Delete, Expr, FromTable, FunctionArg,
    FunctionArgExpr, FunctionArguments, GroupByExpr, Ident, Insert, LimitClause, ObjectNamePart,
    OnConflictAction, OnInsert, OrderBy, OrderByKind, Query, Select, SelectItem, SetExpr,
    Statement, TableFactor, TableWithJoins, Update, Value as SqlValue,
};
use std::collections::BTreeMap;

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
    pub(crate) query: NormalizedPublicReadQuery,
}

impl CanonicalizedRead {
    pub(crate) fn structured_read(&self) -> StructuredPublicRead {
        StructuredPublicRead {
            bound_statement: self.bound_statement.clone(),
            surface_binding: self.surface_binding.clone(),
            read_command: self.read_command.clone(),
            query: self.query.clone(),
        }
    }
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
            "public day-1 canonicalizer only supports query statements",
        ));
    }

    let Statement::Query(query) = &bound_statement.statement else {
        return Err(CanonicalizeError::unsupported(
            "public day-1 canonicalizer requires a top-level query",
        ));
    };

    let select = extract_supported_select(query)?;
    let surface_binding = bind_single_surface(select, registry)?;
    let query_model = normalized_public_read_query(query, select)?;
    let mut root = if surface_binding.resolution_capabilities.canonical_state_scan {
        let scan =
            CanonicalStateScan::from_surface_binding(surface_binding.clone()).ok_or_else(|| {
                CanonicalizeError::unsupported(format!(
                    "surface '{}' did not produce a canonical state scan",
                    surface_binding.descriptor.public_name
                ))
            })?;
        ReadPlan::scan(scan)
    } else if surface_binding
        .resolution_capabilities
        .canonical_change_scan
    {
        let scan = CanonicalChangeScan::from_surface_binding(surface_binding.clone()).ok_or_else(
            || {
                CanonicalizeError::unsupported(format!(
                    "surface '{}' did not produce a canonical change scan",
                    surface_binding.descriptor.public_name
                ))
            },
        )?;
        ReadPlan::change_scan(scan)
    } else if surface_binding
        .resolution_capabilities
        .canonical_working_changes_scan
    {
        let scan = CanonicalWorkingChangesScan::from_surface_binding(surface_binding.clone())
            .ok_or_else(|| {
                CanonicalizeError::unsupported(format!(
                    "surface '{}' did not produce a canonical working-changes scan",
                    surface_binding.descriptor.public_name
                ))
            })?;
        ReadPlan::working_changes_scan(scan)
    } else if surface_binding
        .resolution_capabilities
        .canonical_filesystem_scan
    {
        let scan = CanonicalFilesystemScan::from_surface_binding(surface_binding.clone())
            .ok_or_else(|| {
                CanonicalizeError::unsupported(format!(
                    "surface '{}' did not produce a canonical filesystem scan",
                    surface_binding.descriptor.public_name
                ))
            })?;
        ReadPlan::filesystem_scan(scan)
    } else if surface_binding.resolution_capabilities.canonical_admin_scan {
        let scan =
            CanonicalAdminScan::from_surface_binding(surface_binding.clone()).ok_or_else(|| {
                CanonicalizeError::unsupported(format!(
                    "surface '{}' did not produce a canonical admin scan",
                    surface_binding.descriptor.public_name
                ))
            })?;
        ReadPlan::admin_scan(scan)
    } else {
        return Err(CanonicalizeError::unsupported(format!(
            "surface '{}' does not yet canonicalize through public read planning",
            surface_binding.descriptor.public_name
        )));
    };

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
        query: query_model,
    })
}

pub(crate) fn canonicalize_write(
    bound_statement: BoundStatement,
    registry: &SurfaceRegistry,
) -> Result<CanonicalizedWrite, CanonicalizeError> {
    let statement = bound_statement.statement.clone();
    match statement {
        Statement::Insert(insert) => canonicalize_insert_write(bound_statement, &insert, registry),
        Statement::Update(update) => canonicalize_update_write(bound_statement, &update, registry),
        Statement::Delete(delete) => canonicalize_delete_write(bound_statement, &delete, registry),
        _ => Err(CanonicalizeError::unsupported(
            "public day-1 write canonicalizer only supports INSERT/UPDATE/DELETE statements",
        )),
    }
}

fn canonicalize_insert_write(
    bound_statement: BoundStatement,
    insert: &Insert,
    registry: &SurfaceRegistry,
) -> Result<CanonicalizedWrite, CanonicalizeError> {
    let surface_binding = bind_insert_surface(insert, registry)?;
    reject_filesystem_history_write(&surface_binding, "INSERT")?;
    validate_semantic_write_surface(&surface_binding, insert_write_surface_supported)?;
    if !insert.assignments.is_empty() || insert.returning.is_some() {
        return Err(CanonicalizeError::unsupported(
            "public day-1 write canonicalizer only supports INSERT statements without assignment targets or RETURNING",
        ));
    }

    let payloads = insert_payloads(
        &surface_binding,
        insert,
        &bound_statement.bound_parameters,
        &bound_statement.execution_context,
    )?;
    let on_conflict = match payloads.as_slice() {
        [payload] => insert_on_conflict(
            &surface_binding,
            insert,
            &bound_statement.bound_parameters,
            &bound_statement.execution_context,
            payload,
        )?,
        _ if insert.on.is_some() => {
            return Err(CanonicalizeError::unsupported(
                "public day-1 insert canonicalizer does not yet support ON CONFLICT on multi-row inserts",
            ));
        }
        _ => None,
    };
    let requested_mode = write_mode_request_for_insert_payloads(&surface_binding, &payloads)?;
    let payload = MutationPayload::InsertRows(payloads);

    Ok(CanonicalizedWrite {
        bound_statement: bound_statement.clone(),
        surface_binding: surface_binding.clone(),
        write_command: WriteCommand {
            operation_kind: WriteOperationKind::Insert,
            target: surface_binding,
            selector: WriteSelector::default(),
            payload,
            on_conflict,
            requested_mode,
            bound_parameters: bound_statement.bound_parameters.clone(),
            execution_context: bound_statement.execution_context,
        },
    })
}

fn canonicalize_update_write(
    bound_statement: BoundStatement,
    update: &Update,
    registry: &SurfaceRegistry,
) -> Result<CanonicalizedWrite, CanonicalizeError> {
    if update.from.is_some()
        || update.returning.is_some()
        || update.limit.is_some()
        || update.or.is_some()
    {
        return Err(CanonicalizeError::unsupported(
            "public day-1 update canonicalizer only supports simple UPDATE statements without FROM/RETURNING/LIMIT/OR",
        ));
    }
    let surface_binding = bind_update_surface(update, registry)?;
    reject_filesystem_history_write(&surface_binding, "UPDATE")?;
    validate_semantic_write_surface(&surface_binding, update_delete_surface_supported)?;
    let mut placeholder_state = PlaceholderState::new();
    let payload = assignment_payload(
        &surface_binding,
        &update.assignments,
        &bound_statement.bound_parameters,
        &bound_statement.execution_context,
        &mut placeholder_state,
    )?;
    let selector = match update.selection.as_ref() {
        Some(selection) => write_selector(
            &surface_binding,
            selection,
            &bound_statement.bound_parameters,
            &bound_statement.execution_context,
            &mut placeholder_state,
        )?,
        None if supports_implicit_admin_selector(&surface_binding) => WriteSelector {
            exact_only: true,
            ..WriteSelector::default()
        },
        None => {
            return Err(CanonicalizeError::unsupported(
                "public day-1 update canonicalizer requires an explicit WHERE predicate",
            ));
        }
    };
    let requested_mode =
        write_mode_request_for_surface_and_selector(&surface_binding, &payload, Some(&selector));

    Ok(CanonicalizedWrite {
        bound_statement: bound_statement.clone(),
        surface_binding: surface_binding.clone(),
        write_command: WriteCommand {
            operation_kind: WriteOperationKind::Update,
            target: surface_binding.clone(),
            selector,
            payload: MutationPayload::UpdatePatch(payload),
            on_conflict: None,
            requested_mode,
            bound_parameters: bound_statement.bound_parameters.clone(),
            execution_context: bound_statement.execution_context,
        },
    })
}

fn canonicalize_delete_write(
    bound_statement: BoundStatement,
    delete: &Delete,
    registry: &SurfaceRegistry,
) -> Result<CanonicalizedWrite, CanonicalizeError> {
    if !delete.tables.is_empty()
        || delete.using.is_some()
        || delete.returning.is_some()
        || !delete.order_by.is_empty()
        || delete.limit.is_some()
    {
        return Err(CanonicalizeError::unsupported(
            "public day-1 delete canonicalizer only supports simple DELETE statements without USING/RETURNING/ORDER BY/LIMIT",
        ));
    }
    let surface_binding = bind_delete_surface(delete, registry)?;
    reject_filesystem_history_write(&surface_binding, "DELETE")?;
    validate_semantic_write_surface(&surface_binding, update_delete_surface_supported)?;
    let mut placeholder_state = PlaceholderState::new();
    let selector = match delete.selection.as_ref() {
        Some(selection) => write_selector(
            &surface_binding,
            selection,
            &bound_statement.bound_parameters,
            &bound_statement.execution_context,
            &mut placeholder_state,
        )?,
        None if supports_implicit_admin_selector(&surface_binding) => WriteSelector {
            exact_only: true,
            ..WriteSelector::default()
        },
        None => {
            return Err(CanonicalizeError::unsupported(
                "public day-1 delete canonicalizer requires an explicit WHERE predicate",
            ));
        }
    };

    Ok(CanonicalizedWrite {
        bound_statement: bound_statement.clone(),
        surface_binding: surface_binding.clone(),
        write_command: WriteCommand {
            operation_kind: WriteOperationKind::Delete,
            target: surface_binding.clone(),
            selector: selector.clone(),
            payload: MutationPayload::Tombstone,
            on_conflict: None,
            requested_mode: write_mode_request_for_surface_and_selector(
                &surface_binding,
                &BTreeMap::new(),
                Some(&selector),
            ),
            bound_parameters: bound_statement.bound_parameters.clone(),
            execution_context: bound_statement.execution_context,
        },
    })
}

fn insert_on_conflict(
    surface_binding: &SurfaceBinding,
    insert: &Insert,
    params: &[Value],
    execution_context: &ExecutionContext,
    insert_payload: &BTreeMap<String, Value>,
) -> Result<Option<InsertOnConflict>, CanonicalizeError> {
    let Some(on_insert) = &insert.on else {
        return Ok(None);
    };

    let OnInsert::OnConflict(on_conflict) = on_insert else {
        return Err(CanonicalizeError::unsupported(
            "public day-1 insert canonicalizer only supports ON CONFLICT ... DO UPDATE",
        ));
    };

    let conflict_columns = match &on_conflict.conflict_target {
        Some(ConflictTarget::Columns(columns)) if !columns.is_empty() => columns
            .iter()
            .map(|ident| canonical_write_column_key(surface_binding, &ident.value))
            .collect::<Result<Vec<_>, _>>()?,
        Some(_) => {
            return Err(CanonicalizeError::unsupported(
                "public day-1 insert canonicalizer only supports explicit ON CONFLICT column targets",
            ));
        }
        None => {
            return Err(CanonicalizeError::unsupported(
                "public day-1 insert canonicalizer requires explicit ON CONFLICT columns",
            ));
        }
    };

    match &on_conflict.action {
        OnConflictAction::DoNothing => Ok(Some(InsertOnConflict {
            conflict_columns,
            action: InsertOnConflictAction::DoNothing,
        })),
        OnConflictAction::DoUpdate(update) => {
            if update.selection.is_some() {
                return Err(CanonicalizeError::unsupported(
                    "public day-1 insert canonicalizer does not support ON CONFLICT DO UPDATE WHERE",
                ));
            }
            let mut placeholder_state = PlaceholderState::new();
            let update_payload = assignment_payload(
                surface_binding,
                &update.assignments,
                params,
                execution_context,
                &mut placeholder_state,
            )?;
            for (key, value) in update_payload {
                if insert_payload.get(&key) != Some(&value) {
                    return Err(CanonicalizeError::unsupported(
                        "public day-1 insert canonicalizer only supports ON CONFLICT DO UPDATE assignments that match inserted values",
                    ));
                }
            }
            Ok(Some(InsertOnConflict {
                conflict_columns,
                action: InsertOnConflictAction::DoUpdate,
            }))
        }
    }
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
            "public day-1 canonicalizer does not support WITH/FETCH/LOCK/FOR/SETTINGS/FORMAT clauses",
        ));
    }

    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(CanonicalizeError::unsupported(
            "public day-1 canonicalizer only supports SELECT bodies",
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
            "public day-1 canonicalizer only supports Scan->Filter->Project->Sort->Limit read shapes",
        ));
    }

    match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers) if exprs.is_empty() && modifiers.is_empty() => {}
        GroupByExpr::Expressions(_, _) | GroupByExpr::All(_) => {
            return Err(CanonicalizeError::unsupported(
                "public day-1 canonicalizer does not support GROUP BY",
            ));
        }
    }

    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Err(CanonicalizeError::unsupported(
            "public day-1 canonicalizer requires a single surface scan without joins",
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
            "public day-1 canonicalizer only supports direct table references",
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
            "surface '{surface_name}' is not registered in public SurfaceRegistry"
        ))
    })
}

fn bind_insert_surface(
    insert: &Insert,
    registry: &SurfaceRegistry,
) -> Result<SurfaceBinding, CanonicalizeError> {
    let sqlparser::ast::TableObject::TableName(name) = &insert.table else {
        return Err(CanonicalizeError::unsupported(
            "public day-1 write canonicalizer only supports direct table targets",
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
            "surface '{surface_name}' is not registered in public SurfaceRegistry"
        ))
    })
}

fn bind_update_surface(
    update: &Update,
    registry: &SurfaceRegistry,
) -> Result<SurfaceBinding, CanonicalizeError> {
    bind_table_with_joins_surface(&update.table, registry)
}

fn bind_delete_surface(
    delete: &Delete,
    registry: &SurfaceRegistry,
) -> Result<SurfaceBinding, CanonicalizeError> {
    let tables = match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    let [table] = tables.as_slice() else {
        return Err(CanonicalizeError::unsupported(
            "public day-1 delete canonicalizer requires a single table target",
        ));
    };
    bind_table_with_joins_surface(table, registry)
}

fn bind_table_with_joins_surface(
    table: &TableWithJoins,
    registry: &SurfaceRegistry,
) -> Result<SurfaceBinding, CanonicalizeError> {
    if !table.joins.is_empty() {
        return Err(CanonicalizeError::unsupported(
            "public day-1 write canonicalizer does not support JOIN targets",
        ));
    }
    let TableFactor::Table { name, .. } = &table.relation else {
        return Err(CanonicalizeError::unsupported(
            "public day-1 write canonicalizer only supports direct table targets",
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
            "surface '{surface_name}' is not registered in public SurfaceRegistry"
        ))
    })
}

fn validate_semantic_write_surface(
    surface_binding: &SurfaceBinding,
    surface_rule: impl Fn(&SurfaceBinding) -> bool,
) -> Result<(), CanonicalizeError> {
    if !surface_binding.resolution_capabilities.semantic_write {
        return Err(CanonicalizeError::unsupported(format!(
            "surface '{}' is not writable in public execution",
            surface_binding.descriptor.public_name
        )));
    }
    if !matches!(
        surface_binding.descriptor.surface_family,
        SurfaceFamily::State
            | SurfaceFamily::Entity
            | SurfaceFamily::Admin
            | SurfaceFamily::Filesystem
    ) {
        return Err(CanonicalizeError::unsupported(
            "public write canonicalizer only supports migrated state, entity, admin, and filesystem surfaces",
        ));
    }
    if !surface_rule(surface_binding) {
        return Err(CanonicalizeError::unsupported(format!(
            "public day-1 write canonicalizer does not yet support '{}' for this operation",
            surface_binding.descriptor.public_name
        )));
    }
    Ok(())
}

fn reject_filesystem_history_write(
    surface_binding: &SurfaceBinding,
    operation: &str,
) -> Result<(), CanonicalizeError> {
    if surface_binding.descriptor.surface_family == SurfaceFamily::Filesystem
        && surface_binding.descriptor.surface_variant
            == crate::sql::catalog::SurfaceVariant::History
    {
        return Err(CanonicalizeError::unsupported(format!(
            "{} does not support {operation}",
            surface_binding.descriptor.public_name
        )));
    }

    Ok(())
}

fn insert_write_surface_supported(surface_binding: &SurfaceBinding) -> bool {
    matches!(
        surface_binding.descriptor.surface_family,
        SurfaceFamily::Entity
    ) || matches!(
        surface_binding.descriptor.public_name.as_str(),
        "lix_state"
            | "lix_state_by_version"
            | "lix_version"
            | "lix_file"
            | "lix_file_by_version"
            | "lix_directory"
            | "lix_directory_by_version"
    )
}

fn update_delete_surface_supported(surface_binding: &SurfaceBinding) -> bool {
    matches!(
        surface_binding.descriptor.surface_family,
        SurfaceFamily::Entity
    ) || matches!(
        surface_binding.descriptor.public_name.as_str(),
        "lix_state"
            | "lix_state_by_version"
            | "lix_version"
            | "lix_file"
            | "lix_file_by_version"
            | "lix_directory"
            | "lix_directory_by_version"
    )
}

fn insert_payloads(
    surface_binding: &SurfaceBinding,
    insert: &Insert,
    params: &[Value],
    execution_context: &ExecutionContext,
) -> Result<Vec<BTreeMap<String, Value>>, CanonicalizeError> {
    let Some(source) = &insert.source else {
        if insert.columns.is_empty() {
            return Ok(vec![BTreeMap::new()]);
        }
        return Err(CanonicalizeError::unsupported(
            "public day-1 write canonicalizer requires an INSERT source when columns are specified",
        ));
    };

    let mut placeholder_state = PlaceholderState::new();
    insert_source_payloads(
        surface_binding,
        insert.columns.as_slice(),
        source,
        params,
        execution_context,
        &mut placeholder_state,
    )
}

fn insert_source_payloads(
    surface_binding: &SurfaceBinding,
    insert_columns: &[Ident],
    source: &Query,
    params: &[Value],
    execution_context: &ExecutionContext,
    placeholder_state: &mut PlaceholderState,
) -> Result<Vec<BTreeMap<String, Value>>, CanonicalizeError> {
    reject_unsupported_insert_source_query_clauses(source)?;

    match source.body.as_ref() {
        SetExpr::Values(values) => {
            if values.rows.is_empty() {
                return Err(CanonicalizeError::unsupported(
                    "public day-1 write canonicalizer requires at least one insert row",
                ));
            }

            let mut payloads = Vec::with_capacity(values.rows.len());
            for row in &values.rows {
                payloads.push(insert_payload_from_row_exprs(
                    surface_binding,
                    insert_columns,
                    row.iter(),
                    params,
                    execution_context,
                    placeholder_state,
                )?);
            }
            Ok(payloads)
        }
        SetExpr::Select(select) => Ok(vec![insert_payload_from_select(
            surface_binding,
            insert_columns,
            select,
            params,
            execution_context,
            placeholder_state,
        )?]),
        SetExpr::Query(query) => insert_source_payloads(
            surface_binding,
            insert_columns,
            query,
            params,
            execution_context,
            placeholder_state,
        ),
        SetExpr::SetOperation { .. } => Err(CanonicalizeError::unsupported(
            "public day-1 insert canonicalizer does not yet support set-operation INSERT sources",
        )),
        _ => Err(CanonicalizeError::unsupported(
            "public day-1 insert canonicalizer only supports VALUES and single-row SELECT insert sources",
        )),
    }
}

fn reject_unsupported_insert_source_query_clauses(source: &Query) -> Result<(), CanonicalizeError> {
    if source.with.is_some()
        || source.fetch.is_some()
        || !source.locks.is_empty()
        || source.for_clause.is_some()
        || source.settings.is_some()
        || source.format_clause.is_some()
    {
        return Err(CanonicalizeError::unsupported(
            "public day-1 insert canonicalizer does not support WITH/FETCH/LOCK/FOR/SETTINGS/FORMAT clauses in INSERT sources",
        ));
    }

    if source.order_by.is_some()
        || source.limit_clause.is_some()
        || !source.pipe_operators.is_empty()
    {
        return Err(CanonicalizeError::unsupported(
            "public day-1 insert canonicalizer does not support ORDER BY/LIMIT/pipe operators in INSERT sources",
        ));
    }

    Ok(())
}

fn insert_payload_from_select(
    surface_binding: &SurfaceBinding,
    insert_columns: &[Ident],
    select: &Select,
    params: &[Value],
    execution_context: &ExecutionContext,
    placeholder_state: &mut PlaceholderState,
) -> Result<BTreeMap<String, Value>, CanonicalizeError> {
    if !select.from.is_empty() {
        return Err(CanonicalizeError::unsupported(
            "public day-1 insert canonicalizer does not support table-backed INSERT ... SELECT sources",
        ));
    }

    if select.distinct.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || select.selection.is_some()
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
            "public day-1 insert canonicalizer only supports single-row projection SELECT sources",
        ));
    }

    match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers) if exprs.is_empty() && modifiers.is_empty() => {}
        GroupByExpr::Expressions(_, _) | GroupByExpr::All(_) => {
            return Err(CanonicalizeError::unsupported(
                "public day-1 insert canonicalizer does not support GROUP BY in INSERT ... SELECT sources",
            ));
        }
    }

    let projection_exprs = select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => Ok(expr),
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                Err(CanonicalizeError::unsupported(
                    "public day-1 insert canonicalizer only supports expression projection items in INSERT ... SELECT sources",
                ))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    insert_payload_from_row_exprs(
        surface_binding,
        insert_columns,
        projection_exprs,
        params,
        execution_context,
        placeholder_state,
    )
}

fn insert_payload_from_row_exprs<'a>(
    surface_binding: &SurfaceBinding,
    insert_columns: &[Ident],
    exprs: impl IntoIterator<Item = &'a Expr>,
    params: &[Value],
    execution_context: &ExecutionContext,
    placeholder_state: &mut PlaceholderState,
) -> Result<BTreeMap<String, Value>, CanonicalizeError> {
    let exprs = exprs.into_iter().collect::<Vec<_>>();
    if exprs.len() != insert_columns.len() {
        return Err(CanonicalizeError::unsupported(
            "public day-1 write canonicalizer requires one value per inserted column",
        ));
    }

    let mut payload = BTreeMap::new();
    for (column, expr) in insert_columns.iter().zip(exprs) {
        reject_forbidden_default_state_write_column(surface_binding, &column.value, "insert")?;
        let key = canonical_write_column_key(surface_binding, &column.value)?;
        let value = match expr_to_value(expr, params, execution_context, placeholder_state) {
            Ok(value) => value,
            Err(error) if key == "data" => return Err(filesystem_file_data_error(error)),
            Err(error) => return Err(error),
        };
        payload.insert(key, value);
    }

    Ok(payload)
}

fn write_mode_request_for_insert_payloads(
    surface_binding: &SurfaceBinding,
    payloads: &[BTreeMap<String, Value>],
) -> Result<WriteModeRequest, CanonicalizeError> {
    let Some(first) = payloads.first() else {
        return Err(CanonicalizeError::unsupported(
            "public day-1 write canonicalizer requires at least one insert row",
        ));
    };
    let mode = write_mode_request_for_surface_and_selector(surface_binding, first, None);
    for payload in &payloads[1..] {
        let row_mode = write_mode_request_for_surface_and_selector(surface_binding, payload, None);
        if row_mode != mode {
            return Ok(WriteModeRequest::Auto);
        }
    }
    Ok(mode)
}

fn assignment_payload(
    surface_binding: &SurfaceBinding,
    assignments: &[sqlparser::ast::Assignment],
    params: &[Value],
    execution_context: &ExecutionContext,
    placeholder_state: &mut PlaceholderState,
) -> Result<BTreeMap<String, Value>, CanonicalizeError> {
    if assignments.is_empty() {
        return Err(CanonicalizeError::unsupported(
            "public day-1 update canonicalizer requires at least one assignment",
        ));
    }

    let mut payload = BTreeMap::new();
    for assignment in assignments {
        let AssignmentTarget::ColumnName(column_name) = &assignment.target else {
            return Err(CanonicalizeError::unsupported(
                "public day-1 update canonicalizer only supports named column assignments",
            ));
        };
        let raw_key = column_name
            .0
            .last()
            .and_then(ObjectNamePart::as_ident)
            .map(|ident| ident.value.clone())
            .ok_or_else(|| {
                CanonicalizeError::unsupported(
                    "public day-1 update canonicalizer requires named assignment columns",
                )
            })?;
        reject_forbidden_default_state_write_column(surface_binding, &raw_key, "update")?;
        let key = canonical_write_column_key(surface_binding, &raw_key)?;
        let value = match expr_to_value(
            &assignment.value,
            params,
            execution_context,
            placeholder_state,
        ) {
            Ok(value) => value,
            Err(error) if key == "data" => return Err(filesystem_file_data_error(error)),
            Err(error) => return Err(error),
        };
        payload.insert(key, value);
    }
    Ok(payload)
}

fn write_selector(
    surface_binding: &SurfaceBinding,
    expr: &Expr,
    params: &[Value],
    execution_context: &ExecutionContext,
    placeholder_state: &mut PlaceholderState,
) -> Result<WriteSelector, CanonicalizeError> {
    reject_forbidden_default_state_selector(surface_binding, expr)?;
    reject_unknown_selector_columns(surface_binding, expr)?;
    let mut exact_filters = BTreeMap::new();
    let exact_only = collect_exact_selector_filters(
        surface_binding,
        expr,
        params,
        execution_context,
        placeholder_state,
        &mut exact_filters,
    );

    Ok(WriteSelector {
        residual_predicates: vec![expr.clone()],
        exact_filters,
        exact_only,
    })
}

fn collect_exact_selector_filters(
    surface_binding: &SurfaceBinding,
    expr: &Expr,
    params: &[Value],
    execution_context: &ExecutionContext,
    placeholder_state: &mut PlaceholderState,
    exact_filters: &mut BTreeMap<String, Value>,
) -> bool {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect_exact_selector_filters(
                surface_binding,
                left,
                params,
                execution_context,
                placeholder_state,
                exact_filters,
            ) && collect_exact_selector_filters(
                surface_binding,
                right,
                params,
                execution_context,
                placeholder_state,
                exact_filters,
            )
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            let Some(raw_column) =
                selector_column_name(left).or_else(|| selector_column_name(right))
            else {
                return false;
            };
            let Ok(column) = canonical_write_column_key(surface_binding, &raw_column) else {
                return false;
            };
            if !selector_column_is_supported(surface_binding, &column) {
                return false;
            }
            let value_expr = if selector_column_name(left).is_some() {
                right
            } else {
                left
            };
            let Ok(value) = expr_to_value(value_expr, params, execution_context, placeholder_state)
            else {
                return false;
            };
            match exact_filters.get(&column) {
                Some(existing) if existing != &value => false,
                Some(_) => true,
                None => {
                    exact_filters.insert(column, value);
                    true
                }
            }
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            if list.len() != 1 {
                return false;
            }
            let Some(raw_column) = selector_column_name(expr) else {
                return false;
            };
            let Ok(column) = canonical_write_column_key(surface_binding, &raw_column) else {
                return false;
            };
            if !selector_column_is_supported(surface_binding, &column) {
                return false;
            }
            let Ok(value) = expr_to_value(&list[0], params, execution_context, placeholder_state)
            else {
                return false;
            };
            match exact_filters.get(&column) {
                Some(existing) if existing != &value => false,
                Some(_) => true,
                None => {
                    exact_filters.insert(column, value);
                    true
                }
            }
        }
        Expr::Nested(inner) => collect_exact_selector_filters(
            surface_binding,
            inner,
            params,
            execution_context,
            placeholder_state,
            exact_filters,
        ),
        _ => false,
    }
}

fn selector_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(identifier) => Some(identifier.value.to_ascii_lowercase()),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|identifier| identifier.value.to_ascii_lowercase()),
        Expr::Nested(inner) => selector_column_name(inner),
        _ => None,
    }
}

fn reject_unknown_selector_columns(
    surface_binding: &SurfaceBinding,
    expr: &Expr,
) -> Result<(), CanonicalizeError> {
    if let Some(raw_column) = selector_column_name(expr) {
        match canonical_write_column_key(surface_binding, &raw_column) {
            Ok(column) if selector_column_is_supported(surface_binding, &column) => {}
            Ok(_) | Err(_) => return Err(unknown_write_column_error(surface_binding, &raw_column)),
        }
    }

    match expr {
        Expr::BinaryOp { left, right, .. } => {
            reject_unknown_selector_columns(surface_binding, left)?;
            reject_unknown_selector_columns(surface_binding, right)?;
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. } => reject_unknown_selector_columns(surface_binding, expr)?,
        Expr::InList { expr, list, .. } => {
            reject_unknown_selector_columns(surface_binding, expr)?;
            for item in list {
                reject_unknown_selector_columns(surface_binding, item)?;
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            reject_unknown_selector_columns(surface_binding, expr)?;
            reject_unknown_selector_columns(surface_binding, low)?;
            reject_unknown_selector_columns(surface_binding, high)?;
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            reject_unknown_selector_columns(surface_binding, expr)?;
            reject_unknown_selector_columns(surface_binding, pattern)?;
        }
        _ => {}
    }

    Ok(())
}

fn reject_forbidden_default_state_write_column(
    surface_binding: &SurfaceBinding,
    raw_column: &str,
    operation: &str,
) -> Result<(), CanonicalizeError> {
    if !default_state_surface_rejects_version_id(surface_binding) {
        return Ok(());
    }

    if matches!(
        raw_column.to_ascii_lowercase().as_str(),
        "version_id" | "lixcol_version_id"
    ) {
        return Err(CanonicalizeError::unsupported(format!(
            "lix_state {operation} cannot set version_id; active version is resolved automatically"
        )));
    }

    Ok(())
}

fn reject_forbidden_default_state_selector(
    surface_binding: &SurfaceBinding,
    expr: &Expr,
) -> Result<(), CanonicalizeError> {
    if default_state_surface_rejects_version_id(surface_binding)
        && contains_version_id_reference(expr)
    {
        return Err(CanonicalizeError::unsupported(
            "lix_state does not expose version_id; use lix_state_by_version for explicit version filters",
        ));
    }

    Ok(())
}

fn default_state_surface_rejects_version_id(surface_binding: &SurfaceBinding) -> bool {
    surface_binding
        .descriptor
        .public_name
        .eq_ignore_ascii_case("lix_state")
}

fn contains_version_id_reference(expr: &Expr) -> bool {
    contains_column_reference(expr, "version_id")
        || contains_column_reference(expr, "lixcol_version_id")
}

fn contains_column_reference(expr: &Expr, column: &str) -> bool {
    match expr {
        Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case(column),
        Expr::CompoundIdentifier(idents) => idents
            .last()
            .map(|ident| ident.value.eq_ignore_ascii_case(column))
            .unwrap_or(false),
        Expr::BinaryOp { left, right, .. } => {
            contains_column_reference(left, column) || contains_column_reference(right, column)
        }
        Expr::UnaryOp { expr, .. } => contains_column_reference(expr, column),
        Expr::Nested(inner) => contains_column_reference(inner, column),
        Expr::InList { expr, list, .. } => {
            contains_column_reference(expr, column)
                || list
                    .iter()
                    .any(|item| contains_column_reference(item, column))
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            contains_column_reference(expr, column)
                || contains_column_reference(low, column)
                || contains_column_reference(high, column)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            contains_column_reference(expr, column) || contains_column_reference(pattern, column)
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => contains_column_reference(inner, column),
        Expr::Cast { expr, .. } => contains_column_reference(expr, column),
        Expr::Function(_) => false,
        _ => false,
    }
}

fn selector_column_is_supported(surface_binding: &SurfaceBinding, column: &str) -> bool {
    match surface_binding.descriptor.surface_family {
        SurfaceFamily::State => matches!(
            column,
            "entity_id"
                | "schema_key"
                | "file_id"
                | "version_id"
                | "plugin_key"
                | "schema_version"
                | "global"
                | "untracked"
        ),
        SurfaceFamily::Entity => surface_binding
            .exposed_columns
            .iter()
            .chain(surface_binding.descriptor.hidden_columns.iter())
            .any(|candidate| {
                canonical_write_column_key(surface_binding, candidate)
                    .map(|candidate| candidate == column)
                    .unwrap_or(false)
            }),
        SurfaceFamily::Admin => match surface_binding.descriptor.public_name.as_str() {
            "lix_version" => column == "id",
            _ => false,
        },
        SurfaceFamily::Filesystem => match surface_binding.descriptor.public_name.as_str() {
            "lix_file" | "lix_file_by_version" => matches!(
                column,
                "id" | "path" | "hidden" | "metadata" | "data" | "version_id" | "untracked"
            ),
            "lix_directory" | "lix_directory_by_version" => matches!(
                column,
                "id" | "path" | "parent_id" | "name" | "hidden" | "version_id" | "untracked"
            ),
            _ => false,
        },
        SurfaceFamily::Change => false,
    }
}

fn canonical_write_column_key(
    surface_binding: &SurfaceBinding,
    raw_column: &str,
) -> Result<String, CanonicalizeError> {
    let column = raw_column.to_ascii_lowercase();
    let canonical = candidate_column_key(&column);

    match surface_binding.descriptor.surface_family {
        SurfaceFamily::State => {
            let supported = write_surface_supports_column(surface_binding, raw_column, &canonical);
            if supported {
                Ok(canonical)
            } else {
                Err(unknown_write_column_error(surface_binding, raw_column))
            }
        }
        SurfaceFamily::Entity => {
            let supported = write_surface_supports_column(surface_binding, raw_column, &canonical);
            if supported {
                Ok(canonical)
            } else {
                Err(unknown_write_column_error(surface_binding, raw_column))
            }
        }
        SurfaceFamily::Admin => match surface_binding.descriptor.public_name.as_str() {
            "lix_version" => match canonical.as_str() {
                "id" | "name" | "hidden" | "commit_id" => Ok(canonical.clone()),
                _ => Err(CanonicalizeError::unsupported(format!(
                    "public write canonicalizer does not support column '{raw_column}' on '{}'",
                    surface_binding.descriptor.public_name
                ))),
            },
            _ => Err(CanonicalizeError::unsupported(format!(
                "public write canonicalizer does not yet support '{}' writes",
                surface_binding.descriptor.public_name
            ))),
        },
        SurfaceFamily::Filesystem => match surface_binding.descriptor.public_name.as_str() {
            "lix_file" | "lix_file_by_version" => match canonical.as_str() {
                "id" | "path" | "hidden" | "version_id" | "untracked" | "metadata" | "data" => {
                    Ok(canonical.clone())
                }
                _ => Err(unknown_write_column_error(surface_binding, raw_column)),
            },
            "lix_directory" | "lix_directory_by_version" => match canonical.as_str() {
                "id" | "path" | "parent_id" | "name" | "hidden" | "version_id" | "untracked"
                | "metadata" => Ok(canonical.clone()),
                _ => Err(unknown_write_column_error(surface_binding, raw_column)),
            },
            _ => Err(CanonicalizeError::unsupported(format!(
                "public write canonicalizer does not yet support '{}' writes",
                surface_binding.descriptor.public_name
            ))),
        },
        SurfaceFamily::Change => Err(CanonicalizeError::unsupported(format!(
            "public day-1 write canonicalizer does not support '{}' writes",
            surface_binding.descriptor.public_name
        ))),
    }
}

fn write_surface_supports_column(
    surface_binding: &SurfaceBinding,
    raw_column: &str,
    canonical_column: &str,
) -> bool {
    surface_binding
        .exposed_columns
        .iter()
        .chain(surface_binding.descriptor.hidden_columns.iter())
        .any(|candidate| {
            candidate.eq_ignore_ascii_case(raw_column)
                || candidate_column_key(candidate) == canonical_column
        })
}

fn candidate_column_key(candidate: &str) -> String {
    match candidate.to_ascii_lowercase().as_str() {
        "lixcol_entity_id" => "entity_id",
        "lixcol_schema_key" => "schema_key",
        "lixcol_file_id" => "file_id",
        "lixcol_version_id" => "version_id",
        "lixcol_plugin_key" => "plugin_key",
        "lixcol_schema_version" => "schema_version",
        "lixcol_global" => "global",
        "lixcol_writer_key" => "writer_key",
        "lixcol_untracked" => "untracked",
        "lixcol_metadata" => "metadata",
        other => other,
    }
    .to_string()
}

fn unknown_write_column_error(
    surface_binding: &SurfaceBinding,
    raw_column: &str,
) -> CanonicalizeError {
    CanonicalizeError::unsupported(format!(
        "strict rewrite violation: unknown column '{raw_column}' on '{}'",
        surface_binding.descriptor.public_name
    ))
}

fn filesystem_file_data_error(_error: CanonicalizeError) -> CanonicalizeError {
    CanonicalizeError::unsupported(crate::errors::FILE_DATA_EXPECTS_BYTES_MESSAGE)
}

pub(crate) fn evaluate_public_write_expr_to_value(
    expr: &Expr,
    params: &[Value],
    execution_context: &ExecutionContext,
) -> Result<Value, CanonicalizeError> {
    let mut placeholder_state = PlaceholderState::new();
    expr_to_value(expr, params, execution_context, &mut placeholder_state)
}

fn expr_to_value(
    expr: &Expr,
    params: &[Value],
    execution_context: &ExecutionContext,
    placeholder_state: &mut PlaceholderState,
) -> Result<Value, CanonicalizeError> {
    match expr {
        Expr::Value(value) => sql_value_to_engine_value(&value.value, params, placeholder_state),
        Expr::Function(function)
            if function_name(function)
                .is_some_and(|name| name.eq_ignore_ascii_case("lix_text_encode")) =>
        {
            let encoded = single_function_arg_expr(function, "lix_text_encode")?;
            match expr_to_value(encoded, params, execution_context, placeholder_state)? {
                Value::Text(text) => Ok(Value::Blob(text.into_bytes())),
                Value::Blob(bytes) => Ok(Value::Blob(bytes)),
                Value::Null => Ok(Value::Null),
                _ => Err(CanonicalizeError::unsupported(
                    "public day-1 write canonicalizer only supports string/blob/null lix_text_encode arguments",
                )),
            }
        }
        Expr::Function(function)
            if function_name(function)
                .is_some_and(|name| name.eq_ignore_ascii_case("lix_json")) =>
        {
            let json_expr = single_function_arg_expr(function, "lix_json")?;
            let value = expr_to_value(json_expr, params, execution_context, placeholder_state)?;
            value_to_json_value(value)
        }
        Expr::Function(function)
            if function_name(function)
                .is_some_and(|name| name.eq_ignore_ascii_case("lix_active_version_id")) =>
        {
            ensure_no_function_args(function, "lix_active_version_id")?;
            execution_context
                .requested_version_id
                .clone()
                .map(Value::Text)
                .ok_or_else(|| {
                    CanonicalizeError::unsupported(
                        "lix_active_version_id() requires a requested session version",
                    )
                })
        }
        Expr::Function(function)
            if function_name(function)
                .is_some_and(|name| name.eq_ignore_ascii_case("lix_active_account_ids")) =>
        {
            ensure_no_function_args(function, "lix_active_account_ids")?;
            Ok(Value::Json(serde_json::Value::Array(
                execution_context
                    .active_account_ids
                    .iter()
                    .cloned()
                    .map(serde_json::Value::String)
                    .collect(),
            )))
        }
        Expr::Nested(inner) => expr_to_value(inner, params, execution_context, placeholder_state),
        Expr::UnaryOp { op, expr } if op.to_string() == "-" => {
            match expr_to_value(expr, params, execution_context, placeholder_state)? {
                Value::Integer(value) => Ok(Value::Integer(-value)),
                Value::Real(value) => Ok(Value::Real(-value)),
                _ => Err(CanonicalizeError::unsupported(
                    "public day-1 write canonicalizer only supports numeric unary minus literals",
                )),
            }
        }
        _ => Err(CanonicalizeError::unsupported(
            "public day-1 write canonicalizer only supports literal and placeholder insert row expressions",
        )),
    }
}

fn value_to_json_value(value: Value) -> Result<Value, CanonicalizeError> {
    match value {
        Value::Json(value) => Ok(Value::Json(value)),
        Value::Text(text) => serde_json::from_str::<serde_json::Value>(&text)
            .map(Value::Json)
            .map_err(|error| {
                CanonicalizeError::unsupported(format!(
                    "lix_json() requires valid JSON text: {error}"
                ))
            }),
        Value::Null => Ok(Value::Json(serde_json::Value::Null)),
        Value::Boolean(value) => Ok(Value::Json(serde_json::Value::Bool(value))),
        Value::Integer(value) => Ok(Value::Json(serde_json::Value::Number(value.into()))),
        Value::Real(value) => serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .map(Value::Json)
            .ok_or_else(|| {
                CanonicalizeError::unsupported("lix_json() does not support NaN/inf numeric values")
            }),
        Value::Blob(_) => Err(CanonicalizeError::unsupported(
            "lix_json() does not support blob arguments",
        )),
    }
}

fn function_name(function: &sqlparser::ast::Function) -> Option<&str> {
    function
        .name
        .0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.as_str())
}

fn single_function_arg_expr<'a>(
    function: &'a sqlparser::ast::Function,
    function_name: &str,
) -> Result<&'a Expr, CanonicalizeError> {
    let args = match &function.args {
        FunctionArguments::List(list) if list.clauses.is_empty() => list.args.as_slice(),
        FunctionArguments::None => {
            return Err(CanonicalizeError::unsupported(format!(
                "public day-1 write canonicalizer requires one argument for {function_name}",
            )));
        }
        _ => {
            return Err(CanonicalizeError::unsupported(format!(
                "public day-1 write canonicalizer does not support complex arguments for {function_name}",
            )));
        }
    };
    if args.len() != 1 {
        return Err(CanonicalizeError::unsupported(format!(
            "public day-1 write canonicalizer requires one argument for {function_name}",
        )));
    }
    match &args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
        | FunctionArg::Named {
            arg: FunctionArgExpr::Expr(expr),
            ..
        }
        | FunctionArg::ExprNamed {
            arg: FunctionArgExpr::Expr(expr),
            ..
        } => Ok(expr),
        _ => Err(CanonicalizeError::unsupported(format!(
            "public day-1 write canonicalizer only supports expression arguments for {function_name}",
        ))),
    }
}

fn ensure_no_function_args(
    function: &sqlparser::ast::Function,
    function_name: &str,
) -> Result<(), CanonicalizeError> {
    match &function.args {
        FunctionArguments::None => Ok(()),
        FunctionArguments::List(list) if list.clauses.is_empty() && list.args.is_empty() => Ok(()),
        _ => Err(CanonicalizeError::unsupported(format!(
            "public day-1 write canonicalizer requires zero arguments for {function_name}",
        ))),
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
                    "public day-1 write canonicalizer could not parse numeric literal '{raw}'"
                )))
            }
        }
        SqlValue::Boolean(value) => Ok(Value::Boolean(*value)),
        SqlValue::Null => Ok(Value::Null),
        SqlValue::SingleQuotedByteStringLiteral(value)
        | SqlValue::DoubleQuotedByteStringLiteral(value)
        | SqlValue::TripleSingleQuotedByteStringLiteral(value)
        | SqlValue::TripleDoubleQuotedByteStringLiteral(value) => {
            Ok(Value::Blob(value.clone().into_bytes()))
        }
        SqlValue::HexStringLiteral(value) => Ok(Value::Blob(
            parse_hex_literal(value).map_err(CanonicalizeError::unsupported)?,
        )),
        SqlValue::Placeholder(token) => {
            let index = resolve_placeholder_index(token, params.len(), placeholder_state).map_err(
                |err| {
                    CanonicalizeError::unsupported(format!(
                        "public day-1 write canonicalizer could not bind placeholder: {}",
                        err.description
                    ))
                },
            )?;
            params.get(index).cloned().ok_or_else(|| {
                CanonicalizeError::unsupported(format!(
                    "public day-1 write canonicalizer placeholder index {} was out of bounds",
                    index + 1
                ))
            })
        }
        _ => Err(CanonicalizeError::unsupported(
            "public day-1 write canonicalizer only supports string, numeric, boolean, null, blob, and placeholder insert row values",
        )),
    }
}

fn parse_hex_literal(text: &str) -> Result<Vec<u8>, String> {
    if text.len() % 2 != 0 {
        return Err(format!(
            "hex literal must contain an even number of digits, got {}",
            text.len()
        ));
    }

    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    let mut index = 0;
    while index < bytes.len() {
        let hi = hex_nibble(bytes[index])?;
        let lo = hex_nibble(bytes[index + 1])?;
        out.push((hi << 4) | lo);
        index += 2;
    }
    Ok(out)
}

fn hex_nibble(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(format!("invalid hex digit '{}'", char::from(byte))),
    }
}

fn supports_implicit_admin_selector(surface_binding: &SurfaceBinding) -> bool {
    let _ = surface_binding;
    false
}

fn write_mode_request_for_surface(
    surface_binding: &SurfaceBinding,
    payload: &BTreeMap<String, Value>,
) -> WriteModeRequest {
    let _ = surface_binding;
    write_mode_request_from_payload(payload)
}

fn write_mode_request_for_surface_and_selector(
    surface_binding: &SurfaceBinding,
    payload: &BTreeMap<String, Value>,
    selector: Option<&WriteSelector>,
) -> WriteModeRequest {
    let payload_mode = write_mode_request_for_surface(surface_binding, payload);
    if !matches!(payload_mode, WriteModeRequest::Auto) {
        return payload_mode;
    }

    match selector
        .and_then(|selector| selector.exact_filters.get("untracked"))
        .and_then(value_as_bool)
    {
        Some(true) => WriteModeRequest::ForceUntracked,
        Some(false) => WriteModeRequest::ForceTracked,
        None => payload_mode,
    }
}

fn value_as_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Boolean(value) => Some(*value),
        Value::Integer(value) => Some(*value != 0),
        Value::Text(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" => Some(true),
            "0" | "false" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn write_mode_request_from_payload(payload: &BTreeMap<String, Value>) -> WriteModeRequest {
    match payload
        .get("untracked")
        .or_else(|| payload.get("lixcol_untracked"))
    {
        Some(Value::Boolean(true)) => WriteModeRequest::ForceUntracked,
        Some(Value::Boolean(false)) => WriteModeRequest::ForceTracked,
        Some(Value::Integer(value)) if *value != 0 => WriteModeRequest::ForceUntracked,
        Some(Value::Integer(_)) => WriteModeRequest::ForceTracked,
        Some(Value::Text(value))
            if matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true") =>
        {
            WriteModeRequest::ForceUntracked
        }
        Some(Value::Text(value))
            if matches!(value.trim().to_ascii_lowercase().as_str(), "0" | "false") =>
        {
            WriteModeRequest::ForceTracked
        }
        _ => WriteModeRequest::Auto,
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
                    "mixed wildcard projections are not supported by the public day-1 canonicalizer",
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

fn normalized_public_read_query(
    query: &Query,
    select: &Select,
) -> Result<NormalizedPublicReadQuery, CanonicalizeError> {
    let TableFactor::Table { alias, .. } = &select.from[0].relation else {
        return Err(CanonicalizeError::unsupported(
            "public day-1 canonicalizer only supports direct table references",
        ));
    };

    Ok(NormalizedPublicReadQuery {
        source_alias: alias.clone(),
        projection: select.projection.clone(),
        selection: select.selection.clone(),
        selection_predicates: select
            .selection
            .as_ref()
            .map(split_conjunctive_predicates)
            .unwrap_or_default(),
        group_by: select.group_by.clone(),
        having: select.having.clone(),
        order_by: query.order_by.clone(),
        limit_clause: query.limit_clause.clone(),
    })
}

fn sort_keys(order_by: Option<&OrderBy>) -> Result<Option<Vec<SortKey>>, CanonicalizeError> {
    let Some(order_by) = order_by else {
        return Ok(None);
    };

    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return Err(CanonicalizeError::unsupported(
            "ORDER BY ALL is not supported by the public day-1 canonicalizer",
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
                    "LIMIT BY is not supported by the public day-1 canonicalizer",
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
            "public day-1 canonicalizer only supports literal LIMIT/OFFSET values",
        ));
    };

    let sqlparser::ast::Value::Number(raw, _) = &value.value else {
        return Err(CanonicalizeError::unsupported(
            "public day-1 canonicalizer only supports numeric LIMIT/OFFSET values",
        ));
    };

    raw.parse::<u64>().map_err(|_| {
        CanonicalizeError::unsupported(format!(
            "public day-1 canonicalizer could not parse numeric LIMIT/OFFSET value '{raw}'"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::{canonicalize_read, canonicalize_write};
    use crate::sql::catalog::{DynamicEntitySurfaceSpec, SurfaceRegistry};
    use crate::sql::binder::bind_statement;
    use crate::sql::semantic_ir::{BoundStatement, ExecutionContext};
    use crate::sql::logical_plan::public_ir::{
        MutationPayload, ReadContract, ReadPlan, VersionScope, WriteModeRequest, WriteOperationKind,
    };
    use crate::Value;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn bound_statement(sql: &str) -> BoundStatement {
        let mut statements = crate::sql::parser::parse_sql_script(sql).expect("SQL should parse");
        let statement = statements.pop().expect("single statement");
        bind_statement(statement, Vec::new(), ExecutionContext::default())
    }

    fn bound_statement_with_params(sql: &str, params: Vec<Value>) -> BoundStatement {
        let mut statements = crate::sql::parser::parse_sql_script(sql).expect("SQL should parse");
        let statement = statements.pop().expect("single statement");
        bind_statement(statement, params, ExecutionContext::default())
    }

    fn bound_statement_with_context(
        sql: &str,
        execution_context: ExecutionContext,
    ) -> BoundStatement {
        let mut statements = crate::sql::parser::parse_sql_script(sql).expect("SQL should parse");
        let statement = statements.pop().expect("single statement");
        bind_statement(statement, Vec::new(), execution_context)
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
        assert!(canonicalized.query.source_alias.is_none());
        assert_eq!(canonicalized.query.projection.len(), 2);
        assert_eq!(
            canonicalized
                .query
                .selection
                .as_ref()
                .map(ToString::to_string),
            Some("version_id = 'v1'".to_string())
        );
        assert_eq!(
            canonicalized
                .query
                .selection_predicates
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec!["version_id = 'v1'".to_string()]
        );
        assert!(canonicalized.query.order_by.is_some());
        assert!(canonicalized.query.limit_clause.is_some());

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
            column_types: BTreeMap::new(),
            predicate_overrides: Vec::new(),
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
    fn allows_nested_subqueries_for_day_one_shell() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let canonicalized = canonicalize_read(
            bound_statement(
                "SELECT entity_id FROM lix_state WHERE entity_id IN (SELECT entity_id FROM lix_state_by_version)",
            ),
            &registry,
        )
        .expect("subqueries should canonicalize");

        assert_eq!(
            canonicalized.surface_binding.descriptor.public_name,
            "lix_state"
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
        assert_eq!(
            canonicalized.write_command.requested_mode,
            WriteModeRequest::Auto
        );
        let MutationPayload::InsertRows(rows) = &canonicalized.write_command.payload else {
            panic!("expected insert rows payload");
        };
        let [payload] = rows.as_slice() else {
            panic!("expected exactly one insert row");
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
    fn canonicalizes_state_insert_with_lix_active_version_id() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let canonicalized = canonicalize_write(
            bound_statement_with_context(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'entity-1', 'lix_key_value', 'lix', lix_active_version_id(), 'lix', '{\"key\":\"hello\"}', '1'\
                 )",
                ExecutionContext {
                    requested_version_id: Some("version-active".to_string()),
                    ..ExecutionContext::default()
                },
            ),
            &registry,
        )
        .expect("state insert with active version function should canonicalize");

        let MutationPayload::InsertRows(rows) = &canonicalized.write_command.payload else {
            panic!("expected insert rows payload");
        };
        let [payload] = rows.as_slice() else {
            panic!("expected exactly one insert row");
        };
        assert_eq!(
            payload.get("version_id"),
            Some(&Value::Text("version-active".to_string()))
        );
    }

    #[test]
    fn canonicalizes_parameterized_state_insert_select_into_write_command() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let canonicalized = canonicalize_write(
            bound_statement_with_params(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) SELECT $1, $2, $3, $4, $5, $6, $7",
                vec![
                    Value::Text("entity-1".to_string()),
                    Value::Text("lix_key_value".to_string()),
                    Value::Text("lix".to_string()),
                    Value::Text("version-a".to_string()),
                    Value::Text("lix".to_string()),
                    Value::Text("{\"key\":\"hello\"}".to_string()),
                    Value::Text("1".to_string()),
                ],
            ),
            &registry,
        )
        .expect("state insert select should canonicalize");

        assert_eq!(
            canonicalized.surface_binding.descriptor.public_name,
            "lix_state_by_version"
        );
        assert_eq!(
            canonicalized.write_command.operation_kind,
            WriteOperationKind::Insert
        );
        assert_eq!(
            canonicalized.write_command.requested_mode,
            WriteModeRequest::Auto
        );
        let MutationPayload::InsertRows(rows) = &canonicalized.write_command.payload else {
            panic!("expected insert rows payload");
        };
        let [payload] = rows.as_slice() else {
            panic!("expected exactly one insert row");
        };
        assert_eq!(
            payload.get("entity_id"),
            Some(&Value::Text("entity-1".to_string()))
        );
        assert_eq!(
            payload.get("snapshot_content"),
            Some(&Value::Text("{\"key\":\"hello\"}".to_string()))
        );
    }

    #[test]
    fn canonicalizes_multi_row_filesystem_insert_into_insert_rows() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let canonicalized = canonicalize_write(
            bound_statement(
                "INSERT INTO lix_file (id, path, data) VALUES \
                 ('file-1', '/a.txt', X'01'), \
                 ('file-2', '/b.txt', X'02')",
            ),
            &registry,
        )
        .expect("multi-row filesystem insert should canonicalize");

        let MutationPayload::InsertRows(rows) = &canonicalized.write_command.payload else {
            panic!("expected insert rows payload");
        };
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("id"), Some(&Value::Text("file-1".to_string())));
        assert_eq!(
            rows[1].get("path"),
            Some(&Value::Text("/b.txt".to_string()))
        );
    }

    #[test]
    fn canonicalizes_multi_row_insert_with_mixed_trackedness_as_auto_mode() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let canonicalized = canonicalize_write(
            bound_statement(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked\
                 ) VALUES \
                 ('entity-tracked', 'lix_key_value', 'lix', 'version-a', 'lix', '{\"key\":\"tracked\"}', '1', false), \
                 ('entity-untracked', 'lix_key_value', 'lix', 'version-b', 'lix', '{\"key\":\"untracked\"}', '1', true)",
            ),
            &registry,
        )
        .expect("mixed trackedness insert should canonicalize");

        assert_eq!(
            canonicalized.write_command.requested_mode,
            WriteModeRequest::Auto
        );
        let MutationPayload::InsertRows(rows) = &canonicalized.write_command.payload else {
            panic!("expected insert rows payload");
        };
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("untracked"), Some(&Value::Boolean(false)));
        assert_eq!(rows[1].get("untracked"), Some(&Value::Boolean(true)));
    }

    #[test]
    fn canonicalizes_insert_select_write_mode_from_payload() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let canonicalized = canonicalize_write(
            bound_statement(
                "INSERT INTO lix_file (id, path, data, untracked) \
                 SELECT 'file-1', '/a.txt', X'01', true",
            ),
            &registry,
        )
        .expect("filesystem insert select should canonicalize");

        assert_eq!(
            canonicalized.write_command.requested_mode,
            WriteModeRequest::ForceUntracked
        );
        let MutationPayload::InsertRows(rows) = &canonicalized.write_command.payload else {
            panic!("expected insert rows payload");
        };
        let [payload] = rows.as_slice() else {
            panic!("expected exactly one insert row");
        };
        assert_eq!(payload.get("id"), Some(&Value::Text("file-1".to_string())));
    }

    #[test]
    fn rejects_table_backed_insert_select_sources_with_precise_error() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let error = canonicalize_write(
            bound_statement(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) SELECT \
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version \
                 FROM lix_state_by_version",
            ),
            &registry,
        )
        .expect_err("table-backed insert select should be rejected");

        assert!(
            error
                .message
                .contains("does not support table-backed INSERT ... SELECT sources"),
            "unexpected error: {}",
            error.message
        );
    }

    #[test]
    fn rejects_set_operation_insert_sources_with_precise_error() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let error = canonicalize_write(
            bound_statement(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) SELECT 'entity-1', 'lix_key_value', 'lix', 'version-a', 'lix', '{\"key\":\"hello\"}', '1' \
                 UNION ALL \
                 SELECT 'entity-2', 'lix_key_value', 'lix', 'version-b', 'lix', '{\"key\":\"world\"}', '1'",
            ),
            &registry,
        )
        .expect_err("set-operation insert sources should be rejected");

        assert!(
            error
                .message
                .contains("does not yet support set-operation INSERT sources"),
            "unexpected error: {}",
            error.message
        );
    }

    #[test]
    fn canonicalizes_entity_writes_into_semantic_commands() {
        let mut registry = SurfaceRegistry::with_builtin_surfaces();
        registry.register_dynamic_entity_surfaces(DynamicEntitySurfaceSpec {
            schema_key: "lix_key_value".to_string(),
            visible_columns: vec!["key".to_string(), "value".to_string()],
            column_types: BTreeMap::new(),
            predicate_overrides: Vec::new(),
        });

        let canonicalized = canonicalize_write(
            bound_statement("INSERT INTO lix_key_value (key, value) VALUES ('k', 'v')"),
            &registry,
        )
        .expect("entity writes should canonicalize through the public shell");

        assert_eq!(
            canonicalized.surface_binding.descriptor.public_name,
            "lix_key_value"
        );
        assert!(
            matches!(
                canonicalized.write_command.payload,
                MutationPayload::InsertRows(_)
            ),
            "expected insert rows payload, got: {:?}",
            canonicalized.write_command.payload
        );
    }

    #[test]
    fn canonicalizes_registered_schema_insert_via_lix_json() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let canonicalized = canonicalize_write(
            bound_statement(
                "INSERT INTO lix_registered_schema (value) VALUES (lix_json('{\"x-lix-key\":\"test\",\"x-lix-version\":\"1\"}'))",
            ),
            &registry,
        )
        .expect("registered schema insert should canonicalize");

        let MutationPayload::InsertRows(rows) = &canonicalized.write_command.payload else {
            panic!("expected insert rows payload");
        };
        let [payload] = rows.as_slice() else {
            panic!("expected exactly one insert row");
        };
        assert_eq!(
            payload.get("value"),
            Some(&Value::Json(json!({
                "x-lix-key": "test",
                "x-lix-version": "1"
            })))
        );
    }

    #[test]
    fn canonicalizes_json_typed_entity_update_via_lix_json() {
        let mut registry = SurfaceRegistry::with_builtin_surfaces();
        registry.register_dynamic_entity_surfaces(DynamicEntitySurfaceSpec {
            schema_key: "test_json_entity".to_string(),
            visible_columns: vec!["key".to_string(), "payload".to_string()],
            column_types: BTreeMap::new(),
            predicate_overrides: Vec::new(),
        });
        let canonicalized = canonicalize_write(
            bound_statement(
                "UPDATE test_json_entity SET payload = lix_json('{\"enabled\":true}') WHERE key = 'a'",
            ),
            &registry,
        )
        .expect("json entity update should canonicalize");

        let MutationPayload::UpdatePatch(payload) = &canonicalized.write_command.payload else {
            panic!("expected update payload");
        };
        assert_eq!(
            payload.get("payload"),
            Some(&Value::Json(json!({
                "enabled": true
            })))
        );
    }

    fn assert_surface_write_is_rejected(surface: &str, sql: &str) {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let error = canonicalize_write(bound_statement(sql), &registry)
            .expect_err("derived builtin surface should not be writable");
        assert!(
            error
                .message
                .contains(&format!("surface '{surface}' is not writable")),
            "unexpected error for {surface}: {}",
            error.message
        );
    }

    #[test]
    fn rejects_insert_update_and_delete_for_derived_builtin_entity_surfaces() {
        for surface in [
            "lix_change_author",
            "lix_change_author_by_version",
            "lix_change_set_element",
            "lix_change_set_element_by_version",
            "lix_commit_edge",
            "lix_commit_edge_by_version",
        ] {
            assert_surface_write_is_rejected(
                surface,
                &format!("INSERT INTO {surface} (id) VALUES ('x')"),
            );
            assert_surface_write_is_rejected(
                surface,
                &format!("UPDATE {surface} SET id = 'x' WHERE id = 'y'"),
            );
            assert_surface_write_is_rejected(
                surface,
                &format!("DELETE FROM {surface} WHERE id = 'y'"),
            );
        }
    }

    #[test]
    fn canonicalizes_singleton_in_selector_as_exact_filesystem_delete() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let canonicalized = canonicalize_write(
            bound_statement("DELETE FROM lix_file WHERE id IN ('file-1')"),
            &registry,
        )
        .expect("singleton IN delete should canonicalize");

        assert_eq!(
            canonicalized.write_command.operation_kind,
            WriteOperationKind::Delete
        );
        assert!(canonicalized.write_command.selector.exact_only);
        assert_eq!(
            canonicalized.write_command.selector.exact_filters.get("id"),
            Some(&Value::Text("file-1".to_string()))
        );
    }
}
