use serde_json::Value as JsonValue;
use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, ConflictTarget, Delete, DoUpdate, Expr, Function,
    FunctionArgumentList, FunctionArguments, Ident, ObjectName, ObjectNamePart, OnConflict,
    OnConflictAction, OnInsert, Query, SelectItem, SetExpr, Statement, TableFactor, TableObject,
    TableWithJoins, Update, Value, ValueWithSpan, Values, Visit, Visitor,
};
use std::collections::{BTreeMap, BTreeSet};

use crate::commit::{generate_commit, DomainChangeInput, GenerateCommitArgs};
#[cfg(test)]
use crate::engine::sql::history::commit_runtime::bind_statement_batch_for_dialect as bind_shared_statement_batch_for_dialect;
use crate::engine::sql::history::commit_runtime::{
    build_statement_batch_from_generate_commit_result, load_commit_active_accounts,
    load_version_info_for_versions, CommitQueryExecutor, StatementBatch,
};
use crate::engine::sql::planning::param_context::{
    expr_last_identifier_eq, extract_string_column_values_from_expr, match_bool_column_equality,
};
use crate::engine::sql::planning::rewrite_engine::types::{
    MutationOperation, MutationRow, UpdateValidationPlan, VtableDeletePlan, VtableUpdatePlan,
};
use crate::engine::sql::planning::rewrite_engine::SchemaRegistration;
use crate::engine::sql::planning::rewrite_engine::{
    object_name_matches, resolve_expr_cell_with_state, PlaceholderState, ResolvedCell,
    RowSourceResolver,
};
use crate::functions::LixFunctionProvider;
#[cfg(test)]
use crate::SqlDialect;
use crate::Value as EngineValue;
use crate::{LixBackend, LixError, QueryResult};

const VTABLE_NAME: &str = "lix_internal_state_vtable";
const UNTRACKED_TABLE: &str = "lix_internal_state_untracked";
const SNAPSHOT_TABLE: &str = "lix_internal_snapshot";
const CHANGE_TABLE: &str = "lix_internal_change";
const MATERIALIZED_PREFIX: &str = "lix_internal_state_materialized_v1_";
const UPDATE_RETURNING_COLUMNS: &[&str] = &[
    "entity_id",
    "file_id",
    "version_id",
    "plugin_key",
    "schema_version",
    "snapshot_content",
    "metadata",
    "writer_key",
    "updated_at",
];

struct BackendExecutor<'a> {
    backend: &'a dyn LixBackend,
}

#[async_trait::async_trait(?Send)]
impl CommitQueryExecutor for BackendExecutor<'_> {
    async fn execute(
        &mut self,
        sql: &str,
        params: &[EngineValue],
    ) -> Result<QueryResult, LixError> {
        self.backend.execute(sql, params).await
    }
}

pub struct VtableWriteRewrite {
    pub statements: Vec<Statement>,
    pub params: Vec<EngineValue>,
    pub registrations: Vec<SchemaRegistration>,
    pub mutations: Vec<MutationRow>,
}

#[derive(Debug)]
pub enum UpdateRewrite {
    Statement(VtableUpdateStatement),
    Planned(VtableUpdateRewrite),
}

#[derive(Debug)]
pub struct VtableUpdateStatement {
    pub statement: Statement,
    pub validation: Option<UpdateValidationPlan>,
}

#[derive(Debug)]
pub struct VtableUpdateRewrite {
    pub statement: Statement,
    pub plan: VtableUpdatePlan,
    pub validation: Option<UpdateValidationPlan>,
}

#[derive(Debug)]
pub enum DeleteRewrite {
    Statement(Statement),
    Planned(VtableDeleteRewrite),
}

#[derive(Debug)]
pub struct VtableDeleteRewrite {
    pub statement: Statement,
    pub plan: VtableDeletePlan,
}

pub type DetectedFileDomainChange =
    crate::engine::sql::contracts::effects::DetectedFileDomainChange;

#[cfg(test)]
pub fn rewrite_insert(
    insert: sqlparser::ast::Insert,
    params: &[EngineValue],
    functions: &mut dyn LixFunctionProvider,
) -> Result<Option<VtableWriteRewrite>, LixError> {
    rewrite_insert_with_writer_key(insert, params, None, functions)
}

pub fn rewrite_insert_with_writer_key(
    mut insert: sqlparser::ast::Insert,
    params: &[EngineValue],
    writer_key: Option<&str>,
    functions: &mut dyn LixFunctionProvider,
) -> Result<Option<VtableWriteRewrite>, LixError> {
    if !table_object_is_vtable(&insert.table) {
        return Ok(None);
    }

    validate_and_strip_insert_on_conflict(&mut insert)?;

    if insert.columns.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: "vtable insert requires explicit columns".to_string(),
        });
    }

    let split_rows = split_insert_rows(&insert, params)?;
    let tracked_rows = split_rows.tracked;
    let untracked_rows = split_rows.untracked;

    let mut statements: Vec<Statement> = Vec::new();
    let generated_params: Vec<EngineValue> = Vec::new();
    let mut registrations: Vec<SchemaRegistration> = Vec::new();
    let mut mutations: Vec<MutationRow> = Vec::new();

    if !tracked_rows.is_empty() {
        let tracked = rewrite_tracked_rows(
            &insert,
            tracked_rows,
            &mut registrations,
            &mut mutations,
            writer_key,
            functions,
        )?;
        statements.extend(tracked);
    }

    if !untracked_rows.is_empty() {
        let untracked = build_untracked_insert(&insert, untracked_rows, &mut mutations, functions)?;
        statements.push(untracked);
    }

    Ok(Some(VtableWriteRewrite {
        statements,
        params: generated_params,
        registrations,
        mutations,
    }))
}

pub async fn rewrite_insert_with_backend(
    backend: &dyn LixBackend,
    mut insert: sqlparser::ast::Insert,
    params: &[EngineValue],
    generated_param_offset: usize,
    detected_file_domain_changes: &[DetectedFileDomainChange],
    writer_key: Option<&str>,
    functions: &mut dyn LixFunctionProvider,
) -> Result<Option<VtableWriteRewrite>, LixError> {
    if !table_object_is_vtable(&insert.table) {
        return Ok(None);
    }

    validate_and_strip_insert_on_conflict(&mut insert)?;

    if insert.columns.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: "vtable insert requires explicit columns".to_string(),
        });
    }

    let split_rows = split_insert_rows(&insert, params)?;
    let tracked_rows = split_rows.tracked;
    let untracked_rows = split_rows.untracked;

    let mut statements: Vec<Statement> = Vec::new();
    let mut generated_params: Vec<EngineValue> = Vec::new();
    let mut registrations: Vec<SchemaRegistration> = Vec::new();
    let mut mutations: Vec<MutationRow> = Vec::new();

    if !tracked_rows.is_empty() {
        let tracked = rewrite_tracked_rows_with_backend(
            backend,
            &insert,
            tracked_rows,
            &mut registrations,
            &mut mutations,
            detected_file_domain_changes,
            params.len() + generated_param_offset,
            writer_key,
            functions,
        )
        .await?;
        statements.extend(tracked.statements);
        generated_params.extend(tracked.params);
    }

    if !untracked_rows.is_empty() {
        let untracked = build_untracked_insert(&insert, untracked_rows, &mut mutations, functions)?;
        statements.push(untracked);
    }

    Ok(Some(VtableWriteRewrite {
        statements,
        params: generated_params,
        registrations,
        mutations,
    }))
}

fn validate_and_strip_insert_on_conflict(
    insert: &mut sqlparser::ast::Insert,
) -> Result<(), LixError> {
    let Some(on_insert) = insert.on.take() else {
        return Ok(());
    };

    let OnInsert::OnConflict(on_conflict) = on_insert else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: "vtable insert only supports ON CONFLICT ... DO UPDATE".to_string(),
        });
    };

    match on_conflict.conflict_target {
        Some(ConflictTarget::Columns(columns)) if !columns.is_empty() => {}
        Some(_) => {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: "vtable insert ON CONFLICT only supports explicit column targets"
                    .to_string(),
            })
        }
        None => {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: "vtable insert ON CONFLICT requires explicit conflict columns"
                    .to_string(),
            })
        }
    }

    match on_conflict.action {
        OnConflictAction::DoUpdate(update) => {
            if update.selection.is_some() {
                return Err(LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    title: "Unknown error".to_string(),
                    description: "vtable insert ON CONFLICT DO UPDATE does not support WHERE"
                        .to_string(),
                });
            }
            Ok(())
        }
        OnConflictAction::DoNothing => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: "vtable insert ON CONFLICT DO NOTHING is not supported".to_string(),
        }),
    }
}

pub fn rewrite_update(
    update: Update,
    params: &[EngineValue],
) -> Result<Option<UpdateRewrite>, LixError> {
    if !table_with_joins_is_vtable(&update.table) {
        return Ok(None);
    }

    if update
        .assignments
        .iter()
        .any(|assignment| assignment_target_is_column(&assignment.target, "schema_key"))
    {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: "vtable update cannot change schema_key".to_string(),
        });
    }
    validate_update_assignment_targets(&update.assignments)?;

    let selection = update.selection.as_ref().ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        title: "Unknown error".to_string(),
        description: "vtable update requires a WHERE clause".to_string(),
    })?;

    let has_untracked_true = contains_untracked_true(selection, params);
    let has_untracked_false = contains_untracked_false(selection, params);
    if has_untracked_true && has_untracked_false {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: "vtable update cannot mix untracked predicates".to_string(),
        });
    }

    if has_untracked_true {
        if !can_strip_untracked_predicate(selection, params) {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: "vtable update could not strip untracked predicate".to_string(),
            });
        }
        let mut new_update = update.clone();
        replace_table_with_untracked(&mut new_update.table);
        new_update.assignments = filter_update_assignments(update.assignments);
        ensure_updated_at_assignment(&mut new_update.assignments);
        new_update.selection = try_strip_untracked_predicate(selection, params).unwrap_or(None);
        let validation =
            build_update_validation_plan(&new_update, Some(UNTRACKED_TABLE.to_string()), params)?;
        return Ok(Some(UpdateRewrite::Statement(VtableUpdateStatement {
            statement: Statement::Update(new_update),
            validation,
        })));
    }

    if update.from.is_some() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: "vtable update does not support FROM".to_string(),
        });
    }

    if update.returning.is_some() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: "vtable update does not support custom RETURNING".to_string(),
        });
    }

    let stripped_selection = if has_untracked_false {
        if !can_strip_untracked_false_predicate(selection, params) {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: "vtable update could not strip untracked predicate".to_string(),
            });
        }
        try_strip_untracked_false_predicate(selection, params).unwrap_or(None)
    } else {
        Some(selection.clone())
    };

    let stripped_selection = stripped_selection.ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        title: "Unknown error".to_string(),
        description: "vtable update requires a WHERE clause after stripping untracked".to_string(),
    })?;

    let schema_key = extract_single_schema_key(&stripped_selection, params)?;
    let writer_key_assignment_present = update
        .assignments
        .iter()
        .any(|assignment| assignment_target_is_column(&assignment.target, "writer_key"));
    let explicit_writer_key = extract_explicit_writer_key_assignment(&update.assignments, params)?;

    let mut new_update = update.clone();
    replace_table_with_materialized(&mut new_update.table, &schema_key);
    new_update.assignments = filter_update_assignments(update.assignments);
    ensure_updated_at_assignment(&mut new_update.assignments);
    new_update.selection = Some(stripped_selection);
    new_update.returning = Some(build_update_returning());

    let validation = build_update_validation_plan(
        &new_update,
        Some(format!("{}{}", MATERIALIZED_PREFIX, schema_key)),
        params,
    )?;

    Ok(Some(UpdateRewrite::Planned(VtableUpdateRewrite {
        statement: Statement::Update(new_update),
        plan: VtableUpdatePlan {
            schema_key,
            explicit_writer_key,
            writer_key_assignment_present,
        },
        validation,
    })))
}

pub fn rewrite_delete(
    delete: Delete,
    params: &[EngineValue],
) -> Result<Option<DeleteRewrite>, LixError> {
    rewrite_delete_with_options(delete, false, params)
}

pub fn rewrite_delete_with_options(
    delete: Delete,
    effective_scope_fallback: bool,
    params: &[EngineValue],
) -> Result<Option<DeleteRewrite>, LixError> {
    if !delete_from_is_vtable(&delete) {
        return Ok(None);
    }

    let selection = delete.selection.as_ref().ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        title: "Unknown error".to_string(),
        description: "vtable delete requires a WHERE clause".to_string(),
    })?;

    let has_untracked_true = contains_untracked_true(selection, params);
    let has_untracked_false = contains_untracked_false(selection, params);
    if has_untracked_true && has_untracked_false {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: "vtable delete cannot mix untracked predicates".to_string(),
        });
    }

    if has_untracked_true {
        if !can_strip_untracked_predicate(selection, params) {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: "vtable delete could not strip untracked predicate".to_string(),
            });
        }
        let mut new_delete = delete.clone();
        replace_delete_from_untracked(&mut new_delete);
        new_delete.selection = try_strip_untracked_predicate(selection, params).unwrap_or(None);
        return Ok(Some(DeleteRewrite::Statement(Statement::Delete(
            new_delete,
        ))));
    }

    if delete.using.is_some() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: "vtable delete does not support USING".to_string(),
        });
    }
    if delete.returning.is_some() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: "vtable delete does not support custom RETURNING".to_string(),
        });
    }
    if delete.limit.is_some() || !delete.order_by.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: "vtable delete does not support LIMIT or ORDER BY".to_string(),
        });
    }

    let stripped_selection = if has_untracked_false {
        if !can_strip_untracked_false_predicate(selection, params) {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: "vtable delete could not strip untracked predicate".to_string(),
            });
        }
        try_strip_untracked_false_predicate(selection, params).unwrap_or(None)
    } else {
        Some(selection.clone())
    };

    let stripped_selection = stripped_selection.ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        title: "Unknown error".to_string(),
        description: "vtable delete requires a WHERE clause after stripping untracked".to_string(),
    })?;

    let schema_key = extract_single_schema_key(&stripped_selection, params)?;
    let effective_scope_selection_sql = if effective_scope_fallback {
        Some(stripped_selection.to_string())
    } else {
        None
    };

    let update = Update {
        update_token: AttachedToken::empty(),
        table: table_with_joins_for(&format!("{}{}", MATERIALIZED_PREFIX, schema_key)),
        assignments: vec![
            Assignment {
                target: AssignmentTarget::ColumnName(ObjectName(vec![ObjectNamePart::Identifier(
                    Ident::new("is_tombstone"),
                )])),
                value: number_expr("1"),
            },
            Assignment {
                target: AssignmentTarget::ColumnName(ObjectName(vec![ObjectNamePart::Identifier(
                    Ident::new("updated_at"),
                )])),
                value: lix_timestamp_expr(),
            },
        ],
        from: None,
        selection: Some(stripped_selection),
        returning: Some(build_update_returning()),
        or: None,
        limit: None,
    };

    Ok(Some(DeleteRewrite::Planned(VtableDeleteRewrite {
        statement: Statement::Update(update),
        plan: VtableDeletePlan {
            schema_key,
            effective_scope_fallback,
            effective_scope_selection_sql,
        },
    })))
}

fn build_untracked_on_conflict() -> OnInsert {
    OnInsert::OnConflict(OnConflict {
        conflict_target: Some(ConflictTarget::Columns(vec![
            Ident::new("entity_id"),
            Ident::new("schema_key"),
            Ident::new("file_id"),
            Ident::new("version_id"),
        ])),
        action: OnConflictAction::DoUpdate(DoUpdate {
            assignments: vec![
                Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident::new("snapshot_content")),
                    ])),
                    value: Expr::CompoundIdentifier(vec![
                        Ident::new("excluded"),
                        Ident::new("snapshot_content"),
                    ]),
                },
                Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident::new("plugin_key")),
                    ])),
                    value: Expr::CompoundIdentifier(vec![
                        Ident::new("excluded"),
                        Ident::new("plugin_key"),
                    ]),
                },
                Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident::new("schema_version")),
                    ])),
                    value: Expr::CompoundIdentifier(vec![
                        Ident::new("excluded"),
                        Ident::new("schema_version"),
                    ]),
                },
                Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident::new("metadata")),
                    ])),
                    value: Expr::CompoundIdentifier(vec![
                        Ident::new("excluded"),
                        Ident::new("metadata"),
                    ]),
                },
                Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident::new("updated_at")),
                    ])),
                    value: Expr::CompoundIdentifier(vec![
                        Ident::new("excluded"),
                        Ident::new("updated_at"),
                    ]),
                },
            ],
            selection: None,
        }),
    })
}

fn build_materialized_on_conflict() -> OnInsert {
    OnInsert::OnConflict(OnConflict {
        conflict_target: Some(ConflictTarget::Columns(vec![
            Ident::new("entity_id"),
            Ident::new("file_id"),
            Ident::new("version_id"),
        ])),
        action: OnConflictAction::DoUpdate(DoUpdate {
            assignments: vec![
                Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident::new("snapshot_content")),
                    ])),
                    value: Expr::CompoundIdentifier(vec![
                        Ident::new("excluded"),
                        Ident::new("snapshot_content"),
                    ]),
                },
                Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident::new("schema_version")),
                    ])),
                    value: Expr::CompoundIdentifier(vec![
                        Ident::new("excluded"),
                        Ident::new("schema_version"),
                    ]),
                },
                Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident::new("plugin_key")),
                    ])),
                    value: Expr::CompoundIdentifier(vec![
                        Ident::new("excluded"),
                        Ident::new("plugin_key"),
                    ]),
                },
                Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident::new("change_id")),
                    ])),
                    value: Expr::CompoundIdentifier(vec![
                        Ident::new("excluded"),
                        Ident::new("change_id"),
                    ]),
                },
                Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident::new("metadata")),
                    ])),
                    value: Expr::CompoundIdentifier(vec![
                        Ident::new("excluded"),
                        Ident::new("metadata"),
                    ]),
                },
                Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident::new("writer_key")),
                    ])),
                    value: Expr::CompoundIdentifier(vec![
                        Ident::new("excluded"),
                        Ident::new("writer_key"),
                    ]),
                },
                Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident::new("updated_at")),
                    ])),
                    value: Expr::CompoundIdentifier(vec![
                        Ident::new("excluded"),
                        Ident::new("updated_at"),
                    ]),
                },
            ],
            selection: None,
        }),
    })
}

struct SplitInsertRows {
    tracked: Vec<(Vec<Expr>, Vec<ResolvedCell>)>,
    untracked: Vec<(Vec<Expr>, Vec<ResolvedCell>)>,
}

fn split_insert_rows(
    insert: &sqlparser::ast::Insert,
    params: &[EngineValue],
) -> Result<SplitInsertRows, LixError> {
    let resolver = RowSourceResolver::new(params);
    let row_source = resolver.resolve_insert_required(insert, "vtable insert")?;
    let rows = row_source.rows;
    let resolved_rows = row_source.resolved_rows;

    if rows.is_empty() {
        return Ok(SplitInsertRows {
            tracked: Vec::new(),
            untracked: Vec::new(),
        });
    }

    let untracked_index = find_column_index(&insert.columns, "untracked");
    let mut tracked_rows = Vec::new();
    let mut untracked_rows = Vec::new();

    for (row, resolved_row) in rows.iter().zip(resolved_rows.iter()) {
        let untracked_value = untracked_index.and_then(|idx| resolved_row.get(idx));

        let untracked = match untracked_value {
            None => false,
            Some(cell) => parse_untracked_bool_like_value(cell).ok_or_else(|| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: "vtable insert requires literal or parameter untracked values"
                    .to_string(),
            })?,
        };

        if untracked {
            untracked_rows.push((row.clone(), resolved_row.clone()));
        } else {
            tracked_rows.push((row.clone(), resolved_row.clone()));
        }
    }

    Ok(SplitInsertRows {
        tracked: tracked_rows,
        untracked: untracked_rows,
    })
}

fn rewrite_tracked_rows(
    insert: &sqlparser::ast::Insert,
    rows: Vec<(Vec<Expr>, Vec<ResolvedCell>)>,
    registrations: &mut Vec<SchemaRegistration>,
    mutations: &mut Vec<MutationRow>,
    writer_key: Option<&str>,
    functions: &mut dyn LixFunctionProvider,
) -> Result<Vec<Statement>, LixError> {
    let entity_idx = required_column_index(&insert.columns, "entity_id")?;
    let schema_idx = required_column_index(&insert.columns, "schema_key")?;
    let file_idx = required_column_index(&insert.columns, "file_id")?;
    let version_idx = required_column_index(&insert.columns, "version_id")?;
    let plugin_idx = required_column_index(&insert.columns, "plugin_key")?;
    let schema_version_idx = required_column_index(&insert.columns, "schema_version")?;
    let snapshot_idx = required_column_index(&insert.columns, "snapshot_content")?;
    let metadata_idx = find_column_index(&insert.columns, "metadata");
    let writer_key_idx = find_column_index(&insert.columns, "writer_key");

    let mut ensure_no_content = false;
    let mut snapshot_rows = Vec::new();
    let mut change_rows = Vec::new();
    let mut materialized_by_schema: std::collections::BTreeMap<String, Vec<Vec<Expr>>> =
        std::collections::BTreeMap::new();

    for (row, materialized) in rows {
        let schema_key_expr = row.get(schema_idx).ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: "vtable insert missing schema_key".to_string(),
        })?;
        let schema_key = resolved_string_required(
            materialized.get(schema_idx),
            Some(schema_key_expr),
            "schema_key",
        )?;

        if !registrations.iter().any(|reg| reg.schema_key == schema_key) {
            registrations.push(SchemaRegistration {
                schema_key: schema_key.clone(),
            });
        }

        let snapshot_content =
            resolved_expr_or_original(materialized.get(snapshot_idx), row.get(snapshot_idx))?;
        let snapshot_id = if is_null_literal(&snapshot_content) {
            ensure_no_content = true;
            "no-content".to_string()
        } else {
            let id = functions.uuid_v7();
            snapshot_rows.push(vec![string_expr(&id), snapshot_content.clone()]);
            id
        };

        let change_id = functions.uuid_v7();
        let created_at = functions.timestamp();
        let updated_at = created_at.clone();

        let metadata_expr = match metadata_idx {
            Some(index) => resolved_expr_or_original(materialized.get(index), row.get(index))?,
            None => null_expr(),
        };
        let explicit_writer_key = match writer_key_idx {
            Some(index) => {
                resolved_optional_string(materialized.get(index), row.get(index), "writer_key")?
            }
            None => None,
        };
        let resolved_writer_key =
            explicit_writer_key.or_else(|| writer_key.map(ToString::to_string));
        let writer_key_expr = resolved_writer_key
            .as_ref()
            .map(|value| string_expr(value))
            .unwrap_or_else(null_expr);

        change_rows.push(vec![
            string_expr(&change_id),
            resolved_expr_or_original(materialized.get(entity_idx), row.get(entity_idx))?,
            resolved_expr_or_original(materialized.get(schema_idx), row.get(schema_idx))?,
            resolved_expr_or_original(
                materialized.get(schema_version_idx),
                row.get(schema_version_idx),
            )?,
            resolved_expr_or_original(materialized.get(file_idx), row.get(file_idx))?,
            resolved_expr_or_original(materialized.get(plugin_idx), row.get(plugin_idx))?,
            string_expr(&snapshot_id),
            metadata_expr.clone(),
            string_expr(&created_at),
        ]);

        let resolved_row = vec![
            resolved_expr_or_original(materialized.get(entity_idx), row.get(entity_idx))?,
            resolved_expr_or_original(materialized.get(schema_idx), row.get(schema_idx))?,
            resolved_expr_or_original(
                materialized.get(schema_version_idx),
                row.get(schema_version_idx),
            )?,
            resolved_expr_or_original(materialized.get(file_idx), row.get(file_idx))?,
            resolved_expr_or_original(materialized.get(version_idx), row.get(version_idx))?,
            resolved_expr_or_original(materialized.get(plugin_idx), row.get(plugin_idx))?,
            snapshot_content,
            string_expr(&change_id),
            metadata_expr,
            writer_key_expr,
            number_expr("0"),
            string_expr(&created_at),
            string_expr(&updated_at),
        ];

        materialized_by_schema
            .entry(schema_key.clone())
            .or_default()
            .push(resolved_row);

        mutations.push(MutationRow {
            operation: MutationOperation::Insert,
            entity_id: resolved_string_required(
                materialized.get(entity_idx),
                row.get(entity_idx),
                "entity_id",
            )?,
            schema_key,
            schema_version: resolved_string_required(
                materialized.get(schema_version_idx),
                row.get(schema_version_idx),
                "schema_version",
            )?,
            file_id: resolved_string_required(
                materialized.get(file_idx),
                row.get(file_idx),
                "file_id",
            )?,
            version_id: resolved_string_required(
                materialized.get(version_idx),
                row.get(version_idx),
                "version_id",
            )?,
            plugin_key: resolved_string_required(
                materialized.get(plugin_idx),
                row.get(plugin_idx),
                "plugin_key",
            )?,
            snapshot_content: resolved_snapshot_json(
                materialized.get(snapshot_idx),
                row.get(snapshot_idx),
            )?,
            untracked: false,
        });
    }

    let mut statements = Vec::new();

    if ensure_no_content {
        statements.push(make_insert_statement(
            SNAPSHOT_TABLE,
            vec![Ident::new("id"), Ident::new("content")],
            vec![vec![string_expr("no-content"), null_expr()]],
            Some(build_snapshot_on_conflict()),
        ));
    }

    if !snapshot_rows.is_empty() {
        statements.push(make_insert_statement(
            SNAPSHOT_TABLE,
            vec![Ident::new("id"), Ident::new("content")],
            snapshot_rows,
            Some(build_snapshot_on_conflict()),
        ));
    }

    if !change_rows.is_empty() {
        statements.push(make_insert_statement(
            CHANGE_TABLE,
            vec![
                Ident::new("id"),
                Ident::new("entity_id"),
                Ident::new("schema_key"),
                Ident::new("schema_version"),
                Ident::new("file_id"),
                Ident::new("plugin_key"),
                Ident::new("snapshot_id"),
                Ident::new("metadata"),
                Ident::new("created_at"),
            ],
            change_rows,
            None,
        ));
    }

    for (schema_key, rows) in materialized_by_schema {
        let table_name = format!("{}{}", MATERIALIZED_PREFIX, schema_key);
        statements.push(make_insert_statement(
            &table_name,
            vec![
                Ident::new("entity_id"),
                Ident::new("schema_key"),
                Ident::new("schema_version"),
                Ident::new("file_id"),
                Ident::new("version_id"),
                Ident::new("plugin_key"),
                Ident::new("snapshot_content"),
                Ident::new("change_id"),
                Ident::new("metadata"),
                Ident::new("writer_key"),
                Ident::new("is_tombstone"),
                Ident::new("created_at"),
                Ident::new("updated_at"),
            ],
            rows,
            Some(build_materialized_on_conflict()),
        ));
    }

    Ok(statements)
}

async fn rewrite_tracked_rows_with_backend(
    backend: &dyn LixBackend,
    insert: &sqlparser::ast::Insert,
    rows: Vec<(Vec<Expr>, Vec<ResolvedCell>)>,
    registrations: &mut Vec<SchemaRegistration>,
    mutations: &mut Vec<MutationRow>,
    detected_file_domain_changes: &[DetectedFileDomainChange],
    placeholder_offset: usize,
    writer_key: Option<&str>,
    functions: &mut dyn LixFunctionProvider,
) -> Result<StatementBatch, LixError> {
    let entity_idx = required_column_index(&insert.columns, "entity_id")?;
    let schema_idx = required_column_index(&insert.columns, "schema_key")?;
    let file_idx = required_column_index(&insert.columns, "file_id")?;
    let version_idx = required_column_index(&insert.columns, "version_id")?;
    let plugin_idx = required_column_index(&insert.columns, "plugin_key")?;
    let schema_version_idx = required_column_index(&insert.columns, "schema_version")?;
    let snapshot_idx = required_column_index(&insert.columns, "snapshot_content")?;
    let metadata_idx = find_column_index(&insert.columns, "metadata");
    let writer_key_idx = find_column_index(&insert.columns, "writer_key");

    let timestamp = functions.timestamp();
    let mut domain_changes = Vec::new();
    let mut affected_versions = BTreeSet::new();

    for (row, materialized) in rows {
        let entity_id = resolved_string_required(
            materialized.get(entity_idx),
            row.get(entity_idx),
            "entity_id",
        )?;
        let schema_key = resolved_string_required(
            materialized.get(schema_idx),
            row.get(schema_idx),
            "schema_key",
        )?;
        let file_id =
            resolved_string_required(materialized.get(file_idx), row.get(file_idx), "file_id")?;
        let version_id = resolved_string_required(
            materialized.get(version_idx),
            row.get(version_idx),
            "version_id",
        )?;
        let plugin_key = resolved_string_required(
            materialized.get(plugin_idx),
            row.get(plugin_idx),
            "plugin_key",
        )?;
        let schema_version = resolved_string_required(
            materialized.get(schema_version_idx),
            row.get(schema_version_idx),
            "schema_version",
        )?;

        let snapshot_json =
            resolved_snapshot_json(materialized.get(snapshot_idx), row.get(snapshot_idx))?;
        let metadata_json = match metadata_idx {
            Some(index) => {
                resolved_optional_json(materialized.get(index), row.get(index), "metadata")?
            }
            None => None,
        };
        let explicit_writer_key = match writer_key_idx {
            Some(index) => {
                resolved_optional_string(materialized.get(index), row.get(index), "writer_key")?
            }
            None => None,
        };
        let domain_writer_key = explicit_writer_key.or_else(|| writer_key.map(ToString::to_string));

        ensure_registration(registrations, &schema_key);

        let change_id = functions.uuid_v7();
        affected_versions.insert(version_id.clone());
        domain_changes.push(DomainChangeInput {
            id: change_id.clone(),
            entity_id: entity_id.clone(),
            schema_key: schema_key.clone(),
            schema_version: schema_version.clone(),
            file_id: file_id.clone(),
            plugin_key: plugin_key.clone(),
            snapshot_content: snapshot_json.as_ref().map(JsonValue::to_string),
            metadata: metadata_json.as_ref().map(JsonValue::to_string),
            created_at: timestamp.clone(),
            version_id: version_id.clone(),
            writer_key: domain_writer_key,
        });

        mutations.push(MutationRow {
            operation: MutationOperation::Insert,
            entity_id,
            schema_key,
            schema_version,
            file_id,
            version_id,
            plugin_key,
            snapshot_content: snapshot_json,
            untracked: false,
        });
    }

    for change in detected_file_domain_changes {
        affected_versions.insert(change.version_id.clone());
        ensure_registration(registrations, &change.schema_key);
        let domain_writer_key = change
            .writer_key
            .clone()
            .or_else(|| writer_key.map(ToString::to_string));
        domain_changes.push(DomainChangeInput {
            id: functions.uuid_v7(),
            entity_id: change.entity_id.clone(),
            schema_key: change.schema_key.clone(),
            schema_version: change.schema_version.clone(),
            file_id: change.file_id.clone(),
            version_id: change.version_id.clone(),
            plugin_key: change.plugin_key.clone(),
            snapshot_content: change.snapshot_content.clone(),
            metadata: change.metadata.clone(),
            created_at: timestamp.clone(),
            writer_key: domain_writer_key,
        });
    }

    if domain_changes.is_empty() {
        return Ok(StatementBatch {
            statements: Vec::new(),
            params: Vec::new(),
        });
    }

    let mut executor = BackendExecutor { backend };
    let versions = load_version_info_for_versions(&mut executor, &affected_versions).await?;
    let active_accounts = load_commit_active_accounts(&mut executor, &domain_changes).await?;
    let commit_result = generate_commit(
        GenerateCommitArgs {
            timestamp: timestamp.clone(),
            active_accounts,
            changes: domain_changes,
            versions,
        },
        || functions.uuid_v7(),
    )?;

    for row in &commit_result.materialized_state {
        ensure_registration(registrations, &row.schema_key);
    }

    build_statement_batch_from_generate_commit_result(
        commit_result,
        functions,
        placeholder_offset,
        backend.dialect(),
    )
}

fn ensure_registration(registrations: &mut Vec<SchemaRegistration>, schema_key: &str) {
    if registrations
        .iter()
        .any(|registration| registration.schema_key == schema_key)
    {
        return;
    }
    registrations.push(SchemaRegistration {
        schema_key: schema_key.to_string(),
    });
}

fn build_snapshot_on_conflict() -> OnInsert {
    OnInsert::OnConflict(OnConflict {
        conflict_target: Some(ConflictTarget::Columns(vec![Ident::new("id")])),
        action: OnConflictAction::DoNothing,
    })
}

fn build_untracked_insert(
    insert: &sqlparser::ast::Insert,
    rows: Vec<(Vec<Expr>, Vec<ResolvedCell>)>,
    mutations: &mut Vec<MutationRow>,
    functions: &mut dyn LixFunctionProvider,
) -> Result<Statement, LixError> {
    let entity_idx = required_column_index(&insert.columns, "entity_id")?;
    let schema_idx = required_column_index(&insert.columns, "schema_key")?;
    let file_idx = required_column_index(&insert.columns, "file_id")?;
    let version_idx = required_column_index(&insert.columns, "version_id")?;
    let plugin_idx = required_column_index(&insert.columns, "plugin_key")?;
    let snapshot_idx = required_column_index(&insert.columns, "snapshot_content")?;
    let schema_version_idx = required_column_index(&insert.columns, "schema_version")?;
    let metadata_idx = find_column_index(&insert.columns, "metadata");

    let mut mapped_rows = Vec::new();
    for (row, materialized) in rows {
        let now = functions.timestamp();
        mapped_rows.push(vec![
            resolved_expr_or_original(materialized.get(entity_idx), row.get(entity_idx))?,
            resolved_expr_or_original(materialized.get(schema_idx), row.get(schema_idx))?,
            resolved_expr_or_original(materialized.get(file_idx), row.get(file_idx))?,
            resolved_expr_or_original(materialized.get(version_idx), row.get(version_idx))?,
            resolved_expr_or_original(materialized.get(plugin_idx), row.get(plugin_idx))?,
            resolved_expr_or_original(materialized.get(snapshot_idx), row.get(snapshot_idx))?,
            match metadata_idx {
                Some(index) => resolved_expr_or_original(materialized.get(index), row.get(index))?,
                None => null_expr(),
            },
            resolved_expr_or_original(
                materialized.get(schema_version_idx),
                row.get(schema_version_idx),
            )?,
            string_expr(&now),
            string_expr(&now),
        ]);

        mutations.push(MutationRow {
            operation: MutationOperation::Insert,
            entity_id: resolved_string_required(
                materialized.get(entity_idx),
                row.get(entity_idx),
                "entity_id",
            )?,
            schema_key: resolved_string_required(
                materialized.get(schema_idx),
                row.get(schema_idx),
                "schema_key",
            )?,
            schema_version: resolved_string_required(
                materialized.get(schema_version_idx),
                row.get(schema_version_idx),
                "schema_version",
            )?,
            file_id: resolved_string_required(
                materialized.get(file_idx),
                row.get(file_idx),
                "file_id",
            )?,
            version_id: resolved_string_required(
                materialized.get(version_idx),
                row.get(version_idx),
                "version_id",
            )?,
            plugin_key: resolved_string_required(
                materialized.get(plugin_idx),
                row.get(plugin_idx),
                "plugin_key",
            )?,
            snapshot_content: resolved_snapshot_json(
                materialized.get(snapshot_idx),
                row.get(snapshot_idx),
            )?,
            untracked: true,
        });
    }

    Ok(make_insert_statement(
        UNTRACKED_TABLE,
        vec![
            Ident::new("entity_id"),
            Ident::new("schema_key"),
            Ident::new("file_id"),
            Ident::new("version_id"),
            Ident::new("plugin_key"),
            Ident::new("snapshot_content"),
            Ident::new("metadata"),
            Ident::new("schema_version"),
            Ident::new("created_at"),
            Ident::new("updated_at"),
        ],
        mapped_rows,
        Some(build_untracked_on_conflict()),
    ))
}

#[cfg(test)]
fn bind_statement_batch_for_dialect(
    batch: StatementBatch,
    dialect: SqlDialect,
) -> Result<Vec<crate::engine::sql::contracts::prepared_statement::PreparedStatement>, LixError> {
    bind_shared_statement_batch_for_dialect(batch, dialect)
}

fn filter_update_assignments(assignments: Vec<Assignment>) -> Vec<Assignment> {
    assignments
        .into_iter()
        .filter(|assignment| !assignment_target_is_untracked(&assignment.target))
        .filter(|assignment| !assignment_target_is_column(&assignment.target, "updated_at"))
        .filter(|assignment| !assignment_target_is_column(&assignment.target, "change_id"))
        .collect()
}

fn validate_update_assignment_targets(assignments: &[Assignment]) -> Result<(), LixError> {
    const ALLOWED_MUTABLE_COLUMNS: &[&str] = &[
        "entity_id",
        "file_id",
        "version_id",
        "plugin_key",
        "schema_version",
        "snapshot_content",
        "metadata",
        "writer_key",
    ];
    for assignment in assignments {
        let Some(column) = assignment_target_column_name(&assignment.target) else {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description:
                    "strict rewrite violation: vtable update assignment must target a named column"
                        .to_string(),
            });
        };
        if ALLOWED_MUTABLE_COLUMNS
            .iter()
            .any(|candidate| column.eq_ignore_ascii_case(candidate))
        {
            continue;
        }
        if column.eq_ignore_ascii_case("updated_at") || column.eq_ignore_ascii_case("change_id") {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: format!(
                    "vtable update cannot set {}; it is managed by rewrite",
                    column
                ),
            });
        }
        if column.eq_ignore_ascii_case("untracked") {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description:
                    "vtable update cannot set untracked; use an untracked predicate instead"
                        .to_string(),
            });
        }
        if column.eq_ignore_ascii_case("inherited_from_version_id") {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: "vtable update cannot set inherited_from_version_id; it is computed"
                    .to_string(),
            });
        }
        return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: format!(
                "strict rewrite violation: vtable update assignment references unknown column '{}'; allowed columns: {}",
                column,
                ALLOWED_MUTABLE_COLUMNS.join(", ")
            ),
        });
    }
    Ok(())
}

fn ensure_updated_at_assignment(assignments: &mut Vec<Assignment>) {
    assignments.push(Assignment {
        target: AssignmentTarget::ColumnName(ObjectName(vec![ObjectNamePart::Identifier(
            Ident::new("updated_at"),
        )])),
        value: lix_timestamp_expr(),
    });
}

fn build_update_returning() -> Vec<SelectItem> {
    UPDATE_RETURNING_COLUMNS
        .iter()
        .map(|column| SelectItem::UnnamedExpr(Expr::Identifier(Ident::new(*column))))
        .collect()
}

fn extract_explicit_writer_key_assignment(
    assignments: &[Assignment],
    params: &[EngineValue],
) -> Result<Option<Option<String>>, LixError> {
    let mut state = PlaceholderState::new();
    for assignment in assignments {
        let resolved =
            resolve_assignment_with_placeholder_state(&assignment.value, params, &mut state)?;
        if !assignment_target_is_column(&assignment.target, "writer_key") {
            continue;
        }

        return match resolved.value {
            Some(EngineValue::Text(value)) => Ok(Some(Some(value))),
            Some(EngineValue::Null) => Ok(Some(None)),
            None => Ok(None),
            Some(other) => Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: format!("writer_key assignment expects text or null, got {other:?}"),
            }),
        };
    }

    Ok(None)
}

fn resolve_assignment_with_placeholder_state(
    expr: &Expr,
    params: &[EngineValue],
    state: &mut PlaceholderState,
) -> Result<ResolvedCell, LixError> {
    let resolved = resolve_expr_cell_with_state(expr, params, state)?;
    if resolved.value.is_none() {
        advance_placeholder_state_for_expr(expr, params, state)?;
    }
    Ok(resolved)
}

fn advance_placeholder_state_for_expr(
    expr: &Expr,
    params: &[EngineValue],
    state: &mut PlaceholderState,
) -> Result<(), LixError> {
    struct PlaceholderStateAdvancer<'a> {
        params_len: usize,
        state: &'a mut PlaceholderState,
        error: Option<LixError>,
    }

    impl Visitor for PlaceholderStateAdvancer<'_> {
        type Break = ();

        fn pre_visit_value(&mut self, value: &Value) -> std::ops::ControlFlow<Self::Break> {
            let Value::Placeholder(token) = value else {
                return std::ops::ControlFlow::Continue(());
            };

            if let Err(error) =
                crate::engine::sql::planning::rewrite_engine::params::resolve_placeholder_index(
                    token,
                    self.params_len,
                    self.state,
                )
            {
                self.error = Some(error);
                return std::ops::ControlFlow::Break(());
            }

            std::ops::ControlFlow::Continue(())
        }
    }

    let mut advancer = PlaceholderStateAdvancer {
        params_len: params.len(),
        state,
        error: None,
    };
    let _ = expr.visit(&mut advancer);

    if let Some(error) = advancer.error {
        return Err(error);
    }

    Ok(())
}

fn lix_timestamp_expr() -> Expr {
    Expr::Function(Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
            "lix_timestamp",
        ))]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: Vec::new(),
            clauses: Vec::new(),
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: Vec::new(),
    })
}

#[cfg(test)]
fn build_statements_from_generate_commit_result(
    commit_result: crate::commit::GenerateCommitResult,
    functions: &mut dyn LixFunctionProvider,
    placeholder_offset: usize,
    dialect: SqlDialect,
) -> Result<StatementBatch, LixError> {
    build_statement_batch_from_generate_commit_result(
        commit_result,
        functions,
        placeholder_offset,
        dialect,
    )
}

fn table_with_joins_for(table: &str) -> TableWithJoins {
    TableWithJoins {
        relation: TableFactor::Table {
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(table))]),
            alias: None,
            args: None,
            with_hints: Vec::new(),
            version: None,
            with_ordinality: false,
            partitions: Vec::new(),
            json_path: None,
            sample: None,
            index_hints: Vec::new(),
        },
        joins: Vec::new(),
    }
}

fn value_to_expr(value: &EngineValue) -> Result<Expr, LixError> {
    match value {
        EngineValue::Null => Ok(null_expr()),
        EngineValue::Boolean(value) => Ok(Expr::Value(Value::Boolean(*value).into())),
        EngineValue::Text(text) => Ok(string_expr(text)),
        EngineValue::Integer(value) => {
            Ok(Expr::Value(Value::Number(value.to_string(), false).into()))
        }
        EngineValue::Real(value) => Ok(Expr::Value(Value::Number(value.to_string(), false).into())),
        EngineValue::Blob(_) => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: "vtable update does not support blob snapshot_content".to_string(),
        }),
    }
}

fn make_insert_statement(
    table: &str,
    columns: Vec<Ident>,
    rows: Vec<Vec<Expr>>,
    on: Option<OnInsert>,
) -> Statement {
    let values = Values {
        explicit_row: false,
        value_keyword: false,
        rows,
    };
    let query = Query {
        with: None,
        body: Box::new(SetExpr::Values(values)),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: Vec::new(),
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: Vec::new(),
    };

    Statement::Insert(sqlparser::ast::Insert {
        insert_token: AttachedToken::empty(),
        or: None,
        ignore: false,
        into: true,
        table: TableObject::TableName(ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
            table,
        ))])),
        table_alias: None,
        columns,
        overwrite: false,
        source: Some(Box::new(query)),
        assignments: Vec::new(),
        partitioned: None,
        after_columns: Vec::new(),
        has_table_keyword: false,
        on,
        returning: None,
        replace_into: false,
        priority: None,
        insert_alias: None,
        settings: None,
        format_clause: None,
    })
}

fn required_column_index(columns: &[Ident], name: &str) -> Result<usize, LixError> {
    find_column_index(columns, name).ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        title: "Unknown error".to_string(),
        description: format!("vtable insert requires {name}"),
    })
}

fn resolved_string_required(
    cell: Option<&ResolvedCell>,
    expr: Option<&Expr>,
    name: &str,
) -> Result<String, LixError> {
    if let Some(ResolvedCell {
        value: Some(EngineValue::Text(value)),
        ..
    }) = cell
    {
        return Ok(value.clone());
    }

    literal_string_required(expr, name)
}

fn literal_string_required(expr: Option<&Expr>, name: &str) -> Result<String, LixError> {
    let expr = expr.ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        title: "Unknown error".to_string(),
        description: format!("vtable insert missing {name}"),
    })?;
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(value),
            ..
        }) => Ok(value.clone()),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: format!("vtable insert requires literal {name}"),
        }),
    }
}

fn literal_snapshot_json(expr: Option<&Expr>) -> Result<Option<JsonValue>, LixError> {
    let expr = expr.ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        title: "Unknown error".to_string(),
        description: "vtable insert missing snapshot_content".to_string(),
    })?;
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::Null, ..
        }) => Ok(None),
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(value),
            ..
        }) => serde_json::from_str::<JsonValue>(value)
            .map(Some)
            .map_err(|err| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: format!("vtable insert snapshot_content invalid JSON: {err}"),
            }),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: "vtable insert requires literal snapshot_content".to_string(),
        }),
    }
}

fn resolved_snapshot_json(
    cell: Option<&ResolvedCell>,
    expr: Option<&Expr>,
) -> Result<Option<JsonValue>, LixError> {
    if let Some(cell) = cell {
        match &cell.value {
            Some(EngineValue::Null) => return Ok(None),
            Some(EngineValue::Text(value)) => {
                return serde_json::from_str::<JsonValue>(value)
                    .map(Some)
                    .map_err(|err| LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        title: "Unknown error".to_string(),
                        description: format!("vtable insert snapshot_content invalid JSON: {err}"),
                    })
            }
            _ => {}
        }
    }

    literal_snapshot_json(expr)
}

fn literal_optional_json(
    expr: Option<&Expr>,
    field_name: &str,
) -> Result<Option<JsonValue>, LixError> {
    let Some(expr) = expr else {
        return Ok(None);
    };

    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::Null, ..
        }) => Ok(None),
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(value),
            ..
        }) => serde_json::from_str::<JsonValue>(value)
            .map(Some)
            .map_err(|err| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: format!("vtable insert {field_name} invalid JSON: {err}"),
            }),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: format!("vtable insert requires literal {field_name}"),
        }),
    }
}

fn resolved_optional_json(
    cell: Option<&ResolvedCell>,
    expr: Option<&Expr>,
    field_name: &str,
) -> Result<Option<JsonValue>, LixError> {
    if let Some(cell) = cell {
        match &cell.value {
            Some(EngineValue::Null) => return Ok(None),
            Some(EngineValue::Text(value)) => {
                return serde_json::from_str::<JsonValue>(value)
                    .map(Some)
                    .map_err(|err| LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        title: "Unknown error".to_string(),
                        description: format!("vtable insert {field_name} invalid JSON: {err}"),
                    })
            }
            _ => {}
        }
    }

    literal_optional_json(expr, field_name)
}

fn resolved_optional_string(
    cell: Option<&ResolvedCell>,
    expr: Option<&Expr>,
    field_name: &str,
) -> Result<Option<String>, LixError> {
    if let Some(cell) = cell {
        match &cell.value {
            Some(EngineValue::Null) => return Ok(None),
            Some(EngineValue::Text(value)) => return Ok(Some(value.clone())),
            _ => {}
        }
    }

    let Some(expr) = expr else {
        return Ok(None);
    };

    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::Null, ..
        }) => Ok(None),
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(value),
            ..
        }) => Ok(Some(value.clone())),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: format!("vtable insert requires literal {field_name}"),
        }),
    }
}

fn resolved_expr_or_original(
    cell: Option<&ResolvedCell>,
    expr: Option<&Expr>,
) -> Result<Expr, LixError> {
    if let Some(ResolvedCell {
        value: Some(value), ..
    }) = cell
    {
        return value_to_expr(value);
    }

    Ok(expr.cloned().unwrap_or_else(null_expr))
}

fn string_expr(value: &str) -> Expr {
    Expr::Value(Value::SingleQuotedString(value.to_string()).into())
}

fn number_expr(value: &str) -> Expr {
    Expr::Value(Value::Number(value.to_string(), false).into())
}

fn null_expr() -> Expr {
    Expr::Value(Value::Null.into())
}

fn is_null_literal(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Value(ValueWithSpan {
            value: Value::Null,
            ..
        })
    )
}

fn table_object_is_vtable(table: &TableObject) -> bool {
    match table {
        TableObject::TableName(name) => object_name_matches(name, VTABLE_NAME),
        _ => false,
    }
}

fn table_with_joins_is_vtable(table: &TableWithJoins) -> bool {
    matches!(
        &table.relation,
        TableFactor::Table { name, .. } if object_name_matches(name, VTABLE_NAME)
    )
}

fn delete_from_is_vtable(delete: &Delete) -> bool {
    match &delete.from {
        sqlparser::ast::FromTable::WithFromKeyword(tables)
        | sqlparser::ast::FromTable::WithoutKeyword(tables) => {
            if tables.len() != 1 {
                return false;
            }
            table_with_joins_is_vtable(&tables[0])
        }
    }
}

fn replace_table_with_untracked(table: &mut TableWithJoins) {
    if let TableFactor::Table { name, .. } = &mut table.relation {
        *name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
            UNTRACKED_TABLE,
        ))]);
    }
}

fn replace_table_with_materialized(table: &mut TableWithJoins, schema_key: &str) {
    if let TableFactor::Table { name, .. } = &mut table.relation {
        let table_name = format!("{}{}", MATERIALIZED_PREFIX, schema_key);
        *name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(table_name))]);
    }
}

fn replace_delete_from_untracked(delete: &mut Delete) {
    let tables = match &mut delete.from {
        sqlparser::ast::FromTable::WithFromKeyword(tables)
        | sqlparser::ast::FromTable::WithoutKeyword(tables) => tables,
    };

    if let Some(table) = tables.first_mut() {
        replace_table_with_untracked(table);
    }
}

fn find_column_index(columns: &[Ident], column: &str) -> Option<usize> {
    columns
        .iter()
        .position(|ident| ident.value.eq_ignore_ascii_case(column))
}

fn assignment_target_is_untracked(target: &AssignmentTarget) -> bool {
    assignment_target_is_column(target, "untracked")
}

fn assignment_target_column_name(target: &AssignmentTarget) -> Option<String> {
    match target {
        AssignmentTarget::ColumnName(object_name) => object_name
            .0
            .last()
            .and_then(ObjectNamePart::as_ident)
            .map(|ident| ident.value.clone()),
        AssignmentTarget::Tuple(_) => None,
    }
}

fn assignment_target_is_column(target: &AssignmentTarget, column: &str) -> bool {
    match target {
        AssignmentTarget::ColumnName(name) => object_name_matches(name, column),
        AssignmentTarget::Tuple(columns) => {
            columns.iter().any(|name| object_name_matches(name, column))
        }
    }
}

fn contains_untracked_true(expr: &Expr, params: &[EngineValue]) -> bool {
    if is_untracked_equals_true(expr, params) {
        return true;
    }
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And | BinaryOperator::Or => {
                contains_untracked_true(left, params) || contains_untracked_true(right, params)
            }
            _ => false,
        },
        Expr::Nested(inner) => contains_untracked_true(inner, params),
        _ => false,
    }
}

fn contains_untracked_false(expr: &Expr, params: &[EngineValue]) -> bool {
    if is_untracked_equals_false(expr, params) {
        return true;
    }
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And | BinaryOperator::Or => {
                contains_untracked_false(left, params) || contains_untracked_false(right, params)
            }
            _ => false,
        },
        Expr::Nested(inner) => contains_untracked_false(inner, params),
        _ => false,
    }
}

fn can_strip_untracked_predicate(expr: &Expr, params: &[EngineValue]) -> bool {
    contains_untracked_true(expr, params) && try_strip_untracked_predicate(expr, params).is_some()
}

fn can_strip_untracked_false_predicate(expr: &Expr, params: &[EngineValue]) -> bool {
    contains_untracked_false(expr, params)
        && try_strip_untracked_false_predicate(expr, params).is_some()
}

fn try_strip_untracked_predicate(expr: &Expr, params: &[EngineValue]) -> Option<Option<Expr>> {
    if is_untracked_equals_true(expr, params) {
        return Some(None);
    }

    match expr {
        Expr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
            let left = try_strip_untracked_predicate(left, params)?;
            let right = try_strip_untracked_predicate(right, params)?;

            match (left, right) {
                (None, None) => Some(None),
                (Some(expr), None) | (None, Some(expr)) => Some(Some(expr)),
                (Some(left), Some(right)) => Some(Some(Expr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::And,
                    right: Box::new(right),
                })),
            }
        }
        Expr::Nested(inner) => {
            let stripped = try_strip_untracked_predicate(inner, params)?;
            Some(stripped.map(|expr| Expr::Nested(Box::new(expr))))
        }
        _ => Some(Some(expr.clone())),
    }
}

fn try_strip_untracked_false_predicate(
    expr: &Expr,
    params: &[EngineValue],
) -> Option<Option<Expr>> {
    if is_untracked_equals_false(expr, params) {
        return Some(None);
    }

    match expr {
        Expr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
            let left = try_strip_untracked_false_predicate(left, params)?;
            let right = try_strip_untracked_false_predicate(right, params)?;

            match (left, right) {
                (None, None) => Some(None),
                (Some(expr), None) | (None, Some(expr)) => Some(Some(expr)),
                (Some(left), Some(right)) => Some(Some(Expr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::And,
                    right: Box::new(right),
                })),
            }
        }
        Expr::Nested(inner) => {
            let stripped = try_strip_untracked_false_predicate(inner, params)?;
            Some(stripped.map(|expr| Expr::Nested(Box::new(expr))))
        }
        _ => Some(Some(expr.clone())),
    }
}

fn is_untracked_equals_true(expr: &Expr, params: &[EngineValue]) -> bool {
    match_bool_column_equality(expr, expr_is_untracked_column, params) == Some(true)
}

fn is_untracked_equals_false(expr: &Expr, params: &[EngineValue]) -> bool {
    match_bool_column_equality(expr, expr_is_untracked_column, params) == Some(false)
}

fn expr_is_untracked_column(expr: &Expr) -> bool {
    expr_last_identifier_eq(expr, "untracked")
}

fn parse_untracked_bool_like_value(cell: &ResolvedCell) -> Option<bool> {
    match cell.value.as_ref() {
        Some(EngineValue::Boolean(value)) => Some(*value),
        _ => None,
    }
}

fn build_update_validation_plan(
    update: &Update,
    table_name: Option<String>,
    params: &[EngineValue],
) -> Result<Option<UpdateValidationPlan>, LixError> {
    let (snapshot_content, snapshot_patch) =
        snapshot_content_from_assignments(&update.assignments, params)?;
    let where_clause = update.selection.clone();
    let table = table_name.ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        title: "Unknown error".to_string(),
        description: "update validation requires target table".to_string(),
    })?;

    Ok(Some(UpdateValidationPlan {
        table,
        where_clause,
        snapshot_content,
        snapshot_patch,
    }))
}

fn snapshot_content_from_assignments(
    assignments: &[Assignment],
    params: &[EngineValue],
) -> Result<(Option<JsonValue>, Option<BTreeMap<String, JsonValue>>), LixError> {
    let mut state = PlaceholderState::new();
    for assignment in assignments {
        let value = resolve_expr_cell_with_state(&assignment.value, params, &mut state)?;
        if assignment_target_is_column(&assignment.target, "snapshot_content") {
            if value.value.is_none() {
                if let Some(patch) = extract_snapshot_patch_from_expr(&assignment.value)? {
                    return Ok((None, Some(patch)));
                }
            }
            return Ok((resolved_snapshot_json_value(value.value)?, None));
        }
    }
    Ok((None, None))
}

fn resolved_snapshot_json_value(value: Option<EngineValue>) -> Result<Option<JsonValue>, LixError> {
    match value {
        None => Ok(None),
        Some(EngineValue::Null) => Ok(None),
        Some(EngineValue::Text(value)) => serde_json::from_str::<JsonValue>(&value)
            .map(Some)
            .map_err(|err| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: format!("vtable update snapshot_content invalid JSON: {err}"),
            }),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: "vtable update requires literal snapshot_content".to_string(),
        }),
    }
}

fn extract_snapshot_patch_from_expr(
    expr: &Expr,
) -> Result<Option<BTreeMap<String, JsonValue>>, LixError> {
    if let Some(patch) = parse_sqlite_json_set_patch(expr)? {
        return Ok(Some(patch));
    }
    if let Some(patch) = parse_postgres_jsonb_set_patch(expr)? {
        return Ok(Some(patch));
    }
    Ok(None)
}

fn parse_sqlite_json_set_patch(
    expr: &Expr,
) -> Result<Option<BTreeMap<String, JsonValue>>, LixError> {
    let Expr::Function(function) = expr else {
        return Ok(None);
    };
    if !function_name_matches(&function.name, "json_set") {
        return Ok(None);
    }
    let Some(args) = function_unnamed_expr_args(function) else {
        return Ok(None);
    };
    if args.len() < 3 || args.len() % 2 == 0 {
        return Ok(None);
    }

    let mut patch = BTreeMap::new();
    let mut index = 1usize;
    while index + 1 < args.len() {
        let Some(property) = parse_sqlite_json_path_property(args[index]) else {
            return Ok(None);
        };
        let value = parse_sqlite_patch_value(args[index + 1])?;
        patch.insert(property, value);
        index += 2;
    }
    Ok(Some(patch))
}

fn parse_postgres_jsonb_set_patch(
    expr: &Expr,
) -> Result<Option<BTreeMap<String, JsonValue>>, LixError> {
    let mut patch = BTreeMap::new();
    if collect_postgres_jsonb_set_patch(expr, &mut patch)? {
        return Ok(Some(patch));
    }
    Ok(None)
}

fn collect_postgres_jsonb_set_patch(
    expr: &Expr,
    patch: &mut BTreeMap<String, JsonValue>,
) -> Result<bool, LixError> {
    let expr = unwrap_cast_expr(expr);
    let Expr::Function(function) = expr else {
        return Ok(is_postgres_snapshot_base(expr));
    };
    if !function_name_matches(&function.name, "jsonb_set") {
        return Ok(is_postgres_snapshot_base(expr));
    }
    let Some(args) = function_unnamed_expr_args(function) else {
        return Ok(false);
    };
    if args.len() < 3 {
        return Ok(false);
    }

    let base_ok = if is_postgres_snapshot_base(args[0]) {
        true
    } else {
        collect_postgres_jsonb_set_patch(args[0], patch)?
    };
    if !base_ok {
        return Ok(false);
    }

    let Some(property) = parse_postgres_json_path_property(args[1]) else {
        return Ok(false);
    };
    let value = parse_postgres_patch_value(args[2])?;
    patch.insert(property, value);
    Ok(true)
}

fn parse_sqlite_json_path_property(path: &Expr) -> Option<String> {
    let path = single_quoted_literal(path)?;
    if !(path.starts_with("$.\"") && path.ends_with('"')) {
        return None;
    }
    let property = &path[3..path.len() - 1];
    Some(property.replace("\\\"", "\"").replace("\\\\", "\\"))
}

fn parse_postgres_json_path_property(path: &Expr) -> Option<String> {
    let path = single_quoted_literal(path)?;
    if !(path.starts_with('{') && path.ends_with('}')) {
        return None;
    }
    let property = &path[1..path.len() - 1];
    if property.is_empty() || property.contains(',') {
        return None;
    }
    Some(property.to_string())
}

fn parse_sqlite_patch_value(value: &Expr) -> Result<JsonValue, LixError> {
    if let Some(value) = parse_json_function_value(value)? {
        return Ok(value);
    }
    parse_json_literal_value(value)
}

fn parse_postgres_patch_value(value: &Expr) -> Result<JsonValue, LixError> {
    let value = unwrap_cast_expr(value);
    let raw = single_quoted_literal(value).ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        title: "Unknown error".to_string(),
        description: "vtable update requires JSONB patch values to be single-quoted JSON literals"
            .to_string(),
    })?;
    serde_json::from_str(raw).map_err(|err| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        title: "Unknown error".to_string(),
        description: format!("vtable update JSONB patch value is not valid JSON: {err}"),
    })
}

fn parse_json_function_value(expr: &Expr) -> Result<Option<JsonValue>, LixError> {
    let Expr::Function(function) = expr else {
        return Ok(None);
    };
    if !function_name_matches(&function.name, "json") {
        return Ok(None);
    }
    let Some(args) = function_unnamed_expr_args(function) else {
        return Ok(None);
    };
    if args.len() != 1 {
        return Ok(None);
    }
    let Some(raw) = single_quoted_literal(args[0]) else {
        return Ok(None);
    };
    serde_json::from_str(raw).map(Some).map_err(|err| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        title: "Unknown error".to_string(),
        description: format!("vtable update json(...) patch value is not valid JSON: {err}"),
    })
}

fn parse_json_literal_value(expr: &Expr) -> Result<JsonValue, LixError> {
    let Expr::Value(ValueWithSpan { value, .. }) = expr else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: "vtable update patch requires literal property values".to_string(),
        });
    };

    match value {
        Value::Null => Ok(JsonValue::Null),
        Value::Boolean(value) => Ok(JsonValue::Bool(*value)),
        Value::Number(value, _) => {
            if let Ok(parsed) = value.parse::<i64>() {
                Ok(JsonValue::Number(parsed.into()))
            } else if let Ok(parsed) = value.parse::<f64>() {
                serde_json::Number::from_f64(parsed)
                    .map(JsonValue::Number)
                    .ok_or_else(|| LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        title: "Unknown error".to_string(),
                        description: "vtable update patch contains non-finite number".to_string(),
                    })
            } else {
                Err(LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    title: "Unknown error".to_string(),
                    description: format!(
                        "vtable update patch contains invalid numeric literal '{value}'"
                    ),
                })
            }
        }
        Value::SingleQuotedString(value)
        | Value::DoubleQuotedString(value)
        | Value::TripleSingleQuotedString(value)
        | Value::TripleDoubleQuotedString(value)
        | Value::EscapedStringLiteral(value)
        | Value::UnicodeStringLiteral(value)
        | Value::NationalStringLiteral(value)
        | Value::HexStringLiteral(value)
        | Value::SingleQuotedRawStringLiteral(value)
        | Value::DoubleQuotedRawStringLiteral(value)
        | Value::TripleSingleQuotedRawStringLiteral(value)
        | Value::TripleDoubleQuotedRawStringLiteral(value)
        | Value::SingleQuotedByteStringLiteral(value)
        | Value::DoubleQuotedByteStringLiteral(value)
        | Value::TripleSingleQuotedByteStringLiteral(value)
        | Value::TripleDoubleQuotedByteStringLiteral(value) => Ok(JsonValue::String(value.clone())),
        Value::DollarQuotedString(value) => Ok(JsonValue::String(value.value.clone())),
        Value::Placeholder(token) => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: format!("vtable update patch contains unresolved placeholder '{token}'"),
        }),
    }
}

fn is_postgres_snapshot_base(expr: &Expr) -> bool {
    let expr = unwrap_cast_expr(expr);
    let Expr::Function(function) = expr else {
        return false;
    };
    if !function_name_matches(&function.name, "coalesce") {
        return false;
    }
    let Some(args) = function_unnamed_expr_args(function) else {
        return false;
    };
    if args.len() < 2 {
        return false;
    }
    expr_is_snapshot_content_reference(args[0]) && single_quoted_literal(args[1]) == Some("{}")
}

fn expr_is_snapshot_content_reference(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case("snapshot_content"),
        Expr::CompoundIdentifier(idents) => idents
            .last()
            .map(|ident| ident.value.eq_ignore_ascii_case("snapshot_content"))
            .unwrap_or(false),
        _ => false,
    }
}

fn function_name_matches(name: &ObjectName, target: &str) -> bool {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.eq_ignore_ascii_case(target))
        .unwrap_or(false)
}

fn function_unnamed_expr_args<'a>(function: &'a Function) -> Option<Vec<&'a Expr>> {
    let FunctionArguments::List(FunctionArgumentList { args, .. }) = &function.args else {
        return None;
    };
    let mut out = Vec::with_capacity(args.len());
    for arg in args {
        let sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(expr)) = arg
        else {
            return None;
        };
        out.push(expr);
    }
    Some(out)
}

fn single_quoted_literal(expr: &Expr) -> Option<&str> {
    let Expr::Value(ValueWithSpan {
        value: Value::SingleQuotedString(value),
        ..
    }) = expr
    else {
        return None;
    };
    Some(value.as_str())
}

fn unwrap_cast_expr(mut expr: &Expr) -> &Expr {
    loop {
        match expr {
            Expr::Cast { expr: inner, .. } => {
                expr = inner.as_ref();
            }
            Expr::Nested(inner) => {
                expr = inner.as_ref();
            }
            _ => return expr,
        }
    }
}

fn extract_single_schema_key(expr: &Expr, params: &[EngineValue]) -> Result<String, LixError> {
    let keys = extract_string_column_values_from_expr(expr, expr_is_schema_key_column, params)
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: "vtable update requires schema_key predicate".to_string(),
        })?;
    if keys.len() != 1 {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: "vtable update requires a single schema_key".to_string(),
        });
    }
    Ok(keys[0].clone())
}

fn expr_is_schema_key_column(expr: &Expr) -> bool {
    expr_last_identifier_eq(expr, "schema_key")
}

#[cfg(test)]
mod tests {
    use super::{
        bind_statement_batch_for_dialect, build_statements_from_generate_commit_result,
        rewrite_delete, rewrite_insert, rewrite_update, DeleteRewrite, UpdateRewrite,
        UPDATE_RETURNING_COLUMNS,
    };
    use crate::backend::SqlDialect;
    use crate::commit::{ChangeRow, GenerateCommitResult, MaterializedStateRow};
    use crate::functions::SystemFunctionProvider;
    use crate::Value as EngineValue;
    use serde_json::json;
    use sqlparser::ast::{
        Expr, ObjectNamePart, SetExpr, Statement, TableObject, Value, ValueWithSpan,
    };
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn rewrite_tracked_insert_emits_snapshot_change_and_materialized() {
        let sql = r#"INSERT INTO lix_internal_state_vtable
            (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version)
            VALUES ('entity-1', 'test_schema', 'file-1', 'version-1', 'lix', '{"key":"value"}', '1')"#;
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse sql");
        let statement = statements.remove(0);

        let insert = match statement {
            Statement::Insert(insert) => insert,
            _ => panic!("expected insert"),
        };

        let mut provider = SystemFunctionProvider;
        let rewrite = rewrite_insert(insert, &[], &mut provider)
            .expect("rewrite ok")
            .expect("rewrite applied");

        assert_eq!(rewrite.statements.len(), 3);

        let snapshot_stmt = find_insert(&rewrite.statements, "lix_internal_snapshot");
        let change_stmt = find_insert(&rewrite.statements, "lix_internal_change");
        let materialized_stmt = find_insert(
            &rewrite.statements,
            "lix_internal_state_materialized_v1_test_schema",
        );

        let snapshot_id = extract_string_value(snapshot_stmt, "id");
        let change_snapshot_id = extract_string_value(change_stmt, "snapshot_id");
        assert_eq!(snapshot_id, change_snapshot_id);

        let change_id = extract_string_value(change_stmt, "id");
        let materialized_change_id = extract_string_value(materialized_stmt, "change_id");
        assert_eq!(change_id, materialized_change_id);
    }

    #[test]
    fn rewrite_tracked_insert_uses_no_content_snapshot() {
        let sql = r#"INSERT INTO lix_internal_state_vtable
            (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version)
            VALUES ('entity-1', 'test_schema', 'file-1', 'version-1', 'lix', NULL, '1')"#;
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse sql");
        let statement = statements.remove(0);

        let insert = match statement {
            Statement::Insert(insert) => insert,
            _ => panic!("expected insert"),
        };

        let mut provider = SystemFunctionProvider;
        let rewrite = rewrite_insert(insert, &[], &mut provider)
            .expect("rewrite ok")
            .expect("rewrite applied");

        let change_stmt = find_insert(&rewrite.statements, "lix_internal_change");
        let snapshot_id = extract_string_value(change_stmt, "snapshot_id");
        assert_eq!(snapshot_id, "no-content");

        let snapshot_stmt = find_insert(&rewrite.statements, "lix_internal_snapshot");
        let ensured_id = extract_string_value(snapshot_stmt, "id");
        assert_eq!(ensured_id, "no-content");
    }

    #[test]
    fn commit_result_snapshot_statement_is_parameterized() {
        let mut provider = SystemFunctionProvider;
        let snapshot_content =
            "{\"wordPattern\":\"[^\\\\/\\\\?\\\\s]+\",\"quote\":\"''\"}".to_string();
        let batch = build_statements_from_generate_commit_result(
            GenerateCommitResult {
                changes: vec![ChangeRow {
                    id: "change-1".to_string(),
                    entity_id: "entity-1".to_string(),
                    schema_key: "schema-1".to_string(),
                    schema_version: "1".to_string(),
                    file_id: "file-1".to_string(),
                    plugin_key: "plugin-md-v2".to_string(),
                    snapshot_content: Some(snapshot_content.clone()),
                    metadata: None,
                    created_at: "2025-01-01T00:00:00.000Z".to_string(),
                }],
                materialized_state: Vec::new(),
            },
            &mut provider,
            0,
            SqlDialect::Sqlite,
        )
        .expect("build statements");

        assert_eq!(
            batch.params.len(),
            10,
            "expected exact params: 2 snapshot params + 8 change-row params"
        );
        assert!(
            matches!(batch.params[1], EngineValue::Text(ref value) if value == &snapshot_content)
        );

        let snapshot_stmt = find_insert(&batch.statements, "lix_internal_snapshot");
        let (columns, rows) = insert_values(snapshot_stmt);
        let id_idx = columns
            .iter()
            .position(|name| name.eq_ignore_ascii_case("id"))
            .expect("id column");
        let content_idx = columns
            .iter()
            .position(|name| name.eq_ignore_ascii_case("content"))
            .expect("content column");

        assert_eq!(rows.len(), 1);
        assert!(matches!(
            rows[0][id_idx],
            Expr::Value(ValueWithSpan {
                value: Value::Placeholder(_),
                ..
            })
        ));
        assert!(matches!(
            rows[0][content_idx],
            Expr::Value(ValueWithSpan {
                value: Value::Placeholder(_),
                ..
            })
        ));
    }

    #[test]
    fn snapshot_binding_treats_question_mark_content_as_data() {
        let mut provider = SystemFunctionProvider;
        let snapshot_content = "{\"md\":\"[link](x)? y\",\"quote\":\"''\"}".to_string();
        let batch = build_statements_from_generate_commit_result(
            GenerateCommitResult {
                changes: vec![ChangeRow {
                    id: "change-1".to_string(),
                    entity_id: "entity-1".to_string(),
                    schema_key: "schema-1".to_string(),
                    schema_version: "1".to_string(),
                    file_id: "file-1".to_string(),
                    plugin_key: "plugin-md-v2".to_string(),
                    snapshot_content: Some(snapshot_content.clone()),
                    metadata: None,
                    created_at: "2025-01-01T00:00:00.000Z".to_string(),
                }],
                materialized_state: Vec::new(),
            },
            &mut provider,
            0,
            SqlDialect::Sqlite,
        )
        .expect("build statements");

        let prepared = bind_statement_batch_for_dialect(batch, SqlDialect::Sqlite)
            .expect("bind generated statements");
        let snapshot = prepared
            .iter()
            .find(|statement| statement.sql.contains("INSERT INTO lix_internal_snapshot"))
            .expect("snapshot insert statement");

        assert!(
            !snapshot.sql.contains(&snapshot_content),
            "snapshot content must not be interpolated into SQL text"
        );
        assert!(
            snapshot.sql.contains('?') || snapshot.sql.contains("$1"),
            "snapshot insert should use placeholders"
        );
        assert_eq!(snapshot.params.len(), 2);
        assert!(matches!(
            snapshot.params[1],
            EngineValue::Text(ref value) if value == &snapshot_content
        ));
    }

    #[test]
    fn build_statements_from_commit_result_emits_commit_ancestry_upserts() {
        let mut provider = SystemFunctionProvider;
        let batch = build_statements_from_generate_commit_result(
            GenerateCommitResult {
                changes: Vec::new(),
                materialized_state: vec![
                    MaterializedStateRow {
                        id: "change-commit".to_string(),
                        entity_id: "child-commit".to_string(),
                        schema_key: "lix_commit".to_string(),
                        schema_version: "1".to_string(),
                        file_id: "lix".to_string(),
                        plugin_key: "lix".to_string(),
                        snapshot_content: Some(
                            json!({
                                "id": "child-commit",
                                "change_set_id": "child-change-set",
                                "parent_commit_ids": ["parent-commit"],
                                "change_ids": [],
                            })
                            .to_string(),
                        ),
                        metadata: None,
                        created_at: "2025-01-01T00:00:00.000Z".to_string(),
                        lixcol_version_id: "global".to_string(),
                        lixcol_commit_id: "child-commit".to_string(),
                        writer_key: None,
                    },
                    MaterializedStateRow {
                        id: "change-edge".to_string(),
                        entity_id: "parent-commit~child-commit".to_string(),
                        schema_key: "lix_commit_edge".to_string(),
                        schema_version: "1".to_string(),
                        file_id: "lix".to_string(),
                        plugin_key: "lix".to_string(),
                        snapshot_content: Some(
                            json!({
                                "parent_id": "parent-commit",
                                "child_id": "child-commit",
                            })
                            .to_string(),
                        ),
                        metadata: None,
                        created_at: "2025-01-01T00:00:00.000Z".to_string(),
                        lixcol_version_id: "global".to_string(),
                        lixcol_commit_id: "child-commit".to_string(),
                        writer_key: None,
                    },
                ],
            },
            &mut provider,
            0,
            SqlDialect::Sqlite,
        )
        .expect("build statements");

        let sql = batch
            .statements
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(sql.contains("INSERT INTO lix_internal_commit_ancestry"));
        assert!(sql.contains("ON CONFLICT"));
        assert!(sql.contains("DO UPDATE"));
        assert!(sql.contains("SET depth = CASE"));
        assert!(sql.contains("GROUP BY candidate.ancestor_id"));
    }

    #[test]
    fn rewrite_tracked_insert_multiple_rows_emits_multiple_changes() {
        let sql = r#"INSERT INTO lix_internal_state_vtable
            (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version)
            VALUES
            ('entity-1', 'test_schema', 'file-1', 'version-1', 'lix', '{"key":"one"}', '1'),
            ('entity-2', 'test_schema', 'file-1', 'version-1', 'lix', '{"key":"two"}', '1')"#;
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse sql");
        let statement = statements.remove(0);

        let insert = match statement {
            Statement::Insert(insert) => insert,
            _ => panic!("expected insert"),
        };

        let mut provider = SystemFunctionProvider;
        let rewrite = rewrite_insert(insert, &[], &mut provider)
            .expect("rewrite ok")
            .expect("rewrite applied");

        let change_stmt = find_insert(&rewrite.statements, "lix_internal_change");
        let (columns, rows) = insert_values(change_stmt);
        assert_eq!(rows.len(), 2);

        let id_idx = columns
            .iter()
            .position(|name| name.eq_ignore_ascii_case("id"))
            .expect("id column");
        let entity_idx = columns
            .iter()
            .position(|name| name.eq_ignore_ascii_case("entity_id"))
            .expect("entity_id column");
        let snapshot_idx = columns
            .iter()
            .position(|name| name.eq_ignore_ascii_case("snapshot_id"))
            .expect("snapshot_id column");

        let mut entity_ids = Vec::new();
        let mut snapshot_ids = Vec::new();
        for row in rows {
            match &row[id_idx] {
                Expr::Value(ValueWithSpan {
                    value: Value::SingleQuotedString(_),
                    ..
                }) => {}
                _ => panic!("expected change id literal"),
            }
            entity_ids.push(match &row[entity_idx] {
                Expr::Value(ValueWithSpan {
                    value: Value::SingleQuotedString(value),
                    ..
                }) => value.clone(),
                _ => panic!("expected entity id literal"),
            });
            snapshot_ids.push(match &row[snapshot_idx] {
                Expr::Value(ValueWithSpan {
                    value: Value::SingleQuotedString(value),
                    ..
                }) => value.clone(),
                _ => panic!("expected snapshot id literal"),
            });
        }

        entity_ids.sort();
        snapshot_ids.sort();
        assert_eq!(
            entity_ids,
            vec!["entity-1".to_string(), "entity-2".to_string()]
        );
        assert_eq!(snapshot_ids.len(), 2);
        assert_ne!(snapshot_ids[0], snapshot_ids[1]);

        let snapshot_stmt = find_insert(&rewrite.statements, "lix_internal_snapshot");
        let (snapshot_columns, snapshot_rows) = insert_values(snapshot_stmt);
        let snapshot_id_idx = snapshot_columns
            .iter()
            .position(|name| name.eq_ignore_ascii_case("id"))
            .expect("snapshot id column");
        let snapshot_ids_from_insert = snapshot_rows
            .iter()
            .map(|row| match &row[snapshot_id_idx] {
                Expr::Value(ValueWithSpan {
                    value: Value::SingleQuotedString(value),
                    ..
                }) => value.clone(),
                _ => panic!("expected snapshot id literal"),
            })
            .collect::<Vec<_>>();

        assert_eq!(snapshot_ids_from_insert.len(), 2);
        for id in snapshot_ids_from_insert {
            assert!(snapshot_ids.contains(&id));
        }
    }

    #[test]
    fn rewrite_untracked_insert_routes_to_untracked_table() {
        let sql = r#"INSERT INTO lix_internal_state_vtable
            (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked)
            VALUES ('entity-1', 'test_schema', 'file-1', 'version-1', 'lix', '{"key":"value"}', '1', true)"#;
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse sql");
        let statement = statements.remove(0);

        let insert = match statement {
            Statement::Insert(insert) => insert,
            _ => panic!("expected insert"),
        };

        let mut provider = SystemFunctionProvider;
        let rewrite = rewrite_insert(insert, &[], &mut provider)
            .expect("rewrite ok")
            .expect("rewrite applied");

        assert_eq!(rewrite.statements.len(), 1);
        let stmt = &rewrite.statements[0];
        assert_eq!(table_name(stmt), "lix_internal_state_untracked");
    }

    #[test]
    fn rewrite_untracked_insert_accepts_true_boolean_parameter() {
        let sql = r#"INSERT INTO lix_internal_state_vtable
            (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked)
            VALUES ('entity-1', 'test_schema', 'file-1', 'version-1', 'lix', '{"key":"value"}', '1', ?)"#;
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse sql");
        let statement = statements.remove(0);

        let insert = match statement {
            Statement::Insert(insert) => insert,
            _ => panic!("expected insert"),
        };

        let mut provider = SystemFunctionProvider;
        let rewrite = rewrite_insert(insert, &[EngineValue::Boolean(true)], &mut provider)
            .expect("rewrite ok")
            .expect("rewrite applied");

        assert_eq!(rewrite.statements.len(), 1);
        let stmt = &rewrite.statements[0];
        assert_eq!(table_name(stmt), "lix_internal_state_untracked");
    }

    #[test]
    fn rewrite_untracked_insert_rejects_numeric_parameter() {
        let sql = r#"INSERT INTO lix_internal_state_vtable
            (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked)
            VALUES ('entity-1', 'test_schema', 'file-1', 'version-1', 'lix', '{"key":"value"}', '1', ?)"#;
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse sql");
        let statement = statements.remove(0);

        let insert = match statement {
            Statement::Insert(insert) => insert,
            _ => panic!("expected insert"),
        };

        let mut provider = SystemFunctionProvider;
        let error = match rewrite_insert(insert, &[EngineValue::Integer(1)], &mut provider) {
            Ok(_) => panic!("expected rewrite error"),
            Err(error) => error,
        };
        assert!(
            error
                .description
                .contains("vtable insert requires literal or parameter untracked values"),
            "unexpected error message: {}",
            error.description
        );
    }

    #[test]
    fn rewrite_insert_allows_on_conflict_do_update() {
        let sql = r#"INSERT INTO lix_internal_state_vtable
            (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked)
            VALUES ('entity-1', 'test_schema', 'file-1', 'version-1', 'lix', '{"key":"value"}', '1', true)
            ON CONFLICT (entity_id, schema_key, file_id, version_id)
            DO UPDATE SET snapshot_content = '{"key":"next"}'"#;
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse sql");
        let statement = statements.remove(0);

        let insert = match statement {
            Statement::Insert(insert) => insert,
            _ => panic!("expected insert"),
        };

        let mut provider = SystemFunctionProvider;
        let rewrite = rewrite_insert(insert, &[], &mut provider)
            .expect("rewrite ok")
            .expect("rewrite applied");

        assert_eq!(rewrite.statements.len(), 1);
        assert_eq!(
            table_name(&rewrite.statements[0]),
            "lix_internal_state_untracked"
        );
    }

    #[test]
    fn rewrite_insert_rejects_on_conflict_do_nothing() {
        let sql = r#"INSERT INTO lix_internal_state_vtable
            (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked)
            VALUES ('entity-1', 'test_schema', 'file-1', 'version-1', 'lix', '{"key":"value"}', '1', true)
            ON CONFLICT (entity_id, schema_key, file_id, version_id)
            DO NOTHING"#;
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse sql");
        let statement = statements.remove(0);

        let insert = match statement {
            Statement::Insert(insert) => insert,
            _ => panic!("expected insert"),
        };

        let mut provider = SystemFunctionProvider;
        let error = match rewrite_insert(insert, &[], &mut provider) {
            Ok(_) => panic!("expected rewrite error"),
            Err(error) => error,
        };
        assert!(
            error
                .description
                .contains("ON CONFLICT DO NOTHING is not supported"),
            "unexpected error message: {}",
            error.description
        );
    }

    #[test]
    fn rewrite_tracked_update_adds_returning_and_updated_at() {
        let sql = r#"UPDATE lix_internal_state_vtable
            SET snapshot_content = '{"key":"value"}'
            WHERE schema_key = 'test_schema' AND entity_id = 'entity-1'"#;
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse sql");
        let statement = statements.remove(0);

        let update = match statement {
            Statement::Update(update) => update,
            _ => panic!("expected update"),
        };

        let rewrite = rewrite_update(update, &[])
            .expect("rewrite ok")
            .expect("rewrite applied");

        let planned = match rewrite {
            UpdateRewrite::Planned(planned) => planned,
            _ => panic!("expected planned rewrite"),
        };

        let statement = match planned.statement {
            Statement::Update(update) => update,
            _ => panic!("expected update"),
        };

        let table_name = statement.table.to_string();
        assert!(table_name.contains("lix_internal_state_materialized_v1_test_schema"));

        let returning = statement.returning.expect("returning");
        let returned = returning
            .iter()
            .map(|item| item.to_string())
            .collect::<Vec<_>>();
        let expected = UPDATE_RETURNING_COLUMNS
            .iter()
            .map(|name| name.to_string())
            .collect::<Vec<_>>();
        assert_eq!(returned, expected);

        let assignments = statement
            .assignments
            .iter()
            .map(|assignment| assignment.target.to_string())
            .collect::<Vec<_>>();
        assert!(assignments.iter().any(|name| name == "updated_at"));
    }

    #[test]
    fn rewrite_tracked_update_materializes_parameterized_snapshot_for_validation() {
        let sql = r#"UPDATE lix_internal_state_vtable
            SET snapshot_content = $1
            WHERE schema_key = 'test_schema' AND entity_id = 'entity-1'"#;
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse sql");
        let statement = statements.remove(0);

        let update = match statement {
            Statement::Update(update) => update,
            _ => panic!("expected update"),
        };

        let rewrite = rewrite_update(
            update,
            &[EngineValue::Text("{\"key\":\"value\"}".to_string())],
        )
        .expect("rewrite ok")
        .expect("rewrite applied");

        let planned = match rewrite {
            UpdateRewrite::Planned(planned) => planned,
            _ => panic!("expected planned rewrite"),
        };

        let validation = planned.validation.expect("validation plan");
        assert_eq!(validation.snapshot_content, Some(json!({ "key": "value" })));
    }

    #[test]
    fn rewrite_tracked_update_materializes_sequential_placeholder_snapshot_for_validation() {
        let sql = r#"UPDATE lix_internal_state_vtable
            SET snapshot_content = ?
            WHERE schema_key = 'test_schema' AND entity_id = 'entity-1'"#;
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse sql");
        let statement = statements.remove(0);

        let update = match statement {
            Statement::Update(update) => update,
            _ => panic!("expected update"),
        };

        let rewrite = rewrite_update(
            update,
            &[EngineValue::Text("{\"key\":\"value\"}".to_string())],
        )
        .expect("rewrite ok")
        .expect("rewrite applied");

        let planned = match rewrite {
            UpdateRewrite::Planned(planned) => planned,
            _ => panic!("expected planned rewrite"),
        };

        let validation = planned.validation.expect("validation plan");
        assert_eq!(validation.snapshot_content, Some(json!({ "key": "value" })));
    }

    #[test]
    fn rewrite_untracked_update_strips_only_untracked_predicate() {
        let sql = r#"UPDATE lix_internal_state_vtable
            SET snapshot_content = $1
            WHERE untracked = true
              AND schema_key = 'lix_active_version'
              AND file_id = 'lix'
              AND version_id = 'global'"#;
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse sql");
        let statement = statements.remove(0);

        let update = match statement {
            Statement::Update(update) => update,
            _ => panic!("expected update"),
        };

        let rewrite = rewrite_update(
            update,
            &[EngineValue::Text(
                json!({
                    "id": "active-version-id",
                    "version_id": "version-a"
                })
                .to_string(),
            )],
        )
        .expect("rewrite ok")
        .expect("rewrite applied");

        let statement = match rewrite {
            UpdateRewrite::Statement(statement) => statement.statement,
            _ => panic!("expected untracked statement rewrite"),
        };
        let Statement::Update(update) = statement else {
            panic!("expected update statement");
        };

        assert!(
            update.table.to_string().contains("lix_internal_state_untracked"),
            "expected rewrite target to be untracked table, got: {}",
            update.table
        );
        let selection = update.selection.expect("selection should remain after stripping");
        let selection_sql = selection.to_string();
        assert!(selection_sql.contains("schema_key = 'lix_active_version'"));
        assert!(selection_sql.contains("file_id = 'lix'"));
        assert!(selection_sql.contains("version_id = 'global'"));
        assert!(!selection_sql.contains("untracked = true"));
    }

    #[test]
    fn rewrite_update_extracts_writer_key_after_prior_placeholders() {
        let sql = r#"UPDATE lix_internal_state_vtable
            SET metadata = ? || '', writer_key = ?
            WHERE schema_key = 'test_schema' AND entity_id = 'entity-1'"#;
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse sql");
        let statement = statements.remove(0);

        let update = match statement {
            Statement::Update(update) => update,
            _ => panic!("expected update"),
        };

        let rewrite = rewrite_update(
            update,
            &[
                EngineValue::Text("ignored".to_string()),
                EngineValue::Text("editor:explicit".to_string()),
            ],
        )
        .expect("rewrite ok")
        .expect("rewrite applied");

        let planned = match rewrite {
            UpdateRewrite::Planned(planned) => planned,
            _ => panic!("expected planned rewrite"),
        };

        assert_eq!(
            planned.plan.explicit_writer_key,
            Some(Some("editor:explicit".to_string()))
        );
        assert!(planned.plan.writer_key_assignment_present);
    }

    #[test]
    fn rewrite_update_non_literal_writer_key_does_not_force_null() {
        let sql = r#"UPDATE lix_internal_state_vtable
            SET writer_key = lower('EDITOR')
            WHERE schema_key = 'test_schema' AND entity_id = 'entity-1'"#;
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse sql");
        let statement = statements.remove(0);

        let update = match statement {
            Statement::Update(update) => update,
            _ => panic!("expected update"),
        };

        let rewrite = rewrite_update(update, &[])
            .expect("rewrite ok")
            .expect("rewrite applied");

        let planned = match rewrite {
            UpdateRewrite::Planned(planned) => planned,
            _ => panic!("expected planned rewrite"),
        };

        assert_eq!(planned.plan.explicit_writer_key, None);
        assert!(planned.plan.writer_key_assignment_present);
    }

    #[test]
    fn rewrite_update_without_writer_key_assignment_marks_absent() {
        let sql = r#"UPDATE lix_internal_state_vtable
            SET snapshot_content = '{"key":"value"}'
            WHERE schema_key = 'test_schema' AND entity_id = 'entity-1'"#;
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse sql");
        let statement = statements.remove(0);

        let update = match statement {
            Statement::Update(update) => update,
            _ => panic!("expected update"),
        };

        let rewrite = rewrite_update(update, &[])
            .expect("rewrite ok")
            .expect("rewrite applied");

        let planned = match rewrite {
            UpdateRewrite::Planned(planned) => planned,
            _ => panic!("expected planned rewrite"),
        };

        assert_eq!(planned.plan.explicit_writer_key, None);
        assert!(!planned.plan.writer_key_assignment_present);
    }

    #[test]
    fn rewrite_tracked_delete_updates_materialized_with_returning() {
        let sql = r#"DELETE FROM lix_internal_state_vtable
            WHERE schema_key = 'test_schema' AND entity_id = 'entity-1'"#;
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse sql");
        let statement = statements.remove(0);

        let delete = match statement {
            Statement::Delete(delete) => delete,
            _ => panic!("expected delete"),
        };

        let rewrite = rewrite_delete(delete, &[])
            .expect("rewrite ok")
            .expect("rewrite applied");

        let planned = match rewrite {
            DeleteRewrite::Planned(planned) => planned,
            _ => panic!("expected planned rewrite"),
        };

        let statement = match planned.statement {
            Statement::Update(update) => update,
            _ => panic!("expected update"),
        };

        let table_name = statement.table.to_string();
        assert!(table_name.contains("lix_internal_state_materialized_v1_test_schema"));

        let returning = statement.returning.expect("returning");
        let returned = returning
            .iter()
            .map(|item| item.to_string())
            .collect::<Vec<_>>();
        let expected = UPDATE_RETURNING_COLUMNS
            .iter()
            .map(|name| name.to_string())
            .collect::<Vec<_>>();
        assert_eq!(returned, expected);

        let assignments = statement
            .assignments
            .iter()
            .map(|assignment| assignment.target.to_string())
            .collect::<Vec<_>>();
        assert!(assignments.iter().any(|name| name == "is_tombstone"));
        assert!(!assignments.iter().any(|name| name == "snapshot_content"));
        assert!(assignments.iter().any(|name| name == "updated_at"));
    }

    #[test]
    fn rewrite_update_requires_schema_key_predicate() {
        let sql = r#"UPDATE lix_internal_state_vtable
            SET snapshot_content = '{"key":"value"}'
            WHERE entity_id = 'entity-1'"#;
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse sql");
        let statement = statements.remove(0);

        let update = match statement {
            Statement::Update(update) => update,
            _ => panic!("expected update"),
        };

        let err = rewrite_update(update, &[]).expect_err("expected error");
        assert!(err.description.contains("schema_key"), "{:#?}", err);
    }

    #[test]
    fn rewrite_update_requires_single_schema_key() {
        let sql = r#"UPDATE lix_internal_state_vtable
            SET snapshot_content = '{"key":"value"}'
            WHERE schema_key IN ('a', 'b') AND entity_id = 'entity-1'"#;
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse sql");
        let statement = statements.remove(0);

        let update = match statement {
            Statement::Update(update) => update,
            _ => panic!("expected update"),
        };

        let err = rewrite_update(update, &[]).expect_err("expected error");
        assert!(err.description.contains("single schema_key"), "{:#?}", err);
    }

    #[test]
    fn rewrite_update_rejects_schema_key_assignment() {
        let sql = r#"UPDATE lix_internal_state_vtable
            SET schema_key = 'other'
            WHERE schema_key = 'a' AND entity_id = 'entity-1'"#;
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse sql");
        let statement = statements.remove(0);

        let update = match statement {
            Statement::Update(update) => update,
            _ => panic!("expected update"),
        };

        let err = rewrite_update(update, &[]).expect_err("expected error");
        assert!(
            err.description.contains("cannot change schema_key"),
            "{:#?}",
            err
        );
    }

    #[test]
    fn rewrite_update_rejects_unknown_assignment_column() {
        let sql = r#"UPDATE lix_internal_state_vtable
            SET bogus = 'x'
            WHERE schema_key = 'a' AND entity_id = 'entity-1'"#;
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse sql");
        let statement = statements.remove(0);

        let update = match statement {
            Statement::Update(update) => update,
            _ => panic!("expected update"),
        };

        let err = rewrite_update(update, &[]).expect_err("expected error");
        assert!(
            err.description.contains("strict rewrite violation")
                && err.description.contains("unknown column")
                && err.description.contains("bogus"),
            "{:#?}",
            err
        );
    }

    #[test]
    fn rewrite_update_rejects_updated_at_assignment() {
        let sql = r#"UPDATE lix_internal_state_vtable
            SET updated_at = '2024-01-01T00:00:00Z'
            WHERE schema_key = 'a' AND entity_id = 'entity-1'"#;
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse sql");
        let statement = statements.remove(0);

        let update = match statement {
            Statement::Update(update) => update,
            _ => panic!("expected update"),
        };

        let err = rewrite_update(update, &[]).expect_err("expected error");
        assert!(
            err.description.contains("cannot set updated_at"),
            "{:#?}",
            err
        );
    }

    #[test]
    fn rewrite_delete_requires_schema_key_predicate() {
        let sql = r#"DELETE FROM lix_internal_state_vtable
            WHERE entity_id = 'entity-1'"#;
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse sql");
        let statement = statements.remove(0);

        let delete = match statement {
            Statement::Delete(delete) => delete,
            _ => panic!("expected delete"),
        };

        let err = rewrite_delete(delete, &[]).expect_err("expected error");
        assert!(err.description.contains("schema_key"), "{:#?}", err);
    }

    #[test]
    fn rewrite_delete_requires_single_schema_key() {
        let sql = r#"DELETE FROM lix_internal_state_vtable
            WHERE schema_key = 'a' OR schema_key = 'b'"#;
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse sql");
        let statement = statements.remove(0);

        let delete = match statement {
            Statement::Delete(delete) => delete,
            _ => panic!("expected delete"),
        };

        let err = rewrite_delete(delete, &[]).expect_err("expected error");
        assert!(err.description.contains("single schema_key"), "{:#?}", err);
    }

    fn find_insert<'a>(statements: &'a [Statement], table: &str) -> &'a Statement {
        statements
            .iter()
            .find(|stmt| table_name(stmt) == table)
            .unwrap_or_else(|| panic!("missing insert into {table}"))
    }

    fn table_name(statement: &Statement) -> &str {
        match statement {
            Statement::Insert(insert) => match &insert.table {
                TableObject::TableName(name) => name
                    .0
                    .last()
                    .and_then(ObjectNamePart::as_ident)
                    .map(|ident| ident.value.as_str())
                    .expect("table name ident"),
                _ => panic!("expected table name"),
            },
            _ => panic!("expected insert statement"),
        }
    }

    fn extract_string_value(statement: &Statement, column: &str) -> String {
        let (columns, rows) = insert_values(statement);
        let idx = columns
            .iter()
            .position(|name| name.eq_ignore_ascii_case(column))
            .expect("column present");
        let expr = rows.get(0).and_then(|row| row.get(idx)).expect("row value");
        match expr {
            Expr::Value(ValueWithSpan {
                value: Value::SingleQuotedString(value),
                ..
            }) => value.clone(),
            _ => panic!("expected string literal"),
        }
    }

    fn insert_values(statement: &Statement) -> (Vec<String>, Vec<Vec<Expr>>) {
        match statement {
            Statement::Insert(insert) => {
                let columns = insert
                    .columns
                    .iter()
                    .map(|ident| ident.value.clone())
                    .collect::<Vec<_>>();
                let rows = match insert.source.as_ref().expect("insert source").body.as_ref() {
                    SetExpr::Values(values) => values.rows.clone(),
                    _ => panic!("expected values"),
                };
                (columns, rows)
            }
            _ => panic!("expected insert"),
        }
    }
}
