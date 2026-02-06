use serde_json::Value as JsonValue;
use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, CaseWhen, ConflictTarget, Delete, DoUpdate, Expr,
    Function, FunctionArgumentList, FunctionArguments, Ident, ObjectName, ObjectNamePart,
    OnConflict, OnConflictAction, OnInsert, Query, SelectItem, SetExpr, Statement, TableFactor,
    TableObject, TableWithJoins, Update, Value, ValueWithSpan, Values,
};
use std::collections::{BTreeMap, BTreeSet};

use crate::commit::{
    generate_commit, DomainChangeInput, GenerateCommitArgs, MaterializedStateRow, VersionInfo,
    VersionSnapshot,
};
use crate::functions::LixFunctionProvider;
use crate::sql::types::{
    MutationOperation, MutationRow, UpdateValidationPlan, VtableDeletePlan, VtableUpdatePlan,
};
use crate::sql::SchemaRegistration;
use crate::sql::{resolve_expr_cell_with_state, PlaceholderState, ResolvedCell, RowSourceResolver};
use crate::Value as EngineValue;
use crate::{LixBackend, LixError};

const VTABLE_NAME: &str = "lix_internal_state_vtable";
const UNTRACKED_TABLE: &str = "lix_internal_state_untracked";
const SNAPSHOT_TABLE: &str = "lix_internal_snapshot";
const CHANGE_TABLE: &str = "lix_internal_change";
const MATERIALIZED_PREFIX: &str = "lix_internal_state_materialized_v1_";
const VERSION_TIP_TABLE: &str = "lix_internal_state_materialized_v1_lix_version_tip";
const VERSION_TIP_SCHEMA_KEY: &str = "lix_version_tip";
const GLOBAL_VERSION: &str = "global";
const UPDATE_RETURNING_COLUMNS: &[&str] = &[
    "entity_id",
    "file_id",
    "version_id",
    "plugin_key",
    "schema_version",
    "snapshot_content",
    "updated_at",
];

pub struct VtableWriteRewrite {
    pub statements: Vec<Statement>,
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

pub fn rewrite_insert(
    insert: sqlparser::ast::Insert,
    params: &[EngineValue],
    functions: &mut dyn LixFunctionProvider,
) -> Result<Option<VtableWriteRewrite>, LixError> {
    if !table_object_is_vtable(&insert.table) {
        return Ok(None);
    }

    if insert.on.is_some() {
        return Err(LixError {
            message: "vtable insert does not support ON CONFLICT".to_string(),
        });
    }

    if insert.columns.is_empty() {
        return Err(LixError {
            message: "vtable insert requires explicit columns".to_string(),
        });
    }

    let split_rows = split_insert_rows(&insert, params)?;
    let tracked_rows = split_rows.tracked;
    let untracked_rows = split_rows.untracked;

    let mut statements: Vec<Statement> = Vec::new();
    let mut registrations: Vec<SchemaRegistration> = Vec::new();
    let mut mutations: Vec<MutationRow> = Vec::new();

    if !tracked_rows.is_empty() {
        let tracked = rewrite_tracked_rows(
            &insert,
            tracked_rows,
            &mut registrations,
            &mut mutations,
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
        registrations,
        mutations,
    }))
}

pub async fn rewrite_insert_with_backend(
    backend: &dyn LixBackend,
    insert: sqlparser::ast::Insert,
    params: &[EngineValue],
    functions: &mut dyn LixFunctionProvider,
) -> Result<Option<VtableWriteRewrite>, LixError> {
    if !table_object_is_vtable(&insert.table) {
        return Ok(None);
    }

    if insert.on.is_some() {
        return Err(LixError {
            message: "vtable insert does not support ON CONFLICT".to_string(),
        });
    }

    if insert.columns.is_empty() {
        return Err(LixError {
            message: "vtable insert requires explicit columns".to_string(),
        });
    }

    let split_rows = split_insert_rows(&insert, params)?;
    let tracked_rows = split_rows.tracked;
    let untracked_rows = split_rows.untracked;

    let mut statements: Vec<Statement> = Vec::new();
    let mut registrations: Vec<SchemaRegistration> = Vec::new();
    let mut mutations: Vec<MutationRow> = Vec::new();

    if !tracked_rows.is_empty() {
        let tracked = rewrite_tracked_rows_with_backend(
            backend,
            &insert,
            tracked_rows,
            &mut registrations,
            &mut mutations,
            functions,
        )
        .await?;
        statements.extend(tracked);
    }

    if !untracked_rows.is_empty() {
        let untracked = build_untracked_insert(&insert, untracked_rows, &mut mutations, functions)?;
        statements.push(untracked);
    }

    Ok(Some(VtableWriteRewrite {
        statements,
        registrations,
        mutations,
    }))
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
            message: "vtable update cannot change schema_key".to_string(),
        });
    }

    let selection = update.selection.as_ref().ok_or_else(|| LixError {
        message: "vtable update requires a WHERE clause".to_string(),
    })?;

    let has_untracked_true = contains_untracked_true(selection);
    let has_untracked_false = contains_untracked_false(selection);
    if has_untracked_true && has_untracked_false {
        return Err(LixError {
            message: "vtable update cannot mix untracked predicates".to_string(),
        });
    }

    if has_untracked_true {
        if !can_strip_untracked_predicate(selection) {
            return Err(LixError {
                message: "vtable update could not strip untracked predicate".to_string(),
            });
        }
        let mut new_update = update.clone();
        replace_table_with_untracked(&mut new_update.table);
        new_update.assignments = filter_update_assignments(update.assignments);
        ensure_updated_at_assignment(&mut new_update.assignments);
        new_update.selection = try_strip_untracked_predicate(selection).unwrap_or(None);
        let validation =
            build_update_validation_plan(&new_update, Some(UNTRACKED_TABLE.to_string()), params)?;
        return Ok(Some(UpdateRewrite::Statement(VtableUpdateStatement {
            statement: Statement::Update(new_update),
            validation,
        })));
    }

    if update.from.is_some() {
        return Err(LixError {
            message: "vtable update does not support FROM".to_string(),
        });
    }

    if update.returning.is_some() {
        return Err(LixError {
            message: "vtable update does not support custom RETURNING".to_string(),
        });
    }

    let stripped_selection = if has_untracked_false {
        if !can_strip_untracked_false_predicate(selection) {
            return Err(LixError {
                message: "vtable update could not strip untracked predicate".to_string(),
            });
        }
        try_strip_untracked_false_predicate(selection).unwrap_or(None)
    } else {
        Some(selection.clone())
    };

    let stripped_selection = stripped_selection.ok_or_else(|| LixError {
        message: "vtable update requires a WHERE clause after stripping untracked".to_string(),
    })?;

    let schema_key = extract_single_schema_key(&stripped_selection)?;

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
        plan: VtableUpdatePlan { schema_key },
        validation,
    })))
}

pub fn rewrite_delete(delete: Delete) -> Result<Option<DeleteRewrite>, LixError> {
    if !delete_from_is_vtable(&delete) {
        return Ok(None);
    }

    let selection = delete.selection.as_ref().ok_or_else(|| LixError {
        message: "vtable delete requires a WHERE clause".to_string(),
    })?;

    let has_untracked_true = contains_untracked_true(selection);
    let has_untracked_false = contains_untracked_false(selection);
    if has_untracked_true && has_untracked_false {
        return Err(LixError {
            message: "vtable delete cannot mix untracked predicates".to_string(),
        });
    }

    if has_untracked_true {
        if !can_strip_untracked_predicate(selection) {
            return Err(LixError {
                message: "vtable delete could not strip untracked predicate".to_string(),
            });
        }
        let mut new_delete = delete.clone();
        replace_delete_from_untracked(&mut new_delete);
        new_delete.selection = try_strip_untracked_predicate(selection).unwrap_or(None);
        return Ok(Some(DeleteRewrite::Statement(Statement::Delete(
            new_delete,
        ))));
    }

    if delete.using.is_some() {
        return Err(LixError {
            message: "vtable delete does not support USING".to_string(),
        });
    }
    if delete.returning.is_some() {
        return Err(LixError {
            message: "vtable delete does not support custom RETURNING".to_string(),
        });
    }
    if delete.limit.is_some() || !delete.order_by.is_empty() {
        return Err(LixError {
            message: "vtable delete does not support LIMIT or ORDER BY".to_string(),
        });
    }

    let stripped_selection = if has_untracked_false {
        if !can_strip_untracked_false_predicate(selection) {
            return Err(LixError {
                message: "vtable delete could not strip untracked predicate".to_string(),
            });
        }
        try_strip_untracked_false_predicate(selection).unwrap_or(None)
    } else {
        Some(selection.clone())
    };

    let stripped_selection = stripped_selection.ok_or_else(|| LixError {
        message: "vtable delete requires a WHERE clause after stripping untracked".to_string(),
    })?;

    let schema_key = extract_single_schema_key(&stripped_selection)?;

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
                    Ident::new("snapshot_content"),
                )])),
                value: null_expr(),
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
        plan: VtableDeletePlan { schema_key },
    })))
}

pub fn build_update_followup_sql(
    plan: &VtableUpdatePlan,
    rows: &[Vec<EngineValue>],
    functions: &mut dyn LixFunctionProvider,
) -> Result<String, LixError> {
    let statements = build_update_followup_statements(plan, rows, functions)?;
    Ok(statements
        .into_iter()
        .map(|statement| statement.to_string())
        .collect::<Vec<_>>()
        .join("; "))
}

pub fn build_delete_followup_sql(
    plan: &VtableDeletePlan,
    rows: &[Vec<EngineValue>],
    functions: &mut dyn LixFunctionProvider,
) -> Result<String, LixError> {
    let statements = build_delete_followup_statements(plan, rows, functions)?;
    Ok(statements
        .into_iter()
        .map(|statement| statement.to_string())
        .collect::<Vec<_>>()
        .join("; "))
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
            Some(cell) if is_untracked_true_value(cell) => true,
            Some(cell) if is_untracked_false_value(cell) => false,
            Some(_) => {
                return Err(LixError {
                    message: "vtable insert requires literal or parameter untracked values"
                        .to_string(),
                })
            }
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

    let mut ensure_no_content = false;
    let mut snapshot_rows = Vec::new();
    let mut change_rows = Vec::new();
    let mut materialized_by_schema: std::collections::BTreeMap<String, Vec<Vec<Expr>>> =
        std::collections::BTreeMap::new();

    for (row, materialized) in rows {
        let schema_key_expr = row.get(schema_idx).ok_or_else(|| LixError {
            message: "vtable insert missing schema_key".to_string(),
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
            metadata_expr,
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
            writer_key: None,
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

    if domain_changes.is_empty() {
        return Ok(Vec::new());
    }

    let versions = load_version_info_for_versions(backend, &affected_versions).await?;
    let commit_result = generate_commit(
        GenerateCommitArgs {
            timestamp: timestamp.clone(),
            active_accounts: Vec::new(),
            changes: domain_changes,
            versions,
        },
        || functions.uuid_v7(),
    )?;

    let mut ensure_no_content = false;
    let mut snapshot_rows = Vec::new();
    let mut change_rows = Vec::new();
    let mut materialized_by_schema: BTreeMap<String, Vec<Vec<Expr>>> = BTreeMap::new();

    for change in &commit_result.changes {
        let snapshot_id = match &change.snapshot_content {
            Some(content) => {
                let id = functions.uuid_v7();
                snapshot_rows.push(vec![string_expr(&id), string_expr(content)]);
                id
            }
            None => {
                ensure_no_content = true;
                "no-content".to_string()
            }
        };

        let metadata_expr = change
            .metadata
            .as_ref()
            .map(|value| string_expr(value))
            .unwrap_or_else(null_expr);

        change_rows.push(vec![
            string_expr(&change.id),
            string_expr(&change.entity_id),
            string_expr(&change.schema_key),
            string_expr(&change.schema_version),
            string_expr(&change.file_id),
            string_expr(&change.plugin_key),
            string_expr(&snapshot_id),
            metadata_expr,
            string_expr(&change.created_at),
        ]);
    }

    for row in commit_result.materialized_state {
        ensure_registration(registrations, &row.schema_key);
        materialized_by_schema
            .entry(row.schema_key.clone())
            .or_default()
            .push(materialized_row_values(&row));
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

fn materialized_row_values(row: &MaterializedStateRow) -> Vec<Expr> {
    vec![
        string_expr(&row.entity_id),
        string_expr(&row.schema_key),
        string_expr(&row.schema_version),
        string_expr(&row.file_id),
        string_expr(&row.lixcol_version_id),
        string_expr(&row.plugin_key),
        row.snapshot_content
            .as_ref()
            .map(|value| string_expr(value))
            .unwrap_or_else(null_expr),
        string_expr(&row.id),
        number_expr("0"),
        string_expr(&row.created_at),
        string_expr(&row.created_at),
    ]
}

async fn load_version_info_for_versions(
    backend: &dyn LixBackend,
    version_ids: &BTreeSet<String>,
) -> Result<BTreeMap<String, VersionInfo>, LixError> {
    let mut versions = BTreeMap::new();

    for version_id in version_ids {
        let sql = format!(
            "SELECT snapshot_content \
             FROM {table_name} \
             WHERE schema_key = '{schema_key}' \
               AND entity_id = '{entity_id}' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL \
             LIMIT 1",
            table_name = VERSION_TIP_TABLE,
            schema_key = VERSION_TIP_SCHEMA_KEY,
            entity_id = escape_sql_string(version_id),
            global_version = GLOBAL_VERSION,
        );

        let mut info = VersionInfo {
            parent_commit_ids: Vec::new(),
            snapshot: VersionSnapshot {
                id: version_id.clone(),
                working_commit_id: version_id.clone(),
            },
        };

        match backend.execute(&sql, &[]).await {
            Ok(result) => {
                if let Some(row) = result.rows.first() {
                    if let Some(parsed) = parse_version_info_from_tip_snapshot(row, version_id)? {
                        info = parsed;
                    }
                }
            }
            Err(err) if is_missing_relation_error(&err) => {}
            Err(err) => return Err(err),
        }

        versions.insert(version_id.clone(), info);
    }

    Ok(versions)
}

fn parse_version_info_from_tip_snapshot(
    row: &[EngineValue],
    fallback_version_id: &str,
) -> Result<Option<VersionInfo>, LixError> {
    let Some(first_value) = row.first() else {
        return Ok(None);
    };
    let raw_snapshot = match first_value {
        EngineValue::Text(value) => value,
        EngineValue::Null => return Ok(None),
        _ => {
            return Err(LixError {
                message: "version tip snapshot_content must be text".to_string(),
            })
        }
    };

    let snapshot: JsonValue = serde_json::from_str(raw_snapshot).map_err(|error| LixError {
        message: format!("version tip snapshot_content invalid JSON: {error}"),
    })?;
    let version_id = snapshot
        .get("id")
        .and_then(JsonValue::as_str)
        .unwrap_or(fallback_version_id)
        .to_string();
    let working_commit_id = snapshot
        .get("working_commit_id")
        .and_then(JsonValue::as_str)
        .unwrap_or(fallback_version_id)
        .to_string();
    let parent_commit_ids = snapshot
        .get("commit_id")
        .and_then(JsonValue::as_str)
        .filter(|value| !value.is_empty())
        .map(|value| vec![value.to_string()])
        .unwrap_or_default();

    Ok(Some(VersionInfo {
        parent_commit_ids,
        snapshot: VersionSnapshot {
            id: version_id,
            working_commit_id,
        },
    }))
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
            Ident::new("schema_version"),
            Ident::new("created_at"),
            Ident::new("updated_at"),
        ],
        mapped_rows,
        Some(build_untracked_on_conflict()),
    ))
}

fn build_update_followup_statements(
    plan: &VtableUpdatePlan,
    rows: &[Vec<EngineValue>],
    functions: &mut dyn LixFunctionProvider,
) -> Result<Vec<Statement>, LixError> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut ensure_no_content = false;
    let mut snapshot_rows = Vec::new();
    let mut change_rows = Vec::new();
    let mut change_updates = Vec::new();

    for row in rows {
        if row.len() < UPDATE_RETURNING_COLUMNS.len() {
            return Err(LixError {
                message: "vtable update returning row missing columns".to_string(),
            });
        }

        let entity_id = value_to_string(&row[0], "entity_id")?;
        let file_id = value_to_string(&row[1], "file_id")?;
        let version_id = value_to_string(&row[2], "version_id")?;
        let plugin_key = value_to_string(&row[3], "plugin_key")?;
        let schema_version = value_to_string(&row[4], "schema_version")?;
        let snapshot_content_value = &row[5];
        let updated_at = value_to_string(&row[6], "updated_at")?;

        let snapshot_id = if matches!(snapshot_content_value, EngineValue::Null) {
            ensure_no_content = true;
            "no-content".to_string()
        } else {
            let id = functions.uuid_v7();
            snapshot_rows.push(vec![
                string_expr(&id),
                value_to_expr(snapshot_content_value)?,
            ]);
            id
        };

        let change_id = functions.uuid_v7();

        change_rows.push(vec![
            string_expr(&change_id),
            string_expr(&entity_id),
            string_expr(&plan.schema_key),
            string_expr(&schema_version),
            string_expr(&file_id),
            string_expr(&plugin_key),
            string_expr(&snapshot_id),
            null_expr(),
            string_expr(&updated_at),
        ]);

        change_updates.push(ChangeUpdateRow {
            entity_id,
            file_id,
            version_id,
            change_id,
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

    if !change_updates.is_empty() {
        let table_name = format!("{}{}", MATERIALIZED_PREFIX, plan.schema_key);
        statements.push(build_change_id_update(&table_name, &change_updates));
    }

    Ok(statements)
}

fn build_delete_followup_statements(
    plan: &VtableDeletePlan,
    rows: &[Vec<EngineValue>],
    functions: &mut dyn LixFunctionProvider,
) -> Result<Vec<Statement>, LixError> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut change_rows = Vec::new();
    let mut change_updates = Vec::new();

    for row in rows {
        if row.len() < UPDATE_RETURNING_COLUMNS.len() {
            return Err(LixError {
                message: "vtable delete returning row missing columns".to_string(),
            });
        }

        let entity_id = value_to_string(&row[0], "entity_id")?;
        let file_id = value_to_string(&row[1], "file_id")?;
        let version_id = value_to_string(&row[2], "version_id")?;
        let plugin_key = value_to_string(&row[3], "plugin_key")?;
        let schema_version = value_to_string(&row[4], "schema_version")?;
        let updated_at = value_to_string(&row[6], "updated_at")?;

        let change_id = functions.uuid_v7();

        change_rows.push(vec![
            string_expr(&change_id),
            string_expr(&entity_id),
            string_expr(&plan.schema_key),
            string_expr(&schema_version),
            string_expr(&file_id),
            string_expr(&plugin_key),
            string_expr("no-content"),
            null_expr(),
            string_expr(&updated_at),
        ]);

        change_updates.push(ChangeUpdateRow {
            entity_id,
            file_id,
            version_id,
            change_id,
        });
    }

    let mut statements = Vec::new();
    statements.push(make_insert_statement(
        SNAPSHOT_TABLE,
        vec![Ident::new("id"), Ident::new("content")],
        vec![vec![string_expr("no-content"), null_expr()]],
        Some(build_snapshot_on_conflict()),
    ));

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

    if !change_updates.is_empty() {
        let table_name = format!("{}{}", MATERIALIZED_PREFIX, plan.schema_key);
        statements.push(build_change_id_update(&table_name, &change_updates));
    }

    Ok(statements)
}

fn filter_update_assignments(assignments: Vec<Assignment>) -> Vec<Assignment> {
    assignments
        .into_iter()
        .filter(|assignment| !assignment_target_is_untracked(&assignment.target))
        .filter(|assignment| !assignment_target_is_column(&assignment.target, "updated_at"))
        .filter(|assignment| !assignment_target_is_column(&assignment.target, "change_id"))
        .collect()
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

fn build_change_id_update(table_name: &str, rows: &[ChangeUpdateRow]) -> Statement {
    let case_expr = Expr::Case {
        case_token: AttachedToken::empty(),
        end_token: AttachedToken::empty(),
        operand: None,
        conditions: rows
            .iter()
            .map(|row| CaseWhen {
                condition: match_key_expr(row),
                result: string_expr(&row.change_id),
            })
            .collect(),
        else_result: Some(Box::new(Expr::Identifier(Ident::new("change_id")))),
    };

    let selection = or_exprs(rows.iter().map(match_key_expr).collect());

    Statement::Update(Update {
        update_token: AttachedToken::empty(),
        table: table_with_joins_for(table_name),
        assignments: vec![Assignment {
            target: AssignmentTarget::ColumnName(ObjectName(vec![ObjectNamePart::Identifier(
                Ident::new("change_id"),
            )])),
            value: case_expr,
        }],
        from: None,
        selection: Some(selection),
        returning: None,
        or: None,
        limit: None,
    })
}

#[derive(Debug, Clone)]
struct ChangeUpdateRow {
    entity_id: String,
    file_id: String,
    version_id: String,
    change_id: String,
}

fn match_key_expr(row: &ChangeUpdateRow) -> Expr {
    and_exprs(vec![
        eq_expr("entity_id", &row.entity_id),
        eq_expr("file_id", &row.file_id),
        eq_expr("version_id", &row.version_id),
    ])
}

fn eq_expr(column: &str, value: &str) -> Expr {
    Expr::BinaryOp {
        left: Box::new(Expr::Identifier(Ident::new(column))),
        op: BinaryOperator::Eq,
        right: Box::new(string_expr(value)),
    }
}

fn and_exprs(mut exprs: Vec<Expr>) -> Expr {
    let mut iter = exprs.drain(..);
    let first = iter
        .next()
        .unwrap_or_else(|| Expr::Value(Value::Boolean(true).into()));
    iter.fold(first, |left, right| Expr::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::And,
        right: Box::new(right),
    })
}

fn or_exprs(mut exprs: Vec<Expr>) -> Expr {
    let mut iter = exprs.drain(..);
    let first = iter
        .next()
        .unwrap_or_else(|| Expr::Value(Value::Boolean(false).into()));
    iter.fold(first, |left, right| Expr::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::Or,
        right: Box::new(right),
    })
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
        EngineValue::Text(text) => Ok(string_expr(text)),
        EngineValue::Integer(value) => {
            Ok(Expr::Value(Value::Number(value.to_string(), false).into()))
        }
        EngineValue::Real(value) => Ok(Expr::Value(Value::Number(value.to_string(), false).into())),
        EngineValue::Blob(_) => Err(LixError {
            message: "vtable update does not support blob snapshot_content".to_string(),
        }),
    }
}

fn value_to_string(value: &EngineValue, name: &str) -> Result<String, LixError> {
    match value {
        EngineValue::Text(text) => Ok(text.clone()),
        _ => Err(LixError {
            message: format!("vtable update expected text for {name}"),
        }),
    }
}

fn escape_sql_string(input: &str) -> String {
    input.replace('\'', "''")
}

fn is_missing_relation_error(err: &LixError) -> bool {
    let lower = err.message.to_lowercase();
    lower.contains("no such table")
        || lower.contains("relation")
            && (lower.contains("does not exist")
                || lower.contains("undefined table")
                || lower.contains("unknown"))
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
        message: format!("vtable insert requires {name}"),
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
        message: format!("vtable insert missing {name}"),
    })?;
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(value),
            ..
        }) => Ok(value.clone()),
        _ => Err(LixError {
            message: format!("vtable insert requires literal {name}"),
        }),
    }
}

fn literal_snapshot_json(expr: Option<&Expr>) -> Result<Option<JsonValue>, LixError> {
    let expr = expr.ok_or_else(|| LixError {
        message: "vtable insert missing snapshot_content".to_string(),
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
                message: format!("vtable insert snapshot_content invalid JSON: {err}"),
            }),
        _ => Err(LixError {
            message: "vtable insert requires literal snapshot_content".to_string(),
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
                        message: format!("vtable insert snapshot_content invalid JSON: {err}"),
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
                message: format!("vtable insert {field_name} invalid JSON: {err}"),
            }),
        _ => Err(LixError {
            message: format!("vtable insert requires literal {field_name}"),
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
                        message: format!("vtable insert {field_name} invalid JSON: {err}"),
                    })
            }
            _ => {}
        }
    }

    literal_optional_json(expr, field_name)
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

fn assignment_target_is_column(target: &AssignmentTarget, column: &str) -> bool {
    match target {
        AssignmentTarget::ColumnName(name) => object_name_matches(name, column),
        AssignmentTarget::Tuple(columns) => {
            columns.iter().any(|name| object_name_matches(name, column))
        }
    }
}

fn contains_untracked_true(expr: &Expr) -> bool {
    if is_untracked_equals_true(expr) {
        return true;
    }
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And | BinaryOperator::Or => {
                contains_untracked_true(left) || contains_untracked_true(right)
            }
            _ => false,
        },
        Expr::Nested(inner) => contains_untracked_true(inner),
        _ => false,
    }
}

fn contains_untracked_false(expr: &Expr) -> bool {
    if is_untracked_equals_false(expr) {
        return true;
    }
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And | BinaryOperator::Or => {
                contains_untracked_false(left) || contains_untracked_false(right)
            }
            _ => false,
        },
        Expr::Nested(inner) => contains_untracked_false(inner),
        _ => false,
    }
}

fn can_strip_untracked_predicate(expr: &Expr) -> bool {
    contains_untracked_true(expr) && try_strip_untracked_predicate(expr).is_some()
}

fn can_strip_untracked_false_predicate(expr: &Expr) -> bool {
    contains_untracked_false(expr) && try_strip_untracked_false_predicate(expr).is_some()
}

fn try_strip_untracked_predicate(expr: &Expr) -> Option<Option<Expr>> {
    if is_untracked_equals_true(expr) {
        return Some(None);
    }

    match expr {
        Expr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
            let left = try_strip_untracked_predicate(left)?;
            let right = try_strip_untracked_predicate(right)?;

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
            let stripped = try_strip_untracked_predicate(inner)?;
            Some(stripped.map(|expr| Expr::Nested(Box::new(expr))))
        }
        _ => Some(Some(expr.clone())),
    }
}

fn try_strip_untracked_false_predicate(expr: &Expr) -> Option<Option<Expr>> {
    if is_untracked_equals_false(expr) {
        return Some(None);
    }

    match expr {
        Expr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
            let left = try_strip_untracked_false_predicate(left)?;
            let right = try_strip_untracked_false_predicate(right)?;

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
            let stripped = try_strip_untracked_false_predicate(inner)?;
            Some(stripped.map(|expr| Expr::Nested(Box::new(expr))))
        }
        _ => Some(Some(expr.clone())),
    }
}

fn is_untracked_equals_true(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            (expr_is_untracked_column(left) && is_untracked_true_literal(right))
                || (expr_is_untracked_column(right) && is_untracked_true_literal(left))
        }
        _ => false,
    }
}

fn is_untracked_equals_false(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            (expr_is_untracked_column(left) && is_untracked_false_literal(right))
                || (expr_is_untracked_column(right) && is_untracked_false_literal(left))
        }
        _ => false,
    }
}

fn expr_is_untracked_column(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case("untracked"),
        Expr::CompoundIdentifier(idents) => idents
            .last()
            .map(|ident| ident.value.eq_ignore_ascii_case("untracked"))
            .unwrap_or(false),
        _ => false,
    }
}

fn is_untracked_true_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::Number(value, _),
            ..
        }) => value == "1",
        Expr::Value(ValueWithSpan {
            value: Value::Boolean(value),
            ..
        }) => *value,
        _ => false,
    }
}

fn is_untracked_false_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::Number(value, _),
            ..
        }) => value == "0",
        Expr::Value(ValueWithSpan {
            value: Value::Boolean(value),
            ..
        }) => !*value,
        _ => false,
    }
}

fn is_untracked_true_value(cell: &ResolvedCell) -> bool {
    match cell.value.as_ref() {
        Some(EngineValue::Integer(value)) => *value == 1,
        _ => false,
    }
}

fn is_untracked_false_value(cell: &ResolvedCell) -> bool {
    match cell.value.as_ref() {
        Some(EngineValue::Integer(value)) => *value == 0,
        _ => false,
    }
}

fn build_update_validation_plan(
    update: &Update,
    table_name: Option<String>,
    params: &[EngineValue],
) -> Result<Option<UpdateValidationPlan>, LixError> {
    let snapshot_content = snapshot_content_from_assignments(&update.assignments, params)?;
    let where_clause = update.selection.as_ref().map(|expr| expr.to_string());
    let table = table_name.ok_or_else(|| LixError {
        message: "update validation requires target table".to_string(),
    })?;

    Ok(Some(UpdateValidationPlan {
        table,
        where_clause,
        snapshot_content,
    }))
}

fn snapshot_content_from_assignments(
    assignments: &[Assignment],
    params: &[EngineValue],
) -> Result<Option<JsonValue>, LixError> {
    let mut state = PlaceholderState::new();
    for assignment in assignments {
        let value = resolve_expr_cell_with_state(&assignment.value, params, &mut state)?;
        if assignment_target_is_column(&assignment.target, "snapshot_content") {
            return resolved_snapshot_json_value(value.value);
        }
    }
    Ok(None)
}

fn resolved_snapshot_json_value(value: Option<EngineValue>) -> Result<Option<JsonValue>, LixError> {
    match value {
        Some(EngineValue::Null) => Ok(None),
        Some(EngineValue::Text(value)) => serde_json::from_str::<JsonValue>(&value)
            .map(Some)
            .map_err(|err| LixError {
                message: format!("vtable update snapshot_content invalid JSON: {err}"),
            }),
        _ => Err(LixError {
            message: "vtable update requires literal snapshot_content".to_string(),
        }),
    }
}

fn object_name_matches(name: &ObjectName, target: &str) -> bool {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .map(|ident| ident.value.eq_ignore_ascii_case(target))
        .unwrap_or(false)
}

fn extract_single_schema_key(expr: &Expr) -> Result<String, LixError> {
    let keys = extract_schema_keys_from_expr(expr).ok_or_else(|| LixError {
        message: "vtable update requires schema_key predicate".to_string(),
    })?;
    if keys.len() != 1 {
        return Err(LixError {
            message: "vtable update requires a single schema_key".to_string(),
        });
    }
    Ok(keys[0].clone())
}

fn extract_schema_keys_from_expr(expr: &Expr) -> Option<Vec<String>> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if expr_is_schema_key_column(left) {
                return string_literal_value(right).map(|value| vec![value]);
            }
            if expr_is_schema_key_column(right) {
                return string_literal_value(left).map(|value| vec![value]);
            }
            None
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => match (
            extract_schema_keys_from_expr(left),
            extract_schema_keys_from_expr(right),
        ) {
            (Some(left), Some(right)) => {
                let intersection = intersect_strings(&left, &right);
                if intersection.is_empty() {
                    None
                } else {
                    Some(intersection)
                }
            }
            (Some(keys), None) | (None, Some(keys)) => Some(keys),
            (None, None) => None,
        },
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => match (
            extract_schema_keys_from_expr(left),
            extract_schema_keys_from_expr(right),
        ) {
            (Some(left), Some(right)) => Some(union_strings(&left, &right)),
            _ => None,
        },
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            if !expr_is_schema_key_column(expr) {
                return None;
            }
            let mut values = Vec::with_capacity(list.len());
            for item in list {
                let value = string_literal_value(item)?;
                values.push(value);
            }
            if values.is_empty() {
                None
            } else {
                Some(dedup_strings(values))
            }
        }
        Expr::Nested(inner) => extract_schema_keys_from_expr(inner),
        _ => None,
    }
}

fn expr_is_schema_key_column(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case("schema_key"),
        Expr::CompoundIdentifier(idents) => idents
            .last()
            .map(|ident| ident.value.eq_ignore_ascii_case("schema_key"))
            .unwrap_or(false),
        _ => false,
    }
}

fn string_literal_value(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(value),
            ..
        }) => Some(value.clone()),
        _ => None,
    }
}

fn dedup_strings(values: Vec<String>) -> Vec<String> {
    let mut out = Vec::new();
    for value in values {
        if !out.contains(&value) {
            out.push(value);
        }
    }
    out
}

fn union_strings(left: &[String], right: &[String]) -> Vec<String> {
    let mut out = left.to_vec();
    for value in right {
        if !out.contains(value) {
            out.push(value.clone());
        }
    }
    out
}

fn intersect_strings(left: &[String], right: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for value in left {
        if right.contains(value) && !out.contains(value) {
            out.push(value.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{
        rewrite_delete, rewrite_insert, rewrite_update, DeleteRewrite, UpdateRewrite,
        UPDATE_RETURNING_COLUMNS,
    };
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
            VALUES ('entity-1', 'test_schema', 'file-1', 'version-1', 'lix', '{"key":"value"}', '1', 1)"#;
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
    fn rewrite_tracked_delete_updates_materialized_with_returning() {
        let sql = r#"DELETE FROM lix_internal_state_vtable
            WHERE schema_key = 'test_schema' AND entity_id = 'entity-1'"#;
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse sql");
        let statement = statements.remove(0);

        let delete = match statement {
            Statement::Delete(delete) => delete,
            _ => panic!("expected delete"),
        };

        let rewrite = rewrite_delete(delete)
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
        assert!(assignments.iter().any(|name| name == "snapshot_content"));
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
        assert!(err.message.contains("schema_key"), "{:#?}", err);
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
        assert!(err.message.contains("single schema_key"), "{:#?}", err);
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
            err.message.contains("cannot change schema_key"),
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

        let err = rewrite_delete(delete).expect_err("expected error");
        assert!(err.message.contains("schema_key"), "{:#?}", err);
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

        let err = rewrite_delete(delete).expect_err("expected error");
        assert!(err.message.contains("single schema_key"), "{:#?}", err);
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
