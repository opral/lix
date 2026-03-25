use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    ConflictTarget, DoUpdate, Expr, Ident, ObjectName, ObjectNamePart, OnConflict,
    OnConflictAction, OnInsert, Query, SetExpr, Statement, TableObject, Value as SqlValue, Values,
};
use std::collections::{BTreeMap, BTreeSet};

use crate::account::{
    active_account_file_id, active_account_schema_key, active_account_storage_version_id,
};
use crate::backend::prepared::{PreparedBatch, PreparedStatement};
use crate::functions::LixFunctionProvider;
use crate::schema::builtin::{builtin_schema_definition, decode_lixcol_literal};
use crate::schema::live_layout::load_live_table_layout_with_executor;
use crate::state::internal::quote_ident;
use crate::sql_support::binding::bind_statement_ast;
use crate::sql_support::text::escape_sql_string;

use crate::schema::live_layout::{
    builtin_live_table_layout, normalized_live_column_values, tracked_live_table_name,
    untracked_live_table_name,
};
use crate::{LixError, SqlDialect, Value as EngineValue};

use super::generate_commit::generate_commit;
use super::graph_index::{
    resolve_commit_graph_node_write_rows_with_executor, CommitGraphNodeWriteRow,
    COMMIT_GRAPH_NODE_TABLE,
};
use super::state_source::{load_version_info_for_versions, CommitQueryExecutor};
use super::types::{
    CanonicalCommitOutput, DerivedCommitApplyInput, DomainChangeInput, GenerateCommitArgs,
    GenerateCommitResult, MaterializedStateRow,
};

const SNAPSHOT_TABLE: &str = "lix_internal_snapshot";
const CHANGE_TABLE: &str = "lix_internal_change";
const CHANGE_AUTHOR_SCHEMA_KEY: &str = "lix_change_author";
const GLOBAL_VERSION: &str = "global";
const SQLITE_MAX_BIND_PARAMETERS_PER_STATEMENT: usize = 32_766;
const POSTGRES_MAX_BIND_PARAMETERS_PER_STATEMENT: usize = 65_535;
const SNAPSHOT_INSERT_PARAM_COLUMNS: usize = 2;
const CHANGE_INSERT_PARAM_COLUMNS: usize = 9;

#[derive(Debug, Clone)]
struct SnapshotInsertRow {
    id: String,
    content: String,
}

#[derive(Debug, Clone)]
struct CanonicalChangeInsertRow {
    id: String,
    entity_id: String,
    schema_key: String,
    schema_version: String,
    file_id: String,
    plugin_key: String,
    snapshot_id: String,
    metadata: Option<String>,
    created_at: String,
}

#[derive(Debug)]
struct PreparedLiveStateInsertRow<'a> {
    row: &'a MaterializedStateRow,
    normalized_columns: Vec<(String, EngineValue)>,
}

pub(crate) async fn load_commit_active_accounts(
    executor: &mut dyn CommitQueryExecutor,
    domain_changes: &[DomainChangeInput],
) -> Result<Vec<String>, LixError> {
    if domain_changes.is_empty() {
        return Ok(Vec::new());
    }

    if domain_changes
        .iter()
        .all(|change| change.schema_key == CHANGE_AUTHOR_SCHEMA_KEY)
    {
        return Ok(Vec::new());
    }

    let sql = format!(
        "SELECT account_id \
         FROM {table_name} \
         WHERE file_id = '{file_id}' \
           AND version_id = '{version_id}' \
           AND untracked = true \
           AND account_id IS NOT NULL",
        table_name = quote_ident(&untracked_live_table_name(active_account_schema_key())),
        file_id = escape_sql_string(active_account_file_id()),
        version_id = escape_sql_string(active_account_storage_version_id()),
    );
    let result = executor.execute(&sql, &[]).await?;

    let mut deduped = BTreeSet::new();
    for row in result.rows {
        let Some(value) = row.first() else {
            continue;
        };
        let account_id = match value {
            EngineValue::Text(text) => text.clone(),
            EngineValue::Null => continue,
            _ => {
                return Err(LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: "active account id must be text".to_string(),
                });
            }
        };
        deduped.insert(account_id);
    }

    Ok(deduped.into_iter().collect())
}

pub(crate) async fn build_prepared_batch_from_generate_commit_result_with_executor(
    executor: &mut dyn CommitQueryExecutor,
    mut commit_result: GenerateCommitResult,
    functions: &mut dyn LixFunctionProvider,
) -> Result<PreparedBatch, LixError> {
    commit_result.derived_apply_input.live_layouts = load_live_layouts_for_rows_with_executor(
        executor,
        &commit_result.derived_apply_input.live_state_rows,
    )
    .await?;
    let commit_graph_rows = resolve_commit_graph_node_write_rows_with_executor(
        executor,
        &commit_result.derived_apply_input.live_state_rows,
    )
    .await?;
    build_prepared_batch_from_commit_apply_input(
        &commit_result.canonical_output,
        &commit_result.derived_apply_input,
        &commit_graph_rows,
        functions,
        executor.dialect(),
    )
}

pub(crate) async fn build_prepared_batch_from_domain_changes_with_executor(
    executor: &mut dyn CommitQueryExecutor,
    timestamp: String,
    domain_changes: Vec<DomainChangeInput>,
    affected_versions: &BTreeSet<String>,
    functions: &mut dyn LixFunctionProvider,
) -> Result<PreparedBatch, LixError> {
    if domain_changes.is_empty() {
        return Ok(PreparedBatch { steps: Vec::new() });
    }

    let commit_result = generate_commit_result_from_domain_changes_with_executor(
        executor,
        timestamp,
        domain_changes,
        affected_versions,
        functions,
    )
    .await?;
    build_prepared_batch_from_generate_commit_result_with_executor(executor, commit_result, functions)
        .await
}

pub(crate) async fn generate_commit_result_from_domain_changes_with_executor(
    executor: &mut dyn CommitQueryExecutor,
    timestamp: String,
    domain_changes: Vec<DomainChangeInput>,
    affected_versions: &BTreeSet<String>,
    functions: &mut dyn LixFunctionProvider,
) -> Result<GenerateCommitResult, LixError> {
    let versions = load_version_info_for_versions(executor, affected_versions).await?;
    let active_accounts = load_commit_active_accounts(executor, &domain_changes).await?;
    generate_commit(
        GenerateCommitArgs {
            timestamp,
            active_accounts,
            changes: domain_changes,
            versions,
            force_commit_versions: BTreeSet::new(),
        },
        || functions.uuid_v7(),
    )
}

pub(crate) fn build_prepared_batch_from_commit_apply_input<'a>(
    canonical_output: &CanonicalCommitOutput,
    derived_apply_input: &'a DerivedCommitApplyInput,
    commit_graph_rows: &[CommitGraphNodeWriteRow],
    functions: &mut dyn LixFunctionProvider,
    dialect: SqlDialect,
) -> Result<PreparedBatch, LixError> {
    let mut ensure_no_content = false;
    let mut snapshot_rows = Vec::new();
    let mut change_rows = Vec::new();
    let mut materialized_by_schema: BTreeMap<
        String,
        (bool, Vec<Ident>, Vec<PreparedLiveStateInsertRow<'a>>, usize),
    > = BTreeMap::new();

    for change in &canonical_output.changes {
        let snapshot_id = match &change.snapshot_content {
            Some(content) => {
                let id = functions.uuid_v7();
                snapshot_rows.push(SnapshotInsertRow {
                    id: id.clone(),
                    content: content.as_str().to_string(),
                });
                id
            }
            None => {
                ensure_no_content = true;
                "no-content".to_string()
            }
        };

        change_rows.push(CanonicalChangeInsertRow {
            id: change.id.clone(),
            entity_id: change.entity_id.to_string(),
            schema_key: change.schema_key.to_string(),
            schema_version: change.schema_version.to_string(),
            file_id: change.file_id.to_string(),
            plugin_key: change.plugin_key.to_string(),
            snapshot_id,
            metadata: change
                .metadata
                .as_ref()
                .map(|value| value.as_str().to_string()),
            created_at: change.created_at.clone(),
        });
    }

    for row in &derived_apply_input.live_state_rows {
        let is_untracked = schema_uses_untracked_live_state(&row.schema_key);
        let layout = derived_apply_input
            .live_layouts
            .get(row.schema_key.as_str())
            .cloned()
            .map(Some)
            .unwrap_or(builtin_live_table_layout(&row.schema_key)?);
        let normalized_values = normalized_live_column_values_for_row(
            layout.as_ref(),
            row.snapshot_content.as_deref(),
        )?;
        let columns = live_state_insert_columns(layout.as_ref(), is_untracked);
        let params_per_row = columns.len();
        let entry = materialized_by_schema
            .entry(row.schema_key.to_string())
            .or_insert_with(|| (is_untracked, columns.clone(), Vec::new(), params_per_row));
        entry.2.push(PreparedLiveStateInsertRow {
            row,
            normalized_columns: normalized_values,
        });
    }

    let mut prepared = PreparedBatch { steps: Vec::new() };

    if ensure_no_content {
        push_prepared_statement(
            &mut prepared,
            make_insert_statement(
                SNAPSHOT_TABLE,
                vec![Ident::new("id"), Ident::new("content")],
                vec![vec![string_expr("no-content"), null_expr()]],
                Some(build_snapshot_on_conflict()),
            ),
            Vec::new(),
            dialect,
        )?;
    }

    push_chunked_prepared_insert_statements(
        &mut prepared,
        SNAPSHOT_TABLE,
        vec![Ident::new("id"), Ident::new("content")],
        &snapshot_rows,
        Some(build_snapshot_on_conflict()),
        max_rows_per_insert_for_dialect(dialect, SNAPSHOT_INSERT_PARAM_COLUMNS),
        dialect,
        |row, next_placeholder, params| {
            vec![
                text_param_expr(&row.id, next_placeholder, params),
                text_param_expr(&row.content, next_placeholder, params),
            ]
        },
    )?;

    push_chunked_prepared_insert_statements(
        &mut prepared,
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
        &change_rows,
        None,
        max_rows_per_insert_for_dialect(dialect, CHANGE_INSERT_PARAM_COLUMNS),
        dialect,
        |row, next_placeholder, params| {
            vec![
                text_param_expr(&row.id, next_placeholder, params),
                text_param_expr(&row.entity_id, next_placeholder, params),
                text_param_expr(&row.schema_key, next_placeholder, params),
                text_param_expr(&row.schema_version, next_placeholder, params),
                text_param_expr(&row.file_id, next_placeholder, params),
                text_param_expr(&row.plugin_key, next_placeholder, params),
                text_param_expr(&row.snapshot_id, next_placeholder, params),
                optional_text_param_expr(row.metadata.as_deref(), next_placeholder, params),
                text_param_expr(&row.created_at, next_placeholder, params),
            ]
        },
    )?;

    for (schema_key, (is_untracked, columns, rows, params_per_row)) in materialized_by_schema {
        let table_name = tracked_live_table_name(&schema_key);
        push_chunked_prepared_insert_statements(
            &mut prepared,
            &table_name,
            columns.clone(),
            &rows,
            Some(build_live_state_on_conflict(&columns, is_untracked)),
            max_rows_per_insert_for_dialect(dialect, params_per_row),
            dialect,
            |row, next_placeholder, params| {
                live_state_row_values_parameterized(
                    row.row,
                    None,
                    is_untracked,
                    &row.normalized_columns,
                    next_placeholder,
                    params,
                )
            },
        )?;
    }

    for row in commit_graph_rows {
        prepared.push_statement(build_commit_graph_node_prepared_statement(row, dialect)?);
    }

    Ok(prepared)
}

fn max_bind_parameters_for_dialect(dialect: SqlDialect) -> usize {
    match dialect {
        SqlDialect::Sqlite => SQLITE_MAX_BIND_PARAMETERS_PER_STATEMENT,
        SqlDialect::Postgres => POSTGRES_MAX_BIND_PARAMETERS_PER_STATEMENT,
    }
}

async fn load_live_layouts_for_rows_with_executor(
    executor: &mut dyn CommitQueryExecutor,
    rows: &[MaterializedStateRow],
) -> Result<BTreeMap<String, crate::schema::live_layout::LiveTableLayout>, LixError> {
    let mut layouts = BTreeMap::new();
    let schema_keys = rows
        .iter()
        .map(|row| row.schema_key.clone())
        .collect::<BTreeSet<_>>();
    for schema_key in schema_keys {
        if let Some(layout) = builtin_live_table_layout(&schema_key)? {
            layouts.insert(schema_key.to_string(), layout);
            continue;
        }
        layouts.insert(
            schema_key.to_string(),
            load_live_table_layout_with_executor(executor, &schema_key).await?,
        );
    }
    Ok(layouts)
}

fn schema_uses_untracked_live_state(schema_key: &str) -> bool {
    builtin_schema_definition(schema_key)
        .and_then(|schema| {
            schema
                .get("x-lix-override-lixcols")
                .and_then(|value| value.get("lixcol_untracked"))
                .and_then(|value| value.as_str())
        })
        .map(decode_lixcol_literal)
        .is_some_and(|value| value == "true")
}

fn max_rows_per_insert_for_dialect(dialect: SqlDialect, params_per_row: usize) -> usize {
    (max_bind_parameters_for_dialect(dialect) / params_per_row).max(1)
}

fn push_chunked_prepared_insert_statements<Row, F>(
    prepared: &mut PreparedBatch,
    table: &str,
    columns: Vec<Ident>,
    rows: &[Row],
    on: Option<OnInsert>,
    max_rows_per_statement: usize,
    dialect: SqlDialect,
    mut build_row: F,
) -> Result<(), LixError>
where
    F: FnMut(&Row, &mut usize, &mut Vec<EngineValue>) -> Vec<Expr>,
{
    if rows.is_empty() {
        return Ok(());
    }

    for chunk in rows.chunks(max_rows_per_statement.max(1)) {
        let mut params = Vec::new();
        let mut next_placeholder = 1;
        let mut chunk_rows = Vec::with_capacity(chunk.len());
        for row in chunk {
            chunk_rows.push(build_row(row, &mut next_placeholder, &mut params));
        }
        push_prepared_statement(
            prepared,
            make_insert_statement(table, columns.clone(), chunk_rows, on.clone()),
            params,
            dialect,
        )?;
    }

    Ok(())
}

fn push_prepared_statement(
    prepared: &mut PreparedBatch,
    statement: Statement,
    params: Vec<EngineValue>,
    dialect: SqlDialect,
) -> Result<(), LixError> {
    let bound = bind_statement_ast(&statement, &params, dialect)?;
    prepared.push_statement(PreparedStatement {
        sql: bound.sql,
        params: bound.params,
    });
    Ok(())
}

fn build_commit_graph_node_prepared_statement(
    row: &CommitGraphNodeWriteRow,
    dialect: SqlDialect,
) -> Result<PreparedStatement, LixError> {
    let statement = make_insert_statement(
        COMMIT_GRAPH_NODE_TABLE,
        vec![Ident::new("commit_id"), Ident::new("generation")],
        vec![vec![placeholder_expr(1), placeholder_expr(2)]],
        Some(build_commit_graph_node_on_conflict()),
    );
    let params = vec![
        EngineValue::Text(row.commit_id.clone()),
        EngineValue::Integer(row.generation),
    ];
    let bound = bind_statement_ast(&statement, &params, dialect)?;
    Ok(PreparedStatement {
        sql: bound.sql,
        params: bound.params,
    })
}

fn text_param_expr(
    value: &str,
    next_placeholder: &mut usize,
    params: &mut Vec<EngineValue>,
) -> Expr {
    let index = *next_placeholder;
    *next_placeholder += 1;
    params.push(EngineValue::Text(value.to_string()));
    placeholder_expr(index)
}

fn optional_text_param_expr(
    value: Option<&str>,
    next_placeholder: &mut usize,
    params: &mut Vec<EngineValue>,
) -> Expr {
    match value {
        Some(value) => text_param_expr(value, next_placeholder, params),
        None => null_expr(),
    }
}

fn normalized_live_column_values_for_row(
    layout: Option<&crate::schema::live_layout::LiveTableLayout>,
    snapshot_content: Option<&str>,
) -> Result<Vec<(String, EngineValue)>, LixError> {
    let Some(layout) = layout else {
        return Ok(Vec::new());
    };
    Ok(normalized_live_column_values(layout, snapshot_content)?
        .into_iter()
        .map(|(column_name, value)| (column_name, value_to_engine_value(value)))
        .collect())
}

fn live_state_insert_columns(
    layout: Option<&crate::schema::live_layout::LiveTableLayout>,
    _untracked: bool,
) -> Vec<Ident> {
    let mut columns = vec![
        Ident::new("entity_id"),
        Ident::new("schema_key"),
        Ident::new("schema_version"),
        Ident::new("file_id"),
        Ident::new("version_id"),
        Ident::new("global"),
        Ident::new("plugin_key"),
        Ident::new("change_id"),
        Ident::new("metadata"),
        Ident::new("writer_key"),
        Ident::new("is_tombstone"),
        Ident::new("untracked"),
        Ident::new("created_at"),
        Ident::new("updated_at"),
    ];
    if let Some(layout) = layout {
        columns.extend(
            layout
                .columns
                .iter()
                .map(|column| Ident::new(&column.column_name)),
        );
    }
    columns
}

fn live_state_row_values_parameterized(
    row: &MaterializedStateRow,
    _layout: Option<&crate::schema::live_layout::LiveTableLayout>,
    untracked: bool,
    normalized_columns: &[(String, EngineValue)],
    next_placeholder: &mut usize,
    params: &mut Vec<EngineValue>,
) -> Vec<Expr> {
    let mut values = vec![
        text_param_expr(&row.entity_id, next_placeholder, params),
        text_param_expr(&row.schema_key, next_placeholder, params),
        text_param_expr(&row.schema_version, next_placeholder, params),
        text_param_expr(&row.file_id, next_placeholder, params),
        text_param_expr(&row.lixcol_version_id, next_placeholder, params),
        boolean_expr(row.lixcol_version_id == GLOBAL_VERSION),
        text_param_expr(&row.plugin_key, next_placeholder, params),
        if untracked {
            null_expr()
        } else {
            text_param_expr(&row.id, next_placeholder, params)
        },
        optional_text_param_expr(
            row.metadata.as_ref().map(|value| value.as_str()),
            next_placeholder,
            params,
        ),
        optional_text_param_expr(row.writer_key.as_deref(), next_placeholder, params),
        number_expr(if !untracked && row.snapshot_content.is_none() {
            "1"
        } else {
            "0"
        }),
        boolean_expr(untracked),
        text_param_expr(&row.created_at, next_placeholder, params),
        text_param_expr(&row.created_at, next_placeholder, params),
    ];
    for (_, value) in normalized_columns {
        values.push(value_param_expr(value, next_placeholder, params));
    }
    values
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

fn build_snapshot_on_conflict() -> OnInsert {
    OnInsert::OnConflict(OnConflict {
        conflict_target: Some(ConflictTarget::Columns(vec![Ident::new("id")])),
        action: OnConflictAction::DoNothing,
    })
}

fn build_commit_graph_node_on_conflict() -> OnInsert {
    OnInsert::OnConflict(OnConflict {
        conflict_target: Some(ConflictTarget::Columns(vec![Ident::new("commit_id")])),
        action: OnConflictAction::DoUpdate(DoUpdate {
            assignments: vec![sqlparser::ast::Assignment {
                target: sqlparser::ast::AssignmentTarget::ColumnName(ObjectName(vec![
                    ObjectNamePart::Identifier(Ident::new("generation")),
                ])),
                value: Expr::Case {
                    case_token: AttachedToken::empty(),
                    end_token: AttachedToken::empty(),
                    operand: None,
                    conditions: vec![sqlparser::ast::CaseWhen {
                        condition: Expr::BinaryOp {
                            left: Box::new(Expr::CompoundIdentifier(vec![
                                Ident::new("excluded"),
                                Ident::new("generation"),
                            ])),
                            op: sqlparser::ast::BinaryOperator::Gt,
                            right: Box::new(Expr::CompoundIdentifier(vec![
                                Ident::new(COMMIT_GRAPH_NODE_TABLE),
                                Ident::new("generation"),
                            ])),
                        },
                        result: Expr::CompoundIdentifier(vec![
                            Ident::new("excluded"),
                            Ident::new("generation"),
                        ]),
                    }],
                    else_result: Some(Box::new(Expr::CompoundIdentifier(vec![
                        Ident::new(COMMIT_GRAPH_NODE_TABLE),
                        Ident::new("generation"),
                    ]))),
                },
            }],
            selection: None,
        }),
    })
}

fn build_live_state_on_conflict(columns: &[Ident], _untracked: bool) -> OnInsert {
    let mut assignments = vec![
        sqlparser::ast::Assignment {
            target: sqlparser::ast::AssignmentTarget::ColumnName(ObjectName(vec![
                ObjectNamePart::Identifier(Ident::new("global")),
            ])),
            value: Expr::Identifier(Ident::new("excluded.global")),
        },
        sqlparser::ast::Assignment {
            target: sqlparser::ast::AssignmentTarget::ColumnName(ObjectName(vec![
                ObjectNamePart::Identifier(Ident::new("schema_version")),
            ])),
            value: Expr::Identifier(Ident::new("excluded.schema_version")),
        },
        sqlparser::ast::Assignment {
            target: sqlparser::ast::AssignmentTarget::ColumnName(ObjectName(vec![
                ObjectNamePart::Identifier(Ident::new("plugin_key")),
            ])),
            value: Expr::Identifier(Ident::new("excluded.plugin_key")),
        },
        sqlparser::ast::Assignment {
            target: sqlparser::ast::AssignmentTarget::ColumnName(ObjectName(vec![
                ObjectNamePart::Identifier(Ident::new("metadata")),
            ])),
            value: Expr::Identifier(Ident::new("excluded.metadata")),
        },
        sqlparser::ast::Assignment {
            target: sqlparser::ast::AssignmentTarget::ColumnName(ObjectName(vec![
                ObjectNamePart::Identifier(Ident::new("writer_key")),
            ])),
            value: Expr::Identifier(Ident::new("excluded.writer_key")),
        },
        sqlparser::ast::Assignment {
            target: sqlparser::ast::AssignmentTarget::ColumnName(ObjectName(vec![
                ObjectNamePart::Identifier(Ident::new("updated_at")),
            ])),
            value: Expr::Identifier(Ident::new("excluded.updated_at")),
        },
        sqlparser::ast::Assignment {
            target: sqlparser::ast::AssignmentTarget::ColumnName(ObjectName(vec![
                ObjectNamePart::Identifier(Ident::new("change_id")),
            ])),
            value: Expr::Identifier(Ident::new("excluded.change_id")),
        },
        sqlparser::ast::Assignment {
            target: sqlparser::ast::AssignmentTarget::ColumnName(ObjectName(vec![
                ObjectNamePart::Identifier(Ident::new("is_tombstone")),
            ])),
            value: Expr::Identifier(Ident::new("excluded.is_tombstone")),
        },
        sqlparser::ast::Assignment {
            target: sqlparser::ast::AssignmentTarget::ColumnName(ObjectName(vec![
                ObjectNamePart::Identifier(Ident::new("untracked")),
            ])),
            value: Expr::Identifier(Ident::new("excluded.untracked")),
        },
    ];
    for column in columns {
        let name = column.value.as_str();
        if matches!(
            name,
            "entity_id"
                | "schema_key"
                | "schema_version"
                | "file_id"
                | "version_id"
                | "global"
                | "plugin_key"
                | "change_id"
                | "metadata"
                | "writer_key"
                | "is_tombstone"
                | "untracked"
                | "created_at"
                | "updated_at"
        ) {
            continue;
        }
        assignments.push(sqlparser::ast::Assignment {
            target: sqlparser::ast::AssignmentTarget::ColumnName(ObjectName(vec![
                ObjectNamePart::Identifier(Ident::new(name)),
            ])),
            value: Expr::Identifier(Ident::new(&format!("excluded.{name}"))),
        });
    }
    OnInsert::OnConflict(OnConflict {
        conflict_target: Some(ConflictTarget::Columns(vec![
            Ident::new("entity_id"),
            Ident::new("file_id"),
            Ident::new("version_id"),
            Ident::new("untracked"),
        ])),
        action: OnConflictAction::DoUpdate(DoUpdate {
            assignments,
            selection: None,
        }),
    })
}

fn value_param_expr(
    value: &EngineValue,
    next_placeholder: &mut usize,
    params: &mut Vec<EngineValue>,
) -> Expr {
    let index = *next_placeholder;
    *next_placeholder += 1;
    params.push(value.clone());
    placeholder_expr(index)
}

fn value_to_engine_value(value: crate::Value) -> EngineValue {
    match value {
        crate::Value::Null => EngineValue::Null,
        crate::Value::Boolean(value) => EngineValue::Boolean(value),
        crate::Value::Integer(value) => EngineValue::Integer(value),
        crate::Value::Real(value) => EngineValue::Real(value),
        crate::Value::Text(value) => EngineValue::Text(value),
        crate::Value::Json(value) => EngineValue::Json(value),
        crate::Value::Blob(value) => EngineValue::Blob(value),
    }
}

fn string_expr(value: &str) -> Expr {
    Expr::Value(SqlValue::SingleQuotedString(value.to_string()).into())
}

fn placeholder_expr(index_1_based: usize) -> Expr {
    Expr::Value(SqlValue::Placeholder(format!("?{index_1_based}")).into())
}

fn number_expr(value: &str) -> Expr {
    Expr::Value(SqlValue::Number(value.to_string(), false).into())
}

fn boolean_expr(value: bool) -> Expr {
    Expr::Value(SqlValue::Boolean(value).into())
}

fn null_expr() -> Expr {
    Expr::Value(SqlValue::Null.into())
}
