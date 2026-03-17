use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;

use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    ConflictTarget, DoUpdate, Expr, Ident, ObjectName, ObjectNamePart, OnConflict,
    OnConflictAction, OnInsert, Query, SetExpr, Statement, TableObject, Value as SqlValue, Values,
};
use sqlparser::ast::{VisitMut, VisitorMut};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::account::{
    active_account_file_id, active_account_schema_key, active_account_storage_version_id,
};
use crate::functions::LixFunctionProvider;
use crate::state::internal::quote_ident;

use crate::schema::live_layout::{
    builtin_live_table_layout, normalized_live_column_values, tracked_live_table_name,
    untracked_live_table_name,
};
use crate::sql::ast::utils::{
    bind_sql, parse_sql_statements, resolve_placeholder_index, PlaceholderState,
};
use crate::sql::execution::contracts::prepared_statement::{PreparedBatch, PreparedStatement};
use crate::sql::storage::sql_text::escape_sql_string;
use crate::{LixError, SqlDialect, Value as EngineValue};

use super::graph_index::append_commit_graph_node_statements;
use super::state_source::CommitQueryExecutor;
use super::types::{
    CanonicalCommitOutput, DerivedCommitApplyInput, DomainChangeInput, GenerateCommitResult,
    MaterializedStateRow,
};

const SNAPSHOT_TABLE: &str = "lix_internal_snapshot";
const CHANGE_TABLE: &str = "lix_internal_change";
const CHANGE_AUTHOR_SCHEMA_KEY: &str = "lix_change_author";
const GLOBAL_VERSION: &str = "global";
const SQLITE_MAX_BIND_PARAMETERS_PER_STATEMENT: usize = 32_766;
const POSTGRES_MAX_BIND_PARAMETERS_PER_STATEMENT: usize = 65_535;
const SNAPSHOT_INSERT_PARAM_COLUMNS: usize = 2;
const CHANGE_INSERT_PARAM_COLUMNS: usize = 9;
#[derive(Debug)]
pub(crate) struct StatementBatch {
    pub(crate) statements: Vec<Statement>,
    pub(crate) params: Vec<EngineValue>,
}

pub(crate) fn bind_statement_batch_for_dialect(
    batch: StatementBatch,
    dialect: SqlDialect,
) -> Result<PreparedBatch, LixError> {
    let mut prepared = Vec::with_capacity(batch.statements.len());
    for statement in batch.statements {
        prepared.push(PreparedStatement {
            sql: bind_statement_for_batch(statement, &batch.params, dialect)?,
            params: Vec::new(),
        });
    }

    Ok(PreparedBatch { steps: prepared })
}

fn bind_statement_for_batch(
    statement: Statement,
    params: &[EngineValue],
    dialect: SqlDialect,
) -> Result<String, LixError> {
    let bound = bind_sql(&statement.to_string(), params, dialect)?;
    inline_bound_statement(&bound.sql, &bound.params, dialect)
}

fn inline_bound_statement(
    sql: &str,
    params: &[EngineValue],
    dialect: SqlDialect,
) -> Result<String, LixError> {
    let mut statements = parse_sql_statements(sql)?;
    let mut state = PlaceholderState::new();

    for statement in &mut statements {
        let mut visitor = PlaceholderLiteralInliner {
            params,
            dialect,
            state: &mut state,
        };
        if let ControlFlow::Break(error) = statement.visit(&mut visitor) {
            return Err(error);
        }
    }

    Ok(statements
        .into_iter()
        .map(|statement| statement.to_string())
        .collect::<Vec<_>>()
        .join("; "))
}

struct PlaceholderLiteralInliner<'a> {
    params: &'a [EngineValue],
    dialect: SqlDialect,
    state: &'a mut PlaceholderState,
}

impl VisitorMut for PlaceholderLiteralInliner<'_> {
    type Break = LixError;

    fn pre_visit_value(&mut self, value: &mut SqlValue) -> ControlFlow<Self::Break> {
        let SqlValue::Placeholder(token) = value else {
            return ControlFlow::Continue(());
        };

        let source_index = match resolve_placeholder_index(token, self.params.len(), self.state) {
            Ok(index) => index,
            Err(error) => return ControlFlow::Break(error),
        };

        *value = match engine_value_to_sql_literal(&self.params[source_index], self.dialect) {
            Ok(value) => value,
            Err(error) => return ControlFlow::Break(error),
        };

        ControlFlow::Continue(())
    }
}

fn engine_value_to_sql_literal(
    value: &EngineValue,
    dialect: SqlDialect,
) -> Result<SqlValue, LixError> {
    match value {
        EngineValue::Null => Ok(SqlValue::Null),
        EngineValue::Boolean(value) => Ok(SqlValue::Boolean(*value)),
        EngineValue::Integer(value) => Ok(SqlValue::Number(value.to_string(), false)),
        EngineValue::Real(value) => Ok(SqlValue::Number(value.to_string(), false)),
        EngineValue::Text(value) => Ok(SqlValue::SingleQuotedString(value.clone())),
        EngineValue::Json(value) => Ok(SqlValue::SingleQuotedString(value.to_string())),
        EngineValue::Blob(value) => match dialect {
            SqlDialect::Sqlite => Ok(SqlValue::HexStringLiteral(encode_hex_upper(value))),
            SqlDialect::Postgres => Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "postgres batch literal inlining does not support blob parameters",
            )),
        },
    }
}

fn encode_hex_upper(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0F) as usize] as char);
    }
    out
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

pub(crate) fn build_statement_batch_from_generate_commit_result(
    commit_result: GenerateCommitResult,
    functions: &mut dyn LixFunctionProvider,
    placeholder_offset: usize,
    dialect: SqlDialect,
) -> Result<StatementBatch, LixError> {
    build_statement_batch_from_commit_apply_input(
        &commit_result.canonical_output,
        &commit_result.derived_apply_input,
        functions,
        placeholder_offset,
        dialect,
    )
}

pub(crate) fn build_statement_batch_from_commit_apply_input(
    canonical_output: &CanonicalCommitOutput,
    derived_apply_input: &DerivedCommitApplyInput,
    functions: &mut dyn LixFunctionProvider,
    placeholder_offset: usize,
    dialect: SqlDialect,
) -> Result<StatementBatch, LixError> {
    let mut ensure_no_content = false;
    let mut snapshot_rows = Vec::new();
    let mut statement_params = Vec::new();
    let mut next_placeholder = placeholder_offset + 1;
    let mut change_rows = Vec::new();
    let mut materialized_by_schema: BTreeMap<String, (Vec<Ident>, Vec<Vec<Expr>>, usize)> =
        BTreeMap::new();

    for change in &canonical_output.changes {
        let snapshot_id = match &change.snapshot_content {
            Some(content) => {
                let id = functions.uuid_v7();
                let id_placeholder = next_placeholder;
                next_placeholder += 1;
                statement_params.push(EngineValue::Text(id.clone()));
                let content_placeholder = next_placeholder;
                next_placeholder += 1;
                statement_params.push(EngineValue::Text(content.as_str().to_string()));
                snapshot_rows.push(vec![
                    placeholder_expr(id_placeholder),
                    placeholder_expr(content_placeholder),
                ]);
                id
            }
            None => {
                ensure_no_content = true;
                "no-content".to_string()
            }
        };

        change_rows.push(vec![
            text_param_expr(&change.id, &mut next_placeholder, &mut statement_params),
            text_param_expr(
                &change.entity_id,
                &mut next_placeholder,
                &mut statement_params,
            ),
            text_param_expr(
                &change.schema_key,
                &mut next_placeholder,
                &mut statement_params,
            ),
            text_param_expr(
                &change.schema_version,
                &mut next_placeholder,
                &mut statement_params,
            ),
            text_param_expr(
                &change.file_id,
                &mut next_placeholder,
                &mut statement_params,
            ),
            text_param_expr(
                &change.plugin_key,
                &mut next_placeholder,
                &mut statement_params,
            ),
            text_param_expr(&snapshot_id, &mut next_placeholder, &mut statement_params),
            optional_text_param_expr(
                change.metadata.as_ref().map(|value| value.as_str()),
                &mut next_placeholder,
                &mut statement_params,
            ),
            text_param_expr(
                &change.created_at,
                &mut next_placeholder,
                &mut statement_params,
            ),
        ]);
    }

    for row in &derived_apply_input.live_state_rows {
        let layout = derived_apply_input
            .live_layouts
            .get(&row.schema_key)
            .cloned()
            .map(Some)
            .unwrap_or(builtin_live_table_layout(&row.schema_key)?);
        let normalized_values = normalized_live_column_values_for_row(
            layout.as_ref(),
            row.snapshot_content.as_deref(),
        )?;
        let columns = live_state_insert_columns(layout.as_ref());
        let params_per_row = columns.len();
        let values = live_state_row_values_parameterized(
            row,
            layout.as_ref(),
            &normalized_values,
            &mut next_placeholder,
            &mut statement_params,
        );
        let entry = materialized_by_schema
            .entry(row.schema_key.clone())
            .or_insert_with(|| (columns.clone(), Vec::new(), params_per_row));
        entry.1.push(values);
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
        push_chunked_insert_statements(
            &mut statements,
            SNAPSHOT_TABLE,
            vec![Ident::new("id"), Ident::new("content")],
            snapshot_rows,
            Some(build_snapshot_on_conflict()),
            max_rows_per_insert_for_dialect(dialect, SNAPSHOT_INSERT_PARAM_COLUMNS),
        );
    }

    if !change_rows.is_empty() {
        push_chunked_insert_statements(
            &mut statements,
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
            max_rows_per_insert_for_dialect(dialect, CHANGE_INSERT_PARAM_COLUMNS),
        );
    }

    for (schema_key, (columns, rows, params_per_row)) in materialized_by_schema {
        let table_name = tracked_live_table_name(&schema_key);
        push_chunked_insert_statements(
            &mut statements,
            &table_name,
            columns.clone(),
            rows,
            Some(build_live_state_on_conflict(&columns)),
            max_rows_per_insert_for_dialect(dialect, params_per_row),
        );
    }

    append_commit_graph_node_statements(
        &mut statements,
        &mut statement_params,
        &mut next_placeholder,
        &derived_apply_input.live_state_rows,
    )?;

    Ok(StatementBatch {
        statements,
        params: statement_params,
    })
}

fn max_bind_parameters_for_dialect(dialect: SqlDialect) -> usize {
    match dialect {
        SqlDialect::Sqlite => SQLITE_MAX_BIND_PARAMETERS_PER_STATEMENT,
        SqlDialect::Postgres => POSTGRES_MAX_BIND_PARAMETERS_PER_STATEMENT,
    }
}

fn max_rows_per_insert_for_dialect(dialect: SqlDialect, params_per_row: usize) -> usize {
    (max_bind_parameters_for_dialect(dialect) / params_per_row).max(1)
}

fn push_chunked_insert_statements(
    statements: &mut Vec<Statement>,
    table: &str,
    columns: Vec<Ident>,
    rows: Vec<Vec<Expr>>,
    on: Option<OnInsert>,
    max_rows_per_statement: usize,
) {
    if rows.is_empty() {
        return;
    }

    if rows.len() <= max_rows_per_statement {
        statements.push(make_insert_statement(table, columns, rows, on));
        return;
    }

    let mut chunk = Vec::with_capacity(max_rows_per_statement);
    for row in rows {
        chunk.push(row);
        if chunk.len() == max_rows_per_statement {
            statements.push(make_insert_statement(
                table,
                columns.clone(),
                std::mem::take(&mut chunk),
                on.clone(),
            ));
        }
    }

    if !chunk.is_empty() {
        statements.push(make_insert_statement(table, columns, chunk, on));
    }
}

pub(crate) fn parse_single_statement_from_sql(sql: &str) -> Result<Statement, LixError> {
    let mut statements = Parser::parse_sql(&GenericDialect {}, sql).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: error.to_string(),
    })?;
    if statements.len() != 1 {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "expected a single statement".to_string(),
        });
    }
    Ok(statements.remove(0))
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
        text_param_expr(&row.id, next_placeholder, params),
        optional_text_param_expr(
            row.metadata.as_ref().map(|value| value.as_str()),
            next_placeholder,
            params,
        ),
        optional_text_param_expr(row.writer_key.as_deref(), next_placeholder, params),
        number_expr(if row.snapshot_content.is_some() {
            "0"
        } else {
            "1"
        }),
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

fn build_live_state_on_conflict(columns: &[Ident]) -> OnInsert {
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
                ObjectNamePart::Identifier(Ident::new("change_id")),
            ])),
            value: Expr::Identifier(Ident::new("excluded.change_id")),
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
                ObjectNamePart::Identifier(Ident::new("is_tombstone")),
            ])),
            value: Expr::Identifier(Ident::new("excluded.is_tombstone")),
        },
        sqlparser::ast::Assignment {
            target: sqlparser::ast::AssignmentTarget::ColumnName(ObjectName(vec![
                ObjectNamePart::Identifier(Ident::new("updated_at")),
            ])),
            value: Expr::Identifier(Ident::new("excluded.updated_at")),
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
