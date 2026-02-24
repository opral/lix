use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value as JsonValue;
use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    ConflictTarget, DoUpdate, Expr, Ident, ObjectName, ObjectNamePart, OnConflict,
    OnConflictAction, OnInsert, Query, SetExpr, Statement, TableObject, Value as SqlValue, Values,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::account::{
    active_account_file_id, active_account_schema_key, active_account_storage_version_id,
    parse_active_account_snapshot,
};
use crate::builtin_schema::types::LixVersionPointer;
use crate::commit::{
    DomainChangeInput, GenerateCommitResult, MaterializedStateRow, VersionInfo, VersionSnapshot,
};
use crate::functions::LixFunctionProvider;

use super::super::contracts::prepared_statement::PreparedStatement;
use super::super::storage::sql_text::escape_sql_string;
use crate::{LixError, QueryResult, SqlDialect, Value as EngineValue};

const UNTRACKED_TABLE: &str = "lix_internal_state_untracked";
const SNAPSHOT_TABLE: &str = "lix_internal_snapshot";
const CHANGE_TABLE: &str = "lix_internal_change";
const MATERIALIZED_PREFIX: &str = "lix_internal_state_materialized_v1_";
const VERSION_POINTER_TABLE: &str = "lix_internal_state_materialized_v1_lix_version_pointer";
const VERSION_POINTER_SCHEMA_KEY: &str = "lix_version_pointer";
const CHANGE_AUTHOR_SCHEMA_KEY: &str = "lix_change_author";
const COMMIT_SCHEMA_KEY: &str = "lix_commit";
const COMMIT_EDGE_SCHEMA_KEY: &str = "lix_commit_edge";
const COMMIT_ANCESTRY_TABLE: &str = "lix_internal_commit_ancestry";
const GLOBAL_VERSION: &str = "global";
const SQLITE_MAX_BIND_PARAMETERS_PER_STATEMENT: usize = 32_766;
const POSTGRES_MAX_BIND_PARAMETERS_PER_STATEMENT: usize = 65_535;
const SNAPSHOT_INSERT_PARAM_COLUMNS: usize = 2;
const CHANGE_INSERT_PARAM_COLUMNS: usize = 9;
const MATERIALIZED_INSERT_PARAM_COLUMNS: usize = 13;

#[derive(Debug)]
pub(crate) struct StatementBatch {
    pub(crate) statements: Vec<Statement>,
    pub(crate) params: Vec<EngineValue>,
}

#[async_trait::async_trait(?Send)]
pub(crate) trait CommitQueryExecutor {
    async fn execute(
        &mut self,
        sql: &str,
        params: &[EngineValue],
    ) -> Result<QueryResult, LixError>;
}

pub(crate) fn bind_statement_batch_for_dialect(
    batch: StatementBatch,
    dialect: SqlDialect,
) -> Result<Vec<PreparedStatement>, LixError> {
    let mut prepared = Vec::with_capacity(batch.statements.len());
    for statement in batch.statements {
        let bound = crate::engine::sql2::ast::utils::bind_sql(
            &statement.to_string(),
            &batch.params,
            dialect,
        )?;
        prepared.push(PreparedStatement {
            sql: bound.sql,
            params: bound.params,
        });
    }
    Ok(prepared)
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
        "SELECT snapshot_content \
         FROM {table_name} \
         WHERE schema_key = '{schema_key}' \
           AND file_id = '{file_id}' \
           AND version_id = '{version_id}' \
           AND snapshot_content IS NOT NULL",
        table_name = UNTRACKED_TABLE,
        schema_key = escape_sql_string(active_account_schema_key()),
        file_id = escape_sql_string(active_account_file_id()),
        version_id = escape_sql_string(active_account_storage_version_id()),
    );
    let result = executor.execute(&sql, &[]).await?;

    let mut deduped = BTreeSet::new();
    for row in result.rows {
        let Some(value) = row.first() else {
            continue;
        };
        let snapshot = match value {
            EngineValue::Text(text) => text,
            EngineValue::Null => continue,
            _ => {
                return Err(LixError {
                    message: "active account snapshot_content must be text".to_string(),
                });
            }
        };
        let account_id = parse_active_account_snapshot(snapshot)?;
        deduped.insert(account_id);
    }

    Ok(deduped.into_iter().collect())
}

pub(crate) async fn load_version_info_for_versions(
    executor: &mut dyn CommitQueryExecutor,
    version_ids: &BTreeSet<String>,
) -> Result<BTreeMap<String, VersionInfo>, LixError> {
    let mut versions = BTreeMap::new();
    if version_ids.is_empty() {
        return Ok(versions);
    }

    for version_id in version_ids {
        versions.insert(
            version_id.clone(),
            VersionInfo {
                parent_commit_ids: Vec::new(),
                snapshot: VersionSnapshot {
                    id: version_id.clone(),
                    working_commit_id: version_id.clone(),
                },
            },
        );
    }

    let in_list = version_ids
        .iter()
        .map(|version_id| format!("'{}'", escape_sql_string(version_id)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT entity_id, snapshot_content \
         FROM {table_name} \
         WHERE schema_key = '{schema_key}' \
           AND version_id = '{global_version}' \
           AND is_tombstone = 0 \
           AND snapshot_content IS NOT NULL \
           AND entity_id IN ({in_list})",
        table_name = VERSION_POINTER_TABLE,
        schema_key = VERSION_POINTER_SCHEMA_KEY,
        global_version = GLOBAL_VERSION,
        in_list = in_list,
    );

    match executor.execute(&sql, &[]).await {
        Ok(result) => {
            for row in result.rows {
                if row.len() < 2 {
                    continue;
                }
                let entity_id = match &row[0] {
                    EngineValue::Text(value) => value.clone(),
                    EngineValue::Null => continue,
                    _ => {
                        return Err(LixError {
                            message: "version tip entity_id must be text".to_string(),
                        });
                    }
                };
                if !version_ids.contains(&entity_id) {
                    continue;
                }
                let Some(parsed) = parse_version_info_from_tip_snapshot(&row[1], &entity_id)? else {
                    continue;
                };
                versions.insert(entity_id, parsed);
            }
        }
        Err(err) if is_missing_relation_error(&err) => {}
        Err(err) => return Err(err),
    }

    Ok(versions)
}

fn parse_version_info_from_tip_snapshot(
    value: &EngineValue,
    fallback_version_id: &str,
) -> Result<Option<VersionInfo>, LixError> {
    let raw_snapshot = match value {
        EngineValue::Text(value) => value,
        EngineValue::Null => return Ok(None),
        _ => {
            return Err(LixError {
                message: "version tip snapshot_content must be text".to_string(),
            });
        }
    };

    let snapshot: LixVersionPointer = serde_json::from_str(raw_snapshot).map_err(|error| LixError {
        message: format!("version tip snapshot_content invalid JSON: {error}"),
    })?;
    let version_id = if snapshot.id.is_empty() {
        fallback_version_id.to_string()
    } else {
        snapshot.id
    };
    let working_commit_id = snapshot
        .working_commit_id
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| fallback_version_id.to_string());
    let parent_commit_ids = if snapshot.commit_id.is_empty() || snapshot.commit_id == working_commit_id {
        Vec::new()
    } else {
        vec![snapshot.commit_id]
    };

    Ok(Some(VersionInfo {
        parent_commit_ids,
        snapshot: VersionSnapshot {
            id: version_id,
            working_commit_id,
        },
    }))
}

pub(crate) fn build_statement_batch_from_generate_commit_result(
    commit_result: GenerateCommitResult,
    functions: &mut dyn LixFunctionProvider,
    placeholder_offset: usize,
    dialect: SqlDialect,
) -> Result<StatementBatch, LixError> {
    let mut ensure_no_content = false;
    let mut snapshot_rows = Vec::new();
    let mut statement_params = Vec::new();
    let mut next_placeholder = placeholder_offset + 1;
    let mut change_rows = Vec::new();
    let mut materialized_by_schema: BTreeMap<String, Vec<Vec<Expr>>> = BTreeMap::new();

    for change in &commit_result.changes {
        let snapshot_id = match &change.snapshot_content {
            Some(content) => {
                let id = functions.uuid_v7();
                let id_placeholder = next_placeholder;
                next_placeholder += 1;
                statement_params.push(EngineValue::Text(id.clone()));
                let content_placeholder = next_placeholder;
                next_placeholder += 1;
                statement_params.push(EngineValue::Text(content.clone()));
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
            text_param_expr(&change.entity_id, &mut next_placeholder, &mut statement_params),
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
            text_param_expr(&change.file_id, &mut next_placeholder, &mut statement_params),
            text_param_expr(
                &change.plugin_key,
                &mut next_placeholder,
                &mut statement_params,
            ),
            text_param_expr(&snapshot_id, &mut next_placeholder, &mut statement_params),
            optional_text_param_expr(
                change.metadata.as_deref(),
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

    for row in &commit_result.materialized_state {
        materialized_by_schema
            .entry(row.schema_key.clone())
            .or_default()
            .push(materialized_row_values_parameterized(
                row,
                &mut next_placeholder,
                &mut statement_params,
            ));
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

    for (schema_key, rows) in materialized_by_schema {
        let table_name = format!("{MATERIALIZED_PREFIX}{schema_key}");
        push_chunked_insert_statements(
            &mut statements,
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
            max_rows_per_insert_for_dialect(dialect, MATERIALIZED_INSERT_PARAM_COLUMNS),
        );
    }

    append_commit_ancestry_statements(
        &mut statements,
        &mut statement_params,
        &mut next_placeholder,
        &commit_result.materialized_state,
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

fn append_commit_ancestry_statements(
    statements: &mut Vec<Statement>,
    params: &mut Vec<EngineValue>,
    next_placeholder: &mut usize,
    materialized_state: &[MaterializedStateRow],
) -> Result<(), LixError> {
    let commit_parents = collect_commit_parent_map_for_ancestry(materialized_state)?;
    for (commit_id, parent_ids) in commit_parents {
        let commit_placeholder = *next_placeholder;
        *next_placeholder += 1;
        params.push(EngineValue::Text(commit_id));

        let self_insert_sql = format!(
            "INSERT INTO {table} (commit_id, ancestor_id, depth) \
             VALUES (?{commit_placeholder}, ?{commit_placeholder}, 0) \
             ON CONFLICT (commit_id, ancestor_id) DO NOTHING",
            table = COMMIT_ANCESTRY_TABLE,
            commit_placeholder = commit_placeholder,
        );
        statements.push(parse_single_statement_from_sql(&self_insert_sql)?);

        for parent_id in parent_ids {
            let parent_placeholder = *next_placeholder;
            *next_placeholder += 1;
            params.push(EngineValue::Text(parent_id));

            let insert_parent_ancestry_sql = format!(
                "INSERT INTO {table} (commit_id, ancestor_id, depth) \
                 SELECT ?{commit_placeholder} AS commit_id, candidate.ancestor_id, MIN(candidate.depth) AS depth \
                 FROM ( \
                   SELECT ?{parent_placeholder} AS ancestor_id, 1 AS depth \
                   UNION ALL \
                   SELECT ancestor_id, depth + 1 AS depth \
                   FROM {table} \
                   WHERE commit_id = ?{parent_placeholder} \
                 ) AS candidate \
                 GROUP BY candidate.ancestor_id \
                 ON CONFLICT (commit_id, ancestor_id) DO UPDATE \
                 SET depth = CASE \
                   WHEN excluded.depth < {table}.depth THEN excluded.depth \
                   ELSE {table}.depth \
                 END",
                table = COMMIT_ANCESTRY_TABLE,
                commit_placeholder = commit_placeholder,
                parent_placeholder = parent_placeholder,
            );
            statements.push(parse_single_statement_from_sql(&insert_parent_ancestry_sql)?);
        }
    }
    Ok(())
}

fn collect_commit_parent_map_for_ancestry(
    materialized_state: &[MaterializedStateRow],
) -> Result<BTreeMap<String, BTreeSet<String>>, LixError> {
    let mut out = BTreeMap::<String, BTreeSet<String>>::new();
    for row in materialized_state {
        if row.schema_key == COMMIT_SCHEMA_KEY && row.lixcol_version_id == GLOBAL_VERSION {
            out.entry(row.entity_id.clone()).or_default();
        }
    }

    for row in materialized_state {
        if row.schema_key != COMMIT_EDGE_SCHEMA_KEY || row.lixcol_version_id != GLOBAL_VERSION {
            continue;
        }
        let Some(raw) = row.snapshot_content.as_deref() else {
            continue;
        };
        let Some((parent_id, child_id)) = parse_commit_edge_snapshot_for_ancestry(raw)? else {
            continue;
        };
        if let Some(parents) = out.get_mut(&child_id) {
            parents.insert(parent_id);
        }
    }

    Ok(out)
}

fn parse_commit_edge_snapshot_for_ancestry(raw: &str) -> Result<Option<(String, String)>, LixError> {
    let parsed: JsonValue = serde_json::from_str(raw).map_err(|error| LixError {
        message: format!("commit_edge snapshot invalid JSON: {error}"),
    })?;
    let parent_id = parsed
        .get("parent_id")
        .and_then(JsonValue::as_str)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);
    let child_id = parsed
        .get("child_id")
        .and_then(JsonValue::as_str)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);
    match (parent_id, child_id) {
        (Some(parent_id), Some(child_id)) => Ok(Some((parent_id, child_id))),
        _ => Ok(None),
    }
}

fn parse_single_statement_from_sql(sql: &str) -> Result<Statement, LixError> {
    let mut statements = Parser::parse_sql(&GenericDialect {}, sql).map_err(|error| LixError {
        message: error.to_string(),
    })?;
    if statements.len() != 1 {
        return Err(LixError {
            message: "expected a single statement".to_string(),
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

fn materialized_row_values_parameterized(
    row: &MaterializedStateRow,
    next_placeholder: &mut usize,
    params: &mut Vec<EngineValue>,
) -> Vec<Expr> {
    vec![
        text_param_expr(&row.entity_id, next_placeholder, params),
        text_param_expr(&row.schema_key, next_placeholder, params),
        text_param_expr(&row.schema_version, next_placeholder, params),
        text_param_expr(&row.file_id, next_placeholder, params),
        text_param_expr(&row.lixcol_version_id, next_placeholder, params),
        text_param_expr(&row.plugin_key, next_placeholder, params),
        optional_text_param_expr(row.snapshot_content.as_deref(), next_placeholder, params),
        text_param_expr(&row.id, next_placeholder, params),
        optional_text_param_expr(row.metadata.as_deref(), next_placeholder, params),
        optional_text_param_expr(row.writer_key.as_deref(), next_placeholder, params),
        number_expr("0"),
        text_param_expr(&row.created_at, next_placeholder, params),
        text_param_expr(&row.created_at, next_placeholder, params),
    ]
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

fn build_materialized_on_conflict() -> OnInsert {
    OnInsert::OnConflict(OnConflict {
        conflict_target: Some(ConflictTarget::Columns(vec![
            Ident::new("entity_id"),
            Ident::new("file_id"),
            Ident::new("version_id"),
        ])),
        action: OnConflictAction::DoUpdate(DoUpdate {
            assignments: vec![
                sqlparser::ast::Assignment {
                    target: sqlparser::ast::AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident::new("change_id")),
                    ])),
                    value: Expr::Identifier(Ident::new("excluded.change_id")),
                },
                sqlparser::ast::Assignment {
                    target: sqlparser::ast::AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident::new("snapshot_content")),
                    ])),
                    value: Expr::Identifier(Ident::new("excluded.snapshot_content")),
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
            ],
            selection: None,
        }),
    })
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

fn null_expr() -> Expr {
    Expr::Value(SqlValue::Null.into())
}

fn is_missing_relation_error(err: &LixError) -> bool {
    let lower = err.message.to_lowercase();
    lower.contains("no such table")
        || lower.contains("relation")
            && (lower.contains("does not exist")
                || lower.contains("undefined table")
                || lower.contains("unknown"))
}
