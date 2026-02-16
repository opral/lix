use serde_json::Value as JsonValue;
use sqlparser::ast::{
    Expr, Ident, Insert, ObjectName, ObjectNamePart, Statement, TableObject, Value, ValueWithSpan,
};

use crate::sql::{
    object_name_matches, MutationOperation, MutationRow, ResolvedCell, RowSourceResolver,
    SchemaRegistration,
};
use crate::{LixError, Value as EngineValue};

const STORED_SCHEMA_KEY: &str = "lix_stored_schema";
const GLOBAL_VERSION: &str = "global";
const ENGINE_FILE_ID: &str = "lix";
const ENGINE_PLUGIN_KEY: &str = "lix";

#[derive(Debug, Clone)]
pub struct StoredSchemaRewrite {
    pub statement: Statement,
    pub registration: SchemaRegistration,
    pub mutation: MutationRow,
}

pub fn rewrite_insert(
    insert: Insert,
    params: &[EngineValue],
) -> Result<Option<StoredSchemaRewrite>, LixError> {
    if !table_object_is_vtable(&insert.table) {
        return Ok(None);
    }

    let schema_key_index = find_column_index(&insert.columns, "schema_key");
    let mut columns = insert.columns.clone();
    let mut source = match &insert.source {
        Some(source) => source.clone(),
        None => return Ok(None),
    };

    let resolver = RowSourceResolver::new(params);
    let Some(row_source) = resolver.resolve_insert(&insert)? else {
        return Ok(None);
    };
    if row_source.rows.is_empty() {
        return Ok(None);
    }

    let mut rows = row_source.rows;
    let resolved_rows = row_source.resolved_rows;
    let values_layout = row_source.values_layout;
    resolve_row_literals(&mut rows, &resolved_rows)?;

    let schema_key_index = match schema_key_index {
        Some(index) => index,
        None => return Ok(None),
    };

    if !resolved_rows
        .iter()
        .all(|row| resolved_value_equals(row.get(schema_key_index), STORED_SCHEMA_KEY))
    {
        return Ok(None);
    }

    let snapshot_index = match find_column_index(&columns, "snapshot_content") {
        Some(index) => index,
        None => {
            return Err(LixError {
                message: "stored schema insert requires snapshot_content".to_string(),
            })
        }
    };

    if rows.len() != 1 {
        return Err(LixError {
            message: "stored schema insert supports a single row at a time".to_string(),
        });
    }

    let mut entity_id_value: Option<String> = None;
    let mut schema_key_value: Option<String> = None;
    let mut schema_version_value: Option<String> = None;
    let mut snapshot_literal_value: Option<String> = None;
    for (row_idx, row) in rows.iter().enumerate() {
        let snapshot_expr = row.get(snapshot_index).ok_or_else(|| LixError {
            message: "stored schema insert missing snapshot_content value".to_string(),
        })?;
        let literal = snapshot_literal(
            snapshot_expr,
            resolved_rows
                .get(row_idx)
                .and_then(|r| r.get(snapshot_index)),
        )?;
        if snapshot_literal_value.is_none() {
            snapshot_literal_value = Some(literal.clone());
        }
        let (schema_key, schema_version) = parse_schema_identity(&literal)?;
        let derived_id = format!("{}~{}", schema_key, schema_version);
        schema_key_value = Some(schema_key.clone());
        schema_version_value = Some(schema_version.clone());

        if let Some(existing) = &entity_id_value {
            if existing != &derived_id {
                return Err(LixError {
                    message: "stored schema insert must use a single schema identity".to_string(),
                });
            }
        } else {
            entity_id_value = Some(derived_id);
        }
    }

    let entity_id = entity_id_value.ok_or_else(|| LixError {
        message: "stored schema insert requires schema identity".to_string(),
    })?;
    let schema_key_value = schema_key_value.ok_or_else(|| LixError {
        message: "stored schema insert requires schema key".to_string(),
    })?;
    let schema_version_value = schema_version_value.ok_or_else(|| LixError {
        message: "stored schema insert requires schema version".to_string(),
    })?;
    let snapshot_literal_value = snapshot_literal_value.ok_or_else(|| LixError {
        message: "stored schema insert requires snapshot_content".to_string(),
    })?;
    let snapshot_value: JsonValue =
        serde_json::from_str(&snapshot_literal_value).map_err(|err| LixError {
            message: format!("stored schema snapshot_content must be valid JSON: {err}"),
        })?;

    if let Some(entity_index) = find_column_index(&columns, "entity_id") {
        if !resolved_rows
            .iter()
            .all(|row| resolved_value_equals(row.get(entity_index), &entity_id))
        {
            return Err(LixError {
                message: "stored schema insert entity_id must match schema key + version"
                    .to_string(),
            });
        }
    } else {
        append_column_with_literal(&mut columns, &mut rows, "entity_id", &entity_id);
    }

    ensure_literal_column(
        &mut columns,
        &mut rows,
        &resolved_rows,
        "schema_version",
        &schema_version_value,
    )?;
    ensure_literal_column(
        &mut columns,
        &mut rows,
        &resolved_rows,
        "version_id",
        GLOBAL_VERSION,
    )?;
    ensure_literal_column(
        &mut columns,
        &mut rows,
        &resolved_rows,
        "file_id",
        ENGINE_FILE_ID,
    )?;
    ensure_literal_column(
        &mut columns,
        &mut rows,
        &resolved_rows,
        "plugin_key",
        ENGINE_PLUGIN_KEY,
    )?;
    ensure_literal_column(
        &mut columns,
        &mut rows,
        &resolved_rows,
        "change_id",
        "schema",
    )?;
    ensure_literal_column(&mut columns, &mut rows, &resolved_rows, "is_tombstone", "0")?;
    ensure_literal_column(
        &mut columns,
        &mut rows,
        &resolved_rows,
        "created_at",
        "1970-01-01T00:00:00Z",
    )?;
    ensure_literal_column(
        &mut columns,
        &mut rows,
        &resolved_rows,
        "updated_at",
        "1970-01-01T00:00:00Z",
    )?;

    source.body = Box::new(sqlparser::ast::SetExpr::Values(sqlparser::ast::Values {
        explicit_row: values_layout.explicit_row,
        value_keyword: values_layout.value_keyword,
        rows,
    }));

    let rewritten = Insert {
        columns,
        source: Some(source),
        table: TableObject::TableName(ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
            "lix_internal_state_materialized_v1_lix_stored_schema",
        ))])),
        ..insert
    };

    Ok(Some(StoredSchemaRewrite {
        statement: Statement::Insert(rewritten),
        registration: SchemaRegistration {
            schema_key: schema_key_value,
        },
        mutation: MutationRow {
            operation: MutationOperation::Insert,
            entity_id,
            schema_key: STORED_SCHEMA_KEY.to_string(),
            schema_version: schema_version_value,
            file_id: ENGINE_FILE_ID.to_string(),
            version_id: GLOBAL_VERSION.to_string(),
            plugin_key: ENGINE_PLUGIN_KEY.to_string(),
            snapshot_content: Some(snapshot_value),
            untracked: false,
        },
    }))
}

fn table_object_is_vtable(table: &TableObject) -> bool {
    match table {
        TableObject::TableName(name) => object_name_matches(name, "lix_internal_state_vtable"),
        _ => false,
    }
}

fn find_column_index(columns: &[Ident], column: &str) -> Option<usize> {
    columns
        .iter()
        .position(|ident| ident.value.eq_ignore_ascii_case(column))
}

fn append_column_with_literal(
    columns: &mut Vec<Ident>,
    rows: &mut Vec<Vec<Expr>>,
    name: &str,
    value: &str,
) -> usize {
    columns.push(Ident::new(name));
    let expr = if value.chars().all(|c| c.is_ascii_digit()) {
        Expr::Value(ValueWithSpan::from(Value::Number(value.to_string(), false)))
    } else {
        Expr::Value(ValueWithSpan::from(Value::SingleQuotedString(
            value.to_string(),
        )))
    };
    for row in rows.iter_mut() {
        row.push(expr.clone());
    }
    columns.len() - 1
}

fn resolve_row_literals(
    rows: &mut [Vec<Expr>],
    resolved_rows: &[Vec<ResolvedCell>],
) -> Result<(), LixError> {
    for (row, resolved_row) in rows.iter_mut().zip(resolved_rows.iter()) {
        for (expr, cell) in row.iter_mut().zip(resolved_row.iter()) {
            if let Some(value) = &cell.value {
                *expr = engine_value_to_expr(value);
            }
        }
    }
    Ok(())
}

fn engine_value_to_expr(value: &EngineValue) -> Expr {
    match value {
        EngineValue::Null => Expr::Value(ValueWithSpan::from(Value::Null)),
        EngineValue::Text(value) => Expr::Value(ValueWithSpan::from(Value::SingleQuotedString(
            value.clone(),
        ))),
        EngineValue::Integer(value) => {
            Expr::Value(ValueWithSpan::from(Value::Number(value.to_string(), false)))
        }
        EngineValue::Real(value) => {
            Expr::Value(ValueWithSpan::from(Value::Number(value.to_string(), false)))
        }
        EngineValue::Blob(value) => Expr::Value(ValueWithSpan::from(
            Value::SingleQuotedByteStringLiteral(String::from_utf8_lossy(value).to_string()),
        )),
    }
}

fn ensure_literal_column(
    columns: &mut Vec<Ident>,
    rows: &mut Vec<Vec<Expr>>,
    resolved_rows: &[Vec<ResolvedCell>],
    name: &str,
    value: &str,
) -> Result<(), LixError> {
    if let Some(index) = find_column_index(columns, name) {
        let ok = resolved_rows
            .iter()
            .all(|row| resolved_value_equals(row.get(index), value));
        if !ok {
            return Err(LixError {
                message: format!("stored schema insert requires {name} = '{value}'"),
            });
        }
        return Ok(());
    }
    append_column_with_literal(columns, rows, name, value);
    Ok(())
}

fn resolved_value_equals(cell: Option<&ResolvedCell>, expected: &str) -> bool {
    match cell.and_then(|cell| cell.value.as_ref()) {
        Some(EngineValue::Text(value)) => value == expected,
        Some(EngineValue::Integer(value)) => value.to_string() == expected,
        _ => false,
    }
}

fn snapshot_literal(expr: &Expr, cell: Option<&ResolvedCell>) -> Result<String, LixError> {
    if let Some(ResolvedCell {
        value: Some(EngineValue::Text(value)),
        ..
    }) = cell
    {
        return Ok(value.clone());
    }

    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(value),
            ..
        }) => Ok(value.clone()),
        _ => Err(LixError {
            message: "stored schema insert requires literal snapshot_content".to_string(),
        }),
    }
}

fn parse_schema_identity(snapshot: &str) -> Result<(String, String), LixError> {
    let parsed: JsonValue = serde_json::from_str(snapshot).map_err(|err| LixError {
        message: format!("stored schema snapshot_content must be valid JSON: {err}"),
    })?;
    let value = parsed.get("value").ok_or_else(|| LixError {
        message: "stored schema snapshot_content must contain value".to_string(),
    })?;
    let obj = value.as_object().ok_or_else(|| LixError {
        message: "stored schema value must be an object".to_string(),
    })?;
    let schema_key = obj
        .get("x-lix-key")
        .and_then(|v| v.as_str())
        .ok_or_else(|| LixError {
            message: "stored schema value.x-lix-key must be string".to_string(),
        })?;
    let schema_version = obj
        .get("x-lix-version")
        .and_then(|v| v.as_str())
        .ok_or_else(|| LixError {
            message: "stored schema value.x-lix-version must be string".to_string(),
        })?;

    // Deliberately keep x-lix-version as a monotonic integer (string) so we can evolve
    // translation rules later without locking into semver semantics.
    ensure_monotonic_version(schema_version)?;

    Ok((schema_key.to_string(), schema_version.to_string()))
}

fn ensure_monotonic_version(version: &str) -> Result<(), LixError> {
    if version.is_empty() {
        return Err(LixError {
            message: "stored schema x-lix-version must be a monotonic integer".to_string(),
        });
    }
    let mut chars = version.chars();
    let Some(first) = chars.next() else {
        return Err(LixError {
            message: "stored schema x-lix-version must be a monotonic integer".to_string(),
        });
    };
    if first == '0' || !first.is_ascii_digit() || !chars.all(|c| c.is_ascii_digit()) {
        return Err(LixError {
            message:
                "stored schema x-lix-version must be a monotonic integer without leading zeros"
                    .to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::SetExpr;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn parse_insert(sql: &str) -> Insert {
        let dialect = GenericDialect {};
        let mut statements = Parser::parse_sql(&dialect, sql).expect("parse sql");
        let stmt = statements.remove(0);
        match stmt {
            Statement::Insert(insert) => insert,
            _ => panic!("expected insert"),
        }
    }

    fn extract_row(insert: &Insert) -> Vec<Expr> {
        let source = insert.source.as_ref().expect("insert source");
        match source.body.as_ref() {
            SetExpr::Values(values) => values.rows[0].clone(),
            _ => panic!("expected values"),
        }
    }

    fn column_index(columns: &[Ident], name: &str) -> usize {
        columns
            .iter()
            .position(|ident| ident.value.eq_ignore_ascii_case(name))
            .expect("column missing")
    }

    fn expr_string(expr: &Expr) -> String {
        match expr {
            Expr::Value(ValueWithSpan {
                value: Value::SingleQuotedString(value),
                ..
            }) => value.clone(),
            Expr::Value(ValueWithSpan {
                value: Value::Number(value, _),
                ..
            }) => value.clone(),
            _ => panic!("expected string literal"),
        }
    }

    #[test]
    fn rewrite_stored_schema_insert_adds_overrides() {
        let sql = r#"INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES ('lix_stored_schema', '{"value":{"x-lix-key":"mock_schema","x-lix-version":"1"}}')"#;
        let insert = parse_insert(sql);
        let rewritten = rewrite_insert(insert, &[])
            .expect("rewrite ok")
            .expect("rewritten");
        let insert = match rewritten.statement {
            Statement::Insert(insert) => insert,
            _ => panic!("expected insert"),
        };

        let row = extract_row(&insert);
        let entity_idx = column_index(&insert.columns, "entity_id");
        let version_idx = column_index(&insert.columns, "version_id");
        let schema_version_idx = column_index(&insert.columns, "schema_version");
        let file_idx = column_index(&insert.columns, "file_id");
        let plugin_idx = column_index(&insert.columns, "plugin_key");
        let change_idx = column_index(&insert.columns, "change_id");
        let tombstone_idx = column_index(&insert.columns, "is_tombstone");
        let created_idx = column_index(&insert.columns, "created_at");
        let updated_idx = column_index(&insert.columns, "updated_at");

        assert_eq!(expr_string(&row[entity_idx]), "mock_schema~1");
        assert_eq!(expr_string(&row[version_idx]), "global");
        assert_eq!(expr_string(&row[schema_version_idx]), "1");
        assert_eq!(expr_string(&row[file_idx]), "lix");
        assert_eq!(expr_string(&row[plugin_idx]), "lix");
        assert_eq!(expr_string(&row[change_idx]), "schema");
        assert_eq!(expr_string(&row[tombstone_idx]), "0");
        assert_eq!(expr_string(&row[created_idx]), "1970-01-01T00:00:00Z");
        assert_eq!(expr_string(&row[updated_idx]), "1970-01-01T00:00:00Z");
        assert!(insert
            .table
            .to_string()
            .contains("lix_internal_state_materialized_v1_lix_stored_schema"));
    }

    #[test]
    fn rewrite_stored_schema_insert_supports_cast_placeholder_snapshot_content() {
        let sql = r#"INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES ('lix_stored_schema', CAST(? AS TEXT))"#;
        let insert = parse_insert(sql);
        let rewritten = rewrite_insert(
            insert,
            &[EngineValue::Text(
                "{\"value\":{\"x-lix-key\":\"mock_schema\",\"x-lix-version\":\"1\"}}".to_string(),
            )],
        )
        .expect("rewrite ok")
        .expect("rewritten");
        let insert = match rewritten.statement {
            Statement::Insert(insert) => insert,
            _ => panic!("expected insert"),
        };

        let row = extract_row(&insert);
        let snapshot_idx = column_index(&insert.columns, "snapshot_content");
        let entity_idx = column_index(&insert.columns, "entity_id");
        assert_eq!(
            expr_string(&row[snapshot_idx]),
            "{\"value\":{\"x-lix-key\":\"mock_schema\",\"x-lix-version\":\"1\"}}"
        );
        assert_eq!(expr_string(&row[entity_idx]), "mock_schema~1");
    }

    #[test]
    fn rewrite_stored_schema_requires_monotonic_version() {
        let sql = r#"INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES ('lix_stored_schema', '{"value":{"x-lix-key":"mock_schema","x-lix-version":"v1"}}')"#;
        let insert = parse_insert(sql);
        let err = rewrite_insert(insert, &[]).expect_err("expected error");
        assert!(err.message.contains("monotonic"), "{:#?}", err);
    }

    #[test]
    fn rewrite_ignores_other_schema_keys() {
        let sql = r#"INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES ('other_schema', '{"value":{"x-lix-key":"mock_schema","x-lix-version":"1"}}')"#;
        let insert = parse_insert(sql);
        let rewritten = rewrite_insert(insert, &[]).expect("rewrite ok");
        assert!(rewritten.is_none());
    }
}
