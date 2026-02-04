use serde_json::Value as JsonValue;
use sqlparser::ast::{
    Expr, Ident, Insert, ObjectName, ObjectNamePart, Statement, TableObject, Value, ValueWithSpan,
};

use crate::sql::SchemaRegistration;
use crate::LixError;

const STORED_SCHEMA_KEY: &str = "lix_stored_schema";
const GLOBAL_VERSION: &str = "global";
const ENGINE_FILE_ID: &str = "lix";
const ENGINE_PLUGIN_KEY: &str = "lix";

#[derive(Debug, Clone)]
pub struct StoredSchemaRewrite {
    pub statement: Statement,
    pub registration: SchemaRegistration,
}

pub fn rewrite_insert(insert: Insert) -> Result<Option<StoredSchemaRewrite>, LixError> {
    if !table_object_is_vtable(&insert.table) {
        return Ok(None);
    }

    let schema_key_index = find_column_index(&insert.columns, "schema_key");
    let mut columns = insert.columns.clone();
    let mut source = match &insert.source {
        Some(source) => source.clone(),
        None => return Ok(None),
    };

    let values = match source.body.as_ref() {
        sqlparser::ast::SetExpr::Values(values) => values,
        _ => return Ok(None),
    };

    if values.rows.is_empty() {
        return Ok(None);
    }

    let mut rows = values.rows.clone();

    let schema_key_index = match schema_key_index {
        Some(index) => index,
        None => return Ok(None),
    };

    if !rows
        .iter()
        .all(|row| is_literal_equal(row.get(schema_key_index), STORED_SCHEMA_KEY))
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
    for row in &rows {
        let snapshot_expr = row.get(snapshot_index).ok_or_else(|| LixError {
            message: "stored schema insert missing snapshot_content value".to_string(),
        })?;
        let literal = snapshot_literal(snapshot_expr)?;
        let (schema_key, schema_version) = parse_schema_identity(&literal)?;
        let derived_id = format!("{}~{}", schema_key, schema_version);
        schema_key_value = Some(schema_key.clone());

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

    if let Some(entity_index) = find_column_index(&columns, "entity_id") {
        if !rows
            .iter()
            .all(|row| is_literal_equal(row.get(entity_index), &entity_id))
        {
            return Err(LixError {
                message: "stored schema insert entity_id must match schema key + version".to_string(),
            });
        }
    } else {
        append_column_with_literal(&mut columns, &mut rows, "entity_id", &entity_id);
    }

    ensure_literal_column(&mut columns, &mut rows, "version_id", GLOBAL_VERSION)?;
    ensure_literal_column(&mut columns, &mut rows, "file_id", ENGINE_FILE_ID)?;
    ensure_literal_column(&mut columns, &mut rows, "plugin_key", ENGINE_PLUGIN_KEY)?;
    ensure_literal_column(&mut columns, &mut rows, "change_id", "schema")?;
    ensure_literal_column(&mut columns, &mut rows, "is_tombstone", "0")?;
    ensure_literal_column(
        &mut columns,
        &mut rows,
        "created_at",
        "1970-01-01T00:00:00Z",
    )?;
    ensure_literal_column(
        &mut columns,
        &mut rows,
        "updated_at",
        "1970-01-01T00:00:00Z",
    )?;

    source.body = Box::new(sqlparser::ast::SetExpr::Values(sqlparser::ast::Values {
        explicit_row: values.explicit_row,
        value_keyword: values.value_keyword,
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

fn ensure_literal_column(
    columns: &mut Vec<Ident>,
    rows: &mut Vec<Vec<Expr>>,
    name: &str,
    value: &str,
) -> Result<(), LixError> {
    if let Some(index) = find_column_index(columns, name) {
        let ok = rows
            .iter()
            .all(|row| is_literal_equal(row.get(index), value));
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

fn is_literal_equal(expr: Option<&Expr>, expected: &str) -> bool {
    match expr {
        Some(Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(value),
            ..
        })) => value == expected,
        Some(Expr::Value(ValueWithSpan {
            value: Value::Number(value, _),
            ..
        })) => value == expected,
        _ => false,
    }
}

fn snapshot_literal(expr: &Expr) -> Result<String, LixError> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(value),
            ..
        }) => Ok(value.clone()),
        _ => Err(LixError {
            message:
                "stored schema insert requires literal snapshot_content (prepared statement support TODO)"
                    .to_string(),
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

    ensure_semver(schema_version)?;

    Ok((schema_key.to_string(), schema_version.to_string()))
}

fn ensure_semver(version: &str) -> Result<(), LixError> {
    let parts: Vec<&str> = version.split('.').collect();
    if parts.len() != 3 {
        return Err(LixError {
            message: "stored schema x-lix-version must be semver (major.minor.patch)".to_string(),
        });
    }
    if parts.iter().all(|part| !part.is_empty() && part.chars().all(|c| c.is_ascii_digit())) {
        return Ok(());
    }
    Err(LixError {
        message: "stored schema x-lix-version must be semver (major.minor.patch)".to_string(),
    })
}

fn object_name_matches(name: &ObjectName, target: &str) -> bool {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .map(|ident| ident.value.eq_ignore_ascii_case(target))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use sqlparser::ast::SetExpr;

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
        let sql = r#"INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES ('lix_stored_schema', '{"value":{"x-lix-key":"mock_schema","x-lix-version":"1.0.0"}}')"#;
        let insert = parse_insert(sql);
        let rewritten = rewrite_insert(insert).expect("rewrite ok").expect("rewritten");
        let insert = match rewritten.statement {
            Statement::Insert(insert) => insert,
            _ => panic!("expected insert"),
        };

        let row = extract_row(&insert);
        let entity_idx = column_index(&insert.columns, "entity_id");
        let version_idx = column_index(&insert.columns, "version_id");
        let file_idx = column_index(&insert.columns, "file_id");
        let plugin_idx = column_index(&insert.columns, "plugin_key");
        let change_idx = column_index(&insert.columns, "change_id");
        let tombstone_idx = column_index(&insert.columns, "is_tombstone");
        let created_idx = column_index(&insert.columns, "created_at");
        let updated_idx = column_index(&insert.columns, "updated_at");

        assert_eq!(expr_string(&row[entity_idx]), "mock_schema~1.0.0");
        assert_eq!(expr_string(&row[version_idx]), "global");
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
    fn rewrite_stored_schema_requires_semver() {
        let sql = r#"INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES ('lix_stored_schema', '{"value":{"x-lix-key":"mock_schema","x-lix-version":"1.0"}}')"#;
        let insert = parse_insert(sql);
        let err = rewrite_insert(insert).expect_err("expected error");
        assert!(err.message.contains("semver"), "{:#?}", err);
    }

    #[test]
    fn rewrite_ignores_other_schema_keys() {
        let sql = r#"INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) VALUES ('other_schema', '{"value":{"x-lix-key":"mock_schema","x-lix-version":"1.0.0"}}')"#;
        let insert = parse_insert(sql);
        let rewritten = rewrite_insert(insert).expect("rewrite ok");
        assert!(rewritten.is_none());
    }
}
