use serde_json::Value as JsonValue;
use sqlparser::ast::{
    ConflictTarget, Expr, Ident, Insert, ObjectName, ObjectNamePart, OnConflict, OnConflictAction,
    OnInsert, Statement, TableObject, Value, ValueWithSpan,
};

use crate::schema::live_layout::{
    builtin_live_table_layout, live_table_layout_from_schema, normalized_live_column_values,
};
use crate::state::internal::{
    object_name_matches, MutationOperation, MutationRow, ResolvedCell, RowSourceResolver,
    SchemaLiveTableRequirement,
};
use crate::{CanonicalJson, LixError, Value as EngineValue};

const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";
const GLOBAL_VERSION: &str = "global";
const ENGINE_FILE_ID: &str = "lix";
const ENGINE_PLUGIN_KEY: &str = "lix";
const BOOTSTRAP_TABLE: &str = "lix_internal_registered_schema_bootstrap";
const MATERIALIZED_TABLE: &str = "lix_internal_live_v1_lix_registered_schema";

#[derive(Debug, Clone)]
pub struct RegisteredSchemaRewrite {
    pub statement: Statement,
    pub supplemental_statements: Vec<Statement>,
    pub live_table_requirement: SchemaLiveTableRequirement,
    pub mutation: MutationRow,
}

pub fn rewrite_insert(
    insert: Insert,
    params: &[EngineValue],
) -> Result<Option<RegisteredSchemaRewrite>, LixError> {
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
        .all(|row| resolved_value_equals(row.get(schema_key_index), REGISTERED_SCHEMA_KEY))
    {
        return Ok(None);
    }

    let snapshot_index = match find_column_index(&columns, "snapshot_content") {
        Some(index) => index,
        None => {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "registered schema insert requires snapshot_content".to_string(),
            })
        }
    };

    if rows.len() != 1 {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "registered schema insert supports a single row at a time".to_string(),
        });
    }

    let mut entity_id_value: Option<String> = None;
    let mut schema_key_value: Option<String> = None;
    let mut schema_version_value: Option<String> = None;
    let mut snapshot_literal_value: Option<String> = None;
    for (row_idx, row) in rows.iter().enumerate() {
        let snapshot_expr = row.get(snapshot_index).ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "registered schema insert missing snapshot_content value".to_string(),
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
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: "registered schema insert must use a single schema identity"
                        .to_string(),
                });
            }
        } else {
            entity_id_value = Some(derived_id);
        }
    }

    let entity_id = entity_id_value.ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: "registered schema insert requires schema identity".to_string(),
    })?;
    let schema_key_value = schema_key_value.ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: "registered schema insert requires schema key".to_string(),
    })?;
    let schema_version_value = schema_version_value.ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: "registered schema insert requires schema version".to_string(),
    })?;
    let snapshot_literal_value = snapshot_literal_value.ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: "registered schema insert requires snapshot_content".to_string(),
    })?;
    let snapshot_value: JsonValue =
        serde_json::from_str(&snapshot_literal_value).map_err(|err| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("registered schema snapshot_content must be valid JSON: {err}"),
        })?;
    let snapshot_literal_value = CanonicalJson::from_value(snapshot_value.clone())?.into_string();
    for row in &mut rows {
        row[snapshot_index] =
            engine_value_to_expr(&EngineValue::Text(snapshot_literal_value.clone()));
    }

    if let Some(entity_index) = find_column_index(&columns, "entity_id") {
        if !resolved_rows
            .iter()
            .all(|row| resolved_value_equals(row.get(entity_index), &entity_id))
        {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "registered schema insert entity_id must match schema key + version"
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

    // `lix_registered_schema` materialized storage does not expose `untracked`.
    drop_column_if_present(&mut columns, &mut rows, "untracked");

    let rewritten_rows = rows.clone();
    source.body = Box::new(sqlparser::ast::SetExpr::Values(sqlparser::ast::Values {
        explicit_row: values_layout.explicit_row,
        value_keyword: values_layout.value_keyword,
        rows: rewritten_rows,
    }));

    let rewritten = Insert {
        columns,
        source: Some(source),
        table: table_object(BOOTSTRAP_TABLE),
        on: Some(build_on_conflict_do_nothing()),
        ..insert
    };
    let mut mirrored_columns = rewritten.columns.clone();
    let mut mirrored_rows = rows;
    let layout = builtin_live_table_layout(REGISTERED_SCHEMA_KEY)?;
    drop_column_if_present(
        &mut mirrored_columns,
        &mut mirrored_rows,
        "snapshot_content",
    );
    if let Some(layout) = layout.as_ref() {
        let normalized_values =
            normalized_live_column_values(layout, Some(&snapshot_literal_value))?;
        for column in &layout.columns {
            mirrored_columns.push(Ident::new(column.column_name.clone()));
            let value = normalized_values
                .get(&column.column_name)
                .cloned()
                .unwrap_or(EngineValue::Null);
            for row in &mut mirrored_rows {
                row.push(engine_value_to_expr(&value));
            }
        }
    }
    let mirrored_source = sqlparser::ast::Query {
        body: Box::new(sqlparser::ast::SetExpr::Values(sqlparser::ast::Values {
            explicit_row: values_layout.explicit_row,
            value_keyword: values_layout.value_keyword,
            rows: mirrored_rows,
        })),
        ..(*rewritten
            .source
            .clone()
            .expect("registered schema rewrite must keep source"))
        .clone()
    };
    let mirrored = Insert {
        table: table_object(MATERIALIZED_TABLE),
        columns: mirrored_columns,
        source: Some(Box::new(mirrored_source)),
        ..rewritten.clone()
    };
    let live_layout =
        live_table_layout_from_schema(snapshot_value.get("value").ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "registered schema snapshot_content missing value".to_string(),
        })?)?;

    Ok(Some(RegisteredSchemaRewrite {
        statement: Statement::Insert(rewritten),
        supplemental_statements: vec![Statement::Insert(mirrored)],
        live_table_requirement: SchemaLiveTableRequirement {
            schema_key: schema_key_value,
            layout: Some(live_layout),
        },
        mutation: MutationRow {
            operation: MutationOperation::Insert,
            entity_id,
            schema_key: REGISTERED_SCHEMA_KEY.to_string(),
            schema_version: schema_version_value,
            file_id: ENGINE_FILE_ID.to_string(),
            version_id: GLOBAL_VERSION.to_string(),
            plugin_key: ENGINE_PLUGIN_KEY.to_string(),
            snapshot_content: Some(snapshot_value),
            untracked: false,
        },
    }))
}

fn build_on_conflict_do_nothing() -> OnInsert {
    OnInsert::OnConflict(OnConflict {
        conflict_target: Some(ConflictTarget::Columns(vec![
            Ident::new("entity_id"),
            Ident::new("file_id"),
            Ident::new("version_id"),
        ])),
        action: OnConflictAction::DoNothing,
    })
}

fn table_object(table_name: &str) -> TableObject {
    TableObject::TableName(ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
        table_name,
    ))]))
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

fn drop_column_if_present(columns: &mut Vec<Ident>, rows: &mut Vec<Vec<Expr>>, name: &str) {
    let Some(index) = find_column_index(columns, name) else {
        return;
    };
    columns.remove(index);
    for row in rows.iter_mut() {
        if index < row.len() {
            row.remove(index);
        }
    }
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
        EngineValue::Boolean(value) => Expr::Value(ValueWithSpan::from(Value::Boolean(*value))),
        EngineValue::Text(value) => Expr::Value(ValueWithSpan::from(Value::SingleQuotedString(
            value.clone(),
        ))),
        EngineValue::Json(value) => Expr::Value(ValueWithSpan::from(Value::SingleQuotedString(
            value.to_string(),
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
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!("registered schema insert requires {name} = '{value}'"),
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
        _ => Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), description:
                "registered schema insert requires literal snapshot_content (prepared statement support TODO)"
                    .to_string(),
        }),
    }
}

fn parse_schema_identity(snapshot: &str) -> Result<(String, String), LixError> {
    let parsed: JsonValue = serde_json::from_str(snapshot).map_err(|err| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("registered schema snapshot_content must be valid JSON: {err}"),
    })?;
    let value = parsed.get("value").ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: "registered schema snapshot_content must contain value".to_string(),
    })?;
    let obj = value.as_object().ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: "registered schema value must be an object".to_string(),
    })?;
    let schema_key = obj
        .get("x-lix-key")
        .and_then(|v| v.as_str())
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "registered schema value.x-lix-key must be string".to_string(),
        })?;
    let schema_version = obj
        .get("x-lix-version")
        .and_then(|v| v.as_str())
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "registered schema value.x-lix-version must be string".to_string(),
        })?;

    // Deliberately keep x-lix-version as a monotonic integer (string) so we can evolve
    // translation rules later without locking into semver semantics.
    ensure_monotonic_version(schema_version)?;

    Ok((schema_key.to_string(), schema_version.to_string()))
}

fn ensure_monotonic_version(version: &str) -> Result<(), LixError> {
    if version.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "registered schema x-lix-version must be a monotonic integer".to_string(),
        });
    }
    let mut chars = version.chars();
    let Some(first) = chars.next() else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "registered schema x-lix-version must be a monotonic integer".to_string(),
        });
    };
    if first == '0' || !first.is_ascii_digit() || !chars.all(|c| c.is_ascii_digit()) {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description:
                "registered schema x-lix-version must be a monotonic integer without leading zeros"
                    .to_string(),
        });
    }
    Ok(())
}
