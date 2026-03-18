use serde_json::Value as JsonValue;
use sqlparser::ast::Value as AstValue;
use sqlparser::ast::{Expr, Insert, Statement, TableObject};

use crate::cel::CelEvaluator;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::schema::defaults::apply_defaults_to_snapshot;
use crate::schema::{OverlaySchemaProvider, SchemaKey, SchemaProvider};
use crate::sql::ast::utils::{insert_values_rows_mut, resolve_insert_rows, ResolvedCell};
use crate::sql::ast::walk::object_name_matches;
use crate::{LixBackend, LixError, Value};

const VTABLE_NAME: &str = "lix_internal_state_vtable";
const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";

pub(crate) async fn apply_vtable_insert_defaults<P>(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    statements: &mut [Statement],
    params: &[Value],
    functions: SharedFunctionProvider<P>,
) -> Result<(), LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let mut schema_provider = OverlaySchemaProvider::from_backend(backend);

    for statement in statements {
        let Statement::Insert(insert) = statement else {
            continue;
        };

        if !insert_targets_vtable(insert) {
            continue;
        }

        apply_statement_defaults(
            evaluator,
            insert,
            &mut schema_provider,
            params,
            functions.clone(),
        )
        .await?;
    }

    Ok(())
}

async fn apply_statement_defaults<P>(
    evaluator: &CelEvaluator,
    insert: &mut Insert,
    schema_provider: &mut OverlaySchemaProvider<'_>,
    params: &[Value],
    functions: SharedFunctionProvider<P>,
) -> Result<(), LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let schema_idx = match find_column_index(&insert.columns, "schema_key") {
        Some(index) => index,
        None => return Ok(()),
    };
    let schema_version_idx = find_column_index(&insert.columns, "schema_version");
    let snapshot_idx = match find_column_index(&insert.columns, "snapshot_content") {
        Some(index) => index,
        None => return Ok(()),
    };

    let Some(mut resolved_rows) = resolve_insert_rows(insert, params)? else {
        return Ok(());
    };
    let mut snapshot_updates: Vec<(usize, String)> = Vec::new();

    for (row_idx, resolved_row) in resolved_rows.iter_mut().enumerate() {
        let Some(schema_key_cell) = resolved_row.get(schema_idx) else {
            continue;
        };
        let Some(schema_key) = resolved_string(schema_key_cell) else {
            continue;
        };

        let snapshot = resolved_json(resolved_row.get(snapshot_idx))?;
        let Some(snapshot) = snapshot else {
            continue;
        };

        if schema_key == REGISTERED_SCHEMA_KEY {
            let _ = schema_provider.remember_pending_schema_from_snapshot(&snapshot);
            continue;
        }

        let Some(schema_version_idx) = schema_version_idx else {
            continue;
        };
        let Some(schema_version_cell) = resolved_row.get(schema_version_idx) else {
            continue;
        };
        let Some(schema_version) = resolved_string(schema_version_cell) else {
            continue;
        };

        let mut snapshot_object = match snapshot {
            JsonValue::Object(object) => object,
            _ => continue,
        };

        let context = snapshot_object.clone();
        let key = SchemaKey::new(schema_key.clone(), schema_version.clone());
        let schema = schema_provider.load_schema(&key).await?;

        if apply_defaults_to_snapshot(
            &mut snapshot_object,
            &schema,
            &context,
            evaluator,
            functions.clone(),
            &schema_key,
            &schema_version,
        )? {
            let serialized =
                serde_json::to_string(&JsonValue::Object(snapshot_object)).map_err(|err| {
                    LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        description: format!(
                            "failed to serialize snapshot_content for schema '{}' ({}): {err}",
                            schema_key, schema_version
                        ),
                    }
                })?;
            if let Some(snapshot_cell) = resolved_row.get_mut(snapshot_idx) {
                snapshot_cell.value = Some(Value::Text(serialized.clone()));
                snapshot_cell.placeholder_index = None;
                snapshot_updates.push((row_idx, serialized));
            }
        }
    }

    if snapshot_updates.is_empty() {
        return Ok(());
    }

    let Some(rows) = insert_values_rows_mut(insert) else {
        return Ok(());
    };

    for (row_idx, serialized) in snapshot_updates {
        let Some(row) = rows.get_mut(row_idx) else {
            continue;
        };
        if let Some(snapshot_expr) = row.get_mut(snapshot_idx) {
            *snapshot_expr = Expr::Value(AstValue::SingleQuotedString(serialized).into());
        }
    }

    Ok(())
}

fn insert_targets_vtable(insert: &Insert) -> bool {
    match &insert.table {
        TableObject::TableName(name) => object_name_matches(name, VTABLE_NAME),
        _ => false,
    }
}

fn find_column_index(columns: &[sqlparser::ast::Ident], target: &str) -> Option<usize> {
    columns
        .iter()
        .position(|column| column.value.eq_ignore_ascii_case(target))
}

fn resolved_string(cell: &ResolvedCell) -> Option<String> {
    match &cell.value {
        Some(Value::Text(value)) => Some(value.clone()),
        _ => None,
    }
}

fn resolved_json(cell: Option<&ResolvedCell>) -> Result<Option<JsonValue>, LixError> {
    let Some(cell) = cell else {
        return Ok(None);
    };
    match &cell.value {
        Some(Value::Null) => Ok(None),
        Some(Value::Text(raw)) => serde_json::from_str(raw).map(Some).map_err(|err| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("vtable insert snapshot_content invalid JSON: {err}"),
        }),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value as JsonValue;
    use sqlparser::ast::Value as AstValue;
    use sqlparser::ast::{Expr, SetExpr, Statement};

    use crate::cel::CelEvaluator;
    use crate::functions::{SharedFunctionProvider, SystemFunctionProvider};
    use crate::sql::ast::utils::parse_sql_statements;
    use crate::{QueryResult, SqlDialect};

    use super::apply_vtable_insert_defaults;
    use super::{LixBackend, LixError, Value};

    struct UnexpectedBackendCall;

    #[async_trait::async_trait(?Send)]
    impl LixBackend for UnexpectedBackendCall {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, _: &str, _: &[Value]) -> Result<QueryResult, LixError> {
            panic!("defaulting should resolve schema from pending in-request inserts")
        }

        async fn begin_transaction(&self) -> Result<Box<dyn crate::LixTransaction + '_>, LixError> {
            panic!("defaulting should not open transactions in this test backend")
        }

        async fn begin_savepoint(
            &self,
            _name: &str,
        ) -> Result<Box<dyn crate::LixTransaction + '_>, LixError> {
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "begin_savepoint not supported in test backend",
            ))
        }
    }

    fn system_functions() -> SharedFunctionProvider<SystemFunctionProvider> {
        SharedFunctionProvider::new(SystemFunctionProvider)
    }

    #[tokio::test]
    async fn uses_pending_schema_when_registered_schema_insert_omits_schema_version_column() {
        let evaluator = CelEvaluator::new();
        let backend = UnexpectedBackendCall;
        let mut statements = parse_sql_statements(
            "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) \
             VALUES ('lix_registered_schema', '{\"value\":{\"x-lix-key\":\"same_request_cel_default_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"},\"slug\":{\"type\":\"string\",\"x-lix-default\":\"name + ''-slug''\"}},\"required\":[\"name\"],\"additionalProperties\":false}}'); \
             INSERT INTO lix_internal_state_vtable (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version) \
             VALUES ('entity-1', 'same_request_cel_default_schema', 'file-1', 'version-1', 'lix', '{\"name\":\"Sample\"}', '1')",
        )
        .expect("parse SQL");

        apply_vtable_insert_defaults(
            &backend,
            &evaluator,
            &mut statements,
            &Vec::new(),
            system_functions(),
        )
        .await
        .expect("apply defaults");

        let Statement::Insert(insert) = &statements[1] else {
            panic!("expected second statement to be insert");
        };
        let snapshot_idx = insert
            .columns
            .iter()
            .position(|column| column.value.eq_ignore_ascii_case("snapshot_content"))
            .expect("snapshot_content column");
        let source = insert.source.as_ref().expect("values source");
        let SetExpr::Values(values) = source.body.as_ref() else {
            panic!("expected VALUES body");
        };
        let snapshot_expr = values.rows[0]
            .get(snapshot_idx)
            .expect("snapshot_content value");
        let Expr::Value(value) = snapshot_expr else {
            panic!("expected literal snapshot_content");
        };
        let AstValue::SingleQuotedString(serialized) = &value.value else {
            panic!("expected string snapshot_content");
        };
        let snapshot: JsonValue =
            serde_json::from_str(serialized).expect("snapshot_content must be valid JSON");
        assert_eq!(
            snapshot["slug"],
            JsonValue::String("Sample-slug".to_string())
        );
    }
}
