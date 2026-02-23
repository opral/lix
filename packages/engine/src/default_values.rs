use serde_json::{Map as JsonMap, Value as JsonValue};
use sqlparser::ast::Value as AstValue;
use sqlparser::ast::{Expr, Insert, ObjectName, ObjectNamePart, Statement, TableObject};

use crate::cel::CelEvaluator;
use crate::engine::sql2::ast::utils::{insert_values_rows_mut, resolve_insert_rows, ResolvedCell};
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::schema::{OverlaySchemaProvider, SchemaKey, SchemaProvider};
use crate::{LixBackend, LixError, Value};

const VTABLE_NAME: &str = "lix_internal_state_vtable";
const STORED_SCHEMA_KEY: &str = "lix_stored_schema";

pub async fn apply_vtable_insert_defaults<P>(
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

        if schema_key == STORED_SCHEMA_KEY {
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
                        message: format!(
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

fn apply_defaults_to_snapshot<P>(
    snapshot: &mut JsonMap<String, JsonValue>,
    schema: &JsonValue,
    context: &JsonMap<String, JsonValue>,
    evaluator: &CelEvaluator,
    functions: SharedFunctionProvider<P>,
    schema_key: &str,
    schema_version: &str,
) -> Result<bool, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let Some(properties) = schema.get("properties").and_then(|value| value.as_object()) else {
        return Ok(false);
    };
    let mut ordered_properties: Vec<(&String, &JsonValue)> = properties.iter().collect();
    ordered_properties.sort_by(|(left_name, _), (right_name, _)| left_name.cmp(right_name));

    let mut changed = false;
    for (field_name, field_schema) in ordered_properties {
        if snapshot.contains_key(field_name) {
            continue;
        }

        if let Some(expression) = field_schema
            .get("x-lix-default")
            .and_then(|value| value.as_str())
        {
            let value = evaluator
                .evaluate_with_functions(expression, context, functions.clone())
                .map_err(|err| LixError {
                    message: format!(
                        "failed to evaluate x-lix-default for '{}.{}' ({}): {}",
                        schema_key, field_name, schema_version, err.message
                    ),
                })?;
            snapshot.insert(field_name.clone(), value);
            changed = true;
            continue;
        }

        if let Some(default_value) = field_schema.get("default") {
            snapshot.insert(field_name.clone(), default_value.clone());
            changed = true;
        }
    }

    Ok(changed)
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

fn object_name_matches(name: &ObjectName, target: &str) -> bool {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.eq_ignore_ascii_case(target))
        .unwrap_or(false)
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
            message: format!("vtable insert snapshot_content invalid JSON: {err}"),
        }),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Map as JsonMap, Value as JsonValue};
    use sqlparser::ast::Value as AstValue;
    use sqlparser::ast::{Expr, SetExpr, Statement};

    use crate::cel::CelEvaluator;
    use crate::engine::sql2::ast::utils::parse_sql_statements;
    use crate::functions::{LixFunctionProvider, SharedFunctionProvider, SystemFunctionProvider};
    use crate::{LixBackend, LixError, QueryResult, SqlDialect, Value};

    use super::{apply_defaults_to_snapshot, apply_vtable_insert_defaults};

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
    }

    fn system_functions() -> SharedFunctionProvider<SystemFunctionProvider> {
        SharedFunctionProvider::new(SystemFunctionProvider)
    }

    #[test]
    fn applies_x_lix_default_for_missing_fields() {
        let evaluator = CelEvaluator::new();
        let schema = json!({
            "properties": {
                "slug": {
                    "type": "string",
                    "x-lix-default": "name + '-slug'"
                }
            }
        });
        let mut snapshot = JsonMap::new();
        snapshot.insert("name".to_string(), JsonValue::String("sample".to_string()));
        let context = snapshot.clone();

        let changed = apply_defaults_to_snapshot(
            &mut snapshot,
            &schema,
            &context,
            &evaluator,
            system_functions(),
            "test_schema",
            "1",
        )
        .expect("apply defaults");

        assert!(changed);
        assert_eq!(
            snapshot.get("slug"),
            Some(&JsonValue::String("sample-slug".to_string()))
        );
    }

    #[test]
    fn x_lix_default_overrides_json_default() {
        let evaluator = CelEvaluator::new();
        let schema = json!({
            "properties": {
                "status": {
                    "type": "string",
                    "default": "literal",
                    "x-lix-default": "'computed'"
                }
            }
        });
        let mut snapshot = JsonMap::new();
        let context = snapshot.clone();

        let changed = apply_defaults_to_snapshot(
            &mut snapshot,
            &schema,
            &context,
            &evaluator,
            system_functions(),
            "test_schema",
            "1",
        )
        .expect("apply defaults");

        assert!(changed);
        assert_eq!(
            snapshot.get("status"),
            Some(&JsonValue::String("computed".to_string()))
        );
    }

    #[test]
    fn does_not_default_explicit_null_values() {
        let evaluator = CelEvaluator::new();
        let schema = json!({
            "properties": {
                "status": {
                    "type": "string",
                    "x-lix-default": "'computed'"
                }
            }
        });
        let mut snapshot = JsonMap::new();
        snapshot.insert("status".to_string(), JsonValue::Null);
        let context = snapshot.clone();

        let changed = apply_defaults_to_snapshot(
            &mut snapshot,
            &schema,
            &context,
            &evaluator,
            system_functions(),
            "test_schema",
            "1",
        )
        .expect("apply defaults");

        assert!(!changed);
        assert_eq!(snapshot.get("status"), Some(&JsonValue::Null));
    }

    #[test]
    fn applies_cel_defaults_in_stable_sorted_field_order() {
        struct CountingFunctions {
            next: i64,
        }

        impl LixFunctionProvider for CountingFunctions {
            fn uuid_v7(&mut self) -> String {
                let current = self.next;
                self.next += 1;
                format!("uuid-{current}")
            }

            fn timestamp(&mut self) -> String {
                let current = self.next;
                self.next += 1;
                format!("ts-{current}")
            }
        }

        let evaluator = CelEvaluator::new();
        let schema = json!({
            "properties": {
                "z_uuid": {
                    "type": "string",
                    "x-lix-default": "lix_uuid_v7()"
                },
                "a_timestamp": {
                    "type": "string",
                    "x-lix-default": "lix_timestamp()"
                }
            }
        });
        let mut snapshot = JsonMap::new();
        let context = snapshot.clone();

        let changed = apply_defaults_to_snapshot(
            &mut snapshot,
            &schema,
            &context,
            &evaluator,
            SharedFunctionProvider::new(CountingFunctions { next: 0 }),
            "test_schema",
            "1",
        )
        .expect("apply defaults");

        assert!(changed);
        assert_eq!(
            snapshot.get("a_timestamp"),
            Some(&JsonValue::String("ts-0".to_string()))
        );
        assert_eq!(
            snapshot.get("z_uuid"),
            Some(&JsonValue::String("uuid-1".to_string()))
        );
    }

    #[tokio::test]
    async fn uses_pending_schema_when_stored_schema_insert_omits_schema_version_column() {
        let evaluator = CelEvaluator::new();
        let backend = UnexpectedBackendCall;
        let mut statements = parse_sql_statements(
            "INSERT INTO lix_internal_state_vtable (schema_key, snapshot_content) \
             VALUES ('lix_stored_schema', '{\"value\":{\"x-lix-key\":\"same_request_cel_default_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"},\"slug\":{\"type\":\"string\",\"x-lix-default\":\"name + ''-slug''\"}},\"required\":[\"name\"],\"additionalProperties\":false}}'); \
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
