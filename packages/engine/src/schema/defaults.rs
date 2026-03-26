use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::cel::CelEvaluator;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::LixError;

pub(crate) fn apply_schema_defaults_with_functions<P>(
    snapshot: &mut JsonMap<String, JsonValue>,
    schema: &JsonValue,
    functions: SharedFunctionProvider<P>,
    schema_key: &str,
    schema_version: &str,
) -> Result<bool, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    apply_defaults_to_snapshot(
        snapshot,
        schema,
        &snapshot.clone(),
        &CelEvaluator::new(),
        functions,
        schema_key,
        schema_version,
    )
}

pub(crate) fn apply_defaults_to_snapshot<P>(
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
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!(
                        "failed to evaluate x-lix-default for '{}.{}' ({}): {}",
                        schema_key, field_name, schema_version, err.description
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

#[cfg(test)]
mod tests {
    use serde_json::{json, Map as JsonMap, Value as JsonValue};

    use crate::cel::CelEvaluator;
    use crate::functions::{LixFunctionProvider, SharedFunctionProvider, SystemFunctionProvider};

    use super::apply_defaults_to_snapshot;

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
}
