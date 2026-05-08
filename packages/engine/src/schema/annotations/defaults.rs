use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::cel::{CelEvaluator, CelFunctionProvider};
use crate::LixError;

pub(crate) fn apply_schema_defaults<P>(
    snapshot: &mut JsonMap<String, JsonValue>,
    schema: &JsonValue,
    evaluator: &CelEvaluator,
    functions: P,
    schema_key: &str,
) -> Result<bool, LixError>
where
    P: CelFunctionProvider,
{
    apply_schema_defaults_with_context(
        snapshot,
        schema,
        &snapshot.clone(),
        evaluator,
        functions,
        schema_key,
    )
}

pub(crate) fn apply_schema_defaults_with_shared_runtime<P>(
    snapshot: &mut JsonMap<String, JsonValue>,
    schema: &JsonValue,
    functions: P,
    schema_key: &str,
) -> Result<bool, LixError>
where
    P: CelFunctionProvider,
{
    apply_schema_defaults(
        snapshot,
        schema,
        crate::cel::shared_runtime(),
        functions,
        schema_key,
    )
}

pub(crate) fn apply_schema_defaults_with_context<P>(
    snapshot: &mut JsonMap<String, JsonValue>,
    schema: &JsonValue,
    context: &JsonMap<String, JsonValue>,
    evaluator: &CelEvaluator,
    functions: P,
    schema_key: &str,
) -> Result<bool, LixError>
where
    P: CelFunctionProvider,
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
                    message: format!(
                        "failed to evaluate x-lix-default for '{}.{}': {}",
                        schema_key, field_name, err.message
                    ),
                    hint: None,
                    details: None,
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

    use crate::cel::{CelEvaluator, CelFunctionProvider};

    use super::apply_schema_defaults_with_context;

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

        let changed = apply_schema_defaults_with_context(
            &mut snapshot,
            &schema,
            &context,
            &evaluator,
            fixed_functions(),
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

        let changed = apply_schema_defaults_with_context(
            &mut snapshot,
            &schema,
            &context,
            &evaluator,
            fixed_functions(),
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

        let changed = apply_schema_defaults_with_context(
            &mut snapshot,
            &schema,
            &context,
            &evaluator,
            fixed_functions(),
            "test_schema",
            "1",
        )
        .expect("apply defaults");

        assert!(!changed);
        assert_eq!(snapshot.get("status"), Some(&JsonValue::Null));
    }

    #[test]
    fn applies_cel_defaults_in_stable_sorted_field_order() {
        #[derive(Clone)]
        struct CountingFunctions {
            next: std::sync::Arc<std::sync::atomic::AtomicI64>,
        }

        impl CelFunctionProvider for CountingFunctions {
            fn call_uuid_v7(&self) -> String {
                let current = self.next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                format!("uuid-{current}")
            }

            fn call_timestamp(&self) -> String {
                let current = self.next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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

        let changed = apply_schema_defaults_with_context(
            &mut snapshot,
            &schema,
            &context,
            &evaluator,
            CountingFunctions {
                next: std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0)),
            },
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

    #[derive(Clone)]
    struct FixedFunctions;

    impl CelFunctionProvider for FixedFunctions {
        fn call_uuid_v7(&self) -> String {
            "uuid-fixed".to_string()
        }

        fn call_timestamp(&self) -> String {
            "1970-01-01T00:00:00.000Z".to_string()
        }
    }

    fn fixed_functions() -> FixedFunctions {
        FixedFunctions
    }
}
