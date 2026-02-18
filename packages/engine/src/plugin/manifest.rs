use std::sync::OnceLock;

use globset::Glob;
use jsonschema::JSONSchema;
use serde_json::Value as JsonValue;

use crate::plugin::types::{PluginManifest, ValidatedPluginManifest};
use crate::LixError;

static PLUGIN_MANIFEST_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static PLUGIN_MANIFEST_VALIDATOR: OnceLock<Result<JSONSchema, LixError>> = OnceLock::new();

pub(crate) fn parse_plugin_manifest_json(raw: &str) -> Result<ValidatedPluginManifest, LixError> {
    let manifest_json: JsonValue = serde_json::from_str(raw).map_err(|error| LixError {
        message: format!("Plugin manifest must be valid JSON: {error}"),
    })?;

    validate_plugin_manifest_json(&manifest_json)?;

    let manifest: PluginManifest =
        serde_json::from_value(manifest_json.clone()).map_err(|error| LixError {
            message: format!("Plugin manifest does not match expected shape: {error}"),
        })?;
    validate_detect_changes_glob(&manifest.detect_changes_glob)?;

    let normalized_json = serde_json::to_string(&manifest_json).map_err(|error| LixError {
        message: format!("Failed to normalize plugin manifest JSON: {error}"),
    })?;

    Ok(ValidatedPluginManifest {
        manifest,
        normalized_json,
    })
}

fn validate_detect_changes_glob(glob: &str) -> Result<(), LixError> {
    Glob::new(glob).map_err(|error| LixError {
        message: format!("Invalid plugin manifest: detect_changes_glob is invalid: {error}"),
    })?;
    Ok(())
}

fn validate_plugin_manifest_json(manifest: &JsonValue) -> Result<(), LixError> {
    let validator = plugin_manifest_validator()?;
    if let Err(errors) = validator.validate(manifest) {
        let details = format_validation_errors(errors);
        return Err(LixError {
            message: format!("Invalid plugin manifest: {details}"),
        });
    }
    Ok(())
}

fn plugin_manifest_validator() -> Result<&'static JSONSchema, LixError> {
    let result = PLUGIN_MANIFEST_VALIDATOR.get_or_init(|| {
        JSONSchema::options()
            .with_meta_schemas()
            .compile(plugin_manifest_schema())
            .map_err(|error| LixError {
                message: format!("Failed to compile plugin manifest schema: {error}"),
            })
    });

    match result {
        Ok(schema) => Ok(schema),
        Err(error) => Err(LixError {
            message: error.message.clone(),
        }),
    }
}

fn plugin_manifest_schema() -> &'static JsonValue {
    PLUGIN_MANIFEST_SCHEMA.get_or_init(|| {
        let raw = include_str!("manifest.schema.json");
        serde_json::from_str(raw).expect("manifest.schema.json must be valid JSON")
    })
}

fn format_validation_errors<'a>(
    errors: impl Iterator<Item = jsonschema::ValidationError<'a>>,
) -> String {
    let mut parts = Vec::new();
    for error in errors {
        let path = error.instance_path.to_string();
        let message = error.to_string();
        if path.is_empty() {
            parts.push(message);
        } else {
            parts.push(format!("{path} {message}"));
        }
    }
    if parts.is_empty() {
        "Unknown validation error".to_string()
    } else {
        parts.join("; ")
    }
}

#[cfg(test)]
mod tests {
    use super::parse_plugin_manifest_json;
    use crate::plugin::types::StateContextColumn;

    #[test]
    fn parses_valid_manifest() {
        let validated = parse_plugin_manifest_json(
            r#"{
                "key":"plugin_json",
                "runtime":"wasm-component-v1",
                "api_version":"0.1.0",
                "detect_changes_glob":"*.json"
            }"#,
        )
        .expect("manifest should parse");

        assert_eq!(validated.manifest.key, "plugin_json");
        assert_eq!(validated.manifest.runtime.as_str(), "wasm-component-v1");
        assert_eq!(validated.manifest.entry_or_default(), "plugin.wasm");
    }

    #[test]
    fn rejects_invalid_manifest() {
        let err = parse_plugin_manifest_json(
            r#"{
                "runtime":"wasm-component-v1",
                "api_version":"0.1.0",
                "detect_changes_glob":"*.json"
            }"#,
        )
        .expect_err("manifest should be invalid");

        assert!(err.message.contains("Invalid plugin manifest"));
        assert!(err.message.contains("key"));
    }

    #[test]
    fn rejects_invalid_detect_changes_glob() {
        let err = parse_plugin_manifest_json(
            r#"{
                "key":"plugin_markdown",
                "runtime":"wasm-component-v1",
                "api_version":"0.1.0",
                "detect_changes_glob":"*.{md,mdx"
            }"#,
        )
        .expect_err("invalid glob should fail");

        assert!(err.message.contains("detect_changes_glob"));
    }

    #[test]
    fn parses_manifest_with_active_state_columns() {
        let validated = parse_plugin_manifest_json(
            r#"{
                "key":"plugin_markdown",
                "runtime":"wasm-component-v1",
                "api_version":"0.1.0",
                "detect_changes_glob":"*.{md,mdx}",
                "detect_changes": {
                    "state_context": {
                        "include_active_state": true,
                        "columns": ["entity_id", "schema_key", "snapshot_content"]
                    }
                }
            }"#,
        )
        .expect("manifest should parse");

        let state_context = validated
            .manifest
            .detect_changes
            .expect("detect_changes should be present")
            .state_context
            .expect("state_context should be present");

        assert_eq!(state_context.include_active_state, Some(true));
        assert_eq!(
            state_context.columns,
            Some(vec![
                StateContextColumn::EntityId,
                StateContextColumn::SchemaKey,
                StateContextColumn::SnapshotContent
            ])
        );
    }

    #[test]
    fn parses_manifest_with_active_state_and_default_columns() {
        let validated = parse_plugin_manifest_json(
            r#"{
                "key":"plugin_markdown",
                "runtime":"wasm-component-v1",
                "api_version":"0.1.0",
                "detect_changes_glob":"*.md",
                "detect_changes": {
                    "state_context": {
                        "include_active_state": true
                    }
                }
            }"#,
        )
        .expect("manifest should parse");

        let state_context = validated
            .manifest
            .detect_changes
            .expect("detect_changes should be present")
            .state_context
            .expect("state_context should be present");

        assert_eq!(state_context.include_active_state, Some(true));
        assert_eq!(state_context.columns, None);
    }

    #[test]
    fn rejects_state_columns_when_include_active_state_is_missing() {
        let err = parse_plugin_manifest_json(
            r#"{
                "key":"plugin_markdown",
                "runtime":"wasm-component-v1",
                "api_version":"0.1.0",
                "detect_changes_glob":"*.md",
                "detect_changes": {
                    "state_context": {
                        "columns": ["entity_id", "schema_key"]
                    }
                }
            }"#,
        )
        .expect_err("manifest should be invalid");

        assert!(err.message.contains("detect_changes/state_context"));
        assert!(err.message.contains("columns"));
    }

    #[test]
    fn rejects_state_columns_when_include_active_state_is_false() {
        let err = parse_plugin_manifest_json(
            r#"{
                "key":"plugin_markdown",
                "runtime":"wasm-component-v1",
                "api_version":"0.1.0",
                "detect_changes_glob":"*.md",
                "detect_changes": {
                    "state_context": {
                        "include_active_state": false,
                        "columns": ["entity_id", "schema_key"]
                    }
                }
            }"#,
        )
        .expect_err("manifest should be invalid");

        assert!(err.message.contains("detect_changes/state_context"));
        assert!(err.message.contains("columns"));
    }

    #[test]
    fn rejects_state_columns_without_entity_id() {
        let err = parse_plugin_manifest_json(
            r#"{
                "key":"plugin_markdown",
                "runtime":"wasm-component-v1",
                "api_version":"0.1.0",
                "detect_changes_glob":"*.md",
                "detect_changes": {
                    "state_context": {
                        "include_active_state": true,
                        "columns": ["schema_key", "snapshot_content"]
                    }
                }
            }"#,
        )
        .expect_err("manifest should be invalid");

        assert!(err.message.contains("columns"));
        assert!(err.message.contains("Invalid plugin manifest"));
    }

    #[test]
    fn rejects_unknown_state_column() {
        let err = parse_plugin_manifest_json(
            r#"{
                "key":"plugin_markdown",
                "runtime":"wasm-component-v1",
                "api_version":"0.1.0",
                "detect_changes_glob":"*.md",
                "detect_changes": {
                    "state_context": {
                        "include_active_state": true,
                        "columns": ["entity_id", "unknown_column"]
                    }
                }
            }"#,
        )
        .expect_err("manifest should be invalid");

        assert!(err.message.contains("unknown_column"));
    }
}
