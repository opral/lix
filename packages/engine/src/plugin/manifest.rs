use std::sync::OnceLock;

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

    let normalized_json = serde_json::to_string(&manifest_json).map_err(|error| LixError {
        message: format!("Failed to normalize plugin manifest JSON: {error}"),
    })?;

    Ok(ValidatedPluginManifest {
        manifest,
        normalized_json,
    })
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
}
