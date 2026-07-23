use std::sync::OnceLock;

use globset::{GlobBuilder, GlobMatcher};
use jsonschema::{Draft, JSONSchema};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::LixError;

static PLUGIN_MANIFEST_SCHEMA: OnceLock<JsonValue> = OnceLock::new();
static PLUGIN_MANIFEST_VALIDATOR: OnceLock<Result<JSONSchema, LixError>> = OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PluginRuntime {
    WasmComponentV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginManifest {
    pub key: String,
    pub runtime: PluginRuntime,
    pub api_version: String,
    #[serde(rename = "match")]
    pub file_match: PluginMatch,
    pub entry: String,
    pub schemas: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginMatch {
    pub path_glob: String,
    #[serde(default)]
    pub content_type: Option<PluginContentType>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginContentType {
    Text,
    Binary,
}

impl PluginContentType {
    /// Classifies file bytes only when a caller already has the payload.
    /// Empty bytes are valid UTF-8 and therefore text, matching the bundled
    /// text plugins' treatment of a newly-created empty file.
    pub(crate) fn from_bytes(bytes: &[u8]) -> Self {
        if std::str::from_utf8(bytes).is_ok() {
            Self::Text
        } else {
            Self::Binary
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedPluginManifest {
    pub manifest: PluginManifest,
    pub normalized_json: String,
}

pub fn parse_plugin_manifest_json(raw: &str) -> Result<ValidatedPluginManifest, LixError> {
    let manifest_json: JsonValue = serde_json::from_str(raw).map_err(|error| {
        LixError::new(
            LixError::CODE_INVALID_PLUGIN,
            format!("Plugin manifest must be valid JSON: {error}"),
        )
    })?;

    validate_plugin_manifest_json(&manifest_json)?;

    let manifest: PluginManifest =
        serde_json::from_value(manifest_json.clone()).map_err(|error| {
            LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                format!("Plugin manifest does not match expected shape: {error}"),
            )
        })?;
    compile_path_glob(&manifest.file_match.path_glob).map_err(|error| {
        LixError::new(
            LixError::CODE_INVALID_PLUGIN,
            format!(
                "Plugin manifest path_glob '{}' is invalid: {error}",
                manifest.file_match.path_glob
            ),
        )
    })?;
    let normalized_json = serde_json::to_string(&manifest_json).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("Failed to normalize plugin manifest JSON: {error}"),
        )
    })?;

    Ok(ValidatedPluginManifest {
        manifest,
        normalized_json,
    })
}

#[cfg(test)]
pub fn glob_matches_path(glob: &str, path: &str) -> bool {
    if glob.is_empty() || path.is_empty() {
        return false;
    }
    if is_catch_all_glob(glob) {
        return true;
    }

    compile_path_glob(glob)
        .map(|compiled| compiled.is_match(path))
        .unwrap_or(false)
}

fn compile_path_glob(glob: &str) -> Result<GlobMatcher, globset::Error> {
    GlobBuilder::new(glob)
        .literal_separator(false)
        .build()
        .map(|compiled| compiled.compile_matcher())
}

fn validate_plugin_manifest_json(manifest: &JsonValue) -> Result<(), LixError> {
    let validator = plugin_manifest_validator()?;
    if let Err(errors) = validator.validate(manifest) {
        let details = format_validation_errors(errors);
        return Err(LixError::new(
            LixError::CODE_INVALID_PLUGIN,
            format!("Invalid plugin manifest: {details}"),
        ));
    }
    Ok(())
}

#[cfg(test)]
fn is_catch_all_glob(glob: &str) -> bool {
    glob == "*" || glob == "**/*" || glob == "**"
}

fn plugin_manifest_validator() -> Result<&'static JSONSchema, LixError> {
    let result = PLUGIN_MANIFEST_VALIDATOR.get_or_init(|| {
        let mut options = JSONSchema::options();
        options.with_meta_schemas();
        if plugin_manifest_schema()
            .get("$schema")
            .and_then(JsonValue::as_str)
            .is_some_and(|url| url == "https://json-schema.org/draft/2020-12/schema")
        {
            options.with_draft(Draft::Draft202012);
        }

        options.compile(plugin_manifest_schema()).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("Failed to compile plugin manifest schema: {error}"),
            )
        })
    });

    match result {
        Ok(schema) => Ok(schema),
        Err(error) => Err(error.clone()),
    }
}

fn plugin_manifest_schema() -> &'static JsonValue {
    PLUGIN_MANIFEST_SCHEMA.get_or_init(|| {
        let raw = include_str!("./plugin_manifest.json");
        serde_json::from_str(raw).expect("plugin_manifest.json must be valid JSON")
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
    use crate::LixError;

    use super::{PluginContentType, PluginRuntime, glob_matches_path, parse_plugin_manifest_json};

    #[test]
    fn parses_valid_manifest() {
        let validated = parse_plugin_manifest_json(
            r#"{
                "key":"plugin_json",
                "runtime":"wasm-component-v1",
                "api_version":"0.1.0",
                "match":{"path_glob":"*.json"},
                "entry":"plugin.wasm",
                "schemas":["schema/default.json"]
            }"#,
        )
        .expect("manifest should parse");

        assert_eq!(validated.manifest.key, "plugin_json");
        assert_eq!(validated.manifest.runtime, PluginRuntime::WasmComponentV1);
        assert_eq!(validated.manifest.entry, "plugin.wasm");
    }

    #[test]
    fn rejects_invalid_manifest() {
        let err = parse_plugin_manifest_json(
            r#"{
                "runtime":"wasm-component-v1",
                "api_version":"0.1.0",
                "match":{"path_glob":"*.json"},
                "entry":"plugin.wasm",
                "schemas":["schema/default.json"]
            }"#,
        )
        .expect_err("manifest should be invalid");

        assert_eq!(err.code, LixError::CODE_INVALID_PLUGIN);
        assert!(err.message.contains("Invalid plugin manifest"));
        assert!(err.message.contains("key"));
    }

    #[test]
    fn rejects_invalid_path_glob() {
        let error = parse_plugin_manifest_json(
            r#"{
                "key":"plugin_markdown",
                "runtime":"wasm-component-v1",
                "api_version":"0.1.0",
                "match":{"path_glob":"*.{md,mdx"},
                "entry":"plugin.wasm",
                "schemas":["schema/default.json"]
            }"#,
        )
        .expect_err("invalid path glob should be rejected");

        assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
        assert!(error.message.contains("path_glob"));
    }

    #[test]
    fn enforces_manifest_work_bounds_at_the_boundary() {
        let max_glob = "a".repeat(1024);
        parse_plugin_manifest_json(&manifest_with(&max_glob, &["schema/default.json".into()]))
            .expect("the maximum glob length should be inclusive");

        let oversized_glob = "a".repeat(1025);
        let error = parse_plugin_manifest_json(&manifest_with(
            &oversized_glob,
            &["schema/default.json".into()],
        ))
        .expect_err("a glob over the work bound must be rejected");
        assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
        assert!(error.message.contains("path_glob"), "{error:?}");

        let max_schemas = (0..64)
            .map(|index| format!("schema/{index}.json"))
            .collect::<Vec<_>>();
        parse_plugin_manifest_json(&manifest_with("*.json", &max_schemas))
            .expect("the maximum schema count should be inclusive");

        let oversized_schemas = (0..65)
            .map(|index| format!("schema/{index}.json"))
            .collect::<Vec<_>>();
        let error = parse_plugin_manifest_json(&manifest_with("*.json", &oversized_schemas))
            .expect_err("a schema list over the work bound must be rejected");
        assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
        assert!(error.message.contains("schemas"), "{error:?}");
    }

    #[test]
    fn glob_matching_uses_manifest_and_path_text_verbatim() {
        assert!(glob_matches_path("*.md", "/docs/readme.md"));
        assert!(!glob_matches_path(" *.md", "/docs/readme.md"));
        assert!(!glob_matches_path("/docs/*.md", " /docs/readme.md"));
        assert!(!glob_matches_path("*.MD", "/docs/readme.md"));
    }

    #[test]
    fn parses_manifest_with_content_type_match_filter() {
        let validated = parse_plugin_manifest_json(
            r#"{
                "key":"plugin_text",
                "runtime":"wasm-component-v1",
                "api_version":"0.1.0",
                "match":{"path_glob":"**/*", "content_type":"text"},
                "entry":"plugin.wasm",
                "schemas":["schema/default.json"]
            }"#,
        )
        .expect("manifest should parse");

        assert_eq!(
            validated.manifest.file_match.content_type,
            Some(PluginContentType::Text)
        );
    }

    #[test]
    fn rejects_detect_changes_state_context_config() {
        let err = parse_plugin_manifest_json(
            r#"{
                "key":"plugin_markdown",
                "runtime":"wasm-component-v1",
                "api_version":"0.1.0",
                "match":{"path_glob":"*.{md,mdx}"},
                "entry":"plugin.wasm",
                "schemas":["schema/default.json"],
                "detect_changes": {
                    "state_context": {
                        "include_active_state": true,
                        "columns": ["entity_pk", "schema_key", "snapshot_content"]
                    }
                }
            }"#,
        )
        .expect_err("detect_changes state context config should be rejected");

        assert_eq!(err.code, LixError::CODE_INVALID_PLUGIN);
        assert!(err.message.contains("detect_changes"));
    }

    fn manifest_with(path_glob: &str, schemas: &[String]) -> String {
        serde_json::json!({
            "key": "plugin_bounds",
            "runtime": "wasm-component-v1",
            "api_version": "0.1.0",
            "match": { "path_glob": path_glob },
            "entry": "plugin.wasm",
            "schemas": schemas,
        })
        .to_string()
    }
}
