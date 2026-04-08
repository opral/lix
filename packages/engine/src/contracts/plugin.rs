use std::sync::OnceLock;

use globset::{Glob, GlobBuilder};
use jsonschema::JSONSchema;
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

#[allow(dead_code)]
impl PluginRuntime {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WasmComponentV1 => "wasm-component-v1",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "wasm-component-v1" => Some(Self::WasmComponentV1),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginManifest {
    pub key: String,
    pub runtime: PluginRuntime,
    pub api_version: String,
    #[serde(rename = "match")]
    pub file_match: PluginMatch,
    #[serde(default)]
    pub detect_changes: Option<DetectChangesConfig>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedPluginManifest {
    pub manifest: PluginManifest,
    pub normalized_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstalledPlugin {
    pub key: String,
    pub runtime: PluginRuntime,
    pub api_version: String,
    pub path_glob: String,
    pub content_type: Option<PluginContentType>,
    pub entry: String,
    pub manifest_json: String,
    pub wasm: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DetectChangesConfig {
    #[serde(default)]
    pub state_context: Option<DetectStateContextConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DetectStateContextConfig {
    #[serde(default)]
    pub include_active_state: Option<bool>,
    #[serde(default)]
    pub columns: Option<Vec<StateContextColumn>>,
}

#[allow(dead_code)]
impl DetectStateContextConfig {
    pub fn includes_active_state(&self) -> bool {
        self.include_active_state.unwrap_or(false)
    }

    pub fn resolved_columns_or_default(&self) -> Option<Vec<StateContextColumn>> {
        if !self.includes_active_state() {
            return None;
        }
        Some(
            self.columns
                .clone()
                .unwrap_or_else(|| StateContextColumn::default_active_state_columns().to_vec()),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateContextColumn {
    EntityId,
    SchemaKey,
    SchemaVersion,
    SnapshotContent,
    FileId,
    PluginKey,
    VersionId,
    ChangeId,
    Metadata,
    CreatedAt,
    UpdatedAt,
}

#[allow(dead_code)]
impl StateContextColumn {
    pub const fn default_active_state_columns() -> &'static [StateContextColumn] {
        &[
            StateContextColumn::EntityId,
            StateContextColumn::SchemaKey,
            StateContextColumn::SchemaVersion,
            StateContextColumn::SnapshotContent,
        ]
    }
}

pub(crate) const PLUGIN_STORAGE_ROOT_DIRECTORY_PATH: &str = "/.lix/plugins/";
pub(crate) const PLUGIN_ARCHIVE_FILE_EXTENSION: &str = ".lixplugin";

pub(crate) fn plugin_storage_archive_file_id(plugin_key: &str) -> String {
    format!("lix_plugin_archive::{plugin_key}")
}

pub(crate) fn plugin_storage_archive_path(plugin_key: &str) -> Result<String, LixError> {
    validate_plugin_key_segment(plugin_key)?;
    Ok(format!(
        "{PLUGIN_STORAGE_ROOT_DIRECTORY_PATH}{plugin_key}{PLUGIN_ARCHIVE_FILE_EXTENSION}"
    ))
}

pub(crate) fn plugin_key_from_archive_path(path: &str) -> Option<String> {
    let file_name = path.strip_prefix(PLUGIN_STORAGE_ROOT_DIRECTORY_PATH)?;
    let plugin_key = file_name.strip_suffix(PLUGIN_ARCHIVE_FILE_EXTENSION)?;
    if plugin_key.is_empty()
        || plugin_key == "."
        || plugin_key == ".."
        || plugin_key.contains('/')
        || plugin_key.contains('\\')
    {
        return None;
    }
    Some(plugin_key.to_string())
}

pub(crate) fn parse_plugin_manifest_json(raw: &str) -> Result<ValidatedPluginManifest, LixError> {
    let manifest_json: JsonValue = serde_json::from_str(raw).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("Plugin manifest must be valid JSON: {error}"),
    })?;

    validate_plugin_manifest_json(&manifest_json)?;

    let manifest: PluginManifest =
        serde_json::from_value(manifest_json.clone()).map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("Plugin manifest does not match expected shape: {error}"),
        })?;
    validate_path_glob(&manifest.file_match.path_glob)?;

    let normalized_json = serde_json::to_string(&manifest_json).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("Failed to normalize plugin manifest JSON: {error}"),
    })?;

    Ok(ValidatedPluginManifest {
        manifest,
        normalized_json,
    })
}

pub(crate) fn select_best_glob_match<'a, T, C: Copy + PartialEq>(
    path: &str,
    file_content_type: Option<C>,
    candidates: &'a [T],
    glob: impl Fn(&T) -> &str,
    required_content_type: impl Fn(&T) -> Option<C>,
) -> Option<&'a T> {
    let mut selected: Option<&T> = None;
    let mut selected_rank: Option<(u8, i32)> = None;

    for candidate in candidates {
        let pattern = glob(candidate);
        if !glob_matches_path(pattern, path) {
            continue;
        }
        if let (Some(actual_type), Some(required_type)) =
            (file_content_type, required_content_type(candidate))
        {
            if actual_type != required_type {
                continue;
            }
        }

        let rank = glob_specificity_rank(pattern);
        match selected_rank {
            None => {
                selected = Some(candidate);
                selected_rank = Some(rank);
            }
            Some(existing_rank) if rank > existing_rank => {
                selected = Some(candidate);
                selected_rank = Some(rank);
            }
            _ => {
                // Keep the existing winner on equal rank to preserve candidate-order tie-break.
            }
        }
    }

    selected
}

pub(crate) fn glob_matches_path(glob: &str, path: &str) -> bool {
    let normalized_glob = glob.trim();
    let normalized_path = path.trim();
    if normalized_glob.is_empty() || normalized_path.is_empty() {
        return false;
    }
    if is_catch_all_glob(normalized_glob) {
        return true;
    }

    GlobBuilder::new(normalized_glob)
        .literal_separator(false)
        .case_insensitive(true)
        .build()
        .map(|compiled| compiled.compile_matcher().is_match(normalized_path))
        .unwrap_or(false)
}

fn validate_path_glob(glob: &str) -> Result<(), LixError> {
    Glob::new(glob).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("Invalid plugin manifest: match.path_glob is invalid: {error}"),
    })?;
    Ok(())
}

fn validate_plugin_manifest_json(manifest: &JsonValue) -> Result<(), LixError> {
    let validator = plugin_manifest_validator()?;
    if let Err(errors) = validator.validate(manifest) {
        let details = format_validation_errors(errors);
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("Invalid plugin manifest: {details}"),
        });
    }
    Ok(())
}

fn glob_specificity_rank(glob: &str) -> (u8, i32) {
    let normalized = glob.trim();
    if is_catch_all_glob(normalized) {
        return (0, i32::MIN);
    }
    (1, glob_specificity_score(normalized))
}

fn glob_specificity_score(glob: &str) -> i32 {
    let mut literal_chars = 0i32;
    let mut wildcard_chars = 0i32;
    for ch in glob.chars() {
        match ch {
            '*' | '?' | '[' | ']' | '{' | '}' => wildcard_chars += 1,
            _ => literal_chars += 1,
        }
    }
    literal_chars - wildcard_chars
}

fn is_catch_all_glob(glob: &str) -> bool {
    glob == "*" || glob == "**/*" || glob == "**"
}

fn plugin_manifest_validator() -> Result<&'static JSONSchema, LixError> {
    let result = PLUGIN_MANIFEST_VALIDATOR.get_or_init(|| {
        JSONSchema::options()
            .with_meta_schemas()
            .compile(plugin_manifest_schema())
            .map_err(|error| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!("Failed to compile plugin manifest schema: {error}"),
            })
    });

    match result {
        Ok(schema) => Ok(schema),
        Err(error) => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: error.description.clone(),
        }),
    }
}

fn plugin_manifest_schema() -> &'static JsonValue {
    PLUGIN_MANIFEST_SCHEMA.get_or_init(|| {
        let raw = include_str!("../runtime/plugin/manifest.schema.json");
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

fn validate_plugin_key_segment(plugin_key: &str) -> Result<(), LixError> {
    if plugin_key.is_empty()
        || plugin_key == "."
        || plugin_key == ".."
        || plugin_key.contains('/')
        || plugin_key.contains('\\')
    {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "plugin key '{}' must be a single relative path segment",
                plugin_key
            ),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        parse_plugin_manifest_json, plugin_key_from_archive_path, plugin_storage_archive_path,
        DetectStateContextConfig, PluginContentType, StateContextColumn,
    };

    #[test]
    fn resolved_columns_returns_none_when_active_state_is_not_enabled() {
        let config = DetectStateContextConfig {
            include_active_state: None,
            columns: None,
        };

        assert_eq!(config.resolved_columns_or_default(), None);
    }

    #[test]
    fn resolved_columns_uses_defaults_when_columns_are_omitted() {
        let config = DetectStateContextConfig {
            include_active_state: Some(true),
            columns: None,
        };

        assert_eq!(
            config.resolved_columns_or_default(),
            Some(StateContextColumn::default_active_state_columns().to_vec())
        );
    }

    #[test]
    fn resolved_columns_uses_explicit_column_selection() {
        let config = DetectStateContextConfig {
            include_active_state: Some(true),
            columns: Some(vec![
                StateContextColumn::EntityId,
                StateContextColumn::SchemaKey,
            ]),
        };

        assert_eq!(
            config.resolved_columns_or_default(),
            Some(vec![
                StateContextColumn::EntityId,
                StateContextColumn::SchemaKey
            ])
        );
    }

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
        assert_eq!(validated.manifest.runtime.as_str(), "wasm-component-v1");
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

        assert!(err.description.contains("Invalid plugin manifest"));
        assert!(err.description.contains("key"));
    }

    #[test]
    fn rejects_invalid_path_glob() {
        let err = parse_plugin_manifest_json(
            r#"{
                "key":"plugin_markdown",
                "runtime":"wasm-component-v1",
                "api_version":"0.1.0",
                "match":{"path_glob":"*.{md,mdx"},
                "entry":"plugin.wasm",
                "schemas":["schema/default.json"]
            }"#,
        )
        .expect_err("invalid glob should fail");

        assert!(err.description.contains("match.path_glob"));
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
    fn parses_manifest_with_active_state_columns() {
        let validated = parse_plugin_manifest_json(
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
                "match":{"path_glob":"*.md"},
                "entry":"plugin.wasm",
                "schemas":["schema/default.json"],
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

        assert_eq!(
            state_context.resolved_columns_or_default(),
            Some(StateContextColumn::default_active_state_columns().to_vec())
        );
    }

    #[test]
    fn computes_storage_archive_paths() {
        assert_eq!(
            plugin_storage_archive_path("plugin_json").expect("path should build"),
            "/.lix/plugins/plugin_json.lixplugin"
        );
    }

    #[test]
    fn extracts_plugin_key_from_storage_path() {
        assert_eq!(
            plugin_key_from_archive_path("/.lix/plugins/plugin_json.lixplugin"),
            Some("plugin_json".to_string())
        );
        assert_eq!(
            plugin_key_from_archive_path("/.lix/plugins/nested/plugin.lixplugin"),
            None
        );
    }
}
