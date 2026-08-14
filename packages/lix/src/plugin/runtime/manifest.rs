use std::collections::BTreeSet;

use globset::{GlobBuilder, GlobMatcher};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::LixError;
use crate::plugin::runtime::WASM_COMPONENT_API_VERSION;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PluginRuntime {
    WasmComponent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginManifest {
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_match: Option<PluginMatch>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry: Option<String>,
    pub schemas: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginMatch {
    pub path_glob: String,
    #[serde(default, rename = "content")]
    pub content: Option<PluginContentMatcher>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginContentMatcher {
    /// A complete UTF-8 payload. Existing format plugins use this stricter
    /// contract because their parsers consume Unicode text.
    Text,
    /// A payload that is not valid UTF-8.
    Binary,
    /// A bounded, format-neutral byte predicate.
    PrefixExcludes { byte: u8, bytes: usize },
}

impl PluginContentMatcher {
    /// Returns whether a payload satisfies this matcher contract.
    ///
    pub(crate) fn matches_bytes(self, bytes: &[u8]) -> bool {
        match self {
            Self::Text => std::str::from_utf8(bytes).is_ok(),
            Self::Binary => std::str::from_utf8(bytes).is_err(),
            Self::PrefixExcludes {
                byte,
                bytes: scan_bytes,
            } => !bytes.iter().take(scan_bytes).any(|value| *value == byte),
        }
    }
}

/// Validates the resolved durable ABI. Author manifests do not repeat these
/// constants; the component package is canonically `lix:plugin@1.0.0`.
pub(crate) fn validate_runtime_api_version(
    runtime: PluginRuntime,
    api_version: &str,
) -> Result<(), LixError> {
    if runtime != PluginRuntime::WasmComponent || api_version != WASM_COMPONENT_API_VERSION {
        return Err(LixError::new(
            LixError::CODE_INVALID_PLUGIN,
            format!("plugin component must use lix:plugin@{WASM_COMPONENT_API_VERSION}"),
        ));
    }
    Ok(())
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

    let manifest: PluginManifest =
        serde_json::from_value(manifest_json.clone()).map_err(|error| {
            LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                format!("Invalid plugin manifest: {error}"),
            )
        })?;
    validate_plugin_manifest(&manifest)?;
    if let Some(file_match) = &manifest.file_match {
        compile_path_glob(&file_match.path_glob).map_err(|error| {
            LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                format!(
                    "Plugin manifest path_glob '{}' is invalid: {error}",
                    file_match.path_glob
                ),
            )
        })?;
    }
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

fn validate_plugin_manifest(manifest: &PluginManifest) -> Result<(), LixError> {
    let valid_key = (1..=128).contains(&manifest.key.len())
        && manifest
            .key
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_lowercase)
        && manifest.key.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"_-".contains(&byte)
        });
    if !valid_key {
        return invalid_manifest("key must match ^[a-z][a-z0-9_-]*$ and contain at most 128 bytes");
    }
    if let Some(file_match) = &manifest.file_match {
        if !(1..=1024).contains(&file_match.path_glob.len()) {
            return invalid_manifest("file_match.path_glob must contain between 1 and 1024 bytes");
        }
        if let Some(PluginContentMatcher::PrefixExcludes { bytes, .. }) = file_match.content
            && !(1..=16_777_216).contains(&bytes)
        {
            return invalid_manifest("file_match.content.prefix_excludes.bytes is out of range");
        }
    }
    if let Some(entry) = &manifest.entry
        && !(1..=512).contains(&entry.len())
    {
        return invalid_manifest("entry must contain between 1 and 512 bytes");
    }
    if manifest.file_match.is_some() && manifest.entry.is_none() {
        return invalid_manifest("file_match requires entry");
    }
    if !(1..=64).contains(&manifest.schemas.len()) {
        return invalid_manifest("schemas must contain between 1 and 64 entries");
    }
    let mut schemas = BTreeSet::new();
    for schema in &manifest.schemas {
        if !(1..=512).contains(&schema.len()) {
            return invalid_manifest("each schemas entry must contain between 1 and 512 bytes");
        }
        if !schemas.insert(schema) {
            return invalid_manifest("schemas entries must be unique");
        }
    }
    Ok(())
}

fn invalid_manifest<T>(message: &str) -> Result<T, LixError> {
    Err(LixError::new(
        LixError::CODE_INVALID_PLUGIN,
        format!("Invalid plugin manifest: {message}"),
    ))
}

#[cfg(test)]
fn is_catch_all_glob(glob: &str) -> bool {
    glob == "*" || glob == "**/*" || glob == "**"
}

#[cfg(test)]
mod tests {
    use crate::LixError;

    use super::{PluginContentMatcher, glob_matches_path, parse_plugin_manifest_json};

    #[test]
    fn parses_valid_manifest() {
        let validated = parse_plugin_manifest_json(
            r#"{
                "key":"plugin_json",
                "file_match":{"path_glob":"*.json"},
                "entry":"plugin.wasm",
                "schemas":["schema/default.json"]
            }"#,
        )
        .expect("manifest should parse");

        assert_eq!(validated.manifest.key, "plugin_json");
        assert_eq!(validated.manifest.entry.as_deref(), Some("plugin.wasm"));
    }

    #[test]
    fn parses_schema_only_manifest() {
        let validated = parse_plugin_manifest_json(
            r#"{
                "key":"plugin_notes",
                "schemas":["schema/note.json"]
            }"#,
        )
        .expect("schema-only manifest should parse");

        assert_eq!(validated.manifest.entry, None);
        assert_eq!(validated.manifest.file_match, None);
    }

    #[test]
    fn parses_row_only_component_manifest() {
        let validated = parse_plugin_manifest_json(
            r#"{
                "key":"plugin_notes",
                "entry":"plugin.wasm",
                "schemas":["schema/note.json"]
            }"#,
        )
        .expect("row-only component manifest should parse");

        assert_eq!(validated.manifest.entry.as_deref(), Some("plugin.wasm"));
        assert_eq!(validated.manifest.file_match, None);
    }

    #[test]
    fn rejects_file_match_without_entry() {
        let error = parse_plugin_manifest_json(
            r#"{
                "key":"plugin_notes",
                "file_match":{"path_glob":"*.notes"},
                "schemas":["schema/note.json"]
            }"#,
        )
        .expect_err("file projection requires an executable component");

        assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
        assert!(error.message.contains("file_match requires entry"));
    }

    #[test]
    fn rejects_legacy_match_field() {
        let error = parse_plugin_manifest_json(
            r#"{
                "key":"plugin_json",
                "match":{"path_glob":"*.json"},
                "entry":"plugin.wasm",
                "schemas":["schema/default.json"]
            }"#,
        )
        .expect_err("the hard cut must reject the legacy match field");

        assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
        assert!(error.message.contains("match"));
    }

    #[test]
    fn rejects_removed_runtime_field() {
        let error = parse_plugin_manifest_json(
            r#"{
                "key":"plugin_csv",
                "runtime":"wasm-component",
                "file_match":{"path_glob":"*.csv"},
                "entry":"plugin.wasm",
                "schemas":["schema/csv_row.json"]
            }"#,
        )
        .expect_err("the hard cut must reject the removed runtime field");

        assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
        assert!(error.message.contains("runtime"));
    }

    #[test]
    fn rejects_removed_api_version_field() {
        let error = parse_plugin_manifest_json(
            r#"{
                "key":"plugin_csv",
                "api_version":"1.0.0",
                "file_match":{"path_glob":"*.csv"},
                "entry":"plugin.wasm",
                "schemas":["schema/csv_row.json"]
            }"#,
        )
        .expect_err("the hard cut must reject the removed api_version field");

        assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
        assert!(error.message.contains("api_version"));
    }

    #[test]
    fn rejects_removed_materialization_field() {
        let error = parse_plugin_manifest_json(
            r#"{
                "key":"plugin_csv",
                "materialization":"blob",
                "file_match":{"path_glob":"*.csv"},
                "entry":"plugin.wasm",
                "schemas":["schema/csv_row.json"]
            }"#,
        )
        .expect_err("the hard cut must reject the removed materialization field");

        assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
        assert!(error.message.contains("materialization"));
    }

    #[test]
    fn rejects_invalid_manifest() {
        let err = parse_plugin_manifest_json(
            r#"{
                "file_match":{"path_glob":"*.json"},
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
                "file_match":{"path_glob":"*.{md,mdx"},
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
    fn parses_manifest_with_content_match_filter() {
        let validated = parse_plugin_manifest_json(
            r#"{
                "key":"plugin_text",
                "file_match":{"path_glob":"**/*", "content":"text"},
                "entry":"plugin.wasm",
                "schemas":["schema/default.json"]
            }"#,
        )
        .expect("manifest should parse");

        assert_eq!(
            validated
                .manifest
                .file_match
                .expect("file matcher should be present")
                .content,
            Some(PluginContentMatcher::Text)
        );
    }

    #[test]
    fn rejects_detect_changes_state_context_config() {
        let err = parse_plugin_manifest_json(
            r#"{
                "key":"plugin_markdown",
                "file_match":{"path_glob":"*.{md,mdx}"},
                "entry":"plugin.wasm",
                "schemas":["schema/default.json"],
                "detect_changes": {
                    "state_context": {
                        "include_active_state": true,
                        "columns": ["row_pk", "schema_key", "snapshot_content"]
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
            "file_match": { "path_glob": path_glob },
            "entry": "plugin.wasm",
            "schemas": schemas,
        })
        .to_string()
    }
}
