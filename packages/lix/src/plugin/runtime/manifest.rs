use std::collections::BTreeSet;
use std::fmt;

use globset::{Glob, GlobBuilder, GlobMatcher};
use serde::de::{IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::LixError;
use crate::plugin::runtime::WASM_COMPONENT_API_VERSION;

pub(super) const MAX_PLUGIN_SCHEMA_KEYS: usize = 64;
pub(super) const MAX_PLUGIN_SCHEMA_KEY_BYTES: usize = 512;
pub(super) const MAX_PLUGIN_MANIFEST_BYTES: usize = 64 * 1024;

#[cfg(test)]
thread_local! {
    pub(super) static SCHEMA_KEY_VALUES_DESERIALIZED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

// The visitor bounds retained strings. serde_json may decode an escaped
// string into parser scratch before calling `visit_string`; durable row,
// archive, and manifest input limits bound that transient parser input.
struct BoundedPluginString<const MAX_BYTES: usize>(String);

impl<'de, const MAX_BYTES: usize> Deserialize<'de> for BoundedPluginString<MAX_BYTES> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct BoundedPluginStringVisitor<const MAX_BYTES: usize>;

        impl<'de, const MAX_BYTES: usize> Visitor<'de> for BoundedPluginStringVisitor<MAX_BYTES> {
            type Value = BoundedPluginString<MAX_BYTES>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "a string of at most {MAX_BYTES} bytes")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if value.len() > MAX_BYTES {
                    return Err(E::custom(format!("string exceeds {MAX_BYTES} byte limit")));
                }
                Ok(BoundedPluginString(value.to_owned()))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if value.len() > MAX_BYTES {
                    return Err(E::custom(format!("string exceeds {MAX_BYTES} byte limit")));
                }
                Ok(BoundedPluginString(value))
            }
        }

        deserializer.deserialize_str(BoundedPluginStringVisitor::<MAX_BYTES>)
    }
}

pub(super) fn deserialize_bounded_plugin_string<'de, D, const MAX_BYTES: usize>(
    deserializer: D,
) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    BoundedPluginString::<MAX_BYTES>::deserialize(deserializer).map(|value| value.0)
}

pub(super) fn deserialize_optional_bounded_plugin_string<'de, D, const MAX_BYTES: usize>(
    deserializer: D,
) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<BoundedPluginString<MAX_BYTES>>::deserialize(deserializer)
        .map(|value| value.map(|value| value.0))
}

pub(super) fn deserialize_plugin_schema_keys<'de, D>(
    deserializer: D,
) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct SchemaKeysVisitor;

    impl<'de> Visitor<'de> for SchemaKeysVisitor {
        type Value = Vec<String>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                formatter,
                "at most {MAX_PLUGIN_SCHEMA_KEYS} schema keys of at most {MAX_PLUGIN_SCHEMA_KEY_BYTES} bytes each"
            )
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            if let Some(length) = sequence
                .size_hint()
                .filter(|length| *length > MAX_PLUGIN_SCHEMA_KEYS)
            {
                return Err(serde::de::Error::custom(format!(
                    "schema key count {length} exceeds {MAX_PLUGIN_SCHEMA_KEYS}"
                )));
            }
            let capacity = sequence
                .size_hint()
                .unwrap_or_default()
                .min(MAX_PLUGIN_SCHEMA_KEYS);
            let mut values = Vec::with_capacity(capacity);
            while values.len() < MAX_PLUGIN_SCHEMA_KEYS {
                let Some(BoundedPluginString::<MAX_PLUGIN_SCHEMA_KEY_BYTES>(value)) =
                    sequence.next_element()?
                else {
                    return Ok(values);
                };
                #[cfg(test)]
                SCHEMA_KEY_VALUES_DESERIALIZED.with(|count| count.set(count.get() + 1));
                values.push(value);
            }
            if sequence.next_element::<IgnoredAny>()?.is_some() {
                return Err(serde::de::Error::custom(format!(
                    "schema key count exceeds {MAX_PLUGIN_SCHEMA_KEYS}"
                )));
            }
            Ok(values)
        }
    }

    deserializer
        .deserialize_seq(SchemaKeysVisitor)
        .map_err(|error| serde::de::Error::custom(format!("schemas/schema_keys: {error}")))
}

fn deserialize_plugin_key<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_bounded_plugin_string::<D, 128>(deserializer)
}

fn deserialize_plugin_path_glob<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_bounded_plugin_string::<D, 1024>(deserializer)
        .map_err(|error| serde::de::Error::custom(format!("path_glob: {error}")))
}

fn deserialize_plugin_entry<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_optional_bounded_plugin_string::<D, 512>(deserializer)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PluginRuntime {
    WasmComponent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginManifest {
    #[serde(deserialize_with = "deserialize_plugin_key")]
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_match: Option<PluginMatch>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_plugin_entry"
    )]
    pub entry: Option<String>,
    #[serde(deserialize_with = "deserialize_plugin_schema_keys")]
    pub schemas: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginMatch {
    #[serde(deserialize_with = "deserialize_plugin_path_glob")]
    pub path_glob: String,
    #[serde(default)]
    pub case_insensitive: bool,
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
/// constants; the component package is canonically `lix:plugin-v2`.
/// Existing repositories may retain the historical `2.0.0` spelling.
pub(crate) fn validate_runtime_api_version(
    runtime: PluginRuntime,
    api_version: &str,
) -> Result<(), LixError> {
    if runtime != PluginRuntime::WasmComponent
        || !matches!(api_version, WASM_COMPONENT_API_VERSION | "2.0.0")
    {
        return Err(LixError::new(
            LixError::CODE_INVALID_PLUGIN,
            format!(
                "plugin component requires unsupported API {api_version}; supported API: lix:plugin-v{WASM_COMPONENT_API_VERSION} (legacy lix:plugin@2.0.0)"
            ),
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
    if raw.len() > MAX_PLUGIN_MANIFEST_BYTES {
        return invalid_manifest(&format!(
            "manifest JSON exceeds its {MAX_PLUGIN_MANIFEST_BYTES}-byte bound"
        ));
    }
    let manifest_json: JsonValue = serde_json::from_str(raw).map_err(|error| {
        LixError::new(
            LixError::CODE_INVALID_PLUGIN,
            format!("Plugin manifest must be valid JSON: {error}"),
        )
    })?;

    let manifest: PluginManifest =
        PluginManifest::deserialize(&manifest_json).map_err(|error| {
            LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                format!("Invalid plugin manifest: {error}"),
            )
        })?;
    validate_plugin_manifest(&manifest)?;
    if let Some(file_match) = &manifest.file_match {
        compile_path_glob_with_case(&file_match.path_glob, file_match.case_insensitive).map_err(
            |error| {
                LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    format!(
                        "Plugin manifest path_glob '{}' is invalid: {error}",
                        file_match.path_glob
                    ),
                )
            },
        )?;
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
    compile_path_glob_with_case(glob, false).map(|compiled| compiled.compile_matcher())
}

pub(super) fn compile_path_glob_with_case(
    glob: &str,
    case_insensitive: bool,
) -> Result<Glob, globset::Error> {
    GlobBuilder::new(glob)
        .literal_separator(false)
        .case_insensitive(case_insensitive)
        .build()
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
    if !(1..=MAX_PLUGIN_SCHEMA_KEYS).contains(&manifest.schemas.len()) {
        return invalid_manifest("schemas must contain between 1 and 64 entries");
    }
    let mut schemas = BTreeSet::new();
    for schema in &manifest.schemas {
        if !(1..=MAX_PLUGIN_SCHEMA_KEY_BYTES).contains(&schema.len()) {
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

    use super::{
        PluginContentMatcher, compile_path_glob_with_case, glob_matches_path,
        parse_plugin_manifest_json,
    };

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
    fn markdown_manifest_matches_case_insensitive_extensions() {
        let manifest = parse_plugin_manifest_json(include_str!(
            "../../../../../plugins/markdown/manifest.json"
        ))
        .expect("Markdown manifest should parse");
        let matcher = manifest.manifest.file_match.expect("file matcher");
        assert!(matcher.case_insensitive);
        let path_glob = compile_path_glob_with_case(&matcher.path_glob, matcher.case_insensitive)
            .expect("Markdown glob should compile")
            .compile_matcher();

        for path in [
            "/docs/readme.md",
            "/docs/readme.MD",
            "/docs/readme.mD",
            "/docs/readme.markdown",
            "/docs/readme.MARKDOWN",
            "/docs/readme.MarkDown",
        ] {
            assert!(path_glob.is_match(path), "expected match: {path}");
        }
        assert!(!path_glob.is_match("/docs/readme.mdx"));
    }

    #[test]
    fn case_insensitive_glob_matching_is_opt_in() {
        let sensitive = compile_path_glob_with_case("/Docs/*.md", false)
            .expect("case-sensitive glob should compile")
            .compile_matcher();
        assert!(sensitive.is_match("/Docs/readme.md"));
        assert!(!sensitive.is_match("/docs/readme.MD"));

        let insensitive = compile_path_glob_with_case("/Docs/*.md", true)
            .expect("case-insensitive glob should compile")
            .compile_matcher();
        assert!(insensitive.is_match("/docs/README.MD"));
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

    #[test]
    fn schema_deserializer_bounds_count_and_string_materialization() {
        let oversized_list = serde_json::json!({
            "key": "plugin_test",
            "schemas": (0..=super::MAX_PLUGIN_SCHEMA_KEYS)
                .map(|index| format!("schema_{index}"))
                .collect::<Vec<_>>(),
        });
        let encoded = serde_json::to_string(&oversized_list).unwrap();
        super::SCHEMA_KEY_VALUES_DESERIALIZED.with(|count| count.set(0));
        let error = serde_json::from_str::<super::PluginManifest>(&encoded)
            .expect_err("65 schema values should exceed the visitor bound");
        assert!(error.to_string().contains("schema key count"));
        super::SCHEMA_KEY_VALUES_DESERIALIZED.with(|count| {
            assert!(count.get() <= super::MAX_PLUGIN_SCHEMA_KEYS);
        });

        let oversized_string = serde_json::json!({
            "key": "plugin_test",
            "schemas": ["s".repeat(super::MAX_PLUGIN_SCHEMA_KEY_BYTES + 1)],
        });
        let encoded = serde_json::to_string(&oversized_string).unwrap();
        super::SCHEMA_KEY_VALUES_DESERIALIZED.with(|count| count.set(0));
        let error = serde_json::from_str::<super::PluginManifest>(&encoded)
            .expect_err("oversized schema string should be rejected");
        assert!(error.to_string().contains("512 byte limit"));
        super::SCHEMA_KEY_VALUES_DESERIALIZED.with(|count| assert_eq!(count.get(), 0));
    }

    #[test]
    fn manifest_parser_rejects_input_over_the_archive_bound() {
        let oversized = " ".repeat(super::MAX_PLUGIN_MANIFEST_BYTES + 1);
        let error = parse_plugin_manifest_json(&oversized)
            .expect_err("oversized manifest input should fail before JSON parsing");
        assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
        assert!(error.message.contains("65536-byte bound"));
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
