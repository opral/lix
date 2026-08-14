//! Plugin archive installation.
//!
//! Installing a plugin is a normal tracked write: the declared schemas become
//! `lix_registered_schema` rows and the original archive is stored under the
//! reserved plugin filesystem root.

use serde_json::json;

use crate::LixError;
use crate::plugin::runtime::{
    ParsedPluginArchive, parse_plugin_archive_for_install, plugin_key_from_archive_path,
    plugin_storage_archive_file_id,
};
use crate::schema::registered_schema_row_pk;
use crate::transaction::types::{RawWriteBatch, TransactionJson};

const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";

/// All derived state for one plugin archive write.
///
/// The transaction keeps the original archive bytes as the filesystem/CAS
/// artifact. This plan owns the single validated extraction used to create the
/// registry row, schema rows, and extracted component CAS entry.
#[derive(Debug)]
pub(crate) struct PluginArchiveInstallPlan {
    pub plugin_key: String,
    pub archive_file_id: String,
    pub parsed: ParsedPluginArchive,
    pub schema_rows: RawWriteBatch,
}

pub(crate) fn plugin_install_plan_from_archive_path(
    archive_path: &str,
    archive_bytes: &[u8],
    branch_id: &str,
    global: bool,
    untracked: bool,
) -> Result<PluginArchiveInstallPlan, LixError> {
    if global || untracked {
        return Err(LixError::new(
            LixError::CODE_CONSTRAINT_VIOLATION,
            "Plugin archives must be tracked and branch-local",
        )
        .with_hint("Install the plugin without GLOBAL or UNTRACKED scope."));
    }
    let plugin_key = plugin_key_from_archive_path(archive_path).ok_or_else(|| {
        LixError::new(
            LixError::CODE_CONSTRAINT_VIOLATION,
            format!("plugin archive path '{archive_path}' is not a valid plugin storage path"),
        )
    })?;
    let parsed = parse_plugin_archive_for_install(archive_bytes)?;
    if parsed.manifest.key != plugin_key {
        return Err(LixError::new(
            LixError::CODE_CONSTRAINT_VIOLATION,
            format!(
                "plugin archive path key '{}' does not match manifest key '{}'",
                plugin_key, parsed.manifest.key
            ),
        ));
    }
    let schema_rows = plugin_schema_rows(&parsed, branch_id, global, untracked)?;
    Ok(PluginArchiveInstallPlan {
        archive_file_id: plugin_storage_archive_file_id(&plugin_key),
        plugin_key,
        parsed,
        schema_rows,
    })
}

fn plugin_schema_rows(
    parsed: &ParsedPluginArchive,
    branch_id: &str,
    global: bool,
    untracked: bool,
) -> Result<RawWriteBatch, LixError> {
    if parsed.schemas.len() != parsed.schema_keys.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "Parsed plugin schemas and schema keys must have the same length",
        ));
    }
    let mut rows = RawWriteBatch::with_capacity(parsed.schemas.len());
    for (schema, schema_key) in parsed.schemas.iter().zip(&parsed.schema_keys) {
        rows.push_parts(
            Some(registered_schema_row_pk(schema_key)?),
            REGISTERED_SCHEMA_KEY.into(),
            None,
            Some(TransactionJson::from_value(
                json!({ "schema_key": schema_key, "value": schema }),
                "plugin install registered schema snapshot",
            )?),
            None,
            None,
            None,
            None,
            global,
            None,
            None,
            untracked,
            branch_id.into(),
        );
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};

    use zip::ZipWriter;
    use zip::write::SimpleFileOptions;

    use crate::LixError;
    use crate::binary_cas::BlobId;

    use super::plugin_install_plan_from_archive_path;

    const SCHEMA: &[u8] = br#"{
        "$schema":"https://lix.dev/schema-v1.json",
        "key":"plugin_test_note",
        "columns":[{"name":"id","type":"text","nullable":false}],
        "primary_key":["id"]
    }"#;
    const WASM: &[u8] = b"\0asm\x01\0\0\0";

    #[test]
    fn install_plan_contains_all_parse_once_derived_state() {
        let archive = plugin_archive(None);
        let plan = plugin_install_plan_from_archive_path(
            "/.lix/plugins/plugin_test.lixplugin",
            &archive,
            "draft",
            false,
            false,
        )
        .expect("canonical plugin should produce one install plan");

        assert_eq!(plan.plugin_key, "plugin_test");
        assert_eq!(
            plan.archive_file_id,
            crate::plugin::runtime::plugin_storage_archive_file_id("plugin_test")
        );
        assert_eq!(plan.parsed.manifest.key, "plugin_test");
        assert_eq!(plan.parsed.schema_keys, ["plugin_test_note"]);
        assert_eq!(plan.parsed.wasm_bytes, WASM);
        assert_eq!(plan.parsed.wasm_hash, BlobId::from_canonical_content(WASM));
        assert_eq!(plan.schema_rows.len(), 1);
        let schema_row = plan.schema_rows.row(0);
        assert_eq!(schema_row.schema_key, "lix_registered_schema");
        assert_eq!(schema_row.branch_id, "draft");
        assert_eq!(
            schema_row
                .snapshot
                .expect("schema install row needs a snapshot")["value"]["key"],
            "plugin_test_note"
        );
    }

    #[test]
    fn install_plan_preserves_content_for_registry_matching() {
        let archive = plugin_archive(Some("text"));
        let plan = plugin_install_plan_from_archive_path(
            "/.lix/plugins/plugin_test.lixplugin",
            &archive,
            "main",
            false,
            false,
        )
        .expect("content is part of the durable matcher contract");

        assert_eq!(
            plan.parsed.manifest.file_match.content,
            Some(crate::plugin::runtime::PluginContentMatcher::Text)
        );
    }

    #[test]
    fn bundled_csv_and_markdown_content_manifests_install() {
        let cases = [
            ("plugin_csv", "*.{csv,tsv}"),
            ("plugin_markdown", "*.{md,markdown}"),
        ];

        for (plugin_key, path_glob) in cases {
            let archive = plugin_archive_for(plugin_key, path_glob, Some("text"));
            let path = format!("/.lix/plugins/{plugin_key}.lixplugin");
            let plan = plugin_install_plan_from_archive_path(&path, &archive, "main", false, false)
                .unwrap_or_else(|error| {
                    panic!("bundled {plugin_key} manifest must install: {error:?}")
                });

            assert_eq!(plan.plugin_key, plugin_key);
            assert_eq!(plan.parsed.manifest.file_match.path_glob, path_glob);
            assert_eq!(
                plan.parsed.manifest.file_match.content,
                Some(crate::plugin::runtime::PluginContentMatcher::Text)
            );
        }
    }

    #[test]
    fn install_plan_rejects_path_manifest_key_mismatch() {
        let archive = plugin_archive(None);
        let error = plugin_install_plan_from_archive_path(
            "/.lix/plugins/plugin_other.lixplugin",
            &archive,
            "main",
            false,
            false,
        )
        .expect_err("archive path and manifest keys define one identity");

        assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
        assert!(error.message.contains("does not match"), "{error:?}");
    }

    #[test]
    fn install_plan_rejects_global_and_untracked_lifecycles_before_parsing() {
        for (global, untracked) in [(true, false), (false, true), (true, true)] {
            let error = plugin_install_plan_from_archive_path(
                "/.lix/plugins/plugin_test.lixplugin",
                b"not parsed because the lifecycle scope is unsupported",
                "main",
                global,
                untracked,
            )
            .expect_err("plugin registry entries are tracked and branch-local");

            assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
            assert!(
                error.message.contains("tracked and branch-local"),
                "{error:?}"
            );
        }
    }

    fn plugin_archive(content: Option<&str>) -> Vec<u8> {
        plugin_archive_for("plugin_test", "*.test", content)
    }

    fn plugin_archive_for(plugin_key: &str, path_glob: &str, content: Option<&str>) -> Vec<u8> {
        let content = content
            .map(|value| format!(r#", "content":"{value}""#))
            .unwrap_or_default();
        let manifest = format!(
            r#"{{
                "key":"{plugin_key}",
                "match":{{"path_glob":"{path_glob}"{content}}},
                "entry":"plugin.wasm",
                "schemas":["schema/plugin_test_note.json"]
            }}"#
        );
        let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
        let options = SimpleFileOptions::default();
        for (path, bytes) in [
            ("manifest.json", manifest.as_bytes()),
            ("schema/plugin_test_note.json", SCHEMA),
            ("plugin.wasm", WASM),
        ] {
            writer
                .start_file(path, options)
                .expect("plugin fixture entry should start");
            writer
                .write_all(bytes)
                .expect("plugin fixture entry should write");
        }
        writer
            .finish()
            .expect("plugin fixture should finish")
            .into_inner()
    }
}
