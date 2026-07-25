//! Plugin archive installation.
//!
//! Installing a plugin is a normal tracked write: the declared schemas become
//! `lix_registered_schema` rows and the original archive is stored under the
//! reserved plugin filesystem root.

use serde_json::{Value as JsonValue, json};

use crate::LixError;
use crate::plugin::{
    ParsedPluginArchive, parse_plugin_archive_for_install, plugin_key_from_archive_path,
    plugin_storage_archive_file_id,
};
use crate::schema::registered_schema_entity_pk;
use crate::transaction::types::{TransactionJson, TransactionWriteRow};

const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";

/// All derived state for one plugin archive write.
///
/// The transaction keeps the original archive bytes as the filesystem/CAS
/// artifact. This plan owns the single validated extraction used to create the
/// registry row, schema rows, and extracted component CAS entry.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PluginArchiveInstallPlan {
    pub plugin_key: String,
    pub archive_file_id: String,
    pub parsed: ParsedPluginArchive,
    pub schema_rows: Vec<TransactionWriteRow>,
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
) -> Result<Vec<TransactionWriteRow>, LixError> {
    if parsed.schemas.len() != parsed.schema_keys.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "Parsed plugin schemas and schema keys must have the same length",
        ));
    }
    parsed
        .schemas
        .iter()
        .zip(&parsed.schema_keys)
        .map(|(schema, schema_key)| {
            registered_schema_row(schema, schema_key, branch_id, global, untracked)
        })
        .collect()
}

fn registered_schema_row(
    schema: &JsonValue,
    schema_key: &str,
    branch_id: &str,
    global: bool,
    untracked: bool,
) -> Result<TransactionWriteRow, LixError> {
    let entity_pk = registered_schema_entity_pk(schema_key)?;
    Ok(TransactionWriteRow {
        entity_pk: Some(entity_pk),
        schema_key: REGISTERED_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot: Some(TransactionJson::from_value(
            json!({ "value": schema }),
            "plugin install registered schema snapshot",
        )?),
        metadata: None,
        origin: None,
        created_at: None,
        updated_at: None,
        global,
        change_id: None,
        commit_id: None,
        untracked,
        branch_id: branch_id.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};

    use zip::ZipWriter;
    use zip::write::SimpleFileOptions;

    use crate::LixError;
    use crate::binary_cas::BlobHash;

    use super::plugin_install_plan_from_archive_path;

    const SCHEMA: &[u8] = br#"{
        "x-lix-key":"plugin_test_note",
        "x-lix-primary-key":["/id"],
        "type":"object",
        "properties":{"id":{"type":"string"}},
        "required":["id"],
        "additionalProperties":false
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
        assert_eq!(plan.archive_file_id, "lix_plugin_archive::plugin_test");
        assert_eq!(plan.parsed.manifest.key, "plugin_test");
        assert_eq!(plan.parsed.schema_keys, ["plugin_test_note"]);
        assert_eq!(plan.parsed.wasm_bytes, WASM);
        assert_eq!(plan.parsed.wasm_hash, BlobHash::from_content(WASM));
        assert_eq!(plan.schema_rows.len(), 1);
        assert_eq!(plan.schema_rows[0].schema_key, "lix_registered_schema");
        assert_eq!(plan.schema_rows[0].branch_id, "draft");
        assert_eq!(
            plan.schema_rows[0]
                .snapshot
                .as_ref()
                .expect("schema install row needs a snapshot")["value"]["x-lix-key"],
            "plugin_test_note"
        );
    }

    #[test]
    fn install_plan_preserves_content_type_for_registry_matching() {
        let archive = plugin_archive(Some("text"));
        let plan = plugin_install_plan_from_archive_path(
            "/.lix/plugins/plugin_test.lixplugin",
            &archive,
            "main",
            false,
            false,
        )
        .expect("content_type is part of the durable matcher contract");

        assert_eq!(
            plan.parsed.manifest.file_match.content_type,
            Some(crate::plugin::PluginContentType::Text)
        );
    }

    #[test]
    fn bundled_csv_and_markdown_content_type_manifests_install() {
        let cases = [
            ("plugin_csv", "*.{csv,tsv}"),
            ("plugin_md_v2", "*.{md,markdown}"),
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
                plan.parsed.manifest.file_match.content_type,
                Some(crate::plugin::PluginContentType::Text)
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
            .expect_err("v1 registry entries are tracked and branch-local");

            assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
            assert!(
                error.message.contains("tracked and branch-local"),
                "{error:?}"
            );
        }
    }

    fn plugin_archive(content_type: Option<&str>) -> Vec<u8> {
        plugin_archive_for("plugin_test", "*.test", content_type)
    }

    fn plugin_archive_for(
        plugin_key: &str,
        path_glob: &str,
        content_type: Option<&str>,
    ) -> Vec<u8> {
        let content_type = content_type
            .map(|value| format!(r#", "content_type":"{value}""#))
            .unwrap_or_default();
        let manifest = format!(
            r#"{{
                "key":"{plugin_key}",
                "runtime":"wasm-component-v1",
                "api_version":"0.1.0",
                "match":{{"path_glob":"{path_glob}"{content_type}}},
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
