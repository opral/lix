mod support;

use std::io::{Cursor, Write};

use lix_engine::Value;
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipWriter};

const DEFAULT_SCHEMA_PATH: &str = "schema/plugin_json_schema.json";
const DEFAULT_SCHEMA_JSON: &str = r#"{
  "x-lix-key":"plugin_json_schema",
  "x-lix-version":"1",
  "type":"object",
  "properties":{"value":{"type":"string"}},
  "required":["value"],
  "additionalProperties":false
}"#;

fn build_archive(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
    let cursor = Cursor::new(Vec::new());
    let mut writer = ZipWriter::new(cursor);
    for (path, bytes) in entries {
        writer
            .start_file(*path, options)
            .expect("archive start_file should succeed");
        writer
            .write_all(bytes)
            .expect("archive entry write should succeed");
    }
    writer
        .finish()
        .expect("archive finish should succeed")
        .into_inner()
}

fn build_plugin_archive(
    manifest_json: &str,
    wasm_bytes: &[u8],
    schema_entries: &[(&str, &str)],
) -> Vec<u8> {
    let mut entries = Vec::<(&str, Vec<u8>)>::new();
    entries.push(("manifest.json", manifest_json.as_bytes().to_vec()));
    entries.push(("plugin.wasm", wasm_bytes.to_vec()));
    for (path, schema_json) in schema_entries {
        entries.push((path, schema_json.as_bytes().to_vec()));
    }
    let owned_entries = entries
        .iter()
        .map(|(path, bytes)| (*path, bytes.as_slice()))
        .collect::<Vec<_>>();
    build_archive(&owned_entries)
}

fn plugin_manifest_json(
    key: &str,
    api_version: &str,
    path_glob: &str,
    entry: &str,
    schemas: &[&str],
) -> String {
    let schemas_json = schemas
        .iter()
        .map(|path| format!("\"{path}\""))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        r#"{{
  "key":"{key}",
  "runtime":"wasm-component-v1",
  "api_version":"{api_version}",
  "match":{{"path_glob":"{path_glob}"}},
  "entry":"{entry}",
  "schemas":[{schemas_json}]
}}"#
    )
}

fn value_as_i64(value: &Value) -> i64 {
    match value {
        Value::Integer(number) => *number,
        other => panic!("expected integer value, got {other:?}"),
    }
}

simulation_test!(
    install_plugin_persists_manifest_and_wasm,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.expect("engine init should succeed");

        let manifest = plugin_manifest_json(
            "plugin_json",
            "0.1.0",
            "*.json",
            "plugin.wasm",
            &[DEFAULT_SCHEMA_PATH],
        );
        let wasm = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
        let archive = build_plugin_archive(
            &manifest,
            &wasm,
            &[(DEFAULT_SCHEMA_PATH, DEFAULT_SCHEMA_JSON)],
        );

        engine
            .install_plugin(&archive)
            .await
            .expect("install_plugin should succeed");

        let result = engine
            .execute(
                "SELECT key, runtime, api_version, match_path_glob, entry, manifest_json, wasm \
             FROM lix_internal_plugin \
             WHERE key = 'plugin_json'",
                &[],
            )
            .await
            .expect("plugin lookup should succeed");

        sim.assert_deterministic(result.rows.clone());
        assert_eq!(result.rows.len(), 1);
        let row = &result.rows[0];
        assert_eq!(row[0], Value::Text("plugin_json".to_string()));
        assert_eq!(row[1], Value::Text("wasm-component-v1".to_string()));
        assert_eq!(row[2], Value::Text("0.1.0".to_string()));
        assert_eq!(row[3], Value::Text("*.json".to_string()));
        assert_eq!(row[4], Value::Text("plugin.wasm".to_string()));
        assert_eq!(row[6], Value::Blob(wasm));
    }
);

simulation_test!(install_plugin_rejects_missing_manifest, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.expect("engine init should succeed");

    let archive = build_archive(&[("plugin.wasm", &[0x00, 0x61, 0x73, 0x6d])]);
    let err = engine
        .install_plugin(&archive)
        .await
        .expect_err("missing manifest must fail");

    assert!(err.message.contains("manifest.json"));
});

simulation_test!(
    install_plugin_rejects_missing_entry_file,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.expect("engine init should succeed");

        let manifest = plugin_manifest_json(
            "plugin_json",
            "0.1.0",
            "*.json",
            "missing.wasm",
            &[DEFAULT_SCHEMA_PATH],
        );
        let archive = build_archive(&[
            ("manifest.json", manifest.as_bytes()),
            (DEFAULT_SCHEMA_PATH, DEFAULT_SCHEMA_JSON.as_bytes()),
        ]);
        let err = engine
            .install_plugin(&archive)
            .await
            .expect_err("missing entry must fail");

        assert!(err.message.contains("missing manifest entry file"));
    }
);

simulation_test!(
    install_plugin_rejects_missing_schema_file,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.expect("engine init should succeed");

        let manifest = plugin_manifest_json(
            "plugin_json",
            "0.1.0",
            "*.json",
            "plugin.wasm",
            &[DEFAULT_SCHEMA_PATH],
        );
        let archive = build_archive(&[
            ("manifest.json", manifest.as_bytes()),
            (
                "plugin.wasm",
                &[0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00],
            ),
        ]);
        let err = engine
            .install_plugin(&archive)
            .await
            .expect_err("missing schema must fail");

        assert!(err.message.contains("missing schema file"));
    }
);

simulation_test!(
    install_plugin_rejects_invalid_schema_json,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.expect("engine init should succeed");

        let manifest = plugin_manifest_json(
            "plugin_json",
            "0.1.0",
            "*.json",
            "plugin.wasm",
            &[DEFAULT_SCHEMA_PATH],
        );
        let archive = build_archive(&[
            ("manifest.json", manifest.as_bytes()),
            (
                "plugin.wasm",
                &[0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00],
            ),
            (DEFAULT_SCHEMA_PATH, b"{\"x-lix-key\":\"broken\""),
        ]);
        let err = engine
            .install_plugin(&archive)
            .await
            .expect_err("invalid schema JSON must fail");

        assert!(err.message.contains("invalid JSON"));
    }
);

simulation_test!(
    install_plugin_rejects_duplicate_schema_key_and_version,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.expect("engine init should succeed");

        let manifest = plugin_manifest_json(
            "plugin_json",
            "0.1.0",
            "*.json",
            "plugin.wasm",
            &["schema/a.json", "schema/b.json"],
        );
        let archive = build_plugin_archive(
            &manifest,
            &[0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00],
            &[
                ("schema/a.json", DEFAULT_SCHEMA_JSON),
                ("schema/b.json", DEFAULT_SCHEMA_JSON),
            ],
        );
        let err = engine
            .install_plugin(&archive)
            .await
            .expect_err("duplicate schema key+version must fail");

        assert!(err.message.contains("duplicate schema"));
    }
);

simulation_test!(
    install_plugin_rejects_path_traversal_entries,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.expect("engine init should succeed");

        let manifest = plugin_manifest_json(
            "plugin_json",
            "0.1.0",
            "*.json",
            "plugin.wasm",
            &[DEFAULT_SCHEMA_PATH],
        );
        let archive = build_archive(&[
            ("manifest.json", manifest.as_bytes()),
            (
                "plugin.wasm",
                &[0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00],
            ),
            (DEFAULT_SCHEMA_PATH, DEFAULT_SCHEMA_JSON.as_bytes()),
            ("../evil.txt", b"bad"),
        ]);
        let err = engine
            .install_plugin(&archive)
            .await
            .expect_err("traversal entry must fail");

        assert!(err.message.contains("traversal") || err.message.contains("relative"));
    }
);

simulation_test!(
    install_plugin_is_atomic_when_schema_insert_fails,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.expect("engine init should succeed");

        let manifest = plugin_manifest_json(
            "plugin_json",
            "0.1.0",
            "*.json",
            "plugin.wasm",
            &[DEFAULT_SCHEMA_PATH],
        );
        let archive = build_archive(&[
            ("manifest.json", manifest.as_bytes()),
            (
                "plugin.wasm",
                &[0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00],
            ),
            (DEFAULT_SCHEMA_PATH, b"{\"x-lix-key\":1}"),
        ]);
        let _ = engine
            .install_plugin(&archive)
            .await
            .expect_err("install should fail");

        let plugin_count = engine
            .execute(
                "SELECT COUNT(*) FROM lix_internal_plugin WHERE key = 'plugin_json'",
                &[],
            )
            .await
            .expect("plugin count query should succeed");
        assert_eq!(value_as_i64(&plugin_count.rows[0][0]), 0);

        let schema_count = engine
            .execute(
                "SELECT COUNT(*) \
                 FROM lix_stored_schema_by_version \
                 WHERE lixcol_entity_id = 'plugin_json_schema~1'",
                &[],
            )
            .await
            .expect("schema count query should succeed");
        assert_eq!(value_as_i64(&schema_count.rows[0][0]), 0);
    }
);

simulation_test!(
    install_plugin_upsert_replaces_existing_row_by_key,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.expect("engine init should succeed");

        let first_manifest = plugin_manifest_json(
            "plugin_json",
            "0.1.0",
            "*.json",
            "plugin.wasm",
            &[DEFAULT_SCHEMA_PATH],
        );
        let second_manifest = plugin_manifest_json(
            "plugin_json",
            "0.1.1",
            "*.json5",
            "plugin.wasm",
            &[DEFAULT_SCHEMA_PATH],
        );
        let first_archive = build_plugin_archive(
            &first_manifest,
            &[0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00],
            &[(DEFAULT_SCHEMA_PATH, DEFAULT_SCHEMA_JSON)],
        );
        let second_wasm = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x01];
        let second_archive = build_plugin_archive(
            &second_manifest,
            &second_wasm,
            &[(DEFAULT_SCHEMA_PATH, DEFAULT_SCHEMA_JSON)],
        );

        engine
            .install_plugin(&first_archive)
            .await
            .expect("first install_plugin should succeed");
        engine
            .install_plugin(&second_archive)
            .await
            .expect("second install_plugin should succeed");

        let result = engine
            .execute(
                "SELECT key, api_version, match_path_glob, entry, wasm \
                 FROM lix_internal_plugin \
                 WHERE key = 'plugin_json'",
                &[],
            )
            .await
            .expect("plugin lookup should succeed");

        assert_eq!(result.rows.len(), 1);
        let row = &result.rows[0];
        assert_eq!(row[0], Value::Text("plugin_json".to_string()));
        assert_eq!(row[1], Value::Text("0.1.1".to_string()));
        assert_eq!(row[2], Value::Text("*.json5".to_string()));
        assert_eq!(row[3], Value::Text("plugin.wasm".to_string()));
        assert_eq!(row[4], Value::Blob(second_wasm));
    }
);
