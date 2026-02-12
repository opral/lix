mod support;

use lix_engine::Value;

simulation_test!(
    install_plugin_persists_manifest_and_wasm,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.expect("engine init should succeed");

        let manifest = r#"{
        "key":"plugin_json",
        "runtime":"wasm-component-v1",
        "api_version":"0.1.0",
        "detect_changes_glob":"*.json"
    }"#;
        let wasm = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];

        engine
            .install_plugin(manifest, &wasm)
            .await
            .expect("install_plugin should succeed");

        let result = engine
        .execute(
            "SELECT key, runtime, api_version, detect_changes_glob, entry, manifest_json, wasm \
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

        let manifest_text = match &row[5] {
            Value::Text(value) => value,
            other => panic!("expected manifest_json text value, got {other:?}"),
        };
        let manifest_json: serde_json::Value =
            serde_json::from_str(manifest_text).expect("manifest_json should parse");
        assert_eq!(manifest_json["key"], "plugin_json");
        assert_eq!(manifest_json["runtime"], "wasm-component-v1");
    }
);

simulation_test!(install_plugin_rejects_invalid_manifest, |sim| async move {
    let engine = sim
        .boot_simulated_engine(None)
        .await
        .expect("boot_simulated_engine should succeed");
    engine.init().await.expect("engine init should succeed");

    let err = engine
        .install_plugin(
            r#"{
                "runtime":"wasm-component-v1",
                "api_version":"0.1.0",
                "detect_changes_glob":"*.json"
            }"#,
            &[0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00],
        )
        .await
        .expect_err("manifest without key must fail");

    assert!(err.to_string().contains("Invalid plugin manifest"));
});

simulation_test!(
    install_plugin_rejects_invalid_wasm_bytes,
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.init().await.expect("engine init should succeed");

        let err = engine
            .install_plugin(
                r#"{
                "key":"plugin_json",
                "runtime":"wasm-component-v1",
                "api_version":"0.1.0",
                "detect_changes_glob":"*.json"
            }"#,
                &[0xde, 0xad, 0xbe, 0xef],
            )
            .await
            .expect_err("invalid wasm bytes must fail");

        assert!(err
            .to_string()
            .contains("Plugin wasm bytes must start with a valid wasm header"));
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

        let first_manifest = r#"{
            "key":"plugin_json",
            "runtime":"wasm-component-v1",
            "api_version":"0.1.0",
            "detect_changes_glob":"*.json"
        }"#;
        let second_manifest = r#"{
            "key":"plugin_json",
            "runtime":"wasm-component-v1",
            "api_version":"0.1.1",
            "detect_changes_glob":"*.json5",
            "entry":"main.wasm"
        }"#;
        let first_wasm = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
        let second_wasm = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x01];

        engine
            .install_plugin(first_manifest, &first_wasm)
            .await
            .expect("first install_plugin should succeed");
        engine
            .install_plugin(second_manifest, &second_wasm)
            .await
            .expect("second install_plugin should succeed");

        let result = engine
            .execute(
                "SELECT key, api_version, detect_changes_glob, entry, wasm \
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
        assert_eq!(row[3], Value::Text("main.wasm".to_string()));
        assert_eq!(row[4], Value::Blob(second_wasm));
    }
);
