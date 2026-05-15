use lix_engine::{PluginContentType, PluginRuntime, RegisterPluginOptions, Value};

use crate::fixture::test_plugin_archive;

simulation_test!(
    session_list_plugins_returns_registered_plugin,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = engine
            .open_workspace_session()
            .await
            .expect("workspace session should open");

        session
            .register_plugin(RegisterPluginOptions {
                bytes: test_plugin_archive("test_plugin_json"),
            })
            .await
            .expect("valid plugin archive should register");

        let plugins = session
            .list_plugins()
            .await
            .expect("registered plugins should list");

        assert_eq!(plugins.len(), 1);
        let plugin = &plugins[0];
        assert_eq!(plugin.key, "test_plugin_json");
        assert_eq!(plugin.runtime, PluginRuntime::WasmComponentV1);
        assert_eq!(plugin.api_version, "0.1.0");
        assert_eq!(plugin.path_glob, "*.json");
        assert_eq!(plugin.content_type, Some(PluginContentType::Text));
        assert_eq!(plugin.entry, "plugin.wasm");
        assert_eq!(plugin.wasm, b"\0asm\x01\0\0\0");
        let manifest: serde_json::Value =
            serde_json::from_str(&plugin.manifest_json).expect("manifest JSON should parse");
        assert_eq!(manifest["key"], "test_plugin_json");
    }
);

simulation_test!(
    session_list_plugins_uses_active_version_not_global,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = engine
            .open_workspace_session()
            .await
            .expect("workspace session should open");

        session
            .register_plugin(RegisterPluginOptions {
                bytes: test_plugin_archive("test_plugin_json"),
            })
            .await
            .expect("valid plugin archive should register");

        assert_eq!(
            session
                .list_plugins()
                .await
                .expect("active version plugins should list")
                .len(),
            1
        );

        let global_session = engine
            .open_session("global")
            .await
            .expect("global session should open");
        assert!(global_session
            .list_plugins()
            .await
            .expect("global plugins should list")
            .is_empty());
    }
);

simulation_test!(
    session_list_plugins_ignores_non_plugin_files_under_plugin_root,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = engine
            .open_workspace_session()
            .await
            .expect("workspace session should open");

        session
            .execute(
                "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
                &[
                    Value::Text("/.lix_system/plugins/readme.txt".to_string()),
                    Value::Blob(b"not a plugin".to_vec()),
                ],
            )
            .await
            .expect("non-plugin file should insert");

        assert!(session
            .list_plugins()
            .await
            .expect("plugins should list")
            .is_empty());
    }
);

simulation_test!(
    session_list_plugins_errors_on_corrupt_plugin_archive,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = engine
            .open_workspace_session()
            .await
            .expect("workspace session should open");

        session
            .execute(
                "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
                &[
                    Value::Text("/.lix_system/plugins/corrupt.lixplugin".to_string()),
                    Value::Blob(b"not a zip".to_vec()),
                ],
            )
            .await
            .expect("corrupt plugin archive file should insert");

        let error = session
            .list_plugins()
            .await
            .expect_err("corrupt plugin archive should fail listing");
        assert!(
            error.message.contains("valid zip file"),
            "unexpected error: {error:?}"
        );
    }
);
