use lix_engine::{RegisterPluginOptions, Value};

use crate::fixture::test_plugin_archive;

simulation_test!(
    session_register_plugin_accepts_archive_options_object,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = engine
            .open_workspace_session()
            .await
            .expect("workspace session should open");

        let receipt = session
            .register_plugin(RegisterPluginOptions {
                bytes: test_plugin_archive("test_plugin_json"),
            })
            .await
            .expect("valid plugin archive should register");

        assert_eq!(receipt.plugin_key, "test_plugin_json");
    }
);

simulation_test!(
    session_register_plugin_rejects_invalid_archive_bytes,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = engine
            .open_workspace_session()
            .await
            .expect("workspace session should open");

        let error = session
            .register_plugin(RegisterPluginOptions {
                bytes: b"not a plugin archive".to_vec(),
            })
            .await
            .expect_err("invalid plugin archive should be rejected");

        assert!(
            error.message.contains("valid zip file"),
            "unexpected error: {error:?}"
        );
    }
);

simulation_test!(session_register_plugin_installs_schemas, |sim| async move {
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

    let rows = session
        .execute(
            "SELECT COUNT(*) FROM lix_registered_schema \
             WHERE lix_json_get_text(value, 'x-lix-key') = 'test_json_entity'",
            &[],
        )
        .await
        .expect("schema count should be readable");

    assert_eq!(rows.rows()[0].values(), &[Value::Integer(1)]);
});

simulation_test!(
    session_register_plugin_installs_schemas_as_tracked_local_state,
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

        let rows = session
            .execute(
                "SELECT lixcol_global, lixcol_untracked \
                 FROM lix_registered_schema \
                 WHERE lix_json_get_text(value, 'x-lix-key') = 'test_json_entity'",
                &[],
            )
            .await
            .expect("schema durability should be readable");

        assert_eq!(
            rows.rows()[0].values(),
            &[Value::Boolean(false), Value::Boolean(false)]
        );

        let global_session = engine
            .open_session("global")
            .await
            .expect("global session should open");
        let global_rows = global_session
            .execute(
                "SELECT COUNT(*) FROM lix_registered_schema \
                 WHERE lix_json_get_text(value, 'x-lix-key') = 'test_json_entity'",
                &[],
            )
            .await
            .expect("global schema count should be readable");

        assert_eq!(global_rows.rows()[0].values(), &[Value::Integer(0)]);
    }
);

simulation_test!(
    session_register_plugin_persists_archive_file,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = engine
            .open_workspace_session()
            .await
            .expect("workspace session should open");
        let archive_bytes = test_plugin_archive("test_plugin_json");

        session
            .register_plugin(RegisterPluginOptions {
                bytes: archive_bytes.clone(),
            })
            .await
            .expect("valid plugin archive should register");

        let rows = session
            .execute(
                "SELECT data FROM lix_file \
                 WHERE path = '/.lix_system/plugins/test_plugin_json.lixplugin'",
                &[],
            )
            .await
            .expect("plugin archive file should be readable");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows.rows()[0].values(), &[Value::Blob(archive_bytes)]);
    }
);

simulation_test!(
    session_register_plugin_persists_archive_in_active_version_not_global,
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

        let active_rows = session
            .execute(
                "SELECT COUNT(*) FROM lix_file \
                 WHERE path = '/.lix_system/plugins/test_plugin_json.lixplugin'",
                &[],
            )
            .await
            .expect("active version plugin archive count should be readable");
        assert_eq!(active_rows.rows()[0].values(), &[Value::Integer(1)]);

        let global_session = engine
            .open_session("global")
            .await
            .expect("global session should open");
        let global_rows = global_session
            .execute(
                "SELECT COUNT(*) FROM lix_file \
                 WHERE path = '/.lix_system/plugins/test_plugin_json.lixplugin'",
                &[],
            )
            .await
            .expect("global plugin archive count should be readable");

        assert_eq!(global_rows.rows()[0].values(), &[Value::Integer(0)]);
    }
);

// Plugins are version-local and tracked so teams can experiment with plugin
// behavior on a branch. If that branch is merged to main, future branches from
// main inherit the plugin through ordinary version history.
simulation_test!(
    session_register_plugin_persists_archive_as_tracked_local_state,
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

        let rows = session
            .execute(
                "SELECT lixcol_global, lixcol_untracked \
                 FROM lix_file \
                 WHERE path = '/.lix_system/plugins/test_plugin_json.lixplugin'",
                &[],
            )
            .await
            .expect("plugin archive durability should be readable");

        assert_eq!(
            rows.rows()[0].values(),
            &[Value::Boolean(false), Value::Boolean(false)]
        );
    }
);
