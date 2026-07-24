#[macro_use]
mod support;

use std::collections::BTreeMap;
use std::io::{Cursor, Read, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use lix_engine::wasm::{
    WasmComponentInstance, WasmLimits, WasmPluginDetectedChange, WasmPluginEntityState,
    WasmPluginFile, WasmRuntime,
};
use lix_engine::{Engine, ExecuteResult, LixError, Memory, SessionContext, Value};
use serde_json::json;

simulation_test!(
    sql_file_write_read_and_readdir_roundtrip,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );

        write_file(&session, "/docs/readme.txt", b"hello".to_vec())
            .await
            .expect("file upsert should create parents and data");

        assert_eq!(
            read_file(&session, "/docs/readme.txt")
                .await
                .expect("file read should succeed"),
            Some(b"hello".to_vec())
        );
        assert_eq!(
            read_file(&session, "/docs/missing.txt")
                .await
                .expect("missing file read should succeed"),
            None
        );

        let entries = readdir(&session, "/docs/")
            .await
            .expect("directory read should succeed")
            .expect("directory should exist");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "readme.txt");
        assert_eq!(entries[0].path, "/docs/readme.txt");
        assert_eq!(entries[0].kind, DirEntryKind::File);
    }
);

simulation_test!(sql_path_only_file_reads_as_empty, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_workspace_session()
            .await
            .expect("workspace session should open"),
        &engine,
    );

    session
        .execute("INSERT INTO lix_file (path) VALUES ('/empty.txt')", &[])
        .await
        .expect("path-only file insert should succeed");

    assert_eq!(
        read_file(&session, "/empty.txt")
            .await
            .expect("file read should succeed"),
        Some(Vec::new())
    );
});

simulation_test!(
    exact_lix_file_point_reads_match_generic_sql_across_visible_lanes,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_file (id, path, data, lixcol_global) VALUES \
                 ('shared-point-file', '/shared-point.bin', X'61', false), \
                 ('shared-point-file', '/shared-point.bin', X'62', true)",
                &[],
            )
            .await
            .expect("branch and global point-read fixtures should insert");

        for (fast_sql, generic_sql, parameter) in [
            (
                "SELECT data FROM lix_file WHERE id = $1",
                "SELECT data FROM lix_file WHERE id = $1 AND true",
                "shared-point-file",
            ),
            (
                "SELECT data FROM lix_file WHERE path = $1",
                "SELECT data FROM lix_file WHERE path = $1 AND true",
                "/shared-point.bin",
            ),
            (
                "SELECT lixcol_change_id FROM lix_file WHERE id = $1",
                "SELECT lixcol_change_id FROM lix_file WHERE id = $1 AND true",
                "shared-point-file",
            ),
            (
                "SELECT data FROM lix_file WHERE id = $1",
                "SELECT data FROM lix_file WHERE id = $1 AND true",
                "missing-point-file",
            ),
        ] {
            let params = [Value::Text(parameter.to_string())];
            let fast = session
                .execute(fast_sql, &params)
                .await
                .expect("exact point read should execute");
            let generic = session
                .execute(generic_sql, &params)
                .await
                .expect("generic comparison read should execute");
            assert_eq!(fast, generic, "point read differed for {fast_sql}");
        }
    }
);

#[tokio::test]
async fn sql_plugin_archive_upsert_installs_and_updates_plugin() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage).await.expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");
    let archive = sentinel_plugin_archive();

    install_plugin(&session, "plugin_sentinel", &archive)
        .await
        .expect("plugin archive upsert should install plugin");
    install_plugin(&session, "plugin_sentinel", &archive)
        .await
        .expect("plugin archive upsert should update plugin");

    let plugins = list_installed_plugins(&session)
        .await
        .expect("installed plugins should list");
    assert_eq!(plugins.len(), 1);
    assert_eq!(plugins[0].key, "plugin_sentinel");
    assert_eq!(plugins[0].schema_keys, vec!["plugin_note".to_string()]);
    assert_eq!(
        read_file(&session, "/.lix/plugins/plugin_sentinel.lixplugin")
            .await
            .expect("archive read should succeed")
            .as_deref(),
        Some(archive.as_slice())
    );

    let schemas = session
        .execute(
            "SELECT value FROM lix_registered_schema \
             WHERE lixcol_entity_pk = lix_json('[\"plugin_note\"]')",
            &[],
        )
        .await
        .expect("plugin schema should be queryable");
    assert_eq!(schemas.len(), 1);

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn installing_distinct_plugins_replaces_internal_singleton_rows_under_insert_mode() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage).await.expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    install_plugin(&session, "plugin_sentinel", &sentinel_plugin_archive())
        .await
        .expect("first plugin install should succeed");
    install_plugin(&session, "plugin_second", &second_sentinel_plugin_archive())
        .await
        .expect("second plugin install should replace internal registry/schema rows");

    let plugins = list_installed_plugins(&session)
        .await
        .expect("both installed plugins should list");
    assert_eq!(
        plugins
            .iter()
            .map(|plugin| plugin.key.as_str())
            .collect::<Vec<_>>(),
        vec!["plugin_second", "plugin_sentinel"]
    );
    assert_eq!(registered_plugin_note_schema_count(&session).await, 1);

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn plugin_install_rejects_conflicting_registered_schema_definition() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage).await.expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    install_plugin(&session, "plugin_sentinel", &sentinel_plugin_archive())
        .await
        .expect("first plugin install should succeed");
    let error = install_plugin(
        &session,
        "plugin_conflicting",
        &conflicting_schema_plugin_archive(),
    )
    .await
    .expect_err("different definitions must not share one schema key");
    assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
    assert!(error.message.contains("plugin_note"), "{error:?}");
    assert_eq!(
        list_installed_plugins(&session)
            .await
            .expect("failed install must leave the first plugin intact")
            .len(),
        1
    );
    assert_eq!(registered_plugin_note_schema_count(&session).await, 1);

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn active_plugin_prevents_public_mutation_of_its_registered_schema() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage).await.expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    install_plugin(&session, "plugin_sentinel", &sentinel_plugin_archive())
        .await
        .expect("plugin install should succeed");
    let replacement = json!({
        "x-lix-key": "plugin_note",
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "properties": {
            "id": { "type": "string" },
            "value": { "type": "string" },
            "extra": { "type": "string" }
        },
        "required": ["id", "value"],
        "additionalProperties": false
    });
    let error = session
        .execute(
            "UPDATE lix_registered_schema SET value = $1 \
             WHERE lixcol_entity_pk = lix_json('[\"plugin_note\"]')",
            &[Value::Json(replacement)],
        )
        .await
        .expect_err("active plugin schema mutation must be rejected");
    assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
    assert!(error.message.contains("plugin_sentinel"), "{error:?}");

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn sql_plugin_archive_plain_insert_reuses_deterministic_file_id() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage).await.expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");
    let archive = sentinel_plugin_archive();
    let path = "/.lix/plugins/plugin_sentinel.lixplugin";

    install_plugin(&session, "plugin_sentinel", &archive)
        .await
        .expect("plugin archive upsert should install plugin");

    let error = session
        .execute(
            "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
            &[
                Value::Text(path.to_string()),
                Value::Blob(archive.clone().into()),
            ],
        )
        .await
        .expect_err("plain plugin archive insert should reject duplicate archive id");
    assert_eq!(error.code, LixError::CODE_UNIQUE);
    assert!(
        error
            .message
            .contains("lix_plugin_archive::plugin_sentinel")
    );

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn sql_plugin_archive_path_must_match_manifest_key() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage).await.expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    let error = install_plugin(&session, "plugin_other", &sentinel_plugin_archive())
        .await
        .expect_err("mismatched plugin archive key should fail");

    assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
    assert!(error.message.contains("does not match manifest key"));

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn explicit_transaction_install_is_visible_to_later_plugin_write() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let runtime = Arc::new(SentinelPluginRuntime::default());
    let engine = Engine::new_with_wasm_runtime(storage, runtime)
        .await
        .expect("engine should open with plugin runtime");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");
    let mut tx = session
        .begin_transaction()
        .await
        .expect("transaction should begin");

    tx.execute(
        "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
        &[
            Value::Text("/.lix/plugins/plugin_sentinel.lixplugin".to_string()),
            Value::Blob(sentinel_plugin_archive().into()),
        ],
    )
    .await
    .expect("plugin install should stage");
    tx.execute(
        "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
        &[
            Value::Text("/same-transaction.sentinel".to_string()),
            Value::Blob(b"hello".to_vec().into()),
        ],
    )
    .await
    .expect("later write should see staged registry and extracted WASM");
    tx.commit()
        .await
        .expect("transaction should commit atomically");

    assert_eq!(
        read_file(&session, "/same-transaction.sentinel")
            .await
            .expect("committed plugin file should render"),
        Some(b"plugin-rendered".to_vec())
    );
    session.close().await.expect("session should close");
}

#[tokio::test]
async fn sql_update_path_to_plugin_storage_rejects_plugin_archive_rename() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage).await.expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    write_file(&session, "/normal.txt", b"normal".to_vec())
        .await
        .expect("normal file insert should succeed");

    let error = session
        .execute(
            "UPDATE lix_file SET path = $1, data = $2 WHERE path = $3",
            &[
                Value::Text("/.lix/plugins/plugin_sentinel.lixplugin".to_string()),
                Value::Blob(sentinel_plugin_archive().into()),
                Value::Text("/normal.txt".to_string()),
            ],
        )
        .await
        .expect_err("path update into plugin storage should fail");

    assert!(error.message.contains("plugin archive paths"));

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn sql_update_rejects_invalid_installed_plugin_storage_archive_data() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage).await.expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    install_plugin(&session, "plugin_sentinel", &sentinel_plugin_archive())
        .await
        .expect("plugin install should succeed");

    let error = session
        .execute(
            "UPDATE lix_file \
             SET data = X'626164' \
             WHERE path = '/.lix/plugins/plugin_sentinel.lixplugin'",
            &[],
        )
        .await
        .expect_err("SQL update should reject invalid plugin archive data");

    assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
    assert!(error.message.contains("ZIP"));

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn sql_invalid_plugin_manifest_writes_are_atomic() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage).await.expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");
    let path = "/.lix/plugins/plugin_sentinel.lixplugin";
    let invalid = invalid_glob_sentinel_plugin_archive();

    let error = install_plugin(&session, "plugin_sentinel", &invalid)
        .await
        .expect_err("a fresh install with an invalid glob must fail");
    assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
    assert!(error.message.contains("path_glob"));
    assert_eq!(
        read_file(&session, path)
            .await
            .expect("failed install archive lookup should succeed"),
        None
    );
    assert_eq!(registered_plugin_note_schema_count(&session).await, 0);

    let original = sentinel_plugin_archive();
    install_plugin(&session, "plugin_sentinel", &original)
        .await
        .expect("valid plugin install should succeed");
    let error = install_plugin(&session, "plugin_sentinel", &invalid)
        .await
        .expect_err("an update with an invalid glob must fail");
    assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
    assert_eq!(
        read_file(&session, path)
            .await
            .expect("installed archive lookup should succeed")
            .as_deref(),
        Some(original.as_slice())
    );
    assert_eq!(registered_plugin_note_schema_count(&session).await, 1);

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn sql_public_writes_cannot_forge_plugin_registry_or_owner_rows() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage).await.expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    for key in ["lix_plugin_registry_v1", "lix_plugin_owner_v1"] {
        let error = session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
                &[
                    Value::Text(key.to_string()),
                    Value::Json(json!({"forged": true})),
                ],
            )
            .await
            .expect_err("engine-owned plugin keys must reject public writes");
        assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
        assert!(error.message.contains("reserved"), "{error:?}");
    }

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn sql_exact_archive_delete_uninstalls_plugin_but_recursive_delete_is_rejected() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage).await.expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    install_plugin(&session, "plugin_sentinel", &sentinel_plugin_archive())
        .await
        .expect("plugin install should succeed");

    let recursive_error = session
        .execute(
            "DELETE FROM lix_file \
             WHERE path LIKE '/.lix/plugins/%.lixplugin'",
            &[],
        )
        .await
        .expect_err("recursive plugin archive delete should be rejected");
    assert_eq!(recursive_error.code, LixError::CODE_CONSTRAINT_VIOLATION);
    assert!(
        recursive_error
            .message
            .contains("one exact canonical plugin archive")
    );

    session
        .execute(
            "DELETE FROM lix_file \
             WHERE path = '/.lix/plugins/plugin_sentinel.lixplugin'",
            &[],
        )
        .await
        .expect("one exact canonical archive delete should uninstall the plugin");

    let directory_error = session
        .execute(
            "DELETE FROM lix_directory WHERE path = '/.lix/plugins/'",
            &[],
        )
        .await
        .expect_err("SQL delete should reject plugin storage directory tombstones");
    assert_eq!(directory_error.code, LixError::CODE_CONSTRAINT_VIOLATION);
    assert!(
        directory_error
            .message
            .contains("reserved plugin storage path")
    );

    assert_eq!(
        list_installed_plugins(&session)
            .await
            .expect("uninstalled plugin list should load")
            .len(),
        0
    );

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn uninstall_keeps_owned_state_and_reinstall_resumes_rendering() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let runtime = Arc::new(SentinelPluginRuntime::default());
    let engine = Engine::new_with_wasm_runtime(storage, runtime)
        .await
        .expect("engine should open with plugin runtime");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");
    let archive = sentinel_plugin_archive();

    install_plugin(&session, "plugin_sentinel", &archive)
        .await
        .expect("plugin install should succeed");
    write_file(&session, "/resume.sentinel", b"owned".to_vec())
        .await
        .expect("plugin file write should succeed");
    let id_rows = session
        .execute(
            "SELECT id FROM lix_file WHERE path = '/resume.sentinel'",
            &[],
        )
        .await
        .expect("owned file id should load");
    let [Value::Text(file_id)] = id_rows.rows()[0].values() else {
        panic!("expected owned file id");
    };
    let file_id = file_id.clone();

    session
        .execute(
            "DELETE FROM lix_file \
             WHERE path = '/.lix/plugins/plugin_sentinel.lixplugin'",
            &[],
        )
        .await
        .expect("exact archive delete should uninstall");
    assert_eq!(plugin_owner_count(&session, &file_id).await, 1);
    assert_eq!(plugin_state_count(&session, &file_id).await, 1);
    let unavailable = read_file(&session, "/resume.sentinel")
        .await
        .expect_err("uninstalled plugin state must not silently render as empty bytes");
    assert_eq!(unavailable.code, LixError::CODE_PLUGIN_UNAVAILABLE);
    assert!(unavailable.message.contains("plugin_sentinel"));

    install_plugin(&session, "plugin_sentinel", &archive)
        .await
        .expect("plugin reinstall should succeed");
    assert_eq!(
        read_file(&session, "/resume.sentinel")
            .await
            .expect("reinstalled plugin file should render retained state"),
        Some(b"plugin-rendered".to_vec())
    );

    session
        .execute(
            "DELETE FROM lix_file \
             WHERE path = '/.lix/plugins/plugin_sentinel.lixplugin'",
            &[],
        )
        .await
        .expect("second exact archive delete should uninstall");
    let move_error = session
        .execute(
            "UPDATE lix_file SET path = '/resume.txt' \
             WHERE path = '/resume.sentinel'",
            &[],
        )
        .await
        .expect_err("materialized plugin files cannot move while their plugin is unavailable");
    assert_eq!(move_error.code, LixError::CODE_PLUGIN_UNAVAILABLE);

    install_plugin(&session, "plugin_sentinel", &archive)
        .await
        .expect("second plugin reinstall should succeed");
    session
        .execute(
            "UPDATE lix_file SET path = '/resume.txt' \
             WHERE path = '/resume.sentinel'",
            &[],
        )
        .await
        .expect("path move should succeed after the plugin is reinstalled");
    assert_eq!(
        read_file(&session, "/resume.txt")
            .await
            .expect("moved file should preserve its rendered bytes"),
        Some(b"plugin-rendered".to_vec())
    );

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn deleting_owned_file_after_uninstall_cleans_stale_owner_and_state() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let runtime = Arc::new(SentinelPluginRuntime::default());
    let engine = Engine::new_with_wasm_runtime(storage, runtime)
        .await
        .expect("engine should open with plugin runtime");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    install_plugin(&session, "plugin_sentinel", &sentinel_plugin_archive())
        .await
        .expect("plugin install should succeed");
    write_file(&session, "/delete.sentinel", b"owned".to_vec())
        .await
        .expect("plugin-owned file write should succeed");
    let id_rows = session
        .execute(
            "SELECT id FROM lix_file WHERE path = '/delete.sentinel'",
            &[],
        )
        .await
        .expect("owned file id should load");
    let [Value::Text(file_id)] = id_rows.rows()[0].values() else {
        panic!("expected owned file id");
    };
    let file_id = file_id.clone();

    session
        .execute(
            "DELETE FROM lix_file \
             WHERE path = '/.lix/plugins/plugin_sentinel.lixplugin'",
            &[],
        )
        .await
        .expect("exact archive delete should uninstall");
    session
        .execute("DELETE FROM lix_file WHERE path = '/delete.sentinel'", &[])
        .await
        .expect("deleting a stale-owned file should clean dependent plugin rows");

    assert_eq!(read_file(&session, "/delete.sentinel").await.unwrap(), None);
    assert_eq!(plugin_owner_count(&session, &file_id).await, 0);
    assert_eq!(plugin_state_count(&session, &file_id).await, 0);

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn empty_regular_file_does_not_render_through_later_installed_plugin() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let runtime = Arc::new(SentinelPluginRuntime::default());
    let engine = Engine::new_with_wasm_runtime(storage, runtime.clone())
        .await
        .expect("engine should open with plugin runtime");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    write_file(&session, "/raw.sentinel", Vec::new())
        .await
        .expect("empty file write should succeed");
    install_plugin(&session, "plugin_sentinel", &sentinel_plugin_archive())
        .await
        .expect("plugin install should succeed");

    assert_eq!(
        read_file(&session, "/raw.sentinel")
            .await
            .expect("file read should succeed"),
        Some(Vec::new())
    );

    let files = session
        .execute(
            "SELECT data FROM lix_file WHERE path = '/raw.sentinel'",
            &[],
        )
        .await
        .expect("lix_file data read should succeed");
    assert_eq!(files.len(), 1);
    assert_eq!(files.rows()[0].values(), &[Value::Blob(Vec::new().into())]);
    assert_eq!(runtime.render_calls.load(Ordering::SeqCst), 0);

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn durable_owner_keeps_rendering_when_new_plugin_is_more_specific() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let runtime = Arc::new(SentinelPluginRuntime::default());
    let engine = Engine::new_with_wasm_runtime(storage, runtime.clone())
        .await
        .expect("engine should open with plugin runtime");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    install_plugin(
        &session,
        "plugin_overlap_broad",
        &broad_overlap_plugin_archive(),
    )
    .await
    .expect("broad plugin install should succeed");
    write_file(&session, "/special.overlap", b"owned-by-broad".to_vec())
        .await
        .expect("broad plugin should materialize the file");
    assert_eq!(
        read_file(&session, "/special.overlap").await.unwrap(),
        Some(b"plugin-rendered".to_vec())
    );

    install_plugin(
        &session,
        "plugin_overlap_specific",
        &specific_overlap_plugin_archive(),
    )
    .await
    .expect("more-specific overlapping plugin install should succeed");

    // Installation does not rewrite existing files. The durable broad owner
    // remains valid because its own glob still matches, even though the new
    // plugin would win selection for a fresh write at the same path.
    assert_eq!(
        read_file(&session, "/special.overlap").await.unwrap(),
        Some(b"plugin-rendered".to_vec())
    );
    assert_eq!(runtime.render_calls.load(Ordering::SeqCst), 2);

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn plugin_detect_changes_receives_descriptor_filename() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let runtime = Arc::new(SentinelPluginRuntime::default());
    let engine = Engine::new_with_wasm_runtime(storage, runtime.clone())
        .await
        .expect("engine should open with plugin runtime");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    install_plugin(&session, "plugin_sentinel", &sentinel_plugin_archive())
        .await
        .expect("plugin install should succeed");
    write_file(&session, "/nested/raw.sentinel", b"hello".to_vec())
        .await
        .expect("plugin file write should succeed");
    session
        .execute(
            "UPDATE lix_file SET data = X'776f726c64' \
             WHERE path = '/nested/raw.sentinel'",
            &[],
        )
        .await
        .expect("plugin file data-only update should succeed");

    let filenames = runtime
        .detect_filenames
        .lock()
        .expect("detect filename lock should not be poisoned")
        .clone();
    assert_eq!(
        filenames,
        vec![
            Some("raw.sentinel".to_string()),
            Some("raw.sentinel".to_string()),
        ]
    );

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn non_empty_plugin_overwrite_does_not_stage_blob_ref_tombstone() {
    let storage = Memory::new();
    let receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let runtime = Arc::new(SentinelPluginRuntime::default());
    let engine = Engine::new_with_wasm_runtime(storage, runtime)
        .await
        .expect("engine should open with plugin runtime");
    let session = engine
        .open_session(receipt.main_branch_id.clone())
        .await
        .expect("workspace session should open");

    install_plugin(&session, "plugin_sentinel", &sentinel_plugin_archive())
        .await
        .expect("plugin install should succeed");
    write_file(&session, "/owned.sentinel", b"first".to_vec())
        .await
        .expect("plugin file write should succeed");
    let file = session
        .execute(
            "SELECT id FROM lix_file WHERE path = '/owned.sentinel'",
            &[],
        )
        .await
        .expect("plugin file id should load");
    let [Value::Text(file_id)] = file.rows()[0].values() else {
        panic!("expected plugin file id");
    };
    let blob_ref_changes_before = blob_ref_change_count(&session, file_id).await;

    write_file(&session, "/owned.sentinel", b"second".to_vec())
        .await
        .expect("plugin file overwrite should succeed");
    assert_eq!(
        blob_ref_change_count(&session, file_id).await,
        blob_ref_changes_before,
        "plugin overwrite must not add a binary-blob-ref tombstone"
    );
    session.close().await.expect("session should close");
}

#[tokio::test]
async fn plugin_stale_writes_preserve_disjoint_entity_edits() {
    let (_engine, session_a, session_b) = keyed_collaboration_sessions().await;
    write_file(&session_a, "/shared.keyed", b"a=0\nb=0\n".to_vec())
        .await
        .expect("seed file should write");
    assert_eq!(
        read_file(&session_a, "/shared.keyed").await.unwrap(),
        Some(b"a=0\nb=0\n".to_vec())
    );
    assert_eq!(
        read_file(&session_b, "/shared.keyed").await.unwrap(),
        Some(b"a=0\nb=0\n".to_vec())
    );

    write_file(&session_a, "/shared.keyed", b"a=A\nb=0\n".to_vec())
        .await
        .expect("session A edit should write");
    write_file(&session_b, "/shared.keyed", b"a=0\nb=B\n".to_vec())
        .await
        .expect("session B stale edit should write");

    assert_eq!(
        read_file(&session_a, "/shared.keyed").await.unwrap(),
        Some(b"a=A\nb=B\n".to_vec())
    );
    assert_eq!(
        keyed_entity_values(&session_a, "/shared.keyed").await,
        BTreeMap::from([
            ("a".to_string(), "A".to_string()),
            ("b".to_string(), "B".to_string())
        ])
    );
}

#[tokio::test]
async fn plugin_blind_write_uses_current_identity_hint_without_delete_authority() {
    let runtime = Arc::new(KeyedPluginRuntime::default());
    let (_engine, session_a, session_b) =
        keyed_collaboration_sessions_with_runtime(Memory::new(), Arc::clone(&runtime)).await;
    write_file(&session_a, "/shared.keyed", b"a=0\nb=0\n".to_vec())
        .await
        .expect("seed file should write");
    runtime.take_detect_states();

    // Session B deliberately never reads the file. Current shared state is an
    // identity hint, not delete authority: the omitted `b` must survive.
    write_file(&session_b, "/shared.keyed", b"a=B\n".to_vec())
        .await
        .expect("blind semantic write should succeed");
    assert_eq!(
        runtime.take_detect_states(),
        vec![BTreeMap::from([
            ("a".to_string(), "0".to_string()),
            ("b".to_string(), "0".to_string()),
        ])]
    );
    assert_eq!(
        keyed_entity_values(&session_a, "/shared.keyed").await,
        BTreeMap::from([
            ("a".to_string(), "B".to_string()),
            ("b".to_string(), "0".to_string()),
        ])
    );

    // The successful submission, rather than the merged shared result, is B's
    // next private base. B may delete its own `a`, but still cannot delete the
    // unseen `b` that was preserved above.
    write_file(&session_b, "/shared.keyed", Vec::new())
        .await
        .expect("submitted entity deletion should succeed");
    assert_eq!(
        runtime.take_detect_states(),
        vec![BTreeMap::from([("a".to_string(), "B".to_string())])]
    );
    assert_eq!(
        keyed_entity_values(&session_a, "/shared.keyed").await,
        BTreeMap::from([("b".to_string(), "0".to_string())])
    );

    // An acknowledged empty base is distinct from a missing base. Falling
    // back to current state here would incorrectly expose `b` to deletion.
    write_file(&session_b, "/shared.keyed", b"x=X\n".to_vec())
        .await
        .expect("write from acknowledged empty base should succeed");
    assert_eq!(runtime.take_detect_states(), vec![BTreeMap::new()]);
    assert_eq!(
        keyed_entity_values(&session_a, "/shared.keyed").await,
        BTreeMap::from([
            ("b".to_string(), "0".to_string()),
            ("x".to_string(), "X".to_string()),
        ])
    );
}

#[tokio::test]
async fn plugin_concurrent_writes_preserve_disjoint_entity_edits() {
    let (_engine, session_a, session_b) = keyed_collaboration_sessions().await;
    write_file(&session_a, "/shared.keyed", b"a=0\nb=0\n".to_vec())
        .await
        .expect("seed file should write");
    read_file(&session_a, "/shared.keyed").await.unwrap();
    read_file(&session_b, "/shared.keyed").await.unwrap();

    let (write_a, write_b) = tokio::join!(
        write_file(&session_a, "/shared.keyed", b"a=A\nb=0\n".to_vec()),
        write_file(&session_b, "/shared.keyed", b"a=0\nb=B\n".to_vec()),
    );
    write_a.expect("session A concurrent edit should write");
    write_b.expect("session B concurrent edit should write");

    assert_eq!(
        keyed_entity_values(&session_a, "/shared.keyed").await,
        BTreeMap::from([
            ("a".to_string(), "A".to_string()),
            ("b".to_string(), "B".to_string())
        ])
    );
}

#[tokio::test]
async fn slatedb_plugin_concurrent_writes_preserve_disjoint_entity_edits() {
    let temp_dir = tempfile::tempdir().expect("SlateDB temp directory should create");
    let storage = lix_slatedb_storage::SlateDB::open(temp_dir.path().join("collaboration"))
        .expect("SlateDB storage should open");
    let (_engine, session_a, session_b) = keyed_collaboration_sessions_with_storage(storage).await;
    write_file(&session_a, "/shared.keyed", b"a=0\nb=0\n".to_vec())
        .await
        .expect("seed file should write");
    read_file(&session_a, "/shared.keyed").await.unwrap();
    read_file(&session_b, "/shared.keyed").await.unwrap();

    let (write_a, write_b) = tokio::join!(
        write_file(&session_a, "/shared.keyed", b"a=A\nb=0\n".to_vec()),
        write_file(&session_b, "/shared.keyed", b"a=0\nb=B\n".to_vec()),
    );
    write_a.expect("session A concurrent edit should write");
    write_b.expect("session B concurrent edit should write");

    assert_eq!(
        keyed_entity_values(&session_a, "/shared.keyed").await,
        BTreeMap::from([
            ("a".to_string(), "A".to_string()),
            ("b".to_string(), "B".to_string())
        ])
    );
}

#[tokio::test]
async fn plugin_stale_writes_use_last_writer_wins_for_same_entity() {
    let (_engine, session_a, session_b) = keyed_collaboration_sessions().await;
    write_file(&session_a, "/shared.keyed", b"a=0\n".to_vec())
        .await
        .expect("seed file should write");
    read_file(&session_a, "/shared.keyed").await.unwrap();
    read_file(&session_b, "/shared.keyed").await.unwrap();

    write_file(&session_a, "/shared.keyed", b"a=A\n".to_vec())
        .await
        .expect("session A edit should write");
    write_file(&session_b, "/shared.keyed", b"a=B\n".to_vec())
        .await
        .expect("session B edit should write last");

    assert_eq!(
        read_file(&session_a, "/shared.keyed").await.unwrap(),
        Some(b"a=B\n".to_vec())
    );
    assert_eq!(
        keyed_entity_values(&session_a, "/shared.keyed").await,
        BTreeMap::from([("a".to_string(), "B".to_string())])
    );
}

#[tokio::test]
async fn raw_stale_writes_use_whole_file_last_writer_wins() {
    let storage = Memory::new();
    let receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage).await.expect("engine should open");
    let session_a = engine
        .open_session(receipt.main_branch_id.clone())
        .await
        .expect("session A should open");
    let session_b = engine
        .open_session(receipt.main_branch_id)
        .await
        .expect("session B should open");

    write_file(&session_a, "/shared.bin", b"seed".to_vec())
        .await
        .expect("seed file should write");
    read_file(&session_a, "/shared.bin").await.unwrap();
    read_file(&session_b, "/shared.bin").await.unwrap();
    write_file(&session_a, "/shared.bin", b"first".to_vec())
        .await
        .expect("first raw write should succeed");
    write_file(&session_b, "/shared.bin", b"last".to_vec())
        .await
        .expect("last raw write should succeed");

    assert_eq!(
        read_file(&session_a, "/shared.bin").await.unwrap(),
        Some(b"last".to_vec())
    );
}

#[tokio::test]
async fn plugin_omission_deletes_entity_seen_by_writer() {
    let (_engine, session_a, session_b) = keyed_collaboration_sessions().await;
    write_file(&session_a, "/shared.keyed", b"root=0\n".to_vec())
        .await
        .expect("seed file should write");
    read_file(&session_a, "/shared.keyed").await.unwrap();
    read_file(&session_b, "/shared.keyed").await.unwrap();

    write_file(&session_a, "/shared.keyed", b"remote=R\nroot=0\n".to_vec())
        .await
        .expect("remote entity should write");
    assert_eq!(
        read_file(&session_b, "/shared.keyed").await.unwrap(),
        Some(b"remote=R\nroot=0\n".to_vec())
    );
    read_file(&session_a, "/shared.keyed").await.unwrap();
    write_file(&session_a, "/shared.keyed", b"remote=R\nroot=A\n".to_vec())
        .await
        .expect("unrelated later edit should write");

    write_file(&session_b, "/shared.keyed", b"root=0\n".to_vec())
        .await
        .expect("seen remote entity omission should delete it");

    assert_eq!(
        read_file(&session_a, "/shared.keyed").await.unwrap(),
        Some(b"root=A\n".to_vec())
    );
    assert_eq!(
        keyed_entity_values(&session_a, "/shared.keyed").await,
        BTreeMap::from([("root".to_string(), "A".to_string())])
    );
}

#[tokio::test]
async fn plugin_stale_omission_preserves_unseen_remote_entity() {
    let (_engine, session_a, session_b) = keyed_collaboration_sessions().await;
    write_file(&session_a, "/shared.keyed", b"root=0\n".to_vec())
        .await
        .expect("seed file should write");
    read_file(&session_a, "/shared.keyed").await.unwrap();
    read_file(&session_b, "/shared.keyed").await.unwrap();

    write_file(&session_a, "/shared.keyed", b"remote=R\nroot=0\n".to_vec())
        .await
        .expect("remote entity should write");
    write_file(&session_b, "/shared.keyed", b"root=B\n".to_vec())
        .await
        .expect("stale session edit should write");

    assert_eq!(
        read_file(&session_a, "/shared.keyed").await.unwrap(),
        Some(b"remote=R\nroot=B\n".to_vec())
    );
    assert_eq!(
        keyed_entity_values(&session_a, "/shared.keyed").await,
        BTreeMap::from([
            ("remote".to_string(), "R".to_string()),
            ("root".to_string(), "B".to_string()),
        ])
    );
}

#[tokio::test]
async fn atelier_markdown_point_read_acknowledges_only_delivered_plugin_state() {
    let (_engine, session_a, session_b) = keyed_collaboration_sessions().await;
    write_file(&session_a, "/shared.keyed", b"root=0\nseen=S\n".to_vec())
        .await
        .expect("seed file should write");
    let file = session_a
        .execute(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text("/shared.keyed".to_string())],
        )
        .await
        .expect("file id should load");
    let [Value::Text(file_id)] = file.rows()[0].values() else {
        panic!("expected keyed file id");
    };

    // This is the point-read shape emitted by Atelier's Kysely Markdown view:
    // a projected branch constant precedes the identity placeholder and the
    // unique row is guarded by LIMIT 1.
    let delivered = session_b
        .execute(
            "SELECT file.data AS data, file.path AS path, \
                    file.lixcol_change_id AS change_id, ? AS active_branch_id \
             FROM lix_file AS file WHERE file.id = ? LIMIT 1",
            &[
                Value::Text("main-branch".to_string()),
                Value::Text(file_id.clone()),
            ],
        )
        .await
        .expect("Atelier-shaped point read should execute");
    assert_eq!(delivered.len(), 1);

    write_file(
        &session_a,
        "/shared.keyed",
        b"remote=R\nroot=0\nseen=S\n".to_vec(),
    )
    .await
    .expect("remote entity should write");
    write_file(&session_b, "/shared.keyed", b"root=B\n".to_vec())
        .await
        .expect("stale Atelier edit should write");

    assert_eq!(
        read_file(&session_a, "/shared.keyed").await.unwrap(),
        Some(b"remote=R\nroot=B\n".to_vec())
    );
    assert_eq!(
        keyed_entity_values(&session_a, "/shared.keyed").await,
        BTreeMap::from([
            ("remote".to_string(), "R".to_string()),
            ("root".to_string(), "B".to_string()),
        ])
    );
}

#[tokio::test]
async fn lixray_batch_read_acknowledges_only_delivered_plugin_state() {
    let (_engine, session_a, session_b) = keyed_collaboration_sessions().await;
    write_file(&session_a, "/shared.keyed", b"root=0\nseen=S\n".to_vec())
        .await
        .expect("seed file should write");

    let delivered = session_b
        .execute(
            "SELECT path, data FROM lix_file WHERE path IN ($1, $2)",
            &[
                Value::Text("/missing.keyed".to_string()),
                Value::Text("/shared.keyed".to_string()),
            ],
        )
        .await
        .expect("Lixray-shaped batch read should execute");
    assert_eq!(delivered.len(), 1);
    assert_eq!(
        delivered.rows()[0].values(),
        &[
            Value::Text("/shared.keyed".to_string()),
            Value::Blob(b"root=0\nseen=S\n".to_vec().into()),
        ]
    );

    write_file(
        &session_a,
        "/shared.keyed",
        b"remote=R\nroot=0\nseen=S\n".to_vec(),
    )
    .await
    .expect("remote entity should write");
    write_file(&session_b, "/shared.keyed", b"root=B\n".to_vec())
        .await
        .expect("stale Lixray edit should write");

    assert_eq!(
        read_file(&session_a, "/shared.keyed").await.unwrap(),
        Some(b"remote=R\nroot=B\n".to_vec())
    );
    assert_eq!(
        keyed_entity_values(&session_a, "/shared.keyed").await,
        BTreeMap::from([
            ("remote".to_string(), "R".to_string()),
            ("root".to_string(), "B".to_string()),
        ])
    );
}

#[tokio::test]
async fn late_file_data_read_acknowledges_only_delivered_plugin_state() {
    let (_engine, session_a, session_b) = keyed_collaboration_sessions().await;
    write_file(&session_a, "/shared.keyed", b"root=0\nseen=S\n".to_vec())
        .await
        .expect("seed file should write");

    let delivered = session_b
        .execute(
            "SELECT path, data FROM lix_file WHERE path LIKE $1 ORDER BY path LIMIT 1",
            &[Value::Text("/shared.%".to_string())],
        )
        .await
        .expect("late-materialized file read should execute");
    assert_eq!(delivered.len(), 1);
    assert_eq!(
        delivered.rows()[0].values(),
        &[
            Value::Text("/shared.keyed".to_string()),
            Value::Blob(b"root=0\nseen=S\n".to_vec().into()),
        ]
    );

    write_file(
        &session_a,
        "/shared.keyed",
        b"remote=R\nroot=0\nseen=S\n".to_vec(),
    )
    .await
    .expect("remote entity should write");
    write_file(&session_b, "/shared.keyed", b"root=B\n".to_vec())
        .await
        .expect("stale late-materialized edit should write");

    assert_eq!(
        read_file(&session_a, "/shared.keyed").await.unwrap(),
        Some(b"remote=R\nroot=B\n".to_vec())
    );
    assert_eq!(
        keyed_entity_values(&session_a, "/shared.keyed").await,
        BTreeMap::from([
            ("remote".to_string(), "R".to_string()),
            ("root".to_string(), "B".to_string()),
        ])
    );
}

#[tokio::test]
async fn observe_point_read_acknowledges_delivered_plugin_state_per_session() {
    let (_engine, session_a, session_b) = keyed_collaboration_sessions().await;
    write_file(&session_a, "/shared.keyed", b"a=0\nb=0\n".to_vec())
        .await
        .expect("seed file should write");

    let query = "SELECT data FROM lix_file WHERE path = $1";
    let params = [Value::Text("/shared.keyed".to_string())];
    let mut evaluator_events = session_a
        .observe(query, &params)
        .expect("evaluator observation should open");
    let mut events = session_b
        .observe(query, &params)
        .expect("plugin file observation should open");
    evaluator_events
        .next()
        .await
        .expect("shared observation evaluator should execute")
        .expect("shared evaluator event should be delivered");
    let initial = events
        .next()
        .await
        .expect("initial observation should execute")
        .expect("initial observation should be delivered");
    assert_eq!(
        initial.rows.rows()[0].values(),
        &[Value::Blob(b"a=0\nb=0\n".to_vec().into())]
    );

    write_file(&session_a, "/shared.keyed", b"a=A\nb=0\n".to_vec())
        .await
        .expect("concurrent edit should write");
    write_file(&session_b, "/shared.keyed", b"a=0\nb=B\n".to_vec())
        .await
        .expect("stale observed edit should merge");

    assert_eq!(
        read_file(&session_a, "/shared.keyed").await.unwrap(),
        Some(b"a=A\nb=B\n".to_vec())
    );
}

#[tokio::test]
async fn observe_late_subscriber_acknowledges_fresh_plugin_incarnation_after_unchanged_bytes() {
    let (engine, session_a, session_b) = keyed_collaboration_sessions().await;
    let session_c = engine
        .open_session(
            session_a
                .active_branch_id()
                .await
                .expect("active branch should load"),
        )
        .await
        .expect("late subscriber session should open");
    write_file(&session_a, "/shared.keyed", b"a=0\n".to_vec())
        .await
        .expect("seed file should write");

    let query = "SELECT data FROM lix_file WHERE path = $1";
    let params = [Value::Text("/shared.keyed".to_string())];
    let mut first = session_a
        .observe(query, &params)
        .expect("first observation should open");
    let mut peer = session_b
        .observe(query, &params)
        .expect("peer observation should open");
    first
        .next()
        .await
        .expect("first observation should execute")
        .expect("first snapshot should be delivered");
    peer.next()
        .await
        .expect("peer observation should execute")
        .expect("peer snapshot should be delivered");

    // Recreate the plugin owner while preserving the rendered bytes. The next
    // shared evaluation must keep the fresh owner incarnation even though its
    // visible rows compare equal to the prior generation.
    session_a
        .execute(
            "UPDATE lix_file SET path = '/shared.bin' WHERE path = '/shared.keyed'",
            &[],
        )
        .await
        .expect("plugin-to-raw transition should succeed");
    session_a
        .execute(
            "UPDATE lix_file SET path = '/shared.keyed' WHERE path = '/shared.bin'",
            &[],
        )
        .await
        .expect("raw-to-plugin transition should succeed");
    assert!(
        tokio::time::timeout(Duration::from_millis(250), first.next())
            .await
            .is_err(),
        "unchanged bytes should not emit an observation"
    );

    let mut late = session_c
        .observe(query, &params)
        .expect("late observation should open");
    let late_initial = late
        .next()
        .await
        .expect("late observation should execute")
        .expect("late initial snapshot should be delivered");
    assert_eq!(
        late_initial.rows.rows()[0].values(),
        &[Value::Blob(b"a=0\n".to_vec().into())]
    );

    write_file(&session_a, "/shared.keyed", b"a=0\nb=remote\n".to_vec())
        .await
        .expect("remote entity should write");
    write_file(&session_c, "/shared.keyed", Vec::new())
        .await
        .expect("late subscriber deletion should write");
    assert_eq!(
        keyed_entity_values(&session_a, "/shared.keyed").await,
        BTreeMap::from([("b".to_string(), "remote".to_string())]),
        "the late subscriber may delete acknowledged state but not unseen state"
    );
}

#[tokio::test]
async fn plugin_data_used_by_aggregate_does_not_acknowledge_remote_entities() {
    let (_engine, session_a, session_b) = keyed_collaboration_sessions().await;
    write_file(&session_a, "/shared.keyed", b"root=0\n".to_vec())
        .await
        .expect("seed file should write");
    read_file(&session_b, "/shared.keyed").await.unwrap();

    write_file(&session_a, "/shared.keyed", b"remote=R\nroot=0\n".to_vec())
        .await
        .expect("remote entity should write");
    let count = session_b
        .execute(
            "SELECT COUNT(data) AS file_count FROM lix_file WHERE path = $1",
            &[Value::Text("/shared.keyed".to_string())],
        )
        .await
        .expect("aggregate should read successfully");
    assert_eq!(count.rows()[0].values(), &[Value::Integer(1)]);

    write_file(&session_b, "/shared.keyed", b"root=B\n".to_vec())
        .await
        .expect("stale session edit should write");

    assert_eq!(
        read_file(&session_a, "/shared.keyed").await.unwrap(),
        Some(b"remote=R\nroot=B\n".to_vec())
    );
}

#[tokio::test]
async fn native_file_read_acknowledges_rendered_plugin_entities() {
    let (_engine, session_a, session_b) = keyed_collaboration_sessions().await;
    write_file(&session_a, "/shared.keyed", b"root=0\n".to_vec())
        .await
        .expect("seed file should write");

    assert_eq!(
        session_b
            .read_file_data("/shared.keyed".to_string())
            .await
            .expect("initial native plugin file read")
            .map(|data| data.to_vec()),
        Some(b"root=0\n".to_vec())
    );

    write_file(&session_a, "/shared.keyed", b"remote=R\nroot=0\n".to_vec())
        .await
        .expect("remote entity should write");
    assert_eq!(
        session_b
            .read_file_data("/shared.keyed".to_string())
            .await
            .expect("updated native plugin file read")
            .map(|data| data.to_vec()),
        Some(b"remote=R\nroot=0\n".to_vec())
    );

    // The second native read delivered the remote entity, so a later client
    // omission intentionally removes it instead of treating it as unseen
    // concurrent state that must be preserved.
    write_file(&session_b, "/shared.keyed", b"root=B\n".to_vec())
        .await
        .expect("acknowledged client edit should write");
    assert_eq!(
        keyed_entity_values(&session_a, "/shared.keyed").await,
        BTreeMap::from([("root".to_string(), "B".to_string())])
    );
}

#[tokio::test]
async fn plugin_point_read_filtered_by_null_does_not_acknowledge_remote_entities() {
    let (_engine, session_a, session_b) = keyed_collaboration_sessions().await;
    write_file(&session_a, "/shared.keyed", b"root=0\n".to_vec())
        .await
        .expect("seed file should write");
    read_file(&session_b, "/shared.keyed").await.unwrap();

    write_file(&session_a, "/shared.keyed", b"remote=R\nroot=0\n".to_vec())
        .await
        .expect("remote entity should write");
    let filtered = session_b
        .execute("SELECT data FROM lix_file WHERE path = $1", &[Value::Null])
        .await
        .expect("null point read should execute");
    assert!(filtered.is_empty());

    write_file(&session_b, "/shared.keyed", b"root=B\n".to_vec())
        .await
        .expect("stale session edit should write");

    assert_eq!(
        read_file(&session_a, "/shared.keyed").await.unwrap(),
        Some(b"remote=R\nroot=B\n".to_vec())
    );
}

#[tokio::test]
async fn durable_owner_drives_raw_plugin_raw_path_transitions() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let runtime = Arc::new(SentinelPluginRuntime::default());
    let engine = Engine::new_with_wasm_runtime(storage, runtime)
        .await
        .expect("engine should open with plugin runtime");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    install_plugin(&session, "plugin_sentinel", &sentinel_plugin_archive())
        .await
        .expect("plugin install should succeed");
    write_file(&session, "/transition.txt", b"raw".to_vec())
        .await
        .expect("raw file write should succeed");
    let id_rows = session
        .execute(
            "SELECT id FROM lix_file WHERE path = '/transition.txt'",
            &[],
        )
        .await
        .expect("transition file id should load");
    let [Value::Text(file_id)] = id_rows.rows()[0].values() else {
        panic!("expected transition file id");
    };
    let file_id = file_id.clone();

    session
        .execute(
            "UPDATE lix_file SET path = '/transition.sentinel' \
             WHERE path = '/transition.txt'",
            &[],
        )
        .await
        .expect("raw-to-plugin path move should reconcile bytes");
    assert_eq!(
        read_file(&session, "/transition.sentinel")
            .await
            .expect("plugin-owned file should read"),
        Some(b"plugin-rendered".to_vec())
    );
    assert_eq!(plugin_owner_count(&session, &file_id).await, 1);
    assert_eq!(plugin_state_count(&session, &file_id).await, 1);

    session
        .execute(
            "UPDATE lix_file SET path = '/transition.txt' \
             WHERE path = '/transition.sentinel'",
            &[],
        )
        .await
        .expect("plugin-to-raw path move should materialize rendered bytes");
    assert_eq!(
        read_file(&session, "/transition.txt")
            .await
            .expect("materialized raw file should read"),
        Some(b"plugin-rendered".to_vec())
    );
    assert_eq!(plugin_owner_count(&session, &file_id).await, 0);
    assert_eq!(plugin_state_count(&session, &file_id).await, 0);

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn remote_plugin_raw_plugin_transition_does_not_reuse_stale_session_view() {
    let (_engine, session_a, session_b) = keyed_collaboration_sessions().await;
    write_file(&session_a, "/shared.keyed", b"a=0\n".to_vec())
        .await
        .expect("plugin file should write");
    read_file(&session_b, "/shared.keyed")
        .await
        .expect("session B should cache the plugin incarnation");

    session_a
        .execute(
            "UPDATE lix_file SET path = '/shared.bin' WHERE path = '/shared.keyed'",
            &[],
        )
        .await
        .expect("plugin-to-raw transition should succeed");
    assert_eq!(
        read_file(&session_a, "/shared.bin").await.unwrap(),
        Some(b"a=0\n".to_vec())
    );
    write_file(&session_a, "/shared.bin", b"a=new\n".to_vec())
        .await
        .expect("raw incarnation should update");

    session_a
        .execute(
            "UPDATE lix_file SET path = '/shared.keyed' WHERE path = '/shared.bin'",
            &[],
        )
        .await
        .expect("raw-to-plugin transition should succeed");
    assert_eq!(
        read_file(&session_a, "/shared.keyed").await.unwrap(),
        Some(b"a=new\n".to_vec())
    );

    // B's pre-transition view has the same plugin key and generation, but an
    // older durable owner incarnation. Its omission must not delete the new
    // incarnation's same-ID entity.
    write_file(&session_b, "/shared.keyed", Vec::new())
        .await
        .expect("stale pre-transition write should succeed conservatively");
    assert_eq!(
        keyed_entity_values(&session_a, "/shared.keyed").await,
        BTreeMap::from([("a".to_string(), "new".to_string())])
    );
}

#[tokio::test]
async fn multi_value_file_upsert_reconciles_each_plugin_file() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let runtime = Arc::new(SentinelPluginRuntime::default());
    let engine = Engine::new_with_wasm_runtime(storage, runtime.clone())
        .await
        .expect("engine should open with plugin runtime");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    install_plugin(&session, "plugin_sentinel", &sentinel_plugin_archive())
        .await
        .expect("plugin install should succeed");
    let sql = "INSERT INTO lix_file (path, data, lixcol_metadata) \
               VALUES ($1, $2, $3), ($4, $5, $6) \
               ON CONFLICT (path) DO UPDATE SET \
                 data = excluded.data, lixcol_metadata = excluded.lixcol_metadata";
    let params = [
        Value::Text("/nested/first.sentinel".to_string()),
        Value::Blob(b"first".to_vec().into()),
        Value::Json(json!({"size": 5})),
        Value::Text("/nested/second.sentinel".to_string()),
        Value::Blob(b"second".to_vec().into()),
        Value::Json(json!({"size": 6})),
    ];
    session
        .execute(sql, &params)
        .await
        .expect("multi-value plugin file upsert should succeed");
    session
        .execute(sql, &params)
        .await
        .expect("multi-value plugin file overwrite should succeed");

    let filenames = runtime
        .detect_filenames
        .lock()
        .expect("detect filename lock should not be poisoned")
        .clone();
    assert_eq!(
        filenames,
        vec![
            Some("first.sentinel".to_string()),
            Some("second.sentinel".to_string()),
            Some("first.sentinel".to_string()),
            Some("second.sentinel".to_string()),
        ]
    );

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn empty_write_to_binary_plugin_file_clears_plugin_state() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let runtime = Arc::new(SentinelPluginRuntime::default());
    let engine = Engine::new_with_wasm_runtime(storage, runtime)
        .await
        .expect("engine should open with plugin runtime");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    install_plugin(
        &session,
        "plugin_binary_sentinel",
        &binary_sentinel_plugin_archive(),
    )
    .await
    .expect("plugin install should succeed");

    write_file(&session, "/owned.binary-sentinel", vec![0xff])
        .await
        .expect("binary plugin write should succeed");

    assert_eq!(
        read_file(&session, "/owned.binary-sentinel")
            .await
            .expect("file read should render plugin state"),
        Some(b"plugin-rendered".to_vec())
    );

    let file_id_rows = session
        .execute(
            "SELECT id FROM lix_file WHERE path = '/owned.binary-sentinel'",
            &[],
        )
        .await
        .expect("file id read should succeed");
    let [Value::Text(file_id)] = file_id_rows.rows()[0].values() else {
        panic!(
            "expected file id row, got {:?}",
            file_id_rows.rows()[0].values()
        );
    };

    assert_eq!(plugin_state_count(&session, file_id).await, 1);

    write_file(&session, "/owned.binary-sentinel", Vec::new())
        .await
        .expect("empty plugin write should succeed");

    assert_eq!(
        read_file(&session, "/owned.binary-sentinel")
            .await
            .expect("zero-state owned file should still render through its durable owner"),
        Some(b"plugin-rendered".to_vec())
    );

    assert_eq!(plugin_state_count(&session, file_id).await, 0);

    session.close().await.expect("session should close");
}

simulation_test!(
    sql_mkdir_is_idempotent_and_readdir_distinguishes_missing,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("workspace session should open"),
            &engine,
        );

        mkdir(&session, "/empty/nested/")
            .await
            .expect("mkdir should create parents");
        mkdir(&session, "/empty/nested/")
            .await
            .expect("mkdir should be idempotent");

        assert_eq!(
            readdir(&session, "/empty/nested/")
                .await
                .expect("directory read should succeed"),
            Some(Vec::new())
        );
        assert_eq!(
            readdir(&session, "/missing/")
                .await
                .expect("missing directory read should succeed"),
            None
        );
    }
);

simulation_test!(sql_write_file_upserts_existing_data, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_workspace_session()
            .await
            .expect("workspace session should open"),
        &engine,
    );

    write_file(&session, "/orders.xlsx", b"old".to_vec())
        .await
        .expect("initial write should succeed");
    write_file(&session, "/orders.xlsx", b"new".to_vec())
        .await
        .expect("second write should upsert");

    assert_eq!(
        read_file(&session, "/orders.xlsx")
            .await
            .expect("read should succeed"),
        Some(b"new".to_vec())
    );

    let rows = session
        .execute("SELECT id FROM lix_file WHERE path = '/orders.xlsx'", &[])
        .await
        .expect("query should succeed");
    assert_eq!(rows.len(), 1, "file upsert should not duplicate descriptor");

    write_file(&session, "/orders.xlsx", Vec::new())
        .await
        .expect("empty overwrite should succeed");
    assert_eq!(
        read_file(&session, "/orders.xlsx")
            .await
            .expect("read should succeed"),
        Some(Vec::new())
    );
});

simulation_test!(sql_rm_file_and_recursive_directory, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_workspace_session()
            .await
            .expect("workspace session should open"),
        &engine,
    );

    write_file(&session, "/tmp/a.txt", b"a".to_vec())
        .await
        .expect("write should succeed");
    write_file(&session, "/tmp/nested/b.txt", b"b".to_vec())
        .await
        .expect("nested write should succeed");

    rm_path(&session, "/tmp/")
        .await
        .expect("recursive directory delete should remove tree");

    assert_eq!(
        read_file(&session, "/tmp/a.txt")
            .await
            .expect("read should succeed"),
        None
    );
    assert_eq!(
        readdir(&session, "/tmp/")
            .await
            .expect("directory read should succeed"),
        None
    );

    rm_path(&session, "/tmp/missing.txt")
        .await
        .expect("missing delete should be a no-op");
});

simulation_test!(sql_file_directory_path_constraints, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_workspace_session()
            .await
            .expect("workspace session should open"),
        &engine,
    );

    mkdir(&session, "/docs/")
        .await
        .expect("mkdir should succeed");
    write_file(&session, "/file.txt", b"file".to_vec())
        .await
        .expect("write should succeed");

    write_file(&session, "/docs", b"nope".to_vec())
        .await
        .expect_err("file write over directory should fail");
    mkdir(&session, "/file.txt/")
        .await
        .expect_err("directory create over file should fail");
});

#[async_trait(?Send)]
trait TestSession {
    async fn execute_sql(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, LixError>;
}

#[async_trait(?Send)]
impl<StorageImpl> TestSession for SessionContext<StorageImpl>
where
    StorageImpl: lix_engine::storage::Storage + Clone + Send + Sync + 'static,
{
    async fn execute_sql(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, LixError> {
        self.execute(sql, params).await
    }
}

#[async_trait(?Send)]
impl TestSession for support::simulation_test::engine::SimSession {
    async fn execute_sql(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, LixError> {
        self.execute(sql, params).await
    }
}

async fn install_plugin<S>(session: &S, key: &str, archive: &[u8]) -> Result<(), LixError>
where
    S: TestSession + Sync + ?Sized,
{
    write_file(
        session,
        &format!("/.lix/plugins/{key}.lixplugin"),
        archive.to_vec(),
    )
    .await
}

async fn keyed_collaboration_sessions() -> (Engine, SessionContext, SessionContext) {
    keyed_collaboration_sessions_with_storage(Memory::new()).await
}

async fn keyed_collaboration_sessions_with_storage<StorageImpl>(
    storage: StorageImpl,
) -> (
    Engine<StorageImpl>,
    SessionContext<StorageImpl>,
    SessionContext<StorageImpl>,
)
where
    StorageImpl: lix_engine::storage::Storage + Clone + Send + Sync + 'static,
{
    keyed_collaboration_sessions_with_runtime(storage, Arc::new(KeyedPluginRuntime::default()))
        .await
}

async fn keyed_collaboration_sessions_with_runtime<StorageImpl>(
    storage: StorageImpl,
    runtime: Arc<KeyedPluginRuntime>,
) -> (
    Engine<StorageImpl>,
    SessionContext<StorageImpl>,
    SessionContext<StorageImpl>,
)
where
    StorageImpl: lix_engine::storage::Storage + Clone + Send + Sync + 'static,
{
    let receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new_with_wasm_runtime(storage, runtime)
        .await
        .expect("engine should open with keyed plugin runtime");
    let installer = engine
        .open_session(receipt.main_branch_id.clone())
        .await
        .expect("installer session should open");
    install_plugin(&installer, "plugin_keyed", &keyed_plugin_archive())
        .await
        .expect("keyed plugin should install");
    installer.close().await.expect("installer should close");
    let session_a = engine
        .open_session(receipt.main_branch_id.clone())
        .await
        .expect("session A should open");
    let session_b = engine
        .open_session(receipt.main_branch_id)
        .await
        .expect("session B should open");
    (engine, session_a, session_b)
}

async fn keyed_entity_values<S>(session: &S, path: &str) -> BTreeMap<String, String>
where
    S: TestSession + Sync + ?Sized,
{
    let file = session
        .execute_sql(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await
        .expect("keyed file id should load");
    let [Value::Text(file_id)] = file.rows()[0].values() else {
        panic!("expected keyed file id");
    };
    let entities = session
        .execute_sql(
            "SELECT id, value FROM plugin_note \
             WHERE lixcol_file_id = $1 ORDER BY id",
            &[Value::Text(file_id.clone())],
        )
        .await
        .expect("keyed entities should load");
    entities
        .rows()
        .iter()
        .map(|row| match row.values() {
            [Value::Text(id), Value::Text(value)] => (id.clone(), value.clone()),
            other => panic!("expected keyed id/value row, got {other:?}"),
        })
        .collect()
}

async fn write_file<S>(session: &S, path: &str, data: Vec<u8>) -> Result<(), LixError>
where
    S: TestSession + Sync + ?Sized,
{
    session
        .execute_sql(
            "INSERT INTO lix_file (path, data) VALUES ($1, $2) \
             ON CONFLICT (path) DO UPDATE SET data = excluded.data",
            &[Value::Text(path.to_string()), Value::Blob(data.into())],
        )
        .await?;
    Ok(())
}

async fn read_file<S>(session: &S, path: &str) -> Result<Option<Vec<u8>>, LixError>
where
    S: TestSession + Sync + ?Sized,
{
    let result = session
        .execute_sql(
            "SELECT data FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await?;
    let Some(row) = result.rows().first() else {
        return Ok(None);
    };
    match row.values() {
        [Value::Blob(data)] => Ok(Some(data.to_vec())),
        [Value::Null] => Ok(Some(Vec::new())),
        other => panic!("expected one blob data column, got {other:?}"),
    }
}

async fn registered_plugin_note_schema_count<S>(session: &S) -> usize
where
    S: TestSession + Sync + ?Sized,
{
    session
        .execute_sql(
            "SELECT value FROM lix_registered_schema \
             WHERE lixcol_entity_pk = lix_json('[\"plugin_note\"]')",
            &[],
        )
        .await
        .expect("plugin schema lookup should succeed")
        .len()
}

async fn plugin_owner_count<S>(session: &S, file_id: &str) -> usize
where
    S: TestSession + Sync + ?Sized,
{
    session
        .execute_sql(
            "SELECT key FROM lix_key_value \
             WHERE key = 'lix_plugin_owner_v1' \
               AND lixcol_file_id = $1",
            &[Value::Text(file_id.to_string())],
        )
        .await
        .expect("plugin owner lookup should succeed")
        .len()
}

async fn plugin_state_count<S>(session: &S, file_id: &str) -> usize
where
    S: TestSession + Sync + ?Sized,
{
    session
        .execute_sql(
            "SELECT id FROM plugin_note WHERE lixcol_file_id = $1",
            &[Value::Text(file_id.to_string())],
        )
        .await
        .expect("plugin state lookup should succeed")
        .len()
}

async fn blob_ref_change_count<S>(session: &S, file_id: &str) -> usize
where
    S: TestSession + Sync + ?Sized,
{
    session
        .execute_sql(
            "SELECT id FROM lix_change \
             WHERE schema_key = 'lix_binary_blob_ref' \
               AND lix_json_get_text(entity_pk, 0) = $1",
            &[Value::Text(file_id.to_string())],
        )
        .await
        .expect("blob ref change lookup should succeed")
        .len()
}

async fn mkdir<S>(session: &S, path: &str) -> Result<(), LixError>
where
    S: TestSession + Sync + ?Sized,
{
    session
        .execute_sql(
            "INSERT INTO lix_directory (path) VALUES ($1) \
             ON CONFLICT (path) DO NOTHING",
            &[Value::Text(path.to_string())],
        )
        .await?;
    Ok(())
}

async fn rm_path<S>(session: &S, path: &str) -> Result<(), LixError>
where
    S: TestSession + Sync + ?Sized,
{
    session
        .execute_sql(
            "DELETE FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await?;
    session
        .execute_sql(
            "DELETE FROM lix_directory WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await?;
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
struct DirEntry {
    name: String,
    path: String,
    kind: DirEntryKind,
}

#[derive(Debug, PartialEq, Eq)]
enum DirEntryKind {
    File,
    Directory,
}

async fn readdir<S>(session: &S, path: &str) -> Result<Option<Vec<DirEntry>>, LixError>
where
    S: TestSession + Sync + ?Sized,
{
    let exists = session
        .execute_sql(
            "SELECT path FROM lix_directory WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await?;
    let children = session
        .execute_sql(
            "SELECT path, 'file' AS kind FROM lix_file WHERE path LIKE $1 \
             UNION ALL \
             SELECT path, 'directory' AS kind FROM lix_directory WHERE path LIKE $1 AND path != $2 \
             ORDER BY path",
            &[
                Value::Text(format!("{path}%")),
                Value::Text(path.to_string()),
            ],
        )
        .await?;
    let mut entries = Vec::new();
    for row in children.rows() {
        let [Value::Text(child_path), Value::Text(kind)] = row.values() else {
            panic!("expected path/kind row, got {:?}", row.values());
        };
        let Some(name) = direct_child_name(path, child_path) else {
            continue;
        };
        entries.push(DirEntry {
            name,
            path: child_path.clone(),
            kind: match kind.as_str() {
                "file" => DirEntryKind::File,
                "directory" => DirEntryKind::Directory,
                other => panic!("unexpected directory entry kind {other}"),
            },
        });
    }
    if entries.is_empty() && exists.is_empty() {
        Ok(None)
    } else {
        Ok(Some(entries))
    }
}

fn direct_child_name(parent: &str, child: &str) -> Option<String> {
    let remainder = child.strip_prefix(parent)?;
    if remainder.is_empty() {
        return None;
    }
    let trimmed = remainder.trim_end_matches('/');
    if trimmed.is_empty() || trimmed.contains('/') {
        return None;
    }
    Some(trimmed.to_string())
}

#[derive(Debug, PartialEq, Eq)]
struct InstalledPluginInfo {
    key: String,
    schema_keys: Vec<String>,
}

async fn list_installed_plugins<S>(session: &S) -> Result<Vec<InstalledPluginInfo>, LixError>
where
    S: TestSession + Sync + ?Sized,
{
    let result = session
        .execute_sql(
            "SELECT data FROM lix_file \
             WHERE path LIKE '/.lix/plugins/%.lixplugin' \
             ORDER BY path",
            &[],
        )
        .await?;
    let mut plugins = Vec::new();
    for row in result.rows() {
        let [Value::Blob(data)] = row.values() else {
            panic!("expected plugin archive data row, got {:?}", row.values());
        };
        plugins.push(plugin_info_from_archive(data)?);
    }
    Ok(plugins)
}

fn plugin_info_from_archive(archive: &[u8]) -> Result<InstalledPluginInfo, LixError> {
    let mut zip = zip::ZipArchive::new(Cursor::new(archive)).map_err(test_parse_error)?;
    let mut manifest_json = String::new();
    zip.by_name("manifest.json")
        .map_err(test_parse_error)?
        .read_to_string(&mut manifest_json)
        .map_err(test_parse_error)?;
    let manifest: serde_json::Value =
        serde_json::from_str(&manifest_json).map_err(test_parse_error)?;
    let key = manifest
        .get("key")
        .and_then(|value| value.as_str())
        .expect("test plugin manifest should include key")
        .to_string();
    let schema_paths = manifest
        .get("schemas")
        .and_then(|value| value.as_array())
        .expect("test plugin manifest should include schemas");
    let mut schema_keys = Vec::new();
    for schema_path in schema_paths {
        let schema_path = schema_path
            .as_str()
            .expect("test plugin schema path should be a string");
        let mut schema_json = String::new();
        zip.by_name(schema_path)
            .map_err(test_parse_error)?
            .read_to_string(&mut schema_json)
            .map_err(test_parse_error)?;
        let schema: serde_json::Value =
            serde_json::from_str(&schema_json).map_err(test_parse_error)?;
        schema_keys.push(
            schema
                .get("x-lix-key")
                .and_then(|value| value.as_str())
                .expect("test plugin schema should include x-lix-key")
                .to_string(),
        );
    }
    Ok(InstalledPluginInfo { key, schema_keys })
}

fn test_parse_error(error: impl std::fmt::Display) -> LixError {
    LixError::new(LixError::CODE_INTERNAL_ERROR, error.to_string())
}

#[derive(Default)]
struct KeyedPluginRuntime {
    detect_states: Arc<Mutex<Vec<BTreeMap<String, String>>>>,
}

impl KeyedPluginRuntime {
    fn take_detect_states(&self) -> Vec<BTreeMap<String, String>> {
        std::mem::take(
            &mut *self
                .detect_states
                .lock()
                .expect("detect state lock should not be poisoned"),
        )
    }
}

struct KeyedPluginComponent {
    detect_states: Arc<Mutex<Vec<BTreeMap<String, String>>>>,
}

#[async_trait]
impl WasmRuntime for KeyedPluginRuntime {
    async fn init_component(
        &self,
        _bytes: Vec<u8>,
        _limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
        Ok(Arc::new(KeyedPluginComponent {
            detect_states: Arc::clone(&self.detect_states),
        }))
    }
}

#[async_trait]
impl WasmComponentInstance for KeyedPluginComponent {
    async fn detect_changes(
        &self,
        state: Vec<WasmPluginEntityState>,
        file: WasmPluginFile,
    ) -> Result<Vec<WasmPluginDetectedChange>, LixError> {
        let current = keyed_values_from_state(state)?;
        self.detect_states
            .lock()
            .expect("detect state lock should not be poisoned")
            .push(current.clone());
        let submitted = keyed_values_from_bytes(&file.data)?;
        let mut changes = Vec::new();
        for id in current.keys() {
            if !submitted.contains_key(id) {
                changes.push(WasmPluginDetectedChange {
                    entity_pk: vec![id.clone()],
                    schema_key: "plugin_note".to_string(),
                    snapshot_content: None,
                    metadata: None,
                });
            }
        }
        for (id, value) in &submitted {
            if current.get(id) != Some(value) {
                changes.push(WasmPluginDetectedChange {
                    entity_pk: vec![id.clone()],
                    schema_key: "plugin_note".to_string(),
                    snapshot_content: Some(json!({ "id": id, "value": value }).to_string()),
                    metadata: None,
                });
            }
        }
        Ok(changes)
    }

    async fn render(&self, state: Vec<WasmPluginEntityState>) -> Result<Vec<u8>, LixError> {
        let values = keyed_values_from_state(state)?;
        let mut bytes = Vec::new();
        for (id, value) in values {
            bytes.extend_from_slice(id.as_bytes());
            bytes.push(b'=');
            bytes.extend_from_slice(value.as_bytes());
            bytes.push(b'\n');
        }
        Ok(bytes)
    }
}

fn keyed_values_from_state(
    state: Vec<WasmPluginEntityState>,
) -> Result<BTreeMap<String, String>, LixError> {
    let mut values = BTreeMap::new();
    for entity in state {
        let snapshot: serde_json::Value = serde_json::from_str(&entity.snapshot_content)
            .map_err(|error| test_parse_error(format!("invalid keyed snapshot: {error}")))?;
        let id = snapshot
            .get("id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| test_parse_error("keyed snapshot is missing id"))?;
        let value = snapshot
            .get("value")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| test_parse_error("keyed snapshot is missing value"))?;
        values.insert(id.to_string(), value.to_string());
    }
    Ok(values)
}

fn keyed_values_from_bytes(bytes: &[u8]) -> Result<BTreeMap<String, String>, LixError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|error| test_parse_error(format!("keyed file is not UTF-8: {error}")))?;
    let mut values = BTreeMap::new();
    for line in text.lines().filter(|line| !line.is_empty()) {
        let (id, value) = line
            .split_once('=')
            .ok_or_else(|| test_parse_error(format!("invalid keyed line: {line}")))?;
        values.insert(id.to_string(), value.to_string());
    }
    Ok(values)
}

#[derive(Default)]
struct SentinelPluginRuntime {
    render_calls: Arc<AtomicUsize>,
    detect_filenames: Arc<Mutex<Vec<Option<String>>>>,
}

struct SentinelPluginComponent {
    render_calls: Arc<AtomicUsize>,
    detect_filenames: Arc<Mutex<Vec<Option<String>>>>,
}

#[async_trait]
impl WasmRuntime for SentinelPluginRuntime {
    async fn init_component(
        &self,
        _bytes: Vec<u8>,
        _limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
        Ok(Arc::new(SentinelPluginComponent {
            render_calls: Arc::clone(&self.render_calls),
            detect_filenames: Arc::clone(&self.detect_filenames),
        }))
    }
}

#[async_trait]
impl WasmComponentInstance for SentinelPluginComponent {
    async fn detect_changes(
        &self,
        _state: Vec<WasmPluginEntityState>,
        file: WasmPluginFile,
    ) -> Result<Vec<WasmPluginDetectedChange>, LixError> {
        self.detect_filenames
            .lock()
            .expect("detect filename lock should not be poisoned")
            .push(file.filename.clone());
        if file.data.is_empty() {
            Ok(vec![WasmPluginDetectedChange {
                entity_pk: vec!["note".to_string()],
                schema_key: "plugin_note".to_string(),
                snapshot_content: None,
                metadata: None,
            }])
        } else {
            Ok(vec![WasmPluginDetectedChange {
                entity_pk: vec!["note".to_string()],
                schema_key: "plugin_note".to_string(),
                snapshot_content: Some("{\"id\":\"note\",\"value\":\"detected\"}".to_string()),
                metadata: None,
            }])
        }
    }

    async fn render(&self, _state: Vec<WasmPluginEntityState>) -> Result<Vec<u8>, LixError> {
        self.render_calls.fetch_add(1, Ordering::SeqCst);
        Ok(b"plugin-rendered".to_vec())
    }
}

fn sentinel_plugin_archive() -> Vec<u8> {
    const MANIFEST_JSON: &[u8] = br#"{
        "key": "plugin_sentinel",
        "runtime": "wasm-component-v1",
        "api_version": "0.1.0",
        "match": { "path_glob": "*.sentinel" },
        "entry": "plugin.wasm",
        "schemas": ["schema/plugin_note.json"]
    }"#;
    plugin_archive(MANIFEST_JSON)
}

fn keyed_plugin_archive() -> Vec<u8> {
    const MANIFEST_JSON: &[u8] = br#"{
        "key": "plugin_keyed",
        "runtime": "wasm-component-v1",
        "api_version": "0.1.0",
        "match": { "path_glob": "*.keyed" },
        "entry": "plugin.wasm",
        "schemas": ["schema/plugin_note.json"]
    }"#;
    plugin_archive(MANIFEST_JSON)
}

fn second_sentinel_plugin_archive() -> Vec<u8> {
    const MANIFEST_JSON: &[u8] = br#"{
        "key": "plugin_second",
        "runtime": "wasm-component-v1",
        "api_version": "0.1.0",
        "match": { "path_glob": "*.second" },
        "entry": "plugin.wasm",
        "schemas": ["schema/plugin_note.json"]
    }"#;
    plugin_archive(MANIFEST_JSON)
}

fn conflicting_schema_plugin_archive() -> Vec<u8> {
    const MANIFEST_JSON: &[u8] = br#"{
        "key": "plugin_conflicting",
        "runtime": "wasm-component-v1",
        "api_version": "0.1.0",
        "match": { "path_glob": "*.conflicting" },
        "entry": "plugin.wasm",
        "schemas": ["schema/plugin_note.json"]
    }"#;
    const CONFLICTING_SCHEMA_JSON: &[u8] = br#"{
        "x-lix-key": "plugin_note",
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "properties": {
            "id": { "type": "string" },
            "value": { "type": "integer" }
        },
        "required": ["id", "value"],
        "additionalProperties": false
    }"#;
    plugin_archive_with_schema(MANIFEST_JSON, CONFLICTING_SCHEMA_JSON)
}

fn broad_overlap_plugin_archive() -> Vec<u8> {
    const MANIFEST_JSON: &[u8] = br#"{
        "key": "plugin_overlap_broad",
        "runtime": "wasm-component-v1",
        "api_version": "0.1.0",
        "match": { "path_glob": "*.overlap" },
        "entry": "plugin.wasm",
        "schemas": ["schema/plugin_note.json"]
    }"#;
    plugin_archive(MANIFEST_JSON)
}

fn specific_overlap_plugin_archive() -> Vec<u8> {
    const MANIFEST_JSON: &[u8] = br#"{
        "key": "plugin_overlap_specific",
        "runtime": "wasm-component-v1",
        "api_version": "0.1.0",
        "match": { "path_glob": "*/special.overlap" },
        "entry": "plugin.wasm",
        "schemas": ["schema/plugin_note.json"]
    }"#;
    plugin_archive(MANIFEST_JSON)
}

fn invalid_glob_sentinel_plugin_archive() -> Vec<u8> {
    const MANIFEST_JSON: &[u8] = br#"{
        "key": "plugin_sentinel",
        "runtime": "wasm-component-v1",
        "api_version": "0.1.0",
        "match": { "path_glob": "*.{sentinel" },
        "entry": "plugin.wasm",
        "schemas": ["schema/plugin_note.json"]
    }"#;
    plugin_archive(MANIFEST_JSON)
}

fn binary_sentinel_plugin_archive() -> Vec<u8> {
    const MANIFEST_JSON: &[u8] = br#"{
        "key": "plugin_binary_sentinel",
        "runtime": "wasm-component-v1",
        "api_version": "0.1.0",
        "match": { "path_glob": "*.binary-sentinel" },
        "entry": "plugin.wasm",
        "schemas": ["schema/plugin_note.json"]
    }"#;
    plugin_archive(MANIFEST_JSON)
}

fn plugin_archive(manifest_json: &[u8]) -> Vec<u8> {
    const SCHEMA_JSON: &[u8] = br#"{
        "x-lix-key": "plugin_note",
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "properties": {
            "id": { "type": "string" },
            "value": { "type": "string" }
        },
        "required": ["id", "value"],
        "additionalProperties": false
    }"#;
    plugin_archive_with_schema(manifest_json, SCHEMA_JSON)
}

fn plugin_archive_with_schema(manifest_json: &[u8], schema_json: &[u8]) -> Vec<u8> {
    const WASM_HEADER: &[u8] = b"\0asm\x01\0\0\0";

    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        ("manifest.json", manifest_json),
        ("schema/plugin_note.json", schema_json),
        ("plugin.wasm", WASM_HEADER),
    ] {
        writer.start_file(path, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}
