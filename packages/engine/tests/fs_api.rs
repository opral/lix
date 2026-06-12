#[macro_use]
#[path = "support/mod.rs"]
mod support;

use std::io::{Cursor, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lix_engine::wasm::{
    WasmComponentInstance, WasmLimits, WasmPluginDetectedChange, WasmPluginEntityState,
    WasmPluginFile, WasmRuntime,
};
use lix_engine::{
    Engine, FsDirEntryKind, FsMkdirOptions, FsRmOptions, FsWriteOptions, InMemoryBackend, LixError,
    Value,
};

simulation_test!(fs_write_read_and_readdir_roundtrip, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_workspace_session()
            .await
            .expect("workspace session should open"),
        &engine,
    );

    session
        .fs
        .write_file(
            "/docs/readme.txt",
            b"hello".to_vec(),
            FsWriteOptions::default(),
        )
        .await
        .expect("write_file should create parents and data");

    assert_eq!(
        session
            .fs
            .read_file("/docs/readme.txt")
            .await
            .expect("read_file should succeed"),
        Some(b"hello".to_vec())
    );
    assert_eq!(
        session
            .fs
            .read_file("/docs/missing.txt")
            .await
            .expect("missing read should succeed"),
        None
    );

    let entries = session
        .fs
        .readdir("/docs/")
        .await
        .expect("readdir should succeed")
        .expect("directory should exist");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "readme.txt");
    assert_eq!(entries[0].path, "/docs/readme.txt");
    assert_eq!(entries[0].kind, FsDirEntryKind::File);
});

simulation_test!(
    fs_session_reads_reject_active_explicit_transaction,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = engine
            .open_workspace_session()
            .await
            .expect("workspace session should open");

        let transaction = session
            .begin_transaction()
            .await
            .expect("transaction should begin");

        let read_error = session
            .fs()
            .read_file("/docs/readme.txt")
            .await
            .expect_err("session fs read_file should reject active transaction");
        assert_eq!(read_error.code, "LIX_INVALID_TRANSACTION_STATE");

        let readdir_error = session
            .fs()
            .readdir("/docs/")
            .await
            .expect_err("session fs readdir should reject active transaction");
        assert_eq!(readdir_error.code, "LIX_INVALID_TRANSACTION_STATE");

        transaction
            .rollback()
            .await
            .expect("transaction rollback should succeed");
    }
);

simulation_test!(
    fs_read_file_treats_sql_path_only_file_as_empty,
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
            .execute("INSERT INTO lix_file (path) VALUES ('/empty.txt')", &[])
            .await
            .expect("path-only file insert should succeed");

        assert_eq!(
            session
                .fs
                .read_file("/empty.txt")
                .await
                .expect("read_file should succeed"),
            Some(Vec::new())
        );
    }
);

simulation_test!(
    fs_write_file_canonicalizes_empty_file_without_blob_ref,
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
            .fs
            .write_file("/empty-fs.txt", Vec::new(), FsWriteOptions::default())
            .await
            .expect("empty fs write should succeed");

        assert_eq!(
            session
                .fs
                .read_file("/empty-fs.txt")
                .await
                .expect("read_file should succeed"),
            Some(Vec::new())
        );

        let file_result = session
            .execute("SELECT id FROM lix_file WHERE path = '/empty-fs.txt'", &[])
            .await
            .expect("file id read should succeed");
        let [Value::Text(file_id)] = file_result.rows()[0].values() else {
            panic!(
                "expected file id row, got {:?}",
                file_result.rows()[0].values()
            );
        };

        let blob_ref_result = session
            .execute(
                &format!(
                    "SELECT entity_pk \
                 FROM lix_state \
                 WHERE schema_key = 'lix_binary_blob_ref' \
                   AND entity_pk = lix_json('[\"{file_id}\"]')"
                ),
                &[],
            )
            .await
            .expect("blob ref state read should succeed");
        assert_eq!(blob_ref_result.len(), 0);
    }
);

#[tokio::test]
async fn fs_write_file_to_plugin_storage_installs_plugin_archive() {
    let backend = InMemoryBackend::new();
    Engine::initialize(backend.clone())
        .await
        .expect("backend should initialize");
    let engine = Engine::new(backend).await.expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");
    let archive = sentinel_plugin_archive();

    session
        .fs()
        .write_file(
            "/.lix_system/plugins/plugin_sentinel.lixplugin",
            archive,
            FsWriteOptions::default(),
        )
        .await
        .expect("fs.write_file should install plugin archive");

    let plugins = session
        .list_installed_plugins()
        .await
        .expect("installed plugins should list");
    assert_eq!(plugins.len(), 1);
    assert_eq!(plugins[0].key, "plugin_sentinel");

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn empty_regular_file_does_not_render_through_later_installed_plugin() {
    let backend = InMemoryBackend::new();
    Engine::initialize(backend.clone())
        .await
        .expect("backend should initialize");
    let runtime = Arc::new(SentinelPluginRuntime::default());
    let engine = Engine::new_with_wasm_runtime(backend, runtime.clone())
        .await
        .expect("engine should open with plugin runtime");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    session
        .fs()
        .write_file("/raw.sentinel", Vec::new(), FsWriteOptions::default())
        .await
        .expect("empty file write should succeed");
    session
        .install_plugin(&sentinel_plugin_archive())
        .await
        .expect("plugin install should succeed");

    assert_eq!(
        session
            .fs()
            .read_file("/raw.sentinel")
            .await
            .expect("read_file should succeed"),
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
    assert_eq!(files.rows()[0].values(), &[Value::Blob(Vec::new())]);
    assert_eq!(runtime.render_calls.load(Ordering::SeqCst), 0);

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn plugin_detect_changes_receives_descriptor_filename() {
    let backend = InMemoryBackend::new();
    Engine::initialize(backend.clone())
        .await
        .expect("backend should initialize");
    let runtime = Arc::new(SentinelPluginRuntime::default());
    let engine = Engine::new_with_wasm_runtime(backend, runtime.clone())
        .await
        .expect("engine should open with plugin runtime");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    session
        .install_plugin(&sentinel_plugin_archive())
        .await
        .expect("plugin install should succeed");
    session
        .fs()
        .write_file(
            "/nested/raw.sentinel",
            b"hello".to_vec(),
            FsWriteOptions::default(),
        )
        .await
        .expect("plugin file write should succeed");

    let filenames = runtime
        .detect_filenames
        .lock()
        .expect("detect filename lock should not be poisoned")
        .clone();
    assert_eq!(filenames, vec![Some("raw.sentinel".to_string())]);

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn empty_write_to_binary_plugin_file_clears_plugin_state() {
    let backend = InMemoryBackend::new();
    Engine::initialize(backend.clone())
        .await
        .expect("backend should initialize");
    let runtime = Arc::new(SentinelPluginRuntime::default());
    let engine = Engine::new_with_wasm_runtime(backend, runtime)
        .await
        .expect("engine should open with plugin runtime");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    session
        .install_plugin(&binary_sentinel_plugin_archive())
        .await
        .expect("plugin install should succeed");

    session
        .fs()
        .write_file(
            "/owned.binary-sentinel",
            vec![0xff],
            FsWriteOptions::default(),
        )
        .await
        .expect("binary plugin write should succeed");

    assert_eq!(
        session
            .fs()
            .read_file("/owned.binary-sentinel")
            .await
            .expect("read_file should render plugin state"),
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

    let plugin_rows = session
        .execute(
            &format!(
                "SELECT entity_pk \
                 FROM lix_state \
                 WHERE schema_key = 'plugin_note' \
                   AND file_id = '{file_id}'"
            ),
            &[],
        )
        .await
        .expect("plugin state read should succeed");
    assert_eq!(plugin_rows.len(), 1);

    session
        .fs()
        .write_file(
            "/owned.binary-sentinel",
            Vec::new(),
            FsWriteOptions::default(),
        )
        .await
        .expect("empty plugin write should succeed");

    assert_eq!(
        session
            .fs()
            .read_file("/owned.binary-sentinel")
            .await
            .expect("read_file should succeed"),
        Some(Vec::new())
    );

    let plugin_rows = session
        .execute(
            &format!(
                "SELECT entity_pk \
                 FROM lix_state \
                 WHERE schema_key = 'plugin_note' \
                   AND file_id = '{file_id}'"
            ),
            &[],
        )
        .await
        .expect("plugin state read should succeed");
    assert_eq!(plugin_rows.len(), 0);

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn sql_write_file_to_plugin_storage_installs_plugin_archive() {
    let backend = InMemoryBackend::new();
    Engine::initialize(backend.clone())
        .await
        .expect("backend should initialize");
    let engine = Engine::new(backend).await.expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");
    let archive = sentinel_plugin_archive();

    session
        .execute(
            "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
            &[
                Value::Text("/.lix_system/plugins/plugin_sentinel.lixplugin".to_string()),
                Value::Blob(archive.clone()),
            ],
        )
        .await
        .expect("plugin archive file write should install plugin");

    let plugins = session
        .list_installed_plugins()
        .await
        .expect("installed plugins should list");
    assert_eq!(plugins.len(), 1);
    assert_eq!(plugins[0].key, "plugin_sentinel");
    assert_eq!(plugins[0].schema_keys, vec!["plugin_note".to_string()]);
    assert_eq!(
        session
            .fs()
            .read_file("/.lix_system/plugins/plugin_sentinel.lixplugin")
            .await
            .expect("archive read should succeed")
            .as_deref(),
        Some(archive.as_slice())
    );

    let schemas = session
        .execute(
            "SELECT value FROM lix_registered_schema WHERE lixcol_entity_pk = lix_json('[\"plugin_note\"]')",
            &[],
        )
        .await
        .expect("plugin schema should be queryable");
    assert_eq!(schemas.len(), 1);

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn sql_write_file_to_plugin_storage_rejects_path_manifest_key_mismatch() {
    let backend = InMemoryBackend::new();
    Engine::initialize(backend.clone())
        .await
        .expect("backend should initialize");
    let engine = Engine::new(backend).await.expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    let error = session
        .execute(
            "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
            &[
                Value::Text("/.lix_system/plugins/plugin_other.lixplugin".to_string()),
                Value::Blob(sentinel_plugin_archive()),
            ],
        )
        .await
        .expect_err("mismatched plugin archive key should fail");

    assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
    assert!(error.message.contains("does not match manifest key"));

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn sql_update_path_to_plugin_storage_rejects_plugin_archive_rename() {
    let backend = InMemoryBackend::new();
    Engine::initialize(backend.clone())
        .await
        .expect("backend should initialize");
    let engine = Engine::new(backend).await.expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    session
        .execute(
            "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
            &[
                Value::Text("/normal.txt".to_string()),
                Value::Blob(b"normal".to_vec()),
            ],
        )
        .await
        .expect("normal file insert should succeed");

    let error = session
        .execute(
            "UPDATE lix_file SET path = $1, data = $2 WHERE path = $3",
            &[
                Value::Text("/.lix_system/plugins/plugin_sentinel.lixplugin".to_string()),
                Value::Blob(sentinel_plugin_archive()),
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
    let backend = InMemoryBackend::new();
    Engine::initialize(backend.clone())
        .await
        .expect("backend should initialize");
    let engine = Engine::new(backend).await.expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    session
        .install_plugin(&sentinel_plugin_archive())
        .await
        .expect("plugin install should succeed");

    let error = session
        .execute(
            "UPDATE lix_file \
             SET data = X'626164' \
             WHERE path = '/.lix_system/plugins/plugin_sentinel.lixplugin'",
            &[],
        )
        .await
        .expect_err("SQL update should reject invalid plugin archive data");

    assert!(error.message.contains("valid zip file"));

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn fs_rm_rejects_installed_plugin_storage_deletes() {
    let backend = InMemoryBackend::new();
    Engine::initialize(backend.clone())
        .await
        .expect("backend should initialize");
    let engine = Engine::new(backend).await.expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    session
        .install_plugin(&sentinel_plugin_archive())
        .await
        .expect("plugin install should succeed");

    for (path, options) in [
        (
            "/.lix_system/plugins/plugin_sentinel.lixplugin",
            FsRmOptions::default(),
        ),
        (
            "/.lix_system/plugins/",
            FsRmOptions {
                recursive: true,
                ..FsRmOptions::default()
            },
        ),
        (
            "/.lix_system/",
            FsRmOptions {
                recursive: true,
                ..FsRmOptions::default()
            },
        ),
    ] {
        let error = session
            .fs()
            .rm(path, options)
            .await
            .expect_err("fs.rm should reject plugin storage deletes");
        assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
        assert!(error.message.contains("reserved plugin storage path"));
    }

    assert_eq!(
        session
            .list_installed_plugins()
            .await
            .expect("plugin should still be installed")
            .len(),
        1
    );

    session.close().await.expect("session should close");
}

#[tokio::test]
async fn sql_delete_rejects_installed_plugin_storage_archive_tombstone() {
    let backend = InMemoryBackend::new();
    Engine::initialize(backend.clone())
        .await
        .expect("backend should initialize");
    let engine = Engine::new(backend).await.expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    session
        .install_plugin(&sentinel_plugin_archive())
        .await
        .expect("plugin install should succeed");

    let error = session
        .execute(
            "DELETE FROM lix_file \
             WHERE id = 'lix_plugin_archive::plugin_sentinel'",
            &[],
        )
        .await
        .expect_err("SQL delete should reject installed plugin archive tombstones");

    assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
    assert!(error.message.contains("reserved plugin storage path"));
    assert_eq!(
        session
            .list_installed_plugins()
            .await
            .expect("plugin should still be installed")
            .len(),
        1
    );

    session.close().await.expect("session should close");
}

simulation_test!(
    fs_mkdir_is_idempotent_and_readdir_distinguishes_missing,
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
            .fs
            .mkdir("/empty/nested/", FsMkdirOptions::default())
            .await
            .expect("mkdir should create parents");
        session
            .fs
            .mkdir("/empty/nested/", FsMkdirOptions::default())
            .await
            .expect("mkdir should be idempotent");

        assert_eq!(
            session
                .fs
                .readdir("/empty/nested/")
                .await
                .expect("readdir should succeed"),
            Some(Vec::new())
        );
        assert_eq!(
            session
                .fs
                .readdir("/missing/")
                .await
                .expect("missing readdir should succeed"),
            None
        );
    }
);

simulation_test!(fs_write_file_upserts_existing_data, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_workspace_session()
            .await
            .expect("workspace session should open"),
        &engine,
    );

    session
        .fs
        .write_file("/orders.xlsx", b"old".to_vec(), FsWriteOptions::default())
        .await
        .expect("initial write should succeed");
    session
        .fs
        .write_file("/orders.xlsx", b"new".to_vec(), FsWriteOptions::default())
        .await
        .expect("second write should upsert");

    assert_eq!(
        session
            .fs
            .read_file("/orders.xlsx")
            .await
            .expect("read should succeed"),
        Some(b"new".to_vec())
    );

    let rows = session
        .execute("SELECT id FROM lix_file WHERE path = '/orders.xlsx'", &[])
        .await
        .expect("query should succeed");
    assert_eq!(rows.len(), 1, "write_file should not duplicate descriptor");

    session
        .fs
        .write_file("/orders.xlsx", Vec::new(), FsWriteOptions::default())
        .await
        .expect("empty overwrite should succeed");
    assert_eq!(
        session
            .fs
            .read_file("/orders.xlsx")
            .await
            .expect("read should succeed"),
        Some(Vec::new())
    );

    let [Value::Text(file_id)] = rows.rows()[0].values() else {
        panic!("expected file id row, got {:?}", rows.rows()[0].values());
    };
    let blob_ref_rows = session
        .execute(
            &format!(
                "SELECT entity_pk \
                 FROM lix_state \
                 WHERE schema_key = 'lix_binary_blob_ref' \
                   AND entity_pk = lix_json('[\"{file_id}\"]')"
            ),
            &[],
        )
        .await
        .expect("blob ref query should succeed");
    assert_eq!(blob_ref_rows.len(), 0);
});

simulation_test!(fs_rm_file_and_recursive_directory, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_workspace_session()
            .await
            .expect("workspace session should open"),
        &engine,
    );

    session
        .fs
        .write_file("/tmp/a.txt", b"a".to_vec(), FsWriteOptions::default())
        .await
        .expect("write should succeed");
    session
        .fs
        .write_file(
            "/tmp/nested/b.txt",
            b"b".to_vec(),
            FsWriteOptions::default(),
        )
        .await
        .expect("nested write should succeed");

    let error = session
        .fs
        .rm("/tmp/", FsRmOptions::default())
        .await
        .expect_err("non-recursive rm should reject non-empty directory");
    assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);

    session
        .fs
        .rm(
            "/tmp/",
            FsRmOptions {
                recursive: true,
                ..FsRmOptions::default()
            },
        )
        .await
        .expect("recursive rm should remove tree");

    assert_eq!(
        session
            .fs
            .read_file("/tmp/a.txt")
            .await
            .expect("read should succeed"),
        None
    );
    assert_eq!(
        session
            .fs
            .readdir("/tmp/")
            .await
            .expect("readdir should succeed"),
        None
    );

    session
        .fs
        .rm("/tmp/missing.txt", FsRmOptions::default())
        .await
        .expect("missing rm should be a no-op");
});

simulation_test!(
    fs_rm_resolves_directory_paths_and_rejects_root,
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
            .fs
            .mkdir("/empty/", FsMkdirOptions::default())
            .await
            .expect("mkdir should succeed");
        session
            .fs
            .rm("/empty", FsRmOptions::default())
            .await
            .expect("slashless rm should remove directory");
        assert_eq!(
            session
                .fs
                .readdir("/empty/")
                .await
                .expect("readdir should succeed"),
            None
        );

        session
            .fs
            .write_file("/file.txt", b"file".to_vec(), FsWriteOptions::default())
            .await
            .expect("write should succeed");
        session
            .fs
            .rm("/file.txt/", FsRmOptions::default())
            .await
            .expect_err("directory-form rm on file should fail");
        assert_eq!(
            session
                .fs
                .read_file("/file.txt")
                .await
                .expect("read should succeed"),
            Some(b"file".to_vec())
        );

        session
            .fs
            .rm("/", FsRmOptions::default())
            .await
            .expect_err("rm should reject virtual root");
    }
);

simulation_test!(fs_wrong_kind_errors, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_workspace_session()
            .await
            .expect("workspace session should open"),
        &engine,
    );

    session
        .fs
        .mkdir("/docs/", FsMkdirOptions::default())
        .await
        .expect("mkdir should succeed");
    session
        .fs
        .write_file("/file.txt", b"file".to_vec(), FsWriteOptions::default())
        .await
        .expect("write should succeed");

    session
        .fs
        .read_file("/docs")
        .await
        .expect_err("read_file on directory should fail");
    session
        .fs
        .readdir("/file.txt/")
        .await
        .expect_err("readdir on file should fail");
    session
        .fs
        .write_file("/docs", b"nope".to_vec(), FsWriteOptions::default())
        .await
        .expect_err("write_file over directory should fail");
    session
        .fs
        .mkdir("/file.txt/", FsMkdirOptions::default())
        .await
        .expect_err("mkdir over file should fail");
});

simulation_test!(
    fs_untracked_write_is_visible_and_rejects_tracked_collision,
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
            .fs
            .write_file(
                "/scratch/preview.json",
                b"preview".to_vec(),
                FsWriteOptions {
                    untracked: true,
                    ..FsWriteOptions::default()
                },
            )
            .await
            .expect("untracked write should succeed");
        assert_eq!(
            session
                .fs
                .read_file("/scratch/preview.json")
                .await
                .expect("read should see untracked file"),
            Some(b"preview".to_vec())
        );

        session
            .fs
            .write_file(
                "/tracked.txt",
                b"tracked".to_vec(),
                FsWriteOptions::default(),
            )
            .await
            .expect("tracked write should succeed");
        session
            .fs
            .write_file(
                "/tracked.txt",
                b"untracked".to_vec(),
                FsWriteOptions {
                    untracked: true,
                    ..FsWriteOptions::default()
                },
            )
            .await
            .expect_err("untracked write should not shadow tracked path");
    }
);

simulation_test!(
    fs_rejects_cross_lane_ancestor_collisions,
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
            .fs
            .mkdir("/tracked/", FsMkdirOptions::default())
            .await
            .expect("tracked mkdir should succeed");
        session
            .fs
            .write_file(
                "/tracked/untracked.txt",
                b"nope".to_vec(),
                FsWriteOptions {
                    untracked: true,
                    ..FsWriteOptions::default()
                },
            )
            .await
            .expect_err("untracked write should not create duplicate tracked ancestor");
        session
            .fs
            .mkdir(
                "/tracked/untracked-dir/",
                FsMkdirOptions {
                    untracked: true,
                    ..FsMkdirOptions::default()
                },
            )
            .await
            .expect_err("untracked mkdir should not create duplicate tracked ancestor");

        session
            .fs
            .mkdir(
                "/scratch/",
                FsMkdirOptions {
                    untracked: true,
                    ..FsMkdirOptions::default()
                },
            )
            .await
            .expect("untracked mkdir should succeed");
        session
            .fs
            .write_file(
                "/scratch/tracked.txt",
                b"nope".to_vec(),
                FsWriteOptions::default(),
            )
            .await
            .expect_err("tracked write should not create duplicate untracked ancestor");
        session
            .fs
            .mkdir("/scratch/tracked-dir/", FsMkdirOptions::default())
            .await
            .expect_err("tracked mkdir should not create duplicate untracked ancestor");

        session
            .fs
            .mkdir("/tracked-dir/", FsMkdirOptions::default())
            .await
            .expect("tracked directory should succeed");
        session
            .fs
            .write_file(
                "/tracked-dir",
                b"nope".to_vec(),
                FsWriteOptions {
                    untracked: true,
                    ..FsWriteOptions::default()
                },
            )
            .await
            .expect_err("untracked file should not occupy tracked directory namespace");
        session
            .fs
            .write_file(
                "/tracked-file",
                b"tracked".to_vec(),
                FsWriteOptions::default(),
            )
            .await
            .expect("tracked file should succeed");
        session
            .fs
            .mkdir(
                "/tracked-file/",
                FsMkdirOptions {
                    untracked: true,
                    ..FsMkdirOptions::default()
                },
            )
            .await
            .expect_err("untracked directory should not occupy tracked file namespace");
        session
            .fs
            .write_file(
                "/tracked-file/child.txt",
                b"nope".to_vec(),
                FsWriteOptions {
                    untracked: true,
                    ..FsWriteOptions::default()
                },
            )
            .await
            .expect_err("untracked write should not create directory under tracked file");

        session
            .fs
            .mkdir(
                "/untracked-dir/",
                FsMkdirOptions {
                    untracked: true,
                    ..FsMkdirOptions::default()
                },
            )
            .await
            .expect("untracked directory should succeed");
        session
            .fs
            .write_file(
                "/untracked-dir",
                b"nope".to_vec(),
                FsWriteOptions::default(),
            )
            .await
            .expect_err("tracked file should not occupy untracked directory namespace");
        session
            .fs
            .write_file(
                "/untracked-file",
                b"untracked".to_vec(),
                FsWriteOptions {
                    untracked: true,
                    ..FsWriteOptions::default()
                },
            )
            .await
            .expect("untracked file should succeed");
        session
            .fs
            .mkdir("/untracked-file/", FsMkdirOptions::default())
            .await
            .expect_err("tracked directory should not occupy untracked file namespace");
        session
            .fs
            .mkdir("/untracked-file/child/", FsMkdirOptions::default())
            .await
            .expect_err("tracked mkdir should not create directory under untracked file");
    }
);

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

fn binary_sentinel_plugin_archive() -> Vec<u8> {
    const MANIFEST_JSON: &[u8] = br#"{
        "key": "plugin_binary_sentinel",
        "runtime": "wasm-component-v1",
        "api_version": "0.1.0",
        "match": { "path_glob": "*.binary-sentinel", "content_type": "binary" },
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
    const WASM_HEADER: &[u8] = b"\0asm\x01\0\0\0";

    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        ("manifest.json", manifest_json),
        ("schema/plugin_note.json", SCHEMA_JSON),
        ("plugin.wasm", WASM_HEADER),
    ] {
        writer.start_file(path, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}
