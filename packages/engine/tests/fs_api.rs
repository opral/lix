#[macro_use]
#[path = "support/mod.rs"]
mod support;

use lix_engine::{FsDirEntryKind, FsMkdirOptions, FsRmOptions, FsWriteOptions, LixError};

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
        let [lix_engine::Value::Text(file_id)] = file_result.rows()[0].values() else {
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

    let [lix_engine::Value::Text(file_id)] = rows.rows()[0].values() else {
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
