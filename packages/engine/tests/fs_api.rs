#[macro_use]
mod support;

use async_trait::async_trait;
use lix_engine::{Engine, ExecuteResult, LixError, Memory, SessionContext, Value};

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

simulation_test!(
    sql_path_only_file_reads_as_empty_without_blob_ref,
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
            read_file(&session, "/empty.txt")
                .await
                .expect("file read should succeed"),
            Some(Vec::new())
        );

        let file_result = session
            .execute("SELECT id FROM lix_file WHERE path = '/empty.txt'", &[])
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
async fn sql_update_path_to_plugin_storage_rejects_archive_rename() {
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
                Value::Text("/.lix/plugins/untrusted.lixplugin".to_string()),
                Value::Blob(b"not-a-plugin".to_vec().into()),
                Value::Text("/normal.txt".to_string()),
            ],
        )
        .await
        .expect_err("path update into plugin storage should fail before archive parsing");

    assert!(error.message.contains("plugin archive paths"), "{error:?}");

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
