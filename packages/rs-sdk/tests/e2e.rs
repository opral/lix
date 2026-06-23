use lix_sdk::{
    Backend, CreateBranchOptions, InMemoryBackend, Lix, LixError, MergeBranchOptions,
    MergeBranchOutcome, OpenLixOptions, SwitchBranchOptions, Value, open_lix,
};
#[cfg(feature = "fs_backend")]
use lix_sdk::{FsBackend, open_lix_with_backend};
#[cfg(feature = "fs_backend")]
use std::path::Path;

#[tokio::test]
async fn rs_sdk_open_register_write_query_branch_and_merge_flow() {
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    let main_branch_id = lix.active_branch_id().await.unwrap();

    register_crm_task_schema(&lix).await;

    lix.execute(
        "INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
        &[
            Value::Text("task-1".to_string()),
            Value::Text("Draft RS SDK flow".to_string()),
            Value::Boolean(false),
            Value::Text(r#"{"priority":"high","tags":["sdk","json"]}"#.to_string()),
        ],
    )
    .await
    .unwrap();

    let projected = lix
        .execute(
            "SELECT title, done, meta, lixcol_snapshot_content FROM crm_task WHERE id = $1",
            &[Value::Text("task-1".to_string())],
        )
        .await
        .unwrap();
    assert_crm_task_projection(&projected);

    assert!(!task_done(&lix, "task-1").await);

    let draft = lix
        .create_branch(CreateBranchOptions {
            id: Some("draft-branch".to_string()),
            name: "Draft".to_string(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    assert_eq!(draft.id, "draft-branch");
    assert_eq!(draft.name, "Draft");
    assert!(!draft.hidden);

    lix.switch_branch(SwitchBranchOptions {
        branch_id: draft.id.clone(),
    })
    .await
    .unwrap();

    lix.execute(
        "UPDATE crm_task SET done = $1 WHERE id = $2",
        &[Value::Boolean(true), Value::Text("task-1".to_string())],
    )
    .await
    .unwrap();

    assert!(task_done(&lix, "task-1").await);

    lix.switch_branch(SwitchBranchOptions {
        branch_id: main_branch_id.clone(),
    })
    .await
    .unwrap();

    assert!(!task_done(&lix, "task-1").await);

    let merge = lix
        .merge_branch(MergeBranchOptions {
            source_branch_id: draft.id,
        })
        .await
        .unwrap();

    assert_eq!(merge.outcome, MergeBranchOutcome::FastForward);
    assert_eq!(merge.target_branch_id, main_branch_id);
    assert_eq!(merge.change_stats.total, 1);
    assert_eq!(merge.change_stats.modified, 1);
    assert_eq!(merge.created_merge_commit_id, None);
    assert!(task_done(&lix, "task-1").await);

    lix.close().await.unwrap();
}

#[tokio::test]
async fn rs_sdk_close_is_idempotent_and_rejects_later_operations() {
    let lix = open_lix(OpenLixOptions {
        backend: InMemoryBackend::new(),
        ..Default::default()
    })
    .await
    .unwrap();

    lix.close().await.unwrap();
    lix.close().await.unwrap();

    let error = lix
        .execute("SELECT value FROM lix_key_value WHERE key = 'lix_id'", &[])
        .await
        .expect_err("execute after close should fail");
    assert_closed(error);

    let error = lix
        .active_branch_id()
        .await
        .expect_err("active_branch_id after close should fail");
    assert_closed(error);
}

#[tokio::test]
async fn rs_sdk_close_does_not_destroy_committed_data() {
    let backend = InMemoryBackend::new();
    let first = open_lix(OpenLixOptions {
        backend: backend.clone(),
        ..Default::default()
    })
    .await
    .unwrap();

    first
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('close-key', 'close-value')",
            &[],
        )
        .await
        .unwrap();
    first.close().await.unwrap();

    let error = first
        .execute(
            "SELECT value FROM lix_key_value WHERE key = 'close-key'",
            &[],
        )
        .await
        .expect_err("closed handle should not be usable");
    assert_closed(error);

    let second = open_lix(OpenLixOptions {
        backend,
        ..Default::default()
    })
    .await
    .unwrap();
    let result = second
        .execute(
            "SELECT key FROM lix_key_value WHERE key = 'close-key' AND value = lix_json('\"close-value\"')",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Text("close-key".to_string())]
    );
    second.close().await.unwrap();
}

#[tokio::test]
async fn failed_write_validation_does_not_poison_backend_transaction() {
    let lix = open_lix(OpenLixOptions {
        backend: InMemoryBackend::new(),
        ..Default::default()
    })
    .await
    .unwrap();

    register_poison_task_schema(&lix).await;

    let error = lix
        .execute(
            "INSERT INTO poison_task (id, title) VALUES ($1, $2)",
            &[
                Value::Text("bad-task".to_string()),
                Value::Text("missing meta".to_string()),
            ],
        )
        .await
        .expect_err("schema validation should reject missing required field");
    assert_eq!(error.code, "LIX_ERROR_SCHEMA_VALIDATION");

    let result = lix.execute("SELECT 1 AS ok", &[]).await.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].values(), &[Value::Integer(1)]);

    lix.execute(
        "INSERT INTO poison_task (id, title, meta) VALUES ($1, $2, lix_json($3))",
        &[
            Value::Text("good-task".to_string()),
            Value::Text("valid".to_string()),
            Value::Text(r#"{"priority":"high"}"#.to_string()),
        ],
    )
    .await
    .expect("valid write after failed write should succeed");

    lix.close().await.unwrap();
}

#[tokio::test]
async fn transaction_commits_multiple_statements_together() {
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    register_crm_task_schema(&lix).await;

    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
        &[
            Value::Text("tx-task-1".to_string()),
            Value::Text("First".to_string()),
            Value::Boolean(false),
            Value::Text(r#"{"batch":1}"#.to_string()),
        ],
    )
    .await
    .unwrap();
    tx.execute(
        "INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
        &[
            Value::Text("tx-task-2".to_string()),
            Value::Text("Second".to_string()),
            Value::Boolean(true),
            Value::Text(r#"{"batch":1}"#.to_string()),
        ],
    )
    .await
    .unwrap();

    let staged = tx
        .execute(
            "SELECT id FROM crm_task WHERE id IN ($1, $2) ORDER BY id",
            &[
                Value::Text("tx-task-1".to_string()),
                Value::Text("tx-task-2".to_string()),
            ],
        )
        .await
        .unwrap();
    assert_eq!(staged.len(), 2);

    tx.commit().await.unwrap();

    let committed = lix
        .execute(
            "SELECT id FROM crm_task WHERE id IN ($1, $2) ORDER BY id",
            &[
                Value::Text("tx-task-1".to_string()),
                Value::Text("tx-task-2".to_string()),
            ],
        )
        .await
        .unwrap();
    assert_eq!(committed.len(), 2);
    lix.close().await.unwrap();
}

#[tokio::test]
async fn transaction_lix_file_data_reads_staged_file_bytes() {
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    let mut tx = lix.begin_transaction().await.unwrap();
    let path = "/tx-file-data.bin".to_string();
    let original = b"staged bytes before commit".to_vec();

    tx.execute(
        "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
        &[Value::Text(path.clone()), Value::Blob(original.clone())],
    )
    .await
    .unwrap();

    let selected = tx
        .execute(
            "SELECT data FROM lix_file WHERE path = $1 AND data = $2",
            &[Value::Text(path.clone()), Value::Blob(original.clone())],
        )
        .await
        .unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(
        selected.rows()[0].values(),
        &[Value::Blob(original.clone())]
    );

    let updated = b"updated bytes before commit".to_vec();
    let update = tx
        .execute(
            "UPDATE lix_file SET data = $1 WHERE path = $2",
            &[Value::Blob(updated.clone()), Value::Text(path.clone())],
        )
        .await
        .unwrap();
    assert_eq!(update.rows_affected(), 1);

    let after_update = tx
        .execute(
            "SELECT data FROM lix_file WHERE path = $1 AND data = $2",
            &[Value::Text(path.clone()), Value::Blob(updated.clone())],
        )
        .await
        .unwrap();
    assert_eq!(after_update.len(), 1);
    assert_eq!(
        after_update.rows()[0].values(),
        &[Value::Blob(updated.clone())]
    );

    let delete = tx
        .execute(
            "DELETE FROM lix_file WHERE path = $1",
            &[Value::Text(path.clone())],
        )
        .await
        .unwrap();
    assert_eq!(delete.rows_affected(), 1);

    let after_delete = tx
        .execute(
            "SELECT data FROM lix_file WHERE path = $1",
            &[Value::Text(path)],
        )
        .await
        .unwrap();
    assert_eq!(after_delete.len(), 0);

    tx.rollback().await.unwrap();
    lix.close().await.unwrap();
}

#[tokio::test]
async fn transaction_rollback_discards_staged_writes() {
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    register_crm_task_schema(&lix).await;

    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
        &[
            Value::Text("rolled-back-task".to_string()),
            Value::Text("Rollback".to_string()),
            Value::Boolean(false),
            Value::Text(r#"{"batch":1}"#.to_string()),
        ],
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();

    let result = lix
        .execute(
            "SELECT id FROM crm_task WHERE id = $1",
            &[Value::Text("rolled-back-task".to_string())],
        )
        .await
        .unwrap();
    assert_eq!(result.len(), 0);
    lix.close().await.unwrap();
}

#[tokio::test]
async fn transaction_blocks_session_execute_on_same_handle() {
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    register_crm_task_schema(&lix).await;

    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
        &[
            Value::Text("tx-only-task".to_string()),
            Value::Text("Inside tx".to_string()),
            Value::Boolean(false),
            Value::Text(r#"{"batch":1}"#.to_string()),
        ],
    )
    .await
    .unwrap();

    let error = lix
        .execute(
            "INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
            &[
                Value::Text("outside-task".to_string()),
                Value::Text("Outside tx".to_string()),
                Value::Boolean(false),
                Value::Text(r#"{"batch":1}"#.to_string()),
            ],
        )
        .await
        .expect_err("session writes should be blocked while explicit transaction is active");
    assert_eq!(error.code, "LIX_INVALID_TRANSACTION_STATE");

    let error = lix
        .execute("SELECT 1 AS ok", &[])
        .await
        .expect_err("session reads should be blocked while explicit transaction is active");
    assert_eq!(error.code, "LIX_INVALID_TRANSACTION_STATE");

    let tx_read = tx
        .execute("SELECT 1 AS ok", &[])
        .await
        .expect("transaction reads should remain available");
    assert_eq!(tx_read.rows()[0].get::<i64>("ok").unwrap(), 1);

    tx.commit().await.unwrap();

    let committed = lix
        .execute(
            "SELECT id FROM crm_task WHERE id IN ($1, $2) ORDER BY id",
            &[
                Value::Text("outside-task".to_string()),
                Value::Text("tx-only-task".to_string()),
            ],
        )
        .await
        .unwrap();
    assert_eq!(committed.len(), 1);
    assert_eq!(
        committed.rows()[0].values(),
        &[Value::Text("tx-only-task".to_string())]
    );
    lix.close().await.unwrap();
}

#[tokio::test]
#[cfg(feature = "fs_backend")]
async fn filesystem_mounted_reads_use_local_files_as_source_of_truth() {
    let tempdir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(tempdir.path().join("docs")).unwrap();
    std::fs::create_dir_all(tempdir.path().join("empty")).unwrap();
    std::fs::write(tempdir.path().join("docs/readme.txt"), b"local").unwrap();

    let lix = open_lix_with_filesystem(tempdir.path()).await;

    assert_eq!(
        read_file(&lix, "/docs/readme.txt")
            .await
            .unwrap()
            .as_deref(),
        Some(b"local".as_slice())
    );
    assert!(readdir(&lix, "/empty/").await.unwrap().unwrap().is_empty());
    assert_eq!(
        std::fs::read(tempdir.path().join("docs/readme.txt")).unwrap(),
        b"local"
    );
    lix.close().await.unwrap();
}

#[tokio::test]
#[cfg(feature = "fs_backend")]
async fn filesystem_initialization_wipes_legacy_sqlite_internal_metadata() {
    let tempdir = tempfile::tempdir().unwrap();
    let internal_dir = tempdir.path().join(".lix/.internal");
    std::fs::create_dir_all(&internal_dir).unwrap();
    std::fs::write(internal_dir.join("db.sqlite"), b"legacy sqlite metadata").unwrap();
    std::fs::write(internal_dir.join("old-cache"), b"legacy internal data").unwrap();
    std::fs::write(tempdir.path().join("note.txt"), b"workspace").unwrap();

    let lix = open_lix_with_filesystem(tempdir.path()).await;

    assert!(!internal_dir.join("db.sqlite").exists());
    assert!(!internal_dir.join("old-cache").exists());
    assert!(internal_dir.join("rocksdb").is_dir());
    assert_eq!(
        read_file(&lix, "/.lix/.internal/db.sqlite")
            .await
            .unwrap()
            .as_deref(),
        None
    );
    assert_eq!(
        read_file(&lix, "/note.txt").await.unwrap().as_deref(),
        Some(b"workspace".as_slice())
    );
    lix.close().await.unwrap();
}

#[tokio::test]
#[cfg(feature = "fs_backend")]
async fn filesystem_initialization_wipes_legacy_root_sqlite_and_system_metadata() {
    let tempdir = tempfile::tempdir().unwrap();
    let lix_dir = tempdir.path().join(".lix");
    let system_dir = tempdir.path().join(".lix_system");
    std::fs::create_dir_all(&lix_dir).unwrap();
    std::fs::create_dir_all(&system_dir).unwrap();
    std::fs::write(lix_dir.join("db.sqlite"), b"legacy sqlite metadata").unwrap();
    std::fs::write(lix_dir.join("db.sqlite-wal"), b"legacy sqlite wal").unwrap();
    std::fs::write(system_dir.join("cache"), b"legacy system data").unwrap();
    std::fs::write(tempdir.path().join("note.txt"), b"workspace").unwrap();

    let lix = open_lix_with_filesystem(tempdir.path()).await;

    assert!(!lix_dir.join("db.sqlite").exists());
    assert!(!lix_dir.join("db.sqlite-wal").exists());
    assert!(!system_dir.exists());
    assert_eq!(
        read_file(&lix, "/.lix/db.sqlite").await.unwrap().as_deref(),
        None
    );
    assert_eq!(
        read_file(&lix, "/.lix/db.sqlite-wal")
            .await
            .unwrap()
            .as_deref(),
        None
    );
    assert_eq!(
        read_file(&lix, "/.lix_system/cache")
            .await
            .unwrap()
            .as_deref(),
        None
    );
    assert_eq!(
        read_file(&lix, "/note.txt").await.unwrap().as_deref(),
        Some(b"workspace".as_slice())
    );
    lix.close().await.unwrap();
}

#[cfg(feature = "fs_backend")]
async fn open_lix_with_filesystem(path: &Path) -> Lix<FsBackend> {
    let backend = FsBackend::open(path).await.unwrap();
    open_lix_with_backend(backend).await.unwrap()
}

#[tokio::test]
#[cfg(feature = "fs_backend")]
async fn rocksdb_filesystem_backend_allows_same_process_multi_open() {
    let tempdir = tempfile::tempdir().unwrap();
    let backend_a = FsBackend::open(tempdir.path())
        .await
        .expect("first rocksdb fs backend opens");
    let backend_b = FsBackend::open(tempdir.path())
        .await
        .expect("second rocksdb fs backend reuses process-local DB");
    let lix_a = open_lix_with_backend(backend_a)
        .await
        .expect("first lix opens");
    let lix_b = open_lix_with_backend(backend_b)
        .await
        .expect("second lix opens");

    write_file(&lix_a, "/from-a.txt", b"a".to_vec())
        .await
        .expect("first lix writes");
    assert_eq!(
        read_file(&lix_b, "/from-a.txt").await.unwrap().as_deref(),
        Some(b"a".as_slice())
    );

    write_file(&lix_b, "/from-b.txt", b"b".to_vec())
        .await
        .expect("second lix writes");
    assert_eq!(
        read_file(&lix_a, "/from-b.txt").await.unwrap().as_deref(),
        Some(b"b".as_slice())
    );

    lix_a.close().await.unwrap();
    lix_b.close().await.unwrap();
}

async fn read_file<B>(lix: &Lix<B>, path: &str) -> Result<Option<Vec<u8>>, LixError>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    let result = lix
        .execute(
            "SELECT data FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await?;
    result
        .rows()
        .first()
        .map(|row| row.get::<Vec<u8>>("data"))
        .transpose()
}

async fn write_file<B>(lix: &Lix<B>, path: &str, data: Vec<u8>) -> Result<(), LixError>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    lix.execute(
        "INSERT INTO lix_file (path, data) VALUES ($1, $2) \
         ON CONFLICT (path) DO UPDATE SET data = excluded.data",
        &[Value::Text(path.to_string()), Value::Blob(data)],
    )
    .await?;
    Ok(())
}

async fn readdir<B>(lix: &Lix<B>, path: &str) -> Result<Option<Vec<String>>, LixError>
where
    B: Backend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    if path == "/" {
        let entries = lix
            .execute(
                "SELECT name FROM lix_directory WHERE parent_id IS NULL \
                 UNION ALL \
                 SELECT name FROM lix_file WHERE directory_id IS NULL \
                 ORDER BY name",
                &[],
            )
            .await?;
        return Ok(Some(
            entries
                .rows()
                .iter()
                .map(|row| row.get::<String>("name"))
                .collect::<Result<Vec<_>, _>>()?,
        ));
    }

    let directory = lix
        .execute(
            "SELECT id FROM lix_directory WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await?;
    let Some(directory) = directory.rows().first() else {
        return Ok(None);
    };
    let directory_id = directory.get::<String>("id")?;
    let entries = lix
        .execute(
            "SELECT name FROM lix_directory WHERE parent_id = $1 \
             UNION ALL \
             SELECT name FROM lix_file WHERE directory_id = $1 \
             ORDER BY name",
            &[Value::Text(directory_id)],
        )
        .await?;
    Ok(Some(
        entries
            .rows()
            .iter()
            .map(|row| row.get::<String>("name"))
            .collect::<Result<Vec<_>, _>>()?,
    ))
}

#[tokio::test]
#[cfg(feature = "fs_backend")]
async fn filesystem_creates_gitignore_files_for_lix_metadata() {
    let tempdir = tempfile::tempdir().unwrap();

    let lix = open_lix_with_filesystem(tempdir.path()).await;

    assert_eq!(
        std::fs::read(tempdir.path().join(".lix/.gitignore")).unwrap(),
        b"*\n"
    );
    assert!(!tempdir.path().join(".lix/.internal/.gitignore").exists());
    assert_eq!(read_file(&lix, "/.lix/.gitignore").await.unwrap(), None);
    assert_eq!(
        read_file(&lix, "/.lix/.internal/.gitignore").await.unwrap(),
        None
    );
    lix.close().await.unwrap();
}

#[tokio::test]
#[cfg(feature = "fs_backend")]
async fn filesystem_removes_legacy_lix_system_directory() {
    let tempdir = tempfile::tempdir().unwrap();

    std::fs::create_dir_all(tempdir.path().join(".lix_system/app_data")).unwrap();
    std::fs::write(
        tempdir.path().join(".lix_system/app_data/legacy.txt"),
        b"disk legacy",
    )
    .unwrap();
    std::fs::write(tempdir.path().join(".lix_system/.gitignore"), b"*\n").unwrap();
    std::fs::write(tempdir.path().join(".lix_system/.DS_Store"), b"ds-store").unwrap();
    std::fs::write(
        tempdir.path().join(".lix_system/db.sqlite"),
        b"disk legacy db",
    )
    .unwrap();
    std::fs::create_dir_all(tempdir.path().join(".lix_system/.internal")).unwrap();
    std::fs::write(
        tempdir.path().join(".lix_system/.internal/private"),
        b"disk private",
    )
    .unwrap();

    let lix = open_lix_with_filesystem(tempdir.path()).await;

    assert!(!tempdir.path().join(".lix_system").exists());
    assert!(!tempdir.path().join(".lix/app_data/legacy.txt").exists());
    assert!(!tempdir.path().join(".lix/.DS_Store").exists());
    assert_eq!(readdir(&lix, "/.lix_system/").await.unwrap(), None);
    assert_eq!(
        read_file(&lix, "/.lix_system/settings.json").await.unwrap(),
        None
    );
    assert_eq!(
        read_file(&lix, "/.lix_system/db.sqlite").await.unwrap(),
        None
    );
    assert_eq!(read_file(&lix, "/.lix/db.sqlite").await.unwrap(), None);
    assert!(!tempdir.path().join(".lix/db.sqlite").exists());
    assert_eq!(
        read_file(&lix, "/.lix_system/.internal/private")
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        read_file(&lix, "/.lix/.internal/private").await.unwrap(),
        None
    );
    assert!(!tempdir.path().join(".lix/.internal/private").exists());
    assert_eq!(
        read_file(&lix, "/.lix/app_data/legacy.txt").await.unwrap(),
        None
    );
    lix.close().await.unwrap();
}

#[tokio::test]
#[cfg(feature = "fs_backend")]
async fn filesystem_mounted_reads_ignore_git_entries() {
    let tempdir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(tempdir.path().join(".git/objects")).unwrap();
    std::fs::write(tempdir.path().join(".git/config"), b"git").unwrap();
    std::fs::create_dir_all(tempdir.path().join("nested/.git")).unwrap();
    std::fs::write(tempdir.path().join("nested/.git/config"), b"nested").unwrap();
    std::fs::create_dir_all(tempdir.path().join("docs")).unwrap();
    std::fs::write(tempdir.path().join("docs/.git"), b"git-file").unwrap();
    std::fs::write(tempdir.path().join("docs/readme.txt"), b"normal").unwrap();

    let lix = open_lix_with_filesystem(tempdir.path()).await;

    assert_eq!(
        read_file(&lix, "/docs/readme.txt")
            .await
            .unwrap()
            .as_deref(),
        Some(b"normal".as_slice())
    );
    assert_eq!(readdir(&lix, "/.git/").await.unwrap(), None);
    assert_eq!(read_file(&lix, "/.git/config").await.unwrap(), None);
    assert_eq!(readdir(&lix, "/nested/.git/").await.unwrap(), None);
    assert_eq!(read_file(&lix, "/nested/.git/config").await.unwrap(), None);
    assert_eq!(read_file(&lix, "/docs/.git").await.unwrap(), None);
    assert!(tempdir.path().join(".git/config").is_file());
    assert!(tempdir.path().join("nested/.git/config").is_file());
    assert!(tempdir.path().join("docs/.git").is_file());
    lix.close().await.unwrap();
}

#[tokio::test]
#[cfg(feature = "fs_backend")]
async fn filesystem_lix_writes_materialize_workspace_disk_on_commit() {
    let tempdir = tempfile::tempdir().unwrap();
    let lix = open_lix_with_filesystem(tempdir.path()).await;

    std::fs::write(tempdir.path().join("sdk.txt"), b"disk").unwrap();
    write_file(&lix, "/sdk.txt", b"sdk".to_vec()).await.unwrap();
    assert_eq!(
        read_file(&lix, "/sdk.txt").await.unwrap().as_deref(),
        Some(b"sdk".as_slice())
    );
    assert_eq!(
        std::fs::read(tempdir.path().join("sdk.txt")).unwrap(),
        b"sdk"
    );

    lix.execute(
        "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
        &[
            Value::Text("/sql.txt".to_string()),
            Value::Blob(b"sql".to_vec()),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        read_file(&lix, "/sql.txt").await.unwrap().as_deref(),
        Some(b"sql".as_slice())
    );
    assert_eq!(
        std::fs::read(tempdir.path().join("sql.txt")).unwrap(),
        b"sql"
    );

    lix.execute(
        "UPDATE lix_file SET data = $1 WHERE path = $2",
        &[
            Value::Blob(b"updated".to_vec()),
            Value::Text("/sql.txt".to_string()),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        read_file(&lix, "/sql.txt").await.unwrap().as_deref(),
        Some(b"updated".as_slice())
    );
    assert_eq!(
        std::fs::read(tempdir.path().join("sql.txt")).unwrap(),
        b"updated"
    );

    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
        &[
            Value::Text("/tx.txt".to_string()),
            Value::Blob(b"tx".to_vec()),
        ],
    )
    .await
    .unwrap();
    assert!(!tempdir.path().join("tx.txt").exists());
    tx.commit().await.unwrap();
    assert_eq!(
        read_file(&lix, "/tx.txt").await.unwrap().as_deref(),
        Some(b"tx".as_slice())
    );
    assert_eq!(std::fs::read(tempdir.path().join("tx.txt")).unwrap(), b"tx");

    lix.execute(
        "DELETE FROM lix_file WHERE path = $1",
        &[Value::Text("/sql.txt".to_string())],
    )
    .await
    .unwrap();
    assert_eq!(read_file(&lix, "/sql.txt").await.unwrap(), None);
    assert!(!tempdir.path().join("sql.txt").exists());
    lix.close().await.unwrap();
}

#[tokio::test]
#[cfg(feature = "fs_backend")]
async fn filesystem_untracked_lix_writes_do_not_materialize() {
    let tempdir = tempfile::tempdir().unwrap();
    let lix = open_lix_with_filesystem(tempdir.path()).await;

    lix.execute(
        "INSERT INTO lix_file (id, path, data, lixcol_untracked) VALUES ($1, $2, $3, true)",
        &[
            Value::Text("file-untracked".to_string()),
            Value::Text("/untracked.txt".to_string()),
            Value::Blob(b"untracked".to_vec()),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        read_file(&lix, "/untracked.txt").await.unwrap().as_deref(),
        Some(b"untracked".as_slice())
    );
    assert!(!tempdir.path().join("untracked.txt").exists());

    lix.execute(
        "INSERT INTO lix_directory (id, path, lixcol_untracked) VALUES ($1, $2, true)",
        &[
            Value::Text("dir-untracked".to_string()),
            Value::Text("/untracked-dir/".to_string()),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        readdir(&lix, "/untracked-dir/").await.unwrap(),
        Some(vec![])
    );
    assert!(!tempdir.path().join("untracked-dir").exists());

    lix.close().await.unwrap();
}

#[tokio::test]
#[cfg(feature = "fs_backend")]
async fn filesystem_lix_writes_to_git_paths_do_not_materialize() {
    let tempdir = tempfile::tempdir().unwrap();
    let lix = open_lix_with_filesystem(tempdir.path()).await;

    write_file(&lix, "/.git/config", b"lix".to_vec())
        .await
        .unwrap();
    write_file(&lix, "/docs/.git", b"lix".to_vec())
        .await
        .unwrap();

    assert_eq!(
        read_file(&lix, "/.git/config").await.unwrap().as_deref(),
        Some(b"lix".as_slice())
    );
    assert_eq!(
        read_file(&lix, "/docs/.git").await.unwrap().as_deref(),
        Some(b"lix".as_slice())
    );
    assert!(!tempdir.path().join(".git/config").exists());
    assert!(!tempdir.path().join("docs/.git").exists());
    lix.close().await.unwrap();
}

#[tokio::test]
#[cfg(feature = "fs_backend")]
async fn filesystem_imports_opaque_lix_path_names() {
    let tempdir = tempfile::tempdir().unwrap();
    std::fs::write(tempdir.path().join("bad%name.txt"), b"bad").unwrap();
    std::fs::write(tempdir.path().join("#hash.txt"), b"hash").unwrap();

    let lix = open_lix_with_filesystem(tempdir.path()).await;

    assert_eq!(
        read_file(&lix, "/bad%name.txt").await.unwrap().as_deref(),
        Some(b"bad".as_slice())
    );
    assert_eq!(
        read_file(&lix, "/#hash.txt").await.unwrap().as_deref(),
        Some(b"hash".as_slice())
    );
    write_file(&lix, "/written%23.txt", b"written".to_vec())
        .await
        .unwrap();
    assert_eq!(
        read_file(&lix, "/written%23.txt").await.unwrap().as_deref(),
        Some(b"written".as_slice())
    );
    assert_eq!(
        std::fs::read(tempdir.path().join("written%23.txt")).unwrap(),
        b"written"
    );
    lix.close().await.unwrap();
}

#[tokio::test]
#[cfg(all(unix, feature = "fs_backend"))]
async fn filesystem_rejects_symlink_root() {
    use std::os::unix::fs::symlink;

    let tempdir = tempfile::tempdir().unwrap();
    std::fs::create_dir(tempdir.path().join("real-root")).unwrap();
    symlink("real-root", tempdir.path().join("linked-root")).unwrap();

    let Err(error) = FsBackend::open(tempdir.path().join("linked-root")).await else {
        panic!("symlink root should fail");
    };

    assert_eq!(error.code, "LIX_FILESYSTEM_ERROR");
}

#[tokio::test]
#[cfg(all(unix, feature = "fs_backend"))]
async fn filesystem_mounted_reads_ignore_symlinks() {
    use std::os::unix::fs::symlink;

    let tempdir = tempfile::tempdir().unwrap();
    std::fs::write(tempdir.path().join("target.txt"), b"target").unwrap();
    std::fs::create_dir(tempdir.path().join("real-dir")).unwrap();
    std::fs::write(tempdir.path().join("real-dir/file.txt"), b"nested").unwrap();
    symlink("target.txt", tempdir.path().join("link.txt")).unwrap();
    symlink("real-dir", tempdir.path().join("linked-dir")).unwrap();

    let lix = open_lix_with_filesystem(tempdir.path()).await;

    assert_eq!(
        read_file(&lix, "/target.txt").await.unwrap().as_deref(),
        Some(b"target".as_slice())
    );
    assert_eq!(
        read_file(&lix, "/real-dir/file.txt")
            .await
            .unwrap()
            .as_deref(),
        Some(b"nested".as_slice())
    );
    assert_eq!(read_file(&lix, "/link.txt").await.unwrap(), None);
    assert_eq!(readdir(&lix, "/linked-dir/").await.unwrap(), None);
    assert_eq!(read_file(&lix, "/linked-dir/file.txt").await.unwrap(), None);
    assert!(
        std::fs::symlink_metadata(tempdir.path().join("link.txt"))
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert!(
        std::fs::symlink_metadata(tempdir.path().join("linked-dir"))
            .unwrap()
            .file_type()
            .is_symlink()
    );
    lix.close().await.unwrap();
}

#[tokio::test]
#[cfg(all(unix, feature = "fs_backend"))]
async fn filesystem_ignores_special_and_invalid_utf8_entries() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;
    use std::os::unix::fs::FileTypeExt;
    use std::os::unix::net::UnixListener;

    let tempdir = tempfile::tempdir().unwrap();
    let socket_path = tempdir.path().join("socket");
    let _listener = UnixListener::bind(&socket_path).unwrap();
    let invalid_path = tempdir
        .path()
        .join(OsString::from_vec(b"invalid-\xff.txt".to_vec()));
    let invalid_path_created = std::fs::write(&invalid_path, b"invalid").is_ok();

    let lix = open_lix_with_filesystem(tempdir.path()).await;

    assert_eq!(read_file(&lix, "/socket").await.unwrap(), None);
    assert_eq!(
        lix.execute("SELECT path FROM lix_file ORDER BY path", &[])
            .await
            .unwrap()
            .len(),
        0
    );
    assert!(
        std::fs::symlink_metadata(socket_path)
            .unwrap()
            .file_type()
            .is_socket()
    );
    if invalid_path_created {
        assert!(invalid_path.exists());
    }
    lix.close().await.unwrap();
}

#[tokio::test]
#[cfg(all(unix, feature = "fs_backend"))]
async fn filesystem_lix_writes_do_not_follow_symlink_collisions() {
    use std::os::unix::fs::symlink;

    let tempdir = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("outside.txt"), b"outside").unwrap();
    symlink(
        outside.path().join("outside.txt"),
        tempdir.path().join("blocked.txt"),
    )
    .unwrap();
    symlink(outside.path(), tempdir.path().join("blocked-dir")).unwrap();

    let lix = open_lix_with_filesystem(tempdir.path()).await;
    write_file(&lix, "/blocked.txt", b"lix".to_vec())
        .await
        .unwrap();
    write_file(&lix, "/blocked-dir/file.txt", b"nested".to_vec())
        .await
        .unwrap();

    assert_eq!(
        read_file(&lix, "/blocked.txt").await.unwrap().as_deref(),
        Some(b"lix".as_slice())
    );
    assert_eq!(
        read_file(&lix, "/blocked-dir/file.txt")
            .await
            .unwrap()
            .as_deref(),
        Some(b"nested".as_slice())
    );
    assert!(
        std::fs::symlink_metadata(tempdir.path().join("blocked.txt"))
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert!(
        std::fs::symlink_metadata(tempdir.path().join("blocked-dir"))
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        std::fs::read(outside.path().join("outside.txt")).unwrap(),
        b"outside"
    );
    assert!(!outside.path().join("file.txt").exists());
    lix.close().await.unwrap();
}

#[tokio::test]
#[cfg(all(unix, feature = "fs_backend"))]
async fn filesystem_lix_writes_do_not_replace_special_file_collisions() {
    use std::os::unix::fs::FileTypeExt;
    use std::os::unix::net::UnixListener;

    let tempdir = tempfile::tempdir().unwrap();
    let socket_path = tempdir.path().join("socket");
    let _listener = UnixListener::bind(&socket_path).unwrap();

    let lix = open_lix_with_filesystem(tempdir.path()).await;
    write_file(&lix, "/socket", b"lix".to_vec()).await.unwrap();

    assert_eq!(
        read_file(&lix, "/socket").await.unwrap().as_deref(),
        Some(b"lix".as_slice())
    );
    assert!(
        std::fs::symlink_metadata(socket_path)
            .unwrap()
            .file_type()
            .is_socket()
    );
    lix.close().await.unwrap();
}

async fn register_crm_task_schema(lix: &Lix) {
    let schema = r#"{
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "x-lix-key": "crm_task",
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "required": ["id", "title", "done", "meta"],
        "properties": {
            "id": { "type": "string" },
            "title": { "type": "string" },
            "done": { "type": "boolean" },
            "meta": { "type": "object" }
        },
        "additionalProperties": false
    }"#;

    lix.execute(
        "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
        &[Value::Text(schema.to_string())],
    )
    .await
    .unwrap();
}

fn assert_crm_task_projection(result: &lix_sdk::ExecuteResult) {
    assert_eq!(result.len(), 1);
    let row = &result.rows()[0];
    assert_eq!(
        row.get::<String>("title").unwrap(),
        "Draft RS SDK flow".to_string()
    );
    assert!(!row.get::<bool>("done").unwrap());

    let meta = row.get::<Value>("meta").unwrap();
    let Value::Json(meta) = meta else {
        panic!("expected meta JSON value, got {meta:?}");
    };
    assert_eq!(
        meta.get("priority").and_then(|value| value.as_str()),
        Some("high")
    );
    assert_eq!(
        meta.get("tags")
            .and_then(|value| value.as_array())
            .map(Vec::len),
        Some(2)
    );

    let snapshot = row.get::<Value>("lixcol_snapshot_content").unwrap();
    let Value::Json(snapshot) = snapshot else {
        panic!("expected snapshot JSON value, got {snapshot:?}");
    };
    assert_eq!(
        snapshot.get("id").and_then(|value| value.as_str()),
        Some("task-1")
    );
    assert_eq!(
        snapshot
            .get("meta")
            .and_then(|value| value.get("priority"))
            .and_then(|value| value.as_str()),
        Some("high")
    );

    let missing = row
        .value("missing")
        .expect_err("missing column should return a structured error");
    assert_eq!(missing.code, "LIX_COLUMN_NOT_FOUND");
}

async fn register_poison_task_schema(lix: &Lix) {
    let schema = r#"{
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "x-lix-key": "poison_task",
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "required": ["id", "title", "meta"],
        "properties": {
            "id": { "type": "string" },
            "title": { "type": "string" },
            "meta": { "type": "object" }
        },
        "additionalProperties": false
    }"#;

    lix.execute(
        "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
        &[Value::Text(schema.to_string())],
    )
    .await
    .unwrap();
}

async fn task_done(lix: &Lix, task_id: &str) -> bool {
    let result = lix
        .execute(
            "SELECT done FROM crm_task WHERE id = $1",
            &[Value::Text(task_id.to_string())],
        )
        .await
        .unwrap();

    let rows = result;
    assert_eq!(rows.len(), 1);

    match rows.rows()[0].values().first() {
        Some(Value::Boolean(done)) => *done,
        value => panic!("expected boolean done value, got {value:?}"),
    }
}

fn assert_closed(error: LixError) {
    assert_eq!(error.code, LixError::CODE_CLOSED);
}
