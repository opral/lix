use std::path::Path;
use std::time::{Duration, Instant};

use lix::{Lix, Value};
use lix_storage_filesystem::LocalFilesystem;

const EVENTUAL_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(50);

async fn open_workspace(root: &Path) -> (LocalFilesystem, Lix<LocalFilesystem>) {
    let storage = LocalFilesystem::open(root)
        .await
        .expect("positional LocalFilesystem::open(path)");
    let lix = lix::open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open Lix with LocalFilesystem");
    (storage, lix)
}

async fn read_lix_file(lix: &Lix<LocalFilesystem>, path: &str) -> Option<Vec<u8>> {
    let result = lix
        .execute(
            "SELECT content FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await
        .expect("read lix_file");
    result
        .rows()
        .first()
        .map(|row| row.get::<Vec<u8>>("content").expect("file content"))
}

async fn wait_for_lix_file(lix: &Lix<LocalFilesystem>, path: &str, expected: Option<&[u8]>) {
    let started = Instant::now();
    loop {
        let actual = read_lix_file(lix, path).await;
        if actual.as_deref() == expected {
            return;
        }
        assert!(
            started.elapsed() < EVENTUAL_TIMEOUT,
            "timed out waiting for {path}: actual={actual:?}, expected={expected:?}"
        );
        std::thread::sleep(POLL_INTERVAL);
    }
}

async fn active_head(lix: &Lix<LocalFilesystem>) -> String {
    let result = lix
        .execute("SELECT lix_active_branch_commit_id()", &[])
        .await
        .expect("active head");
    result
        .rows()
        .first()
        .expect("head row")
        .get::<String>("lix_active_branch_commit_id()")
        .expect("head id")
}

#[cfg(target_os = "linux")]
fn filesystem_worker_threads() -> usize {
    std::fs::read_dir("/proc/self/task")
        .expect("read process tasks")
        .filter_map(Result::ok)
        .filter_map(|entry| std::fs::read_to_string(entry.path().join("comm")).ok())
        // Linux task names are truncated to TASK_COMM_LEN - 1 (15 bytes).
        .filter(|name| "lix-sdk-filesystem-sync".starts_with(name.trim()))
        .count()
}

#[cfg(not(target_os = "linux"))]
fn filesystem_worker_threads() -> usize {
    0
}

#[tokio::test]
async fn positional_open_imports_workspace_but_never_physical_lix_metadata() {
    let root = tempfile::tempdir().expect("temp workspace");
    std::fs::write(root.path().join("seed.txt"), b"seed").expect("seed text");
    std::fs::create_dir_all(root.path().join("nested")).expect("nested dir");
    std::fs::write(root.path().join("nested/seed.bin"), [0, 1, 2, 255]).expect("seed binary");
    std::fs::create_dir_all(root.path().join(".lix")).expect("physical .lix");
    std::fs::write(root.path().join(".lix/oracle-sentinel.bin"), b"metadata")
        .expect("metadata sentinel");

    let (storage, lix) = open_workspace(root.path()).await;
    assert_eq!(
        read_lix_file(&lix, "/seed.txt").await.as_deref(),
        Some(&b"seed"[..])
    );
    assert_eq!(
        read_lix_file(&lix, "/nested/seed.bin").await.as_deref(),
        Some(&[0, 1, 2, 255][..])
    );
    assert_eq!(
        read_lix_file(&lix, "/.lix/oracle-sentinel.bin").await,
        None,
        "physical .lix metadata must never become a user lix_file"
    );

    lix.close().await.expect("close Lix");
    drop(lix);
    drop(storage);
}

#[tokio::test]
async fn background_disk_changes_cover_create_modify_delete_rename_nested_binary_without_loop() {
    let root = tempfile::tempdir().expect("temp workspace");
    std::fs::write(root.path().join("seed.txt"), b"first").expect("seed");
    std::fs::create_dir_all(root.path().join("old")).expect("old dir");
    std::fs::write(root.path().join("old/delete.txt"), b"delete").expect("delete seed");

    let (storage, lix) = open_workspace(root.path()).await;
    wait_for_lix_file(&lix, "/seed.txt", Some(b"first")).await;

    std::fs::write(root.path().join("created.txt"), b"created").expect("create");
    wait_for_lix_file(&lix, "/created.txt", Some(b"created")).await;

    std::fs::write(root.path().join("seed.txt"), b"modified").expect("modify");
    wait_for_lix_file(&lix, "/seed.txt", Some(b"modified")).await;

    std::fs::remove_file(root.path().join("old/delete.txt")).expect("delete");
    wait_for_lix_file(&lix, "/old/delete.txt", None).await;

    std::fs::rename(
        root.path().join("created.txt"),
        root.path().join("renamed.txt"),
    )
    .expect("rename");
    wait_for_lix_file(&lix, "/created.txt", None).await;
    wait_for_lix_file(&lix, "/renamed.txt", Some(b"created")).await;

    std::fs::create_dir_all(root.path().join("deep/nested")).expect("deep dirs");
    let binary = [0, 255, 17, 34, 0, 99];
    std::fs::write(root.path().join("deep/nested/data.bin"), binary).expect("binary");
    wait_for_lix_file(&lix, "/deep/nested/data.bin", Some(&binary)).await;

    let stable_head = active_head(&lix).await;
    std::thread::sleep(Duration::from_millis(1_500));
    assert_eq!(
        active_head(&lix).await,
        stable_head,
        "watcher must not republish its own materialized state"
    );

    lix.close().await.expect("close Lix");
    drop(lix);
    drop(storage);
}

#[tokio::test]
async fn acknowledged_lix_writes_are_on_disk_before_close_and_cold_reopen_is_exact() {
    let root = tempfile::tempdir().expect("temp workspace");
    let workers_before = filesystem_worker_threads();
    let (storage, lix) = open_workspace(root.path()).await;
    #[cfg(target_os = "linux")]
    assert_eq!(
        filesystem_worker_threads(),
        workers_before + 1,
        "one LocalFilesystem must own exactly one sync worker"
    );

    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
        &[
            Value::Text("/from-lix.txt".to_string()),
            Value::Blob(b"one".to_vec().into()),
        ],
    )
    .await
    .expect("insert from Lix");
    lix.execute(
        "UPDATE lix_file SET content = $1 WHERE path = $2",
        &[
            Value::Blob(b"two".to_vec().into()),
            Value::Text("/from-lix.txt".to_string()),
        ],
    )
    .await
    .expect("modify from Lix");
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
        &[
            Value::Text("/deep/generated.bin".to_string()),
            Value::Blob(vec![9, 0, 8, 255].into()),
        ],
    )
    .await
    .expect("nested binary from Lix");

    assert_eq!(
        std::fs::read(root.path().join("from-lix.txt")).unwrap(),
        b"two"
    );
    assert_eq!(
        std::fs::read(root.path().join("deep/generated.bin")).unwrap(),
        [9, 0, 8, 255]
    );

    lix.close().await.expect("close drains accepted writes");
    assert_eq!(
        std::fs::read(root.path().join("from-lix.txt")).unwrap(),
        b"two",
        "acknowledged Lix-to-disk work must be visible before close returns"
    );
    drop(lix);
    drop(storage);
    #[cfg(target_os = "linux")]
    assert_eq!(
        filesystem_worker_threads(),
        workers_before,
        "close/drop must join the sole sync worker"
    );

    let (reopened_storage, reopened) = open_workspace(root.path()).await;
    assert_eq!(
        read_lix_file(&reopened, "/from-lix.txt").await.as_deref(),
        Some(&b"two"[..])
    );
    assert_eq!(
        read_lix_file(&reopened, "/deep/generated.bin")
            .await
            .as_deref(),
        Some(&[9, 0, 8, 255][..])
    );
    reopened.close().await.expect("close reopened Lix");
    drop(reopened);
    drop(reopened_storage);
    #[cfg(target_os = "linux")]
    assert_eq!(filesystem_worker_threads(), workers_before);
}
