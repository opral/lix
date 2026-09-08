use lix::{Value, open_lix};
use lix_storage_filesystem::FilesystemStorage;

fn assert_send<T: Send>(_: T) {}

#[test]
fn open_and_start_sync_work_under_plain_block_on() {
    let root = tempfile::tempdir().expect("temporary filesystem root");
    std::fs::write(root.path().join("hello.txt"), b"hello from disk").expect("seed working file");

    futures_lite::future::block_on(async {
        let storage = FilesystemStorage::new(root.path())
            .open()
            .expect("open filesystem storage");
        let lix = open_lix()
            .with_storage(storage.clone())
            .await
            .expect("open Lix under plain block_on");
        assert_send(storage.start_sync(&lix));
        storage
            .start_sync(&lix)
            .await
            .expect("start sync under plain block_on");

        let result = lix
            .execute(
                "SELECT content FROM lix_file WHERE path = $1",
                &[Value::Text("/hello.txt".to_owned())],
            )
            .await
            .expect("read synchronized file");
        assert_eq!(result.rows().len(), 1);

        assert_eq!(
            std::fs::read(root.path().join(".lix/README.md")).expect("bootstrap README on disk"),
            include_bytes!("../../lix/src/init_readme.md"),
        );
        assert!(root.path().join(".lix/app_data").is_dir());
        assert!(root.path().join(".lix/plugins").is_dir());

        storage
            .stop_sync()
            .await
            .expect("stop sync under plain block_on");
        lix.close().await.expect("close Lix");
    });

    // Offline edits and deletions in an existing repository still win on reopen.
    for content in [Some(b"custom guide".as_slice()), None] {
        let path = root.path().join(".lix/README.md");
        if let Some(content) = content {
            std::fs::write(&path, content).unwrap();
        } else {
            std::fs::remove_file(&path).unwrap();
        }
        futures_lite::future::block_on(async {
            let storage = FilesystemStorage::new(root.path()).open().unwrap();
            let lix = open_lix().with_storage(storage.clone()).await.unwrap();
            storage.start_sync(&lix).await.unwrap();
            let result = lix
                .execute(
                    "SELECT content FROM lix_file WHERE path = '/.lix/README.md'",
                    &[],
                )
                .await
                .unwrap();
            if let Some(content) = content {
                assert_eq!(
                    result.rows()[0].values(),
                    &[Value::Blob(content.to_vec().into())]
                );
                assert_eq!(std::fs::read(&path).unwrap(), content);
            } else {
                assert!(result.rows().is_empty());
                assert!(!path.exists());
            }
            storage.stop_sync().await.unwrap();
            lix.close().await.unwrap();
        });
    }
}

#[test]
fn bootstrap_survives_storage_reopen_before_first_sync() {
    let root = tempfile::tempdir().unwrap();
    futures_lite::future::block_on(async {
        let storage = FilesystemStorage::new(root.path()).open().unwrap();
        let lix = open_lix().with_storage(storage.clone()).await.unwrap();
        lix.close().await.unwrap();
    });
    assert!(!root.path().join(".lix/README.md").exists());
    assert!(
        !root
            .path()
            .join(".lix/.internal/filesystem-materialized")
            .exists()
    );
    futures_lite::future::block_on(async {
        let storage = FilesystemStorage::new(root.path()).open().unwrap();
        let lix = open_lix().with_storage(storage.clone()).await.unwrap();
        storage.start_sync(&lix).await.unwrap();
        assert_eq!(
            std::fs::read(root.path().join(".lix/README.md")).unwrap(),
            include_bytes!("../../lix/src/init_readme.md")
        );
        assert!(root.path().join(".lix/app_data").is_dir());
        assert!(
            root.path()
                .join(".lix/.internal/filesystem-materialized")
                .is_file()
        );
        storage.stop_sync().await.unwrap();
        lix.close().await.unwrap();
    });
}

#[test]
fn separately_opened_storage_reads_completion_when_sync_starts() {
    let root = tempfile::tempdir().unwrap();
    futures_lite::future::block_on(async {
        let first = FilesystemStorage::new(root.path()).open().unwrap();
        let second = FilesystemStorage::new(root.path()).open().unwrap();
        let first_lix = open_lix().with_storage(first.clone()).await.unwrap();
        let second_lix = open_lix().with_storage(second.clone()).await.unwrap();
        second.start_sync(&second_lix).await.unwrap();
        let readme = root.path().join(".lix/README.md");
        assert_eq!(
            std::fs::read(&readme).unwrap(),
            include_bytes!("../../lix/src/init_readme.md")
        );
        assert!(root.path().join(".lix/app_data").is_dir());
        second.stop_sync().await.unwrap();
        std::fs::remove_file(&readme).unwrap();
        // The first handle predates completion, but must now honor this deletion.
        first.start_sync(&first_lix).await.unwrap();
        assert!(!readme.exists());
        assert!(
            first_lix
                .execute(
                    "SELECT id FROM lix_file WHERE path = '/.lix/README.md'",
                    &[]
                )
                .await
                .unwrap()
                .rows()
                .is_empty()
        );
        first.stop_sync().await.unwrap();
        first_lix.close().await.unwrap();
        second_lix.close().await.unwrap();
    });
}

#[test]
fn failed_initial_sync_does_not_record_materialization() {
    let root = tempfile::tempdir().unwrap();
    futures_lite::future::block_on(async {
        let storage = FilesystemStorage::new(root.path()).open().unwrap();
        let lix = open_lix().with_storage(storage.clone()).await.unwrap();
        let conflict = root.path().join(".lix/app_data");
        std::fs::write(&conflict, b"blocks the bootstrap directory").unwrap();
        assert!(storage.start_sync(&lix).await.is_err());
        assert!(
            !root
                .path()
                .join(".lix/.internal/filesystem-materialized")
                .exists()
        );
        std::fs::remove_file(conflict).unwrap();
        storage.start_sync(&lix).await.unwrap();
        assert_eq!(
            std::fs::read(root.path().join(".lix/README.md")).unwrap(),
            include_bytes!("../../lix/src/init_readme.md")
        );
        assert!(root.path().join(".lix/app_data").is_dir());
        assert!(
            root.path()
                .join(".lix/.internal/filesystem-materialized")
                .is_file()
        );
        storage.stop_sync().await.unwrap();
        lix.close().await.unwrap();
    });
}
