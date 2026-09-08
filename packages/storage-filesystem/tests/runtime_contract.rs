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
