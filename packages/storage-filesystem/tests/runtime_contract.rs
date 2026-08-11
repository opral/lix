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

        storage
            .stop_sync()
            .await
            .expect("stop sync under plain block_on");
        lix.close().await.expect("close Lix");
    });
}
