use std::io::{Cursor, Write as _};
use std::path::Path;

use async_trait::async_trait;
use lix::storage::Storage;
use lix::{Value, open_lix};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;

#[async_trait]
trait ReopenStorage: Storage + Clone + Send + Sync + Sized + 'static {
    fn open(path: &Path) -> Self;
    async fn flush(&self);
}

#[async_trait]
impl ReopenStorage for RocksDB {
    fn open(path: &Path) -> Self {
        Self::open(path).expect("open RocksDB plugin fixture")
    }

    async fn flush(&self) {
        self.flush().expect("flush RocksDB plugin fixture");
    }
}

#[async_trait]
impl ReopenStorage for SlateDB {
    fn open(path: &Path) -> Self {
        Self::open(path).expect("open SlateDB plugin fixture")
    }

    async fn flush(&self) {
        self.flush().await.expect("flush SlateDB plugin fixture");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rocksdb_plugin_blobref_owner_survives_cold_reopen() {
    plugin_blobref_owner_survives_cold_reopen::<RocksDB>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slatedb_plugin_blobref_owner_survives_cold_reopen() {
    plugin_blobref_owner_survives_cold_reopen::<SlateDB>().await;
}

async fn plugin_blobref_owner_survives_cold_reopen<S: ReopenStorage>() {
    let temp = tempfile::tempdir().expect("create plugin fixture");
    let path = temp.path().join("database");
    {
        let storage = S::open(&path);
        let lix = open_lix()
            .with_storage(storage.clone())
            .await
            .expect("initialize plugin fixture");
        install_csv_plugin(&lix).await;
        write_file(&lix, "/plugin-reopen.csv", b"quick,dog\n").await;
        assert_eq!(read_file(&lix, "/plugin-reopen.csv").await, b"quick,dog\n");
        lix.close().await.expect("close plugin fixture");
        drop(lix);
        storage.flush().await;
    }

    let storage = S::open(&path);
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("reopen plugin fixture");
    assert_eq!(read_file(&lix, "/plugin-reopen.csv").await, b"quick,dog\n");
    lix.close().await.expect("close reopened plugin fixture");
    drop(lix);
    storage.flush().await;
}

async fn install_csv_plugin<S: Storage + Clone + Send + Sync + 'static>(lix: &lix::Lix<S>) {
    write_file(
        lix,
        "/.lix/plugins/plugin_csv.lixplugin",
        &csv_plugin_archive(),
    )
    .await;
}

async fn write_file<S: Storage + Clone + Send + Sync + 'static>(
    lix: &lix::Lix<S>,
    path: &str,
    content: &[u8],
) {
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
         ON CONFLICT (path) DO UPDATE SET content = excluded.content",
        &[
            Value::Text(path.to_owned()),
            Value::Blob(content.to_vec().into()),
        ],
    )
    .await
    .expect("write plugin fixture file");
}

async fn read_file<S: Storage + Clone + Send + Sync + 'static>(
    lix: &lix::Lix<S>,
    path: &str,
) -> Vec<u8> {
    lix.execute(
        "SELECT content FROM lix_file WHERE path = $1",
        &[Value::Text(path.to_owned())],
    )
    .await
    .expect("read plugin fixture file")
    .rows()[0]
        .get::<Vec<u8>>("content")
        .expect("plugin content bytes")
}

fn csv_plugin_archive() -> Vec<u8> {
    let wasm_path = env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_plugin_csv");
    let wasm = std::fs::read(wasm_path).expect("read CSV plugin WASM");
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        (
            "manifest.json",
            include_bytes!("../../../plugins/csv/manifest.json").as_slice(),
        ),
        (
            "schema/csv_table.json",
            include_bytes!("../../../plugins/csv/schema/csv_table.json").as_slice(),
        ),
        (
            "schema/csv_row.json",
            include_bytes!("../../../plugins/csv/schema/csv_row.json").as_slice(),
        ),
        ("plugin.wasm", wasm.as_slice()),
    ] {
        writer
            .start_file(path, options)
            .expect("start plugin archive entry");
        writer.write_all(bytes).expect("write plugin archive entry");
    }
    writer.finish().expect("finish plugin archive").into_inner()
}
