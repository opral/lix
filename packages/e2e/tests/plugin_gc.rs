use std::io::{Cursor, Write as _};
use std::path::Path;

use lix::storage::{Memory, Storage};
use lix::storage_adapter::StorageAdapter;
use lix::storage_bench::{
    collect_repository_gc_for_bench, read_binary_cas_for_bench, write_binary_cas_for_bench,
};
use lix::{CreateBranchOptions, Value, open_lix};

#[tokio::test]
async fn repository_gc_keeps_plugin_wasm_for_cold_runtime_execution() {
    let memory = Memory::new();
    let lix = open_lix().with_storage(memory.clone()).await.unwrap();
    let (archive, wasm) = csv_plugin_archive();
    install_csv_plugin(&lix, archive).await;
    write_file(&lix, "/owned.csv", b"before,gc\n").await;

    let storage = StorageAdapter::new(memory.clone());
    let wasm_hash = write_binary_cas_for_bench(&storage, &wasm).await.unwrap();
    let orphan_hash = write_binary_cas_for_bench(&storage, b"unrelated-orphan")
        .await
        .unwrap();
    collect_repository_gc_for_bench(&storage).await.unwrap();
    assert!(
        read_binary_cas_for_bench(&storage, &wasm_hash)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        read_binary_cas_for_bench(&storage, &orphan_hash)
            .await
            .unwrap()
            .is_none()
    );

    lix.close().await.unwrap();
    let reopened = open_lix().with_storage(memory).await.unwrap();
    write_file(&reopened, "/owned.csv", b"after,gc\n").await;
    assert_eq!(read_file(&reopened, "/owned.csv").await, b"after,gc\n");
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn repository_gc_keeps_graph_reachable_file_history_content() {
    let memory = Memory::new();
    let lix = open_lix().with_storage(memory.clone()).await.unwrap();
    write_file(&lix, "/history.txt", b"before gc").await;
    let file_id = lix
        .execute(
            "SELECT id FROM lix_file WHERE path = '/history.txt'",
            &[],
        )
        .await
        .unwrap()
        .rows()[0]
        .get::<String>("id")
        .unwrap();
    let historical_checkpoint = lix.create_checkpoint().await.unwrap().commit_id;
    write_file(&lix, "/history.txt", b"after gc").await;
    lix.create_checkpoint().await.unwrap();

    let storage = StorageAdapter::new(memory);
    let orphan_hash = write_binary_cas_for_bench(&storage, b"history-gc-orphan")
        .await
        .unwrap();
    collect_repository_gc_for_bench(&storage).await.unwrap();
    assert!(
        read_binary_cas_for_bench(&storage, &orphan_hash)
            .await
            .unwrap()
            .is_none(),
        "the test must run a reclaiming CAS sweep"
    );

    let history = lix
        .execute(
            "SELECT content FROM lix_history('lix_file', $1) \
             WHERE id = $2 ORDER BY lixcol_depth ASC LIMIT 1",
            &[
                Value::Text(historical_checkpoint),
                Value::Text(file_id),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        history.rows()[0].get::<Vec<u8>>("content").unwrap(),
        b"before gc",
    );
    lix.close().await.unwrap();
}

#[tokio::test]
async fn repository_gc_reclaims_plugin_wasm_after_final_registry_root_releases() {
    let memory = Memory::new();
    let lix = open_lix().with_storage(memory.clone()).await.unwrap();
    let (archive, wasm) = csv_plugin_archive();
    install_csv_plugin(&lix, archive).await;
    let storage = StorageAdapter::new(memory);
    let wasm_hash = write_binary_cas_for_bench(&storage, &wasm).await.unwrap();

    let branch = lix
        .create_branch(CreateBranchOptions {
            id: None,
            name: "plugin retained".to_owned(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    lix.execute(
        "DELETE FROM lix_file WHERE path = '/.lix/plugins/plugin_csv.lixplugin'",
        &[],
    )
    .await
    .unwrap();
    lix.create_checkpoint().await.unwrap();
    collect_repository_gc_for_bench(&storage).await.unwrap();
    assert!(
        read_binary_cas_for_bench(&storage, &wasm_hash)
            .await
            .unwrap()
            .is_some()
    );

    lix.execute(
        "DELETE FROM lix_branch WHERE id = $1",
        &[Value::Text(branch.id)],
    )
    .await
    .unwrap();
    collect_repository_gc_for_bench(&storage).await.unwrap();
    assert!(
        read_binary_cas_for_bench(&storage, &wasm_hash)
            .await
            .unwrap()
            .is_none()
    );
    lix.close().await.unwrap();
}

async fn install_csv_plugin<S>(lix: &lix::Lix<S>, archive: Vec<u8>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    write_file(lix, "/.lix/plugins/plugin_csv.lixplugin", &archive).await;
}

async fn write_file<S>(lix: &lix::Lix<S>, path: &str, content: &[u8])
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
         ON CONFLICT (path) DO UPDATE SET content = excluded.content",
        &[
            Value::Text(path.to_owned()),
            Value::Blob(content.to_vec().into()),
        ],
    )
    .await
    .unwrap();
}

async fn read_file<S>(lix: &lix::Lix<S>, path: &str) -> Vec<u8>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "SELECT content FROM lix_file WHERE path = $1",
        &[Value::Text(path.to_owned())],
    )
    .await
    .unwrap()
    .rows()[0]
        .get("content")
        .unwrap()
}

fn csv_plugin_archive() -> (Vec<u8>, Vec<u8>) {
    let wasm_path = Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_plugin_csv"));
    let wasm = std::fs::read(wasm_path).expect("read CSV plugin component");
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        (
            "manifest.json",
            include_str!("../../../plugins/csv/manifest.json").as_bytes(),
        ),
        (
            "schema/csv_table.json",
            include_str!("../../../plugins/csv/schema/csv_table.json").as_bytes(),
        ),
        (
            "schema/csv_row.json",
            include_str!("../../../plugins/csv/schema/csv_row.json").as_bytes(),
        ),
        ("plugin.wasm", wasm.as_slice()),
    ] {
        writer.start_file(path, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    (writer.finish().unwrap().into_inner(), wasm)
}
