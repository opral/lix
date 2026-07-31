use std::io::{Cursor, Write as _};
use std::path::Path;

use lix_sdk::{
    CreateBranchOptions, Lix, MergeBranchOptions, OpenLixOptions, Storage, SwitchBranchOptions,
    Value, open_lix,
};

#[tokio::test]
async fn plugin_resolved_rows_are_included_in_semantic_merge_change_stats() {
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    install_csv_plugin(&lix).await;
    write_file(&lix, "/semantic-merge.csv", b"quick,dog\n").await;
    let main_branch_id = lix.active_branch_id().await.unwrap();
    let source = lix
        .create_branch(CreateBranchOptions {
            id: None,
            name: "Semantic source".to_string(),
            from_commit_id: None,
        })
        .await
        .unwrap();

    lix.switch_branch(SwitchBranchOptions {
        branch_id: source.id.clone(),
    })
    .await
    .unwrap();
    write_file(&lix, "/semantic-merge.csv", b"very quick,dog\n").await;
    lix.switch_branch(SwitchBranchOptions {
        branch_id: main_branch_id,
    })
    .await
    .unwrap();
    write_file(&lix, "/semantic-merge.csv", b"quick,sleepy dog\n").await;

    let preview = lix
        .merge_branch_preview(lix_sdk::MergeBranchPreviewOptions {
            source_branch_id: source.id.clone(),
        })
        .await
        .unwrap();
    assert!(preview.conflicts.is_empty());
    assert_eq!(preview.change_stats.total, 1);
    assert_eq!(preview.change_stats.added, 0);
    assert_eq!(preview.change_stats.modified, 1);
    assert_eq!(preview.change_stats.removed, 0);

    let receipt = lix
        .merge_branch(MergeBranchOptions {
            source_branch_id: source.id,
        })
        .await
        .unwrap();
    assert_eq!(receipt.change_stats, preview.change_stats);
    assert_eq!(
        read_file(&lix, "/semantic-merge.csv").await,
        b"very quick,sleepy dog\n"
    );
    lix.close().await.unwrap();
}

async fn install_csv_plugin<StorageImpl>(lix: &Lix<StorageImpl>)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    write_file(
        lix,
        "/.lix/plugins/plugin_csv.lixplugin",
        &csv_plugin_archive(),
    )
    .await;
}

async fn write_file<StorageImpl>(lix: &Lix<StorageImpl>, path: &str, data: &[u8])
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_file (path, data) VALUES ($1, $2) \
         ON CONFLICT (path) DO UPDATE SET data = excluded.data",
        &[
            Value::Text(path.to_string()),
            Value::Blob(data.to_vec().into()),
        ],
    )
    .await
    .unwrap();
}

async fn read_file<StorageImpl>(lix: &Lix<StorageImpl>, path: &str) -> Vec<u8>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "SELECT data FROM lix_file WHERE path = $1",
        &[Value::Text(path.to_string())],
    )
    .await
    .unwrap()
    .rows()[0]
        .get::<Vec<u8>>("data")
        .unwrap()
}

fn csv_plugin_archive() -> Vec<u8> {
    let wasm_path = env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_plugin_csv");
    let wasm = std::fs::read(Path::new(wasm_path)).expect("read CSV plugin wasm");
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        (
            "manifest.json",
            include_str!("../../../plugins/csv/manifest.json").as_bytes(),
        ),
        (
            "schema/csv_v2_table.json",
            include_str!("../../../plugins/csv/schema/csv_v2_table.json").as_bytes(),
        ),
        (
            "schema/csv_v2_row.json",
            include_str!("../../../plugins/csv/schema/csv_v2_row.json").as_bytes(),
        ),
        ("plugin.wasm", wasm.as_slice()),
    ] {
        writer.start_file(path, options).unwrap();
        writer.write_all(bytes).unwrap();
    }
    writer.finish().unwrap().into_inner()
}
