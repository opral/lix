use std::io::{Cursor, Write as _};
use std::path::Path;

use lix::storage::{Memory, Storage};
use lix::{CreateBranchOptions, Lix, MergeBranchOptions, SwitchBranchOptions, Value, open_lix};

#[tokio::test]
async fn plugin_resolved_rows_are_included_in_semantic_merge_change_stats() {
    let lix = open_lix().await.unwrap();
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
        .merge_branch_preview(lix::MergeBranchPreviewOptions {
            source_branch_id: source.id.clone(),
        })
        .await
        .unwrap();
    assert!(preview.conflicts.is_empty());
    assert_eq!(preview.change_stats.total, 1);
    assert_eq!(preview.change_stats.added, 0);
    assert_eq!(preview.change_stats.modified, 1);
    assert_eq!(preview.change_stats.removed, 0);
    assert_ne!(
        preview.base_commit_id, preview.target_head_commit_id,
        "plugin resolution must retain a distinct authenticated merge base"
    );
    assert_ne!(
        preview.target_head_commit_id, preview.source_head_commit_id,
        "plugin resolution must compare the two branch heads"
    );

    let receipt = lix
        .merge_branch(MergeBranchOptions {
            source_branch_id: source.id,
        })
        .await
        .unwrap();
    assert_eq!(receipt.change_stats, preview.change_stats);
    assert_eq!(receipt.base_commit_id, preview.base_commit_id);
    assert_eq!(
        receipt.target_head_before_commit_id,
        preview.target_head_commit_id
    );
    assert_eq!(
        receipt.source_head_before_commit_id,
        preview.source_head_commit_id
    );
    assert_eq!(
        receipt.created_merge_commit_id.as_deref(),
        Some(receipt.target_head_after_commit_id.as_str())
    );
    assert_eq!(
        read_file(&lix, "/semantic-merge.csv").await,
        b"very quick,sleepy dog\n"
    );
    lix.close().await.unwrap();
}

#[tokio::test]
async fn plugin_wasm_owner_survives_memory_snapshot_reopen() {
    let storage = Memory::new();
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open Memory plugin fixture");
    install_csv_plugin(&lix).await;
    let owner_rows = lix
        .execute(
            "SELECT path FROM lix_file WHERE path = $1",
            &[Value::Text("/plugin_csv.wasm".into())],
        )
        .await
        .expect("inspect plugin WASM owner");
    assert_eq!(
        owner_rows.rows().len(),
        1,
        "plugin WASM owner must be durable"
    );
    let owner_content = lix
        .execute(
            "SELECT content FROM lix_file WHERE path = $1",
            &[Value::Text("/plugin_csv.wasm".into())],
        )
        .await
        .expect("inspect plugin WASM bytes");
    assert_eq!(
        owner_content.rows().len(),
        1,
        "plugin WASM owner payload must be readable"
    );
    write_file(&lix, "/memory-reopen.csv", b"quick,dog\n").await;
    assert_eq!(read_file(&lix, "/memory-reopen.csv").await, b"quick,dog\n");
    let snapshot = storage
        .export_snapshot()
        .expect("export authenticated plugin owner snapshot");
    lix.close().await.expect("close Memory plugin fixture");

    let reopened_storage = Memory::from_snapshot(&snapshot).expect("restore Memory snapshot");
    let reopened = open_lix()
        .with_storage(reopened_storage)
        .await
        .expect("reopen Memory plugin fixture");
    assert_eq!(
        read_file(&reopened, "/memory-reopen.csv").await,
        b"quick,dog\n"
    );
    reopened
        .close()
        .await
        .expect("close reopened Memory plugin fixture");
}

const CONFLICT_COUNT: usize = 1_500;

#[tokio::test]
async fn semantic_merge_resolves_more_conflicts_than_default_transition_caps() {
    let lix = open_lix().await.unwrap();
    install_csv_plugin(&lix).await;
    let base = csv_rows(|index| format!("left-{index:04},right-{index:04}"));
    write_file(&lix, "/large-semantic-merge.csv", &base).await;
    let main_branch_id = lix.active_branch_id().await.unwrap();
    let source = lix
        .create_branch(CreateBranchOptions {
            id: None,
            name: "Large semantic source".to_string(),
            from_commit_id: None,
        })
        .await
        .unwrap();

    lix.switch_branch(SwitchBranchOptions {
        branch_id: source.id.clone(),
    })
    .await
    .unwrap();
    let source_bytes = csv_rows(|index| format!("source-{index:04},right-{index:04}"));
    write_file(&lix, "/large-semantic-merge.csv", &source_bytes).await;
    lix.switch_branch(SwitchBranchOptions {
        branch_id: main_branch_id,
    })
    .await
    .unwrap();
    let target_bytes = csv_rows(|index| format!("left-{index:04},target-{index:04}"));
    write_file(&lix, "/large-semantic-merge.csv", &target_bytes).await;

    lix.merge_branch(MergeBranchOptions {
        source_branch_id: source.id,
    })
    .await
    .unwrap();
    let expected = csv_rows(|index| format!("source-{index:04},target-{index:04}"));
    assert_eq!(read_file(&lix, "/large-semantic-merge.csv").await, expected);
    lix.close().await.unwrap();
}

fn csv_rows(row: impl Fn(usize) -> String) -> Vec<u8> {
    let mut csv = String::new();
    for index in 0..CONFLICT_COUNT {
        csv.push_str(&row(index));
        csv.push('\n');
    }
    csv.into_bytes()
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
        "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
         ON CONFLICT (path) DO UPDATE SET content = excluded.content",
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
        "SELECT content FROM lix_file WHERE path = $1",
        &[Value::Text(path.to_string())],
    )
    .await
    .unwrap()
    .rows()[0]
        .get::<Vec<u8>>("content")
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
    writer.finish().unwrap().into_inner()
}
