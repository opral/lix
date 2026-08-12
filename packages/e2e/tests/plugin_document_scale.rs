use std::fs;
use std::io::{Cursor, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use lix::storage::Storage;
use lix::{ExecuteBatchStatement, Lix, Value, open_lix};

const MARKDOWN_PLUGIN_KEY: &str = "plugin_markdown";

#[tokio::test]
async fn atomic_markdown_import_scales_to_1184_documents() {
    assert_format_scales(
        MARKDOWN_PLUGIN_KEY,
        build_markdown_plugin_archive(),
        "md",
        (0..1_184)
            .map(|index| format!("# Document {index}\n\nBody {index}.\n").into_bytes())
            .collect(),
        "markdown_node",
    )
    .await;
}

#[tokio::test]
async fn atomic_plugin_import_scaling_is_format_independent() {
    assert_format_scales(
        "plugin_csv",
        build_csv_plugin_archive(),
        "csv",
        (0..17)
            .map(|index| format!("name,value\nrow-{index},{index}\n").into_bytes())
            .collect(),
        "csv_row",
    )
    .await;
    assert_format_scales(
        "plugin_text",
        build_git_text_v2_plugin_archive(),
        "txt",
        (0..17)
            .map(|index| format!("line {index}\nsecond line {index}\n").into_bytes())
            .collect(),
        "text_line",
    )
    .await;
    assert_format_scales(
        "plugin_excalidraw",
        build_excalidraw_plugin_archive(),
        "excalidraw",
        (0..17)
            .map(|index| {
                format!(
                    r#"{{"type":"excalidraw","version":2,"source":"https://excalidraw.com","elements":[{{"id":"shape-{index}","type":"rectangle","x":{index},"y":2,"width":3,"height":4,"isDeleted":false}}],"appState":{{}},"files":{{}}}}"#
                )
                .into_bytes()
            })
            .collect(),
        "excalidraw_element",
    )
    .await;
}

#[tokio::test]
async fn sequential_batches_survive_observation_cache_eviction() {
    const DOCUMENTS: usize = 17;

    let lix = open_lix()
        .await
        .expect("document-scale workspace should open");
    install_plugin(&lix, "plugin_text", &build_git_text_v2_plugin_archive()).await;

    lix.execute_batch(&[file_insert_statement(
        "txt",
        (0..DOCUMENTS).map(|index| format!("before {index}\n").into_bytes()),
    )])
    .await
    .expect("the first batch should exceed the Store working set");

    let updates = (0..DOCUMENTS)
        .map(|index| ExecuteBatchStatement {
            label: None,
            sql: "UPDATE lix_file SET content = $1 WHERE path = $2".to_string(),
            params: vec![
                Value::Blob(format!("after {index}\n").into_bytes().into()),
                Value::Text(format!("/scale-document-{index:04}.txt")),
            ],
        })
        .collect::<Vec<_>>();
    lix.execute_batch(&updates)
        .await
        .expect("retained observations must survive cache eviction between batches");

    for index in 0..DOCUMENTS {
        assert_eq!(
            read_file(&lix, &format!("/scale-document-{index:04}.txt")).await,
            format!("after {index}\n").into_bytes()
        );
    }
    lix.close()
        .await
        .expect("document-scale workspace should close");
}

async fn assert_format_scales(
    plugin_key: &str,
    archive: Vec<u8>,
    extension: &str,
    documents: Vec<Vec<u8>>,
    semantic_table: &str,
) -> Duration {
    let lix = open_lix()
        .await
        .expect("document-scale workspace should open");
    install_plugin(&lix, plugin_key, &archive).await;

    let expected_count = documents.len();
    let first = documents.first().cloned().unwrap();
    let last = documents.last().cloned().unwrap();
    let input_bytes = documents.iter().map(Vec::len).sum::<usize>();
    let statement = file_insert_statement(extension, documents);
    let started = Instant::now();
    let results = lix
        .execute_batch(&[statement])
        .await
        .expect("an atomic import must not be bounded by the live actor working set");
    let elapsed = started.elapsed();
    eprintln!(
        "plugin_document_scale format={extension} documents={expected_count} input_bytes={input_bytes} elapsed_ms={:.3} per_document_us={:.3}",
        elapsed.as_secs_f64() * 1_000.0,
        elapsed.as_secs_f64() * 1_000_000.0 / expected_count as f64,
    );
    assert_eq!(results[0].rows_affected(), expected_count as u64);

    let files = lix
        .execute(
            "SELECT COUNT(*) AS count FROM lix_file WHERE path LIKE '/scale-document-%'",
            &[],
        )
        .await
        .expect("imported documents should query");
    assert_eq!(
        files.rows()[0].get::<i64>("count").unwrap(),
        expected_count as i64
    );
    let semantic_rows = lix
        .execute(
            &format!("SELECT COUNT(*) AS count FROM {semantic_table}"),
            &[],
        )
        .await
        .expect("semantic rows should query");
    assert!(
        semantic_rows.rows()[0].get::<i64>("count").unwrap() >= expected_count as i64,
        "{plugin_key} should materialize semantic state for every document"
    );
    assert_eq!(
        read_file(&lix, &format!("/scale-document-0000.{extension}")).await,
        first
    );
    assert_eq!(
        read_file(
            &lix,
            &format!("/scale-document-{:04}.{extension}", expected_count - 1)
        )
        .await,
        last
    );

    lix.close()
        .await
        .expect("document-scale workspace should close");
    elapsed
}

fn file_insert_statement(
    extension: &str,
    documents: impl IntoIterator<Item = Vec<u8>>,
) -> ExecuteBatchStatement {
    let mut sql = String::from("INSERT INTO lix_file (path, content) VALUES ");
    let mut params = Vec::new();
    for (index, document) in documents.into_iter().enumerate() {
        if index > 0 {
            sql.push(',');
        }
        let path_parameter = params.len() + 1;
        let data_parameter = params.len() + 2;
        sql.push_str(&format!("(${path_parameter}, ${data_parameter})"));
        params.push(Value::Text(format!(
            "/scale-document-{index:04}.{extension}"
        )));
        params.push(Value::Blob(document.into()));
    }
    ExecuteBatchStatement {
        sql,
        params,
        label: None,
    }
}

async fn read_file<StorageImpl>(lix: &Lix<StorageImpl>, path: &str) -> Vec<u8>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "SELECT content FROM lix_file WHERE path = $1",
        &[Value::Text(path.to_owned())],
    )
    .await
    .expect("imported file should read")
    .rows()[0]
        .get::<Vec<u8>>("content")
        .expect("imported file data should be bytes")
}

async fn install_plugin<StorageImpl>(lix: &Lix<StorageImpl>, key: &str, archive: &[u8])
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
        &[
            Value::Text(format!("/.lix/plugins/{key}.lixplugin")),
            Value::Blob(archive.to_vec().into()),
        ],
    )
    .await
    .expect("reference plugin should install");
}

fn build_markdown_plugin_archive() -> Vec<u8> {
    build_plugin_archive(
        Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_MARKDOWN_plugin_markdown")),
        include_str!("../../../plugins/markdown/manifest.json"),
        &[(
            "schema/markdown_node.json",
            include_str!("../../../plugins/markdown/schema/markdown_node.json"),
        )],
    )
}

fn build_csv_plugin_archive() -> Vec<u8> {
    build_plugin_archive(
        Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_plugin_csv")),
        include_str!("../../../plugins/csv/manifest.json"),
        &[
            (
                "schema/csv_table.json",
                include_str!("../../../plugins/csv/schema/csv_table.json"),
            ),
            (
                "schema/csv_row.json",
                include_str!("../../../plugins/csv/schema/csv_row.json"),
            ),
        ],
    )
}

fn build_git_text_v2_plugin_archive() -> Vec<u8> {
    build_plugin_archive(
        Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_TEXT_plugin_text")),
        include_str!("../../../plugins/text/manifest.json"),
        &[(
            "schema/text_line.json",
            include_str!("../../../plugins/text/schema/text_line.json"),
        )],
    )
}

fn build_excalidraw_plugin_archive() -> Vec<u8> {
    build_plugin_archive(
        Path::new(env!(
            "CARGO_CDYLIB_FILE_PLUGIN_EXCALIDRAW_plugin_excalidraw"
        )),
        include_str!("../../../plugins/excalidraw/manifest.json"),
        &[
            (
                "schema/excalidraw_scene.json",
                include_str!("../../../plugins/excalidraw/schema/excalidraw_scene.json"),
            ),
            (
                "schema/excalidraw_element.json",
                include_str!("../../../plugins/excalidraw/schema/excalidraw_element.json"),
            ),
            (
                "schema/excalidraw_file.json",
                include_str!("../../../plugins/excalidraw/schema/excalidraw_file.json"),
            ),
        ],
    )
}

fn build_plugin_archive(wasm_path: &Path, manifest: &str, schemas: &[(&str, &str)]) -> Vec<u8> {
    let wasm = fs::read(wasm_path).unwrap_or_else(|error| {
        panic!(
            "failed to read plugin component at {}: {error}",
            wasm_path.display()
        )
    });
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    writer
        .start_file("manifest.json", options)
        .expect("manifest entry should start");
    writer
        .write_all(manifest.as_bytes())
        .expect("manifest should write");
    for (path, schema) in schemas {
        writer
            .start_file(*path, options)
            .expect("schema entry should start");
        writer
            .write_all(schema.as_bytes())
            .expect("schema should write");
    }
    writer
        .start_file("plugin.wasm", options)
        .expect("component entry should start");
    writer.write_all(&wasm).expect("component should write");
    writer
        .finish()
        .expect("plugin archive should finish")
        .into_inner()
}
