//! Matched black-box benchmarks for plugin-backed file and merge workflows.
//!
//! The workload intentionally uses only the public file SQL and transaction
//! APIs. It can therefore be copied unchanged between the frozen `origin/main`
//! worktree and an API-refactor worktree; each worktree supplies its own plugin
//! components and manifests through Cargo artifact dependencies.

#![recursion_limit = "512"]

#[allow(dead_code)]
mod benchmark_metrics;

use std::fs;
use std::io::{Cursor, Write};
use std::path::Path;
use std::time::Instant;

use benchmark_metrics::{
    AllocationScope, BenchmarkFixture, BenchmarkGate, BenchmarkMeasurement, emit_sample,
    emit_summary,
};
use lix::storage::Storage;
use lix::{Lix, Value, open_lix};

const BENCHMARK: &str = "plugin_api_public_workflows";
const DEFAULT_SAMPLES: usize = 7;
const CSV_MERGE_BASE: &[u8] = b"name,score,color\nalice,1,red\n";

struct PluginFixture {
    key: &'static str,
    extension: &'static str,
    archive: Vec<u8>,
    documents: Vec<Vec<u8>>,
    logical_rows: usize,
}

#[tokio::test]
#[ignore = "matched origin/main versus candidate plugin API benchmark"]
async fn plugin_api_public_workflows() {
    let samples = std::env::var("LIX_PLUGIN_API_BENCH_SAMPLES")
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .expect("sample count must be numeric")
        })
        .unwrap_or(DEFAULT_SAMPLES);
    assert!(samples > 0, "at least one benchmark sample is required");

    for fixture in plugin_fixtures(samples) {
        benchmark_file_roundtrip(&fixture, samples).await;
        benchmark_sparse_file_updates(&fixture, samples).await;
    }
    benchmark_markdown_merge(samples).await;
    benchmark_csv_merge(samples).await;
}

async fn benchmark_sparse_file_updates(fixture: &PluginFixture, samples: usize) {
    let lix = open_lix()
        .await
        .expect("sparse benchmark workspace should open");
    install_plugin(&lix, fixture.key, &fixture.archive).await;
    let path = format!("/sparse.{}", fixture.extension);
    write_file(&lix, &path, &fixture.documents[0]).await;
    let lane = match fixture.key {
        "plugin_csv" => "csv-sparse-file-update",
        "plugin_json" => "json-sparse-file-update",
        "plugin_markdown" => "markdown-sparse-file-update",
        "plugin_text" => "text-sparse-file-update",
        "plugin_excalidraw" => "excalidraw-sparse-file-update",
        key => panic!("unexpected sparse benchmark plugin {key}"),
    };
    let benchmark_fixture = BenchmarkFixture {
        input_bytes: fixture.documents[0].len(),
        logical_rows: fixture.logical_rows,
    };
    let mut measurements = Vec::with_capacity(samples);
    for sample in 0..samples {
        let bytes = sparse_document(fixture.key, sample + 1);
        let allocation_scope = AllocationScope::start();
        let started = Instant::now();
        write_file(&lix, &path, &bytes).await;
        let roundtrip = read_file(&lix, &path).await;
        let elapsed = started.elapsed();
        let allocations = allocation_scope.finish();
        assert_eq!(roundtrip, bytes, "{lane} must preserve exact bytes");
        let measurement = BenchmarkMeasurement::new(elapsed, allocations);
        emit_sample(
            BENCHMARK,
            lane,
            sample,
            benchmark_fixture,
            BenchmarkGate::ElapsedRegression,
            measurement,
        );
        measurements.push(measurement);
    }
    emit_summary(
        BENCHMARK,
        lane,
        benchmark_fixture,
        BenchmarkGate::ElapsedRegression,
        &measurements,
    );
    lix.close().await.expect("sparse workspace should close");
}

async fn benchmark_file_roundtrip(fixture: &PluginFixture, samples: usize) {
    let lix = open_lix().await.expect("benchmark workspace should open");
    install_plugin(&lix, fixture.key, &fixture.archive).await;
    let lane = match fixture.key {
        "plugin_csv" => "csv-file-roundtrip",
        "plugin_json" => "json-file-roundtrip",
        "plugin_markdown" => "markdown-file-roundtrip",
        "plugin_text" => "text-file-roundtrip",
        "plugin_excalidraw" => "excalidraw-file-roundtrip",
        key => panic!("unexpected benchmark plugin {key}"),
    };
    let benchmark_fixture = BenchmarkFixture {
        input_bytes: fixture.documents[0].len(),
        logical_rows: fixture.logical_rows,
    };
    let mut measurements = Vec::with_capacity(samples);

    for (sample, bytes) in fixture.documents.iter().enumerate().take(samples) {
        let path = format!("/{sample}.{}", fixture.extension);
        let allocation_scope = AllocationScope::start();
        let started = Instant::now();
        write_file(&lix, &path, bytes).await;
        let roundtrip = read_file(&lix, &path).await;
        let elapsed = started.elapsed();
        let allocations = allocation_scope.finish();
        assert_eq!(roundtrip, *bytes, "{lane} must preserve exact bytes");

        let measurement = BenchmarkMeasurement::new(elapsed, allocations);
        emit_sample(
            BENCHMARK,
            lane,
            sample,
            benchmark_fixture,
            BenchmarkGate::ElapsedRegression,
            measurement,
        );
        measurements.push(measurement);
    }
    emit_summary(
        BENCHMARK,
        lane,
        benchmark_fixture,
        BenchmarkGate::ElapsedRegression,
        &measurements,
    );
    lix.close().await.expect("benchmark workspace should close");
}

async fn benchmark_markdown_merge(samples: usize) {
    let archive = build_markdown_plugin_archive();
    let lix = open_lix()
        .await
        .expect("Markdown merge workspace should open");
    install_plugin(&lix, "plugin_markdown", &archive).await;
    let lane = "markdown-same-row-text-merge";
    let mut measurements = Vec::with_capacity(samples);
    let stable = "stable prose ".repeat(1_024);
    let base = format!("alpha base {stable}omega base\n").into_bytes();
    let a_bytes = format!("alpha changed by A {stable}omega base\n").into_bytes();
    let b_bytes = format!("alpha base {stable}omega changed by B\n").into_bytes();

    for sample in 0..samples {
        let path = format!("/merge-{sample}.md");
        write_file(&lix, &path, &base).await;
        let peer = lix.open_another_session().await.expect("open merge peer");
        let mut a = lix.begin_transaction().await.expect("open transaction A");
        let mut b = peer.begin_transaction().await.expect("open transaction B");
        stage_file_update(&mut a, &path, &a_bytes).await;
        stage_file_update(&mut b, &path, &b_bytes).await;

        let allocation_scope = AllocationScope::start();
        let started = Instant::now();
        a.commit().await.expect("transaction A should commit");
        b.commit().await.expect("transaction B should merge");
        let merged = read_file(&lix, &path).await;
        let elapsed = started.elapsed();
        let allocations = allocation_scope.finish();
        let merged = String::from_utf8(merged).expect("merged Markdown should be UTF-8");
        assert!(merged.contains("alpha changed by A"));
        assert!(merged.contains("omega changed by B"));

        let fixture = BenchmarkFixture {
            input_bytes: base.len(),
            logical_rows: 1,
        };
        let measurement = BenchmarkMeasurement::new(elapsed, allocations);
        emit_sample(
            BENCHMARK,
            lane,
            sample,
            fixture,
            BenchmarkGate::ElapsedRegression,
            measurement,
        );
        measurements.push(measurement);
        peer.close().await.expect("merge peer should close");
    }
    emit_summary(
        BENCHMARK,
        lane,
        BenchmarkFixture {
            input_bytes: base.len(),
            logical_rows: 1,
        },
        BenchmarkGate::ElapsedRegression,
        &measurements,
    );
    lix.close()
        .await
        .expect("Markdown merge workspace should close");
}

async fn benchmark_csv_merge(samples: usize) {
    let archive = build_csv_plugin_archive();
    let lix = open_lix().await.expect("CSV merge workspace should open");
    install_plugin(&lix, "plugin_csv", &archive).await;
    let lane = "csv-same-row-column-merge";
    let mut measurements = Vec::with_capacity(samples);

    for sample in 0..samples {
        let path = format!("/merge-{sample}.csv");
        let base = CSV_MERGE_BASE;
        write_file(&lix, &path, base).await;
        let peer = lix.open_another_session().await.expect("open merge peer");
        let mut a = lix.begin_transaction().await.expect("open transaction A");
        let mut b = peer.begin_transaction().await.expect("open transaction B");
        stage_file_update(&mut a, &path, b"name,score,color\nalice,10,red\n").await;
        stage_file_update(&mut b, &path, b"name,score,color\nalice,1,blue\n").await;

        let allocation_scope = AllocationScope::start();
        let started = Instant::now();
        a.commit().await.expect("transaction A should commit");
        b.commit().await.expect("transaction B should merge");
        let merged = read_file(&lix, &path).await;
        let elapsed = started.elapsed();
        let allocations = allocation_scope.finish();
        let merged = String::from_utf8(merged).expect("merged CSV should be UTF-8");
        assert!(merged.contains("alice,10,blue"));

        let fixture = BenchmarkFixture {
            input_bytes: base.len(),
            logical_rows: 2,
        };
        let measurement = BenchmarkMeasurement::new(elapsed, allocations);
        emit_sample(
            BENCHMARK,
            lane,
            sample,
            fixture,
            BenchmarkGate::ElapsedRegression,
            measurement,
        );
        measurements.push(measurement);
        peer.close().await.expect("merge peer should close");
    }
    emit_summary(
        BENCHMARK,
        lane,
        BenchmarkFixture {
            input_bytes: CSV_MERGE_BASE.len(),
            logical_rows: 2,
        },
        BenchmarkGate::ElapsedRegression,
        &measurements,
    );
    lix.close().await.expect("CSV merge workspace should close");
}

async fn stage_file_update<StorageImpl>(
    transaction: &mut lix::LixTransaction<StorageImpl>,
    path: &str,
    bytes: &[u8],
) where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    transaction
        .execute(
            "UPDATE lix_file SET content = $1 WHERE path = $2",
            &[
                Value::Blob(bytes.to_vec().into()),
                Value::Text(path.to_owned()),
            ],
        )
        .await
        .expect("file update should stage");
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
    .expect("benchmark plugin should install");
}

async fn write_file<StorageImpl>(lix: &Lix<StorageImpl>, path: &str, bytes: &[u8])
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
         ON CONFLICT (path) DO UPDATE SET content = excluded.content",
        &[
            Value::Text(path.to_owned()),
            Value::Blob(bytes.to_vec().into()),
        ],
    )
    .await
    .expect("benchmark file should write");
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
    .expect("benchmark file should read")
    .rows()[0]
        .get::<Vec<u8>>("content")
        .expect("benchmark file content should be bytes")
}

fn plugin_fixtures(samples: usize) -> Vec<PluginFixture> {
    vec![
        PluginFixture {
            key: "plugin_csv",
            extension: "csv",
            archive: build_csv_plugin_archive(),
            documents: (0..samples).map(csv_document).collect(),
            logical_rows: 257,
        },
        PluginFixture {
            key: "plugin_json",
            extension: "json",
            archive: build_json_plugin_archive(),
            documents: (0..samples).map(json_document).collect(),
            logical_rows: 257,
        },
        PluginFixture {
            key: "plugin_markdown",
            extension: "md",
            archive: build_markdown_plugin_archive(),
            documents: (0..samples).map(markdown_document).collect(),
            logical_rows: 257,
        },
        PluginFixture {
            key: "plugin_text",
            extension: "txt",
            archive: build_text_plugin_archive(),
            documents: (0..samples).map(text_document).collect(),
            logical_rows: 256,
        },
        PluginFixture {
            key: "plugin_excalidraw",
            extension: "excalidraw",
            archive: build_excalidraw_plugin_archive(),
            documents: (0..samples).map(excalidraw_document).collect(),
            logical_rows: 66,
        },
    ]
}

fn csv_document(sample: usize) -> Vec<u8> {
    let mut output = String::from("name,score\n");
    for row in 0..256 {
        output.push_str(&format!("item-{row},{}\n", row + sample));
    }
    output.into_bytes()
}

fn json_document(sample: usize) -> Vec<u8> {
    let values = (0..256)
        .map(|index| {
            (
                format!("item-{index}"),
                serde_json::Value::from(index + sample),
            )
        })
        .collect::<serde_json::Map<String, serde_json::Value>>();
    serde_json::to_vec(&values).expect("JSON fixture should serialize")
}

fn markdown_document(sample: usize) -> Vec<u8> {
    let mut output = String::from("# Plugin API benchmark\n\n");
    for paragraph in 0..256 {
        output.push_str(&format!(
            "Paragraph {paragraph} contains stable prose and sample {sample}.\n\n"
        ));
    }
    output.into_bytes()
}

fn text_document(sample: usize) -> Vec<u8> {
    (0..256)
        .map(|line| format!("line {line} sample {sample}\n"))
        .collect::<String>()
        .into_bytes()
}

fn excalidraw_document(sample: usize) -> Vec<u8> {
    let elements = (0..64)
        .map(|index| {
            serde_json::json!({
                "id": format!("shape-{index}"),
                "type": "rectangle",
                "x": index + sample,
                "y": 2,
                "width": 3,
                "height": 4,
                "isDeleted": false
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_vec(&serde_json::json!({
        "type": "excalidraw",
        "version": 2,
        "source": "plugin-api-benchmark",
        "elements": elements,
        "appState": {},
        "files": {}
    }))
    .expect("Excalidraw fixture should serialize")
}

fn sparse_document(plugin_key: &str, sample: usize) -> Vec<u8> {
    match plugin_key {
        "plugin_csv" => {
            let mut output = String::from("name,score\n");
            for row in 0..256 {
                let score = if row == 128 { sample + 10_000 } else { row };
                output.push_str(&format!("item-{row},{score}\n"));
            }
            output.into_bytes()
        }
        "plugin_json" => {
            let mut value: serde_json::Value =
                serde_json::from_slice(&json_document(0)).expect("base JSON");
            value["item-128"] = serde_json::Value::from(sample + 10_000);
            serde_json::to_vec(&value).expect("sparse JSON")
        }
        "plugin_markdown" => {
            let mut output = String::from("# Plugin API benchmark\n\n");
            for paragraph in 0..256 {
                let marker = if paragraph == 128 { sample } else { 0 };
                output.push_str(&format!(
                    "Paragraph {paragraph} contains stable prose and sample {marker}.\n\n"
                ));
            }
            output.into_bytes()
        }
        "plugin_text" => {
            let mut output = String::new();
            for line in 0..256 {
                let marker = if line == 128 { sample } else { 0 };
                output.push_str(&format!("line {line} sample {marker}\n"));
            }
            output.into_bytes()
        }
        "plugin_excalidraw" => {
            let mut value: serde_json::Value =
                serde_json::from_slice(&excalidraw_document(0)).expect("base Excalidraw");
            value["elements"][32]["x"] = serde_json::Value::from(sample + 10_000);
            serde_json::to_vec(&value).expect("sparse Excalidraw")
        }
        key => panic!("unexpected sparse benchmark plugin {key}"),
    }
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

fn build_json_plugin_archive() -> Vec<u8> {
    build_plugin_archive(
        Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_JSON_plugin_json")),
        include_str!("../../../plugins/json/manifest.json"),
        &[
            (
                "schema/json_root.json",
                include_str!("../../../plugins/json/schema/json_root.json"),
            ),
            (
                "schema/json_object_member.json",
                include_str!("../../../plugins/json/schema/json_object_member.json"),
            ),
            (
                "schema/json_array_item.json",
                include_str!("../../../plugins/json/schema/json_array_item.json"),
            ),
        ],
    )
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

fn build_text_plugin_archive() -> Vec<u8> {
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
    let wasm = fs::read(wasm_path)
        .unwrap_or_else(|error| panic!("read plugin component {}: {error}", wasm_path.display()));
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    writer.start_file("manifest.json", options).unwrap();
    writer.write_all(manifest.as_bytes()).unwrap();
    for (path, schema) in schemas {
        writer.start_file(*path, options).unwrap();
        writer.write_all(schema.as_bytes()).unwrap();
    }
    writer.start_file("plugin.wasm", options).unwrap();
    writer.write_all(&wasm).unwrap();
    writer.finish().unwrap().into_inner()
}
