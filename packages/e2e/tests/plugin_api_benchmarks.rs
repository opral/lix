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
    emit_summary, emit_transition_profile,
};
use lix::storage::Storage;
use lix::{Lix, Value, open_lix};
use sha2::{Digest as _, Sha256};

const BENCHMARK: &str = "plugin_api_public_workflows";
const DEFAULT_SAMPLES: usize = 61;
const DEFAULT_WARMUP_SAMPLES: usize = 5;
const JSON_TEN_MIB_BYTES: usize = 10 * 1024 * 1024;
const JSON_TEN_MIB_PROPERTY_COUNT: usize = 4_096;
const CSV_MERGE_BASE: &[u8] = b"name,score,color\nalice,1,red\n";
const CORPUS_MANIFEST: &str = include_str!("../benchmarks/plugin_api_corpus.json");
const EXPECTED_LANES: [&str; 19] = [
    "csv-file-roundtrip",
    "csv-sparse-file-update",
    "csv-direct-row-mutation",
    "json-file-roundtrip",
    "json-sparse-file-update",
    "json-ten-mib-paged-roundtrip",
    "json-direct-row-mutation",
    "markdown-file-roundtrip",
    "markdown-sparse-file-update",
    "markdown-direct-row-mutation",
    "text-file-roundtrip",
    "text-sparse-file-update",
    "text-direct-row-mutation",
    "text-large-typed-attachment-roundtrip",
    "excalidraw-file-roundtrip",
    "excalidraw-sparse-file-update",
    "excalidraw-direct-row-mutation",
    "markdown-same-row-text-merge",
    "csv-same-row-column-merge",
];

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
    let selected_lane = std::env::var("LIX_PLUGIN_API_BENCH_LANE").ok();
    let samples = configured_samples();
    let warmups = configured_base_warmups();
    assert!(samples > 0, "at least one benchmark sample is required");
    validate_corpus_manifest(samples, warmups);
    if std::env::var_os("LIX_PLUGIN_API_BENCH_SAMPLES").is_none() {
        assert_eq!(
            samples, DEFAULT_SAMPLES,
            "the contract run must use the pinned measured sample count"
        );
    }

    for fixture in plugin_fixtures(samples, selected_lane.as_deref()) {
        benchmark_file_roundtrip(&fixture, samples).await;
        benchmark_sparse_file_updates(&fixture, samples).await;
        benchmark_direct_row_mutation(&fixture, samples).await;
    }
    benchmark_large_typed_attachment(samples).await;
    benchmark_json_ten_mib(samples).await;
    benchmark_markdown_merge(samples).await;
    benchmark_csv_merge(samples).await;
}

async fn benchmark_json_ten_mib(samples: usize) {
    let lane = "json-ten-mib-paged-roundtrip";
    if !lane_selected(lane) {
        return;
    }
    let archive = build_json_plugin_archive();
    let lix = open_lix().await.expect("large JSON workspace should open");
    install_plugin(&lix, "plugin_json", &archive).await;
    let bytes = json_ten_mib_document();
    for warmup in 0..lane_warmups(lane) {
        let path = format!("/large-warmup-{warmup}.json");
        write_file(&lix, &path, &bytes).await;
        assert_eq!(read_file(&lix, &path).await, bytes);
    }
    let mut measurements = Vec::with_capacity(samples);
    for local_sample in 0..samples {
        let sample = measured_sample(local_sample);
        let path = format!("/large-{sample}.json");
        lix.reset_plugin_transition_counters();
        if local_sample == 0 {
            cpu_profile_barrier(lane);
        }
        let allocation_scope = AllocationScope::start();
        let started = Instant::now();
        let parse_started = Instant::now();
        write_file(&lix, &path, &bytes).await;
        let parse_ms = parse_started.elapsed();
        let serialize_started = Instant::now();
        let roundtrip = read_file(&lix, &path).await;
        let serialize_ms = serialize_started.elapsed();
        let elapsed = started.elapsed();
        cpu_profile_end(lane);
        let allocations = allocation_scope.finish();
        let counters = lix.plugin_transition_counters();
        assert_eq!(roundtrip, bytes, "{lane} must preserve exact bytes");
        assert!(
            counters.row_output_pages > 1,
            "{lane} must exercise typed row paging: {counters:?}"
        );
        let fixture = BenchmarkFixture {
            input_bytes: bytes.len(),
            logical_rows: JSON_TEN_MIB_PROPERTY_COUNT + 1,
        };
        let measurement = BenchmarkMeasurement::new(elapsed, allocations);
        emit_transition_profile(
            BENCHMARK,
            lane,
            sample,
            counters,
            serde_json::json!({
                "fixture_sha256": sha256_hex(&bytes),
                "roundtrip_sha256": sha256_hex(&roundtrip),
                "logical_rows": JSON_TEN_MIB_PROPERTY_COUNT + 1,
                "phases_ms": {
                    "parse_or_import": duration_ms(parse_ms),
                    "serialize_or_export": duration_ms(serialize_ms),
                    "total": duration_ms(elapsed),
                },
            }),
        );
        emit_sample(
            BENCHMARK,
            lane,
            sample,
            fixture,
            BenchmarkGate::InstrumentationOnly,
            measurement,
        );
        measurements.push(measurement);
    }
    emit_summary(
        BENCHMARK,
        lane,
        BenchmarkFixture {
            input_bytes: bytes.len(),
            logical_rows: JSON_TEN_MIB_PROPERTY_COUNT + 1,
        },
        BenchmarkGate::InstrumentationOnly,
        &measurements,
    );
    lix.close()
        .await
        .expect("large JSON workspace should close");
}

async fn benchmark_large_typed_attachment(samples: usize) {
    let lane = "text-large-typed-attachment-roundtrip";
    if !lane_selected(lane) {
        return;
    }
    let archive = build_text_plugin_archive();
    let lix = open_lix()
        .await
        .expect("large typed attachment workspace should open");
    install_plugin(&lix, "plugin_text", &archive).await;
    let warmup = large_text_document(0);
    for iteration in 0..lane_warmups(lane) {
        let path = format!("/large-warmup-{iteration}.txt");
        write_file(&lix, &path, &warmup).await;
        assert_eq!(read_file(&lix, &path).await, warmup);
    }
    let mut measurements = Vec::with_capacity(samples);
    for local_sample in 0..samples {
        let sample = measured_sample(local_sample);
        let bytes = large_text_document(sample);
        let path = format!("/large-{sample}.txt");
        lix.reset_plugin_transition_counters();
        if local_sample == 0 {
            cpu_profile_barrier(lane);
        }
        let allocation_scope = AllocationScope::start();
        let started = Instant::now();
        let parse_started = Instant::now();
        write_file(&lix, &path, &bytes).await;
        let parse_ms = parse_started.elapsed();
        let serialize_started = Instant::now();
        let roundtrip = read_file(&lix, &path).await;
        let serialize_ms = serialize_started.elapsed();
        let elapsed = started.elapsed();
        cpu_profile_end(lane);
        let allocations = allocation_scope.finish();
        let counters = lix.plugin_transition_counters();
        assert_eq!(roundtrip, bytes, "{lane} must preserve exact bytes");
        assert!(
            counters.row_output_attachment_writes > 0
                && counters.row_output_attachment_bytes > 64 * 1024,
            "{lane} must exercise typed output attachments: {counters:?}"
        );
        let fixture = BenchmarkFixture {
            input_bytes: bytes.len(),
            logical_rows: 2,
        };
        let measurement = BenchmarkMeasurement::new(elapsed, allocations);
        emit_transition_profile(
            BENCHMARK,
            lane,
            sample,
            counters,
            serde_json::json!({
                "fixture_sha256": sha256_hex(&bytes),
                "roundtrip_sha256": sha256_hex(&roundtrip),
                "logical_rows": 2,
                "phases_ms": {
                    "parse_or_import": duration_ms(parse_ms),
                    "serialize_or_export": duration_ms(serialize_ms),
                    "total": duration_ms(elapsed)
                },
            }),
        );
        emit_sample(
            BENCHMARK,
            lane,
            sample,
            fixture,
            BenchmarkGate::InstrumentationOnly,
            measurement,
        );
        measurements.push(measurement);
    }
    emit_summary(
        BENCHMARK,
        lane,
        BenchmarkFixture {
            input_bytes: warmup.len(),
            logical_rows: 2,
        },
        BenchmarkGate::InstrumentationOnly,
        &measurements,
    );
    lix.close()
        .await
        .expect("large typed attachment workspace should close");
}

async fn benchmark_sparse_file_updates(fixture: &PluginFixture, samples: usize) {
    let lane = match fixture.key {
        "plugin_csv" => "csv-sparse-file-update",
        "plugin_json" => "json-sparse-file-update",
        "plugin_markdown" => "markdown-sparse-file-update",
        "plugin_text" => "text-sparse-file-update",
        "plugin_excalidraw" => "excalidraw-sparse-file-update",
        key => panic!("unexpected sparse benchmark plugin {key}"),
    };
    if !lane_selected(lane) {
        return;
    }
    let lix = open_lix()
        .await
        .expect("sparse benchmark workspace should open");
    install_plugin(&lix, fixture.key, &fixture.archive).await;
    let path = format!("/sparse.{}", fixture.extension);
    write_file(&lix, &path, &fixture.documents[0]).await;
    let benchmark_fixture = BenchmarkFixture {
        input_bytes: fixture.documents[0].len(),
        logical_rows: fixture.logical_rows,
    };
    for iteration in 0..lane_warmups(lane) {
        let bytes = sparse_document(fixture.key, iteration + 1);
        write_file(&lix, &path, &bytes).await;
        assert_eq!(read_file(&lix, &path).await, bytes);
    }
    let reset = sparse_document(fixture.key, 0);
    write_file(&lix, &path, &reset).await;
    assert_eq!(read_file(&lix, &path).await, reset);
    let mut measurements = Vec::with_capacity(samples);
    for local_sample in 0..samples {
        let sample = measured_sample(local_sample);
        let bytes = sparse_document(fixture.key, sample + 1);
        lix.reset_plugin_transition_counters();
        if local_sample == 0 {
            cpu_profile_barrier(lane);
        }
        let allocation_scope = AllocationScope::start();
        let started = Instant::now();
        let update_started = Instant::now();
        write_file(&lix, &path, &bytes).await;
        let update_ms = update_started.elapsed();
        let serialize_started = Instant::now();
        let roundtrip = read_file(&lix, &path).await;
        let serialize_ms = serialize_started.elapsed();
        let elapsed = started.elapsed();
        cpu_profile_end(lane);
        let allocations = allocation_scope.finish();
        let counters = lix.plugin_transition_counters();
        assert_eq!(roundtrip, bytes, "{lane} must preserve exact bytes");
        let measurement = BenchmarkMeasurement::new(elapsed, allocations);
        emit_transition_profile(
            BENCHMARK,
            lane,
            sample,
            counters,
            serde_json::json!({
                "fixture_sha256": sha256_hex(&bytes),
                "roundtrip_sha256": sha256_hex(&roundtrip),
                "logical_rows": fixture.logical_rows,
                "phases_ms": {
                    "incremental_update": duration_ms(update_ms),
                    "serialize_or_export": duration_ms(serialize_ms),
                    "total": duration_ms(elapsed),
                },
            }),
        );
        emit_sample(
            BENCHMARK,
            lane,
            sample,
            BenchmarkFixture {
                input_bytes: bytes.len(),
                logical_rows: fixture.logical_rows,
            },
            BenchmarkGate::InstrumentationOnly,
            measurement,
        );
        measurements.push(measurement);
    }
    emit_summary(
        BENCHMARK,
        lane,
        benchmark_fixture,
        BenchmarkGate::InstrumentationOnly,
        &measurements,
    );
    lix.close().await.expect("sparse workspace should close");
}

async fn benchmark_file_roundtrip(fixture: &PluginFixture, samples: usize) {
    let lane = match fixture.key {
        "plugin_csv" => "csv-file-roundtrip",
        "plugin_json" => "json-file-roundtrip",
        "plugin_markdown" => "markdown-file-roundtrip",
        "plugin_text" => "text-file-roundtrip",
        "plugin_excalidraw" => "excalidraw-file-roundtrip",
        key => panic!("unexpected benchmark plugin {key}"),
    };
    if !lane_selected(lane) {
        return;
    }
    let lix = open_lix().await.expect("benchmark workspace should open");
    install_plugin(&lix, fixture.key, &fixture.archive).await;
    let benchmark_fixture = BenchmarkFixture {
        input_bytes: fixture.documents[0].len(),
        logical_rows: fixture.logical_rows,
    };
    for (sample, bytes) in fixture
        .documents
        .iter()
        .enumerate()
        .take(lane_warmups(lane))
    {
        let path = format!("/warmup-{sample}.{}", fixture.extension);
        write_file(&lix, &path, bytes).await;
        assert_eq!(read_file(&lix, &path).await, *bytes);
    }
    let mut measurements = Vec::with_capacity(samples);

    for local_sample in 0..samples {
        let sample = measured_sample(local_sample);
        let bytes = &fixture.documents[sample];
        let path = format!("/{sample}.{}", fixture.extension);
        lix.reset_plugin_transition_counters();
        let _ = lix::storage_bench::take_root_replay_accounting();
        if local_sample == 0 {
            cpu_profile_barrier(lane);
        }
        let allocation_scope = AllocationScope::start();
        let started = Instant::now();
        let parse_started = Instant::now();
        write_file(&lix, &path, bytes).await;
        let parse_ms = parse_started.elapsed();
        let serialize_started = Instant::now();
        let roundtrip = read_file(&lix, &path).await;
        let serialize_ms = serialize_started.elapsed();
        let elapsed = started.elapsed();
        cpu_profile_end(lane);
        let allocations = allocation_scope.finish();
        let counters = lix.plugin_transition_counters();
        let root_replay = lix::storage_bench::take_root_replay_accounting();
        assert_eq!(roundtrip, *bytes, "{lane} must preserve exact bytes");

        let measurement = BenchmarkMeasurement::new(elapsed, allocations);
        emit_transition_profile(
            BENCHMARK,
            lane,
            sample,
            counters,
            serde_json::json!({
                "fixture_sha256": sha256_hex(&bytes),
                "roundtrip_sha256": sha256_hex(&roundtrip),
                "logical_rows": fixture.logical_rows,
                "root_replay": {
                    "boundaries": root_replay.boundaries,
                    "plans_loaded": root_replay.plans_loaded,
                    "plans_staged": root_replay.plans_staged,
                    "max_plans_in_one_boundary": root_replay.max_plans_in_one_boundary,
                    "plan_load_nanos": root_replay.plan_load_nanos,
                    "stage_nanos": root_replay.stage_nanos,
                    "plans_per_boundary": root_replay.plans_per_boundary,
                },
                "phases_ms": {
                    "parse_or_import": duration_ms(parse_ms),
                    "serialize_or_export": duration_ms(serialize_ms),
                    "total": duration_ms(elapsed),
                },
            }),
        );
        emit_sample(
            BENCHMARK,
            lane,
            sample,
            BenchmarkFixture {
                input_bytes: bytes.len(),
                logical_rows: fixture.logical_rows,
            },
            BenchmarkGate::InstrumentationOnly,
            measurement,
        );
        measurements.push(measurement);
    }
    emit_summary(
        BENCHMARK,
        lane,
        benchmark_fixture,
        BenchmarkGate::InstrumentationOnly,
        &measurements,
    );
    lix.close().await.expect("benchmark workspace should close");
}

async fn benchmark_markdown_merge(samples: usize) {
    let lane = "markdown-same-row-text-merge";
    if !lane_selected(lane) {
        return;
    }
    let archive = build_markdown_plugin_archive();
    let lix = open_lix()
        .await
        .expect("Markdown merge workspace should open");
    install_plugin(&lix, "plugin_markdown", &archive).await;
    let mut measurements = Vec::with_capacity(samples);
    let stable = "stable prose ".repeat(1_024);
    let base = format!("alpha base {stable}omega base\n").into_bytes();
    let a_bytes = format!("alpha changed by A {stable}omega base\n").into_bytes();
    let b_bytes = format!("alpha base {stable}omega changed by B\n").into_bytes();

    let warmups = lane_warmups(lane);
    for iteration in 0..samples + warmups {
        let measured = iteration >= warmups;
        let sample = measured_sample(iteration.saturating_sub(warmups));
        let path = format!("/merge-{iteration}.md");
        write_file(&lix, &path, &base).await;
        let peer = lix.open_another_session().await.expect("open merge peer");
        let mut a = lix.begin_transaction().await.expect("open transaction A");
        let mut b = peer.begin_transaction().await.expect("open transaction B");
        stage_file_update(&mut a, &path, &a_bytes).await;
        stage_file_update(&mut b, &path, &b_bytes).await;

        if measured {
            lix.reset_plugin_transition_counters();
        }
        if measured && iteration == warmups {
            cpu_profile_barrier(lane);
        }
        let allocation_scope = measured.then(AllocationScope::start);
        let started = Instant::now();
        let commit_a_started = Instant::now();
        a.commit().await.expect("transaction A should commit");
        let commit_a_ms = commit_a_started.elapsed();
        let commit_b_started = Instant::now();
        b.commit().await.expect("transaction B should merge");
        let commit_b_ms = commit_b_started.elapsed();
        let serialize_started = Instant::now();
        let merged = read_file(&lix, &path).await;
        let serialize_ms = serialize_started.elapsed();
        let elapsed = started.elapsed();
        if measured {
            cpu_profile_end(lane);
        }
        let allocations = allocation_scope.map(AllocationScope::finish);
        let counters = measured.then(|| lix.plugin_transition_counters());
        let merged = String::from_utf8(merged).expect("merged Markdown should be UTF-8");
        assert!(merged.contains("alpha changed by A"));
        assert!(merged.contains("omega changed by B"));
        peer.close().await.expect("merge peer should close");
        if !measured {
            continue;
        }
        let allocations = allocations.expect("measured merge has an allocation scope");
        let counters = counters.expect("measured merge has transition counters");

        let fixture = BenchmarkFixture {
            input_bytes: base.len(),
            logical_rows: 1,
        };
        let measurement = BenchmarkMeasurement::new(elapsed, allocations);
        emit_transition_profile(
            BENCHMARK,
            lane,
            sample,
            counters,
            serde_json::json!({
                "base_sha256": sha256_hex(&base),
                "merged_sha256": sha256_hex(merged.as_bytes()),
                "logical_rows": 1,
                "phases_ms": {
                    "commit_a": duration_ms(commit_a_ms),
                    "commit_b": duration_ms(commit_b_ms),
                    "serialize_or_export": duration_ms(serialize_ms),
                    "total": duration_ms(elapsed),
                },
            }),
        );
        emit_sample(
            BENCHMARK,
            lane,
            sample,
            fixture,
            BenchmarkGate::InstrumentationOnly,
            measurement,
        );
        measurements.push(measurement);
    }
    emit_summary(
        BENCHMARK,
        lane,
        BenchmarkFixture {
            input_bytes: base.len(),
            logical_rows: 1,
        },
        BenchmarkGate::InstrumentationOnly,
        &measurements,
    );
    lix.close()
        .await
        .expect("Markdown merge workspace should close");
}

async fn benchmark_csv_merge(samples: usize) {
    let lane = "csv-same-row-column-merge";
    if !lane_selected(lane) {
        return;
    }
    let archive = build_csv_plugin_archive();
    let lix = open_lix().await.expect("CSV merge workspace should open");
    install_plugin(&lix, "plugin_csv", &archive).await;
    let mut measurements = Vec::with_capacity(samples);

    let warmups = lane_warmups(lane);
    for iteration in 0..samples + warmups {
        let measured = iteration >= warmups;
        let sample = measured_sample(iteration.saturating_sub(warmups));
        let path = format!("/merge-{iteration}.csv");
        let base = CSV_MERGE_BASE;
        write_file(&lix, &path, base).await;
        let peer = lix.open_another_session().await.expect("open merge peer");
        let mut a = lix.begin_transaction().await.expect("open transaction A");
        let mut b = peer.begin_transaction().await.expect("open transaction B");
        stage_file_update(&mut a, &path, b"name,score,color\nalice,10,red\n").await;
        stage_file_update(&mut b, &path, b"name,score,color\nalice,1,blue\n").await;

        if measured {
            lix.reset_plugin_transition_counters();
        }
        if measured && iteration == warmups {
            cpu_profile_barrier(lane);
        }
        let allocation_scope = measured.then(AllocationScope::start);
        let started = Instant::now();
        let commit_a_started = Instant::now();
        a.commit().await.expect("transaction A should commit");
        let commit_a_ms = commit_a_started.elapsed();
        let commit_b_started = Instant::now();
        b.commit().await.expect("transaction B should merge");
        let commit_b_ms = commit_b_started.elapsed();
        let serialize_started = Instant::now();
        let merged = read_file(&lix, &path).await;
        let serialize_ms = serialize_started.elapsed();
        let elapsed = started.elapsed();
        if measured {
            cpu_profile_end(lane);
        }
        let allocations = allocation_scope.map(AllocationScope::finish);
        let counters = measured.then(|| lix.plugin_transition_counters());
        let merged = String::from_utf8(merged).expect("merged CSV should be UTF-8");
        assert!(merged.contains("alice,10,blue"));
        peer.close().await.expect("merge peer should close");
        if !measured {
            continue;
        }
        let allocations = allocations.expect("measured merge has an allocation scope");
        let counters = counters.expect("measured merge has transition counters");

        let fixture = BenchmarkFixture {
            input_bytes: base.len(),
            logical_rows: 2,
        };
        let measurement = BenchmarkMeasurement::new(elapsed, allocations);
        emit_transition_profile(
            BENCHMARK,
            lane,
            sample,
            counters,
            serde_json::json!({
                "base_sha256": sha256_hex(base),
                "merged_sha256": sha256_hex(merged.as_bytes()),
                "logical_rows": 2,
                "phases_ms": {
                    "commit_a": duration_ms(commit_a_ms),
                    "commit_b": duration_ms(commit_b_ms),
                    "serialize_or_export": duration_ms(serialize_ms),
                    "total": duration_ms(elapsed),
                },
            }),
        );
        emit_sample(
            BENCHMARK,
            lane,
            sample,
            fixture,
            BenchmarkGate::InstrumentationOnly,
            measurement,
        );
        measurements.push(measurement);
    }
    emit_summary(
        BENCHMARK,
        lane,
        BenchmarkFixture {
            input_bytes: CSV_MERGE_BASE.len(),
            logical_rows: 2,
        },
        BenchmarkGate::InstrumentationOnly,
        &measurements,
    );
    lix.close().await.expect("CSV merge workspace should close");
}

async fn benchmark_direct_row_mutation(fixture: &PluginFixture, samples: usize) {
    let lane = match fixture.key {
        "plugin_csv" => "csv-direct-row-mutation",
        "plugin_json" => "json-direct-row-mutation",
        "plugin_markdown" => "markdown-direct-row-mutation",
        "plugin_text" => "text-direct-row-mutation",
        "plugin_excalidraw" => "excalidraw-direct-row-mutation",
        key => panic!("unexpected mutation benchmark plugin {key}"),
    };
    if !lane_selected(lane) {
        return;
    }
    if fixture.key == "plugin_json" {
        benchmark_json_scalar_row_mutation(fixture, samples, lane).await;
        return;
    }
    let lix = open_lix()
        .await
        .expect("row mutation workspace should open");
    install_plugin(&lix, fixture.key, &fixture.archive).await;
    let path = format!("/mutation.{}", fixture.extension);
    write_file(&lix, &path, &fixture.documents[0]).await;
    let file_id = lix
        .execute(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text(path.clone())],
        )
        .await
        .expect("mutation file should resolve")
        .rows()[0]
        .get::<String>("id")
        .expect("file id should be text");
    let markdown_template = if fixture.key == "plugin_markdown" {
        let result = lix
            .execute(
                "SELECT kind, parent_id, payload_json, format_json FROM markdown_node WHERE lixcol_file_id = $1 AND order_key IS NOT NULL ORDER BY order_key LIMIT 1",
                &[Value::Text(file_id.clone())],
            )
            .await
            .expect("Markdown mutation template should query");
        let row = &result.rows()[0];
        Some([
            row.get_index(0).expect("Markdown kind").clone(),
            row.get_index(1).expect("Markdown parent").clone(),
            row.get_index(2).expect("Markdown payload").clone(),
            row.get_index(3).expect("Markdown format").clone(),
        ])
    } else {
        None
    };
    let mut measurements = Vec::with_capacity(samples);
    for local_sample in 0..samples {
        let sample = measured_sample(local_sample);
        let uuid = format!("019c6b89-bb18-77a8-9164-{:012x}", sample + 1);
        let text_id = format!("benchmark-created-{sample}");
        let order_key = format!("eeee{sample:011x}f");
        let (
            create_sql,
            create_params,
            update_sql,
            update_params,
            delete_sql,
            delete_params,
            verify_created_sql,
            verify_created_params,
            verify_updated_sql,
            verify_updated_params,
            verify_deleted_sql,
            verify_deleted_params,
        ) = match fixture.key {
            "plugin_csv" => (
                "INSERT INTO csv_row (order_key, cells, lixcol_file_id) VALUES ($1, $2, $3)",
                vec![
                    Value::Text(order_key.clone()),
                    Value::Jsonb(
                        serde_json::json!(["created", sample.to_string(), "café", "true"]).into(),
                    ),
                    Value::Text(file_id.clone()),
                ],
                "UPDATE csv_row SET cells = $1 WHERE order_key = $2",
                vec![
                    Value::Jsonb(
                        serde_json::json!(["updated", sample.to_string(), "東京", "false"]).into(),
                    ),
                    Value::Text(order_key.clone()),
                ],
                "DELETE FROM csv_row WHERE order_key = $1",
                vec![Value::Text(order_key.clone())],
                "SELECT COUNT(*) AS count FROM csv_row WHERE order_key = $1 AND cells = $2",
                vec![
                    Value::Text(order_key.clone()),
                    Value::Jsonb(
                        serde_json::json!(["created", sample.to_string(), "café", "true"]).into(),
                    ),
                ],
                "SELECT COUNT(*) AS count FROM csv_row WHERE order_key = $1 AND cells = $2",
                vec![
                    Value::Text(order_key.clone()),
                    Value::Jsonb(
                        serde_json::json!(["updated", sample.to_string(), "東京", "false"]).into(),
                    ),
                ],
                "SELECT COUNT(*) AS count FROM csv_row WHERE order_key = $1",
                vec![Value::Text(order_key.clone())],
            ),
            "plugin_json" => (
                "INSERT INTO json_object_member (parent_id, key, order_key, kind, scalar_json, lixcol_file_id) VALUES ('root', $1, $2, 'string', $3, $4)",
                vec![
                    Value::Text(text_id.clone()),
                    Value::Text(format!("eeee{sample:011x}f")),
                    Value::Jsonb(serde_json::json!("created").into()),
                    Value::Text(file_id.clone()),
                ],
                "UPDATE json_object_member SET scalar_json = $1 WHERE parent_id = 'root' AND key = $2",
                vec![
                    Value::Jsonb(serde_json::json!("updated").into()),
                    Value::Text(text_id.clone()),
                ],
                "DELETE FROM json_object_member WHERE parent_id = $1 AND key = $2",
                vec![Value::Text("root".to_owned()), Value::Text(text_id.clone())],
                "SELECT COUNT(*) AS count FROM json_object_member WHERE parent_id = 'root' AND key = $1 AND scalar_json = $2",
                vec![
                    Value::Text(text_id.clone()),
                    Value::Jsonb(serde_json::json!("created").into()),
                ],
                "SELECT COUNT(*) AS count FROM json_object_member WHERE parent_id = 'root' AND key = $1 AND scalar_json = $2",
                vec![
                    Value::Text(text_id.clone()),
                    Value::Jsonb(serde_json::json!("updated").into()),
                ],
                "SELECT COUNT(*) AS count FROM json_object_member WHERE parent_id = 'root' AND key = $1",
                vec![Value::Text(text_id.clone())],
            ),
            "plugin_markdown" => {
                let [kind, parent_id, payload_json, format_json] = markdown_template
                    .as_ref()
                    .expect("Markdown template is loaded")
                    .clone();
                (
                    "INSERT INTO markdown_node (id, kind, order_key, parent_id, payload_json, format_json, lixcol_file_id) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                    vec![
                        Value::Text(uuid.clone()),
                        kind,
                        Value::Text(format!("eeee{sample:011x}f")),
                        parent_id,
                        payload_json,
                        format_json,
                        Value::Text(file_id.clone()),
                    ],
                    "UPDATE markdown_node SET order_key = $1 WHERE id = $2",
                    vec![
                        Value::Text(format!("ffff{sample:011x}f")),
                        Value::Text(uuid.clone()),
                    ],
                    "DELETE FROM markdown_node WHERE id = $1",
                    vec![Value::Text(uuid.clone())],
                    "SELECT COUNT(*) AS count FROM markdown_node WHERE id = $1 AND order_key = $2",
                    vec![
                        Value::Text(uuid.clone()),
                        Value::Text(format!("eeee{sample:011x}f")),
                    ],
                    "SELECT COUNT(*) AS count FROM markdown_node WHERE id = $1 AND order_key = $2",
                    vec![
                        Value::Text(uuid.clone()),
                        Value::Text(format!("ffff{sample:011x}f")),
                    ],
                    "SELECT COUNT(*) AS count FROM markdown_node WHERE id = $1",
                    vec![Value::Text(uuid.clone())],
                )
            }
            "plugin_text" => (
                "INSERT INTO text_line (id, order_key, content_base64, lixcol_file_id) VALUES ($1, $2, $3, $4)",
                vec![
                    Value::Text(uuid.clone()),
                    Value::Text(format!("eeee{sample:011x}f")),
                    Value::Text("Y3JlYXRlZAo".to_owned()),
                    Value::Text(file_id.clone()),
                ],
                "UPDATE text_line SET content_base64 = $1 WHERE id = $2",
                vec![
                    Value::Text("dXBkYXRlZAo".to_owned()),
                    Value::Text(uuid.clone()),
                ],
                "DELETE FROM text_line WHERE id = $1",
                vec![Value::Text(uuid.clone())],
                "SELECT COUNT(*) AS count FROM text_line WHERE id = $1 AND content_base64 = $2",
                vec![
                    Value::Text(uuid.clone()),
                    Value::Text("Y3JlYXRlZAo".to_owned()),
                ],
                "SELECT COUNT(*) AS count FROM text_line WHERE id = $1 AND content_base64 = $2",
                vec![
                    Value::Text(uuid.clone()),
                    Value::Text("dXBkYXRlZAo".to_owned()),
                ],
                "SELECT COUNT(*) AS count FROM text_line WHERE id = $1",
                vec![Value::Text(uuid.clone())],
            ),
            "plugin_excalidraw" => {
                let created = serde_json::json!({"id": text_id, "type": "rectangle", "x": sample, "y": 1, "width": 10, "height": 10, "isDeleted": false});
                let updated = serde_json::json!({"id": text_id, "type": "rectangle", "x": sample, "y": 1, "width": 10, "height": 10, "isDeleted": true});
                (
                    "INSERT INTO excalidraw_element (id, order_key, element_type, is_deleted, leading_json, element_json, lixcol_file_id) VALUES ($1, $2, 'rectangle', false, '', $3, $4)",
                    vec![
                        Value::Text(text_id.clone()),
                        Value::Text(format!("eeee{sample:011x}f")),
                        Value::Jsonb(created.clone().into()),
                        Value::Text(file_id.clone()),
                    ],
                    "UPDATE excalidraw_element SET is_deleted = true, element_json = $1 WHERE id = $2",
                    vec![
                        Value::Jsonb(updated.clone().into()),
                        Value::Text(text_id.clone()),
                    ],
                    "DELETE FROM excalidraw_element WHERE id = $1",
                    vec![Value::Text(text_id.clone())],
                    "SELECT COUNT(*) AS count FROM excalidraw_element WHERE id = $1 AND is_deleted = false AND element_json = $2",
                    vec![Value::Text(text_id.clone()), Value::Jsonb(created.into())],
                    "SELECT COUNT(*) AS count FROM excalidraw_element WHERE id = $1 AND is_deleted = true AND element_json = $2",
                    vec![Value::Text(text_id.clone()), Value::Jsonb(updated.into())],
                    "SELECT COUNT(*) AS count FROM excalidraw_element WHERE id = $1",
                    vec![Value::Text(text_id.clone())],
                )
            }
            _ => unreachable!(),
        };
        lix.reset_plugin_transition_counters();
        if local_sample == 0 {
            cpu_profile_barrier(lane);
        }
        let allocation_scope = AllocationScope::start();
        let started = Instant::now();
        let create_started = Instant::now();
        assert_eq!(
            lix.execute(create_sql, &create_params)
                .await
                .unwrap()
                .rows_affected(),
            1
        );
        let create_ms = create_started.elapsed();
        let generated_identity_started = Instant::now();
        let generated_identity = if fixture.key == "plugin_csv" {
            let id = lix
                .execute(
                    "SELECT id FROM csv_row WHERE order_key = $1",
                    &[Value::Text(order_key.clone())],
                )
                .await
                .expect("generated CSV identity should read")
                .rows()[0]
                .get::<String>("id")
                .expect("generated CSV identity should be text");
            let generated = uuid::Uuid::parse_str(&id).expect("generated CSV identity is UUID");
            assert_eq!(
                generated.get_version_num(),
                7,
                "generated CSV identity must be UUIDv7"
            );
            Some(id)
        } else {
            None
        };
        let generated_identity_ms = generated_identity_started.elapsed();
        let serialize_created_started = Instant::now();
        let created_bytes = read_file(&lix, &path).await;
        assert_count(&lix, verify_created_sql, &verify_created_params, 1).await;
        let serialize_created_ms = serialize_created_started.elapsed();
        let update_started = Instant::now();
        let mut update = lix.begin_transaction().await.unwrap();
        assert_eq!(
            update
                .execute(update_sql, &update_params)
                .await
                .unwrap()
                .rows_affected(),
            1
        );
        update.commit().await.unwrap();
        let update_ms = update_started.elapsed();
        let serialize_updated_started = Instant::now();
        let updated_bytes = read_file(&lix, &path).await;
        assert_count(&lix, verify_updated_sql, &verify_updated_params, 1).await;
        let serialize_updated_ms = serialize_updated_started.elapsed();
        let delete_started = Instant::now();
        let mut delete = lix.begin_transaction().await.unwrap();
        assert_eq!(
            delete
                .execute(delete_sql, &delete_params)
                .await
                .unwrap()
                .rows_affected(),
            1
        );
        delete.commit().await.unwrap();
        let delete_ms = delete_started.elapsed();
        let serialize_deleted_started = Instant::now();
        let final_bytes = read_file(&lix, &path).await;
        assert_count(&lix, verify_deleted_sql, &verify_deleted_params, 0).await;
        let serialize_deleted_ms = serialize_deleted_started.elapsed();
        let elapsed = started.elapsed();
        cpu_profile_end(lane);
        let allocations = allocation_scope.finish();
        let counters = lix.plugin_transition_counters();
        assert_ne!(created_bytes, fixture.documents[0]);
        assert_ne!(updated_bytes, created_bytes);
        if matches!(fixture.key, "plugin_csv" | "plugin_text") {
            assert_eq!(
                final_bytes, fixture.documents[0],
                "lossless row plugins must restore the exact original file"
            );
        } else {
            assert!(
                !final_bytes.is_empty(),
                "structured row mutation must retain a renderable document"
            );
        }
        let fixture_metadata = BenchmarkFixture {
            input_bytes: fixture.documents[0].len(),
            logical_rows: fixture.logical_rows,
        };
        let measurement = BenchmarkMeasurement::new(elapsed, allocations);
        emit_transition_profile(
            BENCHMARK,
            lane,
            sample,
            counters,
            serde_json::json!({
                "created_sha256": sha256_hex(&created_bytes),
                "updated_sha256": sha256_hex(&updated_bytes),
                "final_sha256": sha256_hex(&final_bytes),
                "generated_identity": generated_identity.map(|_| serde_json::json!({
                    "is_uuid": true,
                    "version": 7,
                })),
                "logical_rows": fixture.logical_rows,
                "phases_ms": {
                    "row_create": duration_ms(create_ms),
                    "read_generated_identity": duration_ms(generated_identity_ms),
                    "serialize_after_create": duration_ms(serialize_created_ms),
                    "row_update": duration_ms(update_ms),
                    "serialize_after_update": duration_ms(serialize_updated_ms),
                    "row_delete": duration_ms(delete_ms),
                    "serialize_after_delete": duration_ms(serialize_deleted_ms),
                    "total": duration_ms(elapsed),
                },
            }),
        );
        emit_sample(
            BENCHMARK,
            lane,
            sample,
            fixture_metadata,
            BenchmarkGate::InstrumentationOnly,
            measurement,
        );
        measurements.push(measurement);
    }
    emit_summary(
        BENCHMARK,
        lane,
        BenchmarkFixture {
            input_bytes: fixture.documents[0].len(),
            logical_rows: fixture.logical_rows,
        },
        BenchmarkGate::InstrumentationOnly,
        &measurements,
    );
    lix.close()
        .await
        .expect("row mutation workspace should close");
}

async fn benchmark_json_scalar_row_mutation(
    fixture: &PluginFixture,
    samples: usize,
    lane: &'static str,
) {
    let lix = open_lix()
        .await
        .expect("JSON row mutation workspace should open");
    install_plugin(&lix, fixture.key, &fixture.archive).await;
    let path = "/mutation.json";
    write_file(&lix, path, &fixture.documents[0]).await;
    let mut measurements = Vec::with_capacity(samples);
    for local_sample in 0..samples {
        let sample = measured_sample(local_sample);
        let scalar = serde_json::json!(sample + 10_000);
        lix.reset_plugin_transition_counters();
        if local_sample == 0 {
            cpu_profile_barrier(lane);
        }
        let allocation_scope = AllocationScope::start();
        let started = Instant::now();
        let update_started = Instant::now();
        assert_eq!(
            lix.execute(
                "UPDATE json_object_member SET scalar_json = $1 WHERE parent_id = 'root' AND key = 'item-128'",
                &[Value::Jsonb(scalar.clone().into())],
            )
            .await
            .unwrap()
            .rows_affected(),
            1
        );
        let update_ms = update_started.elapsed();
        let serialize_started = Instant::now();
        let bytes = read_file(&lix, path).await;
        assert_count(
            &lix,
            "SELECT COUNT(*) AS count FROM json_object_member WHERE parent_id = 'root' AND key = 'item-128' AND scalar_json = $1",
            &[Value::Jsonb(scalar.into())],
            1,
        )
        .await;
        let serialize_ms = serialize_started.elapsed();
        let elapsed = started.elapsed();
        cpu_profile_end(lane);
        let allocations = allocation_scope.finish();
        let counters = lix.plugin_transition_counters();
        let measurement = BenchmarkMeasurement::new(elapsed, allocations);
        emit_transition_profile(
            BENCHMARK,
            lane,
            sample,
            counters,
            serde_json::json!({
                "updated_sha256": sha256_hex(&bytes),
                "logical_rows": fixture.logical_rows,
                "phases_ms": {
                    "row_update": duration_ms(update_ms),
                    "serialize_after_update": duration_ms(serialize_ms),
                    "total": duration_ms(elapsed),
                },
            }),
        );
        emit_sample(
            BENCHMARK,
            lane,
            sample,
            BenchmarkFixture {
                input_bytes: fixture.documents[0].len(),
                logical_rows: fixture.logical_rows,
            },
            BenchmarkGate::InstrumentationOnly,
            measurement,
        );
        measurements.push(measurement);
    }
    emit_summary(
        BENCHMARK,
        lane,
        BenchmarkFixture {
            input_bytes: fixture.documents[0].len(),
            logical_rows: fixture.logical_rows,
        },
        BenchmarkGate::InstrumentationOnly,
        &measurements,
    );
    lix.close()
        .await
        .expect("JSON row mutation workspace should close");
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

async fn assert_count<StorageImpl>(
    lix: &Lix<StorageImpl>,
    sql: &str,
    params: &[Value],
    expected: i64,
) where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let count = lix
        .execute(sql, params)
        .await
        .expect("semantic mutation verification should execute")
        .rows()[0]
        .get::<i64>("count")
        .expect("semantic mutation verification should return count");
    assert_eq!(count, expected, "semantic mutation verification failed");
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

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn duration_ms(duration: std::time::Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn validate_corpus_manifest(samples: usize, warmups: usize) {
    let manifest: serde_json::Value =
        serde_json::from_str(CORPUS_MANIFEST).expect("benchmark corpus manifest must be JSON");
    assert_eq!(
        manifest["schema"].as_str(),
        Some("lix.plugin-api-benchmark-corpus.v1")
    );
    assert_eq!(
        manifest["corpus_id"].as_str(),
        Some("plugin-api-public-workflows")
    );
    assert_eq!(manifest["revision"].as_str(), Some("typed-row-fixtures-v3"));
    assert_eq!(
        manifest["seed"].as_str(),
        Some("lix-plugin-api-public-workflows-v1")
    );
    assert_eq!(
        manifest["default_samples"].as_u64(),
        Some(DEFAULT_SAMPLES as u64)
    );
    assert_eq!(
        manifest["warmup_samples"].as_u64(),
        Some(DEFAULT_WARMUP_SAMPLES as u64)
    );
    assert_eq!(warmups, DEFAULT_WARMUP_SAMPLES);
    let lanes = manifest["lanes"]
        .as_array()
        .expect("the pinned corpus lanes must be an array")
        .iter()
        .map(|lane| lane.as_str().expect("lane must be text"))
        .collect::<Vec<_>>();
    assert_eq!(lanes, EXPECTED_LANES);
    for (name, source_rows, import_rows, sparse_row) in [
        ("csv", 256, 258, 128),
        ("json", 256, 417, 128),
        ("markdown", 256, 258, 128),
        ("text", 256, 256, 128),
        ("excalidraw", 64, 66, 32),
    ] {
        let fixture = &manifest["fixtures"][name];
        assert_eq!(fixture["source_rows"].as_u64(), Some(source_rows));
        assert_eq!(fixture["import_rows"].as_u64(), Some(import_rows));
        assert_eq!(fixture["sparse_row"].as_u64(), Some(sparse_row));
    }
    for (name, bytes) in [
        ("csv", csv_document(0)),
        ("json", json_document(0)),
        ("markdown", markdown_document(0)),
        ("text", text_document(0)),
        ("excalidraw", excalidraw_document(0)),
        ("large_typed_attachment", large_text_document(0)),
        ("json_ten_mib", json_ten_mib_document()),
    ] {
        let fixture = &manifest["fixtures"][name];
        assert_eq!(fixture["input_bytes"].as_u64(), Some(bytes.len() as u64));
        assert_eq!(
            fixture["sha256"].as_str(),
            Some(sha256_hex(&bytes).as_str())
        );
    }
    assert_eq!(
        manifest["fixtures"]["json_ten_mib"]["warmups"].as_u64(),
        Some(1)
    );
    assert_eq!(
        manifest["warmup_overrides"]["direct_row_mutation"]["warmups"].as_u64(),
        Some(0)
    );
    for override_name in ["json_ten_mib_paged_roundtrip", "direct_row_mutation"] {
        assert!(
            manifest["warmup_overrides"][override_name]["rationale"]
                .as_str()
                .is_some_and(|value| !value.is_empty()),
            "warmup override {override_name} requires a rationale"
        );
    }
    assert_eq!(
        manifest["workload_contract"]["row_mutation"]
            .as_array()
            .map(Vec::len),
        Some(7)
    );
    let lane_contracts = manifest["lane_contracts"]
        .as_object()
        .expect("corpus lane contracts must be an object");
    assert_eq!(lane_contracts.len(), EXPECTED_LANES.len());
    for lane in EXPECTED_LANES {
        let contract_name = lane_contracts[lane]
            .as_str()
            .unwrap_or_else(|| panic!("lane {lane} must select a phase contract"));
        let phases = manifest["workload_contract"][contract_name]
            .as_array()
            .unwrap_or_else(|| panic!("lane {lane} selected unknown contract {contract_name}"));
        assert!(
            !phases.is_empty(),
            "lane {lane} phase contract must not be empty"
        );
        let unique = phases
            .iter()
            .map(|phase| phase.as_str().expect("phase names must be strings"))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            unique.len(),
            phases.len(),
            "lane {lane} phase names must be unique"
        );
    }
    assert!(
        samples > 0,
        "the corpus validator requires measured samples"
    );
}

fn lane_selected(lane: &str) -> bool {
    std::env::var("LIX_PLUGIN_API_BENCH_LANE")
        .map(|selected| selected == lane)
        .unwrap_or(true)
}

fn configured_base_warmups() -> usize {
    std::env::var("LIX_PLUGIN_API_BENCH_WARMUPS")
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .expect("warmup count must be numeric")
        })
        .unwrap_or(DEFAULT_WARMUP_SAMPLES)
}

fn lane_warmups(lane: &str) -> usize {
    match lane {
        "json-ten-mib-paged-roundtrip" => 1,
        "csv-direct-row-mutation"
        | "json-direct-row-mutation"
        | "markdown-direct-row-mutation"
        | "text-direct-row-mutation"
        | "excalidraw-direct-row-mutation" => 0,
        _ => configured_base_warmups(),
    }
}

fn first_sample_index() -> usize {
    std::env::var("LIX_PLUGIN_API_BENCH_SAMPLE_INDEX")
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .expect("sample index must be numeric")
        })
        .unwrap_or(0)
}

fn measured_sample(local: usize) -> usize {
    first_sample_index().saturating_add(local)
}

fn configured_samples() -> usize {
    std::env::var("LIX_PLUGIN_API_BENCH_SAMPLES")
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .expect("sample count must be numeric")
        })
        .unwrap_or(DEFAULT_SAMPLES)
}

fn cpu_profile_barrier(lane: &str) {
    let Ok(profile_lane) = std::env::var("LIX_PLUGIN_API_PROFILE_LANE") else {
        return;
    };
    if profile_lane != lane {
        return;
    }
    let ready = std::env::var("LIX_PLUGIN_API_PROFILE_READY")
        .expect("profile mode requires a ready marker path");
    let go =
        std::env::var("LIX_PLUGIN_API_PROFILE_GO").expect("profile mode requires a go marker path");
    fs::write(&ready, lane).expect("profile ready marker should write");
    let deadline = Instant::now() + std::time::Duration::from_secs(30);
    while !Path::new(&go).is_file() {
        assert!(
            Instant::now() < deadline,
            "profiler did not release measured scope"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

fn cpu_profile_end(lane: &str) {
    let Ok(profile_lane) = std::env::var("LIX_PLUGIN_API_PROFILE_LANE") else {
        return;
    };
    if profile_lane != lane {
        return;
    }
    static MEASURED_ITERATIONS: std::sync::atomic::AtomicUsize =
        std::sync::atomic::AtomicUsize::new(0);
    let expected_iterations = configured_samples();
    let completed_iterations =
        MEASURED_ITERATIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    if completed_iterations < expected_iterations {
        return;
    }
    assert_eq!(
        completed_iterations, expected_iterations,
        "profile end barrier observed more measured iterations than configured"
    );
    let done = std::env::var("LIX_PLUGIN_API_PROFILE_DONE")
        .expect("profile mode requires a done marker path");
    let release = std::env::var("LIX_PLUGIN_API_PROFILE_RELEASE")
        .expect("profile mode requires a release marker path");
    fs::write(&done, lane).expect("profile done marker should write");
    let deadline = Instant::now() + std::time::Duration::from_secs(30);
    while !Path::new(&release).is_file() {
        assert!(
            Instant::now() < deadline,
            "profiler did not release teardown"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

fn plugin_fixtures(samples: usize, selected_lane: Option<&str>) -> Vec<PluginFixture> {
    let fixture_samples = first_sample_index()
        .saturating_add(samples)
        .max(configured_base_warmups());
    let selected_plugin = selected_lane.and_then(|lane| lane.split_once('-').map(|(name, _)| name));
    let mut fixtures = Vec::new();
    if selected_plugin.is_none_or(|plugin| plugin == "csv") {
        fixtures.push(PluginFixture {
            key: "plugin_csv",
            extension: "csv",
            archive: build_csv_plugin_archive(),
            documents: (0..fixture_samples).map(csv_document).collect(),
            logical_rows: 258,
        });
    }
    if selected_plugin.is_none_or(|plugin| plugin == "json") {
        fixtures.push(PluginFixture {
            key: "plugin_json",
            extension: "json",
            archive: build_json_plugin_archive(),
            documents: (0..fixture_samples).map(json_document).collect(),
            logical_rows: 417,
        });
    }
    if selected_plugin.is_none_or(|plugin| plugin == "markdown") {
        fixtures.push(PluginFixture {
            key: "plugin_markdown",
            extension: "md",
            archive: build_markdown_plugin_archive(),
            documents: (0..fixture_samples).map(markdown_document).collect(),
            logical_rows: 258,
        });
    }
    if selected_plugin.is_none_or(|plugin| plugin == "text") {
        fixtures.push(PluginFixture {
            key: "plugin_text",
            extension: "txt",
            archive: build_text_plugin_archive(),
            documents: (0..fixture_samples).map(text_document).collect(),
            logical_rows: 256,
        });
    }
    if selected_plugin.is_none_or(|plugin| plugin == "excalidraw") {
        fixtures.push(PluginFixture {
            key: "plugin_excalidraw",
            extension: "excalidraw",
            archive: build_excalidraw_plugin_archive(),
            documents: (0..fixture_samples).map(excalidraw_document).collect(),
            logical_rows: 66,
        });
    }
    fixtures
}

fn csv_document(sample: usize) -> Vec<u8> {
    let mut output = String::from("name,score,notes,enabled\r\n");
    for row in 0..256 {
        let enabled = row % 3 != 0;
        if row % 16 == 0 {
            output.push_str(&format!(
                "\"item-{row}, quoted\",{},\"line one {sample}\nline two \"\"quoted\"\"\",{enabled}\r\n",
                row + sample
            ));
        } else {
            output.push_str(&format!(
                "item-{row},{},\"unicode café 東京 row {row}\",{enabled}\r\n",
                row + sample
            ));
        }
    }
    output.into_bytes()
}

fn json_document(sample: usize) -> Vec<u8> {
    let values = (0..256)
        .map(|index| {
            let value = match index % 8 {
                0 => serde_json::json!(index + sample),
                1 => serde_json::json!(format!("café 東京 {index} sample {sample}")),
                2 => serde_json::json!(index % 2 == 0),
                3 => serde_json::Value::Null,
                4 => serde_json::json!([index + sample, "nested", true]),
                5 => serde_json::json!({"count": index + sample, "enabled": index % 2 == 0}),
                6 => serde_json::json!((index as f64 + sample as f64) / 10.0),
                _ => serde_json::json!(format!("escaped \\n \\t \\\" {index}")),
            };
            (format!("item-{index}"), value)
        })
        .collect::<serde_json::Map<String, serde_json::Value>>();
    serde_json::to_vec(&values).expect("JSON fixture should serialize")
}

fn markdown_document(sample: usize) -> Vec<u8> {
    let mut output = String::from("# Plugin API benchmark — café 東京\n\n");
    for paragraph in 0..256 {
        output.push_str(&format!(
            "Paragraph {paragraph} has **strong text**, _emphasis_, `code-{paragraph}`, "
        ));
        output.push_str(&format!(
            "[a link](https://example.com/{paragraph}) and sample {sample}.  \n"
        ));
        output.push_str("A deliberate hard break keeps the document structurally rich.\n\n");
    }
    output.into_bytes()
}

fn text_document(sample: usize) -> Vec<u8> {
    (0..256)
        .map(|line| {
            if line % 32 == 0 {
                format!("line {line} sample {sample} café 東京 tail\r\n")
            } else {
                format!("line {line} sample {sample} — unicode\n")
            }
        })
        .collect::<String>()
        .into_bytes()
}

fn large_text_document(sample: usize) -> Vec<u8> {
    format!(
        "large line sample {sample} {}\nsmall line\n",
        "café東京0123456789".repeat(16_384)
    )
    .into_bytes()
}

fn json_ten_mib_document() -> Vec<u8> {
    const BASE_MEMBER_BYTES: usize = 44;
    let base_bytes =
        2 + JSON_TEN_MIB_PROPERTY_COUNT * BASE_MEMBER_BYTES + JSON_TEN_MIB_PROPERTY_COUNT - 1;
    let padding = JSON_TEN_MIB_BYTES - base_bytes;
    let padding_per_property = padding / JSON_TEN_MIB_PROPERTY_COUNT;
    let extra_padding_properties = padding % JSON_TEN_MIB_PROPERTY_COUNT;
    let mut bytes = Vec::with_capacity(JSON_TEN_MIB_BYTES);
    let mut state = 0x6a73_6f6e_2d31_306du64;
    bytes.push(b'{');
    for index in 0..JSON_TEN_MIB_PROPERTY_COUNT {
        if index > 0 {
            bytes.push(b',');
        }
        state = splitmix64(state);
        let first = state;
        state = splitmix64(state);
        let second = state as u32;
        write!(
            &mut bytes,
            "\"property_{index:06}\":\"{first:016x}{second:08x}"
        )
        .expect("write deterministic JSON property");
        let property_padding = padding_per_property + usize::from(index < extra_padding_properties);
        bytes.extend(std::iter::repeat_n(b'f', property_padding));
        bytes.push(b'"');
    }
    bytes.push(b'}');
    assert_eq!(bytes.len(), JSON_TEN_MIB_BYTES);
    bytes
}

fn splitmix64(mut state: u64) -> u64 {
    state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut value = state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
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
                "isDeleted": false,
                "strokeColor": if index % 2 == 0 { "#1e1e1e" } else { "#e03131" },
                "backgroundColor": "transparent",
                "seed": index * 7919,
                "version": 1,
                "customData": {"label": format!("café 東京 {index}"), "nullable": null}
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_vec(&serde_json::json!({
        "type": "excalidraw",
        "version": 2,
        "source": "plugin-api-benchmark",
        "elements": elements,
        "appState": {
            "viewBackgroundColor": "#ffffff",
            "gridSize": 20,
            "zoom": {"value": 1.0},
            "selectedElementIds": {"shape-0": true}
        },
        "files": {
            "asset-0": {
                "mimeType": "image/svg+xml",
                "id": "asset-0",
                "dataURL": "data:image/svg+xml;base64,PHN2Zy8+",
                "created": 1_700_000_000_000_i64,
                "lastRetrieved": 1_700_000_000_000_i64
            }
        }
    }))
    .expect("Excalidraw fixture should serialize")
}

fn sparse_document(plugin_key: &str, sample: usize) -> Vec<u8> {
    match plugin_key {
        "plugin_csv" => {
            let mut value = String::from_utf8(csv_document(0)).expect("base CSV is UTF-8");
            value = value.replacen(
                "\"item-128, quoted\",128,\"line one 0\nline two \"\"quoted\"\"\",true",
                &format!(
                    "\"item-128, quoted\",{},\"line one 0\nline two \"\"quoted\"\"\",true",
                    sample + 10_000
                ),
                1,
            );
            value.into_bytes()
        }
        "plugin_json" => {
            let mut value: serde_json::Value =
                serde_json::from_slice(&json_document(0)).expect("base JSON");
            value["item-128"] = serde_json::Value::from(sample + 10_000);
            serde_json::to_vec(&value).expect("sparse JSON")
        }
        "plugin_markdown" => {
            let mut value = String::from_utf8(markdown_document(0)).expect("base Markdown");
            value = value.replacen(
                "[a link](https://example.com/128) and sample 0.",
                &format!("[a link](https://example.com/128) and sample {sample}."),
                1,
            );
            value.into_bytes()
        }
        "plugin_text" => {
            let mut value = String::from_utf8(text_document(0)).expect("base text");
            value = value.replacen(
                "line 128 sample 0 café 東京 tail",
                &format!("line 128 sample {sample} café 東京 tail"),
                1,
            );
            value.into_bytes()
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
