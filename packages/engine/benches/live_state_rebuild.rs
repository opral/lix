use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use lix_engine::{
    CreateVersionOptions, ExecuteOptions, LiveStateRebuildPlan, LiveStateRebuildRequest,
    LiveStateRebuildScope, Lix, Value,
};
use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::runtime::Runtime;

#[path = "support/mod.rs"]
mod support;

use support::blob_fixture::{boot_new_file_backed_lix, open_existing_file_backed_lix, temp_db};
use support::state_fixture::{
    register_bench_state_schema, BENCH_STATE_SCHEMA_KEY, BENCH_STATE_SCHEMA_VERSION,
};
use support::verify::{scalar_count, scalar_text};

const VERSION_COUNT: usize = 32;
const ROWS_PER_VERSION: usize = 128;
const INSERT_CHUNK_SIZE: usize = 128;
const DELTA_VERSION_COUNT: usize = 4;
const DELTA_ROWS_PER_VERSION: usize = 16;
const MAIN_VALUE_PREFIX: &str = "main";
const PLAN_STAGE_NAMES: &[&str] = &[
    "visibility_projection_rows",
    "version_ref_heads",
    "canonical_visible_state",
    "latest_visible_state",
    "state_materializer",
];

fn bench_live_state_rebuild(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to create tokio runtime");
    let base_template = build_base_template(&runtime);
    let delta_template = build_delta_template(&runtime, &base_template);

    {
        let mut group = c.benchmark_group("live_state_rebuild_plan_full_cold");
        group.sample_size(10);
        group.throughput(Throughput::Elements(
            base_template.expected_plan_writes as u64,
        ));
        group.bench_function("versions_32_rows_128", |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let fixture = open_fixture_from_template(
                        &runtime,
                        &base_template.db_path,
                        "live-state-rebuild-plan.sqlite",
                    );
                    let started = Instant::now();
                    let plan = runtime
                        .block_on(fixture.lix.live_state_rebuild_plan(&full_rebuild_request()))
                        .expect("full live_state rebuild plan should succeed");
                    let elapsed = started.elapsed();

                    assert_eq!(
                        plan.scope,
                        LiveStateRebuildScope::Full,
                        "full plan scope should remain full"
                    );
                    assert_eq!(
                        plan.writes.len(),
                        base_template.expected_plan_writes,
                        "full plan write count should remain stable"
                    );
                    assert!(
                        plan.warnings.is_empty(),
                        "full plan should not emit warnings for bench fixture"
                    );
                    assert!(
                        plan.debug.is_none(),
                        "full plan debug trace should stay disabled in benchmark mode"
                    );
                    assert_expected_stage_names(&plan);

                    total += elapsed;
                }
                total
            });
        });
        group.finish();
    }

    {
        let mut group = c.benchmark_group("live_state_rebuild_apply_full_cold");
        group.sample_size(10);
        group.throughput(Throughput::Elements(
            base_template.expected_plan_writes as u64,
        ));
        group.bench_function("versions_32_rows_128", |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let fixture = open_fixture_from_template(
                        &runtime,
                        &base_template.db_path,
                        "live-state-rebuild-apply.sqlite",
                    );
                    let started = Instant::now();
                    let report = runtime
                        .block_on(
                            fixture
                                .lix
                                .apply_live_state_rebuild_plan(&base_template.full_plan),
                        )
                        .expect("full live_state rebuild apply should succeed");
                    let elapsed = started.elapsed();

                    assert_eq!(
                        report.rows_written, base_template.expected_plan_writes,
                        "full apply should write every planned row"
                    );
                    assert!(
                        report.rows_deleted > 0,
                        "full apply should clear existing live rows before rewriting"
                    );
                    assert_eq!(
                        bench_live_row_count(
                            &runtime,
                            &fixture.lix,
                            &base_template.sentinel_version_id,
                        ),
                        ROWS_PER_VERSION as i64,
                        "full apply should preserve all bench rows for the sentinel version"
                    );
                    assert_eq!(
                        bench_live_value(
                            &runtime,
                            &fixture.lix,
                            &base_template.sentinel_version_id,
                            "entity-00000",
                        ),
                        base_template.sentinel_expected_value,
                        "full apply should preserve sentinel live value"
                    );

                    total += elapsed;
                }
                total
            });
        });
        group.finish();
    }

    {
        let mut group = c.benchmark_group("live_state_rebuild_delta_versions_warm");
        group.sample_size(10);
        group.throughput(Throughput::Elements(
            delta_template.expected_plan_writes as u64,
        ));
        group.bench_function("delta_4_versions_rows_16", |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let fixture = open_fixture_from_template(
                        &runtime,
                        &delta_template.db_path,
                        "live-state-rebuild-delta.sqlite",
                    );
                    let request =
                        version_scoped_rebuild_request(delta_template.request_scope.clone());
                    let started = Instant::now();
                    let plan = runtime
                        .block_on(fixture.lix.live_state_rebuild_plan(&request))
                        .expect("delta live_state rebuild plan should succeed");
                    let report = runtime
                        .block_on(fixture.lix.apply_live_state_rebuild_plan(&plan))
                        .expect("delta live_state rebuild apply should succeed");
                    let elapsed = started.elapsed();

                    assert_eq!(
                        plan.scope,
                        LiveStateRebuildScope::Versions(delta_template.expected_scope.clone()),
                        "delta plan should stay scoped to the requested versions"
                    );
                    assert_eq!(
                        plan.writes.len(),
                        delta_template.expected_plan_writes,
                        "delta plan write count should remain stable"
                    );
                    assert!(
                        plan.warnings.is_empty(),
                        "delta plan should not emit warnings for bench fixture"
                    );
                    assert_eq!(
                        report.rows_written, delta_template.expected_plan_writes,
                        "delta apply should write every planned row"
                    );
                    assert!(
                        report.rows_deleted > 0,
                        "delta apply should replace existing live rows for changed versions"
                    );
                    assert_eq!(
                        bench_live_value(
                            &runtime,
                            &fixture.lix,
                            &delta_template.sentinel_version_id,
                            "entity-00000",
                        ),
                        delta_template.sentinel_expected_value,
                        "delta rebuild should refresh the changed sentinel version"
                    );
                    assert_eq!(
                        bench_live_value(
                            &runtime,
                            &fixture.lix,
                            &delta_template.unchanged_version_id,
                            "entity-00000",
                        ),
                        delta_template.unchanged_expected_value,
                        "delta rebuild should leave untouched versions stable"
                    );

                    total += elapsed;
                }
                total
            });
        });
        group.finish();
    }
}

struct BaseTemplate {
    db_path: PathBuf,
    full_plan: LiveStateRebuildPlan,
    expected_plan_writes: usize,
    main_version_id: String,
    version_ids: Vec<String>,
    sentinel_version_id: String,
    sentinel_expected_value: String,
    _tempdir: TempDir,
}

struct DeltaTemplate {
    db_path: PathBuf,
    request_scope: BTreeSet<String>,
    expected_scope: BTreeSet<String>,
    expected_plan_writes: usize,
    sentinel_version_id: String,
    sentinel_expected_value: String,
    unchanged_version_id: String,
    unchanged_expected_value: String,
    _tempdir: TempDir,
}

struct BenchFixture {
    lix: Arc<Lix>,
    _tempdir: TempDir,
}

fn build_base_template(runtime: &Runtime) -> BaseTemplate {
    let (tempdir, db_path) = temp_db("live-state-rebuild-base.sqlite");
    let lix = boot_new_file_backed_lix(runtime, &db_path, None, true);
    register_bench_state_schema(runtime, &lix);

    let main_version_id = runtime
        .block_on(lix.active_version_id())
        .expect("main version id should load");
    let main_inserts = build_bench_state_insert_sql_batches_for_version(
        &main_version_id,
        ROWS_PER_VERSION,
        INSERT_CHUNK_SIZE,
        MAIN_VALUE_PREFIX,
    );
    execute_sql_batches(runtime, &lix, &main_inserts);

    let mut version_ids = Vec::with_capacity(VERSION_COUNT);
    for version_index in 0..VERSION_COUNT {
        let version_id = format!("live-state-version-{version_index:03}");
        runtime
            .block_on(lix.create_version(CreateVersionOptions {
                id: Some(version_id.clone()),
                ..Default::default()
            }))
            .expect("bench branch version should be created");
        runtime
            .block_on(lix.switch_version(version_id.clone()))
            .expect("bench branch switch should succeed");

        let value_prefix = branch_value_prefix(version_index);
        let inserts = build_bench_state_insert_sql_batches_for_version(
            &version_id,
            ROWS_PER_VERSION,
            INSERT_CHUNK_SIZE,
            &value_prefix,
        );
        execute_sql_batches(runtime, &lix, &inserts);

        version_ids.push(version_id.clone());
        runtime
            .block_on(lix.switch_version(main_version_id.clone()))
            .expect("switch back to main should succeed");
    }

    runtime
        .block_on(lix.switch_version(main_version_id.clone()))
        .expect("final switch back to main should succeed");

    let full_plan = runtime
        .block_on(lix.live_state_rebuild_plan(&full_rebuild_request()))
        .expect("full live_state rebuild plan should build for template");
    assert!(
        full_plan.warnings.is_empty(),
        "template full plan should not emit warnings"
    );
    let seed_report = runtime
        .block_on(lix.apply_live_state_rebuild_plan(&full_plan))
        .expect("template live_state rebuild apply should seed live tables");
    assert_eq!(
        seed_report.rows_written,
        full_plan.writes.len(),
        "template live-state seed should write every planned row"
    );

    let sentinel_version_id = version_ids
        .last()
        .expect("base template should create at least one version")
        .clone();
    let sentinel_expected_value = expected_value_text(&branch_value_prefix(VERSION_COUNT - 1), 0);

    BaseTemplate {
        db_path,
        expected_plan_writes: full_plan.writes.len(),
        full_plan,
        main_version_id,
        version_ids,
        sentinel_version_id,
        sentinel_expected_value,
        _tempdir: tempdir,
    }
}

fn build_delta_template(runtime: &Runtime, base_template: &BaseTemplate) -> DeltaTemplate {
    let (tempdir, db_path) = temp_db("live-state-rebuild-delta-template.sqlite");
    fs::copy(&base_template.db_path, &db_path).expect("delta template db copy should succeed");
    let lix = open_existing_file_backed_lix(runtime, &db_path, None);

    let changed_versions = base_template
        .version_ids
        .iter()
        .rev()
        .take(DELTA_VERSION_COUNT)
        .cloned()
        .collect::<Vec<_>>();
    for (ordinal, version_id) in changed_versions.iter().enumerate() {
        runtime
            .block_on(lix.switch_version(version_id.clone()))
            .expect("delta template version switch should succeed");
        let updates = build_bench_state_update_sql_batches_for_version(
            version_id,
            0,
            DELTA_ROWS_PER_VERSION,
            &delta_value_prefix(ordinal),
        );
        execute_sql_batches(runtime, &lix, &updates);
    }
    runtime
        .block_on(lix.switch_version(base_template.main_version_id.clone()))
        .expect("switch back to main after delta seeding should succeed");

    let request_scope = changed_versions.iter().cloned().collect::<BTreeSet<_>>();
    let delta_plan = runtime
        .block_on(
            lix.live_state_rebuild_plan(&version_scoped_rebuild_request(request_scope.clone())),
        )
        .expect("delta live_state rebuild plan should build for template");
    assert!(
        delta_plan.warnings.is_empty(),
        "delta template plan should not emit warnings"
    );
    let expected_scope = match &delta_plan.scope {
        LiveStateRebuildScope::Versions(versions) => versions.clone(),
        LiveStateRebuildScope::Full => {
            panic!("delta template should not resolve a version-scoped request to full")
        }
    };

    DeltaTemplate {
        db_path,
        request_scope,
        expected_scope,
        expected_plan_writes: delta_plan.writes.len(),
        sentinel_version_id: changed_versions[0].clone(),
        sentinel_expected_value: expected_value_text(&delta_value_prefix(0), 0),
        unchanged_version_id: base_template.version_ids[0].clone(),
        unchanged_expected_value: expected_value_text(&branch_value_prefix(0), 0),
        _tempdir: tempdir,
    }
}

fn open_fixture_from_template(
    runtime: &Runtime,
    template_path: &PathBuf,
    filename: &str,
) -> BenchFixture {
    let (tempdir, db_path) = temp_db(filename);
    fs::copy(template_path, &db_path).expect("fixture db copy should succeed");
    let lix = open_existing_file_backed_lix(runtime, &db_path, None);
    BenchFixture {
        lix,
        _tempdir: tempdir,
    }
}

fn execute_sql_batches(runtime: &Runtime, lix: &Arc<Lix>, sql_batches: &[String]) {
    let mut transaction = runtime
        .block_on(lix.begin_transaction_with_options(ExecuteOptions::default()))
        .expect("bench transaction should start");
    for sql in sql_batches {
        runtime
            .block_on(transaction.execute(sql, &[]))
            .expect("bench sql batch should succeed");
    }
    runtime
        .block_on(transaction.commit())
        .expect("bench transaction should commit");
}

fn build_bench_state_insert_sql_batches_for_version(
    version_id: &str,
    row_count: usize,
    chunk_size: usize,
    value_prefix: &str,
) -> Vec<String> {
    assert!(chunk_size > 0, "chunk size must be greater than 0");

    let mut entries = Vec::with_capacity(row_count);
    for index in 0..row_count {
        let entity_id = format!("entity-{index:05}");
        let snapshot = expected_snapshot_text(value_prefix, index);
        entries.push(format!(
            "('{}', NULL, '{}', '{}', NULL, '{}', '{}')",
            escape_sql_string(&entity_id),
            BENCH_STATE_SCHEMA_KEY,
            escape_sql_string(version_id),
            BENCH_STATE_SCHEMA_VERSION,
            escape_sql_string(&snapshot),
        ));
    }

    let mut statements = Vec::new();
    for chunk in entries.chunks(chunk_size) {
        statements.push(format!(
            "INSERT INTO lix_state_by_version (\
             entity_id, file_id, schema_key, version_id, plugin_key, schema_version, snapshot_content\
             ) VALUES {}",
            chunk.join(", ")
        ));
    }
    statements
}

fn build_bench_state_update_sql_batches_for_version(
    version_id: &str,
    start_index: usize,
    changed_count: usize,
    value_prefix: &str,
) -> Vec<String> {
    let mut statements = Vec::with_capacity(changed_count);
    for index in start_index..(start_index + changed_count) {
        let entity_id = format!("entity-{index:05}");
        let snapshot = expected_snapshot_text(value_prefix, index);
        statements.push(format!(
            "UPDATE lix_state_by_version \
             SET snapshot_content = '{}' \
             WHERE schema_key = '{}' \
               AND entity_id = '{}' \
               AND file_id IS NULL \
               AND version_id = '{}'",
            escape_sql_string(&snapshot),
            BENCH_STATE_SCHEMA_KEY,
            escape_sql_string(&entity_id),
            escape_sql_string(version_id),
        ));
    }
    statements
}

fn bench_live_row_count(runtime: &Runtime, lix: &Arc<Lix>, version_id: &str) -> i64 {
    scalar_count(
        runtime,
        lix,
        "SELECT COUNT(*) \
         FROM lix_internal_live_v1_bench_state_schema \
         WHERE schema_key = $1 \
           AND version_id = $2 \
           AND entity_id LIKE 'entity-%' \
           AND is_tombstone = 0 \
           AND untracked = false",
        &[
            Value::Text(BENCH_STATE_SCHEMA_KEY.to_string()),
            Value::Text(version_id.to_string()),
        ],
    )
}

fn bench_live_value(
    runtime: &Runtime,
    lix: &Arc<Lix>,
    version_id: &str,
    entity_id: &str,
) -> String {
    scalar_text(
        runtime,
        lix,
        "SELECT value \
         FROM lix_internal_live_v1_bench_state_schema \
         WHERE schema_key = $1 \
           AND version_id = $2 \
           AND entity_id = $3 \
           AND is_tombstone = 0 \
           AND untracked = false \
         LIMIT 1",
        &[
            Value::Text(BENCH_STATE_SCHEMA_KEY.to_string()),
            Value::Text(version_id.to_string()),
            Value::Text(entity_id.to_string()),
        ],
    )
}

fn assert_expected_stage_names(plan: &LiveStateRebuildPlan) {
    let stage_names = plan
        .stats
        .iter()
        .map(|stat| stat.stage.as_str())
        .collect::<BTreeSet<_>>();
    for expected_stage in PLAN_STAGE_NAMES {
        assert!(
            stage_names.contains(expected_stage),
            "missing expected plan stage '{expected_stage}'"
        );
    }
}

fn full_rebuild_request() -> LiveStateRebuildRequest {
    LiveStateRebuildRequest {
        scope: LiveStateRebuildScope::Full,
        debug: lix_engine::LiveStateRebuildDebugMode::Off,
        debug_row_limit: 0,
    }
}

fn version_scoped_rebuild_request(scope: BTreeSet<String>) -> LiveStateRebuildRequest {
    LiveStateRebuildRequest {
        scope: LiveStateRebuildScope::Versions(scope),
        debug: lix_engine::LiveStateRebuildDebugMode::Off,
        debug_row_limit: 0,
    }
}

fn branch_value_prefix(index: usize) -> String {
    format!("branch-{index:03}")
}

fn delta_value_prefix(index: usize) -> String {
    format!("delta-{index:02}")
}

fn expected_value_text(prefix: &str, entity_index: usize) -> String {
    format!("{prefix}-{entity_index:05}")
}

fn expected_snapshot_text(prefix: &str, entity_index: usize) -> String {
    format!(
        "{{\"value\":\"{}\"}}",
        expected_value_text(prefix, entity_index)
    )
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

criterion_group!(benches, bench_live_state_rebuild);
criterion_main!(benches);
