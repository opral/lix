use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use lix_engine::{boot, BootArgs, ExecuteOptions, LixError, Value};
use serde_json::json;
use std::hint::black_box;
use std::time::Duration;
use tokio::runtime::Runtime;

mod support;
use support::sqlite_backend::BenchSqliteBackend;

const SCHEMA_KEY: &str = "bench_state_history_schema";
const FILE_ID: &str = "bench-state-history-file";
const PLUGIN_KEY: &str = "lix";
const TARGET_ENTITY_INDEX: usize = 3;
const PHASE0_ALL_ENTITIES_DEPTH: i64 = 10;

const DEFAULT_SEED_CONFIG: HistorySeedConfig = HistorySeedConfig {
    entity_count: 8,
    history_updates: 48,
    branch_roots: 1,
};

const BRANCHY_SEED_CONFIG: HistorySeedConfig = HistorySeedConfig {
    entity_count: 24,
    history_updates: 160,
    branch_roots: 8,
};

const PLUGIN_RUNTIME_HISTORY_QUERY: &str = "WITH target_commit_depth AS (\
    SELECT COALESCE((\
      SELECT depth \
      FROM lix_internal_commit_ancestry \
      WHERE commit_id = $3 \
        AND ancestor_id = $4 \
      LIMIT 1\
    ), $5) AS raw_depth\
 ) \
 SELECT entity_id, schema_key, schema_version, snapshot_content, depth \
 FROM lix_state_history \
 WHERE file_id = $1 \
   AND plugin_key = $2 \
   AND root_commit_id = $3 \
   AND depth >= (SELECT raw_depth FROM target_commit_depth) \
 ORDER BY entity_id ASC, depth ASC";

const HISTORY_COUNT_BY_ROOT_QUERY: &str = "SELECT COUNT(*) \
    FROM lix_state_history \
    WHERE schema_key = ? \
      AND root_commit_id = ? \
      AND snapshot_content IS NOT NULL";

const HISTORY_ENTITY_TIMELINE_SCAN_QUERY: &str = "SELECT depth, snapshot_content \
    FROM lix_state_history \
    WHERE schema_key = ? \
      AND entity_id = ? \
      AND root_commit_id = ? \
    ORDER BY depth ASC";

const HISTORY_FILE_PLUGIN_ROOT_DEPTH_RANGE_TIMELINE_QUERY: &str =
    "SELECT entity_id, depth, snapshot_content \
     FROM lix_state_history \
     WHERE file_id = ? \
       AND plugin_key = ? \
       AND root_commit_id = ? \
       AND depth >= ? \
       AND depth <= ? \
     ORDER BY entity_id ASC, depth ASC";

const HISTORY_FILE_PLUGIN_ROOT_DEPTH_BETWEEN_0_10_QUERY: &str =
    "SELECT entity_id, depth, snapshot_content \
     FROM lix_state_history \
     WHERE file_id = ? \
       AND plugin_key = ? \
       AND root_commit_id = ? \
       AND depth BETWEEN 0 AND 10 \
     ORDER BY entity_id ASC, depth ASC";

const HISTORY_FILE_PLUGIN_ROOT_ALL_ENTITIES_AT_DEPTH_QUERY: &str =
    "SELECT entity_id, depth, snapshot_content \
     FROM lix_state_history \
     WHERE file_id = ? \
       AND plugin_key = ? \
       AND root_commit_id = ? \
       AND depth = ? \
     ORDER BY entity_id ASC";

#[derive(Clone, Copy)]
struct HistorySeedConfig {
    entity_count: usize,
    history_updates: usize,
    branch_roots: usize,
}

impl HistorySeedConfig {
    fn label(self) -> String {
        format!(
            "entities={} depth={} roots={}",
            self.entity_count, self.history_updates, self.branch_roots
        )
    }

    fn slope_units(self) -> u64 {
        (self.entity_count as u64)
            .saturating_mul(self.history_updates as u64)
            .saturating_mul(self.branch_roots.max(1) as u64)
    }
}

fn bench_lix_state_history_count_by_root_commit(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");
    let seed = runtime
        .block_on(seed_engine_with_history())
        .expect("failed to seed benchmark engine");
    let params = vec![
        Value::Text(SCHEMA_KEY.to_string()),
        Value::Text(seed.root_commit_id.clone()),
    ];
    emit_explain_query_plan(
        &runtime,
        &seed.engine,
        "lix_state_history_count_by_root_commit",
        HISTORY_COUNT_BY_ROOT_QUERY,
        &params,
    );
    if explain_only_mode() {
        return;
    }

    c.bench_function("lix_state_history_count_by_root_commit", |b| {
        b.iter(|| {
            let result = runtime
                .block_on(seed.engine.execute(
                    HISTORY_COUNT_BY_ROOT_QUERY,
                    &params,
                    ExecuteOptions::default(),
                ))
                .expect("history count should succeed");
            let scalar = result
                .rows
                .first()
                .and_then(|row| row.first())
                .cloned()
                .expect("history count should return one scalar");
            black_box(scalar);
        });
    });
}

fn bench_lix_state_history_entity_timeline_scan(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");
    let seed = runtime
        .block_on(seed_engine_with_history())
        .expect("failed to seed benchmark engine");
    let params = vec![
        Value::Text(SCHEMA_KEY.to_string()),
        Value::Text(seed.target_entity_id.clone()),
        Value::Text(seed.root_commit_id.clone()),
    ];
    emit_explain_query_plan(
        &runtime,
        &seed.engine,
        "lix_state_history_entity_timeline_scan",
        HISTORY_ENTITY_TIMELINE_SCAN_QUERY,
        &params,
    );
    if explain_only_mode() {
        return;
    }

    c.bench_function("lix_state_history_entity_timeline_scan", |b| {
        b.iter(|| {
            let result = runtime
                .block_on(seed.engine.execute(
                    HISTORY_ENTITY_TIMELINE_SCAN_QUERY,
                    &params,
                    ExecuteOptions::default(),
                ))
                .expect("history timeline scan should succeed");
            black_box(result.rows.len());
        });
    });
}

fn bench_lix_state_history_plugin_runtime_query_exact(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");
    let seed = runtime
        .block_on(seed_engine_with_history())
        .expect("failed to seed benchmark engine");
    let params = plugin_runtime_query_params(&seed);
    emit_explain_query_plan(
        &runtime,
        &seed.engine,
        "lix_state_history_plugin_runtime_query_exact",
        PLUGIN_RUNTIME_HISTORY_QUERY,
        &params,
    );
    if explain_only_mode() {
        return;
    }

    c.bench_function("lix_state_history_plugin_runtime_query_exact", |b| {
        b.iter(|| {
            let result = runtime
                .block_on(seed.engine.execute(
                    PLUGIN_RUNTIME_HISTORY_QUERY,
                    &params,
                    ExecuteOptions::default(),
                ))
                .expect("plugin runtime history query should succeed");
            black_box(result.rows.len());
        });
    });
}

fn bench_lix_state_history_file_plugin_root_depth_range_timeline(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");
    let seed = runtime
        .block_on(seed_engine_with_history())
        .expect("failed to seed benchmark engine");
    let params = file_plugin_depth_range_params(&seed);
    emit_explain_query_plan(
        &runtime,
        &seed.engine,
        "lix_state_history_file_plugin_root_depth_range_timeline",
        HISTORY_FILE_PLUGIN_ROOT_DEPTH_RANGE_TIMELINE_QUERY,
        &params,
    );
    if explain_only_mode() {
        return;
    }

    c.bench_function(
        "lix_state_history_file_plugin_root_depth_range_timeline",
        |b| {
            b.iter(|| {
                let result = runtime
                    .block_on(seed.engine.execute(
                        HISTORY_FILE_PLUGIN_ROOT_DEPTH_RANGE_TIMELINE_QUERY,
                        &params,
                        ExecuteOptions::default(),
                    ))
                    .expect("file/plugin/root/depth timeline query should succeed");
                black_box(result.rows.len());
            });
        },
    );
}

fn bench_lix_state_history_file_plugin_root_depth_between_0_and_10(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");
    let seed = runtime
        .block_on(seed_engine_with_history())
        .expect("failed to seed benchmark engine");
    let params = file_plugin_root_params(&seed);
    emit_explain_query_plan(
        &runtime,
        &seed.engine,
        "lix_state_history_file_plugin_root_depth_between_0_and_10",
        HISTORY_FILE_PLUGIN_ROOT_DEPTH_BETWEEN_0_10_QUERY,
        &params,
    );
    if explain_only_mode() {
        return;
    }

    c.bench_function(
        "lix_state_history_file_plugin_root_depth_between_0_and_10",
        |b| {
            b.iter(|| {
                let result = runtime
                    .block_on(seed.engine.execute(
                        HISTORY_FILE_PLUGIN_ROOT_DEPTH_BETWEEN_0_10_QUERY,
                        &params,
                        ExecuteOptions::default(),
                    ))
                    .expect("file/plugin/root/depth BETWEEN 0 AND 10 query should succeed");
                black_box(result.rows.len());
            });
        },
    );
}

fn bench_lix_state_history_file_plugin_root_all_entities_at_depth(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");
    let seed = runtime
        .block_on(seed_engine_with_history())
        .expect("failed to seed benchmark engine");
    let params = file_plugin_all_entities_at_depth_params(&seed);
    emit_explain_query_plan(
        &runtime,
        &seed.engine,
        "lix_state_history_file_plugin_root_all_entities_at_depth",
        HISTORY_FILE_PLUGIN_ROOT_ALL_ENTITIES_AT_DEPTH_QUERY,
        &params,
    );
    if explain_only_mode() {
        return;
    }

    c.bench_function(
        "lix_state_history_file_plugin_root_all_entities_at_depth",
        |b| {
            b.iter(|| {
                let result = runtime
                    .block_on(seed.engine.execute(
                        HISTORY_FILE_PLUGIN_ROOT_ALL_ENTITIES_AT_DEPTH_QUERY,
                        &params,
                        ExecuteOptions::default(),
                    ))
                    .expect("file/plugin/root/all-entities-at-depth query should succeed");
                black_box(result.rows.len());
            });
        },
    );
}

fn bench_lix_state_history_plugin_runtime_query_exact_branchy_graph(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");
    let seed = runtime
        .block_on(seed_engine_with_history_config(BRANCHY_SEED_CONFIG))
        .expect("failed to seed branchy benchmark engine");
    let params = plugin_runtime_query_params(&seed);
    emit_explain_query_plan(
        &runtime,
        &seed.engine,
        "lix_state_history_plugin_runtime_query_exact_branchy_graph",
        PLUGIN_RUNTIME_HISTORY_QUERY,
        &params,
    );
    if explain_only_mode() {
        return;
    }

    c.bench_function(
        "lix_state_history_plugin_runtime_query_exact_branchy_graph",
        |b| {
            b.iter(|| {
                let result = runtime
                    .block_on(seed.engine.execute(
                        PLUGIN_RUNTIME_HISTORY_QUERY,
                        &params,
                        ExecuteOptions::default(),
                    ))
                    .expect("plugin runtime branchy history query should succeed");
                black_box(result.rows.len());
            });
        },
    );
}

fn bench_lix_state_history_plugin_runtime_scale_matrix(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");
    let mut group = c.benchmark_group("lix_state_history_plugin_runtime_scale_matrix");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));

    for config in scale_matrix_configs() {
        let seed = runtime
            .block_on(seed_engine_with_history_config(config))
            .expect("failed to seed scale matrix engine");
        let params = plugin_runtime_query_params(&seed);
        let label = config.label();
        emit_explain_query_plan(
            &runtime,
            &seed.engine,
            &format!("lix_state_history_plugin_runtime_scale_matrix/{label}"),
            PLUGIN_RUNTIME_HISTORY_QUERY,
            &params,
        );
        if explain_only_mode() {
            continue;
        }
        group.throughput(Throughput::Elements(config.slope_units()));
        group.bench_function(BenchmarkId::new("plugin_runtime_exact", label), |b| {
            b.iter(|| {
                let result = runtime
                    .block_on(seed.engine.execute(
                        PLUGIN_RUNTIME_HISTORY_QUERY,
                        &params,
                        ExecuteOptions::default(),
                    ))
                    .expect("plugin runtime history scale query should succeed");
                black_box(result.rows.len());
            });
        });
    }

    group.finish();
}

fn bench_lix_state_history_phase0_scale_tiers(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to build tokio runtime");
    let mut group = c.benchmark_group("lix_state_history_phase0_scale_tiers");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));

    for config in phase0_scale_tier_configs() {
        let seed = runtime
            .block_on(seed_engine_with_history_config(config))
            .expect("failed to seed phase0 tier engine");
        let params = plugin_runtime_query_params(&seed);
        let label = config.label();
        emit_explain_query_plan(
            &runtime,
            &seed.engine,
            &format!("lix_state_history_phase0_scale_tiers/{label}"),
            PLUGIN_RUNTIME_HISTORY_QUERY,
            &params,
        );
        if explain_only_mode() {
            continue;
        }
        group.throughput(Throughput::Elements(config.slope_units()));
        group.bench_function(BenchmarkId::new("plugin_runtime_exact", label), |b| {
            b.iter(|| {
                let result = runtime
                    .block_on(seed.engine.execute(
                        PLUGIN_RUNTIME_HISTORY_QUERY,
                        &params,
                        ExecuteOptions::default(),
                    ))
                    .expect("plugin runtime history phase0 tier query should succeed");
                black_box(result.rows.len());
            });
        });
    }

    group.finish();
}

struct HistorySeed {
    engine: lix_engine::Engine,
    root_commit_id: String,
    target_entity_id: String,
    plugin_runtime_commit_id: String,
    plugin_runtime_depth_floor: i64,
    depth_range_start: i64,
    depth_range_end: i64,
}

#[derive(Clone)]
struct CommitTip {
    commit_id: String,
    working_commit_id: String,
}

async fn seed_engine_with_history() -> Result<HistorySeed, LixError> {
    seed_engine_with_history_config(DEFAULT_SEED_CONFIG).await
}

async fn seed_engine_with_history_config(
    config: HistorySeedConfig,
) -> Result<HistorySeed, LixError> {
    if config.entity_count == 0 {
        return Err(LixError {
            message: "history benchmark seed requires entity_count > 0".to_string(),
        });
    }
    if config.history_updates == 0 {
        return Err(LixError {
            message: "history benchmark seed requires history_updates > 0".to_string(),
        });
    }

    let backend = Box::new(BenchSqliteBackend::in_memory());
    let engine = boot(BootArgs::new(backend));
    engine.init().await?;

    insert_stored_schema(&engine).await?;
    insert_initial_rows(&engine, config.entity_count).await?;
    let tip_timeline = apply_history_updates(
        &engine,
        config.entity_count,
        config.history_updates,
        "update",
        config.branch_roots > 1,
    )
    .await?;

    let root_commit_ids = if config.branch_roots > 1 {
        let roots = create_branch_roots(&engine, config, &tip_timeline).await?;
        ensure_commit_graph_is_branchy(&engine).await?;
        roots
    } else {
        vec![load_active_commit_id(&engine).await?]
    };
    let root_commit_id = root_commit_ids.last().cloned().ok_or_else(|| LixError {
        message: "history benchmark seed produced no root commits".to_string(),
    })?;
    let target_entity_id = entity_id_at(TARGET_ENTITY_INDEX.min(config.entity_count - 1));
    let (depth_range_start, depth_range_end) =
        load_depth_range_for_file_root(&engine, &root_commit_id).await?;
    let plugin_runtime_commit_id =
        load_commit_id_at_or_after_depth(&engine, &root_commit_id, depth_range_start).await?;

    Ok(HistorySeed {
        engine,
        root_commit_id,
        target_entity_id,
        plugin_runtime_commit_id,
        plugin_runtime_depth_floor: depth_range_start,
        depth_range_start,
        depth_range_end,
    })
}

async fn insert_stored_schema(engine: &lix_engine::Engine) -> Result<(), LixError> {
    let schema_snapshot = json!({
        "value": {
            "x-lix-key": SCHEMA_KEY,
            "x-lix-version": "1",
            "type": "object",
            "properties": {
                "value": { "type": "string" }
            },
            "required": ["value"],
            "additionalProperties": false
        }
    })
    .to_string();

    engine
        .execute(
            "INSERT INTO lix_state_by_version (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
             ) VALUES (?, 'lix_stored_schema', 'lix', 'global', 'lix', ?, '1')",
            &[
                Value::Text(format!("{SCHEMA_KEY}~1")),
                Value::Text(schema_snapshot),
            ],
            ExecuteOptions::default(),
        )
        .await?;
    Ok(())
}

async fn insert_initial_rows(
    engine: &lix_engine::Engine,
    entity_count: usize,
) -> Result<(), LixError> {
    for index in 0..entity_count {
        let entity_id = entity_id_at(index);
        let snapshot = json!({ "value": format!("seed-{index:05}") }).to_string();
        engine
            .execute(
                "INSERT INTO lix_state (\
                 entity_id, schema_key, file_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (?, ?, ?, 'lix', '1', ?)",
                &[
                    Value::Text(entity_id),
                    Value::Text(SCHEMA_KEY.to_string()),
                    Value::Text(FILE_ID.to_string()),
                    Value::Text(snapshot),
                ],
                ExecuteOptions::default(),
            )
            .await?;
    }
    Ok(())
}

async fn apply_history_updates(
    engine: &lix_engine::Engine,
    entity_count: usize,
    history_updates: usize,
    value_prefix: &str,
    capture_tip_timeline: bool,
) -> Result<Vec<CommitTip>, LixError> {
    let mut tip_timeline = Vec::new();
    if capture_tip_timeline {
        tip_timeline.push(load_active_commit_tip(engine).await?);
    }

    for sequence in 0..history_updates {
        apply_single_history_update(engine, entity_count, sequence, value_prefix).await?;
        if capture_tip_timeline {
            tip_timeline.push(load_active_commit_tip(engine).await?);
        }
    }

    Ok(tip_timeline)
}

async fn apply_single_history_update(
    engine: &lix_engine::Engine,
    entity_count: usize,
    sequence: usize,
    value_prefix: &str,
) -> Result<(), LixError> {
    let entity_index = sequence % entity_count;
    let entity_id = entity_id_at(entity_index);
    let snapshot = json!({
        "value": format!("{value_prefix}-{sequence:06}")
    })
    .to_string();

    let sql = format!(
        "UPDATE lix_state \
         SET snapshot_content = '{}' \
         WHERE entity_id = '{}' \
           AND schema_key = '{}' \
           AND file_id = '{}'",
        escape_sql_literal(&snapshot),
        escape_sql_literal(&entity_id),
        escape_sql_literal(SCHEMA_KEY),
        escape_sql_literal(FILE_ID),
    );
    engine.execute(&sql, &[], ExecuteOptions::default()).await?;
    Ok(())
}

async fn create_branch_roots(
    engine: &lix_engine::Engine,
    config: HistorySeedConfig,
    tip_timeline: &[CommitTip],
) -> Result<Vec<String>, LixError> {
    let base_tip = tip_timeline
        .get(tip_timeline.len().saturating_div(2))
        .or_else(|| tip_timeline.last())
        .cloned()
        .ok_or_else(|| LixError {
            message: "branch root seeding requires a non-empty commit timeline".to_string(),
        })?;
    let active_version_id = load_active_version_id(engine).await?;
    let mut roots = Vec::with_capacity(config.branch_roots);

    for branch_index in 0..config.branch_roots {
        rewind_active_version_tip(engine, &active_version_id, &base_tip).await?;
        let sequence = config.history_updates + branch_index;
        let branch_prefix = format!("branch-{branch_index:03}");
        apply_single_history_update(engine, config.entity_count, sequence, &branch_prefix).await?;
        roots.push(load_active_commit_id(engine).await?);
    }

    roots.sort();
    roots.dedup();

    if roots.len() < 2 {
        return Err(LixError {
            message: format!(
                "branch root seeding expected >= 2 unique roots, produced {}",
                roots.len()
            ),
        });
    }
    Ok(roots)
}

async fn load_active_commit_id(engine: &lix_engine::Engine) -> Result<String, LixError> {
    Ok(load_active_commit_tip(engine).await?.commit_id)
}

async fn load_active_commit_tip(engine: &lix_engine::Engine) -> Result<CommitTip, LixError> {
    let result = engine
        .execute(
            "SELECT v.commit_id, v.working_commit_id \
             FROM lix_active_version av \
             JOIN lix_version v ON v.id = av.version_id \
             LIMIT 1",
            &[],
            ExecuteOptions::default(),
        )
        .await?;
    let row = result.rows.first().ok_or_else(|| LixError {
        message: "active commit query returned no rows".to_string(),
    })?;
    let commit_id = row.first().ok_or_else(|| LixError {
        message: "active commit query missing commit_id column".to_string(),
    })?;
    let working_commit_id = row.get(1).ok_or_else(|| LixError {
        message: "active commit query missing working_commit_id column".to_string(),
    })?;

    Ok(CommitTip {
        commit_id: value_as_text(commit_id, "commit_id")?,
        working_commit_id: value_as_text(working_commit_id, "working_commit_id")?,
    })
}

async fn load_active_version_id(engine: &lix_engine::Engine) -> Result<String, LixError> {
    let result = engine
        .execute(
            "SELECT version_id \
             FROM lix_active_version \
             LIMIT 1",
            &[],
            ExecuteOptions::default(),
        )
        .await?;
    let value = result
        .rows
        .first()
        .and_then(|row| row.first())
        .ok_or_else(|| LixError {
            message: "active version query returned no rows".to_string(),
        })?;
    value_as_text(value, "version_id")
}

async fn rewind_active_version_tip(
    engine: &lix_engine::Engine,
    active_version_id: &str,
    tip: &CommitTip,
) -> Result<(), LixError> {
    engine
        .execute(
            "UPDATE lix_version \
             SET commit_id = ?, working_commit_id = ? \
             WHERE id = ?",
            &[
                Value::Text(tip.commit_id.clone()),
                Value::Text(tip.working_commit_id.clone()),
                Value::Text(active_version_id.to_string()),
            ],
            ExecuteOptions::default(),
        )
        .await?;
    Ok(())
}

async fn ensure_commit_graph_is_branchy(engine: &lix_engine::Engine) -> Result<(), LixError> {
    let result = engine
        .execute(
            "SELECT COUNT(*) \
             FROM (\
               SELECT parent_id \
               FROM lix_commit_edge \
               GROUP BY parent_id \
               HAVING COUNT(*) > 1\
             ) AS branch_parents",
            &[],
            ExecuteOptions::default(),
        )
        .await?;
    let count = result
        .rows
        .first()
        .and_then(|row| row.first())
        .ok_or_else(|| LixError {
            message: "branch parent count query returned no rows".to_string(),
        })?;
    let branch_parent_count = value_as_i64(count, "branch_parent_count")?;
    if branch_parent_count < 1 {
        return Err(LixError {
            message: "branch seed did not create commit-parent fan-out".to_string(),
        });
    }
    Ok(())
}

async fn load_depth_range_for_file_root(
    engine: &lix_engine::Engine,
    root_commit_id: &str,
) -> Result<(i64, i64), LixError> {
    let result = engine
        .execute(
            "SELECT COALESCE(MAX(depth), 0) \
             FROM lix_state_history \
             WHERE file_id = ? \
               AND plugin_key = ? \
               AND root_commit_id = ?",
            &[
                Value::Text(FILE_ID.to_string()),
                Value::Text(PLUGIN_KEY.to_string()),
                Value::Text(root_commit_id.to_string()),
            ],
            ExecuteOptions::default(),
        )
        .await?;
    let max_depth = result
        .rows
        .first()
        .and_then(|row| row.first())
        .map(|value| value_as_i64(value, "max_depth"))
        .transpose()?
        .unwrap_or(0);
    if max_depth <= 1 {
        return Ok((0, max_depth.max(0)));
    }

    let start = (max_depth / 3).max(1);
    let width = (max_depth / 4).max(1);
    let end = (start + width).min(max_depth);
    Ok((start, end))
}

async fn load_commit_id_at_or_after_depth(
    engine: &lix_engine::Engine,
    root_commit_id: &str,
    depth_floor: i64,
) -> Result<String, LixError> {
    let result = engine
        .execute(
            "SELECT commit_id \
             FROM lix_state_history \
             WHERE file_id = ? \
               AND plugin_key = ? \
               AND root_commit_id = ? \
               AND depth >= ? \
             ORDER BY depth ASC \
             LIMIT 1",
            &[
                Value::Text(FILE_ID.to_string()),
                Value::Text(PLUGIN_KEY.to_string()),
                Value::Text(root_commit_id.to_string()),
                Value::Integer(depth_floor),
            ],
            ExecuteOptions::default(),
        )
        .await?;
    if let Some(value) = result.rows.first().and_then(|row| row.first()) {
        return value_as_text(value, "commit_id");
    }
    Ok(root_commit_id.to_string())
}

fn plugin_runtime_query_params(seed: &HistorySeed) -> Vec<Value> {
    vec![
        Value::Text(FILE_ID.to_string()),
        Value::Text(PLUGIN_KEY.to_string()),
        Value::Text(seed.root_commit_id.clone()),
        Value::Text(seed.plugin_runtime_commit_id.clone()),
        Value::Integer(seed.plugin_runtime_depth_floor),
    ]
}

fn file_plugin_root_params(seed: &HistorySeed) -> Vec<Value> {
    vec![
        Value::Text(FILE_ID.to_string()),
        Value::Text(PLUGIN_KEY.to_string()),
        Value::Text(seed.root_commit_id.clone()),
    ]
}

fn file_plugin_depth_range_params(seed: &HistorySeed) -> Vec<Value> {
    vec![
        Value::Text(FILE_ID.to_string()),
        Value::Text(PLUGIN_KEY.to_string()),
        Value::Text(seed.root_commit_id.clone()),
        Value::Integer(seed.depth_range_start),
        Value::Integer(seed.depth_range_end),
    ]
}

fn file_plugin_all_entities_at_depth_params(seed: &HistorySeed) -> Vec<Value> {
    vec![
        Value::Text(FILE_ID.to_string()),
        Value::Text(PLUGIN_KEY.to_string()),
        Value::Text(seed.root_commit_id.clone()),
        Value::Integer(seed.depth_range_end.min(PHASE0_ALL_ENTITIES_DEPTH)),
    ]
}

fn entity_id_at(index: usize) -> String {
    format!("bench-history-entity-{index:04}")
}

fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn value_as_text(value: &Value, column: &str) -> Result<String, LixError> {
    match value {
        Value::Text(text) => Ok(text.clone()),
        other => Err(LixError {
            message: format!("{column} must be text, got {other:?}"),
        }),
    }
}

fn value_as_i64(value: &Value, column: &str) -> Result<i64, LixError> {
    match value {
        Value::Integer(number) => Ok(*number),
        Value::Null => Ok(0),
        other => Err(LixError {
            message: format!("{column} must be integer, got {other:?}"),
        }),
    }
}

fn emit_explain_query_plan(
    runtime: &Runtime,
    engine: &lix_engine::Engine,
    label: &str,
    sql: &str,
    params: &[Value],
) {
    let explain_sql = format!("EXPLAIN QUERY PLAN {sql}");
    let result = runtime
        .block_on(engine.execute(&explain_sql, params, ExecuteOptions::default()))
        .unwrap_or_else(|error| panic!("EXPLAIN QUERY PLAN failed for '{label}': {error:?}"));
    println!("[bench-explain] {label}");
    for row in &result.rows {
        let detail = row
            .get(3)
            .or_else(|| row.first())
            .map(value_for_explain)
            .unwrap_or_else(|| "<no explain detail row>".to_string());
        println!("[bench-explain]   {detail}");
    }
}

fn explain_only_mode() -> bool {
    std::env::var("LIX_BENCH_EXPLAIN_ONLY")
        .map(|raw| {
            let normalized = raw.trim().to_ascii_lowercase();
            !normalized.is_empty() && normalized != "0" && normalized != "false"
        })
        .unwrap_or(false)
}

fn value_for_explain(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Integer(number) => number.to_string(),
        Value::Real(number) => number.to_string(),
        Value::Text(text) => text.clone(),
        Value::Blob(blob) => format!("<blob:{} bytes>", blob.len()),
    }
}

fn scale_matrix_configs() -> Vec<HistorySeedConfig> {
    let entity_counts = [8, 32, 128];
    let depth_counts = [32, 128];
    let root_counts = [1, 4, 8];
    let mut out = Vec::with_capacity(entity_counts.len() * depth_counts.len() * root_counts.len());
    for entity_count in entity_counts {
        for history_updates in depth_counts {
            for branch_roots in root_counts {
                out.push(HistorySeedConfig {
                    entity_count,
                    history_updates,
                    branch_roots,
                });
            }
        }
    }
    out
}

fn phase0_scale_tier_configs() -> [HistorySeedConfig; 2] {
    [
        HistorySeedConfig {
            entity_count: 50,
            history_updates: 100,
            branch_roots: 1,
        },
        HistorySeedConfig {
            entity_count: 200,
            history_updates: 200,
            branch_roots: 1,
        },
    ]
}

criterion_group!(
    benches,
    bench_lix_state_history_count_by_root_commit,
    bench_lix_state_history_entity_timeline_scan,
    bench_lix_state_history_plugin_runtime_query_exact,
    bench_lix_state_history_file_plugin_root_depth_range_timeline,
    bench_lix_state_history_file_plugin_root_depth_between_0_and_10,
    bench_lix_state_history_file_plugin_root_all_entities_at_depth,
    bench_lix_state_history_plugin_runtime_query_exact_branchy_graph,
    bench_lix_state_history_plugin_runtime_scale_matrix,
    bench_lix_state_history_phase0_scale_tiers
);
criterion_main!(benches);
