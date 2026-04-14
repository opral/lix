use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use lix_engine::{
    AdditionalSessionOptions, CreateVersionOptions, Lix, MergeOutcome, MergeVersionOptions,
    Session, Value,
};
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::runtime::Runtime;

#[path = "support/mod.rs"]
mod support;

use support::blob_fixture::{boot_new_file_backed_lix, temp_db};
use support::state_fixture::{
    build_state_insert_sql_batches_with_prefix, build_state_update_sql_batches_with_range,
    register_bench_state_schema, BENCH_STATE_FILE_ID, BENCH_STATE_SCHEMA_KEY,
};

const SOURCE_VERSION_ID: &str = "bench-version-merge-source";
const TARGET_VERSION_ID: &str = "bench-version-merge-target";

const LARGE_DOC_ROW_COUNT: usize = 10_000;
const INSERT_CHUNK_SIZE: usize = 250;
const INITIAL_VALUE_PREFIX: &str = "version-merge-initial";
const SOURCE_VALUE_PREFIX: &str = "version-merge-source";
const TARGET_VALUE_PREFIX: &str = "version-merge-target";

const KV_FAST_FORWARD_KEY: &str = "version-merge-fast-forward";
const KV_SOURCE_ONLY_KEY: &str = "version-merge-source-only";
const KV_TARGET_ONLY_KEY: &str = "version-merge-target-only";
const KV_CONFLICT_KEY: &str = "version-merge-overlap-conflict";

#[derive(Clone, Copy, Debug)]
enum MergeCase {
    AlreadyUpToDate,
    FastForward,
    DisjointKeyValue,
    OverlapConflict,
    LargeDocSparseDisjoint,
}

impl MergeCase {
    fn benchmark_id(self) -> &'static str {
        match self {
            Self::AlreadyUpToDate => "already_up_to_date",
            Self::FastForward => "fast_forward",
            Self::DisjointKeyValue => "disjoint_key_value",
            Self::OverlapConflict => "overlap_conflict",
            Self::LargeDocSparseDisjoint => "large_doc_sparse_disjoint",
        }
    }
}

fn bench_version_merge(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to create tokio runtime");
    let merge_cases = [
        MergeCase::AlreadyUpToDate,
        MergeCase::FastForward,
        MergeCase::DisjointKeyValue,
        MergeCase::OverlapConflict,
        MergeCase::LargeDocSparseDisjoint,
    ];

    let mut group = c.benchmark_group("version_merge");
    group.sample_size(10);
    group.throughput(Throughput::Elements(1));

    for merge_case in merge_cases {
        group.bench_with_input(
            BenchmarkId::from_parameter(merge_case.benchmark_id()),
            &merge_case,
            |b, &merge_case| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let fixture = build_fixture(&runtime, merge_case);
                        total += fixture.execute_merge_only(&runtime);
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

enum ExpectedMerge {
    AlreadyUpToDate,
    FastForward {
        merged_key: &'static str,
    },
    DisjointKeyValue {
        source_key: &'static str,
        target_key: &'static str,
    },
    OverlapConflict {
        conflict_key: &'static str,
        target_value: &'static str,
    },
    LargeDocSparseDisjoint {
        source_entity_index: usize,
        target_entity_index: usize,
    },
}

struct BenchFixture {
    lix: Arc<Lix>,
    source_version_id: String,
    target_version_id: String,
    source_head_before: String,
    target_head_before: String,
    expected: ExpectedMerge,
    _tempdir: TempDir,
}

impl BenchFixture {
    fn execute_merge_only(self, runtime: &Runtime) -> Duration {
        let started = Instant::now();
        let merge_result = runtime.block_on(self.lix.merge_version(MergeVersionOptions {
            source_version_id: self.source_version_id.clone(),
            target_version_id: self.target_version_id.clone(),
            expected_heads: None,
        }));
        let elapsed = started.elapsed();

        match self.expected {
            ExpectedMerge::AlreadyUpToDate => {
                let merged = merge_result.expect("version_merge already_up_to_date should succeed");
                assert_eq!(merged.outcome, MergeOutcome::AlreadyUpToDate);
                assert_eq!(merged.source_head_before_commit_id, self.source_head_before);
                assert_eq!(merged.target_head_before_commit_id, self.target_head_before);
                assert_eq!(merged.target_head_after_commit_id, self.target_head_before);
                assert_eq!(merged.created_merge_commit_id, None);
                assert_eq!(merged.applied_change_count, 0);
                assert_eq!(merged.created_tombstone_count, 0);
                assert_eq!(
                    version_commit_id(runtime, &self.lix, &self.target_version_id),
                    self.target_head_before,
                    "version_merge already_up_to_date should not move target head",
                );
            }
            ExpectedMerge::FastForward { merged_key } => {
                let merged = merge_result.expect("version_merge fast_forward should succeed");
                assert_eq!(merged.outcome, MergeOutcome::FastForwarded);
                assert_eq!(merged.source_head_before_commit_id, self.source_head_before);
                assert_eq!(merged.target_head_before_commit_id, self.target_head_before);
                assert_eq!(merged.target_head_after_commit_id, self.source_head_before);
                assert_eq!(merged.created_merge_commit_id, None);
                assert_eq!(merged.applied_change_count, 0);
                assert_eq!(merged.created_tombstone_count, 0);
                assert_eq!(
                    key_value_in_version(runtime, &self.lix, &self.target_version_id, merged_key)
                        .as_deref(),
                    Some("source"),
                    "version_merge fast_forward should expose source write on target",
                );
            }
            ExpectedMerge::DisjointKeyValue {
                source_key,
                target_key,
            } => {
                let merged = merge_result.expect("version_merge disjoint_key_value should succeed");
                assert_eq!(merged.outcome, MergeOutcome::MergeCommitted);
                assert_eq!(merged.source_head_before_commit_id, self.source_head_before);
                assert_eq!(merged.target_head_before_commit_id, self.target_head_before);
                assert_eq!(merged.applied_change_count, 1);
                assert_eq!(merged.created_tombstone_count, 0);
                let merge_commit_id = merged
                    .created_merge_commit_id
                    .clone()
                    .expect("version_merge disjoint_key_value should create a merge commit");
                assert_eq!(merged.target_head_after_commit_id, merge_commit_id);
                assert_eq!(
                    version_commit_id(runtime, &self.lix, &self.source_version_id),
                    self.source_head_before,
                    "version_merge disjoint_key_value should leave source head unchanged",
                );
                assert_eq!(
                    key_value_in_version(runtime, &self.lix, &self.target_version_id, source_key)
                        .as_deref(),
                    Some("source"),
                    "version_merge disjoint_key_value should apply source write onto target",
                );
                assert_eq!(
                    key_value_in_version(runtime, &self.lix, &self.target_version_id, target_key)
                        .as_deref(),
                    Some("target"),
                    "version_merge disjoint_key_value should preserve target write",
                );
            }
            ExpectedMerge::OverlapConflict {
                conflict_key,
                target_value,
            } => {
                let error = merge_result
                    .expect_err("version_merge overlap_conflict should fail with a conflict");
                assert_eq!(error.code, "LIX_ERROR_MERGE_CONFLICT");
                assert!(
                    error.description.contains(conflict_key),
                    "version_merge overlap_conflict should mention conflicting key",
                );
                assert_eq!(
                    version_commit_id(runtime, &self.lix, &self.target_version_id),
                    self.target_head_before,
                    "version_merge overlap_conflict should leave target head unchanged",
                );
                assert_eq!(
                    key_value_in_version(runtime, &self.lix, &self.target_version_id, conflict_key)
                        .as_deref(),
                    Some(target_value),
                    "version_merge overlap_conflict should preserve target value",
                );
            }
            ExpectedMerge::LargeDocSparseDisjoint {
                source_entity_index,
                target_entity_index,
            } => {
                let merged =
                    merge_result.expect("version_merge large_doc_sparse_disjoint should succeed");
                assert_eq!(merged.outcome, MergeOutcome::MergeCommitted);
                assert_eq!(merged.source_head_before_commit_id, self.source_head_before);
                assert_eq!(merged.target_head_before_commit_id, self.target_head_before);
                assert_eq!(merged.applied_change_count, 1);
                assert_eq!(merged.created_tombstone_count, 0);
                let merge_commit_id = merged
                    .created_merge_commit_id
                    .clone()
                    .expect("version_merge large_doc_sparse_disjoint should create merge commit");
                assert_eq!(merged.target_head_after_commit_id, merge_commit_id);
                assert_eq!(
                    visible_state_count(runtime, &self.lix, &self.target_version_id),
                    LARGE_DOC_ROW_COUNT as i64,
                    "version_merge large_doc_sparse_disjoint should preserve visible row count",
                );
                assert_eq!(
                    state_snapshot_in_version(
                        runtime,
                        &self.lix,
                        &self.target_version_id,
                        source_entity_index,
                    )
                    .as_deref(),
                    Some(expected_snapshot(SOURCE_VALUE_PREFIX, source_entity_index).as_str()),
                    "version_merge large_doc_sparse_disjoint should apply source sparse update",
                );
                assert_eq!(
                    state_snapshot_in_version(
                        runtime,
                        &self.lix,
                        &self.target_version_id,
                        target_entity_index,
                    )
                    .as_deref(),
                    Some(expected_snapshot(TARGET_VALUE_PREFIX, target_entity_index).as_str()),
                    "version_merge large_doc_sparse_disjoint should preserve target sparse update",
                );
            }
        }

        elapsed
    }
}

fn build_fixture(runtime: &Runtime, merge_case: MergeCase) -> BenchFixture {
    let (tempdir, db_path) = temp_db("version-merge.sqlite");
    let lix = boot_new_file_backed_lix(runtime, &db_path, None, true);

    if matches!(merge_case, MergeCase::LargeDocSparseDisjoint) {
        register_bench_state_schema(runtime, &lix);
        seed_large_doc_base(runtime, &lix);
    }

    create_version_pair(runtime, &lix);

    let expected = match merge_case {
        MergeCase::AlreadyUpToDate => ExpectedMerge::AlreadyUpToDate,
        MergeCase::FastForward => {
            let source_session = scoped_session(runtime, &lix, SOURCE_VERSION_ID.to_string());
            runtime
                .block_on(source_session.execute(
                    "INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)",
                    &[
                        Value::Text(KV_FAST_FORWARD_KEY.to_string()),
                        Value::Text("source".to_string()),
                    ],
                ))
                .expect("version_merge fast_forward source insert should succeed");

            ExpectedMerge::FastForward {
                merged_key: KV_FAST_FORWARD_KEY,
            }
        }
        MergeCase::DisjointKeyValue => {
            let source_session = scoped_session(runtime, &lix, SOURCE_VERSION_ID.to_string());
            runtime
                .block_on(source_session.execute(
                    "INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)",
                    &[
                        Value::Text(KV_SOURCE_ONLY_KEY.to_string()),
                        Value::Text("source".to_string()),
                    ],
                ))
                .expect("version_merge disjoint_key_value source insert should succeed");

            let target_session = scoped_session(runtime, &lix, TARGET_VERSION_ID.to_string());
            runtime
                .block_on(target_session.execute(
                    "INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)",
                    &[
                        Value::Text(KV_TARGET_ONLY_KEY.to_string()),
                        Value::Text("target".to_string()),
                    ],
                ))
                .expect("version_merge disjoint_key_value target insert should succeed");

            ExpectedMerge::DisjointKeyValue {
                source_key: KV_SOURCE_ONLY_KEY,
                target_key: KV_TARGET_ONLY_KEY,
            }
        }
        MergeCase::OverlapConflict => {
            let source_session = scoped_session(runtime, &lix, SOURCE_VERSION_ID.to_string());
            runtime
                .block_on(source_session.execute(
                    "INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)",
                    &[
                        Value::Text(KV_CONFLICT_KEY.to_string()),
                        Value::Text("source".to_string()),
                    ],
                ))
                .expect("version_merge overlap_conflict source insert should succeed");

            let target_session = scoped_session(runtime, &lix, TARGET_VERSION_ID.to_string());
            runtime
                .block_on(target_session.execute(
                    "INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)",
                    &[
                        Value::Text(KV_CONFLICT_KEY.to_string()),
                        Value::Text("target".to_string()),
                    ],
                ))
                .expect("version_merge overlap_conflict target insert should succeed");

            ExpectedMerge::OverlapConflict {
                conflict_key: KV_CONFLICT_KEY,
                target_value: "target",
            }
        }
        MergeCase::LargeDocSparseDisjoint => {
            let source_session = scoped_session(runtime, &lix, SOURCE_VERSION_ID.to_string());
            for sql in build_state_update_sql_batches_with_range(0, 1, SOURCE_VALUE_PREFIX)
                .expect("version_merge large_doc_sparse_disjoint source updates")
            {
                runtime
                    .block_on(source_session.execute(&sql, &[]))
                    .expect("version_merge large_doc_sparse_disjoint source update should succeed");
            }

            let target_session = scoped_session(runtime, &lix, TARGET_VERSION_ID.to_string());
            for sql in build_state_update_sql_batches_with_range(1, 1, TARGET_VALUE_PREFIX)
                .expect("version_merge large_doc_sparse_disjoint target updates")
            {
                runtime
                    .block_on(target_session.execute(&sql, &[]))
                    .expect("version_merge large_doc_sparse_disjoint target update should succeed");
            }

            ExpectedMerge::LargeDocSparseDisjoint {
                source_entity_index: 0,
                target_entity_index: 1,
            }
        }
    };

    let source_head_before = version_commit_id(runtime, &lix, SOURCE_VERSION_ID);
    let target_head_before = version_commit_id(runtime, &lix, TARGET_VERSION_ID);

    BenchFixture {
        lix,
        source_version_id: SOURCE_VERSION_ID.to_string(),
        target_version_id: TARGET_VERSION_ID.to_string(),
        source_head_before,
        target_head_before,
        expected,
        _tempdir: tempdir,
    }
}

fn create_version_pair(runtime: &Runtime, lix: &Arc<Lix>) {
    for version_id in [SOURCE_VERSION_ID, TARGET_VERSION_ID] {
        runtime
            .block_on(lix.create_version(CreateVersionOptions {
                id: Some(version_id.to_string()),
                name: Some(version_id.to_string()),
                source_version_id: None,
                hidden: false,
            }))
            .expect("version_merge benchmark versions should be created");
    }
}

fn seed_large_doc_base(runtime: &Runtime, lix: &Arc<Lix>) {
    let insert_batches = build_state_insert_sql_batches_with_prefix(
        LARGE_DOC_ROW_COUNT,
        INSERT_CHUNK_SIZE,
        INITIAL_VALUE_PREFIX,
    )
    .expect("version_merge large doc seed insert batches");

    for sql in &insert_batches {
        runtime
            .block_on(lix.execute(sql, &[]))
            .expect("version_merge large doc seed insert should succeed");
    }
}

fn scoped_session(runtime: &Runtime, lix: &Arc<Lix>, version_id: String) -> Session {
    runtime
        .block_on(lix.open_additional_session(AdditionalSessionOptions {
            active_version_id: Some(version_id),
            active_account_ids: None,
        }))
        .expect("scoped session should open")
}

fn version_commit_id(runtime: &Runtime, lix: &Arc<Lix>, version_id: &str) -> String {
    scalar_text(
        runtime,
        lix,
        "SELECT commit_id FROM lix_version WHERE id = ?1 LIMIT 1",
        &[Value::Text(version_id.to_string())],
    )
}

fn key_value_in_version(
    runtime: &Runtime,
    lix: &Arc<Lix>,
    version_id: &str,
    key: &str,
) -> Option<String> {
    optional_text_in_scoped_session(
        runtime,
        lix,
        version_id,
        "SELECT value FROM lix_key_value WHERE key = ?1 LIMIT 1",
        &[Value::Text(key.to_string())],
    )
}

fn visible_state_count(runtime: &Runtime, lix: &Arc<Lix>, version_id: &str) -> i64 {
    let scoped = scoped_session(runtime, lix, version_id.to_string());
    first_integer(
        &runtime
            .block_on(scoped.execute(
                "SELECT COUNT(*) FROM lix_state WHERE file_id = ?1 AND schema_key = ?2",
                &[
                    Value::Text(BENCH_STATE_FILE_ID.to_string()),
                    Value::Text(BENCH_STATE_SCHEMA_KEY.to_string()),
                ],
            ))
            .expect("visible state count query should succeed"),
    )
}

fn state_snapshot_in_version(
    runtime: &Runtime,
    lix: &Arc<Lix>,
    version_id: &str,
    entity_index: usize,
) -> Option<String> {
    optional_text_in_scoped_session(
        runtime,
        lix,
        version_id,
        "SELECT snapshot_content \
         FROM lix_state \
         WHERE file_id = ?1 \
           AND schema_key = ?2 \
           AND entity_id = ?3 \
         LIMIT 1",
        &[
            Value::Text(BENCH_STATE_FILE_ID.to_string()),
            Value::Text(BENCH_STATE_SCHEMA_KEY.to_string()),
            Value::Text(format!("entity-{entity_index:05}")),
        ],
    )
}

fn optional_text_in_scoped_session(
    runtime: &Runtime,
    lix: &Arc<Lix>,
    version_id: &str,
    sql: &str,
    params: &[Value],
) -> Option<String> {
    let scoped = scoped_session(runtime, lix, version_id.to_string());
    let result = runtime
        .block_on(scoped.execute(sql, params))
        .expect("scoped verification query should succeed");
    match result
        .statements
        .first()
        .and_then(|statement| statement.rows.first())
        .and_then(|row| row.first())
    {
        Some(Value::Text(value)) => Some(value.clone()),
        Some(Value::Integer(value)) => Some(value.to_string()),
        None => None,
        other => panic!("expected text-like verification row, got {other:?}"),
    }
}

fn scalar_text(runtime: &Runtime, lix: &Arc<Lix>, sql: &str, params: &[Value]) -> String {
    let result = runtime
        .block_on(lix.execute(sql, params))
        .expect("text verification query should succeed");
    match result
        .statements
        .first()
        .and_then(|statement| statement.rows.first())
        .and_then(|row| row.first())
    {
        Some(Value::Text(value)) => value.clone(),
        Some(Value::Integer(value)) => value.to_string(),
        other => panic!("expected text-like verification row, got {other:?}"),
    }
}

fn first_integer(result: &lix_engine::ExecuteResult) -> i64 {
    match result
        .statements
        .first()
        .and_then(|statement| statement.rows.first())
        .and_then(|row| row.first())
    {
        Some(Value::Integer(value)) => *value,
        other => panic!("expected integer result, got {other:?}"),
    }
}

fn expected_snapshot(value_prefix: &str, entity_index: usize) -> String {
    serde_json::to_string(&json!({
        "value": format!("{value_prefix}-{entity_index:05}")
    }))
    .expect("expected snapshot should serialize")
}

criterion_group!(benches, bench_version_merge);
criterion_main!(benches);
