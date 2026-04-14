use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use lix_engine::{
    AdditionalSessionOptions, CreateVersionOptions, Lix, MergeOutcome, MergeVersionOptions,
    Session, Value,
};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::runtime::Runtime;

#[path = "support/mod.rs"]
mod support;

use support::blob_fixture::{boot_new_file_backed_lix, temp_db};

const LINEAR_VERSION_ID: &str = "bench-cgw-linear";
const MERGE_MAIN_VERSION_ID: &str = "bench-cgw-main";
const DIVERGENT_PREFIX: &str = "bench-cgw-branch-";
const MERGE_BRANCH_PREFIX: &str = "bench-cgw-merge-";

const LINEAR_DEPTHS: &[usize] = &[10_000];
const DIVERGENT_BRANCH_COUNTS: &[usize] = &[16];
const MERGE_COUNTS: &[usize] = &[16];

fn bench_commit_graph_walk(c: &mut Criterion) {
    let runtime = Runtime::new().expect("failed to create tokio runtime");

    let mut linear_group = c.benchmark_group("commit_graph_walk_linear");
    linear_group.sample_size(10);
    linear_group.throughput(Throughput::Elements(1));
    for &depth in LINEAR_DEPTHS {
        linear_group.bench_with_input(BenchmarkId::new("depth", depth), &depth, |b, &depth| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let fixture = build_linear_fixture(&runtime, depth);
                    total += fixture.execute_walk_only(&runtime);
                }
                total
            });
        });
    }
    linear_group.finish();

    let mut divergent_group = c.benchmark_group("commit_graph_walk_divergent_heads");
    divergent_group.sample_size(10);
    divergent_group.throughput(Throughput::Elements(1));
    for &branch_count in DIVERGENT_BRANCH_COUNTS {
        divergent_group.bench_with_input(
            BenchmarkId::new("branch_count", branch_count),
            &branch_count,
            |b, &branch_count| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let fixture = build_divergent_fixture(&runtime, branch_count);
                        total += fixture.execute_walk_only(&runtime);
                    }
                    total
                });
            },
        );
    }
    divergent_group.finish();

    let mut merge_group = c.benchmark_group("commit_graph_walk_merge_heavy");
    merge_group.sample_size(10);
    merge_group.throughput(Throughput::Elements(1));
    for &merge_count in MERGE_COUNTS {
        merge_group.bench_with_input(
            BenchmarkId::new("merge_count", merge_count),
            &merge_count,
            |b, &merge_count| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let fixture = build_merge_heavy_fixture(&runtime, merge_count);
                        total += fixture.execute_walk_only(&runtime);
                    }
                    total
                });
            },
        );
    }
    merge_group.finish();
}

struct BenchFixture {
    lix: Arc<Lix>,
    walk_sql: String,
    expected_reachable_commits: i64,
    _tempdir: TempDir,
}

impl BenchFixture {
    fn execute_walk_only(self, runtime: &Runtime) -> Duration {
        let started = Instant::now();
        let result = runtime
            .block_on(self.lix.execute(&self.walk_sql, &[]))
            .expect("commit_graph_walk query should succeed");
        let elapsed = started.elapsed();

        let reachable_commits = first_integer(&result);
        assert_eq!(
            reachable_commits, self.expected_reachable_commits,
            "commit_graph_walk reachable commit count mismatch"
        );

        elapsed
    }
}

fn build_linear_fixture(runtime: &Runtime, depth: usize) -> BenchFixture {
    let (tempdir, db_path) = temp_db("commit-graph-walk-linear.sqlite");
    let lix = boot_new_file_backed_lix(runtime, &db_path, None, true);

    runtime
        .block_on(lix.create_version(CreateVersionOptions {
            id: Some(LINEAR_VERSION_ID.to_string()),
            name: Some(LINEAR_VERSION_ID.to_string()),
            source_version_id: None,
            hidden: false,
        }))
        .expect("linear commit_graph_walk version should be created");

    let baseline_count = walk_single_version_count(runtime, &lix, LINEAR_VERSION_ID);
    append_linear_commits(
        runtime,
        &lix,
        LINEAR_VERSION_ID,
        "bench-cgw-linear-key",
        depth,
    );

    BenchFixture {
        lix,
        walk_sql: build_single_version_walk_sql(LINEAR_VERSION_ID),
        expected_reachable_commits: baseline_count + depth as i64,
        _tempdir: tempdir,
    }
}

fn build_divergent_fixture(runtime: &Runtime, branch_count: usize) -> BenchFixture {
    let (tempdir, db_path) = temp_db("commit-graph-walk-divergent.sqlite");
    let lix = boot_new_file_backed_lix(runtime, &db_path, None, true);

    for index in 0..branch_count {
        let version_id = divergent_version_id(index);
        runtime
            .block_on(lix.create_version(CreateVersionOptions {
                id: Some(version_id.clone()),
                name: Some(version_id),
                source_version_id: None,
                hidden: false,
            }))
            .expect("divergent commit_graph_walk branch version should be created");
    }

    let baseline_count = walk_prefix_versions_count(runtime, &lix, DIVERGENT_PREFIX);

    for index in 0..branch_count {
        let version_id = divergent_version_id(index);
        let session = scoped_session(runtime, &lix, version_id);
        runtime
            .block_on(session.execute(
                "INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)",
                &[
                    Value::Text(format!("bench-cgw-divergent-key-{index:03}")),
                    Value::Text(format!("branch-{index:03}")),
                ],
            ))
            .expect("divergent commit_graph_walk branch write should succeed");
    }

    BenchFixture {
        lix,
        walk_sql: build_prefix_walk_sql(DIVERGENT_PREFIX),
        expected_reachable_commits: baseline_count + branch_count as i64,
        _tempdir: tempdir,
    }
}

fn build_merge_heavy_fixture(runtime: &Runtime, merge_count: usize) -> BenchFixture {
    let (tempdir, db_path) = temp_db("commit-graph-walk-merge-heavy.sqlite");
    let lix = boot_new_file_backed_lix(runtime, &db_path, None, true);

    runtime
        .block_on(lix.create_version(CreateVersionOptions {
            id: Some(MERGE_MAIN_VERSION_ID.to_string()),
            name: Some(MERGE_MAIN_VERSION_ID.to_string()),
            source_version_id: None,
            hidden: false,
        }))
        .expect("merge-heavy commit_graph_walk main version should be created");

    let baseline_count = walk_single_version_count(runtime, &lix, MERGE_MAIN_VERSION_ID);

    for index in 0..merge_count {
        let branch_id = merge_branch_version_id(index);
        runtime
            .block_on(lix.create_version(CreateVersionOptions {
                id: Some(branch_id.clone()),
                name: Some(branch_id.clone()),
                source_version_id: Some(MERGE_MAIN_VERSION_ID.to_string()),
                hidden: false,
            }))
            .expect("merge-heavy commit_graph_walk branch version should be created");

        let branch_session = scoped_session(runtime, &lix, branch_id.clone());
        runtime
            .block_on(branch_session.execute(
                "INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)",
                &[
                    Value::Text(format!("bench-cgw-merge-branch-key-{index:03}")),
                    Value::Text(format!("branch-{index:03}")),
                ],
            ))
            .expect("merge-heavy commit_graph_walk branch write should succeed");

        let main_session = scoped_session(runtime, &lix, MERGE_MAIN_VERSION_ID.to_string());
        runtime
            .block_on(main_session.execute(
                "INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)",
                &[
                    Value::Text(format!("bench-cgw-merge-main-key-{index:03}")),
                    Value::Text(format!("main-{index:03}")),
                ],
            ))
            .expect("merge-heavy commit_graph_walk main write should succeed");

        let merged = runtime
            .block_on(lix.merge_version(MergeVersionOptions {
                source_version_id: branch_id,
                target_version_id: MERGE_MAIN_VERSION_ID.to_string(),
                expected_heads: None,
            }))
            .expect("merge-heavy commit_graph_walk merge should succeed");
        assert_eq!(
            merged.outcome,
            MergeOutcome::MergeCommitted,
            "merge-heavy commit_graph_walk should create merge commits"
        );
    }

    BenchFixture {
        lix,
        walk_sql: build_single_version_walk_sql(MERGE_MAIN_VERSION_ID),
        expected_reachable_commits: baseline_count + (merge_count as i64 * 3),
        _tempdir: tempdir,
    }
}

fn append_linear_commits(
    runtime: &Runtime,
    lix: &Arc<Lix>,
    version_id: &str,
    key: &str,
    depth: usize,
) {
    if depth == 0 {
        return;
    }

    let session = scoped_session(runtime, lix, version_id.to_string());
    runtime
        .block_on(session.execute(
            "INSERT INTO lix_key_value (key, value) VALUES (?1, ?2)",
            &[
                Value::Text(key.to_string()),
                Value::Text("linear-00000".to_string()),
            ],
        ))
        .expect("linear commit_graph_walk seed insert should succeed");

    for revision in 1..depth {
        runtime
            .block_on(session.execute(
                "UPDATE lix_key_value SET value = ?1 WHERE key = ?2",
                &[
                    Value::Text(format!("linear-{revision:05}")),
                    Value::Text(key.to_string()),
                ],
            ))
            .expect("linear commit_graph_walk update should succeed");
    }
}

fn build_single_version_walk_sql(version_id: &str) -> String {
    let version_id = escape_sql_string(version_id);
    format!(
        "WITH RECURSIVE \
           seed_heads AS ( \
             SELECT commit_id \
             FROM lix_version \
             WHERE id = '{version_id}' \
           ), \
           scoped_edges AS ( \
             SELECT DISTINCT child_id, parent_id \
             FROM lix_commit_edge \
             WHERE version_id = '{version_id}' \
           ), \
           walk(commit_id) AS ( \
             SELECT commit_id FROM seed_heads \
             UNION \
             SELECT scoped_edges.parent_id \
             FROM scoped_edges \
             JOIN walk ON scoped_edges.child_id = walk.commit_id \
           ) \
         SELECT COUNT(DISTINCT commit_id) FROM walk"
    )
}

fn build_prefix_walk_sql(version_prefix: &str) -> String {
    let like_pattern = escape_sql_string(&format!("{version_prefix}%"));
    format!(
        "WITH RECURSIVE \
           seed_heads AS ( \
             SELECT commit_id \
             FROM lix_version \
             WHERE id LIKE '{like_pattern}' \
           ), \
           scoped_edges AS ( \
             SELECT DISTINCT child_id, parent_id \
             FROM lix_commit_edge \
             WHERE version_id LIKE '{like_pattern}' \
           ), \
           walk(commit_id) AS ( \
             SELECT commit_id FROM seed_heads \
             UNION \
             SELECT scoped_edges.parent_id \
             FROM scoped_edges \
             JOIN walk ON scoped_edges.child_id = walk.commit_id \
           ) \
         SELECT COUNT(DISTINCT commit_id) FROM walk"
    )
}

fn walk_single_version_count(runtime: &Runtime, lix: &Arc<Lix>, version_id: &str) -> i64 {
    scalar_count(runtime, lix, &build_single_version_walk_sql(version_id))
}

fn walk_prefix_versions_count(runtime: &Runtime, lix: &Arc<Lix>, version_prefix: &str) -> i64 {
    scalar_count(runtime, lix, &build_prefix_walk_sql(version_prefix))
}

fn scalar_count(runtime: &Runtime, lix: &Arc<Lix>, sql: &str) -> i64 {
    let result = runtime
        .block_on(lix.execute(sql, &[]))
        .expect("commit_graph_walk count query should succeed");
    first_integer(&result)
}

fn scoped_session(runtime: &Runtime, lix: &Arc<Lix>, version_id: String) -> Session {
    runtime
        .block_on(lix.open_additional_session(AdditionalSessionOptions {
            active_version_id: Some(version_id),
            active_account_ids: None,
        }))
        .expect("scoped session should open")
}

fn divergent_version_id(index: usize) -> String {
    format!("{DIVERGENT_PREFIX}{index:03}")
}

fn merge_branch_version_id(index: usize) -> String {
    format!("{MERGE_BRANCH_PREFIX}{index:03}")
}

fn escape_sql_string(input: &str) -> String {
    input.replace('\'', "''")
}

fn first_integer(result: &lix_engine::ExecuteResult) -> i64 {
    match result
        .statements
        .first()
        .and_then(|statement| statement.rows.first())
        .and_then(|row| row.first())
    {
        Some(Value::Integer(value)) => *value,
        Some(Value::Text(value)) => value
            .parse::<i64>()
            .expect("commit_graph_walk integer result should parse"),
        other => panic!("expected integer result, got {other:?}"),
    }
}

criterion_group!(benches, bench_commit_graph_walk);
criterion_main!(benches);
