use std::fmt::Write as _;
use std::future::Future;
use std::hint::black_box;
use std::time::{Duration, Instant};

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use lix::Value;
use lix::storage::Memory;
use lix::{Lix, open_lix};
use serde_json::json;

const WORKING_SOURCE: &str =
    "lix_diff('lix_key_value', lix_root_commit_id(), lix_active_branch_commit_id())";

fn diff_command_benches(c: &mut Criterion) {
    if std::env::var_os("LIX_DIFF_COMMAND_PROFILE").is_some() {
        profile();
        std::process::exit(0);
    }
    let runtime = runtime();
    c.bench_function("diff_commands/working_diff_1000", |b| {
        b.iter_batched(
            || runtime.block_on(seeded_session(1_000)),
            |session| {
                runtime.block_on(execute(
                    &session,
                    &format!("SELECT COUNT(*) FROM {WORKING_SOURCE}"),
                ));
            },
            BatchSize::LargeInput,
        );
    });
    c.bench_function("diff_commands/revert_100_of_1000", |b| {
        b.iter_batched(
            || {
                runtime.block_on(async {
                    let session = seeded_session(1_000).await;
                    let sql = command_sql("lix_revert", WORKING_SOURCE, "", 100);
                    (session, sql)
                })
            },
            |(session, sql)| runtime.block_on(execute(&session, &sql)),
            BatchSize::LargeInput,
        );
    });
    c.bench_function("diff_commands/apply_100_of_1000", |b| {
        b.iter_batched(
            || runtime.block_on(apply_fixture(1_000, 100)),
            |(session, sql)| runtime.block_on(execute(&session, &sql)),
            BatchSize::LargeInput,
        );
    });
    c.bench_function("diff_commands/partial_checkpoint_500_of_1000", |b| {
        b.iter_batched(
            || {
                runtime.block_on(async {
                    let session = seeded_session(1_000).await;
                    let sql = command_sql("lix_create_checkpoint", WORKING_SOURCE, "", 500);
                    (session, sql)
                })
            },
            |(session, sql)| runtime.block_on(execute(&session, &sql)),
            BatchSize::LargeInput,
        );
    });
}

fn profile() {
    let runtime = runtime();
    let samples = std::env::var("LIX_DIFF_COMMAND_PROFILE_SAMPLES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(5);
    let row_counts = std::env::var("LIX_DIFF_COMMAND_PROFILE_ROWS")
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(|rows| {
                    rows.trim()
                        .parse::<usize>()
                        .expect("valid profile row count")
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| vec![100, 1_000, 10_000]);
    for rows in row_counts {
        for sample in 0..samples {
            runtime.block_on(profile_sample(rows, sample));
        }
    }
}

async fn profile_sample(rows: usize, sample: usize) {
    let session = new_session().await;
    let baseline = active_commit(&session).await;
    execute(&session, &insert_sql(rows)).await;
    let head = active_commit(&session).await;
    let selected = (rows / 10).max(1);

    let scan = elapsed(async {
        execute(&session, &format!("SELECT COUNT(*) FROM {WORKING_SOURCE}")).await;
    })
    .await;
    emit("working_diff", rows, rows, sample, scan);

    let atelier_count = elapsed(async {
        execute(
            &session,
            &format!(
                "SELECT COUNT(*) AS change_count, SUM(row_count) AS atom_count \
                 FROM {WORKING_SOURCE}"
            ),
        )
        .await;
    })
    .await;
    emit(
        "working_diff_projected_aggregate",
        rows,
        rows,
        sample,
        atelier_count,
    );

    let revert = elapsed(async {
        execute(
            &session,
            &format!(
                "INSERT INTO lix_revert (row_ref) \
                 SELECT row_ref FROM {WORKING_SOURCE} \
                 ORDER BY key LIMIT {selected}"
            ),
        )
        .await;
    })
    .await;
    emit("revert", rows, selected, sample, revert);

    let apply = elapsed(async {
        execute(
            &session,
            &format!(
                "INSERT INTO lix_apply (row_ref) \
                 SELECT row_ref \
                 FROM lix_diff('lix_key_value', '{baseline}', '{head}') \
                 ORDER BY key LIMIT {selected}"
            ),
        )
        .await;
    })
    .await;
    emit("apply", rows, selected, sample, apply);

    let checkpoint_selected = rows / 2;
    let checkpoint = elapsed(async {
        execute(
            &session,
            &format!(
                "SELECT commit_id FROM lix_create_checkpoint(ARRAY( \
                 SELECT row_ref FROM {WORKING_SOURCE} \
                 ORDER BY key LIMIT {checkpoint_selected}))"
            ),
        )
        .await;
    })
    .await;
    emit(
        "partial_checkpoint",
        rows,
        checkpoint_selected,
        sample,
        checkpoint,
    );
}

async fn elapsed(future: impl Future<Output = ()>) -> Duration {
    let start = Instant::now();
    future.await;
    start.elapsed()
}

fn emit(operation: &str, rows: usize, selected: usize, sample: usize, elapsed: Duration) {
    println!(
        "{}",
        json!({
            "suite": "diff_commands",
            "operation": operation,
            "rows": rows,
            "selected": selected,
            "sample": sample,
            "elapsed_ns": elapsed.as_nanos(),
        })
    );
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create diff command benchmark runtime")
}

async fn new_session() -> Lix<Memory> {
    let storage = Memory::new();
    open_lix()
        .with_storage(storage)
        .await
        .expect("open benchmark lix")
}

async fn seeded_session(rows: usize) -> Lix<Memory> {
    let session = new_session().await;
    execute(&session, &insert_sql(rows)).await;
    session
}

async fn apply_fixture(rows: usize, selected: usize) -> (Lix<Memory>, String) {
    let session = new_session().await;
    let baseline = active_commit(&session).await;
    execute(&session, &insert_sql(rows)).await;
    let head = active_commit(&session).await;
    execute(
        &session,
        &command_sql("lix_revert", WORKING_SOURCE, "", selected),
    )
    .await;
    let source = format!("lix_diff('lix_key_value', '{baseline}', '{head}')");
    (session, command_sql("lix_apply", &source, "", selected))
}

fn command_sql(command: &str, source: &str, extra_predicate: &str, selected: usize) -> String {
    if command == "lix_create_checkpoint" {
        return format!(
            "SELECT commit_id FROM lix_create_checkpoint(ARRAY( \
             SELECT row_ref FROM {source} \
             WHERE true {extra_predicate} \
             ORDER BY key LIMIT {selected}))"
        );
    }
    format!(
        "INSERT INTO {command} (row_ref) \
         SELECT row_ref FROM {source} \
         WHERE true {extra_predicate} \
         ORDER BY key LIMIT {selected}"
    )
}

async fn active_commit(session: &Lix<Memory>) -> String {
    let result = session
        .execute("SELECT lix_active_branch_commit_id()", &[])
        .await
        .expect("read active commit");
    let Value::Text(commit_id) = &result.rows()[0].values()[0] else {
        panic!("active commit id should be text");
    };
    commit_id.clone()
}

async fn execute(session: &Lix<Memory>, sql: &str) {
    black_box(
        session
            .execute(sql, &[])
            .await
            .unwrap_or_else(|error| panic!("benchmark SQL failed: {error:?}\nSQL: {sql}")),
    );
}

fn insert_sql(rows: usize) -> String {
    let mut sql = String::from("INSERT INTO lix_key_value (key, value) VALUES ");
    for index in 0..rows {
        if index > 0 {
            sql.push(',');
        }
        write!(sql, "('diff-{index:05}', 'value-{index:05}')").expect("write benchmark SQL");
    }
    sql
}

criterion_group!(benches, diff_command_benches);
criterion_main!(benches);
