use std::fmt::Write as _;
use std::future::Future;
use std::hint::black_box;
use std::time::{Duration, Instant};

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use lix::Value;
use lix::storage::Memory;
use lix::{Lix, open_lix};
use serde_json::json;

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
                runtime.block_on(execute(&session, "SELECT COUNT(*) FROM lix_working_diff"));
            },
            BatchSize::LargeInput,
        );
    });
    c.bench_function("diff_commands/revert_100_of_1000", |b| {
        b.iter_batched(
            || {
                runtime.block_on(async {
                    let session = seeded_session(1_000).await;
                    let sql = command_sql("lix_revert", "lix_working_diff", "", 100);
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
                    let sql = command_sql("lix_create_checkpoint", "lix_working_diff", "", 500);
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
        execute(&session, "SELECT COUNT(*) FROM lix_working_diff").await;
    })
    .await;
    emit("working_diff", rows, rows, sample, scan);

    let revert = elapsed(async {
        execute(
            &session,
            &format!(
                "INSERT INTO lix_revert (diff_id) \
                 SELECT diff_id FROM lix_working_diff \
                 WHERE schema_key = 'lix_key_value' \
                 ORDER BY entity_pk LIMIT {selected}"
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
                "INSERT INTO lix_apply (diff_id) \
                 SELECT diff_id FROM lix_diff('{baseline}', '{head}') \
                 WHERE schema_key = 'lix_key_value' \
                 ORDER BY entity_pk LIMIT {selected}"
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
                "INSERT INTO lix_create_checkpoint (diff_id) \
                 SELECT diff_id FROM lix_working_diff \
                 WHERE schema_key = 'lix_key_value' \
                 ORDER BY entity_pk LIMIT {checkpoint_selected}"
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
        &command_sql("lix_revert", "lix_working_diff", "", selected),
    )
    .await;
    let source = format!("lix_diff('{baseline}', '{head}')");
    (session, command_sql("lix_apply", &source, "", selected))
}

fn command_sql(command: &str, source: &str, extra_predicate: &str, selected: usize) -> String {
    format!(
        "INSERT INTO {command} (diff_id) \
         SELECT diff_id FROM {source} \
         WHERE schema_key = 'lix_key_value' {extra_predicate} \
         ORDER BY entity_pk LIMIT {selected}"
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
