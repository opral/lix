use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use lix_sdk::{OpenLixOptions, Value, open_lix};

fn bench_branch_session(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("build benchmark runtime");
    let workspace = runtime
        .block_on(open_lix(OpenLixOptions::default()))
        .expect("open benchmark workspace");
    let main_branch_id = runtime
        .block_on(workspace.active_branch_id())
        .expect("load main branch id");
    let pinned = runtime
        .block_on(workspace.open_session(main_branch_id))
        .expect("open branch-pinned session");
    runtime
        .block_on(workspace.execute(
            "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
            &[
                Value::Text("/branch-session-bench.bin".to_string()),
                Value::Blob(vec![0x5a; 2_048]),
            ],
        ))
        .expect("seed benchmark file");

    let sql = "SELECT data FROM lix_file WHERE path = $1";
    let params = [Value::Text("/branch-session-bench.bin".to_string())];
    let mut group = c.benchmark_group("exact_file_read_2k");
    group.bench_function("workspace_selector", |b| {
        b.iter(|| {
            black_box(
                runtime
                    .block_on(workspace.execute(sql, &params))
                    .expect("read through workspace selector"),
            );
        });
    });
    group.bench_function("branch_pinned", |b| {
        b.iter(|| {
            black_box(
                runtime
                    .block_on(pinned.execute(sql, &params))
                    .expect("read through pinned branch"),
            );
        });
    });
    group.finish();
}

criterion_group!(benches, bench_branch_session);
criterion_main!(benches);
