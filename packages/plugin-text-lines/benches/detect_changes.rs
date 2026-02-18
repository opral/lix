mod common;

use common::{detect_scenarios, file_from_bytes};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use plugin_text_lines::detect_changes;
use std::time::Duration;

fn bench_detect_changes(c: &mut Criterion) {
    let scenarios = detect_scenarios();

    let mut group = c.benchmark_group("detect_changes");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(15));

    for scenario in scenarios {
        group.bench_function(scenario.name, |b| {
            b.iter_batched(
                || {
                    let before = scenario
                        .before
                        .as_ref()
                        .map(|bytes| file_from_bytes("f1", "/yarn.lock", bytes));
                    let after = file_from_bytes("f1", "/yarn.lock", &scenario.after);
                    (before, after)
                },
                |(before, after)| {
                    let changes = detect_changes(before, after)
                        .expect("detect_changes benchmark should succeed");
                    black_box(changes);
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_detect_changes);
criterion_main!(benches);
