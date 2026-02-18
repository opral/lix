mod common;

use common::{apply_scenarios, file_from_bytes};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use std::time::Duration;
use text_plugin::apply_changes;

fn bench_apply_changes(c: &mut Criterion) {
    let scenarios = apply_scenarios();

    let mut group = c.benchmark_group("apply_changes");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(15));

    for scenario in scenarios {
        group.bench_function(scenario.name, |b| {
            b.iter_batched(
                || {
                    (
                        file_from_bytes("f1", "/yarn.lock", &scenario.base),
                        scenario.changes.clone(),
                    )
                },
                |(base, changes)| {
                    let reconstructed = apply_changes(base, changes)
                        .expect("apply_changes benchmark should succeed");
                    black_box(reconstructed);
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_apply_changes);
criterion_main!(benches);
