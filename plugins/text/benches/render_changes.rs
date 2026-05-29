mod common;

use common::{file_from_bytes, render_scenarios};
use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};
use std::time::Duration;
use text_plugin::render_changes;

fn bench_render_changes(c: &mut Criterion) {
    let scenarios = render_scenarios();

    let mut group = c.benchmark_group("render_changes");
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
                    let reconstructed = render_changes(base, changes)
                        .expect("render_changes benchmark should succeed");
                    black_box(reconstructed);
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_render_changes);
criterion_main!(benches);
