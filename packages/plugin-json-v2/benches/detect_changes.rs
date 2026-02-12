mod common;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use plugin_json_v2::detect_changes;

fn bench_detect_changes(c: &mut Criterion) {
    let mut group = c.benchmark_group("detect_changes");
    group.sample_size(30);

    for (name, (before, after)) in [
        ("small", common::dataset_small()),
        ("medium", common::dataset_medium()),
        ("large", common::dataset_large()),
    ] {
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    (
                        common::file_from_bytes("f1", "/x.json", &before),
                        common::file_from_bytes("f1", "/x.json", &after),
                    )
                },
                |(before_file, after_file)| {
                    detect_changes(Some(before_file), after_file)
                        .expect("detect_changes benchmark should succeed")
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_detect_changes);
criterion_main!(benches);
