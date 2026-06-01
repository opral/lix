mod common;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
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
                        common::file_from_bytes(&before),
                        common::file_from_bytes(&after),
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
