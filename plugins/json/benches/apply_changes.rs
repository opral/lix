mod common;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use plugin_json_v2::apply_changes;

fn bench_apply_changes(c: &mut Criterion) {
    let mut group = c.benchmark_group("apply_changes");
    group.sample_size(30);

    for (name, (before, after)) in [
        ("small", common::dataset_small()),
        ("medium", common::dataset_medium()),
        ("large", common::dataset_large()),
    ] {
        let projection = common::projection_for_transition(&before, &after);
        let seed = common::file_from_bytes("f1", "/x.json", br#"{"stale":"cache"}"#);

        group.bench_function(name, |b| {
            b.iter_batched(
                || (seed.clone(), projection.clone()),
                |(seed_file, rows)| {
                    apply_changes(seed_file, rows).expect("apply_changes benchmark should succeed")
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_apply_changes);
criterion_main!(benches);
