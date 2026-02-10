mod common;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use plugin_json_v2::{apply_changes, detect_changes};

fn bench_roundtrip_projection(c: &mut Criterion) {
    let mut group = c.benchmark_group("roundtrip_projection");
    group.sample_size(20);

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
                    let baseline = detect_changes(None, before_file.clone())
                        .expect("baseline detect_changes should succeed");
                    let delta = detect_changes(Some(before_file), after_file)
                        .expect("delta detect_changes should succeed");
                    let projection = common::merge_latest_state_rows(vec![baseline, delta]);
                    let seed = common::file_from_bytes("f1", "/x.json", br#"{"stale":"cache"}"#);
                    apply_changes(seed, projection).expect("apply_changes should succeed")
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_roundtrip_projection);
criterion_main!(benches);
