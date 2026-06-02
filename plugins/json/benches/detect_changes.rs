mod common;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use plugin_json_v2::JsonPlugin;
use plugin_json_v2::exports::lix::plugin::api::Guest;

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
                    let before_state = common::active_state_from_changes(
                        JsonPlugin::detect_changes(Vec::new(), before_file)
                            .expect("baseline detect_changes benchmark should succeed"),
                    );
                    JsonPlugin::detect_changes(before_state, after_file)
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
