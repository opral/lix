mod common;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use plugin_json_v2::JsonPlugin;
use plugin_json_v2::exports::lix::plugin::api::Guest;

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
                        common::file_from_bytes(&before),
                        common::file_from_bytes(&after),
                    )
                },
                |(before_file, after_file)| {
                    let baseline = JsonPlugin::detect_changes(Vec::new(), before_file)
                        .expect("baseline detect_changes should succeed");
                    let before_state = common::active_state_from_changes(baseline);
                    let delta = JsonPlugin::detect_changes(before_state.clone(), after_file)
                        .expect("delta detect_changes should succeed");
                    let after_state = common::apply_changes_to_active_state(before_state, delta);
                    JsonPlugin::render(after_state).expect("render should succeed")
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_roundtrip_projection);
criterion_main!(benches);
