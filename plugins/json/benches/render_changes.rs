mod common;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use plugin_json_v2::JsonPlugin;
use plugin_json_v2::exports::lix::plugin::api::Guest;

fn bench_render_changes(c: &mut Criterion) {
    let mut group = c.benchmark_group("render_changes");
    group.sample_size(30);

    for (name, (before, after)) in [
        ("small", common::dataset_small()),
        ("medium", common::dataset_medium()),
        ("large", common::dataset_large()),
    ] {
        let active_state = common::active_state_for_transition(&before, &after);

        group.bench_function(name, |b| {
            b.iter_batched(
                || active_state.clone(),
                |rows| JsonPlugin::render(rows).expect("render benchmark should succeed"),
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_render_changes);
criterion_main!(benches);
