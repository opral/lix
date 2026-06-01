mod common;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use plugin_md_v2::{DetectedChange, detect_changes, detect_changes_with_state_context};

fn to_state_context(rows: &[DetectedChange]) -> Vec<DetectedChange> {
    rows.to_vec()
}

fn bench_detect_changes(c: &mut Criterion) {
    let mut group = c.benchmark_group("detect_changes");
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
                        common::file_from_markdown(&before),
                        common::file_from_markdown(&after),
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

fn bench_detect_changes_with_state_context(c: &mut Criterion) {
    let mut group = c.benchmark_group("detect_changes_with_state_context");
    group.sample_size(20);

    for (name, (before, after)) in [
        ("medium", common::dataset_medium()),
        ("large", common::dataset_large()),
    ] {
        let before_file = common::file_from_markdown(&before);
        let after_file = common::file_from_markdown(&after);
        let bootstrap = detect_changes(None, before_file.clone())
            .expect("bootstrap detect_changes benchmark should succeed");
        let state_context = to_state_context(&bootstrap);

        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    (
                        before_file.clone(),
                        after_file.clone(),
                        state_context.clone(),
                    )
                },
                |(before_file, after_file, state_context)| {
                    detect_changes_with_state_context(
                        Some(before_file),
                        after_file,
                        Some(state_context),
                    )
                    .expect("detect_changes_with_state_context benchmark should succeed")
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_detect_changes,
    bench_detect_changes_with_state_context
);
criterion_main!(benches);
