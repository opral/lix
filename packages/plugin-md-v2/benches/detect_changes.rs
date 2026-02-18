mod common;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use plugin_md_v2::{
    detect_changes, detect_changes_with_state_context, PluginActiveStateRow,
    PluginDetectStateContext, PluginEntityChange,
};

fn to_state_context(rows: &[PluginEntityChange]) -> PluginDetectStateContext {
    PluginDetectStateContext {
        active_state: Some(
            rows.iter()
                .map(|row| PluginActiveStateRow {
                    entity_id: row.entity_id.clone(),
                    schema_key: Some(row.schema_key.clone()),
                    schema_version: Some(row.schema_version.clone()),
                    snapshot_content: row.snapshot_content.clone(),
                    file_id: None,
                    plugin_key: None,
                    version_id: None,
                    change_id: None,
                    metadata: None,
                    created_at: None,
                    updated_at: None,
                })
                .collect::<Vec<_>>(),
        ),
    }
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
                        common::file_from_markdown("f1", "/doc.mdx", &before),
                        common::file_from_markdown("f1", "/doc.mdx", &after),
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
        let before_file = common::file_from_markdown("f1", "/doc.mdx", &before);
        let after_file = common::file_from_markdown("f1", "/doc.mdx", &after);
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
