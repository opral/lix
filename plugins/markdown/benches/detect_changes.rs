mod common;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use plugin_md_v2::exports::lix::plugin::api::{EntityState, Guest};
use plugin_md_v2::{DetectedChange, MarkdownPlugin};
use std::collections::BTreeMap;

fn active_state_from_changes(changes: Vec<DetectedChange>) -> Vec<EntityState> {
    apply_changes_to_active_state(Vec::new(), changes)
}

fn apply_changes_to_active_state(
    active_state: Vec<EntityState>,
    changes: Vec<DetectedChange>,
) -> Vec<EntityState> {
    let mut rows = active_state
        .into_iter()
        .map(|row| ((row.schema_key.clone(), row.entity_pk.clone()), row))
        .collect::<BTreeMap<_, _>>();

    for change in changes {
        let key = (change.schema_key.clone(), change.entity_pk.clone());
        match change.snapshot_content {
            Some(snapshot_content) => {
                rows.insert(
                    key,
                    EntityState {
                        entity_pk: change.entity_pk,
                        schema_key: change.schema_key,
                        snapshot_content,
                        metadata: change.metadata,
                    },
                );
            }
            None => {
                rows.remove(&key);
            }
        }
    }

    rows.into_values().collect()
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
                    let before_state = active_state_from_changes(
                        MarkdownPlugin::detect_changes(Vec::new(), before_file)
                            .expect("baseline detect_changes benchmark should succeed"),
                    );
                    MarkdownPlugin::detect_changes(before_state, after_file)
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
        let bootstrap = MarkdownPlugin::detect_changes(Vec::new(), before_file.clone())
            .expect("bootstrap detect_changes benchmark should succeed");
        let active_state = active_state_from_changes(bootstrap);

        group.bench_function(name, |b| {
            b.iter_batched(
                || (after_file.clone(), active_state.clone()),
                |(after_file, active_state)| {
                    MarkdownPlugin::detect_changes(active_state, after_file)
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
