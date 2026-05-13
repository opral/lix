use criterion::{black_box, BatchSize, Criterion};
use lix_engine::changelog::bench as changelog_bench;
use lix_engine::changelog::bench::{
    BenchChangeProjection, BenchCommitProjection, BenchRebuildMode, BenchStore,
};
use tokio::runtime::Runtime;

use crate::backends::ChangelogBenchBackend;
use crate::config::{
    configure_corpus_group, configure_group, BenchShape, ChangeProjectionShape,
    CommitProjectionShape, CorpusShape, GcShape, HeavyCorpusShape, KeyLayoutShape,
    LookupBatchShape, MembershipFanoutShape, PayloadShape,
};
use crate::fixtures::{
    CorpusFixture, HeavyCorpusFixture, KeyLayoutFixture, MembershipFanoutFixture, PayloadFixture,
    SegmentFixture,
};
use crate::ops;

pub(crate) fn bench(c: &mut Criterion, runtime: &Runtime) {
    bench_single_segment(c, runtime);
    bench_corpus(c, runtime);
    bench_membership_fanout(c, runtime);
    bench_rebuild_modes(c, runtime);
    bench_gc_shapes(c, runtime);
    bench_payload_storage(c, runtime);
    bench_key_layout_storage(c, runtime);
    bench_heavy_corpus(c, runtime);
    bench_concurrent_read_pressure(c, runtime);
    bench_projection_modes(c, runtime);
    bench_lookup_batch_sizes(c, runtime);
}

fn bench_single_segment(c: &mut Criterion, runtime: &Runtime) {
    for backend in ChangelogBenchBackend::CI {
        for shape in BenchShape::ALL {
            let fixture = SegmentFixture::new(shape);
            let mut group = c.benchmark_group(format!(
                "changelog/storage/{}/{}",
                backend.label(),
                shape.label()
            ));
            configure_group(&mut group, shape);

            group.bench_function("stage_segment_raw_no_indexes", |b| {
                b.iter_batched(
                    || (backend.create(), fixture.clone()),
                    |(backend, fixture)| {
                        black_box(
                            runtime
                                .block_on(ops::stage_segment_raw(backend, &fixture))
                                .expect("stage raw changelog segment"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function("stage_segment", |b| {
                b.iter_batched(
                    || (backend.create(), fixture.clone()),
                    |(backend, fixture)| {
                        black_box(
                            runtime
                                .block_on(ops::stage_segment(backend, &fixture))
                                .expect("stage changelog segment"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function("stage_publish_commit", |b| {
                b.iter_batched(
                    || (backend.create(), fixture.clone()),
                    |(backend, fixture)| {
                        black_box(
                            runtime
                                .block_on(ops::stage_publish_commit(backend, &fixture))
                                .expect("stage changelog commit publication"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function("load_commits_physical_batched", |b| {
                b.iter_batched(
                    || {
                        let store = runtime
                            .block_on(ops::prepare_store(backend.create(), &fixture, false))
                            .expect("prepare physical changelog store");
                        (store, fixture.clone())
                    },
                    |(store, fixture)| {
                        black_box(
                            runtime
                                .block_on(ops::load_n_commits_physical(&store, &fixture))
                                .expect("load physical changelog commits"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function("load_commits_visible_batched", |b| {
                b.iter_batched(
                    || {
                        let store = runtime
                            .block_on(ops::prepare_store(backend.create(), &fixture, true))
                            .expect("prepare visible changelog store");
                        (store, fixture.clone())
                    },
                    |(store, fixture)| {
                        black_box(
                            runtime
                                .block_on(ops::load_n_commits_visible(&store, &fixture))
                                .expect("load visible changelog commits"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function("load_changes_physical_batched", |b| {
                b.iter_batched(
                    || {
                        let store = runtime
                            .block_on(ops::prepare_store(backend.create(), &fixture, false))
                            .expect("prepare physical changelog store");
                        (store, fixture.clone())
                    },
                    |(store, fixture)| {
                        black_box(
                            runtime
                                .block_on(ops::load_n_changes_physical(&store, &fixture))
                                .expect("load physical changelog changes"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function("load_changes_visible_batched", |b| {
                b.iter_batched(
                    || {
                        let store = runtime
                            .block_on(ops::prepare_store(backend.create(), &fixture, true))
                            .expect("prepare visible changelog store");
                        (store, fixture.clone())
                    },
                    |(store, fixture)| {
                        black_box(
                            runtime
                                .block_on(ops::load_n_changes_visible(&store, &fixture))
                                .expect("load visible changelog changes"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function("rebuild_mandatory_indexes", |b| {
                b.iter_batched(
                    || {
                        runtime
                            .block_on(ops::prepare_store(backend.create(), &fixture, false))
                            .expect("prepare changelog store for rebuild")
                    },
                    |store| {
                        black_box(
                            runtime
                                .block_on(ops::rebuild_indexes(&store))
                                .expect("rebuild changelog mandatory indexes"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function("plan_gc", |b| {
                b.iter_batched(
                    || {
                        let store = runtime
                            .block_on(ops::prepare_store(backend.create(), &fixture, true))
                            .expect("prepare changelog store for gc plan");
                        (store, fixture.clone())
                    },
                    |(store, fixture)| {
                        black_box(
                            runtime
                                .block_on(ops::plan_gc(&store, &fixture))
                                .expect("plan changelog gc"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function("collect_garbage", |b| {
                b.iter_batched(
                    || {
                        let store = runtime
                            .block_on(ops::prepare_store(backend.create(), &fixture, true))
                            .expect("prepare changelog store for gc");
                        (store, fixture.clone())
                    },
                    |(store, fixture)| {
                        black_box(
                            runtime
                                .block_on(ops::collect_garbage(&store, &fixture))
                                .expect("collect changelog garbage"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.finish();
        }
    }
}

fn bench_corpus(c: &mut Criterion, runtime: &Runtime) {
    for backend in ChangelogBenchBackend::CI {
        for shape in CorpusShape::CI {
            let fixture = CorpusFixture::new(shape);
            let mut group = c.benchmark_group(format!(
                "changelog/storage/{}/corpus/{}",
                backend.label(),
                shape.label()
            ));
            configure_corpus_group(&mut group, shape);

            group.bench_function("stage_segments_raw_no_indexes", |b| {
                b.iter_batched(
                    || (backend.create(), fixture.clone()),
                    |(backend, fixture)| {
                        black_box(
                            runtime
                                .block_on(ops::stage_corpus_raw(backend, &fixture))
                                .expect("stage raw changelog corpus"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function("stage_segments", |b| {
                b.iter_batched(
                    || (backend.create(), fixture.clone()),
                    |(backend, fixture)| {
                        black_box(
                            runtime
                                .block_on(ops::stage_corpus(backend, &fixture))
                                .expect("stage changelog corpus"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function("append_one_segment_raw_no_indexes", |b| {
                b.iter_batched(
                    || (backend.create(), fixture.clone()),
                    |(backend, fixture)| {
                        black_box(
                            runtime
                                .block_on(ops::stage_incremental_segment_raw(backend, &fixture))
                                .expect("append raw changelog segment to existing corpus"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function("append_one_segment_indexed", |b| {
                b.iter_batched(
                    || (backend.create(), fixture.clone()),
                    |(backend, fixture)| {
                        black_box(
                            runtime
                                .block_on(ops::stage_incremental_segment(backend, &fixture))
                                .expect("append indexed changelog segment to existing corpus"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function("scan_segments_decode", |b| {
                b.iter_batched(
                    || {
                        runtime
                            .block_on(ops::prepare_corpus_store(backend.create(), &fixture, false))
                            .expect("prepare corpus for segment iteration")
                    },
                    |store| {
                        black_box(
                            runtime
                                .block_on(ops::scan_segments_decode(&store))
                                .expect("scan and decode changelog segments"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function("publish_all_commits", |b| {
                b.iter_batched(
                    || (backend.create(), fixture.clone()),
                    |(backend, fixture)| {
                        black_box(
                            runtime
                                .block_on(ops::stage_publish_all_commits(backend, &fixture))
                                .expect("publish all changelog corpus commits"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function("by_commit_index_lookup", |b| {
                b.iter_batched(
                    || {
                        let store = runtime
                            .block_on(ops::prepare_corpus_store(backend.create(), &fixture, false))
                            .expect("prepare corpus for by_commit lookup");
                        (store, fixture.commit_ids.clone())
                    },
                    |(store, commit_ids)| {
                        black_box(
                            runtime
                                .block_on(ops::lookup_by_commit_index(&store, &commit_ids))
                                .expect("lookup changelog by_commit index"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function("by_change_index_lookup", |b| {
                b.iter_batched(
                    || {
                        let store = runtime
                            .block_on(ops::prepare_corpus_store(backend.create(), &fixture, false))
                            .expect("prepare corpus for by_change lookup");
                        (store, fixture.change_ids.clone())
                    },
                    |(store, change_ids)| {
                        black_box(
                            runtime
                                .block_on(ops::lookup_by_change_index(&store, &change_ids))
                                .expect("lookup changelog by_change index"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function("load_changes_physical_same_segment", |b| {
                b.iter_batched(
                    || {
                        let store = runtime
                            .block_on(ops::prepare_corpus_store(backend.create(), &fixture, false))
                            .expect("prepare corpus for same-segment physical changes");
                        (store, fixture.first_segment_change_ids.clone())
                    },
                    |(store, change_ids)| {
                        black_box(
                            runtime
                                .block_on(ops::load_corpus_changes_physical(&store, &change_ids))
                                .expect("load same-segment physical changes"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function("load_commits_physical_same_segment", |b| {
                b.iter_batched(
                    || {
                        let store = runtime
                            .block_on(ops::prepare_corpus_store(backend.create(), &fixture, false))
                            .expect("prepare corpus for same-segment physical commits");
                        (store, fixture.first_segment_commit_ids.clone())
                    },
                    |(store, commit_ids)| {
                        black_box(
                            runtime
                                .block_on(ops::load_corpus_commits_physical(&store, &commit_ids))
                                .expect("load same-segment physical commits"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function("load_commits_physical_scattered_segments", |b| {
                b.iter_batched(
                    || {
                        let store = runtime
                            .block_on(ops::prepare_corpus_store(backend.create(), &fixture, false))
                            .expect("prepare corpus for scattered physical commits");
                        (store, fixture.commit_ids.clone())
                    },
                    |(store, commit_ids)| {
                        black_box(
                            runtime
                                .block_on(ops::load_corpus_commits_physical(&store, &commit_ids))
                                .expect("load scattered physical commits"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function("load_changes_physical_scattered_segments", |b| {
                b.iter_batched(
                    || {
                        let store = runtime
                            .block_on(ops::prepare_corpus_store(backend.create(), &fixture, false))
                            .expect("prepare corpus for scattered physical changes");
                        (store, fixture.change_ids.clone())
                    },
                    |(store, change_ids)| {
                        black_box(
                            runtime
                                .block_on(ops::load_corpus_changes_physical(&store, &change_ids))
                                .expect("load scattered physical changes"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function("load_changes_visible_same_segment", |b| {
                b.iter_batched(
                    || {
                        let store = runtime
                            .block_on(ops::prepare_corpus_store(backend.create(), &fixture, true))
                            .expect("prepare corpus for same-segment visible changes");
                        (store, fixture.first_segment_change_ids.clone())
                    },
                    |(store, change_ids)| {
                        black_box(
                            runtime
                                .block_on(ops::load_corpus_changes_visible(&store, &change_ids))
                                .expect("load same-segment visible changes"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function("load_commits_visible_same_segment", |b| {
                b.iter_batched(
                    || {
                        let store = runtime
                            .block_on(ops::prepare_corpus_store(backend.create(), &fixture, true))
                            .expect("prepare corpus for same-segment visible commits");
                        (store, fixture.first_segment_commit_ids.clone())
                    },
                    |(store, commit_ids)| {
                        black_box(
                            runtime
                                .block_on(ops::load_corpus_commits_visible(&store, &commit_ids))
                                .expect("load same-segment visible commits"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function("load_commits_visible_scattered_segments", |b| {
                b.iter_batched(
                    || {
                        let store = runtime
                            .block_on(ops::prepare_corpus_store(backend.create(), &fixture, true))
                            .expect("prepare corpus for scattered visible commits");
                        (store, fixture.commit_ids.clone())
                    },
                    |(store, commit_ids)| {
                        black_box(
                            runtime
                                .block_on(ops::load_corpus_commits_visible(&store, &commit_ids))
                                .expect("load scattered visible commits"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function("load_changes_visible_scattered_segments", |b| {
                b.iter_batched(
                    || {
                        let store = runtime
                            .block_on(ops::prepare_corpus_store(backend.create(), &fixture, true))
                            .expect("prepare corpus for scattered visible changes");
                        (store, fixture.change_ids.clone())
                    },
                    |(store, change_ids)| {
                        black_box(
                            runtime
                                .block_on(ops::load_corpus_changes_visible(&store, &change_ids))
                                .expect("load scattered visible changes"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.finish();
        }
    }
}

fn bench_membership_fanout(c: &mut Criterion, runtime: &Runtime) {
    for backend in ChangelogBenchBackend::CI {
        let mut group = c.benchmark_group(format!(
            "changelog/storage/{}/by_change_membership_prefix",
            backend.label()
        ));
        group.sample_size(20);
        for shape in MembershipFanoutShape::CI {
            let fixture = MembershipFanoutFixture::new(shape);
            group.bench_function(shape.label(), |b| {
                b.iter_batched(
                    || (backend.create(), fixture.clone()),
                    |(backend, fixture)| {
                        black_box(
                            runtime
                                .block_on(ops::scan_membership_fanout(backend, &fixture))
                                .expect("scan changelog by_change_membership prefix"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });
        }
        group.finish();
    }
}

fn bench_rebuild_modes(c: &mut Criterion, runtime: &Runtime) {
    for backend in ChangelogBenchBackend::CI {
        let fixture = CorpusFixture::new(CorpusShape::TenSegments);
        let mut group = c.benchmark_group(format!(
            "changelog/storage/{}/rebuild_mandatory_indexes_modes",
            backend.label()
        ));
        group.sample_size(15);
        for (label, mode) in [
            ("noop", BenchRebuildMode::Noop),
            ("empty_indexes", BenchRebuildMode::EmptyIndexes),
            ("stale_extra_rows", BenchRebuildMode::StaleExtraRows),
            ("corrupt_values", BenchRebuildMode::CorruptValues),
        ] {
            group.bench_function(label, |b| {
                b.iter_batched(
                    || {
                        runtime
                            .block_on(ops::prepare_rebuild_store(backend.create(), &fixture, mode))
                            .expect("prepare changelog rebuild mode")
                    },
                    |store| {
                        black_box(
                            runtime
                                .block_on(ops::rebuild_indexes(&store))
                                .expect("rebuild changelog mandatory indexes"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });
        }
        group.finish();
    }
}

fn bench_gc_shapes(c: &mut Criterion, runtime: &Runtime) {
    for backend in ChangelogBenchBackend::CI {
        let mut group =
            c.benchmark_group(format!("changelog/storage/{}/gc_shapes", backend.label()));
        group.sample_size(10);
        for shape in GcShape::CI {
            let (live_segments, dead_segments) = shape.live_dead_segments();
            group.bench_function(format!("plan_gc/{}", shape.label()), |b| {
                b.iter_batched(
                    || {
                        runtime
                            .block_on(ops::prepare_gc_store(
                                backend.create(),
                                live_segments,
                                dead_segments,
                            ))
                            .expect("prepare changelog gc shape")
                    },
                    |(store, root_commit_id)| {
                        black_box(
                            runtime
                                .block_on(ops::plan_gc_root(&store, &root_commit_id))
                                .expect("plan changelog gc shape"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });
            group.bench_function(format!("collect_garbage/{}", shape.label()), |b| {
                b.iter_batched(
                    || {
                        runtime
                            .block_on(ops::prepare_gc_store(
                                backend.create(),
                                live_segments,
                                dead_segments,
                            ))
                            .expect("prepare changelog collect-garbage shape")
                    },
                    |(store, root_commit_id)| {
                        black_box(
                            runtime
                                .block_on(ops::collect_garbage_root(&store, &root_commit_id))
                                .expect("collect changelog garbage shape"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });
        }
        group.finish();
    }
}

fn bench_payload_storage(c: &mut Criterion, runtime: &Runtime) {
    for backend in ChangelogBenchBackend::CI {
        for shape in PayloadShape::CI {
            let fixture = PayloadFixture::new(shape);
            let mut group = c.benchmark_group(format!(
                "changelog/storage/{}/payload/{}",
                backend.label(),
                shape.label()
            ));
            group.sample_size(match shape {
                PayloadShape::LargeInline => 10,
                _ => 15,
            });

            group.bench_function("stage_segment", |b| {
                b.iter_batched(
                    || (backend.create(), fixture.clone()),
                    |(backend, fixture)| {
                        black_box(
                            runtime
                                .block_on(changelog_bench::stage_segment_once(
                                    backend,
                                    &fixture.segment,
                                ))
                                .expect("stage payload changelog segment"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function("load_changes_physical_batched", |b| {
                b.iter_batched(
                    || {
                        let store = runtime
                            .block_on(changelog_bench::prepare_store(
                                backend.create(),
                                &fixture.segment,
                                false,
                            ))
                            .expect("prepare payload physical store");
                        (store, fixture.change_ids.clone())
                    },
                    |(store, change_ids)| {
                        black_box(
                            runtime
                                .block_on(changelog_bench::load_changes_physical(
                                    &store,
                                    &change_ids,
                                ))
                                .expect("load physical payload changes"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function("load_changes_visible_batched", |b| {
                b.iter_batched(
                    || {
                        let store = runtime
                            .block_on(changelog_bench::prepare_store(
                                backend.create(),
                                &fixture.segment,
                                true,
                            ))
                            .expect("prepare payload visible store");
                        (store, fixture.change_ids.clone())
                    },
                    |(store, change_ids)| {
                        black_box(
                            runtime
                                .block_on(changelog_bench::load_changes_visible(
                                    &store,
                                    &change_ids,
                                ))
                                .expect("load visible payload changes"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.finish();
        }
    }
}

fn bench_key_layout_storage(c: &mut Criterion, runtime: &Runtime) {
    for backend in ChangelogBenchBackend::CI {
        for shape in KeyLayoutShape::CI {
            let fixture = KeyLayoutFixture::new(shape);
            let change_ids = fixture.segment.change_ids();
            let mut group = c.benchmark_group(format!(
                "changelog/storage/{}/key_layout/{}",
                backend.label(),
                shape.label()
            ));
            group.sample_size(15);

            group.bench_function("stage_segment", |b| {
                b.iter_batched(
                    || (backend.create(), fixture.clone()),
                    |(backend, fixture)| {
                        black_box(
                            runtime
                                .block_on(changelog_bench::stage_segment_once(
                                    backend,
                                    &fixture.segment,
                                ))
                                .expect("stage key-layout changelog segment"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function("load_changes_visible_batched", |b| {
                b.iter_batched(
                    || {
                        let store = runtime
                            .block_on(changelog_bench::prepare_store(
                                backend.create(),
                                &fixture.segment,
                                true,
                            ))
                            .expect("prepare key-layout visible store");
                        (store, change_ids.clone())
                    },
                    |(store, change_ids)| {
                        black_box(
                            runtime
                                .block_on(changelog_bench::load_changes_visible(
                                    &store,
                                    &change_ids,
                                ))
                                .expect("load visible key-layout changes"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.finish();
        }
    }
}

fn bench_heavy_corpus(c: &mut Criterion, runtime: &Runtime) {
    for backend in ChangelogBenchBackend::CI {
        for shape in HeavyCorpusShape::ALL {
            let fixture = HeavyCorpusFixture::new(shape);
            let mut group = c.benchmark_group(format!(
                "changelog/storage/{}/heavy_corpus/{}",
                backend.label(),
                shape.label()
            ));
            group.sample_size(10);

            group.bench_function("stage_segments", |b| {
                b.iter_batched(
                    || (backend.create(), fixture.clone()),
                    |(backend, fixture)| {
                        black_box(
                            runtime
                                .block_on(changelog_bench::stage_corpus_once(
                                    backend,
                                    &fixture.corpus,
                                ))
                                .expect("stage heavy changelog corpus"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function("by_change_index_lookup", |b| {
                b.iter_batched(
                    || {
                        let store = runtime
                            .block_on(changelog_bench::prepare_corpus_store(
                                backend.create(),
                                &fixture.corpus,
                                false,
                            ))
                            .expect("prepare heavy corpus for by_change lookup");
                        (store, fixture.change_ids.clone())
                    },
                    |(store, change_ids)| {
                        black_box(
                            runtime
                                .block_on(changelog_bench::lookup_by_change_index(
                                    &store,
                                    &change_ids,
                                ))
                                .expect("lookup heavy changelog by_change index"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function("load_changes_physical_scattered_segments", |b| {
                b.iter_batched(
                    || {
                        let store = runtime
                            .block_on(changelog_bench::prepare_corpus_store(
                                backend.create(),
                                &fixture.corpus,
                                false,
                            ))
                            .expect("prepare heavy corpus for physical change loads");
                        (store, fixture.change_ids.clone())
                    },
                    |(store, change_ids)| {
                        black_box(
                            runtime
                                .block_on(changelog_bench::load_changes_physical(
                                    &store,
                                    &change_ids,
                                ))
                                .expect("load heavy physical changelog changes"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            });

            group.finish();
        }
    }
}

fn bench_concurrent_read_pressure(c: &mut Criterion, runtime: &Runtime) {
    for backend in ChangelogBenchBackend::CI {
        let fixture = CorpusFixture::new(CorpusShape::HundredSegments);
        let mut group = c.benchmark_group(format!(
            "changelog/storage/{}/concurrent_read_pressure",
            backend.label()
        ));
        group.sample_size(10);
        group.bench_function("load_changes_visible_4_threads", |b| {
            b.iter_batched(
                || {
                    let store = runtime
                        .block_on(ops::prepare_corpus_store(backend.create(), &fixture, true))
                        .expect("prepare corpus for concurrent visible reads");
                    (store, fixture.change_ids.clone())
                },
                |(store, change_ids)| {
                    black_box(load_changes_visible_concurrent(store, change_ids, 4))
                },
                BatchSize::LargeInput,
            )
        });
        group.finish();
    }
}

fn load_changes_visible_concurrent(
    store: BenchStore,
    change_ids: Vec<String>,
    threads: usize,
) -> usize {
    let chunk_size = change_ids.len().div_ceil(threads.max(1));
    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for chunk in change_ids.chunks(chunk_size.max(1)) {
            let store = store.clone();
            let ids = chunk.to_vec();
            handles.push(scope.spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("create per-thread changelog bench runtime");
                runtime
                    .block_on(changelog_bench::load_changes_visible(&store, &ids))
                    .expect("load concurrent visible changelog changes")
            }));
        }
        handles
            .into_iter()
            .map(|handle| handle.join().expect("join changelog bench reader thread"))
            .sum()
    })
}

fn bench_projection_modes(c: &mut Criterion, runtime: &Runtime) {
    for backend in ChangelogBenchBackend::CI {
        let fixture = SegmentFixture::new(BenchShape::Medium);
        {
            let mut group = c.benchmark_group(format!(
                "changelog/storage/{}/projection/commits",
                backend.label()
            ));
            group.sample_size(15);
            for projection in CommitProjectionShape::CI {
                group.bench_function(format!("visible/{}", projection.label()), |b| {
                    b.iter_batched(
                        || {
                            let store = runtime
                                .block_on(ops::prepare_store(backend.create(), &fixture, true))
                                .expect("prepare projection commit store");
                            (store, fixture.commit_ids.clone())
                        },
                        |(store, commit_ids)| {
                            black_box(
                                runtime
                                    .block_on(
                                        changelog_bench::load_commits_visible_with_projection(
                                            &store,
                                            &commit_ids,
                                            bench_commit_projection(projection),
                                        ),
                                    )
                                    .expect("load visible commits by projection"),
                            )
                        },
                        BatchSize::LargeInput,
                    )
                });
                group.bench_function(format!("physical/{}", projection.label()), |b| {
                    b.iter_batched(
                        || {
                            let store = runtime
                                .block_on(ops::prepare_store(backend.create(), &fixture, false))
                                .expect("prepare projection physical commit store");
                            (store, fixture.commit_ids.clone())
                        },
                        |(store, commit_ids)| {
                            black_box(
                                runtime
                                    .block_on(
                                        changelog_bench::load_commits_physical_with_projection(
                                            &store,
                                            &commit_ids,
                                            bench_commit_projection(projection),
                                        ),
                                    )
                                    .expect("load physical commits by projection"),
                            )
                        },
                        BatchSize::LargeInput,
                    )
                });
            }
            group.finish();
        }

        {
            let mut group = c.benchmark_group(format!(
                "changelog/storage/{}/projection/changes",
                backend.label()
            ));
            group.sample_size(15);
            for projection in ChangeProjectionShape::CI {
                group.bench_function(format!("visible/{}", projection.label()), |b| {
                    b.iter_batched(
                        || {
                            let store = runtime
                                .block_on(ops::prepare_store(backend.create(), &fixture, true))
                                .expect("prepare projection visible change store");
                            (store, fixture.change_ids.clone())
                        },
                        |(store, change_ids)| {
                            black_box(
                                runtime
                                    .block_on(
                                        changelog_bench::load_changes_visible_with_projection(
                                            &store,
                                            &change_ids,
                                            bench_change_projection(projection),
                                        ),
                                    )
                                    .expect("load visible changes by projection"),
                            )
                        },
                        BatchSize::LargeInput,
                    )
                });
                group.bench_function(format!("physical/{}", projection.label()), |b| {
                    b.iter_batched(
                        || {
                            let store = runtime
                                .block_on(ops::prepare_store(backend.create(), &fixture, false))
                                .expect("prepare projection physical change store");
                            (store, fixture.change_ids.clone())
                        },
                        |(store, change_ids)| {
                            black_box(
                                runtime
                                    .block_on(
                                        changelog_bench::load_changes_physical_with_projection(
                                            &store,
                                            &change_ids,
                                            bench_change_projection(projection),
                                        ),
                                    )
                                    .expect("load physical changes by projection"),
                            )
                        },
                        BatchSize::LargeInput,
                    )
                });
            }
            group.finish();
        }
    }
}

fn bench_lookup_batch_sizes(c: &mut Criterion, runtime: &Runtime) {
    for backend in ChangelogBenchBackend::CI {
        let fixture = SegmentFixture::new(BenchShape::Medium);
        let mut group = c.benchmark_group(format!(
            "changelog/storage/{}/lookup_batch_size",
            backend.label()
        ));
        group.sample_size(15);
        for shape in LookupBatchShape::CI {
            let change_count = shape.len();
            group.bench_function(
                format!("visible_changes_same_segment/{}", shape.label()),
                |b| {
                    b.iter_batched(
                        || {
                            let store = runtime
                                .block_on(ops::prepare_store(backend.create(), &fixture, true))
                                .expect("prepare batch-size visible store");
                            (
                                store,
                                fixture
                                    .change_ids
                                    .iter()
                                    .take(change_count)
                                    .cloned()
                                    .collect::<Vec<_>>(),
                            )
                        },
                        |(store, change_ids)| {
                            black_box(
                                runtime
                                    .block_on(changelog_bench::load_changes_visible(
                                        &store,
                                        &change_ids,
                                    ))
                                    .expect("load visible changes at batch size"),
                            )
                        },
                        BatchSize::LargeInput,
                    )
                },
            );
            group.bench_function(
                format!("physical_changes_same_segment/{}", shape.label()),
                |b| {
                    b.iter_batched(
                        || {
                            let store = runtime
                                .block_on(ops::prepare_store(backend.create(), &fixture, false))
                                .expect("prepare batch-size physical store");
                            (
                                store,
                                fixture
                                    .change_ids
                                    .iter()
                                    .take(change_count)
                                    .cloned()
                                    .collect::<Vec<_>>(),
                            )
                        },
                        |(store, change_ids)| {
                            black_box(
                                runtime
                                    .block_on(changelog_bench::load_changes_physical(
                                        &store,
                                        &change_ids,
                                    ))
                                    .expect("load physical changes at batch size"),
                            )
                        },
                        BatchSize::LargeInput,
                    )
                },
            );
        }
        group.finish();
    }
}

fn bench_commit_projection(projection: CommitProjectionShape) -> BenchCommitProjection {
    match projection {
        CommitProjectionShape::Header => BenchCommitProjection::Header,
        CommitProjectionShape::Body => BenchCommitProjection::Body,
        CommitProjectionShape::Full => BenchCommitProjection::Full,
    }
}

fn bench_change_projection(projection: ChangeProjectionShape) -> BenchChangeProjection {
    match projection {
        ChangeProjectionShape::PhysicalLocation => BenchChangeProjection::PhysicalLocation,
        ChangeProjectionShape::Logical => BenchChangeProjection::Logical,
        ChangeProjectionShape::Segment => BenchChangeProjection::Segment,
    }
}
