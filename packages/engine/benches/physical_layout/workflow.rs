use std::sync::Arc;
use std::time::Duration;

use criterion::{black_box, BatchSize, Criterion};
use lix_engine::storage_bench::{self, StorageBenchConfig};
use lix_engine::Backend;
use tokio::runtime::Runtime;

use crate::{Args, RocksDbBenchBackend, SqliteBenchBackend};

type BackendFactory = fn() -> Arc<dyn Backend + Send + Sync>;

#[derive(Clone, Copy)]
struct BackendProfile {
    name: &'static str,
    create: BackendFactory,
}

pub(crate) fn bench(c: &mut Criterion, runtime: &Runtime, args: Args) {
    for profile in physical_backends() {
        bench_smoke(c, runtime, args, profile);
        bench_fast(c, runtime, args, profile);
        bench_full(c, runtime, args, profile);
    }
}

fn bench_smoke(c: &mut Criterion, runtime: &Runtime, args: Args, profile: BackendProfile) {
    let smoke = args
        .config()
        .with_rows(1_000)
        .with_state_payload_bytes(1024);
    let mut group = c.benchmark_group(format!("physical_layout/workflow/smoke/{}", profile.name));
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(250));
    group.measurement_time(Duration::from_secs(1));

    group.bench_function("insert_tracked_commit_payload_1k/1k", |b| {
        b.iter_batched(
            || prepare_insert_tracked_commit(runtime, smoke, profile),
            |fixture| {
                black_box(
                    runtime
                        .block_on(run_insert_tracked_commit(fixture))
                        .expect("physical_layout/workflow smoke insert succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("update_tracked_commit_1pct_payload_1k/1k", |b| {
        b.iter_batched(
            || prepare_update_tracked_commit(runtime, smoke, profile, 10),
            |fixture| {
                black_box(
                    runtime
                        .block_on(run_update_tracked_commit(fixture))
                        .expect("physical_layout/workflow smoke update succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("diff_update_1pct_payload_1k/1k", |b| {
        b.iter_batched(
            || prepare_diff_update(runtime, smoke, profile, 10),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_diff_commits_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/workflow smoke diff succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("select_tracked_commit_point_hit_payload_1k/1k", |b| {
        b.iter_batched(
            || prepare_select_tracked_commit(runtime, smoke, profile),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_read_point_hit_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/workflow smoke point select succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("select_tracked_commit_headers_only_payload_1k/1k", |b| {
        b.iter_batched(
            || prepare_select_tracked_commit(runtime, smoke, profile),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_scan_headers_only_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/workflow smoke header select succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("select_tracked_commit_full_rows_payload_1k/1k", |b| {
        b.iter_batched(
            || prepare_select_tracked_commit(runtime, smoke, profile),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_scan_full_rows_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/workflow smoke full-row select succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function(
        "select_tracked_commit_file_selective_10pct_payload_1k/1k",
        |b| {
            b.iter_batched(
                || {
                    prepare_select_tracked_commit_file_selective(
                        runtime,
                        smoke.with_selectivity(storage_bench::StorageBenchSelectivity::Percent10),
                        profile,
                    )
                },
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(
                                storage_bench::tracked_state_scan_file_header_selective_prepared(
                                    &backend, &fixture,
                                ),
                            )
                            .expect(
                                "physical_layout/workflow smoke file-selective select succeeds",
                            ),
                    )
                },
                BatchSize::LargeInput,
            )
        },
    );

    group.bench_function("select_after_1pct_update_payload_1k/1k", |b| {
        b.iter_batched(
            || prepare_select_after_update(runtime, smoke, profile, 10),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_scan_full_rows_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/workflow smoke select-after-update succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("select_delta_chain_10x1pct_payload_1k/1k", |b| {
        b.iter_batched(
            || prepare_select_delta_chain(runtime, smoke, profile, 10, 10),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_scan_full_rows_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/workflow smoke select delta chain succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("select_materialized_delta_chain_10x1pct_payload_1k/1k", |b| {
        b.iter_batched(
            || prepare_select_materialized_delta_chain(runtime, smoke, profile, 10, 10),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_scan_full_rows_prepared(
                            &backend, &fixture,
                        ))
                        .expect(
                            "physical_layout/workflow smoke select materialized delta chain succeeds",
                        ),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("diff_delta_chain_10x1pct_payload_1k/1k", |b| {
        b.iter_batched(
            || prepare_diff_delta_chain(runtime, smoke, profile, 10, 10),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_diff_commits_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/workflow smoke diff delta chain succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("materialize_delta_chain_10x1pct_payload_1k/1k", |b| {
        b.iter_batched(
            || prepare_materialize_delta_chain(runtime, smoke, profile, 10, 10),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_materialize_root_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/workflow smoke materialize delta chain succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

fn bench_fast(c: &mut Criterion, runtime: &Runtime, args: Args, profile: BackendProfile) {
    let mut group = c.benchmark_group(format!("physical_layout/workflow/fast/{}", profile.name));

    group.bench_function("insert_tracked_commit_payload_1k/10k", |b| {
        b.iter_batched(
            || {
                prepare_insert_tracked_commit(
                    runtime,
                    args.config().with_state_payload_bytes(1024),
                    profile,
                )
            },
            |fixture| {
                black_box(
                    runtime
                        .block_on(run_insert_tracked_commit(fixture))
                        .expect("physical_layout/workflow insert tracked commit succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("update_tracked_commit_1pct/10k", |b| {
        b.iter_batched(
            || prepare_update_tracked_commit(runtime, args.config(), profile, args.rows / 100),
            |fixture| {
                black_box(
                    runtime
                        .block_on(run_update_tracked_commit(fixture))
                        .expect("physical_layout/workflow update tracked commit succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("diff_update_1pct/10k", |b| {
        b.iter_batched(
            || prepare_diff_update(runtime, args.config(), profile, args.rows / 100),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_diff_commits_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/workflow diff update succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("select_tracked_commit_point_hit/10k", |b| {
        b.iter_batched(
            || prepare_select_tracked_commit(runtime, args.config(), profile),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_read_point_hit_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/workflow point select succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("select_tracked_commit_headers_only/10k", |b| {
        b.iter_batched(
            || prepare_select_tracked_commit(runtime, args.config(), profile),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_scan_headers_only_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/workflow header select succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("select_tracked_commit_full_rows/10k", |b| {
        b.iter_batched(
            || prepare_select_tracked_commit(runtime, args.config(), profile),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_scan_full_rows_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/workflow full-row select succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("select_after_1pct_update/10k", |b| {
        b.iter_batched(
            || prepare_select_after_update(runtime, args.config(), profile, args.rows / 100),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_scan_full_rows_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/workflow select-after-update succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("select_delta_chain_10x1pct/10k", |b| {
        b.iter_batched(
            || prepare_select_delta_chain(runtime, args.config(), profile, 10, args.rows / 100),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_scan_full_rows_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/workflow select delta chain succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("diff_delta_chain_10x1pct/10k", |b| {
        b.iter_batched(
            || prepare_diff_delta_chain(runtime, args.config(), profile, 10, args.rows / 100),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(storage_bench::tracked_state_diff_commits_prepared(
                            &backend, &fixture,
                        ))
                        .expect("physical_layout/workflow diff delta chain succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

fn bench_full(c: &mut Criterion, runtime: &Runtime, args: Args, profile: BackendProfile) {
    let mut group = c.benchmark_group(format!("physical_layout/workflow/full/{}", profile.name));

    for (name, config) in [
        ("insert_tracked_commit_no_payload/10k", args.config()),
        (
            "insert_tracked_commit_payload_1k/10k",
            args.config().with_state_payload_bytes(1024),
        ),
    ] {
        group.bench_function(name, |b| {
            b.iter_batched(
                || prepare_insert_tracked_commit(runtime, config, profile),
                |fixture| {
                    black_box(
                        runtime
                            .block_on(run_insert_tracked_commit(fixture))
                            .expect("physical_layout/workflow full insert succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }

    for (name, changed_rows, tombstone) in [
        ("update_tracked_commit_1pct/10k", args.rows / 100, false),
        ("update_tracked_commit_10pct/10k", args.rows / 10, false),
        ("delete_tracked_commit_10pct/10k", args.rows / 10, true),
    ] {
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    if tombstone {
                        prepare_delete_tracked_commit(runtime, args.config(), profile, changed_rows)
                    } else {
                        prepare_update_tracked_commit(runtime, args.config(), profile, changed_rows)
                    }
                },
                |fixture| {
                    black_box(
                        runtime
                            .block_on(run_update_tracked_commit(fixture))
                            .expect("physical_layout/workflow full update/delete succeeds"),
                    )
                },
                BatchSize::LargeInput,
            )
        });
    }

    group.finish();
}

struct InsertTrackedCommitFixture {
    backend: Arc<dyn Backend + Send + Sync>,
    changelog: storage_bench::ChangelogAppendFixture,
    tracked_state: storage_bench::TrackedStateWriteRootFixture,
}

struct UpdateTrackedCommitFixture {
    backend: Arc<dyn Backend + Send + Sync>,
    changelog: storage_bench::ChangelogAppendFixture,
    tracked_state: storage_bench::TrackedStateUpdateFixture,
}

async fn run_insert_tracked_commit(
    fixture: InsertTrackedCommitFixture,
) -> Result<
    (
        storage_bench::StorageBenchReport,
        storage_bench::StorageBenchReport,
    ),
    lix_engine::LixError,
> {
    let changelog =
        storage_bench::changelog_append_changes_prepared(&fixture.backend, &fixture.changelog)
            .await?;
    let tracked_state =
        storage_bench::tracked_state_write_root_prepared(&fixture.backend, &fixture.tracked_state)
            .await?;
    Ok((changelog, tracked_state))
}

async fn run_update_tracked_commit(
    fixture: UpdateTrackedCommitFixture,
) -> Result<
    (
        storage_bench::StorageBenchReport,
        storage_bench::StorageBenchReport,
    ),
    lix_engine::LixError,
> {
    let changelog =
        storage_bench::changelog_append_changes_prepared(&fixture.backend, &fixture.changelog)
            .await?;
    let tracked_state = storage_bench::tracked_state_update_existing_prepared(
        &fixture.backend,
        &fixture.tracked_state,
    )
    .await?;
    Ok((changelog, tracked_state))
}

fn prepare_insert_tracked_commit(
    runtime: &Runtime,
    config: StorageBenchConfig,
    profile: BackendProfile,
) -> InsertTrackedCommitFixture {
    let backend = (profile.create)();
    let changelog = runtime
        .block_on(storage_bench::prepare_changelog_append_changes(config))
        .expect("prepare physical_layout/workflow insert changelog");
    let tracked_state = runtime
        .block_on(storage_bench::prepare_tracked_state_write_root(config))
        .expect("prepare physical_layout/workflow insert tracked_state");
    InsertTrackedCommitFixture {
        backend,
        changelog,
        tracked_state,
    }
}

fn prepare_update_tracked_commit(
    runtime: &Runtime,
    config: StorageBenchConfig,
    profile: BackendProfile,
    changed_rows: usize,
) -> UpdateTrackedCommitFixture {
    let backend = (profile.create)();
    let changelog = runtime
        .block_on(storage_bench::prepare_changelog_append_changes(
            config.with_rows(changed_rows),
        ))
        .expect("prepare physical_layout/workflow update changelog");
    let tracked_state = runtime
        .block_on(storage_bench::prepare_tracked_state_update_rows(
            &backend,
            config,
            changed_rows,
        ))
        .expect("prepare physical_layout/workflow update tracked_state");
    UpdateTrackedCommitFixture {
        backend,
        changelog,
        tracked_state,
    }
}

fn prepare_delete_tracked_commit(
    runtime: &Runtime,
    config: StorageBenchConfig,
    profile: BackendProfile,
    changed_rows: usize,
) -> UpdateTrackedCommitFixture {
    let backend = (profile.create)();
    let changelog = runtime
        .block_on(storage_bench::prepare_changelog_append_tombstones(
            config.with_rows(changed_rows),
        ))
        .expect("prepare physical_layout/workflow delete changelog");
    let tracked_state = runtime
        .block_on(storage_bench::prepare_tracked_state_tombstone_rows(
            &backend,
            config,
            changed_rows,
        ))
        .expect("prepare physical_layout/workflow delete tracked_state");
    UpdateTrackedCommitFixture {
        backend,
        changelog,
        tracked_state,
    }
}

fn prepare_diff_update(
    runtime: &Runtime,
    config: StorageBenchConfig,
    profile: BackendProfile,
    changed_rows: usize,
) -> (
    Arc<dyn Backend + Send + Sync>,
    storage_bench::TrackedStateDiffFixture,
) {
    let backend = (profile.create)();
    let fixture = runtime
        .block_on(storage_bench::prepare_tracked_state_diff_update_rows(
            &backend,
            config,
            changed_rows,
        ))
        .expect("prepare physical_layout/workflow diff update");
    (backend, fixture)
}

fn prepare_select_tracked_commit(
    runtime: &Runtime,
    config: StorageBenchConfig,
    profile: BackendProfile,
) -> (
    Arc<dyn Backend + Send + Sync>,
    storage_bench::TrackedStateReadFixture,
) {
    let backend = (profile.create)();
    let fixture = runtime
        .block_on(storage_bench::prepare_tracked_state_read(&backend, config))
        .expect("prepare physical_layout/workflow select tracked commit");
    (backend, fixture)
}

fn prepare_select_tracked_commit_file_selective(
    runtime: &Runtime,
    config: StorageBenchConfig,
    profile: BackendProfile,
) -> (
    Arc<dyn Backend + Send + Sync>,
    storage_bench::TrackedStateReadFixture,
) {
    let backend = (profile.create)();
    let fixture = runtime
        .block_on(storage_bench::prepare_tracked_state_read_file_selective(
            &backend, config,
        ))
        .expect("prepare physical_layout/workflow file-selective select");
    (backend, fixture)
}

fn prepare_select_after_update(
    runtime: &Runtime,
    config: StorageBenchConfig,
    profile: BackendProfile,
    changed_rows: usize,
) -> (
    Arc<dyn Backend + Send + Sync>,
    storage_bench::TrackedStateReadFixture,
) {
    let backend = (profile.create)();
    let fixture = runtime
        .block_on(storage_bench::prepare_tracked_state_read_after_update_rows(
            &backend,
            config,
            changed_rows,
        ))
        .expect("prepare physical_layout/workflow select after update");
    (backend, fixture)
}

fn prepare_select_delta_chain(
    runtime: &Runtime,
    config: StorageBenchConfig,
    profile: BackendProfile,
    delta_commits: usize,
    updated_rows_per_commit: usize,
) -> (
    Arc<dyn Backend + Send + Sync>,
    storage_bench::TrackedStateReadFixture,
) {
    let backend = (profile.create)();
    let fixture = runtime
        .block_on(storage_bench::prepare_tracked_state_read_delta_chain(
            &backend,
            config,
            delta_commits,
            updated_rows_per_commit,
        ))
        .expect("prepare physical_layout/workflow select delta chain");
    (backend, fixture)
}

fn prepare_select_materialized_delta_chain(
    runtime: &Runtime,
    config: StorageBenchConfig,
    profile: BackendProfile,
    delta_commits: usize,
    updated_rows_per_commit: usize,
) -> (
    Arc<dyn Backend + Send + Sync>,
    storage_bench::TrackedStateReadFixture,
) {
    let backend = (profile.create)();
    let fixture = runtime
        .block_on(
            storage_bench::prepare_tracked_state_read_materialized_delta_chain(
                &backend,
                config,
                delta_commits,
                updated_rows_per_commit,
            ),
        )
        .expect("prepare physical_layout/workflow select materialized delta chain");
    (backend, fixture)
}

fn prepare_diff_delta_chain(
    runtime: &Runtime,
    config: StorageBenchConfig,
    profile: BackendProfile,
    delta_commits: usize,
    updated_rows_per_commit: usize,
) -> (
    Arc<dyn Backend + Send + Sync>,
    storage_bench::TrackedStateDiffFixture,
) {
    let backend = (profile.create)();
    let fixture = runtime
        .block_on(storage_bench::prepare_tracked_state_diff_delta_chain(
            &backend,
            config,
            delta_commits,
            updated_rows_per_commit,
        ))
        .expect("prepare physical_layout/workflow diff delta chain");
    (backend, fixture)
}

fn prepare_materialize_delta_chain(
    runtime: &Runtime,
    config: StorageBenchConfig,
    profile: BackendProfile,
    delta_commits: usize,
    updated_rows_per_commit: usize,
) -> (
    Arc<dyn Backend + Send + Sync>,
    storage_bench::TrackedStateMaterializeFixture,
) {
    let backend = (profile.create)();
    let fixture = runtime
        .block_on(
            storage_bench::prepare_tracked_state_materialize_delta_chain(
                &backend,
                config,
                delta_commits,
                updated_rows_per_commit,
            ),
        )
        .expect("prepare physical_layout/workflow materialize delta chain");
    (backend, fixture)
}

fn physical_backends() -> [BackendProfile; 2] {
    [
        BackendProfile {
            name: "sqlite_tempfile",
            create: sqlite_tempfile_backend,
        },
        BackendProfile {
            name: "rocksdb_tempdir",
            create: rocksdb_backend,
        },
    ]
}

fn sqlite_tempfile_backend() -> Arc<dyn Backend + Send + Sync> {
    Arc::new(SqliteBenchBackend::tempfile().expect("create sqlite tempfile bench backend"))
}

fn rocksdb_backend() -> Arc<dyn Backend + Send + Sync> {
    Arc::new(RocksDbBenchBackend::new().expect("create rocksdb bench backend"))
}
