use std::{cell::RefCell, future::IntoFuture, time::Instant};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use lix::{ExecuteBatchStatement, Memory, Value};
use lix::{Lix, open_lix};

fn seeded_storage(runtime: &tokio::runtime::Runtime, history_depth: usize) -> Memory {
    runtime.block_on(async move {
        let storage = Memory::new();
        let session = open_lix()
            .with_storage(storage.clone())
            .await
            .expect("benchmark lix opens");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('bench-key', '0')",
                &[],
            )
            .await
            .expect("benchmark history starts");
        for value in 1..history_depth {
            session
                .execute(
                    &format!("UPDATE lix_key_value SET value = '{value}' WHERE key = 'bench-key'"),
                    &[],
                )
                .await
                .expect("benchmark history commit succeeds");
        }
        drop(session);
        storage
    })
}

fn seeded_sparse_gap_storage(runtime: &tokio::runtime::Runtime, history_depth: usize) -> Memory {
    runtime.block_on(async move {
        let storage = Memory::new();
        let session = open_lix()
            .with_storage(storage.clone())
            .await
            .expect("benchmark lix opens");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('target-key', 'before'), ('noise-key', '0')",
                &[],
            )
            .await
            .expect("benchmark rows start");
        for value in 1..history_depth {
            session
                .execute(
                    &format!(
                        "UPDATE lix_key_value SET value = '{value}' WHERE key = 'noise-key'"
                    ),
                    &[],
                )
                .await
                .expect("benchmark unrelated history commit succeeds");
        }
        session
            .execute(
                "UPDATE lix_key_value SET value = 'after' WHERE key = 'target-key'",
                &[],
            )
            .await
            .expect("benchmark target update succeeds");
        drop(session);
        storage
    })
}

fn seeded_wide_parent_storage(runtime: &tokio::runtime::Runtime, parent_width: usize) -> Memory {
    runtime.block_on(async move {
        let storage = Memory::new();
        let session = open_lix()
            .with_storage(storage.clone())
            .await
            .expect("benchmark lix opens");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('target-key', 'before')",
                &[],
            )
            .await
            .expect("benchmark target starts");
        let values = (0..parent_width)
            .map(|index| format!("('noise-{index}', '{index}')"))
            .collect::<Vec<_>>()
            .join(",");
        session
            .execute(
                &format!("INSERT INTO lix_key_value (key, value) VALUES {values}"),
                &[],
            )
            .await
            .expect("wide parent commit succeeds");
        session
            .execute(
                "UPDATE lix_key_value SET value = 'after' WHERE key = 'target-key'",
                &[],
            )
            .await
            .expect("benchmark target update succeeds");
        drop(session);
        storage
    })
}

fn seeded_wide_transition_storage(
    runtime: &tokio::runtime::Runtime,
    transition_width: usize,
) -> Memory {
    runtime.block_on(async move {
        let storage = Memory::new();
        let session = open_lix()
            .with_storage(storage.clone())
            .await
            .expect("benchmark lix opens");
        let before = "b".repeat(256);
        let values = (0..transition_width)
            .map(|index| format!("('transition-{index}', '{before}')"))
            .collect::<Vec<_>>()
            .join(",");
        session
            .execute(
                &format!("INSERT INTO lix_key_value (key, value) VALUES {values}"),
                &[],
            )
            .await
            .expect("transition rows start");
        let after = "a".repeat(256);
        let updates = (0..transition_width)
            .map(|index| ExecuteBatchStatement {
                label: None,
                sql: format!(
                    "UPDATE lix_key_value SET value = '{after}' WHERE key = 'transition-{index}'"
                ),
                params: vec![],
            })
            .collect::<Vec<_>>();
        session
            .execute_batch(&updates)
            .await
            .expect("wide transition commit succeeds");
        drop(session);
        storage
    })
}

fn seeded_descriptor_unrelated_width_storage(
    runtime: &tokio::runtime::Runtime,
    unrelated_width: usize,
) -> Memory {
    runtime.block_on(async move {
        let storage = Memory::new();
        let session = open_lix()
            .with_storage(storage.clone())
            .await
            .expect("benchmark lix opens");
        session
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
                &[
                    Value::Text("/descriptor-target.txt".to_owned()),
                    Value::Blob(b"target".as_slice().into()),
                ],
            )
            .await
            .expect("target file creates");
        let values = (0..unrelated_width)
            .map(|index| format!("('descriptor-noise-{index}', '{index}')"))
            .collect::<Vec<_>>()
            .join(",");
        session
            .execute(
                &format!("INSERT INTO lix_key_value (key, value) VALUES {values}"),
                &[],
            )
            .await
            .expect("unrelated repository rows write");
        session
            .execute(
                "DELETE FROM lix_file WHERE path = '/descriptor-target.txt'",
                &[],
            )
            .await
            .expect("target file deletes");
        drop(session);
        storage
    })
}

fn seeded_checkpoint_inventory_storage(
    runtime: &tokio::runtime::Runtime,
    effect_width: usize,
) -> (Memory, String) {
    runtime.block_on(async move {
        let storage = Memory::new();
        let session = open_lix()
            .with_storage(storage.clone())
            .await
            .expect("benchmark lix opens");
        let values = (0..effect_width)
            .map(|index| format!("('checkpoint-{index}', '{index}')"))
            .collect::<Vec<_>>()
            .join(",");
        session
            .execute(
                &format!("INSERT INTO lix_key_value (key, value) VALUES {values}"),
                &[],
            )
            .await
            .expect("checkpoint inventory rows commit");
        let checkpoint = session
            .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
            .await
            .expect("checkpoint inventory checkpoint succeeds")
            .rows()[0]
            .get::<String>("commit_id")
            .expect("checkpoint inventory receipt");
        drop(session);
        (storage, checkpoint)
    })
}

fn open_session(runtime: &tokio::runtime::Runtime, storage: Memory) -> Lix<Memory> {
    runtime.block_on(async move {
        open_lix()
            .with_storage(storage)
            .await
            .expect("benchmark lix opens")
    })
}

thread_local! {
    static BENCH_TIMINGS: RefCell<Vec<(&'static str, u128)>> = const { RefCell::new(Vec::new()) };
}

fn timed_operation<T>(name: &'static str, operation: impl FnOnce() -> T) -> T {
    if std::env::var_os("LIX_BENCH_TIMINGS").is_none() {
        return operation();
    }
    let started = Instant::now();
    let output = operation();
    BENCH_TIMINGS.with(|timings| {
        timings
            .borrow_mut()
            .push((name, started.elapsed().as_nanos()))
    });
    output
}

fn dump_bench_timings() {
    if std::env::var_os("LIX_BENCH_TIMINGS").is_none() {
        return;
    }
    BENCH_TIMINGS.with(|timings| {
        let mut timings = timings.borrow_mut();
        timings.sort_unstable_by_key(|(name, nanos)| (*name, *nanos));
        let mut start = 0;
        while start < timings.len() {
            let name = timings[start].0;
            let end = timings[start..]
                .iter()
                .position(|(candidate, _)| *candidate != name)
                .map_or(timings.len(), |offset| start + offset);
            let values = &timings[start..end];
            let median = values[values.len() / 2].1;
            println!(
                "LIX_BENCH_TIMING name={name} samples={} min_ns={} median_ns={} max_ns={}",
                values.len(),
                values.first().expect("timing sample exists").1,
                median,
                values.last().expect("timing sample exists").1,
            );
            start = end;
        }
        timings.clear();
    });
}

fn benchmark_undo_redo(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime builds");
    let mut group = criterion.benchmark_group("undo_redo_repeated_identity_depth");
    for history_depth in [10_usize, 1_000] {
        let undo_snapshot = seeded_storage(&runtime, history_depth);
        group.bench_with_input(
            BenchmarkId::new("ordinary_update", history_depth),
            &history_depth,
            |benchmark, _| {
                benchmark.iter_with_setup(
                    || {
                        open_session(
                            &runtime,
                            undo_snapshot
                                .fork()
                                .expect("update benchmark fixture forks"),
                        )
                    },
                    |session| {
                        timed_operation("ordinary_update", || {
                            runtime
                                .block_on(
                                    session
                                        .execute(
                                            "UPDATE lix_key_value SET value = 'next' WHERE key = 'bench-key'",
                                            &[],
                                        )
                                        .into_future(),
                                )
                                .expect("benchmarked ordinary update succeeds");
                        });
                    },
                );
            },
        );
        group.bench_with_input(
            BenchmarkId::new("undo", history_depth),
            &history_depth,
            |benchmark, _| {
                benchmark.iter_with_setup(
                    || {
                        open_session(
                            &runtime,
                            undo_snapshot.fork().expect("undo benchmark fixture forks"),
                        )
                    },
                    |session| {
                        timed_operation("undo", || {
                            runtime
                                .block_on(
                                    session
                                        .execute("SELECT commit_id FROM lix_undo()", &[])
                                        .into_future(),
                                )
                                .expect("benchmarked undo succeeds");
                        });
                    },
                );
            },
        );

        let redo_snapshot = seeded_storage(&runtime, history_depth);
        let redo_storage = redo_snapshot.fork().expect("redo seed fixture forks");
        let redo_session = open_session(&runtime, redo_storage.clone());
        runtime
            .block_on(
                redo_session
                    .execute("SELECT commit_id FROM lix_undo()", &[])
                    .into_future(),
            )
            .expect("redo benchmark starts undone");
        drop(redo_session);
        let redo_snapshot = redo_storage.fork().expect("undone benchmark fixture forks");
        group.bench_with_input(
            BenchmarkId::new("redo", history_depth),
            &history_depth,
            |benchmark, _| {
                benchmark.iter_with_setup(
                    || {
                        open_session(
                            &runtime,
                            redo_snapshot.fork().expect("redo benchmark fixture forks"),
                        )
                    },
                    |session| {
                        timed_operation("redo", || {
                            runtime
                                .block_on(
                                    session
                                        .execute("SELECT commit_id FROM lix_redo()", &[])
                                        .into_future(),
                                )
                                .expect("benchmarked redo succeeds");
                        });
                    },
                );
            },
        );
    }
    group.finish();

    let mut group = criterion.benchmark_group("undo_descriptor_unrelated_repository_width");
    for unrelated_width in [1_usize, 1_000, 10_000] {
        let snapshot = seeded_descriptor_unrelated_width_storage(&runtime, unrelated_width);
        group.bench_with_input(
            BenchmarkId::new("undo_file_delete", unrelated_width),
            &unrelated_width,
            |benchmark, _| {
                benchmark.iter_with_setup(
                    || {
                        open_session(
                            &runtime,
                            snapshot.fork().expect("descriptor benchmark fixture forks"),
                        )
                    },
                    |session| {
                        timed_operation("undo_file_delete", || {
                            runtime
                                .block_on(
                                    session
                                        .execute("SELECT commit_id FROM lix_undo()", &[])
                                        .into_future(),
                                )
                                .expect("benchmarked descriptor undo succeeds");
                        });
                    },
                );
            },
        );
    }
    group.finish();

    // A first undo of a checkpoint must build its complete immutable effect
    // inventory even when the caller selects one row. Keep full and scoped
    // cases beside each other to expose whether that inventory scales with the
    // checkpoint width rather than with the selected row count.
    let mut group = criterion.benchmark_group("undo_checkpoint_effect_inventory");
    for effect_width in [1_usize, 100, 1_000, 10_000] {
        let (snapshot, checkpoint) = seeded_checkpoint_inventory_storage(&runtime, effect_width);
        let target_params = [Value::Text(checkpoint.clone())];
        group.bench_with_input(
            BenchmarkId::new("full", effect_width),
            &effect_width,
            |benchmark, _| {
                benchmark.iter_with_setup(
                    || {
                        open_session(
                            &runtime,
                            snapshot.fork().expect("checkpoint inventory fixture forks"),
                        )
                    },
                    |session| {
                        timed_operation("checkpoint_full", || {
                            runtime
                                .block_on(
                                    session
                                        .execute(
                                            "SELECT commit_id FROM lix_undo($1)",
                                            &target_params,
                                        )
                                        .into_future(),
                                )
                                .expect("full checkpoint undo succeeds");
                        });
                    },
                );
            },
        );
        group.bench_with_input(
            BenchmarkId::new("scoped_first_undo", effect_width),
            &effect_width,
            |benchmark, _| {
                benchmark.iter_with_setup(
                    || {
                        open_session(
                            &runtime,
                            snapshot
                                .fork()
                                .expect("checkpoint inventory fixture forks"),
                        )
                    },
                    |session| {
                        timed_operation("checkpoint_scoped_first_undo", || {
                            runtime
                                .block_on(
                                    session
                                        .execute(
                                            "SELECT commit_id FROM lix_undo($1, ARRAY[lix_row_ref('lix_key_value', 'checkpoint-0')])",
                                            &target_params,
                                        )
                                        .into_future(),
                                )
                                .expect("scoped checkpoint undo succeeds");
                        });
                    },
                );
            },
        );
    }
    group.finish();

    let mut group = criterion.benchmark_group("undo_transition_width");
    for transition_width in [1_usize, 100, 1_000] {
        let snapshot = seeded_wide_transition_storage(&runtime, transition_width);
        group.bench_with_input(
            BenchmarkId::new("undo", transition_width),
            &transition_width,
            |benchmark, _| {
                benchmark.iter_with_setup(
                    || {
                        open_session(
                            &runtime,
                            snapshot
                                .fork()
                                .expect("wide-transition benchmark fixture forks"),
                        )
                    },
                    |session| {
                        timed_operation("transition_undo", || {
                            runtime
                                .block_on(
                                    session
                                        .execute("SELECT commit_id FROM lix_undo()", &[])
                                        .into_future(),
                                )
                                .expect("benchmarked wide-transition undo succeeds");
                        });
                    },
                );
            },
        );
    }
    group.finish();

    let mut group = criterion.benchmark_group("undo_wide_parent_delta");
    for parent_width in [10_usize, 1_000] {
        let snapshot = seeded_wide_parent_storage(&runtime, parent_width);
        group.bench_with_input(
            BenchmarkId::new("undo", parent_width),
            &parent_width,
            |benchmark, _| {
                benchmark.iter_with_setup(
                    || {
                        open_session(
                            &runtime,
                            snapshot
                                .fork()
                                .expect("wide-parent benchmark fixture forks"),
                        )
                    },
                    |session| {
                        timed_operation("wide_parent_undo", || {
                            runtime
                                .block_on(
                                    session
                                        .execute("SELECT commit_id FROM lix_undo()", &[])
                                        .into_future(),
                                )
                                .expect("benchmarked wide-parent undo succeeds");
                        });
                    },
                );
            },
        );
    }
    group.finish();

    let mut group = criterion.benchmark_group("undo_sparse_identity_gap");
    for history_depth in [10_usize, 1_000] {
        let snapshot = seeded_sparse_gap_storage(&runtime, history_depth);
        group.bench_with_input(
            BenchmarkId::new("undo", history_depth),
            &history_depth,
            |benchmark, _| {
                benchmark.iter_with_setup(
                    || {
                        open_session(
                            &runtime,
                            snapshot.fork().expect("sparse-gap benchmark fixture forks"),
                        )
                    },
                    |session| {
                        timed_operation("sparse_undo", || {
                            runtime
                                .block_on(
                                    session
                                        .execute("SELECT commit_id FROM lix_undo()", &[])
                                        .into_future(),
                                )
                                .expect("benchmarked sparse-gap undo succeeds");
                        });
                    },
                );
            },
        );
    }
    group.finish();
    dump_bench_timings();
}

criterion_group!(benches, benchmark_undo_redo);
criterion_main!(benches);
