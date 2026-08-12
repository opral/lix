use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use lix::integration::{Engine, SessionContext};
use lix::{Blob, ExecuteBatchStatement, Memory};

fn seeded_storage(runtime: &tokio::runtime::Runtime, history_depth: usize) -> Vec<u8> {
    runtime.block_on(async move {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("benchmark storage initializes");
        let engine = Engine::new(storage.clone())
            .await
            .expect("benchmark engine opens");
        let session = engine
            .open_session()
            .await
            .expect("benchmark session opens");
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
        storage
            .export_snapshot()
            .expect("benchmark storage snapshot exports")
    })
}

fn seeded_sparse_gap_storage(runtime: &tokio::runtime::Runtime, history_depth: usize) -> Vec<u8> {
    runtime.block_on(async move {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("benchmark storage initializes");
        let engine = Engine::new(storage.clone())
            .await
            .expect("benchmark engine opens");
        let session = engine
            .open_session()
            .await
            .expect("benchmark session opens");
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
        storage
            .export_snapshot()
            .expect("benchmark storage snapshot exports")
    })
}

fn seeded_wide_parent_storage(runtime: &tokio::runtime::Runtime, parent_width: usize) -> Vec<u8> {
    runtime.block_on(async move {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("benchmark storage initializes");
        let engine = Engine::new(storage.clone())
            .await
            .expect("benchmark engine opens");
        let session = engine
            .open_session()
            .await
            .expect("benchmark session opens");
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
        storage
            .export_snapshot()
            .expect("benchmark storage snapshot exports")
    })
}

fn seeded_wide_transition_storage(
    runtime: &tokio::runtime::Runtime,
    transition_width: usize,
) -> Vec<u8> {
    runtime.block_on(async move {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("benchmark storage initializes");
        let engine = Engine::new(storage.clone())
            .await
            .expect("benchmark engine opens");
        let session = engine
            .open_session()
            .await
            .expect("benchmark session opens");
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
        storage
            .export_snapshot()
            .expect("benchmark storage snapshot exports")
    })
}

fn seeded_descriptor_unrelated_width_storage(
    runtime: &tokio::runtime::Runtime,
    unrelated_width: usize,
) -> Vec<u8> {
    runtime.block_on(async move {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("benchmark storage initializes");
        let engine = Engine::new(storage.clone())
            .await
            .expect("benchmark engine opens");
        let session = engine
            .open_session()
            .await
            .expect("benchmark session opens");
        session
            .upsert_file_content(
                "/descriptor-target.txt".into(),
                Blob::from("target".as_bytes()),
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
        storage
            .export_snapshot()
            .expect("benchmark storage snapshot exports")
    })
}

fn open_session(runtime: &tokio::runtime::Runtime, storage: Memory) -> SessionContext<Memory> {
    runtime.block_on(async move {
        Engine::new(storage)
            .await
            .expect("benchmark engine opens")
            .open_session()
            .await
            .expect("benchmark session opens")
    })
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
                benchmark.iter_custom(|iterations| {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iterations {
                        let session = open_session(
                            &runtime,
                            Memory::from_snapshot(&undo_snapshot)
                                .expect("update benchmark snapshot restores"),
                        );
                        let started = Instant::now();
                        runtime
                            .block_on(session.execute(
                                "UPDATE lix_key_value SET value = 'next' WHERE key = 'bench-key'",
                                &[],
                            ))
                            .expect("benchmarked ordinary update succeeds");
                        elapsed += started.elapsed();
                    }
                    elapsed
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("undo", history_depth),
            &history_depth,
            |benchmark, _| {
                benchmark.iter_custom(|iterations| {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iterations {
                        let session = open_session(
                            &runtime,
                            Memory::from_snapshot(&undo_snapshot)
                                .expect("undo benchmark snapshot restores"),
                        );
                        let started = Instant::now();
                        runtime
                            .block_on(session.undo())
                            .expect("benchmarked undo succeeds");
                        elapsed += started.elapsed();
                    }
                    elapsed
                });
            },
        );

        let redo_snapshot = seeded_storage(&runtime, history_depth);
        let redo_storage =
            Memory::from_snapshot(&redo_snapshot).expect("redo seed snapshot restores");
        let redo_session = open_session(&runtime, redo_storage.clone());
        runtime
            .block_on(redo_session.undo())
            .expect("redo benchmark starts undone");
        drop(redo_session);
        let redo_snapshot = redo_storage
            .export_snapshot()
            .expect("undone benchmark snapshot exports");
        group.bench_with_input(
            BenchmarkId::new("redo", history_depth),
            &history_depth,
            |benchmark, _| {
                benchmark.iter_custom(|iterations| {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iterations {
                        let session = open_session(
                            &runtime,
                            Memory::from_snapshot(&redo_snapshot)
                                .expect("redo benchmark snapshot restores"),
                        );
                        let started = Instant::now();
                        runtime
                            .block_on(session.redo())
                            .expect("benchmarked redo succeeds");
                        elapsed += started.elapsed();
                    }
                    elapsed
                });
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
                benchmark.iter_custom(|iterations| {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iterations {
                        let session = open_session(
                            &runtime,
                            Memory::from_snapshot(&snapshot)
                                .expect("descriptor benchmark snapshot restores"),
                        );
                        let started = Instant::now();
                        runtime
                            .block_on(session.undo())
                            .expect("benchmarked descriptor undo succeeds");
                        elapsed += started.elapsed();
                    }
                    elapsed
                });
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
                benchmark.iter_custom(|iterations| {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iterations {
                        let session = open_session(
                            &runtime,
                            Memory::from_snapshot(&snapshot)
                                .expect("wide-transition benchmark snapshot restores"),
                        );
                        let started = Instant::now();
                        runtime
                            .block_on(session.undo())
                            .expect("benchmarked wide-transition undo succeeds");
                        elapsed += started.elapsed();
                    }
                    elapsed
                });
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
                benchmark.iter_custom(|iterations| {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iterations {
                        let session = open_session(
                            &runtime,
                            Memory::from_snapshot(&snapshot)
                                .expect("wide-parent benchmark snapshot restores"),
                        );
                        let started = Instant::now();
                        runtime
                            .block_on(session.undo())
                            .expect("benchmarked wide-parent undo succeeds");
                        elapsed += started.elapsed();
                    }
                    elapsed
                });
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
                benchmark.iter_custom(|iterations| {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iterations {
                        let session = open_session(
                            &runtime,
                            Memory::from_snapshot(&snapshot)
                                .expect("sparse-gap benchmark snapshot restores"),
                        );
                        let started = Instant::now();
                        runtime
                            .block_on(session.undo())
                            .expect("benchmarked sparse-gap undo succeeds");
                        elapsed += started.elapsed();
                    }
                    elapsed
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, benchmark_undo_redo);
criterion_main!(benches);
