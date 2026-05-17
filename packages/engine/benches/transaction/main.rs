use async_trait::async_trait;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use lix_engine::storage_bench::{self, TransactionAccountingReport};
use lix_engine::{
    Backend, BackendKvEntryPage, BackendKvExistsBatch, BackendKvGetRequest, BackendKvKeyPage,
    BackendKvScanRequest, BackendKvValueBatch, BackendKvValuePage, BackendKvWriteBatch,
    BackendKvWriteStats, BackendReadTransaction, BackendWriteTransaction, Engine, LixError,
    SessionContext, Value,
};
use std::collections::{BTreeMap, HashSet};
use std::sync::OnceLock;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Runtime;

#[path = "../storage/backend.rs"]
mod backend;

use backend::BenchBackend;

const ENTITY_ROWS: usize = 10_000;
const LARGE_ENTITY_ROWS: usize = 1_000;
const UPDATE_ROWS_SMALL: usize = 1;
const UPDATE_ROWS_BATCH: usize = 100;
const SCALING_ROWS: &[usize] = &[1_000, 2_000, 5_000, 10_000, 20_000];
const TRANSACTION_LOGIC_ROWS: &[usize] = &[250, 500, 1_000, 2_000];

fn transaction_benches(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create tokio runtime for transaction benchmarks");
    let mut group = c.benchmark_group("transaction");

    group.bench_function("open_empty", |b| {
        b.iter_batched(
            || {
                runtime
                    .block_on(storage_bench::prepare_transaction_commit_empty(
                        BenchBackend::new(),
                    ))
                    .expect("prepare transaction/open_empty")
            },
            |fixture| {
                black_box(
                    runtime
                        .block_on(storage_bench::transaction_open_empty_prepared(&fixture))
                        .unwrap_or_else(|error| panic!("transaction/open_empty succeeds: {error}")),
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("stage_only_entities_no_payload/10k", |b| {
        b.iter_batched(
            || {
                runtime
                    .block_on(
                        storage_bench::prepare_transaction_commit_entities_no_payload(
                            BenchBackend::new(),
                            ENTITY_ROWS,
                        ),
                    )
                    .expect("prepare transaction/stage_only_entities_no_payload")
            },
            |fixture| {
                stage_only(
                    &runtime,
                    fixture,
                    "transaction/stage_only_entities_no_payload",
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("stage_only_entities_payload_1k_unique/10k", |b| {
        b.iter_batched(
            || {
                runtime
                    .block_on(
                        storage_bench::prepare_transaction_commit_entities_payload_1k_unique(
                            BenchBackend::new(),
                            ENTITY_ROWS,
                        ),
                    )
                    .expect("prepare transaction/stage_only_entities_payload_1k_unique")
            },
            |fixture| {
                stage_only(
                    &runtime,
                    fixture,
                    "transaction/stage_only_entities_payload_1k_unique",
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("commit_only_entities_no_payload/10k", |b| {
        b.iter_batched(
            || {
                let fixture = runtime
                    .block_on(
                        storage_bench::prepare_transaction_commit_entities_no_payload(
                            BenchBackend::new(),
                            ENTITY_ROWS,
                        ),
                    )
                    .expect("prepare transaction/commit_only_entities_no_payload fixture");
                runtime
                    .block_on(storage_bench::prepare_transaction_commit_only(fixture))
                    .expect("prepare transaction/commit_only_entities_no_payload")
            },
            |fixture| {
                commit_only(
                    &runtime,
                    fixture,
                    "transaction/commit_only_entities_no_payload",
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("commit_only_entities_payload_1k_same/10k", |b| {
        b.iter_batched(
            || {
                let fixture = runtime
                    .block_on(
                        storage_bench::prepare_transaction_commit_entities_payload_1k_same(
                            BenchBackend::new(),
                            ENTITY_ROWS,
                        ),
                    )
                    .expect("prepare transaction/commit_only_entities_payload_1k_same fixture");
                runtime
                    .block_on(storage_bench::prepare_transaction_commit_only(fixture))
                    .expect("prepare transaction/commit_only_entities_payload_1k_same")
            },
            |fixture| {
                commit_only(
                    &runtime,
                    fixture,
                    "transaction/commit_only_entities_payload_1k_same",
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("commit_only_entities_payload_1k_unique/10k", |b| {
        b.iter_batched(
            || {
                let fixture = runtime
                    .block_on(
                        storage_bench::prepare_transaction_commit_entities_payload_1k_unique(
                            BenchBackend::new(),
                            ENTITY_ROWS,
                        ),
                    )
                    .expect("prepare transaction/commit_only_entities_payload_1k_unique fixture");
                runtime
                    .block_on(storage_bench::prepare_transaction_commit_only(fixture))
                    .expect("prepare transaction/commit_only_entities_payload_1k_unique")
            },
            |fixture| {
                commit_only(
                    &runtime,
                    fixture,
                    "transaction/commit_only_entities_payload_1k_unique",
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("accounting_entities_no_payload/10k", |b| {
        b.iter_batched(
            || {
                prepare_accounting(&runtime, |backend| {
                    storage_bench::prepare_transaction_commit_entities_no_payload(
                        backend,
                        ENTITY_ROWS,
                    )
                })
            },
            |fixture| {
                accounting(
                    &runtime,
                    fixture,
                    "transaction/accounting_entities_no_payload",
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("accounting_entities_payload_1k_unique/10k", |b| {
        b.iter_batched(
            || {
                prepare_accounting(&runtime, |backend| {
                    storage_bench::prepare_transaction_commit_entities_payload_1k_unique(
                        backend,
                        ENTITY_ROWS,
                    )
                })
            },
            |fixture| {
                accounting(
                    &runtime,
                    fixture,
                    "transaction/accounting_entities_payload_1k_unique",
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("accounting_entities_payload_1k_same/10k", |b| {
        b.iter_batched(
            || {
                prepare_accounting(&runtime, |backend| {
                    storage_bench::prepare_transaction_commit_entities_payload_1k_same(
                        backend,
                        ENTITY_ROWS,
                    )
                })
            },
            |fixture| {
                accounting(
                    &runtime,
                    fixture,
                    "transaction/accounting_entities_payload_1k_same",
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("accounting_untracked_payload_1k_same/10k", |b| {
        b.iter_batched(
            || {
                prepare_accounting(&runtime, |backend| {
                    storage_bench::prepare_transaction_commit_untracked_payload_1k_same(
                        backend,
                        ENTITY_ROWS,
                    )
                })
            },
            |fixture| {
                accounting(
                    &runtime,
                    fixture,
                    "transaction/accounting_untracked_payload_1k_same",
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("stage_plus_commit_empty", |b| {
        b.iter_batched(
            || {
                runtime
                    .block_on(storage_bench::prepare_transaction_commit_empty(
                        BenchBackend::new(),
                    ))
                    .expect("prepare transaction/stage_plus_commit_empty")
            },
            |fixture| commit(&runtime, fixture, "transaction/stage_plus_commit_empty"),
            BatchSize::LargeInput,
        )
    });

    group.bench_function("stage_plus_commit_schema_only/1", |b| {
        b.iter_batched(
            || {
                runtime
                    .block_on(storage_bench::prepare_transaction_commit_schema_only(
                        BenchBackend::new(),
                    ))
                    .expect("prepare transaction/stage_plus_commit_schema_only")
            },
            |fixture| {
                commit(
                    &runtime,
                    fixture,
                    "transaction/stage_plus_commit_schema_only",
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("stage_plus_commit_entities_no_payload/10k", |b| {
        b.iter_batched(
            || {
                runtime
                    .block_on(
                        storage_bench::prepare_transaction_commit_entities_no_payload(
                            BenchBackend::new(),
                            ENTITY_ROWS,
                        ),
                    )
                    .expect("prepare transaction/stage_plus_commit_entities_no_payload")
            },
            |fixture| {
                commit(
                    &runtime,
                    fixture,
                    "transaction/stage_plus_commit_entities_no_payload",
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("stage_plus_commit_entities_payload_1k_unique/10k", |b| {
        b.iter_batched(
            || {
                runtime
                    .block_on(
                        storage_bench::prepare_transaction_commit_entities_payload_1k_unique(
                            BenchBackend::new(),
                            ENTITY_ROWS,
                        ),
                    )
                    .expect("prepare transaction/stage_plus_commit_entities_payload_1k_unique")
            },
            |fixture| {
                commit(
                    &runtime,
                    fixture,
                    "transaction/stage_plus_commit_entities_payload_1k_unique",
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("stage_plus_commit_entities_payload_1k_same/10k", |b| {
        b.iter_batched(
            || {
                runtime
                    .block_on(
                        storage_bench::prepare_transaction_commit_entities_payload_1k_same(
                            BenchBackend::new(),
                            ENTITY_ROWS,
                        ),
                    )
                    .expect("prepare transaction/stage_plus_commit_entities_payload_1k_same")
            },
            |fixture| {
                commit(
                    &runtime,
                    fixture,
                    "transaction/stage_plus_commit_entities_payload_1k_same",
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("stage_plus_commit_entities_payload_1k_half_duplicate/10k", |b| {
        b.iter_batched(
            || {
                runtime
                    .block_on(
                        storage_bench::prepare_transaction_commit_entities_payload_1k_half_duplicate(
                            BenchBackend::new(),
                            ENTITY_ROWS,
                        ),
                    )
                    .expect(
                        "prepare transaction/stage_plus_commit_entities_payload_1k_half_duplicate",
                    )
            },
            |fixture| {
                commit(
                    &runtime,
                    fixture,
                    "transaction/stage_plus_commit_entities_payload_1k_half_duplicate",
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("stage_plus_commit_entities_metadata_1k_same/10k", |b| {
        b.iter_batched(
            || {
                runtime
                    .block_on(
                        storage_bench::prepare_transaction_commit_entities_metadata_1k_same(
                            BenchBackend::new(),
                            ENTITY_ROWS,
                        ),
                    )
                    .expect("prepare transaction/stage_plus_commit_entities_metadata_1k_same")
            },
            |fixture| {
                commit(
                    &runtime,
                    fixture,
                    "transaction/stage_plus_commit_entities_metadata_1k_same",
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("stage_plus_commit_entities_payload_16k_unique/1k", |b| {
        b.iter_batched(
            || {
                runtime
                    .block_on(
                        storage_bench::prepare_transaction_commit_entities_payload_16k_unique(
                            BenchBackend::new(),
                            LARGE_ENTITY_ROWS,
                        ),
                    )
                    .expect("prepare transaction/stage_plus_commit_entities_payload_16k_unique")
            },
            |fixture| {
                commit(
                    &runtime,
                    fixture,
                    "transaction/stage_plus_commit_entities_payload_16k_unique",
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function("stage_plus_commit_untracked_payload_1k_same/10k", |b| {
        b.iter_batched(
            || {
                runtime
                    .block_on(
                        storage_bench::prepare_transaction_commit_untracked_payload_1k_same(
                            BenchBackend::new(),
                            ENTITY_ROWS,
                        ),
                    )
                    .expect("prepare transaction/stage_plus_commit_untracked_payload_1k_same")
            },
            |fixture| {
                commit(
                    &runtime,
                    fixture,
                    "transaction/stage_plus_commit_untracked_payload_1k_same",
                )
            },
            BatchSize::LargeInput,
        )
    });

    group.bench_function(
        "stage_plus_commit_update_1_existing_payload_1k/root_10k",
        |b| {
            b.iter_batched(
                || {
                    runtime
                        .block_on(
                            storage_bench::prepare_transaction_update_existing_payload_1k(
                                BenchBackend::new(),
                                ENTITY_ROWS,
                                UPDATE_ROWS_SMALL,
                            ),
                        )
                        .expect(
                            "prepare transaction/stage_plus_commit_update_1_existing_payload_1k",
                        )
                },
                |fixture| {
                    commit(
                        &runtime,
                        fixture,
                        "transaction/stage_plus_commit_update_1_existing_payload_1k",
                    )
                },
                BatchSize::LargeInput,
            )
        },
    );

    group.bench_function(
        "stage_plus_commit_update_100_existing_payload_1k/root_10k",
        |b| {
            b.iter_batched(
                || {
                    runtime
                        .block_on(
                            storage_bench::prepare_transaction_update_existing_payload_1k(
                                BenchBackend::new(),
                                ENTITY_ROWS,
                                UPDATE_ROWS_BATCH,
                            ),
                        )
                        .expect(
                            "prepare transaction/stage_plus_commit_update_100_existing_payload_1k",
                        )
                },
                |fixture| {
                    commit(
                        &runtime,
                        fixture,
                        "transaction/stage_plus_commit_update_100_existing_payload_1k",
                    )
                },
                BatchSize::LargeInput,
            )
        },
    );

    group.finish();

    let mut io_group = c.benchmark_group("transaction_io_100us");

    io_group.bench_function("stage_plus_commit_entities_no_payload/10k", |b| {
        b.iter_batched(
            || {
                runtime
                    .block_on(
                        storage_bench::prepare_transaction_commit_entities_no_payload(
                            latency_backend(),
                            ENTITY_ROWS,
                        ),
                    )
                    .expect("prepare transaction_io_100us/stage_plus_commit_entities_no_payload")
            },
            |fixture| {
                commit(
                    &runtime,
                    fixture,
                    "transaction_io_100us/stage_plus_commit_entities_no_payload",
                )
            },
            BatchSize::LargeInput,
        )
    });

    io_group.bench_function("stage_plus_commit_entities_payload_1k_same/10k", |b| {
        b.iter_batched(
            || {
                runtime
                    .block_on(
                        storage_bench::prepare_transaction_commit_entities_payload_1k_same(
                            latency_backend(),
                            ENTITY_ROWS,
                        ),
                    )
                    .expect(
                        "prepare transaction_io_100us/stage_plus_commit_entities_payload_1k_same",
                    )
            },
            |fixture| {
                commit(
                    &runtime,
                    fixture,
                    "transaction_io_100us/stage_plus_commit_entities_payload_1k_same",
                )
            },
            BatchSize::LargeInput,
        )
    });

    io_group.bench_function("stage_plus_commit_entities_payload_1k_unique/10k", |b| {
        b.iter_batched(
            || {
                runtime
                    .block_on(
                        storage_bench::prepare_transaction_commit_entities_payload_1k_unique(
                            latency_backend(),
                            ENTITY_ROWS,
                        ),
                    )
                    .expect(
                        "prepare transaction_io_100us/stage_plus_commit_entities_payload_1k_unique",
                    )
            },
            |fixture| {
                commit(
                    &runtime,
                    fixture,
                    "transaction_io_100us/stage_plus_commit_entities_payload_1k_unique",
                )
            },
            BatchSize::LargeInput,
        )
    });

    io_group.bench_function("stage_plus_commit_untracked_payload_1k_same/10k", |b| {
        b.iter_batched(
            || {
                runtime
                    .block_on(
                        storage_bench::prepare_transaction_commit_untracked_payload_1k_same(
                            latency_backend(),
                            ENTITY_ROWS,
                        ),
                    )
                    .expect(
                        "prepare transaction_io_100us/stage_plus_commit_untracked_payload_1k_same",
                    )
            },
            |fixture| {
                commit(
                    &runtime,
                    fixture,
                    "transaction_io_100us/stage_plus_commit_untracked_payload_1k_same",
                )
            },
            BatchSize::LargeInput,
        )
    });

    io_group.bench_function("commit_only_entities_payload_1k_same/10k", |b| {
        b.iter_batched(
            || {
                let fixture = runtime
                    .block_on(
                        storage_bench::prepare_transaction_commit_entities_payload_1k_same(
                            latency_backend(),
                            ENTITY_ROWS,
                        ),
                    )
                    .expect(
                        "prepare transaction_io_100us/commit_only_entities_payload_1k_same fixture",
                    );
                runtime
                    .block_on(storage_bench::prepare_transaction_commit_only(fixture))
                    .expect("prepare transaction_io_100us/commit_only_entities_payload_1k_same")
            },
            |fixture| {
                commit_only(
                    &runtime,
                    fixture,
                    "transaction_io_100us/commit_only_entities_payload_1k_same",
                )
            },
            BatchSize::LargeInput,
        )
    });

    io_group.finish();

    let mut logic_group = c.benchmark_group("transaction_logic");
    for &rows in TRANSACTION_LOGIC_ROWS {
        let label = row_count_label(rows);

        logic_group.bench_function(
            format!("stage_rows_batch_no_payload/{label}"),
            |b| {
                b.iter_batched(
                    || {
                        runtime
                            .block_on(
                                storage_bench::prepare_transaction_commit_entities_no_payload(
                                    BenchBackend::new(),
                                    rows,
                                ),
                            )
                            .unwrap_or_else(|error| {
                                panic!(
                                    "prepare transaction_logic/stage_rows_batch_no_payload/{label}: {error}"
                                )
                            })
                    },
                    |fixture| {
                        stage_only(
                            &runtime,
                            fixture,
                            "transaction_logic/stage_rows_batch_no_payload",
                        )
                    },
                    BatchSize::LargeInput,
                )
            },
        );

        logic_group.bench_function(
            format!("stage_rows_individual_no_payload/{label}"),
            |b| {
                b.iter_batched(
                    || {
                        runtime
                            .block_on(
                                storage_bench::prepare_transaction_commit_entities_no_payload(
                                    BenchBackend::new(),
                                    rows,
                                ),
                            )
                            .unwrap_or_else(|error| {
                                panic!(
                                    "prepare transaction_logic/stage_rows_individual_no_payload/{label}: {error}"
                                )
                            })
                    },
                    |fixture| {
                        stage_rows_individually(
                            &runtime,
                            fixture,
                            "transaction_logic/stage_rows_individual_no_payload",
                        )
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }
    logic_group.finish();

    let mut sql_group = c.benchmark_group("transaction_sql_fast_path");
    for &rows in TRANSACTION_LOGIC_ROWS {
        let label = row_count_label(rows);

        sql_group.bench_function(format!("insert_values_batch_no_payload/{label}"), |b| {
            b.iter_batched(
                || {
                    runtime
                        .block_on(prepare_sql_fast_path_fixture(rows))
                        .unwrap_or_else(|error| {
                            panic!(
                                "prepare transaction_sql_fast_path/insert_values_batch_no_payload/{label}: {error}"
                            )
                        })
                },
                |fixture| {
                    black_box(
                        runtime
                            .block_on(sql_fast_path_insert_batch(fixture))
                            .unwrap_or_else(|error| {
                                panic!(
                                    "transaction_sql_fast_path/insert_values_batch_no_payload/{label}: {error}"
                                )
                            }),
                    )
                },
                BatchSize::LargeInput,
            )
        });

        sql_group.bench_function(
            format!("insert_values_individual_no_payload/{label}"),
            |b| {
                b.iter_batched(
                    || {
                        runtime
                            .block_on(prepare_sql_fast_path_fixture(rows))
                            .unwrap_or_else(|error| {
                                panic!(
                                    "prepare transaction_sql_fast_path/insert_values_individual_no_payload/{label}: {error}"
                                )
                            })
                    },
                    |fixture| {
                        black_box(
                            runtime
                                .block_on(sql_fast_path_insert_individual(fixture))
                                .unwrap_or_else(|error| {
                                    panic!(
                                        "transaction_sql_fast_path/insert_values_individual_no_payload/{label}: {error}"
                                    )
                                }),
                        )
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }
    sql_group.finish();

    let mut scaling_group = c.benchmark_group("transaction_scaling");
    for &rows in SCALING_ROWS {
        let label = row_count_label(rows);

        scaling_group.bench_function(
            format!("stage_only_entities_no_payload/{label}"),
            |b| {
                b.iter_batched(
                    || {
                        runtime
                            .block_on(
                                storage_bench::prepare_transaction_commit_entities_no_payload(
                                    BenchBackend::new(),
                                    rows,
                                ),
                            )
                            .unwrap_or_else(|error| {
                                panic!(
                                    "prepare transaction_scaling/stage_only_entities_no_payload/{label}: {error}"
                                )
                            })
                    },
                    |fixture| {
                        stage_only(
                            &runtime,
                            fixture,
                            "transaction_scaling/stage_only_entities_no_payload",
                        )
                    },
                    BatchSize::LargeInput,
                )
            },
        );

        scaling_group.bench_function(
            format!("commit_only_entities_no_payload/{label}"),
            |b| {
                b.iter_batched(
                    || {
                        let fixture = runtime
                            .block_on(
                                storage_bench::prepare_transaction_commit_entities_no_payload(
                                    BenchBackend::new(),
                                    rows,
                                ),
                            )
                            .unwrap_or_else(|error| {
                                panic!(
                                    "prepare transaction_scaling/commit_only_entities_no_payload/{label} fixture: {error}"
                                )
                            });
                        runtime
                            .block_on(storage_bench::prepare_transaction_commit_only(fixture))
                            .unwrap_or_else(|error| {
                                panic!(
                                    "prepare transaction_scaling/commit_only_entities_no_payload/{label}: {error}"
                                )
                            })
                    },
                    |fixture| {
                        commit_only(
                            &runtime,
                            fixture,
                            "transaction_scaling/commit_only_entities_no_payload",
                        )
                    },
                    BatchSize::LargeInput,
                )
            },
        );

        scaling_group.bench_function(
            format!("stage_plus_commit_entities_payload_1k_same/{label}"),
            |b| {
                b.iter_batched(
                    || {
                        runtime
                            .block_on(
                                storage_bench::prepare_transaction_commit_entities_payload_1k_same(
                                    BenchBackend::new(),
                                    rows,
                                ),
                            )
                            .unwrap_or_else(|error| {
                                panic!(
                                    "prepare transaction_scaling/stage_plus_commit_entities_payload_1k_same/{label}: {error}"
                                )
                            })
                    },
                    |fixture| {
                        commit(
                            &runtime,
                            fixture,
                            "transaction_scaling/stage_plus_commit_entities_payload_1k_same",
                        )
                    },
                    BatchSize::LargeInput,
                )
            },
        );

        scaling_group.bench_function(
            format!("stage_plus_commit_entities_payload_1k_unique/{label}"),
            |b| {
                b.iter_batched(
                    || {
                        runtime
                            .block_on(
                                storage_bench::prepare_transaction_commit_entities_payload_1k_unique(
                                    BenchBackend::new(),
                                    rows,
                                ),
                            )
                            .unwrap_or_else(|error| {
                                panic!(
                                    "prepare transaction_scaling/stage_plus_commit_entities_payload_1k_unique/{label}: {error}"
                                )
                            })
                    },
                    |fixture| {
                        commit(
                            &runtime,
                            fixture,
                            "transaction_scaling/stage_plus_commit_entities_payload_1k_unique",
                        )
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }
    scaling_group.finish();

    let mut scaling_io_group = c.benchmark_group("transaction_scaling_io_100us");
    for &rows in SCALING_ROWS {
        let label = row_count_label(rows);
        scaling_io_group.bench_function(
            format!("stage_plus_commit_entities_payload_1k_same/{label}"),
            |b| {
                b.iter_batched(
                    || {
                        runtime
                            .block_on(
                                storage_bench::prepare_transaction_commit_entities_payload_1k_same(
                                    latency_backend(),
                                    rows,
                                ),
                            )
                            .unwrap_or_else(|error| {
                                panic!(
                                    "prepare transaction_scaling_io_100us/stage_plus_commit_entities_payload_1k_same/{label}: {error}"
                                )
                            })
                    },
                    |fixture| {
                        commit(
                            &runtime,
                            fixture,
                            "transaction_scaling_io_100us/stage_plus_commit_entities_payload_1k_same",
                        )
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }
    scaling_io_group.finish();
}

fn row_count_label(rows: usize) -> String {
    if rows % 1_000 == 0 {
        format!("{}k", rows / 1_000)
    } else {
        rows.to_string()
    }
}

fn commit(
    runtime: &Runtime,
    fixture: storage_bench::TransactionBenchFixture,
    label: &str,
) -> storage_bench::StorageBenchReport {
    black_box(
        runtime
            .block_on(storage_bench::transaction_commit_prepared(&fixture))
            .unwrap_or_else(|error| panic!("{label} succeeds: {error}")),
    )
}

fn stage_only(
    runtime: &Runtime,
    fixture: storage_bench::TransactionBenchFixture,
    label: &str,
) -> storage_bench::StorageBenchReport {
    black_box(
        runtime
            .block_on(storage_bench::transaction_stage_only_prepared(&fixture))
            .unwrap_or_else(|error| panic!("{label} succeeds: {error}")),
    )
}

fn stage_rows_individually(
    runtime: &Runtime,
    fixture: storage_bench::TransactionBenchFixture,
    label: &str,
) -> storage_bench::StorageBenchReport {
    black_box(
        runtime
            .block_on(storage_bench::transaction_stage_rows_individually_prepared(
                &fixture,
            ))
            .unwrap_or_else(|error| panic!("{label} succeeds: {error}")),
    )
}

struct SqlFastPathFixture {
    session: SessionContext,
    batch_sql: String,
    row_params: Vec<[Value; 2]>,
}

async fn prepare_sql_fast_path_fixture(rows: usize) -> Result<SqlFastPathFixture, LixError> {
    let backend = backend::BenchBackend::default();
    Engine::initialize(Box::new(backend.clone())).await?;
    let engine = Engine::new(Box::new(backend)).await?;
    let session = engine.open_workspace_session().await?;
    let batch_sql = sql_fast_path_batch_insert_sql(rows);
    let row_params = (0..rows)
        .map(|index| {
            [
                Value::Text(format!(r#"["entity-{index:06}"]"#)),
                Value::Text(format!(
                    r#"{{"key":"entity-{index:06}","value":"value-{index:06}"}}"#
                )),
            ]
        })
        .collect();
    Ok(SqlFastPathFixture {
        session,
        batch_sql,
        row_params,
    })
}

async fn sql_fast_path_insert_batch(fixture: SqlFastPathFixture) -> Result<usize, LixError> {
    let mut tx = fixture.session.begin_transaction().await?;
    let affected = tx.execute(&fixture.batch_sql, &[]).await?.rows_affected() as usize;
    tx.rollback().await?;
    Ok(affected)
}

async fn sql_fast_path_insert_individual(fixture: SqlFastPathFixture) -> Result<usize, LixError> {
    let mut tx = fixture.session.begin_transaction().await?;
    let sql = "INSERT INTO lix_state (entity_id, schema_key, snapshot_content) VALUES (lix_json(?), 'lix_key_value', lix_json(?))";
    let mut affected = 0usize;
    for params in &fixture.row_params {
        affected += tx.execute(sql, params.as_slice()).await?.rows_affected() as usize;
    }
    tx.rollback().await?;
    Ok(affected)
}

fn sql_fast_path_batch_insert_sql(rows: usize) -> String {
    let mut sql =
        String::from("INSERT INTO lix_state (entity_id, schema_key, snapshot_content) VALUES ");
    for index in 0..rows {
        if index > 0 {
            sql.push(',');
        }
        sql.push_str(&format!(
            "(lix_json('[\"entity-{index:06}\"]'), 'lix_key_value', lix_json('{{\"key\":\"entity-{index:06}\",\"value\":\"value-{index:06}\"}}'))"
        ));
    }
    sql
}

fn commit_only(
    runtime: &Runtime,
    fixture: storage_bench::TransactionCommitOnlyFixture,
    label: &str,
) -> storage_bench::StorageBenchReport {
    black_box(
        runtime
            .block_on(storage_bench::transaction_commit_only_prepared(fixture))
            .unwrap_or_else(|error| panic!("{label} succeeds: {error}")),
    )
}

fn latency_backend() -> Arc<dyn Backend + Send + Sync> {
    Arc::new(LatencyBackend {
        inner: BenchBackend::new(),
        read_delay: Duration::from_micros(100),
        write_delay: Duration::from_micros(250),
        commit_delay: Duration::from_micros(500),
    })
}

struct AccountingFixture {
    fixture: storage_bench::TransactionBenchFixture,
    storage: Arc<StorageAccounting>,
}

fn prepare_accounting<F, Fut>(runtime: &Runtime, prepare: F) -> AccountingFixture
where
    F: FnOnce(Arc<dyn Backend + Send + Sync>) -> Fut,
    Fut: std::future::Future<Output = Result<storage_bench::TransactionBenchFixture, LixError>>,
{
    let (backend, storage) = CountingBackend::new(BenchBackend::new());
    let fixture = runtime
        .block_on(prepare(backend))
        .expect("prepare transaction accounting fixture");
    storage.reset();
    storage_bench::reset_transaction_bench_counters();
    AccountingFixture { fixture, storage }
}

fn accounting(
    runtime: &Runtime,
    fixture: AccountingFixture,
    label: &str,
) -> TransactionAccountingReport {
    runtime
        .block_on(storage_bench::transaction_commit_prepared(&fixture.fixture))
        .unwrap_or_else(|error| panic!("{label} succeeds: {error}"));
    let storage = fixture.storage.snapshot();
    let report = TransactionAccountingReport {
        counters: storage_bench::transaction_bench_counters(),
        storage_write_batches: storage.write_batches,
        kv_puts_by_namespace: storage.kv_puts_by_namespace,
        bytes_by_namespace: storage.bytes_by_namespace,
    };
    print_accounting_once(label, &report);
    black_box(report)
}

static PRINTED_ACCOUNTING_LABELS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

fn print_accounting_once(label: &str, report: &TransactionAccountingReport) {
    if std::env::var("LIX_BENCH_PRINT_ACCOUNTING").ok().as_deref() != Some("1") {
        return;
    }
    let labels = PRINTED_ACCOUNTING_LABELS.get_or_init(|| Mutex::new(HashSet::new()));
    let mut labels = labels
        .lock()
        .expect("printed accounting label mutex should lock");
    if !labels.insert(label.to_string()) {
        return;
    }
    eprintln!("{label}: {report:#?}");
}

#[derive(Default)]
struct StorageAccounting {
    inner: Mutex<StorageAccountingSnapshot>,
}

#[derive(Default)]
struct StorageAccountingSnapshot {
    write_batches: usize,
    kv_puts_by_namespace: BTreeMap<String, usize>,
    bytes_by_namespace: BTreeMap<String, usize>,
}

impl StorageAccounting {
    fn reset(&self) {
        *self
            .inner
            .lock()
            .expect("storage accounting mutex should lock") = StorageAccountingSnapshot::default();
    }

    fn record_write_batch(&self, batch: &BackendKvWriteBatch) {
        let mut inner = self
            .inner
            .lock()
            .expect("storage accounting mutex should lock");
        inner.write_batches += 1;
        for group in &batch.groups {
            let namespace = group.namespace().to_string();
            for index in 0..group.put_count() {
                let Some(key) = group.put_key(index) else {
                    continue;
                };
                let Some(value) = group.put_value(index) else {
                    continue;
                };
                *inner
                    .kv_puts_by_namespace
                    .entry(namespace.clone())
                    .or_default() += 1;
                *inner
                    .bytes_by_namespace
                    .entry(namespace.clone())
                    .or_default() += key.len() + value.len();
            }
            for index in 0..group.delete_count() {
                let Some(key) = group.delete_key(index) else {
                    continue;
                };
                *inner
                    .bytes_by_namespace
                    .entry(namespace.clone())
                    .or_default() += key.len();
            }
        }
    }

    fn snapshot(&self) -> StorageAccountingSnapshot {
        let inner = self
            .inner
            .lock()
            .expect("storage accounting mutex should lock");
        StorageAccountingSnapshot {
            write_batches: inner.write_batches,
            kv_puts_by_namespace: inner.kv_puts_by_namespace.clone(),
            bytes_by_namespace: inner.bytes_by_namespace.clone(),
        }
    }
}

struct CountingBackend {
    inner: Arc<dyn Backend + Send + Sync>,
    accounting: Arc<StorageAccounting>,
}

impl CountingBackend {
    fn new(
        inner: Arc<dyn Backend + Send + Sync>,
    ) -> (Arc<dyn Backend + Send + Sync>, Arc<StorageAccounting>) {
        let accounting = Arc::new(StorageAccounting::default());
        (
            Arc::new(Self {
                inner,
                accounting: Arc::clone(&accounting),
            }),
            accounting,
        )
    }
}

struct LatencyBackend {
    inner: Arc<dyn Backend + Send + Sync>,
    read_delay: Duration,
    write_delay: Duration,
    commit_delay: Duration,
}

impl LatencyBackend {
    fn delay(duration: Duration) {
        if !duration.is_zero() {
            std::thread::sleep(duration);
        }
    }
}

#[async_trait]
impl Backend for LatencyBackend {
    async fn begin_read_transaction(
        &self,
    ) -> Result<Box<dyn BackendReadTransaction + Send + Sync + 'static>, LixError> {
        let transaction = self.inner.begin_read_transaction().await?;
        Ok(Box::new(LatencyReadTransaction {
            transaction,
            read_delay: self.read_delay,
        }))
    }

    async fn begin_write_transaction(
        &self,
    ) -> Result<Box<dyn BackendWriteTransaction + Send + Sync + 'static>, LixError> {
        let transaction = self.inner.begin_write_transaction().await?;
        Ok(Box::new(LatencyWriteTransaction {
            transaction,
            read_delay: self.read_delay,
            write_delay: self.write_delay,
            commit_delay: self.commit_delay,
        }))
    }
}

struct LatencyReadTransaction {
    transaction: Box<dyn BackendReadTransaction + Send + Sync + 'static>,
    read_delay: Duration,
}

#[async_trait]
impl BackendReadTransaction for LatencyReadTransaction {
    async fn get_values(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvValueBatch, LixError> {
        LatencyBackend::delay(self.read_delay);
        self.transaction.get_values(request).await
    }

    async fn exists_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvExistsBatch, LixError> {
        LatencyBackend::delay(self.read_delay);
        self.transaction.exists_many(request).await
    }

    async fn scan_keys(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvKeyPage, LixError> {
        LatencyBackend::delay(self.read_delay);
        self.transaction.scan_keys(request).await
    }

    async fn scan_values(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvValuePage, LixError> {
        LatencyBackend::delay(self.read_delay);
        self.transaction.scan_values(request).await
    }

    async fn scan_entries(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvEntryPage, LixError> {
        LatencyBackend::delay(self.read_delay);
        self.transaction.scan_entries(request).await
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        self.transaction.rollback().await
    }
}

struct LatencyWriteTransaction {
    transaction: Box<dyn BackendWriteTransaction + Send + Sync + 'static>,
    read_delay: Duration,
    write_delay: Duration,
    commit_delay: Duration,
}

#[async_trait]
impl BackendReadTransaction for LatencyWriteTransaction {
    async fn get_values(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvValueBatch, LixError> {
        LatencyBackend::delay(self.read_delay);
        self.transaction.get_values(request).await
    }

    async fn exists_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvExistsBatch, LixError> {
        LatencyBackend::delay(self.read_delay);
        self.transaction.exists_many(request).await
    }

    async fn scan_keys(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvKeyPage, LixError> {
        LatencyBackend::delay(self.read_delay);
        self.transaction.scan_keys(request).await
    }

    async fn scan_values(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvValuePage, LixError> {
        LatencyBackend::delay(self.read_delay);
        self.transaction.scan_values(request).await
    }

    async fn scan_entries(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvEntryPage, LixError> {
        LatencyBackend::delay(self.read_delay);
        self.transaction.scan_entries(request).await
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        self.transaction.rollback().await
    }
}

#[async_trait]
impl BackendWriteTransaction for LatencyWriteTransaction {
    async fn write_kv_batch(
        &mut self,
        batch: BackendKvWriteBatch,
    ) -> Result<BackendKvWriteStats, LixError> {
        LatencyBackend::delay(self.write_delay);
        self.transaction.write_kv_batch(batch).await
    }

    async fn commit(self: Box<Self>) -> Result<(), LixError> {
        LatencyBackend::delay(self.commit_delay);
        self.transaction.commit().await
    }
}

#[async_trait]
impl Backend for CountingBackend {
    async fn begin_read_transaction(
        &self,
    ) -> Result<Box<dyn BackendReadTransaction + Send + Sync + 'static>, LixError> {
        let transaction = self.inner.begin_read_transaction().await?;
        Ok(Box::new(CountingReadTransaction { transaction }))
    }

    async fn begin_write_transaction(
        &self,
    ) -> Result<Box<dyn BackendWriteTransaction + Send + Sync + 'static>, LixError> {
        let transaction = self.inner.begin_write_transaction().await?;
        Ok(Box::new(CountingWriteTransaction {
            transaction,
            accounting: Arc::clone(&self.accounting),
        }))
    }
}

struct CountingReadTransaction {
    transaction: Box<dyn BackendReadTransaction + Send + Sync + 'static>,
}

#[async_trait]
impl BackendReadTransaction for CountingReadTransaction {
    async fn get_values(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvValueBatch, LixError> {
        self.transaction.get_values(request).await
    }

    async fn exists_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<lix_engine::BackendKvExistsBatch, LixError> {
        self.transaction.exists_many(request).await
    }

    async fn scan_keys(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvKeyPage, LixError> {
        self.transaction.scan_keys(request).await
    }

    async fn scan_values(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvValuePage, LixError> {
        self.transaction.scan_values(request).await
    }

    async fn scan_entries(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvEntryPage, LixError> {
        self.transaction.scan_entries(request).await
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        self.transaction.rollback().await
    }
}

struct CountingWriteTransaction {
    transaction: Box<dyn BackendWriteTransaction + Send + Sync + 'static>,
    accounting: Arc<StorageAccounting>,
}

#[async_trait]
impl BackendReadTransaction for CountingWriteTransaction {
    async fn get_values(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvValueBatch, LixError> {
        self.transaction.get_values(request).await
    }

    async fn exists_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvExistsBatch, LixError> {
        self.transaction.exists_many(request).await
    }

    async fn scan_keys(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvKeyPage, LixError> {
        self.transaction.scan_keys(request).await
    }

    async fn scan_values(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvValuePage, LixError> {
        self.transaction.scan_values(request).await
    }

    async fn scan_entries(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvEntryPage, LixError> {
        self.transaction.scan_entries(request).await
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        self.transaction.rollback().await
    }
}

#[async_trait]
impl BackendWriteTransaction for CountingWriteTransaction {
    async fn write_kv_batch(
        &mut self,
        batch: BackendKvWriteBatch,
    ) -> Result<BackendKvWriteStats, LixError> {
        self.accounting.record_write_batch(&batch);
        self.transaction.write_kv_batch(batch).await
    }

    async fn commit(self: Box<Self>) -> Result<(), LixError> {
        self.transaction.commit().await
    }
}

criterion_group!(benches, transaction_benches);
criterion_main!(benches);
