use lix_engine::storage_bench::{self, StorageBenchConfig};

use crate::{Args, BenchBackend};
use criterion::{black_box, BatchSize, Criterion};
use tokio::runtime::Runtime;

pub(crate) fn bench(c: &mut Criterion, runtime: &Runtime, args: Args) {
    let mut group = c.benchmark_group("commit_graph");
    group.bench_function("change_history_from_commit/10k", |b| {
        b.iter_batched(
            || prepare_read(runtime, args),
            |(backend, fixture)| {
                black_box(
                    runtime
                        .block_on(
                            storage_bench::commit_graph_change_history_from_commit_prepared(
                                &backend, &fixture,
                            ),
                        )
                        .expect("commit_graph/change_history_from_commit succeeds"),
                )
            },
            BatchSize::LargeInput,
        )
    });
    group.finish();
}

fn prepare_read(
    runtime: &Runtime,
    args: Args,
) -> (
    std::sync::Arc<dyn lix_engine::Backend + Send + Sync>,
    lix_engine::storage_bench::CommitGraphReadFixture,
) {
    let backend = BenchBackend::new();
    let fixture = runtime
        .block_on(storage_bench::prepare_commit_graph_read(
            &backend,
            config(&args),
        ))
        .expect("prepare commit_graph/read");
    (backend, fixture)
}

fn config(args: &Args) -> StorageBenchConfig {
    args.config()
}
