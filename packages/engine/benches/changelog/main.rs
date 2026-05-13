use criterion::{criterion_group, criterion_main, Criterion};

mod backends;
mod config;
mod cpu;
mod fixtures;
mod metrics;
mod ops;
mod shapes;
mod storage;

fn changelog_benches(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create tokio runtime for changelog benchmarks");

    cpu::bench(c);
    storage::bench(c, &runtime);
}

criterion_group!(benches, changelog_benches);
criterion_main!(benches);
