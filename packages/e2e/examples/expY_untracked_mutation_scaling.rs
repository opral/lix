//! Does one untracked mutation cost O(1) or O(total untracked rows)?
//!
//! `HotWriter::stage_untracked_generation` (`hot_state/tracked_head/hot.rs`)
//! loads the whole previous untracked generation, applies the deltas, and
//! stages a *complete* new generation under a fresh generation UUID. If that
//! is what actually happens on the SQL path, then the lane that exists for
//! "fast mutable state" charges every mutation for the entire untracked
//! population.
//!
//! This example seeds `U` untracked `lix_key_value` rows, then times `M`
//! single-row untracked updates, for a sweep of `U`. A flat curve means the
//! republish is avoided; a linear curve means it is not.
//!
//! It also reports the tracked control at the same population, so the shape is
//! attributable to the untracked lane rather than to the key-value surface.
//!
//! Usage: `expY_untracked_mutation_scaling [mutations] [pop1,pop2,...]`
//! (defaults: 20 mutations, populations 1,10,100,1000,10000).

use std::time::Instant;

use lix::Value;
use lix::integration::{Engine, SessionContext};
use lix::storage::Storage;
use lix::storage_adapter::{StorageAdapter, StorageReadOptions};
use lix::storage_bench::layout_accounting;
use lix_storage_rocksdb::RocksDB;

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let mutations = args
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(20);
    let populations = args
        .next()
        .map(|value| {
            value
                .split(',')
                .filter_map(|part| part.trim().parse::<usize>().ok())
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| vec![1, 10, 100, 1_000, 10_000]);

    println!("expY_untracked_mutation_scaling mutations={mutations} populations={populations:?}");
    println!(
        "{:<10} {:>12} {:>16} {:>16} {:>12}",
        "population", "lane", "median_us", "mean_us", "ratio_vs_1"
    );

    let largest = populations.iter().copied().max().unwrap_or(0);
    for lane in ["untracked", "tracked"] {
        let untracked = lane == "untracked";
        let mut first: Option<f64> = None;
        for &population in &populations {
            let median = measure(population, mutations, untracked, population == largest).await;
            let base = *first.get_or_insert(median.0);
            println!(
                "{population:<10} {lane:>12} {:>16.1} {:>16.1} {:>12.2}",
                median.0,
                median.1,
                median.0 / base
            );
        }
    }
}

/// Returns (median_us, mean_us) for one single-row mutation at this population.
async fn measure(
    population: usize,
    mutations: usize,
    untracked: bool,
    report_layout: bool,
) -> (f64, f64) {
    let directory = tempfile::tempdir().expect("create RocksDB directory");
    let storage = RocksDB::open(directory.path()).expect("open RocksDB");
    Engine::initialize(storage.clone())
        .await
        .expect("initialize repository");
    let engine = Engine::new(storage.clone()).await.expect("open engine");
    let session = engine
        .open_session()
        .await
        .expect("open workspace");

    seed(&session, population, untracked).await;

    // Warm the read/write paths once so the first timed sample is not a
    // one-off cache fill.
    mutate(&session, 0, 0).await;

    let mut samples = Vec::with_capacity(mutations);
    for index in 0..mutations {
        let started = Instant::now();
        mutate(&session, 0, index + 1).await;
        samples.push(started.elapsed().as_secs_f64() * 1e6);
    }
    let mean = samples.iter().sum::<f64>() / samples.len() as f64;
    samples.sort_by(|left, right| left.partial_cmp(right).expect("finite samples"));
    if report_layout {
        report_spaces(&storage, population, untracked).await;
    }
    (samples[samples.len() / 2], mean)
}

/// Per-space settled rows and bytes after the mutation sweep, so the byte cost
/// of the lane is reported next to its latency cost.
async fn report_spaces<S>(storage: &S, population: usize, untracked: bool)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let adapter = StorageAdapter::new(storage.clone());
    let read = adapter
        .begin_read(StorageReadOptions::default())
        .await
        .expect("open storage snapshot");
    let lane = if untracked { "untracked" } else { "tracked" };
    let mut total = 0_u64;
    for entry in layout_accounting(&read).await {
        let bytes = entry.key_bytes + entry.value_bytes;
        if bytes == 0 {
            continue;
        }
        total += bytes;
        println!(
            "  layout lane={lane} population={population} space=0x{:08x} {:<52} rows={:<8} bytes={bytes}",
            entry.space_id, entry.space, entry.rows
        );
    }
    println!("  layout lane={lane} population={population} TOTAL bytes={total}");
}

async fn seed<S>(session: &SessionContext<S>, population: usize, untracked: bool)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let mut transaction = session.begin_transaction().await.expect("begin seed");
    for index in 0..population {
        transaction
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_untracked) VALUES ($1, $2, $3)",
                &[
                    Value::Text(format!("expY_{index:08}")),
                    Value::Text(format!("seed-{index}")),
                    Value::Boolean(untracked),
                ],
            )
            .await
            .expect("seed row");
    }
    transaction.commit().await.expect("commit seed");
}

async fn mutate<S>(session: &SessionContext<S>, target: usize, revision: usize)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    session
        .execute(
            "UPDATE lix_key_value SET value = $1 WHERE key = $2",
            &[
                Value::Text(format!("rev-{revision}")),
                Value::Text(format!("expY_{target:08}")),
            ],
        )
        .await
        .expect("mutate row");
}
