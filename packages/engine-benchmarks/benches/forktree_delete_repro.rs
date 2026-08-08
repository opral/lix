//! Test-only deterministic minimization for the bc823 single-row delete bug.

#[path = "forktree_replacement/model.rs"]
mod model;

use std::path::Path;

use lix::storage::Storage;
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;
use model::{ForkTree, Mutation};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Expectation {
    Fail,
    Pass,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    assert_eq!(
        args.len(),
        6,
        "usage: forktree_delete_repro <rocksdb|slatedb> <path> <rows> <batch> <expect-fail|expect-pass>"
    );
    let backend = args[1].as_str();
    let path = Path::new(&args[2]);
    let rows = args[3].parse::<usize>().expect("rows must be an integer");
    let batch = args[4].parse::<usize>().expect("batch must be an integer");
    let expectation = match args[5].as_str() {
        "expect-fail" => Expectation::Fail,
        "expect-pass" => Expectation::Pass,
        other => panic!("unknown expectation '{other}'"),
    };
    assert!(rows > 0 && batch > 0);

    match backend {
        "rocksdb" => {
            let database = RocksDB::open(path).expect("open repro RocksDB");
            let outcome = exercise(database.clone(), rows, batch).await;
            database.flush().expect("flush repro RocksDB");
            drop(database);
            let reopened = RocksDB::open(path).expect("cold reopen repro RocksDB");
            verify(backend, reopened, rows, batch, expectation, outcome).await;
        }
        "slatedb" => {
            let database = SlateDB::open(path).expect("open repro SlateDB");
            let outcome = exercise(database.clone(), rows, batch).await;
            database
                .flush_memtable_for_diagnostics()
                .await
                .expect("flush repro SlateDB");
            drop(database);
            let reopened = SlateDB::open(path).expect("cold reopen repro SlateDB");
            verify(backend, reopened, rows, batch, expectation, outcome).await;
        }
        other => panic!("unknown backend '{other}'"),
    }
}

#[derive(Debug)]
struct Outcome {
    committed_deletes: usize,
    failure: Option<String>,
}

async fn exercise<S>(storage: S, rows: usize, batch: usize) -> Outcome
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let tree = ForkTree::new(storage);
    let initial = (0..rows)
        .map(|index| (row_key(index), row_value(index)))
        .collect::<Vec<_>>();
    tree.initialize(&initial)
        .await
        .expect("initialize delete minimization");
    let mutations = (0..rows)
        .map(|index| Mutation::Delete {
            key: row_key(index),
        })
        .collect::<Vec<_>>();
    let mut committed_deletes = 0;
    let mut failure = None;
    for (chunk_index, chunk) in mutations.chunks(batch).enumerate() {
        if let Err(error) = tree.apply_sorted_mutations(chunk).await {
            failure = Some(format!(
                "chunk_index={chunk_index},first_key={},last_key={},error={error}",
                String::from_utf8_lossy(chunk.first().expect("nonempty chunk").key()),
                String::from_utf8_lossy(chunk.last().expect("nonempty chunk").key()),
            ));
            break;
        }
        committed_deletes += chunk.len();
    }
    Outcome {
        committed_deletes,
        failure,
    }
}

async fn verify<S>(
    backend: &str,
    storage: S,
    rows: usize,
    batch: usize,
    expectation: Expectation,
    outcome: Outcome,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let tree = ForkTree::new(storage);
    let remaining = tree
        .read_relational_all("main")
        .await
        .expect("cold reopen remaining rows");
    assert_eq!(remaining.len(), rows - outcome.committed_deletes);
    match (expectation, outcome.failure.as_deref()) {
        (Expectation::Fail, Some(error)) => println!(
            "forktree_delete_repro,backend={backend},rows={rows},batch={batch},expectation=fail,reproduced=true,committed_deletes={},cold_remaining={},failure={error}",
            outcome.committed_deletes,
            remaining.len(),
        ),
        (Expectation::Pass, None) => {
            assert!(remaining.is_empty());
            println!(
                "forktree_delete_repro,backend={backend},rows={rows},batch={batch},expectation=pass,reproduced=true,committed_deletes={},cold_remaining=0",
                outcome.committed_deletes,
            );
        }
        (Expectation::Fail, None) => panic!("expected delete failure but all deletes committed"),
        (Expectation::Pass, Some(error)) => panic!("expected delete success but failed: {error}"),
    }
}

fn row_key(index: usize) -> Vec<u8> {
    format!("row-{index:09}").into_bytes()
}

fn row_value(index: usize) -> Vec<u8> {
    format!("value-{index:09}").into_bytes()
}
