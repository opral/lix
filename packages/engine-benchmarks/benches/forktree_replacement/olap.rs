use std::sync::{Arc, Mutex};
use std::time::Instant;

use lix::storage::{Memory, Storage};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters};

use super::model::ForkTree;
use super::{
    Backend, CountingStorage, IoStats, Layout, Parameters, begin_allocation_profile,
    directory_bytes, end_allocation_profile, physical_delta, process_cpu_nanos,
    process_resident_bytes, take_stats,
};

#[path = "olap_common.rs"]
#[allow(dead_code)]
mod common;

use common::{Cell, NarrowRow, Query, WideRow};

struct Expected {
    digests: Vec<(Query, [u8; 32], usize)>,
}

pub(super) async fn run(parameters: Parameters) {
    assert_eq!(parameters.layout, Layout::ForkTree);
    println!(
        "forktree_olap_boundary,sql_wiring=false,comparison=authenticated_source_materialization_plus_identical_operators,current_big_o=O(N+Q),forktree_big_o=O(height+touched_blocks+N+Q),forktree_backend_requests=O(height),claim=source_layer_only"
    );
    match parameters.backend {
        Backend::RocksDb => run_rocks(parameters).await,
        Backend::SlateDb => run_slate(parameters).await,
    }
}

pub(super) async fn run_memory_gate(rows: usize) {
    let (storage, stats) = CountingStorage::new(Memory::default());
    let (tree, expected) = prepare(storage, rows).await;
    for &(query, digest, result_rows) in &expected.digests {
        let _ = take_stats(&stats);
        let result = execute(&tree, query).await;
        assert_eq!(result.len(), result_rows);
        assert_eq!(common::digest(&result), digest);
        let io = take_stats(&stats);
        assert_eq!(
            io.begin_reads,
            if query == Query::Join { 2 } else { 1 },
            "each ordered range must use one coherent StorageRead"
        );
    }

    let fault_tree = ForkTree::new(Memory::default());
    fault_tree
        .initialize(&[(b"a".to_vec(), b"value".to_vec())])
        .await
        .expect("initialize ordered-range corruption gate");
    fault_tree
        .verify_projected_range_corruption_fail_closed()
        .await
        .expect("ordered range corruption must fail closed");
    println!(
        "forktree_olap_memory_gate,rows={rows},exact_results=true,snapshot_coherent=true,malformed_fail_closed=true,truncated_fail_closed=true,substituted_fail_closed=true"
    );
}

async fn run_rocks(parameters: Parameters) {
    let directory = tempfile::tempdir().expect("create ForkTree OLAP RocksDB directory");
    let database = RocksDB::open(directory.path()).expect("open ForkTree OLAP RocksDB");
    let (storage, stats) = CountingStorage::new(database.clone());
    let (tree, expected) = prepare(storage, parameters.rows).await;
    database.flush().expect("flush ForkTree OLAP RocksDB setup");
    run_queries(&tree, &expected, parameters, &stats, directory.path(), None).await;
    database
        .flush()
        .expect("flush ForkTree OLAP RocksDB result");
    let disk_bytes = directory_bytes(directory.path());
    drop(tree);
    drop(database);
    let reopened = RocksDB::open(directory.path()).expect("reopen ForkTree OLAP RocksDB");
    let (reopened, _) = CountingStorage::new(reopened);
    verify_reopen(ForkTree::new(reopened), &expected).await;
    println!(
        "forktree_olap_reopen,backend=rocksdb,layout=forktree,rows={},exact_results=true,disk_bytes={disk_bytes}",
        parameters.rows
    );
}

async fn run_slate(parameters: Parameters) {
    let directory = tempfile::tempdir().expect("create ForkTree OLAP SlateDB directory");
    let counters = SlateDBIoCounters::default();
    let database = SlateDB::open_with_io_counters(directory.path(), counters.clone())
        .expect("open ForkTree OLAP SlateDB");
    let (storage, stats) = CountingStorage::new(database.clone());
    let (tree, expected) = prepare(storage, parameters.rows).await;
    database
        .flush_memtable_for_diagnostics()
        .await
        .expect("flush ForkTree OLAP SlateDB setup");
    run_queries(
        &tree,
        &expected,
        parameters,
        &stats,
        directory.path(),
        Some(&counters),
    )
    .await;
    database
        .flush_memtable_for_diagnostics()
        .await
        .expect("flush ForkTree OLAP SlateDB result");
    let disk_bytes = directory_bytes(directory.path());
    drop(tree);
    drop(database);
    let reopened = SlateDB::open(directory.path()).expect("reopen ForkTree OLAP SlateDB");
    let (reopened, _) = CountingStorage::new(reopened);
    verify_reopen(ForkTree::new(reopened), &expected).await;
    println!(
        "forktree_olap_reopen,backend=slatedb,layout=forktree,rows={},exact_results=true,disk_bytes={disk_bytes}",
        parameters.rows
    );
}

async fn prepare<S>(
    storage: CountingStorage<S>,
    rows: usize,
) -> (ForkTree<CountingStorage<S>>, Expected)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let narrow = (0..rows).map(common::narrow_row).collect::<Vec<_>>();
    let wide = (0..rows).map(common::wide_row).collect::<Vec<_>>();
    let dimensions = common::dimension_rows();
    let mut encoded = Vec::with_capacity(rows * 2 + dimensions.len());
    for row in &narrow {
        encoded.push((common::key(b'n', &row.id), common::encode_narrow(row)));
    }
    for row in &wide {
        encoded.push((common::key(b'w', &row.base.id), common::encode_wide(row)));
    }
    for (lane, label) in &dimensions {
        encoded.push((
            common::key(b'd', &format!("{lane:02}")),
            label.as_bytes().to_vec(),
        ));
    }
    encoded.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    let expected = Expected {
        digests: Query::ALL
            .into_iter()
            .map(|query| {
                let result = common::evaluate(query, &narrow, &wide, &dimensions);
                (query, common::digest(&result), result.len())
            })
            .collect(),
    };
    let tree = ForkTree::new(storage);
    tree.initialize(&encoded)
        .await
        .expect("initialize ForkTree OLAP rows");
    (tree, expected)
}

async fn run_queries<S>(
    tree: &ForkTree<CountingStorage<S>>,
    expected: &Expected,
    parameters: Parameters,
    stats: &Arc<Mutex<IoStats>>,
    path: &std::path::Path,
    counters: Option<&SlateDBIoCounters>,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    for &(query, digest, rows) in &expected.digests {
        for sample in 0..parameters.warmups + parameters.samples {
            let _ = take_stats(stats);
            let physical_before = counters.map(SlateDBIoCounters::snapshot);
            let rss_before = process_resident_bytes();
            let cpu_before = process_cpu_nanos();
            begin_allocation_profile();
            let started = Instant::now();
            let result = execute(tree, query).await;
            let wall_us = started.elapsed().as_secs_f64() * 1_000_000.0;
            let cpu_us = process_cpu_nanos().saturating_sub(cpu_before) as f64 / 1_000.0;
            let (allocated_bytes, allocation_calls) = end_allocation_profile();
            let rss_after = process_resident_bytes();
            assert_eq!(result.len(), rows);
            assert_eq!(common::digest(&result), digest);
            let logical = take_stats(stats);
            let physical = physical_delta(counters, physical_before);
            if sample >= parameters.warmups {
                println!(
                    "forktree_olap,sample={},backend={},layout=forktree,rows={},query={},wall_us={wall_us:.3},cpu_us={cpu_us:.3},alloc_bytes={allocated_bytes},alloc_calls={allocation_calls},rss_before_bytes={rss_before},rss_after_bytes={rss_after},begin_reads={},get_calls={},get_keys={},get_values={},get_value_bytes={},scan_calls={},scan_entries={},scan_value_bytes={},physical_read_objects={},physical_read_bytes={},physical_write_objects={},physical_write_bytes={},logical_result_rows={},disk_bytes={}",
                    sample - parameters.warmups + 1,
                    parameters.backend.label(),
                    parameters.rows,
                    query.label(),
                    logical.begin_reads,
                    logical.get_calls,
                    logical.get_keys,
                    logical.get_values,
                    logical.get_value_bytes,
                    logical.scan_calls,
                    logical.scan_entries,
                    logical.scan_value_bytes,
                    physical.read_objects,
                    physical.read_bytes,
                    physical.write_objects,
                    physical.write_bytes,
                    result.len(),
                    directory_bytes(path),
                );
            }
            std::hint::black_box(result);
        }
    }
}

async fn execute<S>(tree: &ForkTree<CountingStorage<S>>, query: Query) -> Vec<Vec<Cell>>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    if query == Query::Projection {
        return load_wide_projection(tree).await;
    }
    let narrow = if matches!(
        query,
        Query::NarrowScan | Query::Filter | Query::Group | Query::OrderLimit | Query::Join
    ) {
        load_narrow(tree).await
    } else {
        Vec::new()
    };
    let wide = if matches!(query, Query::WideScan | Query::Projection) {
        load_wide(tree).await
    } else {
        Vec::new()
    };
    let dimensions = if query == Query::Join {
        load_dimensions(tree).await
    } else {
        Vec::new()
    };
    common::evaluate(query, &narrow, &wide, &dimensions)
}

async fn load_narrow<S>(tree: &ForkTree<CountingStorage<S>>) -> Vec<NarrowRow>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    tree.read_projected_range("main", b"n/", b"n0", |value| Ok(value.to_vec()))
        .await
        .expect("read authenticated narrow range")
        .into_iter()
        .map(|(key, value)| common::decode_narrow(common::strip_key(b'n', &key), &value))
        .collect()
}

async fn load_wide<S>(tree: &ForkTree<CountingStorage<S>>) -> Vec<WideRow>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    tree.read_projected_range("main", b"w/", b"w0", |value| Ok(value.to_vec()))
        .await
        .expect("read authenticated wide range")
        .into_iter()
        .map(|(key, value)| common::decode_wide(common::strip_key(b'w', &key), &value))
        .collect()
}

async fn load_wide_projection<S>(tree: &ForkTree<CountingStorage<S>>) -> Vec<Vec<Cell>>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    tree.read_projected_range("main", b"w/", b"w0", |value| {
        let score = value
            .get(16..24)
            .ok_or_else(|| "projected wide row is truncated before score".to_string())?;
        Ok(i64::from_be_bytes(
            score.try_into().expect("validated score width"),
        ))
    })
    .await
    .expect("read authenticated projected wide range")
    .into_iter()
    .map(|(key, score)| {
        vec![
            Cell::Text(common::strip_key(b'w', &key)),
            Cell::Integer(score),
        ]
    })
    .collect()
}

async fn load_dimensions<S>(tree: &ForkTree<CountingStorage<S>>) -> Vec<(i64, String)>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    tree.read_projected_range("main", b"d/", b"d0", |value| Ok(value.to_vec()))
        .await
        .expect("read authenticated dimension range")
        .into_iter()
        .map(|(key, value)| {
            let lane = common::strip_key(b'd', &key)
                .parse::<i64>()
                .expect("dimension lane integer");
            let label = String::from_utf8(value).expect("dimension label UTF-8");
            (lane, label)
        })
        .collect()
}

async fn verify_reopen<S>(tree: ForkTree<CountingStorage<S>>, expected: &Expected)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    for &(query, digest, rows) in &expected.digests {
        let result = execute(&tree, query).await;
        assert_eq!(result.len(), rows);
        assert_eq!(common::digest(&result), digest);
    }
}
