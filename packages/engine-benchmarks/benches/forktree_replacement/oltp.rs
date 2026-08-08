//! Benchmark-only OLTP driver for the accepted bc823 ForkTree storage model.
//!
//! This is deliberately a direct typed-owner comparison. It is not wired to
//! Lix SQL and does not claim branch/history equivalence with standalone SQL.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use lix_storage_rocksdb::RocksDB;

use super::model::{ForkTree, Mutation, RelationalValue};
use super::{
    CountingStorage, IoStats, begin_allocation_profile, directory_bytes, end_allocation_profile,
    process_cpu_nanos, process_resident_bytes,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Setup,
    Run,
}

impl Mode {
    fn parse(value: &str) -> Self {
        match value {
            "setup" => Self::Setup,
            "run" => Self::Run,
            other => panic!("unknown OLTP mode '{other}', expected setup or run"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Operation {
    Insert,
    PointRead,
    RangeRead,
    UpdateOnePercent,
    UpdateTenPercent,
    Delete,
    Atomic18,
    Upsert,
    Returning,
}

impl Operation {
    fn parse(value: &str) -> Self {
        match value {
            "insert" => Self::Insert,
            "point_read" => Self::PointRead,
            "range_read" => Self::RangeRead,
            "update_1pct" => Self::UpdateOnePercent,
            "update_10pct" => Self::UpdateTenPercent,
            "delete" => Self::Delete,
            "atomic_18" => Self::Atomic18,
            "upsert" => Self::Upsert,
            "returning" => Self::Returning,
            other => panic!("unknown OLTP operation '{other}'"),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::PointRead => "point_read",
            Self::RangeRead => "range_read",
            Self::UpdateOnePercent => "update_1pct",
            Self::UpdateTenPercent => "update_10pct",
            Self::Delete => "delete",
            Self::Atomic18 => "atomic_18",
            Self::Upsert => "upsert",
            Self::Returning => "returning",
        }
    }

    const fn starts_empty(self) -> bool {
        matches!(self, Self::Insert)
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct ProcessIo {
    read_calls: u64,
    write_calls: u64,
    read_bytes: u64,
    write_bytes: u64,
}

impl ProcessIo {
    fn delta(self, before: Self) -> Self {
        Self {
            read_calls: self.read_calls.saturating_sub(before.read_calls),
            write_calls: self.write_calls.saturating_sub(before.write_calls),
            read_bytes: self.read_bytes.saturating_sub(before.read_bytes),
            write_bytes: self.write_bytes.saturating_sub(before.write_bytes),
        }
    }
}

pub async fn run() {
    let args = std::env::args().collect::<Vec<_>>();
    assert_eq!(
        args.len(),
        7,
        "usage: forktree_replacement oltp <setup|run> <path> <rows> <operation> <batch_size>"
    );
    let mode = Mode::parse(&args[2]);
    let path = PathBuf::from(&args[3]);
    let rows = args[4].parse::<usize>().expect("rows must be an integer");
    let operation = Operation::parse(&args[5]);
    let batch_size = args[6]
        .parse::<usize>()
        .expect("batch size must be an integer");
    assert!(rows >= 18 && batch_size > 0);
    println!(
        "oltp_comparator_contract,path=forktree_bc823_direct_typed_owner,mode={mode:?},rows={rows},operation={},batch_size={batch_size},sql_integrated=false,branch_history_enabled=true,setup_excluded=true,backend=rocksdb",
        operation.label()
    );
    match mode {
        Mode::Setup => setup(&path, rows, operation).await,
        Mode::Run => measure(&path, rows, operation, batch_size).await,
    }
}

async fn setup(path: &Path, rows: usize, operation: Operation) {
    std::fs::create_dir_all(path).expect("create ForkTree comparator directory");
    let database = RocksDB::open(path).expect("open ForkTree setup RocksDB");
    let tree = ForkTree::new(database.clone());
    let initial = setup_rows(rows, operation);
    if initial.is_empty() {
        let bootstrap = vec![(
            row_key(0).into_bytes(),
            row_value(0, "bootstrap").into_bytes(),
        )];
        tree.initialize(&bootstrap)
            .await
            .expect("initialize empty ForkTree fixture bootstrap");
        tree.apply_sorted_mutations(&[Mutation::Delete {
            key: row_key(0).into_bytes(),
        }])
        .await
        .expect("remove ForkTree fixture bootstrap");
    } else {
        let encoded = initial
            .iter()
            .map(|(key, value)| (key.as_bytes().to_vec(), value.as_bytes().to_vec()))
            .collect::<Vec<_>>();
        tree.initialize(&encoded)
            .await
            .expect("initialize ForkTree fixture");
    }
    assert_eq!(tree_rows(&tree).await, initial);
    drop(tree);
    database.flush().expect("flush ForkTree setup");
    drop(database);
    println!(
        "oltp_comparator_setup,path=forktree_bc823_direct_typed_owner,rows={rows},operation={},digest={},disk_bytes={}",
        operation.label(),
        map_digest(&initial),
        directory_bytes(path)
    );
}

async fn measure(path: &Path, rows: usize, operation: Operation, batch_size: usize) {
    let database = RocksDB::open(path).expect("open measured ForkTree RocksDB");
    let (storage, stats) = CountingStorage::new(database.clone());
    let tree = ForkTree::new(storage);
    let _ = tree
        .read_relational_point("main", row_key(0).as_bytes())
        .await
        .expect("warm ForkTree point owner");
    *stats.lock().expect("ForkTree stats mutex") = IoStats::default();

    let disk_before = directory_bytes(path);
    let io_before = process_io();
    let rss_before = process_resident_bytes();
    let peak_before = peak_resident_bytes();
    let cpu_before = process_cpu_nanos();
    begin_allocation_profile();
    let started = Instant::now();
    let (result_digest, commits) = operation_run(&tree, rows, operation, batch_size).await;
    let wall_us = started.elapsed().as_secs_f64() * 1_000_000.0;
    let (alloc_bytes, alloc_calls) = end_allocation_profile();
    let cpu_us = process_cpu_nanos().saturating_sub(cpu_before) as f64 / 1_000.0;
    let rss_after = process_resident_bytes();
    let peak_after = peak_resident_bytes();
    let io = process_io().delta(io_before);
    let backend = stats.lock().expect("ForkTree stats mutex").clone();
    let disk_after = directory_bytes(path);

    let expected = expected_after(rows, operation);
    let actual = tree_rows(&tree).await;
    assert_eq!(actual, expected, "ForkTree post-operation rows");
    let state_digest = map_digest(&actual);
    drop(tree);
    let flush_started = Instant::now();
    database.flush().expect("flush measured ForkTree operation");
    let flush_us = flush_started.elapsed().as_secs_f64() * 1_000_000.0;
    drop(database);
    let settled_disk = directory_bytes(path);

    let reopened = RocksDB::open(path).expect("cold reopen ForkTree RocksDB");
    let reopened_tree = ForkTree::new(reopened.clone());
    let cold = tree_rows(&reopened_tree).await;
    assert_eq!(cold, expected, "ForkTree cold-reopen rows");
    let cold_digest = map_digest(&cold);
    assert_eq!(cold_digest, state_digest);

    println!(
        "oltp_comparator_result,path=forktree_bc823_direct_typed_owner,rows={rows},operation={},batch_size={batch_size},wall_us={wall_us:.3},cpu_us={cpu_us:.3},alloc_bytes={alloc_bytes},alloc_calls={alloc_calls},rss_before_bytes={rss_before},rss_after_bytes={rss_after},peak_before_bytes={peak_before},peak_after_bytes={peak_after},process_read_calls={},process_write_calls={},process_read_bytes={},process_write_bytes={},backend_begin_reads={},backend_begin_writes={},backend_get_calls={},backend_get_keys={},backend_get_values={},backend_get_value_bytes={},backend_scan_calls={},backend_scan_entries={},backend_scan_value_bytes={},backend_write_batches={},backend_write_puts={},backend_write_deletes={},backend_write_ranges={},backend_write_bytes={},backend_commits={},logical_commits={commits},disk_before_bytes={disk_before},disk_after_bytes={disk_after},flush_us={flush_us:.3},settled_disk_bytes={settled_disk},result_digest={result_digest},state_digest={state_digest},cold_digest={cold_digest},verified=true,returning_equivalent=direct_owner_postimage",
        operation.label(),
        io.read_calls,
        io.write_calls,
        io.read_bytes,
        io.write_bytes,
        backend.begin_reads,
        backend.begin_writes,
        backend.get_calls,
        backend.get_keys,
        backend.get_values,
        backend.get_value_bytes,
        backend.scan_calls,
        backend.scan_entries,
        backend.scan_value_bytes,
        backend.write_batches,
        backend.write_puts,
        backend.write_deletes,
        backend.write_ranges,
        backend.write_bytes,
        backend.commits,
    );
}

async fn operation_run<S>(
    tree: &ForkTree<CountingStorage<S>>,
    rows: usize,
    operation: Operation,
    batch_size: usize,
) -> (String, u64)
where
    S: lix::storage::Storage + Clone + Send + Sync + 'static,
{
    match operation {
        Operation::PointRead => {
            let mut result = BTreeMap::new();
            for index in 0..rows {
                let key = row_key(index);
                let value = tree
                    .read_relational_point("main", key.as_bytes())
                    .await
                    .expect("ForkTree point read")
                    .expect("ForkTree point row exists");
                result.insert(key, relational_text(value));
            }
            (map_digest(&result), 0)
        }
        Operation::RangeRead => {
            let count = update_count(rows, 10);
            let start = rows / 4;
            let end = (start + count).min(rows);
            let result = tree
                .read_range(
                    "main",
                    row_key(start).as_bytes(),
                    row_key(end.saturating_sub(1)).as_bytes(),
                )
                .await
                .expect("ForkTree range read")
                .into_iter()
                .map(|(key, value)| {
                    (
                        String::from_utf8(key).expect("UTF-8 ForkTree key"),
                        String::from_utf8(value).expect("UTF-8 ForkTree value"),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            assert_eq!(result.len(), end - start);
            (map_digest(&result), 0)
        }
        Operation::Insert => {
            let mutations = base_rows(rows)
                .into_iter()
                .map(|(key, value)| Mutation::Insert {
                    key: key.into_bytes(),
                    value: RelationalValue::Bytes(value.into_bytes()),
                })
                .collect::<Vec<_>>();
            let commits = apply_chunks(tree, &mutations, batch_size).await;
            (format!("affected:{rows}"), commits)
        }
        Operation::UpdateOnePercent | Operation::UpdateTenPercent => {
            let (percent, lane) = if operation == Operation::UpdateOnePercent {
                (1, "update-1")
            } else {
                (10, "update-10")
            };
            let mutations = target_keys(rows, update_count(rows, percent))
                .into_iter()
                .map(|index| Mutation::Update {
                    key: row_key(index).into_bytes(),
                    value: RelationalValue::Bytes(row_value(index, lane).into_bytes()),
                })
                .collect::<Vec<_>>();
            let affected = mutations.len();
            let commits = apply_chunks(tree, &mutations, batch_size).await;
            (format!("affected:{affected}"), commits)
        }
        Operation::Delete => {
            let mutations = (0..rows)
                .map(|index| Mutation::Delete {
                    key: row_key(index).into_bytes(),
                })
                .collect::<Vec<_>>();
            let commits = apply_chunks(tree, &mutations, batch_size).await;
            (format!("affected:{rows}"), commits)
        }
        Operation::Atomic18 => {
            let mut mutations = Vec::with_capacity(18);
            for index in 0..6 {
                mutations.push(Mutation::Update {
                    key: row_key(index).into_bytes(),
                    value: RelationalValue::Bytes(row_value(index, "atomic-update").into_bytes()),
                });
            }
            for index in 6..12 {
                mutations.push(Mutation::Delete {
                    key: row_key(index).into_bytes(),
                });
            }
            for index in 0..6 {
                mutations.push(Mutation::Insert {
                    key: new_key(index).into_bytes(),
                    value: RelationalValue::Bytes(row_value(index, "atomic-insert").into_bytes()),
                });
            }
            mutations.sort_by(|left, right| left.key().cmp(right.key()));
            tree.apply_sorted_mutations(&mutations)
                .await
                .expect("apply ForkTree atomic18");
            ("affected:18".to_string(), 1)
        }
        Operation::Upsert => {
            let mutations = (0..rows)
                .map(|index| {
                    if index % 2 == 0 {
                        Mutation::Update {
                            key: row_key(index).into_bytes(),
                            value: RelationalValue::Bytes(
                                row_value(index, "upsert-update").into_bytes(),
                            ),
                        }
                    } else {
                        Mutation::Insert {
                            key: new_key(index).into_bytes(),
                            value: RelationalValue::Bytes(
                                row_value(index, "upsert-insert").into_bytes(),
                            ),
                        }
                    }
                })
                .collect::<Vec<_>>();
            let mut sorted = mutations;
            sorted.sort_by(|left, right| left.key().cmp(right.key()));
            let commits = apply_chunks(tree, &sorted, batch_size).await;
            (format!("affected:{rows}"), commits)
        }
        Operation::Returning => {
            let targets = target_keys(rows, update_count(rows, 10));
            let mutations = targets
                .iter()
                .map(|&index| Mutation::Update {
                    key: row_key(index).into_bytes(),
                    value: RelationalValue::Bytes(row_value(index, "update-10").into_bytes()),
                })
                .collect::<Vec<_>>();
            let mut returned = BTreeMap::new();
            let mut commits = 0;
            for chunk in mutations.chunks(batch_size) {
                tree.apply_sorted_mutations(chunk)
                    .await
                    .expect("apply ForkTree RETURNING-equivalent chunk");
                commits += 1;
                for mutation in chunk {
                    let key = String::from_utf8(mutation.key().to_vec()).expect("UTF-8 key");
                    let value = tree
                        .read_relational_point("main", mutation.key())
                        .await
                        .expect("read ForkTree postimage")
                        .expect("ForkTree postimage exists");
                    returned.insert(key, relational_text(value));
                }
            }
            assert_eq!(returned, returned_expected(rows));
            (map_digest(&returned), commits)
        }
    }
}

async fn apply_chunks<S>(
    tree: &ForkTree<CountingStorage<S>>,
    mutations: &[Mutation],
    batch_size: usize,
) -> u64
where
    S: lix::storage::Storage + Clone + Send + Sync + 'static,
{
    let mut commits = 0;
    for chunk in mutations.chunks(batch_size) {
        tree.apply_sorted_mutations(chunk)
            .await
            .expect("apply ForkTree mutation chunk");
        commits += 1;
    }
    commits
}

async fn tree_rows<S>(tree: &ForkTree<S>) -> BTreeMap<String, String>
where
    S: lix::storage::Storage + Clone + Send + Sync + 'static,
{
    tree.read_relational_all("main")
        .await
        .expect("read all ForkTree rows")
        .into_iter()
        .map(|(key, value)| {
            (
                String::from_utf8(key).expect("UTF-8 ForkTree key"),
                relational_text(value),
            )
        })
        .collect()
}

fn relational_text(value: RelationalValue) -> String {
    match value {
        RelationalValue::Bytes(bytes) => String::from_utf8(bytes).expect("UTF-8 ForkTree value"),
        RelationalValue::Null => panic!("comparator does not generate NULL values"),
    }
}

fn setup_rows(rows: usize, operation: Operation) -> BTreeMap<String, String> {
    if operation.starts_empty() {
        BTreeMap::new()
    } else {
        base_rows(rows)
    }
}

fn base_rows(rows: usize) -> BTreeMap<String, String> {
    (0..rows)
        .map(|index| (row_key(index), row_value(index, "base")))
        .collect()
}

fn expected_after(rows: usize, operation: Operation) -> BTreeMap<String, String> {
    let mut expected = setup_rows(rows, operation);
    match operation {
        Operation::Insert => expected = base_rows(rows),
        Operation::PointRead | Operation::RangeRead => {}
        Operation::UpdateOnePercent => {
            for index in target_keys(rows, update_count(rows, 1)) {
                expected.insert(row_key(index), row_value(index, "update-1"));
            }
        }
        Operation::UpdateTenPercent | Operation::Returning => {
            for index in target_keys(rows, update_count(rows, 10)) {
                expected.insert(row_key(index), row_value(index, "update-10"));
            }
        }
        Operation::Delete => expected.clear(),
        Operation::Atomic18 => {
            for index in 0..6 {
                expected.insert(row_key(index), row_value(index, "atomic-update"));
            }
            for index in 6..12 {
                expected.remove(&row_key(index));
            }
            for index in 0..6 {
                expected.insert(new_key(index), row_value(index, "atomic-insert"));
            }
        }
        Operation::Upsert => {
            for index in 0..rows {
                if index % 2 == 0 {
                    expected.insert(row_key(index), row_value(index, "upsert-update"));
                } else {
                    expected.insert(new_key(index), row_value(index, "upsert-insert"));
                }
            }
        }
    }
    expected
}

fn returned_expected(rows: usize) -> BTreeMap<String, String> {
    target_keys(rows, update_count(rows, 10))
        .into_iter()
        .map(|index| (row_key(index), row_value(index, "update-10")))
        .collect()
}

fn row_key(index: usize) -> String {
    format!("row-{index:09}")
}

fn new_key(index: usize) -> String {
    format!("new-{index:09}")
}

fn row_value(index: usize, lane: &str) -> String {
    format!(
        r#"{{"ordinal":{index},"lane":"{lane}","payload":"{:032}"}}"#,
        index % 10_000
    )
}

fn update_count(rows: usize, percent: usize) -> usize {
    (rows.saturating_mul(percent) / 100).max(1).min(rows)
}

fn target_keys(rows: usize, count: usize) -> Vec<usize> {
    if count >= rows {
        return (0..rows).collect();
    }
    let mut result = (0..count)
        .map(|ordinal| ordinal.saturating_mul(rows) / count)
        .collect::<Vec<_>>();
    result.sort_unstable();
    result.dedup();
    result
}

fn map_digest(rows: &BTreeMap<String, String>) -> String {
    let mut hasher = blake3::Hasher::new();
    let mut count = 0_u64;
    for (key, value) in rows {
        hasher.update(&(key.len() as u64).to_le_bytes());
        hasher.update(key.as_bytes());
        hasher.update(&(value.len() as u64).to_le_bytes());
        hasher.update(value.as_bytes());
        count += 1;
    }
    hasher.update(&count.to_le_bytes());
    hasher.finalize().to_hex().to_string()
}

fn process_io() -> ProcessIo {
    let Ok(contents) = std::fs::read_to_string("/proc/self/io") else {
        return ProcessIo::default();
    };
    let mut result = ProcessIo::default();
    for line in contents.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim().parse::<u64>().unwrap_or(0);
        match name {
            "syscr" => result.read_calls = value,
            "syscw" => result.write_calls = value,
            "read_bytes" => result.read_bytes = value,
            "write_bytes" => result.write_bytes = value,
            _ => {}
        }
    }
    result
}

fn peak_resident_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find_map(|line| line.strip_prefix("VmHWM:"))
                .and_then(|value| value.split_whitespace().next())
                .and_then(|value| value.parse::<u64>().ok())
        })
        .map_or(0, |kilobytes| kilobytes.saturating_mul(1_024))
}
