use std::alloc::GlobalAlloc;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use duckdb::arrow::array::{
    Array, BooleanArray, Decimal128Array, Int32Array, Int64Array, StringArray, UInt64Array,
};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::types::Value as DuckValue;
use duckdb::{AccessMode, Config, Connection, appender_params_from_iter};

#[path = "../../../packages/engine-benchmarks/benches/forktree_replacement/olap_common.rs"]
#[allow(dead_code)]
mod common;
use common::{Cell, Query};

#[global_allocator]
static ALLOCATOR: AllocationCounter = AllocationCounter;
struct AllocationCounter;
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static ALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOC_ON: AtomicBool = AtomicBool::new(false);

unsafe impl GlobalAlloc for AllocationCounter {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let pointer = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !pointer.is_null() && ALLOC_ON.load(Ordering::Relaxed) {
            ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: std::alloc::Layout) {
        unsafe { mimalloc::MiMalloc.dealloc(pointer, layout) };
    }

    unsafe fn realloc(
        &self,
        pointer: *mut u8,
        layout: std::alloc::Layout,
        new_size: usize,
    ) -> *mut u8 {
        let replacement = unsafe { mimalloc::MiMalloc.realloc(pointer, layout, new_size) };
        if !replacement.is_null() && new_size >= layout.size() && ALLOC_ON.load(Ordering::Relaxed) {
            ALLOC_BYTES.fetch_add((new_size - layout.size()) as u64, Ordering::Relaxed);
            ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        replacement
    }
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let rows = parse(args.get(1), 10_000);
    let samples = parse(args.get(2), 3);
    let warmups = parse(args.get(3), 1);
    let directory = tempfile::tempdir().expect("DuckDB OLAP directory");
    let database = directory.path().join("olap.duckdb");
    println!(
        "forktree_duckdb_boundary,engine=duckdb,version=1.10505.0,authority=none,version_control=false,authenticated_storage=false,comparison=query_engine_only"
    );

    let mut connection = Connection::open(&database).expect("open DuckDB OLAP database");
    seed(&mut connection, rows);
    connection
        .execute_batch("CHECKPOINT")
        .expect("checkpoint DuckDB setup");
    drop(connection);

    let connection = open_read_only(&database);
    let expected = expected(rows);
    for (query, digest, result_rows) in &expected {
        if rows == 10_000 {
            println!(
                "forktree_duckdb_plan,query={},plan={:?}",
                query.label(),
                explain(&connection, query.sql())
            );
        }
        for sample in 0..warmups + samples {
            let proc_before = ProcIo::read();
            let rss_before = rss();
            let cpu_before = cpu_nanos();
            begin_alloc();
            let started = Instant::now();
            let result = execute(&connection, query.sql());
            let wall_us = started.elapsed().as_secs_f64() * 1_000_000.0;
            let cpu_us = cpu_nanos().saturating_sub(cpu_before) as f64 / 1_000.0;
            let (alloc_bytes, alloc_calls) = end_alloc();
            let rss_after = rss();
            let proc_io = ProcIo::read().saturating_sub(proc_before);
            assert_eq!(result.len(), *result_rows);
            assert_eq!(common::digest(&result), *digest);
            assert_eq!(
                proc_io.write_bytes, 0,
                "read-only DuckDB query wrote storage bytes"
            );
            if sample >= warmups {
                println!(
                    "forktree_duckdb_olap,sample={},rows={},query={},wall_us={wall_us:.3},cpu_us={cpu_us:.3},rust_alloc_bytes={alloc_bytes},rust_alloc_calls={alloc_calls},allocation_scope=rust_output_only,rss_before_bytes={rss_before},rss_after_bytes={rss_after},logical_reads=unavailable,use_explain_plan=true,os_read_calls={},os_read_chars={},os_read_bytes={},os_write_calls={},os_write_bytes={},query_writes=0,logical_result_rows={},result_digest={},disk_bytes={}",
                    sample - warmups + 1,
                    rows,
                    query.label(),
                    proc_io.syscr,
                    proc_io.rchar,
                    proc_io.read_bytes,
                    proc_io.syscw,
                    proc_io.write_bytes,
                    result.len(),
                    hex_digest(*digest),
                    directory_bytes(directory.path()),
                );
            }
            std::hint::black_box(result);
        }
    }
    drop(connection);

    let reopened = open_read_only(&database);
    verify(&reopened, &expected);
    println!(
        "forktree_duckdb_reopen,rows={rows},exact_results=true,disk_bytes={},authority_semantics=absent",
        directory_bytes(directory.path())
    );
}

fn seed(connection: &mut Connection, rows: usize) {
    connection
        .execute_batch(
            "SET threads = 1;
             CREATE TABLE forktree_olap_narrow (
               id VARCHAR PRIMARY KEY, ordinal BIGINT, lane BIGINT, score BIGINT, active BOOLEAN
             );
             CREATE TABLE forktree_olap_wide (
               id VARCHAR PRIMARY KEY, ordinal BIGINT, lane BIGINT, score BIGINT, active BOOLEAN,
               c00 BIGINT, c01 BIGINT, c02 BIGINT, c03 BIGINT,
               c04 BIGINT, c05 BIGINT, c06 BIGINT, c07 BIGINT,
               c08 BIGINT, c09 BIGINT, c10 BIGINT, c11 BIGINT,
               c12 BIGINT, c13 BIGINT, c14 BIGINT, c15 BIGINT, payload VARCHAR
             );
             CREATE TABLE forktree_olap_dim (lane BIGINT PRIMARY KEY, label VARCHAR);",
        )
        .expect("create DuckDB OLAP tables");
    let transaction = connection.transaction().expect("begin DuckDB OLAP seed");
    {
        let mut appender = transaction
            .appender("forktree_olap_narrow")
            .expect("narrow DuckDB appender");
        for ordinal in 0..rows {
            let row = common::narrow_row(ordinal);
            appender
                .append_row(appender_params_from_iter([
                    DuckValue::Text(row.id),
                    DuckValue::BigInt(row.ordinal),
                    DuckValue::BigInt(row.lane),
                    DuckValue::BigInt(row.score),
                    DuckValue::Boolean(row.active),
                ]))
                .expect("append DuckDB narrow row");
        }
        appender.flush().expect("flush DuckDB narrow appender");
    }
    {
        let mut appender = transaction
            .appender("forktree_olap_wide")
            .expect("wide DuckDB appender");
        for ordinal in 0..rows {
            let row = common::wide_row(ordinal);
            let mut values = vec![
                DuckValue::Text(row.base.id),
                DuckValue::BigInt(row.base.ordinal),
                DuckValue::BigInt(row.base.lane),
                DuckValue::BigInt(row.base.score),
                DuckValue::Boolean(row.base.active),
            ];
            values.extend(row.columns.into_iter().map(DuckValue::BigInt));
            values.push(DuckValue::Text(row.payload));
            appender
                .append_row(appender_params_from_iter(values))
                .expect("append DuckDB wide row");
        }
        appender.flush().expect("flush DuckDB wide appender");
    }
    {
        let mut appender = transaction
            .appender("forktree_olap_dim")
            .expect("dimension DuckDB appender");
        for (lane, label) in common::dimension_rows() {
            appender
                .append_row(appender_params_from_iter([
                    DuckValue::BigInt(lane),
                    DuckValue::Text(label),
                ]))
                .expect("append DuckDB dimension row");
        }
        appender.flush().expect("flush DuckDB dimension appender");
    }
    transaction.commit().expect("commit DuckDB OLAP seed");
}

fn open_read_only(path: &std::path::Path) -> Connection {
    let config = Config::default()
        .access_mode(AccessMode::ReadOnly)
        .expect("DuckDB read-only configuration");
    Connection::open_with_flags(path, config).expect("open read-only DuckDB OLAP database")
}

fn explain(connection: &Connection, sql: &str) -> Vec<String> {
    let mut statement = connection
        .prepare(&format!("EXPLAIN {sql}"))
        .expect("prepare DuckDB EXPLAIN");
    statement
        .query_map([], |row| row.get::<_, String>(1))
        .expect("run DuckDB EXPLAIN")
        .map(|row| row.expect("read DuckDB EXPLAIN row"))
        .collect()
}

fn execute(connection: &Connection, sql: &str) -> Vec<Vec<Cell>> {
    let mut statement = connection.prepare(sql).expect("prepare DuckDB OLAP query");
    let batches = statement
        .query_arrow([])
        .expect("execute DuckDB OLAP query")
        .collect::<Vec<RecordBatch>>();
    batches_to_cells(batches)
}

fn batches_to_cells(batches: Vec<RecordBatch>) -> Vec<Vec<Cell>> {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let mut values = Vec::with_capacity(batch.num_columns());
            for column in batch.columns() {
                if column.is_null(row) {
                    values.push(Cell::Null);
                } else if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
                    values.push(Cell::Text(array.value(row).to_string()));
                } else if let Some(array) = column.as_any().downcast_ref::<Int32Array>() {
                    values.push(Cell::Integer(i64::from(array.value(row))));
                } else if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
                    values.push(Cell::Integer(array.value(row)));
                } else if let Some(array) = column.as_any().downcast_ref::<UInt64Array>() {
                    values.push(Cell::Integer(
                        i64::try_from(array.value(row)).expect("DuckDB UInt64 result fits i64"),
                    ));
                } else if let Some(array) = column.as_any().downcast_ref::<Decimal128Array>() {
                    assert_eq!(
                        array.scale(),
                        0,
                        "DuckDB aggregate decimal must be integral"
                    );
                    values.push(Cell::Integer(
                        i64::try_from(array.value(row)).expect("DuckDB Decimal128 result fits i64"),
                    ));
                } else if let Some(array) = column.as_any().downcast_ref::<BooleanArray>() {
                    values.push(Cell::Boolean(array.value(row)));
                } else {
                    panic!(
                        "unsupported DuckDB OLAP Arrow array {:?}",
                        column.data_type()
                    );
                }
            }
            rows.push(values);
        }
    }
    rows
}

fn expected(rows: usize) -> Vec<(Query, [u8; 32], usize)> {
    let narrow = (0..rows).map(common::narrow_row).collect::<Vec<_>>();
    let wide = (0..rows).map(common::wide_row).collect::<Vec<_>>();
    let dimensions = common::dimension_rows();
    Query::ALL
        .into_iter()
        .map(|query| {
            let result = common::evaluate(query, &narrow, &wide, &dimensions);
            (query, common::digest(&result), result.len())
        })
        .collect()
}

fn verify(connection: &Connection, expected: &[(Query, [u8; 32], usize)]) {
    for &(query, digest, rows) in expected {
        let result = execute(connection, query.sql());
        assert_eq!(result.len(), rows);
        assert_eq!(common::digest(&result), digest);
    }
}

fn begin_alloc() {
    ALLOC_BYTES.store(0, Ordering::Relaxed);
    ALLOC_CALLS.store(0, Ordering::Relaxed);
    ALLOC_ON.store(true, Ordering::Relaxed);
}

fn end_alloc() -> (u64, u64) {
    ALLOC_ON.store(false, Ordering::Relaxed);
    (
        ALLOC_BYTES.load(Ordering::Relaxed),
        ALLOC_CALLS.load(Ordering::Relaxed),
    )
}

#[derive(Clone, Copy, Default)]
struct ProcIo {
    rchar: u64,
    syscr: u64,
    read_bytes: u64,
    syscw: u64,
    write_bytes: u64,
}

impl ProcIo {
    fn read() -> Self {
        let mut result = Self::default();
        if let Ok(contents) = std::fs::read_to_string("/proc/self/io") {
            for line in contents.lines() {
                let Some((name, value)) = line.split_once(':') else {
                    continue;
                };
                let value = value.trim().parse().unwrap_or(0);
                match name {
                    "rchar" => result.rchar = value,
                    "syscr" => result.syscr = value,
                    "read_bytes" => result.read_bytes = value,
                    "syscw" => result.syscw = value,
                    "write_bytes" => result.write_bytes = value,
                    _ => {}
                }
            }
        }
        result
    }

    fn saturating_sub(self, before: Self) -> Self {
        Self {
            rchar: self.rchar.saturating_sub(before.rchar),
            syscr: self.syscr.saturating_sub(before.syscr),
            read_bytes: self.read_bytes.saturating_sub(before.read_bytes),
            syscw: self.syscw.saturating_sub(before.syscw),
            write_bytes: self.write_bytes.saturating_sub(before.write_bytes),
        }
    }
}

fn rss() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find_map(|line| line.strip_prefix("VmRSS:"))
                .and_then(|value| value.split_whitespace().next())
                .and_then(|value| value.parse::<u64>().ok())
        })
        .map_or(0, |kb| kb * 1024)
}

fn cpu_nanos() -> u64 {
    let mut value = std::mem::MaybeUninit::<libc::timespec>::uninit();
    if unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, value.as_mut_ptr()) } != 0 {
        return 0;
    }
    let value = unsafe { value.assume_init() };
    u64::try_from(value.tv_sec)
        .unwrap_or(0)
        .saturating_mul(1_000_000_000)
        .saturating_add(u64::try_from(value.tv_nsec).unwrap_or(0))
}

fn directory_bytes(path: &std::path::Path) -> u64 {
    std::fs::read_dir(path).map_or(0, |entries| {
        entries.flatten().fold(0_u64, |total, entry| {
            let path = entry.path();
            total.saturating_add(if path.is_dir() {
                directory_bytes(&path)
            } else {
                entry.metadata().map_or(0, |metadata| metadata.len())
            })
        })
    })
}

fn hex_digest(bytes: [u8; 32]) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        write!(encoded, "{byte:02x}").expect("write digest");
    }
    encoded
}

fn parse(value: Option<&String>, default: usize) -> usize {
    value.map_or(default, |value| value.parse().expect("positive integer"))
}
