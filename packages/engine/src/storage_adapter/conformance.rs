use std::ops::Bound;

use bytes::Bytes;

use crate::storage::{
    CoreProjection, GetOptions, Key, KeyRange, Memory, Prefix, ProjectedValue, ReadOptions,
    ScanOptions, SpaceId, StoredValue, WriteOptions,
};
use crate::storage_adapter::{
    PointReadPlan, ScanPlan, StorageAdapter, StorageReadStats, StorageReadStatsCollector,
    StorageSpace, StorageWriteSetError,
};

type StorageConformanceResult = Result<(), String>;

#[derive(Debug, PartialEq, Eq)]
enum StorageConformanceStatus {
    Passed,
    Failed(String),
}

#[derive(Debug, PartialEq, Eq)]
struct StorageConformanceReport {
    tests: Vec<StorageConformanceTestResult>,
}

#[derive(Debug, PartialEq, Eq)]
struct StorageConformanceTestResult {
    name: &'static str,
    status: StorageConformanceStatus,
}

impl StorageConformanceReport {
    fn assert_no_failures(&self) {
        let failures = self
            .tests
            .iter()
            .filter_map(|test| match &test.status {
                StorageConformanceStatus::Passed => None,
                StorageConformanceStatus::Failed(error) => Some((test.name, error.as_str())),
            })
            .collect::<Vec<_>>();

        assert!(
            failures.is_empty(),
            "storage conformance failures: {failures:?}"
        );
    }
}

async fn run_storage_conformance() -> StorageConformanceReport {
    let mut report = StorageConformanceReport { tests: Vec::new() };
    macro_rules! run {
        ($name:literal, $test:ident) => {
            report.tests.push(StorageConformanceTestResult {
                name: $name,
                status: match $test().await {
                    Ok(()) => StorageConformanceStatus::Passed,
                    Err(error) => StorageConformanceStatus::Failed(error),
                },
            });
        };
    }
    run!(
        "write_set_commits_and_reads_back",
        write_set_commits_and_reads_back
    );
    run!(
        "point_reads_preserve_caller_order_duplicates_and_missing",
        point_reads_preserve_caller_order_duplicates_and_missing
    );
    run!(
        "prefix_scan_lowers_to_storage_range",
        prefix_scan_lowers_to_storage_range
    );
    run!(
        "scan_stats_collector_accumulates_chunked_drain_shape",
        scan_stats_collector_accumulates_chunked_drain_shape
    );
    run!("read_scope_pins_snapshot", read_scope_pins_snapshot);
    run!(
        "write_set_rejects_conflicting_space_declarations",
        write_set_rejects_conflicting_space_declarations
    );
    report
}

async fn write_set_commits_and_reads_back() -> StorageConformanceResult {
    let storage = StorageAdapter::new(Memory::new());
    let mut writes = storage.new_write_set();
    writes.put(space_one(), key("a"), value("A"));
    writes.put(space_one(), key("b"), value("B"));
    writes.put(space_two(), key("a"), value("space-two"));
    writes.delete(space_one(), key("missing"));

    let (_commit, stats) = storage
        .commit_write_set(writes, WriteOptions::default())
        .await
        .map_err(|error| format!("commit_write_set failed: {error}"))?;

    assert_eq!(stats.staged_puts, 3);
    assert_eq!(stats.staged_deletes, 1);
    assert_eq!(stats.touched_spaces, 2);
    assert_eq!(stats.put_batches, 2);
    assert_eq!(stats.delete_batches, 1);

    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let result = PointReadPlan::new(space_one(), &[key("a"), key("b")])
        .materialize(&read, GetOptions::default())
        .await
        .map_err(|error| format!("get_many failed: {error}"))?;

    assert_eq!(
        result.value,
        vec![
            Some(ProjectedValue::FullValue(Bytes::from_static(b"A"))),
            Some(ProjectedValue::FullValue(Bytes::from_static(b"B"))),
        ]
    );

    Ok(())
}

async fn point_reads_preserve_caller_order_duplicates_and_missing() -> StorageConformanceResult {
    let storage = StorageAdapter::new(Memory::new());
    let mut writes = storage.new_write_set();
    writes.put(space_one(), key("a"), value("A"));
    writes.put(space_one(), key("b"), value("B"));
    storage
        .commit_write_set(writes, WriteOptions::default())
        .await
        .map_err(|error| format!("seed failed: {error}"))?;

    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let result = PointReadPlan::new(space_one(), &[key("b"), key("missing"), key("a"), key("b")])
        .materialize(
            &read,
            GetOptions {
                projection: CoreProjection::KeyOnly,
            },
        )
        .await
        .map_err(|error| format!("get_many failed: {error}"))?;

    assert_eq!(
        result.value,
        vec![
            Some(ProjectedValue::KeyOnly),
            None,
            Some(ProjectedValue::KeyOnly),
            Some(ProjectedValue::KeyOnly),
        ]
    );

    Ok(())
}

async fn prefix_scan_lowers_to_storage_range() -> StorageConformanceResult {
    let storage = StorageAdapter::new(Memory::new());
    let mut writes = storage.new_write_set();
    writes.put(space_one(), key("aa"), value("AA"));
    writes.put(space_one(), key("ab"), value("AB"));
    writes.put(space_one(), key("b"), value("B"));
    storage
        .commit_write_set(writes, WriteOptions::default())
        .await
        .map_err(|error| format!("seed failed: {error}"))?;

    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let chunk = ScanPlan::prefix(
        space_one(),
        Prefix {
            bytes: Bytes::from_static(b"a"),
        },
    )
    .collect(&read, ScanOptions::default())
    .await
    .map_err(|error| format!("scan_prefix failed: {error}"))?;

    assert_eq!(
        chunk
            .value
            .entries
            .into_iter()
            .map(|entry| entry.key)
            .collect::<Vec<_>>(),
        vec![key("aa"), key("ab")]
    );

    Ok(())
}

async fn scan_stats_collector_accumulates_chunked_drain_shape() -> StorageConformanceResult {
    let storage = StorageAdapter::new(Memory::new());
    let mut writes = storage.new_write_set();
    for suffix in ["0", "1", "2", "3", "4"] {
        writes.put(
            space_one(),
            key_with_prefix("item-", suffix),
            value("value"),
        );
    }
    storage
        .commit_write_set(writes, WriteOptions::default())
        .await
        .map_err(|error| format!("seed failed: {error}"))?;

    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let mut collector = StorageReadStatsCollector::new();
    let mut resume_after = None::<Key>;
    let mut emitted = 0usize;

    loop {
        let result = ScanPlan::prefix(
            space_one(),
            Prefix {
                bytes: Bytes::from_static(b"item-"),
            },
        )
        .collect(
            &read,
            ScanOptions {
                projection: CoreProjection::KeyOnly,
                limit_rows: 2,
                resume_after,
            },
        )
        .await
        .map_err(|error| format!("scan plan collect failed: {error}"))?;

        if result
            .value
            .entries
            .iter()
            .any(|entry| !matches!(entry.value, ProjectedValue::KeyOnly))
        {
            return Err("expected key-only scan value".to_string());
        }
        emitted += result.value.entries.len();
        collector.record(result.stats);
        resume_after = result.value.entries.last().map(|entry| entry.key.clone());

        if !result.value.has_more {
            break;
        }
    }

    let stats = collector.snapshot();
    assert_eq!(emitted, 5);
    assert_eq!(stats.storage_calls, 3);
    assert_eq!(stats.prefix_lowered, 3);
    assert_eq!(stats.prefix_scan_chunks, 3);
    assert_eq!(stats.range_scan_chunks, 0);
    assert_eq!(stats.scan_key_only_chunks, 3);
    assert_eq!(stats.scan_full_value_chunks, 0);
    assert_eq!(stats.scan_rows, 5);
    assert_eq!(stats.scan_has_more, 2);
    assert_eq!(stats.scan_resume_after, 2);
    assert_eq!(stats.scan_limit_rows_total, 6);
    assert_eq!(stats.scan_limit_rows_max, 2);

    let before_reset = stats;
    collector.reset();
    assert_eq!(collector.snapshot(), StorageReadStats::default());
    assert_ne!(before_reset, collector.snapshot());

    Ok(())
}

async fn read_scope_pins_snapshot() -> StorageConformanceResult {
    let storage = StorageAdapter::new(Memory::new());
    let mut seed = storage.new_write_set();
    seed.put(space_one(), key("a"), value("A"));
    storage
        .commit_write_set(seed, WriteOptions::default())
        .await
        .map_err(|error| format!("seed failed: {error}"))?;

    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin_read failed: {error}"))?;

    let mut later = storage.new_write_set();
    later.put(space_one(), key("a"), value("B"));
    storage
        .commit_write_set(later, WriteOptions::default())
        .await
        .map_err(|error| format!("later commit failed: {error}"))?;

    let chunk = ScanPlan::range(
        space_one(),
        KeyRange {
            lower: Bound::Included(key("a")),
            upper: Bound::Included(key("a")),
        },
    )
    .collect(&read, ScanOptions::default())
    .await
    .map_err(|error| format!("scan_range failed: {error}"))?;

    assert_eq!(
        chunk
            .value
            .entries
            .into_iter()
            .map(|entry| entry.value)
            .collect::<Vec<_>>(),
        vec![ProjectedValue::FullValue(Bytes::from_static(b"A"))]
    );

    Ok(())
}

async fn write_set_rejects_conflicting_space_declarations() -> StorageConformanceResult {
    let storage = StorageAdapter::new(Memory::new());
    let mut writes = storage.new_write_set();
    writes.put(space_one(), key("a"), value("A"));
    writes.put(
        StorageSpace::new(SpaceId(1), "storage.conformance.renamed"),
        key("b"),
        value("B"),
    );

    match storage
        .commit_write_set(writes, WriteOptions::default())
        .await
    {
        Err(StorageWriteSetError::ConflictingSpaceDeclaration {
            id: SpaceId(1),
            existing_name: "storage.conformance.one",
            incoming_name: "storage.conformance.renamed",
        }) => Ok(()),
        other => Err(format!(
            "expected conflicting space declaration, got {other:?}"
        )),
    }
}

fn space_one() -> StorageSpace {
    StorageSpace::new(SpaceId(1), "storage.conformance.one")
}

fn space_two() -> StorageSpace {
    StorageSpace::new(SpaceId(2), "storage.conformance.two")
}

fn key(bytes: &'static str) -> Key {
    Key(Bytes::from_static(bytes.as_bytes()))
}

fn key_with_prefix(prefix: &'static str, suffix: &'static str) -> Key {
    let mut bytes = Vec::with_capacity(prefix.len() + suffix.len());
    bytes.extend_from_slice(prefix.as_bytes());
    bytes.extend_from_slice(suffix.as_bytes());
    Key(Bytes::from(bytes))
}

fn value(bytes: &'static str) -> StoredValue {
    StoredValue {
        bytes: Bytes::from_static(bytes.as_bytes()),
    }
}

#[cfg(test)]
mod tests {
    use super::{StorageConformanceStatus, run_storage_conformance};

    #[tokio::test]
    async fn memory_passes_storage_conformance() {
        let report = run_storage_conformance().await;

        report.assert_no_failures();

        let passed = report
            .tests
            .iter()
            .filter(|test| matches!(test.status, StorageConformanceStatus::Passed))
            .map(|test| test.name)
            .collect::<Vec<_>>();
        assert_eq!(
            passed,
            vec![
                "write_set_commits_and_reads_back",
                "point_reads_preserve_caller_order_duplicates_and_missing",
                "prefix_scan_lowers_to_storage_range",
                "scan_stats_collector_accumulates_chunked_drain_shape",
                "read_scope_pins_snapshot",
                "write_set_rejects_conflicting_space_declarations",
            ]
        );
    }
}
