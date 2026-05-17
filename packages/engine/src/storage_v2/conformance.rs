use std::ops::Bound;

use bytes::Bytes;

use crate::backend_v2::{
    ConformanceBackend, CoreProjection, GetOptions, Key, KeyRange, Prefix, ProjectedValue,
    ReadOptions, ScanOptions, SpaceId, StoredValue, WriteOptions,
};
use crate::storage_v2::{StorageContext, StorageReader, StorageSpace, StorageWriteSetError};

type StorageConformanceResult = Result<(), String>;

struct StorageConformanceTest {
    name: &'static str,
    run: fn() -> StorageConformanceResult,
}

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
            "storage_v2 conformance failures: {failures:?}"
        );
    }
}

fn run_storage_conformance() -> StorageConformanceReport {
    let tests = [
        StorageConformanceTest {
            name: "write_set_commits_and_reads_back",
            run: write_set_commits_and_reads_back,
        },
        StorageConformanceTest {
            name: "point_reads_preserve_caller_order_duplicates_and_missing",
            run: point_reads_preserve_caller_order_duplicates_and_missing,
        },
        StorageConformanceTest {
            name: "prefix_scan_lowers_to_backend_range",
            run: prefix_scan_lowers_to_backend_range,
        },
        StorageConformanceTest {
            name: "read_scope_pins_snapshot",
            run: read_scope_pins_snapshot,
        },
        StorageConformanceTest {
            name: "write_set_rejects_conflicting_space_declarations",
            run: write_set_rejects_conflicting_space_declarations,
        },
    ];

    StorageConformanceReport {
        tests: tests
            .into_iter()
            .map(|test| StorageConformanceTestResult {
                name: test.name,
                status: match (test.run)() {
                    Ok(()) => StorageConformanceStatus::Passed,
                    Err(error) => StorageConformanceStatus::Failed(error),
                },
            })
            .collect(),
    }
}

fn write_set_commits_and_reads_back() -> StorageConformanceResult {
    let storage = StorageContext::new(ConformanceBackend::new());
    let mut writes = storage.new_write_set();
    writes.stage_put(space_one(), key("a"), value("A"));
    writes.stage_put(space_one(), key("b"), value("B"));
    writes.stage_put(space_two(), key("a"), value("space-two"));
    writes.stage_delete(space_one(), key("missing"));

    let (_commit, stats) = storage
        .commit_write_set(writes, WriteOptions::default())
        .map_err(|error| format!("commit_write_set failed: {error}"))?;

    assert_eq!(stats.staged_puts, 3);
    assert_eq!(stats.staged_deletes, 1);
    assert_eq!(stats.touched_spaces, 2);
    assert_eq!(stats.put_batches, 2);
    assert_eq!(stats.delete_batches, 1);

    let read = storage
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let slots = read
        .get_many_caller_order(space_one(), &[key("a"), key("b")], GetOptions::default())
        .map_err(|error| format!("get_many_caller_order failed: {error}"))?;

    assert_eq!(
        slots.into_iter().map(|slot| slot.value).collect::<Vec<_>>(),
        vec![
            Some(ProjectedValue::FullValue(Bytes::from_static(b"A"))),
            Some(ProjectedValue::FullValue(Bytes::from_static(b"B"))),
        ]
    );

    Ok(())
}

fn point_reads_preserve_caller_order_duplicates_and_missing() -> StorageConformanceResult {
    let storage = StorageContext::new(ConformanceBackend::new());
    let mut writes = storage.new_write_set();
    writes.stage_put(space_one(), key("a"), value("A"));
    writes.stage_put(space_one(), key("b"), value("B"));
    storage
        .commit_write_set(writes, WriteOptions::default())
        .map_err(|error| format!("seed failed: {error}"))?;

    let read = storage
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let slots = read
        .get_many_caller_order(
            space_one(),
            &[key("b"), key("missing"), key("a"), key("b")],
            GetOptions {
                projection: CoreProjection::KeyOnly,
                ..GetOptions::default()
            },
        )
        .map_err(|error| format!("get_many_caller_order failed: {error}"))?;

    assert_eq!(
        slots
            .iter()
            .map(|slot| (&slot.key, &slot.value))
            .collect::<Vec<_>>(),
        vec![
            (&key("b"), &Some(ProjectedValue::KeyOnly)),
            (&key("missing"), &None),
            (&key("a"), &Some(ProjectedValue::KeyOnly)),
            (&key("b"), &Some(ProjectedValue::KeyOnly)),
        ]
    );

    Ok(())
}

fn prefix_scan_lowers_to_backend_range() -> StorageConformanceResult {
    let storage = StorageContext::new(ConformanceBackend::new());
    let mut writes = storage.new_write_set();
    writes.stage_put(space_one(), key("aa"), value("AA"));
    writes.stage_put(space_one(), key("ab"), value("AB"));
    writes.stage_put(space_one(), key("b"), value("B"));
    storage
        .commit_write_set(writes, WriteOptions::default())
        .map_err(|error| format!("seed failed: {error}"))?;

    let read = storage
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let chunk = read
        .scan_prefix(
            space_one(),
            Prefix {
                bytes: Bytes::from_static(b"a"),
            },
            ScanOptions::default(),
        )
        .map_err(|error| format!("scan_prefix failed: {error}"))?;

    assert_eq!(
        chunk
            .entries
            .entries
            .into_iter()
            .map(|entry| entry.key)
            .collect::<Vec<_>>(),
        vec![key("aa"), key("ab")]
    );

    Ok(())
}

fn read_scope_pins_snapshot() -> StorageConformanceResult {
    let storage = StorageContext::new(ConformanceBackend::new());
    let mut seed = storage.new_write_set();
    seed.stage_put(space_one(), key("a"), value("A"));
    storage
        .commit_write_set(seed, WriteOptions::default())
        .map_err(|error| format!("seed failed: {error}"))?;

    let read = storage
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;

    let mut later = storage.new_write_set();
    later.stage_put(space_one(), key("a"), value("B"));
    storage
        .commit_write_set(later, WriteOptions::default())
        .map_err(|error| format!("later commit failed: {error}"))?;

    let chunk = read
        .scan_range(
            space_one(),
            KeyRange {
                lower: Bound::Included(key("a")),
                upper: Bound::Included(key("a")),
            },
            ScanOptions::default(),
        )
        .map_err(|error| format!("scan_range failed: {error}"))?;

    assert_eq!(
        chunk
            .entries
            .entries
            .into_iter()
            .map(|entry| entry.value)
            .collect::<Vec<_>>(),
        vec![ProjectedValue::FullValue(Bytes::from_static(b"A"))]
    );

    Ok(())
}

fn write_set_rejects_conflicting_space_declarations() -> StorageConformanceResult {
    let storage = StorageContext::new(ConformanceBackend::new());
    let mut writes = storage.new_write_set();
    writes.stage_put(space_one(), key("a"), value("A"));
    writes.stage_put(
        StorageSpace::new(SpaceId(1), "storage.conformance.renamed"),
        key("b"),
        value("B"),
    );

    match storage.commit_write_set(writes, WriteOptions::default()) {
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

fn value(bytes: &'static str) -> StoredValue {
    StoredValue {
        bytes: Bytes::from_static(bytes.as_bytes()),
    }
}

#[cfg(test)]
mod tests {
    use super::{run_storage_conformance, StorageConformanceStatus};

    #[test]
    fn conformance_backend_passes_storage_v2_conformance() {
        let report = run_storage_conformance();

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
                "prefix_scan_lowers_to_backend_range",
                "read_scope_pins_snapshot",
                "write_set_rejects_conflicting_space_declarations",
            ]
        );
    }
}
