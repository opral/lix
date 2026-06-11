use std::collections::BTreeMap;
use std::ops::Bound;

/// Single space used by most baseline fixtures; the cross-space tests at
/// the bottom of this file pin space isolation.
const TEST_SPACE: SpaceId = SpaceId(7);
const OTHER_SPACE: SpaceId = SpaceId(8);

use bytes::Bytes;

use crate::backend::conformance::{
    BackendFactory, ConformanceReport, ConformanceResult,
    fixtures::{full_put, key, put_batch, space},
    open_backend,
};
use crate::backend::{
    Backend, BackendError, BackendRead, BackendWrite, CoreProjection, GetOptions, Key, KeyRange,
    KeyRef, ProjectedValue, ProjectedValueRef, ReadEntry, ReadOptions, ScanChunk, ScanOptions,
    SpaceId, WriteOptions, get_many as backend_get_many,
};

pub(crate) fn register<F>(report: &mut ConformanceReport, factory: &F)
where
    F: BackendFactory,
{
    report.run("baseline::spaces_do_not_collide", || {
        spaces_do_not_collide(factory)
    });
    report.run("baseline::scan_is_space_scoped", || {
        scan_is_space_scoped(factory)
    });
    report.run(
        "baseline::unbounded_delete_range_truncates_only_target_space",
        || unbounded_delete_range_truncates_only_target_space(factory),
    );
    report.run("baseline::empty_space_reads_are_empty", || {
        empty_space_reads_are_empty(factory)
    });
    report.run("baseline::get_many_returns_requested_slots", || {
        get_many_returns_requested_slots(factory)
    });
    report.run("baseline::get_many_empty_key_list", || {
        get_many_empty_key_list(factory)
    });
    report.run("baseline::delete_many_missing_keys_is_idempotent", || {
        delete_many_missing_keys_is_idempotent(factory)
    });
    report.run("baseline::delete_many_removes_existing_keys", || {
        delete_many_removes_existing_keys(factory)
    });
    report.run("baseline::delete_range_removes_exact_range", || {
        delete_range_removes_exact_range(factory)
    });
    report.run("baseline::delete_range_applies_after_staged_puts", || {
        delete_range_applies_after_staged_puts(factory)
    });
    report.run("baseline::put_many_applies_after_delete_range", || {
        put_many_applies_after_delete_range(factory)
    });
    report.run("baseline::put_many_overwrites_existing_value", || {
        put_many_overwrites_existing_value(factory)
    });
    report.run(
        "baseline::scan_range_sees_overwritten_existing_value",
        || scan_range_sees_overwritten_existing_value(factory),
    );
    report.run(
        "baseline::scan_range_returns_forward_row_bounded_chunks",
        || scan_range_returns_forward_row_bounded_chunks(factory),
    );
    report.run("baseline::scan_range_honors_bound_variants", || {
        scan_range_honors_bound_variants(factory)
    });
    report.run(
        "baseline::scan_range_resume_before_lower_does_not_widen_range",
        || scan_range_resume_before_lower_does_not_widen_range(factory),
    );
    report.run("baseline::scan_range_orders_raw_byte_keys", || {
        scan_range_orders_raw_byte_keys(factory)
    });
    report.run("baseline::scan_range_drains_multi_chunk_limits", || {
        scan_range_drains_multi_chunk_limits(factory)
    });
    report.run("baseline::scan_cursor_drains_multi_chunk_limits", || {
        scan_cursor_drains_multi_chunk_limits(factory)
    });
    report.run(
        "baseline::scan_range_empty_range_returns_empty_chunk",
        || scan_range_empty_range_returns_empty_chunk(factory),
    );
    report.run("baseline::commit_is_atomic", || commit_is_atomic(factory));
    report.run("baseline::rollback_discards_staged_mutations", || {
        rollback_discards_staged_mutations(factory)
    });
    report.run("baseline::rollback_discards_overwrite_and_delete", || {
        rollback_discards_overwrite_and_delete(factory)
    });
    report.run("baseline::begin_read_pins_coherent_view", || {
        begin_read_pins_coherent_view(factory)
    });
    report.run("baseline::full_value_and_key_only_are_core", || {
        full_value_and_key_only_are_core(factory)
    });
    report.run("baseline::full_value_preserves_opaque_bytes", || {
        full_value_preserves_opaque_bytes(factory)
    });
}

fn get_many_returns_requested_slots<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = open_backend(factory);
    let test_space = space(1);
    seed_full_values(&backend, test_space, [("a", "A"), ("b", "B")])?;

    let requested = [key("b"), key("missing"), key("a"), key("b")];
    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let result = backend_get_many(&read, TEST_SPACE, &requested, GetOptions::default())
        .map_err(|error| format!("get_many failed: {error}"))?;

    if result.values.len() != requested.len() {
        return Err(format!(
            "get_many returned {} slots for {} requested keys",
            result.values.len(),
            requested.len()
        ));
    }
    let expected_values = vec![
        Some(ProjectedValue::FullValue(Bytes::from_static(b"B"))),
        None,
        Some(ProjectedValue::FullValue(Bytes::from_static(b"A"))),
        Some(ProjectedValue::FullValue(Bytes::from_static(b"B"))),
    ];
    if result.values != expected_values {
        return Err(format!(
            "get_many slot mismatch: expected {:?}, got {:?}",
            expected_values, result.values
        ));
    }

    let entries = result.entries_for_requested_keys(&requested);
    assert_entry_map(
        &entries,
        &[
            (key("a"), Bytes::from_static(b"A")),
            (key("b"), Bytes::from_static(b"B")),
        ],
    )
}

fn get_many_empty_key_list<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = open_backend(factory);
    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let result = backend_get_many(&read, TEST_SPACE, &[], GetOptions::default())
        .map_err(|error| format!("get_many failed: {error}"))?;
    if result.entries_for_requested_keys(&[]).is_empty() {
        Ok(())
    } else {
        Err(format!(
            "empty get_many returned values: {:?}",
            result.values
        ))
    }
}

fn delete_many_missing_keys_is_idempotent<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = open_backend(factory);
    let test_space = space(1);
    seed_full_values(&backend, test_space, [("a", "A")])?;

    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .delete_many(TEST_SPACE, &[key("missing")])
        .map_err(|error| format!("delete_many missing failed: {error}"))?;
    write
        .commit()
        .map_err(|error| format!("commit failed: {error}"))?;

    assert_get_entries(&backend, test_space, &[("a", Some("A"))])
}

fn delete_many_removes_existing_keys<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = open_backend(factory);
    let test_space = space(1);
    seed_full_values(&backend, test_space, [("a", "A"), ("b", "B")])?;

    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .delete_many(TEST_SPACE, &[key("a")])
        .map_err(|error| format!("delete_many existing failed: {error}"))?;
    write
        .commit()
        .map_err(|error| format!("commit failed: {error}"))?;

    assert_get_entries(&backend, test_space, &[("a", None), ("b", Some("B"))])
}

fn delete_range_removes_exact_range<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = open_backend(factory);
    let test_space = space(1);
    seed_full_values(
        &backend,
        test_space,
        [("a", "A"), ("b", "B"), ("c", "C"), ("d", "D"), ("e", "E")],
    )?;

    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .delete_range(
            TEST_SPACE,
            KeyRange {
                lower: Bound::Included(key("b")),
                upper: Bound::Excluded(key("d")),
            },
        )
        .map_err(|error| format!("delete_range failed: {error}"))?;
    write
        .commit()
        .map_err(|error| format!("commit failed: {error}"))?;

    assert_get_entries(
        &backend,
        test_space,
        &[
            ("a", Some("A")),
            ("b", None),
            ("c", None),
            ("d", Some("D")),
            ("e", Some("E")),
        ],
    )
}

fn delete_range_applies_after_staged_puts<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = open_backend(factory);
    let test_space = space(1);
    seed_full_values(&backend, test_space, [("a", "A"), ("c", "C"), ("d", "D")])?;

    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .put_many(
            TEST_SPACE,
            put_batch([full_put(key("b"), "B"), full_put(key("c"), "C2")]),
        )
        .map_err(|error| format!("put_many failed: {error}"))?;
    write
        .delete_range(
            TEST_SPACE,
            KeyRange {
                lower: Bound::Included(key("b")),
                upper: Bound::Excluded(key("d")),
            },
        )
        .map_err(|error| format!("delete_range failed: {error}"))?;
    write
        .commit()
        .map_err(|error| format!("commit failed: {error}"))?;

    assert_get_entries(
        &backend,
        test_space,
        &[("a", Some("A")), ("b", None), ("c", None), ("d", Some("D"))],
    )
}

fn put_many_applies_after_delete_range<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = open_backend(factory);
    let test_space = space(1);
    seed_full_values(&backend, test_space, [("a", "A"), ("b", "B"), ("d", "D")])?;

    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .delete_range(
            TEST_SPACE,
            KeyRange {
                lower: Bound::Included(key("b")),
                upper: Bound::Excluded(key("d")),
            },
        )
        .map_err(|error| format!("delete_range failed: {error}"))?;
    write
        .put_many(TEST_SPACE, put_batch([full_put(key("c"), "C")]))
        .map_err(|error| format!("put_many failed: {error}"))?;
    write
        .commit()
        .map_err(|error| format!("commit failed: {error}"))?;

    assert_get_entries(
        &backend,
        test_space,
        &[
            ("a", Some("A")),
            ("b", None),
            ("c", Some("C")),
            ("d", Some("D")),
        ],
    )
}

fn put_many_overwrites_existing_value<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = open_backend(factory);
    let test_space = space(1);
    seed_full_values(&backend, test_space, [("a", "A")])?;

    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .put_many(TEST_SPACE, put_batch([full_put(key("a"), "B")]))
        .map_err(|error| format!("put_many overwrite failed: {error}"))?;
    write
        .commit()
        .map_err(|error| format!("commit failed: {error}"))?;

    assert_get_entries(&backend, test_space, &[("a", Some("B"))])
}

fn scan_range_sees_overwritten_existing_value<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = open_backend(factory);
    let test_space = space(1);
    seed_full_values(&backend, test_space, [("a", "A")])?;

    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .put_many(TEST_SPACE, put_batch([full_put(key("a"), "B")]))
        .map_err(|error| format!("put_many overwrite failed: {error}"))?;
    write
        .commit()
        .map_err(|error| format!("commit failed: {error}"))?;

    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let chunk = scan_range(
        &read,
        test_space,
        KeyRange {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        },
        ScanOptions::default(),
    )
    .map_err(|error| format!("scan_range failed: {error}"))?;

    assert_read_entries(&chunk.entries, &[("a", "B")])
}

fn scan_range_returns_forward_row_bounded_chunks<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = open_backend(factory);
    let test_space = space(1);
    seed_full_values(
        &backend,
        test_space,
        [("a", "A"), ("b", "B"), ("c", "C"), ("d", "D"), ("e", "E")],
    )?;
    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let range = KeyRange {
        lower: Bound::Included(key("b")),
        upper: Bound::Excluded(key("e")),
    };

    let first = scan_range(
        &read,
        test_space,
        range.clone(),
        ScanOptions {
            limit_rows: 2,
            ..Default::default()
        },
    )
    .map_err(|error| format!("first scan_range failed: {error}"))?;
    assert_read_entries(&first.entries, &[("b", "B"), ("c", "C")])?;
    if !first.has_more {
        return Err("first scan chunk did not report has_more".to_string());
    }

    let second = scan_range(
        &read,
        test_space,
        range,
        ScanOptions {
            limit_rows: 2,
            resume_after: Some(&key("c")),
            ..Default::default()
        },
    )
    .map_err(|error| format!("second scan_range failed: {error}"))?;
    assert_read_entries(&second.entries, &[("d", "D")])?;
    if second.has_more {
        return Err("last scan chunk unexpectedly reported has_more".to_string());
    }
    Ok(())
}

fn scan_range_honors_bound_variants<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = open_backend(factory);
    let test_space = space(1);
    seed_full_values(
        &backend,
        test_space,
        [("a", "A"), ("b", "B"), ("c", "C"), ("d", "D")],
    )?;
    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;

    let included = scan_range(
        &read,
        test_space,
        KeyRange {
            lower: Bound::Included(key("b")),
            upper: Bound::Included(key("c")),
        },
        ScanOptions::default(),
    )
    .map_err(|error| format!("included range scan failed: {error}"))?;
    assert_read_entries(&included.entries, &[("b", "B"), ("c", "C")])?;

    let excluded = scan_range(
        &read,
        test_space,
        KeyRange {
            lower: Bound::Excluded(key("b")),
            upper: Bound::Excluded(key("d")),
        },
        ScanOptions::default(),
    )
    .map_err(|error| format!("excluded range scan failed: {error}"))?;
    assert_read_entries(&excluded.entries, &[("c", "C")])
}

fn scan_range_resume_before_lower_does_not_widen_range<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = open_backend(factory);
    let test_space = space(1);
    seed_full_values(
        &backend,
        test_space,
        [("a", "A"), ("b", "B"), ("c", "C"), ("d", "D")],
    )?;
    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let chunk = scan_range(
        &read,
        test_space,
        KeyRange {
            lower: Bound::Included(key("c")),
            upper: Bound::Excluded(key("e")),
        },
        ScanOptions {
            resume_after: Some(&key("a")),
            ..Default::default()
        },
    )
    .map_err(|error| format!("scan_range failed: {error}"))?;

    assert_read_entries(&chunk.entries, &[("c", "C"), ("d", "D")])
}

fn scan_range_orders_raw_byte_keys<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = open_backend(factory);
    let test_space = space(1);
    seed_full_byte_values(
        &backend,
        test_space,
        [
            (
                Bytes::from_static(&[0xff, 0x00]),
                Bytes::from_static(b"ff00"),
            ),
            (Bytes::from_static(&[0x80]), Bytes::from_static(b"80")),
            (
                Bytes::from_static(&[0x00, 0xff]),
                Bytes::from_static(b"00ff"),
            ),
            (Bytes::new(), Bytes::from_static(b"empty")),
            (Bytes::from_static(&[0x00]), Bytes::from_static(b"00")),
            (Bytes::from_static(&[0xff]), Bytes::from_static(b"ff")),
            (Bytes::from_static(&[0x7f]), Bytes::from_static(b"7f")),
            (Bytes::from_static(&[0x01]), Bytes::from_static(b"01")),
            (
                Bytes::from_static(&[0x00, 0x00]),
                Bytes::from_static(b"0000"),
            ),
        ],
    )?;

    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let chunk = scan_range(
        &read,
        test_space,
        KeyRange {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        },
        ScanOptions::default(),
    )
    .map_err(|error| format!("scan_range failed: {error}"))?;

    assert_read_entries_bytes(
        &chunk.entries,
        &[
            (Bytes::new(), Bytes::from_static(b"empty")),
            (Bytes::from_static(&[0x00]), Bytes::from_static(b"00")),
            (
                Bytes::from_static(&[0x00, 0x00]),
                Bytes::from_static(b"0000"),
            ),
            (
                Bytes::from_static(&[0x00, 0xff]),
                Bytes::from_static(b"00ff"),
            ),
            (Bytes::from_static(&[0x01]), Bytes::from_static(b"01")),
            (Bytes::from_static(&[0x7f]), Bytes::from_static(b"7f")),
            (Bytes::from_static(&[0x80]), Bytes::from_static(b"80")),
            (Bytes::from_static(&[0xff]), Bytes::from_static(b"ff")),
            (
                Bytes::from_static(&[0xff, 0x00]),
                Bytes::from_static(b"ff00"),
            ),
        ],
    )
}

fn scan_range_drains_multi_chunk_limits<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = open_backend(factory);
    let test_space = space(1);
    seed_full_values(
        &backend,
        test_space,
        [
            ("a", "A"),
            ("b", "B"),
            ("c", "C"),
            ("d", "D"),
            ("e", "E"),
            ("f", "F"),
            ("g", "G"),
            ("h", "H"),
        ],
    )?;
    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let range = KeyRange {
        lower: Bound::Included(key("b")),
        upper: Bound::Excluded(key("h")),
    };
    let expected = vec![
        (key("b"), Bytes::from_static(b"B")),
        (key("c"), Bytes::from_static(b"C")),
        (key("d"), Bytes::from_static(b"D")),
        (key("e"), Bytes::from_static(b"E")),
        (key("f"), Bytes::from_static(b"F")),
        (key("g"), Bytes::from_static(b"G")),
    ];

    for limit in [1usize, 2, 3] {
        let mut resume_after = None;
        let mut actual = Vec::new();
        loop {
            let chunk = scan_range(
                &read,
                test_space,
                range.clone(),
                ScanOptions {
                    limit_rows: limit,
                    resume_after: resume_after.as_ref(),
                    ..Default::default()
                },
            )
            .map_err(|error| format!("scan_range limit {limit} failed: {error}"))?;
            actual.extend(entries_to_key_values(&chunk.entries));
            resume_after = chunk.entries.last().map(|entry| entry.key.clone());
            if !chunk.has_more {
                break;
            }
            if actual.len() > expected.len() {
                return Err(format!("limit {limit} emitted too many rows: {actual:?}"));
            }
        }
        if actual != expected {
            return Err(format!(
                "drain mismatch for limit {limit}: expected {expected:?}, got {actual:?}"
            ));
        }
    }
    Ok(())
}

fn scan_cursor_drains_multi_chunk_limits<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = open_backend(factory);
    let test_space = space(1);
    seed_full_values(
        &backend,
        test_space,
        [
            ("a", "A"),
            ("b", "B"),
            ("c", "C"),
            ("d", "D"),
            ("e", "E"),
            ("f", "F"),
            ("g", "G"),
            ("h", "H"),
        ],
    )?;
    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let range = KeyRange {
        lower: Bound::Included(key("b")),
        upper: Bound::Excluded(key("h")),
    };
    let expected = vec![
        (key("b"), Bytes::from_static(b"B")),
        (key("c"), Bytes::from_static(b"C")),
        (key("d"), Bytes::from_static(b"D")),
        (key("e"), Bytes::from_static(b"E")),
        (key("f"), Bytes::from_static(b"F")),
        (key("g"), Bytes::from_static(b"G")),
    ];

    for limit in [1usize, 2, 3] {
        let mut actual = Vec::new();
        loop {
            let mut entries = Vec::new();
            let resume_after = actual.last().map(|(key, _): &(Key, Bytes)| key.clone());
            let result = read
                .scan(
                    TEST_SPACE,
                    range.clone(),
                    ScanOptions {
                        limit_rows: limit,
                        resume_after: resume_after.as_ref(),
                        ..Default::default()
                    },
                    &mut |key: KeyRef<'_>, value: ProjectedValueRef<'_>| {
                        entries.push(ReadEntry {
                            key: key.to_owned_key(),
                            value: value.to_owned(),
                        });
                        Ok(())
                    },
                )
                .map_err(|error| format!("paged scan limit {limit} failed: {error}"))?;
            actual.extend(entries_to_key_values(&entries));
            if !result.has_more {
                break;
            }
            if actual.len() > expected.len() {
                return Err(format!(
                    "paged scan limit {limit} emitted too many rows: {actual:?}"
                ));
            }
        }
        if actual != expected {
            return Err(format!(
                "cursor drain mismatch for limit {limit}: expected {expected:?}, got {actual:?}"
            ));
        }
    }
    Ok(())
}

fn scan_range_empty_range_returns_empty_chunk<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = open_backend(factory);
    let test_space = space(1);
    seed_full_values(&backend, test_space, [("a", "A"), ("b", "B")])?;
    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let chunk = scan_range(
        &read,
        test_space,
        KeyRange {
            lower: Bound::Included(key("b")),
            upper: Bound::Excluded(key("b")),
        },
        ScanOptions::default(),
    )
    .map_err(|error| format!("scan_range failed: {error}"))?;
    if chunk.entries.is_empty() {
        Ok(())
    } else {
        Err(format!("empty range returned entries: {:?}", chunk.entries))
    }
}

fn commit_is_atomic<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = open_backend(factory);
    let test_space = space(1);
    let key_a = key("a");
    let key_b = key("b");

    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .put_many(
            TEST_SPACE,
            put_batch([full_put(key_a.clone(), "A"), full_put(key_b.clone(), "B")]),
        )
        .map_err(|error| format!("put_many failed: {error}"))?;

    let read_before_commit = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read before commit failed: {error}"))?;
    let before_commit = backend_get_many(
        &read_before_commit,
        TEST_SPACE,
        &[key_a.clone(), key_b.clone()],
        GetOptions::default(),
    )
    .map_err(|error| format!("get_many before commit failed: {error}"))?;
    if !before_commit
        .entries_for_requested_keys(&[key_a, key_b])
        .is_empty()
    {
        return Err("uncommitted writes were visible to an independent read".to_string());
    }

    write
        .commit()
        .map_err(|error| format!("commit failed: {error}"))?;
    assert_get_entries(&backend, test_space, &[("a", Some("A")), ("b", Some("B"))])
}

fn rollback_discards_staged_mutations<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = open_backend(factory);
    let test_space = space(1);

    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .put_many(TEST_SPACE, put_batch([full_put(key("a"), "A")]))
        .map_err(|error| format!("put_many failed: {error}"))?;
    write
        .rollback()
        .map_err(|error| format!("rollback failed: {error}"))?;

    assert_get_entries(&backend, test_space, &[("a", None)])
}

fn rollback_discards_overwrite_and_delete<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = open_backend(factory);
    let test_space = space(1);
    seed_full_values(&backend, test_space, [("a", "A"), ("b", "B")])?;

    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .put_many(TEST_SPACE, put_batch([full_put(key("a"), "A2")]))
        .map_err(|error| format!("put_many overwrite failed: {error}"))?;
    write
        .delete_many(TEST_SPACE, &[key("b")])
        .map_err(|error| format!("delete_many failed: {error}"))?;
    write
        .rollback()
        .map_err(|error| format!("rollback failed: {error}"))?;

    assert_get_entries(&backend, test_space, &[("a", Some("A")), ("b", Some("B"))])
}

fn begin_read_pins_coherent_view<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = open_backend(factory);
    let test_space = space(1);
    seed_full_values(&backend, test_space, [("a", "A")])?;
    let old_read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;

    seed_full_values(&backend, test_space, [("a", "B")])?;
    seed_full_values(&backend, test_space, [("a", "C")])?;

    let old_keys = [key("a")];
    let old_result = backend_get_many(&old_read, TEST_SPACE, &old_keys, GetOptions::default())
        .map_err(|error| format!("old read get_many failed: {error}"))?;
    assert_read_entries(
        &old_result.entries_for_requested_keys(&old_keys),
        &[("a", "A")],
    )?;

    let old_scan = scan_range(
        &old_read,
        test_space,
        KeyRange {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        },
        ScanOptions::default(),
    )
    .map_err(|error| format!("old read scan_range failed: {error}"))?;
    assert_read_entries(&old_scan.entries, &[("a", "A")])?;

    assert_get_entries(&backend, test_space, &[("a", Some("C"))])
}

fn full_value_and_key_only_are_core<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = open_backend(factory);
    let test_space = space(1);
    seed_full_values(&backend, test_space, [("a", "A")])?;
    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;

    let full_keys = [key("a")];
    let full = backend_get_many(
        &read,
        TEST_SPACE,
        &full_keys,
        GetOptions {
            projection: CoreProjection::FullValue,
            ..Default::default()
        },
    )
    .map_err(|error| format!("FullValue get_many failed: {error}"))?;
    assert_read_entries(&full.entries_for_requested_keys(&full_keys), &[("a", "A")])?;

    let key_only_keys = [key("a")];
    let key_only = backend_get_many(
        &read,
        TEST_SPACE,
        &key_only_keys,
        GetOptions {
            projection: CoreProjection::KeyOnly,
            ..Default::default()
        },
    )
    .map_err(|error| format!("KeyOnly get_many failed: {error}"))?;
    assert_key_only_entries(
        &key_only.entries_for_requested_keys(&key_only_keys),
        &[key("a")],
    )?;

    let key_only_scan = scan_range(
        &read,
        test_space,
        KeyRange {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        },
        ScanOptions {
            projection: CoreProjection::KeyOnly,
            ..Default::default()
        },
    )
    .map_err(|error| format!("KeyOnly scan_range failed: {error}"))?;
    assert_key_only_entries(&key_only_scan.entries, &[key("a")])
}

fn assert_key_only_entries(entries: &[ReadEntry], expected_keys: &[Key]) -> ConformanceResult {
    let actual = entries
        .iter()
        .map(|entry| {
            if !matches!(entry.value, ProjectedValue::KeyOnly) {
                return Err(format!(
                    "expected KeyOnly projected value for {:?}, got {:?}",
                    entry.key, entry.value
                ));
            }
            Ok(entry.key.clone())
        })
        .collect::<Result<Vec<_>, _>>()?;

    if actual == expected_keys {
        Ok(())
    } else {
        Err(format!(
            "KeyOnly key mismatch: expected {expected_keys:?}, got {actual:?}"
        ))
    }
}

fn full_value_preserves_opaque_bytes<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = open_backend(factory);
    let test_space = space(1);
    let opaque_key = Key(Bytes::from_static(b"\0opaque\xff"));
    let opaque_value = Bytes::from_static(b"\0value\xff\x80\n");
    seed_full_byte_values(
        &backend,
        test_space,
        [(opaque_key.0.clone(), opaque_value.clone())],
    )?;
    let requested = [opaque_key.clone()];
    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let result = backend_get_many(&read, TEST_SPACE, &requested, GetOptions::default())
        .map_err(|error| format!("opaque get_many failed: {error}"))?;
    assert_read_entries_bytes(
        &result.entries_for_requested_keys(&requested),
        &[(opaque_key.0, opaque_value)],
    )
}

/// Spaces are physically independent: the same logical key in two spaces
/// must hold independent values, and deletes must not cross spaces.
fn spaces_do_not_collide<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = open_backend(factory);
    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("begin write failed: {error}"))?;
    write
        .put_many(
            TEST_SPACE,
            put_batch([full_put(key("k"), Bytes::from_static(b"A"))]),
        )
        .map_err(|error| format!("put space A failed: {error}"))?;
    write
        .put_many(
            OTHER_SPACE,
            put_batch([full_put(key("k"), Bytes::from_static(b"B"))]),
        )
        .map_err(|error| format!("put space B failed: {error}"))?;
    write
        .commit()
        .map_err(|error| format!("commit failed: {error}"))?;

    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin read failed: {error}"))?;
    let a = backend_get_many(&read, TEST_SPACE, &[key("k")], GetOptions::default())
        .map_err(|error| format!("get space A failed: {error}"))?;
    let b = backend_get_many(&read, OTHER_SPACE, &[key("k")], GetOptions::default())
        .map_err(|error| format!("get space B failed: {error}"))?;
    if a.values[0].as_ref() != Some(&ProjectedValue::FullValue(Bytes::from_static(b"A")))
        || b.values[0].as_ref() != Some(&ProjectedValue::FullValue(Bytes::from_static(b"B")))
    {
        return Err("same logical key must hold independent values per space".to_string());
    }
    drop(read);

    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("begin delete write failed: {error}"))?;
    write
        .delete_many(TEST_SPACE, &[key("k")])
        .map_err(|error| format!("delete failed: {error}"))?;
    write
        .commit()
        .map_err(|error| format!("commit failed: {error}"))?;
    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin read failed: {error}"))?;
    let a = backend_get_many(&read, TEST_SPACE, &[key("k")], GetOptions::default())
        .map_err(|error| format!("get after delete failed: {error}"))?;
    let b = backend_get_many(&read, OTHER_SPACE, &[key("k")], GetOptions::default())
        .map_err(|error| format!("get other after delete failed: {error}"))?;
    if a.values[0].as_ref().is_some() {
        return Err("delete_many must remove the key in its space".to_string());
    }
    if b.values[0].as_ref().is_none() {
        return Err("delete_many must not cross spaces".to_string());
    }
    Ok(())
}

/// Scans observe only their space, including under resume_after pagination
/// near the end of the space (an off-by-one upper bound leaks the
/// neighbouring space here).
fn scan_is_space_scoped<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = open_backend(factory);
    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("begin write failed: {error}"))?;
    for space in [TEST_SPACE, OTHER_SPACE, SpaceId(9)] {
        write
            .put_many(
                space,
                put_batch([
                    full_put(key("a"), Bytes::from_static(b"1")),
                    full_put(key("b"), Bytes::from_static(b"2")),
                    full_put(key("c"), Bytes::from_static(b"3")),
                ]),
            )
            .map_err(|error| format!("seed failed: {error}"))?;
    }
    write
        .commit()
        .map_err(|error| format!("commit failed: {error}"))?;

    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin read failed: {error}"))?;
    let mut rows = Vec::new();
    let result = read
        .scan(
            OTHER_SPACE,
            full_key_range(),
            ScanOptions::default(),
            &mut |key: KeyRef<'_>, _value: ProjectedValueRef<'_>| {
                rows.push(key.to_owned_key());
                Ok(())
            },
        )
        .map_err(|error| format!("scan failed: {error}"))?;
    if rows != vec![key("a"), key("b"), key("c")] || result.has_more {
        return Err(format!("scan must observe only its space, got {rows:?}"));
    }

    // Resume past the last row: must report exhaustion, never the
    // neighbouring space's rows.
    let mut tail = Vec::new();
    let result = read
        .scan(
            OTHER_SPACE,
            full_key_range(),
            ScanOptions {
                resume_after: Some(&key("c")),
                ..ScanOptions::default()
            },
            &mut |key: KeyRef<'_>, _value: ProjectedValueRef<'_>| {
                tail.push(key.to_owned_key());
                Ok(())
            },
        )
        .map_err(|error| format!("resume scan failed: {error}"))?;
    if !tail.is_empty() || result.has_more {
        return Err(format!(
            "resume past the space's last key must be empty, got {tail:?}"
        ));
    }
    Ok(())
}

/// The truncate idiom: an unbounded delete_range clears exactly its space,
/// and the space accepts writes again afterwards.
fn unbounded_delete_range_truncates_only_target_space<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = open_backend(factory);
    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("begin write failed: {error}"))?;
    for space in [TEST_SPACE, OTHER_SPACE, SpaceId(9)] {
        write
            .put_many(
                space,
                put_batch([
                    full_put(key("a"), Bytes::from_static(b"1")),
                    full_put(key("b"), Bytes::from_static(b"2")),
                ]),
            )
            .map_err(|error| format!("seed failed: {error}"))?;
    }
    write
        .commit()
        .map_err(|error| format!("commit failed: {error}"))?;

    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("begin truncate failed: {error}"))?;
    write
        .delete_range(OTHER_SPACE, full_key_range())
        .map_err(|error| format!("truncate failed: {error}"))?;
    write
        .commit()
        .map_err(|error| format!("commit failed: {error}"))?;

    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin read failed: {error}"))?;
    for (space, expected) in [(TEST_SPACE, 2usize), (OTHER_SPACE, 0), (SpaceId(9), 2)] {
        let mut rows = 0usize;
        read.scan(
            space,
            full_key_range(),
            ScanOptions::default(),
            &mut |_key: KeyRef<'_>, _value: ProjectedValueRef<'_>| {
                rows += 1;
                Ok(())
            },
        )
        .map_err(|error| format!("scan failed: {error}"))?;
        if rows != expected {
            return Err(format!(
                "truncate must clear only its space: space {space:?} held {rows} rows, expected {expected}"
            ));
        }
    }
    drop(read);

    // The truncated space must accept writes again.
    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("begin rewrite failed: {error}"))?;
    write
        .put_many(
            OTHER_SPACE,
            put_batch([full_put(key("z"), Bytes::from_static(b"9"))]),
        )
        .map_err(|error| format!("rewrite failed: {error}"))?;
    write
        .commit()
        .map_err(|error| format!("commit failed: {error}"))?;
    Ok(())
}

/// A never-written space behaves as empty for every read shape.
fn empty_space_reads_are_empty<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = open_backend(factory);
    let empty = SpaceId(0x7777_7777);
    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin read failed: {error}"))?;
    let result = backend_get_many(&read, empty, &[key("a")], GetOptions::default())
        .map_err(|error| format!("get failed: {error}"))?;
    if result.values[0].as_ref().is_some() {
        return Err("never-written space must miss".to_string());
    }
    let mut rows = 0usize;
    let scan = read
        .scan(
            empty,
            full_key_range(),
            ScanOptions::default(),
            &mut |_key: KeyRef<'_>, _value: ProjectedValueRef<'_>| {
                rows += 1;
                Ok(())
            },
        )
        .map_err(|error| format!("scan failed: {error}"))?;
    if rows != 0 || scan.has_more {
        return Err("never-written space must scan empty".to_string());
    }
    drop(read);
    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("begin write failed: {error}"))?;
    write
        .delete_range(empty, full_key_range())
        .map_err(|error| format!("delete_range on empty space failed: {error}"))?;
    write
        .commit()
        .map_err(|error| format!("commit failed: {error}"))?;
    Ok(())
}

fn full_key_range() -> KeyRange {
    KeyRange {
        lower: Bound::Unbounded,
        upper: Bound::Unbounded,
    }
}

fn seed_full_values<B, I>(backend: &B, _test_space: SpaceId, rows: I) -> ConformanceResult
where
    B: Backend,
    I: IntoIterator<Item = (&'static str, &'static str)>,
{
    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("seed begin_write failed: {error}"))?;
    write
        .put_many(
            TEST_SPACE,
            put_batch(
                rows.into_iter()
                    .map(|(key_bytes, value_bytes)| full_put(key(key_bytes), value_bytes)),
            ),
        )
        .map_err(|error| format!("seed put_many failed: {error}"))?;
    write
        .commit()
        .map_err(|error| format!("seed commit failed: {error}"))?;
    Ok(())
}

fn seed_full_byte_values<B, I>(backend: &B, _test_space: SpaceId, rows: I) -> ConformanceResult
where
    B: Backend,
    I: IntoIterator<Item = (Bytes, Bytes)>,
{
    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("seed begin_write failed: {error}"))?;
    write
        .put_many(
            TEST_SPACE,
            put_batch(
                rows.into_iter()
                    .map(|(key_bytes, value_bytes)| full_put(key(key_bytes), value_bytes)),
            ),
        )
        .map_err(|error| format!("seed put_many failed: {error}"))?;
    write
        .commit()
        .map_err(|error| format!("seed commit failed: {error}"))?;
    Ok(())
}

fn scan_range<R>(
    read: &R,
    _test_space: SpaceId,
    range: KeyRange,
    opts: ScanOptions<'_>,
) -> Result<ScanChunk, BackendError>
where
    R: BackendRead,
{
    let mut entries = Vec::with_capacity(opts.limit_rows);
    let result = read.scan(TEST_SPACE, range, opts, &mut |key: KeyRef<'_>,
                                                           value: ProjectedValueRef<
        '_,
    >| {
        entries.push(ReadEntry {
            key: key.to_owned_key(),
            value: value.to_owned(),
        });
        Ok(())
    })?;
    Ok(ScanChunk {
        entries,
        has_more: result.has_more,
    })
}

fn assert_get_entries<B>(
    backend: &B,
    _test_space: SpaceId,
    expected: &[(&str, Option<&str>)],
) -> ConformanceResult
where
    B: Backend,
{
    let keys = expected
        .iter()
        .map(|(key_bytes, _)| key(*key_bytes))
        .collect::<Vec<_>>();
    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let result = backend_get_many(&read, TEST_SPACE, &keys, GetOptions::default())
        .map_err(|error| format!("get_many failed: {error}"))?;
    assert_optional_entry_map(&result.entries_for_requested_keys(&keys), expected)
}

fn assert_optional_entry_map(
    entries: &[ReadEntry],
    expected: &[(&str, Option<&str>)],
) -> ConformanceResult {
    let actual = entries_to_map(entries);
    let expected = expected
        .iter()
        .filter_map(|(key_bytes, value)| {
            value.map(|value| (key(*key_bytes), Bytes::from(value.as_bytes().to_vec())))
        })
        .collect::<BTreeMap<_, _>>();
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "entry map mismatch: expected {expected:?}, got {actual:?}"
        ))
    }
}

fn assert_entry_map(entries: &[ReadEntry], expected: &[(Key, Bytes)]) -> ConformanceResult {
    let actual = entries_to_map(entries);
    let expected = expected.iter().cloned().collect::<BTreeMap<_, _>>();
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "entry map mismatch: expected {expected:?}, got {actual:?}"
        ))
    }
}

fn assert_read_entries(entries: &[ReadEntry], expected: &[(&str, &str)]) -> ConformanceResult {
    let actual = entries_to_key_values(entries);
    let expected = expected
        .iter()
        .map(|(key_bytes, value)| (key(*key_bytes), Bytes::from(value.as_bytes().to_vec())))
        .collect::<Vec<_>>();
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "read entry mismatch: expected {expected:?}, got {actual:?}"
        ))
    }
}

fn assert_read_entries_bytes(
    entries: &[ReadEntry],
    expected: &[(Bytes, Bytes)],
) -> ConformanceResult {
    let actual = entries_to_key_values(entries);
    let expected = expected
        .iter()
        .map(|(key_bytes, value)| (key(key_bytes), value.clone()))
        .collect::<Vec<_>>();
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "read entry mismatch: expected {expected:?}, got {actual:?}"
        ))
    }
}

fn entries_to_map(entries: &[ReadEntry]) -> BTreeMap<Key, Bytes> {
    entries_to_key_values(entries).into_iter().collect()
}

fn entries_to_key_values(entries: &[ReadEntry]) -> Vec<(Key, Bytes)> {
    entries
        .iter()
        .map(|entry| {
            (
                entry.key.clone(),
                projected_value_bytes(entry.value.clone()),
            )
        })
        .collect()
}

fn projected_value_bytes(value: ProjectedValue) -> Bytes {
    match value {
        ProjectedValue::FullValue(bytes) => bytes,
        ProjectedValue::KeyOnly => Bytes::new(),
    }
}
