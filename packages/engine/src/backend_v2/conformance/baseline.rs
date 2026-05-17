use std::collections::BTreeMap;
use std::ops::Bound;

use bytes::Bytes;

use crate::backend_v2::conformance::{
    fixtures::{full_put, key, put_batch, space},
    open_backend, BackendFactory, ConformanceReport, ConformanceResult,
};
use crate::backend_v2::{
    get_many as backend_get_many, Backend, BackendRead, BackendWrite, CoreProjection, GetOptions,
    Key, KeyRange, KeyRef, ProjectedValue, ProjectedValueRef, ReadBatch, ReadEntry, ReadOptions,
    ScanOptions, ScanPage, SpaceId, WriteOptions,
};

pub(crate) fn register<F>(report: &mut ConformanceReport, factory: &F)
where
    F: BackendFactory,
{
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
        "baseline::scan_range_returns_forward_row_bounded_pages",
        || scan_range_returns_forward_row_bounded_pages(factory),
    );
    report.run("baseline::scan_range_honors_bound_variants", || {
        scan_range_honors_bound_variants(factory)
    });
    report.run("baseline::scan_range_orders_raw_byte_keys", || {
        scan_range_orders_raw_byte_keys(factory)
    });
    report.run("baseline::scan_range_drains_multi_page_limits", || {
        scan_range_drains_multi_page_limits(factory)
    });
    report.run(
        "baseline::scan_range_empty_range_returns_empty_page",
        || scan_range_empty_range_returns_empty_page(factory),
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
    let result = backend_get_many(&read, &requested, GetOptions::default())
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
    let result = backend_get_many(&read, &[], GetOptions::default())
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
        .delete_many(&[key("missing")])
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
        .delete_many(&[key("a")])
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
        .delete_range(KeyRange {
            lower: Bound::Included(key("b")),
            upper: Bound::Excluded(key("d")),
        })
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
        .put_many(put_batch([
            full_put(key("b"), "B"),
            full_put(key("c"), "C2"),
        ]))
        .map_err(|error| format!("put_many failed: {error}"))?;
    write
        .delete_range(KeyRange {
            lower: Bound::Included(key("b")),
            upper: Bound::Excluded(key("d")),
        })
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
        .delete_range(KeyRange {
            lower: Bound::Included(key("b")),
            upper: Bound::Excluded(key("d")),
        })
        .map_err(|error| format!("delete_range failed: {error}"))?;
    write
        .put_many(put_batch([full_put(key("c"), "C")]))
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
        .put_many(put_batch([full_put(key("a"), "B")]))
        .map_err(|error| format!("put_many overwrite failed: {error}"))?;
    write
        .commit()
        .map_err(|error| format!("commit failed: {error}"))?;

    assert_get_entries(&backend, test_space, &[("a", Some("B"))])
}

fn scan_range_returns_forward_row_bounded_pages<F>(factory: &F) -> ConformanceResult
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
    assert_read_entries(&first.entries.entries, &[("b", "B"), ("c", "C")])?;
    if !first.has_more {
        return Err("first scan page did not report has_more".to_string());
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
    assert_read_entries(&second.entries.entries, &[("d", "D")])?;
    if second.has_more {
        return Err("last scan page unexpectedly reported has_more".to_string());
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
    assert_read_entries(&included.entries.entries, &[("b", "B"), ("c", "C")])?;

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
    assert_read_entries(&excluded.entries.entries, &[("c", "C")])
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
    let page = scan_range(
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
        &page.entries.entries,
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

fn scan_range_drains_multi_page_limits<F>(factory: &F) -> ConformanceResult
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
            let page = scan_range(
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
            actual.extend(entries_to_key_values(&page.entries.entries));
            resume_after = page.entries.entries.last().map(|entry| entry.key.clone());
            if !page.has_more {
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

fn scan_range_empty_range_returns_empty_page<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = open_backend(factory);
    let test_space = space(1);
    seed_full_values(&backend, test_space, [("a", "A"), ("b", "B")])?;
    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let page = scan_range(
        &read,
        test_space,
        KeyRange {
            lower: Bound::Included(key("b")),
            upper: Bound::Excluded(key("b")),
        },
        ScanOptions::default(),
    )
    .map_err(|error| format!("scan_range failed: {error}"))?;
    if page.entries.entries.is_empty() {
        Ok(())
    } else {
        Err(format!("empty range returned entries: {:?}", page.entries))
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
        .put_many(put_batch([
            full_put(key_a.clone(), "A"),
            full_put(key_b.clone(), "B"),
        ]))
        .map_err(|error| format!("put_many failed: {error}"))?;

    let read_before_commit = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read before commit failed: {error}"))?;
    let before_commit = backend_get_many(
        &read_before_commit,
        &[key_a.clone(), key_b.clone()],
        GetOptions::default(),
    )
    .map_err(|error| format!("get_many before commit failed: {error}"))?;
    if !before_commit
        .entries_for_requested_keys(&[key_a.clone(), key_b.clone()])
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
        .put_many(put_batch([full_put(key("a"), "A")]))
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
        .put_many(put_batch([full_put(key("a"), "A2")]))
        .map_err(|error| format!("put_many overwrite failed: {error}"))?;
    write
        .delete_many(&[key("b")])
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
    let old_result = backend_get_many(&old_read, &old_keys, GetOptions::default())
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
    assert_read_entries(&old_scan.entries.entries, &[("a", "A")])?;

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
    assert_key_only_entries(&key_only_scan.entries.entries, &[key("a")])
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
    let result = backend_get_many(&read, &requested, GetOptions::default())
        .map_err(|error| format!("opaque get_many failed: {error}"))?;
    assert_read_entries_bytes(
        &result.entries_for_requested_keys(&requested),
        &[(opaque_key.0, opaque_value)],
    )
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
        .put_many(put_batch(rows.into_iter().map(
            |(key_bytes, value_bytes)| full_put(key(key_bytes), value_bytes),
        )))
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
        .put_many(put_batch(rows.into_iter().map(
            |(key_bytes, value_bytes)| full_put(key(key_bytes), value_bytes),
        )))
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
) -> Result<ScanPage, crate::backend_v2::BackendError>
where
    R: BackendRead,
{
    let mut entries = Vec::with_capacity(opts.limit_rows);
    let result = read.visit_range(
        range,
        opts,
        &mut |key: KeyRef<'_>, value: ProjectedValueRef<'_>| {
            entries.push(ReadEntry {
                key: key.to_owned_key(),
                value: value.to_owned(),
            });
            Ok(())
        },
    )?;
    Ok(ScanPage {
        entries: ReadBatch { entries },
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
    let result = backend_get_many(&read, &keys, GetOptions::default())
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
