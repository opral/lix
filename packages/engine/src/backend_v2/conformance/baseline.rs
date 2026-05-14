use bytes::Bytes;
use std::ops::Bound;

use crate::backend_v2::conformance::{
    fixtures::{full_put, key, put_batch, space},
    BackendFactory, ConformanceReport, ConformanceResult,
};
use crate::backend_v2::{
    Backend, BackendError, BackendRead, BackendWrite, GetOptions, Key, KeyRange, LimitSupport,
    OrderSupport, Prefix, ProjectedValue, ReadOptions, ScanOptions, Support, ValueProjection,
    WriteOptions,
};

pub(crate) fn register<F>(report: &mut ConformanceReport, factory: &F)
where
    F: BackendFactory,
{
    report.run(
        "baseline::get_many_preserves_caller_order_duplicates_and_missing",
        || get_many_preserves_caller_order_duplicates_and_missing(factory),
    );
    report.run("baseline::get_many_empty_key_list", || {
        get_many_empty_key_list(factory)
    });
    report.run(
        "baseline::get_many_missing_only_and_duplicate_missing",
        || get_many_missing_only_and_duplicate_missing(factory),
    );
    report.run("baseline::write_reads_its_own_writes", || {
        write_reads_its_own_writes(factory)
    });
    report.run("baseline::delete_many_missing_keys_is_idempotent", || {
        delete_many_missing_keys_is_idempotent(factory)
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
    report.run("baseline::scan_range_limit_zero_returns_empty_page", || {
        scan_range_limit_zero_returns_empty_page(factory)
    });
    report.run(
        "baseline::scan_range_empty_range_returns_empty_page",
        || scan_range_empty_range_returns_empty_page(factory),
    );
    report.run("baseline::scan_prefix_matches_equivalent_range", || {
        scan_prefix_matches_equivalent_range(factory)
    });
    report.run(
        "baseline::scan_prefix_empty_prefix_scans_whole_space",
        || scan_prefix_empty_prefix_scans_whole_space(factory),
    );
    report.run(
        "baseline::scan_prefix_ff_prefix_uses_unbounded_upper_range",
        || scan_prefix_ff_prefix_uses_unbounded_upper_range(factory),
    );
    report.run("baseline::commit_is_atomic", || commit_is_atomic(factory));
    report.run("baseline::rollback_discards_staged_mutations", || {
        rollback_discards_staged_mutations(factory)
    });
    report.run("baseline::begin_read_pins_coherent_view", || {
        begin_read_pins_coherent_view(factory)
    });
    report.run("baseline::spaces_are_isolated", || {
        spaces_are_isolated(factory)
    });
    report.run("baseline::full_value_and_key_only_are_core", || {
        full_value_and_key_only_are_core(factory)
    });
    report.run(
        "baseline::read_support_metadata_is_truthful_for_core_reads",
        || read_support_metadata_is_truthful_for_core_reads(factory),
    );
    report.run("baseline::cursor_rejects_changed_range", || {
        cursor_rejects_changed_range(factory)
    });
    report.run("baseline::cursor_rejects_changed_projection", || {
        cursor_rejects_changed_projection(factory)
    });
    report.run(
        "baseline::cursor_rejects_different_read_transaction",
        || cursor_rejects_different_read_transaction(factory),
    );
}

fn get_many_preserves_caller_order_duplicates_and_missing<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = factory.fresh();
    let test_space = space(1);
    let key_a = key("a");
    let key_b = key("b");
    let missing = key("missing");

    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .put_many(
            test_space,
            put_batch([full_put(key_a.clone(), "A"), full_put(key_b.clone(), "B")]),
        )
        .map_err(|error| format!("put_many failed: {error}"))?;
    write
        .commit()
        .map_err(|error| format!("commit failed: {error}"))?;

    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let result = read
        .get_many(
            test_space,
            &[key_b.clone(), key_a.clone(), key_b.clone(), missing.clone()],
            GetOptions {
                projection: ValueProjection::FullValue,
                ..Default::default()
            },
        )
        .map_err(|error| format!("get_many failed: {error}"))?;

    let actual = result
        .entries
        .into_iter()
        .map(|slot| {
            (
                slot.requested_index,
                slot.key,
                slot.value.map(projected_value_bytes),
            )
        })
        .collect::<Vec<_>>();
    let expected = vec![
        (Some(0), key_b.clone(), Some(Bytes::from_static(b"B"))),
        (Some(1), key_a.clone(), Some(Bytes::from_static(b"A"))),
        (Some(2), key_b, Some(Bytes::from_static(b"B"))),
        (Some(3), missing, None),
    ];

    if actual != expected {
        return Err(format!(
            "caller-order get_many mismatch: expected {expected:?}, got {actual:?}"
        ));
    }

    Ok(())
}

fn get_many_empty_key_list<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = factory.fresh();
    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let result = read
        .get_many(space(1), &[], GetOptions::default())
        .map_err(|error| format!("get_many failed: {error}"))?;
    if result.entries.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "empty get_many returned entries: {:?}",
            result.entries
        ))
    }
}

fn get_many_missing_only_and_duplicate_missing<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = factory.fresh();
    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let missing_a = key("missing-a");
    let missing_b = key("missing-b");
    let result = read
        .get_many(
            space(1),
            &[missing_a.clone(), missing_b.clone(), missing_a.clone()],
            GetOptions::default(),
        )
        .map_err(|error| format!("get_many failed: {error}"))?;

    let actual = result
        .entries
        .into_iter()
        .map(|slot| (slot.requested_index, slot.key, slot.value.is_some()))
        .collect::<Vec<_>>();
    let expected = vec![
        (Some(0), missing_a.clone(), false),
        (Some(1), missing_b, false),
        (Some(2), missing_a, false),
    ];
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "missing-only get_many mismatch: expected {expected:?}, got {actual:?}"
        ))
    }
}

fn write_reads_its_own_writes<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = factory.fresh();
    let test_space = space(1);
    let key_a = key("a");

    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .put_many(test_space, put_batch([full_put(key_a.clone(), "A")]))
        .map_err(|error| format!("put_many failed: {error}"))?;

    let result = write
        .get_many(
            test_space,
            &[key_a.clone()],
            GetOptions {
                projection: ValueProjection::FullValue,
                ..Default::default()
            },
        )
        .map_err(|error| format!("write get_many failed: {error}"))?;

    let Some(slot) = result.entries.first() else {
        return Err("write get_many returned no slots".to_string());
    };
    let value = slot
        .value
        .clone()
        .map(projected_value_bytes)
        .ok_or_else(|| "write did not read its staged put".to_string())?;
    if value != Bytes::from_static(b"A") {
        return Err(format!("expected staged value A, got {value:?}"));
    }

    write
        .delete_many(test_space, &[key_a.clone()])
        .map_err(|error| format!("delete_many failed: {error}"))?;
    let result = write
        .get_many(test_space, &[key_a], GetOptions::default())
        .map_err(|error| format!("write get_many after delete failed: {error}"))?;
    if result
        .entries
        .first()
        .and_then(|slot| slot.value.as_ref())
        .is_some()
    {
        return Err("write did not read its staged delete".to_string());
    }

    write
        .rollback()
        .map_err(|error| format!("rollback failed: {error}"))?;

    Ok(())
}

fn delete_many_missing_keys_is_idempotent<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = factory.fresh();
    let test_space = space(1);
    seed_full_values(&backend, test_space, [("a", "A")])?;

    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .delete_many(test_space, &[key("missing")])
        .map_err(|error| format!("delete_many missing failed: {error}"))?;
    write
        .commit()
        .map_err(|error| format!("commit failed: {error}"))?;

    let result = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?
        .get_many(test_space, &[key("a")], GetOptions::default())
        .map_err(|error| format!("get_many failed: {error}"))?;
    assert_slots(&result.entries, &[("a", Some("A"))])
}

fn put_many_overwrites_existing_value<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = factory.fresh();
    let test_space = space(1);
    let key_a = key("a");
    seed_full_values(&backend, test_space, [("a", "A")])?;

    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .put_many(test_space, put_batch([full_put(key_a.clone(), "B")]))
        .map_err(|error| format!("put_many overwrite failed: {error}"))?;
    write
        .commit()
        .map_err(|error| format!("commit failed: {error}"))?;

    let result = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?
        .get_many(test_space, &[key_a], GetOptions::default())
        .map_err(|error| format!("get_many failed: {error}"))?;
    assert_slots(&result.entries, &[("a", Some("B"))])
}

fn scan_range_returns_forward_row_bounded_pages<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = factory.fresh();
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

    let first_page = read
        .scan_range(
            test_space,
            range.clone(),
            ScanOptions {
                limit_rows: Some(2),
                ..Default::default()
            },
        )
        .map_err(|error| format!("first scan_range failed: {error}"))?;
    assert_read_entries(
        &first_page.entries.entries,
        &[("b", Some("B")), ("c", Some("C"))],
    )?;
    let Some(cursor) = first_page.next_cursor.as_ref() else {
        return Err("first scan page did not return continuation cursor".to_string());
    };

    let second_page = read
        .scan_range(
            test_space,
            range,
            ScanOptions {
                limit_rows: Some(2),
                cursor: Some(cursor),
                ..Default::default()
            },
        )
        .map_err(|error| format!("second scan_range failed: {error}"))?;
    assert_read_entries(&second_page.entries.entries, &[("d", Some("D"))])?;
    if second_page.next_cursor.is_some() {
        return Err("last scan page unexpectedly returned continuation cursor".to_string());
    }

    Ok(())
}

fn scan_range_honors_bound_variants<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = factory.fresh();
    let test_space = space(1);
    seed_full_values(
        &backend,
        test_space,
        [("a", "A"), ("b", "B"), ("c", "C"), ("d", "D")],
    )?;
    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;

    let included = read
        .scan_range(
            test_space,
            KeyRange {
                lower: Bound::Included(key("b")),
                upper: Bound::Included(key("c")),
            },
            ScanOptions::default(),
        )
        .map_err(|error| format!("included range scan failed: {error}"))?;
    assert_read_entries(
        &included.entries.entries,
        &[("b", Some("B")), ("c", Some("C"))],
    )?;

    let excluded = read
        .scan_range(
            test_space,
            KeyRange {
                lower: Bound::Excluded(key("b")),
                upper: Bound::Excluded(key("d")),
            },
            ScanOptions::default(),
        )
        .map_err(|error| format!("excluded range scan failed: {error}"))?;
    assert_read_entries(&excluded.entries.entries, &[("c", Some("C"))])?;

    let unbounded_lower = read
        .scan_range(
            test_space,
            KeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Excluded(key("c")),
            },
            ScanOptions::default(),
        )
        .map_err(|error| format!("unbounded lower range scan failed: {error}"))?;
    assert_read_entries(
        &unbounded_lower.entries.entries,
        &[("a", Some("A")), ("b", Some("B"))],
    )?;

    let unbounded_upper = read
        .scan_range(
            test_space,
            KeyRange {
                lower: Bound::Included(key("c")),
                upper: Bound::Unbounded,
            },
            ScanOptions::default(),
        )
        .map_err(|error| format!("unbounded upper range scan failed: {error}"))?;
    assert_read_entries(
        &unbounded_upper.entries.entries,
        &[("c", Some("C")), ("d", Some("D"))],
    )
}

fn scan_range_limit_zero_returns_empty_page<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = factory.fresh();
    let test_space = space(1);
    seed_full_values(&backend, test_space, [("a", "A")])?;
    let page = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?
        .scan_range(
            test_space,
            KeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            ScanOptions {
                limit_rows: Some(0),
                ..Default::default()
            },
        )
        .map_err(|error| format!("scan_range failed: {error}"))?;
    if !page.entries.entries.is_empty() {
        Err(format!("limit_rows=0 returned entries: {:?}", page.entries))
    } else if page.next_cursor.is_some() {
        Err(format!(
            "limit_rows=0 returned cursor: {:?}",
            page.next_cursor
        ))
    } else {
        Ok(())
    }
}

fn scan_range_empty_range_returns_empty_page<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = factory.fresh();
    let test_space = space(1);
    seed_full_values(&backend, test_space, [("a", "A"), ("b", "B")])?;
    let page = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?
        .scan_range(
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

fn scan_prefix_matches_equivalent_range<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = factory.fresh();
    let test_space = space(1);
    seed_full_values(
        &backend,
        test_space,
        [("aa", "AA"), ("ab", "AB"), ("b", "B")],
    )?;

    let prefix = Prefix {
        bytes: Bytes::from_static(b"a"),
    };
    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let prefix_page = read
        .scan_prefix(test_space, prefix.clone(), ScanOptions::default())
        .map_err(|error| format!("scan_prefix failed: {error}"))?;
    let range_page = read
        .scan_range(
            test_space,
            prefix
                .to_range()
                .map_err(|error| format!("prefix range conversion failed: {error}"))?,
            ScanOptions::default(),
        )
        .map_err(|error| format!("scan_range failed: {error}"))?;

    let prefix_entries = entries_to_key_values(&prefix_page.entries.entries);
    let range_entries = entries_to_key_values(&range_page.entries.entries);
    if prefix_entries != range_entries {
        return Err(format!(
            "scan_prefix did not match equivalent range: prefix {prefix_entries:?}, range {range_entries:?}"
        ));
    }
    if prefix_entries
        != vec![
            (key("aa"), Some(Bytes::from_static(b"AA"))),
            (key("ab"), Some(Bytes::from_static(b"AB"))),
        ]
    {
        return Err(format!("unexpected prefix entries: {prefix_entries:?}"));
    }

    Ok(())
}

fn scan_prefix_empty_prefix_scans_whole_space<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = factory.fresh();
    let test_space = space(1);
    seed_full_values(&backend, test_space, [("a", "A"), ("b", "B")])?;
    seed_full_values(&backend, space(2), [("z", "Z")])?;

    let page = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?
        .scan_prefix(
            test_space,
            Prefix {
                bytes: Bytes::new(),
            },
            ScanOptions::default(),
        )
        .map_err(|error| format!("scan_prefix failed: {error}"))?;
    assert_read_entries(&page.entries.entries, &[("a", Some("A")), ("b", Some("B"))])
}

fn scan_prefix_ff_prefix_uses_unbounded_upper_range<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = factory.fresh();
    let test_space = space(1);
    seed_full_byte_values(
        &backend,
        test_space,
        [
            (Bytes::from_static(b"\xfe"), Bytes::from_static(b"FE")),
            (Bytes::from_static(b"\xff"), Bytes::from_static(b"FF")),
            (Bytes::from_static(b"\xff\0"), Bytes::from_static(b"FF00")),
            (Bytes::from_static(b"\xff\xff"), Bytes::from_static(b"FFFF")),
        ],
    )?;

    let page = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?
        .scan_prefix(
            test_space,
            Prefix {
                bytes: Bytes::from_static(b"\xff"),
            },
            ScanOptions::default(),
        )
        .map_err(|error| format!("scan_prefix failed: {error}"))?;
    assert_read_entries_bytes(
        &page.entries.entries,
        &[
            (Bytes::from_static(b"\xff"), Some(Bytes::from_static(b"FF"))),
            (
                Bytes::from_static(b"\xff\0"),
                Some(Bytes::from_static(b"FF00")),
            ),
            (
                Bytes::from_static(b"\xff\xff"),
                Some(Bytes::from_static(b"FFFF")),
            ),
        ],
    )
}

fn commit_is_atomic<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = factory.fresh();
    let test_space = space(1);
    let key_a = key("a");
    let key_b = key("b");

    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .put_many(
            test_space,
            put_batch([full_put(key_a.clone(), "A"), full_put(key_b.clone(), "B")]),
        )
        .map_err(|error| format!("put_many failed: {error}"))?;

    let before_commit = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read before commit failed: {error}"))?
        .get_many(
            test_space,
            &[key_a.clone(), key_b.clone()],
            GetOptions::default(),
        )
        .map_err(|error| format!("get_many before commit failed: {error}"))?;
    if before_commit
        .entries
        .iter()
        .any(|slot| slot.value.is_some())
    {
        return Err("uncommitted writes were visible to an independent read".to_string());
    }

    write
        .commit()
        .map_err(|error| format!("commit failed: {error}"))?;
    let after_commit = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read after commit failed: {error}"))?
        .get_many(test_space, &[key_a, key_b], GetOptions::default())
        .map_err(|error| format!("get_many after commit failed: {error}"))?;
    assert_slots(&after_commit.entries, &[("a", Some("A")), ("b", Some("B"))])?;

    Ok(())
}

fn rollback_discards_staged_mutations<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = factory.fresh();
    let test_space = space(1);
    let key_a = key("a");

    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .put_many(test_space, put_batch([full_put(key_a.clone(), "A")]))
        .map_err(|error| format!("put_many failed: {error}"))?;
    write
        .rollback()
        .map_err(|error| format!("rollback failed: {error}"))?;

    let result = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?
        .get_many(test_space, &[key_a], GetOptions::default())
        .map_err(|error| format!("get_many failed: {error}"))?;
    assert_slots(&result.entries, &[("a", None)])?;

    Ok(())
}

fn begin_read_pins_coherent_view<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = factory.fresh();
    let test_space = space(1);
    let key_a = key("a");
    seed_full_values(&backend, test_space, [("a", "A")])?;

    let old_read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;

    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .put_many(test_space, put_batch([full_put(key_a.clone(), "B")]))
        .map_err(|error| format!("put_many failed: {error}"))?;
    write
        .commit()
        .map_err(|error| format!("commit failed: {error}"))?;

    let old_result = old_read
        .get_many(test_space, &[key_a.clone()], GetOptions::default())
        .map_err(|error| format!("old read get_many failed: {error}"))?;
    assert_slots(&old_result.entries, &[("a", Some("A"))])?;

    let new_result = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("new begin_read failed: {error}"))?
        .get_many(test_space, &[key_a], GetOptions::default())
        .map_err(|error| format!("new read get_many failed: {error}"))?;
    assert_slots(&new_result.entries, &[("a", Some("B"))])?;

    Ok(())
}

fn spaces_are_isolated<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = factory.fresh();
    let first_space = space(1);
    let second_space = space(2);
    let shared_key = key("same");

    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .put_many(
            first_space,
            put_batch([full_put(shared_key.clone(), "one")]),
        )
        .map_err(|error| format!("first put_many failed: {error}"))?;
    write
        .put_many(
            second_space,
            put_batch([full_put(shared_key.clone(), "two")]),
        )
        .map_err(|error| format!("second put_many failed: {error}"))?;
    write
        .commit()
        .map_err(|error| format!("commit failed: {error}"))?;

    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let first = read
        .get_many(first_space, &[shared_key.clone()], GetOptions::default())
        .map_err(|error| format!("first get_many failed: {error}"))?;
    assert_slots(&first.entries, &[("same", Some("one"))])?;

    let second = read
        .get_many(second_space, &[shared_key], GetOptions::default())
        .map_err(|error| format!("second get_many failed: {error}"))?;
    assert_slots(&second.entries, &[("same", Some("two"))])?;

    Ok(())
}

fn full_value_and_key_only_are_core<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = factory.fresh();
    let test_space = space(1);
    let key_a = key("a");
    seed_full_values(&backend, test_space, [("a", "A")])?;

    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let full = read
        .get_many(
            test_space,
            &[key_a.clone()],
            GetOptions {
                projection: ValueProjection::FullValue,
                ..Default::default()
            },
        )
        .map_err(|error| format!("FullValue get_many failed: {error}"))?;
    assert_slots(&full.entries, &[("a", Some("A"))])?;

    let key_only = read
        .get_many(
            test_space,
            &[key_a],
            GetOptions {
                projection: ValueProjection::KeyOnly,
                ..Default::default()
            },
        )
        .map_err(|error| format!("KeyOnly get_many failed: {error}"))?;
    match key_only
        .entries
        .first()
        .and_then(|slot| slot.value.as_ref())
    {
        Some(ProjectedValue::KeyOnly) => Ok(()),
        other => Err(format!("expected KeyOnly projected value, got {other:?}")),
    }
}

fn read_support_metadata_is_truthful_for_core_reads<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = factory.fresh();
    let test_space = space(1);
    seed_full_values(&backend, test_space, [("a", "A"), ("b", "B")])?;

    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let full = read
        .get_many(
            test_space,
            &[key("a")],
            GetOptions {
                projection: ValueProjection::FullValue,
                ..Default::default()
            },
        )
        .map_err(|error| format!("FullValue get_many failed: {error}"))?;
    assert_projection_support(
        &full.support.projection,
        ValueProjection::FullValue,
        ValueProjection::FullValue,
        Support::Exact,
    )?;
    if full.support.order != OrderSupport::Exact {
        return Err(format!(
            "caller-order get_many should report exact order, got {:?}",
            full.support.order
        ));
    }
    if !full.support.predicates.is_empty() {
        return Err(format!(
            "empty predicate request reported predicate support entries: {:?}",
            full.support.predicates
        ));
    }

    let key_only = read
        .get_many(
            test_space,
            &[key("a")],
            GetOptions {
                projection: ValueProjection::KeyOnly,
                ..Default::default()
            },
        )
        .map_err(|error| format!("KeyOnly get_many failed: {error}"))?;
    assert_projection_support(
        &key_only.support.projection,
        ValueProjection::KeyOnly,
        ValueProjection::KeyOnly,
        Support::Exact,
    )?;

    let page = read
        .scan_range(
            test_space,
            KeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            ScanOptions {
                limit_rows: Some(1),
                ..Default::default()
            },
        )
        .map_err(|error| format!("scan_range failed: {error}"))?;
    if page.support.limit != LimitSupport::Final {
        return Err(format!(
            "row-limited scan should report final limit support, got {:?}",
            page.support.limit
        ));
    }
    if !page.support.predicates.is_empty() {
        return Err(format!(
            "empty predicate scan reported predicate support entries: {:?}",
            page.support.predicates
        ));
    }

    Ok(())
}

fn cursor_rejects_changed_range<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = factory.fresh();
    let test_space = space(1);
    seed_full_values(&backend, test_space, [("a", "A"), ("b", "B"), ("c", "C")])?;

    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let original_range = KeyRange {
        lower: Bound::Included(key("a")),
        upper: Bound::Unbounded,
    };
    let cursor = first_page_cursor(&read, test_space, original_range)?;
    let changed_range = KeyRange {
        lower: Bound::Included(key("b")),
        upper: Bound::Unbounded,
    };
    assert_invalid_cursor(read.scan_range(
        test_space,
        changed_range,
        ScanOptions {
            cursor: Some(&cursor),
            ..Default::default()
        },
    ))
}

fn cursor_rejects_changed_projection<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = factory.fresh();
    let test_space = space(1);
    seed_full_values(&backend, test_space, [("a", "A"), ("b", "B")])?;

    let read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let range = KeyRange {
        lower: Bound::Unbounded,
        upper: Bound::Unbounded,
    };
    let cursor = first_page_cursor(&read, test_space, range.clone())?;
    assert_invalid_cursor(read.scan_range(
        test_space,
        range,
        ScanOptions {
            projection: ValueProjection::KeyOnly,
            cursor: Some(&cursor),
            ..Default::default()
        },
    ))
}

fn cursor_rejects_different_read_transaction<F>(factory: &F) -> ConformanceResult
where
    F: BackendFactory,
{
    let backend = factory.fresh();
    let test_space = space(1);
    seed_full_values(&backend, test_space, [("a", "A"), ("b", "B")])?;

    let range = KeyRange {
        lower: Bound::Unbounded,
        upper: Bound::Unbounded,
    };
    let old_read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("old begin_read failed: {error}"))?;
    let cursor = first_page_cursor(&old_read, test_space, range.clone())?;

    let new_read = backend
        .begin_read(ReadOptions::default())
        .map_err(|error| format!("new begin_read failed: {error}"))?;
    assert_invalid_cursor(new_read.scan_range(
        test_space,
        range,
        ScanOptions {
            cursor: Some(&cursor),
            ..Default::default()
        },
    ))
}

fn projected_value_bytes(value: ProjectedValue) -> Bytes {
    match value {
        ProjectedValue::FullValue(bytes)
        | ProjectedValue::Header(bytes)
        | ProjectedValue::Refs(bytes)
        | ProjectedValue::Payload(bytes) => bytes,
        ProjectedValue::HeaderAndRefs { header, refs } => {
            let mut bytes = Vec::with_capacity(header.len() + refs.len());
            bytes.extend_from_slice(&header);
            bytes.extend_from_slice(&refs);
            Bytes::from(bytes)
        }
        ProjectedValue::KeyOnly => Bytes::new(),
    }
}

fn first_page_cursor<R>(
    read: &R,
    test_space: crate::backend_v2::SpaceId,
    range: KeyRange,
) -> Result<crate::backend_v2::Cursor, String>
where
    R: BackendRead,
{
    let page = read
        .scan_range(
            test_space,
            range,
            ScanOptions {
                limit_rows: Some(1),
                ..Default::default()
            },
        )
        .map_err(|error| format!("scan_range for cursor failed: {error}"))?;
    page.next_cursor
        .ok_or_else(|| "expected first scan page to return cursor".to_string())
}

fn assert_invalid_cursor(
    result: Result<crate::backend_v2::ScanPage, BackendError>,
) -> ConformanceResult {
    match result {
        Err(BackendError::InvalidCursor) => Ok(()),
        Err(other) => Err(format!("expected InvalidCursor, got {other:?}")),
        Ok(page) => Err(format!("expected InvalidCursor, got success: {page:?}")),
    }
}

fn assert_projection_support(
    support: &crate::backend_v2::ProjectionSupport,
    requested: ValueProjection,
    returned: ValueProjection,
    expected_support: Support,
) -> ConformanceResult {
    if support.requested == requested
        && support.returned == returned
        && support.support == expected_support
    {
        Ok(())
    } else {
        Err(format!(
            "projection support mismatch: expected requested={requested:?} returned={returned:?} support={expected_support:?}, got {support:?}"
        ))
    }
}

fn seed_full_values<B, I>(
    backend: &B,
    test_space: crate::backend_v2::SpaceId,
    rows: I,
) -> ConformanceResult
where
    B: Backend,
    I: IntoIterator<Item = (&'static str, &'static str)>,
{
    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("seed begin_write failed: {error}"))?;
    write
        .put_many(
            test_space,
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

fn seed_full_byte_values<B, I>(
    backend: &B,
    test_space: crate::backend_v2::SpaceId,
    rows: I,
) -> ConformanceResult
where
    B: Backend,
    I: IntoIterator<Item = (Bytes, Bytes)>,
{
    let mut write = backend
        .begin_write(WriteOptions::default())
        .map_err(|error| format!("seed begin_write failed: {error}"))?;
    write
        .put_many(
            test_space,
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

fn assert_slots(
    slots: &[crate::backend_v2::GetSlot],
    expected: &[(&str, Option<&str>)],
) -> ConformanceResult {
    let actual = slots
        .iter()
        .map(|slot| {
            (
                slot.key.clone(),
                slot.value.clone().map(projected_value_bytes),
            )
        })
        .collect::<Vec<_>>();
    let expected = expected
        .iter()
        .map(|(key_bytes, value)| {
            (
                key(*key_bytes),
                value.map(|value| Bytes::from(value.as_bytes().to_vec())),
            )
        })
        .collect::<Vec<_>>();
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "slot mismatch: expected {expected:?}, got {actual:?}"
        ))
    }
}

fn assert_read_entries(
    entries: &[crate::backend_v2::ReadEntry],
    expected: &[(&str, Option<&str>)],
) -> ConformanceResult {
    let actual = entries_to_key_values(entries);
    let expected = expected
        .iter()
        .map(|(key_bytes, value)| {
            (
                key(*key_bytes),
                value.map(|value| Bytes::from(value.as_bytes().to_vec())),
            )
        })
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
    entries: &[crate::backend_v2::ReadEntry],
    expected: &[(Bytes, Option<Bytes>)],
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

fn entries_to_key_values(entries: &[crate::backend_v2::ReadEntry]) -> Vec<(Key, Option<Bytes>)> {
    entries
        .iter()
        .map(|entry| {
            (
                entry.key.clone(),
                Some(projected_value_bytes(entry.value.clone())),
            )
        })
        .collect()
}
