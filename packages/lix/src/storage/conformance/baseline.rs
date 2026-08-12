use std::collections::BTreeMap;
use std::ops::Bound;

/// Single space used by most baseline fixtures; the cross-space tests at
/// the bottom of this file pin space isolation.
const TEST_SPACE: StorageSpace = StorageSpace::mutable(SpaceId(7), "storage.conformance.test");
const OTHER_SPACE: StorageSpace = StorageSpace::mutable(SpaceId(8), "storage.conformance.other");
/// Same id and bytes as [`TEST_SPACE`] would hold, declared with the value
/// integrity that permits a backend to skip its own value checksum.
const CONTENT_ADDRESSED_SPACE: StorageSpace = StorageSpace::declare_content_addressed(
    SpaceId(9),
    "storage.conformance.content_addressed",
    ValueSemantics::Mutable,
);

use bytes::Bytes;

use crate::storage::conformance::{
    ConformanceReport, ConformanceResult, SingleSpaceStorageRead, StorageFactory,
    fixtures::{full_put, key, put_batch, space},
    open_storage,
};
use crate::storage::{
    BeginScanOptions, CoreProjection, GetOptions, Key, KeyRange, MAX_SCAN_PAGE_ROWS, Precondition,
    ProjectedValue, ReadEntry, ReadOptions, ScanChunk, ScanOrder, SpaceId, Storage, StorageError,
    StorageRead, StorageSpace, StorageWrite, ValueSemantics, WriteOptions,
};

pub(crate) async fn register<F>(report: &mut ConformanceReport, factory: &F)
where
    F: StorageFactory,
{
    macro_rules! run {
        ($name:literal, $test:ident) => {
            report.run($name, $test(factory)).await;
        };
    }

    run!("baseline::spaces_do_not_collide", spaces_do_not_collide);
    run!("baseline::scan_is_space_scoped", scan_is_space_scoped);
    run!(
        "baseline::unbounded_delete_range_truncates_only_target_space",
        unbounded_delete_range_truncates_only_target_space
    );
    run!(
        "baseline::empty_space_reads_are_empty",
        empty_space_reads_are_empty
    );
    run!(
        "baseline::get_many_returns_requested_slots",
        get_many_returns_requested_slots
    );
    run!("baseline::get_many_empty_key_list", get_many_empty_key_list);
    run!(
        "baseline::content_addressed_space_returns_identical_bytes",
        content_addressed_space_returns_identical_bytes
    );
    run!(
        "baseline::delete_many_missing_keys_is_idempotent",
        delete_many_missing_keys_is_idempotent
    );
    run!(
        "baseline::delete_many_removes_existing_keys",
        delete_many_removes_existing_keys
    );
    run!(
        "baseline::delete_range_removes_exact_range",
        delete_range_removes_exact_range
    );
    run!(
        "baseline::put_many_applies_after_delete_range",
        put_many_applies_after_delete_range
    );
    run!(
        "baseline::put_many_overwrites_existing_value",
        put_many_overwrites_existing_value
    );
    run!(
        "baseline::scan_range_sees_overwritten_existing_value",
        scan_range_sees_overwritten_existing_value
    );
    run!(
        "baseline::scan_range_returns_forward_row_bounded_chunks",
        scan_range_returns_forward_row_bounded_chunks
    );
    run!(
        "baseline::scan_range_caps_owned_pages",
        scan_range_caps_owned_pages
    );
    run!(
        "baseline::scan_range_honors_bound_variants",
        scan_range_honors_bound_variants
    );
    run!(
        "baseline::scan_range_resume_before_lower_does_not_widen_range",
        scan_range_resume_before_lower_does_not_widen_range
    );
    run!(
        "baseline::scan_range_orders_raw_byte_keys",
        scan_range_orders_raw_byte_keys
    );
    run!(
        "baseline::scan_range_drains_multi_chunk_limits",
        scan_range_drains_multi_chunk_limits
    );
    run!(
        "baseline::scan_cursor_drains_multi_chunk_limits",
        scan_cursor_drains_multi_chunk_limits
    );
    run!(
        "baseline::scan_range_empty_range_returns_empty_chunk",
        scan_range_empty_range_returns_empty_chunk
    );
    run!("baseline::commit_is_atomic", commit_is_atomic);
    run!(
        "baseline::write_precondition_rejects_stale_value",
        write_precondition_rejects_stale_value
    );
    run!(
        "baseline::rollback_discards_staged_mutations",
        rollback_discards_staged_mutations
    );
    run!(
        "baseline::rollback_discards_overwrite_and_delete",
        rollback_discards_overwrite_and_delete
    );
    run!(
        "baseline::begin_read_pins_coherent_view",
        begin_read_pins_coherent_view
    );
    run!(
        "baseline::scan_cursor_survives_concurrent_commit_and_restarts_exclusively",
        scan_cursor_survives_concurrent_commit_and_restarts_exclusively
    );
    run!(
        "baseline::unpolled_scan_page_cancellation_keeps_cursor_usable",
        unpolled_scan_page_cancellation_keeps_cursor_usable
    );
    run!(
        "baseline::descending_scan_is_explicitly_unsupported",
        descending_scan_is_explicitly_unsupported
    );
    run!(
        "baseline::invalid_scan_range_fails_closed",
        invalid_scan_range_fails_closed
    );
    run!(
        "baseline::full_value_and_key_only_are_core",
        full_value_and_key_only_are_core
    );
    run!(
        "baseline::full_value_preserves_opaque_bytes",
        full_value_preserves_opaque_bytes
    );
    run!(
        "baseline::immutable_identity_is_idempotent_and_write_once",
        immutable_identity_is_idempotent_and_write_once
    );
}

async fn immutable_identity_is_idempotent_and_write_once<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let immutable = StorageSpace::immutable(SpaceId(9), "storage.conformance.immutable");
    let target = key("content-id");
    for value in [
        Bytes::from_static(b"payload"),
        Bytes::from_static(b"payload"),
    ] {
        let mut write = storage
            .begin_write(WriteOptions::default())
            .await
            .map_err(|error| error.to_string())?;
        write
            .put_many(immutable, put_batch([full_put(target.clone(), value)]))
            .await
            .map_err(|error| error.to_string())?;
        write.commit().await.map_err(|error| error.to_string())?;
    }

    let mut conflicting = storage
        .begin_write(WriteOptions::default())
        .await
        .map_err(|error| error.to_string())?;
    let staged = conflicting
        .put_many(
            immutable,
            put_batch([full_put(target.clone(), Bytes::from_static(b"different"))]),
        )
        .await;
    let rejected = match staged {
        Err(StorageError::Corruption(_)) => true,
        Err(error) => return Err(error.to_string()),
        Ok(()) => matches!(conflicting.commit().await, Err(StorageError::Corruption(_))),
    };
    if !rejected {
        return Err("immutable identity accepted different bytes".to_string());
    }
    Ok(())
}

async fn write_precondition_rejects_stale_value<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let test_space = TEST_SPACE;
    let target = key("target");
    seed_full_values(&storage, test_space, [("target", "base")]).await?;

    let mut conditional = storage
        .begin_write(WriteOptions {
            preconditions: vec![Precondition::KeyValueEquals {
                space: test_space,
                key: target.clone(),
                expected: Bytes::from_static(b"base"),
            }],
            ..WriteOptions::default()
        })
        .await
        .map_err(|error| error.to_string())?;
    conditional
        .put_many(
            test_space,
            put_batch([full_put(target.clone(), Bytes::from_static(b"conditional"))]),
        )
        .await
        .map_err(|error| error.to_string())?;
    conditional
        .commit()
        .await
        .map_err(|error| error.to_string())?;

    let stale_options = WriteOptions {
        preconditions: vec![Precondition::KeyValueEquals {
            space: test_space,
            key: target.clone(),
            expected: Bytes::from_static(b"conditional"),
        }],
        ..WriteOptions::default()
    };
    seed_full_values(&storage, test_space, [("target", "winner")]).await?;

    match storage.begin_write(stale_options).await {
        Err(StorageError::PreconditionFailed(failures)) => {
            assert_eq!(failures.len(), 1);
            assert_eq!(failures[0].index, 0);
        }
        Err(error) => return Err(error.to_string()),
        Ok(mut stale) => {
            stale
                .put_many(
                    test_space,
                    put_batch([full_put(target.clone(), Bytes::from_static(b"stale"))]),
                )
                .await
                .map_err(|error| error.to_string())?;
            match stale.commit().await {
                Err(StorageError::PreconditionFailed(failures)) => {
                    assert_eq!(failures.len(), 1);
                    assert_eq!(failures[0].index, 0);
                }
                Err(error) => return Err(error.to_string()),
                Ok(_) => panic!("stale conditional write unexpectedly committed"),
            }
        }
    }

    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| error.to_string())?;
    let value = read
        .get_many_in_space(
            test_space,
            std::slice::from_ref(&target),
            GetOptions::default(),
        )
        .await
        .map_err(|error| error.to_string())?;
    assert_eq!(
        value.values,
        vec![Some(ProjectedValue::FullValue(Bytes::from_static(
            b"winner"
        )))]
    );
    Ok(())
}

async fn get_many_returns_requested_slots<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let test_space = space(1);
    seed_full_values(&storage, test_space, [("a", "A"), ("b", "B")]).await?;

    let requested = [key("b"), key("missing"), key("a"), key("b")];
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let result = read
        .get_many_in_space(TEST_SPACE, &requested, GetOptions::default())
        .await
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

async fn get_many_empty_key_list<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let result = read
        .get_many_in_space(TEST_SPACE, &[], GetOptions::default())
        .await
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

/// `ValueIntegrity::ContentAddressed` must not change what a read returns.
///
/// It is a licence to skip a *redundant* corruption check on bytes the engine
/// authenticates itself, and nothing more. An adapter is free to ignore it
/// entirely — SlateDB does, because `slatedb::config::ReadOptions` has no
/// checksum control — but an adapter that acts on it must return byte-identical
/// values, through both `get_many` and a full-value scan, and must still report
/// a missing key as missing.
///
/// The same rows are written to a `BackendVerified` space and a
/// `ContentAddressed` one and the two reads are compared to each other, so this
/// fails if a backend's opted-out path diverges in any way — not merely if it
/// returns something a hand-written expectation did not anticipate.
async fn content_addressed_space_returns_identical_bytes<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let rows: [(&str, &[u8]); 3] = [
        ("a", b"A"),
        ("b", b"a much longer value than the block would like"),
        ("c", &[0u8; 512]),
    ];

    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .map_err(|error| format!("begin_write failed: {error}"))?;
    for target in [TEST_SPACE, CONTENT_ADDRESSED_SPACE] {
        write
            .put_many(
                target,
                put_batch(
                    rows.iter()
                        .map(|(key_bytes, value_bytes)| full_put(key(key_bytes), *value_bytes)),
                ),
            )
            .await
            .map_err(|error| format!("put_many into {target} failed: {error}"))?;
    }
    write
        .commit()
        .await
        .map_err(|error| format!("commit failed: {error}"))?;

    let requested = [key("a"), key("b"), key("c"), key("missing")];
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin_read failed: {error}"))?;

    let verified = read
        .get_many_in_space(TEST_SPACE, &requested, GetOptions::default())
        .await
        .map_err(|error| format!("get_many on the verified space failed: {error}"))?;
    let addressed = read
        .get_many_in_space(CONTENT_ADDRESSED_SPACE, &requested, GetOptions::default())
        .await
        .map_err(|error| format!("get_many on the content-addressed space failed: {error}"))?;

    if verified.values != addressed.values {
        return Err(format!(
            "content-addressed space returned different values than the verified space: \
             {:?} vs {:?}",
            verified.values, addressed.values
        ));
    }
    if addressed.values.last() != Some(&None) {
        return Err(format!(
            "a missing key must stay missing on a content-addressed space, got {:?}",
            addressed.values.last()
        ));
    }

    let scanned = collect_full_values(&read, CONTENT_ADDRESSED_SPACE).await?;
    let expected = rows
        .iter()
        .map(|(key_bytes, value_bytes)| (key(key_bytes), Bytes::copy_from_slice(value_bytes)))
        .collect::<Vec<_>>();
    if scanned != expected {
        return Err(format!(
            "full-value scan of a content-addressed space returned {scanned:?}, expected {expected:?}"
        ));
    }

    Ok(())
}

/// Drains a full-value scan of `space` into `(key, value)` pairs in key order.
async fn collect_full_values<R>(read: &R, space: StorageSpace) -> Result<Vec<(Key, Bytes)>, String>
where
    R: StorageRead,
{
    let mut cursor = read
        .begin_scan(
            space,
            KeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            BeginScanOptions::default(),
        )
        .await
        .map_err(|error| format!("begin_scan failed: {error}"))?;
    let entries = cursor
        .collect_all()
        .await
        .map_err(|error| format!("scan collect_all failed: {error}"))?;
    let mut out = Vec::with_capacity(entries.len());
    for entry in entries {
        let ReadEntry {
            key: entry_key,
            value: ProjectedValue::FullValue(bytes),
        } = entry
        else {
            return Err("full-value scan yielded a key-only entry".to_string());
        };
        out.push((entry_key, bytes));
    }
    Ok(out)
}

async fn delete_many_missing_keys_is_idempotent<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let test_space = space(1);
    seed_full_values(&storage, test_space, [("a", "A")]).await?;

    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .delete_many(TEST_SPACE, &[key("missing")])
        .await
        .map_err(|error| format!("delete_many missing failed: {error}"))?;
    write
        .commit()
        .await
        .map_err(|error| format!("commit failed: {error}"))?;

    assert_get_entries(&storage, test_space, &[("a", Some("A"))]).await
}

async fn delete_many_removes_existing_keys<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let test_space = space(1);
    seed_full_values(&storage, test_space, [("a", "A"), ("b", "B")]).await?;

    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .delete_many(TEST_SPACE, &[key("a")])
        .await
        .map_err(|error| format!("delete_many existing failed: {error}"))?;
    write
        .commit()
        .await
        .map_err(|error| format!("commit failed: {error}"))?;

    assert_get_entries(&storage, test_space, &[("a", None), ("b", Some("B"))]).await
}

async fn delete_range_removes_exact_range<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let test_space = space(1);
    seed_full_values(
        &storage,
        test_space,
        [("a", "A"), ("b", "B"), ("c", "C"), ("d", "D"), ("e", "E")],
    )
    .await?;

    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .delete_range(
            TEST_SPACE,
            KeyRange {
                lower: Bound::Included(key("b")),
                upper: Bound::Excluded(key("d")),
            },
        )
        .await
        .map_err(|error| format!("delete_range failed: {error}"))?;
    write
        .commit()
        .await
        .map_err(|error| format!("commit failed: {error}"))?;

    assert_get_entries(
        &storage,
        test_space,
        &[
            ("a", Some("A")),
            ("b", None),
            ("c", None),
            ("d", Some("D")),
            ("e", Some("E")),
        ],
    )
    .await
}

async fn put_many_applies_after_delete_range<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let test_space = space(1);
    seed_full_values(&storage, test_space, [("a", "A"), ("b", "B"), ("d", "D")]).await?;

    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .delete_range(
            TEST_SPACE,
            KeyRange {
                lower: Bound::Included(key("b")),
                upper: Bound::Excluded(key("d")),
            },
        )
        .await
        .map_err(|error| format!("delete_range failed: {error}"))?;
    write
        .put_many(TEST_SPACE, put_batch([full_put(key("c"), "C")]))
        .await
        .map_err(|error| format!("put_many failed: {error}"))?;
    write
        .commit()
        .await
        .map_err(|error| format!("commit failed: {error}"))?;

    assert_get_entries(
        &storage,
        test_space,
        &[
            ("a", Some("A")),
            ("b", None),
            ("c", Some("C")),
            ("d", Some("D")),
        ],
    )
    .await
}

async fn put_many_overwrites_existing_value<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let test_space = space(1);
    seed_full_values(&storage, test_space, [("a", "A")]).await?;

    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .put_many(TEST_SPACE, put_batch([full_put(key("a"), "B")]))
        .await
        .map_err(|error| format!("put_many overwrite failed: {error}"))?;
    write
        .commit()
        .await
        .map_err(|error| format!("commit failed: {error}"))?;

    assert_get_entries(&storage, test_space, &[("a", Some("B"))]).await
}

async fn scan_range_sees_overwritten_existing_value<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let test_space = space(1);
    seed_full_values(&storage, test_space, [("a", "A")]).await?;

    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .put_many(TEST_SPACE, put_batch([full_put(key("a"), "B")]))
        .await
        .map_err(|error| format!("put_many overwrite failed: {error}"))?;
    write
        .commit()
        .await
        .map_err(|error| format!("commit failed: {error}"))?;

    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let (chunk, _chunk_has_more) = scan_range(
        &read,
        test_space,
        KeyRange {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        },
        BeginScanOptions::default(),
    )
    .await
    .map_err(|error| format!("scan_range failed: {error}"))?.into_parts();

    assert_read_entries(&chunk, &[("a", "B")])
}

async fn scan_range_returns_forward_row_bounded_chunks<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let test_space = space(1);
    seed_full_values(
        &storage,
        test_space,
        [("a", "A"), ("b", "B"), ("c", "C"), ("d", "D"), ("e", "E")],
    )
    .await?;
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let range = KeyRange {
        lower: Bound::Included(key("b")),
        upper: Bound::Excluded(key("e")),
    };

    let mut cursor = read
        .begin_scan(TEST_SPACE, range, BeginScanOptions::default())
        .await
        .map_err(|error| format!("begin scan_range failed: {error}"))?;
    let (first, first_has_more) = cursor
        .next_page(2)
        .await
        .map_err(|error| format!("first scan_range failed: {error}"))?.into_parts();
    assert_read_entries(&first, &[("b", "B"), ("c", "C")])?;
    if !first_has_more {
        return Err("first scan chunk did not report has_more".to_string());
    }

    let (second, second_has_more) = cursor
        .next_page(2)
        .await
        .map_err(|error| format!("second scan_range failed: {error}"))?
        .into_parts();
    assert_read_entries(&second, &[("d", "D")])?;
    if second_has_more {
        return Err("last scan chunk unexpectedly reported has_more".to_string());
    }
    Ok(())
}

async fn scan_range_caps_owned_pages<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .put_many(
            TEST_SPACE,
            put_batch((0..=MAX_SCAN_PAGE_ROWS).map(|index| {
                let index = u32::try_from(index).expect("scan page test index fits u32");
                full_put(key(index.to_be_bytes()), Bytes::from_static(b"v"))
            })),
        )
        .await
        .map_err(|error| format!("put_many failed: {error}"))?;
    write
        .commit()
        .await
        .map_err(|error| format!("commit failed: {error}"))?;

    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let mut cursor = read
        .begin_scan(
            TEST_SPACE,
            full_key_range(),
            BeginScanOptions {
                projection: CoreProjection::KeyOnly,
                ..BeginScanOptions::default()
            },
        )
        .await
        .map_err(|error| format!("begin capped scan failed: {error}"))?;
    let (first, first_has_more) = cursor
        .next_page(usize::MAX)
        .await
        .map_err(|error| format!("first capped scan failed: {error}"))?.into_parts();
    if first.len() != MAX_SCAN_PAGE_ROWS || !first_has_more {
        return Err(format!(
            "oversized scan returned {} rows with has_more={} (expected {} and true)",
            first.len(),
            first_has_more,
            MAX_SCAN_PAGE_ROWS
        ));
    }

    let (tail, tail_has_more) = cursor
        .next_page(usize::MAX)
        .await
        .map_err(|error| format!("tail scan failed: {error}"))?
        .into_parts();
    if tail.len() != 1 || tail_has_more {
        return Err(format!(
            "tail scan returned {} rows with has_more={} (expected 1 and false)",
            tail.len(),
            tail_has_more
        ));
    }
    let expected_tail = key(u32::try_from(MAX_SCAN_PAGE_ROWS)
        .expect("maximum scan page rows fits u32")
        .to_be_bytes());
    if tail[0].key != expected_tail {
        return Err(format!(
            "tail scan returned key {:?}, expected {:?}",
            tail[0].key, expected_tail
        ));
    }
    Ok(())
}

async fn scan_range_honors_bound_variants<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let test_space = space(1);
    seed_full_values(
        &storage,
        test_space,
        [("a", "A"), ("b", "B"), ("c", "C"), ("d", "D")],
    )
    .await?;
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin_read failed: {error}"))?;

    let (included, _included_has_more) = scan_range(
        &read,
        test_space,
        KeyRange {
            lower: Bound::Included(key("b")),
            upper: Bound::Included(key("c")),
        },
        BeginScanOptions::default(),
    )
    .await
    .map_err(|error| format!("included range scan failed: {error}"))?.into_parts();
    assert_read_entries(&included, &[("b", "B"), ("c", "C")])?;

    let (excluded, _excluded_has_more) = scan_range(
        &read,
        test_space,
        KeyRange {
            lower: Bound::Excluded(key("b")),
            upper: Bound::Excluded(key("d")),
        },
        BeginScanOptions::default(),
    )
    .await
    .map_err(|error| format!("excluded range scan failed: {error}"))?.into_parts();
    assert_read_entries(&excluded, &[("c", "C")])
}

async fn scan_range_resume_before_lower_does_not_widen_range<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let test_space = space(1);
    seed_full_values(
        &storage,
        test_space,
        [("a", "A"), ("b", "B"), ("c", "C"), ("d", "D")],
    )
    .await?;
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let (chunk, _chunk_has_more) = scan_range(
        &read,
        test_space,
        KeyRange {
            lower: Bound::Included(key("c")),
            upper: Bound::Excluded(key("e")),
        },
        BeginScanOptions::default(),
    )
    .await
    .map_err(|error| format!("scan_range failed: {error}"))?.into_parts();

    assert_read_entries(&chunk, &[("c", "C"), ("d", "D")])
}

async fn scan_range_orders_raw_byte_keys<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let test_space = space(1);
    seed_full_byte_values(
        &storage,
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
    )
    .await?;

    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let (chunk, _chunk_has_more) = scan_range(
        &read,
        test_space,
        KeyRange {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        },
        BeginScanOptions::default(),
    )
    .await
    .map_err(|error| format!("scan_range failed: {error}"))?.into_parts();

    assert_read_entries_bytes(
        &chunk,
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

async fn scan_range_drains_multi_chunk_limits<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let test_space = space(1);
    seed_full_values(
        &storage,
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
    )
    .await?;
    let read = storage
        .begin_read(ReadOptions::default())
        .await
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
        let mut cursor = read
            .begin_scan(TEST_SPACE, range.clone(), BeginScanOptions::default())
            .await
            .map_err(|error| format!("begin scan limit {limit} failed: {error}"))?;
        loop {
            let (chunk, chunk_has_more) = cursor
                .next_page(limit)
                .await
                .map_err(|error| format!("scan_range limit {limit} failed: {error}"))?.into_parts();
            actual.extend(entries_to_key_values(&chunk));
            if !chunk_has_more {
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

async fn scan_cursor_drains_multi_chunk_limits<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let test_space = space(1);
    seed_full_values(
        &storage,
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
    )
    .await?;
    let read = storage
        .begin_read(ReadOptions::default())
        .await
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
        let mut cursor = read
            .begin_scan(TEST_SPACE, range.clone(), BeginScanOptions::default())
            .await
            .map_err(|error| format!("begin paged scan limit {limit} failed: {error}"))?;
        loop {
            let (result, result_has_more) = cursor
                .next_page(limit)
                .await
                .map_err(|error| format!("paged scan limit {limit} failed: {error}"))?.into_parts();
            actual.extend(entries_to_key_values(&result));
            if !result_has_more {
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

async fn scan_range_empty_range_returns_empty_chunk<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let test_space = space(1);
    seed_full_values(&storage, test_space, [("a", "A"), ("b", "B")]).await?;
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let (chunk, _chunk_has_more) = scan_range(
        &read,
        test_space,
        KeyRange {
            lower: Bound::Included(key("b")),
            upper: Bound::Excluded(key("b")),
        },
        BeginScanOptions::default(),
    )
    .await
    .map_err(|error| format!("scan_range failed: {error}"))?.into_parts();
    if chunk.is_empty() {
        Ok(())
    } else {
        Err(format!("empty range returned entries: {:?}", chunk))
    }
}

async fn commit_is_atomic<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let test_space = space(1);
    let key_a = key("a");
    let key_b = key("b");

    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .put_many(
            TEST_SPACE,
            put_batch([full_put(key_a.clone(), "A"), full_put(key_b.clone(), "B")]),
        )
        .await
        .map_err(|error| format!("put_many failed: {error}"))?;

    let read_before_commit = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin_read before commit failed: {error}"))?;
    let before_commit = read_before_commit
        .get_many_in_space(
            TEST_SPACE,
            &[key_a.clone(), key_b.clone()],
            GetOptions::default(),
        )
        .await
        .map_err(|error| format!("get_many before commit failed: {error}"))?;
    if !before_commit
        .entries_for_requested_keys(&[key_a, key_b])
        .is_empty()
    {
        return Err("uncommitted writes were visible to an independent read".to_string());
    }

    write
        .commit()
        .await
        .map_err(|error| format!("commit failed: {error}"))?;
    assert_get_entries(&storage, test_space, &[("a", Some("A")), ("b", Some("B"))]).await
}

async fn rollback_discards_staged_mutations<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let test_space = space(1);

    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .put_many(TEST_SPACE, put_batch([full_put(key("a"), "A")]))
        .await
        .map_err(|error| format!("put_many failed: {error}"))?;
    write
        .rollback()
        .await
        .map_err(|error| format!("rollback failed: {error}"))?;

    assert_get_entries(&storage, test_space, &[("a", None)]).await
}

async fn rollback_discards_overwrite_and_delete<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let test_space = space(1);
    seed_full_values(&storage, test_space, [("a", "A"), ("b", "B")]).await?;

    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .map_err(|error| format!("begin_write failed: {error}"))?;
    write
        .put_many(TEST_SPACE, put_batch([full_put(key("a"), "A2")]))
        .await
        .map_err(|error| format!("put_many overwrite failed: {error}"))?;
    write
        .delete_many(TEST_SPACE, &[key("b")])
        .await
        .map_err(|error| format!("delete_many failed: {error}"))?;
    write
        .rollback()
        .await
        .map_err(|error| format!("rollback failed: {error}"))?;

    assert_get_entries(&storage, test_space, &[("a", Some("A")), ("b", Some("B"))]).await
}

async fn begin_read_pins_coherent_view<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let test_space = space(1);
    seed_full_values(&storage, test_space, [("a", "A")]).await?;
    let old_read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin_read failed: {error}"))?;

    seed_full_values(&storage, test_space, [("a", "B")]).await?;
    seed_full_values(&storage, test_space, [("a", "C")]).await?;

    let old_keys = [key("a")];
    let old_result = old_read
        .get_many_in_space(TEST_SPACE, &old_keys, GetOptions::default())
        .await
        .map_err(|error| format!("old read get_many failed: {error}"))?;
    assert_read_entries(
        &old_result.entries_for_requested_keys(&old_keys),
        &[("a", "A")],
    )?;

    let (old_scan, _old_scan_has_more) = scan_range(
        &old_read,
        test_space,
        KeyRange {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        },
        BeginScanOptions::default(),
    )
    .await
    .map_err(|error| format!("old read scan_range failed: {error}"))?.into_parts();
    assert_read_entries(&old_scan, &[("a", "A")])?;

    assert_get_entries(&storage, test_space, &[("a", Some("C"))]).await
}

async fn scan_cursor_survives_concurrent_commit_and_restarts_exclusively<F>(
    factory: &F,
) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    seed_full_values(
        &storage,
        TEST_SPACE,
        [("a", "A"), ("b", "B"), ("c", "C"), ("d", "D")],
    )
    .await?;

    let old_read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin old read failed: {error}"))?;
    let full_range = KeyRange {
        lower: Bound::Unbounded,
        upper: Bound::Unbounded,
    };
    let mut old_cursor = old_read
        .begin_scan(TEST_SPACE, full_range.clone(), BeginScanOptions::default())
        .await
        .map_err(|error| format!("begin old cursor failed: {error}"))?;
    let (first, first_has_more) = old_cursor
        .next_page(2)
        .await
        .map_err(|error| format!("old cursor first page failed: {error}"))?.into_parts();
    assert_read_entries(&first, &[("a", "A"), ("b", "B")])?;
    if !first_has_more {
        return Err("old cursor ended before its second page".to_string());
    }

    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .map_err(|error| format!("begin concurrent write failed: {error}"))?;
    write
        .delete_many(TEST_SPACE, &[key("d")])
        .await
        .map_err(|error| format!("stage concurrent delete failed: {error}"))?;
    write
        .put_many(
            TEST_SPACE,
            put_batch([
                full_put(key("c"), Bytes::from_static(b"C2")),
                full_put(key("e"), Bytes::from_static(b"E")),
            ]),
        )
        .await
        .map_err(|error| format!("stage concurrent puts failed: {error}"))?;
    write
        .commit()
        .await
        .map_err(|error| format!("commit concurrent mutation failed: {error}"))?;

    let (second, second_has_more) = old_cursor
        .next_page(2)
        .await
        .map_err(|error| format!("old cursor second page failed: {error}"))?.into_parts();
    assert_read_entries(&second, &[("c", "C"), ("d", "D")])?;
    if second_has_more {
        return Err("old cursor exposed unexpected rows after its snapshot tail".to_string());
    }
    drop(old_cursor);
    drop(old_read);

    let current_read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin current read failed: {error}"))?;
    let mut restarted = current_read
        .begin_scan(
            TEST_SPACE,
            KeyRange {
                lower: Bound::Excluded(key("b")),
                upper: Bound::Unbounded,
            },
            BeginScanOptions::default(),
        )
        .await
        .map_err(|error| format!("begin exclusive restart failed: {error}"))?;
    let (restarted_page, _restarted_page_has_more) = restarted
        .next_page(usize::MAX)
        .await
        .map_err(|error| format!("exclusive restart failed: {error}"))?.into_parts();
    assert_read_entries(&restarted_page, &[("c", "C2"), ("e", "E")])
}

async fn descending_scan_is_explicitly_unsupported<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin read failed: {error}"))?;
    let result = read
        .begin_scan(
            TEST_SPACE,
            KeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            BeginScanOptions {
                order: ScanOrder::Descending,
                ..BeginScanOptions::default()
            },
        )
        .await;
    match result {
        Err(StorageError::Unsupported(crate::storage::Capability::ReverseScan)) => Ok(()),
        Err(error) => Err(format!("descending scan returned wrong error: {error}")),
        Ok(_) => Err("descending scan unexpectedly succeeded".to_string()),
    }
}

async fn unpolled_scan_page_cancellation_keeps_cursor_usable<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    seed_full_values(&storage, TEST_SPACE, [("a", "A"), ("b", "B")]).await?;
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin read failed: {error}"))?;
    let mut cursor = read
        .begin_scan(
            TEST_SPACE,
            KeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            BeginScanOptions::default(),
        )
        .await
        .map_err(|error| format!("begin cursor failed: {error}"))?;
    let unpolled = cursor.next_page(1);
    drop(unpolled);
    let (page, _page_has_more) = cursor
        .next_page(1)
        .await
        .map_err(|error| format!("cursor failed after unpolled cancellation: {error}"))?.into_parts();
    assert_read_entries(&page, &[("a", "A")])
}

async fn invalid_scan_range_fails_closed<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin read failed: {error}"))?;
    match read
        .begin_scan(
            TEST_SPACE,
            KeyRange {
                lower: Bound::Included(key("z")),
                upper: Bound::Excluded(key("a")),
            },
            BeginScanOptions::default(),
        )
        .await
    {
        Err(StorageError::InvalidCursor) => Ok(()),
        Err(error) => Err(format!("invalid range returned wrong error: {error}")),
        Ok(_) => Err("invalid range unexpectedly opened a cursor".to_string()),
    }
}

async fn full_value_and_key_only_are_core<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let test_space = space(1);
    seed_full_values(&storage, test_space, [("a", "A")]).await?;
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin_read failed: {error}"))?;

    let full_keys = [key("a")];
    let full = read
        .get_many_in_space(
            TEST_SPACE,
            &full_keys,
            GetOptions {
                projection: CoreProjection::FullValue,
            },
        )
        .await
        .map_err(|error| format!("FullValue get_many failed: {error}"))?;
    assert_read_entries(&full.entries_for_requested_keys(&full_keys), &[("a", "A")])?;

    let key_only_keys = [key("a")];
    let key_only = read
        .get_many_in_space(
            TEST_SPACE,
            &key_only_keys,
            GetOptions {
                projection: CoreProjection::KeyOnly,
            },
        )
        .await
        .map_err(|error| format!("KeyOnly get_many failed: {error}"))?;
    assert_key_only_entries(
        &key_only.entries_for_requested_keys(&key_only_keys),
        &[key("a")],
    )?;

    let (key_only_scan, _key_only_scan_has_more) = scan_range(
        &read,
        test_space,
        KeyRange {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        },
        BeginScanOptions {
            projection: CoreProjection::KeyOnly,
            ..Default::default()
        },
    )
    .await
    .map_err(|error| format!("KeyOnly scan_range failed: {error}"))?.into_parts();
    assert_key_only_entries(&key_only_scan, &[key("a")])
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

async fn full_value_preserves_opaque_bytes<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let test_space = space(1);
    let opaque_key = Key(Bytes::from_static(b"\0opaque\xff"));
    let opaque_value = Bytes::from_static(b"\0value\xff\x80\n");
    seed_full_byte_values(
        &storage,
        test_space,
        [(opaque_key.0.clone(), opaque_value.clone())],
    )
    .await?;
    let requested = [opaque_key.clone()];
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let result = read
        .get_many_in_space(TEST_SPACE, &requested, GetOptions::default())
        .await
        .map_err(|error| format!("opaque get_many failed: {error}"))?;
    assert_read_entries_bytes(
        &result.entries_for_requested_keys(&requested),
        &[(opaque_key.0, opaque_value)],
    )
}

/// Spaces are physically independent: the same logical key in two spaces
/// must hold independent values, and deletes must not cross spaces.
async fn spaces_do_not_collide<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .map_err(|error| format!("begin write failed: {error}"))?;
    write
        .put_many(
            TEST_SPACE,
            put_batch([full_put(key("k"), Bytes::from_static(b"A"))]),
        )
        .await
        .map_err(|error| format!("put space A failed: {error}"))?;
    write
        .put_many(
            OTHER_SPACE,
            put_batch([full_put(key("k"), Bytes::from_static(b"B"))]),
        )
        .await
        .map_err(|error| format!("put space B failed: {error}"))?;
    write
        .commit()
        .await
        .map_err(|error| format!("commit failed: {error}"))?;

    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin read failed: {error}"))?;
    let a = read
        .get_many_in_space(TEST_SPACE, &[key("k")], GetOptions::default())
        .await
        .map_err(|error| format!("get space A failed: {error}"))?;
    let b = read
        .get_many_in_space(OTHER_SPACE, &[key("k")], GetOptions::default())
        .await
        .map_err(|error| format!("get space B failed: {error}"))?;
    if a.values[0].as_ref() != Some(&ProjectedValue::FullValue(Bytes::from_static(b"A")))
        || b.values[0].as_ref() != Some(&ProjectedValue::FullValue(Bytes::from_static(b"B")))
    {
        return Err("same logical key must hold independent values per space".to_string());
    }
    drop(read);

    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .map_err(|error| format!("begin delete write failed: {error}"))?;
    write
        .delete_many(TEST_SPACE, &[key("k")])
        .await
        .map_err(|error| format!("delete failed: {error}"))?;
    write
        .commit()
        .await
        .map_err(|error| format!("commit failed: {error}"))?;
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin read failed: {error}"))?;
    let a = read
        .get_many_in_space(TEST_SPACE, &[key("k")], GetOptions::default())
        .await
        .map_err(|error| format!("get after delete failed: {error}"))?;
    let b = read
        .get_many_in_space(OTHER_SPACE, &[key("k")], GetOptions::default())
        .await
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
async fn scan_is_space_scoped<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .map_err(|error| format!("begin write failed: {error}"))?;
    for space in [TEST_SPACE, OTHER_SPACE, space(9)] {
        write
            .put_many(
                space,
                put_batch([
                    full_put(key("a"), Bytes::from_static(b"1")),
                    full_put(key("b"), Bytes::from_static(b"2")),
                    full_put(key("c"), Bytes::from_static(b"3")),
                ]),
            )
            .await
            .map_err(|error| format!("seed failed: {error}"))?;
    }
    write
        .commit()
        .await
        .map_err(|error| format!("commit failed: {error}"))?;

    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin read failed: {error}"))?;
    let (result, result_has_more) = scan_range_in_space(
        &read,
        OTHER_SPACE,
        full_key_range(),
        BeginScanOptions::default(),
        MAX_SCAN_PAGE_ROWS,
    )
    .await
    .map_err(|error| format!("scan failed: {error}"))?.into_parts();
    let rows = result
        .iter()
        .map(|entry| entry.key.clone())
        .collect::<Vec<_>>();
    if rows != vec![key("a"), key("b"), key("c")] || result_has_more {
        return Err(format!("scan must observe only its space, got {rows:?}"));
    }

    // Resume past the last row: must report exhaustion, never the
    // neighbouring space's rows.
    let (result, result_has_more) = scan_range_in_space(
        &read,
        OTHER_SPACE,
        KeyRange {
            lower: Bound::Excluded(key("c")),
            upper: Bound::Unbounded,
        },
        BeginScanOptions::default(),
        MAX_SCAN_PAGE_ROWS,
    )
    .await
    .map_err(|error| format!("resume scan failed: {error}"))?
    .into_parts();
    let tail = result
        .iter()
        .map(|entry| entry.key.clone())
        .collect::<Vec<_>>();
    if !tail.is_empty() || result_has_more {
        return Err(format!(
            "resume past the space's last key must be empty, got {tail:?}"
        ));
    }
    Ok(())
}

/// The truncate idiom: an unbounded delete_range clears exactly its space,
/// and the space accepts writes again afterwards.
async fn unbounded_delete_range_truncates_only_target_space<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .map_err(|error| format!("begin write failed: {error}"))?;
    for space in [TEST_SPACE, OTHER_SPACE, space(9)] {
        write
            .put_many(
                space,
                put_batch([
                    full_put(key("a"), Bytes::from_static(b"1")),
                    full_put(key("b"), Bytes::from_static(b"2")),
                ]),
            )
            .await
            .map_err(|error| format!("seed failed: {error}"))?;
    }
    write
        .commit()
        .await
        .map_err(|error| format!("commit failed: {error}"))?;

    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .map_err(|error| format!("begin truncate failed: {error}"))?;
    write
        .delete_range(OTHER_SPACE, full_key_range())
        .await
        .map_err(|error| format!("truncate failed: {error}"))?;
    write
        .commit()
        .await
        .map_err(|error| format!("commit failed: {error}"))?;

    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin read failed: {error}"))?;
    for (space, expected) in [(TEST_SPACE, 2usize), (OTHER_SPACE, 0), (space(9), 2)] {
        let rows = scan_range_in_space(
            &read,
            space,
            full_key_range(),
            BeginScanOptions::default(),
            MAX_SCAN_PAGE_ROWS,
        )
        .await
        .map_err(|error| format!("scan failed: {error}"))?
        .into_parts()
        .0
        .len();
        if rows != expected {
            return Err(format!(
                "truncate must clear only its space: space {space:?} held {rows} rows, expected {expected}"
            ));
        }
    }
    drop(read);

    // The truncated space must accept writes again.
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .map_err(|error| format!("begin rewrite failed: {error}"))?;
    write
        .put_many(
            OTHER_SPACE,
            put_batch([full_put(key("z"), Bytes::from_static(b"9"))]),
        )
        .await
        .map_err(|error| format!("rewrite failed: {error}"))?;
    write
        .commit()
        .await
        .map_err(|error| format!("commit failed: {error}"))?;
    Ok(())
}

/// A never-written space behaves as empty for every read shape.
async fn empty_space_reads_are_empty<F>(factory: &F) -> ConformanceResult
where
    F: StorageFactory,
{
    let storage = open_storage(factory).await;
    let empty = space(0x7777_7777);
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin read failed: {error}"))?;
    let result = read
        .get_many_in_space(empty, &[key("a")], GetOptions::default())
        .await
        .map_err(|error| format!("get failed: {error}"))?;
    if result.values[0].as_ref().is_some() {
        return Err("never-written space must miss".to_string());
    }
    let (scan, scan_has_more) = scan_range_in_space(
        &read,
        empty,
        full_key_range(),
        BeginScanOptions::default(),
        MAX_SCAN_PAGE_ROWS,
    )
    .await
    .map_err(|error| format!("scan failed: {error}"))?
    .into_parts();
    if !scan.is_empty() || scan_has_more {
        return Err("never-written space must scan empty".to_string());
    }
    drop(read);
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .map_err(|error| format!("begin write failed: {error}"))?;
    write
        .delete_range(empty, full_key_range())
        .await
        .map_err(|error| format!("delete_range on empty space failed: {error}"))?;
    write
        .commit()
        .await
        .map_err(|error| format!("commit failed: {error}"))?;
    Ok(())
}

fn full_key_range() -> KeyRange {
    KeyRange {
        lower: Bound::Unbounded,
        upper: Bound::Unbounded,
    }
}

async fn seed_full_values<StorageImpl, I>(
    storage: &StorageImpl,
    _test_space: StorageSpace,
    rows: I,
) -> ConformanceResult
where
    StorageImpl: Storage,
    I: IntoIterator<Item = (&'static str, &'static str)>,
{
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .map_err(|error| format!("seed begin_write failed: {error}"))?;
    write
        .put_many(
            TEST_SPACE,
            put_batch(
                rows.into_iter()
                    .map(|(key_bytes, value_bytes)| full_put(key(key_bytes), value_bytes)),
            ),
        )
        .await
        .map_err(|error| format!("seed put_many failed: {error}"))?;
    write
        .commit()
        .await
        .map_err(|error| format!("seed commit failed: {error}"))?;
    Ok(())
}

async fn seed_full_byte_values<StorageImpl, I>(
    storage: &StorageImpl,
    _test_space: StorageSpace,
    rows: I,
) -> ConformanceResult
where
    StorageImpl: Storage,
    I: IntoIterator<Item = (Bytes, Bytes)>,
{
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .map_err(|error| format!("seed begin_write failed: {error}"))?;
    write
        .put_many(
            TEST_SPACE,
            put_batch(
                rows.into_iter()
                    .map(|(key_bytes, value_bytes)| full_put(key(key_bytes), value_bytes)),
            ),
        )
        .await
        .map_err(|error| format!("seed put_many failed: {error}"))?;
    write
        .commit()
        .await
        .map_err(|error| format!("seed commit failed: {error}"))?;
    Ok(())
}

async fn scan_range<R>(
    read: &R,
    _test_space: StorageSpace,
    range: KeyRange,
    opts: BeginScanOptions,
) -> Result<ScanChunk, StorageError>
where
    R: StorageRead,
{
    scan_range_in_space(read, TEST_SPACE, range, opts, MAX_SCAN_PAGE_ROWS).await
}

async fn scan_range_in_space<R>(
    read: &R,
    space: StorageSpace,
    range: KeyRange,
    opts: BeginScanOptions,
    limit_rows: usize,
) -> Result<ScanChunk, StorageError>
where
    R: StorageRead,
{
    let mut cursor = read.begin_scan(space, range, opts).await?;
    cursor.next_page(limit_rows).await
}

async fn assert_get_entries<StorageImpl>(
    storage: &StorageImpl,
    _test_space: StorageSpace,
    expected: &[(&str, Option<&str>)],
) -> ConformanceResult
where
    StorageImpl: Storage,
{
    let keys = expected
        .iter()
        .map(|(key_bytes, _)| key(*key_bytes))
        .collect::<Vec<_>>();
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| format!("begin_read failed: {error}"))?;
    let result = read
        .get_many_in_space(TEST_SPACE, &keys, GetOptions::default())
        .await
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
