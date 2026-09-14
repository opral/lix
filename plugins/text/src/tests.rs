use crate::model as lix;
use ::lix::plugin::TypedValue;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

use crate::core::{Document, FileEdit, LINE_SCHEMA_KEY, Line};
use crate::{STATE_PAGE_BYTES, decode_identities, decode_identity_manifest, encode_identities};

fn open(bytes: &[u8]) -> (Document, Vec<lix::RowChange>) {
    let (document, changes) = Document::open_file(bytes.to_vec(), |ordinal| test_id(1, ordinal))
        .expect("Text document should open");
    (
        document,
        changes
            .collect::<Result<Vec<_>, _>>()
            .expect("Text changes should serialize"),
    )
}

fn test_id(namespace: u8, ordinal: u64) -> uuid::Uuid {
    let mut bytes = [0; 16];
    bytes[0] = namespace;
    bytes[8..].copy_from_slice(&ordinal.to_be_bytes());
    uuid::Uuid::from_bytes(bytes)
}

fn ids(document: &Document) -> Vec<uuid::Uuid> {
    document.lines().iter().map(|line| line.id()).collect()
}

fn row_pk(id: uuid::Uuid) -> [TypedValue; 1] {
    [TypedValue::Uuid(id)]
}

fn records(changes: &[lix::RowChange]) -> Vec<lix::RowRecord> {
    changes
        .iter()
        .filter_map(|change| {
            change.row.as_ref().map(|row| lix::RowRecord {
                schema_key: change.schema_key.clone(),
                row_pk: change.row_pk.clone(),
                row: row.clone(),
            })
        })
        .collect()
}

fn row_with_bytes(line: &Line, bytes: &[u8]) -> ::lix::plugin::TypedRow {
    let mut row = line.typed_row().expect("test line should have a UUID id");
    let body = bytes.strip_suffix(b"\n").unwrap_or(bytes);
    let (content, fallback) = match std::str::from_utf8(body) {
        Ok(text) => (TypedValue::Text(text.to_owned()), TypedValue::Null),
        Err(_) => (
            TypedValue::Null,
            TypedValue::Text(URL_SAFE_NO_PAD.encode(body)),
        ),
    };
    row.insert("content", content);
    row.insert("content_base64", fallback);
    row.insert(
        "line_ending",
        TypedValue::Text(if bytes.ends_with(b"\n") { "\n" } else { "" }.to_owned()),
    );
    row
}

fn apply_edits(before: &[u8], edits: &[lix::ByteEdit]) -> Vec<u8> {
    let mut after = Vec::new();
    let mut cursor = 0usize;
    for edit in edits {
        let offset = usize::try_from(edit.offset).expect("test offset fits usize");
        let delete_len = usize::try_from(edit.delete_len).expect("test delete fits usize");
        assert!(offset >= cursor);
        assert!(offset + delete_len <= before.len());
        after.extend_from_slice(&before[cursor..offset]);
        after.extend_from_slice(edit.insert.as_slice());
        cursor = offset + delete_len;
    }
    after.extend_from_slice(&before[cursor..]);
    after
}

#[test]
fn empty_document_has_zero_rows_and_nonempty_to_empty_tombstones_each_line() {
    let (empty, initial) = open(b"");
    assert!(initial.is_empty());
    assert!(empty.lines().is_empty());
    assert_eq!(empty.bytes(), b"");
    let reopened = Document::open_rows(Vec::new()).expect("empty rows should render empty");
    assert_eq!(reopened.bytes(), b"");

    let (nonempty, _) = open(b"one\ntwo\n");
    let (after, changes) = nonempty
        .file_changed(
            &[FileEdit {
                offset: 0,
                delete_len: u64::try_from(nonempty.bytes().len()).unwrap(),
                insert: Vec::new(),
            }],
            |ordinal| test_id(2, ordinal),
        )
        .expect("deleting every line should succeed");
    assert!(after.lines().is_empty());
    assert_eq!(after.bytes(), b"");
    assert_eq!(changes.len(), 2);
    assert!(changes.iter().all(|change| change.row.is_none()));
}

#[test]
fn initial_rows_round_trip_invalid_utf8_and_final_unterminated_line_exactly() {
    let source = [0xff, b'\n', b'a', b'\r', b'\n', 0xfe];
    let (document, changes) = open(&source);
    assert_eq!(document.lines().len(), 3);
    assert_eq!(document.lines()[0].bytes(), &[0xff, b'\n']);
    assert_eq!(document.lines()[1].bytes(), b"a\r\n");
    assert_eq!(document.lines()[2].bytes(), &[0xfe]);

    let reopened = Document::open_rows(records(&changes)).expect("line rows should reopen");
    assert_eq!(reopened.bytes(), source);
    assert_eq!(ids(&reopened), ids(&document));
}

#[test]
fn localized_line_edit_preserves_that_lines_id_and_leaves_unrelated_rows_untouched() {
    let (document, _) = open(b"alpha\nbeta\ngamma\n");
    let before_ids = ids(&document);
    let (after, changes) = document
        .file_changed(
            &[FileEdit {
                offset: 6,
                delete_len: 4,
                insert: b"BETA".to_vec(),
            }],
            |ordinal| test_id(2, ordinal),
        )
        .expect("localized edit should reconcile");

    assert_eq!(after.bytes(), b"alpha\nBETA\ngamma\n");
    assert_eq!(ids(&after), before_ids);
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].row_pk, row_pk(test_id(1, 1)));
    assert!(changes[0].row.is_some());
}

#[test]
fn adding_a_duplicate_line_allocates_a_new_identity() {
    let (document, _) = open(b"a\n");
    let before_ids = ids(&document);
    let (after, changes) = document
        .file_changed(
            &[FileEdit {
                offset: 2,
                delete_len: 0,
                insert: b"a\n".to_vec(),
            }],
            |ordinal| test_id(2, ordinal),
        )
        .expect("a duplicate successor line should reconcile");

    assert_eq!(after.bytes(), b"a\na\n");
    assert_eq!(ids(&after), [before_ids[0].clone(), test_id(2, 0)]);
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].row_pk, row_pk(test_id(2, 0)));
    assert!(changes[0].row.is_some());
}

#[test]
fn line_insertion_adds_one_row_without_rewriting_existing_line_rows() {
    let (document, _) = open(b"alpha\nomega\n");
    let before_ids = ids(&document);
    let (after, changes) = document
        .file_changed(
            &[FileEdit {
                offset: 6,
                delete_len: 0,
                insert: b"middle\n".to_vec(),
            }],
            |ordinal| test_id(2, ordinal),
        )
        .expect("line insertion should reconcile");

    assert_eq!(after.bytes(), b"alpha\nmiddle\nomega\n");
    assert_eq!(
        ids(&after),
        [before_ids[0].clone(), test_id(2, 0), before_ids[1].clone()]
    );
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].row_pk, row_pk(test_id(2, 0)));
    assert!(changes[0].row.is_some());
}

#[test]
fn durable_identities_survive_insert_reopen_and_second_edit() {
    let (document, _) = open(b"alpha\nomega\n");
    let (after_insert, _) = document
        .file_changed(
            &[FileEdit {
                offset: 6,
                delete_len: 0,
                insert: b"middle\n".to_vec(),
            }],
            |ordinal| test_id(3, ordinal),
        )
        .expect("line insertion should reconcile");
    let inserted_id = ids(&after_insert)[1].clone();
    let reopened = Document::open_file_with_identities(
        after_insert.bytes().to_vec(),
        after_insert.identities(),
    )
    .expect("durable identities should reopen");

    let (after_edit, changes) = reopened
        .file_changed(
            &[FileEdit {
                offset: 6,
                delete_len: 6,
                insert: b"MIDDLE".to_vec(),
            }],
            |ordinal| test_id(4, ordinal),
        )
        .expect("second edit should reconcile");

    assert_eq!(ids(&after_edit)[1], inserted_id);
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].row_pk, row_pk(inserted_id));
}

#[test]
fn large_identity_mapping_is_split_into_bounded_pages() {
    let identities = (0..30_000)
        .map(|ordinal| crate::core::LineIdentity {
            id: test_id(5, ordinal),
            order_key: "80".repeat(8),
        })
        .collect::<Vec<_>>();

    let (manifest, pages) = encode_identities(&identities).expect("encode identities");
    let (line_count, page_count) =
        decode_identity_manifest(&manifest).expect("decode identity manifest");
    let decoded = decode_identities(line_count, pages.clone()).expect("decode identities");

    assert!(pages.len() > 1);
    assert!(pages.iter().all(|page| page.len() <= STATE_PAGE_BYTES));
    assert_eq!(page_count as usize, pages.len());
    assert_eq!(decoded, identities);
}

#[test]
fn reorder_preserves_ids_and_updates_only_the_moved_rows_order_key() {
    let (document, _) = open(b"alpha\nbeta\ngamma\n");
    let before_ids = ids(&document);
    let (after, changes) = document
        .file_changed(
            &[FileEdit {
                offset: 0,
                delete_len: u64::try_from(document.bytes().len()).unwrap(),
                insert: b"gamma\nalpha\nbeta\n".to_vec(),
            }],
            |ordinal| test_id(2, ordinal),
        )
        .expect("reorder should reconcile");

    assert_eq!(after.bytes(), b"gamma\nalpha\nbeta\n");
    assert_eq!(
        ids(&after),
        [
            before_ids[2].clone(),
            before_ids[0].clone(),
            before_ids[1].clone()
        ]
    );
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].row_pk, row_pk(before_ids[2]));
    let row = changes[0]
        .row
        .as_ref()
        .expect("moved row should be upserted");
    assert_ne!(
        row.get("order_key"),
        Some(&TypedValue::Text(document.lines()[2].order_key()))
    );
}

#[test]
fn independent_semantic_line_updates_render_as_independent_exact_byte_edits() {
    let (document, _) = open(b"alpha\nbeta\ngamma\n");
    let alpha = &document.lines()[0];
    let gamma = &document.lines()[2];
    let semantic_changes = [
        lix::RowChange::upsert(
            LINE_SCHEMA_KEY,
            vec![TypedValue::Uuid(alpha.id())],
            row_with_bytes(alpha, b"ALPHA\n"),
        ),
        lix::RowChange::upsert(
            LINE_SCHEMA_KEY,
            vec![TypedValue::Uuid(gamma.id())],
            row_with_bytes(gamma, b"GAMMA\n"),
        ),
    ];
    let (after, edits) = document
        .rows_changed(semantic_changes)
        .expect("line row updates should render");

    assert_eq!(after.bytes(), b"ALPHA\nbeta\nGAMMA\n");
    assert_eq!(edits.len(), 2);
    assert_eq!(edits[0].offset, 0);
    assert_eq!(edits[1].offset, 11);
    assert_eq!(apply_edits(document.bytes(), &edits), after.bytes());
}

#[test]
fn text_nul_window_rejects_early_nul_and_allows_nul_after_eight_kib() {
    assert!(
        Document::open_file(b"text\0binary".to_vec(), |ordinal| {
            uuid::Uuid::from_u128(u128::from(ordinal))
        })
        .is_err()
    );

    let mut source = vec![b'x'; 8_000];
    source.extend_from_slice(b"\0still-text\n");
    let (document, changes) = Document::open_file(source.clone(), |ordinal| test_id(1, ordinal))
        .expect("a NUL after Git's scan window remains text");
    let changes = changes
        .collect::<Result<Vec<_>, _>>()
        .expect("late-NUL changes should serialize");
    assert_eq!(document.bytes(), source);
    let reopened = Document::open_rows(records(&changes)).expect("late-NUL row should reopen");
    assert_eq!(reopened.bytes(), source);
}

#[test]
fn semantic_rows_cannot_smuggle_multiple_logical_lines_into_one_row() {
    let (document, _) = open(b"alpha\nbeta\n");
    let alpha = &document.lines()[0];
    let malformed = row_with_bytes(alpha, b"alpha\nbeta\n");
    let error = document
        .rows_changed([lix::RowChange::upsert(
            LINE_SCHEMA_KEY,
            vec![TypedValue::Uuid(alpha.id())],
            malformed,
        )])
        .expect_err("one row cannot represent multiple logical text lines");
    assert!(error.contains("embedded LF"));
}

#[test]
fn text_row_keys_require_native_uuid_values() {
    let (document, _) = open(b"alpha\n");
    let alpha = &document.lines()[0];
    let error = document
        .rows_changed([lix::RowChange::upsert(
            LINE_SCHEMA_KEY,
            vec![TypedValue::Text(alpha.id().to_string())],
            alpha.typed_row().expect("line should produce a typed row"),
        )])
        .expect_err("a textual UUID primary key must not cross the typed boundary");
    assert!(error.contains("UUID primary-key component"));
}

#[test]
fn nonfinal_unterminated_rows_are_rejected_before_identity_state_can_drift() {
    let (document, _) = open(b"a\nb\n");
    let first = &document.lines()[0];
    let change = lix::RowChange::upsert(
        LINE_SCHEMA_KEY,
        row_pk(first.id()).to_vec(),
        row_with_bytes(first, b"a"),
    );
    assert!(
        document
            .rows_changed([change.clone()])
            .unwrap_err()
            .contains("nonfinal")
    );
    let mut rows = records(&open(b"a\nb\n").1);
    rows[0].row = change.row.unwrap();
    assert!(Document::open_rows(rows).unwrap_err().contains("nonfinal"));
    assert_eq!(document.bytes(), b"a\nb\n");

    let last = &document.lines()[1];
    let (after, _) = document
        .rows_changed([lix::RowChange::upsert(
            LINE_SCHEMA_KEY,
            row_pk(last.id()).to_vec(),
            row_with_bytes(last, b"b"),
        )])
        .unwrap();
    assert_eq!(after.bytes(), b"a\nb");
    assert_eq!(
        Document::open_file_with_identities(after.bytes().to_vec(), after.identities()).unwrap(),
        after
    );
}

#[test]
fn duplicate_middle_matching_consumes_each_identity_once() {
    let mut before = b"head\n".to_vec();
    before.extend(b"same\n".repeat(20_000));
    before.extend(b"tail\n");
    let (document, _) = open(&before);
    let mut after = before.clone();
    after[..4].copy_from_slice(b"HEAD");
    let end = after.len();
    after[end - 5..end - 1].copy_from_slice(b"TAIL");
    let (updated, changes) = document
        .file_changed(
            &[FileEdit {
                offset: 0,
                delete_len: before.len() as u64,
                insert: after.clone(),
            }],
            |n| test_id(2, n),
        )
        .unwrap();
    assert_eq!(updated.bytes(), after);
    assert_eq!(ids(&updated), ids(&document));
    assert_eq!(changes.len(), 2);
}

#[test]
fn sequential_end_allocations_keep_order_storage_linear() {
    use crate::core::allocate_order_keys;
    for append in [false, true] {
        let mut key = allocate_order_keys(None, None, 1).unwrap().remove(0);
        let mut total = 0;
        for _ in 0..20_000 {
            let next = if append {
                allocate_order_keys(Some(&key), None, 1)
            } else {
                allocate_order_keys(None, Some(&key), 1)
            }
            .unwrap()
            .remove(0);
            assert!(if append { next > key } else { next < key });
            total += next.to_snapshot_string().len();
            key = next;
        }
        assert!(total <= 34 * 20_000, "end keys must have bounded storage");
    }
}

#[test]
fn concurrent_order_ties_render_deterministically_and_accept_followup_edits() {
    let (document, changes) = open(b"a\nb\n");
    let mut rows = records(&changes);
    let first_order = rows[0].row["order_key"].clone();
    rows[1].row.insert("order_key", first_order);
    let tied = Document::open_rows(rows.clone()).unwrap();
    rows.reverse();
    assert_eq!(Document::open_rows(rows).unwrap(), tied);
    assert_eq!(tied.bytes(), document.bytes());
    let hydrated =
        Document::open_file_with_identities(tied.bytes().to_vec(), tied.identities()).unwrap();
    assert_eq!(hydrated, tied);
    let (_, noop) = tied.file_changed(&[], |n| test_id(2, n)).unwrap();
    assert!(noop.is_empty());
    let (after, mutations) = tied
        .file_changed(
            &[FileEdit {
                offset: 0,
                delete_len: 1,
                insert: b"A".to_vec(),
            }],
            |n| test_id(2, n),
        )
        .unwrap();
    let (replayed, _) = tied.rows_changed(mutations).unwrap();
    assert_eq!(replayed, after);
    assert_eq!(after.bytes(), b"A\nb\n");
}

#[test]
fn sql_content_is_readable_and_edits_preserve_line_endings() {
    let (document, _) = open(b"hello\r\nworld");
    let first = &document.lines()[0];
    let mut row = first.typed_row().unwrap();
    assert_eq!(row["content"], TypedValue::Text("hello\r".to_owned()));
    assert_eq!(row["line_ending"], TypedValue::Text("\n".to_owned()));
    assert_eq!(row["content_base64"], TypedValue::Null);
    row.insert("content", TypedValue::Text("updated\r".to_owned()));
    let (after, edits) = document
        .rows_changed([lix::RowChange::upsert(
            LINE_SCHEMA_KEY,
            row_pk(first.id()).to_vec(),
            row,
        )])
        .unwrap();
    assert_eq!(after.bytes(), b"updated\r\nworld");
    assert_eq!(apply_edits(document.bytes(), &edits), after.bytes());
}

#[test]
fn sql_payload_representation_is_canonical() {
    let (document, _) = open(b"hello\n");
    let first = &document.lines()[0];
    for (content, fallback) in [
        (TypedValue::Null, TypedValue::Null),
        (
            TypedValue::Text("hello".into()),
            TypedValue::Text("_w".into()),
        ),
        (
            TypedValue::Null,
            TypedValue::Text(URL_SAFE_NO_PAD.encode(b"hello")),
        ),
    ] {
        let mut row = first.typed_row().unwrap();
        row.insert("content", content);
        row.insert("content_base64", fallback);
        assert!(
            document
                .rows_changed([lix::RowChange::upsert(
                    LINE_SCHEMA_KEY,
                    row_pk(first.id()).to_vec(),
                    row,
                )])
                .is_err()
        );
    }
}

#[test]
fn distant_structural_sql_edits_do_not_resend_unchanged_lines() {
    let mut source = b"first\n".to_vec();
    source.extend(b"unchanged\n".repeat(10_000));
    source.extend(b"last\n");
    let (document, _) = open(&source);
    let (after, edits) = document
        .rows_changed([
            lix::RowChange::delete(LINE_SCHEMA_KEY, row_pk(document.lines()[1].id()).to_vec()),
            lix::RowChange::delete(
                LINE_SCHEMA_KEY,
                row_pk(document.lines()[10_000].id()).to_vec(),
            ),
        ])
        .unwrap();
    assert_eq!(edits.len(), 2);
    assert!(edits.iter().all(|edit| edit.insert.is_empty()));
    assert_eq!(apply_edits(&source, &edits), after.bytes());
}

#[test]
fn repeated_edits_do_not_retain_one_document_buffer_per_changed_line() {
    let source = b"unchanged line\n".repeat(1_000);
    let (mut document, _) = open(&source);
    for index in 0..100 {
        document = document
            .file_changed(
                &[FileEdit {
                    offset: (index * 15) as u64,
                    delete_len: 1,
                    insert: b"U".to_vec(),
                }],
                |n| test_id(2, n),
            )
            .unwrap()
            .0;
    }
    assert!(document.retained_backing_bytes() <= 3 * source.len());
}

#[test]
fn content_edits_write_no_identity_state_and_deletions_retire_old_pages() {
    #[derive(Default)]
    struct Sink {
        puts: usize,
        deletes: usize,
    }
    impl crate::StateOutput for Sink {
        fn put_state(&mut self, _: &[u8], _: &[u8]) -> ::lix::plugin::Result<()> {
            self.puts += 1;
            Ok(())
        }
        fn delete_state(&mut self, _: &[u8]) -> ::lix::plugin::Result<()> {
            self.deletes += 1;
            Ok(())
        }
    }
    let (document, _) = open(&b"line\n".repeat(50_000));
    let (manifest, pages) = encode_identities(&document.identities()).unwrap();
    assert!(pages.len() > 1);
    let (after, _) = document
        .file_changed(
            &[FileEdit {
                offset: 0,
                delete_len: 1,
                insert: b"L".to_vec(),
            }],
            |n| test_id(2, n),
        )
        .unwrap();
    let mut sink = Sink::default();
    crate::replace_identity_pages(
        Some(manifest.clone()),
        |i| Ok(pages.get(i as usize).cloned()),
        &mut sink,
        &after,
    )
    .unwrap();
    assert_eq!((sink.puts, sink.deletes), (0, 0));
    let (empty, _) = open(b"");
    crate::replace_identity_pages(
        Some(manifest),
        |i| Ok(pages.get(i as usize).cloned()),
        &mut sink,
        &empty,
    )
    .unwrap();
    assert_eq!((sink.puts, sink.deletes), (1, pages.len()));
}

#[test]
fn randomized_byte_edits_replay_rows_render_and_hydrate_losslessly() {
    let mut seed = 0x5eed_u64;
    let mut next = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed as usize
    };
    let alphabet = [b'a', b'b', b'\r', b'\n', 0xff, 0xfe];
    for _ in 0..10_000 {
        let source = (0..next() % 80)
            .map(|_| alphabet[next() % alphabet.len()])
            .collect::<Vec<_>>();
        let (before, _) = open(&source);
        let start = next() % (source.len() + 1);
        let delete = next() % (source.len() - start + 1);
        let insert = (0..next() % 20)
            .map(|_| alphabet[next() % alphabet.len()])
            .collect::<Vec<_>>();
        let mut expected = source.clone();
        expected.splice(start..start + delete, insert.clone());
        let (after, changes) = before
            .file_changed(
                &[FileEdit {
                    offset: start as u64,
                    delete_len: delete as u64,
                    insert,
                }],
                |n| test_id(2, n),
            )
            .unwrap();
        assert_eq!(after.bytes(), expected);
        let (replayed, edits) = before.rows_changed(changes).unwrap();
        assert_eq!(replayed, after);
        assert_eq!(apply_edits(&source, &edits), expected);
        assert_eq!(
            Document::open_file_with_identities(expected, after.identities()).unwrap(),
            after
        );
    }
}

#[test]
#[ignore = "manual scaling probe; run with plugin_text opt-level=3 and --nocapture"]
fn text_core_scaling_probe() {
    use std::time::Instant;
    fn median(mut run: impl FnMut()) -> f64 {
        let mut samples = (0..5)
            .map(|_| {
                let start = Instant::now();
                run();
                start.elapsed().as_secs_f64() * 1000.0
            })
            .collect::<Vec<_>>();
        samples.sort_by(f64::total_cmp);
        samples[2]
    }
    println!(
        "lines,bytes,open_ms,file_edit_ms,row_edit_ms,duplicate_ms,sparse_ms,sparse_insert_bytes"
    );
    for count in [1_000, 10_000, 100_000] {
        let source = (0..count)
            .map(|n| format!("line {n:08} with example content\n"))
            .collect::<String>()
            .into_bytes();
        let (document, _) = open(&source);
        let open_ms = median(|| {
            std::hint::black_box(open(&source));
        });
        let splice = FileEdit {
            offset: (source.len() / 2) as u64,
            delete_len: 1,
            insert: b"X".to_vec(),
        };
        let file_ms = median(|| {
            std::hint::black_box(
                document
                    .file_changed(std::slice::from_ref(&splice), |n| test_id(2, n))
                    .unwrap(),
            );
        });
        let line = &document.lines()[count / 2];
        let change = lix::RowChange::upsert(
            LINE_SCHEMA_KEY,
            row_pk(line.id()).to_vec(),
            row_with_bytes(line, b"changed\n"),
        );
        let row_ms = median(|| {
            std::hint::black_box(document.rows_changed([change.clone()]).unwrap());
        });
        let mut duplicate = b"head\n".to_vec();
        duplicate.extend(b"same\n".repeat(count));
        duplicate.extend(b"tail\n");
        let (dup_document, _) = open(&duplicate);
        duplicate[..4].copy_from_slice(b"HEAD");
        let len = duplicate.len();
        duplicate[len - 5..len - 1].copy_from_slice(b"TAIL");
        let dup_splice = FileEdit {
            offset: 0,
            delete_len: duplicate.len() as u64,
            insert: duplicate,
        };
        let duplicate_ms = median(|| {
            std::hint::black_box(
                dup_document
                    .file_changed(std::slice::from_ref(&dup_splice), |n| test_id(2, n))
                    .unwrap(),
            );
        });
        let deletes = [
            lix::RowChange::delete(LINE_SCHEMA_KEY, row_pk(document.lines()[1].id()).to_vec()),
            lix::RowChange::delete(
                LINE_SCHEMA_KEY,
                row_pk(document.lines()[count - 2].id()).to_vec(),
            ),
        ];
        let mut inserted = 0;
        let sparse_ms = median(|| {
            let (_, edits) = document.rows_changed(deletes.clone()).unwrap();
            inserted = edits.iter().map(|edit| edit.insert.len()).sum::<usize>();
            assert_eq!(edits.len(), 2);
        });
        println!(
            "{count},{},{open_ms:.3},{file_ms:.3},{row_ms:.3},{duplicate_ms:.3},{sparse_ms:.3},{inserted}",
            source.len()
        );
    }
}
