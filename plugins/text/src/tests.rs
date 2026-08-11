use crate::model as lix;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::{Value, json};

use crate::core::{Document, FileEdit, LINE_SCHEMA_KEY, Line};
use crate::{STATE_PAGE_BYTES, decode_identities, decode_identity_manifest, encode_identities};

fn open(bytes: &[u8]) -> (Document, Vec<lix::RowChange>) {
    let (document, changes) =
        Document::open_file(bytes.to_vec(), |ordinal| format!("line-{ordinal}"))
            .expect("Text document should open");
    (
        document,
        changes
            .collect::<Result<Vec<_>, _>>()
            .expect("Text changes should serialize"),
    )
}

fn ids(document: &Document) -> Vec<String> {
    document
        .lines()
        .iter()
        .map(|line| line.id().to_owned())
        .collect()
}

fn records(changes: &[lix::RowChange]) -> Vec<lix::RowRecord> {
    changes
        .iter()
        .filter_map(|change| {
            change.snapshot.as_ref().map(|snapshot| lix::RowRecord {
                schema_key: change.schema_key.clone(),
                row_pk: change.row_pk.clone(),
                snapshot: snapshot.clone(),
            })
        })
        .collect()
}

fn snapshot_with_bytes(line: &Line, bytes: &[u8]) -> Vec<u8> {
    let mut snapshot: Value = serde_json::from_slice(
        &line
            .snapshot_bytes()
            .expect("line snapshot should serialize"),
    )
    .expect("line snapshot should be JSON");
    snapshot["content_base64"] = json!(URL_SAFE_NO_PAD.encode(bytes));
    serde_json::to_vec(&snapshot).expect("edited line snapshot should serialize")
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
            |ordinal| format!("new-{ordinal}"),
        )
        .expect("deleting every line should succeed");
    assert!(after.lines().is_empty());
    assert_eq!(after.bytes(), b"");
    assert_eq!(changes.len(), 2);
    assert!(changes.iter().all(|change| change.snapshot.is_none()));
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
            |ordinal| format!("new-{ordinal}"),
        )
        .expect("localized edit should reconcile");

    assert_eq!(after.bytes(), b"alpha\nBETA\ngamma\n");
    assert_eq!(ids(&after), before_ids);
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].row_pk, ["line-1"]);
    assert!(changes[0].snapshot.is_some());
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
            |ordinal| format!("new-{ordinal}"),
        )
        .expect("a duplicate successor line should reconcile");

    assert_eq!(after.bytes(), b"a\na\n");
    assert_eq!(ids(&after), [before_ids[0].clone(), "new-0".to_owned()]);
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].row_pk, ["new-0"]);
    assert!(changes[0].snapshot.is_some());
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
            |ordinal| format!("new-{ordinal}"),
        )
        .expect("line insertion should reconcile");

    assert_eq!(after.bytes(), b"alpha\nmiddle\nomega\n");
    assert_eq!(
        ids(&after),
        [
            before_ids[0].clone(),
            "new-0".to_owned(),
            before_ids[1].clone()
        ]
    );
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].row_pk, ["new-0"]);
    assert!(changes[0].snapshot.is_some());
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
            |ordinal| format!("created-{ordinal}"),
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
            |ordinal| format!("second-{ordinal}"),
        )
        .expect("second edit should reconcile");

    assert_eq!(ids(&after_edit)[1], inserted_id);
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].row_pk, [inserted_id]);
}

#[test]
fn large_identity_mapping_is_split_into_bounded_pages() {
    let identities = (0..30_000)
        .map(|ordinal| crate::core::LineIdentity {
            id: format!("line-{ordinal:08}"),
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
            |ordinal| format!("new-{ordinal}"),
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
    assert_eq!(changes[0].row_pk, [before_ids[2].clone()]);
    let snapshot: Value = serde_json::from_slice(
        changes[0]
            .snapshot
            .as_ref()
            .expect("moved row should be upserted"),
    )
    .unwrap();
    assert_ne!(snapshot["order_key"], document.lines()[2].order_key());
}

#[test]
fn independent_semantic_line_updates_render_as_independent_exact_byte_edits() {
    let (document, _) = open(b"alpha\nbeta\ngamma\n");
    let alpha = &document.lines()[0];
    let gamma = &document.lines()[2];
    let semantic_changes = [
        lix::RowChange::upsert(
            LINE_SCHEMA_KEY,
            vec![alpha.id().to_owned()],
            snapshot_with_bytes(alpha, b"ALPHA\n"),
        ),
        lix::RowChange::upsert(
            LINE_SCHEMA_KEY,
            vec![gamma.id().to_owned()],
            snapshot_with_bytes(gamma, b"GAMMA\n"),
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
    assert!(Document::open_file(b"text\0binary".to_vec(), |ordinal| ordinal.to_string()).is_err());

    let mut source = vec![b'x'; 8_000];
    source.extend_from_slice(b"\0still-text\n");
    let (document, changes) =
        Document::open_file(source.clone(), |ordinal| format!("line-{ordinal}"))
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
    let malformed = snapshot_with_bytes(alpha, b"alpha\nbeta\n");
    let error = document
        .rows_changed([lix::RowChange::upsert(
            LINE_SCHEMA_KEY,
            vec![alpha.id().to_owned()],
            malformed,
        )])
        .expect_err("one row cannot represent multiple logical text lines");
    assert!(error.contains("embedded LF"));
}
