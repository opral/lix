#![allow(dead_code)]
#[path = "../src/core.rs"]
mod core;
use core::*;

#[test]
fn qa_roundtrip_sparse_batches_keep_original_offsets_and_last_duplicate() {
    let ns = IdNamespace::from_namespace_bytes([0x51; 12]);
    let initial = Document::open_file(b"a,b\n".repeat(1200), None, ns)
        .unwrap()
        .0;
    let mut records = initial.row_records().unwrap();
    for (ordinal, record) in records.iter_mut().skip(1).enumerate() {
        let id = uuid::Uuid::from_u128(0x12340000000000000000000000000000 + ordinal as u128);
        record.row_pk = vec![lix_schema::Value::Uuid(id)];
        record.row.insert("id", lix_schema::Value::Uuid(id));
    }
    let mut doc = Document::open_rows(records).unwrap().0;
    for round in 0..4 {
        let old = doc.row_records().unwrap();
        let mut expected = old.clone();
        let mut changes = Vec::new();
        for (ordinal, cells) in [
            (1100, vec!["longer value", "second"]),
            (0, vec!["x"]),
            (511, vec!["", ""]),
            (512, vec!["quoted,cell", "\r\n"]),
            (1100, vec!["last duplicate"]),
            (1199, vec!["tail", "end"]),
        ] {
            let record = &old[ordinal + 1];
            let mut row = record.row.clone();
            row.insert(
                "cells",
                lix_schema::Value::Jsonb(
                    serde_json::json!(
                        cells
                            .iter()
                            .map(|s| format!("{s}{round}"))
                            .collect::<Vec<_>>()
                    )
                    .into(),
                ),
            );
            expected[ordinal + 1].row = row.clone();
            changes.push(RowChange {
                schema_key: ROW_SCHEMA_KEY.into(),
                row_pk: record.row_pk.clone(),
                row: Some(row),
                effect: ChangeEffect::Content,
            });
        }
        let (after, edits) = doc.rows_changed(&changes).unwrap();
        assert!(edits.len() <= 5);
        assert!(edits.iter().map(|edit| edit.delete_len).sum::<u64>() < 1024);
        let mut replay = Vec::new();
        let before = doc.bytes();
        let mut cursor = 0;
        for edit in edits {
            let start = edit.offset as usize;
            assert!(start >= cursor);
            replay.extend_from_slice(&before[cursor..start]);
            replay.extend_from_slice(&edit.insert);
            cursor = start + edit.delete_len as usize;
        }
        replay.extend_from_slice(&before[cursor..]);
        assert_eq!(replay, after.bytes());
        let cold = Document::open_rows(expected).unwrap().0;
        assert_eq!(after.bytes(), cold.bytes());
        assert_eq!(after.row_records().unwrap(), cold.row_records().unwrap());
        doc = after;
    }
}

#[test]
fn qa_roundtrip_large_sparse_batch_preserves_coalesced_unchanged_rows() {
    let ns = IdNamespace::from_namespace_bytes([0x51; 12]);
    let source = b"a,b\n\"untouched\",x\r\n".repeat(4_500);
    let before = Document::open_file(source.clone(), None, ns).unwrap().0;
    let records = before.row_records().unwrap();
    let mut changes = Vec::new();
    let mut expected = Vec::new();
    for ordinal in 0..9_000 {
        if ordinal % 2 == 0 {
            let mut row = records[ordinal + 1].row.clone();
            let value = if ordinal % 4 == 0 { "longer" } else { "" };
            row.insert(
                "cells",
                lix_schema::Value::Jsonb(serde_json::json!([value]).into()),
            );
            changes.push(RowChange {
                schema_key: ROW_SCHEMA_KEY.into(),
                row_pk: records[ordinal + 1].row_pk.clone(),
                row: Some(row),
                effect: ChangeEffect::Content,
            });
            expected.extend_from_slice(value.as_bytes());
            expected.push(b'\n');
        } else {
            expected.extend_from_slice(b"\"untouched\",x\r\n");
        }
    }
    let (after, edits) = before.rows_changed(&changes).unwrap();
    assert!(edits.len() < 40);
    assert_eq!(after.bytes(), expected);
    assert_eq!(
        after.identity_checkpoint().1,
        before.identity_checkpoint().1
    );
    let mut replay = Vec::new();
    let mut cursor = 0;
    for edit in edits {
        replay.extend_from_slice(&source[cursor..edit.offset as usize]);
        replay.extend_from_slice(&edit.insert);
        cursor = (edit.offset + edit.delete_len) as usize;
    }
    replay.extend_from_slice(&source[cursor..]);
    assert_eq!(replay, expected);
    assert_eq!(
        Document::open_rows(after.row_records().unwrap())
            .unwrap()
            .0
            .bytes(),
        expected
    );
}
