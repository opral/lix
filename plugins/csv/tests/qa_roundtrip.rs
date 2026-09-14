#![allow(dead_code)]
#[path = "../src/core.rs"]
mod core;
use core::*;

#[test]
fn qa_roundtrip_exhaustive_short_csv() {
    let alphabet = b"a,\"\r\n";
    for len in 0..=7u32 {
        for mut code in 0..alphabet.len().pow(len) {
            let mut bytes = vec![0; len as usize];
            for byte in &mut bytes {
                *byte = alphabet[code % alphabet.len()];
                code /= alphabet.len();
            }
            if let Ok((doc, _)) = Document::open_file(
                bytes.clone(),
                None,
                IdNamespace::from_namespace_bytes([0x61; 12]),
            ) {
                let rows = doc.row_records().unwrap();
                let (restored, _) =
                    Document::open_rows(rows).unwrap_or_else(|err| panic!("{bytes:?}: {err}"));
                assert_eq!(restored.bytes(), bytes, "source {bytes:?}");
            }
        }
    }
}

#[test]
fn qa_roundtrip_empty_row_after_cr_preserves_two_rows() {
    let (before, _) = Document::open_file(
        b"a\rb\n".to_vec(),
        None,
        IdNamespace::from_namespace_bytes([0x61; 12]),
    )
    .unwrap();
    let mut records = before.row_records().unwrap();
    records[2].row.insert(
        "cells",
        lix_schema::Value::Jsonb(serde_json::json!([""]).into()),
    );
    let (cold, _) = Document::open_rows(records.clone()).unwrap();
    let (cold_reopened, _) = Document::open_file(
        cold.bytes(),
        None,
        IdNamespace::from_namespace_bytes([0x63; 12]),
    )
    .unwrap();
    assert_eq!(cold_reopened.row_count(), before.row_count());
    assert_eq!(
        parse_csv_row(&cold_reopened.row_records().unwrap()[records.len() - 1].row)
            .unwrap()
            .cells,
        parse_csv_row(&records[records.len() - 1].row)
            .unwrap()
            .cells
    );
    let change = RowChange {
        schema_key: ROW_SCHEMA_KEY.into(),
        row_pk: records[2].row_pk.clone(),
        row: Some(records[2].row.clone()),
        effect: ChangeEffect::Content,
    };
    let (after, _) = before.rows_changed(&[change]).unwrap();
    let (reopened, _) = Document::open_file(
        after.bytes(),
        None,
        IdNamespace::from_namespace_bytes([0x62; 12]),
    )
    .unwrap();
    assert_eq!(reopened.row_count(), 2);
    assert_eq!(
        parse_csv_row(&reopened.row_records().unwrap()[2].row)
            .unwrap()
            .cells,
        vec![""]
    );
}

#[test]
fn qa_roundtrip_delete_before_empty_lf_row_preserves_rows() {
    let (before, _) = Document::open_file(
        b"a\rb\n\n".to_vec(),
        None,
        IdNamespace::from_namespace_bytes([0x61; 12]),
    )
    .unwrap();
    let records = before.row_records().unwrap();
    let change = RowChange {
        schema_key: ROW_SCHEMA_KEY.into(),
        row_pk: records[2].row_pk.clone(),
        row: None,
        effect: ChangeEffect::Content,
    };
    let (after, _) = before.rows_changed(&[change]).unwrap();
    let (reopened, _) = Document::open_file(
        after.bytes(),
        None,
        IdNamespace::from_namespace_bytes([0x62; 12]),
    )
    .unwrap();
    assert_eq!(reopened.row_count(), 2);
    assert_eq!(
        parse_csv_row(&reopened.row_records().unwrap()[2].row)
            .unwrap()
            .cells,
        vec![""]
    );
}
