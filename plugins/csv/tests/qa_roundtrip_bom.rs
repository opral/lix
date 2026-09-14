#![allow(dead_code)]
#[path = "../src/core.rs"]
mod core;
use core::*;

#[test]
fn qa_roundtrip_edit_leading_bom_cell() {
    let (before, _) = Document::open_file(
        b"alpha,beta\n".to_vec(),
        None,
        IdNamespace::from_namespace_bytes([0x61; 12]),
    )
    .unwrap();
    let mut records = before.row_records().unwrap();
    records[1].row.insert(
        "cells",
        lix_schema::Value::Jsonb(serde_json::json!(["\u{feff}alpha", "beta"]).into()),
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
        row_pk: records[1].row_pk.clone(),
        row: Some(records[1].row.clone()),
        effect: ChangeEffect::Content,
    };
    let (after, _) = before.rows_changed(&[change]).unwrap();
    let (reopened, _) = Document::open_file(
        after.bytes(),
        None,
        IdNamespace::from_namespace_bytes([0x62; 12]),
    )
    .unwrap();
    assert_eq!(
        parse_csv_row(&reopened.row_records().unwrap()[1].row)
            .unwrap()
            .cells,
        vec!["\u{feff}alpha", "beta"]
    );
}

#[test]
fn qa_roundtrip_nonfirst_bom_cell_stays_unquoted() {
    let bytes = "first,row\n\u{feff}alpha,beta\n".as_bytes();
    let (before, _) = Document::open_file(
        bytes.to_vec(),
        None,
        IdNamespace::from_namespace_bytes([0x61; 12]),
    )
    .unwrap();
    let (restored, _) = Document::open_rows(before.row_records().unwrap()).unwrap();
    assert_eq!(restored.bytes(), bytes);
}


#[test]
fn qa_roundtrip_delete_exposing_bom_cell() {
    let (before, _) = Document::open_file("first\n\u{feff}alpha\n".as_bytes().to_vec(), None, IdNamespace::from_namespace_bytes([0x61;12])).unwrap();
    let records = before.row_records().unwrap();
    let change = RowChange { schema_key: ROW_SCHEMA_KEY.into(), row_pk: records[1].row_pk.clone(), row: None, effect: ChangeEffect::Content };
    let (after, _) = before.rows_changed(&[change]).unwrap();
    let (reopened, _) = Document::open_file(after.bytes(), None, IdNamespace::from_namespace_bytes([0x62;12])).unwrap();
    assert_eq!(parse_csv_row(&reopened.row_records().unwrap()[1].row).unwrap().cells, vec!["\u{feff}alpha"]);
}

#[test]
fn qa_roundtrip_reorder_exposing_bom_cell() {
    let (before, _) = Document::open_file("first\n\u{feff}alpha\n".as_bytes().to_vec(), None, IdNamespace::from_namespace_bytes([0x61;12])).unwrap();
    let mut records = before.row_records().unwrap();
    records[2].row.insert("order_key", lix_schema::Value::Text("01".into()));
    let change = RowChange { schema_key: ROW_SCHEMA_KEY.into(), row_pk: records[2].row_pk.clone(), row: Some(records[2].row.clone()), effect: ChangeEffect::Content };
    let (after, _) = before.rows_changed(&[change]).unwrap();
    let (reopened, _) = Document::open_file(after.bytes(), None, IdNamespace::from_namespace_bytes([0x62;12])).unwrap();
    assert_eq!(parse_csv_row(&reopened.row_records().unwrap()[1].row).unwrap().cells, vec!["\u{feff}alpha"]);
}
