#![allow(dead_code)]
#[path = "../src/core.rs"]
mod core;
use core::*;

fn cells(doc: &Document) -> Vec<Vec<String>> {
    doc.row_records()
        .unwrap()
        .iter()
        .skip(1)
        .map(|r| parse_csv_row(&r.row).unwrap().cells)
        .collect()
}

#[test]
fn qa_roundtrip_custom_dialect_edit_matrix() {
    let ns = IdNamespace::from_namespace_bytes([0x65; 12]);
    for quote in [Some(b'\''), None] {
        let dialect = Dialect {
            delimiter: b';',
            quote,
            terminator: Terminator::Lf,
            bom: false,
        };
        for source in [
            "a;b\nc;d\n",
            "a\rb\n\n",
            "a\rb\r\nc\n",
            "a\nb",
            "a\n\u{feff}b\n",
        ] {
            let (doc, _) =
                Document::open_file_with_dialect(source.as_bytes().to_vec(), dialect, ns).unwrap();
            assert_eq!(
                Document::open_rows(doc.row_records().unwrap())
                    .unwrap()
                    .0
                    .bytes(),
                source.as_bytes()
            );
            for ordinal in 1..=doc.row_count() {
                for replacement in ["", "x", "\u{feff}x", "x;y", "x\ny", "x'y", "'x", "x\ry"] {
                    let mut records = doc.row_records().unwrap();
                    records[ordinal].row.insert(
                        "cells",
                        lix_schema::Value::Jsonb(serde_json::json!([replacement]).into()),
                    );
                    let change = RowChange {
                        schema_key: ROW_SCHEMA_KEY.into(),
                        row_pk: records[ordinal].row_pk.clone(),
                        row: Some(records[ordinal].row.clone()),
                        effect: ChangeEffect::Content,
                    };
                    let cold = Document::open_rows(records);
                    let warm = doc.rows_changed(&[change]);
                    assert_eq!(
                        cold.is_ok(),
                        warm.is_ok(),
                        "source={source:?} quote={quote:?} ordinal={ordinal} replacement={replacement:?}: cold={:?} warm={:?}",
                        cold.as_ref().err(),
                        warm.as_ref().err()
                    );
                    if let (Ok((cold, _)), Ok((warm, _))) = (cold, warm) {
                        assert_eq!(cells(&cold), cells(&warm));
                        let reopened = Document::open_file_with_stored_dialect(
                            warm.bytes(),
                            warm.dialect(),
                            ns,
                        )
                        .unwrap();
                        assert_eq!(
                            cells(&reopened.0),
                            cells(&warm),
                            "source={source:?} quote={quote:?} ordinal={ordinal} replacement={replacement:?}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn qa_roundtrip_custom_dialect_file_splice_matrix() {
    let ns = IdNamespace::from_namespace_bytes([0x65; 12]);
    for quote in [Some(b'\''), None] {
        let dialect = Dialect {
            delimiter: b';',
            quote,
            terminator: Terminator::Lf,
            bom: false,
        };
        for source in [
            "a;b\nc;d\n",
            "a\rb\n\n",
            "a\rb\r\nc\n",
            "a\nb",
            "a\n\u{feff}b\n",
            "\u{feff}'a';b\r\n",
        ] {
            let (doc, _) =
                Document::open_file_with_dialect(source.as_bytes().to_vec(), dialect, ns).unwrap();
            for offset in 0..=source.len() {
                for delete in 0..=usize::from(offset < source.len()) {
                    for insert in ["", "'", ";", "\r", "\n", "'\n", "\u{feff}"] {
                        let mut bytes = source.as_bytes().to_vec();
                        bytes.splice(offset..offset + delete, insert.bytes());
                        let cold = Document::open_file_with_dialect(bytes, dialect, ns);
                        let warm = doc.file_changed(
                            &[FileEdit {
                                offset: offset as u64,
                                delete_len: delete as u64,
                                insert: insert.as_bytes(),
                            }],
                            IdNamespace::from_namespace_bytes([0x66; 12]),
                        );
                        let context = format!(
                            "source={source:?} quote={quote:?} offset={offset} delete={delete} insert={insert:?}"
                        );
                        assert_eq!(
                            cold.is_ok(),
                            warm.is_ok(),
                            "{context}: cold={:?} warm={:?}",
                            cold.as_ref().err(),
                            warm.as_ref().err()
                        );
                        if let (Ok((cold, _)), Ok((warm, changes))) = (cold, warm) {
                            assert_eq!(cells(&cold), cells(&warm), "{context}");
                            let mut records = doc.row_records().unwrap();
                            for change in changes {
                                records.retain(|r| {
                                    !(r.schema_key == change.schema_key
                                        && r.row_pk == change.row_pk)
                                });
                                if let Some(row) = change.row {
                                    records.push(RowRecord {
                                        schema_key: change.schema_key,
                                        row_pk: change.row_pk,
                                        row,
                                    });
                                }
                            }
                            let restored = Document::open_rows(records).unwrap();
                            assert_eq!(restored.0.bytes(), warm.bytes(), "{context}");
                        }
                    }
                }
            }
        }
    }
}
