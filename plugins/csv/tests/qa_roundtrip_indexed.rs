#![allow(dead_code)]
#[path = "../src/core.rs"]
mod core;
use core::*;

#[test]
fn qa_roundtrip_indexed_rejects_unreadable_wide_row() {
    let bytes = b"a,b\n".to_vec();
    let import = ColdInitialImport::open(bytes.clone(), None).unwrap();
    let index = ArenaRowIndex::decode(&import.arena_state([0x51; 12])).unwrap();
    let mut change = index.row_change(0, bytes).unwrap();
    change.row.as_mut().unwrap().insert(
        "cells",
        lix_schema::Value::Jsonb(serde_json::json!(vec![""; 65_536]).into()),
    );
    assert!(index.update_ordinal(&change).is_err());
    assert!(
        index
            .render_update(0, change.row.as_ref().unwrap())
            .is_err(),
        "indexed renderer accepted a row that the parser cannot reopen"
    );
}

#[test]
fn qa_roundtrip_indexed_layout_matches_general_renderer() {
    let namespace = [0x51; 12];
    for bytes in [
        b"\"a\",b\r\nx,y\nlast".as_slice(),
        b"\xef\xbb\xbf\"a\",b\nx,y\n",
        b"a\"b,c\nd,e\n",
        b"a\nb\r\nc\r",
    ] {
        let import = ColdInitialImport::open(bytes.to_vec(), None).unwrap();
        let index = ArenaRowIndex::decode(&import.arena_state(namespace)).unwrap();
        let doc = Document::open_file(
            bytes.to_vec(),
            None,
            IdNamespace::from_namespace_bytes(namespace),
        )
        .unwrap()
        .0;
        let records = doc.row_records().unwrap();
        for (ordinal, record) in records.iter().skip(1).enumerate() {
            for values in [
                vec![""],
                vec!["x"],
                vec!["x,y", "z\"q"],
                vec!["\u{feff}x", "\n"],
                vec!["a", "b", "c"],
            ] {
                let mut row = record.row.clone();
                row.insert(
                    "cells",
                    lix_schema::Value::Jsonb(serde_json::json!(values).into()),
                );
                let change = RowChange {
                    schema_key: ROW_SCHEMA_KEY.into(),
                    row_pk: record.row_pk.clone(),
                    row: Some(row.clone()),
                    effect: ChangeEffect::Content,
                };
                assert_eq!(index.update_ordinal(&change).unwrap(), Some(ordinal as u32));
                let rendered = index.render_update(ordinal as u32, &row).unwrap();
                let mut patched = Vec::new();
                for (other, record) in records.iter().skip(1).enumerate() {
                    if other == ordinal {
                        patched.extend_from_slice(&rendered);
                    } else {
                        patched.extend_from_slice(
                            &index.render_update(other as u32, &record.row).unwrap(),
                        );
                    }
                }
                if doc.dialect().bom {
                    patched.splice(0..0, [0xef, 0xbb, 0xbf]);
                }
                let general = doc.rows_changed(&[change]).unwrap().0;
                assert_eq!(
                    patched,
                    general.bytes(),
                    "bytes={bytes:?} ordinal={ordinal} values={values:?}"
                );
            }
        }
    }
}

#[test]
fn qa_roundtrip_indexed_noncanonical_identity_and_order_fall_back() {
    let import = ColdInitialImport::open(b"a,b\n".to_vec(), None).unwrap();
    let index = ArenaRowIndex::decode(&import.arena_state([0x51; 12])).unwrap();
    let mut change = index.row_change(0, b"a,b\n".to_vec()).unwrap();
    change
        .row
        .as_mut()
        .unwrap()
        .insert("order_key", lix_schema::Value::Text("01".into()));
    assert_eq!(index.update_ordinal(&change).unwrap(), None);
    let id = uuid::Uuid::from_bytes([0x55; 16]);
    change
        .row
        .as_mut()
        .unwrap()
        .insert("id", lix_schema::Value::Uuid(id));
    change.row_pk = vec![lix_schema::Value::Uuid(id)];
    assert_eq!(index.update_ordinal(&change).unwrap(), None);
}
