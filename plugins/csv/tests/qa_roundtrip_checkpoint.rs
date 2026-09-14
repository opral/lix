#![allow(dead_code)]
#[path = "../src/core.rs"]
mod core;
use core::*;

#[test]
fn qa_roundtrip_checkpoint_compact_keys_survive_grouped_edits_and_reordering() {
    let ns = IdNamespace::from_namespace_bytes([0x72; 12]);
    let bytes = b"a,b\r\nc,d\ne,f\rg,h".to_vec();
    let original = Document::open_file(bytes.clone(), None, ns).unwrap().0;
    let (dialect, mut identities) = original.identity_checkpoint();
    for (ordinal, key) in ["01", "0200000000000001", "020000000000000101", "03"]
        .iter()
        .enumerate()
    {
        identities[ordinal].id = uuid::Uuid::from_u128(ordinal as u128 + 15);
        identities[ordinal].order_key = key.to_string();
    }
    let reopened = Document::open_file_with_identities(bytes, dialect, ns, &identities).unwrap();
    assert_eq!(reopened.identity_checkpoint().1, identities);
    let records = reopened.row_records().unwrap();
    let changes = records
        .iter()
        .skip(1)
        .map(|r| {
            let mut row = r.row.clone();
            row.insert(
                "cells",
                lix_schema::Value::Jsonb(serde_json::json!(["long,quoted", ""]).into()),
            );
            RowChange {
                schema_key: ROW_SCHEMA_KEY.into(),
                row_pk: r.row_pk.clone(),
                row: Some(row),
                effect: ChangeEffect::Content,
            }
        })
        .collect::<Vec<_>>();
    let (edited, edits) = reopened.rows_changed(&changes).unwrap();
    assert_eq!(edits.len(), 1);
    assert_eq!(edited.identity_checkpoint().1, identities);
    let again =
        Document::open_file_with_identities(edited.bytes(), dialect, ns, &identities).unwrap();
    assert_eq!(again.row_records().unwrap(), edited.row_records().unwrap());
    let mut change = changes[3].clone();
    change
        .row
        .as_mut()
        .unwrap()
        .insert("order_key", lix_schema::Value::Text("0001".into()));
    let reordered = again.rows_changed(&[change]).unwrap().0;
    let (dialect, ids) = reordered.identity_checkpoint();
    let restored =
        Document::open_file_with_identities(reordered.bytes(), dialect, ns, &ids).unwrap();
    assert_eq!(
        restored.row_records().unwrap(),
        reordered.row_records().unwrap()
    );
}

#[test]
fn qa_roundtrip_checkpoint_rejects_duplicate_ids_and_unsorted_keys() {
    let ns = IdNamespace::from_namespace_bytes([0x72; 12]);
    let original = Document::open_file(b"a\nb\n".to_vec(), None, ns).unwrap().0;
    let (dialect, identities) = original.identity_checkpoint();
    let mut duplicate = identities.clone();
    duplicate[1].id = duplicate[0].id;
    assert!(
        Document::open_file_with_identities(original.bytes(), dialect, ns, &duplicate).is_err()
    );
    let mut unordered = identities;
    unordered.swap(0, 1);
    assert!(
        Document::open_file_with_identities(original.bytes(), dialect, ns, &unordered).is_err()
    );
}
