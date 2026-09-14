#![allow(dead_code)]
#[path = "../src/core.rs"]
mod core;
use core::*;

#[test]
fn qa_roundtrip_more_than_u16_distinct_uuid_namespaces() {
    let ns = IdNamespace::from_namespace_bytes([0x72; 12]);
    let original = Document::open_file(b"x\n".repeat(65_537), None, ns)
        .unwrap()
        .0;
    let (dialect, mut identities) = original.identity_checkpoint();
    for (ordinal, identity) in identities.iter_mut().enumerate() {
        identity.id = uuid::Uuid::from_u128((ordinal as u128 + 1) << 32);
    }
    let restored = Document::open_file_with_identities(original.bytes(), dialect, ns, &identities)
        .expect("arbitrary UUID identities must not exhaust compact namespace encoding");
    assert_eq!(restored.identity_checkpoint().1, identities);
    let records = restored.row_records().unwrap();
    let cold = Document::open_rows(records.clone()).unwrap().0;
    assert_eq!(cold.row_records().unwrap(), records);
    let id = uuid::Uuid::from_u128(1_000_000u128 << 32);
    let mut row = records.last().unwrap().row.clone();
    row.insert("id", lix_schema::Value::Uuid(id));
    row.insert("order_key", lix_schema::Value::Text("ff".into()));
    let inserted = restored
        .rows_changed(&[RowChange {
            schema_key: ROW_SCHEMA_KEY.into(),
            row_pk: vec![lix_schema::Value::Uuid(id)],
            row: Some(row),
            effect: ChangeEffect::Content,
        }])
        .unwrap()
        .0;
    assert_eq!(inserted.row_count(), restored.row_count() + 1);
    assert!(
        inserted
            .identity_checkpoint()
            .1
            .iter()
            .any(|identity| identity.id == id)
    );
    let file_inserted = restored
        .file_changed(
            &[FileEdit {
                offset: restored.byte_len() as u64,
                delete_len: 0,
                insert: b"y\n",
            }],
            IdNamespace::from_namespace_bytes([0x99; 12]),
        )
        .unwrap()
        .0;
    assert_eq!(file_inserted.row_count(), restored.row_count() + 1);
    assert!(file_inserted.bytes().ends_with(b"y\n"));
}
