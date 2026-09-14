use super::*;

fn setup(
    bytes: &[u8],
) -> (
    sdk::testing::Harness<CsvPlugin>,
    sdk::testing::Snapshot,
    Document,
) {
    let harness = sdk::testing::Harness::<CsvPlugin>::default();
    let namespace = [0x61; 12];
    let file = sdk::testing::Snapshot {
        file_id: "qa-adapter".into(),
        path: "data.csv".into(),
        bytes: bytes.to_vec(),
        ..Default::default()
    };
    let file = harness
        .parse(&file, sdk::CreateContext::from_namespace_bytes(namespace))
        .unwrap()
        .into_snapshot();
    let document = Document::open_file(
        bytes.to_vec(),
        None,
        IdNamespace::from_namespace_bytes(namespace),
    )
    .unwrap()
    .0;
    (harness, file, document)
}

fn change(record: &RowRecord, cells: serde_json::Value) -> sdk::TypedRowChange {
    let mut row = record.row.clone();
    row.insert("cells", sdk::TypedValue::Jsonb(cells.into()));
    sdk::TypedRowChange {
        schema_key: ROW_SCHEMA_KEY.into(),
        schema_fingerprint: typed_schema(ROW_SCHEMA_KEY).unwrap().2,
        primary_key: record.row_pk.clone(),
        row: Some(row),
        local_ref: None,
        effect: sdk::ChangeEffect::Content,
    }
}

#[test]
fn qa_indexed_batches_match_general_renderer_at_mixed_boundaries() {
    for source in [
        b"a\rb\n\n".as_slice(),
        b"a\nb",
        b"a\rb\nc\r",
        b"\xef\xbb\xbfa,b\nc,d\n",
        b"\"a\",b\nc,d\n",
    ] {
        let (harness, file, document) = setup(source);
        let records = document.row_records().unwrap();
        for values in [["", ""], ["longer", "\n"], ["\u{feff}", "x"], ["\r", "\""]] {
            let updates = records
                .iter()
                .skip(1)
                .take(2)
                .zip(values)
                .map(|(record, value)| change(record, serde_json::json!([value])))
                .collect::<Vec<_>>();
            let core_changes = updates
                .iter()
                .map(|c| RowChange {
                    schema_key: c.schema_key.clone(),
                    row_pk: c.primary_key.clone(),
                    row: c.row.clone(),
                    effect: ChangeEffect::Content,
                })
                .collect::<Vec<_>>();
            let expected = document.rows_changed(&core_changes).unwrap().0;
            let actual = harness
                .serialize_changes(&file, &updates)
                .unwrap()
                .into_snapshot();
            assert_eq!(actual.bytes, expected.bytes(), "{source:?} {values:?}");
            // Re-read adapter state with a fresh harness, then edit the last row.
            let last = expected.row_records().unwrap().pop().unwrap();
            let next = change(&last, serde_json::json!(["followup"]));
            let fresh = sdk::testing::Harness::<CsvPlugin>::default();
            let actual2 = fresh
                .serialize_changes(&actual, std::slice::from_ref(&next))
                .unwrap()
                .into_snapshot();
            let expected2 = expected
                .rows_changed(&[RowChange {
                    schema_key: next.schema_key,
                    row_pk: next.primary_key,
                    row: next.row,
                    effect: ChangeEffect::Content,
                }])
                .unwrap()
                .0;
            assert_eq!(
                actual2.bytes,
                expected2.bytes(),
                "followup {source:?} {values:?}"
            );
        }
    }
}
