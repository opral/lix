use super::*;

fn open(bytes: &[u8]) -> Document {
    Document::open_file(
        bytes.to_vec(),
        None,
        IdNamespace::from_namespace_bytes([0x72; 12]),
    )
    .unwrap()
    .0
}

#[test]
fn qa_sql_append_unterminated_file_matches_cold_render() {
    let before = open(b"a");
    let mut records = before.row_records().unwrap();
    let mut row = parse_csv_row(&records[1].row).unwrap();
    row.id = uuid::Uuid::from_bytes([2; 16]);
    row.order_key = "ff".into();
    row.cells = vec!["b".into()];
    row.layout = RowLayout::default();
    let typed = csv_typed_row(row.clone()).unwrap();
    let change = RowChange {
        schema_key: ROW_SCHEMA_KEY.into(),
        row_pk: vec![TypedValue::Uuid(row.id)],
        row: Some(typed.clone()),
        effect: ChangeEffect::Content,
    };
    records.push(RowRecord {
        schema_key: change.schema_key.clone(),
        row_pk: change.row_pk.clone(),
        row: typed,
    });
    let warm = before.rows_changed(&[change]).unwrap().0;
    let cold = Document::open_rows(records).unwrap().0;
    assert_eq!(warm.bytes(), cold.bytes());
}

#[test]
fn qa_sql_empty_row_after_cr_survives_reopen() {
    let before = open(b"a\rb\n");
    let mut records = before.row_records().unwrap();
    let mut row = parse_csv_row(&records[2].row).unwrap();
    row.cells = vec![String::new()];
    records[2].row = csv_typed_row(row).unwrap();
    let change = RowChange {
        schema_key: ROW_SCHEMA_KEY.into(),
        row_pk: records[2].row_pk.clone(),
        row: Some(records[2].row.clone()),
        effect: ChangeEffect::Content,
    };
    let warm = before.rows_changed(&[change]).unwrap().0;
    let cold = Document::open_rows(records).unwrap().0;
    for document in [warm, cold] {
        let reopened = open(&document.bytes());
        assert_eq!(reopened.row_count(), 2);
        let rows = reopened.row_records().unwrap();
        assert_eq!(
            parse_csv_row(&rows[2].row).unwrap().cells,
            vec![String::new()]
        );
    }
}

fn assert_mutation(before: &Document, changes: &[RowChange], context: &str) {
    let records = apply_row_changes(before.row_records().unwrap(), changes).unwrap();
    let mut expected = records
        .iter()
        .filter(|r| r.schema_key.as_ref() == ROW_SCHEMA_KEY)
        .map(|r| parse_csv_row(&r.row).unwrap())
        .collect::<Vec<_>>();
    expected.sort_by(|a, b| a.order_key.cmp(&b.order_key).then(a.id.cmp(&b.id)));
    let expected_cells = expected.into_iter().map(|r| r.cells).collect::<Vec<_>>();
    let cold = Document::open_rows(records)
        .unwrap_or_else(|e| panic!("{context}: cold {e}"))
        .0;
    let warm = before
        .rows_changed(changes)
        .unwrap_or_else(|e| panic!("{context}: warm {e}"))
        .0;
    assert_eq!(warm.bytes(), cold.bytes(), "{context}: warm/cold bytes");
    for (kind, document) in [("warm", warm), ("cold", cold)] {
        let reopened = Document::open_file_with_dialect(
            document.bytes(),
            document.dialect(),
            IdNamespace::from_namespace_bytes([0x73; 12]),
        )
        .unwrap()
        .0;
        let cells = reopened
            .row_records()
            .unwrap()
            .into_iter()
            .filter(|r| r.schema_key.as_ref() == ROW_SCHEMA_KEY)
            .map(|r| parse_csv_row(&r.row).unwrap().cells)
            .collect::<Vec<_>>();
        assert_eq!(
            cells, expected_cells,
            "{context}: {kind} cells after reopen"
        );
    }
}

const SQL_CORPUS: &[&[u8]] = &[
    b"a\rb\n\n",
    b"a\r\nb\rc",
    b"a\nb\rc\n",
    b"a\rb\n",
    b"a\nb",
    b"a\n\"\"",
    b"a\rb\n\nlast",
    b"a\r\"\"\n",
];

fn row_change(record: &RowRecord, row: Option<CsvRow>) -> RowChange {
    RowChange {
        schema_key: ROW_SCHEMA_KEY.into(),
        row_pk: record.row_pk.clone(),
        row: row.map(|r| csv_typed_row(r).unwrap()),
        effect: ChangeEffect::Content,
    }
}

#[test]
fn qa_sql_update_matrix_preserves_record_boundaries() {
    for &bytes in SQL_CORPUS {
        let before = open(bytes);
        for record in before.row_records().unwrap().into_iter().skip(1) {
            for value in ["", "plain", "\n", "\r", "\"", ",", "\u{feff}"] {
                let mut row = parse_csv_row(&record.row).unwrap();
                row.cells = vec![value.into()];
                assert_mutation(
                    &before,
                    &[row_change(&record, Some(row))],
                    &format!("update {bytes:?} {value:?}"),
                );
            }
        }
    }
}

#[test]
fn qa_sql_delete_matrix_preserves_record_boundaries() {
    for &bytes in SQL_CORPUS {
        let before = open(bytes);
        for record in before.row_records().unwrap().into_iter().skip(1) {
            assert_mutation(
                &before,
                &[row_change(&record, None)],
                &format!("delete {bytes:?} {:?}", record.row_pk),
            );
        }
    }
}

#[test]
fn qa_sql_insert_matrix_preserves_record_boundaries() {
    for &bytes in SQL_CORPUS {
        let before = open(bytes);
        let seed = before.row_records().unwrap()[1].clone();
        for order in ["01", "70", "ff"] {
            for value in ["", "inserted", "\n"] {
                for ending in [
                    None,
                    Some(None),
                    Some(Some(Terminator::Lf)),
                    Some(Some(Terminator::Cr)),
                ] {
                    let mut row = parse_csv_row(&seed.row).unwrap();
                    row.id = uuid::Uuid::from_bytes([3; 16]);
                    row.order_key = order.into();
                    row.cells = vec![value.into()];
                    row.layout = RowLayout {
                        terminator: ending,
                        ..RowLayout::default()
                    };
                    let change = RowChange {
                        schema_key: ROW_SCHEMA_KEY.into(),
                        row_pk: vec![TypedValue::Uuid(row.id)],
                        row: Some(csv_typed_row(row).unwrap()),
                        effect: ChangeEffect::Content,
                    };
                    assert_mutation(
                        &before,
                        &[change],
                        &format!("insert {bytes:?} {order} {value:?} {ending:?}"),
                    );
                }
            }
        }
    }
}

#[test]
fn qa_sql_reorder_matrix_preserves_record_boundaries() {
    for &bytes in SQL_CORPUS {
        let before = open(bytes);
        for record in before.row_records().unwrap().into_iter().skip(1) {
            for order in ["01", "70", "ff"] {
                let mut row = parse_csv_row(&record.row).unwrap();
                row.order_key = order.into();
                assert_mutation(
                    &before,
                    &[row_change(&record, Some(row))],
                    &format!("reorder {bytes:?} {order}"),
                );
            }
        }
    }
}

#[test]
fn qa_sql_dialect_matrix_preserves_record_boundaries() {
    for &bytes in SQL_CORPUS {
        let before = open(bytes);
        for delimiter in [b',', b'\t', b';'] {
            for quote in [Some(b'"'), Some(b'\'')] {
                for terminator in [Terminator::Lf, Terminator::Cr, Terminator::CrLf] {
                    let dialect = Dialect {
                        delimiter,
                        quote,
                        terminator,
                        bom: false,
                    };
                    let change = RowChange {
                        schema_key: TABLE_SCHEMA_KEY.into(),
                        row_pk: vec![TypedValue::Text(ROOT_ROW_PK.into())],
                        row: Some(table_row(dialect)),
                        effect: ChangeEffect::Content,
                    };
                    assert_mutation(
                        &before,
                        &[change],
                        &format!("dialect {bytes:?} {dialect:?}"),
                    );
                }
            }
        }
    }
}

#[test]
fn qa_sql_layout_ending_matrix_preserves_record_boundaries() {
    for &bytes in SQL_CORPUS {
        let before = open(bytes);
        for record in before.row_records().unwrap().into_iter().skip(1) {
            for ending in [
                None,
                Some(None),
                Some(Some(Terminator::Lf)),
                Some(Some(Terminator::Cr)),
                Some(Some(Terminator::CrLf)),
            ] {
                let mut row = parse_csv_row(&record.row).unwrap();
                row.layout.terminator = ending;
                assert_mutation(
                    &before,
                    &[row_change(&record, Some(row))],
                    &format!("ending {bytes:?} {ending:?}"),
                );
            }
        }
    }
}

#[test]
fn qa_sql_batch_edits_match_single_edit_sequence() {
    for &bytes in SQL_CORPUS {
        let before = open(bytes);
        let records = before.row_records().unwrap();
        let changes = records
            .iter()
            .skip(1)
            .enumerate()
            .map(|(index, record)| {
                let mut row = parse_csv_row(&record.row).unwrap();
                row.cells = vec![if index % 2 == 0 {
                    String::new()
                } else {
                    "changed".into()
                }];
                row_change(record, Some(row))
            })
            .collect::<Vec<_>>();
        assert_mutation(&before, &changes, &format!("batch {bytes:?}"));
    }
}

#[test]
fn qa_sql_reordering_unterminated_row_does_not_override_new_last_ending() {
    let before = open(b"first\nlast");
    let record = before.row_records().unwrap().pop().unwrap();
    let mut row = parse_csv_row(&record.row).unwrap();
    row.order_key = "01".into();
    let change = row_change(&record, Some(row));
    let after = before
        .rows_changed(std::slice::from_ref(&change))
        .unwrap()
        .0;
    assert_eq!(after.bytes(), b"last\nfirst\n");
    assert_mutation(
        &before,
        &[change],
        "move unterminated row before terminated row",
    );
}
