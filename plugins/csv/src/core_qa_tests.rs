use super::*;

fn open(bytes: &[u8]) -> Document {
    Document::open_file(
        bytes.to_vec(),
        None,
        IdNamespace::from_namespace_bytes([0x71; 12]),
    )
    .unwrap()
    .0
}

fn assert_reconstructs(document: &Document) {
    let records = document.row_records().unwrap();
    let reconstructed = Document::open_rows(records.clone()).unwrap().0;
    assert_eq!(reconstructed.bytes(), document.bytes());
    assert_eq!(reconstructed.row_records().unwrap(), records);
}

#[test]
fn qa_roundtrip_csv_syntax_corpus() {
    for bytes in [
        b"".as_slice(),
        b"\n",
        b"\r\n",
        b"\r",
        b",",
        b",,\n",
        b"\"\"",
        b"a\nb\r\nc\rd",
        b"\"a\",\"b\"\r\n\"x,y\",\"x\"\"y\"\n",
        "α,雪\n,\n\"line\r\nline\"".as_bytes(),
        b"\xef\xbb\xbfplain,b\n",
    ] {
        assert_reconstructs(&open(bytes));
    }
}

#[test]
fn qa_quoted_field_can_be_changed_to_require_quotes() {
    let before = open(b"\"a\",b\n");
    let mut records = before.row_records().unwrap();
    let mut row = parse_csv_row(&records[1].row).unwrap();
    row.cells[0] = "a,b".to_owned();
    records[1].row = csv_typed_row(row).unwrap();
    let change = RowChange {
        schema_key: ROW_SCHEMA_KEY.into(),
        row_pk: records[1].row_pk.clone(),
        row: Some(records[1].row.clone()),
        effect: ChangeEffect::Content,
    };
    let after = before.rows_changed(&[change]).unwrap().0;
    assert_eq!(after.bytes(), b"\"a,b\",b\n");
    assert_reconstructs(&after);
}

#[test]
fn qa_single_byte_splices_match_full_parse() {
    let corpus: &[&[u8]] = &[
        b"a,b\nc,d\ne,f\n",
        b"a\nb\nc\"\nd\n",
        b"a,b\r\nc,d\r\ne,f\r\n",
        b"\"a\",b\n\"c\",d\n\"e\",f",
        b"a\nb\rc\r\nd",
        b"",
        b"\n\n\n",
    ];
    for &bytes in corpus {
        let before = open(bytes);
        for offset in 0..=bytes.len() {
            for delete_len in 0..=usize::from(offset < bytes.len()) {
                for insert in [
                    b"".as_slice(),
                    b"\"",
                    b"\n",
                    b"\r",
                    b",",
                    b"x",
                    b"\"\n",
                    b"\n\"",
                ] {
                    let mut expected = bytes.to_vec();
                    expected.splice(offset..offset + delete_len, insert.iter().copied());
                    let cold = Document::open_file(
                        expected.clone(),
                        None,
                        IdNamespace::from_namespace_bytes([0x72; 12]),
                    );
                    let hot = before.file_changed(
                        &[FileEdit {
                            offset: offset as u64,
                            delete_len: delete_len as u64,
                            insert,
                        }],
                        IdNamespace::from_namespace_bytes([0x73; 12]),
                    );
                    let context = format!(
                        "before={bytes:?}, offset={offset}, delete={delete_len}, insert={insert:?}"
                    );
                    assert_eq!(
                        hot.is_ok(),
                        cold.is_ok(),
                        "{context}; hot={:?}, cold={:?}",
                        hot.as_ref().err(),
                        cold.as_ref().err()
                    );
                    if let (Ok((hot, changes)), Ok((cold, _))) = (hot, cold) {
                        assert_eq!(hot.bytes(), expected, "{context}");
                        let cells = |doc: &Document| {
                            doc.row_records()
                                .unwrap()
                                .into_iter()
                                .skip(1)
                                .map(|r| parse_csv_row(&r.row).unwrap().cells)
                                .collect::<Vec<_>>()
                        };
                        assert_eq!(cells(&hot), cells(&cold), "{context}");
                        // Byte preservation for permissive literal quotes is covered separately.
                        let reconstructed =
                            Document::open_rows(hot.row_records().unwrap()).unwrap().0;
                        assert_eq!(cells(&reconstructed), cells(&cold), "{context}");
                        let persisted =
                            apply_row_changes(before.row_records().unwrap(), &changes).unwrap();
                        let replayed = Document::open_rows(persisted).unwrap().0;
                        assert_eq!(cells(&replayed), cells(&cold), "{context}");
                        assert_eq!(
                            replayed.row_records().unwrap(),
                            hot.row_records().unwrap(),
                            "{context}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn qa_edit_final_single_cell_to_empty_preserves_row_on_reopen() {
    let before = open(b"first\nlast");
    let record = before.row_records().unwrap().pop().unwrap();
    let mut row = parse_csv_row(&record.row).unwrap();
    row.cells[0].clear();
    let change = RowChange {
        schema_key: ROW_SCHEMA_KEY.into(),
        row_pk: record.row_pk,
        row: Some(csv_typed_row(row).unwrap()),
        effect: ChangeEffect::Content,
    };
    let after = before.rows_changed(&[change]).unwrap().0;
    let reopened = open(&after.bytes());
    assert_eq!(reopened.row_count(), 2, "bytes={:?}", after.bytes());
    let row = reopened.row_records().unwrap().pop().unwrap();
    assert_eq!(parse_csv_row(&row.row).unwrap().cells, [""]);
}

#[test]
fn qa_splice_quoted_field_across_existing_rows() {
    let before = open(b"a\nb\nc\"\nd\n");
    let after = before
        .file_changed(
            &[FileEdit {
                offset: 0,
                delete_len: 0,
                insert: b"\"",
            }],
            IdNamespace::from_namespace_bytes([0x74; 12]),
        )
        .unwrap()
        .0;
    assert_eq!(after.row_count(), 2);
    assert_reconstructs(&after);
}

#[test]
fn qa_unquoted_literal_quote_roundtrip() {
    assert_reconstructs(&open(b"a\"b,c\n"));
}

#[test]
fn qa_rejects_nul_before_constructing_unreadable_rows() {
    assert!(
        Document::open_file(
            b"a\0b,c\n".to_vec(),
            None,
            IdNamespace::from_namespace_bytes([0x75; 12])
        )
        .is_err()
    );
}

#[test]
fn qa_inserting_lf_after_cr_does_not_add_phantom_row() {
    let before = open(b"a\nb\rc\r\nd");
    let after = before
        .file_changed(
            &[FileEdit {
                offset: 4,
                delete_len: 0,
                insert: b"\n",
            }],
            IdNamespace::from_namespace_bytes([0x76; 12]),
        )
        .unwrap()
        .0;
    assert_eq!(after.bytes(), b"a\nb\r\nc\r\nd");
    assert_eq!(after.row_count(), 4);
    assert_reconstructs(&after);
}

#[test]
fn qa_cell_edit_matrix_preserves_values_and_emitted_bytes() {
    for bytes in [
        b"\"a\",b\r\nc,d".as_slice(),
        b"a\"b,c\nlast",
        b"\"a,b\",\"x\"\"y\"\n",
        b"a",
        b"\"\"",
        b"first\nlast",
    ] {
        let before = open(bytes);
        for record in before.row_records().unwrap().into_iter().skip(1) {
            let original = parse_csv_row(&record.row).unwrap();
            for index in 0..original.cells.len() {
                for replacement in [
                    "",
                    "normal",
                    "comma,value",
                    "\"leading",
                    "mid\"quote",
                    "line\r\nline",
                    "雪",
                    "\t",
                ] {
                    let mut row = original.clone();
                    row.cells[index] = replacement.to_owned();
                    let expected_cells = row.cells.clone();
                    let change = RowChange {
                        schema_key: ROW_SCHEMA_KEY.into(),
                        row_pk: record.row_pk.clone(),
                        row: Some(csv_typed_row(row).unwrap()),
                        effect: ChangeEffect::Content,
                    };
                    let cold = Document::open_rows(
                        apply_row_changes(
                            before.row_records().unwrap(),
                            std::slice::from_ref(&change),
                        )
                        .unwrap(),
                    )
                    .unwrap()
                    .0;
                    let (after, edits) = before.rows_changed(&[change]).unwrap();
                    assert_eq!(cold.bytes(), after.bytes());
                    assert_eq!(cold.row_records().unwrap(), after.row_records().unwrap());
                    let mut replayed_bytes = before.bytes();
                    for edit in edits.iter().rev() {
                        replayed_bytes.splice(
                            edit.offset as usize..(edit.offset + edit.delete_len) as usize,
                            edit.insert.iter().copied(),
                        );
                    }
                    assert_eq!(replayed_bytes, after.bytes());
                    let reopened = open(&after.bytes());
                    assert_eq!(reopened.row_count(), before.row_count());
                    let actual = after
                        .row_records()
                        .unwrap()
                        .into_iter()
                        .find(|r| r.row_pk == record.row_pk)
                        .unwrap();
                    assert_eq!(parse_csv_row(&actual.row).unwrap().cells, expected_cells);
                    assert_reconstructs(&after);
                }
            }
        }
    }
}

#[test]
fn qa_literal_quote_layout_survives_cold_initial_import() {
    let bytes = b"a\"b,c,d\"e\nlast\n";
    let expected = open(bytes).row_records().unwrap();
    let mut cold = ColdInitialImport::open(bytes.to_vec(), None).unwrap();
    let mut records = vec![RowRecord {
        schema_key: TABLE_SCHEMA_KEY.into(),
        row_pk: vec![TypedValue::Text(ROOT_ROW_PK.to_owned())],
        row: cold.table_change().row.unwrap(),
    }];
    for record in expected.iter().skip(1) {
        let id = parse_csv_row(&record.row).unwrap().id;
        let (_, row) = cold.next_row(id).unwrap().unwrap();
        records.push(RowRecord {
            schema_key: ROW_SCHEMA_KEY.into(),
            row_pk: record.row_pk.clone(),
            row,
        });
    }
    assert_eq!(records, expected);
    assert_eq!(Document::open_rows(records).unwrap().0.bytes(), bytes);
}

#[test]
fn qa_truncating_cells_normalizes_stale_quote_layout() {
    for bytes in [b"\"a\",b,\"c\"\n".as_slice(), b"a\"b,c,d\"e\n"] {
        let before = open(bytes);
        let record = before.row_records().unwrap().pop().unwrap();
        let mut row = parse_csv_row(&record.row).unwrap();
        row.cells.truncate(1);
        let change = RowChange {
            schema_key: ROW_SCHEMA_KEY.into(),
            row_pk: record.row_pk,
            row: Some(csv_typed_row(row.clone()).unwrap()),
            effect: ChangeEffect::Content,
        };
        let cold = Document::open_rows(
            apply_row_changes(before.row_records().unwrap(), std::slice::from_ref(&change))
                .unwrap(),
        )
        .unwrap()
        .0;
        let after = before.rows_changed(&[change]).unwrap().0;
        assert_eq!(after.bytes(), cold.bytes());
        assert_eq!(
            parse_csv_row(&after.row_records().unwrap()[1].row)
                .unwrap()
                .cells,
            row.cells
        );
        assert_reconstructs(&after);
    }
}

#[test]
fn qa_two_splice_matrix_matches_cold_parse_and_persisted_rows() {
    let bytes = b"a,b\r\nc,d\re,f\nlast";
    let before = open(bytes);
    for left in 0..bytes.len() {
        for right in left + 1..=bytes.len() {
            for (a, b) in [
                (b"\"".as_slice(), b"\"".as_slice()),
                (b"\n", b"\r"),
                (b"", b""),
                (b"x", b","),
            ] {
                let edits = [
                    FileEdit {
                        offset: left as u64,
                        delete_len: 1,
                        insert: a,
                    },
                    FileEdit {
                        offset: right as u64,
                        delete_len: u64::from(right < bytes.len()),
                        insert: b,
                    },
                ];
                let mut expected = bytes.to_vec();
                for edit in edits.iter().rev() {
                    expected.splice(
                        edit.offset as usize..(edit.offset + edit.delete_len) as usize,
                        edit.insert.iter().copied(),
                    );
                }
                let cold = Document::open_file(
                    expected.clone(),
                    None,
                    IdNamespace::from_namespace_bytes([0x78; 12]),
                );
                let hot =
                    before.file_changed(&edits, IdNamespace::from_namespace_bytes([0x79; 12]));
                assert_eq!(
                    hot.is_ok(),
                    cold.is_ok(),
                    "left={left} right={right} a={a:?} b={b:?}; hot={:?} cold={:?}",
                    hot.as_ref().err(),
                    cold.as_ref().err()
                );
                if let (Ok((hot, changes)), Ok((cold, _))) = (hot, cold) {
                    let cells = |doc: &Document| {
                        doc.row_records()
                            .unwrap()
                            .into_iter()
                            .skip(1)
                            .map(|r| parse_csv_row(&r.row).unwrap().cells)
                            .collect::<Vec<_>>()
                    };
                    assert_eq!(cells(&hot), cells(&cold));
                    assert_reconstructs(&hot);
                    let replayed = Document::open_rows(
                        apply_row_changes(before.row_records().unwrap(), &changes).unwrap(),
                    )
                    .unwrap()
                    .0;
                    assert_eq!(replayed.row_records().unwrap(), hot.row_records().unwrap());
                    assert_eq!(replayed.bytes(), expected);
                }
            }
        }
    }
}

#[test]
fn qa_reorder_unterminated_final_row_preserves_contents() {
    let before = open(b"first\nlast");
    let record = before.row_records().unwrap().pop().unwrap();
    let mut row = parse_csv_row(&record.row).unwrap();
    row.order_key = "01".to_owned();
    let change = RowChange {
        schema_key: ROW_SCHEMA_KEY.into(),
        row_pk: record.row_pk,
        row: Some(csv_typed_row(row).unwrap()),
        effect: ChangeEffect::Content,
    };
    let cold = Document::open_rows(
        apply_row_changes(before.row_records().unwrap(), std::slice::from_ref(&change)).unwrap(),
    )
    .unwrap()
    .0;
    let after = before.rows_changed(&[change]).unwrap().0;
    assert_eq!(after.bytes(), b"last\nfirst");
    assert_eq!(cold.bytes(), after.bytes());
    assert_reconstructs(&after);
}

#[test]
fn qa_utf8_bom_is_preserved_outside_first_quoted_cell() {
    for bytes in [
        b"\xef\xbb\xbf\"a,b\",c\r\nlast,row".as_slice(),
        b"\xef\xbb\xbfplain,c\n",
        b"\xef\xbb\xbf",
    ] {
        let before = open(bytes);
        assert_reconstructs(&before);
        if before.row_count() > 0 {
            let record = before.row_records().unwrap().remove(1);
            let mut row = parse_csv_row(&record.row).unwrap();
            assert_eq!(
                row.cells[0],
                if bytes.get(3) == Some(&b'"') {
                    "a,b"
                } else {
                    "plain"
                }
            );
            row.cells[0] = "changed,雪".to_owned();
            let after = before
                .rows_changed(&[RowChange {
                    schema_key: ROW_SCHEMA_KEY.into(),
                    row_pk: record.row_pk,
                    row: Some(csv_typed_row(row).unwrap()),
                    effect: ChangeEffect::Content,
                }])
                .unwrap()
                .0;
            assert!(after.bytes().starts_with(b"\xef\xbb\xbf\"changed,"));
            assert_reconstructs(&after);
        } else {
            assert_eq!(bytes, b"\xef\xbb\xbf");
        }
    }
}

#[test]
fn qa_repeated_edits_replay_after_identity_checkpoint_reopen() {
    let mut document = open(b"a,b\r\n\"c\",d\nlast");
    for revision in 0..80 {
        let next = match revision % 4 {
            0 => b"a\"b,c\ninsert,row\r\nlast".as_slice(),
            1 => b"\"a,b\",\"c\"\r\nlast\n".as_slice(),
            2 => b"\n\"\"\nlast\r".as_slice(),
            _ => b"a,b\r\n\"c\",d\nlast".as_slice(),
        };
        let before = document.row_records().unwrap();
        let (after, changes) = document
            .file_changed(
                &[FileEdit {
                    offset: 0,
                    delete_len: document.byte_len() as u64,
                    insert: next,
                }],
                IdNamespace::from_halves(0xabc, revision),
            )
            .unwrap();
        let replayed = Document::open_rows(apply_row_changes(before, &changes).unwrap())
            .unwrap()
            .0;
        assert_eq!(replayed.bytes(), next);
        assert_eq!(
            replayed.row_records().unwrap(),
            after.row_records().unwrap()
        );
        let (dialect, identities) = after.identity_checkpoint();
        document = Document::open_file_with_identities(
            after.bytes(),
            dialect,
            IdNamespace::from_halves(0xdef, revision),
            &identities,
        )
        .unwrap();
        assert_eq!(
            document.row_records().unwrap(),
            after.row_records().unwrap()
        );
    }
}
