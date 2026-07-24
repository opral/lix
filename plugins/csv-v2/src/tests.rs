use super::*;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::Value;

fn namespace() -> IdNamespace {
    IdNamespace::from_halves(0x0011_2233_4455_6677, 0x8899_aabb_ccdd_eeff)
}

fn rows(changes: impl IntoIterator<Item = Result<EntityChange, String>>) -> Vec<EntityChange> {
    changes
        .into_iter()
        .map(Result::unwrap)
        .filter(|change| change.schema_key == ROW_SCHEMA_KEY)
        .collect()
}

fn apply_edits(before: &[u8], edits: &[ByteEdit]) -> Vec<u8> {
    let mut after = Vec::new();
    let mut cursor = 0usize;
    for edit in edits {
        let start = usize::try_from(edit.offset).unwrap();
        let end = start + usize::try_from(edit.delete_len).unwrap();
        assert!(start >= cursor);
        after.extend_from_slice(&before[cursor..start]);
        after.extend_from_slice(&edit.insert);
        cursor = end;
    }
    after.extend_from_slice(&before[cursor..]);
    after
}

fn records(document: &Document) -> Vec<EntityRecord> {
    document
        .initial_changes()
        .map(|change| {
            let change = change.unwrap();
            EntityRecord {
                schema_key: change.schema_key,
                entity_pk: change.entity_pk,
                snapshot: change.snapshot.unwrap(),
            }
        })
        .collect()
}

fn has_number(value: &Value) -> bool {
    match value {
        Value::Number(_) => true,
        Value::Array(values) => values.iter().any(has_number),
        Value::Object(values) => values.values().any(has_number),
        _ => false,
    }
}

#[test]
fn generated_id_is_namespace_plus_big_endian_ordinal() {
    let id = namespace().encode(0x0102_0304_0506_0708);
    assert_eq!(id.len(), 32);
    let bytes = URL_SAFE_NO_PAD.decode(id).unwrap();
    assert_eq!(&bytes[..16], &namespace().0);
    assert_eq!(&bytes[16..], &0x0102_0304_0506_0708u64.to_be_bytes());
}

#[test]
fn import_streams_number_free_complete_snapshots() {
    let (document, changes) = Document::open_file(
        b"one,\"two,too\"\r\nthree,\"four\"\"4\"".to_vec(),
        Some("fixture.csv"),
        namespace(),
    )
    .unwrap();
    assert_eq!(document.row_count(), 2);
    assert_eq!(document.field_count(), 4);
    let changes = changes.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(changes.len(), 3);
    for change in changes {
        let value: Value = serde_json::from_slice(change.snapshot.as_ref().unwrap()).unwrap();
        assert!(!has_number(&value));
    }
}

#[test]
fn one_row_length_changing_edit_emits_one_complete_row() {
    let before = b"a,1\nb,2\nc,3\n".to_vec();
    let (document, initial) = Document::open_file(before, Some("x.csv"), namespace()).unwrap();
    let initial_rows = rows(initial);
    let middle_id = initial_rows[1].entity_pk[0].clone();
    let (after, changes) = document
        .file_changed(
            &[InputSplice {
                offset: 6,
                delete_len: 1,
                insert: b"twenty-two",
            }],
            IdNamespace::from_halves(7, 9),
        )
        .unwrap();
    assert!(document.shares_blob_backing_with(&after));
    assert_eq!(after.blob_piece_count(), 3);
    assert_eq!(after.bytes(), b"a,1\nb,twenty-two\nc,3\n");
    assert_eq!(changes.len(), 1, "{changes:#?}");
    assert_eq!(changes[0].entity_pk, [middle_id]);
    let snapshot = parse_row_snapshot(changes[0].snapshot.as_ref().unwrap()).unwrap();
    assert_eq!(snapshot.cells, ["b", "twenty-two"]);
}

#[test]
fn duplicate_rows_match_deterministically() {
    let (document, initial) = Document::open_file(
        b"same,x\nsame,x\nlast,z\n".to_vec(),
        Some("x.csv"),
        namespace(),
    )
    .unwrap();
    let ids = rows(initial)
        .into_iter()
        .map(|change| change.entity_pk[0].clone())
        .collect::<Vec<_>>();
    let (after, changes) = document
        .file_changed(
            &[InputSplice {
                offset: 12,
                delete_len: 1,
                insert: b"y",
            }],
            IdNamespace::from_halves(1, 2),
        )
        .unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].entity_pk, [ids[1].clone()]);
    let after_ids = rows(after.initial_changes())
        .into_iter()
        .filter(|change| change.schema_key == ROW_SCHEMA_KEY)
        .map(|change| change.entity_pk[0].clone())
        .collect::<Vec<_>>();
    assert_eq!(after_ids, ids);
}

#[test]
fn editing_or_deleting_first_duplicate_preserves_the_second_identity() {
    let source = b"same,x\nsame,x\nlast,z\n".to_vec();
    let (document, initial) = Document::open_file(source, Some("x.csv"), namespace()).unwrap();
    let ids = rows(initial)
        .into_iter()
        .map(|change| change.entity_pk[0].clone())
        .collect::<Vec<_>>();
    let (edited, changes) = document
        .file_changed(
            &[
                InputSplice {
                    offset: 5,
                    delete_len: 1,
                    insert: b"y",
                },
                InputSplice {
                    offset: 12,
                    delete_len: 1,
                    insert: b"x",
                },
            ],
            IdNamespace::from_halves(9, 9),
        )
        .unwrap();
    assert_eq!(changes.len(), 1, "{changes:#?}");
    assert_eq!(changes[0].entity_pk, [ids[0].clone()]);
    assert_eq!(
        rows(edited.initial_changes())[1].entity_pk,
        [ids[1].clone()]
    );

    let (deleted, _) = document
        .entities_changed(&[EntityChange {
            schema_key: ROW_SCHEMA_KEY.to_owned(),
            entity_pk: vec![ids[0].clone()],
            snapshot: None,
            effect: ChangeEffect::Content,
        }])
        .unwrap();
    let remaining = rows(deleted.initial_changes());
    assert_eq!(remaining[0].entity_pk, [ids[1].clone()]);
    assert_eq!(remaining[1].entity_pk, [ids[2].clone()]);
}

#[test]
fn reused_generation_namespace_rejects_duplicate_ordinal() {
    let (document, _) =
        Document::open_file(b"first\n".to_vec(), Some("x.csv"), namespace()).unwrap();
    let error = document
        .file_changed(
            &[InputSplice {
                offset: 6,
                delete_len: 0,
                insert: b"second\n",
            }],
            namespace(),
        )
        .unwrap_err();
    assert!(error.contains("identity already exists"), "{error}");
}

#[test]
fn mixed_line_endings_survive_local_edit() {
    let (document, _) =
        Document::open_file(b"a,1\r\nb,2\nc,3\r".to_vec(), Some("x.csv"), namespace()).unwrap();
    let (after, changes) = document
        .file_changed(
            &[InputSplice {
                offset: 7,
                delete_len: 1,
                insert: b"222",
            }],
            IdNamespace::from_halves(4, 5),
        )
        .unwrap();
    assert_eq!(after.bytes(), b"a,1\r\nb,222\nc,3\r");
    assert_eq!(changes.len(), 1);
}

#[test]
#[ignore = "large-file acceptance gate"]
fn exact_220k_fixture_has_compact_retained_state() {
    let short = b"00000000000000,1111111111,2222222222,3333333333\n";
    let long = b"000000000000000,1111111111,2222222222,3333333333\n";
    let mut bytes = Vec::with_capacity(10_680_000);
    for index in 0..220_000 {
        bytes.extend_from_slice(if index < 120_000 { long } else { short });
    }
    let file_len = bytes.len();
    assert_eq!(file_len, 10_680_000);
    let (document, _) = Document::open_file(bytes, Some("large.csv"), namespace()).unwrap();
    assert_eq!(document.row_count(), 220_000);
    assert_eq!(document.field_count(), 880_000);
    assert_eq!(document.bytes().len(), file_len);
    assert!(
        document.retained_bytes_estimate() < 64 * 1024 * 1024,
        "{}",
        describe_memory(&document)
    );
    assert!(
        document.retained_bytes_estimate() < file_len * 4,
        "{}",
        describe_memory(&document)
    );
}

#[test]
#[ignore = "large-file acceptance gate"]
fn exact_220k_cold_import_is_compact_and_shares_renderer_bytes() {
    const ROW_COUNT: usize = 220_000;
    let mut builder = core::EntityImportBuilder::new();
    for index in 0..ROW_COUNT {
        let id = format!("{index:032x}");
        let numerator = u128::try_from(index + 1).unwrap() * u128::from(u64::MAX);
        let order_rank =
            u64::try_from(numerator / u128::try_from(ROW_COUNT + 1).unwrap()).unwrap() | 1;
        let first = if index < 120_000 {
            "000000000000000"
        } else {
            "00000000000000"
        };
        builder
            .push(EntityRecord {
                schema_key: ROW_SCHEMA_KEY.to_owned(),
                entity_pk: vec![id.clone()],
                snapshot: format!(
                    "{{\"id\":\"{id}\",\"order_key\":\"{order_rank:016x}\",\"cells\":[\"{first}\",\"1111111111\",\"2222222222\",\"3333333333\"]}}"
                )
                .into_bytes(),
            })
            .unwrap();
    }
    builder
        .push(EntityRecord {
            schema_key: TABLE_SCHEMA_KEY.to_owned(),
            entity_pk: vec![ROOT_ENTITY_PK.to_owned()],
            snapshot:
                br#"{"id":"root","dialect":{"delimiter":",","quote":"\"","terminator":"\n"}}"#
                    .to_vec(),
        })
        .unwrap();
    let (document, edit) = builder.finish().unwrap();
    assert_eq!(document.row_count(), ROW_COUNT);
    assert_eq!(document.field_count(), ROW_COUNT * 4);
    assert_eq!(document.bytes().len(), 10_680_000);
    assert_eq!(edit.insert.len(), 10_680_000);
    assert!(
        document.shares_single_blob_with(&edit.insert),
        "the full cold renderer edit must alias the accepted document blob"
    );
    eprintln!("cold_import_memory {}", describe_memory(&document));
    assert!(
        document.retained_bytes_estimate() < 64 * 1024 * 1024,
        "{}",
        describe_memory(&document)
    );
}

#[test]
fn cold_open_preserves_noncompact_ids_and_warm_change_emits_local_bytes() {
    let source = b"alpha,one\nbeta,two\n".to_vec();
    let (warm, initial) = Document::open_file(source.clone(), Some("x.csv"), namespace()).unwrap();
    let initial = initial.collect::<Result<Vec<_>, _>>().unwrap();
    let noncompact_ids = initial
        .iter()
        .filter(|change| change.schema_key == ROW_SCHEMA_KEY)
        .map(|change| format!("noncompact-{}", change.entity_pk[0]))
        .collect::<Vec<_>>();
    let mut records = Vec::new();
    let mut row_index = 0;
    for change in initial {
        let mut snapshot = change.snapshot.unwrap();
        let mut pk = change.entity_pk;
        if change.schema_key == ROW_SCHEMA_KEY {
            let mut value: Value = serde_json::from_slice(&snapshot).unwrap();
            value["id"] = Value::String(noncompact_ids[row_index].clone());
            snapshot = serde_json::to_vec(&value).unwrap();
            pk[0] = noncompact_ids[row_index].clone();
            row_index += 1;
        }
        records.push(EntityRecord {
            schema_key: change.schema_key,
            entity_pk: pk,
            snapshot,
        });
    }
    let (cold, cold_edit) = Document::open_entities(records).unwrap();
    assert_eq!(cold_edit.offset, 0);
    assert_eq!(cold_edit.delete_len, 0);
    assert_eq!(cold_edit.insert.as_slice(), source);
    let cold_ids = rows(cold.initial_changes())
        .into_iter()
        .map(|change| change.entity_pk[0].clone())
        .collect::<Vec<_>>();
    assert_eq!(cold_ids, noncompact_ids);

    let second = rows(cold.initial_changes())[1].clone();
    let mut snapshot: Value = serde_json::from_slice(second.snapshot.as_ref().unwrap()).unwrap();
    snapshot["cells"][1] = Value::String("twenty-two".to_owned());
    let (changed, edits) = cold
        .entities_changed(&[EntityChange {
            schema_key: ROW_SCHEMA_KEY.to_owned(),
            entity_pk: second.entity_pk.clone(),
            snapshot: Some(serde_json::to_vec(&snapshot).unwrap()),
            effect: ChangeEffect::Content,
        }])
        .unwrap();
    assert_eq!(edits.len(), 1);
    assert!(edits[0].delete_len < source.len() as u64);
    assert_eq!(changed.bytes(), b"alpha,one\nbeta,twenty-two\n");
    assert_eq!(
        rows(changed.initial_changes())[1].entity_pk,
        second.entity_pk
    );

    // A fork aliases the immutable accepted version rather than duplicating
    // or mutating it when its successor is produced.
    assert_eq!(warm.fork().bytes(), source);
}

#[test]
fn cold_import_direct_index_preserves_quoting_and_crlf() {
    let source = b"one,\"two,too\"\r\nthree,\"four\"\"4\"\r\n".to_vec();
    let (_, initial) =
        Document::open_file(source.clone(), Some("quoted.csv"), namespace()).unwrap();
    let records = initial
        .map(|change| {
            let change = change.unwrap();
            EntityRecord {
                schema_key: change.schema_key,
                entity_pk: change.entity_pk,
                snapshot: change.snapshot.unwrap(),
            }
        })
        .collect();
    let (cold, edit) = Document::open_entities(records).unwrap();
    assert_eq!(cold.bytes(), source);
    assert_eq!(edit.insert.as_slice(), source);
    assert_eq!(cold.dialect().terminator, Terminator::CrLf);
}

#[test]
fn cold_open_preserves_sparse_lexical_layout_exactly() {
    let source = b"\"plain\",x\r\nnormal,\"a\"\"b\"\nlast,\"unnecessary\"".to_vec();
    let (warm, initial) =
        Document::open_file(source.clone(), Some("lexical.csv"), namespace()).unwrap();
    let records = initial
        .map(|change| {
            let change = change.unwrap();
            EntityRecord {
                schema_key: change.schema_key,
                entity_pk: change.entity_pk,
                snapshot: change.snapshot.unwrap(),
            }
        })
        .collect::<Vec<_>>();
    let row_values = records
        .iter()
        .filter(|record| record.schema_key == ROW_SCHEMA_KEY)
        .map(|record| serde_json::from_slice::<Value>(&record.snapshot).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(row_values[0]["layout"]["force_quote"], "AQ");
    assert!(row_values[0]["layout"].get("terminator").is_none());
    assert_eq!(row_values[1]["layout"]["terminator"], "\n");
    assert!(row_values[1]["layout"].get("force_quote").is_none());
    assert_eq!(row_values[2]["layout"]["force_quote"], "Ag");
    assert_eq!(row_values[2]["layout"]["terminator"], "");

    let (cold, edit) = Document::open_entities(records).unwrap();
    assert_eq!(cold.bytes(), source);
    assert_eq!(edit.insert.as_slice(), source);
    assert_eq!(cold.row_count(), warm.row_count());
}

#[test]
fn lexical_only_file_change_is_durable_and_format_only() {
    let before = b"plain,x\n".to_vec();
    let (document, _) = Document::open_file(before, Some("format.csv"), namespace()).unwrap();
    let (after, changes) = document
        .file_changed(
            &[InputSplice {
                offset: 0,
                delete_len: 5,
                insert: b"\"plain\"",
            }],
            IdNamespace::from_halves(7, 11),
        )
        .unwrap();
    assert_eq!(after.bytes(), b"\"plain\",x\n");
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].effect, ChangeEffect::FormatOnly);
    let snapshot: Value = serde_json::from_slice(changes[0].snapshot.as_ref().unwrap()).unwrap();
    assert_eq!(snapshot["layout"]["force_quote"], "AQ");

    let (cold, edit) = Document::open_entities(records(&after)).unwrap();
    assert_eq!(cold.bytes(), after.bytes());
    assert_eq!(apply_edits(&[], &[edit]), after.bytes());
}

#[test]
fn cold_open_requires_exactly_one_table_root() {
    let row = EntityRecord {
        schema_key: ROW_SCHEMA_KEY.to_owned(),
        entity_pk: vec!["row".to_owned()],
        snapshot: br#"{"id":"row","order_key":"01","cells":["value"]}"#.to_vec(),
    };
    let error = Document::open_entities(vec![row]).unwrap_err();
    assert!(error.contains("missing the table root"), "{error}");

    let table = EntityRecord {
        schema_key: TABLE_SCHEMA_KEY.to_owned(),
        entity_pk: vec![ROOT_ENTITY_PK.to_owned()],
        snapshot: br#"{"id":"root","dialect":{"delimiter":",","quote":"\"","terminator":"\n"}}"#
            .to_vec(),
    };
    let error = Document::open_entities(vec![table.clone(), table]).unwrap_err();
    assert!(error.contains("duplicate table root"), "{error}");
}

#[test]
fn entity_changes_reject_table_root_deletion() {
    let (document, _) =
        Document::open_file(b"a,b\n".to_vec(), Some("table.csv"), namespace()).unwrap();
    let error = document
        .entities_changed(&[EntityChange {
            schema_key: TABLE_SCHEMA_KEY.to_owned(),
            entity_pk: vec![ROOT_ENTITY_PK.to_owned()],
            snapshot: None,
            effect: ChangeEffect::Content,
        }])
        .unwrap_err();
    assert!(error.contains("table root cannot be deleted"), "{error}");
}

#[test]
fn descriptor_only_csv_tsv_rename_matches_fresh_open_in_both_directions() {
    fn cells(document: &Document) -> Vec<Vec<String>> {
        rows(document.initial_changes())
            .into_iter()
            .map(|change| {
                parse_row_snapshot(change.snapshot.as_ref().unwrap())
                    .unwrap()
                    .cells
            })
            .collect()
    }

    let source = b"a,b\nc,d\n".to_vec();
    let (csv, _) = Document::open_file(source.clone(), Some("table.csv"), namespace()).unwrap();
    let (renamed_tsv, changes) = csv
        .file_changed_with_paths(
            &[],
            Some("table.csv"),
            Some("table.tsv"),
            IdNamespace::from_halves(8, 1),
        )
        .unwrap();
    let (fresh_tsv, _) =
        Document::open_file(source.clone(), Some("table.tsv"), namespace()).unwrap();
    assert_eq!(renamed_tsv.bytes(), source);
    assert_eq!(renamed_tsv.dialect(), fresh_tsv.dialect());
    assert_eq!(cells(&renamed_tsv), cells(&fresh_tsv));
    assert_eq!(cells(&renamed_tsv), [vec!["a,b"], vec!["c,d"]]);
    assert_eq!(
        changes
            .iter()
            .filter(|change| change.schema_key == TABLE_SCHEMA_KEY)
            .count(),
        1
    );

    let (renamed_csv, changes) = renamed_tsv
        .file_changed_with_paths(
            &[],
            Some("table.tsv"),
            Some("table.csv"),
            IdNamespace::from_halves(8, 2),
        )
        .unwrap();
    let (fresh_csv, _) = Document::open_file(source, Some("table.csv"), namespace()).unwrap();
    assert_eq!(renamed_csv.dialect(), fresh_csv.dialect());
    assert_eq!(cells(&renamed_csv), cells(&fresh_csv));
    assert_eq!(cells(&renamed_csv), [vec!["a", "b"], vec!["c", "d"]]);
    assert!(!changes.is_empty());
}

#[test]
fn cold_and_entity_updates_reject_non_self_openable_dialects() {
    fn table(delimiter: &str, quote: Option<&str>) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "id": "root",
            "dialect": {
                "delimiter": delimiter,
                "quote": quote,
                "terminator": "\n"
            }
        }))
        .unwrap()
    }

    let row = EntityRecord {
        schema_key: ROW_SCHEMA_KEY.to_owned(),
        entity_pk: vec!["row".to_owned()],
        snapshot: br#"{"id":"row","order_key":"01","cells":["a","b"]}"#.to_vec(),
    };
    for (delimiter, quote) in [
        ("\n", Some("\"")),
        ("é", Some("\"")),
        (",", Some(",")),
        (",", Some("\r")),
    ] {
        let error = Document::open_entities(vec![
            EntityRecord {
                schema_key: TABLE_SCHEMA_KEY.to_owned(),
                entity_pk: vec![ROOT_ENTITY_PK.to_owned()],
                snapshot: table(delimiter, quote),
            },
            row.clone(),
        ])
        .unwrap_err();
        assert!(
            error.contains("ASCII") || error.contains("must differ"),
            "{error}"
        );
    }

    let (document, _) =
        Document::open_file(b"a,b\n".to_vec(), Some("safe.csv"), namespace()).unwrap();
    let invalid = table(",", Some(","));
    let error = document
        .entities_changed(&[EntityChange {
            schema_key: TABLE_SCHEMA_KEY.to_owned(),
            entity_pk: vec![ROOT_ENTITY_PK.to_owned()],
            snapshot: Some(invalid),
            effect: ChangeEffect::Content,
        }])
        .unwrap_err();
    assert!(error.contains("must differ"), "{error}");

    let no_quote = EntityRecord {
        schema_key: TABLE_SCHEMA_KEY.to_owned(),
        entity_pk: vec![ROOT_ENTITY_PK.to_owned()],
        snapshot: table(",", None),
    };
    let unrenderable = EntityRecord {
        schema_key: ROW_SCHEMA_KEY.to_owned(),
        entity_pk: vec!["row".to_owned()],
        snapshot: br#"{"id":"row","order_key":"01","cells":["a,b"]}"#.to_vec(),
    };
    let error = Document::open_entities(vec![no_quote, unrenderable]).unwrap_err();
    assert!(error.contains("cannot represent"), "{error}");

    let custom = EntityRecord {
        schema_key: TABLE_SCHEMA_KEY.to_owned(),
        entity_pk: vec![ROOT_ENTITY_PK.to_owned()],
        snapshot: table(";", Some("'")),
    };
    let custom_row = EntityRecord {
        schema_key: ROW_SCHEMA_KEY.to_owned(),
        entity_pk: vec!["row".to_owned()],
        snapshot: br#"{"id":"row","order_key":"01","cells":["a;b"]}"#.to_vec(),
    };
    let (custom, edit) = Document::open_entities(vec![custom, custom_row]).unwrap();
    assert_eq!(custom.bytes(), b"'a;b'\n");
    assert_eq!(edit.insert.as_slice(), custom.bytes());

    let redundant_layout = EntityRecord {
        schema_key: ROW_SCHEMA_KEY.to_owned(),
        entity_pk: vec!["row".to_owned()],
        snapshot: br#"{"id":"row","order_key":"01","cells":["a,b"],"layout":{"force_quote":"AQ"}}"#
            .to_vec(),
    };
    let error = Document::open_entities(vec![
        EntityRecord {
            schema_key: TABLE_SCHEMA_KEY.to_owned(),
            entity_pk: vec![ROOT_ENTITY_PK.to_owned()],
            snapshot: table(",", Some("\"")),
        },
        redundant_layout,
    ])
    .unwrap_err();
    assert!(error.contains("otherwise-unnecessary"), "{error}");
}

#[test]
fn noncompact_ids_insert_edit_reorder_delete_and_cold_open_stay_sparse() {
    let source = b"alpha,1\nbeta,2\ngamma,3\n".to_vec();
    let (warm, initial) =
        Document::open_file(source.clone(), Some("noncompact.csv"), namespace()).unwrap();
    let noncompact = ["external-row-a", "external-row-b", "external-row-c"];
    let mut imported = Vec::new();
    let mut row = 0usize;
    for change in initial {
        let change = change.unwrap();
        let mut pk = change.entity_pk;
        let mut snapshot = change.snapshot.unwrap();
        if change.schema_key == ROW_SCHEMA_KEY {
            let mut value: Value = serde_json::from_slice(&snapshot).unwrap();
            value["id"] = Value::String(noncompact[row].to_owned());
            pk[0] = noncompact[row].to_owned();
            snapshot = serde_json::to_vec(&value).unwrap();
            row += 1;
        }
        imported.push(EntityRecord {
            schema_key: change.schema_key,
            entity_pk: pk,
            snapshot,
        });
    }
    let (cold, _) = Document::open_entities(imported).unwrap();
    assert_eq!(cold.bytes(), source);

    let inserted_id = "external-row-inserted";
    let inserted_snapshot = format!(
        "{{\"id\":\"{inserted_id}\",\"order_key\":\"6000000000000001\",\"cells\":[\"inserted\",\"4\"]}}"
    )
    .into_bytes();
    let (inserted, edits) = cold
        .entities_changed(&[EntityChange {
            schema_key: ROW_SCHEMA_KEY.to_owned(),
            entity_pk: vec![inserted_id.to_owned()],
            snapshot: Some(inserted_snapshot),
            effect: ChangeEffect::Content,
        }])
        .unwrap();
    assert_eq!(edits.len(), 1);
    assert_eq!(apply_edits(&cold.bytes(), &edits), inserted.bytes());
    assert_eq!(inserted.bytes(), b"alpha,1\ninserted,4\nbeta,2\ngamma,3\n");
    assert!(inserted.sparse_rows_touched() < 512 * 3);

    // Exercise the appended-ID overlay after a cold reopen made every base
    // identity an explicit noncompact value.
    let (reopened, reopened_edit) = Document::open_entities(records(&inserted)).unwrap();
    assert_eq!(reopened_edit.insert.as_slice(), inserted.bytes());
    let edited_snapshot = format!(
        "{{\"id\":\"{inserted_id}\",\"order_key\":\"6000000000000001\",\"cells\":[\"edited\",\"44\"]}}"
    )
    .into_bytes();
    let (edited, edits) = reopened
        .entities_changed(&[EntityChange {
            schema_key: ROW_SCHEMA_KEY.to_owned(),
            entity_pk: vec![inserted_id.to_owned()],
            snapshot: Some(edited_snapshot),
            effect: ChangeEffect::Content,
        }])
        .unwrap();
    assert_eq!(edits.len(), 1);
    assert_eq!(apply_edits(&reopened.bytes(), &edits), edited.bytes());
    assert_eq!(edited.bytes(), b"alpha,1\nedited,44\nbeta,2\ngamma,3\n");

    let reordered_snapshot = format!(
        "{{\"id\":\"{inserted_id}\",\"order_key\":\"f000000000000001\",\"cells\":[\"edited\",\"44\"]}}"
    )
    .into_bytes();
    let (reordered, edits) = edited
        .entities_changed(&[EntityChange {
            schema_key: ROW_SCHEMA_KEY.to_owned(),
            entity_pk: vec![inserted_id.to_owned()],
            snapshot: Some(reordered_snapshot),
            effect: ChangeEffect::Content,
        }])
        .unwrap();
    assert_eq!(edits.len(), 2);
    assert_eq!(apply_edits(&edited.bytes(), &edits), reordered.bytes());
    assert_eq!(reordered.bytes(), b"alpha,1\nbeta,2\ngamma,3\nedited,44\n");
    assert!(reordered.sparse_rows_touched() < 512 * 6);

    let (deleted, edits) = reordered
        .entities_changed(&[EntityChange {
            schema_key: ROW_SCHEMA_KEY.to_owned(),
            entity_pk: vec![inserted_id.to_owned()],
            snapshot: None,
            effect: ChangeEffect::Content,
        }])
        .unwrap();
    assert_eq!(edits.len(), 1);
    assert_eq!(apply_edits(&reordered.bytes(), &edits), deleted.bytes());
    assert_eq!(deleted.bytes(), source);
    assert_eq!(rows(deleted.initial_changes()).len(), noncompact.len());
    assert_eq!(warm.bytes(), source);
}

#[test]
fn file_change_reorder_with_inserted_row_discards_stale_rank_anchor() {
    let initial_bytes = b"old,one\nkeep,two\n".to_vec();
    let (document, initial) =
        Document::open_file(initial_bytes.clone(), Some("noncompact.csv"), namespace()).unwrap();
    let noncompact_ids = ["external-row-a", "external-row-b"];
    let mut imported = Vec::new();
    let mut row = 0usize;
    for change in document.initial_changes() {
        let change = change.unwrap();
        let mut entity_pk = change.entity_pk;
        let mut snapshot = change.snapshot.unwrap();
        if change.schema_key == ROW_SCHEMA_KEY {
            let mut value: Value = serde_json::from_slice(&snapshot).unwrap();
            value["id"] = Value::String(noncompact_ids[row].to_owned());
            entity_pk[0] = noncompact_ids[row].to_owned();
            snapshot = serde_json::to_vec(&value).unwrap();
            row += 1;
        }
        imported.push(EntityRecord {
            schema_key: change.schema_key,
            entity_pk,
            snapshot,
        });
    }
    drop(initial);
    let (cold, _) = Document::open_entities(imported).unwrap();

    // `keep` moves ahead of `old`, is edited, and a new row lands between
    // them. Its retained rank is therefore greater than `old`'s retained
    // rank. The allocator must invalidate the latter as a stale upper anchor
    // before assigning the new row's rank.
    let changed_bytes = b"keep,TWO\nnew,three\nold,one\n";
    let (changed, _) = cold
        .file_changed(
            &[InputSplice {
                offset: 0,
                delete_len: initial_bytes.len() as u64,
                insert: changed_bytes,
            }],
            IdNamespace::from_halves(0x1111_2222_3333_4444, 0x5555_6666_7777_8888),
        )
        .unwrap();
    assert_eq!(changed.bytes(), changed_bytes);

    let changed_rows = rows(changed.initial_changes())
        .into_iter()
        .map(|change| {
            let snapshot = parse_row_snapshot(change.snapshot.as_ref().unwrap()).unwrap();
            (snapshot.cells, change.entity_pk[0].clone())
        })
        .collect::<Vec<_>>();
    assert_eq!(changed_rows[0].1, noncompact_ids[1]);
    assert_eq!(changed_rows[2].1, noncompact_ids[0]);
    assert_eq!(changed_rows[1].1.len(), 32);
}

#[test]
fn full_file_insert_edit_reorder_delete_keeps_duplicate_identities() {
    fn replace_all(document: &Document, bytes: &[u8], namespace: IdNamespace) -> Document {
        document
            .file_changed(
                &[InputSplice {
                    offset: 0,
                    delete_len: document.bytes().len() as u64,
                    insert: bytes,
                }],
                namespace,
            )
            .unwrap()
            .0
    }

    fn row_id(document: &Document, cells: &[&str]) -> String {
        rows(document.initial_changes())
            .into_iter()
            .find_map(|change| {
                let snapshot = parse_row_snapshot(change.snapshot.as_ref().unwrap()).unwrap();
                (snapshot.cells == cells).then(|| change.entity_pk[0].clone())
            })
            .unwrap()
    }

    fn duplicate_ids(document: &Document) -> Vec<String> {
        rows(document.initial_changes())
            .into_iter()
            .filter_map(|change| {
                let snapshot = parse_row_snapshot(change.snapshot.as_ref().unwrap()).unwrap();
                (snapshot.cells == ["dup", "same"]).then(|| change.entity_pk[0].clone())
            })
            .collect()
    }

    let (initial, _) = Document::open_file(
        b"alpha,one\ndup,same\ndup,same\nomega,last\n".to_vec(),
        Some("lifecycle.csv"),
        namespace(),
    )
    .unwrap();
    let alpha_id = row_id(&initial, &["alpha", "one"]);
    let omega_id = row_id(&initial, &["omega", "last"]);
    let duplicates = duplicate_ids(&initial);

    let inserted = replace_all(
        &initial,
        b"alpha,one\ninserted,new\ndup,same\ndup,same\nomega,last\n",
        IdNamespace::from_halves(1, 1),
    );
    let inserted_id = row_id(&inserted, &["inserted", "new"]);
    let edited = replace_all(
        &inserted,
        b"alpha,ONE\ninserted,new\ndup,same\ndup,same\nomega,last\n",
        IdNamespace::from_halves(2, 2),
    );
    assert_eq!(row_id(&edited, &["alpha", "ONE"]), alpha_id);

    let reordered = replace_all(
        &edited,
        b"omega,last\ndup,same\nalpha,ONE\ninserted,new\ndup,same\n",
        IdNamespace::from_halves(3, 3),
    );
    assert_eq!(row_id(&reordered, &["omega", "last"]), omega_id);
    assert_eq!(row_id(&reordered, &["alpha", "ONE"]), alpha_id);
    assert_eq!(row_id(&reordered, &["inserted", "new"]), inserted_id);
    assert_eq!(duplicate_ids(&reordered), duplicates);

    let deleted = replace_all(
        &reordered,
        b"omega,last\ndup,same\ninserted,new\n",
        IdNamespace::from_halves(4, 4),
    );
    assert_eq!(row_id(&deleted, &["omega", "last"]), omega_id);
    assert_eq!(row_id(&deleted, &["inserted", "new"]), inserted_id);
    assert_eq!(duplicate_ids(&deleted).len(), 1);
}

#[test]
fn sparse_eof_insert_preserves_an_unterminated_predecessor() {
    let (document, initial) =
        Document::open_file(b"alpha,1\nbeta,2".to_vec(), Some("x.csv"), namespace()).unwrap();
    let last_order = parse_row_snapshot(rows(initial).last().unwrap().snapshot.as_ref().unwrap())
        .unwrap()
        .order_key;
    let order = u64::from_str_radix(&last_order, 16).unwrap() + 0x1001;
    let id = "20000000-0000-4000-8000-000000000001";
    let snapshot =
        format!("{{\"id\":\"{id}\",\"order_key\":\"{order:016x}\",\"cells\":[\"gamma\",\"3\"]}}")
            .into_bytes();
    let (inserted, edits) = document
        .entities_changed(&[EntityChange {
            schema_key: ROW_SCHEMA_KEY.to_owned(),
            entity_pk: vec![id.to_owned()],
            snapshot: Some(snapshot),
            effect: ChangeEffect::Content,
        }])
        .unwrap();
    assert_eq!(edits.len(), 1);
    assert_eq!(edits[0].offset, document.bytes().len() as u64);
    assert_eq!(inserted.bytes(), b"alpha,1\nbeta,2\ngamma,3\n");
    assert_eq!(apply_edits(&document.bytes(), &edits), inserted.bytes());
    assert!(inserted.sparse_rows_touched() < 512 * 3);
}

#[test]
fn sparse_generated_ordinal_does_not_invalidate_dense_import_lookup() {
    let (document, initial) =
        Document::open_file(b"one\ntwo\nthree\n".to_vec(), Some("x.csv"), namespace()).unwrap();
    let initial = rows(initial);
    let original_id = initial[0].entity_pk[0].clone();
    let middle_order = parse_row_snapshot(initial[1].snapshot.as_ref().unwrap())
        .unwrap()
        .order_key;
    let inserted_id = namespace().encode(999_999);
    let snapshot = format!(
        "{{\"id\":\"{inserted_id}\",\"order_key\":\"{middle_order}\",\"cells\":[\"inserted\"]}}"
    )
    .into_bytes();
    let (inserted, _) = document
        .entities_changed(&[EntityChange {
            schema_key: ROW_SCHEMA_KEY.to_owned(),
            entity_pk: vec![inserted_id],
            snapshot: Some(snapshot),
            effect: ChangeEffect::Content,
        }])
        .unwrap();

    let original = rows(inserted.initial_changes())
        .into_iter()
        .find(|change| change.entity_pk[0] == original_id)
        .unwrap();
    let mut snapshot: Value = serde_json::from_slice(original.snapshot.as_ref().unwrap()).unwrap();
    snapshot["cells"][0] = Value::String("ONE".to_owned());
    let (edited, edits) = inserted
        .entities_changed(&[EntityChange {
            schema_key: ROW_SCHEMA_KEY.to_owned(),
            entity_pk: original.entity_pk,
            snapshot: Some(serde_json::to_vec(&snapshot).unwrap()),
            effect: ChangeEffect::Content,
        }])
        .unwrap();
    assert_eq!(edits.len(), 1);
    assert!(edited.bytes().starts_with(b"ONE\n"));
}

#[test]
fn cold_open_rejects_duplicate_noncompact_row_ids() {
    let table = EntityRecord {
        schema_key: TABLE_SCHEMA_KEY.to_owned(),
        entity_pk: vec![ROOT_ENTITY_PK.to_owned()],
        snapshot: br#"{"id":"root","dialect":{"delimiter":",","quote":"\"","terminator":"\n"}}"#
            .to_vec(),
    };
    let snapshot = |cell: &str| EntityRecord {
        schema_key: ROW_SCHEMA_KEY.to_owned(),
        entity_pk: vec!["same-noncompact-id".to_owned()],
        snapshot: format!(
            "{{\"id\":\"same-noncompact-id\",\"order_key\":\"{}\",\"cells\":[\"{cell}\"]}}",
            if cell == "one" { "4001" } else { "8001" }
        )
        .into_bytes(),
    };
    let error = Document::open_entities(vec![table, snapshot("one"), snapshot("two")]).unwrap_err();
    assert!(error.contains("identities must be unique"), "{error}");
}

#[test]
#[ignore = "large-file acceptance gate"]
fn exact_220k_insert_and_reorder_touch_only_boundary_chunks() {
    const ROW_COUNT: usize = 220_000;
    const CHUNK_ROWS: usize = 512;
    let mut source = Vec::with_capacity(ROW_COUNT * 9);
    for index in 0..ROW_COUNT {
        source.extend_from_slice(format!("{index:08}\n").as_bytes());
    }
    let (document, _) = Document::open_file(source, Some("large.csv"), namespace()).unwrap();
    let left_index = 109_999usize;
    let left = u64::try_from(
        u128::try_from(left_index + 1).unwrap() * u128::from(u64::MAX)
            / u128::try_from(ROW_COUNT + 1).unwrap(),
    )
    .unwrap()
        | 1;
    let right = u64::try_from(
        u128::try_from(left_index + 2).unwrap() * u128::from(u64::MAX)
            / u128::try_from(ROW_COUNT + 1).unwrap(),
    )
    .unwrap()
        | 1;
    let order = left + (right - left) / 2;
    let id = IdNamespace::from_halves(0xfeed_face_dead_beef, 0x0123_4567_89ab_cdef).encode(0);
    let snapshot =
        format!("{{\"id\":\"{id}\",\"order_key\":\"{order:016x}\",\"cells\":[\"inserted\"]}}")
            .into_bytes();
    let (inserted, edits) = document
        .entities_changed(&[EntityChange {
            schema_key: ROW_SCHEMA_KEY.to_owned(),
            entity_pk: vec![id.clone()],
            snapshot: Some(snapshot),
            effect: ChangeEffect::Content,
        }])
        .unwrap();
    assert_eq!(inserted.row_count(), ROW_COUNT + 1);
    assert_eq!(edits.len(), 1);
    assert!(
        inserted.sparse_rows_touched() < CHUNK_ROWS * 3,
        "insert re-indexed {} rows",
        inserted.sparse_rows_touched()
    );

    let snapshot =
        format!("{{\"id\":\"{id}\",\"order_key\":\"0000000000000001\",\"cells\":[\"inserted\"]}}")
            .into_bytes();
    let (reordered, edits) = inserted
        .entities_changed(&[EntityChange {
            schema_key: ROW_SCHEMA_KEY.to_owned(),
            entity_pk: vec![id],
            snapshot: Some(snapshot),
            effect: ChangeEffect::Content,
        }])
        .unwrap();
    assert_eq!(edits.len(), 2);
    assert_eq!(&reordered.bytes()[..9], b"inserted\n");
    assert!(
        reordered.sparse_rows_touched() < CHUNK_ROWS * 6,
        "reorder re-indexed {} rows",
        reordered.sparse_rows_touched()
    );
}

#[test]
fn snapshot_parser_rejects_numbers() {
    let error = parse_row_snapshot(br#"{"id":"x","order_key":"01","cells":[1]}"#).unwrap_err();
    assert!(error.contains("number-bearing"));
}

#[test]
fn rank_allocation_skips_reserved_suffix_without_false_exhaustion() {
    assert_eq!(
        core::ranks_between(Some(0xff), Some(0x103), 2).unwrap(),
        [0x101, 0x102]
    );
}
