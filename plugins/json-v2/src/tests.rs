use crate::core::{
    ChangeEffect, Document, EntityChange, EntityRecord, IdNamespace, InputSplice,
    JsonPropertySnapshot, PROPERTY_SCHEMA_KEY, parse_property_snapshot,
};
use std::io::Write as _;

fn namespace() -> IdNamespace {
    IdNamespace::from_halves(1, 2)
}

fn open(bytes: &[u8]) -> Document {
    Document::open_file(bytes.to_vec(), Some("document.json"), namespace())
        .expect("open canonical JSON")
        .0
}

fn initial_records(document: &Document) -> Vec<EntityRecord> {
    document
        .initial_changes()
        .map(|change| {
            let change = change.expect("initial change");
            EntityRecord {
                schema_key: change.schema_key,
                entity_pk: change.entity_pk,
                snapshot: change.snapshot.expect("initial upsert"),
            }
        })
        .collect()
}

#[test]
fn indexes_arbitrary_json_values_as_number_free_property_snapshots() {
    let document = open(br#"{"a":1.2300e+4,"b":{"nested":[true,null,2]},"c":"text"}"#);
    let changes = document
        .initial_changes()
        .collect::<Result<Vec<_>, _>>()
        .expect("initial snapshots");
    assert_eq!(changes.len(), 3);
    assert_eq!(changes[0].entity_pk, ["a"]);
    assert_eq!(
        parse_property_snapshot(changes[0].snapshot.as_ref().expect("upsert")).expect("snapshot"),
        JsonPropertySnapshot {
            key: "a".to_owned(),
            order_key: "3fffffffffffffff".to_owned(),
            value_json: "1.2300e+4".to_owned(),
        }
    );
    let durable: serde_json::Value =
        serde_json::from_slice(changes[1].snapshot.as_ref().expect("upsert")).expect("JSON");
    assert!(
        durable
            .get("value_json")
            .is_some_and(serde_json::Value::is_string)
    );
}

#[test]
fn rejects_noncanonical_envelope_and_duplicate_keys() {
    for invalid in [
        br#" {"a":1}"#.as_slice(),
        br#"{"a" :1}"#,
        br#"{"a": 1}"#,
        br#"{"a":1 }"#,
        br#"{"\u0061":1}"#,
        br#"{"a":1,"a":2}"#,
        br"[1,2,3]",
    ] {
        assert!(
            Document::open_file(invalid.to_vec(), Some("bad.json"), namespace()).is_err(),
            "accepted invalid input {}",
            String::from_utf8_lossy(invalid)
        );
    }
}

#[test]
fn file_change_inside_one_value_is_one_sparse_upsert() {
    let before = br#"{"a":"one","b":{"n":1},"c":false}"#;
    let document = open(before);
    let offset = before
        .windows(br#"{"n":1}"#.len())
        .position(|window| window == br#"{"n":1}"#)
        .expect("nested value")
        + 5;
    let (after, changes) = document
        .file_changed(
            &[InputSplice {
                offset: u64::try_from(offset).expect("offset fits"),
                delete_len: 1,
                insert: b"2",
            }],
            namespace(),
        )
        .expect("sparse transition");

    assert_eq!(after.bytes(), br#"{"a":"one","b":{"n":2},"c":false}"#);
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].entity_pk, ["b"]);
    assert_eq!(
        parse_property_snapshot(changes[0].snapshot.as_ref().expect("upsert"))
            .expect("snapshot")
            .value_json,
        r#"{"n":2}"#
    );
    assert_eq!(after.sparse_properties_touched(), 1);
    assert!(after.shares_blob_backing_with(&document));
    assert_eq!(after.blob_piece_count(), 3);
}

#[test]
fn length_changing_value_edit_updates_only_one_index_chunk() {
    let before = br#"{"a":"one","b":2,"c":[3]}"#;
    let document = open(before);
    let start = before
        .windows(br#""one""#.len())
        .position(|window| window == br#""one""#)
        .expect("value");
    let (after, changes) = document
        .file_changed(
            &[InputSplice {
                offset: u64::try_from(start).expect("offset fits"),
                delete_len: u64::try_from(br#""one""#.len()).expect("length fits"),
                insert: br#""considerably-longer""#,
            }],
            namespace(),
        )
        .expect("length-changing sparse transition");
    assert_eq!(
        after.bytes(),
        br#"{"a":"considerably-longer","b":2,"c":[3]}"#
    );
    assert_eq!(changes.len(), 1);
    let records = initial_records(&after);
    assert_eq!(
        parse_property_snapshot(&records[2].snapshot)
            .expect("following property snapshot")
            .value_json,
        "[3]"
    );
}

#[test]
fn one_entity_update_returns_one_value_byte_edit() {
    let before = br#"{"a":1,"b":{"n":2},"c":3}"#;
    let document = open(before);
    let current = document
        .initial_changes()
        .nth(1)
        .expect("second property")
        .expect("change");
    let mut snapshot =
        parse_property_snapshot(current.snapshot.as_ref().expect("upsert")).expect("snapshot");
    snapshot.value_json = r#"["changed",4]"#.to_owned();
    let snapshot = serde_json::to_vec(&serde_json::json!({
        "key": snapshot.key,
        "order_key": snapshot.order_key,
        "value_json": snapshot.value_json,
    }))
    .expect("serialize snapshot");
    let (after, edits) = document
        .entities_changed(&[EntityChange {
            schema_key: PROPERTY_SCHEMA_KEY.to_owned(),
            entity_pk: vec!["b".to_owned()],
            snapshot: Some(snapshot),
            effect: ChangeEffect::Content,
        }])
        .expect("sparse entity render");
    assert_eq!(after.bytes(), br#"{"a":1,"b":["changed",4],"c":3}"#);
    assert_eq!(edits.len(), 1);
    assert_eq!(edits[0].insert.as_slice(), br#"["changed",4]"#);
    assert!(after.shares_blob_backing_with(&document));
    assert_eq!(after.sparse_properties_touched(), 1);
}

#[test]
fn cold_entity_import_roundtrips_exact_canonical_bytes() {
    let bytes = br#"{"a":1.00,"b":{"x":[true,false]},"c":"value"}"#;
    let document = open(bytes);
    let records = initial_records(&document);
    let (imported, edit) = Document::open_entities(records).expect("cold entity import");
    assert_eq!(edit.offset, 0);
    assert_eq!(edit.delete_len, 0);
    assert_eq!(edit.insert.as_slice(), bytes);
    assert_eq!(imported.bytes(), bytes);
    assert!(imported.shares_single_blob_with(&edit.insert));
}

#[test]
fn cold_import_retains_supplied_fractional_order_keys() {
    let records = [
        ("later", "f1", "2"),
        ("first", "01", "1"),
        ("middle", "81", r#"{"nested":3}"#),
    ]
    .into_iter()
    .map(|(key, order_key, value_json)| EntityRecord {
        schema_key: PROPERTY_SCHEMA_KEY.to_owned(),
        entity_pk: vec![key.to_owned()],
        snapshot: serde_json::to_vec(&serde_json::json!({
            "key": key,
            "order_key": order_key,
            "value_json": value_json,
        }))
        .expect("snapshot"),
    })
    .collect();
    let (document, edit) = Document::open_entities(records).expect("import reordered properties");
    assert_eq!(
        edit.insert.as_slice(),
        br#"{"first":1,"middle":{"nested":3},"later":2}"#
    );
    let orders = document
        .initial_changes()
        .map(|change| {
            parse_property_snapshot(change.expect("change").snapshot.as_ref().expect("upsert"))
                .expect("snapshot")
                .order_key
        })
        .collect::<Vec<_>>();
    assert_eq!(orders, ["01", "81", "f1"]);
}

#[test]
fn structural_file_edit_uses_exact_fallback() {
    let before = br#"{"a":1,"b":2}"#;
    let document = open(before);
    let (after, changes) = document
        .file_changed(
            &[InputSplice {
                offset: u64::try_from(before.len() - 1).expect("offset fits"),
                delete_len: 0,
                insert: br#","c":{"n":3}"#,
            }],
            namespace(),
        )
        .expect("structural fallback");
    assert_eq!(after.bytes(), br#"{"a":1,"b":2,"c":{"n":3}}"#);
    assert!(changes.iter().any(|change| change.entity_pk == ["c"]));
}

#[test]
fn exact_ten_megabyte_fixture_stays_sparse_and_bounded() {
    const PROPERTY_COUNT: usize = 220_000;
    const TARGET_BYTES: usize = 10_000_000;
    const LONG_VALUE_COUNT: usize = 99_999;

    let mut bytes = Vec::with_capacity(TARGET_BYTES);
    let mut state = 0x6a73_6f6e_2d31_306du64;
    let middle = PROPERTY_COUNT / 2;
    let mut edit_offset = None;
    bytes.push(b'{');
    for index in 0..PROPERTY_COUNT {
        if index > 0 {
            bytes.push(b',');
        }
        state = splitmix64(state);
        let first = state;
        state = splitmix64(state);
        let second = u32::try_from(state & u64::from(u32::MAX)).expect("masked");
        write!(
            &mut bytes,
            "\"property_{index:06}\":\"{first:016x}{second:08x}"
        )
        .expect("write property");
        if index == middle {
            edit_offset = Some(bytes.len() - 24);
        }
        if index < LONG_VALUE_COUNT {
            bytes.push(b'f');
        }
        bytes.push(b'"');
    }
    bytes.push(b'}');
    assert_eq!(bytes.len(), TARGET_BYTES);

    let document = open(&bytes);
    assert_eq!(document.property_count(), PROPERTY_COUNT);
    assert!(
        document.retained_bytes_estimate() < 24 * 1024 * 1024,
        "retained estimate was {} bytes",
        document.retained_bytes_estimate()
    );
    let edit_offset = edit_offset.expect("middle offset");
    let replacement = if bytes[edit_offset] == b'0' {
        b'1'
    } else {
        b'0'
    };
    let (after, changes) = document
        .file_changed(
            &[InputSplice {
                offset: u64::try_from(edit_offset).expect("offset fits"),
                delete_len: 1,
                insert: &[replacement],
            }],
            namespace(),
        )
        .expect("10 MB sparse transition");
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].entity_pk, ["property_110000"]);
    assert_eq!(after.sparse_properties_touched(), 1);
    assert!(after.shares_blob_backing_with(&document));
}

fn splitmix64(mut state: u64) -> u64 {
    state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut value = state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
