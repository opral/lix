use crate::markdown_file::parse_markdown_source;
use crate::{
    ChangeEffect, Document, EntityChange, EntityRecord, IdNamespace, InputSplice, NODE_SCHEMA_KEY,
};
use base64::Engine as _;
use serde_json::Value;

fn assert_number_free(value: &Value) {
    match value {
        Value::Number(number) => panic!("wire snapshot contained JSON number {number}"),
        Value::Array(values) => values.iter().for_each(assert_number_free),
        Value::Object(object) => object.values().for_each(assert_number_free),
        _ => {}
    }
}

fn records(changes: &[EntityChange]) -> Vec<EntityRecord> {
    changes
        .iter()
        .filter_map(|change| {
            change.snapshot.as_ref().map(|snapshot| EntityRecord {
                schema_key: change.schema_key.clone(),
                entity_pk: change.entity_pk.clone(),
                snapshot: snapshot.clone(),
            })
        })
        .collect()
}

fn plain_paragraph_snapshot(text: &str) -> Vec<u8> {
    let (_, changes) = Document::open_file(
        format!("{text}\n").into_bytes(),
        Some("merge.md"),
        IdNamespace::from_halves(41, 42),
    )
    .expect("plain paragraph should parse");
    changes
        .into_iter()
        .find_map(|change| {
            let snapshot = change.snapshot?;
            let wire: Value = serde_json::from_slice(&snapshot).ok()?;
            (wire["kind"] == "paragraph").then_some(snapshot)
        })
        .expect("plain paragraph snapshot")
}

fn snapshot_with_plain_text(snapshot: &[u8], text: &str) -> Vec<u8> {
    let mut wire: Value = serde_json::from_slice(snapshot).expect("valid wire snapshot");
    let mut payload: Value = serde_json::from_str(
        wire["payload_json"]
            .as_str()
            .expect("wire payload JSON string"),
    )
    .expect("valid payload JSON");
    let [inline] = payload["inline"]
        .as_array_mut()
        .expect("plain paragraph inline array")
        .as_mut_slice()
    else {
        panic!("plain paragraph should contain exactly one inline");
    };
    assert_eq!(inline["type"], "text");
    inline["value"] = Value::String(text.to_owned());
    wire["payload_json"] = Value::String(serde_json::to_string(&payload).unwrap());
    serde_json::to_vec(&wire).expect("updated wire snapshot")
}

fn plain_text_from_snapshot(snapshot: &[u8]) -> String {
    let wire: Value = serde_json::from_slice(snapshot).expect("valid wire snapshot");
    let payload: Value = serde_json::from_str(
        wire["payload_json"]
            .as_str()
            .expect("wire payload JSON string"),
    )
    .expect("valid payload JSON");
    payload["inline"]
        .as_array()
        .and_then(|inlines| inlines.first())
        .and_then(|inline| inline["value"].as_str())
        .expect("plain text inline value")
        .to_owned()
}

fn document_format(changes: &[EntityChange]) -> Value {
    let wire = changes
        .iter()
        .filter_map(|change| change.snapshot.as_deref())
        .map(|snapshot| serde_json::from_slice::<Value>(snapshot).expect("valid wire snapshot"))
        .find(|wire| wire["kind"] == "document")
        .expect("document snapshot");
    serde_json::from_str(wire["format_json"].as_str().expect("document format_json"))
        .expect("valid document format JSON")
}

fn utf16le(source: &str) -> Vec<u8> {
    let mut bytes = vec![0xff, 0xfe];
    for unit in source.encode_utf16() {
        bytes.extend_from_slice(&unit.to_le_bytes());
    }
    bytes
}

fn utf16be(source: &str) -> Vec<u8> {
    let mut bytes = vec![0xfe, 0xff];
    for unit in source.encode_utf16() {
        bytes.extend_from_slice(&unit.to_be_bytes());
    }
    bytes
}

#[test]
fn allocated_uuid_v7_ids_are_retry_stable_and_canonical() {
    let source = b"# Heading\n\nText *emphasis*.\n".to_vec();
    let namespace = IdNamespace::from_halves(0x0102_0304_0506_0708, 0x1112_1314);
    let (_, first) = Document::open_file(source.clone(), Some("doc.md"), namespace).unwrap();
    let (_, retry) = Document::open_file(source, Some("doc.md"), namespace).unwrap();
    assert_eq!(first, retry);
    for change in first {
        let id = &change.entity_pk[0];
        assert_eq!(id.len(), 36);
        assert!(uuid::Uuid::parse_str(id).is_ok());
    }
}

#[test]
fn wire_snapshots_are_number_free_even_when_markdown_model_has_numeric_fields() {
    let source = b"## Heading\n\n3. item\n\n````rust\nlet answer = 42;\n````\n".to_vec();
    let (_, changes) =
        Document::open_file(source, Some("numbers.md"), IdNamespace::default()).unwrap();
    for change in changes {
        let snapshot = change.snapshot.expect("initial change is an upsert");
        let value: Value = serde_json::from_slice(&snapshot).unwrap();
        assert_number_free(&value);
        assert!(value["payload_json"].is_string());
        assert!(value["format_json"].is_string());
    }
}

#[test]
fn entity_conflict_keeps_disjoint_plain_paragraph_word_inserts() {
    let base = plain_paragraph_snapshot("word");
    let a = snapshot_with_plain_text(&base, "beginword");
    let b = snapshot_with_plain_text(&base, "wordend");

    let resolved = Document::resolve_entity_conflict(Some(base), Some(a), Some(b))
        .expect("b live entity should produce a resolution");

    assert_eq!(plain_text_from_snapshot(&resolved), "beginwordend");
}

#[test]
fn entity_conflict_orders_same_position_inserts_by_canonical_side() {
    let base = plain_paragraph_snapshot("word");
    let a = snapshot_with_plain_text(&base, "aword");
    let b = snapshot_with_plain_text(&base, "bword");

    let resolved = Document::resolve_entity_conflict(Some(base), Some(a), Some(b))
        .expect("b live entity should produce a resolution");

    assert_eq!(plain_text_from_snapshot(&resolved), "abword");
}

#[test]
fn entity_conflict_uses_exact_b_snapshot_for_overlapping_edits_or_deletes() {
    let base = plain_paragraph_snapshot("word");
    let a = snapshot_with_plain_text(&base, "a");
    let b = snapshot_with_plain_text(&base, "b");

    assert_eq!(
        Document::resolve_entity_conflict(Some(base.clone()), Some(a), Some(b.clone())),
        Some(b),
        "overlapping replacements must remain deterministic b-wins",
    );
    assert_eq!(
        Document::resolve_entity_conflict(Some(base), Some(plain_paragraph_snapshot("a")), None),
        None,
        "a b delete must win without trying to parse a stale side",
    );
}

#[test]
fn cold_entity_open_roundtrips_marker_free_complete_gfm_document() {
    let source = b"---\ntitle: Test\n---\n\n| A | B |\n| --- | --- |\n| *x* | `y` |\n".to_vec();
    assert!(
        !source.windows(2).any(|window| window == b"]:"),
        "fixture must exercise the parser's marker-free definition fast path",
    );
    let (_, changes) = Document::open_file(
        source.clone(),
        Some("table.md"),
        IdNamespace::from_halves(9, 10),
    )
    .unwrap();
    let (document, edits) = Document::open_entities(records(&changes), None).unwrap();
    assert_eq!(edits.len(), 1);
    assert_eq!(edits[0].offset, 0);
    assert_eq!(edits[0].delete_len, 0);
    assert_eq!(document.accepted_bytes(), source);
    assert_eq!(document.accepted_bytes(), edits[0].insert.as_slice());
}

#[test]
fn duplicate_adjacent_table_headers_reach_a_stable_representation() {
    let source = "\
| Command | Description |
|---------|-------------|
| Command | Description |
|---------|-------------|
| `clawdis login` | Link WhatsApp Web via QR |
";
    let parsed =
        parse_markdown_source(source).expect("adjacent table-like headers should stabilize");
    assert!(parsed.canonical_render.is_some());
}

#[test]
fn cold_entity_open_reuses_host_verified_materialized_bytes() {
    let source = b"Alpha\n\nBeta\n".to_vec();
    let (_, changes) = Document::open_file(
        source.clone(),
        Some("accepted.md"),
        IdNamespace::from_halves(9, 10),
    )
    .unwrap();
    let (document, edits) =
        Document::open_entities(records(&changes), Some(source.clone())).unwrap();
    assert!(edits.is_empty());
    assert_eq!(document.accepted_bytes(), source);
}

#[test]
fn cold_entity_open_preserves_accepted_noncanonical_source_bytes() {
    let cases = [
        ("no-final-lf", b"Alpha\n\nBeta".to_vec()),
        ("extra-blank-lines", b"Alpha\n\n\n\nBeta\n".to_vec()),
        ("crlf", b"# A\r\n\r\nB\r\n".to_vec()),
        ("utf8-bom", b"\xef\xbb\xbf# A\n\nB\n".to_vec()),
        ("utf16le", utf16le("# Café\r\n\r\nText\r\n")),
        ("utf16be", utf16be("# Café\n\nText\n")),
    ];

    for (name, source) in cases {
        let (_, changes) = Document::open_file(
            source.clone(),
            Some("cold.md"),
            IdNamespace::from_halves(9, 10),
        )
        .unwrap_or_else(|error| panic!("{name}: open file failed: {error:?}"));
        let (cold, edits) = Document::open_entities(records(&changes), None)
            .unwrap_or_else(|error| panic!("{name}: cold open failed: {error:?}"));
        assert_eq!(cold.accepted_bytes(), source, "{name}");
        assert_eq!(edits.len(), 1, "{name}");
        assert_eq!(edits[0].offset, 0, "{name}");
        assert_eq!(edits[0].delete_len, 0, "{name}");
        assert_eq!(edits[0].insert.as_slice(), source, "{name}");
    }
}

#[test]
fn canonical_source_avoids_fallback_and_encoded_source_preserves_it() {
    let canonical = b"# Heading\n\nBody\n".to_vec();
    let (_, canonical_changes) = Document::open_file(
        canonical,
        Some("canonical.md"),
        IdNamespace::from_halves(1, 2),
    )
    .expect("canonical Markdown should open");
    assert!(
        document_format(&canonical_changes)
            .get("lexical_fallback_base64")
            .is_none(),
        "canonical input must not retain an unnecessary raw fallback",
    );

    let encoded = b"\xef\xbb\xbf# Heading\n\nBody\n".to_vec();
    let (_, encoded_changes) = Document::open_file(
        encoded.clone(),
        Some("encoded.md"),
        IdNamespace::from_halves(3, 4),
    )
    .expect("encoded Markdown should open");
    let expected_fallback = base64::engine::general_purpose::STANDARD.encode(&encoded);
    assert_eq!(
        document_format(&encoded_changes)
            .get("lexical_fallback_base64")
            .and_then(Value::as_str),
        Some(expected_fallback.as_str()),
        "noncanonical bytes must remain available for exact lexical restoration",
    );
    let (restored, edits) = Document::open_entities(records(&encoded_changes), None)
        .expect("encoded Markdown should restore from semantic entities");
    assert_eq!(restored.accepted_bytes(), encoded);
    assert_eq!(edits.len(), 1);
}

#[test]
fn cold_entity_open_ignores_stale_raw_fallback_after_direct_entity_edit() {
    let source = b"\xef\xbb\xbfBefore\n\n\n\nUntouched".to_vec();
    let (_, changes) = Document::open_file(
        source,
        Some("cold-edit.md"),
        IdNamespace::from_halves(11, 12),
    )
    .unwrap();
    let mut records = records(&changes);
    let paragraph = records
        .iter_mut()
        .find(|record| {
            let wire: Value = serde_json::from_slice(&record.snapshot).unwrap();
            wire["kind"] == "paragraph"
                && wire["payload_json"]
                    .as_str()
                    .is_some_and(|payload| payload.contains("Before"))
        })
        .expect("the Before paragraph exists");
    let mut wire: Value = serde_json::from_slice(&paragraph.snapshot).unwrap();
    let mut payload: Value = serde_json::from_str(wire["payload_json"].as_str().unwrap()).unwrap();
    payload["inline"] = serde_json::json!([{"type":"text","value":"After"}]);
    wire["payload_json"] = serde_json::to_string(&payload).unwrap().into();
    paragraph.snapshot = serde_json::to_vec(&wire).unwrap();

    let (cold, edits) = Document::open_entities(records, None).unwrap();
    assert_eq!(cold.accepted_bytes(), b"After\n\nUntouched");
    assert_eq!(edits.len(), 1);
    assert_eq!(edits[0].insert.as_slice(), b"After\n\nUntouched");
}

#[test]
fn localized_text_edit_emits_one_sparse_complete_entity_upsert() {
    let before = b"Before\n\nUntouched\n".to_vec();
    let (document, _) = Document::open_file(
        before.clone(),
        Some("sparse.md"),
        IdNamespace::from_halves(1, 2),
    )
    .unwrap();
    let offset = before
        .windows(b"Before".len())
        .position(|window| window == b"Before")
        .unwrap();
    let (after, changes) = document
        .file_changed(
            &[InputSplice {
                offset: u64::try_from(offset).unwrap(),
                delete_len: u64::try_from("Before".len()).unwrap(),
                insert: b"After",
            }],
            IdNamespace::from_halves(3, 4),
        )
        .unwrap();
    assert_eq!(changes.len(), 1, "{changes:#?}");
    assert_eq!(changes[0].schema_key, NODE_SCHEMA_KEY);
    assert_eq!(changes[0].effect, ChangeEffect::Content);
    assert_eq!(after.accepted_bytes(), b"After\n\nUntouched\n");
}

#[test]
fn incremental_paragraph_edit_preserves_unrelated_entities_after_cold_reopen() {
    let source = b"# Title\n\nAlpha paragraph\n\nBravo paragraph\n\nCharlie paragraph\n".to_vec();
    let (_, initial) = Document::open_file(
        source.clone(),
        Some("incremental.md"),
        IdNamespace::from_halves(1, 2),
    )
    .unwrap();
    let initial_records = records(&initial);
    let initial_ids = initial_records
        .iter()
        .map(|record| record.entity_pk.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let (document, _) = Document::open_entities(initial_records, None).unwrap();
    let offset = source
        .windows(b"Bravo".len())
        .position(|window| window == b"Bravo")
        .unwrap();

    let (after, changes) = document
        .file_changed(
            &[InputSplice {
                offset: u64::try_from(offset).unwrap(),
                delete_len: 1,
                insert: b"G",
            }],
            IdNamespace::from_halves(3, 4),
        )
        .unwrap();

    assert_eq!(
        after.accepted_bytes(),
        b"# Title\n\nAlpha paragraph\n\nGravo paragraph\n\nCharlie paragraph\n"
    );
    assert_eq!(changes.len(), 1, "{changes:#?}");
    assert!(changes[0].snapshot.is_some());
    assert!(initial_ids.contains(&changes[0].entity_pk));
}

#[test]
fn repeated_sparse_edits_share_history_without_losing_prior_successors() {
    let source = b"Alpha paragraph\n\nBravo paragraph\n\nCharlie paragraph\n".to_vec();
    let (document, _) = Document::open_file(
        source.clone(),
        Some("successors.md"),
        IdNamespace::from_halves(1, 2),
    )
    .unwrap();
    let original = document.fork();

    let alpha_offset = source
        .windows(b"Alpha".len())
        .position(|window| window == b"Alpha")
        .unwrap();
    let (after_alpha, alpha_changes) = document
        .file_changed(
            &[InputSplice {
                offset: u64::try_from(alpha_offset).unwrap(),
                delete_len: 1,
                insert: b"O",
            }],
            IdNamespace::from_halves(3, 4),
        )
        .unwrap();
    assert_eq!(alpha_changes.len(), 1);

    let charlie_offset = source
        .windows(b"Charlie".len())
        .position(|window| window == b"Charlie")
        .unwrap();
    let (after_charlie, charlie_changes) = after_alpha
        .file_changed(
            &[InputSplice {
                offset: u64::try_from(charlie_offset).unwrap(),
                delete_len: 1,
                insert: b"K",
            }],
            IdNamespace::from_halves(5, 6),
        )
        .unwrap();

    assert_eq!(charlie_changes.len(), 1);
    assert_eq!(
        after_charlie.accepted_bytes(),
        b"Olpha paragraph\n\nBravo paragraph\n\nKharlie paragraph\n"
    );
    assert_eq!(
        after_alpha.accepted_bytes(),
        b"Olpha paragraph\n\nBravo paragraph\n\nCharlie paragraph\n"
    );
    assert_eq!(original.accepted_bytes(), source);
}

#[test]
fn entity_edit_returns_a_minimal_file_splice_and_preserves_old_fork() {
    let source = b"Before\n\nUntouched\n".to_vec();
    let (document, initial) =
        Document::open_file(source.clone(), Some("edit.md"), IdNamespace::default()).unwrap();
    let old = document.fork();
    let paragraph = initial
        .iter()
        .find(|change| {
            change.snapshot.as_ref().is_some_and(|snapshot| {
                let wire: Value = serde_json::from_slice(snapshot).unwrap();
                wire["kind"] == "paragraph"
                    && wire["payload_json"]
                        .as_str()
                        .is_some_and(|payload| payload.contains("Before"))
            })
        })
        .unwrap();
    let mut wire: Value = serde_json::from_slice(paragraph.snapshot.as_ref().unwrap()).unwrap();
    let mut payload: Value = serde_json::from_str(wire["payload_json"].as_str().unwrap()).unwrap();
    payload["inline"] = serde_json::json!([{"type":"text","value":"After"}]);
    wire["payload_json"] = serde_json::to_string(&payload).unwrap().into();
    let (after, edits) = document
        .entities_changed(vec![EntityChange {
            schema_key: NODE_SCHEMA_KEY.to_owned(),
            entity_pk: paragraph.entity_pk.clone(),
            snapshot: Some(serde_json::to_vec(&wire).unwrap()),
            effect: ChangeEffect::Content,
        }])
        .unwrap();
    assert_eq!(edits.len(), 1);
    assert_eq!(edits[0].offset, 0);
    assert_eq!(edits[0].delete_len, u64::try_from("Before".len()).unwrap());
    assert_eq!(edits[0].insert.as_slice(), b"After");
    assert_eq!(after.accepted_bytes(), b"After\n\nUntouched\n");
    assert_eq!(old.accepted_bytes(), source);
}

#[test]
fn canonical_literal_prose_fast_path_matches_forced_fallback_bytes_and_entities() {
    let cases = [
        "Café, naïve prose — with commas, semicolons; parentheses (like these), and a question?\n\nSecond paragraph has 42% ordinary prose.\n",
        "Unicode punctuation… “quoted prose” stays literal; so do apostrophes.\n\nA second plain paragraph, too.\n",
    ];

    for (index, source) in cases.into_iter().enumerate() {
        let parsed = parse_markdown_source(source).expect("literal prose should parse");
        assert!(
            parsed.canonical_literal_paragraph_layout,
            "{source:?} should meet the strict literal-prose predicate"
        );
        assert_eq!(parsed.canonical_render.as_deref(), Some(source.as_bytes()));

        let namespace = IdNamespace::from_halves(12, u32::try_from(index).unwrap());
        let (fast_document, fast_changes) =
            Document::open_file(source.as_bytes().to_vec(), Some("prose.md"), namespace)
                .expect("fast-path document should open");
        let (fallback_document, fallback_changes) = Document::open_file_forced_canonical_fallback(
            source.as_bytes().to_vec(),
            Some("prose.md"),
            namespace,
        )
        .expect("forced-fallback document should open");

        assert_eq!(fast_document.accepted_bytes(), source.as_bytes());
        assert_eq!(fallback_document.accepted_bytes(), source.as_bytes());
        assert_eq!(fast_changes, fallback_changes);
    }
}

#[test]
fn literal_prose_fast_path_rejects_markdown_syntax_and_noncanonical_layout() {
    let cases = [
        ("heading", "# Heading\n\nPlain prose.\n"),
        ("inline syntax", "Plain *emphasis* is Markdown syntax.\n"),
        (
            "soft line break",
            "Plain prose\ncontinues on the next line.\n",
        ),
        (
            "extra blank line",
            "First paragraph.\n\n\nSecond paragraph.\n",
        ),
        (
            "trailing spaces",
            "First paragraph.  \n\nSecond paragraph.\n",
        ),
        ("crlf", "First paragraph.\r\n\r\nSecond paragraph.\r\n"),
        (
            "unsafe literal punctuation",
            "A [literal] bracket needs serializer escaping.\n",
        ),
    ];

    for (name, source) in cases {
        let parsed = parse_markdown_source(source)
            .unwrap_or_else(|error| panic!("{name} should remain valid Markdown: {error:?}"));
        assert!(
            !parsed.canonical_literal_paragraph_layout,
            "{name} must use the existing canonical-render fallback"
        );
    }
}
