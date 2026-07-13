use plugin_md_v2::exports::lix::plugin::api::{EntityState, Guest};
use plugin_md_v2::{File, MarkdownPlugin, NODE_SCHEMA_KEY, PluginError, ROOT_ENTITY_PK};

fn row(id: &str, value: serde_json::Value) -> EntityState {
    EntityState {
        entity_pk: vec![id.to_string()],
        schema_key: NODE_SCHEMA_KEY.to_string(),
        snapshot_content: value.to_string(),
        metadata: None,
    }
}

fn root() -> EntityState {
    row(
        ROOT_ENTITY_PK,
        serde_json::json!({
            "id": ROOT_ENTITY_PK,
            "kind": "document",
            "parent_id": null,
            "order_key": null,
            "payload": {"dialect": "gfm"},
            "format": {"line_ending": "lf", "final_newline": true}
        }),
    )
}

fn paragraph(id: &str, order_key: &str, text: &str) -> EntityState {
    row(
        id,
        serde_json::json!({
            "id": id,
            "kind": "paragraph",
            "parent_id": ROOT_ENTITY_PK,
            "order_key": order_key,
            "payload": {"inline": [{"type": "text", "value": text}]},
            "format": {}
        }),
    )
}

fn assert_invalid(error: PluginError) {
    assert!(matches!(error, PluginError::InvalidInput(_)));
}

#[test]
fn renders_nodes_by_order_key_then_id() {
    let output = MarkdownPlugin::render(vec![
        paragraph("z", "80", "second tie"),
        paragraph("a", "80", "first tie"),
        paragraph("last", "c0", "last"),
        root(),
    ])
    .unwrap();
    assert_eq!(output, b"first tie\n\nsecond tie\n\nlast\n");
}

#[test]
fn rendered_equal_order_key_collisions_detect_as_no_change() {
    let state = vec![
        paragraph("z", "80", "second tie"),
        paragraph("a", "80", "first tie"),
        root(),
    ];
    let output = MarkdownPlugin::render(state.clone()).unwrap();
    let delta = MarkdownPlugin::detect_changes(
        state,
        File {
            filename: Some("collision.md".to_string()),
            data: output,
        },
    )
    .unwrap();
    assert!(delta.is_empty(), "{delta:#?}");
}

#[test]
fn rejects_missing_root() {
    assert_invalid(MarkdownPlugin::render(vec![paragraph("p", "80", "x")]).unwrap_err());
}

#[test]
fn rejects_duplicate_rows_and_id_mismatches() {
    assert_invalid(MarkdownPlugin::render(vec![root(), root()]).unwrap_err());
    let mut mismatch = paragraph("p", "80", "x");
    mismatch.entity_pk = vec!["other".to_string()];
    assert_invalid(MarkdownPlugin::render(vec![root(), mismatch]).unwrap_err());
}

#[test]
fn rejects_orphans_and_invalid_order_keys() {
    let mut orphan = paragraph("p", "80", "x");
    let mut value: serde_json::Value = serde_json::from_str(&orphan.snapshot_content).unwrap();
    value["parent_id"] = serde_json::json!("missing");
    orphan.snapshot_content = value.to_string();
    assert_invalid(MarkdownPlugin::render(vec![root(), orphan]).unwrap_err());

    let invalid = paragraph("p", "not-an-order-key", "x");
    assert_invalid(MarkdownPlugin::render(vec![root(), invalid]).unwrap_err());
}

#[test]
fn ignores_other_plugin_rows_when_root_is_present() {
    let other = EntityState {
        entity_pk: vec!["other".to_string()],
        schema_key: "other_schema".to_string(),
        snapshot_content: "{}".to_string(),
        metadata: None,
    };
    assert_eq!(MarkdownPlugin::render(vec![other, root()]).unwrap(), b"");
}

#[test]
fn rejects_cycles_even_when_every_parent_exists() {
    let mut a = paragraph("a", "80", "a");
    let mut b = paragraph("b", "c0", "b");
    for (row, parent) in [(&mut a, "b"), (&mut b, "a")] {
        let mut value: serde_json::Value = serde_json::from_str(&row.snapshot_content).unwrap();
        value["parent_id"] = serde_json::json!(parent);
        row.snapshot_content = value.to_string();
    }
    assert_invalid(MarkdownPlugin::render(vec![root(), a, b]).unwrap_err());
}

#[test]
fn nested_order_key_collisions_are_deterministic() {
    let mut quote = row(
        "quote",
        serde_json::json!({
            "id": "quote",
            "kind": "block_quote",
            "parent_id": ROOT_ENTITY_PK,
            "order_key": "80",
            "payload": {},
            "format": {}
        }),
    );
    let mut a = paragraph("a", "80", "first");
    let mut z = paragraph("z", "80", "second");
    for child in [&mut a, &mut z] {
        let mut value: serde_json::Value = serde_json::from_str(&child.snapshot_content).unwrap();
        value["parent_id"] = serde_json::json!("quote");
        child.snapshot_content = value.to_string();
    }
    // Keep row construction deliberately out of entity order: rendering is based on
    // (order_key, id), including at nested levels.
    quote.metadata = Some("ignored".to_string());
    assert_eq!(
        MarkdownPlugin::render(vec![z, root(), quote, a]).unwrap(),
        b"> first\n> \n> second\n"
    );
}
