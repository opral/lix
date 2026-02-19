mod common;

use common::{
    assert_invalid_input, file_from_markdown, is_block_change, is_document_change,
    parse_document_order,
};
use plugin_md_v2::{
    detect_changes, detect_changes_with_state_context, PluginDetectStateContext, BLOCK_SCHEMA_KEY,
    DOCUMENT_SCHEMA_KEY, SCHEMA_VERSION,
};
use std::collections::BTreeSet;

fn count_tombstones(changes: &[plugin_md_v2::PluginEntityChange]) -> usize {
    changes
        .iter()
        .filter(|change| change.schema_key == BLOCK_SCHEMA_KEY && change.snapshot_content.is_none())
        .count()
}

fn count_upserts(changes: &[plugin_md_v2::PluginEntityChange]) -> usize {
    changes
        .iter()
        .filter(|change| change.schema_key == BLOCK_SCHEMA_KEY && change.snapshot_content.is_some())
        .count()
}

fn count_document_rows(changes: &[plugin_md_v2::PluginEntityChange]) -> usize {
    changes
        .iter()
        .filter(|change| change.schema_key == DOCUMENT_SCHEMA_KEY)
        .count()
}

fn upsert_types(changes: &[plugin_md_v2::PluginEntityChange]) -> Vec<String> {
    changes
        .iter()
        .filter(|change| change.schema_key == BLOCK_SCHEMA_KEY)
        .filter_map(|change| change.snapshot_content.as_ref())
        .map(|raw| {
            let parsed: serde_json::Value =
                serde_json::from_str(raw).expect("block snapshot should be valid JSON");
            parsed
                .get("type")
                .and_then(serde_json::Value::as_str)
                .expect("block snapshot should contain type")
                .to_string()
        })
        .collect()
}

fn upsert_markdowns(changes: &[plugin_md_v2::PluginEntityChange]) -> Vec<String> {
    changes
        .iter()
        .filter(|change| change.schema_key == BLOCK_SCHEMA_KEY)
        .filter_map(|change| change.snapshot_content.as_ref())
        .map(|raw| {
            let parsed: serde_json::Value =
                serde_json::from_str(raw).expect("block snapshot should be valid JSON");
            parsed
                .get("markdown")
                .and_then(serde_json::Value::as_str)
                .expect("block snapshot should contain markdown")
                .to_string()
        })
        .collect()
}

fn bootstrap_order(markdown: &str) -> Vec<String> {
    let bootstrap = detect_changes(None, file_from_markdown("bootstrap", "/notes.md", markdown))
        .expect("bootstrap detect_changes should succeed");
    let document = bootstrap
        .iter()
        .find(|change| is_document_change(change))
        .expect("bootstrap should include document row");
    parse_document_order(document)
}

fn document_order_from_changes(
    changes: &[plugin_md_v2::PluginEntityChange],
) -> Option<Vec<String>> {
    changes
        .iter()
        .find(|change| is_document_change(change))
        .map(parse_document_order)
}

fn tombstone_ids(changes: &[plugin_md_v2::PluginEntityChange]) -> Vec<String> {
    changes
        .iter()
        .filter(|change| change.schema_key == BLOCK_SCHEMA_KEY && change.snapshot_content.is_none())
        .map(|change| change.entity_id.clone())
        .collect()
}

fn upsert_ids(changes: &[plugin_md_v2::PluginEntityChange]) -> Vec<String> {
    changes
        .iter()
        .filter(|change| change.schema_key == BLOCK_SCHEMA_KEY && change.snapshot_content.is_some())
        .map(|change| change.entity_id.clone())
        .collect()
}

fn state_context_from_rows(rows: &[plugin_md_v2::PluginEntityChange]) -> PluginDetectStateContext {
    PluginDetectStateContext {
        active_state: Some(
            rows.iter()
                .map(|row| plugin_md_v2::PluginActiveStateRow {
                    entity_id: row.entity_id.clone(),
                    schema_key: Some(row.schema_key.clone()),
                    schema_version: Some(row.schema_version.clone()),
                    snapshot_content: row.snapshot_content.clone(),
                    file_id: None,
                    plugin_key: None,
                    version_id: None,
                    change_id: None,
                    metadata: None,
                    created_at: None,
                    updated_at: None,
                })
                .collect::<Vec<_>>(),
        ),
    }
}

fn bootstrap_state(
    markdown: &str,
) -> (
    plugin_md_v2::PluginFile,
    Vec<String>,
    PluginDetectStateContext,
) {
    let before = file_from_markdown("f1", "/notes.md", markdown);
    let bootstrap =
        detect_changes(None, before.clone()).expect("bootstrap detect_changes should succeed");
    let before_order =
        document_order_from_changes(&bootstrap).expect("bootstrap should include document row");
    let state_context = state_context_from_rows(&bootstrap);
    (before, before_order, state_context)
}

fn make_large_markdown_paragraphs(count: usize) -> Vec<String> {
    (1..=count).map(|idx| format!("P{idx}")).collect::<Vec<_>>()
}

#[test]
fn no_changes_when_documents_are_equal() {
    let before = file_from_markdown("f1", "/notes.md", "# Title\n\nSame paragraph.\n");
    let after = file_from_markdown("f1", "/notes.md", "# Title\n\nSame paragraph.\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert!(changes.is_empty());
}

#[test]
fn emits_document_and_block_rows_for_new_file() {
    let after = file_from_markdown("f1", "/notes.md", "# Title\n\nParagraph.\n");

    let changes = detect_changes(None, after).expect("detect_changes should succeed");

    let document_rows = changes
        .iter()
        .filter(|change| is_document_change(change))
        .collect::<Vec<_>>();
    let block_rows = changes
        .iter()
        .filter(|change| is_block_change(change))
        .collect::<Vec<_>>();

    assert_eq!(document_rows.len(), 1);
    assert_eq!(block_rows.len(), 2);

    for row in block_rows {
        assert_eq!(row.schema_key, BLOCK_SCHEMA_KEY);
        assert_eq!(row.schema_version, SCHEMA_VERSION);
        assert!(row.snapshot_content.is_some());
    }
}

#[test]
fn handles_empty_documents() {
    let before = file_from_markdown("f1", "/notes.md", "");
    let after = file_from_markdown("f1", "/notes.md", "");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert!(changes.is_empty());
}

#[test]
fn rejects_non_utf8_input() {
    let after = plugin_md_v2::PluginFile {
        id: "f1".to_string(),
        path: "/notes.md".to_string(),
        data: vec![0xFF, 0xFE, 0xFD],
    };

    let error = detect_changes(None, after).expect_err("detect_changes should fail");

    assert_invalid_input(error);
}

#[test]
fn inline_html_br_does_not_drop_changes() {
    let after = file_from_markdown(
        "f1",
        "/notes.md",
        "SSH auth: `git clone git@github.com:microsoft/vscode-docs.git`<br>HTTPS auth: `git clone https://github.com/microsoft/vscode-docs.git`\n",
    );

    let changes = detect_changes(None, after).expect("detect_changes should succeed");

    assert!(
        !changes.is_empty(),
        "inline html <br> in .md should not produce an empty change set"
    );
    assert!(changes.iter().any(is_document_change));
    assert!(changes.iter().any(is_block_change));
}

#[test]
fn move_only_emits_document_row() {
    let before = file_from_markdown("f1", "/notes.md", "First paragraph.\n\nSecond paragraph.\n");
    let after = file_from_markdown("f1", "/notes.md", "Second paragraph.\n\nFirst paragraph.\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert_eq!(changes.len(), 1);
    assert!(changes.iter().all(is_document_change));
}

#[test]
fn move_section_emits_document_row_only() {
    let before = file_from_markdown("f1", "/notes.md", "# A\n\npara a\n\n# B\n\npara b\n");
    let after = file_from_markdown("f1", "/notes.md", "# B\n\npara b\n\n# A\n\npara a\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].schema_key, DOCUMENT_SCHEMA_KEY);
}

#[test]
fn cross_type_paragraph_to_heading_emits_delete_add_and_document_update() {
    let before = file_from_markdown("f1", "/notes.md", "Hello\n");
    let after = file_from_markdown("f1", "/notes.md", "# Hello\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    let tombstones = changes
        .iter()
        .filter(|change| change.schema_key == BLOCK_SCHEMA_KEY && change.snapshot_content.is_none())
        .count();
    let upserts = changes
        .iter()
        .filter(|change| change.schema_key == BLOCK_SCHEMA_KEY && change.snapshot_content.is_some())
        .count();
    let document_rows = changes
        .iter()
        .filter(|change| change.schema_key == DOCUMENT_SCHEMA_KEY)
        .count();

    assert_eq!(tombstones, 1);
    assert_eq!(upserts, 1);
    assert_eq!(document_rows, 1);
}

#[test]
fn cross_type_code_to_paragraph_emits_delete_add_and_document_update() {
    let before = file_from_markdown("f1", "/notes.md", "```js\nconsole.log(1)\n```\n");
    let after = file_from_markdown("f1", "/notes.md", "console.log(1)\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    let tombstones = changes
        .iter()
        .filter(|change| change.schema_key == BLOCK_SCHEMA_KEY && change.snapshot_content.is_none())
        .count();
    let upserts = changes
        .iter()
        .filter(|change| change.schema_key == BLOCK_SCHEMA_KEY && change.snapshot_content.is_some())
        .count();
    let document_rows = changes
        .iter()
        .filter(|change| change.schema_key == DOCUMENT_SCHEMA_KEY)
        .count();

    assert_eq!(tombstones, 1);
    assert_eq!(upserts, 1);
    assert_eq!(document_rows, 1);
}

#[test]
fn duplicate_paragraphs_with_no_text_change_emit_no_changes() {
    let before = file_from_markdown("f1", "/notes.md", "Same\n\nSame\n");
    let after = file_from_markdown("f1", "/notes.md", "Same\n\nSame\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert!(changes.is_empty());
}

#[test]
fn insert_duplicate_paragraph_emits_new_block_and_document_update() {
    let before = file_from_markdown("f1", "/notes.md", "Same\n\nOther\n");
    let after = file_from_markdown("f1", "/notes.md", "Same\n\nSame\n\nOther\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    let tombstones = changes
        .iter()
        .filter(|change| change.schema_key == BLOCK_SCHEMA_KEY && change.snapshot_content.is_none())
        .count();
    let upserts = changes
        .iter()
        .filter(|change| change.schema_key == BLOCK_SCHEMA_KEY && change.snapshot_content.is_some())
        .count();
    let document_rows = changes
        .iter()
        .filter(|change| change.schema_key == DOCUMENT_SCHEMA_KEY)
        .count();

    assert_eq!(tombstones, 0);
    assert_eq!(upserts, 1);
    assert_eq!(document_rows, 1);
}

#[test]
fn crlf_vs_lf_normalization_emits_no_changes() {
    let before = file_from_markdown("f1", "/notes.md", "Line A\r\n\r\nLine B\r\n");
    let after = file_from_markdown("f1", "/notes.md", "Line A\n\nLine B\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert!(changes.is_empty());
}

#[test]
fn unicode_nfc_vs_nfd_emits_no_changes() {
    let before = file_from_markdown("f1", "/notes.md", "caf\u{00E9}\n");
    let after = file_from_markdown("f1", "/notes.md", "caf\u{0065}\u{0301}\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert!(changes.is_empty());
}

#[test]
fn large_doc_pure_shuffle_emits_document_row_only() {
    let paragraphs = (0..140).map(|idx| format!("P{idx}")).collect::<Vec<_>>();
    let before_markdown = paragraphs.join("\n\n") + "\n";

    let mut after = paragraphs.clone();
    after.rotate_left(37);
    let after_markdown = after.join("\n\n") + "\n";

    let changes = detect_changes(
        Some(file_from_markdown("f1", "/notes.md", &before_markdown)),
        file_from_markdown("f1", "/notes.md", &after_markdown),
    )
    .expect("detect_changes should succeed");

    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].schema_key, DOCUMENT_SCHEMA_KEY);
}

#[test]
fn paragraph_to_blockquote_emits_delete_add_and_document_update() {
    let before = file_from_markdown("f1", "/notes.md", "Hello\n");
    let after = file_from_markdown("f1", "/notes.md", "> Hello\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert_eq!(count_tombstones(&changes), 1);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 1);
    assert_eq!(upsert_types(&changes), vec!["blockquote".to_string()]);
}

#[test]
fn hard_break_variant_does_not_introduce_extra_blocks() {
    let before = file_from_markdown("f1", "/notes.md", "line  \r\nbreak\r\n");
    let after = file_from_markdown("f1", "/notes.md", "line\\\r\nbreak\r\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    if changes.is_empty() {
        return;
    }

    assert_eq!(count_tombstones(&changes), 1);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 1);
    assert_eq!(upsert_types(&changes), vec!["paragraph".to_string()]);
}

#[test]
fn code_fence_length_variation_does_not_introduce_new_id() {
    let before = file_from_markdown("f1", "/notes.md", "```js\nconsole.log(1)\n```\n");
    let after = file_from_markdown("f1", "/notes.md", "````js\nconsole.log(1)\n````\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    if changes.is_empty() {
        return;
    }

    assert_eq!(count_tombstones(&changes), 1);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 1);
    assert_eq!(upsert_types(&changes), vec!["code".to_string()]);
}

#[test]
fn id_stability_pure_reorder_preserves_existing_ids() {
    let before_markdown = "First\n\nSecond\n";
    let before_order = bootstrap_order(before_markdown);
    assert_eq!(before_order.len(), 2);

    let changes = detect_changes(
        Some(file_from_markdown("f1", "/notes.md", before_markdown)),
        file_from_markdown("f1", "/notes.md", "Second\n\nFirst\n"),
    )
    .expect("detect_changes should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 0);
    assert_eq!(count_document_rows(&changes), 1);

    let after_order =
        document_order_from_changes(&changes).expect("reorder should include document row");
    assert_eq!(
        after_order,
        vec![before_order[1].clone(), before_order[0].clone()]
    );
}

#[test]
fn id_stability_insert_between_keeps_neighbors_and_mints_new_id() {
    let before_markdown = "A\n\nC\n";
    let before_order = bootstrap_order(before_markdown);
    assert_eq!(before_order.len(), 2);

    let changes = detect_changes(
        Some(file_from_markdown("f1", "/notes.md", before_markdown)),
        file_from_markdown("f1", "/notes.md", "A\n\nB\n\nC\n"),
    )
    .expect("detect_changes should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 1);

    let after_order =
        document_order_from_changes(&changes).expect("insert should include document row");
    assert_eq!(after_order[0], before_order[0]);
    assert_eq!(after_order[2], before_order[1]);
    assert_ne!(after_order[1], before_order[0]);
    assert_ne!(after_order[1], before_order[1]);
    assert_eq!(upsert_ids(&changes), vec![after_order[1].clone()]);
}

#[test]
fn id_stability_delete_keeps_survivor_id_and_tombstones_deleted() {
    let before_markdown = "Keep me\n\nDelete me\n";
    let before_order = bootstrap_order(before_markdown);
    assert_eq!(before_order.len(), 2);

    let changes = detect_changes(
        Some(file_from_markdown("f1", "/notes.md", before_markdown)),
        file_from_markdown("f1", "/notes.md", "Keep me\n"),
    )
    .expect("detect_changes should succeed");

    assert_eq!(count_tombstones(&changes), 1);
    assert_eq!(count_upserts(&changes), 0);
    assert_eq!(count_document_rows(&changes), 1);
    assert_eq!(tombstone_ids(&changes), vec![before_order[1].clone()]);
    assert_eq!(
        document_order_from_changes(&changes).expect("delete should include document row"),
        vec![before_order[0].clone()]
    );
}

#[test]
fn id_stability_cross_type_does_not_reuse_old_id() {
    let before_markdown = "Hello\n";
    let before_order = bootstrap_order(before_markdown);
    assert_eq!(before_order.len(), 1);

    let changes = detect_changes(
        Some(file_from_markdown("f1", "/notes.md", before_markdown)),
        file_from_markdown("f1", "/notes.md", "# Hello\n"),
    )
    .expect("detect_changes should succeed");

    assert_eq!(count_tombstones(&changes), 1);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 1);
    assert_eq!(tombstone_ids(&changes), vec![before_order[0].clone()]);

    let upserts = upsert_ids(&changes);
    assert_eq!(upserts.len(), 1);
    assert_ne!(upserts[0], before_order[0]);

    let after_order = document_order_from_changes(&changes).expect("should include doc row");
    assert_eq!(after_order, upserts);
}

#[test]
fn id_stability_large_pure_shuffle_preserves_id_set() {
    let paragraphs = (1..=500).map(|idx| format!("P{idx}")).collect::<Vec<_>>();
    let before_markdown = paragraphs.join("\n\n") + "\n";
    let before_order = bootstrap_order(&before_markdown);
    assert_eq!(before_order.len(), 500);

    let mut after = paragraphs.clone();
    after.rotate_left(123);
    let after_markdown = after.join("\n\n") + "\n";

    let changes = detect_changes(
        Some(file_from_markdown("f1", "/notes.md", &before_markdown)),
        file_from_markdown("f1", "/notes.md", &after_markdown),
    )
    .expect("detect_changes should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 0);
    assert_eq!(count_document_rows(&changes), 1);

    let after_order = document_order_from_changes(&changes).expect("shuffle should include doc");
    let before_set = before_order.into_iter().collect::<BTreeSet<_>>();
    let after_set = after_order.into_iter().collect::<BTreeSet<_>>();
    assert_eq!(before_set, after_set);
}

#[test]
fn with_state_context_paragraph_edit_reuses_existing_id_without_tombstone() {
    let before = file_from_markdown("f1", "/notes.md", "Hello\n\nWorld\n");
    let bootstrap =
        detect_changes(None, before.clone()).expect("bootstrap detect_changes should succeed");
    let before_order = bootstrap_order("Hello\n\nWorld\n");
    let state_context = state_context_from_rows(&bootstrap);

    let changes = detect_changes_with_state_context(
        Some(before),
        file_from_markdown("f1", "/notes.md", "Hello updated\n\nWorld\n"),
        Some(state_context),
    )
    .expect("detect_changes_with_state_context should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 0);
    assert_eq!(upsert_ids(&changes), vec![before_order[0].clone()]);
}

#[test]
fn with_state_context_move_and_edit_reuses_existing_id_and_updates_order() {
    let before_markdown = "Alpha\n\nBeta\n";
    let before = file_from_markdown("f1", "/notes.md", before_markdown);
    let bootstrap =
        detect_changes(None, before.clone()).expect("bootstrap detect_changes should succeed");
    let before_order = bootstrap_order(before_markdown);
    let state_context = state_context_from_rows(&bootstrap);

    let changes = detect_changes_with_state_context(
        Some(before),
        file_from_markdown("f1", "/notes.md", "Beta plus\n\nAlpha\n"),
        Some(state_context),
    )
    .expect("detect_changes_with_state_context should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 1);
    assert_eq!(upsert_ids(&changes), vec![before_order[1].clone()]);
    assert_eq!(
        document_order_from_changes(&changes).expect("document row should be present"),
        vec![before_order[1].clone(), before_order[0].clone()]
    );
}

#[test]
fn with_state_context_insert_between_preserves_neighbor_ids_and_mints_new_id() {
    let before_markdown = "A\n\nC\n";
    let before = file_from_markdown("f1", "/notes.md", before_markdown);
    let bootstrap =
        detect_changes(None, before.clone()).expect("bootstrap detect_changes should succeed");
    let before_order = bootstrap_order(before_markdown);
    let state_context = state_context_from_rows(&bootstrap);

    let changes = detect_changes_with_state_context(
        Some(before),
        file_from_markdown("f1", "/notes.md", "A\n\nB\n\nC\n"),
        Some(state_context),
    )
    .expect("detect_changes_with_state_context should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 1);

    let order = document_order_from_changes(&changes).expect("document row should be present");
    assert_eq!(order[0], before_order[0]);
    assert_eq!(order[2], before_order[1]);
    assert_ne!(order[1], before_order[0]);
    assert_ne!(order[1], before_order[1]);
    assert_eq!(upsert_ids(&changes), vec![order[1].clone()]);
}

#[test]
fn with_state_context_pure_reorder_emits_only_document_row() {
    let (before, before_order, state_context) = bootstrap_state("First\n\nSecond\n");
    assert_eq!(before_order.len(), 2);

    let changes = detect_changes_with_state_context(
        Some(before),
        file_from_markdown("f1", "/notes.md", "Second\n\nFirst\n"),
        Some(state_context),
    )
    .expect("detect_changes_with_state_context should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 0);
    assert_eq!(count_document_rows(&changes), 1);
    assert_eq!(
        document_order_from_changes(&changes).expect("document row should be present"),
        vec![before_order[1].clone(), before_order[0].clone()]
    );
}

#[test]
fn with_state_context_move_section_emits_only_document_row() {
    let (before, before_order, state_context) = bootstrap_state("# A\n\nPara A\n\n# B\n\nPara B\n");
    assert_eq!(before_order.len(), 4);

    let changes = detect_changes_with_state_context(
        Some(before),
        file_from_markdown("f1", "/notes.md", "# B\n\nPara B\n\n# A\n\nPara A\n"),
        Some(state_context),
    )
    .expect("detect_changes_with_state_context should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 0);
    assert_eq!(count_document_rows(&changes), 1);
    assert_eq!(
        document_order_from_changes(&changes).expect("document row should be present"),
        vec![
            before_order[2].clone(),
            before_order[3].clone(),
            before_order[0].clone(),
            before_order[1].clone(),
        ]
    );
}

#[test]
fn with_state_context_large_shuffle_500_emits_only_document_row() {
    let paragraphs = (1..=500).map(|idx| format!("P{idx}")).collect::<Vec<_>>();
    let before_markdown = paragraphs.join("\n\n") + "\n";
    let (before, before_order, state_context) = bootstrap_state(&before_markdown);
    assert_eq!(before_order.len(), 500);

    let mut after = paragraphs;
    after.rotate_left(123);
    let after_markdown = after.join("\n\n") + "\n";

    let changes = detect_changes_with_state_context(
        Some(before),
        file_from_markdown("f1", "/notes.md", &after_markdown),
        Some(state_context),
    )
    .expect("detect_changes_with_state_context should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 0);
    assert_eq!(count_document_rows(&changes), 1);

    let after_order =
        document_order_from_changes(&changes).expect("document row should be present");
    let before_set = before_order.into_iter().collect::<BTreeSet<_>>();
    let after_set = after_order.into_iter().collect::<BTreeSet<_>>();
    assert_eq!(before_set, after_set);
}

#[test]
fn with_state_context_duplicate_edit_second_preserves_first_id_without_document_noise() {
    let (before, before_order, state_context) = bootstrap_state("Same\n\nSame\n");
    assert_eq!(before_order.len(), 2);

    let changes = detect_changes_with_state_context(
        Some(before),
        file_from_markdown("f1", "/notes.md", "Same\n\nSame updated\n"),
        Some(state_context),
    )
    .expect("detect_changes_with_state_context should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 0);
    assert_eq!(upsert_ids(&changes), vec![before_order[1].clone()]);
}

#[test]
fn with_state_context_duplicate_middle_edit_targets_only_middle_entity() {
    let (before, before_order, state_context) = bootstrap_state("Same\n\nSame\n\nSame\n");
    assert_eq!(before_order.len(), 3);

    let changes = detect_changes_with_state_context(
        Some(before),
        file_from_markdown("f1", "/notes.md", "Same\n\nSame updated\n\nSame\n"),
        Some(state_context),
    )
    .expect("detect_changes_with_state_context should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 0);
    assert_eq!(upsert_ids(&changes), vec![before_order[1].clone()]);
}

#[test]
fn with_state_context_list_reorder_emits_single_list_upsert_without_document_row() {
    let (before, before_order, state_context) = bootstrap_state("- one\n- two\n- three\n");
    assert_eq!(before_order.len(), 1);

    let changes = detect_changes_with_state_context(
        Some(before),
        file_from_markdown("f1", "/notes.md", "- three\n- one\n- two\n"),
        Some(state_context),
    )
    .expect("detect_changes_with_state_context should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 0);
    assert_eq!(upsert_types(&changes), vec!["list".to_string()]);
    assert_eq!(upsert_ids(&changes), vec![before_order[0].clone()]);
}

#[test]
fn with_state_context_list_add_item_emits_single_list_upsert_without_document_row() {
    let (before, before_order, state_context) = bootstrap_state("- one\n- two\n");
    assert_eq!(before_order.len(), 1);

    let changes = detect_changes_with_state_context(
        Some(before),
        file_from_markdown("f1", "/notes.md", "- one\n- two\n- three\n"),
        Some(state_context),
    )
    .expect("detect_changes_with_state_context should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 0);
    assert_eq!(upsert_types(&changes), vec!["list".to_string()]);
    assert_eq!(upsert_ids(&changes), vec![before_order[0].clone()]);
}

#[test]
fn with_state_context_list_remove_item_emits_single_list_upsert_without_document_row() {
    let (before, before_order, state_context) = bootstrap_state("- one\n- two\n- three\n");
    assert_eq!(before_order.len(), 1);

    let changes = detect_changes_with_state_context(
        Some(before),
        file_from_markdown("f1", "/notes.md", "- one\n- three\n"),
        Some(state_context),
    )
    .expect("detect_changes_with_state_context should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 0);
    assert_eq!(upsert_types(&changes), vec!["list".to_string()]);
    assert_eq!(upsert_ids(&changes), vec![before_order[0].clone()]);
}

#[test]
fn with_state_context_table_reorder_rows_emits_single_table_upsert_without_document_row() {
    let (before, before_order, state_context) =
        bootstrap_state("| a | b |\n| - | - |\n| 1 | 2 |\n| 3 | 4 |\n| 5 | 6 |\n");
    assert_eq!(before_order.len(), 1);

    let changes = detect_changes_with_state_context(
        Some(before),
        file_from_markdown(
            "f1",
            "/notes.md",
            "| a | b |\n| - | - |\n| 3 | 4 |\n| 5 | 6 |\n| 1 | 2 |\n",
        ),
        Some(state_context),
    )
    .expect("detect_changes_with_state_context should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 0);
    assert_eq!(upsert_types(&changes), vec!["table".to_string()]);
    assert_eq!(upsert_ids(&changes), vec![before_order[0].clone()]);
}

#[test]
fn with_state_context_table_add_row_emits_single_table_upsert_without_document_row() {
    let (before, before_order, state_context) =
        bootstrap_state("| a | b |\n| - | - |\n| 1 | 2 |\n");
    assert_eq!(before_order.len(), 1);

    let changes = detect_changes_with_state_context(
        Some(before),
        file_from_markdown(
            "f1",
            "/notes.md",
            "| a | b |\n| - | - |\n| 1 | 2 |\n| 3 | 4 |\n",
        ),
        Some(state_context),
    )
    .expect("detect_changes_with_state_context should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 0);
    assert_eq!(upsert_types(&changes), vec!["table".to_string()]);
    assert_eq!(upsert_ids(&changes), vec![before_order[0].clone()]);
}

#[test]
fn with_state_context_table_remove_row_emits_single_table_upsert_without_document_row() {
    let (before, before_order, state_context) =
        bootstrap_state("| a | b |\n| - | - |\n| 1 | 2 |\n| 3 | 4 |\n");
    assert_eq!(before_order.len(), 1);

    let changes = detect_changes_with_state_context(
        Some(before),
        file_from_markdown("f1", "/notes.md", "| a | b |\n| - | - |\n| 1 | 2 |\n"),
        Some(state_context),
    )
    .expect("detect_changes_with_state_context should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 0);
    assert_eq!(upsert_types(&changes), vec!["table".to_string()]);
    assert_eq!(upsert_ids(&changes), vec![before_order[0].clone()]);
}

#[test]
fn with_state_context_heading_edit_reuses_existing_id_without_document_row() {
    let (before, before_order, state_context) = bootstrap_state("# Hello\n\nBody\n");
    assert_eq!(before_order.len(), 2);

    let changes = detect_changes_with_state_context(
        Some(before),
        file_from_markdown("f1", "/notes.md", "# Hello World\n\nBody\n"),
        Some(state_context),
    )
    .expect("detect_changes_with_state_context should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 0);
    assert_eq!(upsert_types(&changes), vec!["heading".to_string()]);
    assert_eq!(upsert_ids(&changes), vec![before_order[0].clone()]);
}

#[test]
fn with_state_context_code_edit_reuses_existing_id_without_document_row() {
    let (before, before_order, state_context) = bootstrap_state("```js\nconsole.log(1)\n```\n");
    assert_eq!(before_order.len(), 1);

    let changes = detect_changes_with_state_context(
        Some(before),
        file_from_markdown("f1", "/notes.md", "```js\nconsole.log(2)\n```\n"),
        Some(state_context),
    )
    .expect("detect_changes_with_state_context should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 0);
    assert_eq!(upsert_types(&changes), vec!["code".to_string()]);
    assert_eq!(upsert_ids(&changes), vec![before_order[0].clone()]);
}

#[test]
fn with_state_context_link_text_edit_reuses_existing_id_without_document_row() {
    let (before, before_order, state_context) = bootstrap_state("[text](https://example.com)\n");
    assert_eq!(before_order.len(), 1);

    let changes = detect_changes_with_state_context(
        Some(before),
        file_from_markdown("f1", "/notes.md", "[new](https://example.com)\n"),
        Some(state_context),
    )
    .expect("detect_changes_with_state_context should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 0);
    assert_eq!(upsert_types(&changes), vec!["paragraph".to_string()]);
    assert_eq!(upsert_ids(&changes), vec![before_order[0].clone()]);
}

#[test]
fn with_state_context_link_url_edit_reuses_existing_id_without_document_row() {
    let (before, before_order, state_context) = bootstrap_state("[text](https://example.com)\n");
    assert_eq!(before_order.len(), 1);

    let changes = detect_changes_with_state_context(
        Some(before),
        file_from_markdown("f1", "/notes.md", "[text](https://example.org)\n"),
        Some(state_context),
    )
    .expect("detect_changes_with_state_context should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 0);
    assert_eq!(upsert_types(&changes), vec!["paragraph".to_string()]);
    assert_eq!(upsert_ids(&changes), vec![before_order[0].clone()]);
}

#[test]
fn with_state_context_paragraph_split_reuses_first_id_and_mints_one_new() {
    let (before, before_order, state_context) = bootstrap_state("AB\n");
    assert_eq!(before_order.len(), 1);

    let changes = detect_changes_with_state_context(
        Some(before),
        file_from_markdown("f1", "/notes.md", "A\n\nB\n"),
        Some(state_context),
    )
    .expect("detect_changes_with_state_context should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 2);
    assert_eq!(count_document_rows(&changes), 1);

    let upserts = upsert_ids(&changes);
    assert!(upserts.contains(&before_order[0]));
    assert_eq!(
        upserts.iter().filter(|id| **id != before_order[0]).count(),
        1
    );

    let order = document_order_from_changes(&changes).expect("document row should be present");
    assert_eq!(order.len(), 2);
    assert_eq!(order[0], before_order[0]);
    assert_ne!(order[1], before_order[0]);
}

#[test]
fn with_state_context_paragraph_merge_reuses_first_id_and_tombstones_second() {
    let (before, before_order, state_context) = bootstrap_state("A\n\nB\n");
    assert_eq!(before_order.len(), 2);

    let changes = detect_changes_with_state_context(
        Some(before),
        file_from_markdown("f1", "/notes.md", "AB\n"),
        Some(state_context),
    )
    .expect("detect_changes_with_state_context should succeed");

    assert_eq!(count_tombstones(&changes), 1);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 1);
    assert_eq!(tombstone_ids(&changes), vec![before_order[1].clone()]);
    assert_eq!(upsert_ids(&changes), vec![before_order[0].clone()]);
    assert_eq!(
        document_order_from_changes(&changes).expect("document row should be present"),
        vec![before_order[0].clone()]
    );
}

#[test]
fn with_state_context_large_500_tiny_edits_emit_only_targeted_upserts() {
    let paragraphs = make_large_markdown_paragraphs(500);
    let before_markdown = paragraphs.join("\n\n") + "\n";
    let (before, before_order, state_context) = bootstrap_state(&before_markdown);
    assert_eq!(before_order.len(), 500);

    let mut after = paragraphs;
    let edited_indexes = [10usize, 111, 222, 333, 444];
    for index in edited_indexes {
        after[index] = format!("{} x", after[index]);
    }
    let after_markdown = after.join("\n\n") + "\n";

    let changes = detect_changes_with_state_context(
        Some(before),
        file_from_markdown("f1", "/notes.md", &after_markdown),
        Some(state_context),
    )
    .expect("detect_changes_with_state_context should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 5);
    assert_eq!(count_document_rows(&changes), 0);

    let expected_ids = edited_indexes
        .iter()
        .map(|idx| before_order[*idx].clone())
        .collect::<BTreeSet<_>>();
    let actual_ids = upsert_ids(&changes).into_iter().collect::<BTreeSet<_>>();
    assert_eq!(actual_ids, expected_ids);
}

#[test]
fn with_state_context_large_500_delete_insert_move_emits_minimal_noise() {
    let paragraphs = make_large_markdown_paragraphs(500);
    let before_markdown = paragraphs.join("\n\n") + "\n";
    let (before, before_order, state_context) = bootstrap_state(&before_markdown);
    assert_eq!(before_order.len(), 500);

    let moved = paragraphs[450..460].to_vec();
    let mut remaining = paragraphs[..450].to_vec();
    remaining.extend_from_slice(&paragraphs[460..]);
    remaining.retain(|entry| entry != "P500");

    let idx_p300 = remaining
        .iter()
        .position(|entry| entry == "P300")
        .expect("P300 should exist");

    let mut after = Vec::new();
    after.extend(moved);
    after.extend_from_slice(&remaining[..=idx_p300]);
    after.push("PX".to_string());
    after.extend_from_slice(&remaining[idx_p300 + 1..]);
    let after_markdown = after.join("\n\n") + "\n";

    let changes = detect_changes_with_state_context(
        Some(before),
        file_from_markdown("f1", "/notes.md", &after_markdown),
        Some(state_context),
    )
    .expect("detect_changes_with_state_context should succeed");

    let tombstones = count_tombstones(&changes);
    let upserts = count_upserts(&changes);
    assert!(tombstones <= 1);
    assert_eq!(upserts, 1);
    assert_eq!(count_document_rows(&changes), 1);
    assert!(tombstones + upserts <= 2);

    let deleted_id = before_order[499].clone();
    if tombstones == 1 {
        assert_eq!(tombstone_ids(&changes), vec![deleted_id.clone()]);
    }

    let inserted_id = upsert_ids(&changes)
        .into_iter()
        .next()
        .expect("insert should create one upsert");
    if tombstones == 1 {
        assert!(!before_order.contains(&inserted_id));
    } else {
        assert_eq!(inserted_id, deleted_id);
    }
    assert!(upsert_markdowns(&changes)
        .iter()
        .any(|markdown| markdown.contains("PX")));

    let order = document_order_from_changes(&changes).expect("document row should be present");
    assert_eq!(order.len(), 500);
    assert_eq!(order[0..10], before_order[450..460]);
    if tombstones == 1 {
        assert!(!order.contains(&deleted_id));
    } else {
        assert!(order.contains(&deleted_id));
    }
    let idx_300_in_after = order
        .iter()
        .position(|id| id == &before_order[299])
        .expect("P300 id should remain in order");
    assert_eq!(order[idx_300_in_after + 1], inserted_id);
}

#[test]
fn with_state_context_large_duplicates_edit_350_targets_only_matching_id() {
    let before_paragraphs = (0..500).map(|_| "Same".to_string()).collect::<Vec<_>>();
    let before_markdown = before_paragraphs.join("\n\n") + "\n";
    let (before, before_order, state_context) = bootstrap_state(&before_markdown);
    assert_eq!(before_order.len(), 500);

    let mut after = before_paragraphs;
    after[349] = "Same updated".to_string();
    let after_markdown = after.join("\n\n") + "\n";

    let changes = detect_changes_with_state_context(
        Some(before),
        file_from_markdown("f1", "/notes.md", &after_markdown),
        Some(state_context),
    )
    .expect("detect_changes_with_state_context should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 1);
    assert!(count_document_rows(&changes) <= 1);
    assert!(before_order.contains(&upsert_ids(&changes)[0]));

    if let Some(order) = document_order_from_changes(&changes) {
        let before_set = before_order.into_iter().collect::<BTreeSet<_>>();
        let after_set = order.into_iter().collect::<BTreeSet<_>>();
        assert_eq!(before_set, after_set);
    }
}
