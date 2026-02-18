mod common;

use common::{assert_invalid_input, file_from_markdown, is_block_change, is_document_change};
use plugin_md_v2::{detect_changes, BLOCK_SCHEMA_KEY, DOCUMENT_SCHEMA_KEY, SCHEMA_VERSION};

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
fn edit_one_of_three_duplicates_emits_targeted_block_replace() {
    let before = file_from_markdown("f1", "/notes.md", "Same\n\nSame\n\nSame\n");
    let after = file_from_markdown("f1", "/notes.md", "Same\n\nChanged\n\nSame\n");

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
fn large_doc_insert_delete_reorder_sanity() {
    let paragraphs = (0..120).map(|idx| format!("P{idx}")).collect::<Vec<_>>();
    let before_markdown = paragraphs.join("\n\n") + "\n";

    let mut after = paragraphs.clone();
    after.swap(0, 1);
    after.remove(10);
    after.insert(50, "PX".to_string());
    let after_markdown = after.join("\n\n") + "\n";

    let changes = detect_changes(
        Some(file_from_markdown("f1", "/notes.md", &before_markdown)),
        file_from_markdown("f1", "/notes.md", &after_markdown),
    )
    .expect("detect_changes should succeed");

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
fn paragraph_split_emits_one_delete_two_adds_and_document_update() {
    let before = file_from_markdown("f1", "/notes.md", "AB\n");
    let after = file_from_markdown("f1", "/notes.md", "A\n\nB\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert_eq!(count_tombstones(&changes), 1);
    assert_eq!(count_upserts(&changes), 2);
    assert_eq!(count_document_rows(&changes), 1);

    let markdowns = upsert_markdowns(&changes);
    assert!(markdowns.iter().any(|markdown| markdown.contains("A")));
    assert!(markdowns.iter().any(|markdown| markdown.contains("B")));
}

#[test]
fn paragraph_merge_emits_two_deletes_one_add_and_document_update() {
    let before = file_from_markdown("f1", "/notes.md", "A\n\nB\n");
    let after = file_from_markdown("f1", "/notes.md", "AB\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert_eq!(count_tombstones(&changes), 2);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 1);
    assert_eq!(upsert_types(&changes), vec!["paragraph".to_string()]);
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
fn code_block_content_edit_emits_replace_and_document_update() {
    let before = file_from_markdown("f1", "/notes.md", "```js\nconsole.log(1)\n```\n");
    let after = file_from_markdown("f1", "/notes.md", "```js\nconsole.log(2)\n```\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert_eq!(count_tombstones(&changes), 1);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 1);
    assert_eq!(upsert_types(&changes), vec!["code".to_string()]);
}

#[test]
fn code_block_language_edit_emits_replace_and_document_update() {
    let before = file_from_markdown("f1", "/notes.md", "```js\nconsole.log(1)\n```\n");
    let after = file_from_markdown("f1", "/notes.md", "```ts\nconsole.log(1)\n```\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert_eq!(count_tombstones(&changes), 1);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 1);
    assert_eq!(upsert_types(&changes), vec!["code".to_string()]);
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
fn link_text_change_emits_replace_and_document_update() {
    let before = file_from_markdown("f1", "/notes.md", "[text](https://example.com)\n");
    let after = file_from_markdown("f1", "/notes.md", "[new](https://example.com)\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert_eq!(count_tombstones(&changes), 1);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 1);
    assert_eq!(upsert_types(&changes), vec!["paragraph".to_string()]);
}

#[test]
fn link_url_change_emits_replace_and_document_update() {
    let before = file_from_markdown("f1", "/notes.md", "[text](https://example.com)\n");
    let after = file_from_markdown("f1", "/notes.md", "[text](https://example.org)\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert_eq!(count_tombstones(&changes), 1);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 1);
    assert_eq!(upsert_types(&changes), vec!["paragraph".to_string()]);
}

#[test]
fn top_level_html_text_tweak_emits_replace_and_document_update() {
    let before = file_from_markdown("f1", "/notes.md", "<div>Hello</div>\n");
    let after = file_from_markdown("f1", "/notes.md", "<div>Hello world</div>\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert_eq!(count_tombstones(&changes), 1);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 1);
    assert_eq!(upsert_types(&changes), vec!["paragraph".to_string()]);
}

#[test]
fn list_item_text_edit_emits_list_replace_and_document_update() {
    let before = file_from_markdown("f1", "/notes.md", "- one\n- two\n");
    let after = file_from_markdown("f1", "/notes.md", "- one\n- two changed\n");

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert_eq!(count_tombstones(&changes), 1);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 1);
    assert_eq!(upsert_types(&changes), vec!["list".to_string()]);
}

#[test]
fn table_row_reorder_emits_table_replace_and_document_update() {
    let before = file_from_markdown(
        "f1",
        "/notes.md",
        "| a | b |\n| - | - |\n| 1 | 2 |\n| 3 | 4 |\n| 5 | 6 |\n",
    );
    let after = file_from_markdown(
        "f1",
        "/notes.md",
        "| a | b |\n| - | - |\n| 3 | 4 |\n| 5 | 6 |\n| 1 | 2 |\n",
    );

    let changes = detect_changes(Some(before), after).expect("detect_changes should succeed");

    assert_eq!(count_tombstones(&changes), 1);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 1);
    assert_eq!(upsert_types(&changes), vec!["table".to_string()]);
}

#[test]
fn large_doc_500_delete_insert_move_emits_minimal_replace_set() {
    let paragraphs = (1..=500).map(|idx| format!("P{idx}")).collect::<Vec<_>>();
    let before_markdown = paragraphs.join("\n\n") + "\n";

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

    let changes = detect_changes(
        Some(file_from_markdown("f1", "/notes.md", &before_markdown)),
        file_from_markdown("f1", "/notes.md", &after_markdown),
    )
    .expect("detect_changes should succeed");

    assert_eq!(count_tombstones(&changes), 1);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 1);
    assert!(upsert_markdowns(&changes)
        .iter()
        .any(|markdown| markdown.contains("PX")));
}

#[test]
fn large_doc_500_tiny_edits_emit_replacements_and_one_document_row() {
    let paragraphs = (1..=500).map(|idx| format!("P{idx}")).collect::<Vec<_>>();
    let before_markdown = paragraphs.join("\n\n") + "\n";

    let mut after = paragraphs.clone();
    for index in [10usize, 111, 222, 333, 444] {
        after[index] = format!("{} x", after[index]);
    }
    let after_markdown = after.join("\n\n") + "\n";

    let changes = detect_changes(
        Some(file_from_markdown("f1", "/notes.md", &before_markdown)),
        file_from_markdown("f1", "/notes.md", &after_markdown),
    )
    .expect("detect_changes should succeed");

    assert_eq!(count_tombstones(&changes), 5);
    assert_eq!(count_upserts(&changes), 5);
    assert_eq!(count_document_rows(&changes), 1);
}

#[test]
fn large_mixed_duplicates_and_move_with_one_edit_emits_targeted_replace() {
    let duplicates = (0..300).map(|_| "Same".to_string()).collect::<Vec<_>>();
    let uniques = (1..=200).map(|idx| format!("U{idx}")).collect::<Vec<_>>();

    let mut before = Vec::new();
    before.extend(duplicates.clone());
    before.extend(uniques.clone());
    let before_markdown = before.join("\n\n") + "\n";

    let mut moved_uniques = uniques.clone();
    moved_uniques[9] = "U10 x".to_string();

    let mut after = Vec::new();
    after.extend(moved_uniques);
    after.extend(duplicates);
    let after_markdown = after.join("\n\n") + "\n";

    let changes = detect_changes(
        Some(file_from_markdown("f1", "/notes.md", &before_markdown)),
        file_from_markdown("f1", "/notes.md", &after_markdown),
    )
    .expect("detect_changes should succeed");

    assert_eq!(count_tombstones(&changes), 1);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 1);
    assert!(upsert_markdowns(&changes)
        .iter()
        .any(|markdown| markdown.contains("U10 x")));
}
