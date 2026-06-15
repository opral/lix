use plugin_md_v2::exports::lix::plugin::api::{EntityState, Guest};
use plugin_md_v2::{
    BLOCK_SCHEMA_KEY, DOCUMENT_SCHEMA_KEY, DetectedChange, File, MarkdownPlugin, ROOT_ENTITY_PK,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

fn file_from_markdown(markdown: &str) -> File {
    File {
        filename: None,
        data: markdown.as_bytes().to_vec(),
    }
}

fn detect_from_files(before: Option<&str>, after: &str) -> Vec<DetectedChange> {
    let state = before
        .map(|before| active_state_from_file(file_from_markdown(before)))
        .unwrap_or_default();
    MarkdownPlugin::detect_changes(state, file_from_markdown(after))
        .expect("detect_changes should succeed")
}

fn active_state_from_file(file: File) -> Vec<EntityState> {
    apply_changes_to_active_state(
        Vec::new(),
        MarkdownPlugin::detect_changes(Vec::new(), file).expect("bootstrap should succeed"),
    )
}

fn apply_changes_to_active_state(
    active_state: Vec<EntityState>,
    changes: Vec<DetectedChange>,
) -> Vec<EntityState> {
    let mut rows = active_state
        .into_iter()
        .map(|row| ((row.schema_key.clone(), row.entity_pk.clone()), row))
        .collect::<BTreeMap<_, _>>();

    for change in changes {
        let key = (change.schema_key.clone(), change.entity_pk.clone());
        if let Some(snapshot_content) = change.snapshot_content {
            rows.insert(
                key,
                EntityState {
                    entity_pk: change.entity_pk,
                    schema_key: change.schema_key,
                    snapshot_content,
                    metadata: change.metadata,
                },
            );
        } else {
            rows.remove(&key);
        }
    }

    rows.into_values().collect()
}

fn block_rows(state: &[EntityState]) -> Vec<&EntityState> {
    state
        .iter()
        .filter(|row| row.schema_key == BLOCK_SCHEMA_KEY)
        .collect()
}

fn ordered_block_rows(state: &[EntityState]) -> Vec<&EntityState> {
    let mut rows = block_rows(state);
    rows.sort_by_key(|row| order_key_from_row(row));
    rows
}

fn count_tombstones(changes: &[DetectedChange]) -> usize {
    changes
        .iter()
        .filter(|change| change.schema_key == BLOCK_SCHEMA_KEY && change.snapshot_content.is_none())
        .count()
}

fn count_upserts(changes: &[DetectedChange]) -> usize {
    changes
        .iter()
        .filter(|change| change.schema_key == BLOCK_SCHEMA_KEY && change.snapshot_content.is_some())
        .count()
}

fn count_document_rows(changes: &[DetectedChange]) -> usize {
    changes
        .iter()
        .filter(|change| change.schema_key == DOCUMENT_SCHEMA_KEY)
        .count()
}

fn upsert_ids(changes: &[DetectedChange]) -> Vec<String> {
    changes
        .iter()
        .filter(|change| change.schema_key == BLOCK_SCHEMA_KEY && change.snapshot_content.is_some())
        .map(|change| change.entity_pk[0].clone())
        .collect()
}

fn tombstone_ids(changes: &[DetectedChange]) -> Vec<String> {
    changes
        .iter()
        .filter(|change| change.schema_key == BLOCK_SCHEMA_KEY && change.snapshot_content.is_none())
        .map(|change| change.entity_pk[0].clone())
        .collect()
}

fn snapshot_value(raw: &str) -> Value {
    serde_json::from_str(raw).expect("snapshot should parse")
}

fn change_snapshot_value(change: &DetectedChange) -> Value {
    snapshot_value(
        change
            .snapshot_content
            .as_ref()
            .expect("change should contain snapshot"),
    )
}

fn row_snapshot_value(row: &EntityState) -> Value {
    snapshot_value(&row.snapshot_content)
}

fn block_text_from_change(change: &DetectedChange) -> String {
    change_snapshot_value(change)
        .get("block")
        .and_then(Value::as_str)
        .expect("block snapshot should contain block")
        .to_string()
}

fn block_text_from_row(row: &EntityState) -> String {
    row_snapshot_value(row)
        .get("block")
        .and_then(Value::as_str)
        .expect("block snapshot should contain block")
        .to_string()
}

fn order_key_from_row(row: &EntityState) -> String {
    row_snapshot_value(row)
        .get("order_key")
        .and_then(Value::as_str)
        .expect("block snapshot should contain order_key")
        .to_string()
}

fn ids_by_block(state: &[EntityState]) -> BTreeMap<String, String> {
    block_rows(state)
        .into_iter()
        .map(|row| (block_text_from_row(row), row.entity_pk[0].clone()))
        .collect()
}

fn order_keys_by_block(state: &[EntityState]) -> BTreeMap<String, String> {
    block_rows(state)
        .into_iter()
        .map(|row| (block_text_from_row(row), order_key_from_row(row)))
        .collect()
}

fn render_after_delta(before_state: Vec<EntityState>, changes: Vec<DetectedChange>) -> Vec<u8> {
    MarkdownPlugin::render(apply_changes_to_active_state(before_state, changes))
        .expect("render should succeed")
}

fn assert_inserted_id_is_uuid_v7(changes: &[DetectedChange]) {
    let ids = upsert_ids(changes);
    assert_eq!(ids.len(), 1);
    let uuid = Uuid::parse_str(&ids[0]).expect("inserted id should be a UUID");
    assert_eq!(uuid.get_version_num(), 7);
}

fn assert_single_existing_block_edit(before: &str, after: &str, old_block: &str) {
    let before_state = active_state_from_file(file_from_markdown(before));
    let before_ids = ids_by_block(&before_state);
    let changes = MarkdownPlugin::detect_changes(before_state, file_from_markdown(after))
        .expect("detect_changes should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 0);
    assert_eq!(upsert_ids(&changes), vec![before_ids[old_block].clone()]);
}

#[test]
fn no_changes_when_documents_are_equal() {
    let changes = detect_from_files(
        Some("# Title\n\nSame paragraph.\n"),
        "# Title\n\nSame paragraph.\n",
    );

    assert!(changes.is_empty());
}

#[test]
fn emits_document_and_block_rows_for_new_file() {
    let changes = detect_from_files(None, "# Title\n\nParagraph.\n");

    assert_eq!(count_document_rows(&changes), 1);
    assert_eq!(count_upserts(&changes), 2);
    assert_eq!(
        changes
            .iter()
            .find(|change| change.schema_key == DOCUMENT_SCHEMA_KEY)
            .map(change_snapshot_value),
        Some(serde_json::json!({"id": ROOT_ENTITY_PK}))
    );
    assert_eq!(
        changes
            .iter()
            .filter(|change| change.schema_key == BLOCK_SCHEMA_KEY)
            .map(block_text_from_change)
            .collect::<Vec<_>>(),
        ["# Title", "Paragraph."]
    );
}

#[test]
fn handles_empty_documents() {
    let changes = detect_from_files(Some(""), "");

    assert!(changes.is_empty());
}

#[test]
fn inline_html_br_does_not_drop_changes() {
    let changes = detect_from_files(
        None,
        "SSH auth: `git clone git@github.com:microsoft/vscode-docs.git`<br>HTTPS auth: `git clone https://github.com/microsoft/vscode-docs.git`\n",
    );

    assert_eq!(count_document_rows(&changes), 1);
    assert_eq!(count_upserts(&changes), 1);
    assert!(
        block_text_from_change(
            changes
                .iter()
                .find(|change| change.schema_key == BLOCK_SCHEMA_KEY)
                .expect("block change should exist")
        )
        .contains("<br>HTTPS auth")
    );
}

#[test]
fn duplicate_paragraphs_with_no_text_change_emit_no_changes() {
    let changes = detect_from_files(Some("Same\n\nSame\n"), "Same\n\nSame\n");

    assert!(changes.is_empty());
}

#[test]
fn insert_duplicate_paragraph_emits_new_block() {
    let changes = detect_from_files(Some("Same\n\nOther\n"), "Same\n\nSame\n\nOther\n");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 0);
    assert_eq!(block_text_from_change(&changes[0]), "Same");
    assert_inserted_id_is_uuid_v7(&changes);
}

#[test]
fn crlf_vs_lf_normalization_emits_no_changes() {
    let changes = detect_from_files(Some("Line A\r\n\r\nLine B\r\n"), "Line A\n\nLine B\n");

    assert!(changes.is_empty());
}

#[test]
fn hard_break_variant_reuses_existing_paragraph_id() {
    assert_single_existing_block_edit(
        "line  \r\nbreak\r\n",
        "line\\\r\nbreak\r\n",
        "line  \nbreak",
    );
}

#[test]
fn code_fence_length_variation_reuses_existing_code_block_id() {
    assert_single_existing_block_edit(
        "```js\nconsole.log(1)\n```\n",
        "````js\nconsole.log(1)\n````\n",
        "```js\nconsole.log(1)\n```",
    );
}

#[test]
fn id_stability_insert_between_keeps_neighbors_and_mints_new_id() {
    let before_state = active_state_from_file(file_from_markdown("A\n\nC\n"));
    let before_ids = ids_by_block(&before_state);
    let before_order_keys = order_keys_by_block(&before_state);
    let changes = MarkdownPlugin::detect_changes(before_state, file_from_markdown("A\n\nB\n\nC\n"))
        .expect("detect_changes should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 0);
    assert_inserted_id_is_uuid_v7(&changes);

    let inserted = changes
        .iter()
        .find(|change| change.schema_key == BLOCK_SCHEMA_KEY)
        .expect("inserted block should exist");
    let inserted_value = change_snapshot_value(inserted);
    let inserted_order_key = inserted_value
        .get("order_key")
        .and_then(Value::as_str)
        .expect("insert should contain order_key");

    assert_eq!(block_text_from_change(inserted), "B");
    assert_ne!(inserted.entity_pk[0], before_ids["A"]);
    assert_ne!(inserted.entity_pk[0], before_ids["C"]);
    assert!(inserted_order_key > before_order_keys["A"].as_str());
    assert!(inserted_order_key < before_order_keys["C"].as_str());
}

#[test]
fn id_stability_delete_keeps_survivor_id_and_tombstones_deleted() {
    let before_state = active_state_from_file(file_from_markdown("Keep me\n\nDelete me\n"));
    let before_ids = ids_by_block(&before_state);
    let changes =
        MarkdownPlugin::detect_changes(before_state.clone(), file_from_markdown("Keep me\n"))
            .expect("detect_changes should succeed");

    assert_eq!(count_tombstones(&changes), 1);
    assert_eq!(count_upserts(&changes), 0);
    assert_eq!(count_document_rows(&changes), 0);
    assert_eq!(
        tombstone_ids(&changes),
        vec![before_ids["Delete me"].clone()]
    );

    let after = apply_changes_to_active_state(before_state, changes);
    let after_ids = ids_by_block(&after);
    assert_eq!(after_ids["Keep me"], before_ids["Keep me"]);
}

#[test]
fn with_state_context_paragraph_edit_reuses_existing_id_without_tombstone() {
    assert_single_existing_block_edit("Hello\n\nWorld\n", "Hello updated\n\nWorld\n", "Hello");
}

#[test]
fn with_state_context_insert_between_preserves_neighbor_ids_and_mints_new_id() {
    let before_state = active_state_from_file(file_from_markdown("A\n\nC\n"));
    let before_ids = ids_by_block(&before_state);
    let changes =
        MarkdownPlugin::detect_changes(before_state.clone(), file_from_markdown("A\n\nB\n\nC\n"))
            .expect("detect_changes should succeed");
    let after_state = apply_changes_to_active_state(before_state, changes);
    let after_ids = ids_by_block(&after_state);

    assert_eq!(after_ids["A"], before_ids["A"]);
    assert_eq!(after_ids["C"], before_ids["C"]);
    assert_ne!(after_ids["B"], before_ids["A"]);
    assert_ne!(after_ids["B"], before_ids["C"]);
}

#[test]
fn with_state_context_duplicate_edit_second_preserves_first_id_without_document_noise() {
    let before_state = active_state_from_file(file_from_markdown("Same\n\nSame\n"));
    let before_rows = ordered_block_rows(&before_state);
    let first_id = before_rows[0].entity_pk[0].clone();
    let second_id = before_rows[1].entity_pk[0].clone();

    let changes =
        MarkdownPlugin::detect_changes(before_state, file_from_markdown("Same\n\nSame updated\n"))
            .expect("detect_changes should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 0);
    assert_eq!(upsert_ids(&changes), vec![second_id]);
    assert_ne!(upsert_ids(&changes), vec![first_id]);
}

#[test]
fn with_state_context_duplicate_middle_edit_targets_only_middle_entity() {
    let before_state = active_state_from_file(file_from_markdown("Same\n\nSame\n\nSame\n"));
    let before_rows = ordered_block_rows(&before_state);
    let middle_id = before_rows[1].entity_pk[0].clone();

    let changes = MarkdownPlugin::detect_changes(
        before_state,
        file_from_markdown("Same\n\nSame updated\n\nSame\n"),
    )
    .expect("detect_changes should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 0);
    assert_eq!(upsert_ids(&changes), vec![middle_id]);
}

#[test]
fn with_state_context_list_reorder_emits_single_list_upsert_without_document_row() {
    assert_single_existing_block_edit(
        "- one\n- two\n- three\n",
        "- three\n- one\n- two\n",
        "- one\n- two\n- three",
    );
}

#[test]
fn with_state_context_list_add_item_emits_single_list_upsert_without_document_row() {
    assert_single_existing_block_edit("- one\n- two\n", "- one\n- two\n- three\n", "- one\n- two");
}

#[test]
fn with_state_context_list_remove_item_emits_single_list_upsert_without_document_row() {
    assert_single_existing_block_edit(
        "- one\n- two\n- three\n",
        "- one\n- three\n",
        "- one\n- two\n- three",
    );
}

#[test]
fn with_state_context_table_reorder_rows_emits_single_table_upsert_without_document_row() {
    assert_single_existing_block_edit(
        "| a | b |\n| - | - |\n| 1 | 2 |\n| 3 | 4 |\n| 5 | 6 |\n",
        "| a | b |\n| - | - |\n| 3 | 4 |\n| 5 | 6 |\n| 1 | 2 |\n",
        "| a | b |\n| - | - |\n| 1 | 2 |\n| 3 | 4 |\n| 5 | 6 |",
    );
}

#[test]
fn with_state_context_table_add_row_emits_single_table_upsert_without_document_row() {
    assert_single_existing_block_edit(
        "| a | b |\n| - | - |\n| 1 | 2 |\n",
        "| a | b |\n| - | - |\n| 1 | 2 |\n| 3 | 4 |\n",
        "| a | b |\n| - | - |\n| 1 | 2 |",
    );
}

#[test]
fn with_state_context_table_remove_row_emits_single_table_upsert_without_document_row() {
    assert_single_existing_block_edit(
        "| a | b |\n| - | - |\n| 1 | 2 |\n| 3 | 4 |\n",
        "| a | b |\n| - | - |\n| 1 | 2 |\n",
        "| a | b |\n| - | - |\n| 1 | 2 |\n| 3 | 4 |",
    );
}

#[test]
fn with_state_context_heading_edit_reuses_existing_id_without_document_row() {
    assert_single_existing_block_edit("# Hello\n\nBody\n", "# Hello World\n\nBody\n", "# Hello");
}

#[test]
fn with_state_context_code_edit_reuses_existing_id_without_document_row() {
    assert_single_existing_block_edit(
        "```js\nconsole.log(1)\n```\n",
        "```js\nconsole.log(2)\n```\n",
        "```js\nconsole.log(1)\n```",
    );
}

#[test]
fn with_state_context_link_text_edit_reuses_existing_id_without_document_row() {
    assert_single_existing_block_edit(
        "[text](https://example.com)\n",
        "[new](https://example.com)\n",
        "[text](https://example.com)",
    );
}

#[test]
fn with_state_context_link_url_edit_reuses_existing_id_without_document_row() {
    assert_single_existing_block_edit(
        "[text](https://example.com)\n",
        "[text](https://example.org)\n",
        "[text](https://example.com)",
    );
}

#[test]
fn with_state_context_paragraph_split_reuses_first_id_and_mints_one_new() {
    let before_state = active_state_from_file(file_from_markdown("AB\n"));
    let before_ids = ids_by_block(&before_state);
    let changes =
        MarkdownPlugin::detect_changes(before_state.clone(), file_from_markdown("A\n\nB\n"))
            .expect("detect_changes should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 2);
    assert_eq!(count_document_rows(&changes), 0);
    assert!(upsert_ids(&changes).contains(&before_ids["AB"]));

    let after_state = apply_changes_to_active_state(before_state, changes);
    let after_ids = ids_by_block(&after_state);
    assert_eq!(after_ids["A"], before_ids["AB"]);
    assert_ne!(after_ids["B"], before_ids["AB"]);
}

#[test]
fn with_state_context_paragraph_merge_reuses_first_id_and_tombstones_second() {
    let before_state = active_state_from_file(file_from_markdown("A\n\nB\n"));
    let before_ids = ids_by_block(&before_state);
    let changes = MarkdownPlugin::detect_changes(before_state, file_from_markdown("AB\n"))
        .expect("detect_changes should succeed");

    assert_eq!(count_tombstones(&changes), 1);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 0);
    assert_eq!(tombstone_ids(&changes), vec![before_ids["B"].clone()]);
    assert_eq!(upsert_ids(&changes), vec![before_ids["A"].clone()]);
}

#[test]
fn with_state_context_large_500_tiny_edits_emit_only_targeted_upserts() {
    let paragraphs = make_large_markdown_paragraphs(500);
    let before_markdown = paragraphs.join("\n\n") + "\n";
    let before_state = active_state_from_file(file_from_markdown(&before_markdown));
    let before_ids = ids_by_block(&before_state);

    let mut after = paragraphs;
    let edited_indexes = [10usize, 111, 222, 333, 444];
    for index in edited_indexes {
        after[index] = format!("{} x", after[index]);
    }
    let after_markdown = after.join("\n\n") + "\n";

    let changes = MarkdownPlugin::detect_changes(before_state, file_from_markdown(&after_markdown))
        .expect("detect_changes should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 5);
    assert_eq!(count_document_rows(&changes), 0);

    let expected_ids = edited_indexes
        .iter()
        .map(|index| before_ids[&format!("P{}", index + 1)].clone())
        .collect::<BTreeSet<_>>();
    let actual_ids = upsert_ids(&changes).into_iter().collect::<BTreeSet<_>>();
    assert_eq!(actual_ids, expected_ids);
}

#[test]
fn with_state_context_large_duplicates_edit_350_targets_only_existing_id() {
    let before_paragraphs = (0..500).map(|_| "Same".to_string()).collect::<Vec<_>>();
    let before_markdown = before_paragraphs.join("\n\n") + "\n";
    let before_state = active_state_from_file(file_from_markdown(&before_markdown));
    let target_id = ordered_block_rows(&before_state)[349].entity_pk[0].clone();

    let mut after = before_paragraphs;
    after[349] = "Same updated".to_string();
    let after_markdown = after.join("\n\n") + "\n";

    let changes = MarkdownPlugin::detect_changes(before_state, file_from_markdown(&after_markdown))
        .expect("detect_changes should succeed");

    assert_eq!(count_tombstones(&changes), 0);
    assert_eq!(count_upserts(&changes), 1);
    assert_eq!(count_document_rows(&changes), 0);
    assert_eq!(upsert_ids(&changes), vec![target_id]);
}

#[test]
fn edit_move_delete_delta_renders_expected_markdown() {
    let before_state = active_state_from_file(file_from_markdown("Alpha.\n\nBravo.\n\nCharlie.\n"));
    let changes = MarkdownPlugin::detect_changes(
        before_state.clone(),
        file_from_markdown("Charlie.\n\nAlpha updated.\n"),
    )
    .expect("detect_changes should succeed");

    assert_eq!(
        render_after_delta(before_state, changes),
        b"Charlie.\n\nAlpha updated.\n"
    );
}

fn make_large_markdown_paragraphs(count: usize) -> Vec<String> {
    (1..=count).map(|index| format!("P{index}")).collect()
}
