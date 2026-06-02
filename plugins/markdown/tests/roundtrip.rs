mod common;

use common::{
    StateRows, active_state_from_changes, apply_changes_to_active_state, collect_state_rows,
    decode_utf8, detect_changes_from_files, file_from_markdown, is_document_change, merge_delta,
};
use plugin_md_v2::exports::lix::plugin::api::Guest;
use plugin_md_v2::{BLOCK_SCHEMA_KEY, DOCUMENT_SCHEMA_KEY, DetectedChange, MarkdownPlugin};

fn detect_with_state_context(
    state: &StateRows,
    _before: plugin_md_v2::File,
    after: plugin_md_v2::File,
) -> Vec<DetectedChange> {
    let active_state = active_state_from_changes(collect_state_rows(state));
    MarkdownPlugin::detect_changes(active_state, after)
        .expect("MarkdownPlugin::detect_changes should succeed")
}

fn render_state(state: &StateRows) -> Vec<u8> {
    MarkdownPlugin::render(active_state_from_changes(collect_state_rows(state)))
        .expect("MarkdownPlugin::render should succeed")
}

fn count_tombstones(changes: &[DetectedChange]) -> usize {
    changes
        .iter()
        .filter(|c| c.schema_key == BLOCK_SCHEMA_KEY && c.snapshot_content.is_none())
        .count()
}

fn count_upserts(changes: &[DetectedChange]) -> usize {
    changes
        .iter()
        .filter(|c| c.schema_key == BLOCK_SCHEMA_KEY && c.snapshot_content.is_some())
        .count()
}

fn count_document_rows(changes: &[DetectedChange]) -> usize {
    changes
        .iter()
        .filter(|c| c.schema_key == DOCUMENT_SCHEMA_KEY)
        .count()
}

fn upsert_block_types(changes: &[DetectedChange]) -> Vec<String> {
    changes
        .iter()
        .filter(|c| c.schema_key == BLOCK_SCHEMA_KEY && c.snapshot_content.is_some())
        .map(|c| {
            let raw = c
                .snapshot_content
                .as_ref()
                .expect("upsert should have snapshot");
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

#[test]
fn roundtrip_file_detect_state_render_markdown() {
    let markdown = "# Title\n\nParagraph one.\n\nParagraph two.\n";
    let file = file_from_markdown(markdown);

    let delta =
        MarkdownPlugin::detect_changes(Vec::new(), file).expect("detect_changes should succeed");

    let mut state = StateRows::new();
    merge_delta(&mut state, delta);

    let materialized = render_state(&state);

    assert_eq!(decode_utf8(materialized), markdown);
}

#[test]
fn roundtrip_edit_move_delete_across_block_rows() {
    let before_markdown = "Alpha.\n\nBravo.\n\nCharlie.\n";
    let after_markdown = "Charlie.\n\nAlpha updated.\n";

    let before_file = file_from_markdown(before_markdown);

    let mut state = StateRows::new();
    let bootstrap = MarkdownPlugin::detect_changes(Vec::new(), before_file.clone())
        .expect("bootstrap detect should succeed");
    merge_delta(&mut state, bootstrap);

    let delta = detect_with_state_context(&state, before_file, file_from_markdown(after_markdown));

    assert!(
        delta
            .iter()
            .any(|change| change.schema_key == DOCUMENT_SCHEMA_KEY)
    );
    assert!(delta.iter().any(|change| {
        change.schema_key == BLOCK_SCHEMA_KEY && change.snapshot_content.is_none()
    }));
    assert!(delta.iter().any(|change| {
        change.schema_key == BLOCK_SCHEMA_KEY && change.snapshot_content.is_some()
    }));

    merge_delta(&mut state, delta);

    let materialized = render_state(&state);

    assert_eq!(decode_utf8(materialized), after_markdown);
}

#[test]
fn roundtrip_move_only_updates_document_order() {
    let before_markdown = "First block.\n\nSecond block.\n";
    let after_markdown = "Second block.\n\nFirst block.\n";

    let delta = detect_changes_from_files(
        Some(file_from_markdown(before_markdown)),
        file_from_markdown(after_markdown),
    )
    .expect("detect_changes should succeed");

    assert_eq!(delta.len(), 1);
    assert!(delta.iter().all(is_document_change));
}

#[test]
fn roundtrip_multi_step_evolution() {
    let a = "# Title\n\nOne.\n";
    let b = "# Title v2\n\nOne.\n\nTwo.\n";
    let c = "Two.\n\n# Title v3\n";

    let a_file = file_from_markdown(a);
    let b_file = file_from_markdown(b);
    let c_file = file_from_markdown(c);

    let mut state = StateRows::new();

    let delta_a = MarkdownPlugin::detect_changes(Vec::new(), a_file.clone())
        .expect("detect_changes should succeed");
    merge_delta(&mut state, delta_a);

    let delta_b = detect_with_state_context(&state, a_file, b_file.clone());
    merge_delta(&mut state, delta_b);

    let delta_c = detect_with_state_context(&state, b_file, c_file);
    merge_delta(&mut state, delta_c);

    let materialized = render_state(&state);

    assert_eq!(decode_utf8(materialized), c);
}

#[test]
fn roundtrip_delete_all_blocks_to_empty_document() {
    let before = "A\n\nB\n";
    let before_file = file_from_markdown(before);

    let mut state = StateRows::new();
    let bootstrap = MarkdownPlugin::detect_changes(Vec::new(), before_file.clone())
        .expect("bootstrap detect should succeed");
    merge_delta(&mut state, bootstrap);

    let delta = detect_with_state_context(&state, before_file, file_from_markdown(""));
    merge_delta(&mut state, delta);

    let materialized = render_state(&state);

    assert_eq!(decode_utf8(materialized), "");
}

#[test]
fn roundtrip_list_internal_edit_keeps_top_level_block_model() {
    let before = "- one\n- two\n";
    let after = "- one\n- two changed\n";
    let before_file = file_from_markdown(before);

    let mut state = StateRows::new();
    let bootstrap = MarkdownPlugin::detect_changes(Vec::new(), before_file.clone())
        .expect("bootstrap detect should succeed");
    merge_delta(&mut state, bootstrap);

    let delta = detect_with_state_context(&state, before_file, file_from_markdown(after));

    assert_eq!(count_tombstones(&delta), 0);
    assert_eq!(count_upserts(&delta), 1);
    assert_eq!(count_document_rows(&delta), 0);
    assert_eq!(upsert_block_types(&delta), vec!["list".to_string()]);

    merge_delta(&mut state, delta);

    let materialized = render_state(&state);
    assert_eq!(decode_utf8(materialized), after);
}

#[test]
fn roundtrip_table_row_add_remove_reorder() {
    let initial = "| a | b |\n| - | - |\n| 1 | 2 |\n";
    let add = "| a | b |\n| - | - |\n| 1 | 2 |\n| 3 | 4 |\n";
    let reorder = "| a | b |\n| - | - |\n| 3 | 4 |\n| 1 | 2 |\n";
    let remove = "| a | b |\n| - | - |\n| 3 | 4 |\n";

    let mut state = StateRows::new();
    let initial_file = file_from_markdown(initial);
    let bootstrap = MarkdownPlugin::detect_changes(Vec::new(), initial_file.clone())
        .expect("bootstrap detect should succeed");
    merge_delta(&mut state, bootstrap);

    let delta_add = detect_with_state_context(&state, initial_file, file_from_markdown(add));
    assert_eq!(count_tombstones(&delta_add), 0);
    assert_eq!(count_upserts(&delta_add), 1);
    assert_eq!(count_document_rows(&delta_add), 0);
    assert_eq!(upsert_block_types(&delta_add), vec!["table".to_string()]);
    merge_delta(&mut state, delta_add);

    let delta_reorder =
        detect_with_state_context(&state, file_from_markdown(add), file_from_markdown(reorder));
    assert_eq!(count_tombstones(&delta_reorder), 0);
    assert_eq!(count_upserts(&delta_reorder), 1);
    assert_eq!(count_document_rows(&delta_reorder), 0);
    assert_eq!(
        upsert_block_types(&delta_reorder),
        vec!["table".to_string()]
    );
    merge_delta(&mut state, delta_reorder);

    let delta_remove = detect_with_state_context(
        &state,
        file_from_markdown(reorder),
        file_from_markdown(remove),
    );
    assert_eq!(count_tombstones(&delta_remove), 0);
    assert_eq!(count_upserts(&delta_remove), 1);
    assert_eq!(count_document_rows(&delta_remove), 0);
    assert_eq!(upsert_block_types(&delta_remove), vec!["table".to_string()]);
    merge_delta(&mut state, delta_remove);

    let materialized = render_state(&state);
    assert_eq!(decode_utf8(materialized), remove);
}

#[test]
fn roundtrip_large_shuffle_500_with_state_context_low_noise() {
    let paragraphs = (1..=500).map(|idx| format!("P{idx}")).collect::<Vec<_>>();
    let before_markdown = paragraphs.join("\n\n") + "\n";
    let before_file = file_from_markdown(&before_markdown);

    let mut state = StateRows::new();
    let bootstrap = MarkdownPlugin::detect_changes(Vec::new(), before_file.clone())
        .expect("bootstrap detect should succeed");
    merge_delta(&mut state, bootstrap);

    let mut after = paragraphs;
    after.rotate_left(123);
    let after_markdown = after.join("\n\n") + "\n";

    let delta = detect_with_state_context(&state, before_file, file_from_markdown(&after_markdown));
    assert_eq!(count_tombstones(&delta), 0);
    assert_eq!(count_upserts(&delta), 0);
    assert_eq!(count_document_rows(&delta), 1);
    merge_delta(&mut state, delta);

    let materialized = render_state(&state);
    assert_eq!(decode_utf8(materialized), after_markdown);
}

#[test]
fn roundtrip_large_tiny_edits_500_with_state_context_low_noise() {
    let paragraphs = (1..=500).map(|idx| format!("P{idx}")).collect::<Vec<_>>();
    let before_markdown = paragraphs.join("\n\n") + "\n";
    let before_file = file_from_markdown(&before_markdown);

    let mut state = StateRows::new();
    let bootstrap = MarkdownPlugin::detect_changes(Vec::new(), before_file.clone())
        .expect("bootstrap detect should succeed");
    merge_delta(&mut state, bootstrap);

    let mut after = paragraphs;
    for idx in [10usize, 111, 222, 333, 444] {
        after[idx] = format!("{} x", after[idx]);
    }
    let after_markdown = after.join("\n\n") + "\n";

    let delta = detect_with_state_context(&state, before_file, file_from_markdown(&after_markdown));
    assert_eq!(count_tombstones(&delta), 0);
    assert_eq!(count_upserts(&delta), 5);
    assert_eq!(count_document_rows(&delta), 0);
    merge_delta(&mut state, delta);

    let materialized = render_state(&state);
    assert_eq!(decode_utf8(materialized), after_markdown);
}

#[test]
fn roundtrip_large_duplicate_edit_with_state_context_low_noise() {
    let before_paragraphs = (0..500).map(|_| "Same".to_string()).collect::<Vec<_>>();
    let before_markdown = before_paragraphs.join("\n\n") + "\n";
    let before_file = file_from_markdown(&before_markdown);

    let mut state = StateRows::new();
    let bootstrap = MarkdownPlugin::detect_changes(Vec::new(), before_file.clone())
        .expect("bootstrap detect should succeed");
    merge_delta(&mut state, bootstrap);

    let mut after = before_paragraphs;
    after[349] = "Same updated".to_string();
    let after_markdown = after.join("\n\n") + "\n";

    let delta = detect_with_state_context(&state, before_file, file_from_markdown(&after_markdown));
    assert_eq!(count_tombstones(&delta), 0);
    assert_eq!(count_upserts(&delta), 1);
    assert!(count_document_rows(&delta) <= 1);
    merge_delta(&mut state, delta);

    let materialized = render_state(&state);
    assert_eq!(decode_utf8(materialized), after_markdown);
}

#[test]
fn roundtrip_move_insert_delete_large_with_state_context_low_noise() {
    let paragraphs = (1..=500).map(|idx| format!("P{idx}")).collect::<Vec<_>>();
    let before_markdown = paragraphs.join("\n\n") + "\n";
    let before_file = file_from_markdown(&before_markdown);

    let mut state = StateRows::new();
    let bootstrap = MarkdownPlugin::detect_changes(Vec::new(), before_file.clone())
        .expect("bootstrap detect should succeed");
    merge_delta(&mut state, bootstrap);

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

    let delta = detect_with_state_context(&state, before_file, file_from_markdown(&after_markdown));
    let tombstones = count_tombstones(&delta);
    let upserts = count_upserts(&delta);
    let docs = count_document_rows(&delta);
    assert!(tombstones <= 1);
    assert_eq!(upserts, 1);
    assert_eq!(docs, 1);
    assert!(tombstones + upserts <= 2);
    merge_delta(&mut state, delta);

    let materialized = render_state(&state);
    assert_eq!(decode_utf8(materialized), after_markdown);
}

#[test]
fn guest_interface_uses_active_state_for_low_noise_edit_and_render() {
    let before = "Hello\n\nWorld\n";
    let after = "Hello updated\n\nWorld\n";

    let before_state = active_state_from_changes(
        MarkdownPlugin::detect_changes(Vec::new(), file_from_markdown(before))
            .expect("initial detect_changes should succeed"),
    );

    let delta = MarkdownPlugin::detect_changes(before_state.clone(), file_from_markdown(after))
        .expect("delta detect_changes should succeed");
    assert_eq!(count_tombstones(&delta), 0);
    assert_eq!(count_upserts(&delta), 1);
    assert_eq!(count_document_rows(&delta), 0);

    let after_state = apply_changes_to_active_state(before_state, delta);
    let materialized = MarkdownPlugin::render(after_state).expect("render should succeed");

    assert_eq!(decode_utf8(materialized), after);
}
