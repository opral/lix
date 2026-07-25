mod support;

use plugin_markdown_incremental_v2::EntityState;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use support::{
    apply_changes, changes, evolve, ids_of_kind, render, rows_of_kind, semantic_html, snapshot,
    state_from_source,
};

fn state_map(state: Vec<EntityState>) -> BTreeMap<String, EntityState> {
    state
        .into_iter()
        .map(|row| (row.entity_pk[0].clone(), row))
        .collect()
}

#[test]
fn flashtype_rewrite_retains_list_item_identities() {
    let before_source = concat!(
        "**TL;DR**\n",
        "1. Pasted the ICP and product screenshots into Claude Design\n",
        "2. Added reference sites for the vibe/structure\n",
        "3. Asked for 3 different hero variants\n",
        "4. Picked the strongest direction\n",
        "5. Removed anything outside the core message\n",
    );
    let after_source = concat!(
        "**TL;DR**\n",
        "1. I gave Claude Design context first.\n",
        "2. I asked for three hero ideas.\n",
        "3. I picked the best direction.\n",
        "4. I focused on the obvious value.\n",
        "5. I removed anything distracting.\n",
    );
    let before = state_from_source(before_source);
    let before_ids = ids_of_kind(&before, "list_item")
        .into_iter()
        .collect::<BTreeSet<_>>();
    let after = evolve(before, after_source);
    let after_ids = ids_of_kind(&after, "list_item")
        .into_iter()
        .collect::<BTreeSet<_>>();
    assert_eq!(before_ids, after_ids);
}

#[test]
fn editing_large_table_header_retains_all_rows_cells_and_columns() {
    let rows = (0..100)
        .map(|index| format!("| row-{index:03} | value-{index:03} |"))
        .collect::<Vec<_>>()
        .join("\n");
    let before_source = format!("| Name | Value |\n| --- | --- |\n{rows}\n");
    let after_source = format!("| Label | Value |\n| --- | --- |\n{rows}\n");
    let before = state_from_source(&before_source);
    let durable_kinds = ["table", "table_column", "table_row", "table_cell"];
    let before_ids = durable_kinds
        .iter()
        .flat_map(|kind| ids_of_kind(&before, kind))
        .collect::<BTreeSet<_>>();
    let after = evolve(before, &after_source);
    let after_ids = durable_kinds
        .iter()
        .flat_map(|kind| ids_of_kind(&after, kind))
        .collect::<BTreeSet<_>>();
    assert_eq!(before_ids, after_ids);
}

#[test]
fn two_cell_edits_are_two_independently_addressable_rows() {
    let before_source = "| A | B |\n| --- | --- |\n| a0 | b0 |\n| a1 | b1 |\n";
    let both_source = "| A | B |\n| --- | --- |\n| a0 edited | b0 |\n| a1 | b1 edited |\n";
    let expected_source = "| A | B |\n| --- | --- |\n| a0 | b0 |\n| a1 | b1 edited |\n";
    let before = state_from_source(before_source);
    let delta = changes(before.clone(), both_source);
    assert_eq!(delta.len(), 2, "{delta:#?}");
    assert!(delta.iter().all(|change| {
        serde_json::from_str::<Value>(change.snapshot_content.as_deref().unwrap()).unwrap()["kind"]
            == "table_cell"
    }));
    let both = apply_changes(before.clone(), delta);
    let before_rows = state_map(before);
    let mut mixed = state_map(both);
    let first_cell_id = mixed
        .values()
        .find(|row| row.snapshot_content.contains("a0 edited"))
        .unwrap()
        .entity_pk[0]
        .clone();
    mixed.insert(first_cell_id.clone(), before_rows[&first_cell_id].clone());
    assert_eq!(
        semantic_html(&render(mixed.into_values().collect())),
        semantic_html(expected_source)
    );
}

#[test]
fn direct_leaf_state_write_is_easy_and_local() {
    let mut state = state_from_source("Before\n\nUntouched\n");
    let paragraph = rows_of_kind(&state, "paragraph")
        .into_iter()
        .find(|row| row.snapshot_content.contains("Before"))
        .unwrap()
        .entity_pk[0]
        .clone();
    let row = state
        .iter_mut()
        .find(|row| row.entity_pk[0] == paragraph)
        .unwrap();
    let mut value = snapshot(row);
    value["payload"]["inline"] = serde_json::json!([{"type": "text", "value": "After"}]);
    row.snapshot_content = value.to_string();
    assert_eq!(render(state), "After\n\nUntouched\n");
}

#[test]
fn rows_never_duplicate_descendant_text() {
    let state = state_from_source("- parent\n  - deeply nested child\n");
    for row in rows_of_kind(&state, "list")
        .into_iter()
        .chain(rows_of_kind(&state, "list_item"))
    {
        assert!(!row.snapshot_content.contains("deeply nested child"));
    }
}

#[test]
fn every_non_text_inline_atom_has_a_stable_embedded_id() {
    let state = state_from_source("A *marked* [link](https://example.com) and `code`.\n");
    let paragraph = snapshot(rows_of_kind(&state, "paragraph")[0]);
    let inline = paragraph["payload"]["inline"].as_array().unwrap();
    for atom in inline {
        if atom["type"] != "text" {
            assert!(atom["id"].as_str().is_some(), "{atom:#}");
        }
    }
}

#[test]
fn equivalent_format_spelling_changes_state_but_not_semantics() {
    let before = "Text *em* and **strong**.\n";
    let after = "Text _em_ and __strong__.\n";
    assert_eq!(semantic_html(before), semantic_html(after));
    let delta = changes(state_from_source(before), after);
    assert_eq!(delta.len(), 1);
    let output = render(evolve(state_from_source(before), after));
    assert_eq!(output, after);
}
