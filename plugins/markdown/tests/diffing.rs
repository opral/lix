mod support;

use serde_json::Value;
use std::collections::BTreeSet;
use support::{
    change_snapshot, changes, evolve, ids_of_kind, rows_of_kind, snapshot, state_from_source,
};

fn inline_atom_id(
    state: &[plugin_md_v2::exports::lix::plugin::api::EntityState],
    kind: &str,
) -> String {
    rows_of_kind(state, "paragraph")
        .into_iter()
        .flat_map(|row| {
            snapshot(row)["payload"]["inline"]
                .as_array()
                .expect("paragraph inline payload should be an array")
                .clone()
        })
        .find(|inline| inline["type"] == kind)
        .and_then(|inline| inline["id"].as_str().map(str::to_string))
        .expect("inline atom should have a durable id")
}

fn upserts_for_kind<'a>(
    delta: &'a [plugin_md_v2::DetectedChange],
    kind: &str,
) -> Vec<&'a plugin_md_v2::DetectedChange> {
    delta
        .iter()
        .filter(|change| {
            change.snapshot_content.is_some()
                && change_snapshot(change).get("kind").and_then(Value::as_str) == Some(kind)
        })
        .collect()
}

#[test]
fn projects_lists_to_structural_and_leaf_entities() {
    let state = state_from_source("- Alpha\n- Bravo\n- Charlie\n");
    assert_eq!(rows_of_kind(&state, "document").len(), 1);
    assert_eq!(rows_of_kind(&state, "list").len(), 1);
    assert_eq!(rows_of_kind(&state, "list_item").len(), 3);
    assert_eq!(rows_of_kind(&state, "paragraph").len(), 3);
}

#[test]
fn one_list_item_edit_updates_one_leaf_row() {
    let before = state_from_source("- Alpha\n- Bravo\n- Charlie\n");
    let delta = changes(before, "- Alpha\n- Bravo edited\n- Charlie\n");
    assert_eq!(delta.len(), 1);
    assert_eq!(change_snapshot(&delta[0])["kind"], "paragraph");
    assert!(
        !delta[0]
            .snapshot_content
            .as_deref()
            .unwrap()
            .contains("Alpha")
    );
    assert!(
        !delta[0]
            .snapshot_content
            .as_deref()
            .unwrap()
            .contains("Charlie")
    );
}

#[test]
fn one_edit_in_a_thousand_paragraphs_is_one_row_update() {
    let before = (0..1_000)
        .map(|index| format!("Paragraph {index:04}"))
        .collect::<Vec<_>>()
        .join("\n\n")
        + "\n";
    let after = before.replace("Paragraph 0500", "Paragraph 0500 edited");
    let state = state_from_source(&before);
    assert_eq!(rows_of_kind(&state, "paragraph").len(), 1_000);
    let delta = changes(state, &after);
    assert_eq!(delta.len(), 1);
    assert_eq!(change_snapshot(&delta[0])["kind"], "paragraph");
}

#[test]
fn list_style_edit_updates_only_the_list_container() {
    let before_source = (0..100)
        .map(|index| format!("- Item {index:03}"))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    let after_source = (0..100)
        .map(|index| format!("{}. Item {index:03}", index + 1))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    let delta = changes(state_from_source(&before_source), &after_source);
    assert_eq!(delta.len(), 1);
    assert_eq!(change_snapshot(&delta[0])["kind"], "list");
    assert!(
        !delta[0]
            .snapshot_content
            .as_deref()
            .unwrap()
            .contains("Item 099")
    );
}

#[test]
fn subtree_move_updates_only_the_moved_item() {
    let before = concat!(
        "- Alpha\n",
        "- Moved\n",
        "  - Nested child 00\n",
        "  - Nested child 01\n",
        "- Omega\n",
    );
    let after = concat!(
        "- Alpha\n",
        "- Omega\n",
        "- Moved\n",
        "  - Nested child 00\n",
        "  - Nested child 01\n",
    );
    let delta = changes(state_from_source(before), after);
    assert_eq!(delta.len(), 1, "{delta:#?}");
    assert_eq!(change_snapshot(&delta[0])["kind"], "list_item");
    assert!(
        !delta[0]
            .snapshot_content
            .as_deref()
            .unwrap()
            .contains("Nested child")
    );
}

#[test]
fn duplicate_insertion_and_non_latin_reorder_retain_existing_ids() {
    let before = state_from_source("- Same\n- Same\n");
    let before_ids = ids_of_kind(&before, "list_item");
    let after = evolve(before, "- Same\n- Same\n- Same\n");
    let after_ids = ids_of_kind(&after, "list_item");
    assert!(before_ids.iter().all(|id| after_ids.contains(id)));

    let before = state_from_source("- 同じ\n- مرحبا\n- 同じ\n- 👩🏽‍💻\n");
    let before_ids = ids_of_kind(&before, "list_item");
    let after = evolve(before, "- 👩🏽‍💻\n- 同じ\n- مرحبا\n- 同じ\n");
    let after_ids = ids_of_kind(&after, "list_item");
    assert_eq!(
        before_ids.into_iter().collect::<BTreeSet<_>>(),
        after_ids.into_iter().collect::<BTreeSet<_>>()
    );
}

#[test]
fn unique_cross_parent_move_retains_id_but_copy_mints_a_new_id() {
    let before = state_from_source("> Moved\n\nOutside\n");
    let moved_id = rows_of_kind(&before, "paragraph")
        .into_iter()
        .find(|row| row.snapshot_content.contains("Moved"))
        .unwrap()
        .entity_pk[0]
        .clone();
    let after = evolve(before, "Outside\n\n- Moved\n");
    let after_moved_id = rows_of_kind(&after, "paragraph")
        .into_iter()
        .find(|row| row.snapshot_content.contains("Moved"))
        .unwrap()
        .entity_pk[0]
        .clone();
    assert_eq!(moved_id, after_moved_id);

    let before = state_from_source("> Same\n");
    let original_id = ids_of_kind(&before, "paragraph")[0].clone();
    let after = evolve(before, "> Same\n\nSame\n");
    let after_ids = ids_of_kind(&after, "paragraph");
    assert_eq!(after_ids.len(), 2);
    assert!(after_ids.contains(&original_id));
    assert_eq!(after_ids.iter().collect::<BTreeSet<_>>().len(), 2);
}

#[test]
fn edited_local_subtree_keeps_its_id_when_an_exact_copy_is_added_elsewhere() {
    let before = state_from_source("> Same\n");
    let original_id = ids_of_kind(&before, "paragraph")[0].clone();
    let after = evolve(before, "> Same edited\n\nSame\n");
    let edited_id = rows_of_kind(&after, "paragraph")
        .into_iter()
        .find(|row| row.snapshot_content.contains("Same edited"))
        .unwrap()
        .entity_pk[0]
        .clone();
    assert_eq!(edited_id, original_id);
}

#[test]
fn edited_local_subtree_keeps_its_id_when_copy_is_inserted_earlier_in_document() {
    let before = state_from_source("- Destination\n\n> Same\n");
    let original_id = rows_of_kind(&before, "paragraph")
        .into_iter()
        .find(|row| row.snapshot_content.contains("Same"))
        .unwrap()
        .entity_pk[0]
        .clone();
    let after = evolve(before, "- Destination\n  - Same\n\n> Same edited\n");
    let edited_id = rows_of_kind(&after, "paragraph")
        .into_iter()
        .find(|row| row.snapshot_content.contains("Same edited"))
        .unwrap()
        .entity_pk[0]
        .clone();
    assert_eq!(edited_id, original_id);
}

#[test]
fn exact_cross_parent_subtree_move_preserves_every_descendant_id() {
    let before = state_from_source("> - Moved\n>   - Nested child\n>\n> Keep\n");
    let before_ids = ["list", "list_item"]
        .into_iter()
        .flat_map(|kind| ids_of_kind(&before, kind))
        .chain(
            rows_of_kind(&before, "paragraph")
                .into_iter()
                .filter(|row| {
                    row.snapshot_content.contains("Moved")
                        || row.snapshot_content.contains("Nested child")
                })
                .map(|row| row.entity_pk[0].clone()),
        )
        .collect::<BTreeSet<_>>();

    let delta = changes(before.clone(), "- Moved\n  - Nested child\n\n> Keep\n");
    let after = support::apply_changes(before, delta.clone());
    let after_ids = ["list", "list_item"]
        .into_iter()
        .flat_map(|kind| ids_of_kind(&after, kind))
        .chain(
            rows_of_kind(&after, "paragraph")
                .into_iter()
                .filter(|row| {
                    row.snapshot_content.contains("Moved")
                        || row.snapshot_content.contains("Nested child")
                })
                .map(|row| row.entity_pk[0].clone()),
        )
        .collect::<BTreeSet<_>>();

    assert_eq!(after_ids, before_ids);
    assert_eq!(delta.len(), 1, "{delta:#?}");
    assert_eq!(change_snapshot(&delta[0])["kind"], "list");
}

#[test]
fn unique_cross_parent_move_wins_over_deleted_local_same_kind_candidate() {
    let before = state_from_source("Alpha\n\n> Bravo\n");
    let bravo_id = rows_of_kind(&before, "paragraph")
        .into_iter()
        .find(|row| row.snapshot_content.contains("Bravo"))
        .unwrap()
        .entity_pk[0]
        .clone();
    let after = evolve(before, "Bravo\n");
    let after_bravo_id = rows_of_kind(&after, "paragraph")[0].entity_pk[0].clone();
    assert_eq!(after_bravo_id, bravo_id);
}

#[test]
fn editing_the_middle_duplicate_targets_its_positional_entity() {
    let before = state_from_source("Same\n\nSame\n\nSame\n");
    let ordered_ids = {
        let mut rows = rows_of_kind(&before, "paragraph");
        rows.sort_by_key(|row| snapshot(row)["order_key"].as_str().unwrap().to_string());
        rows.into_iter()
            .map(|row| row.entity_pk[0].clone())
            .collect::<Vec<_>>()
    };

    let delta = changes(before, "Same\n\nSame edited\n\nSame\n");
    assert_eq!(delta.len(), 1, "{delta:#?}");
    assert_eq!(delta[0].entity_pk, vec![ordered_ids[1].clone()]);
    assert_eq!(change_snapshot(&delta[0])["kind"], "paragraph");
}

#[test]
fn editing_a_link_destination_preserves_the_inline_atom_id() {
    let before = state_from_source("Before [label](https://example.com) after.\n");
    let link_id = inline_atom_id(&before, "link");
    let after = evolve(before, "Before [label](https://example.org) after.\n");
    assert_eq!(inline_atom_id(&after, "link"), link_id);
}

#[test]
fn reordering_links_preserves_both_inline_atom_ids() {
    let before = state_from_source("[A](/a) and [B](/b).\n");
    let before_ids = snapshot(rows_of_kind(&before, "paragraph")[0])["payload"]["inline"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|inline| inline["type"] == "link")
        .map(|inline| inline["id"].as_str().unwrap().to_string())
        .collect::<BTreeSet<_>>();
    let after = evolve(before, "[B](/b) and [A](/a).\n");
    let after_ids = snapshot(rows_of_kind(&after, "paragraph")[0])["payload"]["inline"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|inline| inline["type"] == "link")
        .map(|inline| inline["id"].as_str().unwrap().to_string())
        .collect::<BTreeSet<_>>();

    assert_eq!(after_ids, before_ids);
}

#[test]
fn inserting_between_colliding_order_keys_preserves_existing_ids_and_render_order() {
    let mut before = state_from_source("Alpha\n\nOmega\n");
    let before_ids = ids_of_kind(&before, "paragraph");
    for row in &mut before {
        let mut value: Value = serde_json::from_str(&row.snapshot_content).unwrap();
        if value["kind"] == "paragraph" {
            value["order_key"] = Value::String("80".to_string());
            row.snapshot_content = value.to_string();
        }
    }

    let after = evolve(before, "Alpha\n\nInserted\n\nOmega\n");
    let after_ids = ids_of_kind(&after, "paragraph");
    assert!(before_ids.iter().all(|id| after_ids.contains(id)));
    assert_eq!(support::render(after), "Alpha\n\nInserted\n\nOmega\n");
}

#[test]
fn inserting_inside_nested_colliding_order_keys_rebalances_without_losing_ids() {
    let mut before = state_from_source("> Alpha\n>\n> Omega\n");
    let before_ids = ids_of_kind(&before, "paragraph");
    for row in &mut before {
        let mut value: Value = serde_json::from_str(&row.snapshot_content).unwrap();
        if value["kind"] == "paragraph" {
            value["order_key"] = Value::String("80".to_string());
            row.snapshot_content = value.to_string();
        }
    }

    let after = evolve(before, "> Alpha\n>\n> Inserted\n>\n> Omega\n");
    let after_ids = ids_of_kind(&after, "paragraph");
    assert!(before_ids.iter().all(|id| after_ids.contains(id)));
    assert_eq!(
        support::render(after),
        "> Alpha\n> \n> Inserted\n> \n> Omega\n"
    );
}

#[test]
fn table_has_durable_columns_rows_and_cells() {
    let state = state_from_source("| Key | Value |\n| --- | --- |\n| a | one |\n| b | two |\n");
    assert_eq!(rows_of_kind(&state, "table").len(), 1);
    assert_eq!(rows_of_kind(&state, "table_column").len(), 2);
    assert_eq!(rows_of_kind(&state, "table_row").len(), 3);
    assert_eq!(rows_of_kind(&state, "table_cell").len(), 6);
    let column_ids = ids_of_kind(&state, "table_column")
        .into_iter()
        .collect::<BTreeSet<_>>();
    for cell in rows_of_kind(&state, "table_cell") {
        let column_id = snapshot(cell)["payload"]["column_id"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(column_ids.contains(&column_id));
    }
}

#[test]
fn exact_cross_parent_table_move_preserves_ids_and_column_references() {
    let before = state_from_source("> | A | B |\n> | --- | --- |\n> | a | b |\n");
    let durable_kinds = ["table", "table_column", "table_row", "table_cell"];
    let before_ids = durable_kinds
        .into_iter()
        .flat_map(|kind| ids_of_kind(&before, kind))
        .collect::<BTreeSet<_>>();
    let after = evolve(before, "| A | B |\n| --- | --- |\n| a | b |\n");
    let after_ids = durable_kinds
        .into_iter()
        .flat_map(|kind| ids_of_kind(&after, kind))
        .collect::<BTreeSet<_>>();
    let column_ids = ids_of_kind(&after, "table_column")
        .into_iter()
        .collect::<BTreeSet<_>>();

    assert_eq!(after_ids, before_ids);
    for cell in rows_of_kind(&after, "table_cell") {
        let cell = snapshot(cell);
        let column_id = cell["payload"]["column_id"].as_str().unwrap();
        assert!(column_ids.contains(column_id));
    }
    assert_eq!(
        support::render(after),
        "| A | B |\n| --- | --- |\n| a | b |\n"
    );
}

#[test]
fn earlier_table_copy_does_not_steal_ids_or_column_refs_from_local_edit() {
    let before = state_from_source("> | A | B |\n> | --- | --- |\n> | a | b |\n");
    let original_b_cell = rows_of_kind(&before, "table_cell")
        .into_iter()
        .find(|row| row.snapshot_content.contains(r#""value":"b""#))
        .unwrap();
    let original_b_cell_id = original_b_cell.entity_pk[0].clone();
    let original_b_column_id = snapshot(original_b_cell)["payload"]["column_id"]
        .as_str()
        .unwrap()
        .to_string();

    let after = evolve(
        before,
        concat!(
            "| A | B |\n",
            "| --- | --- |\n",
            "| a | b |\n",
            "\n",
            "> | A | B |\n",
            "> | --- | --- |\n",
            "> | a | b edited |\n",
        ),
    );
    let edited_b_cell = rows_of_kind(&after, "table_cell")
        .into_iter()
        .find(|row| row.snapshot_content.contains("b edited"))
        .unwrap();

    assert_eq!(edited_b_cell.entity_pk[0], original_b_cell_id);
    assert_eq!(
        snapshot(edited_b_cell)["payload"]["column_id"],
        original_b_column_id
    );
    assert_eq!(ids_of_kind(&after, "table").len(), 2);
    assert_eq!(ids_of_kind(&after, "table_column").len(), 4);
    assert_eq!(ids_of_kind(&after, "table_cell").len(), 8);

    let columns_by_table = rows_of_kind(&after, "table_column")
        .into_iter()
        .map(|column| {
            (
                column.entity_pk[0].clone(),
                snapshot(column)["parent_id"].as_str().unwrap().to_string(),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let rows_by_id = rows_of_kind(&after, "table_row")
        .into_iter()
        .map(|row| {
            (
                row.entity_pk[0].clone(),
                snapshot(row)["parent_id"].as_str().unwrap().to_string(),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    for cell in rows_of_kind(&after, "table_cell") {
        let cell = snapshot(cell);
        let row_id = cell["parent_id"].as_str().unwrap();
        let column_id = cell["payload"]["column_id"].as_str().unwrap();
        assert_eq!(columns_by_table[column_id], rows_by_id[row_id]);
    }
}

#[test]
fn one_table_cell_edit_updates_one_cell_row() {
    let before = state_from_source("| A | B |\n| --- | --- |\n| a0 | b0 |\n| a1 | b1 |\n");
    let delta = changes(
        before,
        "| A | B |\n| --- | --- |\n| a0 | b0 edited |\n| a1 | b1 |\n",
    );
    assert_eq!(delta.len(), 1, "{delta:#?}");
    let snapshot = change_snapshot(&delta[0]);
    assert_eq!(snapshot["kind"], "table_cell");
    assert_eq!(snapshot["payload"]["inline"].as_array().unwrap().len(), 1);
    assert_eq!(snapshot["payload"]["inline"][0]["value"], "b0 edited");
}

#[test]
fn table_alignment_edit_updates_one_column() {
    let before = state_from_source("| A | B |\n| --- | --- |\n| a | b |\n");
    let delta = changes(before, "| A | B |\n| :--- | --- |\n| a | b |\n");
    assert_eq!(
        upserts_for_kind(&delta, "table_column").len(),
        1,
        "{delta:#?}"
    );
    assert_eq!(delta.len(), 1);
}

#[test]
fn swapping_two_columns_updates_one_column_row_and_zero_cells() {
    let before_rows = (0..100)
        .map(|index| format!("| a-{index:03} | b-{index:03} |"))
        .collect::<Vec<_>>()
        .join("\n");
    let after_rows = (0..100)
        .map(|index| format!("| b-{index:03} | a-{index:03} |"))
        .collect::<Vec<_>>()
        .join("\n");
    let before = format!("| A | B |\n| --- | --- |\n{before_rows}\n");
    let after = format!("| B | A |\n| --- | --- |\n{after_rows}\n");
    let delta = changes(state_from_source(&before), &after);
    assert_eq!(delta.len(), 1, "{delta:#?}");
    assert_eq!(change_snapshot(&delta[0])["kind"], "table_column");
}

#[test]
fn reordering_table_rows_preserves_all_cells_and_updates_only_row_position() {
    let before = state_from_source(
        "| Key | Value |\n| --- | --- |\n| a | one |\n| b | two |\n| c | three |\n",
    );
    let before_row_ids = ids_of_kind(&before, "table_row");
    let before_cell_ids = ids_of_kind(&before, "table_cell");
    let delta = changes(
        before.clone(),
        "| Key | Value |\n| --- | --- |\n| c | three |\n| a | one |\n| b | two |\n",
    );
    let after = support::apply_changes(before, delta.clone());

    assert_eq!(ids_of_kind(&after, "table_row"), before_row_ids);
    assert_eq!(ids_of_kind(&after, "table_cell"), before_cell_ids);
    assert_eq!(delta.len(), 1, "{delta:#?}");
    assert_eq!(change_snapshot(&delta[0])["kind"], "table_row");
}

#[test]
fn inserting_a_table_column_preserves_existing_column_and_cell_ids() {
    let before = state_from_source("| A | B |\n| --- | --- |\n| a | b |\n");
    let before_column_ids = ids_of_kind(&before, "table_column");
    let before_cell_ids = ids_of_kind(&before, "table_cell");
    let delta = changes(
        before.clone(),
        "| A | New | B |\n| --- | --- | --- |\n| a | n | b |\n",
    );
    let after = support::apply_changes(before, delta.clone());
    let after_column_ids = ids_of_kind(&after, "table_column");
    let after_cell_ids = ids_of_kind(&after, "table_cell");

    assert!(
        before_column_ids
            .iter()
            .all(|id| after_column_ids.contains(id))
    );
    assert!(before_cell_ids.iter().all(|id| after_cell_ids.contains(id)));
    assert_eq!(after_column_ids.len(), before_column_ids.len() + 1);
    assert_eq!(after_cell_ids.len(), before_cell_ids.len() + 2);
    assert_eq!(
        upserts_for_kind(&delta, "table_column").len(),
        1,
        "{delta:#?}"
    );
    assert_eq!(
        upserts_for_kind(&delta, "table_cell").len(),
        2,
        "{delta:#?}"
    );
    assert_eq!(
        support::render(after),
        "| A | New | B |\n| --- | --- | --- |\n| a | n | b |\n"
    );
}

#[test]
fn moving_a_table_row_while_editing_one_cell_preserves_row_and_cell_ids() {
    let before = state_from_source(
        "| Key | Value |\n| --- | --- |\n| a | one |\n| b | two |\n| c | three |\n",
    );
    let before_row_ids = ids_of_kind(&before, "table_row");
    let before_cell_ids = ids_of_kind(&before, "table_cell");
    let delta = changes(
        before.clone(),
        "| Key | Value |\n| --- | --- |\n| c | three edited |\n| a | one |\n| b | two |\n",
    );
    let after = support::apply_changes(before, delta.clone());

    assert_eq!(ids_of_kind(&after, "table_row"), before_row_ids);
    assert_eq!(ids_of_kind(&after, "table_cell"), before_cell_ids);
    assert_eq!(upserts_for_kind(&delta, "table_row").len(), 1, "{delta:#?}");
    assert_eq!(
        upserts_for_kind(&delta, "table_cell").len(),
        1,
        "{delta:#?}"
    );
    assert_eq!(delta.len(), 2, "{delta:#?}");
}

#[test]
fn moving_a_table_column_while_editing_one_cell_preserves_column_and_cell_ids() {
    let before = state_from_source(
        "| A | B | C |\n| --- | --- | --- |\n| a0 | b0 | c0 |\n| a1 | b1 | c1 |\n",
    );
    let before_column_ids = ids_of_kind(&before, "table_column");
    let before_cell_ids = ids_of_kind(&before, "table_cell");
    let delta = changes(
        before.clone(),
        "| C | A | B |\n| --- | --- | --- |\n| c0 edited | a0 | b0 |\n| c1 | a1 | b1 |\n",
    );
    let after = support::apply_changes(before, delta.clone());

    assert_eq!(ids_of_kind(&after, "table_column"), before_column_ids);
    assert_eq!(ids_of_kind(&after, "table_cell"), before_cell_ids);
    assert_eq!(
        upserts_for_kind(&delta, "table_column").len(),
        1,
        "{delta:#?}"
    );
    assert_eq!(
        upserts_for_kind(&delta, "table_cell").len(),
        1,
        "{delta:#?}"
    );
    assert_eq!(delta.len(), 2, "{delta:#?}");
}

#[test]
fn format_only_edits_are_local_state_changes() {
    let emphasis = changes(state_from_source("Text *em*.\n"), "Text _em_.\n");
    assert_eq!(emphasis.len(), 1);
    assert_eq!(change_snapshot(&emphasis[0])["kind"], "paragraph");
    assert_eq!(
        emphasis[0].metadata.as_deref(),
        Some(r#"{"impact":"format"}"#)
    );

    let bullet = changes(state_from_source("- one\n- two\n"), "* one\n* two\n");
    assert_eq!(bullet.len(), 1);
    assert_eq!(change_snapshot(&bullet[0])["kind"], "list");
    assert_eq!(
        bullet[0].metadata.as_deref(),
        Some(r#"{"impact":"format"}"#)
    );

    let newline = changes(state_from_source("A\n"), "A");
    assert_eq!(newline.len(), 1);
    assert_eq!(change_snapshot(&newline[0])["kind"], "document");
    assert_eq!(
        newline[0].metadata.as_deref(),
        Some(r#"{"impact":"format"}"#)
    );
}

#[test]
fn formatting_outside_the_v2_contract_canonicalizes_to_no_state_change() {
    let gaps = changes(state_from_source("A\n\nB\n"), "A\n\n\n\nB\n");
    assert!(gaps.is_empty());

    let closing_hash = changes(state_from_source("# Heading\n"), "# Heading #\n");
    assert!(closing_hash.is_empty());

    let table_width = changes(
        state_from_source("| A | B |\n| --- | ---: |\n| x | y |\n"),
        "| A | B |\n| ----- | --------: |\n| x | y |\n",
    );
    assert!(table_width.is_empty());
}
