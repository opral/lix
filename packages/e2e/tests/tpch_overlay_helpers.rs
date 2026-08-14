#[path = "../benches/tpch/overlay.rs"]
mod overlay;

#[test]
fn every_nth_selection_is_exact_and_counts_all_rows() {
    assert_eq!(overlay::ROWS_PER_STATEMENT, 2_048);
    let selection = overlay::select_every_nth(1..=10, 3);
    assert_eq!(selection.items, vec![3, 6, 9]);
    assert_eq!(selection.total_rows, 10);
}

#[test]
fn overlay_sql_uses_the_exact_selected_keys() {
    assert_eq!(
        overlay::lineitem_update_sql(&["000001:01".to_string(), "000002:03".to_string()]),
        "UPDATE lineitem SET l_quantity = 51 WHERE l_rowkey IN ('000001:01','000002:03')"
    );
}
