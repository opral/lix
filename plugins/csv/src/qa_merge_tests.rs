use super::*;

#[test]
fn qa_cell_merge_keeps_disjoint_edits_when_row_order_changes() {
    let (document, _) =
        Document::open_file(b"a,b\n".to_vec(), None, IdNamespace::from_halves(31, 47)).unwrap();
    let base = document.row_records().unwrap()[1].row.clone();
    let mut a = base.clone();
    let mut b = base.clone();
    a.insert(
        "cells",
        sdk::TypedValue::Jsonb(serde_json::json!(["A", "b"]).into()),
    );
    a.insert("order_key", sdk::TypedValue::Text("01".into()));
    b.insert(
        "cells",
        sdk::TypedValue::Jsonb(serde_json::json!(["a", "B"]).into()),
    );
    assert_eq!(
        merge_typed_csv_cells(&base, &a, &b).unwrap(),
        Some(vec![serde_json::json!("A"), serde_json::json!("B")])
    );
}

#[test]
fn qa_cell_merge_keeps_disjoint_edits_when_row_format_changes() {
    let (document, _) =
        Document::open_file(b"a,b\n".to_vec(), None, IdNamespace::from_halves(31, 47)).unwrap();
    let base = document.row_records().unwrap()[1].row.clone();
    let mut a = base.clone();
    let mut b = base.clone();
    a.insert(
        "cells",
        sdk::TypedValue::Jsonb(serde_json::json!(["A", "b"]).into()),
    );
    a.insert(
        "layout",
        sdk::TypedValue::Jsonb(serde_json::json!({"force_quote":"AQ"}).into()),
    );
    b.insert(
        "cells",
        sdk::TypedValue::Jsonb(serde_json::json!(["a", "B"]).into()),
    );
    assert_eq!(
        merge_typed_csv_cells(&base, &a, &b).unwrap(),
        Some(vec![serde_json::json!("A"), serde_json::json!("B")])
    );
}
