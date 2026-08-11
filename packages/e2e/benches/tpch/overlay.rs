pub(crate) const ROWS_PER_STATEMENT: usize = 2_048;

#[derive(Debug)]
pub(crate) struct Selection<T> {
    pub(crate) items: Vec<T>,
    pub(crate) total_rows: usize,
}

pub(crate) fn select_every_nth<T>(
    items: impl IntoIterator<Item = T>,
    divisor: usize,
) -> Selection<T> {
    assert!(divisor > 0, "overlay divisor must be positive");
    let mut selected = Vec::new();
    let mut total_rows = 0_usize;
    for item in items {
        total_rows += 1;
        if total_rows % divisor == 0 {
            selected.push(item);
        }
    }
    Selection {
        items: selected,
        total_rows,
    }
}

pub(crate) fn lineitem_update_sql(rowkeys: &[String]) -> String {
    assert!(!rowkeys.is_empty(), "overlay update requires row keys");
    let rowkeys = rowkeys
        .iter()
        .map(|rowkey| format!("'{rowkey}'"))
        .collect::<Vec<_>>()
        .join(",");
    format!("UPDATE lineitem SET l_quantity = 51 WHERE l_rowkey IN ({rowkeys})")
}
