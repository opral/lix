//! Test-only row plugin that exercises per-column merge callbacks.

#![cfg_attr(
    not(all(target_arch = "wasm32", target_os = "wasi", target_env = "p2")),
    allow(dead_code)
)]

use lix::plugin as sdk;

struct TestColumnMerger;

impl sdk::ColumnMerger for TestColumnMerger {
    fn merge(input: sdk::ColumnMerge<'_>) -> sdk::Result<sdk::ColumnMergeResult> {
        if input.row.schema_key != "merge_test_row" || input.column != "body" {
            return Ok(sdk::ColumnMergeResult::UseLww);
        }
        if input.row.file_id.is_some() {
            return Err(sdk::Error::invalid_input(
                "merge test rows must not be owned by a file projection",
            ));
        }
        let (
            Some(sdk::TypedValue::Text(base)),
            Some(sdk::TypedValue::Text(a)),
            Some(sdk::TypedValue::Text(b)),
        ) = (input.base.value()?, input.a.value()?, input.b.value()?)
        else {
            return Ok(sdk::ColumnMergeResult::UseLww);
        };
        let Some(merged) = merge_disjoint_text(&base, &a, &b) else {
            return Ok(sdk::ColumnMergeResult::UseLww);
        };
        Ok(sdk::ColumnMergeResult::Replace(
            sdk::OwnedColumnValue::typed(&sdk::TypedValue::Text(merged))?,
        ))
    }
}

fn merge_disjoint_text(base: &str, a: &str, b: &str) -> Option<String> {
    let a_edit = changed_span(base, a)?;
    let b_edit = changed_span(base, b)?;
    if a_edit.0 < b_edit.1 && b_edit.0 < a_edit.1 {
        return None;
    }
    let mut edits = [a_edit, b_edit];
    edits.sort_by_key(|edit| edit.0);
    let mut merged = base.to_owned();
    for (start, end, replacement) in edits.into_iter().rev() {
        merged.replace_range(start..end, &replacement);
    }
    Some(merged)
}

fn changed_span(base: &str, changed: &str) -> Option<(usize, usize, String)> {
    if base == changed {
        return None;
    }
    let prefix = base
        .bytes()
        .zip(changed.bytes())
        .take_while(|(left, right)| left == right)
        .count();
    let prefix = floor_char_boundary(base, floor_char_boundary(changed, prefix));
    let base_rest = &base[prefix..];
    let changed_rest = &changed[prefix..];
    let suffix = base_rest
        .bytes()
        .rev()
        .zip(changed_rest.bytes().rev())
        .take_while(|(left, right)| left == right)
        .count()
        .min(base_rest.len())
        .min(changed_rest.len());
    let base_end = floor_char_boundary(base, base.len() - suffix).max(prefix);
    let changed_end = floor_char_boundary(changed, changed.len() - suffix).max(prefix);
    Some((prefix, base_end, changed[prefix..changed_end].to_owned()))
}

fn floor_char_boundary(value: &str, mut index: usize) -> usize {
    index = index.min(value.len());
    while !value.is_char_boundary(index) {
        index -= 1;
    }
    index
}

lix::plugin::export_capabilities! {
    column_merger: TestColumnMerger,
}

#[cfg(test)]
mod tests {
    use super::merge_disjoint_text;

    #[test]
    fn merges_disjoint_text_edits() {
        assert_eq!(
            merge_disjoint_text(
                "Alice said hello.\n\nBob said goodbye.",
                "Alice said HELLO.\n\nBob said goodbye.",
                "Alice said hello.\n\nBob said GOODBYE.",
            ),
            Some("Alice said HELLO.\n\nBob said GOODBYE.".to_owned())
        );
    }

    #[test]
    fn overlapping_edits_keep_lww() {
        assert_eq!(merge_disjoint_text("hello", "hullo", "hallo"), None);
    }
}
