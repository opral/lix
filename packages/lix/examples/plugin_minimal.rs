//! Minimal compiling row-only Lix Component plugin.

use lix::plugin::{
    ColumnMerge, ColumnMergeResult, ColumnMerger, OwnedColumnValue, Result, TypedValue,
};

#[allow(dead_code)]
struct ExampleColumnMerger;

impl ColumnMerger for ExampleColumnMerger {
    fn merge(input: ColumnMerge<'_>) -> Result<ColumnMergeResult> {
        if input.row.schema_key != "example_document" || input.column != "body" {
            return Ok(ColumnMergeResult::UseLww);
        }
        let (Some(TypedValue::Text(base)), Some(TypedValue::Text(a)), Some(TypedValue::Text(b))) =
            (input.base.value()?, input.a.value()?, input.b.value()?)
        else {
            return Ok(ColumnMergeResult::UseLww);
        };

        // This example's domain rule merges two append-only transcript writes.
        // Any edit to existing prose deliberately keeps the host's LWW value.
        let (Some(a_append), Some(b_append)) = (a.strip_prefix(&base), b.strip_prefix(&base))
        else {
            return Ok(ColumnMergeResult::UseLww);
        };
        let merged = format!("{base}{a_append}{b_append}");
        Ok(ColumnMergeResult::Replace(OwnedColumnValue::typed(
            &TypedValue::Text(merged),
        )?))
    }
}

lix::plugin::export_capabilities! { column_merger: ExampleColumnMerger }

// Cargo builds examples as binaries during `cargo test --all-targets`. The
// packaged-plugin qualification copies this source into a downstream
// `wasm32-wasip2` library, where no binary entry point is needed.
#[cfg(not(all(target_arch = "wasm32", target_os = "wasi", target_env = "p2")))]
fn main() {}
