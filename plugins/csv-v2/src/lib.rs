//! Incremental CSV guest for the Lix Wasm Component plugin API v2.

#[cfg(feature = "component-v2")]
mod bindings;
mod core;

pub use core::{
    ByteEdit, ChangeEffect, Dialect, Document, EntityChange, EntityRecord, IdNamespace,
    InitialChanges, InputSplice, ROOT_ENTITY_PK, ROW_SCHEMA_KEY, RowSnapshot, TABLE_SCHEMA_KEY,
    Terminator, V3ColdIndex, V3ColdMetadata, V3InitialChanges, V3RowFramer, V3RowIndexRecord,
    V3RowWindowCheckpoint, V3StreamAnalyzer, describe_memory, encode_row_snapshot,
    parse_row_snapshot, render_row, v3_open_file_stream, v3_stream_row_change,
    v3_stream_table_change,
};

pub const MANIFEST_JSON: &str = include_str!("../manifest.json");
/// API v3 preserves the table/row schemas. Span and identity chunks become
/// independently replaceable opaque host-state pages.
pub const V3_ARENA_LAYOUT: lix_plugin_arena::FormatLayout = lix_plugin_arena::FormatLayout {
    plugin_key: "plugin_csv_v2",
    schema_keys: &["csv_v2_table", "csv_v2_row"],
    state_pages: &[
        lix_plugin_arena::StatePageLayout {
            kind: "dialect",
            target_items: 1,
        },
        lix_plugin_arena::StatePageLayout {
            kind: "row-spans",
            target_items: 512,
        },
        lix_plugin_arena::StatePageLayout {
            kind: "row-identities",
            target_items: 64,
        },
    ],
};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod arena_layout_tests {
    #[test]
    fn v3_layout_retains_the_csv_schemas_and_row_granularity() {
        assert!(super::V3_ARENA_LAYOUT.is_valid());
        assert_eq!(
            super::V3_ARENA_LAYOUT.schema_keys,
            &[super::TABLE_SCHEMA_KEY, super::ROW_SCHEMA_KEY]
        );
    }
}
