//! GitHub Flavored Markdown guest for the Lix Wasm Component plugin API v2.

#[cfg(feature = "component-v2")]
mod bindings;
mod core;
mod markdown_file;
mod model;
pub mod schemas;

pub use core::{
    ByteEdit, ChangeEffect, DetectedChange, Document, EntityChange, EntityRecord, EntityState,
    File, IdNamespace, InputSplice, MarkdownPlugin, NODE_SCHEMA_KEY, PluginError,
    V3TopLevelIndexRecord, v3_reidentify_snapshot, v3_single_top_level_snapshot,
};

pub const MANIFEST_JSON: &str = include_str!("../manifest.json");
/// API v3 keeps the existing node schema and top-level semantic granularity.
/// Only acceleration state moves into host-owned pages.
pub const V3_ARENA_LAYOUT: lix_plugin_arena::FormatLayout = lix_plugin_arena::FormatLayout {
    plugin_key: "plugin_markdown_incremental_v2",
    schema_keys: &["markdown_node_v2"],
    state_pages: &[
        lix_plugin_arena::StatePageLayout {
            kind: "top-level-source-range",
            target_items: 256,
        },
        lix_plugin_arena::StatePageLayout {
            kind: "top-level-node",
            target_items: 256,
        },
        lix_plugin_arena::StatePageLayout {
            kind: "identity-index",
            target_items: 256,
        },
    ],
};
pub const SCHEMAS: [(&str, &str); 1] = [(
    "schema/markdown_node_v2.json",
    include_str!("../schema/markdown_node_v2.json"),
)];

#[cfg(test)]
mod tests;

#[cfg(test)]
mod arena_layout_tests {
    #[test]
    fn v3_layout_retains_the_markdown_schema_and_granularity() {
        assert!(super::V3_ARENA_LAYOUT.is_valid());
        assert_eq!(
            super::V3_ARENA_LAYOUT.schema_keys,
            &[super::NODE_SCHEMA_KEY]
        );
    }
}
