//! Recursive stable-identity JSON guest for the Lix Wasm Component plugin API v2.

#[cfg(feature = "component-v2")]
mod bindings;
mod core;

pub use core::{
    ARRAY_ITEM_SCHEMA_KEY, ByteEdit, ChangeEffect, Document, EntityChange, EntityRecord,
    IdNamespace, InitialChanges, InputSplice, OBJECT_MEMBER_SCHEMA_KEY, ROOT_SCHEMA_KEY,
    V3ScalarIndexRecord,
};

pub const MANIFEST_JSON: &str = include_str!("../manifest.json");
/// API v3 retains the root/member/item entity model and pages only lexical
/// span, parent, and identity acceleration state.
pub const V3_ARENA_LAYOUT: lix_plugin_arena::FormatLayout = lix_plugin_arena::FormatLayout {
    plugin_key: "plugin_json_incremental_v2",
    schema_keys: &["json_root", "json_object_member", "json_array_item"],
    state_pages: &[
        lix_plugin_arena::StatePageLayout {
            kind: "lexical-spans",
            target_items: 512,
        },
        lix_plugin_arena::StatePageLayout {
            kind: "parent-index",
            target_items: 512,
        },
        lix_plugin_arena::StatePageLayout {
            kind: "identity-index",
            target_items: 512,
        },
    ],
};
pub const SCHEMAS: [(&str, &str); 3] = [
    (
        "schema/json_root.json",
        include_str!("../schema/json_root.json"),
    ),
    (
        "schema/json_object_member.json",
        include_str!("../schema/json_object_member.json"),
    ),
    (
        "schema/json_array_item.json",
        include_str!("../schema/json_array_item.json"),
    ),
];

#[cfg(test)]
mod tests;

#[cfg(test)]
mod arena_layout_tests {
    #[test]
    fn v3_layout_retains_the_json_schemas_and_node_granularity() {
        assert!(super::V3_ARENA_LAYOUT.is_valid());
        assert_eq!(
            super::V3_ARENA_LAYOUT.schema_keys,
            &[
                super::ROOT_SCHEMA_KEY,
                super::OBJECT_MEMBER_SCHEMA_KEY,
                super::ARRAY_ITEM_SCHEMA_KEY,
            ]
        );
    }
}
