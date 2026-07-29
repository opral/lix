//! Excalidraw guest for the Lix Wasm Component plugin API v2.

mod bindings;
mod core;

pub use core::{
    ByteEdit, ChangeEffect, Document, ELEMENT_SCHEMA_KEY, EntityChange, EntityImportBuilder,
    EntityRecord, FILE_SCHEMA_KEY, IdNamespace, InitialChanges, InputSplice, SCENE_SCHEMA_KEY,
};

pub const MANIFEST_JSON: &str = include_str!("../manifest.json");
/// API v3 retains scene/element/file entities. The JSON template and span
/// indexes are rebuildable opaque state, split from durable merge authority.
pub const V3_ARENA_LAYOUT: lix_plugin_arena::FormatLayout = lix_plugin_arena::FormatLayout {
    plugin_key: "plugin_excalidraw_v2",
    schema_keys: &["excalidraw_scene", "excalidraw_element", "excalidraw_file"],
    state_pages: &[
        lix_plugin_arena::StatePageLayout {
            kind: "scene-template",
            target_items: 1,
        },
        lix_plugin_arena::StatePageLayout {
            kind: "element-spans",
            target_items: 256,
        },
        lix_plugin_arena::StatePageLayout {
            kind: "file-spans",
            target_items: 256,
        },
    ],
};
pub const SCHEMAS: [(&str, &str); 3] = [
    (
        "schema/excalidraw_scene.json",
        include_str!("../schema/excalidraw_scene.json"),
    ),
    (
        "schema/excalidraw_element.json",
        include_str!("../schema/excalidraw_element.json"),
    ),
    (
        "schema/excalidraw_file.json",
        include_str!("../schema/excalidraw_file.json"),
    ),
];

#[cfg(test)]
mod tests;

#[cfg(test)]
mod arena_layout_tests {
    #[test]
    fn v3_layout_retains_the_excalidraw_schemas_and_object_granularity() {
        assert!(super::V3_ARENA_LAYOUT.is_valid());
        assert_eq!(
            super::V3_ARENA_LAYOUT.schema_keys,
            &[
                super::SCENE_SCHEMA_KEY,
                super::ELEMENT_SCHEMA_KEY,
                super::FILE_SCHEMA_KEY,
            ]
        );
    }
}
