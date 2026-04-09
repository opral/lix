mod annotations;
mod builtin;
mod definition;
mod init;
mod key;
#[cfg(test)]
mod tests;

pub(crate) use annotations::defaults::apply_schema_defaults_with_shared_runtime;
pub(crate) use annotations::overrides::{
    collect_lixcol_overrides, collect_state_column_overrides_with_shared_runtime,
    LixcolOverrideValue,
};
#[cfg(test)]
pub(crate) use annotations::writer_key::WorkspaceWriterKeyReadView;
pub(crate) use annotations::writer_key::{
    apply_workspace_writer_key_annotations_with_executor,
    load_workspace_writer_key_annotation_for_state_row,
    load_workspace_writer_key_annotations_with_executor,
    tracked_writer_key_annotations_from_changes, WORKSPACE_WRITER_KEY_TABLE,
};
#[allow(unused_imports)]
pub(crate) use builtin::{
    builtin_schema_definition, builtin_schema_json, builtin_schema_keys, decode_lixcol_literal,
    LixActiveVersion, LixCommit, LixVersionDescriptor, LixVersionRef,
};
pub use definition::{
    lix_schema_definition, lix_schema_definition_json, validate_lix_schema,
    validate_lix_schema_definition,
};
pub(crate) use init::{init, seed_bootstrap};
pub use key::{schema_from_registered_snapshot, schema_key_from_definition, SchemaKey};
