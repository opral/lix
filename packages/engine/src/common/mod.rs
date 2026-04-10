pub(crate) mod dialect;
pub(crate) mod error;
pub(crate) mod errors;
pub(crate) mod fingerprint;
pub(crate) mod identity;
pub(crate) mod paths;
pub(crate) mod text;
pub(crate) mod types;
pub(crate) mod wire;

#[allow(unused_imports)]
pub(crate) use dialect::SqlDialect;
#[allow(unused_imports)]
pub(crate) use error::LixError;
#[allow(unused_imports)]
pub(crate) use errors::{
    already_initialized_error, live_state_not_ready_error, not_initialized_error,
    unexpected_statement_count_error, ErrorCode,
};
#[allow(unused_imports)]
pub(crate) use fingerprint::stable_content_fingerprint_hex;
#[allow(unused_imports)]
pub(crate) use identity::{
    derive_entity_id_from_json_paths, json_pointer_get, CanonicalPluginKey, CanonicalSchemaKey,
    CanonicalSchemaVersion, EntityId, EntityIdDerivationError, FileId, VersionId,
};
#[allow(unused_imports)]
pub(crate) use paths::filesystem::{
    compose_directory_path, directory_ancestor_paths, directory_name_from_path,
    normalize_directory_path, normalize_file_path, normalize_path_segment, parent_directory_path,
    NormalizedDirectoryPath, NormalizedFilePath, ParsedFilePath,
};
#[allow(unused_imports)]
pub(crate) use text::escape_sql_string;
#[allow(unused_imports)]
pub(crate) use types::{ExecuteResult, QueryResult, Value};
#[allow(unused_imports)]
pub(crate) use wire::{WireQueryResult, WireValue};
