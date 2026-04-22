use crate::common::{normalize_path_segment, NormalizedDirectoryPath, ParsedFilePath};
use crate::functions::DynFunctionProvider;
use crate::schema::{apply_schema_defaults_with_shared_runtime, builtin_schema_definition};
use crate::{LixError, Value};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OptionalTextAssignment {
    Unchanged,
    Set(Option<String>),
}

impl OptionalTextAssignment {
    pub(crate) fn apply(&self, current: Option<String>) -> Option<String> {
        match self {
            Self::Unchanged => current,
            Self::Set(value) => value.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BlobAssignment {
    Unchanged,
    Set(Vec<u8>),
}

impl BlobAssignment {
    pub(crate) fn bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Unchanged => None,
            Self::Set(bytes) => Some(bytes.as_slice()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirectoryUpdateAssignments {
    pub(crate) path: Option<NormalizedDirectoryPath>,
    pub(crate) parent_id: Option<String>,
    pub(crate) name: Option<String>,
    pub(crate) hidden: Option<bool>,
    pub(crate) metadata: OptionalTextAssignment,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FilesystemWriteIntent {
    DirectoryInsert(Vec<DirectoryInsertAssignments>),
    DirectoryUpdate(DirectoryUpdateAssignments),
    DirectoryDelete,
    FileInsert(Vec<FileInsertAssignments>),
    FileUpdate(FileUpdateAssignments),
    FileDelete,
}

impl DirectoryUpdateAssignments {
    pub(crate) fn changes_structure(&self) -> bool {
        self.path.is_some() || self.name.is_some() || self.parent_id.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileUpdateAssignments {
    pub(crate) path: Option<ParsedFilePath>,
    pub(crate) hidden: Option<bool>,
    pub(crate) metadata: OptionalTextAssignment,
    pub(crate) data: BlobAssignment,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirectoryInsertAssignments {
    pub(crate) id: Option<String>,
    pub(crate) parent_id: Option<String>,
    pub(crate) name: Option<String>,
    pub(crate) path: Option<NormalizedDirectoryPath>,
    pub(crate) hidden: bool,
    pub(crate) untracked: Option<bool>,
    pub(crate) metadata: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileInsertAssignments {
    pub(crate) id: Option<String>,
    pub(crate) path: ParsedFilePath,
    pub(crate) hidden: bool,
    pub(crate) untracked: Option<bool>,
    pub(crate) metadata: Option<String>,
    pub(crate) data: Option<Vec<u8>>,
}

fn assignment_error(message: impl Into<String>) -> LixError {
    LixError::new("LIX_ERROR_UNKNOWN", message)
}

pub(crate) fn parse_directory_update_assignments(
    payload: &BTreeMap<String, Value>,
) -> Result<DirectoryUpdateAssignments, LixError> {
    if payload.contains_key("id") {
        return Err(assignment_error(
            "lix_directory id is immutable; create a new row and delete the old row instead",
        ));
    }

    Ok(DirectoryUpdateAssignments {
        path: payload
            .get("path")
            .map(|value| text_value_required(value, "public filesystem directory update", "path"))
            .transpose()?
            .map(|path| NormalizedDirectoryPath::try_from_path(&path))
            .transpose()
            .map_err(filesystem_path_error)?,
        parent_id: payload.get("parent_id").and_then(text_from_value),
        name: payload
            .get("name")
            .map(|value| text_value_required(value, "public filesystem directory update", "name"))
            .transpose()?
            .map(|name| normalize_path_segment(&name))
            .transpose()
            .map_err(filesystem_path_error)?,
        hidden: payload.get("hidden").and_then(value_as_bool),
        metadata: optional_text_assignment(payload, "metadata", "public filesystem directory")?,
    })
}

pub(crate) fn parse_file_update_assignments(
    payload: &BTreeMap<String, Value>,
) -> Result<FileUpdateAssignments, LixError> {
    if payload.contains_key("id") {
        return Err(assignment_error(
            "lix_file id is immutable; create a new row and delete the old row instead",
        ));
    }

    Ok(FileUpdateAssignments {
        path: payload
            .get("path")
            .map(|value| text_value_required(value, "public filesystem file update", "path"))
            .transpose()?
            .map(|path| ParsedFilePath::try_from_path(&path))
            .transpose()
            .map_err(filesystem_path_error)?,
        hidden: payload.get("hidden").and_then(value_as_bool),
        metadata: optional_text_assignment(payload, "metadata", "public filesystem resolver")?,
        data: blob_assignment(payload, "data")?,
    })
}

const FILESYSTEM_DIRECTORY_SCHEMA_KEY: &str = "lix_directory_descriptor";
const FILESYSTEM_FILE_SCHEMA_KEY: &str = "lix_file_descriptor";

pub(crate) fn parse_directory_insert_assignments(
    payload: &BTreeMap<String, Value>,
    functions: &DynFunctionProvider,
) -> Result<DirectoryInsertAssignments, LixError> {
    let defaults = filesystem_insert_defaults(FILESYSTEM_DIRECTORY_SCHEMA_KEY, functions)?;
    Ok(DirectoryInsertAssignments {
        id: payload
            .get("id")
            .and_then(text_from_value)
            .or_else(|| defaults.get("id").and_then(text_from_value)),
        parent_id: payload.get("parent_id").and_then(text_from_value),
        name: payload
            .get("name")
            .map(|value| text_value_required(value, "public filesystem directory insert", "name"))
            .transpose()?
            .map(|name| normalize_path_segment(&name))
            .transpose()
            .map_err(filesystem_path_error)?,
        path: payload
            .get("path")
            .map(|value| text_value_required(value, "public filesystem directory insert", "path"))
            .transpose()?
            .map(|path| NormalizedDirectoryPath::try_from_path(&path))
            .transpose()
            .map_err(filesystem_path_error)?,
        hidden: payload
            .get("hidden")
            .and_then(value_as_bool)
            .or_else(|| defaults.get("hidden").and_then(value_as_bool))
            .unwrap_or(false),
        untracked: payload.get("untracked").and_then(value_as_bool),
        metadata: optional_insert_text(payload, "metadata", "public filesystem directory")?,
    })
}

pub(crate) fn parse_file_insert_assignments(
    payload: &BTreeMap<String, Value>,
    functions: &DynFunctionProvider,
) -> Result<FileInsertAssignments, LixError> {
    if !payload
        .keys()
        .any(|key| !matches!(key.as_str(), "data" | "version_id" | "untracked"))
    {
        return Err(assignment_error(
            "file insert requires at least one non-data column",
        ));
    }

    let raw_path = payload
        .get("path")
        .map(|value| text_value_required(value, "public filesystem file insert", "path"))
        .transpose()?
        .ok_or_else(|| assignment_error("public filesystem file insert requires column 'path'"))?;

    let defaults = filesystem_insert_defaults(FILESYSTEM_FILE_SCHEMA_KEY, functions)?;

    Ok(FileInsertAssignments {
        id: payload
            .get("id")
            .and_then(text_from_value)
            .or_else(|| defaults.get("id").and_then(text_from_value)),
        path: ParsedFilePath::try_from_path(&raw_path).map_err(filesystem_path_error)?,
        hidden: payload
            .get("hidden")
            .and_then(value_as_bool)
            .or_else(|| defaults.get("hidden").and_then(value_as_bool))
            .unwrap_or(false),
        untracked: payload.get("untracked").and_then(value_as_bool),
        metadata: optional_insert_text(payload, "metadata", "public filesystem file")?,
        data: insert_blob_value(payload, "data")?,
    })
}

fn optional_text_assignment(
    payload: &BTreeMap<String, Value>,
    key: &str,
    context: &str,
) -> Result<OptionalTextAssignment, LixError> {
    match payload.get(key) {
        None => Ok(OptionalTextAssignment::Unchanged),
        Some(Value::Null) => Ok(OptionalTextAssignment::Set(None)),
        Some(Value::Text(value)) => Ok(OptionalTextAssignment::Set(Some(value.clone()))),
        Some(other) => Err(assignment_error(format!(
            "{context} expected text/null {key}, got {other:?}"
        ))),
    }
}

fn filesystem_insert_defaults(
    schema_key: &str,
    functions: &DynFunctionProvider,
) -> Result<BTreeMap<String, Value>, LixError> {
    let schema = builtin_schema_definition(schema_key).ok_or_else(|| {
        assignment_error(format!(
            "public filesystem resolver missing builtin schema '{schema_key}'"
        ))
    })?;
    let schema_version = schema
        .get("x-lix-version")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            assignment_error(format!(
                "public filesystem resolver requires string x-lix-version for '{schema_key}'"
            ))
        })?;
    let mut snapshot = JsonMap::new();
    apply_schema_defaults_with_shared_runtime(
        &mut snapshot,
        schema,
        functions.clone(),
        schema_key,
        schema_version,
    )
    .map_err(filesystem_path_error)?;
    snapshot
        .into_iter()
        .map(|(key, value)| {
            json_value_to_engine_value(value)
                .map(|value| (key, value))
                .map_err(assignment_error)
        })
        .collect()
}

fn json_value_to_engine_value(value: JsonValue) -> Result<Value, String> {
    match value {
        JsonValue::Null => Ok(Value::Null),
        JsonValue::Bool(value) => Ok(Value::Boolean(value)),
        JsonValue::String(value) => Ok(Value::Text(value)),
        JsonValue::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(Value::Integer(value))
            } else if let Some(value) = value.as_f64() {
                Ok(Value::Real(value))
            } else {
                Err("public filesystem resolver cannot represent JSON number".to_string())
            }
        }
        JsonValue::Array(_) | JsonValue::Object(_) => Ok(Value::Json(value)),
    }
}

fn blob_assignment(
    payload: &BTreeMap<String, Value>,
    key: &str,
) -> Result<BlobAssignment, LixError> {
    match payload.get(key) {
        None => Ok(BlobAssignment::Unchanged),
        Some(Value::Blob(bytes)) => Ok(BlobAssignment::Set(bytes.clone())),
        Some(Value::Text(_)) => Err(assignment_error(
            crate::sql::diagnostics::FILE_DATA_EXPECTS_BYTES_MESSAGE,
        )),
        Some(other) => Err(assignment_error(format!(
            "public filesystem resolver expected blob {key}, got {other:?}"
        ))),
    }
}

fn insert_blob_value(
    payload: &BTreeMap<String, Value>,
    key: &str,
) -> Result<Option<Vec<u8>>, LixError> {
    match payload.get(key) {
        None => Ok(None),
        Some(Value::Blob(bytes)) => Ok(Some(bytes.clone())),
        Some(Value::Text(_)) => Err(assignment_error(
            crate::sql::diagnostics::FILE_DATA_EXPECTS_BYTES_MESSAGE,
        )),
        Some(other) => Err(assignment_error(format!(
            "public filesystem resolver expected blob {key}, got {other:?}"
        ))),
    }
}

fn optional_insert_text(
    payload: &BTreeMap<String, Value>,
    key: &str,
    context: &str,
) -> Result<Option<String>, LixError> {
    match payload.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => text_from_value(value).map(Some).ok_or_else(|| {
            assignment_error(format!("{context} expected text/null {key}, got {value:?}"))
        }),
    }
}

fn text_value_required(value: &Value, context: &str, key: &str) -> Result<String, LixError> {
    text_from_value(value)
        .ok_or_else(|| assignment_error(format!("{context} requires column '{key}'")))
}

fn filesystem_path_error(error: crate::LixError) -> LixError {
    error
}

fn text_from_value(value: &Value) -> Option<String> {
    match value {
        Value::Text(value) => Some(value.clone()),
        Value::Integer(value) => Some(value.to_string()),
        Value::Boolean(value) => Some(value.to_string()),
        Value::Real(value) => Some(value.to_string()),
        _ => None,
    }
}

fn value_as_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Boolean(value) => Some(*value),
        Value::Integer(value) => Some(*value != 0),
        Value::Text(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" => Some(true),
            "0" | "false" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        parse_directory_insert_assignments, parse_directory_update_assignments,
        parse_file_insert_assignments, parse_file_update_assignments,
    };
    use crate::functions::{DynFunctionProvider, SharedFunctionProvider, SystemFunctionProvider};
    use crate::Value;
    use std::collections::BTreeMap;

    fn system_functions() -> DynFunctionProvider {
        SharedFunctionProvider::new(Box::new(SystemFunctionProvider))
    }

    #[test]
    fn file_insert_and_update_share_path_normalization() {
        let mut payload = BTreeMap::new();
        payload.insert(
            "path".to_string(),
            Value::Text("/docs/readme.md".to_string()),
        );

        let insert = parse_file_insert_assignments(&payload, &system_functions())
            .expect("insert parse should succeed");
        let update = parse_file_update_assignments(&payload).expect("update parse should succeed");

        assert_eq!(
            insert.path.normalized_path,
            update
                .path
                .expect("update path should exist")
                .normalized_path
        );
    }

    #[test]
    fn directory_insert_and_update_share_name_and_path_normalization() {
        let mut payload = BTreeMap::new();
        payload.insert("path".to_string(), Value::Text("/docs/guides/".to_string()));
        payload.insert("name".to_string(), Value::Text("guides".to_string()));

        let insert = parse_directory_insert_assignments(&payload, &system_functions())
            .expect("insert parse should succeed");
        let update =
            parse_directory_update_assignments(&payload).expect("update parse should succeed");

        assert_eq!(insert.path, update.path);
        assert_eq!(insert.name, update.name);
    }

    #[test]
    fn assignment_parsers_accept_rfc_pchar_segments() {
        let special_directory_name = "guide:alpha@beta!$&()*+,;=";
        let special_file_path = "/docs:alpha@beta/report:summary@v1.txt";

        let mut directory_payload = BTreeMap::new();
        directory_payload.insert(
            "path".to_string(),
            Value::Text(format!("/docs:alpha@beta/{special_directory_name}/")),
        );
        directory_payload.insert(
            "name".to_string(),
            Value::Text(special_directory_name.to_string()),
        );

        let directory_insert =
            parse_directory_insert_assignments(&directory_payload, &system_functions())
                .expect("directory insert parse should succeed");
        let directory_update = parse_directory_update_assignments(&directory_payload)
            .expect("directory update parse should succeed");

        assert_eq!(
            directory_insert.name.as_deref(),
            Some(special_directory_name)
        );
        assert_eq!(
            directory_insert
                .path
                .as_ref()
                .expect("directory path")
                .as_str(),
            format!("/docs:alpha@beta/{special_directory_name}/")
        );
        assert_eq!(directory_insert.path, directory_update.path);
        assert_eq!(directory_insert.name, directory_update.name);

        let mut file_payload = BTreeMap::new();
        file_payload.insert(
            "path".to_string(),
            Value::Text(special_file_path.to_string()),
        );

        let file_insert = parse_file_insert_assignments(&file_payload, &system_functions())
            .expect("file insert parse should succeed");
        let file_update =
            parse_file_update_assignments(&file_payload).expect("file update parse should succeed");

        assert_eq!(file_insert.path.normalized_path.as_str(), special_file_path);
        assert_eq!(file_insert.path.name, "report:summary@v1");
        assert_eq!(file_insert.path.extension.as_deref(), Some("txt"));
        assert_eq!(
            file_update
                .path
                .expect("file update path should exist")
                .normalized_path
                .as_str(),
            special_file_path
        );
    }

    #[test]
    fn assignment_path_errors_preserve_recovery_hints_in_message() {
        let mut payload = BTreeMap::new();
        payload.insert(
            "path".to_string(),
            Value::Text("docs/readme.md".to_string()),
        );

        let error = parse_file_insert_assignments(&payload, &system_functions())
            .expect_err("invalid path should fail");

        assert_eq!(error.code, "LIX_ERROR_PATH_MISSING_LEADING_SLASH");
        assert_eq!(error.description, "path must start with '/'");
        assert_eq!(error.hint.as_deref(), Some("prefix the path with '/'"));
    }
}
