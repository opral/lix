use crate::paths::filesystem::{normalize_path_segment, NormalizedDirectoryPath, ParsedFilePath};
use crate::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FilesystemAssignmentsError {
    pub(crate) message: String,
}

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
    pub(crate) metadata: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileInsertAssignments {
    pub(crate) id: Option<String>,
    pub(crate) path: ParsedFilePath,
    pub(crate) hidden: bool,
    pub(crate) metadata: Option<String>,
    pub(crate) data: Option<Vec<u8>>,
}

pub(crate) fn parse_directory_update_assignments(
    payload: &BTreeMap<String, Value>,
) -> Result<DirectoryUpdateAssignments, FilesystemAssignmentsError> {
    if payload.contains_key("id") {
        return Err(FilesystemAssignmentsError {
            message:
                "lix_directory id is immutable; create a new row and delete the old row instead"
                    .to_string(),
        });
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
) -> Result<FileUpdateAssignments, FilesystemAssignmentsError> {
    if payload.contains_key("id") {
        return Err(FilesystemAssignmentsError {
            message: "lix_file id is immutable; create a new row and delete the old row instead"
                .to_string(),
        });
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

pub(crate) fn parse_directory_insert_assignments(
    payload: &BTreeMap<String, Value>,
) -> Result<DirectoryInsertAssignments, FilesystemAssignmentsError> {
    Ok(DirectoryInsertAssignments {
        id: payload.get("id").and_then(text_from_value),
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
            .unwrap_or(false),
        metadata: optional_insert_text(payload, "metadata", "public filesystem directory")?,
    })
}

pub(crate) fn parse_file_insert_assignments(
    payload: &BTreeMap<String, Value>,
) -> Result<FileInsertAssignments, FilesystemAssignmentsError> {
    if !payload
        .keys()
        .any(|key| !matches!(key.as_str(), "data" | "version_id" | "untracked"))
    {
        return Err(FilesystemAssignmentsError {
            message: "file insert requires at least one non-data column".to_string(),
        });
    }

    let raw_path = payload
        .get("path")
        .map(|value| text_value_required(value, "public filesystem file insert", "path"))
        .transpose()?
        .ok_or_else(|| FilesystemAssignmentsError {
            message: "public filesystem file insert requires column 'path'".to_string(),
        })?;

    Ok(FileInsertAssignments {
        id: payload.get("id").and_then(text_from_value),
        path: ParsedFilePath::try_from_path(&raw_path).map_err(filesystem_path_error)?,
        hidden: payload
            .get("hidden")
            .and_then(value_as_bool)
            .unwrap_or(false),
        metadata: optional_insert_text(payload, "metadata", "public filesystem file")?,
        data: insert_blob_value(payload, "data")?,
    })
}

fn optional_text_assignment(
    payload: &BTreeMap<String, Value>,
    key: &str,
    context: &str,
) -> Result<OptionalTextAssignment, FilesystemAssignmentsError> {
    match payload.get(key) {
        None => Ok(OptionalTextAssignment::Unchanged),
        Some(Value::Null) => Ok(OptionalTextAssignment::Set(None)),
        Some(Value::Text(value)) => Ok(OptionalTextAssignment::Set(Some(value.clone()))),
        Some(other) => Err(FilesystemAssignmentsError {
            message: format!("{context} expected text/null {key}, got {other:?}"),
        }),
    }
}

fn blob_assignment(
    payload: &BTreeMap<String, Value>,
    key: &str,
) -> Result<BlobAssignment, FilesystemAssignmentsError> {
    match payload.get(key) {
        None => Ok(BlobAssignment::Unchanged),
        Some(Value::Blob(bytes)) => Ok(BlobAssignment::Set(bytes.clone())),
        Some(Value::Text(_)) => Err(FilesystemAssignmentsError {
            message: crate::errors::FILE_DATA_EXPECTS_BYTES_MESSAGE.to_string(),
        }),
        Some(other) => Err(FilesystemAssignmentsError {
            message: format!("public filesystem resolver expected blob {key}, got {other:?}"),
        }),
    }
}

fn insert_blob_value(
    payload: &BTreeMap<String, Value>,
    key: &str,
) -> Result<Option<Vec<u8>>, FilesystemAssignmentsError> {
    match payload.get(key) {
        None => Ok(None),
        Some(Value::Blob(bytes)) => Ok(Some(bytes.clone())),
        Some(Value::Text(_)) => Err(FilesystemAssignmentsError {
            message: crate::errors::FILE_DATA_EXPECTS_BYTES_MESSAGE.to_string(),
        }),
        Some(other) => Err(FilesystemAssignmentsError {
            message: format!("public filesystem resolver expected blob {key}, got {other:?}"),
        }),
    }
}

fn optional_insert_text(
    payload: &BTreeMap<String, Value>,
    key: &str,
    context: &str,
) -> Result<Option<String>, FilesystemAssignmentsError> {
    match payload.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => text_from_value(value)
            .map(Some)
            .ok_or_else(|| FilesystemAssignmentsError {
                message: format!("{context} expected text/null {key}, got {value:?}"),
            }),
    }
}

fn text_value_required(
    value: &Value,
    context: &str,
    key: &str,
) -> Result<String, FilesystemAssignmentsError> {
    text_from_value(value).ok_or_else(|| FilesystemAssignmentsError {
        message: format!("{context} requires column '{key}'"),
    })
}

fn filesystem_path_error(error: crate::LixError) -> FilesystemAssignmentsError {
    FilesystemAssignmentsError {
        message: error.description,
    }
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
    use crate::Value;
    use std::collections::BTreeMap;

    #[test]
    fn file_insert_and_update_share_path_normalization() {
        let mut payload = BTreeMap::new();
        payload.insert(
            "path".to_string(),
            Value::Text("/docs/readme.md".to_string()),
        );

        let insert = parse_file_insert_assignments(&payload).expect("insert parse should succeed");
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

        let insert =
            parse_directory_insert_assignments(&payload).expect("insert parse should succeed");
        let update =
            parse_directory_update_assignments(&payload).expect("update parse should succeed");

        assert_eq!(insert.path, update.path);
        assert_eq!(insert.name, update.name);
    }
}
