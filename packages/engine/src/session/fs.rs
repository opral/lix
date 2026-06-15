use serde_json::Value as JsonValue;

use crate::LixError;
use crate::Value;
use crate::storage::StorageBackend;

use super::context::SessionContext;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FsWriteOptions {
    pub metadata: Option<JsonValue>,
    pub untracked: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FsMkdirOptions {
    pub metadata: Option<JsonValue>,
    pub untracked: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FsRmOptions {
    pub recursive: bool,
    pub metadata: Option<JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsDirEntry {
    pub name: String,
    pub path: String,
    pub kind: FsDirEntryKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsDirEntryKind {
    File,
    Directory,
}

#[derive(Clone)]
#[expect(missing_debug_implementations)]
pub struct SessionFs<B: StorageBackend = crate::storage::InMemoryStorageBackend> {
    session: SessionContext<B>,
}

impl<B> SessionContext<B>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    pub fn fs(&self) -> SessionFs<B> {
        SessionFs {
            session: self.clone(),
        }
    }
}

impl<B> SessionFs<B>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    pub async fn write_file(
        &self,
        path: &str,
        data: Vec<u8>,
        options: FsWriteOptions,
    ) -> Result<(), LixError> {
        let path_value = Value::Text(path.to_string());
        let lane_value = Value::Boolean(options.untracked);
        let existing = self
            .session
            .execute(
                "SELECT id, lixcol_untracked FROM lix_file WHERE path = $1",
                std::slice::from_ref(&path_value),
            )
            .await?;
        let metadata_value = json_metadata_value(options.metadata);
        for row in existing.rows() {
            let existing_untracked: bool = row.get("lixcol_untracked")?;
            if existing_untracked != options.untracked {
                return Err(filesystem_conflict_error(format!(
                    "fs.write_file cannot write {} path {path:?} over existing {} file",
                    lane_name(options.untracked),
                    lane_name(existing_untracked)
                )));
            }
        }

        if existing.is_empty() {
            self.session
                .execute(
                    "INSERT INTO lix_file (path, data, lixcol_metadata, lixcol_untracked) \
                     VALUES ($1, $2, $3, $4)",
                    &[path_value, Value::Blob(data), metadata_value, lane_value],
                )
                .await?;
        } else {
            self.session
                .execute(
                    "UPDATE lix_file \
                     SET data = $2, lixcol_metadata = $3 \
                     WHERE path = $1 AND lixcol_untracked = $4",
                    &[path_value, Value::Blob(data), metadata_value, lane_value],
                )
                .await?;
        }
        Ok(())
    }

    pub async fn read_file(&self, path: &str) -> Result<Option<Vec<u8>>, LixError> {
        let path_value = Value::Text(path.to_string());
        let file_result = self
            .session
            .execute(
                "SELECT data FROM lix_file WHERE path = $1",
                std::slice::from_ref(&path_value),
            )
            .await?;
        if let Some(row) = file_result.rows().first() {
            return Ok(Some(row.get("data")?));
        }

        let directory_result = self
            .session
            .execute(
                "SELECT id FROM lix_directory \
                 WHERE path = $1 OR path = concat($1, '/') \
                 LIMIT 1",
                &[path_value],
            )
            .await?;
        if !directory_result.is_empty() {
            return Err(wrong_kind_error(path, "file", "directory"));
        }
        Ok(None)
    }

    pub async fn mkdir(&self, path: &str, options: FsMkdirOptions) -> Result<(), LixError> {
        let path_value = Value::Text(path.to_string());
        let root_result = self
            .session
            .execute(
                "SELECT 1 AS is_root WHERE $1 = '/'",
                std::slice::from_ref(&path_value),
            )
            .await?;
        if !root_result.is_empty() {
            return Ok(());
        }

        let existing = self
            .session
            .execute(
                "SELECT id, lixcol_untracked FROM lix_directory WHERE path = $1",
                std::slice::from_ref(&path_value),
            )
            .await?;
        for row in existing.rows() {
            let existing_untracked: bool = row.get("lixcol_untracked")?;
            if existing_untracked != options.untracked {
                return Err(filesystem_conflict_error(format!(
                    "fs.mkdir cannot write {} path {path:?} over existing {} directory",
                    lane_name(options.untracked),
                    lane_name(existing_untracked)
                )));
            }
        }
        if !existing.is_empty() {
            return Ok(());
        }

        self.session
            .execute(
                "INSERT INTO lix_directory (path, lixcol_metadata, lixcol_untracked) \
                 VALUES ($1, $2, $3)",
                &[
                    path_value,
                    json_metadata_value(options.metadata),
                    Value::Boolean(options.untracked),
                ],
            )
            .await?;
        Ok(())
    }

    pub async fn readdir(&self, path: &str) -> Result<Option<Vec<FsDirEntry>>, LixError> {
        let path_value = Value::Text(path.to_string());
        let root_result = self
            .session
            .execute(
                "SELECT 1 AS is_root WHERE $1 = '/'",
                std::slice::from_ref(&path_value),
            )
            .await?;
        let parent_id = if root_result.is_empty() {
            let directory_result = self
                .session
                .execute(
                    "SELECT id FROM lix_directory WHERE path = $1 LIMIT 1",
                    std::slice::from_ref(&path_value),
                )
                .await?;
            if let Some(row) = directory_result.rows().first() {
                Some(row.get::<String>("id")?)
            } else {
                let file_result = self
                    .session
                    .execute(
                        "SELECT id FROM lix_file \
                         WHERE path = $1 OR concat(path, '/') = $1 \
                         LIMIT 1",
                        &[path_value],
                    )
                    .await?;
                if !file_result.is_empty() {
                    return Err(wrong_kind_error(path, "directory", "file"));
                }
                return Ok(None);
            }
        } else {
            None
        };

        let entries = match parent_id {
            Some(parent_id) => {
                self.session
                    .execute(
                        "SELECT name, path, 'directory' AS kind, 0 AS kind_order \
                         FROM lix_directory \
                         WHERE parent_id = $1 \
                         UNION ALL \
                         SELECT name, path, 'file' AS kind, 1 AS kind_order \
                         FROM lix_file \
                         WHERE directory_id = $1 \
                         ORDER BY name, kind_order",
                        &[Value::Text(parent_id)],
                    )
                    .await?
            }
            None => {
                self.session
                    .execute(
                        "SELECT name, path, 'directory' AS kind, 0 AS kind_order \
                         FROM lix_directory \
                         WHERE parent_id IS NULL \
                         UNION ALL \
                         SELECT name, path, 'file' AS kind, 1 AS kind_order \
                         FROM lix_file \
                         WHERE directory_id IS NULL \
                         ORDER BY name, kind_order",
                        &[],
                    )
                    .await?
            }
        };
        Ok(Some(fs_entries_from_rows(&entries)?))
    }

    pub async fn rm(&self, path: &str, options: FsRmOptions) -> Result<(), LixError> {
        let path_value = Value::Text(path.to_string());
        let root_result = self
            .session
            .execute(
                "SELECT 1 AS is_root WHERE $1 = '/'",
                std::slice::from_ref(&path_value),
            )
            .await?;
        if !root_result.is_empty() {
            return Err(LixError::new(
                LixError::CODE_CONSTRAINT_VIOLATION,
                "fs.rm cannot remove the root directory",
            ));
        }

        let file_result = self
            .session
            .execute(
                "SELECT id FROM lix_file WHERE path = $1 LIMIT 1",
                std::slice::from_ref(&path_value),
            )
            .await?;
        if let Some(row) = file_result.rows().first() {
            let file_id: String = row.get("id")?;
            self.session
                .execute(
                    "DELETE FROM lix_file WHERE id = $1",
                    &[Value::Text(file_id)],
                )
                .await?;
            return Ok(());
        }

        let directory_result = self
            .session
            .execute(
                "SELECT id, path FROM lix_directory \
                 WHERE path = $1 OR path = concat($1, '/') \
                 LIMIT 1",
                std::slice::from_ref(&path_value),
            )
            .await?;
        let Some(directory_row) = directory_result.rows().first() else {
            let file_as_directory = self
                .session
                .execute(
                    "SELECT id FROM lix_file WHERE concat(path, '/') = $1 LIMIT 1",
                    &[path_value],
                )
                .await?;
            if !file_as_directory.is_empty() {
                return Err(wrong_kind_error(path, "directory", "file"));
            }
            return Ok(());
        };
        let directory_id: String = directory_row.get("id")?;
        let directory_path: String = directory_row.get("path")?;

        if !options.recursive {
            let child_result = self
                .session
                .execute(
                    "SELECT name FROM lix_directory WHERE parent_id = $1 \
                     UNION ALL \
                     SELECT name FROM lix_file WHERE directory_id = $1 \
                     LIMIT 1",
                    &[Value::Text(directory_id.clone())],
                )
                .await?;
            if !child_result.is_empty() {
                return Err(LixError::new(
                    LixError::CODE_CONSTRAINT_VIOLATION,
                    format!(
                        "fs.rm cannot remove non-empty directory {directory_path:?} without recursive=true"
                    ),
                ));
            }
        }

        self.session
            .execute(
                "DELETE FROM lix_directory WHERE id = $1",
                &[Value::Text(directory_id)],
            )
            .await?;
        Ok(())
    }
}

fn lane_name(untracked: bool) -> &'static str {
    if untracked { "untracked" } else { "tracked" }
}

fn json_metadata_value(value: Option<JsonValue>) -> Value {
    value.map(Value::Json).unwrap_or(Value::Null)
}

fn fs_entries_from_rows(result: &crate::ExecuteResult) -> Result<Vec<FsDirEntry>, LixError> {
    result
        .rows()
        .iter()
        .map(|row| {
            let kind = match row.get::<String>("kind")?.as_str() {
                "directory" => FsDirEntryKind::Directory,
                "file" => FsDirEntryKind::File,
                other => {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("fs.readdir SQL returned unknown entry kind {other:?}"),
                    ));
                }
            };
            Ok(FsDirEntry {
                name: row.get("name")?,
                path: row.get("path")?,
                kind,
            })
        })
        .collect()
}

fn wrong_kind_error(path: &str, expected: &str, actual: &str) -> LixError {
    LixError::new(
        LixError::CODE_CONSTRAINT_VIOLATION,
        format!("fs path {path:?} expected {expected}, found {actual}"),
    )
}

fn filesystem_conflict_error(message: String) -> LixError {
    LixError::new(LixError::CODE_CONSTRAINT_VIOLATION, message)
}
