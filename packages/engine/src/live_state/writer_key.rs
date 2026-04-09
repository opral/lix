#![allow(dead_code)]
//! Durable writer-key annotation storage owned by `live_state`.
//!
//! The storage key is the semantic row identity:
//! `(version_id, schema_key, entity_id, file_id)`.
//!
//! This table is intentionally separate from canonical changes and live-state
//! tracked/untracked row tables. It stores only the writer-key metadata that
//! live-state overlays onto served rows while the migration to pure row-shaped
//! APIs is still in progress.

use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
use async_trait::async_trait;

use crate::backend::ddl::table_exists;
use crate::backend::QueryExecutor;
use crate::contracts::artifacts::RowIdentity;
use crate::contracts::change::TrackedChangeView;
#[cfg(test)]
pub(crate) use crate::contracts::traits::WriterKeyReadView;
use crate::{LixBackend, LixError, Value};

pub(crate) const WRITER_KEY_TABLE: &str = "lix_internal_writer_key";
const LEGACY_WRITER_KEY_TABLE: &str = "lix_internal_workspace_writer_key";
const CREATE_WRITER_KEY_TABLE_SQL: &str = "CREATE TABLE IF NOT EXISTS lix_internal_writer_key (\
     version_id TEXT NOT NULL, \
     schema_key TEXT NOT NULL, \
     entity_id TEXT NOT NULL, \
     file_id TEXT NOT NULL, \
     writer_key TEXT NOT NULL, \
     PRIMARY KEY (version_id, schema_key, entity_id, file_id)\
     )";

#[cfg(test)]
#[async_trait(?Send)]
impl<T> WriterKeyReadView for T
where
    T: LixBackend,
{
    async fn load_annotation(
        &self,
        row_identity: &RowIdentity,
    ) -> Result<Option<String>, LixError> {
        load_writer_key_annotation(self, row_identity).await
    }

    async fn load_annotations(
        &self,
        row_identities: &BTreeSet<RowIdentity>,
    ) -> Result<BTreeMap<RowIdentity, Option<String>>, LixError> {
        load_writer_key_annotations(self, row_identities).await
    }
}

pub(crate) async fn ensure_writer_key_table_ready(
    backend: &dyn LixBackend,
) -> Result<(), LixError> {
    if table_exists(backend, WRITER_KEY_TABLE).await? {
        return Ok(());
    }

    if table_exists(backend, LEGACY_WRITER_KEY_TABLE).await? {
        backend
            .execute(
                &format!("ALTER TABLE {LEGACY_WRITER_KEY_TABLE} RENAME TO {WRITER_KEY_TABLE}"),
                &[],
            )
            .await?;
        return Ok(());
    }

    backend.execute(CREATE_WRITER_KEY_TABLE_SQL, &[]).await?;
    Ok(())
}

pub(crate) fn tracked_writer_key_annotations_from_changes<Change: TrackedChangeView>(
    changes: &[Change],
    execution_writer_key: Option<&str>,
) -> BTreeMap<RowIdentity, Option<String>> {
    let mut annotations = BTreeMap::new();
    for change in changes {
        let Some(row_identity) = tracked_change_row_identity(change) else {
            continue;
        };
        annotations.insert(
            row_identity,
            change
                .writer_key()
                .map(str::to_string)
                .or_else(|| execution_writer_key.map(str::to_string)),
        );
    }
    annotations
}

pub(crate) async fn load_writer_key_annotation(
    backend: &dyn LixBackend,
    row_identity: &RowIdentity,
) -> Result<Option<String>, LixError> {
    let mut executor = backend;
    load_writer_key_annotation_with_executor(&mut executor, row_identity).await
}

pub(crate) async fn load_writer_key_annotation_for_state_row(
    backend: &dyn LixBackend,
    version_id: &str,
    schema_key: &str,
    entity_id: &str,
    file_id: &str,
) -> Result<Option<String>, LixError> {
    load_writer_key_annotation(
        backend,
        &RowIdentity {
            version_id: version_id.to_string(),
            schema_key: schema_key.to_string(),
            entity_id: entity_id.to_string(),
            file_id: file_id.to_string(),
        },
    )
    .await
}

pub(crate) async fn load_writer_key_annotation_with_executor(
    executor: &mut dyn QueryExecutor,
    row_identity: &RowIdentity,
) -> Result<Option<String>, LixError> {
    let result = executor
        .execute(
            &format!(
                "SELECT writer_key \
                 FROM {WRITER_KEY_TABLE} \
                 WHERE version_id = $1 \
                   AND schema_key = $2 \
                   AND entity_id = $3 \
                   AND file_id = $4 \
                 LIMIT 1"
            ),
            &[
                Value::Text(row_identity.version_id.clone()),
                Value::Text(row_identity.schema_key.clone()),
                Value::Text(row_identity.entity_id.clone()),
                Value::Text(row_identity.file_id.clone()),
            ],
        )
        .await?;

    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    match row.first() {
        Some(Value::Text(writer_key)) if !writer_key.is_empty() => Ok(Some(writer_key.clone())),
        Some(Value::Text(_)) | Some(Value::Null) | None => Ok(None),
        Some(other) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("writer_key value must be text, got {other:?}"),
        )),
    }
}

pub(crate) async fn load_writer_key_annotations(
    backend: &dyn LixBackend,
    row_identities: &BTreeSet<RowIdentity>,
) -> Result<BTreeMap<RowIdentity, Option<String>>, LixError> {
    let mut executor = backend;
    load_writer_key_annotations_with_executor(&mut executor, row_identities).await
}

pub(crate) async fn load_writer_key_annotations_with_executor(
    executor: &mut dyn QueryExecutor,
    row_identities: &BTreeSet<RowIdentity>,
) -> Result<BTreeMap<RowIdentity, Option<String>>, LixError> {
    if row_identities.is_empty() {
        return Ok(BTreeMap::new());
    }

    let version_ids = row_identities
        .iter()
        .map(|identity| identity.version_id.clone())
        .collect::<BTreeSet<_>>();
    let stored =
        load_writer_key_annotations_for_versions_with_executor(executor, &version_ids).await?;

    Ok(row_identities
        .iter()
        .cloned()
        .map(|identity| {
            let writer_key = stored.get(&identity).cloned();
            (identity, writer_key)
        })
        .collect())
}

pub(crate) async fn load_writer_key_annotations_for_versions(
    backend: &dyn LixBackend,
    version_ids: &BTreeSet<String>,
) -> Result<BTreeMap<RowIdentity, String>, LixError> {
    let mut executor = backend;
    load_writer_key_annotations_for_versions_with_executor(&mut executor, version_ids).await
}

pub(crate) async fn load_writer_key_annotations_for_versions_with_executor(
    executor: &mut dyn QueryExecutor,
    version_ids: &BTreeSet<String>,
) -> Result<BTreeMap<RowIdentity, String>, LixError> {
    if version_ids.is_empty() {
        return Ok(BTreeMap::new());
    }

    let params = version_ids
        .iter()
        .cloned()
        .map(Value::Text)
        .collect::<Vec<_>>();
    let placeholders = (1..=params.len())
        .map(|index| format!("${index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT version_id, schema_key, entity_id, file_id, writer_key \
         FROM {WRITER_KEY_TABLE} \
         WHERE version_id IN ({placeholders})"
    );
    let result = executor.execute(&sql, &params).await?;

    let mut annotations = BTreeMap::new();
    for row in result.rows {
        annotations.insert(
            RowIdentity {
                version_id: required_text_value(&row, 0, "version_id")?,
                schema_key: required_text_value(&row, 1, "schema_key")?,
                entity_id: required_text_value(&row, 2, "entity_id")?,
                file_id: required_text_value(&row, 3, "file_id")?,
            },
            required_text_value(&row, 4, "writer_key")?,
        );
    }

    Ok(annotations)
}

pub(crate) async fn persist_writer_key_annotation(
    backend: &dyn LixBackend,
    row_identity: &RowIdentity,
    writer_key: &str,
) -> Result<(), LixError> {
    let mut executor = backend;
    persist_writer_key_annotation_with_executor(&mut executor, row_identity, writer_key).await
}

pub(crate) async fn persist_writer_key_annotation_with_executor(
    executor: &mut dyn QueryExecutor,
    row_identity: &RowIdentity,
    writer_key: &str,
) -> Result<(), LixError> {
    executor
        .execute(
            &format!(
                "INSERT INTO {WRITER_KEY_TABLE} (\
                 version_id, schema_key, entity_id, file_id, writer_key\
                 ) VALUES ($1, $2, $3, $4, $5) \
                 ON CONFLICT (version_id, schema_key, entity_id, file_id) \
                 DO UPDATE SET writer_key = excluded.writer_key"
            ),
            &[
                Value::Text(row_identity.version_id.clone()),
                Value::Text(row_identity.schema_key.clone()),
                Value::Text(row_identity.entity_id.clone()),
                Value::Text(row_identity.file_id.clone()),
                Value::Text(writer_key.to_string()),
            ],
        )
        .await?;
    Ok(())
}

pub(crate) async fn apply_writer_key_annotations_with_executor(
    executor: &mut dyn QueryExecutor,
    annotations: &BTreeMap<RowIdentity, Option<String>>,
) -> Result<(), LixError> {
    for (row_identity, writer_key) in annotations {
        match writer_key
            .as_deref()
            .filter(|writer_key| !writer_key.is_empty())
        {
            Some(writer_key) => {
                persist_writer_key_annotation_with_executor(executor, row_identity, writer_key)
                    .await?;
            }
            None => {
                clear_writer_key_annotation_with_executor(executor, row_identity).await?;
            }
        }
    }

    Ok(())
}

async fn clear_writer_key_annotation_with_executor(
    executor: &mut dyn QueryExecutor,
    row_identity: &RowIdentity,
) -> Result<(), LixError> {
    executor
        .execute(
            &format!(
                "DELETE FROM {WRITER_KEY_TABLE} \
                 WHERE version_id = $1 \
                   AND schema_key = $2 \
                   AND entity_id = $3 \
                   AND file_id = $4"
            ),
            &[
                Value::Text(row_identity.version_id.clone()),
                Value::Text(row_identity.schema_key.clone()),
                Value::Text(row_identity.entity_id.clone()),
                Value::Text(row_identity.file_id.clone()),
            ],
        )
        .await?;
    Ok(())
}

fn tracked_change_row_identity<Change: TrackedChangeView>(change: &Change) -> Option<RowIdentity> {
    let file_id = change.file_id()?;
    Some(RowIdentity {
        version_id: change.version_id().to_string(),
        schema_key: change.schema_key().to_string(),
        entity_id: change.entity_id().to_string(),
        file_id: file_id.to_string(),
    })
}

fn required_text_value(row: &[Value], index: usize, column: &str) -> Result<String, LixError> {
    match row.get(index) {
        Some(Value::Text(value)) => Ok(value.clone()),
        Some(other) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("writer_key {column} must be text, got {other:?}"),
        )),
        None => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("writer_key row missing {column}"),
        )),
    }
}
