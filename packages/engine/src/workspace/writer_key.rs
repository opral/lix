#![allow(dead_code)]
// Step C introduces the workspace-owned storage path before execution and read
// plumbing starts consuming it in later steps.

//! Workspace-owned durable annotation storage for `writer_key`.
//!
//! The storage key is the semantic row identity:
//! `(version_id, schema_key, entity_id, file_id)`.
//!
//! This table is intentionally separate from canonical changes, replica-local
//! version-head state, and derived live-state tables. It stores only workspace
//! annotation data that may later be overlaid onto state-shaped reads.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;

use crate::backend::QueryExecutor;
use crate::change_view::TrackedDomainChangeView;
use crate::live_state::RowIdentity;
use crate::{LixBackend, LixError, Value};

pub(crate) const WORKSPACE_WRITER_KEY_TABLE: &str = "lix_internal_workspace_writer_key";

#[async_trait(?Send)]
pub(crate) trait WorkspaceWriterKeyReadView {
    async fn load_annotation(&self, row_identity: &RowIdentity)
        -> Result<Option<String>, LixError>;

    async fn load_annotations(
        &self,
        row_identities: &BTreeSet<RowIdentity>,
    ) -> Result<BTreeMap<RowIdentity, Option<String>>, LixError>;
}

#[async_trait(?Send)]
impl<T> WorkspaceWriterKeyReadView for T
where
    T: LixBackend,
{
    async fn load_annotation(
        &self,
        row_identity: &RowIdentity,
    ) -> Result<Option<String>, LixError> {
        load_workspace_writer_key_annotation(self, row_identity).await
    }

    async fn load_annotations(
        &self,
        row_identities: &BTreeSet<RowIdentity>,
    ) -> Result<BTreeMap<RowIdentity, Option<String>>, LixError> {
        load_workspace_writer_key_annotations(self, row_identities).await
    }
}

pub(crate) fn tracked_writer_key_annotations_from_changes<Change: TrackedDomainChangeView>(
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

pub(crate) async fn load_workspace_writer_key_annotation(
    backend: &dyn LixBackend,
    row_identity: &RowIdentity,
) -> Result<Option<String>, LixError> {
    let mut executor = backend;
    load_workspace_writer_key_annotation_with_executor(&mut executor, row_identity).await
}

pub(crate) async fn load_workspace_writer_key_annotation_with_executor(
    executor: &mut dyn QueryExecutor,
    row_identity: &RowIdentity,
) -> Result<Option<String>, LixError> {
    let result = executor
        .execute(
            &format!(
                "SELECT writer_key \
                 FROM {WORKSPACE_WRITER_KEY_TABLE} \
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
            format!("workspace writer_key value must be text, got {other:?}"),
        )),
    }
}

pub(crate) async fn load_workspace_writer_key_annotations(
    backend: &dyn LixBackend,
    row_identities: &BTreeSet<RowIdentity>,
) -> Result<BTreeMap<RowIdentity, Option<String>>, LixError> {
    let mut executor = backend;
    load_workspace_writer_key_annotations_with_executor(&mut executor, row_identities).await
}

pub(crate) async fn load_workspace_writer_key_annotations_with_executor(
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
        load_workspace_writer_key_annotations_for_versions_with_executor(executor, &version_ids)
            .await?;

    Ok(row_identities
        .iter()
        .cloned()
        .map(|identity| {
            let writer_key = stored.get(&identity).cloned();
            (identity, writer_key)
        })
        .collect())
}

pub(crate) async fn load_workspace_writer_key_annotations_for_versions(
    backend: &dyn LixBackend,
    version_ids: &BTreeSet<String>,
) -> Result<BTreeMap<RowIdentity, String>, LixError> {
    let mut executor = backend;
    load_workspace_writer_key_annotations_for_versions_with_executor(&mut executor, version_ids)
        .await
}

pub(crate) async fn load_workspace_writer_key_annotations_for_versions_with_executor(
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
         FROM {WORKSPACE_WRITER_KEY_TABLE} \
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

pub(crate) async fn persist_workspace_writer_key_annotation(
    backend: &dyn LixBackend,
    row_identity: &RowIdentity,
    writer_key: &str,
) -> Result<(), LixError> {
    let mut executor = backend;
    persist_workspace_writer_key_annotation_with_executor(&mut executor, row_identity, writer_key)
        .await
}

pub(crate) async fn persist_workspace_writer_key_annotation_with_executor(
    executor: &mut dyn QueryExecutor,
    row_identity: &RowIdentity,
    writer_key: &str,
) -> Result<(), LixError> {
    executor
        .execute(
            &format!(
                "INSERT INTO {WORKSPACE_WRITER_KEY_TABLE} (\
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

pub(crate) async fn apply_workspace_writer_key_annotations_with_executor(
    executor: &mut dyn QueryExecutor,
    annotations: &BTreeMap<RowIdentity, Option<String>>,
) -> Result<(), LixError> {
    for (row_identity, writer_key) in annotations {
        match writer_key
            .as_deref()
            .filter(|writer_key| !writer_key.is_empty())
        {
            Some(writer_key) => {
                persist_workspace_writer_key_annotation_with_executor(
                    executor,
                    row_identity,
                    writer_key,
                )
                .await?;
            }
            None => {
                delete_workspace_writer_key_annotation_with_executor(executor, row_identity)
                    .await?;
            }
        }
    }
    Ok(())
}

pub(crate) async fn delete_workspace_writer_key_annotation(
    backend: &dyn LixBackend,
    row_identity: &RowIdentity,
) -> Result<(), LixError> {
    let mut executor = backend;
    delete_workspace_writer_key_annotation_with_executor(&mut executor, row_identity).await
}

pub(crate) async fn delete_workspace_writer_key_annotation_with_executor(
    executor: &mut dyn QueryExecutor,
    row_identity: &RowIdentity,
) -> Result<(), LixError> {
    executor
        .execute(
            &format!(
                "DELETE FROM {WORKSPACE_WRITER_KEY_TABLE} \
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

fn required_text_value(row: &[Value], index: usize, column: &str) -> Result<String, LixError> {
    match row.get(index) {
        Some(Value::Text(value)) => Ok(value.clone()),
        Some(other) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("workspace writer_key column '{column}' must be text, got {other:?}"),
        )),
        None => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("workspace writer_key row is missing required column '{column}'"),
        )),
    }
}

fn tracked_change_row_identity<Change: TrackedDomainChangeView>(
    change: &Change,
) -> Option<RowIdentity> {
    Some(RowIdentity {
        version_id: change.version_id().to_string(),
        schema_key: change.schema_key().to_string(),
        entity_id: change.entity_id().to_string(),
        file_id: change.file_id()?.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::{
        delete_workspace_writer_key_annotation, load_workspace_writer_key_annotation,
        load_workspace_writer_key_annotations_for_versions,
        persist_workspace_writer_key_annotation,
    };
    use crate::live_state::RowIdentity;
    use crate::test_support::boot_test_engine;
    use std::collections::BTreeSet;

    #[test]
    fn workspace_writer_key_annotation_roundtrips_by_row_identity() {
        run_workspace_writer_key_test(|| async {
            let (backend, _engine, _session) = boot_test_engine()
                .await
                .expect("boot test engine should succeed");
            let identity = RowIdentity {
                version_id: "main".to_string(),
                schema_key: "test_schema".to_string(),
                entity_id: "entity-1".to_string(),
                file_id: "file-1".to_string(),
            };

            persist_workspace_writer_key_annotation(&backend, &identity, "writer-a")
                .await
                .expect("persisting workspace writer_key should succeed");

            let loaded = load_workspace_writer_key_annotation(&backend, &identity)
                .await
                .expect("loading workspace writer_key should succeed");
            assert_eq!(loaded.as_deref(), Some("writer-a"));
        });
    }

    #[test]
    fn workspace_writer_key_annotations_are_partitioned_by_row_identity_and_version() {
        run_workspace_writer_key_test(|| async {
            let (backend, _engine, _session) = boot_test_engine()
                .await
                .expect("boot test engine should succeed");
            let main_identity = RowIdentity {
                version_id: "main".to_string(),
                schema_key: "test_schema".to_string(),
                entity_id: "entity-1".to_string(),
                file_id: "file-1".to_string(),
            };
            let other_version_identity = RowIdentity {
                version_id: "other-version".to_string(),
                schema_key: "test_schema".to_string(),
                entity_id: "entity-1".to_string(),
                file_id: "file-1".to_string(),
            };

            persist_workspace_writer_key_annotation(&backend, &main_identity, "writer-main")
                .await
                .expect("persisting main writer_key should succeed");
            persist_workspace_writer_key_annotation(
                &backend,
                &other_version_identity,
                "writer-other",
            )
            .await
            .expect("persisting other version writer_key should succeed");

            let loaded = load_workspace_writer_key_annotations_for_versions(
                &backend,
                &BTreeSet::from(["main".to_string()]),
            )
            .await
            .expect("loading workspace writer_key annotations should succeed");

            assert_eq!(loaded.len(), 1);
            assert_eq!(
                loaded.get(&main_identity).map(String::as_str),
                Some("writer-main")
            );
            assert!(!loaded.contains_key(&other_version_identity));
        });
    }

    #[test]
    fn deleting_workspace_writer_key_annotation_removes_only_annotation_row() {
        run_workspace_writer_key_test(|| async {
            let (backend, _engine, _session) = boot_test_engine()
                .await
                .expect("boot test engine should succeed");
            let identity = RowIdentity {
                version_id: "main".to_string(),
                schema_key: "test_schema".to_string(),
                entity_id: "entity-1".to_string(),
                file_id: "file-1".to_string(),
            };

            persist_workspace_writer_key_annotation(&backend, &identity, "writer-a")
                .await
                .expect("persisting workspace writer_key should succeed");
            delete_workspace_writer_key_annotation(&backend, &identity)
                .await
                .expect("deleting workspace writer_key should succeed");

            let loaded = load_workspace_writer_key_annotation(&backend, &identity)
                .await
                .expect("loading workspace writer_key after delete should succeed");
            assert!(loaded.is_none());
        });
    }

    fn run_workspace_writer_key_test<Factory, Future>(factory: Factory)
    where
        Factory: FnOnce() -> Future + Send + 'static,
        Future: std::future::Future<Output = ()> + 'static,
    {
        std::thread::Builder::new()
            .name("workspace-writer-key-test".to_string())
            .stack_size(64 * 1024 * 1024)
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("workspace writer_key test runtime should build")
                    .block_on(factory());
            })
            .expect("workspace writer_key test thread should spawn")
            .join()
            .expect("workspace writer_key test thread should join");
    }
}
