use crate::backend::QueryExecutor;
use crate::common::errors::classification::is_missing_relation_error;
use crate::contracts::artifacts::CommittedVersionFrontier;
use crate::live_state::schema_access::tracked_relation_name;
use crate::live_state::{builtin_schema_storage_metadata, BuiltinSchemaStorageLane};
use crate::{LixBackend, LixError, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VersionHeadRef {
    pub(crate) version_id: String,
    pub(crate) commit_id: String,
}

pub(crate) async fn load_all_version_head_commit_ids(
    executor: &mut dyn QueryExecutor,
) -> Result<Vec<(String, String)>, LixError> {
    Ok(load_all_version_head_refs_with_executor(executor)
        .await?
        .into_iter()
        .map(|row| (row.version_id, row.commit_id))
        .collect())
}

pub(crate) async fn load_current_committed_version_frontier_with_backend(
    backend: &dyn LixBackend,
) -> Result<CommittedVersionFrontier, LixError> {
    let mut executor = backend;
    load_current_committed_version_frontier_with_executor(&mut executor).await
}

pub(crate) async fn load_current_committed_version_frontier_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<CommittedVersionFrontier, LixError> {
    Ok(CommittedVersionFrontier {
        version_heads: load_all_version_head_refs_with_executor(executor)
            .await?
            .into_iter()
            .map(|row| (row.version_id, row.commit_id))
            .collect(),
    })
}

async fn load_all_version_head_refs_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<Vec<VersionHeadRef>, LixError> {
    let metadata = version_ref_storage_metadata();
    let result = match executor
        .execute(
            &format!(
                "SELECT entity_id, commit_id \
                 FROM {table} \
                 WHERE schema_key = $1 \
                   AND schema_version = $2 \
                   AND file_id = $3 \
                   AND version_id = $4 \
                   AND plugin_key = $5 \
                   AND untracked = true \
                   AND is_tombstone = 0 \
                   AND commit_id IS NOT NULL \
                   AND commit_id <> '' \
                 ORDER BY entity_id ASC, updated_at DESC",
                table = tracked_relation_name(&metadata.schema_key),
            ),
            &[
                Value::Text(metadata.schema_key.clone()),
                Value::Text(metadata.schema_version.clone()),
                Value::Text(metadata.file_id.clone()),
                Value::Text(version_ref_storage_version_id()),
                Value::Text(metadata.plugin_key.clone()),
            ],
        )
        .await
    {
        Ok(result) => result,
        Err(error) if is_missing_relation_error(&error) => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };

    let mut rows = Vec::with_capacity(result.rows.len());
    let mut previous_version_id: Option<String> = None;
    for row in &result.rows {
        let parsed = parse_version_head_ref_row(row)?;
        if matches!(previous_version_id.as_ref(), Some(previous) if previous == &parsed.version_id)
        {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "local version-head resolution for version '{}' found multiple exact rows",
                    parsed.version_id
                ),
            ));
        }
        previous_version_id = Some(parsed.version_id.clone());
        rows.push(parsed);
    }
    Ok(rows)
}

fn version_ref_storage_metadata() -> crate::live_state::BuiltinSchemaStorageMetadata {
    builtin_schema_storage_metadata("lix_version_ref")
        .expect("lix_version_ref builtin storage metadata should exist")
}

fn version_ref_storage_version_id() -> String {
    match version_ref_storage_metadata().storage_lane {
        BuiltinSchemaStorageLane::Global => crate::version_state::GLOBAL_VERSION_ID.to_string(),
        BuiltinSchemaStorageLane::Local => {
            panic!("lix_version_ref must use the global storage lane")
        }
    }
}

fn parse_version_head_ref_row(row: &[Value]) -> Result<VersionHeadRef, LixError> {
    let version_id = required_text_cell(row, 0, "entity_id")?;
    let commit_id = required_text_cell(row, 1, "commit_id")?;
    if commit_id.is_empty() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "local version head for '{}' has empty commit_id",
                version_id
            ),
        ));
    }
    Ok(VersionHeadRef {
        version_id,
        commit_id,
    })
}

fn required_text_cell(row: &[Value], index: usize, column_name: &str) -> Result<String, LixError> {
    match row.get(index) {
        Some(Value::Text(value)) => Ok(value.clone()),
        Some(Value::Integer(value)) => Ok(value.to_string()),
        Some(_) | None => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("local version-ref row is missing text column '{column_name}'"),
        )),
    }
}
