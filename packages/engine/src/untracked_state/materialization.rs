use crate::json_store::{JsonRef, JsonStoreReader};
use crate::storage::StorageReader;
use crate::untracked_state::{MaterializedUntrackedStateRow, UntrackedStateRow};
use crate::{validate_row_metadata, LixError, RowMetadata};

pub(crate) async fn materialize_row<S>(
    json_reader: &mut JsonStoreReader<S>,
    row: UntrackedStateRow,
    projection: &UntrackedMaterializationProjection,
) -> Result<MaterializedUntrackedStateRow, LixError>
where
    S: StorageReader,
{
    let snapshot_content = if projection.snapshot_content {
        load_optional_json(json_reader, row.snapshot_ref.as_ref(), "snapshot_ref").await?
    } else {
        None
    };
    let metadata = if projection.metadata {
        load_optional_metadata(json_reader, row.metadata_ref.as_ref()).await?
    } else {
        None
    };
    Ok(MaterializedUntrackedStateRow {
        entity_id: row.entity_id,
        schema_key: row.schema_key,
        file_id: row.file_id,
        snapshot_content,
        metadata,
        schema_version: row.schema_version,
        created_at: row.created_at,
        updated_at: row.updated_at,
        global: row.global,
        version_id: row.version_id,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UntrackedMaterializationProjection {
    pub(crate) snapshot_content: bool,
    pub(crate) metadata: bool,
}

impl UntrackedMaterializationProjection {
    pub(crate) fn full() -> Self {
        Self {
            snapshot_content: true,
            metadata: true,
        }
    }

    pub(crate) fn from_columns(columns: &[String]) -> Self {
        if columns.is_empty() {
            return Self::full();
        }
        Self {
            snapshot_content: columns.iter().any(|column| column == "snapshot_content"),
            metadata: columns.iter().any(|column| column == "metadata"),
        }
    }
}

async fn load_optional_metadata<S>(
    json_reader: &mut JsonStoreReader<S>,
    json_ref: Option<&JsonRef>,
) -> Result<Option<RowMetadata>, LixError>
where
    S: StorageReader,
{
    let Some(json) = load_optional_json(json_reader, json_ref, "metadata_ref").await? else {
        return Ok(None);
    };
    let metadata = serde_json::from_str::<RowMetadata>(&json).map_err(|error| {
        LixError::new(
            "LIX_ERROR_INVALID_JSON",
            format!("untracked_state metadata_ref is invalid JSON: {error}"),
        )
    })?;
    validate_row_metadata(metadata, "untracked_state metadata_ref").map(Some)
}

async fn load_optional_json<S>(
    json_reader: &mut JsonStoreReader<S>,
    json_ref: Option<&JsonRef>,
    field: &str,
) -> Result<Option<String>, LixError>
where
    S: StorageReader,
{
    let Some(json_ref) = json_ref else {
        return Ok(None);
    };
    let bytes = json_reader.load_bytes(json_ref).await?.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "untracked_state {field} '{}' is missing from json_store",
                json_ref.to_hex()
            ),
        )
    })?;
    String::from_utf8(bytes).map(Some).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "untracked_state {field} '{}' is not valid UTF-8 JSON bytes: {error}",
                json_ref.to_hex()
            ),
        )
    })
}
