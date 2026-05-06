use crate::json_store::{JsonRef, JsonStoreReader};
use crate::storage::StorageReader;
use crate::tracked_state::tree_types::{TrackedStateKey, TrackedStateValue};
use crate::tracked_state::MaterializedTrackedStateRow;
use crate::{parse_row_metadata, LixError};

pub(crate) async fn materialize_value<S>(
    json_reader: &mut JsonStoreReader<S>,
    key: TrackedStateKey,
    value: TrackedStateValue,
    projection: &TrackedMaterializationProjection,
) -> Result<MaterializedTrackedStateRow, LixError>
where
    S: StorageReader,
{
    let snapshot_content = if projection.snapshot_content {
        load_optional_json(json_reader, value.snapshot_ref.as_ref(), "snapshot_ref").await?
    } else {
        None
    };
    let metadata = if projection.metadata {
        load_optional_metadata(json_reader, value.metadata_ref.as_ref()).await?
    } else {
        None
    };
    Ok(value.into_materialized_row(key, snapshot_content, metadata))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TrackedMaterializationProjection {
    pub(crate) snapshot_content: bool,
    pub(crate) metadata: bool,
}

impl TrackedMaterializationProjection {
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
) -> Result<Option<String>, LixError>
where
    S: StorageReader,
{
    let Some(json) = load_optional_json(json_reader, json_ref, "metadata_ref").await? else {
        return Ok(None);
    };
    parse_row_metadata(&json, "tracked_state metadata_ref").map(Some)
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
                "tracked_state {field} '{}' is missing from json_store",
                json_ref.to_hex()
            ),
        )
    })?;
    String::from_utf8(bytes).map(Some).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "tracked_state {field} '{}' is not valid UTF-8 JSON bytes: {error}",
                json_ref.to_hex()
            ),
        )
    })
}
