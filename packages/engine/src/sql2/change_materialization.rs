use crate::changelog::SegmentInlinePayload;
use crate::commit_graph::LocatedChange;
use crate::entity_identity::EntityIdentity;
use crate::json_store::{JsonLoadRequestRef, JsonReadScopeRef, JsonRef, JsonStoreReader};
use crate::storage::StorageRead;
use crate::{parse_row_metadata, LixError};

/// Read-boundary view of a changelog change with JSON refs resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaterializedChange {
    pub(crate) id: String,
    pub(crate) entity_id: EntityIdentity,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) created_at: String,
}

pub(crate) async fn materialize_changelog_change<S>(
    json_reader: &mut JsonStoreReader<S>,
    located: LocatedChange,
) -> Result<MaterializedChange, LixError>
where
    S: StorageRead,
{
    let change = located.record;
    let snapshot_content = load_optional_changelog_json_text(
        json_reader,
        change.snapshot_ref.as_ref(),
        &located.inline_payloads,
        "snapshot_ref",
    )
    .await?;
    let metadata = match load_optional_changelog_json_text(
        json_reader,
        change.metadata_ref.as_ref(),
        &located.inline_payloads,
        "metadata_ref",
    )
    .await?
    {
        Some(value) => Some(parse_row_metadata(&value, "changelog change metadata_ref")?),
        None => None,
    };
    Ok(MaterializedChange {
        id: change.id,
        entity_id: change.entity_id,
        schema_key: change.schema_key,
        file_id: change.file_id,
        snapshot_content,
        metadata,
        created_at: change.created_at,
    })
}

async fn load_optional_changelog_json_text<S>(
    json_reader: &mut JsonStoreReader<S>,
    json_ref: Option<&JsonRef>,
    inline_payloads: &[SegmentInlinePayload],
    field: &str,
) -> Result<Option<String>, LixError>
where
    S: StorageRead,
{
    let Some(json_ref) = json_ref else {
        return Ok(None);
    };
    if let Some(payload) = inline_payloads
        .iter()
        .find(|payload| &payload.json_ref == json_ref)
    {
        return String::from_utf8(payload.bytes.clone())
            .map(Some)
            .map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("changelog change {field} is not UTF-8 JSON: {error}"),
                )
            });
    }

    let batch = json_reader
        .load_bytes_many(JsonLoadRequestRef {
            refs: std::slice::from_ref(json_ref),
            scope: JsonReadScopeRef::OutOfBand,
        })
        .await?;
    let Some(bytes) = batch.into_values().into_iter().next().flatten() else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "changelog change {field} '{}' is missing",
                json_ref.to_hex()
            ),
        ));
    };
    String::from_utf8(bytes).map(Some).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("changelog change {field} is not UTF-8 JSON: {error}"),
        )
    })
}
