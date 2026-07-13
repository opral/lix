use crate::changelog::ChangeRecord;
use crate::entity_pk::EntityPk;
use crate::json_store::{JsonLoadRequestRef, JsonReadScopeRef, JsonStoreReader};
use crate::storage::StorageRead;
use crate::{LixError, parse_row_metadata};

/// Read-boundary view of a changelog change with JSON refs resolved.
///
/// `lix_change` materializes direct durable `changelog.change` facts and
/// derived `lix_commit` changes from `changelog.commit`. History surfaces
/// materialize reachability-aware commit-graph changes, while traversal context
/// stays outside this row shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaterializedChange {
    pub(crate) id: String,
    pub(crate) entity_pk: EntityPk,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) created_at: String,
    pub(crate) origin_key: Option<String>,
}

/// JSON payloads that a change scan must resolve for its output or filters.
///
/// The durable change record keeps these fields as inline JSON or content
/// references. Callers that only need identity columns can leave both flags
/// disabled and avoid crossing the JSON-store boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChangePayloadProjection {
    pub(crate) snapshot_content: bool,
    pub(crate) metadata: bool,
}

impl ChangePayloadProjection {
    pub(crate) const ALL: Self = Self {
        snapshot_content: true,
        metadata: true,
    };
}

pub(crate) async fn materialize_located_history_change<S>(
    json_reader: &mut JsonStoreReader<S>,
    change: crate::commit_graph::CommitGraphChange,
) -> Result<MaterializedChange, LixError>
where
    S: StorageRead,
{
    materialize_commit_graph_change(json_reader, change, ChangePayloadProjection::ALL).await
}

pub(crate) async fn materialize_changelog_change_record<S>(
    json_reader: &mut JsonStoreReader<S>,
    change: ChangeRecord,
    payload_projection: ChangePayloadProjection,
) -> Result<MaterializedChange, LixError>
where
    S: StorageRead,
{
    materialize_commit_graph_change(
        json_reader,
        crate::commit_graph::CommitGraphChange {
            id: change.change_id,
            entity_pk: change.entity_pk,
            schema_key: change.schema_key,
            file_id: change.file_id,
            snapshot: change.snapshot,
            metadata: change.metadata,
            created_at: change.created_at,
            origin_key: change.origin_key,
        },
        payload_projection,
    )
    .await
}

pub(crate) async fn materialize_commit_graph_change<S>(
    json_reader: &mut JsonStoreReader<S>,
    change: crate::commit_graph::CommitGraphChange,
    payload_projection: ChangePayloadProjection,
) -> Result<MaterializedChange, LixError>
where
    S: StorageRead,
{
    let snapshot_content = if payload_projection.snapshot_content {
        load_changelog_json_slot(json_reader, &change.snapshot, "snapshot").await?
    } else {
        None
    };
    let metadata = if payload_projection.metadata {
        match load_changelog_json_slot(json_reader, &change.metadata, "metadata").await? {
            Some(value) => Some(parse_row_metadata(&value, "changelog change metadata_ref")?),
            None => None,
        }
    } else {
        None
    };
    Ok(MaterializedChange {
        id: change.id.to_string(),
        entity_pk: change.entity_pk,
        schema_key: change.schema_key,
        file_id: change.file_id,
        snapshot_content,
        metadata,
        created_at: change.created_at.to_string(),
        origin_key: change.origin_key,
    })
}

async fn load_changelog_json_slot<S>(
    json_reader: &mut JsonStoreReader<S>,
    slot: &crate::json_store::JsonSlot,
    field: &str,
) -> Result<Option<String>, LixError>
where
    S: StorageRead,
{
    let json_ref = match slot {
        crate::json_store::JsonSlot::None => return Ok(None),
        crate::json_store::JsonSlot::Inline(json) => return Ok(Some(json.to_string())),
        crate::json_store::JsonSlot::Ref(json_ref) => json_ref,
    };
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

#[cfg(test)]
mod tests {
    use crate::changelog::ChangeId;
    use crate::commit_graph::CommitGraphChange;
    use crate::common::LixTimestamp;
    use crate::entity_pk::EntityPk;
    use crate::json_store::{
        JsonRef, JsonSlot, JsonStoreContext, JsonWritePlacementRef, NormalizedJsonRef,
    };
    use crate::storage::{
        InMemoryStorageBackend, StorageContext, StorageReadOptions, StorageWriteOptions,
    };

    use super::{ChangePayloadProjection, materialize_commit_graph_change};

    fn change(snapshot: JsonSlot, metadata: JsonSlot) -> CommitGraphChange {
        CommitGraphChange {
            id: ChangeId::for_test_label("change-projection"),
            entity_pk: EntityPk::single("entity-1"),
            schema_key: "example".to_string(),
            file_id: Some("file-1".to_string()),
            snapshot,
            metadata,
            created_at: LixTimestamp::expect_parse("created_at", "2026-01-01T00:00:00Z"),
            origin_key: Some("origin-1".to_string()),
        }
    }

    #[tokio::test]
    async fn unprojected_json_refs_are_not_loaded() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("begin read");
        let mut json_reader = JsonStoreContext::new().reader(read);
        let row = materialize_commit_graph_change(
            &mut json_reader,
            change(
                JsonSlot::Ref(JsonRef::for_content(b"missing snapshot")),
                JsonSlot::Ref(JsonRef::for_content(b"missing metadata")),
            ),
            ChangePayloadProjection {
                snapshot_content: false,
                metadata: false,
            },
        )
        .await
        .expect("unprojected missing refs should not be read");

        assert_eq!(
            row.id,
            ChangeId::for_test_label("change-projection").to_string()
        );
        assert_eq!(row.origin_key.as_deref(), Some("origin-1"));
        assert_eq!(row.snapshot_content, None);
        assert_eq!(row.metadata, None);
    }

    #[tokio::test]
    async fn projected_json_ref_still_reports_missing_payload() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("begin read");
        let mut json_reader = JsonStoreContext::new().reader(read);
        let missing_ref = JsonRef::for_content(b"missing snapshot");
        let error = materialize_commit_graph_change(
            &mut json_reader,
            change(JsonSlot::Ref(missing_ref), JsonSlot::None),
            ChangePayloadProjection {
                snapshot_content: true,
                metadata: false,
            },
        )
        .await
        .expect_err("projected missing ref should fail");

        assert!(error.message.contains(&missing_ref.to_hex()));
        assert!(error.message.contains("snapshot"));
    }

    #[tokio::test]
    async fn projected_json_refs_are_materialized() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let snapshot = "{\"value\":1}";
        let metadata = "{\"source\":\"test\"}";
        let mut writes = storage.new_write_set();
        let refs = JsonStoreContext::new()
            .writer()
            .stage_batch(
                &mut writes,
                JsonWritePlacementRef::OutOfBand,
                [
                    NormalizedJsonRef::new(snapshot),
                    NormalizedJsonRef::new(metadata),
                ],
            )
            .expect("stage json payloads");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("commit json payloads");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("begin read");
        let mut json_reader = JsonStoreContext::new().reader(read);
        let row = materialize_commit_graph_change(
            &mut json_reader,
            change(JsonSlot::Ref(refs[0]), JsonSlot::Ref(refs[1])),
            ChangePayloadProjection::ALL,
        )
        .await
        .expect("projected refs should materialize");

        assert_eq!(row.snapshot_content.as_deref(), Some(snapshot));
        assert_eq!(row.metadata.as_deref(), Some(metadata));
    }
}
