use std::sync::Arc;

use crate::LixError;
use crate::changelog::ChangeRecord;
use crate::common::SharedStr;
use crate::plugin::runtime::WasmTypedRow;
use crate::row_pk::RowPk;

/// Read-boundary view of a changelog change with its native payload projected.
///
/// `lix_change` materializes direct durable `changelog.change` facts and
/// derived `lix_commit` changes from `changelog.commit`. History surfaces
/// materialize reachability-aware commit-graph changes, while traversal context
/// stays outside this row shape.
#[derive(Debug, Clone)]
pub(crate) struct MaterializedChange {
    pub(crate) id: String,
    pub(crate) account_id: String,
    pub(crate) row_pk: RowPk,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_content: Option<SharedStr>,
    pub(crate) metadata: Option<SharedStr>,
    pub(crate) decoded_snapshot: Option<Arc<WasmTypedRow>>,
    pub(crate) created_at: String,
    pub(crate) origin_key: Option<String>,
}

/// Returns the stable public identity for a materialized change.
///
/// A change's physical schema and ownership scope are storage details. Public
/// semantic relations retain their own primary key; filesystem implementation
/// rows collapse to the logical file or directory identity.
pub(crate) fn public_change_row_ref(
    change: &MaterializedChange,
) -> Result<Option<crate::RowRef>, LixError> {
    let (relation, row_pk) =
        if crate::sql2::catalog::schema_exposed_as_schema_surface(&change.schema_key) {
            (change.schema_key.as_str(), change.row_pk.clone())
        } else if change.schema_key == "lix_directory_descriptor" {
            ("lix_directory", change.row_pk.clone())
        } else if let Some(file_id) = change.file_id.as_deref() {
            let row_pk = RowPk::uuid_from_canonical(file_id).map_err(|error| {
                LixError::new(
                    LixError::CODE_TYPE_MISMATCH,
                    format!("lix_change file identity is invalid: {error}"),
                )
            })?;
            ("lix_file", row_pk)
        } else {
            return Ok(None);
        };
    crate::row_ref::encode(relation, &row_pk).map(Some)
}

/// Payloads that a change scan must project for its output or filters.
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

pub(crate) fn materialize_located_history_change(
    change: crate::commit_graph::CommitGraphChange,
) -> Result<MaterializedChange, LixError> {
    materialize_commit_graph_change(change, ChangePayloadProjection::ALL)
}

pub(crate) fn materialize_changelog_change_record(
    change: ChangeRecord,
    payload_projection: ChangePayloadProjection,
) -> Result<MaterializedChange, LixError> {
    materialize_commit_graph_change(
        crate::commit_graph::CommitGraphChange {
            id: change.change_id,
            account_id: change.account_id,
            row_pk: change.row_pk,
            schema_key: change.schema_key,
            file_id: change.file_id,
            metadata: change.metadata,
            snapshot: change.snapshot,
            created_at: change.created_at,
            origin_key: change.origin_key,
        },
        payload_projection,
    )
}

pub(crate) fn materialize_commit_graph_change(
    mut change: crate::commit_graph::CommitGraphChange,
    payload_projection: ChangePayloadProjection,
) -> Result<MaterializedChange, LixError> {
    let decoded_snapshot = change
        .snapshot
        .take()
        .map(|payload| {
            WasmTypedRow::decode_durable_payload(payload.into(), &change.schema_key, &change.row_pk)
                .map(Arc::new)
        })
        .transpose()?;
    let snapshot_content = if payload_projection.snapshot_content {
        decoded_snapshot
            .as_deref()
            .map(WasmTypedRow::to_json_shared)
            .transpose()?
    } else {
        None
    };
    let metadata = if payload_projection.metadata {
        change
            .metadata
            .map(|metadata| {
                metadata
                    .to_json_string()
                    .map(SharedStr::from)
                    .map_err(|error| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!("cannot materialize changelog JSONB metadata: {error}"),
                        )
                    })
            })
            .transpose()?
    } else {
        None
    };
    Ok(MaterializedChange {
        id: change.id.to_string(),
        account_id: change.account_id,
        row_pk: change.row_pk,
        schema_key: change.schema_key,
        file_id: change.file_id,
        snapshot_content,
        metadata,
        decoded_snapshot,
        created_at: change.created_at.to_string(),
        origin_key: change.origin_key,
    })
}

#[cfg(test)]
mod tests {
    use crate::changelog::ChangeId;
    use crate::commit_graph::CommitGraphChange;
    use crate::common::LixTimestamp;
    use crate::row_pk::RowPk;

    use super::{ChangePayloadProjection, materialize_commit_graph_change};

    fn change(snapshot: &str, metadata: Option<lix_schema::Jsonb>) -> CommitGraphChange {
        let row_pk = RowPk::single("row-1");
        let value = serde_json::from_str(snapshot).expect("test snapshot should parse");
        let typed = crate::plugin::runtime::WasmTypedRow::from_test_json_unchecked(&row_pk, &value)
            .expect("test snapshot should type");
        CommitGraphChange {
            id: ChangeId::for_test_label("change-projection"),
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            row_pk,
            schema_key: "example".to_string(),
            file_id: Some("file-1".to_string()),
            metadata,
            snapshot: Some(
                typed
                    .durable_payload()
                    .expect("test typed payload should encode")
                    .to_vec(),
            ),
            created_at: LixTimestamp::expect_parse("created_at", "2026-01-01T00:00:00Z"),
            origin_key: Some("origin-1".to_string()),
        }
    }

    #[test]
    fn unprojected_metadata_is_not_materialized() {
        let row = materialize_commit_graph_change(
            change(
                "{\"value\":1}",
                Some(lix_schema::Jsonb::from_value(serde_json::json!({
                    "source": "ignored"
                }))),
            ),
            ChangePayloadProjection {
                snapshot_content: false,
                metadata: false,
            },
        )
        .expect("unprojected missing refs should not be read");

        assert_eq!(
            row.id,
            ChangeId::for_test_label("change-projection").to_string()
        );
        assert_eq!(row.origin_key.as_deref(), Some("origin-1"));
        assert_eq!(row.snapshot_content, None);
        assert_eq!(row.metadata, None);
    }

    #[test]
    fn projected_invalid_snapshot_is_rejected() {
        let mut change = change("{\"value\":1}", None);
        change.snapshot = Some(vec![1, 2, 3]);
        let error = materialize_commit_graph_change(
            change,
            ChangePayloadProjection {
                snapshot_content: true,
                metadata: false,
            },
        )
        .expect_err("invalid native payload should fail");

        assert!(error.message.contains("typed payload"));
    }

    #[test]
    fn projected_native_jsonb_is_materialized() {
        let snapshot = "{\"value\":1}";
        let metadata = "{\"source\":\"test\"}";
        let row = materialize_commit_graph_change(
            change(
                snapshot,
                Some(lix_schema::Jsonb::from_value(
                    serde_json::from_str(metadata).expect("metadata JSON"),
                )),
            ),
            ChangePayloadProjection::ALL,
        )
        .expect("projected refs should materialize");

        assert_eq!(row.snapshot_content.as_deref(), Some(snapshot));
        assert_eq!(row.metadata.as_deref(), Some(metadata));
    }
}
