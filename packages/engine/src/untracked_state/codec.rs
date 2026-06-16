use super::types::UntrackedPayloadRef;
use crate::LixError;
use crate::storage_codec;
use crate::untracked_state::{UntrackedStateIdentity, UntrackedStateRow, UntrackedStateRowRef};

#[cfg(feature = "storage-benches")]
pub(crate) fn encode_row_ref(row: UntrackedStateRowRef<'_>) -> Result<Vec<u8>, LixError> {
    storage_codec::encode("untracked-state row", &row)
}

pub(crate) fn encode_payload_ref(row: UntrackedStateRowRef<'_>) -> Result<Vec<u8>, LixError> {
    storage_codec::encode(
        "untracked-state payload",
        &UntrackedPayloadRef {
            snapshot_content: row.snapshot_content,
            metadata: row.metadata,
            created_at: row.created_at,
            updated_at: row.updated_at,
            global: row.global,
        },
    )
}

pub(crate) fn decode_payload_with_identity(
    identity: UntrackedStateIdentity,
    bytes: &[u8],
) -> Result<UntrackedStateRow, LixError> {
    let UntrackedPayloadRef {
        snapshot_content,
        metadata,
        created_at,
        updated_at,
        global,
    } = storage_codec::decode("untracked-state payload", bytes)?;

    Ok(UntrackedStateRow {
        entity_pk: identity.entity_pk,
        schema_key: identity.schema_key,
        file_id: identity.file_id,
        snapshot_content: snapshot_content.map(str::to_string),
        metadata: metadata.map(str::to_string),
        created_at,
        updated_at,
        global,
        branch_id: identity.branch_id,
    })
}

#[cfg(test)]
pub(crate) fn decode_row(bytes: &[u8]) -> Result<UntrackedStateRow, LixError> {
    storage_codec::decode("untracked-state row", bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity_pk::EntityPk;

    fn row_ref<'a>(
        entity_pk: &'a EntityPk,
        snapshot_content: Option<&'a str>,
        metadata: Option<&'a str>,
    ) -> UntrackedStateRowRef<'a> {
        UntrackedStateRowRef {
            entity_pk,
            schema_key: "schema.unicode",
            file_id: Some("file-1"),
            snapshot_content,
            metadata,
            created_at: crate::common::LixTimestamp::expect_parse(
                "created_at",
                "2026-05-19T00:00:00.000Z",
            ),
            updated_at: crate::common::LixTimestamp::expect_parse(
                "updated_at",
                "2026-05-19T00:00:01.000Z",
            ),
            global: false,
            branch_id: "branch-1",
        }
    }

    fn identity(entity_pk: EntityPk) -> UntrackedStateIdentity {
        UntrackedStateIdentity {
            branch_id: "branch-1".to_string(),
            schema_key: "schema.unicode".to_string(),
            entity_pk,
            file_id: Some("file-1".to_string()),
        }
    }

    #[test]
    fn payload_roundtrips_with_key_identity() {
        let entity_pk = EntityPk::tuple(vec!["id-1".to_string(), "東京".to_string()])
            .expect("entity primary key should build");
        let bytes = encode_payload_ref(row_ref(
            &entity_pk,
            Some("{\"hello\":\"world\"}"),
            Some("{\"meta\":true}"),
        ))
        .expect("payload should encode");

        let decoded = decode_payload_with_identity(identity(entity_pk.clone()), &bytes)
            .expect("payload should decode");
        assert_eq!(decoded.entity_pk, entity_pk);
        assert_eq!(decoded.schema_key, "schema.unicode");
        assert_eq!(decoded.file_id.as_deref(), Some("file-1"));
        assert_eq!(
            decoded.snapshot_content.as_deref(),
            Some("{\"hello\":\"world\"}")
        );
        assert_eq!(decoded.metadata.as_deref(), Some("{\"meta\":true}"));
        assert_eq!(decoded.created_at().to_string(), "2026-05-19T00:00:00.000Z");
        assert_eq!(decoded.updated_at().to_string(), "2026-05-19T00:00:01.000Z");
        assert!(!decoded.global);
        assert_eq!(decoded.branch_id, "branch-1");
    }

    #[test]
    fn payload_roundtrips_absent_optional_fields() {
        let entity_pk = EntityPk::single("id-1");
        let bytes =
            encode_payload_ref(row_ref(&entity_pk, None, None)).expect("payload should encode");
        let decoded = decode_payload_with_identity(identity(entity_pk), &bytes)
            .expect("payload should decode");
        assert_eq!(decoded.snapshot_content, None);
        assert_eq!(decoded.metadata, None);
    }

    #[test]
    fn payload_decode_rejects_malformed_storage_bytes() {
        let entity_pk = EntityPk::single("id-1");
        let error = decode_payload_with_identity(identity(entity_pk), b"not-musli-payload")
            .expect_err("malformed storage payload is rejected");
        assert!(
            error
                .to_string()
                .contains("failed to decode untracked-state payload")
        );
    }

    #[test]
    fn payload_decode_rejects_trailing_bytes() {
        let entity_pk = EntityPk::single("id-1");
        let mut bytes = encode_payload_ref(row_ref(&entity_pk, Some("{}"), None))
            .expect("payload should encode");
        bytes.push(0);
        let error = decode_payload_with_identity(identity(entity_pk), &bytes)
            .expect_err("trailing bytes should fail");
        assert!(
            error
                .to_string()
                .contains("failed to decode untracked-state payload")
        );
    }

    #[test]
    fn payload_decode_rejects_truncated_string() {
        let entity_pk = EntityPk::single("id-1");
        let mut bytes = encode_payload_ref(row_ref(&entity_pk, Some("{}"), None))
            .expect("payload should encode");
        bytes.truncate(bytes.len() - 2);
        let error = decode_payload_with_identity(identity(entity_pk), &bytes)
            .expect_err("truncated payload should fail");
        assert!(
            error
                .to_string()
                .contains("failed to decode untracked-state payload")
        );
    }

    #[test]
    fn row_decode_rejects_empty_entity_pk() {
        let entity_pk = EntityPk { parts: Vec::new() };
        let bytes = storage_codec::encode(
            "untracked-state row",
            &UntrackedStateRowRef {
                entity_pk: &entity_pk,
                schema_key: "schema",
                file_id: None,
                snapshot_content: None,
                metadata: None,
                created_at: crate::common::LixTimestamp::expect_parse(
                    "created_at",
                    "2026-05-19T00:00:00.000Z",
                ),
                updated_at: crate::common::LixTimestamp::expect_parse(
                    "updated_at",
                    "2026-05-19T00:00:00.000Z",
                ),
                global: false,
                branch_id: "branch-1",
            },
        )
        .expect("invalid row should encode");

        let error = decode_row(&bytes).expect_err("empty entity pk should reject");

        assert!(
            error
                .message
                .contains("entity primary key decoded from storage is invalid")
        );
    }
}
