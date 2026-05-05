use crate::changelog::codec::{decode_change, encode_change};
use crate::changelog::{CanonicalChange, ChangelogScanRequest};
use crate::storage::KvScanRange;
use crate::storage::{KvGetGroup, KvGetRequest, KvScanRequest, StorageReader, StorageWriteSet};
use crate::LixError;

const CHANGELOG_CHANGE_NAMESPACE: &str = "changelog.change";

pub(crate) async fn load_change(
    store: &mut impl StorageReader,
    change_id: &str,
) -> Result<Option<CanonicalChange>, LixError> {
    let bytes = store
        .get_values(KvGetRequest {
            groups: vec![KvGetGroup {
                namespace: CHANGELOG_CHANGE_NAMESPACE.to_string(),
                keys: vec![encode_change_key(change_id)],
            }],
        })
        .await?
        .groups
        .into_iter()
        .next()
        .and_then(|group| group.single_value_owned());
    let Some(bytes) = bytes else {
        return Ok(None);
    };
    decode_change(&bytes).map(Some)
}

pub(crate) async fn scan_changes(
    store: &mut impl StorageReader,
    request: &ChangelogScanRequest,
) -> Result<Vec<CanonicalChange>, LixError> {
    // TODO(engine2): scan by a durable append sequence instead of change id.
    // This first index is enough for exact lookup and deterministic debug scans.
    let page = store
        .scan_values(KvScanRequest {
            namespace: CHANGELOG_CHANGE_NAMESPACE.to_string(),
            range: KvScanRange::prefix(Vec::new()),
            after: None,
            limit: request.limit.unwrap_or(usize::MAX),
        })
        .await?;
    page.values.iter().map(decode_change).collect()
}

pub(crate) fn append_changes(
    writes: &mut StorageWriteSet,
    changes: &[CanonicalChange],
) -> Result<(), LixError> {
    for change in changes {
        writes.put(
            CHANGELOG_CHANGE_NAMESPACE,
            encode_change_key(&change.id),
            encode_change(change)?,
        );
    }
    Ok(())
}

fn encode_change_key(change_id: &str) -> Vec<u8> {
    change_id.as_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::backend::testing::UnitTestBackend;
    use crate::changelog::{
        canonicalize_materialized_change, materialize_change, ChangelogContext,
        ChangelogScanRequest, MaterializedCanonicalChange,
    };
    use crate::json_store::JsonStoreContext;
    use crate::storage::{StorageContext, StorageWriteSet, StorageWriteTransaction};

    use super::*;

    #[tokio::test]
    async fn append_and_load_change_roundtrips() {
        let storage = StorageContext::new(Arc::new(UnitTestBackend::new()));
        let changelog = ChangelogContext::new();
        let change = test_change("change-1");

        let mut tx = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        append_test_changes(&changelog, &mut tx, std::slice::from_ref(&change)).await;
        tx.commit().await.expect("commit should succeed");

        let loaded = load_test_change(&changelog, storage, "change-1").await;
        assert_eq!(loaded, Some(change));
    }

    #[tokio::test]
    async fn append_and_load_composite_entity_identity_roundtrips() {
        let storage = StorageContext::new(Arc::new(UnitTestBackend::new()));
        let changelog = ChangelogContext::new();
        let mut change = test_change("change-composite");
        change.entity_id = crate::entity_identity::EntityIdentity::tuple(vec![
            crate::entity_identity::EntityIdentityPart::String("entity".to_string()),
            crate::entity_identity::EntityIdentityPart::Number("7".to_string()),
            crate::entity_identity::EntityIdentityPart::Bool(true),
        ])
        .expect("composite identity should be valid");

        let mut tx = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        append_test_changes(&changelog, &mut tx, std::slice::from_ref(&change)).await;
        tx.commit().await.expect("commit should succeed");

        let loaded = load_test_change(&changelog, storage, "change-composite").await;
        assert_eq!(loaded, Some(change));
    }

    #[test]
    fn decode_rejects_non_flatbuffer_bytes() {
        let error = decode_change(br#"{"id":"change-json"}"#)
            .expect_err("json changelog payloads are not accepted after the hard cut");
        assert!(
            error
                .message
                .contains("invalid FlatBuffers file identifier"),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn scan_changes_respects_limit() {
        let storage = StorageContext::new(Arc::new(UnitTestBackend::new()));
        let changelog = ChangelogContext::new();
        let mut tx = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        append_test_changes(
            &changelog,
            &mut tx,
            &[test_change("change-1"), test_change("change-2")],
        )
        .await;
        tx.commit().await.expect("commit should succeed");

        let canonical_changes = {
            let reader = changelog.reader(storage.clone());
            reader
                .scan_changes(&ChangelogScanRequest { limit: Some(1) })
                .await
        }
        .expect("scan should succeed");
        let mut json_reader = JsonStoreContext::new().reader(storage);
        let mut changes = Vec::new();
        for change in canonical_changes {
            changes.push(
                materialize_change(&mut json_reader, change)
                    .await
                    .expect("change should materialize"),
            );
        }
        assert_eq!(changes, vec![test_change("change-1")]);
    }

    fn test_change(id: &str) -> MaterializedCanonicalChange {
        MaterializedCanonicalChange {
            id: id.to_string(),
            entity_id: crate::entity_identity::EntityIdentity::single("entity-1"),
            schema_key: "test_schema".to_string(),
            schema_version: "1".to_string(),
            file_id: None,
            snapshot_content: Some("{\"value\":1}".to_string()),
            metadata: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
        }
    }

    async fn append_test_changes(
        changelog: &ChangelogContext,
        tx: &mut Box<dyn StorageWriteTransaction + Send + Sync + 'static>,
        changes: &[MaterializedCanonicalChange],
    ) {
        let mut writes = StorageWriteSet::new();
        let mut json_writer = JsonStoreContext::new().writer(&mut writes);
        let canonical_changes = changes
            .iter()
            .map(|change| canonicalize_materialized_change(&mut json_writer, change))
            .collect::<Result<Vec<_>, _>>()
            .expect("changes should canonicalize");
        let mut writer = changelog.writer(&mut writes);
        writer
            .append_changes(&canonical_changes)
            .expect("append should succeed");
        writes
            .apply(&mut tx.as_mut())
            .await
            .expect("writes should apply");
    }

    async fn load_test_change(
        changelog: &ChangelogContext,
        storage: StorageContext,
        change_id: &str,
    ) -> Option<MaterializedCanonicalChange> {
        let canonical = {
            let reader = changelog.reader(storage.clone());
            reader
                .load_change(change_id)
                .await
                .expect("load should succeed")
        }?;
        let mut json_reader = JsonStoreContext::new().reader(storage);
        materialize_change(&mut json_reader, canonical)
            .await
            .map(Some)
            .expect("change should materialize")
    }
}
