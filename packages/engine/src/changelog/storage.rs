use crate::backend::{KvScanRange, KvStore, KvWriter};
use crate::changelog::codec::{decode_change, encode_change};
use crate::changelog::{CanonicalChange, ChangelogScanRequest};
use crate::LixError;

const CHANGELOG_CHANGE_NAMESPACE: &str = "changelog.change";

pub(crate) async fn load_change(
    store: &mut impl KvStore,
    change_id: &str,
) -> Result<Option<CanonicalChange>, LixError> {
    let Some(bytes) = store
        .kv_get(
            CHANGELOG_CHANGE_NAMESPACE,
            encode_change_key(change_id).as_slice(),
        )
        .await?
    else {
        return Ok(None);
    };
    decode_change(&bytes).map(Some)
}

pub(crate) async fn scan_changes(
    store: &mut impl KvStore,
    request: &ChangelogScanRequest,
) -> Result<Vec<CanonicalChange>, LixError> {
    // TODO(engine2): scan by a durable append sequence instead of change id.
    // This first index is enough for exact lookup and deterministic debug scans.
    store
        .kv_scan(
            CHANGELOG_CHANGE_NAMESPACE,
            KvScanRange::prefix(Vec::new()),
            request.limit,
        )
        .await?
        .into_iter()
        .map(|pair| decode_change(&pair.value))
        .collect()
}

pub(crate) async fn append_changes(
    writer: &mut impl KvWriter,
    changes: &[CanonicalChange],
) -> Result<(), LixError> {
    for change in changes {
        writer
            .kv_put(
                CHANGELOG_CHANGE_NAMESPACE,
                encode_change_key(&change.id).as_slice(),
                encode_change(change)?.as_slice(),
            )
            .await?;
    }
    Ok(())
}

fn encode_change_key(change_id: &str) -> Vec<u8> {
    change_id.as_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::backend::{testing::UnitTestBackend, Backend, TransactionBeginMode};
    use crate::changelog::{
        canonicalize_materialized_change, materialize_change, ChangelogContext,
        ChangelogScanRequest, MaterializedCanonicalChange,
    };
    use crate::json_store::JsonStoreContext;

    use super::*;

    #[tokio::test]
    async fn append_and_load_change_roundtrips() {
        let backend = Arc::new(UnitTestBackend::new());
        let changelog = ChangelogContext::new();
        let change = test_change("change-1");

        let mut tx = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        append_test_changes(&changelog, tx.as_mut(), std::slice::from_ref(&change)).await;
        tx.commit().await.expect("commit should succeed");

        let loaded = load_test_change(&changelog, Arc::clone(&backend), "change-1").await;
        assert_eq!(loaded, Some(change));
    }

    #[tokio::test]
    async fn append_and_load_composite_entity_identity_roundtrips() {
        let backend = Arc::new(UnitTestBackend::new());
        let changelog = ChangelogContext::new();
        let mut change = test_change("change-composite");
        change.entity_id = crate::entity_identity::EntityIdentity::tuple(vec![
            crate::entity_identity::EntityIdentityPart::String("entity".to_string()),
            crate::entity_identity::EntityIdentityPart::Number("7".to_string()),
            crate::entity_identity::EntityIdentityPart::Bool(true),
        ])
        .expect("composite identity should be valid");

        let mut tx = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        append_test_changes(&changelog, tx.as_mut(), std::slice::from_ref(&change)).await;
        tx.commit().await.expect("commit should succeed");

        let loaded = load_test_change(&changelog, Arc::clone(&backend), "change-composite").await;
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
        let backend = Arc::new(UnitTestBackend::new());
        let changelog = ChangelogContext::new();
        let mut tx = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        append_test_changes(
            &changelog,
            tx.as_mut(),
            &[test_change("change-1"), test_change("change-2")],
        )
        .await;
        tx.commit().await.expect("commit should succeed");

        let canonical_changes = {
            let reader = changelog.reader(Arc::clone(&backend));
            reader
                .scan_changes(&ChangelogScanRequest { limit: Some(1) })
                .await
        }
        .expect("scan should succeed");
        let materialize_store = Arc::clone(&backend);
        let mut json_reader = JsonStoreContext::new().reader(materialize_store);
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
        tx: &mut (dyn crate::BackendTransaction + Send + Sync),
        changes: &[MaterializedCanonicalChange],
    ) {
        let mut json_writer = JsonStoreContext::new().writer();
        let canonical_changes = changes
            .iter()
            .map(|change| canonicalize_materialized_change(&mut json_writer, change))
            .collect::<Result<Vec<_>, _>>()
            .expect("changes should canonicalize");
        {
            let mut writer_store = &mut *tx;
            json_writer
                .flush(&mut writer_store)
                .await
                .expect("json should flush");
        }
        changelog
            .writer(tx)
            .append_changes(&canonical_changes)
            .await
            .expect("append should succeed");
    }

    async fn load_test_change(
        changelog: &ChangelogContext,
        backend: Arc<UnitTestBackend>,
        change_id: &str,
    ) -> Option<MaterializedCanonicalChange> {
        let canonical = {
            let reader = changelog.reader(Arc::clone(&backend));
            reader
                .load_change(change_id)
                .await
                .expect("load should succeed")
        }?;
        let mut json_reader = JsonStoreContext::new().reader(backend);
        materialize_change(&mut json_reader, canonical)
            .await
            .map(Some)
            .expect("change should materialize")
    }
}
