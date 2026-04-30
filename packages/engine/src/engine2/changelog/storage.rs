use crate::backend::{KvScanRange, KvStore, KvWriter};
use crate::engine2::changelog::{CanonicalChange, ChangelogScanRequest};
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

fn encode_change(change: &CanonicalChange) -> Result<Vec<u8>, LixError> {
    serde_json::to_vec(change).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to encode changelog change: {error}"),
        )
    })
}

fn decode_change(bytes: &[u8]) -> Result<CanonicalChange, LixError> {
    serde_json::from_slice(bytes).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to decode changelog change: {error}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::backend::{testing::UnitTestBackend, LixBackend, TransactionBeginMode};
    use crate::engine2::changelog::{ChangelogContext, ChangelogScanRequest};

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
        changelog
            .writer(tx.as_mut())
            .append_changes(std::slice::from_ref(&change))
            .await
            .expect("append should succeed");
        tx.commit().await.expect("commit should succeed");

        let loaded = {
            let reader = changelog.reader(backend);
            reader.load_change("change-1").await
        }
        .expect("load should succeed");
        assert_eq!(loaded, Some(change));
    }

    #[tokio::test]
    async fn scan_changes_respects_limit() {
        let backend = Arc::new(UnitTestBackend::new());
        let changelog = ChangelogContext::new();
        let mut tx = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        changelog
            .writer(tx.as_mut())
            .append_changes(&[test_change("change-1"), test_change("change-2")])
            .await
            .expect("append should succeed");
        tx.commit().await.expect("commit should succeed");

        let changes = {
            let reader = changelog.reader(backend);
            reader
                .scan_changes(&ChangelogScanRequest { limit: Some(1) })
                .await
        }
        .expect("scan should succeed");
        assert_eq!(changes, vec![test_change("change-1")]);
    }

    fn test_change(id: &str) -> CanonicalChange {
        CanonicalChange {
            id: id.to_string(),
            entity_id: crate::engine2::entity_identity::EntityIdentity::single("entity-1"),
            schema_key: "test_schema".to_string(),
            schema_version: "1".to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some("{\"value\":1}".to_string()),
            metadata: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
        }
    }
}
