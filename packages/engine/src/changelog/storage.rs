use crate::changelog::codec::decode_change;
use crate::changelog::{CanonicalChange, CanonicalChangeRef, ChangelogScanRequest};
use crate::storage::KvScanRange;
use crate::storage::{KvGetGroup, KvGetRequest, KvScanRequest, StorageReader, StorageWriteSet};
use crate::LixError;

const CHANGELOG_CHANGE_NAMESPACE: &str = "changelog.change";

pub(crate) async fn load_changes(
    store: &mut impl StorageReader,
    change_ids: &[String],
) -> Result<Vec<Option<CanonicalChange>>, LixError> {
    if change_ids.is_empty() {
        return Ok(Vec::new());
    }
    let result = store
        .get_values(KvGetRequest {
            groups: vec![KvGetGroup {
                namespace: CHANGELOG_CHANGE_NAMESPACE.to_string(),
                keys: change_ids
                    .iter()
                    .map(|change_id| encode_change_key(change_id))
                    .collect(),
            }],
        })
        .await?;
    let group = result.groups.into_iter().next().ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "changelog batch load returned no result group",
        )
    })?;
    if group.len() != change_ids.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "changelog batch load returned {} values for {} requested change ids",
                group.len(),
                change_ids.len()
            ),
        ));
    }

    let mut changes = Vec::with_capacity(group.len());
    for index in 0..group.len() {
        let change = match group.value(index).flatten() {
            Some(bytes) => Some(decode_change(bytes)?),
            None => None,
        };
        changes.push(change);
    }
    Ok(changes)
}

pub(crate) async fn scan_changes(
    store: &mut impl StorageReader,
    request: &ChangelogScanRequest,
) -> Result<Vec<CanonicalChange>, LixError> {
    // TODO(engine): scan by a durable append sequence instead of change id.
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

pub(crate) fn stage_changes<'a, I>(writes: &mut StorageWriteSet, changes: I) -> Result<(), LixError>
where
    I: IntoIterator<Item = CanonicalChangeRef<'a>>,
{
    for change in changes {
        writes.put(
            CHANGELOG_CHANGE_NAMESPACE,
            encode_change_key(change.id),
            crate::changelog::codec::encode_change_ref(change)?,
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
        materialize_change, ChangelogContext, ChangelogScanRequest, MaterializedCanonicalChange,
    };
    use crate::json_store::JsonStoreContext;
    use crate::storage::{StorageContext, StorageWriteSet, StorageWriteTransaction};

    use super::*;

    #[tokio::test]
    async fn append_and_load_changes_roundtrips() {
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
            "entity".to_string(),
            "7".to_string(),
            "true".to_string(),
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
        let mut json_writer = JsonStoreContext::new().writer();
        let canonical_changes = changes
            .iter()
            .map(|change| {
                crate::test_support::canonical_change_from_materialized(
                    &mut writes,
                    &mut json_writer,
                    change,
                )
            })
            .collect::<Result<Vec<_>, _>>()
            .expect("changes should canonicalize");
        let mut writer = changelog.writer(&mut writes);
        writer
            .stage_changes(canonical_changes.iter().map(|change| change.as_ref()))
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
                .load_changes(&[change_id.to_string()])
                .await
                .expect("load should succeed")
                .into_iter()
                .next()
                .flatten()
        }?;
        let mut json_reader = JsonStoreContext::new().reader(storage);
        materialize_change(&mut json_reader, canonical)
            .await
            .map(Some)
            .expect("change should materialize")
    }
}
