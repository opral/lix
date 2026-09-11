//! Child of native_global_journal_io. Explicit closed-storage maintenance;
//! ordinary opening never scans journals or performs this cleanup network work.
use super::*;

pub(crate) async fn retry_published_global_conversion_cleanup<
    S: Storage + Clone + Send + Sync + 'static,
>(
    storage: &S,
    authenticated: &crate::sync::AuthenticatedPartialConversion,
) -> Result<usize, LixError> {
    let admitted = admit_partial_epoch(storage).await?;
    let expected = &admitted.state;
    if authenticated.state().repository_id() != expected.repository_id()
        || authenticated.state().active_account_id() != expected.active_account_id()
        || authenticated.state().remote_id() != expected.remote_id()
    {
        return Err(epoch_error(
            "cleanup retry authority/account differs from published partial replica",
        ));
    }
    let (pointer, active) = load_pointer(storage)
        .await?
        .ok_or_else(|| epoch_error("cleanup retry lacks active epoch"))?;
    if !matches!(pointer, PointerState::Active { .. }) {
        return Err(epoch_error("cleanup retry cannot run during migration"));
    }
    // Inventory keys only. One bounded native journal payload is loaded at a
    // time below; the storage owner prevents concurrent migration/key insertion.
    let keys = {
        let read = storage
            .begin_read(ReadOptions {
                durability: crate::storage_adapter::StorageReadDurability::Durable,
                ..Default::default()
            })
            .await
            .map_err(storage_error)?;
        let mut cursor = read
            .begin_scan(
                REPOSITORY_EPOCH_SPACE,
                KeyRange {
                    lower: Bound::Included(Key(Bytes::from_static(b"partial-global-conversion/"))),
                    upper: Bound::Excluded(Key(Bytes::from_static(b"partial-global-conversion0"))),
                },
                BeginScanOptions {
                    projection: CoreProjection::KeyOnly,
                    ..Default::default()
                },
            )
            .await
            .map_err(storage_error)?;
        let mut keys = Vec::new();
        while let Some(chunk) = cursor.next_chunk().await.map_err(storage_error)? {
            if keys.len() + chunk.len() > 65536 {
                return Err(epoch_error(
                    "explicit conversion cleanup inventory exceeds its operation budget",
                ));
            }
            keys.extend(chunk.into_iter().map(|entry| entry.key));
        }
        keys
    };
    let mut completed = 0;
    for key in keys {
        let raw = {
            let read = storage
                .begin_read(ReadOptions {
                    durability: crate::storage_adapter::StorageReadDurability::Durable,
                    ..Default::default()
                })
                .await
                .map_err(storage_error)?;
            let keys = [key.clone()];
            let values = read
                .get_many(&[GetManyRequest {
                    space: REPOSITORY_EPOCH_SPACE,
                    keys: &keys,
                    opts: GetOptions::default(),
                }])
                .await
                .map_err(storage_error)?
                .values;
            if values.len() != 1 {
                return Err(epoch_error("cleanup journal read cardinality changed"));
            }
            match values.into_iter().next().flatten() {
                Some(ProjectedValue::FullValue(raw)) => raw,
                _ => return Err(epoch_error("cleanup journal payload disappeared")),
            }
        };
        if raw.len() > 1024 * 1024 {
            return Err(epoch_error("cleanup journal exceeds fixed payload bound"));
        }
        let mut journal: GlobalConversionJournal = serde_json::from_slice(&raw)
            .map_err(|_| epoch_error("cleanup journal is malformed"))?;
        journal.validate()?;
        // Do not submit another account/repository's private source coordinates.
        if global_journal_key(
            &journal.source_bank,
            expected.repository_id(),
            expected.active_account_id(),
        )? != key
        {
            continue;
        }
        if journal.cleanup_complete || journal.receipt.is_none() {
            continue;
        }
        // No read transaction survives this network await. The authority validates
        // the exact immutable M receipt and guards the surviving root atomically.
        crate::sync::cleanup_global_conversion_authenticated(authenticated, &journal.request)
            .await?;
        journal.cleanup_complete = true;
        persist_global_conversion_journal(
            storage,
            &active,
            expected.repository_id(),
            expected.active_account_id(),
            &journal,
            Some(raw),
        )
        .await?;
        completed += 1;
    }
    Ok(completed)
}
