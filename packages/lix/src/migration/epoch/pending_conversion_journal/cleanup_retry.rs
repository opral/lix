//! Child of pending_conversion_journal. Explicit closed-storage maintenance;
//! ordinary opening never scans journals or performs this cleanup network work.
use super::*;

pub(crate) async fn retry_published_conversion_cleanup<
    S: Storage + Clone + Send + Sync + 'static,
>(
    storage: &S,
    authenticated: &crate::sync::AuthenticatedPartialConversion,
) -> Result<usize, LixError> {
    let admitted = super::super::partial::admit_partial_epoch(storage).await?;
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
                    lower: Bound::Included(Key(Bytes::from_static(b"partial-conversion/"))),
                    upper: Bound::Excluded(Key(Bytes::from_static(b"partial-conversion0"))),
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
        if raw.len() > 4096 {
            return Err(epoch_error("cleanup journal exceeds fixed payload bound"));
        }
        let mut journal: PendingConversionJournal = serde_json::from_slice(&raw)
            .map_err(|_| epoch_error("cleanup journal is malformed"))?;
        journal.validate()?;
        // Do not submit another account/repository's private source coordinates.
        if journal_key(
            &journal.source_bank,
            expected.repository_id(),
            expected.active_account_id(),
            &journal.request.branch_id,
        )? != key
        {
            continue;
        }
        if journal.native_pin_cleaned
            || journal.native_source_pin.is_none()
            || journal.receipt.is_none()
        {
            continue;
        }
        // No read transaction survives this network await. The authority validates
        // the exact immutable M receipt and guards the surviving root atomically.
        crate::sync::cleanup_pending_conversion_authenticated(authenticated, &journal).await?;
        journal.native_pin_cleaned = true;
        persist_pending_conversion_journal(
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
    completed += super::super::native_global_journal_io::retry_published_global_conversion_cleanup(
        storage,
        authenticated,
    )
    .await?;
    Ok(completed)
}
