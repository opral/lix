use super::native_global_conversion_journal::GlobalConversionJournal;
use super::*;
fn global_journal_key(bank: &str, repository: &str, account: &str) -> Result<Key, LixError> {
    // Hash structured coordinates, never interpolate an untrusted path segment.
    let bytes =
        serde_json::to_vec(&(bank, repository, account)).map_err(|e| epoch_error(e.to_string()))?;
    Ok(Key(Bytes::from(format!(
        "partial-global-conversion/{}",
        blake3::hash(&bytes).to_hex()
    ))))
}
pub(crate) async fn load_global_conversion_journal<S: Storage>(
    storage: &S,
    bank: &str,
    repository: &str,
    account: &str,
) -> Result<Option<(GlobalConversionJournal, Bytes)>, LixError> {
    let key = global_journal_key(bank, repository, account)?;
    let read = storage
        .begin_read(ReadOptions {
            durability: crate::storage_adapter::StorageReadDurability::Durable,
            ..Default::default()
        })
        .await
        .map_err(storage_error)?;
    let keys = [key];
    let values = read
        .get_many(&[GetManyRequest {
            space: REPOSITORY_EPOCH_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await
        .map_err(storage_error)?;
    let Some(value) = values.values.into_iter().next().flatten() else {
        return Ok(None);
    };
    let ProjectedValue::FullValue(raw) = value else {
        return Err(epoch_error("conversion journal payload missing"));
    };
    if raw.len() > 1024 * 1024 {
        return Err(epoch_error("conversion journal exceeds bound"));
    }
    let journal: GlobalConversionJournal =
        serde_json::from_slice(&raw).map_err(|e| epoch_error(e.to_string()))?;
    journal.validate()?;
    if journal.source_bank != bank {
        return Err(epoch_error("conversion journal source changed"));
    }
    Ok(Some((journal, raw)))
}
/// Every network transition requires this durable write first. The source
/// pointer claim fences concurrent conversion; key CAS fences delayed replies.
/// Reacquiring a claim after rollback does NOT change the journal key/attempt.
pub(crate) async fn persist_global_conversion_journal<S: Storage>(
    storage: &S,
    claim: &Bytes,
    repository: &str,
    account: &str,
    journal: &GlobalConversionJournal,
    previous: Option<Bytes>,
) -> Result<Bytes, LixError> {
    journal.validate()?;
    let key = global_journal_key(&journal.source_bank, repository, account)?;
    let bytes = Bytes::from(serde_json::to_vec(journal).map_err(|e| epoch_error(e.to_string()))?);
    if bytes.len() > 1024 * 1024 {
        return Err(epoch_error("conversion journal exceeds bound"));
    }
    let mut preconditions = vec![Precondition::KeyValueEquals {
        space: REPOSITORY_EPOCH_SPACE,
        key: Key(Bytes::from_static(REPOSITORY_EPOCH_KEY)),
        expected: claim.clone(),
    }];
    preconditions.push(match previous {
        Some(expected) => Precondition::KeyValueEquals {
            space: REPOSITORY_EPOCH_SPACE,
            key: key.clone(),
            expected,
        },
        None => Precondition::KeyAbsent {
            space: REPOSITORY_EPOCH_SPACE,
            key: key.clone(),
        },
    });
    let mut write = storage
        .begin_write(WriteOptions {
            preconditions,
            await_durable: true,
            ..Default::default()
        })
        .await
        .map_err(storage_error)?;
    write
        .put_many(
            REPOSITORY_EPOCH_SPACE,
            PutBatch {
                entries: vec![PutEntry {
                    key,
                    value: StoredValue {
                        bytes: bytes.clone(),
                    },
                }],
            },
        )
        .await
        .map_err(storage_error)?;
    write.commit().await.map_err(storage_error)?;
    Ok(bytes)
}

mod cleanup_retry;
pub(super) use cleanup_retry::retry_published_global_conversion_cleanup;
