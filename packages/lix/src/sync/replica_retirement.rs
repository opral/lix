//! Authority-local replica retirement. These records are not replicated content.
//! A replacement permanently retires an identity; it never reactivates it.
use super::repository::SYNC_REPLICA_RETIREMENT_SPACE;
use crate::storage_adapter::{
    Storage, StorageGetManyRequest, StorageGetOptions, StorageKey, StoragePrecondition,
    StorageProjectedValue, StorageReadOptions, StorageWriteOptions, exact_get_many,
};
use crate::{Lix, LixError};
use bytes::Bytes;

fn key(account: &str, replica: &str) -> Result<StorageKey, LixError> {
    if replica.is_empty() || replica.len() > 512 {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "replica identity must contain 1 to 512 bytes",
        ));
    }
    Ok(StorageKey(Bytes::from(
        serde_json::to_vec(&("retired-replica-v1", account, replica)).expect("string tuple"),
    )))
}

impl<S: Storage + Clone + Send + Sync + 'static> Lix<S> {
    pub(crate) async fn retired_replica_replacement(
        &self,
        account: &str,
        replica: &str,
    ) -> Result<Option<String>, LixError> {
        let key = key(account, replica)?;
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let values = exact_get_many(
            &read,
            &[StorageGetManyRequest {
                space: SYNC_REPLICA_RETIREMENT_SPACE,
                keys: std::slice::from_ref(&key),
                opts: StorageGetOptions::default(),
            }],
        )
        .await?;
        match values.values.into_iter().next().flatten() {
            None => Ok(None),
            Some(StorageProjectedValue::FullValue(value)) => {
                String::from_utf8(value.to_vec()).map(Some).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "invalid replica retirement record",
                    )
                })
            }
            Some(StorageProjectedValue::KeyOnly) => Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "replica retirement read omitted value",
            )),
        }
    }

    /// Caller must serialize this with the complete lifetime of replica publication,
    /// including detached work after HTTP cancellation.
    pub(crate) async fn retire_replica(
        &self,
        account: &str,
        replica: &str,
        replacement: &str,
    ) -> Result<String, LixError> {
        let source_key = key(account, replica)?;
        key(account, replacement)?;
        if replica == replacement {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "replacement must have a new replica identity",
            ));
        }
        // This independent control write must not wait on a content transaction
        // spanning requests: its eventual commit must acquire the publication gate.
        if let Some(existing) = self.retired_replica_replacement(account, replica).await? {
            return Ok(existing);
        }
        if self
            .retired_replica_replacement(account, replacement)
            .await?
            .is_some()
        {
            return Err(LixError::new(
                "LIX_REPLICA_RETIRED",
                "replacement replica is already retired",
            ));
        }
        let adapter = self.storage_adapter();
        let mut writes = adapter.new_write_set();
        writes.put(
            SYNC_REPLICA_RETIREMENT_SPACE,
            source_key.clone(),
            replacement.as_bytes().to_vec(),
        );
        let committed = adapter
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions: vec![StoragePrecondition::KeyAbsent {
                        space: SYNC_REPLICA_RETIREMENT_SPACE,
                        key: source_key,
                    }],
                    await_durable: true,
                    ..StorageWriteOptions::default()
                },
            )
            .await;
        if let Err(error) = committed {
            // A competing retirement or lost commit acknowledgment may already
            // have established the durable winner. Never mint a second target.
            if let Some(existing) = self.retired_replica_replacement(account, replica).await? {
                return Ok(existing);
            }
            return Err(error.into());
        }
        Ok(replacement.to_owned())
    }
}
