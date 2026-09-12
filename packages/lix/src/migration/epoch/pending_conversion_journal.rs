//! Child of migration::epoch. Deliberately outside disposable epoch banks.
//! 0x0007_001f is NOT used for durability: normal sync spaces are banked.
use super::*;
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PendingConversionJournal {
    pub version: u32,
    /// Immutable lane: Some pins native file/custom-schema waves; None retains KV-only waves.
    pub native_source_pin: Option<String>,
    pub native_pin_cleaned: bool,
    pub source_bank: String,
    pub manifest_digest: [u8; 32],
    pub request: crate::sync::PartialMergeRequest,
    pub accepted_tip: String,
    pub prepared_tip: Option<String>,
    pub receipt: Option<crate::sync::PartialMergeReceipt>,
    pub restart: Option<crate::sync::PartialAttemptRestartRequest>,
    pub restart_receipt: Option<crate::sync::PartialAttemptRestartReceipt>,
}
impl PendingConversionJournal {
    fn validate(&self) -> Result<(), LixError> {
        if self.version != 2 || self.source_bank.is_empty() {
            return Err(epoch_error("invalid pending conversion journal"));
        }
        if let Some(pin) = &self.native_source_pin {
            crate::storage_codec::id_string::uuid_bytes_from_canonical(pin)
                .ok_or_else(|| epoch_error("migration native pin is not canonical"))?;
            if pin == &self.request.branch_id || pin == crate::GLOBAL_BRANCH_ID {
                return Err(epoch_error("migration native pin is not isolated"));
            }
        }
        if self.native_pin_cleaned && (self.native_source_pin.is_none() || self.receipt.is_none()) {
            return Err(epoch_error("completed cleanup lacks native pin/outcome"));
        }
        self.request.validate()?;
        for id in std::iter::once(&self.accepted_tip).chain(self.prepared_tip.iter()) {
            crate::storage_codec::id_string::uuid_bytes_from_canonical(id)
                .ok_or_else(|| epoch_error("conversion frontier is not canonical"))?;
        }
        if let Some(intent) = &self.restart {
            intent.validate()?;
            if intent.old != self.request || self.receipt.is_some() {
                return Err(epoch_error(
                    "conversion restart disagrees with original attempt",
                ));
            }
            if let Some(receipt) = &self.restart_receipt {
                receipt.validate_for(&receipt.repository_id, &receipt.account_id, intent)?;
            }
        } else if self.restart_receipt.is_some() {
            return Err(epoch_error("restart receipt lacks intent"));
        }
        if let Some(receipt) = &self.receipt {
            receipt.validate_for(&self.request)?;
            if self.accepted_tip != self.request.captured_local_head_commit_id
                || self.prepared_tip.is_some()
            {
                return Err(epoch_error(
                    "terminal conversion receipt preceded complete body acknowledgment",
                ));
            }
        }
        Ok(())
    }
}
fn journal_key(bank: &str, repository: &str, account: &str, branch: &str) -> Result<Key, LixError> {
    // Hash structured coordinates, never interpolate an untrusted path segment.
    let bytes = serde_json::to_vec(&(bank, repository, account, branch))
        .map_err(|e| epoch_error(e.to_string()))?;
    Ok(Key(Bytes::from(format!(
        "partial-conversion/{}",
        blake3::hash(&bytes).to_hex()
    ))))
}
pub(crate) async fn load_pending_conversion_journal<S: Storage>(
    storage: &S,
    bank: &str,
    repository: &str,
    account: &str,
    branch: &str,
) -> Result<Option<(PendingConversionJournal, Bytes)>, LixError> {
    let key = journal_key(bank, repository, account, branch)?;
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
    if raw.len() > 4096 {
        return Err(epoch_error("conversion journal exceeds bound"));
    }
    let journal: PendingConversionJournal =
        serde_json::from_slice(&raw).map_err(|e| epoch_error(e.to_string()))?;
    journal.validate()?;
    if let (Some(intent), Some(receipt)) = (&journal.restart, &journal.restart_receipt) {
        receipt.validate_for(repository, account, intent)?;
    }
    if journal.source_bank != bank || journal.request.branch_id != branch {
        return Err(epoch_error("conversion journal source changed"));
    }
    Ok(Some((journal, raw)))
}
/// Every network transition requires this durable write first. The source
/// pointer claim fences concurrent conversion; key CAS fences delayed replies.
/// Reacquiring a claim after rollback does NOT change the journal key/attempt.
pub(crate) async fn persist_pending_conversion_journal<S: Storage>(
    storage: &S,
    claim: &Bytes,
    repository: &str,
    account: &str,
    journal: &PendingConversionJournal,
    previous: Option<Bytes>,
) -> Result<Bytes, LixError> {
    journal.validate()?;
    if let (Some(intent), Some(receipt)) = (&journal.restart, &journal.restart_receipt) {
        receipt.validate_for(repository, account, intent)?;
    }
    let key = journal_key(
        &journal.source_bank,
        repository,
        account,
        &journal.request.branch_id,
    )?;
    let bytes = Bytes::from(serde_json::to_vec(journal).map_err(|e| epoch_error(e.to_string()))?);
    if bytes.len() > 4096 {
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

#[cfg(test)]
mod tests {
    use super::*;
    // Child of migration::epoch::pending_conversion_journal tests.
    // Meaningful durability test: prepared exact target survives rollback; source
    // user rows and native commit IDs remain unchanged. No network is mocked here.
    #[tokio::test]
    async fn prepared_conversion_wave_survives_pointer_rollback_and_source_is_readable() {
        let storage = crate::sync::durable_memory_for_test(crate::Memory::new());
        let source = crate::open_lix()
            .with_storage(storage.clone())
            .await
            .unwrap();
        source
            .execute(
                "INSERT INTO lix_key_value(key,value) VALUES('migration-preserved','original')",
                &[],
            )
            .await
            .unwrap();
        let descriptor = source.partial_replica_descriptor(None).await.unwrap();
        let account = source.active_account_id().to_owned();
        let repository = source.lix_id().to_owned();
        source.close().await.unwrap();
        drop(source);
        let backing = storage;
        let storage = crate::storage_adapter::StorageSession::acquire(backing.clone())
            .await
            .unwrap();
        let (pointer, original) = load_pointer(&storage).await.unwrap().unwrap();
        let PointerState::Active {
            bank,
            generation,
            format,
            ..
        } = pointer
        else {
            panic!("active source expected")
        };
        let claim = encode_pointer(PointerState::Migrating {
            source: bank,
            source_format: format,
            target: replica_generation_bank(generation + 1).unwrap(),
            generation: generation + 1,
            attempt: uuid::Uuid::now_v7(),
        });
        replace_pointer(&storage, &original, &claim).await.unwrap();
        let selected = &descriptor.selected_branch;
        let target = uuid::Uuid::now_v7().to_string();
        let journal = PendingConversionJournal {
            version: 2,
            native_source_pin: None,
            native_pin_cleaned: false,
            source_bank: bank_code(bank),
            manifest_digest: [7; 32],
            request: crate::sync::PartialMergeRequest {
                attempt_id: uuid::Uuid::now_v7().to_string(),
                branch_id: selected.branch_id.clone(),
                base_commit_id: selected.head.commit_id.clone(),
                expected_authority_head_commit_id: selected.head.commit_id.clone(),
                captured_local_head_commit_id: target.clone(),
                expected_authority_checkpoint_commit_id: selected.checkpoint.commit_id.clone(),
                captured_local_checkpoint_commit_id: selected.checkpoint.commit_id.clone(),
                checkpoint_commit_id: selected.checkpoint.commit_id.clone(),
                global_head_commit_id: descriptor.global_branch.head.commit_id.clone(),
                global_checkpoint_commit_id: descriptor.global_branch.checkpoint.commit_id.clone(),
            },
            accepted_tip: selected.head.commit_id.clone(),
            prepared_tip: Some(target.clone()),
            receipt: None,
            restart: None,
            restart_receipt: None,
        };
        persist_pending_conversion_journal(&storage, &claim, &repository, &account, &journal, None)
            .await
            .unwrap();
        // Simulate response loss followed by rollback. No new UUID may be invented.
        replace_pointer(&storage, &claim, &original).await.unwrap();
        let (recovered, _) = load_pending_conversion_journal(
            &storage,
            &bank_code(bank),
            &repository,
            &account,
            &journal.request.branch_id,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(recovered.request, journal.request);
        assert_eq!(recovered.prepared_tip, Some(target));
        assert_eq!(load_pointer(&storage).await.unwrap().unwrap().1, original);
        drop(storage);
        let reopened = crate::open_lix().with_storage(backing).await.unwrap();
        assert_eq!(
            reopened
                .partial_replica_descriptor(None)
                .await
                .unwrap()
                .selected_branch
                .head
                .commit_id,
            selected.head.commit_id
        );
        let rows = reopened
            .execute(
                "SELECT value FROM lix_key_value WHERE key='migration-preserved'",
                &[],
            )
            .await
            .unwrap();
        assert!(format!("{rows:?}").contains("original"));
    }
}

mod cleanup_retry;
pub(crate) use cleanup_retry::retry_published_conversion_cleanup;
