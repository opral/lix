//! Private account-bound authority merge entrypoint. HTTP wiring is separate.
use super::*;
impl<S> Lix<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    pub(crate) async fn restart_native_global_migration_for_account(
        &self,
        intent: &crate::sync::NativeGlobalRestartRequest,
        account: &str,
    ) -> Result<crate::sync::NativeGlobalRestartReceipt, LixError> {
        intent.validate()?;
        if self.sync_mode_state().role() != crate::sync::SyncRole::Authority {
            return Err(LixError::new(
                "LIX_MIGRATION_GLOBAL_RESTART_INVALID",
                "global migration restart requires authority ownership",
            ));
        }
        let adapter = self.storage_adapter();
        for _ in 0..8 {
            let read = adapter.begin_read(Default::default()).await?;
            let mut writes = adapter.new_write_set();
            let (receipt, preconditions) = crate::sync::stage_restart_native_global_migration(
                &read,
                &mut writes,
                self.lix_id(),
                account,
                intent,
            )
            .await?;
            drop(read);
            if writes.is_empty() {
                return Ok(receipt);
            }
            match adapter
                .commit_write_set(
                    writes,
                    crate::storage_adapter::StorageWriteOptions {
                        preconditions,
                        await_durable: true,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(_) => return Ok(receipt),
                Err(error) => {
                    let error: LixError = error.into();
                    if error.code != LixError::CODE_TRANSACTION_CONFLICT {
                        return Err(error);
                    }
                }
            }
        }
        Err(LixError::new(
            LixError::CODE_TRANSACTION_CONFLICT,
            "global migration restart raced publication; retry exact intent",
        ))
    }
    pub(crate) async fn cleanup_native_global_migration_for_account(
        &self,
        request: &crate::sync::NativeGlobalMigrationRequest,
        account: &str,
    ) -> Result<bool, LixError> {
        if self.sync_mode_state().role() != crate::sync::SyncRole::Authority {
            return Err(LixError::new(
                "LIX_MIGRATION_GLOBAL_CLEANUP_UNRESOLVED",
                "migration cleanup requires authority ownership",
            ));
        }
        let adapter = self.storage_adapter();
        for _ in 0..8 {
            let read = adapter.begin_read(Default::default()).await?;
            let proof = crate::sync::authorize_global_migration_cleanup(
                &read,
                self.lix_id(),
                account,
                request,
            )
            .await?;
            let mut writes = adapter.new_write_set();
            let (changed, preconditions) =
                crate::gc::stage_cleanup_global_migration_pin(&read, &mut writes, &proof).await?;
            drop(read);
            if !changed {
                return Ok(false);
            }
            match adapter
                .commit_write_set(
                    writes,
                    crate::storage_adapter::StorageWriteOptions {
                        preconditions,
                        await_durable: true,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(_) => return Ok(true),
                Err(error) => {
                    let error: LixError = error.into();
                    if error.code != LixError::CODE_TRANSACTION_CONFLICT {
                        return Err(error);
                    }
                }
            }
        }
        Err(LixError::new(
            LixError::CODE_TRANSACTION_CONFLICT,
            "global migration cleanup raced authority publication; retry exact request",
        ))
    }
    pub(crate) async fn restart_partial_attempt_for_account(
        &self,
        request: &crate::sync::PartialAttemptRestartRequest,
        account: &str,
    ) -> Result<crate::sync::PartialAttemptRestartOutcome, LixError> {
        request.validate()?;
        if self.sync_mode_state().role() != crate::sync::SyncRole::Authority {
            return Err(LixError::new(
                "LIX_PARTIAL_MERGE_SCOPE_UNSUPPORTED",
                "restart requires an authority",
            ));
        }
        let adapter = self.storage_adapter();
        for _ in 0..8 {
            let read = adapter.begin_read(Default::default()).await?;
            let mut writes = adapter.new_write_set();
            let (outcome, preconditions) = crate::sync::stage_restart_expired_attempt(
                &read,
                &mut writes,
                self.lix_id(),
                account,
                request,
                crate::telemetry::unix_time_ms(),
            )
            .await?;
            drop(read);
            if writes.is_empty() {
                return Ok(outcome);
            }
            match adapter
                .commit_write_set(
                    writes,
                    crate::storage_adapter::StorageWriteOptions {
                        preconditions,
                        await_durable: true,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(_) => return Ok(outcome),
                Err(error) => {
                    let error: LixError = error.into();
                    if error.code != LixError::CODE_TRANSACTION_CONFLICT {
                        return Err(error);
                    }
                }
            }
        }
        Err(LixError::new(
            LixError::CODE_TRANSACTION_CONFLICT,
            "restart raced concurrent authority activity; retry exact request",
        ))
    }

    // Explicit migration route; native outcome shares the authority transaction.
    pub(crate) async fn merge_native_global_migration_for_account(
        &self,
        request: &crate::sync::NativeGlobalMigrationRequest,
        account: &str,
    ) -> Result<crate::sync::NativeGlobalMigrationReceipt, LixError> {
        request.validate()?;
        if self.sync_mode_state().role() != crate::sync::SyncRole::Authority {
            return Err(LixError::new(
                "LIX_PARTIAL_MERGE_SCOPE_UNSUPPORTED",
                "background merge requires an authority",
            ));
        }
        let adapter = self.storage_adapter();
        {
            let read = adapter.begin_read(Default::default()).await?;
            if let Some(receipt) = crate::sync::load_native_global_migration_receipt(
                &read,
                self.lix_id(),
                account,
                request,
            )
            .await?
            {
                return Ok(receipt);
            }
        }
        let session = self
            .open_internal_session(crate::GLOBAL_BRANCH_ID.to_owned(), account.to_owned())
            .await?;
        let result = session
            .session
            .with_write_transaction_lending(async |transaction| {
                transaction
                    .reconcile_native_global_migration(self.lix_id(), request.clone())
                    .await
            })
            .await;
        let closed = session.close().await;
        match result {
            Ok(receipt) => {
                closed?;
                Ok(receipt)
            }
            Err(error) => {
                // A concurrent same-attempt request or an uncertain accepted
                // commit can have won. Recover only the exact immutable receipt.
                let read = adapter.begin_read(Default::default()).await?;
                match crate::sync::load_native_global_migration_receipt(
                    &read,
                    self.lix_id(),
                    account,
                    request,
                )
                .await?
                {
                    Some(receipt) => Ok(receipt),
                    None => Err(error),
                }
            }
        }
    }

    pub(crate) async fn merge_native_migration_for_account(
        &self,
        request: &crate::sync::PartialMergeRequest,
        account: &str,
        source_branch: &str,
    ) -> Result<crate::sync::PartialMergeReceipt, LixError> {
        request.validate()?;
        if self.sync_mode_state().role() != crate::sync::SyncRole::Authority {
            return Err(LixError::new(
                "LIX_PARTIAL_MERGE_SCOPE_UNSUPPORTED",
                "background merge requires an authority",
            ));
        }
        let adapter = self.storage_adapter();
        {
            let read = adapter.begin_read(Default::default()).await?;
            if let Some(receipt) =
                crate::sync::load_authority_merge_receipt(&read, self.lix_id(), account, request)
                    .await?
            {
                return Ok(receipt);
            }
        }
        let session = self
            .open_internal_session(request.branch_id.clone(), account.to_owned())
            .await?;
        let result = session
            .session
            .with_write_transaction_lending(async |transaction| {
                transaction
                    .reconcile_native_migration(self.lix_id(), source_branch, request.clone())
                    .await
            })
            .await;
        let closed = session.close().await;
        match result {
            Ok(receipt) => {
                closed?;
                Ok(receipt)
            }
            Err(error) => {
                // A concurrent same-attempt request or an uncertain accepted
                // commit can have won. Recover only the exact immutable receipt.
                let read = adapter.begin_read(Default::default()).await?;
                match crate::sync::load_authority_merge_receipt(
                    &read,
                    self.lix_id(),
                    account,
                    request,
                )
                .await?
                {
                    Some(receipt) => Ok(receipt),
                    None => Err(error),
                }
            }
        }
    }

    pub(crate) async fn merge_partial_replica_for_account(
        &self,
        request: &crate::sync::PartialMergeRequest,
        account: &str,
    ) -> Result<crate::sync::PartialMergeReceipt, LixError> {
        request.validate()?;
        if self.sync_mode_state().role() != crate::sync::SyncRole::Authority {
            return Err(LixError::new(
                "LIX_PARTIAL_MERGE_SCOPE_UNSUPPORTED",
                "background merge requires an authority",
            ));
        }
        let adapter = self.storage_adapter();
        {
            let read = adapter.begin_read(Default::default()).await?;
            if let Some(receipt) =
                crate::sync::load_authority_merge_receipt(&read, self.lix_id(), account, request)
                    .await?
            {
                return Ok(receipt);
            }
        }
        let session = self
            .open_internal_session(request.branch_id.clone(), account.to_owned())
            .await?;
        let result = session
            .session
            .with_write_transaction_lending(async |transaction| {
                transaction
                    .reconcile_partial_authority_kv(self.lix_id(), request.clone())
                    .await
            })
            .await;
        let closed = session.close().await;
        match result {
            Ok(receipt) => {
                closed?;
                Ok(receipt)
            }
            Err(error) => {
                // A concurrent same-attempt request or an uncertain accepted
                // commit can have won. Recover only the exact immutable receipt.
                let read = adapter.begin_read(Default::default()).await?;
                match crate::sync::load_authority_merge_receipt(
                    &read,
                    self.lix_id(),
                    account,
                    request,
                )
                .await?
                {
                    Some(receipt) => Ok(receipt),
                    None => Err(error),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_adapter::{SharedStorageAdapterRead, StorageWriteOptions};
    async fn publish_wave(
        authority: &Lix<Memory>,
        local: &Lix<Memory>,
        base: &str,
    ) -> crate::sync::PartialMergeRequest {
        let remote = authority.partial_replica_descriptor(None).await.unwrap();
        let source = local.partial_replica_descriptor(None).await.unwrap();
        let request = crate::sync::PartialMergeRequest {
            attempt_id: uuid::Uuid::now_v7().to_string(),
            branch_id: remote.selected_branch.branch_id,
            base_commit_id: base.into(),
            expected_authority_head_commit_id: remote.selected_branch.head.commit_id,
            captured_local_head_commit_id: source.selected_branch.head.commit_id.clone(),
            checkpoint_commit_id: remote.selected_branch.checkpoint.commit_id,
            global_head_commit_id: remote.global_branch.head.commit_id,
            global_checkpoint_commit_id: remote.global_branch.checkpoint.commit_id,
        };
        let commit = crate::sync::export_sync_commit(local, &source.selected_branch.head.commit_id)
            .await
            .unwrap()
            .unwrap();
        let wave = crate::sync::RetainedBodyWaveRequest {
            request: request.clone(),
            expected_previous_commit_id: base.into(),
            bodies: crate::sync::SyncPushRequest {
                commits: vec![commit],
                ref_updates: vec![],
                inline_blobs: vec![],
            },
        };
        let accepted = authority
            .push_retained_body_wave_for_account(&wave, authority.active_account_id())
            .await
            .unwrap();
        assert_eq!(accepted.accepted_tip, request.captured_local_head_commit_id);
        let retry = authority
            .push_retained_body_wave_for_account(&wave, authority.active_account_id())
            .await
            .unwrap();
        assert_eq!(retry.accepted_tip, accepted.accepted_tip);
        assert!(retry.expires_at_ms >= accepted.expires_at_ms);
        let adapter = authority.storage_adapter();
        let read =
            SharedStorageAdapterRead::new(adapter.begin_read(Default::default()).await.unwrap());
        let mut writes = adapter.new_write_set();
        let mut preconditions = Vec::new();
        crate::gc::stage_repository_gc_with_preconditions(read, &mut writes, &mut preconditions)
            .await
            .unwrap();
        adapter
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(
            crate::sync::export_sync_commit(authority, &request.captured_local_head_commit_id)
                .await
                .unwrap()
                .is_some(),
            "accepted unreferenced L must survive GC"
        );
        request
    }
    #[tokio::test]
    async fn retained_body_gc_authority_merge_and_newer_local_suffix_preserve_both_sides() {
        let memory = Memory::new();
        let authority = open_lix()
            .with_storage(memory.clone())
            .await
            .unwrap();
        authority
            .set_sync_role(crate::sync::SyncRole::Authority)
            .unwrap();
        authority.execute("INSERT INTO lix_key_value (key,value) VALUES ('merge-a','base'),('merge-b','base')",&[]).await.unwrap();
        let base = authority
            .partial_replica_descriptor(None)
            .await
            .unwrap()
            .selected_branch
            .head
            .commit_id;
        let local = open_lix()
            .with_storage(memory.fork().unwrap())
            .await
            .unwrap();
        local
            .execute(
                "UPDATE lix_key_value SET value='local' WHERE key='merge-a'",
                &[],
            )
            .await
            .unwrap();
        authority
            .execute(
                "UPDATE lix_key_value SET value='remote' WHERE key='merge-b'",
                &[],
            )
            .await
            .unwrap();
        let first = publish_wave(&authority, &local, &base).await;
        let receipt = authority
            .merge_partial_replica_for_account(&first, authority.active_account_id())
            .await
            .unwrap();
        // A durable M wins even if the caller reaches restart after pin expiry.
        let adapter = authority.storage_adapter();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let intent = crate::sync::PartialAttemptRestartRequest {
            old: first.clone(),
            next_attempt_id: uuid::Uuid::now_v7().to_string(),
        };
        let (outcome, guards) = crate::sync::stage_restart_expired_attempt(
            &read,
            &mut adapter.new_write_set(),
            authority.lix_id(),
            authority.active_account_id(),
            &intent,
            u64::MAX,
        )
        .await
        .unwrap();
        assert!(guards.is_empty());
        assert!(
            matches!(outcome, crate::sync::PartialAttemptRestartOutcome::Committed { receipt: recovered, .. } if recovered == receipt)
        );
        drop(read);
        let merge = crate::sync::export_sync_commit(&authority, &receipt.merge_commit_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            merge.parent_commit_ids,
            vec![
                first.expected_authority_head_commit_id.clone(),
                first.captured_local_head_commit_id.clone()
            ]
        );
        let values = authority
            .execute(
                "SELECT key,value FROM lix_key_value WHERE key LIKE 'merge-%' ORDER BY key",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(values.rows().len(), 2);
        let value = values.rows()[0].get::<Value>("value").unwrap();
        let Value::Jsonb(value) = value else {
            panic!("key/value must retain JSONB value")
        };
        assert_eq!(value.as_json_string().unwrap(), "local");
        // L2 is authored on the original browser L while M exists remotely.
        // The next attempt must traverse M's second parent to prove base L.
        local
            .execute(
                "UPDATE lix_key_value SET value='newer-local' WHERE key='merge-a'",
                &[],
            )
            .await
            .unwrap();
        let second = publish_wave(&authority, &local, &first.captured_local_head_commit_id).await;
        let next = authority
            .merge_partial_replica_for_account(&second, authority.active_account_id())
            .await
            .unwrap();
        let m2 = crate::sync::export_sync_commit(&authority, &next.merge_commit_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            m2.parent_commit_ids,
            vec![
                receipt.merge_commit_id.clone(),
                second.captured_local_head_commit_id
            ]
        );
        authority
            .execute(
                "UPDATE lix_key_value SET value='later' WHERE key='merge-b'",
                &[],
            )
            .await
            .unwrap();
        let before_retry = authority
            .partial_replica_descriptor(None)
            .await
            .unwrap()
            .selected_branch
            .head
            .commit_id;
        assert_eq!(
            authority
                .merge_partial_replica_for_account(&first, authority.active_account_id())
                .await
                .unwrap(),
            receipt
        );
        assert_eq!(
            authority
                .partial_replica_descriptor(None)
                .await
                .unwrap()
                .selected_branch
                .head
                .commit_id,
            before_retry
        );
    }
    #[tokio::test]
    async fn authority_merge_conflict_keeps_both_native_heads_and_live_attempt() {
        let memory = Memory::new();
        let authority = open_lix()
            .with_storage(memory.clone())
            .await
            .unwrap();
        authority
            .set_sync_role(crate::sync::SyncRole::Authority)
            .unwrap();
        authority
            .execute(
                "INSERT INTO lix_key_value (key,value) VALUES ('merge-a','base')",
                &[],
            )
            .await
            .unwrap();
        let base = authority
            .partial_replica_descriptor(None)
            .await
            .unwrap()
            .selected_branch
            .head
            .commit_id;
        let local = open_lix()
            .with_storage(memory.fork().unwrap())
            .await
            .unwrap();
        local
            .execute(
                "UPDATE lix_key_value SET value='local' WHERE key='merge-a'",
                &[],
            )
            .await
            .unwrap();
        authority
            .execute(
                "UPDATE lix_key_value SET value='remote' WHERE key='merge-a'",
                &[],
            )
            .await
            .unwrap();
        let request = publish_wave(&authority, &local, &base).await;
        assert_eq!(
            authority
                .merge_partial_replica_for_account(&request, authority.active_account_id())
                .await
                .unwrap_err()
                .code,
            "LIX_PARTIAL_MERGE_CONFLICT"
        );
        assert_eq!(
            authority
                .partial_replica_descriptor(None)
                .await
                .unwrap()
                .selected_branch
                .head
                .commit_id,
            request.expected_authority_head_commit_id
        );
        assert!(
            crate::sync::export_sync_commit(&authority, &request.captured_local_head_commit_id)
                .await
                .unwrap()
                .is_some()
        );
    }
}

#[cfg(test)]
mod native_migration_tests;

#[cfg(test)]
mod native_global_migration_tests;
