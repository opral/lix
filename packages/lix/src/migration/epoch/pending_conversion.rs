//! Explicit pending-full conversion. The source remains original and retained.
use super::pending_conversion_journal::{
    PendingConversionJournal, load_pending_conversion_journal, persist_pending_conversion_journal,
};
use super::*;
fn same_frozen_request(
    left: &crate::sync::PartialMergeRequest,
    right: &crate::sync::PartialMergeRequest,
) -> bool {
    left.branch_id == right.branch_id
        && left.base_commit_id == right.base_commit_id
        && left.captured_local_head_commit_id == right.captured_local_head_commit_id
        && left.checkpoint_commit_id == right.checkpoint_commit_id
        && left.global_head_commit_id == right.global_head_commit_id
        && left.global_checkpoint_commit_id == right.global_checkpoint_commit_id
}

struct Journal<'a, S> {
    storage: &'a S,
    claim: Bytes,
    repository: String,
    account: String,
    current: PendingConversionJournal,
    raw: Option<Bytes>,
}
impl<S: Storage + Clone + Send + Sync + 'static> crate::sync::ConversionJournalOwner
    for Journal<'_, S>
{
    fn current(&self) -> &PendingConversionJournal {
        &self.current
    }
    fn publish(
        &mut self,
        next: PendingConversionJournal,
    ) -> crate::sync::SyncTransportFuture<'_, ()> {
        Box::pin(async move {
            if next.native_pin_cleaned != self.current.native_pin_cleaned
                || next.native_source_pin != self.current.native_source_pin
                || next.source_bank != self.current.source_bank
                || next.manifest_digest != self.current.manifest_digest
                || !same_frozen_request(&next.request, &self.current.request)
            {
                return Err(epoch_error(
                    "migration journal changed frozen source coordinates",
                ));
            }
            if (next.request.attempt_id != self.current.request.attempt_id
                || next.request.expected_authority_head_commit_id
                    != self.current.request.expected_authority_head_commit_id)
                && !self.current.restart.as_ref().is_some_and(|intent| {
                    self.current.restart_receipt.is_some()
                        && intent.next_attempt_id == next.request.attempt_id
                })
            {
                return Err(epoch_error(
                    "migration attempt changed without durable terminal restart",
                ));
            }
            let raw = persist_pending_conversion_journal(
                self.storage,
                &self.claim,
                &self.repository,
                &self.account,
                &next,
                self.raw.clone(),
            )
            .await?;
            self.current = next;
            self.raw = Some(raw);
            Ok(())
        })
    }
    fn publish_global_successor(
        &mut self,
        next: PendingConversionJournal,
        proof: crate::sync::MigrationGlobalSuccessor,
    ) -> crate::sync::SyncTransportFuture<'_, ()> {
        Box::pin(async move {
            proof.validate_transition(&self.current, &next, &self.repository, &self.account)?;
            if next.source_bank != self.current.source_bank
                || next.manifest_digest != self.current.manifest_digest
                || next.native_pin_cleaned != self.current.native_pin_cleaned
                || next.native_source_pin != self.current.native_source_pin
                || next.accepted_tip != self.current.accepted_tip
                || next.prepared_tip.is_some()
                || next.receipt.is_some()
                || next.restart.is_some()
                || next.restart_receipt.is_some()
            {
                return Err(epoch_error(
                    "global successor changed frozen source or durable upload frontier",
                ));
            }
            let raw = persist_pending_conversion_journal(
                self.storage,
                &self.claim,
                &self.repository,
                &self.account,
                &next,
                self.raw.clone(),
            )
            .await?;
            self.current = next;
            self.raw = Some(raw);
            Ok(())
        })
    }
}
enum FinalConversionBaseline {
    Pending(crate::sync::ReconciledPendingConversion),
    Clean(crate::sync::FinalizedPartialConversion),
}
impl FinalConversionBaseline {
    fn state(&self) -> &crate::sync::PartialReplicaState {
        match self {
            Self::Pending(p) => p.state(),
            Self::Clean(p) => p.state(),
        }
    }
    fn check_deadline(&self) -> Result<(), LixError> {
        match self {
            Self::Pending(p) => p.check_deadline(),
            Self::Clean(p) => p.check_deadline(),
        }
    }
}
pub(super) async fn convert_pending_replica<S: Storage + Clone + Send + Sync + 'static>(
    storage: &S,
    authenticated: &crate::sync::AuthenticatedPartialConversion,
    adapter: StorageAdapter<S>,
) -> Result<PartialEpochAdmission<S>, LixError> {
    let state = authenticated.state();
    let read = adapter.begin_read(ReadOptions::default()).await?;
    let inspected = crate::sync::inspect_full_conversion_manifest(&read).await?;
    if inspected.manifest().repository_id != state.repository_id()
        || inspected.manifest().account_id != state.active_account_id()
    {
        return Err(epoch_error(
            "pending conversion identity differs from authenticated authority",
        ));
    }
    let selected_branch = authenticated.requested_branch().to_owned();
    let global_changed = inspected.manifest().branches.iter().any(|branch| {
        branch.branch_id == crate::GLOBAL_BRANCH_ID
            && branch
                .confirmed
                .as_ref()
                .zip(branch.local.as_ref())
                .is_some_and(|(b, l)| b.head != l.head)
    });
    let global_plan = if global_changed {
        Some(crate::sync::classify_descriptor_global_conversion(
            &inspected,
            &selected_branch,
        )?)
    } else {
        None
    };
    let pending_branches = match &global_plan {
        Some(plan) => plan.existing_dirty_branches.clone(),
        None => crate::sync::ordinary_pending_conversion_branches(&inspected, &selected_branch)?,
    };
    let expected_proofs = pending_branches
        .iter()
        .map(|id| {
            let branch = inspected
                .manifest()
                .branches
                .iter()
                .find(|b| &b.branch_id == id)
                .expect("validated pending branch exists");
            (
                id.clone(),
                branch
                    .local
                    .as_ref()
                    .expect("validated pending local head")
                    .head
                    .clone(),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let manifest_digest = *blake3::hash(
        &serde_json::to_vec(inspected.manifest()).map_err(|e| epoch_error(e.to_string()))?,
    )
    .as_bytes();
    drop(read);
    let (pointer, original) = load_pointer(storage)
        .await?
        .ok_or_else(|| epoch_error("pending source has no epoch"))?;
    let PointerState::Active {
        bank: source_bank,
        generation,
        format,
        ..
    } = pointer
    else {
        return Err(epoch_error("pending source is already migrating"));
    };
    if source_bank != adapter.epoch_bank() {
        return Err(epoch_error("pending source epoch changed"));
    }
    for retained in list_retained_replica_sources(storage).await? {
        if retained.recovery_required && retained.bank != bank_code(source_bank) {
            return Err(LixError::new(
                "LIX_PARTIAL_CONVERSION_UNRESOLVED",
                "another retained source needs explicit export_replica_recovery; all original sources remain preserved",
            ));
        }
    }
    let generation = generation
        .checked_add(1)
        .ok_or_else(|| epoch_error("conversion generation exhausted"))?;
    let target_bank = replica_generation_bank(generation)?;
    let attempt = uuid::Uuid::now_v7();
    let claim = encode_pointer(PointerState::Migrating {
        source: source_bank,
        source_format: format,
        target: target_bank,
        generation,
        attempt,
    });
    replace_pointer(storage, &original, &claim).await?;
    let heartbeat = match start_migration_heartbeat(storage.clone(), claim.clone()) {
        Ok(h) => h,
        Err(e) => {
            replace_pointer(storage, &claim, &original).await?;
            return Err(e);
        }
    };
    let mut published = false;
    let result = async {
        let source =
            StorageAdapter::for_epoch_migration(storage.clone(), source_bank, claim.clone());
        let read = source.begin_read(ReadOptions::default()).await?;
        let current = crate::sync::inspect_full_conversion_manifest(&read).await?;
        let current_digest = *blake3::hash(
            &serde_json::to_vec(current.manifest()).map_err(|e| epoch_error(e.to_string()))?,
        )
        .as_bytes();
        if current_digest != manifest_digest {
            return Err(epoch_error("source changed before conversion freeze"));
        }
        let retention = crate::sync::inspect_replica_rebuild_source(&read, format)
            .await?
            .ok_or_else(|| epoch_error("source identity disappeared"))?;
        drop(read);
        retain_replica_source(storage, &claim, &source, format, &retention).await?;
        let global_proof = match &global_plan {
            Some(plan) => Some(
                native_global_epoch_owner::reconcile_global_source(
                    storage,
                    &claim,
                    &source,
                    source_bank,
                    manifest_digest,
                    &inspected,
                    plan,
                    authenticated,
                )
                .await?,
            ),
            None => None,
        };
        let mut proved = std::collections::BTreeMap::new();
        let mut selected_proof = None;
        for branch_id in &pending_branches {
            let branch_authenticated = crate::sync::authenticate_partial_conversion(
                authenticated.server().clone(),
                Some(branch_id),
            )
            .await?;
            if branch_authenticated.state().repository_id() != state.repository_id()
                || branch_authenticated.state().active_account_id() != state.active_account_id()
            {
                return Err(epoch_error("migration branch authority identity changed"));
            }
            let request = match &global_proof {
                Some(proof) => crate::sync::pending_conversion_request_after_global(
                    &inspected,
                    branch_authenticated.state().descriptor(),
                    uuid::Uuid::now_v7().to_string(),
                    proof,
                )?,
                None => crate::sync::pending_selected_conversion_request(
                    &inspected,
                    branch_authenticated.state().descriptor(),
                    uuid::Uuid::now_v7().to_string(),
                )?,
            };
            let loaded = load_pending_conversion_journal(
                storage,
                &bank_code(source_bank),
                state.repository_id(),
                state.active_account_id(),
                branch_id,
            )
            .await?;
            let (journal, raw) = match loaded {
                Some((j, raw)) => {
                    let mut frozen_request = request.clone();
                    if global_proof.is_some() {
                        // The adopted G belongs to this durable attempt, not the
                        // unchanged frozen source. Resume it exactly before RPC.
                        frozen_request.global_head_commit_id =
                            j.request.global_head_commit_id.clone();
                    }
                    if j.manifest_digest != manifest_digest
                        || !same_frozen_request(&j.request, &frozen_request)
                    {
                        return Err(epoch_error(
                            "retained branch journal differs from frozen source",
                        ));
                    }
                    if let Some(proof) = &global_proof {
                        crate::sync::verify_resumed_global_basis_authenticated(
                            &branch_authenticated,
                            proof,
                            &j.request,
                        )
                        .await?;
                    }
                    (j, Some(raw))
                }
                None => (
                    PendingConversionJournal {
                        version: 2,
                        native_source_pin: Some(uuid::Uuid::now_v7().to_string()),
                        native_pin_cleaned: false,
                        source_bank: bank_code(source_bank),
                        manifest_digest,
                        accepted_tip: request.base_commit_id.clone(),
                        request,
                        prepared_tip: None,
                        receipt: None,
                        restart: None,
                        restart_receipt: None,
                    },
                    None,
                ),
            };
            let mut owner = Journal {
                storage,
                claim: claim.clone(),
                repository: state.repository_id().into(),
                account: state.active_account_id().into(),
                current: journal,
                raw,
            };
            use crate::sync::ConversionJournalOwner as _;
            owner.publish(owner.current.clone()).await?;
            let reconciled = crate::sync::reconcile_pending_conversion_authenticated(
                &source,
                &mut owner,
                &branch_authenticated,
            )
            .await?;
            let (proof_branch, proof_head) = reconciled.source_coordinate();
            if reconciled.manifest_digest() != manifest_digest
                || proof_branch != branch_id
                || expected_proofs.get(proof_branch).map(String::as_str) != Some(proof_head)
            {
                return Err(epoch_error(
                    "native inclusion proof changed frozen branch/source",
                ));
            }
            if proved
                .insert(proof_branch.to_owned(), proof_head.to_owned())
                .is_some()
            {
                return Err(epoch_error("duplicate native branch inclusion proof"));
            }
            if branch_id == &selected_branch {
                selected_proof = Some(FinalConversionBaseline::Pending(reconciled));
            }
        }
        if proved != expected_proofs {
            return Err(epoch_error(
                "conversion lacks an exact proof for every pending source branch",
            ));
        }
        let reconciled = if let Some(selected) = selected_proof {
            selected
        } else {
            let fresh = crate::sync::authenticate_partial_conversion(
                authenticated.server().clone(),
                Some(&selected_branch),
            )
            .await?;
            if fresh.state().repository_id() != state.repository_id()
                || fresh.state().active_account_id() != state.active_account_id()
                || (global_proof.is_none()
                    && fresh.state().descriptor().global_branch.head.commit_id
                        != state.descriptor().global_branch.head.commit_id)
                || fresh
                    .state()
                    .descriptor()
                    .global_branch
                    .checkpoint
                    .commit_id
                    != state.descriptor().global_branch.checkpoint.commit_id
            {
                return Err(epoch_error(
                    "selected conversion baseline changed identity/catalog",
                ));
            }
            FinalConversionBaseline::Clean(fresh.finalize_state().await?)
        };
        if let Some(global) = &global_proof {
            // This proof binds all original new refs and global L to the exact
            // frozen manifest; every remaining old dirty ref was proved above.
            crate::sync::verify_global_conversion_baseline_authenticated(
                authenticated,
                global,
                reconciled.state(),
            )
            .await?;
        }
        reconciled.check_deadline()?;
        let state = reconciled.state();
        let target =
            StorageAdapter::for_epoch_migration(storage.clone(), target_bank, claim.clone());
        clear_bank(&target).await?;
        let read = target.begin_read(Default::default()).await?;
        let mut writes = target.new_write_set();
        let preconditions = crate::sync::stage_partial_bootstrap(&read, &mut writes, state)?;
        crate::init::stage_partial_repository_protocol(&mut writes);
        drop(read);
        target
            .commit_write_set(
                writes,
                WriteOptions {
                    preconditions,
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await?;
        let (_engine, session) =
            Engine::new_partial_replica(target, EngineOptions::new(), state).await?;
        session.close().await?;
        let active = encode_pointer(PointerState::Active {
            bank: target_bank,
            generation,
            format: crate::init::CURRENT_FORMAT_VERSION,
            publication: Some(attempt),
        });
        reconciled.check_deadline()?;
        replace_pointer(storage, &claim, &active).await?;
        published = true;
        reconciled.check_deadline().map_err(|_| {
            LixError::new(
                "LIX_PARTIAL_CONVERSION_EXPIRED_AFTER_PUBLICATION",
                "native merge is durable and full source retained; renew admission before opening",
            )
        })?;
        Ok((
            PartialEpochAdmission {
                adapter: StorageAdapter::for_epoch(storage.clone(), target_bank, active),
                state: state.clone(),
            },
            reconciled,
        ))
    }
    .await;
    let result = match result {
        Ok(v) => Ok(v),
        Err(e) if published || e.code == LixError::CODE_STORAGE_COMMIT_OUTCOME_UNKNOWN => Err(e),
        Err(e) => {
            replace_pointer(storage, &claim, &original).await?;
            Err(e)
        }
    };
    let (admission, proof) = finish_after_heartbeat(heartbeat, result).await?;
    let active = load_pointer(storage)
        .await?
        .ok_or_else(|| epoch_error("published conversion pointer disappeared"))?
        .1;
    crate::sync::finish_conversion_cleanup_bounded(async {
        for branch in &pending_branches {
            let Some((mut journal, raw)) = load_pending_conversion_journal(
                storage,
                &bank_code(source_bank),
                state.repository_id(),
                state.active_account_id(),
                branch,
            )
            .await?
            else {
                return Err(epoch_error("published conversion journal disappeared"));
            };
            if journal.native_pin_cleaned
                || journal.native_source_pin.is_none()
                || journal.receipt.is_none()
            {
                continue;
            }
            crate::sync::cleanup_pending_conversion_authenticated(authenticated, &journal).await?;
            journal.native_pin_cleaned = true;
            persist_pending_conversion_journal(
                storage,
                &active,
                state.repository_id(),
                state.active_account_id(),
                &journal,
                Some(raw),
            )
            .await?;
        }
        if global_plan.is_some() {
            native_global_epoch_owner::cleanup_global_source(
                storage,
                &active,
                source_bank,
                authenticated,
            )
            .await?;
        }
        Ok(())
    })
    .await;
    proof.check_deadline().map_err(|_| {
        LixError::new(
            "LIX_PARTIAL_CONVERSION_EXPIRED_AFTER_PUBLICATION",
            "conversion published durably; renew admission before opening",
        )
    })?;
    Ok(admission)
}

#[cfg(all(test, feature = "server-protocol", not(target_family = "wasm")))]
mod tests;

#[cfg(test)]
mod binding_tests {
    use super::*;
    use crate::sync::ConversionJournalOwner as _;

    #[tokio::test]
    async fn corrupted_frozen_coordinates_cannot_advance_durable_journal() {
        let storage = crate::sync::durable_memory_for_test(crate::Memory::new());
        let source = crate::open_lix()
            .with_storage(storage.clone())
            .await
            .unwrap();
        let descriptor = source.partial_replica_descriptor(None).await.unwrap();
        let repository = source.lix_id().to_owned();
        let account = source.active_account_id().to_owned();
        source.close().await.unwrap();
        drop(source);
        let storage = crate::storage_adapter::StorageSession::acquire(storage)
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
            panic!("expected active source");
        };
        let claim = encode_pointer(PointerState::Migrating {
            source: bank,
            source_format: format,
            target: replica_generation_bank(generation + 1).unwrap(),
            generation: generation + 1,
            attempt: uuid::Uuid::now_v7(),
        });
        replace_pointer(&storage, &original, &claim).await.unwrap();
        let request = crate::sync::PartialMergeRequest {
            attempt_id: uuid::Uuid::now_v7().to_string(),
            branch_id: descriptor.selected_branch.branch_id,
            base_commit_id: descriptor.selected_branch.head.commit_id.clone(),
            expected_authority_head_commit_id: descriptor.selected_branch.head.commit_id.clone(),
            captured_local_head_commit_id: uuid::Uuid::now_v7().to_string(),
            expected_authority_checkpoint_commit_id: descriptor
                .selected_branch
                .checkpoint
                .commit_id
                .clone(),
            captured_local_checkpoint_commit_id: descriptor
                .selected_branch
                .checkpoint
                .commit_id
                .clone(),
            checkpoint_commit_id: descriptor.selected_branch.checkpoint.commit_id,
            global_head_commit_id: descriptor.global_branch.head.commit_id,
            global_checkpoint_commit_id: descriptor.global_branch.checkpoint.commit_id,
        };
        let current = PendingConversionJournal {
            version: 2,
            native_source_pin: None,
            native_pin_cleaned: false,
            source_bank: bank_code(bank),
            manifest_digest: [1; 32],
            accepted_tip: request.base_commit_id.clone(),
            request,
            prepared_tip: None,
            receipt: None,
            restart: None,
            restart_receipt: None,
        };
        let raw = persist_pending_conversion_journal(
            &storage,
            &claim,
            &repository,
            &account,
            &current,
            None,
        )
        .await
        .unwrap();
        let mut owner = Journal {
            storage: &storage,
            claim: claim.clone(),
            repository: repository.clone(),
            account: account.clone(),
            current: current.clone(),
            raw: Some(raw.clone()),
        };
        for field in [
            "checkpointCommitId",
            "globalHeadCommitId",
            "globalCheckpointCommitId",
            "expectedAuthorityHeadCommitId",
        ] {
            let mut json = serde_json::to_value(&current).unwrap();
            json["request"][field] = serde_json::Value::String(uuid::Uuid::now_v7().to_string());
            let corrupted: PendingConversionJournal = serde_json::from_value(json).unwrap();
            corrupted.request.validate().unwrap(); // Syntactically valid corruption still fails closed.
            if field != "expectedAuthorityHeadCommitId" {
                assert!(!same_frozen_request(&corrupted.request, &current.request));
            }
            assert!(owner.publish(corrupted).await.is_err());
            let (_, persisted) = load_pending_conversion_journal(
                &storage,
                &bank_code(bank),
                &repository,
                &account,
                &current.request.branch_id,
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(persisted, raw);
            assert_eq!(load_pointer(&storage).await.unwrap().unwrap().1, claim);
        }
        replace_pointer(&storage, &claim, &original).await.unwrap();
    }
}
