//! Child of epoch pending conversion; global journal shares the source claim.
use super::native_global_conversion_journal::{GlobalBodyFrontier, GlobalConversionJournal};
use super::native_global_journal_io::{
    load_global_conversion_journal, persist_global_conversion_journal,
};
use super::*;
struct GlobalJournal<'a, S> {
    storage: &'a S,
    claim: Bytes,
    repository: String,
    account: String,
    current: GlobalConversionJournal,
    raw: Option<Bytes>,
}
impl<S: Storage + Clone + Send + Sync + 'static> crate::sync::GlobalConversionJournalOwner
    for GlobalJournal<'_, S>
{
    fn current(&self) -> &GlobalConversionJournal {
        &self.current
    }
    fn publish(
        &mut self,
        next: GlobalConversionJournal,
    ) -> crate::sync::SyncTransportFuture<'_, ()> {
        Box::pin(async move {
            next.validate()?;
            if next.source_bank != self.current.source_bank
                || next.manifest_digest != self.current.manifest_digest
                || next.cleanup_complete != self.current.cleanup_complete
                || next.request.base_commit_id != self.current.request.base_commit_id
                || next.request.captured_local_head_commit_id
                    != self.current.request.captured_local_head_commit_id
                || next.request.checkpoint_commit_id != self.current.request.checkpoint_commit_id
                || next.request.new_branches != self.current.request.new_branches
            {
                return Err(epoch_error(
                    "global journal changed frozen original native coordinates",
                ));
            }
            if next.request.attempt_id != self.current.request.attempt_id
                || next.request.expected_authority_head_commit_id
                    != self.current.request.expected_authority_head_commit_id
            {
                let Some(crate::sync::NativeGlobalRestartReceipt::Restarted { intent }) =
                    &self.current.restart_receipt
                else {
                    return Err(epoch_error(
                        "global migration attempt changed without terminal restart receipt",
                    ));
                };
                if intent.next_attempt_id != next.request.attempt_id
                    || next.previous_abort != self.current.restart_receipt
                {
                    return Err(epoch_error(
                        "global migration successor differs from exact durable restart",
                    ));
                }
            }
            let raw = persist_global_conversion_journal(
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
pub(super) async fn reconcile_global_source<S: Storage + Clone + Send + Sync + 'static>(
    storage: &S,
    claim: &Bytes,
    source: &StorageAdapter<S>,
    source_bank: EpochBank,
    manifest_digest: [u8; 32],
    inspected: &crate::sync::InspectedFullConversion,
    plan: &crate::sync::DescriptorGlobalConversion,
    authenticated: &crate::sync::AuthenticatedPartialConversion,
) -> Result<crate::sync::ReconciledGlobalConversion, LixError> {
    let state = authenticated.state();
    let descriptor = state.descriptor();
    if descriptor.global_branch.checkpoint.commit_id != plan.global_base.checkpoint {
        return Err(epoch_error(
            "global checkpoint changed before explicit descriptor migration",
        ));
    }
    let request = crate::sync::NativeGlobalMigrationRequest {
        attempt_id: uuid::Uuid::now_v7().to_string(),
        base_commit_id: plan.global_base.head.clone(),
        expected_authority_head_commit_id: descriptor.global_branch.head.commit_id.clone(),
        captured_local_head_commit_id: plan.global_local.head.clone(),
        checkpoint_commit_id: plan.global_base.checkpoint.clone(),
        new_branches: plan.new_branches.clone(),
    };
    request.validate()?;
    let read = source.begin_read(Default::default()).await?;
    crate::sync::prove_local_descriptor_global_source(&read, &request, state.active_account_id())
        .await?;
    let boundaries =
        crate::sync::global_new_branch_upload_boundaries(&read, inspected.manifest(), &request)
            .await?;
    drop(read);
    let loaded = load_global_conversion_journal(
        storage,
        &bank_code(source_bank),
        state.repository_id(),
        state.active_account_id(),
    )
    .await?;
    let (journal, raw) = match loaded {
        Some((j, raw)) => {
            if j.manifest_digest != manifest_digest
                || j.request.base_commit_id != request.base_commit_id
                || j.request.captured_local_head_commit_id != request.captured_local_head_commit_id
                || j.request.checkpoint_commit_id != request.checkpoint_commit_id
                || j.request.new_branches != request.new_branches
            {
                return Err(epoch_error("global journal differs from frozen source"));
            }
            (j, Some(raw))
        }
        None => (
            GlobalConversionJournal {
                version: 2,
                source_bank: bank_code(source_bank),
                manifest_digest,
                request,
                frontiers: boundaries
                    .into_iter()
                    .map(|(branch, accepted)| {
                        (
                            branch,
                            GlobalBodyFrontier {
                                accepted,
                                prepared: None,
                            },
                        )
                    })
                    .collect(),
                acknowledged_roots: Default::default(),
                restart_intent: None,
                restart_receipt: None,
                previous_abort: None,
                receipt: None,
                cleanup_complete: false,
            },
            None,
        ),
    };
    let mut owner = GlobalJournal {
        storage,
        claim: claim.clone(),
        repository: state.repository_id().into(),
        account: state.active_account_id().into(),
        current: journal,
        raw,
    };
    use crate::sync::GlobalConversionJournalOwner as _;
    owner.publish(owner.current.clone()).await?;
    crate::sync::reconcile_global_conversion_authenticated(
        source,
        inspected.manifest(),
        &mut owner,
        authenticated,
    )
    .await
}

pub(super) async fn cleanup_global_source<S: Storage + Clone + Send + Sync + 'static>(
    storage: &S,
    active: &Bytes,
    source_bank: EpochBank,
    authenticated: &crate::sync::AuthenticatedPartialConversion,
) -> Result<bool, LixError> {
    let state = authenticated.state();
    let Some((mut journal, raw)) = load_global_conversion_journal(
        storage,
        &bank_code(source_bank),
        state.repository_id(),
        state.active_account_id(),
    )
    .await?
    else {
        return Err(epoch_error(
            "global conversion journal disappeared after publication",
        ));
    };
    if journal.cleanup_complete {
        return Ok(false);
    }
    let receipt = journal
        .receipt
        .as_ref()
        .ok_or_else(|| epoch_error("global cleanup requires exact durable native outcome"))?;
    crate::sync::cleanup_global_conversion_authenticated(authenticated, &receipt.request).await?;
    journal.cleanup_complete = true;
    persist_global_conversion_journal(
        storage,
        active,
        state.repository_id(),
        state.active_account_id(),
        &journal,
        Some(raw),
    )
    .await?;
    Ok(true)
}
