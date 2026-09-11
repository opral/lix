//! Explicit clean full-replica conversion; never part of bounded opening.
use super::*;

fn conversion_required(message: &str) -> LixError {
    LixError::new("LIX_PARTIAL_REPLICA_CONVERSION_RECOVERY_REQUIRED", message)
}

/// The caller authenticates `state` against the selected authority before this
/// operation. Existing format migrations retain their original source banks.
/// Local edits, resets, retained rows and unfinished upload recovery prevent
/// publication: this converter supports only fully acknowledged replicas.
pub(crate) async fn convert_clean_replica_to_partial<S>(
    storage: &S,
    authenticated: &crate::sync::AuthenticatedPartialConversion,
    progress: Option<&Arc<dyn OpenProgressSink>>,
) -> Result<PartialEpochAdmission<S>, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    // Explicit migration may inspect the full source; bounded opening does not.
    let admitted =
        admit_repository_with_server(storage, progress, Some(authenticated.server())).await?;
    let read = admitted.adapter.begin_read(ReadOptions::default()).await?;
    let pending =
        crate::sync::inspect_replica_rebuild_source(&read, crate::init::CURRENT_FORMAT_VERSION)
            .await?
            .is_some_and(|proof| proof.recovery_required);
    drop(read);
    if pending {
        return pending_conversion::convert_pending_replica(
            storage,
            authenticated,
            admitted.adapter,
        )
        .await;
    }
    // Clean sources can resolve their requested ref immediately; pending new
    // refs resolve only after the exact native global proof above.
    let selected_authenticated;
    let authenticated = if authenticated.requested_branch()
        != authenticated.state().descriptor().selected_branch.branch_id
    {
        selected_authenticated = crate::sync::authenticate_partial_conversion(
            authenticated.server().clone(),
            Some(authenticated.requested_branch()),
        )
        .await?;
        if selected_authenticated.state().repository_id() != authenticated.state().repository_id()
            || selected_authenticated.state().active_account_id()
                != authenticated.state().active_account_id()
        {
            return Err(conversion_required(
                "selected conversion authority identity changed",
            ));
        }
        &selected_authenticated
    } else {
        authenticated
    };
    convert_clean_replica_to_partial_inner(
        storage,
        authenticated.state(),
        authenticated.server(),
        progress,
        authenticated.finalize_state(),
    )
    .await
}

async fn convert_clean_replica_to_partial_inner<S, F>(
    storage: &S,
    state: &crate::sync::PartialReplicaState,
    server: &crate::ServerOptions,
    progress: Option<&Arc<dyn OpenProgressSink>>,
    finalized: F,
) -> Result<PartialEpochAdmission<S>, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    F: Future<Output = Result<crate::sync::FinalizedPartialConversion, LixError>>,
{
    if load_pointer(storage).await?.is_none()
        && matches!(
            super::super::inspect_lix(storage).await?,
            super::super::MigrationStatus::Missing
        )
    {
        return Err(LixError::new(
            "LIX_NOT_FOUND",
            "conversion requires an existing full replica",
        ));
    }
    let admitted = admit_repository_with_server(storage, progress, Some(server)).await?;
    if list_retained_replica_sources(storage)
        .await?
        .iter()
        .any(|source| source.recovery_required)
    {
        return Err(conversion_required(
            "retained local edits must be recovered before converting to a partial replica",
        ));
    }
    let read = admitted.adapter.begin_read(ReadOptions::default()).await?;
    if !matches!(
        super::super::inspect_lix_with_adapter(&admitted.adapter).await?,
        super::super::MigrationStatus::Current { .. }
    ) {
        return Err(conversion_required(
            "conversion requires the current full repository layout",
        ));
    }
    let proof = crate::sync::inspect_replica_rebuild_source(&read, crate::init::CURRENT_FORMAT_VERSION).await?
        .ok_or_else(|| conversion_required("standalone and authority repositories must establish full sync admission before conversion"))?;
    check_clean_conversion_identity(&proof, state)?;
    drop(read);
    let (source_pointer, original) = load_pointer(storage)
        .await?
        .ok_or_else(|| epoch_error("conversion source has no active epoch"))?;
    let PointerState::Active {
        bank: source_bank,
        generation,
        format,
        ..
    } = source_pointer
    else {
        return Err(epoch_error("conversion source is already migrating"));
    };
    if source_bank != admitted.adapter.epoch_bank() {
        return Err(epoch_error(
            "conversion source epoch changed during inspection",
        ));
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
        Ok(heartbeat) => heartbeat,
        Err(error) => {
            replace_pointer(storage, &claim, &original).await?;
            return Err(error);
        }
    };
    let mut published = false;
    let result = async {
        let source = StorageAdapter::for_epoch_migration(storage.clone(), source_bank, claim.clone());
        let read = source.begin_read(ReadOptions::default()).await?;
        let proof = crate::sync::inspect_replica_rebuild_source(&read, format).await?
            .ok_or_else(|| conversion_required("source sync admission disappeared"))?;
        check_clean_conversion_identity(&proof, state)?;
        for branch in [&state.descriptor().selected_branch, &state.descriptor().global_branch] {
            let control = crate::branch::BranchHeadControlContext::new().reader(&read)
                .load(&branch.branch_id).await?
                .ok_or_else(|| conversion_required("selected authority branch is absent locally; reconcile full sync before conversion"))?;
            if control.head_commit_id != branch.head.commit_id
                || control.working_diff_checkpoint_commit_id.map(|id| id.to_string()).as_deref() != Some(branch.checkpoint.commit_id.as_str()) {
                return Err(conversion_required("authority descriptor changed relative to acknowledged local refs; reconcile full sync before conversion"));
            }
        }
        drop(read);
        retain_replica_source(storage, &claim, &source, format, &proof).await?;
        let target = StorageAdapter::for_epoch_migration(storage.clone(), target_bank, claim.clone());
        clear_bank(&target).await?;
        let finalized=finalized.await?;
        if state.with_renewed_baseline_lease(finalized.state().baseline_lease().clone())?!=*finalized.state() {
            return Err(conversion_required("conversion finalization changed authenticated baseline coordinates"));
        }
        let state=finalized.state();
        finalized.check_deadline()?;

        let read = target.begin_read(ReadOptions::default()).await?;
        let mut writes = target.new_write_set();
        let preconditions = crate::sync::stage_partial_bootstrap(&read, &mut writes, state)?;
        crate::init::stage_partial_repository_protocol(&mut writes);
        drop(read);
        target.commit_write_set(writes, WriteOptions { preconditions, await_durable: true, ..Default::default() }).await?;
        let (_engine, session) = Engine::new_partial_replica(target, EngineOptions::new(), state).await?;
        session.close().await?;
        let active = encode_pointer(PointerState::Active { bank: target_bank, generation, format: crate::init::CURRENT_FORMAT_VERSION, publication: Some(attempt) });
        finalized.check_deadline()?;
        replace_pointer(storage, &claim, &active).await?;
        published = true;
        finalized.check_deadline().map_err(|_| LixError::new(
            "LIX_PARTIAL_CONVERSION_EXPIRED_AFTER_PUBLICATION",
            "conversion became durable after baseline expiry; retained full source is preserved and fresh admission is required",
        ))?;
        Ok(PartialEpochAdmission {
            adapter: StorageAdapter::for_epoch(storage.clone(), target_bank, active), state: state.clone(),
        })
    }.await;
    let result = match result {
        Ok(value) => Ok(value),
        Err(error) if published || error.code == LixError::CODE_STORAGE_COMMIT_OUTCOME_UNKNOWN => {
            Err(error)
        }
        Err(error) => {
            replace_pointer(storage, &claim, &original).await?;
            Err(error)
        }
    };
    finish_after_heartbeat(heartbeat, result).await
}

fn check_clean_conversion_identity(
    proof: &crate::sync::ReplicaRebuildSource,
    state: &crate::sync::PartialReplicaState,
) -> Result<(), LixError> {
    if proof.repository_id != state.repository_id() || proof.account_id != state.active_account_id()
    {
        return Err(conversion_required(
            "authenticated descriptor disagrees with source repository or account",
        ));
    }
    if proof.recovery_required {
        return Err(conversion_required(
            "pending edits, resets, uploads or local retained rows must be recovered before partial conversion",
        ));
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn clean_conversion_publishes_bounded_partial_and_retains_full_source() {
        for (pending, expired, monotonic_expired) in [
            (false, false, false),
            (true, false, false),
            (false, true, false),
            (false, false, true),
        ] {
            let storage = crate::sync::durable_memory_for_test(crate::Memory::new());
            let full = crate::open_lix()
                .with_storage(storage.clone())
                .await
                .unwrap();
            full.execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('convert', 'kept')",
                &[],
            )
            .await
            .unwrap();
            let descriptor = full.partial_replica_descriptor(None).await.unwrap();
            let adapter = full.storage_adapter();
            let read = adapter.begin_read(Default::default()).await.unwrap();
            let controls = crate::branch::BranchHeadControlContext::new()
                .reader(&read)
                .scan()
                .await
                .unwrap();
            let mut confirmed = serde_json::Map::new();
            for (branch, control) in controls {
                confirmed.insert(branch, serde_json::json!({
                    "state": "headed", "headCommitId":control.head_commit_id.to_string(),
                    "checkpointCommitId":control.working_diff_checkpoint_commit_id.unwrap().to_string(),
                }));
            }
            if pending {
                confirmed.clear();
            }
            drop(read);
            let mut writes = adapter.new_write_set();
            writes.put(
                crate::sync::SYNC_REPLICA_STATE_SPACE,
                crate::sync::replica_state_key(),
                serde_json::to_vec(&serde_json::json!({
                    "activeAccountId":crate::ANONYMOUS_ACCOUNT_ID,
                    "cursor":0,"authoritativeBranches":confirmed,"authorityKnownCommitIds":[],
                }))
                .unwrap(),
            );
            adapter
                .commit_write_set(writes, Default::default())
                .await
                .unwrap();
            let state = crate::sync::PartialReplicaState::new(
                format!("https://example.test/lix/{}", full.lix_id()),
                crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
                uuid::Uuid::now_v7().to_string(),
                descriptor,
            )
            .unwrap();
            full.close().await.unwrap();
            drop(full);
            let storage = crate::storage::StorageSession::acquire(storage)
                .await
                .unwrap();
            let before = load_pointer(&storage).await.unwrap().unwrap().1;
            let converted = convert_clean_replica_to_partial_inner(
                &storage,
                &state,
                &crate::ServerOptions::new(state.remote_id()),
                None,
                std::future::ready(if expired {
                    Err(LixError::new(
                        "LIX_PARTIAL_BASELINE_EXPIRED",
                        "fixture authority rejected expired renewal",
                    ))
                } else {
                    Ok(crate::sync::FinalizedPartialConversion::for_test(
                        state.clone(),
                        if monotonic_expired {
                            Duration::ZERO
                        } else {
                            Duration::from_secs(300)
                        },
                    ))
                }),
            )
            .await;
            if pending || expired || monotonic_expired {
                assert_eq!(
                    converted.err().unwrap().code,
                    if pending {
                        "LIX_PARTIAL_REPLICA_CONVERSION_RECOVERY_REQUIRED"
                    } else if monotonic_expired {
                        "LIX_PARTIAL_CANDIDATE_EXPIRED"
                    } else {
                        "LIX_PARTIAL_BASELINE_EXPIRED"
                    }
                );
                assert_eq!(load_pointer(&storage).await.unwrap().unwrap().1, before);
                let full = crate::open_lix().with_storage(storage).await.unwrap();
                assert_eq!(
                    full.execute("SELECT value FROM lix_key_value WHERE key = 'convert'", &[])
                        .await
                        .unwrap()
                        .rows()
                        .len(),
                    1
                );
                full.close().await.unwrap();
            } else {
                let converted = converted.unwrap();
                assert_eq!(
                    admit_partial_epoch(&storage).await.unwrap().state,
                    state
                );
                assert_ne!(load_pointer(&storage).await.unwrap().unwrap().1, before);
                let retained = list_retained_replica_sources(&storage).await.unwrap();
                assert_eq!(retained.len(), 1);
                assert!(!retained[0].recovery_required);
                let read = converted
                    .adapter
                    .begin_read(Default::default())
                    .await
                    .unwrap();
                assert!(
                    crate::init::is_partial_repository_protocol(&read)
                        .await
                        .unwrap()
                );
                // The old full handle is fenced even though its source bank is retained.
                assert!(adapter.begin_read(Default::default()).await.is_err());
            }
        }
    }
}
