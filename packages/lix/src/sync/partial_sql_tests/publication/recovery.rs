use super::*;
use crate::sync::http::CandidateBaselineDeadline;
use crate::sync::partial_publication::{
    PartialRecoveryPolicy, prepare_clean_lease_reacquisition, prepare_partial_publication,
};
fn deadline(wire: &crate::sync::LeasedPartialReplicaDescriptor) -> CandidateBaselineDeadline {
    CandidateBaselineDeadline::for_test(&wire.lease.lease_id, std::time::Duration::from_secs(60))
}
async fn lease_only(
    engine: &Engine<Memory>,
    authority: &Lix<Memory>,
) -> PreparedPartialPublication {
    let wire = authority
        .leased_partial_replica_descriptor(None)
        .await
        .unwrap();
    prepare_clean_lease_reacquisition(engine, &wire, deadline(&wire))
        .await
        .unwrap()
}
async fn recovery_candidate(
    engine: &Engine<Memory>,
    old: &PartialReplicaState,
    authority: &Lix<Memory>,
) -> (Arc<PartialReplicaState>, PreparedPartialPublication) {
    let wire = authority
        .leased_partial_replica_descriptor(None)
        .await
        .unwrap();
    let budget = deadline(&wire);
    let next = Arc::new(
        old.with_leased_descriptor_and_fresh_generations(wire)
            .unwrap(),
    );
    let storage = engine.storage();
    let mut seen = BTreeSet::new();
    for _ in 0..256 {
        let error = match prepare_partial_publication(
            engine,
            next.clone(),
            budget.clone(),
            PartialRecoveryPolicy::ExpiredBaseline,
        )
        .await
        {
            Ok(Some(prepared)) => return (next, prepared),
            Ok(None) => panic!("expiry recovery must rebuild different serving basis"),
            Err(error) => error,
        };
        if let Some(address) = NativeObjectRef::from_missing_error(&error).unwrap() {
            assert!(seen.insert(format!("{address:?}")));
            hydrate_native_object(
                &storage,
                old,
                address,
                32 * 1024 * 1024,
                |request| async move { authority.read_sync_native_object_range(&request).await },
            )
            .await
            .unwrap();
        } else if let Some(address) = NativeMetadataRef::from_missing_error(&error).unwrap() {
            assert!(seen.insert(format!("{address:?}")));
            hydrate_metadata(&storage, old, authority, address, &mut Fetches::default())
                .await
                .unwrap();
        } else {
            panic!("{error:?}");
        }
    }
    panic!("recovery progress budget exceeded")
}
#[tokio::test]
async fn same_basis_lease_recovery_keeps_generations_catalog_and_push_coordinates() {
    let (authority, engine, session, old) = fixture().await;
    let storage = engine.storage();
    execute_hydrating(
        &session,
        &storage,
        &old,
        &authority,
        "SELECT value FROM lix_key_value WHERE key='resident'",
        &[],
        &mut Fetches::default(),
    )
    .await
    .unwrap();
    let controls = admitted_controls(&storage, &old).await.unwrap();
    let read = storage.begin_read(Default::default()).await.unwrap();
    let catalog = crate::catalog::load_catalog_revision(&read).await.unwrap();
    drop(read);
    // No native hydration loop: preparation must succeed from bounded metadata.
    let prepared = lease_only(&engine, &authority).await;
    publish_prepared_partial(engine.clone(), prepared)
        .await
        .unwrap();
    let next = engine.sync_mode().partial_admission().unwrap();
    assert_ne!(
        next.baseline_lease().lease_id,
        old.baseline_lease().lease_id
    );
    assert_eq!(next.descriptor(), old.descriptor());
    assert_eq!(admitted_controls(&storage, &next).await.unwrap(), controls);
    let read = storage.begin_read(Default::default()).await.unwrap();
    assert_eq!(
        crate::catalog::load_catalog_revision(&read).await.unwrap(),
        catalog
    );
    let (push, _, _) = crate::sync::partial_push_state::load_partial_push_state(
        &read,
        &next,
        &next.descriptor().selected_branch.branch_id,
    )
    .await
    .unwrap();
    assert_eq!(push.confirmed.head, controls[0].head_commit_id.to_string());
    drop(read);
    assert!(
        value(
            session
                .execute("SELECT value FROM lix_key_value WHERE key='resident'", &[])
                .await
                .unwrap()
        )
        .contains("before")
    );
}
#[tokio::test]
async fn own_ack_recovery_rebuilds_old_serving_basis_even_when_confirmed_matches() {
    let (authority, engine, session, old) = fixture().await;
    let storage = engine.storage();
    execute_hydrating(
        &session,
        &storage,
        &old,
        &authority,
        "UPDATE lix_key_value SET value='local-acked' WHERE key='resident'",
        &[],
        &mut Fetches::default(),
    )
    .await
    .unwrap();
    for branch in [
        &old.descriptor().global_branch.branch_id,
        &old.descriptor().selected_branch.branch_id,
    ] {
        for wave in 0..4 {
            let uploaded = crate::sync::partial_upload_cycle::upload_partial_once(
                &storage,
                &old,
                branch,
                uuid::Uuid::now_v7().to_string(),
                32,
                1024 * 1024,
                {
                    let remote = &authority;
                    let account = old.active_account_id();
                    move |request| async move {
                        remote
                            .push_sync_repository_for_account(&request, account)
                            .await
                    }
                },
            )
            .await
            .unwrap();
            if !uploaded {
                break;
            }
            assert!(wave < 3);
        }
    }
    let wire = authority
        .leased_partial_replica_descriptor(None)
        .await
        .unwrap();
    let normal = Arc::new(
        old.with_leased_descriptor_and_fresh_generations(wire.clone())
            .unwrap(),
    );
    assert!(
        prepare_partial_publication(
            &engine,
            normal,
            deadline(&wire),
            PartialRecoveryPolicy::Normal
        )
        .await
        .unwrap()
        .is_none()
    );
    assert!(
        prepare_clean_lease_reacquisition(&engine, &wire, deadline(&wire))
            .await
            .is_err()
    );
    let (next, prepared) = recovery_candidate(&engine, &old, &authority).await;
    publish_prepared_partial(engine.clone(), prepared)
        .await
        .unwrap();
    assert_ne!(
        next.serving_generation(&next.descriptor().selected_branch.branch_id)
            .unwrap(),
        old.serving_generation(&old.descriptor().selected_branch.branch_id)
            .unwrap()
    );
    session
        .execute(
            "UPDATE lix_key_value SET value='warm-after-recovery' WHERE key='resident'",
            &[],
        )
        .await
        .unwrap();
}
#[tokio::test]
async fn changed_basis_recovery_prepares_previously_negative_scope() {
    let (authority, engine, session, old) = fixture().await;
    let storage = engine.storage();
    let sql = "SELECT value FROM lix_key_value WHERE key='future-recovery'";
    assert!(
        execute_hydrating(
            &session,
            &storage,
            &old,
            &authority,
            sql,
            &[],
            &mut Fetches::default()
        )
        .await
        .unwrap()
        .rows()
        .is_empty()
    );
    authority
        .execute(
            "INSERT INTO lix_key_value(key,value) VALUES('future-recovery','arrived')",
            &[],
        )
        .await
        .unwrap();
    let (_, prepared) = recovery_candidate(&engine, &old, &authority).await;
    assert!(session.execute(sql, &[]).await.unwrap().rows().is_empty());
    publish_prepared_partial(engine, prepared).await.unwrap();
    assert!(value(session.execute(sql, &[]).await.unwrap()).contains("arrived"));
}
#[tokio::test]
async fn pending_suffix_blocks_recovery_without_losing_local_edit() {
    let (authority, engine, session, old) = fixture().await;
    let storage = engine.storage();
    execute_hydrating(
        &session,
        &storage,
        &old,
        &authority,
        "UPDATE lix_key_value SET value='pending' WHERE key='resident'",
        &[],
        &mut Fetches::default(),
    )
    .await
    .unwrap();
    let before = admitted_controls(&storage, &old).await.unwrap();
    let branch = &old.descriptor().selected_branch.branch_id;
    let read = storage.begin_read(Default::default()).await.unwrap();
    let (push, _, _) =
        crate::sync::partial_push_state::load_partial_push_state(&read, &old, branch)
            .await
            .unwrap();
    let upload = crate::sync::partial_push_state::PreparedPartialUpload {
        created_refs: Vec::new(),
        attempt_id: uuid::Uuid::now_v7().to_string(),
        expected: push.confirmed,
        target: crate::sync::partial_push_state::PartialPushCoordinate {
            head: before[0].head_commit_id.to_string(),
            checkpoint: before[0]
                .working_diff_checkpoint_commit_id
                .unwrap()
                .to_string(),
        },
    };
    let mut writes = storage.new_write_set();
    let preconditions = crate::sync::partial_push_state::stage_prepare_partial_upload(
        &read,
        &mut writes,
        &old,
        branch,
        &upload,
    )
    .await
    .unwrap();
    drop(read);
    storage
        .commit_partial_replica_write_set(
            crate::sync::partial_replica_write_capability(),
            writes,
            StorageWriteOptions {
                preconditions,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let wire = authority
        .leased_partial_replica_descriptor(None)
        .await
        .unwrap();
    let error = match prepare_clean_lease_reacquisition(&engine, &wire, deadline(&wire)).await {
        Err(error) => error,
        Ok(_) => panic!("pending suffix was overwritten"),
    };
    assert_eq!(error.code, "LIX_PARTIAL_REPLICA_BASELINE_RECOVERY_PENDING");
    let read = storage.begin_read(Default::default()).await.unwrap();
    let (retained, _, _) =
        crate::sync::partial_push_state::load_partial_push_state(&read, &old, branch)
            .await
            .unwrap();
    assert_eq!(retained.prepared, Some(upload));
    drop(read);
    assert_eq!(admitted_controls(&storage, &old).await.unwrap(), before);
    assert!(
        value(
            session
                .execute("SELECT value FROM lix_key_value WHERE key='resident'", &[])
                .await
                .unwrap()
        )
        .contains("pending")
    );
}
#[tokio::test]
async fn cached_reopen_remains_usable_after_authority_closes() {
    let (authority, engine, session, old) = fixture().await;
    let storage = engine.storage();
    let sql = "SELECT value FROM lix_key_value WHERE key='resident'";
    execute_hydrating(
        &session,
        &storage,
        &old,
        &authority,
        sql,
        &[],
        &mut Fetches::default(),
    )
    .await
    .unwrap();
    authority.close().await.unwrap();
    // The persisted wall-clock hint is deliberately in the past. Reopening
    // native cached data does not turn that hint into a foreground handshake.
    let mut lease = old.baseline_lease().clone();
    lease.expires_at_ms = 1;
    let expired = Arc::new(old.with_reacquired_baseline_lease(lease).unwrap());
    let read = storage.begin_read(Default::default()).await.unwrap();
    let (_, raw) = crate::sync::partial_state::load_partial_replica_state(&read)
        .await
        .unwrap()
        .unwrap();
    let mut writes = storage.new_write_set();
    let guard =
        crate::sync::partial_state::stage_partial_replica_state(&mut writes, &expired, Some(raw))
            .unwrap();
    drop(read);
    storage
        .commit_partial_replica_write_set(
            crate::sync::partial_replica_write_capability(),
            writes,
            StorageWriteOptions {
                preconditions: vec![guard],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let (reopened, fresh) = Engine::new_partial_replica(storage, EngineOptions::new(), &expired)
        .await
        .unwrap();
    reopened
        .sync_mode()
        .admit_partial_replica(expired, crate::sync::partial_replica_write_capability());
    assert!(value(fresh.execute(sql, &[]).await.unwrap()).contains("before"));
}
#[tokio::test]
async fn lease_recovery_rejects_racing_write_and_elapsed_candidate_budget() {
    let (authority, engine, session, old) = fixture().await;
    let storage = engine.storage();
    let wire = authority
        .leased_partial_replica_descriptor(None)
        .await
        .unwrap();
    assert!(
        prepare_clean_lease_reacquisition(
            &engine,
            &wire,
            CandidateBaselineDeadline::for_test(&wire.lease.lease_id, std::time::Duration::ZERO)
        )
        .await
        .is_err()
    );
    let prepared = prepare_clean_lease_reacquisition(&engine, &wire, deadline(&wire))
        .await
        .unwrap();
    execute_hydrating(
        &session,
        &storage,
        &old,
        &authority,
        "UPDATE lix_key_value SET value='raced' WHERE key='resident'",
        &[],
        &mut Fetches::default(),
    )
    .await
    .unwrap();
    assert!(
        publish_prepared_partial(engine.clone(), prepared)
            .await
            .is_err()
    );
    assert_eq!(
        engine.sync_mode().partial_admission().as_deref(),
        Some(old.as_ref())
    );
    assert!(
        value(
            session
                .execute("SELECT value FROM lix_key_value WHERE key='resident'", &[])
                .await
                .unwrap()
        )
        .contains("raced")
    );
}
