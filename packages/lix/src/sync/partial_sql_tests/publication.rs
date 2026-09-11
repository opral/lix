use super::*;
use crate::sync::partial_publication::{
    PreparedPartialPublication, prepare_clean_partial_publication, publish_prepared_partial,
};
use std::sync::Arc;

async fn fixture() -> (
    Lix<Memory>,
    Arc<Engine<Memory>>,
    SessionContext<Memory>,
    Arc<PartialReplicaState>,
) {
    fixture_with_account(None).await
}
async fn fixture_with_account(
    account: Option<&str>,
) -> (
    Lix<Memory>,
    Arc<Engine<Memory>>,
    SessionContext<Memory>,
    Arc<PartialReplicaState>,
) {
    let authority = open_lix().await.unwrap();
    if let Some(account) = account {
        authority
            .ensure_account(account, "custom principal", "human")
            .await
            .unwrap();
    }
    authority
        .set_sync_role(crate::sync::SyncRole::Authority)
        .unwrap();
    authority
        .execute(
            "INSERT INTO lix_key_value (key,value) VALUES ('resident','before')",
            &[],
        )
        .await
        .unwrap();
    let state = Arc::new(
        PartialReplicaState::new(
            format!("https://example.test/lix/{}", authority.lix_id()),
            account.unwrap_or(authority.active_account_id()).into(),
            uuid::Uuid::now_v7().to_string(),
            authority.partial_replica_descriptor(None).await.unwrap(),
        )
        .unwrap(),
    );
    let storage = StorageAdapter::new(Memory::new());
    let read = storage.begin_read(Default::default()).await.unwrap();
    let mut writes = storage.new_write_set();
    let preconditions = stage_partial_bootstrap(&read, &mut writes, &state).unwrap();
    crate::init::stage_partial_repository_protocol(&mut writes);
    drop(read);
    storage
        .commit_write_set(
            writes,
            StorageWriteOptions {
                preconditions,
                await_durable: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let (engine, session) =
        Engine::new_partial_replica(storage.clone(), EngineOptions::new(), &state)
            .await
            .unwrap();
    engine.sync_mode().admit_partial_replica(
        state.clone(),
        crate::sync::partial_replica_write_capability(),
    );
    storage.admit_partial_replica_writer(crate::sync::partial_replica_write_capability());
    (authority, Arc::new(engine), session, state)
}
async fn prepare_hydrating<S: crate::storage_adapter::Storage + Clone + Send + Sync + 'static>(
    engine: &Engine<S>,
    old: &PartialReplicaState,
    next: Arc<PartialReplicaState>,
    authority: &Lix<Memory>,
) -> PreparedPartialPublication {
    let deadline = super::super::http::CandidateBaselineDeadline::for_test(
        &next.baseline_lease().lease_id,
        std::time::Duration::from_secs(300),
    );
    prepare_hydrating_with_deadline(engine, old, next, authority, deadline).await
}
async fn prepare_hydrating_with_deadline<
    S: crate::storage_adapter::Storage + Clone + Send + Sync + 'static,
>(
    engine: &Engine<S>,
    old: &PartialReplicaState,
    next: Arc<PartialReplicaState>,
    authority: &Lix<Memory>,
    deadline: super::super::http::CandidateBaselineDeadline,
) -> PreparedPartialPublication {
    let storage = engine.storage();
    let mut seen = BTreeSet::new();
    for _ in 0..256 {
        let error =
            match prepare_clean_partial_publication(engine, next.clone(), deadline.clone()).await {
                Ok(Some(prepared)) => return prepared,
                Ok(None) => panic!("fixture requires a remote change"),
                Err(error) => error,
            };
        if let Some(address) = NativeObjectRef::from_missing_error(&error).unwrap() {
            assert!(seen.insert(format!("object:{address:?}")), "{error}");
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
            assert!(seen.insert(format!("metadata:{address:?}")), "{error}");
            hydrate_metadata(&storage, old, authority, address, &mut Fetches::default())
                .await
                .unwrap();
        } else {
            panic!("candidate failed: {error:?}");
        }
    }
    panic!("candidate hydration exceeded bounded progress");
}

#[tokio::test]
async fn remote_publication_prepares_negative_scope_then_keeps_reads_local() {
    let (authority, engine, session, old) = fixture().await;
    let storage = engine.storage();
    let sql = "SELECT value FROM lix_key_value WHERE key='future'";
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
            "INSERT INTO lix_key_value (key,value) VALUES ('future','arrived')",
            &[],
        )
        .await
        .unwrap();
    let next = Arc::new(
        old.with_descriptor_and_fresh_generations(
            authority.partial_replica_descriptor(None).await.unwrap(),
        )
        .unwrap(),
    );
    let prepared = prepare_hydrating(&engine, &old, next.clone(), &authority).await;
    assert!(
        session.execute(sql, &[]).await.unwrap().rows().is_empty(),
        "preparation must not expose new rows"
    );
    publish_prepared_partial(engine.clone(), prepared)
        .await
        .unwrap();
    assert_eq!(
        engine.sync_mode().partial_admission().as_deref(),
        Some(next.as_ref())
    );
    assert!(value(session.execute(sql, &[]).await.unwrap()).contains("arrived"));
    // Fresh engine and session prove this is durable native state, not merely
    // a cached query result held by the original SQL execution.
    let (reopened, reopened_session) =
        Engine::new_partial_replica(storage.clone(), EngineOptions::new(), &next)
            .await
            .unwrap();
    reopened
        .sync_mode()
        .admit_partial_replica(next, crate::sync::partial_replica_write_capability());
    assert!(value(reopened_session.execute(sql, &[]).await.unwrap()).contains("arrived"));
}

#[tokio::test]
async fn remote_publication_rejects_new_interest_after_candidate_preparation() {
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
    authority
        .execute(
            "UPDATE lix_key_value SET value='remote' WHERE key='resident'",
            &[],
        )
        .await
        .unwrap();
    let next = Arc::new(
        old.with_descriptor_and_fresh_generations(
            authority.partial_replica_descriptor(None).await.unwrap(),
        )
        .unwrap(),
    );
    let prepared = prepare_hydrating(&engine, &old, next, &authority).await;
    execute_hydrating(
        &session,
        &storage,
        &old,
        &authority,
        "SELECT value FROM lix_key_value WHERE key='new-negative-interest'",
        &[],
        &mut Fetches::default(),
    )
    .await
    .unwrap();
    let error = publish_prepared_partial(engine.clone(), prepared)
        .await
        .unwrap_err();
    assert_eq!(error.code, "LIX_PARTIAL_READ_INTEREST_CHANGED");
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
        .contains("before")
    );
}

#[tokio::test]
async fn remote_publication_rejects_racing_local_write_without_losing_pending_edit() {
    let (authority, engine, session, old) = fixture().await;
    let storage = engine.storage();
    let select = "SELECT value FROM lix_key_value WHERE key='resident'";
    execute_hydrating(
        &session,
        &storage,
        &old,
        &authority,
        select,
        &[],
        &mut Fetches::default(),
    )
    .await
    .unwrap();
    authority
        .execute(
            "UPDATE lix_key_value SET value='remote' WHERE key='resident'",
            &[],
        )
        .await
        .unwrap();
    let next = Arc::new(
        old.with_descriptor_and_fresh_generations(
            authority.partial_replica_descriptor(None).await.unwrap(),
        )
        .unwrap(),
    );
    let prepared = prepare_hydrating(&engine, &old, next, &authority).await;
    execute_hydrating(
        &session,
        &storage,
        &old,
        &authority,
        "UPDATE lix_key_value SET value='pending-local' WHERE key='resident'",
        &[],
        &mut Fetches::default(),
    )
    .await
    .unwrap();
    let before = admitted_controls(&storage, &old).await.unwrap();
    let error = publish_prepared_partial(engine.clone(), prepared)
        .await
        .unwrap_err();
    assert!(
        error.code == LixError::CODE_TRANSACTION_CONFLICT
            || error.code == "LIX_PARTIAL_READ_INTEREST_CHANGED",
        "{error:?}"
    );
    assert_eq!(
        engine.sync_mode().partial_admission().as_deref(),
        Some(old.as_ref())
    );
    assert_eq!(admitted_controls(&storage, &old).await.unwrap(), before);
    assert!(value(session.execute(select, &[]).await.unwrap()).contains("pending-local"));
    let read = storage.begin_read(Default::default()).await.unwrap();
    let (push, _, _) = crate::sync::partial_push_state::load_partial_push_state(
        &read,
        &old,
        &old.descriptor().selected_branch.branch_id,
    )
    .await
    .unwrap();
    assert_ne!(
        push.confirmed.head,
        before[0].head_commit_id.to_string(),
        "rejected remote publication must retain the unacknowledged local suffix"
    );
}

#[tokio::test]
async fn remote_publication_preserves_local_untracked_rows_in_fresh_generation() {
    let (authority, engine, session, old) = fixture().await;
    let storage = engine.storage();
    execute_hydrating(&session, &storage, &old, &authority,
        "INSERT INTO lix_key_value (key,value,lixcol_untracked) VALUES ('private-one','keep-one',true), ('private-two','keep-two',true)",
        &[], &mut Fetches::default()).await.unwrap();
    let controls_before = admitted_controls(&storage, &old).await.unwrap();
    // Never select the private rows before preparation: copying locally owned
    // state cannot depend on whether their content was a successful read scope.
    authority
        .execute(
            "UPDATE lix_key_value SET value='remote' WHERE key='resident'",
            &[],
        )
        .await
        .unwrap();
    let next = Arc::new(
        old.with_descriptor_and_fresh_generations(
            authority.partial_replica_descriptor(None).await.unwrap(),
        )
        .unwrap(),
    );
    let prepared = prepare_hydrating(&engine, &old, next.clone(), &authority).await;
    assert_eq!(
        admitted_controls(&storage, &old).await.unwrap(),
        controls_before
    );
    publish_prepared_partial(engine.clone(), prepared)
        .await
        .unwrap();
    let (reopened, reopened_session) =
        Engine::new_partial_replica(storage.clone(), EngineOptions::new(), &next)
            .await
            .unwrap();
    reopened.sync_mode().admit_partial_replica(
        next.clone(),
        crate::sync::partial_replica_write_capability(),
    );
    for (key, expected) in [("private-one", "keep-one"), ("private-two", "keep-two")] {
        let sql =
            format!("SELECT value FROM lix_key_value WHERE key='{key}' AND lixcol_untracked=true");
        assert!(value(reopened_session.execute(&sql, &[]).await.unwrap()).contains(expected));
        assert!(
            authority
                .execute(&sql, &[])
                .await
                .unwrap()
                .rows()
                .is_empty(),
            "local untracked state must not be uploaded"
        );
    }
    let controls_after = admitted_controls(&storage, &next).await.unwrap();
    assert_ne!(
        controls_before[0].tracked_generation,
        controls_after[0].tracked_generation
    );
}

#[tokio::test]
async fn ambiguous_publication_failure_blocks_direct_reads_and_writes_until_reopen() {
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
    engine.sync_mode().fail_partial_admission(LixError::new(
        LixError::CODE_STORAGE_COMMIT_OUTCOME_UNKNOWN,
        "injected ambiguous publication",
    ));
    for sql in [
        sql,
        "UPDATE lix_key_value SET value='blocked' WHERE key='resident'",
    ] {
        let error = session.execute(sql, &[]).await.unwrap_err();
        assert_eq!(error.code, LixError::CODE_STORAGE_COMMIT_OUTCOME_UNKNOWN);
    }
    // Rebinding the same instance cannot erase its terminal failure.
    engine
        .sync_mode()
        .admit_partial_replica(old.clone(), crate::sync::partial_replica_write_capability());
    assert!(session.execute(sql, &[]).await.is_err());
    let (reopened, fresh) = Engine::new_partial_replica(storage, EngineOptions::new(), &old)
        .await
        .unwrap();
    reopened
        .sync_mode()
        .admit_partial_replica(old, crate::sync::partial_replica_write_capability());
    assert!(value(fresh.execute(sql, &[]).await.unwrap()).contains("before"));
}

#[tokio::test]
async fn prepared_update_remains_local_after_remote_baseline_publication() {
    let (authority, engine, session, old) = fixture().await;
    let storage = engine.storage();
    let update = "UPDATE lix_key_value SET value=$1 WHERE key='resident'";
    let mut fetches = Fetches::default();
    execute_hydrating(
        &session,
        &storage,
        &old,
        &authority,
        update,
        &[Value::Text("prepared-local".into())],
        &mut fetches,
    )
    .await
    .unwrap();
    prepare_baseline_jump_spines(&storage, &old, &authority, &mut fetches)
        .await
        .unwrap();
    for branch in [
        &old.descriptor().global_branch.branch_id,
        &old.descriptor().selected_branch.branch_id,
    ] {
        for wave in 0..4 {
            let remote = &authority;
            let account = old.active_account_id();
            let uploaded = crate::sync::partial_upload_cycle::upload_partial_once(
                &storage,
                &old,
                branch,
                uuid::Uuid::now_v7().to_string(),
                32,
                1024 * 1024,
                |request| async move {
                    remote
                        .push_sync_repository_for_account(&request, account)
                        .await
                },
            )
            .await
            .unwrap();
            if !uploaded {
                break;
            }
            assert!(wave < 3, "fixture upload must reach confirmed head");
        }
    }
    authority
        .execute(
            "UPDATE lix_key_value SET value='remote-next' WHERE key='resident'",
            &[],
        )
        .await
        .unwrap();
    let next = Arc::new(
        old.with_descriptor_and_fresh_generations(
            authority.partial_replica_descriptor(None).await.unwrap(),
        )
        .unwrap(),
    );
    let prepared = prepare_hydrating(&engine, &old, next, &authority).await;
    publish_prepared_partial(engine.clone(), prepared)
        .await
        .unwrap();
    // These are direct session calls: no hydration callback or network exists.
    for value in ["offline-one", "offline-two", "offline-three"] {
        session
            .execute(update, &[Value::Text(value.into())])
            .await
            .unwrap_or_else(|error| {
                panic!("prepared UPDATE lost a dependency after background publication: {error:?}")
            });
        assert!(
            super::value(
                session
                    .execute("SELECT value FROM lix_key_value WHERE key='resident'", &[])
                    .await
                    .unwrap()
            )
            .contains(value)
        );
    }
    assert!(
        value(
            authority
                .execute("SELECT value FROM lix_key_value WHERE key='resident'", &[])
                .await
                .unwrap()
        )
        .contains("remote-next")
    );
}

#[tokio::test]
async fn prepared_publication_cannot_be_submitted_to_a_copied_storage_owner() {
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
    authority
        .execute(
            "UPDATE lix_key_value SET value='remote' WHERE key='resident'",
            &[],
        )
        .await
        .unwrap();
    let next = Arc::new(
        old.with_descriptor_and_fresh_generations(
            authority.partial_replica_descriptor(None).await.unwrap(),
        )
        .unwrap(),
    );
    let prepared = prepare_hydrating(&engine, &old, next, &authority).await;
    let copied = StorageAdapter::new(storage.storage().fork().unwrap());
    let (other, _) = Engine::new_partial_replica(copied.clone(), EngineOptions::new(), &old)
        .await
        .unwrap();
    other
        .sync_mode()
        .admit_partial_replica(old.clone(), crate::sync::partial_replica_write_capability());
    copied.admit_partial_replica_writer(crate::sync::partial_replica_write_capability());
    let before = copied.load_mutation_revision().await.unwrap();
    let error = publish_prepared_partial(Arc::new(other), prepared)
        .await
        .unwrap_err();
    assert_eq!(error.code, LixError::CODE_TRANSACTION_CONFLICT);
    assert!(error.message.contains("another engine/storage owner"));
    assert_eq!(copied.load_mutation_revision().await.unwrap(), before);
    assert_eq!(
        engine.sync_mode().partial_admission().as_deref(),
        Some(old.as_ref())
    );
}

mod faults;

mod recovery;

mod merge_state;

mod merge_adoption;

#[tokio::test]
async fn remote_file_publication_rotates_live_path_index_and_count_cache() {
    let (authority, engine, session, old) = fixture().await;
    let storage = engine.storage();
    let sql = "SELECT path FROM lix_file WHERE path='/appeared.bin'";
    let count = "SELECT COUNT(*) AS n FROM lix_file";
    for query in [sql, count] {
        execute_hydrating(
            &session,
            &storage,
            &old,
            &authority,
            query,
            &[],
            &mut Fetches::default(),
        )
        .await
        .unwrap();
    }
    assert!(session.execute(sql, &[]).await.unwrap().rows().is_empty());
    let before_count = session.execute(count, &[]).await.unwrap().rows()[0]
        .get::<i64>("n")
        .unwrap();
    authority
        .upsert_file_content("/appeared.bin", vec![1, 2, 3])
        .await
        .unwrap();
    let next = Arc::new(
        old.with_descriptor_and_fresh_generations(
            authority.partial_replica_descriptor(None).await.unwrap(),
        )
        .unwrap(),
    );
    let prepared = prepare_hydrating(&engine, &old, next.clone(), &authority).await;
    // Force a warm old index immediately before the durable switch.
    assert!(session.execute(sql, &[]).await.unwrap().rows().is_empty());
    publish_prepared_partial(engine.clone(), prepared)
        .await
        .unwrap();
    assert_eq!(session.execute(sql, &[]).await.unwrap().rows().len(), 1);
    assert_eq!(
        session.execute(count, &[]).await.unwrap().rows()[0]
            .get::<i64>("n")
            .unwrap(),
        before_count + 1
    );
    let (_, reopened) = Engine::new_partial_replica(storage, EngineOptions::new(), &next)
        .await
        .unwrap();
    assert_eq!(reopened.execute(sql, &[]).await.unwrap().rows().len(), 1);
}

#[tokio::test]
async fn remote_global_publication_invalidates_active_account_proof() {
    let custom = uuid::Uuid::now_v7().to_string();
    let (authority, engine, session, old) = fixture_with_account(Some(&custom)).await;
    assert_ne!(old.active_account_id(), crate::SYSTEM_ACCOUNT_ID);
    let storage = engine.storage();
    let sql = "SELECT status FROM lix_account WHERE id=$1";
    let account = old.active_account_id().to_owned();
    let params = [Value::Text(account.clone())];
    execute_hydrating(
        &session,
        &storage,
        &old,
        &authority,
        sql,
        &params,
        &mut Fetches::default(),
    )
    .await
    .unwrap();
    let retained = engine
        .sync_mode()
        .read_interests()
        .unwrap()
        .snapshot()
        .unwrap();
    assert!(
        retained
            .interests
            .iter()
            .any(|interest| match interest.as_ref() {
                crate::hot_state::LogicalReadInterest::Exact {
                    rows,
                    projection,
                    untracked,
                    ..
                } =>
                    *untracked == None
                        && projection
                            .columns
                            .iter()
                            .any(|column| column == "snapshot_content")
                        && rows.iter().any(|row| row.schema_key == "lix_account"
                            && row.row_pk
                                == crate::row_pk::RowPk::uuid_from_canonical(&account).unwrap()),
                _ => false,
            }),
        "covered account SQL must retain its exact snapshot recipe: {:?}",
        retained.interests
    );
    // Install the disposable token/proof that a previous successful native
    // account mutation and commit validation leave behind. No native rows or
    // coverage are fabricated: the active account was just loaded by SQL.
    let mut writes = storage.new_write_set();
    crate::account::stage_account_revision(&mut writes);
    storage
        .commit_partial_replica_write_set(
            crate::sync::partial_replica_write_capability(),
            writes,
            StorageWriteOptions {
                await_durable: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let read = storage.begin_read(Default::default()).await.unwrap();
    let previous_token = crate::account::load_account_revision(&read)
        .await
        .unwrap()
        .unwrap();
    drop(read);
    crate::account::record_account_proven_active(Some(&previous_token), &account);
    assert!(crate::account::account_proven_active(
        Some(&previous_token),
        &account
    ));
    authority
        .execute(
            "UPDATE lix_account SET status='disabled' WHERE id=$1",
            &params,
        )
        .await
        .unwrap();
    let next = Arc::new(
        old.with_descriptor_and_fresh_generations(
            authority.partial_replica_descriptor(None).await.unwrap(),
        )
        .unwrap(),
    );
    assert_ne!(
        old.descriptor().global_branch.head,
        next.descriptor().global_branch.head
    );
    let prepared = prepare_hydrating(&engine, &old, next.clone(), &authority).await;
    publish_prepared_partial(engine.clone(), prepared)
        .await
        .unwrap();
    let read = storage.begin_read(Default::default()).await.unwrap();
    let token = crate::account::load_account_revision(&read)
        .await
        .unwrap()
        .unwrap();
    drop(read);
    assert_ne!(token, previous_token);
    assert!(!crate::account::account_proven_active(
        Some(&token),
        &account
    ));
    let read = storage.begin_read(Default::default()).await.unwrap();
    let revision_before_read = crate::storage_adapter::load_repository_mutation_revision(&read)
        .await
        .unwrap();
    let controls_before_read = crate::branch::BranchHeadControlContext::new()
        .reader(&read)
        .load_observed(&[
            next.descriptor().selected_branch.branch_id.clone(),
            crate::GLOBAL_BRANCH_ID.to_owned(),
        ])
        .await
        .unwrap()
        .into_iter()
        .map(|control| control.raw_token)
        .collect::<Vec<_>>();
    drop(read);
    // Exact previously covered SQL must remain local after publication,
    // including catalog inputs invalidated by the new global head.
    assert_eq!(
        session.execute(sql, &params).await.unwrap().rows()[0]
            .get::<String>("status")
            .unwrap(),
        "disabled"
    );
    let read = storage.begin_read(Default::default()).await.unwrap();
    assert_eq!(
        crate::storage_adapter::load_repository_mutation_revision(&read)
            .await
            .unwrap(),
        revision_before_read,
        "covered SELECT must not publish an implicit base-refresh write"
    );
    assert_eq!(
        crate::branch::BranchHeadControlContext::new()
            .reader(&read)
            .load_observed(&[
                next.descriptor().selected_branch.branch_id.clone(),
                crate::GLOBAL_BRANCH_ID.to_owned()
            ])
            .await
            .unwrap()
            .into_iter()
            .map(|control| control.raw_token)
            .collect::<Vec<_>>(),
        controls_before_read
    );
    drop(read);
    let error = execute_hydrating(
        &session,
        &storage,
        &next,
        &authority,
        "INSERT INTO lix_key_value(key,value) VALUES('disabled-proof-write','forbidden')",
        &[],
        &mut Fetches::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "LIX_ACCOUNT_DISABLED");
}

#[tokio::test]
async fn remote_global_publication_keeps_global_session_sql_and_catalog_warm() {
    let account = uuid::Uuid::now_v7().to_string();
    let (authority, engine, _selected, old) = fixture_with_account(Some(&account)).await;
    let storage = engine.storage();
    let global = engine
        .open_session_at_with_account(crate::GLOBAL_BRANCH_ID, old.active_account_id())
        .await
        .unwrap();
    let sql = "SELECT name FROM lix_account WHERE id=$1";
    let params = [Value::Text(old.active_account_id().to_owned())];
    let before = execute_hydrating(
        &global,
        &storage,
        &old,
        &authority,
        sql,
        &params,
        &mut Fetches::default(),
    )
    .await
    .unwrap();
    assert_eq!(before.rows().len(), 1);
    let retained = engine
        .sync_mode()
        .read_interests()
        .unwrap()
        .snapshot()
        .unwrap();
    assert!(
        retained
            .interests
            .iter()
            .any(|interest| match interest.as_ref() {
                crate::hot_state::LogicalReadInterest::Exact {
                    rows,
                    projection,
                    untracked,
                    ..
                } =>
                    *untracked == None
                        && projection
                            .columns
                            .iter()
                            .any(|column| column == "snapshot_content")
                        && rows.iter().any(|row| row.schema_key == "lix_account"
                            && row.row_pk
                                == crate::row_pk::RowPk::uuid_from_canonical(&account).unwrap()),
                _ => false,
            }),
        "covered account SQL must retain its exact snapshot recipe: {:?}",
        retained.interests
    );
    authority
        .execute(
            "UPDATE lix_account SET name='global-domain-after' WHERE id=$1",
            &params,
        )
        .await
        .unwrap();
    let next = Arc::new(
        old.with_descriptor_and_fresh_generations(
            authority.partial_replica_descriptor(None).await.unwrap(),
        )
        .unwrap(),
    );
    let prepared = prepare_hydrating(&engine, &old, next.clone(), &authority).await;
    publish_prepared_partial(engine.clone(), prepared)
        .await
        .unwrap();
    let read = storage.begin_read(Default::default()).await.unwrap();
    let revision_before_read = crate::storage_adapter::load_repository_mutation_revision(&read)
        .await
        .unwrap();
    let controls_before_read = crate::branch::BranchHeadControlContext::new()
        .reader(&read)
        .load_observed(&[
            next.descriptor().selected_branch.branch_id.clone(),
            crate::GLOBAL_BRANCH_ID.to_owned(),
        ])
        .await
        .unwrap()
        .into_iter()
        .map(|control| control.raw_token)
        .collect::<Vec<_>>();
    drop(read);
    // This bare session has no retry worker: any native catalog or row miss
    // fails the assertion instead of being hidden by foreground hydration.
    let after = global.execute(sql, &params).await.unwrap();
    assert_eq!(
        after.rows()[0].get::<String>("name").unwrap(),
        "global-domain-after"
    );
    let reopened = engine
        .open_session_at_with_account(crate::GLOBAL_BRANCH_ID, old.active_account_id())
        .await
        .unwrap();
    assert_eq!(
        reopened.execute(sql, &params).await.unwrap().rows()[0]
            .get::<String>("name")
            .unwrap(),
        "global-domain-after"
    );
    let read = storage.begin_read(Default::default()).await.unwrap();
    assert_eq!(
        crate::storage_adapter::load_repository_mutation_revision(&read)
            .await
            .unwrap(),
        revision_before_read,
        "covered SELECT must not publish an implicit base-refresh write"
    );
    assert_eq!(
        crate::branch::BranchHeadControlContext::new()
            .reader(&read)
            .load_observed(&[
                next.descriptor().selected_branch.branch_id.clone(),
                crate::GLOBAL_BRANCH_ID.to_owned()
            ])
            .await
            .unwrap()
            .into_iter()
            .map(|control| control.raw_token)
            .collect::<Vec<_>>(),
        controls_before_read
    );
    drop(read);
    // An actual authored mutation, unlike the covered read, advances selected
    // history and records the coherently published global base.
    execute_hydrating(
        &_selected,
        &storage,
        &next,
        &authority,
        "UPDATE lix_key_value SET value='after-global' WHERE key='resident'",
        &[],
        &mut Fetches::default(),
    )
    .await
    .unwrap();
    let read = storage.begin_read(Default::default()).await.unwrap();
    let head = crate::branch::BranchHeadControlContext::new()
        .reader(&read)
        .load(&next.descriptor().selected_branch.branch_id)
        .await
        .unwrap()
        .unwrap()
        .head_commit_id;
    let node = crate::commit_graph::CommitGraphContext::new()
        .reader(&read)
        .load_node(&head)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        node.base_commit_id.unwrap().to_string(),
        next.descriptor().global_branch.head.commit_id
    );
    assert_eq!(
        node.parent_commit_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        vec![next.descriptor().selected_branch.head.commit_id.clone()]
    );
    drop(read);
    reopened.close().await.unwrap();
    global.close().await.unwrap();
}
