//! SQL integration starting from only a partial descriptor and native demands.
//! No traced HOT state, raw arbitrary-key transfer or full replica bootstrap.

use std::collections::BTreeSet;
use std::time::Instant;

use crate::changelog::ChangelogReader as _;
use crate::engine::{Engine, EngineOptions};
use crate::session::SessionContext;
use crate::storage_adapter::{StorageAdapter, StorageWriteOptions};
use crate::tracked_state::{NativeMetadataRef, NativeObjectRef};
use crate::{ExecuteResult, Lix, LixError, Memory, Value, open_lix};

use super::native_metadata::{NativeMetadataRequest, stage_native_metadata};
use super::partial_bootstrap::stage_partial_bootstrap;
use super::partial_hydration::hydrate_native_object;
use super::partial_state::PartialReplicaState;

#[derive(Default, Debug)]
struct Fetches {
    object_requests: usize,
    metadata_requests: usize,
    payload_bytes: usize,
}

async fn admitted_controls<S: crate::storage_adapter::Storage + Clone + Send + Sync + 'static>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
) -> Result<Vec<crate::branch::BranchHeadControl>, LixError> {
    let read = storage.begin_read(Default::default()).await?;
    let mut controls = Vec::new();
    for branch in [
        &state.descriptor().selected_branch,
        &state.descriptor().global_branch,
    ] {
        controls.push(
            crate::branch::BranchHeadControlContext::new()
                .reader(&read)
                .load(&branch.branch_id)
                .await?
                .expect("admitted partial branch control remains present"),
        );
    }
    Ok(controls)
}

async fn hydrate_metadata<S: crate::storage_adapter::Storage + Clone + Send + Sync + 'static>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    authority: &Lix<Memory>,
    address: NativeMetadataRef,
    fetches: &mut Fetches,
) -> Result<(), LixError> {
    let request = NativeMetadataRequest {
        epoch_id: state.epoch_id().to_owned(),
        objects: vec![address],
    };
    let response = authority.read_sync_native_metadata(&request).await?;
    let wire = serde_json::to_vec(&response).unwrap();
    let response = serde_json::from_slice(&wire).unwrap();
    let read = storage.begin_read(Default::default()).await?;
    let mut writes = storage.new_write_set();
    let preconditions =
        stage_native_metadata(&read, &mut writes, state, &request, &response).await?;
    drop(read);
    storage
        .commit_partial_replica_write_set(
            super::partial_replica_write_capability(),
            writes,
            StorageWriteOptions {
                preconditions,
                await_durable: true,
                ..Default::default()
            },
        )
        .await?;
    fetches.metadata_requests += 1;
    fetches.payload_bytes += response
        .objects
        .iter()
        .map(|object| object.bytes.len())
        .sum::<usize>();
    Ok(())
}

/// Retain only the immutable jump spine rooted in the declared baseline.
/// The Myers recurrence keeps every future jump in this set plus locally
/// authored descendants; preparation never follows ordinary parent ancestry.
async fn prepare_baseline_jump_spines(
    storage: &StorageAdapter<Memory>,
    state: &PartialReplicaState,
    authority: &Lix<Memory>,
    fetches: &mut Fetches,
) -> Result<(), LixError> {
    let mut prepared = BTreeSet::new();
    for branch in [
        &state.descriptor().selected_branch,
        &state.descriptor().global_branch,
    ] {
        let mut cursor = crate::changelog::CommitId::parse(&branch.head.commit_id)
            .map_err(|error| LixError::unknown(error.to_string()))?;
        let mut previous: Option<crate::changelog::CommitRecord> = None;
        loop {
            let read = storage.begin_read(Default::default()).await?;
            let ids = [cursor];
            let record = crate::changelog::ChangelogContext::new()
                .reader(&read)
                .load_commits(crate::changelog::CommitLoadRequest { commit_ids: &ids })
                .await?
                .into_iter()
                .next()
                .unwrap()
                .1;
            drop(read);
            let record = match record {
                Some(record) => record,
                None => {
                    hydrate_metadata(
                        storage,
                        state,
                        authority,
                        NativeMetadataRef::CommitGraphRecord(cursor.to_string()),
                        fetches,
                    )
                    .await?;
                    let read = storage.begin_read(Default::default()).await?;
                    crate::changelog::ChangelogContext::new()
                        .reader(&read)
                        .load_commits(crate::changelog::CommitLoadRequest { commit_ids: &ids })
                        .await?
                        .into_iter()
                        .next()
                        .unwrap()
                        .1
                        .expect("native metadata installation supplies requested graph record")
                }
            };
            if let Some(previous) = previous.as_ref() {
                assert_eq!(
                    previous.generation.checked_sub(record.generation),
                    Some(previous.first_parent_jump_span)
                );
            }
            if !prepared.insert(cursor) {
                break;
            }
            if record.first_parent_jump_commit_id == cursor {
                assert_eq!(record.first_parent_jump_span, 0);
                break;
            }
            cursor = record.first_parent_jump_commit_id;
            previous = Some(record);
        }
    }
    eprintln!(
        "partial SQL prepared fixed Myers spine headers={}",
        prepared.len()
    );
    Ok(())
}

async fn execute_hydrating<S: crate::storage_adapter::Storage + Clone + Send + Sync + 'static>(
    session: &SessionContext<S>,
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    authority: &Lix<Memory>,
    sql: &str,
    params: &[Value],
    fetches: &mut Fetches,
) -> Result<ExecuteResult, LixError> {
    let mut seen = BTreeSet::new();
    for _ in 0..512 {
        let before = admitted_controls(storage, state).await?;
        let error = match session.execute(sql, params).await {
            Ok(result) => return Ok(result),
            Err(error) => error,
        };
        assert_eq!(
            admitted_controls(storage, state).await?,
            before,
            "a failed statement must not publish state before demand retry: {error}"
        );
        if let Some(address) = NativeObjectRef::from_missing_error(&error)? {
            if !seen.insert(format!("object:{address:?}")) {
                return Err(LixError::new(
                    "LIX_PARTIAL_SQL_NO_PROGRESS",
                    format!("object hydration did not resolve {address:?}: {error}"),
                ));
            }
            eprintln!("partial SQL hydrate object {address:?}");
            let report = hydrate_native_object(
                storage,
                state,
                address,
                32 * 1024 * 1024,
                |request| async move {
                    let response = authority.read_sync_native_object_range(&request).await?;
                    // Exercise the actual bounded response's base64 wire format.
                    let wire = serde_json::to_vec(&response).unwrap();
                    Ok(serde_json::from_slice(&wire).unwrap())
                },
            )
            .await?;
            fetches.object_requests += report.requests;
            fetches.payload_bytes += report.payload_bytes;
            continue;
        }
        if let Some(address) = NativeMetadataRef::from_missing_error(&error)? {
            if !seen.insert(format!("metadata:{address:?}")) {
                return Err(LixError::new(
                    "LIX_PARTIAL_SQL_NO_PROGRESS",
                    format!("metadata hydration did not resolve {address:?}: {error}"),
                ));
            }
            eprintln!("partial SQL hydrate metadata {address:?}");
            hydrate_metadata(storage, state, authority, address, fetches).await?;
            continue;
        }
        eprintln!(
            "partial SQL UNHANDLED code={} message={} details={:?}",
            error.code, error.message, error.details
        );
        return Err(error);
    }
    Err(LixError::new(
        "LIX_PARTIAL_SQL_DEMAND_LIMIT",
        "SQL exceeded 512 explicit native dependency demands",
    ))
}

fn value(result: ExecuteResult) -> String {
    assert_eq!(
        result.rows().len(),
        1,
        "cold SQL must not report an unknown native row as absent"
    );
    match result.rows()[0].get::<Value>("value").unwrap() {
        Value::Jsonb(value) => value.as_json_string().unwrap(),
        Value::Text(value) => value,
        value => panic!("unexpected SQL value: {value:?}"),
    }
}

#[tokio::test]
#[ignore = "manual SQL-driven partial native hydration integration gate"]
async fn descriptor_only_sql_hydrates_then_reads_and_writes_offline() {
    for width in [16usize, 1600] {
        let authority = open_lix().await.unwrap();
        let values = (0..width)
            .map(|index| format!("('partial-demand-{index:06}', 'before')"))
            .collect::<Vec<_>>()
            .join(",");
        authority
            .execute(
                &format!("INSERT INTO lix_key_value (key, value) VALUES {values}"),
                &[],
            )
            .await
            .unwrap();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let descriptor = serde_json::from_slice(&serde_json::to_vec(&descriptor).unwrap()).unwrap();
        let state = PartialReplicaState::new(
            format!("https://example.test/lix/{}", authority.lix_id()),
            crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
            "00000000-0000-7000-8000-000000000399".to_owned(),
            descriptor,
        )
        .unwrap();
        let memory = Memory::new();
        let storage = StorageAdapter::new(memory.clone());
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
        let opened = Instant::now();
        let (engine, session) =
            Engine::new_partial_replica(storage.clone(), EngineOptions::new(), &state)
                .await
                .unwrap();
        engine.sync_mode().admit_partial_replica(
            std::sync::Arc::new(state.clone()),
            super::partial_replica_write_capability(),
        );
        storage.admit_partial_replica_writer(super::partial_replica_write_capability());
        let open_us = opened.elapsed().as_micros();
        let sql = "SELECT value FROM lix_key_value WHERE key = $1";
        let params = [Value::Text("partial-demand-000000".to_owned())];
        let mut fetches = Fetches::default();
        let cold = Instant::now();
        let result = execute_hydrating(
            &session,
            &storage,
            &state,
            &authority,
            sql,
            &params,
            &mut fetches,
        )
        .await
        .unwrap_or_else(|error| panic!("descriptor-only cold SQL blocked width={width}: {error}"));
        assert_eq!(value(result), "before");
        eprintln!(
            "partial SQL cold width={width} open_us={open_us} cold_us={} fetches={fetches:?}",
            cold.elapsed().as_micros()
        );
        // Direct session calls below have no demand callback or network path.
        for _ in 0..3 {
            assert_eq!(
                value(session.execute(sql, &params).await.unwrap()),
                "before"
            );
        }
        // A first read does not promise to prepare every validation/publication
        // dependency of an UPDATE. Prepare this workload with one real cold
        // mutation, then retain the fixed remote jump spine for future appends.
        let mut write_fetches = Fetches::default();
        let preparation = Instant::now();
        execute_hydrating(
            &session,
            &storage,
            &state,
            &authority,
            "UPDATE lix_key_value SET value = $2 WHERE key = $1",
            &[params[0].clone(), Value::Text("prepared".to_owned())],
            &mut write_fetches,
        )
        .await
        .unwrap_or_else(|error| panic!("cold write preparation blocked width={width}: {error}"));
        eprintln!(
            "partial SQL cold write preparation width={width} elapsed_us={} fetches={write_fetches:?}",
            preparation.elapsed().as_micros()
        );
        let mut spine_fetches = Fetches::default();
        let spine_started = Instant::now();
        prepare_baseline_jump_spines(&storage, &state, &authority, &mut spine_fetches)
            .await
            .unwrap();
        eprintln!(
            "partial SQL spine preparation width={width} elapsed_us={} fetches={spine_fetches:?}",
            spine_started.elapsed().as_micros()
        );
        const WARM_ITERATIONS: usize = 30;
        let mut partial_update_us = Vec::new();
        let mut partial_read_us = Vec::new();
        let mut partial_update_root = crate::hot_state::root_exact_profile::Profile::default();
        let mut partial_read_root = crate::hot_state::root_exact_profile::Profile::default();
        for index in 0..WARM_ITERATIONS {
            let expected = format!("after-{}", index % 3);
            crate::hot_state::root_exact_profile::begin();
            let start = Instant::now();
            session.execute("UPDATE lix_key_value SET value = $2 WHERE key = $1", &[params[0].clone(), Value::Text(expected.clone())]).await
                .unwrap_or_else(|error| panic!("resident offline SQL update blocked width={width} update={index}: {error}, details={:?}", error.details));
            partial_update_us.push(start.elapsed().as_micros());
            partial_update_root.add(crate::hot_state::root_exact_profile::take());
            crate::hot_state::root_exact_profile::begin();
            let start = Instant::now();
            assert_eq!(
                value(session.execute(sql, &params).await.unwrap()),
                expected
            );
            partial_read_us.push(start.elapsed().as_micros());
            partial_read_root.add(crate::hot_state::root_exact_profile::take());
        }
        // Same binary, fixture and SQL. Root I/O attribution is test-only and
        // includes adapter-returned local bytes, not wire bytes or OS CPU time.
        authority.execute(sql, &params).await.unwrap();
        let mut full_update_us = Vec::new();
        let mut full_read_us = Vec::new();
        let mut full_update_root = crate::hot_state::root_exact_profile::Profile::default();
        let mut full_read_root = crate::hot_state::root_exact_profile::Profile::default();
        for index in 0..WARM_ITERATIONS {
            let expected = format!("authority-{}", index % 3);
            crate::hot_state::root_exact_profile::begin();
            let start = Instant::now();
            authority
                .execute(
                    "UPDATE lix_key_value SET value = $2 WHERE key = $1",
                    &[params[0].clone(), Value::Text(expected.clone())],
                )
                .await
                .unwrap();
            full_update_us.push(start.elapsed().as_micros());
            full_update_root.add(crate::hot_state::root_exact_profile::take());
            crate::hot_state::root_exact_profile::begin();
            let start = Instant::now();
            assert_eq!(
                value(authority.execute(sql, &params).await.unwrap()),
                expected
            );
            full_read_us.push(start.elapsed().as_micros());
            full_read_root.add(crate::hot_state::root_exact_profile::take());
        }
        let native_read_batches = partial_read_root
            .spaces
            .iter()
            .filter(|(name, _)| name.starts_with("tracked_state."))
            .map(|(_, space)| space.batches)
            .sum::<u64>();
        assert!(
            native_read_batches < WARM_ITERATIONS as u64,
            "warm exact serving must reuse immutable root candidates; profile={partial_read_root:?}"
        );
        eprintln!(
            "PARTIAL_WARM_SQL_PROFILE {}",
            serde_json::json!({
                "width": width, "iterations": WARM_ITERATIONS,
                "partial_update_us": partial_update_us, "partial_read_us": partial_read_us,
                "full_update_us": full_update_us, "full_read_us": full_read_us,
                "partial_update_root": partial_update_root, "partial_read_root": partial_read_root,
                "full_update_root": full_update_root, "full_read_root": full_read_root,
            })
        );
        if std::env::var_os("LIX_PARTIAL_PHASE_PROFILE").is_some() {
            let (_, partial_phases) = phase_profile::capture(async {
                for index in 0..30 {
                    session
                        .execute(
                            "UPDATE lix_key_value SET value = $2 WHERE key = $1",
                            &[
                                params[0].clone(),
                                Value::Text(format!("after-{}", index % 3)),
                            ],
                        )
                        .await
                        .unwrap();
                }
            })
            .await;
            let (_, full_phases) = phase_profile::capture(async {
                for index in 0..30 {
                    authority
                        .execute(
                            "UPDATE lix_key_value SET value = $2 WHERE key = $1",
                            &[
                                params[0].clone(),
                                Value::Text(format!("authority-{}", index % 3)),
                            ],
                        )
                        .await
                        .unwrap();
                }
            })
            .await;
            eprintln!(
                "PARTIAL_WARM_WRITE_PHASES {}",
                serde_json::json!({"width":width,"iterations":30,"partial":partial_phases,"full":full_phases})
            );
        }
        session.close().await.unwrap();
        drop(session);
        drop(engine);
        drop(storage);
        let reopened_storage = StorageAdapter::new(memory);
        let (reopened_engine, reopened) =
            Engine::new_partial_replica(reopened_storage.clone(), EngineOptions::new(), &state)
                .await
                .unwrap();
        reopened_engine.sync_mode().admit_partial_replica(
            std::sync::Arc::new(state.clone()),
            super::partial_replica_write_capability(),
        );
        reopened_storage.admit_partial_replica_writer(super::partial_replica_write_capability());
        assert_eq!(
            value(reopened.execute(sql, &params).await.unwrap()),
            "after-2"
        );
        // Rotate only admission epoch, retaining the same heads and native
        // bytes. An old session must not acquire the new writer identity from
        // storage implicitly or publish a mutation under its stale binding.
        let read = reopened_storage
            .begin_read(Default::default())
            .await
            .unwrap();
        let before_control = crate::branch::BranchHeadControlContext::new()
            .reader(&read)
            .load(&state.descriptor().selected_branch.branch_id)
            .await
            .unwrap()
            .unwrap();
        let (_, prior_receipt) = super::partial_state::load_partial_replica_state(&read)
            .await
            .unwrap()
            .unwrap();
        let replacement = PartialReplicaState::new(
            state.remote_id().to_owned(),
            state.active_account_id().to_owned(),
            "00000000-0000-7000-8000-000000000499".to_owned(),
            state.descriptor().clone(),
        )
        .unwrap();
        let mut writes = reopened_storage.new_write_set();
        let guard = super::partial_state::stage_partial_replica_state(
            &mut writes,
            &replacement,
            Some(prior_receipt),
        )
        .unwrap();
        drop(read);
        reopened_storage
            .commit_partial_replica_write_set(
                super::partial_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    preconditions: vec![guard],
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let error = reopened
            .execute(
                "UPDATE lix_key_value SET value = $2 WHERE key = $1",
                &[params[0].clone(), Value::Text("stale-write".to_owned())],
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH");
        let read = reopened_storage
            .begin_read(Default::default())
            .await
            .unwrap();
        let after_control = crate::branch::BranchHeadControlContext::new()
            .reader(&read)
            .load(&state.descriptor().selected_branch.branch_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            after_control, before_control,
            "stale epoch rejection must not publish a local commit"
        );
        drop(read);
        assert_eq!(
            value(reopened.execute(sql, &params).await.unwrap()),
            "after-2"
        );
        reopened.close().await.unwrap();
    }
}

#[path = "partial_file_sql_tests.rs"]
mod file_content;

#[path = "partial_upload_sql_tests.rs"]
mod upload_recovery;

mod checkpoint;

#[path = "partial_sql_phase_profile.rs"]
mod phase_profile;

#[path = "partial_worker_sql_tests.rs"]
mod worker;

#[tokio::test]
async fn partial_catalog_revision_invalidates_negative_and_amended_schema_cache() {
    let authority = open_lix().await.unwrap();
    let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
    let descriptor = serde_json::from_slice(&serde_json::to_vec(&descriptor).unwrap()).unwrap();
    let state = PartialReplicaState::new(
        format!("https://example.test/lix/{}", authority.lix_id()),
        crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
        "00000000-0000-7000-8000-000000000399".to_owned(),
        descriptor,
    )
    .unwrap();
    let memory = Memory::new();
    let storage = StorageAdapter::new(memory.clone());
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
        std::sync::Arc::new(state.clone()),
        super::partial_replica_write_capability(),
    );
    storage.admit_partial_replica_writer(super::partial_replica_write_capability());

    let revision = async || {
        let read = storage.begin_read(Default::default()).await.unwrap();
        crate::catalog::load_catalog_revision(&read)
            .await
            .unwrap()
            .unwrap()
    };
    let initial = revision().await;
    let mut fetches = Fetches::default();
    // A failed cold compilation cannot cache its incomplete schema scan.
    assert!(
        session
            .execute("SELECT id FROM partial_catalog_probe", &[])
            .await
            .is_err()
    );
    execute_hydrating(
        &session,
        &storage,
        &state,
        &authority,
        "SELECT key FROM lix_key_value WHERE key = 'missing'",
        &[],
        &mut fetches,
    )
    .await
    .unwrap();
    // Resident native graph/delta subsets must never masquerade as complete
    // public inventories, including COUNT and point-negative queries.
    for sql in [
        "SELECT COUNT(*) FROM lix_commit",
        "SELECT COUNT(*) FROM lix_change",
        "SELECT * FROM lix_commit WHERE id = '00000000-0000-7000-8000-000000099999'",
    ] {
        let error = session
            .execute(sql, &[])
            .await
            .expect_err("partial inventory must fail closed");
        assert_eq!(
            error.code, "LIX_PARTIAL_REPLICA_SCOPE_UNSUPPORTED",
            "{sql}: {error}"
        );
    }
    session
        .execute("SELECT key FROM lix_key_value WHERE key = 'missing'", &[])
        .await
        .unwrap();
    // Warm the transaction-opening catalog, including absence of this surface.
    execute_hydrating(
        &session,
        &storage,
        &state,
        &authority,
        "UPDATE lix_key_value SET value = 'none' WHERE key = 'missing'",
        &[],
        &mut fetches,
    )
    .await
    .unwrap();
    assert_eq!(revision().await, initial);
    assert!(
        session
            .execute("SELECT id FROM partial_catalog_probe", &[])
            .await
            .is_err()
    );
    let schema = serde_json::json!({
        "$schema":"https://lix.dev/schema-v1.json", "key":"partial_catalog_probe",
        "columns":[{"name":"id","type":"text","nullable":false}], "primary_key":["id"]
    });
    execute_hydrating(&session, &storage, &state, &authority,
        "INSERT INTO lix_registered_schema (schema_key, value, lixcol_global, lixcol_untracked) VALUES ($1 ->> 'key', $1, false, true)",
        &[Value::Jsonb(schema.clone().into())], &mut fetches).await.unwrap();
    let registered = revision().await;
    assert_ne!(registered, initial);
    session
        .execute(
            "INSERT INTO partial_catalog_probe (id, lixcol_untracked) VALUES ('local', true)",
            &[],
        )
        .await
        .unwrap();
    session
        .execute("SELECT id FROM partial_catalog_probe", &[])
        .await
        .unwrap();
    assert_eq!(revision().await, registered);
    assert!(
        session
            .execute("SELECT title FROM partial_catalog_probe", &[])
            .await
            .is_err()
    );
    let mut amended = schema;
    amended["columns"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({"name":"title","type":"text","nullable":true}));
    session.execute("UPDATE lix_registered_schema SET value = $1 WHERE schema_key = 'partial_catalog_probe' AND lixcol_untracked = true",
        &[Value::Jsonb(amended.into())]).await.unwrap();
    assert_ne!(revision().await, registered);
    session.execute("UPDATE partial_catalog_probe SET title = 'fresh' WHERE id = 'local' AND lixcol_untracked = true", &[]).await.unwrap();
    session
        .execute(
            "SELECT title FROM partial_catalog_probe WHERE id = 'local'",
            &[],
        )
        .await
        .unwrap();
    // A same-epoch baseline receipt change must not authorize an already
    // admitted engine implicitly. Simulate another publisher's receipt CAS.
    let read = storage.begin_read(Default::default()).await.unwrap();
    let (_, raw) = super::partial_state::load_partial_replica_state(&read)
        .await
        .unwrap()
        .unwrap();
    let mut descriptor = state.descriptor().clone();
    descriptor.selected_branch.ref_change_id = "00000000-0000-7000-8000-000000000599".into();
    let replacement = PartialReplicaState::new(
        state.remote_id().into(),
        state.active_account_id().into(),
        state.epoch_id().into(),
        descriptor,
    )
    .unwrap();
    let mut writes = storage.new_write_set();
    let guard =
        super::partial_state::stage_partial_replica_state(&mut writes, &replacement, Some(raw))
            .unwrap();
    drop(read);
    storage
        .commit_partial_replica_write_set(
            super::partial_replica_write_capability(),
            writes,
            StorageWriteOptions {
                preconditions: vec![guard],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let before = admitted_controls(&storage, &replacement).await.unwrap();
    let error = session.execute("UPDATE partial_catalog_probe SET title = 'stale' WHERE id = 'local' AND lixcol_untracked = true", &[]).await.unwrap_err();
    assert_eq!(error.code, "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH");
    assert_eq!(
        admitted_controls(&storage, &replacement).await.unwrap(),
        before
    );
    session.close().await.unwrap();
}

mod publication;

#[cfg(feature = "server-protocol")]
mod runtime_http;

#[cfg(feature = "server-protocol")]
mod global_runtime_http;
