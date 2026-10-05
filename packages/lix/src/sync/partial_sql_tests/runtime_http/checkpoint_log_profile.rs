//! Profile Atelier's complete checkpoint timeline query against a realistic
//! file/checkpoint history and verify its active-anchor read recipe.
use super::file_open_probe::TimedClient;
use super::*;

async fn execute_hydrating(
    session: &SessionContext<Memory>,
    storage: &StorageAdapter<Memory>,
    state: &PartialReplicaState,
    transport: &HttpSyncTransport<TimedClient>,
    sql: &str,
    params: &[Value],
) -> Result<ExecuteResult, LixError> {
    let mut seen = BTreeSet::new();
    for _ in 0..256 {
        match session.execute(sql, params).await {
            Ok(result) => return Ok(result),
            Err(error) => {
                let Some(demand) =
                    crate::sync::runtime::native_sync_demand_request_for_error(&error)?
                else {
                    return Err(error);
                };
                if !seen.insert(format!("{demand:?}")) {
                    return Err(error);
                }
                crate::sync::partial_runtime::hydrate_demand(storage, state, transport, demand)
                    .await?;
            }
        }
    }
    Err(LixError::unknown(
        "checkpoint profile hydration exceeded retry cap",
    ))
}

fn physical_fallback_calls(requests: &[serde_json::Value]) -> usize {
    requests
        .iter()
        .filter(|request| {
            matches!(
                request["operation"].as_str(),
                Some("native-objects" | "native-object-range" | "native-metadata")
            )
        })
        .count()
}

#[tokio::test]
async fn checkpoint_log_retirement_interest_profile() {
    const DEPTH: usize = 30;
    // The current Atelier preview asks for all visible rows plus one page;
    // 40 models three loaded pages while keeping the engine window bounded.
    let query_prefix = "SELECT commit_id, parent_commit_id, created_at FROM lix_log() WHERE is_checkpoint ORDER BY position";
    let sql = format!("{query_prefix} LIMIT 40");
    let backing = Memory::new();
    let authority = open_lix().with_storage(backing.clone()).await.unwrap();
    // Seed a real undo-state owner outside the 30 checkpoint rows being
    // profiled. The old checkpoint is retired through the public undo API,
    // so its durable state proves the schema is populated while the timeline
    // under test remains 30 visible checkpoints with negative retirement
    // lookups for every selected identity.
    authority
        .execute(
            "INSERT INTO lix_file(path, content) VALUES ('/checkpoint-profile/retired.txt', $1)",
            &[Value::Blob(b"retired seed".to_vec().into())],
        )
        .await
        .unwrap();
    let retired_checkpoint = authority.create_checkpoint().await.unwrap().commit_id;
    authority
        .execute(
            "SELECT commit_id FROM lix_undo($1)",
            &[Value::Text(retired_checkpoint.clone())],
        )
        .await
        .unwrap();
    for index in 0..DEPTH {
        authority
            .execute(
                "INSERT INTO lix_file(path, content) VALUES ($1, $2)",
                &[
                    Value::Text(format!("/checkpoint-profile/{index}.txt")),
                    Value::Blob(vec![index as u8; 128].into()),
                ],
            )
            .await
            .unwrap();
        authority.create_checkpoint().await.unwrap();
    }
    let expected = authority.execute(&sql, &[]).await.unwrap();
    assert_eq!(
        expected.rows().len(),
        DEPTH,
        "fixture must create real file checkpoints"
    );
    let checkpoint_count = authority
        .execute(
            "SELECT count(*) AS count FROM lix_log() WHERE is_checkpoint",
            &[],
        )
        .await
        .unwrap()
        .rows()[0]
        .get::<i64>("count")
        .unwrap();
    assert_eq!(checkpoint_count, DEPTH as i64);
    let expected_ids = expected
        .rows()
        .iter()
        .map(|row| row.get::<String>("commit_id").unwrap())
        .collect::<BTreeSet<_>>();
    let authority_head = authority
        .execute("SELECT lix_active_branch_commit_id() AS id", &[])
        .await
        .unwrap()
        .rows()[0]
        .get::<String>("id")
        .unwrap();
    let explicit_sql = sql.replacen("lix_log()", &format!("lix_log('{authority_head}')"), 1);
    let authority_storage = authority.storage_adapter();
    let authority_read = authority_storage.begin_read(Default::default()).await.unwrap();
    let retired_key = crate::tracked_state::TrackedStateKey {
        schema_key: crate::undo_redo::UNDO_STATE_SCHEMA_KEY.into(),
        file_id: None,
        row_pk: crate::row_pk::RowPk::uuid_from_canonical(&retired_checkpoint).unwrap(),
    };
    let mut retired_state = crate::tracked_state::TrackedStateContext::new().reader(&authority_read);
    let retired_rows = retired_state
        .load_projected_batch_at_commit(
            &authority_head,
            std::slice::from_ref(&retired_key),
            &crate::changelog::ChangeRecordProjection::from_columns(&["snapshot_content".into()]),
        )
        .await
        .unwrap();
    assert!(
        retired_rows.row(0).is_some(),
        "a real retired checkpoint must leave an UNDO_STATE owner in the graph"
    );
    let retired_log = authority
        .execute(
            "SELECT is_checkpoint FROM lix_log() WHERE commit_id = $1",
            &[Value::Text(retired_checkpoint)],
        )
        .await
        .unwrap();
    assert!(
        retired_log.is_empty(),
        "the unrelated seed checkpoint must stay hidden from the 30 visible rows"
    );
    let retirement_keys = expected_ids
        .iter()
        .map(|commit_id| crate::tracked_state::TrackedStateKey {
            schema_key: crate::undo_redo::UNDO_STATE_SCHEMA_KEY.into(),
            file_id: None,
            row_pk: crate::row_pk::RowPk::uuid_from_canonical(commit_id).unwrap(),
        })
        .collect::<Vec<_>>();
    let mut tracked_state = crate::tracked_state::TrackedStateContext::new().reader(&authority_read);
    let retirement_rows = tracked_state
        .load_projected_batch_at_commit(
            &authority_head,
            &retirement_keys,
            &crate::changelog::ChangeRecordProjection::from_columns(&["snapshot_content".into()]),
        )
        .await
        .unwrap();
    assert!(
        (0..retirement_keys.len()).all(|slot| retirement_rows.row(slot).is_none()),
        "unretired fixture checkpoints must exercise exact negative reads"
    );

    let server = open_lix()
        .with_storage(backing)
        .serve()
        .with_embedded_lix_id()
        .await
        .unwrap();
    for delay in [0, 50] {
        for sample in 0..3 {
            let log = Arc::new(std::sync::Mutex::new(Vec::new()));
            let transport = HttpSyncTransport::connect_with(
                TimedClient {
                    inner: Client {
                        server: server.clone(),
                        lose_body: Arc::new(AtomicBool::new(false)),
                    },
                    log: log.clone(),
                    delay,
                },
                &format!("https://example.test/lix/{}", authority.lix_id()),
            )
            .await
            .unwrap();
            let leased = transport.partial_replica_descriptor(None).await.unwrap();
            let state = Arc::new(
                PartialReplicaState::from_leased(
                    transport.protocol_url().into(),
                    authority.active_account_id().into(),
                    uuid::Uuid::now_v7().to_string(),
                    leased.wire,
                )
                .unwrap(),
            );
            transport
                .bind_native_baseline_lease(state.baseline_lease())
                .unwrap();
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
            log.lock().unwrap().clear();

            let started = Instant::now();
            let mut attempts = 0;
            let mut seen = BTreeSet::new();
            loop {
                attempts += 1;
                assert!(attempts <= 256, "checkpoint read did not converge");
                match session.execute(&sql, &[]).await {
                    Ok(actual) => {
                        assert_eq!(actual.rows(), expected.rows());
                        break;
                    }
                    Err(error) => {
                        let demand =
                            crate::sync::runtime::native_sync_demand_request_for_error(&error)
                                .unwrap()
                                .unwrap();
                        assert!(
                            seen.insert(format!("{demand:?}")),
                            "repeated unresolved demand: {error}"
                        );
                        crate::sync::partial_runtime::hydrate_demand(
                            &storage, &state, &transport, demand,
                        )
                        .await
                        .unwrap();
                    }
                }
            }
            let elapsed_ms = started.elapsed().as_secs_f64() * 1000.;
            let requests = log.lock().unwrap().clone();
            log.lock().unwrap().clear();

            // The explicit same-head anchor is the no-moving-interest
            // baseline. It exercises the same native hydration and point
            // lookups without a test-only provider flag.
            let baseline_log = Arc::new(std::sync::Mutex::new(Vec::new()));
            let baseline_transport = HttpSyncTransport::connect_with(
                TimedClient {
                    inner: Client {
                        server: server.clone(),
                        lose_body: Arc::new(AtomicBool::new(false)),
                    },
                    log: baseline_log.clone(),
                    delay,
                },
                &format!("https://example.test/lix/{}", authority.lix_id()),
            )
            .await
            .unwrap();
            let baseline_lease = baseline_transport
                .partial_replica_descriptor(None)
                .await
                .unwrap();
            let baseline_state = Arc::new(
                PartialReplicaState::from_leased(
                    baseline_transport.protocol_url().into(),
                    authority.active_account_id().into(),
                    uuid::Uuid::now_v7().to_string(),
                    baseline_lease.wire,
                )
                .unwrap(),
            );
            baseline_transport
                .bind_native_baseline_lease(baseline_state.baseline_lease())
                .unwrap();
            let baseline_storage = StorageAdapter::new(Memory::new());
            let baseline_read = baseline_storage
                .begin_read(Default::default())
                .await
                .unwrap();
            let mut baseline_writes = baseline_storage.new_write_set();
            let baseline_preconditions =
                stage_partial_bootstrap(&baseline_read, &mut baseline_writes, &baseline_state)
                    .unwrap();
            crate::init::stage_partial_repository_protocol(&mut baseline_writes);
            drop(baseline_read);
            baseline_storage
                .commit_write_set(
                    baseline_writes,
                    StorageWriteOptions {
                        preconditions: baseline_preconditions,
                        await_durable: true,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let (baseline_engine, baseline_session) = Engine::new_partial_replica(
                baseline_storage.clone(),
                EngineOptions::new(),
                &baseline_state,
            )
            .await
            .unwrap();
            baseline_engine.sync_mode().admit_partial_replica(
                baseline_state.clone(),
                crate::sync::partial_replica_write_capability(),
            );
            baseline_storage
                .admit_partial_replica_writer(crate::sync::partial_replica_write_capability());
            baseline_log.lock().unwrap().clear();
            let baseline_started = Instant::now();
            let mut baseline_attempts = 0;
            let mut baseline_seen = BTreeSet::new();
            loop {
                baseline_attempts += 1;
                assert!(
                    baseline_attempts <= 256,
                    "explicit checkpoint read did not converge"
                );
                match baseline_session.execute(&explicit_sql, &[]).await {
                    Ok(actual) => {
                        assert_eq!(actual.rows(), expected.rows());
                        break;
                    }
                    Err(error) => {
                        let demand =
                            crate::sync::runtime::native_sync_demand_request_for_error(&error)
                                .unwrap()
                                .unwrap();
                        assert!(
                            baseline_seen.insert(format!("{demand:?}")),
                            "repeated explicit-anchor demand: {error}"
                        );
                        crate::sync::partial_runtime::hydrate_demand(
                            &baseline_storage,
                            &baseline_state,
                            &baseline_transport,
                            demand,
                        )
                        .await
                        .unwrap();
                    }
                }
            }
            let baseline_elapsed_ms = baseline_started.elapsed().as_secs_f64() * 1000.;
            let baseline_requests = baseline_log.lock().unwrap().clone();
            let baseline_physical_fallback_calls = physical_fallback_calls(&baseline_requests);
            let baseline_exact_interests = baseline_engine
                .sync_mode()
                .read_interests()
                .unwrap()
                .snapshot()
                .unwrap()
                .interests
                .iter()
                .filter(|interest| match interest.as_ref() {
                    crate::hot_state::LogicalReadInterest::Exact { rows, .. } => rows
                        .iter()
                        .any(|row| row.schema_key == crate::undo_redo::UNDO_STATE_SCHEMA_KEY),
                    _ => false,
                })
                .map(|interest| format!("{interest:#?}"))
                .collect::<Vec<_>>();
            assert_eq!(
                baseline_exact_interests.len(),
                0,
                "explicit anchor is immutable; unexpected checkpoint-retirement interests: {baseline_exact_interests:#?}"
            );
            baseline_session.close().await.unwrap();
            let dynamic_physical_fallback_calls = physical_fallback_calls(&requests);
            let native_metadata_walk_calls = requests
                .iter()
                .filter(|request| request["operation"] == "native-metadata-walk")
                .count();
            let read_fulfillment_calls = requests
                .iter()
                .filter(|request| request["operation"] == "read-fulfillment")
                .count();

            let registry = engine.sync_mode().read_interests().unwrap();
            let dynamic = registry.snapshot().unwrap();
            let exact = dynamic
                .interests
                .iter()
                .filter_map(|interest| match interest.as_ref() {
                    crate::hot_state::LogicalReadInterest::Exact {
                        rows,
                        projection,
                        untracked,
                        include_tombstones,
                    } if rows.iter().any(|row| row.schema_key == crate::undo_redo::UNDO_STATE_SCHEMA_KEY) => {
                        Some((rows, projection, untracked, include_tombstones))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert!(
                !exact.is_empty(),
                "active checkpoint reads must retain Exact"
            );
            let mut retained_ids = BTreeSet::new();
            for (rows, projection, untracked, include_tombstones) in &exact {
                assert!(
                    rows.len() <= 64,
                    "one interest must be bounded to a log page"
                );
                assert_eq!(projection.columns, ["snapshot_content"]);
                assert_eq!(*untracked, &Some(false));
                assert_eq!(*include_tombstones, &false);
                for row in *rows {
                    assert_eq!(row.schema_key, crate::undo_redo::UNDO_STATE_SCHEMA_KEY);
                    assert_eq!(row.branch_id, state.descriptor().selected_branch.branch_id);
                    retained_ids.insert(row.row_pk.as_single_string_owned().unwrap());
                }
            }
            assert_eq!(retained_ids, expected_ids);
            let moving = registry.moving_snapshot().unwrap();
            assert!(moving.as_read_snapshot().interests.iter().any(|interest| {
                matches!(
                    interest.as_ref(),
                    crate::hot_state::LogicalReadInterest::Exact { rows, .. }
                        if rows.iter().any(|row| row.schema_key == crate::undo_redo::UNDO_STATE_SCHEMA_KEY)
                )
            }));

            let warm_started = Instant::now();
            assert_eq!(
                session.execute(&sql, &[]).await.unwrap().rows(),
                expected.rows()
            );
            let warm_ms = warm_started.elapsed().as_secs_f64() * 1000.;
            assert!(
                log.lock().unwrap().is_empty(),
                "warm timeline read must not use network"
            );
            log.lock().unwrap().clear();
            let mut page_profiles = Vec::new();
            for page_limit in [10, 20, 40] {
                let page_sql = format!("{query_prefix} LIMIT {page_limit}");
                let expected_page = authority.execute(&page_sql, &[]).await.unwrap();
                let page_started = Instant::now();
                let actual_page = session.execute(&page_sql, &[]).await.unwrap();
                let page_ms = page_started.elapsed().as_secs_f64() * 1000.;
                assert_eq!(actual_page.rows(), expected_page.rows());
                let expected_page_ids = expected_page
                    .rows()
                    .iter()
                    .map(|row| row.get::<String>("commit_id").unwrap())
                    .collect::<BTreeSet<_>>();
                let page_snapshot = registry.snapshot().unwrap();
                let page_exact = page_snapshot
                    .interests
                    .iter()
                    .filter_map(|interest| match interest.as_ref() {
                        crate::hot_state::LogicalReadInterest::Exact { rows, .. } => Some(rows),
                        _ => None,
                    })
                    .flat_map(|rows| rows.iter())
                    .map(|row| row.row_pk.as_single_string_owned().unwrap())
                    .collect::<BTreeSet<_>>();
                assert!(expected_page_ids.is_subset(&page_exact));
                assert!(
                    log.lock().unwrap().is_empty(),
                    "warm page must not use network"
                );
                page_profiles.push(serde_json::json!({
                    "limit": page_limit,
                    "rows": actual_page.rows().len(),
                    "warm_ms": page_ms,
                    "exact_recipe_count": page_snapshot.interests.iter().filter(|interest| matches!(interest.as_ref(), crate::hot_state::LogicalReadInterest::Exact { .. })).count(),
                }));
            }
            log.lock().unwrap().clear();

            // Advance the active head locally. The omitted-anchor query must
            // add the new checkpoint's demand to the moving recipe inventory.
            execute_hydrating(
                &session,
                &storage,
                &state,
                &transport,
                "INSERT INTO lix_file(path, content) VALUES ('/checkpoint-profile/moved.txt', $1)",
                &[Value::Blob(b"new active head".to_vec().into())],
            )
            .await
            .unwrap();
            let moved_checkpoint = execute_hydrating(
                &session,
                &storage,
                &state,
                &transport,
                "SELECT commit_id FROM lix_create_checkpoint(NULL, NULL)",
                &[],
            )
            .await
            .unwrap()
            .rows()[0]
                .get::<String>("commit_id")
                .unwrap();
            let moved_rows = execute_hydrating(&session, &storage, &state, &transport, &sql, &[])
                .await
                .unwrap();
            assert!(moved_rows.rows().iter().any(|row| {
                row.get::<String>("commit_id").ok().as_deref() == Some(&moved_checkpoint)
            }));
            let moved_retirement_key =
                crate::row_pk::RowPk::uuid_from_canonical(&moved_checkpoint).unwrap();
            let moved_snapshot = registry.snapshot().unwrap();
            assert!(
                moved_snapshot
                    .interests
                    .iter()
                    .any(|interest| match interest.as_ref() {
                        crate::hot_state::LogicalReadInterest::Exact { rows, .. } =>
                            rows.iter().any(|row| {
                                row.schema_key == crate::undo_redo::UNDO_STATE_SCHEMA_KEY
                                    && row.row_pk == moved_retirement_key
                            }),
                        _ => false,
                    }),
                "moving-head read must retain its new checkpoint identity"
            );

            eprintln!(
                "CHECKPOINT_LOG_PROFILE {}",
                serde_json::json!({
                    "depth": DEPTH,
                    "delay_ms": delay,
                    "sample": sample,
                    "attempts": attempts,
                    "elapsed_ms": elapsed_ms,
                    "warm_ms": warm_ms,
                    "page_profiles": page_profiles,
                    "request_count": requests.len(),
                    "physical_fallback_calls": dynamic_physical_fallback_calls,
                    "native_metadata_walk_calls": native_metadata_walk_calls,
                    "read_fulfillment_calls": read_fulfillment_calls,
                    "response_bytes": requests.iter().map(|r| r["response_bytes"].as_u64().unwrap()).sum::<u64>(),
                    "explicit_anchor_baseline": {
                        "elapsed_ms": baseline_elapsed_ms,
                        "request_count": baseline_requests.len(),
                        "physical_fallback_calls": baseline_physical_fallback_calls,
                        "response_bytes": baseline_requests.iter().map(|r| r["response_bytes"].as_u64().unwrap()).sum::<u64>(),
                    },
                    "exact_batches": exact.len(),
                    "exact_identities": retained_ids.len(),
                    "moving_head_exact_seen": true,
                    "explicit_anchor_added_recipe": false,
                })
            );
            session.close().await.unwrap();
        }
    }
}
