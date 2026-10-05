use super::*;

#[tokio::test]
async fn fresh_partial_log_hydrates_checkpoint_conversation_metadata() {
    let backing = Memory::new();
    let authority = open_lix().with_storage(backing.clone()).await.unwrap();
    let created = authority
        .execute(
            "SELECT commit_id FROM lix_create_checkpoint(\
                'Sparse metadata fixture',\
                '{\"_type\":\"zettel_doc\",\"blocks\":[]}'::JSONB\
            )",
            &[],
        )
        .await
        .unwrap();
    let checkpoint_id = created.rows()[0].get::<String>("commit_id").unwrap();
    let expected = authority
        .execute(
            "SELECT conversation_id FROM lix_log() WHERE commit_id = $1",
            &[Value::Text(checkpoint_id.clone())],
        )
        .await
        .unwrap()
        .rows()[0]
        .get::<String>("conversation_id")
        .unwrap();

    let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
    let state = PartialReplicaState::new(
        format!("https://example.test/lix/{}", authority.lix_id()),
        authority.active_account_id().to_owned(),
        uuid::Uuid::now_v7().to_string(),
        serde_json::from_slice(&serde_json::to_vec(&descriptor).unwrap()).unwrap(),
    )
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
        std::sync::Arc::new(state.clone()),
        crate::sync::partial_replica_write_capability(),
    );
    storage.admit_partial_replica_writer(crate::sync::partial_replica_write_capability());

    let mut fetches = Fetches::default();
    let actual = execute_hydrating(
        &session,
        &storage,
        &state,
        &authority,
        "SELECT conversation_id FROM lix_log() WHERE commit_id = $1",
        &[Value::Text(checkpoint_id)],
        &mut fetches,
    )
    .await
    .unwrap()
    .rows()[0]
        .get::<String>("conversation_id")
        .unwrap();
    assert_eq!(actual, expected);
    session.close().await.unwrap();
}

#[cfg(feature = "server-protocol")]
#[tokio::test]
async fn background_checkpoint_upload_hydrates_missing_exact_locator() {
    use crate::server_protocol::{LixServerProtocol, ServerProtocolBody, ServerProtocolContext};
    use crate::sync::SyncTransportFuture;
    use crate::sync::http::{HttpSyncTransport, RawHttpClient, RawHttpRequest, RawHttpResponse};
    use http_body_util::BodyExt;

    #[derive(Clone)]
    struct LocalAuthority(
        LixServerProtocol<Memory>,
        std::sync::Arc<std::sync::Mutex<Vec<(String, Option<Vec<u8>>)>>>,
    );
    impl RawHttpClient for LocalAuthority {
        fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
            Box::pin(async move {
                self.1.lock().unwrap().push((
                    request.url.clone(),
                    request.body.as_ref().map(|body| body.to_vec()),
                ));
                let mut builder = http::Request::builder()
                    .method(request.method)
                    .uri(request.url);
                for (name, value) in request.headers {
                    builder = builder.header(name, value);
                }
                let response = self
                    .0
                    .handle(
                        builder
                            .body(ServerProtocolBody::from(request.body.unwrap_or_default()))
                            .unwrap(),
                        ServerProtocolContext::anonymous(),
                    )
                    .await;
                let status = response.status();
                let body = response
                    .into_body()
                    .collect()
                    .await
                    .map_err(|error| LixError::unknown(error.to_string()))?
                    .to_bytes()
                    .to_vec();
                Ok(RawHttpResponse {
                    status: status.as_u16(),
                    status_text: status.to_string(),
                    body,
                })
            })
        }
    }

    let backing = Memory::new();
    let authority = open_lix().with_storage(backing.clone()).await.unwrap();
    authority
        .execute("INSERT INTO lix_key_value(key,value) VALUES('offline-checkpoint','base'),('sparse-origin','authority-owned'),('unselected-third','keep')", &[])
        .await
        .unwrap();

    // Create a real authority-owned imported change with a non-addressable
    // UUID. Locally-authored changes use UUIDv7 IDs whose owner can be derived
    // without the exact locator row, so deleting one of those would not test
    // the explicit locator hydration path.
    let snapshot = authority
        .pull_sync_repository(None, crate::sync::MAX_SYNC_REQUEST_ITEMS)
        .await
        .unwrap();
    let crate::sync::SyncRepositoryPullResponse::Snapshot { branches, .. } = &snapshot else {
        panic!("initial authority pull should be a snapshot");
    };
    let branch_id = authority.active_branch_id().await.unwrap();
    let mut commits = std::collections::BTreeMap::new();
    let mut headers = std::collections::BTreeMap::new();
    let mut boundaries = std::collections::BTreeMap::new();
    for head in branches
        .iter()
        .flat_map(|branch| {
            [
                branch.head_commit_id.as_deref(),
                branch.checkpoint_commit_id.as_deref(),
            ]
        })
        .flatten()
    {
        let page = authority
            .sync_history(head, crate::sync::MAX_SYNC_HISTORY_PAGE_SIZE)
            .await
            .unwrap();
        commits.extend(
            page.commits
                .into_iter()
                .map(|commit| (commit.commit_id.clone(), commit)),
        );
        headers.extend(
            page.commit_headers
                .into_iter()
                .map(|header| (header.commit_id.clone(), header)),
        );
        boundaries.extend(
            page.boundaries
                .into_iter()
                .map(|boundary| (boundary.commit_id.clone(), boundary)),
        );
    }
    let history = crate::sync::SyncHistoryResponse {
        commits: commits.into_values().collect(),
        commit_headers: headers.into_values().collect(),
        boundaries: boundaries.into_values().collect(),
    };
    let checkpoint_roots = branches
        .iter()
        .filter_map(|branch| {
            let (Some(head), Some(checkpoint)) = (
                branch.head_commit_id.as_deref(),
                branch.checkpoint_commit_id.as_deref(),
            ) else {
                return None;
            };
            (head != checkpoint).then(|| {
                (
                    checkpoint.to_owned(),
                    branch.checkpoint_state_root_id.clone(),
                )
            })
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut snapshot_rows = Vec::new();
    for branch in branches {
        let Some(head) = branch.head_commit_id.as_deref() else {
            continue;
        };
        let mut continuation = None;
        loop {
            let page = authority
                .pull_sync_snapshot_rows(
                    &branch.branch_id,
                    head,
                    continuation.as_deref(),
                    crate::sync::MAX_SYNC_REQUEST_ITEMS,
                )
                .await
                .unwrap();
            snapshot_rows.extend(page.rows);
            let Some(next) = page.continuation else {
                break;
            };
            continuation = Some(next);
        }
    }
    let checkpoint_targets = branches
        .iter()
        .filter_map(|branch| {
            let (Some(head), Some(checkpoint)) = (
                branch.head_commit_id.as_deref(),
                branch.checkpoint_commit_id.as_deref(),
            ) else {
                return None;
            };
            (head != checkpoint).then_some(checkpoint)
        })
        .collect::<BTreeSet<_>>();
    for checkpoint in checkpoint_targets {
        let mut continuation = None;
        loop {
            let page = authority
                .pull_sync_snapshot_rows(
                    checkpoint,
                    checkpoint,
                    continuation.as_deref(),
                    crate::sync::MAX_SYNC_REQUEST_ITEMS,
                )
                .await
                .unwrap();
            snapshot_rows.extend(page.rows);
            let Some(next) = page.continuation else {
                break;
            };
            continuation = Some(next);
        }
    }
    let source_storage = Memory::new();
    Engine::initialize_with_main_branch_id(source_storage.clone(), Some(&branch_id))
        .await
        .unwrap();
    crate::migration::admit_repository(&source_storage, None)
        .await
        .unwrap();
    let source = open_lix().with_storage(source_storage).await.unwrap();
    source
        .set_sync_role(crate::sync::SyncRole::Replica)
        .unwrap();
    source
        .try_install_initial_sync_snapshot(
            "https://example.test/import-source",
            crate::ANONYMOUS_ACCOUNT_ID,
            &snapshot,
            &history.commits,
            &history.commit_headers,
            &snapshot_rows,
            &checkpoint_roots,
        )
        .await
        .unwrap();
    source
        .execute(
            "UPDATE lix_key_value SET value='authority-imported' WHERE key='sparse-origin'",
            &[],
        )
        .await
        .unwrap();
    let mut import = source
        .build_sync_push(
            "https://example.test/import-source",
            crate::sync::MAX_SYNC_REQUEST_ITEMS,
        )
        .await
        .unwrap()
        .unwrap();
    let imported_change = uuid::Uuid::new_v4().to_string();
    let imported_member = import
        .commits
        .iter_mut()
        .flat_map(|commit| &mut commit.members)
        .find(|member| {
            member.authored
                && member.schema_key == "lix_key_value"
                && member
                    .snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.get("key"))
                    .and_then(serde_json::Value::as_str)
                    == Some("sparse-origin")
        })
        .expect("source update has an authored sync member");
    imported_member.change_id = imported_change.clone();
    authority.push_sync_repository(&import).await.unwrap();
    let authoritative_change = authority
        .execute(
            "SELECT lixcol_change_id FROM lix_key_value WHERE key='sparse-origin'",
            &[],
        )
        .await
        .unwrap()
        .rows()[0]
        .get::<String>("lixcol_change_id")
        .unwrap();
    assert_eq!(authoritative_change, imported_change);

    let server = open_lix()
        .with_storage(backing.clone())
        .serve()
        .with_embedded_lix_id()
        .await
        .unwrap();
    let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let client = LocalAuthority(server, requests.clone());
    let remote_id = format!("https://example.test/lix/{}", authority.lix_id());
    let transport = HttpSyncTransport::connect_with(client.clone(), &remote_id)
        .await
        .unwrap();
    let descriptor = transport.partial_replica_descriptor(None).await.unwrap();
    let state = std::sync::Arc::new(
        PartialReplicaState::from_leased(
            remote_id,
            authority.active_account_id().to_owned(),
            uuid::Uuid::now_v7().to_string(),
            descriptor.wire,
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
    let mut fetches = Fetches::default();
    let sparse_owner = execute_hydrating(
        &session,
        &storage,
        &state,
        &authority,
        "SELECT lixcol_change_id FROM lix_key_value WHERE key='sparse-origin'",
        &[],
        &mut fetches,
    )
    .await
    .unwrap()
    .rows()[0]
        .get::<String>("lixcol_change_id")
        .unwrap();
    let change_id =
        crate::changelog::ChangeId::parse_lix(&sparse_owner, "sparse authority owner").unwrap();
    execute_hydrating(
        &session,
        &storage,
        &state,
        &authority,
        "UPDATE lix_key_value SET value='offline-edit' WHERE key='offline-checkpoint'",
        &[],
        &mut fetches,
    )
    .await
    .unwrap();
    let local_checkpoint = execute_hydrating(
        &session,
        &storage,
        &state,
        &authority,
        "SELECT commit_id FROM lix_create_checkpoint(\
            NULL, NULL, ARRAY[\
                lix_row_ref('lix_key_value', NULL, 'offline-checkpoint'),\
                lix_row_ref('lix_key_value', NULL, 'sparse-origin')\
            ]\
        )",
        &[],
        &mut fetches,
    )
    .await
    .unwrap()
    .rows()[0]
        .get::<String>("commit_id")
        .unwrap();

    // Simulate a sparse replica whose checkpoint and selected commit are
    // durable while one exact canonical locator is deferred locally.
    let locator_key = StorageKey(bytes::Bytes::copy_from_slice(
        change_id.as_uuid().as_bytes(),
    ));
    let mut writes = storage.new_write_set();
    writes.delete(
        crate::tracked_state::TRACKED_STATE_CHANGE_LOCATOR_SPACE,
        locator_key,
    );
    // Sparse replicas may hold the selected state row while omitting both the
    // optional standalone changelog row and its canonical locator. Keeping the
    // authority's copies intact makes the exact locator a provable remote
    // dependency, rather than pretending a locally authored change is remote.
    writes.delete(
        crate::changelog::CHANGE_SPACE,
        StorageKey(bytes::Bytes::copy_from_slice(
            change_id.as_uuid().as_bytes(),
        )),
    );
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
    requests.lock().unwrap().clear();

    let checkpoint_id =
        crate::changelog::CommitId::parse_lix(&local_checkpoint, "local checkpoint").unwrap();
    let read = storage.begin_read(Default::default()).await.unwrap();
    let unhydrated =
        crate::tracked_state::load_commit_delta_members_with_payloads(&read, checkpoint_id)
            .await
            .unwrap_err();
    drop(read);
    assert_eq!(
        NativeMetadataRef::from_missing_error(&unhydrated).unwrap(),
        Some(NativeMetadataRef::ChangeLocator(change_id.to_string())),
        "the sparse replacement checkpoint requires the known authority-owned source locator"
    );

    let mut transport = Some(transport);
    let mut connect = || -> SyncTransportFuture<'static, HttpSyncTransport<LocalAuthority>> {
        let client = client.clone();
        let remote = state.remote_id().to_owned();
        Box::pin(async move { HttpSyncTransport::connect_with(client, &remote).await })
    };
    assert!(
        crate::sync::partial_runtime::upload_pending_once(
            &storage,
            &state,
            &mut transport,
            &mut connect,
        )
        .await
        .unwrap()
    );
    let remote = transport
        .as_ref()
        .unwrap()
        .partial_replica_descriptor(None)
        .await
        .unwrap();
    assert_eq!(
        remote.wire.descriptor.selected_branch.checkpoint.commit_id,
        local_checkpoint
    );
    let read = storage.begin_read(Default::default()).await.unwrap();
    let (push, _, _) = crate::sync::partial_push_state::load_partial_push_state(
        &read,
        &state,
        &state.descriptor().selected_branch.branch_id,
    )
    .await
    .unwrap();
    assert!(
        push.prepared.is_none(),
        "successful receipt clears pending upload"
    );
    assert_eq!(push.confirmed.checkpoint, local_checkpoint);
    drop(read);
    let requests = requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|(url, _)| url.ends_with("/sync/native-metadata"))
            .count(),
        1,
        "the missing selected locator is fetched exactly once"
    );
    assert_eq!(
        requests
            .iter()
            .filter(|(url, _)| url.ends_with("/sync/push"))
            .count(),
        1,
        "the checkpoint upload is sent once after hydration"
    );
    let (_, body) = requests
        .iter()
        .find(|(url, _)| url.ends_with("/sync/native-metadata"))
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(body.as_ref().unwrap()).unwrap();
    assert_eq!(
        body["objects"][0]["kind"], "change_locator",
        "hydration must request the exact canonical source locator"
    );
    assert_eq!(body["objects"][0]["changeId"], change_id.to_string());
}

#[tokio::test]
async fn descriptor_only_checkpoint_preserves_native_serving_basis_and_reopens() {
    checkpoint_after_edits(false, false, false).await;
}

#[tokio::test]
async fn checkpoint_after_acknowledged_edits_uploads() {
    checkpoint_after_edits(true, false, false).await;
}

#[tokio::test]
async fn checkpoint_after_pending_ordinary_upload_recovers_offline_send() {
    checkpoint_after_edits(true, true, false).await;
}

#[tokio::test]
async fn described_checkpoint_conversation_survives_partial_upload() {
    checkpoint_after_edits(true, false, true).await;
}

async fn checkpoint_after_edits(acknowledge_edits: bool, pending_ordinary: bool, described: bool) {
    for selected in [false, true] {
        let width = 16usize;
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
        let (engine, session) =
            Engine::new_partial_replica(storage.clone(), EngineOptions::new(), &state)
                .await
                .unwrap();
        engine.sync_mode().admit_partial_replica(
            std::sync::Arc::new(state.clone()),
            crate::sync::partial_replica_write_capability(),
        );
        storage.admit_partial_replica_writer(crate::sync::partial_replica_write_capability());
        let mut fetches = Fetches::default();
        for index in [0, 1] {
            execute_hydrating(
                &session,
                &storage,
                &state,
                &authority,
                "UPDATE lix_key_value SET value = 'edited' WHERE key = $1",
                &[Value::Text(format!("partial-demand-{index:06}"))],
                &mut fetches,
            )
            .await
            .unwrap();
        }
        if acknowledge_edits {
            let uploaded = crate::sync::partial_upload_cycle::upload_partial_once(
                &storage,
                &state,
                &state.descriptor().selected_branch.branch_id,
                uuid::Uuid::now_v7().to_string(),
                32,
                1024 * 1024,
                |request| {
                    let authority = &authority;
                    let state = &state;
                    async move {
                        if pending_ordinary {
                            return Err(LixError::new(
                                "LIX_TRANSPORT_NETWORK",
                                "ordinary edit send interrupted",
                            ));
                        }
                        authority
                            .push_sync_repository_for_account(&request, state.active_account_id())
                            .await
                    }
                },
            )
            .await;
            if pending_ordinary {
                assert_eq!(uploaded.unwrap_err().code, "LIX_TRANSPORT_NETWORK");
            } else {
                assert!(uploaded.unwrap());
            }
        }
        let checkpoint = match (selected, described) {
            (true, true) => {
                "SELECT commit_id FROM lix_create_checkpoint('Checkpoint', '{\"_type\":\"zettel_doc\",\"blocks\":[]}'::JSONB, ARRAY(SELECT row_ref FROM lix_diff('lix_key_value') WHERE key = 'partial-demand-000000'))"
            }
            (false, true) => {
                "SELECT commit_id FROM lix_create_checkpoint('Checkpoint', '{\"_type\":\"zettel_doc\",\"blocks\":[]}'::JSONB)"
            }
            (true, false) => {
                "SELECT commit_id FROM lix_create_checkpoint(NULL, NULL, ARRAY(SELECT row_ref FROM lix_diff('lix_key_value') WHERE key = 'partial-demand-000000'))"
            }
            (false, false) => "SELECT commit_id FROM lix_create_checkpoint(NULL, NULL)",
        };
        execute_hydrating(
            &session,
            &storage,
            &state,
            &authority,
            checkpoint,
            &[],
            &mut fetches,
        )
        .await
        .unwrap_or_else(|error| {
            panic!(
                "partial checkpoint selected={selected}: {error}, details={:?}",
                error.details
            )
        });
        let diff = execute_hydrating(
            &session,
            &storage,
            &state,
            &authority,
            "SELECT COUNT(*) AS n FROM lix_diff('lix_key_value')",
            &[],
            &mut fetches,
        )
        .await
        .unwrap();
        assert_eq!(
            diff.rows()[0].get::<i64>("n").unwrap(),
            if selected { 15 } else { 0 }
        );
        let branch_id = &state.descriptor().selected_branch.branch_id;
        if described {
            assert!(
                crate::sync::partial_upload_cycle::upload_partial_once(
                    &storage,
                    &state,
                    crate::GLOBAL_BRANCH_ID,
                    uuid::Uuid::now_v7().to_string(),
                    32,
                    1024 * 1024,
                    |request| {
                        let authority = &authority;
                        let state = &state;
                        async move {
                            authority
                                .push_sync_repository_for_account(
                                    &request,
                                    state.active_account_id(),
                                )
                                .await
                        }
                    },
                )
                .await
                .expect("upload checkpoint conversation before selected branch")
            );
        }
        if pending_ordinary {
            assert!(
                crate::sync::partial_upload_cycle::upload_partial_once(
                    &storage,
                    &state,
                    branch_id,
                    uuid::Uuid::now_v7().to_string(),
                    32,
                    1024 * 1024,
                    |request| {
                        let authority = &authority;
                        let state = &state;
                        async move {
                            authority
                                .push_sync_repository_for_account(
                                    &request,
                                    state.active_account_id(),
                                )
                                .await
                        }
                    },
                )
                .await
                .unwrap()
            );
        }
        if acknowledge_edits {
            let error = crate::sync::partial_upload_cycle::upload_partial_once(
                &storage,
                &state,
                branch_id,
                uuid::Uuid::now_v7().to_string(),
                32,
                1024 * 1024,
                |_request| async {
                    Err(LixError::new(
                        "LIX_TRANSPORT_NETWORK",
                        "checkpoint authored offline",
                    ))
                },
            )
            .await
            .unwrap_err();
            assert_eq!(error.code, "LIX_TRANSPORT_NETWORK");
        }
        let read = storage.begin_read(Default::default()).await.unwrap();
        let prepared = crate::sync::partial_checkpoint_upload::prepare_partial_checkpoint_upload(
            &read,
            &state,
            branch_id,
            uuid::Uuid::now_v7().to_string(),
            32,
            1024 * 1024,
        )
        .await
        .unwrap_or_else(|error| panic!("checkpoint export selected={selected}: {error}"))
        .unwrap();
        assert!(
            prepared
                .request
                .commits
                .iter()
                .any(|commit| commit.is_checkpoint)
        );
        let mut writes = storage.new_write_set();
        let mut guards = crate::sync::partial_push_state::stage_prepare_partial_upload(
            &read,
            &mut writes,
            &state,
            branch_id,
            &prepared.upload,
        )
        .await
        .unwrap();
        guards.extend(prepared.control_guard);
        drop(read);
        storage
            .commit_partial_replica_write_set(
                crate::sync::partial_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    preconditions: guards,
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        authority
            .push_sync_repository_for_account(&prepared.request, state.active_account_id())
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "checkpoint authority import selected={selected}: {error}, details={:?}",
                    error.details
                )
            });
        assert_eq!(
            authority
                .partial_replica_descriptor(None)
                .await
                .unwrap()
                .selected_branch
                .checkpoint
                .commit_id,
            prepared.upload.target.checkpoint,
            "authority must publish the locally authored checkpoint identity"
        );
        if described {
            let description = authority
                .execute(
                    "SELECT c.title, m.body
                     FROM lix_log() AS l
                     JOIN lix_conversation AS c ON c.id = l.conversation_id
                     JOIN lix_comment AS m ON m.conversation_id = c.id
                     WHERE l.commit_id = $1",
                    &[Value::Text(prepared.upload.target.checkpoint.clone())],
                )
                .await
                .expect("checkpoint description must sync to authority");
            assert_eq!(description.len(), 1);
            assert_eq!(
                description.rows()[0].get::<String>("title").unwrap(),
                "Checkpoint"
            );
        }
        let read = storage.begin_read(Default::default()).await.unwrap();
        let mut writes = storage.new_write_set();
        let guards = crate::sync::partial_push_state::stage_acknowledge_partial_upload(
            &read,
            &mut writes,
            &state,
            branch_id,
            &prepared.upload,
            true,
        )
        .await
        .unwrap();
        drop(read);
        storage
            .commit_partial_replica_write_set(
                crate::sync::partial_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    preconditions: guards,
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        for index in [0, 1] {
            assert_eq!(
                value(
                    authority
                        .execute(
                            "SELECT value FROM lix_key_value WHERE key = $1",
                            &[Value::Text(format!("partial-demand-{index:06}"))]
                        )
                        .await
                        .unwrap()
                ),
                "edited"
            );
        }
        session.close().await.unwrap();
        drop(session);
        drop(engine);
        drop(storage);
        let storage = StorageAdapter::new(memory);
        let (_engine, reopened) =
            Engine::new_partial_replica(storage, EngineOptions::new(), &state)
                .await
                .unwrap_or_else(|error| {
                    panic!("checkpoint stranded admission selected={selected}: {error}")
                });
        for index in [0, 1] {
            assert_eq!(
                value(
                    reopened
                        .execute(
                            "SELECT value FROM lix_key_value WHERE key = $1",
                            &[Value::Text(format!("partial-demand-{index:06}"))]
                        )
                        .await
                        .unwrap()
                ),
                "edited"
            );
        }
        reopened.close().await.unwrap();
    }
}
