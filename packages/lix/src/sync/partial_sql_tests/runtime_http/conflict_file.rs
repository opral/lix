//! Independent native replicas reconcile file rows through the real HTTP owner.
use super::*;

#[tokio::test]
async fn conflicting_file_incoming_acceptance_wins_in_both_orders() {
    for (incoming_sql, remote_sql, expected) in [
        // The incoming content keeps the baseline length while the authority
        // changes it. Hash and length must remain one opaque value.
        (
            "UPDATE lix_file SET content=CAST('L' AS BYTEA) WHERE path='/shared.txt'",
            "UPDATE lix_file SET content=CAST('remote-longer' AS BYTEA) WHERE path='/shared.txt'",
            "L",
        ),
        (
            "UPDATE lix_file SET content=CAST('alpha' AS BYTEA) WHERE path='/shared.txt'",
            "UPDATE lix_file SET content=CAST('beta' AS BYTEA) WHERE path='/shared.txt'",
            "alpha",
        ),
        (
            "UPDATE lix_file SET content=CAST('beta' AS BYTEA) WHERE path='/shared.txt'",
            "UPDATE lix_file SET content=CAST('alpha' AS BYTEA) WHERE path='/shared.txt'",
            "beta",
        ),
    ] {
        conflicting_file_case(incoming_sql, remote_sql, expected, false, false).await;
    }
}

#[tokio::test]
async fn lost_file_merge_ack_then_later_authority_write_does_not_reorder_retry() {
    conflicting_file_case(
        "UPDATE lix_file SET content=CAST('incoming' AS BYTEA) WHERE path='/shared.txt'",
        "UPDATE lix_file SET content=CAST('first' AS BYTEA) WHERE path='/shared.txt'",
        "later",
        true,
        false,
    )
    .await;
}

#[derive(Clone)]
struct MergeClient {
    inner: Client,
    lose_merge: Arc<AtomicBool>,
    requests: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
}
impl RawHttpClient for MergeClient {
    fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
        Box::pin(async move {
            let merge = request.url.ends_with("/sync/merge");
            if merge {
                self.requests
                    .lock()
                    .unwrap()
                    .push(request.body.clone().unwrap_or_default());
            }
            let response = self.inner.send(request).await?;
            if merge
                && (200..300).contains(&response.status)
                && self.lose_merge.swap(false, Ordering::SeqCst)
            {
                return Err(LixError::new(
                    "TEST_LOST_MERGE_ACK",
                    "merge committed before response loss",
                ));
            }
            Ok(response)
        })
    }
}

async fn conflicting_file_case(
    incoming_sql: &str,
    remote_sql: &str,
    expected: &str,
    lost_merge: bool,
    checkpoint: bool,
) {
    conflicting_file_case_at_path(
        incoming_sql,
        remote_sql,
        expected,
        lost_merge,
        checkpoint,
        "/shared.txt",
    )
    .await;
}

async fn conflicting_file_case_at_path(
    incoming_sql: &str,
    remote_sql: &str,
    expected: &str,
    lost_merge: bool,
    checkpoint: bool,
    final_path: &str,
) {
    let backing = Memory::new();
    let authority = open_lix().with_storage(backing.clone()).await.unwrap();
    authority
        .set_sync_role(crate::sync::SyncRole::Authority)
        .unwrap();
    authority
        .execute(
            "INSERT INTO lix_file(path,content) VALUES('/shared.txt',CAST('B' AS BYTEA))",
            &[],
        )
        .await
        .unwrap();
    let server = open_lix()
        .with_storage(backing)
        .serve()
        .with_embedded_lix_id()
        .await
        .unwrap();
    let lose_body = Arc::new(AtomicBool::new(false));
    let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let transport = HttpSyncTransport::connect_with(
        MergeClient {
            inner: Client { server, lose_body },
            lose_merge: Arc::new(AtomicBool::new(lost_merge)),
            requests: requests.clone(),
        },
        &format!("https://example.test/lix/{}", authority.lix_id()),
    )
    .await
    .unwrap();
    let wrapper = transport.partial_replica_descriptor(None).await.unwrap();
    let transport = transport
        .fork_native_baseline_lease(&wrapper.wire.lease)
        .unwrap();
    let old = Arc::new(
        PartialReplicaState::from_leased(
            transport.protocol_url().into(),
            authority.active_account_id().into(),
            uuid::Uuid::now_v7().to_string(),
            wrapper.wire,
        )
        .unwrap(),
    );
    let storage = StorageAdapter::new(Memory::new());
    let read = storage.begin_read(Default::default()).await.unwrap();
    let mut writes = storage.new_write_set();
    let preconditions = stage_partial_bootstrap(&read, &mut writes, &old).unwrap();
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
        Engine::new_partial_replica(storage.clone(), EngineOptions::new(), &old)
            .await
            .unwrap();
    let engine = Arc::new(engine);
    engine
        .sync_mode()
        .admit_partial_replica(old.clone(), crate::sync::partial_replica_write_capability());
    storage.admit_partial_replica_writer(crate::sync::partial_replica_write_capability());
    let mut fetches = Fetches::default();
    for sql in [
        "SELECT content FROM lix_file WHERE path='/shared.txt'",
        incoming_sql,
    ] {
        for attempt in 0..128 {
            match session.execute(sql, &[]).await {
                Ok(_) => break,
                Err(error) => {
                    assert!(attempt < 127, "foreground demand did not settle: {error:?}");
                    let demand = crate::sync::runtime::native_sync_demand_request_for_error(&error)
                        .unwrap()
                        .unwrap_or_else(|| panic!("unexpected foreground error: {error:?}"));
                    crate::sync::partial_runtime::hydrate_demand(
                        &storage, &old, &transport, demand,
                    )
                    .await
                    .unwrap();
                }
            }
        }
    }
    let captured_checkpoint = if checkpoint {
        for sql in [
            "SELECT row_ref FROM lix_diff('lix_file')",
            "SELECT commit_id FROM lix_create_checkpoint(ARRAY(SELECT row_ref FROM lix_diff('lix_file')))",
        ] {
            for attempt in 0..128 {
                match session.execute(sql, &[]).await {
                    Ok(_) => break,
                    Err(error) => {
                        assert!(attempt < 127, "checkpoint demand did not settle: {error:?}");
                        assert!(
                            !error.automatic_retry_is_forbidden(),
                            "checkpoint already completed: {error:?}"
                        );
                        let demand =
                            crate::sync::runtime::native_sync_demand_request_for_error(&error)
                                .unwrap()
                                .unwrap_or_else(|| {
                                    panic!("unexpected checkpoint error: {error:?}")
                                });
                        crate::sync::partial_runtime::hydrate_demand(
                            &storage, &old, &transport, demand,
                        )
                        .await
                        .unwrap();
                    }
                }
            }
        }
        let read = storage.begin_read(Default::default()).await.unwrap();
        Some(
            crate::branch::BranchHeadControlContext::new()
                .reader(&read)
                .load(&old.descriptor().selected_branch.branch_id)
                .await
                .unwrap()
                .unwrap()
                .working_diff_checkpoint_commit_id
                .unwrap()
                .to_string(),
        )
    } else {
        None
    };
    prepare_baseline_jump_spines(&storage, &old, &authority, &mut fetches)
        .await
        .unwrap();
    // Serving installed durable authority ownership in another adapter.
    // Admit this test's existing authority handle through the same native owner.
    crate::sync::admit_sync_authority_storage(&authority.storage_adapter(), None)
        .await
        .unwrap();
    authority.execute(remote_sql, &[]).await.unwrap();
    if checkpoint {
        authority.execute("INSERT INTO lix_key_value(key,value,lixcol_untracked) VALUES('private-authority','retained',true)", &[]).await.unwrap();
    }
    let retired_file = if expected == "<deleted>" || final_path != "/shared.txt" {
        authority
            .execute(
                "SELECT id FROM lix_file WHERE path=$1",
                &[Value::Text(final_path.into())],
            )
            .await
            .unwrap()
            .rows()
            .first()
            .map(|row| row.get::<String>("id").unwrap())
    } else {
        None
    };
    let mut prepared = prepare_descriptor_with_merge(
        engine.clone(),
        old.clone(),
        &transport,
        transport.partial_replica_descriptor(None).await.unwrap(),
        crate::sync::partial_publication::PartialRecoveryPolicy::Normal,
    )
    .await;
    if lost_merge {
        assert_eq!(prepared.err().unwrap().code, "TEST_LOST_MERGE_ACK");
        authority
            .execute(
                "UPDATE lix_file SET content=CAST('later' AS BYTEA) WHERE path='/shared.txt'",
                &[],
            )
            .await
            .unwrap();
        prepared = prepare_descriptor_with_merge(
            engine.clone(),
            old.clone(),
            &transport,
            transport.partial_replica_descriptor(None).await.unwrap(),
            crate::sync::partial_publication::PartialRecoveryPolicy::Normal,
        )
        .await;
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0], requests[1],
            "retry must preserve exact accepted operation identity"
        );
    }
    let crate::sync::partial_reconcile::PreparedDescriptor::Ready(prepared) = prepared.unwrap()
    else {
        panic!("accepted row application must produce a publishable descriptor");
    };
    crate::sync::partial_publication::publish_prepared_partial(engine.clone(), prepared)
        .await
        .unwrap();
    if let Some(checkpoint) = captured_checkpoint {
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        assert_eq!(descriptor.selected_branch.checkpoint.commit_id, checkpoint);
        let authority_storage = authority.storage_adapter();
        let read = authority_storage
            .begin_read(Default::default())
            .await
            .unwrap();
        let merged = crate::sync::partial_merge_analysis::record(
            &read,
            crate::changelog::CommitId::parse_lix(
                &descriptor.selected_branch.head.commit_id,
                "test merge",
            )
            .unwrap(),
            true,
        )
        .await
        .unwrap();
        assert!(
            !merged.is_checkpoint,
            "M remains an ordinary merge with a separately selected checkpoint"
        );
        drop(read);
        let private = authority
            .execute(
                "SELECT value FROM lix_key_value WHERE key='private-authority'",
                &[],
            )
            .await
            .unwrap();
        assert!(format!("{private:?}").contains("retained"));
        let diff = authority
            .execute("SELECT row_ref FROM lix_diff('lix_file')", &[])
            .await
            .unwrap();
        let read = authority_storage
            .begin_read(Default::default())
            .await
            .unwrap();
        let mut native = crate::tracked_state::TrackedStateContext::new().reader(&read);
        let native_diff = native
            .diff_commits(
                &checkpoint,
                &descriptor.selected_branch.head.commit_id,
                &crate::tracked_state::TrackedStateDiffRequest::default(),
            )
            .await
            .unwrap();
        let native_payloads = native_diff
            .entries
            .iter()
            .map(|entry| {
                let payload = |row: &crate::tracked_state::TrackedStateDiffRow| {
                    format!("{:?}", native_diff.payloads().get(row.change_id))
                };
                (
                    entry.identity.clone(),
                    entry.before.as_ref().map(payload),
                    entry.after.as_ref().map(payload),
                )
            })
            .collect::<Vec<_>>();
        drop(native);
        drop(read);
        let checkpoint_file = authority.execute("SELECT CAST(content AS TEXT) AS content FROM lix_as_of('lix_file', $1) WHERE path='/shared.txt'", &[Value::Text(checkpoint.clone())]).await;
        let current_file = authority
            .execute(
                "SELECT CAST(content AS TEXT) AS content FROM lix_file WHERE path='/shared.txt'",
                &[],
            )
            .await;
        assert!(
            diff.rows().is_empty(),
            "accepted content equals incoming checkpoint; diff={diff:?}; native={native_diff:?}; payloads={native_payloads:?}; checkpoint={checkpoint_file:?}; current={current_file:?}"
        );
    }
    if let Some(retired_file) = retired_file {
        let head = authority
            .partial_replica_descriptor(None)
            .await
            .unwrap()
            .selected_branch
            .head
            .commit_id;
        let authority_storage = authority.storage_adapter();
        let read = authority_storage
            .begin_read(Default::default())
            .await
            .unwrap();
        let key = crate::tracked_state::TrackedStateKey {
            schema_key: "lix_binary_blob_ref".into(),
            file_id: Some(retired_file.clone()),
            row_pk: crate::row_pk::RowPk::uuid_from_canonical(&retired_file).unwrap(),
        };
        let rows = crate::tracked_state::TrackedStateContext::new()
            .reader(&read)
            .load_projected_batch_at_commit(
                &head,
                &[key],
                &crate::changelog::ChangeRecordProjection::full(),
            )
            .await
            .unwrap();
        assert!(
            rows.row(0).is_none_or(|row| row.deleted()),
            "normal retirement must tombstone the old blob reference"
        );
    }
    let sql = "SELECT CAST(content AS TEXT) AS content FROM lix_file WHERE path=$1";
    let params = [Value::Text(final_path.into())];
    let local = session.execute(sql, &params).await.unwrap();
    let remote = authority.execute(sql, &params).await.unwrap();
    assert_eq!(local, remote);
    if expected != "<deleted>" {
        assert_eq!(local.rows().len(), 1);
    }
    if expected == "<deleted>" {
        assert!(
            local.rows().is_empty(),
            "incoming deletion must retire the file"
        );
        return;
    }
    assert_eq!(remote.rows()[0].get::<String>("content").unwrap(), expected);
}

#[tokio::test]
async fn file_conflict_accepts_local_checkpoint_and_preserves_authority_untracked_state() {
    conflicting_file_case(
        "UPDATE lix_file SET content=CAST('checkpoint-content' AS BYTEA) WHERE path='/shared.txt'",
        "UPDATE lix_file SET content=CAST('authority-first' AS BYTEA) WHERE path='/shared.txt'",
        "checkpoint-content",
        false,
        true,
    )
    .await;
}

#[tokio::test]
async fn incoming_file_delete_retires_a_concurrent_content_edit() {
    conflicting_file_case(
        "DELETE FROM lix_file WHERE path='/shared.txt'",
        "UPDATE lix_file SET content=CAST('authority-edit' AS BYTEA) WHERE path='/shared.txt'",
        "<deleted>",
        false,
        false,
    )
    .await;
}

#[tokio::test]
async fn incoming_file_edit_restores_its_incarnation_after_authority_delete() {
    conflicting_file_case(
        "UPDATE lix_file SET content=CAST('restored' AS BYTEA) WHERE path='/shared.txt'",
        "DELETE FROM lix_file WHERE path='/shared.txt'",
        "restored",
        false,
        false,
    )
    .await;
}

#[tokio::test]
async fn incoming_file_name_claim_retires_the_other_current_file() {
    conflicting_file_case_at_path(
        "UPDATE lix_file SET path='/collision.txt' WHERE path='/shared.txt'",
        "INSERT INTO lix_file(path,content) VALUES('/collision.txt',CAST('other-file' AS BYTEA))",
        "B",
        false,
        false,
        "/collision.txt",
    )
    .await;
}
