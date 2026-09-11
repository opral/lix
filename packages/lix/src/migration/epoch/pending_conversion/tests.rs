//! Actual conversion over TCP/canonical HTTP, with committed-M response loss.
use super::*;
use crate::server_protocol::{ServerProtocolBody, ServerProtocolContext};
use http_body_util::BodyExt;
use std::{
    io::{Read, Write},
    sync::atomic::{AtomicBool, Ordering},
};
#[tokio::test]
async fn pending_native_conversion_resumes_lost_merge_and_preserves_source() {
    run_pending_native_conversion(false, false, false, false).await;
}
#[tokio::test]
async fn pending_file_and_custom_schema_conversion_resumes_lost_merge_and_preserves_source() {
    run_pending_native_conversion(true, false, false, false).await;
}
#[tokio::test]
async fn pending_two_branches_recover_each_lost_merge_before_conversion() {
    run_pending_native_conversion(false, true, false, false).await;
}
#[tokio::test]
async fn pending_cleanup_lost_response_retries_from_closed_partial_storage() {
    run_pending_native_conversion(false, false, true, false).await;
}
#[tokio::test]
async fn pending_new_branch_recovers_global_and_selected_lost_outcomes() {
    run_pending_native_conversion(false, false, false, true).await;
}
async fn run_pending_native_conversion(
    with_files: bool,
    with_branches: bool,
    with_cleanup_loss: bool,
    with_new_branch: bool,
) {
    let authority_memory = crate::Memory::new();
    let authority = crate::open_lix()
        .with_storage(authority_memory.clone())
        .await
        .unwrap();
    authority
        .execute(
            "INSERT INTO lix_key_value(key,value) VALUES('local','B'),('remote','B')",
            &[],
        )
        .await
        .unwrap();
    if with_files {
        authority
            .upsert_file_content("/local.bin", vec![1, 2])
            .await
            .unwrap();
        authority
            .upsert_file_content("/remote.bin", vec![3, 4])
            .await
            .unwrap();
        let schema = serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"migration_custom_note","columns":[{"name":"id","type":"text","nullable":false},{"name":"value","type":"text","nullable":false}],"primary_key":["id"],"unique":[["value"]]});
        authority
            .execute(
                "INSERT INTO lix_registered_schema(value) VALUES(CAST($1 AS JSONB))",
                &[crate::Value::Text(schema.to_string())],
            )
            .await
            .unwrap();
        authority
            .execute(
                "INSERT INTO migration_custom_note(id,value) VALUES('local','before')",
                &[],
            )
            .await
            .unwrap();
    }
    let additional_branch = if with_branches {
        Some(
            authority
                .create_branch(crate::CreateBranchOptions {
                    id: None,
                    name: "migration-second".into(),
                    from_commit_id: None,
                })
                .await
                .unwrap()
                .id,
        )
    } else {
        None
    };
    let base = authority.partial_replica_descriptor(None).await.unwrap();
    let repository = authority.lix_id().to_owned();
    let local_storage = crate::sync::durable_memory_for_test(authority_memory.fork().unwrap());
    let local = crate::open_lix()
        .with_storage(local_storage.clone())
        .await
        .unwrap();
    let adapter = local.storage_adapter();
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let controls = crate::branch::BranchHeadControlContext::new()
        .reader(&read)
        .scan()
        .await
        .unwrap();
    drop(read);
    let mut confirmed = serde_json::Map::new();
    for (branch, control) in controls {
        confirmed.insert(branch,serde_json::json!({"state":"headed","headCommitId":control.head_commit_id.to_string(),"checkpointCommitId":control.working_diff_checkpoint_commit_id.unwrap().to_string()}));
    }
    local
        .execute("UPDATE lix_key_value SET value='L' WHERE key='local'", &[])
        .await
        .unwrap();
    if with_files {
        local
            .upsert_file_content("/local.bin", vec![8, 9, 10])
            .await
            .unwrap();
        local
            .execute(
                "UPDATE migration_custom_note SET value='after' WHERE id='local'",
                &[],
            )
            .await
            .unwrap();
    }
    if let Some(branch) = &additional_branch {
        let other = local
            .open_another_session()
            .with_branch(branch)
            .await
            .unwrap();
        other
            .execute("UPDATE lix_key_value SET value='XL' WHERE key='local'", &[])
            .await
            .unwrap();
        other.close().await.unwrap();
    }
    // The new branch forks from a still-pending original main commit. Its
    // upload boundary must be real confirmed B, while publication preserves C.
    let new_branch = if with_new_branch {
        let branch = local
            .create_branch(crate::CreateBranchOptions {
                id: None,
                name: "migration-new".into(),
                from_commit_id: None,
            })
            .await
            .unwrap()
            .id;
        let other = local
            .open_another_session()
            .with_branch(&branch)
            .await
            .unwrap();
        let checkpoint = other
            .partial_replica_descriptor(Some(&branch))
            .await
            .unwrap()
            .selected_branch
            .checkpoint
            .commit_id;
        other
            .execute(
                "UPDATE lix_key_value SET value='NEW' WHERE key='local'",
                &[],
            )
            .await
            .unwrap();
        let head = other
            .partial_replica_descriptor(Some(&branch))
            .await
            .unwrap()
            .selected_branch
            .head
            .commit_id;
        other.close().await.unwrap();
        Some((branch, checkpoint, head))
    } else {
        None
    };
    let local_head = local
        .partial_replica_descriptor(None)
        .await
        .unwrap()
        .selected_branch
        .head
        .commit_id;
    // Complete local native fixture, with its true prior B acknowledgment.
    let mut writes = adapter.new_write_set();
    writes.put(crate::sync::SYNC_REPLICA_STATE_SPACE,crate::sync::replica_state_key(),serde_json::to_vec(&serde_json::json!({"activeAccountId":crate::ANONYMOUS_ACCOUNT_ID,"cursor":0,"authoritativeBranches":confirmed,"authorityKnownCommitIds":[]})).unwrap());
    adapter
        .commit_write_set(writes, Default::default())
        .await
        .unwrap();
    local.close().await.unwrap();
    drop(local);
    drop(adapter);
    authority
        .execute("UPDATE lix_key_value SET value='R' WHERE key='remote'", &[])
        .await
        .unwrap();
    if with_files {
        authority
            .upsert_file_content("/remote.bin", vec![5, 6, 7])
            .await
            .unwrap();
    }
    if let Some(branch) = &additional_branch {
        let other = authority
            .open_another_session()
            .with_branch(branch)
            .await
            .unwrap();
        other
            .execute(
                "UPDATE lix_key_value SET value='XR' WHERE key='remote'",
                &[],
            )
            .await
            .unwrap();
        other.close().await.unwrap();
    }
    let server = crate::open_lix()
        .with_storage(authority_memory)
        .serve()
        .with_embedded_lix_id()
        .await
        .unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let locator = format!("http://{}/lix/{repository}", listener.local_addr().unwrap());
    let stopped = Arc::new(AtomicBool::new(false));
    let stop = stopped.clone();
    let lost = Arc::new(AtomicBool::new(false));
    let lost_reply = lost.clone();
    let lost_branches = Arc::new(std::sync::Mutex::new(std::collections::BTreeSet::new()));
    let losses = lost_branches.clone();
    let lost_cleanup = Arc::new(AtomicBool::new(false));
    let cleanup_loss = lost_cleanup.clone();
    let global_before = Arc::new(AtomicBool::new(false));
    let global_after = Arc::new(AtomicBool::new(false));
    let thread = std::thread::spawn(move || {
        while !stop.load(Ordering::SeqCst) {
            let (mut stream, _) = match listener.accept() {
                Ok(c) => c,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(5));
                    continue;
                }
                Err(e) => panic!("{e}"),
            };
            let server = server.clone();
            let global_before = global_before.clone();
            let global_after = global_after.clone();
            let lost_reply = lost_reply.clone();
            let losses = losses.clone();
            let cleanup_loss = cleanup_loss.clone();
            std::thread::spawn(move || {
                stream
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                let mut header = Vec::new();
                while !header.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    if stream.read_exact(&mut byte).is_err() {
                        return;
                    }
                    header.push(byte[0]);
                    assert!(header.len() < 64 * 1024);
                }
                let text = String::from_utf8(header).unwrap();
                let mut lines = text.split("\r\n");
                let mut first = lines.next().unwrap().split_whitespace();
                let method = first.next().unwrap();
                let path = first.next().unwrap().to_owned();
                let mut request = http::Request::builder().method(method).uri(&path);
                let mut length = 0;
                for line in lines.filter(|line| !line.is_empty()) {
                    let (name, value) = line.split_once(':').unwrap();
                    request = request.header(name, value.trim());
                    if name.eq_ignore_ascii_case("content-length") {
                        length = value.trim().parse().unwrap();
                    }
                }
                assert!(length <= 64 * 1024 * 1024);
                let mut bytes = vec![0; length];
                stream.read_exact(&mut bytes).unwrap();
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let copied_headers = request.headers_ref().unwrap().clone();
                let (status, body) = runtime.block_on(async {
                    if with_new_branch
                        && path.ends_with("/sync/migration/merge")
                        && !global_before.swap(true, Ordering::SeqCst)
                    {
                        let mut mutation = http::Request::builder().method("POST").uri(format!(
                            "{}/branch/create",
                            path.strip_suffix("/sync/migration/merge").unwrap()
                        ));
                        *mutation.headers_mut().unwrap() = copied_headers.clone();
                        mutation
                            .headers_mut()
                            .unwrap()
                            .remove(http::header::CONTENT_LENGTH);
                        let response = server
                            .handle(
                                mutation
                                    .body(ServerProtocolBody::full(
                                        serde_json::to_vec(
                                            &serde_json::json!({"name":"global-before-selected-M"}),
                                        )
                                        .unwrap(),
                                    ))
                                    .unwrap(),
                                ServerProtocolContext::anonymous(),
                            )
                            .await;
                        assert!(
                            response.status().is_success(),
                            "pre-M global advance failed: {}",
                            response.status()
                        );
                    }
                    let response = server
                        .handle(
                            request.body(ServerProtocolBody::full(bytes)).unwrap(),
                            ServerProtocolContext::anonymous(),
                        )
                        .await;
                    let status = response.status();
                    let body = response.into_body().collect().await.unwrap().to_bytes();
                    if with_new_branch
                        && path.ends_with("/sync/migration/merge")
                        && status.is_success()
                        && !global_after.swap(true, Ordering::SeqCst)
                    {
                        let mut mutation = http::Request::builder().method("POST").uri(format!(
                            "{}/branch/create",
                            path.strip_suffix("/sync/migration/merge").unwrap()
                        ));
                        *mutation.headers_mut().unwrap() = copied_headers.clone();
                        mutation
                            .headers_mut()
                            .unwrap()
                            .remove(http::header::CONTENT_LENGTH);
                        let response = server
                            .handle(
                                mutation
                                    .body(ServerProtocolBody::full(
                                        serde_json::to_vec(
                                            &serde_json::json!({"name":"global-after-selected-M"}),
                                        )
                                        .unwrap(),
                                    ))
                                    .unwrap(),
                                ServerProtocolContext::anonymous(),
                            )
                            .await;
                        assert!(
                            response.status().is_success(),
                            "post-M global advance failed: {}",
                            response.status()
                        );
                    }
                    (status, body)
                });
                if with_cleanup_loss
                    && path.ends_with("/sync/migration/cleanup")
                    && status.is_success()
                    && !cleanup_loss.swap(true, Ordering::SeqCst)
                {
                    return;
                }
                if (path.ends_with("/sync/migration/merge")
                    || path.ends_with("/sync/migration/global/merge"))
                    && status.is_success()
                {
                    let receipt: serde_json::Value = serde_json::from_slice(&body).unwrap();
                    let branch = receipt["request"]["branchId"]
                        .as_str()
                        .unwrap_or(crate::GLOBAL_BRANCH_ID)
                        .to_owned();
                    if losses.lock().unwrap().insert(branch) {
                        lost_reply.store(true, Ordering::SeqCst);
                        return;
                    }
                }
                let _ = write!(
                    stream,
                    "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    status.as_u16(),
                    status.canonical_reason().unwrap_or("response"),
                    body.len()
                );
                let _ = stream.write_all(&body);
            });
        }
    });
    let original = {
        let owned = crate::storage_adapter::StorageSession::acquire(local_storage.clone())
            .await
            .unwrap();
        load_pointer(&owned).await.unwrap().unwrap().1
    };
    let options = crate::ServerOptions::new(locator.clone());
    let requested_branch = new_branch
        .as_ref()
        .map(|(branch, _, _)| branch.clone())
        .unwrap_or_else(|| base.selected_branch.branch_id.clone());
    let first = crate::convert_replica_to_partial(
        local_storage.clone(),
        options.clone(),
        Some(&requested_branch),
    )
    .await;
    assert!(first.is_err());
    assert!(lost.load(Ordering::SeqCst));
    let owned = crate::storage_adapter::StorageSession::acquire(local_storage.clone())
        .await
        .unwrap();
    assert_eq!(load_pointer(&owned).await.unwrap().unwrap().1, original);
    assert!(
        list_retained_replica_sources(&owned)
            .await
            .unwrap()
            .is_empty()
    );
    let (PointerState::Active { bank, .. }, _) = load_pointer(&owned).await.unwrap().unwrap()
    else {
        panic!("source must remain active after rollback")
    };
    let source = StorageAdapter::for_epoch(owned.clone(), bank, original.clone());
    let read = source.begin_read(Default::default()).await.unwrap();
    let control = crate::branch::BranchHeadControlContext::new()
        .reader(&read)
        .load(&base.selected_branch.branch_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(control.head_commit_id.to_string(), local_head);
    drop(read);
    drop(source);
    drop(owned);
    let mut completed = false;
    for _ in 0..3 {
        let result = crate::convert_replica_to_partial(
            local_storage.clone(),
            options.clone(),
            Some(&requested_branch),
        )
        .await;
        if result.is_ok() {
            completed = true;
            break;
        }
        assert!(
            with_branches || with_new_branch,
            "single branch exact replay failed: {result:?}"
        );
    }
    assert!(
        completed,
        "all branch exact outcomes must eventually resume"
    );
    assert_eq!(
        lost_branches.lock().unwrap().len(),
        1 + usize::from(with_branches) + usize::from(with_new_branch)
    );
    let owned = crate::storage_adapter::StorageSession::acquire(local_storage.clone())
        .await
        .unwrap();
    let admitted = partial::admit_partial_epoch(&owned).await.unwrap();
    assert_eq!(
        admitted.state.descriptor().selected_branch.branch_id,
        requested_branch
    );
    assert_ne!(
        admitted.state.descriptor().selected_branch.head.commit_id,
        base.selected_branch.head.commit_id
    );
    if let Some((branch, checkpoint, head)) = &new_branch {
        let actual = authority
            .partial_replica_descriptor(Some(branch))
            .await
            .unwrap();
        assert_eq!(&actual.selected_branch.head.commit_id, head);
        assert_eq!(&actual.selected_branch.checkpoint.commit_id, checkpoint);
        let session = authority
            .open_another_session()
            .with_branch(branch)
            .await
            .unwrap();
        let result = session
            .execute("SELECT value FROM lix_key_value WHERE key='local'", &[])
            .await
            .unwrap();
        assert!(format!("{result:?}").contains("NEW"));
        session.close().await.unwrap();
    }
    // Global advances invalidate the old setup handle's full-mode base. Its
    // refresh would be a write through a non-authority engine; verify through
    // the actual authority-admitted protocol session instead.
    let verification = crate::open_lix()
        .with_server(options.clone())
        .await
        .unwrap();
    let rows = verification
        .execute(
            "SELECT value FROM lix_key_value WHERE key IN ('local','remote')",
            &[],
        )
        .await
        .unwrap();
    verification.close().await.unwrap();
    let rows = format!("{rows:?}");
    assert!(rows.contains("L"));
    assert!(rows.contains("R"));
    if with_files {
        assert_eq!(
            authority
                .read_file_content("/local.bin", None)
                .await
                .unwrap()
                .unwrap()
                .content()
                .as_bytes()
                .as_ref(),
            &[8, 9, 10]
        );
        assert_eq!(
            authority
                .read_file_content("/remote.bin", None)
                .await
                .unwrap()
                .unwrap()
                .content()
                .as_bytes()
                .as_ref(),
            &[5, 6, 7]
        );
        let result = authority
            .execute(
                "SELECT value FROM migration_custom_note WHERE id='local'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(result.rows()[0].get::<String>("value").unwrap(), "after");
    }
    if let Some(branch) = &additional_branch {
        let other = authority
            .open_another_session()
            .with_branch(branch)
            .await
            .unwrap();
        let rows = other
            .execute(
                "SELECT value FROM lix_key_value WHERE key IN ('local','remote')",
                &[],
            )
            .await
            .unwrap();
        let values = format!("{rows:?}");
        assert!(values.contains("XL"));
        assert!(values.contains("XR"));
        other.close().await.unwrap();
    }
    assert_eq!(
        list_retained_replica_sources(&owned).await.unwrap().len(),
        1
    );
    if with_cleanup_loss {
        assert!(lost_cleanup.load(Ordering::SeqCst));
        let before = admitted.state.clone();
        let pointer_before = load_pointer(&owned).await.unwrap().unwrap().1;
        drop(admitted);
        drop(owned);
        assert_eq!(
            crate::retry_replica_migration_cleanup(local_storage.clone(), options.clone())
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            crate::retry_replica_migration_cleanup(local_storage.clone(), options.clone())
                .await
                .unwrap(),
            0
        );
        let owned = crate::storage_adapter::StorageSession::acquire(local_storage.clone())
            .await
            .unwrap();
        assert_eq!(
            load_pointer(&owned).await.unwrap().unwrap().1,
            pointer_before
        );
        assert_eq!(
            partial::admit_partial_epoch(&owned).await.unwrap().state,
            before
        );
        let (journal, _) = load_pending_conversion_journal(
            &owned,
            &bank_code(bank),
            &repository,
            crate::ANONYMOUS_ACCOUNT_ID,
            &base.selected_branch.branch_id,
        )
        .await
        .unwrap()
        .unwrap();
        assert!(journal.native_pin_cleaned);
    }
    stopped.store(true, Ordering::SeqCst);
    thread.join().unwrap();
}
