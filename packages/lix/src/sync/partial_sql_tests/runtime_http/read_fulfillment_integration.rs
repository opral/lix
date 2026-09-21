//! End-to-end read-fulfillment coverage over the real HTTP dispatcher.
//!
//! These checks deliberately warm shared decoded payload caches before
//! changing the authority. The request still carries the older descriptor
//! roots, so a successful read proves that discovery stays pinned to those
//! roots while exercising the operation-sized blob closure and continuation
//! protocol.
use super::file_open_probe::{TimedClient, authority_execute};
use super::*;

fn fulfillment_requests(
    log: &Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
) -> Vec<serde_json::Value> {
    log.lock()
        .unwrap()
        .iter()
        .filter(|entry| entry["operation"] == "read-fulfillment")
        .cloned()
        .collect()
}

fn response_bytes(requests: &[serde_json::Value]) -> u64 {
    requests
        .iter()
        .map(|entry| entry["response_bytes"].as_u64().unwrap_or_default())
        .sum()
}

fn only_read_fulfillment(log: &Arc<std::sync::Mutex<Vec<serde_json::Value>>>) -> bool {
    log.lock()
        .unwrap()
        .iter()
        .all(|entry| entry["operation"] == "read-fulfillment")
}

#[tokio::test]
async fn cold_ranged_read_is_pinned_and_full_read_pages_canonical_blob_inputs() {
    let backing = Memory::new();
    let authority = open_lix().with_storage(backing.clone()).await.unwrap();
    authority
        .set_sync_role(crate::sync::SyncRole::Authority)
        .unwrap();

    let mut seed = 0x4d595df4d0f33173u64;
    let old_content = (0..12 * 1024 * 1024)
        .map(|_| {
            // Keep the pages distinct so the canonical blob closure cannot
            // satisfy a multi-page read from one repeated chunk.
            seed ^= seed << 7;
            seed ^= seed >> 9;
            seed ^= seed << 8;
            seed as u8
        })
        .collect::<Vec<_>>();
    authority
        .execute(
            "INSERT INTO lix_file(path,content) VALUES('/pinned.bin',$1),('/tx.bin',$2)",
            &[
                Value::Blob(old_content.clone().into()),
                Value::Blob(vec![19u8; 512 * 1024].into()),
            ],
        )
        .await
        .unwrap();

    let server = open_lix()
        .with_storage(backing)
        .serve()
        .with_embedded_lix_id()
        .await
        .unwrap();
    // Warm the shared decoded payload caches before advancing the authority.
    // This keeps the regression sensitive to root pinning and continuation
    // assembly rather than first-use cache construction.
    authority
        .read_file_content("/pinned.bin", None)
        .await
        .unwrap();
    authority.read_file_content("/tx.bin", None).await.unwrap();

    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let transport = HttpSyncTransport::connect_with(
        TimedClient {
            inner: Client {
                server: server.clone(),
                lose_body: Arc::new(AtomicBool::new(false)),
            },
            log: log.clone(),
            delay: 0,
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

    let storage = StorageAdapter::new(
        crate::storage_adapter::StorageSession::acquire(Memory::new())
            .await
            .unwrap(),
    );
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
    let engine = Arc::new(engine);
    engine.sync_mode().admit_partial_replica(
        state.clone(),
        crate::sync::partial_replica_write_capability(),
    );
    storage.admit_partial_replica_writer(crate::sync::partial_replica_write_capability());

    let (sender, mut receiver) = tokio::sync::mpsc::channel::<crate::sync::SyncDemand>(16);
    let replica = Lix::from_partial_engine_for_test(Arc::clone(&engine), session, sender);
    let worker_storage = storage.clone();
    let worker_state = state.clone();
    let worker_transport = transport.clone();
    let worker = tokio::spawn(async move {
        while let Some(demand) = receiver.recv().await {
            let result = crate::sync::partial_runtime::hydrate_demand_with_receipt(
                &worker_storage,
                &worker_state,
                &worker_transport,
                demand.request,
            )
            .await;
            let _ = demand.response.send(result);
        }
    });

    // Exercise the receipt path inside one explicit transaction. The first
    // query is cold; the second query remains pinned after the authority
    // advances, and a staged write must still shadow both reads locally.
    let tx_content = vec![19u8; 512 * 1024];
    let mut transaction = replica.begin_transaction().await.unwrap();
    log.lock().unwrap().clear();
    let first = transaction
        .execute("SELECT content FROM lix_file WHERE path='/tx.bin'", &[])
        .await
        .unwrap();
    assert_eq!(
        first.rows()[0].get::<Vec<u8>>("content").unwrap(),
        tx_content
    );
    assert!(
        !fulfillment_requests(&log).is_empty(),
        "cold transaction read must use read fulfillment"
    );
    assert!(only_read_fulfillment(&log));
    let transaction_request_count = fulfillment_requests(&log).len();

    authority_execute(
        &server,
        authority.lix_id(),
        "UPDATE lix_file SET content=$1 WHERE path='/pinned.bin'",
        &[Value::Blob(vec![231u8; old_content.len()].into())],
    )
    .await;
    authority_execute(
        &server,
        authority.lix_id(),
        "UPDATE lix_file SET content=$1 WHERE path='/tx.bin'",
        &[Value::Blob(vec![232u8; tx_content.len()].into())],
    )
    .await;

    let second = transaction
        .execute("SELECT content FROM lix_file WHERE path='/tx.bin'", &[])
        .await
        .unwrap();
    assert_eq!(
        second.rows()[0].get::<Vec<u8>>("content").unwrap(),
        tx_content
    );
    assert_eq!(
        fulfillment_requests(&log).len(),
        transaction_request_count,
        "the HydratedInputs receipt must make the second transaction read local"
    );
    transaction
        .execute(
            "UPDATE lix_file SET content=$1 WHERE path='/tx.bin'",
            &[Value::Blob(b"transaction pending".to_vec().into())],
        )
        .await
        .unwrap();
    let staged = transaction
        .execute("SELECT content FROM lix_file WHERE path='/tx.bin'", &[])
        .await
        .unwrap();
    assert_eq!(
        staged.rows()[0].get::<Vec<u8>>("content").unwrap(),
        b"transaction pending"
    );
    transaction.rollback().await.unwrap();

    let range_start = 5 * 1024 * 1024;
    let range_end = range_start + 128 * 1024;
    log.lock().unwrap().clear();
    let ranged = replica
        .read_file_content("/pinned.bin", Some(range_start as u64..range_end as u64))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ranged.range(), range_start as u64..range_end as u64);
    assert_eq!(ranged.total_size(), old_content.len() as u64);
    assert_eq!(
        ranged.content().as_ref(),
        &old_content[range_start..range_end]
    );
    let ranged_requests = fulfillment_requests(&log);
    assert!(
        !ranged_requests.is_empty(),
        "cold range must use read fulfillment"
    );
    assert!(only_read_fulfillment(&log));
    assert!(
        response_bytes(&ranged_requests) < old_content.len() as u64,
        "range read transferred the full blob: {ranged_requests:?}"
    );

    log.lock().unwrap().clear();
    let full = replica
        .read_file_content("/pinned.bin", None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(full.range(), 0..old_content.len() as u64);
    assert_eq!(full.content().as_ref(), old_content.as_slice());
    let full_requests = fulfillment_requests(&log);
    assert!(only_read_fulfillment(&log));
    assert!(
        full_requests.len() > 1,
        "full read must cross the 4 MiB continuation boundary: {full_requests:?}"
    );
    assert!(
        full_requests
            .iter()
            .all(|entry| entry["discovery"].is_object()),
        "every continuation must carry a discovery profile: {full_requests:?}"
    );

    replica.close().await.unwrap();
    worker.abort();
    server.close().await.unwrap();
    authority.close().await.unwrap();
}
